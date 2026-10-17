// Package subscriptionreaper removes runtime Presence owned by Gate boots
// whose shared liveness lease has expired.
package subscriptionreaper

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	farmactor "farmgame/internal/farm/actor"
	"farmgame/internal/gateway/bootlease"

	"github.com/prometheus/client_golang/prometheus"
)

// Mode 是 GateBoot 退役(决策 1)的三态开关:
//   - enabled:现行行为,死 Gate 的 Presence 由 Reaper 精确移除;
//   - shadow:照常检查租约权威并统计 DeadBoots/WouldReap,但不执行移除,
//     用于验证 BatchRenew 租约收敛是否与 Reaper 一致;
//   - disabled:不做 GateBoot 检查,订阅生命周期完全交给逐订阅续租(W5B)、
//     精确断开清理与 Hub 租约兜底(PruneExpired 在全模式下保留)。
type Mode string

const (
	ModeEnabled  Mode = "enabled"
	ModeShadow   Mode = "shadow"
	ModeDisabled Mode = "disabled"
)

type Stats struct {
	Sweeps          atomic.Uint64
	BootsChecked    atomic.Uint64
	DeadBoots       atomic.Uint64
	Removed         atomic.Uint64
	WouldReap       atomic.Uint64
	FallbackRemoved atomic.Uint64
	Errors          atomic.Uint64
}

type Reaper struct {
	hub          *farmactor.SubscriptionHub
	store        bootlease.Store
	interval     time.Duration
	checkTimeout time.Duration
	mode         Mode
	stats        Stats
}

func New(hub *farmactor.SubscriptionHub, store bootlease.Store, interval, checkTimeout time.Duration, mode Mode) (*Reaper, error) {
	if hub == nil || store == nil {
		return nil, errors.New("gate boot reaper: hub and lease store are required")
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if checkTimeout <= 0 {
		checkTimeout = 2 * time.Second
	}
	switch mode {
	case ModeEnabled, ModeShadow, ModeDisabled:
	default:
		return nil, errors.New("gate boot reaper: mode must be enabled|shadow|disabled")
	}
	return &Reaper{hub: hub, store: store, interval: interval, checkTimeout: checkTimeout, mode: mode}, nil
}

func (r *Reaper) Mode() Mode { return r.mode }

// Sweep snapshots only GateBoot fences currently present in the local Hub.
// It never enumerates global gateways and never activates a FarmActor.
func (r *Reaper) Sweep(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.stats.Sweeps.Add(1)
	var sweepErr error
	expired, err := r.hub.PruneExpired(ctx, time.Now())
	if err != nil {
		r.stats.Errors.Add(1)
		sweepErr = errors.Join(sweepErr, err)
	} else {
		r.stats.FallbackRemoved.Add(uint64(len(expired)))
	}
	if r.mode != ModeDisabled {
		for _, fence := range r.hub.BootFences() {
			r.stats.BootsChecked.Add(1)
			checkCtx, cancel := context.WithTimeout(ctx, r.checkTimeout)
			alive, err := r.store.Alive(checkCtx, bootlease.Fence{
				GatewayID: fence.GatewayID, BootID: fence.GatewayBootID,
			})
			cancel()
			if err != nil {
				// Authority uncertainty is fail-closed for cleanup: retain Presence
				// and retry. Absence, not a Redis error, is the death proof.
				r.stats.Errors.Add(1)
				sweepErr = errors.Join(sweepErr, err)
				continue
			}
			if alive {
				continue
			}
			r.stats.DeadBoots.Add(1)
			if r.mode == ModeShadow {
				// shadow:只统计"本会移除多少",不执行移除,
				// 观察租约机制是否独立收敛(PruneExpired 应使 WouldReap 归零)。
				if presences := len(r.hub.SubscriptionRefs(fence)); presences > 0 {
					r.stats.WouldReap.Add(uint64(presences))
				}
				continue
			}
			removed, err := r.hub.ReapBoot(ctx, fence)
			if err != nil {
				r.stats.Errors.Add(1)
				sweepErr = errors.Join(sweepErr, err)
				continue
			}
			r.stats.Removed.Add(uint64(len(removed)))
		}
	}
	return sweepErr
}

func (r *Reaper) Run(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = r.Sweep(ctx)
		}
	}
}

func (r *Reaper) Stats() *Stats { return &r.stats }

func RegisterMetrics(reg prometheus.Registerer, reaper *Reaper) {
	if reg == nil || reaper == nil {
		return
	}
	stats := reaper.Stats()
	for _, metric := range []prometheus.Collector{
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "farm_gate_boot_reaper_sweeps_total", Help: "Local GateBootReaper sweeps."}, func() float64 { return float64(stats.Sweeps.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "farm_gate_boot_reaper_boots_checked_total", Help: "Gate boots checked; sourced only from local SubscriptionHub fences."}, func() float64 { return float64(stats.BootsChecked.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "farm_gate_boot_reaper_dead_boots_total", Help: "Expired Gate boot leases observed by this holder."}, func() float64 { return float64(stats.DeadBoots.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "farm_gate_boot_reaper_presences_removed_total", Help: "Exact Presence generations removed for expired Gate boots."}, func() float64 { return float64(stats.Removed.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "farm_gate_boot_reaper_would_reap_total", Help: "Shadow mode: presences that would have been reaped; convergence to zero after lease expiry proves BatchRenew alone handles gate death."}, func() float64 { return float64(stats.WouldReap.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "farm_subscription_fallback_expired_total", Help: "Presence generations removed by the long, non-renewed fallback TTL."}, func() float64 { return float64(stats.FallbackRemoved.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "farm_gate_boot_reaper_errors_total", Help: "Lease authority or Hub cleanup errors; errors retain Presence."}, func() float64 { return float64(stats.Errors.Load()) }),
	} {
		reg.MustRegister(metric)
	}
}
