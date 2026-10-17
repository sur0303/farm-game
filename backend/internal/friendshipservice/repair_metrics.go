package friendshipservice

import (
	"context"
	"fmt"
	"sync"
	"time"

	"farmgame/internal/friend"

	"github.com/prometheus/client_golang/prometheus"
)

const maxInstrumentedRepairViewers = 4096

// ProjectionMetricsRecorder contains only canonical Pair/FriendIndex repair
// signals. It has no legacy-roster observation surface.
type ProjectionMetricsRecorder interface {
	RecordPairIndexMismatch(int)
	SetProjectionRepairAge(time.Duration)
}

type PrometheusProjectionMetrics struct {
	pairIndexMismatch prometheus.Counter
	repairAge         prometheus.Gauge
}

func NewPrometheusProjectionMetrics(registerer prometheus.Registerer) (*PrometheusProjectionMetrics, error) {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	recorder := &PrometheusProjectionMetrics{
		pairIndexMismatch: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "friend_pair_index_mismatch_total",
			Help: "Canonical Pair and FriendIndex projection mismatches repaired by reconciliation.",
		}),
		repairAge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "friend_projection_repair_age_ms",
			Help: "Age in milliseconds of the most recently claimed FriendIndex repair intent.",
		}),
	}
	collectors := []prometheus.Collector{recorder.pairIndexMismatch, recorder.repairAge}
	for index, collector := range collectors {
		registered := collector
		if err := registerer.Register(collector); err != nil {
			existing, ok := err.(prometheus.AlreadyRegisteredError)
			if !ok {
				return nil, err
			}
			registered = existing.ExistingCollector
		}
		switch index {
		case 0:
			var ok bool
			if recorder.pairIndexMismatch, ok = registered.(prometheus.Counter); !ok {
				return nil, fmt.Errorf("friend Pair/Index mismatch metric type %T", registered)
			}
		case 1:
			var ok bool
			if recorder.repairAge, ok = registered.(prometheus.Gauge); !ok {
				return nil, fmt.Errorf("friend projection repair age metric type %T", registered)
			}
		}
	}
	return recorder, nil
}

func (r *PrometheusProjectionMetrics) RecordPairIndexMismatch(count int) {
	if r != nil && count > 0 {
		r.pairIndexMismatch.Add(float64(count))
	}
}

func (r *PrometheusProjectionMetrics) SetProjectionRepairAge(age time.Duration) {
	if r == nil {
		return
	}
	if age < 0 {
		age = 0
	}
	r.repairAge.Set(float64(age.Milliseconds()))
}

type MemoryProjectionMetrics struct {
	PairIndexMismatch int
	RepairAge         time.Duration
}

func (r *MemoryProjectionMetrics) RecordPairIndexMismatch(count int) {
	if r != nil && count > 0 {
		r.PairIndexMismatch += count
	}
}

func (r *MemoryProjectionMetrics) SetProjectionRepairAge(age time.Duration) {
	if r != nil {
		r.RepairAge = age
	}
}

// InstrumentedRepairQueue keeps only bounded process-local timing hints. The
// durable intent and all retry semantics remain in the delegated queue.
type InstrumentedRepairQueue struct {
	delegate friend.FriendIndexRepairQueue
	recorder ProjectionMetricsRecorder
	mu       sync.Mutex
	started  map[uint64]time.Time
}

func NewInstrumentedRepairQueue(delegate friend.FriendIndexRepairQueue, recorder ProjectionMetricsRecorder) *InstrumentedRepairQueue {
	return &InstrumentedRepairQueue{delegate: delegate, recorder: recorder, started: make(map[uint64]time.Time)}
}

func (q *InstrumentedRepairQueue) Enqueue(ctx context.Context, viewer uint64, dueAtMs int64) error {
	if err := q.delegate.Enqueue(ctx, viewer, dueAtMs); err != nil {
		return err
	}
	q.mu.Lock()
	if _, exists := q.started[viewer]; !exists && len(q.started) < maxInstrumentedRepairViewers {
		q.started[viewer] = time.UnixMilli(dueAtMs)
	}
	q.mu.Unlock()
	return nil
}

func (q *InstrumentedRepairQueue) Claim(ctx context.Context, nowMs, leaseMs int64, limit int) ([]friend.FriendIndexRepairLease, error) {
	leases, err := q.delegate.Claim(ctx, nowMs, leaseMs, limit)
	if err != nil {
		return nil, err
	}
	q.mu.Lock()
	for _, lease := range leases {
		if started, exists := q.started[lease.ViewerPlayerID]; exists {
			q.recorder.SetProjectionRepairAge(time.UnixMilli(nowMs).Sub(started))
		}
	}
	q.mu.Unlock()
	return leases, nil
}

func (q *InstrumentedRepairQueue) Complete(ctx context.Context, lease friend.FriendIndexRepairLease, nowMs, repeatDelayMs int64) error {
	if err := q.delegate.Complete(ctx, lease, nowMs, repeatDelayMs); err != nil {
		return err
	}
	q.mu.Lock()
	delete(q.started, lease.ViewerPlayerID)
	q.mu.Unlock()
	return nil
}

func (q *InstrumentedRepairQueue) Retry(ctx context.Context, lease friend.FriendIndexRepairLease, dueAtMs int64) error {
	return q.delegate.Retry(ctx, lease, dueAtMs)
}

func RecordFriendIndexReconcileReport(recorder ProjectionMetricsRecorder) func(friend.FriendIndexReconcileReport) {
	return func(report friend.FriendIndexReconcileReport) {
		if recorder != nil {
			recorder.RecordPairIndexMismatch(report.AppliedRows + report.RemovedRows)
		}
	}
}
