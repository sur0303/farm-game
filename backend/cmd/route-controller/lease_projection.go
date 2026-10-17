package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/routeplan"
	scaleplan "farmgame/internal/scaleplan"
)

type leaseProjectionStatus struct {
	KnownFresh                      bool   `json:"known_fresh"`
	StartupReady                    bool   `json:"startup_ready"`
	Mode                            string `json:"mode"`
	GlobalRevision                  uint64 `json:"global_revision"`
	ObservedShards                  int    `json:"observed_shards"`
	ChangedShards                   int    `json:"changed_shards"`
	MinimumLeaseUntilMs             int64  `json:"minimum_lease_until_ms"`
	LastSuccessAtMs                 int64  `json:"last_success_at_ms"`
	ProjectionIntervalMs            int64  `json:"projection_interval_ms"`
	MinimumRequiredRemainingMs      int64  `json:"minimum_required_remaining_ms"`
	WaveTimeoutMs                   int64  `json:"wave_timeout_ms"`
	LastWaveDurationMs              int64  `json:"last_wave_duration_ms"`
	MaximumSuccessfulWaveDurationMs int64  `json:"maximum_successful_wave_duration_ms"`
	ProjectionAttempts              uint64 `json:"projection_attempts"`
	ProjectionSuccesses             uint64 `json:"projection_successes"`
	ObservationTimeoutFailures      uint64 `json:"observation_timeout_failures"`
	StaleCallbackRejections         uint64 `json:"stale_callback_rejections"`
	InsufficientDeadlineFailures    uint64 `json:"insufficient_deadline_failures"`
	PublishPendingFailures          uint64 `json:"publish_pending_failures"`
	AuthorityObservationFailures    uint64 `json:"authority_observation_failures"`
	FailureClass                    string `json:"failure_class,omitempty"`
}

type leaseProjectionRuntime struct {
	manager          *authority.RouteAssignmentManager
	interval         time.Duration
	minimumRemaining time.Duration
	waveTimeout      time.Duration
	now              func() time.Time
	control          routeplan.ControlMutationOptions
	waveSequence     atomic.Uint64

	mu     sync.RWMutex
	status leaseProjectionStatus
	cancel context.CancelFunc
	done   chan struct{}
}

func newLeaseProjectionRuntime(manager *authority.RouteAssignmentManager, interval, minimumRemaining, waveTimeout time.Duration, now func() time.Time, controls ...routeplan.ControlMutationOptions) (*leaseProjectionRuntime, error) {
	if manager == nil || interval <= 0 || interval > time.Minute || minimumRemaining <= 0 || minimumRemaining > time.Minute ||
		waveTimeout <= 0 || waveTimeout > 2*time.Minute {
		return nil, errors.New("route-controller: invalid lease projection configuration")
	}
	if now == nil {
		now = time.Now
	}
	var control routeplan.ControlMutationOptions
	if len(controls) > 1 {
		return nil, errors.New("route-controller: one control mutation configuration allowed")
	}
	if len(controls) == 1 {
		control = controls[0]
		if err := control.Validate(); err != nil {
			return nil, err
		}
	}
	return &leaseProjectionRuntime{
		manager: manager, interval: interval, minimumRemaining: minimumRemaining, waveTimeout: waveTimeout, now: now,
		control: control,
		status: leaseProjectionStatus{
			Mode: leaseProjectionModeAllActive, ProjectionIntervalMs: interval.Milliseconds(), MinimumRequiredRemainingMs: minimumRemaining.Milliseconds(),
			WaveTimeoutMs: waveTimeout.Milliseconds(),
		},
	}, nil
}

func (r *leaseProjectionRuntime) Bootstrap(ctx context.Context) error {
	if r == nil || ctx == nil {
		return errors.New("route-controller: lease projection runtime unavailable")
	}
	return r.project(ctx, scaleplan.ControlOperationBootstrap)
}

func (r *leaseProjectionRuntime) Start(parent context.Context) error {
	if r == nil || parent == nil {
		return errors.New("route-controller: lease projection runtime unavailable")
	}
	r.mu.Lock()
	if r.cancel != nil {
		r.mu.Unlock()
		return errors.New("route-controller: lease projection runtime already started")
	}
	ctx, cancel := context.WithCancel(parent)
	r.cancel = cancel
	r.done = make(chan struct{})
	done := r.done
	r.mu.Unlock()
	go func() {
		defer close(done)
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				waveCtx, waveCancel := context.WithTimeout(ctx, r.waveTimeout)
				_ = r.project(waveCtx, scaleplan.ControlOperationProjection)
				waveCancel()
			}
		}
	}()
	return nil
}

func (r *leaseProjectionRuntime) Close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	cancel, done := r.cancel, r.done
	r.cancel = nil
	r.done = nil
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (r *leaseProjectionRuntime) Ready() bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	status := r.status
	r.mu.RUnlock()
	now := r.now()
	maximumObservationAge := 2*r.interval + r.waveTimeout
	return status.KnownFresh && status.FailureClass == "" && status.LastSuccessAtMs > 0 &&
		now.UnixMilli()-status.LastSuccessAtMs <= maximumObservationAge.Milliseconds() &&
		status.MinimumLeaseUntilMs-now.UnixMilli() >= r.minimumRemaining.Milliseconds()
}

func (r *leaseProjectionRuntime) Status() leaseProjectionStatus {
	if r == nil {
		return leaseProjectionStatus{FailureClass: "NOT_CONFIGURED"}
	}
	r.mu.RLock()
	status := r.status
	r.mu.RUnlock()
	status.KnownFresh = r.Ready()
	if !status.KnownFresh && status.FailureClass == "" {
		status.FailureClass = "STALE_OR_INSUFFICIENT_DEADLINE"
	}
	return status
}

func (r *leaseProjectionRuntime) Snapshot() actorrouting.RouteSnapshot {
	if r == nil || r.manager == nil {
		return actorrouting.RouteSnapshot{}
	}
	return r.manager.CurrentSnapshot()
}

func (r *leaseProjectionRuntime) project(ctx context.Context, kind scaleplan.ControlOperationKind) error {
	started := time.Now()
	operationID := fmt.Sprintf("route-controller/lease-projection/%d", r.waveSequence.Add(1))
	controlCtx, resolveControl, err := r.control.Begin(ctx, operationID, kind)
	result := authority.LeaseProjectionResult{}
	if err == nil {
		result, err = r.manager.ProjectFreshLeases(controlCtx, r.minimumRemaining)
		if err == nil && resolveControl != nil {
			if resolveErr := resolveControl(); resolveErr != nil {
				err = resolveErr
			}
		}
	}
	durationMs := time.Since(started).Milliseconds()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status.ProjectionAttempts++
	r.status.LastWaveDurationMs = durationMs
	if err != nil {
		r.status.KnownFresh = false
		r.status.FailureClass = leaseProjectionFailureClass(err)
		switch r.status.FailureClass {
		case "OBSERVATION_TIMEOUT":
			r.status.ObservationTimeoutFailures++
		case "STALE_CALLBACK_REJECTED":
			r.status.StaleCallbackRejections++
		case "INSUFFICIENT_DEADLINE":
			r.status.InsufficientDeadlineFailures++
		case "PUBLISH_PENDING":
			r.status.PublishPendingFailures++
		default:
			r.status.AuthorityObservationFailures++
		}
		return err
	}
	r.status.KnownFresh = true
	r.status.GlobalRevision = result.GlobalRevision
	r.status.ObservedShards = result.Observed
	r.status.ChangedShards = result.Changed
	r.status.MinimumLeaseUntilMs = result.MinimumLeaseUntilMs
	r.status.LastSuccessAtMs = result.CompletedAt.UnixMilli()
	r.status.FailureClass = ""
	r.status.ProjectionSuccesses++
	if durationMs > r.status.MaximumSuccessfulWaveDurationMs {
		r.status.MaximumSuccessfulWaveDurationMs = durationMs
	}
	return nil
}

func leaseProjectionFailureClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "OBSERVATION_TIMEOUT"
	case errors.Is(err, authority.ErrLeaseProjectionStale):
		return "STALE_CALLBACK_REJECTED"
	case errors.Is(err, authority.ErrLeaseProjectionTooClose):
		return "INSUFFICIENT_DEADLINE"
	case errors.Is(err, authority.ErrSnapshotPublish), errors.Is(err, authority.ErrLeaseProjectionPending):
		return "PUBLISH_PENDING"
	default:
		return "AUTHORITY_OBSERVATION_FAILED"
	}
}
