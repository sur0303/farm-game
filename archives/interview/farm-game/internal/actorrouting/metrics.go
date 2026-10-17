package actorrouting

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Resolve result labels (bounded).
const (
	ResolveResultSuccess        = "success"
	ResolveResultStale          = "stale"
	ResolveResultNoAssignment   = "no_assignment"
	ResolveResultInvalidAddress = "invalid_address"
	ResolveResultError          = "error"
)

// RedirectReasonStaleRejected is the reason label when ApplyRedirect rejects a stale revision.
const RedirectReasonStaleRejected = "STALE_REVISION"

// Metrics holds route-plane prometheus collectors (Batch 2.6).
type Metrics struct {
	ResolveTotal        *prometheus.CounterVec
	RedirectTotal       *prometheus.CounterVec
	SnapshotRevision    prometheus.Gauge
	WriterEpochMismatch prometheus.Counter
	ResolveLeaseExpired prometheus.Counter
	WatchApplyLagMs     prometheus.Gauge

	loadedAtUnixNano atomic.Int64
}

// RegisterRouteMetrics registers farm_actor_route_* / redirect / snapshot / writer_epoch metrics.
// Safe to call once per process registry; returns the handle used for Inc/Set.
func RegisterRouteMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	m := &Metrics{
		ResolveTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor", Name: "route_resolve_total",
			Help: "Actor route Resolve outcomes by domain and result.",
		}, []string{"domain", "result"}),
		RedirectTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor", Name: "redirect_total",
			Help: "Actor route redirect apply attempts by domain and reason.",
		}, []string{"domain", "reason"}),
		SnapshotRevision: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "actor", Name: "route_snapshot_revision",
			Help: "Last successfully installed RouteSnapshot GlobalRevision.",
		}),
		WriterEpochMismatch: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor", Name: "writer_epoch_mismatch_total",
			Help: "Writer-epoch fence mismatches that triggered actor eviction.",
		}),
		ResolveLeaseExpired: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "route_resolve_lease_expired_total",
			Help: "Local route Resolve attempts rejected because the installed complete Snapshot lease deadline expired.",
		}),
		WatchApplyLagMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "route_watch_apply_lag_ms",
			Help: "Time from complete Watch Snapshot receipt through atomic local installation.",
		}),
	}
	age := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: "farm", Subsystem: "actor", Name: "route_snapshot_age_seconds",
		Help: "Seconds since the local route snapshot was last Replace-d.",
	}, m.snapshotAgeSeconds)
	reg.MustRegister(m.ResolveTotal, m.RedirectTotal, m.SnapshotRevision, m.WriterEpochMismatch, m.ResolveLeaseExpired, m.WatchApplyLagMs, age)
	return m
}

func (m *Metrics) snapshotAgeSeconds() float64 {
	if m == nil {
		return 0
	}
	loaded := m.loadedAtUnixNano.Load()
	if loaded == 0 {
		return 0
	}
	return time.Since(time.Unix(0, loaded)).Seconds()
}

// NoteSnapshot records a successful table Replace (revision + loadedAt for age).
func (m *Metrics) NoteSnapshot(revision uint64, at time.Time) {
	if m == nil {
		return
	}
	if at.IsZero() {
		at = time.Now()
	}
	m.loadedAtUnixNano.Store(at.UnixNano())
	m.SnapshotRevision.Set(float64(revision))
}

// ObserveResolve increments route_resolve_total for a Resolve outcome.
func (m *Metrics) ObserveResolve(domain RouteDomain, err error) {
	if m == nil {
		return
	}
	m.ResolveTotal.WithLabelValues(string(domain), resolveResult(err)).Inc()
	if errors.Is(err, ErrLeaseExpired) {
		m.ResolveLeaseExpired.Inc()
	}
}

// ObserveRedirect increments redirect_total.
// ObserveWatchApplyLag records the bounded receipt-to-install duration for a
// complete remote Snapshot. It never runs from Resolve.
func (m *Metrics) ObserveWatchApplyLag(elapsed time.Duration) {
	if m == nil {
		return
	}
	if elapsed < 0 {
		elapsed = 0
	}
	m.WatchApplyLagMs.Set(float64(elapsed) / float64(time.Millisecond))
}

func (m *Metrics) ObserveRedirect(domain RouteDomain, reason string) {
	if m == nil {
		return
	}
	if reason == "" {
		reason = "UNKNOWN"
	}
	m.RedirectTotal.WithLabelValues(string(domain), reason).Inc()
}

// ObserveWriterEpochMismatch increments writer_epoch_mismatch_total.
func (m *Metrics) ObserveWriterEpochMismatch() {
	if m == nil {
		return
	}
	m.WriterEpochMismatch.Inc()
}

func resolveResult(err error) string {
	switch {
	case err == nil:
		return ResolveResultSuccess
	case errors.Is(err, ErrLeaseExpired), errors.Is(err, ErrStaleRoute):
		return ResolveResultStale
	case errors.Is(err, ErrNoAssignment):
		return ResolveResultNoAssignment
	case errors.Is(err, ErrInvalidAddress):
		return ResolveResultInvalidAddress
	default:
		return ResolveResultError
	}
}
