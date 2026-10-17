package main

import (
	"errors"

	"farmgame/internal/actorrouting/authority"
	core "farmgame/internal/routecontrol"

	"github.com/prometheus/client_golang/prometheus"
)

type routeDeadlineMetrics struct {
	passes            *prometheus.CounterVec
	publishes         prometheus.Counter
	snapshotPublishes *prometheus.CounterVec
	snapshotBytes     prometheus.Counter
	noops             prometheus.Counter
	changedShards     prometheus.Gauge
	casConflicts      prometheus.Counter
	readErrors        prometheus.Counter
	stalledShards     prometheus.Gauge
	minRemainingMs    prometheus.Gauge
	projectionLag     prometheus.Gauge
}

func newRouteDeadlineMetrics(registerer prometheus.Registerer) *routeDeadlineMetrics {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	metrics := &routeDeadlineMetrics{
		passes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "route_deadline_refresh_pass_total",
			Help: "Bounded Manager-owned ACTIVE Authority deadline reconciliation passes by result.",
		}, []string{"result"}),
		publishes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "route_deadline_refresh_publish_total",
			Help: "Complete RouteSnapshot revisions published by deadline reconciliation.",
		}),
		snapshotPublishes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "route_snapshot_publish_total",
			Help: "Complete RouteSnapshot publications by semantic reason.",
		}, []string{"reason"}),
		snapshotBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "route_snapshot_bytes_total",
			Help: "Canonical bytes written for complete RouteSnapshot publications.",
		}),
		noops: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "route_deadline_refresh_noop_total",
			Help: "Deadline reconciliation passes that did not publish a Snapshot.",
		}),
		changedShards: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "route_deadline_refresh_changed_shards",
			Help: "ACTIVE shard deadlines changed by the most recent successful publication.",
		}),
		casConflicts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "route_deadline_refresh_cas_conflict_total",
			Help: "Deadline refresh publications rejected by complete Snapshot CAS.",
		}),
		readErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "route_deadline_refresh_read_error_total",
			Help: "All-or-nothing deadline refresh passes aborted by an exact Authority read.",
		}),
		stalledShards: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "route_deadline_refresh_stalled_shards",
			Help: "ACTIVE shards skipped by the most recent pass because their durable Authority has expired.",
		}),
		minRemainingMs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "route_snapshot_min_remaining_ms",
			Help: "Minimum ACTIVE route deadline remaining in the Manager's captured complete Snapshot.",
		}),
		projectionLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "route_deadline_projection_lag_ms",
			Help: "Maximum observed ACTIVE Authority deadline lead over its projected Snapshot deadline.",
		}),
	}
	registerer.MustRegister(metrics.passes, metrics.publishes, metrics.snapshotPublishes, metrics.snapshotBytes, metrics.noops, metrics.changedShards, metrics.casConflicts, metrics.readErrors, metrics.stalledShards, metrics.minRemainingMs, metrics.projectionLag)
	return metrics
}

func (m *routeDeadlineMetrics) observe(result authority.DeadlineProjectionResult, err error) {
	if m == nil {
		return
	}
	m.minRemainingMs.Set(float64(result.MinRemainingMs))
	m.projectionLag.Set(float64(result.ProjectionLagMs))
	m.stalledShards.Set(float64(result.StalledShards))
	if err != nil {
		if result.Published {
			m.publishes.Inc()
			m.snapshotPublishes.WithLabelValues("deadline_only").Inc()
			m.snapshotBytes.Add(float64(result.SnapshotBytes))
			m.changedShards.Set(float64(result.ChangedShards))
		}
		label := result.FailureStage
		if label == "" {
			label = "error"
		}
		m.passes.WithLabelValues(label).Inc()
		if result.FailureStage == "read" {
			m.readErrors.Inc()
		}
		if errors.Is(err, core.ErrSnapshotCASConflict) {
			m.casConflicts.Inc()
		}
		return
	}
	if result.NotificationReplayed {
		m.passes.WithLabelValues("notification_replayed").Inc()
		return
	}
	if result.Changed {
		if result.Published {
			m.passes.WithLabelValues("published").Inc()
			m.publishes.Inc()
			m.snapshotPublishes.WithLabelValues("deadline_only").Inc()
			m.snapshotBytes.Add(float64(result.SnapshotBytes))
		} else {
			m.passes.WithLabelValues("lease_observed").Inc()
		}
		m.changedShards.Set(float64(result.ChangedShards))
		return
	}
	m.passes.WithLabelValues("noop").Inc()
	m.noops.Inc()
	m.changedShards.Set(0)
}
