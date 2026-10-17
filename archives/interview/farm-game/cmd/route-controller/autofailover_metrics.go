package main

import (
	"farmgame/internal/routeplan/autofailover"

	"github.com/prometheus/client_golang/prometheus"
)

type autoFailoverMetrics struct {
	passes         *prometheus.CounterVec
	plans          prometheus.Counter
	authorityReads prometheus.Counter
	missing        prometheus.Gauge
	waitingLease   prometheus.Gauge
	noTarget       prometheus.Gauge
}

func newAutoFailoverMetrics(registerer prometheus.Registerer) *autoFailoverMetrics {
	if registerer == nil {
		return nil
	}
	metrics := &autoFailoverMetrics{
		passes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "route_holder_failover_pass_total",
			Help: "Automatic holder-failure reconciliation passes by result.",
		}, []string{"result"}),
		plans: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "route_holder_failover_plan_total",
			Help: "Crash-takeover RoutePlans completed by automatic failure recovery.",
		}),
		authorityReads: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "route_holder_failover_authority_read_total",
			Help: "Exact Authority reads made by automatic failure recovery.",
		}),
		missing: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "route_holder_failover_missing_shards",
			Help: "Active routes whose exact holder generation is absent from the verified Kubernetes view.",
		}),
		waitingLease: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "route_holder_failover_waiting_lease_shards",
			Help: "Missing-holder shards waiting for their durable writer lease to end.",
		}),
		noTarget: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "route_holder_failover_no_target_shards",
			Help: "Recoverable shards without an eligible verified target holder.",
		}),
	}
	registerer.MustRegister(metrics.passes, metrics.plans, metrics.authorityReads, metrics.missing, metrics.waitingLease, metrics.noTarget)
	return metrics
}

func (m *autoFailoverMetrics) observe(result autofailover.Result) {
	if m == nil {
		return
	}
	m.authorityReads.Add(float64(result.AuthorityReads))
	m.missing.Set(float64(result.MissingShards))
	m.waitingLease.Set(float64(result.WaitingLease))
	m.noTarget.Set(float64(result.NoTarget))
	status := "noop"
	if result.Err != nil {
		status = "error"
	} else if result.PlansCompleted > 0 {
		status = "recovered"
	} else if result.WaitingLease > 0 {
		status = "waiting_lease"
	} else if result.NoTarget > 0 {
		status = "no_target"
	}
	m.passes.WithLabelValues(status).Inc()
	if result.PlansCompleted > 0 {
		m.plans.Add(float64(result.PlansCompleted))
	}
}
