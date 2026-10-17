package plantrecovery

import (
	"farmgame/internal/workflow"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics contains only the bounded family label required for the WR2
// handoff. It intentionally does not label by workflow ID, operation ID, or
// arbitrary error text.
type Metrics struct {
	Claimed   *prometheus.CounterVec
	Completed *prometheus.CounterVec
	Retried   *prometheus.CounterVec
	LeaseLost *prometheus.CounterVec
}

func NewMetrics(registry *prometheus.Registry) *Metrics {
	metrics := &Metrics{
		Claimed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "workflow_recovery", Name: "claimed_total",
			Help: "Claimed workflow recovery records by bounded work family.",
		}, []string{"family"}),
		Completed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "workflow_recovery", Name: "completed_total",
			Help: "Completed workflow recovery records by bounded work family.",
		}, []string{"family"}),
		Retried: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "workflow_recovery", Name: "retried_total",
			Help: "Retried workflow recovery records by bounded work family.",
		}, []string{"family"}),
		LeaseLost: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "workflow_recovery", Name: "lease_lost_total",
			Help: "Lease-lost workflow recovery records by bounded work family.",
		}, []string{"family"}),
	}
	if registry != nil {
		registry.MustRegister(metrics.Claimed, metrics.Completed, metrics.Retried, metrics.LeaseLost)
	}
	return metrics
}

func (m *Metrics) Observe(stats RoundStats) {
	if m == nil {
		return
	}
	m.observeFamily(FamilyPlant, stats.Plant)
	m.observeFamily(FamilyUnlockPlot, stats.Unlock)
}

func (m *Metrics) observeFamily(family string, stats workflow.RecoveryStats) {
	if m.Claimed == nil || m.Completed == nil || m.Retried == nil || m.LeaseLost == nil {
		return
	}
	m.Claimed.WithLabelValues(family).Add(float64(stats.Claimed))
	m.Completed.WithLabelValues(family).Add(float64(stats.Completed))
	m.Retried.WithLabelValues(family).Add(float64(stats.Retried))
	m.LeaseLost.WithLabelValues(family).Add(float64(stats.LeaseLost))
}
