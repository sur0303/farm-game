package activation

import (
	"github.com/prometheus/client_golang/prometheus"
)

// StatsSource supplies activation control-plane snapshots for GaugeFunc/CounterFunc.
type StatsSource func() Stats

// RegisterActivationMetrics registers farm_actor_activation_* gauges/counters.
// Does not change runtime.RegisterMetrics; call separately from cmd/*-svr.
func RegisterActivationMetrics(reg prometheus.Registerer, kind string, source StatsSource) {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	labels := prometheus.Labels{"actor_kind": kind}
	reg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "actor", Name: "activation_tracked_keys",
			Help: "Tracked activation FSM entries.", ConstLabels: labels,
		}, func() float64 { return float64(source().TrackedKeys) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "actor", Name: "activation_concurrent_loads",
			Help: "In-flight actor loads holding a concurrency slot.", ConstLabels: labels,
		}, func() float64 { return float64(source().ConcurrentLoads) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "actor", Name: "activation_pending_loads",
			Help: "Pending load waiters against the concurrency hard gate.", ConstLabels: labels,
		}, func() float64 { return float64(source().PendingLoads) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor", Name: "activation_throttled_total",
			Help: "Activation / load admissions rejected by rate or concurrency gates.", ConstLabels: labels,
		}, func() float64 { return float64(source().ThrottledTotal) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor", Name: "activation_backoff_total",
			Help: "Load attempts rejected while in load-failure backoff.", ConstLabels: labels,
		}, func() float64 { return float64(source().BackoffTotal) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor", Name: "activation_panic_total",
			Help: "Actors forced ABSENT after panic supervision.", ConstLabels: labels,
		}, func() float64 { return float64(source().PanicTotal) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor", Name: "activation_load_fail_total",
			Help: "Failed actor loads that entered backoff.", ConstLabels: labels,
		}, func() float64 { return float64(source().LoadFailTotal) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor", Name: "activation_passivate_completed_total",
			Help: "Completed PASSIVATING to ABSENT passivations.", ConstLabels: labels,
		}, func() float64 { return float64(source().PassivateCompletedTotal) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor", Name: "activation_reactivated_total",
			Help: "Actor loads following a completed passivation (vs cold starts).", ConstLabels: labels,
		}, func() float64 { return float64(source().ReactivatedTotal) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor", Name: "activation_entry_forgotten_total",
			Help: "ABSENT activation entries removed after passivation released all resources.", ConstLabels: labels,
		}, func() float64 { return float64(source().EntryForgottenTotal) }),
	)

	registerState := func(state string, pick func(Stats) int) {
		stateLabels := prometheus.Labels{"actor_kind": kind, "state": state}
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "actor", Name: "activation_actors",
			Help: "Current actors in the given activation FSM state.", ConstLabels: stateLabels,
		}, func() float64 { return float64(pick(source())) }))
	}
	registerState("ABSENT", func(s Stats) int { return s.StateAbsent })
	registerState("LOADING", func(s Stats) int { return s.StateLoading })
	registerState("CLAIM_UNKNOWN", func(s Stats) int { return s.StateClaimUnknown })
	registerState("ACTIVE", func(s Stats) int { return s.StateActive })
	registerState("RESOLVING_UNKNOWN", func(s Stats) int { return s.StateResolvingUnknown })
	registerState("RECONCILE_REQUIRED", func(s Stats) int { return s.StateReconcileRequired })
	registerState("DRAINING", func(s Stats) int { return s.StateDraining })
	registerState("FENCED", func(s Stats) int { return s.StateFenced })
	registerState("QUIESCING", func(s Stats) int { return s.StateQuiescing })
	registerState("PASSIVATING", func(s Stats) int { return s.StatePassivating })
}
