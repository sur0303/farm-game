package idle

import (
	"github.com/prometheus/client_golang/prometheus"
)

// StatsSource supplies sweeper snapshots for GaugeFunc/CounterFunc.
type StatsSource func() Stats

// RegisterMetrics registers farm_actor_idle_sweeper_* gauges/counters.
// blockedReasons 列出需要暴露为 label 的 blocker 原因(如 turn_running);
// 未知原因仍会累计进 Stats.BlockedTotal,但不单独注册以避免无界 label。
func RegisterMetrics(reg prometheus.Registerer, kind string, source StatsSource, blockedReasons []string) {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	labels := prometheus.Labels{"actor_kind": kind}
	reg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "actor", Name: "idle_sweeper_tracked_keys",
			Help: "Keys currently tracked by the idle sweeper.", ConstLabels: labels,
		}, func() float64 { return float64(source().TrackedKeys) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "actor", Name: "idle_sweeper_buckets",
			Help: "Non-empty time buckets in the idle sweeper.", ConstLabels: labels,
		}, func() float64 { return float64(source().Buckets) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor", Name: "idle_sweeper_sweeps_total",
			Help: "Idle sweeper ticks executed.", ConstLabels: labels,
		}, func() float64 { return float64(source().SweepsTotal) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor", Name: "idle_sweeper_candidates_total",
			Help: "Idle candidates re-validated by the retire callback.", ConstLabels: labels,
		}, func() float64 { return float64(source().CandidatesTotal) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor", Name: "idle_sweeper_retired_total",
			Help: "Candidates fully released by the retire callback.", ConstLabels: labels,
		}, func() float64 { return float64(source().RetiredTotal) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor", Name: "idle_sweeper_not_idle_total",
			Help: "Candidates that showed fresh activity during revalidation.", ConstLabels: labels,
		}, func() float64 { return float64(source().NotIdleTotal) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "farm", Subsystem: "actor", Name: "idle_sweeper_lag_seconds",
			Help: "How far the oldest unprocessed expired bucket lags behind, in seconds.", ConstLabels: labels,
		}, func() float64 { return source().LastLagSeconds }),
	)
	for _, reason := range blockedReasons {
		reasonLabels := prometheus.Labels{"actor_kind": kind, "reason": reason}
		reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "actor", Name: "idle_sweeper_blocked_total",
			Help: "Candidates kept alive by a passivation blocker, by reason.", ConstLabels: reasonLabels,
		}, func() float64 { return float64(source().BlockedTotal[reason]) }))
	}
}
