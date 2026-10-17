package main

import (
	"time"

	"farmgame/internal/scaleplan/autorebalance"

	"github.com/prometheus/client_golang/prometheus"
)

type scalePlanRolloutMetrics struct {
	active      *prometheus.GaugeVec
	phase       *prometheus.GaugeVec
	duration    *prometheus.GaugeVec
	victim      *prometheus.GaugeVec
	oldPods     *prometheus.GaugeVec
	targetReady *prometheus.GaugeVec
	actions     *prometheus.CounterVec
	blocked     *prometheus.CounterVec
	podDeletes  *prometheus.CounterVec
	drainPlans  *prometheus.CounterVec
}

var rolloutMetricPhases = []autorebalance.RolloutPhase{
	autorebalance.RolloutPhaseExpanding,
	autorebalance.RolloutPhaseDraining,
	autorebalance.RolloutPhaseDeleting,
	autorebalance.RolloutPhaseWaitingReplacement,
	autorebalance.RolloutPhaseContracting,
	autorebalance.RolloutPhaseAborting,
	autorebalance.RolloutPhaseBlocked,
}

func newScalePlanRolloutMetrics(registerer prometheus.Registerer) *scalePlanRolloutMetrics {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	metrics := &scalePlanRolloutMetrics{
		active: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "farm_scaleplan_rollout_active", Help: "Whether a managed StatefulSet rollout is active by route domain.",
		}, []string{"domain"}),
		phase: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "farm_scaleplan_rollout_phase", Help: "Current managed rollout phase; exactly one phase is 1 for an active rollout.",
		}, []string{"domain", "phase"}),
		duration: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "farm_scaleplan_rollout_duration_seconds", Help: "Elapsed duration of the current managed rollout.",
		}, []string{"domain"}),
		victim: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "farm_scaleplan_rollout_current_ordinal", Help: "Current victim StatefulSet ordinal, or -1 when none is bound.",
		}, []string{"domain"}),
		oldPods: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "farm_scaleplan_rollout_old_revision_pods", Help: "Old-revision Pods remaining below the original replica count.",
		}, []string{"domain"}),
		targetReady: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "farm_scaleplan_rollout_target_revision_ready_pods", Help: "Ready target-revision Pods below the original replica count.",
		}, []string{"domain"}),
		actions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "farm_scaleplan_rollout_actions_total", Help: "Managed rollout reconciliation actions.",
		}, []string{"domain", "action"}),
		blocked: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "farm_scaleplan_rollout_blocked_total", Help: "Managed rollout transitions into BLOCKED by stable reason.",
		}, []string{"domain", "reason"}),
		podDeletes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "farm_scaleplan_rollout_pod_delete_total", Help: "UID-fenced old Pod deletions committed or observed committed.",
		}, []string{"domain"}),
		drainPlans: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "farm_scaleplan_rollout_drain_plan_total", Help: "Durable DRAIN_HOLDER plans submitted by managed rollout.",
		}, []string{"domain"}),
	}
	registerer.MustRegister(metrics.active, metrics.phase, metrics.duration, metrics.victim, metrics.oldPods,
		metrics.targetReady, metrics.actions, metrics.blocked, metrics.podDeletes, metrics.drainPlans)
	return metrics
}

func (m *scalePlanRolloutMetrics) observe(result autorebalance.Result) {
	if m == nil || result.RolloutID == "" {
		return
	}
	domain := string(result.Workload.Domain)
	if result.Action != autorebalance.ActionNone {
		m.actions.WithLabelValues(domain, string(result.Action)).Inc()
	}
	for _, phase := range rolloutMetricPhases {
		m.phase.WithLabelValues(domain, string(phase)).Set(0)
	}
	if result.RolloutPhase != "" {
		m.phase.WithLabelValues(domain, string(result.RolloutPhase)).Set(1)
	}
	active := 1.0
	if result.Action == autorebalance.ActionRolloutComplete || result.Action == autorebalance.ActionRolloutAborted {
		active = 0
	}
	m.active.WithLabelValues(domain).Set(active)
	if result.RolloutStartedAtMs > 0 {
		m.duration.WithLabelValues(domain).Set(time.Since(time.UnixMilli(result.RolloutStartedAtMs)).Seconds())
	}
	m.victim.WithLabelValues(domain).Set(float64(result.RolloutVictimOrdinal))
	m.oldPods.WithLabelValues(domain).Set(float64(result.OldRevisionPods))
	m.targetReady.WithLabelValues(domain).Set(float64(result.TargetReadyPods))
	if result.RolloutJustBlocked {
		reason := result.RolloutBlockedReason
		if reason == "" {
			reason = "UNKNOWN"
		}
		m.blocked.WithLabelValues(domain, reason).Inc()
	}
	if result.PodDeleted {
		m.podDeletes.WithLabelValues(domain).Inc()
	}
	if result.DrainPlanSubmitted {
		m.drainPlans.WithLabelValues(domain).Inc()
	}
}
