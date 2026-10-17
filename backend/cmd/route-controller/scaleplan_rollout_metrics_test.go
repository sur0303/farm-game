package main

import (
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/scaleplan"
	"farmgame/internal/scaleplan/autorebalance"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestScalePlanRolloutMetricsExposePhaseBlockAndDeletion(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newScalePlanRolloutMetrics(registry)
	result := autorebalance.Result{
		Workload: core.WorkloadRef{Domain: actorrouting.DomainFarm},
		Action:   autorebalance.ActionRolloutBlocked, RolloutID: "rollout-a",
		RolloutPhase: autorebalance.RolloutPhaseBlocked, RolloutStartedAtMs: time.Now().Add(-time.Second).UnixMilli(),
		RolloutVictimOrdinal: 1, RolloutBlockedReason: "TARGET_REVISION_CHANGED", RolloutJustBlocked: true,
		OldRevisionPods: 2, TargetReadyPods: 0, PodDeleted: true, DrainPlanSubmitted: true,
	}
	metrics.observe(result)
	if got := testutil.ToFloat64(metrics.active.WithLabelValues("FARM")); got != 1 {
		t.Fatalf("active=%v", got)
	}
	if got := testutil.ToFloat64(metrics.phase.WithLabelValues("FARM", string(autorebalance.RolloutPhaseBlocked))); got != 1 {
		t.Fatalf("blocked phase=%v", got)
	}
	if got := testutil.ToFloat64(metrics.blocked.WithLabelValues("FARM", "TARGET_REVISION_CHANGED")); got != 1 {
		t.Fatalf("blocked total=%v", got)
	}
	if got := testutil.ToFloat64(metrics.podDeletes.WithLabelValues("FARM")); got != 1 {
		t.Fatalf("pod deletes=%v", got)
	}
	if got := testutil.ToFloat64(metrics.drainPlans.WithLabelValues("FARM")); got != 1 {
		t.Fatalf("drain plans=%v", got)
	}

	result.Action = autorebalance.ActionRolloutComplete
	result.RolloutPhase = autorebalance.RolloutPhaseContracting
	result.RolloutJustBlocked, result.PodDeleted, result.DrainPlanSubmitted = false, false, false
	metrics.observe(result)
	if got := testutil.ToFloat64(metrics.active.WithLabelValues("FARM")); got != 0 {
		t.Fatalf("active after completion=%v", got)
	}
}
