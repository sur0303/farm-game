package main

import "testing"

func healthySamples() []resourceSnapshot {
	out := make([]resourceSnapshot, 60)
	for index := range out {
		out[index] = resourceSnapshot{ServiceCPUPercent: 65, ServiceMemoryPercent: 50, DriverCPUPercent: 40, DependencyCPUPercent: 40, DependencyMemoryPercent: 40, PrimaryQueueItems: 10, MetricValues: map[string]float64{"controlled_drain_pass": 1, "p99_sustained_degradation": 0,
			"p99_stability_observed_seconds": p99SustainedDegradationSeconds, "p99_non_linear_degradation_vs_previous_tier": 1,
			"measured_service_oom_kill_total": 0, "measured_service_fd_percent_of_limit": 20,
		}}
	}
	return out
}
func healthyActual() plannedActual {
	return plannedActual{Planned: 1000, Launched: 1000, Completed: 1000, ActualToPlannedRatio: 1, ArrivalLagMS: map[string]float64{"p99": 10}}
}
func healthyPlan() runPlan { return runPlan{Phase: "SAFE_TIER_CANDIDATE"} }

func TestEvaluateCandidateDoesNotClaimPassSafeTier(t *testing.T) {
	got := evaluateRun(healthyPlan(), healthyActual(), nil, healthySamples(), nil, true, true, "")
	if got.Verdict != "" || got.PhaseStatus != PhaseStatusPass || got.PhaseDecision != "PASS_SAFE_TIER_CANDIDATE" {
		t.Fatalf("got=%+v", got)
	}
}

func TestSmokeWarmupQualificationAndFormalUsePhaseDecisions(t *testing.T) {
	for _, test := range []struct{ phase, decision string }{
		{"OBSERVABILITY_QUALIFICATION", "PASS_OBSERVABILITY_QUALIFICATION"}, {"LOAD_GENERATOR_QUALIFICATION", "PASS_LOAD_GENERATOR_QUALIFICATION"},
		{"SMOKE", "PASS_SMOKE"}, {"WARMUP", "PASS_WARMUP"}, {"FORMAL_1", "PASS_FORMAL_ROUND"},
	} {
		plan := healthyPlan()
		plan.Phase = test.phase
		got := evaluateRun(plan, healthyActual(), nil, healthySamples(), nil, true, true, "")
		if got.Verdict != "" || got.PhaseDecision != test.decision {
			t.Fatalf("phase=%s got=%+v", test.phase, got)
		}
	}
}
func TestLoadGeneratorQualificationRequiresFullSlopeObservation(t *testing.T) {
	plan := healthyPlan()
	plan.Phase = "LOAD_GENERATOR_QUALIFICATION"
	samples := healthySamples()[:minimumLoadGeneratorQualificationSamples-1]
	got := evaluateRun(plan, healthyActual(), nil, samples, nil, true, true, "")
	if got.Verdict != VerdictInconclusiveObservability || got.ObservabilityComplete {
		t.Fatalf("got=%+v", got)
	}
}

// A candidate tier passes on judgments the run made, not on judgments it left at
// their harmless value.
func TestSafeTierNeedsTheJudgmentsToHaveBeenMade(t *testing.T) {
	for _, test := range []struct {
		name   string
		metric string
		value  float64
		drop   bool
	}{
		{name: "p99 stability never watched", metric: "p99_stability_observed_seconds", value: p99SustainedDegradationSeconds - 1},
		{name: "p99 stability metric absent", metric: "p99_stability_observed_seconds", drop: true},
		{name: "drain never observed", metric: "controlled_drain_pass", value: 0},
		{name: "drain metric absent", metric: "controlled_drain_pass", drop: true},
		{name: "tier stayed degraded", metric: "p99_sustained_degradation", value: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			samples := healthySamples()
			for index := range samples {
				if test.drop {
					delete(samples[index].MetricValues, test.metric)
					continue
				}
				samples[index].MetricValues[test.metric] = test.value
			}
			got := evaluateRun(healthyPlan(), healthyActual(), nil, samples, nil, true, true, "")
			if got.Verdict != VerdictInconclusiveObservability || got.ObservabilityComplete {
				t.Fatalf("got=%+v", got)
			}
		})
	}
}

func TestEvaluateCorrectnessHasHighestPrecedence(t *testing.T) {
	s := healthySamples()
	s[0].MetricValues["identity_drift"] = 1
	got := evaluateRun(healthyPlan(), healthyActual(), map[string]int64{"duplicate_grant_total": 1}, s, nil, true, true, VerdictInconclusiveEnvironment)
	if got.Verdict != VerdictFailCorrectness {
		t.Fatalf("got=%s", got.Verdict)
	}
}
func TestEvaluateFailClosedClasses(t *testing.T) {
	tests := []struct {
		name, want, stop string
		actual           plannedActual
		samples          []resourceSnapshot
		complete         bool
	}{
		{"environment", VerdictInconclusiveEnvironment, VerdictInconclusiveEnvironment, healthyActual(), healthySamples(), true},
		{"observability", VerdictInconclusiveObservability, VerdictInconclusiveObservability, healthyActual(), healthySamples(), false},
		{"driver", VerdictInconclusiveDriver, VerdictInconclusiveDriver, healthyActual(), healthySamples(), true},
		{"dependency", VerdictInconclusiveDependency, VerdictInconclusiveDependency, healthyActual(), healthySamples(), true},
		{"target", VerdictFailTargetSaturation, VerdictFailTargetSaturation, healthyActual(), healthySamples(), true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := evaluateRun(healthyPlan(), test.actual, nil, test.samples, nil, true, test.complete, test.stop)
			if got.Verdict != test.want {
				t.Fatalf("got=%s", got.Verdict)
			}
		})
	}
}
func TestQueueSlopeRequiresConfidenceAndNetGrowth(t *testing.T) {
	s := healthySamples()
	for i := range s {
		s[i].PrimaryQueueItems = float64(i)
	}
	got := queueSlope(s)
	if !got.Positive || got.Lower95 <= 0 || got.NetGrowth < 5 {
		t.Fatalf("got=%+v", got)
	}
	for i := range s {
		s[i].PrimaryQueueItems = 10
	}
	if got := queueSlope(s); got.Positive {
		t.Fatalf("stable slope=%+v", got)
	}
}
func TestDriverRatioAndArrivalLag(t *testing.T) {
	a := healthyActual()
	a.Launched = 940
	a.ActualToPlannedRatio = .94
	if got := evaluateRun(healthyPlan(), a, nil, healthySamples(), nil, true, true, ""); got.Verdict != VerdictInconclusiveDriver {
		t.Fatalf("ratio got=%s", got.Verdict)
	}
	a = healthyActual()
	a.ArrivalLagMS["p99"] = 51
	if got := evaluateRun(healthyPlan(), a, nil, healthySamples(), nil, true, true, ""); got.Verdict != VerdictInconclusiveDriver {
		t.Fatalf("lag got=%s", got.Verdict)
	}
}
