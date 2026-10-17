package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func tierPlan(phase string, tier int, metadata map[string]string) runPlan {
	return runPlan{
		Service: "gate", Profile: "login_ticket_wss_hello", Phase: phase, Tier: tier,
		SampleIntervalMS: 1000, Targets: []target{{Kind: "connections", Value: 1000}},
		BindingPath: filepath.Clean(filepath.Join("..", "..", "deploy", "load", "cap-single-pod-r1-binding.json")),
		Metadata:    metadata,
	}
}

func staircaseResult(tier int, connections, p99MS float64) capacityResultDocument {
	return capacityResultDocument{
		BindingID: "R1", RunID: "capsingle-l2-r1-20260810T000000Z-3", Service: "gate", Profile: "login_ticket_wss_hello",
		Phase: "STAIRCASE", Tier: tier, Duration: 720, Targets: []target{{Kind: "connections", Value: connections}},
		PhaseStatus: PhaseStatusPass, PhaseDecision: "PASS_STAIRCASE_TIER",
		Actual: plannedActual{ActualToPlannedRatio: 1, LatencyMS: map[string]float64{"p99": p99MS}},
	}
}

func writeTierEvidence(t *testing.T, directory string, result capacityResultDocument) string {
	t.Helper()
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	entries := make([]map[string]any, 0, len(requiredEvidenceFiles))
	for _, name := range requiredEvidenceFiles {
		raw := []byte("{}\n")
		if name == "service-capacity-result.json" {
			encoded, err := json.Marshal(result)
			if err != nil {
				t.Fatal(err)
			}
			raw = encoded
		}
		if err := os.WriteFile(filepath.Join(directory, name), raw, 0o600); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, map[string]any{"path": name, "bytes": len(raw), "sha256": sha256Bytes(raw)})
	}
	index := map[string]any{"schema_version": "cap-single-pod-l2.evidence-index.v0.1", "complete": true, "files": entries}
	if err := writeJSONExclusive(filepath.Join(directory, "evidence-hash-index.json"), index); err != nil {
		t.Fatal(err)
	}
	return directory
}

func previousTierPlan(t *testing.T, result capacityResultDocument) runPlan {
	t.Helper()
	directory := writeTierEvidence(t, filepath.Join(t.TempDir(), "previous-tier"), result)
	return tierPlan("STAIRCASE", 4, map[string]string{
		"tier_baseline":               tierBaselinePreviousEvidence,
		"previous_tier_evidence_path": directory,
	})
}

func TestTheRatioIsMeasuredAgainstThePreviousTier(t *testing.T) {
	plan := previousTierPlan(t, staircaseResult(3, 500, 20))
	baseline, err := resolveTierBaseline(plan)
	if err != nil {
		t.Fatal(err)
	}
	if baseline.PreviousP99MS != 20 || baseline.PreviousTier != 3 || baseline.FirstTier {
		t.Fatalf("baseline=%+v", baseline)
	}
	for _, test := range []struct {
		name            string
		tierP99MS, want float64
	}{
		{"faster than the tier below", 10, .5},
		{"level with the tier below", 20, 1},
		{"twice the tier below", 40, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			judge := newTierJudge(plan, baseline)
			got := judge.observe(resourceSnapshot{}, plannedActual{LatencyMS: map[string]float64{"p99": test.tierP99MS}}, 0, 0)
			if got["p99_non_linear_degradation_vs_previous_tier"] != test.want {
				t.Fatalf("ratio=%v want %v", got["p99_non_linear_degradation_vs_previous_tier"], test.want)
			}
		})
	}
}

// The first tier is the baseline the tiers above it are read against, so it
// reports the identity ratio. Nothing else can be honest here: there is no
// earlier operating point, and an absent metric would make the first rung of
// every staircase unrunnable.
func TestTheFirstTierIsItsOwnBaseline(t *testing.T) {
	plan := tierPlan("STAIRCASE", 1, map[string]string{"tier_baseline": tierBaselineFirstTier})
	baseline, err := resolveTierBaseline(plan)
	if err != nil || !baseline.FirstTier || baseline.PreviousP99MS != 0 {
		t.Fatalf("baseline=%+v err=%v", baseline, err)
	}
	judge := newTierJudge(plan, baseline)
	got := judge.observe(resourceSnapshot{}, plannedActual{LatencyMS: map[string]float64{"p99": 900}}, 0, 0)
	if got["p99_non_linear_degradation_vs_previous_tier"] != 1 {
		t.Fatalf("ratio=%v", got["p99_non_linear_degradation_vs_previous_tier"])
	}
}

func TestAStaircaseTierWithoutADeclaredBaselineRefusesToRun(t *testing.T) {
	for _, test := range []struct {
		name     string
		metadata map[string]string
	}{
		{"nothing declared", nil},
		{"unknown declaration", map[string]string{"tier_baseline": "PROBABLY_THE_FIRST_ONE"}},
		{"previous tier declared with no evidence", map[string]string{"tier_baseline": tierBaselinePreviousEvidence}},
		{"first tier declared with evidence", map[string]string{"tier_baseline": tierBaselineFirstTier, "previous_tier_evidence_path": "/nonexistent"}},
		{"relative evidence path", map[string]string{"tier_baseline": tierBaselinePreviousEvidence, "previous_tier_evidence_path": "previous"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := resolveTierBaseline(tierPlan("STAIRCASE", 5, test.metadata)); err == nil {
				t.Fatal("a tier that cannot be compared with anything was allowed to run")
			}
		})
	}
}

// A phase that is not a rung of the staircase has no tier below it by
// construction, so it needs no declaration and may not carry one.
func TestOnlyStaircaseRungsDeclareATierBaseline(t *testing.T) {
	baseline, err := resolveTierBaseline(tierPlan("SMOKE", 0, nil))
	if err != nil || !baseline.FirstTier {
		t.Fatalf("baseline=%+v err=%v", baseline, err)
	}
	if _, err := resolveTierBaseline(tierPlan("SMOKE", 0, map[string]string{"tier_baseline": tierBaselineFirstTier})); err == nil {
		t.Fatal("a phase with no tier accepted a tier baseline")
	}
}

func TestPreviousTierEvidenceIsVerifiedBeforeItIsTrusted(t *testing.T) {
	for _, test := range []struct {
		name   string
		result capacityResultDocument
		mutate func(t *testing.T, directory string)
	}{
		{name: "tier ordinal is not below this tier", result: staircaseResult(4, 500, 20)},
		{name: "tier below carries more load", result: staircaseResult(3, 2000, 20)},
		{name: "tier below measured no latency", result: staircaseResult(3, 500, 0)},
		{
			name: "tier below did not pass",
			result: func() capacityResultDocument {
				result := staircaseResult(3, 500, 20)
				result.PhaseStatus = PhaseStatusFail
				result.Verdict = VerdictFailTargetSaturation
				return result
			}(),
		},
		{
			name: "another service",
			result: func() capacityResultDocument {
				result := staircaseResult(3, 500, 20)
				result.Service = "farm"
				return result
			}(),
		},
		{
			name:   "evidence changed after it was indexed",
			result: staircaseResult(3, 500, 20),
			mutate: func(t *testing.T, directory string) {
				result := staircaseResult(3, 500, 5)
				raw, err := json.Marshal(result)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(directory, "service-capacity-result.json"), raw, 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := previousTierPlan(t, test.result)
			if test.mutate != nil {
				test.mutate(t, plan.Metadata["previous_tier_evidence_path"])
			}
			if _, err := resolveTierBaseline(plan); err == nil {
				t.Fatal("unverified previous tier accepted as a baseline")
			}
		})
	}
}

// A judgment the run cannot make is left out of the sample, and a sample that is
// short a required metric stops the run for incomplete observation. It is never
// reported as the value that would let the tier pass.
func TestAnUnjudgeableTierIsAbsentRatherThanPassing(t *testing.T) {
	judge := newTierJudge(tierPlan("STAIRCASE", 5, nil), tierBaseline{})
	sample := resourceSnapshot{MetricValues: map[string]float64{}}
	normalizeSnapshotMetrics(&sample, newRunAccumulator(), judge)
	for _, name := range []string{"p99_non_linear_degradation_vs_previous_tier", "p99_sustained_degradation", "p99_stability_observed_seconds"} {
		if value, found := sample.MetricValues[name]; found {
			t.Fatalf("%s reported as %v by a tier with no baseline", name, value)
		}
	}
	sample.MissingMetrics = missingMetricNames(sample.MetricValues, mandatoryRuntimeMetricNames())
	if got := newCollectionState().addSample(sample, 60, 5); got != VerdictInconclusiveObservability {
		t.Fatalf("stop=%s", got)
	}
}

func TestTheHarnessSuppliesTheTierJudgmentsNobodyElseCanMake(t *testing.T) {
	plan := tierPlan("STAIRCASE", 1, map[string]string{"tier_baseline": tierBaselineFirstTier})
	baseline, err := resolveTierBaseline(plan)
	if err != nil {
		t.Fatal(err)
	}
	sample := resourceSnapshot{MetricValues: map[string]float64{}}
	normalizeSnapshotMetrics(&sample, newRunAccumulator(), newTierJudge(plan, baseline))
	for _, name := range []string{"controlled_drain_pass", "p99_non_linear_degradation_vs_previous_tier", "p99_sustained_degradation", "p99_stability_observed_seconds"} {
		if _, found := sample.MetricValues[name]; !found {
			t.Fatalf("%s absent from a tier the harness can judge", name)
		}
	}
	if sample.MetricValues["controlled_drain_pass"] != 0 || sample.MetricValues["p99_sustained_degradation"] != 0 {
		t.Fatalf("values=%v", sample.MetricValues)
	}
}

func feedTier(judge *tierJudge, samples int, tierP99MS, intervalP99MS float64, intervalCount int64) map[string]float64 {
	var last map[string]float64
	for index := 0; index < samples; index++ {
		last = judge.observe(resourceSnapshot{}, plannedActual{LatencyMS: map[string]float64{"p99": tierP99MS}}, intervalP99MS, intervalCount)
	}
	return last
}

func TestATierThatStaysDegradedIsCaughtAndOneThatRecoversIsNot(t *testing.T) {
	plan := tierPlan("STAIRCASE", 1, map[string]string{"tier_baseline": tierBaselineFirstTier})
	baseline, err := resolveTierBaseline(plan)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name            string
		degradedSamples int
		want            float64
	}{
		{"a spike one second short of the sustain window", p99SustainedDegradationSeconds - 1, 0},
		{"a tier that stayed degraded for the whole window", p99SustainedDegradationSeconds, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			judge := newTierJudge(plan, baseline)
			feedTier(judge, p99ReferenceHoldSeconds, 10, 10, 20)
			feedTier(judge, test.degradedSamples, 10, 10*p99NonLinearDegradationFactor, 20)
			got := feedTier(judge, 30, 10, 10, 20)
			if got["p99_sustained_degradation"] != test.want {
				t.Fatalf("sustained=%v want %v", got["p99_sustained_degradation"], test.want)
			}
			if got["p99_stability_observed_seconds"] != float64(test.degradedSamples+30) {
				t.Fatalf("observed=%v", got["p99_stability_observed_seconds"])
			}
		})
	}
}

// Seconds in which nothing completed are silence. They are not counted as
// stability observed, so a tier that produced no latency to judge cannot reach
// the observation the safe-tier gate demands.
func TestSecondsWithNoCompletionAreNotStabilityObserved(t *testing.T) {
	plan := tierPlan("STAIRCASE", 1, map[string]string{"tier_baseline": tierBaselineFirstTier})
	baseline, _ := resolveTierBaseline(plan)
	judge := newTierJudge(plan, baseline)
	got := feedTier(judge, p99ReferenceHoldSeconds+p99SustainedDegradationSeconds, 0, 0, 0)
	if got["p99_stability_observed_seconds"] != 0 || got["p99_sustained_degradation"] != 0 {
		t.Fatalf("values=%v", got)
	}
	if safeTierObservationComplete(map[string]float64{
		"controlled_drain_pass_max": 1, "p99_sustained_degradation_max": 0, "p99_stability_observed_seconds_max": 0,
		"measured_service_oom_kill_total_max": 0, "measured_service_fd_percent_of_limit_max": 20,
	}) {
		t.Fatal("a tier that watched nothing was accepted as stable")
	}
}

func TestControlledDrainIsOnlyReportedOnceItHasBeenSeen(t *testing.T) {
	plan := tierPlan("STAIRCASE", 1, map[string]string{"tier_baseline": tierBaselineFirstTier})
	baseline, _ := resolveTierBaseline(plan)
	judge := newTierJudge(plan, baseline)
	underLoad := judge.observe(resourceSnapshot{PrimaryQueueItems: 4}, plannedActual{Planned: 100, Launched: 40}, 0, 0)
	if underLoad["controlled_drain_pass"] != 0 {
		t.Fatal("a drain was reported while the load was still arriving")
	}
	judge.arrivalsCompleted()
	stillDraining := judge.observe(resourceSnapshot{PrimaryQueueItems: 90}, plannedActual{Planned: 100, Launched: 100}, 0, 0)
	if stillDraining["controlled_drain_pass"] != 0 || judge.drainDecided() {
		t.Fatal("a queue above the level the run started at was called drained")
	}
	inflight := judge.observe(resourceSnapshot{PrimaryQueueItems: 4}, plannedActual{Planned: 100, Launched: 90}, 0, 0)
	if inflight["controlled_drain_pass"] != 0 {
		t.Fatal("a drain was reported with operations still in flight")
	}
	drained := judge.observe(resourceSnapshot{PrimaryQueueItems: 2}, plannedActual{Planned: 100, Launched: 100}, 0, 0)
	if drained["controlled_drain_pass"] != 1 || !judge.drainDecided() {
		t.Fatalf("an observed drain was not reported: %v", drained)
	}
}

func TestADrainNobodyWatchedIsNotAPass(t *testing.T) {
	plan := tierPlan("STAIRCASE", 1, map[string]string{"tier_baseline": tierBaselineFirstTier})
	baseline, _ := resolveTierBaseline(plan)
	for _, test := range []struct {
		name          string
		arrivalsEnded bool
		queueItems    float64
	}{
		{"the tier was cut short before its load ended", false, 0},
		{"the queue never came back inside the drain the profile allows", true, 50},
	} {
		t.Run(test.name, func(t *testing.T) {
			judge := newTierJudge(plan, baseline)
			judge.observe(resourceSnapshot{}, plannedActual{}, 0, 0)
			if test.arrivalsEnded {
				judge.arrivalsCompleted()
			}
			var last map[string]float64
			for index := 0; index <= controlledDrainMaxSeconds; index++ {
				last = judge.observe(resourceSnapshot{PrimaryQueueItems: test.queueItems}, plannedActual{Planned: 10, Launched: 10}, 0, 0)
			}
			if last["controlled_drain_pass"] != 0 {
				t.Fatal("a drain the run never saw was reported as a pass")
			}
			if test.arrivalsEnded && !judge.drainDecided() {
				t.Fatal("the drain window never closed")
			}
		})
	}
}
