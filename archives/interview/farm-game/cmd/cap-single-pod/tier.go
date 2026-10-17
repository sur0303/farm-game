package main

// The three judgments in this file are the harness's own. Whether a tier ended in
// a controlled drain, whether its p99 stayed degraded, and how its p99 compares
// with the tier below it are statements about a tier, not readings a service or a
// cgroup can hand over: only the process that holds the tier plan, the latency
// histogram and the arrival lifecycle can make them.
//
// None of them may be assumed. A tier whose baseline was never declared cannot be
// compared with anything, so both p99 metrics are left out of the sample and the
// run stops for incomplete observation. A drain nobody watched is reported as no
// drain. The one judgment that is true by construction, the first tier being its
// own baseline, is only reachable when the plan says so in as many words.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	// The p99 rule frozen in stop-conditions.json: a tier is non-linearly degraded
	// at twice the tier below it, and that has to hold for 120 seconds.
	p99NonLinearDegradationFactor  = 2.0
	p99SustainedDegradationSeconds = 120
	// p99ReferenceHoldSeconds is how much of its own hold a tier spends
	// establishing the p99 that its later seconds are judged against. It is the
	// same 60-second window every other sustained rule in the policy uses.
	p99ReferenceHoldSeconds = 60
	// controlledDrainMaxSeconds is durations_seconds.controlled_drain_max in the
	// frozen profile, validated against the profile in validateDrainPolicy.
	controlledDrainMaxSeconds = 300

	tierBaselineFirstTier        = "FIRST_TIER"
	tierBaselinePreviousEvidence = "PREVIOUS_TIER_EVIDENCE"
)

// tierBaseline is what a tier's p99 is judged against. It is resolved once,
// before the run starts, so a tier that cannot be judged never generates load.
type tierBaseline struct {
	Declared       bool
	FirstTier      bool
	PreviousP99MS  float64
	PreviousRunID  string
	PreviousTier   int
	EvidenceSHA256 string
}

// phaseHasPrecedingTier reports whether a phase is a rung of the staircase. Only
// those phases can have a tier below them, and only they are read as evidence
// that a load level is safe.
func phaseHasPrecedingTier(phase string) bool {
	return phase == "STAIRCASE" || isCandidateOrFormal(phase)
}

// resolveTierBaseline reads the tier baseline the plan declares. A rung of the
// staircase must say which: an omitted declaration is an error rather than a
// first tier, because silently treating tier six as a baseline would retire the
// only stop condition that compares tiers.
func resolveTierBaseline(plan runPlan) (tierBaseline, error) {
	declaration := strings.TrimSpace(plan.Metadata["tier_baseline"])
	directory := strings.TrimSpace(plan.Metadata["previous_tier_evidence_path"])
	if !phaseHasPrecedingTier(plan.Phase) {
		if declaration != "" || directory != "" {
			return tierBaseline{}, fmt.Errorf("phase runs no staircase tier: %s", plan.Phase)
		}
		return tierBaseline{Declared: true, FirstTier: true}, nil
	}
	switch declaration {
	case tierBaselineFirstTier:
		if directory != "" {
			return tierBaseline{}, errors.New("first tier declared alongside previous tier evidence")
		}
		return tierBaseline{Declared: true, FirstTier: true}, nil
	case tierBaselinePreviousEvidence:
		return previousTierBaseline(plan, directory)
	default:
		return tierBaseline{}, fmt.Errorf("tier baseline undeclared: metadata.tier_baseline must be %s or %s", tierBaselineFirstTier, tierBaselinePreviousEvidence)
	}
}

// previousTierBaseline takes the p99 of the tier below this one from that tier's
// own evidence, hashes included. A number typed into a plan would be a claim; a
// verified evidence directory is a measurement.
func previousTierBaseline(plan runPlan, directory string) (tierBaseline, error) {
	if !filepath.IsAbs(directory) {
		return tierBaseline{}, errors.New("previous tier evidence path must be absolute")
	}
	if err := verifyRunEvidenceIndex(directory); err != nil {
		return tierBaseline{}, errors.New("previous tier evidence incomplete")
	}
	indexSHA, err := exactFileSHA256(filepath.Join(directory, "evidence-hash-index.json"))
	if err != nil {
		return tierBaseline{}, errors.New("previous tier evidence incomplete")
	}
	raw, err := os.ReadFile(filepath.Join(directory, "service-capacity-result.json"))
	if err != nil {
		return tierBaseline{}, errors.New("previous tier result unreadable")
	}
	var result capacityResultDocument
	if json.Unmarshal(raw, &result) != nil {
		return tierBaseline{}, errors.New("previous tier result JSON")
	}
	binding, err := loadBinding(plan.BindingPath)
	if err != nil {
		return tierBaseline{}, err
	}
	if result.BindingID != binding.BindingID || result.Service != plan.Service || result.Profile != plan.Profile {
		return tierBaseline{}, errors.New("previous tier identity mismatch")
	}
	if result.Phase != "STAIRCASE" || result.PhaseStatus != PhaseStatusPass || result.Verdict != "" || result.SafeClaimed {
		return tierBaseline{}, errors.New("previous tier is not a passed staircase tier")
	}
	if result.Tier >= plan.Tier {
		return tierBaseline{}, errors.New("previous tier ordinal is not below this tier")
	}
	if err := requireLowerLoad(result.Targets, plan.Targets); err != nil {
		return tierBaseline{}, err
	}
	p99 := result.Actual.LatencyMS["p99"]
	if p99 <= 0 {
		return tierBaseline{}, errors.New("previous tier measured no p99 latency")
	}
	return tierBaseline{Declared: true, PreviousP99MS: p99, PreviousRunID: result.RunID, PreviousTier: result.Tier, EvidenceSHA256: indexSHA}, nil
}

// requireLowerLoad holds the staircase to its shape. A tier that does not carry
// more load than the one below it in at least one dimension, and less in none,
// is not the tier below: comparing p99 against it would compare two unrelated
// operating points.
func requireLowerLoad(previous, current []target) error {
	previousByKind := make(map[string]float64, len(previous))
	for _, item := range previous {
		previousByKind[item.Kind] = item.Value
	}
	lower := false
	for _, item := range current {
		value, found := previousByKind[item.Kind]
		if !found {
			return fmt.Errorf("previous tier carries no %s target", item.Kind)
		}
		if value > item.Value {
			return errors.New("previous tier carries more load than this tier")
		}
		if value < item.Value {
			lower = true
		}
	}
	if len(previousByKind) != len(current) || !lower {
		return errors.New("previous tier is not a lower load level")
	}
	return nil
}

// tierJudge turns what a run observes into the tier-relative metrics. It is fed
// one sample per second and answers only for judgments the run has earned.
type tierJudge struct {
	baseline         tierBaseline
	referenceSamples int
	sustainSamples   int
	drainSamplesMax  int

	mu                   sync.Mutex
	samples              int
	referenceP99MS       float64
	degradedSamples      int
	judgedSamples        int
	sustainedDegradation bool
	arrivalsEnded        bool
	drainSamples         int
	drainPassed          bool
	queueBeforeLoad      float64
	queueObserved        bool
}

func newTierJudge(plan runPlan, baseline tierBaseline) *tierJudge {
	interval := plan.SampleIntervalMS
	if interval <= 0 {
		interval = 1000
	}
	samplesFor := func(seconds int) int { return (seconds*1000 + interval - 1) / interval }
	return &tierJudge{
		baseline:         baseline,
		referenceSamples: samplesFor(plan.RampSeconds + p99ReferenceHoldSeconds),
		sustainSamples:   samplesFor(p99SustainedDegradationSeconds),
		drainSamplesMax:  samplesFor(controlledDrainMaxSeconds),
	}
}

// observe records one sample of evidence and returns the metrics the run is
// entitled to report for it. A judgment the run cannot make is left out, which
// the collector records as a missing metric and fails the run closed.
func (j *tierJudge) observe(sample resourceSnapshot, actual plannedActual, intervalP99MS float64, intervalCount int64) map[string]float64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.samples++
	if !j.queueObserved && !j.arrivalsEnded {
		j.queueBeforeLoad = sample.PrimaryQueueItems
		j.queueObserved = true
	}
	values := map[string]float64{"controlled_drain_pass": j.drainLocked(sample, actual)}
	if !j.baseline.Declared {
		return values
	}
	j.watchStabilityLocked(actual.LatencyMS["p99"], intervalP99MS, intervalCount)
	values["p99_non_linear_degradation_vs_previous_tier"] = j.degradationRatioLocked(actual)
	values["p99_sustained_degradation"] = boolMetric(j.sustainedDegradation)
	values["p99_stability_observed_seconds"] = float64(j.judgedSamples)
	return values
}

// degradationRatioLocked is this tier's p99 over the p99 of the tier below it.
func (j *tierJudge) degradationRatioLocked(actual plannedActual) float64 {
	if j.baseline.FirstTier {
		// The first tier is the baseline the tiers above it are read against, so it
		// stands at one times itself. There is no earlier operating point it could
		// have degraded from, and every other stop condition still guards it.
		return 1
	}
	return actual.LatencyMS["p99"] / j.baseline.PreviousP99MS
}

// watchStabilityLocked judges the tier against itself. The reference is the p99
// the tier showed once its ramp and first stable minute were behind it; a second
// that is twice that is degraded, and 120 degraded seconds in a row is a tier
// that stayed degraded. Judging within the tier is what lets the first tier be
// judged at all, and it sees a late deterioration that the cumulative ratio
// against the tier below is far too slow to show.
func (j *tierJudge) watchStabilityLocked(tierP99MS, intervalP99MS float64, intervalCount int64) {
	if j.referenceP99MS == 0 {
		if j.samples >= j.referenceSamples && tierP99MS > 0 {
			j.referenceP99MS = tierP99MS
		}
		return
	}
	if intervalCount == 0 {
		// A second in which nothing completed is silence, not recovery: it neither
		// clears a degraded stretch nor counts as a second of stability observed.
		return
	}
	j.judgedSamples++
	if intervalP99MS >= p99NonLinearDegradationFactor*j.referenceP99MS {
		j.degradedSamples++
		if j.degradedSamples >= j.sustainSamples {
			j.sustainedDegradation = true
		}
		return
	}
	j.degradedSamples = 0
}

// drainLocked reports whether the tier has been seen to end in a controlled
// drain. It is zero until the load has stopped, every operation the plan sent
// has returned and the primary queue is back to the level the run started from,
// all inside the drain the profile allows. A tier cut short by a stop condition
// never reaches arrivalsCompleted and so never reports a drain.
func (j *tierJudge) drainLocked(sample resourceSnapshot, actual plannedActual) float64 {
	if j.drainPassed {
		return 1
	}
	// A run that never saw the queue before its load arrived has no level to
	// drain back to, so it cannot be seen to have drained.
	if !j.arrivalsEnded || !j.queueObserved {
		return 0
	}
	j.drainSamples++
	if j.drainSamples > j.drainSamplesMax {
		return 0
	}
	inflight := actual.Planned - actual.ScheduledUnsent - actual.Launched
	if inflight > 0 || sample.PrimaryQueueItems > j.queueBeforeLoad {
		return 0
	}
	j.drainPassed = true
	return 1
}

// arrivalsCompleted opens the drain window. Only a run that reached the end of
// its planned load calls it: a tier stopped by saturation, by a correctness gate
// or by a signal did not end in a controlled drain, and a drain reported for one
// would be a drain nobody watched.
func (j *tierJudge) arrivalsCompleted() {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.arrivalsEnded = true
}

// drainDecided reports whether the drain window still has anything to say.
func (j *tierJudge) drainDecided() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.drainPassed || j.drainSamples >= j.drainSamplesMax
}

// summaryMetrics records what the run itself established, as opposed to what the
// plan declared, so a reviewer can see how much evidence each judgment rests on.
func (j *tierJudge) summaryMetrics() map[string]float64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return map[string]float64{
		"tier_p99_reference_ms":             j.referenceP99MS,
		"controlled_drain_observed_seconds": float64(j.drainSamples),
	}
}

// evidence is the baseline as it goes into the capacity result, so the tier a
// number was judged against can be followed back to the run that measured it.
func (baseline tierBaseline) evidence() map[string]any {
	return map[string]any{
		"declared": baseline.Declared, "first_tier": baseline.FirstTier,
		"previous_tier": baseline.PreviousTier, "previous_run_id": baseline.PreviousRunID,
		"previous_p99_ms": baseline.PreviousP99MS, "previous_evidence_sha256": baseline.EvidenceSHA256,
	}
}

func boolMetric(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
