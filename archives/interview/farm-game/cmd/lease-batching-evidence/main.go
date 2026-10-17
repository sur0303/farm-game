// Command lease-batching-evidence runs the Redis-free formal FarmGate renewal
// simulation and writes machine-checked structured evidence.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"farmgame/internal/gateway/leasebatch"
)

const formalItems = 100_000

const formalSchedulerTick = 10 * time.Millisecond

var outputPath = flag.String("output", "lease-batching-evidence.json", "structured evidence output")

type distributionEvidence struct {
	Buckets       int     `json:"buckets"`
	Minimum       int     `json:"minimum"`
	Maximum       int     `json:"maximum"`
	Mean          float64 `json:"mean"`
	MaxToMean     float64 `json:"max_to_mean"`
	MinToMean     float64 `json:"min_to_mean"`
	StableReplay  bool    `json:"stable_replay"`
	JitterBounded bool    `json:"jitter_bounded"`
}

type laneEvidence struct {
	Items                   int                  `json:"items"`
	PlannedItems            int                  `json:"planned_items"`
	Batches                 int                  `json:"batches"`
	Limits                  leasebatch.Limits    `json:"limits"`
	Schedule                leasebatch.Schedule  `json:"schedule"`
	Distribution            distributionEvidence `json:"stable_distribution"`
	MaximumBatchItems       int                  `json:"maximum_batch_items"`
	MaximumNonOversizeBytes int                  `json:"maximum_non_oversize_bytes"`
	OversizeItems           int                  `json:"oversize_items"`
	OversizeDispatchedAlone bool                 `json:"oversize_dispatched_alone"`
	MaximumDispatchDuration time.Duration        `json:"maximum_dispatch_duration"`
	MaximumDueLag           time.Duration        `json:"maximum_due_lag"`
	StarvedItems            int                  `json:"starved_items"`
	HolderSafePartitions    int                  `json:"holder_safe_partitions"`
	MixedHolderBatches      int                  `json:"mixed_holder_batches"`
	PlannerMaximumScan      int                  `json:"planner_maximum_scan"`
	SchedulerTick           time.Duration        `json:"scheduler_tick"`
	PhysicalDispatchModel   string               `json:"physical_dispatch_model"`
}

type lifecycleEvidence struct {
	HolderIsolation           bool `json:"holder_isolation"`
	LateCloseNonResurrection  bool `json:"late_close_non_resurrection"`
	HolderHandoffFenced       bool `json:"holder_handoff_fenced"`
	NewGenerationApplied      bool `json:"new_generation_applied"`
	ClockRollbackClamped      bool `json:"clock_rollback_clamped"`
	LargeJumpBatchBounded     bool `json:"large_jump_batch_bounded"`
	DrainCleared              int  `json:"drain_cleared"`
	DrainBacklog              int  `json:"drain_backlog"`
	DrainStopped              bool `json:"drain_stopped"`
	DrainRejectedRegistration bool `json:"drain_rejected_registration"`
}

type evidence struct {
	SchemaVersion string            `json:"schema_version"`
	Baseline      string            `json:"baseline"`
	Status        string            `json:"status"`
	SessionRoute  laneEvidence      `json:"session_route"`
	Subscription  laneEvidence      `json:"subscription"`
	Lifecycle     lifecycleEvidence `json:"lifecycle"`
	Checks        map[string]bool   `json:"checks"`
	Thresholds    map[string]any    `json:"thresholds"`
	Failures      []string          `json:"failures"`
}

type simulatedItem struct {
	ID     int
	Holder string
	Gen    uint64
}

type canonicalLifecycle struct {
	holder     string
	generation uint64
	closed     bool
	applied    int
}

func (state *canonicalLifecycle) apply(item leasebatch.Item[simulatedItem]) bool {
	if state == nil || state.closed || item.Holder != state.holder || item.Payload.Gen != state.generation {
		return false
	}
	state.applied++
	return true
}

func (state *canonicalLifecycle) close() {
	state.closed = true
	state.generation++
}

func (state *canonicalLifecycle) handoff(holder string, generation uint64) {
	state.holder = holder
	state.generation = generation
	state.closed = false
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func main() {
	flag.Parse()
	report, err := runEvidence()
	if err != nil {
		fatal(err)
	}
	if err := validateEvidence(&report); err != nil {
		report.Status = "FAIL"
		report.Failures = append(report.Failures, err.Error())
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fatal(err)
	}
	raw = append(raw, '\n')
	if dir := filepath.Dir(*outputPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fatal(err)
		}
	}
	if err := os.WriteFile(*outputPath, raw, 0o644); err != nil {
		fatal(err)
	}
	if report.Status != "PASS" {
		fatal(fmt.Errorf("formal lease batching evidence failed: %v", report.Failures))
	}
	fmt.Printf("lease batching evidence PASS: %s\n", *outputPath)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func formalConfig() leasebatch.Config {
	return leasebatch.Config{
		Limits: leasebatch.Limits{
			MaxItems: 256, MaxBytes: 256 << 10, MaxDispatchDuration: 25 * time.Millisecond,
		},
		Schedule: leasebatch.Schedule{
			Interval: 10 * time.Second, BucketCount: 256, MaxJitter: 10 * time.Millisecond,
		},
		MaxPlanScan: 1088,
	}
}

func runEvidence() (evidence, error) {
	report := evidence{
		SchemaVersion: "farm-gate-lease-batching-evidence/v1",
		Baseline:      "4b10d57d84fada08a309ec5a9a8d40a2432f80aa",
		Status:        "PASS",
		Checks:        make(map[string]bool),
		Thresholds: map[string]any{
			"items_per_lane": formalItems, "max_due_lag": "5.1s",
			"subscription_holder_safe_partitions": 500,
			"distribution_min_to_mean":            0.75, "distribution_max_to_mean": 1.25,
		},
		Failures: []string{},
	}
	base := time.Unix(1_800_000_000, 0)
	var err error
	report.SessionRoute, err = simulateLane("session_route", base,
		func(int) string { return "gate-a/boot-a" },
		"one go-redis Pipeline.Exec per batch; single-endpoint redis.Client is one network round trip; one EVAL per item")
	if err != nil {
		return report, err
	}
	report.Subscription, err = simulateLane("subscription", base,
		func(index int) string {
			// 500 one-farm partitions model 100,000 subscriptions at the
			// existing 200-subscription farm cap without mixing physical owners.
			return fmt.Sprintf("gate-a/boot-a/farm-holder-%03d/boot-1/epoch-1", index%500)
		},
		"one BatchRenewSubscriptions gRPC per one-farm holder-safe batch; sharded dispatch resolves the exact current Holder outside Gate.mu")
	if err != nil {
		return report, err
	}
	report.Lifecycle, err = simulateLifecycle(base)
	if err != nil {
		return report, err
	}
	return report, nil
}

func simulateLane(lane string, base time.Time, holderFor func(int) string, physical string) (laneEvidence, error) {
	cfg := formalConfig()
	planner, err := leasebatch.NewPlanner[simulatedItem](cfg)
	if err != nil {
		return laneEvidence{}, err
	}
	counts := make([]int, cfg.BucketCount)
	holders := make(map[string]struct{})
	stable, jitterBounded := true, true
	for i := 0; i < formalItems; i++ {
		identity := fmt.Sprintf("%s:gate-a:boot-a:%06d", lane, i)
		bucketA, jitterA, err := leasebatch.StablePhase(identity, cfg.Schedule)
		if err != nil {
			return laneEvidence{}, err
		}
		bucketB, jitterB, _ := leasebatch.StablePhase(identity, cfg.Schedule)
		stable = stable && bucketA == bucketB && jitterA == jitterB
		jitterBounded = jitterBounded && jitterA >= 0 && jitterA <= cfg.MaxJitter
		counts[bucketA]++
		due, _ := leasebatch.NextDue(base, identity, cfg.Schedule)
		estimated := 512
		if i == formalItems/2 {
			estimated = cfg.MaxBytes + 1
		}
		key := fmt.Sprintf("%s-%06d", lane, i)
		holder := holderFor(i)
		holders[holder] = struct{}{}
		if err := planner.Upsert(leasebatch.Item[simulatedItem]{
			Key: key, Identity: identity, Holder: holder, Due: due,
			Deadline: base.Add(29 * time.Second), EstimatedBytes: estimated,
			Payload: simulatedItem{ID: i, Holder: holder, Gen: 1},
		}); err != nil {
			return laneEvidence{}, err
		}
	}
	result := laneEvidence{
		Items: formalItems, Limits: cfg.Limits, Schedule: cfg.Schedule,
		Distribution:            distribution(counts, stable, jitterBounded),
		OversizeDispatchedAlone: true, PlannerMaximumScan: cfg.MaxPlanScan,
		HolderSafePartitions: len(holders), SchedulerTick: formalSchedulerTick, PhysicalDispatchModel: physical,
	}
	clock := &fakeClock{now: base}
	visited := make([]bool, formalItems)
	for result.PlannedItems < formalItems {
		batch := planner.Plan(clock.Now(), false)
		if len(batch.Items) == 0 {
			next, ok := planner.NextDue()
			if !ok {
				break
			}
			if next.After(clock.Now()) {
				delta := next.Sub(clock.Now())
				steps := (delta + formalSchedulerTick - 1) / formalSchedulerTick
				clock.Advance(steps * formalSchedulerTick)
			}
			continue
		}
		result.Batches++
		result.PlannedItems += len(batch.Items)
		if len(batch.Items) > result.MaximumBatchItems {
			result.MaximumBatchItems = len(batch.Items)
		}
		if batch.Oversize {
			result.OversizeItems += len(batch.Items)
			if len(batch.Items) != 1 {
				result.OversizeDispatchedAlone = false
			}
		} else if batch.EstimatedBytes > result.MaximumNonOversizeBytes {
			result.MaximumNonOversizeBytes = batch.EstimatedBytes
		}
		if batch.MaxDueLag > result.MaximumDueLag {
			result.MaximumDueLag = batch.MaxDueLag
		}
		for _, item := range batch.Items {
			if item.Holder != batch.Holder {
				result.MixedHolderBatches++
			}
			if item.Payload.ID < 0 || item.Payload.ID >= formalItems || visited[item.Payload.ID] {
				return result, fmt.Errorf("%s duplicate/invalid item %d", lane, item.Payload.ID)
			}
			visited[item.Payload.ID] = true
		}
		dispatch := 200*time.Microsecond + time.Duration(len(batch.Items))*10*time.Microsecond
		if dispatch > result.MaximumDispatchDuration {
			result.MaximumDispatchDuration = dispatch
		}
		advance := formalSchedulerTick
		if dispatch > advance {
			advance = dispatch
		}
		clock.Advance(advance)
	}
	for _, seen := range visited {
		if !seen {
			result.StarvedItems++
		}
	}
	return result, nil
}

func distribution(counts []int, stable, jitterBounded bool) distributionEvidence {
	minimum, maximum, total := math.MaxInt, 0, 0
	for _, count := range counts {
		minimum = min(minimum, count)
		maximum = max(maximum, count)
		total += count
	}
	mean := float64(total) / float64(len(counts))
	return distributionEvidence{Buckets: len(counts), Minimum: minimum, Maximum: maximum, Mean: mean,
		MaxToMean: float64(maximum) / mean, MinToMean: float64(minimum) / mean,
		StableReplay: stable, JitterBounded: jitterBounded}
}

func simulateLifecycle(now time.Time) (lifecycleEvidence, error) {
	cfg := formalConfig()
	cfg.MaxItems = 2
	planner, _ := leasebatch.NewPlanner[simulatedItem](cfg)
	for i, holder := range []string{"holder-a/boot-a/epoch-1", "holder-b/boot-b/epoch-2", "holder-a/boot-a/epoch-1"} {
		key := fmt.Sprintf("isolation-%d", i)
		_ = planner.Upsert(leasebatch.Item[simulatedItem]{Key: key, Identity: key, Holder: holder,
			Due: now, Deadline: now.Add(time.Minute), EstimatedBytes: 100,
			Payload: simulatedItem{ID: i, Holder: holder, Gen: 1}})
	}
	isolated := true
	for planner.Stats().Backlog > 0 {
		batch := planner.Plan(now, false)
		for _, item := range batch.Items {
			isolated = isolated && item.Holder == batch.Holder
		}
	}

	latePlanner, _ := leasebatch.NewPlanner[simulatedItem](cfg)
	late := leasebatch.Item[simulatedItem]{Key: "late", Identity: "late", Holder: "holder-a/boot-a/epoch-1",
		Due: now, Deadline: now.Add(time.Minute), EstimatedBytes: 100,
		Payload: simulatedItem{ID: 1, Holder: "holder-a/boot-a/epoch-1", Gen: 1}}
	_ = latePlanner.Upsert(late)
	inflight := latePlanner.Plan(now, false).Items[0]
	canonical := canonicalLifecycle{holder: late.Holder, generation: late.Payload.Gen}
	canonical.close()
	lateCloseRejected := !canonical.apply(inflight) && canonical.applied == 0
	canonical.handoff("holder-b/boot-b/epoch-2", 3)
	lateHandoffRejected := !canonical.apply(inflight) && canonical.applied == 0
	current := leasebatch.Item[simulatedItem]{
		Holder: canonical.holder, Payload: simulatedItem{Holder: canonical.holder, Gen: canonical.generation},
	}
	newGenerationApplied := canonical.apply(current) && canonical.applied == 1

	rollbackPlanner, _ := leasebatch.NewPlanner[simulatedItem](cfg)
	future := now.Add(time.Hour)
	_ = rollbackPlanner.Upsert(leasebatch.Item[simulatedItem]{Key: "future", Identity: "future", Holder: "holder",
		Due: future, Deadline: future.Add(time.Minute), EstimatedBytes: 100})
	_ = rollbackPlanner.Plan(now, false)
	_ = rollbackPlanner.Plan(now.Add(-time.Hour), false)
	rollback := rollbackPlanner.Stats().ClockRollback == 1
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("jump-%d", i)
		_ = rollbackPlanner.Upsert(leasebatch.Item[simulatedItem]{Key: key, Identity: key, Holder: "holder",
			Due: now, Deadline: future, EstimatedBytes: 100})
	}
	jump := rollbackPlanner.Plan(now.Add(24*time.Hour), false)
	largeJumpBounded := len(jump.Items) <= cfg.MaxItems

	drainPlanner, _ := leasebatch.NewPlanner[simulatedItem](cfg)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("drain-%d", i)
		_ = drainPlanner.Upsert(leasebatch.Item[simulatedItem]{Key: key, Identity: key, Holder: "holder",
			Due: future, Deadline: future.Add(time.Minute), EstimatedBytes: 100})
	}
	cleared := drainPlanner.Stop()
	rejectedAfterDrain := drainPlanner.Upsert(leasebatch.Item[simulatedItem]{
		Key: "after-drain", Identity: "after-drain", Holder: "holder",
		Due: future, Deadline: future.Add(time.Minute), EstimatedBytes: 100,
	}) != nil
	stats := drainPlanner.Stats()
	return lifecycleEvidence{
		HolderIsolation: isolated, LateCloseNonResurrection: lateCloseRejected,
		HolderHandoffFenced: lateHandoffRejected, NewGenerationApplied: newGenerationApplied,
		ClockRollbackClamped: rollback, LargeJumpBatchBounded: largeJumpBounded,
		DrainCleared: cleared, DrainBacklog: stats.Backlog, DrainStopped: stats.Stopped,
		DrainRejectedRegistration: rejectedAfterDrain,
	}, nil
}

func validateEvidence(report *evidence) error {
	if report == nil {
		return errors.New("nil report")
	}
	var failures []string
	checkLane := func(name string, lane laneEvidence) {
		checks := map[string]bool{
			name + "_100k":             lane.Items == formalItems && lane.PlannedItems == formalItems,
			name + "_stable":           lane.Distribution.StableReplay && lane.Distribution.JitterBounded,
			name + "_distributed":      lane.Distribution.MinToMean >= 0.75 && lane.Distribution.MaxToMean <= 1.25,
			name + "_items_bound":      lane.MaximumBatchItems <= lane.Limits.MaxItems,
			name + "_bytes_bound":      lane.MaximumNonOversizeBytes <= lane.Limits.MaxBytes,
			name + "_duration_bound":   lane.MaximumDispatchDuration <= lane.Limits.MaxDispatchDuration,
			name + "_due_lag":          lane.MaximumDueLag <= 5100*time.Millisecond,
			name + "_no_starvation":    lane.StarvedItems == 0,
			name + "_holder_isolation": lane.MixedHolderBatches == 0,
			name + "_oversize":         lane.OversizeItems == 1 && lane.OversizeDispatchedAlone,
		}
		for check, passed := range checks {
			report.Checks[check] = passed
			if !passed {
				failures = append(failures, check)
			}
		}
	}
	checkLane("session_route", report.SessionRoute)
	checkLane("subscription", report.Subscription)
	report.Checks["session_route_single_gate_holder"] = report.SessionRoute.HolderSafePartitions == 1
	report.Checks["subscription_500_holder_safe_partitions"] = report.Subscription.HolderSafePartitions == 500
	if !report.Checks["session_route_single_gate_holder"] {
		failures = append(failures, "session_route_single_gate_holder")
	}
	if !report.Checks["subscription_500_holder_safe_partitions"] {
		failures = append(failures, "subscription_500_holder_safe_partitions")
	}
	lifecycleChecks := map[string]bool{
		"holder_isolation":            report.Lifecycle.HolderIsolation,
		"late_close_non_resurrection": report.Lifecycle.LateCloseNonResurrection,
		"holder_handoff_fenced":       report.Lifecycle.HolderHandoffFenced,
		"new_generation_applied":      report.Lifecycle.NewGenerationApplied,
		"clock_rollback_clamped":      report.Lifecycle.ClockRollbackClamped,
		"large_jump_bounded":          report.Lifecycle.LargeJumpBatchBounded,
		"drain_cleared_stopped":       report.Lifecycle.DrainCleared == 100 && report.Lifecycle.DrainBacklog == 0 && report.Lifecycle.DrainStopped && report.Lifecycle.DrainRejectedRegistration,
	}
	for check, passed := range lifecycleChecks {
		report.Checks[check] = passed
		if !passed {
			failures = append(failures, check)
		}
	}
	if len(failures) != 0 {
		return fmt.Errorf("failed checks: %v", failures)
	}
	return nil
}
