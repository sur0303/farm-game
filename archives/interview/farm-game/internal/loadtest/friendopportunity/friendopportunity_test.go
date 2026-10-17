package friendopportunity

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func testConfig() RunConfig {
	c := DefaultConfig()
	c.FriendCount = 100
	c.PlotCount = 12
	c.OpportunityDensityPercent = 20
	c.OnlineRatePercent = 5
	c.ArrivalRatePerSecond = 5
	c.DurationMS = 180_000
	c.WorkerConcurrency = 4
	c.MaxInFlight = 64
	return c
}

func TestGenerateTraceIsDeterministicAndStreamsAreIndependent(t *testing.T) {
	c := testConfig()
	first, err := GenerateTrace(c)
	if err != nil {
		t.Fatal(err)
	}
	second, err := GenerateTrace(c)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same config and seed generated different traces")
	}
	if first.Hashes() != second.Hashes() {
		t.Fatal("same trace produced different hashes")
	}

	faster := c
	faster.ArrivalRatePerSecond = 10
	fasterTrace, err := GenerateTrace(faster)
	if err != nil {
		t.Fatal(err)
	}
	if first.Hashes().BoundarySHA256 != fasterTrace.Hashes().BoundarySHA256 {
		t.Fatal("boundary trace changed when only session arrival rate changed")
	}
	if !reflect.DeepEqual(first.BoundaryEvents, fasterTrace.BoundaryEvents) {
		t.Fatal("boundary events changed when only session arrival rate changed")
	}
	if first.Hashes().SessionSHA256 == fasterTrace.Hashes().SessionSHA256 {
		t.Fatal("session trace hash did not change with arrival rate")
	}
}

func TestVariantsShareTraceAndAEqualsB1Fallback(t *testing.T) {
	c := testConfig()
	trace, err := GenerateTrace(c)
	if err != nil {
		t.Fatal(err)
	}
	results, err := RunAll(c, trace)
	if err != nil {
		t.Fatal(err)
	}
	byVariant, err := ResultsByVariant(results)
	if err != nil {
		t.Fatal(err)
	}
	a := byVariant[VariantPull]
	b1 := byVariant[VariantSchedulerSafe]
	b2 := byVariant[VariantSchedulerEfficient]

	if a.Metrics.FallbackPulls != b1.Metrics.FallbackPulls {
		t.Fatalf("A fallback pulls=%d, B1=%d", a.Metrics.FallbackPulls, b1.Metrics.FallbackPulls)
	}
	if a.SessionLifecycle != b1.SessionLifecycle {
		t.Fatalf("A and B1 session lifecycle differs: %+v vs %+v", a.SessionLifecycle, b1.SessionLifecycle)
	}
	for _, result := range results {
		if result.TraceHashes != trace.Hashes() {
			t.Fatalf("%s did not preserve the fixed trace hash", result.Variant)
		}
		if !result.Synthetic || !strings.Contains(result.SyntheticDisclaimer, "SYNTHETIC") {
			t.Fatalf("%s is missing the synthetic marker", result.Variant)
		}
	}
	if b1.Metrics.ScheduleUpserts+b1.Metrics.ScheduleCancels != int64(len(trace.BoundaryEvents)) {
		t.Fatalf("scheduler writes=%d, boundary events=%d",
			b1.Metrics.ScheduleUpserts+b1.Metrics.ScheduleCancels, len(trace.BoundaryEvents))
	}
	if b1.Metrics.WakeupClaims != b1.Metrics.ScheduleUpserts {
		t.Fatalf("claims=%d, upserts=%d", b1.Metrics.WakeupClaims, b1.Metrics.ScheduleUpserts)
	}
	if b2.Metrics.FallbackPulls >= a.Metrics.FallbackPulls {
		t.Fatalf("B2 fallback pulls=%d, want less than A=%d", b2.Metrics.FallbackPulls, a.Metrics.FallbackPulls)
	}
	if b2.PollSavings == nil || b2.PollSavings.SafeFallbackPulls != a.Metrics.FallbackPulls {
		t.Fatalf("B2 poll-savings baseline does not match A: %+v", b2.PollSavings)
	}
	if b2.Metrics.ScheduleUpserts != b1.Metrics.ScheduleUpserts ||
		b2.Metrics.ScheduleCancels != b1.Metrics.ScheduleCancels ||
		b2.Metrics.WakeupClaims != b1.Metrics.WakeupClaims ||
		b2.Metrics.ExactPullsAfterHint != b1.Metrics.ExactPullsAfterHint ||
		b2.Metrics.OnlineHintTargets != b1.Metrics.OnlineHintTargets {
		t.Fatalf("B2 changed Scheduler behavior beyond fallback interval: B1=%+v B2=%+v",
			b1.Metrics, b2.Metrics)
	}
	if b2.PollSavings.SchedulerLogicalOps != b1.Metrics.LogicalRedisOps-a.Metrics.LogicalRedisOps {
		t.Fatalf("B2 scheduler ops=%d, B1-A=%d", b2.PollSavings.SchedulerLogicalOps,
			b1.Metrics.LogicalRedisOps-a.Metrics.LogicalRedisOps)
	}
	if b2.PollSavings.SchedulerLogicalBytes != b1.Metrics.LogicalRedisBytes-a.Metrics.LogicalRedisBytes {
		t.Fatalf("B2 scheduler bytes=%d, B1-A=%d", b2.PollSavings.SchedulerLogicalBytes,
			b1.Metrics.LogicalRedisBytes-a.Metrics.LogicalRedisBytes)
	}
}

func TestScheduleVolumeDoesNotScaleWithArrivalRate(t *testing.T) {
	low := testConfig()
	low.ArrivalRatePerSecond = 1.9
	high := low
	high.ArrivalRatePerSecond = 10

	lowTrace, err := GenerateTrace(low)
	if err != nil {
		t.Fatal(err)
	}
	highTrace, err := GenerateTrace(high)
	if err != nil {
		t.Fatal(err)
	}
	lowResult, err := RunVariant(low, lowTrace, VariantSchedulerSafe)
	if err != nil {
		t.Fatal(err)
	}
	highResult, err := RunVariant(high, highTrace, VariantSchedulerSafe)
	if err != nil {
		t.Fatal(err)
	}
	if lowTrace.Hashes().BoundarySHA256 != highTrace.Hashes().BoundarySHA256 {
		t.Fatal("boundary trace changed with GET_NEXT arrival rate")
	}
	if lowResult.Metrics.ScheduleUpserts != highResult.Metrics.ScheduleUpserts ||
		lowResult.Metrics.ScheduleCancels != highResult.Metrics.ScheduleCancels ||
		lowResult.Metrics.WakeupClaims != highResult.Metrics.WakeupClaims ||
		lowResult.Metrics.OnlineHintTargets != highResult.Metrics.OnlineHintTargets {
		t.Fatalf("scheduler volume changed with arrival rate: low=%+v high=%+v",
			lowResult.Metrics, highResult.Metrics)
	}
	if lowResult.SessionLifecycle.Planned >= highResult.SessionLifecycle.Planned {
		t.Fatal("test did not increase the session request stream")
	}
}

func TestPlotsAndDensityIncreaseBoundariesButNotQueryMultiplier(t *testing.T) {
	small := testConfig()
	small.PlotCount = 5
	small.OpportunityDensityPercent = 5
	large := small
	large.PlotCount = 24
	large.OpportunityDensityPercent = 50

	smallTrace, err := GenerateTrace(small)
	if err != nil {
		t.Fatal(err)
	}
	largeTrace, err := GenerateTrace(large)
	if err != nil {
		t.Fatal(err)
	}
	smallResult, err := RunVariant(small, smallTrace, VariantSchedulerSafe)
	if err != nil {
		t.Fatal(err)
	}
	largeResult, err := RunVariant(large, largeTrace, VariantSchedulerSafe)
	if err != nil {
		t.Fatal(err)
	}
	smallWrites := smallResult.Metrics.ScheduleUpserts + smallResult.Metrics.ScheduleCancels
	largeWrites := largeResult.Metrics.ScheduleUpserts + largeResult.Metrics.ScheduleCancels
	if largeWrites <= smallWrites {
		t.Fatalf("larger plot/density case writes=%d, small=%d", largeWrites, smallWrites)
	}
	if smallWrites != int64(len(smallTrace.BoundaryEvents)) ||
		largeWrites != int64(len(largeTrace.BoundaryEvents)) {
		t.Fatal("schedule writes are not one-for-one with farm boundary mutations")
	}
	if smallResult.SessionLifecycle.Planned != largeResult.SessionLifecycle.Planned ||
		smallResult.Metrics.FallbackPulls != largeResult.Metrics.FallbackPulls {
		t.Fatal("plot/density dimensions multiplied the GET_NEXT/fallback stream")
	}
}

func TestOpenLoopRecordsRejectionInsteadOfReducingPlannedLoad(t *testing.T) {
	c := DefaultConfig()
	c.FriendCount = 300
	c.ArrivalRatePerSecond = 1000
	c.DurationMS = 1_000
	c.WorkerConcurrency = 1
	c.MaxInFlight = 1
	trace, err := GenerateTrace(c)
	if err != nil {
		t.Fatal(err)
	}
	result, err := RunVariant(c, trace, VariantPull)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := result.SessionLifecycle
	if lifecycle.Planned != 1000 || lifecycle.Sent != lifecycle.Planned {
		t.Fatalf("open-loop planned/sent changed: %+v", lifecycle)
	}
	if lifecycle.Rejected == 0 {
		t.Fatalf("expected overload rejection, got %+v", lifecycle)
	}
	if lifecycle.Completed+lifecycle.Rejected != lifecycle.Sent {
		t.Fatalf("lifecycle does not reconcile: %+v", lifecycle)
	}
}

func TestArtifactsContainRawFormatsAndSyntheticMarkers(t *testing.T) {
	c := testConfig()
	c.DurationMS = 2_000
	c.ArrivalRatePerSecond = 1.9
	trace, err := GenerateTrace(c)
	if err != nil {
		t.Fatal(err)
	}
	results, err := RunAll(c, trace)
	if err != nil {
		t.Fatal(err)
	}
	outputDir := t.TempDir()
	if err := WriteArtifacts(outputDir, c, trace, results); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"run-config.json", "trace.jsonl", "results.jsonl", "results.csv", "summary.json", "manifest.json",
	} {
		if _, err := os.Stat(filepath.Join(outputDir, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
	traceData, err := os.ReadFile(filepath.Join(outputDir, "trace.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(traceData, []byte(`"stream":"session_get_next"`)) ||
		!bytes.Contains(traceData, []byte(`"stream":"farm_boundary"`)) {
		t.Fatal("trace JSONL does not contain both independent streams")
	}
	resultsData, err := os.ReadFile(filepath.Join(outputDir, "results.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var decoded RunResult
	if err := json.Unmarshal(bytes.Split(bytes.TrimSpace(resultsData), []byte("\n"))[0], &decoded); err != nil {
		t.Fatalf("results JSONL is not raw decodable JSON: %v", err)
	}
	if !decoded.Synthetic {
		t.Fatal("result artifact lost synthetic marker")
	}
	csvData, err := os.ReadFile(filepath.Join(outputDir, "results.csv"))
	if err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"fallback_pulls", "schedule_upserts", "logical_redis_bytes", "detection_latency_p95_ms"} {
		if !bytes.Contains(csvData, []byte(column)) {
			t.Fatalf("results CSV is missing %s", column)
		}
	}
}

func TestMatrixCoversRequiredDimensions(t *testing.T) {
	configs := MatrixConfigs(DefaultConfig())
	if got, want := len(configs), 3*3*4*3*2; got != want {
		t.Fatalf("matrix size=%d, want %d", got, want)
	}
}
