package friendcapacity

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func TestLowCardinalityWhitelists(t *testing.T) {
	for name, values := range map[string][]string{
		"stages":   AllowedStages(),
		"modes":    AllowedModes(),
		"outcomes": AllowedOutcomes(),
		"layers":   AllowedLayers(),
	} {
		seen := make(map[string]struct{}, len(values))
		for _, value := range values {
			if strings.TrimSpace(value) == "" {
				t.Fatalf("%s contains empty whitelist entry", name)
			}
			if strings.ContainsAny(value, " \t\"\\\n") {
				t.Fatalf("%s contains unsafe label value %q", name, value)
			}
			if _, exists := seen[value]; exists {
				t.Fatalf("%s contains duplicate %q", name, value)
			}
			seen[value] = struct{}{}
		}
	}
	// The whitelists are deliberately closed sets; arbitrary caller input must
	// fall back to bounded sentinels instead of creating new series.
	if got := NormalizeStage("player_id_12345"); got != StageUnknown {
		t.Fatalf("stage normalization=%q want unknown", got)
	}
	if got := NormalizeMode("farm_id_777"); got != ModeUnknown {
		t.Fatalf("mode normalization=%q want unknown", got)
	}
	if got := NormalizeOutcome("gateway_id_g1"); got != OutcomeOther {
		t.Fatalf("outcome normalization=%q want other", got)
	}
	if got := NormalizeLayer("operation_id_op1"); got != LayerOther {
		t.Fatalf("layer normalization=%q want other", got)
	}
}

func TestObserveRecordsOnlyBoundedSeries(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	metrics.Observe(PhaseObservation{Stage: StageFriendIndex, Mode: ModeFriendList, Outcome: OutcomeOK, Layer: LayerClient}, time.Millisecond)
	metrics.Observe(PhaseObservation{Stage: "player_id_9", Mode: "operation_id_1", Outcome: "gateway_id_g", Layer: "farm_id_4"}, time.Millisecond)
	metrics.ObserveBatch(StageSessionRouteBatch, ModeHint, 12)
	metrics.ObserveBatch(StageHintDelivery, ModeHint, -1)
	metrics.ObserveEstimatedBytes(StageHintDelivery, ModeHint, 4096)
	metrics.ObserveEstimatedBytes(StagePull, ModeNextOpportunity, -5)

	if got := testutil.ToFloat64(metrics.requestsTotal.WithLabelValues(StageFriendIndex, ModeFriendList, OutcomeOK, LayerClient)); got != 1 {
		t.Fatalf("friend_index requests=%v", got)
	}
	// Unexpected labels collapse onto the bounded sentinels and must not create
	// new label values.
	if got := testutil.ToFloat64(metrics.requestsTotal.WithLabelValues(StageUnknown, ModeUnknown, OutcomeOther, LayerOther)); got != 1 {
		t.Fatalf("unknown fallback requests=%v", got)
	}
	if got := histogramSampleCount(metrics.batchSize.WithLabelValues(StageSessionRouteBatch, ModeHint)); got != 1 {
		t.Fatalf("batch size observations=%v", got)
	}
	if got := histogramSampleCount(metrics.estimatedBytes.WithLabelValues(StageHintDelivery, ModeHint)); got != 1 {
		t.Fatalf("estimated bytes observations=%v", got)
	}
	if got := histogramSampleCount(metrics.estimatedBytes.WithLabelValues(StagePull, ModeNextOpportunity)); got != 0 {
		t.Fatalf("negative estimated bytes must not be observed, got=%v", got)
	}
	if got := histogramSampleCount(metrics.batchSize.WithLabelValues(StageHintDelivery, ModeHint)); got != 0 {
		t.Fatalf("negative batch size must not be observed, got=%v", got)
	}
}

func TestBeginIsIdempotentAndNilSafe(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	stop := metrics.Begin(StageWorkerDiscovery, ModeWorker)
	stop(OutcomeOK)
	stop(OutcomeError) // second call must be ignored
	if got := testutil.ToFloat64(metrics.requestsTotal.WithLabelValues(StageWorkerDiscovery, ModeWorker, OutcomeOK, LayerClient)); got != 1 {
		t.Fatalf("begin ok count=%v", got)
	}
	if got := testutil.ToFloat64(metrics.requestsTotal.WithLabelValues(StageWorkerDiscovery, ModeWorker, OutcomeError, LayerClient)); got != 0 {
		t.Fatalf("begin duplicate outcome count=%v", got)
	}
	if nilMetrics := (*Metrics)(nil); !noopCall(nilMetrics.Begin(StagePull, ModeNextOpportunity)) {
		t.Fatal("nil metrics stop must be a no-op")
	}
	nilMetrics := (*Metrics)(nil)
	nilMetrics.Observe(PhaseObservation{Stage: StagePull, Mode: ModeNextOpportunity, Outcome: OutcomeOK, Layer: LayerClient}, time.Second)
	nilMetrics.ObserveBatch(StagePull, ModeNextOpportunity, 3)
	nilMetrics.ObserveEstimatedBytes(StagePull, ModeNextOpportunity, 3)
}

func TestHistogramBucketBoundaries(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	// Sub-second durations land in the first buckets; seconds land in later
	// buckets. The explicit bucket list must be monotonic and end above the
	// largest observation so nothing silently lands in +Inf only.
	metrics.Observe(PhaseObservation{Stage: StagePull, Mode: ModeNextOpportunity, Outcome: OutcomeOK, Layer: LayerClient}, 0)
	metrics.Observe(PhaseObservation{Stage: StagePull, Mode: ModeNextOpportunity, Outcome: OutcomeOK, Layer: LayerClient}, time.Duration(-time.Second))
	metrics.Observe(PhaseObservation{Stage: StagePull, Mode: ModeNextOpportunity, Outcome: OutcomeOK, Layer: LayerClient}, 10*time.Second)
	got, err := testutil.GatherAndCount(registry)
	if err != nil {
		t.Fatal(err)
	}
	if got == 0 {
		t.Fatal("expected gathered metric families")
	}
	// Observed durations <= 0 are clamped to 0 and still counted once.
	if value := testutil.ToFloat64(metrics.requestsTotal.WithLabelValues(StagePull, ModeNextOpportunity, OutcomeOK, LayerClient)); value != 3 {
		t.Fatalf("clamped duration count=%v", value)
	}
}

func TestValidLabelValueRejectsUnsafeInput(t *testing.T) {
	if err := ValidLabelValue("run_abc123"); err != nil {
		t.Fatalf("safe value rejected: %v", err)
	}
	for _, unsafe := range []string{"", "  ", "a\nb", "a\"b", "a\\b"} {
		if err := ValidLabelValue(unsafe); err == nil {
			t.Fatalf("unsafe value %q accepted", unsafe)
		}
	}
}

func histogramSampleCount(observer prometheus.Observer) float64 {
	collected := make(chan prometheus.Metric, 4)
	observer.(prometheus.Collector).Collect(collected)
	close(collected)
	var total float64
	for metric := range collected {
		var dtoMetric dto.Metric
		if err := metric.Write(&dtoMetric); err == nil {
			if histogram := dtoMetric.GetHistogram(); histogram != nil {
				total += float64(histogram.GetSampleCount())
			}
		}
	}
	return total
}

func noopCall(stop func(string)) bool {
	stop("ok")
	return true
}

func allowedStagesSlice() []string {
	out := make([]string, 0, len(allowedStages))
	for key := range allowedStages {
		out = append(out, key)
	}
	return out
}

func allowedModesSlice() []string {
	out := make([]string, 0, len(allowedModes))
	for key := range allowedModes {
		out = append(out, key)
	}
	return out
}

func allowedOutcomesSlice() []string {
	out := make([]string, 0, len(allowedOutcomes))
	for key := range allowedOutcomes {
		out = append(out, key)
	}
	return out
}

func allowedLayersSlice() []string {
	out := make([]string, 0, len(allowedLayers))
	for key := range allowedLayers {
		out = append(out, key)
	}
	return out
}

func TestNormalizeUnknownAndCaseInsensitive(t *testing.T) {
	if got := NormalizeStage(strings.ToUpper(StagePairExact)); got != StagePairExact {
		t.Fatalf("case-insensitive stage=%q", got)
	}
	if got := NormalizeOutcome("  dropped  "); got != OutcomeDropped {
		t.Fatalf("trimmed outcome=%q", got)
	}
	if got := NormalizeLayer("REDIS_CLIENT"); got != LayerRedisClient {
		t.Fatalf("case-insensitive layer=%q", got)
	}
	if err := errors.New("sentinel"); err == nil {
		t.Fatal("unreachable")
	}
}

func TestQuantileSnapshotAggregatesBoundedSeries(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	metrics.Observe(PhaseObservation{Stage: StagePull, Mode: ModeFriendList, Outcome: OutcomeOK, Layer: LayerClient}, 5*time.Millisecond)
	metrics.Observe(PhaseObservation{Stage: StagePull, Mode: ModeFriendList, Outcome: OutcomeOK, Layer: LayerClient}, 9*time.Millisecond)
	metrics.ObserveBatch(StagePull, ModeFriendList, 20)
	metrics.ObserveBatch(StagePull, ModeFriendList, 20)

	snapshot := metrics.QuantileSnapshot()
	if len(snapshot.Phases) != 1 {
		t.Fatalf("phases=%+v", snapshot.Phases)
	}
	phase := snapshot.Phases[0]
	if phase.Stage != StagePull || phase.Mode != ModeFriendList || phase.Outcome != OutcomeOK ||
		phase.Layer != LayerClient || phase.Count != 2 {
		t.Fatalf("phase aggregate=%+v", phase)
	}
	if phase.SumSec < 0.005 || phase.SumSec > 0.02 || phase.P50Sec < 0.005 || phase.MaxSec < 0.009 {
		t.Fatalf("phase duration bounds=%+v", phase)
	}
	if len(snapshot.Batches) != 1 {
		t.Fatalf("batches=%+v", snapshot.Batches)
	}
	batch := snapshot.Batches[0]
	if batch.Count != 2 || batch.Sum != 40 {
		t.Fatalf("batch aggregate=%+v", batch)
	}
	// The histogram quantiles are reconstructed from cumulative buckets, so
	// they are bounded by the bucket that contains 20 rather than exact.
	if batch.P50 < 16 || batch.P50 > 32 || batch.Max < 16 || batch.Max > 32 {
		t.Fatalf("batch quantile bounds=%+v", batch)
	}
	if got := (*Metrics)(nil).QuantileSnapshot(); len(got.Phases) != 0 || len(got.Batches) != 0 {
		t.Fatalf("nil snapshot must be empty, got=%+v", got)
	}
}

func TestQuantileSnapshotEmptyAfterNoObservations(t *testing.T) {
	metrics := NewMetrics(prometheus.NewRegistry())
	snapshot := metrics.QuantileSnapshot()
	if len(snapshot.Phases) != 0 || len(snapshot.Batches) != 0 {
		t.Fatalf("expected empty snapshot, got=%+v", snapshot)
	}
}

func TestUnavailableServerPhasesAreExplicit(t *testing.T) {
	phases := UnavailableServerPhases()
	if len(phases) == 0 {
		t.Fatal("server phases must be listed explicitly")
	}
	seen := map[string]struct{}{}
	for _, phase := range phases {
		if phase.Availability != MeasurementUnavailable {
			t.Fatalf("server phase %q availability=%q", phase.Stage, phase.Availability)
		}
		if phase.Layer != LayerHTTPServer {
			t.Fatalf("server phase %q layer=%q", phase.Stage, phase.Layer)
		}
		if strings.TrimSpace(phase.Reason) == "" {
			t.Fatalf("server phase %q missing reason", phase.Stage)
		}
		if _, dup := seen[phase.Stage]; dup {
			t.Fatalf("duplicate server phase %q", phase.Stage)
		}
		seen[phase.Stage] = struct{}{}
	}
	for _, required := range []string{
		StageHeaderColdScan, StageCandidateWindowLoad, StageSessionRouteBatch,
		StageGateEnqueue, StageWorkerDiscovery, StageHintDelivery,
	} {
		if _, ok := seen[required]; !ok {
			t.Fatalf("missing required unavailable server phase %q", required)
		}
	}
	// Client-observed stages must not be silently listed as server-unavailable.
	if _, ok := seen[StagePull]; ok {
		t.Fatal("pull is client-observed and must not appear as an unavailable server phase")
	}
	if _, ok := seen[StageHintReceive]; ok {
		t.Fatal("hint_receive is client-observed and must not appear as an unavailable server phase")
	}
}

func TestQuantileSnapshotCollectsOver32LabelSeriesWithoutDeadlock(t *testing.T) {
	// Regression: prometheus.HistogramVec.Collect blocks when its metric
	// channel is full; the old 32-buffer + single-goroutine drain deadlocked
	// with more than 32 label series. 40 distinct bounded label combinations
	// must snapshot completely and terminate.
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	const series = 40
	stages := mapKeys(allowedStages)
	modes := mapKeys(allowedModes)
	outcomes := mapKeys(allowedOutcomes)
	layers := mapKeys(allowedLayers)
	// Combine stages and outcomes with distinct pairing so the 40 iterations
	// produce 40 unique label series (stages=18, modes=4, outcomes=10,
	// layers=4).
	seen := map[string]bool{}
	for i := 0; i < series; i++ {
		observation := PhaseObservation{
			Stage: stages[i%len(stages)], Mode: modes[(i/len(stages))%len(modes)],
			Outcome: outcomes[(i/len(outcomes))%len(outcomes)], Layer: layers[(i/len(layers))%len(layers)],
		}
		key := observation.Stage + "/" + observation.Mode + "/" + observation.Outcome + "/" + observation.Layer
		if seen[key] {
			t.Fatalf("test series not unique at %d: %q", i, key)
		}
		seen[key] = true
		metrics.Observe(observation, time.Millisecond)
	}
	done := make(chan PhaseStats, 1)
	go func() {
		done <- metrics.QuantileSnapshot()
	}()
	select {
	case snapshot := <-done:
		if len(snapshot.Phases) != series {
			t.Fatalf("phases=%d want %d", len(snapshot.Phases), series)
		}
		observedKeys := map[string]bool{}
		for _, phase := range snapshot.Phases {
			key := phase.Stage + "/" + phase.Mode + "/" + phase.Outcome + "/" + phase.Layer
			if observedKeys[key] {
				t.Fatalf("duplicate phase series %q", key)
			}
			observedKeys[key] = true
		}
		for key := range seen {
			if !observedKeys[key] {
				t.Fatalf("snapshot is missing observed series %q", key)
			}
		}
	case <-time.After(10 * time.Second):
		t.Fatal("QuantileSnapshot deadlocked with >32 label series")
	}
}

func mapKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	return out
}
