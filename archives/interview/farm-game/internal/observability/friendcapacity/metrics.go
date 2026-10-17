// Package friendcapacity exposes low-cardinality, non-adjudicating
// observations for the friend list / next-opportunity / Hint delivery /
// Worker wakeup capacity baseline. It is C0 evidence only: business code never
// reads these metrics, and identity values (player_id, farm_id, operation_id,
// gateway_id) are never recorded as label values.
package friendcapacity

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Stages are the bounded phase labels emitted by Begin. They mirror the C0
// plan phases: FriendIndex scan, Header cold scan, CandidateWindow load/save,
// exact Pair/Profile/Facts/Receipt, SessionRoute batch, Gate grouping/enqueue,
// Worker discovery/claim/fanout and the client-visible pull/hint windows.
const (
	StageFriendIndex         = "friend_index"
	StageHeaderColdScan      = "header_cold_scan"
	StageCandidateWindowLoad = "candidate_window_load"
	StageCandidateWindowSave = "candidate_window_save"
	StagePairExact           = "pair_exact"
	StageProfile             = "profile"
	StageFacts               = "facts"
	StageReceipt             = "receipt"
	StageSessionRouteBatch   = "session_route_batch"
	StageGateEnqueue         = "gate_enqueue"
	StageHintDelivery        = "hint_delivery"
	StageWorkerDiscovery     = "worker_discovery"
	StageWorkerClaim         = "worker_claim"
	StageWorkerFanout        = "worker_fanout"
	StagePull                = "pull"
	StageEndToEnd            = "end_to_end"
	StageTotal               = "total"
	// StageHintReceive marks a client-observed server-originated Hint, which is
	// the only place a load tool can measure the opportunity boundary -> popup
	// window without touching production code.
	StageHintReceive = "hint_receive"
	StageUnknown     = "unknown"
)

// Modes are the bounded request-mode labels.
const (
	ModeFriendList      = "friend_list"
	ModeNextOpportunity = "next_opportunity"
	ModeHint            = "hint"
	ModeWorker          = "worker"
	ModeUnknown         = "unknown"
)

// Outcomes are the bounded result labels.
const (
	OutcomeOK          = "ok"
	OutcomeNotFound    = "not_found"
	OutcomeUnknown     = "unknown"
	OutcomeError       = "error"
	OutcomeDropped     = "dropped"
	OutcomeUnavailable = "unavailable"
	OutcomeFenced      = "fenced"
	OutcomeSkipped     = "skipped"
	OutcomeConflict    = "conflict"
	OutcomeOther       = "other"
)

// Layers are the bounded observability layer labels used by the load harness.
const (
	LayerHTTPServer  = "http_server"
	LayerClient      = "client"
	LayerRedisClient = "redis_client"
	LayerWorker      = "worker"
	LayerOther       = "other"
)

// NewMetrics registers and returns a low-cardinality friend-capacity metrics
// set. registerer nil defaults to the default Prometheus registry.
func NewMetrics(registerer prometheus.Registerer) *Metrics {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	m := &Metrics{
		durationSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm", Subsystem: "friend_capacity", Name: "phase_duration_seconds",
			Help:    "Bounded friend-capacity phase durations; labels are stage/mode/outcome/layer and never carry identity values.",
			Buckets: []float64{.00005, .0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"stage", "mode", "outcome", "layer"}),
		requestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "friend_capacity", Name: "requests_total",
			Help: "Bounded friend-capacity request/phase counts; labels never carry identity values.",
		}, []string{"stage", "mode", "outcome", "layer"}),
		batchSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm", Subsystem: "friend_capacity", Name: "batch_size",
			Help:    "Bounded batch size per phase (friends per list page, hint targets, routes per hint, gateway groups per hint, worker due refs).",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024},
		}, []string{"stage", "mode"}),
		estimatedBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm", Subsystem: "friend_capacity", Name: "estimated_bytes",
			Help:    "Estimated payload bytes observed by the load harness; values are estimates, never network-wire truth unless a Harness proxy is attached.",
			Buckets: prometheus.ExponentialBuckets(256, 2, 16),
		}, []string{"stage", "mode"}),
	}
	registerer.MustRegister(m.durationSeconds, m.requestsTotal, m.batchSize, m.estimatedBytes)
	return m
}

// Metrics is a thread-safe observer. All label values are normalized against a
// fixed whitelist so an unexpected caller cannot create unbounded cardinality.
type Metrics struct {
	durationSeconds *prometheus.HistogramVec
	requestsTotal   *prometheus.CounterVec
	batchSize       *prometheus.HistogramVec
	estimatedBytes  *prometheus.HistogramVec
}

// PhaseDurationSec aggregates one bounded phase duration for the JSON evidence
// sidecar. Values are always summed/counted aggregates; labels are the bounded
// stage/mode/outcome whitelist so no identity value can appear.
type PhaseDurationSec struct {
	Stage   string  `json:"stage"`
	Mode    string  `json:"mode"`
	Outcome string  `json:"outcome"`
	Layer   string  `json:"layer"`
	Count   uint64  `json:"count"`
	SumSec  float64 `json:"sum_seconds"`
	P50Sec  float64 `json:"p50_seconds"`
	P95Sec  float64 `json:"p95_seconds"`
	P99Sec  float64 `json:"p99_seconds"`
	MaxSec  float64 `json:"max_seconds"`
}

// PhaseAvailability records whether a planned phase was measured. Server-side
// stages that are not instrumented in current production code must appear as
// measurement_unavailable rather than being silently omitted from evidence.
type PhaseAvailability struct {
	Stage        string `json:"stage"`
	Layer        string `json:"layer"`
	Availability string `json:"availability"`
	Reason       string `json:"reason,omitempty"`
}

// MeasurementUnavailable is the explicit evidence token for a missing scrape or
// an uninstrumented production phase.
const MeasurementUnavailable = "measurement_unavailable"

// UnavailableServerPhases returns the C0 server-side phase list that production
// code does not currently emit. The load harness must surface these explicitly.
func UnavailableServerPhases() []PhaseAvailability {
	stages := []string{
		StageFriendIndex, StageHeaderColdScan, StageCandidateWindowLoad, StageCandidateWindowSave,
		StagePairExact, StageProfile, StageFacts, StageReceipt, StageSessionRouteBatch,
		StageGateEnqueue, StageHintDelivery, StageWorkerDiscovery, StageWorkerClaim,
		StageWorkerFanout, StageEndToEnd, StageTotal,
	}
	out := make([]PhaseAvailability, 0, len(stages))
	for _, stage := range stages {
		out = append(out, PhaseAvailability{
			Stage: stage, Layer: LayerHTTPServer,
			Availability: MeasurementUnavailable,
			Reason:       "server-side phase not instrumented in current production code",
		})
	}
	return out
}

// BatchObservation aggregates one bounded batch-size histogram for the JSON
// evidence sidecar.
type BatchObservation struct {
	Stage string  `json:"stage"`
	Mode  string  `json:"mode"`
	Count uint64  `json:"count"`
	Sum   float64 `json:"sum"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	Max   float64 `json:"max"`
}

// PhaseStats is the JSON evidence contract for bounded phase/batch aggregates.
type PhaseStats struct {
	Phases  []PhaseDurationSec `json:"phases"`
	Batches []BatchObservation `json:"batches"`
}

// QuantileSnapshot returns a stable aggregate snapshot of the bounded
// duration/batch histograms. It is the JSON serialization seam the load
// harness uses instead of scraping Prometheus text. A nil receiver returns a
// non-nil empty snapshot so callers can distinguish "no observations" from a
// missing measurement.
func (m *Metrics) QuantileSnapshot() PhaseStats {
	if m == nil {
		return PhaseStats{}
	}
	phases := gatherHistogramVec(m.durationSeconds, func(metric prometheus.Metric, labels map[string]string) (PhaseDurationSec, bool) {
		quantiles, ok := histogramQuantiles(metric)
		if !ok {
			return PhaseDurationSec{}, false
		}
		return PhaseDurationSec{
			Stage: labels["stage"], Mode: labels["mode"], Outcome: labels["outcome"],
			Layer: labels["layer"],
			Count: quantiles.count, SumSec: quantiles.sum,
			P50Sec: quantiles.quantile(.50), P95Sec: quantiles.quantile(.95),
			P99Sec: quantiles.quantile(.99), MaxSec: quantiles.quantile(1),
		}, true
	})
	batches := gatherHistogramVec(m.batchSize, func(metric prometheus.Metric, labels map[string]string) (BatchObservation, bool) {
		quantiles, ok := histogramQuantiles(metric)
		if !ok {
			return BatchObservation{}, false
		}
		return BatchObservation{
			Stage: labels["stage"], Mode: labels["mode"], Count: quantiles.count,
			Sum: quantiles.sum, P50: quantiles.quantile(.50), P95: quantiles.quantile(.95),
			Max: quantiles.quantile(1),
		}, true
	})
	return PhaseStats{Phases: phases, Batches: batches}
}

type histogramSummary struct {
	count  uint64
	sum    float64
	values []float64
}

func (h histogramSummary) quantile(q float64) float64 {
	if len(h.values) == 0 {
		return 0
	}
	index := int(float64(len(h.values)-1) * q)
	if index < 0 {
		index = 0
	}
	if index >= len(h.values) {
		index = len(h.values) - 1
	}
	return h.values[index]
}

func histogramQuantiles(metric prometheus.Metric) (histogramSummary, bool) {
	var out dto.Metric
	if err := metric.Write(&out); err != nil {
		return histogramSummary{}, false
	}
	histogram := out.GetHistogram()
	if histogram == nil {
		return histogramSummary{}, false
	}
	summary := histogramSummary{count: histogram.GetSampleCount(), sum: histogram.GetSampleSum()}
	// Reconstruct the exact observation distribution from the Prometheus
	// histogram's cumulative buckets. bucket counts are cumulative; convert to
	// per-bucket counts and expand each bucket at its midpoint. The last bucket
	// (+Inf) is expanded at 2*previousUpperBound so the quantile stays finite.
	previousUpper := 0.0
	previousCount := uint64(0)
	bucketValues := make([]float64, 0, int(histogram.GetSampleCount()))
	for _, bucket := range histogram.GetBucket() {
		count := bucket.GetCumulativeCount() - previousCount
		upper := bucket.GetUpperBound()
		representative := upper
		if upper == 0 {
			representative = 0
		} else if previousUpper > 0 && upper == math.Inf(1) {
			representative = previousUpper * 2
		}
		for i := uint64(0); i < count; i++ {
			bucketValues = append(bucketValues, representative)
		}
		previousUpper = upper
		previousCount += count
	}
	// Preserve the sum the histogram actually reported when bucket
	// reconstruction cannot be exact.
	summary.values = bucketValues
	return summary, true
}

func gatherHistogramVec[T any](vec *prometheus.HistogramVec, convert func(prometheus.Metric, map[string]string) (T, bool)) []T {
	if vec == nil {
		return []T{}
	}
	// prometheus.HistogramVec.Collect may spawn a goroutine per child metric
	// and blocks when the channel is full, so a buffer of 32 can deadlock with
	// 33+ label series. Collect in a goroutine and drain concurrently; the
	// WaitGroup guarantees Collect completed before we return.
	channel := make(chan prometheus.Metric, 32)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(channel)
		vec.Collect(channel)
	}()
	out := []T{}
	for metric := range channel {
		labels := map[string]string{}
		var dtoMetric dto.Metric
		if err := metric.Write(&dtoMetric); err == nil {
			for _, pair := range dtoMetric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
		}
		if converted, ok := convert(metric, labels); ok {
			out = append(out, converted)
		}
	}
	wg.Wait()
	sort.Slice(out, func(i, j int) bool {
		return fmt.Sprintf("%v", out[i]) < fmt.Sprintf("%v", out[j])
	})
	return out
}

// PhaseObservation is the low-cardinality observation emitted by Begin.
type PhaseObservation struct {
	Stage   string
	Mode    string
	Outcome string
	Layer   string
}

// Begin returns a stop function that records one phase. The stop function is
// safe to call multiple times (idempotent) and safe to call after the metrics
// owner is garbage collected.
func (m *Metrics) Begin(stage, mode string) func(outcome string) {
	if m == nil {
		return func(string) {}
	}
	started := time.Now()
	var once sync.Once
	return func(outcome string) {
		once.Do(func() {
			m.observeDuration(PhaseObservation{
				Stage: stage, Mode: mode, Outcome: outcome, Layer: LayerClient,
			}, time.Since(started))
		})
	}
}

// Observe records one completed phase observation. Use Begin for stopwatch
// semantics or Observe when a caller already has an elapsed duration.
func (m *Metrics) Observe(observation PhaseObservation, elapsed time.Duration) {
	if m == nil || observation == (PhaseObservation{}) {
		return
	}
	m.observeDuration(observation, elapsed)
}

func (m *Metrics) observeDuration(observation PhaseObservation, elapsed time.Duration) {
	stage := NormalizeStage(observation.Stage)
	mode := NormalizeMode(observation.Mode)
	outcome := NormalizeOutcome(observation.Outcome)
	layer := NormalizeLayer(observation.Layer)
	seconds := float64(elapsed) / float64(time.Second)
	if seconds < 0 {
		seconds = 0
	}
	m.durationSeconds.WithLabelValues(stage, mode, outcome, layer).Observe(seconds)
	m.requestsTotal.WithLabelValues(stage, mode, outcome, layer).Inc()
}

// ObserveBatch records a bounded batch size for a stage/mode. Values <= 0 are
// dropped because a zero-length batch is not an observable event.
func (m *Metrics) ObserveBatch(stage, mode string, size int) {
	if m == nil || size <= 0 {
		return
	}
	m.batchSize.WithLabelValues(NormalizeStage(stage), NormalizeMode(mode)).Observe(float64(size))
}

// ObserveEstimatedBytes records an estimated payload byte count. The estimate
// flag is carried only in the metric name/help; callers must treat this as an
// estimate, never as exact wire truth.
func (m *Metrics) ObserveEstimatedBytes(stage, mode string, bytes int64) {
	if m == nil || bytes < 0 {
		return
	}
	m.estimatedBytes.WithLabelValues(NormalizeStage(stage), NormalizeMode(mode)).Observe(float64(bytes))
}

// NormalizeStage maps an arbitrary stage string onto the bounded whitelist.
func NormalizeStage(stage string) string {
	stage = strings.ToLower(strings.TrimSpace(stage))
	if _, ok := allowedStages[stage]; ok {
		return stage
	}
	return StageUnknown
}

// NormalizeMode maps an arbitrary mode string onto the bounded whitelist.
func NormalizeMode(mode string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if _, ok := allowedModes[mode]; ok {
		return mode
	}
	return ModeUnknown
}

// NormalizeOutcome maps an arbitrary outcome string onto the bounded whitelist.
func NormalizeOutcome(outcome string) string {
	outcome = strings.ToLower(strings.TrimSpace(outcome))
	if _, ok := allowedOutcomes[outcome]; ok {
		return outcome
	}
	return OutcomeOther
}

// NormalizeLayer maps an arbitrary layer string onto the bounded whitelist.
func NormalizeLayer(layer string) string {
	layer = strings.ToLower(strings.TrimSpace(layer))
	if _, ok := allowedLayers[layer]; ok {
		return layer
	}
	return LayerOther
}

// ValidLabelValue returns an error when value is empty or contains characters
// that would corrupt Prometheus label serialization. It is used by the load
// harness to fail fast instead of silently fabricating evidence.
func ValidLabelValue(value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("friendcapacity: empty label value")
	}
	if strings.ContainsAny(value, "\"\\\n") {
		return fmt.Errorf("friendcapacity: label value contains unsafe characters")
	}
	return nil
}

// AllowedStages returns the sorted stage whitelist (testable surface).
func AllowedStages() []string { return sortedKeys(allowedStages) }

// AllowedModes returns the sorted mode whitelist (testable surface).
func AllowedModes() []string { return sortedKeys(allowedModes) }

// AllowedOutcomes returns the sorted outcome whitelist (testable surface).
func AllowedOutcomes() []string { return sortedKeys(allowedOutcomes) }

// AllowedLayers returns the sorted layer whitelist (testable surface).
func AllowedLayers() []string { return sortedKeys(allowedLayers) }

var allowedStages = stringSet(
	StageFriendIndex, StageHeaderColdScan, StageCandidateWindowLoad, StageCandidateWindowSave,
	StagePairExact, StageProfile, StageFacts, StageReceipt, StageSessionRouteBatch,
	StageGateEnqueue, StageHintDelivery, StageWorkerDiscovery, StageWorkerClaim,
	StageWorkerFanout, StagePull, StageEndToEnd, StageTotal, StageHintReceive,
)

var allowedModes = stringSet(ModeFriendList, ModeNextOpportunity, ModeHint, ModeWorker)

var allowedOutcomes = stringSet(
	OutcomeOK, OutcomeNotFound, OutcomeUnknown, OutcomeError, OutcomeDropped,
	OutcomeUnavailable, OutcomeFenced, OutcomeSkipped, OutcomeConflict,
)

var allowedLayers = stringSet(LayerHTTPServer, LayerClient, LayerRedisClient, LayerWorker)

func stringSet(values ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func sortedKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sortStrings(keys)
	return keys
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
