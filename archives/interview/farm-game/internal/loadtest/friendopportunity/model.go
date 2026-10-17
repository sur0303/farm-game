package friendopportunity

import (
	"container/heap"
	"fmt"
	"math"
	"sort"
)

const syntheticDisclaimer = "SYNTHETIC MODEL ONLY: logical Redis operations/bytes are model estimates, not measurements from company Redis or a real Scheduler."

type LifecycleCounts struct {
	Planned   int64 `json:"planned"`
	Sent      int64 `json:"sent"`
	Completed int64 `json:"completed"`
	Rejected  int64 `json:"rejected"`
}

type LatencySummary struct {
	Count  int64   `json:"count"`
	MinMS  int64   `json:"min_ms"`
	MeanMS float64 `json:"mean_ms"`
	P50MS  int64   `json:"p50_ms"`
	P95MS  int64   `json:"p95_ms"`
	P99MS  int64   `json:"p99_ms"`
	MaxMS  int64   `json:"max_ms"`
}

type Metrics struct {
	FallbackPulls         int64          `json:"fallback_pulls"`
	ExactPullsAfterHint   int64          `json:"exact_pulls_after_hint"`
	PullCount             int64          `json:"pull_count"`
	EvaluatedFriends      int64          `json:"evaluated_friends"`
	ScheduleUpserts       int64          `json:"schedule_upserts"`
	ScheduleCancels       int64          `json:"schedule_cancels"`
	WakeupClaims          int64          `json:"wakeup_claims"`
	AuthorityHeaderReads  int64          `json:"authority_header_reads"`
	AuthorityFactsReads   int64          `json:"authority_facts_reads"`
	FriendIndexReads      int64          `json:"friend_index_reads"`
	SessionRouteReads     int64          `json:"session_route_reads"`
	OnlineHintTargets     int64          `json:"online_hint_targets"`
	GateBatches           int64          `json:"gate_batches"`
	LogicalRedisOps       int64          `json:"logical_redis_ops"`
	LogicalRedisBytes     int64          `json:"logical_redis_bytes"`
	EligibleBoundaries    int64          `json:"eligible_boundaries"`
	DetectedBoundaries    int64          `json:"detected_boundaries"`
	UndetectedBoundaries  int64          `json:"undetected_boundaries"`
	RealtimeDetections    int64          `json:"realtime_detections"`
	RealtimeDetectionRate float64        `json:"realtime_detection_rate"`
	DetectionLatency      LatencySummary `json:"detection_latency"`
}

type PollSavingsComparison struct {
	SafeFallbackPulls      int64 `json:"safe_fallback_pulls"`
	ActualFallbackPulls    int64 `json:"actual_fallback_pulls"`
	SavedFallbackPulls     int64 `json:"saved_fallback_pulls"`
	SavedLogicalRedisOps   int64 `json:"saved_logical_redis_ops"`
	SchedulerLogicalOps    int64 `json:"scheduler_logical_redis_ops"`
	SavedLogicalRedisBytes int64 `json:"saved_logical_redis_bytes"`
	SchedulerLogicalBytes  int64 `json:"scheduler_logical_redis_bytes"`
	CoversSchedulerOps     bool  `json:"poll_savings_cover_scheduler_ops"`
	CoversSchedulerBytes   bool  `json:"poll_savings_cover_scheduler_bytes"`
	NetLogicalOpsVsSafe    int64 `json:"net_logical_redis_ops_vs_safe_pull"`
	NetLogicalBytesVsSafe  int64 `json:"net_logical_redis_bytes_vs_safe_pull"`
}

type RunResult struct {
	SchemaVersion       string                 `json:"schema_version"`
	Synthetic           bool                   `json:"synthetic"`
	SyntheticDisclaimer string                 `json:"synthetic_disclaimer"`
	CostModelVersion    string                 `json:"cost_model_version"`
	Variant             Variant                `json:"variant"`
	ConfigFingerprint   string                 `json:"config_fingerprint"`
	TraceHashes         TraceHashes            `json:"trace_hashes"`
	FallbackIntervalMS  int64                  `json:"fallback_interval_ms"`
	SessionLifecycle    LifecycleCounts        `json:"session_lifecycle"`
	BoundaryLifecycle   LifecycleCounts        `json:"boundary_lifecycle"`
	Metrics             Metrics                `json:"metrics"`
	PollSavings         *PollSavingsComparison `json:"poll_savings,omitempty"`
}

func RunAll(c RunConfig, trace Trace) ([]RunResult, error) {
	variants := []Variant{VariantPull, VariantSchedulerSafe, VariantSchedulerEfficient}
	results := make([]RunResult, 0, len(variants))
	for _, variant := range variants {
		result, err := RunVariant(c, trace, variant)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func RunVariant(c RunConfig, trace Trace, variant Variant) (RunResult, error) {
	if err := c.Validate(); err != nil {
		return RunResult{}, err
	}
	if err := variant.Validate(); err != nil {
		return RunResult{}, err
	}
	if err := trace.Validate(c); err != nil {
		return RunResult{}, err
	}

	accepted, sessionLifecycle := simulateOpenLoop(c, trace)
	fallbackTimes := buildFallbackTimes(c, trace, accepted, variant.FallbackIntervalMS(c))
	metrics := Metrics{FallbackPulls: int64(len(fallbackTimes))}
	addFallbackCosts(c, &metrics)
	boundaryLifecycle := LifecycleCounts{Planned: int64(len(trace.BoundaryEvents))}
	if variant.UsesScheduler() {
		boundaryLifecycle.Sent = boundaryLifecycle.Planned
		boundaryLifecycle.Completed = boundaryLifecycle.Planned
		addSchedulerCosts(c, trace, &metrics)
	}
	addDetectionMetrics(c, trace, fallbackTimes, variant, &metrics)
	finalizeMetrics(c, &metrics)

	result := RunResult{
		SchemaVersion:       ResultSchemaVersion,
		Synthetic:           true,
		SyntheticDisclaimer: syntheticDisclaimer,
		CostModelVersion:    CostModelVersion,
		Variant:             variant,
		ConfigFingerprint:   c.Fingerprint(),
		TraceHashes:         trace.Hashes(),
		FallbackIntervalMS:  variant.FallbackIntervalMS(c),
		SessionLifecycle:    sessionLifecycle,
		BoundaryLifecycle:   boundaryLifecycle,
		Metrics:             metrics,
	}
	if variant == VariantSchedulerEfficient {
		result.PollSavings = pollSavingsComparison(c, trace, accepted, metrics)
	}
	return result, nil
}

func simulateOpenLoop(c RunConfig, trace Trace) ([]bool, LifecycleCounts) {
	accepted := make([]bool, len(trace.SessionRequests))
	lifecycle := LifecycleCounts{
		Planned: int64(len(trace.SessionRequests)),
		Sent:    int64(len(trace.SessionRequests)),
	}
	workers := &timeHeap{}
	for i := 0; i < c.WorkerConcurrency; i++ {
		heap.Push(workers, int64(0))
	}
	inFlight := &timeHeap{}
	serviceMS := pullServiceMS(c)
	for i, request := range trace.SessionRequests {
		for inFlight.Len() > 0 && (*inFlight)[0] <= request.PlannedAtMS {
			heap.Pop(inFlight)
		}
		if c.MaxInFlight > 0 && inFlight.Len() >= c.MaxInFlight {
			lifecycle.Rejected++
			continue
		}
		availableAt := heap.Pop(workers).(int64)
		startAt := request.PlannedAtMS
		if availableAt > startAt {
			startAt = availableAt
		}
		completedAt := startAt + serviceMS
		heap.Push(workers, completedAt)
		heap.Push(inFlight, completedAt)
		accepted[i] = true
		lifecycle.Completed++
	}
	return accepted, lifecycle
}

func pullServiceMS(c RunConfig) int64 {
	pages := (c.FriendCount + c.PullPageSize - 1) / c.PullPageSize
	microseconds := c.CostModel.PullBaseLatencyMS*1000 +
		int64(pages)*c.CostModel.PullPageLatencyMS*1000 +
		int64(c.FriendCount)*c.CostModel.PullFriendLatencyUS
	if microseconds < 1 {
		return 1
	}
	return (microseconds + 999) / 1000
}

func buildFallbackTimes(c RunConfig, trace Trace, accepted []bool, intervalMS int64) []int64 {
	times := make([]int64, 0, len(trace.SessionRequests))
	for i, request := range trace.SessionRequests {
		if !accepted[i] {
			continue
		}
		for at := request.PlannedAtMS; at < c.DurationMS; at += intervalMS {
			times = append(times, at)
			if at > math.MaxInt64-intervalMS {
				break
			}
		}
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	return times
}

func addFallbackCosts(c RunConfig, metrics *Metrics) {
	pulls := metrics.FallbackPulls
	metrics.FriendIndexReads += pulls
	metrics.AuthorityHeaderReads += pulls * int64(c.FriendCount)
	metrics.AuthorityFactsReads += pulls * int64(c.FriendCount)
	metrics.EvaluatedFriends += pulls * int64(c.FriendCount)
}

func addSchedulerCosts(c RunConfig, trace Trace, metrics *Metrics) {
	for _, event := range trace.BoundaryEvents {
		switch event.ScheduleOperation {
		case ScheduleCancel:
			metrics.ScheduleCancels++
			continue
		case ScheduleUpsert:
			metrics.ScheduleUpserts++
		}
		metrics.WakeupClaims++
		metrics.AuthorityHeaderReads++
		metrics.AuthorityFactsReads++
		metrics.FriendIndexReads++
		targets := int64(len(event.OnlineTargetIDs))
		metrics.OnlineHintTargets += targets
		metrics.SessionRouteReads += targets
		metrics.ExactPullsAfterHint += targets
		metrics.AuthorityHeaderReads += targets
		metrics.AuthorityFactsReads += targets
		metrics.EvaluatedFriends += targets
		if targets > 0 {
			metrics.GateBatches += (targets + int64(c.GateBatchSize) - 1) / int64(c.GateBatchSize)
		}
	}
}

func addDetectionMetrics(c RunConfig, trace Trace, fallbackTimes []int64, variant Variant, metrics *Metrics) {
	latencies := make([]int64, 0, len(trace.BoundaryEvents))
	for _, event := range trace.BoundaryEvents {
		if event.ScheduleOperation != ScheduleUpsert {
			continue
		}
		metrics.EligibleBoundaries++
		if variant.UsesScheduler() && len(event.OnlineTargetIDs) > 0 {
			latencies = append(latencies, c.CostModel.HintDeliveryLatencyMS+c.CostModel.ExactPullAfterHintLatencyMS)
			metrics.DetectedBoundaries++
			metrics.RealtimeDetections++
			continue
		}
		index := sort.Search(len(fallbackTimes), func(i int) bool {
			return fallbackTimes[i] >= event.BoundaryAtMS
		})
		if index == len(fallbackTimes) {
			metrics.UndetectedBoundaries++
			continue
		}
		latencies = append(latencies, fallbackTimes[index]-event.BoundaryAtMS)
		metrics.DetectedBoundaries++
	}
	if metrics.EligibleBoundaries > 0 {
		metrics.RealtimeDetectionRate = float64(metrics.RealtimeDetections) / float64(metrics.EligibleBoundaries)
	}
	metrics.DetectionLatency = summarizeLatency(latencies)
}

func finalizeMetrics(c RunConfig, metrics *Metrics) {
	metrics.PullCount = metrics.FallbackPulls + metrics.ExactPullsAfterHint
	metrics.LogicalRedisOps = metrics.FriendIndexReads + metrics.AuthorityHeaderReads +
		metrics.AuthorityFactsReads + metrics.ScheduleUpserts + metrics.ScheduleCancels +
		metrics.WakeupClaims + metrics.SessionRouteReads
	metrics.LogicalRedisBytes =
		metrics.FriendIndexReads*(c.CostModel.FriendIndexReadBaseBytes+int64(c.FriendCount)*c.CostModel.FriendIndexFriendBytes) +
			metrics.AuthorityHeaderReads*c.CostModel.AuthorityHeaderReadBytes +
			metrics.AuthorityFactsReads*c.CostModel.AuthorityFactsReadBytes +
			(metrics.ScheduleUpserts+metrics.ScheduleCancels)*c.CostModel.ScheduleWriteBytes +
			metrics.WakeupClaims*c.CostModel.WakeupClaimBytes +
			metrics.SessionRouteReads*c.CostModel.SessionRouteReadBytes
}

func pollSavingsComparison(c RunConfig, trace Trace, accepted []bool, actual Metrics) *PollSavingsComparison {
	safePulls := int64(len(buildFallbackTimes(c, trace, accepted, c.SafeFallbackIntervalMS)))
	savedPulls := safePulls - actual.FallbackPulls
	perPullOps := int64(1 + 2*c.FriendCount)
	perPullBytes := c.CostModel.FriendIndexReadBaseBytes +
		int64(c.FriendCount)*c.CostModel.FriendIndexFriendBytes +
		int64(c.FriendCount)*(c.CostModel.AuthorityHeaderReadBytes+c.CostModel.AuthorityFactsReadBytes)
	fallbackOps := actual.FallbackPulls * perPullOps
	fallbackBytes := actual.FallbackPulls * perPullBytes
	schedulerOps := actual.LogicalRedisOps - fallbackOps
	schedulerBytes := actual.LogicalRedisBytes - fallbackBytes
	savedOps := savedPulls * perPullOps
	savedBytes := savedPulls * perPullBytes
	return &PollSavingsComparison{
		SafeFallbackPulls:      safePulls,
		ActualFallbackPulls:    actual.FallbackPulls,
		SavedFallbackPulls:     savedPulls,
		SavedLogicalRedisOps:   savedOps,
		SchedulerLogicalOps:    schedulerOps,
		SavedLogicalRedisBytes: savedBytes,
		SchedulerLogicalBytes:  schedulerBytes,
		CoversSchedulerOps:     savedOps >= schedulerOps,
		CoversSchedulerBytes:   savedBytes >= schedulerBytes,
		NetLogicalOpsVsSafe:    schedulerOps - savedOps,
		NetLogicalBytesVsSafe:  schedulerBytes - savedBytes,
	}
}

func summarizeLatency(values []int64) LatencySummary {
	if len(values) == 0 {
		return LatencySummary{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	var total int64
	for _, value := range values {
		total += value
	}
	return LatencySummary{
		Count:  int64(len(values)),
		MinMS:  values[0],
		MeanMS: float64(total) / float64(len(values)),
		P50MS:  percentile(values, 0.50),
		P95MS:  percentile(values, 0.95),
		P99MS:  percentile(values, 0.99),
		MaxMS:  values[len(values)-1],
	}
}

func percentile(values []int64, quantile float64) int64 {
	if len(values) == 0 {
		return 0
	}
	index := int(math.Ceil(quantile*float64(len(values)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

type timeHeap []int64

func (h timeHeap) Len() int           { return len(h) }
func (h timeHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h timeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *timeHeap) Push(value any)    { *h = append(*h, value.(int64)) }
func (h *timeHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

func ResultsByVariant(results []RunResult) (map[Variant]RunResult, error) {
	indexed := make(map[Variant]RunResult, len(results))
	for _, result := range results {
		if _, exists := indexed[result.Variant]; exists {
			return nil, fmt.Errorf("friendopportunity: duplicate result for %s", result.Variant)
		}
		indexed[result.Variant] = result
	}
	return indexed, nil
}
