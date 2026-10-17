package main

import (
	"math"
	"sort"
)

type slopeResult struct {
	SlopePerSecond float64 `json:"slope_per_second"`
	Lower95        float64 `json:"lower_95ci"`
	NetGrowth      float64 `json:"net_growth"`
	Samples        int     `json:"samples"`
	Positive       bool    `json:"positive"`
}

func queueSlope(samples []resourceSnapshot) slopeResult {
	return snapshotSlope(samples, func(sample resourceSnapshot) float64 { return sample.PrimaryQueueItems }, 5)
}

func metricSlope(samples []resourceSnapshot, name string) slopeResult {
	return snapshotSlope(samples, func(sample resourceSnapshot) float64 { return sample.MetricValues[name] }, 0)
}

func snapshotSlope(samples []resourceSnapshot, valueOf func(resourceSnapshot) float64, minimumNetGrowth float64) slopeResult {
	if len(samples) > 60 {
		samples = samples[len(samples)-60:]
	}
	n := len(samples)
	result := slopeResult{Samples: n}
	if n < 3 {
		return result
	}
	meanX, meanY := float64(n-1)/2, 0.0
	for _, sample := range samples {
		meanY += valueOf(sample)
	}
	meanY /= float64(n)
	var sxx, sxy float64
	for index, sample := range samples {
		x := float64(index)
		dx := x - meanX
		sxx += dx * dx
		sxy += dx * (valueOf(sample) - meanY)
	}
	if sxx == 0 {
		return result
	}
	slope := sxy / sxx
	var residual float64
	for index, sample := range samples {
		predicted := meanY + slope*(float64(index)-meanX)
		difference := valueOf(sample) - predicted
		residual += difference * difference
	}
	standardError := math.Sqrt((residual / float64(n-2)) / sxx)
	result.SlopePerSecond = slope
	result.Lower95 = slope - 1.96*standardError
	result.NetGrowth = valueOf(samples[n-1]) - valueOf(samples[0])
	netGrowthEnough := result.NetGrowth > 0
	if minimumNetGrowth > 0 {
		netGrowthEnough = result.NetGrowth >= minimumNetGrowth
	}
	result.Positive = n >= 55 && result.Lower95 > 0 && netGrowthEnough
	return result
}

func evaluateRun(plan runPlan, actual plannedActual, operationGates map[string]int64, samples []resourceSnapshot,
	oracleGates map[string]int64, oracleComplete, observabilityComplete bool, runtimeStop string) evaluation {
	gates := make(map[string]int64)
	for name, value := range operationGates {
		if value > gates[name] {
			gates[name] = value
		}
	}
	for name, value := range oracleGates {
		if value > gates[name] {
			gates[name] = value
		}
	}
	slope := queueSlope(samples)
	metrics := aggregateMetrics(samples)
	result := evaluation{
		PhaseStatus: PhaseStatusPass, PhaseDecision: phasePassDecision(plan.Phase),
		CorrectnessGates: gates, CorrectnessComplete: oracleComplete,
		EnvironmentValid: true, ObservabilityComplete: observabilityComplete,
		LoadGeneratorQualified: true, QueueSlopePerSecond: slope.SlopePerSecond,
		QueueSlopeLower95: slope.Lower95, QueueNetGrowth: slope.NetGrowth, Metrics: metrics,
	}
	if anyNonzero(gates) {
		setFailure(&result, VerdictFailCorrectness, "HARD_CORRECTNESS_GATE_NONZERO")
		return result
	}
	if environmentInvalid(metrics) || runtimeStop == VerdictInconclusiveEnvironment {
		setFailure(&result, VerdictInconclusiveEnvironment, "ENVIRONMENT_INVALID")
		result.EnvironmentValid = false
		return result
	}
	if !observabilityComplete || !oracleComplete || runtimeStop == VerdictInconclusiveObservability {
		setFailure(&result, VerdictInconclusiveObservability, "OBSERVATION_INCOMPLETE")
		result.ObservabilityComplete = false
		return result
	}
	if plan.Phase == "LOAD_GENERATOR_QUALIFICATION" && slope.Samples < minimumLoadGeneratorQualificationSamples {
		setFailure(&result, VerdictInconclusiveObservability, "LOAD_GENERATOR_SLOPE_OBSERVATION_INCOMPLETE")
		result.ObservabilityComplete = false
		return result
	}
	minimumRatio, driverCPUMax := .95, 80.0
	if plan.Phase == "LOAD_GENERATOR_QUALIFICATION" || isFormalPhase(plan.Phase) {
		minimumRatio = .98
	}
	if plan.Phase == "LOAD_GENERATOR_QUALIFICATION" {
		driverCPUMax = 70
	}
	if actual.ActualToPlannedRatio < minimumRatio || actual.ArrivalLagMS["p99"] > 50 || metrics["driver_cpu_percent_max"] > driverCPUMax || runtimeStop == VerdictInconclusiveDriver {
		setFailure(&result, VerdictInconclusiveDriver, "LOAD_GENERATOR_NOT_QUALIFIED")
		result.LoadGeneratorQualified = false
		return result
	}
	if metrics["dependency_cpu_percent_max"] >= 85 || metrics["dependency_memory_percent_max"] >= 90 || runtimeStop == VerdictInconclusiveDependency {
		setFailure(&result, VerdictInconclusiveDependency, "DEPENDENCY_FIRST_BOTTLENECK")
		result.DependencyFirst = true
		return result
	}
	attempted := actual.Completed + actual.Rejected + actual.Timeout + actual.Failed
	badPercent := 0.0
	if attempted > 0 {
		badPercent = 100 * float64(actual.Rejected+actual.Timeout+actual.Failed) / float64(attempted)
	}
	if metrics["service_cpu_percent_max"] >= 90 || metrics["service_memory_percent_of_limit_max"] >= 90 || slope.Positive || badPercent >= 5 || runtimeStop == VerdictFailTargetSaturation {
		setFailure(&result, VerdictFailTargetSaturation, "MEASURED_TARGET_SATURATED")
		result.TargetSaturated = true
		return result
	}
	if isCandidateOrFormal(plan.Phase) && !safeTierObservationComplete(metrics) {
		setFailure(&result, VerdictInconclusiveObservability, "SAFE_TIER_OBSERVATION_INCOMPLETE")
		result.ObservabilityComplete = false
		return result
	}
	result.Reasons = append(result.Reasons, "ALL_APPLICABLE_PHASE_GATES_PASS")
	return result
}

func setFailure(result *evaluation, verdict, reason string) {
	result.Verdict = verdict
	if verdict == VerdictFailCorrectness || verdict == VerdictFailTargetSaturation {
		result.PhaseStatus = PhaseStatusFail
	} else {
		result.PhaseStatus = PhaseStatusInconclusive
	}
	result.PhaseDecision = ""
	result.Reasons = append(result.Reasons, reason)
}

func isFormalPhase(phase string) bool {
	return phase == "FORMAL_1" || phase == "FORMAL_2" || phase == "FORMAL_3"
}
func isCandidateOrFormal(phase string) bool {
	return phase == "SAFE_TIER_CANDIDATE" || isFormalPhase(phase)
}

func safeTierObservationComplete(metrics map[string]float64) bool {
	required := []string{"controlled_drain_pass_max", "p99_sustained_degradation_max", "p99_stability_observed_seconds_max",
		"measured_service_oom_kill_total_max", "measured_service_fd_percent_of_limit_max"}
	for _, name := range required {
		if _, found := metrics[name]; !found {
			return false
		}
	}
	// A zero p99_sustained_degradation says the tier was stable only if the tier
	// watched its own p99 for at least as long as the degraded stretch that would
	// have condemned it. Without that, the zero is silence, not stability.
	return metrics["controlled_drain_pass_max"] == 1 && metrics["p99_sustained_degradation_max"] == 0 &&
		metrics["p99_stability_observed_seconds_max"] >= p99SustainedDegradationSeconds &&
		metrics["measured_service_oom_kill_total_max"] == 0 && metrics["measured_service_fd_percent_of_limit_max"] < 85
}

func phasePassDecision(phase string) string {
	switch phase {
	case "PRECHECK":
		return "PASS_PRECHECK"
	case "OBSERVABILITY_QUALIFICATION":
		return "PASS_OBSERVABILITY_QUALIFICATION"
	case "LOAD_GENERATOR_QUALIFICATION":
		return "PASS_LOAD_GENERATOR_QUALIFICATION"
	case "SMOKE":
		return "PASS_SMOKE"
	case "WARMUP":
		return "PASS_WARMUP"
	case "STAIRCASE":
		return "PASS_STAIRCASE_TIER"
	case "SAFE_TIER_CANDIDATE":
		return "PASS_SAFE_TIER_CANDIDATE"
	case "FORMAL_1", "FORMAL_2", "FORMAL_3":
		return "PASS_FORMAL_ROUND"
	case "COOLDOWN_1", "COOLDOWN_2":
		return "PASS_COOLDOWN"
	case "INDEPENDENT_REVIEW":
		return "PASS_REVIEW_INPUT_VALIDATION"
	default:
		return "PASS_PHASE"
	}
}

func anyNonzero(values map[string]int64) bool {
	for _, value := range values {
		if value != 0 {
			return true
		}
	}
	return false
}

func environmentInvalid(metrics map[string]float64) bool {
	return metrics["identity_drift_max"] > 0 || metrics["cgroup_limit_drift_max"] > 0 ||
		metrics["host_clock_step_ms_max"] > 50 || metrics["observer_cpu_percent_max"] > 10 || metrics["unexpected_container_restart_total_max"] > 0 ||
		metrics["namespace_residual_before_run_max"] > 0
}

func aggregateMetrics(samples []resourceSnapshot) map[string]float64 {
	values := map[string]float64{}
	for _, sample := range samples {
		max(values, "service_cpu_percent_max", sample.ServiceCPUPercent)
		max(values, "service_rss_bytes_max", float64(sample.ServiceRSSBytes))
		max(values, "service_memory_percent_of_limit_max", sample.ServiceMemoryPercent)
		max(values, "driver_cpu_percent_max", sample.DriverCPUPercent)
		max(values, "driver_rss_bytes_max", float64(sample.DriverRSSBytes))
		max(values, "dependency_cpu_percent_max", sample.DependencyCPUPercent)
		max(values, "dependency_memory_percent_max", sample.DependencyMemoryPercent)
		max(values, "primary_queue_items_max", sample.PrimaryQueueItems)
		for name, value := range sample.MetricValues {
			max(values, name+"_max", value)
		}
	}
	return values
}

func max(values map[string]float64, key string, value float64) {
	if current, found := values[key]; !found || value > current {
		values[key] = value
	}
}

func stableGateNames(values map[string]int64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
