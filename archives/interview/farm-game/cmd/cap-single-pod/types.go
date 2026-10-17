package main

import (
	"time"

	"farmgame/internal/testkit/capdriver"
)

const (
	VerdictPassSafeTier              = "PASS_SAFE_TIER"
	VerdictFailCorrectness           = "FAIL_CORRECTNESS"
	VerdictFailTargetSaturation      = "FAIL_TARGET_SATURATION"
	VerdictInconclusiveDriver        = "INCONCLUSIVE_LOAD_GENERATOR"
	VerdictInconclusiveDependency    = "INCONCLUSIVE_DEPENDENCY"
	VerdictInconclusiveObservability = "INCONCLUSIVE_OBSERVABILITY"
	VerdictInconclusiveEnvironment   = "INCONCLUSIVE_ENVIRONMENT"

	PhaseStatusPass         = "PASS"
	PhaseStatusFail         = "FAIL"
	PhaseStatusInconclusive = "INCONCLUSIVE"
)

type target struct {
	Kind  string  `json:"kind"`
	Value float64 `json:"value"`
}

type runPlan struct {
	SchemaVersion                  string            `json:"schema_version"`
	RunID                          string            `json:"run_id"`
	Service                        string            `json:"service"`
	Profile                        string            `json:"profile"`
	Phase                          string            `json:"phase"`
	Tier                           int               `json:"tier"`
	DurationSeconds                int               `json:"duration_seconds"`
	RampSeconds                    int               `json:"ramp_seconds"`
	Targets                        []target          `json:"targets"`
	MaximumLaunchLagMS             int64             `json:"maximum_launch_lag_ms"`
	MaximumInflight                int               `json:"maximum_inflight"`
	DriverURL                      string            `json:"driver_url"`
	DriverHealthURL                string            `json:"driver_health_url"`
	SnapshotURL                    string            `json:"snapshot_url"`
	CollectorHealthURL             string            `json:"collector_health_url"`
	OracleURL                      string            `json:"oracle_url"`
	OracleHealthURL                string            `json:"oracle_health_url"`
	CollectorConfigSHA256          string            `json:"collector_config_sha256"`
	OracleConfigSHA256             string            `json:"oracle_config_sha256"`
	DriverBinarySHA256             string            `json:"driver_binary_sha256"`
	ManifestPath                   string            `json:"manifest_path"`
	ManifestSchemaPath             string            `json:"manifest_schema_path"`
	BindingPath                    string            `json:"binding_path"`
	ContractPath                   string            `json:"contract_path"`
	RequiredMetricsPath            string            `json:"required_metrics_path"`
	StopConditionsPath             string            `json:"stop_conditions_path"`
	VerdictRulesPath               string            `json:"verdict_rules_path"`
	EnvironmentPath                string            `json:"environment_validity_path"`
	ProfilePath                    string            `json:"profile_path"`
	LoadGeneratorQualificationPath string            `json:"load_generator_qualification_path"`
	SampleIntervalMS               int               `json:"sample_interval_ms"`
	RequestTimeoutMS               int               `json:"request_timeout_ms"`
	AllowedErrorCodes              []string          `json:"allowed_error_codes"`
	Metadata                       map[string]string `json:"metadata,omitempty"`
}

// The operation wire types are the driver contract itself, not a restatement
// of it. Both ends reject unknown fields, so a field name that drifts on one
// side fails every operation of a run.
type operationRequest = capdriver.OperationRequest

type operationResponse = capdriver.OperationResponse

type operationResult struct {
	Sequence      uint64           `json:"sequence"`
	ScheduledAt   time.Time        `json:"-"`
	LaunchedAt    time.Time        `json:"-"`
	CompletedAt   time.Time        `json:"-"`
	Status        string           `json:"status"`
	ErrorCode     string           `json:"error_code,omitempty"`
	QueueItems    float64          `json:"queue_items,omitempty"`
	Correctness   map[string]int64 `json:"correctness,omitempty"`
	RequestBytes  int              `json:"request_bytes"`
	ResponseBytes int              `json:"response_bytes"`
}

type resourceSnapshot struct {
	ObservedAt                string             `json:"observed_at"`
	ServiceCPUPercent         float64            `json:"service_cpu_percent"`
	ServiceRSSBytes           int64              `json:"service_rss_bytes"`
	ServiceMemoryPercent      float64            `json:"service_memory_percent_of_limit"`
	ServiceFDOpen             int64              `json:"service_fd_open"`
	DriverCPUPercent          float64            `json:"driver_cpu_percent"`
	DriverRSSBytes            int64              `json:"driver_rss_bytes"`
	DriverFDOpen              int64              `json:"driver_fd_open"`
	DependencyCPUPercent      float64            `json:"dependency_cpu_percent"`
	DependencyMemoryPercent   float64            `json:"dependency_memory_percent_of_limit"`
	PrimaryQueueItems         float64            `json:"primary_queue_items"`
	PrimaryQueueBytes         float64            `json:"primary_queue_bytes"`
	PrimaryQueueOldestSeconds float64            `json:"primary_queue_oldest_age_seconds"`
	RedisPoolWaitSeconds      float64            `json:"redis_pool_wait_seconds"`
	MetricValues              map[string]float64 `json:"metric_values"`
	MissingMetrics            []string           `json:"missing_metrics,omitempty"`
	ScrapeErrorCode           string             `json:"scrape_error_code,omitempty"`
}

type oracleSnapshot struct {
	ObservationComplete bool             `json:"observation_complete"`
	Gates               map[string]int64 `json:"gates"`
	ErrorCode           string           `json:"error_code,omitempty"`
}

type plannedActual struct {
	Planned              int64              `json:"planned_arrivals"`
	Launched             int64              `json:"launched_arrivals"`
	ScheduledUnsent      int64              `json:"scheduled_but_unsent"`
	Completed            int64              `json:"completed"`
	Rejected             int64              `json:"rejected"`
	Timeout              int64              `json:"timeout"`
	Failed               int64              `json:"failed"`
	ActualToPlannedRatio float64            `json:"actual_to_planned_ratio"`
	ArrivalLagMS         map[string]float64 `json:"arrival_lag_ms"`
	LatencyMS            map[string]float64 `json:"latency_ms"`
	RequestBytes         int64              `json:"request_bytes"`
	ResponseBytes        int64              `json:"response_bytes"`
}

type evaluation struct {
	Verdict                string             `json:"verdict,omitempty"`
	PhaseStatus            string             `json:"phase_status"`
	PhaseDecision          string             `json:"phase_decision"`
	Reasons                []string           `json:"reasons"`
	CorrectnessGates       map[string]int64   `json:"correctness_gates"`
	CorrectnessComplete    bool               `json:"correctness_observation_complete"`
	EnvironmentValid       bool               `json:"environment_valid"`
	ObservabilityComplete  bool               `json:"observability_complete"`
	LoadGeneratorQualified bool               `json:"load_generator_qualified"`
	DependencyFirst        bool               `json:"dependency_first_bottleneck"`
	TargetSaturated        bool               `json:"target_saturated"`
	QueueSlopePerSecond    float64            `json:"queue_slope_per_second"`
	QueueSlopeLower95      float64            `json:"queue_slope_lower_95ci"`
	QueueNetGrowth         float64            `json:"queue_net_growth"`
	Metrics                map[string]float64 `json:"metrics"`
}
