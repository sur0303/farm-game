package main

import (
	"time"

	"farmgame/internal/friend"
)

const (
	schemaVersion   = "cap-redis-l1-r1.v1"
	schemaVersionR2 = "cap-redis-l1-r2.v1"
)

type frozenIdentity struct {
	TaskID                string `json:"task_id"`
	BackendBusinessCommit string `json:"backend_business_commit"`
	BackendEvidenceHead   string `json:"backend_evidence_head"`
	H5Commit              string `json:"h5_commit"`
	BackendImageDigest    string `json:"backend_image_digest"`
	H5ImageDigest         string `json:"h5_image_digest"`
}

type runtimeIdentity struct {
	RunID              string            `json:"run_id"`
	RoundKind          string            `json:"round_kind"`
	RoundNumber        int               `json:"round_number"`
	StartedAtUTC       time.Time         `json:"started_at_utc"`
	CompletedAtUTC     time.Time         `json:"completed_at_utc"`
	RedisEndpointHash  string            `json:"redis_endpoint_hash"`
	RedisConfigHash    string            `json:"redis_config_hash"`
	Namespace          string            `json:"namespace"`
	MachineSpecHash    string            `json:"machine_spec_hash"`
	MachineSpec        map[string]string `json:"machine_spec"`
	CPUGovernor        string            `json:"cpu_governor"`
	ContainerCPULimit  string            `json:"container_cpu_limit"`
	ContainerMemory    string            `json:"container_memory_limit"`
	GoGC               string            `json:"go_gc"`
	GoMemLimit         string            `json:"go_mem_limit"`
	RedisPoolSize      int               `json:"redis_pool_size"`
	RedisMinIdleConns  int               `json:"redis_min_idle_conns"`
	ModelLayoutVersion string            `json:"model_layout_version"`
	RouteSnapshotHash  string            `json:"route_snapshot_hash"`
	LeaseProfileHash   string            `json:"lease_profile_hash"`
	ProfileHash        string            `json:"profile_hash"`
	RandomSeed         int64             `json:"random_seed"`
}

type percentileSet struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}

type commandCount struct {
	Name  string `json:"name"`
	Count uint64 `json:"count"`
}

type bucketDiagnostic struct {
	ReceiptCount     int64 `json:"receipt_count"`
	PendingWorkCount int64 `json:"pending_work_count"`
	AttemptCount     int64 `json:"attempt_count"`
	ManifestHLEN     int64 `json:"manifest_hlen"`
}

type dueDiagnostic struct {
	ReceiptDue int64 `json:"receipt_due"`
	SourceDue  int64 `json:"source_due"`
	AttemptDue int64 `json:"attempt_due"`
}

type failureDiagnostic struct {
	OperationIndex               int              `json:"operation_index"`
	OperationIDHash              string           `json:"operation_id_hash"`
	AuthorityKind                string           `json:"authority_kind"`
	AuthorityIDHash              string           `json:"authority_id_hash"`
	AuthorityRecordPresentBefore bool             `json:"authority_record_present_before"`
	AuthorityRecordVersionBefore uint64           `json:"authority_record_version_before"`
	AuthorityRecordPresent       bool             `json:"authority_record_present"`
	AuthorityRecordVersion       uint64           `json:"authority_record_version"`
	PairIDHash                   string           `json:"pair_id_hash,omitempty"`
	BucketID                     uint32           `json:"bucket_id"`
	ErrorClass                   string           `json:"error_class"`
	APIError                     string           `json:"api_error"`
	PairRecordPresentBefore      bool             `json:"pair_record_present_before"`
	PairRecordVersionBefore      uint64           `json:"pair_record_version_before"`
	PairRecordPresent            bool             `json:"pair_record_present"`
	PairStatus                   string           `json:"pair_status"`
	PairRelationEpoch            uint64           `json:"pair_relation_epoch"`
	PairRecordVersion            uint64           `json:"pair_record_version"`
	ReceiptPresent               bool             `json:"receipt_present"`
	ReceiptFieldCount            int64            `json:"receipt_field_count"`
	AttemptPresent               bool             `json:"attempt_present"`
	AttemptFieldCount            int64            `json:"attempt_field_count"`
	AttemptStates                []string         `json:"attempt_states"`
	BucketMeta                   bucketDiagnostic `json:"bucket_meta"`
	DueCounts                    dueDiagnostic    `json:"due_counts"`
	FinalClassification          string           `json:"final_classification"`
	ClassificationBasis          string           `json:"classification_basis"`
	DiagnosticError              string           `json:"diagnostic_error,omitempty"`
	EvidenceHash                 string           `json:"evidence_hash"`
}

type failureHashIndex struct {
	OperationIndex int    `json:"operation_index"`
	EvidenceHash   string `json:"evidence_hash"`
}

type friendSnapshotObservation struct {
	ScriptSHA                 string `json:"complete_snapshot_v2_script_sha"`
	ExpectedUniqueBucketCalls uint64 `json:"expected_unique_bucket_calls"`
	ObservedScriptCalls       uint64 `json:"observed_script_calls"`
	CallsMatchUniqueBuckets   bool   `json:"calls_match_unique_buckets"`
}

type cellResult struct {
	Operation                        string                     `json:"operation"`
	Group                            string                     `json:"group"`
	KeyDistribution                  string                     `json:"key_distribution"`
	Concurrency                      int                        `json:"concurrency"`
	PlannedOperations                int                        `json:"planned_operations"`
	CompletedOperations              int                        `json:"completed_operations"`
	FailedOperations                 int                        `json:"failed_operations"`
	CorrectnessViolations            int                        `json:"correctness_violations"`
	ExpectedFixtureDue               int64                      `json:"expected_fixture_due"`
	ObservedFixtureDue               int64                      `json:"observed_fixture_due"`
	RedisCommandsPerOp               float64                    `json:"redis_commands_per_op"`
	PipelineItemsPerOp               float64                    `json:"pipeline_items_per_op"`
	RequestBytesPerOp                float64                    `json:"request_bytes_per_op"`
	ResponseBytesPerOp               float64                    `json:"response_bytes_per_op"`
	LogicalRequestBytes              percentileSet              `json:"logical_request_bytes"`
	LogicalResponseBytes             percentileSet              `json:"logical_response_bytes"`
	PayloadBytes                     percentileSet              `json:"payload_bytes"`
	ClientLatencyUS                  percentileSet              `json:"client_latency_us"`
	LuaExecutionUS                   percentileSet              `json:"lua_execution_us"`
	NetworkRTTUS                     percentileSet              `json:"network_rtt_us"`
	RedisClientQueueUSPerOp          float64                    `json:"redis_client_queue_us_per_op"`
	SerializationUS                  percentileSet              `json:"serialization_us"`
	DeserializationUS                percentileSet              `json:"deserialization_us"`
	AllocsPerOp                      float64                    `json:"allocs_per_op"`
	ClientCPUUSPerOp                 float64                    `json:"client_cpu_us_per_op"`
	RedisCPUUSPerOp                  float64                    `json:"redis_cpu_us_per_op"`
	ConnectionsRebuilt               uint64                     `json:"connections_rebuilt"`
	Commands                         []commandCount             `json:"commands"`
	LuaCommands                      []string                   `json:"lua_commands"`
	OutcomeCounts                    map[string]int             `json:"outcome_counts"`
	FailureDiagnosticsExpected       int                        `json:"failure_diagnostics_expected"`
	FailureDiagnosticsCaptured       int                        `json:"failure_diagnostics_captured"`
	FailureDiagnosticsComplete       bool                       `json:"failure_diagnostics_complete"`
	FailureDiagnostics               []failureDiagnostic        `json:"failure_diagnostics,omitempty"`
	FailureEvidenceHashIndex         []failureHashIndex         `json:"failure_evidence_hash_index,omitempty"`
	FailureEvidenceAggregateHash     string                     `json:"failure_evidence_aggregate_hash,omitempty"`
	FailureDiagnosticCollectionError string                     `json:"failure_diagnostic_collection_error,omitempty"`
	FriendSnapshot                   *friendSnapshotObservation `json:"friend_complete_snapshot_v2,omitempty"`
	Notes                            []string                   `json:"notes,omitempty"`
}

type hardGates struct {
	DuplicateGrant              int `json:"duplicate_grant_total"`
	DuplicateDebit              int `json:"duplicate_debit_total"`
	StaleHolderCommitSuccess    int `json:"stale_holder_commit_success_total"`
	FarmVersionGap              int `json:"farm_version_gap_total"`
	WorkflowStepUnknownResidual int `json:"workflow_step_unknown_residual"`
	InvalidOrder                int `json:"invalid_order_total"`
	DueResidual                 int `json:"due_residual"`
	H5PendingJournalResidual    int `json:"h5_pending_journal_residual"`
	AuthorityClientMismatch     int `json:"authority_client_mismatch_total"`
	SilentDrop                  int `json:"silent_drop_total"`
	UnexpectedLeaseExpiry       int `json:"unexpected_lease_expiry_total"`
	FriendPairIndexUnrepaired   int `json:"friend_pair_index_unrepaired"`
	FriendAuthorityReconcile    int `json:"friend_authority_reconcile_required_total"`
	FriendSnapshotUnobserved    int `json:"friend_complete_snapshot_v2_unobserved_total"`
	FriendBatchBucketMismatch   int `json:"friend_batch_bucket_mismatch_total"`
	NamespaceResidual           int `json:"namespace_residual_total"`
}

func (g hardGates) total() int {
	return g.DuplicateGrant + g.DuplicateDebit + g.StaleHolderCommitSuccess + g.FarmVersionGap +
		g.WorkflowStepUnknownResidual + g.InvalidOrder + g.DueResidual + g.H5PendingJournalResidual +
		g.AuthorityClientMismatch + g.SilentDrop + g.UnexpectedLeaseExpiry + g.FriendPairIndexUnrepaired + g.FriendAuthorityReconcile +
		g.FriendSnapshotUnobserved + g.FriendBatchBucketMismatch + g.NamespaceResidual
}

type faultResult struct {
	Name                   string   `json:"name"`
	Verdict                string   `json:"verdict"`
	ObservedClassification string   `json:"observed_classification"`
	CorrectnessViolations  int      `json:"correctness_violations"`
	Notes                  []string `json:"notes,omitempty"`
}

type r2RoundAssertions struct {
	FriendCompleteSnapshotV2Observed   bool   `json:"friend_complete_snapshot_v2_observed"`
	FriendBatchExpectedUniqueBuckets   uint64 `json:"friend_batch_expected_unique_buckets"`
	FriendBatchObservedScriptCalls     uint64 `json:"friend_batch_observed_script_calls"`
	FriendBatchCallsMatchUniqueBuckets bool   `json:"friend_batch_calls_match_unique_buckets"`
}

type roundReport struct {
	SchemaVersion                  string             `json:"schema_version"`
	Purpose                        string             `json:"purpose"`
	BaselineEligible               bool               `json:"baseline_eligible"`
	Supports30MDAU                 bool               `json:"supports_30m_dau"`
	Frozen                         frozenIdentity     `json:"frozen_identity"`
	Runtime                        runtimeIdentity    `json:"runtime_identity"`
	Cells                          []cellResult       `json:"cells"`
	Faults                         []faultResult      `json:"faults"`
	HardGates                      hardGates          `json:"hard_correctness_gates"`
	CorrectnessObservationComplete bool               `json:"correctness_observation_complete"`
	ObservabilityGaps              []string           `json:"observability_required_gaps"`
	R2Assertions                   *r2RoundAssertions `json:"r2_hard_assertions,omitempty"`
	Verdict                        string             `json:"verdict"`
	VerdictReasons                 []string           `json:"verdict_reasons"`
}

type operationDiagnosticInput struct {
	OperationID                  string
	AuthorityKind                friend.FriendshipAuthorityKind
	AuthorityID                  string
	AuthorityBucketID            uint32
	AuthorityRecordPresentBefore bool
	AuthorityRecordVersionBefore uint64

	PairLowPlayerID  uint64
	PairHighPlayerID uint64

	// Exact bounded Redis locators are retained in memory only. They are never
	// serialized into evidence; persisted operation and authority identities
	// are domain-separated hashes.
	recordKey         string
	receiptKey        string
	receiptField      string
	attemptKey        string
	attemptStateField string
}

type sampleResult struct {
	Outcome              string
	LogicalRequestBytes  int
	LogicalResponseBytes int
	PayloadBytes         int
	SerializationTime    time.Duration
	DeserializationTime  time.Duration
	Diagnostic           *operationDiagnosticInput
}
