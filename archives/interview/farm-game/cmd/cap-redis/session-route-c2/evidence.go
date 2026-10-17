package main

import "time"

const (
	evidenceSchemaVersion = "session-route-c2.v1"
	topologySingleNode    = "SINGLE_NODE_ONLY"
	topologyClusterNV     = "NOT_VERIFIED"
	fieldUnavailable      = "unavailable"
)

type optionalFloat struct {
	Available bool    `json:"available"`
	Value     float64 `json:"value,omitempty"`
	Reason    string  `json:"reason,omitempty"`
}

func measuredFloat(value float64) optionalFloat {
	return optionalFloat{Available: true, Value: value}
}

func unavailableFloat(reason string) optionalFloat {
	return optionalFloat{Available: false, Reason: reason}
}

type optionalUint64 struct {
	Available bool   `json:"available"`
	Value     uint64 `json:"value,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

func measuredUint64(value uint64) optionalUint64 {
	return optionalUint64{Available: true, Value: value}
}

func unavailableUint64(reason string) optionalUint64 {
	return optionalUint64{Available: false, Reason: reason}
}

type cellEvidence struct {
	CellName                string         `json:"cell_name"`
	FriendCount             int            `json:"friend_count"`
	OnlineRatioPct          int            `json:"online_ratio_pct"`
	DispatchBatchSize       int            `json:"dispatch_batch_size"`
	RouteStateMode          string         `json:"route_state_mode"`
	Iterations              int            `json:"iterations"`
	ExpectedActive          int            `json:"expected_active"`
	ExpectedFiltered        int            `json:"expected_filtered"`
	ExpectedMalformed       int            `json:"expected_malformed"`
	ObservedActive          int            `json:"observed_active"`
	ObservedFiltered        int            `json:"observed_filtered"`
	ObservedMalformed       optionalUint64 `json:"observed_malformed"`
	SuccessfulCount         int            `json:"successful_count"`
	FilteredCount           int            `json:"filtered_count"`
	MalformedCount          optionalUint64 `json:"malformed_count"`
	EnqueuedCount           int            `json:"enqueued_count"`
	DeliveredCount          int            `json:"delivered_count"`
	ObservedRPCBatchSizes   []int          `json:"observed_rpc_batch_sizes"`
	RPCBatchSizePercentiles percentileSet  `json:"rpc_batch_size_percentiles"`
	LatencyUS               percentileSet  `json:"latency_us"`
	RedisCommands           optionalUint64 `json:"redis_command_count"`
	PipelineItemCount       optionalUint64 `json:"pipeline_item_count"`
	RequestBytes            optionalUint64 `json:"request_bytes"`
	ResponseBytes           optionalUint64 `json:"response_bytes"`
	RedisCPUUS              optionalFloat  `json:"redis_cpu_us"`
	Slowlog                 optionalFloat  `json:"slowlog"`
	TopologyLabel           string         `json:"topology_label"`
	ClusterVerification     string         `json:"cluster_verification"`
	CorrectnessOK           bool           `json:"correctness_ok"`
	Violations              []string       `json:"violations,omitempty"`
	Notes                   []string       `json:"notes,omitempty"`
}

type probeReport struct {
	SchemaVersion       string         `json:"schema_version"`
	Purpose             string         `json:"purpose"`
	RunID               string         `json:"run_id"`
	StartedAtUTC        time.Time      `json:"started_at_utc"`
	CompletedAtUTC      time.Time      `json:"completed_at_utc"`
	Mode                string         `json:"mode"`
	TopologyLabel       string         `json:"topology_label"`
	ClusterVerification string         `json:"cluster_verification"`
	Supports30MDAU      bool           `json:"supports_30m_dau"`
	GoNoGo              string         `json:"go_no_go"`
	CorrectnessOK       bool           `json:"correctness_ok"`
	Cells               []cellEvidence `json:"cells"`
	Notes               []string       `json:"notes"`
}
