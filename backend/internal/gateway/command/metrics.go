package command

import (
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/gateway/commandcatalog"

	"github.com/prometheus/client_golang/prometheus"
)

// Bounded rejection reasons for command admission. These are internal metric
// labels only; client-visible reconnect reason codes remain Batch 3.
const (
	ReasonRouteLeaseExpired    = "ROUTE_LEASE_EXPIRED"
	ReasonStaleSession         = "STALE_SESSION"
	ReasonGateBootLeaseExpired = "GATE_BOOT_LEASE_EXPIRED"
	// ReasonUnknownCommand covers COMMAND_TYPE_UNSPECIFIED and any CommandType
	// missing from CommandCatalog (Batch 1 Slice A / contract 28 §6.2).
	ReasonUnknownCommand = "UNKNOWN_COMMAND"
	// Slice B SessionAdmissionPolicy target / subscription rejects (bounded labels).
	ReasonOwnCurrentFarmMismatch = "OWN_CURRENT_FARM_MISMATCH"
	ReasonCurrentFarmMismatch    = "CURRENT_FARM_MISMATCH"
	ReasonSubscriptionRequired   = "SUBSCRIPTION_REQUIRED"
	ReasonSubscriptionChanged    = "SUBSCRIPTION_CHANGED"
	ReasonRecoveryIncomplete     = "RECOVERY_INCOMPLETE"
	// Slice C / 1.5 payload admission rejects.
	ReasonPayloadTooLarge   = "PAYLOAD_TOO_LARGE"
	ReasonPayloadDecodeFail = "PAYLOAD_DECODE_FAILED"
	// ReasonCommandUnimplemented covers commands registered in CommandCatalog
	// whose AvailabilityPolicy is feature-disabled (business handler not wired).
	ReasonCommandUnimplemented = "COMMAND_UNIMPLEMENTED"
	// ReasonRouteKeyInvalid covers RouteKey extraction failures (zero id or
	// missing declared payload field) after typed decode.
	ReasonRouteKeyInvalid = "ROUTE_KEY_INVALID"
	// Backend boundary reject reasons for BackendRejected. BACKEND_SLOTS_FULL
	// is the bounded backend pool being exhausted; RPC_TIMEOUT / RPC_UNAVAILABLE
	// distinguish backend deadline vs transport/unavailable failures;
	// OUTCOME_INVALID covers malformed ExecuteCommand responses.
	ReasonBackendSlotsFull       = "BACKEND_SLOTS_FULL"
	ReasonBackendRPCTimeout      = "RPC_TIMEOUT"
	ReasonBackendRPCUnavail      = "RPC_UNAVAILABLE"
	ReasonBackendRouteGeneration = "ROUTE_GENERATION_TRANSITION"
	ReasonBackendStaleRoute      = "STALE_ROUTE"
	ReasonBackendOutcomeInvalid  = "OUTCOME_INVALID"
)

// Admission stage labels for CommandAdmission CounterVec.
const (
	StageAdmitted = "ADMITTED"
)

// Bounded labels for Subscription Lease outcomes (contract 26 §11.5). Status
// labels mirror the wire enum one-to-one; phase/reason are closed sets.
const (
	LeaseStatusUnspecified = "UNSPECIFIED"
	LeaseStatusRenewed     = "RENEWED"
	LeaseStatusNotFound    = "NOT_FOUND"
	LeaseStatusStaleFence  = "STALE_FENCE"
	LeaseStatusClosed      = "CLOSED"
	LeaseStatusRouteStale  = "ROUTE_STALE"
	LeaseStatusUnavailable = "UNAVAILABLE"

	LeasePhasePending = "pending"
	LeasePhaseActive  = "active"

	LeaseFailClosedUnsupportedLogic  = "UNSUPPORTED_LOGIC"
	LeaseFailClosedRPCError          = "RPC_ERROR"
	LeaseFailClosedMalformedResponse = "MALFORMED_RESPONSE"
)

// Metrics tracks Gate command-admission outcomes with bounded labels only.
type Metrics struct {
	CommandRejected  *prometheus.CounterVec
	CommandAdmission *prometheus.CounterVec
	// CommandResults is the client-visible command correctness ledger. It
	// records both Gate-generated outcomes and valid Logic business outcomes;
	// status and error_code are bounded protocol enums, not free-form text.
	CommandResults *prometheus.CounterVec
	// Phase breakdown of the command pipeline, keyed by command type.
	PrePermitSeconds *prometheus.HistogramVec // admission/decode/lookup before permit
	PermitSeconds    *prometheus.HistogramVec // session permit issuance (Redis round trips)
	RPCServerSeconds *prometheus.HistogramVec // logic.ExecuteCommand RPC round-trip
	PostSeconds      *prometheus.HistogramVec // response encode + subscription bookkeeping
	// ProtocolVersionRejected 统计被显式拒绝的旧协议版本客户端(决策 2 条件一:
	// 旧 H5 不得"碰巧解码一半")。
	ProtocolVersionRejected *prometheus.CounterVec
	// Subscription Lease (26 §11.5): batch volume, per-entry results, renew
	// latency, local fail-closed sweeps and their triggers.
	SubscriptionLeaseRenewBatches prometheus.Counter
	SubscriptionLeaseRenewEntries prometheus.Counter
	SubscriptionLeaseResults      *prometheus.CounterVec
	SubscriptionLeaseRenewSeconds prometheus.Histogram
	SubscriptionLeaseLocalExpired *prometheus.CounterVec
	SubscriptionLeaseFailClosed   *prometheus.CounterVec
	// Unified low-cardinality proof for both bounded renewal lanes. lane is
	// session_route/subscription; outcome is a closed internal set.
	LeaseEventsTotal          *prometheus.CounterVec
	LeaseInFlight             *prometheus.GaugeVec
	LeaseBatchItems           *prometheus.HistogramVec
	LeaseBatchEstimatedBytes  *prometheus.HistogramVec
	LeaseDispatchSeconds      *prometheus.HistogramVec
	LeaseBacklog              *prometheus.GaugeVec
	LeaseDueLagSeconds        *prometheus.HistogramVec
	LeaseHolderIsolationTotal *prometheus.CounterVec
	LeaseStaleNonResurrection *prometheus.CounterVec
	LeaseDrainTotal           *prometheus.CounterVec
	// ReceiptConflictLeaks is a hard correctness gate. Idempotency guards exist
	// to answer a retry with the original receipt; when the conflict itself
	// reaches the client, the guard failed at the only place that matters.
	ReceiptConflictLeaks *prometheus.CounterVec

	// BackendInFlight gauges how many WS commands currently hold a backend RPC
	// slot inside logic.ExecuteCommand. One login should hold exactly one slot
	// after the bootstrap-into-login merge; a multi-command login shows a
	// staircase of 2+ slots per connection instead.
	BackendInFlight prometheus.Gauge
	// BackendRejected counts commands that never reached logic.ExecuteCommand
	// or failed at the backend boundary, by bounded reason and command type.
	BackendRejected *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	buckets := []float64{.0001, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}
	metrics := &Metrics{
		CommandRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate",
			Subsystem: "command",
			Name:      "rejected_total",
			Help:      "Gate command admissions rejected by internal session reason.",
		}, []string{"reason"}),
		CommandAdmission: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate",
			Subsystem: "command",
			Name:      "admission_total",
			Help:      "Gate command admission pipeline stage counters (ADMITTED = passed all Gate checks before logic.ExecuteCommand).",
		}, []string{"stage"}),
		CommandResults: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate",
			Subsystem: "command",
			Name:      "results_total",
			Help:      "Client-visible command results by command type, protocol status, and bounded business error code.",
		}, []string{"command_type", "status", "error_code"}),
		PrePermitSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm_gate", Subsystem: "command", Name: "pre_permit_seconds",
			Help:    "Gate command pipeline: admission/decode/lookup before permit issuance.",
			Buckets: buckets,
		}, []string{"command_type"}),
		PermitSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm_gate", Subsystem: "command", Name: "permit_seconds",
			Help:    "Gate command pipeline: session permit issuance (Redis round trips).",
			Buckets: buckets,
		}, []string{"command_type"}),
		RPCServerSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm_gate", Subsystem: "command", Name: "rpc_server_seconds",
			Help:    "Gate command pipeline: logic.ExecuteCommand RPC round-trip.",
			Buckets: buckets,
		}, []string{"command_type"}),
		PostSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm_gate", Subsystem: "command", Name: "post_seconds",
			Help:    "Gate command pipeline: response encode + subscription bookkeeping.",
			Buckets: buckets,
		}, []string{"command_type"}),
		ProtocolVersionRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "command", Name: "protocol_version_rejected_total",
			Help: "Client frames rejected because FarmFrame.protocol_version is not the current wire version.",
		}, []string{"version"}),
		SubscriptionLeaseRenewBatches: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "subscription_lease", Name: "renew_batches_total",
			Help: "BatchRenewSubscriptions RPC batches sent to Farm Holders.",
		}),
		SubscriptionLeaseRenewEntries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "subscription_lease", Name: "renew_entries_total",
			Help: "Individual subscription lease entries sent inside renew batches.",
		}),
		SubscriptionLeaseResults: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "subscription_lease", Name: "results_total",
			Help: "Per-entry subscription lease renew results by wire status.",
		}, []string{"status"}),
		SubscriptionLeaseRenewSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "farm_gate", Subsystem: "subscription_lease", Name: "renew_duration_seconds",
			Help:    "BatchRenewSubscriptions RPC round-trip latency.",
			Buckets: buckets,
		}),
		SubscriptionLeaseLocalExpired: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "subscription_lease", Name: "local_expired_total",
			Help: "Subscriptions driven to a non-revivable terminal state by Gate-local lease expiry.",
		}, []string{"phase"}),
		SubscriptionLeaseFailClosed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "subscription_lease", Name: "fail_closed_total",
			Help: "Fail-closed sweeps triggered by renew errors or malformed Holder responses.",
		}, []string{"reason"}),
		LeaseEventsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "lease_batch", Name: "events_total",
			Help: "Lease renewal items/events by bounded lane and outcome (planned, attempted, succeeded, partial, lost, transient, oversize).",
		}, []string{"lane", "outcome"}),
		LeaseInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "farm_gate", Subsystem: "lease_batch", Name: "in_flight",
			Help: "Lease renewal items currently inside dispatch and fenced result processing.",
		}, []string{"lane"}),
		LeaseBatchItems: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm_gate", Subsystem: "lease_batch", Name: "items",
			Help:    "Items in each holder-isolated physical lease dispatch.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024},
		}, []string{"lane"}),
		LeaseBatchEstimatedBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm_gate", Subsystem: "lease_batch", Name: "estimated_bytes",
			Help:    "Conservative estimated wire/storage bytes in each planned lease dispatch.",
			Buckets: prometheus.ExponentialBuckets(256, 2, 14),
		}, []string{"lane"}),
		LeaseDispatchSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm_gate", Subsystem: "lease_batch", Name: "dispatch_duration_seconds",
			Help: "Physical Redis pipeline or Holder gRPC dispatch duration.", Buckets: buckets,
		}, []string{"lane"}),
		LeaseBacklog: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "farm_gate", Subsystem: "lease_batch", Name: "backlog",
			Help: "Pending indexed lease items; excludes in-flight items.",
		}, []string{"lane"}),
		LeaseDueLagSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm_gate", Subsystem: "lease_batch", Name: "due_lag_seconds",
			Help:    "Lag between deterministic due time and bounded planning.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 5.1, 10, 30, 60},
		}, []string{"lane"}),
		LeaseHolderIsolationTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "lease_batch", Name: "holder_isolated_total",
			Help: "Planned batches proven to contain one holder-safe partition; Subscription uses one farm and resolves the exact Holder generation outside Gate.mu before I/O.",
		}, []string{"lane"}),
		LeaseStaleNonResurrection: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "lease_batch", Name: "stale_non_resurrection_total",
			Help: "Late/stale planned or returned items discarded by the canonical generation fence.",
		}, []string{"lane"}),
		LeaseDrainTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "lease_batch", Name: "drain_total",
			Help: "Lease schedulers stopped and cleared at the Gate drain linearization point.",
		}, []string{"lane"}),
		ReceiptConflictLeaks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "command", Name: "receipt_conflict_leak_total",
			Help: "Idempotency conflicts delivered to the client, which leave the caller unable to learn the original outcome.",
		}, []string{"command_type"}),
		BackendInFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "farm_gate", Subsystem: "command", Name: "backend_in_flight",
			Help: "WS commands currently holding a backend RPC slot inside logic.ExecuteCommand.",
		}),
		BackendRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm_gate", Subsystem: "command", Name: "backend_rejected_total",
			Help: "Commands that never reached the backend or failed at the backend boundary, by bounded reason.",
		}, []string{"reason"}),
	}
	reg.MustRegister(metrics.CommandRejected, metrics.CommandAdmission, metrics.CommandResults,
		metrics.PrePermitSeconds, metrics.PermitSeconds, metrics.RPCServerSeconds, metrics.PostSeconds,
		metrics.ProtocolVersionRejected,
		metrics.SubscriptionLeaseRenewBatches, metrics.SubscriptionLeaseRenewEntries,
		metrics.SubscriptionLeaseResults, metrics.SubscriptionLeaseRenewSeconds,
		metrics.SubscriptionLeaseLocalExpired, metrics.SubscriptionLeaseFailClosed,
		metrics.LeaseEventsTotal, metrics.LeaseInFlight, metrics.LeaseBatchItems, metrics.LeaseBatchEstimatedBytes,
		metrics.LeaseDispatchSeconds, metrics.LeaseBacklog, metrics.LeaseDueLagSeconds,
		metrics.LeaseHolderIsolationTotal, metrics.LeaseStaleNonResurrection, metrics.LeaseDrainTotal,
		metrics.ReceiptConflictLeaks, metrics.BackendInFlight, metrics.BackendRejected)
	// A gate that has never fired must still be visible as zero: an absent
	// series reads as "never measured", which is not the same claim.
	for _, spec := range commandcatalog.All() {
		metrics.ReceiptConflictLeaks.WithLabelValues(spec.CommandType.String())
	}
	// Local subscription lease expiry is read as the unexpected-expiry gate,
	// because Disconnect removes a connection and its schedules before they can
	// expire. Reaching the deadline therefore means renewal failed under a
	// connection the Gate still held.
	metrics.SubscriptionLeaseLocalExpired.WithLabelValues(LeasePhaseActive)
	metrics.SubscriptionLeaseLocalExpired.WithLabelValues(LeasePhasePending)
	return metrics
}

// SubscriptionLeaseStatusLabel maps the wire enum to its bounded metric label.
func SubscriptionLeaseStatusLabel(status logicrpcpb.SubscriptionLeaseStatus) string {
	switch status {
	case logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED:
		return LeaseStatusRenewed
	case logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_NOT_FOUND:
		return LeaseStatusNotFound
	case logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_STALE_FENCE:
		return LeaseStatusStaleFence
	case logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_CLOSED:
		return LeaseStatusClosed
	case logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_ROUTE_STALE:
		return LeaseStatusRouteStale
	case logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_UNAVAILABLE:
		return LeaseStatusUnavailable
	default:
		return LeaseStatusUnspecified
	}
}
