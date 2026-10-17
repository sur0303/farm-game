// Package commandcatalog is the server-side static CommandCatalog for
// contract 28 §6. It is the single registry of the 33 public commands
// (L2-PROTO-37/45): Gate and FirstHop share the schema; there is no second
// runtime catalog and no legacy 12-command mainline.
package commandcatalog

import (
	commonpb "farmgame/api/gen/common"
	"farmgame/internal/gateway/session"

	"google.golang.org/protobuf/proto"
)

// MaxPayloadBytesUnlimited marks a command with no per-command payload cap.
// Specs must use either a positive MaxPayloadBytes or this constant (never 0).
const MaxPayloadBytesUnlimited = -1

// DefaultMaxPayloadBytes is the conservative P0 per-command payload cap.
// Per-command cfg.max_payload.* probe values refine it without schema change.
const DefaultMaxPayloadBytes = 64 << 10

// OpenEndedProtocolVersion marks MaxProtocolVersion as open-ended
// (28 §6.1: ProtocolRange P0_BASE..OPEN_ENDED).
const OpenEndedProtocolVersion uint32 = 0

// TargetFarmPolicy constrains how a command's farm target is derived/checked
// (28 §6.1 Session 准入与目标策略). Values are mutually exclusive
// alternatives, not ordered ranks.
type TargetFarmPolicy uint8

const (
	TargetFarmNone TargetFarmPolicy = iota
	// TargetFarmPayloadTarget: JOIN_FARM,目标只来自 Typed Payload。
	TargetFarmPayloadTarget
	// TargetFarmSessionPlayer: 操作者/路由主体只来自 TrustedCommandContext.session.player_id。
	TargetFarmSessionPlayer
	// TargetFarmOwnCurrentFarm: target == admitted_farm == session.player_id。
	TargetFarmOwnCurrentFarm
	// TargetFarmCurrentFarm: target == admitted_farm(好友权限另由 ActionPermit 裁决)。
	TargetFarmCurrentFarm
	// TargetFarmCurrentFriendFarm: target == admitted_farm != session.player_id,
	// 且可信上下文带 relation id/epoch。
	TargetFarmCurrentFriendFarm
	// TargetFarmSubjectRootOperation: 主体来自当前 Session,Root 由原命令与
	// 原目标/Locator 派生(GET_OPERATION_RESULT)。
	TargetFarmSubjectRootOperation
	// TargetFarmTokenInviterSessionRedeemer: REDEEM_SHARE;inviter 来自 Token,
	// redeemer 只来自当前 Session。
	TargetFarmTokenInviterSessionRedeemer
	// TargetFarmSessionCallerPayloadPeer: caller 只来自当前 Session,peer 来自 Payload。
	TargetFarmSessionCallerPayloadPeer
)

func (p TargetFarmPolicy) String() string {
	switch p {
	case TargetFarmNone:
		return "NONE"
	case TargetFarmPayloadTarget:
		return "PAYLOAD_TARGET"
	case TargetFarmSessionPlayer:
		return "SESSION_PLAYER"
	case TargetFarmOwnCurrentFarm:
		return "OWN_CURRENT_FARM"
	case TargetFarmCurrentFarm:
		return "CURRENT_FARM"
	case TargetFarmCurrentFriendFarm:
		return "CURRENT_FRIEND_FARM"
	case TargetFarmSubjectRootOperation:
		return "SUBJECT_ROOT_OPERATION"
	case TargetFarmTokenInviterSessionRedeemer:
		return "TOKEN_INVITER_SESSION_REDEEMER"
	case TargetFarmSessionCallerPayloadPeer:
		return "SESSION_CALLER_PAYLOAD_PEER"
	default:
		return "UNKNOWN"
	}
}

// SessionAdmissionPolicy is a capability-bit set (contract 27 §3 / 28 §6.1).
// Session state is NOT a linear level — never compare these as ordinal ranks.
type SessionAdmissionPolicy struct {
	RequiresAuthenticated       bool
	RequiresRecoveryComplete    bool
	RequiresCurrentSubscription bool
	TargetFarmPolicy            TargetFarmPolicy
}

// HasAll reports whether every required capability bit in want is set in p.
// This is set inclusion, not rank comparison.
func (p SessionAdmissionPolicy) HasAll(want SessionAdmissionPolicy) bool {
	if want.RequiresAuthenticated && !p.RequiresAuthenticated {
		return false
	}
	if want.RequiresRecoveryComplete && !p.RequiresRecoveryComplete {
		return false
	}
	if want.RequiresCurrentSubscription && !p.RequiresCurrentSubscription {
		return false
	}
	return true
}

// ExecutionKind selects how FirstHop executes the command (28 §6.1).
type ExecutionKind uint8

const (
	ExecutionKindUnspecified ExecutionKind = iota
	ExecutionDomainRole
	ExecutionActorDirect
	ExecutionWorkflowCoordinator
)

func (k ExecutionKind) String() string {
	switch k {
	case ExecutionDomainRole:
		return "DOMAIN_ROLE"
	case ExecutionActorDirect:
		return "ACTOR_DIRECT"
	case ExecutionWorkflowCoordinator:
		return "WORKFLOW_COORDINATOR"
	default:
		return "UNSPECIFIED"
	}
}

// ExecutionAcceptPolicy defines when FirstHop has formally taken over the
// business (28 §6:Route Redirect 只允许发生在对应接受边界之前).
type ExecutionAcceptPolicy uint8

const (
	ExecutionAcceptUnspecified ExecutionAcceptPolicy = iota
	ExecutionAcceptWorkCreated
	ExecutionAcceptActorMailboxAccepted
	ExecutionAcceptAuthorityCommitted
	ExecutionAcceptFriendActionAdmissionCommitted
	ExecutionAcceptRuntimeOperationAccepted
	ExecutionAcceptStatelessHandlerStarted
	// ExecutionAcceptOwnerMailboxFriendAdmission is CARE's dual boundary
	// (L2-PROTO-43):Owner 护理为 ACTOR_MAILBOX_ACCEPTED,好友护理为
	// FRIEND_ACTION_ADMISSION_COMMITTED,由可信 viewer/owner 关系选择。
	ExecutionAcceptOwnerMailboxFriendAdmission
)

func (p ExecutionAcceptPolicy) String() string {
	switch p {
	case ExecutionAcceptWorkCreated:
		return "WORK_CREATED"
	case ExecutionAcceptActorMailboxAccepted:
		return "ACTOR_MAILBOX_ACCEPTED"
	case ExecutionAcceptAuthorityCommitted:
		return "AUTHORITY_COMMITTED"
	case ExecutionAcceptFriendActionAdmissionCommitted:
		return "FRIEND_ACTION_ADMISSION_COMMITTED"
	case ExecutionAcceptRuntimeOperationAccepted:
		return "RUNTIME_OPERATION_ACCEPTED"
	case ExecutionAcceptStatelessHandlerStarted:
		return "STATELESS_HANDLER_STARTED"
	case ExecutionAcceptOwnerMailboxFriendAdmission:
		return "OWNER_MAILBOX_OR_FRIEND_ADMISSION_COMMITTED"
	default:
		return "UNSPECIFIED"
	}
}

// TerminalOutcomePolicy is the P0 durable-outcome contract (28 §6).
type TerminalOutcomePolicy uint8

const (
	TerminalOutcomeUnspecified TerminalOutcomePolicy = iota
	// TerminalOutcomeNoDurable: Join 或纯查询不建立持久业务终态。
	TerminalOutcomeNoDurable
	// TerminalOutcomeDurableAfterAccept: 越过 Execution-Accept 后确定性终态
	// 必须按原 EffectiveIdempotencyKey 保存(L2-PROTO-44)。
	TerminalOutcomeDurableAfterAccept
)

func (p TerminalOutcomePolicy) String() string {
	switch p {
	case TerminalOutcomeNoDurable:
		return "NO_DURABLE_OUTCOME"
	case TerminalOutcomeDurableAfterAccept:
		return "DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT"
	default:
		return "UNSPECIFIED"
	}
}

// RouteMode is the Gate routing mode (28 §6, L2-PROTO-23).
type RouteMode uint8

const (
	RouteModeUnspecified RouteMode = iota
	RouteModeActorAffinity
	RouteModeServiceLB
	RouteModeNone
)

func (m RouteMode) String() string {
	switch m {
	case RouteModeActorAffinity:
		return "ACTOR_AFFINITY"
	case RouteModeServiceLB:
		return "SERVICE_LB"
	case RouteModeNone:
		return "NONE"
	default:
		return "UNSPECIFIED"
	}
}

// RouteDomain is the Actor route domain for ACTOR_AFFINITY commands.
type RouteDomain uint8

const (
	RouteDomainUnspecified RouteDomain = iota
	RouteDomainPlayer
	RouteDomainFarm
)

func (d RouteDomain) String() string {
	switch d {
	case RouteDomainPlayer:
		return "PLAYER"
	case RouteDomainFarm:
		return "FARM"
	default:
		return "UNSPECIFIED"
	}
}

// RouteKeySource is the mandatory declared origin of the Actor route key
// (28 §6.1:必须注册 PAYLOAD_FIELD 或 TRUSTED_CONTEXT_FIELD;TrustedContext
// 来源不得回退读取 Payload 中的操作者 ID).
type RouteKeySource uint8

const (
	RouteKeySourceUnspecified RouteKeySource = iota
	RouteKeySourcePayloadField
	RouteKeySourceTrustedContextField
	RouteKeySourceNone
)

func (s RouteKeySource) String() string {
	switch s {
	case RouteKeySourcePayloadField:
		return "PAYLOAD_FIELD"
	case RouteKeySourceTrustedContextField:
		return "TRUSTED_CONTEXT_FIELD"
	case RouteKeySourceNone:
		return "NONE"
	default:
		return "UNSPECIFIED"
	}
}

// FirstHopRole names the first process role that accepts the command.
type FirstHopRole uint8

const (
	FirstHopUnspecified FirstHopRole = iota
	FirstHopPlayerSvr
	FirstHopFarmSvr
	FirstHopGateLocal
)

func (r FirstHopRole) String() string {
	switch r {
	case FirstHopPlayerSvr:
		return "player-svr"
	case FirstHopFarmSvr:
		return "farm-svr"
	case FirstHopGateLocal:
		return "gate-local"
	default:
		return "UNSPECIFIED"
	}
}

// OperationRootKind is the Root authority kind of the effective idempotency
// key (28 §3.2/§6.1 OperationRootPolicy).
type OperationRootKind uint8

const (
	OperationRootUnspecified OperationRootKind = iota
	OperationRootNone                          // 查询、Join 与可安全重入的弱写不建立持久 OperationRoot
	OperationRootPlayerActor
	OperationRootFarmActor
	OperationRootFriendPair
	OperationRootFriendShareIssuer
	OperationRootPlayerMailEntry
	// OperationRootCareDual 是 CARE 的双分支 Root(L2-PROTO-43):
	// Owner 护理 FARM_ACTOR:target_farm_id;好友护理
	// FRIEND_PAIR:canonical_pair(subject,target_owner),Execution-Accept 前
	// 依据可信主体与目标唯一选定。
	OperationRootCareDual
)

func (k OperationRootKind) String() string {
	switch k {
	case OperationRootNone:
		return "NONE"
	case OperationRootPlayerActor:
		return "PLAYER_ACTOR"
	case OperationRootFarmActor:
		return "FARM_ACTOR"
	case OperationRootFriendPair:
		return "FRIEND_PAIR"
	case OperationRootFriendShareIssuer:
		return "FRIEND_SHARE_ISSUER"
	case OperationRootPlayerMailEntry:
		return "PLAYER_MAIL_ENTRY"
	case OperationRootCareDual:
		return "OWNER_FARM_OR_FRIEND_PAIR"
	default:
		return "UNSPECIFIED"
	}
}

// IdempotencyPolicy is the operation_id requirement level (28 §3.2).
type IdempotencyPolicy uint8

const (
	IdempotencyUnspecified IdempotencyPolicy = iota
	// IdempotencyNone: 查询类命令可为 NONE。
	IdempotencyNone
	// IdempotencyRequired: 写命令与可恢复 Workflow 必填并进入持久
	// Receipt/Result 合同。
	IdempotencyRequired
	// IdempotencyRuntimeRequired: Join 等运行时意图;必填并参与当前
	// PendingJoin 防重,但不进入长期 OperationResult。
	IdempotencyRuntimeRequired
)

func (p IdempotencyPolicy) String() string {
	switch p {
	case IdempotencyNone:
		return "NONE"
	case IdempotencyRequired:
		return "REQUIRED"
	case IdempotencyRuntimeRequired:
		return "RUNTIME_REQUIRED"
	default:
		return "UNSPECIFIED"
	}
}

// RequiresOperationID reports whether the envelope operation_id is mandatory.
func (p IdempotencyPolicy) RequiresOperationID() bool {
	return p == IdempotencyRequired || p == IdempotencyRuntimeRequired
}

// RequiresCanonicalBuilder reports whether a CanonicalPayloadBuilder is
// mandatory (28 §3.5/§6.4).
func (p IdempotencyPolicy) RequiresCanonicalBuilder() bool {
	return p == IdempotencyRequired || p == IdempotencyRuntimeRequired
}

// RetryPolicyKind is the declarative retry contract name (28 §6.1 RetryPolicy
// 列).数值预算仍待探针;Kind 本身只是合同标签,Gate 不得据此发明重试次数。
type RetryPolicyKind uint8

const (
	RetryUnspecified RetryPolicyKind = iota
	// RetrySafeQuery: 查询失败可直接安全重查。
	RetrySafeQuery
	// RetrySafeMonotonic: 单调 Ack/状态安全重入。
	RetrySafeMonotonic
	// RetrySameOperation: 保持原 operation_id 有界重试或查询/恢复原 operation。
	RetrySameOperation
	// RetryPollOperationResult: 保持原 operation_id 轮询结果。
	RetryPollOperationResult
)

func (k RetryPolicyKind) String() string {
	switch k {
	case RetrySafeQuery:
		return "SAFE_QUERY_RETRY"
	case RetrySafeMonotonic:
		return "SAFE_MONOTONIC_RETRY"
	case RetrySameOperation:
		return "SAME_OPERATION_ID"
	case RetryPollOperationResult:
		return "POLL_OPERATION_RESULT"
	default:
		return "UNSPECIFIED"
	}
}

// RetryPolicy couples the declarative contract kind with the still-inert
// numeric budget placeholder (cfg.retry.* probe values, TODO(32)).
type RetryPolicy struct {
	Kind RetryPolicyKind
	// Placeholder only — do not interpret zero as "no retry" yet. TODO(32)
	MaxAttempts int
}

// RecoveryPolicy is the NOT_FOUND/EXPIRED recovery contract (28 §3.8/§6.1).
type RecoveryPolicy uint8

const (
	RecoveryUnspecified RecoveryPolicy = iota
	// RecoveryRequery: 纯查询直接重查。
	RecoveryRequery
	// RecoveryFreshJoin: JoinFarm 使用 Fresh Join,不进入持久 OperationResult。
	RecoveryFreshJoin
	// RecoveryPlayerRetrySameOperation: NOT_FOUND 后新 request_id +
	// 原 operation_id + 原 Payload 重发。
	RecoveryPlayerRetrySameOperation
	// RecoveryFarmFreshJoinThenRetry: 先 Fresh Join 原 target_farm_id,再用
	// 原 operation_id 和原 Payload 重发。
	RecoveryFarmFreshJoinThenRetry
)

func (p RecoveryPolicy) String() string {
	switch p {
	case RecoveryRequery:
		return "REQUERY"
	case RecoveryFreshJoin:
		return "FRESH_JOIN"
	case RecoveryPlayerRetrySameOperation:
		return "PLAYER_RETRY_SAME_OPERATION"
	case RecoveryFarmFreshJoinThenRetry:
		return "FARM_FRESH_JOIN_THEN_RETRY_SAME_OPERATION"
	default:
		return "UNSPECIFIED"
	}
}

// TimeoutBudget couples the cfg.timeout.* config key with the still-inert
// numeric placeholder (28 §6.1:字段必须存在并引用版本化配置,数值待探针).
type TimeoutBudget struct {
	ConfigKey string
	// Placeholder only. TODO(32)
	RPCTimeoutMillis int
}

// RateLimitPolicy couples the cfg.rate.* config key with the still-inert
// numeric placeholder. Gate must not enforce a limiter from this placeholder.
type RateLimitPolicy struct {
	ConfigKey string
	// Placeholder only. TODO(32)
	TokensPerSecond float64
}

// AvailabilityPolicy gates command visibility (28 §6.1:FeatureGate 键).
// Enabled=false 的命令已注册合同但业务 Handler 尚未接线;Gate 在准入阶段以
// ERR_COMMAND_UNIMPLEMENTED 明确拒绝,不得伪造业务成功。
type AvailabilityPolicy struct {
	Enabled    bool
	FeatureKey string
}

// PayloadDecoder decodes Typed Payload bytes into the command's request
// proto. Every registered command has exactly one decoder; even an empty
// payload message must still be decoded once so malformed bytes fail closed.
type PayloadDecoder func(payload []byte) (proto.Message, error)

// StructuralValidator is the optional 28 §6 SharedCommandSchema 结构校验钩子,
// 在 typed decode 之后、准入之前执行。它只表达"已注册但尚未接线的功能切片"
// 之类的 fail-closed 边界(例如 CARE 只接 WATER),不得新增业务规则。
type StructuralValidator func(decoded proto.Message) error

// CanonicalPayloadBuilder builds the per-command semantic canonical payload
// (28 §3.5,29 §7.11):command_type 前缀 + 已解码 Typed Payload 的确定性
// 规范化字节(固定字段顺序、定宽整数、presence bit、计数前缀;禁止
// JSON/map/非确定编码)。返回的是 Hash 前原像,便于黄金向量逐字节钉死。
type CanonicalPayloadBuilder func(decoded proto.Message) ([]byte, error)

// RouteKeyExtractor derives the Actor route key from the decoded Typed
// Payload or the trusted session player id. TrustedContext 来源不得回退读取
// Payload 中的操作者 ID(28 §6.4 测试 4).
type RouteKeyExtractor func(decoded proto.Message, sessionPlayerID uint64) (uint64, error)

// PermitClass maps to session.Action without embedding permit issuance here.
type PermitClass uint8

const (
	PermitReadOnly PermitClass = iota + 1
	PermitJoinFarm
	PermitOwnerWrite
)

func (c PermitClass) SessionAction() session.Action {
	switch c {
	case PermitJoinFarm:
		return session.ActionJoinFarm
	case PermitOwnerWrite:
		return session.ActionOwnerWrite
	default:
		return session.ActionReadOnly
	}
}

// CommandSpec is the single registered spec of one public command (28 §6.1;
// 每个公开 CommandType 恰好一个 CommandSpec).
type CommandSpec struct {
	CommandType commonpb.CommandType

	// PayloadType / ResultType are fully-qualified proto message names.
	PayloadType string
	ResultType  string

	Decoder                 PayloadDecoder
	Validator               StructuralValidator // 可选;nil = 无额外结构校验
	CanonicalPayloadBuilder CanonicalPayloadBuilder
	RouteKeyExtractor       RouteKeyExtractor

	MaxPayloadBytes    int
	MinProtocolVersion uint32
	MaxProtocolVersion uint32 // OpenEndedProtocolVersion = OPEN_ENDED
	Availability       AvailabilityPolicy
	SessionAdmission   SessionAdmissionPolicy

	ExecutionKind   ExecutionKind
	ExecutionAccept ExecutionAcceptPolicy
	TerminalOutcome TerminalOutcomePolicy
	FirstHop        FirstHopRole
	FirstHopName    string // 例:"player-svr / PlayerIngress"
	RouteMode       RouteMode
	RouteDomain     RouteDomain
	RouteKeySource  RouteKeySource
	OperationRoot   OperationRootKind
	Idempotency     IdempotencyPolicy
	Retry           RetryPolicy
	Recovery        RecoveryPolicy
	Timeout         TimeoutBudget
	RateLimit       RateLimitPolicy
	PermitClass     PermitClass
}
