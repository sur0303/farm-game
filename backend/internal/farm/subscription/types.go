// Package subscription owns ephemeral Presence and subscription lifecycle state
// independently from FarmActor activation (阶段 5 拆耦,26 §12)。
// Hub 与 FarmActor 同 Holder,但 Hub 不是 FarmActor,也不拥有业务真值。
package subscription

import (
	"time"

	farmpb "farmgame/api/gen/farm"
	publicpb "farmgame/api/gen/public"
)

type PresencePhase int

const (
	PresenceJoining PresencePhase = iota + 1
	PresenceFlushing
	// PresenceActivating 表示 catch-up 已连续推进到 ready_cut_version=N,
	// Hub 等待 ActivateSubscription/ReadyQueueAccepted(26 §12)。
	PresenceActivating
	PresenceInFarm
	// PresenceResyncRequired is a terminal read-only fence. It remains indexed
	// until the exact REJOIN_REQUIRED queue result permits CleanupSubscription.
	PresenceResyncRequired
)

type Presence struct {
	JoinOperationID     string
	JoinAttemptID       string
	SubscriptionID      string
	ViewerPlayerID      uint64
	SessionEpoch        uint64
	GatewayID           string
	GatewayBootID       string
	ConnectionID        string
	TransportGeneration uint64
	RouteRevision       uint64
	PresenceEpoch       uint64
	SnapshotVersion     uint64
	// BaselineSnapshot 是 ArmSnapshot 在 Farm Runtime Lane 中安装的唯一
	// 运行时基线。它不持久化；重复 Arm 复用同一版本和内容。
	BaselineSnapshot  *farmpb.FarmSnapshot
	LastPushedVersion uint64
	Phase             PresencePhase
	Buffer            []*farmpb.FarmChange
	BufferBytes       int
	JoiningStartedAt  time.Time
	FallbackExpiresAt time.Time
	// LeaseExpiresAt is the short, renewable FarmSubscription lifetime. It is
	// independent from the long FallbackExpiresAt safety net and is advanced
	// only by an exact BatchRenewSubscriptions fence.
	LeaseExpiresAt time.Time

	// ── 26 §12 Join 状态机(W1A) ──
	// Armed 表示 ArmSnapshot 已授权发快照(HC-ACCESS-24)。当前双锁存路径在创建时
	// 即视为 armed(保持既有行为);W2 的 CreateOrReuseJoining 创建 unarmed Presence,
	// 未 Arm 前不得捕获任何 Change(HC-ACCESS-37)。
	Armed bool
	// ReadyCutVersion 是有限 ready_cut_version=N(HC-ACCESS-25):冻结时确定,
	// 不等待变化流静止。N=0 对新建空农场合法,是否冻结使用独立布尔值。
	ReadyCutVersion uint64
	ReadyCutFrozen  bool
	// CatchupAcceptedThrough is the highest version whose finite catch-up batch
	// has already crossed Gate's QueueAccepted boundary. It starts at the
	// Snapshot version and makes repeated callbacks idempotent without retaining
	// an unbounded history of acknowledged batches.
	CatchupAcceptedThrough uint64
	// PostReadyBuffer 收集 version>N 的变化,排在 SUBSCRIPTION_READY 后(HC-ACCESS-39)。
	PostReadyBuffer      []*farmpb.FarmChange
	PostReadyBufferBytes int
	PostReadyLastVersion uint64
	// DeliveryGeneration 是该订阅安装基线时的实时投递代(27 §5.17);
	// 失效置位后 generation++ 前的订阅为旧代,必须 Fresh Join。
	DeliveryGeneration uint64
}

type JoinRequest struct {
	OperationID   string
	JoinAttemptID string
	// ForceFresh is set by Gate after it has revoked the previous local
	// subscription. It prevents a still-in-flight remote cleanup from making
	// this new Join reuse the old Presence and old JoinAttemptID.
	ForceFresh          bool
	SubscriptionID      string
	ViewerPlayerID      uint64
	SessionEpoch        uint64
	GatewayID           string
	GatewayBootID       string
	ConnectionID        string
	TransportGeneration uint64
	RouteRevision       uint64
}

type JoinResult struct {
	SubscriptionID  string
	JoinAttemptID   string
	ViewerPlayerID  uint64
	SessionEpoch    uint64
	GatewayID       string
	GatewayBootID   string
	ConnectionID    string
	Generation      uint64
	RouteRevision   uint64
	PresenceEpoch   uint64
	ActivationEpoch uint64
	Snapshot        *farmpb.FarmSnapshot
	Replayed        bool
	// AcceptedSubscriptionTTL is the conservative remaining Holder lifetime
	// returned to Gate for its local monotonic deadline.
	AcceptedSubscriptionTTL time.Duration
}

type SubscriptionLeaseStatus uint8

const (
	SubscriptionLeaseRenewed SubscriptionLeaseStatus = iota + 1
	SubscriptionLeaseNotFound
	SubscriptionLeaseStaleFence
	SubscriptionLeaseClosed
	SubscriptionLeaseRouteStale
	SubscriptionLeaseUnavailable
)

// SubscriptionLeaseRequest is evaluated inside the target Farm Hub lane. The
// embedded LeaveRequest reuses the complete DownlinkTargetFence fields; the
// server-issued subscription_id plus join_attempt_id complete the lease fence.
type SubscriptionLeaseRequest struct {
	SubscriptionID string
	JoinAttemptID  string
	Fence          LeaveRequest
	RequestedTTL   time.Duration
}

type SubscriptionLeaseResult struct {
	Status      SubscriptionLeaseStatus
	AcceptedTTL time.Duration
	ServerTime  time.Time
}

// ArmSnapshotRequest 只允许完整匹配 CreateOrReuseJoining 创建的 unarmed
// Presence。Fence 复用 LeaveRequest，避免为同一连接代次再造一套字段。
type ArmSnapshotRequest struct {
	SubscriptionID     string
	JoinAttemptID      string
	Fence              LeaveRequest
	OwnerPublicProfile *publicpb.PublicProfileView
}

// JoinControlFence is the Hub-local form of the complete connection fence.
// QueueAccepted callbacks validate it inside the Farm Runtime Lane so Reaper,
// Leave and late callbacks cannot race on the presence map.
type JoinControlFence struct {
	SubscriptionID      string
	JoinAttemptID       string
	ViewerPlayerID      uint64
	SessionEpoch        uint64
	GatewayID           string
	GatewayBootID       string
	ConnectionID        string
	TransportGeneration uint64
	RouteRevision       uint64
}

type LeaveRequest struct {
	SubscriptionID      string
	JoinAttemptID       string
	ViewerPlayerID      uint64
	SessionEpoch        uint64
	GatewayID           string
	GatewayBootID       string
	ConnectionID        string
	TransportGeneration uint64
	RouteRevision       uint64
	PresenceEpoch       uint64
}

// CancelAttemptStatus is the Hub decision returned by CancelJoinAttempt.
// ALREADY_PROGRESSED tells Gate to continue with exact CleanupSubscription,
// but only while its local attempt is still CANCELLING.
type CancelAttemptStatus uint8

const (
	CancelAttemptCancelled CancelAttemptStatus = iota + 1
	CancelAttemptStaleFence
	CancelAttemptAlreadyProgressed
)

type CancelJoinAttemptRequest struct {
	JoinAttemptID  string
	OperationID    string
	SubscriptionID string
	Fence          LeaveRequest
	Now            time.Time
}

type CancelJoinAttemptResult struct {
	Status         CancelAttemptStatus
	SubscriptionID string
}

// CancelTombstone orders a cancellation that arrived before its unarmed Join.
// It is runtime-only, bounded per Farm lane, and retained long enough to cover
// the Join deadline plus bounded RPC retry and late-response windows.
type CancelTombstone struct {
	JoinAttemptID string
	OperationID   string
	Fence         LeaveRequest
	CancelledAt   time.Time
	ExpiresAt     time.Time
}

// CancelJoinRequest identifies a Join by its durable retry identity and full
// connection fence. SubscriptionID is optional because Gate may time out
// before the ACCEPTED result reveals it.
type CancelJoinRequest struct {
	OperationID    string
	SubscriptionID string
	Fence          LeaveRequest
}

type ResyncNotice struct {
	Presence               Presence
	Reason                 string
	LatestKnownFarmVersion uint64
	ActivationEpoch        uint64
}

// JoinIdempotencyEntry 是 Join 幂等条目;刻意仅运行时(Presence 同为运行时态,
// Actor 重新激活后旧订阅即失效,客户端必须重新 Join)。
type JoinIdempotencyEntry struct {
	SubscriptionID string
	IntentHash     []byte
	ExpiresAt      time.Time
}

// GateBootFence identifies one gate process lifetime. It is intentionally a
// runtime ownership key, not a friendship/operation permission.
type GateBootFence struct {
	GatewayID     string
	GatewayBootID string
}

type SubscriptionRef struct {
	FarmID         uint64
	SubscriptionID string
	PresenceEpoch  uint64
}
