package actor

import "farmgame/internal/farm/subscription"

// M0 搬包桥接层:订阅域类型已迁入 internal/farm/subscription(Hub 不是 Actor,
// 不拥有业务真值)。以下别名保持 actor 包既有代码零改动;新代码应直接使用
// subscription 包。
type (
	Presence                 = subscription.Presence
	PresencePhase            = subscription.PresencePhase
	JoinRequest              = subscription.JoinRequest
	JoinResult               = subscription.JoinResult
	ArmSnapshotRequest       = subscription.ArmSnapshotRequest
	LeaveRequest             = subscription.LeaveRequest
	CancelJoinRequest        = subscription.CancelJoinRequest
	CancelJoinAttemptRequest = subscription.CancelJoinAttemptRequest
	CancelJoinAttemptResult  = subscription.CancelJoinAttemptResult
	CancelAttemptStatus      = subscription.CancelAttemptStatus
	ResyncNotice             = subscription.ResyncNotice
	// joinIdempotencyEntry 保留小写别名,仅兼容本包历史字段名。
	joinIdempotencyEntry = subscription.JoinIdempotencyEntry

	GateBootFence        = subscription.GateBootFence
	SubscriptionRef      = subscription.SubscriptionRef
	FarmEventLane        = subscription.FarmEventLane
	SubscriptionHub      = subscription.SubscriptionHub
	SubscriptionHubStats = subscription.SubscriptionHubStats
)

const (
	PresenceJoining                = subscription.PresenceJoining
	PresenceFlushing               = subscription.PresenceFlushing
	PresenceActivating             = subscription.PresenceActivating
	PresenceInFarm                 = subscription.PresenceInFarm
	PresenceResyncRequired         = subscription.PresenceResyncRequired
	CancelAttemptCancelled         = subscription.CancelAttemptCancelled
	CancelAttemptStaleFence        = subscription.CancelAttemptStaleFence
	CancelAttemptAlreadyProgressed = subscription.CancelAttemptAlreadyProgressed
)

var (
	NewSubscriptionHub         = subscription.NewSubscriptionHub
	ErrFarmEventLaneFull       = subscription.ErrFarmEventLaneFull
	ErrFarmEventLaneRetired    = subscription.ErrFarmEventLaneRetired
	ErrJoinAttemptCancelled    = subscription.ErrJoinAttemptCancelled
	ErrCancelTombstoneCapacity = subscription.ErrCancelTombstoneCapacity
)

// FarmActor 实现 subscription.ActorBinding:把 Presence/Join 幂等映射别名进
// Lane 状态,事件结束后由 Lane 回收 Join 幂等顺序。
func (a *FarmActor) BindState(presence map[string]*subscription.Presence, byPlayer map[uint64]string,
	byAttempt map[string]string, idem map[string]subscription.JoinIdempotencyEntry,
	order []string, realtime subscription.FarmRealtimeState) {
	a.presence = presence
	a.presenceByPlayer = byPlayer
	a.presenceByAttempt = byAttempt
	a.joinIdempotency = idem
	a.joinIdempotencyOrder = order
	a.realtimeState = realtime
}

func (a *FarmActor) IdemOrder() []string { return a.joinIdempotencyOrder }
