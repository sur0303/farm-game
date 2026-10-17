package downlink

import (
	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"

	"google.golang.org/protobuf/proto"
)

// PushScope is the Gate-derived delivery fence class for ServerPushEnvelope.
// Wire format has no scope field yet (Batch 3); Gate derives it from push_type
// per contract 28 §2.1 / 26 §11.2.
type PushScope uint8

const (
	// PushScopeFarmSubscription applies current_subscription_id fencing.
	PushScopeFarmSubscription PushScope = iota + 1
	// PushScopeSessionOrPlayer is unaffected by farm subscription switches.
	PushScopeSessionOrPlayer
)

// DerivePushScope maps push_type → PushScope. Unknown / unspecified types fail
// closed to FarmSubscription so legacy DeliverBatch frames without a typed
// envelope body keep the existing subscription fence.
func DerivePushScope(pushType commonpb.PushType) PushScope {
	switch pushType {
	case commonpb.PushType_FARM_SNAPSHOT,
		commonpb.PushType_FARM_CHANGE,
		commonpb.PushType_FARM_DELTA_BATCH,
		commonpb.PushType_SUBSCRIPTION_READY,
		commonpb.PushType_REJOIN_REQUIRED:
		return PushScopeFarmSubscription
	case commonpb.PushType_KICK,
		commonpb.PushType_DRAIN_NOTICE,
		commonpb.PushType_FRIEND_ROSTER_CHANGED,
		commonpb.PushType_FRIEND_OPPORTUNITY_CHANGED,
		commonpb.PushType_SYSTEM_NOTICE,
		commonpb.PushType_OPERATION_RESULT:
		return PushScopeSessionOrPlayer
	default:
		return PushScopeFarmSubscription
	}
}

// requiresFarmSubscriptionFence inspects a FarmFrame payload and reports whether
// DeliverBatch must enforce item.SubscriptionId == connection currentSubscription.
// CommandResult frames never reach this path (normalizeFrameSequence rejects
// non-SERVER_PUSH); sync CommandResult responses bypass downlink entirely.
func requiresFarmSubscriptionFence(frameBytes []byte) bool {
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(frameBytes, &frame); err != nil {
		return true
	}
	push := frame.GetServerPush()
	if push == nil {
		return true
	}
	return DerivePushScope(push.PushType) == PushScopeFarmSubscription
}

// requiresCompleteSessionTargetFence identifies account-scoped pushes whose
// producer is outside Gate and therefore must carry the full current
// SessionRoute fence. The list is intentionally explicit; legacy player
// pushes remain compatible until their producers are migrated deliberately.
func requiresCompleteSessionTargetFence(frameBytes []byte) bool {
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(frameBytes, &frame); err != nil {
		return false
	}
	push := frame.GetServerPush()
	return push != nil && push.PushType == commonpb.PushType_FRIEND_OPPORTUNITY_CHANGED
}

func validPlayerPushTarget(frameBytes []byte, target *logicrpcpb.DownlinkTargetFence) bool {
	if target == nil || target.TargetPlayerId == 0 {
		return false
	}
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(frameBytes, &frame); err != nil {
		return false
	}
	push := frame.GetServerPush()
	return push != nil && push.GetPlayer() != nil &&
		push.GetPlayer().TargetPlayerId == target.TargetPlayerId
}

func isFarmSnapshotPush(frameBytes []byte) bool {
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(frameBytes, &frame); err != nil {
		return false
	}
	push := frame.GetServerPush()
	return push != nil && push.PushType == commonpb.PushType_FARM_SNAPSHOT
}
