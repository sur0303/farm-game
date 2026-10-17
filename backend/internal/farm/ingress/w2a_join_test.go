package ingress

import (
	"context"
	"sync"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	farmactor "farmgame/internal/farm/actor"
	playeractor "farmgame/internal/player/actor"

	"google.golang.org/protobuf/proto"
)

type w2aCaptureDownlink struct {
	mu          sync.Mutex
	items       []*logicrpcpb.DownlinkItem
	callbacks   []func(*logicrpcpb.DeliverBatchItemResult)
	activations []*logicrpcpb.ActivateSubscriptionRequest
}

func (c *w2aCaptureDownlink) Enqueue(item *logicrpcpb.DownlinkItem) error {
	return c.EnqueueWithResult(item, nil)
}

func (c *w2aCaptureDownlink) EnqueueWithResult(item *logicrpcpb.DownlinkItem,
	callback func(*logicrpcpb.DeliverBatchItemResult)) error {
	c.mu.Lock()
	c.items = append(c.items, proto.Clone(item).(*logicrpcpb.DownlinkItem))
	c.callbacks = append(c.callbacks, callback)
	c.mu.Unlock()
	return nil
}

func (c *w2aCaptureDownlink) ActivateSubscription(_ context.Context,
	req *logicrpcpb.ActivateSubscriptionRequest) (*logicrpcpb.ActivateSubscriptionResponse, error) {
	c.mu.Lock()
	c.activations = append(c.activations, proto.Clone(req).(*logicrpcpb.ActivateSubscriptionRequest))
	c.mu.Unlock()
	return &logicrpcpb.ActivateSubscriptionResponse{
		Status:              logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED,
		ReadyThroughVersion: req.ReadyThroughVersion,
	}, nil
}

func TestW2AJoinDoesNotSnapshotBeforeArmAndReadiesAfterQueueAccepted(t *testing.T) {
	stack := newTestStack(t)
	capture := &w2aCaptureDownlink{}
	stack.farm.SetDownlinkEnqueuer(capture)
	trusted := joinTrusted(9, "conn-w2a-ingress")
	response, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command:  typedCommand(t, "join-w2a-op", "join-w2a-request", commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}),
		Trusted:  trusted,
		Dispatch: farmActorDispatch(9),
	})
	if err != nil || response.GetBusiness().GetStatus() != commonpb.CommandStatus_ACCEPTED {
		t.Fatalf("execute=%v err=%v", response, err)
	}
	metadata := response.GetBusiness().GetJoinAccepted()
	if metadata == nil || metadata.SubscriptionId == "" ||
		metadata.JoinAttemptId != trusted.JoinAttempt.JoinAttemptId {
		t.Fatalf("join metadata=%v", metadata)
	}
	capture.mu.Lock()
	preArmItems := len(capture.items)
	capture.mu.Unlock()
	if preArmItems != 0 {
		t.Fatalf("snapshot escaped before ArmSnapshot: items=%d", preArmItems)
	}
	if phase, ok := presencePhase(t, stack, 9, metadata.SubscriptionId); !ok || phase != farmactor.PresenceJoining {
		t.Fatalf("unarmed phase=%v ok=%v", phase, ok)
	}
	target := &logicrpcpb.DownlinkTargetFence{
		TargetPlayerId: trusted.Session.PlayerId, GatewayId: trusted.Session.GatewayId,
		GatewayBootId: trusted.Session.GatewayBootId, ConnectionId: trusted.Session.ConnectionId,
		ConnectionGeneration: trusted.Session.ConnectionGeneration,
		SessionEpoch:         trusted.Session.ActiveSessionEpoch,
		SessionRouteRevision: trusted.Session.SessionRouteRevision,
	}
	fence := &logicrpcpb.SubscriptionFence{
		FarmId: 9, SubscriptionId: metadata.SubscriptionId, JoinAttemptId: metadata.JoinAttemptId,
	}
	armed, err := stack.farm.ArmSnapshot(context.Background(), &logicrpcpb.ArmSnapshotRequest{
		Subscription: fence, Target: target,
	})
	if err != nil || armed.GetStatus() != logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_ARMED ||
		armed.SnapshotVersion == nil {
		t.Fatalf("arm=%v err=%v", armed, err)
	}
	capture.mu.Lock()
	if len(capture.items) != 1 || len(capture.callbacks) != 1 {
		capture.mu.Unlock()
		t.Fatalf("snapshot dispatch items/callbacks=%d/%d", len(capture.items), len(capture.callbacks))
	}
	item := proto.Clone(capture.items[0]).(*logicrpcpb.DownlinkItem)
	callback := capture.callbacks[0]
	capture.mu.Unlock()
	if item.JoinFence.GetJoinAttemptId() != metadata.JoinAttemptId ||
		item.TargetFence.GetConnectionId() != target.ConnectionId || item.SnapshotVersion != *armed.SnapshotVersion {
		t.Fatalf("snapshot fences=%v/%v version=%d", item.JoinFence, item.TargetFence, item.SnapshotVersion)
	}
	var frame commonpb.FarmFrame
	var snapshotPush farmpb.FarmSnapshotPush
	if proto.Unmarshal(item.Frame, &frame) != nil || frame.GetServerPush() == nil ||
		frame.GetServerPush().PushType != commonpb.PushType_FARM_SNAPSHOT ||
		proto.Unmarshal(frame.GetServerPush().Payload, &snapshotPush) != nil ||
		snapshotPush.JoinAttemptId != metadata.JoinAttemptId {
		t.Fatalf("invalid typed snapshot frame")
	}
	if profile := snapshotPush.GetSnapshot().GetOwnerPublicProfile(); profile.GetPlayerId() != 9 ||
		profile.GetDisplayName() != playeractor.DefaultDisplayName || profile.GetAvatarId() != playeractor.DefaultAvatarID || profile.GetLevel() != 1 {
		t.Fatalf("snapshot owner profile=%v", profile)
	}
	callback(&logicrpcpb.DeliverBatchItemResult{ItemId: item.ItemId,
		Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		phase, ok := presencePhase(t, stack, 9, metadata.SubscriptionId)
		if ok && phase == farmactor.PresenceInFarm {
			capture.mu.Lock()
			activations := len(capture.activations)
			capture.mu.Unlock()
			if activations != 1 {
				t.Fatalf("activations=%d", activations)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("join did not reach IN_FARM")
}
