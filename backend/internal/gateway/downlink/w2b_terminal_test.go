package downlink

import (
	"context"
	"errors"
	"testing"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"

	"google.golang.org/protobuf/proto"
)

func TestW2BRejoinRequiredSharesFIFOAndTerminatesExactSubscription(t *testing.T) {
	registry := NewConnectionRegistry()
	writer := NewConnectionWriter(7, 16, 1<<20)
	target, fence := activateW2BSubscription(t, registry, writer, "sub-old", "attempt-old", 17)
	drainWriter(t, registry, target.ConnectionId, target.ConnectionGeneration, 3)

	var termination *SubscriptionTermination
	registry.SetSubscriptionTerminationHandler(func(got SubscriptionTermination) {
		copy := got
		termination = &copy
	})
	changeFrame := w2aPushFrame(t, commonpb.PushType_FARM_CHANGE, target, fence,
		&farmpb.FarmChange{OwnerId: fence.FarmId, FarmVersion: 11})
	rejoinFrame := w2aPushFrame(t, commonpb.PushType_REJOIN_REQUIRED, target, fence,
		&farmpb.RejoinRequiredPush{Reason: farmpb.RejoinReason_REJOIN_REASON_STREAM_GAP})
	response, err := registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{
		Items: []*logicrpcpb.DownlinkItem{
			{
				ItemId: "change-11", SubscriptionId: fence.SubscriptionId,
				GatewayId:    target.GatewayId,
				ConnectionId: target.ConnectionId, TransportGeneration: target.ConnectionGeneration,
				PresenceEpoch: 17, Frame: changeFrame, JoinFence: fence, TargetFence: target,
			},
			{
				ItemId: "terminal-old", SubscriptionId: fence.SubscriptionId,
				GatewayId:    target.GatewayId,
				ConnectionId: target.ConnectionId, TransportGeneration: target.ConnectionGeneration,
				PresenceEpoch: 17, Frame: rejoinFrame, JoinFence: fence, TargetFence: target,
			},
		},
	})
	if err != nil || len(response.Results) != 2 ||
		response.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED ||
		response.Results[1].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("terminal delivery response=%v err=%v", response, err)
	}
	if termination == nil || termination.SubscriptionID != fence.SubscriptionId ||
		termination.JoinAttemptID != fence.JoinAttemptId || termination.PresenceEpoch != 17 ||
		termination.ConnectionClosed {
		t.Fatalf("termination=%+v", termination)
	}
	assertQueuedPush(t, registry, target, commonpb.PushType_FARM_CHANGE)
	assertQueuedPush(t, registry, target, commonpb.PushType_REJOIN_REQUIRED)

	registry.mu.Lock()
	entry := registry.connections[target.ConnectionId]
	registry.mu.Unlock()
	if entry.subscription != "" || entry.activeAttempt != "" || entry.presenceEpoch != 0 {
		t.Fatalf("terminal retained active fence: %+v", entry)
	}

	// Re-delivery is idempotent even though the active fence has been cleared.
	again, err := registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{
		Items: []*logicrpcpb.DownlinkItem{{
			ItemId: "terminal-old", SubscriptionId: fence.SubscriptionId,
			GatewayId:    target.GatewayId,
			ConnectionId: target.ConnectionId, TransportGeneration: target.ConnectionGeneration,
			PresenceEpoch: 17, Frame: rejoinFrame, JoinFence: fence, TargetFence: target,
		}},
	})
	if err != nil || again.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("duplicate terminal=%v err=%v", again, err)
	}
}

func TestW2BRejoinRequiredQueueFullClosesConnectionWriter(t *testing.T) {
	registry := NewConnectionRegistry()
	// ACCEPTED + Snapshot + Ready exactly fill this writer.
	writer := NewConnectionWriter(7, 3, 1<<20)
	target, fence := activateW2BSubscription(t, registry, writer, "sub-full", "attempt-full", 23)
	var termination *SubscriptionTermination
	registry.SetSubscriptionTerminationHandler(func(got SubscriptionTermination) {
		copy := got
		termination = &copy
	})
	rejoinFrame := w2aPushFrame(t, commonpb.PushType_REJOIN_REQUIRED, target, fence,
		&farmpb.RejoinRequiredPush{Reason: farmpb.RejoinReason_REJOIN_REASON_DELIVERY_FAILED})
	response, err := registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{
		Items: []*logicrpcpb.DownlinkItem{{
			ItemId: "terminal-full", SubscriptionId: fence.SubscriptionId,
			GatewayId:    target.GatewayId,
			ConnectionId: target.ConnectionId, TransportGeneration: target.ConnectionGeneration,
			PresenceEpoch: 23, Frame: rejoinFrame, JoinFence: fence, TargetFence: target,
		}},
	})
	if err != nil || response.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_FULL {
		t.Fatalf("queue-full terminal=%v err=%v", response, err)
	}
	if termination == nil || !termination.ConnectionClosed || termination.SubscriptionID != fence.SubscriptionId {
		t.Fatalf("termination=%+v", termination)
	}
	if err := writer.Enqueue(7, []byte("after-close")); !errors.Is(err, ErrClosed) {
		t.Fatalf("writer enqueue after terminal overflow=%v, want ErrClosed", err)
	}
}

func activateW2BSubscription(t *testing.T, registry *ConnectionRegistry, writer *ConnectionWriter,
	subscriptionID, attemptID string, presenceEpoch uint64) (*logicrpcpb.DownlinkTargetFence, *logicrpcpb.SubscriptionFence) {
	t.Helper()
	registry.Bind("conn-w2b", "", 7, writer)
	if !registry.BindPlayer("conn-w2b", 7, 101) {
		t.Fatal("bind player")
	}
	target := &logicrpcpb.DownlinkTargetFence{
		TargetPlayerId: 101, GatewayId: "gate-a", GatewayBootId: "boot-a",
		ConnectionId: "conn-w2b", ConnectionGeneration: 7,
		SessionEpoch: 11, SessionRouteRevision: 13,
	}
	if !registry.BindSessionTarget(target) {
		t.Fatal("bind target")
	}
	operationID := "join-" + subscriptionID
	if !registry.PrepareJoinAttempt(target.ConnectionId, target.ConnectionGeneration,
		operationID, 202, attemptID) {
		t.Fatal("prepare join")
	}
	accepted := &commonpb.CommandResultEnvelope{
		OperationId: operationID, CommandType: commonpb.CommandType_JOIN_FARM,
		Status: commonpb.CommandStatus_ACCEPTED,
		ResultContext: &commonpb.CommandResultEnvelope_Join{Join: &commonpb.JoinResultContext{
			TargetFarmId: 202, SubscriptionId: subscriptionID, JoinAttemptId: attemptID,
		}},
	}
	if !registry.QueueJoinAccepted(target.ConnectionId, target.ConnectionGeneration, operationID,
		202, attemptID, subscriptionID, accepted) ||
		!registry.MarkSnapshotArmRequested(target.ConnectionId, target.ConnectionGeneration,
			operationID, 202, attemptID, subscriptionID) {
		t.Fatal("queue accepted/mark arm")
	}
	fence := &logicrpcpb.SubscriptionFence{FarmId: 202, SubscriptionId: subscriptionID, JoinAttemptId: attemptID}
	snapshotFrame := w2aPushFrame(t, commonpb.PushType_FARM_SNAPSHOT, target, fence,
		&farmpb.FarmSnapshotPush{JoinAttemptId: attemptID, SnapshotVersion: 10,
			Snapshot: &farmpb.FarmSnapshot{OwnerId: 202, FarmVersion: 10}})
	snapshot, err := registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{
		Items: []*logicrpcpb.DownlinkItem{{
			ItemId: "snapshot-" + subscriptionID, SubscriptionId: subscriptionID,
			GatewayId:    target.GatewayId,
			ConnectionId: target.ConnectionId, TransportGeneration: target.ConnectionGeneration,
			PresenceEpoch: presenceEpoch, Frame: snapshotFrame, JoinFence: fence, TargetFence: target,
			SnapshotVersion: 10,
		}},
	})
	if err != nil || snapshot.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("snapshot=%v err=%v", snapshot, err)
	}
	ready, err := registry.ActivateSubscription(context.Background(), &logicrpcpb.ActivateSubscriptionRequest{
		Subscription: fence, Target: target, ReadyThroughVersion: 10,
	})
	if err != nil || ready.Status != logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED {
		t.Fatalf("ready=%v err=%v", ready, err)
	}
	return target, fence
}

func drainWriter(t *testing.T, registry *ConnectionRegistry, connectionID string, generation uint64, count int) {
	t.Helper()
	for index := 0; index < count; index++ {
		if _, ok := registry.Dequeue(connectionID, generation); !ok {
			t.Fatalf("missing queued frame %d", index)
		}
	}
}

func assertQueuedPush(t *testing.T, registry *ConnectionRegistry,
	target *logicrpcpb.DownlinkTargetFence, want commonpb.PushType) {
	t.Helper()
	raw, ok := registry.Dequeue(target.ConnectionId, target.ConnectionGeneration)
	if !ok {
		t.Fatalf("missing queued %s", want)
	}
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(raw, &frame); err != nil {
		t.Fatal(err)
	}
	push := frame.GetServerPush()
	if push == nil {
		t.Fatalf("missing server push in queued frame, want %s", want)
	}
	if push.PushType != want {
		t.Fatalf("push=%s want=%s", push.PushType, want)
	}
}
