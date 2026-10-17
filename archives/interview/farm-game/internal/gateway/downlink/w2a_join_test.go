package downlink

import (
	"context"
	"testing"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	gateprotocol "farmgame/internal/gateway/protocol"

	"google.golang.org/protobuf/proto"
)

func TestW2AJoinUsesOneWriterAndActivatesOnlyAfterReady(t *testing.T) {
	registry := NewConnectionRegistry()
	writer := NewConnectionWriter(7, 32, 1<<20)
	registry.Bind("conn-w2a", "", 7, writer)
	if !registry.BindPlayer("conn-w2a", 7, 101) {
		t.Fatal("bind player")
	}
	target := &logicrpcpb.DownlinkTargetFence{
		TargetPlayerId: 101, GatewayId: "gate-a", GatewayBootId: "boot-a",
		ConnectionId: "conn-w2a", ConnectionGeneration: 7,
		SessionEpoch: 11, SessionRouteRevision: 13,
	}
	if !registry.BindSessionTarget(target) {
		t.Fatal("bind target fence")
	}
	registry.SetNextSequence("conn-w2a", 7, 2)
	if !registry.PrepareJoinAttempt("conn-w2a", 7, "join-op", 202, "join-attempt") {
		t.Fatal("prepare join")
	}
	accepted := &commonpb.CommandResultEnvelope{
		OperationId: "join-op", RequestId: "join-request", CommandType: commonpb.CommandType_JOIN_FARM,
		Status: commonpb.CommandStatus_ACCEPTED,
		ResultContext: &commonpb.CommandResultEnvelope_Join{Join: &commonpb.JoinResultContext{
			TargetFarmId: 202, SubscriptionId: "sub-w2a", JoinAttemptId: "join-attempt",
		}},
	}
	if !registry.QueueJoinAccepted("conn-w2a", 7, "join-op", 202, "join-attempt", "sub-w2a", accepted) {
		t.Fatal("queue accepted")
	}
	if !registry.MarkSnapshotArmRequested("conn-w2a", 7, "join-op", 202, "join-attempt", "sub-w2a") {
		t.Fatal("mark arm")
	}
	fence := &logicrpcpb.SubscriptionFence{FarmId: 202, SubscriptionId: "sub-w2a", JoinAttemptId: "join-attempt"}
	snapshot := &farmpb.FarmSnapshot{OwnerId: 202, FarmVersion: 10}
	snapshotFrame := w2aPushFrame(t, commonpb.PushType_FARM_SNAPSHOT, target, fence,
		&farmpb.FarmSnapshotPush{JoinAttemptId: "join-attempt", SnapshotVersion: 10, Snapshot: snapshot})
	snapshotResult, err := registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{
		Items: []*logicrpcpb.DownlinkItem{{
			ItemId: "snapshot-10", SubscriptionId: "sub-w2a", GatewayId: "gate-a",
			ConnectionId: "conn-w2a", TransportGeneration: 7, PresenceEpoch: 17,
			Frame: snapshotFrame, JoinFence: fence, TargetFence: target, SnapshotVersion: 10,
		}},
	})
	if err != nil || snapshotResult.GetResults()[0].GetStatus() != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("snapshot result=%v err=%v", snapshotResult, err)
	}
	registry.mu.Lock()
	afterSnapshot := registry.connections["conn-w2a"]
	registry.mu.Unlock()
	if afterSnapshot.subscription != "" || afterSnapshot.pendingJoin.stage != pendingJoinStageSnapshotQueued {
		t.Fatalf("snapshot promoted early: subscription=%q stage=%d",
			afterSnapshot.subscription, afterSnapshot.pendingJoin.stage)
	}

	deltas := []*farmpb.FarmChange{{OwnerId: 202, FarmVersion: 11}, {OwnerId: 202, FarmVersion: 12}}
	deltaFrame := w2aPushFrame(t, commonpb.PushType_FARM_DELTA_BATCH, target, fence,
		&farmpb.FarmDeltaBatchPush{JoinAttemptId: "join-attempt", FromVersion: 11, ThroughVersion: 12, Deltas: deltas})
	deltaResult, err := registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{
		Items: []*logicrpcpb.DownlinkItem{{
			ItemId: "delta-11-12", SubscriptionId: "sub-w2a", GatewayId: "gate-a",
			ConnectionId: "conn-w2a", TransportGeneration: 7, Frame: deltaFrame,
			JoinFence: fence, TargetFence: target, FromVersion: 11, ThroughVersion: 12,
		}},
	})
	if err != nil || deltaResult.GetResults()[0].GetStatus() != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("delta result=%v err=%v", deltaResult, err)
	}
	ready, err := registry.ActivateSubscription(context.Background(), &logicrpcpb.ActivateSubscriptionRequest{
		Subscription: fence, Target: target, ReadyThroughVersion: 12,
	})
	if err != nil || ready.GetStatus() != logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED {
		t.Fatalf("activate=%v err=%v", ready, err)
	}

	// 期望下行顺序:ACCEPTED(CommandResult) → FARM_SNAPSHOT → FARM_DELTA_BATCH →
	// SUBSCRIPTION_READY(28 L2-PROTO-34);CommandResult 与 ServerPush 是顶层
	// oneof 的不同分支。
	wantCommandResult := []bool{true, false, false, false}
	wantPushes := []commonpb.PushType{
		commonpb.PushType_PUSH_TYPE_UNSPECIFIED,
		commonpb.PushType_FARM_SNAPSHOT,
		commonpb.PushType_FARM_DELTA_BATCH,
		commonpb.PushType_SUBSCRIPTION_READY,
	}
	for index := range wantCommandResult {
		raw, ok := registry.Dequeue("conn-w2a", 7)
		if !ok {
			t.Fatalf("missing writer frame %d", index)
		}
		var frame commonpb.FarmFrame
		if err := proto.Unmarshal(raw, &frame); err != nil {
			t.Fatal(err)
		}
		isResult := frame.GetCommandResult() != nil
		if frame.GetSequence() != uint64(index+2) || isResult != wantCommandResult[index] {
			t.Fatalf("frame[%d] seq=%d command_result=%v want=%v",
				index, frame.GetSequence(), isResult, wantCommandResult[index])
		}
		if push := frame.GetServerPush(); push != nil && push.PushType != wantPushes[index] {
			t.Fatalf("frame[%d] push=%s want=%s", index, push.PushType, wantPushes[index])
		}
	}
	registry.mu.Lock()
	active := registry.connections["conn-w2a"]
	registry.mu.Unlock()
	if active.subscription != "sub-w2a" || active.activeAttempt != "join-attempt" ||
		active.pendingJoin.stage != pendingJoinStageActive {
		t.Fatalf("active state=%+v", active)
	}
	if current, _ := registry.pendingJoinStats(); current != 0 {
		t.Fatalf("active subscription counted as pending: %d", current)
	}
	if registry.ObserveJoinTimeout("conn-w2a", 7, "join-op", 202) {
		t.Fatal("active subscription observed as timed-out PendingJoin")
	}
	if registry.CancelJoin("conn-w2a", 7, "join-op", 202) {
		t.Fatal("active subscription canceled through PendingJoin path")
	}
	if !registry.EndSubscription("conn-w2a", 7, "sub-w2a") {
		t.Fatal("end active subscription")
	}
	registry.mu.Lock()
	ended := registry.connections["conn-w2a"]
	registry.mu.Unlock()
	if ended.subscription != "" || ended.activeAttempt != "" || ended.pendingJoin != nil {
		t.Fatalf("ended subscription retained active fences: %+v", ended)
	}
}

// TestW2APostReadyChangeWithFullFenceAccepted 钉死 26.4 审计缺陷修复的 Gate 侧
// 验收:SUBSCRIPTION_READY 激活后,farm-ingress 补发的 post-ready FARM_CHANGE
// 携带完整 Fence(PresenceEpoch/subscription_id/connection generation/JoinFence/
// TargetFence)时必须被真实 Gate 接受;任一 Fence 残缺仍判 stale。
func TestW2APostReadyChangeWithFullFenceAccepted(t *testing.T) {
	registry := NewConnectionRegistry()
	writer := NewConnectionWriter(7, 32, 1<<20)
	registry.Bind("conn-w2a", "", 7, writer)
	if !registry.BindPlayer("conn-w2a", 7, 101) {
		t.Fatal("bind player")
	}
	target := &logicrpcpb.DownlinkTargetFence{
		TargetPlayerId: 101, GatewayId: "gate-a", GatewayBootId: "boot-a",
		ConnectionId: "conn-w2a", ConnectionGeneration: 7,
		SessionEpoch: 11, SessionRouteRevision: 13,
	}
	if !registry.BindSessionTarget(target) {
		t.Fatal("bind target fence")
	}
	registry.SetNextSequence("conn-w2a", 7, 2)
	if !registry.PrepareJoinAttempt("conn-w2a", 7, "join-op", 202, "join-attempt") {
		t.Fatal("prepare join")
	}
	accepted := &commonpb.CommandResultEnvelope{
		OperationId: "join-op", RequestId: "join-request", CommandType: commonpb.CommandType_JOIN_FARM,
		Status: commonpb.CommandStatus_ACCEPTED,
		ResultContext: &commonpb.CommandResultEnvelope_Join{Join: &commonpb.JoinResultContext{
			TargetFarmId: 202, SubscriptionId: "sub-w2a", JoinAttemptId: "join-attempt",
		}},
	}
	if !registry.QueueJoinAccepted("conn-w2a", 7, "join-op", 202, "join-attempt", "sub-w2a", accepted) {
		t.Fatal("queue accepted")
	}
	if !registry.MarkSnapshotArmRequested("conn-w2a", 7, "join-op", 202, "join-attempt", "sub-w2a") {
		t.Fatal("mark arm")
	}
	fence := &logicrpcpb.SubscriptionFence{FarmId: 202, SubscriptionId: "sub-w2a", JoinAttemptId: "join-attempt"}
	snapshotFrame := w2aPushFrame(t, commonpb.PushType_FARM_SNAPSHOT, target, fence,
		&farmpb.FarmSnapshotPush{JoinAttemptId: "join-attempt", SnapshotVersion: 10,
			Snapshot: &farmpb.FarmSnapshot{OwnerId: 202, FarmVersion: 10}})
	snapshotResult, err := registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{
		Items: []*logicrpcpb.DownlinkItem{{
			ItemId: "snapshot-10", SubscriptionId: "sub-w2a", GatewayId: "gate-a",
			ConnectionId: "conn-w2a", TransportGeneration: 7, PresenceEpoch: 17,
			Frame: snapshotFrame, JoinFence: fence, TargetFence: target, SnapshotVersion: 10,
		}},
	})
	if err != nil || snapshotResult.GetResults()[0].GetStatus() != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("snapshot result=%v err=%v", snapshotResult, err)
	}
	ready, err := registry.ActivateSubscription(context.Background(), &logicrpcpb.ActivateSubscriptionRequest{
		Subscription: fence, Target: target, ReadyThroughVersion: 10,
	})
	if err != nil || ready.GetStatus() != logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED {
		t.Fatalf("activate=%v err=%v", ready, err)
	}
	// 排空 ACCEPTED/SNAPSHOT/SUBSCRIPTION_READY,只观察 post-ready 帧。
	for i := 0; i < 3; i++ {
		if _, ok := registry.Dequeue("conn-w2a", 7); !ok {
			t.Fatalf("missing pre-ready frame %d", i)
		}
	}

	change := &farmpb.FarmChange{OwnerId: 202, FarmVersion: 11}
	changeFrame := w2aPushFrame(t, commonpb.PushType_FARM_CHANGE, target, fence,
		&farmpb.FarmChangePush{Change: change})
	deliver := func(item *logicrpcpb.DownlinkItem) logicrpcpb.DownlinkItemStatus {
		t.Helper()
		result, err := registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{
			Items: []*logicrpcpb.DownlinkItem{item},
		})
		if err != nil || len(result.GetResults()) != 1 {
			t.Fatalf("deliver result=%v err=%v", result, err)
		}
		return result.GetResults()[0].GetStatus()
	}

	// 完整 Fence(含 PresenceEpoch):激活后必须被接受,不得判 stale。
	if got := deliver(&logicrpcpb.DownlinkItem{
		ItemId: "post-ready-11", SubscriptionId: "sub-w2a", GatewayId: "gate-a",
		ConnectionId: "conn-w2a", TransportGeneration: 7, PresenceEpoch: 17,
		Frame: changeFrame, JoinFence: fence, TargetFence: target,
		FromVersion: 11, ThroughVersion: 11,
	}); got != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("fully fenced post-ready change rejected: %s", got)
	}
	raw, ok := registry.Dequeue("conn-w2a", 7)
	if !ok {
		t.Fatal("post-ready change was not written to the connection writer")
	}
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(raw, &frame); err != nil {
		t.Fatal(err)
	}
	if got := frame.GetServerPush().GetPushType(); got != commonpb.PushType_FARM_CHANGE {
		t.Fatalf("post-ready push_type=%s, want FARM_CHANGE", got)
	}

	// 连接代际不匹配:判 STALE_CONNECTION。
	if got := deliver(&logicrpcpb.DownlinkItem{
		ItemId: "post-ready-stale-gen", SubscriptionId: "sub-w2a", GatewayId: "gate-a",
		ConnectionId: "conn-w2a", TransportGeneration: 8, PresenceEpoch: 17,
		Frame: changeFrame, JoinFence: fence, TargetFence: target,
		FromVersion: 12, ThroughVersion: 12,
	}); got != logicrpcpb.DownlinkItemStatus_STALE_CONNECTION {
		t.Fatalf("stale generation must fail closed: %s", got)
	}
	// Join 尝试不匹配:判 STALE_SUBSCRIPTION。
	if got := deliver(&logicrpcpb.DownlinkItem{
		ItemId: "post-ready-stale-attempt", SubscriptionId: "sub-w2a", GatewayId: "gate-a",
		ConnectionId: "conn-w2a", TransportGeneration: 7, PresenceEpoch: 17,
		Frame:       changeFrame,
		JoinFence:   &logicrpcpb.SubscriptionFence{FarmId: 202, SubscriptionId: "sub-w2a", JoinAttemptId: "old-attempt"},
		TargetFence: target, FromVersion: 12, ThroughVersion: 12,
	}); got != logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION {
		t.Fatalf("stale join attempt must fail closed: %s", got)
	}
}

func w2aPushFrame(t *testing.T, pushType commonpb.PushType, target *logicrpcpb.DownlinkTargetFence,
	fence *logicrpcpb.SubscriptionFence, payload proto.Message) []byte {
	t.Helper()
	payloadBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(
		gateprotocol.NewServerPushFrame(1, &commonpb.ServerPushEnvelope{
			PushType: pushType,
			PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{
				FarmSubscription: &commonpb.FarmSubscriptionPushContext{
					TargetPlayerId: target.TargetPlayerId, TargetFarmId: fence.FarmId,
					SubscriptionId: fence.SubscriptionId,
				},
			},
			Payload: payloadBytes,
		}))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
