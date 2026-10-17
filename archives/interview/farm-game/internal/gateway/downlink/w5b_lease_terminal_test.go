package downlink

import (
	"context"
	"testing"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
)

// W5B(26 §11.5):PendingJoin 在 Snapshot/Ready 完成前已持有真实 Holder
// Presence,因此也是可续租租约。租约过期的 REJOIN_REQUIRED 在 Pending 阶段
// 必须与 Active 阶段一样走 ConnectionWriter FIFO(控制保留通道),
// PresenceEpoch 不是续租 RPC 的一部分,Pending 终态帧允许携带 0。
func TestW5BRejoinRequiredQueuedDuringPendingJoin(t *testing.T) {
	registry := NewConnectionRegistry()
	writer := NewConnectionWriter(7, 16, 1<<20)
	registry.Bind("conn-w5b", "", 7, writer)
	if !registry.BindPlayer("conn-w5b", 7, 101) {
		t.Fatal("bind player")
	}
	target := &logicrpcpb.DownlinkTargetFence{
		TargetPlayerId: 101, GatewayId: "gate-a", GatewayBootId: "boot-a",
		ConnectionId: "conn-w5b", ConnectionGeneration: 7,
		SessionEpoch: 11, SessionRouteRevision: 13,
	}
	if !registry.BindSessionTarget(target) {
		t.Fatal("bind target")
	}
	if !registry.PrepareJoinAttempt("conn-w5b", 7, "op-pending", 202, "attempt-p") {
		t.Fatal("prepare join")
	}
	accepted := &commonpb.CommandResultEnvelope{
		OperationId: "op-pending", CommandType: commonpb.CommandType_JOIN_FARM,
		Status: commonpb.CommandStatus_ACCEPTED,
		ResultContext: &commonpb.CommandResultEnvelope_Join{Join: &commonpb.JoinResultContext{
			TargetFarmId: 202, SubscriptionId: "sub-p", JoinAttemptId: "attempt-p",
		}},
	}
	if !registry.QueueJoinAccepted("conn-w5b", 7, "op-pending", 202, "attempt-p", "sub-p", accepted) {
		t.Fatal("queue accepted")
	}
	drainWriter(t, registry, "conn-w5b", 7, 1)

	var termination *SubscriptionTermination
	registry.SetSubscriptionTerminationHandler(func(got SubscriptionTermination) {
		copy := got
		termination = &copy
	})
	fence := &logicrpcpb.SubscriptionFence{FarmId: 202, SubscriptionId: "sub-p", JoinAttemptId: "attempt-p"}
	rejoinFrame := w2aPushFrame(t, commonpb.PushType_REJOIN_REQUIRED, target, fence,
		&farmpb.RejoinRequiredPush{Reason: farmpb.RejoinReason_REJOIN_REASON_SUBSCRIPTION_LEASE_LOST})
	response, err := registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{
		Items: []*logicrpcpb.DownlinkItem{{
			ItemId: "lease-expired-pending", SubscriptionId: "sub-p",
			GatewayId:    target.GatewayId,
			ConnectionId: "conn-w5b", TransportGeneration: 7,
			Frame: rejoinFrame, JoinFence: fence, TargetFence: target,
		}},
	})
	if err != nil || len(response.Results) != 1 ||
		response.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("pending terminal delivery=%v err=%v", response, err)
	}
	if termination == nil || termination.SubscriptionID != "sub-p" ||
		termination.JoinAttemptID != "attempt-p" || termination.ConnectionClosed {
		t.Fatalf("termination=%+v", termination)
	}
	assertQueuedPush(t, registry, target, commonpb.PushType_REJOIN_REQUIRED)
}

// Pending 阶段前置:ACCEPTED 尚未入队(stage < AcceptedQueued)时不存在可续租
// Presence,终态帧必须被拒绝,不能污染尚未建立的订阅。
func TestW5BRejoinRequiredRejectedBeforeAccepted(t *testing.T) {
	registry := NewConnectionRegistry()
	writer := NewConnectionWriter(7, 16, 1<<20)
	registry.Bind("conn-w5b", "", 7, writer)
	if !registry.BindPlayer("conn-w5b", 7, 101) {
		t.Fatal("bind player")
	}
	target := &logicrpcpb.DownlinkTargetFence{
		TargetPlayerId: 101, GatewayId: "gate-a", GatewayBootId: "boot-a",
		ConnectionId: "conn-w5b", ConnectionGeneration: 7,
		SessionEpoch: 11, SessionRouteRevision: 13,
	}
	if !registry.BindSessionTarget(target) {
		t.Fatal("bind target")
	}
	if !registry.PrepareJoinAttempt("conn-w5b", 7, "op-early", 202, "attempt-early") {
		t.Fatal("prepare join")
	}
	fence := &logicrpcpb.SubscriptionFence{FarmId: 202, SubscriptionId: "sub-early", JoinAttemptId: "attempt-early"}
	rejoinFrame := w2aPushFrame(t, commonpb.PushType_REJOIN_REQUIRED, target, fence,
		&farmpb.RejoinRequiredPush{Reason: farmpb.RejoinReason_REJOIN_REASON_SUBSCRIPTION_LEASE_LOST})
	response, err := registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{
		Items: []*logicrpcpb.DownlinkItem{{
			ItemId: "lease-expired-early", SubscriptionId: "sub-early",
			GatewayId:    target.GatewayId,
			ConnectionId: "conn-w5b", TransportGeneration: 7,
			Frame: rejoinFrame, JoinFence: fence, TargetFence: target,
		}},
	})
	if err != nil || response.Results[0].Status != logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION {
		t.Fatalf("pre-accepted terminal=%v err=%v", response, err)
	}
}

// Pending 阶段 Fence 必须精确:过期 attempt 的迟到终态帧不得终止新 attempt。
func TestW5BRejoinRequiredStaleAttemptRejectedDuringPending(t *testing.T) {
	registry := NewConnectionRegistry()
	writer := NewConnectionWriter(7, 16, 1<<20)
	registry.Bind("conn-w5b", "", 7, writer)
	if !registry.BindPlayer("conn-w5b", 7, 101) {
		t.Fatal("bind player")
	}
	target := &logicrpcpb.DownlinkTargetFence{
		TargetPlayerId: 101, GatewayId: "gate-a", GatewayBootId: "boot-a",
		ConnectionId: "conn-w5b", ConnectionGeneration: 7,
		SessionEpoch: 11, SessionRouteRevision: 13,
	}
	if !registry.BindSessionTarget(target) {
		t.Fatal("bind target")
	}
	if !registry.PrepareJoinAttempt("conn-w5b", 7, "op-new", 202, "attempt-new") {
		t.Fatal("prepare join")
	}
	accepted := &commonpb.CommandResultEnvelope{
		OperationId: "op-new", CommandType: commonpb.CommandType_JOIN_FARM,
		Status: commonpb.CommandStatus_ACCEPTED,
		ResultContext: &commonpb.CommandResultEnvelope_Join{Join: &commonpb.JoinResultContext{
			TargetFarmId: 202, SubscriptionId: "sub-new", JoinAttemptId: "attempt-new",
		}},
	}
	if !registry.QueueJoinAccepted("conn-w5b", 7, "op-new", 202, "attempt-new", "sub-new", accepted) {
		t.Fatal("queue accepted")
	}
	drainWriter(t, registry, "conn-w5b", 7, 1)

	staleFence := &logicrpcpb.SubscriptionFence{FarmId: 202, SubscriptionId: "sub-new", JoinAttemptId: "attempt-old"}
	rejoinFrame := w2aPushFrame(t, commonpb.PushType_REJOIN_REQUIRED, target, staleFence,
		&farmpb.RejoinRequiredPush{Reason: farmpb.RejoinReason_REJOIN_REASON_SUBSCRIPTION_LEASE_LOST})
	response, err := registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{
		Items: []*logicrpcpb.DownlinkItem{{
			ItemId: "lease-expired-stale", SubscriptionId: "sub-new",
			GatewayId:    target.GatewayId,
			ConnectionId: "conn-w5b", TransportGeneration: 7,
			Frame: rejoinFrame, JoinFence: staleFence, TargetFence: target,
		}},
	})
	if err != nil || response.Results[0].Status != logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION {
		t.Fatalf("stale attempt terminal=%v err=%v", response, err)
	}
	// 新 attempt 未被误终止:后续同 Fence 终态帧仍可入队。
	fence := &logicrpcpb.SubscriptionFence{FarmId: 202, SubscriptionId: "sub-new", JoinAttemptId: "attempt-new"}
	validFrame := w2aPushFrame(t, commonpb.PushType_REJOIN_REQUIRED, target, fence,
		&farmpb.RejoinRequiredPush{Reason: farmpb.RejoinReason_REJOIN_REASON_SUBSCRIPTION_LEASE_LOST})
	again, err := registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{
		Items: []*logicrpcpb.DownlinkItem{{
			ItemId: "lease-expired-valid", SubscriptionId: "sub-new",
			GatewayId:    target.GatewayId,
			ConnectionId: "conn-w5b", TransportGeneration: 7,
			Frame: validFrame, JoinFence: fence, TargetFence: target,
		}},
	})
	if err != nil || again.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("valid terminal after stale=%v err=%v", again, err)
	}
}
