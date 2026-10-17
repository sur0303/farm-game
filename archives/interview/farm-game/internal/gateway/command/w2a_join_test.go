package command

import (
	"context"
	"errors"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/gateway/auth"
	"farmgame/internal/gateway/downlink"
	gateprotocol "farmgame/internal/gateway/protocol"

	"google.golang.org/protobuf/proto"
)

type w2aJoinLogic struct {
	registry *downlink.ConnectionRegistry
	accepted chan []byte
	done     chan error
}

func (l *w2aJoinLogic) ExecuteCommand(_ context.Context,
	req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	if req.GetCommand().GetCommandType() != commonpb.CommandType_JOIN_FARM || req.GetTrusted().GetJoinAttempt() == nil {
		return &logicrpcpb.ExecuteCommandResponse{Outcome: &logicrpcpb.ExecuteCommandResponse_Business{
			Business: &logicrpcpb.BusinessCommandOutcome{Status: commonpb.CommandStatus_SUCCEEDED},
		}}, nil
	}
	join := req.Trusted.JoinAttempt
	return &logicrpcpb.ExecuteCommandResponse{Outcome: &logicrpcpb.ExecuteCommandResponse_Business{
		Business: &logicrpcpb.BusinessCommandOutcome{
			Status: commonpb.CommandStatus_ACCEPTED,
			OutcomeContext: &logicrpcpb.BusinessCommandOutcome_JoinAccepted{
				JoinAccepted: &logicrpcpb.JoinAcceptedMetadata{
					TargetFarmId: join.TargetFarmId, SubscriptionId: "sub-command-w2a",
					JoinAttemptId: join.JoinAttemptId, AcceptedSubscriptionTtlMs: 30_000,
				},
			},
		},
	}}, nil
}

func (l *w2aJoinLogic) ArmSnapshot(_ context.Context,
	req *logicrpcpb.ArmSnapshotRequest) (*logicrpcpb.ArmSnapshotResponse, error) {
	accepted, ok := l.registry.Dequeue(req.Target.ConnectionId, req.Target.ConnectionGeneration)
	if !ok {
		l.done <- errors.New("ArmSnapshot ran before ACCEPTED entered ConnectionWriter")
		return &logicrpcpb.ArmSnapshotResponse{Status: logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_UNAVAILABLE}, nil
	}
	l.accepted <- accepted
	snapshot := &farmpb.FarmSnapshot{OwnerId: req.Subscription.FarmId, FarmVersion: 0}
	frame := commandW2APushFrame(commonpb.PushType_FARM_SNAPSHOT, req.Target, req.Subscription,
		&farmpb.FarmSnapshotPush{JoinAttemptId: req.Subscription.JoinAttemptId, SnapshotVersion: 0, Snapshot: snapshot})
	delivered, err := l.registry.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{
		Items: []*logicrpcpb.DownlinkItem{{
			ItemId: "snapshot-command-w2a", SubscriptionId: req.Subscription.SubscriptionId,
			GatewayId: req.Target.GatewayId, ConnectionId: req.Target.ConnectionId,
			TransportGeneration: req.Target.ConnectionGeneration, PresenceEpoch: 19,
			Frame: frame, JoinFence: req.Subscription, TargetFence: req.Target, SnapshotVersion: 0,
		}},
	})
	if err != nil || delivered.GetResults()[0].GetStatus() != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		l.done <- errors.New("snapshot was not queue accepted")
		return &logicrpcpb.ArmSnapshotResponse{Status: logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_UNAVAILABLE}, nil
	}
	ready, err := l.registry.ActivateSubscription(context.Background(), &logicrpcpb.ActivateSubscriptionRequest{
		Subscription: req.Subscription, Target: req.Target, ReadyThroughVersion: 0,
	})
	if err != nil || ready.GetStatus() != logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED {
		l.done <- errors.New("Ready was not queue accepted")
		return &logicrpcpb.ArmSnapshotResponse{Status: logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_UNAVAILABLE}, nil
	}
	l.done <- nil
	version := uint64(0)
	return &logicrpcpb.ArmSnapshotResponse{
		Status: logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_ARMED, SnapshotVersion: &version,
	}, nil
}

func TestGateW2AQueuesAcceptedBeforeArmAndUsesNoSyncJoinResponse(t *testing.T) {
	registry := downlink.NewConnectionRegistry()
	logic := &w2aJoinLogic{registry: registry, accepted: make(chan []byte, 1), done: make(chan error, 1)}
	authService := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	login, err := authService.Login(context.Background(), auth.LoginRequest{
		PlatformToken: "w2a-player", BootstrapID: "w2a-player",
	})
	if err != nil {
		t.Fatal(err)
	}
	gate := NewGate(authService, logic, 64<<10)
	gate.SetDownlinkRegistry(registry)
	helloBody, _ := proto.Marshal(&commonpb.ClientHello{
		ConnectionTicket: login.ConnectionTicket, ClientBuild: "farm-h5/0.3",
	})
	hello := clientFrameRaw(t, testFrameClientHello, 1, helloBody)
	if response, err := gate.HandleBinary(context.Background(), "conn-command-w2a", 7, hello); err != nil || len(response) == 0 {
		t.Fatalf("hello response=%d err=%v", len(response), err)
	}
	joinBody, _ := proto.Marshal(&commonpb.ClientCommandEnvelope{
		OperationId: "join-command-w2a", RequestId: "join-command-request",
		CommandType: commonpb.CommandType_JOIN_FARM, Payload: joinFarmPayload(t, login.PlayerID),
	})
	joinFrame := clientFrameRaw(t, testFrameClientCommand, 2, joinBody)
	response, err := gate.HandleBinary(context.Background(), "conn-command-w2a", 7, joinFrame)
	if err != nil || len(response) != 0 {
		t.Fatalf("join synchronous response=%d err=%v", len(response), err)
	}
	select {
	case err := <-logic.done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ArmSnapshot did not finish")
	}
	accepted := <-logic.accepted
	assertCommandW2AFrame(t, accepted, 2, testFrameCommandResult,
		commonpb.PushType_PUSH_TYPE_UNSPECIFIED)
	snapshot, ok := registry.Dequeue("conn-command-w2a", 7)
	if !ok {
		t.Fatal("missing Snapshot")
	}
	ready, ok := registry.Dequeue("conn-command-w2a", 7)
	if !ok {
		t.Fatal("missing Ready")
	}
	assertCommandW2AFrame(t, snapshot, 3, testFrameServerPush,
		commonpb.PushType_FARM_SNAPSHOT)
	assertCommandW2AFrame(t, ready, 4, testFrameServerPush,
		commonpb.PushType_SUBSCRIPTION_READY)
	gate.mu.Lock()
	current := gate.connections["conn-command-w2a"]
	gate.mu.Unlock()
	if current.currentFarmID != login.PlayerID || current.currentSubscriptionID != "sub-command-w2a" {
		t.Fatalf("active farm/subscription=%d/%q", current.currentFarmID, current.currentSubscriptionID)
	}
}

func commandW2APushFrame(pushType commonpb.PushType, target *logicrpcpb.DownlinkTargetFence,
	fence *logicrpcpb.SubscriptionFence, payload proto.Message) []byte {
	payloadBytes, _ := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(
		gateprotocol.NewServerPushFrame(1, &commonpb.ServerPushEnvelope{
			PushType: pushType,
			PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{
				FarmSubscription: &commonpb.FarmSubscriptionPushContext{
					TargetPlayerId: target.TargetPlayerId, TargetFarmId: fence.FarmId,
					SubscriptionId: fence.SubscriptionId,
				},
			}, Payload: payloadBytes,
		}))
	return raw
}

// assertCommandW2AFrame 校验下行顺序帧:CommandResult 与 ServerPush 是顶层
// oneof 的不同分支(28 §2.1),kind 只用于区分期望类别。
func assertCommandW2AFrame(t *testing.T, raw []byte, sequence uint64,
	kind testFrameKind, pushType commonpb.PushType) {
	t.Helper()
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(raw, &frame); err != nil {
		t.Fatal(err)
	}
	var category testFrameKind
	switch {
	case frame.GetCommandResult() != nil:
		category = testFrameCommandResult
	case frame.GetServerPush() != nil:
		category = testFrameServerPush
	case frame.GetServerHello() != nil:
		category = testFrameServerHello
	}
	if frame.GetSequence() != sequence || category != kind {
		t.Fatalf("frame seq/category=%d/%v want=%d/%v", frame.GetSequence(), category, sequence, kind)
	}
	if kind == testFrameServerPush {
		push := frame.GetServerPush()
		if push == nil {
			t.Fatal("missing server push")
		}
		if push.PushType != pushType {
			t.Fatalf("push=%s want=%s", push.PushType, pushType)
		}
	}
}
