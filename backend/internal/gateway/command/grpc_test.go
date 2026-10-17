package command

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/gateway/auth"
	"farmgame/internal/gateway/downlink"
	"farmgame/internal/gateway/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type grpcLogicStub struct {
	logicrpcpb.UnimplementedLogicServiceServer
	downlink logicrpcpb.GatewayDownlinkServiceClient
	cleanup  chan *logicrpcpb.CleanupSubscriptionRequest
	joinDone chan error
}

func (s *grpcLogicStub) CleanupSubscription(_ context.Context, req *logicrpcpb.CleanupSubscriptionRequest) (*logicrpcpb.CleanupSubscriptionResponse, error) {
	if s.cleanup != nil {
		s.cleanup <- proto.Clone(req).(*logicrpcpb.CleanupSubscriptionRequest)
	}
	return &logicrpcpb.CleanupSubscriptionResponse{Removed: true}, nil
}

func (s *grpcLogicStub) ExecuteCommand(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	status := commonpb.CommandStatus_SUCCEEDED
	business := &logicrpcpb.BusinessCommandOutcome{Status: status}
	if req.Command.CommandType == commonpb.CommandType_JOIN_FARM {
		join := req.GetTrusted().GetJoinAttempt()
		if join == nil {
			return nil, errors.New("grpcLogicStub: missing JoinAdmissionContext")
		}
		business.Status = commonpb.CommandStatus_ACCEPTED
		business.OutcomeContext = &logicrpcpb.BusinessCommandOutcome_JoinAccepted{
			JoinAccepted: &logicrpcpb.JoinAcceptedMetadata{
				TargetFarmId: join.TargetFarmId, SubscriptionId: "server-subscription",
				JoinAttemptId: join.JoinAttemptId, AcceptedSubscriptionTtlMs: 30_000,
			},
		}
	} else {
		business.ResultPayload = mustMarshal(&farmpb.FarmCommandResult{})
	}
	return &logicrpcpb.ExecuteCommandResponse{Outcome: &logicrpcpb.ExecuteCommandResponse_Business{
		Business: business,
	}}, nil
}

func (s *grpcLogicStub) ArmSnapshot(ctx context.Context,
	req *logicrpcpb.ArmSnapshotRequest) (*logicrpcpb.ArmSnapshotResponse, error) {
	fail := func(err error) (*logicrpcpb.ArmSnapshotResponse, error) {
		if s.joinDone != nil {
			s.joinDone <- err
		}
		return &logicrpcpb.ArmSnapshotResponse{Status: logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_UNAVAILABLE}, nil
	}
	if s.downlink == nil || req.GetSubscription() == nil || req.GetTarget() == nil {
		return fail(errors.New("grpcLogicStub: incomplete ArmSnapshot"))
	}
	snapshot := &farmpb.FarmSnapshot{OwnerId: req.Subscription.FarmId, FarmVersion: 0}
	frame := commandW2APushFrame(commonpb.PushType_FARM_SNAPSHOT, req.Target, req.Subscription,
		&farmpb.FarmSnapshotPush{JoinAttemptId: req.Subscription.JoinAttemptId, SnapshotVersion: 0, Snapshot: snapshot})
	delivered, err := s.downlink.DeliverBatch(ctx, &logicrpcpb.DeliverBatchRequest{
		Items: []*logicrpcpb.DownlinkItem{{
			ItemId: "remote-join-snapshot", SubscriptionId: req.Subscription.SubscriptionId,
			GatewayId: req.Target.GatewayId, ConnectionId: req.Target.ConnectionId,
			TransportGeneration: req.Target.ConnectionGeneration, PresenceEpoch: 1,
			Frame: frame, JoinFence: req.Subscription, TargetFence: req.Target, SnapshotVersion: 0,
		}},
	})
	if err != nil || delivered.GetResults()[0].GetStatus() != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		return fail(errors.New("grpcLogicStub: snapshot was not queue accepted"))
	}
	ready, err := s.downlink.ActivateSubscription(ctx, &logicrpcpb.ActivateSubscriptionRequest{
		Subscription: req.Subscription, Target: req.Target, ReadyThroughVersion: 0,
	})
	if err != nil || ready.GetStatus() != logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED {
		return fail(errors.New("grpcLogicStub: Ready was not queue accepted"))
	}
	if s.joinDone != nil {
		s.joinDone <- nil
	}
	version := uint64(0)
	return &logicrpcpb.ArmSnapshotResponse{
		Status: logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_ARMED, SnapshotVersion: &version,
	}, nil
}

func TestGateForwardsThroughGRPCLogicService(t *testing.T) {
	logicServer := grpc.NewServer()
	stub := &grpcLogicStub{cleanup: make(chan *logicrpcpb.CleanupSubscriptionRequest, 1)}
	logicrpcpb.RegisterLogicServiceServer(logicServer, stub)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = logicServer.Serve(lis) }()
	defer logicServer.Stop()
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	login, err := authSvc.Login(ctx, auth.LoginRequest{PlatformToken: "p", BootstrapID: "boot"})
	if err != nil {
		t.Fatal(err)
	}
	gate := NewGate(authSvc, NewGRPCLogicClient(logicrpcpb.NewLogicServiceClient(conn)), 64<<10)
	hello, _ := proto.Marshal(&commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket, ClientBuild: "farm-h5/0.2"})
	raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
	if _, err := gate.HandleBinary(ctx, "grpc-conn", 1, raw); err != nil {
		t.Fatal(err)
	}
	setRecoveryCompleteForTest(t, gate, "grpc-conn", 1, true)
	buyPayload, _ := proto.Marshal(&publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1})
	commandBody, _ := proto.Marshal(&commonpb.ClientCommandEnvelope{OperationId: "grpc-op", RequestId: "grpc-req", CommandType: commonpb.CommandType_BUY, Payload: buyPayload})
	raw = clientFrameRaw(t, testFrameClientCommand, 2, commandBody)
	response, err := gate.HandleBinary(ctx, "grpc-conn", 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.Decode(response, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	result := frame.GetCommandResult()
	if result == nil {
		t.Fatal("response is not a command result")
	}
	if result.RequestId != "grpc-req" || result.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("result request=%q status=%s", result.RequestId, result.Status)
	}
	client := NewGRPCLogicClient(logicrpcpb.NewLogicServiceClient(conn))
	exact, ok := client.(SubscriptionCleaner)
	if !ok {
		t.Fatal("gRPC Logic client does not expose exact cleanup")
	}
	cleanupResponse, err := exact.CleanupSubscription(ctx, &logicrpcpb.CleanupSubscriptionRequest{
		Subscription: &logicrpcpb.SubscriptionFence{FarmId: 101, SubscriptionId: "sub", JoinAttemptId: "attempt"},
		Target:       &logicrpcpb.DownlinkTargetFence{TargetPlayerId: 7, GatewayId: "gw", ConnectionId: "conn", ConnectionGeneration: 3},
	})
	if err != nil || cleanupResponse == nil || !cleanupResponse.Removed {
		t.Fatalf("cleanup response=%v err=%v", cleanupResponse, err)
	}
	if forwarded := <-stub.cleanup; forwarded.GetSubscription().GetFarmId() != 101 || forwarded.GetSubscription().GetSubscriptionId() != "sub" || forwarded.GetSubscription().GetJoinAttemptId() != "attempt" {
		t.Fatalf("forwarded cleanup=%+v", forwarded)
	}
}

func TestRemoteJoinDeliversSnapshotThroughGatewayDownlinkGRPC(t *testing.T) {
	// GatewayDownlinkService is a separate bufconn-backed process here; Logic
	// only sees the generated gRPC client and never the gateway registry type.
	gatewayDownlink := downlink.NewConnectionRegistry()
	downlinkServer := grpc.NewServer()
	logicrpcpb.RegisterGatewayDownlinkServiceServer(downlinkServer, gatewayDownlink)
	downlinkLis := bufconn.Listen(1 << 20)
	go func() { _ = downlinkServer.Serve(downlinkLis) }()
	defer downlinkServer.Stop()
	downlinkConn, err := grpc.DialContext(context.Background(), "downlink-bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return downlinkLis.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer downlinkConn.Close()
	logicServer := grpc.NewServer()
	stub := &grpcLogicStub{
		downlink: logicrpcpb.NewGatewayDownlinkServiceClient(downlinkConn),
		joinDone: make(chan error, 1),
	}
	logicrpcpb.RegisterLogicServiceServer(logicServer, stub)
	logicLis := bufconn.Listen(1 << 20)
	go func() { _ = logicServer.Serve(logicLis) }()
	defer logicServer.Stop()
	logicConn, err := grpc.DialContext(context.Background(), "logic-bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return logicLis.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer logicConn.Close()

	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	login, err := authSvc.Login(context.Background(), auth.LoginRequest{PlatformToken: "p", BootstrapID: "remote-join"})
	if err != nil {
		t.Fatal(err)
	}
	gateway := NewGate(authSvc, NewGRPCLogicClient(logicrpcpb.NewLogicServiceClient(logicConn)), 64<<10)
	gateway.SetDownlinkRegistry(gatewayDownlink)

	hello, _ := proto.Marshal(&commonpb.ClientHello{ConnectionTicket: login.ConnectionTicket, ClientBuild: "farm-h5/0.2"})
	raw := clientFrameRaw(t, testFrameClientHello, 1, hello)
	if _, err := gateway.HandleBinary(context.Background(), "remote-c", 1, raw); err != nil {
		t.Fatal(err)
	}
	join, _ := proto.Marshal(&commonpb.ClientCommandEnvelope{OperationId: "remote-join-op", RequestId: "remote-join-req", CommandType: commonpb.CommandType_JOIN_FARM, Payload: joinFarmPayload(t, 101)})
	raw = clientFrameRaw(t, testFrameClientCommand, 2, join)
	response, err := gateway.HandleBinary(context.Background(), "remote-c", 1, raw)
	if err != nil || len(response) != 0 {
		t.Fatalf("Join synchronous response=%d err=%v", len(response), err)
	}
	select {
	case err := <-stub.joinDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("remote ArmSnapshot did not finish")
	}

	accepted, ok := gatewayDownlink.Dequeue("remote-c", 1)
	if !ok {
		t.Fatal("remote Join ACCEPTED was not queued")
	}
	snapshot, ok := gatewayDownlink.Dequeue("remote-c", 1)
	if !ok {
		t.Fatal("remote Join Snapshot was not queued")
	}
	ready, ok := gatewayDownlink.Dequeue("remote-c", 1)
	if !ok {
		t.Fatal("remote Join Ready was not queued")
	}
	assertCommandW2AFrame(t, accepted, 2, testFrameCommandResult,
		commonpb.PushType_PUSH_TYPE_UNSPECIFIED)
	assertCommandW2AFrame(t, snapshot, 3, testFrameServerPush,
		commonpb.PushType_FARM_SNAPSHOT)
	assertCommandW2AFrame(t, ready, 4, testFrameServerPush,
		commonpb.PushType_SUBSCRIPTION_READY)
	var acceptedFrame commonpb.FarmFrame
	if err := proto.Unmarshal(accepted, &acceptedFrame); err != nil {
		t.Fatal(err)
	}
	result := acceptedFrame.GetCommandResult()
	if result == nil {
		t.Fatal("accepted frame is not a command result")
	}
	if result.Status != commonpb.CommandStatus_ACCEPTED || result.GetJoin() == nil || len(result.Payload) != 0 {
		t.Fatalf("Join ACCEPTED=%v", result)
	}
}
