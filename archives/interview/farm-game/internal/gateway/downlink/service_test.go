package downlink

import (
	"context"
	"net"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	friendpb "farmgame/api/gen/friend"
	logicrpcpb "farmgame/api/gen/logicrpc"
	gateprotocol "farmgame/internal/gateway/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func TestConnectionRegistryRejectsCancelledOrNilDelivery(t *testing.T) {
	r := NewConnectionRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resp, err := r.DeliverBatch(ctx, &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{nil}})
	if err != nil || len(resp.Results) != 1 || resp.Results[0].Status != logicrpcpb.DownlinkItemStatus_UNAVAILABLE {
		t.Fatalf("cancelled/nil delivery=%v err=%v", resp, err)
	}
}

func TestDrainNoticeUsesExistingWriterAndSequence(t *testing.T) {
	r := NewConnectionRegistry()
	w := NewConnectionWriter(3, 4, 1<<20)
	r.BindWithPresence("connection-a", "subscription-a", 3, 8, w)
	r.SetNextSequence("connection-a", 3, 12)
	if got := r.EnqueueDrainNotice("drain-1", 2*time.Second, time.UnixMilli(12345)); got != 1 {
		t.Fatalf("notices=%d, want 1", got)
	}
	item, ok := w.Dequeue()
	if !ok {
		t.Fatal("missing queued DrainNotice")
	}
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(item.Data, &frame); err != nil {
		t.Fatal(err)
	}
	if frame.GetSequence() != 12 || frame.GetServerPush() == nil {
		t.Fatalf("frame=%v", &frame)
	}
	push := frame.GetServerPush()
	if push == nil {
		t.Fatal("missing server push in frame")
	}
	if push.PushType != commonpb.PushType_DRAIN_NOTICE || push.GetSession() == nil {
		t.Fatalf("push=%v", &push)
	}
	var notice commonpb.DrainNotice
	if err := proto.Unmarshal(push.Payload, &notice); err != nil {
		t.Fatal(err)
	}
	if notice.DrainId != "drain-1" || notice.DeadlineMs != 12345 || notice.ReconnectAfterMs > 2000 {
		t.Fatalf("notice=%v", &notice)
	}
}

func TestFriendRosterChangeTargetsAuthenticatedPlayerConnections(t *testing.T) {
	r := NewConnectionRegistry()
	first := NewConnectionWriter(3, 4, 1<<20)
	second := NewConnectionWriter(4, 4, 1<<20)
	other := NewConnectionWriter(5, 4, 1<<20)
	r.Bind("first", "farm-a", 3, first)
	r.Bind("second", "farm-b", 4, second)
	r.Bind("other", "farm-c", 5, other)
	if !r.BindPlayer("first", 3, 10001) || !r.BindPlayer("second", 4, 10001) || !r.BindPlayer("other", 5, 10002) {
		t.Fatal("failed to bind authenticated players")
	}
	r.SetNextSequence("first", 3, 9)

	if got := r.NotifyFriendRosterChanged(10001, 10002, 7, friendpb.FriendMutationKind_ACCEPT_SHARE); got != 2 {
		t.Fatalf("enqueued=%d, want 2", got)
	}
	if _, ok := other.Dequeue(); ok {
		t.Fatal("notification leaked to another player")
	}
	item, ok := first.Dequeue()
	if !ok {
		t.Fatal("missing first notification")
	}
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(item.Data, &frame); err != nil {
		t.Fatal(err)
	}
	if frame.GetSequence() != 9 {
		t.Fatalf("sequence=%d, want 9", frame.GetSequence())
	}
	push := frame.GetServerPush()
	if push == nil {
		t.Fatal("missing server push in frame")
	}
	if push.PushType != commonpb.PushType_FRIEND_ROSTER_CHANGED {
		t.Fatalf("push type=%s", push.PushType)
	}
	var changed friendpb.FriendRosterChanged
	if err := proto.Unmarshal(push.Payload, &changed); err != nil {
		t.Fatal(err)
	}
	if changed.CounterpartPlayerId != 10002 || changed.RelationEpoch != 7 || changed.Mutation != friendpb.FriendMutationKind_ACCEPT_SHARE {
		t.Fatalf("changed=%v", &changed)
	}
}

func TestFriendOpportunityHintTargetsPlayerAndContainsNoActionMask(t *testing.T) {
	r := NewConnectionRegistry()
	writer := NewConnectionWriter(3, 4, 1<<20)
	r.Bind("friend", "farm-a", 3, writer)
	if !r.BindPlayer("friend", 3, 10001) {
		t.Fatal("bind player")
	}
	if got := r.NotifyFriendOpportunityChanged(10001, 10002, 7, 8); got != 1 {
		t.Fatalf("enqueued=%d", got)
	}
	item, ok := writer.Dequeue()
	if !ok {
		t.Fatal("missing opportunity hint")
	}
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(item.Data, &frame); err != nil {
		t.Fatal(err)
	}
	push := frame.GetServerPush()
	if push.GetPlayer().GetTargetPlayerId() != 10001 || push.PushType != commonpb.PushType_FRIEND_OPPORTUNITY_CHANGED {
		t.Fatalf("push=%v", push)
	}
	var hint friendpb.FriendOpportunityChanged
	if err := proto.Unmarshal(push.Payload, &hint); err != nil {
		t.Fatal(err)
	}
	if hint.TargetOwnerId != 10002 || hint.SourceFarmVersion != 7 || hint.OpportunityRevision != 8 {
		t.Fatalf("hint=%v", &hint)
	}
}

func TestSessionReplacementTargetsOnlyOlderPlayerConnections(t *testing.T) {
	r := NewConnectionRegistry()
	oldWriter := NewConnectionWriter(3, 4, 1<<20)
	newWriter := NewConnectionWriter(4, 4, 1<<20)
	otherWriter := NewConnectionWriter(5, 4, 1<<20)
	r.Bind("old", "farm-old", 3, oldWriter)
	r.Bind("new", "farm-new", 4, newWriter)
	r.Bind("other", "farm-other", 5, otherWriter)
	if !r.BindPlayer("old", 3, 10001) || !r.BindPlayer("new", 4, 10001) || !r.BindPlayer("other", 5, 10002) {
		t.Fatal("failed to bind authenticated players")
	}
	r.SetNextSequence("old", 3, 11)

	if got := r.NotifySessionReplaced(10001, "new"); got != 1 {
		t.Fatalf("enqueued=%d, want 1", got)
	}
	if _, ok := newWriter.Dequeue(); ok {
		t.Fatal("replacement notification reached the new connection")
	}
	if _, ok := otherWriter.Dequeue(); ok {
		t.Fatal("replacement notification leaked to another player")
	}
	item, ok := oldWriter.Dequeue()
	if !ok {
		t.Fatal("missing replacement notification")
	}
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(item.Data, &frame); err != nil {
		t.Fatal(err)
	}
	if frame.GetSequence() != 11 || frame.GetServerPush() == nil {
		t.Fatalf("frame=%v", &frame)
	}
	push := frame.GetServerPush()
	if push == nil {
		t.Fatal("missing server push in frame")
	}
	if push.PushType != commonpb.PushType_KICK {
		t.Fatalf("push type=%s", push.PushType)
	}
	var kick commonpb.ConnectionError
	if err := proto.Unmarshal(push.Payload, &kick); err != nil {
		t.Fatal(err)
	}
	if kick.ErrorCode != commonpb.ErrorCode_ERR_SESSION_EXPIRED || kick.ErrorMessage == "" {
		t.Fatalf("kick=%v", &kick)
	}
}

func TestConnectionRegistryMapsQueueStatuses(t *testing.T) {
	r := NewConnectionRegistry()
	w := NewConnectionWriter(3, 1, 1024)
	r.Bind("c", "sub", 3, w)
	frame := sessionPushFrame(t)
	resp, err := r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{ItemId: "a", SubscriptionId: "sub", ConnectionId: "c", TransportGeneration: 3, PresenceEpoch: 1, Frame: frame}}})
	if err != nil || resp.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("resp=%v err=%v", resp, err)
	}
	resp, _ = r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{ItemId: "b", SubscriptionId: "old", ConnectionId: "c", TransportGeneration: 3, PresenceEpoch: 1, Frame: frame}}})
	if resp.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_FULL {
		t.Fatalf("status=%v", resp.Results[0].Status)
	}
}

func TestConnectionRegistryReportsQueueFullWithoutDroppingWriter(t *testing.T) {
	r := NewConnectionRegistry()
	w := NewConnectionWriter(5, 1, 1024)
	r.BindWithPresence("slow", "join-slow", 5, 9, w)
	frame := sessionPushFrame(t)
	first, err := r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{
		ItemId: "snap", SubscriptionId: "join-slow", ConnectionId: "slow",
		TransportGeneration: 5, PresenceEpoch: 9, Frame: frame,
	}}})
	if err != nil || first.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("first=%v err=%v", first, err)
	}
	second, err := r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{
		ItemId: "delta", SubscriptionId: "join-slow", ConnectionId: "slow",
		TransportGeneration: 5, PresenceEpoch: 9, Frame: frame,
	}}})
	if err != nil || second.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_FULL {
		t.Fatalf("queue-full status=%v err=%v", second, err)
	}
	if item, ok := w.Dequeue(); !ok || string(item.Data) == "" {
		t.Fatal("QUEUE_FULL dropped the already accepted writer frame")
	}
}

func TestConnectionRegistryDeduplicatesItemID(t *testing.T) {
	r := NewConnectionRegistry()
	w := NewConnectionWriter(3, 4, 1<<20)
	r.BindWithPresence("c", "sub", 3, 1, w)
	frame := sessionPushFrame(t)
	req := &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{ItemId: "stable-1", SubscriptionId: "sub", ConnectionId: "c", TransportGeneration: 3, PresenceEpoch: 1, Frame: frame}}}
	first, err := r.DeliverBatch(context.Background(), req)
	if err != nil || first.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("first delivery=%v err=%v", first, err)
	}
	second, err := r.DeliverBatch(context.Background(), req)
	if err != nil || second.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("duplicate delivery=%v err=%v", second, err)
	}
	if _, ok := w.Dequeue(); !ok {
		t.Fatal("expected one queued item")
	}
	if _, ok := w.Dequeue(); ok {
		t.Fatal("duplicate item_id must not enqueue a second frame")
	}
}

func TestConnectionRegistryRejectsDelayedOldGeneration(t *testing.T) {
	r := NewConnectionRegistry()
	oldWriter := NewConnectionWriter(1, 4, 1<<20)
	newWriter := NewConnectionWriter(2, 4, 1<<20)
	r.BindWithPresence("reused", "sub", 1, 9, oldWriter)
	r.BindWithPresence("reused", "sub", 2, 10, newWriter)
	frame := sessionPushFrame(t)
	resp, err := r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{ItemId: "old-item", SubscriptionId: "sub", ConnectionId: "reused", TransportGeneration: 1, PresenceEpoch: 9, Frame: frame}}})
	if err != nil || resp.Results[0].Status != logicrpcpb.DownlinkItemStatus_STALE_CONNECTION {
		t.Fatalf("old generation delivery=%v err=%v", resp, err)
	}
	if items, _ := newWriter.Len(); items != 0 {
		t.Fatalf("old generation reached new writer: %d items", items)
	}
}

func TestBeginSubscriptionDoesNotAllowUnfencedFarmPush(t *testing.T) {
	r := NewConnectionRegistry()
	w := NewConnectionWriter(7, 8, 1<<20)
	r.Bind("conn", "", 7, w)
	if !r.BeginSubscription("conn", 7, "join-new") {
		t.Fatal("begin subscription failed")
	}
	frame, err := proto.Marshal(gateprotocol.NewServerPushFrame(1, &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_FARM_SNAPSHOT,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{
			FarmSubscription: &commonpb.FarmSubscriptionPushContext{SubscriptionId: "join-new"},
		},
		Payload: []byte("push"),
	}))
	if err != nil {
		t.Fatal(err)
	}
	deliver := func(item *logicrpcpb.DownlinkItem) logicrpcpb.DownlinkItemStatus {
		t.Helper()
		response, err := r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{item}})
		if err != nil {
			t.Fatal(err)
		}
		return response.Results[0].Status
	}
	if got := deliver(&logicrpcpb.DownlinkItem{ItemId: "old", SubscriptionId: "join-old", ConnectionId: "conn", TransportGeneration: 7, PresenceEpoch: 1, Frame: frame}); got != logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION {
		t.Fatalf("old status=%s", got)
	}
	if got := deliver(&logicrpcpb.DownlinkItem{ItemId: "new", SubscriptionId: "join-new", ConnectionId: "conn", TransportGeneration: 7, PresenceEpoch: 2, Frame: frame}); got != logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION {
		t.Fatalf("new status=%s", got)
	}
}

func TestConnectionRegistryServesGatewayDownlinkGRPC(t *testing.T) {
	r := NewConnectionRegistry()
	w := NewConnectionWriter(7, 4, 1<<20)
	r.Bind("c", "", 7, w)
	if !r.BeginSubscription("c", 7, "sub") {
		t.Fatal("begin subscription failed")
	}
	server := grpc.NewServer()
	logicrpcpb.RegisterGatewayDownlinkServiceServer(server, r)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = server.Serve(lis) }()
	defer server.Stop()
	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	frame := sessionPushFrame(t)
	resp, err := logicrpcpb.NewGatewayDownlinkServiceClient(conn).DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{ItemId: "i", SubscriptionId: "sub", ConnectionId: "c", TransportGeneration: 7, PresenceEpoch: 9, Frame: frame}}})
	if err != nil || len(resp.Results) != 1 || resp.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("resp=%v err=%v", resp, err)
	}
	item, ok := w.Dequeue()
	if !ok {
		t.Fatal("expected queued frame")
	}
	var got commonpb.FarmFrame
	if err := proto.Unmarshal(item.Data, &got); err != nil {
		t.Fatal(err)
	}
	if got.GetSequence() != 1 {
		t.Fatalf("first downlink seq=%d", got.GetSequence())
	}
}

func typedPushFrame(t *testing.T, pushType commonpb.PushType, subscriptionID string) []byte {
	t.Helper()
	frame, err := proto.Marshal(gateprotocol.NewServerPushFrame(1, &commonpb.ServerPushEnvelope{
		PushType: pushType,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{
			FarmSubscription: &commonpb.FarmSubscriptionPushContext{SubscriptionId: subscriptionID},
		},
		Payload: []byte("payload"),
	}))
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func sessionPushFrame(t *testing.T) []byte {
	t.Helper()
	frame, err := proto.Marshal(gateprotocol.NewServerPushFrame(1, &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_SYSTEM_NOTICE,
		PushContext: &commonpb.ServerPushEnvelope_Session{
			Session: &commonpb.SessionPushContext{TargetPlayerId: 1},
		},
		Payload: []byte("payload"),
	}))
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func friendOpportunityPushFrame(t *testing.T, targetPlayerID uint64) []byte {
	t.Helper()
	frame, err := proto.Marshal(gateprotocol.NewServerPushFrame(1, &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_FRIEND_OPPORTUNITY_CHANGED,
		PushContext: &commonpb.ServerPushEnvelope_Player{
			Player: &commonpb.PlayerPushContext{TargetPlayerId: targetPlayerID},
		},
		Payload: []byte("payload"),
	}))
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func TestPendingJoinRejectsLegacyUnfencedSnapshot(t *testing.T) {
	r := NewConnectionRegistry()
	w := NewConnectionWriter(7, 8, 1<<20)
	r.BindWithPresence("conn", "sub-old", 7, 1, w)
	if !r.PrepareJoinAttempt("conn", 7, "join-op", 202, "attempt-new") {
		t.Fatal("prepare join failed")
	}
	response, err := r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{
		ItemId: "snapshot-legacy", SubscriptionId: "sub-new", ConnectionId: "conn",
		TransportGeneration: 7, PresenceEpoch: 2, JoinOperationId: "join-op", TargetFarmId: 202,
		Frame: typedPushFrame(t, commonpb.PushType_FARM_SNAPSHOT, "sub-new"),
	}}})
	if err != nil || response.Results[0].Status != logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION {
		t.Fatalf("legacy snapshot=%v err=%v", response, err)
	}
	if _, ok := w.Dequeue(); ok {
		t.Fatal("legacy unfenced snapshot reached writer")
	}
}

// TestPushScopeRejectsFarmChangeWrongSubscription covers Slice C4.1: connection
// subscribed to A must not enqueue FARM_CHANGE for subscription B.
func TestPushScopeRejectsFarmChangeWrongSubscription(t *testing.T) {
	r := NewConnectionRegistry()
	w := NewConnectionWriter(3, 8, 1<<20)
	r.BindWithPresence("conn-a", "sub-A", 3, 1, w)
	frame := typedPushFrame(t, commonpb.PushType_FARM_CHANGE, "sub-B")
	resp, err := r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{
		ItemId: "farm-change-b", SubscriptionId: "sub-B", ConnectionId: "conn-a",
		TransportGeneration: 3, PresenceEpoch: 1, Frame: frame,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Results[0].Status != logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION {
		t.Fatalf("status=%s, want STALE_SUBSCRIPTION", resp.Results[0].Status)
	}
	if items, _ := w.Len(); items != 0 {
		t.Fatalf("wrong-subscription FARM_CHANGE wrote %d items to connection writer", items)
	}
}

// TestPushScopeAllowsSessionPlayerPushesDespiteSubscriptionMismatch covers C4.2:
// KICK and FRIEND_ROSTER_CHANGED are SESSION/PLAYER scope and must still deliver
// when item.SubscriptionId != connection currentSubscription.
func TestPushScopeAllowsSessionPlayerPushesDespiteSubscriptionMismatch(t *testing.T) {
	r := NewConnectionRegistry()
	w := NewConnectionWriter(3, 8, 1<<20)
	r.BindWithPresence("conn-sess", "sub-A", 3, 1, w)
	r.BindPlayer("conn-sess", 3, 10001)

	kickFrame := typedPushFrame(t, commonpb.PushType_KICK, "sub-B")
	kickResp, err := r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{
		ItemId: "kick-mismatch", SubscriptionId: "sub-B", ConnectionId: "conn-sess",
		TransportGeneration: 3, PresenceEpoch: 1, Frame: kickFrame,
	}}})
	if err != nil || kickResp.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("KICK status=%v err=%v, want QUEUE_ACCEPTED", kickResp, err)
	}
	if items, _ := w.Len(); items != 1 {
		t.Fatalf("KICK writer items=%d, want 1", items)
	}

	rosterFrame := typedPushFrame(t, commonpb.PushType_FRIEND_ROSTER_CHANGED, "sub-B")
	rosterResp, err := r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{
		ItemId: "roster-mismatch", SubscriptionId: "sub-B", ConnectionId: "conn-sess",
		TransportGeneration: 3, PresenceEpoch: 1, Frame: rosterFrame,
	}}})
	if err != nil || rosterResp.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("FRIEND_ROSTER status=%v err=%v, want QUEUE_ACCEPTED", rosterResp, err)
	}
	if items, _ := w.Len(); items != 2 {
		t.Fatalf("after roster writer items=%d, want 2", items)
	}

	// Local NotifyFriendRosterChanged path must also remain unfenced by farm sub.
	if got := r.NotifyFriendRosterChanged(10001, 10002, 1, friendpb.FriendMutationKind_ACCEPT_SHARE); got != 1 {
		t.Fatalf("NotifyFriendRosterChanged enqueued=%d, want 1", got)
	}
	if items, _ := w.Len(); items != 3 {
		t.Fatalf("NotifyFriendRosterChanged writer items=%d, want 3", items)
	}
}

func TestPlayerPushWithSessionTargetFenceDoesNotReuseFarmPresenceEpoch(t *testing.T) {
	r := NewConnectionRegistry()
	w := NewConnectionWriter(3, 8, 1<<20)
	r.BindWithPresence("conn-target", "farm-sub", 3, 99, w)
	r.BindPlayer("conn-target", 3, 10001)
	target := &logicrpcpb.DownlinkTargetFence{
		TargetPlayerId: 10001, GatewayId: "gate-a", GatewayBootId: "boot-a",
		ConnectionId: "conn-target", ConnectionGeneration: 3,
		SessionEpoch: 7, SessionRouteRevision: 11,
	}
	if !r.BindSessionTarget(target) {
		t.Fatal("bind session target failed")
	}
	frame := friendOpportunityPushFrame(t, 10001)
	response, err := r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{
		ItemId: "opportunity-target", ConnectionId: "conn-target", GatewayId: "gate-a",
		TransportGeneration: 3, TargetFence: proto.Clone(target).(*logicrpcpb.DownlinkTargetFence), Frame: frame,
	}}})
	if err != nil || response.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("target-fenced player push response=%v err=%v", response, err)
	}
	if items, _ := w.Len(); items != 1 {
		t.Fatalf("target-fenced player push wrote %d items, want 1", items)
	}

	staleCases := []struct {
		name   string
		mutate func(*logicrpcpb.DownlinkTargetFence)
	}{
		{
			name: "wrong Gate",
			mutate: func(value *logicrpcpb.DownlinkTargetFence) {
				value.GatewayId = "gate-b"
			},
		},
		{
			name: "wrong Gate Boot",
			mutate: func(value *logicrpcpb.DownlinkTargetFence) {
				value.GatewayBootId = "boot-b"
			},
		},
		{
			name: "wrong Session Epoch",
			mutate: func(value *logicrpcpb.DownlinkTargetFence) {
				value.SessionEpoch++
			},
		},
		{
			name: "wrong Route Revision",
			mutate: func(value *logicrpcpb.DownlinkTargetFence) {
				value.SessionRouteRevision++
			},
		},
	}
	for _, testCase := range staleCases {
		t.Run(testCase.name, func(t *testing.T) {
			stale := proto.Clone(target).(*logicrpcpb.DownlinkTargetFence)
			testCase.mutate(stale)
			response, err := r.DeliverBatch(
				context.Background(),
				&logicrpcpb.DeliverBatchRequest{
					Items: []*logicrpcpb.DownlinkItem{{
						ItemId:              "opportunity-stale-" + testCase.name,
						ConnectionId:        "conn-target",
						GatewayId:           stale.GatewayId,
						TransportGeneration: 3,
						TargetFence:         stale,
						Frame:               frame,
					}},
				},
			)
			if err != nil ||
				response.Results[0].Status !=
					logicrpcpb.DownlinkItemStatus_STALE_CONNECTION {
				t.Fatalf(
					"stale target-fenced player push response=%v err=%v",
					response,
					err,
				)
			}
		})
	}
	if items, _ := w.Len(); items != 1 {
		t.Fatalf("stale fence reached writer: items=%d", items)
	}

	response, err = r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{
		ItemId: "opportunity-unfenced", ConnectionId: "conn-target", GatewayId: "gate-a",
		TransportGeneration: 3, PresenceEpoch: 99, Frame: frame,
	}}})
	if err != nil || response.Results[0].Status != logicrpcpb.DownlinkItemStatus_STALE_CONNECTION {
		t.Fatalf("unfenced opportunity push response=%v err=%v", response, err)
	}

	wrongContext := typedPushFrame(t, commonpb.PushType_FRIEND_OPPORTUNITY_CHANGED, "farm-sub")
	response, err = r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{
		ItemId: "opportunity-wrong-context", ConnectionId: "conn-target", GatewayId: "gate-a",
		TransportGeneration: 3, TargetFence: proto.Clone(target).(*logicrpcpb.DownlinkTargetFence), Frame: wrongContext,
	}}})
	if err != nil || response.Results[0].Status != logicrpcpb.DownlinkItemStatus_STALE_CONNECTION {
		t.Fatalf("wrong-context opportunity push response=%v err=%v", response, err)
	}

	if !r.RevokeSessionTarget(target) {
		t.Fatal("revoke session target failed")
	}
	response, err = r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{
		ItemId: "opportunity-after-revoke", ConnectionId: "conn-target", GatewayId: "gate-a",
		TransportGeneration: 3, TargetFence: proto.Clone(target).(*logicrpcpb.DownlinkTargetFence), Frame: frame,
	}}})
	if err != nil || response.Results[0].Status != logicrpcpb.DownlinkItemStatus_STALE_CONNECTION {
		t.Fatalf("revoked opportunity push response=%v err=%v", response, err)
	}
	if got := r.NotifyFriendOpportunityChanged(10001, 20002, 9, 12); got != 0 {
		t.Fatalf("revoked local hint enqueued=%d, want 0", got)
	}
}

func TestNotifySessionFenceRevokesLaterTargetedPushes(t *testing.T) {
	r := NewConnectionRegistry()
	w := NewConnectionWriter(4, 8, 1<<20)
	r.Bind("kick-target", "", 4, w)
	r.BindPlayer("kick-target", 4, 7001)
	target := &logicrpcpb.DownlinkTargetFence{
		TargetPlayerId: 7001, GatewayId: "gate-k", GatewayBootId: "boot-k",
		ConnectionId: "kick-target", ConnectionGeneration: 4,
		SessionEpoch: 8, SessionRouteRevision: 9,
	}
	if !r.BindSessionTarget(target) {
		t.Fatal("bind target")
	}
	if !r.NotifySessionFence(7001, "kick-target", 4) {
		t.Fatal("expected terminal KICK to enqueue")
	}
	response, err := r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{
		ItemId: "late-opportunity", ConnectionId: "kick-target", GatewayId: "gate-k",
		TransportGeneration: 4, TargetFence: proto.Clone(target).(*logicrpcpb.DownlinkTargetFence),
		Frame: friendOpportunityPushFrame(t, 7001),
	}}})
	if err != nil || response.Results[0].Status != logicrpcpb.DownlinkItemStatus_STALE_CONNECTION {
		t.Fatalf("late targeted push response=%v err=%v", response, err)
	}
	if items, _ := w.Len(); items != 1 {
		t.Fatalf("writer contains %d items, want only KICK", items)
	}
}

func TestDerivePushScopeTable(t *testing.T) {
	cases := []struct {
		pushType commonpb.PushType
		want     PushScope
	}{
		{commonpb.PushType_FARM_SNAPSHOT, PushScopeFarmSubscription},
		{commonpb.PushType_FARM_CHANGE, PushScopeFarmSubscription},
		{commonpb.PushType_FARM_DELTA_BATCH, PushScopeFarmSubscription},
		{commonpb.PushType_SUBSCRIPTION_READY, PushScopeFarmSubscription},
		{commonpb.PushType_REJOIN_REQUIRED, PushScopeFarmSubscription},
		{commonpb.PushType_KICK, PushScopeSessionOrPlayer},
		{commonpb.PushType_DRAIN_NOTICE, PushScopeSessionOrPlayer},
		{commonpb.PushType_FRIEND_ROSTER_CHANGED, PushScopeSessionOrPlayer},
		{commonpb.PushType_FRIEND_OPPORTUNITY_CHANGED, PushScopeSessionOrPlayer},
		{commonpb.PushType_SYSTEM_NOTICE, PushScopeSessionOrPlayer},
		{commonpb.PushType_OPERATION_RESULT, PushScopeSessionOrPlayer},
		{commonpb.PushType_PUSH_TYPE_UNSPECIFIED, PushScopeFarmSubscription},
	}
	for _, tc := range cases {
		if got := DerivePushScope(tc.pushType); got != tc.want {
			t.Fatalf("push_type=%s scope=%v, want %v", tc.pushType, got, tc.want)
		}
	}
}

func TestNotifyFriendOpportunityHintSoftQuotaAndSequence(t *testing.T) {
	r := NewConnectionRegistry()
	w := NewConnectionWriterWithCapacity(3, WriterCapacity{
		MaxItems: 32, MaxBytes: 1 << 20,
		Hint: HintPolicy{MaxHintItems: 2, MaxHintBytes: 4096},
	})
	r.Bind("hint-quota", "farm-a", 3, w)
	if !r.BindPlayer("hint-quota", 3, 10001) {
		t.Fatal("bind player")
	}
	r.SetNextSequence("hint-quota", 3, 9)
	if got := r.NotifyFriendOpportunityChanged(10001, 10002, 7, 8); got != 1 {
		t.Fatalf("first hint enqueued=%d, want 1", got)
	}
	if got := r.NotifyFriendOpportunityChanged(10001, 10002, 8, 9); got != 1 {
		t.Fatalf("second hint enqueued=%d, want 1", got)
	}
	if got := r.NotifyFriendOpportunityChanged(10001, 10002, 9, 10); got != 0 {
		t.Fatalf("third hint enqueued=%d, want 0 (soft quota)", got)
	}
	stats := w.Stats()
	if got := stats.RejectedKind(FrameKindHint, "reject", "quota"); got != 1 {
		t.Fatalf("hint quota rejects=%d, want 1", got)
	}
	// Hint rejection consumed no sequence: the next accepted frame continues
	// at the next sequence after the two accepted hints.
	if got := r.NotifyFriendRosterChanged(10001, 10002, 1, friendpb.FriendMutationKind_ACCEPT_SHARE); got != 1 {
		t.Fatalf("roster enqueued=%d, want 1", got)
	}
	first, _ := w.Dequeue()
	var f1 commonpb.FarmFrame
	if err := proto.Unmarshal(first.Data, &f1); err != nil {
		t.Fatal(err)
	}
	if f1.GetSequence() != 9 {
		t.Fatalf("first sequence=%d, want 9", f1.GetSequence())
	}
	second, _ := w.Dequeue()
	var f2 commonpb.FarmFrame
	if err := proto.Unmarshal(second.Data, &f2); err != nil {
		t.Fatal(err)
	}
	if f2.GetSequence() != 10 {
		t.Fatalf("second sequence=%d, want 10", f2.GetSequence())
	}
	third, _ := w.Dequeue()
	var f3 commonpb.FarmFrame
	if err := proto.Unmarshal(third.Data, &f3); err != nil {
		t.Fatal(err)
	}
	if f3.GetSequence() != 11 || f3.GetServerPush().GetPushType() != commonpb.PushType_FRIEND_ROSTER_CHANGED {
		t.Fatalf("third frame seq=%d push=%v", f3.GetSequence(), f3.GetServerPush())
	}
}

func TestHintQuotaKeepsFarmChangeAndCommandResultCapacity(t *testing.T) {
	r := NewConnectionRegistry()
	w := NewConnectionWriterWithCapacity(4, WriterCapacity{
		MaxItems: 64, MaxBytes: 1 << 20,
		Hint: HintPolicy{MaxHintItems: 1, MaxHintBytes: 1024},
	})
	r.BindWithPresence("hint-fair", "farm-a", 4, 19, w)
	if !r.BindPlayer("hint-fair", 4, 10001) {
		t.Fatal("bind player")
	}
	r.SetNextSequence("hint-fair", 4, 5)
	if got := r.NotifyFriendOpportunityChanged(10001, 10002, 1, 1); got != 1 {
		t.Fatalf("first hint enqueued=%d", got)
	}
	if got := r.NotifyFriendOpportunityChanged(10001, 10002, 2, 2); got != 0 {
		t.Fatalf("second hint enqueued=%d, want 0", got)
	}
	// FarmChange still enters the FIFO via the W2 fence path and keeps its own
	// exact delivery semantics (QUEUE_FULL → upstream REJOIN, never soft drop).
	// The fence walk below must pass through the full W2A barrier so the
	// FARM_CHANGE frame is classified by the shared helper, not by a generic
	// session frame.
	target := &logicrpcpb.DownlinkTargetFence{
		TargetPlayerId: 10001, GatewayId: "gate-a", GatewayBootId: "boot-a",
		ConnectionId: "hint-fair", ConnectionGeneration: 4,
		SessionEpoch: 7, SessionRouteRevision: 11,
	}
	if !r.BindSessionTarget(target) {
		t.Fatal("bind session target")
	}
	joinFence := &logicrpcpb.SubscriptionFence{FarmId: 202, SubscriptionId: "farm-a", JoinAttemptId: "attempt-new"}
	if !r.PrepareJoinAttempt("hint-fair", 4, "join-op", 202, "attempt-new") {
		t.Fatal("prepare join")
	}
	if !r.QueueJoinAccepted("hint-fair", 4, "join-op", 202, "attempt-new", "farm-a",
		&commonpb.CommandResultEnvelope{RequestId: "join-request"}) {
		t.Fatal("queue join accepted")
	}
	if !r.MarkSnapshotArmRequested("hint-fair", 4, "join-op", 202, "attempt-new", "farm-a") {
		t.Fatal("mark snapshot arm")
	}
	snapshotFrame := w2FarmPushFrame(t, commonpb.PushType_FARM_SNAPSHOT, target, joinFence,
		&farmpb.FarmSnapshotPush{JoinAttemptId: "attempt-new", SnapshotVersion: 3,
			Snapshot: &farmpb.FarmSnapshot{OwnerId: 202, FarmVersion: 3}})
	snapshotResp, err := r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{
		ItemId: "snapshot-1", SubscriptionId: "farm-a", ConnectionId: "hint-fair",
		GatewayId: "gate-a", TransportGeneration: 4, PresenceEpoch: 19, SnapshotVersion: 3,
		Frame: snapshotFrame, JoinFence: joinFence, TargetFence: proto.Clone(target).(*logicrpcpb.DownlinkTargetFence),
	}}})
	if err != nil || snapshotResp.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("snapshot after hint quota=%v err=%v", snapshotResp, err)
	}
	ready, err := r.ActivateSubscription(context.Background(), &logicrpcpb.ActivateSubscriptionRequest{
		Subscription: joinFence, Target: target, ReadyThroughVersion: 3,
	})
	if err != nil || ready.GetStatus() != logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED {
		t.Fatalf("activate subscription=%v err=%v", ready, err)
	}
	changeFrame := w2FarmPushFrame(t, commonpb.PushType_FARM_CHANGE, target, joinFence,
		&farmpb.FarmChangePush{Change: &farmpb.FarmChange{OwnerId: 202, FarmVersion: 4}})
	changeResp, err := r.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{{
		ItemId: "change-1", SubscriptionId: "farm-a", ConnectionId: "hint-fair",
		GatewayId: "gate-a", TransportGeneration: 4, PresenceEpoch: 19,
		Frame: changeFrame, JoinFence: joinFence, TargetFence: proto.Clone(target).(*logicrpcpb.DownlinkTargetFence),
	}}})
	if err != nil || changeResp.Results[0].Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("farm change after hint quota=%v err=%v", changeResp, err)
	}
	stats := w.Stats()
	if got := stats.EnqueuedItems[FrameKindHint]; got != 1 {
		t.Fatalf("hint enqueued=%v, want 1", got)
	}
	if got := stats.EnqueuedItems[FrameKindCommandResult]; got != 1 {
		t.Fatalf("command result enqueued=%v, want 1", got)
	}
	if got := stats.EnqueuedItems[FrameKindFarmChange]; got != 1 {
		t.Fatalf("farm change enqueued=%v, want 1", got)
	}
	// FARM_SNAPSHOT and SUBSCRIPTION_READY classify as system; the FARM_CHANGE
	// frame itself must never fall into that bucket.
	if got := stats.EnqueuedItems[FrameKindSystem]; got != 2 {
		t.Fatalf("system enqueued=%v, want 2 (snapshot+ready)", got)
	}
}

// w2FarmPushFrame builds a true FarmSubscription-scoped ServerPush frame for
// the W2A fence path. The push context must carry the exact target/join fence
// values that deliverW2JoinItemLocked validates.
func w2FarmPushFrame(t *testing.T, pushType commonpb.PushType, target *logicrpcpb.DownlinkTargetFence,
	fence *logicrpcpb.SubscriptionFence, payload proto.Message) []byte {
	t.Helper()
	payloadBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := proto.Marshal(gateprotocol.NewServerPushFrame(1, &commonpb.ServerPushEnvelope{
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
	return frame
}
