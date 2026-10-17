package friend

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/gateway/session"
	logicdownlink "farmgame/internal/logic/downlink"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type directRouteResolver struct {
	routes map[uint64]session.ActiveRoute
}

type recordingOpportunityHintTransport struct {
	calls int
	err   error
}

func (t *recordingOpportunityHintTransport) PublishTargetedHint(
	context.Context,
	uint64,
	uint64,
	uint64,
	uint64,
) error {
	t.calls++
	return t.err
}

func (r directRouteResolver) BatchGetActiveRoutes(_ context.Context, playerIDs []uint64) (map[uint64]session.ActiveRoute, error) {
	result := make(map[uint64]session.ActiveRoute)
	for _, playerID := range playerIDs {
		if route, ok := r.routes[playerID]; ok {
			result[playerID] = route
		}
	}
	return result, nil
}

type directGateCapture struct {
	mu       sync.Mutex
	requests []*logicrpcpb.DeliverBatchRequest
	notify   chan struct{}
}

func (c *directGateCapture) DeliverBatch(_ context.Context, request *logicrpcpb.DeliverBatchRequest,
	_ ...grpc.CallOption) (*logicrpcpb.DeliverBatchResponse, error) {
	c.mu.Lock()
	c.requests = append(c.requests, proto.Clone(request).(*logicrpcpb.DeliverBatchRequest))
	c.mu.Unlock()
	select {
	case c.notify <- struct{}{}:
	default:
	}
	response := &logicrpcpb.DeliverBatchResponse{}
	for _, item := range request.Items {
		response.Results = append(response.Results, &logicrpcpb.DeliverBatchItemResult{
			ItemId: item.ItemId, Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
		})
	}
	return response, nil
}

func TestDirectOpportunityHintTransportRoutesOnlyActiveViewers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	index := NewMemoryFriendIndexRepository()
	for _, viewer := range []uint64{601, 602, 603} {
		if _, err := index.Apply(ctx, 501, FriendIndexEntry{
			FriendPlayerID: viewer, RelationID: pairIDMust(501, viewer), RelationEpoch: 1,
			ActivatedAtMs: int64(viewer),
		}); err != nil {
			t.Fatal(err)
		}
	}
	active := func(playerID uint64, gatewayID string, generation uint64) session.ActiveRoute {
		return session.ActiveRoute{
			RouteFence: session.RouteFence{GatewayID: gatewayID, GatewayBootID: "boot-" + gatewayID,
				ConnectionID: "connection-" + gatewayID, ConnectionGeneration: generation},
			PlayerID: playerID, SessionID: "session", SessionEpoch: 7, RouteRevision: 3,
			LeaseExpiresAt: time.Now().Add(time.Minute),
		}
	}
	routes := directRouteResolver{routes: map[uint64]session.ActiveRoute{
		601: active(601, "gate-a", 11),
		603: active(603, "gate-b", 13),
	}}
	gateA := &directGateCapture{notify: make(chan struct{}, 1)}
	gateB := &directGateCapture{notify: make(chan struct{}, 1)}
	routed, err := logicdownlink.NewRoutedGatewayClient(map[string]logicdownlink.GatewayClient{
		"gate-a": gateA, "gate-b": gateB,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	config := logicdownlink.DefaultDispatcherConfig()
	config.BatchSize = 16
	config.RPCTimeout = time.Second
	dispatcher := logicdownlink.NewDispatcherWithConfig(routed, config)
	defer dispatcher.Close()
	transport, err := NewDirectOpportunityHintTransport(index, routes, dispatcher)
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.PublishTargetedHint(ctx, 501, 501, 9, 12); err != nil {
		t.Fatal(err)
	}
	for _, notify := range []chan struct{}{gateA.notify, gateB.notify} {
		select {
		case <-notify:
		case <-ctx.Done():
			t.Fatal("direct Hint was not delivered before deadline")
		}
	}
	assertTarget := func(capture *directGateCapture, wantPlayer uint64, wantGateway string, wantGeneration uint64) {
		t.Helper()
		capture.mu.Lock()
		defer capture.mu.Unlock()
		if len(capture.requests) != 1 || len(capture.requests[0].Items) != 1 {
			t.Fatalf("gateway %s requests=%v", wantGateway, capture.requests)
		}
		item := capture.requests[0].Items[0]
		if item.GatewayId != wantGateway || item.TransportGeneration != wantGeneration || item.PresenceEpoch != 0 {
			t.Fatalf("gateway %s item=%+v", wantGateway, item)
		}
		fence := item.GetTargetFence()
		if fence.GetTargetPlayerId() != wantPlayer || fence.GetGatewayId() != wantGateway ||
			fence.GetGatewayBootId() != "boot-"+wantGateway || fence.GetConnectionId() != "connection-"+wantGateway ||
			fence.GetConnectionGeneration() != wantGeneration || fence.GetSessionEpoch() != 7 ||
			fence.GetSessionRouteRevision() != 3 {
			t.Fatalf("gateway %s target fence=%+v", wantGateway, fence)
		}
		var frame commonpb.FarmFrame
		if err := proto.Unmarshal(item.Frame, &frame); err != nil {
			t.Fatal(err)
		}
		push := frame.GetServerPush()
		if push == nil || push.GetPlayer().GetTargetPlayerId() != wantPlayer || push.GetPushType() != commonpb.PushType_FRIEND_OPPORTUNITY_CHANGED {
			t.Fatalf("gateway %s push=%+v", wantGateway, push)
		}
	}
	assertTarget(gateA, 601, "gate-a", 11)
	assertTarget(gateB, 603, "gate-b", 13)
	stats := transport.Stats()
	if stats.Targets != 3 || stats.Routes != 2 || stats.MissingRoutes != 1 || stats.Enqueued != 2 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestOpportunityHintTransportModeFailsClosed(t *testing.T) {
	if _, err := ParseOpportunityHintTransportMode(""); err == nil {
		t.Fatal("empty transport mode must not silently select a path")
	}
	if _, err := SelectOpportunityHintTransport("direct", nil, nil); err == nil {
		t.Fatal("direct mode without a direct transport must fail closed")
	}
	if _, err := SelectOpportunityHintTransport(
		"shadow-direct",
		nil,
		nil,
	); err == nil {
		t.Fatal("shadow-direct without primary and direct transports must fail closed")
	}
}

func TestShadowDirectOpportunityHintTransportPreservesPrimaryDelivery(t *testing.T) {
	ctx := context.Background()
	index := NewMemoryFriendIndexRepository()
	if _, err := index.Apply(ctx, 501, FriendIndexEntry{
		FriendPlayerID: 601,
		RelationID:     pairIDMust(501, 601),
		RelationEpoch:  1,
		ActivatedAtMs:  1,
	}); err != nil {
		t.Fatal(err)
	}
	routes := directRouteResolver{routes: map[uint64]session.ActiveRoute{
		601: {
			RouteFence: session.RouteFence{
				GatewayID:            "gate-a",
				GatewayBootID:        "gate-a-boot",
				ConnectionID:         "connection-a",
				ConnectionGeneration: 1,
			},
			PlayerID:       601,
			SessionID:      "session-a",
			SessionEpoch:   2,
			RouteRevision:  3,
			LeaseExpiresAt: time.Now().Add(time.Minute),
		},
	}}
	gate := &directGateCapture{notify: make(chan struct{}, 1)}
	dispatcher := logicdownlink.NewDispatcher(gate, 16)
	defer dispatcher.Close()
	direct, err := NewDirectOpportunityHintTransport(
		index,
		routes,
		dispatcher,
	)
	if err != nil {
		t.Fatal(err)
	}
	primary := &recordingOpportunityHintTransport{}
	selected, err := NewSwitchableOpportunityHintTransport(
		primary,
		direct,
		OpportunityHintTransportShadowDirect,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := selected.PublishTargetedHint(ctx, 501, 501, 9, 12); err != nil {
		t.Fatal(err)
	}
	if primary.calls != 1 {
		t.Fatalf("primary calls=%d", primary.calls)
	}
	stats := direct.Stats()
	if stats.Targets != 1 || stats.Routes != 1 ||
		stats.Suppressed != 1 || stats.Enqueued != 0 {
		t.Fatalf("shadow stats=%+v", stats)
	}
	gate.mu.Lock()
	if len(gate.requests) != 0 {
		gate.mu.Unlock()
		t.Fatalf("shadow-direct reached Gate: %+v", gate.requests)
	}
	gate.mu.Unlock()
	if err := selected.SetMode(OpportunityHintTransportDirect); err != nil {
		t.Fatal(err)
	}
	if err := selected.PublishTargetedHint(ctx, 501, 501, 10, 13); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gate.notify:
	case <-time.After(time.Second):
		t.Fatal("direct mode did not reach Gate")
	}
	if selected.Mode() != OpportunityHintTransportDirect {
		t.Fatalf("selected mode=%q", selected.Mode())
	}
	if primary.calls != 1 {
		t.Fatalf("direct mode called primary, calls=%d", primary.calls)
	}
	if err := selected.SetMode("invalid"); err == nil {
		t.Fatal("invalid dynamic mode was accepted")
	}
	if selected.Mode() != OpportunityHintTransportDirect {
		t.Fatalf("invalid update changed mode=%q", selected.Mode())
	}
	gate.mu.Lock()
	requestCount := len(gate.requests)
	gate.mu.Unlock()
	if requestCount != 1 {
		t.Fatalf("direct request count=%d", requestCount)
	}
}

func TestShadowDirectOpportunityHintTransportFailsWithPrimary(t *testing.T) {
	sentinel := errors.New("pubsub unavailable")
	primary := &recordingOpportunityHintTransport{err: sentinel}
	direct := &DirectOpportunityHintTransport{}
	transport, err := NewShadowDirectOpportunityHintTransport(
		primary,
		direct,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.PublishTargetedHint(
		context.Background(),
		1,
		1,
		1,
		1,
	); !errors.Is(err, sentinel) {
		t.Fatalf("primary error=%v", err)
	}
	if direct.Stats() != (DirectOpportunityHintStats{}) {
		t.Fatalf("shadow ran after primary failure: %+v", direct.Stats())
	}
}
