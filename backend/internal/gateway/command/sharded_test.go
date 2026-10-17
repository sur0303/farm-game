package command

import (
	"context"
	"errors"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"
	"farmgame/internal/gateway/auth"
	"farmgame/internal/gateway/commandcatalog"
	"farmgame/internal/gateway/protocol"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type recordingLogicClient struct {
	calls              int
	route              actorrouting.RoutedContext
	renewCalls         int
	renewRequests      []*logicrpcpb.BatchRenewSubscriptionsRequest
	renewRoutes        []actorrouting.RoutedContext
	renewStatus        logicrpcpb.SubscriptionLeaseStatus
	malformedRenewItem bool
	request            *logicrpcpb.ExecuteCommandRequest
}

func outgoingRouteForTest(ctx context.Context) actorrouting.RoutedContext {
	md, _ := metadata.FromOutgoingContext(ctx)
	routed, _ := actorrouting.FromIncomingContext(metadata.NewIncomingContext(context.Background(), md))
	return routed
}

func (c *recordingLogicClient) ExecuteCommand(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	c.calls++
	c.request = proto.Clone(req).(*logicrpcpb.ExecuteCommandRequest)
	// Convert outgoing metadata to incoming metadata as a gRPC transport would.
	c.route = outgoingRouteForTest(ctx)
	return &logicrpcpb.ExecuteCommandResponse{Outcome: &logicrpcpb.ExecuteCommandResponse_Business{
		Business: &logicrpcpb.BusinessCommandOutcome{Status: commonpb.CommandStatus_SUCCEEDED},
	}}, nil
}

func (c *recordingLogicClient) CleanupSubscription(ctx context.Context, _ *logicrpcpb.CleanupSubscriptionRequest) (*logicrpcpb.CleanupSubscriptionResponse, error) {
	c.calls++
	c.route = outgoingRouteForTest(ctx)
	return &logicrpcpb.CleanupSubscriptionResponse{Removed: true}, nil
}

func (c *recordingLogicClient) BatchRenewSubscriptions(ctx context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
	c.renewCalls++
	c.renewRequests = append(c.renewRequests, req)
	c.renewRoutes = append(c.renewRoutes, outgoingRouteForTest(ctx))
	status := c.renewStatus
	if status == logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_UNSPECIFIED {
		status = logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED
	}
	response := &logicrpcpb.BatchRenewSubscriptionsResponse{Results: make([]*logicrpcpb.SubscriptionLeaseResult, len(req.Entries))}
	for i, entry := range req.Entries {
		response.Results[i] = &logicrpcpb.SubscriptionLeaseResult{
			TargetFarmId:   entry.GetTargetFarmId(),
			SubscriptionId: entry.GetSubscriptionId(),
			JoinAttemptId:  entry.GetJoinAttemptId(),
			Status:         status,
			AcceptedTtlMs:  30_000,
		}
	}
	if c.malformedRenewItem && len(response.Results) != 0 {
		response.Results[0].SubscriptionId = "wrong-subscription"
	}
	return response, nil
}

func TestShardedLogicClientRoutesByConflictActorKind(t *testing.T) {
	snapshot := actorrouting.RouteSnapshot{
		Realm: "classic-farm", GlobalRevision: 1,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608030001,
		Player: actorrouting.DomainSnapshot{
			ShardCount: 1,
			Routes: []actorrouting.ShardRoute{
				{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 2, ExpectedWriterEpoch: 3, ExpectedLeaseEpoch: 3, LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli()},
			},
		},
		Farm: actorrouting.DomainSnapshot{
			ShardCount: 1,
			Routes: []actorrouting.ShardRoute{
				{HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: 4, ExpectedWriterEpoch: 5, ExpectedLeaseEpoch: 5, LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli()},
			},
		},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "player-0", BootID: "player-0-boot", Endpoint: "player:9100", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-0", BootID: "farm-0-boot", Endpoint: "farm:9090", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
	resolver, err := actorrouting.NewStaticResolver(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	routes, _ := actorrouting.NewActorRouteClient(resolver, time.Second)
	player := &recordingLogicClient{}
	farm := &recordingLogicClient{}
	client, _ := NewShardedLogicClient("classic-farm", routes, map[string]LogicClient{
		"player:9100": player,
		"farm:9090":   farm,
	})
	request := func(command commonpb.CommandType) *logicrpcpb.ExecuteCommandRequest {
		// 复刻 Gate 的 Catalog 路由产出(29 §4):BUY → PLAYER:session.player_id;
		// PLANT → FARM:typed payload target_farm_id。
		dispatch := &logicrpcpb.DispatchContext{
			RouteMode:  logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY,
			ActorRoute: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: 10001},
		}
		if command == commonpb.CommandType_PLANT {
			dispatch.ActorRoute = &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: 20002}
		}
		return &logicrpcpb.ExecuteCommandRequest{
			Command:  &commonpb.ClientCommandEnvelope{CommandType: command},
			Trusted:  &logicrpcpb.TrustedCommandContext{Session: &logicrpcpb.SessionPermitSnapshot{PlayerId: 10001}},
			Dispatch: dispatch,
		}
	}
	if _, err := client.ExecuteCommand(context.Background(), request(commonpb.CommandType_BUY)); err != nil {
		t.Fatal(err)
	}
	if player.calls != 1 || player.route.Address.Kind != actorrouting.KindPlayer || player.route.Address.ID != 10001 || player.route.Assignment.WriterEpoch != 3 {
		t.Fatalf("player route=%+v calls=%d", player.route, player.calls)
	}
	if _, err := client.ExecuteCommand(context.Background(), request(commonpb.CommandType_PLANT)); err != nil {
		t.Fatal(err)
	}
	if farm.calls != 1 || farm.route.Address.Kind != actorrouting.KindFarm || farm.route.Address.ID != 20002 || farm.route.Assignment.WriterEpoch != 5 {
		t.Fatalf("farm route=%+v calls=%d", farm.route, farm.calls)
	}
	if _, err := client.CleanupSubscription(context.Background(), &logicrpcpb.CleanupSubscriptionRequest{Subscription: &logicrpcpb.SubscriptionFence{FarmId: 10001}}); err != nil {
		t.Fatal(err)
	}
	if farm.calls != 2 || farm.route.Address.Kind != actorrouting.KindFarm || farm.route.Address.ID != 10001 {
		t.Fatalf("cleanup farm route=%+v calls=%d", farm.route, farm.calls)
	}
}

func TestShardedLogicClientGroupsSubscriptionRenewalsByFarmHolderAndPreservesOrder(t *testing.T) {
	snapshot := actorrouting.RouteSnapshot{
		Realm: "classic-farm", GlobalRevision: 1,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608030001,
		Player: actorrouting.DomainSnapshot{
			ShardCount: 1,
			Routes:     []actorrouting.ShardRoute{{HolderIndex: 2, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli()}},
		},
		Farm: actorrouting.DomainSnapshot{
			ShardCount: 2,
			Routes: []actorrouting.ShardRoute{
				{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 2, ExpectedWriterEpoch: 2, ExpectedLeaseEpoch: 2, LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli()},
				{HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: 3, ExpectedWriterEpoch: 3, ExpectedLeaseEpoch: 3, LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli()},
			},
		},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "farm-a", BootID: "farm-a-boot", Endpoint: "farm-a:9090", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-b", BootID: "farm-b-boot", Endpoint: "farm-b:9090", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "player-a", BootID: "player-a-boot", Endpoint: "player-a:9100", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
	resolver, err := actorrouting.NewStaticResolver(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := actorrouting.NewActorRouteClient(resolver, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	idsByEndpoint := map[string][]uint64{}
	for id := uint64(1); id < 100 && (len(idsByEndpoint["farm-a:9090"]) < 2 || len(idsByEndpoint["farm-b:9090"]) < 1); id++ {
		assignment, resolveErr := routes.Resolve(context.Background(), actorrouting.Address{Realm: "classic-farm", Kind: actorrouting.KindFarm, ID: id})
		if resolveErr != nil {
			t.Fatal(resolveErr)
		}
		idsByEndpoint[assignment.Endpoint] = append(idsByEndpoint[assignment.Endpoint], id)
	}
	if len(idsByEndpoint["farm-a:9090"]) < 2 || len(idsByEndpoint["farm-b:9090"]) < 1 {
		t.Fatalf("failed to find farm ids for both holders: %+v", idsByEndpoint)
	}
	farmA := &recordingLogicClient{}
	farmB := &recordingLogicClient{renewStatus: logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_CLOSED}
	client, err := NewShardedLogicClient("classic-farm", routes, map[string]LogicClient{
		"farm-a:9090": farmA,
		"farm-b:9090": farmB,
	})
	if err != nil {
		t.Fatal(err)
	}
	entries := []*logicrpcpb.SubscriptionLeaseEntry{
		{TargetFarmId: idsByEndpoint["farm-a:9090"][0], SubscriptionId: "sub-a1", JoinAttemptId: "attempt-a1"},
		{TargetFarmId: idsByEndpoint["farm-b:9090"][0], SubscriptionId: "sub-b1", JoinAttemptId: "attempt-b1"},
		{TargetFarmId: idsByEndpoint["farm-a:9090"][1], SubscriptionId: "sub-a2", JoinAttemptId: "attempt-a2"},
	}
	response, err := client.BatchRenewSubscriptions(context.Background(), &logicrpcpb.BatchRenewSubscriptionsRequest{
		SourceGatewayId: "gate-a", SourceGatewayBootId: "boot-a", Entries: entries,
	})
	if err != nil {
		t.Fatal(err)
	}
	if farmA.renewCalls != 1 || len(farmA.renewRequests[0].Entries) != 2 || farmB.renewCalls != 1 || len(farmB.renewRequests[0].Entries) != 1 {
		t.Fatalf("unexpected holder batches: farmA=%d/%d farmB=%d/%d", farmA.renewCalls, len(farmA.renewRequests[0].Entries), farmB.renewCalls, len(farmB.renewRequests[0].Entries))
	}
	for i, result := range response.Results {
		if result.GetTargetFarmId() != entries[i].GetTargetFarmId() || result.GetSubscriptionId() != entries[i].GetSubscriptionId() || result.GetJoinAttemptId() != entries[i].GetJoinAttemptId() {
			t.Fatalf("result[%d] lost input order or identity: %+v", i, result)
		}
	}
	if response.Results[0].GetStatus() != logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED ||
		response.Results[1].GetStatus() != logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_CLOSED ||
		response.Results[2].GetStatus() != logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED {
		t.Fatalf("unexpected per-entry statuses: %+v", response.Results)
	}
}

func TestShardedLogicClientRejectsMalformedHolderRenewResultPerEntry(t *testing.T) {
	snapshot := actorrouting.RouteSnapshot{
		Realm: "classic-farm", GlobalRevision: 1,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 1,
		Player:  actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli()}}},
		Farm:    actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli()}}},
		Holders: []actorrouting.HolderRef{{InstanceID: "farm-a", BootID: "farm-a-boot", Endpoint: "farm-a:9090", State: actorrouting.HolderStateActive, Weight: 1}, {InstanceID: "player-a", BootID: "player-a-boot", Endpoint: "player-a:9100", State: actorrouting.HolderStateActive, Weight: 1}},
	}
	resolver, err := actorrouting.NewStaticResolver(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := actorrouting.NewActorRouteClient(resolver, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	holder := &recordingLogicClient{malformedRenewItem: true}
	client, err := NewShardedLogicClient("classic-farm", routes, map[string]LogicClient{"farm-a:9090": holder})
	if err != nil {
		t.Fatal(err)
	}
	entry := &logicrpcpb.SubscriptionLeaseEntry{TargetFarmId: 7, SubscriptionId: "sub-7", JoinAttemptId: "attempt-7"}
	response, err := client.BatchRenewSubscriptions(context.Background(), &logicrpcpb.BatchRenewSubscriptionsRequest{Entries: []*logicrpcpb.SubscriptionLeaseEntry{entry}})
	if err != nil {
		t.Fatal(err)
	}
	result := response.Results[0]
	if result.GetStatus() != logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_UNAVAILABLE || result.GetSubscriptionId() != entry.GetSubscriptionId() {
		t.Fatalf("malformed holder response was trusted: %+v", result)
	}
}

// A pooled endpoint client holds one connection for every Holder and chooses
// between them by reading the route off the outgoing context. A renewal
// dispatched without that route never reaches a Holder at all: it fails inside
// the client, in microseconds, and every entry comes back UNAVAILABLE. Commands
// carry the route, so a deployment can serve joins perfectly while no
// subscription lease is ever renewed.
func TestSubscriptionLeaseDispatchCarriesTheRouteAPooledClientNeeds(t *testing.T) {
	snapshot := actorrouting.RouteSnapshot{
		Realm: "classic-farm", GlobalRevision: 1,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 1,
		Player:  actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli()}}},
		Farm:    actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli()}}},
		Holders: []actorrouting.HolderRef{{InstanceID: "farm-a", BootID: "farm-a-boot", Endpoint: "farm-a:9090", State: actorrouting.HolderStateActive, Weight: 1}, {InstanceID: "player-a", BootID: "player-a-boot", Endpoint: "player-a:9100", State: actorrouting.HolderStateActive, Weight: 1}},
	}
	resolver, err := actorrouting.NewStaticResolver(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := actorrouting.NewActorRouteClient(resolver, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	holder := &recordingLogicClient{}
	client, err := NewShardedLogicClient("classic-farm", routes, map[string]LogicClient{"farm-a:9090": holder})
	if err != nil {
		t.Fatal(err)
	}
	entry := &logicrpcpb.SubscriptionLeaseEntry{TargetFarmId: 7, SubscriptionId: "sub-7", JoinAttemptId: "attempt-7"}
	request := &logicrpcpb.BatchRenewSubscriptionsRequest{
		SourceGatewayId: "gate-a", SourceGatewayBootId: "boot-a",
		Entries: []*logicrpcpb.SubscriptionLeaseEntry{entry},
	}
	partition, err := client.SubscriptionLeasePartition(context.Background(), entry.TargetFarmId)
	if err != nil {
		t.Fatal(err)
	}
	holderPartition := subscriptionLeaseDispatchHolder(request.SourceGatewayId, request.SourceGatewayBootId, partition)

	for _, dispatch := range []struct {
		name string
		call func() (*logicrpcpb.BatchRenewSubscriptionsResponse, error)
	}{
		{"grouped", func() (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
			return client.BatchRenewSubscriptions(context.Background(), request)
		}},
		{"partitioned", func() (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
			return client.BatchRenewSubscriptionsPartition(context.Background(), holderPartition, request)
		}},
	} {
		t.Run(dispatch.name, func(t *testing.T) {
			holder.renewRoutes = nil
			if _, err := dispatch.call(); err != nil {
				t.Fatal(err)
			}
			if len(holder.renewRoutes) != 1 {
				t.Fatalf("holder saw %d renewals, want 1", len(holder.renewRoutes))
			}
			route := holder.renewRoutes[0]
			if route.Address.Realm != "classic-farm" || route.Address.Kind != actorrouting.KindFarm ||
				route.Address.ID != entry.TargetFarmId {
				t.Fatalf("renewal carried no usable address: %+v", route.Address)
			}
			if route.Assignment.HolderPod != "farm-a" || route.Assignment.ShardRevision != 1 {
				t.Fatalf("renewal carried no usable assignment: %+v", route.Assignment)
			}
		})
	}
}

type mutableLeaseResolver struct{ assignment actorrouting.Assignment }

func (r *mutableLeaseResolver) Resolve(context.Context, actorrouting.Address) (actorrouting.Assignment, error) {
	return r.assignment, nil
}

func TestShardedLogicClientFencesOldSubscriptionHolderAfterHandoff(t *testing.T) {
	resolver := &mutableLeaseResolver{assignment: actorrouting.Assignment{
		Kind: actorrouting.KindFarm, ShardID: 7, HolderPod: "holder-a", HolderBootID: "boot-a",
		Endpoint: "farm-a:9090", ShardRevision: 11, WriterEpoch: 12, LeaseEpoch: 13,
	}}
	routes, err := actorrouting.NewActorRouteClient(resolver, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	farmA := &recordingLogicClient{}
	farmB := &recordingLogicClient{}
	client, err := NewShardedLogicClient("classic-farm", routes, map[string]LogicClient{
		"farm-a:9090": farmA, "farm-b:9090": farmB,
	})
	if err != nil {
		t.Fatal(err)
	}
	entry := &logicrpcpb.SubscriptionLeaseEntry{
		TargetFarmId: 42, SubscriptionId: "subscription-42", JoinAttemptId: "attempt-42",
	}
	request := &logicrpcpb.BatchRenewSubscriptionsRequest{
		SourceGatewayId: "gate-a", SourceGatewayBootId: "gate-boot-a",
		Entries: []*logicrpcpb.SubscriptionLeaseEntry{entry},
	}
	oldPartition, err := client.SubscriptionLeasePartition(context.Background(), entry.TargetFarmId)
	if err != nil {
		t.Fatal(err)
	}
	resolver.assignment = actorrouting.Assignment{
		Kind: actorrouting.KindFarm, ShardID: 7, HolderPod: "holder-b", HolderBootID: "boot-b",
		Endpoint: "farm-b:9090", ShardRevision: 14, WriterEpoch: 15, LeaseEpoch: 16,
	}
	response, err := client.BatchRenewSubscriptionsPartition(context.Background(),
		subscriptionLeaseDispatchHolder("gate-a", "gate-boot-a", oldPartition), request)
	if err != nil {
		t.Fatal(err)
	}
	if got := response.GetResults()[0].GetStatus(); got != logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_ROUTE_STALE {
		t.Fatalf("old holder handoff status=%s", got)
	}
	if farmA.renewCalls != 0 || farmB.renewCalls != 0 {
		t.Fatalf("old partition crossed handoff: farmA=%d farmB=%d", farmA.renewCalls, farmB.renewCalls)
	}
	newPartition, err := client.SubscriptionLeasePartition(context.Background(), entry.TargetFarmId)
	if err != nil {
		t.Fatal(err)
	}
	response, err = client.BatchRenewSubscriptionsPartition(context.Background(),
		subscriptionLeaseDispatchHolder("gate-a", "gate-boot-a", newPartition), request)
	if err != nil {
		t.Fatal(err)
	}
	if got := response.GetResults()[0].GetStatus(); got != logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED || farmB.renewCalls != 1 {
		t.Fatalf("new holder was not independently renewable: status=%s calls=%d", got, farmB.renewCalls)
	}

	// Gate still accepts the conservative one-farm key so a static client, or a
	// candidate that failed Holder resolution, can follow a handoff without
	// mixing generations in one RPC.
	farmPartition := subscriptionLeaseDispatchHolder("gate-a", "gate-boot-a",
		subscriptionLeaseFarmPartition(entry.TargetFarmId))
	response, err = client.BatchRenewSubscriptionsPartition(context.Background(), farmPartition, request)
	if err != nil {
		t.Fatal(err)
	}
	if got := response.GetResults()[0].GetStatus(); got != logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED || farmB.renewCalls != 2 {
		t.Fatalf("farm-isolated dispatch did not resolve current holder: status=%s calls=%d", got, farmB.renewCalls)
	}
}

func TestShardedLogicClientRenewsFarmsThatShareAHolderInOneRPC(t *testing.T) {
	snapshot := actorrouting.RouteSnapshot{
		Realm: "classic-farm", GlobalRevision: 1,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 1,
		Player:  actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli()}}},
		Farm:    actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli()}}},
		Holders: []actorrouting.HolderRef{{InstanceID: "farm-a", BootID: "farm-a-boot", Endpoint: "farm-a:9090", State: actorrouting.HolderStateActive, Weight: 1}, {InstanceID: "player-a", BootID: "player-a-boot", Endpoint: "player-a:9100", State: actorrouting.HolderStateActive, Weight: 1}},
	}
	resolver, err := actorrouting.NewStaticResolver(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := actorrouting.NewActorRouteClient(resolver, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	holder := &recordingLogicClient{}
	client, err := NewShardedLogicClient("classic-farm", routes, map[string]LogicClient{"farm-a:9090": holder})
	if err != nil {
		t.Fatal(err)
	}
	request := &logicrpcpb.BatchRenewSubscriptionsRequest{
		SourceGatewayId: "gate-a", SourceGatewayBootId: "boot-a",
		Entries: []*logicrpcpb.SubscriptionLeaseEntry{
			{TargetFarmId: 7, SubscriptionId: "sub-7", JoinAttemptId: "attempt-7"},
			{TargetFarmId: 8, SubscriptionId: "sub-8", JoinAttemptId: "attempt-8"},
		},
	}
	partition, err := client.SubscriptionLeasePartition(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.BatchRenewSubscriptionsPartition(context.Background(),
		subscriptionLeaseDispatchHolder(request.SourceGatewayId, request.SourceGatewayBootId, partition), request)
	if err != nil {
		t.Fatal(err)
	}
	if holder.renewCalls != 1 || len(holder.renewRequests[0].Entries) != 2 {
		t.Fatalf("shared-holder farms were not one RPC: calls=%d entries=%v", holder.renewCalls, holder.renewRequests)
	}
	if got := response.GetResults()[0].GetStatus(); got != logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED {
		t.Fatalf("first farm status=%s", got)
	}
	if got := response.GetResults()[1].GetStatus(); got != logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED {
		t.Fatalf("second farm status=%s", got)
	}
}

type serviceLBCountingResolver struct {
	resolver actorrouting.Resolver
	calls    int
}

func (r *serviceLBCountingResolver) Resolve(ctx context.Context, address actorrouting.Address) (actorrouting.Assignment, error) {
	r.calls++
	return r.resolver.Resolve(ctx, address)
}

type serviceLBRecordingClient struct {
	calls    int
	request  *logicrpcpb.ExecuteCommandRequest
	route    actorrouting.RoutedContext
	deadline time.Time
	response *logicrpcpb.ExecuteCommandResponse
	err      error
}

func (c *serviceLBRecordingClient) ExecuteCommand(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	c.calls++
	c.request = proto.Clone(req).(*logicrpcpb.ExecuteCommandRequest)
	c.route = outgoingRouteForTest(ctx)
	c.deadline, _ = ctx.Deadline()
	if c.err != nil {
		return nil, c.err
	}
	if c.response != nil {
		return c.response, nil
	}
	return &logicrpcpb.ExecuteCommandResponse{Outcome: &logicrpcpb.ExecuteCommandResponse_Business{
		Business: &logicrpcpb.BusinessCommandOutcome{Status: commonpb.CommandStatus_SUCCEEDED},
	}}, nil
}

func newStaticAuthorityLogicClient(t *testing.T, serviceLB map[commandcatalog.FirstHopRole]LogicClient) (*ShardedLogicClient, *recordingLogicClient, *recordingLogicClient, *serviceLBCountingResolver) {
	t.Helper()
	snapshot := actorrouting.RouteSnapshot{
		Realm: "classic-farm", GlobalRevision: 11,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608050001,
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 12,
			ExpectedWriterEpoch: 13, ExpectedLeaseEpoch: 13, LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli(),
		}}},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: 14,
			ExpectedWriterEpoch: 15, ExpectedLeaseEpoch: 15, LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli(),
		}}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "player-holder", BootID: "player-boot", Endpoint: "player-holder:9100", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-holder", BootID: "farm-boot", Endpoint: "farm-holder:9090", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
	staticResolver, err := actorrouting.NewStaticResolver(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	resolver := &serviceLBCountingResolver{resolver: staticResolver}
	routes, err := actorrouting.NewActorRouteClient(resolver, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	playerHolder := &recordingLogicClient{}
	farmHolder := &recordingLogicClient{}
	client, err := NewShardedLogicClientWithServiceLB("classic-farm", routes, map[string]LogicClient{
		"player-holder:9100": playerHolder,
		"farm-holder:9090":   farmHolder,
	}, serviceLB)
	if err != nil {
		t.Fatal(err)
	}
	return client, playerHolder, farmHolder, resolver
}

func operationResultServiceLBRequest() *logicrpcpb.ExecuteCommandRequest {
	return &logicrpcpb.ExecuteCommandRequest{
		Command: &commonpb.ClientCommandEnvelope{
			RequestId: "lookup-request", OperationId: "lookup-operation",
			CommandType: commonpb.CommandType_GET_OPERATION_RESULT, Payload: []byte{1, 2, 3},
		},
		Trusted: &logicrpcpb.TrustedCommandContext{Session: &logicrpcpb.SessionPermitSnapshot{
			PlayerId: 101, ActiveSessionEpoch: 7, GatewayId: "gate-a", GatewayBootId: "gate-a-boot",
			ConnectionId: "connection-a", ConnectionGeneration: 3, SessionRouteRevision: 9,
		}},
		Dispatch: &logicrpcpb.DispatchContext{RouteMode: logicrpcpb.RouteMode_ROUTE_MODE_SERVICE_LB},
		TraceId:  "lookup-trace",
	}
}

func TestStaticAuthorityServiceLBRoutesOperationResultToDeclaredPlayerFirstHop(t *testing.T) {
	playerService := &serviceLBRecordingClient{}
	client, playerHolder, farmHolder, resolver := newStaticAuthorityLogicClient(t,
		map[commandcatalog.FirstHopRole]LogicClient{commandcatalog.FirstHopPlayerSvr: playerService})

	deadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	request := operationResultServiceLBRequest()
	if _, err := client.ExecuteCommand(ctx, request); err != nil {
		t.Fatal(err)
	}
	if playerService.calls != 1 || playerHolder.calls != 0 || farmHolder.calls != 0 || resolver.calls != 0 {
		t.Fatalf("ServiceLB calls=%d player_holder=%d farm_holder=%d resolver=%d",
			playerService.calls, playerHolder.calls, farmHolder.calls, resolver.calls)
	}
	forward := playerService.request
	if forward.GetDispatch().GetRouteMode() != logicrpcpb.RouteMode_ROUTE_MODE_SERVICE_LB || forward.GetDispatch().GetActorRoute() != nil {
		t.Fatalf("ServiceLB dispatch forged ActorRoute: %+v", forward.GetDispatch())
	}
	if !proto.Equal(forward.GetCommand(), request.GetCommand()) ||
		forward.GetTraceId() != request.GetTraceId() || !proto.Equal(forward.GetTrusted(), request.GetTrusted()) {
		t.Fatalf("ServiceLB forwarding changed trusted/request identity: %+v", forward)
	}
	if !playerService.deadline.Equal(deadline) {
		t.Fatalf("ServiceLB deadline=%s, want %s", playerService.deadline, deadline)
	}
	if playerService.route.Address.ID != 0 || playerService.route.Assignment.Endpoint != "" {
		t.Fatalf("ServiceLB carried Actor route metadata: %+v", playerService.route)
	}
}

func TestStaticAuthorityActorAffinityStillUsesShardHolder(t *testing.T) {
	playerService := &serviceLBRecordingClient{}
	client, playerHolder, farmHolder, resolver := newStaticAuthorityLogicClient(t,
		map[commandcatalog.FirstHopRole]LogicClient{commandcatalog.FirstHopPlayerSvr: playerService})
	request := &logicrpcpb.ExecuteCommandRequest{
		Command: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_BUY},
		Dispatch: &logicrpcpb.DispatchContext{
			RouteMode: logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY,
			ActorRoute: &logicrpcpb.ActorRouteHeader{
				RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: 101,
			},
		},
	}
	if _, err := client.ExecuteCommand(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if playerHolder.calls != 1 || farmHolder.calls != 0 || playerService.calls != 0 || resolver.calls != 1 {
		t.Fatalf("ActorAffinity player_holder=%d farm_holder=%d ServiceLB=%d resolver=%d",
			playerHolder.calls, farmHolder.calls, playerService.calls, resolver.calls)
	}
	route := playerHolder.request.GetDispatch().GetActorRoute()
	if route.GetVirtualShard() != 0 || route.GetExpectedHolderId() != "player-holder" ||
		route.GetExpectedHolderBootId() != "player-boot" || route.GetExpectedWriterEpoch() != 13 {
		t.Fatalf("ActorAffinity did not complete holder route: %+v", route)
	}
}

func TestStaticAuthorityServiceLBUnavailableMapsAtPublicBoundary(t *testing.T) {
	tests := []struct {
		name       string
		service    *serviceLBRecordingClient
		serviceMap func(*serviceLBRecordingClient) map[commandcatalog.FirstHopRole]LogicClient
	}{
		{name: "missing", serviceMap: func(*serviceLBRecordingClient) map[commandcatalog.FirstHopRole]LogicClient { return nil }},
		{name: "unavailable", service: &serviceLBRecordingClient{err: errors.New("player service unavailable")}, serviceMap: func(service *serviceLBRecordingClient) map[commandcatalog.FirstHopRole]LogicClient {
			return map[commandcatalog.FirstHopRole]LogicClient{commandcatalog.FirstHopPlayerSvr: service}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logic, playerHolder, farmHolder, resolver := newStaticAuthorityLogicClient(t, test.serviceMap(test.service))
			authService := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
			gate := NewGate(authService, logic, 64<<10)
			activateHello(t, gate, authService, "servicelb-"+test.name)
			payload, err := proto.Marshal(&publicpb.GetOperationResultRequest{
				OriginalCommandType: commonpb.CommandType_THEFT,
				OriginalOperationId: "theft-operation",
				OriginalTarget: &publicpb.OperationTargetLocator{Target: &publicpb.OperationTargetLocator_TargetFarmId{
					TargetFarmId: 202,
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			raw, err := gate.HandleBinary(context.Background(), "servicelb-"+test.name, 1,
				encodeTypedCommandFrame(t, 2, &commonpb.ClientCommandEnvelope{
					RequestId: "public-lookup-request", OperationId: "public-lookup-operation",
					CommandType: commonpb.CommandType_GET_OPERATION_RESULT, Payload: payload,
				}))
			if err != nil {
				t.Fatalf("public boundary closed connection: %v", err)
			}
			frame, err := protocol.Decode(raw, 64<<10)
			if err != nil {
				t.Fatal(err)
			}
			result := frame.GetCommandResult()
			if result.GetStatus() != commonpb.CommandStatus_UNAVAILABLE ||
				result.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_STORE_UNAVAILABLE ||
				result.GetRetryDirective() != commonpb.RetryDirective_RETRY_SAME_OPERATION {
				t.Fatalf("public unavailable mapping=%+v", result)
			}
			if result.GetRequestId() != "public-lookup-request" || result.GetOperationId() != "public-lookup-operation" {
				t.Fatalf("public unavailable lost request identity: %+v", result)
			}
			if playerHolder.calls != 0 || farmHolder.calls != 0 || resolver.calls != 0 {
				t.Fatalf("ServiceLB unavailable fell back to Actor holder: player=%d farm=%d resolver=%d",
					playerHolder.calls, farmHolder.calls, resolver.calls)
			}
			if test.service != nil && test.service.calls != 1 {
				t.Fatalf("configured unavailable ServiceLB calls=%d, want 1", test.service.calls)
			}
		})
	}
}

func TestStaticAuthorityServiceLBDoesNotEnterActorRedirectLoop(t *testing.T) {
	playerService := &serviceLBRecordingClient{response: &logicrpcpb.ExecuteCommandResponse{
		Outcome: &logicrpcpb.ExecuteCommandResponse_RouteRejection{RouteRejection: &logicrpcpb.RouteRejection{
			Reason: logicrpcpb.RouteRejectReason_ROUTE_REJECT_REDIRECT,
		}},
	}}
	client, playerHolder, farmHolder, resolver := newStaticAuthorityLogicClient(t,
		map[commandcatalog.FirstHopRole]LogicClient{commandcatalog.FirstHopPlayerSvr: playerService})
	response, err := client.ExecuteCommand(context.Background(), operationResultServiceLBRequest())
	if err != nil {
		t.Fatal(err)
	}
	if response.GetRouteRejection().GetReason() != logicrpcpb.RouteRejectReason_ROUTE_REJECT_REDIRECT {
		t.Fatalf("ServiceLB response=%+v", response)
	}
	if playerService.calls != 1 || playerHolder.calls != 0 || farmHolder.calls != 0 || resolver.calls != 0 {
		t.Fatalf("ServiceLB redirect looped through Actor routing: service=%d player=%d farm=%d resolver=%d",
			playerService.calls, playerHolder.calls, farmHolder.calls, resolver.calls)
	}
}
