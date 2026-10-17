package command

import (
	"context"
	"testing"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
)

type routeCapture struct {
	calls      int
	renewCalls int
}

func (c *routeCapture) ExecuteCommand(context.Context, *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	c.calls++
	return &logicrpcpb.ExecuteCommandResponse{Outcome: &logicrpcpb.ExecuteCommandResponse_Business{
		Business: &logicrpcpb.BusinessCommandOutcome{},
	}}, nil
}

func (c *routeCapture) CleanupSubscription(context.Context, *logicrpcpb.CleanupSubscriptionRequest) (*logicrpcpb.CleanupSubscriptionResponse, error) {
	c.calls++
	return &logicrpcpb.CleanupSubscriptionResponse{Removed: true}, nil
}

func (c *routeCapture) BatchRenewSubscriptions(_ context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
	c.renewCalls++
	results := make([]*logicrpcpb.SubscriptionLeaseResult, len(req.Entries))
	for index, entry := range req.Entries {
		results[index] = &logicrpcpb.SubscriptionLeaseResult{
			TargetFarmId: entry.GetTargetFarmId(), SubscriptionId: entry.GetSubscriptionId(),
			JoinAttemptId: entry.GetJoinAttemptId(),
			Status:        logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED,
			AcceptedTtlMs: 30_000,
		}
	}
	return &logicrpcpb.BatchRenewSubscriptionsResponse{Results: results}, nil
}

func TestRoutedLogicClientUsesActorDomain(t *testing.T) {
	player, farm := &routeCapture{}, &routeCapture{}
	client := NewRoutedLogicClient(player, farm)
	for _, commandType := range []commonpb.CommandType{commonpb.CommandType_GET_PLAYER_BOOTSTRAP, commonpb.CommandType_BUY, commonpb.CommandType_SELL, commonpb.CommandType_PLANT, commonpb.CommandType_JOIN_FARM} {
		_, err := client.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{Command: &commonpb.ClientCommandEnvelope{CommandType: commandType}})
		if err != nil {
			t.Fatal(err)
		}
	}
	if player.calls != 3 || farm.calls != 2 {
		t.Fatalf("player=%d farm=%d", player.calls, farm.calls)
	}
}

func TestRoutedLogicClientSendsExactCleanupOnlyToFarm(t *testing.T) {
	player, farm := &routeCapture{}, &routeCapture{}
	client := NewRoutedLogicClient(player, farm)
	cleaner := client.(SubscriptionCleaner)
	response, err := cleaner.CleanupSubscription(context.Background(), &logicrpcpb.CleanupSubscriptionRequest{Subscription: &logicrpcpb.SubscriptionFence{FarmId: 9}})
	if err != nil || response == nil || !response.Removed {
		t.Fatalf("cleanup response=%v err=%v", response, err)
	}
	if player.calls != 0 || farm.calls != 1 {
		t.Fatalf("cleanup player_calls=%d farm_calls=%d", player.calls, farm.calls)
	}
}

func TestRoutedLogicClientIsolatesStaticRenewalByFarm(t *testing.T) {
	player, farm := &routeCapture{}, &routeCapture{}
	client := NewRoutedLogicClient(player, farm).(*RoutedLogicClient)
	partition, err := client.SubscriptionLeasePartition(context.Background(), 9)
	if err != nil {
		t.Fatal(err)
	}
	entries := []*logicrpcpb.SubscriptionLeaseEntry{
		{TargetFarmId: 9, SubscriptionId: "sub-9", JoinAttemptId: "attempt-9"},
		{TargetFarmId: 10, SubscriptionId: "sub-10", JoinAttemptId: "attempt-10"},
	}
	response, err := client.BatchRenewSubscriptionsPartition(context.Background(),
		subscriptionLeaseDispatchHolder("gate-a", "boot-a", partition),
		&logicrpcpb.BatchRenewSubscriptionsRequest{
			SourceGatewayId: "gate-a", SourceGatewayBootId: "boot-a", Entries: entries,
		})
	if err != nil {
		t.Fatal(err)
	}
	if farm.renewCalls != 1 || response.GetResults()[0].GetStatus() != logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED ||
		response.GetResults()[1].GetStatus() != logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_ROUTE_STALE {
		t.Fatalf("static farm partition mixed ownership: calls=%d results=%v", farm.renewCalls, response.GetResults())
	}
}
