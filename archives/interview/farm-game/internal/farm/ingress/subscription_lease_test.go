package ingress

import (
	"context"
	"testing"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"
	farmactor "farmgame/internal/farm/actor"
	"farmgame/internal/storage/blobstore"
)

func TestBatchRenewSubscriptionsUsesOneTrustedNowAndIndependentResults(t *testing.T) {
	registry := farmactor.NewRegistry(blobstore.NewMemoryStore(), 16)
	defer registry.Stop()
	registry.SubscriptionHub().SetSubscriptionLeaseConfig(5*time.Second, 7*time.Second)

	createdAt := time.UnixMilli(1_900_000_000_000)
	var join *farmactor.JoinResult
	_, err := registry.Submit(context.Background(), 501, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		var joinErr error
		join, joinErr = actor.CreateOrReuseJoining(farmactor.JoinRequest{
			OperationID: "join-op", JoinAttemptID: "attempt-1",
			ViewerPlayerID: 71, SessionEpoch: 9,
			GatewayID: "gate-a", GatewayBootID: "boot-a", ConnectionID: "conn-a",
			TransportGeneration: 4, RouteRevision: 12,
		}, createdAt)
		return join, joinErr
	})
	if err != nil || join == nil {
		t.Fatalf("join=%+v err=%v", join, err)
	}

	service := NewService(registry, nil)
	trustedNow := createdAt.Add(time.Second)
	nowCalls := 0
	service.now = func() time.Time {
		nowCalls++
		return trustedNow
	}
	target := &logicrpcpb.DownlinkTargetFence{
		TargetPlayerId: 71, GatewayId: "gate-a", GatewayBootId: "boot-a",
		ConnectionId: "conn-a", ConnectionGeneration: 4, SessionEpoch: 9,
		SessionRouteRevision: 12,
	}
	response, err := service.BatchRenewSubscriptions(context.Background(),
		&logicrpcpb.BatchRenewSubscriptionsRequest{
			SourceGatewayId: "gate-a", SourceGatewayBootId: "boot-a",
			Entries: []*logicrpcpb.SubscriptionLeaseEntry{
				{
					TargetFarmId: 501, SubscriptionId: join.SubscriptionID,
					JoinAttemptId: join.JoinAttemptID, Target: target, RequestedTtlMs: 30_000,
				},
				{
					TargetFarmId: 501, SubscriptionId: join.SubscriptionID,
					JoinAttemptId: join.JoinAttemptID,
					Target: &logicrpcpb.DownlinkTargetFence{
						TargetPlayerId: 71, GatewayId: "gate-b", GatewayBootId: "boot-a",
						ConnectionId: "conn-a", ConnectionGeneration: 4, SessionEpoch: 9,
						SessionRouteRevision: 12,
					},
					RequestedTtlMs: 30_000,
				},
			},
		})
	if err != nil {
		t.Fatal(err)
	}
	if nowCalls != 1 {
		t.Fatalf("trusted now calls=%d want=1", nowCalls)
	}
	if len(response.Results) != 2 {
		t.Fatalf("results=%d", len(response.Results))
	}
	if got := response.Results[0]; got.Status != logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED ||
		got.AcceptedTtlMs != 7_000 || got.GetServerTimeMs() != trustedNow.UnixMilli() {
		t.Fatalf("renewed result=%+v", got)
	}
	if got := response.Results[1]; got.Status != logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_STALE_FENCE ||
		got.GetServerTimeMs() != trustedNow.UnixMilli() {
		t.Fatalf("stale result=%+v", got)
	}
}
