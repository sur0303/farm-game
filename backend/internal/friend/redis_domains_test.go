package friend

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"farmgame/internal/storage/redis/rediskey"
)

// TestRedisFriendshipDomainsSplitAuthorityFromProjection proves the C6A
// domain split: Pair/Quota/Share authority facts land on the authority Redis
// while FriendIndex, index repair and navigation projections land on the
// projection Redis, even when both clients point at different endpoints.
func TestRedisFriendshipDomainsSplitAuthorityFromProjection(t *testing.T) {
	authorityServer := miniredis.RunT(t)
	projectionServer := miniredis.RunT(t)
	authorityClient := redis.NewClient(&redis.Options{Addr: authorityServer.Addr()})
	projectionClient := redis.NewClient(&redis.Options{Addr: projectionServer.Addr()})
	t.Cleanup(func() { _ = authorityClient.Close() })
	t.Cleanup(func() { _ = projectionClient.Close() })

	namespace, err := rediskey.NewNamespace("test", "friend-domains")
	if err != nil {
		t.Fatal(err)
	}
	options := RedisFriendshipOptions{Profile: DefaultFriendRedisProfile(namespace), Limits: FriendRedisLimits{MaxFriends: 10}}
	repositories, err := NewRedisFriendshipRepositoriesForDomains(authorityClient, projectionClient, options)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now().UnixMilli()

	// Authority write: full RedeemShare lifecycle touches Pair, Quota and
	// Share; none of it may touch the projection Redis.
	token, outcome, err := repositories.Share.GenerateShareToken(ctx, 31001, "domain-token-1",
		redisFriendPublic(31001, "GENERATE_SHARE", "domain-generate"), redisFriendHash("domain-generate"), now)
	if err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("generate share token: outcome=%+v err=%v", outcome, err)
	}
	result, err := repositories.svcRedeem(t, ctx, 31002, 31001, token, "domain-redeem", now)
	if err != nil || result.Outcome.Status != StatusSucceeded {
		t.Fatalf("redeem share: result=%+v err=%v", result, err)
	}
	// Projection write: index row, repair due entry and navigation visited
	// mark must land only on the projection Redis.
	if _, err := repositories.Index.Apply(ctx, 31002, FriendIndexEntry{
		FriendPlayerID: 31001, RelationID: result.RelationID, RelationEpoch: result.RelationEpoch, ActivatedAtMs: now,
	}); err != nil {
		t.Fatalf("apply friend index: %v", err)
	}
	if err := repositories.RepairQueue.Enqueue(ctx, 31002, now+1000); err != nil {
		t.Fatalf("enqueue repair: %v", err)
	}
	if err := repositories.Navigation.MarkVisited(ctx, VisitFence{
		ViewerPlayerID: 31002, TargetFarmID: 31001, ActiveSessionEpoch: 1, SubscriptionID: "domain-sub",
	}); err != nil {
		t.Fatalf("mark navigation visited: %v", err)
	}

	assertNoProjectionKeys(t, authorityServer, "authority")
	assertNoAuthorityKeys(t, projectionServer, "projection")

	// Authority reads succeed against the authority Redis and fail closed on
	// the projection Redis.
	if snap, err := repositories.Quota.GetQuota(ctx, 31002); err != nil || snap.Occupancy() == 0 {
		t.Fatalf("quota on authority: %+v err=%v", snap, err)
	}
	if pair, err := repositories.Pair.GetPair(ctx, 31001, 31002); err != nil || pair.Relation.Status != RelationStatusActive {
		t.Fatalf("pair on authority: %+v err=%v", pair.Relation, err)
	}
	if _, _, err := repositories.Index.Lookup(ctx, 31002, 31001); err != nil {
		t.Fatalf("index lookup on projection: %v", err)
	}
	visited, err := repositories.Navigation.ListVisited(ctx, 31002)
	if err != nil || len(visited) != 1 || !containsUint64(visited, 31001) {
		t.Fatalf("navigation visited on projection: %v err=%v", visited, err)
	}
}

func (r *RedisFriendshipRepositories) svcRedeem(t *testing.T, ctx context.Context, redeemer, inviter uint64,
	token ShareToken, operationID string, nowMs int64) (RedeemShareResult, error) {
	t.Helper()
	svc, err := NewDomainService(r.Pair, r.Quota, r.Share)
	if err != nil {
		t.Fatal(err)
	}
	return svc.RedeemShare(ctx, RedeemShareCommand{
		RedeemerPlayerID: redeemer, InviterPlayerID: inviter,
		TokenID: token.TokenID, TokenGeneration: token.TokenGeneration,
		OperationID: operationID, PayloadHash: testHash(operationID), NowMs: nowMs,
	})
}

func assertNoAuthorityKeys(t *testing.T, server *miniredis.Miniredis, label string) {
	t.Helper()
	for _, key := range server.Keys() {
		if containsString(key, ":friend-auth:") {
			t.Fatalf("%s Redis unexpectedly holds authority key %q", label, key)
		}
	}
}

func assertNoProjectionKeys(t *testing.T, server *miniredis.Miniredis, label string) {
	t.Helper()
	for _, key := range server.Keys() {
		if containsString(key, ":friend-index-v1:") || containsString(key, ":friend-index-repair-v1:") ||
			containsString(key, ":friend-navigation-v3:") {
			t.Fatalf("%s Redis unexpectedly holds projection key %q", label, key)
		}
	}
}

func containsString(haystack, needle string) bool {
	return len(needle) == 0 || len(haystack) >= len(needle) && haystackContains(haystack, needle)
}

func haystackContains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

func containsUint64(values map[uint64]struct{}, want uint64) bool {
	_, ok := values[want]
	return ok
}
