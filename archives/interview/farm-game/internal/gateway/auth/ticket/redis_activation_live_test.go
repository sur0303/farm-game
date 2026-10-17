package ticket

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"farmgame/internal/gateway/session"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/redis/go-redis/v9"
)

// TestLiveRedisTicketActivation verifies the exact two-key Lua contract used
// by ClientHello against the configured company Redis. It is skipped in unit
// environments; staging/CI can opt in with FARM_REDIS_URL.
func TestLiveRedisTicketActivation(t *testing.T) {
	uri := os.Getenv("FARM_REDIS_URL")
	if uri == "" {
		t.Skip("FARM_REDIS_URL not set; skipping live ticket activation test")
	}
	options, err := redis.ParseURL(uri)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	playerID := uint64(time.Now().UnixNano())
	if playerID == 0 {
		playerID = 1
	}
	authority := session.NewRedis(client, session.Config{
		Secret: []byte("live-ticket-test"), AuthorityEpoch: 1,
	})
	store := NewRedisStore(client)
	tickets := New(store, WithTTL(100*time.Millisecond))
	issuedTokens := make([]string, 0, 4)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		keys := []string{rediskey.Session(playerID)}
		for _, token := range issuedTokens {
			keys = append(keys, rediskey.ConnectionTicketForPlayer(playerID, Hash(token)))
		}
		_ = client.Del(cleanupCtx, keys...).Err()
	})

	issue := func(candidate *session.Candidate) *Issued {
		t.Helper()
		value, issueErr := tickets.Issue(ctx, Bound{
			PlayerID: playerID, SessionID: candidate.SessionID,
			SessionAuthorityEpoch: candidate.AuthorityEpoch,
			ReservedSessionEpoch:  candidate.ReservedSessionEpoch,
		})
		if issueErr != nil {
			t.Fatal(issueErr)
		}
		issuedTokens = append(issuedTokens, value.Token)
		return value
	}
	route := func(suffix string) session.RouteFence {
		return session.RouteFence{
			GatewayID: "gate-" + suffix, GatewayBootID: "boot-" + suffix,
			ConnectionID: "connection-" + suffix, ConnectionGeneration: 1,
		}
	}

	firstCandidate, err := authority.Reserve(ctx, playerID, "")
	if err != nil {
		t.Fatal(err)
	}
	first := issue(firstCandidate)
	redisBefore, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	requestedLeaseTTL := time.Second
	_, activated, err := tickets.Activate(ctx, authority, first.Token, route("a"), requestedLeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	redisAfter, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if activated.Route.SessionEpoch != firstCandidate.ReservedSessionEpoch || activated.Route.RouteRevision != 1 {
		t.Fatalf("first activation=%+v", activated)
	}
	if activated.AcceptedLeaseTTL != requestedLeaseTTL {
		t.Fatalf("accepted lease ttl=%s want=%s", activated.AcceptedLeaseTTL, requestedLeaseTTL)
	}
	if activated.Route.LeaseExpiresAt.Before(redisBefore.Add(requestedLeaseTTL)) ||
		activated.Route.LeaseExpiresAt.After(redisAfter.Add(requestedLeaseTTL)) {
		t.Fatalf("lease expiry=%s is not based on Redis TIME window [%s,%s]",
			activated.Route.LeaseExpiresAt, redisBefore.Add(requestedLeaseTTL), redisAfter.Add(requestedLeaseTTL))
	}

	// The original UNUSED expiry has elapsed, but the consumed activation
	// tombstone must still resolve an Unknown outcome for the exact same fence.
	time.Sleep(120 * time.Millisecond)
	_, replay, err := tickets.Activate(ctx, authority, first.Token, route("a"), time.Second)
	if err != nil || replay.Route.RouteRevision != activated.Route.RouteRevision {
		t.Fatalf("activation tombstone replay=%+v err=%v", replay, err)
	}
	if replay.AcceptedLeaseTTL <= 0 || replay.AcceptedLeaseTTL >= activated.AcceptedLeaseTTL {
		t.Fatalf("activation replay ttl=%s must be positive remaining ttl below original %s",
			replay.AcceptedLeaseTTL, activated.AcceptedLeaseTTL)
	}
	if _, _, err := tickets.Activate(ctx, authority, first.Token, route("other"), time.Second); !errors.Is(err, ErrRedeemFailed) {
		t.Fatalf("consumed ticket accepted a different fence: %v", err)
	}

	olderCandidate, err := authority.Reserve(ctx, playerID, "")
	if err != nil {
		t.Fatal(err)
	}
	older := issue(olderCandidate)
	newerCandidate, err := authority.Reserve(ctx, playerID, "")
	if err != nil {
		t.Fatal(err)
	}
	newer := issue(newerCandidate)
	_, newest, err := tickets.Activate(ctx, authority, newer.Token, route("new"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if newest.OldRoute == nil || newest.OldRoute.ConnectionID != route("a").ConnectionID {
		t.Fatalf("old route not returned: %+v", newest.OldRoute)
	}
	if _, _, err := tickets.Activate(ctx, authority, first.Token, route("a"), time.Second); !errors.Is(err, session.ErrActivationSuperseded) {
		t.Fatalf("consumed A tombstone replayed after B activation: %v", err)
	}
	if _, _, err := tickets.Activate(ctx, authority, older.Token, route("old"), time.Second); !errors.Is(err, session.ErrActivationSuperseded) {
		t.Fatalf("older candidate reclaimed active route: %v", err)
	}
	if cleared, err := authority.CompareAndClearRoute(ctx, *newest.OldRoute); err != nil || cleared {
		t.Fatalf("delayed old disconnect cleared new route: cleared=%v err=%v", cleared, err)
	}
	if active, err := authority.ActiveEpoch(ctx, playerID); err != nil || active != newerCandidate.ReservedSessionEpoch {
		t.Fatalf("active epoch=%d want=%d err=%v", active, newerCandidate.ReservedSessionEpoch, err)
	}
	renewed, err := authority.RenewRoutes(ctx, []session.ActiveRoute{newest.Route}, 2*time.Second)
	if err != nil || len(renewed) != 1 || !renewed[0].Renewed ||
		renewed[0].AcceptedLeaseTTL != 2*time.Second ||
		!renewed[0].Route.LeaseExpiresAt.After(newest.Route.LeaseExpiresAt) {
		t.Fatalf("route lease renewal=%+v err=%v", renewed, err)
	}

	// A matching Fence is not enough after the Redis-authoritative deadline.
	// Force the stored lease into the past and prove the Lua script reports pure
	// expiry instead of reviving it.
	redisNow, err := client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	expiredAtMS := redisNow.Add(-time.Millisecond).UnixMilli()
	if err := client.HSet(ctx, rediskey.Session(playerID),
		"route_lease_expire_at_ms", expiredAtMS).Err(); err != nil {
		t.Fatal(err)
	}
	expiredRenewal, err := authority.RenewRoutes(ctx, []session.ActiveRoute{newest.Route}, time.Second)
	if err != nil || len(expiredRenewal) != 1 || expiredRenewal[0].Renewed ||
		!expiredRenewal[0].LeaseExpired {
		t.Fatalf("expired route renewal=%+v err=%v", expiredRenewal, err)
	}
	storedExpiry, err := client.HGet(ctx, rediskey.Session(playerID), "route_lease_expire_at_ms").Result()
	if err != nil || storedExpiry != strconv.FormatInt(expiredAtMS, 10) {
		t.Fatalf("expired route was rewritten: stored=%q want=%d err=%v", storedExpiry, expiredAtMS, err)
	}
	if err := authority.Revoke(ctx, playerID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := tickets.Activate(ctx, authority, newer.Token, route("new"), time.Second); !errors.Is(err, session.ErrActivationSuperseded) {
		t.Fatalf("consumed B tombstone replayed after revoke/kick fence: %v", err)
	}
}

// TestLiveRedisUint64EpochBoundaries runs the same mixed-version, 2^53±1,
// MaxUint64, replay, TTL, and no-partial-write contract as miniredis against
// opt-in real Redis Lua.
func TestLiveRedisUint64EpochBoundaries(t *testing.T) {
	uri := os.Getenv("FARM_REDIS_URL")
	if uri == "" {
		t.Skip("FARM_REDIS_URL not set; skipping live uint64 epoch test")
	}
	options, err := redis.ParseURL(uri)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	playerBase := uint64(time.Now().UnixNano())
	if playerBase == 0 || playerBase > ^uint64(0)-2 {
		playerBase = 1
	}
	exerciseRedisTicketUint64Boundaries(t, client, playerBase)
}

func TestTicketAndSessionKeysSharePlayerHashTag(t *testing.T) {
	playerID := uint64(10001)
	tag := fmt.Sprintf("{%d}", playerID)
	ticketKey := rediskey.ConnectionTicketForPlayer(playerID, "hash")
	sessionKey := rediskey.Session(playerID)
	if !strings.Contains(ticketKey, tag) || !strings.Contains(sessionKey, tag) {
		t.Fatalf("keys must share Redis Cluster tag: ticket=%q session=%q", ticketKey, sessionKey)
	}
}
