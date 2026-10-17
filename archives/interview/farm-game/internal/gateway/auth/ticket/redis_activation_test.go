package ticket

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/gateway/session"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type redisActivationFixture struct {
	server    *miniredis.Miniredis
	client    *redis.Client
	authority *session.RedisAuthority
	store     *RedisStore
	tickets   *Service
	playerID  uint64
	randByte  atomic.Uint32
}

func newRedisActivationFixture(t *testing.T, playerID uint64) *redisActivationFixture {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	fixture := &redisActivationFixture{
		server: server,
		client: client,
		authority: session.NewRedis(client, session.Config{
			Secret: []byte("ticket-activation-test-secret"), AuthorityEpoch: 1,
		}),
		store:    NewRedisStore(client),
		playerID: playerID,
	}
	fixture.tickets = New(fixture.store, WithTTL(time.Minute), WithRand(func(buf []byte) (int, error) {
		value := byte(fixture.randByte.Add(1))
		for index := range buf {
			buf[index] = value + byte(index)
		}
		return len(buf), nil
	}))
	return fixture
}

func (f *redisActivationFixture) issue(t *testing.T) (*Issued, *session.Candidate) {
	t.Helper()
	candidate, err := f.authority.Reserve(context.Background(), f.playerID, "")
	if err != nil {
		t.Fatalf("reserve ticket candidate: %v", err)
	}
	issued, err := f.tickets.Issue(context.Background(), Bound{
		PlayerID: f.playerID, SessionID: candidate.SessionID,
		SessionAuthorityEpoch: candidate.AuthorityEpoch,
		ReservedSessionEpoch:  candidate.ReservedSessionEpoch,
	})
	if err != nil {
		t.Fatalf("issue ticket: %v", err)
	}
	return issued, candidate
}

func activationRoute(suffix string) session.RouteFence {
	return session.RouteFence{
		GatewayID: "gate-" + suffix, GatewayBootID: "boot-" + suffix,
		ConnectionID: "connection-" + suffix, ConnectionGeneration: 1,
	}
}

func exerciseRedisTicketUint64Boundaries(t *testing.T, client *redis.Client, playerBase uint64) {
	t.Helper()
	ctx := context.Background()
	authority := session.NewRedis(client, session.Config{
		Secret: []byte("ticket-uint64-boundary-secret"), AuthorityEpoch: 0,
	})
	globalAuthority, err := authority.AuthorityEpoch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	localAuthority := globalAuthority
	if localAuthority == 0 {
		localAuthority = 1
	}
	localAuthorityText := strconv.FormatUint(localAuthority, 10)
	const (
		doubleBoundary = uint64(1 << 53)
		belowBoundary  = doubleBoundary - 1
		aboveBoundary  = doubleBoundary + 1
		maximum        = ^uint64(0)
	)
	maximumText := strconv.FormatUint(maximum, 10)
	beforeMaximumText := strconv.FormatUint(maximum-1, 10)
	store := NewRedisStore(client)
	tickets := New(store, WithTTL(time.Minute))
	keys := make([]string, 0, 6)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Del(cleanupCtx, keys...).Err()
	})

	trackTicket := func(token string, playerID uint64) string {
		t.Helper()
		key := rediskey.ConnectionTicketForPlayer(playerID, Hash(token))
		keys = append(keys, key)
		return key
	}
	issue := func(playerID uint64, candidate *session.Candidate) (*Issued, string) {
		t.Helper()
		issued, issueErr := tickets.Issue(ctx, Bound{
			PlayerID: playerID, SessionID: candidate.SessionID,
			SessionAuthorityEpoch: candidate.AuthorityEpoch,
			ReservedSessionEpoch:  candidate.ReservedSessionEpoch,
		})
		if issueErr != nil {
			t.Fatal(issueErr)
		}
		return issued, trackTicket(issued.Token, playerID)
	}
	assertUnchanged := func(t *testing.T, key string, before map[string]string, beforeTTL time.Duration) {
		t.Helper()
		after, stateErr := client.HGetAll(ctx, key).Result()
		if stateErr != nil {
			t.Fatal(stateErr)
		}
		if !reflect.DeepEqual(after, before) {
			t.Fatalf("failed increment changed %q: before=%v after=%v", key, before, after)
		}
		afterTTL, ttlErr := client.PTTL(ctx, key).Result()
		if ttlErr != nil {
			t.Fatal(ttlErr)
		}
		if afterTTL > beforeTTL || afterTTL < beforeTTL-5*time.Second {
			t.Fatalf("failed increment reset/changed %q TTL: before=%s after=%s", key, beforeTTL, afterTTL)
		}
	}

	// A rolling-upgrade fixture has only the legacy authority `epoch` and only
	// the legacy ticket `active_session_epoch`. Leading zeroes prove comparison
	// is canonical decimal comparison rather than raw text equality.
	mixedPlayer := playerBase
	mixedKey := rediskey.Session(mixedPlayer)
	keys = append(keys, mixedKey)
	if err := client.HSet(ctx, mixedKey,
		"authority_epoch", "000"+localAuthorityText,
		"epoch", "000"+strconv.FormatUint(belowBoundary, 10),
		"next_epoch", "000"+strconv.FormatUint(doubleBoundary, 10),
		"route_revision", "000"+strconv.FormatUint(doubleBoundary, 10),
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.PExpire(ctx, mixedKey, 10*time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	mixedCandidate, err := authority.Reserve(ctx, mixedPlayer, "ticket-mixed-version-session")
	if err != nil {
		t.Fatal(err)
	}
	if mixedCandidate.ActiveSessionEpoch != belowBoundary ||
		mixedCandidate.ReservedSessionEpoch != aboveBoundary ||
		mixedCandidate.AuthorityEpoch != localAuthority {
		t.Fatalf("mixed candidate=%+v, want active=%d reserved=%d authority=%d",
			mixedCandidate, belowBoundary, aboveBoundary, localAuthority)
	}
	mixedTicket, mixedTicketKey := issue(mixedPlayer, mixedCandidate)
	if err := client.HDel(ctx, mixedKey, "active_epoch").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, mixedKey, "epoch", "000"+strconv.FormatUint(doubleBoundary, 10)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HDel(ctx, mixedTicketKey, "reserved_session_epoch").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(ctx, mixedTicketKey,
		"session_authority_epoch", "000"+localAuthorityText,
		"active_session_epoch", "000"+strconv.FormatUint(aboveBoundary, 10),
	).Err(); err != nil {
		t.Fatal(err)
	}
	mixedRoute := activationRoute("uint64-mixed")
	mixedBound, mixedActivation, err := tickets.Activate(ctx, authority, mixedTicket.Token, mixedRoute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if mixedBound.SessionAuthorityEpoch != localAuthority || mixedBound.ReservedSessionEpoch != aboveBoundary ||
		mixedActivation.Route.SessionEpoch != aboveBoundary || mixedActivation.Route.RouteRevision != aboveBoundary {
		t.Fatalf("mixed ticket activation bound=%+v activation=%+v", mixedBound, mixedActivation)
	}
	mixedFields, err := client.HGetAll(ctx, mixedKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if mixedFields["active_epoch"] != strconv.FormatUint(aboveBoundary, 10) ||
		mixedFields["epoch"] != strconv.FormatUint(aboveBoundary, 10) ||
		mixedFields["route_revision"] != strconv.FormatUint(aboveBoundary, 10) {
		t.Fatalf("mixed activation did not canonicalize exact fences: %+v", mixedFields)
	}
	if _, replay, err := tickets.Activate(ctx, authority, mixedTicket.Token, mixedRoute, time.Second); err != nil ||
		replay.Route.SessionEpoch != aboveBoundary || replay.Route.RouteRevision != aboveBoundary {
		t.Fatalf("high-epoch replay=%+v err=%v", replay, err)
	}

	// MaxUint64 is a valid final reservation and activation. Every subsequent
	// increment must fail without changing fields or refreshing the key TTL.
	maximumPlayer := playerBase + 1
	maximumKey := rediskey.Session(maximumPlayer)
	keys = append(keys, maximumKey)
	if err := client.HSet(ctx, maximumKey,
		"authority_epoch", localAuthorityText,
		"active_epoch", beforeMaximumText, "epoch", beforeMaximumText,
		"next_epoch", beforeMaximumText, "route_revision", "0",
	).Err(); err != nil {
		t.Fatal(err)
	}
	maximumCandidate, err := authority.Reserve(ctx, maximumPlayer, "ticket-maximum-session")
	if err != nil || maximumCandidate.ReservedSessionEpoch != maximum {
		t.Fatalf("maximum candidate=%+v err=%v", maximumCandidate, err)
	}
	maximumTicket, _ := issue(maximumPlayer, maximumCandidate)
	_, maximumActivation, err := tickets.Activate(ctx, authority, maximumTicket.Token,
		activationRoute("uint64-maximum"), time.Second)
	if err != nil || maximumActivation.Route.SessionEpoch != maximum {
		t.Fatalf("maximum ticket activation=%+v err=%v", maximumActivation, err)
	}
	if err := client.PExpire(ctx, maximumKey, 37*time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	maximumBefore, err := client.HGetAll(ctx, maximumKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	maximumBeforeTTL, err := client.PTTL(ctx, maximumKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Reserve(ctx, maximumPlayer, "ticket-overflow-session"); err == nil {
		t.Fatal("reserve incremented MaxUint64 after ticket activation")
	}
	assertUnchanged(t, maximumKey, maximumBefore, maximumBeforeTTL)
	if err := authority.Revoke(ctx, maximumPlayer); err == nil {
		t.Fatal("revoke incremented MaxUint64 after ticket activation")
	}
	assertUnchanged(t, maximumKey, maximumBefore, maximumBeforeTTL)

	// The ticket script also increments route_revision. It must not consume the
	// ticket or partially replace authority when that independent uint64 fence
	// is already at MaxUint64.
	revisionPlayer := playerBase + 2
	revisionKey := rediskey.Session(revisionPlayer)
	keys = append(keys, revisionKey)
	if err := client.HSet(ctx, revisionKey,
		"authority_epoch", localAuthorityText, "active_epoch", "1", "epoch", "1",
		"next_epoch", "2", "route_revision", maximumText,
	).Err(); err != nil {
		t.Fatal(err)
	}
	revisionCandidate := &session.Candidate{
		PlayerID: revisionPlayer, SessionID: "ticket-revision-overflow-session",
		AuthorityEpoch: localAuthority, ReservedSessionEpoch: 2, ActiveSessionEpoch: 1,
	}
	revisionTicket, revisionTicketKey := issue(revisionPlayer, revisionCandidate)
	if err := client.PExpire(ctx, revisionKey, 37*time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	revisionBefore, err := client.HGetAll(ctx, revisionKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	revisionBeforeTTL, err := client.PTTL(ctx, revisionKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	ticketBefore, err := client.HGetAll(ctx, revisionTicketKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	ticketBeforeTTL, err := client.PTTL(ctx, revisionTicketKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := tickets.Activate(ctx, authority, revisionTicket.Token,
		activationRoute("uint64-revision-overflow"), time.Second); err == nil {
		t.Fatal("ticket activation incremented MaxUint64 route revision")
	}
	assertUnchanged(t, revisionKey, revisionBefore, revisionBeforeTTL)
	assertUnchanged(t, revisionTicketKey, ticketBefore, ticketBeforeTTL)
}

func TestRedisTicketUint64EpochBoundaries(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	exerciseRedisTicketUint64Boundaries(t, client, 66_000)
}

func TestConsumedTicketReplayRequiresCurrentCompleteAuthorityFence(t *testing.T) {
	fixture := newRedisActivationFixture(t, 61_001)
	ctx := context.Background()
	first, firstCandidate := fixture.issue(t)
	firstRoute := activationRoute("a")
	_, firstActivation, err := fixture.tickets.Activate(ctx, fixture.authority, first.Token, firstRoute, time.Second)
	if err != nil {
		t.Fatalf("activate A: %v", err)
	}
	if firstActivation.Route.SessionEpoch != firstCandidate.ReservedSessionEpoch {
		t.Fatalf("A epoch=%d want=%d", firstActivation.Route.SessionEpoch, firstCandidate.ReservedSessionEpoch)
	}
	if _, replay, err := fixture.tickets.Activate(ctx, fixture.authority, first.Token, firstRoute, time.Second); err != nil ||
		replay.Route.RouteRevision != firstActivation.Route.RouteRevision {
		t.Fatalf("current exact A replay=%+v err=%v", replay, err)
	}

	second, secondCandidate := fixture.issue(t)
	secondRoute := activationRoute("b")
	_, secondActivation, err := fixture.tickets.Activate(ctx, fixture.authority, second.Token, secondRoute, time.Second)
	if err != nil {
		t.Fatalf("activate B: %v", err)
	}
	if secondActivation.Route.SessionEpoch != secondCandidate.ReservedSessionEpoch {
		t.Fatalf("B epoch=%d want=%d", secondActivation.Route.SessionEpoch, secondCandidate.ReservedSessionEpoch)
	}

	// The consumed A tombstone still names A's exact transport, but B is now
	// authoritative. A must be classified superseded and must not be returned to
	// another Gate as an apparently live binding.
	if _, _, err := fixture.tickets.Activate(ctx, fixture.authority, first.Token, firstRoute, time.Second); !errors.Is(err, session.ErrActivationSuperseded) {
		t.Fatalf("consumed A replay after B err=%v, want ErrActivationSuperseded", err)
	}
	fields, err := fixture.client.HGetAll(ctx, rediskey.Session(fixture.playerID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if fields["session_id"] != second.Bound.SessionID ||
		fields["active_epoch"] != strconv.FormatUint(secondCandidate.ReservedSessionEpoch, 10) ||
		fields["route_gate_id"] != secondRoute.GatewayID ||
		fields["route_gate_boot_id"] != secondRoute.GatewayBootID ||
		fields["route_connection_id"] != secondRoute.ConnectionID ||
		fields["route_connection_generation"] != "1" ||
		fields["route_revision"] != strconv.FormatUint(secondActivation.Route.RouteRevision, 10) {
		t.Fatalf("A replay changed B authority: %+v", fields)
	}

	third, _ := fixture.issue(t)
	thirdRoute := activationRoute("c")
	if _, _, err := fixture.tickets.Activate(ctx, fixture.authority, third.Token, thirdRoute, time.Second); err != nil {
		t.Fatalf("activate C: %v", err)
	}
	if err := fixture.authority.Revoke(ctx, fixture.playerID); err != nil {
		t.Fatalf("revoke C: %v", err)
	}
	if _, _, err := fixture.tickets.Activate(ctx, fixture.authority, third.Token, thirdRoute, time.Second); !errors.Is(err, session.ErrActivationSuperseded) {
		t.Fatalf("consumed C replay after revoke err=%v, want ErrActivationSuperseded", err)
	}
}

func TestConsumedTicketReplayRaceCannotOutliveReplacement(t *testing.T) {
	fixture := newRedisActivationFixture(t, 61_101)
	ctx := context.Background()
	for iteration := 0; iteration < 32; iteration++ {
		first, _ := fixture.issue(t)
		firstRoute := activationRoute("race-a-" + strconv.Itoa(iteration))
		if _, _, err := fixture.tickets.Activate(ctx, fixture.authority, first.Token, firstRoute, time.Second); err != nil {
			t.Fatalf("iteration %d activate A: %v", iteration, err)
		}
		second, _ := fixture.issue(t)
		secondRoute := activationRoute("race-b-" + strconv.Itoa(iteration))
		start := make(chan struct{})
		replayErr := make(chan error, 1)
		replacementErr := make(chan error, 1)
		go func() {
			<-start
			_, _, err := fixture.tickets.Activate(ctx, fixture.authority, first.Token, firstRoute, time.Second)
			replayErr <- err
		}()
		go func() {
			<-start
			_, _, err := fixture.tickets.Activate(ctx, fixture.authority, second.Token, secondRoute, time.Second)
			replacementErr <- err
		}()
		close(start)
		// A replay may linearize before B and resolve the still-current unknown
		// outcome. Regardless of that order, B must win and every replay after B
		// completion must be fenced.
		if err := <-replayErr; err != nil && !errors.Is(err, session.ErrActivationSuperseded) {
			t.Fatalf("iteration %d concurrent A replay: %v", iteration, err)
		}
		if err := <-replacementErr; err != nil {
			t.Fatalf("iteration %d activate B: %v", iteration, err)
		}
		if _, _, err := fixture.tickets.Activate(ctx, fixture.authority, first.Token, firstRoute, time.Second); !errors.Is(err, session.ErrActivationSuperseded) {
			t.Fatalf("iteration %d post-B replay err=%v, want ErrActivationSuperseded", iteration, err)
		}
		fields, err := fixture.client.HGetAll(ctx, rediskey.Session(fixture.playerID)).Result()
		if err != nil {
			t.Fatal(err)
		}
		if fields["session_id"] != second.Bound.SessionID ||
			fields["route_connection_id"] != secondRoute.ConnectionID {
			t.Fatalf("iteration %d final authority is not B: %+v", iteration, fields)
		}
	}
}

func TestConsumedTicketReplayComparesEveryCurrentRouteComponent(t *testing.T) {
	mutations := map[string]func(context.Context, *redisActivationFixture, *session.ActivationResult) error{
		"authority epoch": func(ctx context.Context, f *redisActivationFixture, activated *session.ActivationResult) error {
			return f.client.HSet(ctx, rediskey.Session(f.playerID), "authority_epoch", activated.Session.AuthorityEpoch+1).Err()
		},
		"session id": func(ctx context.Context, f *redisActivationFixture, _ *session.ActivationResult) error {
			return f.client.HSet(ctx, rediskey.Session(f.playerID), "session_id", "replacement-session").Err()
		},
		"active epoch": func(ctx context.Context, f *redisActivationFixture, activated *session.ActivationResult) error {
			return f.client.HSet(ctx, rediskey.Session(f.playerID), "active_epoch", activated.Route.SessionEpoch+1).Err()
		},
		"gateway id": func(ctx context.Context, f *redisActivationFixture, _ *session.ActivationResult) error {
			return f.client.HSet(ctx, rediskey.Session(f.playerID), "route_gate_id", "replacement-gate").Err()
		},
		"gateway boot id": func(ctx context.Context, f *redisActivationFixture, _ *session.ActivationResult) error {
			return f.client.HSet(ctx, rediskey.Session(f.playerID), "route_gate_boot_id", "replacement-boot").Err()
		},
		"connection id": func(ctx context.Context, f *redisActivationFixture, _ *session.ActivationResult) error {
			return f.client.HSet(ctx, rediskey.Session(f.playerID), "route_connection_id", "replacement-connection").Err()
		},
		"connection generation": func(ctx context.Context, f *redisActivationFixture, _ *session.ActivationResult) error {
			return f.client.HSet(ctx, rediskey.Session(f.playerID), "route_connection_generation", 2).Err()
		},
		"route revision": func(ctx context.Context, f *redisActivationFixture, activated *session.ActivationResult) error {
			return f.client.HSet(ctx, rediskey.Session(f.playerID), "route_revision", activated.Route.RouteRevision+1).Err()
		},
		"cleared route": func(ctx context.Context, f *redisActivationFixture, _ *session.ActivationResult) error {
			return f.client.HDel(ctx, rediskey.Session(f.playerID),
				"route_gate_id", "route_gate_boot_id", "route_connection_id",
				"route_connection_generation", "route_revision", "route_lease_expire_at_ms").Err()
		},
	}
	var playerID uint64 = 62_000
	for name, mutate := range mutations {
		playerID++
		currentPlayerID := playerID
		t.Run(name, func(t *testing.T) {
			fixture := newRedisActivationFixture(t, currentPlayerID)
			ctx := context.Background()
			issued, _ := fixture.issue(t)
			route := activationRoute(strconv.FormatUint(currentPlayerID, 10))
			_, activated, err := fixture.tickets.Activate(ctx, fixture.authority, issued.Token, route, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if err := mutate(ctx, fixture, activated); err != nil {
				t.Fatal(err)
			}
			if _, _, err := fixture.tickets.Activate(ctx, fixture.authority, issued.Token, route, time.Second); !errors.Is(err, session.ErrActivationSuperseded) {
				t.Fatalf("replay with changed %s err=%v, want ErrActivationSuperseded", name, err)
			}
			key := rediskey.ConnectionTicketForPlayer(fixture.playerID, Hash(issued.Token))
			if status, err := fixture.client.HGet(ctx, key, "status").Result(); err != nil || status != "SUPERSEDED" {
				t.Fatalf("ticket status=%q want SUPERSEDED err=%v", status, err)
			}
		})
	}
}

func TestConsumedTicketReplayUsesRemainingCurrentRenewedLease(t *testing.T) {
	fixture := newRedisActivationFixture(t, 63_001)
	ctx := context.Background()
	issued, _ := fixture.issue(t)
	route := activationRoute("renewed")
	_, activated, err := fixture.tickets.Activate(ctx, fixture.authority, issued.Token, route, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	renewals, err := fixture.authority.RenewRoutes(ctx, []session.ActiveRoute{activated.Route}, 2*time.Second)
	if err != nil || len(renewals) != 1 || !renewals[0].Renewed {
		t.Fatalf("renewals=%+v err=%v", renewals, err)
	}
	_, replay, err := fixture.tickets.Activate(ctx, fixture.authority, issued.Token, route, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if replay.AcceptedLeaseTTL <= time.Second || replay.AcceptedLeaseTTL > 2*time.Second {
		t.Fatalf("replay remaining renewed TTL=%s, want (1s,2s]", replay.AcceptedLeaseTTL)
	}
	if !replay.Route.LeaseExpiresAt.Equal(renewals[0].Route.LeaseExpiresAt) {
		t.Fatalf("replay lease expiry=%s want renewed authority expiry=%s",
			replay.Route.LeaseExpiresAt, renewals[0].Route.LeaseExpiresAt)
	}
}

func TestRedisTicketTTLIsPositiveAndMillisecondGranular(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisStore(client)
	ctx := context.Background()
	bound := Bound{
		PlayerID: 64_001, SessionID: "ttl-session", SessionAuthorityEpoch: 1,
		ReservedSessionEpoch: 1, IssuedAt: time.Now(),
	}

	const roundedToken = "v1.64001.rounded"
	if err := store.Save(ctx, roundedToken, bound, time.Nanosecond); err != nil {
		t.Fatalf("positive sub-ms save: %v", err)
	}
	roundedKey, err := store.key(roundedToken, bound.PlayerID)
	if err != nil {
		t.Fatal(err)
	}
	if !server.Exists(roundedKey) || server.TTL(roundedKey) != time.Millisecond {
		t.Fatalf("rounded ticket exists=%v TTL=%s, want true/1ms", server.Exists(roundedKey), server.TTL(roundedKey))
	}

	for _, ttl := range []time.Duration{0, -time.Nanosecond} {
		token := "v1.64001.rejected-" + strconv.FormatInt(int64(ttl), 10)
		if err := store.Save(ctx, token, bound, ttl); err == nil {
			t.Fatalf("Save TTL %s succeeded", ttl)
		}
		key, keyErr := store.key(token, bound.PlayerID)
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		if server.Exists(key) {
			t.Fatalf("rejected TTL %s left a ticket", ttl)
		}
	}

	// Keep the Lua contract defensive even if a future caller bypasses the Go
	// validation: PEXPIRE 0 deletes a key, so the script must not report success.
	const zeroScriptToken = "v1.64001.lua-zero"
	zeroKey, err := store.key(zeroScriptToken, bound.PlayerID)
	if err != nil {
		t.Fatal(err)
	}
	value, err := store.scripts.Run(ctx, saveScriptName, []string{zeroKey},
		bound.PlayerID, bound.SessionID, bound.SessionAuthorityEpoch, bound.ReservedSessionEpoch,
		bound.ActiveSessionEpoch, bound.IssuedAt.UnixMilli(), time.Now().Add(time.Minute).UnixMilli(), 0).Int64()
	if err != nil {
		t.Fatal(err)
	}
	if value != 0 || server.Exists(zeroKey) {
		t.Fatalf("zero-TTL Lua result=%d exists=%v, want 0/false", value, server.Exists(zeroKey))
	}

	service := New(store, WithTTL(time.Nanosecond))
	if service.ttl != time.Millisecond {
		t.Fatalf("service sub-ms TTL=%s want 1ms", service.ttl)
	}
}

func TestRedisTicketActivationRoundsPositiveSubMillisecondLease(t *testing.T) {
	fixture := newRedisActivationFixture(t, 65_001)
	issued, _ := fixture.issue(t)
	_, activated, err := fixture.tickets.Activate(context.Background(), fixture.authority,
		issued.Token, activationRoute("sub-ms"), time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	if activated.AcceptedLeaseTTL != time.Millisecond {
		t.Fatalf("accepted sub-ms lease TTL=%s want 1ms", activated.AcceptedLeaseTTL)
	}
	if !fixture.server.Exists(rediskey.Session(fixture.playerID)) {
		t.Fatal("activation reported success after deleting the authority key")
	}
}
