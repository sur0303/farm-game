package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"

	"farmgame/internal/friend"
	"farmgame/internal/gateway/session"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/redis/go-redis/v9"
)

const (
	probeGatewayID     = "gate-session-route-c2"
	probeGatewayBootID = "boot-session-route-c2"
)

type routeState string

const (
	routeStateActive    routeState = "ACTIVE"
	routeStateMissing   routeState = "missing"
	routeStateExpired   routeState = "expired"
	routeStateMalformed routeState = "malformed"
)

type fixturePlan struct {
	FriendCount    int
	OnlineRatioPct int
	DispatchBatch  int
	RouteStateMode routeState // empty => ACTIVE/missing by online ratio
	Iterations     int
	PlayerIDBase   uint64
	CellIndex      int
	RunID          string
	CellName       string
}

type fixtureCard struct {
	OwnerID           uint64
	FriendIDs         []uint64
	ExpectedActive    []uint64
	ExpectedMissing   []uint64
	ExpectedExpired   []uint64
	ExpectedMalformed []uint64
	OnlineRatioPct    int
	FriendCount       int
}

func expectedOnlineCount(friendCount, onlineRatioPct int) int {
	if friendCount <= 0 || onlineRatioPct <= 0 {
		return 0
	}
	return friendCount * onlineRatioPct / 100
}

// deterministicPlayerIDBase derives a stable high-range player-ID base from the
// run id plus cell/iteration identity so concurrent probe runs do not collide.
func deterministicPlayerIDBase(runID string, cellIndex, iteration int) uint64 {
	sum := sha256.Sum256([]byte(fmt.Sprintf(
		"session-route-c2|%s|cell=%d|iter=%d", runID, cellIndex, iteration)))
	n := binary.BigEndian.Uint64(sum[:8]) & ((1 << 48) - 1)
	return 0x7000_0000_0000_0000 | n
}

func ownershipMarker(runID string, playerID uint64) string {
	return fmt.Sprintf("c2:%s:%d", runID, playerID)
}

func ownershipGenerationFor(runID string) uint64 {
	sum := sha256.Sum256([]byte("session-route-c2-gen|" + runID))
	generation := binary.BigEndian.Uint64(sum[:8]) & 0xffff_ffff
	if generation == 0 {
		generation = 1
	}
	return generation
}

func buildFixtureCard(plan fixturePlan) fixtureCard {
	ownerID := plan.PlayerIDBase
	friends := make([]uint64, plan.FriendCount)
	for i := 0; i < plan.FriendCount; i++ {
		friends[i] = plan.PlayerIDBase + 1 + uint64(i)
	}
	card := fixtureCard{
		OwnerID: ownerID, FriendIDs: friends,
		OnlineRatioPct: plan.OnlineRatioPct, FriendCount: plan.FriendCount,
	}
	switch plan.RouteStateMode {
	case routeStateExpired:
		card.ExpectedExpired = append([]uint64(nil), friends...)
	case routeStateMalformed:
		card.ExpectedMalformed = append([]uint64(nil), friends...)
	case routeStateMissing:
		card.ExpectedMissing = append([]uint64(nil), friends...)
	default:
		online := expectedOnlineCount(plan.FriendCount, plan.OnlineRatioPct)
		card.ExpectedActive = append([]uint64(nil), friends[:online]...)
		card.ExpectedMissing = append([]uint64(nil), friends[online:]...)
	}
	return card
}

func installFriendIndex(ctx context.Context, index *friend.MemoryFriendIndexRepository, card fixtureCard) error {
	for _, peer := range card.FriendIDs {
		relationID, err := friend.RelationIDFor(card.OwnerID, peer)
		if err != nil {
			return err
		}
		if _, err := index.Apply(ctx, card.OwnerID, friend.FriendIndexEntry{
			FriendPlayerID: peer, RelationID: relationID, RelationEpoch: 1,
			ActivatedAtMs: int64(peer),
		}); err != nil {
			return err
		}
	}
	return nil
}

func ensureSessionKeyAbsent(ctx context.Context, client redis.UniversalClient, playerID uint64) error {
	key := rediskey.Session(playerID)
	exists, err := client.Exists(ctx, key).Result()
	if err != nil {
		return err
	}
	if exists > 0 {
		return fmt.Errorf("session key pre-exists (refusing to overwrite): %s", key)
	}
	return nil
}

func writeActiveRoute(ctx context.Context, client redis.UniversalClient, authority *session.RedisAuthority,
	tracker *exactKeyTracker, runID string, generation uint64, playerID uint64) error {
	if err := ensureSessionKeyAbsent(ctx, client, playerID); err != nil {
		return err
	}
	candidate, err := authority.Reserve(ctx, playerID, "")
	if err != nil {
		return err
	}
	key := rediskey.Session(playerID)
	marker := ownershipMarker(runID, playerID)
	generationText := formatGeneration(generation)
	// Never track rediskey.SessionAuthorityEpoch(): it is global, not run-owned.
	_, err = authority.Activate(ctx, session.ActivationRequest{
		PlayerID: playerID, SessionID: candidate.SessionID,
		AuthorityEpoch: candidate.AuthorityEpoch, ReservedSessionEpoch: candidate.ReservedSessionEpoch,
		Route: session.RouteFence{
			GatewayID: probeGatewayID, GatewayBootID: probeGatewayBootID,
			ConnectionID: marker, ConnectionGeneration: generation,
		},
		LeaseTTL: time.Minute,
	})
	if err != nil {
		return err
	}
	tracker.TrackOwned(key, marker, generationText)
	return nil
}

func writeExpiredRoute(ctx context.Context, client redis.UniversalClient, authority *session.RedisAuthority,
	tracker *exactKeyTracker, runID string, generation uint64, playerID uint64) error {
	if err := writeActiveRoute(ctx, client, authority, tracker, runID, generation, playerID); err != nil {
		return err
	}
	key := rediskey.Session(playerID)
	return client.HSet(ctx, key, "route_lease_expire_at_ms", time.Now().Add(-time.Minute).UnixMilli()).Err()
}

func writeMalformedRoute(ctx context.Context, client redis.UniversalClient, authority *session.RedisAuthority,
	tracker *exactKeyTracker, runID string, generation uint64, playerID uint64) error {
	if err := writeActiveRoute(ctx, client, authority, tracker, runID, generation, playerID); err != nil {
		return err
	}
	key := rediskey.Session(playerID)
	const badGeneration = "bad"
	if err := client.HSet(ctx, key, "route_connection_generation", badGeneration).Err(); err != nil {
		return err
	}
	// Record the intentional fixture generation so cleanup still owns this key.
	tracker.TrackOwned(key, ownershipMarker(runID, playerID), badGeneration)
	return nil
}

func installSessionRoutes(ctx context.Context, client redis.UniversalClient, authority *session.RedisAuthority,
	tracker *exactKeyTracker, runID string, generation uint64, card fixtureCard, mode routeState) error {
	// Every fixture state — including routeStateMissing — must refuse measurement
	// when any production Session key for this card already exists (foreign data).
	for _, playerID := range card.FriendIDs {
		if err := ensureSessionKeyAbsent(ctx, client, playerID); err != nil {
			return err
		}
	}
	switch mode {
	case routeStateExpired:
		for _, playerID := range card.ExpectedExpired {
			if err := writeExpiredRoute(ctx, client, authority, tracker, runID, generation, playerID); err != nil {
				return err
			}
		}
	case routeStateMalformed:
		for _, playerID := range card.ExpectedMalformed {
			if err := writeMalformedRoute(ctx, client, authority, tracker, runID, generation, playerID); err != nil {
				return err
			}
		}
	case routeStateMissing:
		// Intentionally write nothing; pre-exist checks above already ran.
	default:
		for _, playerID := range card.ExpectedActive {
			if err := writeActiveRoute(ctx, client, authority, tracker, runID, generation, playerID); err != nil {
				return err
			}
		}
		// Expected-missing friends must remain absent (checked above; re-assert
		// so a concurrent foreign write before measurement fails the cell).
		for _, playerID := range card.ExpectedMissing {
			if err := ensureSessionKeyAbsent(ctx, client, playerID); err != nil {
				return err
			}
		}
	}
	return nil
}
