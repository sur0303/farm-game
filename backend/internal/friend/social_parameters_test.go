package friend

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"farmgame/internal/socialprofile"
)

func TestConfirmedFriendLimitThroughCanonicalAddWorkflow(t *testing.T) {
	limit := socialprofile.Current().EffectiveMaxFriends
	env := newTestEnv(t, limit)
	token := env.generateToken(t, 10001, 0)
	for ordinal := 1; ordinal <= limit+1; ordinal++ {
		result := env.redeem(t, uint64(20000+ordinal), 10001, token, fmt.Sprintf("profile-boundary-%d", ordinal), int64(ordinal))
		switch {
		case ordinal <= limit:
			if result.Outcome.Status != StatusSucceeded {
				t.Fatalf("friend %d = %v/%v, want success", ordinal, result.Outcome.Status, result.Outcome.Reason)
			}
		case result.Outcome.Status != StatusRejected || result.Outcome.Reason != ReasonFriendLimitReached ||
			"ERR_"+string(result.Outcome.Reason) != "ERR_FRIEND_LIMIT_REACHED":
			t.Fatalf("friend %d = %v/%v, want ERR_FRIEND_LIMIT_REACHED", ordinal, result.Outcome.Status, result.Outcome.Reason)
		}
	}
	snapshot, err := env.quotas.GetQuota(context0, 10001)
	if err != nil || snapshot.Occupancy() != limit {
		t.Fatalf("inviter quota = %+v err=%v", snapshot, err)
	}
}

func seedQuota(t *testing.T, repository *MemoryQuotaRepository, playerID uint64, count int) {
	t.Helper()
	for ordinal := 0; ordinal < count; ordinal++ {
		relationID := fmt.Sprintf("seed-%d-%d", playerID, ordinal)
		reservation := reservationFor(t, fmt.Sprintf("seed-wf-%d", ordinal), playerID, relationID, 1)
		reserveIdentity := CommitIdentity{ParticipantInvocationID: "seed-reserve-" + relationID, ParticipantStep: "RESERVE"}
		outcome, err := repository.ReserveFriendSlot(context0, playerID, reservation, reserveIdentity, testHash("reserve-"+relationID), int64(ordinal+1))
		if err != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("seed reserve %s = %+v/%v", relationID, outcome, err)
		}
		finalizeIdentity := CommitIdentity{ParticipantInvocationID: "seed-finalize-" + relationID, ParticipantStep: "FINALIZE"}
		outcome, err = repository.FinalizeFriendSlot(context0, playerID, reservation, finalizeIdentity, testHash("finalize-"+relationID), int64(ordinal+1))
		if err != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("seed finalize %s = %+v/%v", relationID, outcome, err)
		}
	}
}

func TestConfirmedFriendLimitTwoSidedConcurrentNearCapacity(t *testing.T) {
	limit := socialprofile.Current().EffectiveMaxFriends
	env := newTestEnv(t, limit)
	players := []uint64{11001, 11002, 11003, 11004}
	for _, playerID := range players {
		seedQuota(t, env.quotas, playerID, limit-1)
	}
	tokenC := env.generateToken(t, players[2], 0)
	tokenD := env.generateToken(t, players[3], 0)
	commands := []struct {
		redeemer uint64
		inviter  uint64
		token    ShareToken
	}{
		{players[0], players[2], tokenC}, {players[1], players[2], tokenC},
		{players[0], players[3], tokenD}, {players[1], players[3], tokenD},
	}
	var wg sync.WaitGroup
	results := make([]RedeemShareResult, len(commands))
	errs := make([]error, len(commands))
	for index, command := range commands {
		wg.Add(1)
		go func(index int, command struct {
			redeemer uint64
			inviter  uint64
			token    ShareToken
		}) {
			defer wg.Done()
			results[index], errs[index] = env.svc.RedeemShare(context0, RedeemShareCommand{
				RedeemerPlayerID: command.redeemer, InviterPlayerID: command.inviter,
				TokenID: command.token.TokenID, TokenGeneration: command.token.TokenGeneration,
				OperationID: fmt.Sprintf("near-capacity-%d", index), PayloadHash: testHash(fmt.Sprintf("near-capacity-%d", index)), NowMs: 100,
			})
		}(index, command)
	}
	wg.Wait()
	for index, err := range errs {
		if err != nil {
			t.Fatalf("command %d: %v", index, err)
		}
		outcome := results[index].Outcome
		if outcome.Status != StatusSucceeded && (outcome.Status != StatusRejected || outcome.Reason != ReasonFriendLimitReached) {
			t.Fatalf("command %d = %v/%v", index, outcome.Status, outcome.Reason)
		}
	}
	for _, playerID := range players {
		snapshot, err := env.quotas.GetQuota(context0, playerID)
		if err != nil || snapshot.Occupancy() > limit {
			t.Fatalf("player %d quota = %+v err=%v", playerID, snapshot, err)
		}
	}
}

func TestConfirmedShareTokenLifecycleAndEstablishedRelationIsolation(t *testing.T) {
	ttl := socialprofile.Current().ShareTokenTTL.Milliseconds()
	record := mustIssuer(t, 12001)
	token, outcome, err := record.GenerateShareToken("token-current", ttl, 1000)
	if err != nil || outcome.Status != StatusSucceeded || token.ExpiresAtMs != 1000+ttl {
		t.Fatalf("generate = %+v %+v %v", token, outcome, err)
	}
	again, outcome, err := record.GenerateShareToken("token-unused-candidate", ttl, token.ExpiresAtMs-1)
	if err != nil || outcome.Status != StatusNoChange || again.TokenID != token.TokenID {
		t.Fatalf("repeat generate = %+v %+v %v", again, outcome, err)
	}
	if _, outcome, err = record.AcceptShareTokenAdmission(token.TokenID, token.TokenGeneration, SharePurposeAddFriend, 12002, "before-expiry", token.ExpiresAtMs-1); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("expiry-1ms admission = %+v %v", outcome, err)
	}
	if _, outcome, err = record.AcceptShareTokenAdmission(token.TokenID, token.TokenGeneration, SharePurposeAddFriend, 12003, "at-expiry", token.ExpiresAtMs); err != nil || outcome.Status != StatusRejected || outcome.Reason != ReasonShareTokenExpired {
		t.Fatalf("expiry boundary = %+v %v", outcome, err)
	}

	rotated, _, err := record.RotateShareToken("token-rotated", ttl, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if _, outcome, err = record.AcceptShareTokenAdmission(token.TokenID, token.TokenGeneration, SharePurposeAddFriend, 12004, "after-rotate", 2001); err != nil || outcome.Status != StatusRejected {
		t.Fatalf("old token after rotate = %+v %v", outcome, err)
	}
	record.RevokeShareToken(2002)
	if _, outcome, err = record.AcceptShareTokenAdmission(rotated.TokenID, rotated.TokenGeneration, SharePurposeAddFriend, 12005, "after-revoke", 2003); err != nil || outcome.Status != StatusRejected {
		t.Fatalf("token after revoke = %+v %v", outcome, err)
	}

	env := newTestEnv(t, socialprofile.Current().EffectiveMaxFriends)
	relationToken := env.generateToken(t, 13001, 0)
	result := env.redeem(t, 13002, 13001, relationToken, "establish-relation", 1)
	if result.Outcome.Status != StatusSucceeded {
		t.Fatal(result.Outcome)
	}
	if _, _, err := env.shares.RotateShareToken(context0, 13001, "replacement-token",
		publicIdentity(13001, "ROTATE_SHARE_TOKEN", "rotate-established"), testHash("rotate-established"), 2); err != nil {
		t.Fatal(err)
	}
	if _, _, err := env.shares.RevokeShareToken(context0, 13001,
		publicIdentity(13001, "REVOKE_SHARE_TOKEN", "revoke-established"), testHash("revoke-established"), 3); err != nil {
		t.Fatal(err)
	}
	pair, err := env.pairs.GetPair(context0, 13001, 13002)
	if err != nil || pair.Relation.Status != RelationStatusActive || pair.Relation.RelationID != result.RelationID {
		t.Fatalf("established relation changed with token lifecycle: %+v err=%v", pair.Relation, err)
	}
}

func TestRedisShareTokenBoundaryUsesServerTime(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, socialprofile.Current().EffectiveMaxFriends)
	defer server.Close()
	base := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	server.SetTime(base)
	ctx := context.Background()
	token, outcome, err := repositories.Share.GenerateShareToken(ctx, 14001, "redis-time-token",
		redisFriendPublic(14001, "GENERATE_SHARE", "redis-time-generate"), redisFriendHash("redis-time-generate"), -1)
	if err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("generate = %+v/%+v/%v", token, outcome, err)
	}
	server.SetTime(time.UnixMilli(token.ExpiresAtMs - 1))
	if _, outcome, err = repositories.Share.AcceptShareTokenAdmission(ctx, 14001, token.TokenID, token.TokenGeneration, 14002,
		"redis-time-before", redisFriendPublic(14002, "ACCEPT_SHARE", "redis-time-before"), redisFriendHash("redis-time-before"), token.ExpiresAtMs+999999); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("server expiry-1ms = %+v %v", outcome, err)
	}
	server.SetTime(time.UnixMilli(token.ExpiresAtMs))
	if _, outcome, err = repositories.Share.AcceptShareTokenAdmission(ctx, 14001, token.TokenID, token.TokenGeneration, 14003,
		"redis-time-at", redisFriendPublic(14003, "ACCEPT_SHARE", "redis-time-at"), redisFriendHash("redis-time-at"), 0); err != nil || outcome.Status != StatusRejected || outcome.Reason != ReasonShareTokenExpired {
		t.Fatalf("server expiry boundary = %+v %v", outcome, err)
	}
}

func seedRedisQuotaFixture(t *testing.T, repositories *RedisFriendshipRepositories, playerID uint64, count, effectiveLimit int) []QuotaReservation {
	t.Helper()
	ctx := context.Background()
	authorityID, err := quotaAuthorityID(playerID)
	if err != nil {
		t.Fatal(err)
	}
	group, err := repositories.authority.group(FriendQuotaAuthority, authorityID, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	meta := map[string]string{
		"schema_version": "friend-auth-v1", "authority_kind": string(FriendQuotaAuthority),
		"layout_version": group.ref.LayoutVersion, "bucket_id": strconv.FormatUint(uint64(group.ref.BucketID), 10),
		"receipt_count": "0", "receipt_bytes": "0", "pending_work_count": "0", "pending_work_bytes": "0",
		"attempt_count": "0", "attempt_bytes": "0", "last_updated_at_ms": "1",
	}
	record := map[string]string{
		"_meta:schema_version": "friend-quota-v1", "_meta:authority_layout_version": group.ref.LayoutVersion,
		"_meta:record_version": "1", "_meta:updated_at_ms": "1", "_meta:business_state_digest": strings.Repeat("0", 64),
		"_meta:active_count": strconv.Itoa(count), "_meta:reserved_count": "0", "_meta:effective_max_friends": strconv.Itoa(effectiveLimit),
	}
	reservations := make([]QuotaReservation, count)
	for index := range reservations {
		relationID := fmt.Sprintf("grandfathered-%d", index)
		reservations[index] = QuotaReservation{ReservationID: "res-" + relationID, WorkflowID: "wf-" + relationID, RelationID: relationID, TargetRelationEpoch: 1}
		record["active:"+relationID] = "1"
	}
	record["_meta:logical_bytes"] = strconv.FormatUint(friendHashLogicalBytes(record), 10)
	for key, fields := range map[string]map[string]string{string(group.meta): meta, string(group.record): record} {
		values := make([]any, 0, len(fields)*2)
		for field, value := range fields {
			values = append(values, field, value)
		}
		if err := repositories.authority.client.HSet(ctx, key, values...).Err(); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}
	return reservations
}

func TestRedisQuotaGrandfatheredProfileDecrease(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	const playerID uint64 = 15001
	reservations := seedRedisQuotaFixture(t, repositories, playerID, 250, 300)
	base := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	server.SetTime(base)
	nowMs := base.UnixMilli()

	lowered, err := NewRedisFriendshipRepositories(repositories.authority.client, RedisFriendshipOptions{
		Profile: repositories.authority.keys.Profile(), Limits: FriendRedisLimits{MaxFriends: 200},
	})
	if err != nil {
		t.Fatal(err)
	}
	candidate := QuotaReservation{ReservationID: "overflow-candidate", WorkflowID: "overflow-wf", RelationID: "overflow-relation", TargetRelationEpoch: 1}
	outcome, err := lowered.Quota.ReserveFriendSlot(ctx, playerID, candidate,
		redisFriendPublic(playerID, "RESERVE", "overflow-reject"), redisFriendHash("overflow-reject"), nowMs)
	if err != nil || outcome.Status != StatusRejected || outcome.Reason != ReasonFriendLimitReached {
		t.Fatalf("overflow reserve = %+v/%v", outcome, err)
	}
	snapshot, err := lowered.Quota.GetQuota(ctx, playerID)
	if err != nil || snapshot.EffectiveMaxFriends != 200 || len(snapshot.ActiveRelations) != 250 {
		t.Fatalf("lowered snapshot = %+v/%v", snapshot, err)
	}
	remove := func(repository *RedisFriendshipRepositories, reservation QuotaReservation, operation string) {
		t.Helper()
		removed, removeErr := repository.Quota.RemoveActiveRelationIfEpochMatches(ctx, playerID, reservation.RelationID, 1,
			redisFriendPublic(playerID, "REMOVE", operation), redisFriendHash("remove-"+operation), nowMs)
		if removeErr != nil || removed.Status != StatusSucceeded {
			t.Fatalf("remove %s = %+v/%v", operation, removed, removeErr)
		}
	}
	// A grandfathered record remains mutable: production Remove accepts a
	// deletion while occupancy is above the lowered limit.
	remove(lowered, reservations[249], "grandfathered-249")
	if snapshot, err = lowered.Quota.GetQuota(ctx, playerID); err != nil || snapshot.Occupancy() != 249 {
		t.Fatalf("grandfathered delete snapshot = %+v/%v", snapshot, err)
	}

	// Use a second canonical fixture at the exact threshold so this policy test
	// does not turn into an unrelated receipt-retention stress test.
	thresholdRepositories, thresholdServer := newFriendRedisTestRepositories(t, 300)
	defer thresholdServer.Close()
	thresholdReservations := seedRedisQuotaFixture(t, thresholdRepositories, playerID, 200, 300)
	thresholdServer.SetTime(base)
	loweredAtThreshold, err := NewRedisFriendshipRepositories(thresholdRepositories.authority.client, RedisFriendshipOptions{
		Profile: thresholdRepositories.authority.keys.Profile(), Limits: FriendRedisLimits{MaxFriends: 200},
	})
	if err != nil {
		t.Fatal(err)
	}
	remove(loweredAtThreshold, thresholdReservations[199], "threshold-199")
	outcome, err = loweredAtThreshold.Quota.ReserveFriendSlot(ctx, playerID, candidate,
		redisFriendPublic(playerID, "RESERVE", "overflow-allow-at-199"), redisFriendHash("overflow-allow-at-199"), nowMs)
	if err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("reserve at 199 = %+v/%v", outcome, err)
	}
	snapshot, err = loweredAtThreshold.Quota.GetQuota(ctx, playerID)
	if err != nil || snapshot.Occupancy() != 200 || snapshot.EffectiveMaxFriends != 200 {
		t.Fatalf("final snapshot = %+v/%v", snapshot, err)
	}
}
