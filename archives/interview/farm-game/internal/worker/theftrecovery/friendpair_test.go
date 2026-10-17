package theftrecovery

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"farmgame/internal/friend"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type friendPairRecoveryFixture struct {
	client       *redis.Client
	server       *miniredis.Miniredis
	repositories *friend.RedisFriendshipRepositories
	pairID       string
	bucketID     uint32
	orderID      string
	cycleID      string
}

func newFriendPairRecoveryFixture(t *testing.T) *friendPairRecoveryFixture {
	t.Helper()
	server := miniredis.RunT(t)
	now := time.UnixMilli(1_800_000_100_000)
	server.SetTime(now)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), PoolSize: 16})
	namespace, err := rediskey.NewNamespace("test", "th3c")
	if err != nil {
		t.Fatal(err)
	}
	profile := friend.DefaultFriendRedisProfile(namespace)
	profile.PairBucketCount = 8
	profile.QuotaBucketCount = 8
	profile.ShareBucketCount = 8
	repositories, err := friend.NewRedisFriendshipRepositories(client, friend.RedisFriendshipOptions{Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		server.Close()
	})

	const visitor, owner = uint64(41001), uint64(42002)
	pairID, err := friend.RelationIDFor(visitor, owner)
	if err != nil {
		t.Fatal(err)
	}
	activateHash := sha256.Sum256([]byte("friend-pair-recovery-activate"))
	activated, err := repositories.Pair.ActivateFriendRelation(context.Background(), friend.ActivateCommand{
		InviterPlayerID: visitor, RedeemerPlayerID: owner, TargetRelationEpoch: 1,
		TokenID: "friend-pair-recovery-token", TokenGeneration: 1, OperationID: "friend-pair-recovery-activate",
	}, friend.CommitIdentity{WorkAddressID: pairID, ParticipantInvocationID: "friend-pair-recovery-activate", ParticipantStep: "ACTIVATE"}, activateHash[:])
	if err != nil || activated.Status != friend.StatusSucceeded {
		t.Fatalf("activate pair: outcome=%+v err=%v", activated, err)
	}

	payloadHash := sha256.Sum256([]byte("canonical-theft-payload"))
	targetBindingHash := sha256.Sum256([]byte("farm=42002/plot=3"))
	action := friend.ActionAdmissionCommand{
		Identity:    friend.CommitIdentity{SubjectPlayerID: visitor, CommandType: "THEFT", OperationID: "friend-pair-recovery-theft"},
		PayloadHash: payloadHash[:], VisitorPlayerID: visitor, OwnerPlayerID: owner, TargetFarmID: owner,
		Scope: friend.ActionScopeTheft, TargetBindingHash: targetBindingHash[:], NowMs: now.UnixMilli(), PermitTTLMs: 60_000,
	}
	admitted, err := repositories.Pair.CommitFriendActionAdmission(context.Background(), action)
	if err != nil || admitted.Outcome.Status != friend.StatusSucceeded || admitted.DispatchOrderID == "" {
		t.Fatalf("admit theft: outcome=%+v err=%v", admitted, err)
	}
	builder, err := friend.NewFriendshipKeyBuilder(profile)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := builder.BucketFor(friend.FriendPairAuthority, pairID)
	if err != nil {
		t.Fatal(err)
	}
	cycleID, err := workidentity.StableOpaqueID("friend-pair-recovery-cycle", action.Identity.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	return &friendPairRecoveryFixture{client: client, server: server, repositories: repositories,
		pairID: pairID, bucketID: bucket.BucketID, orderID: admitted.DispatchOrderID, cycleID: cycleID}
}

func TestFriendPairRecoveryUsesValidatedDueIndexAndSameSlotLeaseKeys(t *testing.T) {
	fixture := newFriendPairRecoveryFixture(t)
	ctx := context.Background()
	decodeCalls := 0
	dispatch, err := NewFriendPairDispatchRepository(FriendPairDispatchRepositoryConfig{
		Repository: fixture.repositories,
		TargetDecode: func(_ context.Context, _ friend.PairRecoverySourceOrder, permit friend.ActionPermitSnapshot) (FriendPairTheftTarget, error) {
			decodeCalls++
			if permit.Scope != friend.ActionScopeTheft || permit.RelationID != fixture.pairID {
				return FriendPairTheftTarget{}, fmt.Errorf("unexpected permit: %+v", permit)
			}
			return FriendPairTheftTarget{PlotID: 3, CropCycleID: fixture.cycleID}, nil
		},
		MaxBuckets: int(fixture.bucketID + 1),
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err := dispatch.Discover(ctx, DiscoveryRequest{Limit: 8})
	if err != nil {
		t.Fatal(err)
	}
	var candidate DispatchCandidate
	for _, item := range page.Candidates {
		ref, decodeErr := workidentity.DecodeWorkRefV1(item.Member)
		if decodeErr == nil && ref.WorkType == FriendActionDispatchOrder {
			candidate = item
			break
		}
	}
	if candidate.Member == "" {
		t.Fatalf("DueIndex page=%+v did not contain dispatch order", page)
	}
	order, err := dispatch.LoadDispatch(ctx, candidate)
	if err != nil {
		t.Fatal(err)
	}
	order, err = NormalizeDispatchOrder(order)
	if err != nil {
		t.Fatal(err)
	}
	if order.Work.WorkID != fixture.orderID || order.PairID != fixture.pairID || order.CropCycleID != fixture.cycleID {
		t.Fatalf("decoded order=%+v", order)
	}
	if decodeCalls != 1 {
		t.Fatalf("target decoder calls=%d, want 1", decodeCalls)
	}

	keyResolver := FriendPairWorkLeaseKeys{Repository: fixture.repositories}
	workKey, err := keyResolver.WorkKey(order.Work)
	if err != nil {
		t.Fatal(err)
	}
	dueKey, err := keyResolver.DueIndexKey(order.Work)
	if err != nil {
		t.Fatal(err)
	}
	metaKey, err := keyResolver.BucketMetaKey(order.Work)
	if err != nil {
		t.Fatal(err)
	}
	recoveryKey, err := keyResolver.RecoveryKey(order.Work)
	if err != nil {
		t.Fatal(err)
	}
	if err := rediskey.AssertSameSlot(rediskey.RedisKey(workKey), rediskey.RedisKey(dueKey), rediskey.RedisKey(metaKey), rediskey.RedisKey(recoveryKey)); err != nil {
		t.Fatalf("Friend Pair lease keys are not co-located: %v", err)
	}

	leases, err := NewFriendPairWorkLeaseRepository(fixture.repositories, 10*time.Millisecond, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := leases.Claim(ctx, ClaimRequest{Order: order, HolderID: "friend-worker-1", BootID: "friend-boot-1", LeaseToken: "friend-lease-1", RequestedTTL: 200 * time.Millisecond})
	if err != nil || claim.Status != LeaseClaimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	score, err := fixture.client.ZScore(ctx, dueKey, order.WorkRef).Result()
	if err != nil || int64(score) != claim.LeaseExpireAtMs {
		t.Fatalf("claim DueIndex score=%v claim expiry=%d err=%v", score, claim.LeaseExpireAtMs, err)
	}
	proof := SourceProofHash(order)
	renewed, err := leases.Renew(ctx, RenewRequest{Order: order, Fence: LeaseFence{HolderID: "friend-worker-1", BootID: "friend-boot-1", LeaseToken: "friend-lease-1", LeaseEpoch: claim.LeaseEpoch, WorkVersion: claim.WorkVersion}, RequestedTTL: 300 * time.Millisecond, PayloadHash: order.PayloadHash, SourceProofHash: proof})
	if err != nil || renewed.Status != LeaseRenewed {
		t.Fatalf("renew=%+v err=%v", renewed, err)
	}
	score, err = fixture.client.ZScore(ctx, dueKey, order.WorkRef).Result()
	if err != nil || int64(score) != renewed.LeaseExpireAtMs {
		t.Fatalf("renew DueIndex score=%v renewal expiry=%d err=%v", score, renewed.LeaseExpireAtMs, err)
	}
	resultProof := sha256.Sum256([]byte("friend-pair-recovery-result"))
	completed, err := leases.Complete(ctx, CompleteRequest{Order: order,
		Fence: LeaseFence{HolderID: "friend-worker-1", BootID: "friend-boot-1", LeaseToken: "friend-lease-1", LeaseEpoch: claim.LeaseEpoch, WorkVersion: claim.WorkVersion},
		Proof: CompletionProof{PayloadHash: order.PayloadHash, SourceProofHash: proof, ResultProofHash: resultProof[:]}})
	if err != nil || completed.Status != LeaseCompleted {
		t.Fatalf("complete=%+v err=%v", completed, err)
	}
	if _, err := fixture.client.ZScore(ctx, dueKey, order.WorkRef).Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("completed dispatch remains in DueIndex: err=%v", err)
	}
	state, err := fixture.client.HGet(ctx, workKey, "state").Result()
	if err != nil || state != "COMPLETED" {
		t.Fatalf("work state=%q err=%v", state, err)
	}
	storedResult, err := fixture.client.HGet(ctx, recoveryKey, "result_proof_hash").Result()
	if err != nil || storedResult != HexHash(resultProof[:]) {
		t.Fatalf("recovery result proof=%q err=%v", storedResult, err)
	}
}

type resultProcessorSpy struct{ calls int }

func (p *resultProcessorSpy) ProcessResultOrder(context.Context, DispatchCandidate) (bool, error) {
	p.calls++
	return false, nil
}

func TestTheftRecoveryRoutesCareBeforeTargetLookupAndLeavesCareOrdersUntouched(t *testing.T) {
	fixture := newFriendPairRecoveryFixture(t)
	ctx := context.Background()
	payloadHash := sha256.Sum256([]byte("canonical-care-payload"))
	targetHash := sha256.Sum256([]byte("care-target-binding"))
	care, err := fixture.repositories.Pair.CommitFriendActionAdmission(ctx, friend.ActionAdmissionCommand{
		Identity:    friend.CommitIdentity{SubjectPlayerID: 41001, CommandType: "CARE", OperationID: "friend-pair-recovery-care"},
		PayloadHash: payloadHash[:], VisitorPlayerID: 41001, OwnerPlayerID: 42002, TargetFarmID: 42002,
		Scope: friend.ActionScopeCare, TargetBindingHash: targetHash[:], PermitTTLMs: 60_000,
	})
	if err != nil || care.Outcome.Status != friend.StatusSucceeded || care.DispatchOrderID == "" {
		t.Fatalf("admit care: outcome=%+v err=%v", care, err)
	}

	targetDecodeCalls := 0
	dispatch, err := NewFriendPairDispatchRepository(FriendPairDispatchRepositoryConfig{
		Repository: fixture.repositories,
		TargetDecode: func(context.Context, friend.PairRecoverySourceOrder, friend.ActionPermitSnapshot) (FriendPairTheftTarget, error) {
			targetDecodeCalls++
			return FriendPairTheftTarget{PlotID: 3, CropCycleID: fixture.cycleID}, nil
		},
		MaxBuckets: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err := dispatch.Discover(ctx, DiscoveryRequest{Limit: 16})
	if err != nil {
		t.Fatal(err)
	}
	var careCandidates []DispatchCandidate
	for _, candidate := range page.Candidates {
		scope, scopeErr := dispatch.ActionScope(ctx, candidate)
		if scopeErr != nil {
			t.Fatalf("classify %q: %v", candidate.Member, scopeErr)
		}
		if scope == friend.ActionScopeCare {
			careCandidates = append(careCandidates, candidate)
		}
	}
	if len(careCandidates) != 2 {
		t.Fatalf("CARE candidates=%+v, want dispatch and result", careCandidates)
	}

	type persisted struct {
		fields map[string]string
		score  float64
	}
	before := make(map[string]persisted, len(careCandidates))
	for _, candidate := range careCandidates {
		ref, _ := workidentity.DecodeWorkRefV1(candidate.Member)
		key, keyErr := fixture.repositories.PairSourceOrderKey(candidate.BucketID, ref.WorkType, ref.WorkID)
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		fields, readErr := fixture.client.HGetAll(ctx, key).Result()
		if readErr != nil {
			t.Fatal(readErr)
		}
		due, _ := fixture.repositories.PairDueIndexKey(candidate.BucketID)
		score, scoreErr := fixture.client.ZScore(ctx, due, candidate.Member).Result()
		if scoreErr != nil {
			t.Fatal(scoreErr)
		}
		before[candidate.Member] = persisted{fields: fields, score: score}
	}

	leases, err := NewFriendPairWorkLeaseRepository(fixture.repositories, 10*time.Millisecond, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	resultSpy := &resultProcessorSpy{}
	worker := newTestWorker(t, dispatch, leases, &fakeAdvancer{createResult: appliedAdvanceResult(t, "scope-routed-theft")}, time.UnixMilli(1_800_000_100_000))
	worker.config.ResultProcessor = resultSpy
	stats, err := worker.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.NotOwned != 2 || stats.InvalidOrders != 0 || stats.Claimed != 1 || stats.Completed != 1 ||
		stats.Errors != 0 || resultSpy.calls != 1 || targetDecodeCalls != 1 {
		t.Fatalf("stats=%+v result_calls=%d target_decode_calls=%d", stats, resultSpy.calls, targetDecodeCalls)
	}

	for _, candidate := range careCandidates {
		ref, _ := workidentity.DecodeWorkRefV1(candidate.Member)
		key, _ := fixture.repositories.PairSourceOrderKey(candidate.BucketID, ref.WorkType, ref.WorkID)
		fields, readErr := fixture.client.HGetAll(ctx, key).Result()
		if readErr != nil {
			t.Fatal(readErr)
		}
		due, _ := fixture.repositories.PairDueIndexKey(candidate.BucketID)
		score, scoreErr := fixture.client.ZScore(ctx, due, candidate.Member).Result()
		if scoreErr != nil {
			t.Fatal(scoreErr)
		}
		want := before[candidate.Member]
		if !reflect.DeepEqual(fields, want.fields) || score != want.score {
			t.Fatalf("CARE order %q mutated by Theft worker\nbefore=%+v score=%v\nafter=%+v score=%v", candidate.Member, want.fields, want.score, fields, score)
		}
	}

	// A fresh worker boot sees the same non-owned CARE work without turning it
	// into InvalidOrder or touching its retry/lease tuple.
	restarted := newTestWorkerWithIdentity(t, dispatch, leases, &fakeAdvancer{}, time.UnixMilli(1_800_000_100_000), "friend-worker-2", "friend-boot-2", "friend-lease-2")
	restarted.config.ResultProcessor = resultSpy
	restartStats, err := restarted.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if restartStats.NotOwned != 2 || restartStats.InvalidOrders != 0 || restartStats.Claimed != 0 {
		t.Fatalf("restart stats=%+v", restartStats)
	}
}

func TestFriendPairRecoveryAfterPermitExpiryUsesFrozenWorkflowDeadline(t *testing.T) {
	fixture := newFriendPairRecoveryFixture(t)
	restartNow := time.UnixMilli(1_800_000_100_000).Add(61 * time.Second)
	fixture.server.SetTime(restartNow)
	dispatch, err := NewFriendPairDispatchRepository(FriendPairDispatchRepositoryConfig{
		Repository: fixture.repositories,
		TargetDecode: func(context.Context, friend.PairRecoverySourceOrder, friend.ActionPermitSnapshot) (FriendPairTheftTarget, error) {
			return FriendPairTheftTarget{PlotID: 3, CropCycleID: fixture.cycleID}, nil
		},
		MaxBuckets: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	order, err := dispatch.LoadByID(context.Background(), fixture.orderID)
	if err != nil {
		t.Fatal(err)
	}
	order, err = NormalizeDispatchOrder(order)
	if err != nil {
		t.Fatal(err)
	}
	if restartNow.UnixMilli() <= order.Permit.ExpireAtAuthorityMs || order.DeadlineAtMs <= restartNow.UnixMilli() ||
		order.DeadlineAtMs <= order.Permit.ExpireAtAuthorityMs {
		t.Fatalf("permit/deadline not independent now=%d permit=%d workflow=%d", restartNow.UnixMilli(),
			order.Permit.ExpireAtAuthorityMs, order.DeadlineAtMs)
	}
	leases, err := NewFriendPairWorkLeaseRepository(fixture.repositories, 10*time.Millisecond, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// No online Dispatch was performed: a restarted worker discovers the
	// response-loss order after permit expiry and completes it under the
	// independently frozen workflow deadline.
	worker := newTestWorker(t, dispatch, leases, &fakeAdvancer{createResult: appliedAdvanceResult(t, "expired-permit-recovery")}, restartNow)
	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Completed != 1 || stats.Claimed != 1 || stats.InvalidOrders != 0 {
		t.Fatalf("restart recovery stats=%+v", stats)
	}
}

func TestFriendPairRecoveryCorruptSourceOrDueIndexFailsClosed(t *testing.T) {
	fixture := newFriendPairRecoveryFixture(t)
	ctx := context.Background()
	orderKey, err := fixture.repositories.PairSourceOrderKey(fixture.bucketID, FriendActionDispatchOrder, fixture.orderID)
	if err != nil {
		t.Fatal(err)
	}
	dueKey, err := fixture.repositories.PairDueIndexKey(fixture.bucketID)
	if err != nil {
		t.Fatal(err)
	}

	if err := fixture.client.HSet(ctx, orderKey, "unexpected", "field").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.repositories.ReadPairDispatchOrder(ctx, fixture.bucketID, FriendActionDispatchOrder, fixture.orderID); !errors.Is(err, friend.ErrFriendReconcile) {
		t.Fatalf("extra source field error=%v, want ErrFriendReconcile", err)
	}
	if err := fixture.client.HDel(ctx, orderKey, "unexpected").Err(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.client.ZAdd(ctx, dueKey, redis.Z{Score: 1, Member: mustWorkRef(t, fixture.orderID)}).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.repositories.ReadPairDispatchOrder(ctx, fixture.bucketID, FriendActionDispatchOrder, fixture.orderID); !errors.Is(err, friend.ErrFriendReconcile) {
		t.Fatalf("DueIndex drift error=%v, want ErrFriendReconcile", err)
	}
	if err := fixture.client.ZAdd(ctx, dueKey, redis.Z{Score: 0, Member: mustWorkRef(t, fixture.orderID)}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := fixture.client.HSet(ctx, orderKey, "retry_count", "-1").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.repositories.ReadPairDispatchOrder(ctx, fixture.bucketID, FriendActionDispatchOrder, fixture.orderID); !errors.Is(err, friend.ErrFriendReconcile) {
		t.Fatalf("negative mutable field error=%v, want ErrFriendReconcile", err)
	}
	if err := fixture.client.HSet(ctx, orderKey, "retry_count", "0", "dispatch_order_id", "wrong-dispatch-id").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.repositories.ReadPairDispatchOrder(ctx, fixture.bucketID, FriendActionDispatchOrder, fixture.orderID); !errors.Is(err, friend.ErrFriendReconcile) {
		t.Fatalf("dispatch identity drift error=%v, want ErrFriendReconcile", err)
	}
}

func TestFriendPairRecoveryAdaptersRejectMissingDependencies(t *testing.T) {
	if _, err := NewFriendPairDispatchRepository(FriendPairDispatchRepositoryConfig{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil dispatch adapter error=%v, want ErrInvalidConfig", err)
	}
	if _, err := NewFriendPairWorkLeaseRepository(nil, time.Millisecond, time.Second); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil lease adapter error=%v, want ErrInvalidConfig", err)
	}
}

func TestRedisPairRecoveryLoaderRejectsForeignWorkType(t *testing.T) {
	fixture := newFriendPairRecoveryFixture(t)
	foreignID, err := workidentity.StableOpaqueID("friend-pair-recovery-foreign", "care")
	if err != nil {
		t.Fatal(err)
	}
	foreignRef, err := workidentity.EncodeWorkRefV1("CARE_ORDER", foreignID)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	discovery, err := NewRedisPairDispatchRepository(RedisPairDiscoveryConfig{
		Client: fixture.client, BucketCount: fixture.repositories.PairBucketCount(), MaxBuckets: 1,
		DueIndexKey: fixture.repositories.PairDueIndexKey,
		LoadDispatch: func(context.Context, uint32, string, string) (DispatchOrder, error) {
			called = true
			return DispatchOrder{}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := discovery.LoadDispatch(context.Background(), DispatchCandidate{BucketID: fixture.bucketID, Member: foreignRef}); !errors.Is(err, ErrNotTheft) {
		t.Fatalf("foreign loader error=%v, want ErrNotTheft", err)
	}
	if called {
		t.Fatal("foreign work type reached the dispatch loader")
	}
}

func mustWorkRef(t *testing.T, workID string) string {
	t.Helper()
	ref, err := workidentity.EncodeWorkRefV1(FriendActionDispatchOrder, workID)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}
