package theftrecovery

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"farmgame/internal/friend"
	"farmgame/internal/workidentity"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestTheftRecoveryDispatchResponseLossUsesReceiptFirstAndCompletesOneWork(t *testing.T) {
	rig := newTheftRedisRig(t)
	order := testDispatchOrder(t, rig.now)
	rig.seed(order)

	pair := &fakePairRepository{candidate: DispatchCandidate{BucketID: order.Work.BucketID, Member: order.WorkRef, ScoreMs: rig.now.UnixMilli()}, order: order}
	advancer := &fakeAdvancer{createResult: AdvanceResult{}, createErr: errors.New("dispatch response lost"), receiptResult: appliedAdvanceResult(t, "loss-receipt")}
	worker := newTestWorker(t, pair, rig.leases, advancer, rig.now)

	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Claimed != 1 || stats.Completed != 1 || stats.Retried != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	if advancer.createCalls() != 1 || advancer.receiptCalls() != 1 {
		t.Fatalf("advancer calls create=%d receipt=%d", advancer.createCalls(), advancer.receiptCalls())
	}
	state := rig.readState(order)
	if state != "COMPLETED" {
		t.Fatalf("state=%q, want COMPLETED", state)
	}
}

func TestTheftRecoverySameOrderConcurrentClaimHasOneWinner(t *testing.T) {
	rig := newTheftRedisRig(t)
	order := testDispatchOrder(t, rig.now)
	rig.seed(order)
	pair := &fakePairRepository{candidate: DispatchCandidate{BucketID: order.Work.BucketID, Member: order.WorkRef, ScoreMs: rig.now.UnixMilli()}, order: order}
	advancer := &fakeAdvancer{createResult: appliedAdvanceResult(t, "concurrent-result"), blockCreate: make(chan struct{}), createStarted: make(chan struct{})}
	first := newTestWorker(t, pair, rig.leases, advancer, rig.now)
	second := newTestWorkerWithIdentity(t, pair, rig.leases, advancer, rig.now, "worker-2", "boot-2", "lease-2")

	firstDone := make(chan error, 1)
	go func() {
		_, runErr := first.RunOnce(context.Background())
		firstDone <- runErr
	}()
	select {
	case <-advancer.createStarted:
	case <-time.After(time.Second):
		t.Fatal("first worker did not reach advancer")
	}
	secondStats, err := second.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if secondStats.Claimed != 0 || secondStats.LeaseHeld != 1 {
		t.Fatalf("second stats=%+v", secondStats)
	}
	close(advancer.blockCreate)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if advancer.createCalls() != 1 {
		t.Fatalf("CreateIfAbsent calls=%d, want 1", advancer.createCalls())
	}
}

func TestTheftRecoveryRestartPreservesRetryCountAndReclaimsSameWork(t *testing.T) {
	rig := newTheftRedisRig(t)
	order := testDispatchOrder(t, rig.now)
	rig.seed(order)
	pair := &fakePairRepository{candidate: DispatchCandidate{BucketID: order.Work.BucketID, Member: order.WorkRef, ScoreMs: rig.now.UnixMilli()}, order: order}
	advancer := &fakeAdvancer{createErr: errors.New("participant unavailable")}
	first := newTestWorker(t, pair, rig.leases, advancer, rig.now)

	stats, err := first.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Retried != 1 || stats.Reconciled != 0 {
		t.Fatalf("first stats=%+v", stats)
	}
	retry, nextDue, err := rig.readRetry(order)
	if err != nil {
		t.Fatal(err)
	}
	if retry != 1 || nextDue <= rig.now.UnixMilli() {
		t.Fatalf("persisted retry=%d next_due=%d", retry, nextDue)
	}

	rig.setTime(time.UnixMilli(nextDue + 1))
	second := newTestWorkerWithIdentity(t, pair, rig.leases, advancer, rig.now, "worker-2", "boot-2", "lease-2")
	stats, err = second.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Claimed != 1 || stats.Retried != 1 {
		t.Fatalf("second stats=%+v", stats)
	}
	retry, _, err = rig.readRetry(order)
	if err != nil {
		t.Fatal(err)
	}
	if retry != 2 {
		t.Fatalf("retry reset or skipped after restart: %d", retry)
	}
}

func TestTheftRecoveryOldLeaseCannotCompleteAfterTakeover(t *testing.T) {
	rig := newTheftRedisRig(t)
	order := testDispatchOrder(t, rig.now)
	rig.seed(order)

	first, err := rig.leases.Claim(context.Background(), ClaimRequest{Order: order, HolderID: "worker-1", BootID: "boot-1", LeaseToken: "lease-1", RequestedTTL: time.Second})
	if err != nil || first.Status != LeaseClaimed {
		t.Fatalf("first claim=%+v err=%v", first, err)
	}
	rig.setTime(rig.now.Add(2 * time.Second))
	second, err := rig.leases.Claim(context.Background(), ClaimRequest{Order: order, HolderID: "worker-2", BootID: "boot-2", LeaseToken: "lease-2", RequestedTTL: time.Second})
	if err != nil || second.Status != LeaseClaimed || second.LeaseEpoch <= first.LeaseEpoch {
		t.Fatalf("takeover=%+v err=%v first=%+v", second, err, first)
	}
	result := appliedAdvanceResult(t, "old-lease-result")
	complete, err := rig.leases.Complete(context.Background(), CompleteRequest{Order: order,
		Fence: LeaseFence{HolderID: "worker-1", BootID: "boot-1", LeaseToken: "lease-1", LeaseEpoch: first.LeaseEpoch, WorkVersion: first.WorkVersion},
		Proof: CompletionProof{PayloadHash: order.PayloadHash, SourceProofHash: SourceProofHash(order), ResultProofHash: result.ResultProofHash}})
	if err != nil {
		t.Fatal(err)
	}
	if complete.Status != LeaseCompleteFenceMismatch {
		t.Fatalf("old lease complete=%v, want fence mismatch", complete.Status)
	}
}

func TestPairDecideRecoveryLeaseVerificationUsesExactFenceExpiryAndDueIndex(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*theftRedisRig, DispatchOrder, *LeaseFence, LeaseClaim)
		valid  bool
	}{
		{name: "valid", valid: true},
		{name: "holder", mutate: func(_ *theftRedisRig, _ DispatchOrder, fence *LeaseFence, _ LeaseClaim) { fence.HolderID = "other" }},
		{name: "boot", mutate: func(_ *theftRedisRig, _ DispatchOrder, fence *LeaseFence, _ LeaseClaim) { fence.BootID = "other" }},
		{name: "token", mutate: func(_ *theftRedisRig, _ DispatchOrder, fence *LeaseFence, _ LeaseClaim) { fence.LeaseToken = "other" }},
		{name: "epoch", mutate: func(_ *theftRedisRig, _ DispatchOrder, fence *LeaseFence, _ LeaseClaim) { fence.LeaseEpoch++ }},
		{name: "work version", mutate: func(_ *theftRedisRig, _ DispatchOrder, fence *LeaseFence, _ LeaseClaim) { fence.WorkVersion++ }},
		{name: "expired", mutate: func(rig *theftRedisRig, _ DispatchOrder, _ *LeaseFence, claim LeaseClaim) {
			rig.setTime(time.UnixMilli(claim.LeaseExpireAtMs))
		}},
		{name: "DueIndex missing", mutate: func(rig *theftRedisRig, order DispatchOrder, _ *LeaseFence, _ LeaseClaim) {
			if err := rig.client.ZRem(context.Background(), rig.dueKey(order), order.WorkRef).Err(); err != nil {
				rig.t.Fatal(err)
			}
		}},
		{name: "DueIndex score", mutate: func(rig *theftRedisRig, order DispatchOrder, _ *LeaseFence, claim LeaseClaim) {
			if err := rig.client.ZAdd(context.Background(), rig.dueKey(order), redis.Z{Score: float64(claim.LeaseExpireAtMs + 1), Member: order.WorkRef}).Err(); err != nil {
				rig.t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newTheftRedisRig(t)
			order := testDispatchOrder(t, rig.now)
			rig.seed(order)
			claim, err := rig.leases.Claim(context.Background(), ClaimRequest{Order: order, HolderID: "worker-1", BootID: "boot-1", LeaseToken: "lease-1", RequestedTTL: time.Second})
			if err != nil || claim.Status != LeaseClaimed {
				t.Fatalf("claim=%+v err=%v", claim, err)
			}
			order.WorkVersion = claim.WorkVersion
			fence := LeaseFence{HolderID: "worker-1", BootID: "boot-1", LeaseToken: "lease-1", LeaseEpoch: claim.LeaseEpoch, WorkVersion: claim.WorkVersion}
			if tc.mutate != nil {
				tc.mutate(rig, order, &fence, claim)
			}
			proof, err := rig.leases.VerifyRecoveryLease(context.Background(), VerifyRecoveryLeaseRequest{Order: order, Fence: fence})
			if tc.valid {
				if err != nil || proof.WorkVersion != claim.WorkVersion || proof.LeaseEpoch != claim.LeaseEpoch ||
					proof.LeaseExpireAtMs != claim.LeaseExpireAtMs || proof.DueIndexScoreMs != claim.LeaseExpireAtMs || proof.TrustedNowMs >= proof.LeaseExpireAtMs {
					t.Fatalf("proof=%+v err=%v claim=%+v", proof, err, claim)
				}
			} else if !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("mutation admitted: proof=%+v err=%v", proof, err)
			}
		})
	}
}

func TestRedisWorkLeaseRejectsInvalidFenceAndRescheduleBoundsBeforeMutation(t *testing.T) {
	rig := newTheftRedisRig(t)
	order := testDispatchOrder(t, rig.now)
	rig.seed(order)
	if _, err := rig.leases.Claim(context.Background(), ClaimRequest{Order: order, HolderID: "", BootID: "boot-1", LeaseToken: "lease-1", RequestedTTL: time.Second}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty claim identity error=%v, want ErrInvalidConfig", err)
	}
	if leaseEpoch := rig.client.HGet(context.Background(), rig.workKey(order), "lease_epoch").Val(); leaseEpoch != "0" {
		t.Fatalf("empty claim mutated lease epoch=%q", leaseEpoch)
	}
	claim, err := rig.leases.Claim(context.Background(), ClaimRequest{Order: order, HolderID: "worker-1", BootID: "boot-1", LeaseToken: "lease-1", RequestedTTL: time.Second})
	if err != nil || claim.Status != LeaseClaimed {
		t.Fatalf("valid claim=%+v err=%v", claim, err)
	}
	proof := SourceProofHash(order)
	if _, err := rig.leases.Reschedule(context.Background(), RescheduleRequest{Order: order,
		Fence:      LeaseFence{HolderID: "worker-1", BootID: "boot-1", LeaseToken: "lease-1", LeaseEpoch: claim.LeaseEpoch, WorkVersion: claim.WorkVersion},
		RetryCount: 1, DeadlineAtMs: order.DeadlineAtMs, NextDueAtMs: order.DeadlineAtMs + 1,
		ErrorClass: "INVALID", Proof: CompletionProof{PayloadHash: order.PayloadHash, SourceProofHash: proof}}); !errors.Is(err, ErrInvalidDispatch) {
		t.Fatalf("invalid reschedule bounds error=%v, want ErrInvalidDispatch", err)
	}
	state := rig.readState(order)
	if state != "PENDING" {
		t.Fatalf("invalid reschedule changed state=%q", state)
	}
}

func TestTheftRecoveryForgedClaimProofStopsBeforeAdvancer(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	order := testDispatchOrder(t, now)
	forged := sha256.Sum256([]byte("forged-source-proof"))
	leases := &forgedClaimLease{claim: LeaseClaim{Status: LeaseClaimed, WorkVersion: 1,
		DeadlineAtMs: order.DeadlineAtMs, NextDueAtMs: order.NextDueAtMs, HolderID: "worker-1", BootID: "boot-1",
		LeaseEpoch: 1, AcceptedTTL: time.Second, PayloadHash: append([]byte(nil), order.PayloadHash...), SourceProofHash: forged[:]}}
	advancer := &fakeAdvancer{createResult: appliedAdvanceResult(t, "must-not-advance")}
	worker := newTestWorker(t, &fakePairRepository{candidate: DispatchCandidate{BucketID: order.Work.BucketID, Member: order.WorkRef}, order: order}, leases, advancer, now)
	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Claimed != 1 || stats.LeaseLost != 1 || advancer.createCalls() != 0 || leases.renewCalls != 0 {
		t.Fatalf("stats=%+v create=%d renew=%d", stats, advancer.createCalls(), leases.renewCalls)
	}
}

func TestTheftRecoveryRetryExhaustionPersistsReconcileRequired(t *testing.T) {
	rig := newTheftRedisRig(t)
	order := testDispatchOrder(t, rig.now)
	rig.seed(order)
	pair := &fakePairRepository{candidate: DispatchCandidate{BucketID: order.Work.BucketID, Member: order.WorkRef, ScoreMs: rig.now.UnixMilli()}, order: order}
	advancer := &fakeAdvancer{createErr: errors.New("permanent participant failure")}
	worker := newTestWorkerWithConfig(t, pair, rig.leases, advancer, Config{
		HolderID: "worker-1", BootID: "boot-1", LeaseToken: "lease-1", LeaseTTL: time.Second,
		OperationTimeout: time.Second, MaxAttempts: 1, Backoff: Backoff{Base: time.Millisecond, Max: time.Millisecond}, Now: func() time.Time { return rig.now },
	})
	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Reconciled != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	if state := rig.readState(order); state != ReconcileRequiredState {
		t.Fatalf("state=%q, want %s", state, ReconcileRequiredState)
	}
	retry, _, err := rig.readRetry(order)
	if err != nil {
		t.Fatal(err)
	}
	if retry != 1 {
		t.Fatalf("terminal retry=%d, want 1", retry)
	}
}

func TestTheftRecoveryMalformedAndForeignMembersAreBoundedAndUntouched(t *testing.T) {
	rig := newTheftRedisRig(t)
	order := testDispatchOrder(t, rig.now)
	rig.seed(order)
	otherID, err := workidentity.StableOpaqueID("foreign-order", "1")
	if err != nil {
		t.Fatal(err)
	}
	otherRef, err := workidentity.EncodeWorkRefV1("CARE_ORDER", otherID)
	if err != nil {
		t.Fatal(err)
	}
	badPrefix := "0" + order.WorkRef
	pair := &fakePairRepository{page: DiscoveryPage{Candidates: []DispatchCandidate{
		{BucketID: order.Work.BucketID, Member: badPrefix},
		{BucketID: order.Work.BucketID, Member: otherRef},
		{BucketID: order.Work.BucketID, Member: order.WorkRef},
	}, Exhausted: true}, order: order}
	advancer := &fakeAdvancer{createResult: appliedAdvanceResult(t, "bounded-result")}
	worker := newTestWorker(t, pair, rig.leases, advancer, rig.now)
	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.MalformedMembers != 1 || stats.NonTheft != 1 || stats.Completed != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	if _, err := rig.client.ZScore(context.Background(), rig.dueKey(order), badPrefix).Result(); !errors.Is(err, redis.Nil) {
		// The malformed member was supplied by the fake repository rather than
		// Redis; this branch documents that the worker has no delete path.
		if err != nil {
			t.Fatalf("unexpected malformed member lookup error: %v", err)
		}
	}
}

func TestTheftRecoveryRejectsPayloadHashForgeryAndAddressDrift(t *testing.T) {
	rig := newTheftRedisRig(t)
	order := testDispatchOrder(t, rig.now)
	for _, mutate := range []func(*DispatchOrder){
		func(value *DispatchOrder) { value.Payload[0] ^= 1 },
		func(value *DispatchOrder) { value.CanonicalPayloadHash[0] ^= 1 },
		func(value *DispatchOrder) { value.Work.CanonicalSourceAuthority = "forged" },
	} {
		candidate := order
		candidate.Payload = append([]byte(nil), order.Payload...)
		candidate.CanonicalPayloadHash = append([]byte(nil), order.CanonicalPayloadHash...)
		mutate(&candidate)
		pair := &fakePairRepository{candidate: DispatchCandidate{BucketID: order.Work.BucketID, Member: order.WorkRef}, order: candidate}
		worker := newTestWorker(t, pair, rig.leases, &fakeAdvancer{createResult: appliedAdvanceResult(t, "unused")}, rig.now)
		stats, err := worker.RunOnce(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if stats.InvalidOrders != 1 || stats.Claimed != 0 {
			t.Fatalf("forged order stats=%+v", stats)
		}
	}
}

func TestRedisPairDiscoveryHeadPagingDoesNotSkipAfterScoreMoves(t *testing.T) {
	rig := newTheftRedisRig(t)
	order := testDispatchOrder(t, rig.now)
	key := rig.dueKey(order)
	refs := make([]string, 0, 4)
	for _, suffix := range []string{"a", "b", "c", "d"} {
		id, err := workidentity.StableOpaqueID("theft-discovery-page", suffix)
		if err != nil {
			t.Fatal(err)
		}
		ref, err := workidentity.EncodeWorkRefV1(FriendActionDispatchOrder, id)
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	entries := make([]redis.Z, 0, len(refs))
	for index, ref := range refs {
		entries = append(entries, redis.Z{Score: float64(rig.now.UnixMilli() - int64(10-index)), Member: ref})
	}
	if err := rig.client.ZAdd(context.Background(), key, entries...).Err(); err != nil {
		t.Fatal(err)
	}
	discovery, err := NewRedisPairDispatchRepository(RedisPairDiscoveryConfig{
		Client: rig.client, BucketCount: order.Work.BucketID + 1, MaxBuckets: 8,
		DueIndexKey:  func(bucketID uint32) (string, error) { return fmt.Sprintf("{theft:%d}:due", bucketID), nil },
		LoadDispatch: func(context.Context, uint32, string, string) (DispatchOrder, error) { return order, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := discovery.Discover(context.Background(), DiscoveryRequest{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Candidates) != 2 || first.Candidates[0].Member != refs[0] || first.Candidates[1].Member != refs[1] ||
		first.Next.HeadFingerprint == "" {
		t.Fatalf("first page=%+v", first)
	}
	// Claim/reschedule changes the scores of the first page. A rank offset of
	// two would now skip C/D; the head cursor must expose them.
	if err := rig.client.ZAdd(context.Background(), key,
		redis.Z{Score: float64(rig.now.Add(time.Minute).UnixMilli()), Member: refs[0]},
		redis.Z{Score: float64(rig.now.Add(time.Minute).UnixMilli()), Member: refs[1]}).Err(); err != nil {
		t.Fatal(err)
	}
	second, err := discovery.Discover(context.Background(), DiscoveryRequest{Limit: 2, Cursor: first.Next})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Candidates) != 2 || second.Candidates[0].Member != refs[2] || second.Candidates[1].Member != refs[3] {
		t.Fatalf("second page=%+v", second)
	}
}

func TestRedisPairDiscoveryBucketCursorReachesBeyond256(t *testing.T) {
	rig := newTheftRedisRig(t)
	const targetBucket = uint32(300)
	targetKey := fmt.Sprintf("{theft:%d}:due", targetBucket)
	id, err := workidentity.StableOpaqueID("theft-discovery-high-bucket", "one")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := workidentity.EncodeWorkRefV1(FriendActionDispatchOrder, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.client.ZAdd(context.Background(), targetKey,
		redis.Z{Score: float64(rig.now.UnixMilli()), Member: ref}).Err(); err != nil {
		t.Fatal(err)
	}
	discovery, err := NewRedisPairDispatchRepository(RedisPairDiscoveryConfig{
		Client: rig.client, BucketCount: 512, MaxBuckets: 64,
		DueIndexKey:  func(bucketID uint32) (string, error) { return fmt.Sprintf("{theft:%d}:due", bucketID), nil },
		LoadDispatch: func(context.Context, uint32, string, string) (DispatchOrder, error) { return DispatchOrder{}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	cursor := DiscoveryCursor{}
	found := false
	for round := 0; round < 8 && !found; round++ {
		page, err := discovery.Discover(context.Background(), DiscoveryRequest{Limit: 8, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, candidate := range page.Candidates {
			found = found || candidate.BucketID == targetBucket && candidate.Member == ref
		}
		cursor = page.Next
		if page.Exhausted {
			cursor = DiscoveryCursor{}
		}
	}
	if !found {
		t.Fatal("bucket cursor never reached a due member above bucket 256")
	}
}

func TestRedisPairDiscoveryEmptyRoundHonorsBucketQueryBudget(t *testing.T) {
	rig := newTheftRedisRig(t)
	discovery, err := NewRedisPairDispatchRepository(RedisPairDiscoveryConfig{
		Client: rig.client, BucketCount: 4096, MaxBuckets: 32,
		DueIndexKey:  func(bucketID uint32) (string, error) { return fmt.Sprintf("{theft:%d}:due", bucketID), nil },
		LoadDispatch: func(context.Context, uint32, string, string) (DispatchOrder, error) { return DispatchOrder{}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	before := rig.server.CommandCount()
	page, err := discovery.Discover(context.Background(), DiscoveryRequest{Limit: 8})
	if err != nil {
		t.Fatal(err)
	}
	queries := rig.server.CommandCount() - before
	if len(page.Candidates) != 0 || page.Exhausted || page.Next.Token != "bucket:32" || queries > 33 {
		t.Fatalf("empty page=%+v Redis queries=%d, want at most TIME + 32 buckets", page, queries)
	}
}

type fakePairRepository struct {
	mu        sync.Mutex
	page      DiscoveryPage
	candidate DispatchCandidate
	order     DispatchOrder
	scope     friend.ActionScope
	scopeErr  error
	loadErr   error
}

func (p *fakePairRepository) Discover(context.Context, DiscoveryRequest) (DiscoveryPage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.page.Candidates != nil {
		return p.page, nil
	}
	return DiscoveryPage{Candidates: []DispatchCandidate{p.candidate}, Exhausted: true}, nil
}

func (p *fakePairRepository) ActionScope(context.Context, DispatchCandidate) (friend.ActionScope, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.scopeErr != nil {
		return friend.ActionScopeUnspecified, p.scopeErr
	}
	if p.scope == friend.ActionScopeUnspecified {
		return friend.ActionScopeTheft, nil
	}
	return p.scope, nil
}

func (p *fakePairRepository) LoadDispatch(context.Context, DispatchCandidate) (DispatchOrder, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.loadErr != nil {
		return DispatchOrder{}, p.loadErr
	}
	return p.order, nil
}

type fakeAdvancer struct {
	mu            sync.Mutex
	create        int
	receipt       int
	createResult  AdvanceResult
	createErr     error
	receiptResult AdvanceResult
	blockCreate   chan struct{}
	createStarted chan struct{}
}

type forgedClaimLease struct {
	claim      LeaseClaim
	renewCalls int
}

func (l *forgedClaimLease) Claim(context.Context, ClaimRequest) (LeaseClaim, error) {
	return l.claim, nil
}

func (l *forgedClaimLease) Renew(context.Context, RenewRequest) (LeaseRenewResult, error) {
	l.renewCalls++
	return LeaseRenewResult{Status: LeaseRenewed}, nil
}

func (*forgedClaimLease) Complete(context.Context, CompleteRequest) (LeaseCompleteResult, error) {
	return LeaseCompleteResult{}, nil
}

func (*forgedClaimLease) Reschedule(context.Context, RescheduleRequest) (LeaseRescheduleResult, error) {
	return LeaseRescheduleResult{}, nil
}

func (a *fakeAdvancer) CreateIfAbsentOrContinue(ctx context.Context, _ AdvanceRequest) (AdvanceResult, error) {
	a.mu.Lock()
	a.create++
	if a.createStarted == nil {
		a.createStarted = make(chan struct{})
	}
	if a.create == 1 {
		close(a.createStarted)
	}
	block := a.blockCreate
	result, runErr := a.createResult, a.createErr
	a.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return AdvanceResult{}, ctx.Err()
		}
	}
	return result, runErr
}

func (a *fakeAdvancer) ReceiptFirst(context.Context, ReceiptRequest) (AdvanceResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.receipt++
	return a.receiptResult, nil
}

func (a *fakeAdvancer) createCalls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.create
}

func (a *fakeAdvancer) receiptCalls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.receipt
}

type theftRedisRig struct {
	t      *testing.T
	server *miniredis.Miniredis
	client *redis.Client
	leases *RedisWorkLeaseRepository
	now    time.Time
}

func newTheftRedisRig(t *testing.T) *theftRedisRig {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	now := time.UnixMilli(1_800_000_000_000)
	server.SetTime(now)
	rig := &theftRedisRig{t: t, server: server, client: client, now: now}
	keys := WorkLeaseKeyFuncs{
		Work: func(order WorkAddress) (string, error) {
			return fmt.Sprintf("{theft:%d}:work:%s", order.BucketID, order.WorkID), nil
		},
		Due:        func(order WorkAddress) (string, error) { return fmt.Sprintf("{theft:%d}:due", order.BucketID), nil },
		BucketMeta: func(order WorkAddress) (string, error) { return fmt.Sprintf("{theft:%d}:meta", order.BucketID), nil },
		Recovery: func(order WorkAddress) (string, error) {
			return fmt.Sprintf("{theft:%d}:recovery:%s", order.BucketID, order.WorkID), nil
		},
	}
	lease, err := NewRedisWorkLeaseRepository(RedisWorkLeaseConfig{Client: client, Keys: keys, MinTTL: 10 * time.Millisecond, MaxTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	rig.leases = lease
	t.Cleanup(func() { _ = client.Close() })
	return rig
}

func (r *theftRedisRig) setTime(now time.Time) {
	r.now = now
	r.server.SetTime(now)
}

func (r *theftRedisRig) workKey(order DispatchOrder) string {
	return fmt.Sprintf("{theft:%d}:work:%s", order.Work.BucketID, order.Work.WorkID)
}

func (r *theftRedisRig) dueKey(order DispatchOrder) string {
	return fmt.Sprintf("{theft:%d}:due", order.Work.BucketID)
}

func (r *theftRedisRig) recoveryKey(order DispatchOrder) string {
	return fmt.Sprintf("{theft:%d}:recovery:%s", order.Work.BucketID, order.Work.WorkID)
}

func (r *theftRedisRig) seed(order DispatchOrder) {
	r.t.Helper()
	err := r.client.HSet(context.Background(), r.workKey(order), map[string]interface{}{
		"work_id": order.Work.WorkID, "work_type": order.Work.WorkType,
		"placement_kind": order.Work.PlacementKind, "bucket_version": order.Work.BucketVersion,
		"bucket_id": fmt.Sprintf("%d", order.Work.BucketID), "source_authority_kind": order.Work.SourceAuthorityKind,
		"canonical_source_authority_id": order.Work.CanonicalSourceAuthority, "state": "PENDING",
		"work_version": "1", "typed_payload": string(order.Payload), "work_ref": order.WorkRef,
		"payload_hash": HexHash(order.PayloadHash), "next_due_at_ms": order.NextDueAtMs,
		"retry_count": order.RetryCount, "lease_epoch": "0", "lease_holder_id": "",
		"lease_token_hash": "", "lease_expire_at_ms": "0", "created_at_ms": r.now.UnixMilli(),
		"updated_at_ms": r.now.UnixMilli(), "completed_at_ms": "0",
	}).Err()
	if err != nil {
		r.t.Fatal(err)
	}
	if err := r.client.HSet(context.Background(), fmt.Sprintf("{theft:%d}:meta", order.Work.BucketID),
		"pending_work_count", 1, "pending_work_bytes", pairSourceOrderChargedBytes(order)).Err(); err != nil {
		r.t.Fatal(err)
	}
	if err := r.client.ZAdd(context.Background(), r.dueKey(order), redis.Z{Score: float64(order.NextDueAtMs), Member: order.WorkRef}).Err(); err != nil {
		r.t.Fatal(err)
	}
}

func (r *theftRedisRig) readState(order DispatchOrder) string {
	state, err := r.client.HGet(context.Background(), r.workKey(order), "state").Result()
	if err != nil {
		r.t.Fatal(err)
	}
	return state
}

func (r *theftRedisRig) readRetry(order DispatchOrder) (uint64, int64, error) {
	values, err := r.client.HMGet(context.Background(), r.workKey(order), "retry_count", "next_due_at_ms").Result()
	if err != nil {
		return 0, 0, err
	}
	retry, err := strconv.ParseUint(fmt.Sprint(values[0]), 10, 64)
	if err != nil {
		return 0, 0, err
	}
	due, err := strconv.ParseInt(fmt.Sprint(values[1]), 10, 64)
	return retry, due, err
}

func testDispatchOrder(t *testing.T, now time.Time) DispatchOrder {
	t.Helper()
	pairID, err := friend.RelationIDFor(10001, 20002)
	if err != nil {
		t.Fatal(err)
	}
	operationID := "theft-order-1"
	workID, err := workidentity.StableOpaqueID("theft-dispatch-test", operationID)
	if err != nil {
		t.Fatal(err)
	}
	cycleID, err := workidentity.StableOpaqueID("theft-cycle-test", operationID)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("TLP1test-dispatch-payload")
	payloadHash := sha256.Sum256(payload)
	canonical := sha256.Sum256([]byte("canonical-theft-payload"))
	binding := sha256.Sum256([]byte("farm=20002/plot=3/cycle=" + cycleID))
	permitID, err := workidentity.StableOpaqueID("permit-test", operationID)
	if err != nil {
		t.Fatal(err)
	}
	permit := friend.ActionPermitSnapshot{PermitID: permitID, RelationID: pairID, RelationEpoch: 7,
		VisitorPlayerID: 10001, OwnerPlayerID: 20002, TargetFarmID: 20002, OperationID: operationID,
		Scope: friend.ActionScopeTheft, PayloadHash: canonical[:], TargetBindingHash: binding[:],
		IssuedAtAuthorityMs: now.UnixMilli(), ExpireAtAuthorityMs: now.Add(time.Hour).UnixMilli(),
		ExecutionAcceptedAtAuthorityMs: now.Add(time.Millisecond).UnixMilli()}
	order := DispatchOrder{Work: WorkAddress{WorkID: workID, WorkType: FriendActionDispatchOrder,
		PlacementKind: SourcePersistenceBucket, BucketVersion: "friend-auth-v1", BucketID: 7,
		SourceAuthorityKind: FriendPairAuthority, CanonicalSourceAuthority: pairID, SourceLayoutVersion: "friend-auth-v1"},
		Payload: payload, PayloadHash: payloadHash[:], CanonicalPayloadHash: canonical[:], TargetBindingHash: binding[:],
		PairID: pairID, OperationID: operationID, VisitorPlayerID: 10001, OwnerPlayerID: 20002, TargetFarmID: 20002,
		PlotID: 3, CropCycleID: cycleID, Permit: permit, Admission: AdmissionProof{ReceiptID: "admission-receipt",
			PayloadHash: canonical[:], PermitHash: PermitHash(permit), AcceptedAtMs: permit.ExecutionAcceptedAtAuthorityMs},
		NextDueAtMs: now.UnixMilli(), DeadlineAtMs: now.Add(2 * time.Hour).UnixMilli()}
	order.WorkRef, err = workidentity.EncodeWorkRefV1(order.Work.WorkType, order.Work.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := NormalizeDispatchOrder(order)
	if err != nil {
		t.Fatal(err)
	}
	return normalized
}

func appliedAdvanceResult(t *testing.T, seed string) AdvanceResult {
	t.Helper()
	hash := sha256.Sum256([]byte(seed))
	return AdvanceResult{Status: AdvanceApplied, ResultProofHash: hash[:], ReceiptID: seed}
}

func newTestWorker(t *testing.T, pair PairDispatchRepository, leases WorkLeaseRepository, advancer TheftWorkflowAdvancer, now time.Time) *Worker {
	return newTestWorkerWithConfig(t, pair, leases, advancer, Config{HolderID: "worker-1", BootID: "boot-1", LeaseToken: "lease-1",
		LeaseTTL: time.Second, OperationTimeout: time.Second, MaxAttempts: 8, Backoff: Backoff{Base: time.Millisecond, Max: time.Millisecond}, Now: func() time.Time { return now }})
}

func newTestWorkerWithIdentity(t *testing.T, pair PairDispatchRepository, leases WorkLeaseRepository, advancer TheftWorkflowAdvancer, now time.Time, holder, boot, token string) *Worker {
	return newTestWorkerWithConfig(t, pair, leases, advancer, Config{HolderID: holder, BootID: boot, LeaseToken: token,
		LeaseTTL: time.Second, OperationTimeout: time.Second, MaxAttempts: 8, Backoff: Backoff{Base: time.Millisecond, Max: time.Millisecond}, Now: func() time.Time { return now }})
}

func newTestWorkerWithConfig(t *testing.T, pair PairDispatchRepository, leases WorkLeaseRepository, advancer TheftWorkflowAdvancer, config Config) *Worker {
	config.PairRepository, config.Leases, config.Advancer = pair, leases, advancer
	worker, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return worker
}
