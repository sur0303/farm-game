package authoritystore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
)

type shardTestClock struct {
	mu  sync.Mutex
	now time.Time
}

func newShardTestClock() *shardTestClock {
	return &shardTestClock{now: time.UnixMilli(100_000)}
}

func (c *shardTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *shardTestClock) Advance(delta time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(delta)
	c.mu.Unlock()
}

func (c *shardTestClock) Set(next time.Time) {
	c.mu.Lock()
	c.now = next
	c.mu.Unlock()
}

func TestShardWriteAuthorityClaimRenewAndExpiredTakeover(t *testing.T) {
	store, clock := newShardAuthorityTestStore(t)
	claim := claimRequest("claim-a", "holder-a", "boot-a")

	first, err := store.ClaimShardAuthority(context.Background(), claim)
	if err != nil || first.Status != ShardTransitionApplied {
		t.Fatalf("claim=%+v err=%v", first, err)
	}
	if first.Authority.WriterEpoch != 1 || first.Authority.LeaseEpoch != 1 || first.Authority.AssignmentRevision != 1 || first.AcceptedTTLms != 1_000 {
		t.Fatalf("first authority=%+v", first)
	}
	if first.Authority.HashVersion != actorrouting.HashVersionXXH64V2 || first.Authority.HashSeed != testAuthorityHashSeed {
		t.Fatalf("claim did not persist route hash profile: %+v", first.Authority)
	}

	replay, err := store.ClaimShardAuthority(context.Background(), claim)
	if err != nil || replay.Status != ShardTransitionAlreadyApplied || replay.Authority.WriterEpoch != 1 {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	failoverReplay := claim
	failoverReplay.Caller.InstanceID = "route-manager-2"
	failoverReplay.Caller.BootID = "rm-boot-2"
	if got, err := store.ClaimShardAuthority(context.Background(), failoverReplay); err != nil || got.Status != ShardTransitionAlreadyApplied {
		t.Fatalf("route-manager failover replay=%+v err=%v", got, err)
	}
	conflictingReplay := claim
	conflictingReplay.NewHolderID = "holder-b"
	conflictingReplay.NewHolderBootID = "boot-b"
	if got, err := store.ClaimShardAuthority(context.Background(), conflictingReplay); err != nil || got.Status != ShardTransitionReconcileRequired {
		t.Fatalf("conflicting replay=%+v err=%v", got, err)
	}
	busy := claimRequest("claim-busy", "holder-b", "boot-b")
	if got, err := store.ClaimShardAuthority(context.Background(), busy); err != nil || got.Status != ShardTransitionActiveLeaseConflict {
		t.Fatalf("busy claim=%+v err=%v", got, err)
	}

	clock.Advance(200 * time.Millisecond)
	renew := RenewShardAuthorityRequest{
		Caller: holderCaller("holder-a", "boot-a"), AuthorityLayoutVersion: "auth-v1",
		Domain: actorrouting.DomainFarm, VirtualShard: 17,
		ExpectedFence: fenceOf(first.Authority), RequestedTTLms: 1_000,
	}
	renewed, err := store.RenewShardAuthority(context.Background(), renew)
	if err != nil || renewed.Status != ShardTransitionApplied {
		t.Fatalf("renew=%+v err=%v", renewed, err)
	}
	if renewed.Authority.WriterEpoch != 1 || renewed.Authority.LeaseEpoch != 1 || renewed.Authority.AssignmentRevision != 1 || renewed.AcceptedTTLms != 1_000 {
		t.Fatalf("renew changed generation=%+v", renewed)
	}
	wrongCaller := renew
	wrongCaller.Caller = holderCaller("holder-b", "boot-b")
	if got, err := store.RenewShardAuthority(context.Background(), wrongCaller); err != nil || got.Status != ShardTransitionUnauthorized {
		t.Fatalf("wrong caller=%+v err=%v", got, err)
	}

	clock.Set(time.UnixMilli(renewed.Authority.LeaseUntilMs))
	if got, err := store.RenewShardAuthority(context.Background(), renew); err != nil || got.Status != ShardTransitionLeaseExpired {
		t.Fatalf("expired renew=%+v err=%v", got, err)
	}
	takeover := claimRequest("claim-takeover", "holder-b", "boot-b")
	taken, err := store.ClaimShardAuthority(context.Background(), takeover)
	if err != nil || taken.Status != ShardTransitionApplied {
		t.Fatalf("takeover=%+v err=%v", taken, err)
	}
	if taken.Authority.WriterEpoch != 2 || taken.Authority.LeaseEpoch != 2 || taken.Authority.AssignmentRevision != 2 {
		t.Fatalf("takeover generation=%+v", taken.Authority)
	}
}

func TestExpiredTakeoverRequiresEndedLeaseAndExactFence(t *testing.T) {
	store, clock := newShardAuthorityTestStore(t)
	claimed := mustClaimShard(t, store, claimRequest("expired-source", "holder-a", "boot-a"))
	req := transferRequest("expired-takeover", claimed.Authority, "holder-b", "boot-b")

	active, err := store.TakeoverExpiredShardAuthority(context.Background(), req)
	if err != nil || active.Status != ShardTransitionActiveLeaseConflict {
		t.Fatalf("active lease takeover=%+v err=%v", active, err)
	}

	clock.Set(time.UnixMilli(claimed.Authority.LeaseUntilMs))
	wrongFence := req
	wrongFence.ExpectedFence.ExpectedWriterEpoch++
	wrongFence.TransitionID = "expired-wrong-fence"
	wrong, err := store.TakeoverExpiredShardAuthority(context.Background(), wrongFence)
	if err != nil || wrong.Status != ShardTransitionFenceMismatch {
		t.Fatalf("wrong-fence takeover=%+v err=%v", wrong, err)
	}

	taken, err := store.TakeoverExpiredShardAuthority(context.Background(), req)
	if err != nil || taken.Status != ShardTransitionApplied {
		t.Fatalf("expired takeover=%+v err=%v", taken, err)
	}
	if taken.Authority.HolderID != "holder-b" || taken.Authority.HolderBootID != "boot-b" ||
		taken.Authority.WriterEpoch != claimed.Authority.WriterEpoch+1 ||
		taken.Authority.LeaseEpoch != claimed.Authority.LeaseEpoch+1 ||
		taken.Authority.AssignmentRevision != claimed.Authority.AssignmentRevision+1 {
		t.Fatalf("taken authority=%+v", taken.Authority)
	}
	replay, err := store.TakeoverExpiredShardAuthority(context.Background(), req)
	if err != nil || replay.Status != ShardTransitionAlreadyApplied || replay.Authority != taken.Authority {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
}

func TestFarmWorkerShardAuthorityIsIndependentFromFarmActorAuthority(t *testing.T) {
	store, _ := newShardAuthorityTestStore(t)
	farm := claimRequest("farm-claim", "farm-holder", "farm-boot")
	farmResult := mustClaimShard(t, store, farm)

	worker := claimRequest("worker-claim", "farm-worker-a", "worker-a-boot")
	worker.Domain = actorrouting.DomainFarmWorker
	workerResult := mustClaimShard(t, store, worker)
	if workerResult.Authority.Domain != actorrouting.DomainFarmWorker {
		t.Fatalf("worker authority=%+v", workerResult.Authority)
	}

	moved, err := store.TransferShardAuthority(
		context.Background(),
		transferRequest(
			"worker-transfer",
			workerResult.Authority,
			"farm-worker-b",
			"worker-b-boot",
		),
	)
	if err != nil || moved.Status != ShardTransitionApplied {
		t.Fatalf("worker transfer=%+v err=%v", moved, err)
	}
	gotFarm, found := store.InspectShardWriteAuthority(
		actorrouting.DomainFarm,
		farm.VirtualShard,
	)
	if !found || gotFarm != farmResult.Authority {
		t.Fatalf("FARM authority changed by FARM_WORKER transition: %+v", gotFarm)
	}
	gotWorker, found := store.InspectShardWriteAuthority(
		actorrouting.DomainFarmWorker,
		worker.VirtualShard,
	)
	if !found || gotWorker.HolderID != "farm-worker-b" ||
		gotWorker.AssignmentRevision != 2 {
		t.Fatalf("FARM_WORKER authority=%+v", gotWorker)
	}
	if err := validateAddress(ActorAddress{
		Domain:  actorrouting.DomainFarmWorker,
		ActorID: 1,
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("FARM_WORKER actor address accepted: %v", err)
	}
}

func TestShardWriteAuthorityRejectsStoredHashProfileDrift(t *testing.T) {
	store, _ := newShardAuthorityTestStore(t)
	bad := ShardWriteAuthority{
		AuthorityLayoutVersion: "auth-v1",
		HashVersion:            actorrouting.HashVersionXXH64V2,
		HashSeed:               testAuthorityHashSeed + 1,
		Domain:                 actorrouting.DomainFarm,
		VirtualShard:           17,
		HolderID:               "holder-a",
		HolderBootID:           "boot-a",
		AssignmentRevision:     1,
		WriterEpoch:            1,
		LeaseEpoch:             1,
		State:                  ShardAuthorityActive,
		LeaseUntilMs:           time.UnixMilli(100_000).Add(time.Second).UnixMilli(),
	}
	if err := store.SetShardWriteAuthority(bad); !errors.Is(err, ErrAuthorityAddressMismatch) {
		t.Fatalf("mismatched seed Set err=%v", err)
	}

	claimed := mustClaimShard(t, store, claimRequest("profile-claim", "holder-a", "boot-a"))
	store.mu.Lock()
	store.shards[shardKey{domain: string(actorrouting.DomainFarm), shard: 17}].authority.HashSeed++
	store.mu.Unlock()
	renew := RenewShardAuthorityRequest{
		Caller: holderCaller("holder-a", "boot-a"), AuthorityLayoutVersion: "auth-v1",
		Domain: actorrouting.DomainFarm, VirtualShard: 17,
		ExpectedFence: fenceOf(claimed.Authority), RequestedTTLms: 1_000,
	}
	if _, err := store.RenewShardAuthority(context.Background(), renew); !errors.Is(err, ErrAuthorityAddressMismatch) {
		t.Fatalf("corrupt stored profile renew err=%v", err)
	}
}

func TestShardWriteAuthorityTransferUnassignAndReclaim(t *testing.T) {
	store, _ := newShardAuthorityTestStore(t)
	claimed := mustClaimShard(t, store, claimRequest("claim-a", "holder-a", "boot-a"))

	transfer := transferRequest("transfer-a-b", claimed.Authority, "holder-b", "boot-b")
	moved, err := store.TransferShardAuthority(context.Background(), transfer)
	if err != nil || moved.Status != ShardTransitionApplied {
		t.Fatalf("transfer=%+v err=%v", moved, err)
	}
	if moved.Authority.WriterEpoch != 2 || moved.Authority.LeaseEpoch != 2 || moved.Authority.AssignmentRevision != 2 || moved.Authority.HolderID != "holder-b" {
		t.Fatalf("moved=%+v", moved.Authority)
	}
	if replay, err := store.TransferShardAuthority(context.Background(), transfer); err != nil || replay.Status != ShardTransitionAlreadyApplied {
		t.Fatalf("transfer replay=%+v err=%v", replay, err)
	}

	unassign := UnassignShardAuthorityRequest{
		Caller: routeManagerCaller(), TransitionID: "unassign-b", AuthorityLayoutVersion: "auth-v1",
		Domain: actorrouting.DomainFarm, VirtualShard: 17, ExpectedFence: fenceOf(moved.Authority),
	}
	unassigned, err := store.UnassignShardAuthority(context.Background(), unassign)
	if err != nil || unassigned.Status != ShardTransitionApplied {
		t.Fatalf("unassign=%+v err=%v", unassigned, err)
	}
	if unassigned.Authority.State != ShardAuthorityUnassigned || unassigned.Authority.HolderID != "" || unassigned.Authority.LeaseUntilMs != 0 || unassigned.Authority.WriterEpoch != 3 || unassigned.Authority.LeaseEpoch != 3 || unassigned.Authority.AssignmentRevision != 3 {
		t.Fatalf("unassigned authority=%+v", unassigned.Authority)
	}
	if replay, err := store.UnassignShardAuthority(context.Background(), unassign); err != nil || replay.Status != ShardTransitionAlreadyApplied {
		t.Fatalf("unassign replay=%+v err=%v", replay, err)
	}
	repeatedUnassign := unassign
	repeatedUnassign.TransitionID = "unassign-again"
	repeatedUnassign.ExpectedFence = fenceOf(unassigned.Authority)
	if got, err := store.UnassignShardAuthority(context.Background(), repeatedUnassign); err != nil || got.Status != ShardTransitionFenceMismatch {
		t.Fatalf("repeated unassign=%+v err=%v", got, err)
	}

	lateRenew := RenewShardAuthorityRequest{
		Caller: holderCaller("holder-b", "boot-b"), AuthorityLayoutVersion: "auth-v1",
		Domain: actorrouting.DomainFarm, VirtualShard: 17,
		ExpectedFence: fenceOf(moved.Authority), RequestedTTLms: 1_000,
	}
	if got, err := store.RenewShardAuthority(context.Background(), lateRenew); err != nil || got.Status != ShardTransitionLeaseExpired {
		t.Fatalf("late renew=%+v err=%v", got, err)
	}

	reclaimed := mustClaimShard(t, store, claimRequest("claim-c", "holder-c", "boot-c"))
	if reclaimed.Authority.WriterEpoch != 4 || reclaimed.Authority.LeaseEpoch != 4 || reclaimed.Authority.AssignmentRevision != 4 {
		t.Fatalf("reclaimed generation=%+v", reclaimed.Authority)
	}
}

func TestShardTransferAndActorCommitHaveOneLinearizationOrder(t *testing.T) {
	for _, tc := range []struct {
		name        string
		commitFirst bool
		wantCommit  AuthorityCommitStatus
		wantLookup  IdempotencyLookupStatus
	}{
		{name: "commit_C8_then_transfer_E9", commitFirst: true, wantCommit: CommitCommitted, wantLookup: IdempotencyFoundMatch},
		{name: "transfer_E9_then_late_commit_C8", commitFirst: false, wantCommit: CommitWriterEpochMismatch, wantLookup: IdempotencyNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, address, oldAuthority, loaded := newE8Actor(t)
			commitReq := testCommit(address, fenceOf(oldAuthority), "claim-e8", loaded.RecordVersion, "ordered-write", ModelState{"plots": []byte("changed")})
			transferReq := transferRequest("transfer-e8-e9", oldAuthority, "farm-holder-b", "boot-b")

			commitStart := make(chan struct{})
			transferStart := make(chan struct{})
			commitDone := make(chan struct{})
			transferDone := make(chan struct{})
			var commitResult CommitResult
			var transferResult ShardAuthorityTransitionResult
			var commitErr, transferErr error
			go func() {
				<-commitStart
				commitResult, commitErr = store.Commit(context.Background(), commitReq)
				close(commitDone)
			}()
			go func() {
				<-transferStart
				transferResult, transferErr = store.TransferShardAuthority(context.Background(), transferReq)
				close(transferDone)
			}()

			if tc.commitFirst {
				close(commitStart)
				<-commitDone
				close(transferStart)
				<-transferDone
			} else {
				close(transferStart)
				<-transferDone
				close(commitStart)
				<-commitDone
			}
			if commitErr != nil || transferErr != nil || transferResult.Status != ShardTransitionApplied || commitResult.Status != tc.wantCommit {
				t.Fatalf("commit=%+v/%v transfer=%+v/%v", commitResult, commitErr, transferResult, transferErr)
			}
			if transferResult.Authority.WriterEpoch != 9 {
				t.Fatalf("writer epoch=%d", transferResult.Authority.WriterEpoch)
			}
			lookup, err := store.LookupIdempotencyOutcome(context.Background(), lookupFor(commitReq))
			if err != nil || lookup.Status != tc.wantLookup {
				t.Fatalf("lookup=%+v err=%v", lookup, err)
			}
		})
	}
}

func TestShardAuthorityConcurrentClaimAndTransferAreIdempotent(t *testing.T) {
	store, _ := newShardAuthorityTestStore(t)
	claim := claimRequest("claim-once", "holder-a", "boot-a")
	statuses := runConcurrentTransitions(t, 100, func() (ShardAuthorityTransitionResult, error) {
		return store.ClaimShardAuthority(context.Background(), claim)
	})
	assertStatusCounts(t, statuses, map[ShardAuthorityTransitionStatus]int{ShardTransitionApplied: 1, ShardTransitionAlreadyApplied: 99})

	authority, ok := store.InspectShardWriteAuthority(actorrouting.DomainFarm, 17)
	if !ok {
		t.Fatal("authority missing")
	}
	transfer := transferRequest("transfer-once", authority, "holder-b", "boot-b")
	statuses = runConcurrentTransitions(t, 100, func() (ShardAuthorityTransitionResult, error) {
		return store.TransferShardAuthority(context.Background(), transfer)
	})
	assertStatusCounts(t, statuses, map[ShardAuthorityTransitionStatus]int{ShardTransitionApplied: 1, ShardTransitionAlreadyApplied: 99})

	competingStore, _ := newShardAuthorityTestStore(t)
	statuses = runConcurrentTransitionsIndexed(t, 100, func(index int) (ShardAuthorityTransitionResult, error) {
		return competingStore.ClaimShardAuthority(context.Background(), claimRequest(
			fmt.Sprintf("competing-claim-%d", index),
			fmt.Sprintf("holder-%d", index),
			fmt.Sprintf("boot-%d", index),
		))
	})
	assertStatusCounts(t, statuses, map[ShardAuthorityTransitionStatus]int{ShardTransitionApplied: 1, ShardTransitionActiveLeaseConflict: 99})
}

func TestShardAuthorityTimeoutUnknownResolvesByStableTransitionID(t *testing.T) {
	store, _ := newShardAuthorityTestStore(t)
	faults := NewFaultStore(store)
	claim := claimRequest("claim-unknown", "holder-a", "boot-a")
	faults.Inject(MethodClaimShardAuthority, FaultTimeoutUnknownAfterApply)
	unknown, err := faults.ClaimShardAuthority(context.Background(), claim)
	if err != nil || unknown.Status != ShardTransitionTimeoutUnknown {
		t.Fatalf("claim unknown=%+v err=%v", unknown, err)
	}
	resolved, err := faults.ClaimShardAuthority(context.Background(), claim)
	if err != nil || resolved.Status != ShardTransitionAlreadyApplied || resolved.Authority.WriterEpoch != 1 {
		t.Fatalf("claim resolved=%+v err=%v", resolved, err)
	}

	transfer := transferRequest("transfer-unknown", resolved.Authority, "holder-b", "boot-b")
	faults.Inject(MethodTransferShardAuthority, FaultTimeoutUnknownAfterApply)
	unknown, err = faults.TransferShardAuthority(context.Background(), transfer)
	if err != nil || unknown.Status != ShardTransitionTimeoutUnknown {
		t.Fatalf("transfer unknown=%+v err=%v", unknown, err)
	}
	resolved, err = faults.TransferShardAuthority(context.Background(), transfer)
	if err != nil || resolved.Status != ShardTransitionAlreadyApplied || resolved.Authority.WriterEpoch != 2 {
		t.Fatalf("transfer resolved=%+v err=%v", resolved, err)
	}
}

func TestShardAuthorityTimeoutUnknownBeforeApplyDoesNotMutate(t *testing.T) {
	store, _ := newShardAuthorityTestStore(t)
	faults := NewFaultStore(store)
	claim := claimRequest("claim-before-apply", "holder-a", "boot-a")
	faults.Inject(MethodClaimShardAuthority, FaultTimeoutUnknownBeforeApply)
	unknown, err := faults.ClaimShardAuthority(context.Background(), claim)
	if err != nil || unknown.Status != ShardTransitionTimeoutUnknown {
		t.Fatalf("unknown=%+v err=%v", unknown, err)
	}
	if _, ok := store.InspectShardWriteAuthority(claim.Domain, claim.VirtualShard); ok {
		t.Fatal("before-apply timeout mutated authority")
	}
	applied, err := faults.ClaimShardAuthority(context.Background(), claim)
	if err != nil || applied.Status != ShardTransitionApplied || applied.Authority.WriterEpoch != 1 {
		t.Fatalf("retry=%+v err=%v", applied, err)
	}
}

func TestHolderLeaseGuardUsesRequestStartAndSelfFences(t *testing.T) {
	store, clock := newShardAuthorityTestStore(t)
	requestStarted := clock.Now()
	claimed := mustClaimShard(t, store, claimRequest("claim-a", "holder-a", "boot-a"))
	guard, err := NewHolderLeaseGuard("holder-a", "boot-a", 100*time.Millisecond, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := guard.InstallGrant(requestStarted, claimed); err != nil {
		t.Fatal(err)
	}
	if !guard.CanAdmitWrite() || !guard.CanActivateActor() || !guard.CanStartActorTurn() {
		t.Fatal("fresh lease did not admit all three hot paths")
	}
	clock.Advance(899 * time.Millisecond)
	if !guard.CanStartActorTurn() {
		t.Fatal("guard fenced before conservative deadline")
	}
	clock.Advance(time.Millisecond)
	if guard.CanAdmitWrite() || guard.CanActivateActor() || guard.CanStartActorTurn() {
		t.Fatal("guard did not self-fence at deadline")
	}
	state, _, _ := guard.Snapshot()
	if state != HolderLeaseGuardFenced {
		t.Fatalf("state=%d", state)
	}
}

func TestHolderLeaseGuardRenewAndTransferObservation(t *testing.T) {
	store, clock := newShardAuthorityTestStore(t)
	claimStart := clock.Now()
	claimed := mustClaimShard(t, store, claimRequest("claim-a", "holder-a", "boot-a"))
	guardA, _ := NewHolderLeaseGuard("holder-a", "boot-a", 100*time.Millisecond, clock.Now)
	if err := guardA.InstallGrant(claimStart, claimed); err != nil {
		t.Fatal(err)
	}

	clock.Advance(500 * time.Millisecond)
	renewStart := clock.Now()
	renewed, err := store.RenewShardAuthority(context.Background(), RenewShardAuthorityRequest{
		Caller: holderCaller("holder-a", "boot-a"), AuthorityLayoutVersion: "auth-v1",
		Domain: actorrouting.DomainFarm, VirtualShard: 17,
		ExpectedFence: fenceOf(claimed.Authority), RequestedTTLms: 1_000,
	})
	if err != nil || renewed.Status != ShardTransitionApplied {
		t.Fatalf("renew=%+v err=%v", renewed, err)
	}
	if err := guardA.InstallGrant(renewStart, renewed); err != nil {
		t.Fatal(err)
	}
	clock.Advance(500 * time.Millisecond)
	if !guardA.CanStartActorTurn() {
		t.Fatal("renew did not extend local deadline")
	}

	transferStart := clock.Now()
	moved, err := store.TransferShardAuthority(context.Background(), transferRequest("transfer-a-b", renewed.Authority, "holder-b", "boot-b"))
	if err != nil || moved.Status != ShardTransitionApplied {
		t.Fatalf("transfer=%+v err=%v", moved, err)
	}
	if err := guardA.ObserveAuthority(moved.Authority); err != nil {
		t.Fatal(err)
	}
	if guardA.CanAdmitWrite() {
		t.Fatal("old holder remained writable after observing transfer")
	}
	guardB, _ := NewHolderLeaseGuard("holder-b", "boot-b", 100*time.Millisecond, clock.Now)
	if err := guardB.InstallGrant(transferStart, moved); err != nil || !guardB.CanActivateActor() {
		t.Fatalf("new holder guard err=%v active=%v", err, guardB.CanActivateActor())
	}
}

func TestHolderLeaseGuardRejectsDelayedAndStaleGrant(t *testing.T) {
	store, clock := newShardAuthorityTestStore(t)
	requestStarted := clock.Now()
	claimed := mustClaimShard(t, store, claimRequest("claim-a", "holder-a", "boot-a"))
	guard, _ := NewHolderLeaseGuard("holder-a", "boot-a", 100*time.Millisecond, clock.Now)
	clock.Advance(950 * time.Millisecond)
	if err := guard.InstallGrant(requestStarted, claimed); !errors.Is(err, ErrLeaseDeadlineExpired) {
		t.Fatalf("delayed grant err=%v", err)
	}
	if guard.CanAdmitWrite() {
		t.Fatal("delayed response resurrected lease")
	}

	clock.Set(time.UnixMilli(100_000))
	guard, _ = NewHolderLeaseGuard("holder-a", "boot-a", 100*time.Millisecond, clock.Now)
	if err := guard.InstallGrant(clock.Now(), claimed); err != nil {
		t.Fatal(err)
	}
	newer := claimed
	newer.Authority.AssignmentRevision++
	newer.Authority.WriterEpoch++
	newer.Authority.LeaseEpoch++
	if err := guard.InstallGrant(clock.Now(), newer); err != nil {
		t.Fatal(err)
	}
	if err := guard.InstallGrant(clock.Now(), claimed); !errors.Is(err, ErrStaleLeaseGrant) {
		t.Fatalf("stale grant err=%v", err)
	}
}

func TestHolderLeaseGuardExplicitFenceCannotBeUndoneBySameRevisionRenew(t *testing.T) {
	store, clock := newShardAuthorityTestStore(t)
	claimed := mustClaimShard(t, store, claimRequest("claim-a", "holder-a", "boot-a"))
	guard, _ := NewHolderLeaseGuard("holder-a", "boot-a", 100*time.Millisecond, clock.Now)
	if err := guard.InstallGrant(clock.Now(), claimed); err != nil {
		t.Fatal(err)
	}

	renewStarted := clock.Now()
	renewed, err := store.RenewShardAuthority(context.Background(), RenewShardAuthorityRequest{
		Caller: holderCaller("holder-a", "boot-a"), AuthorityLayoutVersion: "auth-v1",
		Domain: actorrouting.DomainFarm, VirtualShard: 17,
		ExpectedFence: fenceOf(claimed.Authority), RequestedTTLms: 1_000,
	})
	if err != nil || renewed.Status != ShardTransitionApplied {
		t.Fatalf("renew=%+v err=%v", renewed, err)
	}
	guard.Fence()
	if err := guard.InstallGrant(renewStarted, renewed); !errors.Is(err, ErrStaleLeaseGrant) {
		t.Fatalf("late same-revision renew err=%v", err)
	}
	if guard.CanAdmitWrite() || guard.CanActivateActor() || guard.CanStartActorTurn() {
		t.Fatal("explicitly fenced guard was resurrected")
	}
}

func TestHolderLeaseGuardCorruptNewerAssignmentFailsClosedWithoutLoweringEpoch(t *testing.T) {
	store, clock := newShardAuthorityTestStore(t)
	claimed := mustClaimShard(t, store, claimRequest("claim-a", "holder-a", "boot-a"))
	guard, _ := NewHolderLeaseGuard("holder-a", "boot-a", 100*time.Millisecond, clock.Now)
	if err := guard.InstallGrant(clock.Now(), claimed); err != nil {
		t.Fatal(err)
	}

	corrupt := claimed.Authority
	corrupt.AssignmentRevision++
	corrupt.WriterEpoch = 0
	corrupt.LeaseEpoch = 0
	if err := guard.ObserveAuthority(corrupt); !errors.Is(err, ErrInconsistentLeaseGrant) {
		t.Fatalf("corrupt observation err=%v", err)
	}
	state, retained, _ := guard.Snapshot()
	if state != HolderLeaseGuardFenced || retained.WriterEpoch != claimed.Authority.WriterEpoch || retained.LeaseEpoch != claimed.Authority.LeaseEpoch {
		t.Fatalf("state=%d retained=%+v", state, retained)
	}
	if guard.CanStartActorTurn() {
		t.Fatal("corrupt newer assignment did not fail closed")
	}
}

func newShardAuthorityTestStore(t *testing.T) (*MemoryStore, *shardTestClock) {
	t.Helper()
	calculator, err := actorrouting.NewCalculator(
		"test-realm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: testAuthorityHashSeed},
		64,
		64,
	)
	if err != nil {
		t.Fatal(err)
	}
	clock := newShardTestClock()
	store, err := NewMemoryStore(MemoryOptions{
		Resolver: calculator, AuthorityLayoutVersion: "auth-v1", Now: clock.Now,
		LeaseLimits: LeaseLimits{MinTTL: 100 * time.Millisecond, MaxTTL: 10 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	return store, clock
}

func claimRequest(id, holder, boot string) ClaimShardAuthorityRequest {
	return ClaimShardAuthorityRequest{
		Caller: routeManagerCaller(), TransitionID: id, AuthorityLayoutVersion: "auth-v1",
		Domain: actorrouting.DomainFarm, VirtualShard: 17,
		NewHolderID: holder, NewHolderBootID: boot, RequestedTTLms: 1_000,
	}
}

func transferRequest(id string, old ShardWriteAuthority, holder, boot string) TransferShardAuthorityRequest {
	return TransferShardAuthorityRequest{
		Caller: routeManagerCaller(), TransitionID: id, AuthorityLayoutVersion: "auth-v1",
		Domain: old.Domain, VirtualShard: old.VirtualShard, ExpectedFence: fenceOf(old),
		NewHolderID: holder, NewHolderBootID: boot, RequestedTTLms: 1_000,
	}
}

func routeManagerCaller() TrustedShardAuthorityCaller {
	return TrustedShardAuthorityCaller{Kind: ShardAuthorityCallerRouteManager, InstanceID: "route-manager-1", BootID: "rm-boot-1"}
}

func holderCaller(holder, boot string) TrustedShardAuthorityCaller {
	return TrustedShardAuthorityCaller{Kind: ShardAuthorityCallerHolder, InstanceID: holder, BootID: boot}
}

func mustClaimShard(t *testing.T, store *MemoryStore, req ClaimShardAuthorityRequest) ShardAuthorityTransitionResult {
	t.Helper()
	result, err := store.ClaimShardAuthority(context.Background(), req)
	if err != nil || result.Status != ShardTransitionApplied {
		t.Fatalf("claim=%+v err=%v", result, err)
	}
	return result
}

func newE8Actor(t *testing.T) (*MemoryStore, ActorAddress, ShardWriteAuthority, AuthoritySnapshot) {
	t.Helper()
	calculator, err := actorrouting.NewCalculator(
		"test-realm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: testAuthorityHashSeed},
		64,
		64,
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(100_000)
	store, err := NewMemoryStore(MemoryOptions{Resolver: calculator, AuthorityLayoutVersion: "auth-v1", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	address := ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 9001}
	shard, err := calculator.ShardFor(address.Domain, address.ActorID)
	if err != nil {
		t.Fatal(err)
	}
	authority := ShardWriteAuthority{
		AuthorityLayoutVersion: "auth-v1", HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: testAuthorityHashSeed,
		Domain: address.Domain, VirtualShard: shard,
		HolderID: "farm-holder-a", HolderBootID: "boot-a",
		AssignmentRevision: 8, WriterEpoch: 8, LeaseEpoch: 8,
		State: ShardAuthorityActive, LeaseUntilMs: now.Add(time.Minute).UnixMilli(),
	}
	if err := store.SetShardWriteAuthority(authority); err != nil {
		t.Fatal(err)
	}
	loaded := mustLoad(t, store, address, fenceOf(authority), "claim-e8")
	return store, address, authority, loaded
}

func runConcurrentTransitions(t *testing.T, count int, fn func() (ShardAuthorityTransitionResult, error)) []ShardAuthorityTransitionStatus {
	return runConcurrentTransitionsIndexed(t, count, func(int) (ShardAuthorityTransitionResult, error) { return fn() })
}

func runConcurrentTransitionsIndexed(t *testing.T, count int, fn func(int) (ShardAuthorityTransitionResult, error)) []ShardAuthorityTransitionStatus {
	t.Helper()
	start := make(chan struct{})
	statuses := make(chan ShardAuthorityTransitionStatus, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		index := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := fn(index)
			if err != nil {
				t.Errorf("transition err=%v", err)
				return
			}
			statuses <- result.Status
		}()
	}
	close(start)
	wg.Wait()
	close(statuses)
	result := make([]ShardAuthorityTransitionStatus, 0, count)
	for status := range statuses {
		result = append(result, status)
	}
	return result
}

func assertStatusCounts(t *testing.T, statuses []ShardAuthorityTransitionStatus, want map[ShardAuthorityTransitionStatus]int) {
	t.Helper()
	got := make(map[ShardAuthorityTransitionStatus]int)
	for _, status := range statuses {
		got[status]++
	}
	if len(got) != len(want) {
		t.Fatalf("status counts=%v want=%v", got, want)
	}
	for status, count := range want {
		if got[status] != count {
			t.Fatalf("status counts=%v want=%v", got, want)
		}
	}
}
