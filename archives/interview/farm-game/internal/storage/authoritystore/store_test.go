package authoritystore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
)

const testAuthorityHashSeed uint64 = 202608030001

func TestMemoryStoreLoadAndClaimIsIdempotentAndFencesOldClaim(t *testing.T) {
	store, address, fence := newTestStore(t)
	req := LoadAndClaimRequest{
		ActorAddress:           address,
		AuthorityLayoutVersion: "auth-v1",
		ShardWriteFence:        fence,
		ClaimOperationID:       "claim-op-1",
		NewWriterClaimID:       "claim-1",
		InitialModelFields:     ModelState{"profile": []byte("v1")},
	}

	first, err := store.LoadAndClaim(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != LoadAndClaimClaimed || first.Snapshot.RecordVersion != 1 || first.Snapshot.WriterClaimID != "claim-1" {
		t.Fatalf("first=%+v", first)
	}
	if usage := first.Snapshot.PhysicalUsage; usage.Validate(first.Snapshot.RecordVersion) != nil || usage.ModelBytes != len("profile")+len("v1") || usage.ReceiptItems != 0 || usage.ReceiptBytes != 0 {
		t.Fatalf("first physical usage=%+v", usage)
	}
	first.Snapshot.ModelFields["profile"][0] = 'X'

	again, err := store.LoadAndClaim(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != LoadAndClaimAlreadyClaimed || string(again.Snapshot.ModelFields["profile"]) != "v1" {
		t.Fatalf("again=%+v", again)
	}

	nextReq := req
	nextReq.ClaimOperationID = "claim-op-2"
	nextReq.NewWriterClaimID = "claim-2"
	nextReq.InitialModelFields = ModelState{"profile": []byte("must-not-reinitialize")}
	next, err := store.LoadAndClaim(context.Background(), nextReq)
	if err != nil {
		t.Fatal(err)
	}
	if next.Status != LoadAndClaimClaimed || next.Snapshot.WriterClaimID != "claim-2" || string(next.Snapshot.ModelFields["profile"]) != "v1" {
		t.Fatalf("next=%+v", next)
	}

	oldCommit := testCommit(address, fence, "claim-1", next.Snapshot.RecordVersion, "op-old", ModelState{"profile": []byte("illegal")})
	got, err := store.Commit(context.Background(), oldCommit)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != CommitWriterClaimMismatch {
		t.Fatalf("status=%s", got.Status)
	}
}

func TestMemoryStoreOrdinaryCommitHasNoExactOutputOrExtraClockAcrossModelSizes(t *testing.T) {
	store, address, fence := newTestStore(t)
	loaded := mustLoad(t, store, address, fence, "off-overhead")
	clockReads := 0
	store.now = func() time.Time {
		clockReads++
		return time.UnixMilli(10_000)
	}
	version := loaded.RecordVersion
	for index, size := range []int{1, 256 << 10} {
		request := testCommit(address, fence, "off-overhead", version, fmt.Sprintf("off-overhead-%d", index),
			ModelState{"record": bytes.Repeat([]byte{'x'}, size)})
		request.AttachedOrders = []AttachedOrder{{OrderID: fmt.Sprintf("off-order-%d", index), OrderType: "CARE_REWARD", Payload: []byte("payload"), DueAtMs: 10_000}}
		beforeReads := clockReads
		result, err := store.Commit(context.Background(), request)
		if err != nil || result.Status != CommitCommitted || result.CommitStamp != nil {
			t.Fatalf("ordinary size=%d result=%+v err=%v", size, result, err)
		}
		// The one trusted-time read is the mandatory lease fence. Exact output
		// would add a second read regardless of model size.
		if got := clockReads - beforeReads; got != 1 {
			t.Fatalf("ordinary size=%d clock reads=%d, want lease fence only", size, got)
		}
		if _, explicit := result.ExplicitCommit(request); explicit {
			t.Fatalf("ordinary size=%d exposed exact output", size)
		}
		version = result.Snapshot.RecordVersion
	}
}

func TestMemoryStoreCommitChecksReceiptBeforeRecordVersion(t *testing.T) {
	store, address, fence := newTestStore(t)
	loaded := mustLoad(t, store, address, fence, "claim-1")
	req := testCommit(address, fence, "claim-1", loaded.RecordVersion, "plant-op-1", ModelState{"plots": []byte("planted")})
	req.AttachedOrders = []AttachedOrder{{OrderID: "order-1", OrderType: "CARE_REWARD", Payload: []byte("payload"), DueAtMs: 123}}

	first, err := store.CommitExact(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != CommitCommitted || first.Snapshot.RecordVersion != loaded.RecordVersion+1 {
		t.Fatalf("first=%+v", first)
	}
	commit, explicit := first.ExplicitCommit(req)
	if !explicit || commit.RecordVersion() != first.Snapshot.RecordVersion || commit.CommittedAtMs() <= 0 {
		t.Fatalf("explicit commit output=%+v valid=%v", commit, explicit)
	}
	exact, ok := NewExactCommitEvidence(req, first)
	if !ok {
		t.Fatal("exact first-commit evidence missing")
	}
	req.AttachedOrders[0].Payload[0] = 'X'
	openedRequest, openedResult, ok := exact.Open()
	if !ok || string(openedRequest.AttachedOrders[0].Payload) != "payload" || openedResult.CommitStamp == nil {
		t.Fatalf("exact evidence was retargeted by caller mutation: request=%+v result=%+v ok=%v", openedRequest, openedResult, ok)
	}

	changed := req
	changed.AttachedOrders = append([]AttachedOrder(nil), req.AttachedOrders...)
	changed.AttachedOrders[0].DueAtMs++
	if _, explicit := first.ExplicitCommit(changed); explicit {
		t.Fatal("commit stamp accepted a different request")
	}
	projected, err := store.ProjectPhysicalCommit(req)
	if err != nil {
		t.Fatal(err)
	}
	if usage := first.Snapshot.PhysicalUsage; usage.Validate(first.Snapshot.RecordVersion) != nil ||
		usage.ModelBytes != projected.ModelBytes || usage.ReceiptItems != projected.ReceiptItems || usage.ReceiptBytes != projected.ReceiptBytes {
		t.Fatalf("committed physical usage=%+v projected=%+v", usage, projected)
	}

	// The retry deliberately carries the old expected version. Receipt lookup
	// must run before RecordVersion comparison and return the original result.
	replay := req
	replay.CandidateState = nil
	replay.OperationResult = StoredOperationOutcome{Status: TerminalUnknown}
	replay.Receipt.Outcome = replay.OperationResult
	again, err := store.Commit(context.Background(), replay)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != CommitAlreadyCommitted || again.Outcome.Status != TerminalSucceeded || again.CommitStamp != nil {
		t.Fatalf("again=%+v", again)
	}
	if _, explicit := again.ExplicitCommit(replay); explicit {
		t.Fatal("ALREADY_COMMITTED exposed an explicit commit stamp")
	}

	conflict := req
	conflict.CanonicalPayloadHash = sha256.Sum256([]byte("different-payload"))
	conflict.Receipt.CanonicalPayloadHash = conflict.CanonicalPayloadHash
	got, err := store.Commit(context.Background(), conflict)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != CommitIdempotencyConflict {
		t.Fatalf("status=%s", got.Status)
	}

	snapshot, ok := store.Inspect(address)
	if !ok || snapshot.RecordVersion != first.Snapshot.RecordVersion || string(snapshot.ModelFields["plots"]) != "planted" {
		t.Fatalf("snapshot=%+v ok=%v", snapshot, ok)
	}
	if got := store.AttachedOrderCount(address); got != 1 {
		t.Fatalf("orders=%d, want 1", got)
	}
}

func TestMemoryStoreCommitEnforcesAllThreeFences(t *testing.T) {
	store, address, fence := newTestStore(t)
	loaded := mustLoad(t, store, address, fence, "claim-1")

	cases := []struct {
		name   string
		mutate func(*AuthorityCommitRequest)
		want   AuthorityCommitStatus
	}{
		{"holder", func(r *AuthorityCommitRequest) { r.ShardWriteFence.ExpectedHolderID = "other" }, CommitWriterEpochMismatch},
		{"boot", func(r *AuthorityCommitRequest) { r.ShardWriteFence.ExpectedHolderBootID = "other" }, CommitWriterEpochMismatch},
		{"assignment", func(r *AuthorityCommitRequest) { r.ShardWriteFence.ExpectedAssignmentRevision++ }, CommitWriterEpochMismatch},
		{"writer_epoch", func(r *AuthorityCommitRequest) { r.ShardWriteFence.ExpectedWriterEpoch++ }, CommitWriterEpochMismatch},
		{"lease_epoch", func(r *AuthorityCommitRequest) { r.ShardWriteFence.ExpectedLeaseEpoch++ }, CommitWriterEpochMismatch},
		{"claim", func(r *AuthorityCommitRequest) { r.ExpectedWriterClaimID = "old-claim" }, CommitWriterClaimMismatch},
		{"record_version", func(r *AuthorityCommitRequest) { r.ExpectedRecordVersion++ }, CommitRecordVersionConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := testCommit(address, fence, "claim-1", loaded.RecordVersion, "op-"+tc.name, ModelState{"economy": []byte(tc.name)})
			tc.mutate(&req)
			got, err := store.Commit(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != tc.want {
				t.Fatalf("status=%s want=%s", got.Status, tc.want)
			}
		})
	}

	snapshot, _ := store.Inspect(address)
	if snapshot.RecordVersion != loaded.RecordVersion {
		t.Fatalf("record version changed on rejected writes: %d", snapshot.RecordVersion)
	}

	group, err := store.deriveGroup(address, "auth-v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetShardWriteAuthority(ShardWriteAuthority{
		Domain: group.Domain, VirtualShard: group.VirtualShard,
		HolderID: fence.ExpectedHolderID, HolderBootID: fence.ExpectedHolderBootID,
		AssignmentRevision: fence.ExpectedAssignmentRevision, WriterEpoch: fence.ExpectedWriterEpoch,
		LeaseEpoch: fence.ExpectedLeaseEpoch, State: ShardAuthorityActive, LeaseUntilMs: 9_999,
	}); err != nil {
		t.Fatal(err)
	}
	expired := testCommit(address, fence, "claim-1", loaded.RecordVersion, "op-expired", ModelState{"economy": []byte("expired")})
	if got, err := store.Commit(context.Background(), expired); err != nil || got.Status != CommitWriterEpochMismatch {
		t.Fatalf("expired lease got=%+v err=%v", got, err)
	}
}

func TestMemoryStoreDeterministicNoChangeIsDurableTerminal(t *testing.T) {
	store, address, fence := newTestStore(t)
	loaded := mustLoad(t, store, address, fence, "claim-1")
	req := testCommit(address, fence, "claim-1", loaded.RecordVersion, "water-no-change", loaded.ModelFields)
	req.OperationResult.Status = TerminalNoChange
	req.Receipt.Outcome = cloneOutcome(req.OperationResult)

	got, err := store.Commit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != CommitCommitted || got.Snapshot.RecordVersion != loaded.RecordVersion+1 {
		t.Fatalf("got=%+v", got)
	}

	retry, err := store.Commit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Status != CommitAlreadyCommitted || retry.Outcome.Status != TerminalNoChange {
		t.Fatalf("retry=%+v", retry)
	}
}

func TestMemoryStoreConcurrentCommitProducesOneEffect(t *testing.T) {
	store, address, fence := newTestStore(t)
	loaded := mustLoad(t, store, address, fence, "claim-1")
	req := testCommit(address, fence, "claim-1", loaded.RecordVersion, "concurrent-op", ModelState{"inventory": []byte("+1")})

	const callers = 100
	var wg sync.WaitGroup
	statuses := make(chan AuthorityCommitStatus, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := store.Commit(context.Background(), req)
			if err != nil {
				t.Errorf("commit: %v", err)
				return
			}
			statuses <- result.Status
		}()
	}
	wg.Wait()
	close(statuses)
	committed, already := 0, 0
	for status := range statuses {
		switch status {
		case CommitCommitted:
			committed++
		case CommitAlreadyCommitted:
			already++
		default:
			t.Errorf("unexpected status %s", status)
		}
	}
	if committed != 1 || already != callers-1 {
		t.Fatalf("committed=%d already=%d", committed, already)
	}
}

func TestFaultStoreDistinguishesNotSentAndUnknownBeforeAfterApply(t *testing.T) {
	base, address, fence := newTestStore(t)
	loaded := mustLoad(t, base, address, fence, "claim-1")
	faults := NewFaultStore(base)

	notSentReq := testCommit(address, fence, "claim-1", loaded.RecordVersion, "not-sent-op", ModelState{"value": []byte("not-sent")})
	faults.Inject(MethodCommit, FaultUnavailableNotSent)
	notSent, err := faults.Commit(context.Background(), notSentReq)
	if err != nil || notSent.Status != CommitUnavailableNotSent {
		t.Fatalf("notSent=%+v err=%v", notSent, err)
	}
	lookup, err := base.LookupIdempotencyOutcome(context.Background(), lookupFor(notSentReq))
	if err != nil || lookup.Status != IdempotencyNotFound {
		t.Fatalf("lookup=%+v err=%v", lookup, err)
	}

	unknownBeforeReq := testCommit(address, fence, "claim-1", loaded.RecordVersion, "unknown-before", ModelState{"value": []byte("before")})
	faults.Inject(MethodCommit, FaultTimeoutUnknownBeforeApply)
	unknownBefore, err := faults.Commit(context.Background(), unknownBeforeReq)
	if err != nil || unknownBefore.Status != CommitTimeoutUnknown {
		t.Fatalf("unknownBefore=%+v err=%v", unknownBefore, err)
	}
	lookup, _ = base.LookupIdempotencyOutcome(context.Background(), lookupFor(unknownBeforeReq))
	if lookup.Status != IdempotencyNotFound {
		t.Fatalf("unknown-before unexpectedly applied: %+v", lookup)
	}

	unknownAfterReq := testCommit(address, fence, "claim-1", loaded.RecordVersion, "unknown-after", ModelState{"value": []byte("after")})
	faults.Inject(MethodCommit, FaultTimeoutUnknownAfterApply)
	unknownAfter, err := faults.Commit(context.Background(), unknownAfterReq)
	if err != nil || unknownAfter.Status != CommitTimeoutUnknown {
		t.Fatalf("unknownAfter=%+v err=%v", unknownAfter, err)
	}
	lookup, _ = base.LookupIdempotencyOutcome(context.Background(), lookupFor(unknownAfterReq))
	if lookup.Status != IdempotencyFoundMatch || lookup.Outcome.Status != TerminalSucceeded {
		t.Fatalf("unknown-after receipt missing: %+v", lookup)
	}
}

func TestFaultStoreLoadAndClaimUnknownPreservesOriginalClaimIdentity(t *testing.T) {
	beforeStore, address, fence := newTestStore(t)
	beforeFaults := NewFaultStore(beforeStore)
	req := LoadAndClaimRequest{
		ActorAddress:           address,
		AuthorityLayoutVersion: "auth-v1",
		ShardWriteFence:        fence,
		ClaimOperationID:       "claim-op-stable",
		NewWriterClaimID:       "claim-stable",
		InitialModelFields:     ModelState{"initial": []byte("state")},
	}
	beforeFaults.Inject(MethodLoadAndClaim, FaultTimeoutUnknownBeforeApply)
	before, err := beforeFaults.LoadAndClaim(context.Background(), req)
	if err != nil || before.Status != LoadAndClaimTimeoutUnknown {
		t.Fatalf("before=%+v err=%v", before, err)
	}
	if _, exists := beforeStore.Inspect(address); exists {
		t.Fatal("before-apply timeout unexpectedly created an Actor record")
	}
	retried, err := beforeStore.LoadAndClaim(context.Background(), req)
	if err != nil || retried.Status != LoadAndClaimClaimed || retried.Snapshot.WriterClaimID != req.NewWriterClaimID {
		t.Fatalf("before retry=%+v err=%v", retried, err)
	}

	afterStore, afterAddress, afterFence := newTestStore(t)
	afterFaults := NewFaultStore(afterStore)
	req.ActorAddress = afterAddress
	req.ShardWriteFence = afterFence
	afterFaults.Inject(MethodLoadAndClaim, FaultTimeoutUnknownAfterApply)
	after, err := afterFaults.LoadAndClaim(context.Background(), req)
	if err != nil || after.Status != LoadAndClaimTimeoutUnknown {
		t.Fatalf("after=%+v err=%v", after, err)
	}
	snapshot, exists := afterStore.Inspect(afterAddress)
	if !exists || snapshot.WriterClaimID != req.NewWriterClaimID {
		t.Fatalf("after-apply claim missing: snapshot=%+v exists=%v", snapshot, exists)
	}
	recovered, err := afterStore.LoadAndClaim(context.Background(), req)
	if err != nil || recovered.Status != LoadAndClaimAlreadyClaimed || recovered.Snapshot.WriterClaimID != req.NewWriterClaimID {
		t.Fatalf("after recovery=%+v err=%v", recovered, err)
	}
}

func TestMemoryStoreFenceAndClaimPersistsStableResolution(t *testing.T) {
	store, address, fence := newTestStore(t)
	loaded := mustLoad(t, store, address, fence, "claim-1")
	original := testCommit(address, fence, "claim-1", loaded.RecordVersion, "theft-op", ModelState{"crop": []byte("remaining=8")})
	committed, err := store.Commit(context.Background(), original)
	if err != nil || committed.Status != CommitCommitted {
		t.Fatalf("commit=%+v err=%v", committed, err)
	}

	fenceReq := FenceAndClaimRequest{
		ResolveOperationID:        "resolve-1",
		ActorAddress:              address,
		AuthorityLayoutVersion:    "auth-v1",
		ShardWriteFence:           fence,
		OldWriterClaimID:          "claim-1",
		NewWriterClaimID:          "claim-2",
		OriginalIdempotencyKey:    original.IdempotencyKey,
		CanonicalPayloadHash:      original.CanonicalPayloadHash,
		BeforeRecordVersion:       loaded.RecordVersion,
		BeforeBusinessStateDigest: loaded.BusinessStateDigest,
	}
	resolved, err := store.FenceAndClaim(context.Background(), fenceReq)
	if err != nil || resolved.Status != FenceResolutionCommitted || resolved.Snapshot.WriterClaimID != "claim-2" {
		t.Fatalf("resolved=%+v err=%v", resolved, err)
	}

	// A later C2 write must not change the persisted answer of resolve-1.
	later := testCommit(address, fence, "claim-2", resolved.Snapshot.RecordVersion, "later-op", ModelState{"crop": []byte("remaining=7")})
	if got, err := store.Commit(context.Background(), later); err != nil || got.Status != CommitCommitted {
		t.Fatalf("later=%+v err=%v", got, err)
	}
	again, err := store.FenceAndClaim(context.Background(), fenceReq)
	if err != nil || again.Status != FenceResolutionCommitted || again.Resolution.CutRecordVersion != resolved.Resolution.CutRecordVersion {
		t.Fatalf("again=%+v err=%v", again, err)
	}

	lateOld := testCommit(address, fence, "claim-1", committed.Snapshot.RecordVersion, "late-old", ModelState{"crop": []byte("illegal")})
	if got, err := store.Commit(context.Background(), lateOld); err != nil || got.Status != CommitWriterClaimMismatch {
		t.Fatalf("lateOld=%+v err=%v", got, err)
	}
}

func TestMemoryStoreFenceBeforeOriginalCommitProvesNotCommitted(t *testing.T) {
	store, address, fence := newTestStore(t)
	loaded := mustLoad(t, store, address, fence, "claim-1")
	original := testCommit(address, fence, "claim-1", loaded.RecordVersion, "plant-op", ModelState{"plot": []byte("planted")})
	fenceReq := FenceAndClaimRequest{
		ResolveOperationID:        "resolve-before",
		ActorAddress:              address,
		AuthorityLayoutVersion:    "auth-v1",
		ShardWriteFence:           fence,
		OldWriterClaimID:          "claim-1",
		NewWriterClaimID:          "claim-2",
		OriginalIdempotencyKey:    original.IdempotencyKey,
		CanonicalPayloadHash:      original.CanonicalPayloadHash,
		BeforeRecordVersion:       loaded.RecordVersion,
		BeforeBusinessStateDigest: loaded.BusinessStateDigest,
	}
	resolved, err := store.FenceAndClaim(context.Background(), fenceReq)
	if err != nil || resolved.Status != FenceResolutionNotCommitted {
		t.Fatalf("resolved=%+v err=%v", resolved, err)
	}
	late, err := store.Commit(context.Background(), original)
	if err != nil || late.Status != CommitWriterClaimMismatch {
		t.Fatalf("late=%+v err=%v", late, err)
	}
}

func TestMemoryStoreRejectsAuthorityGroupAssertionBeforeMutation(t *testing.T) {
	store, address, fence := newTestStore(t)
	loaded := mustLoad(t, store, address, fence, "claim-1")
	req := testCommit(address, fence, "claim-1", loaded.RecordVersion, "bad-group", ModelState{"x": []byte("y")})
	wrong := AuthorityGroupRef{AuthorityLayoutVersion: "auth-v1", Domain: actorrouting.DomainFarm, ActorID: address.ActorID, VirtualShard: 1}
	req.AssertedAuthorityGroup = &wrong
	_, err := store.Commit(context.Background(), req)
	if !errors.Is(err, ErrAuthorityAddressMismatch) {
		t.Fatalf("err=%v", err)
	}
	snapshot, _ := store.Inspect(address)
	if snapshot.RecordVersion != loaded.RecordVersion {
		t.Fatalf("mutation occurred: %+v", snapshot)
	}
}

func TestMemoryStoreCommitAcceptsFriendPairRootOnFarmActor(t *testing.T) {
	store, _, _ := newTestStore(t)
	farmAddress := ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 20002}
	calculator, err := actorrouting.NewCalculator(
		"test-realm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: testAuthorityHashSeed},
		64, 64,
	)
	if err != nil {
		t.Fatal(err)
	}
	farmShard, err := calculator.ShardFor(actorrouting.DomainFarm, farmAddress.ActorID)
	if err != nil {
		t.Fatal(err)
	}
	farmAuthority := ShardWriteAuthority{
		Domain: actorrouting.DomainFarm, VirtualShard: farmShard,
		HolderID: "farm-pod-1", HolderBootID: "boot-1", AssignmentRevision: 11,
		WriterEpoch: 7, LeaseEpoch: 3, State: ShardAuthorityActive,
		LeaseUntilMs: 70_000,
	}
	if err := store.SetShardWriteAuthority(farmAuthority); err != nil {
		t.Fatal(err)
	}
	fence := ShardWriteFence{
		ExpectedHolderID:           farmAuthority.HolderID,
		ExpectedHolderBootID:       farmAuthority.HolderBootID,
		ExpectedAssignmentRevision: farmAuthority.AssignmentRevision,
		ExpectedWriterEpoch:        farmAuthority.WriterEpoch,
		ExpectedLeaseEpoch:         farmAuthority.LeaseEpoch,
	}
	loaded := mustLoad(t, store, farmAddress, fence, "claim-f1")

	// A friend CARE/THEFT operation is rooted at the canonical pair, yet its
	// first authoritative ruling commits into the target FarmActor.
	pairRoot := OperationRootRef{Kind: RootKindFriendPair, CanonicalID: "sid1_pair_canonical_10001_20002"}
	key := IdempotencyKey{SubjectPlayerID: 10001, StableCommandTypeName: "WATER_CROP", OperationID: "care-op-1", Root: pairRoot}
	hash := sha256.Sum256([]byte("care-op-1:payload"))
	outcome := StoredOperationOutcome{Status: TerminalSucceeded, Payload: []byte("ok:care-op-1")}
	req := AuthorityCommitRequest{
		CommitRequestID:        "commit-care-op-1",
		ActorAddress:           farmAddress,
		AuthorityLayoutVersion: "auth-v1",
		ShardWriteFence:        fence,
		ExpectedWriterClaimID:  "claim-f1",
		ExpectedRecordVersion:  loaded.RecordVersion,
		IdempotencyKey:         key,
		CanonicalPayloadHash:   hash,
		CandidateState:         ModelState{"plots": []byte("watered")},
		Receipt:                AuthorityReceipt{ReceiptID: "receipt-care-op-1", IdempotencyKey: key, CanonicalPayloadHash: hash, Outcome: outcome},
		OperationResult:        outcome,
	}
	first, err := store.Commit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != CommitCommitted {
		t.Fatalf("friend-pair root commit=%s", first.Status)
	}

	// Replay of the full key + same hash returns the original outcome.
	replay, err := store.Commit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != CommitAlreadyCommitted || string(replay.Outcome.Payload) != "ok:care-op-1" {
		t.Fatalf("replay=%+v", replay)
	}

	// Same full key + different hash: conflict.
	conflicted := req
	conflicted.CanonicalPayloadHash[0] ^= 0xff
	conflicted.Receipt.CanonicalPayloadHash = conflicted.CanonicalPayloadHash
	conflict, err := store.Commit(context.Background(), conflicted)
	if err != nil {
		t.Fatal(err)
	}
	if conflict.Status != CommitIdempotencyConflict {
		t.Fatalf("conflict=%s", conflict.Status)
	}

	// Lookup honors the stored full key, not only the digest.
	lookup, err := store.LookupIdempotencyOutcome(context.Background(), IdempotencyLookupRequest{
		ActorAddress: farmAddress, IdempotencyKey: key, CanonicalPayloadHash: hash,
	})
	if err != nil || lookup.Status != IdempotencyFoundMatch || lookup.ReceiptRef != "receipt-care-op-1" {
		t.Fatalf("lookup=%+v err=%v", lookup, err)
	}
}

func TestMemoryStoreRootScopedDigestIsolation(t *testing.T) {
	store, address, fence := newTestStore(t)
	loaded := mustLoad(t, store, address, fence, "claim-1")

	base := IdempotencyKey{SubjectPlayerID: 10001, StableCommandTypeName: "TEST_COMMAND", OperationID: "shared-op",
		Root: OperationRootRef{Kind: RootKindPlayerActor, CanonicalID: "10001"}}
	other := base
	other.Root = OperationRootRef{Kind: RootKindFriendPair, CanonicalID: "sid1_pair_canonical_10001_20002"}
	digestA, err := base.OperationDigest()
	if err != nil {
		t.Fatal(err)
	}
	digestB, err := other.OperationDigest()
	if err != nil {
		t.Fatal(err)
	}
	if digestA == digestB {
		t.Fatal("same operation_id under different roots must produce different digests")
	}

	// Both roots may commit independently under the same operation_id.
	reqA := testCommit(address, fence, "claim-1", loaded.RecordVersion, "shared-op", ModelState{"x": []byte("a")})
	if _, err := store.Commit(context.Background(), reqA); err != nil {
		t.Fatal(err)
	}
	reqB := testCommit(address, fence, "claim-1", loaded.RecordVersion+1, "shared-op", ModelState{"x": []byte("b")})
	reqB.IdempotencyKey = other
	reqB.Receipt.IdempotencyKey = other
	committedB, err := store.Commit(context.Background(), reqB)
	if err != nil {
		t.Fatal(err)
	}
	if committedB.Status != CommitCommitted {
		t.Fatalf("second root commit=%s", committedB.Status)
	}
	// Replays stay isolated per root.
	replayA, err := store.Commit(context.Background(), reqA)
	if err != nil {
		t.Fatal(err)
	}
	if replayA.Status != CommitAlreadyCommitted || string(replayA.Outcome.Payload) != "ok:shared-op" {
		t.Fatalf("replayA=%+v", replayA)
	}
}

func TestMemoryStoreRejectsEmptyRoot(t *testing.T) {
	store, address, fence := newTestStore(t)
	loaded := mustLoad(t, store, address, fence, "claim-1")
	req := testCommit(address, fence, "claim-1", loaded.RecordVersion, "no-root", ModelState{"x": []byte("y")})
	req.IdempotencyKey.Root = OperationRootRef{}
	req.Receipt.IdempotencyKey = req.IdempotencyKey
	if _, err := store.Commit(context.Background(), req); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("empty root err=%v", err)
	}
}

func TestMemoryStoreRecoveryOverdueNeverFallsBackToNotFoundOrReplay(t *testing.T) {
	store, address, fence := newTestStore(t)
	loaded := mustLoad(t, store, address, fence, "claim-1")
	req := testCommit(address, fence, "claim-1", loaded.RecordVersion, "overdue-op", ModelState{"x": []byte("y")})
	if err := store.MarkRecoveryOverdue(address, req.IdempotencyKey, req.CanonicalPayloadHash); err != nil {
		t.Fatal(err)
	}
	lookup, err := store.LookupIdempotencyOutcome(context.Background(), lookupFor(req))
	if err != nil || lookup.Status != IdempotencyRecoveryOverdue {
		t.Fatalf("lookup=%+v err=%v", lookup, err)
	}
	if _, err := store.Commit(context.Background(), req); !errors.Is(err, ErrRecoveryOverdue) {
		t.Fatalf("commit err=%v", err)
	}
	snapshot, _ := store.Inspect(address)
	if snapshot.RecordVersion != loaded.RecordVersion {
		t.Fatalf("overdue evidence allowed mutation: %+v", snapshot)
	}
}

func TestMemoryStoreCapacityFailureDoesNotPartiallyCommitStateReceiptOrOrders(t *testing.T) {
	store, address, fence := newTestStore(t)
	loaded := mustLoad(t, store, address, fence, "claim-1")
	store.limits.MaxOrders = 1
	req := testCommit(address, fence, "claim-1", loaded.RecordVersion, "capacity-op", ModelState{"x": []byte("changed")})
	req.AttachedOrders = []AttachedOrder{
		{OrderID: "order-1", OrderType: "REWARD", Payload: []byte("one")},
		{OrderID: "order-2", OrderType: "REWARD", Payload: []byte("two")},
	}
	if _, err := store.Commit(context.Background(), req); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("commit err=%v", err)
	}
	snapshot, ok := store.Inspect(address)
	if !ok || snapshot.RecordVersion != loaded.RecordVersion || string(snapshot.ModelFields["initial"]) != "state" {
		t.Fatalf("state partially committed: snapshot=%+v ok=%v", snapshot, ok)
	}
	if got := store.AttachedOrderCount(address); got != 0 {
		t.Fatalf("orders partially committed: %d", got)
	}
	lookup, err := store.LookupIdempotencyOutcome(context.Background(), lookupFor(req))
	if err != nil || lookup.Status != IdempotencyNotFound {
		t.Fatalf("receipt partially committed: lookup=%+v err=%v", lookup, err)
	}

	commitLimited := testCommit(address, fence, "claim-1", loaded.RecordVersion, "commit-bytes-op", ModelState{"x": []byte("changed")})
	store.limits.MaxOrders = DefaultLimits().MaxOrders
	store.limits.MaxCommitBytes = AuthorityCommitSerializedBytes(commitLimited) - 1
	if _, err := store.Commit(context.Background(), commitLimited); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatalf("serialized commit err=%v", err)
	}
	caps, err := store.PhysicalCapabilities()
	if err != nil || caps.MaxCommitBytes != store.limits.MaxCommitBytes || caps.MaxSingleReceiptBytes != store.limits.MaxSingleReceiptBytes {
		t.Fatalf("physical capabilities=%+v err=%v", caps, err)
	}
}

func newTestStore(t *testing.T) (*MemoryStore, ActorAddress, ShardWriteFence) {
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
	now := time.UnixMilli(10_000)
	store, err := NewMemoryStore(MemoryOptions{
		Resolver:               calculator,
		AuthorityLayoutVersion: "auth-v1",
		Now:                    func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	address := ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 10001}
	shard, err := calculator.ShardFor(address.Domain, address.ActorID)
	if err != nil {
		t.Fatal(err)
	}
	authority := ShardWriteAuthority{
		Domain:             address.Domain,
		VirtualShard:       shard,
		HolderID:           "player-pod-1",
		HolderBootID:       "boot-1",
		AssignmentRevision: 11,
		WriterEpoch:        7,
		LeaseEpoch:         3,
		State:              ShardAuthorityActive,
		LeaseUntilMs:       now.Add(time.Minute).UnixMilli(),
	}
	if err := store.SetShardWriteAuthority(authority); err != nil {
		t.Fatal(err)
	}
	fence := ShardWriteFence{
		ExpectedHolderID:           authority.HolderID,
		ExpectedHolderBootID:       authority.HolderBootID,
		ExpectedAssignmentRevision: authority.AssignmentRevision,
		ExpectedWriterEpoch:        authority.WriterEpoch,
		ExpectedLeaseEpoch:         authority.LeaseEpoch,
	}
	return store, address, fence
}

func mustLoad(t *testing.T, store *MemoryStore, address ActorAddress, fence ShardWriteFence, claim string) AuthoritySnapshot {
	t.Helper()
	result, err := store.LoadAndClaim(context.Background(), LoadAndClaimRequest{
		ActorAddress:           address,
		AuthorityLayoutVersion: "auth-v1",
		ShardWriteFence:        fence,
		ClaimOperationID:       "load-" + claim,
		NewWriterClaimID:       claim,
		InitialModelFields:     ModelState{"initial": []byte("state")},
	})
	if err != nil || (result.Status != LoadAndClaimClaimed && result.Status != LoadAndClaimAlreadyClaimed) {
		t.Fatalf("load=%+v err=%v", result, err)
	}
	return result.Snapshot
}

func testCommit(address ActorAddress, fence ShardWriteFence, claim string, version uint64, operationID string, state ModelState) AuthorityCommitRequest {
	hash := sha256.Sum256([]byte(operationID + ":payload"))
	key := IdempotencyKey{SubjectPlayerID: 10001, StableCommandTypeName: "TEST_COMMAND", OperationID: operationID,
		Root: OperationRootRef{Kind: RootKindPlayerActor, CanonicalID: "10001"}}
	outcome := StoredOperationOutcome{Status: TerminalSucceeded, Payload: []byte("ok:" + operationID)}
	return AuthorityCommitRequest{
		CommitRequestID:        "commit-" + operationID,
		ActorAddress:           address,
		AuthorityLayoutVersion: "auth-v1",
		ShardWriteFence:        fence,
		ExpectedWriterClaimID:  claim,
		ExpectedRecordVersion:  version,
		IdempotencyKey:         key,
		CanonicalPayloadHash:   hash,
		CandidateState:         cloneModelState(state),
		Receipt: AuthorityReceipt{
			ReceiptID:            "receipt-" + operationID,
			IdempotencyKey:       key,
			CanonicalPayloadHash: hash,
			Outcome:              cloneOutcome(outcome),
		},
		OperationResult: outcome,
	}
}

func lookupFor(req AuthorityCommitRequest) IdempotencyLookupRequest {
	return IdempotencyLookupRequest{
		ActorAddress:         req.ActorAddress,
		IdempotencyKey:       req.IdempotencyKey,
		CanonicalPayloadHash: req.CanonicalPayloadHash,
	}
}
