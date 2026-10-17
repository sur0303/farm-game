package authoritystore

import (
	"context"
	"testing"
	"time"
)

type scheduledStore struct {
	inner         *MemoryStore
	commitEntered chan struct{}
	fenceEntered  chan struct{}
	releaseCommit chan struct{}
	releaseFence  chan struct{}
}

type commitCallResult struct {
	result CommitResult
	err    error
}

type fenceCallResult struct {
	result FenceAndClaimResult
	err    error
}

func newScheduledStore(inner *MemoryStore) *scheduledStore {
	return &scheduledStore{
		inner:         inner,
		commitEntered: make(chan struct{}),
		fenceEntered:  make(chan struct{}),
		releaseCommit: make(chan struct{}),
		releaseFence:  make(chan struct{}),
	}
}

func (s *scheduledStore) commit(ctx context.Context, req AuthorityCommitRequest) (CommitResult, error) {
	close(s.commitEntered)
	select {
	case <-s.releaseCommit:
		return s.inner.Commit(ctx, req)
	case <-ctx.Done():
		return CommitResult{}, ctx.Err()
	}
}

func (s *scheduledStore) fence(ctx context.Context, req FenceAndClaimRequest) (FenceAndClaimResult, error) {
	close(s.fenceEntered)
	select {
	case <-s.releaseFence:
		return s.inner.FenceAndClaim(ctx, req)
	case <-ctx.Done():
		return FenceAndClaimResult{}, ctx.Err()
	}
}

func TestFenceRaceCommitLinearizesBeforeFence(t *testing.T) {
	store, address, shardFence := newTestStore(t)
	before := mustLoad(t, store, address, shardFence, "claim-c1")
	commitReq := testCommit(address, shardFence, "claim-c1", before.RecordVersion, "unknown-r-before-f", ModelState{"crop": []byte("remaining=8")})
	fenceReq := testFenceRequest(address, shardFence, before, commitReq, "resolve-r-before-f", "claim-c1", "claim-c2")
	scheduled := newScheduledStore(store)

	commitResult := make(chan commitCallResult, 1)
	fenceResult := make(chan fenceCallResult, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		result, err := scheduled.commit(ctx, commitReq)
		commitResult <- commitCallResult{result: result, err: err}
	}()
	go func() {
		result, err := scheduled.fence(ctx, fenceReq)
		fenceResult <- fenceCallResult{result: result, err: err}
	}()
	waitSignal(t, scheduled.commitEntered)
	waitSignal(t, scheduled.fenceEntered)

	close(scheduled.releaseCommit)
	committed := <-commitResult
	if committed.err != nil || committed.result.Status != CommitCommitted {
		t.Fatalf("R result=%+v err=%v", committed.result, committed.err)
	}
	close(scheduled.releaseFence)
	resolved := <-fenceResult
	if resolved.err != nil || resolved.result.Status != FenceResolutionCommitted {
		t.Fatalf("F result=%+v err=%v", resolved.result, resolved.err)
	}
	if resolved.result.Outcome.Status != TerminalSucceeded || resolved.result.Resolution.OriginalReceiptID != commitReq.Receipt.ReceiptID {
		t.Fatalf("resolution did not preserve original receipt/outcome: %+v", resolved.result)
	}
	if resolved.result.Snapshot.WriterClaimID != "claim-c2" || resolved.result.Resolution.CutRecordVersion != committed.result.Snapshot.RecordVersion {
		t.Fatalf("wrong R->F cut: committed=%+v resolved=%+v", committed.result.Snapshot, resolved.result)
	}
}

func TestFenceRaceFenceLinearizesBeforeCommit(t *testing.T) {
	store, address, shardFence := newTestStore(t)
	before := mustLoad(t, store, address, shardFence, "claim-c1")
	commitReq := testCommit(address, shardFence, "claim-c1", before.RecordVersion, "unknown-f-before-r", ModelState{"plot": []byte("planted")})
	fenceReq := testFenceRequest(address, shardFence, before, commitReq, "resolve-f-before-r", "claim-c1", "claim-c2")
	scheduled := newScheduledStore(store)

	commitResult := make(chan commitCallResult, 1)
	fenceResult := make(chan fenceCallResult, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		result, err := scheduled.commit(ctx, commitReq)
		commitResult <- commitCallResult{result: result, err: err}
	}()
	go func() {
		result, err := scheduled.fence(ctx, fenceReq)
		fenceResult <- fenceCallResult{result: result, err: err}
	}()
	waitSignal(t, scheduled.commitEntered)
	waitSignal(t, scheduled.fenceEntered)

	close(scheduled.releaseFence)
	resolved := <-fenceResult
	if resolved.err != nil || resolved.result.Status != FenceResolutionNotCommitted {
		t.Fatalf("F result=%+v err=%v", resolved.result, resolved.err)
	}
	if resolved.result.Resolution.CutRecordVersion != before.RecordVersion || resolved.result.Resolution.CutBusinessStateDigest != before.BusinessStateDigest {
		t.Fatalf("wrong F->R cut: before=%+v resolved=%+v", before, resolved.result.Resolution)
	}
	close(scheduled.releaseCommit)
	late := <-commitResult
	if late.err != nil || late.result.Status != CommitWriterClaimMismatch {
		t.Fatalf("late R crossed fence: result=%+v err=%v", late.result, late.err)
	}
	lookup, err := store.LookupIdempotencyOutcome(context.Background(), lookupFor(commitReq))
	if err != nil || lookup.Status != IdempotencyNotFound {
		t.Fatalf("late R left receipt: lookup=%+v err=%v", lookup, err)
	}
}

func TestFenceTimeoutUnknownRetriesSameResolverAndReturnsStableReceipt(t *testing.T) {
	store, address, shardFence := newTestStore(t)
	before := mustLoad(t, store, address, shardFence, "claim-c1")
	original := testCommit(address, shardFence, "claim-c1", before.RecordVersion, "fence-timeout", ModelState{"crop": []byte("remaining=8")})
	fenceReq := testFenceRequest(address, shardFence, before, original, "resolve-stable", "claim-c1", "claim-c2")
	faults := NewFaultStore(store)
	faults.Inject(MethodFenceAndClaim, FaultTimeoutUnknownAfterApply)

	unknown, err := faults.FenceAndClaim(context.Background(), fenceReq)
	if err != nil || unknown.Status != FenceResolutionTimeoutUnknown {
		t.Fatalf("unknown=%+v err=%v", unknown, err)
	}
	cut, ok := store.Inspect(address)
	if !ok || cut.WriterClaimID != "claim-c2" {
		t.Fatalf("hidden fence was not applied: snapshot=%+v ok=%v", cut, ok)
	}
	retried, err := store.FenceAndClaim(context.Background(), fenceReq)
	if err != nil || retried.Status != FenceResolutionNotCommitted || retried.Resolution.ResolveOperationID != fenceReq.ResolveOperationID {
		t.Fatalf("retry=%+v err=%v", retried, err)
	}

	later := testCommit(address, shardFence, "claim-c2", retried.Snapshot.RecordVersion, "c2-later", ModelState{"crop": []byte("remaining=7")})
	if got, err := store.Commit(context.Background(), later); err != nil || got.Status != CommitCommitted {
		t.Fatalf("later=%+v err=%v", got, err)
	}
	stable, err := store.FenceAndClaim(context.Background(), fenceReq)
	if err != nil || stable.Status != FenceResolutionNotCommitted || stable.Resolution.CutRecordVersion != retried.Resolution.CutRecordVersion || stable.Resolution.CutBusinessStateDigest != retried.Resolution.CutBusinessStateDigest {
		t.Fatalf("resolution drifted after C2 write: first=%+v stable=%+v err=%v", retried.Resolution, stable.Resolution, err)
	}
	conflict := fenceReq
	conflict.NewWriterClaimID = "claim-c3"
	if got, err := store.FenceAndClaim(context.Background(), conflict); err != nil || got.Status != FenceResolutionReconcileRequired {
		t.Fatalf("conflicting resolver identity got=%+v err=%v", got, err)
	}
}

func TestFenceTimeoutUnknownBeforeApplyDoesNotMoveClaimUntilSameRequestRetries(t *testing.T) {
	store, address, shardFence := newTestStore(t)
	before := mustLoad(t, store, address, shardFence, "claim-c1")
	original := testCommit(address, shardFence, "claim-c1", before.RecordVersion, "fence-before-apply", ModelState{"plot": []byte("planted")})
	fenceReq := testFenceRequest(address, shardFence, before, original, "resolve-before-apply", "claim-c1", "claim-c2")
	faults := NewFaultStore(store)
	faults.Inject(MethodFenceAndClaim, FaultTimeoutUnknownBeforeApply)

	unknown, err := faults.FenceAndClaim(context.Background(), fenceReq)
	if err != nil || unknown.Status != FenceResolutionTimeoutUnknown {
		t.Fatalf("unknown=%+v err=%v", unknown, err)
	}
	unchanged, ok := store.Inspect(address)
	if !ok || unchanged.WriterClaimID != "claim-c1" || unchanged.RecordVersion != before.RecordVersion {
		t.Fatalf("before-apply timeout changed authority: snapshot=%+v ok=%v", unchanged, ok)
	}
	retried, err := store.FenceAndClaim(context.Background(), fenceReq)
	if err != nil || retried.Status != FenceResolutionNotCommitted || retried.Snapshot.WriterClaimID != "claim-c2" {
		t.Fatalf("same resolver retry=%+v err=%v", retried, err)
	}
}

func testFenceRequest(address ActorAddress, shardFence ShardWriteFence, before AuthoritySnapshot, original AuthorityCommitRequest, resolveID, oldClaim, newClaim string) FenceAndClaimRequest {
	return FenceAndClaimRequest{
		ResolveOperationID:        resolveID,
		ActorAddress:              address,
		AuthorityLayoutVersion:    "auth-v1",
		ShardWriteFence:           shardFence,
		OldWriterClaimID:          oldClaim,
		NewWriterClaimID:          newClaim,
		OriginalIdempotencyKey:    original.IdempotencyKey,
		CanonicalPayloadHash:      original.CanonicalPayloadHash,
		BeforeRecordVersion:       before.RecordVersion,
		BeforeBusinessStateDigest: before.BusinessStateDigest,
	}
}

func waitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scheduled operation to reach its gate")
	}
}
