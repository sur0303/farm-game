package workflow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	workflowpb "farmgame/api/gen/workflow"
)

func testHash(value byte) []byte {
	return []byte{value, value + 1, value + 2}
}

func testWorkflowRecord(operationID string, kind workflowpb.WorkflowKind, farmID, initiator uint64, payloadHash []byte, now time.Time) *workflowpb.WorkflowRecord {
	workflowID, err := StableID(initiator, kind, operationID)
	if err != nil {
		panic(err)
	}
	return NewRecord(workflowID, operationID, kind, farmID, initiator, payloadHash, now)
}

func TestMemoryStoreCreateOrLoadIsAtomicAndBindsIntent(t *testing.T) {
	store := NewMemoryStore()
	now := time.UnixMilli(1_800_000_000_000)
	candidate := testWorkflowRecord("plant-1", workflowpb.WorkflowKind_PLANT, 9, 9, testHash(1), now)
	var created atomic.Int32
	var failures atomic.Int32
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			record, inserted, err := store.CreateOrLoad(context.Background(), candidate)
			if err != nil || record.WorkflowRevision != 1 {
				failures.Add(1)
				return
			}
			if inserted {
				created.Add(1)
			}
		}()
	}
	wait.Wait()
	if failures.Load() != 0 || created.Load() != 1 {
		t.Fatalf("failures=%d created=%d", failures.Load(), created.Load())
	}

	conflict := testWorkflowRecord("plant-1", workflowpb.WorkflowKind_PLANT, 9, 9, testHash(9), now)
	if _, _, err := store.CreateOrLoad(context.Background(), conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different payload reused workflow id: %v", err)
	}
}

func TestMemoryStoreClaimsWithDurableLeaseAndFencesStaleOwners(t *testing.T) {
	store := NewMemoryStore()
	now := time.UnixMilli(1_800_000_000_000)
	due := testWorkflowRecord("due", workflowpb.WorkflowKind_HARVEST, 9, 9, testHash(1), now)
	due.NextRetryAtMs = now.UnixMilli()
	future := testWorkflowRecord("future", workflowpb.WorkflowKind_THEFT, 9, 10, testHash(2), now)
	future.NextRetryAtMs = now.Add(time.Minute).UnixMilli()
	done := testWorkflowRecord("done", workflowpb.WorkflowKind_PLANT, 9, 9, testHash(3), now)
	for _, record := range []*workflowpb.WorkflowRecord{due, future, done} {
		if _, _, err := store.CreateOrLoad(context.Background(), record); err != nil {
			t.Fatal(err)
		}
	}
	storedDone, _ := store.Get(context.Background(), done.WorkflowId)
	storedDone.State = workflowpb.WorkflowState_COMPLETED
	storedDone.NextRetryAtMs = 0
	if _, err := store.Update(context.Background(), storedDone, UpdateOptions{
		ExpectedRevision: storedDone.WorkflowRevision,
		Now:              now,
	}); err != nil {
		t.Fatal(err)
	}

	claimed, err := store.ClaimDue(context.Background(), "worker-a", now, 10, 5*time.Second)
	if err != nil || len(claimed) != 1 || claimed[0].WorkflowId != due.WorkflowId {
		t.Fatalf("claimed=%v err=%v", claimed, err)
	}
	first := claimed[0]
	if first.WorkflowRevision != 2 || first.LeaseOwner != "worker-a" ||
		first.LeaseEpoch != 1 || first.LeaseUntilMs != now.Add(5*time.Second).UnixMilli() {
		t.Fatalf("claim metadata=%v", first)
	}
	if repeated, err := store.ClaimDue(context.Background(), "worker-b", now.Add(time.Second), 10, 5*time.Second); err != nil || len(repeated) != 0 {
		t.Fatalf("lease did not hide record: %v err=%v", repeated, err)
	}

	staleOnline := clone(first)
	staleOnline.State = workflowpb.WorkflowState_COMPLETED
	if _, err := store.Update(context.Background(), staleOnline, UpdateOptions{
		ExpectedRevision: first.WorkflowRevision,
		Now:              now.Add(time.Second),
	}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("online writer bypassed worker lease: %v", err)
	}

	reclaimed, err := store.ClaimDue(context.Background(), "worker-b", now.Add(6*time.Second), 10, 5*time.Second)
	if err != nil || len(reclaimed) != 1 {
		t.Fatalf("reclaim=%v err=%v", reclaimed, err)
	}
	if reclaimed[0].LeaseEpoch != 2 || reclaimed[0].WorkflowRevision != 3 {
		t.Fatalf("reclaim did not advance fences: %v", reclaimed[0])
	}
	staleWorker := clone(first)
	staleWorker.State = workflowpb.WorkflowState_COMPLETED
	if _, err := store.Update(context.Background(), staleWorker, UpdateOptions{
		ExpectedRevision: first.WorkflowRevision,
		LeaseOwner:       first.LeaseOwner,
		LeaseEpoch:       first.LeaseEpoch,
		Now:              now.Add(6 * time.Second),
	}); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale worker overwrote reclaimed workflow: %v", err)
	}
}

func TestMemoryStoreClaimedUpdateReleasesLease(t *testing.T) {
	store := NewMemoryStore()
	now := time.UnixMilli(1_800_000_000_000)
	record := testWorkflowRecord("plant-1", workflowpb.WorkflowKind_PLANT, 9, 9, testHash(1), now)
	record.NextRetryAtMs = now.UnixMilli()
	if _, _, err := store.CreateOrLoad(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimDue(context.Background(), "worker-a", now, 1, time.Minute)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	next := clone(claimed[0])
	next.State = workflowpb.WorkflowState_RETRY_WAIT
	next.NextRetryAtMs = now.Add(time.Second).UnixMilli()
	updated, err := store.Update(context.Background(), next, UpdateOptions{
		ExpectedRevision: claimed[0].WorkflowRevision,
		LeaseOwner:       claimed[0].LeaseOwner,
		LeaseEpoch:       claimed[0].LeaseEpoch,
		Now:              now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.LeaseOwner != "" || updated.LeaseUntilMs != 0 ||
		updated.LeaseEpoch != 1 || updated.WorkflowRevision != 3 {
		t.Fatalf("update did not release lease while preserving epoch: %v", updated)
	}
}
