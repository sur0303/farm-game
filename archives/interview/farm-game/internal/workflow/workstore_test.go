package workflow

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/workidentity"
)

func testAddress(workType, operationID string) workidentity.WorkAddress {
	workID, err := workidentity.StableOpaqueID("test-work-id-v1", workType, operationID)
	if err != nil {
		panic(err)
	}
	return workidentity.WorkAddress{
		WorkID:        workID,
		WorkType:      workType,
		PlacementKind: workidentity.IndependentWorkBucket,
		BucketVersion: 1,
		BucketID:      42,
	}
}

func testPayloadHash(seed string) [32]byte { return sha256.Sum256([]byte(seed)) }

func testWorkRecord(t *testing.T, repo *MemoryWorkRepository, engine *Engine, def Definition, operationID string, now time.Time) *WorkRecord {
	t.Helper()
	record, created, err := engine.Create(context.Background(), def, testAddress(def.WorkType(), operationID),
		9001, operationID, []byte(fmt.Sprintf(`{"operation":%q}`, operationID)), testPayloadHash(operationID), time.Time{})
	if err != nil || !created {
		t.Fatalf("create=%v created=%v err=%v", record, created, err)
	}
	return record
}

func TestMemoryWorkRepositoryCreateIfAbsentIsAtomic(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	repo := NewMemoryWorkRepository(func() time.Time { return now })
	candidate, err := NewWorkRecord(testAddress("TEST_WORKFLOW", "op-1"), 9001, "op-1",
		[]byte(`{"a":1}`), testPayloadHash("op-1"), []string{"STEP_A"}, now, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	var createdCount atomic.Int32
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			record, created, err := repo.CreateIfAbsent(context.Background(), candidate)
			if err != nil || record.WorkVersion != 1 {
				t.Errorf("create=%v err=%v", record, err)
				return
			}
			if created {
				createdCount.Add(1)
			}
		}()
	}
	wg.Wait()
	if createdCount.Load() != 1 {
		t.Fatalf("created=%d, want 1", createdCount.Load())
	}

	conflict, err := NewWorkRecord(testAddress("TEST_WORKFLOW", "op-1"), 9001, "op-1",
		[]byte(`{"a":2}`), testPayloadHash("op-1-different"), []string{"STEP_A"}, now, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.CreateIfAbsent(context.Background(), conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different intent accepted: %v", err)
	}
}

func TestMemoryWorkRepositoryAdvanceCASAndTerminal(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	repo := NewMemoryWorkRepository(func() time.Time { return now })
	record, _, err := repo.CreateIfAbsent(context.Background(), mustWorkRecord(t, "op-cas", now))
	if err != nil {
		t.Fatal(err)
	}
	next := cloneWorkRecord(record)
	next.State = WorkStateRunning
	if _, err := repo.Advance(context.Background(), next, AdvanceOptions{ExpectedWorkVersion: record.WorkVersion + 1}); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale version accepted: %v", err)
	}
	updated, err := repo.Advance(context.Background(), next, AdvanceOptions{ExpectedWorkVersion: record.WorkVersion})
	if err != nil || updated.WorkVersion != 2 {
		t.Fatalf("advance=%v err=%v", updated, err)
	}
	terminal := cloneWorkRecord(updated)
	terminal.State = WorkStateCompleted
	completed, err := repo.Advance(context.Background(), terminal, AdvanceOptions{ExpectedWorkVersion: updated.WorkVersion})
	if err != nil || completed.CompletedAtMs == 0 {
		t.Fatalf("complete=%v err=%v", completed, err)
	}
	if _, err := repo.Advance(context.Background(), cloneWorkRecord(completed), AdvanceOptions{ExpectedWorkVersion: completed.WorkVersion}); !errors.Is(err, ErrTerminal) {
		t.Fatalf("terminal record mutated: %v", err)
	}
	if pending, _ := repo.PendingDue(context.Background()); pending != 0 {
		t.Fatalf("terminal record still due: %d", pending)
	}
}

func TestMemoryWorkRepositoryClaimLeaseFenceAndReclaim(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	repo := NewMemoryWorkRepository(func() time.Time { return now })
	record, _, err := repo.CreateIfAbsent(context.Background(), mustWorkRecord(t, "op-lease", now))
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := repo.ClaimDue(context.Background(), "worker-a", "token-a", 4, 5*time.Second)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claimed=%v err=%v", claimed, err)
	}
	leaseA := claimed[0].Record
	if leaseA.LeaseOwner != "worker-a" || leaseA.LeaseEpoch != 1 || leaseA.WorkVersion != record.WorkVersion+1 {
		t.Fatalf("lease=%v", leaseA)
	}
	if again, _ := repo.ClaimDue(context.Background(), "worker-b", "token-b", 4, 5*time.Second); len(again) != 0 {
		t.Fatalf("live lease did not hide record: %v", again)
	}

	// Online (unleased) advance is rejected while the worker lease is live.
	online := cloneWorkRecord(leaseA)
	online.State = WorkStateRunning
	if _, err := repo.Advance(context.Background(), online, AdvanceOptions{ExpectedWorkVersion: leaseA.WorkVersion}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("unleased advance bypassed lease: %v", err)
	}

	// Lease expiry makes the work discoverable again with a higher epoch.
	now = now.Add(6 * time.Second)
	reclaimed, err := repo.ClaimDue(context.Background(), "worker-b", "token-b", 4, 5*time.Second)
	if err != nil || len(reclaimed) != 1 {
		t.Fatalf("reclaimed=%v err=%v", reclaimed, err)
	}
	leaseB := reclaimed[0].Record
	if leaseB.LeaseEpoch != 2 || leaseB.LeaseOwner != "worker-b" {
		t.Fatalf("reclaim=%v", leaseB)
	}
	staleA := cloneWorkRecord(leaseA)
	staleA.State = WorkStateRunning
	if _, err := repo.Advance(context.Background(), staleA, AdvanceOptions{
		ExpectedWorkVersion: leaseA.WorkVersion,
		LeaseOwner:          leaseA.LeaseOwner,
		LeaseEpoch:          leaseA.LeaseEpoch,
	}); !errors.Is(err, ErrLeaseLost) && !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale worker advanced after reclaim: %v", err)
	}
}

func TestMemoryWorkRepositoryConcurrentClaimSingleWinner(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	repo := NewMemoryWorkRepository(func() time.Time { return now })
	if _, _, err := repo.CreateIfAbsent(context.Background(), mustWorkRecord(t, "op-race", now)); err != nil {
		t.Fatal(err)
	}
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			claimed, err := repo.ClaimDue(context.Background(), fmt.Sprintf("worker-%d", i), "token", 4, time.Minute)
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			wins.Add(int32(len(claimed)))
		}(i)
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("claims=%d, want exactly 1", wins.Load())
	}
}

func TestMemoryWorkRepositoryRenewLease(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	repo := NewMemoryWorkRepository(func() time.Time { return now })
	if _, _, err := repo.CreateIfAbsent(context.Background(), mustWorkRecord(t, "op-renew", now)); err != nil {
		t.Fatal(err)
	}
	claimed, err := repo.ClaimDue(context.Background(), "worker-a", "token-a", 1, 5*time.Second)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claimed=%v err=%v", claimed, err)
	}
	lease := claimed[0].Record
	if _, err := repo.RenewLease(context.Background(), lease.Address.WorkType, lease.Address.WorkID,
		"worker-a", "wrong-token", lease.LeaseEpoch, 5*time.Second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("wrong token renewed: %v", err)
	}
	now = now.Add(2 * time.Second)
	renewed, err := repo.RenewLease(context.Background(), lease.Address.WorkType, lease.Address.WorkID,
		"worker-a", "token-a", lease.LeaseEpoch, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.WorkVersion != lease.WorkVersion || renewed.LeaseExpireAtMs != now.Add(5*time.Second).UnixMilli() {
		t.Fatalf("renew mutated version or wrong expiry: %v", renewed)
	}
}

func mustWorkRecord(t *testing.T, operationID string, now time.Time) *WorkRecord {
	t.Helper()
	record, err := NewWorkRecord(testAddress("TEST_WORKFLOW", operationID), 9001, operationID,
		[]byte(fmt.Sprintf(`{"operation":%q}`, operationID)), testPayloadHash(operationID),
		[]string{"STEP_A", "STEP_B"}, now, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	return record
}
