package authoritystore

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"farmgame/internal/workidentity"
)

func testAttachedSchedule(t *testing.T, operationID string, generation uint64, dueAtMs int64, payload string) AttachedSchedule {
	t.Helper()
	scheduleID, err := workidentity.StableOpaqueID("schedule", "FRIEND_OPPORTUNITY", operationID)
	if err != nil {
		t.Fatal(err)
	}
	return AttachedSchedule{
		ScheduleID:   scheduleID,
		ScheduleType: "FRIEND_OPPORTUNITY",
		Generation:   generation,
		DueAtMs:      dueAtMs,
		Payload:      []byte(payload),
	}
}

func TestMemoryAttachedScheduleGenerationAndCounters(t *testing.T) {
	store, address, fence := newTestStore(t)
	loaded := mustLoad(t, store, address, fence, "claim-schedule")
	firstSchedule := testAttachedSchedule(t, "schedule-life", 1, 10_000, "wake-v1")
	firstRequest := testCommit(address, fence, "claim-schedule", loaded.RecordVersion, "schedule-v1", ModelState{"state": []byte("v1")})
	firstRequest.AttachedSchedules = []AttachedSchedule{firstSchedule}
	first, err := store.Commit(context.Background(), firstRequest)
	if err != nil || first.Status != CommitCommitted {
		t.Fatalf("first schedule commit=%+v err=%v", first, err)
	}

	usageCount, usageBytes, ok := store.PendingScheduleUsage(address)
	if !ok || usageCount != 1 || usageBytes != len(firstSchedule.Payload) {
		t.Fatalf("initial pending schedule usage=(%d,%d,%v)", usageCount, usageBytes, ok)
	}
	claimed, err := store.ClaimScheduledWork(context.Background(), address, firstSchedule.ScheduleType, firstSchedule.ScheduleID, "worker-1", "boot-1", "token-1", 30_000)
	if err != nil || claimed.Status != ScheduleWorkClaimed || claimed.Snapshot.WorkVersion != 1 || claimed.Snapshot.Generation != 1 {
		t.Fatalf("schedule claim=%+v err=%v", claimed, err)
	}

	secondSchedule := testAttachedSchedule(t, "schedule-life", 2, 20_000, "wake-v2")
	secondRequest := testCommit(address, fence, "claim-schedule", first.Snapshot.RecordVersion, "schedule-v2", ModelState{"state": []byte("v2")})
	secondRequest.AttachedSchedules = []AttachedSchedule{secondSchedule}
	second, err := store.Commit(context.Background(), secondRequest)
	if err != nil || second.Status != CommitCommitted {
		t.Fatalf("supersede commit=%+v err=%v", second, err)
	}
	current, ok := store.InspectSchedule(address, secondSchedule.ScheduleType, secondSchedule.ScheduleID)
	if !ok || current.Generation != 2 || current.WorkVersion != 2 || current.State != "PENDING" || current.LeaseHolderID != "" {
		t.Fatalf("superseded schedule=%+v found=%v", current, ok)
	}
	usageCount, usageBytes, _ = store.PendingScheduleUsage(address)
	if usageCount != 1 || usageBytes != len(secondSchedule.Payload) {
		t.Fatalf("superseded pending schedule usage=(%d,%d)", usageCount, usageBytes)
	}
	oldAdvance, err := store.AdvanceScheduledWork(context.Background(), address, firstSchedule.ScheduleType, firstSchedule.ScheduleID,
		"worker-1", "boot-1", claimed.Snapshot.LeaseEpoch, claimed.Snapshot.WorkVersion, claimed.Snapshot.Generation,
		"token-1", 30_000, []byte("old-worker"), PayloadHash{} /* replaced below */)
	if err == nil || oldAdvance.Status != ScheduleWorkUnknown {
		// The payload hash is intentionally invalid in this first call; verify
		// input validation is fail-closed before exercising the stale fence below.
		t.Fatalf("invalid old advance=%+v err=%v", oldAdvance, err)
	}
	oldPayloadHash := schedulePayloadHash([]byte("old-worker"))
	oldAdvance, err = store.AdvanceScheduledWork(context.Background(), address, firstSchedule.ScheduleType, firstSchedule.ScheduleID,
		"worker-1", "boot-1", claimed.Snapshot.LeaseEpoch, claimed.Snapshot.WorkVersion, claimed.Snapshot.Generation,
		"token-1", 30_000, []byte("old-worker"), oldPayloadHash)
	if err != nil || oldAdvance.Status != ScheduleWorkStaleGeneration {
		t.Fatalf("stale old advance=%+v err=%v", oldAdvance, err)
	}

	staleSchedule := testAttachedSchedule(t, "schedule-life", 1, 1, "stale-content")
	staleRequest := testCommit(address, fence, "claim-schedule", second.Snapshot.RecordVersion, "schedule-stale", ModelState{"state": []byte("v3")})
	staleRequest.AttachedSchedules = []AttachedSchedule{staleSchedule}
	staleResult, err := store.Commit(context.Background(), staleRequest)
	if err != nil || staleResult.Status != CommitCommitted {
		t.Fatalf("stale schedule must not fence actor commit=%+v err=%v", staleResult, err)
	}
	unchanged, _ := store.InspectSchedule(address, secondSchedule.ScheduleType, secondSchedule.ScheduleID)
	if unchanged.Generation != 2 || string(unchanged.Payload) != string(secondSchedule.Payload) {
		t.Fatalf("stale schedule changed current=%+v", unchanged)
	}

	cancel := testAttachedSchedule(t, "schedule-life", 3, 0, "cancel-v3")
	cancelRequest := testCommit(address, fence, "claim-schedule", staleResult.Snapshot.RecordVersion, "schedule-cancel", ModelState{"state": []byte("v4")})
	cancelRequest.AttachedSchedules = []AttachedSchedule{cancel}
	cancelResult, err := store.Commit(context.Background(), cancelRequest)
	if err != nil || cancelResult.Status != CommitCommitted {
		t.Fatalf("cancel commit=%+v err=%v", cancelResult, err)
	}
	cancelled, _ := store.InspectSchedule(address, cancel.ScheduleType, cancel.ScheduleID)
	if cancelled.State != "SUPERSEDED" || cancelled.DueAtMs != 0 || cancelled.LeaseHolderID != "" || cancelled.WorkVersion != 3 {
		t.Fatalf("cancelled schedule=%+v", cancelled)
	}
	usageCount, usageBytes, _ = store.PendingScheduleUsage(address)
	if usageCount != 0 || usageBytes != 0 {
		t.Fatalf("cancelled pending schedule usage=(%d,%d)", usageCount, usageBytes)
	}

	reactivate := testAttachedSchedule(t, "schedule-life", 4, 30_000, "wake-v4")
	reactivateRequest := testCommit(address, fence, "claim-schedule", cancelResult.Snapshot.RecordVersion, "schedule-reactivate", ModelState{"state": []byte("v5")})
	reactivateRequest.AttachedSchedules = []AttachedSchedule{reactivate}
	reactivateResult, err := store.Commit(context.Background(), reactivateRequest)
	if err != nil || reactivateResult.Status != CommitCommitted {
		t.Fatalf("reactivate commit=%+v err=%v", reactivateResult, err)
	}
	usageCount, usageBytes, _ = store.PendingScheduleUsage(address)
	if usageCount != 1 || usageBytes != len(reactivate.Payload) {
		t.Fatalf("reactivated pending schedule usage=(%d,%d)", usageCount, usageBytes)
	}
	reactivated, _ := store.InspectSchedule(address, reactivate.ScheduleType, reactivate.ScheduleID)
	if reactivated.State != "PENDING" || reactivated.Generation != 4 || reactivated.WorkVersion != 4 {
		t.Fatalf("reactivated schedule=%+v", reactivated)
	}

	finalClaim, err := store.ClaimScheduledWork(context.Background(), address, reactivate.ScheduleType, reactivate.ScheduleID, "worker-2", "boot-2", "token-2", 30_000)
	if err != nil || finalClaim.Status != ScheduleWorkNotDue {
		t.Fatalf("future reactivated claim=%+v err=%v", finalClaim, err)
	}
}

func TestMemoryAttachedScheduleSameGenerationConflict(t *testing.T) {
	store, address, fence := newTestStore(t)
	loaded := mustLoad(t, store, address, fence, "claim-schedule-conflict")
	schedule := testAttachedSchedule(t, "schedule-conflict", 1, 10_000, "same")
	firstRequest := testCommit(address, fence, "claim-schedule-conflict", loaded.RecordVersion, "schedule-conflict-v1", ModelState{"state": []byte("v1")})
	firstRequest.AttachedSchedules = []AttachedSchedule{schedule}
	first, err := store.Commit(context.Background(), firstRequest)
	if err != nil {
		t.Fatal(err)
	}
	conflict := testAttachedSchedule(t, "schedule-conflict", 1, 10_000, "different")
	conflictRequest := testCommit(address, fence, "claim-schedule-conflict", first.Snapshot.RecordVersion, "schedule-conflict-v2", ModelState{"state": []byte("v2")})
	conflictRequest.AttachedSchedules = []AttachedSchedule{conflict}
	if _, err := store.Commit(context.Background(), conflictRequest); !errors.Is(err, ErrScheduleConflict) {
		t.Fatalf("same-generation conflict err=%v", err)
	}
	unchanged, _ := store.InspectSchedule(address, schedule.ScheduleType, schedule.ScheduleID)
	if unchanged.Generation != 1 || string(unchanged.Payload) != "same" {
		t.Fatalf("conflicting schedule mutated=%+v", unchanged)
	}
}

func schedulePayloadHash(payload []byte) PayloadHash {
	return PayloadHash(sha256.Sum256(payload))
}
