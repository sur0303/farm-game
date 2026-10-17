package authority

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/workidentity"
)

func testStoreWithScheduleDueIndexMode(t *testing.T, rig *testRig, mode ScheduleDueIndexMode) *Store {
	t.Helper()
	store, err := NewStore(rig.client, nil, Options{
		Profile: testProfile(t), Resolver: testCalculator(t), ScheduleDueIndexMode: mode,
		ScheduleDueIndexDedicatedReady: mode == ScheduleDueIndexDedicatedOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestScheduleDueIndexModeParsing(t *testing.T) {
	for _, raw := range []string{"", "shared", "DUAL_SHADOW", "dedicated_fallback", "dedicated_only"} {
		if _, err := ParseScheduleDueIndexMode(raw); err != nil {
			t.Fatalf("mode %q: %v", raw, err)
		}
	}
	if _, err := ParseScheduleDueIndexMode("typo"); err == nil {
		t.Fatal("unknown mode must fail closed")
	}
}

func TestScheduleDueIndexDedicatedOnlyRequiresReadinessProof(t *testing.T) {
	rig := newTestRig(t)
	if _, err := NewStore(rig.client, nil, Options{
		Profile: testProfile(t), Resolver: testCalculator(t), ScheduleDueIndexMode: ScheduleDueIndexDedicatedOnly,
	}); err == nil {
		t.Fatal("dedicated-only store started without migration readiness proof")
	}
}

func TestScheduleDueIndexDualLifecycle(t *testing.T) {
	rig := newTestRigWithScheduleDueIndexMode(t, ScheduleDueIndexDualShadow)
	loaded := rig.load(t, "schedule-dual-lifecycle")
	schedule := testSchedule(t, "schedule-dual-lifecycle", 1, testNow.UnixMilli(), "wake")
	req := rig.commitReq("schedule-dual-lifecycle", loaded.RecordVersion, "schedule-dual-create", authoritystore.ModelState{"state": []byte("v1")})
	req.AttachedSchedules = []authoritystore.AttachedSchedule{schedule}
	if result, err := rig.store.Commit(context.Background(), req); err != nil || result.Status != authoritystore.CommitCommitted {
		t.Fatalf("commit=%+v err=%v", result, err)
	}

	shared, _ := rig.store.keys.SourceDueIndex(rig.bucket)
	dedicated, _ := rig.store.keys.SourceScheduleDueIndex(rig.bucket)
	member, err := workidentity.EncodeScheduleRefV1(schedule.ScheduleType, schedule.ScheduleID)
	if err != nil {
		t.Fatal(err)
	}
	assertScore := func(key string, want int64) {
		t.Helper()
		got, scoreErr := rig.client.ZScore(context.Background(), key, member).Result()
		if scoreErr != nil || int64(got) != want {
			t.Fatalf("key=%s score=%v want=%d err=%v", key, got, want, scoreErr)
		}
	}
	assertScore(shared.String(), schedule.DueAtMs)
	assertScore(dedicated.String(), schedule.DueAtMs)

	lease := rig.store.WorkLease()
	claim, err := lease.ClaimScheduledWorkWithBoot(context.Background(), rig.bucket, schedule.ScheduleType, schedule.ScheduleID,
		"worker", "boot", "token", 30_000)
	if err != nil || claim.Status != WorkClaimClaimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	nextDue := testNow.Add(2 * time.Second).UnixMilli()
	payload := []byte("wake-next")
	advanced, err := lease.AdvanceScheduledWork(context.Background(), rig.bucket, schedule.ScheduleType, schedule.ScheduleID,
		"worker", "boot", claim.LeaseEpoch, claim.WorkVersion, claim.Generation, "token", nextDue, payload, fmt.Sprintf("%x", sha256Sum(payload)))
	if err != nil || advanced.Status != WorkLeaseRenewed {
		t.Fatalf("advance=%+v err=%v", advanced, err)
	}
	assertScore(shared.String(), nextDue)
	assertScore(dedicated.String(), nextDue)

	rig.server.SetTime(time.UnixMilli(nextDue))
	claim, err = lease.ClaimScheduledWorkWithBoot(context.Background(), rig.bucket, schedule.ScheduleType, schedule.ScheduleID,
		"worker-2", "boot-2", "token-2", 30_000)
	if err != nil || claim.Status != WorkClaimClaimed {
		t.Fatalf("second claim=%+v err=%v", claim, err)
	}
	completed, err := lease.CompleteScheduledWork(context.Background(), rig.bucket, schedule.ScheduleType, schedule.ScheduleID,
		"worker-2", "boot-2", claim.LeaseEpoch, claim.WorkVersion, claim.Generation, "token-2")
	if err != nil || completed.Status != WorkLeaseCompleted {
		t.Fatalf("complete=%+v err=%v", completed, err)
	}
	for _, key := range []string{shared.String(), dedicated.String()} {
		if _, scoreErr := rig.client.ZScore(context.Background(), key, member).Result(); scoreErr != redis.Nil {
			t.Fatalf("completed member remains in %s: %v", key, scoreErr)
		}
	}
}

func sha256Sum(payload []byte) [32]byte {
	return sha256.Sum256(payload)
}

func TestScheduleDueIndexFallbackMergesByGlobalDueOrder(t *testing.T) {
	rig := newTestRigWithScheduleDueIndexMode(t, ScheduleDueIndexDedicatedFallback)
	shared, _ := rig.store.keys.SourceDueIndex(rig.bucket)
	dedicated, _ := rig.store.keys.SourceScheduleDueIndex(rig.bucket)
	early := testSchedule(t, "fallback-early", 1, testNow.UnixMilli(), "early")
	late := testSchedule(t, "fallback-late", 1, testNow.Add(time.Second).UnixMilli(), "late")
	earlyRef, _ := workidentity.EncodeScheduleRefV1(early.ScheduleType, early.ScheduleID)
	lateRef, _ := workidentity.EncodeScheduleRefV1(late.ScheduleType, late.ScheduleID)
	if err := rig.client.ZAdd(context.Background(), shared.String(),
		redis.Z{Score: float64(early.DueAtMs), Member: earlyRef}, redis.Z{Score: float64(late.DueAtMs), Member: lateRef}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rig.client.ZAdd(context.Background(), dedicated.String(), redis.Z{Score: float64(late.DueAtMs), Member: lateRef}).Err(); err != nil {
		t.Fatal(err)
	}
	refs, stats, err := rig.store.WorkLease().ListDueScheduledWorkWithStats(context.Background(), rig.bucket, late.DueAtMs, 1)
	if err != nil || len(refs) != 1 || refs[0].Member != earlyRef {
		t.Fatalf("refs=%+v stats=%+v err=%v", refs, stats, err)
	}
	if stats.FallbackHits != 1 {
		t.Fatalf("fallback hits=%d want=1", stats.FallbackHits)
	}
}

func TestScheduleDueIndexDedicatedOnlyIgnoresOrdinaryOrderPrefix(t *testing.T) {
	rig := newTestRigWithScheduleDueIndexMode(t, ScheduleDueIndexDedicatedOnly)
	shared, _ := rig.store.keys.SourceDueIndex(rig.bucket)
	dedicated, _ := rig.store.keys.SourceScheduleDueIndex(rig.bucket)
	entries := make([]redis.Z, 100_000)
	for index := range entries {
		entries[index] = redis.Z{Score: float64(testNow.UnixMilli()), Member: fmt.Sprintf("wr1_noise_%06d", index)}
	}
	if err := rig.client.ZAdd(context.Background(), shared.String(), entries...).Err(); err != nil {
		t.Fatal(err)
	}
	schedule := testSchedule(t, "dedicated-only", 1, testNow.UnixMilli(), "wake")
	member, _ := workidentity.EncodeScheduleRefV1(schedule.ScheduleType, schedule.ScheduleID)
	if err := rig.client.ZAdd(context.Background(), dedicated.String(), redis.Z{Score: float64(schedule.DueAtMs), Member: member}).Err(); err != nil {
		t.Fatal(err)
	}
	refs, stats, err := rig.store.WorkLease().ListDueScheduledWorkWithStats(context.Background(), rig.bucket, schedule.DueAtMs, 1)
	if err != nil || len(refs) != 1 || refs[0].Member != member || stats.DedicatedHits != 1 || stats.FallbackHits != 0 {
		t.Fatalf("refs=%+v stats=%+v err=%v", refs, stats, err)
	}
}

func TestScheduleDueIndexDedicatedOnlyCommitDoesNotRetainSharedMember(t *testing.T) {
	rig := newTestRigWithScheduleDueIndexMode(t, ScheduleDueIndexDedicatedOnly)
	loaded := rig.load(t, "schedule-dedicated-commit")
	schedule := testSchedule(t, "schedule-dedicated-commit", 1, testNow.UnixMilli(), "wake")
	req := rig.commitReq("schedule-dedicated-commit", loaded.RecordVersion, "schedule-dedicated-create", authoritystore.ModelState{"state": []byte("v1")})
	req.AttachedSchedules = []authoritystore.AttachedSchedule{schedule}
	if _, err := rig.store.Commit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	member, _ := workidentity.EncodeScheduleRefV1(schedule.ScheduleType, schedule.ScheduleID)
	shared, _ := rig.store.keys.SourceDueIndex(rig.bucket)
	dedicated, _ := rig.store.keys.SourceScheduleDueIndex(rig.bucket)
	if _, err := rig.client.ZScore(context.Background(), shared.String(), member).Result(); err != redis.Nil {
		t.Fatalf("dedicated-only retained shared member: %v", err)
	}
	if _, err := rig.client.ZScore(context.Background(), dedicated.String(), member).Result(); err != nil {
		t.Fatalf("dedicated member missing: %v", err)
	}
}

func TestScheduleDueIndexBackfillIsIdempotentAndFenced(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "schedule-backfill")
	schedule := testSchedule(t, "schedule-backfill", 1, testNow.UnixMilli(), "wake")
	req := rig.commitReq("schedule-backfill", loaded.RecordVersion, "schedule-backfill-create", authoritystore.ModelState{"state": []byte("v1")})
	req.AttachedSchedules = []authoritystore.AttachedSchedule{schedule}
	result, err := rig.store.Commit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	fallbackStore := testStoreWithScheduleDueIndexMode(t, rig, ScheduleDueIndexDedicatedFallback)
	stats, err := fallbackStore.WorkLease().BackfillScheduleDueIndexPage(context.Background(), rig.bucket, 0, 32)
	if err != nil || stats.Scanned != 1 || stats.Repaired != 1 {
		t.Fatalf("first backfill=%+v err=%v", stats, err)
	}
	stats, err = fallbackStore.WorkLease().BackfillScheduleDueIndexPage(context.Background(), rig.bucket, 0, 32)
	if err != nil || stats.Scanned != 1 || stats.Repaired != 1 {
		t.Fatalf("idempotent backfill=%+v err=%v", stats, err)
	}

	newDue := testNow.Add(5 * time.Second).UnixMilli()
	higher := testSchedule(t, "schedule-backfill", 2, newDue, "wake-v2")
	update := rig.commitReq("schedule-backfill", result.Snapshot.RecordVersion, "schedule-backfill-update", authoritystore.ModelState{"state": []byte("v2")})
	update.AttachedSchedules = []authoritystore.AttachedSchedule{higher}
	if _, err := rig.store.Commit(context.Background(), update); err != nil {
		t.Fatal(err)
	}
	member, _ := workidentity.EncodeScheduleRefV1(schedule.ScheduleType, schedule.ScheduleID)
	repaired, err := fallbackStore.WorkLease().BackfillScheduleDueIndex(context.Background(), rig.bucket, ScheduledDueRef{
		ScheduleType: schedule.ScheduleType, ScheduleID: schedule.ScheduleID, DueAtMs: schedule.DueAtMs, Member: member,
	})
	if err != nil || !repaired {
		t.Fatalf("stale-score reconcile repaired=%v err=%v", repaired, err)
	}
	dedicated, _ := rig.store.keys.SourceScheduleDueIndex(rig.bucket)
	score, err := rig.client.ZScore(context.Background(), dedicated.String(), member).Result()
	if err != nil || int64(score) != newDue {
		t.Fatalf("dedicated score=%v want=%d err=%v", score, newDue, err)
	}

	rig.server.SetTime(time.UnixMilli(newDue))
	claim, err := rig.store.WorkLease().ClaimScheduledWorkWithBoot(context.Background(), rig.bucket, higher.ScheduleType, higher.ScheduleID,
		"old-worker", "old-boot", "old-token", 30_000)
	if err != nil || claim.Status != WorkClaimClaimed {
		t.Fatalf("legacy claim=%+v err=%v", claim, err)
	}
	completed, err := rig.store.WorkLease().CompleteScheduledWork(context.Background(), rig.bucket, higher.ScheduleType, higher.ScheduleID,
		"old-worker", "old-boot", claim.LeaseEpoch, claim.WorkVersion, claim.Generation, "old-token")
	if err != nil || completed.Status != WorkLeaseCompleted {
		t.Fatalf("legacy complete=%+v err=%v", completed, err)
	}
	if _, err := rig.client.ZScore(context.Background(), dedicated.String(), member).Result(); err != nil {
		t.Fatalf("fixture expected stale dedicated member: %v", err)
	}
	if repaired, err = fallbackStore.WorkLease().BackfillScheduleDueIndex(context.Background(), rig.bucket, ScheduledDueRef{
		ScheduleType: schedule.ScheduleType, ScheduleID: schedule.ScheduleID, DueAtMs: newDue, Member: member,
	}); err != nil || repaired {
		t.Fatalf("completed reconcile repaired=%v err=%v", repaired, err)
	}
	if _, err := rig.client.ZScore(context.Background(), dedicated.String(), member).Result(); err != redis.Nil {
		t.Fatalf("completed schedule was resurrected: %v", err)
	}
}

func TestScheduleDueIndexBackfillRejectsMismatchedMemberIdentity(t *testing.T) {
	rig := newTestRig(t)
	store := testStoreWithScheduleDueIndexMode(t, rig, ScheduleDueIndexDualShadow)
	differentID, err := workidentity.StableOpaqueID("schedule", "FRIEND_OPPORTUNITY", "different-schedule")
	if err != nil {
		t.Fatal(err)
	}
	expectedID, err := workidentity.StableOpaqueID("schedule", "FRIEND_OPPORTUNITY", "expected-schedule")
	if err != nil {
		t.Fatal(err)
	}
	member, err := workidentity.EncodeScheduleRefV1("FRIEND_OPPORTUNITY", differentID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.WorkLease().BackfillScheduleDueIndex(context.Background(), rig.bucket, ScheduledDueRef{
		ScheduleType: "FRIEND_OPPORTUNITY",
		ScheduleID:   expectedID,
		DueAtMs:      testNow.UnixMilli(),
		Member:       member,
	}); err == nil {
		t.Fatal("mismatched schedule member identity must fail closed")
	}
}

func TestScheduleDueIndexBackfillRequiresMigrationMode(t *testing.T) {
	rig := newTestRig(t)
	for _, mode := range []ScheduleDueIndexMode{ScheduleDueIndexShared, ScheduleDueIndexDedicatedOnly} {
		store := testStoreWithScheduleDueIndexMode(t, rig, mode)
		if _, err := store.WorkLease().BackfillScheduleDueIndexPage(context.Background(), rig.bucket, 0, 32); err == nil {
			t.Fatalf("mode %s allowed migration backfill", mode)
		}
	}
}
