package authority

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/workidentity"
)

func testSchedule(t *testing.T, operationID string, generation uint64, dueAtMs int64, payload string) authoritystore.AttachedSchedule {
	t.Helper()
	scheduleID, err := workidentity.StableOpaqueID("schedule", "FRIEND_OPPORTUNITY", operationID)
	if err != nil {
		t.Fatal(err)
	}
	return authoritystore.AttachedSchedule{
		ScheduleID:   scheduleID,
		ScheduleType: "FRIEND_OPPORTUNITY",
		Generation:   generation,
		DueAtMs:      dueAtMs,
		Payload:      []byte(payload),
	}
}

func testScheduleBucketUsage(t *testing.T, rig *testRig) (count, bytes, schedules int64) {
	t.Helper()
	metaKey, err := rig.store.keys.BucketMeta(rig.bucket)
	if err != nil {
		t.Fatal(err)
	}
	values, err := rig.client.HMGet(context.Background(), metaKey.String(), "pending_work_count", "pending_work_bytes", "schedule_count").Result()
	if err != nil {
		t.Fatal(err)
	}
	parse := func(value interface{}) int64 {
		text, ok := value.(string)
		if !ok || text == "" {
			return 0
		}
		parsed, parseErr := strconv.ParseInt(text, 10, 64)
		if parseErr != nil {
			t.Fatalf("bad bucket meta value=%v err=%v", value, parseErr)
		}
		return parsed
	}
	return parse(values[0]), parse(values[1]), parse(values[2])
}

func TestRedisAttachedScheduleDuplicateConflictAndStaleNoop(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "schedule-duplicate-claim")
	schedule := testSchedule(t, "schedule-duplicate", 1, testNow.UnixMilli(), "wake-v1")
	firstRequest := rig.commitReq("schedule-duplicate-claim", loaded.RecordVersion, "schedule-duplicate-v1", authoritystore.ModelState{"state": []byte("v1")})
	firstRequest.AttachedSchedules = []authoritystore.AttachedSchedule{schedule}
	first, err := rig.store.Commit(context.Background(), firstRequest)
	if err != nil || first.Status != authoritystore.CommitCommitted {
		t.Fatalf("first schedule commit=%+v err=%v", first, err)
	}
	firstSnapshot, err := rig.store.WorkLease().GetScheduledWork(context.Background(), rig.bucket, schedule.ScheduleType, schedule.ScheduleID)
	if err != nil {
		t.Fatal(err)
	}
	if firstSnapshot.State != "PENDING" || firstSnapshot.Generation != 1 || firstSnapshot.WorkVersion != 1 || string(firstSnapshot.Payload) != "wake-v1" {
		t.Fatalf("first schedule snapshot=%+v", firstSnapshot)
	}
	count, bytes, schedules := testScheduleBucketUsage(t, rig)
	if count != 1 || bytes != int64(len(schedule.Payload)) || schedules != 1 {
		t.Fatalf("first bucket usage=(%d,%d,%d)", count, bytes, schedules)
	}

	duplicateRequest := rig.commitReq("schedule-duplicate-claim", first.Snapshot.RecordVersion, "schedule-duplicate-v2", authoritystore.ModelState{"state": []byte("v2")})
	duplicateRequest.AttachedSchedules = []authoritystore.AttachedSchedule{schedule}
	duplicate, err := rig.store.Commit(context.Background(), duplicateRequest)
	if err != nil || duplicate.Status != authoritystore.CommitCommitted {
		t.Fatalf("same-generation duplicate=%+v err=%v", duplicate, err)
	}
	duplicateSnapshot, err := rig.store.WorkLease().GetScheduledWork(context.Background(), rig.bucket, schedule.ScheduleType, schedule.ScheduleID)
	if err != nil {
		t.Fatal(err)
	}
	if duplicateSnapshot.WorkVersion != 1 || duplicateSnapshot.Generation != 1 {
		t.Fatalf("duplicate changed schedule=%+v", duplicateSnapshot)
	}
	count, bytes, schedules = testScheduleBucketUsage(t, rig)
	if count != 1 || bytes != int64(len(schedule.Payload)) || schedules != 1 {
		t.Fatalf("duplicate bucket usage=(%d,%d,%d)", count, bytes, schedules)
	}

	conflictRequest := rig.commitReq("schedule-duplicate-claim", duplicate.Snapshot.RecordVersion, "schedule-duplicate-conflict", authoritystore.ModelState{"state": []byte("conflict")})
	conflictRequest.AttachedSchedules = []authoritystore.AttachedSchedule{testSchedule(t, "schedule-duplicate", 1, testNow.UnixMilli(), "different")}
	if _, err := rig.store.Commit(context.Background(), conflictRequest); !errors.Is(err, authoritystore.ErrScheduleConflict) {
		t.Fatalf("same-generation conflict err=%v", err)
	}
	unchangedActor, err := rig.store.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
		ActorAddress: rig.addr, AuthorityLayoutVersion: "v1", ShardWriteFence: rig.fence,
		ClaimOperationID: "schedule-duplicate-read", NewWriterClaimID: "schedule-duplicate-claim",
		InitialModelFields: authoritystore.ModelState{"initial": []byte("state")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if unchangedActor.Snapshot.RecordVersion != duplicate.Snapshot.RecordVersion {
		t.Fatalf("conflict changed actor record=%+v", unchangedActor.Snapshot)
	}

	higher := testSchedule(t, "schedule-duplicate", 2, testNow.UnixMilli()+1_000, "wake-v2")
	higherRequest := rig.commitReq("schedule-duplicate-claim", duplicate.Snapshot.RecordVersion, "schedule-duplicate-v3", authoritystore.ModelState{"state": []byte("v3")})
	higherRequest.AttachedSchedules = []authoritystore.AttachedSchedule{higher}
	higherResult, err := rig.store.Commit(context.Background(), higherRequest)
	if err != nil || higherResult.Status != authoritystore.CommitCommitted {
		t.Fatalf("higher generation commit=%+v err=%v", higherResult, err)
	}
	staleRequest := rig.commitReq("schedule-duplicate-claim", higherResult.Snapshot.RecordVersion, "schedule-duplicate-stale", authoritystore.ModelState{"state": []byte("v4")})
	staleRequest.AttachedSchedules = []authoritystore.AttachedSchedule{testSchedule(t, "schedule-duplicate", 1, 1, "old-content")}
	staleResult, err := rig.store.Commit(context.Background(), staleRequest)
	if err != nil || staleResult.Status != authoritystore.CommitCommitted {
		t.Fatalf("stale schedule blocked actor=%+v err=%v", staleResult, err)
	}
	finalSnapshot, err := rig.store.WorkLease().GetScheduledWork(context.Background(), rig.bucket, higher.ScheduleType, higher.ScheduleID)
	if err != nil {
		t.Fatal(err)
	}
	if finalSnapshot.Generation != 2 || string(finalSnapshot.Payload) != "wake-v2" {
		t.Fatalf("stale schedule changed current=%+v", finalSnapshot)
	}
}

func TestRedisAttachedScheduleClaimAdvanceCompleteAndReactivate(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "schedule-lifecycle-claim")
	schedule := testSchedule(t, "schedule-lifecycle", 1, testNow.UnixMilli(), "wake-v1")
	request := rig.commitReq("schedule-lifecycle-claim", loaded.RecordVersion, "schedule-lifecycle-v1", authoritystore.ModelState{"state": []byte("v1")})
	request.AttachedSchedules = []authoritystore.AttachedSchedule{schedule}
	first, err := rig.store.Commit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	lease := rig.store.WorkLease()
	claim, err := lease.ClaimScheduledWorkWithBoot(context.Background(), rig.bucket, schedule.ScheduleType, schedule.ScheduleID, "worker-1", "boot-1", "token-1", 30_000)
	if err != nil || claim.Status != WorkClaimClaimed || claim.Generation != 1 || claim.WorkVersion != 1 || claim.LeaseEpoch != 1 {
		t.Fatalf("schedule claim=%+v err=%v", claim, err)
	}

	higher := testSchedule(t, "schedule-lifecycle", 2, testNow.UnixMilli()+1_000, "wake-v2")
	supersedeRequest := rig.commitReq("schedule-lifecycle-claim", first.Snapshot.RecordVersion, "schedule-lifecycle-v2", authoritystore.ModelState{"state": []byte("v2")})
	supersedeRequest.AttachedSchedules = []authoritystore.AttachedSchedule{higher}
	superseded, err := rig.store.Commit(context.Background(), supersedeRequest)
	if err != nil {
		t.Fatal(err)
	}
	oldPayload := []byte("old-worker")
	oldHash := fmt.Sprintf("%x", sha256.Sum256(oldPayload))
	oldAdvance, err := lease.AdvanceScheduledWork(context.Background(), rig.bucket, schedule.ScheduleType, schedule.ScheduleID,
		"worker-1", "boot-1", claim.LeaseEpoch, claim.WorkVersion, claim.Generation, "token-1", testNow.UnixMilli()+2_000, oldPayload, oldHash)
	if err != nil || oldAdvance.Status != WorkLeaseStaleGeneration {
		t.Fatalf("old worker advance=%+v err=%v", oldAdvance, err)
	}
	current, err := lease.GetScheduledWork(context.Background(), rig.bucket, higher.ScheduleType, higher.ScheduleID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Generation != 2 || current.WorkVersion != 2 || current.State != "PENDING" || current.LeaseHolderID != "" || string(current.Payload) != "wake-v2" {
		t.Fatalf("superseded current=%+v", current)
	}

	rig.server.SetTime(testNow.Add(2 * time.Second))
	claim2, err := lease.ClaimScheduledWorkWithBoot(context.Background(), rig.bucket, higher.ScheduleType, higher.ScheduleID, "worker-2", "boot-2", "token-2", 30_000)
	if err != nil || claim2.Status != WorkClaimClaimed || claim2.Generation != 2 || claim2.WorkVersion != 2 {
		t.Fatalf("new generation claim=%+v err=%v", claim2, err)
	}
	nextPayload := []byte("wake-v2-next")
	nextHash := fmt.Sprintf("%x", sha256.Sum256(nextPayload))
	advanced, err := lease.AdvanceScheduledWork(context.Background(), rig.bucket, higher.ScheduleType, higher.ScheduleID,
		"worker-2", "boot-2", claim2.LeaseEpoch, claim2.WorkVersion, claim2.Generation, "token-2", testNow.UnixMilli()+3_000, nextPayload, nextHash)
	if err != nil || advanced.Status != WorkLeaseRenewed || advanced.WorkVersion != 3 || advanced.Generation != 2 {
		t.Fatalf("schedule advance=%+v err=%v", advanced, err)
	}
	advancedSnapshot, err := lease.GetScheduledWork(context.Background(), rig.bucket, higher.ScheduleType, higher.ScheduleID)
	if err != nil {
		t.Fatal(err)
	}
	if advancedSnapshot.State != "PENDING" || advancedSnapshot.WorkVersion != 3 || advancedSnapshot.NextDueAtMs != testNow.UnixMilli()+3_000 || string(advancedSnapshot.Payload) != string(nextPayload) {
		t.Fatalf("advanced schedule=%+v", advancedSnapshot)
	}

	rig.server.SetTime(testNow.Add(3 * time.Second))
	claim3, err := lease.ClaimScheduledWorkWithBoot(context.Background(), rig.bucket, higher.ScheduleType, higher.ScheduleID, "worker-3", "boot-3", "token-3", 30_000)
	if err != nil || claim3.Status != WorkClaimClaimed {
		t.Fatalf("completion claim=%+v err=%v", claim3, err)
	}
	completed, err := lease.CompleteScheduledWork(context.Background(), rig.bucket, higher.ScheduleType, higher.ScheduleID,
		"worker-3", "boot-3", claim3.LeaseEpoch, claim3.WorkVersion, claim3.Generation, "token-3")
	if err != nil || completed.Status != WorkLeaseCompleted {
		t.Fatalf("schedule complete=%+v err=%v", completed, err)
	}
	count, bytes, schedules := testScheduleBucketUsage(t, rig)
	if count != 0 || bytes != 0 || schedules != 1 {
		t.Fatalf("completed bucket usage=(%d,%d,%d)", count, bytes, schedules)
	}
	again, err := lease.CompleteScheduledWork(context.Background(), rig.bucket, higher.ScheduleType, higher.ScheduleID,
		"worker-3", "boot-3", claim3.LeaseEpoch, claim3.WorkVersion, claim3.Generation, "token-3")
	if err != nil || again.Status != WorkLeaseAlreadyCompleted {
		t.Fatalf("repeat schedule complete=%+v err=%v", again, err)
	}

	reactivate := testSchedule(t, "schedule-lifecycle", 3, testNow.UnixMilli()+4_000, "wake-v3")
	reactivateRequest := rig.commitReq("schedule-lifecycle-claim", superseded.Snapshot.RecordVersion, "schedule-lifecycle-v3", authoritystore.ModelState{"state": []byte("v3")})
	reactivateRequest.AttachedSchedules = []authoritystore.AttachedSchedule{reactivate}
	if _, err := rig.store.Commit(context.Background(), reactivateRequest); err != nil {
		t.Fatal(err)
	}
	count, bytes, schedules = testScheduleBucketUsage(t, rig)
	if count != 1 || bytes != int64(len(reactivate.Payload)) || schedules != 1 {
		t.Fatalf("reactivated bucket usage=(%d,%d,%d)", count, bytes, schedules)
	}
	reactivated, err := lease.GetScheduledWork(context.Background(), rig.bucket, reactivate.ScheduleType, reactivate.ScheduleID)
	if err != nil {
		t.Fatal(err)
	}
	if reactivated.State != "PENDING" || reactivated.Generation != 3 || reactivated.WorkVersion != 4 || reactivated.NextDueAtMs != reactivate.DueAtMs {
		t.Fatalf("reactivated schedule=%+v", reactivated)
	}

	due, err := lease.ListDueScheduledWork(context.Background(), rig.bucket, testNow.UnixMilli()+4_000, 32)
	if err != nil || len(due) != 1 || due[0].ScheduleType != reactivate.ScheduleType || due[0].ScheduleID != reactivate.ScheduleID {
		t.Fatalf("schedule due discovery=%+v err=%v", due, err)
	}
}

func TestRedisScheduleAndOrderDueMembersDoNotCollide(t *testing.T) {
	rig := newTestRig(t)
	order := dueOrder(t, "FRIEND_OPPORTUNITY", "same-member", testNow.UnixMilli())
	schedule := testSchedule(t, "same-member", 1, testNow.UnixMilli(), "schedule")
	orderRef, err := workRefOf(order)
	if err != nil {
		t.Fatal(err)
	}
	scheduleRef, err := workidentity.EncodeScheduleRefV1(schedule.ScheduleType, schedule.ScheduleID)
	if err != nil {
		t.Fatal(err)
	}
	if orderRef == scheduleRef {
		t.Fatalf("order and schedule due members collided: %q", orderRef)
	}
	dueKey, err := rig.store.keys.SourceDueIndex(rig.bucket)
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.client.ZAdd(context.Background(), dueKey.String(), redis.Z{Score: float64(testNow.UnixMilli()), Member: orderRef}, redis.Z{Score: float64(testNow.UnixMilli()), Member: scheduleRef}).Err(); err != nil {
		t.Fatal(err)
	}
	members, err := rig.client.ZCard(context.Background(), dueKey.String()).Result()
	if err != nil || members != 2 {
		t.Fatalf("due members=%d err=%v", members, err)
	}
}

func TestRedisScheduleDiscoveryPagesPastOrderPrefix(t *testing.T) {
	rig := newTestRig(t)
	dueKey, err := rig.store.keys.SourceDueIndex(rig.bucket)
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]redis.Z, 0, 257)
	for index := 0; index < 256; index++ {
		order := dueOrder(t, "FRIEND_OPPORTUNITY", fmt.Sprintf("order-prefix-%03d", index), testNow.UnixMilli())
		orderRef, refErr := workRefOf(order)
		if refErr != nil {
			t.Fatal(refErr)
		}
		entries = append(entries, redis.Z{Score: float64(testNow.UnixMilli()), Member: orderRef})
	}
	schedule := testSchedule(t, "schedule-after-order-prefix", 1, testNow.UnixMilli()+1, "wake")
	scheduleRef, err := workidentity.EncodeScheduleRefV1(schedule.ScheduleType, schedule.ScheduleID)
	if err != nil {
		t.Fatal(err)
	}
	entries = append(entries, redis.Z{Score: float64(schedule.DueAtMs), Member: scheduleRef})
	if err := rig.client.ZAdd(context.Background(), dueKey.String(), entries...).Err(); err != nil {
		t.Fatal(err)
	}

	due, err := rig.store.WorkLease().ListDueScheduledWork(context.Background(), rig.bucket, schedule.DueAtMs, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].ScheduleType != schedule.ScheduleType || due[0].ScheduleID != schedule.ScheduleID {
		t.Fatalf("schedule starved behind order prefix: %+v", due)
	}
}

func TestRedisAttachedScheduleConcurrentDuplicateSubmission(t *testing.T) {
	rig := newTestRig(t)
	loaded := rig.load(t, "schedule-race-claim")
	schedule := testSchedule(t, "schedule-race", 1, testNow.UnixMilli(), "wake-race")
	request := rig.commitReq("schedule-race-claim", loaded.RecordVersion, "schedule-race-op", authoritystore.ModelState{"state": []byte("race")})
	request.AttachedSchedules = []authoritystore.AttachedSchedule{schedule}

	results := make([]authoritystore.CommitResult, 2)
	errorsSeen := make([]error, 2)
	var group sync.WaitGroup
	group.Add(2)
	for index := range results {
		go func(index int) {
			defer group.Done()
			results[index], errorsSeen[index] = rig.store.Commit(context.Background(), request)
		}(index)
	}
	group.Wait()
	for index := range results {
		if errorsSeen[index] != nil {
			t.Fatalf("duplicate submission[%d] err=%v", index, errorsSeen[index])
		}
	}
	committed, replayed := 0, 0
	for _, result := range results {
		switch result.Status {
		case authoritystore.CommitCommitted:
			committed++
		case authoritystore.CommitAlreadyCommitted:
			replayed++
		default:
			t.Fatalf("duplicate submission status=%s result=%+v", result.Status, result)
		}
	}
	if committed != 1 || replayed != 1 {
		t.Fatalf("duplicate linearization committed=%d replayed=%d", committed, replayed)
	}
	snapshot, err := rig.store.WorkLease().GetScheduledWork(context.Background(), rig.bucket, schedule.ScheduleType, schedule.ScheduleID)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Generation != 1 || snapshot.WorkVersion != 1 || snapshot.State != "PENDING" {
		t.Fatalf("concurrent duplicate schedule=%+v", snapshot)
	}
	count, bytes, schedules := testScheduleBucketUsage(t, rig)
	if count != 1 || bytes != int64(len(schedule.Payload)) || schedules != 1 {
		t.Fatalf("concurrent duplicate usage=(%d,%d,%d)", count, bytes, schedules)
	}
}
