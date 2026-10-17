package friend

import (
	"fmt"
	"sync"
	"testing"
)

func mustQuota(t *testing.T, player uint64, max int) *QuotaRecord {
	t.Helper()
	record, err := NewQuotaRecord(player, max)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func reservationFor(t *testing.T, workflowID string, player uint64, relationID string, epoch uint64) QuotaReservation {
	t.Helper()
	id, err := QuotaReservationID(workflowID, player, relationID)
	if err != nil {
		t.Fatal(err)
	}
	return QuotaReservation{
		ReservationID: id, WorkflowID: workflowID,
		RelationID: relationID, TargetRelationEpoch: epoch,
	}
}

func TestQuotaReserveFinalizeRelease(t *testing.T) {
	q := mustQuota(t, 10001, 2)
	res := reservationFor(t, "wf-1", 10001, "rel-a", 1)

	outcome, err := q.ReserveFriendSlot(res, 1000)
	if err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("reserve = %v %v", outcome, err)
	}
	// Same reservation id with identical fields replays.
	replay, err := q.ReserveFriendSlot(res, 1100)
	if err != nil || replay.Status != StatusSucceeded {
		t.Fatalf("replay = %v %v", replay, err)
	}
	// Same reservation id with different fields conflicts.
	conflicting := res
	conflicting.TargetRelationEpoch = 2
	if _, err := q.ReserveFriendSlot(conflicting, 1200); err != ErrIdempotentConflict {
		t.Fatalf("conflicting reserve err = %v", err)
	}

	fin, err := q.FinalizeFriendSlot(res, 1300)
	if err != nil || fin.Status != StatusSucceeded {
		t.Fatalf("finalize = %v %v", fin, err)
	}
	if !q.HasActiveRelation("rel-a", 1) {
		t.Fatal("relation should occupy a slot")
	}

	// Termination removes the active relation only at the captured epoch and
	// installs the release fence.
	rm := q.RemoveActiveRelationIfEpochMatches("rel-a", 1, 1400)
	if rm.Status != StatusSucceeded {
		t.Fatalf("remove = %v/%v", rm.Status, rm.Reason)
	}
	if q.HasActiveRelation("rel-a", 1) {
		t.Fatal("slot should be released")
	}
	// Idempotent: second remove at the same epoch is NO_CHANGE via fence.
	rm2 := q.RemoveActiveRelationIfEpochMatches("rel-a", 1, 1500)
	if rm2.Status != StatusNoChange {
		t.Fatalf("second remove = %v", rm2.Status)
	}
}

func TestReleaseFenceBlocksLateFinalize(t *testing.T) {
	q := mustQuota(t, 10001, 300)
	res := reservationFor(t, "wf-1", 10001, "rel-a", 5)
	if _, err := q.ReserveFriendSlot(res, 1000); err != nil {
		t.Fatal(err)
	}
	// Termination release happens while the slot is still RESERVED.
	rm := q.RemoveActiveRelationIfEpochMatches("rel-a", 5, 1100)
	if rm.Status != StatusSucceeded {
		t.Fatalf("release = %v", rm.Status)
	}
	// Late finalize must not re-occupy the slot (33A §5.4).
	fin, err := q.FinalizeFriendSlot(res, 1200)
	if err != nil {
		t.Fatal(err)
	}
	if fin.Status != StatusNoChange || fin.Reason != ReasonRelationAlreadyTerminated {
		t.Fatalf("late finalize = %v/%v", fin.Status, fin.Reason)
	}
	if q.ActiveCount() != 0 || len(q.Reservations) != 0 {
		t.Fatalf("slot leaked: active=%d reservations=%d", q.ActiveCount(), len(q.Reservations))
	}
}

func TestReleaseFenceDoesNotTouchNewerEpoch(t *testing.T) {
	q := mustQuota(t, 10001, 300)
	// Old epoch 5 was terminated; the pair re-activated at epoch 6.
	q.ActiveRelations["rel-a"] = 6
	rm := q.RemoveActiveRelationIfEpochMatches("rel-a", 5, 1000)
	if rm.Status != StatusSucceeded { // fence installed, nothing removed
		t.Fatalf("remove = %v", rm.Status)
	}
	if !q.HasActiveRelation("rel-a", 6) {
		t.Fatal("new epoch slot must survive old-epoch release")
	}
}

func TestQuotaHardLimit(t *testing.T) {
	q := mustQuota(t, 10001, 3)
	for i := 0; i < 3; i++ {
		res := reservationFor(t, "wf", 10001, fmt.Sprintf("rel-%d", i), 1)
		if _, err := q.ReserveFriendSlot(res, 1000); err != nil {
			t.Fatal(err)
		}
	}
	over := reservationFor(t, "wf", 10001, "rel-over", 1)
	outcome, err := q.ReserveFriendSlot(over, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Status != StatusRejected || outcome.Reason != ReasonFriendLimitReached {
		t.Fatalf("over-limit reserve = %v/%v", outcome.Status, outcome.Reason)
	}
}

func TestGrandfatheredOverflow(t *testing.T) {
	q := mustQuota(t, 10001, 300)
	for i := 0; i < 250; i++ {
		q.ActiveRelations[fmt.Sprintf("rel-%d", i)] = 1
	}
	// Limit lowered 300 -> 200: existing relations kept, new reserve denied.
	q.EffectiveMaxFriends = 200
	if q.ActiveCount() != 250 {
		t.Fatal("existing relations must be preserved")
	}
	res := reservationFor(t, "wf", 10001, "rel-new", 1)
	outcome, err := q.ReserveFriendSlot(res, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Status != StatusRejected || outcome.Reason != ReasonFriendLimitReached {
		t.Fatalf("overflow reserve = %v/%v", outcome.Status, outcome.Reason)
	}
	// Removing existing relations is the only way out of overflow. At 200 the
	// next reserve is still forbidden; after reaching 199 exactly one new slot
	// may be reserved to return to 200.
	for i := 249; i >= 199; i-- {
		q.RemoveActiveRelationIfEpochMatches(fmt.Sprintf("rel-%d", i), 1, int64(2000-i))
	}
	if q.ActiveCount() != 199 {
		t.Fatalf("active after removals = %d, want 199", q.ActiveCount())
	}
	outcome, err = q.ReserveFriendSlot(res, 3000)
	if err != nil || outcome.Status != StatusSucceeded || q.occupancy() != 200 {
		t.Fatalf("reserve at 199 = %+v err=%v occupancy=%d", outcome, err, q.occupancy())
	}
}

func TestConcurrentReserveNeverExceedsLimit(t *testing.T) {
	repo := NewMemoryQuotaRepository(300)
	const workers = 64
	const attempts = 500
	var wg sync.WaitGroup
	succeeded := make(chan struct{}, attempts)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < attempts/workers+1; i++ {
				relationID := fmt.Sprintf("rel-%d-%d", w, i)
				res := reservationFor(t, "wf", 10001, relationID, 1)
				identity := CommitIdentity{ParticipantInvocationID: "inv-" + relationID, ParticipantStep: "RESERVE"}
				outcome, err := repo.ReserveFriendSlot(context0, 10001, res, identity, []byte("h"), 1000)
				if err != nil {
					t.Error(err)
					return
				}
				if outcome.Status == StatusSucceeded {
					succeeded <- struct{}{}
				}
			}
		}(w)
	}
	wg.Wait()
	close(succeeded)
	count := 0
	for range succeeded {
		count++
	}
	if count > 300 {
		t.Fatalf("301st reservation slipped through: %d", count)
	}
	snap, err := repo.GetQuota(context0, 10001)
	if err != nil {
		t.Fatal(err)
	}
	if snap.OpenReservations != count || snap.OpenReservations > 300 {
		t.Fatalf("open reservations = %d, successes = %d", snap.OpenReservations, count)
	}
}

func TestMemoryQuotaRecordVersionOnlyChangesOnBusinessMutation(t *testing.T) {
	repo := NewMemoryQuotaRepository(300)
	reservation := reservationFor(t, "wf-version", 10001, "rel-version", 1)
	identity := CommitIdentity{ParticipantInvocationID: "version-reserve", ParticipantStep: "RESERVE"}
	if outcome, err := repo.ReserveFriendSlot(context0, 10001, reservation, identity, []byte("reserve"), 1000); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("reserve = %+v/%v", outcome, err)
	}
	snapshot, err := repo.GetQuota(context0, 10001)
	if err != nil {
		t.Fatal(err)
	}
	versionAfterReserve := snapshot.RecordVersion
	if outcome, err := repo.ReserveFriendSlot(context0, 10001, reservation,
		CommitIdentity{ParticipantInvocationID: "version-reserve-again", ParticipantStep: "RESERVE"}, []byte("reserve-again"), 2000); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("same reservation replay = %+v/%v", outcome, err)
	}
	snapshot, err = repo.GetQuota(context0, 10001)
	if err != nil || snapshot.RecordVersion != versionAfterReserve {
		t.Fatalf("idempotent reserve advanced record version: %+v err=%v", snapshot, err)
	}
	if outcome, err := repo.FinalizeFriendSlot(context0, 10001, reservation,
		CommitIdentity{ParticipantInvocationID: "version-finalize", ParticipantStep: "FINALIZE"}, []byte("finalize"), 3000); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("finalize = %+v/%v", outcome, err)
	}
	snapshot, err = repo.GetQuota(context0, 10001)
	if err != nil || snapshot.RecordVersion != versionAfterReserve+1 {
		t.Fatalf("finalize version = %+v err=%v", snapshot, err)
	}
	versionAfterFinalize := snapshot.RecordVersion
	if outcome, err := repo.FinalizeFriendSlot(context0, 10001, reservation,
		CommitIdentity{ParticipantInvocationID: "version-finalize-again", ParticipantStep: "FINALIZE"}, []byte("finalize-again"), 4000); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("idempotent finalize = %+v/%v", outcome, err)
	}
	snapshot, err = repo.GetQuota(context0, 10001)
	if err != nil || snapshot.RecordVersion != versionAfterFinalize {
		t.Fatalf("idempotent finalize advanced record version: %+v err=%v", snapshot, err)
	}
}
