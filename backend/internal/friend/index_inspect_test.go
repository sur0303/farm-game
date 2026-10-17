package friend

import (
	"context"
	"testing"
)

const (
	inspectViewer = uint64(1)
	inspectPeer   = uint64(2)
	activatedAtMs = int64(1_800_000_000_000)
)

// activePair builds the canonical side of one friendship: the pair record says
// the two are friends, and each player's quota says so too.
func activePair(t *testing.T) (*MemoryPairRepository, *MemoryQuotaRepository, string) {
	t.Helper()
	ctx := context.Background()
	pairs := NewMemoryPairRepository()
	quotas := NewMemoryQuotaRepository(defaultMaxFriends())
	relationID, err := RelationIDFor(inspectViewer, inspectPeer)
	if err != nil {
		t.Fatal(err)
	}
	identity := CommitIdentity{SubjectPlayerID: inspectViewer, OperationID: "activate-1", CommandType: "REDEEM_SHARE"}
	outcome, err := pairs.ActivateFriendRelation(ctx, ActivateCommand{
		InviterPlayerID: inspectPeer, RedeemerPlayerID: inspectViewer,
		TargetRelationEpoch: 1, TokenID: "token-1", TokenGeneration: 1,
		OperationID: "activate-1", NowMs: activatedAtMs,
	}, identity, []byte("payload"))
	if err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("activate outcome=%+v err=%v", outcome, err)
	}
	for _, player := range []uint64{inspectViewer, inspectPeer} {
		reservation := QuotaReservation{
			ReservationID: "res-" + itoa(player), WorkflowID: "wf-" + itoa(player),
			RelationID: relationID, TargetRelationEpoch: 1, CreatedAtMs: activatedAtMs,
		}
		if _, err := quotas.ReserveFriendSlot(ctx, player, reservation,
			CommitIdentity{SubjectPlayerID: player, OperationID: "reserve-" + itoa(player), CommandType: "REDEEM_SHARE"},
			[]byte("payload"), activatedAtMs); err != nil {
			t.Fatal(err)
		}
		if _, err := quotas.FinalizeFriendSlot(ctx, player, reservation,
			CommitIdentity{SubjectPlayerID: player, OperationID: "finalize-" + itoa(player), CommandType: "REDEEM_SHARE"},
			[]byte("payload"), activatedAtMs); err != nil {
			t.Fatal(err)
		}
	}
	return pairs, quotas, relationID
}

func inspect(t *testing.T, index FriendIndexRepository, pairs *MemoryPairRepository,
	quotas *MemoryQuotaRepository, settledBeforeMs int64) FriendIndexInspection {
	t.Helper()
	reconciler, err := NewFriendIndexReconciler(index, pairs, quotas)
	if err != nil {
		t.Fatal(err)
	}
	inspection, err := reconciler.InspectViewer(context.Background(), inspectViewer, settledBeforeMs)
	if err != nil {
		t.Fatal(err)
	}
	return inspection
}

// A projection that never received the row is the case a player notices: the
// friend is missing from the list even though the pair says otherwise.
func TestInspectFindsTheRowTheProjectionNeverReceived(t *testing.T) {
	pairs, quotas, _ := activePair(t)
	index := NewMemoryFriendIndexRepository()

	inspection := inspect(t, index, pairs, quotas, 0)
	if inspection.MissingRows != 1 || inspection.ExtraRows != 0 {
		t.Fatalf("inspection=%+v, want exactly one missing row", inspection)
	}
}

// What a reconcile fixes, an inspection must have reported. Otherwise the
// audit would call a projection clean that the repair is still changing.
func TestInspectAgreesWithWhatAReconcileChanges(t *testing.T) {
	pairs, quotas, _ := activePair(t)
	index := NewMemoryFriendIndexRepository()
	reconciler, err := NewFriendIndexReconciler(index, pairs, quotas)
	if err != nil {
		t.Fatal(err)
	}
	before := inspect(t, index, pairs, quotas, 0)
	if before.Unrepaired() == 0 {
		t.Fatal("fixture left nothing to repair, so agreement would be vacuous")
	}

	report, err := reconciler.ReconcileViewer(context.Background(), inspectViewer)
	if err != nil {
		t.Fatal(err)
	}
	if changed := report.AppliedRows + report.RemovedRows; changed != before.Unrepaired() {
		t.Fatalf("reconcile changed %d rows, inspection reported %d", changed, before.Unrepaired())
	}
	if after := inspect(t, index, pairs, quotas, 0); after.Unrepaired() != 0 {
		t.Fatalf("inspection=%+v after a reconcile, want nothing left", after)
	}
}

// A row the canonical authority does not support is the other half: a read
// drops it, so it costs work rather than showing a friend who is not one.
func TestInspectFindsTheRowNoPairSupports(t *testing.T) {
	pairs, quotas, _ := activePair(t)
	index := NewMemoryFriendIndexRepository()
	if _, err := index.Apply(context.Background(), inspectViewer, FriendIndexEntry{
		FriendPlayerID: 99, RelationID: "relation-that-does-not-exist",
		RelationEpoch: 1, ActivatedAtMs: activatedAtMs,
	}); err != nil {
		t.Fatal(err)
	}

	inspection := inspect(t, index, pairs, quotas, 0)
	if inspection.ExtraRows != 1 {
		t.Fatalf("inspection=%+v, want the unsupported row counted", inspection)
	}
}

// Repair is queued before the pair commits and gets a bounded number of
// rounds. A disagreement younger than that window is work in progress.
func TestInspectLeavesRecentChangesToTheRepairWorker(t *testing.T) {
	pairs, quotas, _ := activePair(t)
	index := NewMemoryFriendIndexRepository()

	tooYoung := inspect(t, index, pairs, quotas, activatedAtMs-1)
	if tooYoung.Unrepaired() != 0 {
		t.Fatalf("inspection=%+v for a pair that just changed, want it left alone", tooYoung)
	}
	settled := inspect(t, index, pairs, quotas, activatedAtMs+1)
	if settled.MissingRows != 1 {
		t.Fatalf("inspection=%+v once the window passed, want the missing row counted", settled)
	}
}
