package friend

import "testing"

func mustPairRecord(t *testing.T, low, high uint64) *PairRecord {
	t.Helper()
	record, err := NewPairRecord(low, high)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func activatePair(t *testing.T, record *PairRecord, epoch uint64) {
	t.Helper()
	outcome, err := record.ApplyActivate(ActivateCommand{
		InviterPlayerID: record.LowPlayerID, RedeemerPlayerID: record.HighPlayerID,
		TargetRelationEpoch: epoch, TokenID: "tok", TokenGeneration: 1,
		OperationID: "op", NowMs: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Status != StatusSucceeded {
		t.Fatalf("activate = %v/%v", outcome.Status, outcome.Reason)
	}
}

func TestPairLifecycleTable(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, r *PairRecord)
	}{
		{"delete non active is NO_CHANGE", func(t *testing.T, r *PairRecord) {
			outcome, order, err := r.ApplyDelete(1, "op-del", 2000)
			if err != nil {
				t.Fatal(err)
			}
			if outcome.Status != StatusNoChange || outcome.Reason != ReasonNotFriends || order != nil {
				t.Fatalf("delete on empty pair = %v/%v order=%v", outcome.Status, outcome.Reason, order)
			}
		}},
		{"delete active captures epoch and opens barrier PENDING", func(t *testing.T, r *PairRecord) {
			activatePair(t, r, 1)
			outcome, order, err := r.ApplyDelete(1, "op-del", 2000)
			if err != nil {
				t.Fatal(err)
			}
			if outcome.Status != StatusSucceeded || outcome.CommittedEpoch != 2 {
				t.Fatalf("delete = %v epoch=%d", outcome.Status, outcome.CommittedEpoch)
			}
			if r.Relation.Status != RelationStatusDeleted || r.Relation.TerminationReason != TerminationUserDelete {
				t.Fatalf("relation = %v reason=%v", r.Relation.Status, r.Relation.TerminationReason)
			}
			if r.Barrier.State != BarrierPending || r.Barrier.TerminatedRelationEpoch != 1 || r.Barrier.BarrierRevision != 1 {
				t.Fatalf("barrier = %+v", r.Barrier)
			}
			if order == nil || order.CapturedActiveEpoch != 1 {
				t.Fatalf("order = %+v", order)
			}
		}},
		{"block terminates relation atomically", func(t *testing.T, r *PairRecord) {
			activatePair(t, r, 1)
			outcome, order, err := r.ApplyBlock(2, 1, "op-block", 3000)
			if err != nil {
				t.Fatal(err)
			}
			if outcome.Status != StatusSucceeded {
				t.Fatalf("block = %v", outcome.Status)
			}
			if r.HighBlocksLow.Status != BlockStatusActive || r.Relation.Status != RelationStatusDeleted {
				t.Fatalf("block/relation = %v/%v", r.HighBlocksLow.Status, r.Relation.Status)
			}
			if r.Relation.TerminationReason != TerminationBlocked || order == nil {
				t.Fatalf("termination = %v order=%v", r.Relation.TerminationReason, order)
			}
		}},
		{"block without relation succeeds without order", func(t *testing.T, r *PairRecord) {
			outcome, order, err := r.ApplyBlock(1, 2, "op-block", 3000)
			if err != nil {
				t.Fatal(err)
			}
			if outcome.Status != StatusSucceeded || order != nil {
				t.Fatalf("block = %v order=%v", outcome.Status, order)
			}
			if r.Relation.Status != RelationStatusNone {
				t.Fatalf("relation = %v", r.Relation.Status)
			}
		}},
		{"repeat block and unblock are deterministic NO_CHANGE", func(t *testing.T, r *PairRecord) {
			if _, _, err := r.ApplyBlock(1, 2, "op-1", 1000); err != nil {
				t.Fatal(err)
			}
			again, _, err := r.ApplyBlock(1, 2, "op-2", 1100)
			if err != nil {
				t.Fatal(err)
			}
			if again.Status != StatusNoChange || again.Reason != ReasonAlreadyBlocked {
				t.Fatalf("repeat block = %v/%v", again.Status, again.Reason)
			}
			un, err := r.ApplyUnblock(1, 2, 1200)
			if err != nil {
				t.Fatal(err)
			}
			if un.Status != StatusSucceeded || r.LowBlocksHigh.Status != BlockStatusRemoved {
				t.Fatalf("unblock = %v dir=%v", un.Status, r.LowBlocksHigh.Status)
			}
			reUn, err := r.ApplyUnblock(1, 2, 1300)
			if err != nil {
				t.Fatal(err)
			}
			if reUn.Status != StatusNoChange || reUn.Reason != ReasonAlreadyUnblocked {
				t.Fatalf("repeat unblock = %v/%v", reUn.Status, reUn.Reason)
			}
		}},
		{"unblock never restores friendship", func(t *testing.T, r *PairRecord) {
			activatePair(t, r, 1)
			if _, _, err := r.ApplyBlock(1, 2, "op-b", 2000); err != nil {
				t.Fatal(err)
			}
			if _, err := r.ApplyUnblock(1, 2, 3000); err != nil {
				t.Fatal(err)
			}
			if r.Relation.Status != RelationStatusDeleted {
				t.Fatalf("relation restored to %v", r.Relation.Status)
			}
			if r.CanIssuePermit(1, 2) || r.CanIssuePermit(2, 1) {
				t.Fatal("permit must not be issuable after unblock")
			}
		}},
		{"activate blocked pair fails closed", func(t *testing.T, r *PairRecord) {
			if _, _, err := r.ApplyBlock(1, 2, "op-b", 1000); err != nil {
				t.Fatal(err)
			}
			outcome, err := r.ApplyActivate(ActivateCommand{
				InviterPlayerID: 1, RedeemerPlayerID: 2, TargetRelationEpoch: 1,
				TokenID: "t", TokenGeneration: 5, OperationID: "op", NowMs: 2000,
			})
			if err != nil {
				t.Fatal(err)
			}
			if outcome.Status != StatusRejected || outcome.Reason != ReasonBlocked {
				t.Fatalf("activate = %v/%v", outcome.Status, outcome.Reason)
			}
		}},
		{"activate with stale target epoch fails closed", func(t *testing.T, r *PairRecord) {
			activatePair(t, r, 1)
			outcome, _, err := r.ApplyDelete(1, "op-d", 2000)
			if err != nil {
				t.Fatal(err)
			}
			_ = outcome
			stale, err := r.ApplyActivate(ActivateCommand{
				InviterPlayerID: 1, RedeemerPlayerID: 2, TargetRelationEpoch: 1,
				TokenID: "t", TokenGeneration: 9, OperationID: "op", NowMs: 3000,
			})
			if err != nil {
				t.Fatal(err)
			}
			if stale.Status != StatusRejected || stale.Reason != ReasonDomainEpochConflict {
				t.Fatalf("stale activate = %v/%v", stale.Status, stale.Reason)
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t, mustPairRecord(t, 1, 2))
		})
	}
}

func TestReactivationBarrierCycle(t *testing.T) {
	r := mustPairRecord(t, 1, 2)
	activatePair(t, r, 1)
	if _, _, err := r.ApplyDelete(1, "op-del", 2000); err != nil {
		t.Fatal(err)
	}

	// PENDING: any re-add is rejected regardless of token freshness.
	snap, err := r.ValidateForAdd(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !snap.Rejected || snap.Reason != ReasonRelationReactivationPending {
		t.Fatalf("pending validate = %v/%v", snap.Rejected, snap.Reason)
	}

	// Floors commit only for the matching deleted epoch + barrier revision.
	if outcome := r.CommitReactivationGenerationFloors(1, 1, 3, 4, 3000); outcome.Status != StatusNoChange {
		t.Fatalf("wrong epoch floors = %v", outcome.Status)
	}
	if outcome := r.CommitReactivationGenerationFloors(2, 9, 3, 4, 3000); outcome.Status != StatusNoChange {
		t.Fatalf("wrong revision floors = %v", outcome.Status)
	}
	if outcome := r.CommitReactivationGenerationFloors(2, 1, 3, 4, 3000); outcome.Status != StatusSucceeded {
		t.Fatalf("floors = %v/%v", outcome.Status, outcome.Reason)
	}
	if r.Barrier.State != BarrierReady || r.Barrier.LowShareGenerationFloor != 3 || r.Barrier.HighShareGenerationFloor != 4 {
		t.Fatalf("barrier = %+v", r.Barrier)
	}

	// READY: stale token generation rejected.
	snap, err = r.ValidateForAdd(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Rejected || snap.RequiredInviterFloor != 3 || snap.BarrierRevision != 1 {
		t.Fatalf("ready validate = %+v", snap)
	}
	outcome, err := r.ApplyActivate(ActivateCommand{
		InviterPlayerID: 1, RedeemerPlayerID: 2, TargetRelationEpoch: 3,
		TokenID: "old", TokenGeneration: 3, RotateReceiptPresent: true, OperationID: "op", NowMs: 4000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Status != StatusRejected || outcome.Reason != ReasonShareTokenPredatesRelationDel {
		t.Fatalf("old generation activate = %v/%v", outcome.Status, outcome.Reason)
	}

	// READY: fresh token without rotate receipt rejected.
	outcome, err = r.ApplyActivate(ActivateCommand{
		InviterPlayerID: 1, RedeemerPlayerID: 2, TargetRelationEpoch: 3,
		TokenID: "new", TokenGeneration: 4, RotateReceiptPresent: false, OperationID: "op", NowMs: 4000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Status != StatusRejected || outcome.Reason != ReasonReactivationRotateRequired {
		t.Fatalf("no-receipt activate = %v/%v", outcome.Status, outcome.Reason)
	}

	// READY + fresh token + rotate receipt: activation consumes the barrier.
	receiptID, err := ReactivationRotateReceiptID(r.PairID, r.Barrier.BarrierRevision, "new")
	if err != nil {
		t.Fatal(err)
	}
	outcome, err = r.ApplyActivate(ActivateCommand{
		InviterPlayerID: 1, RedeemerPlayerID: 2, TargetRelationEpoch: 3,
		TokenID: "new", TokenGeneration: 4,
		RotateReceipt: RotateReceipt{ReceiptID: receiptID, PairID: r.PairID,
			TerminatedRelationEpoch: 1, BarrierRevision: 1, TokenID: "new", TokenGeneration: 4,
			LowShareGenerationFloor: 3, HighShareGenerationFloor: 4, CommittedAtMs: 3500},
		RotateReceiptPresent: true, OperationID: "op", NowMs: 4000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Status != StatusSucceeded || outcome.CommittedEpoch != 3 {
		t.Fatalf("reactivate = %v/%v", outcome.Status, outcome.Reason)
	}
	if r.Barrier.State != BarrierConsumed {
		t.Fatalf("barrier = %v", r.Barrier.State)
	}

	// A second delete starts a brand-new barrier cycle with a new revision.
	if _, _, err := r.ApplyDelete(2, "op-del2", 5000); err != nil {
		t.Fatal(err)
	}
	if r.Barrier.State != BarrierPending || r.Barrier.BarrierRevision != 2 || r.Barrier.TerminatedRelationEpoch != 3 {
		t.Fatalf("second barrier = %+v", r.Barrier)
	}
}

func TestBarrierProofOnlyWhenReady(t *testing.T) {
	r := mustPairRecord(t, 1, 2)
	activatePair(t, r, 1)
	if _, _, err := r.ApplyDelete(1, "op", 100); err != nil {
		t.Fatal(err)
	}
	if _, err := r.GetBarrierProof(); err == nil {
		t.Fatal("PENDING barrier must not hand out proofs")
	}
	if outcome := r.CommitReactivationGenerationFloors(2, 1, 1, 1, 200); outcome.Status != StatusSucceeded {
		t.Fatal(outcome)
	}
	proof, err := r.GetBarrierProof()
	if err != nil {
		t.Fatal(err)
	}
	if proof.PairID != r.PairID || proof.BarrierRevision != 1 || proof.TerminatedRelationEpoch != 1 {
		t.Fatalf("proof = %+v", proof)
	}
}
