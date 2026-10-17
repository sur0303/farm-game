package friend

import "testing"

func mustIssuer(t *testing.T, inviter uint64) *ShareIssuerRecord {
	t.Helper()
	record, err := NewShareIssuerRecord(inviter)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestShareTokenLifecycle(t *testing.T) {
	r := mustIssuer(t, 10001)

	// First generate creates generation 1.
	token, outcome, err := r.GenerateShareToken("tok-1", 1000, 500)
	if err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("generate = %v %v", outcome, err)
	}
	if token.TokenGeneration != 1 || token.Status != TokenStatusActive || token.ExpiresAtMs != 1500 {
		t.Fatalf("token = %+v", token)
	}
	// Repeated share clicks reuse the same link, no generation bump.
	again, outcome, err := r.GenerateShareToken("tok-other", 1000, 600)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Status != StatusNoChange || again.TokenID != "tok-1" || again.TokenGeneration != 1 {
		t.Fatalf("re-generate = %v token=%+v", outcome, again)
	}

	// Rotate invalidates the old link immediately.
	rotated, outcome, err := r.RotateShareToken("tok-2", 1000, 700)
	if err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("rotate = %v %v", outcome, err)
	}
	if rotated.TokenGeneration != 2 || rotated.TokenID != "tok-2" {
		t.Fatalf("rotated = %+v", rotated)
	}
	if _, outcome, err = r.AcceptShareTokenAdmission("tok-1", 1, SharePurposeAddFriend, 20002, "wf-1", 800); err != nil {
		t.Fatal(err)
	}
	if outcome.Status != StatusRejected || outcome.Reason != ReasonShareTokenRevoked {
		t.Fatalf("old token admission = %v/%v", outcome.Status, outcome.Reason)
	}

	// Revoke leaves no current token; admission of the last token fails.
	if outcome = r.RevokeShareToken(900); outcome.Status != StatusSucceeded {
		t.Fatal(outcome)
	}
	if _, outcome, err = r.AcceptShareTokenAdmission("tok-2", 2, SharePurposeAddFriend, 20002, "wf-2", 950); err != nil {
		t.Fatal(err)
	}
	if outcome.Status != StatusRejected {
		t.Fatalf("revoked admission = %v/%v", outcome.Status, outcome.Reason)
	}

	// Expiry is judged by trusted authority time.
	if _, outcome, err = r.GenerateShareToken("tok-3", 100, 1000); err != nil {
		t.Fatal(err)
	}
	if _, outcome, err = r.AcceptShareTokenAdmission("tok-3", 4, SharePurposeAddFriend, 20002, "wf-3", 1100); err != nil {
		t.Fatal(err)
	}
	if outcome.Status != StatusRejected || outcome.Reason != ReasonShareTokenExpired {
		t.Fatalf("expired admission = %v/%v", outcome.Status, outcome.Reason)
	}
}

func TestShareTokenAdmissionMultiUse(t *testing.T) {
	r := mustIssuer(t, 10001)
	if _, _, err := r.GenerateShareToken("tok-1", 10000, 0); err != nil {
		t.Fatal(err)
	}
	// Many different redeemers may use the same token; no use counter.
	for _, redeemer := range []uint64{20002, 20003, 20004} {
		receipt, outcome, err := r.AcceptShareTokenAdmission("tok-1", 1, SharePurposeAddFriend, redeemer, "wf-"+itoa(redeemer), 100)
		if err != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("admission %d = %v %v", redeemer, outcome, err)
		}
		if receipt.ReceiptID == "" || receipt.TokenGeneration != 1 {
			t.Fatalf("receipt = %+v", receipt)
		}
	}
	if len(r.ValidationReceipts) != 3 {
		t.Fatalf("receipts = %d", len(r.ValidationReceipts))
	}
	// Self-redemption is rejected deterministically.
	_, outcome, err := r.AcceptShareTokenAdmission("tok-1", 1, SharePurposeAddFriend, 10001, "wf-self", 100)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Status != StatusRejected || outcome.Reason != ReasonSelfAdd {
		t.Fatalf("self admission = %v/%v", outcome.Status, outcome.Reason)
	}
	// Same workflow admission replays the same receipt.
	first := r.ValidationReceipts
	receipt1, _, err := r.AcceptShareTokenAdmission("tok-1", 1, SharePurposeAddFriend, 20002, "wf-20002", 200)
	if err != nil {
		t.Fatal(err)
	}
	receipt2, _, err := r.AcceptShareTokenAdmission("tok-1", 1, SharePurposeAddFriend, 20002, "wf-20002", 300)
	if err != nil {
		t.Fatal(err)
	}
	if receipt1.ReceiptID != receipt2.ReceiptID || len(first) != 3 {
		t.Fatal("same workflow must replay the same validation receipt")
	}
}

func TestRotateForPairReactivation(t *testing.T) {
	r := mustIssuer(t, 10001)
	if _, _, err := r.GenerateShareToken("tok-1", 10000, 0); err != nil {
		t.Fatal(err)
	}
	pairID, err := RelationIDFor(10001, 20002)
	if err != nil {
		t.Fatal(err)
	}
	proof := BarrierProof{State: BarrierReady, PairID: pairID, TerminatedRelationEpoch: 1, BarrierRevision: 1}

	// Plain rotate produces no pair proof.
	if _, _, err := r.RotateShareToken("tok-2", 10000, 100); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.HasRotateReceiptFor(pairID, 1); ok {
		t.Fatal("plain rotate must not satisfy the pair barrier")
	}

	// Typed rotate binds the barrier revision.
	token, receipt, outcome, err := r.RotateForPairReactivation(proof, "tok-3", 10000, 200)
	if err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("typed rotate = %v %v", outcome, err)
	}
	if receipt.BarrierRevision != 1 || receipt.TokenID != token.TokenID || receipt.TokenGeneration != token.TokenGeneration {
		t.Fatalf("receipt = %+v token=%+v", receipt, token)
	}
	if _, ok := r.HasRotateReceiptFor(pairID, 1); !ok {
		t.Fatal("rotate receipt missing")
	}
	// A new barrier revision invalidates the old receipt.
	if _, ok := r.HasRotateReceiptFor(pairID, 2); ok {
		t.Fatal("old barrier receipt must not match a new revision")
	}
	// Garbage proof is rejected.
	if _, _, _, err := r.RotateForPairReactivation(BarrierProof{}, "tok-x", 1000, 300); err != ErrBarrierProofMismatch {
		t.Fatalf("empty proof err = %v", err)
	}
}
