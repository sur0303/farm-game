package mail

import (
	"context"
	"testing"
)

// FIX-1 regression: while the expire worker has not yet materialized the
// expired flag (expire_at passed, expired_at=0), Delete/MarkRead/
// ApplyClaimProjection must still perform the summary/index transition
// exactly once, and the later expire pass must not double-decrement.

func shortLivedRewardEntry(t *testing.T, player uint64, seq int, now int64) MailEntry {
	t.Helper()
	p := rewardDelivery(player, seq, now)
	p.RetentionMs = 1000
	entry, err := BuildMailEntry(p, LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func TestFix1DeleteInExpireDelayWindow(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	player := uint64(10001)
	store.SetBucketOf(player, 7)
	entry := shortLivedRewardEntry(t, player, 1, now)
	mustInsert(t, store, entry)

	// expire_at passes; the expire worker has NOT run.
	now = entry.ExpireAtMs + 1
	proof := ClaimProof{MailID: entry.MailID, Claimed: true, ClaimMaterialHash: entry.ClaimMaterialHash, ReceiptRef: "rcpt-1"}
	res, err := store.DeleteMail(ctx, player, entry.MailID, "op-del", AllowDeleteWithProof(proof))
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != MutationDeleted {
		t.Fatalf("delete: %s", res.Status)
	}
	// The old state was still materially counted, so every counter moves.
	if res.Summary.VisibleCount != 0 || res.Summary.UnreadCount != 0 || res.Summary.ClaimableCount != 0 {
		t.Fatalf("summary after delete: %+v", res.Summary)
	}
	// The original expire DueRef is stale now; after re-registration the
	// expire pass is NO_CHANGE and must not decrement again.
	oldRef := MailDueRefV1{Kind: DueKindExpireVisibility, PlayerID: player, MailID: entry.MailID, MailVersion: 1, ExpectedStateVersion: 1}
	stale, err := store.ExpireVisibility(ctx, oldRef)
	if err != nil {
		t.Fatal(err)
	}
	if stale.Status != ExpireStaleRef || stale.CurrentStateVersion != 2 {
		t.Fatalf("stale expire ref: %+v", stale)
	}
	freshRef := MailDueRefV1{Kind: DueKindExpireVisibility, PlayerID: player, MailID: entry.MailID, MailVersion: 1, ExpectedStateVersion: stale.CurrentStateVersion}
	if err := store.RegisterDueRef(ctx, freshRef, entry.ExpireAtMs); err != nil {
		t.Fatal(err)
	}
	again, err := store.ExpireVisibility(ctx, freshRef)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != ExpireNoChange {
		t.Fatalf("expire after delete: %s", again.Status)
	}
	summary, err := store.GetSummary(ctx, player)
	if err != nil {
		t.Fatal(err)
	}
	if summary.VisibleCount != 0 || summary.UnreadCount != 0 || summary.ClaimableCount != 0 {
		t.Fatalf("expire must not double-decrement: %+v", summary)
	}
}

func TestFix1MarkReadInExpireDelayWindow(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	player := uint64(10001)
	store.SetBucketOf(player, 7)
	entry := shortLivedRewardEntry(t, player, 1, now)
	mustInsert(t, store, entry)

	now = entry.ExpireAtMs + 1
	res, err := store.MarkMailRead(ctx, player, entry.MailID, "op-read")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != MutationReadMarked || res.Summary.UnreadCount != 0 {
		t.Fatalf("mark read: %+v", res)
	}
	// Expire later: visible and claimable drain exactly once; unread stays.
	expireRes, err := store.ExpireVisibility(ctx, MailDueRefV1{
		Kind: DueKindExpireVisibility, PlayerID: player, MailID: entry.MailID,
		MailVersion: 1, ExpectedStateVersion: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if expireRes.Status != ExpireApplied {
		t.Fatalf("expire: %s", expireRes.Status)
	}
	summary, err := store.GetSummary(ctx, player)
	if err != nil {
		t.Fatal(err)
	}
	if summary.VisibleCount != 0 || summary.UnreadCount != 0 || summary.ClaimableCount != 0 {
		t.Fatalf("counters must drain exactly once: %+v", summary)
	}
}

func TestFix1ProjectionInExpireDelayWindow(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	player := uint64(10001)
	store.SetBucketOf(player, 7)
	entry := shortLivedRewardEntry(t, player, 1, now)
	mustInsert(t, store, entry)

	now = entry.ExpireAtMs + 1
	order := ClaimProjectionOrder{
		RecipientPlayerID: player, WorkID: "w-1", WorkVersion: 1,
		MailID: entry.MailID, ClaimReceiptRef: "rcpt-1",
		ClaimMaterialHash: entry.ClaimMaterialHash, ClaimedAtMs: now,
	}
	res, err := store.ApplyMailClaimProjection(ctx, order)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != ProjectionApplied || res.Summary.ClaimableCount != 0 {
		t.Fatalf("projection: %+v", res)
	}
	expireRes, err := store.ExpireVisibility(ctx, MailDueRefV1{
		Kind: DueKindExpireVisibility, PlayerID: player, MailID: entry.MailID,
		MailVersion: 1, ExpectedStateVersion: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if expireRes.Status != ExpireApplied {
		t.Fatalf("expire: %s", expireRes.Status)
	}
	summary, err := store.GetSummary(ctx, player)
	if err != nil {
		t.Fatal(err)
	}
	if summary.ClaimableCount != 0 || summary.VisibleCount != 0 {
		t.Fatalf("claimable must decrement exactly once: %+v", summary)
	}
}

// FIX-2 regression: a claim proof only authorizes deletion when it fully
// binds the target mail.
func TestFix2ClaimProofBinding(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	player := uint64(10001)
	mailA := mustBuild(t, rewardDelivery(player, 1, now))
	mailB := mustBuild(t, rewardDelivery(player, 2, now))
	mustInsert(t, store, mailA)
	mustInsert(t, store, mailB)

	// A's proof must not delete B.
	proofA := ClaimProof{MailID: mailA.MailID, Claimed: true, ClaimMaterialHash: mailA.ClaimMaterialHash, ReceiptRef: "rcpt-a"}
	if _, err := store.DeleteMail(ctx, player, mailB.MailID, "op-x1", AllowDeleteWithProof(proofA)); err == nil {
		t.Fatal("proof of A must not authorize deleting B")
	}
	if _, state, err := store.GetEntry(ctx, player, mailB.MailID); err != nil || state.DeletedAtMs != 0 {
		t.Fatalf("B must survive: %+v %v", state, err)
	}
	// Decision layer: A's proof on B's entry is not a proof.
	_, stateB, err := store.GetEntry(ctx, player, mailB.MailID)
	if err != nil {
		t.Fatal(err)
	}
	if d := DecideDeleteEligibility(mailB, stateB, now, &proofA); d != DeleteRejectUnclaimed {
		t.Fatalf("decision with foreign proof: %s", d)
	}
	// Well-formed but wrong hash is rejected.
	wrongHash := ClaimProof{MailID: mailB.MailID, Claimed: true, ClaimMaterialHash: mailA.ClaimMaterialHash, ReceiptRef: "rcpt-b"}
	if _, err := store.DeleteMail(ctx, player, mailB.MailID, "op-x2", AllowDeleteWithProof(wrongHash)); err == nil {
		t.Fatal("hash mismatch must be rejected")
	}
	// Empty receipt ref is rejected structurally.
	noReceipt := ClaimProof{MailID: mailB.MailID, Claimed: true, ClaimMaterialHash: mailB.ClaimMaterialHash}
	if err := AllowDeleteWithProof(noReceipt).Validate(mailB.MailID); err == nil {
		t.Fatal("empty receipt ref must be rejected")
	}
	// Not-claimed proof is rejected structurally.
	notClaimed := ClaimProof{MailID: mailB.MailID, ClaimMaterialHash: mailB.ClaimMaterialHash, ReceiptRef: "rcpt-b"}
	if err := AllowDeleteWithProof(notClaimed).Validate(mailB.MailID); err == nil {
		t.Fatal("unclaimed proof must be rejected")
	}
	// Plain ALLOW must not carry a proof.
	if err := (DeleteAuthorization{Eligibility: EligibilityAllow, ClaimProof: &proofA}).Validate(mailB.MailID); err == nil {
		t.Fatal("plain ALLOW with proof must be rejected")
	}
	// The correct proof deletes B.
	res, err := store.DeleteMail(ctx, player, mailB.MailID, "op-ok",
		AllowDeleteWithProof(ClaimProof{MailID: mailB.MailID, Claimed: true, ClaimMaterialHash: mailB.ClaimMaterialHash, ReceiptRef: "rcpt-b"}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != MutationDeleted {
		t.Fatalf("correct proof: %s", res.Status)
	}
}
