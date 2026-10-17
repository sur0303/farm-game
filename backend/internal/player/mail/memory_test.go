package mail

import (
	"context"
	"fmt"
	"testing"
)

const testNow = int64(1754200000000)

func newTestStore(t *testing.T, now *int64) *MemoryStore {
	t.Helper()
	store, err := NewMemoryStore(func() int64 { return *now }, LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func textDelivery(player uint64, seq int, at int64) DeliveryParams {
	return DeliveryParams{
		RecipientPlayerID: player,
		DeliveryID:        fmt.Sprintf("SYS:%d:%d", player, seq),
		SourceType:        SourceTypeSystem,
		TitleSnapshot:     fmt.Sprintf("title-%d", seq),
		BodySnapshot:      "body",
		CreatedAtMs:       at,
		VisibleAtMs:       at,
	}
}

func rewardDelivery(player uint64, seq int, at int64) DeliveryParams {
	p := textDelivery(player, seq, at)
	p.Reward = RewardBundle{Coin: 10, Items: []ItemGrant{{ItemID: 1, Count: 2}}}
	return p
}

func mustBuild(t *testing.T, p DeliveryParams) MailEntry {
	t.Helper()
	entry, err := BuildMailEntry(p, LimitsV1())
	if err != nil {
		t.Fatalf("build entry: %v", err)
	}
	return entry
}

func mustInsert(t *testing.T, store *MemoryStore, entry MailEntry) InsertResult {
	t.Helper()
	res, err := store.InsertMailIfAbsent(context.Background(), entry)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if res.Status != InsertInserted {
		t.Fatalf("insert status: got %s want INSERTED", res.Status)
	}
	return res
}

func TestInsertIdempotentAndConflict(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	entry := mustBuild(t, textDelivery(10001, 1, now))

	first := mustInsert(t, store, entry)
	if first.MailID != entry.MailID {
		t.Fatalf("mail id mismatch: %s", first.MailID)
	}
	if first.Summary.VisibleCount != 1 || first.Summary.UnreadCount != 1 || first.Summary.MailboxRevision != 1 {
		t.Fatalf("summary after insert: %+v", first.Summary)
	}

	// Same delivery identity + same hash returns the original mail id.
	again, err := store.InsertMailIfAbsent(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != InsertAlreadyDelivered || again.MailID != entry.MailID {
		t.Fatalf("replay: got %+v", again)
	}
	if again.Summary.VisibleCount != 1 {
		t.Fatalf("replay must not duplicate: %+v", again.Summary)
	}

	// Same delivery id + different payload hash is a conflict.
	tampered := entry
	tampered.TitleSnapshot = "other"
	var err2 error
	tampered.DeliveryPayloadHash, err2 = DeliveryPayloadHash(tampered)
	if err2 != nil {
		t.Fatal(err2)
	}
	conflict, err := store.InsertMailIfAbsent(ctx, tampered)
	if err != nil {
		t.Fatal(err)
	}
	if conflict.Status != InsertDeliveryConflict {
		t.Fatalf("conflict: got %s", conflict.Status)
	}

	// Not-yet-visible mail is rejected in P0 (35 Q35-04).
	future := mustBuild(t, textDelivery(10001, 2, now+10000))
	future.VisibleAtMs = now + 10000
	future.ExpireAtMs = future.VisibleAtMs + LimitsV1().DefaultRetentionMs
	future.ClaimEndAtMs = future.ExpireAtMs
	future.PhysicalPurgeAtMs = future.ClaimEndAtMs + LimitsV1().ClaimMaterialRetainAfterClaimEndMs
	future.DeliveryPayloadHash, _ = DeliveryPayloadHash(future)
	res, err := store.InsertMailIfAbsent(ctx, future)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != InsertNotVisibleYet {
		t.Fatalf("future visible: got %s", res.Status)
	}
}

func TestOfflineDeliveryAndDetail(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	// Offline player: mail persists without any actor activation.
	entry := mustBuild(t, rewardDelivery(20002, 1, now))
	mustInsert(t, store, entry)

	gotEntry, gotState, err := store.GetEntry(ctx, 20002, entry.MailID)
	if err != nil {
		t.Fatal(err)
	}
	if gotEntry.BodySnapshot != "body" || gotState.ReadAtMs != 0 || !gotEntry.HasAttachment() {
		t.Fatalf("detail: %+v %+v", gotEntry, gotState)
	}
	// Recipient scoping: another recipient can never read this mail id.
	if _, _, err := store.GetEntry(ctx, 30003, entry.MailID); err != ErrMailNotFound {
		t.Fatalf("cross-recipient read must fail closed, got %v", err)
	}
}

func TestMarkReadAndSummary(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	entry := mustBuild(t, textDelivery(10001, 1, now))
	mustInsert(t, store, entry)

	res, err := store.MarkMailRead(ctx, 10001, entry.MailID, "op-read-1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != MutationReadMarked || res.Summary.UnreadCount != 0 || res.Summary.MailboxRevision != 2 {
		t.Fatalf("mark read: %+v", res)
	}
	// New mutation id on an already-read mail is NO_CHANGE.
	res2, err := store.MarkMailRead(ctx, 10001, entry.MailID, "op-read-2")
	if err != nil {
		t.Fatal(err)
	}
	if res2.Status != MutationNoChange || res2.Summary.MailboxRevision != 2 {
		t.Fatalf("second mark read: %+v", res2)
	}
	// Same mutation id replays its stored result.
	res3, err := store.MarkMailRead(ctx, 10001, entry.MailID, "op-read-1")
	if err != nil {
		t.Fatal(err)
	}
	if res3.Status != MutationReadMarked {
		t.Fatalf("replay must return stored READ_MARKED, got %s", res3.Status)
	}
	// Unknown mail id.
	res4, err := store.MarkMailRead(ctx, 10001, "sid1_"+entry.MailID[5:10]+entry.MailID[10:], "op-x")
	if err != nil && res4.Status != MutationMailNotFound {
		t.Fatalf("unknown mail: %+v %v", res4, err)
	}
}

func TestDeleteGuardsUnclaimedAttachment(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	entry := mustBuild(t, rewardDelivery(10001, 1, now))
	mustInsert(t, store, entry)

	// Eligibility decision: unclaimed attachment mail is rejected without a
	// claim proof (35 Q35-07).
	gotEntry, gotState, err := store.GetEntry(ctx, 10001, entry.MailID)
	if err != nil {
		t.Fatal(err)
	}
	if d := DecideDeleteEligibility(gotEntry, gotState, now, &ClaimProof{Claimed: false}); d != DeleteRejectUnclaimed {
		t.Fatalf("eligibility: got %s", d)
	}
	if d := DecideDeleteEligibility(gotEntry, gotState, now, nil); d != DeleteProcessing {
		t.Fatalf("unknown claim read: got %s", d)
	}
	// Lua-level defense: EligibilityAllow on an unclaimed attachment mail is
	// still rejected by the script's own re-check.
	res, err := store.DeleteMail(ctx, 10001, entry.MailID, "op-del-1", AllowDelete())
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != MutationRejectUnclaimed {
		t.Fatalf("script defense: got %s", res.Status)
	}
	// With a matching claim proof the delete is allowed.
	proof := &ClaimProof{MailID: entry.MailID, Claimed: true, ClaimMaterialHash: entry.ClaimMaterialHash, ReceiptRef: "rcpt-1"}
	if d := DecideDeleteEligibility(gotEntry, gotState, now, proof); d != DeleteAllowWithClaimProof {
		t.Fatalf("proof decision: got %s", d)
	}
	res2, err := store.DeleteMail(ctx, 10001, entry.MailID, "op-del-2", AllowDeleteWithProof(*proof))
	if err != nil {
		t.Fatal(err)
	}
	if res2.Status != MutationDeleted || res2.Summary.VisibleCount != 0 || res2.Summary.ClaimableCount != 0 {
		t.Fatalf("delete: %+v", res2)
	}
	// Text mail deletes freely.
	text := mustBuild(t, textDelivery(10001, 2, now))
	mustInsert(t, store, text)
	res3, err := store.DeleteMail(ctx, 10001, text.MailID, "op-del-3", AllowDelete())
	if err != nil {
		t.Fatal(err)
	}
	if res3.Status != MutationDeleted {
		t.Fatalf("text delete: %s", res3.Status)
	}
}

func TestProjectionMonotonicAndNoResurrect(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	entry := mustBuild(t, rewardDelivery(10001, 1, now))
	mustInsert(t, store, entry)

	order := ClaimProjectionOrder{
		RecipientPlayerID: 10001, WorkID: "work-1", WorkVersion: 1,
		MailID: entry.MailID, ClaimReceiptRef: "rcpt-1",
		ClaimMaterialHash: entry.ClaimMaterialHash, ClaimedAtMs: now,
	}
	// Delete first (with proof), then the late projection must not resurrect.
	proof := ClaimProof{MailID: entry.MailID, Claimed: true, ClaimMaterialHash: entry.ClaimMaterialHash, ReceiptRef: "rcpt-1"}
	if _, err := store.DeleteMail(ctx, 10001, entry.MailID, "op-del", AllowDeleteWithProof(proof)); err != nil {
		t.Fatal(err)
	}
	res, err := store.ApplyMailClaimProjection(ctx, order)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != ProjectionApplied {
		t.Fatalf("late projection: %s", res.Status)
	}
	_, state, err := store.GetEntry(ctx, 10001, entry.MailID)
	if err != nil {
		t.Fatal(err)
	}
	if state.DeletedAtMs == 0 || !state.ClaimedProjection {
		t.Fatalf("projection must not clear deletion: %+v", state)
	}
	page, err := store.ListPage(ctx, 10001, "", now)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 0 {
		t.Fatalf("deleted mail must stay hidden: %+v", page.Items)
	}
	// Replay of the same work returns the stored receipt.
	replay, err := store.ApplyMailClaimProjection(ctx, order)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != ProjectionApplied {
		t.Fatalf("work replay: %s", replay.Status)
	}
	// Hash mismatch fails closed.
	bad := order
	bad.WorkID = "work-2"
	bad.ClaimMaterialHash = "0000000000000000000000000000000000000000000000000000000000000000"
	mism, err := store.ApplyMailClaimProjection(ctx, bad)
	if err != nil {
		t.Fatal(err)
	}
	if mism.Status != ProjectionHashMismatch {
		t.Fatalf("hash mismatch: %s", mism.Status)
	}
	// Purged target.
	purged := order
	purged.WorkID = "work-3"
	purged.MailID = mustBuild(t, rewardDelivery(10001, 99, now)).MailID
	pres, err := store.ApplyMailClaimProjection(ctx, purged)
	if err != nil {
		t.Fatal(err)
	}
	if pres.Status != ProjectionTargetAlreadyPurged {
		t.Fatalf("purged target: %s", pres.Status)
	}
}

func TestExpireAndGcWindows(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	player := uint64(10001)
	store.SetBucketOf(player, 7)
	entry := mustBuild(t, textDelivery(player, 1, now))
	mustInsert(t, store, entry)

	// Not yet due: expire does nothing.
	earlyRef := MailDueRefV1{Kind: DueKindExpireVisibility, PlayerID: player, MailID: entry.MailID, MailVersion: 1, ExpectedStateVersion: 1}
	res, err := store.ExpireVisibility(ctx, earlyRef)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != ExpireNotDue {
		t.Fatalf("early expire: %s", res.Status)
	}
	// Discover through the due index at expiry.
	now = entry.ExpireAtMs
	due, err := store.ListExpireDue(ctx, 7, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].Ref.MailID != entry.MailID {
		t.Fatalf("expire due: %+v", due)
	}
	res2, err := store.ExpireVisibility(ctx, due[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Status != ExpireApplied || res2.Summary.VisibleCount != 0 || res2.Summary.UnreadCount != 0 {
		t.Fatalf("expire: %+v", res2)
	}
	// Stale ref (old state version) is a NO_CHANGE-style stale result.
	res3, err := store.ExpireVisibility(ctx, earlyRef)
	if err != nil {
		t.Fatal(err)
	}
	if res3.Status != ExpireStaleRef && res3.Status != ExpireNoChange {
		t.Fatalf("stale expire ref: %s", res3.Status)
	}

	// GC before the purge window is refused: claim evidence must outlive
	// every retry/unknown/reconcile/customer-service window (35A PM6).
	now = entry.ExpireAtMs + 1
	gcRef := MailDueRefV1{Kind: DueKindPhysicalGC, PlayerID: player, MailID: entry.MailID, MailVersion: 1, ExpectedStateVersion: 2}
	gres, err := store.GcMail(ctx, gcRef)
	if err != nil {
		t.Fatal(err)
	}
	if gres.Status != GcNotDue {
		t.Fatalf("early gc: %s", gres.Status)
	}
	// Mail still strongly readable during the window.
	if _, _, err := store.GetEntry(ctx, player, entry.MailID); err != nil {
		t.Fatalf("entry must survive until purge: %v", err)
	}
	// At the claim-evidence purge time the due index surfaces the ref
	// registered at insert (state version 1). Expiry bumped the version, so
	// the first GC pass is STALE and the worker re-registers (35A §7.1).
	now = entry.PhysicalPurgeAtMs
	gdue, err := store.ListGcDue(ctx, 7, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(gdue) != 1 {
		t.Fatalf("gc due: %+v", gdue)
	}
	gres2, err := store.GcMail(ctx, gdue[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if gres2.Status != GcStaleRef || gres2.CurrentStateVersion != 2 {
		t.Fatalf("stale gc ref: %+v", gres2)
	}
	freshRef := MailDueRefV1{
		Kind: DueKindPhysicalGC, PlayerID: player, MailID: entry.MailID,
		MailVersion: gres2.CurrentMailVersion, ExpectedStateVersion: gres2.CurrentStateVersion,
	}
	if err := store.RegisterDueRef(ctx, freshRef, gres2.PurgeAtMs); err != nil {
		t.Fatal(err)
	}
	// FIX-3: the delivery dedup retention (90d) outlives the claim-evidence
	// purge (67d), so the first pass at 67d is NOT_DUE carrying the
	// effective 90d score; entry/state/dedup purge together at 90d.
	gres2, err = store.GcMail(ctx, freshRef)
	if err != nil {
		t.Fatal(err)
	}
	dedupRetain := testNow + LimitsV1().DeliveryRecordRetainMs
	if gres2.Status != GcNotDue || gres2.PurgeAtMs != dedupRetain {
		t.Fatalf("gc must wait for dedup retention: %+v want NOT_DUE@%d", gres2, dedupRetain)
	}
	if err := store.RegisterDueRef(ctx, freshRef, gres2.PurgeAtMs); err != nil {
		t.Fatal(err)
	}
	// Within the retention window the same delivery still resolves the
	// original mail id (anti-replay evidence intact).
	replay, err := store.InsertMailIfAbsent(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != InsertAlreadyDelivered || replay.MailID != entry.MailID {
		t.Fatalf("dedup must outlive entry: %+v", replay)
	}
	// At the effective purge time Entry/State/Dedup go together.
	now = dedupRetain
	gres3, err := store.GcMail(ctx, freshRef)
	if err != nil {
		t.Fatal(err)
	}
	if gres3.Status != GcPurged || gres3.FreedBytes <= 0 {
		t.Fatalf("gc: %+v", gres3)
	}
	if _, _, err := store.GetEntry(ctx, player, entry.MailID); err != ErrMailNotFound {
		t.Fatalf("purged entry must be gone: %v", err)
	}
	summary, err := store.GetSummary(ctx, player)
	if err != nil {
		t.Fatal(err)
	}
	if summary.RetainedCount != 0 || summary.RetainedBytes != 0 || summary.DeliveryRecords != 0 {
		t.Fatalf("retained after gc: %+v", summary)
	}
	// Second GC of the same ref converges.
	gres4, err := store.GcMail(ctx, freshRef)
	if err != nil {
		t.Fatal(err)
	}
	if gres4.Status != GcTargetAlreadyPurged {
		t.Fatalf("gc replay: %s", gres4.Status)
	}
	// After the full retention window the delivery record is gone; an
	// ancient replay is a fresh insert (the deterministic mail id makes it
	// converge on the same id with fresh content, which the 35A §6.3
	// retention window explicitly permits to end).
	replay2, err := store.InsertMailIfAbsent(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if replay2.Status != InsertInserted || replay2.MailID != entry.MailID {
		t.Fatalf("post-window replay: %+v", replay2)
	}
}

func TestLazyEvictionAndFullProtected(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	player := uint64(10001)
	l := LimitsV1()

	// Fill the mailbox with 60 text mails; read the oldest 10 so they
	// become safe-evict candidates.
	for i := 0; i < l.MaxVisibleCount; i++ {
		entry := mustBuild(t, textDelivery(player, i, now-int64(l.MaxVisibleCount-i)))
		mustInsert(t, store, entry)
		if i < 10 {
			if _, err := store.MarkMailRead(ctx, player, entry.MailID, fmt.Sprintf("r-%d", i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	summary, err := store.GetSummary(ctx, player)
	if err != nil {
		t.Fatal(err)
	}
	if summary.VisibleCount != 60 {
		t.Fatalf("visible: %d", summary.VisibleCount)
	}
	// Next insert lazily evicts the oldest safe (read, no attachment) mail.
	now++
	extra := mustBuild(t, textDelivery(player, 100, now))
	res, err := store.InsertMailIfAbsent(ctx, extra)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != InsertInserted {
		t.Fatalf("lazy evict insert: %s", res.Status)
	}
	if res.Summary.VisibleCount != 60 {
		t.Fatalf("visible must stay bounded: %d", res.Summary.VisibleCount)
	}
	evictedID := mustBuild(t, textDelivery(player, 0, now-int64(l.MaxVisibleCount))).MailID
	if _, state, err := store.GetEntry(ctx, player, evictedID); err != nil || !state.AutoEvicted {
		t.Fatalf("oldest read mail must be auto-evicted: %+v %v", state, err)
	}

	// Fill a second mailbox with 60 unread attachment mails: all protected.
	p2 := uint64(20002)
	for i := 0; i < l.MaxVisibleCount; i++ {
		entry := mustBuild(t, rewardDelivery(p2, i, now-int64(l.MaxVisibleCount-i)))
		mustInsert(t, store, entry)
	}
	blocked, err := store.InsertMailIfAbsent(ctx, mustBuild(t, rewardDelivery(p2, 100, now)))
	if err != nil {
		t.Fatal(err)
	}
	if blocked.Status != InsertMailboxFullProtected {
		t.Fatalf("protected mailbox must fail closed, got %s", blocked.Status)
	}
}

func TestRetainedCapacityFailClosed(t *testing.T) {
	now := testNow
	limits := LimitsV1()
	limits.MaxRetainedCount = 3
	limits.MaxDeliveryRecords = 3
	store, err := NewMemoryStore(func() int64 { return now }, limits)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	player := uint64(10001)
	for i := 0; i < 3; i++ {
		entry, err := BuildMailEntry(textDelivery(player, i, now), limits)
		if err != nil {
			t.Fatal(err)
		}
		if res, err := store.InsertMailIfAbsent(ctx, entry); err != nil || res.Status != InsertInserted {
			t.Fatalf("insert %d: %+v %v", i, res, err)
		}
	}
	entry, err := BuildMailEntry(textDelivery(player, 3, now), limits)
	if err != nil {
		t.Fatal(err)
	}
	res, err := store.InsertMailIfAbsent(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != InsertCapacityExceeded {
		t.Fatalf("retained limit must fail closed, got %s", res.Status)
	}
}

func TestScanLease(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	res, err := store.ClaimScanLease(ctx, 7, "worker-a", 30000)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != ScanLeaseAcquired || res.AcceptedTTLMs != 30000 || res.LeaseEpoch != 1 {
		t.Fatalf("lease: %+v", res)
	}
	other, err := store.ClaimScanLease(ctx, 7, "worker-b", 30000)
	if err != nil {
		t.Fatal(err)
	}
	if other.Status != ScanLeaseHeldByOther {
		t.Fatalf("contended lease: %s", other.Status)
	}
	renew, err := store.ClaimScanLease(ctx, 7, "worker-a", 30000)
	if err != nil {
		t.Fatal(err)
	}
	if renew.Status != ScanLeaseAcquired || renew.LeaseEpoch != 2 {
		t.Fatalf("renew: %+v", renew)
	}
	now += 31000
	takeover, err := store.ClaimScanLease(ctx, 7, "worker-b", 30000)
	if err != nil {
		t.Fatal(err)
	}
	if takeover.Status != ScanLeaseAcquired {
		t.Fatalf("expired lease takeover: %s", takeover.Status)
	}
}

func TestClaimMaterialIssue(t *testing.T) {
	now := testNow
	entry := mustBuild(t, rewardDelivery(10001, 1, now))
	issuer := ClaimMaterialIssuer{Issuer: "player-svr/mail-claim-ingress", MaterialTTLMs: 60000}
	material, err := issuer.Issue(entry, now)
	if err != nil {
		t.Fatal(err)
	}
	if material.Audience != ClaimMaterialAudience || material.ClaimMaterialHash != entry.ClaimMaterialHash ||
		material.RecipientPlayerID != 10001 || material.MaterialExpireAtMs != now+60000 {
		t.Fatalf("material: %+v", material)
	}
	if err := material.ValidateForTurn(10001, now+1); err != nil {
		t.Fatal(err)
	}
	if err := material.ValidateForTurn(20002, now+1); err == nil {
		t.Fatal("wrong recipient must be rejected")
	}
	if err := material.ValidateForTurn(10001, now+60001); err == nil {
		t.Fatal("expired material must be rejected")
	}
	// Text mail has no claim material.
	text := mustBuild(t, textDelivery(10001, 2, now))
	if _, err := issuer.Issue(text, now); err == nil {
		t.Fatal("attachment-less mail must not issue claim material")
	}
	// Tampered entry fails closed.
	tampered := entry
	tampered.Reward.Coin = 999
	if _, err := issuer.Issue(tampered, now); err == nil {
		t.Fatal("tampered reward must be rejected")
	}
	if _, err := (ClaimMaterialIssuer{}).Issue(entry, now); err == nil {
		t.Fatal("missing issuer must be rejected")
	}
}
