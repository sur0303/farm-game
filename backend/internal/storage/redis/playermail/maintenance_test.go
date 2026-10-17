package playermail

import (
	"context"
	"testing"

	"farmgame/internal/player/mail"
)

func TestRedisExpireAndGcWindows(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	entry := textEntry(t, player, "D:1", rig.now)
	mustInsert(t, rig.store, entry)
	bucket, err := rig.store.Keys().BucketOf(player)
	if err != nil {
		t.Fatal(err)
	}

	// Not yet due.
	earlyRef := mail.MailDueRefV1{Kind: mail.DueKindExpireVisibility, PlayerID: player, MailID: entry.MailID, MailVersion: 1, ExpectedStateVersion: 1}
	res, err := rig.store.ExpireVisibility(ctx, earlyRef)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.ExpireNotDue {
		t.Fatalf("early expire: %s", res.Status)
	}

	// Discover through the bucket due index at expiry.
	rig.setNow(entry.ExpireAtMs)
	due, err := rig.store.ListExpireDue(ctx, bucket, rig.now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].Ref.MailID != entry.MailID || due[0].DueAtMs != entry.ExpireAtMs {
		t.Fatalf("expire due: %+v", due)
	}
	res2, err := rig.store.ExpireVisibility(ctx, due[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Status != mail.ExpireApplied || res2.Summary.VisibleCount != 0 || res2.Summary.UnreadCount != 0 {
		t.Fatalf("expire: %+v", res2)
	}
	// The due member was consumed.
	due2, err := rig.store.ListExpireDue(ctx, bucket, rig.now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(due2) != 0 {
		t.Fatalf("expire member must be consumed: %+v", due2)
	}
	// Stale ref is a no-change repair, never a delete of newer versions.
	res3, err := rig.store.ExpireVisibility(ctx, earlyRef)
	if err != nil {
		t.Fatal(err)
	}
	if res3.Status != mail.ExpireNoChange && res3.Status != mail.ExpireStaleRef {
		t.Fatalf("stale expire ref: %s", res3.Status)
	}

	// GC before the purge window is refused (35A PM6). FIX-3: the returned
	// score is the effective purge = max(entry purge, dedup retention).
	dedupRetain := testNowMs + mail.LimitsV1().DeliveryRecordRetainMs
	gcRef := mail.MailDueRefV1{Kind: mail.DueKindPhysicalGC, PlayerID: player, MailID: entry.MailID, MailVersion: 1, ExpectedStateVersion: 2}
	gres, err := rig.store.GcMail(ctx, gcRef)
	if err != nil {
		t.Fatal(err)
	}
	if gres.Status != mail.GcNotDue || gres.PurgeAtMs != dedupRetain {
		t.Fatalf("early gc: %+v want NOT_DUE@%d", gres, dedupRetain)
	}
	if _, _, err := rig.store.GetEntry(ctx, player, entry.MailID); err != nil {
		t.Fatalf("entry must survive until purge: %v", err)
	}

	// At the claim-evidence purge time the insert-time ref (state version
	// 1) is stale: the worker re-registers, but the first pass is NOT_DUE
	// with the 90d effective score (FIX-3).
	rig.setNow(entry.PhysicalPurgeAtMs)
	gdue, err := rig.store.ListGcDue(ctx, bucket, rig.now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(gdue) != 1 {
		t.Fatalf("gc due: %+v", gdue)
	}
	gres2, err := rig.store.GcMail(ctx, gdue[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if gres2.Status != mail.GcStaleRef || gres2.CurrentStateVersion != 2 {
		t.Fatalf("stale gc ref: %+v", gres2)
	}
	freshRef := mail.MailDueRefV1{
		Kind: mail.DueKindPhysicalGC, PlayerID: player, MailID: entry.MailID,
		MailVersion: gres2.CurrentMailVersion, ExpectedStateVersion: gres2.CurrentStateVersion,
	}
	if err := rig.store.RegisterDueRef(ctx, freshRef, gres2.PurgeAtMs); err != nil {
		t.Fatal(err)
	}
	gres3, err := rig.store.GcMail(ctx, freshRef)
	if err != nil {
		t.Fatal(err)
	}
	if gres3.Status != mail.GcNotDue || gres3.PurgeAtMs != dedupRetain {
		t.Fatalf("gc must wait for dedup retention: %+v want NOT_DUE@%d", gres3, dedupRetain)
	}
	if err := rig.store.RegisterDueRef(ctx, freshRef, gres3.PurgeAtMs); err != nil {
		t.Fatal(err)
	}
	// Within the retention window the same delivery still resolves the
	// original mail id.
	replay, err := rig.store.InsertMailIfAbsent(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != mail.InsertAlreadyDelivered || replay.MailID != entry.MailID {
		t.Fatalf("dedup must outlive entry: %+v", replay)
	}
	// At the effective purge time Entry/State/Dedup go together.
	rig.setNow(dedupRetain)
	gres4, err := rig.store.GcMail(ctx, freshRef)
	if err != nil {
		t.Fatal(err)
	}
	if gres4.Status != mail.GcPurged || gres4.FreedBytes <= 0 {
		t.Fatalf("gc: %+v", gres4)
	}
	if _, _, err := rig.store.GetEntry(ctx, player, entry.MailID); err != mail.ErrMailNotFound {
		t.Fatalf("purged entry must be gone: %v", err)
	}
	summary, err := rig.store.GetSummary(ctx, player)
	if err != nil {
		t.Fatal(err)
	}
	if summary.RetainedCount != 0 || summary.RetainedBytes != 0 || summary.DeliveryRecords != 0 {
		t.Fatalf("retained after gc: %+v", summary)
	}
	// GC replay converges.
	gres5, err := rig.store.GcMail(ctx, freshRef)
	if err != nil {
		t.Fatal(err)
	}
	if gres5.Status != mail.GcTargetAlreadyPurged {
		t.Fatalf("gc replay: %s", gres5.Status)
	}
	// After the full retention window the delivery record is gone; an
	// ancient replay is a fresh insert converging on the deterministic id.
	replay2, err := rig.store.InsertMailIfAbsent(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if replay2.Status != mail.InsertInserted || replay2.MailID != entry.MailID {
		t.Fatalf("post-window replay: %+v", replay2)
	}
}

func TestRedisGcPreservesUnclaimedAttachment(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	entry := rewardEntry(t, player, "D:1", rig.now)
	mustInsert(t, rig.store, entry)

	// The mail is expired by time but the expire worker has not run: the
	// state carries no deleted/expired flag, so GC must not purge claimable
	// material early even at purge_at (35A PM6).
	rig.setNow(entry.PhysicalPurgeAtMs + 1)
	ref := mail.MailDueRefV1{Kind: mail.DueKindPhysicalGC, PlayerID: player, MailID: entry.MailID, MailVersion: 1, ExpectedStateVersion: 1}
	res, err := rig.store.GcMail(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.GcNotDue {
		t.Fatalf("gc of live-flag mail must be not-due, got %s", res.Status)
	}
	// After the expire worker flags it, GC still waits for the effective
	// purge (dedup retention, FIX-3) and then purges everything together.
	if _, err := rig.store.ExpireVisibility(ctx, mail.MailDueRefV1{
		Kind: mail.DueKindExpireVisibility, PlayerID: player, MailID: entry.MailID,
		MailVersion: 1, ExpectedStateVersion: 1,
	}); err != nil {
		t.Fatal(err)
	}
	res2, err := rig.store.GcMail(ctx, mail.MailDueRefV1{
		Kind: mail.DueKindPhysicalGC, PlayerID: player, MailID: entry.MailID,
		MailVersion: 1, ExpectedStateVersion: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res2.Status != mail.GcNotDue {
		t.Fatalf("gc before effective purge: %s", res2.Status)
	}
	rig.setNow(testNowMs + mail.LimitsV1().DeliveryRecordRetainMs)
	res3, err := rig.store.GcMail(ctx, mail.MailDueRefV1{
		Kind: mail.DueKindPhysicalGC, PlayerID: player, MailID: entry.MailID,
		MailVersion: 1, ExpectedStateVersion: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res3.Status != mail.GcPurged {
		t.Fatalf("gc at effective purge: %s", res3.Status)
	}
}

func TestRedisScanLease(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	res, err := rig.store.ClaimScanLease(ctx, 7, "worker-a", 30000)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.ScanLeaseAcquired || res.AcceptedTTLMs != 30000 || res.LeaseEpoch != 1 {
		t.Fatalf("lease: %+v", res)
	}
	other, err := rig.store.ClaimScanLease(ctx, 7, "worker-b", 30000)
	if err != nil {
		t.Fatal(err)
	}
	if other.Status != mail.ScanLeaseHeldByOther {
		t.Fatalf("contended: %s", other.Status)
	}
	renew, err := rig.store.ClaimScanLease(ctx, 7, "worker-a", 30000)
	if err != nil {
		t.Fatal(err)
	}
	if renew.Status != mail.ScanLeaseAcquired || renew.LeaseEpoch != 2 {
		t.Fatalf("renew: %+v", renew)
	}
	// TTL is clamped by the store, not the worker.
	clamped, err := rig.store.ClaimScanLease(ctx, 8, "worker-c", 99999999)
	if err != nil {
		t.Fatal(err)
	}
	if clamped.Status != mail.ScanLeaseAcquired || clamped.AcceptedTTLMs != mail.MaxScanLeaseTTLMs {
		t.Fatalf("clamp: %+v", clamped)
	}
	rig.setNow(rig.now + 31000)
	takeover, err := rig.store.ClaimScanLease(ctx, 7, "worker-b", 30000)
	if err != nil {
		t.Fatal(err)
	}
	if takeover.Status != mail.ScanLeaseAcquired {
		t.Fatalf("expired takeover: %s", takeover.Status)
	}
}
