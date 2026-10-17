package playermail

import (
	"context"
	"testing"

	"farmgame/internal/player/mail"
	"github.com/redis/go-redis/v9"
)

func TestInsertUnknownAfterApplyReentersCommitted(t *testing.T) {
	drop := &dropClient{}
	rig := newDropRig(t, drop)
	ctx := context.Background()
	entry := textEntry(t, 10001, "D:1", rig.now)

	drop.armAfter(1)
	res, err := rig.store.InsertMailIfAbsent(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.InsertTimeoutUnknown {
		t.Fatalf("status=%s want TIMEOUT_UNKNOWN", res.Status)
	}
	// Re-enter with the identical delivery identity: the committed insert
	// resolves through the delivery dedup record, never a duplicate.
	again, err := rig.store.InsertMailIfAbsent(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != mail.InsertAlreadyDelivered || again.MailID != entry.MailID {
		t.Fatalf("re-enter: %+v", again)
	}
	if again.Summary.VisibleCount != 1 || again.Summary.UnreadCount != 1 {
		t.Fatalf("no double apply: %+v", again.Summary)
	}
}

func TestInsertUnknownBeforeApplyReentriesClean(t *testing.T) {
	drop := &dropClient{}
	rig := newDropRig(t, drop)
	ctx := context.Background()
	entry := textEntry(t, 10001, "D:1", rig.now)

	drop.armBefore(1)
	res, err := rig.store.InsertMailIfAbsent(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.InsertTimeoutUnknown {
		t.Fatalf("status=%s want TIMEOUT_UNKNOWN", res.Status)
	}
	// The script never executed; the same delivery identity inserts once.
	again, err := rig.store.InsertMailIfAbsent(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != mail.InsertInserted {
		t.Fatalf("re-enter: %s", again.Status)
	}
	if again.Summary.VisibleCount != 1 {
		t.Fatalf("exactly once: %+v", again.Summary)
	}
}

func TestMarkReadUnknownAfterApplyReplaysReceipt(t *testing.T) {
	drop := &dropClient{}
	rig := newDropRig(t, drop)
	ctx := context.Background()
	entry := textEntry(t, 10001, "D:1", rig.now)
	mustInsert(t, rig.store, entry)

	drop.armAfter(1)
	res, err := rig.store.MarkMailRead(ctx, 10001, entry.MailID, "op-read")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.MutationTimeoutUnknown {
		t.Fatalf("status=%s want TIMEOUT_UNKNOWN", res.Status)
	}
	again, err := rig.store.MarkMailRead(ctx, 10001, entry.MailID, "op-read")
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != mail.MutationReadMarked {
		t.Fatalf("receipt replay: %s", again.Status)
	}
	if again.Summary.UnreadCount != 0 {
		t.Fatalf("unread must decrement once: %+v", again.Summary)
	}
}

func TestMarkReadUnknownBeforeApplyNoReceipt(t *testing.T) {
	drop := &dropClient{}
	rig := newDropRig(t, drop)
	ctx := context.Background()
	entry := textEntry(t, 10001, "D:1", rig.now)
	mustInsert(t, rig.store, entry)

	drop.armBefore(1)
	res, err := rig.store.MarkMailRead(ctx, 10001, entry.MailID, "op-read")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.MutationTimeoutUnknown {
		t.Fatalf("status=%s", res.Status)
	}
	_, state, err := rig.store.GetEntry(ctx, 10001, entry.MailID)
	if err != nil {
		t.Fatal(err)
	}
	if state.ReadAtMs != 0 {
		t.Fatal("before-apply unknown must not mark read")
	}
	again, err := rig.store.MarkMailRead(ctx, 10001, entry.MailID, "op-read")
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != mail.MutationReadMarked {
		t.Fatalf("first real apply: %s", again.Status)
	}
}

func TestDeleteUnknownAfterApplyReplaysReceipt(t *testing.T) {
	drop := &dropClient{}
	rig := newDropRig(t, drop)
	ctx := context.Background()
	entry := textEntry(t, 10001, "D:1", rig.now)
	mustInsert(t, rig.store, entry)

	drop.armAfter(1)
	res, err := rig.store.DeleteMail(ctx, 10001, entry.MailID, "op-del", mail.AllowDelete())
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.MutationTimeoutUnknown {
		t.Fatalf("status=%s", res.Status)
	}
	again, err := rig.store.DeleteMail(ctx, 10001, entry.MailID, "op-del", mail.AllowDelete())
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != mail.MutationDeleted || again.Summary.VisibleCount != 0 {
		t.Fatalf("delete replay: %+v", again)
	}
}

func TestProjectionUnknownAfterApplyReplaysReceipt(t *testing.T) {
	drop := &dropClient{}
	rig := newDropRig(t, drop)
	ctx := context.Background()
	entry := rewardEntry(t, 10001, "D:1", rig.now)
	mustInsert(t, rig.store, entry)
	order := mail.ClaimProjectionOrder{
		RecipientPlayerID: 10001, WorkID: "work-1", WorkVersion: 1,
		MailID: entry.MailID, ClaimReceiptRef: "rcpt-1",
		ClaimMaterialHash: entry.ClaimMaterialHash, ClaimedAtMs: rig.now,
	}

	drop.armAfter(1)
	res, err := rig.store.ApplyMailClaimProjection(ctx, order)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.ProjectionTimeoutUnknown {
		t.Fatalf("status=%s", res.Status)
	}
	again, err := rig.store.ApplyMailClaimProjection(ctx, order)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != mail.ProjectionApplied || again.Summary.ClaimableCount != 0 {
		t.Fatalf("projection replay: %+v", again)
	}
}

func TestExpireUnknownAfterApplyConvergesStale(t *testing.T) {
	drop := &dropClient{}
	rig := newDropRig(t, drop)
	ctx := context.Background()
	player := uint64(10001)
	entry := textEntry(t, player, "D:1", rig.now)
	mustInsert(t, rig.store, entry)
	rig.setNow(entry.ExpireAtMs)
	ref := mail.MailDueRefV1{Kind: mail.DueKindExpireVisibility, PlayerID: player, MailID: entry.MailID, MailVersion: 1, ExpectedStateVersion: 1}

	drop.armAfter(1)
	res, err := rig.store.ExpireVisibility(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.ExpireTimeoutUnknown {
		t.Fatalf("status=%s", res.Status)
	}
	// The same ref is now stale (state version advanced): a stable
	// no-delete repair, never a double decrement.
	again, err := rig.store.ExpireVisibility(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != mail.ExpireStaleRef {
		t.Fatalf("converge: %s", again.Status)
	}
	summary, err := rig.store.GetSummary(ctx, player)
	if err != nil {
		t.Fatal(err)
	}
	if summary.VisibleCount != 0 || summary.UnreadCount != 0 {
		t.Fatalf("expire applied exactly once: %+v", summary)
	}
}

func TestGcUnknownAfterApplyConvergesPurged(t *testing.T) {
	drop := &dropClient{}
	rig := newDropRig(t, drop)
	ctx := context.Background()
	player := uint64(10001)
	entry := textEntry(t, player, "D:1", rig.now)
	mustInsert(t, rig.store, entry)
	rig.setNow(entry.ExpireAtMs)
	if _, err := rig.store.ExpireVisibility(ctx, mail.MailDueRefV1{
		Kind: mail.DueKindExpireVisibility, PlayerID: player, MailID: entry.MailID,
		MailVersion: 1, ExpectedStateVersion: 1,
	}); err != nil {
		t.Fatal(err)
	}
	rig.setNow(testNowMs + mail.LimitsV1().DeliveryRecordRetainMs)
	ref := mail.MailDueRefV1{Kind: mail.DueKindPhysicalGC, PlayerID: player, MailID: entry.MailID, MailVersion: 1, ExpectedStateVersion: 2}

	drop.armAfter(1)
	res, err := rig.store.GcMail(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.GcTimeoutUnknown {
		t.Fatalf("status=%s", res.Status)
	}
	again, err := rig.store.GcMail(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != mail.GcTargetAlreadyPurged {
		t.Fatalf("converge: %s", again.Status)
	}
	summary, err := rig.store.GetSummary(ctx, player)
	if err != nil {
		t.Fatal(err)
	}
	if summary.RetainedCount != 0 {
		t.Fatalf("gc applied exactly once: %+v", summary)
	}
}

func TestUnavailableNotSentOnClosedClient(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	entry := textEntry(t, 10001, "D:1", rig.now)

	closed := redis.NewClient(&redis.Options{Addr: rig.server.Addr()})
	store, err := NewStore(closed, Profile{Namespace: testNamespace(t), BucketCount: DefaultBucketCount}, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	res, err := store.InsertMailIfAbsent(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.InsertUnavailableNotSent {
		t.Fatalf("closed client must be UNAVAILABLE_NOT_SENT, got %s", res.Status)
	}
	// Nothing was written: the same delivery inserts cleanly afterwards.
	again, err := rig.store.InsertMailIfAbsent(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != mail.InsertInserted {
		t.Fatalf("after not-sent: %s", again.Status)
	}
}

func TestNoScriptRecovery(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	// A fresh store on a fresh miniredis has no cached scripts: the first
	// EVALSHA gets NOSCRIPT and the registry reloads transparently.
	entry := textEntry(t, 10001, "D:1", rig.now)
	res, err := rig.store.InsertMailIfAbsent(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.InsertInserted {
		t.Fatalf("noscript recovery: %s", res.Status)
	}
	// Flush the script cache: the next call recovers again.
	rig.server.FlushAll()
	entry2 := textEntry(t, 10001, "D:2", rig.now)
	res2, err := rig.store.InsertMailIfAbsent(ctx, entry2)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Status != mail.InsertInserted {
		t.Fatalf("noscript after flush: %s", res2.Status)
	}
}
