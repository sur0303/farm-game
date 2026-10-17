package playermail

import (
	"context"
	"fmt"
	"testing"

	"farmgame/internal/player/mail"
)

func TestRedisInsertIdempotentAndConflict(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	entry := textEntry(t, 10001, "WELCOME:10001:1", rig.now)

	first := mustInsert(t, rig.store, entry)
	if first.MailID != entry.MailID {
		t.Fatalf("mail id: %s", first.MailID)
	}
	if first.Summary.VisibleCount != 1 || first.Summary.UnreadCount != 1 || first.Summary.MailboxRevision != 1 {
		t.Fatalf("summary: %+v", first.Summary)
	}

	again, err := rig.store.InsertMailIfAbsent(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != mail.InsertAlreadyDelivered || again.MailID != entry.MailID {
		t.Fatalf("replay: %+v", again)
	}
	if again.Summary.VisibleCount != 1 {
		t.Fatalf("replay must not duplicate: %+v", again.Summary)
	}

	tampered := entry
	tampered.TitleSnapshot = "other"
	tampered.DeliveryPayloadHash, err = mail.DeliveryPayloadHash(tampered)
	if err != nil {
		t.Fatal(err)
	}
	conflict, err := rig.store.InsertMailIfAbsent(ctx, tampered)
	if err != nil {
		t.Fatal(err)
	}
	if conflict.Status != mail.InsertDeliveryConflict {
		t.Fatalf("conflict: %s", conflict.Status)
	}

	future := textEntry(t, 10001, "FUTURE:1", rig.now)
	future.VisibleAtMs = rig.now + 10000
	res, err := rig.store.InsertMailIfAbsent(ctx, future)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.InsertNotVisibleYet {
		t.Fatalf("future visible: %s", res.Status)
	}
}

func TestRedisMarkReadFlows(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	entry := textEntry(t, 10001, "D:1", rig.now)
	mustInsert(t, rig.store, entry)

	res, err := rig.store.MarkMailRead(ctx, 10001, entry.MailID, "op-read-1")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.MutationReadMarked || res.Summary.UnreadCount != 0 || res.Summary.MailboxRevision != 2 {
		t.Fatalf("mark read: %+v", res)
	}
	if res.AppliedAtMs != rig.now {
		t.Fatalf("applied must be redis time: %d", res.AppliedAtMs)
	}
	// Same mutation id replays the stored receipt.
	replay, err := rig.store.MarkMailRead(ctx, 10001, entry.MailID, "op-read-1")
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != mail.MutationReadMarked || replay.AppliedAtMs != res.AppliedAtMs {
		t.Fatalf("receipt replay: %+v", replay)
	}
	// New mutation id on read mail: NO_CHANGE.
	res2, err := rig.store.MarkMailRead(ctx, 10001, entry.MailID, "op-read-2")
	if err != nil {
		t.Fatal(err)
	}
	if res2.Status != mail.MutationNoChange {
		t.Fatalf("second read: %s", res2.Status)
	}
	// Read mail without attachment joins the safe-evict index.
	bucket, err := rig.store.Keys().BucketOf(10001)
	if err != nil {
		t.Fatal(err)
	}
	safeKey, err := rig.store.Keys().SafeEvictIndex(bucket, 10001)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rig.server.ZScore(safeKey.String(), sortMemberOf(t, entry)); err != nil {
		t.Fatal("read text mail must be safe-evictable")
	}
}

func sortMemberOf(t *testing.T, entry mail.MailEntry) string {
	t.Helper()
	member, err := mail.EncodeSortMember(entry.CreatedAtMs, entry.MailID)
	if err != nil {
		t.Fatal(err)
	}
	return string(member)
}

func TestRedisDeleteGuards(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	entry := rewardEntry(t, 10001, "D:1", rig.now)
	mustInsert(t, rig.store, entry)

	// Script-level defense: unclaimed attachment mail rejects even when the
	// caller wrongly passes EligibilityAllow.
	res, err := rig.store.DeleteMail(ctx, 10001, entry.MailID, "op-del-1", mail.AllowDelete())
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.MutationRejectUnclaimed {
		t.Fatalf("unclaimed delete: %s", res.Status)
	}
	// Same mutation id replays the rejection (stable stored result).
	replay, err := rig.store.DeleteMail(ctx, 10001, entry.MailID, "op-del-1", mail.AllowDeleteWithProof(mail.ClaimProof{MailID: entry.MailID, Claimed: true, ClaimMaterialHash: entry.ClaimMaterialHash, ReceiptRef: "rcpt-1"}))
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != mail.MutationRejectUnclaimed {
		t.Fatalf("rejection replay must return the stored result, got %s", replay.Status)
	}
	// Claim proof allows.
	res2, err := rig.store.DeleteMail(ctx, 10001, entry.MailID, "op-del-2", mail.AllowDeleteWithProof(mail.ClaimProof{MailID: entry.MailID, Claimed: true, ClaimMaterialHash: entry.ClaimMaterialHash, ReceiptRef: "rcpt-1"}))
	if err != nil {
		t.Fatal(err)
	}
	if res2.Status != mail.MutationDeleted || res2.Summary.VisibleCount != 0 || res2.Summary.ClaimableCount != 0 {
		t.Fatalf("delete: %+v", res2)
	}
	// Expired attachment mail is deletable (claim window passed).
	old := rewardEntry(t, 10001, "D:2", rig.now)
	mustInsert(t, rig.store, old)
	rig.setNow(old.ClaimEndAtMs + 1)
	res3, err := rig.store.DeleteMail(ctx, 10001, old.MailID, "op-del-3", mail.AllowDelete())
	if err != nil {
		t.Fatal(err)
	}
	if res3.Status != mail.MutationDeleted {
		t.Fatalf("expired delete: %s", res3.Status)
	}
}

func TestRedisProjectionMonotonic(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	entry := rewardEntry(t, 10001, "D:1", rig.now)
	mustInsert(t, rig.store, entry)

	order := mail.ClaimProjectionOrder{
		RecipientPlayerID: 10001, WorkID: "work-1", WorkVersion: 1,
		MailID: entry.MailID, ClaimReceiptRef: "rcpt-1",
		ClaimMaterialHash: entry.ClaimMaterialHash, ClaimedAtMs: rig.now,
	}
	// Delete first, then the late projection applies but never resurrects.
	if _, err := rig.store.DeleteMail(ctx, 10001, entry.MailID, "op-del", mail.AllowDeleteWithProof(mail.ClaimProof{MailID: entry.MailID, Claimed: true, ClaimMaterialHash: entry.ClaimMaterialHash, ReceiptRef: "rcpt-1"})); err != nil {
		t.Fatal(err)
	}
	res, err := rig.store.ApplyMailClaimProjection(ctx, order)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.ProjectionApplied {
		t.Fatalf("late projection: %s", res.Status)
	}
	_, state, err := rig.store.GetEntry(ctx, 10001, entry.MailID)
	if err != nil {
		t.Fatal(err)
	}
	if state.DeletedAtMs == 0 || !state.ClaimedProjection || state.ClaimReceiptRef != "rcpt-1" {
		t.Fatalf("projection must not clear deletion: %+v", state)
	}
	page, err := rig.store.ListPage(ctx, 10001, "", rig.now)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 0 {
		t.Fatalf("deleted mail must stay hidden: %+v", page.Items)
	}
	// Replay of the same work hits the projection receipt.
	replay, err := rig.store.ApplyMailClaimProjection(ctx, order)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != mail.ProjectionApplied {
		t.Fatalf("work replay: %s", replay.Status)
	}
	// A different work on the already-claimed mail is NO_CHANGE.
	older := order
	older.WorkID = "work-0"
	res2, err := rig.store.ApplyMailClaimProjection(ctx, older)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Status != mail.ProjectionNoChange {
		t.Fatalf("second work: %s", res2.Status)
	}
	// Hash mismatch fails closed.
	bad := order
	bad.WorkID = "work-2"
	bad.ClaimMaterialHash = "0000000000000000000000000000000000000000000000000000000000000000"
	res3, err := rig.store.ApplyMailClaimProjection(ctx, bad)
	if err != nil {
		t.Fatal(err)
	}
	if res3.Status != mail.ProjectionHashMismatch {
		t.Fatalf("hash mismatch: %s", res3.Status)
	}
}

func TestRedisLazyEvictionAndFullProtected(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	l := mail.LimitsV1()

	for i := 0; i < l.MaxVisibleCount; i++ {
		entry := textEntry(t, player, fmt.Sprintf("D:%d", i), rig.now-int64(l.MaxVisibleCount-i))
		mustInsert(t, rig.store, entry)
		if i < 10 {
			if _, err := rig.store.MarkMailRead(ctx, player, entry.MailID, fmt.Sprintf("r-%d", i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	rig.setNow(rig.now + 1)
	extra := textEntry(t, player, "D:100", rig.now)
	res, err := rig.store.InsertMailIfAbsent(ctx, extra)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.InsertInserted || res.Summary.VisibleCount != 60 {
		t.Fatalf("lazy evict insert: %+v", res)
	}
	evictedID := textEntry(t, player, "D:0", rig.now-60).MailID
	_, state, err := rig.store.GetEntry(ctx, player, evictedID)
	if err != nil {
		t.Fatal(err)
	}
	if !state.AutoEvicted {
		t.Fatalf("oldest read mail must be auto-evicted: %+v", state)
	}

	// 60 unread attachment mails: fully protected.
	p2 := uint64(20002)
	for i := 0; i < l.MaxVisibleCount; i++ {
		entry := rewardEntry(t, p2, fmt.Sprintf("D:%d", i), rig.now-int64(l.MaxVisibleCount-i))
		mustInsert(t, rig.store, entry)
	}
	blocked, err := rig.store.InsertMailIfAbsent(ctx, rewardEntry(t, p2, "D:100", rig.now))
	if err != nil {
		t.Fatal(err)
	}
	if blocked.Status != mail.InsertMailboxFullProtected {
		t.Fatalf("protected mailbox must fail closed, got %s", blocked.Status)
	}
}

func TestRedisRetainedCapacityFailClosed(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	// Shrink the store limits through a dedicated store instance.
	ns := testNamespace(t)
	limits := mail.LimitsV1()
	limits.MaxRetainedCount = 3
	limits.MaxDeliveryRecords = 3
	store, err := NewStore(rig.client, Profile{Namespace: ns, BucketCount: DefaultBucketCount}, limits)
	if err != nil {
		t.Fatal(err)
	}
	build := func(seq int) mail.MailEntry {
		entry, err := mail.BuildMailEntry(mail.DeliveryParams{
			RecipientPlayerID: player, DeliveryID: fmt.Sprintf("D:%d", seq),
			SourceType: mail.SourceTypeSystem, TitleSnapshot: "t",
			CreatedAtMs: rig.now, VisibleAtMs: rig.now,
		}, limits)
		if err != nil {
			t.Fatal(err)
		}
		return entry
	}
	for i := 0; i < 3; i++ {
		res, err := store.InsertMailIfAbsent(ctx, build(i))
		if err != nil || res.Status != mail.InsertInserted {
			t.Fatalf("insert %d: %+v %v", i, res, err)
		}
	}
	res, err := store.InsertMailIfAbsent(ctx, build(3))
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.InsertCapacityExceeded {
		t.Fatalf("retained limit must fail closed, got %s", res.Status)
	}
}

func TestRedisByteCapacityFailClosed(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	limits := mail.LimitsV1()
	limits.MaxRetainedBytes = 1200
	store, err := NewStore(rig.client, Profile{Namespace: testNamespace(t), BucketCount: DefaultBucketCount}, limits)
	if err != nil {
		t.Fatal(err)
	}
	build := func(seq int) mail.MailEntry {
		entry, err := mail.BuildMailEntry(mail.DeliveryParams{
			RecipientPlayerID: player, DeliveryID: fmt.Sprintf("D:%d", seq),
			SourceType: mail.SourceTypeSystem, TitleSnapshot: "t", BodySnapshot: string(make([]byte, 512)),
			CreatedAtMs: rig.now, VisibleAtMs: rig.now,
		}, limits)
		if err != nil {
			t.Fatal(err)
		}
		return entry
	}
	first, err := store.InsertMailIfAbsent(ctx, build(1))
	if err != nil || first.Status != mail.InsertInserted {
		t.Fatalf("first: %+v %v", first, err)
	}
	second, err := store.InsertMailIfAbsent(ctx, build(2))
	if err != nil {
		t.Fatal(err)
	}
	if second.Status != mail.InsertCapacityExceeded {
		t.Fatalf("byte limit must fail closed, got %s", second.Status)
	}
}
