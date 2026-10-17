package playermail

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"farmgame/internal/player/mail"
	"github.com/redis/go-redis/v9"
)

func TestRedisReceiptCapacityFailsClosedBeforeMutation(t *testing.T) {
	trig := newTestRig(t)
	ctx := context.Background()
	limits := mail.LimitsV1()
	limits.MaxReceiptItems = 1
	store, err := NewStore(trig.client, Profile{Namespace: testNamespace(t), BucketCount: DefaultBucketCount}, limits)
	if err != nil {
		t.Fatal(err)
	}
	entry := textEntry(t, 10001, "receipt-cap-item", trig.now)
	mustInsert(t, store, entry)

	first, err := store.MarkMailRead(ctx, entry.RecipientPlayerID, entry.MailID, "read-1")
	if err != nil || first.Status != mail.MutationReadMarked {
		t.Fatalf("first receipt: %+v %v", first, err)
	}
	second, err := store.MarkMailRead(ctx, entry.RecipientPlayerID, entry.MailID, "read-2")
	if !errors.Is(err, mail.ErrReceiptCapacityExceeded) {
		t.Fatalf("second receipt error=%v, want receipt capacity", err)
	}
	if second != (mail.MutationResult{}) {
		t.Fatalf("capacity failure must not return a mutation result: %+v", second)
	}

	bucket, err := store.Keys().BucketOf(entry.RecipientPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	box, err := store.Keys().Mailbox(bucket, entry.RecipientPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := mail.MutationDigest(entry.RecipientPlayerID, entry.MailID, mail.MutationKindMarkRead, "read-2")
	if err != nil {
		t.Fatal(err)
	}
	mutationField, err := store.fields.MutationField(digest)
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := trig.client.HExists(ctx, box.String(), mutationField).Result(); err != nil || exists {
		t.Fatalf("rejected receipt was written: exists=%v err=%v", exists, err)
	}
	meta, err := trig.client.HMGet(ctx, box.String(), MetaReceiptItems, MetaReceiptBytes).Result()
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(meta[0]); got != "1" || meta[1] == nil || fmt.Sprint(meta[1]) == "0" {
		t.Fatalf("receipt metadata after fail-closed write: %v", meta)
	}
}

func TestRedisReceiptByteCapacityFailsClosed(t *testing.T) {
	trig := newTestRig(t)
	ctx := context.Background()
	limits := mail.LimitsV1()
	entry := textEntry(t, 10001, "receipt-cap-byte", trig.now)

	// The first MarkRead receipt is deterministic under miniredis TIME. Set the
	// aggregate byte cap exactly to its encoded size, then require the next
	// distinct mutation to fail before changing state or metadata.
	firstReceipt, err := mail.EncodeMutationReceipt(mail.MutationReceipt{
		Kind: mail.MutationKindMarkRead, MailID: entry.MailID, MutationID: "read-1",
		ResultStatus: "READ_MARKED", AppliedAtMs: trig.now, MailboxRevision: 2,
		RetainUntilMs: trig.now + limits.MutationReceiptRetainMs,
	}, limits)
	if err != nil {
		t.Fatal(err)
	}
	limits.MaxReceiptTotalBytes = len(firstReceipt)
	store, err := NewStore(trig.client, Profile{Namespace: testNamespace(t), BucketCount: DefaultBucketCount}, limits)
	if err != nil {
		t.Fatal(err)
	}
	mustInsert(t, store, entry)
	if _, err := store.MarkMailRead(ctx, entry.RecipientPlayerID, entry.MailID, "read-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkMailRead(ctx, entry.RecipientPlayerID, entry.MailID, "read-2"); !errors.Is(err, mail.ErrReceiptCapacityExceeded) {
		t.Fatalf("byte overflow error=%v, want receipt capacity", err)
	}
}

func TestRedisProjectionReceiptCapacityFailsClosed(t *testing.T) {
	trig := newTestRig(t)
	ctx := context.Background()
	limits := mail.LimitsV1()
	limits.MaxReceiptItems = 1
	store, err := NewStore(trig.client, Profile{Namespace: testNamespace(t), BucketCount: DefaultBucketCount}, limits)
	if err != nil {
		t.Fatal(err)
	}
	entry := rewardEntry(t, 10001, "projection-capacity", trig.now)
	mustInsert(t, store, entry)
	order := mail.ClaimProjectionOrder{
		RecipientPlayerID: 10001,
		WorkID:            "projection-work-1",
		WorkVersion:       1,
		MailID:            entry.MailID,
		ClaimReceiptRef:   "claim-receipt-1",
		ClaimMaterialHash: entry.ClaimMaterialHash,
		ClaimedAtMs:       trig.now,
	}
	first, err := store.ApplyMailClaimProjection(ctx, order)
	if err != nil || first.Status != mail.ProjectionApplied {
		t.Fatalf("first projection receipt: %+v %v", first, err)
	}
	secondOrder := order
	secondOrder.WorkID = "projection-work-2"
	second, err := store.ApplyMailClaimProjection(ctx, secondOrder)
	if !errors.Is(err, mail.ErrReceiptCapacityExceeded) {
		t.Fatalf("second projection error=%v, want receipt capacity", err)
	}
	if second != (mail.ProjectionResult{}) {
		t.Fatalf("capacity failure must not return a projection result: %+v", second)
	}
}

func TestRedisGcMailScopesReceiptCleanupAndKeepsActiveEvidence(t *testing.T) {
	trig := newTestRig(t)
	ctx := context.Background()
	limits := mail.LimitsV1()
	limits.ClaimMaterialRetainAfterClaimEndMs = 100
	limits.DeliveryRecordRetainMs = 100
	limits.MutationReceiptRetainMs = 1000
	limits.ProjectionReceiptRetainMs = 1000
	store, err := NewStore(trig.client, Profile{Namespace: testNamespace(t), BucketCount: DefaultBucketCount}, limits)
	if err != nil {
		t.Fatal(err)
	}
	player := uint64(10001)
	a := shortLivedTextWithLimits(t, player, "gc-receipt-a", trig.now, limits)
	b := shortLivedTextWithLimits(t, player, "gc-receipt-b", trig.now, limits)
	mustInsert(t, store, a)
	mustInsert(t, store, b)
	if _, err := store.MarkMailRead(ctx, player, a.MailID, "read-a"); err != nil {
		t.Fatal(err)
	}
	// Make B's receipt active later than A's, so GcReceipt can prove that an
	// active receipt for another mail is not removed by A's cleanup.
	trig.setNow(trig.now + 500)
	if _, err := store.MarkMailRead(ctx, player, b.MailID, "read-b"); err != nil {
		t.Fatal(err)
	}
	trig.setNow(trig.now + 1)
	for _, entry := range []mail.MailEntry{a, b} {
		res, err := store.ExpireVisibility(ctx, mail.MailDueRefV1{
			Kind: mail.DueKindExpireVisibility, PlayerID: player, MailID: entry.MailID,
			MailVersion: 1, ExpectedStateVersion: 2,
		})
		if err != nil || res.Status != mail.ExpireApplied {
			t.Fatalf("expire %s: %+v %v", entry.MailID, res, err)
		}
	}

	// Both receipts are active at this point. Physical GC of A may remove the
	// mail body, but it must retain A's active receipt and all of B's evidence.
	gcA, err := store.GcMail(ctx, mail.MailDueRefV1{
		Kind: mail.DueKindPhysicalGC, PlayerID: player, MailID: a.MailID,
		MailVersion: 1, ExpectedStateVersion: 3,
	})
	if err != nil || gcA.Status != mail.GcPurged {
		t.Fatalf("gc A: %+v %v", gcA, err)
	}
	aField := mutationFieldFor(t, store, player, a.MailID, "read-a")
	bField := mutationFieldFor(t, store, player, b.MailID, "read-b")
	bucket, err := store.Keys().BucketOf(player)
	if err != nil {
		t.Fatal(err)
	}
	box, err := store.Keys().Mailbox(bucket, player)
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := trig.client.HExists(ctx, box.String(), aField).Result(); err != nil || !exists {
		t.Fatalf("active A receipt was removed by mail GC: exists=%v err=%v", exists, err)
	}
	if exists, err := trig.client.HExists(ctx, box.String(), bField).Result(); err != nil || !exists {
		t.Fatalf("B receipt/mail evidence was removed by A GC: exists=%v err=%v", exists, err)
	}
	if _, _, err := store.GetEntry(ctx, player, b.MailID); err != nil {
		t.Fatalf("B mail must survive A GC: %v", err)
	}

	// A's receipt expires first; GcReceipt removes only A and leaves B active.
	trig.setNow(testNowMs + 1001)
	aRef := ReceiptDueRef{Kind: ReceiptDueMutation, PlayerID: player, MailID: a.MailID, ReceiptField: aField}
	aReceipt, err := store.GcReceipt(ctx, aRef)
	if err != nil || aReceipt.Status != ReceiptGcPurged {
		t.Fatalf("receipt GC A: %+v %v", aReceipt, err)
	}
	if exists, err := trig.client.HExists(ctx, box.String(), aField).Result(); err != nil || exists {
		t.Fatalf("A receipt must be purged: exists=%v err=%v", exists, err)
	}
	if exists, err := trig.client.HExists(ctx, box.String(), bField).Result(); err != nil || !exists {
		t.Fatalf("active B receipt must survive: exists=%v err=%v", exists, err)
	}
}

func TestRedisReceiptDueReclaimsReceiptWithoutMail(t *testing.T) {
	trig := newTestRig(t)
	ctx := context.Background()
	limits := mail.LimitsV1()
	limits.MutationReceiptRetainMs = 100
	store, err := NewStore(trig.client, Profile{Namespace: testNamespace(t), BucketCount: DefaultBucketCount}, limits)
	if err != nil {
		t.Fatal(err)
	}
	entry := textEntry(t, 10001, "receipt-orphan", trig.now)
	// No Entry/State exists, but the stable mutation receipt is still a
	// bounded replay record and must have an expiry discovery path.
	if res, err := store.MarkMailRead(ctx, entry.RecipientPlayerID, entry.MailID, "missing-read"); err != nil || res.Status != mail.MutationMailNotFound {
		t.Fatalf("missing-mail receipt: %+v %v", res, err)
	}
	bucket, err := store.Keys().BucketOf(entry.RecipientPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	trig.setNow(trig.now + 101)
	due, err := store.ListReceiptDue(ctx, bucket, trig.now, 10)
	if err != nil || len(due) != 1 {
		t.Fatalf("receipt due: %+v %v", due, err)
	}
	result, err := store.GcReceipt(ctx, due[0].Ref)
	if err != nil || result.Status != ReceiptGcPurged {
		t.Fatalf("orphan receipt GC: %+v %v", result, err)
	}
	box, err := store.Keys().Mailbox(bucket, entry.RecipientPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := trig.client.HMGet(ctx, box.String(), MetaReceiptItems, MetaReceiptBytes).Result()
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(meta[0]) != "0" || fmt.Sprint(meta[1]) != "0" {
		t.Fatalf("receipt metadata after orphan GC: %v", meta)
	}
}

func TestRedisReceiptNotDueAtomicallyAdvancesScore(t *testing.T) {
	trig := newTestRig(t)
	ctx := context.Background()
	limits := mail.LimitsV1()
	limits.MutationReceiptRetainMs = 1000
	store, err := NewStore(trig.client, Profile{Namespace: testNamespace(t), BucketCount: DefaultBucketCount}, limits)
	if err != nil {
		t.Fatal(err)
	}
	entry := textEntry(t, 10001, "receipt-not-due", trig.now)
	mustInsert(t, store, entry)
	if result, markErr := store.MarkMailRead(ctx, entry.RecipientPlayerID, entry.MailID, "early-index"); markErr != nil || result.Status != mail.MutationReadMarked {
		t.Fatalf("mark read: %+v %v", result, markErr)
	}
	field := mutationFieldFor(t, store, entry.RecipientPlayerID, entry.MailID, "early-index")
	ref := ReceiptDueRef{Kind: ReceiptDueMutation, PlayerID: entry.RecipientPlayerID, MailID: entry.MailID, ReceiptField: field}
	member, err := EncodeReceiptDueRefV1(ref)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := store.Keys().BucketOf(entry.RecipientPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	dueKey, err := store.Keys().ReceiptDueIndex(bucket)
	if err != nil {
		t.Fatal(err)
	}
	if err := trig.client.ZAdd(ctx, dueKey.String(), redis.Z{Score: float64(trig.now - 1), Member: member}).Err(); err != nil {
		t.Fatal(err)
	}
	due, err := store.ListReceiptDue(ctx, bucket, trig.now, 10)
	if err != nil || len(due) != 1 {
		t.Fatalf("forced early due: %+v %v", due, err)
	}
	result, err := store.GcReceipt(ctx, due[0].Ref)
	if err != nil || result.Status != ReceiptGcNotDue || result.RetainUntil != trig.now+limits.MutationReceiptRetainMs {
		t.Fatalf("receipt NOT_DUE: %+v %v", result, err)
	}
	score, err := trig.client.ZScore(ctx, dueKey.String(), member).Result()
	if err != nil || int64(score) != result.RetainUntil {
		t.Fatalf("advanced receipt score=%v err=%v want=%d", score, err, result.RetainUntil)
	}
	again, err := store.ListReceiptDue(ctx, bucket, trig.now, 10)
	if err != nil || len(again) != 0 {
		t.Fatalf("receipt remained hot-loop due: %+v %v", again, err)
	}
}

func TestReceiptDueSharesMailboxHashSlot(t *testing.T) {
	kb := testKeyBuilder(t)
	bucket, err := kb.BucketOf(10001)
	if err != nil {
		t.Fatal(err)
	}
	box, err := kb.Mailbox(bucket, 10001)
	if err != nil {
		t.Fatal(err)
	}
	due, err := kb.ReceiptDueIndex(bucket)
	if err != nil {
		t.Fatal(err)
	}
	if err := kb.AssertSameSlot(box, due); err != nil {
		t.Fatalf("receipt due key must share mailbox slot: %v", err)
	}
}

func shortLivedTextWithLimits(t *testing.T, player uint64, deliveryID string, now int64, limits mail.Limits) mail.MailEntry {
	t.Helper()
	entry, err := mail.BuildMailEntry(mail.DeliveryParams{
		RecipientPlayerID: player, DeliveryID: deliveryID, SourceType: mail.SourceTypeSystem,
		TitleSnapshot: "title", BodySnapshot: "body", CreatedAtMs: now, VisibleAtMs: now,
		RetentionMs: 100,
	}, limits)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func mutationFieldFor(t *testing.T, store *Store, player uint64, mailID, mutationID string) string {
	t.Helper()
	digest, err := mail.MutationDigest(player, mailID, mail.MutationKindMarkRead, mutationID)
	if err != nil {
		t.Fatal(err)
	}
	field, err := store.fields.MutationField(digest)
	if err != nil {
		t.Fatal(err)
	}
	return field
}
