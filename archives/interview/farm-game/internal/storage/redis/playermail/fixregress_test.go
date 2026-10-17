package playermail

import (
	"context"
	"strings"
	"testing"

	"farmgame/internal/player/mail"
	"farmgame/internal/storage/redis/rediskey"
)

// FIX-1 regression on miniredis: while the expire worker has not
// materialized the expired flag, Delete/MarkRead/ApplyClaimProjection must
// move the materialized summary exactly once, and the later expire pass
// must be a no-change repair.

func shortLivedReward(t *testing.T, player uint64, deliveryID string, now int64) mail.MailEntry {
	t.Helper()
	entry, err := mail.BuildMailEntry(mail.DeliveryParams{
		RecipientPlayerID: player, DeliveryID: deliveryID,
		SourceType: mail.SourceTypeSystem, TitleSnapshot: "t",
		Reward:      mail.RewardBundle{Coin: 10, Items: []mail.ItemGrant{{ItemID: 1, Count: 2}}},
		CreatedAtMs: now, VisibleAtMs: now, RetentionMs: 1000,
	}, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func TestFix1RedisDeleteInExpireDelayWindow(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	entry := shortLivedReward(t, player, "D:1", rig.now)
	mustInsert(t, rig.store, entry)

	rig.setNow(entry.ExpireAtMs + 1)
	res, err := rig.store.DeleteMail(ctx, player, entry.MailID, "op-del",
		mail.AllowDeleteWithProof(mail.ClaimProof{MailID: entry.MailID, Claimed: true, ClaimMaterialHash: entry.ClaimMaterialHash, ReceiptRef: "rcpt-1"}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.MutationDeleted {
		t.Fatalf("delete: %s", res.Status)
	}
	if res.Summary.VisibleCount != 0 || res.Summary.UnreadCount != 0 || res.Summary.ClaimableCount != 0 {
		t.Fatalf("summary after delete: %+v", res.Summary)
	}
	// Old DueRef is stale; after re-registration the expire pass is
	// NO_CHANGE and the counters stay put.
	stale, err := rig.store.ExpireVisibility(ctx, mail.MailDueRefV1{
		Kind: mail.DueKindExpireVisibility, PlayerID: player, MailID: entry.MailID,
		MailVersion: 1, ExpectedStateVersion: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stale.Status != mail.ExpireStaleRef || stale.CurrentStateVersion != 2 {
		t.Fatalf("stale expire ref: %+v", stale)
	}
	freshRef := mail.MailDueRefV1{Kind: mail.DueKindExpireVisibility, PlayerID: player, MailID: entry.MailID, MailVersion: 1, ExpectedStateVersion: 2}
	if err := rig.store.RegisterDueRef(ctx, freshRef, entry.ExpireAtMs); err != nil {
		t.Fatal(err)
	}
	again, err := rig.store.ExpireVisibility(ctx, freshRef)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != mail.ExpireNoChange {
		t.Fatalf("expire after delete: %s", again.Status)
	}
	summary, err := rig.store.GetSummary(ctx, player)
	if err != nil {
		t.Fatal(err)
	}
	if summary.VisibleCount != 0 || summary.UnreadCount != 0 || summary.ClaimableCount != 0 {
		t.Fatalf("expire must not double-decrement: %+v", summary)
	}
}

func TestFix1RedisMarkReadInExpireDelayWindow(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	entry := shortLivedReward(t, player, "D:1", rig.now)
	mustInsert(t, rig.store, entry)

	rig.setNow(entry.ExpireAtMs + 1)
	res, err := rig.store.MarkMailRead(ctx, player, entry.MailID, "op-read")
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.MutationReadMarked || res.Summary.UnreadCount != 0 {
		t.Fatalf("mark read: %+v", res)
	}
	expireRes, err := rig.store.ExpireVisibility(ctx, mail.MailDueRefV1{
		Kind: mail.DueKindExpireVisibility, PlayerID: player, MailID: entry.MailID,
		MailVersion: 1, ExpectedStateVersion: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if expireRes.Status != mail.ExpireApplied {
		t.Fatalf("expire: %s", expireRes.Status)
	}
	summary, err := rig.store.GetSummary(ctx, player)
	if err != nil {
		t.Fatal(err)
	}
	if summary.VisibleCount != 0 || summary.UnreadCount != 0 || summary.ClaimableCount != 0 {
		t.Fatalf("counters must drain exactly once: %+v", summary)
	}
}

func TestFix1RedisProjectionInExpireDelayWindow(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	entry := shortLivedReward(t, player, "D:1", rig.now)
	mustInsert(t, rig.store, entry)

	rig.setNow(entry.ExpireAtMs + 1)
	res, err := rig.store.ApplyMailClaimProjection(ctx, mail.ClaimProjectionOrder{
		RecipientPlayerID: player, WorkID: "w-1", WorkVersion: 1,
		MailID: entry.MailID, ClaimReceiptRef: "rcpt-1",
		ClaimMaterialHash: entry.ClaimMaterialHash, ClaimedAtMs: rig.now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.ProjectionApplied || res.Summary.ClaimableCount != 0 {
		t.Fatalf("projection: %+v", res)
	}
	expireRes, err := rig.store.ExpireVisibility(ctx, mail.MailDueRefV1{
		Kind: mail.DueKindExpireVisibility, PlayerID: player, MailID: entry.MailID,
		MailVersion: 1, ExpectedStateVersion: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if expireRes.Status != mail.ExpireApplied {
		t.Fatalf("expire: %s", expireRes.Status)
	}
	summary, err := rig.store.GetSummary(ctx, player)
	if err != nil {
		t.Fatal(err)
	}
	if summary.ClaimableCount != 0 || summary.VisibleCount != 0 {
		t.Fatalf("claimable must decrement exactly once: %+v", summary)
	}
}

// FIX-2 regression: the proof must bind the exact mail; Go validates the
// structure and the Lua script re-verifies against the immutable entry.
func TestFix2RedisClaimProofBinding(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	mailA := rewardEntry(t, player, "D:A", rig.now)
	mailB := rewardEntry(t, player, "D:B", rig.now)
	mustInsert(t, rig.store, mailA)
	mustInsert(t, rig.store, mailB)

	// A's proof must not delete B (rejected before the script runs).
	if _, err := rig.store.DeleteMail(ctx, player, mailB.MailID, "op-x1",
		mail.AllowDeleteWithProof(mail.ClaimProof{MailID: mailA.MailID, Claimed: true, ClaimMaterialHash: mailA.ClaimMaterialHash, ReceiptRef: "rcpt-a"})); err == nil {
		t.Fatal("proof of A must not authorize deleting B")
	}
	if _, state, err := rig.store.GetEntry(ctx, player, mailB.MailID); err != nil || state.DeletedAtMs != 0 {
		t.Fatalf("B must survive: %+v %v", state, err)
	}
	// A well-formed proof whose hash does not match the immutable entry is
	// rejected by the script's own defense (CLAIM_PROOF_INVALID).
	if _, err := rig.store.DeleteMail(ctx, player, mailB.MailID, "op-x2",
		mail.AllowDeleteWithProof(mail.ClaimProof{MailID: mailB.MailID, Claimed: true, ClaimMaterialHash: mailA.ClaimMaterialHash, ReceiptRef: "rcpt-b"})); err == nil {
		t.Fatal("hash mismatch must be rejected")
	}
	// Empty receipt ref is rejected structurally.
	if err := mail.AllowDeleteWithProof(mail.ClaimProof{MailID: mailB.MailID, Claimed: true, ClaimMaterialHash: mailB.ClaimMaterialHash}).Validate(mailB.MailID); err == nil {
		t.Fatal("empty receipt ref must be rejected")
	}
	// The correct proof deletes B.
	res, err := rig.store.DeleteMail(ctx, player, mailB.MailID, "op-ok",
		mail.AllowDeleteWithProof(mail.ClaimProof{MailID: mailB.MailID, Claimed: true, ClaimMaterialHash: mailB.ClaimMaterialHash, ReceiptRef: "rcpt-b"}))
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != mail.MutationDeleted {
		t.Fatalf("correct proof: %s", res.Status)
	}
}

// TestFix2LuaDefenseDirectly invokes DeleteMailV2 with a forged proof that
// bypasses Go validation (empty receipt ref) and proves the script itself
// refuses to delete.
func TestFix2LuaDefenseDirectly(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	entry := rewardEntry(t, player, "D:1", rig.now)
	mustInsert(t, rig.store, entry)

	bucket, err := rig.store.Keys().BucketOf(player)
	if err != nil {
		t.Fatal(err)
	}
	tag, err := rig.store.Keys().HashTag(bucket)
	if err != nil {
		t.Fatal(err)
	}
	box, _ := rig.store.Keys().Mailbox(bucket, player)
	index, _ := rig.store.Keys().VisibleIndex(bucket, player)
	safe, _ := rig.store.Keys().SafeEvictIndex(bucket, player)
	digest, _ := mail.MutationDigest(player, entry.MailID, mail.MutationKindDelete, "forged-1")
	mutationField, _ := rig.store.fields.MutationField(digest)
	entryField, _ := rig.store.fields.EntryField(entry.MailID)
	stateField, _ := rig.store.fields.StateField(entry.MailID)
	member, _ := mail.EncodeSortMember(entry.CreatedAtMs, entry.MailID)

	out, err := rig.store.runScript(ctx, ScriptDeleteMailV2,
		[]rediskey.RedisKey{box, index, safe},
		tag, mutationField, stateField, entryField, entry.MailID, string(member),
		"ALLOW_WITH_CLAIM_PROOF", entry.MailID, entry.ClaimMaterialHash, "",
		"forged-1", mail.LimitsV1().MutationReceiptRetainMs)
	if err != nil {
		t.Fatalf("script call itself must succeed: %v", err)
	}
	if len(out) == 0 || out[0] != "CLAIM_PROOF_INVALID" {
		t.Fatalf("forged proof must be refused by the script, got %v", out)
	}
	if _, state, gerr := rig.store.GetEntry(ctx, player, entry.MailID); gerr != nil || state.DeletedAtMs != 0 {
		t.Fatalf("mail must survive forged proof: %+v %v", state, gerr)
	}
}

// FIX-3 regression: a missing dedup record fails closed into reconcile;
// the anti-replay evidence is never dropped silently.
func TestFix3RedisGcMissingDedupFailsClosed(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	entry := textEntry(t, player, "D:1", rig.now)
	mustInsert(t, rig.store, entry)

	// Expire and reach the effective purge point.
	rig.setNow(entry.ExpireAtMs)
	if _, err := rig.store.ExpireVisibility(ctx, mail.MailDueRefV1{
		Kind: mail.DueKindExpireVisibility, PlayerID: player, MailID: entry.MailID,
		MailVersion: 1, ExpectedStateVersion: 1,
	}); err != nil {
		t.Fatal(err)
	}
	rig.setNow(testNowMs + mail.LimitsV1().DeliveryRecordRetainMs)
	// Destroy the dedup record out of band (simulates corruption).
	bucket, err := rig.store.Keys().BucketOf(player)
	if err != nil {
		t.Fatal(err)
	}
	box, _ := rig.store.Keys().Mailbox(bucket, player)
	digest, _ := mail.DeliveryDigest(player, entry.DeliveryID)
	deliveryField, _ := rig.store.fields.DeliveryField(digest)
	if err := rig.client.HDel(ctx, box.String(), deliveryField).Err(); err != nil {
		t.Fatal(err)
	}
	_, err = rig.store.GcMail(ctx, mail.MailDueRefV1{
		Kind: mail.DueKindPhysicalGC, PlayerID: player, MailID: entry.MailID,
		MailVersion: 1, ExpectedStateVersion: 2,
	})
	if err == nil {
		t.Fatal("missing dedup must fail closed, not silently purge")
	}
	// Entry must survive: GC refused to run.
	if _, _, gerr := rig.store.GetEntry(ctx, player, entry.MailID); gerr != nil {
		t.Fatalf("entry must survive reconcile-required: %v", gerr)
	}
}

// FIX-4 regression: with max-size entries and a tight byte budget, every
// page is short but bounded, and the traversal stays complete and
// duplicate-free.
func TestFix4RedisByteBudgetPaging(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	limits := mail.LimitsV1()
	// Budget for roughly two worst-case candidates per page.
	limits.PageScanBytes = 2 * (limits.MaxMailEntryBytes + limits.MaxMailStateBytes + maxSortMemberBytes)
	store, err := NewStore(rig.client, Profile{Namespace: testNamespace(t), BucketCount: DefaultBucketCount}, limits)
	if err != nil {
		t.Fatal(err)
	}
	const total = 9
	ids := make([]string, 0, total)
	for i := 0; i < total; i++ {
		entry, err := mail.BuildMailEntry(mail.DeliveryParams{
			RecipientPlayerID: player, DeliveryID: strings.Repeat("D", 100) + string(rune('0'+i)),
			SourceType:    mail.SourceTypeSystem,
			TitleSnapshot: strings.Repeat("T", 256),
			BodySnapshot:  strings.Repeat("B", 4096),
			CreatedAtMs:   rig.now - int64(total-i), VisibleAtMs: rig.now - int64(total-i),
		}, limits)
		if err != nil {
			t.Fatal(err)
		}
		res, err := store.InsertMailIfAbsent(ctx, entry)
		if err != nil || res.Status != mail.InsertInserted {
			t.Fatalf("insert %d: %+v %v", i, res, err)
		}
		ids = append(ids, entry.MailID)
	}
	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		page, err := store.ListPage(ctx, player, cursor, rig.now)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		if len(page.Items) > limits.PageSize {
			t.Fatalf("page exceeds 6: %d", len(page.Items))
		}
		for _, item := range page.Items {
			if seen[item.MailID] {
				t.Fatalf("duplicate: %s", item.MailID)
			}
			seen[item.MailID] = true
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
		if pages > 20 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != total {
		t.Fatalf("traversal saw %d of %d", len(seen), total)
	}
	if pages < 2 {
		t.Fatalf("tight byte budget must force multiple short pages, got %d", pages)
	}
}
