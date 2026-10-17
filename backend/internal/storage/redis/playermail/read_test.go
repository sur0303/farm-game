package playermail

import (
	"context"
	"fmt"
	"testing"

	"farmgame/internal/player/mail"
)

func insertPageEntries(t *testing.T, rig *testRig, player uint64, n int, startAt int64) []mail.MailEntry {
	t.Helper()
	out := make([]mail.MailEntry, 0, n)
	for i := 0; i < n; i++ {
		entry := textEntry(t, player, fmt.Sprintf("P:%d", i), startAt+int64(i))
		mustInsert(t, rig.store, entry)
		out = append(out, entry)
	}
	return out
}

func TestRedisListPageExclusiveCursor(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	entries := insertPageEntries(t, rig, player, 14, rig.now-100)

	p1, err := rig.store.ListPage(ctx, player, "", rig.now)
	if err != nil {
		t.Fatal(err)
	}
	if len(p1.Items) != 6 || p1.NextCursor == "" {
		t.Fatalf("page1: %d items cursor=%q", len(p1.Items), p1.NextCursor)
	}
	for i := 0; i < 6; i++ {
		if p1.Items[i].MailID != entries[13-i].MailID {
			t.Fatalf("page1[%d]: got %s want %s", i, p1.Items[i].MailID, entries[13-i].MailID)
		}
	}
	p2, err := rig.store.ListPage(ctx, player, p1.NextCursor, rig.now)
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.Items) != 6 || p2.NextCursor == "" {
		t.Fatalf("page2: %d items cursor=%q", len(p2.Items), p2.NextCursor)
	}
	for i := 0; i < 6; i++ {
		if p2.Items[i].MailID != entries[7-i].MailID {
			t.Fatalf("page2[%d]: got %s want %s", i, p2.Items[i].MailID, entries[7-i].MailID)
		}
	}
	p3, err := rig.store.ListPage(ctx, player, p2.NextCursor, rig.now)
	if err != nil {
		t.Fatal(err)
	}
	if len(p3.Items) != 2 {
		t.Fatalf("page3: %d items", len(p3.Items))
	}
	// A trailing probe cursor with no remaining items terminates cleanly.
	if p3.NextCursor != "" {
		p4, err := rig.store.ListPage(ctx, player, p3.NextCursor, rig.now)
		if err != nil {
			t.Fatal(err)
		}
		if len(p4.Items) != 0 || p4.NextCursor != "" {
			t.Fatalf("page4 must be empty: %+v", p4)
		}
	}
}

func TestRedisListPageSameMillisecond(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	same := rig.now - 50
	entries := make([]mail.MailEntry, 0, 8)
	for i := 0; i < 8; i++ {
		entry := textEntry(t, player, fmt.Sprintf("S:%d", i), same)
		mustInsert(t, rig.store, entry)
		entries = append(entries, entry)
	}
	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		page, err := rig.store.ListPage(ctx, player, cursor, rig.now)
		if err != nil {
			t.Fatal(err)
		}
		pages++
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
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	if len(seen) != 8 {
		t.Fatalf("same-ms pagination saw %d of 8", len(seen))
	}
	first, err := rig.store.ListPage(ctx, player, "", rig.now)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(first.Items)-1; i++ {
		if first.Items[i].MailID > first.Items[i+1].MailID {
			t.Fatalf("same-ms tie-break must follow ascending mail id: %s vs %s",
				first.Items[i].MailID, first.Items[i+1].MailID)
		}
	}
}

func TestRedisListPageInsertDuringPagination(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	insertPageEntries(t, rig, player, 10, rig.now-100)

	p1, err := rig.store.ListPage(ctx, player, "", rig.now)
	if err != nil {
		t.Fatal(err)
	}
	fresh := textEntry(t, player, "P:100", rig.now)
	mustInsert(t, rig.store, fresh)
	p2, err := rig.store.ListPage(ctx, player, p1.NextCursor, rig.now)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range p2.Items {
		if item.MailID == fresh.MailID {
			t.Fatal("new mail must not join an old cursor's later pages")
		}
	}
	p1r, err := rig.store.ListPage(ctx, player, "", rig.now)
	if err != nil {
		t.Fatal(err)
	}
	if p1r.Items[0].MailID != fresh.MailID {
		t.Fatalf("refresh: got %s want %s", p1r.Items[0].MailID, fresh.MailID)
	}
}

func TestRedisListPageDeleteDuringPagination(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	entries := insertPageEntries(t, rig, player, 12, rig.now-100)

	p1, err := rig.store.ListPage(ctx, player, "", rig.now)
	if err != nil {
		t.Fatal(err)
	}
	victims := map[string]bool{entries[6].MailID: true, entries[7].MailID: true}
	for id := range victims {
		if _, err := rig.store.DeleteMail(ctx, player, id, "del-"+id, mail.AllowDelete()); err != nil {
			t.Fatal(err)
		}
	}
	p2, err := rig.store.ListPage(ctx, player, p1.NextCursor, rig.now)
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.Items) != 6 {
		t.Fatalf("bounded overscan should backfill to 6, got %d", len(p2.Items))
	}
	for _, item := range p2.Items {
		if victims[item.MailID] {
			t.Fatalf("deleted mail leaked: %s", item.MailID)
		}
	}
}

func TestRedisListPageBoundedShortPage(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	limits := mail.LimitsV1()
	limits.PageScanItems = 8
	store, err := NewStore(rig.client, Profile{Namespace: testNamespace(t), BucketCount: DefaultBucketCount}, limits)
	if err != nil {
		t.Fatal(err)
	}
	// Four long-lived older mails, then eight short-lived newer mails.
	for i := 0; i < 4; i++ {
		entry := textEntry(t, player, fmt.Sprintf("L:%d", i), rig.now-1000+int64(i))
		res, err := store.InsertMailIfAbsent(ctx, entry)
		if err != nil || res.Status != mail.InsertInserted {
			t.Fatalf("long insert: %+v %v", res, err)
		}
	}
	for i := 0; i < 8; i++ {
		entry, err := mail.BuildMailEntry(mail.DeliveryParams{
			RecipientPlayerID: player, DeliveryID: fmt.Sprintf("H:%d", i),
			SourceType: mail.SourceTypeSystem, TitleSnapshot: "t",
			CreatedAtMs: rig.now - 100 + int64(i), VisibleAtMs: rig.now - 100 + int64(i),
			RetentionMs: 10,
		}, limits)
		if err != nil {
			t.Fatal(err)
		}
		res, err := store.InsertMailIfAbsent(ctx, entry)
		if err != nil || res.Status != mail.InsertInserted {
			t.Fatalf("short insert: %+v %v", res, err)
		}
	}
	rig.setNow(rig.now + 1000)
	page, err := store.ListPage(ctx, player, "", rig.now)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) >= limits.PageSize {
		t.Fatalf("scan cap must allow a short page, got %d", len(page.Items))
	}
	if page.NextCursor == "" {
		t.Fatal("short page with remaining candidates must offer a cursor")
	}
	page2, err := store.ListPage(ctx, player, page.NextCursor, rig.now)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Items) != 4 {
		t.Fatalf("remaining visible mail must appear on the next page, got %d", len(page2.Items))
	}
}

func TestRedisListPageExpiredFilteredByTime(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	entry := textEntry(t, player, "D:1", rig.now)
	mustInsert(t, rig.store, entry)
	rig.setNow(entry.ExpireAtMs + 1)
	page, err := rig.store.ListPage(ctx, player, "", rig.now)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 0 {
		t.Fatalf("expired mail must be time-filtered: %+v", page.Items)
	}
}

func TestRedisDetailAndSummaryReads(t *testing.T) {
	rig := newTestRig(t)
	ctx := context.Background()
	player := uint64(10001)
	entry := rewardEntry(t, player, "D:1", rig.now)
	mustInsert(t, rig.store, entry)

	gotEntry, gotState, err := rig.store.GetEntry(ctx, player, entry.MailID)
	if err != nil {
		t.Fatal(err)
	}
	if gotEntry.BodySnapshot != "body" || gotState.ReadAtMs != 0 || !gotEntry.HasAttachment() {
		t.Fatalf("detail: %+v %+v", gotEntry, gotState)
	}
	if _, _, err := rig.store.GetEntry(ctx, 20002, entry.MailID); err != mail.ErrMailNotFound {
		t.Fatalf("cross-recipient read must fail closed, got %v", err)
	}
	summary, err := rig.store.GetSummary(ctx, player)
	if err != nil {
		t.Fatal(err)
	}
	if summary.VisibleCount != 1 || summary.UnreadCount != 1 || summary.ClaimableCount != 1 || summary.DeliveryRecords != 1 {
		t.Fatalf("summary: %+v", summary)
	}
}
