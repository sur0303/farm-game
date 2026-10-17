package mail

import (
	"context"
	"fmt"
	"testing"
)

func insertN(t *testing.T, store *MemoryStore, player uint64, startSeq, n int, createdAtMs int64) []MailEntry {
	t.Helper()
	out := make([]MailEntry, 0, n)
	for i := 0; i < n; i++ {
		entry := mustBuild(t, textDelivery(player, startSeq+i, createdAtMs+int64(i)))
		mustInsert(t, store, entry)
		out = append(out, entry)
	}
	return out
}

func TestListPageExclusiveCursor(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	player := uint64(10001)
	entries := insertN(t, store, player, 0, 14, now-100)

	// Page 1: newest 6, newest first.
	p1, err := store.ListPage(ctx, player, "", now)
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
	// Page 2 is exclusive: the page-1 tail never repeats.
	p2, err := store.ListPage(ctx, player, p1.NextCursor, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.Items) != 6 || p2.NextCursor == "" {
		t.Fatalf("page2: %d items", len(p2.Items))
	}
	for i := 0; i < 6; i++ {
		if p2.Items[i].MailID != entries[7-i].MailID {
			t.Fatalf("page2[%d]: got %s want %s", i, p2.Items[i].MailID, entries[7-i].MailID)
		}
	}
	p3, err := store.ListPage(ctx, player, p2.NextCursor, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(p3.Items) != 2 || p3.NextCursor != "" {
		t.Fatalf("page3: %d items cursor=%q", len(p3.Items), p3.NextCursor)
	}
}

func TestListPageSameMillisecond(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	player := uint64(10001)
	// Eight mails sharing one millisecond: mail id breaks the tie.
	same := now - 50
	entries := make([]MailEntry, 0, 8)
	for i := 0; i < 8; i++ {
		entry := mustBuild(t, textDelivery(player, i, same))
		mustInsert(t, store, entry)
		entries = append(entries, entry)
	}
	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		page, err := store.ListPage(ctx, player, cursor, now)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, item := range page.Items {
			if seen[item.MailID] {
				t.Fatalf("duplicate across pages: %s", item.MailID)
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
	// Within the same millisecond, ascending mail id order is the frozen
	// physical tie-break (35A §3: lex-ascending members, time prefix equal).
	first, err := store.ListPage(ctx, player, "", now)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(first.Items)-1; i++ {
		if first.Items[i].MailID > first.Items[i+1].MailID {
			t.Fatalf("same-ms order: %s should be < %s", first.Items[i].MailID, first.Items[i+1].MailID)
		}
	}
}

func TestListPageInsertDuringPagination(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	player := uint64(10001)
	entries := insertN(t, store, player, 0, 10, now-100)

	p1, err := store.ListPage(ctx, player, "", now)
	if err != nil {
		t.Fatal(err)
	}
	// A new mail inserted after page 1 must not appear in the old cursor's
	// later pages (35 Q35-05).
	fresh := mustBuild(t, textDelivery(player, 100, now))
	mustInsert(t, store, fresh)
	p2, err := store.ListPage(ctx, player, p1.NextCursor, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range p2.Items {
		if item.MailID == fresh.MailID {
			t.Fatal("new mail must not join an old cursor's later pages")
		}
	}
	// Refreshing page 1 shows it.
	p1r, err := store.ListPage(ctx, player, "", now)
	if err != nil {
		t.Fatal(err)
	}
	if p1r.Items[0].MailID != fresh.MailID {
		t.Fatalf("refresh: got %s want %s", p1r.Items[0].MailID, fresh.MailID)
	}
	_ = entries
}

func TestListPageDeleteDuringPagination(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	player := uint64(10001)
	entries := insertN(t, store, player, 0, 12, now-100)

	p1, err := store.ListPage(ctx, player, "", now)
	if err != nil {
		t.Fatal(err)
	}
	// Delete two mails that belong to page 2's range; the bounded overscan
	// backfills toward six.
	victim1 := entries[6].MailID // oldest of page 2
	victim2 := entries[7].MailID
	if _, err := store.DeleteMail(ctx, player, victim1, "d-1", AllowDelete()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeleteMail(ctx, player, victim2, "d-2", AllowDelete()); err != nil {
		t.Fatal(err)
	}
	p2, err := store.ListPage(ctx, player, p1.NextCursor, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.Items) != 6 {
		t.Fatalf("bounded overscan should backfill to 6, got %d", len(p2.Items))
	}
	for _, item := range p2.Items {
		if item.MailID == victim1 || item.MailID == victim2 {
			t.Fatalf("deleted mail leaked: %s", item.MailID)
		}
	}
}

func TestListPageBoundedShortPage(t *testing.T) {
	now := testNow
	limits := LimitsV1()
	limits.PageScanItems = 8
	store, err := NewMemoryStore(func() int64 { return now }, limits)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	player := uint64(10001)
	// Four long-lived older mails, then eight short-lived newer mails. The
	// expire worker has not run, so the eight expired entries still occupy
	// the head of the index; reads filter them by trusted time.
	insertN(t, store, player, 0, 4, now-1000)
	shortParams := func(seq int, at int64) DeliveryParams {
		p := textDelivery(player, seq, at)
		p.RetentionMs = 10
		return p
	}
	for i := 0; i < 8; i++ {
		entry, err := BuildMailEntry(shortParams(100+i, now-100+int64(i)), limits)
		if err != nil {
			t.Fatal(err)
		}
		mustInsert(t, store, entry)
	}
	// Advance past the short retention: the first scan window (8 items)
	// contains only time-expired candidates, so the page may return fewer
	// than six (35 Q35-05) while still offering a cursor.
	now += 1000
	page, err := store.ListPage(ctx, player, "", now)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) >= limits.PageSize {
		t.Fatalf("scan cap must allow a short page, got %d", len(page.Items))
	}
	if page.NextCursor == "" {
		t.Fatal("short page with remaining candidates must still offer a cursor")
	}
	// The next page continues after the scanned window and finds the four
	// long-lived mails.
	page2, err := store.ListPage(ctx, player, page.NextCursor, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Items) != 4 {
		t.Fatalf("remaining visible mail must appear on the next page, got %d", len(page2.Items))
	}
}

func TestListPageExpiredFilteredByTime(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	player := uint64(10001)
	entry := mustBuild(t, textDelivery(player, 0, now))
	mustInsert(t, store, entry)
	// The expire worker has not run, but reads filter by trusted time (35A
	// §7.2): an expired mail never shows as claimable in the list.
	now = entry.ExpireAtMs + 1
	page, err := store.ListPage(ctx, player, "", now)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 0 {
		t.Fatalf("expired mail must be time-filtered: %+v", page.Items)
	}
}

func TestListPageCursorIsolationBetweenRecipients(t *testing.T) {
	now := testNow
	store := newTestStore(t, &now)
	ctx := context.Background()
	entry := mustBuild(t, textDelivery(10001, 0, now))
	mustInsert(t, store, entry)
	page, err := store.ListPage(ctx, 10001, "", now)
	if err != nil {
		t.Fatal(err)
	}
	// A cursor from another recipient's page decodes fine but scans an empty
	// mailbox; it can never leak entries (recipient is the physical key).
	if _, err := store.ListPage(ctx, 20002, page.NextCursor, now); err != nil {
		t.Fatal(err)
	}
	if got, err := store.ListPage(ctx, 20002, "", now); err != nil || len(got.Items) != 0 {
		t.Fatalf("empty mailbox: %+v %v", got, err)
	}
	fmt.Sscan("") // keep fmt import if unused elsewhere
}
