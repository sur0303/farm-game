package friend

import (
	"fmt"
	"testing"
)

func buildRoster(t *testing.T, viewer uint64, friends int) (*FriendIndex, *MemoryPairRepository) {
	t.Helper()
	ix := NewFriendIndex(viewer)
	pairs := NewMemoryPairRepository()
	for i := 0; i < friends; i++ {
		friendID := uint64(90000 + i)
		relationID := pairIDMust(viewer, friendID)
		epoch := uint64(1)
		activatedAt := int64(1000 + int64(i)*10)
		// Make the canonical pair ACTIVE so pages validate.
		entry, err := pairs.entryFor(viewer, friendID)
		if err != nil {
			t.Fatal(err)
		}
		record, err := entry.loadOrCreate(viewer, friendID, true)
		if err != nil {
			t.Fatal(err)
		}
		record.Relation.Status = RelationStatusActive
		record.Relation.RelationEpoch = epoch
		record.Relation.ActivatedAtMs = activatedAt
		ix.ApplyRelationActivated(FriendIndexEntry{
			FriendPlayerID: friendID, RelationID: relationID,
			RelationEpoch: epoch, ActivatedAtMs: activatedAt,
		})
	}
	return ix, pairs
}

func TestRosterPaginationStable(t *testing.T) {
	viewer := uint64(10001)
	ix, pairs := buildRoster(t, viewer, 45)

	// Page 1.
	page1, err := ListRoster(context0, ix, pairs, "", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1.Entries) != 20 || page1.NextCursor == "" {
		t.Fatalf("page1 = %d entries cursor=%q", len(page1.Entries), page1.NextCursor)
	}
	// Stable order: activated_at DESC, friend id ASC.
	for i := 1; i < len(page1.Entries); i++ {
		prev, cur := page1.Entries[i-1], page1.Entries[i]
		if prev.ActivatedAtMs < cur.ActivatedAtMs ||
			(prev.ActivatedAtMs == cur.ActivatedAtMs && prev.FriendPlayerID > cur.FriendPlayerID) {
			t.Fatalf("unstable order at %d: %+v -> %+v", i, prev, cur)
		}
	}
	// Page 2 continues without overlap.
	page2, err := ListRoster(context0, ix, pairs, page1.NextCursor, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Entries) != 20 {
		t.Fatalf("page2 = %d entries", len(page2.Entries))
	}
	seen := map[uint64]bool{}
	for _, e := range append(page1.Entries, page2.Entries...) {
		if seen[e.FriendPlayerID] {
			t.Fatalf("duplicate friend %d across pages", e.FriendPlayerID)
		}
		seen[e.FriendPlayerID] = true
	}
	// Page 3 drains the roster.
	page3, err := ListRoster(context0, ix, pairs, page2.NextCursor, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(page3.Entries) != 5 || page3.NextCursor != "" {
		t.Fatalf("page3 = %d entries cursor=%q", len(page3.Entries), page3.NextCursor)
	}
}

func TestRosterCursorFailsOnRevisionChange(t *testing.T) {
	viewer := uint64(10001)
	ix, pairs := buildRoster(t, viewer, 30)
	page1, err := ListRoster(context0, ix, pairs, "", 20)
	if err != nil {
		t.Fatal(err)
	}
	// Roster changes: old cursor must fail, never mix old/new rosters.
	ix.ApplyRelationActivated(FriendIndexEntry{
		FriendPlayerID: 77777, RelationID: pairIDMust(viewer, 77777),
		RelationEpoch: 1, ActivatedAtMs: 99999,
	})
	if _, err := ListRoster(context0, ix, pairs, page1.NextCursor, 20); err != ErrRosterChanged {
		t.Fatalf("stale cursor err = %v", err)
	}
	// Foreign viewer cursor is invalid.
	foreign := ListCursor{ViewerPlayerID: 42, SortMode: SortModeActivatedDesc, RosterRevision: ix.RosterRevision}
	if _, err := ListRoster(context0, ix, pairs, foreign.Encode(), 20); err != ErrInvalidCursor {
		t.Fatalf("foreign cursor err = %v", err)
	}
}

func TestRosterStaleEntriesFilteredAndRefilled(t *testing.T) {
	viewer := uint64(10001)
	ix, pairs := buildRoster(t, viewer, 25)
	// Delete the canonical relation of the 3 newest entries; the index still
	// holds stale rows (projection lag).
	stale := ix.sortedEntries()[:3]
	for _, entry := range stale {
		entryRepo, err := pairs.entryFor(viewer, entry.FriendPlayerID)
		if err != nil {
			t.Fatal(err)
		}
		entryRepo.mu.Lock()
		entryRepo.record.Relation.Status = RelationStatusDeleted
		entryRepo.record.Relation.RelationEpoch++
		entryRepo.mu.Unlock()
	}
	page, err := ListRoster(context0, ix, pairs, "", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 20 {
		t.Fatalf("page = %d entries, want 20 refilled past stale rows", len(page.Entries))
	}
	for _, entry := range page.Entries {
		for _, s := range stale {
			if entry.FriendPlayerID == s.FriendPlayerID {
				t.Fatalf("stale friend %d leaked into the page", s.FriendPlayerID)
			}
		}
	}
}

func TestRosterRebuildFromCanonical(t *testing.T) {
	viewer := uint64(10001)
	ix, pairs := buildRoster(t, viewer, 10)
	// Corrupt the projection arbitrarily, then rebuild from the truth.
	ix.Entries[1] = FriendIndexEntry{FriendPlayerID: 1, RelationID: "bogus", RelationEpoch: 9}
	var snaps []PairSnapshot
	for friendID := range map[uint64]bool{} {
		_ = friendID
	}
	for i := 0; i < 10; i++ {
		snap, err := pairs.GetPair(context0, viewer, uint64(90000+i))
		if err != nil {
			t.Fatal(err)
		}
		snaps = append(snaps, snap)
	}
	ix.RebuildFromCanonical(snaps)
	if len(ix.Entries) != 10 {
		t.Fatalf("rebuilt entries = %d", len(ix.Entries))
	}
	if _, bogus := ix.Entries[1]; bogus {
		t.Fatal("bogus entry survived rebuild")
	}
	page, err := ListRoster(context0, ix, pairs, "", 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 10 {
		t.Fatalf("page after rebuild = %d", len(page.Entries))
	}
}

func TestRosterStaleTerminationEventDoesNotRemoveNewerEntry(t *testing.T) {
	ix := NewFriendIndex(10001)
	relationID := pairIDMust(10001, 20002)
	ix.ApplyRelationActivated(FriendIndexEntry{FriendPlayerID: 20002, RelationID: relationID, RelationEpoch: 3, ActivatedAtMs: 500})
	// A late termination event for epoch 1 must not remove the epoch-3 row.
	if ix.ApplyRelationTerminated(20002, relationID, 1) {
		t.Fatal("stale termination removed a newer entry")
	}
	// Exact (relation, epoch) match removes.
	if !ix.ApplyRelationTerminated(20002, relationID, 3) {
		t.Fatal("matching termination did not remove the entry")
	}
	if len(ix.Entries) != 0 {
		t.Fatal("entry should be gone")
	}
}

func TestCursorOpaqueRoundTrip(t *testing.T) {
	cursor := ListCursor{
		ViewerPlayerID: 10001, SortMode: SortModeActivatedDesc,
		RosterRevision: 7, LastActivatedAtMs: 12345, LastFriendPlayerID: 6789,
	}
	decoded, err := DecodeListCursor(cursor.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if decoded != cursor {
		t.Fatalf("round trip = %+v, want %+v", decoded, cursor)
	}
	if _, err := DecodeListCursor("not-a-cursor"); err != ErrInvalidCursor {
		t.Fatalf("garbage cursor err = %v", err)
	}
}

func BenchmarkListRosterPage20(b *testing.B) {
	viewer := uint64(10001)
	ix := NewFriendIndex(viewer)
	pairs := NewMemoryPairRepository()
	for i := 0; i < 300; i++ {
		friendID := uint64(90000 + i)
		relationID := pairIDMust(viewer, friendID)
		entry, _ := pairs.entryFor(viewer, friendID)
		record, _ := entry.loadOrCreate(viewer, friendID, true)
		record.Relation.Status = RelationStatusActive
		record.Relation.RelationEpoch = 1
		record.Relation.ActivatedAtMs = int64(i)
		ix.ApplyRelationActivated(FriendIndexEntry{
			FriendPlayerID: friendID, RelationID: relationID, RelationEpoch: 1, ActivatedAtMs: int64(i),
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ListRoster(context0, ix, pairs, "", 20); err != nil {
			b.Fatal(err)
		}
	}
	_ = fmt.Sprint()
}
