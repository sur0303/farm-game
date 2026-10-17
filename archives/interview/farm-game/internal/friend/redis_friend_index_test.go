package friend

import (
	"context"
	"errors"
	"testing"
)

func TestRedisFriendIndexEmptyScanReturnsAuthoritativeEmptyPage(t *testing.T) {
	repositories, _ := newFriendRedisTestRepositories(t, 300)
	scan, err := repositories.Index.Scan(context.Background(), 44, "", 20)
	if err != nil {
		t.Fatalf("empty scan: %v", err)
	}
	if scan.Entries == nil || len(scan.Entries) != 0 || scan.HasMore || scan.RosterRevision != 0 {
		t.Fatalf("empty scan=%+v", scan)
	}
}

func TestRedisFriendIndexBatchLookupAndRosterRevision(t *testing.T) {
	repositories, _ := newFriendRedisTestRepositories(t, 300)
	ctx := context.Background()
	const viewer uint64 = 55
	first := FriendIndexEntry{FriendPlayerID: 8001, RelationID: pairIDMust(viewer, 8001), RelationEpoch: 1, ActivatedAtMs: 1000}
	second := FriendIndexEntry{FriendPlayerID: 8002, RelationID: pairIDMust(viewer, 8002), RelationEpoch: 2, ActivatedAtMs: 2000}
	if _, err := repositories.Index.Apply(ctx, viewer, first); err != nil {
		t.Fatal(err)
	}
	if _, err := repositories.Index.Apply(ctx, viewer, second); err != nil {
		t.Fatal(err)
	}
	revision, err := repositories.Index.RosterRevision(ctx, viewer)
	if err != nil || revision != 2 {
		t.Fatalf("revision=%d err=%v", revision, err)
	}
	entries, err := repositories.Index.BatchLookup(ctx, viewer, []uint64{8002, 8001, 8099})
	if err != nil || len(entries) != 2 || entries[0].FriendPlayerID != 8002 || entries[1].FriendPlayerID != 8001 {
		t.Fatalf("batch=%+v err=%v", entries, err)
	}
}

func TestRedisFriendIndexPagedOrderEpochFenceAndRevisionCursor(t *testing.T) {
	repositories, _ := newFriendRedisTestRepositories(t, 300)
	ctx := context.Background()
	const viewer uint64 = 44
	for i := 0; i < 25; i++ {
		entry := FriendIndexEntry{FriendPlayerID: uint64(7000 + i), RelationID: pairIDMust(viewer, uint64(7000+i)), RelationEpoch: 1, ActivatedAtMs: int64(1000 + i)}
		changed, err := repositories.Index.Apply(ctx, viewer, entry)
		if err != nil || !changed {
			t.Fatalf("apply %d changed=%v err=%v", i, changed, err)
		}
		if changed, err := repositories.Index.Apply(ctx, viewer, entry); err != nil || changed {
			t.Fatalf("idempotent apply %d changed=%v err=%v", i, changed, err)
		}
	}
	first, err := repositories.Index.Scan(ctx, viewer, "", 20)
	if err != nil || len(first.Entries) != 20 || !first.HasMore || first.RosterRevision != 25 {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	if first.Entries[0].FriendPlayerID != 7024 || first.Entries[19].FriendPlayerID != 7005 {
		t.Fatalf("order first=%d last=%d", first.Entries[0].FriendPlayerID, first.Entries[19].FriendPlayerID)
	}
	cursor := cursorAfterEntry(viewer, first.RosterRevision, first.Entries[19])
	second, err := repositories.Index.Scan(ctx, viewer, cursor, 20)
	if err != nil || len(second.Entries) != 5 || second.HasMore || second.Entries[0].FriendPlayerID != 7004 {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	// Older projection events cannot remove a newer epoch.
	newest := first.Entries[0]
	newest.RelationEpoch = 2
	newest.ActivatedAtMs = 2000
	if changed, err := repositories.Index.Apply(ctx, viewer, newest); err != nil || !changed {
		t.Fatalf("epoch2 apply changed=%v err=%v", changed, err)
	}
	if changed, err := repositories.Index.Remove(ctx, viewer, newest.FriendPlayerID, newest.RelationID, 1); err != nil || changed {
		t.Fatalf("stale remove changed=%v err=%v", changed, err)
	}
	if _, err := repositories.Index.Scan(ctx, viewer, cursor, 20); !errors.Is(err, ErrRosterChanged) {
		t.Fatalf("old cursor err=%v", err)
	}
	if changed, err := repositories.Index.Remove(ctx, viewer, newest.FriendPlayerID, newest.RelationID, 2); err != nil || !changed {
		t.Fatalf("epoch2 remove changed=%v err=%v", changed, err)
	}
}

func TestRedisNavigationVisitedRequiresServerFenceAndIsIdempotent(t *testing.T) {
	repositories, _ := newFriendRedisTestRepositories(t, 300)
	ctx := context.Background()
	if err := repositories.Navigation.MarkVisited(ctx, VisitFence{ViewerPlayerID: 91, TargetFarmID: 91,
		ActiveSessionEpoch: 1, SubscriptionID: "self"}); err == nil {
		t.Fatal("self farm must not become a friend visited mark")
	}
	fence := VisitFence{ViewerPlayerID: 91, TargetFarmID: 92, ActiveSessionEpoch: 3, SubscriptionID: "sub-92"}
	if err := repositories.Navigation.MarkVisited(ctx, fence); err != nil {
		t.Fatal(err)
	}
	if err := repositories.Navigation.MarkVisited(ctx, fence); err != nil {
		t.Fatal(err)
	}
	visited, err := repositories.Navigation.ListVisited(ctx, 91)
	if err != nil || len(visited) != 1 {
		t.Fatalf("visited=%v err=%v", visited, err)
	}
	if _, ok := visited[92]; !ok {
		t.Fatalf("visited=%v", visited)
	}
}

func TestRedisFriendIndexRepairQueueLeasesAndRunsThreeRecoveryRounds(t *testing.T) {
	repositories, _ := newFriendRedisTestRepositories(t, 300)
	ctx := context.Background()
	queue := repositories.RepairQueue
	if err := queue.Enqueue(ctx, 91, 1000); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < defaultFriendIndexRepairRounds; round++ {
		now := int64(1000 + round*100)
		leases, err := queue.Claim(ctx, now, 50, 10)
		if err != nil || len(leases) != 1 || leases[0].ViewerPlayerID != 91 {
			t.Fatalf("round=%d leases=%v err=%v", round, leases, err)
		}
		if err := queue.Complete(ctx, leases[0], now+1, 99); err != nil {
			t.Fatal(err)
		}
	}
	leases, err := queue.Claim(ctx, 2000, 50, 10)
	if err != nil || len(leases) != 0 {
		t.Fatalf("completed queue leases=%v err=%v", leases, err)
	}
	// An enqueue racing a lease moves the due score. Completion of the old
	// lease cannot delete the newer repair intent.
	if err := queue.Enqueue(ctx, 92, 3000); err != nil {
		t.Fatal(err)
	}
	leased, _ := queue.Claim(ctx, 3000, 100, 1)
	if err := queue.Enqueue(ctx, 92, 3001); err != nil {
		t.Fatal(err)
	}
	if err := queue.Complete(ctx, leased[0], 3002, 100); err != nil {
		t.Fatal(err)
	}
	again, err := queue.Claim(ctx, 3002, 100, 1)
	if err != nil || len(again) != 1 || again[0].ViewerPlayerID != 92 {
		t.Fatalf("newer intent lost leases=%v err=%v", again, err)
	}
}

func TestFriendIndexKeysAreViewerLocalAndSameSlot(t *testing.T) {
	repositories, _ := newFriendRedisTestRepositories(t, 300)
	keys, err := repositories.Index.keyStrings(91)
	if err != nil || len(keys) != 6 {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
	for _, key := range keys {
		if key == "" {
			t.Fatal("empty friend index key")
		}
	}
	other, err := repositories.Index.keyStrings(92)
	if err != nil || keys[0] == other[0] {
		t.Fatalf("viewer key isolation keys=%v other=%v err=%v", keys, other, err)
	}
}
