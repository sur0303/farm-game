package friend

import (
	"context"
	"fmt"
	"testing"

	publicpb "farmgame/api/gen/public"
)

type readTestProfiles struct {
	calls int
	ids   []uint64
}

func (r *readTestProfiles) BatchGetPublicProfiles(_ context.Context, ids []uint64) (map[uint64]*publicpb.PublicProfileView, error) {
	r.calls++
	r.ids = append([]uint64(nil), ids...)
	out := make(map[uint64]*publicpb.PublicProfileView, len(ids))
	for _, id := range ids {
		out[id] = &publicpb.PublicProfileView{PlayerId: id, DisplayName: fmt.Sprintf("friend-%d", id), AvatarId: 1, Level: 2}
	}
	return out, nil
}

type readTestOpportunities struct {
	calls            int
	ids              []uint64
	omitTheftTargets bool
}

func (r *readTestOpportunities) BatchEvaluateFriendOpportunity(_ context.Context, _ uint64, ids []uint64, _ int64) ([]FriendOpportunityEvaluation, error) {
	r.calls++
	r.ids = append([]uint64(nil), ids...)
	out := make([]FriendOpportunityEvaluation, len(ids))
	for i, id := range ids {
		status, mask := OpportunityReadReady, FriendActionCanWater|FriendActionCanTheft
		if i == 1 {
			status, mask = OpportunityReadUnknown, FriendActionCanPest
		}
		out[i] = FriendOpportunityEvaluation{OwnerPlayerID: id, Status: status, ActionMask: mask, OpportunityRevision: uint64(100 + i)}
		if status == OpportunityReadReady && !r.omitTheftTargets {
			out[i].EligibleTheftTargets = []FriendTheftTarget{{PlotID: 1, CropCycleID: fmt.Sprintf("cycle-%d", id)}}
		}
	}
	return out, nil
}

func activateReadPair(t *testing.T, pairs *MemoryPairRepository, left, right uint64, epoch uint64, activatedAt int64) PairSnapshot {
	t.Helper()
	entry, err := pairs.entryFor(left, right)
	if err != nil {
		t.Fatal(err)
	}
	record, err := entry.loadOrCreate(left, right, true)
	if err != nil {
		t.Fatal(err)
	}
	record.Relation.Status = RelationStatusActive
	record.Relation.RelationEpoch = epoch
	record.Relation.ActivatedAtMs = activatedAt
	snapshot, err := pairs.GetPair(context.Background(), left, right)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func addReadFriend(t *testing.T, index *MemoryFriendIndexRepository, pairs *MemoryPairRepository, viewer, peer uint64, activatedAt int64) {
	t.Helper()
	pair := activateReadPair(t, pairs, viewer, peer, 1, activatedAt)
	if _, err := index.Apply(context.Background(), viewer, FriendIndexEntry{
		FriendPlayerID: peer, RelationID: pair.Relation.RelationID, RelationEpoch: 1, ActivatedAtMs: activatedAt,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFriendReadUsesOneProfileAndOpportunityBatchAndPreservesUnknown(t *testing.T) {
	const viewer uint64 = 77
	index, pairs := NewMemoryFriendIndexRepository(), NewMemoryPairRepository()
	for i := 0; i < 25; i++ {
		addReadFriend(t, index, pairs, viewer, uint64(9000+i), int64(1000+i))
	}
	profiles, opportunities := &readTestProfiles{}, &readTestOpportunities{}
	api, err := NewReadAPI(index, pairs, profiles, opportunities, func() int64 { return 5000 })
	if err != nil {
		t.Fatal(err)
	}
	page, err := api.GetFriendList(context.Background(), viewer, "", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Friends) != 20 || page.NextCursor == "" || profiles.calls != 1 || opportunities.calls != 1 {
		t.Fatalf("page=%d next=%q profileCalls=%d opportunityCalls=%d", len(page.Friends), page.NextCursor, profiles.calls, opportunities.calls)
	}
	if page.Friends[0].FriendPlayerID != 9024 || page.Friends[19].FriendPlayerID != 9005 {
		t.Fatalf("activation order first=%d last=%d", page.Friends[0].FriendPlayerID, page.Friends[19].FriendPlayerID)
	}
	if uint32(page.Friends[0].Action) != FriendActionCanVisit|FriendActionCanWater|FriendActionCanTheft {
		t.Fatalf("ready action mask=%d", page.Friends[0].Action)
	}
	if page.Friends[1].OpportunityStatus != OpportunityReadUnknown || uint32(page.Friends[1].Action) != FriendActionCanVisit {
		t.Fatalf("UNKNOWN was downgraded or enabled actions: %+v", page.Friends[1])
	}
	page2, err := api.GetFriendList(context.Background(), viewer, page.NextCursor, 20)
	if err != nil || len(page2.Friends) != 5 || page2.NextCursor != "" {
		t.Fatalf("page2=%+v err=%v", page2, err)
	}
}

func TestFriendReadMapsTheftMaskTargetMismatchToPerCardUnknown(t *testing.T) {
	const viewer, peer = uint64(78), uint64(9100)
	index, pairs := NewMemoryFriendIndexRepository(), NewMemoryPairRepository()
	addReadFriend(t, index, pairs, viewer, peer, 1000)
	api, err := NewReadAPI(index, pairs, &readTestProfiles{}, &readTestOpportunities{omitTheftTargets: true}, func() int64 { return 5000 })
	if err != nil {
		t.Fatal(err)
	}
	page, err := api.GetFriendList(context.Background(), viewer, "", 20)
	if err != nil || len(page.Friends) != 1 || page.Friends[0].OpportunityStatus != OpportunityReadUnknown ||
		uint32(page.Friends[0].Action) != FriendActionCanVisit || len(page.Friends[0].EligibleTheftTargets) != 0 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
}

func TestFriendIndexReconcilerRepairsMissingAndStaleRowsFromQuotaAndPairTruth(t *testing.T) {
	env := newTestEnv(t, 300)
	token := env.generateToken(t, 100, 1000)
	result := env.redeem(t, 200, 100, token, "reconcile-redeem", 1100)
	if result.Outcome.Status != StatusSucceeded {
		t.Fatalf("redeem=%+v", result)
	}
	index := NewMemoryFriendIndexRepository()
	staleRelation := pairIDMust(100, 300)
	if _, err := index.Apply(context.Background(), 100, FriendIndexEntry{FriendPlayerID: 300,
		RelationID: staleRelation, RelationEpoch: 1, ActivatedAtMs: 900}); err != nil {
		t.Fatal(err)
	}
	reconciler, err := NewFriendIndexReconciler(index, env.pairs, env.quotas)
	if err != nil {
		t.Fatal(err)
	}
	report, err := reconciler.ReconcileViewer(context.Background(), 100)
	if err != nil || report.CanonicalRows != 1 || report.AppliedRows != 1 || report.RemovedRows != 1 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	scan, err := index.Scan(context.Background(), 100, "", 20)
	if err != nil || len(scan.Entries) != 1 || scan.Entries[0].FriendPlayerID != 200 ||
		scan.Entries[0].RelationID != result.RelationID || scan.Entries[0].RelationEpoch != result.RelationEpoch {
		t.Fatalf("reconciled scan=%+v err=%v", scan, err)
	}
	repeat, err := reconciler.ReconcileViewer(context.Background(), 100)
	if err != nil || repeat.AppliedRows != 0 || repeat.RemovedRows != 0 {
		t.Fatalf("repeat=%+v err=%v", repeat, err)
	}
}

func TestFriendReadRefillsPastStaleProjectionAndCursorIsRevisionFenced(t *testing.T) {
	const viewer uint64 = 88
	index, pairs := NewMemoryFriendIndexRepository(), NewMemoryPairRepository()
	for i := 0; i < 21; i++ {
		addReadFriend(t, index, pairs, viewer, uint64(8000+i), int64(2000+i))
	}
	// Leave the newest projection present while canonical Pair becomes blocked.
	entry, _ := pairs.entryFor(viewer, 8020)
	record, _ := entry.loadOrCreate(viewer, 8020, false)
	record.Relation.Status = RelationStatusDeleted
	profiles, opportunities := &readTestProfiles{}, &readTestOpportunities{}
	api, _ := NewReadAPI(index, pairs, profiles, opportunities, func() int64 { return 6000 })
	page, err := api.GetFriendList(context.Background(), viewer, "", 20)
	if err != nil || len(page.Friends) != 20 || page.Friends[0].FriendPlayerID != 8019 {
		t.Fatalf("refilled page=%+v err=%v", page, err)
	}
	if page.NextCursor != "" {
		t.Fatalf("all candidates consumed but next=%q", page.NextCursor)
	}
	// A projection revision change invalidates an old cursor rather than
	// silently continuing across a reordered roster.
	first, err := index.Scan(context.Background(), viewer, "", 5)
	if err != nil {
		t.Fatal(err)
	}
	oldCursor := cursorAfterEntry(viewer, first.RosterRevision, first.Entries[4])
	addReadFriend(t, index, pairs, viewer, 8999, 9999)
	if _, err := api.GetFriendList(context.Background(), viewer, oldCursor, 5); err == nil {
		t.Fatal("expected roster revision fence")
	}
}

type countingPairs struct {
	*MemoryPairRepository
	getCalls int
}

func (c *countingPairs) GetPair(ctx context.Context, playerA, playerB uint64) (PairSnapshot, error) {
	c.getCalls++
	return c.MemoryPairRepository.GetPair(ctx, playerA, playerB)
}

func TestScanFriendIndexRosterSkipsPairAndKeepActiveFriendsFiltersStale(t *testing.T) {
	const viewer uint64 = 501
	index := NewMemoryFriendIndexRepository()
	memoryPairs := NewMemoryPairRepository()
	pairs := &countingPairs{MemoryPairRepository: memoryPairs}
	profiles := &readTestProfiles{}
	opportunities := &readTestOpportunities{}
	api, err := NewReadAPI(index, pairs, profiles, opportunities, func() int64 { return 7000 })
	if err != nil {
		t.Fatal(err)
	}
	addReadFriend(t, index, memoryPairs, viewer, 601, 3000)
	addReadFriend(t, index, memoryPairs, viewer, 602, 2000)
	addReadFriend(t, index, memoryPairs, viewer, 603, 1000)
	// Leave a stale Index row after deleting the canonical Pair.
	entry, err := memoryPairs.entryFor(viewer, 602)
	if err != nil {
		t.Fatal(err)
	}
	record, err := entry.loadOrCreate(viewer, 602, false)
	if err != nil {
		t.Fatal(err)
	}
	record.Relation.Status = RelationStatusDeleted

	pairs.getCalls = 0
	roster, err := api.ScanFriendIndexRoster(context.Background(), viewer, 300)
	if err != nil {
		t.Fatal(err)
	}
	if pairs.getCalls != 0 {
		t.Fatalf("ScanFriendIndexRoster pair calls=%d, want 0", pairs.getCalls)
	}
	if len(roster.FarmIDs) != 3 {
		t.Fatalf("index roster=%v, want all three projected farms", roster.FarmIDs)
	}

	kept, err := api.KeepActiveFriends(context.Background(), viewer, roster.FarmIDs)
	if err != nil {
		t.Fatal(err)
	}
	if pairs.getCalls != 3 {
		t.Fatalf("KeepActiveFriends pair calls=%d, want 3", pairs.getCalls)
	}
	if len(kept) != 2 || kept[0] != 601 || kept[1] != 603 {
		t.Fatalf("KeepActiveFriends=%v, want active 601 then 603", kept)
	}

	validated, err := api.GetValidatedFriendRoster(context.Background(), viewer, 300)
	if err != nil {
		t.Fatal(err)
	}
	if len(validated.FarmIDs) != 2 || validated.FarmIDs[0] != 601 || validated.FarmIDs[1] != 603 {
		t.Fatalf("validated roster=%v, want Pair-filtered farms", validated.FarmIDs)
	}
}

type countingIndex struct {
	*MemoryFriendIndexRepository
	lookupCalls int
	batchLookups int
	scans       int
}

func (c *countingIndex) Lookup(ctx context.Context, viewer, peer uint64) (FriendIndexEntry, bool, error) {
	c.lookupCalls++
	return c.MemoryFriendIndexRepository.Lookup(ctx, viewer, peer)
}

func (c *countingIndex) BatchLookup(ctx context.Context, viewer uint64, peers []uint64) ([]FriendIndexEntry, error) {
	c.batchLookups++
	return c.MemoryFriendIndexRepository.BatchLookup(ctx, viewer, peers)
}

func (c *countingIndex) Scan(ctx context.Context, viewer uint64, cursor string, limit int) (FriendIndexScan, error) {
	c.scans++
	return c.MemoryFriendIndexRepository.Scan(ctx, viewer, cursor, limit)
}

func TestKeepActiveFriendsUsesOneIndexBatchLookup(t *testing.T) {
	const viewer uint64 = 701
	memory := NewMemoryFriendIndexRepository()
	index := &countingIndex{MemoryFriendIndexRepository: memory}
	pairs := NewMemoryPairRepository()
	api, err := NewReadAPI(index, pairs, &readTestProfiles{}, &readTestOpportunities{}, func() int64 { return 7000 })
	if err != nil {
		t.Fatal(err)
	}
	addReadFriend(t, memory, pairs, viewer, 801, 3000)
	addReadFriend(t, memory, pairs, viewer, 802, 2000)
	kept, err := api.KeepActiveFriends(context.Background(), viewer, []uint64{801, 802})
	if err != nil {
		t.Fatal(err)
	}
	if index.batchLookups != 1 {
		t.Fatalf("batch lookups=%d, want 1", index.batchLookups)
	}
	if len(kept) != 2 {
		t.Fatalf("kept=%v", kept)
	}
}

func TestGetFriendRosterRevisionDoesNotScan(t *testing.T) {
	const viewer uint64 = 711
	memory := NewMemoryFriendIndexRepository()
	index := &countingIndex{MemoryFriendIndexRepository: memory}
	pairs := NewMemoryPairRepository()
	api, err := NewReadAPI(index, pairs, &readTestProfiles{}, &readTestOpportunities{}, func() int64 { return 7000 })
	if err != nil {
		t.Fatal(err)
	}
	addReadFriend(t, memory, pairs, viewer, 811, 3000)
	revision, err := api.GetFriendRosterRevision(context.Background(), viewer)
	if err != nil || revision == 0 {
		t.Fatalf("revision=%d err=%v", revision, err)
	}
	if index.scans != 0 {
		t.Fatalf("scans=%d, want revision HGET only", index.scans)
	}
}

func TestAssembleFriendCardSkipsIndexPairAndFacts(t *testing.T) {
	const viewer uint64 = 721
	index := NewMemoryFriendIndexRepository()
	pairs := NewMemoryPairRepository()
	profiles := &readTestProfiles{}
	opportunities := &readTestOpportunities{}
	api, err := NewReadAPI(index, pairs, profiles, opportunities, func() int64 { return 7000 })
	if err != nil {
		t.Fatal(err)
	}
	addReadFriend(t, index, pairs, viewer, 821, 3000)
	entry, found, err := index.Lookup(context.Background(), viewer, 821)
	if err != nil || !found {
		t.Fatalf("lookup found=%v err=%v", found, err)
	}
	card, err := api.AssembleFriendCard(context.Background(), viewer, entry, FriendOpportunityEvaluation{
		OwnerPlayerID: 821, Status: OpportunityReadReady, ActionMask: FriendActionCanWater, OpportunityRevision: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if opportunities.calls != 0 || profiles.calls != 1 {
		t.Fatalf("facts=%d profiles=%d, want facts 0 profiles 1", opportunities.calls, profiles.calls)
	}
	if card.FriendPlayerID != 821 || card.OpportunityRevision != 4 || !card.Action.Actionable() {
		t.Fatalf("card=%+v", card)
	}
}
