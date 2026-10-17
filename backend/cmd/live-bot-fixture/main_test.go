package main

import "testing"

func TestFriendRingPairsCanonicalAndUnique(t *testing.T) {
	pairs := friendRingPairs(100, 4, 1)
	want := []fixtureFriendPair{{100, 101}, {100, 103}, {101, 102}, {102, 103}}
	if len(pairs) != len(want) {
		t.Fatalf("pairs=%v, want %v", pairs, want)
	}
	for index := range want {
		if pairs[index] != want[index] {
			t.Fatalf("pairs[%d]=%v, want %v", index, pairs[index], want[index])
		}
	}
}

func TestFriendRingPairsDeduplicatesOppositeEdges(t *testing.T) {
	pairs := friendRingPairs(200, 4, 2)
	want := []fixtureFriendPair{{200, 202}, {201, 203}}
	if len(pairs) != len(want) {
		t.Fatalf("pairs=%v, want %v", pairs, want)
	}
	for index := range want {
		if pairs[index] != want[index] {
			t.Fatalf("pairs[%d]=%v, want %v", index, pairs[index], want[index])
		}
	}
}

func TestFriendRingPairsRejectsInvalidShape(t *testing.T) {
	for _, testCase := range []struct {
		start, players, offset int
	}{
		{0, 2, 1},
		{100, 1, 1},
		{100, 2, 0},
		{100, 2, 2},
	} {
		if got := friendRingPairs(uint64(testCase.start), testCase.players, testCase.offset); got != nil {
			t.Fatalf("friendRingPairs(%d,%d,%d)=%v, want nil", testCase.start, testCase.players, testCase.offset, got)
		}
	}
}

func TestFriendRingQuotaRelationsGroupByPlayer(t *testing.T) {
	pairs := []fixtureFriendPair{{100, 101}, {100, 103}, {101, 102}, {102, 103}}
	relations, err := friendRingQuotaRelations(pairs)
	if err != nil {
		t.Fatal(err)
	}
	if len(relations) != 4 {
		t.Fatalf("players=%d, want 4", len(relations))
	}
	for playerID, playerRelations := range relations {
		if len(playerRelations) != 2 {
			t.Fatalf("player %d relations=%v, want 2", playerID, playerRelations)
		}
		if playerRelations[0].relationID >= playerRelations[1].relationID {
			t.Fatalf("player %d relations are not sorted: %v", playerID, playerRelations)
		}
	}
}
