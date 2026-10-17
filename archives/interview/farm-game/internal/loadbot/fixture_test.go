package loadbot

import (
	"reflect"
	"strings"
	"testing"
)

func fixtureForTest(t *testing.T, playerCount int) FixtureManifest {
	t.Helper()
	config := DefaultConfig()
	config.RunID = "fixture-contract-test"
	config.HotFarmViewerCounts = []int{2, 4}
	spec, err := NewRunSpec(config, "fixture-contract", string(EnvironmentSandbox), "")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildDryRun(config, playerCount, 3)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := BuildFixtureManifest(spec, plan)
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func TestFixtureManifestContainsCanonicalPairBothIndexesAndPreparation(t *testing.T) {
	manifest := fixtureForTest(t, 6)
	if err := manifest.Validate(); err != nil {
		t.Fatalf("FixtureManifest.Validate() error = %v", err)
	}
	if len(manifest.Accounts) != 6 || len(manifest.Players) != 6 || len(manifest.Farms) != 6 {
		t.Fatalf("entity counts accounts=%d players=%d farms=%d", len(manifest.Accounts), len(manifest.Players), len(manifest.Farms))
	}
	if len(manifest.CanonicalPairs) != 5 {
		t.Fatalf("canonical pair count = %d, want 5", len(manifest.CanonicalPairs))
	}
	for _, pair := range manifest.CanonicalPairs {
		if pair.LowPlayerID >= pair.HighPlayerID || pair.PairID == "" || pair.PairID != pair.RelationID {
			t.Fatalf("pair is not a canonical FriendRelation: %+v", pair)
		}
		participants, ok := manifest.Index.PairByID[pair.PairID]
		if !ok || participants.LowPlayerID != pair.LowPlayerID || participants.HighPlayerID != pair.HighPlayerID {
			t.Fatalf("pair index missing %q: %+v", pair.PairID, participants)
		}
		cleanupKey := manifest.Cleanup.Scope + "/" + string(CleanupIndex) + "/pair-by-id/" + pair.PairID
		if !containsCleanupKey(manifest.Cleanup, cleanupKey) {
			t.Fatalf("PairByID cleanup key missing for %q", pair.PairID)
		}
		forward := manifest.Index.FriendIndexForward[pair.LowPlayerID]
		reverse := manifest.Index.FriendIndexReverse[pair.HighPlayerID]
		if !containsPlayer(forward, pair.HighPlayerID) || !containsPlayer(reverse, pair.LowPlayerID) {
			t.Fatalf("directed FriendIndex rows missing for pair %+v: forward=%v reverse=%v", pair, forward, reverse)
		}
	}
	for _, account := range manifest.Accounts {
		if manifest.Index.AccountToPlayer[account.AccountID] != account.PlayerID ||
			manifest.Index.PlayerToAccount[account.PlayerID] != account.AccountID {
			t.Fatalf("account index is not bidirectional: %+v", account)
		}
	}
	for _, farm := range manifest.Farms {
		if manifest.Index.PlayerToFarm[farm.OwnerPlayerID] != farm.FarmID ||
			manifest.Index.FarmToPlayer[farm.FarmID] != farm.OwnerPlayerID {
			t.Fatalf("farm index is not bidirectional: %+v", farm)
		}
	}
	if len(manifest.Hotspots) != 2 || len(manifest.Opportunities) != len(manifest.CanonicalPairs) {
		t.Fatalf("preparation counts hotspots=%d opportunities=%d", len(manifest.Hotspots), len(manifest.Opportunities))
	}
	for _, hotspot := range manifest.Hotspots {
		if !hotspot.Ready || hotspot.FarmID == 0 || hotspot.ViewerCount <= 0 ||
			hotspot.RequestedViewerCount <= 0 || hotspot.ViewerCount != len(hotspot.ViewerPlayerIDs) ||
			hotspot.ViewerCount != hotspot.RequestedViewerCount {
			t.Fatalf("hotspot is not prepared: %+v", hotspot)
		}
	}
	for _, opportunity := range manifest.Opportunities {
		if !opportunity.Ready || opportunity.PairID == "" || opportunity.FarmID == 0 ||
			(opportunity.RequiredAction != ActionCare && opportunity.RequiredAction != ActionTheft) {
			t.Fatalf("opportunity is not prepared: %+v", opportunity)
		}
	}
	if !manifest.Cleanup.Exact || len(manifest.Cleanup.Items) == 0 {
		t.Fatalf("cleanup is not exact: %+v", manifest.Cleanup)
	}
	for _, item := range manifest.Cleanup.Items {
		if !strings.HasPrefix(item.Key, manifest.Cleanup.Scope+"/") {
			t.Fatalf("cleanup key escaped scope: %q", item.Key)
		}
	}
}

func TestFixtureManifestIsDeterministicAndRejectsTampering(t *testing.T) {
	first := fixtureForTest(t, 5)
	second := fixtureForTest(t, 5)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same RunSpec and plan produced different fixture manifests")
	}
	first.CanonicalPairs[0].RelationID = "tampered"
	if err := first.Validate(); err == nil {
		t.Fatal("tampered canonical FriendRelation was accepted")
	}

	manifest := fixtureForTest(t, 5)
	manifest.Index.FriendIndexForward[manifest.Players[0].PlayerID] = nil
	if err := manifest.Validate(); err == nil {
		t.Fatal("tampered forward FriendIndex was accepted")
	}

	manifest = fixtureForTest(t, 5)
	manifest.Cleanup.Items[0].Key = manifest.Cleanup.Scope + "/friend_index/*"
	if err := manifest.Validate(); err == nil {
		t.Fatal("wildcard cleanup key was accepted")
	}
}

func containsPlayer(values []uint64, want uint64) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func containsCleanupKey(cleanup CleanupManifest, want string) bool {
	for _, item := range cleanup.Items {
		if item.Key == want {
			return true
		}
	}
	return false
}

func TestFixtureDefaultHotspotsUseEffectiveViewerCount(t *testing.T) {
	config := DefaultConfig()
	config.RunID = "fixture-default-hotspot-test"
	spec, err := NewRunSpec(config, "fixture-default-hotspot", string(EnvironmentSandbox), "")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildDryRun(config, 20, 3)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := BuildFixtureManifest(spec, plan)
	if err != nil {
		t.Fatal(err)
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("FixtureManifest.Validate() error = %v", err)
	}
	if len(manifest.Hotspots) != 2 {
		t.Fatalf("hotspot count = %d, want 2", len(manifest.Hotspots))
	}
	seenRequested := map[int]bool{50: false, 200: false}
	for _, hotspot := range manifest.Hotspots {
		if _, expected := seenRequested[hotspot.RequestedViewerCount]; !expected {
			t.Fatalf("unexpected requested viewer count: %+v", hotspot)
		}
		seenRequested[hotspot.RequestedViewerCount] = true
		if hotspot.ViewerCount != 20 || hotspot.ViewerCount != len(hotspot.ViewerPlayerIDs) {
			t.Fatalf("hotspot effective viewer count is inconsistent: %+v", hotspot)
		}
		if hotspot.Ready {
			t.Fatalf("20-player hotspot claims ready for %d viewers", hotspot.RequestedViewerCount)
		}
	}
	for requested, seen := range seenRequested {
		if !seen {
			t.Fatalf("missing default hotspot request for %d viewers", requested)
		}
	}
}
