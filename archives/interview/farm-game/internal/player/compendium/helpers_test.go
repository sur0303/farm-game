package compendium

// testLimits returns a generous but valid LimitProfile. Tests that exercise
// capacity fences build their own tighter profiles.
func testLimits() LimitProfile {
	return LimitProfile{
		ProfileVersion:           1,
		MaxCatalogEntries:        64,
		MaxActivityArchives:      8,
		MaxUnlockedEntries:       64,
		MaxCompendiumStateBytes:  1 << 20,
		MaxCompendiumPageEntries: 64,
		MaxCompendiumPageBytes:   1 << 20,
	}
}

// snap builds a plant-time frozen unlock snapshot.
func snap(cropID uint32, eligible bool, plantedBy uint64, activityID string) UnlockSnapshot {
	return UnlockSnapshot{
		CropDefinitionID:         cropID,
		UnlockEligible:           eligible,
		PlantedByPlayerID:        plantedBy,
		ActivityID:               activityID,
		CatalogVersionAtPlant:    7,
		DefinitionVersionAtPlant: 3,
	}
}

// ownHarvestCmd builds the unlock adjudication input of a successful harvest
// by the owner on their own farm.
func ownHarvestCmd(playerID uint64, cropID uint32, snapshot UnlockSnapshot) UnlockCommand {
	return UnlockCommand{
		PlayerID:         playerID,
		OwnerPlayerID:    playerID,
		CropDefinitionID: cropID,
		Snapshot:         snapshot,
	}
}

// normalEntry builds a NORMAL catalog entry.
func normalEntry(cropID uint32) PlantCatalogEntry {
	return PlantCatalogEntry{
		CropDefinitionID:    cropID,
		DefinitionVersion:   1,
		DisplayDefinitionID: "display_normal",
		Category:            EntryCategoryNormal,
		SilhouetteAssetID:   "sil_normal",
		DetailAssetID:       "detail_normal",
	}
}

// activityEntry builds an ACTIVITY catalog entry.
func activityEntry(cropID uint32, activityID string, st ActivityState, legacyPlantable bool) PlantCatalogEntry {
	return PlantCatalogEntry{
		CropDefinitionID:    cropID,
		DefinitionVersion:   1,
		DisplayDefinitionID: "display_" + activityID,
		Category:            EntryCategoryActivity,
		ActivityID:          activityID,
		ActivityState:       st,
		SilhouetteAssetID:   "sil_" + activityID,
		DetailAssetID:       "detail_" + activityID,
		LegacySeedPlantable: legacyPlantable,
	}
}

// unlockOne applies one successful first-unlock to state or fails the test.
func unlockOne(t testT, state State, playerID uint64, cropID uint32, activityID string) State {
	out := DecideUnlock(state, ownHarvestCmd(playerID, cropID, snap(cropID, true, playerID, activityID)), testLimits())
	if out.Status != StatusSucceeded {
		t.Fatalf("unlock of crop %d: got %s err=%v", cropID, out.Status, out.Err)
	}
	return out.Candidate
}

// testT is the minimal subset of *testing.T used by helpers.
type testT interface {
	Fatalf(format string, args ...any)
}
