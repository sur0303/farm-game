package compendium

import (
	"testing"

	playerpb "farmgame/api/gen/player"
)

func recordLimits() LimitProfile {
	return LimitProfile{
		ProfileVersion: 1, MaxCatalogEntries: 256, MaxActivityArchives: 64,
		MaxUnlockedEntries: 256, MaxCompendiumStateBytes: 64 << 10,
		MaxCompendiumPageEntries: 256, MaxCompendiumPageBytes: 256 << 10,
	}
}

func TestPlayerRecordRoundTripPreservesRevisionAndProvenance(t *testing.T) {
	record := &playerpb.PlayerRecord{PlayerId: 7}
	want := State{Revision: 2, SeenRevision: 1, Facts: []UnlockFact{
		{CropDefinitionID: 2, UnlockRevision: 2, SourceActivityID: "spring", CatalogVersionAtUnlock: 11, DefinitionVersionAtUnlock: 12},
		{CropDefinitionID: 9, UnlockRevision: 1, CatalogVersionAtUnlock: 8, DefinitionVersionAtUnlock: 9},
	}}
	if err := ApplyStateToPlayerRecord(record, want, "harvest-op", 1234); err != nil {
		t.Fatal(err)
	}
	got, err := StateFromPlayerRecord(record, recordLimits())
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != want.Revision || got.SeenRevision != want.SeenRevision || len(got.Facts) != 2 {
		t.Fatalf("state=%+v want=%+v", got, want)
	}
	for i := range want.Facts {
		if got.Facts[i] != want.Facts[i] {
			t.Fatalf("fact[%d]=%+v want=%+v", i, got.Facts[i], want.Facts[i])
		}
	}
}

func TestLegacyPlayerRecordMigrationDoesNotForgeActivity(t *testing.T) {
	record := &playerpb.PlayerRecord{PlayerId: 7, CodexEntries: []*playerpb.CodexEntry{
		{CropDefinitionId: 9, FirstHarvestedAtMs: 200, SourceOperationId: "op-new"},
		{CropDefinitionId: 2, FirstHarvestedAtMs: 100, SourceOperationId: "op-old"},
	}}
	got, err := StateFromPlayerRecord(record, recordLimits())
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != 2 || got.SeenRevision != 2 || got.HasNewUnlock() {
		t.Fatalf("legacy revisions=%+v", got)
	}
	if got.Facts[0].CropDefinitionID != 2 || got.Facts[0].UnlockRevision != 1 || got.Facts[0].SourceActivityID != "" ||
		got.Facts[1].CropDefinitionID != 9 || got.Facts[1].UnlockRevision != 2 || got.Facts[1].SourceActivityID != "" {
		t.Fatalf("legacy facts=%+v", got.Facts)
	}
}

func TestPlayerRecordRejectsInvalidCompendiumState(t *testing.T) {
	tests := []*playerpb.PlayerRecord{
		{PlayerId: 7, CodexEntries: []*playerpb.CodexEntry{{CropDefinitionId: -1, UnlockRevision: 1}}, CompendiumRevision: 1},
		{PlayerId: 7, CodexEntries: []*playerpb.CodexEntry{{CropDefinitionId: 1, UnlockRevision: 1}, {CropDefinitionId: 1, UnlockRevision: 2}}, CompendiumRevision: 2},
		{PlayerId: 7, CodexEntries: []*playerpb.CodexEntry{{CropDefinitionId: 1, UnlockRevision: 1}}, CompendiumRevision: 1, CompendiumSeenRevision: 2},
	}
	for i, record := range tests {
		if _, err := StateFromPlayerRecord(record, recordLimits()); err == nil {
			t.Fatalf("case %d: expected error", i)
		}
	}
}
