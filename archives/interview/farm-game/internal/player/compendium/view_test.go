package compendium

import (
	"fmt"
	"reflect"
	"testing"
)

// findCategory locates a page group by kind + id.
func findCategory(v PageView, kind CategoryKind, id string) *PageCategory {
	for i := range v.Categories {
		if v.Categories[i].Kind == kind && v.Categories[i].CategoryID == id {
			return &v.Categories[i]
		}
	}
	return nil
}

// TestBuildPageView_ActivityLifecycleMatrix covers the four combinations of
// activity end (docs/design/37 §1.1.3-5, acceptance 5):
//
//	active   + unlocked → full display in the active group
//	active   + locked   → gray silhouette in the active group
//	archived + unlocked → full display in the activity archive, forever
//	archived + locked   → gray silhouette in the activity archive
func TestBuildPageView_ActivityLifecycleMatrix(t *testing.T) {
	catalog := CatalogSnapshot{
		CatalogVersion: 9,
		Entries: []PlantCatalogEntry{
			activityEntry(1, "fest_a", ActivityStateActive, false),
			activityEntry(2, "fest_a", ActivityStateActive, false),
			activityEntry(3, "fest_b", ActivityStateArchived, true),
			activityEntry(4, "fest_b", ActivityStateArchived, true),
		},
		Archives: []ActivityArchiveDefinition{{
			ActivityID:           "fest_b",
			ArchiveVersion:       2,
			CropDefinitionIDs:    []uint32{3, 4},
			ArchiveTitleSnapshot: "春节活动",
			ArchiveOrder:         1,
		}},
	}
	state := NewState()
	state = unlockOne(t, state, 42, 1, "fest_a")
	state = unlockOne(t, state, 42, 3, "fest_b")

	view, err := BuildPageView(state, catalog, testLimits(), "")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if view.CatalogVersion != 9 || view.Revision != 2 || !view.HasNewUnlock {
		t.Fatalf("view head = %+v", view)
	}

	active := findCategory(view, CategoryKindActivityActive, "fest_a")
	if active == nil || len(active.Entries) != 2 {
		t.Fatalf("active group = %+v", active)
	}
	if active.Entries[0].DisplayState != DisplayStateUnlockedFull ||
		active.Entries[0].DisplayDefinitionID == "" {
		t.Fatalf("active unlocked = %+v", active.Entries[0])
	}
	if active.Entries[1].DisplayState != DisplayStateLockedSilhouette ||
		active.Entries[1].DisplayDefinitionID != "" ||
		active.Entries[1].SilhouetteAssetID == "" {
		t.Fatalf("active locked must be a bare silhouette: %+v", active.Entries[1])
	}

	archive := findCategory(view, CategoryKindActivityArchive, "fest_b")
	if archive == nil || archive.Title != "春节活动" || len(archive.Entries) != 2 {
		t.Fatalf("archive group = %+v", archive)
	}
	if archive.Entries[0].DisplayState != DisplayStateUnlockedFull ||
		archive.Entries[0].DisplayDefinitionID != "display_fest_b" {
		t.Fatalf("archived unlocked = %+v", archive.Entries[0])
	}
	if archive.Entries[1].DisplayState != DisplayStateLockedSilhouette ||
		archive.Entries[1].DisplayDefinitionID != "" {
		t.Fatalf("archived locked = %+v", archive.Entries[1])
	}
	// legacy_seed_plantable is mirrored for the client (28 §5.9).
	if !archive.Entries[0].LegacySeedPlantable {
		t.Fatal("legacy_seed_plantable flag lost")
	}
	if view.NextArchiveCursor != "" {
		t.Fatalf("unexpected cursor %q", view.NextArchiveCursor)
	}
	if len(view.CatalogMissingCropIDs) != 0 {
		t.Fatalf("missing = %v", view.CatalogMissingCropIDs)
	}
}

// TestBuildPageView_LegacySeedEndToEnd: archived activity + leftover seed →
// plant still allowed (flag), self-harvest unlocks, archive then shows the
// full entry (acceptance 6/13 domain slice).
func TestBuildPageView_LegacySeedEndToEnd(t *testing.T) {
	catalog := CatalogSnapshot{
		CatalogVersion: 2,
		Entries: []PlantCatalogEntry{
			activityEntry(7, "fest", ActivityStateArchived, true),
		},
		Archives: []ActivityArchiveDefinition{{
			ActivityID: "fest", ArchiveVersion: 1,
			CropDefinitionIDs: []uint32{7}, ArchiveOrder: 1,
		}},
	}
	// Activity already archived when the legacy seed is planted; the frozen
	// snapshot still carries unlock_eligible=true (design 37 §3.1).
	out := DecideUnlock(NewState(), ownHarvestCmd(42, 7, snap(7, true, 42, "fest")), testLimits())
	if out.Status != StatusSucceeded {
		t.Fatalf("legacy harvest unlock = %s", out.Status)
	}
	view, err := BuildPageView(out.Candidate, catalog, testLimits(), "")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	archive := findCategory(view, CategoryKindActivityArchive, "fest")
	if archive == nil || archive.Entries[0].DisplayState != DisplayStateUnlockedFull {
		t.Fatalf("archive after legacy unlock = %+v", archive)
	}
}

// TestBuildPageView_CatalogMissingKeepsFacts: an unlocked crop whose
// definition left the catalog stays UNLOCKED with a safe placeholder and is
// reported for the catalog_missing alert — never degraded, hidden or
// deleted (docs/design/37 §4.2, acceptance 7/15).
func TestBuildPageView_CatalogMissingKeepsFacts(t *testing.T) {
	state := NewState()
	state = unlockOne(t, state, 42, 5, "")
	state = unlockOne(t, state, 42, 9, "old_fest")
	catalog := CatalogSnapshot{
		CatalogVersion: 3,
		Entries:        []PlantCatalogEntry{normalEntry(5)}, // crop 9 delisted
	}
	view, err := BuildPageView(state, catalog, testLimits(), "")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !reflect.DeepEqual(view.CatalogMissingCropIDs, []uint32{9}) {
		t.Fatalf("missing = %v", view.CatalogMissingCropIDs)
	}
	missing := findCategory(view, CategoryKindCatalogMissing, catalogMissingCategoryID)
	if missing == nil || len(missing.Entries) != 1 {
		t.Fatalf("placeholder group = %+v", missing)
	}
	ph := missing.Entries[0]
	if ph.DisplayState != DisplayStateUnlockedFull ||
		ph.DisplayDefinitionID != PlaceholderDisplayDefinitionID ||
		ph.ActivityID != "old_fest" {
		t.Fatalf("placeholder = %+v", ph)
	}
	// The normal entry is untouched and the authoritative fact survives.
	if !state.IsUnlocked(9) {
		t.Fatal("view build mutated state")
	}
	normal := findCategory(view, CategoryKindNormal, normalCategoryID)
	if normal == nil || normal.Entries[0].DisplayState != DisplayStateUnlockedFull {
		t.Fatalf("normal group = %+v", normal)
	}
}

// TestBuildPageView_CropReusedAcrossArchives: one permanent unlock fact
// shows the crop unlocked in every archive that references it
// (docs/design/37 §2, acceptance 16).
func TestBuildPageView_CropReusedAcrossArchives(t *testing.T) {
	catalog := CatalogSnapshot{
		CatalogVersion: 1,
		Entries: []PlantCatalogEntry{
			activityEntry(8, "fest_a", ActivityStateArchived, false),
			activityEntry(9, "fest_b", ActivityStateArchived, false),
		},
		Archives: []ActivityArchiveDefinition{
			{ActivityID: "fest_a", ArchiveVersion: 1, CropDefinitionIDs: []uint32{8}, ArchiveOrder: 1},
			{ActivityID: "fest_b", ArchiveVersion: 1, CropDefinitionIDs: []uint32{9, 8}, ArchiveOrder: 2},
		},
	}
	// fest_b reusing crop 8 requires a catalog entry for it; the archive
	// membership lists it alongside 9. The single unlock fact of 8 unlocks
	// both renderings.
	state := unlockOne(t, NewState(), 42, 8, "fest_a")
	view, err := BuildPageView(state, catalog, testLimits(), "")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	a := findCategory(view, CategoryKindActivityArchive, "fest_a")
	b := findCategory(view, CategoryKindActivityArchive, "fest_b")
	if a == nil || b == nil {
		t.Fatalf("archives = %+v", view.Categories)
	}
	if a.Entries[0].DisplayState != DisplayStateUnlockedFull {
		t.Fatal("fest_a rendering locked")
	}
	var reused *PageEntry
	for i := range b.Entries {
		if b.Entries[i].CropDefinitionID == 8 {
			reused = &b.Entries[i]
		}
	}
	if reused == nil || reused.DisplayState != DisplayStateUnlockedFull {
		t.Fatalf("fest_b reuse of crop 8 = %+v", b.Entries)
	}
}

// TestBuildPageView_ArchivePagination: archives paginate by the stable
// (archive_order, activity_id) cursor without silent truncation
// (docs/design/37 §5.4, acceptance 18).
func TestBuildPageView_ArchivePagination(t *testing.T) {
	mkCatalog := func() CatalogSnapshot {
		c := CatalogSnapshot{CatalogVersion: 1}
		for i := uint32(1); i <= 4; i++ {
			id := fmt.Sprintf("fest_%d", i)
			crop := 100 + i
			c.Entries = append(c.Entries, activityEntry(crop, id, ActivityStateArchived, false))
			c.Archives = append(c.Archives, ActivityArchiveDefinition{
				ActivityID: id, ArchiveVersion: 1,
				CropDefinitionIDs: []uint32{crop}, ArchiveOrder: i,
			})
		}
		return c
	}
	limits := testLimits()
	limits.MaxCompendiumPageEntries = 2 // one archive per page
	catalog := mkCatalog()

	var seen []string
	cursor := ""
	for page := 0; ; page++ {
		if page > 8 {
			t.Fatal("pagination did not terminate")
		}
		view, err := BuildPageView(NewState(), catalog, limits, cursor)
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		for _, cat := range view.Categories {
			if cat.Kind == CategoryKindActivityArchive {
				seen = append(seen, cat.CategoryID)
			}
		}
		if view.NextArchiveCursor == "" {
			break
		}
		cursor = view.NextArchiveCursor
	}
	want := []string{"fest_1", "fest_2", "fest_3", "fest_4"}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("pages delivered %v, want %v (no truncation, no dup)", seen, want)
	}
}

// TestBuildPageView_ArchiveOrderIsStable: archive display order follows
// (archive_order, activity_id), not the order of the config slice.
func TestBuildPageView_ArchiveOrderIsStable(t *testing.T) {
	catalog := CatalogSnapshot{
		CatalogVersion: 1,
		Entries: []PlantCatalogEntry{
			activityEntry(1, "b_fest", ActivityStateArchived, false),
			activityEntry(2, "a_fest", ActivityStateArchived, false),
		},
		Archives: []ActivityArchiveDefinition{
			{ActivityID: "b_fest", ArchiveVersion: 1, CropDefinitionIDs: []uint32{1}, ArchiveOrder: 2},
			{ActivityID: "a_fest", ArchiveVersion: 1, CropDefinitionIDs: []uint32{2}, ArchiveOrder: 1},
		},
	}
	view, err := BuildPageView(NewState(), catalog, testLimits(), "")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	var order []string
	for _, cat := range view.Categories {
		if cat.Kind == CategoryKindActivityArchive {
			order = append(order, cat.CategoryID)
		}
	}
	if !reflect.DeepEqual(order, []string{"a_fest", "b_fest"}) {
		t.Fatalf("archive order = %v", order)
	}
}

func TestBuildPageView_PaginationFailClosed(t *testing.T) {
	catalog := CatalogSnapshot{
		CatalogVersion: 1,
		Entries:        []PlantCatalogEntry{activityEntry(1, "fest", ActivityStateArchived, false)},
		Archives: []ActivityArchiveDefinition{
			{ActivityID: "fest", ArchiveVersion: 1, CropDefinitionIDs: []uint32{1}, ArchiveOrder: 1},
		},
	}
	t.Run("single archive too large", func(t *testing.T) {
		limits := testLimits()
		limits.MaxCompendiumPageEntries = 0 // invalid profile
		if _, err := BuildPageView(NewState(), catalog, limits, ""); err == nil {
			t.Fatal("invalid profile must fail")
		}
		limits = testLimits()
		limits.MaxCompendiumPageBytes = pageFixedBytes + pageCategoryBytes + 1 // header only
		if _, err := BuildPageView(NewState(), catalog, limits, ""); err == nil ||
			err.Code != ErrWorkCapacityExceeded {
			t.Fatalf("got %v, want WORK_CAPACITY_EXCEEDED (no infinite cursor loop)", err)
		}
	})
	t.Run("malformed cursor", func(t *testing.T) {
		if _, err := BuildPageView(NewState(), catalog, testLimits(), "garbage"); err == nil ||
			err.Code != ErrInvalidArgument {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("stale cursor from another catalog version", func(t *testing.T) {
		if _, err := BuildPageView(NewState(), catalog, testLimits(), "0000000009|ghost"); err == nil ||
			err.Code != ErrInvalidArgument {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("invalid catalog fails closed", func(t *testing.T) {
		broken := catalog.Clone()
		broken.Archives = nil
		if _, err := BuildPageView(NewState(), broken, testLimits(), ""); err == nil {
			t.Fatal("archived entry without archive def must fail the query")
		}
	})
}

// TestBuildPageView_ByteBudgetPagination: the byte budget paginates archives
// exactly like the item budget.
func TestBuildPageView_ByteBudgetPagination(t *testing.T) {
	catalog := CatalogSnapshot{
		CatalogVersion: 1,
		Entries: []PlantCatalogEntry{
			activityEntry(1, "f1", ActivityStateArchived, false),
			activityEntry(2, "f2", ActivityStateArchived, false),
		},
		Archives: []ActivityArchiveDefinition{
			{ActivityID: "f1", ArchiveVersion: 1, CropDefinitionIDs: []uint32{1}, ArchiveOrder: 1},
			{ActivityID: "f2", ArchiveVersion: 1, CropDefinitionIDs: []uint32{2}, ArchiveOrder: 2},
		},
	}
	limits := testLimits()
	// header + normal group + exactly one archive (category + 1 entry).
	limits.MaxCompendiumPageBytes = pageFixedBytes + 2*pageCategoryBytes + pageEntryBytes
	view, err := BuildPageView(NewState(), catalog, limits, "")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if view.NextArchiveCursor == "" {
		t.Fatal("byte budget must paginate the second archive")
	}
	view2, err := BuildPageView(NewState(), catalog, limits, view.NextArchiveCursor)
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if view2.NextArchiveCursor != "" {
		t.Fatalf("page 2 cursor = %q", view2.NextArchiveCursor)
	}
	last := view2.Categories[len(view2.Categories)-1]
	if last.Kind != CategoryKindActivityArchive || last.CategoryID != "f2" {
		t.Fatalf("page 2 = %+v", view2.Categories)
	}
}

// TestBuildPageView_Deterministic: identical inputs produce identical views,
// and building never mutates state, catalog or the seen watermark
// (GET_COMPENDIUM is a pure read, docs/design/37 §5.2).
func TestBuildPageView_Deterministic(t *testing.T) {
	catalog := validCatalog()
	state := NewState()
	state = unlockOne(t, state, 42, 1, "")
	state = unlockOne(t, state, 42, 3, "winter_fest")

	first, err := BuildPageView(state, catalog, testLimits(), "")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for i := 0; i < 20; i++ {
		again, err := BuildPageView(state, catalog, testLimits(), "")
		if err != nil {
			t.Fatalf("rebuild: %v", err)
		}
		if !reflect.DeepEqual(first, again) {
			t.Fatalf("run %d diverged", i)
		}
	}
	if state.SeenRevision != 0 || state.Revision != 2 {
		t.Fatal("GET must not touch the seen watermark or the revision")
	}
	if first.HasNewUnlock != state.HasNewUnlock() {
		t.Fatal("has_new_unlock must be derived, not stored")
	}
}
