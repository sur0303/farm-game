package compendium

import (
	"fmt"
	"sync"
	"testing"

	"farmgame/internal/cropcatalog"
)

func TestCatalogProviderSnapshotsAreDeepCopies(t *testing.T) {
	input := validCatalog()
	provider, err := NewStaticCatalogProvider(input, testLimits())
	if err != nil {
		t.Fatal(err)
	}

	input.Entries[0].DisplayDefinitionID = "mutated-input"
	input.Archives[0].CropDefinitionIDs[0] = 999

	first, err := provider.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	first.Entries[0].DisplayDefinitionID = "mutated-reader"
	first.Archives[0].CropDefinitionIDs[0] = 998

	second, err := provider.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if second.Entries[0].DisplayDefinitionID != "display_normal" {
		t.Fatalf("input or reader mutation leaked into provider: %+v", second.Entries[0])
	}
	if second.Archives[0].CropDefinitionIDs[0] != 3 {
		t.Fatalf("archive membership was not detached: %+v", second.Archives[0])
	}
}

func TestAtomicCatalogProviderConcurrentPublishAndRead(t *testing.T) {
	first := validCatalog()
	second := validCatalog()
	second.CatalogVersion = 2
	for i := range second.Entries {
		second.Entries[i].DisplayDefinitionID = fmt.Sprintf("display-v2-%d", second.Entries[i].CropDefinitionID)
	}

	provider, err := NewAtomicCatalogProvider(first, testLimits())
	if err != nil {
		t.Fatal(err)
	}

	const readers = 8
	const iterations = 500
	errs := make(chan error, readers)
	var wg sync.WaitGroup
	wg.Add(readers + 1)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				snapshot, err := provider.Snapshot()
				if err != nil {
					errs <- err
					return
				}
				if err := ValidateCatalog(snapshot, testLimits()); err != nil {
					errs <- fmt.Errorf("reader observed invalid snapshot: %w", err)
					return
				}
				switch snapshot.CatalogVersion {
				case 1:
					if snapshot.Entries[0].DisplayDefinitionID != "display_normal" {
						errs <- fmt.Errorf("reader observed mixed version 1: %+v", snapshot.Entries[0])
						return
					}
				case 2:
					if snapshot.Entries[0].DisplayDefinitionID != "display-v2-1" {
						errs <- fmt.Errorf("reader observed mixed version 2: %+v", snapshot.Entries[0])
						return
					}
				default:
					errs <- fmt.Errorf("reader observed unknown catalog version %d", snapshot.CatalogVersion)
					return
				}
			}
		}()
	}
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			if err := provider.Publish(second); err != nil {
				errs <- err
				return
			}
			if err := provider.Publish(first); err != nil {
				errs <- err
				return
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestAtomicCatalogProviderRejectsInvalidPublishAndKeepsOldVersion(t *testing.T) {
	old := validCatalog()
	provider, err := NewAtomicCatalogProvider(old, testLimits())
	if err != nil {
		t.Fatal(err)
	}

	bad := old.Clone()
	bad.CatalogVersion = 2
	bad.Entries[0].CropDefinitionID = 0
	if err := provider.Publish(bad); err == nil {
		t.Fatal("invalid catalog publish unexpectedly succeeded")
	}

	got, err := provider.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got.CatalogVersion != old.CatalogVersion || got.Entries[0].CropDefinitionID != old.Entries[0].CropDefinitionID {
		t.Fatalf("old catalog was not retained after rejected publish: %+v", got)
	}
}

func TestDefaultCatalogV1MapsAllCropDefinitions(t *testing.T) {
	definitions := cropcatalog.All()
	catalog, err := BuildDefaultCatalogV1()
	if err != nil {
		t.Fatal(err)
	}
	if catalog.CatalogVersion != DefaultCatalogVersionV1 {
		t.Fatalf("catalog version=%d want %d", catalog.CatalogVersion, DefaultCatalogVersionV1)
	}
	if len(catalog.Entries) != len(definitions) {
		t.Fatalf("entry count=%d want %d", len(catalog.Entries), len(definitions))
	}
	for _, definition := range definitions {
		entry, ok := catalog.FindEntry(uint32(definition.ID))
		if !ok {
			t.Fatalf("missing crop definition %d", definition.ID)
		}
		if entry.DefinitionVersion != definition.DefinitionVersion ||
			entry.Category != EntryCategoryNormal ||
			entry.ActivityID != "" ||
			entry.ActivityState != ActivityStateUnspecified ||
			entry.DisplayDefinitionID == "" ||
			entry.SilhouetteAssetID == "" ||
			entry.DetailAssetID == "" ||
			entry.LegacySeedPlantable != definition.LegacySeedPlantable {
			t.Fatalf("incomplete mapping for crop %d: %+v", definition.ID, entry)
		}
	}
}

func TestDefaultCatalogV1BuildsPageView(t *testing.T) {
	view, err := BuildPageView(NewState(), DefaultCatalogV1(), DefaultLimitsV1(), "")
	if err != nil {
		t.Fatal(err)
	}
	if view.CatalogVersion != DefaultCatalogVersionV1 || len(view.Categories) != 1 {
		t.Fatalf("unexpected page header/categories: %+v", view)
	}
	category := view.Categories[0]
	if category.Kind != CategoryKindNormal || len(category.Entries) != len(cropcatalog.All()) {
		t.Fatalf("unexpected default page category: %+v", category)
	}
	for _, entry := range category.Entries {
		if entry.DisplayState != DisplayStateLockedSilhouette ||
			entry.DisplayDefinitionID != "" ||
			entry.SilhouetteAssetID == "" {
			t.Fatalf("unexpected locked default entry: %+v", entry)
		}
	}
}
