package compendium

import (
	"fmt"

	"farmgame/internal/cropcatalog"
)

// DefaultCatalogVersionV1 is the catalog version of the built-in P0 adapter.
const DefaultCatalogVersionV1 uint64 = 1

// DefaultCatalogV1 maps the current cropcatalog definitions to the minimal P0
// compendium catalog. Display and silhouette IDs are deterministic, versioned
// resource keys derived from the stable crop definition ID.
func DefaultCatalogV1() CatalogSnapshot {
	definitions := cropcatalog.All()
	entries := make([]PlantCatalogEntry, 0, len(definitions))
	for _, definition := range definitions {
		resourceKey := fmt.Sprintf("crop-%d-v%d", definition.ID, definition.DefinitionVersion)
		entries = append(entries, PlantCatalogEntry{
			CropDefinitionID:    uint32(definition.ID),
			DefinitionVersion:   definition.DefinitionVersion,
			DisplayDefinitionID: resourceKey,
			Category:            EntryCategoryNormal,
			SilhouetteAssetID:   resourceKey + "-silhouette",
			DetailAssetID:       resourceKey + "-detail",
			LegacySeedPlantable: definition.LegacySeedPlantable,
		})
	}
	return CatalogSnapshot{
		CatalogVersion: DefaultCatalogVersionV1,
		Entries:        entries,
	}
}

// BuildDefaultCatalogV1 returns the built-in catalog after applying the same
// publication gate used by providers.
func BuildDefaultCatalogV1() (CatalogSnapshot, error) {
	catalog := DefaultCatalogV1()
	if err := ValidateCatalog(catalog, DefaultLimitsV1()); err != nil {
		return CatalogSnapshot{}, err
	}
	return catalog, nil
}
