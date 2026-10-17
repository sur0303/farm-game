package compendium

import (
	"fmt"
	"strconv"
	"strings"
)

// CategoryKind distinguishes the three page groups a client must render
// differently (docs/design/37 §5.2, task freeze: 普通目录 / 活动中 /
// 活动档案). The gray silhouette is a per-entry DisplayState, orthogonal to
// the group.
type CategoryKind int32

const (
	CategoryKindUnspecified CategoryKind = iota
	CategoryKindNormal
	CategoryKindActivityActive
	CategoryKindActivityArchive
	// CategoryKindCatalogMissing holds safe placeholders for permanent
	// unlock facts whose crop_definition_id is absent from the current
	// catalog (docs/design/37 §4.2). Facts are never degraded to locked,
	// hidden or deleted.
	CategoryKindCatalogMissing
)

func (k CategoryKind) String() string {
	switch k {
	case CategoryKindNormal:
		return "NORMAL"
	case CategoryKindActivityActive:
		return "ACTIVITY_ACTIVE"
	case CategoryKindActivityArchive:
		return "ACTIVITY_ARCHIVE"
	case CategoryKindCatalogMissing:
		return "CATALOG_MISSING"
	default:
		return "UNSPECIFIED"
	}
}

// PlaceholderDisplayDefinitionID is the safe placeholder resource reference
// returned for unlocked facts missing from the current catalog
// (docs/design/37 §4.2). The integrator maps it to a versioned static
// resource; it is never a real plant definition.
const PlaceholderDisplayDefinitionID = "catalog_missing_placeholder"

// PageEntry is one rendered compendium entry (docs/design/37 §5.2,
// docs/design/28 §5.9).
type PageEntry struct {
	CropDefinitionID uint32
	DisplayState     DisplayState
	// DisplayDefinitionID is set iff DisplayState == UNLOCKED_FULL; a locked
	// silhouette never exposes the full detail of an activity plant.
	DisplayDefinitionID string
	SilhouetteAssetID   string
	// ActivityID is set for activity and archive entries, empty for normal.
	ActivityID string
	// LegacySeedPlantable mirrors the catalog flag so the client can offer
	// planting leftover seeds after the activity archived
	// (docs/design/28 §5.9).
	LegacySeedPlantable bool
}

// PageCategory is one display group of the page.
type PageCategory struct {
	Kind CategoryKind
	// CategoryID is "normal" for the normal group, the activity_id for
	// active and archive groups, and "catalog_missing" for placeholders.
	CategoryID string
	// Title is the archive_title_snapshot for archives; active activities
	// have no frozen title in the P0 catalog model and use their
	// activity_id as a stable placeholder.
	Title   string
	Entries []PageEntry
}

// PageView is the deterministic read result assembled outside the Actor Turn
// from an authoritative State snapshot and an immutable CatalogSnapshot
// (docs/design/37 §5.1/§5.2). It carries no mutable references to either
// input.
type PageView struct {
	CatalogVersion uint64
	Revision       uint64
	SeenRevision   uint64
	HasNewUnlock   bool
	Categories     []PageCategory
	// NextArchiveCursor is the opaque stable cursor (archive_order +
	// activity_id) of the first archive NOT included in this page; empty
	// means all archives were returned (docs/design/37 §5.4).
	NextArchiveCursor string
	// CatalogMissingCropIDs lists, sorted, the permanent unlock facts with
	// no catalog entry. The integrator must raise a catalog_missing alert
	// for these; the domain itself only reports them.
	CatalogMissingCropIDs []uint32
}

const normalCategoryID = "normal"
const catalogMissingCategoryID = "catalog_missing"

// BuildPageView merges state with catalog into the deterministic page model
// (docs/design/37 §5.2/§5.4). It is a pure read: state and catalog are never
// modified, and the seen watermark is never touched (GET_COMPENDIUM must not
// implicitly clear the red dot).
//
// Fail-closed rules:
//   - the catalog is re-validated against the LimitProfile on every build;
//     an invalid snapshot fails the query (client retries) instead of
//     rendering a corrupted page;
//   - normal entries, active-activity groups and catalog-missing
//     placeholders must always fit the page limits — the publication gate
//     guarantees this, so a runtime overflow is REJECTED, never silently
//     truncated;
//   - activity archives paginate by stable (archive_order, activity_id)
//     cursor against the remaining item/byte budget; an archive that can
//     never fit alone is REJECTED rather than split mid-group.
//
// archiveCursor is empty for the first page; otherwise it must name an
// existing archive to resume from (inclusive).
func BuildPageView(state State, catalog CatalogSnapshot, limits LimitProfile, archiveCursor string) (PageView, *Error) {
	if err := ValidateCatalog(catalog, limits); err != nil {
		return PageView{}, err
	}

	view := PageView{
		CatalogVersion: catalog.CatalogVersion,
		Revision:       state.Revision,
		SeenRevision:   state.SeenRevision,
		HasNewUnlock:   state.HasNewUnlock(),
	}

	entryOf := func(e PlantCatalogEntry) PageEntry {
		out := PageEntry{
			CropDefinitionID:    e.CropDefinitionID,
			SilhouetteAssetID:   e.SilhouetteAssetID,
			LegacySeedPlantable: e.LegacySeedPlantable,
		}
		if e.Category == EntryCategoryActivity {
			out.ActivityID = e.ActivityID
		}
		if state.IsUnlocked(e.CropDefinitionID) {
			out.DisplayState = DisplayStateUnlockedFull
			out.DisplayDefinitionID = e.DisplayDefinitionID
		} else {
			out.DisplayState = DisplayStateLockedSilhouette
		}
		return out
	}

	var entriesCount uint32
	bytesUsed := uint64(pageFixedBytes)
	overBudget := func(categoryExtra bool, entries uint32) bool {
		nextBytes := bytesUsed + uint64(entries)*pageEntryBytes
		if categoryExtra {
			nextBytes += pageCategoryBytes
		}
		nextCount := entriesCount + entries
		return nextCount > limits.MaxCompendiumPageEntries ||
			nextBytes > uint64(limits.MaxCompendiumPageBytes)
	}
	commit := func(categoryExtra bool, entries uint32) {
		entriesCount += entries
		bytesUsed += uint64(entries) * pageEntryBytes
		if categoryExtra {
			bytesUsed += pageCategoryBytes
		}
	}

	// 1. Normal group, in catalog (publication-controlled) order.
	normal := PageCategory{Kind: CategoryKindNormal, CategoryID: normalCategoryID, Title: normalCategoryID}
	for _, e := range catalog.Entries {
		if e.Category == EntryCategoryNormal {
			normal.Entries = append(normal.Entries, entryOf(e))
		}
	}
	if overBudget(true, uint32(len(normal.Entries))) {
		return PageView{}, reject(ErrWorkCapacityExceeded,
			"normal group of %d entries exceeds page limits %d/%d",
			len(normal.Entries), limits.MaxCompendiumPageEntries, limits.MaxCompendiumPageBytes)
	}
	commit(true, uint32(len(normal.Entries)))
	view.Categories = append(view.Categories, normal)

	// 2. Active-activity groups, in first-appearance order of the catalog.
	activeIdx := map[string]int{}
	for _, e := range catalog.Entries {
		if e.Category != EntryCategoryActivity || e.ActivityState != ActivityStateActive {
			continue
		}
		idx, ok := activeIdx[e.ActivityID]
		if !ok {
			if overBudget(true, 0) {
				return PageView{}, reject(ErrWorkCapacityExceeded,
					"active activity %q exceeds page limits", e.ActivityID)
			}
			commit(true, 0)
			view.Categories = append(view.Categories, PageCategory{
				Kind:       CategoryKindActivityActive,
				CategoryID: e.ActivityID,
				Title:      e.ActivityID,
			})
			idx = len(view.Categories) - 1
			activeIdx[e.ActivityID] = idx
		}
		if overBudget(false, 1) {
			return PageView{}, reject(ErrWorkCapacityExceeded,
				"active activity %q exceeds page limits", e.ActivityID)
		}
		commit(false, 1)
		view.Categories[idx].Entries = append(view.Categories[idx].Entries, entryOf(e))
	}

	// 3. Catalog-missing placeholders for permanent facts the current
	// catalog no longer defines. Facts survive; display degrades to a safe
	// placeholder; the integrator raises catalog_missing (docs/design/37
	// §4.2).
	missing := PageCategory{
		Kind:       CategoryKindCatalogMissing,
		CategoryID: catalogMissingCategoryID,
		Title:      catalogMissingCategoryID,
	}
	for _, f := range state.Facts { // Facts is sorted by crop_definition_id.
		if _, ok := catalog.FindEntry(f.CropDefinitionID); ok {
			continue
		}
		view.CatalogMissingCropIDs = append(view.CatalogMissingCropIDs, f.CropDefinitionID)
		missing.Entries = append(missing.Entries, PageEntry{
			CropDefinitionID:    f.CropDefinitionID,
			DisplayState:        DisplayStateUnlockedFull,
			DisplayDefinitionID: PlaceholderDisplayDefinitionID,
			SilhouetteAssetID:   PlaceholderDisplayDefinitionID,
			ActivityID:          f.SourceActivityID,
		})
	}
	if len(missing.Entries) > 0 {
		if overBudget(true, uint32(len(missing.Entries))) {
			return PageView{}, reject(ErrWorkCapacityExceeded,
				"%d catalog-missing placeholders exceed page limits", len(missing.Entries))
		}
		commit(true, uint32(len(missing.Entries)))
		view.Categories = append(view.Categories, missing)
	}

	// 4. Activity archives in stable (archive_order, activity_id) order,
	// paginated by cursor against the remaining budget (docs/design/37
	// §5.4). No silent truncation: the cursor names the first archive not
	// on this page.
	archives := sortedArchives(catalog.Archives)
	start := 0
	if archiveCursor != "" {
		order, id, err := parseArchiveCursor(archiveCursor)
		if err != nil {
			return PageView{}, err
		}
		start = -1
		for i, a := range archives {
			if a.ArchiveOrder == order && a.ActivityID == id {
				start = i
				break
			}
		}
		if start < 0 {
			return PageView{}, reject(ErrInvalidArgument,
				"archive cursor %q does not match catalog_version %d", archiveCursor, catalog.CatalogVersion)
		}
	}
	for i := start; i < len(archives); i++ {
		a := archives[i]
		n := uint32(len(a.CropDefinitionIDs))
		if overBudget(true, n) {
			if i == start {
				// A single archive can never fit the page budget: a
				// publication-gate failure. Fail closed instead of
				// emitting a cursor the client can never advance past.
				return PageView{}, reject(ErrWorkCapacityExceeded,
					"archive %q with %d entries alone exceeds page limits %d/%d",
					a.ActivityID, n, limits.MaxCompendiumPageEntries, limits.MaxCompendiumPageBytes)
			}
			view.NextArchiveCursor = formatArchiveCursor(a)
			break
		}
		commit(true, n)
		cat := PageCategory{
			Kind:       CategoryKindActivityArchive,
			CategoryID: a.ActivityID,
			Title:      a.ArchiveTitleSnapshot,
		}
		for _, id := range a.CropDefinitionIDs {
			e, _ := catalog.FindEntry(id) // existence proven by ValidateCatalog
			cat.Entries = append(cat.Entries, entryOf(e))
		}
		view.Categories = append(view.Categories, cat)
	}

	return view, nil
}

// sortedArchives returns archives in stable (archive_order, activity_id)
// order without mutating the input slice. ValidateCatalog has already
// rejected duplicate archive_order values, so the order is total.
func sortedArchives(in []ActivityArchiveDefinition) []ActivityArchiveDefinition {
	out := make([]ActivityArchiveDefinition, len(in))
	copy(out, in)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && lessArchive(out[j], out[j-1]); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func lessArchive(a, b ActivityArchiveDefinition) bool {
	if a.ArchiveOrder != b.ArchiveOrder {
		return a.ArchiveOrder < b.ArchiveOrder
	}
	return a.ActivityID < b.ActivityID
}

// formatArchiveCursor encodes the stable cursor (docs/design/37 §5.4:
// archive_order + activity_id).
func formatArchiveCursor(a ActivityArchiveDefinition) string {
	return fmt.Sprintf("%010d|%s", a.ArchiveOrder, a.ActivityID)
}

func parseArchiveCursor(cursor string) (uint32, string, *Error) {
	sep := strings.IndexByte(cursor, '|')
	if sep <= 0 || sep == len(cursor)-1 {
		return 0, "", reject(ErrInvalidArgument, "malformed archive cursor %q", cursor)
	}
	order, err := strconv.ParseUint(cursor[:sep], 10, 32)
	if err != nil {
		return 0, "", reject(ErrInvalidArgument, "malformed archive cursor %q", cursor)
	}
	return uint32(order), cursor[sep+1:], nil
}
