package compendium

// PlantCatalogEntry is one versioned plant definition of the read-side
// catalog (docs/design/37 §4.1). The catalog is display configuration only:
// it never changes authoritative unlock facts, and an entry going missing
// must degrade to a safe placeholder, never to "locked" or deletion
// (docs/design/37 §4.2).
type PlantCatalogEntry struct {
	CropDefinitionID uint32
	// DefinitionVersion is the per-plant definition version; it is not
	// interchangeable with catalog_version or archive_version.
	DefinitionVersion uint64
	// DisplayDefinitionID identifies display resources only; it never
	// participates in unlock identity.
	DisplayDefinitionID string
	Category            EntryCategory
	// ActivityID is set iff Category == EntryCategoryActivity.
	ActivityID    string
	ActivityState ActivityState
	// SilhouetteAssetID is the gray-silhouette resource shown for locked
	// entries; versioned so CDN GC can never rewrite history.
	SilhouetteAssetID string
	// DetailAssetID is the full-detail resource shown for unlocked entries.
	DetailAssetID string
	// LegacySeedPlantable allows planting leftover seeds after the activity
	// archived (docs/design/37 §4.2); activity end alone never vetoes it.
	LegacySeedPlantable bool
}

// ActivityArchiveDefinition is the immutable archive formed when an activity
// enters ARCHIVED (docs/design/37 §4.1). Its historical display meaning is
// never rewritten in place; retention is global (the archive stays within
// the product display range), never computed by scanning players.
type ActivityArchiveDefinition struct {
	ActivityID string
	// ArchiveVersion is independent of catalog_version and
	// definition_version.
	ArchiveVersion uint64
	// CropDefinitionIDs is the stable membership of the archive. The same
	// crop may appear in several archives; one permanent unlock fact shows
	// it unlocked in all of them (docs/design/37 §2).
	CropDefinitionIDs    []uint32
	ArchiveTitleSnapshot string
	// ArchiveOrder is the stable pagination/display order key
	// (docs/design/37 §5.4: cursor is archive_order + activity_id).
	ArchiveOrder uint32
	EndAtMs      int64
}

// CatalogSnapshot is the immutable, versioned plant catalog captured by the
// read ingress from local Watch state (docs/design/37 §4.1/§5.1). It is
// assembled and validated outside any Actor Turn; the domain view builder
// treats it as read-only.
type CatalogSnapshot struct {
	CatalogVersion uint64
	Entries        []PlantCatalogEntry
	Archives       []ActivityArchiveDefinition
}

// FindEntry locates an entry by crop definition ID.
func (c CatalogSnapshot) FindEntry(cropDefinitionID uint32) (PlantCatalogEntry, bool) {
	for _, e := range c.Entries {
		if e.CropDefinitionID == cropDefinitionID {
			return e, true
		}
	}
	return PlantCatalogEntry{}, false
}

// Clone returns a deep copy that shares no mutable memory with c.
func (c CatalogSnapshot) Clone() CatalogSnapshot {
	out := c
	out.Entries = append([]PlantCatalogEntry(nil), c.Entries...)
	out.Archives = make([]ActivityArchiveDefinition, len(c.Archives))
	for i, a := range c.Archives {
		out.Archives[i] = a
		out.Archives[i].CropDefinitionIDs = append([]uint32(nil), a.CropDefinitionIDs...)
	}
	return out
}

// ValidateCatalog enforces the publication-time invariants of
// docs/design/37 §4.1/§5.4. It fails closed: an invalid or oversized catalog
// must be rejected before release so a live Harvest never silently skips a
// permanent unlock and a page is never silently truncated.
//
// Checks:
//   - catalog fits the active LimitProfile (entries, archives);
//   - crop_definition_id unique and non-zero;
//   - NORMAL entries carry no activity fields; ACTIVITY entries carry a
//     non-empty activity_id and a well-formed activity_state;
//   - archived ACTIVITY entries have a matching archive definition whose
//     membership lists them; archive definitions are unique, sorted-cursor
//     safe (archive_order + activity_id distinct) and reference known crops;
//   - unlocked-relevant caps: the number of collectible entries does not
//     exceed max_unlocked_entries.
func ValidateCatalog(c CatalogSnapshot, limits LimitProfile) *Error {
	if err := limits.Validate(); err != nil {
		return err
	}
	if c.CatalogVersion == 0 {
		return reject(ErrInvalidArgument, "catalog_version must be > 0")
	}
	if uint32(len(c.Entries)) > limits.MaxCatalogEntries {
		return reject(ErrWorkCapacityExceeded,
			"catalog has %d entries, limit %d", len(c.Entries), limits.MaxCatalogEntries)
	}
	if uint32(len(c.Archives)) > limits.MaxActivityArchives {
		return reject(ErrWorkCapacityExceeded,
			"catalog has %d archives, limit %d", len(c.Archives), limits.MaxActivityArchives)
	}

	seen := make(map[uint32]struct{}, len(c.Entries))
	for _, e := range c.Entries {
		if e.CropDefinitionID == 0 {
			return reject(ErrInvalidArgument, "catalog entry with zero crop_definition_id")
		}
		if _, dup := seen[e.CropDefinitionID]; dup {
			return reject(ErrInvalidArgument, "duplicate crop_definition_id %d", e.CropDefinitionID)
		}
		seen[e.CropDefinitionID] = struct{}{}
		switch e.Category {
		case EntryCategoryNormal:
			if e.ActivityID != "" || e.ActivityState != ActivityStateUnspecified {
				return reject(ErrInvalidArgument,
					"normal entry %d carries activity fields", e.CropDefinitionID)
			}
		case EntryCategoryActivity:
			if e.ActivityID == "" {
				return reject(ErrInvalidArgument,
					"activity entry %d has empty activity_id", e.CropDefinitionID)
			}
			if e.ActivityState != ActivityStateActive && e.ActivityState != ActivityStateArchived {
				return reject(ErrInvalidArgument,
					"activity entry %d has invalid activity_state %s", e.CropDefinitionID, e.ActivityState)
			}
		default:
			return reject(ErrInvalidArgument,
				"entry %d has invalid category %s", e.CropDefinitionID, e.Category)
		}
	}
	// Every catalog entry is collectible; the permanent membership of one
	// player can therefore never exceed max_unlocked_entries if the profile
	// is enforced here.
	if uint32(len(c.Entries)) > limits.MaxUnlockedEntries {
		return reject(ErrWorkCapacityExceeded,
			"catalog has %d collectible entries, limit %d", len(c.Entries), limits.MaxUnlockedEntries)
	}

	archiveSeen := make(map[string]struct{}, len(c.Archives))
	orderSeen := make(map[uint32]string, len(c.Archives))
	for _, a := range c.Archives {
		if a.ActivityID == "" {
			return reject(ErrInvalidArgument, "archive with empty activity_id")
		}
		if _, dup := archiveSeen[a.ActivityID]; dup {
			return reject(ErrInvalidArgument, "duplicate archive %q", a.ActivityID)
		}
		archiveSeen[a.ActivityID] = struct{}{}
		if prev, dup := orderSeen[a.ArchiveOrder]; dup {
			return reject(ErrInvalidArgument,
				"archives %q and %q share archive_order %d; cursor (archive_order, activity_id) must be total",
				prev, a.ActivityID, a.ArchiveOrder)
		}
		orderSeen[a.ArchiveOrder] = a.ActivityID
		memberSeen := make(map[uint32]struct{}, len(a.CropDefinitionIDs))
		for _, id := range a.CropDefinitionIDs {
			if _, dup := memberSeen[id]; dup {
				return reject(ErrInvalidArgument, "archive %q lists crop %d twice", a.ActivityID, id)
			}
			memberSeen[id] = struct{}{}
			if _, ok := seen[id]; !ok {
				return reject(ErrInvalidArgument,
					"archive %q references unknown crop_definition_id %d", a.ActivityID, id)
			}
		}
	}

	// Archived activity entries must be covered by their archive definition
	// so the page can group them deterministically.
	for _, e := range c.Entries {
		if e.Category != EntryCategoryActivity || e.ActivityState != ActivityStateArchived {
			continue
		}
		archiveIdx := -1
		for i := range c.Archives {
			if c.Archives[i].ActivityID == e.ActivityID {
				archiveIdx = i
				break
			}
		}
		if archiveIdx < 0 {
			return reject(ErrInvalidArgument,
				"archived entry %d has no archive definition for activity %q",
				e.CropDefinitionID, e.ActivityID)
		}
		listed := false
		for _, id := range c.Archives[archiveIdx].CropDefinitionIDs {
			if id == e.CropDefinitionID {
				listed = true
				break
			}
		}
		if !listed {
			return reject(ErrInvalidArgument,
				"archive %q does not list archived entry %d", e.ActivityID, e.CropDefinitionID)
		}
	}
	return nil
}
