package compendium

// LimitProfile is the versioned capacity contract every compendium state and
// page must satisfy (docs/design/37 §5.4, docs/design/31). The config
// publication gate must prove "existing catalog + new content" fits the
// active profile before release; the domain layer enforces the same profile
// at runtime and fails closed — it never silently drops an unlock, truncates
// a page or grows an unbounded collection.
type LimitProfile struct {
	ProfileVersion uint64
	// MaxCatalogEntries bounds PlantCatalogSnapshot.entries.
	MaxCatalogEntries uint32
	// MaxActivityArchives bounds PlantCatalogSnapshot.activity_archives.
	MaxActivityArchives uint32
	// MaxUnlockedEntries bounds the permanent membership set of one player.
	MaxUnlockedEntries uint32
	// MaxCompendiumStateBytes bounds the estimated serialized size of one
	// PlayerCompendiumState.
	MaxCompendiumStateBytes uint32
	// MaxCompendiumPageEntries bounds the entries of one page response.
	// Normal and active-activity entries are always returned in full (the
	// publication gate guarantees they fit); activity archives paginate
	// against this limit.
	MaxCompendiumPageEntries uint32
	// MaxCompendiumPageBytes bounds the estimated serialized size of one
	// page response; archives paginate against this limit too.
	MaxCompendiumPageBytes uint32
}

// Validate rejects a profile that cannot act as a fail-closed fence. All
// bounds must be positive; a zero bound would silently disable the
// corresponding capacity check.
func (p LimitProfile) Validate() *Error {
	if p.MaxCatalogEntries == 0 {
		return reject(ErrInvalidArgument, "limit profile %d: max_catalog_entries must be > 0", p.ProfileVersion)
	}
	if p.MaxActivityArchives == 0 {
		return reject(ErrInvalidArgument, "limit profile %d: max_activity_archives must be > 0", p.ProfileVersion)
	}
	if p.MaxUnlockedEntries == 0 {
		return reject(ErrInvalidArgument, "limit profile %d: max_unlocked_entries must be > 0", p.ProfileVersion)
	}
	if p.MaxCompendiumStateBytes == 0 {
		return reject(ErrInvalidArgument, "limit profile %d: max_compendium_state_bytes must be > 0", p.ProfileVersion)
	}
	if p.MaxCompendiumPageEntries == 0 {
		return reject(ErrInvalidArgument, "limit profile %d: max_compendium_page_entries must be > 0", p.ProfileVersion)
	}
	if p.MaxCompendiumPageBytes == 0 {
		return reject(ErrInvalidArgument, "limit profile %d: max_compendium_page_bytes must be > 0", p.ProfileVersion)
	}
	return nil
}

// Deterministic byte estimates. The exact physical encoding (bounded sorted
// array vs ordinal bitmap) is PHYSICAL_PENDING in docs/design/37 §5.4; these
// constants give the publication gate and the runtime fence a single shared,
// versioned estimate so both sides agree on when content stops fitting.
const (
	// stateFixedBytes: unlocked_count + compendium_revision +
	// compendium_seen_revision + framing.
	stateFixedBytes = 32
	// stateFactBytes: crop_definition_id + unlock_revision +
	// catalog_version_at_unlock + definition_version_at_unlock + activity
	// id allowance.
	stateFactBytes = 64
	// pageFixedBytes: catalog_version + revisions + flags + framing.
	pageFixedBytes = 64
	// pageEntryBytes: ids + enums + asset/activity string allowance.
	pageEntryBytes = 160
	// pageCategoryBytes: category id + title allowance per group.
	pageCategoryBytes = 128
)

// EstimateStateBytes returns the deterministic byte estimate of s used by
// the capacity fence. It is independent of the final physical encoding.
func EstimateStateBytes(s State) uint64 {
	return uint64(stateFixedBytes) + uint64(len(s.Facts))*stateFactBytes
}

// EstimatePageBytes returns the deterministic byte estimate of v.
func EstimatePageBytes(v PageView) uint64 {
	total := uint64(pageFixedBytes)
	for _, cat := range v.Categories {
		total += pageCategoryBytes + uint64(len(cat.Entries))*pageEntryBytes
	}
	return total
}

const (
	defaultLimitsProfileVersionV1     uint64 = 1
	defaultMaxCatalogEntriesV1        uint32 = 256
	defaultMaxActivityArchivesV1      uint32 = 64
	defaultMaxUnlockedEntriesV1       uint32 = 256
	defaultMaxCompendiumStateBytesV1  uint32 = 64 << 10
	defaultMaxCompendiumPageEntriesV1 uint32 = 256
	defaultMaxCompendiumPageBytesV1   uint32 = 256 << 10
)

// DefaultLimitsV1 returns the non-zero, versioned P0 capacity profile used by
// the production compendium provider.
func DefaultLimitsV1() LimitProfile {
	return LimitProfile{
		ProfileVersion:           defaultLimitsProfileVersionV1,
		MaxCatalogEntries:        defaultMaxCatalogEntriesV1,
		MaxActivityArchives:      defaultMaxActivityArchivesV1,
		MaxUnlockedEntries:       defaultMaxUnlockedEntriesV1,
		MaxCompendiumStateBytes:  defaultMaxCompendiumStateBytesV1,
		MaxCompendiumPageEntries: defaultMaxCompendiumPageEntriesV1,
		MaxCompendiumPageBytes:   defaultMaxCompendiumPageBytesV1,
	}
}
