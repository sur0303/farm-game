package compendium

// EntryCategory is the display classification of one catalog entry
// (docs/design/37 §4.1 PlantCatalogEntry.category).
type EntryCategory int32

const (
	EntryCategoryUnspecified EntryCategory = iota
	EntryCategoryNormal
	EntryCategoryActivity
)

func (c EntryCategory) String() string {
	switch c {
	case EntryCategoryNormal:
		return "NORMAL"
	case EntryCategoryActivity:
		return "ACTIVITY"
	default:
		return "UNSPECIFIED"
	}
}

// ActivityState is the lifecycle state of an activity catalog entry
// (docs/design/37 §4.1). Once ARCHIVED the entry belongs to the immutable
// activity archive; the player's unlock facts are never touched.
type ActivityState int32

const (
	ActivityStateUnspecified ActivityState = iota
	ActivityStateActive
	ActivityStateArchived
)

func (s ActivityState) String() string {
	switch s {
	case ActivityStateActive:
		return "ACTIVE"
	case ActivityStateArchived:
		return "ARCHIVED"
	default:
		return "UNSPECIFIED"
	}
}

// DisplayState is the per-entry render state of the compendium page
// (docs/design/37 §5.2). LOCKED_SILHOUETTE exposes only the gray silhouette
// and basic placeholder information, never the full detail of a locked
// activity plant.
type DisplayState int32

const (
	DisplayStateUnspecified DisplayState = iota
	// DisplayStateUnlockedFull: full image, name and configured description.
	DisplayStateUnlockedFull
	// DisplayStateLockedSilhouette: gray silhouette only.
	DisplayStateLockedSilhouette
)

func (s DisplayState) String() string {
	switch s {
	case DisplayStateUnlockedFull:
		return "UNLOCKED_FULL"
	case DisplayStateLockedSilhouette:
		return "LOCKED_SILHOUETTE"
	default:
		return "UNSPECIFIED"
	}
}

// ActionStatus is the deterministic terminal status of a compendium domain
// decision, mirroring the status vocabulary of the other player subdomains
// (docs/design/36 §6.6). Recoverable execution states never appear here.
type ActionStatus uint8

const (
	StatusSucceeded ActionStatus = iota + 1
	StatusNoChange
	StatusRejected
)

func (s ActionStatus) String() string {
	switch s {
	case StatusSucceeded:
		return "SUCCEEDED"
	case StatusNoChange:
		return "NO_CHANGE"
	case StatusRejected:
		return "REJECTED"
	default:
		return "UNKNOWN"
	}
}
