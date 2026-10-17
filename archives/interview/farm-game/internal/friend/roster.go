package friend

// FriendIndex projection and roster pagination (contract 33 §6, 31 §11.4).
//
// FriendIndex is a rebuildable query copy: it answers "who shows up in the
// friend list quickly", never "is this pair still friends". Every returned
// page is re-validated against the canonical pair authority; stale entries
// are dropped and the page refills within a bounded budget. The cursor is an
// opaque server-side value bound to viewer, sort mode and roster revision —
// once the revision changes, old cursors fail with ErrRosterChanged and the
// client restarts from page one (33 §6.2).

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// DefaultPageSize and MaxPageSize are the configured pagination candidates.
const (
	DefaultPageSize = 20
	MaxPageSize     = 50
)

// SortModeActivatedDesc is the only P0 stable roster order
// (activated_at_ms DESC, friend_player_id ASC).
const SortModeActivatedDesc = "activated_desc"

// ErrRosterChanged fails cursors whose roster revision is stale
// (FRIEND_ROSTER_CHANGED, 33 §6.2).
var ErrRosterChanged = errors.New("friend: roster changed")

// ErrInvalidCursor fails malformed or foreign cursors.
var ErrInvalidCursor = errors.New("friend: invalid cursor")

// FriendIndexEntry is one projection row (33 §6.2).
type FriendIndexEntry struct {
	FriendPlayerID uint64
	RelationID     string
	RelationEpoch  uint64
	ActivatedAtMs  int64
}

// FriendIndex is the per-viewer rebuildable roster projection.
type FriendIndex struct {
	ViewerPlayerID uint64
	Entries        map[uint64]FriendIndexEntry
	RosterRevision uint64
}

// NewFriendIndex creates an empty projection for one viewer.
func NewFriendIndex(viewer uint64) *FriendIndex {
	return &FriendIndex{ViewerPlayerID: viewer, Entries: make(map[uint64]FriendIndexEntry)}
}

// ApplyRelationActivated upserts one entry on relation activation. Only a
// strictly newer epoch replaces an existing row; stale projection events
// never resurrect an old relation.
func (ix *FriendIndex) ApplyRelationActivated(entry FriendIndexEntry) bool {
	existing, ok := ix.Entries[entry.FriendPlayerID]
	if ok && existing.RelationEpoch >= entry.RelationEpoch && existing.RelationID == entry.RelationID {
		return false
	}
	ix.Entries[entry.FriendPlayerID] = entry
	ix.RosterRevision++
	return true
}

// ApplyRelationTerminated removes the entry only when it still points at
// the terminated (relation, epoch); a stale termination event never removes
// a newer re-activation (33 §9 "旧TerminationOrder迟到").
func (ix *FriendIndex) ApplyRelationTerminated(friendPlayerID uint64, relationID string, terminatedEpoch uint64) bool {
	existing, ok := ix.Entries[friendPlayerID]
	if !ok || existing.RelationID != relationID || existing.RelationEpoch != terminatedEpoch {
		return false
	}
	delete(ix.Entries, friendPlayerID)
	ix.RosterRevision++
	return true
}

// RebuildFromCanonical replaces the projection from canonical pair
// snapshots — the index is always re-derivable from the truth (33 §1.2).
func (ix *FriendIndex) RebuildFromCanonical(pairs []PairSnapshot) {
	ix.Entries = make(map[uint64]FriendIndexEntry, len(pairs))
	for _, snap := range pairs {
		if snap.Relation.Status != RelationStatusActive {
			continue
		}
		var friend uint64
		switch ix.ViewerPlayerID {
		case snap.LowPlayerID:
			friend = snap.HighPlayerID
		case snap.HighPlayerID:
			friend = snap.LowPlayerID
		default:
			continue
		}
		ix.Entries[friend] = FriendIndexEntry{
			FriendPlayerID: friend,
			RelationID:     snap.Relation.RelationID,
			RelationEpoch:  snap.Relation.RelationEpoch,
			ActivatedAtMs:  snap.Relation.ActivatedAtMs,
		}
	}
	ix.RosterRevision++
}

// ListCursor is the opaque pagination cursor (33 §6.2).
type ListCursor struct {
	ViewerPlayerID     uint64
	SortMode           string
	RosterRevision     uint64
	LastActivatedAtMs  int64
	LastFriendPlayerID uint64
}

// Encode renders the cursor as an opaque value.
func (c ListCursor) Encode() string {
	raw := strings.Join([]string{
		"v1",
		strconv.FormatUint(c.ViewerPlayerID, 10),
		c.SortMode,
		strconv.FormatUint(c.RosterRevision, 10),
		strconv.FormatInt(c.LastActivatedAtMs, 10),
		strconv.FormatUint(c.LastFriendPlayerID, 10),
	}, "|")
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// DecodeListCursor parses and validates an opaque cursor.
func DecodeListCursor(encoded string) (ListCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return ListCursor{}, ErrInvalidCursor
	}
	parts := strings.Split(string(raw), "|")
	if len(parts) != 6 || parts[0] != "v1" || parts[2] != SortModeActivatedDesc {
		return ListCursor{}, ErrInvalidCursor
	}
	viewer, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return ListCursor{}, ErrInvalidCursor
	}
	revision, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return ListCursor{}, ErrInvalidCursor
	}
	lastActivated, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		return ListCursor{}, ErrInvalidCursor
	}
	lastFriend, err := strconv.ParseUint(parts[5], 10, 64)
	if err != nil {
		return ListCursor{}, ErrInvalidCursor
	}
	return ListCursor{
		ViewerPlayerID: viewer, SortMode: parts[2], RosterRevision: revision,
		LastActivatedAtMs: lastActivated, LastFriendPlayerID: lastFriend,
	}, nil
}

// RosterPage is one validated page of the friend list.
type RosterPage struct {
	Entries        []FriendIndexEntry
	NextCursor     string // empty when the roster is exhausted
	RosterRevision uint64
}

// refillBudgetPages bounds how many extra candidates one page read scans
// while dropping stale entries (33 §6.2 "有界 refill budget").
const refillBudgetPages = 3

// sortedEntries orders the projection in the stable roster order.
func (ix *FriendIndex) sortedEntries() []FriendIndexEntry {
	entries := make([]FriendIndexEntry, 0, len(ix.Entries))
	for _, entry := range ix.Entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ActivatedAtMs != entries[j].ActivatedAtMs {
			return entries[i].ActivatedAtMs > entries[j].ActivatedAtMs
		}
		return entries[i].FriendPlayerID < entries[j].FriendPlayerID
	})
	return entries
}

// after reports whether entry sorts strictly after the cursor position.
func cursorAfter(entry FriendIndexEntry, cursor ListCursor) bool {
	if entry.ActivatedAtMs != cursor.LastActivatedAtMs {
		return entry.ActivatedAtMs < cursor.LastActivatedAtMs
	}
	return entry.FriendPlayerID > cursor.LastFriendPlayerID
}

// ListRoster reads one page: it validates every candidate against the
// canonical pair authority (relation still ACTIVE at the same epoch), drops
// stale rows and refills within a bounded budget until the page is full or
// the roster ends (33 §6.2).
func ListRoster(ctx context.Context, ix *FriendIndex, pairs PairRepository, cursor string, pageSize int) (RosterPage, error) {
	if ix == nil || pairs == nil {
		return RosterPage{}, fmt.Errorf("%w: roster inputs", ErrInvalidCommand)
	}
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}
	if pageSize > MaxPageSize {
		pageSize = MaxPageSize
	}
	var position ListCursor
	hasCursor := cursor != ""
	if hasCursor {
		decoded, err := DecodeListCursor(cursor)
		if err != nil {
			return RosterPage{}, err
		}
		if decoded.ViewerPlayerID != ix.ViewerPlayerID || decoded.SortMode != SortModeActivatedDesc {
			return RosterPage{}, ErrInvalidCursor
		}
		if decoded.RosterRevision != ix.RosterRevision {
			return RosterPage{}, ErrRosterChanged
		}
		position = decoded
	}
	ordered := ix.sortedEntries()
	start := 0
	if hasCursor {
		for start < len(ordered) && !cursorAfter(ordered[start], position) {
			start++
		}
	}
	budget := pageSize * refillBudgetPages
	page := make([]FriendIndexEntry, 0, pageSize)
	scanned := 0
	last := position
	for i := start; i < len(ordered) && len(page) < pageSize && scanned < budget; i++ {
		candidate := ordered[i]
		scanned++
		last = ListCursor{
			ViewerPlayerID: ix.ViewerPlayerID, SortMode: SortModeActivatedDesc,
			RosterRevision:    ix.RosterRevision,
			LastActivatedAtMs: candidate.ActivatedAtMs, LastFriendPlayerID: candidate.FriendPlayerID,
		}
		snap, err := pairs.GetPair(ctx, ix.ViewerPlayerID, candidate.FriendPlayerID)
		if err != nil {
			return RosterPage{}, err
		}
		if snap.Relation.Status != RelationStatusActive ||
			snap.Relation.RelationID != candidate.RelationID ||
			snap.Relation.RelationEpoch != candidate.RelationEpoch {
			continue // stale projection row; pair authority is the truth
		}
		page = append(page, candidate)
	}
	result := RosterPage{Entries: page, RosterRevision: ix.RosterRevision}
	if len(page) == pageSize {
		result.NextCursor = last.Encode()
	}
	return result, nil
}
