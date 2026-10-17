package compendium

// UnlockFact is one permanent membership record of the compendium
// (docs/design/37 §2). The unique compendium identity is crop_definition_id;
// display resources and activity instance ids never participate in the
// permanent set. The provenance fields are copied from the Plant-time frozen
// snapshot of the cycle that first unlocked the entry and are never
// rewritten afterwards — not by activity end, catalog hot updates, Actor
// migration or retries.
type UnlockFact struct {
	CropDefinitionID uint32
	// UnlockRevision is the compendium_revision value this fact created.
	UnlockRevision uint64
	// SourceActivityID is the activity the first-unlock cycle belonged to,
	// empty for normal plants.
	SourceActivityID          string
	CatalogVersionAtUnlock    uint64
	DefinitionVersionAtUnlock uint64
}

// State is the authoritative player-side compendium state
// (docs/design/37 §2 CompendiumState). It is the single truth of permanent
// unlock facts and the red-dot revisions. Physical placement
// (model:compendium_meta + model:compendium_membership inside the Player
// Authority record, sharing WriterClaim/RecordVersion/Durable Lane with
// Wallet/Inventory) is the integrator's concern (docs/design/31).
//
// State is treated as an immutable value by this package: every mutation
// function returns a new State and never modifies its input.
//
// Invariants (enforced by every successful transition):
//   - Facts is sorted by CropDefinitionID, deduplicated, and bounded by the
//     active LimitProfile;
//   - Revision increments exactly once per first unlock; NO_CHANGE and
//     REJECTED never move it;
//   - SeenRevision is monotonic and never exceeds Revision;
//   - has_new_unlock of the read path is derived as SeenRevision < Revision,
//     never stored (docs/design/37 §5.2).
type State struct {
	Facts        []UnlockFact
	Revision     uint64
	SeenRevision uint64
}

// NewState returns the empty initial state of a new player.
func NewState() State { return State{} }

// Clone returns a deep copy that shares no mutable memory with s.
func (s State) Clone() State {
	out := s
	if s.Facts != nil {
		out.Facts = make([]UnlockFact, len(s.Facts))
		copy(out.Facts, s.Facts)
	}
	return out
}

// UnlockedCount reports the permanent membership size.
func (s State) UnlockedCount() uint32 { return uint32(len(s.Facts)) }

// IsUnlocked reports whether cropDefinitionID is a permanent member.
func (s State) IsUnlocked(cropDefinitionID uint32) bool {
	_, ok := s.findFact(cropDefinitionID)
	return ok
}

// FactOf returns the stored fact of cropDefinitionID, if present.
func (s State) FactOf(cropDefinitionID uint32) (UnlockFact, bool) {
	return s.findFact(cropDefinitionID)
}

// HasNewUnlock derives the red-dot flag (docs/design/37 §5.2).
func (s State) HasNewUnlock() bool { return s.SeenRevision < s.Revision }

// findFact binary-searches the sorted fact set.
func (s State) findFact(cropDefinitionID uint32) (UnlockFact, bool) {
	lo, hi := 0, len(s.Facts)
	for lo < hi {
		mid := (lo + hi) / 2
		switch {
		case s.Facts[mid].CropDefinitionID < cropDefinitionID:
			lo = mid + 1
		case s.Facts[mid].CropDefinitionID > cropDefinitionID:
			hi = mid
		default:
			return s.Facts[mid], true
		}
	}
	return UnlockFact{}, false
}

// insertFact returns a new sorted fact set with fact added. The caller must
// have verified the crop is not already unlocked.
func insertFact(facts []UnlockFact, fact UnlockFact) []UnlockFact {
	out := make([]UnlockFact, 0, len(facts)+1)
	inserted := false
	for _, f := range facts {
		if !inserted && fact.CropDefinitionID < f.CropDefinitionID {
			out = append(out, fact)
			inserted = true
		}
		out = append(out, f)
	}
	if !inserted {
		out = append(out, fact)
	}
	return out
}
