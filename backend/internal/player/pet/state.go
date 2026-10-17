package pet

// State is the authoritative player-side pet state (docs/design/36 §6.1
// PetState). It is the single truth for owned pets, the selected guard pet
// and the guard deadline; FarmActor only ever sees the versioned
// GuardSnapshot derived from it.
//
// State is treated as an immutable value by this package: every mutation
// function returns a new CandidateState and never modifies its input.
//
// Invariants (enforced by every successful transition):
//   - OwnedPetIDs is a sorted, deduplicated set of size <= MaxOwnedPets;
//   - ActiveGuardPetID == 0 means "no pet selected"; a non-zero value must
//     be present in OwnedPetIDs;
//   - GuardUntilMs is UTC Unix milliseconds, initially 0;
//   - PetStateVersion increments on any real state change (unlock, feed,
//     switch); GuardVersion increments only on feed or real switch;
//   - GuardRuleSnapshot is the frozen rule of the current GuardVersion, never
//     a reference to hot config.
type State struct {
	OwnedPetIDs       []int32
	ActiveGuardPetID  int32
	GuardUntilMs      int64
	PetStateVersion   uint64
	GuardVersion      uint64
	GuardRuleSnapshot *GuardRuleSnapshot
}

// NewState returns the empty initial state of a new player.
func NewState() State {
	return State{}
}

// Clone returns a deep copy that shares no mutable memory with s.
func (s State) Clone() State {
	out := s
	if s.OwnedPetIDs != nil {
		out.OwnedPetIDs = make([]int32, len(s.OwnedPetIDs))
		copy(out.OwnedPetIDs, s.OwnedPetIDs)
	}
	if s.GuardRuleSnapshot != nil {
		snap := *s.GuardRuleSnapshot
		out.GuardRuleSnapshot = &snap
	}
	return out
}

// Owns reports whether petID is in the owned set.
func (s State) Owns(petID int32) bool {
	for _, id := range s.OwnedPetIDs {
		if id == petID {
			return true
		}
	}
	return false
}

// HasActiveGuardPet reports whether a guard pet is currently selected.
func (s State) HasActiveGuardPet() bool {
	return s.ActiveGuardPetID != 0
}

// GuardEffective reports whether the guard produces any effect at
// trustedNowMs (docs/design/36 §3.1, §4.5). It is computed lazily from the
// trusted clock; no expiry timer or "EXPIRED" write ever exists. The
// boundary is strict: trusted_now_ms == guard_until_ms is already expired.
func (s State) GuardEffective(trustedNowMs int64) bool {
	return s.ActiveGuardPetID != 0 && trustedNowMs < s.GuardUntilMs
}

// insertOwned returns a new sorted owned set with petID added. The caller
// must have verified the pet is not already owned.
func insertOwned(owned []int32, petID int32) []int32 {
	out := make([]int32, 0, len(owned)+1)
	inserted := false
	for _, id := range owned {
		if !inserted && petID < id {
			out = append(out, petID)
			inserted = true
		}
		out = append(out, id)
	}
	if !inserted {
		out = append(out, petID)
	}
	return out
}

// GuardRuleSnapshot is the frozen guard rule attached to one GuardVersion
// (docs/design/36 §6.1 rule 7). It is constructed once per successful
// feed/switch from the confirming PetDefinition and must be byte-identical
// between PetState and the emitted GuardSync input.
type GuardRuleSnapshot struct {
	PetConfigVersion       uint64
	GuardRuleVersion       uint64
	TriggerProbabilityBp   uint32
	InterceptPolicy        InterceptPolicy
	RandomAlgorithmVersion uint64
	RandomKeyVersion       uint64
}

// freezeGuardRule snapshots the rule of def at decision time.
func freezeGuardRule(def Definition) GuardRuleSnapshot {
	return GuardRuleSnapshot{
		PetConfigVersion:       def.PetConfigVersion,
		GuardRuleVersion:       def.GuardRule.GuardRuleVersion,
		TriggerProbabilityBp:   def.GuardRule.TriggerProbabilityBp,
		InterceptPolicy:        def.GuardRule.InterceptPolicy,
		RandomAlgorithmVersion: def.GuardRule.RandomAlgorithmVersion,
		RandomKeyVersion:       def.GuardRule.RandomKeyVersion,
	}
}
