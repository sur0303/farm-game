package pet

// SwitchReceipt is the business receipt payload of a guard-pet switch
// (docs/design/36 §5.3 PetSwitchReceipt).
type SwitchReceipt struct {
	OperationID               string
	PreviousActiveGuardPetID  int32
	ResultingActiveGuardPetID int32
	PetConfigVersion          uint64
	GuardRuleVersion          uint64
}

// SwitchRequest mirrors the public PET_SWITCH logical fields
// (docs/design/36 §5.2). The client cannot submit remaining time,
// probabilities or guard_version.
type SwitchRequest struct {
	OperationID              string
	TargetPetID              int32
	ExpectedPetConfigVersion uint64
	ExpectedGuardRuleVersion uint64
}

// SwitchResult is the deterministic outcome of a switch attempt. Candidate
// and GuardSync are valid only when Status == StatusSucceeded. Switching
// never consumes dog food and never charges the wallet.
type SwitchResult struct {
	Status    ActionStatus
	ErrorCode ErrorCode
	Candidate State
	Receipt   SwitchReceipt
	GuardSync GuardSyncInput
}

// Switch selects another already-unlocked pet as the current guard pet
// (docs/design/36 §5 Q36-04). The guard deadline belongs to the player's
// single guard slot, not to the pet: a real switch keeps guard_until_ms
// unchanged, costs no dog food, and bumps both pet_state_version and
// guard_version with a freshly frozen rule snapshot.
//
// Switching to the already-active pet is NO_CHANGE/PET_ALREADY_ACTIVE: no
// version bump, no GuardSyncOrder. Switching an unowned pet is
// REJECTED/PET_NOT_UNLOCKED with no auto-purchase. Even an expired guard is
// synced on a real switch so the farm baseline stays consistent (§5.4).
func Switch(s State, cat CatalogSnapshot, req SwitchRequest) SwitchResult {
	// The active pet is already the requested state. Keep this stable
	// idempotent result even if hot config has since delisted or removed it.
	if req.TargetPetID != 0 && s.ActiveGuardPetID == req.TargetPetID {
		return SwitchResult{Status: StatusNoChange, ErrorCode: ErrPetAlreadyActive}
	}
	def, ok := cat.Find(req.TargetPetID)
	if !ok {
		return SwitchResult{Status: StatusRejected, ErrorCode: ErrPetNotFound}
	}
	if def.PetConfigVersion != req.ExpectedPetConfigVersion {
		return SwitchResult{Status: StatusRejected, ErrorCode: ErrPetConfigVersionChanged}
	}
	if def.GuardRule.GuardRuleVersion != req.ExpectedGuardRuleVersion {
		return SwitchResult{Status: StatusRejected, ErrorCode: ErrPetGuardRuleChanged}
	}
	if !s.Owns(req.TargetPetID) {
		return SwitchResult{Status: StatusRejected, ErrorCode: ErrPetNotUnlocked}
	}

	frozen := freezeGuardRule(def)
	candidate := s.Clone()
	candidate.ActiveGuardPetID = req.TargetPetID
	// guard_until_ms preserved verbatim (§5.1).
	candidate.GuardRuleSnapshot = &frozen
	candidate.PetStateVersion++
	candidate.GuardVersion++

	sync, ok := BuildGuardSyncInput(candidate)
	if !ok {
		return SwitchResult{Status: StatusRejected, ErrorCode: ErrPetNotAvailable}
	}
	return SwitchResult{
		Status:    StatusSucceeded,
		Candidate: candidate,
		Receipt: SwitchReceipt{
			OperationID:               req.OperationID,
			PreviousActiveGuardPetID:  s.ActiveGuardPetID,
			ResultingActiveGuardPetID: req.TargetPetID,
			PetConfigVersion:          def.PetConfigVersion,
			GuardRuleVersion:          def.GuardRule.GuardRuleVersion,
		},
		GuardSync: sync,
	}
}
