package pet

import (
	"fmt"
	"sort"

	guardpb "farmgame/api/gen/guard"
	playerpb "farmgame/api/gen/player"
)

// StateFromPlayerRecord decodes and validates the bounded PetState carried by
// PlayerActor. Empty state is valid for a new player. Corrupt records fail
// closed instead of silently disabling the guard or losing ownership.
func StateFromPlayerRecord(record *playerpb.PlayerRecord) (State, error) {
	if record == nil || record.GetPlayerId() == 0 {
		return State{}, fmt.Errorf("pet state: invalid player record")
	}
	pb := record.GetPetState()
	if pb == nil {
		return NewState(), nil
	}
	state := State{
		OwnedPetIDs:     append([]int32(nil), pb.GetOwnedPetIds()...),
		GuardUntilMs:    pb.GetGuardUntilMs(),
		PetStateVersion: pb.GetPetStateVersion(),
		GuardVersion:    pb.GetGuardVersion(),
	}
	if pb.ActiveGuardPetId != nil {
		state.ActiveGuardPetID = pb.GetActiveGuardPetId()
	}
	if rule := pb.GetGuardRuleSnapshot(); rule != nil {
		state.GuardRuleSnapshot = &GuardRuleSnapshot{
			PetConfigVersion: rule.GetPetConfigVersion(), GuardRuleVersion: rule.GetGuardRuleVersion(),
			TriggerProbabilityBp:   rule.GetTriggerProbabilityBp(),
			InterceptPolicy:        InterceptPolicy(rule.GetInterceptPolicy()),
			RandomAlgorithmVersion: rule.GetRandomAlgorithmVersion(), RandomKeyVersion: rule.GetRandomKeyVersion(),
		}
	}
	if err := ValidateState(state); err != nil {
		return State{}, err
	}
	return state, nil
}

// ApplyStateToPlayerRecord writes one validated immutable domain state.
func ApplyStateToPlayerRecord(record *playerpb.PlayerRecord, state State) error {
	if record == nil || record.GetPlayerId() == 0 {
		return fmt.Errorf("pet state: invalid player record")
	}
	if err := ValidateState(state); err != nil {
		return err
	}
	pb := &playerpb.PetState{
		OwnedPetIds: append([]int32(nil), state.OwnedPetIDs...), GuardUntilMs: state.GuardUntilMs,
		PetStateVersion: state.PetStateVersion, GuardVersion: state.GuardVersion,
	}
	if state.ActiveGuardPetID != 0 {
		id := state.ActiveGuardPetID
		pb.ActiveGuardPetId = &id
	}
	if rule := state.GuardRuleSnapshot; rule != nil {
		pb.GuardRuleSnapshot = &guardpb.GuardRuleSnapshot{
			PetConfigVersion: rule.PetConfigVersion, GuardRuleVersion: rule.GuardRuleVersion,
			TriggerProbabilityBp:   rule.TriggerProbabilityBp,
			InterceptPolicy:        guardpb.GuardInterceptPolicy(rule.InterceptPolicy),
			RandomAlgorithmVersion: rule.RandomAlgorithmVersion, RandomKeyVersion: rule.RandomKeyVersion,
		}
	}
	record.PetState = pb
	return nil
}

// ValidateState enforces every persistence invariant independently from hot
// config. It is used on both read and write so malformed state can never be
// admitted merely because a command happens to be a query.
func ValidateState(state State) error {
	if len(state.OwnedPetIDs) > MaxOwnedPets {
		return fmt.Errorf("pet state: owned count %d exceeds %d", len(state.OwnedPetIDs), MaxOwnedPets)
	}
	if !sort.SliceIsSorted(state.OwnedPetIDs, func(i, j int) bool { return state.OwnedPetIDs[i] < state.OwnedPetIDs[j] }) {
		return fmt.Errorf("pet state: owned ids are not strictly sorted")
	}
	for i, id := range state.OwnedPetIDs {
		if id <= 0 || (i > 0 && state.OwnedPetIDs[i-1] == id) {
			return fmt.Errorf("pet state: invalid or duplicate owned id %d", id)
		}
	}
	if state.GuardVersion > state.PetStateVersion {
		return fmt.Errorf("pet state: guard version exceeds pet state version")
	}
	if state.ActiveGuardPetID == 0 {
		if state.GuardVersion != 0 || state.GuardUntilMs != 0 || state.GuardRuleSnapshot != nil {
			return fmt.Errorf("pet state: guard fields exist without active pet")
		}
		return nil
	}
	if !state.Owns(state.ActiveGuardPetID) {
		return fmt.Errorf("pet state: active pet is not owned")
	}
	if state.GuardVersion == 0 || state.GuardRuleSnapshot == nil {
		return fmt.Errorf("pet state: active pet lacks frozen guard rule")
	}
	rule := state.GuardRuleSnapshot
	if state.GuardUntilMs < 0 || rule.PetConfigVersion == 0 || rule.GuardRuleVersion == 0 ||
		rule.TriggerProbabilityBp > 10000 || rule.InterceptPolicy != InterceptPolicyFullGross ||
		rule.RandomAlgorithmVersion == 0 || rule.RandomKeyVersion == 0 {
		return fmt.Errorf("pet state: invalid frozen guard rule")
	}
	return nil
}
