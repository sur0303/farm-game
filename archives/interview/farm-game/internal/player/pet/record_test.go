package pet

import (
	"testing"

	playerpb "farmgame/api/gen/player"
)

func validRecordState() State {
	return State{
		OwnedPetIDs: []int32{2, 7}, ActiveGuardPetID: 7, GuardUntilMs: 12345,
		PetStateVersion: 4, GuardVersion: 3,
		GuardRuleSnapshot: &GuardRuleSnapshot{
			PetConfigVersion: 8, GuardRuleVersion: 9, TriggerProbabilityBp: 5000,
			InterceptPolicy: InterceptPolicyFullGross, RandomAlgorithmVersion: 1, RandomKeyVersion: 2,
		},
	}
}

func TestPlayerRecordPetRoundTrip(t *testing.T) {
	record := &playerpb.PlayerRecord{PlayerId: 42}
	want := validRecordState()
	if err := ApplyStateToPlayerRecord(record, want); err != nil {
		t.Fatal(err)
	}
	got, err := StateFromPlayerRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.OwnedPetIDs) != 2 || got.ActiveGuardPetID != 7 || got.GuardRuleSnapshot == nil ||
		got.GuardRuleSnapshot.GuardRuleVersion != 9 {
		t.Fatalf("round trip state=%+v", got)
	}
}

func TestPetStatePersistenceFailsClosed(t *testing.T) {
	cases := []State{
		{OwnedPetIDs: []int32{2, 1}},
		{OwnedPetIDs: []int32{1, 1}},
		{OwnedPetIDs: []int32{1}, ActiveGuardPetID: 2, PetStateVersion: 1, GuardVersion: 1, GuardRuleSnapshot: validRecordState().GuardRuleSnapshot},
		{OwnedPetIDs: []int32{7}, ActiveGuardPetID: 7, PetStateVersion: 1, GuardVersion: 2, GuardRuleSnapshot: validRecordState().GuardRuleSnapshot},
		{OwnedPetIDs: []int32{7}, ActiveGuardPetID: 7, PetStateVersion: 1, GuardVersion: 1},
	}
	for i, state := range cases {
		if err := ValidateState(state); err == nil {
			t.Fatalf("case %d accepted", i)
		}
	}
}

func TestEmptyPetStateIsValid(t *testing.T) {
	got, err := StateFromPlayerRecord(&playerpb.PlayerRecord{PlayerId: 42})
	if err != nil || len(got.OwnedPetIDs) != 0 {
		t.Fatalf("state=%+v err=%v", got, err)
	}
}
