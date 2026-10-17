package actor

import "testing"

func TestFarmV1InitialFixtureSerializedBytes(t *testing.T) {
	state, _, _, err := buildFarmAuthorityModelState(InitialRecord(20_002))
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for name, value := range state {
		total += len(name) + len(value)
	}
	if len(state["record"]) != 249 || len(state[FarmOpportunityHeaderModelField]) != 48 ||
		len(state[FarmOpportunityFactsModelField]) != 20 || total != 358 {
		t.Fatalf("v1 fixture drift: record=%d header=%d facts=%d model_state=%d", len(state["record"]),
			len(state[FarmOpportunityHeaderModelField]), len(state[FarmOpportunityFactsModelField]), total)
	}
}
