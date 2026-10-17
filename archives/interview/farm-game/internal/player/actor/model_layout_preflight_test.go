package actor

import "testing"

func TestPlayerV1InitialFixtureSerializedBytes(t *testing.T) {
	record := initialRecord(10_001, "preflight-bootstrap")
	for _, task := range record.Tasks {
		if task != nil {
			task.UpdatedAtMs = 1_700_000_000_000
		}
	}
	state, err := buildPlayerAuthorityModelState(record)
	if err != nil {
		t.Fatal(err)
	}
	modelBytes := len("record") + len(state["record"]) + len("public_profile") + len(state["public_profile"])
	if len(state["record"]) != 81 || len(state["public_profile"]) != 18 || modelBytes != 119 {
		t.Fatalf("v1 fixture drift: record=%d public_profile=%d model_state=%d", len(state["record"]), len(state["public_profile"]), modelBytes)
	}
}
