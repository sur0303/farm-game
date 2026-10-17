package ingress

import (
	"testing"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
)

func TestSkipsDecideTheftWorkRecordFence(t *testing.T) {
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT)
	if err != nil {
		t.Fatal(err)
	}
	sourceCall := &contracts.ParticipantCall{
		Spec: spec,
		Work: &contractspb.WorkAddress{
			PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
		},
	}
	if !skipsDecideTheftWorkRecordFence(sourceCall) {
		t.Fatal("source-order DECIDE_THEFT must skip the dest WorkRecord fence")
	}
	if skipsFarmWorkRecordFence(sourceCall) {
		t.Fatal("DECIDE_THEFT must not reuse the plant/unlock fence skip")
	}
	legacyCall := &contracts.ParticipantCall{
		Spec: spec,
		Work: &contractspb.WorkAddress{
			PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET,
		},
	}
	if skipsDecideTheftWorkRecordFence(legacyCall) {
		t.Fatal("legacy WorkRecord DECIDE_THEFT must keep the dest fence")
	}
}
