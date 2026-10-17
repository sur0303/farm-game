package main

import (
	"crypto/sha256"
	"testing"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"

	"google.golang.org/protobuf/proto"
)

func TestParticipantPayloadHashUsesReceiptCanonicalBinding(t *testing.T) {
	payload := &contractspb.DecideTheftPayload{
		VisitorPlayerId: 2,
		OwnerPlayerId:   1,
		FarmId:          1,
		PlotId:          1,
		CropCycleId:     "cycle-1",
		TheftAttemptId:  "attempt-1",
		SettlementId:    "settlement-1",
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	got, err := participantPayloadHash(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT, data)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT)
	if err != nil {
		t.Fatal(err)
	}
	want, err := contracts.CanonicalParticipantHash(spec, payload)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("canonical receipt hash mismatch: got=%x want=%x", got, want)
	}
	if got == sha256.Sum256(data) {
		t.Fatal("audit used the raw workflow payload hash instead of the participant receipt binding")
	}

	payload.SettlementId = "settlement-2"
	changed, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	changedHash, err := participantPayloadHash(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT, changed)
	if err != nil {
		t.Fatal(err)
	}
	if changedHash == got {
		t.Fatal("different participant payload retained the same receipt binding")
	}
}
