package sourceorder

import (
	"bytes"
	"fmt"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

func DecodeCommitPlantPayload(workID string, payloadBytes []byte) (*contractspb.CommitPlantPayload, error) {
	if workidentity.ValidateStableOpaqueID(workID) != nil || len(payloadBytes) == 0 {
		return nil, fmt.Errorf("sourceorder: plant payload identity")
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT)
	if err != nil {
		return nil, err
	}
	payload := spec.NewPayload().(*contractspb.CommitPlantPayload)
	if err := proto.Unmarshal(payloadBytes, payload); err != nil {
		return nil, fmt.Errorf("sourceorder: plant protobuf decode: %w", err)
	}
	deterministic, err := (proto.MarshalOptions{Deterministic: true}).Marshal(payload)
	if err != nil || !bytes.Equal(deterministic, payloadBytes) {
		return nil, fmt.Errorf("sourceorder: plant payload is not deterministic protobuf")
	}
	if _, err := contracts.CanonicalParticipantHash(spec, payload); err != nil {
		return nil, fmt.Errorf("sourceorder: plant canonical payload: %w", err)
	}
	if payload.GetOwnerPlayerId() == 0 || payload.GetFarmId() == 0 ||
		payload.GetOwnerPlayerId() != payload.GetFarmId() || payload.GetPlotId() == 0 ||
		payload.GetCropCycleId() == "" || payload.GetReservationId() == "" ||
		payload.GetCropRuleSnapshot() == nil || len(payload.GetCropRuleSnapshotHash()) != 32 ||
		len(payload.GetReserveSeedResultHash()) != 32 {
		return nil, fmt.Errorf("sourceorder: plant payload binding")
	}
	wantCycle, err := identity.CropCycleID(workID)
	if err != nil || payload.GetCropCycleId() != wantCycle {
		return nil, fmt.Errorf("sourceorder: plant crop cycle binding")
	}
	wantReservation, err := identity.SeedReservationID(workID)
	if err != nil || payload.GetReservationId() != wantReservation {
		return nil, fmt.Errorf("sourceorder: plant reservation binding")
	}
	return payload, nil
}

func PlantPayloadHash(payload *contractspb.CommitPlantPayload) ([32]byte, error) {
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT)
	if err != nil {
		return [32]byte{}, err
	}
	return contracts.CanonicalParticipantHash(spec, payload)
}

func PlantInvocation(orderID string) (string, error) {
	return identity.ParticipantInvocationID(identity.WorkTypePlant, orderID, identity.StepCommitPlant, 1)
}

func PlantSourceOrderID(playerID uint64, operationID string) (string, error) {
	return identity.SourceOrderID(identity.WorkTypePlant, identity.SourceKindPlayerActor,
		CanonicalActorID(playerID), operationID, 1)
}
