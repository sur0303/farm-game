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

func DecodeCommitPlotUnlockPayload(workID string, payloadBytes []byte) (*contractspb.CommitPlotUnlockPayload, error) {
	if workidentity.ValidateStableOpaqueID(workID) != nil || len(payloadBytes) == 0 {
		return nil, fmt.Errorf("sourceorder: unlock payload identity")
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK)
	if err != nil {
		return nil, err
	}
	payload := spec.NewPayload().(*contractspb.CommitPlotUnlockPayload)
	if err := proto.Unmarshal(payloadBytes, payload); err != nil {
		return nil, fmt.Errorf("sourceorder: unlock protobuf decode: %w", err)
	}
	deterministic, err := (proto.MarshalOptions{Deterministic: true}).Marshal(payload)
	if err != nil || !bytes.Equal(deterministic, payloadBytes) {
		return nil, fmt.Errorf("sourceorder: unlock payload is not deterministic protobuf")
	}
	if _, err := contracts.CanonicalParticipantHash(spec, payload); err != nil {
		return nil, fmt.Errorf("sourceorder: unlock canonical payload: %w", err)
	}
	if payload.GetOwnerPlayerId() == 0 || payload.GetFarmId() == 0 ||
		payload.GetOwnerPlayerId() != payload.GetFarmId() || payload.GetPlotId() == 0 ||
		payload.GetReservationId() == "" || len(payload.GetReserveUnlockResultHash()) != 32 ||
		len(payload.GetUnlockRuleSnapshotHash()) != 32 {
		return nil, fmt.Errorf("sourceorder: unlock payload binding")
	}
	wantReservation, err := identity.UnlockCoinReservationID(workID)
	if err != nil || payload.GetReservationId() != wantReservation {
		return nil, fmt.Errorf("sourceorder: unlock reservation binding")
	}
	return payload, nil
}

func UnlockPayloadHash(payload *contractspb.CommitPlotUnlockPayload) ([32]byte, error) {
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK)
	if err != nil {
		return [32]byte{}, err
	}
	return contracts.CanonicalParticipantHash(spec, payload)
}

func UnlockInvocation(orderID string) (string, error) {
	return identity.ParticipantInvocationID(identity.WorkTypeUnlockPlot, orderID, identity.StepCommitPlotUnlock, 1)
}

func UnlockSourceOrderID(playerID uint64, operationID string) (string, error) {
	return identity.SourceOrderID(identity.WorkTypeUnlockPlot, identity.SourceKindPlayerActor,
		CanonicalActorID(playerID), operationID, 1)
}
