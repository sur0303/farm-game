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

func DecodeGrantHarvestPayload(workID string, payloadBytes []byte) (*contractspb.GrantHarvestRewardPayload, error) {
	if workidentity.ValidateStableOpaqueID(workID) != nil || len(payloadBytes) == 0 {
		return nil, fmt.Errorf("sourceorder: harvest payload identity")
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD)
	if err != nil {
		return nil, err
	}
	payload := spec.NewPayload().(*contractspb.GrantHarvestRewardPayload)
	if err := proto.Unmarshal(payloadBytes, payload); err != nil {
		return nil, fmt.Errorf("sourceorder: harvest protobuf decode: %w", err)
	}
	deterministic, err := (proto.MarshalOptions{Deterministic: true}).Marshal(payload)
	if err != nil || !bytes.Equal(deterministic, payloadBytes) {
		return nil, fmt.Errorf("sourceorder: harvest payload is not deterministic protobuf")
	}
	if _, err := contracts.CanonicalParticipantHash(spec, payload); err != nil {
		return nil, fmt.Errorf("sourceorder: harvest canonical payload: %w", err)
	}
	decision := payload.GetHarvestDecision()
	if payload.GetPlayerId() == 0 || decision == nil || decision.GetFarmId() == 0 ||
		decision.GetOwnerPlayerId() != payload.GetPlayerId() || decision.GetPlotId() == 0 ||
		decision.GetCropCycleId() == "" || decision.GetWorkflowId() != workID ||
		len(payload.GetCommitHarvestResultHash()) != 32 || len(decision.GetHarvestDecisionHash()) != 32 {
		return nil, fmt.Errorf("sourceorder: harvest payload binding")
	}
	return payload, nil
}

func HarvestPayloadHash(payload *contractspb.GrantHarvestRewardPayload) ([32]byte, error) {
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD)
	if err != nil {
		return [32]byte{}, err
	}
	return contracts.CanonicalParticipantHash(spec, payload)
}

func HarvestInvocation(orderID string) (string, error) {
	return identity.ParticipantInvocationID(identity.WorkTypeHarvest, orderID, identity.StepGrantHarvestReward, 1)
}
