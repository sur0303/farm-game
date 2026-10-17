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

func DecodeSettleTheftVisitorPayload(workID string, payloadBytes []byte) (*contractspb.SettleTheftVisitorPayload, error) {
	if workidentity.ValidateStableOpaqueID(workID) != nil || len(payloadBytes) == 0 {
		return nil, fmt.Errorf("sourceorder: theft visitor payload identity")
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR)
	if err != nil {
		return nil, err
	}
	payload := spec.NewPayload().(*contractspb.SettleTheftVisitorPayload)
	if err := proto.Unmarshal(payloadBytes, payload); err != nil {
		return nil, fmt.Errorf("sourceorder: theft visitor protobuf decode: %w", err)
	}
	deterministic, err := (proto.MarshalOptions{Deterministic: true}).Marshal(payload)
	if err != nil || !bytes.Equal(deterministic, payloadBytes) {
		return nil, fmt.Errorf("sourceorder: theft visitor payload is not deterministic protobuf")
	}
	if _, err := contracts.CanonicalParticipantHash(spec, payload); err != nil {
		return nil, fmt.Errorf("sourceorder: theft visitor canonical payload: %w", err)
	}
	decision := payload.GetTheftTargetDecision()
	if payload.GetVisitorPlayerId() == 0 || decision == nil ||
		decision.GetVisitorPlayerId() != payload.GetVisitorPlayerId() ||
		decision.GetWorkflowId() != workID || len(payload.GetDecideTheftResultHash()) != 32 ||
		len(decision.GetTheftDecisionHash()) != 32 {
		return nil, fmt.Errorf("sourceorder: theft visitor payload binding")
	}
	return payload, nil
}

func DecodeCreditGuardIncomePayload(workID string, payloadBytes []byte) (*contractspb.CreditGuardIncomePayload, error) {
	if workidentity.ValidateStableOpaqueID(workID) != nil || len(payloadBytes) == 0 {
		return nil, fmt.Errorf("sourceorder: theft guard payload identity")
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME)
	if err != nil {
		return nil, err
	}
	payload := spec.NewPayload().(*contractspb.CreditGuardIncomePayload)
	if err := proto.Unmarshal(payloadBytes, payload); err != nil {
		return nil, fmt.Errorf("sourceorder: theft guard protobuf decode: %w", err)
	}
	deterministic, err := (proto.MarshalOptions{Deterministic: true}).Marshal(payload)
	if err != nil || !bytes.Equal(deterministic, payloadBytes) {
		return nil, fmt.Errorf("sourceorder: theft guard payload is not deterministic protobuf")
	}
	if _, err := contracts.CanonicalParticipantHash(spec, payload); err != nil {
		return nil, fmt.Errorf("sourceorder: theft guard canonical payload: %w", err)
	}
	decision := payload.GetTheftTargetDecision()
	if payload.GetOwnerPlayerId() == 0 || decision == nil ||
		decision.GetOwnerPlayerId() != payload.GetOwnerPlayerId() ||
		decision.GetWorkflowId() != workID || decision.GetGuardCoinValue() == 0 ||
		len(payload.GetDecideTheftResultHash()) != 32 || len(payload.GetSettleVisitorResultHash()) != 32 {
		return nil, fmt.Errorf("sourceorder: theft guard payload binding")
	}
	return payload, nil
}

func TheftVisitorPayloadHash(payload *contractspb.SettleTheftVisitorPayload) ([32]byte, error) {
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR)
	if err != nil {
		return [32]byte{}, err
	}
	return contracts.CanonicalParticipantHash(spec, payload)
}

func TheftGuardPayloadHash(payload *contractspb.CreditGuardIncomePayload) ([32]byte, error) {
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME)
	if err != nil {
		return [32]byte{}, err
	}
	return contracts.CanonicalParticipantHash(spec, payload)
}

func TheftVisitorInvocation(orderID string) (string, error) {
	return identity.ParticipantInvocationID(identity.WorkTypeTheft, orderID, identity.StepSettleTheftVisitor, 1)
}

func TheftGuardInvocation(orderID string) (string, error) {
	return identity.ParticipantInvocationID(identity.WorkTypeTheft, orderID, identity.StepCreditGuardIncome, 1)
}

func TheftVisitorSourceOrderID(farmID uint64, operationID string) (string, error) {
	return identity.SourceOrderID(identity.WorkTypeTheftVisitor, identity.SourceKindFarmActor,
		CanonicalActorID(farmID), operationID, 1)
}

func TheftGuardSourceOrderID(farmID uint64, operationID string) (string, error) {
	return identity.SourceOrderID(identity.WorkTypeTheftGuard, identity.SourceKindFarmActor,
		CanonicalActorID(farmID), operationID, 1)
}
