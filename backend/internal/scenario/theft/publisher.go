package theft

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"

	commonpb "farmgame/api/gen/common"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/identity"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"
)

// ResultPublication is the immutable terminal projection handed to the
// composition layer. The publisher owns its delivery/idempotency mechanism;
// this package never constructs Gate or Redis keys.
type ResultPublication struct {
	WorkflowID      string
	OperationID     string
	OperationDigest string
	SubjectPlayerID uint64
	PairID          string
	WorkAddress     workidentity.WorkAddress
	SourceRef       string
	PayloadHash     []byte
	Outcome         *publicpb.StoredOperationOutcome
}

// ResultPublisher is the only result-delivery port used by this scenario.
type ResultPublisher interface {
	Publish(context.Context, ResultPublication) error
}

// PublishResult is also exposed on Coordinator for online composition roots;
// publication remains an injected side effect after durable completion.
func (c *Coordinator) PublishResult(ctx context.Context, record *workflow.WorkRecord, publisher ResultPublisher) error {
	if c == nil {
		return ErrInvalidInput
	}
	return PublishResult(ctx, record, publisher)
}

// PublishResult verifies a legal durable terminal, builds its public stored
// outcome, and delegates delivery to the injected projection port.
func PublishResult(ctx context.Context, record *workflow.WorkRecord, publisher ResultPublisher) error {
	if publisher == nil {
		return ErrInvalidInput
	}
	outcome, payloadHash, err := PublicOutcome(record)
	if err != nil {
		return err
	}
	key, err := identityKeyForRecord(record)
	if err != nil {
		return err
	}
	digest, err := key.OperationDigest()
	if err != nil {
		return err
	}
	return publisher.Publish(ctx, ResultPublication{
		WorkflowID:      record.Address.WorkID,
		OperationID:     record.OperationID,
		OperationDigest: digest,
		SubjectPlayerID: record.SubjectPlayerID,
		PairID:          key.Root.CanonicalID,
		WorkAddress:     record.Address,
		PayloadHash:     payloadHash,
		Outcome:         outcome,
	})
}

// PublicOutcome returns the frozen GET_OPERATION_RESULT payload for every
// legal Theft terminal. PROCESSING and RECONCILE_REQUIRED are deliberately
// excluded because neither is a public terminal result.
func PublicOutcome(record *workflow.WorkRecord) (*publicpb.StoredOperationOutcome, []byte, error) {
	if record == nil || !record.Terminal() {
		return nil, nil, ErrTerminalResult
	}
	root, err := DecodeRoot(record)
	if err != nil || root.GetActionPermitSnapshot() == nil {
		return nil, nil, fmt.Errorf("%w: Theft root", ErrTerminalResult)
	}
	payloadHash := root.GetActionPermitSnapshot().GetCanonicalPayloadHash()
	if len(payloadHash) != sha256.Size || root.GetVisitorPlayerId() != record.SubjectPlayerID ||
		root.GetActionPermitSnapshot().GetOperationId() != record.OperationID || root.GetFarmId() == 0 {
		return nil, nil, fmt.Errorf("%w: terminal identity binding", ErrTerminalResult)
	}
	stored := &publicpb.StoredOperationOutcome{
		OriginalCommandType: commonpb.CommandType_THEFT,
		OriginalOperationId: record.OperationID,
		FarmContext:         &publicpb.StoredFarmOutcomeContext{TargetFarmId: root.GetFarmId()},
	}
	switch OutcomeFor(record) {
	case OutcomeSucceeded:
		result, resultErr := PublicResult(record)
		if resultErr != nil {
			return nil, nil, resultErr
		}
		stored.Status = commonpb.CommandStatus_SUCCEEDED
		stored.ResultPayload, err = deterministicProto(result)
		if err != nil {
			return nil, nil, err
		}
	case OutcomeNoChange:
		stored.Status = commonpb.CommandStatus_NO_CHANGE
	case OutcomeRejected:
		stored.Status = commonpb.CommandStatus_REJECTED
		code := commonpb.ErrorCode_ERR_INTERNAL
		for _, class := range terminalErrorClasses(record) {
			switch strings.ToUpper(strings.TrimSpace(class)) {
			case ErrorClassParticipantIdempotencyConflict, "ERR_IDEMPOTENCY_CONFLICT", "IDEMPOTENCY_CONFLICT":
				code = commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT
			default:
				if value, ok := commonpb.ErrorCode_value[class]; ok && value != int32(commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED) {
					code = commonpb.ErrorCode(value)
				}
			}
		}
		stored.Error = &commonpb.ErrorDetail{ErrorCode: code, ErrorMessage: "Theft was rejected"}
	default:
		return nil, nil, ErrTerminalResult
	}
	return stored, append([]byte(nil), payloadHash...), nil
}

func terminalErrorClasses(record *workflow.WorkRecord) []string {
	classes := []string{record.LastErrorClass}
	for i := range record.Steps {
		if record.Steps[i].Status == workflow.StepRejected {
			classes = append(classes, record.Steps[i].ErrorClass)
		}
	}
	return classes
}

func identityKeyForRecord(record *workflow.WorkRecord) (identity.EffectiveIdempotencyKey, error) {
	root, err := DecodeRoot(record)
	if err != nil {
		return identity.EffectiveIdempotencyKey{}, err
	}
	pairID, err := identity.CanonicalFriendRelationID(root.GetVisitorPlayerId(), root.GetOwnerPlayerId())
	if err != nil {
		return identity.EffectiveIdempotencyKey{}, err
	}
	return identity.EffectiveIdempotencyKey{
		SubjectPlayerID:       record.SubjectPlayerID,
		StableCommandTypeName: string(identity.WorkTypeTheft),
		OperationID:           record.OperationID,
		Root: identity.OperationRootRef{
			Kind:        identity.RootKindFriendPair,
			CanonicalID: pairID,
		},
	}, nil
}
