package participant

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	economypb "farmgame/api/gen/economy"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	"farmgame/internal/contracts"
	"farmgame/internal/cropcatalog"
	"farmgame/internal/identity"
	playeractor "farmgame/internal/player/actor"
	playeringress "farmgame/internal/player/ingress"
	playertask "farmgame/internal/player/task"
	"farmgame/internal/sourceordermetrics"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const maxTheftTypedResultBytes = 256

func isTheftPlayerCommand(call *contracts.ParticipantCall) bool {
	if call == nil {
		return false
	}
	return call.Spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR ||
		call.Spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME
}

func maxTheftResultProofEnvelopeBytes(invocationID string, _ contractspb.ParticipantCommandType) int {
	return participantResultProofFixedBytes + len(invocationID) + maxTheftTypedResultBytes
}

func maxTheftParticipantResultBytes(command contractspb.ParticipantCommandType) int {
	if command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR ||
		command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME {
		return maxTheftTypedResultBytes
	}
	return maxParticipantResultBytes(command)
}

func theftOperationResultCapacityEstimate(command contractspb.ParticipantCommandType) authoritystore.StoredOperationOutcome {
	return authoritystore.StoredOperationOutcome{
		Status:  authoritystore.TerminalSucceeded,
		Payload: make([]byte, maxTheftTypedResultBytes),
	}
}

func theftStoredOperationOutcome(outcome contractspb.ParticipantOutcome, errorCode commonpb.ErrorCode, typedResult []byte) authoritystore.StoredOperationOutcome {
	statusValue := authoritystore.TerminalRejected
	switch outcome {
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED:
		statusValue = authoritystore.TerminalSucceeded
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE:
		statusValue = authoritystore.TerminalNoChange
	}
	errorText := ""
	if errorCode != commonpb.ErrorCode_OK && errorCode != commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED {
		errorText = errorCode.String()
	}
	return authoritystore.StoredOperationOutcome{Status: statusValue, ErrorCode: errorText, Payload: append([]byte(nil), typedResult...)}
}

// TheftDecisionHash computes the Farm-frozen decision hash over all semantic
// fields except theft_decision_hash. Player settlement never recomputes a
// quantity; it only checks this proof and consumes its signed values.
func TheftDecisionHash(decision *contractspb.TheftTargetDecision) ([32]byte, error) {
	if decision == nil {
		return [32]byte{}, status.Error(codes.InvalidArgument, "theft target decision is required")
	}
	clone, ok := proto.Clone(decision).(*contractspb.TheftTargetDecision)
	if !ok {
		return [32]byte{}, status.Error(codes.Internal, "theft target decision clone failed")
	}
	clone.TheftDecisionHash = nil
	return contracts.CanonicalHash(clone)
}

// DecideTheftResultHash is the result proof carried by both Player steps.
func DecideTheftResultHash(result *contractspb.DecideTheftResult) ([32]byte, error) {
	if result == nil {
		return [32]byte{}, status.Error(codes.InvalidArgument, "decide theft result is required")
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT)
	if err != nil {
		return [32]byte{}, err
	}
	return contracts.CanonicalParticipantHash(spec, result)
}

func SettleTheftVisitorResultHash(result *contractspb.SettleTheftVisitorResult) ([32]byte, error) {
	if result == nil {
		return [32]byte{}, status.Error(codes.InvalidArgument, "settle theft visitor result is required")
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR)
	if err != nil {
		return [32]byte{}, err
	}
	return contracts.CanonicalParticipantHash(spec, result)
}

func validateTheftPayload(call *contracts.ParticipantCall) error {
	if call == nil || call.RouteHeader == nil || call.Work == nil {
		return status.Error(codes.InvalidArgument, "player participant Theft call is incomplete")
	}
	decision := (*contractspb.TheftTargetDecision)(nil)
	switch payload := call.Payload.(type) {
	case *contractspb.SettleTheftVisitorPayload:
		if call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR ||
			payload.GetVisitorPlayerId() == 0 || payload.GetVisitorPlayerId() != call.RouteHeader.GetActorId() ||
			(call.SubjectPlayerID != 0 && call.SubjectPlayerID != payload.GetVisitorPlayerId()) ||
			len(payload.GetDecideTheftResultHash()) != sha256.Size {
			return status.Error(codes.FailedPrecondition, "player participant Theft visitor binding is invalid")
		}
		decision = payload.GetTheftTargetDecision()
	case *contractspb.CreditGuardIncomePayload:
		if call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME ||
			payload.GetOwnerPlayerId() == 0 || payload.GetOwnerPlayerId() != call.RouteHeader.GetActorId() ||
			(call.SubjectPlayerID != 0 && call.SubjectPlayerID != payload.GetOwnerPlayerId()) ||
			len(payload.GetDecideTheftResultHash()) != sha256.Size || len(payload.GetSettleVisitorResultHash()) != sha256.Size {
			return status.Error(codes.FailedPrecondition, "player participant guard income binding is invalid")
		}
		decision = payload.GetTheftTargetDecision()
		if decision == nil || decision.GetGuardCoinValue() == 0 {
			return status.Error(codes.FailedPrecondition, "player participant guard income requires positive guard coin")
		}
	default:
		return status.Error(codes.Internal, "player participant Theft payload schema mismatch")
	}
	if err := validateTheftDecision(call, decision); err != nil {
		return err
	}
	if call.TrustedResolvedAtMs < 0 {
		return status.Error(codes.FailedPrecondition, "player participant Theft resolved time is invalid")
	}
	return nil
}

func validateTheftDecision(call *contracts.ParticipantCall, decision *contractspb.TheftTargetDecision) error {
	if decision == nil || decision.GetWorkflowId() == "" || decision.GetWorkflowId() != call.Work.GetWorkId() ||
		decision.GetSettlementId() == "" || decision.GetTheftAttemptId() == "" ||
		decision.GetVisitorPlayerId() == 0 || decision.GetOwnerPlayerId() == 0 ||
		decision.GetVisitorPlayerId() == decision.GetOwnerPlayerId() || decision.GetFarmId() == 0 ||
		decision.GetPlotId() == 0 || decision.GetCropCycleId() == "" || decision.GetRewardItemId() == 0 ||
		decision.GetCropRuleVersion() == 0 || len(decision.GetTheftDecisionHash()) != sha256.Size {
		return status.Error(codes.FailedPrecondition, "player participant Theft decision identity is incomplete")
	}
	wantSettlement, settlementErr := identity.TheftSettlementID(call.Work.GetWorkId())
	wantAttempt, attemptErr := identity.TheftAttemptID(decision.GetFarmId(), decision.GetCropCycleId(), decision.GetVisitorPlayerId())
	if settlementErr != nil || attemptErr != nil || decision.GetSettlementId() != wantSettlement || decision.GetTheftAttemptId() != wantAttempt {
		return status.Error(codes.FailedPrecondition, "player participant Theft settlement/attempt identity is not work-derived")
	}
	if decision.GetAppliedSlotRatioBp() > 10000 || decision.GetGrossStolenQuantity() == 0 ||
		decision.GetGuardLostQuantity() > decision.GetGrossStolenQuantity() ||
		decision.GetNetReceivedQuantity() != decision.GetGrossStolenQuantity()-decision.GetGuardLostQuantity() {
		return status.Error(codes.FailedPrecondition, "player participant Theft quantity relation is inconsistent")
	}
	if decision.GetGuardLostQuantity() > 0 && decision.GetCropUnitCoinValue() > math.MaxUint64/decision.GetGuardLostQuantity() {
		return status.Error(codes.FailedPrecondition, "player participant Theft guard coin multiplication overflows")
	}
	if decision.GetGuardCoinValue() != decision.GetGuardLostQuantity()*decision.GetCropUnitCoinValue() {
		return status.Error(codes.FailedPrecondition, "player participant Theft guard coin value is inconsistent")
	}
	if decision.GetRewardItemId() > math.MaxInt32 {
		return status.Error(codes.FailedPrecondition, "player participant Theft reward item is out of range")
	}
	if _, ok := cropcatalog.ByProduceItemID(int32(decision.GetRewardItemId())); !ok {
		return status.Error(codes.FailedPrecondition, "player participant Theft reward item is not a produce item")
	}
	switch decision.GetTheftOutcome() {
	case contractspb.TheftOutcome_THEFT_OUTCOME_STOLEN:
		if decision.GetGuardLostQuantity() != 0 {
			return status.Error(codes.FailedPrecondition, "player participant Theft stolen outcome has guard loss")
		}
	case contractspb.TheftOutcome_THEFT_OUTCOME_BLOCKED_BY_GUARD:
		if decision.GetGuardLostQuantity() != decision.GetGrossStolenQuantity() || decision.GetNetReceivedQuantity() != 0 {
			return status.Error(codes.FailedPrecondition, "player participant Theft blocked outcome is inconsistent")
		}
	default:
		return status.Error(codes.FailedPrecondition, "player participant Theft outcome is not a P0 outcome")
	}
	snapshot := decision.GetGuardDecisionSnapshot()
	if snapshot == nil || snapshot.GetTheftAttemptId() != decision.GetTheftAttemptId() ||
		snapshot.GetGrossStolenQuantity() != decision.GetGrossStolenQuantity() ||
		snapshot.GetGuardLostQuantity() != decision.GetGuardLostQuantity() ||
		snapshot.GetNetReceivedQuantity() != decision.GetNetReceivedQuantity() ||
		snapshot.GetCropUnitCoinValue() != decision.GetCropUnitCoinValue() ||
		snapshot.GetGuardCoinValue() != decision.GetGuardCoinValue() ||
		snapshot.GetGuardTriggered() != (decision.GetGuardLostQuantity() > 0) {
		return status.Error(codes.FailedPrecondition, "player participant Theft guard snapshot is inconsistent")
	}
	wantHash, err := TheftDecisionHash(decision)
	if err != nil || !bytes.Equal(wantHash[:], decision.GetTheftDecisionHash()) {
		return status.Error(codes.FailedPrecondition, "player participant Theft decision hash is inconsistent")
	}
	return nil
}

func (h *plantHandler) executeTheftVisitor(ctx context.Context, call *contracts.ParticipantCall, validate func() error, payload *contractspb.SettleTheftVisitorPayload) (*contracts.StepOutcome, error) {
	return h.executeTheft(ctx, call, validate, func(rec *playerpb.PlayerRecord, currentVersion uint64) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode, error) {
		if h.taskDomainErr != nil || h.taskDomain == nil || h.taskLocation == nil {
			return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("player participant Theft TaskDomain is unavailable")
		}
		decision := payload.GetTheftTargetDecision()
		if call.TrustedResolvedAtMs <= 0 {
			return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("player participant Theft source resolved time is unavailable")
		}
		if decision.GetNetReceivedQuantity() > math.MaxInt64 {
			return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("player participant Theft net quantity exceeds PlayerRecord range")
		}
		itemID := int32(decision.GetRewardItemId())
		current := playeractor.ItemCount(rec, itemID)
		if current < 0 || decision.GetNetReceivedQuantity() > math.MaxInt64-uint64(current) {
			return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("player participant Theft inventory quantity overflows")
		}
		if decision.GetNetReceivedQuantity() > 0 && !playeractor.AddItem(rec, itemID, int64(decision.GetNetReceivedQuantity())) {
			sourceordermetrics.ObserveMailOverflow()
		}
		state, err := playertask.DomainStateFromRecord(rec)
		if err != nil {
			return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, err
		}
		fact, err := h.taskDomain.RecordFriendInteraction(&state, decision.GetTheftAttemptId(), call.TrustedResolvedAtMs, h.nowMs())
		if err != nil {
			return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, err
		}
		if err := playertask.ApplyDomainStateToRecord(rec, &state); err != nil {
			return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, err
		}
		if rec.GetWallet() == nil {
			rec.Wallet = &economypb.Wallet{}
		}
		if rec.GetEconomyVersion() == math.MaxUint64 {
			return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("player participant Theft economy version overflows")
		}
		rec.EconomyVersion++
		result := &contractspb.SettleTheftVisitorResult{
			SettlementId:                   decision.GetSettlementId(),
			RewardItemId:                   decision.GetRewardItemId(),
			GrantedNetQuantity:             decision.GetNetReceivedQuantity(),
			InventoryQuantityAfter:         uint64(playeractor.ItemCount(rec, itemID)),
			FriendInteractionFactCommitted: true,
			FriendInteractionTaskAdvanced:  fact.Status == playertask.FactApplied,
		}
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, commonpb.ErrorCode_OK, nil
	})
}

func (h *plantHandler) executeGuardIncome(ctx context.Context, call *contracts.ParticipantCall, validate func() error, payload *contractspb.CreditGuardIncomePayload) (*contracts.StepOutcome, error) {
	return h.executeTheft(ctx, call, validate, func(rec *playerpb.PlayerRecord, _ uint64) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode, error) {
		decision := payload.GetTheftTargetDecision()
		coin := decision.GetGuardCoinValue()
		if coin == 0 || coin > math.MaxInt64 {
			return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("player participant guard coin is invalid")
		}
		if rec.GetWallet() == nil {
			rec.Wallet = &economypb.Wallet{}
		}
		if rec.Wallet.GetCoin() < 0 || rec.Wallet.GetCoin() > math.MaxInt64-int64(coin) {
			return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("player participant guard coin balance overflows")
		}
		rec.Wallet.Coin += int64(coin)
		if rec.GetEconomyVersion() == math.MaxUint64 {
			return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("player participant guard income economy version overflows")
		}
		rec.EconomyVersion++
		return &contractspb.CreditGuardIncomeResult{
			SettlementId:           decision.GetSettlementId(),
			CreditedNormalCoin:     coin,
			NormalCoinBalanceAfter: uint64(rec.Wallet.GetCoin()),
		}, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, commonpb.ErrorCode_OK, nil
	})
}

type theftTurnDecision func(*playerpb.PlayerRecord, uint64) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode, error)

func (h *plantHandler) executeTheft(ctx context.Context, call *contracts.ParticipantCall, validate func() error, decide theftTurnDecision) (*contracts.StepOutcome, error) {
	if h.registry == nil {
		return nil, status.Error(codes.Unavailable, "player actor registry unavailable")
	}
	if err := contracts.CheckParticipantBudget(ctx, call); err != nil {
		return nil, err
	}
	budgetCtx, cancel := context.WithTimeout(ctx, time.Duration(call.RemainingBudgetMs)*time.Millisecond)
	defer cancel()
	result, err := h.registry.SubmitParticipantVersionedWithOutcomeFromAdmission(budgetCtx, call.RouteHeader.GetActorId(), call.AdmissionRecordVersion, func(rec *playerpb.PlayerRecord, currentVersion uint64, guard playeractor.ParticipantCapacityGuard) (any, bool, authoritystore.StoredOperationOutcome, error) {
		if receipt := playeringress.FindParticipantOperationReceipt(rec, call.InvocationID); receipt != nil {
			if !bytes.Equal(receipt.GetPayloadHash(), call.CanonicalStepPayloadHash[:]) {
				return nil, false, authoritystore.StoredOperationOutcome{}, status.Error(codes.FailedPrecondition, "PARTICIPANT_IDEMPOTENCY_CONFLICT")
			}
			outcome, typedResult, resultHash, failure, committedVersion, decodeErr := decodeTheftReceipt(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash[:], receipt)
			if decodeErr != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, decodeErr
			}
			if err := validateReceiptRecordVersion(committedVersion, currentVersion); err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, err
			}
			return &contracts.StepOutcome{Outcome: outcome, ReplayedFromReceipt: true, DurableReceiptCommitted: true, CommittedResultHash: resultHash, Result: typedResult, Failure: failure, CommittedRecordVersion: committedVersion}, false, authoritystore.StoredOperationOutcome{}, nil
		}
		if err := contracts.CheckParticipantBudget(budgetCtx, call); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		if err := validate(); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		capacityNowMs := h.nowMs()
		if guard.PhysicalProjectionRequired() {
			estimate, err := playeringress.EstimateParticipantRecordCapacityAt(rec, call.InvocationID, call.CanonicalStepPayloadHash[:], maxTheftResultProofEnvelopeBytes(call.InvocationID, call.Spec.Command), capacityNowMs)
			if err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, err
			}
			if err := playeringress.CheckParticipantRecordCapacity(estimate); err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, participantReceiptCapacityError()
			}
			if err := guard.CheckWithOutcome(estimate.ModelBytes, theftOperationResultCapacityEstimate(call.Spec.Command)); err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, participantReceiptCapacityError()
			}
		}
		if err := playeringress.EnsureParticipantOperationReceiptCapacity(rec, capacityNowMs); err != nil {
			if errors.Is(err, playeringress.ErrParticipantReceiptCapacity) {
				return nil, false, authoritystore.StoredOperationOutcome{}, participantReceiptCapacityError()
			}
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		typedResult, outcome, errorCode, decisionErr := decide(rec, currentVersion)
		if decisionErr != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, status.Error(codes.Internal, decisionErr.Error())
		}
		if typedResult == nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, status.Error(codes.Internal, "player participant Theft returned nil typed result")
		}
		committedVersion := currentVersion + 1
		if err := setTheftRecordVersion(typedResult, committedVersion); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		resultProof, resultHash, err := encodeParticipantResultProof(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash, typedResult)
		if err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		typedResultBytes, _, err := contracts.ValidateTypedResult(call.Spec, typedResult)
		if err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		storedOutcome := theftStoredOperationOutcome(outcome, errorCode, typedResultBytes)
		if guard.PhysicalProjectionRequired() {
			candidate := proto.Clone(rec).(*playerpb.PlayerRecord)
			playeringress.AppendParticipantOperationReceiptAt(candidate, call.InvocationID, call.CanonicalStepPayloadHash[:], resultProof, commandStatus(outcome), errorCode, h.nowMs())
			if err := guard.CheckWithOutcome(len("record")+proto.Size(candidate), storedOutcome); err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, participantReceiptCapacityError()
			}
		}
		playeringress.AppendParticipantOperationReceiptAt(rec, call.InvocationID, call.CanonicalStepPayloadHash[:], resultProof, commandStatus(outcome), errorCode, h.nowMs())
		return &contracts.StepOutcome{Outcome: outcome, DurableReceiptCommitted: true, CommittedResultHash: resultHash, Result: typedResult, Failure: plantFailure(errorCode), CommittedRecordVersion: committedVersion}, true, storedOutcome, nil
	})
	if err != nil {
		if errorsIsTimeoutUnknown(err) {
			return &contracts.StepOutcome{Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN, Failure: &contractspb.ParticipantFailure{Error: &logicrpcpb.ErrorDetail{ErrorCode: commonpb.ErrorCode_ERR_TIMEOUT}, RetryableAfterResolution: true}}, nil
		}
		if errors.Is(err, blobstore.ErrWriterEpochMismatch) {
			return nil, status.Error(codes.FailedPrecondition, "WRITER_EPOCH_MISMATCH")
		}
		if errors.Is(err, blobstore.ErrCASConflict) {
			return nil, status.Error(codes.Aborted, "RECORD_VERSION_CONFLICT")
		}
		if errors.Is(err, blobstore.ErrUnavailable) {
			return nil, status.Error(codes.Unavailable, "PLAYER_AUTHORITY_UNAVAILABLE")
		}
		return nil, normalizeParticipantError(err)
	}
	outcome, ok := result.Value.(*contracts.StepOutcome)
	if !ok || outcome == nil {
		return nil, status.Error(codes.Internal, "player participant Theft returned invalid turn result")
	}
	return outcome, nil
}

func setTheftRecordVersion(result proto.Message, version uint64) error {
	switch result := result.(type) {
	case *contractspb.SettleTheftVisitorResult:
		result.PlayerRecordVersion = version
	case *contractspb.CreditGuardIncomeResult:
		result.PlayerRecordVersion = version
	default:
		return status.Error(codes.Internal, "player participant Theft result has no PlayerRecordVersion")
	}
	return nil
}

func theftRecordVersion(result proto.Message) uint64 {
	switch result := result.(type) {
	case *contractspb.SettleTheftVisitorResult:
		return result.GetPlayerRecordVersion()
	case *contractspb.CreditGuardIncomeResult:
		return result.GetPlayerRecordVersion()
	default:
		return 0
	}
}

func decodeTheftReceipt(spec contracts.ParticipantCommandSpec, expectedInvocationID string, expectedStepHash []byte, receipt *playerpb.PlayerOperationReceipt) (contractspb.ParticipantOutcome, proto.Message, []byte, *contractspb.ParticipantFailure, uint64, error) {
	var outcome contractspb.ParticipantOutcome
	switch receipt.GetStatus() {
	case commonpb.CommandStatus_SUCCEEDED:
		outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED
	case commonpb.CommandStatus_REJECTED:
		outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED
	case commonpb.CommandStatus_NO_CHANGE:
		outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE
	default:
		return 0, nil, nil, nil, 0, status.Error(codes.Internal, "player participant Theft receipt has non-terminal status")
	}
	if len(receipt.GetResultPayload()) == 0 || !bytes.Equal(receipt.GetPayloadHash(), expectedStepHash) {
		return 0, nil, nil, nil, 0, status.Error(codes.Internal, "player participant Theft receipt/proof binding is corrupt")
	}
	result, resultHash, err := decodeParticipantResultProof(spec, expectedInvocationID, expectedStepHash, receipt.GetResultPayload())
	if err != nil {
		return 0, nil, nil, nil, 0, err
	}
	version := theftRecordVersion(result)
	if version == 0 {
		return 0, nil, nil, nil, 0, status.Error(codes.Internal, "player participant Theft receipt missing record version")
	}
	var failure *contractspb.ParticipantFailure
	if receipt.GetErrorCode() != commonpb.ErrorCode_OK && receipt.GetErrorCode() != commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED {
		failure = plantFailure(receipt.GetErrorCode())
	}
	return outcome, result, resultHash, failure, version, nil
}
