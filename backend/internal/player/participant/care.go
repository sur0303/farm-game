package participant

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"strconv"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	playeractor "farmgame/internal/player/actor"
	playeringress "farmgame/internal/player/ingress"
	playertask "farmgame/internal/player/task"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	// The legacy PlayerRecord has no separately generated CareRewardReceipt
	// message. The existing namespaced ParticipantReceipt is therefore the
	// durable CareRewardReceipt carrier too: its typed result contains the
	// order/incident identity and is committed with the reward.
	maxCareTypedResultBytes = 512
)

func isCarePlayerCommand(call *contracts.ParticipantCall) bool {
	return call != nil && call.Spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD
}

// CareResolutionHash computes the frozen hash over every CareResolutionSnapshot
// semantic field except care_resolution_hash itself. It is deliberately the
// generic contract canonical hash, not a protobuf marshal hash.
func CareResolutionHash(snapshot *contractspb.CareResolutionSnapshot) ([32]byte, error) {
	if snapshot == nil {
		return [32]byte{}, status.Error(codes.InvalidArgument, "care resolution snapshot is required")
	}
	copyOf, ok := proto.Clone(snapshot).(*contractspb.CareResolutionSnapshot)
	if !ok {
		return [32]byte{}, status.Error(codes.Internal, "care resolution snapshot clone failed")
	}
	copyOf.CareResolutionHash = nil
	return contracts.CanonicalHash(copyOf)
}

// validateCarePayload is intentionally pure. The only accepted reward inputs
// are the already-frozen Order identity and CareResolutionSnapshot; there is
// no Farm/config read here or in the actor turn.
func validateCarePayload(call *contracts.ParticipantCall) error {
	if call == nil || call.RouteHeader == nil || call.Work == nil {
		return status.Error(codes.InvalidArgument, "player participant Care call is incomplete")
	}
	if call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD {
		return status.Error(codes.InvalidArgument, "player participant Care command mismatch")
	}
	payload, ok := call.Payload.(*contractspb.ApplyCareRewardPayload)
	if !ok || payload == nil {
		return status.Error(codes.Internal, "player participant Care payload schema mismatch")
	}
	if call.RouteHeader.GetActorId() == 0 || payload.GetCarerPlayerId() == 0 ||
		payload.GetCarerPlayerId() != call.RouteHeader.GetActorId() ||
		(call.SubjectPlayerID != 0 && call.SubjectPlayerID != payload.GetCarerPlayerId()) {
		return status.Error(codes.FailedPrecondition, "player participant Care target player binding is invalid")
	}
	if payload.GetCareRewardOrderId() == "" || payload.GetCareRewardOrderId() != call.Work.GetWorkId() {
		return status.Error(codes.FailedPrecondition, "player participant Care order binding is invalid")
	}
	if len(payload.GetCareResolutionHash()) != sha256.Size {
		return status.Error(codes.FailedPrecondition, "player participant Care resolution hash is invalid")
	}
	snapshot := payload.GetCareResolutionSnapshot()
	if err := validateCareSnapshot(snapshot, payload.GetCarerPlayerId(), payload.GetCareResolutionHash(), call); err != nil {
		return err
	}
	return nil
}

func validateCareSnapshot(snapshot *contractspb.CareResolutionSnapshot, targetPlayerID uint64, payloadHash []byte, call *contracts.ParticipantCall) error {
	if snapshot == nil || snapshot.GetSourceFarmId() == 0 || snapshot.GetSourceOwnerPlayerId() == 0 ||
		snapshot.GetCarerPlayerId() == 0 || snapshot.GetCarerPlayerId() != targetPlayerID ||
		snapshot.GetPlotId() == 0 || snapshot.GetCropCycleId() == "" || snapshot.GetIncidentId() == "" ||
		snapshot.GetResolvedAtMs() <= 0 || snapshot.GetCommittedPlotVersion() == 0 ||
		snapshot.GetCommittedFarmVersion() == 0 || snapshot.GetCareRewardRuleVersion() == 0 ||
		len(snapshot.GetCareResolutionHash()) != sha256.Size || len(payloadHash) != sha256.Size {
		return status.Error(codes.FailedPrecondition, "player participant Care snapshot fields are invalid")
	}
	if !bytes.Equal(snapshot.GetCareResolutionHash(), payloadHash) {
		return status.Error(codes.FailedPrecondition, "player participant Care payload/snapshot hash mismatch")
	}
	wantHash, err := CareResolutionHash(snapshot)
	if err != nil || !bytes.Equal(wantHash[:], snapshot.GetCareResolutionHash()) {
		return status.Error(codes.FailedPrecondition, "player participant Care resolution hash is inconsistent")
	}
	source := (*contractspb.SourceAuthorityRef)(nil)
	if call != nil && call.Work != nil {
		source = call.Work.GetSourceAuthority()
	}
	if source == nil || source.GetAuthorityKind() != contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_FARM_ACTOR ||
		source.GetActorRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM || source.GetAuthorityLayoutVersion() == "" ||
		source.GetCanonicalAuthorityId() != strconv.FormatUint(snapshot.GetSourceFarmId(), 10) {
		return status.Error(codes.FailedPrecondition, "player participant Care source farm binding is invalid")
	}
	switch snapshot.GetCareType() {
	case contractspb.CareType_CARE_TYPE_WATER, contractspb.CareType_CARE_TYPE_REMOVE_PEST:
	default:
		return status.Error(codes.FailedPrecondition, "player participant Care type is invalid")
	}
	switch snapshot.GetCareContext() {
	case contractspb.CareContext_CARE_CONTEXT_OWNER:
		if snapshot.GetCarerPlayerId() != snapshot.GetSourceOwnerPlayerId() || snapshot.GetFriendInteractionEligible() {
			return status.Error(codes.FailedPrecondition, "player participant Care owner snapshot is inconsistent")
		}
	case contractspb.CareContext_CARE_CONTEXT_FRIEND:
		if snapshot.GetCarerPlayerId() == snapshot.GetSourceOwnerPlayerId() || !snapshot.GetFriendInteractionEligible() {
			return status.Error(codes.FailedPrecondition, "player participant Care friend snapshot is inconsistent")
		}
	default:
		return status.Error(codes.FailedPrecondition, "player participant Care context is invalid")
	}
	return nil
}

// validateCareDependencyProof binds the structured Farm SourceOrder evidence
// to the already-decoded Participant call. This is the last admission check
// before the PlayerActor mailbox; it never performs a Farm read itself.
func validateCareDependencyProof(call *contracts.ParticipantCall, proof CareDependencyProof) error {
	if call == nil || call.Work == nil {
		return status.Error(codes.InvalidArgument, "player participant Care dependency call is incomplete")
	}
	payload, ok := call.Payload.(*contractspb.ApplyCareRewardPayload)
	if !ok || payload == nil || payload.GetCareResolutionSnapshot() == nil {
		return status.Error(codes.Internal, "player participant Care dependency payload schema mismatch")
	}
	snapshot := payload.GetCareResolutionSnapshot()
	if proof.Snapshot == nil {
		return status.Error(codes.FailedPrecondition, "player participant Care source-order snapshot proof is missing")
	}
	if !proof.SourceOrderFound || proof.SourceOrderID == "" || proof.SourceOrderID != payload.GetCareRewardOrderId() ||
		proof.SourceOrderType != contractspb.WorkType_WORK_TYPE_CARE_REWARD ||
		proof.SourceOrderID != call.Work.GetWorkId() ||
		!proto.Equal(proof.SourceAuthority, call.Work.GetSourceAuthority()) ||
		!proto.Equal(proof.Snapshot, snapshot) ||
		!bytes.Equal(proof.SnapshotHash, payload.GetCareResolutionHash()) ||
		len(proof.SnapshotHash) != sha256.Size ||
		!bytes.Equal(proof.Snapshot.GetCareResolutionHash(), proof.SnapshotHash) ||
		proof.WorkVersion != call.ExpectedWorkVersion || proof.InvocationID != call.InvocationID ||
		proof.ParticipantStep != call.ParticipantStep || !proof.DueIndexBound || !proof.FenceValid {
		return status.Error(codes.FailedPrecondition, "player participant Care source-order proof binding is invalid")
	}
	wantHash, err := CareResolutionHash(proof.Snapshot)
	if err != nil || !bytes.Equal(wantHash[:], proof.SnapshotHash) {
		return status.Error(codes.FailedPrecondition, "player participant Care source-order snapshot hash proof is invalid")
	}
	wantOrderID, err := identity.SourceOrderID(
		identity.WorkTypeCareReward,
		identity.SourceKindFarmActor,
		strconv.FormatUint(snapshot.GetSourceFarmId(), 10),
		snapshot.GetIncidentId(), snapshot.GetCommittedFarmVersion(),
	)
	if err != nil || wantOrderID != proof.SourceOrderID {
		return status.Error(codes.FailedPrecondition, "player participant Care source-order deterministic identity is invalid")
	}
	switch proof.OrderState {
	case CareRewardOrderStatePending, CareRewardOrderStateRewardDispatching:
	default:
		return status.Error(codes.FailedPrecondition, "player participant Care source-order state is not dispatchable")
	}
	// PENDING is the only unleased DirectDispatcher/Coordinator admission;
	// REWARD_DISPATCHING is the claimed Worker recovery state. This prevents a
	// caller from relabeling an active lease as a fresh coordinator delivery.
	switch call.CallerKind {
	case contracts.ParticipantCallerCoordinator:
		if proof.OrderState != CareRewardOrderStatePending || proof.ExecutionMode != CareDependencyExecutionModeCoordinator || proof.LeaseValid || proof.LeaseOwnerToken != "" || proof.LeaseEpoch != 0 || proof.LeaseExpireAtMs != 0 {
			return status.Error(codes.FailedPrecondition, "player participant Care coordinator invocation fence is invalid")
		}
	case contracts.ParticipantCallerWorkerRecovery:
		if proof.OrderState != CareRewardOrderStateRewardDispatching || proof.ExecutionMode != CareDependencyExecutionModeRecoveryLease || !proof.LeaseValid || proof.LeaseOwnerToken == "" ||
			proof.LeaseOwnerToken != call.LeaseOwnerToken || proof.LeaseEpoch == 0 || proof.LeaseEpoch != call.LeaseEpoch || proof.LeaseExpireAtMs <= 0 {
			return status.Error(codes.FailedPrecondition, "player participant Care recovery lease proof is invalid")
		}
	default:
		return status.Error(codes.InvalidArgument, "player participant Care caller kind is invalid")
	}
	return nil
}

func careOperationResultCapacityEstimate() authoritystore.StoredOperationOutcome {
	return authoritystore.StoredOperationOutcome{
		Status:  authoritystore.TerminalSucceeded,
		Payload: make([]byte, maxCareTypedResultBytes),
	}
}

func careStoredOperationOutcome(outcome contractspb.ParticipantOutcome, errorCode commonpb.ErrorCode, typedResult []byte) authoritystore.StoredOperationOutcome {
	statusValue := authoritystore.TerminalRejected
	switch outcome {
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED:
		statusValue = authoritystore.TerminalSucceeded
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE:
		statusValue = authoritystore.TerminalNoChange
	}
	errorCodeText := ""
	if errorCode != commonpb.ErrorCode_OK && errorCode != commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED {
		errorCodeText = errorCode.String()
	}
	return authoritystore.StoredOperationOutcome{Status: statusValue, ErrorCode: errorCodeText, Payload: append([]byte(nil), typedResult...)}
}

func validateCareReceiptBinding(payload *contractspb.ApplyCareRewardPayload, result proto.Message) error {
	careResult, ok := result.(*contractspb.ApplyCareRewardResult)
	if !ok || payload == nil || payload.GetCareResolutionSnapshot() == nil {
		return status.Error(codes.Internal, "player participant Care receipt/result binding is corrupt")
	}
	snapshot := payload.GetCareResolutionSnapshot()
	if careResult.GetCareRewardOrderId() != payload.GetCareRewardOrderId() ||
		careResult.GetIncidentId() != snapshot.GetIncidentId() ||
		careResult.GetGrantedExp() != snapshot.GetRewardExp() ||
		careResult.GetFriendInteractionFactCommitted() != (snapshot.GetCareContext() == contractspb.CareContext_CARE_CONTEXT_FRIEND && snapshot.GetFriendInteractionEligible()) ||
		careResult.GetTaskPeriodKey() == "" {
		return status.Error(codes.Internal, "player participant Care receipt/result binding is corrupt")
	}
	return nil
}

func (h *plantHandler) executeCare(ctx context.Context, call *contracts.ParticipantCall, validate func() error, payload *contractspb.ApplyCareRewardPayload) (*contracts.StepOutcome, error) {
	if h.registry == nil {
		return nil, status.Error(codes.Unavailable, "player actor registry unavailable")
	}
	if h.taskTimezoneErr != nil || h.taskLocation == nil {
		return nil, status.Error(codes.FailedPrecondition, "player participant Care task timezone is unavailable")
	}
	if h.taskDomainErr != nil || h.taskDomain == nil {
		return nil, status.Error(codes.FailedPrecondition, "player participant Care TaskDomain configuration is unavailable")
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
			outcome, typedResult, resultHash, failure, committedVersion, decodeErr := decodeCareReceipt(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash[:], receipt)
			if decodeErr != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, decodeErr
			}
			if err := validateReceiptRecordVersion(committedVersion, currentVersion); err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, err
			}
			if err := validateCareReceiptBinding(payload, typedResult); err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, err
			}
			return &contracts.StepOutcome{Outcome: outcome, ReplayedFromReceipt: true, DurableReceiptCommitted: true, CommittedResultHash: resultHash, Result: typedResult, Failure: failure, CommittedRecordVersion: committedVersion}, false, authoritystore.StoredOperationOutcome{}, nil
		}
		if err := contracts.CheckParticipantBudget(budgetCtx, call); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		// Semantic validation deliberately follows the receipt lookup. A used
		// invocation with a changed canonical hash is an idempotency conflict,
		// even if the changed snapshot is otherwise malformed.
		if err := validate(); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		if err := contracts.CheckParticipantBudget(budgetCtx, call); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		turnNowMs := h.nowMs()
		capacityNowMs := turnNowMs
		if guard.PhysicalProjectionRequired() {
			estimate, estimateErr := playeringress.EstimateParticipantRecordCapacityAt(rec, call.InvocationID, call.CanonicalStepPayloadHash[:], maxCareResultProofEnvelopeBytes(call.InvocationID), capacityNowMs)
			if estimateErr != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, estimateErr
			}
			if err := playeringress.CheckParticipantRecordCapacity(estimate); err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, participantReceiptCapacityError()
			}
			if err := guard.CheckWithOutcome(estimate.ModelBytes, careOperationResultCapacityEstimate()); err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, participantReceiptCapacityError()
			}
		}
		if err := playeringress.EnsureParticipantOperationReceiptCapacity(rec, capacityNowMs); err != nil {
			if errors.Is(err, playeringress.ErrParticipantReceiptCapacity) {
				return nil, false, authoritystore.StoredOperationOutcome{}, participantReceiptCapacityError()
			}
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		if err := contracts.CheckParticipantBudget(budgetCtx, call); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		careResult, outcome, errorCode, decisionErr := h.applyCareReward(rec, payload, turnNowMs)
		if decisionErr != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, status.Error(codes.Internal, decisionErr.Error())
		}
		committedVersion := currentVersion + 1
		careResult.PlayerRecordVersion = committedVersion
		resultProof, resultHash, proofErr := encodeParticipantResultProof(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash, careResult)
		if proofErr != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, proofErr
		}
		typedResultBytes, _, typedResultErr := contracts.ValidateTypedResult(call.Spec, careResult)
		if typedResultErr != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, typedResultErr
		}
		storedOutcome := careStoredOperationOutcome(outcome, errorCode, typedResultBytes)
		if guard.PhysicalProjectionRequired() {
			candidate := proto.Clone(rec).(*playerpb.PlayerRecord)
			playeringress.AppendParticipantOperationReceiptAt(candidate, call.InvocationID, call.CanonicalStepPayloadHash[:], resultProof, commandStatus(outcome), errorCode, capacityNowMs)
			candidateModelBytes := len("record") + proto.Size(candidate)
			if err := guard.CheckWithOutcome(candidateModelBytes, storedOutcome); err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, participantReceiptCapacityError()
			}
		}
		if err := contracts.CheckParticipantBudget(budgetCtx, call); err != nil {
			return nil, false, authoritystore.StoredOperationOutcome{}, err
		}
		playeringress.AppendParticipantOperationReceiptAt(rec, call.InvocationID, call.CanonicalStepPayloadHash[:], resultProof, commandStatus(outcome), errorCode, capacityNowMs)
		return &contracts.StepOutcome{Outcome: outcome, DurableReceiptCommitted: true, CommittedResultHash: resultHash, Result: careResult, Failure: plantFailure(errorCode), CommittedRecordVersion: committedVersion}, true, storedOutcome, nil
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
		return nil, status.Error(codes.Internal, "player participant Care returned invalid turn result")
	}
	return outcome, nil
}

func (h *plantHandler) applyCareReward(rec *playerpb.PlayerRecord, payload *contractspb.ApplyCareRewardPayload, nowMs int64) (*contractspb.ApplyCareRewardResult, contractspb.ParticipantOutcome, commonpb.ErrorCode, error) {
	if rec == nil || rec.GetProgression() == nil || payload == nil || payload.GetCareResolutionSnapshot() == nil {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("player participant Care progression is unavailable")
	}
	snapshot := payload.GetCareResolutionSnapshot()
	if rec.Progression.Exp < 0 {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("player participant Care progression experience is invalid")
	}
	if snapshot.GetRewardExp() > math.MaxInt64 {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("player participant Care experience exceeds PlayerRecord integer range")
	}
	rewardExp := int64(snapshot.GetRewardExp())
	if rec.Progression.Exp > math.MaxInt64-rewardExp {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("player participant Care experience overflow")
	}
	newExp := rec.Progression.Exp + rewardExp
	level64 := newExp/20 + 1
	if level64 > math.MaxInt32 {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, errors.New("player participant Care level overflow")
	}
	periodKey := playertask.DailyPeriodID(snapshot.GetResolvedAtMs(), h.taskLocation)
	taskAdvanced := false
	if snapshot.GetCareContext() == contractspb.CareContext_CARE_CONTEXT_FRIEND && snapshot.GetFriendInteractionEligible() {
		taskState, err := playertask.DomainStateFromRecord(rec)
		if err != nil {
			return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, err
		}
		fact, err := h.taskDomain.RecordFriendInteraction(&taskState, payload.GetCareRewardOrderId(), snapshot.GetResolvedAtMs(), nowMs)
		if err != nil {
			return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, err
		}
		if err := playertask.ApplyDomainStateToRecord(rec, &taskState); err != nil {
			return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, err
		}
		taskAdvanced = fact.Status == playertask.FactApplied
	}
	rec.Progression.Exp = newExp
	rec.Progression.Level = int32(level64)
	rec.EconomyVersion++
	return &contractspb.ApplyCareRewardResult{
		CareRewardOrderId:              payload.GetCareRewardOrderId(),
		IncidentId:                     snapshot.GetIncidentId(),
		GrantedExp:                     snapshot.GetRewardExp(),
		PlayerExpAfter:                 uint64(newExp),
		PlayerLevelAfter:               uint32(level64),
		FriendInteractionFactCommitted: snapshot.GetCareContext() == contractspb.CareContext_CARE_CONTEXT_FRIEND && snapshot.GetFriendInteractionEligible(),
		FriendInteractionTaskAdvanced:  taskAdvanced,
		TaskPeriodKey:                  periodKey,
	}, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, commonpb.ErrorCode_OK, nil
}

func maxCareResultProofEnvelopeBytes(invocationID string) int {
	return participantResultProofFixedBytes + len(invocationID) + maxCareTypedResultBytes
}

func decodeCareReceipt(spec contracts.ParticipantCommandSpec, expectedInvocationID string, expectedStepHash []byte, receipt *playerpb.PlayerOperationReceipt) (contractspb.ParticipantOutcome, proto.Message, []byte, *contractspb.ParticipantFailure, uint64, error) {
	var outcome contractspb.ParticipantOutcome
	switch receipt.GetStatus() {
	case commonpb.CommandStatus_SUCCEEDED:
		outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED
	case commonpb.CommandStatus_REJECTED:
		outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED
	case commonpb.CommandStatus_NO_CHANGE:
		outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE
	default:
		return 0, nil, nil, nil, 0, status.Error(codes.Internal, "player participant Care receipt has non-terminal status")
	}
	if len(receipt.GetResultPayload()) == 0 || !bytes.Equal(receipt.GetPayloadHash(), expectedStepHash) {
		return 0, nil, nil, nil, 0, status.Error(codes.Internal, "player participant Care receipt/proof binding is corrupt")
	}
	result, resultHash, err := decodeParticipantResultProof(spec, expectedInvocationID, expectedStepHash, receipt.GetResultPayload())
	if err != nil {
		return 0, nil, nil, nil, 0, err
	}
	careResult, ok := result.(*contractspb.ApplyCareRewardResult)
	if !ok || careResult.GetCareRewardOrderId() == "" || careResult.GetIncidentId() == "" || careResult.GetPlayerRecordVersion() == 0 {
		return 0, nil, nil, nil, 0, status.Error(codes.Internal, "player participant Care receipt result is malformed")
	}
	var failure *contractspb.ParticipantFailure
	if receipt.GetErrorCode() != commonpb.ErrorCode_OK && receipt.GetErrorCode() != commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED {
		failure = &contractspb.ParticipantFailure{Error: &logicrpcpb.ErrorDetail{ErrorCode: receipt.GetErrorCode()}}
	}
	return outcome, result, resultHash, failure, careResult.GetPlayerRecordVersion(), nil
}
