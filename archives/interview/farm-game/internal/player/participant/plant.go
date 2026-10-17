package participant

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"strings"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	"farmgame/internal/contracts"
	"farmgame/internal/cropcatalog"
	"farmgame/internal/identity"
	playeractor "farmgame/internal/player/actor"
	playeringress "farmgame/internal/player/ingress"
	playertask "farmgame/internal/player/task"
	"farmgame/internal/sourceordermetrics"
	"farmgame/internal/storage/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type plantHandler struct {
	registry        *playeractor.Registry
	nowMs           func() int64
	taskLocation    *time.Location
	taskTimezoneErr error
	taskDomain      *playertask.Domain
	taskDomainErr   error
}

func newPlantHandler(registry *playeractor.Registry, nowMs func() int64) Handler {
	return newPlayerHandler(registry, nowMs, playertask.DefaultGameTimezone, nil)
}

func newPlayerHandler(registry *playeractor.Registry, nowMs func() int64, taskTimezone string, taskRepository playertask.Repository) Handler {
	if nowMs == nil {
		nowMs = playeractor.NowMs
	}
	if taskTimezone == "" {
		taskTimezone = playertask.DefaultGameTimezone
	}
	location, err := time.LoadLocation(taskTimezone)
	handler := &plantHandler{registry: registry, nowMs: nowMs, taskLocation: location, taskTimezoneErr: err}
	if err != nil {
		handler.taskDomainErr = err
	} else if taskRepository == nil {
		handler.taskDomainErr = errors.New("player participant TaskConfigRepository is not configured")
	} else {
		handler.taskDomain, handler.taskDomainErr = playertask.NewDomain(taskRepository, taskTimezone)
	}
	return handler
}

// plantDecision is evaluated inside the PlayerActor Turn. A decision always
// returns the first typed result for the invocation; executePlant then stores
// that exact bytes value in PlayerRecord.RecentOperationReceipts.
type plantDecision func(*playerpb.PlayerRecord, uint64) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode)

func (h *plantHandler) Handle(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, error) {
	if call == nil || call.RouteHeader == nil || call.Work == nil {
		return nil, status.Error(codes.InvalidArgument, "player participant call is incomplete")
	}
	switch call.Spec.Command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_UNLOCK_COIN:
		payload, ok := call.Payload.(*contractspb.ReserveUnlockCoinPayload)
		if !ok {
			return nil, status.Error(codes.Internal, "player participant reserve unlock payload schema mismatch")
		}
		if unlockSourcePlacement(call) {
			return h.executeUnlockSource(ctx, call, payload)
		}
		return h.executeUnlock(ctx, call, func() error { return validateReserveUnlockCoinPayload(call, payload) }, func(rec *playerpb.PlayerRecord, version uint64) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode) {
			return h.reserveUnlockCoin(rec, call, payload)
		})
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_UNLOCK_COIN_RESERVATION:
		payload, ok := call.Payload.(*contractspb.CommitUnlockCoinReservationPayload)
		if !ok {
			return nil, status.Error(codes.Internal, "player participant commit unlock payload schema mismatch")
		}
		return h.executeUnlock(ctx, call, func() error { return validateCommitUnlockCoinReservationPayload(call, payload) }, func(rec *playerpb.PlayerRecord, version uint64) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode) {
			return h.commitUnlockCoinReservation(rec, payload)
		})
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_UNLOCK_COIN_RESERVATION:
		payload, ok := call.Payload.(*contractspb.ReleaseUnlockCoinReservationPayload)
		if !ok {
			return nil, status.Error(codes.Internal, "player participant release unlock payload schema mismatch")
		}
		return h.executeUnlock(ctx, call, func() error { return validateReleaseUnlockCoinReservationPayload(call, payload) }, func(rec *playerpb.PlayerRecord, version uint64) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode) {
			return h.releaseUnlockCoinReservation(rec, payload)
		})
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED:
		payload, ok := call.Payload.(*contractspb.ReserveSeedPayload)
		if !ok {
			return nil, status.Error(codes.Internal, "player participant reserve payload schema mismatch")
		}
		if plantSourcePlacement(call) {
			return h.executePlantSource(ctx, call, payload)
		}
		return h.executePlant(ctx, call, func() error { return validateReserveSeedPayload(call, payload) }, func(rec *playerpb.PlayerRecord, version uint64) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode) {
			return h.reserveSeed(rec, call, payload)
		})
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_SEED_RESERVATION:
		payload, ok := call.Payload.(*contractspb.CommitSeedReservationPayload)
		if !ok {
			return nil, status.Error(codes.Internal, "player participant commit payload schema mismatch")
		}
		return h.executePlant(ctx, call, func() error { return validateCommitSeedReservationPayload(call, payload) }, func(rec *playerpb.PlayerRecord, version uint64) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode) {
			return h.commitSeedReservation(rec, call, payload)
		})
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_SEED_RESERVATION:
		payload, ok := call.Payload.(*contractspb.ReleaseSeedReservationPayload)
		if !ok {
			return nil, status.Error(codes.Internal, "player participant release payload schema mismatch")
		}
		return h.executePlant(ctx, call, func() error { return validateReleaseSeedReservationPayload(call, payload) }, func(rec *playerpb.PlayerRecord, version uint64) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode) {
			return h.releaseSeedReservation(rec, call, payload)
		})
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD:
		return h.executeHarvest(ctx, call, func() error { return validateHarvestPayload(call) })
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR:
		payload, ok := call.Payload.(*contractspb.SettleTheftVisitorPayload)
		if !ok {
			return nil, status.Error(codes.Internal, "player participant Theft visitor payload schema mismatch")
		}
		return h.executeTheftVisitor(ctx, call, func() error { return validateTheftPayload(call) }, payload)
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME:
		payload, ok := call.Payload.(*contractspb.CreditGuardIncomePayload)
		if !ok {
			return nil, status.Error(codes.Internal, "player participant guard income payload schema mismatch")
		}
		return h.executeGuardIncome(ctx, call, func() error { return validateTheftPayload(call) }, payload)
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD:
		payload, ok := call.Payload.(*contractspb.ApplyCareRewardPayload)
		if !ok {
			return nil, status.Error(codes.Internal, "player participant Care payload schema mismatch")
		}
		return h.executeCare(ctx, call, func() error { return validateCarePayload(call) }, payload)
	default:
		// Player commands other than the three Plant steps and Harvest reward
		// are deliberately unimplemented. They must not return a fabricated
		// successful result or touch PlayerRecord.
		return nil, status.Error(codes.Unimplemented, "player participant command is not implemented")
	}
}

// ResolveParticipantReceipt is the durable preflight used by Server before
// WorkVersion/WorkLease verification. It reads the same PlayerRecord receipt
// carrier used by executePlant; a same-hash terminal retry therefore returns
// the original typed result even when its Work/Lease context is stale.
func (h *plantHandler) ResolveParticipantReceipt(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error) {
	if !isPlantPlayerCommand(call) && !isUnlockPlayerCommand(call) && !isHarvestPlayerCommand(call) && !isTheftPlayerCommand(call) && !isCarePlayerCommand(call) {
		return nil, false, nil
	}
	if h.registry == nil {
		return nil, false, status.Error(codes.Unavailable, "player actor registry unavailable")
	}
	// Receipt replay is an admission read, not a PlayerActor turn. The direct
	// registry seam preserves the writer fence and exact record version without
	// putting a Redis/Authority wait inside the mailbox.
	rec, recordVersion, err := h.registry.ReadOutsideMailbox(ctx, call.RouteHeader.GetActorId())
	if err != nil {
		if errors.Is(err, blobstore.ErrWriterEpochMismatch) {
			return nil, false, status.Error(codes.FailedPrecondition, "WRITER_EPOCH_MISMATCH")
		}
		if errors.Is(err, blobstore.ErrUnavailable) {
			return nil, false, status.Error(codes.Unavailable, "PLAYER_AUTHORITY_UNAVAILABLE")
		}
		return nil, false, err
	}
	receipt := playeringress.FindParticipantOperationReceipt(rec, call.InvocationID)
	if receipt == nil {
		nowMs := h.nowMs()
		proofBytes := maxPlantResultProofEnvelopeBytes(call.InvocationID)
		if isUnlockPlayerCommand(call) {
			proofBytes = maxUnlockResultProofEnvelopeBytes(call.InvocationID)
		}
		if isHarvestPlayerCommand(call) {
			proofBytes = maxHarvestResultProofEnvelopeBytes(call.InvocationID)
		} else if isTheftPlayerCommand(call) {
			proofBytes = maxTheftResultProofEnvelopeBytes(call.InvocationID, call.Spec.Command)
		} else if isCarePlayerCommand(call) {
			proofBytes = maxCareResultProofEnvelopeBytes(call.InvocationID)
		}
		estimate, estimateErr := playeringress.EstimateParticipantRecordCapacityAt(
			rec, call.InvocationID, call.CanonicalStepPayloadHash[:], proofBytes, nowMs,
		)
		if estimateErr != nil {
			return nil, false, estimateErr
		}
		call.AdmissionCapacityKnown = true
		call.AdmissionRecordVersion = recordVersion
		call.AdmissionCandidateReceiptItems = estimate.ReceiptItems
		call.AdmissionCandidateModelBytes = estimate.ModelBytes
		call.AdmissionCandidateReceiptBytes = estimate.ReceiptBytes
		call.AdmissionCandidateSingleReceiptBytes = estimate.SingleReceiptBytes
		return nil, false, nil
	}
	if !bytes.Equal(receipt.GetPayloadHash(), call.CanonicalStepPayloadHash[:]) {
		return nil, true, status.Error(codes.FailedPrecondition, "PARTICIPANT_IDEMPOTENCY_CONFLICT")
	}
	var outcome contractspb.ParticipantOutcome
	var typedResult proto.Message
	var resultHash []byte
	var failure *contractspb.ParticipantFailure
	var committedVersion uint64
	if isHarvestPlayerCommand(call) {
		outcome, typedResult, resultHash, failure, committedVersion, err = decodeHarvestReceipt(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash[:], receipt)
	} else if isTheftPlayerCommand(call) {
		outcome, typedResult, resultHash, failure, committedVersion, err = decodeTheftReceipt(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash[:], receipt)
	} else if isCarePlayerCommand(call) {
		outcome, typedResult, resultHash, failure, committedVersion, err = decodeCareReceipt(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash[:], receipt)
		if err == nil {
			payload, ok := call.Payload.(*contractspb.ApplyCareRewardPayload)
			if !ok {
				err = status.Error(codes.Internal, "player participant Care payload schema mismatch")
			} else {
				err = validateCareReceiptBinding(payload, typedResult)
			}
		}
	} else if isUnlockPlayerCommand(call) {
		outcome, typedResult, resultHash, failure, committedVersion, err = decodeUnlockReceipt(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash[:], receipt)
	} else {
		outcome, typedResult, resultHash, failure, committedVersion, err = decodePlantReceipt(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash[:], receipt)
	}
	if err != nil {
		return nil, true, err
	}
	if err := validateReceiptRecordVersion(committedVersion, recordVersion); err != nil {
		return nil, true, err
	}
	return &contracts.StepOutcome{
		Outcome:                 outcome,
		ReplayedFromReceipt:     true,
		DurableReceiptCommitted: true,
		CommittedResultHash:     resultHash,
		Result:                  typedResult,
		Failure:                 failure,
		CommittedRecordVersion:  committedVersion,
	}, true, nil
}

func isPlantPlayerCommand(call *contracts.ParticipantCall) bool {
	if call == nil {
		return false
	}
	switch call.Spec.Command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_SEED_RESERVATION,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_SEED_RESERVATION:
		return true
	default:
		return false
	}
}

// validatePlantPayload is a pure admission check. It deliberately contains
// no PlayerRecord/Farm access, so malformed or semantically impossible Plant
// payloads are rejected before the dependency proof and before the Actor
// mailbox. The turn repeats it after its durable receipt lookup for the
// race-safe business path.
func validatePlantPayload(call *contracts.ParticipantCall) error {
	if call == nil {
		return status.Error(codes.InvalidArgument, "player participant call is incomplete")
	}
	switch payload := call.Payload.(type) {
	case *contractspb.ReserveSeedPayload:
		return validateReserveSeedPayload(call, payload)
	case *contractspb.CommitSeedReservationPayload:
		return validateCommitSeedReservationPayload(call, payload)
	case *contractspb.ReleaseSeedReservationPayload:
		return validateReleaseSeedReservationPayload(call, payload)
	default:
		return status.Error(codes.Internal, "player participant Plant payload schema mismatch")
	}
}

func (h *plantHandler) executePlant(ctx context.Context, call *contracts.ParticipantCall, validate func() error, decide plantDecision) (*contracts.StepOutcome, error) {
	if h.registry == nil {
		return nil, status.Error(codes.Unavailable, "player actor registry unavailable")
	}
	if err := contracts.CheckParticipantBudget(ctx, call); err != nil {
		return nil, err
	}
	// The context deadline bounds the potentially blocking authority load and
	// commit. The callback below still performs the trusted monotonic budget
	// checks because a request budget may be shorter than the gRPC deadline and
	// tests may use an injected clock.
	budgetCtx, cancel := context.WithTimeout(ctx, time.Duration(call.RemainingBudgetMs)*time.Millisecond)
	defer cancel()
	result, err := h.registry.SubmitParticipantVersionedFromAdmission(budgetCtx, call.RouteHeader.GetActorId(), call.AdmissionRecordVersion, func(rec *playerpb.PlayerRecord, currentVersion uint64, guard playeractor.ParticipantCapacityGuard) (any, bool, error) {
		// P0 ParticipantReceipt physical mapping:
		// internal operation_id=ParticipantReceiptOperationKey(participant_invocation_id),
		// payload_hash=CanonicalStepPayloadHash,
		// result_payload=versioned proof(step hash + canonical result hash +
		// deterministic typed result). The mapping lives in the durable
		// PlayerRecord carrier, never only in a process-local map.
		if receipt := playeringress.FindParticipantOperationReceipt(rec, call.InvocationID); receipt != nil {
			if !bytes.Equal(receipt.GetPayloadHash(), call.CanonicalStepPayloadHash[:]) {
				return nil, false, status.Error(codes.FailedPrecondition, "PARTICIPANT_IDEMPOTENCY_CONFLICT")
			}
			outcome, typedResult, resultHash, failure, committedVersion, err := decodePlantReceipt(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash[:], receipt)
			if err != nil {
				return nil, false, err
			}
			if err := validateReceiptRecordVersion(committedVersion, currentVersion); err != nil {
				return nil, false, err
			}
			return &contracts.StepOutcome{
				Outcome:                 outcome,
				ReplayedFromReceipt:     true,
				DurableReceiptCommitted: true,
				CommittedResultHash:     resultHash,
				Result:                  typedResult,
				Failure:                 failure,
				CommittedRecordVersion:  committedVersion,
			}, false, nil
		}
		if err := contracts.CheckParticipantBudget(budgetCtx, call); err != nil {
			return nil, false, err
		}
		// Semantic payload validation is deliberately after the durable receipt
		// lookup: an already-used invocation with a different canonical hash
		// must surface PARTICIPANT_IDEMPOTENCY_CONFLICT even if the retry also
		// contains a business-invalid field.
		if err := validate(); err != nil {
			return nil, false, err
		}
		if err := contracts.CheckParticipantBudget(budgetCtx, call); err != nil {
			return nil, false, err
		}
		capacityNowMs := h.nowMs()
		estimate, err := playeringress.EstimateParticipantRecordCapacityAt(
			rec, call.InvocationID, call.CanonicalStepPayloadHash[:], maxPlantResultProofEnvelopeBytes(call.InvocationID), capacityNowMs,
		)
		if err != nil {
			return nil, false, err
		}
		if err := playeringress.CheckParticipantRecordCapacity(estimate); err != nil {
			return nil, false, participantReceiptCapacityError()
		}
		if err := guard.Check(estimate.ModelBytes); err != nil {
			return nil, false, participantReceiptCapacityError()
		}
		if err := playeringress.EnsureParticipantOperationReceiptCapacity(rec, capacityNowMs); err != nil {
			// The detached checks above run before this retention mutation. This
			// call applies the same pruning to the live candidate only after both
			// nested and outer capacity have admitted it.
			if errors.Is(err, playeringress.ErrParticipantReceiptCapacity) {
				return nil, false, participantReceiptCapacityError()
			}
			return nil, false, err
		}
		if err := contracts.CheckParticipantBudget(budgetCtx, call); err != nil {
			return nil, false, err
		}
		typedResult, outcome, errorCode := decide(rec, currentVersion)
		if typedResult == nil {
			return nil, false, status.Error(codes.Internal, "player participant decision returned nil typed result")
		}
		committedVersion := currentVersion + 1
		if err := setPlantRecordVersion(typedResult, committedVersion); err != nil {
			return nil, false, err
		}
		resultProof, resultHash, err := encodeParticipantResultProof(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash, typedResult)
		if err != nil {
			return nil, false, err
		}
		if err := contracts.CheckParticipantBudget(budgetCtx, call); err != nil {
			return nil, false, err
		}
		playeringress.AppendParticipantOperationReceiptAt(
			rec,
			call.InvocationID,
			call.CanonicalStepPayloadHash[:],
			resultProof,
			commandStatus(outcome),
			errorCode,
			h.nowMs(),
		)
		return &contracts.StepOutcome{
			Outcome:                 outcome,
			DurableReceiptCommitted: true,
			CommittedResultHash:     resultHash,
			Result:                  typedResult,
			Failure:                 plantFailure(errorCode),
			CommittedRecordVersion:  committedVersion,
		}, true, nil
	})
	if err != nil {
		if errorsIsTimeoutUnknown(err) {
			return &contracts.StepOutcome{
				Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN,
				Failure: &contractspb.ParticipantFailure{
					Error:                    &logicrpcpb.ErrorDetail{ErrorCode: commonpb.ErrorCode_ERR_TIMEOUT},
					RetryableAfterResolution: true,
				},
			}, nil
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
		return nil, status.Error(codes.Internal, "player participant returned invalid turn result")
	}
	return outcome, nil
}

func (h *plantHandler) reserveSeed(rec *playerpb.PlayerRecord, call *contracts.ParticipantCall, payload *contractspb.ReserveSeedPayload) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode) {
	available := func() uint32 { return availableSeed(rec, payload.GetSeedItemId()) }
	result := &contractspb.ReserveSeedResult{
		ReservationId: payload.GetReservationId(), SeedItemId: payload.GetSeedItemId(), ReservedQuantity: payload.GetQuantity(),
	}
	existing := playeringress.FindReservation(rec, payload.GetReservationId())
	if existing != nil {
		if existing.GetItemId() != int32(payload.GetSeedItemId()) || existing.GetCount() != int64(payload.GetQuantity()) ||
			!bytes.Equal(existing.GetPayloadHash(), call.CanonicalStepPayloadHash[:]) {
			result.AvailableSeedQuantityAfter = available()
			return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT
		}
		result.AvailableSeedQuantityAfter = available()
		switch existing.GetState() {
		case playerpb.ReservationState_PREPARED, playerpb.ReservationState_COMMITTED:
			return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE, commonpb.ErrorCode_OK
		default:
			return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM
		}
	}
	if err := playeringress.EnsureReservationCapacity(rec, h.nowMs()); err != nil {
		result.AvailableSeedQuantityAfter = available()
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_RATE_LIMITED
	}
	if recAvailable := playeractor.ItemCount(rec, int32(payload.GetSeedItemId())); recAvailable < int64(payload.GetQuantity()) {
		result.AvailableSeedQuantityAfter = available()
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM
	}
	if !playeractor.AddItem(rec, int32(payload.GetSeedItemId()), -int64(payload.GetQuantity())) {
		result.AvailableSeedQuantityAfter = available()
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM
	}
	nowMs := h.nowMs()
	playeringress.PruneExpiredReservations(rec, nowMs)
	rec.ItemReservations = append(rec.ItemReservations, &playerpb.ItemReservation{
		ReservationId: payload.GetReservationId(),
		ItemId:        int32(payload.GetSeedItemId()),
		Count:         int64(payload.GetQuantity()),
		State:         playerpb.ReservationState_PREPARED, // RESERVED in the frozen Participant contract.
		UpdatedAtMs:   nowMs,
		PayloadHash:   append([]byte(nil), call.CanonicalStepPayloadHash[:]...),
	})
	rec.EconomyVersion++
	result.AvailableSeedQuantityAfter = available()
	return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, commonpb.ErrorCode_OK
}

func (h *plantHandler) commitSeedReservation(rec *playerpb.PlayerRecord, call *contracts.ParticipantCall, payload *contractspb.CommitSeedReservationPayload) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode) {
	result := &contractspb.CommitSeedReservationResult{ReservationId: payload.GetReservationId()}
	reservation := playeringress.FindReservation(rec, payload.GetReservationId())
	if reservation == nil {
		result.AvailableSeedQuantity = availableSeed(rec, 0)
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM
	}
	result.SeedItemId = uint32(reservation.GetItemId())
	result.AvailableSeedQuantity = availableSeed(rec, result.GetSeedItemId())
	switch reservation.GetState() {
	case playerpb.ReservationState_PREPARED:
		reservation.State = playerpb.ReservationState_COMMITTED // CONSUMED in typed contract.
		reservation.UpdatedAtMs = h.nowMs()
		rec.EconomyVersion++
		result.ReservationState = contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_CONSUMED
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, commonpb.ErrorCode_OK
	case playerpb.ReservationState_COMMITTED:
		result.ReservationState = contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_CONSUMED
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE, commonpb.ErrorCode_OK
	case playerpb.ReservationState_RELEASED:
		result.ReservationState = contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_RELEASED
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM
	default:
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INTERNAL
	}
}

func (h *plantHandler) releaseSeedReservation(rec *playerpb.PlayerRecord, call *contracts.ParticipantCall, payload *contractspb.ReleaseSeedReservationPayload) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode) {
	result := &contractspb.ReleaseSeedReservationResult{ReservationId: payload.GetReservationId(), SeedItemId: payload.GetSeedItemId()}
	reservation := playeringress.FindReservation(rec, payload.GetReservationId())
	if reservation == nil {
		result.AvailableSeedQuantityAfter = availableSeed(rec, payload.GetSeedItemId())
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM
	}
	if reservation.GetItemId() != int32(payload.GetSeedItemId()) || reservation.GetCount() != int64(payload.GetQuantity()) {
		result.AvailableSeedQuantityAfter = availableSeed(rec, payload.GetSeedItemId())
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT
	}
	switch reservation.GetState() {
	case playerpb.ReservationState_PREPARED:
		if !playeractor.AddItem(rec, reservation.GetItemId(), reservation.GetCount()) {
			return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INTERNAL
		}
		reservation.State = playerpb.ReservationState_RELEASED
		reservation.UpdatedAtMs = h.nowMs()
		rec.EconomyVersion++
		result.ReservationState = contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_RELEASED
		result.AvailableSeedQuantityAfter = availableSeed(rec, payload.GetSeedItemId())
		sourceordermetrics.ObserveReleaseSeed(payload.GetReleaseReason())
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, commonpb.ErrorCode_OK
	case playerpb.ReservationState_RELEASED:
		result.ReservationState = contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_RELEASED
		result.AvailableSeedQuantityAfter = availableSeed(rec, payload.GetSeedItemId())
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE, commonpb.ErrorCode_OK
	case playerpb.ReservationState_COMMITTED:
		result.ReservationState = contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_CONSUMED
		result.AvailableSeedQuantityAfter = availableSeed(rec, payload.GetSeedItemId())
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INTERNAL
	default:
		result.AvailableSeedQuantityAfter = availableSeed(rec, payload.GetSeedItemId())
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INTERNAL
	}
}

func validateReserveSeedPayload(call *contracts.ParticipantCall, payload *contractspb.ReserveSeedPayload) error {
	if payload.GetPlayerId() != call.RouteHeader.GetActorId() || payload.GetFarmId() == 0 || payload.GetPlotId() == 0 || payload.GetQuantity() != 1 {
		return status.Error(codes.InvalidArgument, "player participant reserve payload target/quantity mismatch")
	}
	definition, ok := cropcatalog.ByDefinitionID(int32(payload.GetCropDefinitionId()))
	if !ok || definition.SeedItemID != int32(payload.GetSeedItemId()) || payload.GetCropRuleVersion() == 0 || len(payload.GetCropRuleSnapshotHash()) != sha256.Size {
		return status.Error(codes.InvalidArgument, "player participant reserve crop rule mismatch")
	}
	wantReservation, err := identity.SeedReservationID(call.Work.GetWorkId())
	if err != nil || payload.GetReservationId() != wantReservation {
		return status.Error(codes.InvalidArgument, "player participant reservation id is not work-derived")
	}
	return nil
}

func validateCommitSeedReservationPayload(call *contracts.ParticipantCall, payload *contractspb.CommitSeedReservationPayload) error {
	if payload.GetPlayerId() != call.RouteHeader.GetActorId() || payload.GetFarmId() == 0 || payload.GetPlotId() == 0 || len(payload.GetCommitPlantResultHash()) != sha256.Size {
		return status.Error(codes.InvalidArgument, "player participant commit payload mismatch")
	}
	wantReservation, err := identity.SeedReservationID(call.Work.GetWorkId())
	if err != nil || payload.GetReservationId() != wantReservation {
		return status.Error(codes.InvalidArgument, "player participant reservation id is not work-derived")
	}
	wantCycle, err := identity.CropCycleID(call.Work.GetWorkId())
	if err != nil || payload.GetCropCycleId() != wantCycle {
		return status.Error(codes.InvalidArgument, "player participant crop cycle id is not work-derived")
	}
	return nil
}

func validateReleaseSeedReservationPayload(call *contracts.ParticipantCall, payload *contractspb.ReleaseSeedReservationPayload) error {
	if payload.GetPlayerId() != call.RouteHeader.GetActorId() || payload.GetFarmId() == 0 || payload.GetPlotId() == 0 || payload.GetSeedItemId() == 0 || payload.GetQuantity() != 1 || len(payload.GetTerminalFarmResultHash()) != sha256.Size {
		return status.Error(codes.InvalidArgument, "player participant release payload mismatch")
	}
	// Plant P0 has only the FARM_REJECTED compensation branch. Keep this
	// diagnostic field closed over the frozen state-machine value; a free-form
	// string must never be enough to authorize a refund. The dependency
	// verifier separately proves the Farm terminal state and hash.
	if strings.TrimSpace(payload.GetReleaseReason()) != "FARM_REJECTED" || payload.GetReleaseReason() != "FARM_REJECTED" {
		return status.Error(codes.InvalidArgument, "player participant release reason is not FARM_REJECTED")
	}
	wantReservation, err := identity.SeedReservationID(call.Work.GetWorkId())
	if err != nil || payload.GetReservationId() != wantReservation {
		return status.Error(codes.InvalidArgument, "player participant reservation id is not work-derived")
	}
	return nil
}

func availableSeed(rec *playerpb.PlayerRecord, itemID uint32) uint32 {
	if itemID == 0 {
		return 0
	}
	count := playeractor.ItemCount(rec, int32(itemID))
	if count <= 0 {
		return 0
	}
	if count > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(count)
}

func setPlantRecordVersion(result proto.Message, version uint64) error {
	switch result := result.(type) {
	case *contractspb.ReserveSeedResult:
		result.PlayerRecordVersion = version
	case *contractspb.CommitSeedReservationResult:
		result.PlayerRecordVersion = version
	case *contractspb.ReleaseSeedReservationResult:
		result.PlayerRecordVersion = version
	default:
		return status.Error(codes.Internal, "player participant result has no PlayerRecordVersion")
	}
	return nil
}

func plantRecordVersion(result proto.Message) uint64 {
	switch result := result.(type) {
	case *contractspb.ReserveSeedResult:
		return result.GetPlayerRecordVersion()
	case *contractspb.CommitSeedReservationResult:
		return result.GetPlayerRecordVersion()
	case *contractspb.ReleaseSeedReservationResult:
		return result.GetPlayerRecordVersion()
	default:
		return 0
	}
}

func validateReceiptRecordVersion(receiptVersion, recordVersion uint64) error {
	if receiptVersion == 0 || recordVersion == 0 || receiptVersion > recordVersion {
		return status.Error(codes.Internal, "player participant receipt record version is ahead of actor record")
	}
	return nil
}

func decodePlantReceipt(spec contracts.ParticipantCommandSpec, expectedInvocationID string, expectedStepHash []byte, receipt *playerpb.PlayerOperationReceipt) (contractspb.ParticipantOutcome, proto.Message, []byte, *contractspb.ParticipantFailure, uint64, error) {
	var outcome contractspb.ParticipantOutcome
	switch receipt.GetStatus() {
	case commonpb.CommandStatus_SUCCEEDED:
		outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED
	case commonpb.CommandStatus_REJECTED:
		outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED
	case commonpb.CommandStatus_NO_CHANGE:
		outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE
	default:
		return 0, nil, nil, nil, 0, status.Error(codes.Internal, "player participant receipt has non-terminal status")
	}
	if len(receipt.GetResultPayload()) == 0 {
		return 0, nil, nil, nil, 0, status.Error(codes.Internal, "player participant receipt is missing durable result proof")
	}
	if !bytes.Equal(receipt.GetPayloadHash(), expectedStepHash) {
		return 0, nil, nil, nil, 0, status.Error(codes.Internal, "player participant receipt/proof payload hash mismatch")
	}
	result, resultHash, err := decodeParticipantResultProof(spec, expectedInvocationID, receipt.GetPayloadHash(), receipt.GetResultPayload())
	if err != nil {
		return 0, nil, nil, nil, 0, err
	}
	version := plantRecordVersion(result)
	if version == 0 {
		return 0, nil, nil, nil, 0, status.Error(codes.Internal, "player participant receipt missing record version")
	}
	var failure *contractspb.ParticipantFailure
	if receipt.GetErrorCode() != commonpb.ErrorCode_OK && receipt.GetErrorCode() != commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED {
		failure = plantFailure(receipt.GetErrorCode())
	}
	return outcome, result, resultHash, failure, version, nil
}

func commandStatus(outcome contractspb.ParticipantOutcome) commonpb.CommandStatus {
	switch outcome {
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED:
		return commonpb.CommandStatus_SUCCEEDED
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE:
		return commonpb.CommandStatus_NO_CHANGE
	default:
		return commonpb.CommandStatus_REJECTED
	}
}

func plantFailure(code commonpb.ErrorCode) *contractspb.ParticipantFailure {
	if code == commonpb.ErrorCode_OK || code == commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED {
		return nil
	}
	return &contractspb.ParticipantFailure{Error: &logicrpcpb.ErrorDetail{ErrorCode: code}}
}

func participantReceiptCapacityError() error {
	stable := status.New(codes.ResourceExhausted, "ERR_RATE_LIMITED")
	withDetails, err := stable.WithDetails(&contractspb.ParticipantFailure{
		Error: &logicrpcpb.ErrorDetail{ErrorCode: commonpb.ErrorCode_ERR_RATE_LIMITED},
	})
	if err != nil {
		return stable.Err()
	}
	return withDetails.Err()
}

func errorsIsTimeoutUnknown(err error) bool {
	// blobstore errors are ordinary Go errors in both Registry modes.
	return errors.Is(err, blobstore.ErrTimeoutUnknown)
}
