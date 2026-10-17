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
	"farmgame/internal/farm/domain"
	"farmgame/internal/identity"
	playeractor "farmgame/internal/player/actor"
	playeringress "farmgame/internal/player/ingress"
	"farmgame/internal/storage/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	maxUnlockPlots             = 24
	firstUnlockPlot            = 6
	maxUnlockCoinReservations  = 1024
	unlockReservationRetention = 7 * 24 * time.Hour
)

type unlockDecision func(*playerpb.PlayerRecord, uint64) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode)

func isUnlockPlayerCommand(call *contracts.ParticipantCall) bool {
	if call == nil {
		return false
	}
	switch call.Spec.Command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_UNLOCK_COIN,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_UNLOCK_COIN_RESERVATION,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_UNLOCK_COIN_RESERVATION:
		return true
	default:
		return false
	}
}

func (h *plantHandler) executeUnlock(ctx context.Context, call *contracts.ParticipantCall, validate func() error, decide unlockDecision) (*contracts.StepOutcome, error) {
	if h.registry == nil {
		return nil, status.Error(codes.Unavailable, "player actor registry unavailable")
	}
	if err := contracts.CheckParticipantBudget(ctx, call); err != nil {
		return nil, err
	}
	budgetCtx, cancel := context.WithTimeout(ctx, time.Duration(call.RemainingBudgetMs)*time.Millisecond)
	defer cancel()
	result, err := h.registry.SubmitParticipantVersionedFromAdmission(budgetCtx, call.RouteHeader.GetActorId(), call.AdmissionRecordVersion, func(rec *playerpb.PlayerRecord, currentVersion uint64, guard playeractor.ParticipantCapacityGuard) (any, bool, error) {
		if receipt := playeringress.FindParticipantOperationReceipt(rec, call.InvocationID); receipt != nil {
			if !bytes.Equal(receipt.GetPayloadHash(), call.CanonicalStepPayloadHash[:]) {
				return nil, false, status.Error(codes.FailedPrecondition, "PARTICIPANT_IDEMPOTENCY_CONFLICT")
			}
			outcome, typedResult, resultHash, failure, committedVersion, decodeErr := decodeUnlockReceipt(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash[:], receipt)
			if decodeErr != nil {
				return nil, false, decodeErr
			}
			if err := validateReceiptRecordVersion(committedVersion, currentVersion); err != nil {
				return nil, false, err
			}
			return &contracts.StepOutcome{
				Outcome: outcome, ReplayedFromReceipt: true, DurableReceiptCommitted: true,
				CommittedResultHash: resultHash, Result: typedResult, Failure: failure,
				CommittedRecordVersion: committedVersion,
			}, false, nil
		}
		if err := contracts.CheckParticipantBudget(budgetCtx, call); err != nil {
			return nil, false, err
		}
		// A receipt with the same invocation but a different payload is handled
		// above. Only a first invocation reaches semantic validation and the
		// PlayerRecord mutation below.
		if err := validate(); err != nil {
			return nil, false, err
		}
		capacityNowMs := h.nowMs()
		if guard.PhysicalProjectionRequired() {
			estimate, err := playeringress.EstimateParticipantRecordCapacityAt(
				rec, call.InvocationID, call.CanonicalStepPayloadHash[:], maxUnlockResultProofEnvelopeBytes(call.InvocationID), capacityNowMs,
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
		}
		if err := playeringress.EnsureParticipantOperationReceiptCapacity(rec, capacityNowMs); err != nil {
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
			return nil, false, status.Error(codes.Internal, "player participant unlock decision returned nil typed result")
		}
		committedVersion := currentVersion + 1
		if err := setUnlockRecordVersion(typedResult, committedVersion); err != nil {
			return nil, false, err
		}
		resultProof, resultHash, err := encodeParticipantResultProof(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash, typedResult)
		if err != nil {
			return nil, false, err
		}
		if err := contracts.CheckParticipantBudget(budgetCtx, call); err != nil {
			return nil, false, err
		}
		playeringress.AppendParticipantOperationReceiptAt(rec, call.InvocationID, call.CanonicalStepPayloadHash[:], resultProof, commandStatus(outcome), errorCode, h.nowMs())
		return &contracts.StepOutcome{
			Outcome: outcome, DurableReceiptCommitted: true, CommittedResultHash: resultHash,
			Result: typedResult, Failure: plantFailure(errorCode), CommittedRecordVersion: committedVersion,
		}, true, nil
	})
	if err != nil {
		if errorsIsTimeoutUnknown(err) {
			return &contracts.StepOutcome{
				Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN,
				Failure: &contractspb.ParticipantFailure{Error: &logicrpcpb.ErrorDetail{ErrorCode: commonpb.ErrorCode_ERR_TIMEOUT}},
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
		return nil, status.Error(codes.Internal, "player participant returned invalid unlock turn result")
	}
	return outcome, nil
}

func validateUnlockPayload(call *contracts.ParticipantCall) error {
	if call == nil {
		return status.Error(codes.InvalidArgument, "player participant unlock call is incomplete")
	}
	switch payload := call.Payload.(type) {
	case *contractspb.ReserveUnlockCoinPayload:
		return validateReserveUnlockCoinPayload(call, payload)
	case *contractspb.CommitUnlockCoinReservationPayload:
		return validateCommitUnlockCoinReservationPayload(call, payload)
	case *contractspb.ReleaseUnlockCoinReservationPayload:
		return validateReleaseUnlockCoinReservationPayload(call, payload)
	default:
		return status.Error(codes.Internal, "player participant UnlockPlot payload schema mismatch")
	}
}

func validateUnlockPlotID(plotID uint32) bool {
	return plotID >= firstUnlockPlot && plotID <= maxUnlockPlots
}

func validateUnlockReservationID(call *contracts.ParticipantCall, reservationID string) error {
	want, err := identity.UnlockCoinReservationID(call.Work.GetWorkId())
	if err != nil || reservationID != want {
		return status.Error(codes.InvalidArgument, "player participant unlock reservation id is not work-derived")
	}
	return nil
}

func validateReserveUnlockCoinPayload(call *contracts.ParticipantCall, payload *contractspb.ReserveUnlockCoinPayload) error {
	rule := domain.DefaultPlotUnlockRule()
	entry, ok := rule.EntryOf(payload.GetPlotId())
	ruleHash := domain.PlotUnlockRuleSnapshotHash(rule)
	if payload.GetPlayerId() != call.RouteHeader.GetActorId() || payload.GetFarmId() != payload.GetPlayerId() || !validateUnlockPlotID(payload.GetPlotId()) ||
		!ok || payload.GetRequiredPlayerLevel() != entry.RequiredLevel || payload.GetAmountNormalCoin() != entry.NormalCoinPrice ||
		payload.GetUnlockRuleVersion() != rule.RuleVersion || len(payload.GetUnlockRuleSnapshotHash()) != sha256.Size || !bytes.Equal(payload.GetUnlockRuleSnapshotHash(), ruleHash[:]) {
		return status.Error(codes.InvalidArgument, "player participant reserve unlock payload mismatch")
	}
	return validateUnlockReservationID(call, payload.GetReservationId())
}

func validateCommitUnlockCoinReservationPayload(call *contracts.ParticipantCall, payload *contractspb.CommitUnlockCoinReservationPayload) error {
	if payload.GetPlayerId() != call.RouteHeader.GetActorId() || payload.GetFarmId() != payload.GetPlayerId() || !validateUnlockPlotID(payload.GetPlotId()) ||
		payload.GetAmountNormalCoin() == 0 || len(payload.GetCommitPlotUnlockResultHash()) != sha256.Size {
		return status.Error(codes.InvalidArgument, "player participant commit unlock payload mismatch")
	}
	return validateUnlockReservationID(call, payload.GetReservationId())
}

func validateReleaseUnlockCoinReservationPayload(call *contracts.ParticipantCall, payload *contractspb.ReleaseUnlockCoinReservationPayload) error {
	if payload.GetPlayerId() != call.RouteHeader.GetActorId() || payload.GetFarmId() != payload.GetPlayerId() || !validateUnlockPlotID(payload.GetPlotId()) ||
		payload.GetAmountNormalCoin() == 0 || len(payload.GetTerminalFarmResultHash()) != sha256.Size {
		return status.Error(codes.InvalidArgument, "player participant release unlock payload mismatch")
	}
	reason := payload.GetReleaseReason()
	if reason != "FARM_REJECTED" && reason != "FARM_NO_CHANGE" {
		return status.Error(codes.InvalidArgument, "player participant release unlock reason is not frozen")
	}
	return validateUnlockReservationID(call, payload.GetReservationId())
}

func (h *plantHandler) reserveUnlockCoin(rec *playerpb.PlayerRecord, call *contracts.ParticipantCall, payload *contractspb.ReserveUnlockCoinPayload) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode) {
	result := &contractspb.ReserveUnlockCoinResult{ReservationId: payload.GetReservationId(), ReservedAmountNormalCoin: payload.GetAmountNormalCoin()}
	if rec.Wallet == nil {
		rec.Wallet = &economypb.Wallet{}
	}
	result.AvailableNormalCoinAfter = uint64(maxInt64NonNegative(rec.Wallet.Coin))
	if existing := findUnlockCoinReservation(rec, payload.GetReservationId()); existing != nil {
		if existing.GetFarmId() != payload.GetFarmId() || existing.GetPlotId() != payload.GetPlotId() ||
			existing.GetAmountNormalCoin() != payload.GetAmountNormalCoin() || existing.GetUnlockRuleVersion() != payload.GetUnlockRuleVersion() ||
			!bytes.Equal(existing.GetPayloadHash(), call.CanonicalStepPayloadHash[:]) {
			return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT
		}
		switch existing.GetState() {
		case playerpb.ReservationState_PREPARED, playerpb.ReservationState_COMMITTED:
			result.AvailableNormalCoinAfter = uint64(maxInt64NonNegative(rec.Wallet.Coin))
			return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE, commonpb.ErrorCode_OK
		default:
			return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT
		}
	}
	if err := ensureUnlockCoinReservationCapacity(rec, h.nowMs()); err != nil {
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_RATE_LIMITED
	}
	level := uint32(0)
	if rec.Progression != nil && rec.Progression.Level > 0 {
		level = uint32(rec.Progression.Level)
	}
	result.PlayerLevelAtReserve = level
	if level < payload.GetRequiredPlayerLevel() {
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_LEVEL_REQUIREMENT_NOT_MET
	}
	if payload.GetAmountNormalCoin() > math.MaxInt64 || rec.Wallet.Coin < 0 || uint64(rec.Wallet.Coin) < payload.GetAmountNormalCoin() {
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INSUFFICIENT_COIN
	}
	rec.Wallet.Coin -= int64(payload.GetAmountNormalCoin())
	rec.UnlockCoinReservations = append(rec.UnlockCoinReservations, &playerpb.UnlockCoinReservation{
		ReservationId: payload.GetReservationId(), FarmId: payload.GetFarmId(), PlotId: payload.GetPlotId(),
		AmountNormalCoin: payload.GetAmountNormalCoin(), State: playerpb.ReservationState_PREPARED,
		UnlockRuleVersion: payload.GetUnlockRuleVersion(), UpdatedAtMs: h.nowMs(), PayloadHash: append([]byte(nil), call.CanonicalStepPayloadHash[:]...),
	})
	rec.EconomyVersion++
	result.AvailableNormalCoinAfter = uint64(rec.Wallet.Coin)
	return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, commonpb.ErrorCode_OK
}

func (h *plantHandler) commitUnlockCoinReservation(rec *playerpb.PlayerRecord, payload *contractspb.CommitUnlockCoinReservationPayload) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode) {
	result := &contractspb.CommitUnlockCoinReservationResult{ReservationId: payload.GetReservationId(), NormalCoinBalance: uint64(maxInt64NonNegative(rec.GetWallet().GetCoin()))}
	reservation := findUnlockCoinReservation(rec, payload.GetReservationId())
	if reservation == nil || reservation.GetFarmId() != payload.GetFarmId() || reservation.GetPlotId() != payload.GetPlotId() || reservation.GetAmountNormalCoin() != payload.GetAmountNormalCoin() {
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT
	}
	switch reservation.GetState() {
	case playerpb.ReservationState_PREPARED:
		reservation.State = playerpb.ReservationState_COMMITTED
		reservation.UpdatedAtMs = h.nowMs()
		rec.EconomyVersion++
		result.ReservationState = contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_CONSUMED
		result.NormalCoinBalance = uint64(maxInt64NonNegative(rec.GetWallet().GetCoin()))
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, commonpb.ErrorCode_OK
	case playerpb.ReservationState_COMMITTED:
		result.ReservationState = contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_CONSUMED
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE, commonpb.ErrorCode_OK
	case playerpb.ReservationState_RELEASED:
		result.ReservationState = contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_RELEASED
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT
	default:
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INTERNAL
	}
}

func (h *plantHandler) releaseUnlockCoinReservation(rec *playerpb.PlayerRecord, payload *contractspb.ReleaseUnlockCoinReservationPayload) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode) {
	result := &contractspb.ReleaseUnlockCoinReservationResult{ReservationId: payload.GetReservationId(), AvailableNormalCoinAfter: uint64(maxInt64NonNegative(rec.GetWallet().GetCoin()))}
	reservation := findUnlockCoinReservation(rec, payload.GetReservationId())
	if reservation == nil || reservation.GetFarmId() != payload.GetFarmId() || reservation.GetPlotId() != payload.GetPlotId() || reservation.GetAmountNormalCoin() != payload.GetAmountNormalCoin() {
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT
	}
	switch reservation.GetState() {
	case playerpb.ReservationState_PREPARED:
		if rec.Wallet == nil || rec.Wallet.Coin < 0 || payload.GetAmountNormalCoin() > math.MaxInt64 || uint64(rec.Wallet.Coin) > math.MaxInt64-payload.GetAmountNormalCoin() {
			return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INTERNAL
		}
		rec.Wallet.Coin += int64(payload.GetAmountNormalCoin())
		reservation.State = playerpb.ReservationState_RELEASED
		reservation.UpdatedAtMs = h.nowMs()
		rec.EconomyVersion++
		result.ReservationState = contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_RELEASED
		result.AvailableNormalCoinAfter = uint64(rec.Wallet.Coin)
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, commonpb.ErrorCode_OK
	case playerpb.ReservationState_RELEASED:
		result.ReservationState = contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_RELEASED
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE, commonpb.ErrorCode_OK
	case playerpb.ReservationState_COMMITTED:
		result.ReservationState = contractspb.ReservationTerminalState_RESERVATION_TERMINAL_STATE_CONSUMED
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT
	default:
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INTERNAL
	}
}

func findUnlockCoinReservation(rec *playerpb.PlayerRecord, reservationID string) *playerpb.UnlockCoinReservation {
	if rec == nil || reservationID == "" {
		return nil
	}
	for i := len(rec.UnlockCoinReservations) - 1; i >= 0; i-- {
		if rec.UnlockCoinReservations[i] != nil && rec.UnlockCoinReservations[i].GetReservationId() == reservationID {
			return rec.UnlockCoinReservations[i]
		}
	}
	return nil
}

func ensureUnlockCoinReservationCapacity(rec *playerpb.PlayerRecord, nowMs int64) error {
	if rec == nil {
		return errors.New("nil PlayerRecord")
	}
	cutoff := nowMs - unlockReservationRetention.Milliseconds()
	active := 0
	for _, reservation := range rec.UnlockCoinReservations {
		if reservation != nil && (reservation.GetState() == playerpb.ReservationState_PREPARED || reservation.GetUpdatedAtMs() >= cutoff) {
			active++
		}
	}
	if active >= maxUnlockCoinReservations {
		return errors.New("unlock coin reservation retention is full")
	}
	kept := rec.UnlockCoinReservations[:0]
	for _, reservation := range rec.UnlockCoinReservations {
		if reservation != nil && (reservation.GetState() == playerpb.ReservationState_PREPARED || reservation.GetUpdatedAtMs() >= cutoff) {
			kept = append(kept, reservation)
		}
	}
	rec.UnlockCoinReservations = kept
	return nil
}

func maxInt64NonNegative(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func setUnlockRecordVersion(result proto.Message, version uint64) error {
	switch result := result.(type) {
	case *contractspb.ReserveUnlockCoinResult:
		result.PlayerRecordVersion = version
	case *contractspb.CommitUnlockCoinReservationResult:
		result.PlayerRecordVersion = version
	case *contractspb.ReleaseUnlockCoinReservationResult:
		result.PlayerRecordVersion = version
	default:
		return status.Error(codes.Internal, "player participant unlock result has no PlayerRecordVersion")
	}
	return nil
}

func unlockRecordVersion(result proto.Message) uint64 {
	switch result := result.(type) {
	case *contractspb.ReserveUnlockCoinResult:
		return result.GetPlayerRecordVersion()
	case *contractspb.CommitUnlockCoinReservationResult:
		return result.GetPlayerRecordVersion()
	case *contractspb.ReleaseUnlockCoinReservationResult:
		return result.GetPlayerRecordVersion()
	default:
		return 0
	}
}

func decodeUnlockReceipt(spec contracts.ParticipantCommandSpec, expectedInvocationID string, expectedStepHash []byte, receipt *playerpb.PlayerOperationReceipt) (contractspb.ParticipantOutcome, proto.Message, []byte, *contractspb.ParticipantFailure, uint64, error) {
	var outcome contractspb.ParticipantOutcome
	switch receipt.GetStatus() {
	case commonpb.CommandStatus_SUCCEEDED:
		outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED
	case commonpb.CommandStatus_REJECTED:
		outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED
	case commonpb.CommandStatus_NO_CHANGE:
		outcome = contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE
	default:
		return 0, nil, nil, nil, 0, status.Error(codes.Internal, "player participant UnlockPlot receipt has non-terminal status")
	}
	if len(receipt.GetResultPayload()) == 0 || !bytes.Equal(receipt.GetPayloadHash(), expectedStepHash) {
		return 0, nil, nil, nil, 0, status.Error(codes.Internal, "player participant UnlockPlot receipt/proof binding is corrupt")
	}
	result, resultHash, err := decodeParticipantResultProof(spec, expectedInvocationID, expectedStepHash, receipt.GetResultPayload())
	if err != nil {
		return 0, nil, nil, nil, 0, err
	}
	if _, ok := result.(*contractspb.ReserveUnlockCoinResult); !ok {
		if _, ok := result.(*contractspb.CommitUnlockCoinReservationResult); !ok {
			if _, ok := result.(*contractspb.ReleaseUnlockCoinReservationResult); !ok {
				return 0, nil, nil, nil, 0, status.Error(codes.Internal, "player participant UnlockPlot receipt result schema mismatch")
			}
		}
	}
	version := unlockRecordVersion(result)
	if version == 0 {
		return 0, nil, nil, nil, 0, status.Error(codes.Internal, "player participant UnlockPlot receipt missing record version")
	}
	var failure *contractspb.ParticipantFailure
	if receipt.GetErrorCode() != commonpb.ErrorCode_OK && receipt.GetErrorCode() != commonpb.ErrorCode_ERROR_CODE_UNSPECIFIED {
		failure = plantFailure(receipt.GetErrorCode())
	}
	return outcome, result, resultHash, failure, version, nil
}
