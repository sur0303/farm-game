package participant

import (
	"bytes"
	"context"
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
	"farmgame/internal/player/occupancy"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
	"farmgame/internal/worker/sourceorder"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func unlockSourcePlacement(call *contracts.ParticipantCall) bool {
	return sourcePlacement(call) && call != nil &&
		call.Spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_UNLOCK_COIN
}

func (h *plantHandler) executeUnlockSource(ctx context.Context, call *contracts.ParticipantCall, payload *contractspb.ReserveUnlockCoinPayload) (*contracts.StepOutcome, error) {
	if h.registry == nil {
		return nil, status.Error(codes.Unavailable, "player actor registry unavailable")
	}
	if err := contracts.CheckParticipantBudget(ctx, call); err != nil {
		return nil, err
	}
	budgetCtx, cancel := context.WithTimeout(ctx, time.Duration(call.RemainingBudgetMs)*time.Millisecond)
	defer cancel()
	result, err := h.registry.SubmitParticipantVersionedWithOrdersFromAdmission(budgetCtx, call.RouteHeader.GetActorId(), call.AdmissionRecordVersion,
		func(rec *playerpb.PlayerRecord, currentVersion uint64, guard playeractor.ParticipantCapacityGuard) (any, bool, authoritystore.StoredOperationOutcome, []authoritystore.AttachedOrder, error) {
			if receipt := playeringress.FindParticipantOperationReceipt(rec, call.InvocationID); receipt != nil {
				if !bytes.Equal(receipt.GetPayloadHash(), call.CanonicalStepPayloadHash[:]) {
					return nil, false, authoritystore.StoredOperationOutcome{}, nil, status.Error(codes.FailedPrecondition, "PARTICIPANT_IDEMPOTENCY_CONFLICT")
				}
				outcome, typedResult, resultHash, failure, committedVersion, decodeErr := decodeUnlockReceipt(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash[:], receipt)
				if decodeErr != nil {
					return nil, false, authoritystore.StoredOperationOutcome{}, nil, decodeErr
				}
				if err := validateReceiptRecordVersion(committedVersion, currentVersion); err != nil {
					return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
				}
				return &contracts.StepOutcome{
					Outcome: outcome, ReplayedFromReceipt: true, DurableReceiptCommitted: true,
					CommittedResultHash: resultHash, Result: typedResult, Failure: failure,
					CommittedRecordVersion: committedVersion,
				}, false, authoritystore.StoredOperationOutcome{}, nil, nil
			}
			if err := validateReserveUnlockCoinPayload(call, payload); err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
			}
			capacityNowMs := h.nowMs()
			if guard.PhysicalProjectionRequired() {
				estimate, err := playeringress.EstimateParticipantRecordCapacityAt(
					rec, call.InvocationID, call.CanonicalStepPayloadHash[:], maxPlantResultProofEnvelopeBytes(call.InvocationID), capacityNowMs)
				if err != nil {
					return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
				}
				if err := playeringress.CheckParticipantRecordCapacity(estimate); err != nil {
					return nil, false, authoritystore.StoredOperationOutcome{}, nil, participantReceiptCapacityError()
				}
				if err := guard.Check(estimate.ModelBytes); err != nil {
					return nil, false, authoritystore.StoredOperationOutcome{}, nil, participantReceiptCapacityError()
				}
			}
			if err := playeringress.EnsureParticipantOperationReceiptCapacity(rec, capacityNowMs); err != nil {
				if errors.Is(err, playeringress.ErrParticipantReceiptCapacity) {
					return nil, false, authoritystore.StoredOperationOutcome{}, nil, participantReceiptCapacityError()
				}
				return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
			}
			typedResult, outcome, errorCode, order := h.reserveUnlockSource(rec, call, payload, currentVersion+1)
			if err := setUnlockRecordVersion(typedResult, currentVersion+1); err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
			}
			resultProof, resultHash, err := encodeParticipantResultProof(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash, typedResult)
			if err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
			}
			playeringress.AppendParticipantOperationReceiptAt(
				rec, call.InvocationID, call.CanonicalStepPayloadHash[:], resultProof,
				commandStatus(outcome), errorCode, h.nowMs())
			var orders []authoritystore.AttachedOrder
			var created *contractspb.CreatedSourceOrderEnvelope
			if outcome == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED && order.OrderID != "" {
				orders = []authoritystore.AttachedOrder{order}
				created, err = createdSourceOrderEnvelope(call, order)
				if err != nil {
					return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
				}
			}
			return &contracts.StepOutcome{
				Outcome: outcome, DurableReceiptCommitted: true, CommittedResultHash: resultHash,
				Result: typedResult, Failure: plantFailure(errorCode), CommittedRecordVersion: currentVersion + 1,
				CreatedSourceOrder: created,
			}, true, harvestStoredOperationOutcome(outcome, errorCode, nil), orders, nil
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

func (h *plantHandler) reserveUnlockSource(rec *playerpb.PlayerRecord, call *contracts.ParticipantCall, payload *contractspb.ReserveUnlockCoinPayload, committedVersion uint64) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode, authoritystore.AttachedOrder) {
	result := &contractspb.ReserveUnlockCoinResult{ReservationId: payload.GetReservationId(), ReservedAmountNormalCoin: payload.GetAmountNormalCoin()}
	if rec.Wallet == nil {
		rec.Wallet = &economypb.Wallet{}
	}
	result.AvailableNormalCoinAfter = uint64(maxInt64NonNegative(rec.Wallet.Coin))
	if existing, occupied := occupancy.UnlockOccupied(rec, payload.GetPlotId()); occupied {
		if existing == payload.GetReservationId() {
			return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE, commonpb.ErrorCode_OK, authoritystore.AttachedOrder{}
		}
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_PLOT_LOCKED, authoritystore.AttachedOrder{}
	}
	level := uint32(0)
	if rec.Progression != nil && rec.Progression.Level > 0 {
		level = uint32(rec.Progression.Level)
	}
	result.PlayerLevelAtReserve = level
	if level < payload.GetRequiredPlayerLevel() {
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_LEVEL_REQUIREMENT_NOT_MET, authoritystore.AttachedOrder{}
	}
	if payload.GetAmountNormalCoin() > math.MaxInt64 || rec.Wallet.Coin < 0 || uint64(rec.Wallet.Coin) < payload.GetAmountNormalCoin() {
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INSUFFICIENT_COIN, authoritystore.AttachedOrder{}
	}
	rec.Wallet.Coin -= int64(payload.GetAmountNormalCoin())
	if err := occupancy.OccupyUnlock(rec, payload.GetPlotId(), payload.GetReservationId(), int64(payload.GetAmountNormalCoin()), h.nowMs()); err != nil {
		rec.Wallet.Coin += int64(payload.GetAmountNormalCoin())
		result.AvailableNormalCoinAfter = uint64(maxInt64NonNegative(rec.Wallet.Coin))
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_PLOT_LOCKED, authoritystore.AttachedOrder{}
	}
	rec.EconomyVersion++
	result.AvailableNormalCoinAfter = uint64(rec.Wallet.Coin)
	result.PlayerRecordVersion = committedVersion
	order, err := buildCommitUnlockOrder(call, payload, result, h.nowMs())
	if err != nil {
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INTERNAL, authoritystore.AttachedOrder{}
	}
	return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, commonpb.ErrorCode_OK, order
}

func buildCommitUnlockOrder(call *contracts.ParticipantCall, reserve *contractspb.ReserveUnlockCoinPayload, reserveResult *contractspb.ReserveUnlockCoinResult, dueAtMs int64) (authoritystore.AttachedOrder, error) {
	rule := domain.DefaultPlotUnlockRule()
	entry, ok := rule.EntryOf(reserve.GetPlotId())
	if !ok {
		return authoritystore.AttachedOrder{}, status.Error(codes.InvalidArgument, "unlock plot is not purchasable")
	}
	reserveSpec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_UNLOCK_COIN)
	if err != nil {
		return authoritystore.AttachedOrder{}, err
	}
	reserveHash, err := contracts.CanonicalParticipantHash(reserveSpec, reserveResult)
	if err != nil {
		return authoritystore.AttachedOrder{}, err
	}
	commit := &contractspb.CommitPlotUnlockPayload{
		OwnerPlayerId: reserve.GetPlayerId(), FarmId: reserve.GetFarmId(), PlotId: reserve.GetPlotId(),
		ExpectedPlotVersion: 1, ExpectedUnlockRuleVersion: reserve.GetUnlockRuleVersion(),
		UnlockOrder: entry.UnlockOrder, ReservationId: reserve.GetReservationId(),
		ReserveUnlockResultHash: append([]byte(nil), reserveHash[:]...),
		UnlockRuleSnapshotHash:  append([]byte(nil), reserve.GetUnlockRuleSnapshotHash()...),
		RequiredPlayerLevel:     reserve.GetRequiredPlayerLevel(), AmountNormalCoin: reserve.GetAmountNormalCoin(),
	}
	if reserve.GetPlotId() > domain.P0InitialUnlockedPlots+1 {
		pred := reserve.GetPlotId() - 1
		commit.PredecessorPlotId = &pred
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(commit)
	if err != nil {
		return authoritystore.AttachedOrder{}, err
	}
	hash, err := sourceorder.UnlockPayloadHash(commit)
	if err != nil {
		return authoritystore.AttachedOrder{}, err
	}
	return authoritystore.AttachedOrder{
		OrderID: call.Work.GetWorkId(), OrderType: string(identity.WorkTypeUnlockPlot), Payload: payload,
		PayloadHash: authoritystore.PayloadHash(hash), DueAtMs: dueAtMs,
		OperationID: call.OperationID, SubjectPlayerID: reserve.GetPlayerId(),
	}, nil
}
