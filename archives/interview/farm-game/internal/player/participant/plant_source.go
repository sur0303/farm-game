package participant

import (
	"bytes"
	"context"
	"errors"
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
	"farmgame/internal/player/occupancy"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
	"farmgame/internal/worker/sourceorder"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func plantSourcePlacement(call *contracts.ParticipantCall) bool {
	return sourcePlacement(call) && call != nil &&
		call.Spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED
}

func (h *plantHandler) executePlantSource(ctx context.Context, call *contracts.ParticipantCall, payload *contractspb.ReserveSeedPayload) (*contracts.StepOutcome, error) {
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
				outcome, typedResult, resultHash, failure, committedVersion, decodeErr := decodePlantReceipt(call.Spec, call.InvocationID, call.CanonicalStepPayloadHash[:], receipt)
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
			if err := contracts.CheckParticipantBudget(budgetCtx, call); err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
			}
			if err := validateReserveSeedPayload(call, payload); err != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
			}
			if payload.GetFarmId() != payload.GetPlayerId() {
				return nil, false, authoritystore.StoredOperationOutcome{}, nil, status.Error(codes.PermissionDenied, "只能在自己的农场播种")
			}
			capacityNowMs := h.nowMs()
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
			if err := playeringress.EnsureParticipantOperationReceiptCapacity(rec, capacityNowMs); err != nil {
				if errors.Is(err, playeringress.ErrParticipantReceiptCapacity) {
					return nil, false, authoritystore.StoredOperationOutcome{}, nil, participantReceiptCapacityError()
				}
				return nil, false, authoritystore.StoredOperationOutcome{}, nil, err
			}
			typedResult, outcome, errorCode, order, decideErr := h.reserveSeedSource(rec, call, payload, currentVersion+1)
			if decideErr != nil {
				return nil, false, authoritystore.StoredOperationOutcome{}, nil, decideErr
			}
			if err := setPlantRecordVersion(typedResult, currentVersion+1); err != nil {
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

func (h *plantHandler) reserveSeedSource(rec *playerpb.PlayerRecord, call *contracts.ParticipantCall, payload *contractspb.ReserveSeedPayload, committedVersion uint64) (proto.Message, contractspb.ParticipantOutcome, commonpb.ErrorCode, authoritystore.AttachedOrder, error) {
	available := func() uint32 { return availableSeed(rec, payload.GetSeedItemId()) }
	result := &contractspb.ReserveSeedResult{
		ReservationId: payload.GetReservationId(), SeedItemId: payload.GetSeedItemId(), ReservedQuantity: payload.GetQuantity(),
	}
	cycleID, err := identity.CropCycleID(call.Work.GetWorkId())
	if err != nil {
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_DECODE, authoritystore.AttachedOrder{}, err
	}
	if existing, occupied := occupancy.PlotOccupied(rec, payload.GetPlotId()); occupied {
		result.AvailableSeedQuantityAfter = available()
		if existing == cycleID {
			return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE, commonpb.ErrorCode_OK, authoritystore.AttachedOrder{}, nil
		}
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_PLOT_NOT_EMPTY, authoritystore.AttachedOrder{}, nil
	}
	if recAvailable := playeractor.ItemCount(rec, int32(payload.GetSeedItemId())); recAvailable < int64(payload.GetQuantity()) {
		result.AvailableSeedQuantityAfter = available()
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM, authoritystore.AttachedOrder{}, nil
	}
	if !playeractor.AddItem(rec, int32(payload.GetSeedItemId()), -int64(payload.GetQuantity())) {
		result.AvailableSeedQuantityAfter = available()
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM, authoritystore.AttachedOrder{}, nil
	}
	nowMs := h.nowMs()
	if err := occupancy.OccupyPlot(rec, payload.GetPlotId(), cycleID, int32(payload.GetSeedItemId()), nowMs); err != nil {
		playeractor.AddItem(rec, int32(payload.GetSeedItemId()), int64(payload.GetQuantity()))
		result.AvailableSeedQuantityAfter = available()
		return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED, commonpb.ErrorCode_ERR_PLOT_NOT_EMPTY, authoritystore.AttachedOrder{}, nil
	}
	rec.EconomyVersion++
	result.AvailableSeedQuantityAfter = available()
	result.PlayerRecordVersion = committedVersion
	order, err := buildCommitPlantOrder(call, payload, result, cycleID, nowMs)
	if err != nil {
		return nil, 0, commonpb.ErrorCode_ERR_INTERNAL, authoritystore.AttachedOrder{}, err
	}
	return result, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, commonpb.ErrorCode_OK, order, nil
}

func buildCommitPlantOrder(call *contracts.ParticipantCall, reserve *contractspb.ReserveSeedPayload, reserveResult *contractspb.ReserveSeedResult, cycleID string, dueAtMs int64) (authoritystore.AttachedOrder, error) {
	definition, ok := cropcatalog.ByDefinitionID(int32(reserve.GetCropDefinitionId()))
	if !ok {
		return authoritystore.AttachedOrder{}, status.Error(codes.InvalidArgument, "player participant crop definition is unknown")
	}
	snapshot, err := cropcatalog.BuildCropRuleSnapshot(definition)
	if err != nil {
		return authoritystore.AttachedOrder{}, err
	}
	snapshotHash, err := contracts.CanonicalHash(snapshot)
	if err != nil || !bytes.Equal(snapshotHash[:], reserve.GetCropRuleSnapshotHash()) {
		return authoritystore.AttachedOrder{}, status.Error(codes.FailedPrecondition, "player participant crop rule snapshot hash mismatch")
	}
	reserveSpec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED)
	if err != nil {
		return authoritystore.AttachedOrder{}, err
	}
	reserveHash, err := contracts.CanonicalParticipantHash(reserveSpec, reserveResult)
	if err != nil {
		return authoritystore.AttachedOrder{}, err
	}
	commit := &contractspb.CommitPlantPayload{
		OwnerPlayerId: reserve.GetPlayerId(), FarmId: reserve.GetFarmId(), PlotId: reserve.GetPlotId(),
		ExpectedPlotVersion: 1, ReservationId: reserve.GetReservationId(),
		ReserveSeedResultHash: append([]byte(nil), reserveHash[:]...), CropCycleId: cycleID,
		CropRuleSnapshot: snapshot, CropRuleSnapshotHash: append([]byte(nil), snapshotHash[:]...),
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(commit)
	if err != nil {
		return authoritystore.AttachedOrder{}, err
	}
	hash, err := sourceorder.PlantPayloadHash(commit)
	if err != nil {
		return authoritystore.AttachedOrder{}, err
	}
	return authoritystore.AttachedOrder{
		OrderID: call.Work.GetWorkId(), OrderType: string(identity.WorkTypePlant), Payload: payload,
		PayloadHash: authoritystore.PayloadHash(hash), DueAtMs: dueAtMs,
		OperationID: call.OperationID, SubjectPlayerID: reserve.GetPlayerId(),
	}, nil
}
