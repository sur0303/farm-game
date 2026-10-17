package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	workflowpb "farmgame/api/gen/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

const InitialRecoveryDelay = 5 * time.Second
const DefaultWorkflowDeadline = 24 * time.Hour

type PlayerParticipant interface {
	CommitReservation(context.Context, *logicrpcpb.CommitReservationRequest) (*logicrpcpb.ReservationResponse, error)
	ReleaseReservation(context.Context, *logicrpcpb.ReleaseReservationRequest) (*logicrpcpb.ReservationResponse, error)
	GrantItem(context.Context, *logicrpcpb.GrantItemRequest) (*logicrpcpb.GrantItemResponse, error)
	ApplyReward(context.Context, *logicrpcpb.ApplyRewardRequest) (*logicrpcpb.ApplyRewardResponse, error)
}

type FarmParticipant interface {
	InspectFarmOperation(context.Context, *logicrpcpb.InspectFarmOperationRequest) (*logicrpcpb.InspectFarmOperationResponse, error)
	CompleteFarmOperation(context.Context, *logicrpcpb.CompleteFarmOperationRequest) (*logicrpcpb.CompleteFarmOperationResponse, error)
}

type Runner struct {
	store  Store
	player PlayerParticipant
	farm   FarmParticipant
	now    func() time.Time
}

func NewRunner(store Store, player PlayerParticipant, farm FarmParticipant) (*Runner, error) {
	if store == nil || player == nil || farm == nil {
		return nil, errors.New("workflow: store and participants required")
	}
	return &Runner{store: store, player: player, farm: farm, now: time.Now}, nil
}

func NewRecord(workflowID, operationID string, kind workflowpb.WorkflowKind, farmID, initiator uint64, payloadHash []byte, now time.Time) *workflowpb.WorkflowRecord {
	return &workflowpb.WorkflowRecord{
		WorkflowId: workflowID, OperationId: operationID, Kind: kind, State: workflowpb.WorkflowState_PENDING,
		FarmId: farmID, InitiatorPlayerId: initiator,
		NextRetryAtMs: now.Add(InitialRecoveryDelay).UnixMilli(),
		CreatedAtMs:   now.UnixMilli(),
		UpdatedAtMs:   now.UnixMilli(),
		PayloadHash:   append([]byte(nil), payloadHash...),
		DeadlineAtMs:  now.Add(DefaultWorkflowDeadline).UnixMilli(),
	}
}

func (r *Runner) Process(ctx context.Context, record *workflowpb.WorkflowRecord) error {
	if err := Validate(record); err != nil {
		return err
	}
	if Terminal(record.State) {
		return nil
	}
	if record.LeaseOwner == "" || record.LeaseEpoch == 0 {
		return ErrLeaseLost
	}
	inspection, err := r.farm.InspectFarmOperation(ctx, &logicrpcpb.InspectFarmOperationRequest{
		FarmId: record.FarmId, OperationId: record.OperationId,
	})
	if err != nil {
		return r.retry(ctx, record, err)
	}
	if inspection.Status == commonpb.CommandStatus_REJECTED &&
		inspection.ErrorCode == commonpb.ErrorCode_ERR_SETTLEMENT_NOT_FOUND {
		if record.Kind == workflowpb.WorkflowKind_PLANT {
			reservationID, idErr := workidentity.SeedReservationID(record.WorkflowId)
			if idErr != nil {
				return r.retry(ctx, record, idErr)
			}
			response, releaseErr := r.player.ReleaseReservation(ctx, &logicrpcpb.ReleaseReservationRequest{
				PlayerId: record.InitiatorPlayerId, ReservationId: reservationID,
			})
			if releaseErr != nil {
				return r.retry(ctx, record, releaseErr)
			}
			if response.Status == commonpb.CommandStatus_REJECTED && response.ErrorCode != commonpb.ErrorCode_ERR_INSUFFICIENT_ITEM {
				return r.retry(ctx, record, errors.New("plant reservation could not be released"))
			}
		}
		return r.finish(ctx, record, workflowpb.WorkflowState_COMPENSATED)
	}
	if inspection.Status == commonpb.CommandStatus_SUCCEEDED {
		return r.finish(ctx, record, workflowpb.WorkflowState_COMPLETED)
	}
	if inspection.Status != commonpb.CommandStatus_PROCESSING {
		return r.retry(ctx, record, fmt.Errorf("unexpected farm operation status %s", inspection.Status))
	}

	var privateState []byte
	switch record.Kind {
	case workflowpb.WorkflowKind_PLANT:
		reservationID, idErr := workidentity.SeedReservationID(record.WorkflowId)
		if idErr != nil {
			return r.retry(ctx, record, idErr)
		}
		response, commitErr := r.player.CommitReservation(ctx, &logicrpcpb.CommitReservationRequest{
			PlayerId: record.InitiatorPlayerId, ReservationId: reservationID,
		})
		if commitErr != nil {
			return r.retry(ctx, record, commitErr)
		}
		if response.Status == commonpb.CommandStatus_REJECTED {
			return r.retry(ctx, record, errors.New("plant reservation commit rejected"))
		}
		privateState, _ = proto.MarshalOptions{Deterministic: true}.Marshal(response.PrivateState)
	case workflowpb.WorkflowKind_HARVEST, workflowpb.WorkflowKind_THEFT, workflowpb.WorkflowKind_CARE:
		settlement := inspection.Settlement
		if settlement == nil {
			return r.retry(ctx, record, errors.New("farm settlement is incomplete"))
		}
		participants := settlement.Participants
		if len(participants) == 0 && settlement.BeneficiaryPlayerId != 0 {
			participants = []*farmpb.FarmSettlementParticipant{{
				PlayerId: settlement.BeneficiaryPlayerId,
				ItemId:   settlement.ItemId, ItemCount: settlement.ItemCount,
				ExpDelta: settlement.ItemCount,
			}}
		}
		if len(participants) == 0 {
			return r.retry(ctx, record, errors.New("farm settlement has no participants"))
		}
		for _, participant := range participants {
			rewardHash, hashErr := RewardPayloadHash(record.PayloadHash, participant)
			if hashErr != nil {
				return r.retry(ctx, record, hashErr)
			}
			response, rewardErr := r.player.ApplyReward(ctx, &logicrpcpb.ApplyRewardRequest{
				PlayerId: participant.PlayerId, OperationId: record.OperationId,
				ItemId: participant.ItemId, ItemCount: participant.ItemCount,
				CoinDelta: participant.CoinDelta, ExpDelta: participant.ExpDelta,
				CodexCropDefinitionId: participant.CodexCropDefinitionId,
				PayloadHash:           rewardHash,
			})
			if rewardErr != nil {
				return r.retry(ctx, record, rewardErr)
			}
			if response.Status == commonpb.CommandStatus_REJECTED {
				return r.retry(ctx, record, errors.New("player reward rejected"))
			}
			if participant.PlayerId == record.InitiatorPlayerId {
				privateState, _ = proto.MarshalOptions{Deterministic: true}.Marshal(response.PrivateState)
			}
		}
	default:
		return r.retry(ctx, record, ErrInvalid)
	}

	completed, err := r.farm.CompleteFarmOperation(ctx, &logicrpcpb.CompleteFarmOperationRequest{
		FarmId: record.FarmId, OperationId: record.OperationId, PrivateState: privateState,
	})
	if err != nil {
		return r.retry(ctx, record, err)
	}
	if completed.Status != commonpb.CommandStatus_SUCCEEDED && completed.Status != commonpb.CommandStatus_NO_CHANGE {
		return r.retry(ctx, record, fmt.Errorf("farm completion returned %s", completed.Status))
	}
	return r.finish(ctx, record, workflowpb.WorkflowState_COMPLETED)
}

func (r *Runner) finish(ctx context.Context, record *workflowpb.WorkflowRecord, state workflowpb.WorkflowState) error {
	options := r.updateOptions(record)
	record.State = state
	record.LastError = ""
	record.UpdatedAtMs = r.now().UnixMilli()
	record.NextRetryAtMs = 0
	updated, err := r.store.Update(ctx, record, options)
	if err != nil {
		return err
	}
	proto.Reset(record)
	proto.Merge(record, updated)
	return nil
}

func (r *Runner) retry(ctx context.Context, record *workflowpb.WorkflowRecord, cause error) error {
	options := r.updateOptions(record)
	record.State = workflowpb.WorkflowState_RETRY_WAIT
	record.Attempts++
	record.LastError = cause.Error()
	record.UpdatedAtMs = r.now().UnixMilli()
	backoff := 250 * time.Millisecond
	for attempt := int32(1); attempt < record.Attempts && backoff < 30*time.Second; attempt++ {
		backoff *= 2
	}
	if backoff > 30*time.Second {
		backoff = 30 * time.Second
	}
	record.NextRetryAtMs = r.now().Add(backoff).UnixMilli()
	updated, err := r.store.Update(ctx, record, options)
	if err != nil {
		return errors.Join(cause, err)
	}
	proto.Reset(record)
	proto.Merge(record, updated)
	return cause
}

func (r *Runner) updateOptions(record *workflowpb.WorkflowRecord) UpdateOptions {
	return UpdateOptions{
		ExpectedRevision: record.WorkflowRevision,
		LeaseOwner:       record.LeaseOwner,
		LeaseEpoch:       record.LeaseEpoch,
		Now:              r.now(),
	}
}
