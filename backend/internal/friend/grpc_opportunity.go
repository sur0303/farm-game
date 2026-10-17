package friend

import (
	"context"
	"errors"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/farm/readpath"
	"farmgame/internal/workidentity"
)

type GRPCOpportunityEvaluator struct {
	client  logicrpcpb.LogicServiceClient
	timeout time.Duration
}

func NewGRPCOpportunityEvaluator(client logicrpcpb.LogicServiceClient, timeout time.Duration) (*GRPCOpportunityEvaluator, error) {
	if client == nil {
		return nil, errors.New("friend opportunity Logic client is required")
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &GRPCOpportunityEvaluator{client: client, timeout: timeout}, nil
}

// BatchGetCandidates reads only FarmOpportunityHeader for coarse navigation.
// It keeps UNKNOWN rows visible to the caller and never turns malformed
// per-row protocol data into a false "no opportunity" result.
func (e *GRPCOpportunityEvaluator) BatchGetCandidates(ctx context.Context, farmIDs []uint64, nowMs int64) (map[uint64]FarmOpportunityCandidate, error) {
	if len(farmIDs) == 0 || len(farmIDs) > readpath.MaxBatchFarmIDs {
		return nil, errors.New("friend opportunity header RPC batch size is invalid")
	}
	for _, farmID := range farmIDs {
		if farmID == 0 {
			return nil, errors.New("friend opportunity header RPC farm id is required")
		}
	}
	readCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	response, err := e.client.BatchReadFarmOpportunityHeader(readCtx, &logicrpcpb.BatchReadFarmOpportunityHeaderRequest{
		OwnerPlayerIds: append([]uint64(nil), farmIDs...),
	})
	if err != nil {
		return nil, err
	}
	if len(response.GetResults()) != len(farmIDs) {
		return nil, errors.New("friend opportunity header RPC cardinality mismatch")
	}
	out := make(map[uint64]FarmOpportunityCandidate, len(farmIDs))
	for i, row := range response.GetResults() {
		if row == nil || row.GetOwnerPlayerId() != farmIDs[i] {
			return nil, errors.New("friend opportunity header RPC order mismatch")
		}
		candidate := FarmOpportunityCandidate{
			FarmID:            farmIDs[i],
			SourceFarmVersion: row.GetSourceFarmVersion(),
			State:             CandidateUnknown,
		}
		switch row.GetStatus() {
		case logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_FOUND:
			candidate.ActionableAtMs = row.GetNextOpportunityAtMs()
			candidate.State = CandidateNone
			if candidate.ActionableAtMs > 0 && candidate.ActionableAtMs <= nowMs {
				candidate.State = CandidateYes
			}
		case logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_NOT_FOUND,
			logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_UNKNOWN:
			// A missing or uncertain Header must continue to exact evaluation;
			// neither status is a known "no opportunity" fact.
		default:
			return nil, errors.New("friend opportunity header RPC status invalid")
		}
		out[farmIDs[i]] = candidate
	}
	return out, nil
}

func (e *GRPCOpportunityEvaluator) BatchEvaluateFriendOpportunity(ctx context.Context, viewer uint64, ownerIDs []uint64, nowMs int64) ([]FriendOpportunityEvaluation, error) {
	readCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	response, err := e.client.BatchEvaluateFarmOpportunity(readCtx, &logicrpcpb.BatchEvaluateFarmOpportunityRequest{
		ViewerPlayerId: viewer, OwnerPlayerIds: append([]uint64(nil), ownerIDs...), TrustedNowMs: nowMs,
	})
	if err != nil {
		return nil, err
	}
	if len(response.GetResults()) != len(ownerIDs) {
		return nil, errors.New("friend opportunity RPC cardinality mismatch")
	}
	out := make([]FriendOpportunityEvaluation, len(ownerIDs))
	for i, row := range response.Results {
		if row == nil || row.OwnerPlayerId != ownerIDs[i] {
			return nil, errors.New("friend opportunity RPC order mismatch")
		}
		result, decodeErr := friendOpportunityEvaluationFromRPC(row)
		if decodeErr != nil {
			return nil, decodeErr
		}
		out[i] = result
	}
	return out, nil
}

// BatchEvaluate adapts the detailed farm read to Navigator's compact exact
// port. UNKNOWN remains explicit; it is never converted into ActionNone.
func (e *GRPCOpportunityEvaluator) BatchEvaluate(ctx context.Context, viewer uint64, ownerIDs []uint64, nowMs int64) (map[uint64]ActionMask, error) {
	rows, err := e.BatchEvaluateFriendOpportunity(ctx, viewer, ownerIDs, nowMs)
	if err != nil {
		return nil, err
	}
	result := make(map[uint64]ActionMask, len(rows))
	for _, row := range rows {
		if row.Status != OpportunityReadReady {
			result[row.OwnerPlayerID] = ActionUnknown
			continue
		}
		result[row.OwnerPlayerID] = ActionMask(row.ActionMask)
	}
	return result, nil
}

func friendOpportunityEvaluationFromRPC(row *logicrpcpb.FarmOpportunityReadResult) (FriendOpportunityEvaluation, error) {
	result := FriendOpportunityEvaluation{OwnerPlayerID: row.GetOwnerPlayerId(), Status: OpportunityReadUnknown}
	switch row.GetStatus() {
	case logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_FOUND:
		candidate := FriendOpportunityEvaluation{
			OwnerPlayerID: row.GetOwnerPlayerId(), Status: OpportunityReadReady,
			ActionMask: row.GetActionMask(), OpportunityRevision: row.GetOpportunityRevision(),
		}
		seenPlots := make(map[uint32]struct{}, len(row.GetEligibleTheftTargets()))
		seenCycles := make(map[string]struct{}, len(row.GetEligibleTheftTargets()))
		for _, target := range row.GetEligibleTheftTargets() {
			if target == nil || target.GetPlotId() == 0 || target.GetPlotId() > 24 || workidentity.ValidateStableOpaqueID(target.GetCropCycleId()) != nil {
				return result, nil
			}
			if _, duplicate := seenPlots[target.GetPlotId()]; duplicate {
				return result, nil
			}
			if _, duplicate := seenCycles[target.GetCropCycleId()]; duplicate {
				return result, nil
			}
			seenPlots[target.GetPlotId()] = struct{}{}
			seenCycles[target.GetCropCycleId()] = struct{}{}
			candidate.EligibleTheftTargets = append(candidate.EligibleTheftTargets,
				FriendTheftTarget{PlotID: target.GetPlotId(), CropCycleID: target.GetCropCycleId()})
		}
		if ((row.GetActionMask() & FriendActionCanTheft) != 0) != (len(candidate.EligibleTheftTargets) > 0) {
			return result, nil
		}
		return candidate, nil
	case logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_NOT_FOUND,
		logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_UNKNOWN:
		// Any targets on a non-ready row are discarded. Preserve UNKNOWN rather
		// than failing the whole page or interpreting malformed data as NONE.
		return result, nil
	default:
		return FriendOpportunityEvaluation{}, errors.New("friend opportunity RPC status invalid")
	}
}

var _ CandidateReader = (*GRPCOpportunityEvaluator)(nil)
var _ ExactEvaluator = (*GRPCOpportunityEvaluator)(nil)
var _ FriendOpportunityBatchEvaluator = (*GRPCOpportunityEvaluator)(nil)
