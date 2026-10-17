package ingress

import (
	"context"
	"errors"

	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/farm/domain"
	"farmgame/internal/farm/readpath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type FriendOpportunityReader interface {
	BatchEvaluate(context.Context, uint64, []uint64, int64) ([]readpath.EvaluationResult, error)
}

type FriendOpportunityHeaderReader interface {
	BatchReadHeaders(context.Context, []uint64) ([]readpath.HeaderResult, error)
}

// BatchEvaluateFarmOpportunity is a read-only service-LB RPC. Its provider
// reads Header/Facts model fields directly and never resolves or activates a
// FarmActor. Per-row corruption/unavailability remains UNKNOWN; a batch-level
// store failure is a retryable RPC failure.
func (s *Service) BatchEvaluateFarmOpportunity(ctx context.Context, req *logicrpcpb.BatchEvaluateFarmOpportunityRequest) (*logicrpcpb.BatchEvaluateFarmOpportunityResponse, error) {
	if req == nil || req.ViewerPlayerId == 0 || req.TrustedNowMs <= 0 ||
		len(req.OwnerPlayerIds) == 0 || len(req.OwnerPlayerIds) > readpath.MaxBatchFarmIDs {
		return nil, status.Error(codes.InvalidArgument, "invalid friend opportunity batch")
	}
	if s == nil || s.friendOpportunityReader == nil {
		return nil, status.Error(codes.Unavailable, "friend opportunity authority reader unavailable")
	}
	rows, err := s.friendOpportunityReader.BatchEvaluate(ctx, req.ViewerPlayerId, req.OwnerPlayerIds, req.TrustedNowMs)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if len(rows) != len(req.OwnerPlayerIds) {
		return nil, status.Error(codes.DataLoss, "friend opportunity batch cardinality mismatch")
	}
	response := &logicrpcpb.BatchEvaluateFarmOpportunityResponse{Results: make([]*logicrpcpb.FarmOpportunityReadResult, 0, len(rows))}
	for i, row := range rows {
		if row.FarmID != req.OwnerPlayerIds[i] {
			return nil, status.Error(codes.DataLoss, "friend opportunity batch order mismatch")
		}
		result := &logicrpcpb.FarmOpportunityReadResult{OwnerPlayerId: row.FarmID,
			Status: logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_UNKNOWN}
		switch row.Status {
		case readpath.StatusFound:
			if row.Err != nil {
				break
			}
			if ((row.Evaluation.Mask & domain.ViewerCanSteal) != 0) != (len(row.Evaluation.EligibleTheftTargets) > 0) {
				return nil, status.Error(codes.DataLoss, "friend opportunity theft target mask mismatch")
			}
			result.Status = logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_FOUND
			result.ActionMask = uint32(row.Evaluation.Mask)
			result.OpportunityRevision = row.OpportunityRevision
			result.EligibleTheftTargets = make([]*logicrpcpb.FarmOpportunityTheftTarget, 0, len(row.Evaluation.EligibleTheftTargets))
			for _, target := range row.Evaluation.EligibleTheftTargets {
				result.EligibleTheftTargets = append(result.EligibleTheftTargets, &logicrpcpb.FarmOpportunityTheftTarget{
					PlotId: target.PlotID, CropCycleId: target.CycleID,
				})
			}
		case readpath.StatusNotFound:
			result.Status = logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_NOT_FOUND
		case readpath.StatusUnknown:
		default:
			if row.Err == nil {
				return nil, status.Error(codes.DataLoss, "invalid friend opportunity row status")
			}
		}
		response.Results = append(response.Results, result)
	}
	if len(response.Results) == 0 {
		return nil, status.Error(codes.DataLoss, errors.New("empty friend opportunity result").Error())
	}
	return response, nil
}

// BatchReadFarmOpportunityHeader is the read-only Header coarse-filter RPC.
// The provider reads only the compact Header model and never activates a
// FarmActor. Per-row read uncertainty remains UNKNOWN; a store or batch-shape
// failure is returned as an RPC error.
func (s *Service) BatchReadFarmOpportunityHeader(ctx context.Context, req *logicrpcpb.BatchReadFarmOpportunityHeaderRequest) (*logicrpcpb.BatchReadFarmOpportunityHeaderResponse, error) {
	if req == nil || len(req.OwnerPlayerIds) == 0 || len(req.OwnerPlayerIds) > readpath.MaxBatchFarmIDs {
		return nil, status.Error(codes.InvalidArgument, "invalid friend opportunity header batch")
	}
	for _, ownerID := range req.OwnerPlayerIds {
		if ownerID == 0 {
			return nil, status.Error(codes.InvalidArgument, "friend opportunity header owner is required")
		}
	}
	if s == nil {
		return nil, status.Error(codes.Unavailable, "friend opportunity header authority reader unavailable")
	}
	reader, ok := s.friendOpportunityReader.(FriendOpportunityHeaderReader)
	if !ok || reader == nil {
		return nil, status.Error(codes.Unavailable, "friend opportunity header authority reader unavailable")
	}
	rows, err := reader.BatchReadHeaders(ctx, req.OwnerPlayerIds)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if len(rows) != len(req.OwnerPlayerIds) {
		return nil, status.Error(codes.DataLoss, "friend opportunity header batch cardinality mismatch")
	}
	response := &logicrpcpb.BatchReadFarmOpportunityHeaderResponse{
		Results: make([]*logicrpcpb.FarmOpportunityHeaderReadResult, 0, len(rows)),
	}
	for i, row := range rows {
		if row.FarmID != req.OwnerPlayerIds[i] {
			return nil, status.Error(codes.DataLoss, "friend opportunity header batch order mismatch")
		}
		result := &logicrpcpb.FarmOpportunityHeaderReadResult{
			OwnerPlayerId: row.FarmID,
			Status:        logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_UNKNOWN,
		}
		if row.Err != nil {
			response.Results = append(response.Results, result)
			continue
		}
		switch row.Status {
		case readpath.StatusFound:
			result.Status = logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_FOUND
			result.SourceFarmVersion = row.Header.SourceFarmVersion
			result.NextOpportunityAtMs = row.Header.NextOpportunityAtMs
		case readpath.StatusNotFound:
			result.Status = logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_NOT_FOUND
		case readpath.StatusUnknown:
		default:
			return nil, status.Error(codes.DataLoss, "invalid friend opportunity header row status")
		}
		response.Results = append(response.Results, result)
	}
	if len(response.Results) == 0 {
		return nil, status.Error(codes.DataLoss, errors.New("empty friend opportunity header result").Error())
	}
	return response, nil
}
