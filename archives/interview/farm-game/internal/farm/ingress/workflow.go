package ingress

import (
	"context"
	"errors"
	"strings"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	farmactor "farmgame/internal/farm/actor"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) InspectFarmOperation(ctx context.Context, request *logicrpcpb.InspectFarmOperationRequest) (*logicrpcpb.InspectFarmOperationResponse, error) {
	if request == nil || request.FarmId == 0 || strings.TrimSpace(request.OperationId) == "" {
		return nil, status.Error(codes.InvalidArgument, "farm_id and operation_id required")
	}
	value, err := s.registry.Submit(ctx, request.FarmId, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		return actor.Operation(request.OperationId)
	})
	if err != nil {
		var domain *farmactor.Error
		if errors.As(err, &domain) {
			return &logicrpcpb.InspectFarmOperationResponse{
				Status: commonpb.CommandStatus_REJECTED, ErrorCode: domain.Code,
			}, nil
		}
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	action := value.(*farmactor.ActionResult)
	return &logicrpcpb.InspectFarmOperationResponse{
		Status: action.Status, ErrorCode: commonpb.ErrorCode_OK, Settlement: action.Settlement,
	}, nil
}

func (s *Service) CompleteFarmOperation(ctx context.Context, request *logicrpcpb.CompleteFarmOperationRequest) (*logicrpcpb.CompleteFarmOperationResponse, error) {
	if request == nil || request.FarmId == 0 || strings.TrimSpace(request.OperationId) == "" {
		return nil, status.Error(codes.InvalidArgument, "farm_id and operation_id required")
	}
	value, err := s.registry.Submit(ctx, request.FarmId, func(ctx context.Context, actor *farmactor.FarmActor) (any, error) {
		action, err := actor.Operation(request.OperationId)
		if err != nil {
			return nil, err
		}
		if action.Status == commonpb.CommandStatus_SUCCEEDED {
			return commonpb.CommandStatus_NO_CHANGE, nil
		}
		if action.Result == nil {
			action.Result = &farmpb.FarmCommandResult{}
		}
		if len(request.PrivateState) > 0 {
			action.Result.PrivateState = append([]byte(nil), request.PrivateState...)
		}
		_, err = actor.CompleteOperation(ctx, request.OperationId, action.Result, s.now())
		if err != nil {
			return nil, err
		}
		return commonpb.CommandStatus_SUCCEEDED, nil
	})
	if err != nil {
		var domain *farmactor.Error
		if errors.As(err, &domain) {
			return &logicrpcpb.CompleteFarmOperationResponse{Status: commonpb.CommandStatus_REJECTED, ErrorCode: domain.Code}, nil
		}
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &logicrpcpb.CompleteFarmOperationResponse{Status: value.(commonpb.CommandStatus), ErrorCode: commonpb.ErrorCode_OK}, nil
}
