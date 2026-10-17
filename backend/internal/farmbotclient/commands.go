package farmbotclient

import (
	"context"
	"errors"

	commonpb "farmgame/api/gen/common"
	publicpb "farmgame/api/gen/public"
)

// Aliases keep convenience calls tied to the generated Proto shapes. No
// parallel client-side payload schema is introduced.
type BuyRequest = publicpb.BuyRequest
type SellRequest = publicpb.SellRequest
type PlantRequest = publicpb.PlantRequest
type CareRequest = publicpb.CareRequest
type TheftRequest = publicpb.TheftRequest
type HarvestRequest = publicpb.HarvestRequest
type UnlockPlotRequest = publicpb.UnlockPlotRequest
type CareType = publicpb.CareType

const (
	CareWater      = publicpb.CareType_CARE_TYPE_WATER
	CareRemovePest = publicpb.CareType_CARE_TYPE_PEST
)

func (s *Session) Buy(ctx context.Context, operationID, requestID string, request *publicpb.BuyRequest) (*CommandResult, error) {
	if request == nil {
		return nil, errors.New("farmbotclient: Buy request is nil")
	}
	result, err := s.Execute(ctx, ExecuteRequest{
		OperationID: operationID, RequestID: requestID,
		CommandType: commonpb.CommandType_BUY, Payload: request,
	})
	return requireImmediateTerminal(result, err)
}

// Sell turns harvested produce back into coins. A bot that only ever buys
// runs out after its starting balance and then spends the rest of the run
// being rejected, which reduces the load it applies instead of sustaining it.
func (s *Session) Sell(ctx context.Context, operationID, requestID string, request *publicpb.SellRequest) (*CommandResult, error) {
	if request == nil {
		return nil, errors.New("farmbotclient: Sell request is nil")
	}
	result, err := s.Execute(ctx, ExecuteRequest{
		OperationID: operationID, RequestID: requestID,
		CommandType: commonpb.CommandType_SELL, Payload: request,
	})
	return requireImmediateTerminal(result, err)
}

func (s *Session) Plant(ctx context.Context, operationID, requestID string, request *publicpb.PlantRequest) (*CommandResult, error) {
	if request == nil {
		return nil, errors.New("farmbotclient: Plant request is nil")
	}
	result, err := s.PlantAdmission(ctx, operationID, requestID, request)
	return s.resolveIfProcessing(ctx, result, err, OperationResolutionRequest{
		CommandType:            commonpb.CommandType_PLANT,
		OperationID:            operationID,
		RequestID:              requestID,
		TargetFarmID:           request.GetTargetFarmId(),
		ServerOperationLocator: resultLocator(result),
	})
}

// PlantAdmission returns the first server verdict without waiting for a
// PROCESSING durable workflow to settle. Sustained bots use this admission
// path so one slow cross-actor operation does not cancel the whole session;
// the original operation ID remains the only retry identity.
func (s *Session) PlantAdmission(ctx context.Context, operationID, requestID string, request *publicpb.PlantRequest) (*CommandResult, error) {
	if request == nil {
		return nil, errors.New("farmbotclient: Plant request is nil")
	}
	return s.Execute(ctx, ExecuteRequest{
		OperationID: operationID, RequestID: requestID,
		CommandType: commonpb.CommandType_PLANT, Payload: request,
	})
}

func (s *Session) Care(ctx context.Context, operationID, requestID string, request *publicpb.CareRequest) (*CommandResult, error) {
	if request == nil {
		return nil, errors.New("farmbotclient: Care request is nil")
	}
	result, err := s.Execute(ctx, ExecuteRequest{
		OperationID: operationID, RequestID: requestID,
		CommandType: commonpb.CommandType_CARE, Payload: request,
	})
	return requireImmediateTerminal(result, err)
}

func (s *Session) Theft(ctx context.Context, operationID, requestID string, request *publicpb.TheftRequest) (*CommandResult, error) {
	if request == nil {
		return nil, errors.New("farmbotclient: Theft request is nil")
	}
	result, err := s.Execute(ctx, ExecuteRequest{
		OperationID: operationID, RequestID: requestID,
		CommandType: commonpb.CommandType_THEFT, Payload: request,
	})
	return s.resolveIfProcessing(ctx, result, err, OperationResolutionRequest{
		CommandType:            commonpb.CommandType_THEFT,
		OperationID:            operationID,
		RequestID:              requestID,
		TargetFarmID:           request.GetTargetFarmId(),
		ServerOperationLocator: resultLocator(result),
	})
}

func (s *Session) Harvest(ctx context.Context, operationID, requestID string, request *publicpb.HarvestRequest) (*CommandResult, error) {
	if request == nil {
		return nil, errors.New("farmbotclient: Harvest request is nil")
	}
	result, err := s.HarvestAdmission(ctx, operationID, requestID, request)
	return s.resolveIfProcessing(ctx, result, err, OperationResolutionRequest{
		CommandType:            commonpb.CommandType_HARVEST,
		OperationID:            operationID,
		RequestID:              requestID,
		TargetFarmID:           request.GetTargetFarmId(),
		ServerOperationLocator: resultLocator(result),
	})
}

// HarvestAdmission returns the first server verdict without waiting for a
// PROCESSING durable workflow to settle. The caller must use Player-visible
// inventory before issuing SELL when the verdict is not terminal.
func (s *Session) HarvestAdmission(ctx context.Context, operationID, requestID string, request *publicpb.HarvestRequest) (*CommandResult, error) {
	if request == nil {
		return nil, errors.New("farmbotclient: Harvest request is nil")
	}
	return s.Execute(ctx, ExecuteRequest{
		OperationID: operationID, RequestID: requestID,
		CommandType: commonpb.CommandType_HARVEST, Payload: request,
	})
}

// UnlockPlot buys one more plot. Plots unlock in a fixed order, so a caller
// working towards a full farm must unlock the candidate the server names
// rather than picking a plot id.
func (s *Session) UnlockPlot(ctx context.Context, operationID, requestID string, request *publicpb.UnlockPlotRequest) (*CommandResult, error) {
	if request == nil {
		return nil, errors.New("farmbotclient: UnlockPlot request is nil")
	}
	result, err := s.UnlockPlotAdmission(ctx, operationID, requestID, request)
	return s.resolveIfProcessing(ctx, result, err, OperationResolutionRequest{
		CommandType:            commonpb.CommandType_UNLOCK_PLOT,
		OperationID:            operationID,
		RequestID:              requestID,
		TargetFarmID:           request.GetTargetFarmId(),
		ServerOperationLocator: resultLocator(result),
	})
}

// UnlockPlotAdmission sends UNLOCK_PLOT and returns the first server verdict.
// A warmup caller may deliberately stop after PROCESSING: the durable
// workflow remains identified by the original operation ID and must not be
// retried, while the bot need not hold the entire warmup behind one slow
// cross-actor resolution.
func (s *Session) UnlockPlotAdmission(ctx context.Context, operationID, requestID string, request *publicpb.UnlockPlotRequest) (*CommandResult, error) {
	if request == nil {
		return nil, errors.New("farmbotclient: UnlockPlot request is nil")
	}
	return s.Execute(ctx, ExecuteRequest{
		OperationID: operationID, RequestID: requestID,
		CommandType: commonpb.CommandType_UNLOCK_PLOT, Payload: request,
	})
}

// GetPlayerBootstrap is the WSS fallback for a login that did not attach a
// home projection. Callers that already have LoginResult.HomeBootstrap must
// not send this; the contract treats a present projection as the first paint.
func (s *Session) GetPlayerBootstrap(ctx context.Context, operationID, requestID string) (*CommandResult, error) {
	result, err := s.Execute(ctx, ExecuteRequest{
		OperationID: operationID, RequestID: requestID,
		CommandType: commonpb.CommandType_GET_PLAYER_BOOTSTRAP,
		Payload:     &publicpb.GetPlayerBootstrapRequest{},
	})
	return requireImmediateTerminal(result, err)
}

func (s *Session) resolveIfProcessing(ctx context.Context, result *CommandResult, err error, request OperationResolutionRequest) (*CommandResult, error) {
	if err != nil || result == nil || result.GetStatus() != commonpb.CommandStatus_PROCESSING {
		return result, err
	}
	return s.ResolveOperation(ctx, request)
}

func resultLocator(result *CommandResult) string {
	if result == nil {
		return ""
	}
	return result.GetServerOperationLocator()
}

func requireImmediateTerminal(result *CommandResult, err error) (*CommandResult, error) {
	if err != nil || result == nil || result.GetStatus() != commonpb.CommandStatus_PROCESSING {
		return result, err
	}
	return result, &UnexpectedStatusError{OperationID: result.GetOperationId(), Status: result.GetStatus()}
}
