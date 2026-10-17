package ingress

import (
	"context"
	"fmt"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	farmactor "farmgame/internal/farm/actor"
	"farmgame/internal/identity"
	"farmgame/internal/logic/trusted"
	unlockscenario "farmgame/internal/scenario/unlockplot"
	"farmgame/internal/unlockplotonline"
	"farmgame/internal/worker/sourceorder"

	"google.golang.org/protobuf/proto"
)

func (s *Service) unlockPlotSourceOrder(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest, verified *trusted.VerifiedCommand) (*unlockplotonline.CommandResult, error) {
	if s.playerCoordinator == nil || s.playerRoute == nil || s.playerBucketFor == nil {
		return nil, fmt.Errorf("unlock source-order ports are unavailable")
	}
	typed, ok := verified.Decoded.(*publicpb.UnlockPlotRequest)
	if !ok || typed == nil {
		return nil, fmt.Errorf("UnlockPlot payload is not admitted")
	}
	playerID := req.Trusted.Session.PlayerId
	target := verified.RouteKey
	if target != playerID {
		return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_PERMISSION_DENIED, Message: "只能在自己的当前农场解锁地块"}
	}
	if typed.GetTargetFarmId() != target || typed.GetPlotId() >= uint32(farmactor.DefaultPlotCount) || typed.GetExpectedPlotVersion() == 0 {
		return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_VERSION_CONFLICT, Message: "解锁地块版本或目标无效"}
	}
	now := s.now()
	value, err := s.registry.Submit(ctx, target, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		return actor.Snapshot(now), nil
	})
	if err != nil {
		return nil, err
	}
	snapshot, ok := value.(*farmpb.FarmSnapshot)
	if !ok || snapshot == nil {
		return nil, fmt.Errorf("UnlockPlot snapshot read returned invalid result")
	}
	var view *farmpb.PlotView
	for _, candidate := range snapshot.GetPlots() {
		if candidate != nil && candidate.GetPlotIndex() == typed.GetPlotId() {
			view = candidate
			break
		}
	}
	if view == nil || view.GetLockState() == farmpb.PlotLockState_PLOT_LOCK_STATE_UNSPECIFIED || view.GetUnlockRuleVersion() == 0 {
		return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_INTERNAL, Message: "权威 PlotUnlock 投影不完整"}
	}
	if view.GetUnlockOrder() == 0 || view.GetRequiredLevel() == 0 || view.GetNormalCoinPrice() == 0 {
		return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_PLOT_LOCKED, Message: "地块不可购买解锁"}
	}
	expectedRuleVersion := typed.GetExpectedUnlockRuleVersion()
	if expectedRuleVersion == 0 {
		expectedRuleVersion = view.GetUnlockRuleVersion()
	}
	if expectedRuleVersion != view.GetUnlockRuleVersion() {
		return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_RULE_VERSION_MISMATCH, Message: "解锁规则版本已变化"}
	}
	ruleHash := unlockscenario.DefaultRuleSnapshotHash()
	orderID, err := sourceorder.UnlockSourceOrderID(playerID, req.Command.OperationId)
	if err != nil {
		return nil, err
	}
	reservationID, err := identity.UnlockCoinReservationID(orderID)
	if err != nil {
		return nil, err
	}
	bucket, err := s.playerBucketFor(playerID)
	if err != nil {
		return nil, err
	}
	payload := &contractspb.ReserveUnlockCoinPayload{
		PlayerId: playerID, FarmId: target, PlotId: typed.GetPlotId() + 1,
		ReservationId: reservationID, RequiredPlayerLevel: view.GetRequiredLevel(),
		AmountNormalCoin: view.GetNormalCoinPrice(), UnlockRuleVersion: expectedRuleVersion,
		UnlockRuleSnapshotHash: append([]byte(nil), ruleHash[:]...),
	}
	payloadBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		return nil, err
	}
	request, err := s.playerSourceRequest(ctx, playerID, orderID, bucket,
		contractspb.WorkType_WORK_TYPE_UNLOCK_PLOT,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_UNLOCK_COIN,
		contractspb.ParticipantStep_PARTICIPANT_STEP_RESERVE_UNLOCK_COIN,
		identity.WorkTypeUnlockPlot, identity.StepReserveUnlockCoin, req.Command.OperationId, payloadBytes)
	if err != nil {
		return nil, err
	}
	response, err := s.playerCoordinator(ctx, request)
	if err != nil {
		if mapped := playerCoordinatorFailure(err); mapped != nil {
			return nil, mapped
		}
		return &unlockplotonline.CommandResult{Status: commonpb.CommandStatus_PROCESSING}, nil
	}
	if response.GetOutcome() == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN {
		return &unlockplotonline.CommandResult{Status: commonpb.CommandStatus_PROCESSING}, nil
	}
	if response.GetOutcome() == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED {
		return nil, participantFailureError(response)
	}
	dispatched := false
	if response.GetCreatedSourceOrder() != nil && s.unlockCommittedOrderDispatcher != nil {
		committed, envelopeErr := attachedPlayerSourceOrder(response.GetCreatedSourceOrder(), bucket, playerID, orderID, string(identity.WorkTypeUnlockPlot))
		if envelopeErr == nil {
			if dispatchErr := s.unlockCommittedOrderDispatcher(ctx, playerID, committed); dispatchErr == nil {
				dispatched = true
			}
		}
	}
	if !dispatched && s.unlockOrderDispatcher != nil {
		if dispatchErr := s.unlockOrderDispatcher(ctx, playerID, orderID); dispatchErr == nil {
			dispatched = true
		}
	}
	if !dispatched {
		return &unlockplotonline.CommandResult{Status: commonpb.CommandStatus_PROCESSING}, nil
	}
	coinAfter := uint64(0)
	if spec, specErr := decodeReserveUnlockResult(response); specErr == nil {
		coinAfter = spec.GetAvailableNormalCoinAfter()
	}
	return &unlockplotonline.CommandResult{
		Status: commonpb.CommandStatus_SUCCEEDED,
		Result: &publicpb.UnlockPlotResult{
			FarmId: target, PlotId: typed.GetPlotId(),
			AppliedUnlockRuleVersion: expectedRuleVersion,
			PaidNormalCoin:           view.GetNormalCoinPrice(),
			NormalCoinBalance:        coinAfter,
		},
	}, nil
}

func decodeReserveUnlockResult(response *contractspb.ParticipantResponse) (*contractspb.ReserveUnlockCoinResult, error) {
	if response == nil || len(response.GetTypedResult()) == 0 {
		return nil, fmt.Errorf("unlock reserve result missing")
	}
	result := &contractspb.ReserveUnlockCoinResult{}
	if err := proto.Unmarshal(response.GetTypedResult(), result); err != nil {
		return nil, err
	}
	return result, nil
}
