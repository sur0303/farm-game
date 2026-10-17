package ingress

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/contracts"
	"farmgame/internal/cropcatalog"
	farmactor "farmgame/internal/farm/actor"
	"farmgame/internal/identity"
	"farmgame/internal/logic/trusted"
	plantscenario "farmgame/internal/scenario/plant"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/sourceorder"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// PlayerCoordinatorExecute is the farm-svr typed Player Coordinator port used
// by PLANT/UNLOCK SourceOrder FirstHop. The dest Farm hop is DirectDispatcher.
type PlayerCoordinatorExecute func(context.Context, *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error)

// ActorRouteResolver returns a current fenced route header for one actor id.
type ActorRouteResolver func(context.Context, uint64) (*logicrpcpb.ActorRouteHeader, error)

// SourceOrderDispatcher runs Direct.DispatchOnce for one source-persistence order.
type SourceOrderDispatcher func(ctx context.Context, sourceActorID uint64, orderID string) error

// CommittedPlayerSourceOrderDispatcher avoids reading back the exact order
// just committed by Player. Destination Participant Redis verification remains
// mandatory, so this envelope cannot authorize a Farm mutation by itself.
type CommittedPlayerSourceOrderDispatcher func(ctx context.Context, sourceActorID uint64, order authoritystore.AttachedOrder) error

func (s *Service) SetPlayerCoordinator(exec PlayerCoordinatorExecute, route ActorRouteResolver) {
	if s == nil {
		return
	}
	s.playerCoordinator = exec
	s.playerRoute = route
}

func (s *Service) SetPlantOrderDispatcher(dispatch SourceOrderDispatcher) {
	if s != nil {
		s.plantOrderDispatcher = dispatch
	}
}

func (s *Service) SetUnlockOrderDispatcher(dispatch SourceOrderDispatcher) {
	if s != nil {
		s.unlockOrderDispatcher = dispatch
	}
}

func (s *Service) SetPlantCommittedOrderDispatcher(dispatch CommittedPlayerSourceOrderDispatcher) {
	if s != nil {
		s.plantCommittedOrderDispatcher = dispatch
	}
}

func (s *Service) SetUnlockCommittedOrderDispatcher(dispatch CommittedPlayerSourceOrderDispatcher) {
	if s != nil {
		s.unlockCommittedOrderDispatcher = dispatch
	}
}

func (s *Service) SetPlayerBucketResolver(resolve func(uint64) (rediskey.AuthorityBucketRef, error)) {
	if s != nil {
		s.playerBucketFor = resolve
	}
}

func (s *Service) SetTheftOrderEnqueuer(visitor, guard CareOrderEnqueuer) {
	if s == nil {
		return
	}
	s.theftVisitorEnqueuer = visitor
	s.theftGuardEnqueuer = guard
}

func (s *Service) plantSourceOrder(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest, verified *trusted.VerifiedCommand, now time.Time) (*farmactor.ActionResult, error) {
	if s.playerCoordinator == nil || s.playerRoute == nil || s.playerBucketFor == nil {
		return nil, fmt.Errorf("plant source-order ports are unavailable")
	}
	typed, ok := verified.Decoded.(*publicpb.PlantRequest)
	if !ok || typed == nil {
		return nil, fmt.Errorf("Plant payload is not admitted")
	}
	playerID := req.Trusted.Session.PlayerId
	target := verified.RouteKey
	if target != playerID {
		return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_PERMISSION_DENIED, Message: "只能在自己的农场播种"}
	}
	cropDefinitionID := typed.GetCropDefinitionId()
	if cropDefinitionID == 0 {
		cropDefinitionID = uint32(cropcatalog.CarrotDefinitionID)
	}
	definition, ok := cropcatalog.ByDefinitionID(int32(cropDefinitionID))
	if !ok {
		return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_RULE_VERSION_MISMATCH, Message: "作物定义不存在"}
	}
	expectedRuleVersion := typed.GetExpectedCropRuleVersion()
	if expectedRuleVersion == 0 {
		expectedRuleVersion = definition.CropRuleVersion
	}
	if expectedRuleVersion != definition.CropRuleVersion {
		return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_RULE_VERSION_MISMATCH, Message: "作物规则版本不匹配"}
	}
	snapshot, err := cropcatalog.BuildCropRuleSnapshot(definition)
	if err != nil {
		return nil, err
	}
	snapshotHash, err := contracts.CanonicalHash(snapshot)
	if err != nil {
		return nil, err
	}
	orderID, err := sourceorder.PlantSourceOrderID(playerID, req.Command.OperationId)
	if err != nil {
		return nil, err
	}
	reservationID, err := identity.SeedReservationID(orderID)
	if err != nil {
		return nil, err
	}
	bucket, err := s.playerBucketFor(playerID)
	if err != nil {
		return nil, err
	}
	payload := &contractspb.ReserveSeedPayload{
		PlayerId: playerID, FarmId: target, PlotId: typed.GetPlotId() + 1,
		ReservationId: reservationID, CropDefinitionId: cropDefinitionID,
		SeedItemId: uint32(definition.SeedItemID), Quantity: plantscenario.Quantity,
		CropRuleVersion: expectedRuleVersion, CropRuleSnapshotHash: append([]byte(nil), snapshotHash[:]...),
	}
	payloadBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		return nil, err
	}
	request, err := s.playerSourceRequest(ctx, playerID, orderID, bucket,
		contractspb.WorkType_WORK_TYPE_PLANT,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED,
		contractspb.ParticipantStep_PARTICIPANT_STEP_RESERVE_SEED,
		identity.WorkTypePlant, identity.StepReserveSeed, req.Command.OperationId, payloadBytes)
	if err != nil {
		return nil, err
	}
	response, err := s.playerCoordinator(ctx, request)
	if err != nil {
		if mapped := playerCoordinatorFailure(err); mapped != nil {
			return nil, mapped
		}
		log.Printf("plant source-order player coordinator: %v", err)
		return &farmactor.ActionResult{Status: commonpb.CommandStatus_PROCESSING, Result: &farmpb.FarmCommandResult{}}, nil
	}
	if response.GetOutcome() == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN {
		return &farmactor.ActionResult{Status: commonpb.CommandStatus_PROCESSING, Result: &farmpb.FarmCommandResult{}}, nil
	}
	if response.GetOutcome() == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED {
		return nil, participantFailureError(response)
	}
	if response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED &&
		response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE {
		return nil, fmt.Errorf("plant source player outcome is not terminal")
	}
	dispatched := false
	if response.GetCreatedSourceOrder() != nil && s.plantCommittedOrderDispatcher != nil {
		committed, envelopeErr := attachedPlayerSourceOrder(response.GetCreatedSourceOrder(), bucket, playerID, orderID, string(identity.WorkTypePlant))
		if envelopeErr != nil {
			log.Printf("plant committed SourceOrder envelope rejected: %v", envelopeErr)
		} else if dispatchErr := s.plantCommittedOrderDispatcher(ctx, playerID, committed); dispatchErr != nil {
			log.Printf("plant committed source-order direct dispatch: %v", dispatchErr)
		} else {
			dispatched = true
		}
	}
	if !dispatched && s.plantOrderDispatcher != nil {
		if dispatchErr := s.plantOrderDispatcher(ctx, playerID, orderID); dispatchErr != nil {
			log.Printf("plant source-order direct dispatch: %v", dispatchErr)
			return &farmactor.ActionResult{Status: commonpb.CommandStatus_PROCESSING, Result: &farmpb.FarmCommandResult{}}, nil
		}
		dispatched = true
	}
	if !dispatched {
		log.Printf("plant source-order direct dispatcher is not configured")
		return &farmactor.ActionResult{Status: commonpb.CommandStatus_PROCESSING, Result: &farmpb.FarmCommandResult{}}, nil
	}
	return s.plantSourcePublicResult(ctx, target, now)
}

func (s *Service) plantSourcePublicResult(ctx context.Context, farmID uint64, now time.Time) (*farmactor.ActionResult, error) {
	if s.registry == nil {
		return &farmactor.ActionResult{Status: commonpb.CommandStatus_PROCESSING, Result: &farmpb.FarmCommandResult{}}, nil
	}
	value, err := s.registry.Submit(ctx, farmID, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		return actor.Snapshot(now), nil
	})
	if err != nil {
		return &farmactor.ActionResult{Status: commonpb.CommandStatus_PROCESSING, Result: &farmpb.FarmCommandResult{}}, nil
	}
	snapshot, _ := value.(*farmpb.FarmSnapshot)
	result := &farmpb.FarmCommandResult{Snapshot: snapshot}
	if s.player != nil {
		private, privateErr := s.player.GetPrivateState(ctx, &logicrpcpb.GetPrivateStateRequest{PlayerId: farmID})
		if privateErr == nil && private != nil && private.PrivateState != nil {
			if encoded, encErr := (proto.MarshalOptions{Deterministic: true}).Marshal(private.PrivateState); encErr == nil {
				result.PrivateState = encoded
			}
		}
	}
	return &farmactor.ActionResult{Status: commonpb.CommandStatus_SUCCEEDED, Result: result}, nil
}

func (s *Service) playerSourceRequest(ctx context.Context, playerID uint64, orderID string, bucket rediskey.AuthorityBucketRef, workType contractspb.WorkType, command contractspb.ParticipantCommandType, step contractspb.ParticipantStep, idType identity.WorkType, idStep identity.ParticipantStep, operationID string, payload []byte) (*contractspb.ParticipantRequest, error) {
	route, err := s.playerRoute(ctx, playerID)
	if err != nil {
		return nil, err
	}
	invocation, err := identity.ParticipantInvocationID(idType, orderID, idStep, 1)
	if err != nil {
		return nil, err
	}
	domain := logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER
	wire := &contractspb.WorkAddress{
		WorkId: orderID, WorkType: workType,
		PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
		BucketVersion: 1, BucketId: bucket.VirtualShard(),
		SourceAuthority: &contractspb.SourceAuthorityRef{
			AuthorityKind:          contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR,
			CanonicalAuthorityId:   sourceorder.CanonicalActorID(playerID),
			AuthorityLayoutVersion: bucket.LayoutVersion(),
			ActorRouteDomain:       &domain,
		},
	}
	return &contractspb.ParticipantRequest{
		RouteHeader: route, ParticipantCommand: command, TypedPayload: payload,
		CallerContext: &contractspb.ParticipantRequest_Coordinator{Coordinator: &contractspb.CoordinatorWorkContext{
			Work: wire, ExpectedWorkVersion: 1, ParticipantStep: step,
			ParticipantInvocationId: invocation, OperationId: operationID,
			SubjectPlayerId: playerID, RemainingBudgetMs: uint32(contracts.ParticipantMaxBudgetMs),
		}},
	}, nil
}

// playerCoordinatorFailure maps a Player Coordinator ExecuteStep error.
// Returns:
//   - *farmactor.Error for definite rejections (idempotency conflict, FailedPrecondition, InvalidArgument, PermissionDenied)
//   - nil if the caller should surface PROCESSING (deadline/unavailable/unknown transport)
func playerCoordinatorFailure(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	st := status.Convert(err)
	switch st.Code() {
	case codes.DeadlineExceeded, codes.Canceled, codes.Unavailable:
		return nil
	}
	if strings.Contains(st.Message(), "PARTICIPANT_IDEMPOTENCY_CONFLICT") {
		return &farmactor.Error{Code: commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT, Message: "源步骤幂等冲突"}
	}
	switch st.Code() {
	case codes.FailedPrecondition, codes.InvalidArgument, codes.PermissionDenied, codes.AlreadyExists, codes.NotFound:
		code := commonpb.ErrorCode_ERR_INTERNAL
		if st.Code() == codes.PermissionDenied {
			code = commonpb.ErrorCode_ERR_PERMISSION_DENIED
		}
		message := st.Message()
		if message == "" {
			message = "Player source step was rejected"
		}
		return &farmactor.Error{Code: code, Message: message}
	}
	return nil
}

func participantFailureError(response *contractspb.ParticipantResponse) error {
	code := commonpb.ErrorCode_ERR_INTERNAL
	message := "Player source step was rejected"
	if response != nil && response.GetFailure() != nil && response.GetFailure().GetError() != nil {
		code = response.GetFailure().GetError().GetErrorCode()
		if response.GetFailure().GetError().GetErrorMessage() != "" {
			message = response.GetFailure().GetError().GetErrorMessage()
		}
	}
	return &farmactor.Error{Code: code, Message: message}
}
