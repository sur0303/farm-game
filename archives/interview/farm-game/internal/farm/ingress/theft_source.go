package ingress

import (
	"context"
	"fmt"
	"strconv"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/contracts"
	farmactor "farmgame/internal/farm/actor"
	"farmgame/internal/identity"
	"farmgame/internal/scenario/theft"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/sourceorder"

	"google.golang.org/protobuf/proto"
)

// CommitTheftSource writes DecideTheft + THEFT_VISITOR/THEFT_GUARD orders in
// one Farm Lua. Pair admission remains the caller; this only replaces the
// WorkRecord engine as the propagator.
func CommitTheftSource(
	ctx context.Context,
	registry *farmactor.Registry,
	bucketFor func(uint64) (rediskey.AuthorityBucketRef, error),
	input theft.Input,
	nowMs int64,
	publish func(uint64, *farmactor.ActionResult),
) (visitorOrderID, guardOrderID string, err error) {
	if registry == nil || bucketFor == nil {
		return "", "", fmt.Errorf("theft source-order farm ports are unavailable")
	}
	call, err := BuildTheftSourceCall(input, bucketFor)
	if err != nil {
		return "", "", err
	}
	value, err := registry.SubmitParticipant(ctx, input.TargetFarmID, func(turnCtx context.Context, actor *farmactor.FarmActor, guard farmactor.ParticipantCapacityGuard) (any, error) {
		return actor.DecideTheftWithAction(turnCtx, call, guard, func() int64 { return nowMs })
	})
	if err != nil {
		return "", "", err
	}
	turn, ok := value.(*farmactor.DecideTheftTurnResult)
	if !ok || turn == nil || turn.Outcome == nil {
		return "", "", fmt.Errorf("theft source commit returned an invalid farm turn")
	}
	if turn.Action != nil {
		// DecideTheftWithAction has already committed the FarmActor turn and
		// advanced the subscription Hub cursor. The online SourceOrder path does
		// not pass through Service.ExecuteCommand, so it must still hand the
		// committed candidate to the normal Gate downlink publisher; otherwise
		// the owner remains on the pre-theft plot version until a manual Join.
		if publish != nil {
			publish(input.TargetFarmID, turn.Action)
		}
		return turn.Action.TheftVisitorOrderID, turn.Action.TheftGuardOrderID, nil
	}
	return "", "", nil
}

// BuildTheftSourceCall is the shared DECIDE_THEFT Participant call used by
// in-process farm-svr SourceCommit and farm-worker recovery ResumeStep.
func BuildTheftSourceCall(input theft.Input, bucketFor func(uint64) (rediskey.AuthorityBucketRef, error)) (*contracts.ParticipantCall, error) {
	root, err := theft.BuildRoot(input)
	if err != nil {
		return nil, err
	}
	visitorOrderID, err := sourceorder.TheftVisitorSourceOrderID(input.TargetFarmID, input.OperationID)
	if err != nil {
		return nil, err
	}
	settlementID, err := identity.TheftSettlementID(visitorOrderID)
	if err != nil {
		return nil, err
	}
	payload := proto.Clone(root.Payload).(*contractspb.DecideTheftPayload)
	payload.SettlementId = settlementID
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT)
	if err != nil {
		return nil, err
	}
	stepHash, err := contracts.CanonicalParticipantHash(spec, payload)
	if err != nil {
		return nil, err
	}
	invocation, err := identity.ParticipantInvocationID(identity.WorkTypeTheft, visitorOrderID, identity.StepDecideTheft, 1)
	if err != nil {
		return nil, err
	}
	bucket, err := bucketFor(input.TargetFarmID)
	if err != nil {
		return nil, err
	}
	domain := logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM
	return &contracts.ParticipantCall{
		Spec: spec,
		RouteHeader: &logicrpcpb.ActorRouteHeader{
			RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: input.TargetFarmID,
		},
		Work: &contractspb.WorkAddress{
			WorkId: visitorOrderID, WorkType: contractspb.WorkType_WORK_TYPE_THEFT,
			PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
			BucketVersion: 1, BucketId: bucket.VirtualShard(),
			SourceAuthority: &contractspb.SourceAuthorityRef{
				AuthorityKind:          contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_FARM_ACTOR,
				CanonicalAuthorityId:   strconv.FormatUint(input.TargetFarmID, 10),
				AuthorityLayoutVersion: bucket.LayoutVersion(),
				ActorRouteDomain:       &domain,
			},
		},
		CallerKind: contracts.ParticipantCallerCoordinator, ParticipantStep: string(spec.Step),
		InvocationID: invocation, OperationID: input.OperationID, SubjectPlayerID: input.VisitorPlayerID,
		ExpectedWorkVersion: 1, RemainingBudgetMs: uint32(contracts.ParticipantMaxBudgetMs),
		Payload: payload, CanonicalStepPayloadHash: stepHash,
	}, nil
}
