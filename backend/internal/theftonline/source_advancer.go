package theftonline

import (
	"context"
	"crypto/sha256"
	"fmt"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/contracts"
	farmingress "farmgame/internal/farm/ingress"
	"farmgame/internal/scenario/theft"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/sourceorder"
	"farmgame/internal/worker/theftrecovery"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// SourceOrderAdvancer recovers Pair THEFT dispatch by committing
// THEFT_VISITOR/THEFT_GUARD SourceOrders. It never creates a dest WorkRecord.
type SourceOrderAdvancer struct {
	sourceCommit     func(context.Context, theft.Input, theftrecovery.LeaseFence) (visitorID, guardID string, err error)
	finalizeNoChange func(context.Context, theftrecovery.DispatchOrder) error
	enqueueVisitor   func(context.Context, uint64, string) error
	enqueueGuard     func(context.Context, uint64, string) error
}

type SourceOrderAdvancerConfig struct {
	SourceCommit func(context.Context, theft.Input, theftrecovery.LeaseFence) (visitorID, guardID string, err error)
	// FinalizeNoChange closes the Pair result order when the target Farm
	// authoritatively reports NO_CHANGE. NO_CHANGE deliberately creates no
	// THEFT_VISITOR SourceOrder because the Player settlement payload requires
	// a positive theft decision; the Pair result must therefore be terminalized
	// through this explicit side effect instead of being mistaken for a missing
	// source commit.
	FinalizeNoChange func(context.Context, theftrecovery.DispatchOrder) error
	EnqueueVisitor   func(context.Context, uint64, string) error
	EnqueueGuard     func(context.Context, uint64, string) error
}

func NewSourceOrderAdvancer(config SourceOrderAdvancerConfig) (*SourceOrderAdvancer, error) {
	if config.SourceCommit == nil || config.FinalizeNoChange == nil {
		return nil, fmt.Errorf("%w: theft source commit and NO_CHANGE finalizer are required", ErrInvalidComposition)
	}
	return &SourceOrderAdvancer{
		sourceCommit: config.SourceCommit, finalizeNoChange: config.FinalizeNoChange,
		enqueueVisitor: config.EnqueueVisitor, enqueueGuard: config.EnqueueGuard,
	}, nil
}

var _ theftrecovery.TheftWorkflowAdvancer = (*SourceOrderAdvancer)(nil)

func (a *SourceOrderAdvancer) CreateIfAbsentOrContinue(
	ctx context.Context,
	request theftrecovery.AdvanceRequest,
) (theftrecovery.AdvanceResult, error) {
	return a.commit(ctx, request.Order, request.Lease, request.Identity)
}

func (a *SourceOrderAdvancer) ReceiptFirst(
	ctx context.Context,
	request theftrecovery.ReceiptRequest,
) (theftrecovery.AdvanceResult, error) {
	return a.commit(ctx, request.Order, request.Lease, request.Identity)
}

func (a *SourceOrderAdvancer) commit(
	ctx context.Context,
	order theftrecovery.DispatchOrder,
	lease theftrecovery.LeaseFence,
	ids theftrecovery.TheftIdentity,
) (theftrecovery.AdvanceResult, error) {
	if a == nil || a.sourceCommit == nil {
		return theftrecovery.AdvanceResult{}, ErrDispatchUnavailable
	}
	order, err := theftrecovery.NormalizeDispatchOrder(order)
	if err != nil {
		return theftrecovery.AdvanceResult{}, err
	}
	derived, err := theftrecovery.DeriveTheftIdentity(order)
	if err != nil || derived.WorkflowID != ids.WorkflowID {
		return theftrecovery.AdvanceResult{}, fmt.Errorf("%w: worker identity mismatch", ErrInvalidComposition)
	}
	input, err := inputFromDispatch(order)
	if err != nil {
		return theftrecovery.AdvanceResult{}, err
	}
	visitorID, guardID, commitErr := a.sourceCommit(ctx, input, lease)
	if commitErr != nil {
		return theftrecovery.AdvanceResult{
			Status: theftrecovery.AdvanceNotCommitted, ReceiptID: ids.WorkflowID,
		}, commitErr
	}
	if visitorID == "" && guardID == "" {
		// Farm NO_CHANGE is a valid terminal business answer. It does not create
		// a visitor SourceOrder, so the Pair RESULT_PUBLISH_ORDER must be closed
		// now; otherwise ResultOrderProcessor will retry forever looking for a
		// source record that is intentionally absent.
		if err := a.finalizeNoChange(ctx, order); err != nil {
			return theftrecovery.AdvanceResult{Status: theftrecovery.AdvanceNotCommitted, ReceiptID: ids.WorkflowID}, err
		}
		return theftrecovery.AdvanceResult{
			Status: theftrecovery.AdvanceNoChange, ReceiptID: ids.WorkflowID,
			ResultProofHash: sourceOrderAdvanceProof(order, visitorID, guardID),
		}, nil
	}
	if visitorID != "" && a.enqueueVisitor != nil {
		_ = a.enqueueVisitor(ctx, input.TargetFarmID, visitorID)
	}
	if guardID != "" && a.enqueueGuard != nil {
		_ = a.enqueueGuard(ctx, input.TargetFarmID, guardID)
	}
	return theftrecovery.AdvanceResult{
		Status: theftrecovery.AdvanceApplied, ReceiptID: visitorID, ResultProofHash: sourceOrderAdvanceProof(order, visitorID, guardID),
	}, nil
}

func sourceOrderAdvanceProof(order theftrecovery.DispatchOrder, visitorID, guardID string) []byte {
	h := sha256.New()
	_, _ = h.Write([]byte("theft-source-order-advance-v1"))
	_, _ = h.Write([]byte(order.OperationID))
	_, _ = h.Write([]byte(visitorID))
	_, _ = h.Write([]byte(guardID))
	_, _ = h.Write(order.CanonicalPayloadHash)
	return h.Sum(nil)
}

// SourceOrderAdvanceProof is the immutable result proof shared by the online
// accelerator and Pair recovery. Keeping one derivation prevents a direct
// completion from becoming an unverified alternate terminal path.
func SourceOrderAdvanceProof(order theftrecovery.DispatchOrder, visitorID, guardID string) []byte {
	return sourceOrderAdvanceProof(order, visitorID, guardID)
}

type FarmRecoveryClient interface {
	ResumeStep(context.Context, *contractspb.ParticipantRequest, ...grpc.CallOption) (*contractspb.ParticipantResponse, error)
}

type FarmRouteResolver func(context.Context, uint64) (*logicrpcpb.ActorRouteHeader, error)

type RecoverySourceCommitConfig struct {
	Farm      FarmRecoveryClient
	Route     FarmRouteResolver
	BucketFor func(uint64) (rediskey.AuthorityBucketRef, error)
	BudgetMs  uint32
}

func NewRecoverySourceCommit(
	config RecoverySourceCommitConfig,
) func(context.Context, theft.Input, theftrecovery.LeaseFence) (string, string, error) {
	return func(ctx context.Context, input theft.Input, lease theftrecovery.LeaseFence) (string, string, error) {
		if config.Farm == nil || config.Route == nil || config.BucketFor == nil {
			return "", "", fmt.Errorf("%w: farm recovery source commit ports are required", ErrInvalidComposition)
		}
		if lease.LeaseEpoch == 0 || lease.WorkVersion == 0 || lease.HolderID == "" || lease.BootID == "" || lease.LeaseToken == "" {
			return "", "", fmt.Errorf("%w: pair lease fence", ErrInvalidComposition)
		}
		call, err := farmingress.BuildTheftSourceCall(input, config.BucketFor)
		if err != nil {
			return "", "", err
		}
		route, err := config.Route(ctx, input.TargetFarmID)
		if err != nil {
			return "", "", err
		}
		request, err := decideTheftRecoveryRequest(route, call, lease, config.BudgetMs)
		if err != nil {
			return "", "", err
		}
		response, err := config.Farm.ResumeStep(ctx, request)
		if err != nil {
			return "", "", err
		}
		if response == nil ||
			response.GetParticipantCommand() != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT {
			return "", "", fmt.Errorf("%w: DECIDE_THEFT response", ErrInvalidComposition)
		}
		switch response.GetOutcome() {
		case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED:
		case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE:
			// A stale opportunity is a valid terminal business outcome. Farm does
			// not create settlement SourceOrders for NO_CHANGE because the Player
			// theft payload requires a positive signed quantity. Let the Pair
			// result finalizer publish NO_CHANGE instead of inventing source IDs.
			return "", "", nil
		default:
			return "", "", fmt.Errorf("%w: DECIDE_THEFT outcome %s", ErrDispatchUnavailable, response.GetOutcome().String())
		}
		visitorID, err := sourceorder.TheftVisitorSourceOrderID(input.TargetFarmID, input.OperationID)
		if err != nil {
			return "", "", err
		}
		guardID, err := sourceorder.TheftGuardSourceOrderID(input.TargetFarmID, input.OperationID)
		if err != nil {
			return "", "", err
		}
		return visitorID, guardID, nil
	}
}

func decideTheftRecoveryRequest(
	route *logicrpcpb.ActorRouteHeader,
	call *contracts.ParticipantCall,
	lease theftrecovery.LeaseFence,
	budget uint32,
) (*contractspb.ParticipantRequest, error) {
	if route == nil || call == nil || call.Work == nil || call.Payload == nil {
		return nil, fmt.Errorf("%w: DECIDE_THEFT recovery request", ErrInvalidComposition)
	}
	if route.GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM ||
		route.GetActorId() == 0 {
		return nil, fmt.Errorf("%w: FARM route", ErrInvalidComposition)
	}
	payload, err := proto.Marshal(call.Payload)
	if err != nil {
		return nil, err
	}
	if budget == 0 {
		budget = uint32(contracts.ParticipantMaxBudgetMs)
	}
	return &contractspb.ParticipantRequest{
		RouteHeader:        route,
		ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT,
		TypedPayload:       payload,
		CallerContext: &contractspb.ParticipantRequest_Recovery{Recovery: &contractspb.InternalCallerContext{
			CallerType:              contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY,
			CallerInstanceId:        lease.HolderID,
			CallerBootId:            lease.BootID,
			Work:                    call.Work,
			ExpectedWorkVersion:     lease.WorkVersion,
			ParticipantStep:         contractspb.ParticipantStep_PARTICIPANT_STEP_DECIDE_THEFT,
			ParticipantInvocationId: call.InvocationID,
			LeaseOwnerToken:         lease.LeaseToken,
			LeaseEpoch:              lease.LeaseEpoch,
			RemainingBudgetMs:       budget,
		}},
	}, nil
}
