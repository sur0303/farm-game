package plant

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/contracts"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// RouteProvider supplies the already-admitted physical actor route fence.
// Routing, holder fencing and bucket resolution stay in composition roots;
// the scenario only checks that the route agrees with the typed payload.
type RouteProvider func(context.Context, workflow.StepCall, contractspb.ParticipantCommandType, uint64) (*logicrpcpb.ActorRouteHeader, error)

// CoordinatorParticipantClient is the existing Coordinator Participant RPC
// contract. It is deliberately narrower than a concrete Player/Farm client.
type CoordinatorParticipantClient interface {
	ExecuteStep(context.Context, *contractspb.ParticipantRequest, ...grpc.CallOption) (*contractspb.ParticipantResponse, error)
}

// WorkerRecoveryParticipantClient is the existing Worker Recovery Participant
// RPC contract. It is intentionally distinct from the coordinator client so a
// recovery adapter cannot accidentally send CoordinatorWorkContext to
// ResumeStep.
type WorkerRecoveryParticipantClient interface {
	ResumeStep(context.Context, *contractspb.ParticipantRequest, ...grpc.CallOption) (*contractspb.ParticipantResponse, error)
}

// WorkerLeaseContext is the trusted lease identity supplied by the recovery
// composition. The Participant ingress rechecks it against the Work
// Repository before a first execution.
type WorkerLeaseContext struct {
	CallerInstanceID string
	CallerBootID     string
	LeaseOwnerToken  string
	LeaseEpoch       uint64
}

// RPCInvoker adapts one existing Coordinator Participant RPC endpoint to the
// generic workflow.ParticipantInvoker boundary. Configure one instance for
// the Player authority and one for the Farm authority, then pass both to
// NewDefinition.
type RPCInvoker struct {
	client   CoordinatorParticipantClient
	route    RouteProvider
	budgetMs uint32
}

// CoordinatorInvoker is an expressive alias for RPCInvoker.
type CoordinatorInvoker = RPCInvoker

// WorkerRecoveryInvoker adapts the Worker Recovery Participant RPC endpoint.
// It never shares the Coordinator request context: ResumeStep receives an
// InternalCallerContext carrying the lease fence.
type WorkerRecoveryInvoker struct {
	client   WorkerRecoveryParticipantClient
	route    RouteProvider
	lease    WorkerLeaseContext
	budgetMs uint32
}

// WorkerInvoker is a concise alias for the explicit recovery adapter.
type WorkerInvoker = WorkerRecoveryInvoker

var (
	_ workflow.ParticipantInvoker = (*RPCInvoker)(nil)
	_ workflow.ParticipantInvoker = (*WorkerRecoveryInvoker)(nil)
)

// RPCInvokerOption configures the transport adapter without changing the
// durable scenario definition.
type RPCInvokerOption func(*RPCInvoker)

// WorkerRPCInvokerOption configures the recovery transport adapter.
type WorkerRPCInvokerOption func(*WorkerRecoveryInvoker)

// WithRPCBudgetMs sets the bounded Participant request budget. The Participant
// ingress still applies its own command maximum and physical admission rules.
func WithRPCBudgetMs(budgetMs uint32) RPCInvokerOption {
	return func(invoker *RPCInvoker) {
		if budgetMs != 0 {
			invoker.budgetMs = budgetMs
		}
	}
}

// WithWorkerRPCBudgetMs sets the bounded Worker Recovery request budget.
func WithWorkerRPCBudgetMs(budgetMs uint32) WorkerRPCInvokerOption {
	return func(invoker *WorkerRecoveryInvoker) {
		if budgetMs != 0 {
			invoker.budgetMs = budgetMs
		}
	}
}

// NewRPCInvoker creates a composition-neutral adapter over the existing
// CoordinatorParticipantService client. It never imports an Actor package.
func NewRPCInvoker(client CoordinatorParticipantClient, route RouteProvider, options ...RPCInvokerOption) (*RPCInvoker, error) {
	if client == nil || route == nil {
		return nil, fmt.Errorf("%w: Participant RPC client and route provider are required", ErrInvalidInput)
	}
	invoker := &RPCInvoker{client: client, route: route, budgetMs: contracts.ParticipantMaxBudgetMs}
	for _, option := range options {
		if option != nil {
			option(invoker)
		}
	}
	if invoker.budgetMs == 0 {
		return nil, fmt.Errorf("%w: Participant RPC budget", ErrInvalidInput)
	}
	return invoker, nil
}

// NewCoordinatorInvoker is the named constructor for the Coordinator RPC
// adapter; it is equivalent to NewRPCInvoker.
func NewCoordinatorInvoker(client CoordinatorParticipantClient, route RouteProvider, options ...RPCInvokerOption) (*RPCInvoker, error) {
	return NewRPCInvoker(client, route, options...)
}

// NewWorkerRecoveryInvoker creates the explicit Worker/Recovery adapter. The
// caller must supply the current Worker lease identity; this constructor does
// not infer or mint a lease.
func NewWorkerRecoveryInvoker(client WorkerRecoveryParticipantClient, route RouteProvider, lease WorkerLeaseContext, options ...WorkerRPCInvokerOption) (*WorkerRecoveryInvoker, error) {
	if client == nil || route == nil {
		return nil, fmt.Errorf("%w: Worker Recovery Participant RPC client and route provider are required", ErrInvalidInput)
	}
	if lease.CallerInstanceID == "" || lease.CallerBootID == "" || lease.LeaseOwnerToken == "" || lease.LeaseEpoch == 0 {
		return nil, fmt.Errorf("%w: Worker lease context is incomplete", ErrInvalidInput)
	}
	invoker := &WorkerRecoveryInvoker{client: client, route: route, lease: lease, budgetMs: contracts.ParticipantMaxBudgetMs}
	for _, option := range options {
		if option != nil {
			option(invoker)
		}
	}
	if invoker.budgetMs == 0 {
		return nil, fmt.Errorf("%w: Worker Recovery RPC budget", ErrInvalidInput)
	}
	return invoker, nil
}

type preparedParticipantCall struct {
	command contractspb.ParticipantCommandType
	route   *logicrpcpb.ActorRouteHeader
	work    *contractspb.WorkAddress
	step    contractspb.ParticipantStep
}

func prepareParticipantCall(ctx context.Context, call workflow.StepCall, routeProvider RouteProvider) (preparedParticipantCall, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := call.Work.ValidateShape(); err != nil {
		return preparedParticipantCall{}, fmt.Errorf("%w: WorkAddress: %v", ErrInvalidInput, err)
	}
	command, err := commandForStep(call.StepName)
	if err != nil {
		return preparedParticipantCall{}, err
	}
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		return preparedParticipantCall{}, err
	}
	payload := spec.NewPayload()
	if err := proto.Unmarshal(call.Payload, payload); err != nil {
		return preparedParticipantCall{}, fmt.Errorf("%w: typed payload: %v", ErrInvalidInput, err)
	}
	deterministic, err := deterministicProto(payload)
	if err != nil || !bytes.Equal(deterministic, call.Payload) {
		return preparedParticipantCall{}, fmt.Errorf("%w: typed payload is not deterministic protobuf", ErrInvalidInput)
	}
	if _, err := contracts.CanonicalParticipantHash(spec, payload); err != nil {
		return preparedParticipantCall{}, fmt.Errorf("%w: canonical payload: %v", ErrInvalidInput, err)
	}
	actorID, ok := participantPayloadActorID(payload)
	if !ok || actorID == 0 {
		return preparedParticipantCall{}, fmt.Errorf("%w: participant target actor", ErrInvalidInput)
	}
	route, err := routeProvider(ctx, call, command, actorID)
	if err != nil {
		return preparedParticipantCall{}, err
	}
	if route == nil || route.GetActorId() != actorID || route.GetRouteDomain() != spec.TargetRouteDomain {
		return preparedParticipantCall{}, fmt.Errorf("%w: route does not match Participant target", ErrInvalidInput)
	}
	work, err := wireWorkAddress(call.Work)
	if err != nil {
		return preparedParticipantCall{}, err
	}
	step, ok := contractspb.ParticipantStep_value["PARTICIPANT_STEP_"+call.StepName]
	if !ok {
		return preparedParticipantCall{}, fmt.Errorf("%w: Participant step %q", ErrInvalidInput, call.StepName)
	}
	return preparedParticipantCall{
		command: command,
		route:   route,
		work:    work,
		step:    contractspb.ParticipantStep(step),
	}, nil
}

func (i *RPCInvoker) Invoke(ctx context.Context, call workflow.StepCall) (workflow.StepResult, error) {
	if i == nil || i.client == nil || i.route == nil {
		return workflow.StepResult{}, fmt.Errorf("%w: Coordinator RPC invoker", ErrInvalidInput)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	prepared, err := prepareParticipantCall(ctx, call, i.route)
	if err != nil {
		return workflow.StepResult{}, err
	}
	request := &contractspb.ParticipantRequest{
		RouteHeader:        prepared.route,
		ParticipantCommand: prepared.command,
		TypedPayload:       append([]byte(nil), call.Payload...),
		CallerContext: &contractspb.ParticipantRequest_Coordinator{Coordinator: &contractspb.CoordinatorWorkContext{
			Work:                    prepared.work,
			ExpectedWorkVersion:     call.WorkVersion,
			ParticipantStep:         prepared.step,
			ParticipantInvocationId: call.InvocationID,
			OperationId:             call.OperationID,
			SubjectPlayerId:         call.SubjectPlayerID,
			RemainingBudgetMs:       i.budgetMs,
		}},
	}
	response, err := i.client.ExecuteStep(ctx, request)
	if err != nil {
		// WF-WORKVERSION-PROOF-RACE: surface a WorkVersion proof mismatch as
		// ErrStaleWorkProof so the engine reloads instead of classifying it as
		// INVOCATION_UNKNOWN or leaking a business REJECTED. Applies to the
		// Coordinator/DirectDispatcher fast path as well as the worker path.
		return workflow.StepResult{}, workflow.ClassifyStaleWorkProof(err)
	}
	return decodeParticipantResponse(response, prepared.command, call.InvocationID)
}

func (i *WorkerRecoveryInvoker) Invoke(ctx context.Context, call workflow.StepCall) (workflow.StepResult, error) {
	if i == nil || i.client == nil || i.route == nil {
		return workflow.StepResult{}, fmt.Errorf("%w: Worker Recovery RPC invoker", ErrInvalidInput)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	prepared, err := prepareParticipantCall(ctx, call, i.route)
	if err != nil {
		return workflow.StepResult{}, err
	}
	request := &contractspb.ParticipantRequest{
		RouteHeader:        prepared.route,
		ParticipantCommand: prepared.command,
		TypedPayload:       append([]byte(nil), call.Payload...),
		CallerContext: &contractspb.ParticipantRequest_Recovery{Recovery: &contractspb.InternalCallerContext{
			CallerType:              contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY,
			CallerInstanceId:        i.lease.CallerInstanceID,
			CallerBootId:            i.lease.CallerBootID,
			Work:                    prepared.work,
			ExpectedWorkVersion:     call.WorkVersion,
			ParticipantStep:         prepared.step,
			ParticipantInvocationId: call.InvocationID,
			LeaseOwnerToken:         i.lease.LeaseOwnerToken,
			LeaseEpoch:              i.lease.LeaseEpoch,
			RemainingBudgetMs:       i.budgetMs,
		}},
	}
	// Q1 WF-PARTICIPANT-INVOCATION-UNKNOWN-DIAG: worker participant client
	// begin/end witness. Aligns with player server begin/end via the same
	// workflow_id + invocation_id; distinguishes transport-before-call from
	// server-side handling vs response-loss vs error classification.
	begin := time.Now()
	log.Printf("participant_rpc_client_begin workflow=%s type=%s step=%s invocation=%s player=%d version=%d budget_ms=%d caller=%s lease_epoch=%d",
		call.Work.WorkID, call.Work.WorkType, prepared.step, call.InvocationID, call.SubjectPlayerID,
		call.WorkVersion, i.budgetMs, i.lease.CallerInstanceID, i.lease.LeaseEpoch)
	response, err := i.client.ResumeStep(ctx, request)
	durationMs := time.Since(begin).Milliseconds()
	if err != nil {
		log.Printf("participant_rpc_client_end workflow=%s step=%s invocation=%s player=%d duration_ms=%d error_class=CLIENT_RPC_ERROR grpc_code=%s",
			call.Work.WorkID, prepared.step, call.InvocationID, call.SubjectPlayerID, durationMs, status.Code(err))
		// WF-WORKVERSION-PROOF-RACE: surface a WorkVersion proof mismatch as
		// ErrStaleWorkProof so the engine reloads instead of classifying it as
		// INVOCATION_UNKNOWN or leaking a business REJECTED.
		return workflow.StepResult{}, workflow.ClassifyStaleWorkProof(err)
	}
	outcome := ""
	if response != nil {
		outcome = response.GetOutcome().String()
	}
	log.Printf("participant_rpc_client_end workflow=%s step=%s invocation=%s player=%d duration_ms=%d outcome=%s replay=%v",
		call.Work.WorkID, prepared.step, call.InvocationID, call.SubjectPlayerID, durationMs, outcome,
		response != nil && response.GetReplayedFromReceipt())
	return decodeParticipantResponse(response, prepared.command, call.InvocationID)
}

func decodeParticipantResponse(response *contractspb.ParticipantResponse, command contractspb.ParticipantCommandType, invocationID string) (workflow.StepResult, error) {
	if response == nil || response.GetParticipantCommand() != command || response.GetParticipantInvocationId() != invocationID {
		return workflow.StepResult{}, fmt.Errorf("%w: Participant response identity", ErrInvalidInput)
	}
	spec, err := contracts.LookupParticipantCommand(command)
	if err != nil {
		return workflow.StepResult{}, err
	}
	result := workflow.StepResult{Replayed: response.GetReplayedFromReceipt()}
	switch response.GetOutcome() {
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED:
		result.Outcome = workflow.StepOutcomeApplied
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED:
		result.Outcome = workflow.StepOutcomeRejected
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE:
		result.Outcome = workflow.StepOutcomeNoChange
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN:
		result.Outcome = workflow.StepOutcomeTimeoutUnknown
		return result, nil
	default:
		return workflow.StepResult{}, fmt.Errorf("%w: Participant outcome %d", ErrInvalidInput, response.GetOutcome())
	}
	if failure := response.GetFailure(); failure != nil && failure.GetError() != nil {
		result.ErrorClass = failure.GetError().GetErrorCode().String()
	}
	if len(response.GetTypedResult()) == 0 {
		finalPlayerStep := command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_SEED_RESERVATION ||
			command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_SEED_RESERVATION ||
			command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_UNLOCK_COIN_RESERVATION ||
			command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_UNLOCK_COIN_RESERVATION
		if finalPlayerStep && (result.Outcome == workflow.StepOutcomeApplied || result.Outcome == workflow.StepOutcomeNoChange) {
			return workflow.StepResult{}, fmt.Errorf("%w: terminal response is missing typed result", ErrInvalidInput)
		}
		if len(response.GetParticipantResultHash()) != 0 {
			return workflow.StepResult{}, fmt.Errorf("%w: result hash without typed result", ErrInvalidInput)
		}
		return result, nil
	}
	typedResult := spec.NewResult()
	if err := proto.Unmarshal(response.GetTypedResult(), typedResult); err != nil {
		return workflow.StepResult{}, fmt.Errorf("%w: typed result: %v", ErrInvalidInput, err)
	}
	deterministic, err := deterministicProto(typedResult)
	if err != nil || !bytes.Equal(deterministic, response.GetTypedResult()) {
		return workflow.StepResult{}, fmt.Errorf("%w: typed result is not deterministic protobuf", ErrInvalidInput)
	}
	resultHash, err := contracts.CanonicalParticipantHash(spec, typedResult)
	if err != nil || len(response.GetParticipantResultHash()) != len(resultHash) || !bytes.Equal(response.GetParticipantResultHash(), resultHash[:]) {
		return workflow.StepResult{}, fmt.Errorf("%w: typed result hash", ErrInvalidInput)
	}
	result.Result = append([]byte(nil), response.GetTypedResult()...)
	result.ResultHash = resultHash
	return result, nil
}

func participantPayloadActorID(payload proto.Message) (uint64, bool) {
	switch payload := payload.(type) {
	case *contractspb.ReserveUnlockCoinPayload:
		return payload.GetPlayerId(), true
	case *contractspb.CommitPlotUnlockPayload:
		return payload.GetFarmId(), true
	case *contractspb.CommitUnlockCoinReservationPayload:
		return payload.GetPlayerId(), true
	case *contractspb.ReleaseUnlockCoinReservationPayload:
		return payload.GetPlayerId(), true
	case *contractspb.ReserveSeedPayload:
		return payload.GetPlayerId(), true
	case *contractspb.CommitPlantPayload:
		return payload.GetFarmId(), true
	case *contractspb.CommitSeedReservationPayload:
		return payload.GetPlayerId(), true
	case *contractspb.ReleaseSeedReservationPayload:
		return payload.GetPlayerId(), true
	default:
		return 0, false
	}
}

func wireWorkAddress(address workidentity.WorkAddress) (*contractspb.WorkAddress, error) {
	workType, ok := contractspb.WorkType_value["WORK_TYPE_"+address.WorkType]
	if !ok {
		return nil, fmt.Errorf("%w: WorkType %q", ErrInvalidInput, address.WorkType)
	}
	var placement contractspb.WorkPlacementKind
	switch address.PlacementKind {
	case workidentity.IndependentWorkBucket:
		placement = contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET
	case workidentity.SourcePersistenceBucket:
		placement = contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET
	default:
		return nil, fmt.Errorf("%w: Work placement", ErrInvalidInput)
	}
	return &contractspb.WorkAddress{
		WorkId:        address.WorkID,
		WorkType:      contractspb.WorkType(workType),
		PlacementKind: placement,
		BucketVersion: address.BucketVersion,
		BucketId:      address.BucketID,
	}, nil
}
