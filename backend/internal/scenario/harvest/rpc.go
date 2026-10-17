package harvest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// RouteProvider supplies the current physical route fence for a typed
// Participant call. Harvest owns this small adapter boundary instead of
// changing the existing Plant route implementation.
type RouteProvider func(context.Context, workflow.StepCall, contractspb.ParticipantCommandType, uint64) (*logicrpcpb.ActorRouteHeader, error)

type CoordinatorParticipantClient interface {
	ExecuteStep(context.Context, *contractspb.ParticipantRequest, ...grpc.CallOption) (*contractspb.ParticipantResponse, error)
}

type WorkerRecoveryParticipantClient interface {
	ResumeStep(context.Context, *contractspb.ParticipantRequest, ...grpc.CallOption) (*contractspb.ParticipantResponse, error)
}

// ParticipantRPCErrorClass keeps transport fate separate from a Participant
// business receipt. Only response-lost/TimeoutUnknown is allowed to surface
// as a generic Engine UNKNOWN; an explicit idempotency conflict becomes a
// durable rejected StepResult, while a proven not-sent call is retried with
// the same invocation before the caller sees an error.
type ParticipantRPCErrorClass uint8

const (
	ParticipantRPCErrorUnspecified ParticipantRPCErrorClass = iota
	ParticipantRPCErrorIdempotencyConflict
	ParticipantRPCErrorNotSent
	ParticipantRPCErrorResponseLost
)

var ErrParticipantNotSent = errors.New("harvest: Participant request was not sent")

func (c ParticipantRPCErrorClass) String() string {
	switch c {
	case ParticipantRPCErrorIdempotencyConflict:
		return ErrorClassParticipantIdempotencyConflict
	case ParticipantRPCErrorNotSent:
		return ErrorClassParticipantUnavailableNotSent
	case ParticipantRPCErrorResponseLost:
		return "PARTICIPANT_RESPONSE_LOST"
	default:
		return "UNSPECIFIED"
	}
}

// ClassifyParticipantRPCError uses only stable status/error markers. An
// unmarked transport error is conservatively response-lost because the client
// cannot prove whether the Participant mailbox accepted the request.
func ClassifyParticipantRPCError(err error) ParticipantRPCErrorClass {
	if err == nil {
		return ParticipantRPCErrorUnspecified
	}
	message := strings.ToUpper(status.Convert(err).Message())
	if strings.Contains(message, ErrorClassParticipantIdempotencyConflict) || strings.Contains(message, ErrorClassIdempotencyConflict) || strings.Contains(message, "IDEMPOTENCY_CONFLICT") {
		return ParticipantRPCErrorIdempotencyConflict
	}
	if errors.Is(err, ErrParticipantNotSent) || strings.Contains(message, "UNAVAILABLE_NOT_SENT") || strings.Contains(message, "NOT_SENT") {
		return ParticipantRPCErrorNotSent
	}
	if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded || strings.Contains(message, "TIMEOUT_UNKNOWN") {
		return ParticipantRPCErrorResponseLost
	}
	return ParticipantRPCErrorResponseLost
}

// WorkerLeaseContext is the trusted lease identity forwarded to ResumeStep.
type WorkerLeaseContext struct {
	CallerInstanceID string
	CallerBootID     string
	LeaseOwnerToken  string
	LeaseEpoch       uint64
}

type RPCInvoker struct {
	client   CoordinatorParticipantClient
	route    RouteProvider
	budgetMs uint32
}

type WorkerRecoveryInvoker struct {
	client   WorkerRecoveryParticipantClient
	route    RouteProvider
	lease    WorkerLeaseContext
	budgetMs uint32
}

// These aliases keep the Harvest adapter easy to compose beside the existing
// Plant adapter without importing or changing the Plant package.
type CoordinatorInvoker = RPCInvoker
type WorkerInvoker = WorkerRecoveryInvoker

var (
	_ workflow.ParticipantInvoker = (*RPCInvoker)(nil)
	_ workflow.ParticipantInvoker = (*WorkerRecoveryInvoker)(nil)
)

type RPCInvokerOption func(*RPCInvoker)
type WorkerRPCInvokerOption func(*WorkerRecoveryInvoker)

const participantNotSentAttempts = 2

func WithRPCBudgetMs(budgetMs uint32) RPCInvokerOption {
	return func(invoker *RPCInvoker) {
		if budgetMs != 0 {
			invoker.budgetMs = budgetMs
		}
	}
}

func WithWorkerRPCBudgetMs(budgetMs uint32) WorkerRPCInvokerOption {
	return func(invoker *WorkerRecoveryInvoker) {
		if budgetMs != 0 {
			invoker.budgetMs = budgetMs
		}
	}
}

func NewCoordinatorInvoker(client CoordinatorParticipantClient, route RouteProvider, options ...RPCInvokerOption) (*RPCInvoker, error) {
	if client == nil || route == nil {
		return nil, fmt.Errorf("%w: Coordinator Participant RPC client and route are required", ErrInvalidInput)
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

// NewRPCInvoker is the concise constructor used by composition roots that do
// not need to distinguish the Coordinator transport name.
func NewRPCInvoker(client CoordinatorParticipantClient, route RouteProvider, options ...RPCInvokerOption) (*RPCInvoker, error) {
	return NewCoordinatorInvoker(client, route, options...)
}

func NewWorkerRecoveryInvoker(client WorkerRecoveryParticipantClient, route RouteProvider, lease WorkerLeaseContext, options ...WorkerRPCInvokerOption) (*WorkerRecoveryInvoker, error) {
	if client == nil || route == nil {
		return nil, fmt.Errorf("%w: Worker Recovery Participant RPC client and route are required", ErrInvalidInput)
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
	if call.Work.WorkType != string(identity.WorkTypeHarvest) {
		return preparedParticipantCall{}, fmt.Errorf("%w: WorkAddress work type", ErrInvalidInput)
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
	if !ok || identity.ParticipantStep(call.StepName) != spec.Step {
		return preparedParticipantCall{}, fmt.Errorf("%w: Participant step %q", ErrInvalidInput, call.StepName)
	}
	return preparedParticipantCall{command: command, route: route, work: work, step: contractspb.ParticipantStep(step)}, nil
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
	response, err := executeCoordinatorRPC(ctx, i.client, request)
	if err != nil {
		// WF-WORKVERSION-PROOF-RACE: stale proof is classified FIRST (before
		// the generic RPC-error classifier) so the engine reloads instead of
		// treating it as a business REJECTED. Applies to the
		// Coordinator/DirectDispatcher fast path as well as the worker path.
		if wrapped := workflow.ClassifyStaleWorkProof(err); wrapped != err {
			return workflow.StepResult{}, wrapped
		}
		if result, handled := participantRPCErrorResult(err); handled {
			return result, nil
		}
		return workflow.StepResult{}, err
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
	// begin/end witness (same alignment key as player server begin/end).
	begin := time.Now()
	log.Printf("participant_rpc_client_begin workflow=%s type=%s step=%s invocation=%s player=%d version=%d budget_ms=%d caller=%s lease_epoch=%d",
		call.Work.WorkID, call.Work.WorkType, prepared.step, call.InvocationID, call.SubjectPlayerID,
		call.WorkVersion, i.budgetMs, i.lease.CallerInstanceID, i.lease.LeaseEpoch)
	response, err := executeRecoveryRPC(ctx, i.client, request)
	durationMs := time.Since(begin).Milliseconds()
	if err != nil {
		log.Printf("participant_rpc_client_end workflow=%s step=%s invocation=%s player=%d duration_ms=%d error_class=CLIENT_RPC_ERROR grpc_code=%s",
			call.Work.WorkID, prepared.step, call.InvocationID, call.SubjectPlayerID, durationMs, status.Code(err))
		// WF-WORKVERSION-PROOF-RACE: surface a WorkVersion proof mismatch as
		// ErrStaleWorkProof FIRST (before the generic RPC-error classifier) so
		// the engine reloads instead of treating it as a business REJECTED.
		if wrapped := workflow.ClassifyStaleWorkProof(err); wrapped != err {
			return workflow.StepResult{}, wrapped
		}
		if result, handled := participantRPCErrorResult(err); handled {
			return result, nil
		}
		return workflow.StepResult{}, err
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

func executeCoordinatorRPC(ctx context.Context, client CoordinatorParticipantClient, request *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error) {
	return executeParticipantRPC(ctx, func() (*contractspb.ParticipantResponse, error) {
		return client.ExecuteStep(ctx, request)
	})
}

func executeRecoveryRPC(ctx context.Context, client WorkerRecoveryParticipantClient, request *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error) {
	return executeParticipantRPC(ctx, func() (*contractspb.ParticipantResponse, error) {
		return client.ResumeStep(ctx, request)
	})
}

func executeParticipantRPC(ctx context.Context, invoke func() (*contractspb.ParticipantResponse, error)) (*contractspb.ParticipantResponse, error) {
	if invoke == nil {
		return nil, fmt.Errorf("%w: nil Participant RPC", ErrInvalidInput)
	}
	for attempt := 0; attempt < participantNotSentAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrParticipantNotSent, err)
		}
		response, err := invoke()
		if err == nil {
			return response, nil
		}
		switch ClassifyParticipantRPCError(err) {
		case ParticipantRPCErrorNotSent:
			if attempt+1 < participantNotSentAttempts {
				continue
			}
			return nil, fmt.Errorf("%w: %v", ErrParticipantNotSent, err)
		default:
			return nil, err
		}
	}
	return nil, fmt.Errorf("%w: Participant RPC retry budget exhausted", ErrParticipantNotSent)
}

func participantRPCErrorResult(err error) (workflow.StepResult, bool) {
	if ClassifyParticipantRPCError(err) != ParticipantRPCErrorIdempotencyConflict {
		return workflow.StepResult{}, false
	}
	return workflow.StepResult{
		Outcome:    workflow.StepOutcomeRejected,
		ErrorClass: ErrorClassParticipantIdempotencyConflict,
	}, true
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
		result.ErrorClass = participantFailureClass(failure.GetError())
	}
	if len(response.GetTypedResult()) == 0 {
		needsResult := command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST ||
			command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD
		if needsResult && (result.Outcome == workflow.StepOutcomeApplied || result.Outcome == workflow.StepOutcomeNoChange) {
			return workflow.StepResult{}, fmt.Errorf("%w: Harvest response is missing typed result", ErrInvalidInput)
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

func participantFailureClass(detail *logicrpcpb.ErrorDetail) string {
	if detail == nil {
		return ""
	}
	message := strings.ToUpper(strings.TrimSpace(detail.GetErrorMessage()))
	if strings.Contains(message, ErrorClassReconcileRequired) {
		return ErrorClassReconcileRequired
	}
	if detail.GetErrorCode() == commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT || strings.Contains(message, "IDEMPOTENCY_CONFLICT") {
		return ErrorClassParticipantIdempotencyConflict
	}
	return detail.GetErrorCode().String()
}

func participantPayloadActorID(payload proto.Message) (uint64, bool) {
	switch payload := payload.(type) {
	case *contractspb.CommitHarvestPayload:
		return payload.GetFarmId(), true
	case *contractspb.GrantHarvestRewardPayload:
		return payload.GetPlayerId(), true
	default:
		return 0, false
	}
}

func wireWorkAddress(address workidentity.WorkAddress) (*contractspb.WorkAddress, error) {
	workType, ok := contractspb.WorkType_value["WORK_TYPE_"+address.WorkType]
	if !ok || address.WorkType != string(identity.WorkTypeHarvest) {
		return nil, fmt.Errorf("%w: WorkType %q", ErrInvalidInput, address.WorkType)
	}
	return &contractspb.WorkAddress{
		WorkId:        address.WorkID,
		WorkType:      contractspb.WorkType(workType),
		PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET,
		BucketVersion: address.BucketVersion,
		BucketId:      address.BucketID,
	}, nil
}
