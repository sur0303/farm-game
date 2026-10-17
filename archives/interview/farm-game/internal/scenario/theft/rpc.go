package theft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

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

// RouteProvider supplies the current physical route fence for one typed
// Participant call. It is deliberately a port: this package does not know
// Gate, Redis, actor registries or route-key layouts.
type RouteProvider func(context.Context, workflow.StepCall, contractspb.ParticipantCommandType, uint64) (*logicrpcpb.ActorRouteHeader, error)

type CoordinatorParticipantClient interface {
	ExecuteStep(context.Context, *contractspb.ParticipantRequest, ...grpc.CallOption) (*contractspb.ParticipantResponse, error)
}

type WorkerRecoveryParticipantClient interface {
	ResumeStep(context.Context, *contractspb.ParticipantRequest, ...grpc.CallOption) (*contractspb.ParticipantResponse, error)
}

type ParticipantRPCErrorClass uint8

const (
	ParticipantRPCErrorUnspecified ParticipantRPCErrorClass = iota
	ParticipantRPCErrorIdempotencyConflict
	ParticipantRPCErrorNotSent
	ParticipantRPCErrorResponseLost
)

var ErrParticipantNotSent = errors.New("theft: Participant request was not sent")

func (c ParticipantRPCErrorClass) String() string {
	switch c {
	case ParticipantRPCErrorIdempotencyConflict:
		return ErrorClassParticipantIdempotencyConflict
	case ParticipantRPCErrorNotSent:
		return "PARTICIPANT_UNAVAILABLE_NOT_SENT"
	case ParticipantRPCErrorResponseLost:
		return "PARTICIPANT_RESPONSE_LOST"
	default:
		return "UNSPECIFIED"
	}
}

// ClassifyParticipantRPCError is conservative: an unmarked transport error
// is response-lost, so the durable step is retried with the same invocation.
func ClassifyParticipantRPCError(err error) ParticipantRPCErrorClass {
	if err == nil {
		return ParticipantRPCErrorUnspecified
	}
	message := strings.ToUpper(status.Convert(err).Message())
	if strings.Contains(message, ErrorClassParticipantIdempotencyConflict) ||
		strings.Contains(message, ErrorClassIdempotencyConflict) || strings.Contains(message, "IDEMPOTENCY_CONFLICT") {
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

type CoordinatorInvoker = RPCInvoker
type WorkerInvoker = WorkerRecoveryInvoker

var _ workflow.ParticipantInvoker = (*RPCInvoker)(nil)
var _ workflow.ParticipantInvoker = (*WorkerRecoveryInvoker)(nil)

type RPCInvokerOption func(*RPCInvoker)
type WorkerRPCInvokerOption func(*WorkerRecoveryInvoker)

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
		return nil, fmt.Errorf("%w: Coordinator Participant client/route", ErrInvalidInput)
	}
	invoker := &RPCInvoker{client: client, route: route, budgetMs: contracts.ParticipantMaxBudgetMs}
	for _, option := range options {
		if option != nil {
			option(invoker)
		}
	}
	if invoker.budgetMs == 0 {
		return nil, fmt.Errorf("%w: Participant budget", ErrInvalidInput)
	}
	return invoker, nil
}

func NewRPCInvoker(client CoordinatorParticipantClient, route RouteProvider, options ...RPCInvokerOption) (*RPCInvoker, error) {
	return NewCoordinatorInvoker(client, route, options...)
}

func NewWorkerRecoveryInvoker(client WorkerRecoveryParticipantClient, route RouteProvider, lease WorkerLeaseContext, options ...WorkerRPCInvokerOption) (*WorkerRecoveryInvoker, error) {
	if client == nil || route == nil {
		return nil, fmt.Errorf("%w: Worker Participant client/route", ErrInvalidInput)
	}
	if lease.CallerInstanceID == "" || lease.CallerBootID == "" || lease.LeaseOwnerToken == "" || lease.LeaseEpoch == 0 {
		return nil, fmt.Errorf("%w: incomplete worker lease", ErrInvalidInput)
	}
	invoker := &WorkerRecoveryInvoker{client: client, route: route, lease: lease, budgetMs: contracts.ParticipantMaxBudgetMs}
	for _, option := range options {
		if option != nil {
			option(invoker)
		}
	}
	if invoker.budgetMs == 0 {
		return nil, fmt.Errorf("%w: Worker Participant budget", ErrInvalidInput)
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
	if err := call.Work.ValidateShape(); err != nil || call.Work.WorkType != string(identity.WorkTypeTheft) {
		return preparedParticipantCall{}, fmt.Errorf("%w: Theft WorkAddress", ErrInvalidInput)
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
		return preparedParticipantCall{}, fmt.Errorf("%w: non-deterministic typed payload", ErrInvalidInput)
	}
	if _, err := contracts.CanonicalParticipantHash(spec, payload); err != nil {
		return preparedParticipantCall{}, fmt.Errorf("%w: typed payload hash: %v", ErrInvalidInput, err)
	}
	actorID, ok := participantPayloadActorID(payload)
	if !ok || actorID == 0 {
		return preparedParticipantCall{}, fmt.Errorf("%w: participant actor", ErrInvalidInput)
	}
	route, err := routeProvider(ctx, call, command, actorID)
	if err != nil {
		return preparedParticipantCall{}, err
	}
	if route == nil || route.GetActorId() != actorID || route.GetRouteDomain() != spec.TargetRouteDomain {
		return preparedParticipantCall{}, fmt.Errorf("%w: route target", ErrInvalidInput)
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
		return workflow.StepResult{}, fmt.Errorf("%w: Coordinator invoker", ErrInvalidInput)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	prepared, err := prepareParticipantCall(ctx, call, i.route)
	if err != nil {
		return workflow.StepResult{}, err
	}
	request := &contractspb.ParticipantRequest{
		RouteHeader: prepared.route, ParticipantCommand: prepared.command, TypedPayload: append([]byte(nil), call.Payload...),
		CallerContext: &contractspb.ParticipantRequest_Coordinator{Coordinator: &contractspb.CoordinatorWorkContext{
			Work: prepared.work, ExpectedWorkVersion: call.WorkVersion, ParticipantStep: prepared.step,
			ParticipantInvocationId: call.InvocationID, OperationId: call.OperationID,
			SubjectPlayerId: call.SubjectPlayerID, RemainingBudgetMs: i.budgetMs,
		}},
	}
	response, err := executeCoordinatorRPC(ctx, i.client, request)
	if err != nil {
		// WF-WORKVERSION-PROOF-RACE: stale proof is classified FIRST (before
		// the generic RPC-error classifier) so the engine reloads instead of
		// treating it as a business REJECTED.
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
		return workflow.StepResult{}, fmt.Errorf("%w: Worker invoker", ErrInvalidInput)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	prepared, err := prepareParticipantCall(ctx, call, i.route)
	if err != nil {
		return workflow.StepResult{}, err
	}
	request := &contractspb.ParticipantRequest{
		RouteHeader: prepared.route, ParticipantCommand: prepared.command, TypedPayload: append([]byte(nil), call.Payload...),
		CallerContext: &contractspb.ParticipantRequest_Recovery{Recovery: &contractspb.InternalCallerContext{
			CallerType:       contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY,
			CallerInstanceId: i.lease.CallerInstanceID, CallerBootId: i.lease.CallerBootID,
			Work: prepared.work, ExpectedWorkVersion: call.WorkVersion, ParticipantStep: prepared.step,
			ParticipantInvocationId: call.InvocationID, LeaseOwnerToken: i.lease.LeaseOwnerToken,
			LeaseEpoch: i.lease.LeaseEpoch, RemainingBudgetMs: i.budgetMs,
		}},
	}
	response, err := executeRecoveryRPC(ctx, i.client, request)
	if err != nil {
		// WF-WORKVERSION-PROOF-RACE: stale proof is classified FIRST (before
		// the generic RPC-error classifier) so the engine reloads instead of
		// treating it as a business REJECTED.
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

func executeCoordinatorRPC(ctx context.Context, client CoordinatorParticipantClient, request *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error) {
	return executeParticipantRPC(ctx, func() (*contractspb.ParticipantResponse, error) { return client.ExecuteStep(ctx, request) })
}

func executeRecoveryRPC(ctx context.Context, client WorkerRecoveryParticipantClient, request *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error) {
	return executeParticipantRPC(ctx, func() (*contractspb.ParticipantResponse, error) { return client.ResumeStep(ctx, request) })
}

func executeParticipantRPC(ctx context.Context, invoke func() (*contractspb.ParticipantResponse, error)) (*contractspb.ParticipantResponse, error) {
	for attempt := 0; attempt < 2; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrParticipantNotSent, err)
		}
		response, err := invoke()
		if err == nil {
			return response, nil
		}
		if ClassifyParticipantRPCError(err) == ParticipantRPCErrorNotSent && attempt == 0 {
			continue
		}
		return nil, err
	}
	return nil, fmt.Errorf("%w: retry budget exhausted", ErrParticipantNotSent)
}

func participantRPCErrorResult(err error) (workflow.StepResult, bool) {
	if ClassifyParticipantRPCError(err) != ParticipantRPCErrorIdempotencyConflict {
		return workflow.StepResult{}, false
	}
	return workflow.StepResult{Outcome: workflow.StepOutcomeRejected, ErrorClass: ErrorClassParticipantIdempotencyConflict}, true
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
		if result.Outcome == workflow.StepOutcomeApplied {
			return workflow.StepResult{}, fmt.Errorf("%w: applied Theft response missing typed result", ErrInvalidInput)
		}
		if len(response.GetParticipantResultHash()) != 0 {
			return workflow.StepResult{}, fmt.Errorf("%w: result hash without typed result", ErrInvalidInput)
		}
		return result, nil
	}
	if response.GetCommittedRecordVersion() == 0 {
		return workflow.StepResult{}, fmt.Errorf("%w: Participant work version missing", ErrInvalidInput)
	}
	typedResult := spec.NewResult()
	if err := proto.Unmarshal(response.GetTypedResult(), typedResult); err != nil {
		return workflow.StepResult{}, fmt.Errorf("%w: typed result: %v", ErrInvalidInput, err)
	}
	deterministic, err := deterministicProto(typedResult)
	if err != nil || !bytes.Equal(deterministic, response.GetTypedResult()) {
		return workflow.StepResult{}, fmt.Errorf("%w: non-deterministic typed result", ErrInvalidInput)
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
	case *contractspb.DecideTheftPayload:
		return payload.GetFarmId(), true
	case *contractspb.SettleTheftVisitorPayload:
		return payload.GetVisitorPlayerId(), true
	case *contractspb.CreditGuardIncomePayload:
		return payload.GetOwnerPlayerId(), true
	default:
		return 0, false
	}
}

func wireWorkAddress(address workidentity.WorkAddress) (*contractspb.WorkAddress, error) {
	workType, ok := contractspb.WorkType_value["WORK_TYPE_"+address.WorkType]
	if !ok || address.WorkType != string(identity.WorkTypeTheft) {
		return nil, fmt.Errorf("%w: WorkType %q", ErrInvalidInput, address.WorkType)
	}
	return &contractspb.WorkAddress{
		WorkId: address.WorkID, WorkType: contractspb.WorkType(workType),
		PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET,
		BucketVersion: address.BucketVersion, BucketId: address.BucketID,
	}, nil
}
