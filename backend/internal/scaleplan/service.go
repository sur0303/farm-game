package scaleplan

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"

	scalepb "farmgame/api/gen/scaleplan"
	"farmgame/internal/actorrouting"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	DefaultRecoverableListLimit uint32 = 100
	MaxRecoverableListLimit     uint32 = 1000
	operatorPauseReason                = "OPERATOR_PAUSED"
)

// MethodAuthorizer is deliberately narrower than the transport implementation.
// The transport obtains the identity from the authenticated mTLS peer; the
// service only receives an exact full-method authorization decision.
type MethodAuthorizer interface {
	Authorize(context.Context, string) error
}

// ScalePlanExecutor is the only mutation surface the manual API consumes. It
// intentionally has no generic CAS, unlock, delete, or verification bypass.
type ScalePlanExecutor interface {
	Submit(context.Context, Submission) (PlanRecord, error)
	Pause(context.Context, string, string) (PlanRecord, error)
	Resume(context.Context, string) (PlanRecord, error)
	OperatorFailBlocked(context.Context, BlockedTerminationRequest) (PlanRecord, error)
}

// ScalePlanReadSurface is service-local so exposing the API does not require
// exporting a new mutation capability from the core package.
type ScalePlanReadSurface interface {
	Load(context.Context, string) (PlanRecord, bool, error)
	ListRecoverable(context.Context) ([]PlanRecord, error)
}

// WorkloadAllowlist is an exact, immutable service boundary. A workload is
// allowed only when the complete WorkloadRef key matches one configured entry;
// there are no wildcard realms, namespaces, or names.
type WorkloadAllowlist struct {
	Realm     string
	Workloads []WorkloadRef
}

func (a WorkloadAllowlist) Validate() error {
	if err := validateOpaque("service realm", a.Realm, MaxRealmBytes); err != nil {
		return err
	}
	if len(a.Workloads) == 0 {
		return fmt.Errorf("%w: service workload allowlist is empty", ErrInvalidPlan)
	}
	seen := make(map[string]struct{}, len(a.Workloads))
	for _, workload := range a.Workloads {
		if workload.Realm != a.Realm {
			return fmt.Errorf("%w: allowlist realm mismatch", ErrInvalidPlan)
		}
		if err := workload.Validate(); err != nil {
			return err
		}
		if _, duplicate := seen[workload.Key()]; duplicate {
			return fmt.Errorf("%w: duplicate allowlist workload", ErrInvalidPlan)
		}
		seen[workload.Key()] = struct{}{}
	}
	return nil
}

type ScalePlanServiceOptions struct {
	Executor    ScalePlanExecutor
	ReadSurface ScalePlanReadSurface
	Authorizer  MethodAuthorizer
	Ready       func() bool
	Allowlist   WorkloadAllowlist
	MaxList     uint32
}

// ScalePlanService implements the small, authenticated manual API. It is
// intentionally independent from route-controller composition; composition is
// responsible for registering it only in explicit manual mode.
type ScalePlanService struct {
	scalepb.UnimplementedScalePlanServiceServer
	executor    ScalePlanExecutor
	readSurface ScalePlanReadSurface
	authorizer  MethodAuthorizer
	ready       func() bool
	allowlist   WorkloadAllowlist
	allowed     map[string]struct{}
	maxList     uint32
}

var _ scalepb.ScalePlanServiceServer = (*ScalePlanService)(nil)

func NewScalePlanService(options ScalePlanServiceOptions) (*ScalePlanService, error) {
	if options.Executor == nil || options.ReadSurface == nil || options.Authorizer == nil || options.Ready == nil {
		return nil, fmt.Errorf("%w: service dependencies", ErrInvalidPlan)
	}
	if err := options.Allowlist.Validate(); err != nil {
		return nil, err
	}
	if options.MaxList == 0 {
		options.MaxList = DefaultRecoverableListLimit
	}
	if options.MaxList > MaxRecoverableListLimit {
		return nil, fmt.Errorf("%w: recoverable list bound", ErrInvalidPlan)
	}
	allowed := make(map[string]struct{}, len(options.Allowlist.Workloads))
	for _, workload := range options.Allowlist.Workloads {
		allowed[workload.Key()] = struct{}{}
	}
	return &ScalePlanService{executor: options.Executor, readSurface: options.ReadSurface,
		authorizer: options.Authorizer, ready: options.Ready, allowlist: options.Allowlist,
		allowed: allowed, maxList: options.MaxList}, nil
}

// NewService is kept as the concise package constructor used by in-process
// callers; the explicit name remains useful at composition call sites.
func NewService(options ScalePlanServiceOptions) (*ScalePlanService, error) {
	return NewScalePlanService(options)
}

func (s *ScalePlanService) authorize(ctx context.Context, method string) error {
	if s == nil || s.authorizer == nil || ctx == nil {
		return status.Error(codes.PermissionDenied, "scale plan method is not authorized")
	}
	if err := s.authorizer.Authorize(ctx, method); err != nil {
		return status.Error(codes.PermissionDenied, "scale plan method is not authorized")
	}
	return nil
}

func (s *ScalePlanService) requireReady() error {
	if s == nil || s.ready == nil || !s.ready() {
		return status.Error(codes.Unavailable, "scale plan recovery is not complete")
	}
	return nil
}

func (s *ScalePlanService) SubmitScalePlan(ctx context.Context, request *scalepb.SubmitScalePlanRequest) (*scalepb.SubmitScalePlanResponse, error) {
	if err := s.authorize(ctx, scalepb.ScalePlanService_SubmitScalePlan_FullMethodName); err != nil {
		return nil, err
	}
	if err := s.requireReady(); err != nil {
		return nil, err
	}
	submission, err := submissionFromProto(request)
	if err != nil {
		return nil, scalePlanRPCError(err)
	}
	normalized, err := submission.Normalize()
	if err != nil {
		return nil, scalePlanRPCError(err)
	}
	if !s.allows(normalized.Workload) || normalized.Realm != s.allowlist.Realm {
		return nil, status.Error(codes.PermissionDenied, "scale plan workload is not authorized")
	}
	record, err := s.executor.Submit(ctx, normalized)
	if err != nil {
		return nil, scalePlanRPCError(err)
	}
	response, err := planResponse(record)
	if err != nil {
		return nil, scalePlanRPCError(err)
	}
	return &scalepb.SubmitScalePlanResponse{Plan: response}, nil
}

func (s *ScalePlanService) GetScalePlan(ctx context.Context, request *scalepb.GetScalePlanRequest) (*scalepb.GetScalePlanResponse, error) {
	if err := s.authorize(ctx, scalepb.ScalePlanService_GetScalePlan_FullMethodName); err != nil {
		return nil, err
	}
	if request == nil || !validServiceID(request.GetPlanId()) {
		return nil, scalePlanRPCError(ErrInvalidPlan)
	}
	record, err := s.loadAllowed(ctx, request.GetPlanId())
	if err != nil {
		return nil, scalePlanRPCError(err)
	}
	response, err := planResponse(record)
	if err != nil {
		return nil, scalePlanRPCError(err)
	}
	return &scalepb.GetScalePlanResponse{Plan: response}, nil
}

func (s *ScalePlanService) ListRecoverableScalePlans(ctx context.Context, request *scalepb.ListRecoverableScalePlansRequest) (*scalepb.ListRecoverableScalePlansResponse, error) {
	if err := s.authorize(ctx, scalepb.ScalePlanService_ListRecoverableScalePlans_FullMethodName); err != nil {
		return nil, err
	}
	if request == nil || request.GetLimit() == 0 || request.GetLimit() > s.maxList {
		return nil, scalePlanRPCError(ErrInvalidPlan)
	}
	records, err := s.readSurface.ListRecoverable(ctx)
	if err != nil {
		return nil, scalePlanRPCError(err)
	}
	sort.SliceStable(records, func(i, j int) bool {
		if records[i].Plan.UpdatedAtMs == records[j].Plan.UpdatedAtMs {
			return records[i].Plan.PlanID < records[j].Plan.PlanID
		}
		return records[i].Plan.UpdatedAtMs < records[j].Plan.UpdatedAtMs
	})
	allowedRecords := make([]PlanRecord, 0, len(records))
	for _, record := range records {
		if err := validateReadRecord(record, ""); err != nil {
			return nil, scalePlanRPCError(err)
		}
		if s.allows(record.Plan.Submission.Workload) {
			allowedRecords = append(allowedRecords, record)
		}
	}
	limit := int(request.GetLimit())
	response := &scalepb.ListRecoverableScalePlansResponse{Truncated: len(allowedRecords) > limit}
	if response.Truncated {
		allowedRecords = allowedRecords[:limit]
	}
	response.Plans = make([]*scalepb.ScalePlan, 0, len(allowedRecords))
	for _, record := range allowedRecords {
		plan, convertErr := planProto(record.Plan)
		if convertErr != nil {
			return nil, scalePlanRPCError(convertErr)
		}
		response.Plans = append(response.Plans, plan)
	}
	return response, nil
}

func (s *ScalePlanService) Pause(ctx context.Context, request *scalepb.PauseRequest) (*scalepb.PauseResponse, error) {
	if err := s.authorize(ctx, scalepb.ScalePlanService_Pause_FullMethodName); err != nil {
		return nil, err
	}
	if err := s.requireReady(); err != nil {
		return nil, err
	}
	if request == nil || !validServiceID(request.GetPlanId()) {
		return nil, scalePlanRPCError(ErrInvalidPlan)
	}
	reason := request.GetReason()
	if reason == "" {
		reason = operatorPauseReason
	}
	if reason != operatorPauseReason {
		return nil, scalePlanRPCError(ErrInvalidTransition)
	}
	if _, err := s.loadAllowed(ctx, request.GetPlanId()); err != nil {
		return nil, scalePlanRPCError(err)
	}
	record, err := s.executor.Pause(ctx, request.GetPlanId(), reason)
	if err != nil {
		return nil, scalePlanRPCError(err)
	}
	plan, err := planResponse(record)
	if err != nil {
		return nil, scalePlanRPCError(err)
	}
	return &scalepb.PauseResponse{Plan: plan}, nil
}

func (s *ScalePlanService) Resume(ctx context.Context, request *scalepb.ResumeRequest) (*scalepb.ResumeResponse, error) {
	if err := s.authorize(ctx, scalepb.ScalePlanService_Resume_FullMethodName); err != nil {
		return nil, err
	}
	if err := s.requireReady(); err != nil {
		return nil, err
	}
	if request == nil || !validServiceID(request.GetPlanId()) {
		return nil, scalePlanRPCError(ErrInvalidPlan)
	}
	if _, err := s.loadAllowed(ctx, request.GetPlanId()); err != nil {
		return nil, scalePlanRPCError(err)
	}
	record, err := s.executor.Resume(ctx, request.GetPlanId())
	if err != nil {
		return nil, scalePlanRPCError(err)
	}
	plan, err := planResponse(record)
	if err != nil {
		return nil, scalePlanRPCError(err)
	}
	return &scalepb.ResumeResponse{Plan: plan}, nil
}

func (s *ScalePlanService) TerminateBlocked(ctx context.Context, request *scalepb.TerminateBlockedRequest) (*scalepb.TerminateBlockedResponse, error) {
	if err := s.authorize(ctx, scalepb.ScalePlanService_TerminateBlocked_FullMethodName); err != nil {
		return nil, err
	}
	if err := s.requireReady(); err != nil {
		return nil, err
	}
	if request == nil || !validServiceID(request.GetPlanId()) || request.GetExpectedRevision() == 0 ||
		!validServiceCode(request.GetExpectedBlockedReason()) || !canonicalSHA256(request.GetExpectedPayloadDigest()) ||
		!canonicalSHA256(request.GetExpectedCheckpointDigest()) || validateOpaque("termination id", request.GetTerminationId(), MaxOpaqueIDBytes) != nil {
		return nil, scalePlanRPCError(ErrInvalidPlan)
	}
	record, err := s.loadAllowed(ctx, request.GetPlanId())
	if err != nil {
		return nil, scalePlanRPCError(err)
	}
	if record.Plan.Status != StatusBlocked {
		return nil, scalePlanRPCError(ErrInvalidTransition)
	}
	if record.Plan.BlockedReason == "DEGRADED_EXCLUSION_UNRELEASED" || record.Plan.BlockedReason == "DEGRADED_BARRIER_UNRESOLVED" {
		return nil, status.Error(codes.FailedPrecondition, "blocked plan is not eligible for termination")
	}
	checkpoint, err := BlockedCheckpointDigest(record.Plan)
	if err != nil {
		return nil, scalePlanRPCError(err)
	}
	if request.GetExpectedRevision() != record.Plan.Revision || request.GetExpectedBlockedReason() != record.Plan.BlockedReason ||
		request.GetExpectedPayloadDigest() != record.Plan.PayloadDigest || request.GetExpectedCheckpointDigest() != checkpoint {
		return nil, status.Error(codes.Aborted, "blocked plan checkpoint changed")
	}
	result, err := s.executor.OperatorFailBlocked(ctx, BlockedTerminationRequest{
		PlanID: request.GetPlanId(), ExpectedRevision: request.GetExpectedRevision(),
		ExpectedBlockedReason: request.GetExpectedBlockedReason(), ExpectedPayloadDigest: request.GetExpectedPayloadDigest(),
		ExpectedCheckpointDigest: request.GetExpectedCheckpointDigest(), TerminationID: request.GetTerminationId(),
	})
	if err != nil {
		log.Printf("scaleplan blocked termination rejected plan=%s err=%v", request.GetPlanId(), err)
		return nil, scalePlanRPCError(err)
	}
	plan, err := planResponse(result)
	if err != nil {
		return nil, scalePlanRPCError(err)
	}
	return &scalepb.TerminateBlockedResponse{Plan: plan}, nil
}

func (s *ScalePlanService) allows(workload WorkloadRef) bool {
	return s != nil && workload.Realm == s.allowlist.Realm && func() bool {
		_, ok := s.allowed[workload.Key()]
		return ok
	}()
}

func (s *ScalePlanService) loadAllowed(ctx context.Context, planID string) (PlanRecord, error) {
	record, found, err := s.readSurface.Load(ctx, planID)
	if err != nil {
		return PlanRecord{}, err
	}
	if !found {
		return PlanRecord{}, ErrPlanNotFound
	}
	if err := validateReadRecord(record, planID); err != nil {
		return PlanRecord{}, err
	}
	if !s.allows(record.Plan.Submission.Workload) {
		return PlanRecord{}, fmt.Errorf("%w: plan workload is outside allowlist", ErrConflict)
	}
	return record, nil
}

func validateReadRecord(record PlanRecord, expectedID string) error {
	if expectedID != "" && record.Plan.PlanID != expectedID {
		return ErrCorruptRecord
	}
	if err := record.Plan.Validate(); err != nil {
		return fmt.Errorf("%w: read plan validation", ErrCorruptRecord)
	}
	return nil
}

func planResponse(record PlanRecord) (*scalepb.ScalePlan, error) {
	if err := validateReadRecord(record, record.Plan.PlanID); err != nil {
		return nil, err
	}
	return planProto(record.Plan)
}

func planProto(plan Plan) (*scalepb.ScalePlan, error) {
	if err := plan.Validate(); err != nil {
		return nil, fmt.Errorf("%w: response plan validation", ErrCorruptRecord)
	}
	response := &scalepb.ScalePlan{
		SchemaVersion: plan.SchemaVersion, PlanId: plan.PlanID, PayloadDigest: plan.PayloadDigest,
		Realm: plan.Submission.Realm, Domain: string(plan.Submission.Domain), Operation: operationProto(plan.Submission.Operation),
		Workload: workloadProto(plan.Submission.Workload), ExpectedReplicas: plan.Submission.ExpectedReplicas,
		DesiredReplicas: plan.Submission.DesiredReplicas, RoutePlanTtlMs: plan.Submission.RoutePlanTTLms,
		PlanTtlMs: plan.Submission.PlanTTLms, RecoveryRetentionMs: plan.Submission.RecoveryRetentionMs,
		MaxRoutePlansInflight: plan.Submission.Limits.MaxRoutePlansInflight,
		MaxShardsPerWave:      plan.Submission.Limits.MaxShardsPerWave, Status: string(plan.Status), Phase: string(plan.Phase),
		WaveIndex: plan.WaveIndex, Attempt: plan.Attempt, Revision: plan.Revision,
		BlockedReason: safePlanCode(plan.BlockedReason), PausedReason: safePlanCode(plan.PausedReason),
		ErrorCode: safePlanCode(plan.ErrorCode), CreatedAtMs: plan.CreatedAtMs, UpdatedAtMs: plan.UpdatedAtMs,
		CompletedAtMs: plan.CompletedAtMs, StartDeadlineMs: plan.StartDeadlineMs,
		RecoveryRetainUntilMs: plan.RecoveryRetainUntilMs,
	}
	response.Moves = make([]*scalepb.ShardMove, 0, len(plan.Submission.Moves))
	for _, move := range plan.Submission.Moves {
		response.Moves = append(response.Moves, &scalepb.ShardMove{Domain: string(move.Domain), VirtualShard: move.VirtualShard,
			SourceHolderId: move.SourceHolderID, TargetHolderId: move.TargetHolderID, TargetOrdinal: move.TargetOrdinal})
	}
	if plan.Status == StatusBlocked {
		checkpoint, err := BlockedCheckpointDigest(plan)
		if err != nil {
			return nil, fmt.Errorf("%w: blocked checkpoint", ErrCorruptRecord)
		}
		response.BlockedCheckpointDigest = checkpoint
	} else if plan.BlockedTermination != nil {
		response.BlockedCheckpointDigest = plan.BlockedTermination.ExpectedCheckpointDigest
	}
	return response, nil
}

func workloadProto(workload WorkloadRef) *scalepb.WorkloadRef {
	return &scalepb.WorkloadRef{Realm: workload.Realm, Domain: string(workload.Domain), Namespace: workload.Namespace, Kind: workload.Kind, Name: workload.Name}
}

func operationProto(operation Operation) scalepb.ScalePlanOperation {
	switch operation {
	case OperationScaleOut:
		return scalepb.ScalePlanOperation_SCALE_PLAN_OPERATION_SCALE_OUT
	case OperationScaleIn:
		return scalepb.ScalePlanOperation_SCALE_PLAN_OPERATION_SCALE_IN
	case OperationDrainHolder:
		return scalepb.ScalePlanOperation_SCALE_PLAN_OPERATION_DRAIN_HOLDER
	default:
		return scalepb.ScalePlanOperation_SCALE_PLAN_OPERATION_UNSPECIFIED
	}
}

func submissionFromProto(request *scalepb.SubmitScalePlanRequest) (Submission, error) {
	if request == nil || request.GetWorkload() == nil {
		return Submission{}, ErrInvalidPlan
	}
	domain := actorrouting.RouteDomain(request.GetDomain())
	operation, err := operationFromProto(request.GetOperation())
	if err != nil {
		return Submission{}, err
	}
	workload := request.GetWorkload()
	moves := make([]ShardMoveIntent, 0, len(request.GetMoves()))
	for _, move := range request.GetMoves() {
		if move == nil {
			return Submission{}, ErrInvalidPlan
		}
		moves = append(moves, ShardMoveIntent{Domain: actorrouting.RouteDomain(move.GetDomain()), VirtualShard: move.GetVirtualShard(),
			SourceHolderID: move.GetSourceHolderId(), TargetHolderID: move.GetTargetHolderId(), TargetOrdinal: move.GetTargetOrdinal()})
	}
	return Submission{IdempotencyKey: request.GetIdempotencyKey(), Realm: request.GetRealm(), Domain: domain, Operation: operation,
		Workload:         WorkloadRef{Realm: workload.GetRealm(), Domain: actorrouting.RouteDomain(workload.GetDomain()), Namespace: workload.GetNamespace(), Kind: workload.GetKind(), Name: workload.GetName()},
		ExpectedReplicas: request.GetExpectedReplicas(), DesiredReplicas: request.GetDesiredReplicas(), Moves: moves,
		RoutePlanTTLms: request.GetRoutePlanTtlMs(), PlanTTLms: request.GetPlanTtlMs(), RecoveryRetentionMs: request.GetRecoveryRetentionMs(),
		Limits: FunctionalLimits{MaxRoutePlansInflight: request.GetMaxRoutePlansInflight(), MaxShardsPerWave: request.GetMaxShardsPerWave()}}, nil
}

func operationFromProto(operation scalepb.ScalePlanOperation) (Operation, error) {
	switch operation {
	case scalepb.ScalePlanOperation_SCALE_PLAN_OPERATION_SCALE_OUT:
		return OperationScaleOut, nil
	case scalepb.ScalePlanOperation_SCALE_PLAN_OPERATION_SCALE_IN:
		return OperationScaleIn, nil
	case scalepb.ScalePlanOperation_SCALE_PLAN_OPERATION_DRAIN_HOLDER:
		// DRAIN_HOLDER is an internal child operation owned by the managed
		// rollout controller. A standalone manual drain would leave the Holder
		// excluded with no parent responsible for replacement or cleanup.
		return "", ErrInvalidPlan
	default:
		return "", ErrInvalidPlan
	}
}

func validServiceID(value string) bool {
	return validateVisibleASCII("service id", value, MaxOpaqueIDBytes) == nil
}

func validServiceCode(value string) bool {
	if value == "" || len(value) > MaxErrorCodeBytes {
		return false
	}
	for _, char := range value {
		if (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '_' && char != '-' {
			return false
		}
	}
	return true
}

func safePlanCode(value string) string {
	if validServiceCode(value) {
		return value
	}
	return ""
}

func scalePlanRPCError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	case errors.Is(err, ErrInvalidPlan), errors.Is(err, ErrInvalidTransition):
		return status.Error(codes.InvalidArgument, "invalid scale plan request")
	case errors.Is(err, ErrPlanNotFound):
		return status.Error(codes.NotFound, "scale plan not found")
	case errors.Is(err, ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, "scale plan idempotency conflict")
	case errors.Is(err, ErrWorkloadBusy), errors.Is(err, ErrRevisionConflict), errors.Is(err, ErrConflict):
		return status.Error(codes.Aborted, "scale plan conflict")
	case errors.Is(err, ErrCapacityExceeded):
		return status.Error(codes.ResourceExhausted, "scale plan capacity exhausted")
	case errors.Is(err, ErrPaused), errors.Is(err, ErrBlocked), errors.Is(err, ErrVerification), errors.Is(err, ErrOutcomeUnknown):
		return status.Error(codes.FailedPrecondition, "scale plan requires reconciliation")
	case errors.Is(err, ErrPending), errors.Is(err, ErrDependency):
		return status.Error(codes.Unavailable, "scale plan dependency is unavailable")
	case errors.Is(err, ErrCorruptRecord):
		return status.Error(codes.Internal, "scale plan record is corrupt")
	default:
		return status.Error(codes.Internal, "scale plan operation failed")
	}
}
