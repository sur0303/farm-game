package scaleplan

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	scalepb "farmgame/api/gen/scaleplan"
	"farmgame/internal/actorrouting"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

type serviceTestAuthorizer struct {
	err     error
	methods []string
}

func (a *serviceTestAuthorizer) Authorize(_ context.Context, method string) error {
	a.methods = append(a.methods, method)
	return a.err
}

type serviceTestReader struct {
	records map[string]PlanRecord
	list    []PlanRecord
	err     error
}

func (r *serviceTestReader) Load(_ context.Context, planID string) (PlanRecord, bool, error) {
	if r.err != nil {
		return PlanRecord{}, false, r.err
	}
	record, found := r.records[planID]
	return record, found, nil
}

func (r *serviceTestReader) ListRecoverable(_ context.Context) ([]PlanRecord, error) {
	if r.err != nil {
		return nil, r.err
	}
	return append([]PlanRecord(nil), r.list...), nil
}

type serviceTestExecutor struct {
	submitCalls int
	pauseCalls  int
	resumeCalls int
	terminate   int
	record      PlanRecord
	pauseReason string
	termination BlockedTerminationRequest
	err         error
}

func (e *serviceTestExecutor) Submit(_ context.Context, _ Submission) (PlanRecord, error) {
	e.submitCalls++
	return e.record, e.err
}

func (e *serviceTestExecutor) Pause(_ context.Context, _ string, reason string) (PlanRecord, error) {
	e.pauseCalls++
	e.pauseReason = reason
	return e.record, e.err
}

func (e *serviceTestExecutor) Resume(_ context.Context, _ string) (PlanRecord, error) {
	e.resumeCalls++
	return e.record, e.err
}

func (e *serviceTestExecutor) OperatorFailBlocked(_ context.Context, request BlockedTerminationRequest) (PlanRecord, error) {
	e.terminate++
	e.termination = request
	return e.record, e.err
}

func serviceTestSubmission() Submission {
	workload := WorkloadRef{Realm: "realm-a", Domain: actorrouting.DomainPlayer, Namespace: "farm", Kind: "StatefulSet", Name: "player-svr"}
	return Submission{IdempotencyKey: "idempotency-secret", Realm: workload.Realm, Domain: workload.Domain, Operation: OperationScaleOut,
		Workload: workload, ExpectedReplicas: 1, DesiredReplicas: 2,
		Moves:          []ShardMoveIntent{{Domain: workload.Domain, VirtualShard: 7, SourceHolderID: "holder-0", TargetHolderID: "holder-1", TargetOrdinal: 1}},
		RoutePlanTTLms: time.Minute.Milliseconds(), PlanTTLms: time.Hour.Milliseconds(), RecoveryRetentionMs: time.Hour.Milliseconds(),
		Limits: FunctionalLimits{MaxRoutePlansInflight: 1, MaxShardsPerWave: 1}}
}

func newServiceTestFixture(t *testing.T) (*ScalePlanService, *serviceTestExecutor, *serviceTestReader, *serviceTestAuthorizer, Submission, PlanRecord) {
	t.Helper()
	submission := serviceTestSubmission()
	plan, err := BuildInitialPlan(submission, time.UnixMilli(1_000_000))
	if err != nil {
		t.Fatal(err)
	}
	record := PlanRecord{Plan: plan}
	authorizer := &serviceTestAuthorizer{}
	executor := &serviceTestExecutor{record: record}
	reader := &serviceTestReader{records: map[string]PlanRecord{plan.PlanID: record}, list: []PlanRecord{record}}
	service, err := NewScalePlanService(ScalePlanServiceOptions{Executor: executor, ReadSurface: reader, Authorizer: authorizer,
		Ready: func() bool { return true }, Allowlist: WorkloadAllowlist{Realm: submission.Realm, Workloads: []WorkloadRef{submission.Workload}}, MaxList: 2})
	if err != nil {
		t.Fatal(err)
	}
	return service, executor, reader, authorizer, submission, record
}

func submissionProto(submission Submission) *scalepb.SubmitScalePlanRequest {
	moves := make([]*scalepb.ShardMove, 0, len(submission.Moves))
	for _, move := range submission.Moves {
		moves = append(moves, &scalepb.ShardMove{Domain: string(move.Domain), VirtualShard: move.VirtualShard,
			SourceHolderId: move.SourceHolderID, TargetHolderId: move.TargetHolderID, TargetOrdinal: move.TargetOrdinal})
	}
	return &scalepb.SubmitScalePlanRequest{IdempotencyKey: submission.IdempotencyKey, Realm: submission.Realm, Domain: string(submission.Domain),
		Operation: operationProto(submission.Operation), Workload: workloadProto(submission.Workload), ExpectedReplicas: submission.ExpectedReplicas,
		DesiredReplicas: submission.DesiredReplicas, Moves: moves, RoutePlanTtlMs: submission.RoutePlanTTLms, PlanTtlMs: submission.PlanTTLms,
		RecoveryRetentionMs: submission.RecoveryRetentionMs, MaxRoutePlansInflight: 1, MaxShardsPerWave: 1}
}

func TestScalePlanServiceRequiresReadyAuthorizerReadSurfaceAndExactAllowlist(t *testing.T) {
	_, executor, reader, authorizer, submission, _ := newServiceTestFixture(t)
	valid := ScalePlanServiceOptions{Executor: executor, ReadSurface: reader, Authorizer: authorizer, Ready: func() bool { return true },
		Allowlist: WorkloadAllowlist{Realm: submission.Realm, Workloads: []WorkloadRef{submission.Workload}}}
	for name, mutate := range map[string]func(*ScalePlanServiceOptions){
		"executor":     func(o *ScalePlanServiceOptions) { o.Executor = nil },
		"read surface": func(o *ScalePlanServiceOptions) { o.ReadSurface = nil },
		"authorizer":   func(o *ScalePlanServiceOptions) { o.Authorizer = nil },
		"ready":        func(o *ScalePlanServiceOptions) { o.Ready = nil },
		"allowlist":    func(o *ScalePlanServiceOptions) { o.Allowlist = WorkloadAllowlist{} },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if _, err := NewScalePlanService(candidate); !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestScalePlanServiceSubmitRequiresExplicitManifestAndExactAllowlist(t *testing.T) {
	service, executor, _, authorizer, submission, _ := newServiceTestFixture(t)
	response, err := service.SubmitScalePlan(context.Background(), submissionProto(submission))
	if err != nil || response == nil || response.GetPlan().GetPayloadDigest() == "" || executor.submitCalls != 1 {
		t.Fatalf("response=%v err=%v calls=%d", response, err, executor.submitCalls)
	}
	if got := authorizer.methods[len(authorizer.methods)-1]; got != scalepb.ScalePlanService_SubmitScalePlan_FullMethodName {
		t.Fatalf("authorized method=%q", got)
	}
	bad := submissionProto(submission)
	bad.Moves = nil
	if _, err := service.SubmitScalePlan(context.Background(), bad); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing manifest code=%s err=%v", status.Code(err), err)
	}
	outside := submissionProto(submission)
	outside.Workload.Name = "other-statefulset"
	if _, err := service.SubmitScalePlan(context.Background(), outside); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("outside allowlist code=%s err=%v", status.Code(err), err)
	}
	if executor.submitCalls != 1 {
		t.Fatalf("rejected submit reached executor calls=%d", executor.submitCalls)
	}
}

func TestScalePlanServiceReadyGatesMutationsButGetRemainsReadable(t *testing.T) {
	service, executor, _, _, _, record := newServiceTestFixture(t)
	service.ready = func() bool { return false }
	if _, err := service.Pause(context.Background(), &scalepb.PauseRequest{PlanId: record.Plan.PlanID}); status.Code(err) != codes.Unavailable {
		t.Fatalf("pause code=%s err=%v", status.Code(err), err)
	}
	if _, err := service.Resume(context.Background(), &scalepb.ResumeRequest{PlanId: record.Plan.PlanID}); status.Code(err) != codes.Unavailable {
		t.Fatalf("resume code=%s err=%v", status.Code(err), err)
	}
	if executor.pauseCalls != 0 || executor.resumeCalls != 0 {
		t.Fatal("mutation reached executor before ready")
	}
	response, err := service.GetScalePlan(context.Background(), &scalepb.GetScalePlanRequest{PlanId: record.Plan.PlanID})
	if err != nil || response.GetPlan().GetPlanId() != record.Plan.PlanID {
		t.Fatalf("read response=%v err=%v", response, err)
	}
}

func TestScalePlanServicePauseUsesOnlyOperatorReasonAndResponseOmitsIdempotency(t *testing.T) {
	service, executor, _, _, submission, record := newServiceTestFixture(t)
	if _, err := service.Pause(context.Background(), &scalepb.PauseRequest{PlanId: record.Plan.PlanID, Reason: "DEPENDENCY_UNAVAILABLE"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("dependency pause code=%s err=%v", status.Code(err), err)
	}
	response, err := service.Pause(context.Background(), &scalepb.PauseRequest{PlanId: record.Plan.PlanID})
	if err != nil || response.GetPlan().GetPlanId() != record.Plan.PlanID || executor.pauseReason != operatorPauseReason {
		t.Fatalf("response=%v err=%v reason=%q", response, err, executor.pauseReason)
	}
	encoded, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(response.GetPlan())
	if err != nil || strings.Contains(string(encoded), submission.IdempotencyKey) {
		t.Fatalf("response leaked input secret/token: %s", encoded)
	}
}

func TestScalePlanServiceListIsBoundedAndSorted(t *testing.T) {
	service, _, reader, _, submission, record := newServiceTestFixture(t)
	secondSubmission := submission
	secondSubmission.IdempotencyKey = "second-idempotency"
	secondPlan, err := BuildInitialPlan(secondSubmission, time.UnixMilli(999_000))
	if err != nil {
		t.Fatal(err)
	}
	reader.list = []PlanRecord{{Plan: record.Plan}, {Plan: secondPlan}}
	response, err := service.ListRecoverableScalePlans(context.Background(), &scalepb.ListRecoverableScalePlansRequest{Limit: 1})
	if err != nil || !response.GetTruncated() || len(response.GetPlans()) != 1 || response.GetPlans()[0].GetPlanId() != secondPlan.PlanID {
		t.Fatalf("response=%v err=%v", response, err)
	}
	if _, err := service.ListRecoverableScalePlans(context.Background(), &scalepb.ListRecoverableScalePlansRequest{Limit: 3}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("over-bound code=%s err=%v", status.Code(err), err)
	}
}

func TestScalePlanServiceTerminateBlockedRequiresExactCheckpointAndTerminationID(t *testing.T) {
	service, executor, reader, _, _, record := newServiceTestFixture(t)
	blocked := record.Plan.Clone()
	blocked.Status = StatusBlocked
	blocked.BlockedReason = "CHILD_ROUTEPLAN_PAUSED"
	blocked.Revision++
	blocked.UpdatedAtMs++
	if err := blocked.Validate(); err != nil {
		t.Fatal(err)
	}
	reader.records[blocked.PlanID] = PlanRecord{Plan: blocked}
	checkpoint, err := BlockedCheckpointDigest(blocked)
	if err != nil {
		t.Fatal(err)
	}
	request := &scalepb.TerminateBlockedRequest{PlanId: blocked.PlanID, ExpectedRevision: blocked.Revision,
		ExpectedBlockedReason: blocked.BlockedReason, ExpectedPayloadDigest: blocked.PayloadDigest,
		ExpectedCheckpointDigest: checkpoint, TerminationId: "termination-1"}
	mismatch := &scalepb.TerminateBlockedRequest{PlanId: request.GetPlanId(), ExpectedRevision: request.GetExpectedRevision(),
		ExpectedBlockedReason: request.GetExpectedBlockedReason(), ExpectedPayloadDigest: request.GetExpectedPayloadDigest(),
		ExpectedCheckpointDigest: strings.Repeat("a", 64), TerminationId: request.GetTerminationId()}
	if _, err := service.TerminateBlocked(context.Background(), mismatch); status.Code(err) != codes.Aborted || executor.terminate != 0 {
		t.Fatalf("mismatch code=%s err=%v calls=%d", status.Code(err), err, executor.terminate)
	}
	response, err := service.TerminateBlocked(context.Background(), request)
	if err != nil || response.GetPlan().GetPlanId() != blocked.PlanID || executor.terminate != 1 {
		t.Fatalf("response=%v err=%v calls=%d", response, err, executor.terminate)
	}
	if executor.termination.ExpectedCheckpointDigest != checkpoint || executor.termination.TerminationID != "termination-1" {
		t.Fatalf("termination request=%+v", executor.termination)
	}
}

func TestScalePlanServiceAuthorizerFailureIsRedacted(t *testing.T) {
	service, _, _, authorizer, _, record := newServiceTestFixture(t)
	authorizer.err = errors.New("secret endpoint:9443")
	_, err := service.GetScalePlan(context.Background(), &scalepb.GetScalePlanRequest{PlanId: record.Plan.PlanID})
	if status.Code(err) != codes.PermissionDenied || strings.Contains(err.Error(), "secret") {
		t.Fatalf("err=%v", err)
	}
}
