package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"errors"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	routeplanpb "farmgame/api/gen/routeplan"
	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	core "farmgame/internal/routecontrol"
	model "farmgame/internal/routeplan"
	"farmgame/internal/storage/authoritystore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestFarmWorkerDomainSupportsObservationAndPlannedMutation(t *testing.T) {
	if !validObservedDomain("FARM_WORKER") {
		t.Fatal("FARM_WORKER observation rejected")
	}
	if !validDomain("FARM_WORKER") {
		t.Fatal("FARM_WORKER planned mutation rejected")
	}
}

func TestWriteRedactedErrorEmitsOnlyGRPCCode(t *testing.T) {
	var output bytes.Buffer
	err := status.Error(codes.InvalidArgument, "old holder live at secret:9443")
	if writeErr := writeRedactedError(&output, err); writeErr != nil {
		t.Fatal(writeErr)
	}
	if got := strings.TrimSpace(output.String()); got != `{"grpc_code":"InvalidArgument"}` {
		t.Fatalf("redacted error=%q", got)
	}
	if strings.Contains(output.String(), "secret") {
		t.Fatal("redacted output leaked RPC details")
	}
}

type fakeRoutePlanClient struct {
	verifyRequest *routeplanpb.VerifyRequest
	verify        *routeplanpb.VerifyResponse
	shardRequest  *routeplanpb.GetShardRequest
	shard         *routeplanpb.GetShardResponse
	shardErr      error
	planRequest   *routeplanpb.GetPlanRequest
	plan          *routeplanpb.GetPlanResponse
	planErr       error
	moveRequest   *routeplanpb.MoveShardRequest
	move          *routeplanpb.MoveShardResponse
	moveErr       error
	drain         *routeplanpb.DrainHolderResponse
	drainErr      error
}

func (f *fakeRoutePlanClient) GetStatus(context.Context, *routeplanpb.GetStatusRequest, ...grpc.CallOption) (*routeplanpb.GetStatusResponse, error) {
	return nil, errors.New("unused")
}
func (f *fakeRoutePlanClient) GetSnapshot(context.Context, *routeplanpb.GetSnapshotRequest, ...grpc.CallOption) (*routeplanpb.GetSnapshotResponse, error) {
	return nil, errors.New("unused")
}
func (f *fakeRoutePlanClient) GetHolders(context.Context, *routeplanpb.GetHoldersRequest, ...grpc.CallOption) (*routeplanpb.GetHoldersResponse, error) {
	return nil, errors.New("unused")
}
func (f *fakeRoutePlanClient) GetShard(_ context.Context, request *routeplanpb.GetShardRequest, _ ...grpc.CallOption) (*routeplanpb.GetShardResponse, error) {
	f.shardRequest = request
	return f.shard, f.shardErr
}
func (f *fakeRoutePlanClient) GetPlan(_ context.Context, request *routeplanpb.GetPlanRequest, _ ...grpc.CallOption) (*routeplanpb.GetPlanResponse, error) {
	f.planRequest = request
	return f.plan, f.planErr
}
func (f *fakeRoutePlanClient) MoveShard(_ context.Context, request *routeplanpb.MoveShardRequest, _ ...grpc.CallOption) (*routeplanpb.MoveShardResponse, error) {
	f.moveRequest = request
	if f.move != nil || f.moveErr != nil {
		return f.move, f.moveErr
	}
	return nil, errors.New("unused")
}
func (f *fakeRoutePlanClient) DrainHolder(context.Context, *routeplanpb.DrainHolderRequest, ...grpc.CallOption) (*routeplanpb.DrainHolderResponse, error) {
	return f.drain, f.drainErr
}
func (f *fakeRoutePlanClient) TakeoverAfterExpiry(context.Context, *routeplanpb.TakeoverAfterExpiryRequest, ...grpc.CallOption) (*routeplanpb.TakeoverAfterExpiryResponse, error) {
	return nil, errors.New("unused")
}
func (f *fakeRoutePlanClient) Verify(_ context.Context, request *routeplanpb.VerifyRequest, _ ...grpc.CallOption) (*routeplanpb.VerifyResponse, error) {
	f.verifyRequest = request
	return f.verify, nil
}

func TestRoutectlExactPlanCommand(t *testing.T) {
	client := &fakeRoutePlanClient{plan: &routeplanpb.GetPlanResponse{Plan: &routeplanpb.Plan{PlanId: "plan-terminal", Phase: routeplanpb.PlanPhase_PLAN_PHASE_SUCCEEDED}}}
	var output strings.Builder
	cfg := globalConfig{rpcTimeout: time.Second}
	if err := runCommand(cfg, "plan", []string{"--plan-id", "plan-terminal"}, client, &output); err != nil {
		t.Fatal(err)
	}
	if client.planRequest.GetPlanId() != "plan-terminal" || !strings.Contains(output.String(), `"plan_id":"plan-terminal"`) ||
		!strings.Contains(output.String(), `"phase":"SUCCEEDED"`) {
		t.Fatalf("request=%+v output=%s", client.planRequest, output.String())
	}
	for _, id := range []string{"has space", "unicode\u00a0space", strings.Repeat("x", model.MaxPlanIDBytes+1)} {
		if err := runCommand(cfg, "plan", []string{"--plan-id", id}, client, &strings.Builder{}); !errors.Is(err, errUsage) {
			t.Fatalf("id=%q err=%v", id, err)
		}
	}
}

func TestRoutectlCanonicalPlanIDAppliesToEveryCommand(t *testing.T) {
	cfg := globalConfig{rpcTimeout: time.Second}
	bad := "bad plan"
	fence := []string{"--expected-holder-id", "holder-a", "--expected-holder-boot-id", "boot-a", "--expected-assignment-revision", "1", "--expected-writer-epoch", "1", "--expected-lease-epoch", "1"}
	commands := []struct {
		name string
		args []string
	}{
		{"move-shard", append([]string{"--plan-id", bad, "--transition-id", "transition", "--domain", "PLAYER", "--shard", "0", "--source-holder-id", "holder-a", "--source-boot-id", "boot-a", "--source-endpoint", "source:443", "--target-holder-id", "holder-b", "--target-boot-id", "boot-b", "--target-endpoint", "target:443", "--requested-ttl-ms", "1000"}, fence...)},
		{"takeover-after-expiry", append([]string{"--plan-id", bad, "--transition-id", "transition", "--domain", "PLAYER", "--shard", "0", "--old-holder-id", "holder-a", "--old-boot-id", "boot-a", "--target-holder-id", "holder-b", "--target-boot-id", "boot-b", "--target-endpoint", "target:443", "--lease-expired", "--evidence", "expired", "--requested-ttl-ms", "1000"}, fence...)},
		{"verify", []string{"--plan-id", bad, "--domain", "PLAYER", "--shard", "0"}},
		{"drain-holder", []string{"--plan-id-prefix", bad, "--transition-id-prefix", "transition", "--domain", "PLAYER", "--source-holder-id", "holder-a", "--source-boot-id", "boot-a", "--target-holder-id", "holder-b", "--target-boot-id", "boot-b", "--target-endpoint", "target:443", "--requested-ttl-ms", "1000"}},
	}
	client := &fakeRoutePlanClient{}
	for _, test := range commands {
		if err := runCommand(cfg, test.name, test.args, client, &strings.Builder{}); !errors.Is(err, errUsage) {
			t.Fatalf("command=%s err=%v", test.name, err)
		}
	}
}

func TestRoutectlVerifyPrintsRedactedJSONAndReturnsNonZeroOnMismatch(t *testing.T) {
	client := &fakeRoutePlanClient{verify: &routeplanpb.VerifyResponse{
		Consistent: false,
		Plan:       &routeplanpb.Plan{PlanId: "plan-1", TransitionId: "transition-1", Domain: "PLAYER", ErrorCode: "EXECUTION_FAILED"},
		Checks:     []*routeplanpb.VerifyCheck{{Name: "source_holder", Ok: false, ErrorCode: "SOURCE_HOLDER"}},
	}}
	var output strings.Builder
	err := runCommand(globalConfig{rpcTimeout: time.Second}, "verify", []string{"-plan-id", "plan-1", "-domain", "PLAYER", "-shard=0"}, client, &output)
	if !errors.Is(err, errVerificationMatch) {
		t.Fatalf("err=%v", err)
	}
	if !strings.Contains(output.String(), `"consistent":false`) || strings.Contains(output.String(), "secret-evidence") {
		t.Fatalf("output=%s", output.String())
	}
	if client.verifyRequest == nil || client.verifyRequest.GetPlanId() != "plan-1" || client.verifyRequest.GetDomain() != "PLAYER" || client.verifyRequest.GetShard() != 0 {
		t.Fatalf("request=%v", client.verifyRequest)
	}
}

func TestRoutectlMoveRequiresAndBindsExplicitFence(t *testing.T) {
	client := &fakeRoutePlanClient{}
	var output strings.Builder
	err := runMove(globalConfig{rpcTimeout: time.Second}, []string{
		"-plan-id", "plan-1", "-transition-id", "transition-1", "-domain", "PLAYER", "-shard", "0",
		"-source-holder-id", "holder-a", "-source-boot-id", "boot-a", "-source-endpoint", "source:443",
		"-target-holder-id", "holder-b", "-target-boot-id", "boot-b", "-target-endpoint", "target:443", "-requested-ttl-ms", "1000",
		"-expected-holder-id", "holder-a", "-expected-holder-boot-id", "boot-a", "-expected-assignment-revision", "1", "-expected-writer-epoch", "1", "-expected-lease-epoch", "1",
	}, client, &output)
	if err == nil {
		t.Fatal("nil fake RPC error unexpectedly accepted")
	}
}

func TestRoutectlShardPresenceDistinguishesOmittedFromExplicitZero(t *testing.T) {
	var shard shardFlagValue
	if shard.set {
		t.Fatal("zero-value shard flag unexpectedly present")
	}
	if err := shard.Set("0"); err != nil || !shard.set || shard.value != 0 {
		t.Fatalf("explicit shard zero rejected: value=%d set=%v err=%v", shard.value, shard.set, err)
	}
	client := &fakeRoutePlanClient{}
	var output strings.Builder
	err := runMove(globalConfig{rpcTimeout: time.Second}, []string{
		"-plan-id", "plan-1", "-transition-id", "transition-1", "-domain", "PLAYER",
		"-source-holder-id", "holder-a", "-source-boot-id", "boot-a", "-source-endpoint", "source:443",
		"-target-holder-id", "holder-b", "-target-boot-id", "boot-b", "-target-endpoint", "target:443", "-requested-ttl-ms", "1000",
		"-expected-holder-id", "holder-a", "-expected-holder-boot-id", "boot-a", "-expected-assignment-revision", "1", "-expected-writer-epoch", "1", "-expected-lease-epoch", "1",
	}, client, &output)
	if !errors.Is(err, errUsage) {
		t.Fatalf("move without shard err=%v", err)
	}
}

func TestRoutectlShardBindsExactResponseIdentity(t *testing.T) {
	valid := &routeplanpb.GetShardResponse{
		Domain: "PLAYER", Shard: 0, SnapshotState: "ACTIVE", SnapshotRevision: 7,
		Authority:    &routeplanpb.AuthorityView{Domain: "PLAYER", Shard: 0},
		RelatedPlans: []*routeplanpb.Plan{{PlanId: "plan-1", Domain: "PLAYER", Shard: 0}},
	}
	client := &fakeRoutePlanClient{shard: valid}
	var output strings.Builder
	if err := runCommand(globalConfig{rpcTimeout: time.Second}, "shard", []string{"-domain", "PLAYER", "-shard=0"}, client, &output); err != nil {
		t.Fatalf("explicit shard zero err=%v", err)
	}
	if client.shardRequest == nil || client.shardRequest.GetDomain() != "PLAYER" || client.shardRequest.GetShard() != 0 {
		t.Fatalf("request=%v", client.shardRequest)
	}
	for name, response := range map[string]*routeplanpb.GetShardResponse{
		"nil":             nil,
		"wrong domain":    {Domain: "FARM", Shard: 0},
		"wrong shard":     {Domain: "PLAYER", Shard: 1},
		"wrong authority": {Domain: "PLAYER", Shard: 0, Authority: &routeplanpb.AuthorityView{Domain: "FARM", Shard: 0}},
		"wrong plan":      {Domain: "PLAYER", Shard: 0, RelatedPlans: []*routeplanpb.Plan{{PlanId: "plan-1", Domain: "PLAYER", Shard: 1}}},
		"nil plan":        {Domain: "PLAYER", Shard: 0, RelatedPlans: []*routeplanpb.Plan{nil}},
	} {
		t.Run(name, func(t *testing.T) {
			client := &fakeRoutePlanClient{shard: response}
			var output strings.Builder
			if err := runCommand(globalConfig{rpcTimeout: time.Second}, "shard", []string{"-domain", "PLAYER", "-shard=0"}, client, &output); !errors.Is(err, errVerificationMatch) {
				t.Fatalf("err=%v output=%q", err, output.String())
			}
		})
	}
}

func validVerifyPlanFixture() *routeplanpb.Plan {
	return &routeplanpb.Plan{
		PlanId: "plan-1", TransitionId: "transition-1", Domain: "PLAYER", Shard: 0,
		SourceHolderId: "holder-a", SourceBootId: "boot-a", SourceEndpoint: "holder-a.internal:443",
		TargetHolderId: "holder-b", TargetBootId: "boot-b", TargetEndpoint: "holder-b.internal:443",
		ExpectedSourceFence: &routeplanpb.AuthorityFence{HolderId: "holder-a", HolderBootId: "boot-a", AssignmentRevision: 1, WriterEpoch: 2, LeaseEpoch: 3},
		Operation:           routeplanpb.OperationKind_OPERATION_KIND_PLANNED_DRAIN, Phase: routeplanpb.PlanPhase_PLAN_PHASE_SUCCEEDED,
		Revision: 4, CreatedAtMs: 1000, UpdatedAtMs: 2000, StartedAtMs: 1500, CompletedAtMs: 2000, ExpiresAtMs: 3000,
		ChecksumSha256: strings.Repeat("a", 64), RequestedTtlMs: 1000,
	}
}

var verifySuccessCodes = map[string]string{
	"authority_active_lease": "OK",
	"authority_tuple":        "AUTHORITY_TUPLE",
	"snapshot_read":          "OK",
	"snapshot_tuple":         "SNAPSHOT_TUPLE",
	"target_holder":          "OK",
	"gate_resolution":        "GATE_RESOLUTION",
	"transition":             "TRANSITION",
	"recovery":               "RECOVERY",
	"tombstone":              "TOMBSTONE",
	"plan_state":             "PLAN_STATE",
	"final_freshness":        "FINAL_FRESHNESS",
}

func TestRoutectlVerifyAcceptsFactuallyReconciledTerminalAuditTimes(t *testing.T) {
	for name, mutate := range map[string]func(*routeplanpb.Plan){
		"before attempt bump":  func(plan *routeplanpb.Plan) { plan.StartedAtMs = 0 },
		"after logical expiry": func(plan *routeplanpb.Plan) { plan.ExpiresAtMs = plan.UpdatedAtMs - 1 },
	} {
		t.Run(name, func(t *testing.T) {
			plan := validVerifyPlanFixture()
			mutate(plan)
			if !validVerifyPlan("plan-1", "PLAYER", 0, plan) {
				t.Fatalf("valid reconciled plan rejected: %+v", plan)
			}
		})
	}
}

func TestRoutectlVerifyValidatesExactCompleteResponse(t *testing.T) {
	response := &routeplanpb.VerifyResponse{Consistent: true, Plan: validVerifyPlanFixture()}
	for _, name := range []string{"authority_active_lease", "authority_tuple", "snapshot_read", "snapshot_tuple", "target_holder", "gate_resolution", "transition", "recovery", "tombstone", "plan_state", "final_freshness"} {
		response.Checks = append(response.Checks, &routeplanpb.VerifyCheck{Name: name, Ok: true, ErrorCode: verifySuccessCodes[name]})
	}
	if !validVerifyResponse("plan-1", "PLAYER", 0, response) {
		t.Fatal("exact complete verify response rejected")
	}
	for name, mutate := range map[string]func(*routeplanpb.VerifyResponse){
		"wrong plan": func(value *routeplanpb.VerifyResponse) { value.Plan.PlanId = "other" },
		"missing":    func(value *routeplanpb.VerifyResponse) { value.Checks = value.Checks[:len(value.Checks)-1] },
		"duplicate":  func(value *routeplanpb.VerifyResponse) { value.Checks = append(value.Checks, value.Checks[0]) },
		"failed":     func(value *routeplanpb.VerifyResponse) { value.Checks[0].Ok = false; value.Consistent = false },
		"wrong code": func(value *routeplanpb.VerifyResponse) { value.Checks[1].ErrorCode = "OK" },
		"lying bool": func(value *routeplanpb.VerifyResponse) { value.Consistent = false },
		"bad phase": func(value *routeplanpb.VerifyResponse) {
			value.Plan.Phase = routeplanpb.PlanPhase_PLAN_PHASE_UNSPECIFIED
		},
		"bad revision": func(value *routeplanpb.VerifyResponse) {
			value.Plan.Revision = 0
		},
		"bad checksum": func(value *routeplanpb.VerifyResponse) { value.Plan.ChecksumSha256 = strings.Repeat("A", 64) },
		"bad times":    func(value *routeplanpb.VerifyResponse) { value.Plan.UpdatedAtMs = value.Plan.CreatedAtMs - 1 },
		"not succeeded": func(value *routeplanpb.VerifyResponse) {
			value.Plan.Phase = routeplanpb.PlanPhase_PLAN_PHASE_ACTIVE
			value.Plan.CompletedAtMs = 0
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := proto.Clone(response).(*routeplanpb.VerifyResponse)
			mutate(candidate)
			if validVerifyResponse("plan-1", "PLAYER", 0, candidate) {
				t.Fatal("invalid verify response accepted")
			}
		})
	}
}

type routectlVerifyManager struct{ snapshot actorrouting.RouteSnapshot }

func (m *routectlVerifyManager) CurrentSnapshot() actorrouting.RouteSnapshot { return m.snapshot }
func (*routectlVerifyManager) DrainValidated(context.Context, authority.TransferRequest, authority.DrainController, authority.PendingProjectionTargetValidator) (authoritystore.ShardAuthorityTransitionResult, error) {
	return authoritystore.ShardAuthorityTransitionResult{}, errors.New("unused")
}
func (*routectlVerifyManager) HandoffWorkerValidated(context.Context, authority.TransferRequest, authority.WorkerHandoffSourceValidator, authority.PendingProjectionTargetValidator) (authoritystore.ShardAuthorityTransitionResult, error) {
	return authoritystore.ShardAuthorityTransitionResult{}, errors.New("unused")
}
func (*routectlVerifyManager) TransferValidated(context.Context, authority.TransferRequest, authority.PendingProjectionTargetValidator) (authoritystore.ShardAuthorityTransitionResult, error) {
	return authoritystore.ShardAuthorityTransitionResult{}, errors.New("unused")
}

type routectlVerifyAuthority struct {
	value authoritystore.ShardWriteAuthority
}

func (r routectlVerifyAuthority) ReadShardWriteAuthority(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error) {
	return r.value, true, nil
}

type routectlVerifyHolders struct{ target core.HolderRecord }

func (r routectlVerifyHolders) Get(_ context.Context, _ actorrouting.RouteDomain, holderID string) (core.HolderRecord, bool, error) {
	if holderID != r.target.HolderID {
		return core.HolderRecord{}, false, nil
	}
	return r.target, true, nil
}
func (r routectlVerifyHolders) List(context.Context, actorrouting.RouteDomain) ([]core.HolderRecord, error) {
	return []core.HolderRecord{r.target}, nil
}

type routectlVerifyDependencies struct{}

func (routectlVerifyDependencies) VerifyGateResolution(context.Context, model.Plan, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot) error {
	return nil
}
func (routectlVerifyDependencies) VerifyTransition(context.Context, model.Plan, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot) error {
	return nil
}
func (routectlVerifyDependencies) VerifyRecovery(context.Context, model.Plan, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot) error {
	return nil
}
func (routectlVerifyDependencies) VerifyTombstone(context.Context, model.Plan, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot) error {
	return nil
}

type routectlAllowAuthorizer struct{}

func (routectlAllowAuthorizer) Authorize(context.Context, string) error { return nil }

func TestRoutectlAcceptsProductionExecutorVerifyServiceResponse(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	leaseUntil := now.Add(10 * time.Minute).UnixMilli()
	snapshot := actorrouting.RouteSnapshot{
		Realm: "realm", GlobalRevision: 1, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608030001,
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 2,
			ExpectedWriterEpoch: 2, ExpectedLeaseEpoch: 2, LeaseUntilMs: leaseUntil,
		}}},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: 1,
			ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: leaseUntil,
		}}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "holder-b", BootID: "boot-b", Endpoint: "holder-b.internal:443", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-holder", BootID: "farm-boot", Endpoint: "farm.internal:443", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
	snapshotStore := core.NewMemoryRouteSnapshotStore()
	if _, err := snapshotStore.Bootstrap(context.Background(), core.NestedControlOperationToken{}, snapshot); err != nil {
		t.Fatal(err)
	}
	authorityValue := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "v1", HashVersion: snapshot.HashVersion, HashSeed: snapshot.HashSeed,
		Domain: actorrouting.DomainPlayer, VirtualShard: 0, HolderID: "holder-b", HolderBootID: "boot-b",
		AssignmentRevision: 2, WriterEpoch: 2, LeaseEpoch: 2, State: authoritystore.ShardAuthorityActive,
		LeaseUntilMs: leaseUntil, LastTransitionID: "transition-1", LastTransitionAtMs: now.Add(-time.Second).UnixMilli(),
	}
	authorityReader := routectlVerifyAuthority{value: authorityValue}
	holders := routectlVerifyHolders{target: core.HolderRecord{
		Domain: actorrouting.DomainPlayer, HolderID: "holder-b", BootID: "boot-b",
		LogicEndpoint: "holder-b.internal:443", HolderControlEndpoint: "holder-b.internal:443", TLSDNSIdentity: "holder-b.internal",
		State: core.HolderStateServing, ProcessReady: true, LeaseUntil: now.Add(10 * time.Minute),
		SnapshotRevision: 1, CapacityWeight: 1,
	}}
	store, err := model.NewMemoryPlanStore(model.MemoryPlanStoreOptions{Now: func() time.Time { return now }, PlanTTL: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	created := now.Add(-time.Second).UnixMilli()
	plan := model.Plan{
		PlanID: "plan-1", TransitionID: "transition-1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		SourceHolderID: "holder-a", SourceBootID: "boot-a", SourceEndpoint: "holder-a.internal:443",
		TargetHolderID: "holder-b", TargetBootID: "boot-b", TargetEndpoint: "holder-b.internal:443",
		ExpectedSourceFence: authoritystore.ShardWriteFence{ExpectedHolderID: "holder-a", ExpectedHolderBootID: "boot-a", ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1},
		RequestedTTLms:      1000, Operation: model.OperationPlannedDrain, Phase: model.PhasePending,
		Revision: 1, CreatedAtMs: created, UpdatedAtMs: created, ExpiresAtMs: now.Add(time.Hour).UnixMilli(),
	}
	if _, err := store.CompareAndSwap(context.Background(), 0, plan); err != nil {
		t.Fatal(err)
	}
	plan.Phase, plan.Attempt, plan.Revision, plan.StartedAtMs, plan.UpdatedAtMs = model.PhaseAssigning, 1, 2, created+100, created+100
	if _, err := store.CompareAndSwap(context.Background(), 1, plan); err != nil {
		t.Fatal(err)
	}
	plan.Phase, plan.Revision, plan.UpdatedAtMs = model.PhaseActive, 3, created+200
	if _, err := store.CompareAndSwap(context.Background(), 2, plan); err != nil {
		t.Fatal(err)
	}
	plan.Phase, plan.Revision, plan.UpdatedAtMs, plan.CompletedAtMs = model.PhaseSucceeded, 4, created+300, created+300
	if _, err := store.CompareAndSwap(context.Background(), 3, plan); err != nil {
		t.Fatal(err)
	}
	executor, err := model.NewPlanExecutor(model.PlanExecutorOptions{
		Store: store, Manager: &routectlVerifyManager{snapshot: snapshot}, AuthorityReader: authorityReader,
		SnapshotStore: snapshotStore, HolderReader: holders, Now: func() time.Time { return now },
		OperationTimeout: time.Second, PlanTTL: 24 * time.Hour, Verification: routectlVerifyDependencies{},
		SingleController: model.SingleControllerAttestation{Contract: model.SingleControllerContract, ControllerID: "controller", BootID: "boot"},
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := model.NewPlanService(model.PlanServiceOptions{
		Executor: executor, SnapshotStore: snapshotStore, HolderReader: holders, AuthorityReader: authorityReader,
		Authorizer: routectlAllowAuthorizer{}, ControllerID: "controller", Boot: "boot", Realm: "realm", Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.Verify(context.Background(), &routeplanpb.VerifyRequest{PlanId: plan.PlanID, Domain: string(plan.Domain), Shard: plan.VirtualShard})
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeRoutePlanClient{verify: response}
	var output strings.Builder
	if err := runCommand(globalConfig{rpcTimeout: time.Second}, "verify", []string{"-plan-id", plan.PlanID, "-domain", string(plan.Domain), "-shard=0"}, client, &output); err != nil {
		t.Fatalf("production Verify response rejected: %v response=%+v", err, response)
	}
	if !strings.Contains(output.String(), `"consistent":true`) {
		t.Fatalf("output=%s", output.String())
	}
}

func TestRoutectlCanonicalDNSValidation(t *testing.T) {
	for _, value := range []string{"controller.internal", "route-controller-1.example.test"} {
		if !validCanonicalDNS(value) {
			t.Fatalf("valid DNS rejected: %q", value)
		}
	}
	for _, value := range []string{"Controller.internal", "127.0.0.1", "route_controller.internal", "a..b"} {
		if validCanonicalDNS(value) {
			t.Fatalf("invalid DNS accepted: %q", value)
		}
	}
}

func TestRoutectlBoundsTimeoutTargetTTLAndShardBeforeConversion(t *testing.T) {
	base := globalConfig{
		target: "controller.internal:443", serverName: "controller.internal", expectedServerIdentity: "controller.internal",
		certFile: "/cert", keyFile: "/key", caFile: "/ca", dialTimeout: time.Millisecond,
		rpcTimeout: 30 * time.Second, maxMessageBytes: 1024,
	}
	if err := base.validate(); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []func(*globalConfig){
		func(c *globalConfig) { c.dialTimeout = time.Millisecond - 1 },
		func(c *globalConfig) { c.rpcTimeout = 30*time.Second + time.Nanosecond },
		func(c *globalConfig) { c.target = "Controller.internal:443" },
		func(c *globalConfig) { c.target = "*.internal:443" },
		func(c *globalConfig) { c.target = "controller.internal:0443" },
		func(c *globalConfig) { c.target = "controller.internal" },
	} {
		candidate := base
		invalid(&candidate)
		if err := candidate.validate(); !errors.Is(err, errUsage) {
			t.Fatalf("candidate=%+v err=%v", candidate, err)
		}
	}
	for _, ttl := range []int64{0, -1, maximumTTLms + 1, int64(^uint64(0) >> 1)} {
		if validRequestedTTL(ttl) {
			t.Fatalf("ttl=%d accepted", ttl)
		}
	}
	for _, value := range []string{"4294967296", "-1", "01", "+1"} {
		var shard shardFlagValue
		if err := shard.Set(value); !errors.Is(err, errUsage) {
			t.Fatalf("shard=%q err=%v", value, err)
		}
	}
	var maximum shardFlagValue
	if err := maximum.Set("4294967295"); err != nil || maximum.value != ^uint32(0) || !maximum.set {
		t.Fatalf("maximum shard=%d err=%v", maximum.value, err)
	}
}

func TestRoutectlTLSIdentityRequiresOneExactRawDNSSAN(t *testing.T) {
	valid := tlsStateForCertificate(t, certificateWithSANs(t, []string{"controller.internal"}, nil, nil, nil, ""))
	if err := verifyRouteControllerConnection(valid, "controller.internal"); err != nil {
		t.Fatalf("valid identity err=%v", err)
	}
	callbackPhase := valid
	callbackPhase.HandshakeComplete = false
	if err := verifyRouteControllerConnection(callbackPhase, "controller.internal"); err != nil {
		t.Fatalf("callback-phase HandshakeComplete=false rejected: %v", err)
	}
	uri, err := url.Parse("spiffe://farm.test/controller")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		cert     *x509.Certificate
		expected string
	}{
		{name: "wildcard", cert: certificateWithSANs(t, []string{"*.internal"}, nil, nil, nil, ""), expected: "*.internal"},
		{name: "multiple DNS", cert: certificateWithSANs(t, []string{"controller.internal", "other.internal"}, nil, nil, nil, ""), expected: "controller.internal"},
		{name: "URI", cert: certificateWithSANs(t, nil, nil, nil, []*url.URL{uri}, ""), expected: "controller.internal"},
		{name: "IP", cert: certificateWithSANs(t, nil, []net.IP{net.ParseIP("127.0.0.1")}, nil, nil, ""), expected: "controller.internal"},
		{name: "email", cert: certificateWithSANs(t, nil, nil, []string{"ops@example.test"}, nil, ""), expected: "controller.internal"},
		{name: "common name", cert: certificateWithSANs(t, []string{"controller.internal"}, nil, nil, nil, "controller.internal"), expected: "controller.internal"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := verifyRouteControllerConnection(tlsStateForCertificate(t, test.cert), test.expected); !errors.Is(err, errUsage) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	t.Run("unverified chain", func(t *testing.T) {
		state := tlsStateForCertificate(t, certificateWithSANs(t, []string{"controller.internal"}, nil, nil, nil, ""))
		state.VerifiedChains = nil
		if err := verifyRouteControllerConnection(state, "controller.internal"); !errors.Is(err, errUsage) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("tls12 version", func(t *testing.T) {
		state := tlsStateForCertificate(t, certificateWithSANs(t, []string{"controller.internal"}, nil, nil, nil, ""))
		state.Version = tls.VersionTLS12
		if err := verifyRouteControllerConnection(state, "controller.internal"); !errors.Is(err, errUsage) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("peer leaf mismatch", func(t *testing.T) {
		leaf := certificateWithSANs(t, []string{"controller.internal"}, nil, nil, nil, "")
		other := certificateWithSANs(t, []string{"other.internal"}, nil, nil, nil, "")
		state := tlsStateForCertificate(t, leaf)
		state.PeerCertificates = []*x509.Certificate{other}
		if err := verifyRouteControllerConnection(state, "controller.internal"); !errors.Is(err, errUsage) {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestRoutectlDrainRejectsEmptySuccess(t *testing.T) {
	client := &fakeRoutePlanClient{drain: &routeplanpb.DrainHolderResponse{}}
	var output strings.Builder
	err := runDrain(globalConfig{rpcTimeout: time.Second}, []string{
		"-plan-id-prefix", "plan", "-transition-id-prefix", "transition", "-domain", "PLAYER",
		"-source-holder-id", "holder-a", "-source-boot-id", "boot-a",
		"-target-holder-id", "holder-b", "-target-boot-id", "boot-b", "-target-endpoint", "target:443",
		"-requested-ttl-ms", "1000",
	}, client, &output)
	if !errors.Is(err, errUsage) || output.Len() != 0 {
		t.Fatalf("err=%v output=%q", err, output.String())
	}
}

func certificateWithSANs(t *testing.T, dns []string, ips []net.IP, emails []string, uris []*url.URL, commonName string) *x509.Certificate {
	t.Helper()
	names := make([]asn1.RawValue, 0, len(dns)+len(ips)+len(emails)+len(uris))
	for _, value := range dns {
		names = append(names, asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 2, Bytes: []byte(value)})
	}
	for _, value := range emails {
		names = append(names, asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 1, Bytes: []byte(value)})
	}
	for _, value := range uris {
		names = append(names, asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 6, Bytes: []byte(value.String())})
	}
	for _, value := range ips {
		names = append(names, asn1.RawValue{Class: asn1.ClassContextSpecific, Tag: 7, Bytes: []byte(value.To4())})
	}
	raw, err := asn1.Marshal(names)
	if err != nil {
		t.Fatal(err)
	}
	return &x509.Certificate{
		DNSNames: dns, IPAddresses: ips, EmailAddresses: emails, URIs: uris,
		Subject:    pkix.Name{CommonName: commonName},
		Extensions: []pkix.Extension{{Id: subjectAltNameOID, Value: raw}},
	}
}

func tlsStateForCertificate(t *testing.T, certificate *x509.Certificate) tls.ConnectionState {
	t.Helper()
	return tls.ConnectionState{
		HandshakeComplete: true, Version: tls.VersionTLS13,
		VerifiedChains: [][]*x509.Certificate{{certificate}},
	}
}

func TestRoutectlMoveBindsExpectedFenceIntoRequest(t *testing.T) {
	challenge := "021debcf7e9ddc2ec06909091b3161f8"
	client := &fakeRoutePlanClient{move: &routeplanpb.MoveShardResponse{Plan: &routeplanpb.Plan{
		PlanId: "rt2q-" + challenge + "-s2-move", TransitionId: "rt2q-" + challenge + "-s2-transition",
		Domain: "PLAYER", Shard: 0, SourceHolderId: "player-a-" + challenge, SourceBootId: "player-a-boot-" + challenge,
		SourceEndpoint: "127.0.0.1:40001", TargetHolderId: "player-b-" + challenge, TargetBootId: "player-b-boot-" + challenge,
		TargetEndpoint: "127.0.0.1:40002", Operation: routeplanpb.OperationKind_OPERATION_KIND_PLANNED_DRAIN,
		Phase: routeplanpb.PlanPhase_PLAN_PHASE_SUCCEEDED, Revision: 2, RequestedTtlMs: 60000,
		ChecksumSha256: strings.Repeat("ab", 32),
		ExpectedSourceFence: &routeplanpb.AuthorityFence{
			HolderId: "player-a-" + challenge, HolderBootId: "player-a-boot-" + challenge,
			AssignmentRevision: 7, WriterEpoch: 8, LeaseEpoch: 9,
		},
	}}}
	var output strings.Builder
	err := runMove(globalConfig{rpcTimeout: time.Second}, []string{
		"-plan-id", "rt2q-" + challenge + "-s2-move",
		"-transition-id", "rt2q-" + challenge + "-s2-transition",
		"-domain", "PLAYER", "-shard", "0",
		"-source-holder-id", "player-a-" + challenge, "-source-boot-id", "player-a-boot-" + challenge, "-source-endpoint", "127.0.0.1:40001",
		"-target-holder-id", "player-b-" + challenge, "-target-boot-id", "player-b-boot-" + challenge, "-target-endpoint", "127.0.0.1:40002",
		"-requested-ttl-ms", "60000",
		"-expected-holder-id", "player-a-" + challenge, "-expected-holder-boot-id", "player-a-boot-" + challenge,
		"-expected-assignment-revision", "7", "-expected-writer-epoch", "8", "-expected-lease-epoch", "9",
	}, client, &output)
	if err != nil {
		t.Fatalf("harness-shaped move args rejected: %v output=%q", err, output.String())
	}
	if client.moveRequest == nil || client.moveRequest.GetExpectedSourceFence() == nil {
		t.Fatal("MoveShard fence was not bound")
	}
	fence := client.moveRequest.GetExpectedSourceFence()
	if fence.GetHolderId() != "player-a-"+challenge || fence.GetHolderBootId() != "player-a-boot-"+challenge ||
		fence.GetAssignmentRevision() != 7 || fence.GetWriterEpoch() != 8 || fence.GetLeaseEpoch() != 9 {
		t.Fatalf("fence=%v", fence)
	}
}

func TestRegisterFenceFlagsMutatesReturnedStorage(t *testing.T) {
	fs := commandFlagSet("fence")
	fence := registerFenceFlags(fs)
	if err := fs.Parse([]string{
		"-expected-holder-id", "holder-a", "-expected-holder-boot-id", "boot-a",
		"-expected-assignment-revision", "3", "-expected-writer-epoch", "4", "-expected-lease-epoch", "5",
	}); err != nil {
		t.Fatal(err)
	}
	if !validFence(fence) || fence.holderID != "holder-a" || fence.assignmentRevision != 3 || fence.writerEpoch != 4 || fence.leaseEpoch != 5 {
		t.Fatalf("fence flags did not mutate returned storage: %#v", fence)
	}
}
