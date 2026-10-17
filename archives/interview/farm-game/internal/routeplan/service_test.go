package routeplan

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	routeplanpb "farmgame/api/gen/routeplan"
	"farmgame/internal/actorrouting"
	"farmgame/internal/holderidentity"
	core "farmgame/internal/routecontrol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type serviceAuthorizerFake struct {
	err     error
	methods []string
}

func (a *serviceAuthorizerFake) Authorize(_ context.Context, method string) error {
	a.methods = append(a.methods, method)
	return a.err
}

func TestHolderProtoCarriesExactDeploymentIdentityAndMarksLegacyZero(t *testing.T) {
	exact := core.HolderRecord{HolderControlEndpoint: "holder.internal:9443", DeploymentIdentity: holderidentity.DeploymentIdentity{
		PodUID: "pod-uid", WorkloadUID: "workload-uid", Ordinal: 3,
	}}
	view := holderProto(exact)
	if !view.GetDeploymentIdentityPresent() || view.GetPodUid() != "pod-uid" ||
		view.GetWorkloadUid() != "workload-uid" || view.GetOrdinal() != 3 ||
		view.GetHolderControlEndpoint() != "holder.internal:9443" {
		t.Fatalf("view=%+v", view)
	}
	legacy := holderProto(core.HolderRecord{})
	if legacy.GetDeploymentIdentityPresent() || legacy.GetPodUid() != "" || legacy.GetWorkloadUid() != "" || legacy.GetOrdinal() != 0 {
		t.Fatalf("legacy=%+v", legacy)
	}
}

func TestPlanServiceAuthorizesEveryMethodExactly(t *testing.T) {
	fixture := newExecutorFixture(t)
	authorizer := &serviceAuthorizerFake{err: errors.New("denied")}
	service, err := NewPlanService(PlanServiceOptions{Executor: fixture.executor, SnapshotStore: fixture.snapshot, HolderReader: fixture.holders, AuthorityReader: fixture.authority, Authorizer: authorizer, ControllerID: "controller", Boot: "boot", Realm: "realm"})
	if err != nil {
		t.Fatal(err)
	}
	calls := []struct {
		method string
		call   func() error
	}{
		{routeplanpb.RoutePlanService_GetStatus_FullMethodName, func() error {
			_, err := service.GetStatus(context.Background(), &routeplanpb.GetStatusRequest{})
			return err
		}},
		{routeplanpb.RoutePlanService_GetSnapshot_FullMethodName, func() error {
			_, err := service.GetSnapshot(context.Background(), &routeplanpb.GetSnapshotRequest{})
			return err
		}},
		{routeplanpb.RoutePlanService_GetHolders_FullMethodName, func() error {
			_, err := service.GetHolders(context.Background(), &routeplanpb.GetHoldersRequest{Domain: "PLAYER"})
			return err
		}},
		{routeplanpb.RoutePlanService_GetShard_FullMethodName, func() error {
			_, err := service.GetShard(context.Background(), &routeplanpb.GetShardRequest{Domain: "PLAYER"})
			return err
		}},
		{routeplanpb.RoutePlanService_GetPlan_FullMethodName, func() error {
			_, err := service.GetPlan(context.Background(), &routeplanpb.GetPlanRequest{PlanId: "plan-1"})
			return err
		}},
		{routeplanpb.RoutePlanService_MoveShard_FullMethodName, func() error {
			_, err := service.MoveShard(context.Background(), &routeplanpb.MoveShardRequest{})
			return err
		}},
		{routeplanpb.RoutePlanService_DrainHolder_FullMethodName, func() error {
			_, err := service.DrainHolder(context.Background(), &routeplanpb.DrainHolderRequest{})
			return err
		}},
		{routeplanpb.RoutePlanService_TakeoverAfterExpiry_FullMethodName, func() error {
			_, err := service.TakeoverAfterExpiry(context.Background(), &routeplanpb.TakeoverAfterExpiryRequest{})
			return err
		}},
		{routeplanpb.RoutePlanService_Verify_FullMethodName, func() error { _, err := service.Verify(context.Background(), &routeplanpb.VerifyRequest{}); return err }},
	}
	for _, test := range calls {
		authorizer.methods = nil
		if err := test.call(); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("method=%s err=%v", test.method, err)
		}
		if len(authorizer.methods) != 1 || authorizer.methods[0] != test.method {
			t.Fatalf("method=%s authorized=%v", test.method, authorizer.methods)
		}
	}
}

func TestPlanServiceStartupReadinessBlocksEveryMutation(t *testing.T) {
	fixture := newExecutorFixture(t)
	ready := false
	service, err := NewPlanService(PlanServiceOptions{
		Executor: fixture.executor, SnapshotStore: fixture.snapshot, HolderReader: fixture.holders,
		AuthorityReader: fixture.authority, Authorizer: &serviceAuthorizerFake{},
		ControllerID: "controller", Boot: "boot", Realm: "realm", Ready: func() bool { return ready },
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.GetStatus(context.Background(), &routeplanpb.GetStatusRequest{})
	if err != nil || response.GetReady() {
		t.Fatalf("startup status ready=%v err=%v", response.GetReady(), err)
	}
	for name, call := range map[string]func() error{
		"move": func() error {
			_, err := service.MoveShard(context.Background(), &routeplanpb.MoveShardRequest{})
			return err
		},
		"drain": func() error {
			_, err := service.DrainHolder(context.Background(), &routeplanpb.DrainHolderRequest{})
			return err
		},
		"takeover": func() error {
			_, err := service.TakeoverAfterExpiry(context.Background(), &routeplanpb.TakeoverAfterExpiryRequest{})
			return err
		},
	} {
		if err := call(); status.Code(err) != codes.Unavailable {
			t.Fatalf("%s during startup status=%s err=%v", name, status.Code(err), err)
		}
	}
	ready = true
	response, err = service.GetStatus(context.Background(), &routeplanpb.GetStatusRequest{})
	if err != nil || !response.GetReady() {
		t.Fatalf("recovered status ready=%v err=%v", response.GetReady(), err)
	}
	if _, err := service.MoveShard(context.Background(), &routeplanpb.MoveShardRequest{}); status.Code(err) == codes.Unavailable {
		t.Fatalf("ready mutation remained startup-gated: %v", err)
	}
}

func TestPlanServiceMoveShardCannotMintScalePlanProvenance(t *testing.T) {
	fixture := newExecutorFixture(t)
	service, err := NewPlanService(PlanServiceOptions{
		Executor: fixture.executor, SnapshotStore: fixture.snapshot, HolderReader: fixture.holders,
		AuthorityReader: fixture.authority, Authorizer: &serviceAuthorizerFake{},
		ControllerID: "controller", Boot: "boot", Realm: "realm", Ready: func() bool { return true },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.MoveShard(context.Background(), &routeplanpb.MoveShardRequest{
		OwnerScalePlanId: "sp-v2-forged", OwnerRetainUntilMs: fixture.now.Add(time.Hour).UnixMilli(),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("forged owner status=%s err=%v", status.Code(err), err)
	}
}

func TestPlanServiceAwaitsCompetingRecoveryOwnerAfterSubmit(t *testing.T) {
	fixture := newExecutorFixture(t)
	submitted, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	service := &PlanService{executor: fixture.executor}
	response, err := service.executeResponse(context.Background(), submitted, func(planID string) (PlanRecord, error) {
		final, executeErr := fixture.executor.Execute(context.Background(), planID)
		if executeErr != nil || final.Plan.Phase != PhaseSucceeded {
			t.Fatalf("competing owner phase=%s err=%v", final.Plan.Phase, executeErr)
		}
		return submitted, ErrShardBusy
	})
	if err != nil || response.GetPlan().GetPhase() != routeplanpb.PlanPhase_PLAN_PHASE_SUCCEEDED {
		t.Fatalf("response phase=%s err=%v", response.GetPlan().GetPhase(), err)
	}
}

func TestPlanServiceDoesNotReportNonterminalExecutionErrorAsResponse(t *testing.T) {
	fixture := newExecutorFixture(t)
	submitted, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	service := &PlanService{executor: fixture.executor}
	response, err := service.executeResponse(context.Background(), submitted, func(string) (PlanRecord, error) {
		return submitted, context.DeadlineExceeded
	})
	if response != nil || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("response=%v status=%s err=%v", response, status.Code(err), err)
	}
}

func TestPlanServiceRequiresAuthorizer(t *testing.T) {
	fixture := newExecutorFixture(t)
	_, err := NewPlanService(PlanServiceOptions{
		Executor: fixture.executor, SnapshotStore: fixture.snapshot, HolderReader: fixture.holders,
		AuthorityReader: fixture.authority, ControllerID: "controller", Boot: "boot", Realm: "realm",
	})
	if !errors.Is(err, ErrExecutorInvalid) {
		t.Fatalf("constructor err=%v want invalid executor", err)
	}
}

func TestRequestedTTLBoundsBeforeDurationConversion(t *testing.T) {
	for _, valid := range []int64{1, MaxRequestedTTLms} {
		got, err := requestedTTLFromMilliseconds(valid)
		if err != nil || got != time.Duration(valid)*time.Millisecond {
			t.Fatalf("value=%d duration=%v err=%v", valid, got, err)
		}
	}
	for _, invalid := range []int64{0, -1, MaxRequestedTTLms + 1, int64(^uint64(0) >> 1)} {
		if _, err := requestedTTLFromMilliseconds(invalid); !errors.Is(err, ErrInvalidPlan) {
			t.Fatalf("value=%d err=%v", invalid, err)
		}
	}
}

func TestFarmWorkerDomainIsPlannedHandoffEligible(t *testing.T) {
	domain, err := parseObservedDomain(string(actorrouting.DomainFarmWorker))
	if err != nil || domain != actorrouting.DomainFarmWorker {
		t.Fatalf("observed domain=%q err=%v", domain, err)
	}
	if planned, err := parseDomain(string(actorrouting.DomainFarmWorker)); err != nil ||
		planned != actorrouting.DomainFarmWorker {
		t.Fatalf("FARM_WORKER planned handoff domain=%q err=%v", planned, err)
	}
	snapshot := actorrouting.RouteSnapshot{
		FarmWorker: actorrouting.DomainSnapshot{
			ShardCount: 1,
			Routes: []actorrouting.ShardRoute{{
				HolderIndex: 0,
				State:       actorrouting.ShardStateActive,
			}},
		},
		Holders: []actorrouting.HolderRef{{InstanceID: "farm-worker-a"}},
	}
	route, holder, err := snapshotRoute(snapshot, actorrouting.DomainFarmWorker, 0)
	if err != nil || route.State != actorrouting.ShardStateActive ||
		holder.InstanceID != "farm-worker-a" {
		t.Fatalf("worker route=%+v holder=%+v err=%v", route, holder, err)
	}
}

func TestDrainHolderRequiresFreshRegisteredSourceWithActiveShard(t *testing.T) {
	newService := func(t *testing.T, fixture *executorFixture) *PlanService {
		t.Helper()
		service, err := NewPlanService(PlanServiceOptions{
			Executor: fixture.executor, SnapshotStore: fixture.snapshot, HolderReader: fixture.holders,
			AuthorityReader: fixture.authority, Authorizer: &serviceAuthorizerFake{},
			ControllerID: "controller", Boot: "boot", Realm: "realm", Now: func() time.Time { return fixture.now },
		})
		if err != nil {
			t.Fatal(err)
		}
		return service
	}
	request := func(sourceID, sourceBoot string) *routeplanpb.DrainHolderRequest {
		return &routeplanpb.DrainHolderRequest{
			PlanIdPrefix: "plan", TransitionIdPrefix: "transition", Domain: "PLAYER",
			SourceHolderId: sourceID, SourceBootId: sourceBoot,
			TargetHolderId: "holder-b", TargetBootId: "boot-b", TargetEndpoint: "target:443",
			RequestedTtlMs: 1000,
		}
	}

	t.Run("missing registry identity", func(t *testing.T) {
		fixture := newExecutorFixture(t)
		delete(fixture.holders.records, "holder-a")
		_, err := newService(t, fixture).DrainHolder(context.Background(), request("holder-a", "boot-a"))
		if status.Code(err) != codes.NotFound || fixture.manager.drainCalls != 0 {
			t.Fatalf("status=%s err=%v drain=%d", status.Code(err), err, fixture.manager.drainCalls)
		}
	})

	t.Run("stale generation", func(t *testing.T) {
		fixture := newExecutorFixture(t)
		source := fixture.holders.records["holder-a"]
		source.LeaseUntil = fixture.now
		fixture.holders.records["holder-a"] = source
		_, err := newService(t, fixture).DrainHolder(context.Background(), request("holder-a", "boot-a"))
		if status.Code(err) != codes.FailedPrecondition || fixture.manager.drainCalls != 0 {
			t.Fatalf("status=%s err=%v drain=%d", status.Code(err), err, fixture.manager.drainCalls)
		}
	})

	t.Run("no matching active shards", func(t *testing.T) {
		fixture := newExecutorFixture(t)
		fixture.holders.records["holder-x"] = core.HolderRecord{
			Domain: actorrouting.DomainPlayer, HolderID: "holder-x", BootID: "boot-x", LogicEndpoint: "x:443",
			HolderControlEndpoint: "x-control:443",
			State:                 core.HolderStateServing, ProcessReady: true, LeaseUntil: fixture.now.Add(time.Minute), SnapshotRevision: 1,
		}
		_, err := newService(t, fixture).DrainHolder(context.Background(), request("holder-x", "boot-x"))
		if status.Code(err) != codes.FailedPrecondition || fixture.manager.drainCalls != 0 {
			t.Fatalf("status=%s err=%v drain=%d", status.Code(err), err, fixture.manager.drainCalls)
		}
	})
}

func TestSubmissionRejectCode(t *testing.T) {
	if got := submissionRejectCode(fmt.Errorf("%w: target holder not ready", ErrPlanConflict)); got != "TARGET_NOT_READY" {
		t.Fatalf("got=%s", got)
	}
	if got := submissionRejectCode(ErrShardBusy); got != "SHARD_BUSY" {
		t.Fatalf("got=%s", got)
	}
}
