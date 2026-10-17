package routeplan

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/holderidentity"
	core "farmgame/internal/routecontrol"
	"farmgame/internal/storage/authoritystore"
)

type executorFixture struct {
	now         time.Time
	store       *MemoryPlanStore
	snapshot    *core.MemoryRouteSnapshotStore
	manager     *executorFakeManager
	authority   *executorFakeAuthorityReader
	holders     *executorFakeHolderReader
	executor    *PlanExecutor
	sourceFence authoritystore.ShardWriteFence
	source      authoritystore.ShardWriteAuthority
	target      authoritystore.ShardWriteAuthority
}

type listCountingPlanStore struct {
	PlanStore
	mu    sync.Mutex
	lists int
}

func (s *listCountingPlanStore) ListIncomplete(ctx context.Context) ([]PlanRecord, error) {
	s.mu.Lock()
	s.lists++
	s.mu.Unlock()
	return s.PlanStore.ListIncomplete(ctx)
}

func (s *listCountingPlanStore) listCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lists
}

func newExecutorFixture(t *testing.T) *executorFixture {
	t.Helper()
	now := time.UnixMilli(1_900_000_000_000)
	sourceLease := now.Add(10 * time.Minute).UnixMilli()
	snapshotValue := actorrouting.RouteSnapshot{
		Realm: "realm", GlobalRevision: 1, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608030001,
		Player:  actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: sourceLease}}},
		Farm:    actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: sourceLease}}},
		Holders: []actorrouting.HolderRef{{InstanceID: "holder-a", BootID: "boot-a", Endpoint: "source:443", State: actorrouting.HolderStateActive, Weight: 1}, {InstanceID: "farm-holder", BootID: "farm-boot", Endpoint: "farm:443", State: actorrouting.HolderStateActive, Weight: 1}},
	}
	snapshotStore := core.NewMemoryRouteSnapshotStore()
	if _, err := snapshotStore.Bootstrap(context.Background(), core.NestedControlOperationToken{}, snapshotValue); err != nil {
		t.Fatal(err)
	}
	sourceFence := authoritystore.ShardWriteFence{ExpectedHolderID: "holder-a", ExpectedHolderBootID: "boot-a", ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1}
	source := authoritystore.ShardWriteAuthority{AuthorityLayoutVersion: "v1", HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: snapshotValue.HashSeed, Domain: actorrouting.DomainPlayer, VirtualShard: 0, HolderID: "holder-a", HolderBootID: "boot-a", AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1, State: authoritystore.ShardAuthorityActive, LeaseUntilMs: sourceLease, LastTransitionID: "initial", LastTransitionAtMs: now.Add(-time.Minute).UnixMilli()}
	target := source
	target.HolderID, target.HolderBootID, target.AssignmentRevision, target.WriterEpoch, target.LeaseEpoch, target.LastTransitionID, target.LeaseUntilMs = "holder-b", "boot-b", 2, 2, 2, "transition-1", now.Add(10*time.Minute).UnixMilli()
	store, err := NewMemoryPlanStore(MemoryPlanStoreOptions{Now: func() time.Time { return now }, PlanTTL: 24 * time.Hour, AuditRetention: 38 * 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	authorityReader := &executorFakeAuthorityReader{value: source, found: true}
	holders := &executorFakeHolderReader{records: map[string]core.HolderRecord{
		"holder-a": {Domain: actorrouting.DomainPlayer, HolderID: "holder-a", BootID: "boot-a", LogicEndpoint: "source:443", HolderControlEndpoint: "source-control:9443", TLSDNSIdentity: "source.internal", State: core.HolderStateServing, ProcessReady: true, LeaseUntil: now.Add(10 * time.Minute), SnapshotRevision: 1, CapacityWeight: 1},
		"holder-b": {Domain: actorrouting.DomainPlayer, HolderID: "holder-b", BootID: "boot-b", LogicEndpoint: "target:443", HolderControlEndpoint: "target-control:9443", TLSDNSIdentity: "target.internal", State: core.HolderStateReadyZeroShard, ProcessReady: true, LeaseUntil: now.Add(10 * time.Minute), SnapshotRevision: 1, CapacityWeight: 1},
	}}
	manager := &executorFakeManager{snapshot: snapshotValue, authorityReader: authorityReader, snapshotStore: snapshotStore, target: target}
	executor, err := NewPlanExecutor(PlanExecutorOptions{Store: store, Manager: manager, AuthorityReader: authorityReader, SnapshotStore: snapshotStore, HolderReader: holders, Now: func() time.Time { return now }, OperationTimeout: time.Second, PlanTTL: 24 * time.Hour, DrainControllerFactory: func(context.Context, Plan) (authority.DrainController, func() error, error) {
		return &executorFakeDrainController{}, func() error { return nil }, nil
	}, SingleController: SingleControllerAttestation{Contract: SingleControllerContract, ControllerID: "controller-a", BootID: "controller-boot-a"}})
	if err != nil {
		t.Fatal(err)
	}
	return &executorFixture{now: now, store: store, snapshot: snapshotStore, manager: manager, authority: authorityReader, holders: holders, executor: executor, sourceFence: sourceFence, source: source, target: target}
}

func (f *executorFixture) moveRequest() MoveRequest {
	return MoveRequest{PlanID: "plan-1", TransitionID: "transition-1", Domain: actorrouting.DomainPlayer, VirtualShard: 0, SourceHolderID: "holder-a", SourceBootID: "boot-a", SourceEndpoint: "source:443", TargetHolderID: "holder-b", TargetBootID: "boot-b", TargetEndpoint: "target:443", ExpectedSourceFence: f.sourceFence, RequestedTTL: time.Minute}
}

func (f *executorFixture) takeoverRequest() TakeoverRequest {
	req := f.moveRequest()
	return TakeoverRequest{PlanID: req.PlanID, TransitionID: req.TransitionID, Domain: req.Domain, VirtualShard: req.VirtualShard, SourceHolderID: req.SourceHolderID, SourceBootID: req.SourceBootID, SourceEndpoint: req.SourceEndpoint, TargetHolderID: req.TargetHolderID, TargetBootID: req.TargetBootID, TargetEndpoint: req.TargetEndpoint, ExpectedSourceFence: req.ExpectedSourceFence, RequestedTTL: req.RequestedTTL, Proof: TakeoverProof{ExpectedHolderBootID: "boot-a", LeaseExpired: true, Evidence: "old holder lease expired"}}
}

func TestCrashTakeoverForegroundBatchScansIncompletePlansOnce(t *testing.T) {
	fixture := newExecutorFixture(t)
	fixture.authority.value.LeaseUntilMs = fixture.now.Add(-time.Second).UnixMilli()
	fixture.holders.records["holder-a"] = core.HolderRecord{Domain: actorrouting.DomainPlayer, HolderID: "holder-a", BootID: "boot-a", LogicEndpoint: "source:443", State: core.HolderStateDraining, Draining: true, ProcessReady: false, LeaseUntil: fixture.now.Add(-time.Second)}
	counting := &listCountingPlanStore{PlanStore: fixture.store}
	fixture.executor.store = counting
	release, err := fixture.executor.BeginForegroundBatch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if calls := counting.listCalls(); calls != 1 {
		t.Fatalf("foreground initialization ListIncomplete calls=%d, want 1", calls)
	}
	record, err := fixture.executor.SubmitCrashTakeover(context.Background(), fixture.takeoverRequest())
	if err != nil {
		release()
		t.Fatal(err)
	}
	if calls := counting.listCalls(); calls != 1 {
		release()
		t.Fatalf("foreground submit rescanned incomplete plans: calls=%d", calls)
	}
	release()

	replay, err := fixture.executor.SubmitCrashTakeover(context.Background(), fixture.takeoverRequest())
	if err != nil || replay.Plan.PlanID != record.Plan.PlanID {
		t.Fatalf("standalone replay=%+v err=%v", replay.Plan, err)
	}
	if calls := counting.listCalls(); calls != 2 {
		t.Fatalf("standalone submission did not retain full conflict proof: calls=%d", calls)
	}
}

func (f *executorFixture) ownedMoveRequest() MoveRequest {
	req := f.moveRequest()
	workloadUID := "workload-player"
	req.OwnerScalePlanID = "sp-v2-sliding"
	req.OwnerRetainUntilMs = f.now.Add(48 * time.Hour).UnixMilli()
	req.SourceDeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-a", WorkloadUID: workloadUID, Ordinal: 0}
	req.TargetDeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-b", WorkloadUID: workloadUID, Ordinal: 1}
	source := f.holders.records[req.SourceHolderID]
	source.DeploymentIdentity = req.SourceDeploymentIdentity
	f.holders.records[req.SourceHolderID] = source
	target := f.holders.records[req.TargetHolderID]
	target.DeploymentIdentity = req.TargetDeploymentIdentity
	f.holders.records[req.TargetHolderID] = target
	return req
}

func (f *executorFixture) persistOwnedMove(t *testing.T, req MoveRequest) PlanRecord {
	t.Helper()
	plan := Plan{
		PlanID: req.PlanID, TransitionID: req.TransitionID, Domain: req.Domain, VirtualShard: req.VirtualShard,
		SourceHolderID: req.SourceHolderID, SourceBootID: req.SourceBootID,
		SourceDeploymentIdentity: req.SourceDeploymentIdentity, SourceEndpoint: req.SourceEndpoint,
		TargetHolderID: req.TargetHolderID, TargetBootID: req.TargetBootID,
		TargetDeploymentIdentity: req.TargetDeploymentIdentity, TargetEndpoint: req.TargetEndpoint,
		ExpectedSourceFence: req.ExpectedSourceFence, RequestedTTLms: req.RequestedTTL.Milliseconds(),
		Operation: OperationPlannedDrain, Phase: PhasePending, Revision: 1,
		CreatedAtMs: f.now.UnixMilli(), UpdatedAtMs: f.now.UnixMilli(), ExpiresAtMs: f.now.Add(24 * time.Hour).UnixMilli(),
		OwnerScalePlanID: req.OwnerScalePlanID, OwnerRetainUntilMs: req.OwnerRetainUntilMs,
	}
	result, err := f.store.CompareAndSwap(context.Background(), 0, plan)
	if err != nil || result.Status != CASCommitted {
		t.Fatalf("persist owned child result=%+v err=%v", result, err)
	}
	return result.Record
}

func TestPlanExecutorOwnedChildContinuesDuringUnrelatedAssignmentReconcile(t *testing.T) {
	fixture := newExecutorFixture(t)
	req := fixture.ownedMoveRequest()
	for _, holderID := range []string{req.SourceHolderID, req.TargetHolderID} {
		record := fixture.holders.records[holderID]
		record.State = core.HolderStateServing
		record.ProcessReady = false
		record.Draining = false
		record.ServingShardCount = 1
		fixture.holders.records[holderID] = record
	}
	record := fixture.persistOwnedMove(t, req)
	result, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || result.Plan.Phase != PhaseSucceeded {
		t.Fatalf("result=%+v err=%v", result.Plan, err)
	}
}

func TestPlanExecutorDirectMoveCannotMintScalePlanProvenance(t *testing.T) {
	fixture := newExecutorFixture(t)
	if _, err := fixture.executor.SubmitMove(context.Background(), fixture.ownedMoveRequest()); !errors.Is(err, ErrInvalidPlan) {
		t.Fatalf("direct move minted ScalePlan owner provenance: err=%v", err)
	}
}

func TestPlanExecutorUnownedMoveStillRejectsUnreadyServingRuntime(t *testing.T) {
	fixture := newExecutorFixture(t)
	source := fixture.holders.records["holder-a"]
	source.ProcessReady = false
	source.ServingShardCount = 1
	fixture.holders.records["holder-a"] = source
	if _, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest()); !errors.Is(err, ErrPlanConflict) {
		t.Fatalf("unowned move err=%v want=%v", err, ErrPlanConflict)
	}
}

func TestPlanExecutorOwnedChildRejectsDrainingRuntime(t *testing.T) {
	fixture := newExecutorFixture(t)
	req := fixture.ownedMoveRequest()
	target := fixture.holders.records[req.TargetHolderID]
	target.State = core.HolderStateDraining
	target.ProcessReady = false
	target.Draining = true
	target.ServingShardCount = 1
	fixture.holders.records[req.TargetHolderID] = target
	record := fixture.persistOwnedMove(t, req)
	result, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
	if err == nil || result.Plan.Phase != PhasePaused || fixture.manager.drainCalls != 0 {
		t.Fatalf("owned child admitted draining runtime: result=%+v err=%v drain=%d", result.Plan, err, fixture.manager.drainCalls)
	}
}

func TestPlanExecutorScalePlanChildUsesCachedVerifiedHolderView(t *testing.T) {
	fixture := newExecutorFixture(t)
	var freshCalls, cachedCalls int
	fixture.holders.getHook = func(context.Context, actorrouting.RouteDomain, string) (core.HolderRecord, bool, error) {
		freshCalls++
		return core.HolderRecord{}, false, errors.New("unexpected synchronous domain refresh")
	}
	fixture.holders.cachedHook = func(_ context.Context, _ actorrouting.RouteDomain, id string) (core.HolderRecord, bool, error) {
		cachedCalls++
		value, found := fixture.holders.records[id]
		return value, found, nil
	}

	owned := Plan{Domain: actorrouting.DomainPlayer, OwnerScalePlanID: "sp-v2-owned"}
	record, found, err := fixture.executor.holderForPlan(context.Background(), owned, "holder-a")
	if err != nil || !found || record.HolderID != "holder-a" || cachedCalls != 1 || freshCalls != 0 {
		t.Fatalf("owned lookup record=%+v found=%t cached=%d fresh=%d err=%v", record, found, cachedCalls, freshCalls, err)
	}
	crash := Plan{
		Domain: actorrouting.DomainPlayer, Operation: OperationCrashTakeover,
		TargetDeploymentIdentity: holderidentity.DeploymentIdentity{PodUID: "target-pod", WorkloadUID: "workload", Ordinal: 1},
	}
	record, found, err = fixture.executor.holderForPlan(context.Background(), crash, "holder-b")
	if err != nil || !found || record.HolderID != "holder-b" || cachedCalls != 2 || freshCalls != 0 {
		t.Fatalf("pinned crash lookup record=%+v found=%t cached=%d fresh=%d err=%v", record, found, cachedCalls, freshCalls, err)
	}

	manual := Plan{Domain: actorrouting.DomainPlayer}
	if _, _, err := fixture.executor.holderForPlan(context.Background(), manual, "holder-a"); err == nil || freshCalls != 1 {
		t.Fatalf("manual lookup fresh=%d err=%v", freshCalls, err)
	}
}

func TestPlanExecutorAttemptUsesOneAuditTimestamp(t *testing.T) {
	fixture := newExecutorFixture(t)
	record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	base := fixture.now.Add(time.Second)
	fixture.executor.now = func() time.Time {
		value := base.Add(time.Duration(calls) * time.Millisecond)
		calls++
		return value
	}
	plan, err := fixture.executor.bumpAttempt(context.Background(), record.Plan.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || plan.Attempt != 1 || plan.StartedAtMs != plan.UpdatedAtMs || plan.StartedAtMs != base.UnixMilli() {
		t.Fatalf("calls=%d plan=%+v", calls, plan)
	}
}

func TestPlanExecutorPlannedDrainOrderAndNoPublicTransfer(t *testing.T) {
	fixture := newExecutorFixture(t)
	record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil {
		t.Fatalf("execute result=%+v err=%v", result.Plan, err)
	}
	if result.Plan.Phase != PhaseSucceeded || fixture.manager.drainCalls != 1 || fixture.manager.transferCalls != 0 {
		t.Fatalf("plan=%+v drain=%d transfer=%d", result.Plan, fixture.manager.drainCalls, fixture.manager.transferCalls)
	}
	wantOrder := []string{"stop", "wait", "facts", "cold-load"}
	if got := stringsJoin(fixture.manager.controllerOrder); got != stringsJoin(wantOrder) {
		t.Fatalf("controller order=%v", fixture.manager.controllerOrder)
	}
}

func TestWorkerHandoffSourceAcceptsLaterSnapshotRevision(t *testing.T) {
	fixture := newExecutorFixture(t)
	seed, err := fixture.executor.SubmitMove(
		context.Background(),
		fixture.moveRequest(),
	)
	if err != nil {
		t.Fatal(err)
	}
	plan := seed.Plan
	plan.PlanID = "worker-plan"
	plan.TransitionID = "worker-transition"
	plan.Domain = actorrouting.DomainFarmWorker
	plan.SourceHolderID = "source"
	plan.SourceBootID = "source-boot"
	plan.SourceEndpoint = "source-worker:9201"
	plan.TargetHolderID = "target"
	plan.TargetBootID = "target-boot"
	plan.TargetEndpoint = "target-worker:9201"
	plan.ExpectedSourceFence.ExpectedHolderID = plan.SourceHolderID
	plan.ExpectedSourceFence.ExpectedHolderBootID = plan.SourceBootID

	store, err := NewMemoryPlanStore(MemoryPlanStoreOptions{
		Now:     func() time.Time { return fixture.now },
		PlanTTL: 24 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result, createErr := store.CompareAndSwap(
		context.Background(),
		0,
		plan,
	); createErr != nil || result.Status != CASCommitted {
		t.Fatalf("create worker plan result=%+v err=%v", result, createErr)
	}
	fixture.executor.store = store
	if _, err := fixture.executor.bumpAttempt(
		context.Background(),
		plan.PlanID,
	); err != nil {
		t.Fatal(err)
	}
	if err := fixture.executor.advancePhase(
		context.Background(),
		plan.PlanID,
		PhaseDraining,
	); err != nil {
		t.Fatal(err)
	}
	fixture.holders.records[plan.SourceHolderID] = core.HolderRecord{
		Domain:            plan.Domain,
		HolderID:          plan.SourceHolderID,
		BootID:            plan.SourceBootID,
		State:             core.HolderStateServing,
		ProcessReady:      true,
		ServingShardCount: 2,
		SnapshotRevision:  11,
		LeaseUntil:        fixture.now.Add(time.Minute),
	}
	getCalls := 0
	fixture.holders.getHook = func(
		context.Context,
		actorrouting.RouteDomain,
		string,
	) (core.HolderRecord, bool, error) {
		getCalls++
		if getCalls == 1 {
			return core.HolderRecord{}, false,
				core.ErrHolderRegistryUnavailable
		}
		record, found := fixture.holders.records[plan.SourceHolderID]
		return record, found, nil
	}
	validator := fixture.executor.workerHandoffSourceValidator(plan.PlanID)
	if err := validator(
		context.Background(),
		authority.WorkerHandoffSourceObservation{
			Domain:                plan.Domain,
			VirtualShard:          plan.VirtualShard,
			SourceHolderID:        plan.SourceHolderID,
			SourceHolderBootID:    plan.SourceBootID,
			SnapshotRevision:      10,
			ExpectedServingShards: 2,
		},
	); err != nil {
		t.Fatalf("later source revision rejected: %v", err)
	}
	record, found, err := store.Load(context.Background(), plan.PlanID)
	if err != nil || !found || record.Plan.Phase != PhaseAssigning {
		t.Fatalf("worker plan=%+v found=%t err=%v", record.Plan, found, err)
	}
	if getCalls != 2 {
		t.Fatalf("source holder reads=%d", getCalls)
	}
}

func TestTargetValidationWaitsForAppliedSnapshotRevision(t *testing.T) {
	fixture := newExecutorFixture(t)
	record, err := fixture.executor.SubmitMove(
		context.Background(),
		fixture.moveRequest(),
	)
	if err != nil {
		t.Fatal(err)
	}
	getCalls := 0
	fixture.holders.getHook = func(
		context.Context,
		actorrouting.RouteDomain,
		string,
	) (core.HolderRecord, bool, error) {
		getCalls++
		if getCalls == 1 {
			return core.HolderRecord{}, false,
				core.ErrHolderRegistryUnavailable
		}
		target := fixture.holders.records["holder-b"]
		if getCalls >= 3 {
			target.SnapshotRevision = 2
		}
		return target, true, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := fixture.executor.validateTargetForSnapshot(
		ctx,
		record.Plan,
		2,
		false,
	); err != nil {
		t.Fatalf("target did not catch up: %v", err)
	}
	if getCalls != 3 {
		t.Fatalf("target holder reads=%d", getCalls)
	}
}

func TestTargetValidationAcceptsLaterSnapshotRevision(t *testing.T) {
	fixture := newExecutorFixture(t)
	record, err := fixture.executor.SubmitMove(
		context.Background(),
		fixture.moveRequest(),
	)
	if err != nil {
		t.Fatal(err)
	}
	target := fixture.holders.records["holder-b"]
	target.SnapshotRevision = 3
	fixture.holders.records["holder-b"] = target
	if err := fixture.executor.validateTargetForSnapshot(
		context.Background(),
		record.Plan,
		2,
		false,
	); err != nil {
		t.Fatalf("later target revision rejected: %v", err)
	}
}

func TestScalePlanProjectionValidationIsOneShotAndUsesFrozenShardRevision(t *testing.T) {
	fixture := newExecutorFixture(t)
	req := fixture.ownedMoveRequest()
	record := fixture.persistOwnedMove(t, req)
	reads := 0
	fixture.holders.cachedHook = func(_ context.Context, _ actorrouting.RouteDomain, id string) (core.HolderRecord, bool, error) {
		reads++
		value, found := fixture.holders.records[id]
		return value, found, nil
	}
	recovery := authority.TransitionRecovery{
		TransitionID: req.TransitionID, Domain: req.Domain, VirtualShard: req.VirtualShard,
		ExpectedFence: req.ExpectedSourceFence, NewHolderID: req.TargetHolderID, NewHolderBootID: req.TargetBootID,
		Endpoint: req.TargetEndpoint, RequestedTTLms: req.RequestedTTL.Milliseconds(),
		InternalDrain: true, Mode: authority.TransferModeDrainHandoff, Committed: true, Published: true,
		LastState: actorrouting.ShardStateAssigning, PublishedSnapshotRevision: 2,
	}
	validator := fixture.executor.pendingProjectionTargetValidator(record.Plan.PlanID)
	if err := validator(context.Background(), recovery, 99); !errors.Is(err, ErrPlanConflict) {
		t.Fatalf("stale target validation err=%v", err)
	}
	if reads != 1 {
		t.Fatalf("Manager callback polled under lock: reads=%d", reads)
	}
	target := fixture.holders.records[req.TargetHolderID]
	target.SnapshotRevision = recovery.PublishedSnapshotRevision
	fixture.holders.records[req.TargetHolderID] = target
	if err := validator(context.Background(), recovery, 99); err != nil {
		t.Fatalf("unrelated global revision invalidated shard-local projection: %v", err)
	}
	if reads != 2 {
		t.Fatalf("successful Manager callback reads=%d want=2", reads)
	}
}

func TestPlanSubmissionAcceptsLaterTargetSnapshotRevision(t *testing.T) {
	fixture := newExecutorFixture(t)
	target := fixture.holders.records["holder-b"]
	target.SnapshotRevision = fixture.manager.snapshot.GlobalRevision + 1
	fixture.holders.records["holder-b"] = target
	if _, err := fixture.executor.SubmitMove(
		context.Background(),
		fixture.moveRequest(),
	); err != nil {
		t.Fatalf("later target submission rejected: %v", err)
	}
}

func TestPlanSubmissionIgnoresUnrelatedGlobalRevisionAdvanceAtSource(t *testing.T) {
	fixture := newExecutorFixture(t)
	fixture.manager.snapshot.GlobalRevision++
	target := fixture.holders.records["holder-b"]
	target.SnapshotRevision = fixture.manager.snapshot.GlobalRevision
	fixture.holders.records["holder-b"] = target
	if source := fixture.holders.records["holder-a"]; source.SnapshotRevision != 1 {
		t.Fatalf("source revision=%d, want unchanged 1", source.SnapshotRevision)
	}
	if _, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest()); err != nil {
		t.Fatalf("unrelated global revision invalidated per-shard source fence: %v", err)
	}
}

func TestPlanSubmissionWaitsForTargetSnapshotCatchUp(t *testing.T) {
	fixture := newExecutorFixture(t)
	targetReads := 0
	fixture.holders.getHook = func(
		ctx context.Context,
		domain actorrouting.RouteDomain,
		id string,
	) (core.HolderRecord, bool, error) {
		if id != "holder-b" {
			record, ok := fixture.holders.records[id]
			return record, ok, nil
		}
		targetReads++
		record := fixture.holders.records["holder-b"]
		if targetReads == 1 {
			return core.HolderRecord{}, false,
				core.ErrHolderRegistryUnavailable
		}
		if targetReads == 2 {
			record.SnapshotRevision = 0
			return record, true, nil
		}
		return record, true, nil
	}
	if _, err := fixture.executor.SubmitMove(
		context.Background(),
		fixture.moveRequest(),
	); err != nil {
		t.Fatalf("catch-up target submission rejected: %v", err)
	}
	if targetReads != 3 {
		t.Fatalf("target holder reads=%d", targetReads)
	}
}

func TestPlanExecutorPublishesLogicEndpointOnly(t *testing.T) {
	fixture := newExecutorFixture(t)
	record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID); err != nil {
		t.Fatal(err)
	}
	snapshot := fixture.manager.CurrentSnapshot()
	route := snapshot.Player.Routes[0]
	holder := snapshot.Holders[route.HolderIndex]
	if holder.Endpoint != fixture.holders.records["holder-b"].LogicEndpoint {
		t.Fatalf("snapshot endpoint=%q want logic=%q", holder.Endpoint, fixture.holders.records["holder-b"].LogicEndpoint)
	}
	if holder.Endpoint == fixture.holders.records["holder-b"].HolderControlEndpoint {
		t.Fatalf("snapshot leaked HolderControl endpoint=%q", holder.Endpoint)
	}
}

func TestPlanExecutorRejectsMissingOrSwappedEndpointRolesBeforeManager(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*core.HolderRecord)
	}{
		{name: "missing control", mutate: func(record *core.HolderRecord) { record.HolderControlEndpoint = "" }},
		{name: "swapped", mutate: func(record *core.HolderRecord) {
			record.LogicEndpoint, record.HolderControlEndpoint = record.HolderControlEndpoint, record.LogicEndpoint
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExecutorFixture(t)
			target := fixture.holders.records["holder-b"]
			test.mutate(&target)
			fixture.holders.records["holder-b"] = target
			if _, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest()); !errors.Is(err, ErrPlanConflict) {
				t.Fatalf("submit err=%v", err)
			}
			if fixture.manager.drainCalls != 0 || fixture.manager.transferCalls != 0 {
				t.Fatalf("manager calls drain=%d transfer=%d", fixture.manager.drainCalls, fixture.manager.transferCalls)
			}
		})
	}
}

func TestPlanExecutorCrashProofTable(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*executorFixture, *TakeoverRequest)
		wantSubmit error
	}{
		{name: "no proof", mutate: func(_ *executorFixture, req *TakeoverRequest) {
			req.Proof = TakeoverProof{ExpectedHolderBootID: "boot-a", Evidence: "e"}
		}, wantSubmit: ErrTakeoverProof},
		{name: "live holder", mutate: func(_ *executorFixture, req *TakeoverRequest) {
			req.Proof = TakeoverProof{ExpectedHolderBootID: "boot-a", HolderUnavailable: true, Evidence: "e"}
		}, wantSubmit: ErrTakeoverProof},
		{name: "wrong boot", mutate: func(_ *executorFixture, req *TakeoverRequest) {
			req.Proof = TakeoverProof{ExpectedHolderBootID: "other-boot", LeaseExpired: true, Evidence: "e"}
		}, wantSubmit: ErrTakeoverProof},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExecutorFixture(t)
			req := fixture.takeoverRequest()
			test.mutate(fixture, &req)
			if _, err := fixture.executor.SubmitCrashTakeover(context.Background(), req); !errors.Is(err, test.wantSubmit) {
				t.Fatalf("submit err=%v want=%v", err, test.wantSubmit)
			}
		})
	}
	fixture := newExecutorFixture(t)
	target := fixture.holders.records["holder-b"]
	target.DeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-b", WorkloadUID: "workload-b", Ordinal: 1}
	fixture.holders.records["holder-b"] = target
	fixture.authority.value.LeaseUntilMs = fixture.now.Add(-time.Second).UnixMilli()
	fixture.holders.records["holder-a"] = core.HolderRecord{Domain: actorrouting.DomainPlayer, HolderID: "holder-a", BootID: "boot-a", LogicEndpoint: "source:443", State: core.HolderStateDraining, Draining: true, ProcessReady: false, LeaseUntil: fixture.now.Add(-time.Second)}
	record, err := fixture.executor.SubmitCrashTakeover(context.Background(), fixture.takeoverRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID); err != nil {
		t.Fatal(err)
	}
	if fixture.manager.transferCalls != 1 || fixture.manager.drainCalls != 0 {
		t.Fatalf("drain=%d transfer=%d", fixture.manager.drainCalls, fixture.manager.transferCalls)
	}
}

func TestPlanExecutorReconcileSkipsManagerAfterDurableTargetCommit(t *testing.T) {
	fixture := newExecutorFixture(t)
	fixture.authority.value = fixture.target
	fixture.manager.snapshot = targetSnapshot(fixture.manager.snapshot, fixture.target)
	if _, err := fixture.snapshot.CompareAndSwap(context.Background(), core.NestedControlOperationToken{}, 1, fixture.manager.snapshot); err != nil {
		t.Fatal(err)
	}
	plan := fixture.moveRequest()
	planRecord := Plan{PlanID: plan.PlanID, TransitionID: plan.TransitionID, Domain: plan.Domain, VirtualShard: plan.VirtualShard, SourceHolderID: plan.SourceHolderID, SourceBootID: plan.SourceBootID, SourceEndpoint: plan.SourceEndpoint, TargetHolderID: plan.TargetHolderID, TargetBootID: plan.TargetBootID, TargetEndpoint: plan.TargetEndpoint, ExpectedSourceFence: plan.ExpectedSourceFence, RequestedTTLms: plan.RequestedTTL.Milliseconds(), Operation: OperationPlannedDrain, Phase: PhasePending, Revision: 1, CreatedAtMs: fixture.now.UnixMilli(), UpdatedAtMs: fixture.now.UnixMilli(), ExpiresAtMs: fixture.now.Add(time.Hour).UnixMilli()}
	if _, err := fixture.store.CompareAndSwap(context.Background(), 0, planRecord); err != nil {
		t.Fatal(err)
	}
	result, err := fixture.executor.Execute(context.Background(), planRecord.PlanID)
	if err != nil || result.Plan.Phase != PhaseSucceeded || fixture.manager.drainCalls != 0 || fixture.manager.transferCalls != 0 {
		t.Fatalf("result=%+v err=%v drain=%d transfer=%d", result.Plan, err, fixture.manager.drainCalls, fixture.manager.transferCalls)
	}
}

func TestPlanExecutorCrashTakeoverAcceptsVerifiedWarmingColdTarget(t *testing.T) {
	fixture := newExecutorFixture(t)
	fixture.authority.value.LeaseUntilMs = fixture.now.Add(-time.Second).UnixMilli()
	target := fixture.holders.records["holder-b"]
	target.State = core.HolderStateWarming
	target.ProcessReady = false
	target.ServingShardCount = 0
	fixture.holders.records["holder-b"] = target
	record, err := fixture.executor.SubmitCrashTakeover(context.Background(), fixture.takeoverRequest())
	if err != nil {
		t.Fatalf("verified WARMING cold target rejected: %v", err)
	}
	if record.Plan.Phase != PhasePending || record.Plan.TargetBootID != "boot-b" {
		t.Fatalf("unexpected takeover plan: %+v", record.Plan)
	}
}

func TestCrashTakeoverProjectionAcceptsExactTransitionalTargetAfterAssigningPublish(t *testing.T) {
	fixture := newExecutorFixture(t)
	fixture.authority.value.LeaseUntilMs = fixture.now.Add(-time.Second).UnixMilli()
	record, err := fixture.executor.SubmitCrashTakeover(context.Background(), fixture.takeoverRequest())
	if err != nil {
		t.Fatal(err)
	}
	target := fixture.holders.records[record.Plan.TargetHolderID]
	target.State = core.HolderStateWarming
	target.ProcessReady = false
	target.ServingShardCount = 0
	target.SnapshotRevision = 2
	fixture.holders.records[record.Plan.TargetHolderID] = target
	recovery := authority.TransitionRecovery{
		TransitionID: record.Plan.TransitionID, Domain: record.Plan.Domain, VirtualShard: record.Plan.VirtualShard,
		ExpectedFence: record.Plan.ExpectedSourceFence, NewHolderID: record.Plan.TargetHolderID,
		NewHolderBootID: record.Plan.TargetBootID, Endpoint: record.Plan.TargetEndpoint,
		RequestedTTLms: record.Plan.RequestedTTLms, Mode: authority.TransferModeCrashTakeover,
		TakeoverProof: authority.TransferTakeoverProof{
			ExpectedHolderBootID: record.Plan.TakeoverProof.ExpectedHolderBootID,
			HolderUnavailable:    record.Plan.TakeoverProof.HolderUnavailable,
			LeaseExpired:         record.Plan.TakeoverProof.LeaseExpired,
			Evidence:             record.Plan.TakeoverProof.Evidence,
		},
		Committed: true, Published: true, LastState: actorrouting.ShardStateAssigning,
		PublishedSnapshotRevision: target.SnapshotRevision,
	}
	if err := fixture.executor.pendingProjectionTargetValidator(record.Plan.PlanID)(context.Background(), recovery, target.SnapshotRevision); err != nil {
		t.Fatalf("exact WARMING target rejected after ASSIGNING publish: %v", err)
	}
	target.State = core.HolderStateServing
	target.ServingShardCount = 1
	fixture.holders.records[record.Plan.TargetHolderID] = target
	if err := fixture.executor.pendingProjectionTargetValidator(record.Plan.PlanID)(context.Background(), recovery, target.SnapshotRevision); err != nil {
		t.Fatalf("exact unready SERVING target rejected after ASSIGNING publish: %v", err)
	}
}

func TestPlanExecutorRetriesLegacyProjectionFailureForExactCrashTakeover(t *testing.T) {
	fixture := newExecutorFixture(t)
	fixture.authority.value.LeaseUntilMs = fixture.now.Add(-time.Second).UnixMilli()
	record, err := fixture.executor.SubmitCrashTakeover(context.Background(), fixture.takeoverRequest())
	if err != nil {
		t.Fatal(err)
	}
	paused := record.Plan
	paused.Phase = PhasePaused
	paused.ErrorCode = "EXECUTION_FAILED"
	paused.CompletedAtMs = fixture.now.UnixMilli()
	paused.Revision++
	result, err := fixture.store.CompareAndSwap(context.Background(), record.Plan.Revision, paused)
	if err != nil || result.Status != CASCommitted {
		t.Fatalf("pause result=%+v err=%v", result, err)
	}
	completed, err := fixture.executor.Execute(context.Background(), paused.PlanID)
	if err != nil || completed.Plan.Phase != PhaseSucceeded || completed.Plan.Attempt != 1 {
		t.Fatalf("completed=%+v err=%v", completed.Plan, err)
	}
}

func TestPlanExecutorAuthorityCommitSnapshotPendingNeverReplaysManager(t *testing.T) {
	for _, operation := range []OperationKind{OperationPlannedDrain, OperationCrashTakeover} {
		t.Run(string(operation), func(t *testing.T) {
			fixture := newExecutorFixture(t)
			var record PlanRecord
			var err error
			if operation == OperationCrashTakeover {
				fixture.authority.value.LeaseUntilMs = fixture.now.Add(-time.Second).UnixMilli()
				fixture.holders.records["holder-a"] = core.HolderRecord{
					Domain: actorrouting.DomainPlayer, HolderID: "holder-a", BootID: "boot-a", LogicEndpoint: "source:443",
					State: core.HolderStateDraining, Draining: true, LeaseUntil: fixture.now.Add(-time.Second),
				}
				record, err = fixture.executor.SubmitCrashTakeover(context.Background(), fixture.takeoverRequest())
			} else {
				record, err = fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
			}
			if err != nil {
				t.Fatal(err)
			}
			fixture.authority.value = fixture.target
			paused, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
			if !errors.Is(err, ErrVerification) || paused.Plan.Phase != PhasePaused || paused.Plan.ErrorCode != ErrorAuthorityCommittedSnapshotPending {
				t.Fatalf("paused=%+v err=%v", paused.Plan, err)
			}
			if fixture.manager.drainCalls != 0 || fixture.manager.transferCalls != 0 {
				t.Fatalf("manager calls drain=%d transfer=%d", fixture.manager.drainCalls, fixture.manager.transferCalls)
			}

			revision := paused.Plan.Revision
			recoveredSnapshot := targetSnapshot(fixture.manager.snapshot, fixture.target)
			if _, err := fixture.snapshot.CompareAndSwap(context.Background(), core.NestedControlOperationToken{}, 1, recoveredSnapshot); err != nil {
				t.Fatal(err)
			}
			reconciled, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
			if err != nil || reconciled.Plan.Phase != PhasePaused || reconciled.Plan.Revision != revision ||
				reconciled.Plan.ErrorCode != ErrorAuthorityCommittedSnapshotPending {
				t.Fatalf("reconciled=%+v err=%v", reconciled.Plan, err)
			}
			if fixture.manager.drainCalls != 0 || fixture.manager.transferCalls != 0 {
				t.Fatalf("manager replayed drain=%d transfer=%d", fixture.manager.drainCalls, fixture.manager.transferCalls)
			}
		})
	}
}

func TestPlanExecutorRevalidatesTargetImmediatelyBeforeManager(t *testing.T) {
	fixture := newExecutorFixture(t)
	fixture.executor.drainFactory = func(context.Context, Plan) (authority.DrainController, func() error, error) {
		target := fixture.holders.records["holder-b"]
		target.BootID = "replacement-boot"
		fixture.holders.records["holder-b"] = target
		return &executorFakeDrainController{}, nil, nil
	}
	record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, ErrPlanConflict) || result.Plan.Phase != PhasePaused {
		t.Fatalf("result=%+v err=%v", result.Plan, err)
	}
	if fixture.manager.drainCalls != 0 || fixture.manager.transferCalls != 0 {
		t.Fatalf("manager calls drain=%d transfer=%d", fixture.manager.drainCalls, fixture.manager.transferCalls)
	}
}

func TestPlanExecutorCanonicalEndpointsFailBeforeEveryManagerPath(t *testing.T) {
	invalid := []string{
		"not-a-socket", "Target:443", " target:443", "target:0", "target:01", "target:65536",
		"target..internal:443", "999.0.0.1:443", "127.0.0.01:443", "h\u00f6lder:443",
	}
	for _, operation := range []OperationKind{OperationPlannedDrain, OperationCrashTakeover} {
		for _, field := range []string{"logic", "control", "swapped", "missing"} {
			values := invalid
			if field == "swapped" || field == "missing" {
				values = []string{""}
			}
			for _, endpoint := range values {
				t.Run(string(operation)+"_"+field+"_"+endpoint, func(t *testing.T) {
					fixture := newExecutorFixture(t)
					if operation == OperationCrashTakeover {
						fixture.authority.value.LeaseUntilMs = fixture.now.Add(-time.Second).UnixMilli()
					}
					var record PlanRecord
					var err error
					if operation == OperationCrashTakeover {
						record, err = fixture.executor.SubmitCrashTakeover(context.Background(), fixture.takeoverRequest())
					} else {
						record, err = fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
					}
					if err != nil {
						t.Fatal(err)
					}
					mutate := func() {
						target := fixture.holders.records["holder-b"]
						switch field {
						case "logic":
							target.LogicEndpoint = endpoint
						case "control":
							target.HolderControlEndpoint = endpoint
						case "swapped":
							target.LogicEndpoint, target.HolderControlEndpoint = target.HolderControlEndpoint, target.LogicEndpoint
						case "missing":
							target.HolderControlEndpoint = ""
						}
						fixture.holders.records["holder-b"] = target
					}
					if operation == OperationPlannedDrain {
						fixture.executor.drainFactory = func(context.Context, Plan) (authority.DrainController, func() error, error) {
							mutate()
							return &executorFakeDrainController{}, nil, nil
						}
					} else {
						mutate()
					}
					result, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
					if !errors.Is(err, ErrPlanConflict) || result.Plan.Phase != PhasePaused {
						t.Fatalf("result=%+v err=%v", result.Plan, err)
					}
					if fixture.manager.drainCalls != 0 || fixture.manager.transferCalls != 0 {
						t.Fatalf("manager calls drain=%d transfer=%d", fixture.manager.drainCalls, fixture.manager.transferCalls)
					}
				})
			}
		}
	}
}

func TestPlanExecutorRevalidatesTargetAfterDrainFactsBeforeAuthority(t *testing.T) {
	fixture := newExecutorFixture(t)
	fixture.executor.drainFactory = func(context.Context, Plan) (authority.DrainController, func() error, error) {
		return &executorCallbackDrainController{afterFacts: func() {
			target := fixture.holders.records["holder-b"]
			target.CapacityWeight = 0
			fixture.holders.records["holder-b"] = target
		}}, nil, nil
	}
	record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	result, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, ErrPlanConflict) || result.Plan.Phase != PhasePaused || fixture.manager.authorityCalls != 0 {
		t.Fatalf("result=%+v err=%v authority_calls=%d", result.Plan, err, fixture.manager.authorityCalls)
	}
}

func TestPlanExecutorRecoverAcceptsTargetSnapshotAdvance(t *testing.T) {
	fixture := newExecutorFixture(t)
	fixture.executor.slidingRecoveryWindow = 1
	if _, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest()); err != nil {
		t.Fatal(err)
	}
	target := fixture.holders.records["holder-b"]
	target.SnapshotRevision++
	fixture.holders.records["holder-b"] = target
	if err := fixture.executor.Recover(context.Background()); err != nil {
		t.Fatalf("recover err=%v", err)
	}
	if fixture.manager.drainCalls != 1 || fixture.manager.transferCalls != 0 {
		t.Fatalf("manager calls drain=%d transfer=%d", fixture.manager.drainCalls, fixture.manager.transferCalls)
	}
	record, found, err := fixture.store.Load(context.Background(), "plan-1")
	if err != nil || !found || record.Plan.Phase != PhaseSucceeded {
		t.Fatalf("record=%+v found=%v err=%v", record.Plan, found, err)
	}
}

func TestPlanExecutorRecoverWaitsForSlidingWindowBeforeDrain(t *testing.T) {
	fixture := newExecutorFixture(t)
	fixture.executor.slidingRecoveryWindow = 2
	fixture.executor.legacyRecoveryGatherTimeout = 80 * time.Millisecond
	fixture.executor.legacyRecoveryGatherPollInterval = 10 * time.Millisecond
	started := make(chan struct{})
	fixture.executor.drainFactory = func(context.Context, Plan) (authority.DrainController, func() error, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		return &executorFakeDrainController{}, func() error { return nil }, nil
	}
	if _, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest()); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- fixture.executor.Recover(context.Background()) }()
	select {
	case <-started:
		t.Fatal("Recover started Drain before the sliding gather elapsed")
	case <-time.After(40 * time.Millisecond):
	}
	if err := <-done; err != nil {
		t.Fatalf("recover err=%v", err)
	}
	if fixture.manager.drainCalls != 1 {
		t.Fatalf("drain calls=%d after gather timeout", fixture.manager.drainCalls)
	}
}

func TestPlanExecutorRequiresStrictSideEffectTTLBudget(t *testing.T) {
	fixture := newExecutorFixture(t)
	executor, err := NewPlanExecutor(PlanExecutorOptions{
		Store: fixture.store, Manager: fixture.manager, AuthorityReader: fixture.authority,
		SnapshotStore: fixture.snapshot, HolderReader: fixture.holders,
		DrainControllerFactory: func(context.Context, Plan) (authority.DrainController, func() error, error) {
			return &executorFakeDrainController{}, nil, nil
		},
		Now: func() time.Time { return fixture.now }, OperationTimeout: time.Second,
		PlanTTL:          time.Second + executorTTLSafety + time.Nanosecond,
		SingleController: SingleControllerAttestation{Contract: SingleControllerContract, ControllerID: "controller-a", BootID: "boot-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	record, err := executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, errPlanTTLInsufficient) || result.Plan.Phase != PhasePaused || result.Plan.ErrorCode != "PLAN_TTL_INSUFFICIENT" {
		t.Fatalf("result=%+v err=%v", result.Plan, err)
	}
	if fixture.manager.drainCalls != 0 || fixture.manager.transferCalls != 0 {
		t.Fatalf("manager calls drain=%d transfer=%d", fixture.manager.drainCalls, fixture.manager.transferCalls)
	}
}

func TestPlanExecutorRecoveryPersistsExpiredPlanWithoutManager(t *testing.T) {
	fixture := newExecutorFixture(t)
	clock := fixture.now
	fixture.executor.now = func() time.Time { return clock }
	fixture.executor.slidingRecoveryWindow = 1
	fixture.store.options.Now = func() time.Time { return clock }
	if _, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest()); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(25 * time.Hour)
	if err := fixture.executor.Recover(context.Background()); err != nil {
		t.Fatalf("recover err=%v", err)
	}
	record, found, err := fixture.store.Load(context.Background(), "plan-1")
	if err != nil || !found || record.Plan.Phase != PhaseFailed || record.Plan.ErrorCode != "PLAN_EXPIRED" {
		t.Fatalf("record=%+v found=%v err=%v", record.Plan, found, err)
	}
	if fixture.manager.drainCalls != 0 || fixture.manager.transferCalls != 0 {
		t.Fatalf("manager calls drain=%d transfer=%d", fixture.manager.drainCalls, fixture.manager.transferCalls)
	}
}

func TestPlanExecutorExpiredRecoveryReconcilesCommittedAuthorityBeforeFailure(t *testing.T) {
	for _, snapshotReady := range []bool{false, true} {
		t.Run(map[bool]string{false: "snapshot pending", true: "snapshot complete"}[snapshotReady], func(t *testing.T) {
			fixture := newExecutorFixture(t)
			clock := fixture.now
			fixture.executor.now = func() time.Time { return clock }
			fixture.executor.slidingRecoveryWindow = 1
			fixture.store.options.Now = func() time.Time { return clock }
			record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
			if err != nil {
				t.Fatal(err)
			}
			fixture.authority.value = fixture.target
			if snapshotReady {
				recovered := targetSnapshot(fixture.manager.snapshot, fixture.target)
				if _, err := fixture.snapshot.CompareAndSwap(context.Background(), core.NestedControlOperationToken{}, 1, recovered); err != nil {
					t.Fatal(err)
				}
			}
			clock = clock.Add(25 * time.Hour)
			if err := fixture.executor.Recover(context.Background()); err != nil {
				t.Fatalf("recover err=%v", err)
			}
			got, found, err := fixture.store.Load(context.Background(), record.Plan.PlanID)
			if err != nil || !found {
				t.Fatalf("record=%+v found=%v err=%v", got.Plan, found, err)
			}
			if snapshotReady {
				if got.Plan.Phase != PhaseSucceeded {
					t.Fatalf("complete plan=%+v", got.Plan)
				}
			} else if got.Plan.Phase != PhasePaused || got.Plan.ErrorCode != ErrorAuthorityCommittedSnapshotPending {
				t.Fatalf("pending plan=%+v", got.Plan)
			}
			if fixture.manager.drainCalls != 0 || fixture.manager.transferCalls != 0 {
				t.Fatalf("manager calls drain=%d transfer=%d", fixture.manager.drainCalls, fixture.manager.transferCalls)
			}
		})
	}
}

func TestPlanExecutorPostManagerSnapshotPendingReconcilesWithoutReplay(t *testing.T) {
	fixture := newExecutorFixture(t)
	fixture.manager.skipSnapshot = true
	record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	paused, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, ErrVerification) || paused.Plan.Phase != PhasePaused || paused.Plan.ErrorCode != ErrorAuthorityCommittedSnapshotPending {
		t.Fatalf("paused=%+v err=%v", paused.Plan, err)
	}
	if fixture.manager.drainCalls != 1 || fixture.manager.transferCalls != 0 {
		t.Fatalf("manager calls drain=%d transfer=%d", fixture.manager.drainCalls, fixture.manager.transferCalls)
	}
	recoveredSnapshot := targetSnapshot(fixture.manager.snapshot, fixture.target)
	if _, err := fixture.snapshot.CompareAndSwap(context.Background(), core.NestedControlOperationToken{}, 1, recoveredSnapshot); err != nil {
		t.Fatal(err)
	}
	reconciled, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || reconciled.Plan.Revision != paused.Plan.Revision || reconciled.Plan.Phase != PhasePaused {
		t.Fatalf("reconciled=%+v err=%v", reconciled.Plan, err)
	}
	if fixture.manager.drainCalls != 1 || fixture.manager.transferCalls != 0 {
		t.Fatalf("manager replayed drain=%d transfer=%d", fixture.manager.drainCalls, fixture.manager.transferCalls)
	}
}

func TestPlanExecutorOwnedCanceledChildFinishesFromCommittedShardFacts(t *testing.T) {
	fixture := newExecutorFixture(t)
	record := fixture.persistOwnedMove(t, fixture.ownedMoveRequest())
	paused, err := fixture.executor.pausePlan(record, "CANCELED", context.Canceled)
	if !errors.Is(err, context.Canceled) || paused.Plan.Phase != PhasePaused {
		t.Fatalf("paused=%+v err=%v", paused.Plan, err)
	}
	fixture.authority.value = fixture.target
	recoveredSnapshot := targetSnapshot(fixture.manager.snapshot, fixture.target)
	if _, err := fixture.snapshot.CompareAndSwap(context.Background(), core.NestedControlOperationToken{}, 1, recoveredSnapshot); err != nil {
		t.Fatal(err)
	}

	reconciled, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || reconciled.Plan.Phase != PhaseSucceeded || reconciled.Plan.ErrorCode != "" {
		t.Fatalf("reconciled=%+v err=%v", reconciled.Plan, err)
	}
	if fixture.manager.drainCalls != 0 || fixture.manager.transferCalls != 0 {
		t.Fatalf("manager replayed drain=%d transfer=%d", fixture.manager.drainCalls, fixture.manager.transferCalls)
	}
}

func TestPlanExecutorCommitUnknownPausedRecordFactuallyReconciles(t *testing.T) {
	fixture := newExecutorFixture(t)
	fixture.manager.resultErr = authority.ErrAuthorityCommitUnknown
	record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	paused, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, authority.ErrAuthorityCommitUnknown) || paused.Plan.Phase != PhasePaused || paused.Plan.ErrorCode != "AUTHORITY_COMMIT_UNKNOWN" {
		t.Fatalf("paused=%+v err=%v", paused.Plan, err)
	}
	reconciled, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || reconciled.Plan.Revision != paused.Plan.Revision {
		t.Fatalf("reconciled=%+v err=%v", reconciled.Plan, err)
	}
	if fixture.manager.drainCalls != 1 || fixture.manager.transferCalls != 0 {
		t.Fatalf("manager replayed drain=%d transfer=%d", fixture.manager.drainCalls, fixture.manager.transferCalls)
	}
	fixture.executor.verification = executorVerificationFake{}
	target := fixture.holders.records["holder-b"]
	target.State = core.HolderStateServing
	target.SnapshotRevision = 2
	fixture.holders.records["holder-b"] = target
	report, err := fixture.executor.Verify(context.Background(), record.Plan.PlanID)
	if err != nil || !report.Consistent {
		t.Fatalf("paused factual verify=%+v err=%v", report.Checks, err)
	}
}

func TestPlanExecutorRequiresExplicitSingleControllerAttestation(t *testing.T) {
	fixture := newExecutorFixture(t)
	base := PlanExecutorOptions{
		Store: fixture.store, Manager: fixture.manager, AuthorityReader: fixture.authority,
		SnapshotStore: fixture.snapshot, HolderReader: fixture.holders,
		SingleController: SingleControllerAttestation{Contract: SingleControllerContract, ControllerID: "controller-a", BootID: "boot-a"},
	}
	for _, invalid := range []SingleControllerAttestation{
		{},
		{Contract: "leader-election", ControllerID: "controller-a", BootID: "boot-a"},
		{Contract: SingleControllerContract, ControllerID: "", BootID: "boot-a"},
		{Contract: SingleControllerContract, ControllerID: "controller-a", BootID: ""},
	} {
		candidate := base
		candidate.SingleController = invalid
		if _, err := NewPlanExecutor(candidate); !errors.Is(err, ErrExecutorInvalid) {
			t.Fatalf("attestation=%+v err=%v", invalid, err)
		}
	}
}

func TestPlanExecutorRecoveryGuardFailurePausesBeforeManager(t *testing.T) {
	fixture := newExecutorFixture(t)
	record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	guard := &executorGuardFake{err: errors.New("manager projection recovery unavailable")}
	fixture.executor.executionGuard = guard
	result, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
	if err == nil || result.Plan.Phase != PhasePaused || result.Plan.ErrorCode != "EXECUTION_FAILED" {
		t.Fatalf("result=%+v err=%v", result.Plan, err)
	}
	if guard.entries != 1 || fixture.manager.drainCalls != 0 || fixture.manager.transferCalls != 0 {
		t.Fatalf("guard entries=%d manager drain=%d transfer=%d", guard.entries, fixture.manager.drainCalls, fixture.manager.transferCalls)
	}
}

func TestPlanExecutorRecoveryGuardFailurePreservesCommittedSnapshotPendingReason(t *testing.T) {
	for _, operation := range []OperationKind{OperationPlannedDrain, OperationCrashTakeover} {
		t.Run(string(operation), func(t *testing.T) {
			fixture := newExecutorFixture(t)
			var record PlanRecord
			var err error
			if operation == OperationCrashTakeover {
				fixture.authority.value.LeaseUntilMs = fixture.now.Add(-time.Second).UnixMilli()
				fixture.holders.records["holder-a"] = core.HolderRecord{
					Domain: actorrouting.DomainPlayer, HolderID: "holder-a", BootID: "boot-a", LogicEndpoint: "source:443",
					State: core.HolderStateDraining, Draining: true, LeaseUntil: fixture.now.Add(-time.Second),
				}
				record, err = fixture.executor.SubmitCrashTakeover(context.Background(), fixture.takeoverRequest())
			} else {
				record, err = fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
			}
			if err != nil {
				t.Fatal(err)
			}
			fixture.authority.value = fixture.target
			guard := &executorGuardFake{err: errors.New("manager projection recovery unavailable")}
			fixture.executor.executionGuard = guard
			result, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID)
			if err == nil || result.Plan.Phase != PhasePaused || result.Plan.ErrorCode != ErrorAuthorityCommittedSnapshotPending {
				t.Fatalf("result=%+v err=%v", result.Plan, err)
			}
			if guard.entries != 1 || fixture.manager.drainCalls != 0 || fixture.manager.transferCalls != 0 {
				t.Fatalf("guard entries=%d manager drain=%d transfer=%d", guard.entries, fixture.manager.drainCalls, fixture.manager.transferCalls)
			}
		})
	}
}

func TestPlanExecutorVerifyFailsClosedAndRequiresEveryCheck(t *testing.T) {
	fixture := newExecutorFixture(t)
	record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID); err != nil {
		t.Fatal(err)
	}
	report, err := fixture.executor.Verify(context.Background(), record.Plan.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Consistent {
		t.Fatal("missing verification port unexpectedly passed")
	}
	names := make(map[string]int)
	for _, check := range report.Checks {
		names[check.Name]++
	}
	for _, name := range []string{"gate_resolution", "transition", "recovery", "tombstone", "plan_state", "final_freshness"} {
		if names[name] != 1 {
			t.Fatalf("check %q count=%d all=%+v", name, names[name], report.Checks)
		}
	}
}

func TestPlanExecutorVerifyPassRequiresAllFacts(t *testing.T) {
	fixture := newExecutorFixture(t)
	fixture.executor.verification = executorVerificationFake{}
	record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID); err != nil {
		t.Fatal(err)
	}
	target := fixture.holders.records["holder-b"]
	target.State = core.HolderStateServing
	target.SnapshotRevision = 2
	fixture.holders.records["holder-b"] = target
	report, err := fixture.executor.Verify(context.Background(), record.Plan.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Consistent {
		t.Fatalf("report=%+v", report.Checks)
	}
	names := make(map[string]bool)
	for _, check := range report.Checks {
		if !check.OK || names[check.Name] {
			t.Fatalf("check=%+v names=%v", check, names)
		}
		names[check.Name] = true
	}
	for _, name := range []string{"authority_active_lease", "authority_tuple", "snapshot_read", "snapshot_tuple", "target_holder", "gate_resolution", "transition", "recovery", "tombstone", "plan_state", "final_freshness"} {
		if !names[name] {
			t.Fatalf("missing check %q: %+v", name, report.Checks)
		}
	}
}

func TestPlanExecutorVerifyAcceptsOnlyMonotonicAuthorityDeadlineRenewal(t *testing.T) {
	fixture := newExecutorFixture(t)
	fixture.executor.verification = executorVerificationFake{}
	record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID); err != nil {
		t.Fatal(err)
	}
	target := fixture.holders.records["holder-b"]
	target.State = core.HolderStateServing
	target.SnapshotRevision = 2
	fixture.holders.records["holder-b"] = target
	fixture.authority.value.LeaseUntilMs += time.Minute.Milliseconds()
	report, err := fixture.executor.Verify(context.Background(), record.Plan.PlanID)
	if err != nil || !report.Consistent {
		t.Fatalf("monotonic deadline renewal report=%+v err=%v", report.Checks, err)
	}
	fixture.authority.value.WriterEpoch++
	report, err = fixture.executor.Verify(context.Background(), record.Plan.PlanID)
	if err != nil || report.Consistent {
		t.Fatalf("identity drift passed report=%+v err=%v", report.Checks, err)
	}
}

func TestPlanExecutorFinalFreshnessRejectsLeaseExpiringAfterGateEvidence(t *testing.T) {
	fixture := newExecutorFixture(t)
	clock := fixture.now
	fixture.executor.now = func() time.Time { return clock }
	record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.executor.Execute(context.Background(), record.Plan.PlanID); err != nil {
		t.Fatal(err)
	}
	target := fixture.holders.records["holder-b"]
	target.State = core.HolderStateServing
	target.SnapshotRevision = 2
	fixture.holders.records["holder-b"] = target
	deadline := time.UnixMilli(fixture.authority.value.LeaseUntilMs)
	fixture.executor.verification = executorVerificationBarrier{afterGate: func() { clock = deadline }}
	report, err := fixture.executor.Verify(context.Background(), record.Plan.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	if report.Consistent {
		t.Fatalf("lease expiring after Gate evidence passed: %+v", report.Checks)
	}
	found := false
	for _, check := range report.Checks {
		if check.Name == "final_freshness" {
			found = true
			if check.OK {
				t.Fatalf("final freshness passed at deadline: %+v", check)
			}
		}
	}
	if !found {
		t.Fatal("final freshness check missing")
	}
}

func TestPlanExecutorCancellationPausesPlan(t *testing.T) {
	fixture := newExecutorFixture(t)
	fixture.executor.drainFactory = func(ctx context.Context, _ Plan) (authority.DrainController, func() error, error) {
		return &executorFakeDrainController{}, nil, nil
	}
	fixture.manager.block = true
	record, err := fixture.executor.SubmitMove(context.Background(), fixture.moveRequest())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result, err := fixture.executor.Execute(ctx, record.Plan.PlanID)
	if !errors.Is(err, context.DeadlineExceeded) || result.Plan.Phase != PhasePaused {
		t.Fatalf("result=%+v err=%v", result.Plan, err)
	}
}

func targetSnapshot(snapshot actorrouting.RouteSnapshot, target authoritystore.ShardWriteAuthority) actorrouting.RouteSnapshot {
	result := actorrouting.CloneRouteSnapshot(snapshot)
	result.GlobalRevision++
	result.Holders = append(result.Holders, actorrouting.HolderRef{InstanceID: target.HolderID, BootID: target.HolderBootID, Endpoint: "target:443", State: actorrouting.HolderStateActive, Weight: 1})
	route := result.Player.Routes[0]
	route.HolderIndex = len(result.Holders) - 1
	route.ShardRevision = target.AssignmentRevision
	route.ExpectedWriterEpoch = target.WriterEpoch
	route.ExpectedLeaseEpoch = target.LeaseEpoch
	route.LeaseUntilMs = target.LeaseUntilMs
	route.State = actorrouting.ShardStateActive
	result.Player.Routes[0] = route
	return result
}

type executorFakeManager struct {
	mu              sync.Mutex
	snapshot        actorrouting.RouteSnapshot
	authorityReader *executorFakeAuthorityReader
	snapshotStore   *core.MemoryRouteSnapshotStore
	target          authoritystore.ShardWriteAuthority
	drainCalls      int
	transferCalls   int
	controllerOrder []string
	block           bool
	skipSnapshot    bool
	resultErr       error
	authorityCalls  int
	beforeDrain     func()
}

func (m *executorFakeManager) CurrentSnapshot() actorrouting.RouteSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return actorrouting.CloneRouteSnapshot(m.snapshot)
}

func (m *executorFakeManager) DrainValidated(ctx context.Context, _ authority.TransferRequest, controller authority.DrainController, _ authority.PendingProjectionTargetValidator) (authoritystore.ShardAuthorityTransitionResult, error) {
	m.mu.Lock()
	m.drainCalls++
	beforeDrain := m.beforeDrain
	m.beforeDrain = nil
	m.mu.Unlock()
	if beforeDrain != nil {
		beforeDrain()
	}
	m.mu.Lock()
	m.controllerOrder = append(m.controllerOrder, "stop")
	m.mu.Unlock()
	if err := controller.StopNewActivationAndTurns(ctx, actorrouting.DomainPlayer, 0); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	m.mu.Lock()
	m.controllerOrder = append(m.controllerOrder, "wait")
	m.mu.Unlock()
	if err := controller.WaitCurrentTurns(ctx, actorrouting.DomainPlayer, 0); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	m.mu.Lock()
	m.controllerOrder = append(m.controllerOrder, "facts")
	m.mu.Unlock()
	if err := controller.ConfirmFactsCommitted(ctx, actorrouting.DomainPlayer, 0); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	if m.block {
		<-ctx.Done()
		return authoritystore.ShardAuthorityTransitionResult{}, ctx.Err()
	}
	m.mu.Lock()
	m.controllerOrder = append(m.controllerOrder, "cold-load")
	m.mu.Unlock()
	if err := controller.ColdLoadNewHolder(ctx, actorrouting.DomainPlayer, 0, m.target); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	m.mu.Lock()
	m.authorityCalls++
	m.mu.Unlock()
	m.commitTarget(ctx)
	return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionApplied, Authority: m.target}, m.resultErr
}

func (m *executorFakeManager) HandoffWorkerValidated(
	ctx context.Context,
	_ authority.TransferRequest,
	sourceValidator authority.WorkerHandoffSourceValidator,
	_ authority.PendingProjectionTargetValidator,
) (authoritystore.ShardAuthorityTransitionResult, error) {
	m.mu.Lock()
	m.drainCalls++
	m.mu.Unlock()
	if err := sourceValidator(ctx, authority.WorkerHandoffSourceObservation{
		Domain:                actorrouting.DomainFarmWorker,
		VirtualShard:          0,
		SourceHolderID:        "source",
		SourceHolderBootID:    "source-boot",
		SnapshotRevision:      m.snapshot.GlobalRevision,
		ExpectedServingShards: 0,
	}); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	m.commitTarget(ctx)
	return authoritystore.ShardAuthorityTransitionResult{
		Status:    authoritystore.ShardTransitionApplied,
		Authority: m.target,
	}, m.resultErr
}

func (m *executorFakeManager) TransferValidated(ctx context.Context, _ authority.TransferRequest, _ authority.PendingProjectionTargetValidator) (authoritystore.ShardAuthorityTransitionResult, error) {
	m.mu.Lock()
	m.transferCalls++
	block := m.block
	m.mu.Unlock()
	if block {
		<-ctx.Done()
		return authoritystore.ShardAuthorityTransitionResult{}, ctx.Err()
	}
	m.mu.Lock()
	m.authorityCalls++
	m.mu.Unlock()
	m.commitTarget(ctx)
	return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionApplied, Authority: m.target}, m.resultErr
}

func (m *executorFakeManager) commitTarget(ctx context.Context) {
	m.authorityReader.value = m.target
	if m.skipSnapshot {
		return
	}
	m.snapshot = targetSnapshot(m.snapshot, m.target)
	current, found, _ := m.snapshotStore.LoadCurrent(ctx)
	if found {
		_, _ = m.snapshotStore.CompareAndSwap(ctx, core.NestedControlOperationToken{}, current.Revision, m.snapshot)
	}
}

type executorFakeAuthorityReader struct {
	mu    sync.Mutex
	value authoritystore.ShardWriteAuthority
	found bool
	err   error
}

func (r *executorFakeAuthorityReader) ReadShardWriteAuthority(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.value, r.found, r.err
}

type executorFakeHolderReader struct {
	records map[string]core.HolderRecord
	getHook func(
		context.Context,
		actorrouting.RouteDomain,
		string,
	) (core.HolderRecord, bool, error)
	cachedHook func(
		context.Context,
		actorrouting.RouteDomain,
		string,
	) (core.HolderRecord, bool, error)
}

func (r *executorFakeHolderReader) Get(ctx context.Context, domain actorrouting.RouteDomain, id string) (core.HolderRecord, bool, error) {
	if r.getHook != nil {
		return r.getHook(ctx, domain, id)
	}
	value, ok := r.records[id]
	return value, ok, nil
}
func (r *executorFakeHolderReader) GetCached(ctx context.Context, domain actorrouting.RouteDomain, id string) (core.HolderRecord, bool, error) {
	if r.cachedHook != nil {
		return r.cachedHook(ctx, domain, id)
	}
	value, ok := r.records[id]
	return value, ok, nil
}
func (r *executorFakeHolderReader) List(_ context.Context, domain actorrouting.RouteDomain) ([]core.HolderRecord, error) {
	result := make([]core.HolderRecord, 0)
	for _, value := range r.records {
		if value.Domain == domain {
			result = append(result, value)
		}
	}
	return result, nil
}

type executorFakeDrainController struct{}

func (*executorFakeDrainController) StopNewActivationAndTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	return nil
}
func (*executorFakeDrainController) WaitCurrentTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	return nil
}
func (*executorFakeDrainController) ConfirmFactsCommitted(context.Context, actorrouting.RouteDomain, uint32) error {
	return nil
}
func (*executorFakeDrainController) ColdLoadNewHolder(context.Context, actorrouting.RouteDomain, uint32, authoritystore.ShardWriteAuthority) error {
	return nil
}

type executorCallbackDrainController struct{ afterFacts func() }

func (*executorCallbackDrainController) StopNewActivationAndTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	return nil
}
func (*executorCallbackDrainController) WaitCurrentTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	return nil
}
func (c *executorCallbackDrainController) ConfirmFactsCommitted(context.Context, actorrouting.RouteDomain, uint32) error {
	if c.afterFacts != nil {
		c.afterFacts()
	}
	return nil
}
func (*executorCallbackDrainController) ColdLoadNewHolder(context.Context, actorrouting.RouteDomain, uint32, authoritystore.ShardWriteAuthority) error {
	return nil
}

func stringsJoin(values []string) string { return strings.Join(values, ",") }

type executorVerificationFake struct{}

func (executorVerificationFake) VerifyGateResolution(context.Context, Plan, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot) error {
	return nil
}
func (executorVerificationFake) VerifyTransition(context.Context, Plan, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot) error {
	return nil
}
func (executorVerificationFake) VerifyRecovery(context.Context, Plan, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot) error {
	return nil
}
func (executorVerificationFake) VerifyTombstone(context.Context, Plan, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot) error {
	return nil
}

type executorVerificationBarrier struct{ afterGate func() }

func (v executorVerificationBarrier) VerifyGateResolution(context.Context, Plan, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot) error {
	if v.afterGate != nil {
		v.afterGate()
	}
	return nil
}
func (executorVerificationBarrier) VerifyTransition(context.Context, Plan, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot) error {
	return nil
}
func (executorVerificationBarrier) VerifyRecovery(context.Context, Plan, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot) error {
	return nil
}
func (executorVerificationBarrier) VerifyTombstone(context.Context, Plan, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot) error {
	return nil
}

type executorGuardFake struct {
	entries  int
	releases int
	err      error
}

func (g *executorGuardFake) Enter(context.Context) (func(), error) {
	g.entries++
	if g.err != nil {
		return nil, g.err
	}
	return func() { g.releases++ }, nil
}
