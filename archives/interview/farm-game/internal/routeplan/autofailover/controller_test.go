package autofailover

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/routecontrol/verifiedholder"
	"farmgame/internal/routeplan"
	"farmgame/internal/storage/authoritystore"
)

type fakeSnapshots struct{ value actorrouting.RouteSnapshot }

func (f *fakeSnapshots) CurrentSnapshot() actorrouting.RouteSnapshot {
	return actorrouting.CloneRouteSnapshot(f.value)
}

type fakeAuthorities struct {
	values map[uint32]authoritystore.ShardWriteAuthority
	reads  int
}

func (f *fakeAuthorities) ReadShardWriteAuthority(_ context.Context, _ actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool, error) {
	f.reads++
	value, ok := f.values[shard]
	return value, ok, nil
}

type fakeHolders struct{ values []verifiedholder.Holder }

func (f *fakeHolders) ListVerifiedDomain(actorrouting.RouteDomain) []verifiedholder.Holder {
	return append([]verifiedholder.Holder(nil), f.values...)
}

type fakeExecutor struct {
	mu       sync.Mutex
	requests []routeplan.TakeoverRequest
}

func (*fakeExecutor) BeginForegroundBatch(context.Context) (func(), error) { return func() {}, nil }

func (f *fakeExecutor) SubmitCrashTakeover(_ context.Context, request routeplan.TakeoverRequest) (routeplan.PlanRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, request)
	return routeplan.PlanRecord{Plan: routeplan.Plan{PlanID: request.PlanID}}, nil
}
func (f *fakeExecutor) Execute(context.Context, string) (routeplan.PlanRecord, error) {
	return routeplan.PlanRecord{}, nil
}

type concurrentExecutor struct {
	started   chan string
	release   <-chan struct{}
	mu        sync.Mutex
	submitted int
	inflight  int
	max       int
	batchHeld bool
}

func (f *concurrentExecutor) BeginForegroundBatch(context.Context) (func(), error) {
	f.mu.Lock()
	f.batchHeld = true
	f.mu.Unlock()
	return func() {
		f.mu.Lock()
		f.batchHeld = false
		f.mu.Unlock()
	}, nil
}

func (f *concurrentExecutor) SubmitCrashTakeover(_ context.Context, request routeplan.TakeoverRequest) (routeplan.PlanRecord, error) {
	f.mu.Lock()
	if !f.batchHeld {
		f.mu.Unlock()
		return routeplan.PlanRecord{}, errors.New("takeover intent persisted outside foreground batch")
	}
	f.submitted++
	f.mu.Unlock()
	return routeplan.PlanRecord{Plan: routeplan.Plan{PlanID: request.PlanID}}, nil
}

func (f *concurrentExecutor) Execute(_ context.Context, planID string) (routeplan.PlanRecord, error) {
	f.mu.Lock()
	f.inflight++
	if f.inflight > f.max {
		f.max = f.inflight
	}
	f.mu.Unlock()
	f.started <- planID
	<-f.release
	f.mu.Lock()
	f.inflight--
	f.mu.Unlock()
	return routeplan.PlanRecord{Plan: routeplan.Plan{PlanID: planID}}, nil
}

type deadlineExecutor struct {
	submitDeadline  time.Time
	executeDeadline time.Time
}

func (*deadlineExecutor) BeginForegroundBatch(context.Context) (func(), error) { return func() {}, nil }

func (f *deadlineExecutor) SubmitCrashTakeover(ctx context.Context, request routeplan.TakeoverRequest) (routeplan.PlanRecord, error) {
	f.submitDeadline, _ = ctx.Deadline()
	time.Sleep(2 * time.Millisecond)
	return routeplan.PlanRecord{Plan: routeplan.Plan{PlanID: request.PlanID}}, nil
}

func (f *deadlineExecutor) Execute(ctx context.Context, planID string) (routeplan.PlanRecord, error) {
	f.executeDeadline, _ = ctx.Deadline()
	return routeplan.PlanRecord{Plan: routeplan.Plan{PlanID: planID}}, nil
}

func TestActiveMissingHolderWaitsWithoutAuthorityFanout(t *testing.T) {
	now := time.Unix(100, 0)
	snapshot, authorityValue := failoverFixture(now.Add(time.Minute))
	authorities := &fakeAuthorities{values: map[uint32]authoritystore.ShardWriteAuthority{0: authorityValue}}
	executor := &fakeExecutor{}
	controller := newTestController(t, now, snapshot, authorities, nil, executor)
	result := controller.Reconcile(context.Background())
	if result.WaitingLease != 1 || result.AuthorityReads != 0 || authorities.reads != 0 || len(executor.requests) != 0 {
		t.Fatalf("result=%+v reads=%d requests=%d", result, authorities.reads, len(executor.requests))
	}
}

func TestVerifiedReplacementBootTakesOverBeforeOldLeaseExpiry(t *testing.T) {
	now := time.Unix(100, 0)
	snapshot, authorityValue := failoverFixture(now.Add(time.Minute))
	authorities := &fakeAuthorities{values: map[uint32]authoritystore.ShardWriteAuthority{0: authorityValue}}
	holders := []verifiedholder.Holder{healthyHolder(now, "source", "boot-new", 0)}
	executor := &fakeExecutor{}
	controller := newTestController(t, now, snapshot, authorities, holders, executor)
	result := controller.Reconcile(context.Background())
	if result.Err != nil || result.PlansCompleted != 1 || len(executor.requests) != 1 {
		t.Fatalf("result=%+v requests=%d", result, len(executor.requests))
	}
	request := executor.requests[0]
	if !request.Proof.HolderUnavailable || request.Proof.LeaseExpired || request.TargetBootID != "boot-new" {
		t.Fatalf("unexpected request: %+v", request)
	}
	if !request.TargetDeploymentIdentity.Exact() || request.TargetDeploymentIdentity.PodUID != "source-pod" || request.TargetDeploymentIdentity.WorkloadUID != "workload" {
		t.Fatalf("crash target identity was not pinned: %+v", request.TargetDeploymentIdentity)
	}
}

func TestVerifiedWarmingReplacementBootCanColdLoadTakeover(t *testing.T) {
	now := time.Unix(100, 0)
	snapshot, authorityValue := failoverFixture(now.Add(time.Minute))
	authorities := &fakeAuthorities{values: map[uint32]authoritystore.ShardWriteAuthority{0: authorityValue}}
	warming := healthyHolder(now, "source", "boot-new", 0)
	warming.Lifecycle = verifiedholder.LifecycleWarming
	warming.ProcessReady = false
	executor := &fakeExecutor{}
	controller := newTestController(t, now, snapshot, authorities, []verifiedholder.Holder{warming}, executor)
	result := controller.Reconcile(context.Background())
	if result.Err != nil || result.PlansCompleted != 1 || len(executor.requests) != 1 {
		t.Fatalf("result=%+v requests=%d", result, len(executor.requests))
	}
	request := executor.requests[0]
	if !request.Proof.HolderUnavailable || request.TargetHolderID != "source" || request.TargetBootID != "boot-new" {
		t.Fatalf("unexpected warming takeover request: %+v", request)
	}
}

func TestCrashTakeoverPreparesExactTargetBeforeSubmit(t *testing.T) {
	now := time.Unix(100, 0)
	snapshot, authorityValue := failoverFixture(now.Add(-time.Second))
	authorities := &fakeAuthorities{values: map[uint32]authoritystore.ShardWriteAuthority{0: authorityValue}}
	target := healthyHolder(now, "source", "boot-new", 0)
	target.Lifecycle = verifiedholder.LifecycleWarming
	target.ProcessReady = false
	executor := &fakeExecutor{}
	prepared := false
	controller, err := New(Options{
		Snapshots: &fakeSnapshots{value: snapshot}, Authorities: authorities,
		Holders: &fakeHolders{values: []verifiedholder.Holder{target}}, Executor: executor,
		PrepareTarget: func(_ context.Context, actual verifiedholder.Holder) error {
			if actual.HolderID != target.HolderID || actual.BootID != target.BootID {
				t.Fatalf("prepared target=%+v, want %+v", actual, target)
			}
			prepared = true
			return nil
		},
		Domains: []actorrouting.RouteDomain{actorrouting.DomainFarm}, Interval: time.Second,
		StepTimeout: time.Second, PlanTTL: time.Hour, MaxPlansPerPass: 1, MaxAuthorityReads: 4,
		Ready: func() bool { return true }, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if result.Err != nil || !prepared || len(executor.requests) != 1 {
		t.Fatalf("result=%+v prepared=%t requests=%d", result, prepared, len(executor.requests))
	}
}

func TestCrashTakeoverDoesNotSubmitWhenTargetPreparationFails(t *testing.T) {
	now := time.Unix(100, 0)
	snapshot, authorityValue := failoverFixture(now.Add(-time.Second))
	authorities := &fakeAuthorities{values: map[uint32]authoritystore.ShardWriteAuthority{0: authorityValue}}
	executor := &fakeExecutor{}
	want := context.DeadlineExceeded
	controller, err := New(Options{
		Snapshots: &fakeSnapshots{value: snapshot}, Authorities: authorities,
		Holders: &fakeHolders{values: []verifiedholder.Holder{healthyHolder(now, "target", "target-boot", 1)}}, Executor: executor,
		PrepareTarget: func(context.Context, verifiedholder.Holder) error { return want },
		Domains:       []actorrouting.RouteDomain{actorrouting.DomainFarm}, Interval: time.Second,
		StepTimeout: time.Second, PlanTTL: time.Hour, MaxPlansPerPass: 1, MaxAuthorityReads: 4,
		Ready: func() bool { return true }, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if !errors.Is(result.Err, want) || len(executor.requests) != 0 {
		t.Fatalf("result=%+v requests=%d", result, len(executor.requests))
	}
}

func TestExpiredLeaseUsesFencedExpiredTakeover(t *testing.T) {
	now := time.Unix(100, 0)
	snapshot, authorityValue := failoverFixture(now.Add(-time.Second))
	authorities := &fakeAuthorities{values: map[uint32]authoritystore.ShardWriteAuthority{0: authorityValue}}
	holders := []verifiedholder.Holder{healthyHolder(now, "target", "target-boot", 1)}
	executor := &fakeExecutor{}
	controller := newTestController(t, now, snapshot, authorities, holders, executor)
	result := controller.Reconcile(context.Background())
	if result.Err != nil || result.PlansCompleted != 1 || len(executor.requests) != 1 {
		t.Fatalf("result=%+v requests=%d", result, len(executor.requests))
	}
	request := executor.requests[0]
	if !request.Proof.LeaseExpired || request.Proof.HolderUnavailable {
		t.Fatalf("unexpected proof: %+v", request.Proof)
	}
	firstID := request.PlanID
	controller = newTestController(t, now, snapshot, authorities, holders, &fakeExecutor{})
	item, _, err := controller.findCandidate(context.Background(), 4)
	if err != nil || item == nil || item.request.PlanID != firstID {
		t.Fatalf("deterministic plan id first=%s item=%+v err=%v", firstID, item, err)
	}
}

func TestSubmitAndExecuteReceiveIndependentBoundedDeadlines(t *testing.T) {
	now := time.Now()
	snapshot, authorityValue := failoverFixture(now.Add(-time.Second))
	authorities := &fakeAuthorities{values: map[uint32]authoritystore.ShardWriteAuthority{0: authorityValue}}
	executor := &deadlineExecutor{}
	controller, err := New(Options{
		Snapshots: &fakeSnapshots{value: snapshot}, Authorities: authorities,
		Holders:  &fakeHolders{values: []verifiedholder.Holder{healthyHolder(now, "target", "target-boot", 1)}},
		Executor: executor, Domains: []actorrouting.RouteDomain{actorrouting.DomainFarm},
		Interval: time.Second, StepTimeout: 100 * time.Millisecond, PlanTTL: time.Hour,
		MaxPlansPerPass: 1, MaxAuthorityReads: 4, Ready: func() bool { return true }, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if result.Err != nil || result.PlansCompleted != 1 {
		t.Fatalf("result=%+v", result)
	}
	if executor.submitDeadline.IsZero() || !executor.executeDeadline.After(executor.submitDeadline) {
		t.Fatalf("submit deadline=%s execute deadline=%s", executor.submitDeadline, executor.executeDeadline)
	}
}

func TestCrashTakeoverExecutesIndependentShardsConcurrently(t *testing.T) {
	now := time.Unix(100, 0)
	snapshot, authorityValue := failoverFixture(now.Add(-time.Second))
	const shardCount = 4
	snapshot.Farm.ShardCount = shardCount
	snapshot.Farm.Routes = make([]actorrouting.ShardRoute, shardCount)
	authorities := &fakeAuthorities{values: make(map[uint32]authoritystore.ShardWriteAuthority, shardCount)}
	for shard := uint32(0); shard < shardCount; shard++ {
		route := actorrouting.ShardRoute{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 7, ExpectedWriterEpoch: 8, ExpectedLeaseEpoch: 9, LeaseUntilMs: now.Add(-time.Second).UnixMilli()}
		snapshot.Farm.Routes[shard] = route
		value := authorityValue
		value.VirtualShard = shard
		authorities.values[shard] = value
	}
	release := make(chan struct{})
	executor := &concurrentExecutor{started: make(chan string, shardCount), release: release}
	prepareCalls := 0
	controller, err := New(Options{
		Snapshots: &fakeSnapshots{value: snapshot}, Authorities: authorities,
		Holders: &fakeHolders{values: []verifiedholder.Holder{healthyHolder(now, "target", "target-boot", 1)}},
		PrepareTarget: func(context.Context, verifiedholder.Holder) error {
			prepareCalls++
			return nil
		},
		Executor: executor, Domains: []actorrouting.RouteDomain{actorrouting.DomainFarm},
		Interval: time.Second, StepTimeout: time.Second, PlanTTL: time.Hour,
		MaxPlansPerPass: shardCount, MaxAuthorityReads: shardCount,
		Ready: func() bool { return true }, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	resultCh := make(chan Result, 1)
	go func() { resultCh <- controller.Reconcile(context.Background()) }()
	for i := 0; i < shardCount; i++ {
		select {
		case <-executor.started:
		case <-time.After(time.Second):
			t.Fatalf("only %d/%d shard executions started concurrently", i, shardCount)
		}
	}
	close(release)
	result := <-resultCh
	if result.Err != nil || result.PlansCompleted != shardCount {
		t.Fatalf("result=%+v", result)
	}
	executor.mu.Lock()
	maxInflight := executor.max
	executor.mu.Unlock()
	if maxInflight != shardCount {
		t.Fatalf("max inflight=%d, want %d", maxInflight, shardCount)
	}
	if prepareCalls != 1 {
		t.Fatalf("same target generation prepared %d times, want 1", prepareCalls)
	}
}

func TestCrashTakeoverRefillsSlidingSlotBeforeSlowPeerCompletes(t *testing.T) {
	now := time.Unix(100, 0)
	snapshot, authorityValue := failoverFixture(now.Add(-time.Second))
	const shardCount = 3
	snapshot.Farm.ShardCount = shardCount
	snapshot.Farm.Routes = make([]actorrouting.ShardRoute, shardCount)
	authorities := &fakeAuthorities{values: make(map[uint32]authoritystore.ShardWriteAuthority, shardCount)}
	for shard := uint32(0); shard < shardCount; shard++ {
		route := actorrouting.ShardRoute{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 7, ExpectedWriterEpoch: 8, ExpectedLeaseEpoch: 9, LeaseUntilMs: now.Add(-time.Second).UnixMilli()}
		snapshot.Farm.Routes[shard] = route
		value := authorityValue
		value.VirtualShard = shard
		authorities.values[shard] = value
	}
	release := make(chan struct{})
	executor := &concurrentExecutor{started: make(chan string, shardCount), release: release}
	controller, err := New(Options{
		Snapshots: &fakeSnapshots{value: snapshot}, Authorities: authorities,
		Holders:  &fakeHolders{values: []verifiedholder.Holder{healthyHolder(now, "target", "target-boot", 1)}},
		Executor: executor, Domains: []actorrouting.RouteDomain{actorrouting.DomainFarm},
		Interval: time.Second, StepTimeout: time.Second, PlanTTL: time.Hour,
		MaxPlansPerPass: 2, MaxAuthorityReads: shardCount,
		Ready: func() bool { return true }, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	resultCh := make(chan Result, 1)
	go func() { resultCh <- controller.Reconcile(context.Background()) }()
	for i := 0; i < 2; i++ {
		select {
		case <-executor.started:
		case <-time.After(time.Second):
			t.Fatalf("only %d/2 initial sliding slots started", i)
		}
	}
	// Release exactly one occupied slot. The third Shard must start while the
	// other original worker remains blocked; a fixed wave would not do this.
	release <- struct{}{}
	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("third shard did not refill the first completed sliding slot")
	}
	release <- struct{}{}
	release <- struct{}{}
	result := <-resultCh
	if result.Err != nil || result.PlansCompleted != shardCount || result.AuthorityReads != shardCount {
		t.Fatalf("result=%+v", result)
	}
	executor.mu.Lock()
	maxInflight := executor.max
	executor.mu.Unlock()
	if maxInflight != 2 {
		t.Fatalf("max inflight=%d, want sliding limit 2", maxInflight)
	}
}

func TestMissingPodWaitsForLeaseThenRecoversOnlyOnce(t *testing.T) {
	now := time.Unix(100, 0)
	snapshot, authorityValue := failoverFixture(now.Add(time.Second))
	snapshots := &fakeSnapshots{value: snapshot}
	authorities := &fakeAuthorities{values: map[uint32]authoritystore.ShardWriteAuthority{0: authorityValue}}
	holders := []verifiedholder.Holder{healthyHolder(now, "target", "target-boot", 1)}
	executor := &fakeExecutor{}
	controller, err := New(Options{
		Snapshots: snapshots, Authorities: authorities, Holders: &fakeHolders{values: holders}, Executor: executor,
		Domains: []actorrouting.RouteDomain{actorrouting.DomainFarm}, Interval: time.Second,
		StepTimeout: time.Second, PlanTTL: time.Hour, MaxPlansPerPass: 1, MaxAuthorityReads: 4,
		Ready: func() bool { return true }, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if waiting := controller.Reconcile(context.Background()); waiting.WaitingLease != 1 || waiting.PlansCompleted != 0 {
		t.Fatalf("pre-expiry result=%+v", waiting)
	}

	now = now.Add(2 * time.Second)
	authorityValue.LeaseUntilMs = now.Add(-time.Second).UnixMilli()
	authorities.values[0] = authorityValue
	recovered := controller.Reconcile(context.Background())
	if recovered.Err != nil || recovered.PlansCompleted != 1 || len(executor.requests) != 1 || !executor.requests[0].Proof.LeaseExpired {
		t.Fatalf("recovered=%+v requests=%+v", recovered, executor.requests)
	}

	request := executor.requests[0]
	snapshots.value.Holders = append(snapshots.value.Holders, actorrouting.HolderRef{
		InstanceID: request.TargetHolderID, BootID: request.TargetBootID, Endpoint: request.TargetEndpoint,
		State: actorrouting.HolderStateActive, Weight: 1,
	})
	route := &snapshots.value.Farm.Routes[0]
	route.HolderIndex = len(snapshots.value.Holders) - 1
	route.ShardRevision++
	route.ExpectedWriterEpoch++
	route.ExpectedLeaseEpoch++
	route.LeaseUntilMs = now.Add(time.Minute).UnixMilli()
	authorities.values[0] = authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "auth-v1", HashVersion: snapshots.value.HashVersion, HashSeed: snapshots.value.HashSeed,
		Domain: actorrouting.DomainFarm, VirtualShard: 0, HolderID: request.TargetHolderID, HolderBootID: request.TargetBootID,
		AssignmentRevision: route.ShardRevision, WriterEpoch: route.ExpectedWriterEpoch, LeaseEpoch: route.ExpectedLeaseEpoch,
		State: authoritystore.ShardAuthorityActive, LeaseUntilMs: route.LeaseUntilMs, LastTransitionID: request.TransitionID,
	}
	if stable := controller.Reconcile(context.Background()); stable.PlansCompleted != 0 || stable.MissingShards != 0 || len(executor.requests) != 1 {
		t.Fatalf("stable=%+v requests=%+v", stable, executor.requests)
	}
}

func TestInformerViewRemovalDrivesWaitThenAutomaticTakeover(t *testing.T) {
	now := time.Unix(100, 0)
	snapshot, authorityValue := failoverFixture(now.Add(time.Second))
	authorities := &fakeAuthorities{values: map[uint32]authoritystore.ShardWriteAuthority{0: authorityValue}}
	executor := &fakeExecutor{}
	view := verifiedholder.NewStore(func() time.Time { return now })
	source := healthyHolder(now, "source", "boot-old", 0)
	source.Lifecycle = verifiedholder.LifecycleServing
	source.ServingShardCount = 1
	target := healthyHolder(now, "target", "target-boot", 1)
	workload := verifiedholder.WorkloadSnapshot{
		Key: source.WorkloadKey, WorkloadUID: source.WorkloadUID, KubeGeneration: 2,
		KubeResourceVersion: "rv-1", KubeListResourceVersion: "list-rv-1",
		DiscoveryRevision: 1, CapturedAt: now, Holders: []verifiedholder.Holder{source, target},
	}
	if err := view.ReplaceWorkload(workload); err != nil {
		t.Fatal(err)
	}
	controller, err := New(Options{
		Snapshots: &fakeSnapshots{value: snapshot}, Authorities: authorities, Holders: view, Executor: executor,
		Domains: []actorrouting.RouteDomain{actorrouting.DomainFarm}, Interval: time.Second,
		StepTimeout: time.Second, PlanTTL: time.Hour, MaxPlansPerPass: 1, MaxAuthorityReads: 4,
		Ready: func() bool { return true }, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if stable := controller.Reconcile(context.Background()); stable.MissingShards != 0 || stable.PlansCompleted != 0 {
		t.Fatalf("healthy informer view was treated as failed: %+v", stable)
	}

	workload.KubeResourceVersion = "rv-2"
	workload.KubeListResourceVersion = "list-rv-2"
	workload.DiscoveryRevision = 2
	workload.CapturedAt = now.Add(100 * time.Millisecond)
	workload.Holders = []verifiedholder.Holder{target}
	if err := view.ReplaceWorkload(workload); err != nil {
		t.Fatal(err)
	}
	if waiting := controller.Reconcile(context.Background()); waiting.MissingShards != 1 || waiting.WaitingLease != 1 || waiting.PlansCompleted != 0 {
		t.Fatalf("missing holder did not wait for the live authority lease: %+v", waiting)
	}

	now = now.Add(2 * time.Second)
	authorityValue.LeaseUntilMs = now.Add(-time.Second).UnixMilli()
	authorities.values[0] = authorityValue
	recovered := controller.Reconcile(context.Background())
	if recovered.Err != nil || recovered.PlansCompleted != 1 || len(executor.requests) != 1 {
		t.Fatalf("informer-driven recovery=%+v requests=%+v", recovered, executor.requests)
	}
	request := executor.requests[0]
	if !request.Proof.LeaseExpired || request.TargetHolderID != "target" || request.TargetBootID != "target-boot" {
		t.Fatalf("unexpected automatic takeover request: %+v", request)
	}
}

func newTestController(t *testing.T, now time.Time, snapshot actorrouting.RouteSnapshot, authorities *fakeAuthorities, holders []verifiedholder.Holder, executor *fakeExecutor) *Controller {
	t.Helper()
	controller, err := New(Options{
		Snapshots: &fakeSnapshots{value: snapshot}, Authorities: authorities,
		Holders: &fakeHolders{values: holders}, Executor: executor,
		Domains:  []actorrouting.RouteDomain{actorrouting.DomainFarm},
		Interval: time.Second, StepTimeout: time.Second, PlanTTL: time.Hour,
		MaxPlansPerPass: 1, MaxAuthorityReads: 4, Ready: func() bool { return true }, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func failoverFixture(leaseUntil time.Time) (actorrouting.RouteSnapshot, authoritystore.ShardWriteAuthority) {
	snapshot := actorrouting.RouteSnapshot{
		Realm: "r", GlobalRevision: 10, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 1,
		Player:  actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 7, ExpectedWriterEpoch: 8, ExpectedLeaseEpoch: 9, LeaseUntilMs: leaseUntil.UnixMilli()}}},
		Farm:    actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 7, ExpectedWriterEpoch: 8, ExpectedLeaseEpoch: 9, LeaseUntilMs: leaseUntil.UnixMilli()}}},
		Holders: []actorrouting.HolderRef{{InstanceID: "source", BootID: "boot-old", Endpoint: "source:9000", State: actorrouting.HolderStateActive, Weight: 1}},
	}
	value := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "auth-v1", HashVersion: snapshot.HashVersion, HashSeed: snapshot.HashSeed,
		Domain: actorrouting.DomainFarm, VirtualShard: 0, HolderID: "source", HolderBootID: "boot-old",
		AssignmentRevision: 7, WriterEpoch: 8, LeaseEpoch: 9, State: authoritystore.ShardAuthorityActive,
		LeaseUntilMs: leaseUntil.UnixMilli(),
	}
	return snapshot, value
}

func healthyHolder(now time.Time, holderID, bootID string, ordinal int32) verifiedholder.Holder {
	key := verifiedholder.WorkloadKey{Realm: "r", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	return verifiedholder.Holder{
		WorkloadKey: key, WorkloadUID: "workload", Domain: actorrouting.DomainFarm,
		HolderID: holderID, BootID: bootID, PodName: holderID, PodUID: holderID + "-pod", Ordinal: ordinal,
		LogicEndpoint: holderID + ":9000", ControlEndpoint: holderID + ":9001", TLSDNSIdentity: holderID,
		Lifecycle: verifiedholder.LifecycleReadyZeroShard, ProcessReady: true, CapacityWeight: 1,
		AttestationHash: "sha256:test", VerifiedAt: now.Add(-time.Second), VerifiedUntil: now.Add(time.Minute),
	}
}
