package routeplan

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/holderidentity"
	core "farmgame/internal/routecontrol"
	"farmgame/internal/storage/authoritystore"
)

func TestHandleExecutionErrorDoesNotHideUnbufferedWaveFailure(t *testing.T) {
	now := time.Now()
	snapshot := actorrouting.RouteSnapshot{
		Realm: "wave", GlobalRevision: 1, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 7,
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1,
			ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: now.Add(time.Hour).UnixMilli(),
		}}},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 2, State: actorrouting.ShardStateActive, ShardRevision: 1,
			ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: now.Add(time.Hour).UnixMilli(),
		}}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "source", BootID: "source-boot", Endpoint: "source:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "target", BootID: "target-boot", Endpoint: "target:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm", BootID: "farm-boot", Endpoint: "farm:9000", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
	stored, err := core.CanonicalizeRouteSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	member := core.AuthorityWaveMember{Domain: actorrouting.DomainPlayer, VirtualShard: 0}
	manager := &waveManager{
		snapshot: snapshot,
		store:    &waveSnapshotStore{stored: stored},
		authorities: map[core.AuthorityWaveMember]authoritystore.ShardWriteAuthority{member: {
			AuthorityLayoutVersion: "v1", HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 7,
			Domain: actorrouting.DomainPlayer, VirtualShard: 0,
			HolderID: "target", HolderBootID: "target-boot", AssignmentRevision: 2,
			WriterEpoch: 2, LeaseEpoch: 2, State: authoritystore.ShardAuthorityActive,
			LeaseUntilMs: now.Add(time.Hour).UnixMilli(), LastTransitionID: "transition-0",
		}},
		activeWave: "wave-1", stage: core.AuthorityWaveBatchStageAssigning,
		members:  map[core.AuthorityWaveMember]struct{}{member: {}},
		buffered: make(map[core.AuthorityWaveMember]struct{}),
	}
	plans, err := NewMemoryPlanStore(MemoryPlanStoreOptions{Now: func() time.Time { return now }, PlanTTL: time.Hour, AuditRetention: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	plan := Plan{
		PlanID: "plan-0", TransitionID: "transition-0", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		SourceHolderID: "source", SourceBootID: "source-boot", SourceEndpoint: "source:9000",
		SourceDeploymentIdentity: holderidentity.DeploymentIdentity{PodUID: "source-pod", WorkloadUID: "workload", Ordinal: 0},
		TargetHolderID:           "target", TargetBootID: "target-boot", TargetEndpoint: "target:9000",
		TargetDeploymentIdentity: holderidentity.DeploymentIdentity{PodUID: "target-pod", WorkloadUID: "workload", Ordinal: 1},
		ExpectedSourceFence: authoritystore.ShardWriteFence{
			ExpectedHolderID: "source", ExpectedHolderBootID: "source-boot",
			ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
		},
		RequestedTTLms: int64(time.Hour / time.Millisecond),
		Operation:      OperationPlannedDrain, Phase: PhasePending, Revision: 1,
		CreatedAtMs: now.UnixMilli(), UpdatedAtMs: now.UnixMilli(), ExpiresAtMs: now.Add(time.Hour).UnixMilli(),
		OwnerScalePlanID: "scale-plan", OwnerRetainUntilMs: now.Add(24 * time.Hour).UnixMilli(),
		OwnerWaveID: "wave-1", OwnerWaveSize: 1,
	}
	result, err := plans.CompareAndSwap(context.Background(), 0, plan)
	if err != nil || result.Status != CASCommitted {
		t.Fatalf("store plan status=%s err=%v", result.Status, err)
	}
	executor := &PlanExecutor{
		store: plans, manager: manager, authorityReader: waveAuthorityReader{manager},
		snapshotStore: manager.store, timeout: time.Second,
	}
	want := errors.New("target admission failed before wave publication")
	gotRecord, gotErr := executor.handleExecutionError(result.Record, want)
	if !errors.Is(gotErr, want) {
		t.Fatalf("error=%v want original unbuffered failure", gotErr)
	}
	if gotRecord.Plan.Phase != PhasePending {
		t.Fatalf("phase=%s want PENDING", gotRecord.Plan.Phase)
	}
}

func TestPendingProjectionAllowsExactCommittedPlannedDrainTargetWarming(t *testing.T) {
	now := time.Now()
	snapshot := actorrouting.RouteSnapshot{
		Realm: "wave", GlobalRevision: 9, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 7,
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1,
			ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: now.Add(time.Hour).UnixMilli(),
		}}},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 2, State: actorrouting.ShardStateActive, ShardRevision: 1,
			ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: now.Add(time.Hour).UnixMilli(),
		}}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "source", BootID: "source-boot", Endpoint: "source:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "target", BootID: "target-boot", Endpoint: "target:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm", BootID: "farm-boot", Endpoint: "farm:9000", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
	stored, err := core.CanonicalizeRouteSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	plans, err := NewMemoryPlanStore(MemoryPlanStoreOptions{Now: func() time.Time { return now }, PlanTTL: time.Hour, AuditRetention: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	plan := Plan{
		PlanID: "plan-warming", TransitionID: "transition-warming", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		SourceHolderID: "source", SourceBootID: "source-boot", SourceEndpoint: "source:9000",
		SourceDeploymentIdentity: holderidentity.DeploymentIdentity{PodUID: "source-pod", WorkloadUID: "workload", Ordinal: 0},
		TargetHolderID:           "target", TargetBootID: "target-boot", TargetEndpoint: "target:9000",
		TargetDeploymentIdentity: holderidentity.DeploymentIdentity{PodUID: "target-pod", WorkloadUID: "workload", Ordinal: 1},
		ExpectedSourceFence: authoritystore.ShardWriteFence{
			ExpectedHolderID: "source", ExpectedHolderBootID: "source-boot",
			ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
		},
		RequestedTTLms: int64(time.Hour / time.Millisecond), Operation: OperationPlannedDrain,
		Phase: PhasePending, Revision: 1, CreatedAtMs: now.UnixMilli(), UpdatedAtMs: now.UnixMilli(), ExpiresAtMs: now.Add(time.Hour).UnixMilli(),
		OwnerScalePlanID: "scale-plan", OwnerRetainUntilMs: now.Add(24 * time.Hour).UnixMilli(), OwnerWaveID: "wave-warming", OwnerWaveSize: 1,
	}
	storedPlan, err := plans.CompareAndSwap(context.Background(), 0, plan)
	if err != nil || storedPlan.Status != CASCommitted {
		t.Fatalf("store plan status=%s err=%v", storedPlan.Status, err)
	}
	manager := &waveManager{activeWave: "wave-warming", stage: core.AuthorityWaveBatchStageAssigning, buffered: make(map[core.AuthorityWaveMember]struct{})}
	holders := waveHolderReader{values: map[string]core.HolderRecord{"target": {
		Domain: actorrouting.DomainPlayer, HolderID: "target", BootID: "target-boot",
		DeploymentIdentity: plan.TargetDeploymentIdentity,
		LogicEndpoint:      "target:9000", HolderControlEndpoint: "target-control:9001", TLSDNSIdentity: "target.internal",
		State: core.HolderStateWarming, ProcessReady: false, Draining: false, ServingShardCount: 0,
		LeaseUntil: now.Add(time.Hour), SnapshotRevision: 9, CapacityWeight: 1,
	}}}
	executor := &PlanExecutor{store: plans, manager: manager, snapshotStore: &waveSnapshotStore{stored: stored}, holderReader: holders, now: func() time.Time { return now }, timeout: time.Second}
	recovery := authority.TransitionRecovery{
		TransitionID: plan.TransitionID, Domain: plan.Domain, VirtualShard: plan.VirtualShard,
		ExpectedFence: plan.ExpectedSourceFence, NewHolderID: plan.TargetHolderID, NewHolderBootID: plan.TargetBootID,
		Endpoint: plan.TargetEndpoint, RequestedTTLms: plan.RequestedTTLms,
		Mode: authority.TransferModeDrainHandoff, InternalDrain: true, Committed: true, Published: false,
	}
	if err := executor.pendingProjectionTargetValidator(plan.PlanID)(context.Background(), recovery, snapshot.GlobalRevision); err != nil {
		t.Fatalf("exact committed planned-drain recovery must admit zero-shard WARMING target: %v", err)
	}

	recovery.Committed = false
	if err := executor.pendingProjectionTargetValidator(plan.PlanID)(context.Background(), recovery, snapshot.GlobalRevision); err == nil {
		t.Fatal("fresh planned drain must not admit a WARMING target")
	}
}

type waveSnapshotStore struct {
	mu     sync.Mutex
	stored core.StoredRouteSnapshot
}

func (s *waveSnapshotStore) LoadCurrent(context.Context) (core.StoredRouteSnapshot, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stored.Clone(), true, nil
}
func (*waveSnapshotStore) CompareAndSwap(context.Context, core.NestedControlOperationToken, uint64, actorrouting.RouteSnapshot) (core.SnapshotCASResult, error) {
	return core.SnapshotCASResult{}, fmt.Errorf("ordinary snapshot CAS must not be used")
}

type waveAuthorityReader struct{ manager *waveManager }

func (r waveAuthorityReader) ReadShardWriteAuthority(_ context.Context, domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool, error) {
	r.manager.mu.Lock()
	defer r.manager.mu.Unlock()
	value, found := r.manager.authorities[core.AuthorityWaveMember{Domain: domain, VirtualShard: shard}]
	return value, found, nil
}

type waveHolderReader struct{ values map[string]core.HolderRecord }

func (r waveHolderReader) Get(_ context.Context, _ actorrouting.RouteDomain, id string) (core.HolderRecord, bool, error) {
	value, found := r.values[id]
	return value, found, nil
}
func (r waveHolderReader) GetCached(ctx context.Context, domain actorrouting.RouteDomain, id string) (core.HolderRecord, bool, error) {
	return r.Get(ctx, domain, id)
}
func (r waveHolderReader) List(context.Context, actorrouting.RouteDomain) ([]core.HolderRecord, error) {
	result := make([]core.HolderRecord, 0, len(r.values))
	for _, value := range r.values {
		result = append(result, value)
	}
	return result, nil
}

type waveManager struct {
	mu                      sync.Mutex
	snapshot                actorrouting.RouteSnapshot
	store                   *waveSnapshotStore
	authorities             map[core.AuthorityWaveMember]authoritystore.ShardWriteAuthority
	activeWave              string
	stage                   core.AuthorityWaveBatchStage
	members                 map[core.AuthorityWaveMember]struct{}
	buffered                map[core.AuthorityWaveMember]struct{}
	beginCalls              int
	commitCalls             int
	drainCalls              int
	returnProjectionPending bool
}

func (m *waveManager) CurrentSnapshot() actorrouting.RouteSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return actorrouting.CloneRouteSnapshot(m.snapshot)
}
func (m *waveManager) BeginAuthorityWaveBatch(_ context.Context, id string, members []core.AuthorityWaveMember) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.beginCalls++
	if m.activeWave == id && m.buffered != nil {
		// The production publisher keeps buffered steps across a failed commit;
		// a retry of the same wave id must not discard already-buffered members.
		return nil
	}
	m.activeWave = id
	m.stage = core.AuthorityWaveBatchStageAssigning
	for _, member := range members {
		var route actorrouting.ShardRoute
		switch member.Domain {
		case actorrouting.DomainPlayer:
			route = m.snapshot.Player.Routes[member.VirtualShard]
		case actorrouting.DomainFarm:
			route = m.snapshot.Farm.Routes[member.VirtualShard]
		}
		if route.State == actorrouting.ShardStateAssigning {
			m.stage = core.AuthorityWaveBatchStageActive
			break
		}
	}
	m.members = make(map[core.AuthorityWaveMember]struct{}, len(members))
	m.buffered = make(map[core.AuthorityWaveMember]struct{}, len(members))
	for _, member := range members {
		m.members[member] = struct{}{}
	}
	return nil
}
func (m *waveManager) AuthorityWaveBatchStage(id string) (core.AuthorityWaveBatchStage, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stage, m.activeWave == id
}
func (m *waveManager) AuthorityWaveBatchBaseRevision(id string) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return 1, m.activeWave == id
}
func (m *waveManager) AuthorityWaveBatchContains(id string, domain actorrouting.RouteDomain, shard uint32) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.buffered[core.AuthorityWaveMember{Domain: domain, VirtualShard: shard}]
	return m.activeWave == id && ok
}
func (m *waveManager) CommitAuthorityWaveBatch(_ context.Context, id string) (int, core.SnapshotCASResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.activeWave != id || len(m.buffered) != 32 {
		return 0, core.SnapshotCASResult{}, fmt.Errorf("incomplete wave")
	}
	m.commitCalls++
	stored, err := core.CanonicalizeRouteSnapshot(m.snapshot)
	if err != nil {
		return 0, core.SnapshotCASResult{}, err
	}
	stored.CommittedAtMs = time.Now().UnixMilli()
	m.store.mu.Lock()
	m.store.stored = stored.Clone()
	m.store.mu.Unlock()
	m.activeWave = ""
	return len(m.buffered), core.SnapshotCASResult{Status: core.SnapshotCASCommitted, Stored: stored}, nil
}
func (m *waveManager) AbortEmptyAuthorityWaveBatch(string) error { return nil }
func (m *waveManager) DrainValidated(ctx context.Context, req authority.TransferRequest, controller authority.DrainController, _ authority.PendingProjectionTargetValidator) (authoritystore.ShardAuthorityTransitionResult, error) {
	if err := controller.StopNewActivationAndTurns(ctx, req.Domain, req.VirtualShard); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	if err := controller.WaitCurrentTurns(ctx, req.Domain, req.VirtualShard); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	if err := controller.ConfirmFactsCommitted(ctx, req.Domain, req.VirtualShard); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	m.mu.Lock()
	member := core.AuthorityWaveMember{Domain: req.Domain, VirtualShard: req.VirtualShard}
	if _, already := m.buffered[member]; already {
		m.mu.Unlock()
		return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionAlreadyApplied, Authority: m.authorities[member]}, nil
	}
	previous := m.authorities[member]
	target := previous
	if m.stage == core.AuthorityWaveBatchStageAssigning && previous.HolderID != req.NewHolderID {
		target.HolderID, target.HolderBootID = req.NewHolderID, req.NewHolderBootID
		target.AssignmentRevision = previous.AssignmentRevision + 1
		target.WriterEpoch = previous.WriterEpoch + 1
		target.LeaseEpoch = previous.LeaseEpoch + 1
		target.LeaseUntilMs = time.Now().Add(time.Hour).UnixMilli()
		target.LastTransitionID = req.TransitionID
	}
	m.mu.Unlock()
	// Production commits the complete ASSIGNING stage before it asks any
	// target Holder to cold-load. Keep the fake faithful to that ordering so
	// active-stage partial recovery is exercised by the tests below.
	if m.stage == core.AuthorityWaveBatchStageActive {
		if err := controller.ColdLoadNewHolder(ctx, req.Domain, req.VirtualShard, target); err != nil {
			return authoritystore.ShardAuthorityTransitionResult{}, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stage == core.AuthorityWaveBatchStageAssigning && previous.HolderID != req.NewHolderID {
		m.drainCalls++
		m.authorities[member] = target
	}
	m.snapshot.GlobalRevision++
	route := m.snapshot.Player.Routes[req.VirtualShard]
	route.HolderIndex = 1
	if m.stage == core.AuthorityWaveBatchStageAssigning {
		route.State = actorrouting.ShardStateAssigning
	} else {
		route.State = actorrouting.ShardStateActive
	}
	route.ShardRevision, route.ExpectedWriterEpoch, route.ExpectedLeaseEpoch, route.LeaseUntilMs = target.AssignmentRevision, target.WriterEpoch, target.LeaseEpoch, target.LeaseUntilMs
	m.snapshot.Player.Routes[req.VirtualShard] = route
	m.buffered[member] = struct{}{}
	result := authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionApplied, Authority: target}
	if m.stage == core.AuthorityWaveBatchStageAssigning {
		return result, authority.ErrAuthorityWaveStagePending
	}
	return result, nil
}
func (*waveManager) HandoffWorkerValidated(context.Context, authority.TransferRequest, authority.WorkerHandoffSourceValidator, authority.PendingProjectionTargetValidator) (authoritystore.ShardAuthorityTransitionResult, error) {
	return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("unexpected")
}
func (*waveManager) TransferValidated(context.Context, authority.TransferRequest, authority.PendingProjectionTargetValidator) (authoritystore.ShardAuthorityTransitionResult, error) {
	return authoritystore.ShardAuthorityTransitionResult{}, fmt.Errorf("unexpected")
}

type waveDrainController struct{}

func (waveDrainController) StopNewActivationAndTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	return nil
}
func (waveDrainController) WaitCurrentTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	return nil
}
func (waveDrainController) ConfirmFactsCommitted(context.Context, actorrouting.RouteDomain, uint32) error {
	return nil
}
func (waveDrainController) ColdLoadNewHolder(context.Context, actorrouting.RouteDomain, uint32, authoritystore.ShardWriteAuthority) error {
	return nil
}

type waveExecutionGuard struct {
	mu              sync.Mutex
	waveEntries     int
	ordinaryEntries int
}

func (g *waveExecutionGuard) Enter(context.Context) (func(), error) {
	g.mu.Lock()
	g.ordinaryEntries++
	g.mu.Unlock()
	return nil, fmt.Errorf("ordinary per-plan guard must be bypassed inside a wave")
}

func (g *waveExecutionGuard) EnterAuthorityWave(context.Context) (func(), error) {
	g.mu.Lock()
	g.waveEntries++
	g.mu.Unlock()
	return func() {}, nil
}

func TestRecoverExecutesThirtyTwoOwnedPlansWithOneWaveCommit(t *testing.T) {
	now := time.Now()
	routes := make([]actorrouting.ShardRoute, 32)
	authorities := make(map[core.AuthorityWaveMember]authoritystore.ShardWriteAuthority, 32)
	for shard := range routes {
		routes[shard] = actorrouting.ShardRoute{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: now.Add(time.Hour).UnixMilli()}
		authorities[core.AuthorityWaveMember{Domain: actorrouting.DomainPlayer, VirtualShard: uint32(shard)}] = authoritystore.ShardWriteAuthority{AuthorityLayoutVersion: "v1", HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 7, Domain: actorrouting.DomainPlayer, VirtualShard: uint32(shard), HolderID: "source", HolderBootID: "source-boot", AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1, State: authoritystore.ShardAuthorityActive, LeaseUntilMs: now.Add(time.Hour).UnixMilli(), LastTransitionID: "initial"}
	}
	snapshot := actorrouting.RouteSnapshot{
		Realm: "wave", GlobalRevision: 1, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 7,
		Player: actorrouting.DomainSnapshot{ShardCount: 32, Routes: routes},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 2, State: actorrouting.ShardStateActive, ShardRevision: 1,
			ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: now.Add(time.Hour).UnixMilli(),
		}}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "source", BootID: "source-boot", Endpoint: "source:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "target", BootID: "target-boot", Endpoint: "target:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm", BootID: "farm-boot", Endpoint: "farm:9000", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
	stored, err := core.CanonicalizeRouteSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshotStore := &waveSnapshotStore{stored: stored}
	manager := &waveManager{snapshot: snapshot, store: snapshotStore, authorities: authorities, returnProjectionPending: true}
	plans, err := NewMemoryPlanStore(MemoryPlanStoreOptions{Now: func() time.Time { return now }, PlanTTL: 24 * time.Hour, AuditRetention: 38 * 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	identitySource := holderidentity.DeploymentIdentity{PodUID: "source-pod", WorkloadUID: "workload", Ordinal: 0}
	identityTarget := holderidentity.DeploymentIdentity{PodUID: "target-pod", WorkloadUID: "workload", Ordinal: 2}
	for shard := 0; shard < 32; shard++ {
		plan := Plan{PlanID: fmt.Sprintf("plan-%02d", shard), TransitionID: fmt.Sprintf("transition-%02d", shard), Domain: actorrouting.DomainPlayer, VirtualShard: uint32(shard), SourceHolderID: "source", SourceBootID: "source-boot", SourceDeploymentIdentity: identitySource, SourceEndpoint: "source:9000", TargetHolderID: "target", TargetBootID: "target-boot", TargetDeploymentIdentity: identityTarget, TargetEndpoint: "target:9000", ExpectedSourceFence: authoritystore.ShardWriteFence{ExpectedHolderID: "source", ExpectedHolderBootID: "source-boot", ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1}, RequestedTTLms: int64(time.Hour / time.Millisecond), Operation: OperationPlannedDrain, Phase: PhasePending, Revision: 1, CreatedAtMs: now.UnixMilli(), UpdatedAtMs: now.UnixMilli(), ExpiresAtMs: now.Add(24 * time.Hour).UnixMilli(), OwnerScalePlanID: "scale-plan", OwnerRetainUntilMs: now.Add(7 * 24 * time.Hour).UnixMilli(), OwnerWaveID: "wave-32", OwnerWaveSize: 32, OwnerWavePosition: uint32(shard)}
		if result, err := plans.CompareAndSwap(context.Background(), 0, plan); err != nil || !result.Record.Plan.Phase.Terminal() && result.Status != CASCommitted {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	holders := waveHolderReader{values: map[string]core.HolderRecord{"source": {Domain: actorrouting.DomainPlayer, HolderID: "source", BootID: "source-boot", DeploymentIdentity: identitySource, LogicEndpoint: "source:9000", HolderControlEndpoint: "source-control:9001", TLSDNSIdentity: "source.internal", State: core.HolderStateServing, ProcessReady: true, LeaseUntil: now.Add(time.Hour), SnapshotRevision: 1, CapacityWeight: 1}, "target": {Domain: actorrouting.DomainPlayer, HolderID: "target", BootID: "target-boot", DeploymentIdentity: identityTarget, LogicEndpoint: "target:9000", HolderControlEndpoint: "target-control:9001", TLSDNSIdentity: "target.internal", State: core.HolderStateReadyZeroShard, ProcessReady: true, LeaseUntil: now.Add(time.Hour), SnapshotRevision: 1, CapacityWeight: 1}}}
	coordinator := newControlMutationTestCoordinator()
	guard := &waveExecutionGuard{}
	executor, err := NewPlanExecutor(PlanExecutorOptions{Store: plans, Manager: manager, AuthorityReader: waveAuthorityReader{manager}, SnapshotStore: snapshotStore, HolderReader: holders, DrainControllerFactory: func(context.Context, Plan) (authority.DrainController, func() error, error) {
		return waveDrainController{}, func() error { return nil }, nil
	}, Now: func() time.Time { return now }, OperationTimeout: 5 * time.Minute, PlanTTL: 24 * time.Hour, SingleController: SingleControllerAttestation{Contract: SingleControllerContract, ControllerID: "controller", BootID: "boot"}, ExecutionGuard: guard, ControlMutation: controlMutationTestOptions(coordinator)})
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if manager.beginCalls != 2 || manager.commitCalls != 2 || manager.drainCalls != 32 {
		t.Fatalf("begin=%d commit=%d drains=%d", manager.beginCalls, manager.commitCalls, manager.drainCalls)
	}
	guard.mu.Lock()
	waveEntries, ordinaryEntries := guard.waveEntries, guard.ordinaryEntries
	guard.mu.Unlock()
	if waveEntries != 1 || ordinaryEntries != 0 {
		t.Fatalf("wave guard entries=%d ordinary entries=%d want 1/0", waveEntries, ordinaryEntries)
	}
	if coordinator.acquireCalls != 99 || coordinator.resolveCalls != 99 {
		t.Fatalf("control acquires=%d resolves=%d, want 99/99", coordinator.acquireCalls, coordinator.resolveCalls)
	}
	current, _, _ := snapshotStore.LoadCurrent(context.Background())
	if current.Revision != 65 {
		t.Fatalf("revision=%d", current.Revision)
	}
	for shard := 0; shard < 32; shard++ {
		record, found, err := plans.Load(context.Background(), fmt.Sprintf("plan-%02d", shard))
		if err != nil || !found || record.Plan.Phase != PhaseSucceeded {
			t.Fatalf("shard=%d phase=%s found=%t err=%v", shard, record.Plan.Phase, found, err)
		}
	}
}

type flakyWaveDrainController struct {
	mu        sync.Mutex
	failShard uint32
	failOnce  bool
}

func (c *flakyWaveDrainController) StopNewActivationAndTurns(ctx context.Context, _ actorrouting.RouteDomain, shard uint32) error {
	return nil
}
func (c *flakyWaveDrainController) WaitCurrentTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	return nil
}
func (c *flakyWaveDrainController) ConfirmFactsCommitted(context.Context, actorrouting.RouteDomain, uint32) error {
	return nil
}

func (c *flakyWaveDrainController) ColdLoadNewHolder(_ context.Context, _ actorrouting.RouteDomain, shard uint32, _ authoritystore.ShardWriteAuthority) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if shard == c.failShard && !c.failOnce {
		c.failOnce = true
		return context.DeadlineExceeded
	}
	return nil
}

// TestRecoverRetriesTransientlyPausedWaveMember simulates the real two-stage
// failure: ASSIGNING is already durable, then one target cold-load times out
// while the other ACTIVE projections remain buffered in memory. A second
// recovery pass must execute only the missing member and publish the complete
// ACTIVE stage instead of duplicating buffered members or blocking the parent.
func TestRecoverRetriesTransientlyPausedWaveMember(t *testing.T) {
	now := time.Now()
	routes := make([]actorrouting.ShardRoute, 32)
	authorities := make(map[core.AuthorityWaveMember]authoritystore.ShardWriteAuthority, 32)
	for shard := range routes {
		routes[shard] = actorrouting.ShardRoute{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: now.Add(time.Hour).UnixMilli()}
		authorities[core.AuthorityWaveMember{Domain: actorrouting.DomainPlayer, VirtualShard: uint32(shard)}] = authoritystore.ShardWriteAuthority{AuthorityLayoutVersion: "v1", HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 7, Domain: actorrouting.DomainPlayer, VirtualShard: uint32(shard), HolderID: "source", HolderBootID: "source-boot", AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1, State: authoritystore.ShardAuthorityActive, LeaseUntilMs: now.Add(time.Hour).UnixMilli(), LastTransitionID: "initial"}
	}
	snapshot := actorrouting.RouteSnapshot{
		Realm: "wave", GlobalRevision: 1, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 7,
		Player: actorrouting.DomainSnapshot{ShardCount: 32, Routes: routes},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 2, State: actorrouting.ShardStateActive, ShardRevision: 1,
			ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: now.Add(time.Hour).UnixMilli(),
		}}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "source", BootID: "source-boot", Endpoint: "source:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "target", BootID: "target-boot", Endpoint: "target:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm", BootID: "farm-boot", Endpoint: "farm:9000", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
	stored, err := core.CanonicalizeRouteSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshotStore := &waveSnapshotStore{stored: stored}
	manager := &waveManager{snapshot: snapshot, store: snapshotStore, authorities: authorities}
	plans, err := NewMemoryPlanStore(MemoryPlanStoreOptions{Now: func() time.Time { return now }, PlanTTL: 24 * time.Hour, AuditRetention: 38 * 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	identitySource := holderidentity.DeploymentIdentity{PodUID: "source-pod", WorkloadUID: "workload", Ordinal: 0}
	identityTarget := holderidentity.DeploymentIdentity{PodUID: "target-pod", WorkloadUID: "workload", Ordinal: 2}
	for shard := 0; shard < 32; shard++ {
		plan := Plan{PlanID: fmt.Sprintf("plan-%02d", shard), TransitionID: fmt.Sprintf("transition-%02d", shard), Domain: actorrouting.DomainPlayer, VirtualShard: uint32(shard), SourceHolderID: "source", SourceBootID: "source-boot", SourceDeploymentIdentity: identitySource, SourceEndpoint: "source:9000", TargetHolderID: "target", TargetBootID: "target-boot", TargetDeploymentIdentity: identityTarget, TargetEndpoint: "target:9000", ExpectedSourceFence: authoritystore.ShardWriteFence{ExpectedHolderID: "source", ExpectedHolderBootID: "source-boot", ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1}, RequestedTTLms: int64(time.Hour / time.Millisecond), Operation: OperationPlannedDrain, Phase: PhasePending, Revision: 1, CreatedAtMs: now.UnixMilli(), UpdatedAtMs: now.UnixMilli(), ExpiresAtMs: now.Add(24 * time.Hour).UnixMilli(), OwnerScalePlanID: "scale-plan", OwnerRetainUntilMs: now.Add(7 * 24 * time.Hour).UnixMilli(), OwnerWaveID: "wave-32", OwnerWaveSize: 32, OwnerWavePosition: uint32(shard)}
		if result, err := plans.CompareAndSwap(context.Background(), 0, plan); err != nil || !result.Record.Plan.Phase.Terminal() && result.Status != CASCommitted {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	holders := waveHolderReader{values: map[string]core.HolderRecord{"source": {Domain: actorrouting.DomainPlayer, HolderID: "source", BootID: "source-boot", DeploymentIdentity: identitySource, LogicEndpoint: "source:9000", HolderControlEndpoint: "source-control:9001", TLSDNSIdentity: "source.internal", State: core.HolderStateServing, ProcessReady: true, LeaseUntil: now.Add(time.Hour), SnapshotRevision: 1, CapacityWeight: 1}, "target": {Domain: actorrouting.DomainPlayer, HolderID: "target", BootID: "target-boot", DeploymentIdentity: identityTarget, LogicEndpoint: "target:9000", HolderControlEndpoint: "target-control:9001", TLSDNSIdentity: "target.internal", State: core.HolderStateReadyZeroShard, ProcessReady: true, LeaseUntil: now.Add(time.Hour), SnapshotRevision: 1, CapacityWeight: 1}}}
	flaky := &flakyWaveDrainController{failShard: 5}
	executor, err := NewPlanExecutor(PlanExecutorOptions{Store: plans, Manager: manager, AuthorityReader: waveAuthorityReader{manager}, SnapshotStore: snapshotStore, HolderReader: holders, DrainControllerFactory: func(context.Context, Plan) (authority.DrainController, func() error, error) {
		return flaky, func() error { return nil }, nil
	}, Now: func() time.Time { return now }, OperationTimeout: 5 * time.Minute, PlanTTL: 24 * time.Hour, SingleController: SingleControllerAttestation{Contract: SingleControllerContract, ControllerID: "controller", BootID: "boot"}})
	if err != nil {
		t.Fatal(err)
	}
	firstErr := executor.Recover(context.Background())
	if firstErr == nil {
		t.Fatal("first recovery pass must fail when a member times out")
	}
	manager.mu.Lock()
	firstStage, firstBuffered, firstCommits := manager.stage, len(manager.buffered), manager.commitCalls
	manager.mu.Unlock()
	if firstStage != core.AuthorityWaveBatchStageActive || firstBuffered != 31 || firstCommits != 1 {
		t.Fatalf("stage=%d buffered=%d commits=%d want ACTIVE/31/1", firstStage, firstBuffered, firstCommits)
	}
	firstSnapshot, _, _ := snapshotStore.LoadCurrent(context.Background())
	if firstSnapshot.Revision != 33 {
		t.Fatalf("first snapshot revision=%d want 33", firstSnapshot.Revision)
	}

	// The second pass must unpause the member and complete the whole wave.
	if err := executor.Recover(context.Background()); err != nil {
		t.Fatalf("second recovery pass: %v", err)
	}
	if manager.beginCalls != 3 || manager.commitCalls != 2 || manager.drainCalls != 32 {
		t.Fatalf("begin=%d commit=%d drains=%d want 3/2/32", manager.beginCalls, manager.commitCalls, manager.drainCalls)
	}
	current, _, _ := snapshotStore.LoadCurrent(context.Background())
	if current.Revision != 65 {
		t.Fatalf("revision=%d want 65", current.Revision)
	}
	for shard := 0; shard < 32; shard++ {
		record, found, err := plans.Load(context.Background(), fmt.Sprintf("plan-%02d", shard))
		if err != nil || !found || record.Plan.Phase != PhaseSucceeded {
			t.Fatalf("shard=%d phase=%s found=%t err=%v", shard, record.Plan.Phase, found, err)
		}
	}
}

// TestRecoverRetriesMixedPausedWaveMembers covers the exact r15 production
// mix: some members paused with authority_committed_snapshot_pending (their
// Authority was already written by an earlier wave attempt) and one paused
// with TIMEOUT (its transition never completed). A second recovery pass must
// re-aggregate every member, replay committed transitions idempotently, and
// complete one wave commit.
func TestRecoverRetriesMixedPausedWaveMembers(t *testing.T) {
	now := time.Now()
	routes := make([]actorrouting.ShardRoute, 32)
	authorities := make(map[core.AuthorityWaveMember]authoritystore.ShardWriteAuthority, 32)
	for shard := range routes {
		routes[shard] = actorrouting.ShardRoute{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: now.Add(time.Hour).UnixMilli()}
		authorities[core.AuthorityWaveMember{Domain: actorrouting.DomainPlayer, VirtualShard: uint32(shard)}] = authoritystore.ShardWriteAuthority{AuthorityLayoutVersion: "v1", HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 7, Domain: actorrouting.DomainPlayer, VirtualShard: uint32(shard), HolderID: "source", HolderBootID: "source-boot", AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1, State: authoritystore.ShardAuthorityActive, LeaseUntilMs: now.Add(time.Hour).UnixMilli(), LastTransitionID: "initial"}
	}
	snapshot := actorrouting.RouteSnapshot{
		Realm: "wave", GlobalRevision: 1, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 7,
		Player: actorrouting.DomainSnapshot{ShardCount: 32, Routes: routes},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 2, State: actorrouting.ShardStateActive, ShardRevision: 1,
			ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: now.Add(time.Hour).UnixMilli(),
		}}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "source", BootID: "source-boot", Endpoint: "source:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "target", BootID: "target-boot", Endpoint: "target:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm", BootID: "farm-boot", Endpoint: "farm:9000", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
	stored, err := core.CanonicalizeRouteSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshotStore := &waveSnapshotStore{stored: stored}
	manager := &waveManager{snapshot: snapshot, store: snapshotStore, authorities: authorities}
	plans, err := NewMemoryPlanStore(MemoryPlanStoreOptions{Now: func() time.Time { return now }, PlanTTL: 24 * time.Hour, AuditRetention: 38 * 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	identitySource := holderidentity.DeploymentIdentity{PodUID: "source-pod", WorkloadUID: "workload", Ordinal: 0}
	identityTarget := holderidentity.DeploymentIdentity{PodUID: "target-pod", WorkloadUID: "workload", Ordinal: 2}
	for shard := 0; shard < 32; shard++ {
		plan := Plan{PlanID: fmt.Sprintf("plan-%02d", shard), TransitionID: fmt.Sprintf("transition-%02d", shard), Domain: actorrouting.DomainPlayer, VirtualShard: uint32(shard), SourceHolderID: "source", SourceBootID: "source-boot", SourceDeploymentIdentity: identitySource, SourceEndpoint: "source:9000", TargetHolderID: "target", TargetBootID: "target-boot", TargetDeploymentIdentity: identityTarget, TargetEndpoint: "target:9000", ExpectedSourceFence: authoritystore.ShardWriteFence{ExpectedHolderID: "source", ExpectedHolderBootID: "source-boot", ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1}, RequestedTTLms: int64(time.Hour / time.Millisecond), Operation: OperationPlannedDrain, Phase: PhasePending, Revision: 1, CreatedAtMs: now.UnixMilli(), UpdatedAtMs: now.UnixMilli(), ExpiresAtMs: now.Add(24 * time.Hour).UnixMilli(), OwnerScalePlanID: "scale-plan", OwnerRetainUntilMs: now.Add(7 * 24 * time.Hour).UnixMilli(), OwnerWaveID: "wave-32", OwnerWaveSize: 32, OwnerWavePosition: uint32(shard)}
		if _, err := plans.CompareAndSwap(context.Background(), 0, plan); err != nil {
			t.Fatal(err)
		}
	}
	// Simulate the r15 mid-wave crash state: members 0..4 already wrote their
	// Authority and are paused waiting for the wave commit; member 5 timed out
	// before its transition; the rest are mid-wave pre-terminal. The wave was
	// already begun by the earlier attempt and its steps were never discarded.
	manager.mu.Lock()
	manager.activeWave = "wave-32"
	manager.stage = core.AuthorityWaveBatchStageAssigning
	manager.buffered = make(map[core.AuthorityWaveMember]struct{})
	manager.mu.Unlock()
	for shard := 0; shard < 5; shard++ {
		manager.mu.Lock()
		member := core.AuthorityWaveMember{Domain: actorrouting.DomainPlayer, VirtualShard: uint32(shard)}
		previous := manager.authorities[member]
		target := previous
		target.HolderID, target.HolderBootID = "target", "target-boot"
		target.AssignmentRevision, target.WriterEpoch, target.LeaseEpoch = 2, 2, 2
		target.LastTransitionID = fmt.Sprintf("transition-%02d", shard)
		manager.authorities[member] = target
		// Simulate the earlier wave attempt that wrote this Authority: the
		// member's ASSIGNING projection step was already buffered (route installed in the
		// manager snapshot) before the commit never landed; the production
		// publisher keeps those steps across retries.
		route := manager.snapshot.Player.Routes[shard]
		route.HolderIndex, route.State = 1, actorrouting.ShardStateAssigning
		route.ShardRevision, route.ExpectedWriterEpoch, route.ExpectedLeaseEpoch, route.LeaseUntilMs = target.AssignmentRevision, target.WriterEpoch, target.LeaseEpoch, target.LeaseUntilMs
		manager.snapshot.Player.Routes[shard] = route
		manager.snapshot.GlobalRevision++
		manager.buffered[member] = struct{}{}
		manager.mu.Unlock()
		record, _, _ := plans.Load(context.Background(), fmt.Sprintf("plan-%02d", shard))
		_, err := plans.CompareAndSwap(context.Background(), record.Plan.Revision, Plan{PlanID: fmt.Sprintf("plan-%02d", shard), TransitionID: fmt.Sprintf("transition-%02d", shard), Domain: actorrouting.DomainPlayer, VirtualShard: uint32(shard), SourceHolderID: "source", SourceBootID: "source-boot", SourceDeploymentIdentity: identitySource, SourceEndpoint: "source:9000", TargetHolderID: "target", TargetBootID: "target-boot", TargetDeploymentIdentity: identityTarget, TargetEndpoint: "target:9000", ExpectedSourceFence: authoritystore.ShardWriteFence{ExpectedHolderID: "source", ExpectedHolderBootID: "source-boot", ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1}, RequestedTTLms: int64(time.Hour / time.Millisecond), Operation: OperationPlannedDrain, Phase: PhasePaused, ErrorCode: ErrorAuthorityCommittedSnapshotPending, CompletedAtMs: now.UnixMilli(), Revision: record.Plan.Revision + 1, Attempt: 1, CreatedAtMs: now.UnixMilli(), UpdatedAtMs: now.UnixMilli(), ExpiresAtMs: now.Add(24 * time.Hour).UnixMilli(), OwnerScalePlanID: "scale-plan", OwnerRetainUntilMs: now.Add(7 * 24 * time.Hour).UnixMilli(), OwnerWaveID: "wave-32", OwnerWaveSize: 32, OwnerWavePosition: uint32(shard)})
		if err != nil {
			t.Fatal(err)
		}
	}
	{
		record, _, _ := plans.Load(context.Background(), "plan-05")
		_, err := plans.CompareAndSwap(context.Background(), record.Plan.Revision, Plan{PlanID: "plan-05", TransitionID: "transition-05", Domain: actorrouting.DomainPlayer, VirtualShard: 5, SourceHolderID: "source", SourceBootID: "source-boot", SourceDeploymentIdentity: identitySource, SourceEndpoint: "source:9000", TargetHolderID: "target", TargetBootID: "target-boot", TargetDeploymentIdentity: identityTarget, TargetEndpoint: "target:9000", ExpectedSourceFence: authoritystore.ShardWriteFence{ExpectedHolderID: "source", ExpectedHolderBootID: "source-boot", ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1}, RequestedTTLms: int64(time.Hour / time.Millisecond), Operation: OperationPlannedDrain, Phase: PhasePaused, ErrorCode: "TIMEOUT", CompletedAtMs: now.UnixMilli(), Revision: record.Plan.Revision + 1, Attempt: 1, CreatedAtMs: now.UnixMilli(), UpdatedAtMs: now.UnixMilli(), ExpiresAtMs: now.Add(24 * time.Hour).UnixMilli(), OwnerScalePlanID: "scale-plan", OwnerRetainUntilMs: now.Add(7 * 24 * time.Hour).UnixMilli(), OwnerWaveID: "wave-32", OwnerWaveSize: 32, OwnerWavePosition: 5})
		if err != nil {
			t.Fatal(err)
		}
	}
	holders := waveHolderReader{values: map[string]core.HolderRecord{"source": {Domain: actorrouting.DomainPlayer, HolderID: "source", BootID: "source-boot", DeploymentIdentity: identitySource, LogicEndpoint: "source:9000", HolderControlEndpoint: "source-control:9001", TLSDNSIdentity: "source.internal", State: core.HolderStateServing, ProcessReady: true, LeaseUntil: now.Add(time.Hour), SnapshotRevision: 1, CapacityWeight: 1}, "target": {Domain: actorrouting.DomainPlayer, HolderID: "target", BootID: "target-boot", DeploymentIdentity: identityTarget, LogicEndpoint: "target:9000", HolderControlEndpoint: "target-control:9001", TLSDNSIdentity: "target.internal", State: core.HolderStateReadyZeroShard, ProcessReady: true, LeaseUntil: now.Add(time.Hour), SnapshotRevision: 1, CapacityWeight: 1}}}
	executor, err := NewPlanExecutor(PlanExecutorOptions{Store: plans, Manager: manager, AuthorityReader: waveAuthorityReader{manager}, SnapshotStore: snapshotStore, HolderReader: holders, DrainControllerFactory: func(context.Context, Plan) (authority.DrainController, func() error, error) {
		return waveDrainController{}, func() error { return nil }, nil
	}, Now: func() time.Time { return now }, OperationTimeout: 5 * time.Minute, PlanTTL: 24 * time.Hour, SingleController: SingleControllerAttestation{Contract: SingleControllerContract, ControllerID: "controller", BootID: "boot"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := executor.Recover(context.Background()); err != nil {
		t.Fatalf("recovery pass over mixed paused members: %v", err)
	}
	if manager.beginCalls != 2 || manager.commitCalls != 2 {
		t.Fatalf("begin=%d commit=%d want 2/2", manager.beginCalls, manager.commitCalls)
	}
	current, _, _ := snapshotStore.LoadCurrent(context.Background())
	if current.Revision != 65 {
		t.Fatalf("revision=%d want 65", current.Revision)
	}
	for shard := 0; shard < 32; shard++ {
		record, found, err := plans.Load(context.Background(), fmt.Sprintf("plan-%02d", shard))
		if err != nil || !found || record.Plan.Phase != PhaseSucceeded {
			t.Fatalf("shard=%d phase=%s found=%t err=%v", shard, record.Plan.Phase, found, err)
		}
	}
}
