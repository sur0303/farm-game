package holdercontrol

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderidentity"
	core "farmgame/internal/routecontrol"
)

type fakeAgentRegistry struct {
	mu            sync.Mutex
	record        core.HolderRecord
	registers     int
	registrations []core.HolderRegistration
	heartbeats    []core.HolderHeartbeat
	failAfter     int
	getErr        error
}

type expiringPriorBootRegistry struct {
	base          *fakeAgentRegistry
	prior         core.HolderRecord
	registerCalls int
}

type materializedGenerationRegistry struct {
	base        *fakeAgentRegistry
	durable     core.HolderRecord
	registers   int
	registerErr error
}

func (r *materializedGenerationRegistry) Register(
	_ context.Context,
	_ core.HolderRegistration,
) (core.HolderRecord, error) {
	r.registers++
	return core.HolderRecord{}, r.registerErr
}

func (r *materializedGenerationRegistry) Heartbeat(
	ctx context.Context,
	input core.HolderHeartbeat,
) (core.HolderRecord, error) {
	return r.base.Heartbeat(ctx, input)
}

func (r *materializedGenerationRegistry) Get(
	ctx context.Context,
	domain actorrouting.RouteDomain,
	holderID string,
) (core.HolderRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return core.HolderRecord{}, false, err
	}
	if r.durable.Domain != domain || r.durable.HolderID != holderID {
		return core.HolderRecord{}, false, nil
	}
	return cloneRecord(r.durable), true, nil
}

func (r *expiringPriorBootRegistry) Register(
	ctx context.Context,
	input core.HolderRegistration,
) (core.HolderRecord, error) {
	r.registerCalls++
	if r.registerCalls == 1 {
		return core.HolderRecord{}, core.ErrHolderBootConflict
	}
	return r.base.Register(ctx, input)
}

func (r *expiringPriorBootRegistry) Heartbeat(
	ctx context.Context,
	input core.HolderHeartbeat,
) (core.HolderRecord, error) {
	return r.base.Heartbeat(ctx, input)
}

func (r *expiringPriorBootRegistry) Get(
	ctx context.Context,
	domain actorrouting.RouteDomain,
	holderID string,
) (core.HolderRecord, bool, error) {
	if r.registerCalls == 1 {
		return cloneRecord(r.prior), true, nil
	}
	return r.base.Get(ctx, domain, holderID)
}

func (r *fakeAgentRegistry) Register(_ context.Context, input core.HolderRegistration) (core.HolderRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registers++
	r.registrations = append(r.registrations, cloneRecord(input))
	r.record = cloneRecord(input)
	return cloneRecord(r.record), nil
}

func (r *fakeAgentRegistry) Heartbeat(_ context.Context, input core.HolderHeartbeat) (core.HolderRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failAfter >= 0 && len(r.heartbeats) >= r.failAfter {
		return core.HolderRecord{}, errors.New("registry unavailable")
	}
	r.heartbeats = append(r.heartbeats, input)
	r.record.State = input.State
	r.record.ProcessReady = input.ProcessReady
	r.record.Draining = input.Draining
	r.record.ServingShardCount = input.ServingShardCount
	r.record.SnapshotRevision = input.SnapshotRevision
	r.record.HeartbeatAt = input.HeartbeatAt
	r.record.LeaseUntil = input.LeaseUntil
	r.record.Sequence = input.Sequence
	r.record.CapacityWeight = input.CapacityWeight
	r.record.ResourceSummary = cloneResourceSummary(input.ResourceSummary)
	return cloneRecord(r.record), nil
}

func (r *fakeAgentRegistry) Get(
	ctx context.Context,
	domain actorrouting.RouteDomain,
	holderID string,
) (core.HolderRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return core.HolderRecord{}, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.getErr != nil {
		return core.HolderRecord{}, false, r.getErr
	}
	if r.record.HolderID == "" || r.record.Domain != domain || r.record.HolderID != holderID {
		return core.HolderRecord{}, false, nil
	}
	return cloneRecord(r.record), true, nil
}

func (r *fakeAgentRegistry) setFailAfter(value int) {
	r.mu.Lock()
	r.failAfter = value
	r.mu.Unlock()
}

type transitionCheckingAgentRegistry struct{ *fakeAgentRegistry }

func (r *transitionCheckingAgentRegistry) Heartbeat(ctx context.Context, input core.HolderHeartbeat) (core.HolderRecord, error) {
	r.mu.Lock()
	previous := r.record.State
	r.mu.Unlock()
	if !core.ValidHolderStateTransition(previous, input.State) {
		return core.HolderRecord{}, core.ErrHolderStateTransition
	}
	return r.fakeAgentRegistry.Heartbeat(ctx, input)
}

type fakeProcessStatus struct {
	mu     sync.Mutex
	status ProcessStatus
}

func (s *fakeProcessStatus) Status(context.Context) (ProcessStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status.ResourceSummary = cloneResourceSummary(s.status.ResourceSummary)
	return s.status, nil
}

func (s *fakeProcessStatus) set(status ProcessStatus) {
	s.mu.Lock()
	s.status = status
	s.mu.Unlock()
}

func TestHolderAgentRegistersExactDeploymentIdentityAndPreservesIt(t *testing.T) {
	status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, CapacityWeight: 1}}
	registry := &fakeAgentRegistry{failAfter: -1}
	now := time.UnixMilli(1_800_000_000_000)
	identity := holderidentity.DeploymentIdentity{PodUID: "pod-uid", WorkloadUID: "workload-uid", Ordinal: 2}
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: registry, ProcessStatus: status, Domain: actorrouting.DomainPlayer,
		HolderID: "holder-a", DeploymentIdentity: identity,
		LogicEndpoint: "holder-a.internal:9000", HolderControlEndpoint: "holder-a.internal:9443",
		TLSDNSIdentity: "holder-a.internal", ServerReady: func() bool { return true },
		BootID: func() (string, error) { return "boot-a", nil }, HeartbeatInterval: 5 * time.Second,
		HeartbeatTimeout: time.Second, CloseTimeout: time.Second, ClosePollInterval: time.Millisecond,
		LeaseTTL: 10 * time.Second, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		status.set(ProcessStatus{ProcessReady: false})
		_ = agent.Close(context.Background())
	}()
	if len(registry.registrations) != 1 || registry.registrations[0].DeploymentIdentity != identity {
		t.Fatalf("registrations=%+v", registry.registrations)
	}
	if got := agent.Status().Holder.DeploymentIdentity; got != identity {
		t.Fatalf("status identity=%+v", got)
	}
	bad := DefaultHolderAgentOptions()
	bad.Registry, bad.ProcessStatus = registry, status
	bad.Domain, bad.HolderID = actorrouting.DomainPlayer, "holder-b"
	bad.LogicEndpoint, bad.HolderControlEndpoint, bad.TLSDNSIdentity = "holder-b.internal:9000", "holder-b.internal:9443", "holder-b.internal"
	bad.DeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-only"}
	if _, err := NewHolderAgent(bad); !errors.Is(err, ErrAgentInvalid) {
		t.Fatalf("partial identity err=%v", err)
	}
}

func TestHolderAgentAdoptsExactMaterializedGenerationWithLaterLease(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	identity := holderidentity.DeploymentIdentity{PodUID: "pod-uid", WorkloadUID: "workload-uid", Ordinal: 2}
	durable := core.HolderRecord{
		Domain: actorrouting.DomainPlayer, HolderID: "holder-a", BootID: "boot-a",
		DeploymentIdentity: identity,
		LogicEndpoint:      "holder-a.internal:9000", HolderControlEndpoint: "holder-a.internal:9443",
		TLSDNSIdentity: "holder-a.internal", State: core.HolderStateWarming,
		// Discovery attested and materialized this exact generation just before
		// Agent constructed its uncommitted Register candidate. The durable
		// heartbeat timestamp may therefore be earlier; the longer durable lease
		// is the monotonic baseline adopted by the first real heartbeat.
		StartedAt: now.Add(-2 * time.Second), HeartbeatAt: now.Add(-time.Second), LeaseUntil: now.Add(11 * time.Second),
		Sequence: 1, CapacityWeight: 1,
	}
	base := &fakeAgentRegistry{record: cloneRecord(durable), failAfter: -1}
	registry := &materializedGenerationRegistry{
		base: base, durable: durable, registerErr: core.ErrHolderHeartbeatStale,
	}
	status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, CapacityWeight: 1}}
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: registry, ProcessStatus: status,
		Domain: actorrouting.DomainPlayer, HolderID: "holder-a", DeploymentIdentity: identity,
		LogicEndpoint: "holder-a.internal:9000", HolderControlEndpoint: "holder-a.internal:9443",
		TLSDNSIdentity: "holder-a.internal", ServerReady: func() bool { return true },
		BootID: func() (string, error) { return "boot-a", nil }, HeartbeatInterval: 5 * time.Second,
		HeartbeatTimeout: time.Second, CloseTimeout: time.Second, ClosePollInterval: time.Millisecond,
		LeaseTTL: 10 * time.Second, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if registry.registers != 1 || len(base.heartbeats) != 1 || base.heartbeats[0].Sequence != 2 {
		t.Fatalf("registers=%d heartbeats=%+v", registry.registers, base.heartbeats)
	}
	if got := agent.Status().Holder.BootID; got != "boot-a" {
		t.Fatalf("boot=%q", got)
	}
	status.set(ProcessStatus{ProcessReady: false, CapacityWeight: 1})
	if err := agent.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHolderAgentRejectsMaterializedDifferentGeneration(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	identity := holderidentity.DeploymentIdentity{PodUID: "pod-uid", WorkloadUID: "workload-uid", Ordinal: 2}
	durable := core.HolderRecord{
		Domain: actorrouting.DomainPlayer, HolderID: "holder-a", BootID: "boot-a",
		DeploymentIdentity: holderidentity.DeploymentIdentity{PodUID: "other-pod", WorkloadUID: "workload-uid", Ordinal: 2},
		LogicEndpoint:      "holder-a.internal:9000", HolderControlEndpoint: "holder-a.internal:9443",
		TLSDNSIdentity: "holder-a.internal", State: core.HolderStateWarming,
		StartedAt: now, HeartbeatAt: now.Add(time.Second), LeaseUntil: now.Add(11 * time.Second),
		Sequence: 1, CapacityWeight: 1,
	}
	base := &fakeAgentRegistry{record: cloneRecord(durable), failAfter: -1}
	registry := &materializedGenerationRegistry{
		base: base, durable: durable, registerErr: core.ErrHolderHeartbeatStale,
	}
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: registry, ProcessStatus: &fakeProcessStatus{status: ProcessStatus{CapacityWeight: 1}},
		Domain: actorrouting.DomainPlayer, HolderID: "holder-a", DeploymentIdentity: identity,
		LogicEndpoint: "holder-a.internal:9000", HolderControlEndpoint: "holder-a.internal:9443",
		TLSDNSIdentity: "holder-a.internal", ServerReady: func() bool { return true },
		BootID: func() (string, error) { return "boot-a", nil }, HeartbeatInterval: 5 * time.Second,
		HeartbeatTimeout: time.Second, CloseTimeout: time.Second, ClosePollInterval: time.Millisecond,
		LeaseTTL: 10 * time.Second, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Start(context.Background()); !errors.Is(err, ErrAgentUnhealthy) {
		t.Fatalf("err=%v", err)
	}
	if len(base.heartbeats) != 0 {
		t.Fatalf("foreign generation heartbeats=%+v", base.heartbeats)
	}
}

func TestHolderAgentStartsWarmingThenClosesThroughDrained(t *testing.T) {
	status := &fakeProcessStatus{status: ProcessStatus{
		ProcessReady: true, CapacityWeight: 4,
		ResourceSummary: map[string]string{"cpu": "2"},
	}}
	registry := &fakeAgentRegistry{failAfter: -1}
	now := time.UnixMilli(1_800_000_000_000)
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: registry, ProcessStatus: status,
		Domain: actorrouting.DomainPlayer, HolderID: "holder-a",
		LogicEndpoint: "holder-a.internal:9000", HolderControlEndpoint: "holder-a.internal:9000", TLSDNSIdentity: "holder-a.internal",
		ServerReady:       func() bool { return true },
		BootID:            func() (string, error) { return "boot-a", nil },
		HeartbeatInterval: 5 * time.Second, HeartbeatTimeout: time.Second,
		CloseTimeout: time.Second, ClosePollInterval: time.Millisecond,
		LeaseTTL: 10 * time.Second, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := agent.BootID(); got != "boot-a" {
		t.Fatalf("boot=%q", got)
	}
	if got := agent.Status().State; got != AgentStateRunning {
		t.Fatalf("state=%s", got)
	}
	if registry.registers != 1 || len(registry.heartbeats) != 1 ||
		registry.heartbeats[0].State != core.HolderStateReadyZeroShard {
		t.Fatalf("registers=%d heartbeats=%+v", registry.registers, registry.heartbeats)
	}
	if len(registry.registrations) != 1 || registry.registrations[0].ProcessReady ||
		registry.registrations[0].Draining || registry.registrations[0].ServingShardCount != 0 {
		t.Fatalf("WARMING registration flags=%+v", registry.registrations)
	}
	if err := agent.Start(context.Background()); err != nil {
		t.Fatalf("idempotent Start err=%v", err)
	}
	status.set(ProcessStatus{
		ProcessReady: true, ServingShardCount: 1, CapacityWeight: 4,
		ResourceSummary: map[string]string{"cpu": "2"},
	})
	if err := agent.heartbeatOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := agent.Status().Holder.State; got != core.HolderStateServing {
		t.Fatalf("serving state=%s", got)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- agent.Close(context.Background()) }()
	time.Sleep(5 * time.Millisecond)
	status.set(ProcessStatus{ProcessReady: false, ServingShardCount: 0, CapacityWeight: 4})
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not reach zero shards")
	}
	if got := agent.Status().State; got != AgentStateDrained {
		t.Fatalf("final state=%s", got)
	}
	if len(registry.heartbeats) < 4 ||
		registry.heartbeats[len(registry.heartbeats)-2].State != core.HolderStateDraining ||
		registry.heartbeats[len(registry.heartbeats)-1].State != core.HolderStateDrained {
		t.Fatalf("lifecycle heartbeats=%+v", registry.heartbeats)
	}
	if err := agent.Close(context.Background()); err != nil {
		t.Fatalf("idempotent Close err=%v", err)
	}
}

func TestHolderAgentNotifySnapshotAppliedHeartbeatsBeforeInterval(t *testing.T) {
	status := &fakeProcessStatus{status: ProcessStatus{
		ProcessReady: true, CapacityWeight: 1, SnapshotRevision: 3,
	}}
	registry := &fakeAgentRegistry{failAfter: -1}
	now := time.UnixMilli(1_800_000_000_000)
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: registry, ProcessStatus: status,
		Domain: actorrouting.DomainPlayer, HolderID: "holder-a",
		LogicEndpoint: "holder-a.internal:9000", HolderControlEndpoint: "holder-a.internal:9000", TLSDNSIdentity: "holder-a.internal",
		ServerReady:       func() bool { return true },
		BootID:            func() (string, error) { return "boot-a", nil },
		HeartbeatInterval: 5 * time.Second, HeartbeatTimeout: time.Second,
		CloseTimeout: time.Second, ClosePollInterval: time.Millisecond,
		LeaseTTL: 10 * time.Second, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(registry.heartbeats) != 1 {
		t.Fatalf("initial heartbeats=%d", len(registry.heartbeats))
	}
	status.set(ProcessStatus{ProcessReady: true, CapacityWeight: 1, SnapshotRevision: 9})
	agent.NotifySnapshotApplied()
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		registry.mu.Lock()
		n := len(registry.heartbeats)
		rev := uint64(0)
		if n > 0 {
			rev = registry.heartbeats[n-1].SnapshotRevision
		}
		registry.mu.Unlock()
		if n >= 2 && rev == 9 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot apply did not heartbeat before interval heartbeats=%d rev=%d", n, rev)
		}
		time.Sleep(5 * time.Millisecond)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- agent.Close(context.Background()) }()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish")
	}
}

func TestHolderAgentWaitsForPriorBootLeaseBeforeRegister(t *testing.T) {
	now := time.UnixMilli(time.Now().UnixMilli())
	base := &fakeAgentRegistry{failAfter: -1}
	registry := &expiringPriorBootRegistry{
		base: base,
		prior: core.HolderRecord{
			Domain:                actorrouting.DomainPlayer,
			HolderID:              "holder-a",
			BootID:                "prior-boot",
			LogicEndpoint:         "holder-a.internal:9000",
			HolderControlEndpoint: "holder-a.internal:9000",
			TLSDNSIdentity:        "holder-a.internal",
			State:                 core.HolderStateServing,
			ProcessReady:          true,
			ServingShardCount:     1,
			SnapshotRevision:      1,
			StartedAt:             now.Add(-time.Second),
			HeartbeatAt:           now,
			LeaseUntil:            now.Add(10 * time.Millisecond),
			Sequence:              2,
			CapacityWeight:        1,
		},
	}
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: registry,
		ProcessStatus: &fakeProcessStatus{status: ProcessStatus{
			ProcessReady: true, SnapshotRevision: 1, CapacityWeight: 1,
		}},
		Domain:                actorrouting.DomainPlayer,
		HolderID:              "holder-a",
		LogicEndpoint:         "holder-a.internal:9000",
		HolderControlEndpoint: "holder-a.internal:9000",
		TLSDNSIdentity:        "holder-a.internal",
		ServerReady:           func() bool { return true },
		BootID:                func() (string, error) { return "next-boot", nil },
		HeartbeatInterval:     time.Second,
		HeartbeatTimeout:      time.Second,
		CloseTimeout:          time.Second,
		ClosePollInterval:     time.Millisecond,
		LeaseTTL:              10 * time.Second,
		Now:                   time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if registry.registerCalls != 2 || agent.BootID() != "next-boot" {
		t.Fatalf(
			"register calls=%d boot=%q",
			registry.registerCalls,
			agent.BootID(),
		)
	}
	if err := agent.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// steppingClock reports a time the test moves forward on purpose, so a wait in
// the code under test can be given the duration it has in production.
type steppingClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *steppingClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *steppingClock) step(by time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(by)
	c.mu.Unlock()
}

// lateExpiringPriorBootRegistry refuses one attempt and spends a whole lease
// doing it, which is what a crash-restart meets: the dead boot's lease has to
// run out before this boot can take the name.
type lateExpiringPriorBootRegistry struct {
	base  *fakeAgentRegistry
	prior core.HolderRecord
	clock *steppingClock
	wait  time.Duration
	calls int
}

func (r *lateExpiringPriorBootRegistry) Register(
	ctx context.Context,
	input core.HolderRegistration,
) (core.HolderRecord, error) {
	r.calls++
	if r.calls == 1 {
		r.clock.step(r.wait)
		return core.HolderRecord{}, core.ErrHolderBootConflict
	}
	return r.base.Register(ctx, input)
}

func (r *lateExpiringPriorBootRegistry) Heartbeat(
	ctx context.Context,
	input core.HolderHeartbeat,
) (core.HolderRecord, error) {
	return r.base.Heartbeat(ctx, input)
}

func (r *lateExpiringPriorBootRegistry) Get(
	ctx context.Context,
	domain actorrouting.RouteDomain,
	holderID string,
) (core.HolderRecord, bool, error) {
	if r.calls == 1 {
		return cloneRecord(r.prior), true, nil
	}
	return r.base.Get(ctx, domain, holderID)
}

// A boot that waits out its predecessor's lease must still be able to register.
// The wait is longer than the registry's clock skew allowance by construction,
// because a lease outlasts that allowance, so judging the response against the
// clock as it was before the wait rejects the agent's own good registration.
// Getting this wrong turns one crash into a crash loop: each dead boot leaves a
// fresh lease for the next one to wait out.
func TestHolderAgentRegistersAfterWaitingOutALeaseLongerThanTheSkewAllowance(t *testing.T) {
	const leaseTTL = 10 * time.Second
	clock := &steppingClock{now: time.UnixMilli(1_800_000_000_000)}
	start := clock.Now()
	registry := &lateExpiringPriorBootRegistry{
		base:  &fakeAgentRegistry{failAfter: -1},
		clock: clock,
		wait:  leaseTTL + 2*time.Second,
		prior: core.HolderRecord{
			Domain:                actorrouting.DomainPlayer,
			HolderID:              "holder-slow",
			BootID:                "prior-boot",
			LogicEndpoint:         "holder-slow.internal:9000",
			HolderControlEndpoint: "holder-slow.internal:9000",
			TLSDNSIdentity:        "holder-slow.internal",
			State:                 core.HolderStateServing,
			ProcessReady:          true,
			ServingShardCount:     1,
			SnapshotRevision:      1,
			StartedAt:             start.Add(-time.Second),
			HeartbeatAt:           start,
			LeaseUntil:            start.Add(leaseTTL),
			Sequence:              2,
			CapacityWeight:        1,
		},
	}
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: registry,
		ProcessStatus: &fakeProcessStatus{status: ProcessStatus{
			ProcessReady: true, SnapshotRevision: 1, CapacityWeight: 1,
		}},
		Domain:                actorrouting.DomainPlayer,
		HolderID:              "holder-slow",
		LogicEndpoint:         "holder-slow.internal:9000",
		HolderControlEndpoint: "holder-slow.internal:9000",
		TLSDNSIdentity:        "holder-slow.internal",
		ServerReady:           func() bool { return true },
		BootID:                func() (string, error) { return "next-boot", nil },
		HeartbeatInterval:     time.Second,
		HeartbeatTimeout:      time.Second,
		CloseTimeout:          time.Second,
		ClosePollInterval:     time.Millisecond,
		LeaseTTL:              leaseTTL,
		Now:                   clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Start(context.Background()); err != nil {
		t.Fatalf("registration refused after waiting out the prior lease: %v", err)
	}
	if registry.calls != 2 || agent.BootID() != "next-boot" {
		t.Fatalf("register calls=%d boot=%q", registry.calls, agent.BootID())
	}
	if err := agent.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHolderAgentReadyZeroShardRecoversAfterTransientAssignmentOutage(t *testing.T) {
	status := &fakeProcessStatus{status: ProcessStatus{
		ProcessReady: true, CapacityWeight: 1,
	}}
	registry := &fakeAgentRegistry{failAfter: -1}
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: registry, ProcessStatus: status,
		Domain:                actorrouting.DomainPlayer,
		HolderID:              "holder-ready-zero-recovery",
		LogicEndpoint:         "holder-ready-zero-recovery.internal:9000",
		HolderControlEndpoint: "holder-ready-zero-recovery.internal:9001",
		TLSDNSIdentity:        "holder-ready-zero-recovery.internal",
		ServerReady:           func() bool { return true },
		BootID:                func() (string, error) { return "boot-ready-zero-recovery", nil },
		HeartbeatInterval:     5 * time.Second, HeartbeatTimeout: time.Second,
		CloseTimeout: time.Second, ClosePollInterval: time.Millisecond,
		LeaseTTL: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(registry.heartbeats) != 1 ||
		registry.heartbeats[0].State != core.HolderStateReadyZeroShard {
		t.Fatalf("initial heartbeats=%+v", registry.heartbeats)
	}

	status.set(ProcessStatus{ProcessReady: false, CapacityWeight: 1})
	if err := agent.heartbeatOnce(context.Background()); err != nil {
		t.Fatalf("transient outage heartbeat err=%v", err)
	}
	if got := agent.Status(); got.State != AgentStateRunning || got.Unhealthy {
		t.Fatalf("transient outage made Agent unhealthy: %+v", got)
	}
	if len(registry.heartbeats) != 1 {
		t.Fatalf("transient outage mutated durable state: %+v", registry.heartbeats)
	}

	status.set(ProcessStatus{ProcessReady: true, CapacityWeight: 1})
	if err := agent.heartbeatOnce(context.Background()); err != nil {
		t.Fatalf("recovery heartbeat err=%v", err)
	}
	if len(registry.heartbeats) != 2 ||
		registry.heartbeats[1].State != core.HolderStateReadyZeroShard {
		t.Fatalf("recovery heartbeats=%+v", registry.heartbeats)
	}
	if err := agent.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHolderAgentDefersReadyZeroShardUntilNewerSnapshotRevision(t *testing.T) {
	status := &fakeProcessStatus{status: ProcessStatus{
		ProcessReady: true, ServingShardCount: 2, SnapshotRevision: 2, CapacityWeight: 1,
	}}
	registry := &fakeAgentRegistry{failAfter: -1}
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: registry, ProcessStatus: status,
		Domain: actorrouting.DomainPlayer, HolderID: "holder-proving-snapshot",
		LogicEndpoint: "holder-proving-snapshot.internal:9000", HolderControlEndpoint: "holder-proving-snapshot.internal:9001", TLSDNSIdentity: "holder-proving-snapshot.internal",
		AllowInitialServing: true, ServerReady: func() bool { return true },
		BootID:            func() (string, error) { return "boot-proving-snapshot", nil },
		HeartbeatInterval: 5 * time.Second, HeartbeatTimeout: time.Second,
		CloseTimeout: time.Second, ClosePollInterval: time.Millisecond,
		LeaseTTL: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(registry.heartbeats) != 1 ||
		registry.heartbeats[0].State != core.HolderStateServing ||
		registry.heartbeats[0].SnapshotRevision != 2 {
		t.Fatalf("initial serving heartbeats=%+v", registry.heartbeats)
	}

	// The status source withholds the revision that already emptied this
	// holder, so a still-serving observation carries no new evidence.
	status.set(ProcessStatus{ProcessReady: true, ServingShardCount: 1, SnapshotRevision: 0, CapacityWeight: 1})
	if err := agent.heartbeatOnce(context.Background()); err != nil {
		t.Fatalf("withheld-evidence serving heartbeat err=%v", err)
	}
	if len(registry.heartbeats) != 2 ||
		registry.heartbeats[1].State != core.HolderStateServing ||
		registry.heartbeats[1].SnapshotRevision != 2 {
		t.Fatalf("withheld evidence changed the durable revision: %+v", registry.heartbeats)
	}

	// Zero shards without a newer revision is an illegal Redis transition; it
	// must be skipped instead of failing the Agent.
	status.set(ProcessStatus{ProcessReady: true, ServingShardCount: 0, SnapshotRevision: 0, CapacityWeight: 1})
	if err := agent.heartbeatOnce(context.Background()); err != nil {
		t.Fatalf("deferred zero-shard heartbeat err=%v", err)
	}
	if len(registry.heartbeats) != 2 {
		t.Fatalf("published zero shards without a newer revision: %+v", registry.heartbeats)
	}
	if got := agent.Status(); got.State != AgentStateRunning || got.Unhealthy {
		t.Fatalf("deferred transition made Agent unhealthy: %+v", got)
	}

	status.set(ProcessStatus{ProcessReady: true, ServingShardCount: 0, SnapshotRevision: 9, CapacityWeight: 1})
	if err := agent.heartbeatOnce(context.Background()); err != nil {
		t.Fatalf("zero-shard heartbeat err=%v", err)
	}
	if len(registry.heartbeats) != 3 ||
		registry.heartbeats[2].State != core.HolderStateReadyZeroShard ||
		registry.heartbeats[2].SnapshotRevision != 9 ||
		registry.heartbeats[2].ServingShardCount != 0 {
		t.Fatalf("ready zero shard heartbeats=%+v", registry.heartbeats)
	}
	if got := agent.Status(); got.State != AgentStateRunning || got.Unhealthy {
		t.Fatalf("zero-shard transition made Agent unhealthy: %+v", got)
	}
	if err := agent.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHolderAgentServingLastShardDefersTransientUnready(t *testing.T) {
	status := &fakeProcessStatus{status: ProcessStatus{
		ProcessReady: true, ServingShardCount: 1, SnapshotRevision: 2, CapacityWeight: 1,
	}}
	registry := &fakeAgentRegistry{failAfter: -1}
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: registry, ProcessStatus: status,
		Domain: actorrouting.DomainPlayer, HolderID: "holder-last-shard-unready",
		LogicEndpoint: "holder-last-shard-unready.internal:9000", HolderControlEndpoint: "holder-last-shard-unready.internal:9001", TLSDNSIdentity: "holder-last-shard-unready.internal",
		AllowInitialServing: true, ServerReady: func() bool { return true },
		BootID:            func() (string, error) { return "boot-last-shard-unready", nil },
		HeartbeatInterval: 5 * time.Second, HeartbeatTimeout: time.Second,
		CloseTimeout: time.Second, ClosePollInterval: time.Millisecond,
		LeaseTTL: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	status.set(ProcessStatus{ProcessReady: false, ServingShardCount: 0, SnapshotRevision: 9, CapacityWeight: 1})
	if err := agent.heartbeatOnce(context.Background()); err != nil {
		t.Fatalf("transient unready zero-shard heartbeat err=%v", err)
	}
	if got := agent.Status(); got.State != AgentStateRunning || got.Unhealthy {
		t.Fatalf("transient unready made Agent unhealthy: %+v", got)
	}
	if len(registry.heartbeats) != 1 || registry.heartbeats[0].State != core.HolderStateServing {
		t.Fatalf("transient unready mutated durable state: %+v", registry.heartbeats)
	}

	status.set(ProcessStatus{ProcessReady: true, ServingShardCount: 0, SnapshotRevision: 9, CapacityWeight: 1})
	if err := agent.heartbeatOnce(context.Background()); err != nil {
		t.Fatalf("recovery zero-shard heartbeat err=%v", err)
	}
	if len(registry.heartbeats) != 2 ||
		registry.heartbeats[1].State != core.HolderStateReadyZeroShard ||
		registry.heartbeats[1].SnapshotRevision != 9 {
		t.Fatalf("recovery heartbeats=%+v", registry.heartbeats)
	}
	if err := agent.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHolderAgentExplicitStaticBootstrapStartsWithFactualServingCount(t *testing.T) {
	status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, ServingShardCount: 17, CapacityWeight: 2}}
	registry := &fakeAgentRegistry{failAfter: -1}
	now := time.UnixMilli(1_800_000_000_000)
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: registry, ProcessStatus: status,
		Domain: actorrouting.DomainPlayer, HolderID: "holder-static",
		LogicEndpoint: "holder-static.internal:9000", HolderControlEndpoint: "holder-static.internal:9001", TLSDNSIdentity: "holder-static.internal",
		AllowInitialServing: true, ServerReady: func() bool { return true },
		BootID:            func() (string, error) { return "boot-static", nil },
		HeartbeatInterval: 5 * time.Second, HeartbeatTimeout: time.Second,
		CloseTimeout: time.Second, ClosePollInterval: time.Millisecond,
		LeaseTTL: 10 * time.Second, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(registry.heartbeats) != 1 || registry.heartbeats[0].State != core.HolderStateServing ||
		registry.heartbeats[0].ServingShardCount != 17 || !registry.heartbeats[0].ProcessReady {
		t.Fatalf("static bootstrap heartbeat=%+v", registry.heartbeats)
	}
	status.set(ProcessStatus{ProcessReady: false, Draining: true, CapacityWeight: 2})
	if err := agent.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHolderAgentLeaseBudgetConstructionBoundaries(t *testing.T) {
	baseOptions := func() HolderAgentOptions {
		return HolderAgentOptions{
			Registry: &fakeAgentRegistry{failAfter: -1},
			ProcessStatus: &fakeProcessStatus{status: ProcessStatus{
				ProcessReady: true, CapacityWeight: 1,
			}},
			Domain: actorrouting.DomainPlayer, HolderID: "holder-budget",
			LogicEndpoint: "holder-budget.internal:9000", HolderControlEndpoint: "holder-budget.internal:9000", TLSDNSIdentity: "holder-budget.internal",
			HeartbeatInterval: time.Millisecond, HeartbeatTimeout: 2 * time.Millisecond,
			CloseTimeout: 10 * time.Millisecond, ClosePollInterval: time.Millisecond,
			LeaseTTL: 6 * time.Millisecond,
		}
	}
	tests := []struct {
		name   string
		adjust func(*HolderAgentOptions)
		valid  bool
	}{
		{name: "both-strictly-under", valid: true},
		{name: "heartbeat-equal", adjust: func(options *HolderAgentOptions) {
			options.HeartbeatInterval = 2 * time.Millisecond
		}, valid: false},
		{name: "heartbeat-over", adjust: func(options *HolderAgentOptions) {
			options.HeartbeatInterval = 3 * time.Millisecond
		}, valid: false},
		{name: "drain-equal", adjust: func(options *HolderAgentOptions) {
			options.LeaseTTL = 7 * time.Millisecond
			options.ClosePollInterval = 3 * time.Millisecond
		}, valid: false},
		{name: "drain-over", adjust: func(options *HolderAgentOptions) {
			options.ClosePollInterval = 4 * time.Millisecond
		}, valid: false},
		{name: "addition-overflow-cannot-pass", adjust: func(options *HolderAgentOptions) {
			options.HeartbeatInterval = time.Duration(1<<63-1) - time.Millisecond
			options.HeartbeatTimeout = time.Millisecond
			options.LeaseTTL = time.Duration(1<<63 - 1)
		}, valid: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := baseOptions()
			if test.adjust != nil {
				test.adjust(&options)
			}
			_, err := NewHolderAgent(options)
			if test.valid && err != nil {
				t.Fatalf("valid budget rejected: %v", err)
			}
			if !test.valid && !errors.Is(err, ErrAgentInvalid) {
				t.Fatalf("unsafe budget err=%v", err)
			}
		})
	}

	defaults := DefaultHolderAgentOptions()
	defaults.Registry = &fakeAgentRegistry{failAfter: -1}
	defaults.ProcessStatus = &fakeProcessStatus{status: ProcessStatus{ProcessReady: true}}
	defaults.Domain = actorrouting.DomainPlayer
	defaults.HolderID = "holder-default-budget"
	defaults.LogicEndpoint = "holder-default-budget.internal:9000"
	defaults.HolderControlEndpoint = "holder-default-budget.internal:9000"
	defaults.TLSDNSIdentity = "holder-default-budget.internal"
	if _, err := NewHolderAgent(defaults); err != nil {
		t.Fatalf("default lease budget became invalid: %v", err)
	}
}

func TestHolderAgentStickyUnhealthyDoesNotPublishDrained(t *testing.T) {
	status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, CapacityWeight: 1}}
	registry := &fakeAgentRegistry{failAfter: 0, getErr: errors.New("registry Get unavailable")}
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: registry, ProcessStatus: status,
		Domain: actorrouting.DomainFarm, HolderID: "farm-a",
		LogicEndpoint: "farm-a.internal:9000", HolderControlEndpoint: "farm-a.internal:9000", TLSDNSIdentity: "farm-a.internal",
		BootID:                      func() (string, error) { return "boot-a", nil },
		ConsecutiveFailureThreshold: 1, HeartbeatTimeout: time.Second,
		LeaseTTL: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Start(context.Background()); err == nil {
		t.Fatal("registry failure unexpectedly allowed Start")
	}
	if got := agent.Status(); !got.Unhealthy || got.State != AgentStateUnhealthy {
		t.Fatalf("status=%+v", got)
	}
	if len(registry.heartbeats) != 0 {
		t.Fatalf("failed heartbeat was recorded: %+v", registry.heartbeats)
	}
	if err := agent.Close(context.Background()); !errors.Is(err, ErrAgentUnhealthy) {
		t.Fatalf("unhealthy Close err=%v", err)
	}
	for _, heartbeat := range registry.heartbeats {
		if heartbeat.State == core.HolderStateDrained {
			t.Fatal("unhealthy agent published DRAINED")
		}
	}
}

type blockingAgentRegistry struct {
	base            *fakeAgentRegistry
	registerEntered chan struct{}
	releaseRegister chan struct{}
	registerOnce    sync.Once
}

type stubbornBlockingAgentRegistry struct {
	base            *fakeAgentRegistry
	registerEntered chan struct{}
	releaseRegister chan struct{}
	registerOnce    sync.Once
}

type closeDeadlineGetRegistry struct {
	base       *fakeAgentRegistry
	mu         sync.Mutex
	heartbeats int
	anomaly    error
	getBudget  chan time.Duration
}

func (r *closeDeadlineGetRegistry) Register(ctx context.Context, input core.HolderRegistration) (core.HolderRecord, error) {
	return r.base.Register(ctx, input)
}

func (r *closeDeadlineGetRegistry) Heartbeat(ctx context.Context, input core.HolderHeartbeat) (core.HolderRecord, error) {
	r.mu.Lock()
	r.heartbeats++
	call := r.heartbeats
	r.mu.Unlock()
	if call > 1 {
		return core.HolderRecord{}, r.anomaly
	}
	return r.base.Heartbeat(ctx, input)
}

func (r *closeDeadlineGetRegistry) Get(
	ctx context.Context,
	domain actorrouting.RouteDomain,
	holderID string,
) (core.HolderRecord, bool, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return core.HolderRecord{}, false, errors.New("reconcile Get missing deadline")
	}
	select {
	case r.getBudget <- time.Until(deadline):
	default:
	}
	<-ctx.Done()
	return core.HolderRecord{}, false, ctx.Err()
}

func (r *stubbornBlockingAgentRegistry) Register(_ context.Context, input core.HolderRegistration) (core.HolderRecord, error) {
	r.registerOnce.Do(func() { close(r.registerEntered) })
	<-r.releaseRegister
	return r.base.Register(context.Background(), input)
}

func (r *stubbornBlockingAgentRegistry) Heartbeat(ctx context.Context, input core.HolderHeartbeat) (core.HolderRecord, error) {
	return r.base.Heartbeat(ctx, input)
}

func (r *stubbornBlockingAgentRegistry) Get(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (core.HolderRecord, bool, error) {
	return r.base.Get(ctx, domain, holderID)
}

type heartbeatResponseDriftRegistry struct {
	base         *fakeAgentRegistry
	mu           sync.Mutex
	calls        int
	shouldMutate func(int) bool
}

func (r *heartbeatResponseDriftRegistry) Register(ctx context.Context, input core.HolderRegistration) (core.HolderRecord, error) {
	return r.base.Register(ctx, input)
}

func (r *heartbeatResponseDriftRegistry) Heartbeat(ctx context.Context, input core.HolderHeartbeat) (core.HolderRecord, error) {
	record, err := r.base.Heartbeat(ctx, input)
	if err != nil {
		return core.HolderRecord{}, err
	}
	r.mu.Lock()
	r.calls++
	call := r.calls
	mutate := r.shouldMutate != nil && r.shouldMutate(call)
	r.mu.Unlock()
	if mutate {
		record.ResourceSummary = map[string]string{"response": "drift"}
	}
	return record, nil
}

func (r *heartbeatResponseDriftRegistry) Get(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (core.HolderRecord, bool, error) {
	return r.base.Get(ctx, domain, holderID)
}

func (r *blockingAgentRegistry) Register(ctx context.Context, input core.HolderRegistration) (core.HolderRecord, error) {
	r.registerOnce.Do(func() { close(r.registerEntered) })
	select {
	case <-r.releaseRegister:
	case <-ctx.Done():
		return core.HolderRecord{}, ctx.Err()
	}
	return r.base.Register(ctx, input)
}

func (r *blockingAgentRegistry) Heartbeat(ctx context.Context, input core.HolderHeartbeat) (core.HolderRecord, error) {
	return r.base.Heartbeat(ctx, input)
}

func (r *blockingAgentRegistry) Get(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (core.HolderRecord, bool, error) {
	return r.base.Get(ctx, domain, holderID)
}

type committedHeartbeatBlockRegistry struct {
	base      *fakeAgentRegistry
	mu        sync.Mutex
	calls     int
	blockAt   int
	committed chan uint64
	release   chan struct{}
}

func (r *committedHeartbeatBlockRegistry) Register(ctx context.Context, input core.HolderRegistration) (core.HolderRecord, error) {
	return r.base.Register(ctx, input)
}

func (r *committedHeartbeatBlockRegistry) Heartbeat(ctx context.Context, input core.HolderHeartbeat) (core.HolderRecord, error) {
	record, err := r.base.Heartbeat(ctx, input)
	if err != nil {
		return core.HolderRecord{}, err
	}
	r.mu.Lock()
	r.calls++
	shouldBlock := r.calls == r.blockAt
	r.mu.Unlock()
	if shouldBlock {
		r.committed <- input.Sequence
		<-r.release
	}
	return record, nil
}

func (r *committedHeartbeatBlockRegistry) Get(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (core.HolderRecord, bool, error) {
	return r.base.Get(ctx, domain, holderID)
}

type inflightCommitCancelRegistry struct {
	base         *fakeAgentRegistry
	mu           sync.Mutex
	heartbeats   int
	committed    chan struct{}
	getEntered   chan struct{}
	releaseGet   chan struct{}
	getDeadline  chan time.Time
	commitSecond bool
	commitOnce   sync.Once
	getEnterOnce sync.Once
}

func (r *inflightCommitCancelRegistry) Register(ctx context.Context, input core.HolderRegistration) (core.HolderRecord, error) {
	return r.base.Register(ctx, input)
}

func (r *inflightCommitCancelRegistry) Heartbeat(ctx context.Context, input core.HolderHeartbeat) (core.HolderRecord, error) {
	r.mu.Lock()
	r.heartbeats++
	call := r.heartbeats
	r.mu.Unlock()
	if call == 2 && !r.commitSecond {
		r.commitOnce.Do(func() { close(r.committed) })
		<-ctx.Done()
		return core.HolderRecord{}, ctx.Err()
	}
	record, err := r.base.Heartbeat(ctx, input)
	if err != nil {
		return core.HolderRecord{}, err
	}
	if call == 2 {
		r.commitOnce.Do(func() { close(r.committed) })
		<-ctx.Done()
		return core.HolderRecord{}, ctx.Err()
	}
	return record, nil
}

func (r *inflightCommitCancelRegistry) Get(
	ctx context.Context,
	domain actorrouting.RouteDomain,
	holderID string,
) (core.HolderRecord, bool, error) {
	if deadline, ok := ctx.Deadline(); ok {
		select {
		case r.getDeadline <- deadline:
		default:
		}
	}
	r.getEnterOnce.Do(func() { close(r.getEntered) })
	select {
	case <-r.releaseGet:
	case <-ctx.Done():
		return core.HolderRecord{}, false, ctx.Err()
	}
	return r.base.Get(ctx, domain, holderID)
}

type hardBoundAgentDependencies struct {
	base     *fakeAgentRegistry
	mode     string
	entered  chan struct{}
	release  chan struct{}
	returned chan struct{}

	mu             sync.Mutex
	statusCalls    int
	heartbeatCalls int
	getCalls       int
	enterOnce      sync.Once
	returnOnce     sync.Once
}

func (d *hardBoundAgentDependencies) signalEntered() {
	d.enterOnce.Do(func() { close(d.entered) })
}

func (d *hardBoundAgentDependencies) waitIgnoringContext() {
	d.signalEntered()
	<-d.release
	d.returnOnce.Do(func() { close(d.returned) })
}

func (d *hardBoundAgentDependencies) Status(context.Context) (ProcessStatus, error) {
	d.mu.Lock()
	d.statusCalls++
	call := d.statusCalls
	d.mu.Unlock()
	if d.mode == "status" && call == 3 {
		d.waitIgnoringContext()
	}
	return ProcessStatus{ProcessReady: true, CapacityWeight: 1}, nil
}

func (d *hardBoundAgentDependencies) Register(ctx context.Context, input core.HolderRegistration) (core.HolderRecord, error) {
	return d.base.Register(ctx, input)
}

func (d *hardBoundAgentDependencies) Heartbeat(ctx context.Context, input core.HolderHeartbeat) (core.HolderRecord, error) {
	d.mu.Lock()
	d.heartbeatCalls++
	call := d.heartbeatCalls
	d.mu.Unlock()
	record, err := d.base.Heartbeat(ctx, input)
	if err != nil {
		return core.HolderRecord{}, err
	}
	if d.mode == "heartbeat" && call == 2 {
		d.waitIgnoringContext()
		return record, nil
	}
	if d.mode == "get" && call == 2 {
		return core.HolderRecord{}, errors.New("committed close heartbeat response lost")
	}
	return record, nil
}

func (d *hardBoundAgentDependencies) Get(
	_ context.Context,
	domain actorrouting.RouteDomain,
	holderID string,
) (core.HolderRecord, bool, error) {
	d.mu.Lock()
	d.getCalls++
	call := d.getCalls
	d.mu.Unlock()
	if d.mode == "get" && call == 1 {
		d.waitIgnoringContext()
	}
	return d.base.Get(context.Background(), domain, holderID)
}

func (d *hardBoundAgentDependencies) counts() (status, heartbeat, get int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.statusCalls, d.heartbeatCalls, d.getCalls
}

type registrationResponseRegistry struct {
	base   *fakeAgentRegistry
	mutate func(*core.HolderRecord)
}

type commitThenErrorRegistry struct {
	base                  *fakeAgentRegistry
	registerErr           error
	heartbeatErrorBefore  map[int]error
	heartbeatErrorAt      map[int]error
	heartbeatAfterCommit  map[int]func()
	heartbeatResponseAt   map[int]func(*core.HolderRecord)
	mu                    sync.Mutex
	heartbeatCalls        int
	reconciliationGetErr  error
	reconciliationMissing bool
	reconciliationGetEdit func(*core.HolderRecord)
}

func (r *commitThenErrorRegistry) Register(ctx context.Context, input core.HolderRegistration) (core.HolderRecord, error) {
	record, err := r.base.Register(ctx, input)
	if err != nil {
		return core.HolderRecord{}, err
	}
	if r.registerErr != nil {
		return core.HolderRecord{}, r.registerErr
	}
	return record, nil
}

func (r *commitThenErrorRegistry) Heartbeat(ctx context.Context, input core.HolderHeartbeat) (core.HolderRecord, error) {
	r.mu.Lock()
	r.heartbeatCalls++
	call := r.heartbeatCalls
	beforeErr := r.heartbeatErrorBefore[call]
	callErr := r.heartbeatErrorAt[call]
	afterCommit := r.heartbeatAfterCommit[call]
	mutate := r.heartbeatResponseAt[call]
	r.mu.Unlock()
	if beforeErr != nil {
		return core.HolderRecord{}, beforeErr
	}
	record, err := r.base.Heartbeat(ctx, input)
	if err != nil {
		return core.HolderRecord{}, err
	}
	if afterCommit != nil {
		afterCommit()
	}
	if callErr != nil {
		return core.HolderRecord{}, callErr
	}
	if mutate != nil {
		mutate(&record)
	}
	return record, nil
}

func (r *commitThenErrorRegistry) Get(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (core.HolderRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return core.HolderRecord{}, false, err
	}
	if r.reconciliationGetErr != nil {
		return core.HolderRecord{}, false, r.reconciliationGetErr
	}
	if r.reconciliationMissing {
		return core.HolderRecord{}, false, nil
	}
	record, found, err := r.base.Get(ctx, domain, holderID)
	if err == nil && found && r.reconciliationGetEdit != nil {
		r.reconciliationGetEdit(&record)
	}
	return record, found, err
}

func (r *registrationResponseRegistry) Register(
	ctx context.Context,
	input core.HolderRegistration,
) (core.HolderRecord, error) {
	record, err := r.base.Register(ctx, input)
	if err == nil && r.mutate != nil {
		r.mutate(&record)
	}
	return record, err
}

func (r *registrationResponseRegistry) Heartbeat(
	ctx context.Context,
	input core.HolderHeartbeat,
) (core.HolderRecord, error) {
	return r.base.Heartbeat(ctx, input)
}

func (r *registrationResponseRegistry) Get(
	ctx context.Context,
	domain actorrouting.RouteDomain,
	holderID string,
) (core.HolderRecord, bool, error) {
	return r.base.Get(ctx, domain, holderID)
}

func newTestAgent(registry HolderRegistryPort, status ProcessStatusSource) *HolderAgent {
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: registry, ProcessStatus: status,
		Domain: actorrouting.DomainPlayer, HolderID: "holder-a",
		LogicEndpoint: "holder-a.internal:9000", HolderControlEndpoint: "holder-a.internal:9000", TLSDNSIdentity: "holder-a.internal",
		BootID:            func() (string, error) { return "boot-a", nil },
		HeartbeatInterval: 5 * time.Second, HeartbeatTimeout: time.Second,
		CloseTimeout: time.Second, ClosePollInterval: time.Millisecond,
		LeaseTTL: 10 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	return agent
}

func newHardBoundTestAgent(
	t *testing.T,
	dependencies *hardBoundAgentDependencies,
	closeTimeout time.Duration,
) *HolderAgent {
	t.Helper()
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: dependencies, ProcessStatus: dependencies,
		Domain: actorrouting.DomainPlayer, HolderID: "holder-hard-bound-" + dependencies.mode,
		LogicEndpoint: "holder-hard-bound.internal:9000", HolderControlEndpoint: "holder-hard-bound.internal:9000", TLSDNSIdentity: "holder-hard-bound.internal",
		BootID:            func() (string, error) { return "boot-hard-bound", nil },
		HeartbeatInterval: 200 * time.Millisecond,
		HeartbeatTimeout:  250 * time.Millisecond,
		CloseTimeout:      closeTimeout,
		ClosePollInterval: 2 * time.Millisecond,
		LeaseTTL:          time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return agent
}

func TestHolderAgentConcurrentStartSingleflightAndCloseConverges(t *testing.T) {
	status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, CapacityWeight: 1}}
	base := &fakeAgentRegistry{failAfter: -1}
	registry := &blockingAgentRegistry{
		base:            base,
		registerEntered: make(chan struct{}),
		releaseRegister: make(chan struct{}),
	}
	agent := newTestAgent(registry, status)
	startOne := make(chan error, 1)
	startTwo := make(chan error, 1)
	go func() { startOne <- agent.Start(context.Background()) }()
	<-registry.registerEntered
	go func() { startTwo <- agent.Start(context.Background()) }()
	closeResult := make(chan error, 1)
	go func() { closeResult <- agent.Close(context.Background()) }()
	select {
	case err := <-closeResult:
		t.Fatalf("Close completed before Start converged: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(registry.releaseRegister)
	if err := <-startOne; !errors.Is(err, ErrAgentClosed) {
		t.Fatalf("first Start err=%v", err)
	}
	if err := <-startTwo; !errors.Is(err, ErrAgentClosed) {
		t.Fatalf("second Start err=%v", err)
	}
	if err := <-closeResult; err != nil {
		t.Fatalf("Close err=%v", err)
	}
	if base.registers != 1 {
		t.Fatalf("registers=%d", base.registers)
	}
	if len(base.heartbeats) < 2 {
		t.Fatalf("heartbeats=%+v", base.heartbeats)
	}
	states := make([]core.HolderState, 0, len(base.heartbeats))
	for _, heartbeat := range base.heartbeats {
		states = append(states, heartbeat.State)
		if heartbeat.State == core.HolderStateReadyZeroShard {
			t.Fatalf("Close won during Register but READY_ZERO_SHARD was published: %v", states)
		}
	}
	if states[len(states)-2] != core.HolderStateDraining || states[len(states)-1] != core.HolderStateDrained {
		t.Fatalf("close states=%v", states)
	}
}

func TestHolderAgentCloseCallerTimeoutLeavesIndependentStartingCleanupOwner(t *testing.T) {
	status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, CapacityWeight: 1}}
	base := &fakeAgentRegistry{failAfter: -1}
	registry := &stubbornBlockingAgentRegistry{
		base: base, registerEntered: make(chan struct{}), releaseRegister: make(chan struct{}),
	}
	agent := newTestAgent(registry, status)
	startResult := make(chan error, 1)
	go func() { startResult <- agent.Start(context.Background()) }()
	<-registry.registerEntered

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := agent.Close(closeCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed Close err=%v", err)
	}
	close(registry.releaseRegister)
	if err := <-startResult; !errors.Is(err, ErrAgentClosed) {
		t.Fatalf("Start after Close request err=%v", err)
	}
	if err := agent.Close(context.Background()); err != nil {
		t.Fatalf("internal close worker err=%v", err)
	}
	base.mu.Lock()
	heartbeats := append([]core.HolderHeartbeat(nil), base.heartbeats...)
	base.mu.Unlock()
	if len(heartbeats) != 2 || heartbeats[0].State != core.HolderStateDraining ||
		heartbeats[1].State != core.HolderStateDrained {
		t.Fatalf("post-Register cleanup order=%+v", heartbeats)
	}
	for _, heartbeat := range heartbeats {
		if heartbeat.State == core.HolderStateReadyZeroShard {
			t.Fatalf("Close-before-Register-return published READY_ZERO_SHARD: %+v", heartbeats)
		}
	}
	if got := agent.Status(); got.State != AgentStateDrained {
		t.Fatalf("cleanup did not complete status=%+v", got)
	}
}

func TestHolderAgentCloseWorkerTimeoutBoundsStartAttemptWait(t *testing.T) {
	newAgent := func(registry HolderRegistryPort, suffix string) *HolderAgent {
		agent, err := NewHolderAgent(HolderAgentOptions{
			Registry: registry,
			ProcessStatus: &fakeProcessStatus{status: ProcessStatus{
				ProcessReady: true, CapacityWeight: 1,
			}},
			Domain: actorrouting.DomainPlayer, HolderID: "holder-stubborn-" + suffix,
			LogicEndpoint:         "holder-stubborn-" + suffix + ".internal:9000",
			HolderControlEndpoint: "holder-stubborn-" + suffix + ".internal:9000",
			TLSDNSIdentity:        "holder-stubborn-" + suffix + ".internal",
			BootID:                func() (string, error) { return "boot-stubborn-" + suffix, nil },
			HeartbeatInterval:     20 * time.Millisecond,
			HeartbeatTimeout:      5 * time.Millisecond,
			CloseTimeout:          30 * time.Millisecond,
			ClosePollInterval:     10 * time.Millisecond,
			LeaseTTL:              100 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		return agent
	}

	t.Run("never-released-register", func(t *testing.T) {
		base := &fakeAgentRegistry{failAfter: -1}
		registry := &stubbornBlockingAgentRegistry{
			base: base, registerEntered: make(chan struct{}), releaseRegister: make(chan struct{}),
		}
		agent := newAgent(registry, "never")
		go func() { _ = agent.Start(context.Background()) }()
		<-registry.registerEntered
		started := time.Now()
		err := agent.Close(context.Background())
		elapsed := time.Since(started)
		if !errors.Is(err, ErrAgentCloseTimeout) || !errors.Is(err, ErrAgentUnhealthy) {
			t.Fatalf("bounded Close err=%v", err)
		}
		if elapsed > 100*time.Millisecond {
			t.Fatalf("Close waited beyond worker budget elapsed=%v", elapsed)
		}
		agent.mu.Lock()
		closeDone := agent.closeDone
		attemptDone := agent.startAttempt.done
		complete := agent.closeComplete
		runStarted := agent.runStarted
		agent.mu.Unlock()
		select {
		case <-closeDone:
		default:
			t.Fatal("background closeDone still waits on stubborn Register")
		}
		select {
		case <-attemptDone:
		default:
			t.Fatal("Start attempt waiters still depend on stubborn Register release")
		}
		if !complete || runStarted {
			t.Fatalf("closeComplete=%t runStarted=%t", complete, runStarted)
		}
	})

	t.Run("late-register-commit-cannot-reopen", func(t *testing.T) {
		base := &fakeAgentRegistry{failAfter: -1}
		registry := &stubbornBlockingAgentRegistry{
			base: base, registerEntered: make(chan struct{}), releaseRegister: make(chan struct{}),
		}
		agent := newAgent(registry, "late")
		startResult := make(chan error, 1)
		go func() { startResult <- agent.Start(context.Background()) }()
		<-registry.registerEntered
		if err := agent.Close(context.Background()); !errors.Is(err, ErrAgentCloseTimeout) {
			t.Fatalf("Close err=%v", err)
		}
		close(registry.releaseRegister)
		select {
		case err := <-startResult:
			if !errors.Is(err, ErrAgentClosed) {
				t.Fatalf("late Start err=%v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("late Register return did not converge Start")
		}
		base.mu.Lock()
		durable := cloneRecord(base.record)
		heartbeats := append([]core.HolderHeartbeat(nil), base.heartbeats...)
		base.mu.Unlock()
		if durable.State != core.HolderStateWarming || len(heartbeats) != 0 {
			t.Fatalf("late commit reopened cleanup durable=%+v heartbeats=%+v", durable, heartbeats)
		}
		if got := agent.Status(); got.State != AgentStateUnhealthy {
			t.Fatalf("late commit changed fail-closed status=%+v", got)
		}
		agent.mu.Lock()
		runStarted := agent.runStarted
		closeWorkerStarted := agent.closeWorkerStarted
		closeComplete := agent.closeComplete
		agent.mu.Unlock()
		if runStarted || !closeWorkerStarted || !closeComplete {
			t.Fatalf("runStarted=%t closeWorkerStarted=%t closeComplete=%t", runStarted, closeWorkerStarted, closeComplete)
		}
	})
}

func TestHolderAgentCloseHeartbeatReconcileInheritsCloseDeadline(t *testing.T) {
	closeTimeout := 40 * time.Millisecond
	registry := &closeDeadlineGetRegistry{
		base: &fakeAgentRegistry{failAfter: -1}, anomaly: errors.New("close heartbeat uncertain"),
		getBudget: make(chan time.Duration, 1),
	}
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: registry,
		ProcessStatus: &fakeProcessStatus{status: ProcessStatus{
			ProcessReady: true, CapacityWeight: 1,
		}},
		Domain: actorrouting.DomainPlayer, HolderID: "holder-close-deadline",
		LogicEndpoint:         "holder-close-deadline.internal:9000",
		HolderControlEndpoint: "holder-close-deadline.internal:9000",
		TLSDNSIdentity:        "holder-close-deadline.internal",
		BootID:                func() (string, error) { return "boot-close-deadline", nil },
		HeartbeatInterval:     100 * time.Millisecond,
		HeartbeatTimeout:      250 * time.Millisecond,
		CloseTimeout:          closeTimeout,
		ClosePollInterval:     10 * time.Millisecond,
		LeaseTTL:              time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := agent.Close(context.Background()); err == nil {
		t.Fatal("uncertain close heartbeat unexpectedly succeeded")
	}
	elapsed := time.Since(started)
	if elapsed > closeTimeout+60*time.Millisecond {
		t.Fatalf("Close reconciliation escaped CloseTimeout elapsed=%v timeout=%v", elapsed, closeTimeout)
	}
	select {
	case budget := <-registry.getBudget:
		if budget <= 0 || budget > closeTimeout+10*time.Millisecond {
			t.Fatalf("Get deadline budget=%v close timeout=%v", budget, closeTimeout)
		}
	case <-time.After(time.Second):
		t.Fatal("reconciliation Get did not observe a deadline")
	}
}

func TestHolderAgentInflightPeriodicOutcomeReconcilesWithinSharedCloseDeadline(t *testing.T) {
	tests := []struct {
		name           string
		periodicStatus ProcessStatus
		periodicState  core.HolderState
		commitPeriodic bool
	}{
		{
			name: "ready", periodicStatus: ProcessStatus{ProcessReady: true, CapacityWeight: 1},
			periodicState: core.HolderStateReadyZeroShard, commitPeriodic: true,
		},
		{
			name: "serving", periodicStatus: ProcessStatus{
				ProcessReady: true, ServingShardCount: 1, CapacityWeight: 1,
			}, periodicState: core.HolderStateServing, commitPeriodic: true,
		},
		{
			name: "durable-previous", periodicStatus: ProcessStatus{
				ProcessReady: true, ServingShardCount: 1, CapacityWeight: 1,
			}, periodicState: core.HolderStateServing,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, CapacityWeight: 1}}
			base := &fakeAgentRegistry{failAfter: -1}
			registry := &inflightCommitCancelRegistry{
				base: base, committed: make(chan struct{}), getEntered: make(chan struct{}),
				releaseGet: make(chan struct{}), getDeadline: make(chan time.Time, 1),
				commitSecond: test.commitPeriodic,
			}
			closeTimeout := 120 * time.Millisecond
			agent, err := NewHolderAgent(HolderAgentOptions{
				Registry: registry, ProcessStatus: status,
				Domain: actorrouting.DomainPlayer, HolderID: "holder-inflight-reconcile-" + test.name,
				LogicEndpoint:               "holder-inflight-reconcile.internal:9000",
				HolderControlEndpoint:       "holder-inflight-reconcile.internal:9000",
				TLSDNSIdentity:              "holder-inflight-reconcile.internal",
				BootID:                      func() (string, error) { return "boot-inflight-reconcile", nil },
				HeartbeatInterval:           2 * time.Millisecond,
				HeartbeatTimeout:            250 * time.Millisecond,
				CloseTimeout:                closeTimeout,
				ClosePollInterval:           2 * time.Millisecond,
				ConsecutiveFailureThreshold: 1,
				LeaseTTL:                    time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := agent.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			status.set(test.periodicStatus)
			select {
			case <-registry.committed:
			case <-time.After(time.Second):
				t.Fatal("periodic heartbeat did not commit")
			}

			started := time.Now()
			closeResult := make(chan error, 1)
			go func() { closeResult <- agent.Close(context.Background()) }()
			select {
			case <-registry.getEntered:
			case <-time.After(time.Second):
				t.Fatal("ambiguous heartbeat did not reach reconciliation Get")
			}
			agent.mu.Lock()
			sharedDeadline := agent.closeDeadline
			agent.mu.Unlock()
			select {
			case getDeadline := <-registry.getDeadline:
				if sharedDeadline.IsZero() || !getDeadline.Equal(sharedDeadline) {
					t.Fatalf("Get deadline=%v shared close deadline=%v", getDeadline, sharedDeadline)
				}
			case <-time.After(time.Second):
				t.Fatal("reconciliation Get had no deadline")
			}
			// Get is genuinely blocked after runCtx cancellation, then recovers
			// while the one Close budget is still live.
			time.Sleep(5 * time.Millisecond)
			status.set(ProcessStatus{ProcessReady: false, CapacityWeight: 1})
			close(registry.releaseGet)
			select {
			case err := <-closeResult:
				if err != nil {
					t.Fatalf("Close err=%v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close did not converge")
			}
			if elapsed := time.Since(started); elapsed > closeTimeout+80*time.Millisecond {
				t.Fatalf("Close escaped shared deadline elapsed=%v", elapsed)
			}

			base.mu.Lock()
			heartbeats := append([]core.HolderHeartbeat(nil), base.heartbeats...)
			durable := cloneRecord(base.record)
			base.mu.Unlock()
			wantHeartbeats := 3
			if test.commitPeriodic {
				wantHeartbeats = 4
			}
			if len(heartbeats) != wantHeartbeats {
				t.Fatalf("heartbeat order=%+v", heartbeats)
			}
			if test.commitPeriodic && heartbeats[1].State != test.periodicState {
				t.Fatalf("periodic state=%s want=%s", heartbeats[1].State, test.periodicState)
			}
			if heartbeats[len(heartbeats)-2].State != core.HolderStateDraining ||
				heartbeats[len(heartbeats)-1].State != core.HolderStateDrained {
				t.Fatalf("heartbeat order=%+v", heartbeats)
			}
			for index := 1; index < len(heartbeats); index++ {
				if heartbeats[index].Sequence != heartbeats[index-1].Sequence+1 {
					t.Fatalf("sequence reused/staled heartbeats=%+v", heartbeats)
				}
			}
			got := agent.Status()
			if got.State != AgentStateDrained || got.ConsecutiveFailures != 0 || got.Unhealthy ||
				durable.State != core.HolderStateDrained || durable.Sequence != got.Holder.Sequence {
				t.Fatalf("close did not install exact in-flight baseline status=%+v durable=%+v", got, durable)
			}
		})
	}
}

func TestHolderAgentCloseDeadlineHardBoundsNonCooperativeDependencies(t *testing.T) {
	for _, mode := range []string{"status", "heartbeat", "get"} {
		t.Run(mode, func(t *testing.T) {
			dependencies := &hardBoundAgentDependencies{
				base: &fakeAgentRegistry{failAfter: -1}, mode: mode,
				entered: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{}),
			}
			closeTimeout := 35 * time.Millisecond
			agent := newHardBoundTestAgent(t, dependencies, closeTimeout)
			if err := agent.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			closeResult := make(chan error, 1)
			go func() { closeResult <- agent.Close(context.Background()) }()
			select {
			case <-dependencies.entered:
			case <-time.After(time.Second):
				t.Fatalf("%s dependency was not called", mode)
			}
			agent.mu.Lock()
			firstCloseDeadline := agent.closeDeadline
			agent.mu.Unlock()
			if firstCloseDeadline.IsZero() {
				t.Fatal("unique close worker had no immutable deadline")
			}
			var closeErr error
			select {
			case closeErr = <-closeResult:
			case <-time.After(time.Second):
				t.Fatalf("Close remained blocked on %s", mode)
			}
			elapsed := time.Since(started)
			if !errors.Is(closeErr, ErrAgentCloseTimeout) || !errors.Is(closeErr, ErrAgentUnhealthy) {
				t.Fatalf("sticky timeout err=%v", closeErr)
			}
			if elapsed < closeTimeout/2 || elapsed > closeTimeout+100*time.Millisecond {
				t.Fatalf("Close elapsed=%v timeout=%v", elapsed, closeTimeout)
			}
			gotBeforeLateReturn := agent.Status()
			if gotBeforeLateReturn.State != AgentStateUnhealthy || !gotBeforeLateReturn.Unhealthy {
				t.Fatalf("timeout status=%+v", gotBeforeLateReturn)
			}
			agent.mu.Lock()
			closeDone := agent.closeDone
			closeComplete := agent.closeComplete
			agent.mu.Unlock()
			if !closeComplete {
				t.Fatal("close owner did not complete")
			}
			select {
			case <-closeDone:
			default:
				t.Fatal("closeDone remained open")
			}
			if repeated := agent.Close(context.Background()); repeated == nil ||
				repeated.Error() != closeErr.Error() {
				t.Fatalf("sticky repeated Close=%v first=%v", repeated, closeErr)
			}
			agent.mu.Lock()
			repeatedCloseDeadline := agent.closeDeadline
			agent.mu.Unlock()
			if !repeatedCloseDeadline.Equal(firstCloseDeadline) {
				t.Fatalf("repeated Close extended deadline first=%v repeated=%v", firstCloseDeadline, repeatedCloseDeadline)
			}
			statusCalls, heartbeatCalls, getCalls := dependencies.counts()
			close(dependencies.release)
			select {
			case <-dependencies.returned:
			case <-time.After(time.Second):
				t.Fatal("late dependency did not return")
			}
			time.Sleep(5 * time.Millisecond)
			gotAfterLateReturn := agent.Status()
			if gotAfterLateReturn.State != AgentStateUnhealthy ||
				gotAfterLateReturn.Holder.Sequence != gotBeforeLateReturn.Holder.Sequence ||
				gotAfterLateReturn.Holder.State != gotBeforeLateReturn.Holder.State {
				t.Fatalf("late result mutated Agent before=%+v after=%+v", gotBeforeLateReturn, gotAfterLateReturn)
			}
			laterStatus, laterHeartbeat, laterGet := dependencies.counts()
			if laterStatus != statusCalls || laterHeartbeat != heartbeatCalls || laterGet != getCalls {
				t.Fatalf(
					"new call after deadline before=(%d,%d,%d) after=(%d,%d,%d)",
					statusCalls, heartbeatCalls, getCalls, laterStatus, laterHeartbeat, laterGet,
				)
			}
			dependencies.base.mu.Lock()
			durable := cloneRecord(dependencies.base.record)
			dependencies.base.mu.Unlock()
			if durable.State == core.HolderStateDrained {
				t.Fatalf("deadline failure claimed DRAINED durable=%+v", durable)
			}
		})
	}
}

func TestHolderAgentBoundedDependenciesReturningWithinDeadlineDrainNormally(t *testing.T) {
	for _, mode := range []string{"status", "heartbeat", "get"} {
		t.Run(mode, func(t *testing.T) {
			dependencies := &hardBoundAgentDependencies{
				base: &fakeAgentRegistry{failAfter: -1}, mode: mode,
				entered: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{}),
			}
			agent := newHardBoundTestAgent(t, dependencies, 100*time.Millisecond)
			if err := agent.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			closeResult := make(chan error, 1)
			go func() { closeResult <- agent.Close(context.Background()) }()
			select {
			case <-dependencies.entered:
			case <-time.After(time.Second):
				t.Fatalf("%s dependency was not called", mode)
			}
			time.Sleep(2 * time.Millisecond)
			close(dependencies.release)
			select {
			case err := <-closeResult:
				if err != nil {
					t.Fatalf("Close err=%v", err)
				}
			case <-time.After(time.Second):
				t.Fatalf("Close did not recover from %s", mode)
			}
			select {
			case <-dependencies.returned:
			default:
				t.Fatal("dependency release was not observed")
			}
			dependencies.base.mu.Lock()
			durable := cloneRecord(dependencies.base.record)
			dependencies.base.mu.Unlock()
			got := agent.Status()
			if got.State != AgentStateDrained || got.Unhealthy || got.ConsecutiveFailures != 0 ||
				durable.State != core.HolderStateDrained || durable.Sequence != got.Holder.Sequence {
				t.Fatalf("bounded dependency did not drain status=%+v durable=%+v", got, durable)
			}
		})
	}
}

func TestHolderAgentCloseRequestedStartFinishWindowRejectsThirdStart(t *testing.T) {
	status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, CapacityWeight: 1}}
	base := &fakeAgentRegistry{failAfter: -1}
	registry := &committedHeartbeatBlockRegistry{
		base: base, blockAt: 1, committed: make(chan uint64, 1), release: make(chan struct{}),
	}
	agent := newTestAgent(registry, status)
	finishEntered := make(chan struct{})
	releaseFinish := make(chan struct{})
	agent.startFinishHook = func(*agentStartAttempt) {
		close(finishEntered)
		<-releaseFinish
	}

	startResult := make(chan error, 1)
	go func() { startResult <- agent.Start(context.Background()) }()
	select {
	case <-registry.committed:
	case <-time.After(time.Second):
		t.Fatal("initial heartbeat did not commit")
	}
	closeResult := make(chan error, 1)
	go func() { closeResult <- agent.Close(context.Background()) }()
	deadline := time.After(time.Second)
	for {
		agent.mu.Lock()
		requested := agent.closeRequested
		agent.mu.Unlock()
		if requested {
			break
		}
		select {
		case <-deadline:
			t.Fatal("Close did not request startup closure")
		case <-time.After(time.Millisecond):
		}
	}
	close(registry.release)
	select {
	case <-finishEntered:
	case <-time.After(time.Second):
		t.Fatal("Start did not reach finish window")
	}
	if err := agent.Start(context.Background()); !errors.Is(err, ErrAgentClosed) {
		t.Fatalf("third Start in close window err=%v", err)
	}
	close(releaseFinish)
	if err := <-startResult; !errors.Is(err, ErrAgentClosed) {
		t.Fatalf("owner Start err=%v", err)
	}
	if err := <-closeResult; err != nil {
		t.Fatalf("Close owner failed to finish: %v", err)
	}
}

func TestHolderAgentStartHandoffHookCannotDelayRunOwnership(t *testing.T) {
	status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, CapacityWeight: 1}}
	registry := &fakeAgentRegistry{failAfter: -1}
	agent := newTestAgent(registry, status)
	hookEntered := make(chan struct{})
	releaseHook := make(chan struct{})
	agent.startFinishHook = func(attempt *agentStartAttempt) {
		if !attempt.complete {
			t.Error("finish hook observed an incomplete attempt")
		}
		close(hookEntered)
		<-releaseHook
	}

	startResult := make(chan error, 1)
	go func() { startResult <- agent.Start(context.Background()) }()
	select {
	case <-hookEntered:
	case <-time.After(time.Second):
		t.Fatal("Start did not reach handoff hook")
	}
	agent.mu.Lock()
	runStarted := agent.runStarted
	lifecycle := agent.lifecycle
	done := agent.done
	agent.mu.Unlock()
	if !runStarted || lifecycle != agentLifecycleStarted || done == nil {
		t.Fatalf("handoff exposed no run owner runStarted=%t lifecycle=%d done=%v", runStarted, lifecycle, done)
	}

	// Keep Start's hook blocked. Close must still cancel a real run owner,
	// observe done, and publish DRAINING/DRAINED without consuming its budget
	// waiting for the hook or the Start caller to return.
	closeResult := make(chan error, 1)
	go func() { closeResult <- agent.Close(context.Background()) }()
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatalf("Close err=%v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Close waited for post-handoff hook")
	}
	close(releaseHook)
	if err := <-startResult; err != nil {
		t.Fatalf("linearized Start result=%v", err)
	}
	registry.mu.Lock()
	durable := cloneRecord(registry.record)
	heartbeats := append([]core.HolderHeartbeat(nil), registry.heartbeats...)
	registry.mu.Unlock()
	if durable.State != core.HolderStateDrained || len(heartbeats) != 3 ||
		heartbeats[0].State != core.HolderStateReadyZeroShard ||
		heartbeats[1].State != core.HolderStateDraining ||
		heartbeats[2].State != core.HolderStateDrained {
		t.Fatalf("handoff left READY residue durable=%+v heartbeats=%+v", durable, heartbeats)
	}
}

func TestHolderAgentCloseDeadlineInitializationIsSingleAndOverflowSafe(t *testing.T) {
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: &fakeAgentRegistry{failAfter: -1},
		ProcessStatus: &fakeProcessStatus{status: ProcessStatus{
			ProcessReady: true, CapacityWeight: 1,
		}},
		Domain: actorrouting.DomainPlayer, HolderID: "holder-close-deadline-overflow",
		LogicEndpoint:         "holder-close-deadline-overflow.internal:9000",
		HolderControlEndpoint: "holder-close-deadline-overflow.internal:9000",
		TLSDNSIdentity:        "holder-close-deadline-overflow.internal",
		HeartbeatInterval:     5 * time.Second,
		HeartbeatTimeout:      time.Second,
		CloseTimeout:          time.Duration(1<<63 - 1),
		ClosePollInterval:     time.Millisecond,
		LeaseTTL:              10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	agent.mu.Lock()
	firstStarted := agent.startCloseWorkerLocked()
	firstDeadline := agent.closeDeadline
	secondStarted := agent.startCloseWorkerLocked()
	secondDeadline := agent.closeDeadline
	agent.mu.Unlock()
	if !firstStarted || secondStarted {
		t.Fatalf("close worker starts first=%t second=%t", firstStarted, secondStarted)
	}
	if firstDeadline.Before(before) || !secondDeadline.Equal(firstDeadline) {
		t.Fatalf("wrapped/extended deadline before=%v first=%v second=%v", before, firstDeadline, secondDeadline)
	}
}

func TestHolderAgentRejectsRegistrationResponseDriftBeforeBaseline(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*core.HolderRecord)
	}{
		{name: "foreign-holder", mutate: func(record *core.HolderRecord) { record.HolderID = "foreign" }},
		{name: "endpoint", mutate: func(record *core.HolderRecord) { record.LogicEndpoint = "drift.internal:9000" }},
		{name: "tls-identity", mutate: func(record *core.HolderRecord) { record.TLSDNSIdentity = "drift.internal" }},
		{name: "started-at", mutate: func(record *core.HolderRecord) { record.StartedAt = record.StartedAt.Add(time.Millisecond) }},
		{name: "resource", mutate: func(record *core.HolderRecord) { record.ResourceSummary = map[string]string{"cpu": "99"} }},
		{name: "illegal-sequence", mutate: func(record *core.HolderRecord) { record.Sequence = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := &fakeAgentRegistry{failAfter: -1}
			registry := &registrationResponseRegistry{base: base, mutate: test.mutate}
			status := &fakeProcessStatus{status: ProcessStatus{
				ProcessReady: true, CapacityWeight: 2,
				ResourceSummary: map[string]string{"cpu": "2"},
			}}
			now := time.UnixMilli(1_800_000_000_000)
			agent, err := NewHolderAgent(HolderAgentOptions{
				Registry: registry, ProcessStatus: status,
				Domain: actorrouting.DomainPlayer, HolderID: "holder-response",
				LogicEndpoint:         "holder-response.internal:9000",
				HolderControlEndpoint: "holder-response.internal:9000",
				TLSDNSIdentity:        "holder-response.internal",
				BootID:                func() (string, error) { return "boot-response", nil },
				HeartbeatInterval:     5 * time.Second, HeartbeatTimeout: time.Second,
				CloseTimeout: time.Second, ClosePollInterval: time.Millisecond,
				LeaseTTL: 10 * time.Second, Now: func() time.Time { return now },
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := agent.Start(context.Background()); !errors.Is(err, ErrAgentInvalid) {
				t.Fatalf("drifted Register response err=%v", err)
			}
			deadline := time.Now().Add(time.Second)
			for agent.Status().State != AgentStateDrained && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			got := agent.Status()
			if got.State != AgentStateDrained || got.Holder.HolderID != "holder-response" ||
				agent.BootID() != "boot-response" {
				t.Fatalf("committed registration was not durably cleaned status=%+v boot=%q", got, agent.BootID())
			}
			base.mu.Lock()
			heartbeats := append([]core.HolderHeartbeat(nil), base.heartbeats...)
			base.mu.Unlock()
			if len(heartbeats) != 2 || heartbeats[0].State != core.HolderStateDraining ||
				heartbeats[1].State != core.HolderStateDrained {
				t.Fatalf("drifted registration cleanup=%+v", heartbeats)
			}
			if err := agent.Close(context.Background()); err != nil {
				t.Fatalf("idempotent cleanup Close err=%v", err)
			}
		})
	}
}

func TestHolderAgentRegisterCommitThenErrorCleansDurableWarming(t *testing.T) {
	base := &fakeAgentRegistry{failAfter: -1}
	registry := &commitThenErrorRegistry{base: base, registerErr: context.Canceled}
	status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, CapacityWeight: 1}}
	agent := newTestAgent(registry, status)
	if err := agent.Start(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start err=%v", err)
	}
	waitForAgentState(t, agent, AgentStateDrained)
	base.mu.Lock()
	heartbeats := append([]core.HolderHeartbeat(nil), base.heartbeats...)
	durable := cloneRecord(base.record)
	base.mu.Unlock()
	if len(heartbeats) != 2 || heartbeats[0].Sequence != 2 || heartbeats[0].State != core.HolderStateDraining ||
		heartbeats[1].Sequence != 3 || heartbeats[1].State != core.HolderStateDrained ||
		durable.State != core.HolderStateDrained {
		t.Fatalf("commit/error cleanup heartbeats=%+v durable=%+v", heartbeats, durable)
	}
}

func TestHolderAgentRegisterReconcileForeignOrUnreadableIsStickyUnhealthy(t *testing.T) {
	tests := []struct {
		name string
		edit func(*commitThenErrorRegistry)
	}{
		{name: "foreign", edit: func(registry *commitThenErrorRegistry) {
			registry.reconciliationGetEdit = func(record *core.HolderRecord) { record.BootID = "foreign" }
		}},
		{name: "unreadable", edit: func(registry *commitThenErrorRegistry) {
			registry.reconciliationGetErr = errors.New("read unavailable")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := &fakeAgentRegistry{failAfter: -1}
			registry := &commitThenErrorRegistry{base: base, registerErr: context.Canceled}
			test.edit(registry)
			agent := newTestAgent(registry, &fakeProcessStatus{status: ProcessStatus{
				ProcessReady: true, CapacityWeight: 1,
			}})
			if err := agent.Start(context.Background()); !errors.Is(err, ErrAgentUnhealthy) {
				t.Fatalf("Start err=%v", err)
			}
			if got := agent.Status(); got.State != AgentStateUnhealthy || !got.Unhealthy {
				t.Fatalf("status=%+v", got)
			}
			base.mu.Lock()
			heartbeats := len(base.heartbeats)
			base.mu.Unlock()
			if heartbeats != 0 {
				t.Fatalf("uncertain registration published cleanup heartbeats=%d", heartbeats)
			}
			if err := agent.Close(context.Background()); !errors.Is(err, ErrAgentUnhealthy) {
				t.Fatalf("Close err=%v", err)
			}
		})
	}
}

func TestHolderAgentHeartbeatCommitThenErrorReconcilesSequenceForAllModes(t *testing.T) {
	t.Run("initial", func(t *testing.T) {
		base := &fakeAgentRegistry{failAfter: -1}
		registry := &commitThenErrorRegistry{
			base: base, heartbeatErrorAt: map[int]error{1: context.Canceled},
		}
		agent := newTestAgent(registry, &fakeProcessStatus{status: ProcessStatus{
			ProcessReady: true, CapacityWeight: 1,
		}})
		if err := agent.Start(context.Background()); !errors.Is(err, context.Canceled) {
			t.Fatalf("Start err=%v", err)
		}
		waitForAgentState(t, agent, AgentStateDrained)
		base.mu.Lock()
		heartbeats := append([]core.HolderHeartbeat(nil), base.heartbeats...)
		base.mu.Unlock()
		if len(heartbeats) != 3 || heartbeats[0].Sequence != 2 ||
			heartbeats[1].Sequence != 3 || heartbeats[1].State != core.HolderStateDraining ||
			heartbeats[2].Sequence != 4 || heartbeats[2].State != core.HolderStateDrained {
			t.Fatalf("initial reconcile sequence=%+v", heartbeats)
		}
	})

	t.Run("periodic", func(t *testing.T) {
		base := &fakeAgentRegistry{failAfter: -1}
		registry := &commitThenErrorRegistry{
			base: base, heartbeatErrorAt: map[int]error{2: context.Canceled},
		}
		agent := newTestAgent(registry, &fakeProcessStatus{status: ProcessStatus{
			ProcessReady: true, CapacityWeight: 1,
		}})
		if err := agent.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := agent.heartbeatOnce(context.Background()); !errors.Is(err, context.Canceled) {
			t.Fatalf("commit/cancel heartbeat err=%v", err)
		}
		if got := agent.Status().Holder.Sequence; got != 3 {
			t.Fatalf("reconciled local sequence=%d", got)
		}
		if err := agent.heartbeatOnce(context.Background()); err != nil {
			t.Fatalf("post-reconcile heartbeat err=%v", err)
		}
		base.mu.Lock()
		heartbeats := append([]core.HolderHeartbeat(nil), base.heartbeats...)
		base.mu.Unlock()
		if len(heartbeats) < 3 || heartbeats[0].Sequence != 2 ||
			heartbeats[1].Sequence != 3 || heartbeats[2].Sequence != 4 {
			t.Fatalf("periodic sequence reuse=%+v", heartbeats)
		}
		if err := agent.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("close", func(t *testing.T) {
		base := &fakeAgentRegistry{failAfter: -1}
		registry := &commitThenErrorRegistry{
			base: base, heartbeatErrorAt: map[int]error{2: context.Canceled},
		}
		agent := newTestAgent(registry, &fakeProcessStatus{status: ProcessStatus{
			ProcessReady: true, CapacityWeight: 1,
		}})
		if err := agent.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := agent.Close(context.Background()); err != nil {
			t.Fatalf("reconciled Close err=%v", err)
		}
		base.mu.Lock()
		heartbeats := append([]core.HolderHeartbeat(nil), base.heartbeats...)
		base.mu.Unlock()
		if len(heartbeats) != 3 || heartbeats[1].Sequence != 3 ||
			heartbeats[1].State != core.HolderStateDraining || heartbeats[2].Sequence != 4 ||
			heartbeats[2].State != core.HolderStateDrained {
			t.Fatalf("close reconcile sequence=%+v", heartbeats)
		}
	})

	t.Run("close-nil-response-drift-does-not-cross-runtime-threshold", func(t *testing.T) {
		base := &fakeAgentRegistry{failAfter: -1}
		registry := &commitThenErrorRegistry{
			base: base,
			heartbeatResponseAt: map[int]func(*core.HolderRecord){
				2: func(record *core.HolderRecord) { record.ResourceSummary = map[string]string{"reply": "drift"} },
			},
		}
		status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, CapacityWeight: 1}}
		agent, err := NewHolderAgent(HolderAgentOptions{
			Registry: registry, ProcessStatus: status,
			Domain: actorrouting.DomainPlayer, HolderID: "holder-close-drift",
			LogicEndpoint: "holder-close-drift.internal:9000", HolderControlEndpoint: "holder-close-drift.internal:9000", TLSDNSIdentity: "holder-close-drift.internal",
			BootID:                      func() (string, error) { return "boot-close-drift", nil },
			HeartbeatInterval:           5 * time.Second,
			HeartbeatTimeout:            time.Second,
			CloseTimeout:                time.Second,
			ClosePollInterval:           time.Millisecond,
			ConsecutiveFailureThreshold: 1,
			LeaseTTL:                    10 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := agent.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := agent.Close(context.Background()); err != nil {
			t.Fatalf("drift Close err=%v", err)
		}
		if got := agent.Status(); got.State != AgentStateDrained || got.Holder.Sequence != 4 ||
			got.ConsecutiveFailures != 0 {
			t.Fatalf("close drift crossed runtime threshold status=%+v", got)
		}
		base.mu.Lock()
		durable := cloneRecord(base.record)
		heartbeats := append([]core.HolderHeartbeat(nil), base.heartbeats...)
		base.mu.Unlock()
		if durable.State != core.HolderStateDrained || durable.Sequence != 4 {
			t.Fatalf("durable drift candidate=%+v heartbeats=%+v", durable, heartbeats)
		}
		if len(heartbeats) != 3 || heartbeats[1].State != core.HolderStateDraining ||
			heartbeats[2].State != core.HolderStateDrained {
			t.Fatalf("close drift cleanup order=%+v", heartbeats)
		}
	})
}

func TestHolderAgentInitialHeartbeatUncertainOutcomeThresholdOne(t *testing.T) {
	anomaly := errors.New("initial heartbeat transport anomaly")
	tests := []struct {
		name       string
		configure  func(*commitThenErrorRegistry)
		wantState  AgentState
		wantHB     int
		wantSticky bool
	}{
		{
			name: "durable-expected",
			configure: func(registry *commitThenErrorRegistry) {
				registry.heartbeatErrorAt = map[int]error{1: anomaly}
			},
			wantState: AgentStateDrained, wantHB: 3,
		},
		{
			name: "durable-previous",
			configure: func(registry *commitThenErrorRegistry) {
				registry.heartbeatErrorBefore = map[int]error{1: anomaly}
			},
			wantState: AgentStateDrained, wantHB: 2,
		},
		{
			name: "unresolved",
			configure: func(registry *commitThenErrorRegistry) {
				registry.heartbeatErrorBefore = map[int]error{1: anomaly}
				registry.reconciliationMissing = true
			},
			wantState: AgentStateUnhealthy, wantSticky: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := &fakeAgentRegistry{failAfter: -1}
			registry := &commitThenErrorRegistry{base: base}
			test.configure(registry)
			startCtx := context.Background()
			if test.name == "durable-expected" {
				canceledCtx, cancelStart := context.WithCancel(context.Background())
				registry.heartbeatAfterCommit = map[int]func(){1: cancelStart}
				startCtx = canceledCtx
			}
			agent, err := NewHolderAgent(HolderAgentOptions{
				Registry: registry,
				ProcessStatus: &fakeProcessStatus{status: ProcessStatus{
					ProcessReady: true, CapacityWeight: 1,
				}},
				Domain: actorrouting.DomainPlayer, HolderID: "holder-initial-uncertain",
				LogicEndpoint:               "holder-initial-uncertain.internal:9000",
				HolderControlEndpoint:       "holder-initial-uncertain.internal:9000",
				TLSDNSIdentity:              "holder-initial-uncertain.internal",
				BootID:                      func() (string, error) { return "boot-initial-uncertain", nil },
				HeartbeatInterval:           5 * time.Second,
				HeartbeatTimeout:            time.Second,
				CloseTimeout:                time.Second,
				ClosePollInterval:           time.Millisecond,
				ConsecutiveFailureThreshold: 1,
				LeaseTTL:                    10 * time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := agent.Start(startCtx); !errors.Is(err, anomaly) {
				t.Fatalf("Start did not preserve original anomaly: %v", err)
			}
			got := agent.Status()
			if got.State != test.wantState || got.Unhealthy != test.wantSticky {
				t.Fatalf("status=%+v wantState=%s sticky=%t", got, test.wantState, test.wantSticky)
			}
			base.mu.Lock()
			durable := cloneRecord(base.record)
			heartbeats := append([]core.HolderHeartbeat(nil), base.heartbeats...)
			base.mu.Unlock()
			if test.wantSticky {
				if durable.State != core.HolderStateWarming || len(heartbeats) != 0 {
					t.Fatalf("unresolved outcome was cleaned unsafely durable=%+v heartbeats=%+v", durable, heartbeats)
				}
				if err := agent.Close(context.Background()); !errors.Is(err, ErrAgentUnhealthy) {
					t.Fatalf("sticky Close err=%v", err)
				}
				return
			}
			if len(heartbeats) != test.wantHB || durable.State != core.HolderStateDrained ||
				durable.ProcessReady || !durable.Draining || durable.ServingShardCount != 0 {
				t.Fatalf("resolved initial outcome left READY residue durable=%+v heartbeats=%+v", durable, heartbeats)
			}
			if got.ConsecutiveFailures != 0 {
				t.Fatalf("initial anomaly crossed runtime threshold: %+v", got)
			}
		})
	}
}

func TestHolderAgentStartCallerCancelDoesNotStopPeriodicHeartbeat(t *testing.T) {
	base := &fakeAgentRegistry{failAfter: -1}
	status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, CapacityWeight: 1}}
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: base, ProcessStatus: status,
		Domain: actorrouting.DomainPlayer, HolderID: "holder-owned-context",
		LogicEndpoint: "holder-owned-context.internal:9000", HolderControlEndpoint: "holder-owned-context.internal:9000", TLSDNSIdentity: "holder-owned-context.internal",
		BootID:            func() (string, error) { return "boot-owned-context", nil },
		HeartbeatInterval: 2 * time.Millisecond, HeartbeatTimeout: 20 * time.Millisecond,
		CloseTimeout: 200 * time.Millisecond, ClosePollInterval: time.Millisecond,
		LeaseTTL: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	startCtx, cancelStart := context.WithTimeout(context.Background(), 30*time.Millisecond)
	if err := agent.Start(startCtx); err != nil {
		t.Fatal(err)
	}
	cancelStart()
	deadline := time.Now().Add(200 * time.Millisecond)
	for {
		base.mu.Lock()
		count := len(base.heartbeats)
		base.mu.Unlock()
		if count >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("periodic heartbeat stopped with Start caller count=%d", count)
		}
		time.Sleep(time.Millisecond)
	}
	if err := agent.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHolderAgentInitialServingStatusFailsClosedWithoutServingHeartbeat(t *testing.T) {
	status := &fakeProcessStatus{status: ProcessStatus{
		ProcessReady: true, ServingShardCount: 1, CapacityWeight: 1,
	}}
	registry := &fakeAgentRegistry{failAfter: -1}
	agent := newTestAgent(registry, status)
	startResult := make(chan error, 1)
	go func() { startResult <- agent.Start(context.Background()) }()
	deadline := time.After(time.Second)
	for {
		registry.mu.Lock()
		count := len(registry.heartbeats)
		registry.mu.Unlock()
		if count >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("startup cleanup did not publish DRAINING")
		case <-time.After(time.Millisecond):
		}
	}
	status.set(ProcessStatus{ProcessReady: false, ServingShardCount: 0, CapacityWeight: 1})
	if err := <-startResult; err == nil {
		t.Fatal("serving startup unexpectedly succeeded")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, heartbeat := range registry.heartbeats {
		if heartbeat.State == core.HolderStateServing {
			t.Fatalf("startup published SERVING: %+v", registry.heartbeats)
		}
	}
	if len(registry.heartbeats) < 2 ||
		registry.heartbeats[len(registry.heartbeats)-2].State != core.HolderStateDraining ||
		registry.heartbeats[len(registry.heartbeats)-1].State != core.HolderStateDrained {
		t.Fatalf("startup cleanup heartbeats=%+v", registry.heartbeats)
	}
}

func TestHolderAgentHeartbeatResponseDriftCountsOnlyAtRuntimeAndBecomesSticky(t *testing.T) {
	t.Run("runtime-threshold", func(t *testing.T) {
		status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, CapacityWeight: 1}}
		base := &fakeAgentRegistry{failAfter: -1}
		registry := &heartbeatResponseDriftRegistry{
			base: base, shouldMutate: func(call int) bool { return call > 1 },
		}
		agent, err := NewHolderAgent(HolderAgentOptions{
			Registry: registry, ProcessStatus: status,
			Domain: actorrouting.DomainPlayer, HolderID: "holder-runtime-drift",
			LogicEndpoint: "holder-runtime-drift.internal:9000", HolderControlEndpoint: "holder-runtime-drift.internal:9000", TLSDNSIdentity: "holder-runtime-drift.internal",
			BootID:            func() (string, error) { return "boot-runtime-drift", nil },
			HeartbeatInterval: 5 * time.Second, HeartbeatTimeout: time.Second,
			CloseTimeout: time.Second, ClosePollInterval: time.Millisecond,
			ConsecutiveFailureThreshold: 2, LeaseTTL: 10 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := agent.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		for attempt := 1; attempt <= 2; attempt++ {
			if err := agent.heartbeatOnce(context.Background()); !errors.Is(err, ErrAgentInvalid) {
				t.Fatalf("drift attempt=%d err=%v", attempt, err)
			}
			got := agent.Status()
			if got.ConsecutiveFailures != attempt {
				t.Fatalf("attempt=%d failures=%d", attempt, got.ConsecutiveFailures)
			}
		}
		if got := agent.Status(); got.State != AgentStateUnhealthy || !got.Unhealthy {
			t.Fatalf("runtime drift did not become sticky status=%+v", got)
		}
		if err := agent.Close(context.Background()); !errors.Is(err, ErrAgentUnhealthy) {
			t.Fatalf("sticky Close err=%v", err)
		}
	})

	t.Run("initial-not-counted", func(t *testing.T) {
		status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, CapacityWeight: 1}}
		base := &fakeAgentRegistry{failAfter: -1}
		registry := &heartbeatResponseDriftRegistry{
			base: base, shouldMutate: func(call int) bool { return call == 1 },
		}
		agent := newTestAgent(registry, status)
		if err := agent.Start(context.Background()); !errors.Is(err, ErrAgentInvalid) {
			t.Fatalf("initial response drift err=%v", err)
		}
		if got := agent.Status(); got.ConsecutiveFailures != 0 || got.State != AgentStateDrained {
			t.Fatalf("initial drift was counted/repeated status=%+v", got)
		}
	})
}

func TestHolderAgentCloseHeartbeatCopiesResourceMapAndRejectsSequenceOverflow(t *testing.T) {
	status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, CapacityWeight: 1, ResourceSummary: map[string]string{"cpu": "2"}}}
	registry := &fakeAgentRegistry{failAfter: -1}
	agent := newTestAgent(registry, status)
	if err := agent.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	heartbeat, err := agent.nextCloseHeartbeat(core.HolderStateDraining, ProcessStatus{})
	if err != nil {
		t.Fatal(err)
	}
	heartbeat.ResourceSummary["cpu"] = "mutated"
	if got := agent.Status().Holder.ResourceSummary["cpu"]; got != "2" {
		t.Fatalf("close heartbeat aliased record map: %v", got)
	}
	if err := agent.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	agent.mu.Lock()
	agent.record.Sequence = ^uint64(0)
	agent.lifecycle = agentLifecycleStarted
	agent.state = AgentStateRunning
	agent.closeComplete = false
	agent.closeWorkerStarted = false
	agent.closeDeadline = time.Time{}
	agent.closeErr = nil
	agent.closeDone = make(chan struct{})
	agent.done = make(chan struct{})
	close(agent.done)
	agent.doneClosed = true
	agent.mu.Unlock()
	if err := agent.Close(context.Background()); !errors.Is(err, ErrAgentInvalid) {
		t.Fatalf("sequence overflow close err=%v", err)
	}
}

type firstAttemptFailStatus struct {
	mu       sync.Mutex
	calls    int
	entered  chan struct{}
	release  chan struct{}
	firstErr error
}

func (s *firstAttemptFailStatus) Status(context.Context) (ProcessStatus, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	if call == 1 {
		close(s.entered)
		<-s.release
		return ProcessStatus{}, s.firstErr
	}
	return ProcessStatus{ProcessReady: true, CapacityWeight: 1}, nil
}

func TestHolderAgentStartWaiterKeepsFailedAttemptResultAcrossImmediateRetry(t *testing.T) {
	firstErr := errors.New("first attempt failed")
	status := &firstAttemptFailStatus{
		entered: make(chan struct{}), release: make(chan struct{}), firstErr: firstErr,
	}
	registry := &fakeAgentRegistry{failAfter: -1}
	agent := newTestAgent(registry, status)
	joined := make(chan struct{}, 1)
	waitBlocked := make(chan struct{}, 1)
	releaseWaiter := make(chan struct{})
	agent.startJoinHook = func(*agentStartAttempt) {
		joined <- struct{}{}
	}
	agent.startWaitHook = func(*agentStartAttempt) {
		waitBlocked <- struct{}{}
		<-releaseWaiter
	}

	ownerResult := make(chan error, 1)
	go func() { ownerResult <- agent.Start(context.Background()) }()
	<-status.entered
	waiterResult := make(chan error, 1)
	go func() { waiterResult <- agent.Start(context.Background()) }()
	<-joined
	close(status.release)
	if err := <-ownerResult; !errors.Is(err, firstErr) {
		t.Fatalf("first owner err=%v", err)
	}
	<-waitBlocked

	if err := agent.Start(context.Background()); err != nil {
		t.Fatalf("second attempt err=%v", err)
	}
	close(releaseWaiter)
	if err := <-waiterResult; !errors.Is(err, firstErr) {
		t.Fatalf("first attempt waiter observed later result: %v", err)
	}
	if registry.registers != 1 {
		t.Fatalf("registers=%d", registry.registers)
	}
	if err := agent.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestHolderAgentRuntimeUnhealthyCloseIsStickyAndPublishesNothing(t *testing.T) {
	status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, CapacityWeight: 1}}
	registry := &fakeAgentRegistry{failAfter: -1}
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: registry, ProcessStatus: status,
		Domain: actorrouting.DomainFarm, HolderID: "farm-runtime-unhealthy",
		LogicEndpoint:         "farm-runtime-unhealthy.internal:9000",
		HolderControlEndpoint: "farm-runtime-unhealthy.internal:9000",
		TLSDNSIdentity:        "farm-runtime-unhealthy.internal",
		BootID:                func() (string, error) { return "boot-runtime", nil },
		HeartbeatInterval:     5 * time.Second, HeartbeatTimeout: time.Second,
		CloseTimeout: time.Second, ClosePollInterval: time.Millisecond,
		ConsecutiveFailureThreshold: 1, LeaseTTL: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	before := len(registry.heartbeats)
	registry.mu.Unlock()
	registry.setFailAfter(before)
	if err := agent.heartbeatOnce(context.Background()); err == nil {
		t.Fatal("runtime heartbeat failure missing")
	}
	if got := agent.Status(); got.State != AgentStateUnhealthy || !got.Unhealthy {
		t.Fatalf("unhealthy status=%+v", got)
	}

	registry.setFailAfter(-1)
	closeErr := agent.Close(context.Background())
	if !errors.Is(closeErr, ErrAgentUnhealthy) {
		t.Fatalf("Close err=%v", closeErr)
	}
	registry.mu.Lock()
	after := append([]core.HolderHeartbeat(nil), registry.heartbeats...)
	registry.mu.Unlock()
	if len(after) != before {
		t.Fatalf("unhealthy Close published heartbeats: before=%d after=%+v", before, after)
	}
	for _, heartbeat := range after {
		if heartbeat.State == core.HolderStateDraining || heartbeat.State == core.HolderStateDrained {
			t.Fatalf("unhealthy Close published state=%s", heartbeat.State)
		}
	}
	if got := agent.Status(); got.State != AgentStateUnhealthy || !got.Unhealthy {
		t.Fatalf("post-Close status=%+v", got)
	}
	if err := agent.Close(context.Background()); !errors.Is(err, ErrAgentUnhealthy) ||
		err.Error() != closeErr.Error() {
		t.Fatalf("idempotent unhealthy Close err=%v first=%v", err, closeErr)
	}
}

func TestHolderAgentCloseAdvancesFromCommittedInflightHeartbeat(t *testing.T) {
	status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, CapacityWeight: 1}}
	base := &fakeAgentRegistry{failAfter: -1}
	registry := &committedHeartbeatBlockRegistry{
		base: base, blockAt: 2, committed: make(chan uint64, 1), release: make(chan struct{}),
	}
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: registry, ProcessStatus: status,
		Domain: actorrouting.DomainPlayer, HolderID: "holder-inflight-close",
		LogicEndpoint:         "holder-inflight-close.internal:9000",
		HolderControlEndpoint: "holder-inflight-close.internal:9000",
		TLSDNSIdentity:        "holder-inflight-close.internal",
		BootID:                func() (string, error) { return "boot-inflight-close", nil },
		HeartbeatInterval:     time.Millisecond, HeartbeatTimeout: time.Second,
		CloseTimeout: time.Second, ClosePollInterval: time.Millisecond,
		LeaseTTL: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var committedSequence uint64
	select {
	case committedSequence = <-registry.committed:
	case <-time.After(time.Second):
		t.Fatal("periodic heartbeat did not commit")
	}
	closeResult := make(chan error, 1)
	go func() { closeResult <- agent.Close(context.Background()) }()
	deadline := time.After(time.Second)
	for {
		agent.mu.Lock()
		closing := agent.lifecycle == agentLifecycleClosing
		requested := agent.closeRequested
		agent.mu.Unlock()
		if closing || requested {
			break
		}
		select {
		case <-deadline:
			t.Fatal("Close did not enter closing")
		case <-time.After(time.Millisecond):
		}
	}
	close(registry.release)
	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}
	base.mu.Lock()
	heartbeats := append([]core.HolderHeartbeat(nil), base.heartbeats...)
	base.mu.Unlock()
	var draining, drained core.HolderHeartbeat
	for _, heartbeat := range heartbeats {
		switch heartbeat.State {
		case core.HolderStateDraining:
			draining = heartbeat
		case core.HolderStateDrained:
			drained = heartbeat
		}
	}
	if draining.Sequence != committedSequence+1 || drained.Sequence != committedSequence+2 {
		t.Fatalf(
			"close sequences committed=%d draining=%d drained=%d heartbeats=%+v",
			committedSequence, draining.Sequence, drained.Sequence, heartbeats,
		)
	}
}

func TestHolderAgentDrainRenewsBeforeLeaseExpiryUntilShardsReachZero(t *testing.T) {
	status := &fakeProcessStatus{status: ProcessStatus{ProcessReady: true, CapacityWeight: 1}}
	registry := &fakeAgentRegistry{failAfter: -1}
	agent, err := NewHolderAgent(HolderAgentOptions{
		Registry: registry, ProcessStatus: status,
		Domain: actorrouting.DomainPlayer, HolderID: "holder-drain-renew",
		LogicEndpoint: "holder-drain-renew.internal:9000", HolderControlEndpoint: "holder-drain-renew.internal:9000", TLSDNSIdentity: "holder-drain-renew.internal",
		BootID:            func() (string, error) { return "boot-drain-renew", nil },
		HeartbeatInterval: 10 * time.Millisecond, HeartbeatTimeout: 4 * time.Millisecond,
		CloseTimeout: 250 * time.Millisecond, ClosePollInterval: 4 * time.Millisecond,
		LeaseTTL: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := agent.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	status.set(ProcessStatus{ProcessReady: true, ServingShardCount: 2, CapacityWeight: 1})
	if err := agent.heartbeatOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	closeResult := make(chan error, 1)
	go func() { closeResult <- agent.Close(context.Background()) }()
	// Stay non-zero for several complete logical leases. Close must keep the
	// DRAINING record alive rather than merely polling local status.
	time.Sleep(65 * time.Millisecond)
	status.set(ProcessStatus{ProcessReady: false, ServingShardCount: 0, CapacityWeight: 1})
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("renewing Close did not converge")
	}
	registry.mu.Lock()
	heartbeats := append([]core.HolderHeartbeat(nil), registry.heartbeats...)
	registry.mu.Unlock()
	var draining []core.HolderHeartbeat
	for _, heartbeat := range heartbeats {
		if heartbeat.State == core.HolderStateDraining {
			draining = append(draining, heartbeat)
		}
	}
	if len(draining) < 4 || heartbeats[len(heartbeats)-1].State != core.HolderStateDrained {
		t.Fatalf("insufficient drain renewals heartbeats=%+v", heartbeats)
	}
	for index := 1; index < len(draining); index++ {
		if draining[index].Sequence != draining[index-1].Sequence+1 ||
			draining[index].LeaseUntil.Before(draining[index-1].LeaseUntil) {
			t.Fatalf("non-monotonic drain renewal=%+v", draining)
		}
	}
}

func waitForAgentState(t *testing.T, agent *HolderAgent, want AgentState) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if agent.Status().State == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("agent state=%+v want=%s", agent.Status(), want)
}
