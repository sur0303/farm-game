package gatewayapp

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holdercontrol"
	actorRuntime "farmgame/internal/logic/actor/runtime"
	core "farmgame/internal/routecontrol"
	"farmgame/internal/routecontrol/transport"
)

type holderProcessTestAuthorizer struct{}

func (holderProcessTestAuthorizer) Authorize(context.Context, string) error { return nil }

type nonCooperativeGRPCStopper struct {
	release chan struct{}
	stopped chan struct{}
	once    sync.Once
}

func (s *nonCooperativeGRPCStopper) GracefulStop() { <-s.release }
func (s *nonCooperativeGRPCStopper) Stop() {
	s.once.Do(func() { close(s.stopped) })
}

func TestBoundedGRPCGracefulStopDoesNotBlockLaterCleanup(t *testing.T) {
	stopper := &nonCooperativeGRPCStopper{release: make(chan struct{}), stopped: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- BoundedGRPCGracefulStop(ctx, stopper) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("bounded stop err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("bounded graceful stop blocked after its deadline")
	}
	select {
	case <-stopper.stopped:
	default:
		t.Fatal("forced Stop fallback was not called")
	}
	close(stopper.release)
}

type holderProcessTestRuntime struct{}

func (holderProcessTestRuntime) StopNewActivationAndTurns(context.Context, uint32) error {
	return nil
}
func (holderProcessTestRuntime) WaitCurrentTurns(context.Context, uint32) error { return nil }
func (holderProcessTestRuntime) ConfirmFactsCommitted(context.Context, uint32) error {
	return nil
}
func (holderProcessTestRuntime) GetShardStatus(context.Context, uint32) (actorRuntime.ShardDrainStatus, error) {
	return actorRuntime.ShardDrainStatus{}, nil
}

type holderProcessTestRegistry struct {
	mu              sync.Mutex
	record          core.HolderRecord
	registers       int
	registerEntered chan struct{}
	registerRelease <-chan struct{}
	registerOnce    sync.Once
}

type mutableHolderProcessStatus struct {
	mu     sync.Mutex
	status holdercontrol.ProcessStatus
	calls  int
}

func (s *mutableHolderProcessStatus) Status(ctx context.Context) (holdercontrol.ProcessStatus, error) {
	if err := ctx.Err(); err != nil {
		return holdercontrol.ProcessStatus{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return s.status, nil
}

func (s *mutableHolderProcessStatus) set(status holdercontrol.ProcessStatus) {
	s.mu.Lock()
	s.status = status
	s.mu.Unlock()
}

func (s *mutableHolderProcessStatus) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (r *holderProcessTestRegistry) Register(_ context.Context, record core.HolderRegistration) (core.HolderRecord, error) {
	r.mu.Lock()
	r.registers++
	r.record = record
	entered := r.registerEntered
	release := r.registerRelease
	r.mu.Unlock()
	if entered != nil {
		r.registerOnce.Do(func() { close(entered) })
	}
	if release != nil {
		<-release
	}
	return record, nil
}

func (r *holderProcessTestRegistry) Heartbeat(_ context.Context, heartbeat core.HolderHeartbeat) (core.HolderRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.record.State = heartbeat.State
	r.record.ProcessReady = heartbeat.ProcessReady
	r.record.Draining = heartbeat.Draining
	r.record.ServingShardCount = heartbeat.ServingShardCount
	r.record.SnapshotRevision = heartbeat.SnapshotRevision
	r.record.HeartbeatAt = heartbeat.HeartbeatAt
	r.record.LeaseUntil = heartbeat.LeaseUntil
	r.record.Sequence = heartbeat.Sequence
	r.record.CapacityWeight = heartbeat.CapacityWeight
	r.record.ResourceSummary = heartbeat.ResourceSummary
	return r.record, nil
}

func (r *holderProcessTestRegistry) Get(_ context.Context, domain actorrouting.RouteDomain, holderID string) (core.HolderRecord, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.record.Domain != domain || r.record.HolderID != holderID {
		return core.HolderRecord{}, false, nil
	}
	return r.record, true, nil
}

func (r *holderProcessTestRegistry) registerCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.registers
}

func (r *holderProcessTestRegistry) snapshot() core.HolderRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.record
}

func TestHolderProcessImmediateServeFailureNeverRegisters(t *testing.T) {
	registry := &holderProcessTestRegistry{}
	process := newHolderProcessForTest(t, registry, func(*transportSeamServer, net.Listener) error {
		return errors.New("injected immediate serve failure")
	})
	if err := process.Start(context.Background()); err == nil {
		t.Fatal("immediate Serve failure unexpectedly started Holder process")
	}
	if got := registry.registerCount(); got != 0 {
		t.Fatalf("immediate Serve failure registered holder %d times", got)
	}
	if process.Ready() {
		t.Fatal("immediate Serve failure left Holder ready")
	}
}

func TestHolderProcessWaitsForProcessReadyBeforeFirstRegistryMutation(t *testing.T) {
	registry := &holderProcessTestRegistry{}
	status := &mutableHolderProcessStatus{status: holdercontrol.ProcessStatus{SnapshotRevision: 1, CapacityWeight: 1}}
	process := newHolderProcessWithStatusForTest(t, registry, nil, status, time.Second)
	started := make(chan error, 1)
	go func() { started <- process.Start(context.Background()) }()
	time.Sleep(25 * time.Millisecond)
	if got := registry.registerCount(); got != 0 {
		t.Fatalf("not-ready cold start registered holder %d times", got)
	}
	status.set(holdercontrol.ProcessStatus{ProcessReady: true, SnapshotRevision: 1, CapacityWeight: 1})
	select {
	case err := <-started:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("ready cold start did not register")
	}
	if got := registry.registerCount(); got != 1 {
		t.Fatalf("ready cold start registrations=%d", got)
	}
}

func TestHolderProcessCanExposeWarmingControlBeforeAgentRegistration(t *testing.T) {
	registry := &holderProcessTestRegistry{}
	status := &mutableHolderProcessStatus{status: holdercontrol.ProcessStatus{SnapshotRevision: 1, CapacityWeight: 1}}
	process := newHolderProcessWithStatusForTest(t, registry, nil, status, time.Second)
	if err := process.StartControlServer(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := registry.registerCount(); got != 0 {
		t.Fatalf("control-only WARMING start registered holder %d times", got)
	}
	if process.Ready() {
		t.Fatal("control-only WARMING start advertised data-plane readiness")
	}
	status.set(holdercontrol.ProcessStatus{ProcessReady: true, SnapshotRevision: 2, CapacityWeight: 1})
	if err := process.StartAgent(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := registry.registerCount(); got != 1 {
		t.Fatalf("ready Agent registrations=%d", got)
	}
}

func TestHolderProcessReadinessTimeoutLeavesBootUnregistered(t *testing.T) {
	registry := &holderProcessTestRegistry{}
	status := &mutableHolderProcessStatus{status: holdercontrol.ProcessStatus{SnapshotRevision: 1, CapacityWeight: 1}}
	process := newHolderProcessWithStatusForTest(t, registry, nil, status, 20*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := process.Start(ctx); err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("not-ready cold start err=%v", err)
	}
	if got := registry.registerCount(); got != 0 {
		t.Fatalf("timed-out cold start registered holder %d times", got)
	}
	if process.Ready() {
		t.Fatal("timed-out cold start remained ready")
	}
}

func TestHolderProcessRunningServeFailureFailsClosed(t *testing.T) {
	registry := &holderProcessTestRegistry{}
	failServe := make(chan struct{})
	process := newHolderProcessForTest(t, registry, func(_ *transportSeamServer, listener net.Listener) error {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		_ = conn.Close()
		<-failServe
		return errors.New("injected running serve failure")
	})
	if err := process.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := registry.registerCount(); got != 1 {
		t.Fatalf("running Holder registration count=%d want 1", got)
	}
	if !process.Ready() {
		t.Fatal("running Holder never became ready")
	}
	close(failServe)
	waitHolderProcessCondition(t, func() bool { return process.TerminalError() != nil }, "Serve terminal error")
	if process.Ready() {
		t.Fatal("Serve failure left Holder ready")
	}
	waitHolderProcessCondition(t, func() bool {
		record := registry.snapshot()
		return (record.State == core.HolderStateDraining || record.State == core.HolderStateDrained) &&
			!record.ProcessReady && record.Draining
	}, "durable Serve-failure drain")
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := process.GracefulStop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestHolderProcessServeFailureDuringRegisterDrainsDurableWarmingRecord(t *testing.T) {
	registerEntered := make(chan struct{})
	registerRelease := make(chan struct{})
	registry := &holderProcessTestRegistry{registerEntered: registerEntered, registerRelease: registerRelease}
	failServe := make(chan struct{})
	process := newHolderProcessForTest(t, registry, func(_ *transportSeamServer, listener net.Listener) error {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		_ = conn.Close()
		<-failServe
		return errors.New("injected Serve failure during Register")
	})
	startDone := make(chan error, 1)
	go func() { startDone <- process.Start(context.Background()) }()
	select {
	case <-registerEntered:
	case <-time.After(time.Second):
		t.Fatal("HolderAgent did not enter Register")
	}
	if record := registry.snapshot(); record.State != core.HolderStateWarming || record.ProcessReady {
		t.Fatalf("blocked Register durable baseline=%+v", record)
	}
	close(failServe)
	waitHolderProcessCondition(t, func() bool { return process.TerminalError() != nil }, "Serve terminal during Register")
	close(registerRelease)
	select {
	case err := <-startDone:
		if err == nil {
			t.Fatal("Serve failure racing Register unexpectedly started Holder")
		}
	case <-time.After(time.Second):
		t.Fatal("Holder Start did not join the failed Register race")
	}
	waitHolderProcessCondition(t, func() bool {
		record := registry.snapshot()
		return (record.State == core.HolderStateDraining || record.State == core.HolderStateDrained) &&
			!record.ProcessReady && record.Draining
	}, "durable Register-race drain")
}

func TestHolderProcessAssignmentTerminalStopsReadyAndClosesAgent(t *testing.T) {
	registry := &holderProcessTestRegistry{}
	process := newHolderProcessForTest(t, registry, nil)
	if err := process.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !process.Ready() {
		t.Fatal("running Holder never became ready")
	}
	process.FailClosed(errors.New("injected assignment terminal"))
	if process.Ready() {
		t.Fatal("assignment terminal left Holder ready")
	}
	waitHolderProcessCondition(t, func() bool {
		state := process.Agent.Status().State
		return state == holdercontrol.AgentStateDrained || state == holdercontrol.AgentStateClosed
	}, "HolderAgent fail-closed drain")
	record := registry.snapshot()
	if (record.State != core.HolderStateDraining && record.State != core.HolderStateDrained) || record.ProcessReady || !record.Draining {
		t.Fatalf("assignment terminal durable record=%+v, want DRAINING/DRAINED and process-ready=false", record)
	}
}

func TestHolderProcessAgentStatusIsConstrainedByServerLifecycle(t *testing.T) {
	want := holdercontrol.ProcessStatus{
		ProcessReady: true, ServingShardCount: 7, SnapshotRevision: 19, CapacityWeight: 3,
		ResourceSummary: map[string]string{"cpu": "2"},
	}
	process := &HolderProcess{processStatus: holdercontrol.ProcessStatusSourceFunc(func(context.Context) (holdercontrol.ProcessStatus, error) {
		return want, nil
	})}

	status, err := process.agentProcessStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.ProcessReady || !status.Draining || status.ServingShardCount != want.ServingShardCount ||
		status.SnapshotRevision != want.SnapshotRevision || status.CapacityWeight != want.CapacityWeight || status.ResourceSummary["cpu"] != "2" {
		t.Fatalf("not-started constrained status=%+v", status)
	}

	process.serverReady.Store(true)
	status, err = process.agentProcessStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !status.ProcessReady || status.Draining {
		t.Fatalf("healthy server altered real status=%+v", status)
	}

	process.recordTerminal(errors.New("injected terminal"))
	status, err = process.agentProcessStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.ProcessReady || !status.Draining || status.ServingShardCount != want.ServingShardCount || status.SnapshotRevision != want.SnapshotRevision {
		t.Fatalf("terminal constrained status=%+v", status)
	}
}

func TestHolderRuntimeProcessStatusLifecycleFlags(t *testing.T) {
	base := holdercontrol.ProcessStatus{
		ProcessReady: true, Draining: true, ServingShardCount: 2, SnapshotRevision: 9,
		CapacityWeight: 4, ResourceSummary: map[string]string{"facts": "keep"},
	}
	cases := []struct {
		name                              string
		runtimeHealthy, assignmentHealthy bool
		shutdownDraining                  bool
		wantProcessReady, wantDraining    bool
	}{
		{name: "healthy", runtimeHealthy: true, assignmentHealthy: true, wantProcessReady: true},
		{name: "assignment-outage", runtimeHealthy: true, assignmentHealthy: false},
		{name: "runtime-fatal", runtimeHealthy: false, assignmentHealthy: true, wantDraining: true},
		{name: "shutdown", runtimeHealthy: true, assignmentHealthy: true, shutdownDraining: true, wantDraining: true},
		{name: "assignment-and-runtime", runtimeHealthy: false, assignmentHealthy: false, wantDraining: true},
		{name: "shutdown-during-assignment-outage", runtimeHealthy: true, assignmentHealthy: false, shutdownDraining: true, wantDraining: true},
		{name: "all-unhealthy", runtimeHealthy: false, assignmentHealthy: false, shutdownDraining: true, wantDraining: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := HolderRuntimeProcessStatus(base, tc.runtimeHealthy, tc.assignmentHealthy, tc.shutdownDraining)
			if got.ProcessReady != tc.wantProcessReady || got.Draining != tc.wantDraining {
				t.Fatalf("flags ProcessReady=%v Draining=%v want ProcessReady=%v Draining=%v",
					got.ProcessReady, got.Draining, tc.wantProcessReady, tc.wantDraining)
			}
			if got.ServingShardCount != base.ServingShardCount || got.SnapshotRevision != base.SnapshotRevision ||
				got.CapacityWeight != base.CapacityWeight || got.ResourceSummary["facts"] != "keep" {
				t.Fatalf("lifecycle derivation mutated real facts: %+v", got)
			}
		})
	}
}

func TestHolderSnapshotRevisionEvidenceWithholdsTheEmptyingRevision(t *testing.T) {
	cases := []struct {
		name     string
		applied  uint64
		serving  uint32
		desired  int
		expected uint64
	}{
		{name: "serving-what-it-wants", applied: 9, serving: 2, desired: 2, expected: 9},
		{name: "target-of-a-move", applied: 9, serving: 2, desired: 3, expected: 9},
		{name: "emptying-shards-not-gone-yet", applied: 9, serving: 1, desired: 0, expected: 0},
		{name: "emptied", applied: 9, serving: 0, desired: 0, expected: 9},
		{name: "warming-up", applied: 9, serving: 0, desired: 4, expected: 9},
		{name: "no-consistent-observation", applied: 0, serving: 2, desired: 2, expected: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := HolderSnapshotRevisionEvidence(tc.applied, tc.serving, tc.desired); got != tc.expected {
				t.Fatalf("evidence revision=%d, want %d", got, tc.expected)
			}
		})
	}
}

func TestTransientAssignmentOutageFailsClosedWithoutDurableDrain(t *testing.T) {
	registry := &holderProcessTestRegistry{}
	process := newHolderProcessForTest(t, registry, nil)
	statusSource := &mutableHolderProcessStatus{status: HolderRuntimeProcessStatus(holdercontrol.ProcessStatus{
		ServingShardCount: 0, SnapshotRevision: 1,
	}, true, true, false)}
	process.processStatus = statusSource
	if err := process.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	statusSource.set(HolderRuntimeProcessStatus(holdercontrol.ProcessStatus{
		ServingShardCount: 1, SnapshotRevision: 2,
	}, true, true, false))
	waitHolderProcessCondition(t, func() bool {
		record := registry.snapshot()
		return record.State == core.HolderStateServing && record.ProcessReady && !record.Draining
	}, "durable SERVING before assignment outage")
	serving := registry.snapshot()

	outage := HolderRuntimeProcessStatus(holdercontrol.ProcessStatus{
		ServingShardCount: 1, SnapshotRevision: 2,
	}, true, false, false)
	if outage.ProcessReady || outage.Draining {
		t.Fatalf("transient assignment outage requested durable drain: %+v", outage)
	}
	beforeCalls := statusSource.callCount()
	statusSource.set(outage)
	// Cross the normal three-failure threshold and prove each fail-closed,
	// non-draining observation is deferred without consuming the Agent failure
	// budget or mutating the durable SERVING record.
	waitHolderProcessCondition(t, func() bool {
		return statusSource.callCount() >= beforeCalls+4
	}, "assignment outage observed across the terminal failure horizon")
	observed := process.Agent.Status()
	during := registry.snapshot()
	if observed.State != holdercontrol.AgentStateRunning || observed.Unhealthy || observed.ConsecutiveFailures != 0 {
		t.Fatalf("bounded assignment outage consumed Agent terminal budget: %+v", observed)
	}
	if during.State != core.HolderStateServing || during.Draining || !during.ProcessReady ||
		during.Sequence != serving.Sequence {
		t.Fatalf("assignment outage mutated durable SERVING: before=%+v during=%+v", serving, during)
	}
	statusSource.set(HolderRuntimeProcessStatus(holdercontrol.ProcessStatus{
		ServingShardCount: 1, SnapshotRevision: 3,
	}, true, true, false))
	waitHolderProcessCondition(t, func() bool {
		record := registry.snapshot()
		status := process.Agent.Status()
		return record.State == core.HolderStateServing && record.ProcessReady && !record.Draining &&
			record.SnapshotRevision == 3 && record.Sequence > serving.Sequence &&
			status.State == holdercontrol.AgentStateRunning && !status.Unhealthy && status.ConsecutiveFailures == 0 &&
			status.Holder.SnapshotRevision == 3
	}, "durable SERVING heartbeat and Agent recovery after assignment outage")
}

func TestRuntimeFatalStatusDurablyDrainsHolderWithoutLeaseExpiry(t *testing.T) {
	registry := &holderProcessTestRegistry{}
	process := newHolderProcessForTest(t, registry, nil)
	statusSource := &mutableHolderProcessStatus{status: HolderRuntimeProcessStatus(holdercontrol.ProcessStatus{
		ServingShardCount: 0, SnapshotRevision: 1, CapacityWeight: 2,
		ResourceSummary: map[string]string{"facts": "real"},
	}, true, true, false)}
	process.processStatus = statusSource
	if err := process.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitHolderProcessCondition(t, func() bool {
		return registry.snapshot().State == core.HolderStateReadyZeroShard
	}, "initial durable READY_ZERO_SHARD")

	statusSource.set(HolderRuntimeProcessStatus(holdercontrol.ProcessStatus{
		ServingShardCount: 1, SnapshotRevision: 2, CapacityWeight: 3,
		ResourceSummary: map[string]string{"facts": "serving"},
	}, true, true, false))
	waitHolderProcessCondition(t, func() bool {
		record := registry.snapshot()
		return record.State == core.HolderStateServing && record.ProcessReady && !record.Draining
	}, "durable SERVING")
	serving := registry.snapshot()

	fatalStatus := HolderRuntimeProcessStatus(holdercontrol.ProcessStatus{
		// These are the real post-fatal facts; only lifecycle flags are derived.
		ServingShardCount: 0, SnapshotRevision: 3, CapacityWeight: 7,
		ResourceSummary: map[string]string{"facts": "fatal"},
	}, false, true, false)
	if fatalStatus.ProcessReady || !fatalStatus.Draining || fatalStatus.SnapshotRevision != 3 ||
		fatalStatus.CapacityWeight != 7 || fatalStatus.ResourceSummary["facts"] != "fatal" {
		t.Fatalf("runtime-fatal derived status=%+v", fatalStatus)
	}
	statusSource.set(fatalStatus)
	waitHolderProcessCondition(t, func() bool {
		record := registry.snapshot()
		return record.State == core.HolderStateDraining && !record.ProcessReady && record.Draining
	}, "runtime-fatal durable DRAINING")
	draining := registry.snapshot()
	if draining.Sequence <= serving.Sequence || !draining.LeaseUntil.Equal(serving.LeaseUntil) {
		t.Fatalf("DRAINING was not a direct heartbeat transition: serving=%+v draining=%+v", serving, draining)
	}
	if draining.SnapshotRevision != 3 || draining.CapacityWeight != 7 || draining.ResourceSummary["facts"] != "fatal" {
		t.Fatalf("DRAINING lost real facts: %+v", draining)
	}

	closeCtx, cancelClose := context.WithTimeout(context.Background(), time.Second)
	defer cancelClose()
	if err := process.Agent.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	record := registry.snapshot()
	if record.State != core.HolderStateDrained || record.ProcessReady || !record.Draining {
		t.Fatalf("runtime-fatal close durable record=%+v", record)
	}
}

func TestShutdownDrainingStatusDurablyDrainsHolder(t *testing.T) {
	registry := &holderProcessTestRegistry{}
	process := newHolderProcessForTest(t, registry, nil)
	statusSource := &mutableHolderProcessStatus{status: HolderRuntimeProcessStatus(holdercontrol.ProcessStatus{
		ServingShardCount: 0, SnapshotRevision: 1, CapacityWeight: 2,
		ResourceSummary: map[string]string{"facts": "boot"},
	}, true, true, false)}
	process.processStatus = statusSource
	if err := process.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	statusSource.set(HolderRuntimeProcessStatus(holdercontrol.ProcessStatus{
		ServingShardCount: 1, SnapshotRevision: 2, CapacityWeight: 3,
		ResourceSummary: map[string]string{"facts": "serving"},
	}, true, true, false))
	waitHolderProcessCondition(t, func() bool {
		record := registry.snapshot()
		return record.State == core.HolderStateServing && record.ProcessReady && !record.Draining
	}, "durable SERVING before planned shutdown drain")
	serving := registry.snapshot()

	shutdownStatus := HolderRuntimeProcessStatus(holdercontrol.ProcessStatus{
		ServingShardCount: 0, SnapshotRevision: 4, CapacityWeight: 5,
		ResourceSummary: map[string]string{"facts": "shutdown"},
	}, true, true, true)
	if shutdownStatus.ProcessReady || !shutdownStatus.Draining || shutdownStatus.SnapshotRevision != 4 ||
		shutdownStatus.CapacityWeight != 5 || shutdownStatus.ResourceSummary["facts"] != "shutdown" {
		t.Fatalf("shutdown derived status=%+v", shutdownStatus)
	}
	statusSource.set(shutdownStatus)
	waitHolderProcessCondition(t, func() bool {
		record := registry.snapshot()
		return record.State == core.HolderStateDraining && !record.ProcessReady && record.Draining
	}, "shutdown durable DRAINING")
	draining := registry.snapshot()
	if draining.Sequence <= serving.Sequence || !draining.LeaseUntil.Equal(serving.LeaseUntil) {
		t.Fatalf("DRAINING was not a direct heartbeat transition: serving=%+v draining=%+v", serving, draining)
	}
	if draining.SnapshotRevision != 4 || draining.CapacityWeight != 5 || draining.ResourceSummary["facts"] != "shutdown" {
		t.Fatalf("DRAINING lost real shutdown facts: %+v", draining)
	}
}

// Alias the sealed server solely to keep the injected Serve test function
// readable while retaining the exact production callback type.
type transportSeamServer = transport.SealedGRPCServer

func newHolderProcessForTest(t *testing.T, registry *holderProcessTestRegistry, serve func(*transportSeamServer, net.Listener) error) *HolderProcess {
	t.Helper()
	return newHolderProcessWithStatusForTest(t, registry, serve,
		holdercontrol.ProcessStatusSourceFunc(func(context.Context) (holdercontrol.ProcessStatus, error) {
			return holdercontrol.ProcessStatus{ProcessReady: true, SnapshotRevision: 1, CapacityWeight: 1}, nil
		}), time.Second)
}

func newHolderProcessWithStatusForTest(t *testing.T, registry *holderProcessTestRegistry, serve func(*transportSeamServer, net.Listener) error, status holdercontrol.ProcessStatusSource, startupTimeout time.Duration) *HolderProcess {
	t.Helper()
	certFile, keyFile, caFile := writeHolderProcessTLSFiles(t, x509.Certificate{DNSNames: []string{"holder.test"}})
	service, err := holdercontrol.NewLocalService(holdercontrol.LocalServiceOptions{
		Authorizer: holderProcessTestAuthorizer{}, LocalHolderID: "holder-a", LocalBootID: "boot-a",
		Now: time.Now, OperationTimeout: time.Second,
		Player: holdercontrol.DomainRuntimeOptions{Runtime: holderProcessTestRuntime{}, ShardCount: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(1_800_000_000_000)
	process, err := NewHolderProcess(HolderProcessOptions{
		ListenAddr: "127.0.0.1:0", ServerCertFile: certFile, ServerKeyFile: keyFile, ClientCAFile: caFile,
		ControllerIdentity: "controller.test", LocalService: service, Registry: registry,
		ProcessStatus: status, StartupReadyTimeout: startupTimeout,
		AgentOptions: holdercontrol.HolderAgentOptions{
			Domain: actorrouting.DomainPlayer, HolderID: "holder-a", LogicEndpoint: "holder.test:9000",
			HolderControlEndpoint: "holder.test:9001",
			TLSDNSIdentity:        "holder.test", BootID: func() (string, error) { return "boot-a", nil },
			HeartbeatInterval: 20 * time.Millisecond, HeartbeatTimeout: 5 * time.Millisecond,
			CloseTimeout: 100 * time.Millisecond, ClosePollInterval: 2 * time.Millisecond,
			LeaseTTL: 100 * time.Millisecond, Now: func() time.Time { return now },
		},
		Serve: serve,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = process.GracefulStop(ctx)
	})
	return process
}

func TestValidateHolderCertificateIdentityRejectsAmbiguousIdentities(t *testing.T) {
	validURI, err := url.Parse("spiffe://holder.test/service")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name     string
		identity string
		template x509.Certificate
		wantErr  bool
	}{
		{name: "exact", identity: "holder.test", template: x509.Certificate{DNSNames: []string{"holder.test"}}},
		{name: "mismatch", identity: "other.test", template: x509.Certificate{DNSNames: []string{"holder.test"}}, wantErr: true},
		{name: "wildcard", identity: "holder.test", template: x509.Certificate{DNSNames: []string{"*.test"}}, wantErr: true},
		{name: "multi-dns", identity: "holder.test", template: x509.Certificate{DNSNames: []string{"holder.test", "other.test"}}, wantErr: true},
		{name: "uri", identity: "holder.test", template: x509.Certificate{DNSNames: []string{"holder.test"}, URIs: []*url.URL{validURI}}, wantErr: true},
		{name: "ip", identity: "holder.test", template: x509.Certificate{DNSNames: []string{"holder.test"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}, wantErr: true},
		{name: "email", identity: "holder.test", template: x509.Certificate{DNSNames: []string{"holder.test"}, EmailAddresses: []string{"holder@test"}}, wantErr: true},
		{name: "common-name", identity: "holder.test", template: x509.Certificate{Subject: pkix.Name{CommonName: "holder.test"}, DNSNames: []string{"holder.test"}}, wantErr: true},
		{name: "noncanonical-agent", identity: "Holder.test", template: x509.Certificate{DNSNames: []string{"holder.test"}}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			certificate := makeHolderProcessTLSCertificate(t, tc.template)
			err := validateHolderCertificateIdentity([]tls.Certificate{certificate}, tc.identity)
			if (err != nil) != tc.wantErr {
				t.Fatalf("validation err=%v wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func writeHolderProcessTLSFiles(t *testing.T, leafTemplate x509.Certificate) (string, string, string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	caTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test-ca"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, &caTemplate, &caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate.SerialNumber = big.NewInt(2)
	leafTemplate.NotBefore = now.Add(-time.Hour)
	leafTemplate.NotAfter = now.Add(time.Hour)
	leafTemplate.KeyUsage = x509.KeyUsageDigitalSignature
	leafTemplate.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	leafDER, err := x509.CreateCertificate(rand.Reader, &leafTemplate, &caTemplate, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	certFile := filepath.Join(directory, "holder.crt")
	keyFile := filepath.Join(directory, "holder.key")
	caFile := filepath.Join(directory, "client-ca.crt")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile, caFile
}

func makeHolderProcessTLSCertificate(t *testing.T, template x509.Certificate) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template.SerialNumber = big.NewInt(1)
	template.NotBefore = time.Now().Add(-time.Hour)
	template.NotAfter = time.Now().Add(time.Hour)
	template.KeyUsage = x509.KeyUsageDigitalSignature
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func waitHolderProcessCondition(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !condition() {
		t.Fatalf("timed out waiting for %s", description)
	}
}
