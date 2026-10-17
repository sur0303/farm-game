package workflow

import (
	"context"
	"errors"
	"testing"
	"time"
)

// crashFixture wires the generic framework over hermetic storage with a
// controllable trusted clock. A "process crash" is simulated by abandoning
// the engine/dispatcher instance and building a fresh one over the same
// repository.
type crashFixture struct {
	now    time.Time
	repo   *MemoryWorkRepository
	engine *Engine
	p1     *fakeParticipant
	p2     *fakeParticipant
	def    *testWorkflowDefinition
	order  *testOrderDefinition
}

func newCrashFixture(t *testing.T) *crashFixture {
	t.Helper()
	f := &crashFixture{now: time.UnixMilli(1_800_000_000_000)}
	f.repo = NewMemoryWorkRepository(func() time.Time { return f.now })
	f.p1 = newFakeParticipant("p1")
	f.p2 = newFakeParticipant("p2")
	f.def = newTestWorkflowDefinition(f.p1, f.p2)
	f.order = &testOrderDefinition{participant: f.p1}
	engine, err := NewEngine(f.repo, []Definition{f.def, f.order},
		WithEngineClock(func() time.Time { return f.now }),
		WithEngineBackoff(BackoffConfig{Base: time.Second, Max: 10 * time.Second}))
	if err != nil {
		t.Fatal(err)
	}
	f.engine = engine
	return f
}

func (f *crashFixture) create(t *testing.T, operationID string) *WorkRecord {
	t.Helper()
	return testWorkRecord(t, f.repo, f.engine, f.def, operationID, f.now)
}

func (f *crashFixture) createOrder(t *testing.T, operationID string) *WorkRecord {
	t.Helper()
	return testWorkRecord(t, f.repo, f.engine, f.order, operationID, f.now)
}

func (f *crashFixture) recoverOnce(t *testing.T, owner string) RecoveryStats {
	t.Helper()
	loop, err := NewRecoveryLoop(f.repo, map[string]WorkHandler{
		testWorkType:  EngineHandler{Engine: f.engine},
		testOrderType: EngineHandler{Engine: f.engine},
	}, RecoveryConfig{Owner: owner, Token: owner + "-token", LeaseTTL: 30 * time.Second, ClaimLimit: 16})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := loop.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return stats
}

func (f *crashFixture) get(t *testing.T, record *WorkRecord) *WorkRecord {
	t.Helper()
	stored, err := f.repo.Get(context.Background(), record.Address.WorkType, record.Address.WorkID)
	if err != nil {
		t.Fatal(err)
	}
	return stored
}

func (f *crashFixture) assertSingleApplication(t *testing.T, record *WorkRecord) {
	t.Helper()
	for _, step := range record.Steps {
		if step.Name == stepRelease && step.Status == StepPending {
			continue
		}
		for _, p := range []*fakeParticipant{f.p1, f.p2} {
			if got := p.applyCount(step.InvocationID); got > 1 {
				t.Fatalf("step %s applied %d times", step.Name, got)
			}
		}
	}
}

// Crash point: before/after CreateIfAbsent. The same business intent must
// converge to exactly one durable record.
func TestCreateIfAbsentConvergesAcrossCreateCrash(t *testing.T) {
	f := newCrashFixture(t)
	first := f.create(t, "op-create")
	again, created, err := f.engine.Create(context.Background(), f.def, first.Address,
		first.SubjectPlayerID, first.OperationID, first.Payload, first.PayloadHash, time.Time{})
	if err != nil || created || again.WorkVersion != first.WorkVersion {
		t.Fatalf("re-create=%v created=%v err=%v", again, created, err)
	}
	if _, _, err := f.engine.Create(context.Background(), f.def, first.Address,
		first.SubjectPlayerID, first.OperationID, []byte(`{"different":true}`), testPayloadHash("different"), time.Time{}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different payload reused work id: %v", err)
	}
}

// Crash point: workflow created but the coordinator crashed before any
// dispatch. The recovery loop must discover it from the due index.
func TestCrashAfterCreateBeforeAnyDispatch(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-after-create")
	stats := f.recoverOnce(t, "worker-1")
	if stats.Claimed != 1 || stats.Completed != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	stored := f.get(t, record)
	if stored.State != WorkStateCompleted {
		t.Fatalf("state=%s", stored.State)
	}
	f.assertSingleApplication(t, stored)
	if f.p1.totalApplies() != 2 || f.p2.totalApplies() != 1 {
		t.Fatalf("applies p1=%d p2=%d", f.p1.totalApplies(), f.p2.totalApplies())
	}
}

// Crash point: source fact + order committed, but the fast-path dispatch
// never happened (queue full). The durable due index drives recovery.
func TestOrderCommittedButDispatchDropped(t *testing.T) {
	f := newCrashFixture(t)
	order := f.createOrder(t, "op-order-dropped")
	dispatcher, err := NewDirectDispatcher(f.repo, f.engine, DispatcherConfig{MaxItems: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	other := f.createOrder(t, "op-order-other")
	if err := dispatcher.Enqueue(other.Address, 1); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Enqueue(order.Address, 1); !errors.Is(err, ErrDispatcherFullItems) {
		t.Fatalf("full queue accepted item: %v", err)
	}
	// The dropped order is recovered by the worker model, not by the queue.
	stats := f.recoverOnce(t, "worker-1")
	if stats.Completed != 2 {
		t.Fatalf("stats=%+v", stats)
	}
	if f.get(t, order).State != WorkStateCompleted || f.p1.totalApplies() != 2 {
		t.Fatalf("order=%s applies=%d", f.get(t, order).State, f.p1.totalApplies())
	}
}

// Crash point: participant applied the step but the response was lost.
// Recovery must resolve through the receipt, not re-apply.
func TestApplyThenResponseLostResolvesViaReceipt(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-lost-response")
	reserveInvocation := testInvocationID(t, testWorkType, record.Address.WorkID, stepReserve)
	f.p1.script(reserveInvocation, faultApplyLoseResponse)

	if err := f.engine.Resume(context.Background(), f.get(t, record), LeaseFence{}, nil); !errors.Is(err, ErrRetryScheduled) {
		t.Fatalf("first resume=%v", err)
	}
	stored := f.get(t, record)
	if stored.State != WorkStateStepUnknown || stored.Steps[0].Status != StepUnknown {
		t.Fatalf("after lost response: state=%s step=%s", stored.State, stored.Steps[0].Status)
	}
	f.now = f.now.Add(2 * time.Second)
	stats := f.recoverOnce(t, "worker-1")
	if stats.Completed != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	stored = f.get(t, record)
	if stored.State != WorkStateCompleted {
		t.Fatalf("state=%s", stored.State)
	}
	if got := f.p1.applyCount(reserveInvocation); got != 1 {
		t.Fatalf("reserve applied %d times", got)
	}
	f.assertSingleApplication(t, stored)
}

// Crash point: participant reports TIMEOUT_UNKNOWN. The workflow must not
// advance past the unknown step until the same invocation resolves.
func TestParticipantUnknownBlocksAdvanceUntilResolved(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-unknown")
	commitInvocation := testInvocationID(t, testWorkType, record.Address.WorkID, stepCommit)
	f.p2.script(commitInvocation, faultTimeoutUnknown)

	if err := f.engine.Resume(context.Background(), f.get(t, record), LeaseFence{}, nil); !errors.Is(err, ErrRetryScheduled) {
		t.Fatalf("first resume=%v", err)
	}
	stored := f.get(t, record)
	if stored.State != WorkStateStepUnknown || stored.CurrentStep != 1 {
		t.Fatalf("state=%s step=%d", stored.State, stored.CurrentStep)
	}
	if f.p1.callCount() != 1 || f.p2.callCount() != 1 {
		t.Fatalf("finalize invoked across unknown boundary: p1=%d p2=%d", f.p1.callCount(), f.p2.callCount())
	}

	f.now = f.now.Add(2 * time.Second)
	stats := f.recoverOnce(t, "worker-1")
	if stats.Completed != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	stored = f.get(t, record)
	if stored.State != WorkStateCompleted {
		t.Fatalf("state=%s", stored.State)
	}
	if got := f.p2.applyCount(commitInvocation); got != 1 {
		t.Fatalf("commit applied %d times", got)
	}
	if got := f.p1.callCount(); got != 2 { // RESERVE + FINALIZE
		t.Fatalf("p1 calls=%d", got)
	}
	if got := f.p2.callCount(); got != 2 { // unknown + receipt replay
		t.Fatalf("p2 calls=%d", got)
	}
	f.assertSingleApplication(t, stored)
}

// Crash point: worker A's lease expires mid-recovery; worker B takes over.
// A's late advance must be fenced and effects must stay exactly-once.
func TestLeaseInvalidationHandsOverWithoutDoubleApply(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-lease-handover")
	claimedA, err := f.repo.ClaimDue(context.Background(), "worker-a", "token-a", 1, 3*time.Second)
	if err != nil || len(claimedA) != 1 {
		t.Fatalf("claimA=%v err=%v", claimedA, err)
	}
	f.now = f.now.Add(4 * time.Second) // A's lease expires (trusted time)
	claimedB, err := f.repo.ClaimDue(context.Background(), "worker-b", "token-b", 1, 30*time.Second)
	if err != nil || len(claimedB) != 1 {
		t.Fatalf("claimB=%v err=%v", claimedB, err)
	}
	fenceA := LeaseFence{Owner: "worker-a", Epoch: claimedA[0].Record.LeaseEpoch}
	err = f.engine.Resume(context.Background(), claimedA[0].Record, fenceA, nil)
	if !errors.Is(err, ErrLeaseLost) && !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale worker advanced: %v", err)
	}
	if f.p1.totalApplies() != 0 || f.p2.totalApplies() != 0 {
		t.Fatalf("stale worker caused effects: p1=%d p2=%d", f.p1.totalApplies(), f.p2.totalApplies())
	}
	fenceB := LeaseFence{Owner: "worker-b", Epoch: claimedB[0].Record.LeaseEpoch}
	if err := f.engine.Resume(context.Background(), claimedB[0].Record, fenceB, nil); err != nil {
		t.Fatalf("worker-b resume: %v", err)
	}
	stored := f.get(t, record)
	if stored.State != WorkStateCompleted {
		t.Fatalf("state=%s", stored.State)
	}
	f.assertSingleApplication(t, stored)
}

// Duplicate recovery: a second worker finds nothing to claim and nothing is
// re-executed.
func TestDuplicateRecoveryDoesNotReExecute(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-duplicate")
	first := f.recoverOnce(t, "worker-1")
	if first.Completed != 1 {
		t.Fatalf("first=%+v", first)
	}
	second := f.recoverOnce(t, "worker-2")
	if second.Claimed != 0 {
		t.Fatalf("second=%+v", second)
	}
	stored := f.get(t, record)
	if stored.State != WorkStateCompleted {
		t.Fatalf("state=%s", stored.State)
	}
	if f.p1.totalApplies() != 2 || f.p2.totalApplies() != 1 {
		t.Fatalf("re-executed: p1=%d p2=%d", f.p1.totalApplies(), f.p2.totalApplies())
	}
}

// flakyAdvanceRepository fails the Nth Advance call to simulate a crash
// between the last participant effect and the workflow's final ack.
type flakyAdvanceRepository struct {
	inner   *MemoryWorkRepository
	failAt  int
	advance int
}

func (r *flakyAdvanceRepository) CreateIfAbsent(ctx context.Context, c *WorkRecord) (*WorkRecord, bool, error) {
	return r.inner.CreateIfAbsent(ctx, c)
}
func (r *flakyAdvanceRepository) Get(ctx context.Context, wt, id string) (*WorkRecord, error) {
	return r.inner.Get(ctx, wt, id)
}
func (r *flakyAdvanceRepository) Advance(ctx context.Context, next *WorkRecord, opts AdvanceOptions) (*WorkRecord, error) {
	r.advance++
	if r.advance == r.failAt {
		return nil, errors.New("storage: connection lost before ack")
	}
	return r.inner.Advance(ctx, next, opts)
}
func (r *flakyAdvanceRepository) ClaimDue(ctx context.Context, owner, token string, limit int, ttl time.Duration) ([]*ClaimedWork, error) {
	return r.inner.ClaimDue(ctx, owner, token, limit, ttl)
}
func (r *flakyAdvanceRepository) RenewLease(ctx context.Context, wt, id, owner, token string, epoch uint64, ttl time.Duration) (*WorkRecord, error) {
	return r.inner.RenewLease(ctx, wt, id, owner, token, epoch, ttl)
}
func (r *flakyAdvanceRepository) PendingDue(ctx context.Context) (int64, error) {
	return r.inner.PendingDue(ctx)
}

// Crash point: all steps applied, but the process died before the final
// workflow ack persisted. Recovery replays the last step from its receipt
// and completes exactly once.
func TestCrashBeforeFinalAckCompletesFromReceipts(t *testing.T) {
	f := newCrashFixture(t)
	flaky := &flakyAdvanceRepository{inner: f.repo, failAt: 6} // 2 advances per step × 3 steps
	engine, err := NewEngine(flaky, []Definition{f.def}, WithEngineClock(func() time.Time { return f.now }))
	if err != nil {
		t.Fatal(err)
	}
	record, _, err := engine.Create(context.Background(), f.def, testAddress(testWorkType, "op-before-ack"),
		9001, "op-before-ack", []byte(`{"x":1}`), testPayloadHash("op-before-ack"), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Resume(context.Background(), record, LeaseFence{}, nil); err == nil {
		t.Fatal("expected crash before ack")
	}
	stored := f.get(t, record)
	if stored.Terminal() {
		t.Fatalf("ack unexpectedly persisted: %s", stored.State)
	}
	finalizeInvocation := testInvocationID(t, testWorkType, record.Address.WorkID, stepFinalize)
	if got := f.p1.applyCount(finalizeInvocation); got != 1 {
		t.Fatalf("finalize applied %d times before crash", got)
	}

	stats := f.recoverOnce(t, "worker-1")
	if stats.Completed != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	stored = f.get(t, record)
	if stored.State != WorkStateCompleted {
		t.Fatalf("state=%s", stored.State)
	}
	if got := f.p1.applyCount(finalizeInvocation); got != 1 {
		t.Fatalf("finalize re-applied after recovery: %d", got)
	}
	f.assertSingleApplication(t, stored)
}

// Crash point: after the final ack. Terminal work leaves the due index and
// is never dispatched again.
func TestCrashAfterAckNeverRedispatches(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-after-ack")
	if stats := f.recoverOnce(t, "worker-1"); stats.Completed != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	callsP1, callsP2 := f.p1.callCount(), f.p2.callCount()
	if stats := f.recoverOnce(t, "worker-2"); stats.Claimed != 0 {
		t.Fatalf("terminal reclaimed: %+v", stats)
	}
	dispatcher, err := NewDirectDispatcher(f.repo, f.engine, DispatcherConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.DispatchOnce(context.Background(), record.Address); err != nil {
		t.Fatal(err)
	}
	if f.p1.callCount() != callsP1 || f.p2.callCount() != callsP2 {
		t.Fatalf("terminal work re-invoked: p1=%d→%d p2=%d→%d", callsP1, f.p1.callCount(), callsP2, f.p2.callCount())
	}
}

// Same-invocation replay must hit the participant receipt: no re-execution,
// original result returned.
func TestSameInvocationReplayHitsReceipt(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-replay")
	if stats := f.recoverOnce(t, "worker-1"); stats.Completed != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	stored := f.get(t, record)
	step := stored.Steps[0]
	call := StepCall{
		Work:         stored.Address,
		WorkVersion:  stored.WorkVersion,
		StepIndex:    0,
		StepName:     step.Name,
		InvocationID: step.InvocationID,
		Payload:      step.Payload,
		PayloadHash:  step.PayloadHash,
	}
	before := f.p1.applyCount(step.InvocationID)
	replayed, err := f.p1.Invoke(context.Background(), call)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Replayed || replayed.ResultHash != step.ResultHash {
		t.Fatalf("replay=%+v want receipt of %x", replayed, step.ResultHash)
	}
	if got := f.p1.applyCount(step.InvocationID); got != before {
		t.Fatalf("replay re-applied: %d→%d", before, got)
	}
	// Same invocation with a different payload hash is a hard conflict.
	call.PayloadHash = testPayloadHash("tampered")
	if _, err := f.p1.Invoke(context.Background(), call); err == nil {
		t.Fatal("tampered invocation accepted")
	}
}

// SUPERSEDED is terminal: never dispatched, never recovered, idempotent.
func TestSupersedeSkipsDispatchAndRecovery(t *testing.T) {
	f := newCrashFixture(t)
	order := f.createOrder(t, "op-superseded")
	superseded, err := f.engine.Supersede(context.Background(), order.Address.WorkType, order.Address.WorkID)
	if err != nil || superseded.State != WorkStateSuperseded {
		t.Fatalf("supersede=%v err=%v", superseded, err)
	}
	if _, err := f.engine.Supersede(context.Background(), order.Address.WorkType, order.Address.WorkID); err != nil {
		t.Fatalf("supersede not idempotent: %v", err)
	}
	dispatcher, err := NewDirectDispatcher(f.repo, f.engine, DispatcherConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.DispatchOnce(context.Background(), order.Address); err != nil {
		t.Fatal(err)
	}
	if stats := f.recoverOnce(t, "worker-1"); stats.Claimed != 0 {
		t.Fatalf("superseded claimed: %+v", stats)
	}
	if f.p1.totalApplies() != 0 {
		t.Fatalf("superseded work executed: %d", f.p1.totalApplies())
	}
}

// A record past its deadline fails closed into the dead-letter terminal
// instead of running more steps.
func TestDeadlineExceededFailsClosed(t *testing.T) {
	f := newCrashFixture(t)
	record, _, err := f.engine.Create(context.Background(), f.def, testAddress(testWorkType, "op-deadline"),
		9001, "op-deadline", []byte(`{"x":1}`), testPayloadHash("op-deadline"), f.now.Add(-time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.engine.Resume(context.Background(), record, LeaseFence{}, nil); err != nil {
		t.Fatal(err)
	}
	stored := f.get(t, record)
	if stored.State != WorkStateFailed || stored.LastErrorClass != "DEADLINE_EXCEEDED" {
		t.Fatalf("state=%s error=%s", stored.State, stored.LastErrorClass)
	}
	if f.p1.totalApplies() != 0 {
		t.Fatalf("dead work executed: %d", f.p1.totalApplies())
	}
	if stats := f.recoverOnce(t, "worker-1"); stats.Claimed != 0 {
		t.Fatalf("dead-letter claimed: %+v", stats)
	}
}

// Deterministic business rejection drives the compensation branch: COMMIT
// rejected → RELEASE → COMPENSATED; FINALIZE never runs.
func TestRejectedCommitRunsCompensationBranch(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-compensate")
	commitInvocation := testInvocationID(t, testWorkType, record.Address.WorkID, stepCommit)
	f.p2.reject(commitInvocation, "PLOT_OCCUPIED")

	if stats := f.recoverOnce(t, "worker-1"); stats.Completed != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	stored := f.get(t, record)
	if stored.State != WorkStateCompensated {
		t.Fatalf("state=%s", stored.State)
	}
	if stored.Steps[1].Status != StepRejected || stored.Steps[3].Status != StepApplied {
		t.Fatalf("steps=%+v", stored.Steps)
	}
	if got := f.p1.callCount(); got != 2 { // RESERVE + RELEASE, never FINALIZE
		t.Fatalf("p1 calls=%d", got)
	}
	if stats := f.recoverOnce(t, "worker-2"); stats.Claimed != 0 {
		t.Fatalf("compensated reclaimed: %+v", stats)
	}
}

// Rejection at the first step terminates without any downstream effect.
func TestRejectedReserveTerminatesImmediately(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-reject-first")
	reserveInvocation := testInvocationID(t, testWorkType, record.Address.WorkID, stepReserve)
	f.p1.reject(reserveInvocation, "INSUFFICIENT_FUNDS")
	if stats := f.recoverOnce(t, "worker-1"); stats.Completed != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	stored := f.get(t, record)
	if stored.State != WorkStateCompensated || f.p2.callCount() != 0 {
		t.Fatalf("state=%s p2=%d", stored.State, f.p2.callCount())
	}
}

// A definition that no longer reproduces the frozen step payload must fail
// closed, never guess.
func TestDefinitionDriftFailsClosed(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-drift")
	commitInvocation := testInvocationID(t, testWorkType, record.Address.WorkID, stepCommit)
	// Freeze the COMMIT payload, then lose the invocation so it stays frozen.
	f.p2.script(commitInvocation, faultCrashBeforeApply)
	if err := f.engine.Resume(context.Background(), f.get(t, record), LeaseFence{}, nil); !errors.Is(err, ErrRetryScheduled) {
		t.Fatalf("first resume=%v", err)
	}
	frozen := f.get(t, record)
	if frozen.Steps[1].PayloadHash == [32]byte{} {
		t.Fatal("step payload not frozen before invoke")
	}
	// Simulate an incompatible deploy: same work type, different payload builder.
	driftedDef := &driftedDefinition{base: f.def}
	engine2, err := NewEngine(f.repo, []Definition{driftedDef}, WithEngineClock(func() time.Time { return f.now }))
	if err != nil {
		t.Fatal(err)
	}
	err = engine2.Resume(context.Background(), f.get(t, record), LeaseFence{}, nil)
	if !errors.Is(err, ErrDefinitionDrift) {
		t.Fatalf("resume=%v, want definition drift", err)
	}
	stored := f.get(t, record)
	if stored.State != WorkStateFailed || stored.LastErrorClass != "DEFINITION_DRIFT" {
		t.Fatalf("state=%s error=%s", stored.State, stored.LastErrorClass)
	}
	if got := f.p2.applyCount(commitInvocation); got != 0 {
		t.Fatalf("drifted definition applied step: %d", got)
	}
}

type driftedDefinition struct{ base *testWorkflowDefinition }

func (d *driftedDefinition) WorkType() string     { return d.base.WorkType() }
func (d *driftedDefinition) StepNames() []string  { return d.base.StepNames() }
func (d *driftedDefinition) Invoker(i int) ParticipantInvoker {
	return d.base.Invoker(i)
}
func (d *driftedDefinition) Decide(r *WorkRecord, i int, res StepResult) (Decision, error) {
	return d.base.Decide(r, i, res)
}
func (d *driftedDefinition) BuildPayload(record *WorkRecord, stepIndex int) ([]byte, error) {
	payload, err := d.base.BuildPayload(record, stepIndex)
	return append(payload, "-drifted"...), err
}
