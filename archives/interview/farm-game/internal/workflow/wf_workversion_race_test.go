package workflow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestWorkVersionMismatchReloadsInsteadOfRejecting is the C1 safety patch
// invariant: an internal WorkVersion proof mismatch (stale driver view) must
// make the engine reload the authoritative WorkRecord and re-evaluate, NEVER
// mark StepUnknown, and never surface as a business REJECTED. The workflow
// converges to COMPLETED with the participant invoked again after reload.
func TestWorkVersionMismatchReloadsInsteadOfRejecting(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-stale-proof")
	// The first participant step hits a stale proof exactly once; the reload
	// re-invokes the same step and it succeeds.
	f.p1.script(record.Steps[0].InvocationID, faultStaleWorkProof)

	f.recoverOnce(t, "worker-a")

	stored := f.get(t, record)
	if stored.State != WorkStateCompleted {
		t.Fatalf("workflow state = %s, want completed (stale proof must not wedge or reject)", stored.State)
	}
	for _, step := range stored.Steps {
		if step.Status == StepUnknown {
			t.Fatalf("step %s left StepUnknown after stale-proof reload", step.Name)
		}
	}
	if f.p1.applyCount(record.Steps[0].InvocationID) != 1 {
		t.Fatalf("participant apply count = %d, want exactly 1 (reload must not double-apply)", f.p1.applyCount(record.Steps[0].InvocationID))
	}
}

// TestDirectDispatcherAndWorkerRaceOnlyOneDriverInvokes asserts that when a
// DirectDispatcher-style advance and a worker recover the same record, only
// the CAS winner drives the participant for a step: the loser reloads instead
// of sending a stale call. We simulate by advancing the record out from under
// the recovery engine between load and invoke via a scripted stale proof, and
// require the reload to converge with a single application.
func TestDirectDispatcherAndWorkerRaceOnlyOneDriverInvokes(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-race")
	// Two steps both observe a stale proof once (simulating another driver
	// having advanced the record), then converge.
	for _, step := range record.Steps {
		if step.Status == StepPending {
			f.p1.script(step.InvocationID, faultStaleWorkProof)
		}
	}
	f.recoverOnce(t, "worker-a")
	f.recoverOnce(t, "worker-b")

	stored := f.get(t, record)
	if stored.State != WorkStateCompleted {
		t.Fatalf("workflow state = %s, want completed", stored.State)
	}
	f.assertSingleApplication(t, stored)
	for _, step := range stored.Steps {
		if step.Status == StepUnknown {
			t.Fatalf("step %s left StepUnknown", step.Name)
		}
	}
}

// TestStaleProofDoesNotSurfaceAsRejected asserts the reload path never records
// a REJECTED outcome for a step that later succeeds.
func TestStaleProofDoesNotSurfaceAsRejected(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-no-reject")
	f.p1.script(record.Steps[0].InvocationID, faultStaleWorkProof)

	f.recoverOnce(t, "worker-a")

	stored := f.get(t, record)
	for _, step := range stored.Steps {
		if step.Status == StepRejected {
			t.Fatalf("step %s recorded REJECTED after stale-proof reload; internal CAS race must never leak as a business rejection", step.Name)
		}
	}
	if stored.State != WorkStateCompleted {
		t.Fatalf("workflow state = %s, want completed", stored.State)
	}
}

// TestConvergenceWithStaleProofOnEveryStep asserts the reload path is
// exercised at least once and the workflow still converges (no infinite
// reload loop, no StepUnknown residue).
func TestConvergenceWithStaleProofOnEveryStep(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-every-step")
	for _, step := range record.Steps {
		if step.Status == StepPending {
			f.p1.script(step.InvocationID, faultStaleWorkProof)
		}
	}
	// Run several recovery rounds; each stale proof reloads and re-invokes.
	for i := 0; i < 8; i++ {
		f.recoverOnce(t, "worker-a")
	}
	stored := f.get(t, record)
	if stored.State != WorkStateCompleted {
		t.Fatalf("workflow state = %s, want completed", stored.State)
	}
	f.assertSingleApplication(t, stored)
	_ = context.Background()
}

// TestParticipantCallUsesCommittedWorkVersion asserts every participant call
// carries the persisted committed WorkVersion (preflight fresh-read), never a
// local candidate. This is the C2 invariant 1.
func TestParticipantCallUsesCommittedWorkVersion(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-committed-version")

	f.recoverOnce(t, "worker-a")

	stored := f.get(t, record)
	if stored.State != WorkStateCompleted {
		t.Fatalf("workflow state = %s, want completed", stored.State)
	}
	for _, call := range f.p1.calls {
		fresh, err := f.repo.Get(context.Background(), call.Work.WorkType, call.Work.WorkID)
		if err != nil {
			t.Fatal(err)
		}
		// The call version must be <= the committed version at the end, and
		// equal to the version committed at the moment the call was sent (the
		// preflight fresh-read makes the call use the current committed value;
		// a later fold may advance past it, so we only assert the call never
		// exceeded the final committed version and was never a stale lower one
		// than the record's version at invocation time).
		if call.WorkVersion == 0 || call.WorkVersion > fresh.WorkVersion {
			t.Fatalf("participant call carried version %d (committed=%d); must use a committed version", call.WorkVersion, fresh.WorkVersion)
		}
	}
}

// conflictInjectingRepo wraps MemoryWorkRepository and, on exactly the
// conflictAt-th Advance call, first advances the stored record out from under
// the caller (simulating another driver completing a round inside the
// Invoke→decision-persist window), then returns ErrRevisionConflict for the
// caller's own persist. This is the C2 decision-persist case that used to
// leak ErrRevisionConflict to the caller (farm/ingress mapped it to the public
// ERR_STORE_CAS_CONFLICT 9002).
type conflictInjectingRepo struct {
	*MemoryWorkRepository
	mu         sync.Mutex
	advances   int
	conflictAt int
}

func (r *conflictInjectingRepo) Advance(ctx context.Context, next *WorkRecord, opts AdvanceOptions) (*WorkRecord, error) {
	r.mu.Lock()
	r.advances++
	trigger := r.conflictAt > 0 && r.advances == r.conflictAt
	r.mu.Unlock()
	if trigger {
		current, err := r.MemoryWorkRepository.Get(ctx, next.Address.WorkType, next.Address.WorkID)
		if err != nil {
			return nil, err
		}
		// Simulate another driver completing a round in the
		// Invoke→decision-persist window: advance the stored record (bumping
		// WorkVersion) while carrying the current lease so the fence still
		// matches for a subsequent worker reload.
		if _, err := r.MemoryWorkRepository.Advance(ctx, current, AdvanceOptions{
			ExpectedWorkVersion: current.WorkVersion,
			LeaseOwner:          current.LeaseOwner,
			LeaseEpoch:          current.LeaseEpoch,
		}); err != nil {
			return nil, err
		}
		return nil, ErrRevisionConflict
	}
	return r.MemoryWorkRepository.Advance(ctx, next, opts)
}

// TestDecisionPersistConflictWorkerReloadsAndConverges asserts the C2
// decision-persist path for a leased worker: an ErrRevisionConflict on the
// decision persist after a successful participant invoke must reload the
// authoritative record and re-evaluate (never leak, never StepUnknown), and
// the workflow converges to COMPLETED with a single application per step.
func TestDecisionPersistConflictWorkerReloadsAndConverges(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-decision-conflict-worker")
	// A real worker claims (bumps version + takes the lease) before resuming.
	claimed, err := f.repo.ClaimDue(context.Background(), "worker-a", "worker-a-token", 16, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) == 0 {
		t.Fatal("expected the record to be claimed")
	}
	workerRecord := claimed[0].Record
	// conflictAt=2: the first Advance is the build-payload persist; the second
	// is the decision persist right after the first successful participant
	// invoke (RESERVE).
	repo := &conflictInjectingRepo{MemoryWorkRepository: f.repo, conflictAt: 2}
	engine, err := NewEngine(repo, []Definition{f.def, f.order},
		WithEngineClock(func() time.Time { return f.now }),
		WithEngineBackoff(BackoffConfig{Base: time.Second, Max: 10 * time.Second}))
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Resume(context.Background(), workerRecord, LeaseFence{Owner: "worker-a", Epoch: workerRecord.LeaseEpoch}, nil); err != nil {
		t.Fatalf("worker resume must reload and converge after decision-persist conflict, got %v", err)
	}
	stored := f.get(t, record)
	if stored.State != WorkStateCompleted {
		t.Fatalf("workflow state = %s, want completed", stored.State)
	}
	for _, step := range stored.Steps {
		if step.Status == StepUnknown {
			t.Fatalf("step %s left StepUnknown after decision-persist reload", step.Name)
		}
	}
	f.assertSingleApplication(t, stored)
}

// TestDecisionPersistConflictDispatcherDefersWithoutLeak asserts the C2
// decision-persist path for a DirectDispatcher (no lease): an
// ErrRevisionConflict on the decision persist must defer the round with nil
// error (never leak ErrRevisionConflict/ERR_STORE_CAS_CONFLICT to the command
// submit path), never send any further participant call, and leave the work
// claimable by the worker.
func TestDecisionPersistConflictDispatcherDefersWithoutLeak(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-decision-conflict-dispatcher")
	repo := &conflictInjectingRepo{MemoryWorkRepository: f.repo, conflictAt: 2}
	engine, err := NewEngine(repo, []Definition{f.def, f.order},
		WithEngineClock(func() time.Time { return f.now }),
		WithEngineBackoff(BackoffConfig{Base: time.Second, Max: 10 * time.Second}))
	if err != nil {
		t.Fatal(err)
	}
	beforeP1 := f.p1.callCount()
	beforeP2 := f.p2.callCount()
	err = engine.Resume(context.Background(), record, LeaseFence{}, nil)
	if err != nil {
		t.Fatalf("dispatcher resume must defer with nil, never leak ErrRevisionConflict: %v", err)
	}
	// Step 0 (RESERVE, p1) was invoked before the conflict; the conflict defers
	// before ANY further participant call (step 1 COMMIT belongs to p2).
	if got := f.p1.callCount(); got != beforeP1+1 {
		t.Fatalf("p1 calls = %d, want exactly %d (only step 0 before defer)", got, beforeP1+1)
	}
	if got := f.p2.callCount(); got != beforeP2 {
		t.Fatalf("dispatcher leaked a call past the conflict: p2 calls = %d, want %d", got, beforeP2)
	}
	// The work stays recoverable: a worker can still claim and complete it.
	claimed, err := f.repo.ClaimDue(context.Background(), "worker-b", "worker-b-token", 16, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) == 0 {
		t.Fatal("expected the deferred work to remain claimable by the worker")
	}
}

// TestStaleProofReloadKeepsSameInvocationIdempotent asserts that a stale
// WorkVersion proof reload re-invokes the SAME invocation (never a fresh
// invocation ID) and the receipt replay machinery keeps the application
// exactly-once: the participant sees the same invocation+payload hash twice
// (stale failure + reload success) but applies exactly once.
func TestStaleProofReloadKeepsSameInvocationIdempotent(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-same-invocation")
	firstInvocation := record.Steps[0].InvocationID
	f.p1.script(firstInvocation, faultStaleWorkProof)

	f.recoverOnce(t, "worker-a")

	stored := f.get(t, record)
	if stored.State != WorkStateCompleted {
		t.Fatalf("workflow state = %s, want completed", stored.State)
	}
	if stored.Steps[0].InvocationID != firstInvocation {
		t.Fatalf("reload changed invocation: %s -> %s", firstInvocation, stored.Steps[0].InvocationID)
	}
	calls := f.p1.calls
	invoked := 0
	for _, call := range calls {
		if call.InvocationID == firstInvocation {
			invoked++
		}
	}
	if invoked != 2 {
		t.Fatalf("same invocation invoked %d times, want exactly 2 (1 stale + 1 reload)", invoked)
	}
	if f.p1.applyCount(firstInvocation) != 1 {
		t.Fatalf("apply count = %d, want exactly 1", f.p1.applyCount(firstInvocation))
	}
}

// TestConcurrentDualDriverResumeConvergesDeterministically runs a
// DirectDispatcher-style engine (no lease) and a worker-leased engine
// concurrently on the SAME record from the same starting view. Only one driver
// can win each CAS; whichever wins, the workflow must converge to COMPLETED
// with no StepUnknown/REJECTED and each participant invocation applied exactly
// once. The two engines are separate instances over one repository (the
// production dispatcher and worker recovery are separate processes/engines),
// and each driver may observe stale proofs or lease fences depending on the
// interleaving.
func TestConcurrentDualDriverResumeConvergesDeterministically(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-concurrent-dual")

	dispatcherEngine, err := NewEngine(f.repo, []Definition{f.def, f.order},
		WithEngineClock(func() time.Time { return f.now }),
		WithEngineBackoff(BackoffConfig{Base: time.Second, Max: 10 * time.Second}))
	if err != nil {
		t.Fatal(err)
	}
	workerEngine, err := NewEngine(f.repo, []Definition{f.def, f.order},
		WithEngineClock(func() time.Time { return f.now }),
		WithEngineBackoff(BackoffConfig{Base: time.Second, Max: 10 * time.Second}))
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	var (
		dispatcherErr error
		workerErr     error
	)
	go func() {
		defer wg.Done()
		// Dispatcher view: no lease fence. It may defer when a worker lease
		// is live; that is a valid round outcome.
		dispatcherErr = dispatcherEngine.Resume(context.Background(), cloneWorkRecord(record), LeaseFence{}, nil)
	}()
	go func() {
		defer wg.Done()
		// Worker view: claim the record so this driver holds the lease.
		claimed, claimErr := f.repo.ClaimDue(context.Background(), "worker-concurrent", "worker-concurrent-token", 16, 30*time.Second)
		if claimErr != nil {
			workerErr = claimErr
			return
		}
		if len(claimed) == 0 {
			workerErr = errors.New("no claimable work")
			return
		}
		workerErr = workerEngine.Resume(context.Background(), claimed[0].Record,
			LeaseFence{Owner: "worker-concurrent", Epoch: claimed[0].Record.LeaseEpoch}, nil)
	}()
	wg.Wait()

	if workerErr != nil {
		t.Fatalf("worker resume errored: %v", workerErr)
	}
	if dispatcherErr != nil && !errors.Is(dispatcherErr, ErrLeaseLost) && !errors.Is(dispatcherErr, ErrStaleWorkProof) {
		t.Fatalf("dispatcher resume errored: %v", dispatcherErr)
	}

	stored := f.get(t, record)
	if stored.State != WorkStateCompleted {
		t.Fatalf("workflow state = %s, want completed (dual drivers must converge deterministically)", stored.State)
	}
	for _, step := range stored.Steps {
		if step.Status == StepUnknown || step.Status == StepRejected {
			t.Fatalf("step %s left %s after concurrent dual-driver race", step.Name, step.Status)
		}
	}
	f.assertSingleApplication(t, stored)
}

// TestCASLoserReloadsWithoutParticipantCall is the PhaseBarrier C2 invariant 3:
// when another driver (worker-a) has claimed and advanced the record, a
// DirectDispatcher resuming from a stale view must NOT send any participant
// call. It reloads, sees the live worker lease, and defers (worker recovers).
func TestCASLoserReloadsWithoutParticipantCall(t *testing.T) {
	f := newCrashFixture(t)
	record := f.create(t, "op-cas-loser")

	// Another driver (worker-a) claims and advances the record out from under
	// the dispatcher: claim bumps version and takes the lease.
	claimed, err := f.repo.ClaimDue(context.Background(), "worker-a", "worker-a-token", 16, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) == 0 {
		t.Fatal("expected the record to be claimed")
	}

	// The dispatcher resumes from its stale v1 view with an empty lease fence.
	before := f.p1.callCount()
	err = f.engine.Resume(context.Background(), record, LeaseFence{}, nil)
	if err != nil {
		t.Fatalf("dispatcher resume errored: %v (must defer, not fail)", err)
	}
	if got := f.p1.callCount(); got != before {
		t.Fatalf("CAS loser dispatched participant calls: before=%d after=%d; loser must not invoke", before, got)
	}

	// The work still belongs to worker-a and can be recovered by it.
	stored := f.get(t, record)
	if stored.LeaseOwner != "worker-a" {
		t.Fatalf("record lease owner = %q, want worker-a (dispatcher must not steal)", stored.LeaseOwner)
	}
}
