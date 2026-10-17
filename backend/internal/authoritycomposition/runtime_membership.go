package authoritycomposition

import (
	"context"
	"errors"
	"fmt"
	"time"

	"farmgame/internal/actorrouting"
)

// membershipObserverJitterSeed keeps the per-shard scheduler's deterministic
// jitter stable across tests and restarts.
const membershipObserverJitterSeed int64 = 20260807

// NewMembershipRuntime composes the per-shard membership pipeline as the SOLE
// lease-renewal owner for this holder process:
//
//	RealMembershipAdapter → ShardMembershipReconciler → PerShardLeaseScheduler
//
// Startup order:
//  1. The static startup snapshot is loaded and the local holder identity is
//     validated (buildCompositionCommon).
//  2. The reconciler seals the TopologyFingerprint at its first Observe; any
//     later topology change is rejected (Fail Closed).
//  3. The adapter proves every install against the repository.
//  4. The scheduler renews each installed generation with exactly one entry
//     (a single Renew Owner per generation).
//  5. Observe(启动快照) converges BEFORE ProcessReady becomes true: a
//     registered zero-shard holder is ready immediately; a failed install, a
//     rejected topology, or any non-converged startup fails closed instead of
//     ever serving a half-admitted membership.
//
// Unlike New, this path NEVER starts the legacy global runRenewal loop.
// On construction failure it returns (nil, err) per the conventional Go
// contract, so a failed runtime can never be installed in a composition root.
// Tests probe rollback state through newMembershipRuntimeForTest instead.
func NewMembershipRuntime(ctx context.Context, opts Options) (*Runtime, error) {
	return newMembershipRuntimeWithAssignment(ctx, opts, false, nil)
}

// newMembershipRuntimeForTest is TEST_ONLY: on construction failure it returns
// the fully fail-closed runtime alongside the error so tests can externally
// probe guards / entries / serving counts proving partial-startup rollback.
// Production must use NewMembershipRuntime (nil, err contract).
func newMembershipRuntimeForTest(ctx context.Context, opts Options) (*Runtime, error) {
	return newMembershipRuntimeWithAssignment(ctx, opts, true, nil)
}

func newMembershipRuntime(ctx context.Context, opts Options, keepRuntimeOnFailure bool) (*Runtime, error) {
	return newMembershipRuntimeWithAssignment(ctx, opts, keepRuntimeOnFailure, nil)
}

func newMembershipRuntimeWithAssignment(ctx context.Context, opts Options, keepRuntimeOnFailure bool, assignment *AssignmentComposition) (*Runtime, error) {
	runtime, snapshot, err := buildCompositionCommonWithAssignment(ctx, opts, assignment)
	if err != nil {
		return nil, err
	}

	// Per-shard lease scheduler: the only renewal owner.  A fatal condition
	// (un-fencable lost generation, topology mismatch, entry panic) pulls
	// ProcessReady down through OnFatal.
	baseDelay := opts.LeaseBaseDelay
	if baseDelay <= 0 {
		baseDelay = opts.RenewInterval
	}
	maxDelay := opts.LeaseMaxDelay
	if maxDelay <= 0 {
		maxDelay = max(opts.RenewInterval*2, opts.LeaseTTL)
	}
	jitter := opts.LeaseJitter
	if jitter <= 0 {
		jitter = opts.RenewInterval / 10
	}
	attemptTimeout := opts.RenewAttemptTimeout
	if attemptTimeout <= 0 {
		attemptTimeout = min(opts.RenewInterval, opts.LeaseTTL/4)
	}
	// RT1B-3D: recovery cadence defaults are derived INSIDE the scheduler with
	// a safety-margin cap; the runtime only passes the margin and any explicit
	// tuning the caller supplied.
	recoveryTimeout := opts.RenewRecoveryTimeout
	maxRetries := opts.MembershipMaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}
	retryDelay := opts.MembershipRetryDelay
	if retryDelay <= 0 {
		retryDelay = opts.RenewInterval / 4
		if retryDelay <= 0 {
			retryDelay = 50 * time.Millisecond
		}
	}
	observeInterval := min(opts.RenewInterval, 250*time.Millisecond)

	adapter, err := NewRealMembershipAdapter(RealMembershipAdapterOptions{
		Store: runtime.Store, Registry: runtime.Guards,
		AuthorityLayoutVersion: opts.Profile.AuthorityLayoutVersion,
		HashVersion:            snapshot.HashVersion, HashSeed: snapshot.HashSeed,
		HolderID: opts.HolderID, HolderBootID: opts.HolderBootID,
		LeaseTTL: opts.LeaseTTL, SafetyMargin: opts.SafetyMargin, Now: opts.Now,
	})
	if err != nil {
		return nil, err
	}
	scheduler, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store: runtime.Store, Registry: runtime.Guards,
		AuthorityLayoutVersion: opts.Profile.AuthorityLayoutVersion,
		HashVersion:            snapshot.HashVersion, HashSeed: snapshot.HashSeed,
		HolderID: opts.HolderID, HolderBootID: opts.HolderBootID,
		LeaseTTL: opts.LeaseTTL, BaseDelay: baseDelay, MaxDelay: maxDelay, Jitter: jitter,
		SafetyMargin:      opts.SafetyMargin,
		RecoveryBaseDelay: opts.LeaseRecoveryBaseDelay, RecoveryMaxDelay: opts.LeaseRecoveryMaxDelay,
		RecoveryJitterMax: opts.LeaseRecoveryJitterMax, RecoverySchedulerSlack: opts.LeaseRecoverySchedulerSlack,
		Now: opts.Now, JitterSeed: membershipObserverJitterSeed,
		RenewAttemptTimeout: attemptTimeout, RenewRecoveryTimeout: recoveryTimeout,
		// A fatal is terminal: it moves the one-way state machine to Fatal,
		// fails the local data plane closed (fence guards, cancel entries), and
		// can never be overwritten by the startup ready transition.
		OnFatal: func(err error) { runtime.enterFatal(err) },
	})
	if err != nil {
		return nil, err
	}
	runtime.adapter = adapter
	runtime.scheduler = scheduler
	runtime.recoveryBudget = scheduler.RecoveryBudgetInfo()
	runtime.OnFatal = opts.OnFatal
	// Runtime Fatal Gate: every local membership side effect is refused once
	// the runtime is terminal (Fatal/Closed), so a reconciler action that
	// outlives enterFatal -- an in-flight Prepare/Commit or a degraded retry --
	// can never re-install a guard or mutate the data plane.
	gate := &runtimeGateMembershipAdapter{inner: adapter, runtime: runtime}
	var installer MembershipInstaller = gate
	if opts.MembershipInstallerDecorator != nil {
		installer, err = opts.MembershipInstallerDecorator(gate)
		if err != nil {
			scheduler.Close()
			return nil, fmt.Errorf(
				"authority composition: membership installer decorator: %w",
				err,
			)
		}
		if installer == nil {
			scheduler.Close()
			return nil, errors.New(
				"authority composition: membership installer decorator returned nil",
			)
		}
	}
	runtime.membershipInstaller = installer

	reconciler, err := NewShardMembershipReconciler(MembershipReconcilerOptions{
		HolderID: opts.HolderID, HolderBootID: opts.HolderBootID,
		Installer: installer, Fencer: gate, Remover: gate,
		AllowedDomains:         []actorrouting.RouteDomain{opts.Domain},
		AuthorityLayoutVersion: opts.Profile.AuthorityLayoutVersion,
		Scheduler:              scheduler,
		MaxRetries:             maxRetries, RetryDelay: retryDelay, Now: opts.Now,
		actionPhaseHook: opts.membershipActionHook,
	})
	if err != nil {
		scheduler.Close()
		return nil, err
	}
	if err := runtime.SetMembershipReconciler(reconciler); err != nil {
		scheduler.Close()
		return nil, err
	}

	// Observe the static startup snapshot and converge BEFORE reporting
	// process ready.  Fail closed: never serve with a half-admitted startup.
	// The ordered plan installs shard 0 before shard N (sorted by shard key),
	// so a partially successful startup has real installed guards/entries that
	// are rolled back here; the returned runtime (non-nil with err) is fully
	// fail-closed so callers can probe guard/entry/serving counts.
	initial := reconciler.ObserveContext(ctx, snapshot)
	if initial.Rejected || initial.Stale || !initial.FullyReconciled {
		scheduler.Close()
		reconciler.Close()
		runtime.failClosedLocalState()
		err := fmt.Errorf("authority composition: startup membership did not fully reconcile: observed=%d fullyReconciled=%v rejected=%v stale=%v degraded=%+v",
			initial.ObservedRevision, initial.FullyReconciled, initial.Rejected, initial.Stale, initial.Degraded)
		if keepRuntimeOnFailure {
			return runtime, err
		}
		return nil, err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runtime.ctx, runtime.cancel = runCtx, cancel
	runtime.observeInterval = observeInterval
	// One-way bootstrap: a fatal observed during startup observation (an entry
	// panic or unsafe fence right after Activate) already moved the state to
	// Fatal, so this CAS fails and the construction returns an error instead of
	// overwriting the fatal with ready=true.
	if !runtime.membershipState.CompareAndSwap(membershipStateBootstrap, membershipStateReady) {
		scheduler.Close()
		reconciler.Close()
		runtime.failClosedLocalState()
		err := errors.New("authority composition: scheduler became fatal during startup observation")
		if keepRuntimeOnFailure {
			return runtime, err
		}
		return nil, err
	}
	// The membership observer always belongs to Runtime. Static composition
	// also owns its applier; remote composition does not, because
	// AssignmentComposition is the sole owner of the shared applier watch.
	runtime.wg.Add(1)
	if runtime.ownsApplier {
		runtime.wg.Add(1)
		go runtime.runApplier(runCtx)
	}
	go runtime.runMembershipObserver(runCtx)
	return runtime, nil
}

// observeMembership drives the membership reconciler with a snapshot.  It is
// TEST_ONLY / CONTROL_PLANE_ONLY: the PRODUCTION snapshot input owner is
// runMembershipObserver (a single monotonically increasing stream from the
// dynamic assignment provider), so there is exactly one caller of
// Reconciler.Observe in production.  Because production has a single input
// owner, any stale or rejected snapshot is a control-plane contract violation
// and fails the process closed; a duplicate snapshot is ignored without
// touching ProcessReady.
func (r *Runtime) observeMembership(snapshot actorrouting.RouteSnapshot) ObserveResult {
	if r == nil {
		return ObserveResult{Closed: true}
	}
	m := r.MembershipReconciler()
	if m == nil {
		return ObserveResult{Closed: true}
	}
	result := m.Observe(snapshot)
	r.checkMembershipResult(result)
	return result
}

// checkMembershipResult moves the runtime to Fatal when an observe result
// proves the local membership can no longer be trusted:
//
//   - a rejected or stale snapshot (control-plane contract violation under the
//     single-input-owner model), or
//   - a REMOVE/REPLACE fence that failed while the exact old generation is
//     still admitted (safety can no longer be proven; we must not wait for the
//     next scheduler renewal to discover it through OnFatal).
//
// A transiently non-converged ADD or a successful-fence-but-failed-remove
// cleanup does NOT lower ProcessReady: the target shard stays fail-closed and
// the background retry may safely finish, while the other shards keep serving
// (single-shard failure isolation).
func (r *Runtime) checkMembershipResult(result ObserveResult) {
	if result.Rejected || result.Stale {
		r.enterFatal(fmt.Errorf("authority composition: membership snapshot rejected or stale (control-plane contract violation): rejected=%v stale=%v observed=%d", result.Rejected, result.Stale, result.ObservedRevision))
		return
	}
	for _, degraded := range result.Degraded {
		if degraded.Action != MembershipActionRemove && degraded.Action != MembershipActionReplace {
			continue
		}
		if r.oldGenerationStillAdmitted(degraded.Shard, degraded.OldGeneration) {
			r.enterFatal(fmt.Errorf("authority composition: cannot prove lost generation %v stopped local admission (shard %v)", degraded.Shard, degraded.OldGeneration))
			return
		}
	}
}

// oldGenerationStillAdmitted reports whether the guard installed for the
// shard still admits writes for exactly the given generation.
func (r *Runtime) oldGenerationStillAdmitted(key ShardKey, gen MembershipGeneration) bool {
	snapshot, ok := r.Guards.Snapshot(key.Domain, key.Shard)
	if !ok || !snapshot.Admitted {
		return false
	}
	a := snapshot.Authority
	return a.HolderID == gen.HolderID && a.HolderBootID == gen.HolderBootID &&
		a.AssignmentRevision == gen.AssignmentRevision && a.WriterEpoch == gen.WriterEpoch && a.LeaseEpoch == gen.LeaseEpoch
}

// runMembershipObserver consumes the existing dynamic assignment provider's
// snapshot stream and drives the reconciler, so 0→1→G1→G2→0 transitions flow
// through the composed runtime without the legacy global renewal loop.  It
// does not implement any new remote-watch mechanism; it only observes the
// provider already owned by the runtime.  The provider is the single validated
// snapshot source: every published snapshot has already passed
// validateRouteSnapshot, so the route applier and this observer consume the
// same accepted, monotonically increasing revision sequence.
func (r *Runtime) runMembershipObserver(ctx context.Context) {
	defer r.wg.Done()
	m := r.MembershipReconciler()
	if m == nil {
		return
	}
	interval := r.observeInterval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	var last uint64
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			revision := r.AssignmentProvider.CurrentRevision()
			if revision == 0 || revision == last {
				continue
			}
			snapshot := r.AssignmentProvider.Snapshot()
			if snapshot.GlobalRevision != revision {
				// Publication raced the revision observation. Do not advance last;
				// the next tick fetches the newest complete generation.
				continue
			}
			last = revision
			r.checkMembershipResult(m.ObserveContext(ctx, snapshot))
		}
	}
}

// ErrRuntimeTerminal is returned by the runtime-gated membership adapter when a
// reconciler action outlives the terminal Fatal/Closed state.
var ErrRuntimeTerminal = errors.New("authority composition: runtime is terminal")

// runtimeGateMembershipAdapter wraps the real adapter and refuses every local
// membership side effect (prepare/commit/fence/remove) once the runtime is
// terminal, so an in-flight or retried reconcile action can never re-install a
// guard or mutate the data plane after enterFatal.
type runtimeGateMembershipAdapter struct {
	inner   *RealMembershipAdapter
	runtime *Runtime
}

func (g *runtimeGateMembershipAdapter) PrepareMembership(ctx context.Context, member ShardMembership) (*PreparedMembership, error) {
	if err := g.runtime.ensureMembershipActive(); err != nil {
		return nil, err
	}
	return g.inner.PrepareMembership(ctx, member)
}

func (g *runtimeGateMembershipAdapter) CommitPreparedMembership(ctx context.Context, prepared PreparedMembership) error {
	if err := g.runtime.ensureMembershipActive(); err != nil {
		return err
	}
	return g.inner.CommitPreparedMembership(ctx, prepared)
}

func (g *runtimeGateMembershipAdapter) FenceMembership(ctx context.Context, member ShardMembership) error {
	if err := g.runtime.ensureMembershipActive(); err != nil {
		return err
	}
	return g.inner.FenceMembership(ctx, member)
}

func (g *runtimeGateMembershipAdapter) RemoveMembership(ctx context.Context, member ShardMembership) error {
	if err := g.runtime.ensureMembershipActive(); err != nil {
		return err
	}
	return g.inner.RemoveMembership(ctx, member)
}

// RecoveryBudget returns the effective lease recovery cadence/budget resolved
// at scheduler construction, for startup logging and operator diagnostics
// (RT1B-3D).  The zero value is returned for a nil runtime or a legacy
// (non-membership) runtime that never constructed the scheduler.
func (r *Runtime) RecoveryBudget() RecoveryBudgetInfo {
	if r == nil {
		return RecoveryBudgetInfo{}
	}
	return r.recoveryBudget
}

// ensureMembershipActive permits local membership side effects only while the
// runtime is still Bootstrapping (startup convergence) or Ready.
func (r *Runtime) ensureMembershipActive() error {
	if r == nil {
		return ErrRuntimeTerminal
	}
	switch r.membershipState.Load() {
	case membershipStateBootstrap, membershipStateReady:
		return nil
	default:
		return ErrRuntimeTerminal
	}
}
