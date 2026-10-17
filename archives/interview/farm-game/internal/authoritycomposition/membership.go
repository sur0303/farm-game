// Package authoritycomposition additionally hosts the RT1B-2A local
// ShardMembershipReconciler core.  It computes, from one complete
// RouteSnapshot, the deterministic set of local shard membership changes a
// holder must apply: ADD / REMOVE / REPLACE / UNCHANGED.
//
// Scope: this file implements only the membership state machine, diff,
// revision tracking, bounded retry, per-shard degraded status and the close
// lifecycle.  It does not touch Farm/Player registries, cmd wiring, Redis
// authority data, or cross-process watches.  The installer/fencer/remover are
// Ports; RT1B-2A ships Fake implementations in the testkit package, and
// RT1B-3 wires the real lease-grant adapters.
package authoritycomposition

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

// MembershipPort errors returned by this core.  They wrap underlying Port
// failures so callers can classify a per-shard outage without inspecting the
// concrete adapter.
var (
	// ErrMembershipStaleSnapshot reports a snapshot whose GlobalRevision is
	// older than the last observed revision.  The snapshot is rejected; no
	// shard is revived or removed from it.
	ErrMembershipStaleSnapshot = errors.New("authoritycomposition: stale route snapshot rejected")
	// ErrMembershipClosed reports an operation issued after Close.
	ErrMembershipClosed = errors.New("authoritycomposition: membership reconciler closed")
	// ErrMembershipTopologyChanged reports a RouteSnapshot whose physical
	// topology (HashVersion/HashSeed/ShardCount/AuthorityLayoutVersion)
	// changed since the first observed snapshot.  A topology change is a
	// persistence migration, never an ordinary ADD/REMOVE; the snapshot is
	// rejected so the holder never reinterprets its shards under a new layout.
	ErrMembershipTopologyChanged = errors.New("authoritycomposition: route snapshot topology changed")
)

// TopologyFingerprint is the immutable physical layout a holder's membership
// may be reconciled under.  Changing any component requires an explicit
// persistence migration (contract 27 §39), so the reconciler seals the
// fingerprint at first observation and fails closed on any later change.
type TopologyFingerprint struct {
	AuthorityLayoutVersion string
	HashVersion            uint32
	HashSeed               uint64
	PlayerShardCount       uint32
	FarmShardCount         uint32
}

func (f TopologyFingerprint) zero() bool {
	return f.AuthorityLayoutVersion == "" && f.HashVersion == 0 && f.HashSeed == 0 && f.PlayerShardCount == 0 && f.FarmShardCount == 0
}

func (f TopologyFingerprint) equal(o TopologyFingerprint) bool {
	return f == o
}

// fingerprintFromSnapshot derives the fingerprint from one complete snapshot
// plus the process's authority layout version (which the snapshot does not
// carry itself).
func fingerprintFromSnapshot(snapshot actorrouting.RouteSnapshot, authorityLayoutVersion string) TopologyFingerprint {
	return TopologyFingerprint{
		AuthorityLayoutVersion: authorityLayoutVersion,
		HashVersion:            snapshot.HashVersion,
		HashSeed:               snapshot.HashSeed,
		PlayerShardCount:       snapshot.Player.ShardCount,
		FarmShardCount:         snapshot.Farm.ShardCount,
	}
}

// ShardKey identifies one virtual shard of one domain.
type ShardKey struct {
	Domain actorrouting.RouteDomain
	Shard  uint32
}

// MembershipGeneration carries the immutable fenced Authority generation
// identity plus its latest projected mutable deadline. It mirrors ShardRoute
// and HolderRef so a membership diff detects a true generation replacement
// without consulting Redis. LeaseUntilMs is a renewable liveness projection
// and never turns a same-generation deadline refresh into fence/replace.
type MembershipGeneration struct {
	AssignmentRevision uint64
	WriterEpoch        uint64
	LeaseEpoch         uint64
	HolderID           string
	HolderBootID       string
	LeaseUntilMs       int64
}

func (g MembershipGeneration) isZero() bool {
	return g.AssignmentRevision == 0 && g.WriterEpoch == 0 && g.LeaseEpoch == 0
}

// equal compares the immutable Authority generation identity. LeaseUntilMs is
// deliberately excluded: Holder Renew may advance that deadline without
// changing holder/boot/assignment/writer/lease epochs, and a projected
// deadline extension must never fence and reinstall the same generation.
func (g MembershipGeneration) equal(o MembershipGeneration) bool {
	return g.AssignmentRevision == o.AssignmentRevision &&
		g.WriterEpoch == o.WriterEpoch && g.LeaseEpoch == o.LeaseEpoch &&
		g.HolderID == o.HolderID && g.HolderBootID == o.HolderBootID
}

// ShardMembership is a concrete installed membership: the shard key plus the
// generation that was admitted.
type ShardMembership struct {
	Key        ShardKey
	Generation MembershipGeneration
}

// MembershipStatus is the per-shard lifecycle status inside the reconciler.
type MembershipStatus uint8

const (
	// MembershipAbsent means the shard is not installed.
	MembershipAbsent MembershipStatus = iota
	// MembershipInstalling means an ADD/REPLACE install is in flight.
	MembershipInstalling
	// MembershipInstalled means the shard membership is fully installed.
	MembershipInstalled
	// MembershipFencing means a REMOVE/REPLACE is fencing the old generation.
	MembershipFencing
	// MembershipRemoving means a REMOVE is uninstalling the shard.
	MembershipRemoving
	// MembershipDegraded means the target shard failed and is in bounded retry.
	MembershipDegraded
)

// MembershipActionType is the deterministic diff classification.
type MembershipActionType uint8

const (
	// MembershipActionUnchanged means desired == installed.
	MembershipActionUnchanged MembershipActionType = iota
	// MembershipActionAdd installs a new shard membership (ADMIT).
	MembershipActionAdd
	// MembershipActionRemove fences and uninstalls an owned shard (REMOVE).
	MembershipActionRemove
	// MembershipActionReplace fences the old generation, then installs the new
	// generation on the same key (FENCE before ADMIT).
	MembershipActionReplace
)

// ShardAction is one ordered membership transition.  Token is the unique
// action identity assigned by replan; a Port result is committed only when the
// token still matches the current plan, which makes an in-flight old action
// harmless after a newer revision supersedes it.
type ShardAction struct {
	Type          MembershipActionType
	Shard         ShardKey
	Generation    MembershipGeneration // desired/new generation for ADD/REPLACE
	OldGeneration MembershipGeneration // installed generation for REMOVE/REPLACE
	Token         uint64
}

// ShardDegraded is the observable per-shard degraded status.  It carries the
// full generation the failed action targeted so a downstream operator can
// distinguish a REMOVE fence failure from an ADD install failure without
// re-deriving state.
type ShardDegraded struct {
	Shard             ShardKey
	Action            MembershipActionType
	Retries           int
	LastErr           string
	DesiredGeneration MembershipGeneration // ADD/REPLACE target
	OldGeneration     MembershipGeneration // REMOVE/REPLACE expected old
	SnapshotRevision  uint64
	ActionToken       uint64
}

// ObserveResult is the outcome of one Observe or Reconcile pass.
type ObserveResult struct {
	ObservedRevision        uint64
	FullyReconciledRevision uint64
	Actions                 []ShardAction
	Degraded                []ShardDegraded
	FullyReconciled         bool
	Stale                   bool
	Duplicate               bool
	Closed                  bool
	Rejected                bool
}

// PreparedMembership is the outcome of a successful PrepareMembership: the
// desired generation has been proven against the repository but nothing has
// been admitted.  CommitPreparedMembership applies it locally, only while the
// reconciler's action token still matches the current plan.  The carried
// RequestStarted/AcceptedTTLms drive the local monotonic deadline conversion.
type PreparedMembership struct {
	Key            ShardKey
	Generation     MembershipGeneration
	RequestStarted time.Time
	AcceptedTTLms  int64
	Authority      authoritystore.ShardWriteAuthority
}

// MembershipInstaller admits a shard membership locally through two phases.
// PrepareMembership proves the full generation against the repository outside
// the reconciler lock and returns a PreparedMembership without touching the
// LeaseGuardRegistry; CommitPreparedMembership is a purely local, bounded
// operation that installs (or, for an identical generation, refreshes) the
// guard and must only be called while the action token still matches.
type MembershipInstaller interface {
	PrepareMembership(ctx context.Context, member ShardMembership) (*PreparedMembership, error)
	CommitPreparedMembership(ctx context.Context, prepared PreparedMembership) error
}

// MembershipFencer fences an installed shard membership before removal or
// replacement.  Fencing always precedes any install on the same shard.
type MembershipFencer interface {
	FenceMembership(ctx context.Context, member ShardMembership) error
}

// MembershipRemover uninstalls an owned shard membership after fencing.
type MembershipRemover interface {
	RemoveMembership(ctx context.Context, member ShardMembership) error
}

// LeaseRegistrationReservation is the scheduler-side placeholder created
// before the local guard is installed.  A successful Reserve guarantees the
// scheduler will accept an Activate for that generation even if the reconciler
// is about to hold its commit lock; Reserve/Activate/CancelReservation are all
// purely local and bounded.
type LeaseRegistrationReservation struct {
	Key        ShardKey
	Generation MembershipGeneration
	Token      uint64
}

// LeaseScheduler is the narrow per-shard lease scheduler seam the reconciler
// drives:
//
//   - Reserve is called before CommitPreparedMembership so a closed scheduler
//     aborts before any guard is installed;
//   - Activate starts the lease goroutine only after the guard commit
//     succeeded (a failed Activate rolls the guard back);
//   - CancelReservation releases an un-activated reservation;
//   - Cancel(key, generation) stops a generation entry as soon as the old
//     generation is fenced, so a removed shard stops refreshing before it is
//     uninstalled.
type LeaseScheduler interface {
	Reserve(key ShardKey, generation MembershipGeneration) (*LeaseRegistrationReservation, error)
	Activate(reservation *LeaseRegistrationReservation) error
	CancelReservation(reservation *LeaseRegistrationReservation)
	Cancel(key ShardKey, generation MembershipGeneration) error
}

// membershipActionPhase identifies exact TEST_ONLY action boundaries. The
// hook runs synchronously under the single action owner, so tests can deliver
// cancellation without sleeps or helper goroutines.
type membershipActionPhase uint8

const (
	membershipActionAfterReserve membershipActionPhase = iota + 1
	membershipActionAfterGuardCommit
	membershipActionAfterActivate
)

// MembershipReconcilerOptions configures the core.  Retry bounds keep a
// permanently failing shard from spinning; the installer/fencer/remover are
// required.  AllowedDomains restricts which route domains the reconciler may
// manage (empty means both domains); AuthorityLayoutVersion is the process
// layout and is sealed into the topology fingerprint.
type MembershipReconcilerOptions struct {
	HolderID     string
	HolderBootID string

	Installer MembershipInstaller
	Fencer    MembershipFencer
	Remover   MembershipRemover

	// AllowedDomains restricts reconciliation to these domains.  Empty means
	// every domain in the snapshot is eligible.  A player runtime should pass
	// only DomainPlayer, a farm runtime only DomainFarm.
	AllowedDomains []actorrouting.RouteDomain
	// AuthorityLayoutVersion is sealed at first observation.  An empty value
	// is only admissible in Fake-only tests; production wiring must supply the
	// frozen layout (currently "v1").
	AuthorityLayoutVersion string

	// Scheduler, when set, receives Register/Cancel events for installed
	// memberships.  It is optional (nil disables lease scheduling).
	Scheduler LeaseScheduler

	MaxRetries int
	RetryDelay time.Duration
	Now        func() time.Time

	// actionPhaseHook is TEST_ONLY. Production never sets it.
	actionPhaseHook func(membershipActionPhase)
}

// ShardMembershipReconciler is the deterministic local membership core.  All
// exported methods are safe for concurrent use; each Observe/Reconcile pass is
// serialized by an internal mutex.
type ShardMembershipReconciler struct {
	holderID     string
	holderBootID string

	installer MembershipInstaller
	fencer    MembershipFencer
	remover   MembershipRemover
	scheduler LeaseScheduler

	allowedDomains  map[actorrouting.RouteDomain]bool
	authorityLayout string
	topology        TopologyFingerprint
	topologySealed  bool

	maxRetries int
	retryDelay time.Duration

	// mu guards all membership state: installed/pending/retries/degraded,
	// revisions, topology seal, tokens and inflight.
	mu sync.Mutex

	lastObservedRevision        uint64
	lastFullyReconciledRevision uint64

	// installed holds the generations that were successfully admitted.
	installed map[ShardKey]ShardMembership
	// pending holds actions not yet successfully applied (first attempt and
	// bounded retries).  A successful action is removed from pending.
	pending map[ShardKey]ShardAction
	// retries counts consecutive failures per shard (bounded by MaxRetries).
	retries map[ShardKey]int
	// degraded is the last failure detail per shard; cleared on success.
	degraded map[ShardKey]ShardDegraded
	// tokenSeq issues strictly increasing ActionTokens to replanned actions.
	tokenSeq uint64
	// successful action counters are monotonic process-local evidence. A
	// deadline-only projection must leave all three unchanged after startup.
	successfulAdds     uint64
	successfulReplaces uint64
	successfulRemoves  uint64
	// inflight records the token currently executing for each shard.
	inflight map[ShardKey]uint64

	// actionMu is the single action worker: Port calls are serialized under it
	// and never under mu, so a blocked Port cannot stall state access.
	actionMu sync.Mutex

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closed    atomic.Bool
	closeOnce sync.Once

	// actionPhaseHook is a synchronous TEST_ONLY barrier owned by actionMu.
	actionPhaseHook func(membershipActionPhase)
}

// NewShardMembershipReconciler builds the core and starts its bounded retry
// loop.  A registered holder with zero desired shards yields a fully
// reconciled, zero-action reconciler (process-ready zero-shard topology).
func NewShardMembershipReconciler(opts MembershipReconcilerOptions) (*ShardMembershipReconciler, error) {
	if opts.HolderID == "" || opts.HolderBootID == "" {
		return nil, fmt.Errorf("%w: holder id and boot id are required", ErrMembershipStaleSnapshot)
	}
	if opts.Installer == nil || opts.Fencer == nil || opts.Remover == nil {
		return nil, errors.New("authoritycomposition: membership installer/fencer/remover ports are required")
	}
	if opts.MaxRetries < 0 {
		return nil, errors.New("authoritycomposition: max retries must be non-negative")
	}
	if opts.RetryDelay <= 0 {
		opts.RetryDelay = time.Second
	}
	allowed := make(map[actorrouting.RouteDomain]bool)
	for _, domain := range opts.AllowedDomains {
		allowed[domain] = true
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &ShardMembershipReconciler{
		holderID:        opts.HolderID,
		holderBootID:    opts.HolderBootID,
		installer:       opts.Installer,
		fencer:          opts.Fencer,
		remover:         opts.Remover,
		scheduler:       opts.Scheduler,
		allowedDomains:  allowed,
		authorityLayout: opts.AuthorityLayoutVersion,
		maxRetries:      opts.MaxRetries,
		retryDelay:      opts.RetryDelay,
		installed:       make(map[ShardKey]ShardMembership),
		pending:         make(map[ShardKey]ShardAction),
		retries:         make(map[ShardKey]int),
		degraded:        make(map[ShardKey]ShardDegraded),
		inflight:        make(map[ShardKey]uint64),
		ctx:             ctx,
		cancel:          cancel,
		actionPhaseHook: opts.actionPhaseHook,
	}
	r.wg.Add(1)
	go r.run()
	return r, nil
}

// Observe computes the desired local membership from a complete RouteSnapshot,
// diffs it against the installed set, and executes the ordered actions once.
// A stale snapshot is rejected; a duplicate snapshot is idempotent and retries
// any unfinished actions.
func (r *ShardMembershipReconciler) Observe(snapshot actorrouting.RouteSnapshot) ObserveResult {
	if r == nil {
		return ObserveResult{Closed: true}
	}
	return r.observe(r.ctx, snapshot, false)
}

// ObserveContext applies one snapshot using a caller-bounded action context.
// The reconciler lifetime remains independently cancelable, while startup
// callers can bound blocking repository work without lending that deadline to
// the later observer/retry lifecycle.
func (r *ShardMembershipReconciler) ObserveContext(ctx context.Context, snapshot actorrouting.RouteSnapshot) ObserveResult {
	return r.observe(ctx, snapshot, false)
}

// Reconcile retries all pending/degraded actions once, in the frozen order
// (fence first, install last).  It does not accept a new snapshot.
func (r *ShardMembershipReconciler) Reconcile() ObserveResult {
	if r == nil {
		return ObserveResult{Closed: true}
	}
	return r.observe(r.ctx, actorrouting.RouteSnapshot{}, true)
}

func (r *ShardMembershipReconciler) observe(actionParent context.Context, snapshot actorrouting.RouteSnapshot, reconcileOnly bool) ObserveResult {
	if r == nil || actionParent == nil || actionParent.Err() != nil {
		return ObserveResult{Closed: true}
	}
	if r.closed.Load() {
		return ObserveResult{Closed: true}
	}
	actionCtx, cancelAction := context.WithCancel(actionParent)
	stopLifetimeLink := context.AfterFunc(r.ctx, cancelAction)
	defer func() {
		stopLifetimeLink()
		cancelAction()
	}()
	r.mu.Lock()
	if r.ctx.Err() != nil || actionCtx.Err() != nil {
		r.mu.Unlock()
		return ObserveResult{Closed: true}
	}

	result := ObserveResult{FullyReconciledRevision: r.lastFullyReconciledRevision}
	if !reconcileOnly {
		if snapshot.GlobalRevision < r.lastObservedRevision {
			// Stale revisions are rejected before any other interpretation.
			result.Stale = true
			result.ObservedRevision = snapshot.GlobalRevision
			r.mu.Unlock()
			return result
		}
		if snapshot.GlobalRevision == r.lastObservedRevision && r.lastObservedRevision != 0 {
			result.Duplicate = true
			result.ObservedRevision = snapshot.GlobalRevision
		} else if snapshot.GlobalRevision > r.lastObservedRevision {
			fingerprint := fingerprintFromSnapshot(snapshot, r.authorityLayout)
			if r.topologySealed {
				if !r.topology.equal(fingerprint) {
					// A topology change is a persistence migration, never an
					// ordinary ADD/REMOVE.  Reject the snapshot and keep all
					// existing state; no shard is added, removed, or replaced.
					result.Rejected = true
					result.ObservedRevision = snapshot.GlobalRevision
					r.mu.Unlock()
					return result
				}
			} else {
				r.topology = fingerprint
				r.topologySealed = true
			}
			r.lastObservedRevision = snapshot.GlobalRevision
			result.ObservedRevision = snapshot.GlobalRevision
			// A new generation supersedes earlier failures; keep the retry
			// counters but start a fresh action plan for this revision.
			r.replan(snapshot)
		}
	}
	result.ObservedRevision = r.lastObservedRevision

	// Compute the ordered execution plan from the current pending set.  A
	// duplicate snapshot keeps the existing plan (same revision, same desired
	// set); a retry keeps the failed actions.
	plan := r.orderedPlan()
	result.Actions = plan
	result.FullyReconciled = len(plan) == 0 && len(r.inflight) == 0

	// Phase A is complete: release the state lock before touching the Ports.
	r.mu.Unlock()

	// Phase B executes the plan outside the state lock.  A single action
	// worker (actionMu) serializes Port calls; if another pass is already
	// holding it with a blocked Port, this pass leaves the actions pending for
	// the retry loop instead of blocking Observe/Reconcile/getters.
	r.tryExecutePlan(actionCtx, plan)

	// Phase C re-acquires the lock and publishes the coherent outcome.
	r.mu.Lock()
	r.applyRevisionProgress()
	result.FullyReconciledRevision = r.lastFullyReconciledRevision
	result.FullyReconciled = len(r.pending) == 0 && len(r.retries) == 0 && len(r.inflight) == 0
	result.Degraded = r.degradedSnapshot()
	r.mu.Unlock()
	return result
}

// replan replaces the pending plan from a newly observed snapshot.  It runs
// under r.mu.  Every newly planned action receives a fresh ActionToken so an
// in-flight Port call for an older plan can be detected and dropped on
// commit; retry counters are intentionally kept across revisions (bounded
// retry budget).
func (r *ShardMembershipReconciler) replan(snapshot actorrouting.RouteSnapshot) {
	desired := desiredLocalShards(snapshot, r.holderID, r.holderBootID, r.allowedDomains)
	actions := diffMembership(desired, r.installed)
	next := make(map[ShardKey]ShardAction, len(actions))
	for _, action := range actions {
		if action.Type == MembershipActionUnchanged {
			// Deadline is mutable within one Authority generation. Advance only
			// reconciler bookkeeping; the sole lease scheduler already refreshed
			// the live guard from the same durable Renew. Never run Fence,
			// Remove, Prepare, or a second scheduler entry for this projection.
			installed := r.installed[action.Shard]
			if action.Generation.LeaseUntilMs >= installed.Generation.LeaseUntilMs {
				installed.Generation.LeaseUntilMs = action.Generation.LeaseUntilMs
				r.installed[action.Shard] = installed
			}
			continue
		}
		r.tokenSeq++
		action.Token = r.tokenSeq
		next[action.Shard] = action
	}
	r.pending = next
	// Successful installed entries stay; actions now pending drive the
	// install/fence/remove calls.
}

// orderedPlan returns the actions to execute in the frozen order.  It runs
// under r.mu.  REMOVE/REPLACE fence operations precede ADD installs so an
// old generation is never left serving while a new generation is admitted.
func (r *ShardMembershipReconciler) orderedPlan() []ShardAction {
	if len(r.pending) == 0 {
		return nil
	}
	plan := make([]ShardAction, 0, len(r.pending))
	// Stable ordering: sort keys so plans are deterministic.
	keys := make([]ShardKey, 0, len(r.pending))
	for key := range r.pending {
		keys = append(keys, key)
	}
	sortShardKeys(keys)
	// REMOVE/REPLACE first (fence), ADD last.
	var removes, adds []ShardAction
	for _, key := range keys {
		action := r.pending[key]
		switch action.Type {
		case MembershipActionRemove, MembershipActionReplace:
			removes = append(removes, action)
		case MembershipActionAdd:
			adds = append(adds, action)
		}
	}
	plan = append(plan, removes...)
	plan = append(plan, adds...)
	return plan
}

// tryExecutePlan runs the ordered plan under the single action worker.  If
// another pass already holds the worker (its Port is in flight), this pass
// returns immediately and leaves the actions pending for the retry loop, so a
// blocked Port never stalls Observe/Reconcile/getters.  It runs without r.mu.
func (r *ShardMembershipReconciler) tryExecutePlan(ctx context.Context, plan []ShardAction) {
	if len(plan) == 0 {
		return
	}
	if !r.actionMu.TryLock() {
		// The action worker is busy with a (possibly blocked) Port call;
		// leave the plan pending instead of blocking this pass.
		return
	}
	defer r.actionMu.Unlock()
	for _, action := range plan {
		if r.ctx.Err() != nil || ctx.Err() != nil {
			break
		}
		r.executeAction(ctx, action)
	}
}

// executeAction runs one action: it validates the action token under the state
// lock, performs the preparation phase outside every lock, then commits the
// result only if the token still matches the current plan (no newer revision
// superseded it).  ADD/REPLACE use the two-phase install so a superseded
// prepare is discarded and never admits a stale generation into the
// LeaseGuardRegistry; REMOVE is a lock-free local fence+remove followed by a
// token-checked commit.
func (r *ShardMembershipReconciler) executeAction(ctx context.Context, action ShardAction) {
	if action.Type == MembershipActionUnchanged {
		r.mu.Lock()
		delete(r.pending, action.Shard)
		delete(r.retries, action.Shard)
		delete(r.degraded, action.Shard)
		r.mu.Unlock()
		return
	}
	token, ok := r.beginAction(ctx, action)
	if !ok {
		return // superseded or closed; never touch Ports
	}
	switch action.Type {
	case MembershipActionAdd:
		// Phase 1 (prepare) is the only network touch and runs lock-free.
		prepared, err := r.installer.PrepareMembership(ctx, ShardMembership{Key: action.Shard, Generation: action.Generation})
		if err != nil {
			r.commitFailure(action, token, err)
			return
		}
		// Phase 2 (commit) is purely local and token-gated: a superseded
		// prepare is discarded without admitting anything (F1).
		r.commitPrepared(ctx, action, token, prepared)
	case MembershipActionReplace:
		// REPLACE fences the old generation before preparing the new one; the
		// fence is a local compare-and-act and runs lock-free.  The old lease
		// entry is cancelled as soon as the old generation is fenced.
		old := ShardMembership{Key: action.Shard, Generation: action.OldGeneration}
		if err := r.fencer.FenceMembership(ctx, old); err != nil {
			r.commitFailure(action, token, err)
			return
		}
		r.schedulerCancel(action.Shard, action.OldGeneration)
		prepared, err := r.installer.PrepareMembership(ctx, ShardMembership{Key: action.Shard, Generation: action.Generation})
		if err != nil {
			r.commitFailure(action, token, err)
			return
		}
		r.commitPrepared(ctx, action, token, prepared)
	case MembershipActionRemove:
		old := ShardMembership{Key: action.Shard, Generation: action.OldGeneration}
		if err := r.fencer.FenceMembership(ctx, old); err != nil {
			r.commitFailure(action, token, err)
			return
		}
		// The lease entry must stop refreshing before the guard is removed.
		r.schedulerCancel(action.Shard, action.OldGeneration)
		if err := r.remover.RemoveMembership(ctx, old); err != nil {
			r.commitFailure(action, token, err)
			return
		}
		r.commitRemove(action, token)
	default:
		r.commitFailure(action, token, fmt.Errorf("authoritycomposition: unknown membership action %d", action.Type))
	}
}

// beginAction validates that the action is still the current pending plan for
// its shard and marks it in flight.  It runs under r.mu and never blocks.
func (r *ShardMembershipReconciler) beginAction(ctx context.Context, action ShardAction) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed.Load() || r.ctx.Err() != nil || ctx == nil || ctx.Err() != nil {
		return 0, false
	}
	current, ok := r.pending[action.Shard]
	if !ok || current.Token != action.Token {
		return 0, false
	}
	r.inflight[action.Shard] = action.Token
	return action.Token, true
}

// commitFailure publishes a failed Port result only when the action token
// still matches the current pending plan.  A stale failure is dropped.  It
// runs under r.mu.
func (r *ShardMembershipReconciler) commitFailure(action ShardAction, token uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.inflight, action.Shard)
	if r.closed.Load() {
		return
	}
	current, ok := r.pending[action.Shard]
	if !ok || current.Token != token {
		return
	}
	r.recordFailure(action, err)
}

// commitRemove publishes a successful REMOVE only when the token still
// matches, atomically removing the installed membership.  It runs under r.mu.
func (r *ShardMembershipReconciler) commitRemove(action ShardAction, token uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.inflight, action.Shard)
	if r.closed.Load() {
		return
	}
	current, ok := r.pending[action.Shard]
	if !ok || current.Token != token {
		return
	}
	// The lease entry was already cancelled when the old generation was fenced.
	r.successfulRemoves++
	delete(r.installed, action.Shard)
	delete(r.pending, action.Shard)
	delete(r.retries, action.Shard)
	delete(r.degraded, action.Shard)
}

// commitPrepared publishes a prepared ADD/REPLACE.  It runs under r.mu: the
// token is re-validated and only then is the local commit executed and the
// installed membership updated atomically.  The lease scheduler is wired
// through a reservation so a closed scheduler aborts before any guard is
// installed, and a failed activate rolls the guard back (F2: never leave an
// admitted shard without a lease entry).  A superseded token discards the
// prepared generation without admitting anything.
func (r *ShardMembershipReconciler) commitPrepared(ctx context.Context, action ShardAction, token uint64, prepared *PreparedMembership) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.inflight, action.Shard)
	if r.closed.Load() || prepared == nil {
		return
	}
	current, ok := r.pending[action.Shard]
	if !ok || current.Token != token {
		// The plan moved on while the network phase was in flight: discard the
		// prepared membership, never install a stale generation.
		return
	}
	member := ShardMembership{Key: action.Shard, Generation: action.Generation}
	var reservation *LeaseRegistrationReservation
	entryActive := false
	cancelReservation := func() {
		if r.scheduler != nil && reservation != nil {
			r.scheduler.CancelReservation(reservation)
			reservation = nil
		}
	}
	rollbackCommitted := func(cancelEntry bool) {
		if r.scheduler != nil && (cancelEntry || entryActive) {
			_ = r.scheduler.Cancel(action.Shard, action.Generation)
			entryActive = false
		}
		cancelReservation()
		// Rollback is a local fail-closed operation. Use the reconciler
		// lifetime rather than the expired startup action context so the exact
		// generation is fenced and removed inside this same action owner.
		_ = r.fencer.FenceMembership(r.ctx, member)
		_ = r.remover.RemoveMembership(r.ctx, member)
	}
	failContext := func(committed, cancelEntry bool) bool {
		err := membershipActionContextErr(ctx)
		if err == nil {
			return false
		}
		if committed {
			rollbackCommitted(cancelEntry)
		} else {
			cancelReservation()
		}
		r.recordFailure(action, err)
		return true
	}

	// Pre-Reserve: network preparation may have returned concurrently with
	// the bounded startup deadline. No scheduler state exists yet.
	if failContext(false, false) {
		return
	}
	// Reserve before installing the guard: a closed scheduler fails here and
	// nothing is admitted.
	if r.scheduler != nil {
		var err error
		reservation, err = r.scheduler.Reserve(action.Shard, action.Generation)
		if err != nil {
			r.recordFailure(action, err)
			return
		}
		defer cancelReservation()
		if hook := r.actionPhaseHook; hook != nil {
			hook(membershipActionAfterReserve)
		}
		// Post-Reserve: cancellation owns and removes the reservation before
		// any guard can be admitted.
		if failContext(false, false) {
			return
		}
	}
	// Pre-CommitPreparedMembership. This deliberately remains a distinct
	// check from the post-Reserve barrier so future local work cannot open a
	// deadline gap between the two phases.
	if failContext(false, false) {
		return
	}
	if err := r.installer.CommitPreparedMembership(ctx, *prepared); err != nil {
		// A future installer seam may return an error after a partial local
		// side effect. Exact-generation rollback is idempotent and safer than
		// assuming an error always means no guard was installed.
		rollbackCommitted(false)
		r.recordFailure(action, err)
		return
	}
	if hook := r.actionPhaseHook; hook != nil {
		hook(membershipActionAfterGuardCommit)
	}
	// Post-Commit: a guard now exists, so cancellation must remove both it and
	// the outstanding reservation in this action owner.
	if failContext(true, false) {
		return
	}
	if r.scheduler != nil {
		// Pre-Activate remains an explicit phase barrier even though the
		// post-Commit check is adjacent today.
		if failContext(true, false) {
			return
		}
		if err := r.scheduler.Activate(reservation); err != nil {
			// Cancel covers a scheduler seam that failed after partially
			// activating; CancelReservation covers the ordinary pre-activate
			// failure. Both are exact-generation and idempotent.
			rollbackCommitted(true)
			r.recordFailure(action, err)
			return
		}
		reservation = nil // the reservation has become the active entry
		entryActive = true
		if hook := r.actionPhaseHook; hook != nil {
			hook(membershipActionAfterActivate)
		}
		// Post-Activate is the final cancellation linearization point. If the
		// deadline arrived first, cancel/join ownership transfers to the
		// scheduler entry and the guard is rolled back before publication.
		if failContext(true, true) {
			return
		}
	}
	// Cancellation arriving after the final postcheck is allowed to linearize
	// after this successful action; callers do not require clairvoyance about
	// a future deadline signal.
	switch action.Type {
	case MembershipActionAdd:
		r.successfulAdds++
	case MembershipActionReplace:
		r.successfulReplaces++
	}
	r.installed[action.Shard] = member
	delete(r.pending, action.Shard)
	delete(r.retries, action.Shard)
	delete(r.degraded, action.Shard)
}

func membershipActionContextErr(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	return ctx.Err()
}

// schedulerCancel notifies the lease scheduler that a shard generation was
// fenced or removed and must stop refreshing.
func (r *ShardMembershipReconciler) schedulerCancel(key ShardKey, generation MembershipGeneration) {
	if r.scheduler != nil {
		_ = r.scheduler.Cancel(key, generation)
	}
}

// recordFailure marks one shard failed, bounded by MaxRetries.  When the bound
// is reached the shard settles in degraded state and is no longer retried
// automatically (a newer snapshot or explicit Reconcile with a fresh attempt
// may retry after the plan is rebuilt).
func (r *ShardMembershipReconciler) recordFailure(action ShardAction, err error) {
	count := r.retries[action.Shard] + 1
	r.retries[action.Shard] = count
	r.degraded[action.Shard] = ShardDegraded{
		Shard:             action.Shard,
		Action:            action.Type,
		Retries:           count,
		LastErr:           err.Error(),
		DesiredGeneration: action.Generation,
		OldGeneration:     action.OldGeneration,
		SnapshotRevision:  r.lastObservedRevision,
		ActionToken:       action.Token,
	}
	if r.maxRetries > 0 && count >= r.maxRetries {
		delete(r.pending, action.Shard)
	}
}

// applyRevisionProgress advances the fully-reconciled revision only when every
// desired shard has been applied (no pending and no degraded).  It runs under
// r.mu.
func (r *ShardMembershipReconciler) applyRevisionProgress() {
	if len(r.pending) != 0 || len(r.degraded) != 0 || len(r.inflight) != 0 {
		return
	}
	r.lastFullyReconciledRevision = r.lastObservedRevision
}

type MembershipEvidenceStats struct {
	LastObservedRevision        uint64 `json:"last_observed_revision"`
	LastFullyReconciledRevision uint64 `json:"last_fully_reconciled_revision"`
	InstalledMemberships        int    `json:"installed_memberships"`
	PendingActions              int    `json:"pending_actions"`
	DegradedShards              int    `json:"degraded_shards"`
	InflightActions             int    `json:"inflight_actions"`
	ActionTokenSequence         uint64 `json:"action_token_sequence"`
	SuccessfulAdds              uint64 `json:"successful_adds"`
	SuccessfulReplaces          uint64 `json:"successful_replaces"`
	SuccessfulRemoves           uint64 `json:"successful_removes"`
}

func (r *ShardMembershipReconciler) EvidenceStats() MembershipEvidenceStats {
	if r == nil {
		return MembershipEvidenceStats{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return MembershipEvidenceStats{
		LastObservedRevision: r.lastObservedRevision, LastFullyReconciledRevision: r.lastFullyReconciledRevision,
		InstalledMemberships: len(r.installed), PendingActions: len(r.pending), DegradedShards: len(r.degraded),
		InflightActions: len(r.inflight), ActionTokenSequence: r.tokenSeq,
		SuccessfulAdds: r.successfulAdds, SuccessfulReplaces: r.successfulReplaces, SuccessfulRemoves: r.successfulRemoves,
	}
}

// LastObservedRevision is the concurrency-safe getter for the newest accepted
// snapshot revision.
func (r *ShardMembershipReconciler) LastObservedRevision() uint64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastObservedRevision
}

// LastFullyReconciledRevision is the concurrency-safe getter for the newest
// revision whose desired set was fully applied.
func (r *ShardMembershipReconciler) LastFullyReconciledRevision() uint64 {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastFullyReconciledRevision
}

// currentInstalledMembership returns a snapshot of installed memberships.
func (r *ShardMembershipReconciler) currentInstalledMembership() []ShardMembership {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ShardMembership, 0, len(r.installed))
	for _, member := range r.installed {
		out = append(out, member)
	}
	sortMemberships(out)
	return out
}

// CurrentInstalledMembership is the concurrency-safe getter.
func (r *ShardMembershipReconciler) CurrentInstalledMembership() []ShardMembership {
	return r.currentInstalledMembership()
}

// degradedSnapshot returns the observable degraded set.
func (r *ShardMembershipReconciler) degradedSnapshot() []ShardDegraded {
	if len(r.degraded) == 0 {
		return nil
	}
	out := make([]ShardDegraded, 0, len(r.degraded))
	keys := make([]ShardKey, 0, len(r.degraded))
	for key := range r.degraded {
		keys = append(keys, key)
	}
	sortShardKeys(keys)
	for _, key := range keys {
		out = append(out, r.degraded[key])
	}
	return out
}

// run is the bounded retry loop.  It wakes every RetryDelay while actions
// remain pending and stops on Close.
func (r *ShardMembershipReconciler) run() {
	defer r.wg.Done()
	ticker := time.NewTicker(r.retryDelay)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			if r.closed.Load() {
				return
			}
			if r.hasPending() {
				r.Reconcile()
			}
		}
	}
}

func (r *ShardMembershipReconciler) hasPending() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending) != 0
}

// Close stops the retry loop and WAITS for any in-flight Port call to finish,
// so the composition runtime's fail-closed sweep (failClosedLocalState) is
// guaranteed to run AFTER the last possible local Guard install and can never
// miss a guard that an in-flight commit admitted.  It is idempotent and safe
// to call from the composition runtime teardown.
func (r *ShardMembershipReconciler) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		r.cancel()
		// Barrier: hold the single action worker so an in-flight action -- one
		// that already passed the runtime gate and is parked in commitPrepared
		// between the gate check and the real local install -- finishes BEFORE
		// we wait for the retry worker. The action runs on a canceled runtime or
		// reconciler context, so a context-compliant network Port aborts; local
		// commit/rollback operations are bounded. sync.Once also makes a
		// concurrent second Close join this complete barrier instead of merely
		// observing closed=true and returning early.
		r.actionMu.Lock()
		r.wg.Wait()
		r.actionMu.Unlock()
	})
}
