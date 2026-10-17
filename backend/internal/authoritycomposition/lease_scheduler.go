// Package authoritycomposition hosts the RT1B-3B per-shard lease scheduler.
//
// The scheduler is driven purely by membership registration: it never calls
// Reconciler.Observe, which stays driven by assignment-snapshot updates.  Each
// installed MembershipGeneration owns exactly one leaseEntry with its own
// goroutine, token and bounded exponential backoff; a failure of one shard
// never fences or stops another shard.
//
// A local safety-deadline expiry only pauses admission: the entry enters
// LOCAL_FENCED_RENEWING and keeps issuing bounded exact-generation renewals
// (PrepareRenew) so that, while the repository authority lease is still valid,
// a successful renewal re-arms the local deadline (RefreshOrRearmIfMatches)
// and restores admission.  An authoritative Expired/FenceMismatch is terminal
// for the entry.
package authoritycomposition

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	routeauthority "farmgame/internal/logic/actor/authority"
	"farmgame/internal/storage/authoritystore"
)

// ErrLeaseSchedulerClosed reports an operation issued after Close.
var ErrLeaseSchedulerClosed = errors.New("authority composition: lease scheduler closed")

// ErrLeaseSchedulerEntryMissing reports that no guard is installed for the
// entry's shard/generation (removed or replaced while renewing).
var ErrLeaseSchedulerEntryMissing = errors.New("authority composition: lease scheduler entry missing")

// LeaseEntryState is the observable per-shard lease entry state.
type LeaseEntryState uint8

const (
	// LeaseEntryActive: the guard is admitted and renewals are refreshing it.
	LeaseEntryActive LeaseEntryState = iota
	// LeaseEntryLocalFencedRenewing: the local safety deadline passed; writes
	// are paused but exact-generation renewals continue (recoverable).
	LeaseEntryLocalFencedRenewing
	// LeaseEntryLostGeneration: the repository proved the generation was lost.
	LeaseEntryLostGeneration
	// LeaseEntryCancelled: no active entry for this shard.
	LeaseEntryCancelled
)

// PerShardLeaseSchedulerOptions configures the scheduler.  Store, Registry,
// layout, hash profile, holder identity, a positive LeaseTTL and positive
// backoff bounds are required.
type PerShardLeaseSchedulerOptions struct {
	Store                  authoritystore.ShardWriteAuthorityStore
	Registry               *routeauthority.LeaseGuardRegistry
	AuthorityLayoutVersion string
	HashVersion            uint32
	HashSeed               uint64
	HolderID               string
	HolderBootID           string
	LeaseTTL               time.Duration
	BaseDelay              time.Duration
	MaxDelay               time.Duration
	Jitter                 time.Duration
	Now                    func() time.Time
	JitterSeed             int64
	// RenewAttemptTimeout bounds each renewal RPC attempt.  Before the local
	// safety deadline it never exceeds the remaining local window, so a blocked
	// store can neither stall the entry forever nor block other shards.
	RenewAttemptTimeout time.Duration
	// RenewRecoveryTimeout bounds a renewal attempt once the local deadline has
	// passed (the shard is fail-closed and the entry is recovering).
	RenewRecoveryTimeout time.Duration
	// SafetyMargin bounds the recovery budget.  The frozen guarantee is:
	// if the network recovers before latest_safe_restore_at (= authority
	// lease expiry - RecoveryCompletionBudget), the scheduler MUST complete at
	// least one same-generation renew before the lease expires.  Zero disables
	// the startup budget check (used by legacy tests).
	SafetyMargin time.Duration
	// RecoveryBaseDelay / RecoveryMaxDelay / RecoveryJitterMax /
	// RecoverySchedulerSlack define the INDEPENDENT recovery cadence used once
	// an entry is LOCAL_FENCED_RENEWING.  Entering the local fence resets the
	// ordinary transient backoff (which may have grown to seconds) so a
	// recovery renew fires promptly; the ordinary 13s-style backoff must not be
	// inherited into the recovery path.
	RecoveryBaseDelay      time.Duration
	RecoveryMaxDelay       time.Duration
	RecoveryJitterMax      time.Duration
	RecoverySchedulerSlack time.Duration
	// OnFatal is invoked (once) when the scheduler observes a fatal condition
	// (an un-fencable lost generation, a topology mismatch, or an entry panic).
	OnFatal func(error)
}

// PerShardLeaseScheduler renews each installed shard lease independently.
// With zero entries it holds no goroutine and can be closed safely.
type PerShardLeaseScheduler struct {
	store          authoritystore.ShardWriteAuthorityStore
	registry       *routeauthority.LeaseGuardRegistry
	layout         string
	hashVersion    uint32
	hashSeed       uint64
	holderID       string
	holderBootID   string
	leaseTTL       time.Duration
	baseDelay      time.Duration
	maxDelay       time.Duration
	jitter         time.Duration
	renewTimeout   time.Duration
	recoverTimeout time.Duration
	recoveryBase   time.Duration
	recoveryMax    time.Duration
	recoveryJitter time.Duration
	recoverySlack  time.Duration
	safetyMargin   time.Duration
	now            func() time.Time
	rand           *rand.Rand
	onFatal        func(error)

	mu           sync.Mutex
	entries      map[ShardKey]*leaseEntry
	reservations map[ShardKey]*leaseReservation
	nextToken    uint64
	fatal        atomic.Bool
	fatalErr     atomic.Pointer[errorRecord]

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closed    atomic.Bool
	closeOnce sync.Once
	closeDone chan struct{}
	// closeEnterHook is TEST_ONLY. It observes each concurrent Close caller
	// before it joins closeOnce/closeDone.
	closeEnterHook func()
	// entryExitHook is TEST_ONLY. It runs immediately before an entry's
	// wg.Done so concurrent Close tests can freeze the shared join barrier.
	entryExitHook func()
	// postRenewHook is a test-only injection point invoked outside s.mu right
	// after a renewal's post-renew revalidation, before the outcome is handled.
	postRenewHook func()
	// lostGenHook is a test-only injection point invoked (outside s.mu) after
	// a LostGeneration entry has been cancelled, so tests can deterministically
	// wait for a stale late LostGeneration response to be fully processed.
	lostGenHook func(key ShardKey, gen MembershipGeneration)
}

// errorRecord boxes the fatal error stored atomically on the scheduler so a
// fatal condition or a panicked OnFatal callback is observable without ever
// re-invoking OnFatal.
type errorRecord struct {
	err error
}

type leaseEntry struct {
	key        ShardKey
	generation MembershipGeneration
	token      uint64
	active     bool
	backoff    int
	fenced     bool // true while the guard's fence reason is LocalDeadlineExpired
	runtime    *routeauthority.HolderLeaseRuntime
	stopCh     chan struct{}
}

type leaseReservation struct {
	key        ShardKey
	generation MembershipGeneration
	token      uint64
	active     bool
}

// NewPerShardLeaseScheduler validates the options and builds the scheduler.
func NewPerShardLeaseScheduler(opts PerShardLeaseSchedulerOptions) (*PerShardLeaseScheduler, error) {
	if opts.Store == nil || opts.Registry == nil {
		return nil, errors.New("authority composition: lease scheduler requires store and registry")
	}
	if opts.AuthorityLayoutVersion == "" || opts.HolderID == "" || opts.HolderBootID == "" {
		return nil, errors.New("authority composition: lease scheduler requires layout and holder identity")
	}
	if opts.HashVersion == 0 || opts.HashSeed == 0 {
		return nil, errors.New("authority composition: lease scheduler requires the snapshot hash profile")
	}
	if opts.LeaseTTL <= 0 || opts.BaseDelay <= 0 || opts.MaxDelay <= 0 || opts.MaxDelay < opts.BaseDelay {
		return nil, errors.New("authority composition: lease scheduler requires positive TTL and monotonic backoff bounds")
	}
	if opts.RenewAttemptTimeout <= 0 {
		return nil, errors.New("authority composition: lease scheduler requires a positive renew attempt timeout")
	}
	if opts.RenewRecoveryTimeout < 0 {
		return nil, errors.New("authority composition: lease scheduler requires a non-negative recovery attempt timeout")
	}
	if opts.RecoveryBaseDelay < 0 || opts.RecoveryMaxDelay < 0 {
		return nil, errors.New("authority composition: lease scheduler requires non-negative recovery backoff bounds")
	}
	// RT1B-3D recovery budget hardening (Focused Review P1 #1 + P1 #2).
	//
	// P1 #1 -- the budget check MUST run against the FINAL EFFECTIVE recovery
	// parameters, unconditionally whenever a safety margin is configured.  A
	// partially explicit configuration (e.g. an explicit jitter/slack with a
	// derived max) must never bypass the fail-closed invariant just because
	// RecoveryMaxDelay happened to be the zero-valued field.
	//
	// P1 #2 -- the completion budget covers the worst-case restore timeline
	// conservatively:
	//
	//   network restore while Recovery Attempt A is already in flight
	//     -> A's connection is not reusable; it burns a full attempt timeout
	//   -> recovery cadence delay (max + jitter)
	//   -> Attempt B must still complete within one more attempt timeout
	//
	// so the budget is
	//   2*RenewRecoveryTimeout + RecoveryMaxDelay + RecoveryJitterMax +
	//   RecoverySchedulerSlack < SafetyMargin.
	// Structurally no third timeout is possible: renewOnce runs serially on the
	// entry goroutine and every attempt is bounded by its own context deadline,
	// so at most ONE attempt is ever in flight and it always returns before
	// the next attempt starts.
	margin := opts.SafetyMargin
	if margin < 0 {
		return nil, errors.New("authority composition: lease scheduler requires a non-negative safety margin")
	}
	recoveryBase := opts.RecoveryBaseDelay
	if recoveryBase <= 0 {
		recoveryBase = opts.BaseDelay
	}
	recoveryMax := opts.RecoveryMaxDelay
	recoveryTimeout := opts.RenewRecoveryTimeout
	if recoveryTimeout <= 0 {
		recoveryTimeout = opts.RenewAttemptTimeout
	}
	recoveryJitter := opts.RecoveryJitterMax
	if recoveryJitter < 0 {
		return nil, errors.New("authority composition: lease scheduler requires a non-negative recovery jitter max")
	}
	recoverySlack := opts.RecoverySchedulerSlack
	if recoverySlack < 0 {
		return nil, errors.New("authority composition: lease scheduler requires a non-negative recovery scheduler slack")
	}

	// Default derivation applies ONLY to fields the caller left at zero; a
	// derived cadence is capped inside the safety margin so the all-default
	// path is safe by construction.  An explicitly configured field is never
	// silently reduced -- it participates in the unconditional budget check
	// below and fails startup if the budget cannot fit inside the margin.
	if margin > 0 {
		if recoveryMax <= 0 {
			recoveryMax = opts.MaxDelay
			if cap := margin / 4; recoveryMax > cap {
				recoveryMax = cap
			}
		}
		if opts.RenewRecoveryTimeout <= 0 {
			if cap := margin / 4; recoveryTimeout > cap {
				recoveryTimeout = cap
			}
		}
		if recoveryJitter == 0 {
			recoveryJitter = recoveryMax / 4
		}
	}
	if recoveryMax <= 0 {
		recoveryMax = opts.MaxDelay
	}
	if recoveryJitter == 0 {
		recoveryJitter = recoveryMax / 4
	}
	if recoveryBase > recoveryMax {
		// A DERIVED base is capped to the effective max; an EXPLICIT base
		// above the effective max is a configuration contradiction and fails
		// closed below.
		if opts.RecoveryBaseDelay <= 0 {
			recoveryBase = recoveryMax
		} else {
			return nil, fmt.Errorf("authority composition: lease scheduler recovery base %v exceeds recovery max %v", recoveryBase, recoveryMax)
		}
	}

	if margin > 0 {
		// Unconditional fail-closed budget check against the FINAL EFFECTIVE
		// values.  checkedRecoveryBudgetSum also detects duration overflow
		// (an overflowing budget is by definition larger than any finite
		// safety margin and must fail startup).
		budget, ok := checkedRecoveryBudgetSum(recoveryTimeout, recoveryTimeout, recoveryMax, recoveryJitter, recoverySlack)
		if !ok || budget >= margin {
			return nil, fmt.Errorf(
				"authority composition: unsafe lease recovery budget: effective budget %v (attempt_timeout=%v max_delay=%v jitter=%v slack=%v) must be < safety margin %v",
				budget, recoveryTimeout, recoveryMax, recoveryJitter, recoverySlack, margin)
		}
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.JitterSeed == 0 {
		opts.JitterSeed = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &PerShardLeaseScheduler{
		store:          opts.Store,
		registry:       opts.Registry,
		layout:         opts.AuthorityLayoutVersion,
		hashVersion:    opts.HashVersion,
		hashSeed:       opts.HashSeed,
		holderID:       opts.HolderID,
		holderBootID:   opts.HolderBootID,
		leaseTTL:       opts.LeaseTTL,
		baseDelay:      opts.BaseDelay,
		maxDelay:       opts.MaxDelay,
		jitter:         opts.Jitter,
		renewTimeout:   opts.RenewAttemptTimeout,
		recoverTimeout: recoveryTimeout,
		recoveryBase:   recoveryBase,
		recoveryMax:    recoveryMax,
		recoveryJitter: recoveryJitter,
		recoverySlack:  recoverySlack,
		safetyMargin:   margin,
		now:            opts.Now,
		rand:           rand.New(rand.NewSource(opts.JitterSeed)),
		onFatal:        opts.OnFatal,
		entries:        make(map[ShardKey]*leaseEntry),
		reservations:   make(map[ShardKey]*leaseReservation),
		ctx:            ctx,
		cancel:         cancel,
		closeDone:      make(chan struct{}),
	}, nil
}

// RecoveryBudgetInfo exposes the FINAL EFFECTIVE recovery cadence resolved at
// construction, for startup logging and operator diagnostics.  It is read-only
// and safe to call at any time.
type RecoveryBudgetInfo struct {
	SafetyMargin           time.Duration
	RecoveryBaseDelay      time.Duration
	RecoveryMaxDelay       time.Duration
	RecoveryJitterMax      time.Duration
	RecoveryAttemptTimeout time.Duration
	RecoverySchedulerSlack time.Duration
	// CompletionBudget is 2*RecoveryAttemptTimeout + RecoveryMaxDelay +
	// RecoveryJitterMax + RecoverySchedulerSlack: the worst-case time from a
	// restore point to a successful same-generation renew (a blocked attempt
	// burns one timeout, the cadence waits max+jitter, the next attempt burns
	// one more timeout).
	CompletionBudget time.Duration
	// Satisfied reports CompletionBudget < SafetyMargin (false when the
	// safety margin is zero / not configured).
	Satisfied bool
}

// RecoveryBudgetInfo returns the effective recovery budget for startup
// logging.  A nil scheduler returns the zero value.
func (s *PerShardLeaseScheduler) RecoveryBudgetInfo() RecoveryBudgetInfo {
	if s == nil {
		return RecoveryBudgetInfo{}
	}
	budget, _ := checkedRecoveryBudgetSum(s.recoverTimeout, s.recoverTimeout, s.recoveryMax, s.recoveryJitter, s.recoverySlack)
	return RecoveryBudgetInfo{
		SafetyMargin:           s.safetyMargin,
		RecoveryBaseDelay:      s.recoveryBase,
		RecoveryMaxDelay:       s.recoveryMax,
		RecoveryJitterMax:      s.recoveryJitter,
		RecoveryAttemptTimeout: s.recoverTimeout,
		RecoverySchedulerSlack: s.recoverySlack,
		CompletionBudget:       budget,
		Satisfied:              s.safetyMargin > 0 && budget < s.safetyMargin,
	}
}

// checkedRecoveryBudgetSum adds non-negative durations with overflow
// detection.  ok=false reports an un-representable budget: by construction it
// exceeds any finite safety margin and must fail closed.
func checkedRecoveryBudgetSum(durations ...time.Duration) (sum time.Duration, ok bool) {
	var acc time.Duration
	for _, d := range durations {
		if d < 0 {
			return 0, false
		}
		if d > 0 && acc > time.Duration(math.MaxInt64)-d {
			return 0, false
		}
		acc += d
	}
	return acc, true
}

// Reserve creates an un-activated reservation before the local guard is
// installed.  It is purely local and bounded; it never starts a goroutine and
// never touches Redis.  A closed scheduler fails here so nothing is admitted.
func (s *PerShardLeaseScheduler) Reserve(key ShardKey, generation MembershipGeneration) (*LeaseRegistrationReservation, error) {
	if s == nil {
		return nil, ErrLeaseSchedulerClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return nil, ErrLeaseSchedulerClosed
	}
	s.nextToken++
	reservation := &leaseReservation{key: key, generation: generation, token: s.nextToken, active: true}
	s.reservations[key] = reservation
	return &LeaseRegistrationReservation{Key: key, Generation: generation, Token: reservation.token}, nil
}

// Activate promotes a reservation to a live entry and starts its goroutine.
// It is purely local.  A reservation that was cancelled, a closed scheduler, or
// a stale reservation all fail; the caller then rolls the guard back.
func (s *PerShardLeaseScheduler) Activate(reservation *LeaseRegistrationReservation) error {
	if s == nil || reservation == nil {
		return ErrLeaseSchedulerEntryMissing
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return ErrLeaseSchedulerClosed
	}
	r, ok := s.reservations[reservation.Key]
	if !ok || !r.active || r.token != reservation.Token {
		return ErrLeaseSchedulerEntryMissing
	}
	if existing, ok := s.entries[reservation.Key]; ok {
		s.stopEntryLocked(existing)
		delete(s.entries, reservation.Key)
	}
	entry := &leaseEntry{
		key:        reservation.Key,
		generation: reservation.Generation,
		token:      r.token,
		active:     true,
		stopCh:     make(chan struct{}),
	}
	r.active = false
	delete(s.reservations, reservation.Key)
	s.entries[reservation.Key] = entry
	s.wg.Add(1)
	go entry.run(s)
	return nil
}

// CancelReservation releases an un-activated reservation.
func (s *PerShardLeaseScheduler) CancelReservation(reservation *LeaseRegistrationReservation) {
	if s == nil || reservation == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.reservations[reservation.Key]; ok && r.token == reservation.Token {
		r.active = false
		delete(s.reservations, reservation.Key)
	}
}

// Cancel stops the lease entry for a shard generation: the entry is made
// non-refreshable immediately and its goroutine exits.  Only the exact
// generation is cancelled, so an old Cancel can never stop a newer entry.  It
// is idempotent.
func (s *PerShardLeaseScheduler) Cancel(key ShardKey, generation MembershipGeneration) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, ok := s.entries[key]; ok && entry.generation.equal(generation) {
		s.stopEntryLocked(entry)
		delete(s.entries, key)
	}
	return nil
}

// EntryCount returns the number of active lease entries.
func (s *PerShardLeaseScheduler) EntryCount() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// ActiveEntries returns a coherent snapshot of the active entry generations,
// keyed by shard.  It is purely local and bounded; consistency tests compare it
// against the reconciler's installed memberships and the registry's guards.
func (s *PerShardLeaseScheduler) ActiveEntries() map[ShardKey]MembershipGeneration {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[ShardKey]MembershipGeneration, len(s.entries))
	for key, entry := range s.entries {
		out[key] = entry.generation
	}
	return out
}

// EntryState returns the observable state for one shard, derived from the
// guard snapshot and the active entry.
func (s *PerShardLeaseScheduler) EntryState(key ShardKey) LeaseEntryState {
	if s == nil {
		return LeaseEntryCancelled
	}
	s.mu.Lock()
	_, hasEntry := s.entries[key]
	s.mu.Unlock()
	if !hasEntry {
		return LeaseEntryCancelled
	}
	handle, ok := s.registry.Guard(key.Domain, key.Shard)
	if !ok || handle == nil {
		return LeaseEntryCancelled
	}
	snapshot := handle.Snapshot()
	if snapshot.Admitted {
		return LeaseEntryActive
	}
	switch handle.FenceReason() {
	case routeauthority.LeaseFenceLocalDeadlineExpired:
		return LeaseEntryLocalFencedRenewing
	case routeauthority.LeaseFenceLostGeneration, routeauthority.LeaseFenceMembershipRemoved, routeauthority.LeaseFenceRuntimeFatal:
		return LeaseEntryLostGeneration
	default:
		return LeaseEntryCancelled
	}
}

// Fatal reports whether a fatal condition has been observed.
func (s *PerShardLeaseScheduler) Fatal() bool {
	if s == nil {
		return false
	}
	return s.fatal.Load()
}

// Close cancels every in-flight renewal and stops all entry goroutines.  It is
// idempotent and safe with zero entries. The unique close owner marks closed
// and stops every entry under mu, which prevents any concurrent Activate from
// calling wg.Add after wg.Wait begins. Every concurrent caller then waits for
// the same closeDone barrier and cannot return before all entry owners exit.
func (s *PerShardLeaseScheduler) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	closeEnterHook := s.closeEnterHook
	s.mu.Unlock()
	if closeEnterHook != nil {
		closeEnterHook()
	}
	s.closeOnce.Do(func() {
		defer close(s.closeDone)
		s.closed.Store(true)
		s.cancel()
		s.mu.Lock()
		for _, entry := range s.entries {
			s.stopEntryLocked(entry)
		}
		s.entries = make(map[ShardKey]*leaseEntry)
		s.reservations = make(map[ShardKey]*leaseReservation)
		s.mu.Unlock()
		s.wg.Wait()
	})
	<-s.closeDone
}

func (s *PerShardLeaseScheduler) stopEntryLocked(entry *leaseEntry) {
	entry.active = false
	select {
	case <-entry.stopCh:
	default:
		close(entry.stopCh)
	}
}

func (e *leaseEntry) run(s *PerShardLeaseScheduler) {
	// A panic is a code/runtime fault, never a network failure: recover, fence
	// the target shard precisely, cancel the entry and raise the fatal flag.
	defer func() {
		if recovered := recover(); recovered != nil {
			s.handleEntryPanic(e, recovered, debug.Stack())
		}
	}()
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		hook := s.entryExitHook
		s.mu.Unlock()
		if hook != nil {
			hook()
		}
	}()
	initialDelay := s.initialDelay()
	if dw := s.deadlineAwareDelay(e, initialDelay); dw < initialDelay {
		initialDelay = dw
	}
	timer := time.NewTimer(initialDelay)
	defer timer.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-e.stopCh:
			return
		case <-timer.C:
			switch e.renewOnce(s) {
			case renewDone:
				return
			case renewRetry:
				// RT1B-3D deadline-aware wakeup: the next renew must fire no
				// later than the local safety deadline, so an ordinary
				// backoff that grew to seconds can NEVER sleep past the
				// deadline and delay the recoverable fence transition.
				delay := s.backoffDelay(e)
				if dw := s.deadlineAwareDelay(e, delay); dw < delay {
					delay = dw
				}
				timer.Reset(delay)
			}
		}
	}
}

// handleEntryPanic contains an entry goroutine panic: it stops the entry,
// fences the target generation locally (only the shard this entry owns), and
// raises OnFatal so ProcessReady can be pulled down.
func (s *PerShardLeaseScheduler) handleEntryPanic(e *leaseEntry, recovered any, stack []byte) {
	s.mu.Lock()
	if entry, ok := s.entries[e.key]; ok && entry == e {
		s.stopEntryLocked(entry)
		delete(s.entries, e.key)
	}
	s.mu.Unlock()
	// Precisely fence the target shard if its guard is still this generation.
	if handle, ok := s.registry.Guard(e.key.Domain, e.key.Shard); ok && handle != nil {
		snapshot := handle.Snapshot()
		if snapshot.Authority.AssignmentRevision != 0 && s.authorityMatches(snapshot.Authority, e.key, e.generation) {
			handle.FenceForReason(routeauthority.LeaseFenceRuntimeFatal)
		}
	}
	s.notifyFatal(fmt.Errorf("authority composition: lease entry panic on %v: %v\n%s", e.key, recovered, stack))
}

type renewStep uint8

const (
	renewDone renewStep = iota
	renewRetry
)

func (e *leaseEntry) renewOnce(s *PerShardLeaseScheduler) renewStep {
	s.mu.Lock()
	if !e.active || s.closed.Load() {
		s.mu.Unlock()
		return renewDone
	}
	token, gen := e.token, e.generation
	runtime := e.runtime
	s.mu.Unlock()

	// RT1B-3D recovery transition: the first time the entry observes the local
	// safety deadline (LOCAL_FENCED_RENEWING), reset the ordinary transient
	// backoff so the recovery path starts immediately with the independent
	// recovery cadence instead of inheriting a multi-second ordinary backoff.
	fenced := s.isLocalFenced(e)
	s.mu.Lock()
	e.markFenceTransition(fenced)
	s.mu.Unlock()

	if runtime == nil {
		r, err := s.runtimeFor(e.key, gen)
		if err != nil {
			// No guard for the entry's generation: removed or replaced.  Stop
			// the entry instead of re-admitting anything.  The cancel is
			// generation-fenced: a replacement entry for the same shard under
			// a newer generation is never touched by this stale entry.
			_ = s.Cancel(e.key, gen)
			return renewDone
		}
		s.mu.Lock()
		if e.runtime == nil {
			e.runtime = r
			runtime = r
		}
		s.mu.Unlock()
	}

	req := routeauthority.RenewWithFenceRequest(
		s.layout, e.key.Domain, e.key.Shard, s.holderID, s.holderBootID,
		gen.AssignmentRevision, gen.WriterEpoch, gen.LeaseEpoch, s.leaseTTL.Milliseconds(),
	)
	// Every attempt gets an independent bounded RPC timeout so a blocked store
	// can never stall this entry forever or block other shards.
	attemptCtx, cancel := context.WithTimeout(s.ctx, s.attemptTimeout(e))
	defer cancel()
	outcome, prepared, _ := runtime.PrepareRenew(attemptCtx, s.store, req)

	// LostGeneration is a normal control-plane event: the repository proved
	// this generation moved away (planned migration / replacement / removal).
	// PrepareRenew already fenced the local guard for exactly this generation;
	// the entry is cancelled and the shard stops refreshing.  A normal lost
	// generation must NEVER raise Scheduler.Fatal or pull down ProcessReady.
	// Only a failed local fence -- the exact generation's guard is still
	// admitted -- is an invariant violation and goes to OnFatal.  The check is
	// generation-aware so a newer guard installed for the same shard (G2
	// replacing G1) is never misread as G1 still being admitted.
	if outcome == routeauthority.LeaseRenewLostGeneration {
		// A stale entry whose renewal returned LostGeneration after the shard
		// was replaced must only cancel ITS OWN generation: the newer entry
		// (and its guard) for the same key must survive untouched.  The fence
		// itself is generation-aware (isAdmittedGeneration), so a newer guard
		// is never misread as this generation still being admitted.
		admitted := s.isAdmittedGeneration(e.key, gen)
		_ = s.Cancel(e.key, gen)
		// Test hooks are read under s.mu so a test installing them while
		// entries are running is race-free.
		s.mu.Lock()
		hook := s.lostGenHook
		s.mu.Unlock()
		if hook != nil {
			hook(e.key, gen)
		}
		if admitted {
			s.notifyFatal(lostGenerationFenceFatal(e.key, gen))
		}
		return renewDone
	}

	// Late-response revalidation.  All registry/handle reads happen outside
	// s.mu because they take the registry/handle locks and must never run
	// under the scheduler lock: a panic on that path would unwind into the
	// entry's recover handler, which re-locks s.mu, and deadlock.  The
	// scheduler lock is taken only afterwards to re-confirm the entry token,
	// generation and active state before the response is applied, so a late
	// renewal can never re-admit a removed shard.
	installedDesired := s.isInstalledAndDesired(e.key, gen)
	s.mu.Lock()
	valid := e.active && e.token == token && e.generation.equal(gen) && installedDesired
	s.mu.Unlock()
	if !valid {
		// A stale late response must only cancel its own generation, never a
		// replacement entry on the same shard key.
		_ = s.Cancel(e.key, gen)
		return renewDone
	}
	s.mu.Lock()
	postRenewHook := s.postRenewHook
	s.mu.Unlock()
	if postRenewHook != nil {
		postRenewHook()
	}

	// RT1B-3D deadline-aware recovery wakeup: the registry Snapshot() taken
	// inside attemptTimeout/runtimeFor promotes the recoverable local-deadline
	// fence exactly during THIS renewOnce.  Re-observe the fence here and
	// transition (reset the ordinary backoff) in the same pass, so the
	// recovery cadence computed on renewRetry starts from a fresh backoff
	// instead of inheriting a seconds-class ordinary backoff for one more
	// cycle.  markFenceTransition still resets only on the first non-fenced ->
	// fenced edge, so a continuously fenced entry never busy-loops.
	fencedNow := s.isLocalFenced(e)
	s.mu.Lock()
	e.markFenceTransition(fencedNow)
	s.mu.Unlock()

	switch outcome {
	case routeauthority.LeaseRenewSuccess:
		if prepared == nil {
			_ = s.Cancel(e.key, gen)
			return renewDone
		}
		// Apply locally only while the exact generation is still installed and
		// its fence reason is recoverable (None or LocalDeadlineExpired).
		if err := s.registry.RefreshOrRearmIfMatches(e.key.Domain, e.key.Shard, s.expectedAuthority(e.key, gen), *prepared); err != nil {
			_ = s.Cancel(e.key, gen)
			return renewDone
		}
		e.resetBackoff()
		return renewRetry
	case routeauthority.LeaseRenewTransient:
		// Bounded backoff; the guard stays admitted until its local deadline.
		e.incBackoff()
		return renewRetry
	default:
		_ = s.Cancel(e.key, gen)
		return renewDone
	}
}

// lostGenerationFenceFatal builds the fatal diagnostic for a lost generation
// whose local guard is still admitted.  It carries both the shard key and the
// FULL membership generation so an operator can pinpoint which authority epoch
// failed to fence without relying on the storage-layer fallback
// (ShardWriteAuthority/WriterEpoch/WriterClaimID/RecordVersion/Receipt).
func lostGenerationFenceFatal(key ShardKey, gen MembershipGeneration) error {
	return fmt.Errorf("lost generation could not be fenced locally: key=%v generation=%+v", key, gen)
}

func (s *PerShardLeaseScheduler) runtimeFor(key ShardKey, gen MembershipGeneration) (*routeauthority.HolderLeaseRuntime, error) {
	handle, ok := s.registry.Guard(key.Domain, key.Shard)
	if !ok || handle == nil {
		return nil, ErrLeaseSchedulerEntryMissing
	}
	snapshot := handle.Snapshot()
	if snapshot.Authority.AssignmentRevision == 0 || !s.authorityMatches(snapshot.Authority, key, gen) {
		return nil, ErrLeaseSchedulerEntryMissing
	}
	// A local-deadline fenced guard is still this generation's guard and may be
	// rearmed by PrepareRenew/RefreshOrRearm; terminal reasons are not.
	switch handle.FenceReason() {
	case routeauthority.LeaseFenceMembershipRemoved, routeauthority.LeaseFenceLostGeneration, routeauthority.LeaseFenceRuntimeFatal:
		return nil, ErrLeaseSchedulerEntryMissing
	}
	runtime, err := routeauthority.NewHolderLeaseRuntime(handle.Guard(), s.now)
	if err != nil {
		return nil, err
	}
	return runtime, nil
}

// isInstalledAndDesired verifies that the registry still holds the exact
// generation this entry renews for, regardless of whether it is locally fenced
// by a recoverable reason (installed == desired projection).
func (s *PerShardLeaseScheduler) isInstalledAndDesired(key ShardKey, gen MembershipGeneration) bool {
	handle, ok := s.registry.Guard(key.Domain, key.Shard)
	if !ok || handle == nil {
		return false
	}
	snapshot := handle.Snapshot()
	if snapshot.Authority.AssignmentRevision == 0 || !s.authorityMatches(snapshot.Authority, key, gen) {
		return false
	}
	switch handle.FenceReason() {
	case routeauthority.LeaseFenceMembershipRemoved, routeauthority.LeaseFenceLostGeneration, routeauthority.LeaseFenceRuntimeFatal:
		return false
	}
	return true
}

func (s *PerShardLeaseScheduler) isAdmitted(key ShardKey) bool {
	snapshot, ok := s.registry.Snapshot(key.Domain, key.Shard)
	return ok && snapshot.Admitted
}

// isAdmittedGeneration reports whether the guard installed for the exact
// generation is still admitting writes.  It is generation-aware: a newer guard
// installed for the same shard (G2 replacing G1) never makes G1 look admitted,
// so a lost G1 can never be misclassified as an un-fencable invariant
// violation while G2 is serving.
func (s *PerShardLeaseScheduler) isAdmittedGeneration(key ShardKey, gen MembershipGeneration) bool {
	handle, ok := s.registry.Guard(key.Domain, key.Shard)
	if !ok || handle == nil {
		return false
	}
	snapshot := handle.Snapshot()
	return snapshot.Authority.AssignmentRevision != 0 && s.authorityMatches(snapshot.Authority, key, gen) && snapshot.Admitted
}

func (s *PerShardLeaseScheduler) authorityMatches(authority authoritystore.ShardWriteAuthority, key ShardKey, gen MembershipGeneration) bool {
	return authority.AuthorityLayoutVersion == s.layout &&
		authority.HashVersion == s.hashVersion && authority.HashSeed == s.hashSeed &&
		authority.Domain == key.Domain && authority.VirtualShard == key.Shard &&
		authority.HolderID == gen.HolderID && authority.HolderBootID == gen.HolderBootID &&
		authority.AssignmentRevision == gen.AssignmentRevision &&
		authority.WriterEpoch == gen.WriterEpoch && authority.LeaseEpoch == gen.LeaseEpoch
}

func (s *PerShardLeaseScheduler) expectedAuthority(key ShardKey, gen MembershipGeneration) authoritystore.ShardWriteAuthority {
	return authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: s.layout,
		HashVersion:            s.hashVersion,
		HashSeed:               s.hashSeed,
		Domain:                 key.Domain,
		VirtualShard:           key.Shard,
		HolderID:               gen.HolderID,
		HolderBootID:           gen.HolderBootID,
		AssignmentRevision:     gen.AssignmentRevision,
		WriterEpoch:            gen.WriterEpoch,
		LeaseEpoch:             gen.LeaseEpoch,
		State:                  authoritystore.ShardAuthorityActive,
	}
}

// notifyFatal marks the scheduler fatal once, records the fatal error, and only
// then invokes the OnFatal callback through safeNotifyFatal.  Callers must have
// already committed the local fail-closed state (entry cancelled, target shard
// fenced) before calling it, so a misbehaving callback can never reopen a
// shard.
func (s *PerShardLeaseScheduler) notifyFatal(err error) {
	if !s.fatal.CompareAndSwap(false, true) {
		return
	}
	s.fatalErr.Store(&errorRecord{err: err})
	s.safeNotifyFatal(err)
}

// safeNotifyFatal invokes the OnFatal callback without holding any scheduler or
// handle lock and contains a callback panic so a misbehaving OnFatal can
// neither deadlock nor crash the process.  The fatal state and the target shard
// fence are already committed before the callback runs.
func (s *PerShardLeaseScheduler) safeNotifyFatal(err error) {
	callback := s.onFatal
	if callback == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			s.recordFatalCallbackPanic(recovered, debug.Stack())
		}
	}()
	callback(err)
}

// recordFatalCallbackPanic records that the OnFatal callback itself panicked.
// The fatal flag is already set and the entry/fence are already committed, so
// the system stays fail-closed; this only preserves the diagnostic and never
// re-invokes OnFatal (no recursion).  The original fatal cause is preserved as
// the wrapped primary error and the callback panic is attached as a secondary
// diagnostic, so FatalErr never loses the reason the runtime became fatal.
func (s *PerShardLeaseScheduler) recordFatalCallbackPanic(recovered any, stack []byte) {
	secondary := fmt.Errorf("OnFatal callback panicked: %v\n%s", recovered, stack)
	if primary := s.fatalErr.Load(); primary != nil {
		s.fatalErr.Store(&errorRecord{err: fmt.Errorf("%w\n--- %v", primary.err, secondary)})
		return
	}
	s.fatalErr.Store(&errorRecord{err: fmt.Errorf("authority composition: %v", secondary)})
}

// FatalErr returns the fatal error recorded when the fatal flag was raised, or
// nil while the scheduler is not fatal.  A panicked OnFatal callback replaces
// the original fatal error with the callback-panic diagnostic.
func (s *PerShardLeaseScheduler) FatalErr() error {
	if s == nil {
		return nil
	}
	if rec := s.fatalErr.Load(); rec != nil {
		return rec.err
	}
	return nil
}

func (e *leaseEntry) incBackoff() {
	e.backoff++
}

func (e *leaseEntry) resetBackoff() {
	e.backoff = 0
}

// markFenceTransition tracks the local-fence state transition: the FIRST
// observation of the recoverable local deadline resets the ordinary transient
// backoff (RT1B-3D) so the recovery cadence starts fresh instead of inheriting
// a multi-second ordinary backoff.
func (e *leaseEntry) markFenceTransition(fenced bool) {
	if fenced && !e.fenced {
		e.fenced = true
		e.backoff = 0
	} else if !fenced {
		e.fenced = false
	}
}

// attemptTimeout bounds one renewal RPC: before the local safety deadline it is
// capped by the remaining local window; once the deadline has passed (the shard
// is fail-closed and the entry is recovering) it uses the configured recovery
// timeout.
func (s *PerShardLeaseScheduler) attemptTimeout(e *leaseEntry) time.Duration {
	handle, ok := s.registry.Guard(e.key.Domain, e.key.Shard)
	if ok && handle != nil {
		snapshot := handle.Snapshot()
		if !snapshot.Deadline.IsZero() {
			remaining := snapshot.Deadline.Sub(s.now())
			if remaining > 0 {
				return min(s.renewTimeout, remaining)
			}
		}
	}
	return s.recoverTimeout
}

// initialDelay scatters N entries with deterministic jitter so renewals do not
// pile up on the same tick.
func (s *PerShardLeaseScheduler) initialDelay() time.Duration {
	delay := s.baseDelay / 2
	if s.jitter > 0 {
		delay += s.jitterDelay()
	}
	return delay
}

// backoffDelay computes the delay before the next renew attempt.  For an
// entry in the LOCAL_FENCED_RENEWING recovery state it uses the INDEPENDENT
// recovery cadence (RecoveryBaseDelay..RecoveryMaxDelay with recovery jitter),
// never the ordinary transient backoff that may have grown to seconds.  For a
// healthy entry it returns min(baseDelay<<backoff, maxDelay) plus stable jitter.
func (s *PerShardLeaseScheduler) backoffDelay(e *leaseEntry) time.Duration {
	if s.isLocalFenced(e) {
		shift := min(e.backoff, 20)
		// Saturate before shifting so an extreme but representable legacy
		// configuration cannot wrap the duration negative and create an
		// immediate retry loop.
		delay := s.recoveryMax
		if shift == 0 || s.recoveryBase <= s.recoveryMax>>shift {
			delay = s.recoveryBase << shift
		}
		if s.recoveryJitter > 0 {
			delta := s.recoveryJitterDelay()
			if delta > 0 && delay > time.Duration(math.MaxInt64)-delta {
				delay = time.Duration(math.MaxInt64)
			} else {
				delay += delta
			}
		}
		// Symmetric jitter may shorten a retry but can never erase the
		// positive recovery cadence and turn a fenced shard into a busy loop.
		return max(delay, s.recoveryBase)
	}
	shift := min(e.backoff, 20)
	delay := min(s.baseDelay<<shift, s.maxDelay)
	if s.jitter > 0 {
		delay += s.jitterDelay()
	}
	return delay
}

// isLocalFenced reports whether the entry's guard is currently in the
// recoverable local-deadline fence.
func (s *PerShardLeaseScheduler) isLocalFenced(e *leaseEntry) bool {
	handle, ok := s.registry.Guard(e.key.Domain, e.key.Shard)
	if !ok || handle == nil {
		return false
	}
	return handle.FenceReason() == routeauthority.LeaseFenceLocalDeadlineExpired
}

// deadlineAwareDelay caps the next-renew wakeup so an entry can NEVER sleep
// past its local safety deadline (RT1B-3D-FIX-DEADLINE-AWARE-RECOVERY-WAKEUP).
//
// For an entry still in the ordinary (non-fenced) path the wakeup time is
//
//	min(backoff retry time, local deadline)
//
// so an ordinary backoff that grew to seconds still fires before the deadline.
// Once the deadline has passed but this entry has not yet observed it, the
// delay collapses to zero: the very next loop iteration runs renewOnce, whose
// attemptTimeout/registry Snapshot promotes the recoverable local-deadline
// fence, markFenceTransition resets the ordinary backoff exactly once, and the
// entry moves onto the INDEPENDENT recovery cadence.
//
// For an entry already fenced (or guarded by a terminal reason) the recovery
// cadence is already budget-constrained, so the retryDelay is returned
// unchanged -- a terminal fence can never be woken into a recovery cadence and
// no busy loop is introduced.
func (s *PerShardLeaseScheduler) deadlineAwareDelay(e *leaseEntry, retryDelay time.Duration) time.Duration {
	if s.isLocalFenced(e) {
		return retryDelay
	}
	handle, ok := s.registry.Guard(e.key.Domain, e.key.Shard)
	if !ok || handle == nil {
		return retryDelay
	}
	switch handle.FenceReason() {
	case routeauthority.LeaseFenceMembershipRemoved, routeauthority.LeaseFenceLostGeneration, routeauthority.LeaseFenceRuntimeFatal:
		return retryDelay
	}
	// Snapshot() also promotes the local-deadline fence once the deadline has
	// passed, so the returned remaining window is coherent with FenceReason.
	snapshot := handle.Snapshot()
	if snapshot.Deadline.IsZero() {
		return retryDelay
	}
	remaining := snapshot.Deadline.Sub(s.now())
	if remaining <= 0 {
		// The local safety deadline passed but this entry is still on an
		// ordinary timer: wake immediately to enter the recoverable fence.
		return 0
	}
	if remaining < retryDelay {
		return remaining
	}
	return retryDelay
}

// recoveryJitterDelay returns a deterministic value bounded by
// [-recoveryJitter, +recoveryJitter]. The scheduler PRNG is shared by all
// per-shard goroutines, so access is serialized. Sampling magnitude and sign
// separately avoids overflowing 2*recoveryJitter for large legacy values.
func (s *PerShardLeaseScheduler) recoveryJitterDelay() time.Duration {
	if s.recoveryJitter <= 0 {
		return 0
	}
	s.mu.Lock()
	limit := int64(s.recoveryJitter)
	var magnitude int64
	if limit == math.MaxInt64 {
		magnitude = s.rand.Int63()
	} else {
		magnitude = s.rand.Int63n(limit + 1)
	}
	negative := s.rand.Intn(2) == 0
	s.mu.Unlock()
	if negative {
		return -time.Duration(magnitude)
	}
	return time.Duration(magnitude)
}

// jitterDelay returns a deterministic value in [-jitter, +jitter).
func (s *PerShardLeaseScheduler) jitterDelay() time.Duration {
	if s.jitter <= 0 {
		return 0
	}
	s.mu.Lock()
	delta := time.Duration(s.rand.Int63n(int64(2*s.jitter)+1)) - s.jitter
	s.mu.Unlock()
	return delta
}
