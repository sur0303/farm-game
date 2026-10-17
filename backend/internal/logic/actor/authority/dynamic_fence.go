package authority

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

var (
	ErrDynamicFenceUnavailable = errors.New("authority: dynamic shard fence unavailable")
	ErrLeaseAdmission          = errors.New("authority: holder lease admission denied")
	ErrRenewGenerationChanged  = errors.New("authority: holder renewal changed generation")
)

// RouteSnapshotSource is a local, complete-snapshot view. Implementations
// must return a deep copy or an immutable value; it is intentionally not a
// storage client and is never called against Redis on an Actor turn.
type RouteSnapshotSource interface {
	Snapshot() actorrouting.RouteSnapshot
}

// ShardRouteSnapshotSource is the optional hot-path view implemented by the
// process-local dynamic assignment provider. SnapshotSource remains the
// compatibility fallback for test and control-plane providers.
type ShardRouteSnapshotSource interface {
	LookupShard(actorrouting.RouteDomain, uint32) (actorrouting.ShardRouteSnapshot, error)
}

type shardLeaseKey struct {
	domain actorrouting.RouteDomain
	shard  uint32
}

// LeaseGuardRegistry holds one local HolderLeaseGuard per domain/shard. A
// guard is installed only after a successful Claim/Transfer/Renew result has
// been validated by the holder runtime.
type LeaseGuardRegistry struct {
	mu           sync.RWMutex
	guards       map[shardLeaseKey]*authoritystore.HolderLeaseGuard
	handles      map[shardLeaseKey]*LeaseGuardHandle
	floors       map[shardLeaseKey]authorityGeneration
	profileBound bool
	layout       string
	hashVersion  uint32
	hashSeed     uint64
}

func NewLeaseGuardRegistry() *LeaseGuardRegistry {
	return &LeaseGuardRegistry{
		guards:  make(map[shardLeaseKey]*authoritystore.HolderLeaseGuard),
		handles: make(map[shardLeaseKey]*LeaseGuardHandle),
		floors:  make(map[shardLeaseKey]authorityGeneration),
	}
}

func (r *LeaseGuardRegistry) Install(domain actorrouting.RouteDomain, shard uint32, guard *authoritystore.HolderLeaseGuard) error {
	if r == nil || guard == nil || (domain != actorrouting.DomainPlayer && domain != actorrouting.DomainFarm) {
		return fmt.Errorf("%w: guard/domain", ErrLeaseAdmission)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.guards == nil {
		r.guards = make(map[shardLeaseKey]*authoritystore.HolderLeaseGuard)
	}
	if r.handles == nil {
		r.handles = make(map[shardLeaseKey]*LeaseGuardHandle)
	}
	if r.floors == nil {
		r.floors = make(map[shardLeaseKey]authorityGeneration)
	}
	key := shardLeaseKey{domain: domain, shard: shard}
	handle := newLeaseGuardHandle(guard, time.Now)
	if r.profileBound {
		if err := handle.bindProfile(r.layout, actorrouting.HashProfile{Version: r.hashVersion, Seed: r.hashSeed}); err != nil {
			handle.Fence()
			return err
		}
	}
	incoming := handle.Snapshot()
	r.revokeHandleLocked(key, r.handles[key])
	if err := r.validateFloorLocked(key, incoming.Authority); err != nil {
		handle.Fence()
		return err
	}
	r.guards[key] = guard
	r.handles[key] = handle
	return nil
}

func (r *LeaseGuardRegistry) installHandle(domain actorrouting.RouteDomain, shard uint32, handle *LeaseGuardHandle) error {
	if r == nil || handle == nil || handle.Guard() == nil || (domain != actorrouting.DomainPlayer && domain != actorrouting.DomainFarm) {
		return fmt.Errorf("%w: guard/domain", ErrLeaseAdmission)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.guards == nil {
		r.guards = make(map[shardLeaseKey]*authoritystore.HolderLeaseGuard)
	}
	if r.handles == nil {
		r.handles = make(map[shardLeaseKey]*LeaseGuardHandle)
	}
	if r.floors == nil {
		r.floors = make(map[shardLeaseKey]authorityGeneration)
	}
	key := shardLeaseKey{domain: domain, shard: shard}
	if r.profileBound {
		if err := handle.bindProfile(r.layout, actorrouting.HashProfile{Version: r.hashVersion, Seed: r.hashSeed}); err != nil {
			handle.Fence()
			return err
		}
	}
	incoming := handle.Snapshot()
	r.revokeHandleLocked(key, r.handles[key])
	if err := r.validateFloorLocked(key, incoming.Authority); err != nil {
		handle.Fence()
		return err
	}
	r.guards[key] = handle.Guard()
	r.handles[key] = handle
	return nil
}

func (r *LeaseGuardRegistry) bindProfile(layout string, profile actorrouting.HashProfile) error {
	if r == nil || layout == "" {
		return fmt.Errorf("%w: authority layout", ErrLeaseAdmission)
	}
	if err := profile.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.profileBound && (r.layout != layout || r.hashVersion != profile.Version || r.hashSeed != profile.Seed) {
		return fmt.Errorf("%w: authority profile already bound", ErrDynamicFenceUnavailable)
	}
	r.profileBound = true
	r.layout, r.hashVersion, r.hashSeed = layout, profile.Version, profile.Seed
	for key, handle := range r.handles {
		if err := handle.bindProfile(layout, profile); err != nil {
			handle.Fence()
			return fmt.Errorf("%w: %s/%d: %v", ErrLeaseAdmission, key.domain, key.shard, err)
		}
	}
	return nil
}

func (r *LeaseGuardRegistry) Remove(domain actorrouting.RouteDomain, shard uint32) {
	if r == nil {
		return
	}
	r.mu.Lock()
	key := shardLeaseKey{domain: domain, shard: shard}
	r.revokeHandleLocked(key, r.handles[key])
	delete(r.guards, key)
	delete(r.handles, key)
	r.mu.Unlock()
}

// Replace revokes the old handle before installing the replacement. A
// replacement must carry a strictly newer authority generation; reusing the
// same generation after a safety window is never allowed.
func (r *LeaseGuardRegistry) Replace(domain actorrouting.RouteDomain, shard uint32, guard *authoritystore.HolderLeaseGuard) error {
	return r.Install(domain, shard, guard)
}

func (r *LeaseGuardRegistry) revokeHandleLocked(key shardLeaseKey, handle *LeaseGuardHandle) {
	if r == nil || handle == nil {
		return
	}
	handle.Fence()
	snapshot := handle.Snapshot()
	if snapshot.Authority.AssignmentRevision == 0 {
		return
	}
	if r.floors == nil {
		r.floors = make(map[shardLeaseKey]authorityGeneration)
	}
	floor := generationOf(snapshot.Authority)
	if previous, ok := r.floors[key]; !ok || generationStrictlyNewer(floor, previous) {
		r.floors[key] = floor
	}
}

func (r *LeaseGuardRegistry) validateFloorLocked(key shardLeaseKey, authority authoritystore.ShardWriteAuthority) error {
	if authority.AssignmentRevision == 0 {
		return nil // An empty guard has no admission generation yet.
	}
	incoming := generationOf(authority)
	floor, ok := r.floors[key]
	if ok && !generationStrictlyNewer(incoming, floor) {
		return fmt.Errorf("%w: generation floor assignment=%d writer=%d lease=%d", authoritystore.ErrStaleLeaseGrant, floor.assignment, floor.writer, floor.lease)
	}
	return nil
}

func (r *LeaseGuardRegistry) handle(domain actorrouting.RouteDomain, shard uint32) (*LeaseGuardHandle, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	handle, ok := r.handles[shardLeaseKey{domain: domain, shard: shard}]
	r.mu.RUnlock()
	return handle, ok
}

// LeaseGenerationSnapshot is a coherent local view. Admitted is derived from
// the same guard snapshot as Authority and Deadline; callers must not combine
// it with a separate CanAdmitWrite call.
type LeaseGenerationSnapshot struct {
	State     authoritystore.HolderLeaseGuardState
	Authority authoritystore.ShardWriteAuthority
	Deadline  time.Time
	Admitted  bool
}

// LeaseGuardHandle is the RT1 lifecycle wrapper around the frozen base guard.
// Once the base guard has fenced a generation (including expiry), that exact
// generation cannot be installed again through this handle. A strictly newer
// assignment generation may be installed after the old one is fenced.
type LeaseGuardHandle struct {
	mu               sync.Mutex
	guard            *authoritystore.HolderLeaseGuard
	expired          authorityGeneration
	authorityNow     func() time.Time
	localNow         func() time.Time
	localDeadline    time.Time
	localDeadlineSet bool
	profileBound     bool
	layout           string
	hashVersion      uint32
	hashSeed         uint64
	fenceReason      LeaseFenceReason
}

type authorityGeneration struct {
	layout      string
	hashVersion uint32
	hashSeed    uint64
	domain      actorrouting.RouteDomain
	shard       uint32
	holderID    string
	holderBoot  string
	assignment  uint64
	writer      uint64
	lease       uint64
	state       authoritystore.ShardAuthorityState
}

func newLeaseGuardHandle(guard *authoritystore.HolderLeaseGuard, now func() time.Time) *LeaseGuardHandle {
	if now == nil {
		now = time.Now
	}
	return newLeaseGuardHandleWithClocks(guard, now, now)
}

func newLeaseGuardHandleWithClocks(guard *authoritystore.HolderLeaseGuard, authorityNow, localNow func() time.Time) *LeaseGuardHandle {
	if authorityNow == nil {
		authorityNow = time.Now
	}
	if localNow == nil {
		localNow = time.Now
	}
	return &LeaseGuardHandle{guard: guard, authorityNow: authorityNow, localNow: localNow}
}

func (h *LeaseGuardHandle) bindProfile(layout string, profile actorrouting.HashProfile) error {
	if h == nil || h.guard == nil || layout == "" {
		return ErrLeaseAdmission
	}
	if err := profile.Validate(); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.profileBound && (h.layout != layout || h.hashVersion != profile.Version || h.hashSeed != profile.Seed) {
		return authoritystore.ErrInconsistentLeaseGrant
	}
	h.profileBound = true
	h.layout, h.hashVersion, h.hashSeed = layout, profile.Version, profile.Seed
	if h.guard != nil {
		_, authority, _ := h.guard.Snapshot()
		if authority.AssignmentRevision != 0 && !h.profileMatchesLocked(authority) {
			return authoritystore.ErrInconsistentLeaseGrant
		}
	}
	return nil
}

func (h *LeaseGuardHandle) profileMatchesLocked(authority authoritystore.ShardWriteAuthority) bool {
	return !h.profileBound || (authority.AuthorityLayoutVersion == h.layout && authority.HashVersion == h.hashVersion && authority.HashSeed == h.hashSeed)
}

func (h *LeaseGuardHandle) Guard() *authoritystore.HolderLeaseGuard {
	if h == nil {
		return nil
	}
	return h.guard
}

func generationOf(authority authoritystore.ShardWriteAuthority) authorityGeneration {
	return authorityGeneration{layout: authority.AuthorityLayoutVersion, hashVersion: authority.HashVersion, hashSeed: authority.HashSeed, domain: authority.Domain, shard: authority.VirtualShard, holderID: authority.HolderID, holderBoot: authority.HolderBootID, assignment: authority.AssignmentRevision, writer: authority.WriterEpoch, lease: authority.LeaseEpoch, state: authority.State}
}

func sameGeneration(left, right authorityGeneration) bool {
	return left == right
}

func generationStrictlyNewer(next, previous authorityGeneration) bool {
	return next.layout == previous.layout && next.hashVersion == previous.hashVersion && next.hashSeed == previous.hashSeed && next.domain == previous.domain && next.shard == previous.shard &&
		next.assignment > previous.assignment && next.writer > previous.writer && next.lease > previous.lease
}

func (h *LeaseGuardHandle) snapshotLocked() LeaseGenerationSnapshot {
	if h == nil || h.guard == nil {
		return LeaseGenerationSnapshot{State: authoritystore.HolderLeaseGuardFenced}
	}
	state, authority, deadline := h.guard.Snapshot()
	// localDeadline is converted once from a trusted authority result (it
	// mirrors the guard's safety-margin-adjusted deadline). The hot path
	// compares the process-local clock to that local deadline; it does not
	// reinterpret the authority's wall-clock milliseconds on every turn.
	if h.localDeadlineSet && h.localNow != nil && !h.localNow().Before(h.localDeadline) {
		// Local safety deadline expired (the guard layer already fenced it via
		// expireLocked, or fences it here): stop admitting new writes, but keep
		// the reason recoverable so an exact-generation renewal can rearm.
		// promoteFenceReasonLocked refuses to downgrade a terminal reason, so
		// a membership-removed / lost-generation / fatal guard can never be
		// silently rewritten to a recoverable expiry.
		if state == authoritystore.HolderLeaseGuardActive {
			h.guard.Fence()
		}
		h.promoteFenceReasonLocked(LeaseFenceLocalDeadlineExpired)
		state, authority, deadline = h.guard.Snapshot()
	}
	generation := generationOf(authority)
	if state == authoritystore.HolderLeaseGuardFenced && authority.AssignmentRevision != 0 && h.fenceReason != LeaseFenceLocalDeadlineExpired {
		// A terminal fence (membership removed / lost generation / fatal) marks
		// the generation expired so it can never be revived by InstallGrant.
		h.expired = generation
	}
	admitted := state == authoritystore.HolderLeaseGuardActive && !sameGeneration(h.expired, generation)
	if h.localDeadlineSet && (deadline.IsZero() || h.localDeadline.Before(deadline)) {
		deadline = h.localDeadline
	}
	return LeaseGenerationSnapshot{State: state, Authority: authority, Deadline: deadline, Admitted: admitted}
}

func (h *LeaseGuardHandle) setLocalDeadlineLocked(authority authoritystore.ShardWriteAuthority, requestStarted time.Time, acceptedTTLms int64) error {
	if h == nil || h.authorityNow == nil || h.localNow == nil {
		return ErrLeaseAdmission
	}
	if authority.State != authoritystore.ShardAuthorityActive || authority.LeaseUntilMs <= 0 {
		h.localDeadline = time.Time{}
		h.localDeadlineSet = false
		return nil
	}
	localNow := h.localNow()
	// The guard layer already computed the safety-margin-adjusted deadline from
	// the trusted accepted TTL (requestStarted + TTL - margin).  The handle
	// mirrors exactly that deadline so both layers expire at the same instant.
	_, _, guardDeadline := h.guard.Snapshot()
	deadline := guardDeadline
	if deadline.IsZero() || !localNow.Before(deadline) {
		return authoritystore.ErrLeaseDeadlineExpired
	}
	if h.localDeadlineSet {
		_, currentAuthority, _ := h.guard.Snapshot()
		if sameGeneration(generationOf(currentAuthority), generationOf(authority)) && deadline.Before(h.localDeadline) {
			// Same-generation observations/renewals can extend, never shorten,
			// the locally accepted deadline.
			deadline = h.localDeadline
		}
	}
	h.localDeadline = deadline
	h.localDeadlineSet = true
	return nil
}

func (h *LeaseGuardHandle) Snapshot() LeaseGenerationSnapshot {
	if h == nil {
		return LeaseGenerationSnapshot{State: authoritystore.HolderLeaseGuardFenced}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.snapshotLocked()
}

func (h *LeaseGuardHandle) withAdmission(fn func(LeaseGenerationSnapshot) bool) bool {
	if h == nil || fn == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	snapshot := h.snapshotLocked()
	if !snapshot.Admitted {
		return false
	}
	return fn(snapshot)
}

func (h *LeaseGuardHandle) withFence(fn func(LeaseGenerationSnapshot) (authoritystore.ShardWriteFence, error)) (authoritystore.ShardWriteFence, error) {
	if h == nil || fn == nil {
		return authoritystore.ShardWriteFence{}, ErrLeaseAdmission
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	snapshot := h.snapshotLocked()
	if !snapshot.Admitted {
		return authoritystore.ShardWriteFence{}, ErrLeaseAdmission
	}
	return fn(snapshot)
}

func (h *LeaseGuardHandle) InstallGrant(requestStarted time.Time, result authoritystore.ShardAuthorityTransitionResult) error {
	if h == nil || h.guard == nil {
		return ErrLeaseAdmission
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	current := h.snapshotLocked()
	incoming := generationOf(result.Authority)
	if !h.profileMatchesLocked(result.Authority) {
		h.guard.Fence()
		h.snapshotLocked()
		return authoritystore.ErrInconsistentLeaseGrant
	}
	if current.Authority.AssignmentRevision != 0 && sameGeneration(h.expired, incoming) {
		return fmt.Errorf("%w: expired generation cannot be revived", authoritystore.ErrStaleLeaseGrant)
	}
	if current.Authority.AssignmentRevision != 0 && sameGeneration(generationOf(current.Authority), generationOf(result.Authority)) && result.Authority.LeaseUntilMs < current.Authority.LeaseUntilMs {
		h.guard.Fence()
		h.snapshotLocked()
		return fmt.Errorf("%w: same-generation lease deadline regressed", authoritystore.ErrStaleLeaseGrant)
	}
	if err := h.guard.InstallGrant(requestStarted, result); err != nil {
		return err
	}
	if err := h.setLocalDeadlineLocked(result.Authority, requestStarted, result.AcceptedTTLms); err != nil {
		h.guard.Fence()
		h.snapshotLocked()
		return err
	}
	if h.expired.assignment != 0 && result.Authority.AssignmentRevision > h.expired.assignment {
		h.expired = authorityGeneration{}
	}
	// A successful grant re-arms the guard and clears the fence reason so the
	// scheduler sees LeaseEntryActive again.  A terminal reason can never reach
	// this point (the expired-generation check above and the guard layer's
	// revoked fence both reject the same-generation grant), but the guard stays
	// defensive: an unrecoverable reason is never cleared.
	if !isTerminalFenceReason(h.fenceReason) {
		h.fenceReason = LeaseFenceNone
	}
	return nil
}

func validObservedAuthority(authority authoritystore.ShardWriteAuthority) bool {
	if authority.AuthorityLayoutVersion == "" || authority.HashVersion == 0 || authority.HashSeed == 0 || authority.AssignmentRevision == 0 || authority.WriterEpoch == 0 || authority.LeaseEpoch == 0 {
		return false
	}
	switch authority.State {
	case authoritystore.ShardAuthorityActive:
		return authority.HolderID != "" && authority.HolderBootID != "" && authority.LeaseUntilMs > 0
	case authoritystore.ShardAuthorityUnassigned:
		return authority.HolderID == "" && authority.HolderBootID == "" && authority.LeaseUntilMs == 0
	default:
		return false
	}
}

func (h *LeaseGuardHandle) ObserveAuthority(authority authoritystore.ShardWriteAuthority) error {
	if h == nil || h.guard == nil {
		return ErrLeaseAdmission
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !validObservedAuthority(authority) {
		h.guard.Fence()
		h.snapshotLocked()
		return authoritystore.ErrInconsistentLeaseGrant
	}
	current := h.snapshotLocked()
	if !h.profileMatchesLocked(authority) {
		h.guard.Fence()
		h.snapshotLocked()
		return authoritystore.ErrInconsistentLeaseGrant
	}
	if current.Authority.AssignmentRevision != 0 {
		incoming := generationOf(authority)
		observed := generationOf(current.Authority)
		if authority.Domain != current.Authority.Domain || authority.VirtualShard != current.Authority.VirtualShard {
			h.guard.Fence()
			h.snapshotLocked()
			return fmt.Errorf("%w: authority address changed", authoritystore.ErrInconsistentLeaseGrant)
		}
		if authority.AssignmentRevision < current.Authority.AssignmentRevision {
			h.guard.Fence()
			h.snapshotLocked()
			return authoritystore.ErrStaleLeaseGrant
		}
		if authority.AssignmentRevision == current.Authority.AssignmentRevision {
			if !sameGeneration(incoming, observed) {
				h.guard.Fence()
				h.snapshotLocked()
				return authoritystore.ErrInconsistentLeaseGrant
			}
			if current.State == authoritystore.HolderLeaseGuardFenced || sameGeneration(h.expired, incoming) {
				return authoritystore.ErrStaleLeaseGrant
			}
			if authority.LeaseUntilMs < current.Authority.LeaseUntilMs {
				h.guard.Fence()
				h.snapshotLocked()
				return fmt.Errorf("%w: same-generation lease deadline regressed", authoritystore.ErrStaleLeaseGrant)
			}
			return nil
		}
		if !generationStrictlyNewer(incoming, observed) {
			h.guard.Fence()
			h.snapshotLocked()
			return authoritystore.ErrInconsistentLeaseGrant
		}
	}
	err := h.guard.ObserveAuthority(authority)
	if err == nil {
		if deadlineErr := h.setLocalDeadlineLocked(authority, time.Time{}, 0); deadlineErr != nil {
			h.guard.Fence()
			h.snapshotLocked()
			return deadlineErr
		}
	}
	h.snapshotLocked()
	return err
}

func (h *LeaseGuardHandle) Fence() {
	h.FenceForReason(LeaseFenceLostGeneration)
}

// FenceForReason fences the guard and records why.  Only
// LeaseFenceLocalDeadlineExpired allows a later exact-generation renewal to
// rearm the guard; every other reason is terminal for that generation.  The
// reason is set through promoteFenceReasonLocked so a terminal reason is
// sticky: the follow-up snapshotLocked can never downgrade it to a
// LocalDeadlineExpired.
func (h *LeaseGuardHandle) FenceForReason(reason LeaseFenceReason) {
	if h == nil || h.guard == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.promoteFenceReasonLocked(reason)
	h.guard.Fence()
	h.snapshotLocked()
}

// promoteFenceReasonLocked is the single write entry for fenceReason; callers
// must hold h.mu.  A terminal reason (MembershipRemoved / LostGeneration /
// RuntimeFatal) is sticky: once set it can never be downgraded by a later
// LocalDeadlineExpired.  LocalDeadlineExpired is only entered from
// LeaseFenceNone, so a recoverable expiry never erases an earlier terminal
// reason and a terminal reason never erases a recoverable one.
func (h *LeaseGuardHandle) promoteFenceReasonLocked(next LeaseFenceReason) {
	current := h.fenceReason
	if isTerminalFenceReason(current) {
		return
	}
	if isTerminalFenceReason(next) {
		h.fenceReason = next
		return
	}
	if current == LeaseFenceNone && next == LeaseFenceLocalDeadlineExpired {
		h.fenceReason = next
	}
}

// FenceReason returns the reason the guard was last fenced (LeaseFenceNone
// while admitted).
func (h *LeaseGuardHandle) FenceReason() LeaseFenceReason {
	if h == nil {
		return LeaseFenceNone
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.fenceReason
}

// Snapshot returns one guard state for FirstHop/other local adapters.
func (r *LeaseGuardRegistry) Snapshot(domain actorrouting.RouteDomain, shard uint32) (LeaseGenerationSnapshot, bool) {
	handle, ok := r.handle(domain, shard)
	if !ok || handle == nil {
		return LeaseGenerationSnapshot{}, false
	}
	return handle.Snapshot(), true
}

func (r *LeaseGuardRegistry) Guard(domain actorrouting.RouteDomain, shard uint32) (*LeaseGuardHandle, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	handle, ok := r.handles[shardLeaseKey{domain: domain, shard: shard}]
	r.mu.RUnlock()
	return handle, ok
}

// LeaseGuardAdmission adapts a per-shard guard registry to the gateway
// FirstHop lease seam. It performs only local guard checks.
type LeaseGuardAdmission struct {
	Guards *LeaseGuardRegistry
}

func NewLeaseGuardAdmission(guards *LeaseGuardRegistry) (LeaseGuardAdmission, error) {
	if guards == nil {
		return LeaseGuardAdmission{}, fmt.Errorf("%w: guard registry required", ErrLeaseAdmission)
	}
	return LeaseGuardAdmission{Guards: guards}, nil
}

func (a LeaseGuardAdmission) CanStartActorTurn(domain actorrouting.RouteDomain, shard uint32) bool {
	snapshot, ok := a.Guards.Snapshot(domain, shard)
	return ok && snapshot.Admitted
}

// ValidateRouteGeneration is the complete FirstHop local guard seam. It
// validates domain/shard, holder identity, boot, assignment revision, writer
// epoch, and lease epoch from one coherent guard snapshot.
func (a LeaseGuardAdmission) ValidateRouteGeneration(domain actorrouting.RouteDomain, shard uint32, holderID, holderBootID string, assignmentRevision, writerEpoch, leaseEpoch uint64) bool {
	handle, ok := a.Guards.handle(domain, shard)
	if !ok || handle == nil {
		return false
	}
	return handle.withAdmission(func(snapshot LeaseGenerationSnapshot) bool {
		authority := snapshot.Authority
		return authority.Domain == domain && authority.VirtualShard == shard && authority.HolderID == holderID && authority.HolderBootID == holderBootID &&
			authority.AssignmentRevision == assignmentRevision && authority.WriterEpoch == writerEpoch && authority.LeaseEpoch == leaseEpoch &&
			authority.State == authoritystore.ShardAuthorityActive
	})
}

// DynamicFenceSource derives the current ShardWriteFence from the latest
// complete assignment and the local monotonic lease guard. Its WriteFence
// method performs only hash/table/guard work; it never reaches Redis or any
// remote authority.
type DynamicFenceSource struct {
	calculator *actorrouting.Calculator
	snapshots  RouteSnapshotSource
	guards     *LeaseGuardRegistry
	layout     string
	now        func() time.Time
}

type DynamicFenceSourceOptions struct {
	Calculator             *actorrouting.Calculator
	Snapshots              RouteSnapshotSource
	Guards                 *LeaseGuardRegistry
	AuthorityLayoutVersion string
	Now                    func() time.Time
}

func NewDynamicFenceSource(opts DynamicFenceSourceOptions) (*DynamicFenceSource, error) {
	if opts.Calculator == nil || opts.Snapshots == nil || opts.Guards == nil || opts.AuthorityLayoutVersion == "" {
		return nil, fmt.Errorf("%w: calculator/snapshots/guards/layout required", ErrDynamicFenceUnavailable)
	}
	profile := opts.Calculator.Profile()
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	if err := opts.Guards.bindProfile(opts.AuthorityLayoutVersion, profile); err != nil {
		return nil, err
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &DynamicFenceSource{calculator: opts.Calculator, snapshots: opts.Snapshots, guards: opts.Guards, layout: opts.AuthorityLayoutVersion, now: opts.Now}, nil
}

var _ FenceSource = (*DynamicFenceSource)(nil)

func (s *DynamicFenceSource) WriteFence(ctx context.Context, address authoritystore.ActorAddress) (authoritystore.ShardWriteFence, error) {
	if s == nil || s.calculator == nil || s.snapshots == nil || s.guards == nil {
		return authoritystore.ShardWriteFence{}, ErrDynamicFenceUnavailable
	}
	if ctx == nil {
		return authoritystore.ShardWriteFence{}, ErrDynamicFenceUnavailable
	}
	if err := ctx.Err(); err != nil {
		return authoritystore.ShardWriteFence{}, err
	}
	if address.ActorID == 0 || (address.Domain != actorrouting.DomainPlayer && address.Domain != actorrouting.DomainFarm) {
		return authoritystore.ShardWriteFence{}, fmt.Errorf("%w: actor address", ErrLeaseAdmission)
	}
	shard, err := s.calculator.ShardFor(address.Domain, address.ActorID)
	if err != nil {
		return authoritystore.ShardWriteFence{}, err
	}
	var (
		globalRevision uint64
		hashVersion    uint32
		hashSeed       uint64
		route          actorrouting.ShardRoute
		holder         actorrouting.HolderRef
	)
	if fast, ok := s.snapshots.(ShardRouteSnapshotSource); ok {
		view, lookupErr := fast.LookupShard(address.Domain, shard)
		if lookupErr != nil {
			return authoritystore.ShardWriteFence{}, fmt.Errorf("%w: shard lookup: %v", ErrDynamicFenceUnavailable, lookupErr)
		}
		globalRevision, hashVersion, hashSeed = view.GlobalRevision, view.HashVersion, view.HashSeed
		route, holder = view.Route, view.Holder
	} else {
		// Compatibility path for static/test providers. Production uses the
		// shard-only lookup above and does not clone all route tables.
		snapshot := s.snapshots.Snapshot()
		globalRevision, hashVersion, hashSeed = snapshot.GlobalRevision, snapshot.HashVersion, snapshot.HashSeed
		routes := domainSnapshot(snapshot, address.Domain)
		if shard >= routes.ShardCount || int(shard) >= len(routes.Routes) {
			return authoritystore.ShardWriteFence{}, fmt.Errorf("%w: shard coverage", ErrDynamicFenceUnavailable)
		}
		route = routes.Routes[shard]
		if route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
			return authoritystore.ShardWriteFence{}, fmt.Errorf("%w: holder index", ErrDynamicFenceUnavailable)
		}
		holder = snapshot.Holders[route.HolderIndex]
	}
	if globalRevision == 0 || hashVersion != s.calculator.Profile().Version || hashSeed != s.calculator.Profile().Seed {
		return authoritystore.ShardWriteFence{}, fmt.Errorf("%w: snapshot profile/revision", ErrDynamicFenceUnavailable)
	}
	if route.State != actorrouting.ShardStateActive || route.ShardRevision == 0 || route.ExpectedWriterEpoch == 0 || route.ExpectedLeaseEpoch == 0 || route.LeaseUntilMs <= 0 {
		return authoritystore.ShardWriteFence{}, fmt.Errorf("%w: route inactive or incomplete", ErrLeaseAdmission)
	}
	if holder.State != actorrouting.HolderStateActive || holder.InstanceID == "" || holder.BootID == "" {
		return authoritystore.ShardWriteFence{}, fmt.Errorf("%w: holder inactive or incomplete", ErrLeaseAdmission)
	}
	handle, ok := s.guards.handle(address.Domain, shard)
	if !ok || handle == nil {
		return authoritystore.ShardWriteFence{}, fmt.Errorf("%w: lease guard", ErrLeaseAdmission)
	}
	// The handle holds its lifecycle lock while the coherent guard snapshot is
	// checked and the fence is constructed. A concurrent Fence/Observe cannot
	// pass between a boolean admission check and the returned tuple.
	return handle.withFence(func(lease LeaseGenerationSnapshot) (authoritystore.ShardWriteFence, error) {
		authority := lease.Authority
		if !lease.Admitted || authority.State != authoritystore.ShardAuthorityActive || authority.AuthorityLayoutVersion != s.layout || authority.HashVersion != s.calculator.Profile().Version || authority.HashSeed != s.calculator.Profile().Seed || authority.Domain != address.Domain || authority.VirtualShard != shard ||
			authority.HolderID != holder.InstanceID || authority.HolderBootID != holder.BootID ||
			authority.AssignmentRevision != route.ShardRevision || authority.WriterEpoch != route.ExpectedWriterEpoch || authority.LeaseEpoch != route.ExpectedLeaseEpoch {
			return authoritystore.ShardWriteFence{}, fmt.Errorf("%w: assignment/lease tuple mismatch", ErrLeaseAdmission)
		}
		return authoritystore.ShardWriteFence{
			ExpectedHolderID: authority.HolderID, ExpectedHolderBootID: authority.HolderBootID,
			ExpectedAssignmentRevision: authority.AssignmentRevision,
			ExpectedWriterEpoch:        authority.WriterEpoch,
			ExpectedLeaseEpoch:         authority.LeaseEpoch,
		}, nil
	})
}

// HolderLeaseRuntime is the holder-side seam used by a runtime after the
// RouteManager has granted a generation. It centralizes installation, observe,
// renew, and fail-closed admission without changing Actor Registry packages.
type HolderLeaseRuntime struct {
	handle       *LeaseGuardHandle
	authorityNow func() time.Time
	localNow     func() time.Time
}

func NewHolderLeaseRuntime(guard *authoritystore.HolderLeaseGuard, now func() time.Time) (*HolderLeaseRuntime, error) {
	return newHolderLeaseRuntimeWithClocks(guard, now, now)
}

// NewHolderLeaseRuntimeWithClocks is the explicit clock seam for runtimes
// whose trusted authority wall clock and process monotonic clock are supplied
// by different adapters. It owns construction of the frozen base guard so its
// expiry checks necessarily use the same local clock as the wrapper's
// monotonic deadline; callers cannot accidentally supply a wall-clock guard
// that fences a still-valid local deadline.
func NewHolderLeaseRuntimeWithClocks(holderID, holderBootID string, safetyMargin time.Duration, authorityNow, localNow func() time.Time) (*HolderLeaseRuntime, error) {
	if localNow == nil {
		localNow = time.Now
	}
	guard, err := authoritystore.NewHolderLeaseGuard(holderID, holderBootID, safetyMargin, localNow)
	if err != nil {
		return nil, err
	}
	return newHolderLeaseRuntimeWithClocks(guard, authorityNow, localNow)
}

func newHolderLeaseRuntimeWithClocks(guard *authoritystore.HolderLeaseGuard, authorityNow, localNow func() time.Time) (*HolderLeaseRuntime, error) {
	if guard == nil {
		return nil, fmt.Errorf("%w: guard required", ErrLeaseAdmission)
	}
	if authorityNow == nil {
		authorityNow = time.Now
	}
	if localNow == nil {
		localNow = time.Now
	}
	return &HolderLeaseRuntime{handle: newLeaseGuardHandleWithClocks(guard, authorityNow, localNow), authorityNow: authorityNow, localNow: localNow}, nil
}

// InstallHolderLeaseGrant is the normal Claim/Transfer/Observe handoff: it
// validates and installs the local guard first, then makes that guard visible
// to DynamicFenceSource. A failed install never enters the registry.
func InstallHolderLeaseGrant(registry *LeaseGuardRegistry, domain actorrouting.RouteDomain, shard uint32, guard *authoritystore.HolderLeaseGuard, requestStarted time.Time, result authoritystore.ShardAuthorityTransitionResult, now func() time.Time) (*HolderLeaseRuntime, error) {
	if registry == nil {
		return nil, fmt.Errorf("%w: guard registry required", ErrLeaseAdmission)
	}
	runtime, err := NewHolderLeaseRuntime(guard, now)
	if err != nil {
		return nil, err
	}
	if err := runtime.InstallGrant(requestStarted, result); err != nil {
		return nil, err
	}
	if err := registry.installHandle(domain, shard, runtime.handle); err != nil {
		return nil, err
	}
	return runtime, nil
}

func (r *HolderLeaseRuntime) Guard() *LeaseGuardHandle {
	if r == nil || r.handle == nil {
		return nil
	}
	return r.handle
}

// CanActivateActor and CanStartActorTurn are local admission checks. They do
// not refresh or query the remote authority.
func (r *HolderLeaseRuntime) CanActivateActor() bool {
	return r != nil && r.handle != nil && r.handle.Snapshot().Admitted
}

func (r *HolderLeaseRuntime) CanStartActorTurn() bool {
	return r != nil && r.handle != nil && r.handle.Snapshot().Admitted
}

func (r *HolderLeaseRuntime) InstallGrant(requestStarted time.Time, result authoritystore.ShardAuthorityTransitionResult) error {
	if r == nil || r.handle == nil {
		return ErrLeaseAdmission
	}
	return r.handle.InstallGrant(requestStarted, result)
}

func (r *HolderLeaseRuntime) Observe(authority authoritystore.ShardWriteAuthority) error {
	if r == nil || r.handle == nil {
		return ErrLeaseAdmission
	}
	return r.handle.ObserveAuthority(authority)
}

// Renew captures request start before the store call. Success extends only
// the local monotonic deadline; it cannot change holder boot, writer epoch,
// lease epoch, or assignment revision.
func (r *HolderLeaseRuntime) Renew(ctx context.Context, store authoritystore.ShardWriteAuthorityStore, req authoritystore.RenewShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	if r == nil || r.handle == nil || store == nil {
		return authoritystore.ShardAuthorityTransitionResult{}, ErrLeaseAdmission
	}
	if req.Caller.Kind != authoritystore.ShardAuthorityCallerHolder || req.Caller.InstanceID == "" || req.Caller.BootID == "" {
		r.handle.Fence()
		return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionUnauthorized}, ErrLeaseAdmission
	}
	snapshot := r.handle.Snapshot()
	authority := snapshot.Authority
	if !snapshot.Admitted || authority.HolderID != req.Caller.InstanceID || authority.HolderBootID != req.Caller.BootID ||
		authority.Domain != req.Domain || authority.VirtualShard != req.VirtualShard || !fenceEqualsAuthority(req.ExpectedFence, authority) {
		r.handle.Fence()
		return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionFenceMismatch}, ErrLeaseAdmission
	}
	// HolderLeaseGuard's request-start contract is local/monotonic. Authority
	// wall time is used only by the wrapper for one-time deadline conversion.
	requestStarted := r.localNow()
	result, err := store.RenewShardAuthority(ctx, req)
	if err != nil {
		r.handle.Fence()
		return result, err
	}
	if result.Status != authoritystore.ShardTransitionApplied && result.Status != authoritystore.ShardTransitionAlreadyApplied {
		r.handle.Fence()
		return result, fmt.Errorf("%w: renew status=%s", ErrLeaseAdmission, result.Status)
	}
	if !renewalMatchesRequest(req, authority, result) {
		r.handle.Fence()
		return result, ErrRenewGenerationChanged
	}
	if err := r.handle.InstallGrant(requestStarted, result); err != nil {
		r.handle.Fence()
		return result, err
	}
	return result, nil
}

func renewalMatchesRequest(req authoritystore.RenewShardAuthorityRequest, expected authoritystore.ShardWriteAuthority, result authoritystore.ShardAuthorityTransitionResult) bool {
	authority := result.Authority
	return result.AcceptedTTLms > 0 && authority.State == authoritystore.ShardAuthorityActive &&
		authority.AuthorityLayoutVersion == req.AuthorityLayoutVersion && authority.HashVersion == expected.HashVersion && authority.HashSeed == expected.HashSeed &&
		authority.Domain == req.Domain && authority.VirtualShard == req.VirtualShard &&
		fenceEqualsAuthority(req.ExpectedFence, authority) && authority.LeaseUntilMs > 0
}

func fenceEqualsAuthority(fence authoritystore.ShardWriteFence, authority authoritystore.ShardWriteAuthority) bool {
	return fence.ExpectedHolderID == authority.HolderID && fence.ExpectedHolderBootID == authority.HolderBootID &&
		fence.ExpectedAssignmentRevision == authority.AssignmentRevision && fence.ExpectedWriterEpoch == authority.WriterEpoch && fence.ExpectedLeaseEpoch == authority.LeaseEpoch
}

func domainSnapshot(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain) actorrouting.DomainSnapshot {
	if domain == actorrouting.DomainPlayer {
		return snapshot.Player
	}
	return snapshot.Farm
}
