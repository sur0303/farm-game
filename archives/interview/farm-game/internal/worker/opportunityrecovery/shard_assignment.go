package opportunityrecovery

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/redis/rediskey"
)

var (
	ErrWorkerAssignmentUnavailable  = errors.New("opportunity recovery: worker assignment unavailable")
	ErrWorkerAssignmentStale        = errors.New("opportunity recovery: worker assignment stale")
	ErrWorkerAssignmentGap          = errors.New("opportunity recovery: worker assignment revision gap")
	ErrWorkerShardNotOwned          = errors.New("opportunity recovery: worker shard not owned")
	ErrWorkerAuthorityFenceMismatch = errors.New("opportunity recovery: worker authority fence mismatch")
	ErrWorkerAuthorityLeaseExpired  = errors.New("opportunity recovery: worker authority lease expired")
	ErrWorkerAuthorityLeaseRollback = errors.New("opportunity recovery: worker authority lease rollback")
)

// WorkerShardAssignment is one immutable, process-local FARM_WORKER view.
// Revision is the FARM_WORKER route/fence generation, not the complete global
// RouteSnapshot revision. Zero OwnedShards is valid for a dynamic worker during
// rebalance.
type WorkerShardAssignment struct {
	Revision    uint64
	OwnedShards []uint32
}

// WorkerShardAssignmentProvider resolves the shard set a worker may scan.
// Implementations used by RunOnce must be local-memory reads; they must not
// query Redis or another control-plane service on the polling hot path.
type WorkerShardAssignmentProvider interface {
	Assignment(context.Context) (WorkerShardAssignment, error)
}

// WorkerShardClaimAdmission rechecks ownership immediately before a new
// WorkLease claim. Assignment changes do not cancel already-claimed work.
type WorkerShardClaimAdmission interface {
	CanClaimShard(context.Context, uint32, uint64) error
}

// providerBackedClaimAdmission preserves compatibility for existing local
// assignment providers. Production dynamic routing should implement the
// admission interface directly so Assignment and CanClaim share one atomic
// snapshot without rebuilding state.
type providerBackedClaimAdmission struct {
	provider WorkerShardAssignmentProvider
}

func (a providerBackedClaimAdmission) CanClaimShard(ctx context.Context, shard uint32, observedRevision uint64) error {
	assignment, err := a.provider.Assignment(ctx)
	if err != nil {
		return err
	}
	if assignment.Revision == 0 {
		assignment.Revision = 1
	}
	if observedRevision != assignment.Revision {
		return fmt.Errorf("%w: observed=%d current=%d", ErrWorkerAssignmentStale, observedRevision, assignment.Revision)
	}
	owned, err := normalizeOwnedShards(assignment.OwnedShards, true)
	if err != nil {
		return err
	}
	index := sort.Search(len(owned), func(i int) bool { return owned[i] >= shard })
	if index >= len(owned) || owned[index] != shard {
		return fmt.Errorf("%w: shard=%d", ErrWorkerShardNotOwned, shard)
	}
	return nil
}

// StaticWorkerShardAssignmentProvider is the explicit single-worker fallback.
// It is intentionally non-empty; dynamic zero-shard states use the route
// provider below.
type StaticWorkerShardAssignmentProvider struct {
	owned []uint32
}

func NewStaticWorkerShardAssignmentProvider(shards []uint32) (*StaticWorkerShardAssignmentProvider, error) {
	owned, err := normalizeOwnedShards(shards, false)
	if err != nil {
		return nil, err
	}
	return &StaticWorkerShardAssignmentProvider{owned: owned}, nil
}

func NewAllConfiguredShardsProvider(configuredShards []uint32) (*StaticWorkerShardAssignmentProvider, error) {
	return NewStaticWorkerShardAssignmentProvider(configuredShards)
}

func (p *StaticWorkerShardAssignmentProvider) Assignment(_ context.Context) (WorkerShardAssignment, error) {
	if p == nil || len(p.owned) == 0 {
		return WorkerShardAssignment{}, fmt.Errorf("%w: static shard assignment is empty", ErrInvalidConfig)
	}
	return WorkerShardAssignment{Revision: 1, OwnedShards: append([]uint32(nil), p.owned...)}, nil
}

func (p *StaticWorkerShardAssignmentProvider) CanClaimShard(ctx context.Context, shard uint32, observedRevision uint64) error {
	if ctx == nil {
		return fmt.Errorf("%w: context required", ErrInvalidConfig)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if observedRevision != 1 {
		return fmt.Errorf("%w: observed=%d current=1", ErrWorkerAssignmentStale, observedRevision)
	}
	owned := p.OwnedShards()
	index := sort.Search(len(owned), func(i int) bool { return owned[i] >= shard })
	if index >= len(owned) || owned[index] != shard {
		return fmt.Errorf("%w: shard=%d", ErrWorkerShardNotOwned, shard)
	}
	return nil
}

func (p *StaticWorkerShardAssignmentProvider) OwnedShards() []uint32 {
	if p == nil {
		return nil
	}
	return append([]uint32(nil), p.owned...)
}

// WorkerShardRouteView is the low-frequency assignment view supplied by the
// Controller. It answers which Worker holder is assigned to a shard; it does
// not contain a liveness deadline.
type WorkerShardRouteView struct {
	Shard              uint32
	HolderID           string
	HolderBootID       string
	AssignmentRevision uint64
	WriterEpoch        uint64
	LeaseEpoch         uint64
}

// WorkerShardLeaseView is the high-frequency liveness view supplied by the
// Redis Authority renewal result. It is intentionally separate from the
// Assignment Snapshot so an ordinary renewal does not require a route watch
// update before the Worker can continue scanning.
type WorkerShardLeaseView struct {
	Shard              uint32
	HolderID           string
	HolderBootID       string
	AssignmentRevision uint64
	WriterEpoch        uint64
	LeaseEpoch         uint64
	LeaseUntilMs       int64
}

// WorkerAuthorityLeaseObserver is implemented by the route provider and is
// called only after Redis has accepted a fenced authority renewal.
type WorkerAuthorityLeaseObserver interface {
	ObserveAuthorityLease(WorkerShardLeaseView) error
}

type RouteWorkerShardAssignmentOptions struct {
	LeaseCheckMode actorrouting.LeaseCheckMode
	SafetyMargin   time.Duration
	Now            func() time.Time
}

func (o RouteWorkerShardAssignmentOptions) normalized() (RouteWorkerShardAssignmentOptions, error) {
	if o.LeaseCheckMode == "" {
		o.LeaseCheckMode = actorrouting.LeaseCheckSnapshot
	}
	if o.LeaseCheckMode != actorrouting.LeaseCheckSnapshot &&
		o.LeaseCheckMode != actorrouting.LeaseCheckAuthorityDelegated {
		return RouteWorkerShardAssignmentOptions{}, fmt.Errorf("%w: unsupported worker lease check mode %q", ErrInvalidConfig, o.LeaseCheckMode)
	}
	if o.SafetyMargin < 0 || o.SafetyMargin > time.Minute {
		return RouteWorkerShardAssignmentOptions{}, fmt.Errorf("%w: invalid worker lease safety margin", ErrInvalidConfig)
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o, nil
}

// RouteWorkerShardAssignmentProvider consumes complete RouteSnapshots through
// the existing revisioned watch contract. The polling hot path reads only the
// local immutable state guarded by mu. Watch gaps and reconnects first mark the
// provider unavailable, so no new claim is admitted while a full reload runs.
//
// Route and lease state are deliberately separate: route changes replace the
// fence view, while successful Redis renewals update only leases. In the
// authority-delegated mode, LeaseUntilMs in a RouteSnapshot is never used as
// the Worker liveness source.
type RouteWorkerShardAssignmentProvider struct {
	source   actorrouting.RevisionedAssignmentProvider
	holderID string
	bootID   string
	now      func() time.Time
	mode     actorrouting.LeaseCheckMode
	margin   time.Duration

	mu                 sync.RWMutex
	ready              bool
	revision           uint64 // complete RouteSnapshot watch revision
	assignmentRevision uint64 // FARM_WORKER route/fence generation
	snapshot           actorrouting.RouteSnapshot
	routes             map[uint32]WorkerShardRouteView
	leases             map[uint32]WorkerShardLeaseView

	watchGaps     atomic.Uint64
	fullReloads   atomic.Uint64
	claimRejected atomic.Uint64
}

type RouteWorkerAssignmentStats struct {
	Revision           uint64
	AssignmentRevision uint64
	Ready              bool
	OwnedShards        uint64
	LeaseCached        uint64
	WatchGaps          uint64
	FullReloads        uint64
	ClaimRejected      uint64
}

func NewRouteWorkerShardAssignmentProvider(source actorrouting.RevisionedAssignmentProvider, holderID, bootID string) (*RouteWorkerShardAssignmentProvider, error) {
	return NewRouteWorkerShardAssignmentProviderWithOptions(source, holderID, bootID, RouteWorkerShardAssignmentOptions{})
}

func NewRouteWorkerShardAssignmentProviderWithOptions(
	source actorrouting.RevisionedAssignmentProvider,
	holderID, bootID string,
	options RouteWorkerShardAssignmentOptions,
) (*RouteWorkerShardAssignmentProvider, error) {
	holderID = strings.TrimSpace(holderID)
	bootID = strings.TrimSpace(bootID)
	if source == nil || holderID == "" || bootID == "" {
		return nil, fmt.Errorf("%w: route source, holder id, and boot id are required", ErrInvalidConfig)
	}
	normalized, err := options.normalized()
	if err != nil {
		return nil, err
	}
	return &RouteWorkerShardAssignmentProvider{
		source: source, holderID: holderID, bootID: bootID,
		now: normalized.Now, mode: normalized.LeaseCheckMode, margin: normalized.SafetyMargin,
		routes: make(map[uint32]WorkerShardRouteView), leases: make(map[uint32]WorkerShardLeaseView),
	}, nil
}

// Bootstrap installs one trusted complete snapshot. A full reload may jump
// revisions because it replaces, rather than patches, the local view.
func (p *RouteWorkerShardAssignmentProvider) Bootstrap(ctx context.Context) error {
	if p == nil || p.source == nil {
		return ErrWorkerAssignmentUnavailable
	}
	if ctx == nil {
		return fmt.Errorf("%w: context required", ErrInvalidConfig)
	}
	snapshot, err := p.source.GetSnapshot(ctx)
	if err != nil {
		p.failClosed()
		return err
	}
	if err := p.install(snapshot, true); err != nil {
		p.failClosed()
		return err
	}
	p.fullReloads.Add(1)
	return nil
}

// Run keeps the local snapshot current. Production discovery can implement
// RevisionedAssignmentProvider without changing Worker. Static providers are
// also valid and simply block until ctx is cancelled.
func (p *RouteWorkerShardAssignmentProvider) Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context required", ErrInvalidConfig)
	}
	if _, ready := p.currentRevision(); !ready {
		if err := p.Bootstrap(ctx); err != nil {
			return err
		}
	}
watchLoop:
	for {
		revision, ready := p.currentRevision()
		if !ready {
			if err := p.Bootstrap(ctx); err != nil {
				return err
			}
			revision, _ = p.currentRevision()
		}
		updates, err := p.source.WatchFrom(ctx, revision)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			p.failClosed()
			if err := p.reloadAfterGap(ctx); err != nil {
				return err
			}
			continue
		}
		for update := range updates {
			if update.Revision == 0 || update.Revision != update.Snapshot.GlobalRevision {
				p.failClosed()
				return fmt.Errorf("%w: update=%d snapshot=%d", ErrWorkerAssignmentGap, update.Revision, update.Snapshot.GlobalRevision)
			}
			current, _ := p.currentRevision()
			switch {
			case update.Revision < current:
				continue
			case update.Revision == current:
				p.mu.RLock()
				same := reflect.DeepEqual(p.snapshot, update.Snapshot)
				p.mu.RUnlock()
				if !same {
					p.failClosed()
					return fmt.Errorf("%w: conflicting revision=%d", ErrWorkerAssignmentGap, current)
				}
				continue
			case update.Revision != current+1:
				p.failClosed()
				if err := p.reloadAfterGap(ctx); err != nil {
					return err
				}
				// The old stream is no longer a trustworthy continuation after a
				// full reload. Re-subscribe from the newly installed revision instead
				// of consuming any buffered updates from the stale stream.
				continue watchLoop
			}
			if err := p.install(update.Snapshot, false); err != nil {
				p.failClosed()
				return err
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		p.failClosed()
		if err := p.reloadAfterGap(ctx); err != nil {
			return err
		}
	}
}

func (p *RouteWorkerShardAssignmentProvider) reloadAfterGap(ctx context.Context) error {
	p.watchGaps.Add(1)
	return p.Bootstrap(ctx)
}

func (p *RouteWorkerShardAssignmentProvider) install(snapshot actorrouting.RouteSnapshot, full bool) error {
	// Reuse the canonical route validator; this also verifies holder namespaces,
	// lease fields, route generations, and the FARM_WORKER/FARM shard count.
	if _, err := actorrouting.NewStaticAssignmentProvider(snapshot); err != nil {
		return err
	}
	if snapshot.FarmWorker.ShardCount == 0 {
		return fmt.Errorf("%w: snapshot has no FARM_WORKER domain", ErrWorkerAssignmentUnavailable)
	}
	routes := make(map[uint32]WorkerShardRouteView)
	for shard, route := range snapshot.FarmWorker.Routes {
		holder := snapshot.Holders[route.HolderIndex]
		if route.State == actorrouting.ShardStateActive && holder.State == actorrouting.HolderStateActive &&
			holder.InstanceID == p.holderID && holder.BootID == p.bootID {
			routes[uint32(shard)] = WorkerShardRouteView{
				Shard:              uint32(shard),
				HolderID:           holder.InstanceID,
				HolderBootID:       holder.BootID,
				AssignmentRevision: route.ShardRevision,
				WriterEpoch:        route.ExpectedWriterEpoch,
				LeaseEpoch:         route.ExpectedLeaseEpoch,
			}
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ready {
		switch {
		case snapshot.GlobalRevision < p.revision:
			return fmt.Errorf("%w: current=%d reload=%d", ErrWorkerAssignmentStale, p.revision, snapshot.GlobalRevision)
		case snapshot.GlobalRevision == p.revision:
			if !reflect.DeepEqual(p.snapshot, snapshot) {
				return fmt.Errorf("%w: conflicting revision=%d", ErrWorkerAssignmentGap, p.revision)
			}
			return nil
		case !full && snapshot.GlobalRevision != p.revision+1:
			return fmt.Errorf("%w: current=%d next=%d", ErrWorkerAssignmentGap, p.revision, snapshot.GlobalRevision)
		}
	}
	routeChanged := !p.ready || !sameWorkerRouteSet(p.routes, routes)
	assignmentRevision := p.assignmentRevision
	if !p.ready {
		assignmentRevision = snapshot.GlobalRevision
	} else if routeChanged {
		if assignmentRevision == ^uint64(0) {
			return fmt.Errorf("%w: worker assignment revision overflow", ErrWorkerAssignmentGap)
		}
		assignmentRevision++
	}
	leases := make(map[uint32]WorkerShardLeaseView, len(routes))
	for shard, route := range routes {
		if previousRoute, ok := p.routes[shard]; ok {
			if previousLease, hasLease := p.leases[shard]; hasLease && sameWorkerRouteFence(previousRoute, route) &&
				sameWorkerLeaseFence(previousLease, route) {
				leases[shard] = previousLease
			}
		}
		if p.mode == actorrouting.LeaseCheckSnapshot {
			// Compatibility mode retains the old complete-snapshot behavior. The
			// authority-delegated mode intentionally does not seed liveness from
			// this field; it must receive a successful Redis renewal instead.
			lease := snapshot.FarmWorker.Routes[shard]
			leases[shard] = WorkerShardLeaseView{
				Shard:              shard,
				HolderID:           route.HolderID,
				HolderBootID:       route.HolderBootID,
				AssignmentRevision: route.AssignmentRevision,
				WriterEpoch:        route.WriterEpoch,
				LeaseEpoch:         route.LeaseEpoch,
				LeaseUntilMs:       lease.LeaseUntilMs,
			}
		}
	}
	p.snapshot = actorrouting.CloneRouteSnapshot(snapshot)
	p.revision = snapshot.GlobalRevision
	p.assignmentRevision = assignmentRevision
	p.routes = routes
	p.leases = leases
	p.ready = true
	return nil
}

func (p *RouteWorkerShardAssignmentProvider) failClosed() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.ready = false
	p.mu.Unlock()
}

func (p *RouteWorkerShardAssignmentProvider) currentRevision() (uint64, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.revision, p.ready
}

func (p *RouteWorkerShardAssignmentProvider) Assignment(ctx context.Context) (WorkerShardAssignment, error) {
	if p == nil || ctx == nil {
		return WorkerShardAssignment{}, ErrWorkerAssignmentUnavailable
	}
	if err := ctx.Err(); err != nil {
		return WorkerShardAssignment{}, err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.ready {
		return WorkerShardAssignment{}, ErrWorkerAssignmentUnavailable
	}
	deadline := p.now().Add(p.margin).UnixMilli()
	owned := make([]uint32, 0, len(p.routes))
	for shard, route := range p.routes {
		lease, hasLease := p.leases[shard]
		if hasLease && sameWorkerLeaseFence(lease, route) && lease.LeaseUntilMs > deadline {
			owned = append(owned, shard)
		}
	}
	sort.Slice(owned, func(i, j int) bool { return owned[i] < owned[j] })
	return WorkerShardAssignment{Revision: p.assignmentRevision, OwnedShards: owned}, nil
}

func (p *RouteWorkerShardAssignmentProvider) CanClaimShard(ctx context.Context, shard uint32, observedRevision uint64) error {
	if p == nil || ctx == nil {
		return ErrWorkerAssignmentUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.ready {
		p.claimRejected.Add(1)
		return ErrWorkerAssignmentUnavailable
	}
	if observedRevision != p.assignmentRevision {
		p.claimRejected.Add(1)
		if observedRevision < p.assignmentRevision {
			return fmt.Errorf("%w: observed=%d current=%d", ErrWorkerAssignmentStale, observedRevision, p.assignmentRevision)
		}
		return fmt.Errorf("%w: observed=%d current=%d", ErrWorkerAssignmentGap, observedRevision, p.assignmentRevision)
	}
	route, routeOwned := p.routes[shard]
	lease, hasLease := p.leases[shard]
	if !routeOwned {
		p.claimRejected.Add(1)
		return fmt.Errorf("%w: shard=%d revision=%d", ErrWorkerShardNotOwned, shard, p.assignmentRevision)
	}
	if !hasLease || !sameWorkerLeaseFence(lease, route) {
		p.claimRejected.Add(1)
		return fmt.Errorf("%w: shard=%d revision=%d", ErrWorkerAuthorityFenceMismatch, shard, p.assignmentRevision)
	}
	if lease.LeaseUntilMs <= p.now().Add(p.margin).UnixMilli() {
		p.claimRejected.Add(1)
		return fmt.Errorf("%w: %w: shard=%d revision=%d", ErrWorkerShardNotOwned, ErrWorkerAuthorityLeaseExpired, shard, p.assignmentRevision)
	}
	return nil
}

// ObserveAuthorityLease installs the exact fence returned by a successful
// Redis RenewShardAuthority call. It is the only write path for delegated
// liveness state.
func (p *RouteWorkerShardAssignmentProvider) ObserveAuthorityLease(lease WorkerShardLeaseView) error {
	if p == nil {
		return ErrWorkerAssignmentUnavailable
	}
	if lease.HolderID == "" || lease.HolderBootID == "" || lease.AssignmentRevision == 0 ||
		lease.WriterEpoch == 0 || lease.LeaseEpoch == 0 || lease.LeaseUntilMs <= 0 {
		return fmt.Errorf("%w: invalid authority lease shard=%d", ErrWorkerAuthorityFenceMismatch, lease.Shard)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.ready {
		return ErrWorkerAssignmentUnavailable
	}
	route, ok := p.routes[lease.Shard]
	if !ok || !sameWorkerLeaseFence(lease, route) {
		return fmt.Errorf("%w: shard=%d", ErrWorkerAuthorityFenceMismatch, lease.Shard)
	}
	if previous, exists := p.leases[lease.Shard]; exists && lease.LeaseUntilMs < previous.LeaseUntilMs {
		return fmt.Errorf("%w: shard=%d previous=%d current=%d", ErrWorkerAuthorityLeaseRollback, lease.Shard, previous.LeaseUntilMs, lease.LeaseUntilMs)
	}
	p.leases[lease.Shard] = lease
	return nil
}

func (p *RouteWorkerShardAssignmentProvider) Stats() RouteWorkerAssignmentStats {
	if p == nil {
		return RouteWorkerAssignmentStats{}
	}
	assignment, err := p.Assignment(context.Background())
	revision, ready := p.currentRevision()
	owned := uint64(0)
	cached := uint64(0)
	if err == nil {
		owned = uint64(len(assignment.OwnedShards))
	}
	p.mu.RLock()
	cached = uint64(len(p.leases))
	p.mu.RUnlock()
	p.mu.RLock()
	assignmentRevision := p.assignmentRevision
	p.mu.RUnlock()
	return RouteWorkerAssignmentStats{Revision: revision, AssignmentRevision: assignmentRevision, Ready: ready, OwnedShards: owned, LeaseCached: cached,
		WatchGaps: p.watchGaps.Load(), FullReloads: p.fullReloads.Load(), ClaimRejected: p.claimRejected.Load()}
}

func sameWorkerRouteSet(left, right map[uint32]WorkerShardRouteView) bool {
	if len(left) != len(right) {
		return false
	}
	for shard, route := range left {
		other, ok := right[shard]
		if !ok || !sameWorkerRouteFence(route, other) {
			return false
		}
	}
	return true
}

func sameWorkerRouteFence(left, right WorkerShardRouteView) bool {
	return left.Shard == right.Shard && left.HolderID == right.HolderID &&
		left.HolderBootID == right.HolderBootID && left.AssignmentRevision == right.AssignmentRevision &&
		left.WriterEpoch == right.WriterEpoch && left.LeaseEpoch == right.LeaseEpoch
}

func sameWorkerLeaseFence(lease WorkerShardLeaseView, route WorkerShardRouteView) bool {
	return lease.Shard == route.Shard && lease.HolderID == route.HolderID &&
		lease.HolderBootID == route.HolderBootID && lease.AssignmentRevision == route.AssignmentRevision &&
		lease.WriterEpoch == route.WriterEpoch && lease.LeaseEpoch == route.LeaseEpoch
}

func normalizeOwnedShards(shards []uint32, allowEmpty bool) ([]uint32, error) {
	if len(shards) == 0 {
		if allowEmpty {
			return []uint32{}, nil
		}
		return nil, fmt.Errorf("%w: owned shards must be non-empty", ErrInvalidConfig)
	}
	if len(shards) > maxBucketCount {
		return nil, fmt.Errorf("%w: owned shards must be <= %d", ErrInvalidConfig, maxBucketCount)
	}
	seen := make(map[uint32]struct{}, len(shards))
	owned := make([]uint32, 0, len(shards))
	for _, shard := range shards {
		if _, exists := seen[shard]; exists {
			return nil, fmt.Errorf("%w: duplicate owned shard %d", ErrInvalidConfig, shard)
		}
		seen[shard] = struct{}{}
		owned = append(owned, shard)
	}
	sort.Slice(owned, func(i, j int) bool { return owned[i] < owned[j] })
	return owned, nil
}

func configuredShardsFromBuckets(buckets []rediskey.AuthorityBucketRef) []uint32 {
	shards := make([]uint32, len(buckets))
	for i, bucket := range buckets {
		shards[i] = bucket.VirtualShard()
	}
	return shards
}
