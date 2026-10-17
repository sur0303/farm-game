package actorrouting

import (
	"context"
	"farmgame/internal/controlfence"
	"fmt"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// AssignmentUpdate is one complete, immutable RouteSnapshot update. The
// Revision field is redundant by design: it makes a malformed producer update
// detectable before it can enter the resolver.
type AssignmentUpdate struct {
	Revision uint64
	Snapshot RouteSnapshot
}

// RevisionedAssignmentProvider is the control-plane watch extension. The
// legacy AssignmentProvider interface remains unchanged so static deployments
// and existing callers continue to compile; new providers must implement
// WatchFrom and are consumed by SnapshotApplier.Run through this interface.
type RevisionedAssignmentProvider interface {
	GetSnapshot(context.Context) (RouteSnapshot, error)
	WatchFrom(context.Context, uint64) (<-chan AssignmentUpdate, error)
}

// RemoteAssignmentInstaller is the Pump-only capability for a remote,
// complete-snapshot consumer. It deliberately has no read or publication
// methods, and it is never derived from an AssignmentProvider.
type RemoteAssignmentInstaller interface {
	InstallFullSnapshot(context.Context, RouteSnapshot) error
}

// AssignmentProvider is the control-plane source of RouteSnapshot tables.
// Static is implemented in this batch; Discovery (etcd/HTTP) is deferred.
type AssignmentProvider interface {
	// GetSnapshot returns a full validated RouteSnapshot (or an error).
	GetSnapshot(ctx context.Context) (RouteSnapshot, error)
	// Watch returns a channel of incremental/full snapshots.
	// StaticAssignmentProvider never pushes; the channel closes when ctx is done.
	Watch(ctx context.Context) (<-chan RouteSnapshot, error)
}

// StaticAssignmentProvider serves a fixed RouteSnapshot loaded at construction.
// It does not push updates on Watch — use Replace on StaticResolver / SnapshotApplier
// for test-driven or out-of-band table swaps.
type StaticAssignmentProvider struct {
	snapshot RouteSnapshot
}

// NewStaticAssignmentProvider validates HashVersion / ShardCount / Routes length /
// HolderIndex (fail-closed) then stores the snapshot.
func NewStaticAssignmentProvider(snapshot RouteSnapshot) (*StaticAssignmentProvider, error) {
	if err := validateRouteSnapshot(snapshot); err != nil {
		return nil, err
	}
	return &StaticAssignmentProvider{snapshot: cloneRouteSnapshot(snapshot)}, nil
}

// LoadStaticAssignmentProvider reads the v2 RouteSnapshot JSON and builds a
// validated provider. Legacy flat shapes return ErrHashMigrationRequired.
func LoadStaticAssignmentProvider(path string) (*StaticAssignmentProvider, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	snapshot, err := ParseRouteSnapshotJSON(data)
	if err != nil {
		return nil, err
	}
	return NewStaticAssignmentProvider(snapshot)
}

// GetSnapshot returns a deep copy so callers cannot mutate the provider's
// immutable control-plane snapshot through nested slices.
func (p *StaticAssignmentProvider) GetSnapshot(ctx context.Context) (RouteSnapshot, error) {
	if p == nil {
		return RouteSnapshot{}, fmt.Errorf("%w: nil provider", ErrInvalidSnapshot)
	}
	if ctx == nil {
		return RouteSnapshot{}, fmt.Errorf("%w: nil context", ErrInvalidSnapshot)
	}
	if err := ctx.Err(); err != nil {
		return RouteSnapshot{}, err
	}
	snapshot := cloneRouteSnapshot(p.snapshot)
	if err := ctx.Err(); err != nil {
		return RouteSnapshot{}, err
	}
	return snapshot, nil
}

// Watch never emits snapshots (static has no push path). The returned channel
// is closed when ctx is cancelled or the deadline fires.
func (p *StaticAssignmentProvider) Watch(ctx context.Context) (<-chan RouteSnapshot, error) {
	if p == nil {
		return nil, fmt.Errorf("%w: nil provider", ErrInvalidSnapshot)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidSnapshot)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ch := make(chan RouteSnapshot)
	go func() {
		defer close(ch)
		<-ctx.Done()
	}()
	return ch, nil
}

// WatchFrom gives the revisioned shape to static sources without inventing a
// change stream. A caller behind the fixed snapshot receives one full reload;
// a caller at the current revision waits for cancellation.
func (p *StaticAssignmentProvider) WatchFrom(ctx context.Context, afterRevision uint64) (<-chan AssignmentUpdate, error) {
	if p == nil {
		return nil, fmt.Errorf("%w: nil provider", ErrInvalidSnapshot)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidSnapshot)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ch := make(chan AssignmentUpdate, 1)
	if afterRevision < p.snapshot.GlobalRevision {
		update := AssignmentUpdate{Revision: p.snapshot.GlobalRevision, Snapshot: cloneRouteSnapshot(p.snapshot)}
		select {
		case ch <- update:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	go func() {
		defer close(ch)
		<-ctx.Done()
	}()
	return ch, nil
}

// DynamicAssignmentProvider is the read/watch half of the process-local
// full-snapshot assignment source. Its method set deliberately contains no
// publication operation, so a data-plane consumer cannot recover write
// authority from this handle.
type DynamicAssignmentProvider struct {
	state *dynamicAssignmentState
}

// DynamicAssignmentPublisher is the separately-owned write half created with
// a DynamicAssignmentProvider. Composition must give this handle only to the
// RouteAssignmentManager. The hidden shared state cannot be reached through
// the provider's public API.
type DynamicAssignmentPublisher struct {
	state *dynamicAssignmentState
}

type dynamicAssignmentState struct {
	mu             sync.Mutex
	snapshot       RouteSnapshot
	initialized    bool
	nextID         uint64
	buffer         int
	watches        map[uint64]*assignmentWatch
	watcherCleanup atomic.Uint64
}

type assignmentWatch struct {
	ch        chan AssignmentUpdate
	done      chan struct{}
	closeOnce sync.Once
}

func (w *assignmentWatch) close() {
	if w == nil {
		return
	}
	w.closeOnce.Do(func() {
		close(w.ch)
		close(w.done)
	})
}

// NewDynamicAssignmentProvider validates and installs the initial complete
// snapshot and returns distinct read and write capabilities. buffer is the
// per-subscriber bounded queue; non-positive values use a conservative
// default. There is intentionally no API that derives the publisher from the
// provider after construction.
func NewDynamicAssignmentProvider(initial RouteSnapshot, buffer int) (*DynamicAssignmentProvider, *DynamicAssignmentPublisher, error) {
	if err := validateRouteSnapshot(initial); err != nil {
		return nil, nil, err
	}
	if buffer <= 0 {
		buffer = 16
	}
	state := &dynamicAssignmentState{
		snapshot:    cloneRouteSnapshot(initial),
		initialized: true,
		buffer:      buffer,
		watches:     make(map[uint64]*assignmentWatch),
	}
	return &DynamicAssignmentProvider{state: state}, &DynamicAssignmentPublisher{state: state}, nil
}

// NewRemoteAssignmentProvider creates a fail-closed read provider and its
// Pump-only complete-snapshot installer. Until the installer accepts the first
// full snapshot, GetSnapshot and WatchFrom return ErrNoAssignment and the
// provider has no usable local table.
//
// The variadic buffer keeps the constructor explicit while allowing the
// conservative default to be selected without a caller-side sentinel.
func NewRemoteAssignmentProvider(buffer ...int) (*DynamicAssignmentProvider, RemoteAssignmentInstaller, error) {
	queueSize := 16
	if len(buffer) > 0 && buffer[0] > 0 {
		queueSize = buffer[0]
	}
	state := &dynamicAssignmentState{
		buffer:  queueSize,
		watches: make(map[uint64]*assignmentWatch),
	}
	return &DynamicAssignmentProvider{state: state}, &remoteAssignmentInstaller{state: state}, nil
}

// GetSnapshot returns a deep copy of the latest complete snapshot.
func (p *DynamicAssignmentProvider) GetSnapshot(ctx context.Context) (RouteSnapshot, error) {
	if p == nil || p.state == nil {
		return RouteSnapshot{}, fmt.Errorf("%w: nil provider", ErrInvalidSnapshot)
	}
	if ctx == nil {
		return RouteSnapshot{}, fmt.Errorf("%w: nil context", ErrInvalidSnapshot)
	}
	if err := ctx.Err(); err != nil {
		return RouteSnapshot{}, err
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return RouteSnapshot{}, err
	}
	if !p.state.initialized {
		return RouteSnapshot{}, ErrNoAssignment
	}
	snapshot := cloneRouteSnapshot(p.state.snapshot)
	if err := ctx.Err(); err != nil {
		return RouteSnapshot{}, err
	}
	return snapshot, nil
}

// CurrentSnapshot is the synchronous local view used by hot-path adapters.
// It never contacts an authority store or network.
func (p *DynamicAssignmentProvider) CurrentSnapshot() RouteSnapshot {
	if p == nil || p.state == nil {
		return RouteSnapshot{}
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	if !p.state.initialized {
		return RouteSnapshot{}
	}
	return cloneRouteSnapshot(p.state.snapshot)
}

// CurrentRevision returns the revision of the latest complete local snapshot
// without cloning its route and holder tables. A zero revision means that the
// provider is nil or has not installed its first complete snapshot yet.
//
// This is intentionally a separate observation from CurrentSnapshot: callers
// may use it to avoid a full copy while polling for change, but must fetch and
// re-check a complete Snapshot before acting on a newly observed revision.
func (p *DynamicAssignmentProvider) CurrentRevision() uint64 {
	if p == nil || p.state == nil {
		return 0
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	if !p.state.initialized {
		return 0
	}
	return p.state.snapshot.GlobalRevision
}

// Snapshot implements the local RouteSnapshotSource seam used by hot-path
// authority adapters.
func (p *DynamicAssignmentProvider) Snapshot() RouteSnapshot {
	return p.CurrentSnapshot()
}

// LookupShard returns only the route and Holder needed by a data-plane fence
// check. It takes the same state lock used by CurrentSnapshot, so the route,
// Holder and snapshot revision are coherent, but it does not clone every
// domain's route table for a single Actor activation.
func (p *DynamicAssignmentProvider) LookupShard(domain RouteDomain, shard uint32) (ShardRouteSnapshot, error) {
	if p == nil || p.state == nil {
		return ShardRouteSnapshot{}, ErrNoAssignment
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	if !p.state.initialized {
		return ShardRouteSnapshot{}, ErrNoAssignment
	}
	var domainSnapshot DomainSnapshot
	switch domain {
	case DomainPlayer:
		domainSnapshot = p.state.snapshot.Player
	case DomainFarm:
		domainSnapshot = p.state.snapshot.Farm
	case DomainFarmWorker:
		domainSnapshot = p.state.snapshot.FarmWorker
	default:
		return ShardRouteSnapshot{}, ErrInvalidAddress
	}
	if shard >= domainSnapshot.ShardCount || int(shard) >= len(domainSnapshot.Routes) {
		return ShardRouteSnapshot{}, ErrInvalidSnapshot
	}
	route := domainSnapshot.Routes[shard]
	if route.HolderIndex < 0 || route.HolderIndex >= len(p.state.snapshot.Holders) {
		return ShardRouteSnapshot{}, ErrInvalidSnapshot
	}
	return ShardRouteSnapshot{
		Realm: p.state.snapshot.Realm, GlobalRevision: p.state.snapshot.GlobalRevision,
		HashVersion: p.state.snapshot.HashVersion, HashSeed: p.state.snapshot.HashSeed,
		Domain: domain, Shard: shard, ShardCount: domainSnapshot.ShardCount,
		Route: route, Holder: p.state.snapshot.Holders[route.HolderIndex],
	}, nil
}

// WatchFrom subscribes afterRevision. If the caller is behind the current
// revision, the first item is the current full snapshot; it is not a guessed
// patch. If the bounded queue fills, the subscriber is closed and the caller
// must GetSnapshot before reconnecting.
func (p *DynamicAssignmentProvider) WatchFrom(ctx context.Context, afterRevision uint64) (<-chan AssignmentUpdate, error) {
	if p == nil || p.state == nil {
		return nil, fmt.Errorf("%w: nil provider", ErrInvalidSnapshot)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidSnapshot)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	p.state.mu.Lock()
	if err := ctx.Err(); err != nil {
		p.state.mu.Unlock()
		return nil, err
	}
	if !p.state.initialized {
		p.state.mu.Unlock()
		return nil, ErrNoAssignment
	}
	p.state.nextID++
	id := p.state.nextID
	watch := &assignmentWatch{ch: make(chan AssignmentUpdate, p.state.buffer), done: make(chan struct{})}
	current := cloneRouteSnapshot(p.state.snapshot)
	if afterRevision < current.GlobalRevision {
		select {
		case watch.ch <- AssignmentUpdate{Revision: current.GlobalRevision, Snapshot: current}:
		case <-ctx.Done():
			p.state.mu.Unlock()
			watch.close()
			return nil, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		p.state.mu.Unlock()
		watch.close()
		return nil, err
	}
	p.state.watches[id] = watch
	p.state.mu.Unlock()

	go func() {
		defer p.state.watcherCleanup.Add(1)
		select {
		case <-ctx.Done():
			p.removeWatch(id, watch)
		case <-watch.done:
			// Publish/another cleanup path already removed and closed this watch.
		}
	}()
	return watch.ch, nil
}

// Watch retains the original full-snapshot provider shape. New code should
// use WatchFrom so SnapshotApplier can reconnect from its last revision.
func (p *DynamicAssignmentProvider) Watch(ctx context.Context) (<-chan RouteSnapshot, error) {
	updates, err := p.WatchFrom(ctx, p.CurrentSnapshot().GlobalRevision)
	if err != nil {
		return nil, err
	}
	out := make(chan RouteSnapshot, 1)
	go func() {
		defer close(out)
		for update := range updates {
			select {
			case out <- cloneRouteSnapshot(update.Snapshot):
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// Publish atomically installs one ordinary complete snapshot and notifies all
// current subscribers. Same-generation activation remains forbidden here.
func (p *DynamicAssignmentPublisher) Publish(ctx context.Context, _ controlfence.NestedControlOperationToken, snapshot RouteSnapshot) error {
	return p.publish(ctx, snapshot, false, false)
}

// PublishAuthoritySnapshot is the manager-owned projection path for one
// initial placeholder or committed Drain finalization. Authority provenance is
// the possession of this separate write capability; no caller-constructible
// proof crosses into the read/watch API.
func (p *DynamicAssignmentPublisher) PublishAuthoritySnapshot(ctx context.Context, _ controlfence.NestedControlOperationToken, snapshot RouteSnapshot) error {
	return p.publish(ctx, snapshot, true, false)
}

// PublishAuthorityWaveSnapshot installs one GlobalRevision that may activate
// many already-assigned routes. Sliding Drain coalescing uses this path so
// concurrent shard moves share a snapshot CAS without freezing revision as a
// cross-shard fence.
func (p *DynamicAssignmentPublisher) PublishAuthorityWaveSnapshot(ctx context.Context, _ controlfence.NestedControlOperationToken, snapshot RouteSnapshot) error {
	return p.publish(ctx, snapshot, true, true)
}

func (p *DynamicAssignmentPublisher) publish(ctx context.Context, snapshot RouteSnapshot, authorityActivation, wave bool) error {
	if p == nil || p.state == nil {
		return fmt.Errorf("%w: nil provider", ErrInvalidSnapshot)
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidSnapshot)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateRouteSnapshot(snapshot); err != nil {
		return err
	}

	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	current := p.state.snapshot
	// A complete replay of the already-installed revision is idempotent. Do
	// this before transition-specific validation: recovery may replay a stale
	// ASSIGNING -> ACTIVE finalization after the provider already installed it.
	if snapshot.GlobalRevision == current.GlobalRevision && reflect.DeepEqual(current, snapshot) {
		return nil
	}
	if wave {
		if err := ValidateAuthorityWaveSnapshotUpdate(current, snapshot); err != nil {
			return err
		}
	} else if authorityActivation {
		if err := ValidateAuthoritySnapshotUpdate(current, snapshot); err != nil {
			return err
		}
	} else {
		if err := ValidateRouteSnapshotUpdate(current, snapshot); err != nil {
			return err
		}
	}
	if snapshot.GlobalRevision > current.GlobalRevision &&
		(current.GlobalRevision == ^uint64(0) || snapshot.GlobalRevision != current.GlobalRevision+1) {
		return fmt.Errorf("%w: current=%d next=%d", ErrSnapshotRevisionGap, current.GlobalRevision, snapshot.GlobalRevision)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.state.snapshot = cloneRouteSnapshot(snapshot)
	p.state.initialized = true
	p.state.notifyLocked(snapshot)
	return nil
}

type remoteAssignmentInstaller struct {
	state *dynamicAssignmentState
}

func (i *remoteAssignmentInstaller) InstallFullSnapshot(ctx context.Context, snapshot RouteSnapshot) error {
	if i == nil || i.state == nil {
		return fmt.Errorf("%w: nil installer", ErrInvalidSnapshot)
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil installer context", ErrContextRequired)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateRouteSnapshot(snapshot); err != nil {
		return err
	}

	i.state.mu.Lock()
	defer i.state.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if !i.state.initialized {
		i.state.snapshot = cloneRouteSnapshot(snapshot)
		i.state.initialized = true
		i.state.notifyLocked(snapshot)
		return nil
	}

	current := i.state.snapshot
	if snapshot.GlobalRevision == current.GlobalRevision {
		if reflect.DeepEqual(current, snapshot) {
			return nil
		}
		return ErrSnapshotRevisionConflict
	}
	if snapshot.GlobalRevision < current.GlobalRevision {
		return ErrStaleRoute
	}
	if err := validateTrustedProviderSnapshotReload(current, snapshot); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	i.state.snapshot = cloneRouteSnapshot(snapshot)
	i.state.notifyLocked(snapshot)
	return nil
}

func (s *dynamicAssignmentState) notifyLocked(snapshot RouteSnapshot) {
	for id, watch := range s.watches {
		select {
		case watch.ch <- AssignmentUpdate{Revision: snapshot.GlobalRevision, Snapshot: cloneRouteSnapshot(snapshot)}:
		default:
			// A slow consumer must not block route authority publication. Closing
			// forces its applier to GetSnapshot before it reconnects.
			delete(s.watches, id)
			watch.close()
		}
	}
}

func (p *DynamicAssignmentProvider) removeWatch(id uint64, watch *assignmentWatch) {
	if p == nil || p.state == nil {
		return
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	if current, ok := p.state.watches[id]; ok && current == watch {
		delete(p.state.watches, id)
		watch.close()
	}
}

// SnapshotApplier applies provider snapshots onto a replaceable StaticResolver.
// It tracks lastGlobalRevision and, on a revision gap (rev > last+1), triggers
// GetSnapshot for a full refresh. Atomic counters remain process-local; prometheus
// snapshot_revision/age are updated via StaticResolver.Replace → Route Metrics.
type SnapshotApplier struct {
	provider        AssignmentProvider
	resolver        *StaticResolver
	trustedProvider bool

	mu   sync.Mutex
	last uint64

	// OnApplied runs after the applier mutex is released, once a newer
	// GlobalRevision is installed. Holders use it to heartbeat immediately
	// instead of waiting for the periodic ticker.
	OnApplied func(revision uint64)

	StaleIgnored atomic.Uint64
	Duplicates   atomic.Uint64
	GapReloads   atomic.Uint64
	Reconnects   atomic.Uint64
	Applied      atomic.Uint64
}

// NewSnapshotApplier wires a provider to a resolver. Both are required.
// The resolver's current table is not overwritten until Apply / Bootstrap.
func NewSnapshotApplier(provider AssignmentProvider, resolver *StaticResolver) (*SnapshotApplier, error) {
	if provider == nil {
		return nil, fmt.Errorf("%w: provider required", ErrInvalidSnapshot)
	}
	if resolver == nil {
		return nil, fmt.Errorf("%w: resolver required", ErrInvalidSnapshot)
	}
	// Trust the exact constructor-produced read handle only. A private marker
	// interface is insufficient here because embedding DynamicAssignmentProvider
	// promotes its methods onto a wrapper that can override GetSnapshot. Requiring
	// the exact dynamic type leaves no wrapper dispatch point for forged reloads.
	dynamicProvider, trusted := provider.(*DynamicAssignmentProvider)
	trusted = trusted && dynamicProvider != nil && dynamicProvider.state != nil
	return &SnapshotApplier{provider: provider, resolver: resolver, trustedProvider: trusted}, nil
}

// LastGlobalRevision returns the last successfully applied GlobalRevision.
func (a *SnapshotApplier) LastGlobalRevision() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.last
}

// NoteAppliedRevision advances last when rev is newer. Used by RedirectApplier
// so a successful in-process redirect stays coherent with Watch/Apply ordering.
func (a *SnapshotApplier) NoteAppliedRevision(rev uint64) {
	if a == nil || rev == 0 {
		return
	}
	a.mu.Lock()
	advanced := rev > a.last
	if advanced {
		a.last = rev
	}
	onApplied := a.OnApplied
	a.mu.Unlock()
	if advanced {
		fireSnapshotApplied(onApplied, rev)
	}
}

// Bootstrap loads a full snapshot via GetSnapshot and applies it.
func (a *SnapshotApplier) Bootstrap(ctx context.Context) error {
	if a == nil {
		return ErrNoAssignment
	}
	if ctx == nil {
		return ErrContextRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	snap, err := a.provider.GetSnapshot(ctx)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return a.apply(ctx, snap, a.trustedProvider)
}

// Apply validates revision ordering then Replace-s the resolver table.
//
//	last==0          → bootstrap apply (no gap check)
//	rev < last       → ignore (stale), counter++
//	rev == last      → ignore (duplicate), counter++
//	rev == last+1    → Replace(snap)
//	rev > last+1     → gap: GetSnapshot full reload then Replace
//
// Replace validation failure leaves the old table and last revision unchanged.
func (a *SnapshotApplier) Apply(ctx context.Context, snap RouteSnapshot) error {
	return a.apply(ctx, snap, false)
}

func (a *SnapshotApplier) apply(ctx context.Context, snap RouteSnapshot, trustedProviderSnapshot bool) error {
	if a == nil {
		return ErrNoAssignment
	}
	if ctx == nil {
		return ErrContextRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateRouteSnapshot(snap); err != nil {
		return err
	}
	var appliedRev uint64
	a.mu.Lock()
	err := a.applyLocked(ctx, snap, trustedProviderSnapshot, &appliedRev)
	onApplied := a.OnApplied
	a.mu.Unlock()
	if err == nil && appliedRev > 0 {
		fireSnapshotApplied(onApplied, appliedRev)
	}
	return err
}

func (a *SnapshotApplier) applyLocked(ctx context.Context, snap RouteSnapshot, trustedProviderSnapshot bool, appliedRev *uint64) error {
	rev := snap.GlobalRevision
	last := a.last

	if last > 0 {
		switch {
		case rev < last:
			a.StaleIgnored.Add(1)
			return nil
		case rev == last:
			if !reflect.DeepEqual(a.resolver.Snapshot(), snap) {
				return ErrSnapshotRevisionConflict
			}
			a.Duplicates.Add(1)
			return nil
		case rev > last+1:
			a.GapReloads.Add(1)
			full, err := a.provider.GetSnapshot(ctx)
			if err != nil {
				return err
			}
			if full.GlobalRevision < rev || full.GlobalRevision <= last {
				return fmt.Errorf("%w: gap reload revision=%d push=%d last=%d", ErrStaleRoute, full.GlobalRevision, rev, last)
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := a.replace(ctx, full, a.trustedProvider); err != nil {
				return err
			}
			a.last = full.GlobalRevision
			a.Applied.Add(1)
			*appliedRev = a.last
			return nil
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	if err := a.replace(ctx, snap, trustedProviderSnapshot); err != nil {
		return err
	}
	a.last = rev
	a.Applied.Add(1)
	*appliedRev = rev
	return nil
}

func fireSnapshotApplied(onApplied func(uint64), revision uint64) {
	if onApplied == nil || revision == 0 {
		return
	}
	onApplied(revision)
}

func (a *SnapshotApplier) replace(ctx context.Context, snapshot RouteSnapshot, trustedProviderSnapshot bool) error {
	if trustedProviderSnapshot {
		return a.resolver.replaceTrustedProviderSnapshot(ctx, snapshot)
	}
	return a.resolver.ReplaceContext(ctx, snapshot)
}

// Run bootstraps once then consumes Watch until ctx is done or the channel closes.
// Static providers never push; Run blocks on Watch until cancellation.
func (a *SnapshotApplier) Run(ctx context.Context) error {
	if ctx == nil {
		return ErrContextRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := a.Bootstrap(ctx); err != nil {
		return err
	}
	if provider, ok := a.provider.(RevisionedAssignmentProvider); ok {
		return a.runRevisioned(ctx, provider)
	}
	ch, err := a.provider.Watch(ctx)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case snap, ok := <-ch:
			if !ok {
				return nil
			}
			if err := a.Apply(ctx, snap); err != nil {
				return err
			}
		}
	}
}

func (a *SnapshotApplier) runRevisioned(ctx context.Context, provider RevisionedAssignmentProvider) error {
	for {
		ch, err := provider.WatchFrom(ctx, a.LastGlobalRevision())
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			a.Reconnects.Add(1)
			if err := a.Bootstrap(ctx); err != nil {
				return err
			}
			if err := waitAssignmentReconnect(ctx); err != nil {
				return err
			}
			continue
		}
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case update, ok := <-ch:
				if !ok {
					a.Reconnects.Add(1)
					if err := a.Bootstrap(ctx); err != nil {
						return err
					}
					goto reconnect
				}
				if update.Revision == 0 || update.Revision != update.Snapshot.GlobalRevision {
					return fmt.Errorf("%w: update revision=%d snapshot=%d", ErrInvalidSnapshot, update.Revision, update.Snapshot.GlobalRevision)
				}
				if err := a.apply(ctx, update.Snapshot, a.trustedProvider); err != nil {
					return err
				}
			}
		}
	reconnect:
	}
}

func waitAssignmentReconnect(ctx context.Context) error {
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
