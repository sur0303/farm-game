package routecontrol

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
)

var (
	ErrSnapshotHubClosed            = errors.New("routecontrol: snapshot watch hub is closed")
	ErrSnapshotWatchClientCapacity  = errors.New("routecontrol: snapshot watch client capacity exceeded")
	ErrSnapshotWatchSlowConsumer    = errors.New("routecontrol: snapshot watch slow consumer disconnected")
	ErrSnapshotWatchContextRequired = errors.New("routecontrol: snapshot watch context is required")
	ErrSnapshotDurableMissing       = errors.New("routecontrol: durable snapshot disappeared")
	ErrSnapshotDurableRollback      = errors.New("routecontrol: durable snapshot revision rolled back")
	ErrSnapshotDurableConflict      = errors.New("routecontrol: durable snapshot revision conflict")
	ErrSnapshotDurableContract      = errors.New("routecontrol: durable snapshot contract violation")
)

const (
	defaultSnapshotWatchClients      = 128
	defaultSnapshotWatchQueue        = 8
	defaultWatchSnapshotBytes        = 4 << 20
	defaultSnapshotReconcileInterval = 250 * time.Millisecond
	defaultSnapshotReconcileTimeout  = 2 * time.Second
	slowConsumerDrainTimeout         = 100 * time.Millisecond
)

// SnapshotWatchHubOptions bounds process-local fanout. The hub never keeps
// revision history: a subscriber receives at most the current durable
// snapshot plus snapshots that arrive while it remains within its queue.
type SnapshotWatchHubOptions struct {
	MaxClients        int
	QueueSize         int
	MaxSnapshotBytes  int
	ReconcileInterval time.Duration
	ReconcileTimeout  time.Duration
}

type SnapshotWatchOptions = SnapshotWatchHubOptions

type normalizedSnapshotWatchOptions struct {
	maxClients        int
	queueSize         int
	maxSnapshotBytes  int
	reconcileInterval time.Duration
	reconcileTimeout  time.Duration
}

func (o SnapshotWatchHubOptions) normalize() normalizedSnapshotWatchOptions {
	result := normalizedSnapshotWatchOptions{
		maxClients:        o.MaxClients,
		queueSize:         o.QueueSize,
		maxSnapshotBytes:  o.MaxSnapshotBytes,
		reconcileInterval: o.ReconcileInterval,
		reconcileTimeout:  o.ReconcileTimeout,
	}
	if result.maxClients <= 0 {
		result.maxClients = defaultSnapshotWatchClients
	}
	if result.queueSize <= 0 {
		result.queueSize = defaultSnapshotWatchQueue
	}
	if result.maxSnapshotBytes <= 0 {
		result.maxSnapshotBytes = defaultWatchSnapshotBytes
	}
	if result.reconcileInterval <= 0 {
		result.reconcileInterval = defaultSnapshotReconcileInterval
	}
	if result.reconcileTimeout <= 0 {
		result.reconcileTimeout = defaultSnapshotReconcileTimeout
	}
	return result
}

// SnapshotWatchHub is the process-local read/watch projection of a durable
// RouteSnapshotStore. Store reads are serialized independently from the
// in-memory state mutex, so Close, cancellation cleanup, and diagnostics never
// wait behind Redis.
type SnapshotWatchHub struct {
	store   RouteSnapshotStore
	options normalizedSnapshotWatchOptions

	mu          sync.Mutex
	closed      bool
	terminalErr error
	current     StoredRouteSnapshot
	currentRev  *snapshotRevision
	found       bool
	nextID      uint64
	clients     map[uint64]*SnapshotSubscription

	reconcileGate   chan struct{}
	reconcileCtx    context.Context
	reconcileCancel context.CancelFunc
	reconcileDone   chan struct{}
}

// snapshotRevision is immutable inside the hub. Subscribers only receive a
// deep clone created by their dispatcher, never this shared value.
type snapshotRevision struct {
	stored StoredRouteSnapshot
}

// SnapshotSubscription is one bounded full-snapshot stream. The internal
// queue stores shared immutable revision pointers; the public Updates channel
// receives an owned deep clone at consumption time.
type SnapshotSubscription struct {
	Updates <-chan StoredRouteSnapshot
	Done    <-chan struct{}

	hub   *SnapshotWatchHub
	id    uint64
	queue chan *snapshotRevision
	done  chan struct{}

	updates      chan StoredRouteSnapshot
	dispatchDone chan struct{}
	initial      *snapshotRevision

	stateMu sync.RWMutex
	err     error
	closed  bool
	drain   bool
}

// NewSnapshotWatchHub constructs a hub and synchronously reconciles its
// starting projection from the durable store using a background context.
func NewSnapshotWatchHub(store RouteSnapshotStore, options ...SnapshotWatchHubOptions) (*SnapshotWatchHub, error) {
	var opts SnapshotWatchHubOptions
	if len(options) > 0 {
		opts = options[0]
	}
	return NewSnapshotWatchHubWithContext(context.Background(), store, opts)
}

// NewSnapshotWatchHubWithContext is the cancellation-aware constructor used
// by compositions that already have a bounded startup context.
func NewSnapshotWatchHubWithContext(ctx context.Context, store RouteSnapshotStore, options SnapshotWatchHubOptions) (*SnapshotWatchHub, error) {
	if ctx == nil {
		return nil, ErrSnapshotWatchContextRequired
	}
	if store == nil {
		return nil, fmt.Errorf("%w: durable store required", ErrSnapshotCASConflict)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reconcileCtx, reconcileCancel := context.WithCancel(context.Background())
	hub := &SnapshotWatchHub{
		store:           store,
		options:         options.normalize(),
		clients:         make(map[uint64]*SnapshotSubscription),
		reconcileGate:   make(chan struct{}, 1),
		reconcileCtx:    reconcileCtx,
		reconcileCancel: reconcileCancel,
		reconcileDone:   make(chan struct{}),
	}
	if err := hub.reconcile(ctx, nil); err != nil {
		reconcileCancel()
		return nil, err
	}
	go hub.reconcileLoop()
	return hub, nil
}

// Subscribe atomically reconciles the durable current value and registers a
// bounded client. If afterRevision is behind the durable current revision,
// that complete current envelope is queued before registration is published.
func (h *SnapshotWatchHub) Subscribe(ctx context.Context, afterRevision uint64) (*SnapshotSubscription, error) {
	if h == nil {
		return nil, ErrSnapshotHubClosed
	}
	if ctx == nil {
		return nil, ErrSnapshotWatchContextRequired
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := h.reconcile(ctx, nil); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.stateErrorLocked(); err != nil {
		return nil, err
	}
	if len(h.clients) >= h.options.maxClients {
		return nil, ErrSnapshotWatchClientCapacity
	}
	h.nextID++
	var initial *snapshotRevision
	if h.found && afterRevision < h.current.Revision {
		initial = h.currentRev
	}
	sub := newSnapshotSubscription(h, h.nextID, h.options.queueSize, initial)
	h.clients[sub.id] = sub
	go sub.dispatch()
	go h.watchSubscriptionContext(ctx, sub)
	return sub, nil
}

// WatchFrom is an alias for callers using the transport-neutral source
// vocabulary.
func (h *SnapshotWatchHub) WatchFrom(ctx context.Context, afterRevision uint64) (*SnapshotSubscription, error) {
	return h.Subscribe(ctx, afterRevision)
}

// SubscribeChannel is a small compatibility helper for code that only needs a
// receive channel and an explicit cleanup function.
func (h *SnapshotWatchHub) SubscribeChannel(ctx context.Context, afterRevision uint64) (<-chan StoredRouteSnapshot, func(), error) {
	sub, err := h.Subscribe(ctx, afterRevision)
	if err != nil {
		return nil, nil, err
	}
	return sub.Updates, sub.Close, nil
}

// Current re-reads the durable store before returning. It therefore also
// repairs the read side after a committed store write whose notification was
// lost before the Hub was called.
func (h *SnapshotWatchHub) Current(ctx context.Context) (StoredRouteSnapshot, bool, error) {
	if h == nil {
		return StoredRouteSnapshot{}, false, ErrSnapshotHubClosed
	}
	if ctx == nil {
		return StoredRouteSnapshot{}, false, ErrSnapshotWatchContextRequired
	}
	if err := ctx.Err(); err != nil {
		return StoredRouteSnapshot{}, false, err
	}
	if err := h.reconcile(ctx, nil); err != nil {
		return StoredRouteSnapshot{}, false, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.stateErrorLocked(); err != nil {
		return StoredRouteSnapshot{}, false, err
	}
	if !h.found {
		return StoredRouteSnapshot{}, false, nil
	}
	return h.current.Clone(), true, nil
}

// Notify implements the ordinary store-before-watch capability. The incoming
// snapshot is only a claim: the durable store is re-read and the complete
// envelope must exact-match before any client can observe it.
func (h *SnapshotWatchHub) Notify(ctx context.Context, snapshot actorrouting.RouteSnapshot) error {
	return h.notify(ctx, snapshot)
}

// AuthorityNotify is intentionally a distinct capability method used only by
// the manager-owned publisher path.
func (h *SnapshotWatchHub) AuthorityNotify(ctx context.Context, snapshot actorrouting.RouteSnapshot) error {
	return h.notify(ctx, snapshot)
}

// Notifier returns an ordinary-only capability handle. Callers that do not
// need authority publication should retain this narrower interface.
func (h *SnapshotWatchHub) Notifier() SnapshotNotifier {
	return snapshotHubNotifier{hub: h}
}

// AuthorityNotifier returns the manager-only capability handle.
func (h *SnapshotWatchHub) AuthorityNotifier() AuthoritySnapshotNotifier {
	return authoritySnapshotHubNotifier{hub: h}
}

// ClientCount is diagnostic only and never contacts the durable store.
func (h *SnapshotWatchHub) ClientCount() int {
	if h == nil {
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

// Close closes all current subscriptions and prevents new registration. It
// cancels periodic reconciliation without waiting for an in-flight store read.
func (h *SnapshotWatchHub) Close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	cancel := h.reconcileCancel
	h.reconcileCancel = nil
	ids := h.clientIDsLocked()
	for _, id := range ids {
		h.closeSubscriptionLocked(h.clients[id], ErrSnapshotHubClosed)
	}
	h.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

func (h *SnapshotWatchHub) notify(ctx context.Context, snapshot actorrouting.RouteSnapshot) error {
	if h == nil {
		return ErrSnapshotHubClosed
	}
	if ctx == nil {
		return ErrSnapshotWatchContextRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	candidate, err := CanonicalizeRouteSnapshot(snapshot)
	if err != nil {
		return err
	}
	if len(candidate.CanonicalJSON) > h.options.maxSnapshotBytes {
		return fmt.Errorf("%w: snapshot bytes=%d limit=%d", ErrSnapshotCapacity, len(candidate.CanonicalJSON), h.options.maxSnapshotBytes)
	}
	if err := h.reconcile(ctx, &candidate); err != nil {
		if errors.Is(err, ErrSnapshotDurableMissing) ||
			errors.Is(err, ErrSnapshotDurableRollback) ||
			errors.Is(err, ErrSnapshotDurableConflict) ||
			errors.Is(err, ErrSnapshotDurableContract) {
			return err
		}
		return fmt.Errorf("%w: durable store reread: %v", ErrSnapshotNotify, err)
	}
	return nil
}

func (h *SnapshotWatchHub) reconcile(ctx context.Context, candidate *StoredRouteSnapshot) error {
	if ctx == nil {
		return ErrSnapshotWatchContextRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case h.reconcileGate <- struct{}{}:
		defer func() { <-h.reconcileGate }()
	case <-ctx.Done():
		return ctx.Err()
	case <-h.reconcileCtx.Done():
		return ErrSnapshotHubClosed
	}
	if err := h.stateError(); err != nil {
		return err
	}
	stored, found, err := h.store.LoadCurrent(ctx)
	if err != nil {
		return err
	}
	if err := h.applyLoaded(stored, found, candidate); err != nil {
		return err
	}
	return nil
}

func (h *SnapshotWatchHub) applyLoaded(stored StoredRouteSnapshot, found bool, candidate *StoredRouteSnapshot) error {
	if found {
		if err := validateStoredRouteSnapshot(stored); err != nil {
			return h.failClosed(fmt.Errorf("%w: %v", ErrSnapshotDurableContract, err))
		}
		if len(stored.CanonicalJSON) > h.options.maxSnapshotBytes {
			return h.failClosed(fmt.Errorf("%w: %v", ErrSnapshotDurableContract,
				fmt.Errorf("%w: snapshot bytes=%d limit=%d", ErrSnapshotCapacity, len(stored.CanonicalJSON), h.options.maxSnapshotBytes)))
		}
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.stateErrorLocked(); err != nil {
		return err
	}
	if !found {
		if h.found {
			return h.failClosedLocked(ErrSnapshotDurableMissing)
		}
		if candidate != nil {
			return fmt.Errorf("%w: durable store is empty", ErrSnapshotNotify)
		}
		return nil
	}
	if candidate != nil &&
		(!stored.EnvelopeEqual(*candidate) || !reflect.DeepEqual(stored.Snapshot, candidate.Snapshot)) {
		return fmt.Errorf("%w: notification does not exact-match durable store", ErrSnapshotNotify)
	}
	if h.found {
		if err := validateDurableSuccessor(h.current, stored); err != nil {
			return h.failClosedLocked(err)
		}
	}
	changed := !h.found ||
		!h.current.EnvelopeEqual(stored) ||
		!reflect.DeepEqual(h.current.Snapshot, stored.Snapshot)
	h.current = stored.Clone()
	h.found = true
	if changed {
		h.currentRev = newSnapshotRevision(h.current)
		h.notifyLocked(h.currentRev)
	}
	return nil
}

func validateDurableSuccessor(current, next StoredRouteSnapshot) error {
	switch {
	case next.Revision < current.Revision:
		return fmt.Errorf("%w: current=%d next=%d", ErrSnapshotDurableRollback, current.Revision, next.Revision)
	case next.Revision == current.Revision:
		if next.EnvelopeEqual(current) && reflect.DeepEqual(next.Snapshot, current.Snapshot) {
			return nil
		}
		return fmt.Errorf("%w: revision=%d", ErrSnapshotDurableConflict, next.Revision)
	}
	if current.SchemaVersion != next.SchemaVersion ||
		current.Realm != next.Realm ||
		current.TopologyFingerprint != next.TopologyFingerprint {
		return fmt.Errorf("%w: %w: topology/layout changed current=%q next=%q",
			ErrSnapshotDurableContract, actorrouting.ErrHashMigrationRequired,
			current.TopologyFingerprint, next.TopologyFingerprint)
	}
	// The durable store is the published source of truth. Full envelope and
	// topology/layout checks above are sufficient here; RT2B's Pump owns the
	// per-shard generation and transition validation.
	return nil
}

func (h *SnapshotWatchHub) failClosed(err error) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.failClosedLocked(err)
}

func (h *SnapshotWatchHub) failClosedLocked(err error) error {
	if h.terminalErr == nil {
		h.terminalErr = err
		ids := h.clientIDsLocked()
		for _, id := range ids {
			h.closeSubscriptionLocked(h.clients[id], err)
		}
	}
	return h.terminalErr
}

func (h *SnapshotWatchHub) stateError() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.stateErrorLocked()
}

func (h *SnapshotWatchHub) stateErrorLocked() error {
	if h.closed {
		return ErrSnapshotHubClosed
	}
	if h.terminalErr != nil {
		return h.terminalErr
	}
	return nil
}

func (h *SnapshotWatchHub) reconcileLoop() {
	defer close(h.reconcileDone)
	ticker := time.NewTicker(h.options.reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-h.reconcileCtx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(h.reconcileCtx, h.options.reconcileTimeout)
			_ = h.reconcile(ctx, nil)
			cancel()
		}
	}
}

func (h *SnapshotWatchHub) notifyLocked(revision *snapshotRevision) {
	for _, id := range h.clientIDsLocked() {
		sub := h.clients[id]
		select {
		case sub.queue <- revision:
		default:
			h.closeSubscriptionLocked(sub, ErrSnapshotWatchSlowConsumer)
		}
	}
}

func (h *SnapshotWatchHub) clientIDsLocked() []uint64 {
	ids := make([]uint64, 0, len(h.clients))
	for id := range h.clients {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (h *SnapshotWatchHub) watchSubscriptionContext(ctx context.Context, sub *SnapshotSubscription) {
	select {
	case <-ctx.Done():
		h.removeSubscription(sub, ctx.Err())
	case <-sub.Done:
	}
}

func (h *SnapshotWatchHub) removeSubscription(sub *SnapshotSubscription, reason error) {
	if h == nil || sub == nil {
		return
	}
	h.mu.Lock()
	if current, ok := h.clients[sub.id]; ok && current == sub {
		h.closeSubscriptionLocked(sub, reason)
	}
	h.mu.Unlock()
}

func (h *SnapshotWatchHub) closeSubscriptionLocked(sub *SnapshotSubscription, reason error) {
	if sub == nil {
		return
	}
	delete(h.clients, sub.id)
	sub.stateMu.Lock()
	if sub.closed {
		sub.stateMu.Unlock()
		return
	}
	sub.closed = true
	sub.err = reason
	sub.drain = errors.Is(reason, ErrSnapshotWatchSlowConsumer)
	sub.stateMu.Unlock()
	close(sub.done)
}

func newSnapshotRevision(stored StoredRouteSnapshot) *snapshotRevision {
	return &snapshotRevision{stored: stored.Clone()}
}

func (r *snapshotRevision) clone() StoredRouteSnapshot {
	if r == nil {
		return StoredRouteSnapshot{}
	}
	return r.stored.Clone()
}

func newSnapshotSubscription(h *SnapshotWatchHub, id uint64, queueSize int, initial *snapshotRevision) *SnapshotSubscription {
	queueCapacity := queueSize - 1
	if queueCapacity < 0 {
		queueCapacity = 0
	}
	queue := make(chan *snapshotRevision, queueCapacity)
	done := make(chan struct{})
	updates := make(chan StoredRouteSnapshot)
	return &SnapshotSubscription{
		Updates:      updates,
		Done:         done,
		hub:          h,
		id:           id,
		queue:        queue,
		done:         done,
		updates:      updates,
		dispatchDone: make(chan struct{}),
		initial:      initial,
	}
}

func (s *SnapshotSubscription) dispatch() {
	defer close(s.dispatchDone)
	defer close(s.updates)
	pending := s.initial
	s.initial = nil
	draining := false
	for {
		if pending != nil {
			snapshot := pending.clone()
			if draining {
				timer := time.NewTimer(slowConsumerDrainTimeout)
				select {
				case s.updates <- snapshot:
					timer.Stop()
					pending = nil
				case <-timer.C:
					return
				}
				continue
			}
			select {
			case s.updates <- snapshot:
				pending = nil
			case <-s.done:
				if !s.shouldDrain() {
					return
				}
				draining = true
			}
			continue
		}
		if draining {
			select {
			case pending = <-s.queue:
				if pending == nil {
					return
				}
			default:
				return
			}
			continue
		}
		select {
		case <-s.done:
			if !s.shouldDrain() {
				return
			}
			draining = true
		case revision := <-s.queue:
			if revision == nil {
				return
			}
			pending = revision
		}
	}
}

func (s *SnapshotSubscription) shouldDrain() bool {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.drain
}

// Close removes the subscription from its hub. It is safe to call more than
// once, including after the Hub already closed it.
func (s *SnapshotSubscription) Close() {
	if s == nil || s.hub == nil {
		return
	}
	s.hub.removeSubscription(s, ErrSnapshotHubClosed)
}

// Err reports why the subscription closed. A nil error means it is still
// active or was closed by a consumer that supplied no reason.
func (s *SnapshotSubscription) Err() error {
	if s == nil {
		return ErrSnapshotHubClosed
	}
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.err
}

type snapshotHubNotifier struct {
	hub *SnapshotWatchHub
}

func (n snapshotHubNotifier) Notify(ctx context.Context, snapshot actorrouting.RouteSnapshot) error {
	return n.hub.Notify(ctx, snapshot)
}

type authoritySnapshotHubNotifier struct {
	hub *SnapshotWatchHub
}

func (n authoritySnapshotHubNotifier) AuthorityNotify(ctx context.Context, snapshot actorrouting.RouteSnapshot) error {
	return n.hub.AuthorityNotify(ctx, snapshot)
}
