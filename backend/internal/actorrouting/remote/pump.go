// Package remote contains the transport-neutral remote route consumer.
// Transport authentication and envelope verification belong to the adapter
// that implements RemoteSnapshotSource; this package only consumes complete
// RouteSnapshot values and validates them again before local installation.
package remote

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"farmgame/internal/actorrouting"
)

// RemoteSnapshotSource is the trusted, transport-neutral source port. A
// transport adapter must authenticate and envelope-validate before returning
// a complete snapshot. This package does not implement transport security.
type RemoteSnapshotSource interface {
	FetchFullSnapshot(context.Context) (actorrouting.RouteSnapshot, error)
	WatchFrom(context.Context, uint64) (<-chan actorrouting.RouteSnapshot, error)
}

// ResyncBaseline binds a restart-aware full fetch to the exact complete
// snapshot most recently installed by the Pump. Revision is repeated
// explicitly so an adapter cannot accidentally compare a rollover response
// with a merely transport-accepted snapshot that the Pump never installed.
type ResyncBaseline struct {
	Snapshot actorrouting.RouteSnapshot
	Revision uint64
}

// RestartAwareRemoteSnapshotSource is an optional, narrow extension used only
// after an established Watch channel has closed. Existing transport-neutral
// sources remain compatible and continue to use FetchFullSnapshot for resync.
type RestartAwareRemoteSnapshotSource interface {
	FetchFullSnapshotForResync(context.Context, ResyncBaseline) (actorrouting.RouteSnapshot, error)
}

// State is the lifecycle state of a RemoteAssignmentPump.
type State string

const (
	StateBootstrapping State = "BOOTSTRAPPING"
	StateWatching      State = "WATCHING"
	StateResyncing     State = "RESYNCING"
	StateDegraded      State = "DEGRADED"
	StateClosed        State = "CLOSED"

	Bootstrapping = StateBootstrapping
	Watching      = StateWatching
	Resyncing     = StateResyncing
	Degraded      = StateDegraded
	Closed        = StateClosed
)

var (
	ErrSourceRequired         = errors.New("remote route: snapshot source required")
	ErrInstallerRequired      = errors.New("remote route: assignment installer required")
	ErrPumpRunning            = errors.New("remote route: pump is already running")
	ErrWatchUnavailable       = errors.New("remote route: watch unavailable")
	ErrWatchLivenessExhausted = errors.New("remote route: watch liveness retry budget exhausted")
	ErrResyncExhausted        = errors.New("remote route: full resync retry budget exhausted")
)

// Options controls bounded resync. Backoff, when supplied, is called with a
// one-based retry attempt and is useful for deterministic tests.
//
// ContinuousReconnect keeps an already bootstrapped Pump alive across a
// prolonged source outage. MaxResyncAttempts and MaxWatchFailures remain hard
// bounds for one retry burst and for backoff calculation; they no longer turn
// a recoverable transport outage into a permanent process-local disconnect.
// Snapshot validation and installer contract errors remain terminal.
type Options struct {
	Buffer              int
	MaxResyncAttempts   int
	MaxWatchFailures    int
	RetryBackoff        time.Duration
	MaxRetryBackoff     time.Duration
	Backoff             func(attempt int) time.Duration
	ContinuousReconnect bool
}

const (
	defaultAssignmentBuffer = 16
	defaultResyncAttempts   = 3
	defaultWatchFailures    = 3
	defaultRetryBackoff     = 10 * time.Millisecond
	defaultMaxRetryBackoff  = 100 * time.Millisecond
)

type normalizedOptions struct {
	buffer              int
	maxResyncAttempts   int
	maxWatchFailures    int
	retryBackoff        time.Duration
	maxRetryBackoff     time.Duration
	backoff             func(attempt int) time.Duration
	continuousReconnect bool
}

func (o Options) normalize() normalizedOptions {
	result := normalizedOptions{
		buffer:              o.Buffer,
		maxResyncAttempts:   o.MaxResyncAttempts,
		maxWatchFailures:    o.MaxWatchFailures,
		retryBackoff:        o.RetryBackoff,
		maxRetryBackoff:     o.MaxRetryBackoff,
		backoff:             o.Backoff,
		continuousReconnect: o.ContinuousReconnect,
	}
	if result.buffer <= 0 {
		result.buffer = defaultAssignmentBuffer
	}
	if result.maxResyncAttempts <= 0 {
		result.maxResyncAttempts = defaultResyncAttempts
	}
	if result.maxWatchFailures <= 0 {
		result.maxWatchFailures = result.maxResyncAttempts
		if result.maxWatchFailures <= 0 {
			result.maxWatchFailures = defaultWatchFailures
		}
	}
	if result.retryBackoff <= 0 {
		result.retryBackoff = defaultRetryBackoff
	}
	if result.maxRetryBackoff <= 0 {
		result.maxRetryBackoff = defaultMaxRetryBackoff
	}
	if result.maxRetryBackoff < result.retryBackoff {
		result.maxRetryBackoff = result.retryBackoff
	}
	return result
}

// RemoteAssignmentPump fetches and watches complete remote snapshots, then
// installs them through the Pump-only capability. It never performs remote
// work from the local provider's hot path.
type RemoteAssignmentPump struct {
	source    RemoteSnapshotSource
	installer actorrouting.RemoteAssignmentInstaller
	options   normalizedOptions

	stateMu     sync.RWMutex
	state       State
	lastErr     error
	revision    uint64
	initialized bool
	snapshot    actorrouting.RouteSnapshot

	installMu sync.Mutex
	runMu     sync.Mutex
	running   bool
	runCancel context.CancelFunc
	closed    chan struct{}
	closeOnce sync.Once

	metrics      atomic.Pointer[actorrouting.Metrics]
	StaleIgnored atomic.Uint64
	Duplicates   atomic.Uint64
	Installed    atomic.Uint64
	Reconnects   atomic.Uint64
	Resyncs      atomic.Uint64
}

// SetMetrics attaches route consumer metrics. Nil clears the observer.
func (p *RemoteAssignmentPump) SetMetrics(metrics *actorrouting.Metrics) {
	if p != nil {
		p.metrics.Store(metrics)
	}
}

// NewRemoteAssignmentPump constructs a pump from the two explicit trusted
// ports. The Pump cannot accept DynamicAssignmentPublisher because that type
// does not implement RemoteAssignmentInstaller.
func NewRemoteAssignmentPump(source RemoteSnapshotSource, installer actorrouting.RemoteAssignmentInstaller, options ...Options) (*RemoteAssignmentPump, error) {
	if isNil(source) {
		return nil, ErrSourceRequired
	}
	if isNil(installer) {
		return nil, ErrInstallerRequired
	}
	var opts Options
	if len(options) > 0 {
		opts = options[0]
	}
	return &RemoteAssignmentPump{
		source:    source,
		installer: installer,
		options:   opts.normalize(),
		state:     StateBootstrapping,
		closed:    make(chan struct{}),
	}, nil
}

// NewRemoteAssignmentProvider is the explicit remote composition constructor.
// It returns only the read provider and its Pump; the installer remains
// private to the Pump construction path.
func NewRemoteAssignmentProvider(source RemoteSnapshotSource, options ...Options) (*actorrouting.DynamicAssignmentProvider, *RemoteAssignmentPump, error) {
	var opts Options
	if len(options) > 0 {
		opts = options[0]
	}
	provider, installer, err := actorrouting.NewRemoteAssignmentProvider(opts.Buffer)
	if err != nil {
		return nil, nil, err
	}
	pump, err := NewRemoteAssignmentPump(source, installer, opts)
	if err != nil {
		return nil, nil, err
	}
	return provider, pump, nil
}

// NewRemoteAssignmentConsumer is a descriptive alias for callers that prefer
// the consumer terminology used by the route-control handoff.
func NewRemoteAssignmentConsumer(source RemoteSnapshotSource, options ...Options) (*actorrouting.DynamicAssignmentProvider, *RemoteAssignmentPump, error) {
	return NewRemoteAssignmentProvider(source, options...)
}

// State returns the current lifecycle state without contacting the source.
func (p *RemoteAssignmentPump) State() State {
	if p == nil {
		return StateClosed
	}
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.state
}

// LastError returns the most recent terminal or degraded error.
func (p *RemoteAssignmentPump) LastError() error {
	if p == nil {
		return nil
	}
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.lastErr
}

// CurrentRevision is the last revision successfully installed by this Pump.
func (p *RemoteAssignmentPump) CurrentRevision() uint64 {
	if p == nil {
		return 0
	}
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return p.revision
}

// Snapshot returns the Pump's local snapshot copy. It never contacts the
// remote source and is intended only for diagnostics/tests; data-plane
// resolution should use the returned DynamicAssignmentProvider.
func (p *RemoteAssignmentPump) Snapshot() actorrouting.RouteSnapshot {
	if p == nil {
		return actorrouting.RouteSnapshot{}
	}
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	if !p.initialized {
		return actorrouting.RouteSnapshot{}
	}
	return actorrouting.CloneRouteSnapshot(p.snapshot)
}

// Close cancels a blocked FetchFullSnapshot, WatchFrom, or retry backoff.
// Normal Close is not a fatal pump error and causes Run to return nil.
func (p *RemoteAssignmentPump) Close() error {
	if p == nil {
		return nil
	}
	p.closeOnce.Do(func() {
		close(p.closed)
		p.runMu.Lock()
		if p.runCancel != nil {
			p.runCancel()
		}
		p.runMu.Unlock()
		p.setState(StateClosed)
	})
	return nil
}

// Run performs the initial full fetch and then watches until closed, the
// caller's context ends, or a non-recoverable route contract error occurs.
func (p *RemoteAssignmentPump) Run(ctx context.Context) error {
	if p == nil {
		return ErrSourceRequired
	}
	if ctx == nil {
		return actorrouting.ErrContextRequired
	}

	p.runMu.Lock()
	if p.running {
		p.runMu.Unlock()
		return ErrPumpRunning
	}
	if p.isClosed() || p.State() == StateClosed {
		p.runMu.Unlock()
		p.setState(StateClosed)
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	p.running = true
	p.runCancel = cancel
	p.runMu.Unlock()

	go func() {
		select {
		case <-p.closed:
			cancel()
		case <-runCtx.Done():
		}
	}()
	defer func() {
		cancel()
		p.runMu.Lock()
		p.running = false
		p.runCancel = nil
		p.runMu.Unlock()
	}()

	p.setState(StateBootstrapping)
	err := p.runLoop(runCtx)
	if p.isClosed() {
		p.setState(StateClosed)
		return nil
	}
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		p.setState(StateClosed)
		return err
	}
	if p.fail(err) {
		return nil
	}
	return err
}

func (p *RemoteAssignmentPump) runLoop(ctx context.Context) error {
	snapshot, err := p.source.FetchFullSnapshot(ctx)
	if err != nil {
		return err
	}
	if err := p.install(ctx, snapshot); err != nil {
		return err
	}
	p.setState(StateWatching)
	watchFailures := 0

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		watch, err := p.source.WatchFrom(ctx, p.CurrentRevision())
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := p.recoverWatchLiveness(ctx, &watchFailures, false); err != nil {
				return err
			}
			continue
		}
		if watch == nil {
			if err := p.recoverWatchLiveness(ctx, &watchFailures, false); err != nil {
				return err
			}
			continue
		}
		p.setState(StateWatching)
		err, progressed := p.consumeWatch(ctx, watch)
		if progressed {
			watchFailures = 0
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Only an exhausted watch channel is a transport/liveness event.
			// Snapshot contract errors are terminal: resyncing here could
			// replace an invalid remote update with a later valid snapshot and
			// hide the fail-closed condition.
			if !errors.Is(err, ErrWatchUnavailable) {
				return err
			}
			if err := p.recoverWatchLiveness(ctx, &watchFailures, true); err != nil {
				return err
			}
		}
	}
}

func (p *RemoteAssignmentPump) consumeWatch(ctx context.Context, watch <-chan actorrouting.RouteSnapshot) (error, bool) {
	progressed := false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err(), progressed
		case snapshot, ok := <-watch:
			if !ok {
				return ErrWatchUnavailable, progressed
			}
			if err := p.install(ctx, snapshot); err != nil {
				if errors.Is(err, actorrouting.ErrStaleRoute) {
					continue
				}
				return err, progressed
			}
			progressed = true
		}
	}
}

func (p *RemoteAssignmentPump) recoverWatchLiveness(ctx context.Context, failures *int, restartAware bool) error {
	*failures = *failures + 1
	p.Reconnects.Add(1)
	p.setState(StateResyncing)
	attempt := *failures
	if attempt > p.options.maxWatchFailures && !p.options.continuousReconnect {
		return fmt.Errorf("%w: failures=%d", ErrWatchLivenessExhausted, *failures)
	}
	if attempt > p.options.maxWatchFailures {
		attempt = p.options.maxWatchFailures
		*failures = p.options.maxWatchFailures
	}
	if err := p.waitBackoff(ctx, attempt); err != nil {
		return err
	}
	for {
		err := p.resync(ctx, restartAware)
		if err == nil {
			return nil
		}
		if !p.options.continuousReconnect || !errors.Is(err, ErrResyncExhausted) {
			return err
		}
		// A controller outage may outlive one bounded retry burst. Keep the
		// last installed snapshot fail-closed, remain non-ready in RESYNCING,
		// and begin another bounded burst after capped backoff. Close or the
		// caller context still interrupts both the fetch and the wait.
		if err := p.waitBackoff(ctx, p.options.maxWatchFailures); err != nil {
			return err
		}
	}
}

func (p *RemoteAssignmentPump) resync(ctx context.Context, restartAware bool) error {
	p.Resyncs.Add(1)
	p.setState(StateResyncing)
	baseline := p.resyncBaseline()
	var lastErr error
	for attempt := 1; attempt <= p.options.maxResyncAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var snapshot actorrouting.RouteSnapshot
		var fetchErr error
		if source, ok := p.source.(RestartAwareRemoteSnapshotSource); ok && restartAware {
			snapshot, fetchErr = source.FetchFullSnapshotForResync(ctx, baseline)
		} else {
			snapshot, fetchErr = p.source.FetchFullSnapshot(ctx)
		}
		if fetchErr != nil {
			lastErr = fetchErr
			if attempt < p.options.maxResyncAttempts {
				if err := p.waitBackoff(ctx, attempt); err != nil {
					return err
				}
			}
			continue
		}
		err := p.install(ctx, snapshot)
		if err == nil {
			p.setState(StateWatching)
			return nil
		}
		lastErr = err
		if !errors.Is(err, actorrouting.ErrStaleRoute) {
			return err
		}
		if attempt < p.options.maxResyncAttempts {
			if err := p.waitBackoff(ctx, attempt); err != nil {
				return err
			}
		}
	}
	if lastErr == nil {
		lastErr = ErrWatchUnavailable
	}
	return fmt.Errorf("%w: %w", ErrResyncExhausted, lastErr)
}

func (p *RemoteAssignmentPump) resyncBaseline() ResyncBaseline {
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	if !p.initialized {
		return ResyncBaseline{}
	}
	return ResyncBaseline{
		Snapshot: actorrouting.CloneRouteSnapshot(p.snapshot),
		Revision: p.revision,
	}
}

func (p *RemoteAssignmentPump) waitBackoff(ctx context.Context, attempt int) error {
	delay := p.options.retryBackoff
	if p.options.backoff != nil {
		delay = p.options.backoff(attempt)
	} else {
		for i := 1; i < attempt; i++ {
			if delay >= p.options.maxRetryBackoff/2 {
				delay = p.options.maxRetryBackoff
				break
			}
			delay *= 2
		}
		if delay > p.options.maxRetryBackoff {
			delay = p.options.maxRetryBackoff
		}
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (p *RemoteAssignmentPump) install(ctx context.Context, snapshot actorrouting.RouteSnapshot) error {
	started := time.Now()
	if ctx == nil {
		return actorrouting.ErrContextRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.installMu.Lock()
	defer p.installMu.Unlock()

	previous := p.Snapshot()
	previousRevision := p.CurrentRevision()
	if err := validateIncoming(previous, previousRevision, snapshot); err != nil {
		if errors.Is(err, actorrouting.ErrStaleRoute) {
			p.StaleIgnored.Add(1)
		}
		return err
	}
	if err := p.installer.InstallFullSnapshot(ctx, snapshot); err != nil {
		if errors.Is(err, actorrouting.ErrStaleRoute) {
			p.StaleIgnored.Add(1)
		}
		return err
	}

	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	if p.initialized && snapshot.GlobalRevision == p.revision {
		p.Duplicates.Add(1)
		p.snapshot = actorrouting.CloneRouteSnapshot(snapshot)
		return nil
	}
	p.snapshot = actorrouting.CloneRouteSnapshot(snapshot)
	p.revision = snapshot.GlobalRevision
	p.initialized = true
	p.Installed.Add(1)
	if metrics := p.metrics.Load(); metrics != nil {
		metrics.ObserveWatchApplyLag(time.Since(started))
	}
	return nil
}

func validateIncoming(previous actorrouting.RouteSnapshot, previousRevision uint64, next actorrouting.RouteSnapshot) error {
	if _, err := actorrouting.NewStaticResolver(next); err != nil {
		return err
	}
	if previousRevision == 0 {
		return nil
	}
	if next.GlobalRevision < previousRevision {
		return actorrouting.ErrStaleRoute
	}
	if next.GlobalRevision == previousRevision {
		if reflect.DeepEqual(previous, next) {
			return nil
		}
		return actorrouting.ErrSnapshotRevisionConflict
	}
	if !sameTopology(previous, next) {
		return actorrouting.ErrHashMigrationRequired
	}

	// The shared validator intentionally rejects same-generation state
	// changes. For a remote full snapshot, a trusted complete ASSIGNING ->
	// ACTIVE publication is legal, so validate a shape-equivalent previous
	// snapshot with only those transitions normalized to ACTIVE.
	validationPrevious := actorrouting.CloneRouteSnapshot(previous)
	for _, domain := range routeDomains(previous) {
		oldDomain := domainSnapshot(previous, domain)
		newDomain := domainSnapshot(next, domain)
		normalizedDomain := domainSnapshotPtr(&validationPrevious, domain)
		for shardID := uint32(0); shardID < oldDomain.ShardCount; shardID++ {
			oldRoute := oldDomain.Routes[shardID]
			newRoute := newDomain.Routes[shardID]
			sameGeneration := newRoute.ShardRevision == oldRoute.ShardRevision &&
				newRoute.ExpectedWriterEpoch == oldRoute.ExpectedWriterEpoch &&
				newRoute.ExpectedLeaseEpoch == oldRoute.ExpectedLeaseEpoch
			if !sameGeneration || oldRoute.State != actorrouting.ShardStateAssigning || newRoute.State != actorrouting.ShardStateActive {
				continue
			}
			normalizedDomain.Routes[shardID].State = actorrouting.ShardStateActive
			normalizedDomain.Routes[shardID].ExpectedLeaseEpoch = newRoute.ExpectedLeaseEpoch
			if normalizedDomain.Routes[shardID].LeaseUntilMs <= 0 {
				normalizedDomain.Routes[shardID].LeaseUntilMs = newRoute.LeaseUntilMs
			}
			// A Manager-owned planned migration deliberately keeps the source
			// holder in the complete ASSIGNING Snapshot, then selects the target
			// holder in ACTIVE without incrementing the already-committed target
			// generation. Normalize only this exact forward state transition to
			// the authenticated ACTIVE holder. Same-generation holder changes in
			// every other state continue to fail in ValidateRouteSnapshotUpdate.
			newHolder := next.Holders[newRoute.HolderIndex]
			normalizedDomain.Routes[shardID].HolderIndex = len(validationPrevious.Holders)
			validationPrevious.Holders = append(validationPrevious.Holders, newHolder)
		}
	}
	return actorrouting.ValidateRouteSnapshotUpdate(validationPrevious, next)
}

func sameTopology(left, right actorrouting.RouteSnapshot) bool {
	return left.Realm == right.Realm &&
		left.HashVersion == right.HashVersion &&
		left.HashSeed == right.HashSeed &&
		left.Player.ShardCount == right.Player.ShardCount &&
		left.Farm.ShardCount == right.Farm.ShardCount &&
		left.FarmWorker.ShardCount == right.FarmWorker.ShardCount
}

func domainSnapshot(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain) actorrouting.DomainSnapshot {
	switch domain {
	case actorrouting.DomainPlayer:
		return snapshot.Player
	case actorrouting.DomainFarm:
		return snapshot.Farm
	case actorrouting.DomainFarmWorker:
		return snapshot.FarmWorker
	default:
		return actorrouting.DomainSnapshot{}
	}
}

func domainSnapshotPtr(snapshot *actorrouting.RouteSnapshot, domain actorrouting.RouteDomain) *actorrouting.DomainSnapshot {
	switch domain {
	case actorrouting.DomainPlayer:
		return &snapshot.Player
	case actorrouting.DomainFarm:
		return &snapshot.Farm
	case actorrouting.DomainFarmWorker:
		return &snapshot.FarmWorker
	default:
		return nil
	}
}

func routeDomains(snapshot actorrouting.RouteSnapshot) []actorrouting.RouteDomain {
	domains := []actorrouting.RouteDomain{
		actorrouting.DomainPlayer,
		actorrouting.DomainFarm,
	}
	if snapshot.FarmWorker.ShardCount != 0 {
		domains = append(domains, actorrouting.DomainFarmWorker)
	}
	return domains
}

func (p *RemoteAssignmentPump) fail(err error) bool {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	if p.state == StateClosed || p.isClosedLocked() {
		p.state = StateClosed
		return true
	}
	p.lastErr = err
	p.state = StateDegraded
	return false
}

func (p *RemoteAssignmentPump) setState(state State) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	if p.state == StateClosed && state != StateClosed {
		return
	}
	if p.isClosedLocked() {
		p.state = StateClosed
		return
	}
	p.state = state
}

func (p *RemoteAssignmentPump) isClosed() bool {
	select {
	case <-p.closed:
		return true
	default:
		return false
	}
}

func (p *RemoteAssignmentPump) isClosedLocked() bool {
	select {
	case <-p.closed:
		return true
	default:
		return false
	}
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
