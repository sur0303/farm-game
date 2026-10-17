package authoritycomposition

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/netendpoint"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	// PooledEndpoint is process-local only. It lets the existing immutable
	// routed-client maps contain one stable client while PooledResolver and
	// PooledClientConn select the real endpoint from the same pool generation.
	PooledEndpoint = "route-pool.invalid:1"

	defaultEndpointDialTimeout = 5 * time.Second
	defaultEndpointRetireGrace = 5 * time.Second
	maxEndpointDialConcurrency = 16
)

var (
	ErrEndpointPoolUnavailable = errors.New("authority composition: endpoint pool unavailable")
	ErrEndpointPoolGeneration  = errors.New("authority composition: endpoint pool generation mismatch")
	errEndpointDialUnavailable = errors.New("authority composition: endpoint dial unavailable")
)

// ManagedEndpointConnection is the smallest connection capability owned by
// EndpointConnectionManager. grpc.ClientConn satisfies this interface; tests
// can inject a hermetic connection without opening sockets.
type ManagedEndpointConnection interface {
	grpc.ClientConnInterface
	Close() error
}

type EndpointDialFunc func(context.Context, string) (ManagedEndpointConnection, error)

type EndpointConnectionOptions struct {
	MaxEndpoints int
	DialTimeout  time.Duration
	RetireGrace  time.Duration
	// DialOptions are appended to the pool's transport/security options. They
	// are used for process-local client instrumentation and must not replace
	// the mandatory insecure transport option used by this deployment.
	DialOptions []grpc.DialOption
	Dial        EndpointDialFunc
}

func EndpointConnectionOptionsFromEnvironment() EndpointConnectionOptions {
	return EndpointConnectionOptions{
		MaxEndpoints: ActorRouteEndpointLimitFromEnvironment(),
		DialTimeout: envPositiveDurationRange(
			"ACTOR_ROUTE_ENDPOINT_DIAL_TIMEOUT", defaultEndpointDialTimeout, time.Millisecond, maxRemoteTimeout,
		),
		RetireGrace: envPositiveDurationRange(
			"ACTOR_ROUTE_ENDPOINT_RETIRE_GRACE", defaultEndpointRetireGrace, time.Millisecond, maxRemoteTimeout,
		),
	}
}

// EndpointKey identifies the logical holder transport, not merely its
// network address. A deadline-only Snapshot revision keeps this key stable;
// a holder boot or endpoint change produces a new key and therefore a new
// pre-dialed transport target.
type EndpointKey struct {
	HolderID     string
	HolderBootID string
	Endpoint     string
}

type endpointConnection struct {
	endpoint string
	conn     ManagedEndpointConnection

	mu         sync.Mutex
	inflight   int
	retired    bool
	closed     bool
	drained    chan struct{}
	retireOnce sync.Once
}

type endpointGeneration struct {
	// revision is the newest complete RouteSnapshot observed by the pool. It
	// advances for deadline-only projections but does not imply a transport
	// generation swap.
	revision uint64
	// transportGeneration changes only when the EndpointKey set changes.
	transportGeneration uint64
	keys                map[EndpointKey]struct{}
	entries             map[string]*endpointConnection
}

type endpointRetirement struct {
	entry   *endpointConnection
	drained bool
}

// EndpointConnectionManager pre-dials complete endpoint generations and
// atomically swaps them. Removed connections stay alive until their inflight
// leases drain or the bounded retirement grace expires.
type EndpointConnectionManager struct {
	provider *actorrouting.DynamicAssignmentProvider
	opts     EndpointConnectionOptions

	mu       sync.Mutex
	current  endpointGeneration
	running  bool
	closed   bool
	terminal error

	runCancel         context.CancelFunc
	closeOnce         sync.Once
	closeCh           chan struct{}
	generationChanged chan struct{}
	retireWG          sync.WaitGroup

	// These hooks are package-private deterministic concurrency seams. They
	// are nil in production and must never run while mu is held.
	testAfterGenerationSwap  func()
	testBeforeRetirementWait func()
}

func NewEndpointConnectionManager(provider *actorrouting.DynamicAssignmentProvider, options EndpointConnectionOptions) (*EndpointConnectionManager, error) {
	if provider == nil {
		return nil, fmt.Errorf("%w: assignment provider is required", ErrEndpointPoolUnavailable)
	}
	if options.MaxEndpoints <= 0 {
		options.MaxEndpoints = DefaultActorRouteMaxEndpoints
	}
	if options.MaxEndpoints > maxActorRouteEndpoints {
		options.MaxEndpoints = maxActorRouteEndpoints
	}
	if options.DialTimeout < time.Millisecond {
		options.DialTimeout = defaultEndpointDialTimeout
	}
	if options.DialTimeout > maxRemoteTimeout {
		options.DialTimeout = maxRemoteTimeout
	}
	if options.RetireGrace < time.Millisecond {
		options.RetireGrace = defaultEndpointRetireGrace
	}
	if options.RetireGrace > maxRemoteTimeout {
		options.RetireGrace = maxRemoteTimeout
	}
	if options.Dial == nil {
		dialOptions := make([]grpc.DialOption, 0, 2+len(options.DialOptions))
		dialOptions = append(dialOptions,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		dialOptions = append(dialOptions, options.DialOptions...)
		options.Dial = func(ctx context.Context, endpoint string) (ManagedEndpointConnection, error) {
			return grpc.DialContext(ctx, endpoint, dialOptions...)
		}
	}
	return &EndpointConnectionManager{
		provider:          provider,
		opts:              options,
		closeCh:           make(chan struct{}),
		generationChanged: make(chan struct{}),
	}, nil
}

func (m *EndpointConnectionManager) Run(ctx context.Context) error {
	if m == nil || m.provider == nil {
		return ErrEndpointPoolUnavailable
	}
	if ctx == nil {
		return actorrouting.ErrContextRequired
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	if m.running {
		m.mu.Unlock()
		return fmt.Errorf("%w: already running", ErrEndpointPoolUnavailable)
	}
	runCtx, cancel := context.WithCancel(ctx)
	m.running = true
	m.runCancel = cancel
	m.mu.Unlock()
	defer func() {
		cancel()
		m.mu.Lock()
		m.running = false
		m.runCancel = nil
		m.mu.Unlock()
	}()

	if err := m.installInitialGeneration(runCtx); err != nil {
		return m.failRun(runCtx, err)
	}
	for {
		m.mu.Lock()
		revision := m.current.revision
		m.mu.Unlock()
		updates, err := m.provider.WatchFrom(runCtx, revision)
		if err != nil {
			if runCtx.Err() != nil {
				return runCtx.Err()
			}
			if err := m.reload(runCtx); err != nil {
				return m.failRun(runCtx, err)
			}
			if err := waitEndpointReconnect(runCtx); err != nil {
				return err
			}
			continue
		}
		for {
			select {
			case <-runCtx.Done():
				return runCtx.Err()
			case update, ok := <-updates:
				if !ok {
					if err := m.reload(runCtx); err != nil {
						return m.failRun(runCtx, err)
					}
					if err := waitEndpointReconnect(runCtx); err != nil {
						return err
					}
					goto reconnect
				}
				if update.Revision == 0 || update.Revision != update.Snapshot.GlobalRevision {
					return m.failRun(runCtx, fmt.Errorf("%w: invalid endpoint update revision", ErrEndpointPoolGeneration))
				}
				if err := m.install(runCtx, update.Snapshot); err != nil {
					return m.failRun(runCtx, err)
				}
			}
		}
	reconnect:
	}
}

func (m *EndpointConnectionManager) failRun(ctx context.Context, err error) error {
	if err == nil || ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	m.mu.Lock()
	if m.terminal == nil {
		m.terminal = err
	}
	m.mu.Unlock()
	return err
}

// installInitialGeneration retries only transient all-or-nothing endpoint dial
// failures. No generation is published until every endpoint is connected, so
// callers remain fail-closed while concurrently starting holders bring their
// already-bound logic listeners into DNS. Snapshot/fence/shape errors remain
// terminal, and a later published generation still terminates on dial failure.
func (m *EndpointConnectionManager) installInitialGeneration(ctx context.Context) error {
	for {
		err := m.reload(ctx)
		if err == nil {
			return nil
		}
		if !errors.Is(err, errEndpointDialUnavailable) {
			return err
		}
		if err := waitEndpointReconnect(ctx); err != nil {
			return err
		}
	}
}

func (m *EndpointConnectionManager) reload(ctx context.Context) error {
	snapshot, err := m.provider.GetSnapshot(ctx)
	if err != nil {
		return err
	}
	return m.install(ctx, snapshot)
}

func (m *EndpointConnectionManager) install(ctx context.Context, snapshot actorrouting.RouteSnapshot) error {
	keys, err := canonicalSnapshotEndpointKeys(snapshot, m.opts.MaxEndpoints)
	if err != nil {
		return err
	}
	endpoints := uniqueEndpointValues(keys)
	keySet := endpointKeySet(keys)
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return context.Canceled
	}
	if snapshot.GlobalRevision < m.current.revision {
		m.mu.Unlock()
		return nil
	}
	if snapshot.GlobalRevision == m.current.revision && m.current.revision != 0 {
		consistent := sameEndpointKeySet(keySet, m.current.keys)
		m.mu.Unlock()
		if !consistent {
			return fmt.Errorf("%w: same revision has different endpoint identities", ErrEndpointPoolGeneration)
		}
		return nil
	}
	old := m.current

	// A deadline-only projection changes the observed Snapshot revision but not
	// the physical holder targets. Advance the observation in place: no dial,
	// retirement, or transport-generation swap is needed.
	if old.revision != 0 && sameEndpointKeySet(keySet, old.keys) {
		m.current.revision = snapshot.GlobalRevision
		m.signalGenerationChangedLocked()
		m.mu.Unlock()
		return nil
	}

	reused := make(map[string]*endpointConnection, len(endpoints))
	missingSet := make(map[string]struct{}, len(endpoints))
	missing := make([]string, 0, len(endpoints))
	for _, key := range keys {
		if entry := old.entries[key.Endpoint]; entry != nil {
			if _, sameTarget := old.keys[key]; sameTarget {
				reused[key.Endpoint] = entry
				continue
			}
		}
		if _, alreadyMissing := missingSet[key.Endpoint]; !alreadyMissing {
			missingSet[key.Endpoint] = struct{}{}
			missing = append(missing, key.Endpoint)
		}
	}
	m.mu.Unlock()

	dialed, err := m.dialMissing(ctx, missing)
	if err != nil {
		closeDialed(dialed)
		return err
	}
	for endpoint, entry := range dialed {
		reused[endpoint] = entry
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		closeDialed(dialed)
		return context.Canceled
	}
	// Another installer cannot normally race Run, but retain fail-closed
	// semantics if a future bootstrap path invokes install concurrently.
	if m.current.revision != old.revision {
		m.mu.Unlock()
		closeDialed(dialed)
		return fmt.Errorf("%w: concurrent endpoint publication", ErrEndpointPoolGeneration)
	}
	retirements := make([]endpointRetirement, 0, len(old.entries))
	for endpoint, entry := range old.entries {
		if currentEntry, stillCurrent := reused[endpoint]; !stillCurrent || currentEntry != entry {
			if retirement, registered := m.registerRetirementLocked(entry); registered {
				retirements = append(retirements, retirement)
			}
		}
	}
	transportGeneration := old.transportGeneration + 1
	if transportGeneration == 0 {
		transportGeneration = 1
	}
	m.current = endpointGeneration{
		revision:            snapshot.GlobalRevision,
		transportGeneration: transportGeneration,
		keys:                keySet,
		entries:             reused,
	}
	m.signalGenerationChangedLocked()
	afterSwap := m.testAfterGenerationSwap
	m.mu.Unlock()

	if afterSwap != nil {
		afterSwap()
	}
	m.startRetirements(retirements)
	return nil
}

func (m *EndpointConnectionManager) dialMissing(ctx context.Context, endpoints []string) (map[string]*endpointConnection, error) {
	result := make(map[string]*endpointConnection, len(endpoints))
	if len(endpoints) == 0 {
		return result, nil
	}
	type dialResult struct {
		endpoint string
		conn     ManagedEndpointConnection
		err      error
	}
	results := make(chan dialResult, len(endpoints))
	semaphore := make(chan struct{}, maxEndpointDialConcurrency)
	var wg sync.WaitGroup
	for _, endpoint := range endpoints {
		endpoint := endpoint
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case semaphore <- struct{}{}:
			case <-ctx.Done():
				results <- dialResult{endpoint: endpoint, err: ctx.Err()}
				return
			}
			defer func() { <-semaphore }()
			dialCtx, cancel := context.WithTimeout(ctx, m.opts.DialTimeout)
			conn, err := m.opts.Dial(dialCtx, endpoint)
			cancel()
			results <- dialResult{endpoint: endpoint, conn: conn, err: err}
		}()
	}
	wg.Wait()
	close(results)
	var firstErr error
	for item := range results {
		if item.err != nil || item.conn == nil {
			if firstErr == nil {
				if item.err == nil {
					item.err = ErrEndpointPoolUnavailable
				}
				firstErr = fmt.Errorf("%w: %w: dial endpoint %s: %v", ErrEndpointPoolUnavailable, errEndpointDialUnavailable, item.endpoint, item.err)
			}
			continue
		}
		result[item.endpoint] = &endpointConnection{
			endpoint: item.endpoint, conn: item.conn, drained: make(chan struct{}, 1),
		}
	}
	if firstErr != nil {
		return result, firstErr
	}
	return result, nil
}

func (m *EndpointConnectionManager) ReadyFor(snapshot actorrouting.RouteSnapshot) bool {
	if m == nil {
		return false
	}
	keys, err := canonicalSnapshotEndpointKeys(snapshot, m.opts.MaxEndpoints)
	if err != nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running && !m.closed && m.terminal == nil &&
		m.current.revision == snapshot.GlobalRevision && sameEndpointKeySet(endpointKeySet(keys), m.current.keys)
}

type grpcConnectionState interface {
	GetState() connectivity.State
	Connect()
}

// ConnectionsReady reports whether every pre-dialed connection in the current
// complete generation is in gRPC READY. Idle connections are explicitly woken
// and remain fail-closed until a later observation sees READY.
func (m *EndpointConnectionManager) ConnectionsReady() bool {
	return m.connectionsReady(0, nil)
}

// ConnectionsReadyFor pins the observation to one exact complete Snapshot
// generation. A generation swap around the state reads makes it false.
func (m *EndpointConnectionManager) ConnectionsReadyFor(snapshot actorrouting.RouteSnapshot) bool {
	if m == nil {
		return false
	}
	keys, err := canonicalSnapshotEndpointKeys(snapshot, m.opts.MaxEndpoints)
	if err != nil || snapshot.GlobalRevision == 0 {
		return false
	}
	return m.connectionsReady(snapshot.GlobalRevision, endpointKeySet(keys))
}

func (m *EndpointConnectionManager) connectionsReady(revision uint64, keys map[EndpointKey]struct{}) bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	if !m.running || m.closed || m.terminal != nil || m.current.revision == 0 ||
		(revision == 0 && len(m.current.entries) == 0) ||
		(revision != 0 && (m.current.revision != revision || !sameEndpointKeySet(keys, m.current.keys))) {
		m.mu.Unlock()
		return false
	}
	if len(m.current.entries) == 0 {
		m.mu.Unlock()
		return revision != 0 && len(keys) == 0
	}
	observedRevision := m.current.revision
	connections := make([]grpcConnectionState, 0, len(m.current.entries))
	for _, entry := range m.current.entries {
		if entry == nil || entry.conn == nil {
			m.mu.Unlock()
			return false
		}
		state, ok := entry.conn.(grpcConnectionState)
		if !ok {
			m.mu.Unlock()
			return false
		}
		connections = append(connections, state)
	}
	m.mu.Unlock()

	for _, connection := range connections {
		state := connection.GetState()
		if state == connectivity.Idle {
			connection.Connect()
			return false
		}
		if state != connectivity.Ready {
			return false
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running && !m.closed && m.terminal == nil && m.current.revision == observedRevision &&
		(revision == 0 || sameEndpointKeySet(keys, m.current.keys))
}

func (m *EndpointConnectionManager) Contains(endpoint string, revision uint64) bool {
	if m == nil || endpoint == "" || revision == 0 {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running && !m.closed && m.terminal == nil && m.current.revision == revision && m.current.entries[endpoint] != nil
}

// ContainsAssignment verifies the logical holder target independently from the
// complete Snapshot revision. Deadline-only projections keep this key stable.
func (m *EndpointConnectionManager) ContainsAssignment(assignment actorrouting.Assignment) bool {
	key := EndpointKey{HolderID: assignment.HolderPod, HolderBootID: assignment.HolderBootID, Endpoint: assignment.Endpoint}
	if key.HolderID == "" || key.HolderBootID == "" || key.Endpoint == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, knownKey := m.current.keys[key]
	return m.running && !m.closed && m.terminal == nil && knownKey && m.current.entries[key.Endpoint] != nil
}

// waitForSnapshotRevision waits until the endpoint watcher has observed at
// least revision. It is used only during a route/transport convergence window;
// it never dials on the request path.
func (m *EndpointConnectionManager) signalGenerationChangedLocked() {
	if m.generationChanged == nil {
		m.generationChanged = make(chan struct{})
		return
	}
	close(m.generationChanged)
	m.generationChanged = make(chan struct{})
}

func (m *EndpointConnectionManager) waitForSnapshotRevision(ctx context.Context, revision uint64) error {
	if m == nil || ctx == nil || revision == 0 {
		return fmt.Errorf("%w: invalid generation wait", ErrEndpointPoolGeneration)
	}
	for {
		m.mu.Lock()
		if m.closed || m.terminal != nil || !m.running {
			m.mu.Unlock()
			return fmt.Errorf("%w: endpoint pool is not running", ErrEndpointPoolGeneration)
		}
		if m.current.revision >= revision {
			m.mu.Unlock()
			return nil
		}
		changed := m.generationChanged
		if changed == nil {
			changed = make(chan struct{})
			m.generationChanged = changed
		}
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: wait revision=%d: %w", ErrEndpointPoolGeneration, revision, ctx.Err())
		case <-m.closeCh:
			return fmt.Errorf("%w: endpoint pool closed", ErrEndpointPoolGeneration)
		case <-changed:
		}
	}
}

func (m *EndpointConnectionManager) acquire(endpoint string, revision uint64) (ManagedEndpointConnection, func(), error) {
	if m == nil {
		return nil, nil, ErrEndpointPoolUnavailable
	}
	m.mu.Lock()
	if m.closed || m.terminal != nil || !m.running || m.current.revision != revision {
		m.mu.Unlock()
		return nil, nil, fmt.Errorf("%w: endpoint=%s revision=%d", ErrEndpointPoolGeneration, endpoint, revision)
	}
	conn, entry, err := m.acquireLocked(endpoint, ErrEndpointPoolUnavailable)
	m.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}
	return conn, m.releaseEntry(entry), nil
}

func (m *EndpointConnectionManager) acquireAssignment(assignment actorrouting.Assignment) (ManagedEndpointConnection, func(), error) {
	if m == nil {
		return nil, nil, ErrEndpointPoolUnavailable
	}
	key := EndpointKey{HolderID: assignment.HolderPod, HolderBootID: assignment.HolderBootID, Endpoint: assignment.Endpoint}
	m.mu.Lock()
	if m.closed || m.terminal != nil || !m.running {
		m.mu.Unlock()
		return nil, nil, fmt.Errorf("%w: endpoint=%s pool unavailable", actorrouting.ErrTransportGenerationTransition, assignment.Endpoint)
	}
	if _, knownKey := m.current.keys[key]; !knownKey {
		m.mu.Unlock()
		return nil, nil, fmt.Errorf("%w: endpoint=%s holder=%s boot=%s", actorrouting.ErrTransportGenerationTransition, assignment.Endpoint, assignment.HolderPod, assignment.HolderBootID)
	}
	conn, entry, err := m.acquireLocked(assignment.Endpoint, ErrEndpointPoolGeneration)
	m.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}
	return conn, m.releaseEntry(entry), nil
}

// acquireLocked requires m.mu and entry.mu to be acquired only briefly. It
// returns a lease entry whose inflight count has already been incremented.
func (m *EndpointConnectionManager) acquireLocked(endpoint string, missing error) (ManagedEndpointConnection, *endpointConnection, error) {
	entry := m.current.entries[endpoint]
	if entry == nil {
		return nil, nil, fmt.Errorf("%w: endpoint=%s", missing, endpoint)
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.closed || entry.retired {
		return nil, nil, fmt.Errorf("%w: endpoint=%s retired", ErrEndpointPoolUnavailable, endpoint)
	}
	entry.inflight++
	return entry.conn, entry, nil
}

func (m *EndpointConnectionManager) releaseEntry(entry *endpointConnection) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			entry.mu.Lock()
			entry.inflight--
			if entry.retired && entry.inflight == 0 {
				select {
				case entry.drained <- struct{}{}:
				default:
				}
			}
			entry.mu.Unlock()
		})
	}
}

// registerRetirementLocked linearizes retirement ownership with generation
// publication and Close. The WaitGroup Add happens while the manager gate is
// locked, so after Close sets closed and releases mu no installer can add work
// concurrently with Close's Wait.
func (m *EndpointConnectionManager) registerRetirementLocked(entry *endpointConnection) (endpointRetirement, bool) {
	if entry == nil {
		return endpointRetirement{}, false
	}
	var retirement endpointRetirement
	registered := false
	entry.retireOnce.Do(func() {
		entry.mu.Lock()
		entry.retired = true
		retirement = endpointRetirement{entry: entry, drained: entry.inflight == 0}
		entry.mu.Unlock()
		m.retireWG.Add(1)
		registered = true
	})
	return retirement, registered
}

// startRetirements performs every wait and external connection Close after the
// manager lock has been released.
func (m *EndpointConnectionManager) startRetirements(retirements []endpointRetirement) {
	for _, retirement := range retirements {
		retirement := retirement
		go func() {
			defer m.retireWG.Done()
			entry := retirement.entry
			if !retirement.drained {
				timer := time.NewTimer(m.opts.RetireGrace)
				select {
				case <-entry.drained:
					if !timer.Stop() {
						<-timer.C
					}
				case <-timer.C:
				case <-m.closeCh:
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
				}
			}
			entry.mu.Lock()
			var conn ManagedEndpointConnection
			if !entry.closed {
				entry.closed = true
				conn = entry.conn
			}
			entry.mu.Unlock()
			if conn != nil {
				_ = conn.Close()
			}
		}()
	}
}

func (m *EndpointConnectionManager) Close() error {
	if m == nil {
		return nil
	}
	m.closeOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		cancel := m.runCancel
		retirements := make([]endpointRetirement, 0, len(m.current.entries))
		for _, entry := range m.current.entries {
			if retirement, registered := m.registerRetirementLocked(entry); registered {
				retirements = append(retirements, retirement)
			}
		}
		m.current = endpointGeneration{}
		beforeWait := m.testBeforeRetirementWait
		m.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		close(m.closeCh)
		m.startRetirements(retirements)
		if beforeWait != nil {
			beforeWait()
		}
	})
	m.retireWG.Wait()
	return nil
}

func (m *EndpointConnectionManager) TerminalError() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.terminal
}

func canonicalSnapshotEndpoints(snapshot actorrouting.RouteSnapshot, maximum int) ([]string, error) {
	keys, err := canonicalSnapshotEndpointKeys(snapshot, maximum)
	if err != nil {
		return nil, err
	}
	return uniqueEndpointValues(keys), nil
}

func canonicalSnapshotEndpointKeys(snapshot actorrouting.RouteSnapshot, maximum int) ([]EndpointKey, error) {
	keys := make([]EndpointKey, 0, len(snapshot.Holders))
	seenKeys := make(map[EndpointKey]struct{}, len(snapshot.Holders))
	seenEndpoints := make(map[string]struct{}, len(snapshot.Holders))
	workerHolders := make(map[int]struct{}, len(snapshot.FarmWorker.Routes))
	actorHolders := make(map[int]struct{}, len(snapshot.Player.Routes)+len(snapshot.Farm.Routes))
	for _, route := range snapshot.FarmWorker.Routes {
		if route.HolderIndex >= 0 && route.HolderIndex < len(snapshot.Holders) {
			workerHolders[route.HolderIndex] = struct{}{}
		}
	}
	for _, routes := range [][]actorrouting.ShardRoute{snapshot.Player.Routes, snapshot.Farm.Routes} {
		for _, route := range routes {
			if route.HolderIndex >= 0 && route.HolderIndex < len(snapshot.Holders) {
				actorHolders[route.HolderIndex] = struct{}{}
			}
		}
	}
	for index, holder := range snapshot.Holders {
		if _, workerOnly := workerHolders[index]; workerOnly {
			if _, actor := actorHolders[index]; actor {
				return nil, fmt.Errorf(
					"%w: holder %d is shared by actor and FARM_WORKER routes",
					ErrEndpointPoolUnavailable,
					index,
				)
			}
			continue
		}
		endpoint, err := canonicalEndpoint(holder.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("%w: holder %d endpoint: %v", ErrEndpointPoolUnavailable, index, err)
		}
		if endpoint != holder.Endpoint {
			return nil, fmt.Errorf("%w: holder %d endpoint is not canonical", ErrEndpointPoolUnavailable, index)
		}
		key := EndpointKey{HolderID: holder.InstanceID, HolderBootID: holder.BootID, Endpoint: endpoint}
		if _, exists := seenKeys[key]; !exists {
			seenKeys[key] = struct{}{}
			keys = append(keys, key)
		}
		seenEndpoints[endpoint] = struct{}{}
	}
	if maximum <= 0 {
		maximum = DefaultActorRouteMaxEndpoints
	}
	if len(seenEndpoints) > maximum {
		return nil, fmt.Errorf("%w: snapshot has %d endpoints, maximum %d", ErrEndpointPoolUnavailable, len(seenEndpoints), maximum)
	}
	return keys, nil
}

func uniqueEndpointValues(keys []EndpointKey) []string {
	seen := make(map[string]struct{}, len(keys))
	endpoints := make([]string, 0, len(keys))
	for _, key := range keys {
		if _, exists := seen[key.Endpoint]; exists {
			continue
		}
		seen[key.Endpoint] = struct{}{}
		endpoints = append(endpoints, key.Endpoint)
	}
	return endpoints
}

func endpointKeySet(keys []EndpointKey) map[EndpointKey]struct{} {
	set := make(map[EndpointKey]struct{}, len(keys))
	for _, key := range keys {
		set[key] = struct{}{}
	}
	return set
}

func sameEndpointKeySet(left, right map[EndpointKey]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for key := range left {
		if _, exists := right[key]; !exists {
			return false
		}
	}
	return true
}

func canonicalEndpoint(value string) (string, error) {
	return netendpoint.Canonical(value, 512)
}

func sameEndpointSet(endpoints []string, entries map[string]*endpointConnection) bool {
	if len(endpoints) != len(entries) {
		return false
	}
	for _, endpoint := range endpoints {
		if entries[endpoint] == nil {
			return false
		}
	}
	return true
}

func closeDialed(entries map[string]*endpointConnection) {
	for _, entry := range entries {
		if entry != nil && entry.conn != nil {
			_ = entry.conn.Close()
		}
	}
}

func waitEndpointReconnect(ctx context.Context) error {
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// PooledResolver preserves all route fences while replacing only the
// process-local map key after proving the selected logical EndpointKey is
// present in the current transport generation. Snapshot deadline revisions do
// not by themselves invalidate a physical connection.
type PooledResolver struct {
	inner   actorrouting.Resolver
	pool    *EndpointConnectionManager
	domains map[actorrouting.RouteDomain]bool
}

func NewPooledResolver(inner actorrouting.Resolver, pool *EndpointConnectionManager) (*PooledResolver, error) {
	return NewPooledResolverForDomains(inner, pool, actorrouting.DomainPlayer, actorrouting.DomainFarm)
}

func NewPooledResolverForDomains(inner actorrouting.Resolver, pool *EndpointConnectionManager, domains ...actorrouting.RouteDomain) (*PooledResolver, error) {
	if inner == nil || pool == nil {
		return nil, fmt.Errorf("%w: resolver and pool are required", ErrEndpointPoolUnavailable)
	}
	selected := make(map[actorrouting.RouteDomain]bool, len(domains))
	for _, domain := range domains {
		if domain != actorrouting.DomainPlayer && domain != actorrouting.DomainFarm {
			return nil, fmt.Errorf("%w: invalid pooled route domain", ErrEndpointPoolUnavailable)
		}
		selected[domain] = true
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("%w: at least one pooled route domain is required", ErrEndpointPoolUnavailable)
	}
	return &PooledResolver{inner: inner, pool: pool, domains: selected}, nil
}

func (r *PooledResolver) Resolve(ctx context.Context, address actorrouting.Address) (actorrouting.Assignment, error) {
	const maxConvergenceAttempts = 8
	for attempt := 0; attempt < maxConvergenceAttempts; attempt++ {
		assignment, err := r.inner.Resolve(ctx, address)
		if err != nil {
			return actorrouting.Assignment{}, err
		}
		if !r.domains[actorrouting.RouteDomain(address.Kind)] {
			return assignment, nil
		}
		if r.pool.ContainsAssignment(assignment) {
			assignment.Endpoint = PooledEndpoint
			return assignment, nil
		}
		// Resolver and endpoint-pool watchers consume the same provider update
		// independently. Wait for the exact pool generation instead of exposing
		// their normal sub-millisecond publication gap as a backend outage. If
		// the pool is already newer than this assignment, the short retry gives
		// the resolver watcher a chance to install the same update.
		if err := r.pool.waitForSnapshotRevision(ctx, assignment.SnapshotRevision); err != nil {
			return actorrouting.Assignment{}, fmt.Errorf("%w: endpoint=%s revision=%d: %w", actorrouting.ErrTransportGenerationTransition, assignment.Endpoint, assignment.SnapshotRevision, err)
		}
		if err := waitEndpointReconnect(ctx); err != nil {
			return actorrouting.Assignment{}, err
		}
	}
	return actorrouting.Assignment{}, fmt.Errorf("%w: resolver/pool did not converge for %v", actorrouting.ErrTransportGenerationTransition, address)
}

// PooledClientConn is a local grpc.ClientConnInterface multiplexer. It never
// dials on the request path: it re-resolves the already-attached route
// metadata, validates the complete actor fence, leases the current logical
// EndpointKey connection, and invokes it.
type PooledClientConn struct {
	resolver actorrouting.Resolver
	pool     *EndpointConnectionManager
}

func NewPooledClientConn(resolver actorrouting.Resolver, pool *EndpointConnectionManager) (*PooledClientConn, error) {
	if resolver == nil || pool == nil {
		return nil, fmt.Errorf("%w: resolver and pool are required", ErrEndpointPoolUnavailable)
	}
	return &PooledClientConn{resolver: resolver, pool: pool}, nil
}

func (c *PooledClientConn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	conn, release, err := c.connection(ctx)
	if err != nil {
		return err
	}
	defer release()
	return conn.Invoke(ctx, method, args, reply, opts...)
}

func (c *PooledClientConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	conn, release, err := c.connection(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := conn.NewStream(ctx, desc, method, opts...)
	if err != nil {
		release()
		return nil, err
	}
	return &pooledClientStream{ClientStream: stream, release: release}, nil
}

func (c *PooledClientConn) connection(ctx context.Context) (ManagedEndpointConnection, func(), error) {
	address, expected, err := outgoingRoute(ctx)
	if err != nil {
		return nil, nil, err
	}
	const maxConvergenceAttempts = 8
	for attempt := 0; attempt < maxConvergenceAttempts; attempt++ {
		assignment, err := c.resolver.Resolve(ctx, address)
		if err != nil {
			return nil, nil, err
		}
		generationMatches := uint64(assignment.ShardID) == expected.shard &&
			assignment.ShardRevision == expected.shardRevision &&
			assignment.WriterEpoch == expected.writerEpoch &&
			assignment.HolderPod == expected.holder &&
			assignment.HolderBootID == expected.holderBoot &&
			assignment.LeaseEpoch == expected.leaseEpoch
		if !generationMatches {
			return nil, nil, fmt.Errorf("%w: outgoing route is stale", actorrouting.ErrStaleRoute)
		}
		if !c.pool.ContainsAssignment(assignment) {
			// A route-only metadata observation can be one provider update ahead
			// of the local pool, or vice versa. Wait for observation convergence;
			// the actor fence above remains mandatory on every retry.
			if err := c.pool.waitForSnapshotRevision(ctx, assignment.SnapshotRevision); err != nil {
				return nil, nil, fmt.Errorf("%w: endpoint=%s revision=%d: %w", actorrouting.ErrTransportGenerationTransition, assignment.Endpoint, assignment.SnapshotRevision, err)
			}
			if err := waitEndpointReconnect(ctx); err != nil {
				return nil, nil, err
			}
			continue
		}
		conn, release, err := c.pool.acquireAssignment(assignment)
		if err != nil && errors.Is(err, ErrEndpointPoolGeneration) {
			return nil, nil, fmt.Errorf("%w: %w", actorrouting.ErrTransportGenerationTransition, err)
		}
		return conn, release, err
	}
	return nil, nil, fmt.Errorf("%w: client/pool did not converge for %v", actorrouting.ErrTransportGenerationTransition, address)
}

type outgoingAssignment struct {
	shard, shardRevision, writerEpoch, leaseEpoch, snapshotRevision uint64
	holder, holderBoot                                              string
}

func outgoingRoute(ctx context.Context) (actorrouting.Address, outgoingAssignment, error) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return actorrouting.Address{}, outgoingAssignment{}, fmt.Errorf("%w: route metadata required", ErrEndpointPoolUnavailable)
	}
	value := func(key string) (string, error) {
		values := md.Get(key)
		if len(values) == 0 || values[len(values)-1] == "" {
			return "", fmt.Errorf("%w: route metadata %s required", ErrEndpointPoolUnavailable, key)
		}
		return values[len(values)-1], nil
	}
	parse := func(key string) (uint64, error) {
		raw, err := value(key)
		if err != nil {
			return 0, err
		}
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || parsed == 0 {
			return 0, fmt.Errorf("%w: invalid route metadata %s", ErrEndpointPoolUnavailable, key)
		}
		return parsed, nil
	}
	realm, err := value("x-farm-actor-realm")
	if err != nil {
		return actorrouting.Address{}, outgoingAssignment{}, err
	}
	kindRaw, err := value("x-farm-actor-kind")
	if err != nil {
		return actorrouting.Address{}, outgoingAssignment{}, err
	}
	kind := actorrouting.Kind(kindRaw)
	if kind != actorrouting.KindPlayer && kind != actorrouting.KindFarm {
		return actorrouting.Address{}, outgoingAssignment{}, fmt.Errorf("%w: invalid route domain", ErrEndpointPoolUnavailable)
	}
	id, err := parse("x-farm-actor-id")
	if err != nil {
		return actorrouting.Address{}, outgoingAssignment{}, err
	}
	shard, err := parse("x-farm-virtual-shard")
	if err != nil {
		// Virtual shard zero is legal; parse it separately.
		raw, rawErr := value("x-farm-virtual-shard")
		if rawErr != nil {
			return actorrouting.Address{}, outgoingAssignment{}, rawErr
		}
		shard, err = strconv.ParseUint(raw, 10, 32)
		if err != nil {
			return actorrouting.Address{}, outgoingAssignment{}, fmt.Errorf("%w: invalid virtual shard", ErrEndpointPoolUnavailable)
		}
	}
	shardRevision, err := parse("x-farm-shard-revision")
	if err != nil {
		return actorrouting.Address{}, outgoingAssignment{}, err
	}
	writerEpoch, err := parse("x-farm-writer-epoch")
	if err != nil {
		return actorrouting.Address{}, outgoingAssignment{}, err
	}
	holder, err := value("x-farm-holder-pod")
	if err != nil {
		return actorrouting.Address{}, outgoingAssignment{}, err
	}
	holderBoot, err := value("x-farm-holder-boot-id")
	if err != nil {
		return actorrouting.Address{}, outgoingAssignment{}, err
	}
	leaseEpoch, err := parse("x-farm-lease-epoch")
	if err != nil {
		return actorrouting.Address{}, outgoingAssignment{}, err
	}
	snapshotRevision, err := parse("x-farm-snapshot-revision")
	if err != nil {
		return actorrouting.Address{}, outgoingAssignment{}, err
	}
	return actorrouting.Address{Realm: realm, Kind: kind, ID: id}, outgoingAssignment{
		shard: shard, shardRevision: shardRevision, writerEpoch: writerEpoch,
		holder: holder, holderBoot: holderBoot, leaseEpoch: leaseEpoch, snapshotRevision: snapshotRevision,
	}, nil
}

type pooledClientStream struct {
	grpc.ClientStream
	release func()
	once    sync.Once
}

func (s *pooledClientStream) RecvMsg(message any) error {
	err := s.ClientStream.RecvMsg(message)
	if err != nil {
		s.once.Do(s.release)
	}
	return err
}

func (s *pooledClientStream) CloseSend() error {
	err := s.ClientStream.CloseSend()
	if err != nil && !errors.Is(err, io.EOF) {
		s.once.Do(s.release)
	}
	return err
}
