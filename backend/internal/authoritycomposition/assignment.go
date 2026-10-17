package authoritycomposition

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
	remoteroute "farmgame/internal/actorrouting/remote"
	"farmgame/internal/routecontrol/transport"
)

// AssignmentMode selects the process-local source of the complete route
// snapshot.  Static is the compatibility default; remote-watch is an
// explicit opt-in and never falls back to a file.
type AssignmentMode string

const (
	AssignmentModeStatic      AssignmentMode = "static"
	AssignmentModeRemoteWatch AssignmentMode = "remote-watch"

	// These are process-local safety ceilings. Deployments may choose smaller
	// values, but environment input cannot turn the route consumer into an
	// unbounded message, queue, retry, or endpoint holder.
	DefaultActorRouteMaxEndpoints = 256
	maxActorRouteEndpoints        = 4096
	maxRemoteMessageBytes         = 4 << 20
	maxRemoteConcurrent           = 64
	maxRemoteWatchBuffer          = 1024
	maxRemoteAssignmentBuffer     = 1024
	maxRemoteRetryAttempts        = 32
	maxRemoteTimeout              = 30 * time.Second
	maxRemoteStartupTimeout       = 5 * time.Minute
	defaultRemoteResyncAttempts   = 32
	defaultRemoteWatchFailures    = 32
	defaultRemoteRetryBackoff     = 100 * time.Millisecond
	defaultRemoteMaxRetryBackoff  = time.Second
	defaultRemoteRetiredBootLimit = 8
	maxRemoteRetiredBootLimit     = 64
)

var (
	ErrAssignmentModeInvalid       = errors.New("authority composition: invalid actor assignment mode")
	ErrAssignmentModeCombination   = errors.New("authority composition: unsupported membership/assignment mode combination")
	ErrRemoteAssignmentComposition = errors.New("authority composition: remote assignment composition is required")
)

// ShardObservation is a point-in-time projection from the exact immutable
// Resolver table used by normal Gate hot routing.
type ShardObservation struct {
	Realm              string
	Domain             actorrouting.RouteDomain
	VirtualShard       uint32
	HolderID           string
	HolderBootID       string
	LogicEndpoint      string
	State              actorrouting.ShardRouteState
	AssignmentRevision uint64
	WriterEpoch        uint64
	LeaseEpoch         uint64
	LeaseUntilMs       int64
	SnapshotRevision   uint64
}

// AssignmentModeFromEnvironment parses ACTOR_ASSIGNMENT_MODE.  The empty
// value deliberately means static so existing deployments keep their exact
// file-backed behavior.  Unlike the historical membership parser this value
// has no aliases: a typo must fail startup.
func AssignmentModeFromEnvironment() (AssignmentMode, error) {
	return assignmentModeFromEnvironment(os.Getenv)
}

// ResolverOptionsFromEnvironment selects how remote data-plane consumers treat
// LeaseUntilMs. Authority-delegated mode is only safe when Holder LeaseGuard
// and Redis Authority remain the final write-admission fence.
func ResolverOptionsFromEnvironment() (actorrouting.ResolverOptions, error) {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("ACTOR_ROUTE_LEASE_CHECK_MODE")))
	if raw == "" {
		raw = string(actorrouting.LeaseCheckSnapshot)
	}
	switch actorrouting.LeaseCheckMode(raw) {
	case actorrouting.LeaseCheckSnapshot, actorrouting.LeaseCheckAuthorityDelegated:
		return actorrouting.ResolverOptions{LeaseCheckMode: actorrouting.LeaseCheckMode(raw)}, nil
	default:
		return actorrouting.ResolverOptions{}, fmt.Errorf("%w: ACTOR_ROUTE_LEASE_CHECK_MODE must be %q or %q, got %q", ErrAssignmentModeInvalid, actorrouting.LeaseCheckSnapshot, actorrouting.LeaseCheckAuthorityDelegated, raw)
	}
}

func assignmentModeFromEnvironment(getenv func(string) string) (AssignmentMode, error) {
	if getenv == nil {
		return "", fmt.Errorf("%w: environment reader required", ErrAssignmentModeInvalid)
	}
	raw := strings.ToLower(strings.TrimSpace(getenv("ACTOR_ASSIGNMENT_MODE")))
	switch raw {
	case "", string(AssignmentModeStatic):
		return AssignmentModeStatic, nil
	case string(AssignmentModeRemoteWatch):
		return AssignmentModeRemoteWatch, nil
	default:
		return "", fmt.Errorf("%w: ACTOR_ASSIGNMENT_MODE must be %q or %q, got %q", ErrAssignmentModeInvalid, AssignmentModeStatic, AssignmentModeRemoteWatch, raw)
	}
}

// ValidateModeCombination freezes the RT2I matrix.  Membership and route
// assignment are orthogonal, except that the legacy renewal path cannot
// consume a remotely changing assignment table.
func ValidateModeCombination(membership MembershipMode, assignment AssignmentMode) error {
	if assignment == "" {
		assignment = AssignmentModeStatic
	}
	switch membership {
	case MembershipModeLegacy, MembershipModeComposed:
	default:
		return fmt.Errorf("%w: membership=%q", ErrAssignmentModeCombination, membership)
	}
	switch assignment {
	case AssignmentModeStatic:
		return nil
	case AssignmentModeRemoteWatch:
		if membership == MembershipModeLegacy {
			return fmt.Errorf("%w: legacy+remote-watch is forbidden", ErrAssignmentModeCombination)
		}
		return nil
	default:
		return fmt.Errorf("%w: assignment=%q", ErrAssignmentModeCombination, assignment)
	}
}

// HolderCompositionEnabled is the sole command-root gate for constructing
// HolderRegistry, HolderAgent, and HolderControl. Composed+static intentionally
// retains the original static authority lifecycle and needs no Holder TLS.
func HolderCompositionEnabled(membership MembershipMode, assignment AssignmentMode) bool {
	return membership == MembershipModeComposed && assignment == AssignmentModeRemoteWatch
}

// AssignmentComposition is the process-local route composition shared by an
// authority runtime and any data-plane consumer in the same process.  The
// provider/resolver/applier are read-only capabilities; Pump is the only
// owner of the remote source and its private installer.
type AssignmentComposition struct {
	Initial        actorrouting.RouteSnapshot
	Provider       *actorrouting.DynamicAssignmentProvider
	Resolver       *actorrouting.StaticResolver
	Applier        *actorrouting.SnapshotApplier
	Pump           *remoteroute.RemoteAssignmentPump
	Endpoints      *EndpointConnectionManager
	leaseCheckMode actorrouting.LeaseCheckMode
	now            func() time.Time

	runMu       sync.Mutex
	running     bool
	closed      bool
	runCancel   context.CancelFunc
	runDone     chan struct{}
	terminalErr error
	closeErr    error
	closeOnce   sync.Once
}

// NewRemoteAssignmentComposition performs the mandatory initial fetch before
// returning.  Transport adapters authenticate and envelope-verify the fetch;
// the existing remote installer performs the shared route schema, topology,
// revision and holder-generation validation.  No validation is reimplemented
// here.
func NewRemoteAssignmentComposition(ctx context.Context, source remoteroute.RemoteSnapshotSource, options remoteroute.Options) (*AssignmentComposition, error) {
	if ctx == nil {
		return nil, actorrouting.ErrContextRequired
	}
	if source == nil {
		return nil, remoteroute.ErrSourceRequired
	}
	initial, err := source.FetchFullSnapshot(ctx)
	if err != nil {
		return nil, fmt.Errorf("authority composition: initial remote route fetch: %w", err)
	}
	maxEndpoints := ActorRouteEndpointLimitFromEnvironment()
	if err := validateSnapshotEndpointLimit(initial, maxEndpoints); err != nil {
		return nil, err
	}
	options = boundedRemoteAssignmentOptions(options)
	provider, installer, err := actorrouting.NewRemoteAssignmentProvider(options.Buffer)
	if err != nil {
		return nil, fmt.Errorf("authority composition: remote route provider: %w", err)
	}
	boundedInstaller := &endpointBoundAssignmentInstaller{inner: installer, maxEndpoints: maxEndpoints}
	if err := boundedInstaller.InstallFullSnapshot(ctx, initial); err != nil {
		return nil, fmt.Errorf("authority composition: install initial remote route snapshot: %w", err)
	}
	resolverOptions, err := ResolverOptionsFromEnvironment()
	if err != nil {
		return nil, fmt.Errorf("authority composition: remote route resolver options: %w", err)
	}
	resolver, err := actorrouting.NewStaticResolverFromProviderWithOptions(ctx, provider, resolverOptions)
	if err != nil {
		return nil, fmt.Errorf("authority composition: remote route resolver: %w", err)
	}
	applier, err := actorrouting.NewSnapshotApplier(provider, resolver)
	if err != nil {
		return nil, fmt.Errorf("authority composition: remote route snapshot applier: %w", err)
	}
	if err := applier.Bootstrap(ctx); err != nil {
		return nil, fmt.Errorf("authority composition: remote route snapshot bootstrap: %w", err)
	}

	// Pump.Run has its own mandatory initial fetch.  Cache the already verified
	// fetch for that one call so startup performs one source operation and the
	// same source/client remains the sole owner after startup.
	cached := &firstFetchSource{initial: actorrouting.CloneRouteSnapshot(initial), source: source}
	pump, err := remoteroute.NewRemoteAssignmentPump(cached, boundedInstaller, options)
	if err != nil {
		return nil, fmt.Errorf("authority composition: remote route pump: %w", err)
	}
	return &AssignmentComposition{
		Initial:        actorrouting.CloneRouteSnapshot(initial),
		Provider:       provider,
		Resolver:       resolver,
		Applier:        applier,
		Pump:           pump,
		leaseCheckMode: resolverOptions.LeaseCheckMode,
		now:            time.Now,
		runDone:        make(chan struct{}),
	}, nil
}

// ConfigureEndpointConnections installs the one process-local data-plane
// endpoint manager before Run. It does not dial until Run, allowing holder
// roots to bind and start their business listener before pre-dialing a
// snapshot that may contain the process itself.
func (c *AssignmentComposition) ConfigureEndpointConnections(options EndpointConnectionOptions) error {
	if c == nil || c.Provider == nil {
		return ErrRemoteAssignmentComposition
	}
	c.runMu.Lock()
	defer c.runMu.Unlock()
	if c.running || c.closed || c.Endpoints != nil {
		return fmt.Errorf("%w: endpoint pool must be configured exactly once before Run", ErrRemoteAssignmentComposition)
	}
	manager, err := NewEndpointConnectionManager(c.Provider, options)
	if err != nil {
		return err
	}
	c.Endpoints = manager
	return nil
}

// Run owns and joins the Pump, resolver Applier, and endpoint manager. Any
// terminal exit cancels the siblings and atomically degrades the composition;
// ordinary RT2B watch disconnects remain internal to Pump.Run.
func (c *AssignmentComposition) Run(ctx context.Context) error {
	if c == nil || c.Pump == nil || c.Applier == nil || c.Endpoints == nil {
		return ErrRemoteAssignmentComposition
	}
	if ctx == nil {
		return actorrouting.ErrContextRequired
	}
	c.runMu.Lock()
	if c.closed {
		c.runMu.Unlock()
		return nil
	}
	if c.running {
		c.runMu.Unlock()
		return fmt.Errorf("%w: composition already running", ErrRemoteAssignmentComposition)
	}
	runCtx, cancel := context.WithCancel(ctx)
	c.running = true
	c.runCancel = cancel
	c.runMu.Unlock()

	type runResult struct {
		component string
		err       error
	}
	results := make(chan runResult, 3)
	var wg sync.WaitGroup
	start := func(component string, run func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- runResult{component: component, err: run(runCtx)}
		}()
	}
	start("Pump", c.Pump.Run)
	start("Applier", c.Applier.Run)
	start("endpoint pool", c.Endpoints.Run)

	result := <-results
	terminal := result.err
	if terminal == nil && runCtx.Err() == nil {
		terminal = fmt.Errorf("authority composition: %s exited without cancellation", result.component)
	}
	if terminal != nil && runCtx.Err() == nil {
		terminal = fmt.Errorf("authority composition: %s terminal: %w", result.component, terminal)
		c.runMu.Lock()
		if c.terminalErr == nil {
			c.terminalErr = terminal
		}
		c.runMu.Unlock()
	}
	cancel()
	_ = c.Pump.Close()
	_ = c.Endpoints.Close()
	wg.Wait()
	c.runMu.Lock()
	c.running = false
	c.runCancel = nil
	select {
	case <-c.runDone:
	default:
		close(c.runDone)
	}
	c.runMu.Unlock()
	return terminal
}

// Healthy is a local, zero-RPC control-plane readiness check. It requires the
// Pump to be in healthy WATCHING state and the provider, resolver/applier, and
// endpoint pool to have installed one identical complete snapshot generation.
// Per-shard state and lease validity are deliberately excluded: Resolve and
// the local HolderLeaseGuard fail closed for the affected shard. Coupling one
// expired or ASSIGNING shard to process readiness would withdraw every healthy
// shard behind this process (and every process observing the same snapshot).
// Connection-state transients are also excluded because a Holder Agent treats
// process-readiness regression as terminal.
func (c *AssignmentComposition) LeaseCheckMode() actorrouting.LeaseCheckMode {
	if c == nil || c.leaseCheckMode == "" {
		return actorrouting.LeaseCheckSnapshot
	}
	return c.leaseCheckMode
}

func (c *AssignmentComposition) LeaseAuthorityDelegated() bool {
	return c.LeaseCheckMode() == actorrouting.LeaseCheckAuthorityDelegated
}

func (c *AssignmentComposition) Healthy() bool {
	if c == nil || c.Provider == nil || c.Resolver == nil || c.Applier == nil || c.Pump == nil || c.Endpoints == nil {
		return false
	}
	c.runMu.Lock()
	running := c.running && !c.closed && c.terminalErr == nil
	c.runMu.Unlock()
	if !running || c.Pump.State() != remoteroute.StateWatching {
		return false
	}
	revision := c.Provider.CurrentRevision()
	if revision == 0 || c.Applier.LastGlobalRevision() != revision {
		return false
	}
	if c.Resolver.CurrentRevision() != revision {
		return false
	}
	if !c.Endpoints.ReadyForRevision(revision) {
		return false
	}
	// A publication may race the individual local observations above. Re-read
	// the cheap revision markers so Healthy never combines two generations.
	return c.Provider.CurrentRevision() == revision &&
		c.Applier.LastGlobalRevision() == revision &&
		c.Resolver.CurrentRevision() == revision &&
		c.Endpoints.ReadyForRevision(revision)
}

// AppliedSnapshotRevision reports the newest complete RouteSnapshot revision
// installed in the local resolver/applier pair. It deliberately does not
// require the provider to equal that revision: while a later snapshot is being
// installed, the previous complete revision remains valid per-shard evidence.
// ProcessReady continues to use Healthy, so a process in this transient state
// is never eligible for fresh placement.
func (c *AssignmentComposition) AppliedSnapshotRevision() uint64 {
	if c == nil || c.Provider == nil || c.Resolver == nil || c.Applier == nil || c.Pump == nil {
		return 0
	}
	c.runMu.Lock()
	running := c.running && !c.closed && c.terminalErr == nil
	c.runMu.Unlock()
	if !running || c.Pump.State() != remoteroute.StateWatching {
		return 0
	}
	resolvedRevision := c.Resolver.CurrentRevision()
	if resolvedRevision == 0 || c.Applier.LastGlobalRevision() != resolvedRevision {
		return 0
	}
	if c.Provider.CurrentRevision() < resolvedRevision {
		return 0
	}
	return resolvedRevision
}

// AppliedSnapshotRevisionForHolder brackets AppliedSnapshotRevision with the
// number of shards that same revision assigns to one local holder generation,
// so a caller can never pair a revision with a shard count taken from another
// revision. A resolver that moves between the two reads yields (0, 0): there is
// no consistent local evidence for this observation.
func (c *AssignmentComposition) AppliedSnapshotRevisionForHolder(domain actorrouting.RouteDomain, holderID, bootID string) (uint64, int) {
	if c == nil || c.Resolver == nil || holderID == "" || bootID == "" {
		return 0, 0
	}
	revision := c.AppliedSnapshotRevision()
	if revision == 0 {
		return 0, 0
	}
	observedRevision, count := c.Resolver.CurrentRevisionForHolder(domain, holderID, bootID)
	if observedRevision != revision {
		return 0, 0
	}
	return revision, count
}

// DataPlaneHealthy adds exact-generation gRPC connection readiness for pooled
// business consumers. Unlike Healthy, it may transiently fail closed during a
// complete-generation install without poisoning Holder Agent liveness.
func (c *AssignmentComposition) DataPlaneHealthy() bool {
	if !c.Healthy() {
		return false
	}
	revision := c.Provider.CurrentRevision()
	if revision == 0 || c.Pump.CurrentRevision() != revision ||
		c.Applier.LastGlobalRevision() != revision || c.Resolver.CurrentRevision() != revision ||
		!c.Endpoints.ConnectionsReadyForRevision(revision) {
		return false
	}
	return c.Healthy() && c.Pump.State() == remoteroute.StateWatching &&
		c.Provider.CurrentRevision() == revision &&
		c.Pump.CurrentRevision() == revision &&
		c.Applier.LastGlobalRevision() == revision &&
		c.Resolver.CurrentRevision() == revision
}

// ObserveShard brackets one atomic Resolver snapshot with full composition
// health checks and requires the same global revision afterward. It never
// invokes a remote source and does not alter normal Resolve.
func (c *AssignmentComposition) ObserveShard(ctx context.Context, realm string, domain actorrouting.RouteDomain, shard uint32) (ShardObservation, error) {
	if c == nil || ctx == nil || c.Resolver == nil || realm == "" ||
		(domain != actorrouting.DomainPlayer &&
			domain != actorrouting.DomainFarm &&
			domain != actorrouting.DomainFarmWorker) {
		return ShardObservation{}, ErrRemoteAssignmentComposition
	}
	if err := ctx.Err(); err != nil {
		return ShardObservation{}, err
	}
	if !c.Healthy() {
		return ShardObservation{}, ErrRemoteAssignmentComposition
	}
	snapshot := c.Resolver.Snapshot()
	if snapshot.Realm != realm || snapshot.GlobalRevision == 0 {
		return ShardObservation{}, ErrRemoteAssignmentComposition
	}
	table := snapshot.Player
	switch domain {
	case actorrouting.DomainFarm:
		table = snapshot.Farm
	case actorrouting.DomainFarmWorker:
		table = snapshot.FarmWorker
	}
	if shard >= table.ShardCount || int(shard) >= len(table.Routes) {
		return ShardObservation{}, ErrRemoteAssignmentComposition
	}
	route := table.Routes[shard]
	now := time.Now
	if c.now != nil {
		now = c.now
	}
	if route.State != actorrouting.ShardStateActive || route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) ||
		route.ShardRevision == 0 || route.ExpectedWriterEpoch == 0 || route.ExpectedLeaseEpoch == 0 || route.LeaseUntilMs <= 0 {
		return ShardObservation{}, ErrRemoteAssignmentComposition
	}
	if !c.LeaseAuthorityDelegated() && route.LeaseUntilMs <= now().UnixMilli() {
		return ShardObservation{}, ErrRemoteAssignmentComposition
	}
	holder := snapshot.Holders[route.HolderIndex]
	if holder.InstanceID == "" || holder.BootID == "" || holder.Endpoint == "" || holder.State != actorrouting.HolderStateActive {
		return ShardObservation{}, ErrRemoteAssignmentComposition
	}
	if err := ctx.Err(); err != nil {
		return ShardObservation{}, err
	}
	if !c.Healthy() || c.Resolver.Snapshot().GlobalRevision != snapshot.GlobalRevision ||
		(!c.LeaseAuthorityDelegated() && route.LeaseUntilMs <= now().UnixMilli()) {
		return ShardObservation{}, ErrRemoteAssignmentComposition
	}
	return ShardObservation{
		Realm: snapshot.Realm, Domain: domain, VirtualShard: shard,
		HolderID: holder.InstanceID, HolderBootID: holder.BootID, LogicEndpoint: holder.Endpoint,
		State: route.State, AssignmentRevision: route.ShardRevision, WriterEpoch: route.ExpectedWriterEpoch,
		LeaseEpoch: route.ExpectedLeaseEpoch, LeaseUntilMs: route.LeaseUntilMs, SnapshotRevision: snapshot.GlobalRevision,
	}, nil
}

func (c *AssignmentComposition) WaitHealthy(ctx context.Context) error {
	if c == nil || ctx == nil {
		return actorrouting.ErrContextRequired
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if c.Healthy() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.Done():
			if err := c.TerminalError(); err != nil {
				return err
			}
			return ErrRemoteAssignmentComposition
		case <-ticker.C:
		}
	}
}

func (c *AssignmentComposition) TerminalError() error {
	if c == nil {
		return nil
	}
	c.runMu.Lock()
	defer c.runMu.Unlock()
	return c.terminalErr
}

func (c *AssignmentComposition) Done() <-chan struct{} {
	if c == nil || c.runDone == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return c.runDone
}

// Close cancels and boundedly joins all process-local assignment goroutines.
// The authenticated transport client remains command-root owned.
func (c *AssignmentComposition) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		var closeErr error
		c.runMu.Lock()
		c.closed = true
		cancel := c.runCancel
		done := c.runDone
		running := c.running
		c.runMu.Unlock()
		if cancel != nil {
			cancel()
		}
		if c.Pump != nil {
			closeErr = c.Pump.Close()
		}
		if c.Endpoints != nil {
			if err := c.Endpoints.Close(); closeErr == nil {
				closeErr = err
			}
		}
		if running {
			timer := time.NewTimer(maxRemoteTimeout)
			defer timer.Stop()
			select {
			case <-done:
			case <-timer.C:
				if closeErr == nil {
					closeErr = errors.New("authority composition: timed out joining assignment goroutines")
				}
			}
		} else {
			c.runMu.Lock()
			select {
			case <-c.runDone:
			default:
				close(c.runDone)
			}
			c.runMu.Unlock()
		}
		c.runMu.Lock()
		c.closeErr = closeErr
		c.runMu.Unlock()
	})
	c.runMu.Lock()
	defer c.runMu.Unlock()
	return c.closeErr
}

// NewRemoteSnapshotClientFromEnvironment is the shared command-root adapter
// for the authenticated route-control source. It reads only file paths and
// non-secret identities from the environment; certificate/key/CA contents
// are loaded by transport and are never logged here.
func NewRemoteSnapshotClientFromEnvironment(ctx context.Context) (*transport.SnapshotClient, error) {
	if ctx == nil {
		return nil, actorrouting.ErrContextRequired
	}
	retiredBootLimit, err := remoteRetiredBootLimitFromEnvironment(os.LookupEnv)
	if err != nil {
		return nil, err
	}
	tlsConfig, err := transport.LoadClientTLSConfig(
		requiredRemoteEnv("ROUTE_CONTROL_CLIENT_CERT_FILE"),
		requiredRemoteEnv("ROUTE_CONTROL_CLIENT_KEY_FILE"),
		requiredRemoteEnv("ROUTE_CONTROL_SERVER_CA_FILE"),
	)
	if err != nil {
		return nil, fmt.Errorf("authority composition: load route-control client TLS files: %w", err)
	}
	client, err := transport.DialRouteSnapshot(ctx, requiredRemoteEnv("ROUTE_CONTROL_ADDR"), tlsConfig, transport.ClientOptions{
		ExpectedControllerID:   requiredRemoteEnv("ROUTE_CONTROL_CONTROLLER_ID"),
		ExpectedRealm:          strings.TrimSpace(os.Getenv("ACTOR_ROUTE_REALM")),
		ExpectedTopology:       strings.TrimSpace(os.Getenv("ACTOR_ROUTE_TOPOLOGY")),
		ServerName:             requiredRemoteEnv("ROUTE_CONTROL_SERVER_NAME"),
		ExpectedServerIdentity: requiredRemoteEnv("ROUTE_CONTROL_SERVER_IDENTITY"),
		MaxMessageBytes:        envPositiveIntBounded("ROUTE_CONTROL_MAX_MESSAGE_BYTES", 4<<20, maxRemoteMessageBytes),
		MaxConcurrent:          envPositiveIntBounded("ROUTE_CONTROL_MAX_CONCURRENT", 8, maxRemoteConcurrent),
		WatchBuffer:            envPositiveIntBounded("ROUTE_CONTROL_WATCH_BUFFER", 8, maxRemoteWatchBuffer),
		CallTimeout:            envPositiveDurationBounded("ROUTE_CONTROL_RPC_TIMEOUT", 5*time.Second, maxRemoteTimeout),
		RetiredBootLimit:       retiredBootLimit,
	})
	if err != nil {
		return nil, fmt.Errorf("authority composition: dial route-control snapshot source: %w", err)
	}
	return client, nil
}

func remoteRetiredBootLimitFromEnvironment(lookup func(string) (string, bool)) (int, error) {
	const key = "ROUTE_CONTROL_RETIRED_BOOT_LIMIT"
	if lookup == nil {
		return 0, errors.New("authority composition: environment lookup is required")
	}
	raw, present := lookup(key)
	if !present {
		return defaultRemoteRetiredBootLimit, nil
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value < 1 || value > maxRemoteRetiredBootLimit {
		return 0, fmt.Errorf("authority composition: %s must be an integer within [1,%d], got %q", key, maxRemoteRetiredBootLimit, raw)
	}
	return value, nil
}

func RemoteAssignmentOptionsFromEnvironment() remoteroute.Options {
	return remoteroute.Options{
		Buffer:            envPositiveIntBounded("ACTOR_ROUTE_WATCH_BUFFER", 64, maxRemoteAssignmentBuffer),
		MaxResyncAttempts: envPositiveIntBounded("ACTOR_ROUTE_MAX_RESYNC_ATTEMPTS", defaultRemoteResyncAttempts, maxRemoteRetryAttempts),
		MaxWatchFailures:  envPositiveIntBounded("ACTOR_ROUTE_MAX_WATCH_FAILURES", defaultRemoteWatchFailures, maxRemoteRetryAttempts),
		RetryBackoff:      envPositiveDurationBounded("ACTOR_ROUTE_RETRY_BACKOFF", defaultRemoteRetryBackoff, maxRemoteTimeout),
		MaxRetryBackoff:   envPositiveDurationBounded("ACTOR_ROUTE_MAX_RETRY_BACKOFF", defaultRemoteMaxRetryBackoff, maxRemoteTimeout),
		// A running Holder must not permanently lose RouteControl merely
		// because a controller rollout or outage exceeds one retry burst.
		// Initial bootstrap remains bounded by the command-root startup
		// timeout, while established streams reconnect until shutdown.
		ContinuousReconnect: true,
	}
}

// RemoteDialTimeoutFromEnvironment and RemoteInitialFetchTimeoutFromEnvironment
// keep every command root on the same bounded startup contract.
func RemoteDialTimeoutFromEnvironment() time.Duration {
	return envPositiveDurationRange("ROUTE_CONTROL_DIAL_TIMEOUT", 5*time.Second, time.Millisecond, maxRemoteTimeout)
}

func RemoteInitialFetchTimeoutFromEnvironment() time.Duration {
	return envPositiveDurationRange("ACTOR_ROUTE_INITIAL_FETCH_TIMEOUT", 5*time.Second, time.Millisecond, maxRemoteTimeout)
}

func RemoteStartupTimeoutFromEnvironment() time.Duration {
	return envPositiveDurationRange("ACTOR_ROUTE_STARTUP_TIMEOUT", 30*time.Second, time.Millisecond, maxRemoteStartupTimeout)
}

func ActorRouteEndpointLimitFromEnvironment() int {
	value := envPositiveIntBounded("ACTOR_ROUTE_MAX_ENDPOINTS", DefaultActorRouteMaxEndpoints, maxActorRouteEndpoints)
	if value <= 0 {
		return DefaultActorRouteMaxEndpoints
	}
	return value
}

func boundedRemoteAssignmentOptions(options remoteroute.Options) remoteroute.Options {
	if options.Buffer > maxRemoteAssignmentBuffer {
		options.Buffer = maxRemoteAssignmentBuffer
	}
	if options.MaxResyncAttempts > maxRemoteRetryAttempts {
		options.MaxResyncAttempts = maxRemoteRetryAttempts
	}
	if options.MaxWatchFailures > maxRemoteRetryAttempts {
		options.MaxWatchFailures = maxRemoteRetryAttempts
	}
	if options.RetryBackoff > maxRemoteTimeout {
		options.RetryBackoff = maxRemoteTimeout
	}
	if options.MaxRetryBackoff > maxRemoteTimeout {
		options.MaxRetryBackoff = maxRemoteTimeout
	}
	return options
}

func validateSnapshotEndpointLimit(snapshot actorrouting.RouteSnapshot, maxEndpoints int) error {
	if maxEndpoints <= 0 {
		maxEndpoints = DefaultActorRouteMaxEndpoints
	}
	seen := make(map[string]struct{}, len(snapshot.Holders))
	for _, holder := range snapshot.Holders {
		seen[holder.Endpoint] = struct{}{}
	}
	if len(seen) > maxEndpoints {
		return fmt.Errorf("authority composition: route snapshot has %d endpoints, exceeds ACTOR_ROUTE_MAX_ENDPOINTS=%d", len(seen), maxEndpoints)
	}
	return nil
}

type endpointBoundAssignmentInstaller struct {
	inner        actorrouting.RemoteAssignmentInstaller
	maxEndpoints int
}

func (i *endpointBoundAssignmentInstaller) InstallFullSnapshot(ctx context.Context, snapshot actorrouting.RouteSnapshot) error {
	if i == nil || i.inner == nil {
		return errors.New("authority composition: remote assignment installer is required")
	}
	if err := validateSnapshotEndpointLimit(snapshot, i.maxEndpoints); err != nil {
		return err
	}
	return i.inner.InstallFullSnapshot(ctx, snapshot)
}

func requiredRemoteEnv(key string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return ""
	}
	return value
}

func envPositiveDuration(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envPositiveIntBounded(key string, fallback, maximum int) int {
	return envPositiveIntBoundedWithGetter(os.Getenv, key, fallback, maximum)
}

func envPositiveIntBoundedWithGetter(getenv func(string) string, key string, fallback, maximum int) int {
	if getenv == nil {
		return fallback
	}
	value, err := strconv.Atoi(strings.TrimSpace(getenv(key)))
	if err != nil || value <= 0 {
		value = fallback
	}
	if maximum > 0 && value > maximum {
		return maximum
	}
	return value
}

func envPositiveDurationBounded(key string, fallback, maximum time.Duration) time.Duration {
	value := envPositiveDuration(key, fallback)
	if maximum > 0 && value > maximum {
		return maximum
	}
	return value
}

func envPositiveDurationRange(key string, fallback, minimum, maximum time.Duration) time.Duration {
	value := envPositiveDuration(key, fallback)
	if minimum > 0 && value < minimum {
		return minimum
	}
	if maximum > 0 && value > maximum {
		return maximum
	}
	return value
}

type firstFetchSource struct {
	initial actorrouting.RouteSnapshot
	source  remoteroute.RemoteSnapshotSource
	mu      sync.Mutex
	used    bool
}

func (s *firstFetchSource) FetchFullSnapshot(ctx context.Context) (actorrouting.RouteSnapshot, error) {
	if s == nil || s.source == nil {
		return actorrouting.RouteSnapshot{}, remoteroute.ErrSourceRequired
	}
	s.mu.Lock()
	if !s.used {
		s.used = true
		initial := actorrouting.CloneRouteSnapshot(s.initial)
		s.mu.Unlock()
		if ctx == nil {
			return actorrouting.RouteSnapshot{}, actorrouting.ErrContextRequired
		}
		if err := ctx.Err(); err != nil {
			return actorrouting.RouteSnapshot{}, err
		}
		return initial, nil
	}
	s.mu.Unlock()
	return s.source.FetchFullSnapshot(ctx)
}

func (s *firstFetchSource) WatchFrom(ctx context.Context, revision uint64) (<-chan actorrouting.RouteSnapshot, error) {
	if s == nil || s.source == nil {
		return nil, remoteroute.ErrSourceRequired
	}
	return s.source.WatchFrom(ctx, revision)
}

func (s *firstFetchSource) FetchFullSnapshotForResync(ctx context.Context, baseline remoteroute.ResyncBaseline) (actorrouting.RouteSnapshot, error) {
	if s == nil || s.source == nil {
		return actorrouting.RouteSnapshot{}, remoteroute.ErrSourceRequired
	}
	if source, ok := s.source.(remoteroute.RestartAwareRemoteSnapshotSource); ok {
		return source.FetchFullSnapshotForResync(ctx, baseline)
	}
	return s.source.FetchFullSnapshot(ctx)
}
