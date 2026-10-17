package authoritycomposition

import (
	"context"
	"errors"
	"farmgame/internal/controlfence"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/actorrouting"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type endpointPoolTestConn struct {
	closed    chan struct{}
	closeOnce sync.Once
	invokes   atomic.Int32
	connects  atomic.Int32
	state     atomic.Int32
}

func newEndpointPoolTestConn() *endpointPoolTestConn {
	connection := &endpointPoolTestConn{closed: make(chan struct{})}
	connection.state.Store(int32(connectivity.Ready))
	return connection
}

func (c *endpointPoolTestConn) GetState() connectivity.State {
	return connectivity.State(c.state.Load())
}

func (c *endpointPoolTestConn) Connect() {
	c.connects.Add(1)
	c.state.CompareAndSwap(int32(connectivity.Idle), int32(connectivity.Ready))
}

func (c *endpointPoolTestConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	c.invokes.Add(1)
	return nil
}

func (*endpointPoolTestConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("test stream is not implemented")
}

func TestActorEndpointPoolExcludesFarmWorkerEndpoints(t *testing.T) {
	fixture := newCompositionFixture(t)
	snapshot := actorrouting.CloneRouteSnapshot(fixture.snap)
	workerIndex := len(snapshot.Holders)
	snapshot.Holders = append(snapshot.Holders, actorrouting.HolderRef{
		InstanceID: "farm-worker-a",
		BootID:     "worker-a-boot",
		Endpoint:   "farm-worker-a:9201",
		State:      actorrouting.HolderStateActive,
		Weight:     1,
	})
	snapshot.FarmWorker = actorrouting.DomainSnapshot{
		ShardCount: snapshot.Farm.ShardCount,
		Routes:     append([]actorrouting.ShardRoute(nil), snapshot.Farm.Routes...),
	}
	for index := range snapshot.FarmWorker.Routes {
		snapshot.FarmWorker.Routes[index].HolderIndex = workerIndex
	}
	endpoints, err := canonicalSnapshotEndpoints(snapshot, DefaultActorRouteMaxEndpoints)
	if err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range endpoints {
		if endpoint == "farm-worker-a:9201" {
			t.Fatalf("FARM_WORKER endpoint entered actor gRPC pool: %v", endpoints)
		}
	}
	if len(endpoints) != len(fixture.snap.Holders) {
		t.Fatalf("actor endpoints=%v holders=%v", endpoints, fixture.snap.Holders)
	}
}

func TestActorEndpointPoolRejectsHolderSharedWithFarmWorker(t *testing.T) {
	fixture := newCompositionFixture(t)
	snapshot := actorrouting.CloneRouteSnapshot(fixture.snap)
	snapshot.FarmWorker = actorrouting.DomainSnapshot{
		ShardCount: snapshot.Farm.ShardCount,
		Routes:     append([]actorrouting.ShardRoute(nil), snapshot.Farm.Routes...),
	}
	for index := range snapshot.FarmWorker.Routes {
		snapshot.FarmWorker.Routes[index].HolderIndex = snapshot.Player.Routes[0].HolderIndex
	}
	if _, err := canonicalSnapshotEndpoints(
		snapshot,
		DefaultActorRouteMaxEndpoints,
	); !errors.Is(err, ErrEndpointPoolUnavailable) {
		t.Fatalf("shared actor/FARM_WORKER holder error=%v", err)
	}
}

func TestPooledClientHotPathUsesPredialedGeneration(t *testing.T) {
	fixture := newCompositionFixture(t)
	provider, _, err := actorrouting.NewDynamicAssignmentProvider(fixture.snap, 4)
	if err != nil {
		t.Fatal(err)
	}
	var connections sync.Map
	var dials atomic.Int32
	manager, err := NewEndpointConnectionManager(provider, EndpointConnectionOptions{
		MaxEndpoints: 4, DialTimeout: time.Second, RetireGrace: time.Second,
		Dial: func(_ context.Context, endpoint string) (ManagedEndpointConnection, error) {
			dials.Add(1)
			connection := newEndpointPoolTestConn()
			connections.Store(endpoint, connection)
			return connection, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- manager.Run(ctx) }()
	waitEndpointPoolCondition(t, func() bool { return manager.ReadyFor(fixture.snap) }, "predialed generation")
	if !manager.ReadyForRevision(fixture.snap.GlobalRevision) || manager.ReadyForRevision(fixture.snap.GlobalRevision+1) {
		t.Fatal("revision-only endpoint readiness did not pin the installed generation")
	}
	if !manager.ConnectionsReady() {
		t.Fatal("predialed connection generation is not READY")
	}
	var changed *endpointPoolTestConn
	connections.Range(func(_, value any) bool {
		changed = value.(*endpointPoolTestConn)
		return false
	})
	changed.state.Store(int32(connectivity.TransientFailure))
	if manager.ConnectionsReady() {
		t.Fatal("transient connection generation reported READY")
	}
	changed.state.Store(int32(connectivity.Idle))
	if manager.ConnectionsReady() {
		t.Fatal("idle connection generation reported READY")
	}
	if changed.connects.Load() != 1 {
		t.Fatalf("idle connection Connect calls=%d want=1", changed.connects.Load())
	}
	if !manager.ConnectionsReady() {
		t.Fatal("woken connection generation is not READY")
	}
	if !manager.ConnectionsReadyFor(fixture.snap) {
		t.Fatal("exact Snapshot connection generation is not READY")
	}
	if !manager.ConnectionsReadyForRevision(fixture.snap.GlobalRevision) || manager.ConnectionsReadyForRevision(fixture.snap.GlobalRevision+1) {
		t.Fatal("revision-only connection readiness did not pin the installed generation")
	}

	resolver, err := actorrouting.NewStaticResolverFromProvider(context.Background(), provider)
	if err != nil {
		t.Fatal(err)
	}
	pooledResolver, err := NewPooledResolver(resolver, manager)
	if err != nil {
		t.Fatal(err)
	}
	pooledConn, err := NewPooledClientConn(resolver, manager)
	if err != nil {
		t.Fatal(err)
	}
	address := actorrouting.Address{Realm: fixture.snap.Realm, Kind: actorrouting.KindPlayer, ID: 42}
	assignment, err := pooledResolver.Resolve(context.Background(), address)
	if err != nil {
		t.Fatal(err)
	}
	if assignment.Endpoint != PooledEndpoint {
		t.Fatalf("pooled endpoint=%q", assignment.Endpoint)
	}
	requestCtx := actorrouting.WithRoute(context.Background(), address, assignment)
	if err := pooledConn.Invoke(requestCtx, "/test.Service/Unary", nil, nil); err != nil {
		t.Fatal(err)
	}
	if got, want := dials.Load(), int32(len(fixture.snap.Holders)); got != want {
		t.Fatalf("dial count after request=%d want startup-only %d", got, want)
	}
	realAssignment, err := resolver.Resolve(context.Background(), address)
	if err != nil {
		t.Fatal(err)
	}
	selectedRaw, ok := connections.Load(realAssignment.Endpoint)
	if !ok {
		t.Fatalf("selected endpoint %q was not predialed", realAssignment.Endpoint)
	}
	if got := selectedRaw.(*endpointPoolTestConn).invokes.Load(); got != 1 {
		t.Fatalf("selected pooled connection invokes=%d want 1", got)
	}

	cancel()
	_ = manager.Close()
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("pool Run did not join")
	}
}

func TestEndpointPoolDeadlineOnlyRevisionReusesTransportGeneration(t *testing.T) {
	fixture := newCompositionFixture(t)
	provider, publisher, err := actorrouting.NewDynamicAssignmentProvider(fixture.snap, 4)
	if err != nil {
		t.Fatal(err)
	}
	var dials atomic.Int32
	manager, err := NewEndpointConnectionManager(provider, EndpointConnectionOptions{
		MaxEndpoints: 4, DialTimeout: time.Second, RetireGrace: time.Second,
		Dial: func(_ context.Context, _ string) (ManagedEndpointConnection, error) {
			dials.Add(1)
			return newEndpointPoolTestConn(), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer manager.Close()
	runDone := make(chan error, 1)
	go func() { runDone <- manager.Run(ctx) }()
	waitEndpointPoolCondition(t, func() bool { return manager.ReadyFor(fixture.snap) }, "initial endpoint generation")

	manager.mu.Lock()
	beforeGeneration := manager.current.transportGeneration
	beforeEntries := make(map[string]*endpointConnection, len(manager.current.entries))
	for endpoint, entry := range manager.current.entries {
		beforeEntries[endpoint] = entry
	}
	manager.mu.Unlock()
	beforeDials := dials.Load()

	next := actorrouting.CloneRouteSnapshot(fixture.snap)
	next.GlobalRevision++
	next.Player.Routes[0].LeaseUntilMs++
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, next); err != nil {
		t.Fatal(err)
	}
	waitEndpointPoolCondition(t, func() bool { return manager.ReadyFor(next) }, "deadline-only endpoint observation")

	manager.mu.Lock()
	afterGeneration := manager.current.transportGeneration
	for endpoint, before := range beforeEntries {
		if manager.current.entries[endpoint] != before {
			manager.mu.Unlock()
			t.Fatalf("deadline-only update replaced endpoint %q connection", endpoint)
		}
	}
	manager.mu.Unlock()
	if afterGeneration != beforeGeneration {
		t.Fatalf("deadline-only update changed transport generation %d -> %d", beforeGeneration, afterGeneration)
	}
	if got := dials.Load(); got != beforeDials {
		t.Fatalf("deadline-only update dial count=%d want=%d", got, beforeDials)
	}

	select {
	case err := <-runDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("pool terminated after deadline-only update: %v", err)
		}
	default:
	}
}

func (c *endpointPoolTestConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func TestPooledResolverAcceptsIndependentRevisionOrdering(t *testing.T) {
	fixture := newCompositionFixture(t)
	provider, publisher, err := actorrouting.NewDynamicAssignmentProvider(fixture.snap, 4)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewEndpointConnectionManager(provider, EndpointConnectionOptions{
		MaxEndpoints: 4, DialTimeout: time.Second, RetireGrace: time.Second,
		Dial: func(_ context.Context, _ string) (ManagedEndpointConnection, error) {
			return newEndpointPoolTestConn(), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer manager.Close()
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	waitEndpointPoolCondition(t, func() bool { return manager.ReadyFor(fixture.snap) }, "revision ordering initial generation")

	resolverAhead, err := actorrouting.NewStaticResolver(fixture.snap)
	if err != nil {
		t.Fatal(err)
	}
	pooledAhead, err := NewPooledResolver(resolverAhead, manager)
	if err != nil {
		t.Fatal(err)
	}
	address := actorrouting.Address{Realm: fixture.snap.Realm, Kind: actorrouting.KindPlayer, ID: 42}
	next := actorrouting.CloneRouteSnapshot(fixture.snap)
	next.GlobalRevision++
	next.Player.Routes[0].LeaseUntilMs++

	// Resolver observes the deadline-only revision first; the pool still has
	// the old observation but the physical EndpointKey is already present.
	if err := resolverAhead.Replace(next); err != nil {
		t.Fatal(err)
	}
	assignment, err := pooledAhead.Resolve(context.Background(), address)
	if err != nil || assignment.Endpoint != PooledEndpoint || assignment.SnapshotRevision != next.GlobalRevision {
		t.Fatalf("resolver-ahead ordering failed: assignment=%+v err=%v", assignment, err)
	}

	// Pool observes the new revision first; a separate resolver still returns
	// the old route and must continue using the same physical transport.
	resolverBehind, err := actorrouting.NewStaticResolver(fixture.snap)
	if err != nil {
		t.Fatal(err)
	}
	pooledBehind, err := NewPooledResolver(resolverBehind, manager)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, next); err != nil {
		t.Fatal(err)
	}
	waitEndpointPoolCondition(t, func() bool { return manager.ReadyFor(next) }, "pool-ahead revision ordering")
	assignment, err = pooledBehind.Resolve(context.Background(), address)
	if err != nil || assignment.Endpoint != PooledEndpoint || assignment.SnapshotRevision != fixture.snap.GlobalRevision {
		t.Fatalf("pool-ahead ordering failed: assignment=%+v err=%v", assignment, err)
	}
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("pool terminated after revision ordering: %v", err)
		}
	default:
	}
}

func TestEndpointPoolHolderBootChangeReplacesSameEndpointTransport(t *testing.T) {
	fixture := newCompositionFixture(t)
	provider, publisher, err := actorrouting.NewDynamicAssignmentProvider(fixture.snap, 4)
	if err != nil {
		t.Fatal(err)
	}
	var dials atomic.Int32
	var dialedMu sync.Mutex
	var playerConnections []*endpointPoolTestConn
	manager, err := NewEndpointConnectionManager(provider, EndpointConnectionOptions{
		MaxEndpoints: 4, DialTimeout: time.Second, RetireGrace: time.Second,
		Dial: func(_ context.Context, endpoint string) (ManagedEndpointConnection, error) {
			dials.Add(1)
			conn := newEndpointPoolTestConn()
			if endpoint == "player.local:9100" {
				dialedMu.Lock()
				playerConnections = append(playerConnections, conn)
				dialedMu.Unlock()
			}
			return conn, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer manager.Close()
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	waitEndpointPoolCondition(t, func() bool { return manager.ReadyFor(fixture.snap) }, "initial boot generation")
	manager.mu.Lock()
	beforeGeneration := manager.current.transportGeneration
	manager.mu.Unlock()
	beforeDials := dials.Load()

	next := actorrouting.CloneRouteSnapshot(fixture.snap)
	next.GlobalRevision++
	next.Holders[0].BootID = "player-boot-rotated"
	next.Player.Routes[0].ShardRevision++
	next.Player.Routes[0].ExpectedWriterEpoch++
	next.Player.Routes[0].ExpectedLeaseEpoch++
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, next); err != nil {
		t.Fatal(err)
	}
	waitEndpointPoolCondition(t, func() bool { return manager.ReadyFor(next) }, "holder boot generation")
	manager.mu.Lock()
	afterGeneration := manager.current.transportGeneration
	manager.mu.Unlock()
	if afterGeneration == beforeGeneration {
		t.Fatalf("holder boot change reused transport generation %d", afterGeneration)
	}
	if got := dials.Load(); got != beforeDials+1 {
		t.Fatalf("same endpoint holder boot change dials=%d want=%d", got, beforeDials+1)
	}
	dialedMu.Lock()
	if len(playerConnections) < 2 {
		dialedMu.Unlock()
		t.Fatalf("player dial records=%d want initial plus replacement", len(playerConnections))
	}
	old := playerConnections[0]
	dialedMu.Unlock()
	select {
	case <-old.closed:
	case <-time.After(time.Second):
		t.Fatal("old holder-boot transport was not retired")
	}
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("pool terminated after holder boot change: %v", err)
		}
	default:
	}
}

func TestEndpointConnectionManagerRetriesInitialUnavailableWithoutPublishingPartial(t *testing.T) {
	fixture := newCompositionFixture(t)
	initial := actorrouting.CloneRouteSnapshot(fixture.snap)
	for index := range initial.Holders {
		initial.Holders[index].Endpoint = "late.test:9100"
	}
	provider, _, err := actorrouting.NewDynamicAssignmentProvider(initial, 4)
	if err != nil {
		t.Fatal(err)
	}
	var allowed atomic.Bool
	var attempts atomic.Int32
	manager, err := NewEndpointConnectionManager(provider, EndpointConnectionOptions{
		MaxEndpoints: 4, DialTimeout: time.Second, RetireGrace: time.Second,
		Dial: func(context.Context, string) (ManagedEndpointConnection, error) {
			attempts.Add(1)
			if !allowed.Load() {
				return nil, errors.New("injected holder startup race")
			}
			return newEndpointPoolTestConn(), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer manager.Close()
	runDone := make(chan error, 1)
	go func() { runDone <- manager.Run(ctx) }()
	waitEndpointPoolCondition(t, func() bool { return attempts.Load() >= 2 }, "initial dial retry")
	select {
	case err := <-runDone:
		t.Fatalf("initial transient dial terminated manager: %v", err)
	default:
	}
	manager.mu.Lock()
	before := manager.current
	manager.mu.Unlock()
	if before.revision != 0 || len(before.entries) != 0 || manager.ReadyFor(initial) {
		t.Fatalf("partial initial generation published: %+v", before)
	}
	allowed.Store(true)
	waitEndpointPoolCondition(t, func() bool { return manager.ReadyFor(initial) }, "complete retried initial generation")
	cancel()
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("retried pool Run did not join")
	}
}

func TestEndpointConnectionManagerRejectsInvalidInitialShapeWithoutRetry(t *testing.T) {
	fixture := newCompositionFixture(t)
	initial := actorrouting.CloneRouteSnapshot(fixture.snap)
	initial.Holders[0].Endpoint = "not-an-endpoint"
	provider, _, err := actorrouting.NewDynamicAssignmentProvider(initial, 4)
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	manager, err := NewEndpointConnectionManager(provider, EndpointConnectionOptions{
		MaxEndpoints: 4, DialTimeout: time.Second, RetireGrace: time.Second,
		Dial: func(context.Context, string) (ManagedEndpointConnection, error) {
			attempts.Add(1)
			return newEndpointPoolTestConn(), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	err = manager.Run(context.Background())
	if err == nil || !errors.Is(err, ErrEndpointPoolUnavailable) {
		t.Fatalf("invalid initial shape error=%v", err)
	}
	if got := attempts.Load(); got != 0 {
		t.Fatalf("invalid initial shape dial attempts=%d", got)
	}
}

func TestEndpointConnectionManagerPublishesCompleteGenerationAndDrainsRetiredConnection(t *testing.T) {
	fixture := newCompositionFixture(t)
	initial := actorrouting.CloneRouteSnapshot(fixture.snap)
	initial.Holders[0].Endpoint = "a.test:9100"
	initial.Holders[1].Endpoint = "a.test:9100"
	provider, publisher, err := actorrouting.NewDynamicAssignmentProvider(initial, 4)
	if err != nil {
		t.Fatal(err)
	}

	aConn := newEndpointPoolTestConn()
	bConn := newEndpointPoolTestConn()
	bDialStarted := make(chan struct{})
	bDialAllowed := make(chan struct{})
	manager, err := NewEndpointConnectionManager(provider, EndpointConnectionOptions{
		MaxEndpoints: 4,
		DialTimeout:  time.Second,
		RetireGrace:  time.Second,
		Dial: func(ctx context.Context, endpoint string) (ManagedEndpointConnection, error) {
			switch endpoint {
			case "a.test:9100":
				return aConn, nil
			case "b.test:9200":
				close(bDialStarted)
				select {
				case <-bDialAllowed:
					return bConn, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			case "failure.test:9300":
				return nil, errors.New("injected dial failure")
			default:
				return nil, errors.New("unexpected endpoint")
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer manager.Close()
	runDone := make(chan error, 1)
	go func() { runDone <- manager.Run(ctx) }()
	waitEndpointPoolCondition(t, func() bool { return manager.ReadyFor(initial) }, "initial endpoint generation")

	_, releaseA, err := manager.acquire("a.test:9100", initial.GlobalRevision)
	if err != nil {
		t.Fatal(err)
	}
	next := actorrouting.CloneRouteSnapshot(initial)
	next.GlobalRevision++
	for index := range next.Holders {
		next.Holders[index].Endpoint = "b.test:9200"
	}
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, next); err != nil {
		t.Fatal(err)
	}
	select {
	case <-bDialStarted:
	case <-time.After(time.Second):
		t.Fatal("new endpoint dial did not start")
	}
	manager.mu.Lock()
	installedDuringDial := manager.current
	manager.mu.Unlock()
	if installedDuringDial.revision != initial.GlobalRevision || installedDuringDial.entries["a.test:9100"] == nil || installedDuringDial.entries["b.test:9200"] != nil {
		t.Fatalf("partial generation published during pre-dial: %+v", installedDuringDial)
	}
	close(bDialAllowed)
	waitEndpointPoolCondition(t, func() bool { return manager.ReadyFor(next) }, "replacement endpoint generation")
	select {
	case <-aConn.closed:
		t.Fatal("retired endpoint closed while an RPC lease was inflight")
	default:
	}
	releaseA()
	select {
	case <-aConn.closed:
	case <-time.After(time.Second):
		t.Fatal("retired endpoint did not close after inflight reached zero")
	}

	failed := actorrouting.CloneRouteSnapshot(next)
	failed.GlobalRevision++
	for index := range failed.Holders {
		failed.Holders[index].Endpoint = "failure.test:9300"
	}
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, failed); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-runDone:
		if err == nil || !errors.Is(err, ErrEndpointPoolUnavailable) {
			t.Fatalf("terminal pool error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("failed endpoint generation did not terminate the manager")
	}
	manager.mu.Lock()
	installedAfterFailure := manager.current
	manager.mu.Unlock()
	if installedAfterFailure.revision != next.GlobalRevision || installedAfterFailure.entries["b.test:9200"] == nil || installedAfterFailure.entries["failure.test:9300"] != nil {
		t.Fatalf("failed generation partially replaced last good pool: %+v", installedAfterFailure)
	}
}

// TestEndpointConnectionManagerCloseLinearizesPublishedRetirement freezes the
// A -> B / Close interleaving that used to call WaitGroup.Add after Close had
// begun Wait: publication has swapped to B, but A's retirement worker has not
// started yet. Retirement registration is already visible to Close, so Close
// cannot return (or race Add/Wait) until the parked installer starts and joins
// A's worker.
func TestEndpointConnectionManagerCloseLinearizesPublishedRetirement(t *testing.T) {
	fixture := newCompositionFixture(t)
	initial := actorrouting.CloneRouteSnapshot(fixture.snap)
	for index := range initial.Holders {
		initial.Holders[index].Endpoint = "a.test:9100"
	}
	provider, _, err := actorrouting.NewDynamicAssignmentProvider(initial, 4)
	if err != nil {
		t.Fatal(err)
	}
	aConn := newEndpointPoolTestConn()
	bConn := newEndpointPoolTestConn()
	manager, err := NewEndpointConnectionManager(provider, EndpointConnectionOptions{
		MaxEndpoints: 4, DialTimeout: time.Second, RetireGrace: time.Second,
		Dial: func(_ context.Context, endpoint string) (ManagedEndpointConnection, error) {
			switch endpoint {
			case "a.test:9100":
				return aConn, nil
			case "b.test:9200":
				return bConn, nil
			default:
				return nil, errors.New("unexpected endpoint")
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.install(context.Background(), initial); err != nil {
		t.Fatal(err)
	}

	swapped := make(chan struct{})
	releaseRetirementStart := make(chan struct{})
	closeReachedWait := make(chan struct{})
	manager.testAfterGenerationSwap = func() {
		close(swapped)
		<-releaseRetirementStart
	}
	manager.testBeforeRetirementWait = func() { close(closeReachedWait) }

	next := actorrouting.CloneRouteSnapshot(initial)
	next.GlobalRevision++
	for index := range next.Holders {
		next.Holders[index].Endpoint = "b.test:9200"
	}
	installDone := make(chan error, 1)
	go func() { installDone <- manager.install(context.Background(), next) }()
	<-swapped
	manager.mu.Lock()
	installed := manager.current
	manager.mu.Unlock()
	if installed.revision != next.GlobalRevision || installed.entries["b.test:9200"] == nil || installed.entries["a.test:9100"] != nil {
		t.Fatalf("replacement generation was not atomically published: %+v", installed)
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- manager.Close() }()
	<-closeReachedWait
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before the published A retirement was started: %v", err)
	default:
	}
	select {
	case <-aConn.closed:
		t.Fatal("A closed before its parked retirement owner started")
	default:
	}

	close(releaseRetirementStart)
	if err := <-installDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	for endpoint, connection := range map[string]*endpointPoolTestConn{
		"a.test:9100": aConn,
		"b.test:9200": bConn,
	} {
		select {
		case <-connection.closed:
		default:
			t.Fatalf("endpoint %s leaked after Close", endpoint)
		}
	}
}

func waitEndpointPoolCondition(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !condition() {
		t.Fatalf("timed out waiting for %s", description)
	}
}
