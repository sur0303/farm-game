package transport

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	routecontrolpb "farmgame/api/gen/routecontrol"
	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/remote"
	core "farmgame/internal/routecontrol"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func TestWatchHandshakeAuthorizationAndSubscriptionRejectionReturnBeforeChannel(t *testing.T) {
	t.Run("authorization", func(t *testing.T) {
		harness := newTransportHarness(t, "boot-c0", 1<<20)
		defer harness.close()
		// Construction already copied the authenticated allowlist. Change the
		// package-local test service before its first RPC so the verified client
		// certificate is rejected by the production authorization path.
		harness.service.allowedPeers = map[string]struct{}{"other.test": {}}
		updates, err := harness.client.WatchFrom(context.Background(), 1)
		if updates != nil || status.Code(err) != codes.Unauthenticated {
			t.Fatalf("authorization rejection updates=%v err=%v", updates, err)
		}
	})

	t.Run("subscription capacity keeps restart-aware fetch disabled", func(t *testing.T) {
		harness := newTransportHarness(t, "boot-c0", 1<<20)
		defer harness.close()
		var subscriptions []*core.SnapshotSubscription
		for range 16 {
			subscription, err := harness.hub.Subscribe(context.Background(), 1)
			if err != nil {
				t.Fatal(err)
			}
			subscriptions = append(subscriptions, subscription)
		}
		defer func() {
			for _, subscription := range subscriptions {
				subscription.Close()
			}
		}()

		source := &countingRestartSource{client: harness.client}
		_, pump, err := remote.NewRemoteAssignmentProvider(source, remote.Options{
			MaxResyncAttempts: 1,
			MaxWatchFailures:  1,
			Backoff:           func(int) time.Duration { return 0 },
		})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- pump.Run(context.Background()) }()
		select {
		case err := <-done:
			if !errors.Is(err, remote.ErrWatchLivenessExhausted) {
				t.Fatalf("Pump rejection err=%v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Pump did not finish its bounded pre-establishment retries")
		}
		if source.restartFetches.Load() != 0 {
			t.Fatalf("restart-aware fetches=%d want 0", source.restartFetches.Load())
		}
		if source.fetches.Load() != 2 || source.watches.Load() != 2 {
			t.Fatalf("ordinary fetches=%d watches=%d want 2/2", source.fetches.Load(), source.watches.Load())
		}
	})
}

type countingRestartSource struct {
	client         *SnapshotClient
	fetches        atomic.Int32
	watches        atomic.Int32
	restartFetches atomic.Int32
}

func (s *countingRestartSource) FetchFullSnapshot(ctx context.Context) (actorrouting.RouteSnapshot, error) {
	s.fetches.Add(1)
	return s.client.FetchFullSnapshot(ctx)
}

func (s *countingRestartSource) WatchFrom(ctx context.Context, revision uint64) (<-chan actorrouting.RouteSnapshot, error) {
	s.watches.Add(1)
	return s.client.WatchFrom(ctx, revision)
}

func (s *countingRestartSource) FetchFullSnapshotForResync(ctx context.Context, baseline remote.ResyncBaseline) (actorrouting.RouteSnapshot, error) {
	s.restartFetches.Add(1)
	return s.client.FetchFullSnapshotForResync(ctx, baseline)
}

func TestWatchHandshakeCurrentSubscribeRaceOrdersRThenRPlusOne(t *testing.T) {
	ctx := context.Background()
	inner := core.NewMemoryRouteSnapshotStore()
	initial := transportSnapshot(1)
	if _, err := inner.Bootstrap(ctx, core.NestedControlOperationToken{}, initial); err != nil {
		t.Fatal(err)
	}
	store := &handshakeRaceStore{inner: inner, next: transportSnapshot(2)}
	hub, err := core.NewSnapshotWatchHub(store, core.SnapshotWatchHubOptions{
		MaxClients: 8, QueueSize: 8, ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Hub construction is load 1. Server Current is load 2 and atomically
	// advances durable state immediately after returning R; Subscribe then
	// reconciles R+1 and must queue it behind the R handshake.
	harness := newTransportHarnessForHub(t, hub, "boot-c0", 1<<20)
	defer harness.close()
	harness.client.opts.watchBuffer = 1

	watchCtx, cancel := context.WithCancel(context.Background())
	updates, err := harness.client.WatchFrom(watchCtx, 1)
	if err != nil {
		t.Fatal(err)
	}
	for revision := uint64(1); revision <= 2; revision++ {
		select {
		case snapshot := <-updates:
			if snapshot.GlobalRevision != revision {
				t.Fatalf("ordered Watch revision=%d want=%d", snapshot.GlobalRevision, revision)
			}
		case <-time.After(time.Second):
			t.Fatalf("Watch did not deliver revision %d", revision)
		}
	}
	select {
	case snapshot := <-updates:
		t.Fatalf("Watch emitted unsafe duplicate/extra revision %d", snapshot.GlobalRevision)
	default:
	}
	cancel()
	for range updates {
	}
	updates, err = harness.client.WatchFrom(context.Background(), 3)
	if updates != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ahead Watch updates=%v err=%v", updates, err)
	}
}

type handshakeRaceStore struct {
	inner *core.MemoryRouteSnapshotStore
	next  actorrouting.RouteSnapshot
	loads atomic.Int32
	once  sync.Once
}

func (s *handshakeRaceStore) LoadCurrent(ctx context.Context) (core.StoredRouteSnapshot, bool, error) {
	stored, found, err := s.inner.LoadCurrent(ctx)
	if err == nil && found && s.loads.Add(1) == 2 {
		s.once.Do(func() {
			_, err = s.inner.CompareAndSwap(ctx, core.NestedControlOperationToken{}, stored.Revision, s.next)
		})
	}
	return stored, found, err
}

func (s *handshakeRaceStore) CompareAndSwap(ctx context.Context, token core.NestedControlOperationToken, expected uint64, next actorrouting.RouteSnapshot) (core.SnapshotCASResult, error) {
	return s.inner.CompareAndSwap(ctx, token, expected, next)
}

func TestWatchHandshakeBlockedRecvHonorsContextAndClose(t *testing.T) {
	stored, err := core.CanonicalizeRouteSnapshot(transportSnapshot(1))
	if err != nil {
		t.Fatal(err)
	}
	valid, err := storedToEnvelope(stored, "controller-a", "boot-a", "transport-test", 1<<20)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("context", func(t *testing.T) {
		harness := newReviewClientHarness(t, valid, valid, "no-handshake")
		defer harness.close()
		ctx, cancel := context.WithCancel(context.Background())
		type result struct {
			updates <-chan actorrouting.RouteSnapshot
			err     error
		}
		done := make(chan result, 1)
		go func() {
			updates, err := harness.client.WatchFrom(ctx, 1)
			done <- result{updates: updates, err: err}
		}()
		<-harness.server.watchStarted
		cancel()
		select {
		case got := <-done:
			if got.updates != nil || !errors.Is(got.err, context.Canceled) {
				t.Fatalf("canceled handshake updates=%v err=%v", got.updates, got.err)
			}
		case <-time.After(time.Second):
			t.Fatal("canceled handshake did not join")
		}
		assertNoActiveClientOperations(t, harness.client)
	})

	t.Run("establishment timeout", func(t *testing.T) {
		harness := newReviewClientHarness(t, valid, valid, "no-handshake")
		defer harness.close()
		harness.client.opts.callTimeout = 20 * time.Millisecond
		updates, err := harness.client.WatchFrom(context.Background(), 1)
		if updates != nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("timed-out handshake updates=%v err=%v", updates, err)
		}
		assertNoActiveClientOperations(t, harness.client)
	})

	t.Run("Close", func(t *testing.T) {
		harness := newReviewClientHarness(t, valid, valid, "no-handshake")
		type result struct {
			updates <-chan actorrouting.RouteSnapshot
			err     error
		}
		watchDone := make(chan result, 1)
		go func() {
			updates, err := harness.client.WatchFrom(context.Background(), 1)
			watchDone <- result{updates: updates, err: err}
		}()
		<-harness.server.watchStarted
		closeDone := make(chan error, 1)
		go func() { closeDone <- harness.client.Close() }()
		select {
		case got := <-watchDone:
			if got.updates != nil || !errors.Is(got.err, ErrClientClosed) {
				t.Fatalf("closed handshake updates=%v err=%v", got.updates, got.err)
			}
		case <-time.After(time.Second):
			t.Fatal("Close did not release blocked WatchFrom")
		}
		select {
		case err := <-closeDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("Close did not join blocked handshake")
		}
		assertNoActiveClientOperations(t, harness.client)
		harness.grpc.Stop()
		_ = harness.listener.Close()
	})
}

func assertNoActiveClientOperations(t *testing.T, client *SnapshotClient) {
	t.Helper()
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.activeWatches) != 0 || len(client.activeOperations) != 0 {
		t.Fatalf("active watches=%d operations=%d", len(client.activeWatches), len(client.activeOperations))
	}
}

func TestPumpRealGRPCMandatoryHandshakesDriveC0C1C2AndRetireC1(t *testing.T) {
	base := transportSnapshot(10)
	c0 := rolloverEnvelope(t, base, "boot-c0", 100)
	c1 := rolloverEnvelope(t, base, "boot-c1", 100)
	c2 := rolloverEnvelope(t, base, "boot-c2", 100)
	server := newHandshakeRolloverServer(c0)
	harness := newRolloverGRPCHarness(t, server)
	defer harness.close()
	source := &countingRestartSource{client: harness.client}
	provider, pump, err := remote.NewRemoteAssignmentProvider(source, remote.Options{
		MaxResyncAttempts: 1,
		MaxWatchFailures:  1,
		Backoff:           func(int) time.Duration { return 0 },
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- pump.Run(context.Background()) }()

	watch0 := receiveRolloverWatch(t, server)
	waitUntilTransport(t, time.Second, func() bool { return pump.Duplicates.Load() == 1 })
	server.setCurrent(c1)
	watch0.close()
	watch1 := receiveRolloverWatch(t, server)
	waitUntilTransport(t, time.Second, func() bool { return pump.Duplicates.Load() == 3 })
	assertPinnedBoots(t, harness.client, "boot-c1", "boot-c0")
	if got := provider.CurrentSnapshot(); !reflect.DeepEqual(got, base) {
		t.Fatal("quiet C1 handshake changed the complete installed snapshot")
	}

	server.setCurrent(c2)
	watch1.close()
	watch2 := receiveRolloverWatch(t, server)
	waitUntilTransport(t, time.Second, func() bool { return pump.Duplicates.Load() == 5 })
	assertPinnedBoots(t, harness.client, "boot-c2", "boot-c0", "boot-c1")
	if source.restartFetches.Load() != 2 {
		t.Fatalf("restart-aware C1/C2 fetches=%d want 2", source.restartFetches.Load())
	}

	watch2.send(c1)
	select {
	case err := <-done:
		if !errors.Is(err, remote.ErrResyncExhausted) || !errors.Is(harness.client.LastWatchError(), ErrEnvelopeBootRetired) {
			t.Fatalf("retired C1 Pump err=%v client err=%v", err, harness.client.LastWatchError())
		}
	case <-time.After(time.Second):
		t.Fatal("retired C1 did not terminally stop the Pump")
	}
}

type handshakeRolloverServer struct {
	routecontrolpb.UnimplementedRouteSnapshotServiceServer
	mu      sync.Mutex
	current *routecontrolpb.RouteSnapshotEnvelope
	watches chan *rolloverServerWatch
}

type rolloverWatchCommand struct {
	envelope *routecontrolpb.RouteSnapshotEnvelope
	close    bool
}

type rolloverServerWatch struct {
	after    uint64
	commands chan rolloverWatchCommand
}

func newHandshakeRolloverServer(initial *routecontrolpb.RouteSnapshotEnvelope) *handshakeRolloverServer {
	return &handshakeRolloverServer{
		current: proto.Clone(initial).(*routecontrolpb.RouteSnapshotEnvelope),
		watches: make(chan *rolloverServerWatch, 8),
	}
}

func (s *handshakeRolloverServer) setCurrent(envelope *routecontrolpb.RouteSnapshotEnvelope) {
	s.mu.Lock()
	s.current = proto.Clone(envelope).(*routecontrolpb.RouteSnapshotEnvelope)
	s.mu.Unlock()
}

func (s *handshakeRolloverServer) currentEnvelope() *routecontrolpb.RouteSnapshotEnvelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	return proto.Clone(s.current).(*routecontrolpb.RouteSnapshotEnvelope)
}

func (s *handshakeRolloverServer) GetSnapshot(context.Context, *routecontrolpb.GetSnapshotRequest) (*routecontrolpb.RouteSnapshotEnvelope, error) {
	return s.currentEnvelope(), nil
}

func (s *handshakeRolloverServer) GetCurrentRevision(context.Context, *routecontrolpb.GetCurrentRevisionRequest) (*routecontrolpb.RouteSnapshotEnvelope, error) {
	return s.currentEnvelope(), nil
}

func (s *handshakeRolloverServer) WatchFrom(request *routecontrolpb.WatchFromRequest, stream grpc.ServerStreamingServer[routecontrolpb.RouteSnapshotEnvelope]) error {
	watch := &rolloverServerWatch{after: request.GetAfterRevision(), commands: make(chan rolloverWatchCommand, 4)}
	select {
	case s.watches <- watch:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	if err := stream.Send(s.currentEnvelope()); err != nil {
		return err
	}
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case command := <-watch.commands:
			if command.close {
				return nil
			}
			if err := stream.Send(proto.Clone(command.envelope).(*routecontrolpb.RouteSnapshotEnvelope)); err != nil {
				return err
			}
		}
	}
}

func receiveRolloverWatch(t *testing.T, server *handshakeRolloverServer) *rolloverServerWatch {
	t.Helper()
	select {
	case watch := <-server.watches:
		return watch
	case <-time.After(time.Second):
		t.Fatal("real gRPC Watch did not start")
		return nil
	}
}

func (w *rolloverServerWatch) close() {
	w.commands <- rolloverWatchCommand{close: true}
}

func (w *rolloverServerWatch) send(envelope *routecontrolpb.RouteSnapshotEnvelope) {
	w.commands <- rolloverWatchCommand{envelope: envelope}
}

type rolloverGRPCHarness struct {
	grpc     *grpc.Server
	listener *bufconn.Listener
	conn     *grpc.ClientConn
	client   *SnapshotClient
}

func newRolloverGRPCHarness(t *testing.T, service routecontrolpb.RouteSnapshotServiceServer) *rolloverGRPCHarness {
	t.Helper()
	bundle := newCertificateBundle(t)
	serverTLS, err := NewServerTLSConfig(bundle.serverCert, bundle.serverKey, bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	routecontrolpb.RegisterRouteSnapshotServiceServer(grpcServer, service)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = grpcServer.Serve(listener) }()
	clientTLS, err := NewClientTLSConfig(bundle.clientCert, bundle.clientKey, bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS.ServerName = "server.test"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "rollover-bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	client, err := newClient(conn, (ClientOptions{
		ExpectedControllerID: "controller-a",
		ExpectedRealm:        "transport-test",
		MaxMessageBytes:      1 << 20,
		CallTimeout:          time.Second,
		RetiredBootLimit:     4,
	}).normalize(), conn.Close)
	if err != nil {
		t.Fatal(err)
	}
	return &rolloverGRPCHarness{grpc: grpcServer, listener: listener, conn: conn, client: client}
}

func (h *rolloverGRPCHarness) close() {
	_ = h.client.Close()
	h.grpc.Stop()
	_ = h.conn.Close()
	_ = h.listener.Close()
}

func newTransportHarnessForHub(t *testing.T, hub *core.SnapshotWatchHub, boot string, maxMessageBytes int) *transportHarness {
	t.Helper()
	bundle := newCertificateBundle(t)
	service, err := NewRouteSnapshotServer(hub, ServerOptions{
		ControllerID: "controller-a", Boot: boot, Realm: "transport-test",
		AllowedClientIdentity: []string{"client.test"}, MaxMessageBytes: maxMessageBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, err := NewServerTLSConfig(bundle.serverCert, bundle.serverKey, bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewGRPCServer(service, serverTLS, GRPCServerOptions{MaxMessageBytes: maxMessageBytes})
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	go func() { _ = server.Serve(listener) }()
	clientTLS, err := NewClientTLSConfig(bundle.clientCert, bundle.clientKey, bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS.ServerName = "server.test"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "handshake-bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	client, err := newClient(conn, (ClientOptions{
		ExpectedControllerID: "controller-a", ExpectedRealm: "transport-test",
		MaxMessageBytes: maxMessageBytes, CallTimeout: time.Second,
	}).normalize(), conn.Close)
	if err != nil {
		t.Fatal(err)
	}
	return &transportHarness{
		hub: hub, service: service, server: server, listener: listener,
		conn: conn, client: client, bundle: bundle,
	}
}
