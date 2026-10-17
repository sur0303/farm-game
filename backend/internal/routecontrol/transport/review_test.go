package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"net"
	"net/url"
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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func TestAllowedClientIdentityIsRequiredCanonicalAndUnique(t *testing.T) {
	hubStore := core.NewMemoryRouteSnapshotStore()
	if _, err := hubStore.Bootstrap(context.Background(), core.NestedControlOperationToken{}, transportSnapshot(1)); err != nil {
		t.Fatal(err)
	}
	hub, err := core.NewSnapshotWatchHub(hubStore)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()

	tests := []struct {
		name  string
		value []string
	}{
		{name: "empty", value: nil},
		{name: "blank item", value: []string{" "}},
		{name: "not canonical", value: []string{" client.test"}},
		{name: "duplicate", value: []string{"client.test", "client.test"}},
		{name: "case duplicate", value: []string{"client.test", "CLIENT.TEST"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewRouteSnapshotServer(hub, ServerOptions{
				ControllerID:          "controller-a",
				Boot:                  "boot-a",
				AllowedClientIdentity: test.value,
			}); err == nil {
				t.Fatal("invalid allowlist accepted")
			}
		})
	}
	if _, err := NewRouteSnapshotServer(hub, ServerOptions{
		ControllerID:          "controller-a",
		Boot:                  "boot-a",
		AllowedClientIdentity: []string{"client.test", "spiffe://farm.test/client-a"},
	}); err != nil {
		t.Fatalf("valid allowlist rejected: %v", err)
	}
}

func TestCertificateIdentityRejectsAmbiguousSANAndCNFallback(t *testing.T) {
	uri := &url.URL{Scheme: "spiffe", Host: "farm.test", Path: "/client-a"}
	tests := []struct {
		name string
		cert *x509.Certificate
		want string
	}{
		{
			name: "single URI",
			cert: &x509.Certificate{URIs: []*url.URL{uri}},
			want: uri.String(),
		},
		{
			name: "single DNS",
			cert: &x509.Certificate{DNSNames: []string{"CLIENT.TEST"}},
			want: "client.test",
		},
		{
			name: "URI and DNS",
			cert: &x509.Certificate{URIs: []*url.URL{uri}, DNSNames: []string{"client.test"}},
		},
		{
			name: "IP only",
			cert: &x509.Certificate{IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}},
		},
		{
			name: "URI and IP",
			cert: &x509.Certificate{URIs: []*url.URL{uri}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}},
		},
		{
			name: "DNS and IP",
			cert: &x509.Certificate{DNSNames: []string{"client.test"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}},
		},
		{
			name: "email only",
			cert: &x509.Certificate{EmailAddresses: []string{"client@example.test"}},
		},
		{
			name: "URI and email",
			cert: &x509.Certificate{URIs: []*url.URL{uri}, EmailAddresses: []string{"client@example.test"}},
		},
		{
			name: "multiple URI",
			cert: &x509.Certificate{URIs: []*url.URL{uri, uri}},
		},
		{
			name: "multiple DNS",
			cert: &x509.Certificate{DNSNames: []string{"a.test", "b.test"}},
		},
		{
			name: "CN fallback",
			cert: &x509.Certificate{Subject: pkixName("client.test")},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := certificateIdentity(testCertificateWithSAN(test.cert))
			if test.want != "" {
				if err != nil || got != test.want {
					t.Fatalf("identity=%q err=%v want=%q", got, err, test.want)
				}
				return
			}
			if !errors.Is(err, ErrCertificateIdentityAmbiguous) {
				t.Fatalf("identity=%q err=%v want ambiguous", got, err)
			}
		})
	}
}

func TestCanonicalIdentityRejectsIPLiteralAllowlistServerNameAndExpectedIdentity(t *testing.T) {
	for _, value := range []string{"127.0.0.1", "::1", "[::1]"} {
		t.Run(value, func(t *testing.T) {
			if _, err := ValidateAllowedClientIdentities([]string{value}); !errors.Is(err, ErrPeerIdentityNotAllowed) {
				t.Fatalf("IP allowlist value=%q err=%v", value, err)
			}
			options := ClientOptions{
				ExpectedControllerID:   "controller-a",
				ServerName:             value,
				ExpectedServerIdentity: value,
			}
			if _, err := options.validate(); !errors.Is(err, ErrTLSConfig) {
				t.Fatalf("IP ServerName=%q err=%v", value, err)
			}
		})
	}
	if _, err := (ClientOptions{
		ExpectedControllerID:   "controller-a",
		ServerName:             "server.test",
		ExpectedServerIdentity: "127.0.0.1",
	}).validate(); !errors.Is(err, ErrTLSIdentityMismatch) {
		t.Fatalf("IP expected server identity err=%v", err)
	}
}

func TestTLSRejectsTLS12AndRequiresExplicitDialIdentity(t *testing.T) {
	harness := newTransportHarness(t, "controller-boot-a", 1<<20)
	defer harness.close()

	serverTLS, err := NewServerTLSConfig(harness.bundle.serverCert, harness.bundle.serverKey, harness.bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}
	serverTLS.MinVersion = tls.VersionTLS12
	if _, err := NewGRPCServer(harness.service, serverTLS); !errors.Is(err, ErrTLSConfig) {
		t.Fatalf("TLS1.2 server err=%v", err)
	}
	clientTLS, err := NewClientTLSConfig(harness.bundle.clientCert, harness.bundle.clientKey, harness.bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS.MinVersion = tls.VersionTLS12
	if _, err := cloneTLSConfig(clientTLS); !errors.Is(err, ErrTLSConfig) {
		t.Fatalf("TLS1.2 client err=%v", err)
	}
	if _, err := Dial(context.Background(), "bufnet", clientTLS, ClientOptions{
		ExpectedControllerID: "controller-a",
	}); !errors.Is(err, ErrTLSConfig) && !errors.Is(err, ErrTLSIdentityMismatch) {
		t.Fatalf("missing explicit TLS identity err=%v", err)
	}
	if _, err := Dial(context.Background(), "bufnet", harnessTLSConfig(harness), ClientOptions{
		ExpectedControllerID: "controller-a",
		ServerName:           "server.test",
	}); !errors.Is(err, ErrTLSIdentityMismatch) {
		t.Fatalf("missing expected TLS identity err=%v", err)
	}
}

func TestSnapshotTransportStatusMapsDurableIntegrityToFailedPrecondition(t *testing.T) {
	tests := []error{
		core.ErrSnapshotDurableMissing,
		core.ErrSnapshotDurableRollback,
		core.ErrSnapshotDurableConflict,
		errors.Join(core.ErrSnapshotDurableContract, actorrouting.ErrHashMigrationRequired),
		errors.Join(core.ErrSnapshotDurableContract, core.ErrSnapshotChecksumMismatch),
	}
	for _, test := range tests {
		if got := status.Code(transportStatusError(test)); got != codes.FailedPrecondition {
			t.Fatalf("error=%v status=%s want %s", test, got, codes.FailedPrecondition)
		}
	}
}

func TestSnapshotClientTerminalContractErrorIsStickyAndBlocksValidFetch(t *testing.T) {
	stored, err := core.CanonicalizeRouteSnapshot(transportSnapshot(1))
	if err != nil {
		t.Fatal(err)
	}
	valid, err := storedToEnvelope(stored, "controller-a", "boot-a", "transport-test", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	malformed := proto.Clone(valid).(*routecontrolpb.RouteSnapshotEnvelope)
	malformed.Schema = "route-snapshot-v0"
	harness := newReviewClientHarness(t, valid, malformed, "single")
	defer harness.close()

	if _, err := harness.client.FetchFullSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if updates, err := harness.client.WatchFrom(context.Background(), 1); !errors.Is(err, ErrEnvelopeMalformed) || updates != nil {
		t.Fatalf("malformed handshake updates=%v err=%v", updates, err)
	}
	if !errors.Is(harness.client.LastWatchError(), ErrEnvelopeMalformed) {
		t.Fatalf("last watch error=%v", harness.client.LastWatchError())
	}
	getCalls := harness.server.getCalls.Load()
	if _, err := harness.client.FetchFullSnapshot(context.Background()); !errors.Is(err, ErrEnvelopeMalformed) {
		t.Fatalf("valid Fetch after terminal err=%v", err)
	}
	if got := harness.server.getCalls.Load(); got != getCalls {
		t.Fatalf("terminal Fetch contacted server: calls=%d before=%d", got, getCalls)
	}
	if _, err := harness.client.WatchFrom(context.Background(), 1); !errors.Is(err, ErrEnvelopeMalformed) {
		t.Fatalf("terminal Watch after terminal err=%v", err)
	}
}

func TestSnapshotClientAllAsyncEnvelopeContractErrorsLatch(t *testing.T) {
	baseStored, err := core.CanonicalizeRouteSnapshot(transportSnapshot(1))
	if err != nil {
		t.Fatal(err)
	}
	base, err := storedToEnvelope(baseStored, "controller-a", "boot-a", "transport-test", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*routecontrolpb.RouteSnapshotEnvelope)
		want   error
		setup  func(*SnapshotClient) error
	}{
		{
			name: "controller",
			mutate: func(envelope *routecontrolpb.RouteSnapshotEnvelope) {
				envelope.ControllerId = "controller-b"
			},
			want: ErrEnvelopeControllerMismatch,
		},
		{
			name: "boot",
			mutate: func(envelope *routecontrolpb.RouteSnapshotEnvelope) {
				envelope.Boot = "boot-b"
			},
			want: ErrEnvelopeBootDrift,
			setup: func(client *SnapshotClient) error {
				return seedClientPin(client, base)
			},
		},
		{
			name: "realm",
			want: ErrEnvelopeRealmMismatch,
		},
		{
			name: "topology",
			want: ErrEnvelopeTopologyDrift,
			setup: func(client *SnapshotClient) error {
				return seedClientPin(client, base)
			},
		},
		{
			name: "capacity",
			want: ErrEnvelopeCapacity,
			setup: func(client *SnapshotClient) error {
				client.opts.maxMessageBytes = 16
				return nil
			},
		},
		{
			name: "retired boot",
			want: ErrEnvelopeBootRetired,
			setup: func(client *SnapshotClient) error {
				client.mu.Lock()
				client.retiredBoots["boot-a"] = struct{}{}
				client.mu.Unlock()
				return nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			watch := proto.Clone(base).(*routecontrolpb.RouteSnapshotEnvelope)
			if test.name == "realm" {
				realmSnapshot := transportSnapshot(1)
				realmSnapshot.Realm = "other-realm"
				realmStored, err := core.CanonicalizeRouteSnapshot(realmSnapshot)
				if err != nil {
					t.Fatal(err)
				}
				watch, err = storedToEnvelope(realmStored, "controller-a", "boot-a", "other-realm", 1<<20)
				if err != nil {
					t.Fatal(err)
				}
			}
			if test.name == "topology" {
				topologySnapshot := transportSnapshot(1)
				topologySnapshot.HashSeed++
				topologyStored, err := core.CanonicalizeRouteSnapshot(topologySnapshot)
				if err != nil {
					t.Fatal(err)
				}
				watch, err = storedToEnvelope(topologyStored, "controller-a", "boot-a", "transport-test", 1<<20)
				if err != nil {
					t.Fatal(err)
				}
			}
			if test.mutate != nil {
				test.mutate(watch)
			}
			harness := newReviewClientHarness(t, base, watch, "single")
			defer harness.close()
			if test.setup != nil {
				if err := test.setup(harness.client); err != nil {
					t.Fatal(err)
				}
			}
			updates, err := harness.client.WatchFrom(context.Background(), 1)
			if !errors.Is(err, test.want) || updates != nil {
				t.Fatalf("handshake updates=%v err=%v want=%v", updates, err, test.want)
			}
			if _, err := harness.client.WatchFrom(context.Background(), 1); !errors.Is(err, test.want) {
				t.Fatalf("subsequent Watch err=%v want=%v", err, test.want)
			}
		})
	}
}

func seedClientPin(client *SnapshotClient, envelope *routecontrolpb.RouteSnapshotEnvelope) error {
	_, err := client.validateEnvelope(envelope)
	return err
}

func TestSnapshotClientAsyncTerminalPreventsPumpRecovery(t *testing.T) {
	stored, err := core.CanonicalizeRouteSnapshot(transportSnapshot(1))
	if err != nil {
		t.Fatal(err)
	}
	valid, err := storedToEnvelope(stored, "controller-a", "boot-a", "transport-test", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	malformed := proto.Clone(valid).(*routecontrolpb.RouteSnapshotEnvelope)
	malformed.Schema = "route-snapshot-v0"
	harness := newReviewClientHarness(t, valid, malformed, "single")
	defer harness.close()
	_, pump, err := remote.NewRemoteAssignmentProvider(harness.client, remote.Options{
		MaxResyncAttempts: 2,
		MaxWatchFailures:  1,
		Backoff:           func(int) time.Duration { return 0 },
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- pump.Run(context.Background()) }()
	select {
	case <-harness.server.watchStarted:
	case <-time.After(time.Second):
		t.Fatal("pump did not start watch")
	}
	select {
	case err := <-done:
		if !errors.Is(err, remote.ErrResyncExhausted) {
			t.Fatalf("pump err=%v want resync exhaustion", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pump recovered after terminal envelope error")
	}
	if got := harness.server.getCalls.Load(); got != 1 {
		t.Fatalf("terminal pump retried valid Fetch: calls=%d want=1", got)
	}
}

func TestSnapshotClientHubIntegrityWatchLatchesSubsequentValidFetch(t *testing.T) {
	ctx := context.Background()
	store := core.NewMemoryRouteSnapshotStore()
	initial := transportSnapshot(1)
	if _, err := store.Bootstrap(ctx, core.NestedControlOperationToken{}, initial); err != nil {
		t.Fatal(err)
	}
	hub, err := core.NewSnapshotWatchHub(store, core.SnapshotWatchHubOptions{
		ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()

	stored, err := core.CanonicalizeRouteSnapshot(initial)
	if err != nil {
		t.Fatal(err)
	}
	valid, err := storedToEnvelope(stored, "controller-a", "boot-a", "transport-test", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	harness := newReviewClientHarness(t, valid, valid, "blocked")
	defer harness.close()
	harness.server.hub = hub

	updates, err := harness.client.WatchFrom(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	waitTransport(t, time.Second, func() bool {
		return harness.server.watchStarted != nil && hub.ClientCount() == 1
	})

	next := transportSnapshot(2)
	next.HashSeed++
	if result, err := store.CompareAndSwap(ctx, core.NestedControlOperationToken{}, 1, next); err != nil || result.Status != core.SnapshotCASCommitted {
		t.Fatalf("durable topology change status=%v err=%v", result.Status, err)
	}
	if err := hub.Notify(ctx, next); !errors.Is(err, core.ErrSnapshotDurableContract) {
		t.Fatalf("hub integrity Notify err=%v", err)
	}
	for range updates {
	}
	waitTransport(t, time.Second, func() bool {
		return errors.Is(harness.client.LastWatchError(), ErrEnvelopeMalformed)
	})

	getCalls := harness.server.getCalls.Load()
	if _, err := harness.client.FetchFullSnapshot(ctx); !errors.Is(err, ErrEnvelopeMalformed) {
		t.Fatalf("valid Fetch after Hub integrity failure err=%v", err)
	}
	if got := harness.server.getCalls.Load(); got != getCalls {
		t.Fatalf("sticky terminal Fetch contacted server: calls=%d before=%d", got, getCalls)
	}
}

func TestSnapshotClientTransientBackpressureRemainsRecoverable(t *testing.T) {
	stored, err := core.CanonicalizeRouteSnapshot(transportSnapshot(1))
	if err != nil {
		t.Fatal(err)
	}
	valid, err := storedToEnvelope(stored, "controller-a", "boot-a", "transport-test", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	harness := newReviewClientHarness(t, valid, valid, "double")
	defer harness.close()
	harness.client.opts.watchBuffer = 1
	updates, err := harness.client.WatchFrom(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	waitTransport(t, time.Second, func() bool {
		return errors.Is(harness.client.LastWatchError(), ErrWatchBackpressure)
	})
	if !errors.Is(harness.client.LastWatchError(), ErrWatchBackpressure) {
		t.Fatalf("backpressure error=%v", harness.client.LastWatchError())
	}
	for range updates {
	}
	if _, err := harness.client.FetchFullSnapshot(context.Background()); err != nil {
		t.Fatalf("transient backpressure blocked Fetch: %v", err)
	}
}

func TestSnapshotClientCloseCancelsBlockedWatchUnblocksSemaphoreAndSticksCloseError(t *testing.T) {
	stored, err := core.CanonicalizeRouteSnapshot(transportSnapshot(1))
	if err != nil {
		t.Fatal(err)
	}
	valid, err := storedToEnvelope(stored, "controller-a", "boot-a", "transport-test", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	harness := newReviewClientHarness(t, valid, valid, "blocked")
	closeErr := errors.New("close failed")
	harness.client.closeFn = func() error { return closeErr }
	harness.client.opts.maxConcurrent = 1
	harness.client.sem = make(chan struct{}, 1)
	updates, err := harness.client.WatchFrom(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-updates:
	case <-time.After(time.Second):
		t.Fatal("established Watch did not publish its handshake")
	}
	select {
	case <-harness.server.watchStarted:
	case <-time.After(time.Second):
		t.Fatal("blocked watch did not start")
	}
	fetchDone := make(chan error, 1)
	go func() {
		_, err := harness.client.FetchFullSnapshot(context.Background())
		fetchDone <- err
	}()
	if err := harness.client.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close err=%v want=%v", err, closeErr)
	}
	if err := harness.client.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("sticky Close err=%v want=%v", err, closeErr)
	}
	select {
	case err := <-fetchDone:
		if !errors.Is(err, ErrClientClosed) {
			t.Fatalf("blocked Fetch err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked semaphore Fetch did not unblock")
	}
	select {
	case _, ok := <-updates:
		if ok {
			t.Fatal("closed watch emitted an update")
		}
	case <-time.After(time.Second):
		t.Fatal("closed watch channel remained open")
	}
	waitTransport(t, time.Second, func() bool {
		harness.client.mu.Lock()
		defer harness.client.mu.Unlock()
		return len(harness.client.activeWatches) == 0
	})
	_ = harness.conn.Close()
}

func TestSnapshotClientConstructorRequiresOwnedCloseFunction(t *testing.T) {
	harness := newTransportHarness(t, "controller-boot-a", 1<<20)
	defer harness.close()
	if _, err := newClient(harness.conn, (ClientOptions{
		ExpectedControllerID: "controller-a",
	}).normalize(), nil); !errors.Is(err, ErrTLSRequired) {
		t.Fatalf("nil close seam err=%v", err)
	}
}

func TestSnapshotTransportStatusMapsWatchCapacity(t *testing.T) {
	if got := status.Code(transportStatusError(core.ErrSnapshotWatchClientCapacity)); got != codes.ResourceExhausted {
		t.Fatalf("capacity status=%s", got)
	}
}

func TestRouteSnapshotServerSendTimeoutAndCancellationConverge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stream := &reviewServerStream{
		ctx:      ctx,
		cancel:   cancel,
		sendDone: make(chan struct{}),
		sendFn: func(*routecontrolpb.RouteSnapshotEnvelope) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	server := &RouteSnapshotServer{sendTimeout: 10 * time.Millisecond}
	err := server.sendEnvelope(stream, &routecontrolpb.RouteSnapshotEnvelope{})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("send timeout err=%v", err)
	}
	select {
	case <-stream.sendDone:
	case <-time.After(time.Second):
		t.Fatal("timed-out Send goroutine did not converge")
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	stream2 := &reviewServerStream{
		ctx:      ctx2,
		cancel:   cancel2,
		sendDone: make(chan struct{}),
		sendFn: func(*routecontrolpb.RouteSnapshotEnvelope) error {
			<-ctx2.Done()
			return ctx2.Err()
		},
	}
	done := make(chan error, 1)
	go func() { done <- server.sendEnvelope(stream2, &routecontrolpb.RouteSnapshotEnvelope{}) }()
	cancel2()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Send err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled Send did not converge")
	}
}

type reviewClientHarness struct {
	server   *reviewSnapshotServer
	grpc     *grpc.Server
	listener *bufconn.Listener
	conn     *grpc.ClientConn
	client   *SnapshotClient
}

func newReviewClientHarness(t *testing.T, valid, watch *routecontrolpb.RouteSnapshotEnvelope, mode string) *reviewClientHarness {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := &reviewSnapshotServer{
		valid:        valid,
		watch:        watch,
		mode:         mode,
		watchStarted: make(chan struct{}),
	}
	grpcServer := grpc.NewServer()
	routecontrolpb.RegisterRouteSnapshotServiceServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "review-bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatal(err)
	}
	client, err := newClient(conn, (ClientOptions{
		ExpectedControllerID: "controller-a",
		ExpectedRealm:        "transport-test",
		MaxMessageBytes:      1 << 20,
		WatchBuffer:          8,
		CallTimeout:          time.Second,
	}).normalize(), conn.Close)
	if err != nil {
		_ = conn.Close()
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatal(err)
	}
	return &reviewClientHarness{
		server: server, grpc: grpcServer, listener: listener, conn: conn, client: client,
	}
}

func (h *reviewClientHarness) close() {
	_ = h.client.Close()
	h.grpc.Stop()
	_ = h.conn.Close()
	_ = h.listener.Close()
}

type reviewSnapshotServer struct {
	routecontrolpb.UnimplementedRouteSnapshotServiceServer
	valid        *routecontrolpb.RouteSnapshotEnvelope
	watch        *routecontrolpb.RouteSnapshotEnvelope
	hub          *core.SnapshotWatchHub
	mode         string
	watchStarted chan struct{}
	startOnce    sync.Once
	getCalls     atomic.Int32
}

func (s *reviewSnapshotServer) GetSnapshot(context.Context, *routecontrolpb.GetSnapshotRequest) (*routecontrolpb.RouteSnapshotEnvelope, error) {
	s.getCalls.Add(1)
	return proto.Clone(s.valid).(*routecontrolpb.RouteSnapshotEnvelope), nil
}

func (s *reviewSnapshotServer) GetCurrentRevision(context.Context, *routecontrolpb.GetCurrentRevisionRequest) (*routecontrolpb.RouteSnapshotEnvelope, error) {
	s.getCalls.Add(1)
	return proto.Clone(s.valid).(*routecontrolpb.RouteSnapshotEnvelope), nil
}

func (s *reviewSnapshotServer) WatchFrom(request *routecontrolpb.WatchFromRequest, stream grpc.ServerStreamingServer[routecontrolpb.RouteSnapshotEnvelope]) error {
	s.startOnce.Do(func() { close(s.watchStarted) })
	if s.hub != nil {
		if request == nil {
			return status.Error(codes.InvalidArgument, "watch request required")
		}
		subscription, err := s.hub.Subscribe(stream.Context(), request.GetAfterRevision())
		if err != nil {
			return transportStatusError(err)
		}
		defer subscription.Close()
		if err := stream.Send(proto.Clone(s.watch).(*routecontrolpb.RouteSnapshotEnvelope)); err != nil {
			return err
		}
		for {
			select {
			case <-stream.Context().Done():
				return stream.Context().Err()
			case _, ok := <-subscription.Updates:
				if !ok {
					if err := subscription.Err(); err != nil {
						return transportStatusError(err)
					}
					return nil
				}
				if err := stream.Send(proto.Clone(s.valid).(*routecontrolpb.RouteSnapshotEnvelope)); err != nil {
					return err
				}
			}
		}
	}
	switch s.mode {
	case "no-handshake":
		<-stream.Context().Done()
		return stream.Context().Err()
	case "blocked":
		if err := stream.Send(proto.Clone(s.watch).(*routecontrolpb.RouteSnapshotEnvelope)); err != nil {
			return err
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	case "double":
		if err := stream.Send(proto.Clone(s.watch).(*routecontrolpb.RouteSnapshotEnvelope)); err != nil {
			return err
		}
		if err := stream.Send(proto.Clone(s.watch).(*routecontrolpb.RouteSnapshotEnvelope)); err != nil {
			return err
		}
		return stream.Send(proto.Clone(s.watch).(*routecontrolpb.RouteSnapshotEnvelope))
	default:
		return stream.Send(proto.Clone(s.watch).(*routecontrolpb.RouteSnapshotEnvelope))
	}
}

type reviewServerStream struct {
	ctx      context.Context
	cancel   context.CancelFunc
	sendFn   func(*routecontrolpb.RouteSnapshotEnvelope) error
	sendDone chan struct{}
}

func (s *reviewServerStream) Send(envelope *routecontrolpb.RouteSnapshotEnvelope) error {
	defer close(s.sendDone)
	return s.sendFn(envelope)
}

func (s *reviewServerStream) CancelSend() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *reviewServerStream) Context() context.Context     { return s.ctx }
func (s *reviewServerStream) SendHeader(metadata.MD) error { return nil }
func (s *reviewServerStream) SetHeader(metadata.MD) error  { return nil }
func (s *reviewServerStream) SetTrailer(metadata.MD)       {}
func (s *reviewServerStream) SendMsg(any) error            { return nil }
func (s *reviewServerStream) RecvMsg(any) error            { return nil }

func pkixName(commonName string) pkix.Name {
	return pkix.Name{CommonName: commonName}
}

func harnessTLSConfig(h *transportHarness) *tls.Config {
	config, err := NewClientTLSConfig(h.bundle.clientCert, h.bundle.clientKey, h.bundle.caCert)
	if err != nil {
		panic(err)
	}
	config.ServerName = "server.test"
	return config
}

func waitTransport(t *testing.T, timeout time.Duration, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
