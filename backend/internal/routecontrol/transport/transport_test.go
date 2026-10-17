package transport

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"reflect"
	"testing"
	"time"

	routecontrolpb "farmgame/api/gen/routecontrol"
	"farmgame/internal/actorrouting"
	core "farmgame/internal/routecontrol"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func TestRouteControlProtoSchemaAndWireGolden(t *testing.T) {
	message := &routecontrolpb.RouteSnapshotEnvelope{
		Schema:        "route-snapshot-v1",
		Realm:         "r",
		Revision:      7,
		CanonicalJson: []byte("{}"),
		Checksum:      "c",
		Topology:      "t",
		CommittedAt:   9,
		ControllerId:  "c1",
		Boot:          "b1",
	}
	raw, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	const wantHex = "0a11726f7574652d736e617073686f742d7631120172180722027b7d2a01633201743809420263314a026231"
	if got := hex.EncodeToString(raw); got != wantHex {
		t.Fatalf("wire golden=%s want=%s", got, wantHex)
	}

	descriptor := message.ProtoReflect().Descriptor()
	wantFields := map[string]protoreflect.FieldNumber{
		"schema": 1, "realm": 2, "revision": 3, "canonical_json": 4,
		"checksum": 5, "topology": 6, "committed_at": 7,
		"controller_id": 8, "boot": 9,
	}
	for name, number := range wantFields {
		field := descriptor.Fields().ByName(protoreflect.Name(name))
		if field == nil || field.Number() != number {
			t.Fatalf("field %s=%d missing or renumbered", name, number)
		}
	}
	service, err := protoregistry.GlobalFiles.FindDescriptorByName("routecontrol.RouteSnapshotService")
	if err != nil {
		t.Fatal(err)
	}
	serviceDescriptor := service.(protoreflect.ServiceDescriptor)
	if got := serviceDescriptor.Methods().Len(); got != 3 {
		t.Fatalf("service methods=%d want 3", got)
	}
	for _, name := range []string{"GetSnapshot", "GetCurrentRevision", "WatchFrom"} {
		if serviceDescriptor.Methods().ByName(protoreflect.Name(name)) == nil {
			t.Fatalf("service method %s missing", name)
		}
	}
	if method := serviceDescriptor.Methods().ByName("WatchFrom"); method == nil || !method.IsStreamingServer() || method.IsStreamingClient() {
		t.Fatal("WatchFrom must be server-streaming only")
	}
}

func TestRouteSnapshotTransportFetchCurrentRevisionAndWatch(t *testing.T) {
	harness := newTransportHarness(t, "controller-boot-a", 1<<20)
	defer harness.close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := harness.client.FetchFullSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(harness.snapshot, got) {
		t.Fatalf("FetchFullSnapshot mismatch: got=%+v want=%+v", got, harness.snapshot)
	}
	current, err := harness.client.GetCurrentRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, current) {
		t.Fatal("GetCurrentRevision did not return the complete current snapshot")
	}

	watchCtx, watchCancel := context.WithCancel(context.Background())
	updates, err := harness.client.WatchFrom(watchCtx, 1)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case handshake := <-updates:
		if !reflect.DeepEqual(handshake, harness.snapshot) {
			t.Fatal("Watch handshake did not carry the complete current snapshot")
		}
	case <-time.After(time.Second):
		t.Fatal("Watch did not deliver its mandatory handshake")
	}
	next := transportSnapshot(2)
	if result, err := harness.store.CompareAndSwap(ctx, core.NestedControlOperationToken{}, 1, next); err != nil || result.Status != core.SnapshotCASCommitted {
		t.Fatalf("CAS result=%v err=%v", result.Status, err)
	}
	if err := harness.hub.Notify(ctx, next); err != nil {
		t.Fatal(err)
	}
	select {
	case update := <-updates:
		if update.GlobalRevision != 2 {
			t.Fatalf("watch revision=%d want 2", update.GlobalRevision)
		}
	case <-time.After(time.Second):
		t.Fatal("watch did not deliver revision 2")
	}
	watchCancel()
	select {
	case _, ok := <-updates:
		if ok {
			t.Fatal("watch channel remained open after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("watch channel did not close after cancellation")
	}
}

func TestRouteSnapshotTransportRejectsUnauthenticatedAndDirectUnauthenticatedCall(t *testing.T) {
	harness := newTransportHarness(t, "controller-boot-a", 1<<20)
	defer harness.close()

	unauthenticated := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    harness.bundle.serverRoots,
		ServerName: "server.test",
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet-unauth",
		grpc.WithContextDialer(harness.dialer),
		grpc.WithTransportCredentials(credentials.NewTLS(unauthenticated)),
		grpc.WithBlock(),
	)
	if err == nil {
		defer conn.Close()
		_, err = routecontrolpb.NewRouteSnapshotServiceClient(conn).GetSnapshot(ctx, &routecontrolpb.GetSnapshotRequest{})
	}
	if err == nil {
		t.Fatal("unauthenticated TLS client unexpectedly connected")
	}
	if _, err := harness.service.GetSnapshot(context.Background(), &routecontrolpb.GetSnapshotRequest{}); err == nil {
		t.Fatal("direct request without TLS peer unexpectedly succeeded")
	}
}

func TestRouteSnapshotTransportRejectsMalformedEnvelopeAndPinsIdentityBootRealmTopology(t *testing.T) {
	stored, err := core.CanonicalizeRouteSnapshot(transportSnapshot(1))
	if err != nil {
		t.Fatal(err)
	}
	valid, err := storedToEnvelope(stored, "controller-a", "boot-a", "transport-test", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*routecontrolpb.RouteSnapshotEnvelope)
		want   error
	}{
		{"schema", func(envelope *routecontrolpb.RouteSnapshotEnvelope) { envelope.Schema = "route-snapshot-v0" }, core.ErrSnapshotSchemaMismatch},
		{"checksum", func(envelope *routecontrolpb.RouteSnapshotEnvelope) {
			replacement := byte('0')
			if envelope.Checksum[0] == replacement {
				replacement = '1'
			}
			envelope.Checksum = string(replacement) + envelope.Checksum[1:]
		}, core.ErrSnapshotChecksumMismatch},
		{"topology", func(envelope *routecontrolpb.RouteSnapshotEnvelope) {
			replacement := byte('0')
			if envelope.Topology[0] == replacement {
				replacement = '1'
			}
			envelope.Topology = string(replacement) + envelope.Topology[1:]
		}, core.ErrSnapshotTopologyMismatch},
		{"canonical json", func(envelope *routecontrolpb.RouteSnapshotEnvelope) {
			envelope.CanonicalJson = []byte(`{"realm":"transport-test"}`)
		}, core.ErrSnapshotEnvelopeMalformed},
		{"controller missing", func(envelope *routecontrolpb.RouteSnapshotEnvelope) { envelope.ControllerId = "" }, ErrEnvelopeMalformed},
		{"boot missing", func(envelope *routecontrolpb.RouteSnapshotEnvelope) { envelope.Boot = "" }, ErrEnvelopeMalformed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := proto.Clone(valid).(*routecontrolpb.RouteSnapshotEnvelope)
			test.mutate(candidate)
			if _, err := decodeEnvelope(candidate, 1<<20); !errors.Is(err, test.want) {
				t.Fatalf("decode err=%v want %v", err, test.want)
			}
		})
	}

	client := &SnapshotClient{
		opts: normalizedClientOptions{
			expectedControllerID: "controller-a",
			expectedRealm:        "transport-test",
			maxMessageBytes:      1 << 20,
		},
		closedCh: make(chan struct{}),
	}
	if _, err := client.validateEnvelope(valid); err != nil {
		t.Fatal(err)
	}
	identityDrift := proto.Clone(valid).(*routecontrolpb.RouteSnapshotEnvelope)
	identityDrift.ControllerId = "controller-b"
	if _, err := client.validateEnvelope(identityDrift); !errors.Is(err, ErrEnvelopeControllerMismatch) {
		t.Fatalf("controller drift err=%v", err)
	}
	bootDrift := proto.Clone(valid).(*routecontrolpb.RouteSnapshotEnvelope)
	bootDrift.Boot = "boot-b"
	if _, err := client.validateEnvelope(bootDrift); !errors.Is(err, ErrEnvelopeBootDrift) {
		t.Fatalf("boot drift err=%v", err)
	}
	topologySnapshot := transportSnapshot(2)
	topologySnapshot.HashSeed++
	topologyStored, err := core.CanonicalizeRouteSnapshot(topologySnapshot)
	if err != nil {
		t.Fatal(err)
	}
	topologyDrift, err := storedToEnvelope(topologyStored, "controller-a", "boot-a", "transport-test", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.validateEnvelope(topologyDrift); !errors.Is(err, ErrEnvelopeTopologyDrift) {
		t.Fatalf("topology drift err=%v", err)
	}
}

func TestRouteSnapshotTransportEnforcesMessageLimit(t *testing.T) {
	stored, err := core.CanonicalizeRouteSnapshot(transportSnapshot(1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storedToEnvelope(stored, "controller-a", "boot-a", "transport-test", 16); !errors.Is(err, ErrEnvelopeCapacity) {
		t.Fatalf("server size err=%v want capacity", err)
	}
	large := &routecontrolpb.RouteSnapshotEnvelope{
		Schema:        "route-snapshot-v1",
		Realm:         "transport-test",
		CanonicalJson: make([]byte, 128),
		ControllerId:  "controller-a",
		Boot:          "boot-a",
	}
	if _, err := decodeEnvelope(large, 32); !errors.Is(err, ErrEnvelopeCapacity) {
		t.Fatalf("client size err=%v want capacity", err)
	}
}

func TestRouteSnapshotTransportConstructorsRequireExpectedIdentityAndMTLS(t *testing.T) {
	harness := newTransportHarness(t, "controller-boot-a", 1<<20)
	defer harness.close()
	if _, err := newClient(harness.conn, ClientOptions{}.normalize(), harness.conn.Close); !errors.Is(err, ErrEnvelopeControllerMismatch) {
		t.Fatalf("client without expected controller err=%v", err)
	}
	if _, err := NewGRPCServer(harness.service, nil); !errors.Is(err, ErrTLSRequired) {
		t.Fatalf("nil server TLS err=%v", err)
	}
	tlsConfig, err := NewServerTLSConfig(harness.bundle.serverCert, harness.bundle.serverKey, harness.bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}
	tlsConfig.ClientAuth = tls.RequireAnyClientCert
	if _, err := NewGRPCServer(harness.service, tlsConfig); !errors.Is(err, ErrTLSConfig) {
		t.Fatalf("one-way/non-verifying server TLS err=%v", err)
	}
}

func TestRouteSnapshotTransportBlockedWatchCancellationRemovesHubClient(t *testing.T) {
	harness := newTransportHarness(t, "controller-boot-a", 1<<20)
	defer harness.close()
	ctx, cancel := context.WithCancel(context.Background())
	updates, err := harness.client.WatchFrom(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case handshake := <-updates:
		if handshake.GlobalRevision != 1 {
			t.Fatalf("handshake revision=%d want 1", handshake.GlobalRevision)
		}
	case <-time.After(time.Second):
		t.Fatal("Watch handshake was not published")
	}
	waitUntilTransport(t, time.Second, func() bool { return harness.hub.ClientCount() == 1 })
	cancel()
	waitUntilTransport(t, time.Second, func() bool { return harness.hub.ClientCount() == 0 })
	select {
	case _, ok := <-updates:
		if ok {
			t.Fatal("canceled watch delivered a message")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled watch channel remained open")
	}
}

type transportHarness struct {
	store    *core.MemoryRouteSnapshotStore
	hub      *core.SnapshotWatchHub
	service  *RouteSnapshotServer
	server   *grpc.Server
	sealed   *SealedGRPCServer
	listener *bufconn.Listener
	conn     *grpc.ClientConn
	client   *SnapshotClient
	dialer   func(context.Context, string) (net.Conn, error)
	bundle   certificateBundle
	snapshot actorrouting.RouteSnapshot
}

func newTransportHarness(t *testing.T, boot string, maxMessageBytes int) *transportHarness {
	return newTransportHarnessWithAuthorizer(t, boot, maxMessageBytes, nil)
}

func newTransportHarnessWithAuthorizer(t *testing.T, boot string, maxMessageBytes int, authorizer *MethodAuthorizer, registrations ...GRPCServiceRegistration) *transportHarness {
	t.Helper()
	bundle := newCertificateBundle(t)
	store := core.NewMemoryRouteSnapshotStore()
	snapshot := transportSnapshot(1)
	if _, err := store.Bootstrap(context.Background(), core.NestedControlOperationToken{}, snapshot); err != nil {
		t.Fatal(err)
	}
	hub, err := core.NewSnapshotWatchHub(store, core.SnapshotWatchHubOptions{
		MaxClients:       16,
		QueueSize:        8,
		MaxSnapshotBytes: maxMessageBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewRouteSnapshotServer(hub, ServerOptions{
		ControllerID:          "controller-a",
		Boot:                  boot,
		Realm:                 "transport-test",
		AllowedClientIdentity: []string{"client.test"},
		MaxMessageBytes:       maxMessageBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, err := NewServerTLSConfig(bundle.serverCert, bundle.serverKey, bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}
	serverOptions := GRPCServerOptions{
		MaxMessageBytes:  maxMessageBytes,
		MethodAuthorizer: authorizer,
	}
	var server *grpc.Server
	var sealed *SealedGRPCServer
	if len(registrations) > 0 {
		sealed, err = NewMultiServiceGRPCServer(service, serverTLS, serverOptions, registrations...)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		server, err = NewGRPCServer(service, serverTLS, serverOptions)
		if err != nil {
			t.Fatal(err)
		}
	}
	listener := bufconn.Listen(1 << 20)
	if sealed != nil {
		go func() { _ = sealed.Serve(listener) }()
	} else {
		go func() { _ = server.Serve(listener) }()
	}
	dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	clientTLS, err := NewClientTLSConfig(bundle.clientCert, bundle.clientKey, bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS.ServerName = "server.test"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)),
		grpc.WithBlock(),
	)
	if err != nil {
		if sealed != nil {
			sealed.Stop()
		} else {
			server.Stop()
		}
		listener.Close()
		hub.Close()
		t.Fatal(err)
	}
	client, err := newClient(conn, ClientOptions{
		ExpectedControllerID: "controller-a",
		ExpectedRealm:        "transport-test",
		MaxMessageBytes:      maxMessageBytes,
		CallTimeout:          time.Second,
	}.normalize(), conn.Close)
	if err != nil {
		conn.Close()
		if sealed != nil {
			sealed.Stop()
		} else {
			server.Stop()
		}
		listener.Close()
		hub.Close()
		t.Fatal(err)
	}
	return &transportHarness{
		store: store, hub: hub, service: service, server: server, sealed: sealed,
		listener: listener, conn: conn, client: client, dialer: dialer,
		bundle: bundle, snapshot: snapshot,
	}
}

func (h *transportHarness) close() {
	_ = h.client.Close()
	_ = h.conn.Close()
	if h.sealed != nil {
		h.sealed.Stop()
	} else if h.server != nil {
		h.server.Stop()
	}
	_ = h.listener.Close()
	_ = h.hub.Close()
}

type certificateBundle struct {
	caCert      []byte
	caKey       *rsa.PrivateKey
	serverCert  []byte
	serverKey   []byte
	clientCert  []byte
	clientKey   []byte
	serverRoots *x509.CertPool
}

func newCertificateBundle(t *testing.T) certificateBundle {
	t.Helper()
	caKey, caCert, caPEM := makeCertificate(t, nil, nil, "routecontrol-ca", true, nil)
	serverKey, _, serverPEM := makeCertificate(t, caCert, caKey, "server.test", false, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	clientKey, _, clientPEM := makeCertificate(t, caCert, caKey, "client.test", false, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("append test CA")
	}
	return certificateBundle{
		caKey:  caKey,
		caCert: caPEM, serverCert: serverPEM, serverKey: pemPrivateKey(serverKey),
		clientCert: clientPEM, clientKey: pemPrivateKey(clientKey), serverRoots: roots,
	}
}

func makeCertificate(t *testing.T, parent *x509.Certificate, parentKey *rsa.PrivateKey, name string, isCA bool, usages []x509.ExtKeyUsage) (*rsa.PrivateKey, *x509.Certificate, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           usages,
		BasicConstraintsValid: true,
		IsCA:                  isCA,
		DNSNames:              []string{name},
	}
	if isCA {
		template.KeyUsage |= x509.KeyUsageCertSign
		template.DNSNames = nil
	}
	if parent == nil {
		parent = template
		parentKey = key
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return key, certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func pemPrivateKey(key *rsa.PrivateKey) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

func transportSnapshot(revision uint64) actorrouting.RouteSnapshot {
	leaseUntil := time.Now().Add(time.Hour).UnixMilli()
	return actorrouting.RouteSnapshot{
		Realm: "transport-test", GlobalRevision: revision,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608080001,
		Player: actorrouting.DomainSnapshot{
			ShardCount: 1,
			Routes: []actorrouting.ShardRoute{{
				HolderIndex: 0, State: actorrouting.ShardStateActive,
				ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
				LeaseUntilMs: leaseUntil,
			}},
		},
		Farm: actorrouting.DomainSnapshot{
			ShardCount: 1,
			Routes: []actorrouting.ShardRoute{{
				HolderIndex: 1, State: actorrouting.ShardStateActive,
				ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
				LeaseUntilMs: leaseUntil,
			}},
		},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "player-a", BootID: "player-boot", Endpoint: "player:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-a", BootID: "farm-boot", Endpoint: "farm:9001", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
}

func waitUntilTransport(t *testing.T, timeout time.Duration, predicate func() bool) {
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
