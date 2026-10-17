package gateverification

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	gateverificationpb "farmgame/api/gen/gateverification"
	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/authoritycomposition"
	"farmgame/internal/routecontrol/transport"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"
	redisroutecontrol "farmgame/internal/storage/redis/routecontrol"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const verificationTestCallTimeout = 250 * time.Millisecond

type gateObserverFake struct {
	observation authoritycomposition.ShardObservation
	err         error
	started     chan struct{}
	block       bool
	once        sync.Once
}

type redisEvidenceGateObserver struct {
	store       *redisroutecontrol.TransitionStore
	transition  string
	observation authoritycomposition.ShardObservation
	calls       atomic.Int32
}

func (o *redisEvidenceGateObserver) ObserveShard(ctx context.Context, _ string, _ actorrouting.RouteDomain, _ uint32) (authoritycomposition.ShardObservation, error) {
	recovery, recoveryFound, _, tombstoneFound, err := o.store.ObserveTransitionEvidence(ctx, o.transition)
	if err != nil || !recoveryFound || tombstoneFound || recovery.TransitionID != o.transition || !recovery.Committed || !recovery.Published {
		return authoritycomposition.ShardObservation{}, errors.New("exact Redis evidence unavailable")
	}
	o.calls.Add(1)
	return o.observation, nil
}

type delegatedGateObserver struct{ gateObserverFake }

func (delegatedGateObserver) LeaseAuthorityDelegated() bool { return true }

func (f *gateObserverFake) ObserveShard(ctx context.Context, _ string, _ actorrouting.RouteDomain, _ uint32) (authoritycomposition.ShardObservation, error) {
	if f.started != nil {
		f.once.Do(func() { close(f.started) })
	}
	if f.block {
		<-ctx.Done()
		return authoritycomposition.ShardObservation{}, ctx.Err()
	}
	return f.observation, f.err
}

type verificationCertificateBundle struct {
	caCert     *x509.Certificate
	caKey      crypto.Signer
	caPEM      []byte
	serverCert []byte
	serverKey  []byte
	clientCert []byte
	clientKey  []byte
}

func verificationCertificates(t *testing.T) verificationCertificateBundle {
	t.Helper()
	caKey, ca, caPEM := verificationCertificate(t, nil, nil, "verification-ca", true, nil, nil, nil)
	serverKey, _, serverPEM := verificationCertificate(t, ca, caKey, "server.test", false, []string{"server.test"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	clientKey, _, clientPEM := verificationCertificate(t, ca, caKey, "controller.test", false, []string{"controller.test"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	return verificationCertificateBundle{
		caCert: ca, caKey: caKey, caPEM: caPEM, serverCert: serverPEM, serverKey: verificationPrivateKey(serverKey),
		clientCert: clientPEM, clientKey: verificationPrivateKey(clientKey),
	}
}

func verificationCertificate(t *testing.T, parent *x509.Certificate, parentKey crypto.Signer, commonName string, isCA bool, dns []string, uris []*url.URL, usages []x509.ExtKeyUsage, mutations ...func(*x509.Certificate)) (crypto.Signer, *x509.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: commonName}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: usages, BasicConstraintsValid: true, IsCA: isCA,
		DNSNames: dns, URIs: uris,
	}
	if isCA {
		template.KeyUsage |= x509.KeyUsageCertSign
		template.MaxPathLen = 2
	}
	for _, mutate := range mutations {
		mutate(template)
	}
	if parent == nil {
		parent, parentKey = template, key
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, key.Public(), parentKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return key, certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func verificationPrivateKey(key crypto.Signer) []byte {
	encoded, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		panic(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded})
}

func validGateObservation() authoritycomposition.ShardObservation {
	return authoritycomposition.ShardObservation{
		Realm: "realm-a", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		HolderID: "holder-b", HolderBootID: "boot-b", LogicEndpoint: "holder-b:9000",
		State: actorrouting.ShardStateActive, AssignmentRevision: 2, WriterEpoch: 3, LeaseEpoch: 4,
		LeaseUntilMs: time.Now().Add(time.Minute).UnixMilli(), SnapshotRevision: 5,
	}
}

func TestGateVerificationServiceObservesFarmWorker(t *testing.T) {
	observation := validGateObservation()
	observation.Domain = actorrouting.DomainFarmWorker
	observation.HolderID = "farm-worker-b"
	observation.HolderBootID = "farm-worker-b-boot"
	observation.LogicEndpoint = "farm-worker-b:9201"
	service, err := NewService(
		&gateObserverFake{observation: observation},
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.ObserveShard(
		context.Background(),
		&gateverificationpb.ObserveShardRequest{
			Realm:  observation.Realm,
			Domain: string(actorrouting.DomainFarmWorker),
			Shard:  observation.VirtualShard,
		},
	)
	if err != nil ||
		response.GetDomain() != string(actorrouting.DomainFarmWorker) ||
		response.GetHolderId() != observation.HolderID ||
		response.GetLogicEndpoint() != observation.LogicEndpoint {
		t.Fatalf("worker response=%+v err=%v", response, err)
	}
}

type runningVerificationServer struct {
	server   *Server
	listener net.Listener
	done     chan error
}

func startVerificationServer(t *testing.T, observer ShardObserver, serverCert, serverKey, clientCA []byte, identity string) *runningVerificationServer {
	t.Helper()
	tlsConfig, err := transport.NewServerTLSConfig(serverCert, serverKey, clientCA)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(observer, tlsConfig, []string{identity}, defaultMessageBytes)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	running := &runningVerificationServer{server: server, listener: listener, done: make(chan error, 1)}
	go func() { running.done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-running.done
	})
	return running
}

func newVerificationFleet(t *testing.T, bundle verificationCertificateBundle, peers []Peer) *ClientFleet {
	t.Helper()
	clientTLS, err := transport.NewClientTLSConfig(bundle.clientCert, bundle.clientKey, bundle.caPEM)
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := NewClientFleet(peers, clientTLS, ClientOptions{CallTimeout: verificationTestCallTimeout, MaxMessageBytes: defaultMessageBytes})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fleet.Close() })
	return fleet
}

func TestGateVerificationRealMTLSObservesEveryDirectPeer(t *testing.T) {
	bundle := verificationCertificates(t)
	observation := validGateObservation()
	first := startVerificationServer(t, &gateObserverFake{observation: observation}, bundle.serverCert, bundle.serverKey, bundle.caPEM, "controller.test")
	second := startVerificationServer(t, &gateObserverFake{observation: observation}, bundle.serverCert, bundle.serverKey, bundle.caPEM, "controller.test")
	fleet := newVerificationFleet(t, bundle, []Peer{
		{Name: "gate-a", Address: first.listener.Addr().String(), ServerName: "server.test", ExpectedIdentity: "server.test"},
		{Name: "gate-b", Address: second.listener.Addr().String(), ServerName: "server.test", ExpectedIdentity: "server.test"},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	observed, err := fleet.ObserveShard(ctx, observation.Realm, observation.Domain, observation.VirtualShard)
	if err != nil || len(observed) != 2 || observed[0].Observation != observation || observed[1].Observation != observation {
		t.Fatalf("observed=%+v err=%v", observed, err)
	}
}

func TestGateVerificationFleetObservesFarmWorker(t *testing.T) {
	bundle := verificationCertificates(t)
	observation := validGateObservation()
	observation.Domain = actorrouting.DomainFarmWorker
	observation.HolderID = "farm-worker-b"
	observation.HolderBootID = "farm-worker-b-boot"
	observation.LogicEndpoint = "farm-worker-b:9201"
	running := startVerificationServer(
		t,
		&gateObserverFake{observation: observation},
		bundle.serverCert,
		bundle.serverKey,
		bundle.caPEM,
		"controller.test",
	)
	fleet := newVerificationFleet(t, bundle, []Peer{{
		Name:             "gate-a",
		Address:          running.listener.Addr().String(),
		ServerName:       "server.test",
		ExpectedIdentity: "server.test",
	}})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	observed, err := fleet.ObserveShard(
		ctx,
		observation.Realm,
		observation.Domain,
		observation.VirtualShard,
	)
	if err != nil ||
		len(observed) != 1 ||
		observed[0].Observation != observation {
		t.Fatalf("worker observed=%+v err=%v", observed, err)
	}
}

func TestGateVerificationDNSOnlyClientAuthorizationRejectsEveryNonExactLeaf(t *testing.T) {
	bundle := verificationCertificates(t)
	observer := &gateObserverFake{observation: validGateObservation()}
	running := startVerificationServer(t, observer, bundle.serverCert, bundle.serverKey, bundle.caPEM, "controller.test")
	spiffe := &url.URL{Scheme: "spiffe", Host: "farm.test", Path: "/controller"}
	attacks := []struct {
		name   string
		dns    []string
		uris   []*url.URL
		mutate func(*x509.Certificate)
	}{
		{name: "URI-only", uris: []*url.URL{spiffe}},
		{name: "DNS-plus-URI", dns: []string{"controller.test"}, uris: []*url.URL{spiffe}},
		{name: "IP-plus-DNS", dns: []string{"controller.test"}, mutate: func(v *x509.Certificate) { v.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")} }},
		{name: "CN-only"},
		{name: "multi-DNS", dns: []string{"controller.test", "other.test"}},
		{name: "duplicate-DNS", dns: []string{"controller.test", "controller.test"}},
		{name: "malformed-DNS", dns: []string{"controller_test"}},
	}
	for _, attack := range attacks {
		t.Run(attack.name, func(t *testing.T) {
			mutations := []func(*x509.Certificate){}
			if attack.mutate != nil {
				mutations = append(mutations, attack.mutate)
			}
			key, _, cert := verificationCertificate(t, bundle.caCert, bundle.caKey, "controller.test", false, attack.dns, attack.uris, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, mutations...)
			clientTLS, err := transport.NewClientTLSConfig(cert, verificationPrivateKey(key), bundle.caPEM)
			if err != nil {
				t.Fatal(err)
			}
			fleet, err := NewClientFleet([]Peer{{Name: "gate-a", Address: running.listener.Addr().String(), ServerName: "server.test", ExpectedIdentity: "server.test"}}, clientTLS, ClientOptions{CallTimeout: verificationTestCallTimeout})
			if err != nil {
				t.Fatal(err)
			}
			defer fleet.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := fleet.ObserveShard(ctx, "realm-a", actorrouting.DomainPlayer, 0); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("non-exact client leaf passed Gate ObserveShard: %v", err)
			}
		})
	}
}

func TestGateVerificationDNSOnlyAllowlistStartupValidation(t *testing.T) {
	bundle := verificationCertificates(t)
	tlsConfig, err := transport.NewServerTLSConfig(bundle.serverCert, bundle.serverKey, bundle.caPEM)
	if err != nil {
		t.Fatal(err)
	}
	for name, identities := range map[string][]string{
		"URI":       {"spiffe://farm.test/controller"},
		"IP":        {"127.0.0.1"},
		"uppercase": {"Controller.test"},
		"malformed": {"controller_test"},
		"duplicate": {"controller.test", "controller.test"},
	} {
		t.Run(name, func(t *testing.T) {
			if server, err := NewServer(&gateObserverFake{observation: validGateObservation()}, tlsConfig, identities, defaultMessageBytes); err == nil || server != nil {
				t.Fatalf("DNS-only startup accepted identities=%v err=%v", identities, err)
			}
		})
	}
}

func TestGateVerificationLeaseFreshnessUsesInjectedClocksWithoutSleep(t *testing.T) {
	observation := validGateObservation()
	deadline := time.UnixMilli(observation.LeaseUntilMs)
	service, err := NewServiceWithOptions(&gateObserverFake{observation: observation}, ServiceOptions{Now: func() time.Time { return deadline }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ObserveShard(context.Background(), &gateverificationpb.ObserveShardRequest{Realm: observation.Realm, Domain: string(observation.Domain), Shard: observation.VirtualShard}); status.Code(err) != codes.Unavailable {
		t.Fatalf("service returned fresh observation at deadline: %v", err)
	}

	bundle := verificationCertificates(t)
	running := startVerificationServer(t, &gateObserverFake{observation: observation}, bundle.serverCert, bundle.serverKey, bundle.caPEM, "controller.test")
	clientTLS, err := transport.NewClientTLSConfig(bundle.clientCert, bundle.clientKey, bundle.caPEM)
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := NewClientFleet([]Peer{{Name: "gate-a", Address: running.listener.Addr().String(), ServerName: "server.test", ExpectedIdentity: "server.test"}}, clientTLS, ClientOptions{
		CallTimeout: verificationTestCallTimeout, Now: func() time.Time { return deadline },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fleet.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := fleet.ObserveShard(ctx, observation.Realm, observation.Domain, observation.VirtualShard); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("client accepted observation at deadline: %v", err)
	}
}

func TestGateVerificationDelegatedModeAllowsStaleSnapshotDeadlineWithExplicitAuthorityMode(t *testing.T) {
	observation := validGateObservation()
	deadline := time.UnixMilli(observation.LeaseUntilMs)
	delegated := delegatedGateObserver{gateObserverFake{observation: observation}}
	service, err := NewServiceWithOptions(&delegated, ServiceOptions{Now: func() time.Time { return deadline }, LeaseCheckMode: actorrouting.LeaseCheckAuthorityDelegated})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ObserveShard(context.Background(), &gateverificationpb.ObserveShardRequest{Realm: observation.Realm, Domain: string(observation.Domain), Shard: observation.VirtualShard}); err != nil {
		t.Fatalf("delegated service rejected authority-owned observation: %v", err)
	}

	bundle := verificationCertificates(t)
	running := startVerificationServer(t, &delegated, bundle.serverCert, bundle.serverKey, bundle.caPEM, "controller.test")
	clientTLS, err := transport.NewClientTLSConfig(bundle.clientCert, bundle.clientKey, bundle.caPEM)
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := NewClientFleet([]Peer{{Name: "gate-a", Address: running.listener.Addr().String(), ServerName: "server.test", ExpectedIdentity: "server.test"}}, clientTLS, ClientOptions{
		CallTimeout: verificationTestCallTimeout, Now: func() time.Time { return deadline }, LeaseCheckMode: actorrouting.LeaseCheckAuthorityDelegated,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer fleet.Close()
	observed, err := fleet.ObserveShard(context.Background(), observation.Realm, observation.Domain, observation.VirtualShard)
	if err != nil || len(observed) != 1 {
		t.Fatalf("delegated client observations=%+v err=%v", observed, err)
	}
}

func TestGateVerificationRealMTLSAndRedisExactEvidenceComponentPath(t *testing.T) {
	redisServer := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = redisClient.Close() })
	namespace, err := rediskey.NewNamespace("gate-verification-component", "loopback")
	if err != nil {
		t.Fatal(err)
	}
	options := redisroutecontrol.DefaultOptions(namespace)
	options.OperationTimeout = time.Second
	store, err := redisroutecontrol.NewTransitionStore(redisClient, options)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	recovery := authority.TransitionRecovery{
		TransitionID: "gate-verification-component-transition", Fingerprint: "route-transition-fingerprint-v1:component",
		Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "holder-b", NewHolderBootID: "boot-b",
		Endpoint: "holder-b:9000", RequestedTTLms: 30_000,
		Result: authoritystore.ShardAuthorityTransitionResult{
			Status: authoritystore.ShardTransitionApplied, AcceptedTTLms: 30_000,
			Authority: authoritystore.ShardWriteAuthority{
				AuthorityLayoutVersion: "layout-v1", HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 7,
				Domain: actorrouting.DomainPlayer, VirtualShard: 0, HolderID: "holder-b", HolderBootID: "boot-b",
				State: authoritystore.ShardAuthorityActive, AssignmentRevision: 2, WriterEpoch: 2, LeaseEpoch: 2,
				LeaseUntilMs: now.Add(time.Minute).UnixMilli(), LastTransitionID: "gate-verification-component-transition",
			},
		},
		LastState: actorrouting.ShardStateActive, Published: true, AuthorityAttempted: true,
		AuthorityAttemptedPresent: true, RecoveryWireVersion: authority.TransitionRecoveryWireAuthorityAttempted,
		Committed: true, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.ReserveTransitionRecovery(context.Background(), recovery); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitTransitionRecovery(context.Background(), recovery); err != nil {
		t.Fatal(err)
	}

	bundle := verificationCertificates(t)
	observer := &redisEvidenceGateObserver{store: store, transition: recovery.TransitionID, observation: validGateObservation()}
	running := startVerificationServer(t, observer, bundle.serverCert, bundle.serverKey, bundle.caPEM, "controller.test")
	fleet := newVerificationFleet(t, bundle, []Peer{{Name: "gate-a", Address: running.listener.Addr().String(), ServerName: "server.test", ExpectedIdentity: "server.test"}})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	observed, err := fleet.ObserveShard(ctx, observer.observation.Realm, observer.observation.Domain, observer.observation.VirtualShard)
	if err != nil || len(observed) != 1 || observed[0].Observation != observer.observation || observer.calls.Load() != 1 {
		t.Fatalf("observed=%+v Redis_calls=%d err=%v", observed, observer.calls.Load(), err)
	}
}

func TestGateVerificationClientCloseCancelsAndJoinsInflightRPC(t *testing.T) {
	bundle := verificationCertificates(t)
	observer := &gateObserverFake{observation: validGateObservation(), started: make(chan struct{}), block: true}
	running := startVerificationServer(t, observer, bundle.serverCert, bundle.serverKey, bundle.caPEM, "controller.test")
	clientTLS, err := transport.NewClientTLSConfig(bundle.clientCert, bundle.clientKey, bundle.caPEM)
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := NewClientFleet([]Peer{{Name: "gate-a", Address: running.listener.Addr().String(), ServerName: "server.test", ExpectedIdentity: "server.test"}}, clientTLS, ClientOptions{CallTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	callDone := make(chan error, 1)
	go func() {
		_, callErr := fleet.ObserveShard(context.Background(), "realm-a", actorrouting.DomainPlayer, 0)
		callDone <- callErr
	}()
	select {
	case <-observer.started:
	case <-time.After(2 * time.Second):
		t.Fatal("RPC did not reach observer")
	}
	closeDone := make(chan error, 2)
	go func() { closeDone <- fleet.Close() }()
	go func() { closeDone <- fleet.Close() }()
	for range 2 {
		select {
		case err := <-closeDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Close did not join")
		}
	}
	if err := <-callDone; !errors.Is(err, ErrUnavailable) {
		t.Fatalf("call err=%v", err)
	}
}

func TestGateVerificationTLSAndPeerConfigurationFailClosed(t *testing.T) {
	bundle := verificationCertificates(t)
	validTLS, err := transport.NewClientTLSConfig(bundle.clientCert, bundle.clientKey, bundle.caPEM)
	if err != nil {
		t.Fatal(err)
	}
	for name, peers := range map[string][]Peer{
		"empty":           nil,
		"duplicate-name":  {{Name: "same", Address: "127.0.0.1:1", ServerName: "server.test", ExpectedIdentity: "server.test"}, {Name: "same", Address: "127.0.0.1:2", ServerName: "server.test", ExpectedIdentity: "server.test"}},
		"duplicate-addr":  {{Name: "a", Address: "127.0.0.1:1", ServerName: "server.test", ExpectedIdentity: "server.test"}, {Name: "b", Address: "127.0.0.1:1", ServerName: "server.test", ExpectedIdentity: "server.test"}},
		"load-balanced":   {{Name: "a", Address: "dns:///gate:19091", ServerName: "server.test", ExpectedIdentity: "server.test"}},
		"uppercase-alias": {{Name: "a", Address: "Gate.Internal:19091", ServerName: "server.test", ExpectedIdentity: "server.test"}},
		"padded-address":  {{Name: "a", Address: " gate.internal:19091", ServerName: "server.test", ExpectedIdentity: "server.test"}},
		"zero-port":       {{Name: "a", Address: "gate.internal:0", ServerName: "server.test", ExpectedIdentity: "server.test"}},
		"leading-port":    {{Name: "a", Address: "gate.internal:019091", ServerName: "server.test", ExpectedIdentity: "server.test"}},
		"overflow-port":   {{Name: "a", Address: "gate.internal:65536", ServerName: "server.test", ExpectedIdentity: "server.test"}},
		"malformed-DNS":   {{Name: "a", Address: "gate..internal:19091", ServerName: "server.test", ExpectedIdentity: "server.test"}},
		"alternate-IPv4":  {{Name: "a", Address: "127.0.0.01:19091", ServerName: "server.test", ExpectedIdentity: "server.test"}},
		"alternate-IPv6":  {{Name: "a", Address: "[0:0:0:0:0:0:0:1]:19091", ServerName: "server.test", ExpectedIdentity: "server.test"}},
		"identity-drift":  {{Name: "a", Address: "127.0.0.1:1", ServerName: "server.test", ExpectedIdentity: "other.test"}},
		"ip-identity":     {{Name: "a", Address: "127.0.0.1:1", ServerName: "127.0.0.1", ExpectedIdentity: "127.0.0.1"}},
	} {
		t.Run(name, func(t *testing.T) {
			if fleet, err := NewClientFleet(peers, validTLS, ClientOptions{}); err == nil || fleet != nil {
				t.Fatalf("fleet=%v err=%v", fleet, err)
			}
		})
	}

	attacks := []struct {
		name string
		dns  []string
		uris []*url.URL
	}{
		{name: "CN-only"},
		{name: "multi-SAN", dns: []string{"server.test", "other.test"}},
		{name: "URI-SAN", uris: []*url.URL{{Scheme: "spiffe", Host: "farm.test", Path: "/gate"}}},
	}
	for _, attack := range attacks {
		t.Run(attack.name, func(t *testing.T) {
			key, _, cert := verificationCertificate(t, bundle.caCert, bundle.caKey, "server.test", false, attack.dns, attack.uris, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
			running := startVerificationServer(t, &gateObserverFake{observation: validGateObservation()}, cert, verificationPrivateKey(key), bundle.caPEM, "controller.test")
			fleet := newVerificationFleet(t, bundle, []Peer{{Name: "gate-a", Address: running.listener.Addr().String(), ServerName: "server.test", ExpectedIdentity: "server.test"}})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := fleet.ObserveShard(ctx, "realm-a", actorrouting.DomainPlayer, 0); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("attack passed err=%v", err)
			}
		})
	}

	t.Run("wrong-CA", func(t *testing.T) {
		running := startVerificationServer(t, &gateObserverFake{observation: validGateObservation()}, bundle.serverCert, bundle.serverKey, bundle.caPEM, "controller.test")
		wrongBundle := verificationCertificates(t)
		wrongTrust, err := transport.NewClientTLSConfig(bundle.clientCert, bundle.clientKey, wrongBundle.caPEM)
		if err != nil {
			t.Fatal(err)
		}
		fleet, err := NewClientFleet([]Peer{{Name: "gate-a", Address: running.listener.Addr().String(), ServerName: "server.test", ExpectedIdentity: "server.test"}}, wrongTrust, ClientOptions{CallTimeout: verificationTestCallTimeout})
		if err != nil {
			t.Fatal(err)
		}
		defer fleet.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := fleet.ObserveShard(ctx, "realm-a", actorrouting.DomainPlayer, 0); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("wrong CA passed err=%v", err)
		}
	})

	t.Run("intermediate-identity", func(t *testing.T) {
		intermediateKey, intermediate, intermediatePEM := verificationCertificate(t, bundle.caCert, bundle.caKey, "intermediate.test", true, []string{"server.test"}, nil, nil)
		leafKey, _, leafPEM := verificationCertificate(t, intermediate, intermediateKey, "other.test", false, []string{"other.test"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
		serverChain := append(append([]byte(nil), leafPEM...), intermediatePEM...)
		running := startVerificationServer(t, &gateObserverFake{observation: validGateObservation()}, serverChain, verificationPrivateKey(leafKey), bundle.caPEM, "controller.test")
		fleet := newVerificationFleet(t, bundle, []Peer{{Name: "gate-a", Address: running.listener.Addr().String(), ServerName: "server.test", ExpectedIdentity: "server.test"}})
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := fleet.ObserveShard(ctx, "realm-a", actorrouting.DomainPlayer, 0); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("intermediate identity passed err=%v", err)
		}
	})

	t.Run("metadata-cannot-spoof-client-identity", func(t *testing.T) {
		observer := &gateObserverFake{observation: validGateObservation()}
		running := startVerificationServer(t, observer, bundle.serverCert, bundle.serverKey, bundle.caPEM, "controller.test")
		attackerKey, _, attackerPEM := verificationCertificate(t, bundle.caCert, bundle.caKey, "attacker.test", false, []string{"attacker.test"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
		attackerTLS, err := transport.NewClientTLSConfig(attackerPEM, verificationPrivateKey(attackerKey), bundle.caPEM)
		if err != nil {
			t.Fatal(err)
		}
		fleet, err := NewClientFleet([]Peer{{Name: "gate-a", Address: running.listener.Addr().String(), ServerName: "server.test", ExpectedIdentity: "server.test"}}, attackerTLS, ClientOptions{CallTimeout: verificationTestCallTimeout})
		if err != nil {
			t.Fatal(err)
		}
		defer fleet.Close()
		base, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		ctx := metadata.NewOutgoingContext(base, metadata.Pairs("caller_role", "controller", "client_identity", "controller.test"))
		if _, err := fleet.ObserveShard(ctx, "realm-a", actorrouting.DomainPlayer, 0); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("metadata identity spoof passed err=%v", err)
		}
	})

	tls12 := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: validTLS.Certificates, RootCAs: validTLS.RootCAs}
	if fleet, err := NewClientFleet([]Peer{{Name: "a", Address: "127.0.0.1:1", ServerName: "server.test", ExpectedIdentity: "server.test"}}, tls12, ClientOptions{}); err == nil || fleet != nil {
		t.Fatalf("TLS 1.2 fleet=%v err=%v", fleet, err)
	}
}
