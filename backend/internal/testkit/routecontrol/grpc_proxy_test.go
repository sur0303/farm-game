package routecontrol

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gateverificationpb "farmgame/api/gen/gateverification"
	"farmgame/internal/actorrouting"
	"farmgame/internal/authoritycomposition"
	"farmgame/internal/routecontrol/gateverification"
	"farmgame/internal/routecontrol/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestSnapshotProxyRecordsOnlySafeRPCErrorCode(t *testing.T) {
	proxy := &SnapshotForwardProxy{}
	proxy.recordSnapshot("GetSnapshot", nil, false, false, status.Error(codes.Unavailable, "private endpoint detail"))
	events := proxy.Events()
	if len(events) != 1 || events[0].UpstreamOK || events[0].ErrorCode != codes.Unavailable.String() {
		t.Fatalf("events=%+v", events)
	}
}

func TestGateVerifierProxyStaleRevisionFailClosedAndClear(t *testing.T) {
	dir := t.TempDir()
	serverDNS := "gate-verify.rt2q.test"
	clientDNS := "controller-gate.rt2q.test"
	caFile := filepath.Join(dir, "ca.pem")
	serverCert, serverKey := mustWriteTLSLeafUnderCA(t, dir, "server", serverDNS)
	clientCert, clientKey := mustWriteTLSLeafUnderCA(t, dir, "client", clientDNS)

	observer := staticShardObserver{observation: authoritycomposition.ShardObservation{
		Realm: "realm", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		HolderID: "holder-b", HolderBootID: "boot-b", LogicEndpoint: "127.0.0.1:9",
		State: actorrouting.ShardStateActive, AssignmentRevision: 2, WriterEpoch: 2, LeaseEpoch: 2,
		LeaseUntilMs: time.Now().Add(time.Minute).UnixMilli(), SnapshotRevision: 7,
	}}
	upstreamTLS, err := transport.LoadServerTLSConfig(serverCert, serverKey, caFile)
	if err != nil {
		t.Fatal(err)
	}
	upstream, err := gateverification.NewServer(observer, upstreamTLS, []string{clientDNS}, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	upstreamListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstreamListener.Close()
	go func() { _ = upstream.Serve(upstreamListener) }()
	defer upstream.Stop()

	proxy, err := StartGateVerifierForwardProxy("gate-1", ForwardTLSConfig{
		ServerCertFile: serverCert, ServerKeyFile: serverKey, ClientCAFile: caFile,
		AllowedClientIdentity: clientDNS, UpstreamTarget: upstreamListener.Addr().String(), UpstreamServerName: serverDNS,
		UpstreamClientCert: clientCert, UpstreamClientKey: clientKey, UpstreamServerCA: caFile,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	if err := proxy.ArmFault(GateFaultStaleRevision); err != nil {
		t.Fatal(err)
	}

	clientTLS, err := transport.LoadClientTLSConfig(clientCert, clientKey, caFile)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err = transport.BindExactServerIdentity(clientTLS, serverDNS, serverDNS)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, proxy.Address(), grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := gateverificationpb.NewGateVerificationServiceClient(conn)
	stale, err := client.ObserveShard(ctx, &gateverificationpb.ObserveShardRequest{Realm: "realm", Domain: "PLAYER", Shard: 0})
	if err != nil || stale.GetSnapshotRevision() != 6 {
		t.Fatalf("stale observe=%+v err=%v", stale, err)
	}
	proxy.ClearFault()
	current, err := client.ObserveShard(ctx, &gateverificationpb.ObserveShardRequest{Realm: "realm", Domain: "PLAYER", Shard: 0})
	if err != nil || current.GetSnapshotRevision() != 7 {
		t.Fatalf("cleared observe=%+v err=%v", current, err)
	}
	events := proxy.Events()
	if len(events) < 2 || !events[0].Mutated || events[0].Fault != string(GateFaultStaleRevision) || !events[1].Identical {
		t.Fatalf("events=%+v", events)
	}
	if err := proxy.ArmFault(GateFaultUnavailable); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ObserveShard(ctx, &gateverificationpb.ObserveShardRequest{Realm: "realm", Domain: "PLAYER", Shard: 0}); status.Code(err) != codes.Unavailable {
		t.Fatalf("unavailable fault err=%v", err)
	}
	proxy.ClearFault()
	if err := proxy.ArmFault(GateFaultPending); err != nil {
		t.Fatal(err)
	}
	pending, err := client.ObserveShard(ctx, &gateverificationpb.ObserveShardRequest{Realm: "realm", Domain: "PLAYER", Shard: 0})
	if err != nil || pending.GetState() != "ASSIGNING" {
		t.Fatalf("pending observe=%+v err=%v", pending, err)
	}
	proxy.ClearFault()
	if err := proxy.ArmFault(GateFaultUnknown); err != nil {
		t.Fatal(err)
	}
	unknown, err := client.ObserveShard(ctx, &gateverificationpb.ObserveShardRequest{Realm: "realm", Domain: "PLAYER", Shard: 0})
	if err != nil || unknown.GetWriterEpoch() != 0 {
		t.Fatalf("unknown observe=%+v err=%v", unknown, err)
	}
	proxy.ClearFault()
	if err := proxy.ArmFault(GateFaultMissingReplay); err != nil {
		t.Fatal(err)
	}
	missing, err := client.ObserveShard(ctx, &gateverificationpb.ObserveShardRequest{Realm: "realm", Domain: "PLAYER", Shard: 0})
	if err != nil || missing.GetAssignmentRevision() != 1 || !strings.HasSuffix(missing.GetHolderBootId(), "-missing-replay") {
		t.Fatalf("missing-replay observe=%+v err=%v", missing, err)
	}
	proxy.ClearFault()
}

type staticShardObserver struct {
	observation authoritycomposition.ShardObservation
}

func (s staticShardObserver) ObserveShard(context.Context, string, actorrouting.RouteDomain, uint32) (authoritycomposition.ShardObservation, error) {
	return s.observation, nil
}

func mustWriteTLSLeafUnderCA(t *testing.T, dir, name, dns string) (certFile, keyFile string) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "rt2q-test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign, BasicConstraintsValid: true,
	}
	caPath := filepath.Join(dir, "ca.pem")
	if _, err := os.Stat(caPath); err != nil {
		caDER, createErr := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
		if createErr != nil {
			t.Fatal(createErr)
		}
		if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
			t.Fatal(err)
		}
		keyDER, marshalErr := x509.MarshalECPrivateKey(caKey)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if err := os.WriteFile(filepath.Join(dir, "ca.key"), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatal(err)
	}
	caBlock, _ := pem.Decode(caPEM)
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	caKeyPEM, err := os.ReadFile(filepath.Join(dir, "ca.key"))
	if err != nil {
		t.Fatal(err)
	}
	caKeyBlock, _ := pem.Decode(caKeyPEM)
	parsedCAKey, err := x509.ParseECPrivateKey(caKeyBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{dns},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, parsedCAKey)
	if err != nil {
		t.Fatal(err)
	}
	certFile = filepath.Join(dir, name+".crt")
	keyFile = filepath.Join(dir, name+".key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}
