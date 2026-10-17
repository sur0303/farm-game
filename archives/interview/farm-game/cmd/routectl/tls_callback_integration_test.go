package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	routeplanpb "farmgame/api/gen/routeplan"
	"farmgame/internal/routecontrol/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type statusOnlyRoutePlanServer struct {
	routeplanpb.UnimplementedRoutePlanServiceServer
}

func (statusOnlyRoutePlanServer) GetStatus(context.Context, *routeplanpb.GetStatusRequest) (*routeplanpb.GetStatusResponse, error) {
	return &routeplanpb.GetStatusResponse{
		ControllerId:     "controller-test",
		Boot:             "boot-test",
		Realm:            "realm-test",
		SnapshotRevision: 5,
		Ready:            true,
	}, nil
}

func TestRoutectlVerifyConnectionObservesLiveCallbackState(t *testing.T) {
	identity := "route-controller.test"
	bundle := newRoutectlTLSBundle(t, identity, nil)

	listener, stop := startRoutectlTLSGRPCServer(t, bundle.serverTLS)
	defer stop()

	var observed tls.ConnectionState
	var observedCalls int
	clientTLS := bundle.clientTLS.Clone()
	clientTLS.ServerName = identity
	clientTLS.VerifyConnection = func(state tls.ConnectionState) error {
		observedCalls++
		observed = state
		return verifyRouteControllerConnection(state, identity)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, listener.Addr().String(),
		grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)), grpc.WithBlock())
	if err != nil {
		t.Fatalf("exact identity dial failed: %v", err)
	}
	defer conn.Close()

	if observedCalls != 1 {
		t.Fatalf("VerifyConnection calls=%d want 1", observedCalls)
	}
	if observed.HandshakeComplete {
		t.Fatal("live VerifyConnection unexpectedly reported HandshakeComplete=true")
	}
	if observed.Version < tls.VersionTLS13 {
		t.Fatalf("version=%x want TLS1.3+", observed.Version)
	}
	if len(observed.VerifiedChains) == 0 || len(observed.VerifiedChains[0]) == 0 {
		t.Fatal("verified chains missing in live callback")
	}
	leaf := observed.VerifiedChains[0][0]
	if leaf.Subject.CommonName != "" || len(leaf.DNSNames) != 1 || leaf.DNSNames[0] != identity {
		t.Fatalf("callback leaf cn=%q dns=%v", leaf.Subject.CommonName, leaf.DNSNames)
	}
	if err := verifyRouteControllerConnection(observed, identity); err != nil {
		t.Fatalf("verify on observed callback state: %v", err)
	}

	client := routeplanpb.NewRoutePlanServiceClient(conn)
	status, err := client.GetStatus(ctx, &routeplanpb.GetStatusRequest{})
	if err != nil || status == nil || !status.GetReady() || status.GetSnapshotRevision() != 5 {
		t.Fatalf("GetStatus ready=%v rev=%d err=%v", status.GetReady(), status.GetSnapshotRevision(), err)
	}

	files := writeRoutectlTLSFiles(t, bundle)
	dialed, err := dialRoutePlan(globalConfig{
		target: listener.Addr().String(), serverName: identity, expectedServerIdentity: identity,
		certFile: files.clientCert, keyFile: files.clientKey, caFile: files.caCert,
		dialTimeout: time.Second, rpcTimeout: time.Second, maxMessageBytes: defaultMaxMessage,
	})
	if err != nil {
		t.Fatalf("dialRoutePlan exact identity failed: %v", err)
	}
	defer dialed.Close()
}

func TestRoutectlVerifyConnectionLiveRejectsWrongIdentityChainAndTLS12(t *testing.T) {
	identity := "route-controller.test"
	good := newRoutectlTLSBundle(t, identity, nil)

	t.Run("wrong expected identity", func(t *testing.T) {
		listener, stop := startRoutectlTLSGRPCServer(t, good.serverTLS)
		defer stop()
		// Keep ServerName aligned with the leaf so chain verification succeeds and
		// VerifyConnection is the rejector for the mismatched expected identity.
		err := dialRoutectlWithVerifyExpecting(t, listener.Addr().String(), good.clientTLS, identity, "other-controller.test")
		if err == nil {
			t.Fatal("wrong identity dial unexpectedly succeeded")
		}
		if redactedTransportError(err) == "" {
			t.Fatal("empty dial error")
		}
	})

	t.Run("extra DNS SAN", func(t *testing.T) {
		extra := newRoutectlTLSBundle(t, identity, []string{"route-controller.test", "extra.controller.test"})
		listener, stop := startRoutectlTLSGRPCServer(t, extra.serverTLS)
		defer stop()
		if err := dialRoutectlWithVerifyExpecting(t, listener.Addr().String(), extra.clientTLS, identity, identity); err == nil {
			t.Fatal("multi-SAN dial unexpectedly succeeded")
		}
	})

	t.Run("untrusted chain", func(t *testing.T) {
		other := newRoutectlTLSBundle(t, identity, nil)
		listener, stop := startRoutectlTLSGRPCServer(t, good.serverTLS)
		defer stop()
		if err := dialRoutectlWithVerifyExpecting(t, listener.Addr().String(), other.clientTLS, identity, identity); err == nil {
			t.Fatal("foreign CA dial unexpectedly succeeded")
		}
	})

	t.Run("tls12 only server", func(t *testing.T) {
		serverTLS := good.serverTLS.Clone()
		serverTLS.MinVersion = tls.VersionTLS12
		serverTLS.MaxVersion = tls.VersionTLS12
		listener, stop := startRoutectlTLSGRPCServer(t, serverTLS)
		defer stop()
		if err := dialRoutectlWithVerifyExpecting(t, listener.Addr().String(), good.clientTLS, identity, identity); err == nil {
			t.Fatal("TLS1.2 dial unexpectedly succeeded")
		}
	})
}

type routectlTLSBundle struct {
	serverTLS *tls.Config
	clientTLS *tls.Config
	caPEM     []byte
	clientPEM []byte
	clientKey []byte
	serverPEM []byte
	serverKey []byte
}

type routectlTLSFiles struct {
	caCert, clientCert, clientKey string
}

func newRoutectlTLSBundle(t *testing.T, primaryDNS string, dnsNames []string) routectlTLSBundle {
	t.Helper()
	if len(dnsNames) == 0 {
		dnsNames = []string{primaryDNS}
	}
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "routectl-test-ca"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	serverPEM, serverKeyPEM := mintRoutectlLeaf(t, caCert, caKey, dnsNames, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, now)
	clientPEM, clientKeyPEM := mintRoutectlLeaf(t, caCert, caKey, []string{"operator.test"}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, now)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	serverTLS, err := transport.NewServerTLSConfig(serverPEM, serverKeyPEM, caPEM)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS, err := transport.NewClientTLSConfig(clientPEM, clientKeyPEM, caPEM)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS = clientTLS.Clone()
	clientTLS.MinVersion = tls.VersionTLS13
	clientTLS.ServerName = primaryDNS
	return routectlTLSBundle{
		serverTLS: serverTLS, clientTLS: clientTLS,
		caPEM: caPEM, clientPEM: clientPEM, clientKey: clientKeyPEM, serverPEM: serverPEM, serverKey: serverKeyPEM,
	}
}

func mintRoutectlLeaf(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey, dnsNames []string, usages []x509.ExtKeyUsage, now time.Time) ([]byte, []byte) {
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
		SerialNumber: serial,
		Subject:      pkix.Name{},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usages,
		DNSNames:     append([]string(nil), dnsNames...),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

func startRoutectlTLSGRPCServer(t *testing.T, serverTLS *tls.Config) (net.Listener, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	routeplanpb.RegisterRoutePlanServiceServer(server, statusOnlyRoutePlanServer{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	return listener, func() {
		server.Stop()
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("routeplan TLS server did not stop")
		}
	}
}

func dialRoutectlWithVerifyExpecting(t *testing.T, target string, baseClient *tls.Config, serverName, expected string) error {
	t.Helper()
	clientTLS := baseClient.Clone()
	clientTLS.MinVersion = tls.VersionTLS13
	clientTLS.ServerName = serverName
	clientTLS.VerifyConnection = func(state tls.ConnectionState) error {
		return verifyRouteControllerConnection(state, expected)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, target,
		grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)), grpc.WithBlock())
	if err != nil {
		return err
	}
	_ = conn.Close()
	return nil
}

func writeRoutectlTLSFiles(t *testing.T, bundle routectlTLSBundle) routectlTLSFiles {
	t.Helper()
	dir := t.TempDir()
	files := routectlTLSFiles{
		caCert:     filepath.Join(dir, "ca.crt"),
		clientCert: filepath.Join(dir, "client.crt"),
		clientKey:  filepath.Join(dir, "client.key"),
	}
	for path, payload := range map[string][]byte{
		files.caCert: bundle.caPEM, files.clientCert: bundle.clientPEM, files.clientKey: bundle.clientKey,
	} {
		if err := os.WriteFile(path, payload, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return files
}

func redactedTransportError(err error) string {
	if err == nil {
		return ""
	}
	// Keep assertions on presence only; never return certificate or endpoint details.
	return "dial_failed"
}
