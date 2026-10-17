package holdercontrol

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	holdercontrolpb "farmgame/api/gen/holdercontrol"
	routeTransport "farmgame/internal/routecontrol/transport"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type holderTLSBundle struct {
	caCert, serverCert, serverKey []byte
	clientCert, clientKey         []byte
	caX509                        *x509.Certificate
	caKey                         crypto.Signer
}

func TestHolderControlTLSClientAndRegistrationSeams(t *testing.T) {
	logicListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logicListener.Close() })
	bundle := newHolderTLSBundle(t)
	serverTLS, err := routeTransport.NewServerTLSConfig(bundle.serverCert, bundle.serverKey, bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}
	rpcServer := &testHolderControlServer{}
	grpcServer := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(serverTLS)),
	)
	holdercontrolpb.RegisterHolderControlServiceServer(grpcServer, rpcServer)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		select {
		case <-serveDone:
		case <-time.After(time.Second):
			t.Error("TLS test server did not stop")
		}
	})

	client, err := DialHolderControl(context.Background(), listener.Addr().String(),
		bundle.clientCert, bundle.clientKey, bundle.caCert,
		HolderControlDialOptions{
			ServerName: "holder.test", ExpectedServerIdentity: "holder.test",
			MaxMessageBytes: 1 << 10, DialTimeout: time.Second, RPCTimeout: time.Second,
		})
	if err != nil {
		t.Fatal(err)
	}
	if client.Identity() != "holder.test" {
		t.Fatalf("client identity=%q", client.Identity())
	}
	if logicListener.Addr().String() == listener.Addr().String() {
		t.Fatal("logic and HolderControl listeners unexpectedly share a port")
	}
	if err := validateHolderPeer(HolderPeer{
		Client: client, Domain: "PLAYER", HolderID: "holder-a", BootID: "boot-a",
		LogicEndpoint: logicListener.Addr().String(), HolderControlEndpoint: listener.Addr().String(),
		TLSDNSIdentity: "holder.test",
	}); err != nil {
		t.Fatalf("cross-port Holder peer rejected: %v", err)
	}
	if _, err := client.StopNewActivationAndTurns(context.Background(),
		&holdercontrolpb.StopNewActivationAndTurnsRequest{Domain: "PLAYER", Shard: 1}); err != nil {
		t.Fatal(err)
	}
	rpcServer.mu.Lock()
	rpcServer.oversized = true
	rpcServer.mu.Unlock()
	if _, err := client.GetShardStatus(context.Background(),
		&holdercontrolpb.GetShardStatusRequest{Domain: "PLAYER", Shard: 1}); err == nil {
		t.Fatal("oversized response crossed client message limit")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	_, err = DialHolderControl(context.Background(), listener.Addr().String(),
		bundle.clientCert, bundle.clientKey, bundle.caCert,
		HolderControlDialOptions{ServerName: "wrong.test", ExpectedServerIdentity: "holder.test"})
	if !errors.Is(err, ErrHolderTLSIdentity) {
		t.Fatalf("identity mismatch err=%v", err)
	}
	_, err = DialHolderControl(context.Background(), listener.Addr().String(),
		nil, nil, nil,
		HolderControlDialOptions{ServerName: "holder.test", ExpectedServerIdentity: "holder.test"})
	if !errors.Is(err, ErrHolderTLSConfig) {
		t.Fatalf("missing PEM roots/cert err=%v", err)
	}
}

func TestHolderControlACLIsExact(t *testing.T) {
	methods := HolderControlFullMethodNames()
	if len(methods) != 7 {
		t.Fatalf("method count=%d", len(methods))
	}
	seen := make(map[string]struct{}, len(methods))
	for _, method := range methods {
		if _, ok := seen[method]; ok {
			t.Fatalf("duplicate method=%q", method)
		}
		seen[method] = struct{}{}
	}
	rules := HolderControlMethodACL([]string{"controller.test"})
	if err := ValidateHolderControlMethodACL(rules); err != nil {
		t.Fatal(err)
	}
	rules["/extra.Service/Method"] = []string{"controller.test"}
	if err := ValidateHolderControlMethodACL(rules); !errors.Is(err, ErrHolderMethodACL) {
		t.Fatalf("extra ACL err=%v", err)
	}
}

func TestHolderControlStrictCanonicalDNSAndBudgets(t *testing.T) {
	valid := "holder-1.example.test"
	if err := validateHolderServerIdentity(valid, valid); err != nil {
		t.Fatalf("valid identity rejected: %v", err)
	}
	invalid := []string{
		"Holder.example.test",
		"holder.example.test.",
		"holder_example.test",
		"holder.example.test/path",
		"*.example.test",
		"127.0.0.1",
		"holder。example",
		"-holder.example",
		"holder-.example",
		"holder..example",
		strings.Repeat("a", 64) + ".example",
		strings.Repeat("a", 254),
	}
	for _, value := range invalid {
		t.Run(value, func(t *testing.T) {
			if err := validateHolderServerIdentity(value, value); !errors.Is(err, ErrHolderTLSIdentity) {
				t.Fatalf("identity %q err=%v", value, err)
			}
		})
	}

	defaults, err := normalizeDialOptions(HolderControlDialOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if defaults.MaxMessageBytes != 1<<20 || defaults.DialTimeout != 5*time.Second || defaults.RPCTimeout != 5*time.Second {
		t.Fatalf("defaults=%+v", defaults)
	}
	for _, opts := range []HolderControlDialOptions{
		{MaxMessageBytes: (1 << 10) - 1},
		{MaxMessageBytes: (4 << 20) + 1},
		{DialTimeout: time.Microsecond},
		{RPCTimeout: 31 * time.Second},
	} {
		if _, err := normalizeDialOptions(opts); !errors.Is(err, ErrHolderBudget) {
			t.Fatalf("options=%+v err=%v", opts, err)
		}
	}
}

func TestHolderControlRequiresExactSingleDNSLeafOnRealTLS(t *testing.T) {
	bundle := newHolderTLSBundle(t)
	exactTemplate := func() *x509.Certificate {
		return &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			NotBefore:    time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
			DNSNames: []string{"holder.test"}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		}
	}
	uri, err := url.Parse("spiffe://farm.test/holder")
	if err != nil {
		t.Fatal(err)
	}
	sanDNS, err := asn1.Marshal([]asn1.RawValue{{Class: asn1.ClassContextSpecific, Tag: 2, Bytes: []byte("holder.test")}})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		build  func() ([]byte, []byte)
		wantOK bool
	}{
		{name: "exact-one-DNS", wantOK: true, build: func() ([]byte, []byte) {
			return holderServerCertificate(t, bundle.caX509, bundle.caKey, exactTemplate())
		}},
		{name: "multiple-DNS", build: func() ([]byte, []byte) {
			template := exactTemplate()
			template.DNSNames = []string{"holder.test", "other.test"}
			return holderServerCertificate(t, bundle.caX509, bundle.caKey, template)
		}},
		{name: "uppercase-noncanonical-DNS", build: func() ([]byte, []byte) {
			template := exactTemplate()
			template.DNSNames = []string{"HOLDER.TEST"}
			return holderServerCertificate(t, bundle.caX509, bundle.caKey, template)
		}},
		{name: "URI-only", build: func() ([]byte, []byte) {
			template := exactTemplate()
			template.DNSNames = nil
			template.URIs = []*url.URL{uri}
			return holderServerCertificate(t, bundle.caX509, bundle.caKey, template)
		}},
		{name: "IP-only", build: func() ([]byte, []byte) {
			template := exactTemplate()
			template.DNSNames = nil
			template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
			return holderServerCertificate(t, bundle.caX509, bundle.caKey, template)
		}},
		{name: "CN-only", build: func() ([]byte, []byte) {
			template := exactTemplate()
			template.DNSNames = nil
			template.Subject.CommonName = "holder.test"
			return holderServerCertificate(t, bundle.caX509, bundle.caKey, template)
		}},
		{name: "duplicate-SAN-extension", build: func() ([]byte, []byte) {
			template := exactTemplate()
			template.DNSNames = nil
			template.ExtraExtensions = []pkix.Extension{
				{Id: asn1.ObjectIdentifier{2, 5, 29, 17}, Value: sanDNS},
				{Id: asn1.ObjectIdentifier{2, 5, 29, 17}, Value: sanDNS},
			}
			return holderServerCertificate(t, bundle.caX509, bundle.caKey, template)
		}},
		{name: "malformed-SAN", build: func() ([]byte, []byte) {
			template := exactTemplate()
			template.DNSNames = nil
			template.ExtraExtensions = []pkix.Extension{{
				Id: asn1.ObjectIdentifier{2, 5, 29, 17}, Value: []byte{0x30, 0x01, 0xff},
			}}
			return holderServerCertificate(t, bundle.caX509, bundle.caKey, template)
		}},
		{name: "matching-intermediate-wrong-leaf", build: func() ([]byte, []byte) {
			intermediate := exactTemplate()
			intermediate.IsCA = true
			intermediate.BasicConstraintsValid = true
			intermediate.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature
			intermediate.ExtKeyUsage = nil
			intermediatePEM, _, intermediateX509, intermediateKey := holderCertificateWithParsed(t, bundle.caX509, bundle.caKey, intermediate)
			leaf := exactTemplate()
			leaf.DNSNames = []string{"other.test"}
			leafPEM, leafKey := holderServerCertificate(t, intermediateX509, intermediateKey, leaf)
			return append(append([]byte(nil), leafPEM...), intermediatePEM...), leafKey
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			serverCert, serverKey := test.build()
			err := dialHolderTLSCertificate(t, bundle, serverCert, serverKey)
			if test.wantOK && err != nil {
				t.Fatalf("exact leaf rejected: %v", err)
			}
			if !test.wantOK && err == nil {
				t.Fatal("non-exact leaf completed HolderControl dial")
			}
		})
	}
}

func TestHolderControlClosePermanentlyClosesClient(t *testing.T) {
	client := &HolderControlClient{rpcTimeout: time.Second}
	if err := client.CloseContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetShardStatus(context.Background(), &holdercontrolpb.GetShardStatusRequest{}); !errors.Is(err, ErrHolderClientClosed) {
		t.Fatalf("closed client RPC err=%v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

type testHolderControlServer struct {
	holdercontrolpb.UnimplementedHolderControlServiceServer
	mu        sync.Mutex
	oversized bool
}

func (s *testHolderControlServer) StopNewActivationAndTurns(_ context.Context, req *holdercontrolpb.StopNewActivationAndTurnsRequest) (*holdercontrolpb.HolderControlResponse, error) {
	return &holdercontrolpb.HolderControlResponse{Status: &holdercontrolpb.ShardStatus{
		Domain: req.GetDomain(), Shard: req.GetShard(),
		Phase: holdercontrolpb.DrainPhase_DRAIN_PHASE_STOPPED,
	}}, nil
}

func (s *testHolderControlServer) WaitCurrentTurns(_ context.Context, req *holdercontrolpb.WaitCurrentTurnsRequest) (*holdercontrolpb.HolderControlResponse, error) {
	return &holdercontrolpb.HolderControlResponse{Status: &holdercontrolpb.ShardStatus{
		Domain: req.GetDomain(), Shard: req.GetShard(),
		Phase: holdercontrolpb.DrainPhase_DRAIN_PHASE_WAITED, WaitComplete: true,
	}}, nil
}

func (s *testHolderControlServer) ConfirmFactsCommitted(_ context.Context, req *holdercontrolpb.ConfirmFactsCommittedRequest) (*holdercontrolpb.HolderControlResponse, error) {
	return &holdercontrolpb.HolderControlResponse{Status: &holdercontrolpb.ShardStatus{
		Domain: req.GetDomain(), Shard: req.GetShard(),
		Phase: holdercontrolpb.DrainPhase_DRAIN_PHASE_FACTS_COMMITTED, WaitComplete: true,
	}}, nil
}

func (s *testHolderControlServer) PrepareShardGeneration(_ context.Context, req *holdercontrolpb.PrepareShardGenerationRequest) (*holdercontrolpb.HolderControlResponse, error) {
	return &holdercontrolpb.HolderControlResponse{Status: &holdercontrolpb.ShardStatus{
		Domain: req.GetDomain(), Shard: req.GetShard(), Prepared: true,
		Phase:              holdercontrolpb.DrainPhase_DRAIN_PHASE_OPEN,
		PreparedGeneration: req.GetGeneration(),
	}}, nil
}

func (s *testHolderControlServer) GetShardStatus(_ context.Context, req *holdercontrolpb.GetShardStatusRequest) (*holdercontrolpb.GetShardStatusResponse, error) {
	s.mu.Lock()
	oversized := s.oversized
	s.mu.Unlock()
	domain := req.GetDomain()
	if oversized {
		domain = strings.Repeat("x", 2048)
	}
	return &holdercontrolpb.GetShardStatusResponse{Status: &holdercontrolpb.ShardStatus{
		Domain: domain, Shard: req.GetShard(),
		Phase: holdercontrolpb.DrainPhase_DRAIN_PHASE_OPEN,
	}}, nil
}

func newHolderTLSBundle(t *testing.T) holderTLSBundle {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "holder-test-ca"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert := pemEncode("CERTIFICATE", caDER)
	makeLeaf := func(serial int64, dns string, usages []x509.ExtKeyUsage) ([]byte, []byte) {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: dns},
			NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
			DNSNames: []string{dns}, ExtKeyUsage: usages,
			KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		}
		der, err := x509.CreateCertificate(rand.Reader, template, caTemplate, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatal(err)
		}
		return pemEncode("CERTIFICATE", der), pemEncode("PRIVATE KEY", keyDER)
	}
	serverCert, serverKey := makeLeaf(2, "holder.test", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	clientCert, clientKey := makeLeaf(3, "controller.test", []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	parsedCA, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	return holderTLSBundle{caCert: caCert, serverCert: serverCert, serverKey: serverKey, clientCert: clientCert, clientKey: clientKey, caX509: parsedCA, caKey: caKey}
}

func holderServerCertificate(t *testing.T, parent *x509.Certificate, parentKey crypto.Signer, template *x509.Certificate) ([]byte, []byte) {
	t.Helper()
	certificate, key, _, _ := holderCertificateWithParsed(t, parent, parentKey, template)
	return certificate, key
}

func holderCertificateWithParsed(t *testing.T, parent *x509.Certificate, parentKey crypto.Signer, template *x509.Certificate) ([]byte, []byte, *x509.Certificate, crypto.Signer) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, key.Public(), parentKey)
	if err != nil {
		t.Fatal(err)
	}
	parsed, parseErr := x509.ParseCertificate(der)
	if parseErr != nil {
		// Keep malformed/duplicate extension DER available to the real TLS
		// loader; the returned parsed value is never used for those variants.
		parsed = template
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pemEncode("CERTIFICATE", der), pemEncode("PRIVATE KEY", keyDER), parsed, key
}

func dialHolderTLSCertificate(t *testing.T, bundle holderTLSBundle, serverCert, serverKey []byte) error {
	t.Helper()
	serverTLS, err := routeTransport.NewServerTLSConfig(serverCert, serverKey, bundle.caCert)
	if err != nil {
		return err
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)))
	holdercontrolpb.RegisterHolderControlServiceServer(server, &testHolderControlServer{})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() {
		server.Stop()
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("HolderControl TLS variant server did not stop")
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err := DialHolderControl(ctx, listener.Addr().String(), bundle.clientCert, bundle.clientKey, bundle.caCert,
		HolderControlDialOptions{ServerName: "holder.test", ExpectedServerIdentity: "holder.test", DialTimeout: 50 * time.Millisecond})
	if err != nil {
		return err
	}
	return client.Close()
}

func pemEncode(kind string, data []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: data})
}
