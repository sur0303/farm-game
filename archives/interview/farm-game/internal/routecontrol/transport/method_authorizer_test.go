package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	routecontrolpb "farmgame/api/gen/routecontrol"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	holderMutateFullMethod   = "/holder.Holder/Mutate"
	holderUnlistedFullMethod = "/holder.Holder/Unlisted"
)

func TestVerifiedPeerIdentityRequiresVerifiedTLS13AndCanonicalSAN(t *testing.T) {
	uri, err := url.Parse("spiffe://farm.test/controller")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name      string
		ctx       context.Context
		want      string
		wantErrIs error
	}{
		{
			name: "dns is canonicalized",
			ctx:  peerTLSContext(&x509.Certificate{DNSNames: []string{"CONTROLLER.TEST"}}, tls.VersionTLS13, true, true),
			want: "controller.test",
		},
		{
			name: "uri is preserved",
			ctx:  peerTLSContext(&x509.Certificate{URIs: []*url.URL{uri}}, tls.VersionTLS13, true, true),
			want: uri.String(),
		},
		{
			name:      "missing peer",
			ctx:       context.Background(),
			wantErrIs: ErrPeerUnauthenticated,
		},
		{
			name:      "unverified chain",
			ctx:       peerTLSContext(&x509.Certificate{DNSNames: []string{"controller.test"}}, tls.VersionTLS13, true, false),
			wantErrIs: ErrPeerUnauthenticated,
		},
		{
			name:      "tls 1.2",
			ctx:       peerTLSContext(&x509.Certificate{DNSNames: []string{"controller.test"}}, tls.VersionTLS12, true, true),
			wantErrIs: ErrPeerUnauthenticated,
		},
		{
			name: "ambiguous san",
			ctx: peerTLSContext(&x509.Certificate{
				URIs:     []*url.URL{uri},
				DNSNames: []string{"controller.test"},
			}, tls.VersionTLS13, true, true),
			wantErrIs: ErrPeerUnauthenticated,
		},
		{
			name:      "incomplete handshake",
			ctx:       peerTLSContext(&x509.Certificate{DNSNames: []string{"controller.test"}}, tls.VersionTLS13, false, true),
			wantErrIs: ErrPeerUnauthenticated,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := VerifiedPeerIdentity(test.ctx)
			if test.wantErrIs != nil {
				if !errors.Is(err, test.wantErrIs) {
					t.Fatalf("identity=%q err=%v want errors.Is(%v)", got, err, test.wantErrIs)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("identity=%q err=%v want=%q", got, err, test.want)
			}
		})
	}
}

func TestMethodAuthorizerConstructionIsStrictAndCopiesInputs(t *testing.T) {
	invalid := []struct {
		name  string
		rules map[string][]string
	}{
		{name: "nil rules"},
		{name: "empty rules", rules: map[string][]string{}},
		{name: "empty identities", rules: map[string][]string{holderMutateFullMethod: nil}},
		{name: "empty identity", rules: map[string][]string{holderMutateFullMethod: {""}}},
		{name: "whitespace identity", rules: map[string][]string{holderMutateFullMethod: {" controller.test"}}},
		{name: "noncanonical identity", rules: map[string][]string{holderMutateFullMethod: {"CONTROLLER.TEST"}}},
		{name: "duplicate identity", rules: map[string][]string{holderMutateFullMethod: {"controller.test", "controller.test"}}},
		{name: "wildcard method", rules: map[string][]string{"/holder.Holder/*": {"controller.test"}}},
		{name: "method prefix", rules: map[string][]string{"/holder.Holder": {"controller.test"}}},
		{name: "missing leading slash", rules: map[string][]string{"holder.Holder/Mutate": {"controller.test"}}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewMethodAuthorizer(test.rules); !errors.Is(err, ErrServerConfiguration) {
				t.Fatalf("authorizer err=%v want server configuration", err)
			}
		})
	}

	rules := map[string][]string{
		holderMutateFullMethod: {"controller.test", "spiffe://farm.test/controller"},
	}
	authorizer, err := NewMethodAuthorizer(rules)
	if err != nil {
		t.Fatal(err)
	}
	rules[holderMutateFullMethod][0] = "attacker.test"
	rules[holderMutateFullMethod] = []string{"attacker.test"}
	rules[holderUnlistedFullMethod] = []string{"attacker.test"}

	if err := authorizer.Authorize(peerTLSContext(&x509.Certificate{DNSNames: []string{"CONTROLLER.TEST"}}, tls.VersionTLS13, true, true), holderMutateFullMethod); err != nil {
		t.Fatalf("mutating caller-owned inputs changed ACL: %v", err)
	}
	if err := authorizer.Authorize(peerTLSContext(&x509.Certificate{DNSNames: []string{"attacker.test"}}, tls.VersionTLS13, true, true), holderMutateFullMethod); err == nil {
		t.Fatal("mutated caller-owned identity unexpectedly authorized")
	}
	if err := authorizer.Authorize(peerTLSContext(&x509.Certificate{DNSNames: []string{"controller.test"}}, tls.VersionTLS13, true, true), holderUnlistedFullMethod); err == nil {
		t.Fatal("mutated caller-owned method unexpectedly authorized")
	}
	if err := authorizer.Authorize(peerTLSContext(&x509.Certificate{URIs: []*url.URL{mustParseURL(t, "spiffe://farm.test/controller")}}, tls.VersionTLS13, true, true), holderMutateFullMethod); err != nil {
		t.Fatalf("canonical URI identity rejected: %v", err)
	}
}

func TestMethodAuthorizerInterceptorsFailClosedAndAuthorizeExactStream(t *testing.T) {
	authorizer, err := NewMethodAuthorizer(map[string][]string{
		holderMutateFullMethod: {"controller.test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	valid := peerTLSContext(&x509.Certificate{DNSNames: []string{"controller.test"}}, tls.VersionTLS13, true, true)

	if _, err := authorizer.UnaryServerInterceptor(valid, &emptypb.Empty{}, nil, func(context.Context, any) (any, error) {
		t.Fatal("nil info reached unary handler")
		return nil, nil
	}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("nil unary info status=%s want unauthenticated", status.Code(err))
	}
	if _, err := authorizer.UnaryServerInterceptor(valid, &emptypb.Empty{}, &grpc.UnaryServerInfo{FullMethod: holderMutateFullMethod}, nil); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("nil unary handler status=%s want unauthenticated", status.Code(err))
	}
	if _, err := authorizer.UnaryServerInterceptor(nil, &emptypb.Empty{}, &grpc.UnaryServerInfo{FullMethod: holderMutateFullMethod}, func(context.Context, any) (any, error) {
		t.Fatal("nil context reached unary handler")
		return nil, nil
	}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("nil unary context status=%s want unauthenticated", status.Code(err))
	}

	if err := authorizer.StreamServerInterceptor(nil, nil, &grpc.StreamServerInfo{FullMethod: holderMutateFullMethod}, func(any, grpc.ServerStream) error {
		t.Fatal("nil stream reached handler")
		return nil
	}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("nil stream status=%s want unauthenticated", status.Code(err))
	}
	if err := authorizer.StreamServerInterceptor(nil, &authorizerTestStream{ctx: valid}, nil, func(any, grpc.ServerStream) error {
		t.Fatal("nil stream info reached handler")
		return nil
	}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("nil stream info status=%s want unauthenticated", status.Code(err))
	}
	if err := authorizer.StreamServerInterceptor(nil, &authorizerTestStream{ctx: valid}, &grpc.StreamServerInfo{FullMethod: holderMutateFullMethod}, nil); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("nil stream handler status=%s want unauthenticated", status.Code(err))
	}
	if err := authorizer.StreamServerInterceptor(nil, &authorizerTestStream{}, &grpc.StreamServerInfo{FullMethod: holderMutateFullMethod}, func(any, grpc.ServerStream) error {
		t.Fatal("nil stream context reached handler")
		return nil
	}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("nil stream context status=%s want unauthenticated", status.Code(err))
	}

	called := false
	if err := authorizer.StreamServerInterceptor(nil, &authorizerTestStream{ctx: valid}, &grpc.StreamServerInfo{FullMethod: holderMutateFullMethod}, func(any, grpc.ServerStream) error {
		called = true
		return nil
	}); err != nil || !called {
		t.Fatalf("authorized stream err=%v called=%v", err, called)
	}
}

func TestNewGRPCServerMethodAuthorizerRequiresAllSnapshotMethods(t *testing.T) {
	harness := newTransportHarness(t, "controller-boot-a", 1<<20)
	defer harness.close()
	serverTLS, err := NewServerTLSConfig(harness.bundle.serverCert, harness.bundle.serverKey, harness.bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}
	for _, missing := range routeSnapshotFullMethods {
		t.Run(missing, func(t *testing.T) {
			rules := map[string][]string{
				holderMutateFullMethod: {"controller.test"},
			}
			for _, method := range routeSnapshotFullMethods {
				if method != missing {
					rules[method] = []string{"client.test"}
				}
			}
			authorizer, err := NewMethodAuthorizer(rules)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := NewGRPCServer(harness.service, serverTLS, GRPCServerOptions{MethodAuthorizer: authorizer}); !errors.Is(err, ErrServerConfiguration) {
				t.Fatalf("missing=%q err=%v want server configuration", missing, err)
			}
		})
	}
}

func TestMultiServiceGRPCServerRequiresExactSnapshotACLAndConstructionRegistrations(t *testing.T) {
	harness := newTransportHarness(t, "controller-boot-a", 1<<20)
	defer harness.close()
	serverTLS, err := NewServerTLSConfig(harness.bundle.serverCert, harness.bundle.serverKey, harness.bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}
	validAuthorizer := func(t *testing.T) *MethodAuthorizer {
		t.Helper()
		authorizer, err := NewMethodAuthorizer(map[string][]string{
			routecontrolpb.RouteSnapshotService_GetSnapshot_FullMethodName:        {"client.test"},
			routecontrolpb.RouteSnapshotService_GetCurrentRevision_FullMethodName: {"client.test"},
			routecontrolpb.RouteSnapshotService_WatchFrom_FullMethodName:          {"client.test"},
			holderMutateFullMethod:   {"controller.test"},
			holderUnlistedFullMethod: {"other.test"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return authorizer
	}
	validRegistration := func(registrar grpc.ServiceRegistrar) {
		registrar.RegisterService(&simulatedHolderServiceDesc, &simulatedHolderServer{})
	}

	tests := []struct {
		name          string
		authorizer    *MethodAuthorizer
		registrations []GRPCServiceRegistration
	}{
		{
			name:          "nil authorizer",
			registrations: []GRPCServiceRegistration{validRegistration},
		},
		{
			name:       "no extra",
			authorizer: validAuthorizer(t),
		},
		{
			name:          "no-op callback",
			authorizer:    validAuthorizer(t),
			registrations: []GRPCServiceRegistration{func(grpc.ServiceRegistrar) {}},
		},
		{
			name:          "panic callback",
			authorizer:    validAuthorizer(t),
			registrations: []GRPCServiceRegistration{func(grpc.ServiceRegistrar) { panic("registration boom") }},
		},
		{
			name:          "panic nil callback",
			authorizer:    validAuthorizer(t),
			registrations: []GRPCServiceRegistration{func(grpc.ServiceRegistrar) { panic(nil) }},
		},
		{
			name:       "duplicate callback",
			authorizer: validAuthorizer(t),
			registrations: []GRPCServiceRegistration{func(registrar grpc.ServiceRegistrar) {
				holder := &simulatedHolderServer{}
				registrar.RegisterService(&simulatedHolderServiceDesc, holder)
				registrar.RegisterService(&simulatedHolderServiceDesc, holder)
			}},
		},
		{
			name:       "nil callback",
			authorizer: validAuthorizer(t),
			registrations: []GRPCServiceRegistration{
				nil,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sealed, err := NewMultiServiceGRPCServer(harness.service, serverTLS, GRPCServerOptions{
				MaxMessageBytes:  1 << 20,
				MethodAuthorizer: test.authorizer,
			}, test.registrations...)
			if sealed != nil {
				t.Fatal("failed construction returned a runnable sealed server")
			}
			if !errors.Is(err, ErrServerConfiguration) {
				t.Fatalf("construction err=%v want server configuration", err)
			}
		})
	}

	mismatched := []struct {
		name  string
		rules map[string][]string
	}{
		{
			name: "extra snapshot identity",
			rules: map[string][]string{
				routecontrolpb.RouteSnapshotService_GetSnapshot_FullMethodName:        {"client.test", "extra.test"},
				routecontrolpb.RouteSnapshotService_GetCurrentRevision_FullMethodName: {"client.test"},
				routecontrolpb.RouteSnapshotService_WatchFrom_FullMethodName:          {"client.test"},
				holderMutateFullMethod: {"controller.test"},
			},
		},
		{
			name: "missing snapshot identity",
			rules: map[string][]string{
				routecontrolpb.RouteSnapshotService_GetSnapshot_FullMethodName:        {"client.test"},
				routecontrolpb.RouteSnapshotService_GetCurrentRevision_FullMethodName: {"client.test"},
				routecontrolpb.RouteSnapshotService_WatchFrom_FullMethodName:          {"other.test"},
				holderMutateFullMethod: {"controller.test"},
			},
		},
	}
	for _, test := range mismatched {
		t.Run(test.name, func(t *testing.T) {
			authorizer, err := NewMethodAuthorizer(test.rules)
			if err != nil {
				t.Fatal(err)
			}
			sealed, err := NewMultiServiceGRPCServer(harness.service, serverTLS, GRPCServerOptions{
				MaxMessageBytes:  1 << 20,
				MethodAuthorizer: authorizer,
			}, validRegistration)
			if sealed != nil {
				t.Fatal("mismatched ACL returned a runnable sealed server")
			}
			if !errors.Is(err, ErrServerConfiguration) {
				t.Fatalf("mismatched ACL err=%v want server configuration", err)
			}
		})
	}
}

func TestGRPCServerRejectsMultipleOptionsValues(t *testing.T) {
	harness := newTransportHarness(t, "controller-boot-a", 1<<20)
	defer harness.close()
	serverTLS, err := NewServerTLSConfig(harness.bundle.serverCert, harness.bundle.serverKey, harness.bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}
	options := []GRPCServerOptions{{}, {}}
	if _, err := NewGRPCServer(harness.service, serverTLS, options...); !errors.Is(err, ErrServerConfiguration) {
		t.Fatalf("NewGRPCServer options err=%v want server configuration", err)
	}
	if _, err := Serve(nil, harness.service, serverTLS, options...); !errors.Is(err, ErrServerConfiguration) {
		t.Fatalf("Serve options err=%v want server configuration", err)
	}
}

func TestMethodAuthorizerProtectsRegisteredHolderMethodsByVerifiedIdentity(t *testing.T) {
	authorizer, err := NewMethodAuthorizer(map[string][]string{
		routecontrolpb.RouteSnapshotService_GetSnapshot_FullMethodName:        {"client.test"},
		routecontrolpb.RouteSnapshotService_GetCurrentRevision_FullMethodName: {"client.test"},
		routecontrolpb.RouteSnapshotService_WatchFrom_FullMethodName:          {"client.test"},
		holderMutateFullMethod:   {"controller.test"},
		holderUnlistedFullMethod: {"other.test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	holder := &simulatedHolderServer{}
	harness := newTransportHarnessWithAuthorizer(t, "controller-boot-a", 1<<20, authorizer, func(registrar grpc.ServiceRegistrar) {
		registrar.RegisterService(&simulatedHolderServiceDesc, holder)
	})
	defer harness.close()
	if _, ok := any(harness.sealed).(grpc.ServiceRegistrar); ok {
		t.Fatal("sealed multi-service server unexpectedly implements grpc.ServiceRegistrar")
	}
	serviceInfo := harness.sealed.GetServiceInfo()
	if _, ok := serviceInfo[routecontrolpb.RouteSnapshotService_ServiceDesc.ServiceName]; !ok {
		t.Fatal("sealed server omitted RouteSnapshot service info")
	}
	if _, ok := serviceInfo[simulatedHolderServiceDesc.ServiceName]; !ok {
		t.Fatal("sealed server omitted extra service info")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	call := func(ctx context.Context, conn *grpc.ClientConn, method string) error {
		return conn.Invoke(ctx, method, &emptypb.Empty{}, &emptypb.Empty{})
	}
	metadataCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs("caller_role", "controller"))
	if err := call(metadataCtx, harness.conn, holderMutateFullMethod); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("snapshot client holder call status=%s err=%v want unauthenticated", status.Code(err), err)
	}
	if got := holder.mutateCalls.Load(); got != 0 {
		t.Fatalf("snapshot client reached holder mutate: calls=%d", got)
	}

	controllerCert, controllerKey := signedClientCertificate(t, harness, "controller.test")
	controllerConn := dialTransportConnection(t, harness, controllerCert, controllerKey)
	defer controllerConn.Close()
	if err := call(ctx, controllerConn, holderMutateFullMethod); err != nil {
		t.Fatalf("controller holder call err=%v", err)
	}
	if got := holder.mutateCalls.Load(); got != 1 {
		t.Fatalf("controller holder calls=%d want 1", got)
	}
	if err := call(ctx, controllerConn, holderUnlistedFullMethod); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("controller unlisted holder call status=%s err=%v want unauthenticated", status.Code(err), err)
	}
	if got := holder.unlistedCalls.Load(); got != 0 {
		t.Fatalf("controller reached unlisted holder method: calls=%d", got)
	}
	if _, err := harness.client.FetchFullSnapshot(ctx); err != nil {
		t.Fatalf("snapshot client lost exact snapshot authorization: %v", err)
	}
}

func peerTLSContext(certificate *x509.Certificate, version uint16, handshakeComplete, verified bool) context.Context {
	certificate = testCertificateWithSAN(certificate)
	var chains [][]*x509.Certificate
	if verified {
		chains = [][]*x509.Certificate{{certificate}}
	}
	return peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{
			State: tls.ConnectionState{
				Version:           version,
				HandshakeComplete: handshakeComplete,
				VerifiedChains:    chains,
			},
		},
	})
}

func mustParseURL(t *testing.T, value string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

type authorizerTestStream struct {
	ctx context.Context
}

func (s *authorizerTestStream) Context() context.Context     { return s.ctx }
func (s *authorizerTestStream) SendHeader(metadata.MD) error { return nil }
func (s *authorizerTestStream) SetHeader(metadata.MD) error  { return nil }
func (s *authorizerTestStream) SetTrailer(metadata.MD)       {}
func (s *authorizerTestStream) SendMsg(any) error            { return nil }
func (s *authorizerTestStream) RecvMsg(any) error            { return nil }

type simulatedHolderService interface{}

type simulatedHolderServer struct {
	mutateCalls   atomic.Int32
	unlistedCalls atomic.Int32
}

var simulatedHolderServiceDesc = grpc.ServiceDesc{
	ServiceName: "holder.Holder",
	HandlerType: (*simulatedHolderService)(nil),
	Methods: []grpc.MethodDesc{
		{MethodName: "Mutate", Handler: simulatedHolderMutateHandler},
		{MethodName: "Unlisted", Handler: simulatedHolderUnlistedHandler},
	},
}

func simulatedHolderMutateHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(*simulatedHolderServer).mutate(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: holderMutateFullMethod}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(*simulatedHolderServer).mutate(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func simulatedHolderUnlistedHandler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(*simulatedHolderServer).unlisted(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: holderUnlistedFullMethod}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(*simulatedHolderServer).unlisted(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func (s *simulatedHolderServer) mutate(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	s.mutateCalls.Add(1)
	return &emptypb.Empty{}, nil
}

func (s *simulatedHolderServer) unlisted(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
	s.unlistedCalls.Add(1)
	return &emptypb.Empty{}, nil
}

func signedClientCertificate(t *testing.T, harness *transportHarness, name string) ([]byte, []byte) {
	t.Helper()
	block, _ := pem.Decode(harness.bundle.caCert)
	if block == nil {
		t.Fatal("decode test CA")
	}
	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	key, _, certPEM := makeCertificate(t, caCert, harness.bundle.caKey, name, false, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	return certPEM, pemPrivateKey(key)
}

func dialTransportConnection(t *testing.T, harness *transportHarness, certPEM, keyPEM []byte) *grpc.ClientConn {
	t.Helper()
	clientTLS, err := NewClientTLSConfig(certPEM, keyPEM, harness.bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}
	clientTLS.ServerName = "server.test"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(harness.dialer),
		grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)),
		grpc.WithBlock(),
	)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}
