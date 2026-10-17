package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

const holderOnlyUnaryFullMethod = "/holderonly.Holder/Mutate"

type holderOnlyService interface {
	holderOnlyServiceMarker()
}

type holderOnlyServer struct {
	calls atomic.Int32
}

func (*holderOnlyServer) holderOnlyServiceMarker() {}

func holderOnlyUnaryDescriptor(serviceName string) *grpc.ServiceDesc {
	return &grpc.ServiceDesc{
		ServiceName: serviceName,
		HandlerType: (*holderOnlyService)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Mutate",
			Handler:    holderOnlyUnaryHandler,
		}},
		Metadata: "holder/holder.proto",
	}
}

func holderOnlyUnaryHandler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	request := new(emptypb.Empty)
	if err := dec(request); err != nil {
		return nil, err
	}
	srv.(*holderOnlyServer).calls.Add(1)
	return &emptypb.Empty{}, nil
}

func holderOnlyTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{{}},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    x509.NewCertPool(),
	}
}

func holderOnlyAuthorizer(t *testing.T, methods ...string) *MethodAuthorizer {
	t.Helper()
	rules := make(map[string][]string, len(methods))
	for _, method := range methods {
		rules[method] = []string{"controller.test"}
	}
	authorizer, err := NewMethodAuthorizer(rules)
	if err != nil {
		t.Fatal(err)
	}
	return authorizer
}

func holderOnlyRegistration(desc *grpc.ServiceDesc, impl any) GRPCServiceRegistration {
	return func(registrar grpc.ServiceRegistrar) {
		registrar.RegisterService(desc, impl)
	}
}

func TestNewHolderOnlySealedGRPCServerConstructsAndCopiesDescriptor(t *testing.T) {
	desc := holderOnlyUnaryDescriptor("holderonly.Holder")
	sealed, err := NewHolderOnlySealedGRPCServer(holderOnlyTLSConfig(), GRPCServerOptions{
		MaxMessageBytes:  1 << 20,
		MethodAuthorizer: holderOnlyAuthorizer(t, holderOnlyUnaryFullMethod),
	}, holderOnlyRegistration(desc, &holderOnlyServer{}))
	if err != nil {
		t.Fatal(err)
	}
	defer sealed.Stop()

	if _, ok := any(sealed).(grpc.ServiceRegistrar); ok {
		t.Fatal("holder-only sealed server unexpectedly implements grpc.ServiceRegistrar")
	}

	desc.ServiceName = "mutated.Service"
	desc.Methods[0].MethodName = "Mutated"
	desc.Methods[0].Handler = nil
	desc.Metadata = "mutated.proto"

	info, ok := sealed.GetServiceInfo()["holderonly.Holder"]
	if !ok {
		t.Fatal("sealed server lost copied service descriptor")
	}
	if info.Metadata != "holder/holder.proto" {
		t.Fatalf("service metadata=%v want original metadata", info.Metadata)
	}
	if len(info.Methods) != 1 || info.Methods[0].Name != "Mutate" {
		t.Fatalf("service methods=%v want Mutate", info.Methods)
	}
}

func TestNewHolderOnlySealedGRPCServerValidatesTLSAuthorizerAndRegistrations(t *testing.T) {
	desc := holderOnlyUnaryDescriptor("holderonly.Inputs")
	registration := holderOnlyRegistration(desc, &holderOnlyServer{})
	validAuthorizer := holderOnlyAuthorizer(t, "/holderonly.Inputs/Mutate")
	validTLS := holderOnlyTLSConfig()

	tests := []struct {
		name          string
		tlsConfig     *tls.Config
		authorizer    *MethodAuthorizer
		registrations []GRPCServiceRegistration
		want          error
	}{
		{
			name:          "nil TLS",
			tlsConfig:     nil,
			authorizer:    validAuthorizer,
			registrations: []GRPCServiceRegistration{registration},
			want:          ErrTLSRequired,
		},
		{
			name: "TLS 1.2",
			tlsConfig: func() *tls.Config {
				config := holderOnlyTLSConfig()
				config.MinVersion = tls.VersionTLS12
				return config
			}(),
			authorizer:    validAuthorizer,
			registrations: []GRPCServiceRegistration{registration},
			want:          ErrTLSConfig,
		},
		{
			name: "non verifying client auth",
			tlsConfig: func() *tls.Config {
				config := holderOnlyTLSConfig()
				config.ClientAuth = tls.RequireAnyClientCert
				return config
			}(),
			authorizer:    validAuthorizer,
			registrations: []GRPCServiceRegistration{registration},
			want:          ErrTLSConfig,
		},
		{
			name:          "nil authorizer",
			tlsConfig:     validTLS,
			registrations: []GRPCServiceRegistration{registration},
			want:          ErrServerConfiguration,
		},
		{
			name:       "empty registrations",
			tlsConfig:  validTLS,
			authorizer: validAuthorizer,
			want:       ErrServerConfiguration,
		},
		{
			name:          "nil registration",
			tlsConfig:     validTLS,
			authorizer:    validAuthorizer,
			registrations: []GRPCServiceRegistration{nil},
			want:          ErrServerConfiguration,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sealed, err := NewHolderOnlySealedGRPCServer(test.tlsConfig, GRPCServerOptions{
				MethodAuthorizer: test.authorizer,
			}, test.registrations...)
			if sealed != nil {
				sealed.Stop()
				t.Fatal("invalid construction returned a sealed server")
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("construction err=%v want errors.Is(%v)", err, test.want)
			}
		})
	}
}

func TestNewHolderOnlySealedGRPCServerRequiresExactRegisteredACL(t *testing.T) {
	desc := holderOnlyUnaryDescriptor("holderonly.ACL")
	registration := holderOnlyRegistration(desc, &holderOnlyServer{})
	tests := []struct {
		name          string
		rules         map[string][]string
		wantSubstring string
	}{
		{
			name: "missing method ACL",
			rules: map[string][]string{
				"/holderonly.ACL/Other": {"controller.test"},
			},
			wantSubstring: "missing a non-empty ACL",
		},
		{
			name: "extra method ACL",
			rules: map[string][]string{
				holderOnlyUnaryFullMethodFor("holderonly.ACL"): {"controller.test"},
				"/holderonly.ACL/Other":                        {"controller.test"},
			},
			wantSubstring: "unregistered method",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authorizer, err := NewMethodAuthorizer(test.rules)
			if err != nil {
				t.Fatal(err)
			}
			sealed, err := NewHolderOnlySealedGRPCServer(holderOnlyTLSConfig(), GRPCServerOptions{
				MethodAuthorizer: authorizer,
			}, registration)
			if sealed != nil {
				sealed.Stop()
				t.Fatal("ACL mismatch returned a sealed server")
			}
			if !errors.Is(err, ErrServerConfiguration) || !strings.Contains(err.Error(), test.wantSubstring) {
				t.Fatalf("construction err=%v want configuration containing %q", err, test.wantSubstring)
			}
		})
	}
}

func holderOnlyUnaryFullMethodFor(serviceName string) string {
	return "/" + serviceName + "/Mutate"
}

func TestNewHolderOnlySealedGRPCServerRegistrationHardening(t *testing.T) {
	desc := holderOnlyUnaryDescriptor("holderonly.Hardening")
	validAuthorizer := holderOnlyAuthorizer(t, holderOnlyUnaryFullMethodFor("holderonly.Hardening"))
	var typedNil *holderOnlyServer
	tests := []struct {
		name string
		call GRPCServiceRegistration
	}{
		{
			name: "no-op",
			call: func(grpc.ServiceRegistrar) {},
		},
		{
			name: "panic",
			call: func(grpc.ServiceRegistrar) { panic("registration boom") },
		},
		{
			name: "panic nil",
			call: func(grpc.ServiceRegistrar) { panic(nil) },
		},
		{
			name: "typed nil implementation",
			call: func(registrar grpc.ServiceRegistrar) {
				registrar.RegisterService(desc, typedNil)
			},
		},
		{
			name: "duplicate service",
			call: func(registrar grpc.ServiceRegistrar) {
				registrar.RegisterService(desc, &holderOnlyServer{})
				registrar.RegisterService(desc, &holderOnlyServer{})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sealed, err := NewHolderOnlySealedGRPCServer(holderOnlyTLSConfig(), GRPCServerOptions{
				MethodAuthorizer: validAuthorizer,
			}, test.call)
			if sealed != nil {
				sealed.Stop()
				t.Fatal("invalid registration returned a sealed server")
			}
			if !errors.Is(err, ErrServerConfiguration) {
				t.Fatalf("construction err=%v want server configuration", err)
			}
		})
	}
}

func TestNewHolderOnlySealedGRPCServerRejectsRetainedRegistrarAfterSeal(t *testing.T) {
	const serviceName = "holderonly.Retained"
	desc := holderOnlyUnaryDescriptor(serviceName)
	authorizer := holderOnlyAuthorizer(t, holderOnlyUnaryFullMethodFor(serviceName))
	var retained grpc.ServiceRegistrar
	sealed, err := NewHolderOnlySealedGRPCServer(holderOnlyTLSConfig(), GRPCServerOptions{
		MethodAuthorizer: authorizer,
	}, func(registrar grpc.ServiceRegistrar) {
		retained = registrar
		registrar.RegisterService(desc, &holderOnlyServer{})
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sealed.Stop()
	if retained == nil {
		t.Fatal("registration callback did not receive registrar")
	}

	before := sealed.GetServiceInfo()
	var wait sync.WaitGroup
	for index := 0; index < 64; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			retained.RegisterService(holderOnlyUnaryDescriptor("holderonly.Late"), &holderOnlyServer{})
		}()
	}
	wait.Wait()
	after := sealed.GetServiceInfo()
	if !sameGRPCServiceInfo(before, after) {
		t.Fatalf("late registrar changed sealed service info: before=%v after=%v", before, after)
	}
	if _, ok := after["holderonly.Late"]; ok {
		t.Fatal("late registrar added a service after seal")
	}
}

func TestNewHolderOnlySealedGRPCServerAuthorizesWrappedHandlersAndUnknownMethodsFailClosed(t *testing.T) {
	bundle := newCertificateBundle(t)
	serverTLS, err := NewServerTLSConfig(bundle.serverCert, bundle.serverKey, bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}
	holder := &holderOnlyServer{}
	sealed, err := NewHolderOnlySealedGRPCServer(serverTLS, GRPCServerOptions{
		MethodAuthorizer: holderOnlyAuthorizer(t, holderOnlyUnaryFullMethod),
	}, holderOnlyRegistration(holderOnlyUnaryDescriptor("holderonly.Holder"), holder))
	if err != nil {
		t.Fatal(err)
	}
	defer sealed.Stop()
	listener := bufconn.Listen(1 << 20)
	defer listener.Close()
	go func() { _ = sealed.Serve(listener) }()

	dial := func(certPEM, keyPEM []byte) *grpc.ClientConn {
		t.Helper()
		clientTLS, err := NewClientTLSConfig(certPEM, keyPEM, bundle.caCert)
		if err != nil {
			t.Fatal(err)
		}
		clientTLS.ServerName = "server.test"
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		conn, err := grpc.DialContext(ctx, "bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)),
			grpc.WithBlock(),
		)
		if err != nil {
			t.Fatal(err)
		}
		return conn
	}

	call := func(ctx context.Context, conn *grpc.ClientConn, method string) error {
		return conn.Invoke(ctx, method, &emptypb.Empty{}, &emptypb.Empty{})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	unauthorized := dial(bundle.clientCert, bundle.clientKey)
	defer unauthorized.Close()
	if err := call(ctx, unauthorized, holderOnlyUnaryFullMethod); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthorized holder call status=%s err=%v", status.Code(err), err)
	}
	if got := holder.calls.Load(); got != 0 {
		t.Fatalf("unauthorized holder call reached handler: calls=%d", got)
	}
	if err := call(ctx, unauthorized, "/holderonly.Holder/Unknown"); status.Code(err) != codes.Unimplemented {
		t.Fatalf("unknown holder call status=%s err=%v", status.Code(err), err)
	}

	controllerCert, controllerKey := signedClientCertificate(t, &transportHarness{bundle: bundle}, "controller.test")
	authorized := dial(controllerCert, controllerKey)
	defer authorized.Close()
	if err := call(ctx, authorized, holderOnlyUnaryFullMethod); err != nil {
		t.Fatalf("authorized holder call err=%v", err)
	}
	if got := holder.calls.Load(); got != 1 {
		t.Fatalf("authorized holder calls=%d want 1", got)
	}
}
