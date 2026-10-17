package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	routecontrolpb "farmgame/api/gen/routecontrol"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	hardeningUnaryFullMethod  = "/hardening.Hardening/Unary"
	hardeningStreamFullMethod = "/hardening.Hardening/Stream"
)

func TestMultiServiceCollectorCopiesDescriptorAndAcceptsImmutableMetadata(t *testing.T) {
	authorizer := newHardeningAuthorizer(t, hardeningUnaryFullMethod, hardeningStreamFullMethod)
	var desc *grpc.ServiceDesc
	harness := newTransportHarnessWithAuthorizer(t, "controller-boot-a", 1<<20, authorizer, func(registrar grpc.ServiceRegistrar) {
		desc = &grpc.ServiceDesc{
			ServiceName: "hardening.Hardening",
			HandlerType: (*hardeningService)(nil),
			Methods: []grpc.MethodDesc{{
				MethodName: "Unary",
				Handler:    hardeningUnaryHandler,
			}},
			Streams: []grpc.StreamDesc{{
				StreamName:    "Stream",
				Handler:       hardeningStreamHandler,
				ServerStreams: true,
			}},
			Metadata: "hardening/hardening.proto",
		}
		registrar.RegisterService(desc, &hardeningServer{})
	})
	defer harness.close()

	desc.ServiceName = "mutated.Service"
	desc.Methods[0].MethodName = "MutatedUnary"
	desc.Methods[0].Handler = nil
	desc.Streams[0].StreamName = "MutatedStream"
	desc.Streams[0].Handler = nil
	desc.Streams[0].ServerStreams = false
	desc.Metadata = "mutated.proto"

	info, ok := harness.sealed.GetServiceInfo()["hardening.Hardening"]
	if !ok {
		t.Fatal("sealed server lost copied service descriptor")
	}
	if len(info.Methods) != 2 {
		t.Fatalf("service methods=%d want 2", len(info.Methods))
	}
	methodNames := map[string]bool{}
	for _, method := range info.Methods {
		methodNames[method.Name] = true
	}
	if !methodNames["Unary"] || !methodNames["Stream"] {
		t.Fatalf("copied method names=%v", methodNames)
	}
	metadata, ok := info.Metadata.(string)
	if !ok {
		t.Fatalf("metadata type=%T want string", info.Metadata)
	}
	if metadata != "hardening/hardening.proto" {
		t.Fatalf("copied metadata=%q", metadata)
	}
}

func TestMultiServiceCollectorRejectsNonStringMetadata(t *testing.T) {
	harness := newTransportHarness(t, "controller-boot-a", 1<<20)
	defer harness.close()
	serverTLS, err := NewServerTLSConfig(harness.bundle.serverCert, harness.bundle.serverKey, harness.bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name        string
		serviceName string
		metadata    any
	}{
		{
			name:        "map",
			serviceName: "hardening.MetadataMap",
			metadata:    map[string][]byte{"source": {1, 2, 3}},
		},
		{
			name:        "slice",
			serviceName: "hardening.MetadataSlice",
			metadata:    []byte{1, 2, 3},
		},
		{
			name:        "pointer",
			serviceName: "hardening.MetadataPointer",
			metadata:    new(string),
		},
		{
			name:        "struct",
			serviceName: "hardening.MetadataStruct",
			metadata:    struct{ Source string }{Source: "mutable"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			desc := hardeningUnaryDescriptor(test.serviceName, hardeningUnaryHandler)
			desc.Metadata = test.metadata
			authorizer := newHardeningAuthorizer(t, "/"+test.serviceName+"/Unary")
			sealed, err := NewMultiServiceGRPCServer(harness.service, serverTLS, GRPCServerOptions{
				MaxMessageBytes:  1 << 20,
				MethodAuthorizer: authorizer,
			}, func(registrar grpc.ServiceRegistrar) {
				registrar.RegisterService(desc, &hardeningServer{})
			})
			if sealed != nil {
				t.Fatal("invalid metadata returned a runnable sealed server")
			}
			if !errors.Is(err, ErrServerConfiguration) ||
				!strings.Contains(err.Error(), "only nil or string is supported") {
				t.Fatalf("construction err=%v want metadata configuration error", err)
			}
		})
	}
}

func TestMultiServiceCollectorRejectsNilInvalidAndUnimplementedRegistrations(t *testing.T) {
	harness := newTransportHarness(t, "controller-boot-a", 1<<20)
	defer harness.close()
	serverTLS, err := NewServerTLSConfig(harness.bundle.serverCert, harness.bundle.serverKey, harness.bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}

	var typedNil *hardeningServer
	tests := []struct {
		name         string
		desc         *grpc.ServiceDesc
		impl         any
		rules        map[string][]string
		wantContains string
	}{
		{
			name: "nil implementation",
			desc: hardeningUnaryDescriptor("hardening.NilImpl", hardeningUnaryHandler),
			rules: map[string][]string{
				"/hardening.NilImpl/Unary": {"controller.test"},
			},
			wantContains: "implementation is nil",
		},
		{
			name:         "typed nil implementation",
			desc:         hardeningUnaryDescriptor("hardening.TypedNilImpl", hardeningUnaryHandler),
			impl:         typedNil,
			rules:        map[string][]string{"/hardening.TypedNilImpl/Unary": {"controller.test"}},
			wantContains: "implementation is nil",
		},
		{
			name: "nil method handler",
			desc: &grpc.ServiceDesc{
				ServiceName: "hardening.NilMethod",
				HandlerType: (*hardeningService)(nil),
				Methods:     []grpc.MethodDesc{{MethodName: "Unary"}},
			},
			impl:         &hardeningServer{},
			rules:        map[string][]string{"/hardening.NilMethod/Unary": {"controller.test"}},
			wantContains: "method \"Unary\" handler is nil",
		},
		{
			name: "nil stream handler",
			desc: &grpc.ServiceDesc{
				ServiceName: "hardening.NilStream",
				HandlerType: (*hardeningService)(nil),
				Streams:     []grpc.StreamDesc{{StreamName: "Stream", ServerStreams: true}},
			},
			impl:         &hardeningServer{},
			rules:        map[string][]string{"/hardening.NilStream/Stream": {"controller.test"}},
			wantContains: "stream \"Stream\" handler is nil",
		},
		{
			name: "implementation does not implement interface",
			desc: hardeningUnaryDescriptor("hardening.WrongImpl", hardeningUnaryHandler),
			impl: &wrongHardeningServer{},
			rules: map[string][]string{
				"/hardening.WrongImpl/Unary": {"controller.test"},
			},
			wantContains: "does not implement",
		},
		{
			name: "invalid method name",
			desc: &grpc.ServiceDesc{
				ServiceName: "hardening.InvalidName",
				HandlerType: (*hardeningService)(nil),
				Methods:     []grpc.MethodDesc{{MethodName: "bad/name", Handler: hardeningUnaryHandler}},
			},
			impl:         &hardeningServer{},
			wantContains: "RPC name",
		},
		{
			name: "duplicate method and stream full method",
			desc: &grpc.ServiceDesc{
				ServiceName: "hardening.DuplicateName",
				HandlerType: (*hardeningService)(nil),
				Methods:     []grpc.MethodDesc{{MethodName: "Same", Handler: hardeningUnaryHandler}},
				Streams:     []grpc.StreamDesc{{StreamName: "Same", Handler: hardeningStreamHandler, ServerStreams: true}},
			},
			impl: &hardeningServer{},
			rules: map[string][]string{
				"/hardening.DuplicateName/Same": {"controller.test"},
			},
			wantContains: "is duplicated",
		},
		{
			name:         "missing method ACL",
			desc:         hardeningUnaryDescriptor("hardening.MissingACL", hardeningUnaryHandler),
			impl:         &hardeningServer{},
			wantContains: "missing a non-empty ACL",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rules := map[string][]string{
				routecontrolpb.RouteSnapshotService_GetSnapshot_FullMethodName:        {"client.test"},
				routecontrolpb.RouteSnapshotService_GetCurrentRevision_FullMethodName: {"client.test"},
				routecontrolpb.RouteSnapshotService_WatchFrom_FullMethodName:          {"client.test"},
			}
			for method, identities := range test.rules {
				rules[method] = identities
			}
			authorizer, err := NewMethodAuthorizer(rules)
			if err != nil {
				t.Fatal(err)
			}
			sealed, err := NewMultiServiceGRPCServer(harness.service, serverTLS, GRPCServerOptions{
				MaxMessageBytes:  1 << 20,
				MethodAuthorizer: authorizer,
			}, func(registrar grpc.ServiceRegistrar) {
				registrar.RegisterService(test.desc, test.impl)
			})
			if sealed != nil {
				t.Fatal("invalid registration returned a runnable sealed server")
			}
			if !errors.Is(err, ErrServerConfiguration) || !strings.Contains(err.Error(), test.wantContains) {
				t.Fatalf("construction err=%v want configuration containing %q", err, test.wantContains)
			}
		})
	}
}

func TestMultiServiceCallbackMustRegisterNewServiceEachTime(t *testing.T) {
	harness := newTransportHarness(t, "controller-boot-a", 1<<20)
	defer harness.close()
	serverTLS, err := NewServerTLSConfig(harness.bundle.serverCert, harness.bundle.serverKey, harness.bundle.caCert)
	if err != nil {
		t.Fatal(err)
	}
	authorizer := newHardeningAuthorizer(t, "/hardening.ValidCallback/Unary")
	valid := func(registrar grpc.ServiceRegistrar) {
		registrar.RegisterService(hardeningUnaryDescriptor("hardening.ValidCallback", hardeningUnaryHandler), &hardeningServer{})
	}
	sealed, err := NewMultiServiceGRPCServer(harness.service, serverTLS, GRPCServerOptions{
		MaxMessageBytes:  1 << 20,
		MethodAuthorizer: authorizer,
	}, valid, func(grpc.ServiceRegistrar) {})
	if sealed != nil {
		t.Fatal("valid+noop callbacks returned a runnable sealed server")
	}
	if !errors.Is(err, ErrServerConfiguration) || !strings.Contains(err.Error(), "callback 1") {
		t.Fatalf("valid+noop err=%v want callback 1 configuration error", err)
	}
}

func TestMultiServiceCollectorSealRejectsRetainedRegistrarAndConcurrentLateCalls(t *testing.T) {
	authorizer := newHardeningAuthorizer(t, "/hardening.Retained/Unary")
	var retained grpc.ServiceRegistrar
	harness := newTransportHarnessWithAuthorizer(t, "controller-boot-a", 1<<20, authorizer, func(registrar grpc.ServiceRegistrar) {
		retained = registrar
		registrar.RegisterService(hardeningUnaryDescriptor("hardening.Retained", hardeningUnaryHandler), &hardeningServer{})
	})
	defer harness.close()
	if retained == nil {
		t.Fatal("callback did not receive registrar")
	}
	if _, ok := retained.(*grpc.Server); ok {
		t.Fatal("callback received a runnable grpc.Server")
	}

	before := harness.sealed.GetServiceInfo()
	var wait sync.WaitGroup
	for index := 0; index < 64; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			retained.RegisterService(hardeningUnaryDescriptor("hardening.Late", hardeningUnaryHandler), &hardeningServer{})
		}()
	}
	wait.Wait()
	after := harness.sealed.GetServiceInfo()
	if !sameGRPCServiceInfo(before, after) {
		t.Fatalf("late retained registrar changed sealed service info: before=%v after=%v", before, after)
	}
	if _, ok := after["hardening.Late"]; ok {
		t.Fatal("late retained registrar added a service after seal")
	}
}

func sameGRPCServiceInfo(left, right map[string]grpc.ServiceInfo) bool {
	if len(left) != len(right) {
		return false
	}
	for serviceName, leftInfo := range left {
		rightInfo, ok := right[serviceName]
		if !ok || !reflect.DeepEqual(leftInfo.Metadata, rightInfo.Metadata) || len(leftInfo.Methods) != len(rightInfo.Methods) {
			return false
		}
		leftMethods := make(map[string]grpc.MethodInfo, len(leftInfo.Methods))
		for _, method := range leftInfo.Methods {
			leftMethods[method.Name] = method
		}
		rightMethods := make(map[string]grpc.MethodInfo, len(rightInfo.Methods))
		for _, method := range rightInfo.Methods {
			rightMethods[method.Name] = method
		}
		if !reflect.DeepEqual(leftMethods, rightMethods) {
			return false
		}
	}
	return true
}

func TestMultiServiceForceAuthorizationRejectsHandlerThatIgnoresInterceptor(t *testing.T) {
	authorizer := newHardeningAuthorizer(t, hardeningUnaryFullMethod)
	holder := &hardeningServer{}
	harness := newTransportHarnessWithAuthorizer(t, "controller-boot-a", 1<<20, authorizer, func(registrar grpc.ServiceRegistrar) {
		registrar.RegisterService(hardeningUnaryDescriptor("hardening.Hardening", hardeningUnaryHandler), holder)
	})
	defer harness.close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	invoke := func(ctx context.Context, conn interface {
		Invoke(context.Context, string, any, any, ...grpc.CallOption) error
	}) error {
		return conn.Invoke(ctx, hardeningUnaryFullMethod, &emptypb.Empty{}, &emptypb.Empty{})
	}
	if err := invoke(ctx, harness.conn); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthorized malicious handler call status=%s err=%v", status.Code(err), err)
	}
	if got := holder.unaryCalls.Load(); got != 0 {
		t.Fatalf("unauthorized malicious handler reached implementation: calls=%d", got)
	}

	controllerCert, controllerKey := signedClientCertificate(t, harness, "controller.test")
	controllerConn := dialTransportConnection(t, harness, controllerCert, controllerKey)
	defer controllerConn.Close()
	if err := invoke(ctx, controllerConn); err != nil {
		t.Fatalf("authorized malicious handler call err=%v", err)
	}
	if got := holder.unaryCalls.Load(); got != 1 {
		t.Fatalf("authorized malicious handler calls=%d want 1", got)
	}
}

func TestMultiServiceForceAuthorizationWrapsStreamHandler(t *testing.T) {
	authorizer, err := NewMethodAuthorizer(map[string][]string{
		hardeningStreamFullMethod: {"controller.test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	called := false
	handler := forceStreamAuthorization(authorizer, hardeningStreamFullMethod, func(any, grpc.ServerStream) error {
		called = true
		return nil
	})
	if err := handler(nil, &authorizerTestStream{
		ctx: context.Background(),
	}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthorized stream status=%s err=%v", status.Code(err), err)
	}
	if called {
		t.Fatal("unauthorized stream reached original handler")
	}
	var typedNil *authorizerTestStream
	if err := handler(nil, typedNil); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("typed-nil stream status=%s err=%v", status.Code(err), err)
	}
	if err := handler(nil, &authorizerTestStream{
		ctx: peerTLSContext(&x509.Certificate{DNSNames: []string{"controller.test"}}, tls.VersionTLS13, true, true),
	}); err != nil {
		t.Fatalf("authorized stream err=%v", err)
	}
	if !called {
		t.Fatal("authorized stream did not reach original handler")
	}
}

func newHardeningAuthorizer(t *testing.T, extraMethods ...string) *MethodAuthorizer {
	t.Helper()
	rules := map[string][]string{
		routecontrolpb.RouteSnapshotService_GetSnapshot_FullMethodName:        {"client.test"},
		routecontrolpb.RouteSnapshotService_GetCurrentRevision_FullMethodName: {"client.test"},
		routecontrolpb.RouteSnapshotService_WatchFrom_FullMethodName:          {"client.test"},
	}
	for _, method := range extraMethods {
		rules[method] = []string{"controller.test"}
	}
	authorizer, err := NewMethodAuthorizer(rules)
	if err != nil {
		t.Fatal(err)
	}
	return authorizer
}

func hardeningUnaryDescriptor(serviceName string, handler grpc.MethodHandler) *grpc.ServiceDesc {
	return &grpc.ServiceDesc{
		ServiceName: serviceName,
		HandlerType: (*hardeningService)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Unary",
			Handler:    handler,
		}},
	}
}

type hardeningService interface {
	hardeningServiceMarker()
}

type hardeningServer struct {
	unaryCalls  atomic.Int32
	streamCalls atomic.Int32
}

func (*hardeningServer) hardeningServiceMarker() {}

type wrongHardeningServer struct{}

func hardeningUnaryHandler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	request := new(emptypb.Empty)
	if err := dec(request); err != nil {
		return nil, err
	}
	srv.(*hardeningServer).unaryCalls.Add(1)
	return &emptypb.Empty{}, nil
}

func hardeningStreamHandler(srv any, stream grpc.ServerStream) error {
	request := new(emptypb.Empty)
	if err := stream.RecvMsg(request); err != nil {
		return err
	}
	srv.(*hardeningServer).streamCalls.Add(1)
	return stream.SendMsg(&emptypb.Empty{})
}
