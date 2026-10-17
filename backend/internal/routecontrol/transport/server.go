package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	routecontrolpb "farmgame/api/gen/routecontrol"
	core "farmgame/internal/routecontrol"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// RouteSnapshotServer exposes only the read-only complete-snapshot service.
// ControllerID and Boot are construction-time facts and are never accepted
// from an RPC request or metadata.
type RouteSnapshotServer struct {
	routecontrolpb.UnimplementedRouteSnapshotServiceServer

	hub          *core.SnapshotWatchHub
	controllerID string
	boot         string
	realm        string
	maxMessage   int
	sendTimeout  time.Duration
	allowedPeers map[string]struct{}
}

type Server = RouteSnapshotServer

// NewRouteSnapshotServer builds the read-only service around an already
// bootstrapped/reconciled SnapshotWatchHub.
func NewRouteSnapshotServer(hub *core.SnapshotWatchHub, options ServerOptions) (*RouteSnapshotServer, error) {
	if hub == nil {
		return nil, invalidConfiguration("snapshot watch hub required")
	}
	if !validRequiredIdentity(options.ControllerID) || !validRequiredIdentity(options.Boot) {
		return nil, invalidConfiguration("controller id and boot must be non-empty")
	}
	allowedPeers, err := normalizeAllowedClientIdentities(options.AllowedClientIdentity)
	if err != nil {
		return nil, invalidConfiguration("allowed client identity: %v", err)
	}
	result := &RouteSnapshotServer{
		hub:          hub,
		controllerID: strings.TrimSpace(options.ControllerID),
		boot:         strings.TrimSpace(options.Boot),
		realm:        strings.TrimSpace(options.Realm),
		maxMessage:   normalizedMaxMessageBytes(options.MaxMessageBytes),
		sendTimeout:  normalizedSendTimeout(options.SendTimeout),
		allowedPeers: allowedPeers,
	}
	return result, nil
}

func NewServer(hub *core.SnapshotWatchHub, options ServerOptions) (*RouteSnapshotServer, error) {
	return NewRouteSnapshotServer(hub, options)
}

// NewGRPCServer is the compatibility constructor for the RouteSnapshot service
// alone. Its raw *grpc.Server result is intentionally not the multi-service
// composition API.
func NewGRPCServer(service *RouteSnapshotServer, tlsConfig *tls.Config, options ...GRPCServerOptions) (*grpc.Server, error) {
	opts, err := singleGRPCServerOptions(options)
	if err != nil {
		return nil, err
	}
	server, err := newGRPCServer(service, tlsConfig, opts)
	if err != nil {
		return nil, err
	}
	if err := registerGRPCServices(server, []collectedServiceRegistration{{
		desc: &routecontrolpb.RouteSnapshotService_ServiceDesc,
		impl: service,
	}}); err != nil {
		server.Stop()
		return nil, err
	}
	return server, nil
}

// GRPCServiceRegistration is a construction-time service registration
// callback. The registrar is valid only while the constructor is collecting
// registrations; it is never exposed by the returned server.
type GRPCServiceRegistration = func(grpc.ServiceRegistrar)

// SealedGRPCServer exposes only the runtime lifecycle of a composed gRPC
// server. It deliberately does not implement grpc.ServiceRegistrar.
type SealedGRPCServer struct {
	server *grpc.Server
}

// NewMultiServiceGRPCServer constructs a TLS-only server containing the
// RouteSnapshot service and at least one additional service. All registrations
// happen during construction, under an exact method ACL, before the sealed
// lifecycle-only wrapper is returned.
func NewMultiServiceGRPCServer(service *RouteSnapshotServer, tlsConfig *tls.Config, options GRPCServerOptions, registrations ...GRPCServiceRegistration) (*SealedGRPCServer, error) {
	if service == nil {
		return nil, invalidConfiguration("route snapshot service required")
	}
	if options.MethodAuthorizer == nil {
		return nil, invalidConfiguration("multi-service method authorizer required")
	}
	if len(registrations) == 0 {
		return nil, invalidConfiguration("at least one extra service registration is required")
	}
	if err := options.MethodAuthorizer.validateRouteSnapshotACL(service.allowedPeers); err != nil {
		return nil, invalidConfiguration("method authorizer: %v", err)
	}

	collector := newServiceRegistrationCollector(options.MethodAuthorizer)
	defer collector.abort()
	collector.RegisterService(&routecontrolpb.RouteSnapshotService_ServiceDesc, service)
	if err := collector.currentError(); err != nil {
		return nil, err
	}
	for index, registration := range registrations {
		if registration == nil {
			return nil, invalidConfiguration("service registration callback %d is nil", index)
		}
		if err := collector.invoke(index, registration); err != nil {
			return nil, err
		}
	}
	entries, err := collector.seal()
	if err != nil {
		return nil, err
	}
	if len(entries) <= 1 {
		return nil, invalidConfiguration("at least one extra service must be registered")
	}

	server, err := newGRPCServer(service, tlsConfig, options)
	if err != nil {
		return nil, err
	}
	if err := registerGRPCServices(server, entries); err != nil {
		server.Stop()
		return nil, err
	}
	return &SealedGRPCServer{server: server}, nil
}

// NewHolderOnlySealedGRPCServer constructs a sealed gRPC server for one or
// more construction-time Holder-side service registrations. It deliberately
// has no RouteSnapshotServer dependency; callers receive only the sealed
// lifecycle wrapper after every registered descriptor has been copied,
// validated, and authorized.
func NewHolderOnlySealedGRPCServer(tlsConfig *tls.Config, options GRPCServerOptions, registrations ...GRPCServiceRegistration) (*SealedGRPCServer, error) {
	if options.MethodAuthorizer == nil {
		return nil, invalidConfiguration("holder-only method authorizer required")
	}
	if len(registrations) == 0 {
		return nil, invalidConfiguration("at least one holder service registration is required")
	}

	collector := newServiceRegistrationCollector(options.MethodAuthorizer)
	defer collector.abort()
	for index, registration := range registrations {
		if registration == nil {
			return nil, invalidConfiguration("service registration callback %d is nil", index)
		}
		if err := collector.invoke(index, registration); err != nil {
			return nil, err
		}
	}
	entries, err := collector.seal()
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, invalidConfiguration("at least one holder service must be registered")
	}
	if err := validateExactRegisteredMethodACL(options.MethodAuthorizer, entries); err != nil {
		return nil, err
	}

	server, err := newTLSAuthorizerGRPCServer(tlsConfig, options)
	if err != nil {
		return nil, err
	}
	if err := registerGRPCServices(server, entries); err != nil {
		server.Stop()
		return nil, err
	}
	return &SealedGRPCServer{server: server}, nil
}

// Serve accepts TLS-authenticated gRPC connections and blocks until the
// sealed server is stopped or the listener fails.
func (s *SealedGRPCServer) Serve(listener net.Listener) error {
	if s == nil || s.server == nil {
		return invalidConfiguration("sealed gRPC server required")
	}
	if listener == nil {
		return invalidConfiguration("listener required")
	}
	return s.server.Serve(listener)
}

func (s *SealedGRPCServer) GracefulStop() {
	if s != nil && s.server != nil {
		s.server.GracefulStop()
	}
}

func (s *SealedGRPCServer) Stop() {
	if s != nil && s.server != nil {
		s.server.Stop()
	}
}

func (s *SealedGRPCServer) GetServiceInfo() map[string]grpc.ServiceInfo {
	if s == nil || s.server == nil {
		return nil
	}
	return s.server.GetServiceInfo()
}

func singleGRPCServerOptions(options []GRPCServerOptions) (GRPCServerOptions, error) {
	if len(options) > 1 {
		return GRPCServerOptions{}, invalidConfiguration("at most one gRPC server options value is allowed")
	}
	if len(options) == 0 {
		return GRPCServerOptions{}, nil
	}
	return options[0], nil
}

func newGRPCServer(service *RouteSnapshotServer, tlsConfig *tls.Config, opts GRPCServerOptions) (*grpc.Server, error) {
	if service == nil {
		return nil, invalidConfiguration("route snapshot service required")
	}
	if tlsConfig == nil {
		return nil, ErrTLSRequired
	}
	clone := tlsConfig.Clone()
	if clone.MinVersion == 0 {
		clone.MinVersion = tls.VersionTLS13
	}
	if err := validateServerTLSConfig(clone); err != nil {
		return nil, err
	}
	if opts.MethodAuthorizer != nil {
		if err := opts.MethodAuthorizer.validateRouteSnapshotACL(service.allowedPeers); err != nil {
			return nil, invalidConfiguration("method authorizer: %v", err)
		}
	}
	if opts.SendTimeout > 0 {
		service.sendTimeout = opts.SendTimeout
	}
	maxMessage := normalizedMaxMessageBytes(opts.MaxMessageBytes)
	unaryInterceptor := grpc.UnaryServerInterceptor(service.unaryAuthInterceptor)
	streamInterceptor := grpc.StreamServerInterceptor(service.streamAuthInterceptor)
	if opts.MethodAuthorizer != nil {
		unaryInterceptor = opts.MethodAuthorizer.UnaryServerInterceptor
		streamInterceptor = opts.MethodAuthorizer.StreamServerInterceptor
	}
	server := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(clone)),
		grpc.MaxRecvMsgSize(maxMessage),
		grpc.MaxSendMsgSize(maxMessage),
		grpc.UnaryInterceptor(unaryInterceptor),
		grpc.StreamInterceptor(streamInterceptor),
	)
	return server, nil
}

// newTLSAuthorizerGRPCServer is the low-level server constructor for sealed
// non-snapshot composition. Keeping this separate preserves the existing
// RouteSnapshot-specific validation in newGRPCServer while sharing its strict
// TLS, message-limit, and transport-owned authorization properties.
func newTLSAuthorizerGRPCServer(tlsConfig *tls.Config, opts GRPCServerOptions) (*grpc.Server, error) {
	if tlsConfig == nil {
		return nil, ErrTLSRequired
	}
	if opts.MethodAuthorizer == nil {
		return nil, invalidConfiguration("method authorizer required")
	}
	clone := tlsConfig.Clone()
	if clone.MinVersion == 0 {
		clone.MinVersion = tls.VersionTLS13
	}
	if err := validateServerTLSConfig(clone); err != nil {
		return nil, err
	}
	maxMessage := normalizedMaxMessageBytes(opts.MaxMessageBytes)
	return grpc.NewServer(
		grpc.Creds(credentials.NewTLS(clone)),
		grpc.MaxRecvMsgSize(maxMessage),
		grpc.MaxSendMsgSize(maxMessage),
		grpc.UnaryInterceptor(opts.MethodAuthorizer.UnaryServerInterceptor),
		grpc.StreamInterceptor(opts.MethodAuthorizer.StreamServerInterceptor),
	), nil
}

// Serve accepts TLS-authenticated gRPC connections and blocks until the
// server is stopped or the listener fails.
func Serve(listener net.Listener, service *RouteSnapshotServer, tlsConfig *tls.Config, options ...GRPCServerOptions) (*grpc.Server, error) {
	if len(options) > 1 {
		return nil, invalidConfiguration("at most one gRPC server options value is allowed")
	}
	if listener == nil {
		return nil, invalidConfiguration("listener required")
	}
	server, err := NewGRPCServer(service, tlsConfig, options...)
	if err != nil {
		return nil, err
	}
	if err := server.Serve(listener); err != nil {
		return server, err
	}
	return server, nil
}

type collectedServiceRegistration struct {
	desc *grpc.ServiceDesc
	impl any
}

type serviceRegistrationCollector struct {
	mu         sync.Mutex
	closed     bool
	entries    []collectedServiceRegistration
	services   map[string]struct{}
	err        error
	authorizer *MethodAuthorizer
}

func newServiceRegistrationCollector(authorizers ...*MethodAuthorizer) *serviceRegistrationCollector {
	var authorizer *MethodAuthorizer
	if len(authorizers) > 0 {
		authorizer = authorizers[0]
	}
	return &serviceRegistrationCollector{
		services:   make(map[string]struct{}),
		authorizer: authorizer,
	}
}

func (c *serviceRegistrationCollector) RegisterService(desc *grpc.ServiceDesc, impl any) {
	if c == nil {
		return
	}
	c.registerService(desc, impl)
}

func (c *serviceRegistrationCollector) invoke(index int, registration GRPCServiceRegistration) (err error) {
	if c == nil {
		return invalidConfiguration("service registration collector is nil")
	}
	before := c.entryCount()
	scope := &scopedServiceRegistrar{
		collector: c,
		active:    true,
	}
	returned := false
	defer func() {
		registered := scope.close()
		if recovered := recover(); recovered != nil {
			err = invalidConfiguration("service registration callback %d panicked: %v", index, recovered)
		} else if !returned {
			err = invalidConfiguration("service registration callback %d panicked", index)
		} else if collectorErr := c.currentError(); collectorErr != nil {
			err = collectorErr
		} else if after := c.entryCount(); after-before != registered || registered == 0 {
			err = invalidConfiguration("service registration callback %d must register at least one new service", index)
		}
	}()
	registration(scope)
	returned = true
	return nil
}

func (c *serviceRegistrationCollector) registerService(desc *grpc.ServiceDesc, impl any) bool {
	entry, err := c.prepare(desc, impl)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.err != nil {
		return false
	}
	if err != nil {
		c.err = err
		return false
	}
	if _, exists := c.services[entry.desc.ServiceName]; exists {
		c.err = invalidConfiguration("duplicate service registration %q", entry.desc.ServiceName)
		return false
	}
	c.services[entry.desc.ServiceName] = struct{}{}
	c.entries = append(c.entries, entry)
	return true
}

func (c *serviceRegistrationCollector) prepare(desc *grpc.ServiceDesc, impl any) (collectedServiceRegistration, error) {
	if desc == nil {
		return collectedServiceRegistration{}, invalidConfiguration("service descriptor is nil")
	}
	if strings.TrimSpace(desc.ServiceName) != desc.ServiceName || !validGRPCName(desc.ServiceName, true) {
		return collectedServiceRegistration{}, invalidConfiguration("service name %q is invalid", desc.ServiceName)
	}
	if isNilValue(impl) {
		return collectedServiceRegistration{}, invalidConfiguration("service %q implementation is nil", desc.ServiceName)
	}
	if desc.HandlerType == nil {
		return collectedServiceRegistration{}, invalidConfiguration("service %q handler type is nil", desc.ServiceName)
	}
	handlerType := reflect.TypeOf(desc.HandlerType)
	if handlerType.Kind() != reflect.Pointer || handlerType.Elem().Kind() != reflect.Interface {
		return collectedServiceRegistration{}, invalidConfiguration("service %q handler type is invalid", desc.ServiceName)
	}
	implType := reflect.TypeOf(impl)
	if !implType.Implements(handlerType.Elem()) {
		return collectedServiceRegistration{}, invalidConfiguration("service %q implementation %v does not implement %v", desc.ServiceName, implType, handlerType.Elem())
	}

	clone := &grpc.ServiceDesc{
		ServiceName: desc.ServiceName,
		HandlerType: desc.HandlerType,
	}
	metadata, err := immutableServiceMetadata(desc.Metadata)
	if err != nil {
		return collectedServiceRegistration{}, invalidConfiguration("service %q metadata: %v", desc.ServiceName, err)
	}
	clone.Metadata = metadata

	if desc.Methods != nil {
		clone.Methods = make([]grpc.MethodDesc, len(desc.Methods))
	}
	if desc.Streams != nil {
		clone.Streams = make([]grpc.StreamDesc, len(desc.Streams))
	}
	names := make(map[string]struct{}, len(desc.Methods)+len(desc.Streams))
	for index, method := range desc.Methods {
		fullMethod, err := canonicalRegisteredFullMethod(desc.ServiceName, method.MethodName)
		if err != nil {
			return collectedServiceRegistration{}, invalidConfiguration("service %q method %d: %v", desc.ServiceName, index, err)
		}
		if method.Handler == nil {
			return collectedServiceRegistration{}, invalidConfiguration("service %q method %q handler is nil", desc.ServiceName, method.MethodName)
		}
		if _, exists := names[method.MethodName]; exists {
			return collectedServiceRegistration{}, invalidConfiguration("service %q method %q is duplicated", desc.ServiceName, method.MethodName)
		}
		if err := c.authorizer.validateRegisteredMethod(fullMethod); err != nil {
			return collectedServiceRegistration{}, invalidConfiguration("service %q method %q ACL: %v", desc.ServiceName, method.MethodName, err)
		}
		names[method.MethodName] = struct{}{}
		method.Handler = forceUnaryAuthorization(c.authorizer, fullMethod, method.Handler)
		clone.Methods[index] = method
	}
	for index, stream := range desc.Streams {
		fullMethod, err := canonicalRegisteredFullMethod(desc.ServiceName, stream.StreamName)
		if err != nil {
			return collectedServiceRegistration{}, invalidConfiguration("service %q stream %d: %v", desc.ServiceName, index, err)
		}
		if stream.Handler == nil {
			return collectedServiceRegistration{}, invalidConfiguration("service %q stream %q handler is nil", desc.ServiceName, stream.StreamName)
		}
		if _, exists := names[stream.StreamName]; exists {
			return collectedServiceRegistration{}, invalidConfiguration("service %q stream %q is duplicated", desc.ServiceName, stream.StreamName)
		}
		if err := c.authorizer.validateRegisteredMethod(fullMethod); err != nil {
			return collectedServiceRegistration{}, invalidConfiguration("service %q stream %q ACL: %v", desc.ServiceName, stream.StreamName, err)
		}
		names[stream.StreamName] = struct{}{}
		stream.Handler = forceStreamAuthorization(c.authorizer, fullMethod, stream.Handler)
		clone.Streams[index] = stream
	}
	return collectedServiceRegistration{desc: clone, impl: impl}, nil
}

func (c *serviceRegistrationCollector) entryCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

func (c *serviceRegistrationCollector) currentError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *serviceRegistrationCollector) seal() ([]collectedServiceRegistration, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, invalidConfiguration("service registration collector is already closed")
	}
	c.closed = true
	if c.err != nil {
		return nil, c.err
	}
	entries := make([]collectedServiceRegistration, len(c.entries))
	copy(entries, c.entries)
	return entries, nil
}

func (c *serviceRegistrationCollector) abort() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
}

func validateExactRegisteredMethodACL(authorizer *MethodAuthorizer, entries []collectedServiceRegistration) error {
	if authorizer == nil || len(authorizer.rules) == 0 {
		return invalidConfiguration("method authorizer is unavailable")
	}
	registered := make(map[string]struct{})
	for _, entry := range entries {
		if entry.desc == nil {
			return invalidConfiguration("collected service descriptor is nil")
		}
		for _, method := range entry.desc.Methods {
			fullMethod, err := canonicalRegisteredFullMethod(entry.desc.ServiceName, method.MethodName)
			if err != nil {
				return invalidConfiguration("registered method ACL: %v", err)
			}
			registered[fullMethod] = struct{}{}
		}
		for _, stream := range entry.desc.Streams {
			fullMethod, err := canonicalRegisteredFullMethod(entry.desc.ServiceName, stream.StreamName)
			if err != nil {
				return invalidConfiguration("registered stream ACL: %v", err)
			}
			registered[fullMethod] = struct{}{}
		}
	}
	for method := range authorizer.rules {
		if _, ok := registered[method]; !ok {
			return invalidConfiguration("method authorizer ACL contains unregistered method %q", method)
		}
	}
	if len(registered) != len(authorizer.rules) {
		return invalidConfiguration("method authorizer ACL must exactly match registered methods")
	}
	return nil
}

type scopedServiceRegistrar struct {
	collector  *serviceRegistrationCollector
	mu         sync.Mutex
	active     bool
	registered int
}

func (r *scopedServiceRegistrar) RegisterService(desc *grpc.ServiceDesc, impl any) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.active {
		return
	}
	if r.collector.registerService(desc, impl) {
		r.registered++
	}
}

func (r *scopedServiceRegistrar) close() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.active = false
	return r.registered
}

func canonicalRegisteredFullMethod(serviceName, methodName string) (string, error) {
	if strings.TrimSpace(methodName) != methodName || !validGRPCName(methodName, false) {
		return "", fmt.Errorf("RPC name %q is invalid", methodName)
	}
	return canonicalFullMethod("/" + serviceName + "/" + methodName)
}

func registerGRPCServices(server *grpc.Server, entries []collectedServiceRegistration) (err error) {
	if server == nil {
		return invalidConfiguration("gRPC server is nil")
	}
	completed := false
	defer func() {
		if recovered := recover(); recovered != nil {
			err = invalidConfiguration("gRPC service registration panicked: %v", recovered)
		} else if !completed {
			err = invalidConfiguration("gRPC service registration panicked")
		}
	}()
	for _, entry := range entries {
		server.RegisterService(entry.desc, entry.impl)
	}
	completed = true
	return nil
}

func forceUnaryAuthorization(authorizer *MethodAuthorizer, fullMethod string, handler grpc.MethodHandler) grpc.MethodHandler {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		if err := authorizer.Authorize(ctx, fullMethod); err != nil {
			return nil, transportStatusError(err)
		}
		return handler(srv, ctx, dec, interceptor)
	}
}

func forceStreamAuthorization(authorizer *MethodAuthorizer, fullMethod string, handler grpc.StreamHandler) grpc.StreamHandler {
	return func(srv any, stream grpc.ServerStream) error {
		if isNilValue(stream) {
			return transportStatusError(ErrPeerUnauthenticated)
		}
		if err := authorizer.Authorize(stream.Context(), fullMethod); err != nil {
			return transportStatusError(err)
		}
		return handler(srv, stream)
	}
}

func immutableServiceMetadata(metadata any) (any, error) {
	if metadata == nil {
		return nil, nil
	}
	if value, ok := metadata.(string); ok {
		return value, nil
	}
	return nil, fmt.Errorf("metadata type %T is invalid; only nil or string is supported", metadata)
}

func (s *RouteSnapshotServer) GetSnapshot(ctx context.Context, _ *routecontrolpb.GetSnapshotRequest) (*routecontrolpb.RouteSnapshotEnvelope, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, transportStatusError(err)
	}
	return s.currentEnvelope(ctx)
}

func (s *RouteSnapshotServer) GetCurrentRevision(ctx context.Context, _ *routecontrolpb.GetCurrentRevisionRequest) (*routecontrolpb.RouteSnapshotEnvelope, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, transportStatusError(err)
	}
	// The method name is intentionally revision-oriented, but it returns the
	// same complete validated envelope as GetSnapshot.
	return s.currentEnvelope(ctx)
}

func (s *RouteSnapshotServer) WatchFrom(request *routecontrolpb.WatchFromRequest, stream grpc.ServerStreamingServer[routecontrolpb.RouteSnapshotEnvelope]) error {
	if err := s.authorize(stream.Context()); err != nil {
		return transportStatusError(err)
	}
	if request == nil {
		return status.Error(codes.InvalidArgument, "watch request required")
	}

	// A Watch is not established until the peer has been authorized, a real
	// bounded Hub subscription exists, and the complete authenticated current
	// envelope has been sent. Read durable current R first, then subscribe from
	// R (never from caller-controlled history). If R+1 races after the read,
	// Subscribe reconciles/registers atomically and queues R+1 behind the
	// mandatory R handshake. A caller ahead of durable current is a rollback
	// contract violation rather than an empty, apparently established Watch.
	current, found, err := s.hub.Current(stream.Context())
	if err != nil {
		return transportStatusError(err)
	}
	if !found {
		return transportStatusError(ErrSnapshotUnavailable)
	}
	if request.GetAfterRevision() > current.Revision {
		return status.Error(codes.FailedPrecondition, "watch revision is ahead of durable current")
	}
	subscription, err := s.hub.Subscribe(stream.Context(), current.Revision)
	if err != nil {
		return transportStatusError(err)
	}
	defer subscription.Close()
	handshake, err := storedToEnvelope(current, s.controllerID, s.boot, s.realm, s.maxMessage)
	if err != nil {
		return transportStatusError(err)
	}
	if err := s.sendEnvelope(stream, handshake); err != nil {
		return err
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case stored, ok := <-subscription.Updates:
			if !ok {
				if err := subscription.Err(); err != nil {
					if errors.Is(err, core.ErrSnapshotWatchSlowConsumer) {
						return status.Error(codes.ResourceExhausted, "watch consumer too slow")
					}
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						return err
					}
					if errors.Is(err, core.ErrSnapshotHubClosed) {
						return status.Error(codes.Unavailable, "snapshot watch hub closed")
					}
					return transportStatusError(err)
				}
				return nil
			}
			envelope, err := storedToEnvelope(stored, s.controllerID, s.boot, s.realm, s.maxMessage)
			if err != nil {
				return transportStatusError(err)
			}
			if err := s.sendEnvelope(stream, envelope); err != nil {
				return err
			}
		}
	}
}

func (s *RouteSnapshotServer) sendEnvelope(stream grpc.ServerStreamingServer[routecontrolpb.RouteSnapshotEnvelope], envelope *routecontrolpb.RouteSnapshotEnvelope) error {
	if stream == nil {
		return status.Error(codes.Internal, "snapshot stream is required")
	}
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- stream.Send(envelope)
	}()
	timer := time.NewTimer(s.sendTimeout)
	defer timer.Stop()
	select {
	case err := <-sendDone:
		return err
	case <-stream.Context().Done():
		select {
		case err := <-sendDone:
			return err
		case <-time.After(serverSendCancelGrace):
			return stream.Context().Err()
		}
	case <-timer.C:
		if canceler, ok := any(stream).(interface{ CancelSend() }); ok {
			canceler.CancelSend()
			select {
			case <-sendDone:
			case <-time.After(serverSendCancelGrace):
			}
		}
		return status.Error(codes.ResourceExhausted, "snapshot stream send timeout")
	}
}

func (s *RouteSnapshotServer) currentEnvelope(ctx context.Context) (*routecontrolpb.RouteSnapshotEnvelope, error) {
	if ctx == nil {
		return nil, status.Error(codes.InvalidArgument, "context required")
	}
	stored, found, err := s.hub.Current(ctx)
	if err != nil {
		return nil, transportStatusError(err)
	}
	if !found {
		return nil, transportStatusError(ErrSnapshotUnavailable)
	}
	envelope, err := storedToEnvelope(stored, s.controllerID, s.boot, s.realm, s.maxMessage)
	if err != nil {
		return nil, transportStatusError(err)
	}
	return envelope, nil
}

func (s *RouteSnapshotServer) authorize(ctx context.Context) error {
	if s == nil {
		return ErrPeerUnauthenticated
	}
	identity, err := VerifiedPeerIdentity(ctx)
	if err != nil {
		return err
	}
	if len(s.allowedPeers) > 0 {
		if _, ok := s.allowedPeers[identity]; !ok {
			return fmt.Errorf("%w: %s", ErrPeerIdentityNotAllowed, identity)
		}
	}
	return nil
}

func (s *RouteSnapshotServer) unaryAuthInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, transportStatusError(err)
	}
	return handler(ctx, req)
}

func (s *RouteSnapshotServer) streamAuthInterceptor(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := s.authorize(stream.Context()); err != nil {
		return transportStatusError(err)
	}
	return handler(srv, stream)
}
