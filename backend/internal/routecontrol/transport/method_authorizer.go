package transport

import (
	"context"
	"fmt"
	"strings"

	routecontrolpb "farmgame/api/gen/routecontrol"

	"google.golang.org/grpc"
)

// MethodAuthorizer is an immutable exact gRPC full-method ACL. Its rules are
// copied and canonicalized by NewMethodAuthorizer.
type MethodAuthorizer struct {
	rules          map[string]map[string]struct{}
	verifyIdentity func(context.Context) (string, error)
}

// NewMethodAuthorizer constructs an exact full-method ACL. Every configured
// method must have at least one canonical peer identity; no wildcard or
// prefix matching is supported.
func NewMethodAuthorizer(rules map[string][]string) (*MethodAuthorizer, error) {
	return newMethodAuthorizer(rules, normalizeMethodIdentities, VerifiedPeerIdentity)
}

// NewExactDNSMethodAuthorizer constructs the DNS-only exact-leaf ACL used by
// Gate verification. It intentionally does not change the generic URI-capable
// authorizer used by Holder/SPIFFE consumers.
func NewExactDNSMethodAuthorizer(rules map[string][]string) (*MethodAuthorizer, error) {
	return newMethodAuthorizer(rules, normalizeExactDNSMethodIdentities, VerifiedExactDNSPeerIdentity)
}

func newMethodAuthorizer(rules map[string][]string, normalize func([]string) (map[string]struct{}, error), verify func(context.Context) (string, error)) (*MethodAuthorizer, error) {
	if len(rules) == 0 {
		return nil, invalidConfiguration("method authorizer rules required")
	}
	if normalize == nil || verify == nil {
		return nil, invalidConfiguration("method authorizer identity policy required")
	}
	normalized := make(map[string]map[string]struct{}, len(rules))
	for fullMethod, identities := range rules {
		method, err := canonicalFullMethod(fullMethod)
		if err != nil {
			return nil, invalidConfiguration("method %q: %v", fullMethod, err)
		}
		allowed, err := normalize(identities)
		if err != nil {
			return nil, invalidConfiguration("method %q identities: %v", fullMethod, err)
		}
		normalized[method] = allowed
	}
	return &MethodAuthorizer{rules: normalized, verifyIdentity: verify}, nil
}

// Authorize authenticates the verified TLS peer for one exact full method.
func (a *MethodAuthorizer) Authorize(ctx context.Context, fullMethod string) error {
	if isNilValue(ctx) {
		return ErrPeerUnauthenticated
	}
	if a == nil || len(a.rules) == 0 || a.verifyIdentity == nil {
		return methodAuthorizationError("method authorizer is unavailable")
	}
	method, err := canonicalFullMethod(fullMethod)
	if err != nil {
		return methodAuthorizationError("invalid gRPC full method: %v", err)
	}
	allowed, ok := a.rules[method]
	if !ok || len(allowed) == 0 {
		return methodAuthorizationError("method %q is not authorized", method)
	}
	identity, err := a.verifyIdentity(ctx)
	if err != nil {
		return err
	}
	if _, ok := allowed[identity]; !ok {
		return methodAuthorizationError("identity %q is not authorized for method %q", identity, method)
	}
	return nil
}

// UnaryServerInterceptor returns a fail-closed unary gRPC interceptor.
func (a *MethodAuthorizer) UnaryServerInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if isNilValue(ctx) || info == nil || handler == nil {
		return nil, transportStatusError(ErrPeerUnauthenticated)
	}
	if err := a.Authorize(ctx, info.FullMethod); err != nil {
		return nil, transportStatusError(err)
	}
	return handler(ctx, req)
}

// StreamServerInterceptor returns a fail-closed streaming gRPC interceptor.
func (a *MethodAuthorizer) StreamServerInterceptor(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if isNilValue(stream) || info == nil || handler == nil {
		return transportStatusError(ErrPeerUnauthenticated)
	}
	ctx := stream.Context()
	if isNilValue(ctx) {
		return transportStatusError(ErrPeerUnauthenticated)
	}
	if err := a.Authorize(ctx, info.FullMethod); err != nil {
		return transportStatusError(err)
	}
	return handler(srv, stream)
}

func (a *MethodAuthorizer) validateRouteSnapshotMethods() error {
	if a == nil || len(a.rules) == 0 {
		return fmt.Errorf("method authorizer is unavailable")
	}
	for _, method := range routeSnapshotFullMethods {
		allowed, ok := a.rules[method]
		if !ok || len(allowed) == 0 {
			return fmt.Errorf("required route snapshot method %q is missing", method)
		}
	}
	return nil
}

func (a *MethodAuthorizer) validateRegisteredMethod(fullMethod string) error {
	if a == nil || len(a.rules) == 0 {
		return fmt.Errorf("method authorizer is unavailable")
	}
	method, err := canonicalFullMethod(fullMethod)
	if err != nil {
		return err
	}
	allowed, ok := a.rules[method]
	if !ok || len(allowed) == 0 {
		return fmt.Errorf("method %q is missing a non-empty ACL", method)
	}
	return nil
}

func (a *MethodAuthorizer) validateRouteSnapshotACL(allowedPeers map[string]struct{}) error {
	if err := a.validateRouteSnapshotMethods(); err != nil {
		return err
	}
	for _, method := range routeSnapshotFullMethods {
		if !sameIdentitySet(a.rules[method], allowedPeers) {
			return fmt.Errorf("route snapshot method %q ACL must exactly match service allowed peers", method)
		}
	}
	return nil
}

func sameIdentitySet(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for identity := range left {
		if _, ok := right[identity]; !ok {
			return false
		}
	}
	return true
}

func normalizeMethodIdentities(values []string) (map[string]struct{}, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("%w: at least one method identity is required", ErrPeerIdentityNotAllowed)
	}
	allowed := make(map[string]struct{}, len(values))
	for _, value := range values {
		identity, err := canonicalPeerIdentity(value)
		if err != nil {
			return nil, err
		}
		if identity != value {
			return nil, fmt.Errorf("%w: identity %q is not canonical", ErrPeerIdentityNotAllowed, value)
		}
		if _, exists := allowed[identity]; exists {
			return nil, fmt.Errorf("%w: duplicate method identity %q", ErrPeerIdentityNotAllowed, identity)
		}
		allowed[identity] = struct{}{}
	}
	return allowed, nil
}

func normalizeExactDNSMethodIdentities(values []string) (map[string]struct{}, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("%w: at least one exact DNS method identity is required", ErrPeerIdentityNotAllowed)
	}
	allowed := make(map[string]struct{}, len(values))
	for _, value := range values {
		identity, err := canonicalDNSIdentity(value)
		if err != nil || identity != value {
			return nil, fmt.Errorf("%w: identity %q is not a canonical DNS name", ErrPeerIdentityNotAllowed, value)
		}
		if _, exists := allowed[identity]; exists {
			return nil, fmt.Errorf("%w: duplicate method identity %q", ErrPeerIdentityNotAllowed, identity)
		}
		allowed[identity] = struct{}{}
	}
	return allowed, nil
}

func canonicalFullMethod(value string) (string, error) {
	if value == "" || strings.TrimSpace(value) != value {
		return "", fmt.Errorf("full method must be non-empty and trimmed")
	}
	if strings.Count(value, "/") != 2 || !strings.HasPrefix(value, "/") {
		return "", fmt.Errorf("full method must be /service/method")
	}
	parts := strings.Split(value[1:], "/")
	if len(parts) != 2 || !validGRPCName(parts[0], true) || !validGRPCName(parts[1], false) {
		return "", fmt.Errorf("full method must use canonical gRPC service and method names")
	}
	return value, nil
}

func validGRPCName(value string, qualified bool) bool {
	parts := strings.Split(value, ".")
	if !qualified && len(parts) != 1 {
		return false
	}
	for _, part := range parts {
		if part == "" || !isGRPCNameStart(part[0]) {
			return false
		}
		for i := 1; i < len(part); i++ {
			if !isGRPCNamePart(part[i]) {
				return false
			}
		}
	}
	return true
}

func isGRPCNameStart(value byte) bool {
	return value == '_' ||
		(value >= 'A' && value <= 'Z') ||
		(value >= 'a' && value <= 'z')
}

func isGRPCNamePart(value byte) bool {
	return isGRPCNameStart(value) || (value >= '0' && value <= '9')
}

func methodAuthorizationError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrPeerIdentityNotAllowed, fmt.Sprintf(format, args...))
}

var routeSnapshotFullMethods = [...]string{
	routecontrolpb.RouteSnapshotService_GetSnapshot_FullMethodName,
	routecontrolpb.RouteSnapshotService_GetCurrentRevision_FullMethodName,
	routecontrolpb.RouteSnapshotService_WatchFrom_FullMethodName,
}
