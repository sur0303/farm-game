package authority

import (
	"context"
	"errors"
	"fmt"

	"farmgame/internal/routecontrol"
	core "farmgame/internal/scaleplan"
)

var (
	// ErrControlTokenRequired is returned before an Authority mutation when a
	// Manager was composed with a coordinator but the caller did not inherit
	// an issued token.
	ErrControlTokenRequired = errors.New("route authority: issued control token required")
	// ErrControlTokenRejected distinguishes a token that exists from one that
	// the configured coordinator no longer accepts (for example, foreign or
	// expired).
	ErrControlTokenRejected = errors.New("route authority: control token rejected")
)

// controlOperationTokenContextKey is deliberately private. Callers can carry
// an issuer-bound token through nested calls, but cannot manufacture a value
// that the Manager will accept: the configured coordinator verifies the token
// against its own issuer, scope and lease.
type controlOperationTokenContextKey struct{}

// WithControlOperationToken carries one inherited control token to nested
// RoutePlan -> Manager -> publisher calls. It does not mint or validate a
// token; validation remains the coordinator's responsibility.
func WithControlOperationToken(ctx context.Context, token routecontrol.NestedControlOperationToken) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, controlOperationTokenContextKey{}, token)
}

// ControlOperationTokenFromContext returns only an issued token. The boolean
// is presence information, never an authorization result; a Manager must still
// call its coordinator's Verify method.
func ControlOperationTokenFromContext(ctx context.Context) (routecontrol.NestedControlOperationToken, bool) {
	if ctx == nil {
		return routecontrol.NestedControlOperationToken{}, false
	}
	token, ok := ctx.Value(controlOperationTokenContextKey{}).(routecontrol.NestedControlOperationToken)
	if !ok || !token.Issued() {
		return routecontrol.NestedControlOperationToken{}, false
	}
	return token, true
}

func verifyControlOperationToken(ctx context.Context, coordinator core.ControlMutationCoordinator, scope core.MutationScope) (routecontrol.NestedControlOperationToken, error) {
	if coordinator == nil {
		return routecontrol.NestedControlOperationToken{}, nil
	}
	token, ok := ControlOperationTokenFromContext(ctx)
	if !ok {
		return routecontrol.NestedControlOperationToken{}, ErrControlTokenRequired
	}
	if err := coordinator.Verify(ctx, token, scope); err != nil {
		return routecontrol.NestedControlOperationToken{}, fmt.Errorf("%w: %v", ErrControlTokenRejected, err)
	}
	return token, nil
}
