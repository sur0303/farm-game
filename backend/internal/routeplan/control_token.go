package routeplan

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"

	"farmgame/internal/actorrouting/authority"
	core "farmgame/internal/scaleplan"
)

var ErrControlMutation = errors.New("routeplan: control mutation gate")

// ErrControlOperationResolved is returned when Begin cannot acquire because the
// coordinator already resolved this operation ID. The plan revision must
// advance before the same logical execute can be retried.
var ErrControlOperationResolved = errors.New("routeplan: control operation already resolved")

// ControlMutationOptions is the small composition seam shared by RoutePlan
// execution and route-controller background writers. A nil Coordinator is the
// deliberate G21/off path; a non-nil Coordinator makes every top-level
// operation acquire or inherit an issuer-bound token.
type ControlMutationOptions struct {
	Coordinator core.ControlMutationCoordinator
	Scope       core.MutationScope
	Owner       core.MutationLeaseOwner
	NonceSource func() (string, error)
}

func (o ControlMutationOptions) Validate() error {
	if o.Coordinator == nil {
		return nil
	}
	if err := o.Scope.Validate(); err != nil {
		return fmt.Errorf("%w: scope: %v", ErrControlMutation, err)
	}
	if o.Owner.OperationNonce != "" {
		return fmt.Errorf("%w: owner nonce is allocated per top-level operation", ErrControlMutation)
	}
	owner := o.Owner
	owner.OperationNonce = "validation"
	if err := owner.Validate(); err != nil {
		return fmt.Errorf("%w: owner: %v", ErrControlMutation, err)
	}
	if o.NonceSource == nil {
		return fmt.Errorf("%w: nonce source required", ErrControlMutation)
	}
	return nil
}

func planControlOperationID(kind, logicalID string) string {
	digest := sha256.Sum256([]byte(kind + "\x00" + logicalID))
	return fmt.Sprintf("routeplan/%s/%x", kind, digest[:])
}

// Begin starts one top-level mutation or verifies the inherited token when a
// caller is already inside a ScalePlan operation. The returned resolver must
// be called only after the durable effect is known complete. On an unknown
// effect callers intentionally keep the coordinator operation unresolved so
// the gate fails closed until the owner can recover or the lease expires.
func (o ControlMutationOptions) Begin(ctx context.Context, operationID string, kind core.ControlOperationKind) (context.Context, func() error, error) {
	if ctx == nil {
		return nil, nil, fmt.Errorf("%w: context required", ErrControlMutation)
	}
	if o.Coordinator == nil {
		return ctx, nil, nil
	}
	if err := o.Validate(); err != nil {
		return nil, nil, err
	}
	if token, inherited := authority.ControlOperationTokenFromContext(ctx); inherited {
		if err := o.Coordinator.Verify(ctx, token, o.Scope); err != nil {
			return nil, nil, fmt.Errorf("%w: inherited token: %v", ErrControlMutation, err)
		}
		return ctx, nil, nil
	}
	nonce, err := o.NonceSource()
	if err != nil {
		return nil, nil, fmt.Errorf("%w: allocate operation nonce: %v", ErrControlMutation, err)
	}
	owner := o.Owner
	owner.OperationNonce = nonce
	if err := owner.Validate(); err != nil {
		return nil, nil, fmt.Errorf("%w: owner: %v", ErrControlMutation, err)
	}
	request := core.ControlOperationRequest{
		OperationID: operationID,
		Kind:        kind,
		Scope:       o.Scope,
		Owner:       owner,
	}
	token, err := o.Coordinator.Acquire(ctx, request)
	if err != nil {
		return nil, nil, wrapControlAcquireError(operationID, err)
	}
	if !token.Issued() {
		return nil, nil, fmt.Errorf("%w: coordinator returned an unissued token", ErrControlMutation)
	}
	if err := o.Coordinator.Verify(ctx, token, o.Scope); err != nil {
		return nil, nil, fmt.Errorf("%w: acquired token: %v", ErrControlMutation, err)
	}
	bound := authority.WithControlOperationToken(ctx, token)
	var once sync.Once
	var resolveErr error
	resolve := func() error {
		once.Do(func() {
			resolveErr = o.Coordinator.Resolve(context.WithoutCancel(ctx), token)
		})
		return resolveErr
	}
	return bound, resolve, nil
}

func wrapControlAcquireError(operationID string, err error) error {
	if err == nil {
		return fmt.Errorf("%w: acquire %s", ErrControlMutation, operationID)
	}
	wrapped := errors.Join(ErrControlMutation, fmt.Errorf("acquire %s: %w", operationID, err))
	if !isAlreadyResolvedCoordinatorError(err) {
		return wrapped
	}
	return errors.Join(ErrControlOperationResolved, wrapped)
}

func isAlreadyResolvedCoordinatorError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "operation already resolved")
}
