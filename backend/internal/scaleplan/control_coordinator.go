package scaleplan

import (
	"context"
	"fmt"

	"farmgame/internal/controlfence"
)

// ControlOperationToken is the inheritable proof that a control-plane mutation
// runs under an acquired gate. It aliases the leaf package's type so that the
// data plane and the control plane can both name it without importing each
// other (§8.1).
type ControlOperationToken = controlfence.NestedControlOperationToken

// ControlGateMode is the durable gate state, not the process mode.
type ControlGateMode string

const (
	ControlGateOpen      ControlGateMode = "OPEN"
	ControlGateClosing   ControlGateMode = "CLOSING"
	ControlGateExclusive ControlGateMode = "EXCLUSIVE"
)

// ControlOperationKind distinguishes the writers a gate must account for when
// deciding whether the active set is empty.
type ControlOperationKind string

const (
	ControlOperationScalePlan   ControlOperationKind = "SCALEPLAN"
	ControlOperationRoutePlan   ControlOperationKind = "ROUTEPLAN"
	ControlOperationWorkload    ControlOperationKind = "WORKLOAD"
	ControlOperationProjection  ControlOperationKind = "PROJECTION"
	ControlOperationRecovery    ControlOperationKind = "RECOVERY"
	ControlOperationBootstrap   ControlOperationKind = "BOOTSTRAP"
	ControlOperationResolveOnly ControlOperationKind = "RESOLUTION_ONLY"
)

// ControlOperationRequest registers one top-level writer. The operation ID is
// deterministic so that a controller which crashes between registering and
// resolving finds its own record on restart instead of creating a second one.
type ControlOperationRequest struct {
	OperationID string
	Kind        ControlOperationKind
	Scope       MutationScope
	Owner       MutationLeaseOwner
	// ResolutionOnly requests the one exception the gate makes during CLOSING:
	// the gate owner may still register work whose only purpose is to resolve
	// an operation that is already active.
	ResolutionOnly bool
}

func (r ControlOperationRequest) Validate() error {
	if err := validateOpaque("control operation id", r.OperationID, MaxOpaqueIDBytes); err != nil {
		return err
	}
	if r.Kind == "" {
		return fmt.Errorf("%w: control operation kind", ErrInvalidPlan)
	}
	if err := r.Scope.Validate(); err != nil {
		return err
	}
	return r.Owner.Validate()
}

// ControlMutationCoordinator is the shared gate every Manager, RoutePlan and
// Kubernetes control writer passes through. A0.5 freezes the contract; the
// production implementation is A3's.
type ControlMutationCoordinator interface {
	// Acquire registers a top-level writer and returns an inheritable token.
	// It fails closed when the gate is CLOSING or EXCLUSIVE unless the request
	// is resolution-only and the caller owns the gate.
	Acquire(context.Context, ControlOperationRequest) (ControlOperationToken, error)
	// Verify checks an inherited token without registering anything. Nested
	// callers use it; re-registering would deadlock a non-reentrant gate.
	Verify(context.Context, ControlOperationToken, MutationScope) error
	// Resolve removes the active operation after its effect is durable.
	Resolve(context.Context, ControlOperationToken) error
	// Mode reports the durable gate state for the scope.
	Mode(context.Context, MutationScope) (ControlGateMode, error)
}

// RuntimeMode is the process mode. Auto still uses the same durable mutation
// coordinator and ScalePlan executor as manual mode; only the source of the
// explicit manifest changes.
type RuntimeMode string

const (
	RuntimeModeOff    RuntimeMode = "off"
	RuntimeModeManual RuntimeMode = "manual"
	RuntimeModeAuto   RuntimeMode = "auto"
)

func (m RuntimeMode) Valid() bool {
	return m == RuntimeModeOff || m == RuntimeModeManual || m == RuntimeModeAuto
}

// BindCoordinatorToMode is the startup-time tri-state check. Both mismatches
// are fatal, and deliberately so in opposite directions: a non-off process
// without a coordinator would perform ungated writes, and an off process with
// one would have side effects that off promises not to have.
//
// The decision happens once, at startup, rather than on every mutation. A
// missing token at mutation time then means "this process should not be
// running", which is a state the process cannot reach, instead of a branch
// that silently permits the write.
func BindCoordinatorToMode(mode RuntimeMode, coordinator ControlMutationCoordinator) error {
	if !mode.Valid() {
		return fmt.Errorf("%w: unknown runtime mode %q", ErrInvalidPlan, string(mode))
	}
	present := coordinator != nil
	if mode == RuntimeModeOff && present {
		return fmt.Errorf("%w: mode=off must not be assembled with a control coordinator", ErrInvalidPlan)
	}
	if mode != RuntimeModeOff && !present {
		return fmt.Errorf("%w: mode=%s requires a control coordinator", ErrInvalidPlan, string(mode))
	}
	return nil
}

// RequireControlToken is the check every gated writer performs on an inherited
// token. It exists so that no caller has to remember that the zero value is
// not a credential.
func RequireControlToken(token ControlOperationToken) error {
	if !token.Issued() {
		return fmt.Errorf("%w: control mutation without an issued token", ErrInvalidPlan)
	}
	return nil
}
