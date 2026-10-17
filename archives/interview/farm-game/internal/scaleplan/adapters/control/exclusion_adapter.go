package control

import (
	"context"
	"errors"
	"fmt"

	"farmgame/internal/actorrouting"
	routecore "farmgame/internal/routecontrol"
	core "farmgame/internal/scaleplan"
	redisroutecontrol "farmgame/internal/storage/redis/routecontrol"
)

var (
	ErrPlacementExclusionAdapterInvalid = errors.New("scaleplan control adapter: invalid placement exclusion dependency")
	ErrPlacementExclusionAdapterDrift   = errors.New("scaleplan control adapter: placement exclusion drift")
	ErrPlacementExclusionAdapterToken   = errors.New("scaleplan control adapter: placement exclusion token unavailable")
)

// PlacementExclusionStore is the exact production seam shared by the
// in-process HolderRegistry and the Redis-backed HolderStore.
type PlacementExclusionStore interface {
	GetPlacementExclusion(context.Context, actorrouting.RouteDomain, string, string) (routecore.PlacementExclusion, bool, error)
	ExcludePlacement(context.Context, actorrouting.RouteDomain, string, string, routecore.PlacementExclusionPrecondition) (routecore.PlacementExclusionResult, error)
	ReleasePlacementExclusionForOwner(context.Context, routecore.NestedControlOperationToken, actorrouting.RouteDomain, string, string, routecore.PlacementExclusionRelease) (routecore.PlacementExclusionResult, error)
}

// PlacementExclusionTokenSource supplies the inherited coordinator token for
// the frozen core ReleaseForOwner method.
type PlacementExclusionTokenSource func(context.Context) (core.ControlOperationToken, error)

type placementExclusionTokenContextKey struct{}

// WithControlOperationToken is the adapter-local context bridge for callers
// that already carry a coordinator-issued token.
func WithControlOperationToken(ctx context.Context, token core.ControlOperationToken) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, placementExclusionTokenContextKey{}, token)
}

// ControlOperationTokenFromContext is the default token source.
func ControlOperationTokenFromContext(ctx context.Context) (core.ControlOperationToken, error) {
	if ctx == nil {
		return core.ControlOperationToken{}, ErrPlacementExclusionAdapterInvalid
	}
	token, ok := ctx.Value(placementExclusionTokenContextKey{}).(core.ControlOperationToken)
	if !ok {
		return core.ControlOperationToken{}, ErrPlacementExclusionAdapterToken
	}
	return token, nil
}

type PlacementExclusionPortOptions struct {
	Store       PlacementExclusionStore
	Coordinator core.ControlMutationCoordinator
	Scope       core.MutationScope
	TokenSource PlacementExclusionTokenSource
}

type placementExclusionAdapter struct {
	store       PlacementExclusionStore
	coordinator core.ControlMutationCoordinator
	scope       core.MutationScope
	tokenSource PlacementExclusionTokenSource
}

// NewPlacementExclusionPort constructs the production PlacementExclusionPort.
func NewPlacementExclusionPort(options PlacementExclusionPortOptions) (core.PlacementExclusionPort, error) {
	if options.Store == nil || options.Coordinator == nil {
		return nil, ErrPlacementExclusionAdapterInvalid
	}
	if err := options.Scope.Validate(); err != nil {
		return nil, err
	}
	if options.TokenSource == nil {
		options.TokenSource = ControlOperationTokenFromContext
	}
	return &placementExclusionAdapter{
		store: options.Store, coordinator: options.Coordinator,
		scope: options.Scope, tokenSource: options.TokenSource,
	}, nil
}

func (r *placementExclusionAdapter) Get(ctx context.Context, holder core.HolderRef) (core.PlacementExclusion, bool, error) {
	if err := placementExclusionContext(ctx); err != nil {
		return core.PlacementExclusion{}, false, err
	}
	if r == nil || r.store == nil || !holder.Exact() {
		return core.PlacementExclusion{}, false, ErrPlacementExclusionAdapterInvalid
	}
	value, found, err := r.store.GetPlacementExclusion(ctx, holder.Domain, holder.HolderID, holder.BootID)
	if err != nil || !found {
		return core.PlacementExclusion{}, found, err
	}
	state, err := mapPlacementExclusionState(holder, value)
	if err != nil {
		return core.PlacementExclusion{}, false, err
	}
	return state, true, nil
}

func (r *placementExclusionAdapter) Exclude(ctx context.Context, holder core.HolderRef,
	expected core.PlacementExclusionPrecondition) (core.PlacementExclusionResult, error) {
	if err := placementExclusionContext(ctx); err != nil {
		return core.PlacementExclusionResult{}, err
	}
	if r == nil || r.store == nil || !holder.Exact() {
		return core.PlacementExclusionResult{}, ErrPlacementExclusionAdapterInvalid
	}
	if expected.ExpectedRevision == 0 || expected.ExpectedExcluded || expected.OwnerPlanID == "" {
		return core.PlacementExclusionResult{}, fmt.Errorf("%w: exclusion precondition", ErrPlacementExclusionAdapterInvalid)
	}
	result, err := r.store.ExcludePlacement(ctx, holder.Domain, holder.HolderID, holder.BootID, routecore.PlacementExclusionPrecondition{
		ExpectedRevision: expected.ExpectedRevision,
		ExpectedExcluded: expected.ExpectedExcluded,
		OwnerPlanID:      expected.OwnerPlanID,
	})
	if err != nil {
		return core.PlacementExclusionResult{}, err
	}
	state, err := mapPlacementExclusionState(holder, result.State)
	if err != nil {
		return core.PlacementExclusionResult{}, err
	}
	if !state.Excluded || state.OwnerPlanID != expected.OwnerPlanID || state.Revision <= expected.ExpectedRevision {
		return core.PlacementExclusionResult{}, fmt.Errorf("%w: exclusion commit projection", ErrPlacementExclusionAdapterDrift)
	}
	return core.PlacementExclusionResult{Committed: result.Committed, State: state}, nil
}

func (r *placementExclusionAdapter) ReleaseForOwner(ctx context.Context, holder core.HolderRef,
	expected core.PlacementExclusionRelease) (core.PlacementExclusionResult, error) {
	if err := placementExclusionContext(ctx); err != nil {
		return core.PlacementExclusionResult{}, err
	}
	if r == nil || r.tokenSource == nil {
		return core.PlacementExclusionResult{Outcome: core.PlacementReleaseUnknown}, ErrPlacementExclusionAdapterToken
	}
	token, err := r.tokenSource(ctx)
	if err != nil {
		return core.PlacementExclusionResult{Outcome: core.PlacementReleaseUnknown}, err
	}
	return r.ReleaseForOwnerWithToken(ctx, token, holder, expected)
}

// ReleaseForOwnerWithToken is the adapter-local extension used by the
// EXCLUSIVE blocked-termination path.
func (r *placementExclusionAdapter) ReleaseForOwnerWithToken(ctx context.Context, token core.ControlOperationToken,
	holder core.HolderRef, expected core.PlacementExclusionRelease) (core.PlacementExclusionResult, error) {
	if err := placementExclusionContext(ctx); err != nil {
		return core.PlacementExclusionResult{}, err
	}
	if r == nil || r.store == nil || r.coordinator == nil || !holder.Exact() {
		return core.PlacementExclusionResult{Outcome: core.PlacementReleaseUnknown}, ErrPlacementExclusionAdapterInvalid
	}
	if expected.ExpectedRevision == 0 || expected.OwnerPlanID == "" {
		return core.PlacementExclusionResult{Outcome: core.PlacementReleaseUnknown},
			fmt.Errorf("%w: release precondition", ErrPlacementExclusionAdapterInvalid)
	}
	if !token.Issued() {
		return core.PlacementExclusionResult{Outcome: core.PlacementReleaseUnknown}, ErrPlacementExclusionAdapterToken
	}
	if err := r.coordinator.Verify(ctx, token, r.scope); err != nil {
		return core.PlacementExclusionResult{Outcome: core.PlacementReleaseUnknown},
			fmt.Errorf("%w: %v", ErrPlacementExclusionAdapterToken, err)
	}
	result, err := r.store.ReleasePlacementExclusionForOwner(ctx, token, holder.Domain, holder.HolderID, holder.BootID,
		routecore.PlacementExclusionRelease{OwnerPlanID: expected.OwnerPlanID, ExpectedRevision: expected.ExpectedRevision})
	if err != nil && result.Outcome == "" {
		return core.PlacementExclusionResult{Outcome: core.PlacementReleaseUnknown}, err
	}
	mapped, mapErr := mapPlacementReleaseResult(holder, result)
	if mapErr != nil {
		return mapped, mapErr
	}
	if err != nil {
		return mapped, err
	}
	return mapped, nil
}

func mapPlacementExclusionState(holder core.HolderRef, value routecore.PlacementExclusion) (core.PlacementExclusion, error) {
	if !holder.Exact() {
		return core.PlacementExclusion{}, ErrPlacementExclusionAdapterInvalid
	}
	if err := routecore.ValidatePlacementExclusion(value); err != nil ||
		value.Domain != holder.Domain || value.HolderID != holder.HolderID || value.BootID != holder.BootID {
		if err == nil {
			err = ErrPlacementExclusionAdapterDrift
		}
		return core.PlacementExclusion{}, fmt.Errorf("%w: %v", ErrPlacementExclusionAdapterDrift, err)
	}
	return core.PlacementExclusion{
		Holder: holder, Excluded: value.Excluded, Revision: value.Revision, OwnerPlanID: value.OwnerPlanID,
	}, nil
}

func mapPlacementReleaseResult(holder core.HolderRef, result routecore.PlacementExclusionResult) (core.PlacementExclusionResult, error) {
	outcome, ok := mapPlacementReleaseOutcome(result.Outcome)
	if !ok {
		return core.PlacementExclusionResult{Outcome: core.PlacementReleaseUnknown},
			fmt.Errorf("%w: unenumerated release outcome %q", ErrPlacementExclusionAdapterDrift, string(result.Outcome))
	}
	mapped := core.PlacementExclusionResult{Committed: result.Committed, Outcome: outcome}
	if result.State == (routecore.PlacementExclusion{}) {
		return mapped, nil
	}
	state, err := mapPlacementExclusionState(holder, result.State)
	if err != nil {
		return core.PlacementExclusionResult{Outcome: core.PlacementReleaseUnknown}, err
	}
	mapped.State = state
	return mapped, nil
}

func mapPlacementReleaseOutcome(value routecore.PlacementReleaseOutcome) (core.PlacementReleaseOutcome, bool) {
	switch value {
	case routecore.PlacementReleaseReleased:
		return core.PlacementReleaseReleased, true
	case routecore.PlacementReleaseAlreadyReleased:
		return core.PlacementReleaseAlreadyReleased, true
	case routecore.PlacementReleaseByBootRollover:
		return core.PlacementReleaseBootRollover, true
	case routecore.PlacementReleaseNotFound:
		return core.PlacementReleaseNotFound, true
	case routecore.PlacementReleaseOwnerMismatch:
		return core.PlacementReleaseOwnerMismatch, true
	case routecore.PlacementReleaseRevisionConflict:
		return core.PlacementReleaseRevisionConflict, true
	case routecore.PlacementReleaseMalformed:
		return core.PlacementReleaseMalformed, true
	default:
		return core.PlacementReleaseUnknown, false
	}
}

func placementExclusionContext(ctx context.Context) error {
	if ctx == nil {
		return ErrPlacementExclusionAdapterInvalid
	}
	return ctx.Err()
}

// TokenAwarePlacementExclusionPort is deliberately adapter-local. Core keeps
// its frozen PlacementExclusionPort signature; only the A3 blocked-release
// adapter may pass the inherited EXCLUSIVE token explicitly.
type TokenAwarePlacementExclusionPort interface {
	core.PlacementExclusionPort
	ReleaseForOwnerWithToken(context.Context, core.ControlOperationToken, core.HolderRef, core.PlacementExclusionRelease) (core.PlacementExclusionResult, error)
}

var _ core.PlacementExclusionPort = (*placementExclusionAdapter)(nil)
var _ TokenAwarePlacementExclusionPort = (*placementExclusionAdapter)(nil)
var _ PlacementExclusionStore = (*routecore.HolderRegistry)(nil)
var _ PlacementExclusionStore = (*redisroutecontrol.HolderStore)(nil)
