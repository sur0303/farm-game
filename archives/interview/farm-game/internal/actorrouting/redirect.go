package actorrouting

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// Redirect reason codes (process-local; not wire enums).
const (
	RedirectReasonStaleRoute  = "STALE_ROUTE"
	RedirectReasonHolderDrain = "HOLDER_DRAIN"
	RedirectReasonNotHolding  = "NOT_HOLDING"
)

// RouteRedirect is the in-process redirect signal (Batch 2.5).
// It is not a proto / RPC field; callers translate trusted internal errors into this shape.
//
// Revision rule: GlobalRevision (and ShardRevision when set) must be strictly
// greater than the locally known values, otherwise ApplyRedirect rejects with
// ErrStaleRedirect and leaves the table unchanged.
//
// No proxy: receiving NOT_HOLDING / STALE_ROUTE must only update the local route
// view; the caller retries Resolve→new Holder with the original operation_id.
// This package intentionally exposes no Forward(ctx, endpoint, req) API.
type RouteRedirect struct {
	Domain               RouteDomain
	ActorID              uint64 // optional; 0 = shard-level
	ShardID              uint32
	GlobalRevision       uint64
	ShardRevision        uint64
	ExpectedHolderID     string
	ExpectedHolderBootID string
	ExpectedWriterEpoch  uint64
	ExpectedLeaseEpoch   uint64
	LeaseUntilMs         int64
	NewHolderIndex       int
	Reason               string

	// Snapshot, when non-nil, is installed wholesale via Replace after revision checks.
	// Prefer this when the new Holder is not already in the local table.
	Snapshot *RouteSnapshot
}

// RedirectError is returned by attempt functions so RetryingResolver can
// ApplyRedirect and retry with the same operation_id.
type RedirectError struct {
	Redirect RouteRedirect
}

func (e *RedirectError) Error() string {
	if e == nil {
		return "actor route: redirect"
	}
	reason := e.Redirect.Reason
	if reason == "" {
		reason = "redirect"
	}
	return fmt.Sprintf("actor route: %s domain=%s shard=%d rev=%d",
		reason, e.Redirect.Domain, e.Redirect.ShardID, e.Redirect.GlobalRevision)
}

// AsRedirect unwraps a RedirectError from err.
func AsRedirect(err error) (*RedirectError, bool) {
	var re *RedirectError
	if errors.As(err, &re) && re != nil {
		return re, true
	}
	return nil, false
}

// RedirectApplier installs RouteRedirect into a StaticResolver. A redirect
// without a whole snapshot must carry the complete target generation; a
// partial patch is never allowed to preserve stale lease fields.
type RedirectApplier struct {
	resolver  *StaticResolver
	snapshots *SnapshotApplier // optional collaboration
	metrics   atomic.Pointer[Metrics]

	StaleRejected atomic.Uint64
	Applied       atomic.Uint64
}

// NewRedirectApplier wires a StaticResolver. resolver is required.
func NewRedirectApplier(resolver *StaticResolver) (*RedirectApplier, error) {
	if resolver == nil {
		return nil, fmt.Errorf("%w: resolver required", ErrInvalidRedirect)
	}
	return &RedirectApplier{resolver: resolver}, nil
}

// SetSnapshotApplier optionally syncs last GlobalRevision after a successful redirect.
func (a *RedirectApplier) SetSnapshotApplier(sa *SnapshotApplier) {
	if a == nil {
		return
	}
	a.snapshots = sa
}

// SetMetrics attaches Batch 2.6 redirect counters. Nil clears.
func (a *RedirectApplier) SetMetrics(m *Metrics) {
	if a == nil {
		return
	}
	a.metrics.Store(m)
}

// ApplyRedirect validates revision freshness then replaces the local table.
// When a SnapshotApplier is attached, gap handling is delegated to its full
// reload path instead of marking the redirect revision applied directly.
func (a *RedirectApplier) ApplyRedirect(ctx context.Context, redirect RouteRedirect) error {
	if a == nil || a.resolver == nil {
		return ErrNoAssignment
	}
	if ctx == nil {
		return ErrContextRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateRedirect(redirect); err != nil {
		// A holder may only know that its route is stale and lack a complete
		// generation. Only an applier bound to the exact dynamic control-plane
		// provider may reload the authoritative full snapshot; wrappers and
		// ordinary providers fail closed. Never synthesize a patch carrying the
		// old lease fields.
		if a.snapshots != nil && a.snapshots.trustedProvider && redirect.Snapshot == nil && redirect.GlobalRevision > 0 && (redirect.Domain == DomainPlayer || redirect.Domain == DomainFarm) {
			if reloadErr := a.snapshots.Bootstrap(ctx); reloadErr != nil {
				return fmt.Errorf("%w: incomplete redirect reload: %v", ErrRedirectReloadRequired, reloadErr)
			}
			return nil
		}
		return err
	}

	current := a.resolver.Snapshot()
	if redirect.GlobalRevision <= current.GlobalRevision {
		a.StaleRejected.Add(1)
		if m := a.metrics.Load(); m != nil {
			m.ObserveRedirect(redirect.Domain, RedirectReasonStaleRejected)
		}
		return fmt.Errorf("%w: global_revision %d <= local %d",
			ErrStaleRedirect, redirect.GlobalRevision, current.GlobalRevision)
	}

	ds := current.domain(redirect.Domain)
	if uint32(len(ds.Routes)) > 0 && redirect.ShardID < uint32(len(ds.Routes)) {
		localShardRev := ds.Routes[redirect.ShardID].ShardRevision
		if redirect.ShardRevision > 0 && redirect.ShardRevision <= localShardRev {
			a.StaleRejected.Add(1)
			if m := a.metrics.Load(); m != nil {
				m.ObserveRedirect(redirect.Domain, RedirectReasonStaleRejected)
			}
			return fmt.Errorf("%w: shard_revision %d <= local %d",
				ErrStaleRedirect, redirect.ShardRevision, localShardRev)
		}
	}

	next, err := buildRedirectSnapshot(current, redirect)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.snapshots != nil {
		if a.snapshots.LastGlobalRevision() == 0 {
			a.snapshots.NoteAppliedRevision(current.GlobalRevision)
		}
		if err := a.snapshots.Apply(ctx, next); err != nil {
			return err
		}
	} else {
		if redirect.GlobalRevision != current.GlobalRevision+1 {
			return fmt.Errorf("%w: current=%d redirect=%d", ErrRedirectReloadRequired, current.GlobalRevision, redirect.GlobalRevision)
		}
		if err := a.resolver.ReplaceContext(ctx, next); err != nil {
			return err
		}
	}
	// Replacement is the linearization point. A cancellation after it must not
	// turn an installed table into an apparent failed operation with stale
	// redirect/applier revision bookkeeping.
	installed := a.resolver.Snapshot()
	a.Applied.Add(1)
	reason := redirect.Reason
	if reason == "" {
		reason = RedirectReasonNotHolding
	}
	if m := a.metrics.Load(); m != nil {
		m.ObserveRedirect(redirect.Domain, reason)
		// Replace also Notes when resolver.SetMetrics was used; keep applier-only wiring correct.
		m.NoteSnapshot(installed.GlobalRevision, time.Now())
	}
	return nil
}

func validateRedirect(r RouteRedirect) error {
	if r.Domain != DomainPlayer && r.Domain != DomainFarm {
		return fmt.Errorf("%w: domain", ErrInvalidRedirect)
	}
	if r.GlobalRevision == 0 {
		return fmt.Errorf("%w: global_revision required", ErrInvalidRedirect)
	}
	if r.Snapshot == nil {
		if r.ShardRevision == 0 || r.ExpectedHolderID == "" || r.ExpectedHolderBootID == "" || r.ExpectedWriterEpoch == 0 || r.ExpectedLeaseEpoch == 0 || r.LeaseUntilMs <= 0 {
			return fmt.Errorf("%w: complete target generation required", ErrInvalidRedirect)
		}
		if r.NewHolderIndex < 0 {
			return fmt.Errorf("%w: new_holder_index", ErrInvalidRedirect)
		}
	}
	return nil
}

func buildRedirectSnapshot(current RouteSnapshot, redirect RouteRedirect) (RouteSnapshot, error) {
	if redirect.Snapshot != nil {
		snap := *redirect.Snapshot
		if snap.GlobalRevision == 0 {
			snap.GlobalRevision = redirect.GlobalRevision
		}
		if snap.GlobalRevision != redirect.GlobalRevision {
			return RouteSnapshot{}, fmt.Errorf("%w: snapshot global_revision mismatch", ErrInvalidRedirect)
		}
		return snap, nil
	}

	next := cloneRouteSnapshot(current)
	next.GlobalRevision = redirect.GlobalRevision
	var routes *[]ShardRoute
	var shardCount uint32
	switch redirect.Domain {
	case DomainPlayer:
		routes = &next.Player.Routes
		shardCount = next.Player.ShardCount
	case DomainFarm:
		routes = &next.Farm.Routes
		shardCount = next.Farm.ShardCount
	}
	if redirect.ShardID >= shardCount || routes == nil || int(redirect.ShardID) >= len(*routes) {
		return RouteSnapshot{}, fmt.Errorf("%w: shard_id %d out of range", ErrInvalidRedirect, redirect.ShardID)
	}
	if redirect.NewHolderIndex >= len(next.Holders) {
		return RouteSnapshot{}, fmt.Errorf("%w: new_holder_index %d out of range", ErrInvalidRedirect, redirect.NewHolderIndex)
	}
	holder := next.Holders[redirect.NewHolderIndex]
	if holder.InstanceID != redirect.ExpectedHolderID || holder.BootID != redirect.ExpectedHolderBootID {
		return RouteSnapshot{}, fmt.Errorf("%w: redirect holder identity is not authoritative", ErrInvalidRedirect)
	}
	route := (*routes)[redirect.ShardID]
	route.HolderIndex = redirect.NewHolderIndex
	route.ShardRevision = redirect.ShardRevision
	route.ExpectedWriterEpoch = redirect.ExpectedWriterEpoch
	route.ExpectedLeaseEpoch = redirect.ExpectedLeaseEpoch
	route.LeaseUntilMs = redirect.LeaseUntilMs
	(*routes)[redirect.ShardID] = route
	return next, nil
}

func cloneRouteSnapshot(s RouteSnapshot) RouteSnapshot {
	out := s
	out.Holders = append([]HolderRef(nil), s.Holders...)
	out.Player.Routes = append([]ShardRoute(nil), s.Player.Routes...)
	out.Farm.Routes = append([]ShardRoute(nil), s.Farm.Routes...)
	out.FarmWorker.Routes = append([]ShardRoute(nil), s.FarmWorker.Routes...)
	return out
}

// AttemptFunc is one Resolve→invoke attempt. operationID is an invariant:
// RetryingResolver always passes the same string across redirects.
type AttemptFunc func(ctx context.Context, assignment Assignment, operationID string) error

// RetryingResolver bounds redirect retries while preserving operation_id.
// Retries are always Resolve → AttemptFunc; there is no proxy/forward path.
type RetryingResolver struct {
	Resolver     Resolver
	Applier      *RedirectApplier
	MaxRedirects int // default 3; max number of ApplyRedirect+retry cycles
}

const defaultMaxRedirects = 3

// Invoke resolves addr, calls fn with the fixed operationID, and on RedirectError
// applies the redirect then re-resolves. Exceeding MaxRedirects → ErrTooManyRedirects.
func (r *RetryingResolver) Invoke(ctx context.Context, addr Address, operationID string, fn AttemptFunc) error {
	if r == nil || r.Resolver == nil {
		return ErrNoAssignment
	}
	if fn == nil {
		return fmt.Errorf("%w: attempt func required", ErrInvalidRedirect)
	}
	if operationID == "" {
		return fmt.Errorf("%w: operation_id required", ErrInvalidRedirect)
	}
	max := r.MaxRedirects
	if max <= 0 {
		max = defaultMaxRedirects
	}

	redirects := 0
	for {
		assignment, err := r.Resolver.Resolve(ctx, addr)
		if err != nil {
			return err
		}
		err = fn(ctx, assignment, operationID)
		re, ok := AsRedirect(err)
		if !ok {
			return err
		}
		if redirects >= max {
			return fmt.Errorf("%w: limit %d", ErrTooManyRedirects, max)
		}
		if r.Applier == nil {
			return fmt.Errorf("%w: applier required for redirect", ErrInvalidRedirect)
		}
		if applyErr := r.Applier.ApplyRedirect(ctx, re.Redirect); applyErr != nil {
			return applyErr
		}
		redirects++
	}
}

// HandleLocalMiss is the old-Holder path for "this process no longer holds the shard":
// update the local route view and return RedirectError so the caller retries with
// the original operation_id. It never forwards/proxies the request to the new Holder.
func HandleLocalMiss(ctx context.Context, applier *RedirectApplier, redirect RouteRedirect) error {
	if applier == nil {
		return fmt.Errorf("%w: applier required", ErrInvalidRedirect)
	}
	if redirect.Reason == "" {
		redirect.Reason = RedirectReasonNotHolding
	}
	if err := applier.ApplyRedirect(ctx, redirect); err != nil {
		return err
	}
	return &RedirectError{Redirect: redirect}
}
