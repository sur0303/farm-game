package scaleplan

import (
	"context"
	"fmt"
	"strconv"
	"sync"
)

// MaxLuaSafeInteger bounds every integer that a backend Lua script compares or
// increments (§6.13). Redis 7.x runs Lua 5.1, whose only numeric type is a
// double, so anything above 2^53-1 stops being exactly representable. Reaching
// the bound is a permanent fail-closed, never a wrap.
const MaxLuaSafeInteger uint64 = 9007199254740991

// maxCanonicalFenceDigits admits every value up to MaxLuaSafeInteger (16
// digits) and rejects longer strings before they reach strconv.
const maxCanonicalFenceDigits = 16

// MutationScope is the serialization domain of a plan mutation lease. The
// granularity is realm-global rather than per-plan, which is forced rather
// than chosen: Submit enters the barrier before a PlanID exists (§5.3).
type MutationScope struct {
	Namespace string
	Realm     string
}

func (s MutationScope) Key() string { return s.Namespace + "\x1f" + s.Realm }

func (s MutationScope) Validate() error {
	if err := validateOpaque("mutation scope namespace", s.Namespace, MaxNamespaceBytes); err != nil {
		return err
	}
	return validateOpaque("mutation scope realm", s.Realm, MaxRealmBytes)
}

// MutationLeaseOwner identifies one attempt by one controller generation. The
// nonce is per-acquire, so a restarted controller never presents the identity
// of its former self.
type MutationLeaseOwner struct {
	ControllerID     string
	ControllerBootID string
	OperationNonce   string
}

func (o MutationLeaseOwner) Validate() error {
	if err := validateOpaque("controller id", o.ControllerID, MaxOpaqueIDBytes); err != nil {
		return err
	}
	if err := validateOpaque("controller boot id", o.ControllerBootID, MaxOpaqueIDBytes); err != nil {
		return err
	}
	return validateOpaque("operation nonce", o.OperationNonce, MaxOpaqueIDBytes)
}

func (o MutationLeaseOwner) equal(other MutationLeaseOwner) bool { return o == other }

// BackendMutationLease is the wire shape a DurableRepositoryBackend returns.
// It is a plain struct because backends live in other packages and must be
// able to construct it. RepositoryID is a stable, non-secret identity for the
// physical repository/keyspace that allocated the fence. It prevents equal
// scope/owner/fence tuples produced by two independent stores from being
// mistaken for the same write authority.
type BackendMutationLease struct {
	RepositoryID string
	Scope        MutationScope
	Owner        MutationLeaseOwner
	Fence        string
	ExpiresAtMs  int64
}

func (l BackendMutationLease) Validate() error {
	if err := validateOpaque("mutation lease repository id", l.RepositoryID, MaxOpaqueIDBytes); err != nil {
		return err
	}
	if err := l.Scope.Validate(); err != nil {
		return err
	}
	if err := l.Owner.Validate(); err != nil {
		return err
	}
	if err := validateCanonicalFence(l.Fence); err != nil {
		return err
	}
	if l.ExpiresAtMs <= 0 {
		return fmt.Errorf("%w: mutation lease expiry", ErrInvalidPlan)
	}
	return nil
}

// DurableMutationLease is the opaque value core hands to callers. Its fields
// are private so that no caller outside this package can assemble one and
// present it as proof of having entered the barrier.
type DurableMutationLease struct {
	repositoryID string
	scope        MutationScope
	owner        MutationLeaseOwner
	fence        string
	expiresAtMs  int64
}

func (l DurableMutationLease) RepositoryID() string      { return l.repositoryID }
func (l DurableMutationLease) Scope() MutationScope      { return l.scope }
func (l DurableMutationLease) Owner() MutationLeaseOwner { return l.owner }
func (l DurableMutationLease) Fence() string             { return l.fence }
func (l DurableMutationLease) ExpiresAtMs() int64        { return l.expiresAtMs }

// Held reports whether this value came from an acquire. A zero lease is never
// held, which is what makes the zero value useless as a credential.
func (l DurableMutationLease) Held() bool {
	return l.repositoryID != "" && l.fence != "" && l.owner.OperationNonce != ""
}

func (l DurableMutationLease) backend() BackendMutationLease {
	return BackendMutationLease{RepositoryID: l.repositoryID, Scope: l.scope, Owner: l.owner, Fence: l.fence, ExpiresAtMs: l.expiresAtMs}
}

func adoptBackendLease(lease BackendMutationLease) (DurableMutationLease, error) {
	if err := lease.Validate(); err != nil {
		return DurableMutationLease{}, err
	}
	return DurableMutationLease{repositoryID: lease.RepositoryID, scope: lease.Scope, owner: lease.Owner, fence: lease.Fence,
		expiresAtMs: lease.ExpiresAtMs}, nil
}

// sameWriteAuthority compares the stable authorization tuple. ExpiresAtMs is
// deliberately excluded: renewal may extend the deadline without changing
// which repository, owner and fence are allowed to write.
func (l DurableMutationLease) sameWriteAuthority(other DurableMutationLease) bool {
	return l.repositoryID == other.repositoryID && l.scope == other.scope &&
		l.owner == other.owner && l.fence == other.fence
}

// PlanMutationGuard replaces the bare release func the in-memory barrier used
// to return. The context is the point of the change: losing the lease must
// cancel the work the lease was protecting, and a release func cannot express
// that.
type PlanMutationGuard interface {
	// Context is derived from the caller's and is cancelled when renewal is
	// lost. Every protected read, effect and commit must use it.
	Context() context.Context
	Lease() DurableMutationLease
	// Release joins the renewer before comparing owner and fence, so a
	// returned nil means no renewal can still be in flight.
	Release(context.Context) error
}

// releaseGuard discards the release error deliberately. The seven entry points
// discarded it before this seam existed, and preserving that is what makes the
// before/after equivalence provable; a lease that fails to release is already
// reported through the renewer's cancellation of the guard context.
func releaseGuard(guard PlanMutationGuard, ctx context.Context) {
	if guard == nil {
		return
	}
	_ = guard.Release(ctx)
}

// validateCanonicalFence enforces the §6.13 digit-string syntax before any
// numeric conversion: unsigned, no leading zero, at most 16 digits.
func validateCanonicalFence(fence string) error {
	if fence == "" || len(fence) > maxCanonicalFenceDigits {
		return fmt.Errorf("%w: fence malformed", ErrInvalidPlan)
	}
	if fence[0] == '0' && len(fence) > 1 {
		return fmt.Errorf("%w: fence malformed", ErrInvalidPlan)
	}
	for i := 0; i < len(fence); i++ {
		if fence[i] < '0' || fence[i] > '9' {
			return fmt.Errorf("%w: fence malformed", ErrInvalidPlan)
		}
	}
	value, err := strconv.ParseUint(fence, 10, 64)
	if err != nil || value > MaxLuaSafeInteger {
		return fmt.Errorf("%w: fence exhausted", ErrInvalidPlan)
	}
	return nil
}

// memoryMutationGuard is the no-renew guard used by the in-memory repository.
// It still derives a cancellable context so that the release-cancels-context
// half of the contract is exercised without a backend.
type memoryMutationGuard struct {
	ctx     context.Context
	cancel  context.CancelFunc
	lease   DurableMutationLease
	release func()
	once    sync.Once
}

func (g *memoryMutationGuard) Context() context.Context { return g.ctx }
func (g *memoryMutationGuard) Lease() DurableMutationLease {
	return g.lease
}

func (g *memoryMutationGuard) Release(context.Context) error {
	g.once.Do(func() {
		g.cancel()
		g.release()
	})
	return nil
}
