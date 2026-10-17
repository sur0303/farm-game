package routecontrol

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
)

var (
	ErrSnapshotBootstrapRequired = errors.New("routecontrol: snapshot bootstrap required")
	ErrSnapshotCASConflict       = errors.New("routecontrol: snapshot CAS conflict")
	ErrSnapshotStaleExpectation  = errors.New("routecontrol: stale snapshot expectation")
	ErrSnapshotOverflow          = errors.New("routecontrol: snapshot revision overflow")
	ErrSnapshotNotify            = errors.New("routecontrol: snapshot notification failed")
	ErrSnapshotCapacity          = errors.New("routecontrol: snapshot capacity exceeded")
	ErrSnapshotContextRequired   = errors.New("routecontrol: context is required")
)

var (
	// ErrSnapshotBootstrapExists is retained as a compatibility alias. Bootstrap
	// conflicts and ordinary CAS conflicts intentionally share one error type.
	ErrSnapshotBootstrapExists  = ErrSnapshotCASConflict
	ErrSnapshotRevisionConflict = ErrSnapshotCASConflict
	ErrSnapshotStale            = ErrSnapshotStaleExpectation
	ErrNotifyFailure            = ErrSnapshotNotify
)

// RouteSnapshotStore is the logical durable store contract. A false found
// result is reserved for a confirmed physical absence of the snapshot key.
//
// Mutating methods take a NestedControlOperationToken so a snapshot write
// cannot happen outside an acquired ControlMutationCoordinator gate. LoadCurrent
// stays token-free: it is a read that callers perform under a capture token.
type RouteSnapshotStore interface {
	LoadCurrent(context.Context) (StoredRouteSnapshot, bool, error)
	CompareAndSwap(context.Context, NestedControlOperationToken, uint64, actorrouting.RouteSnapshot) (SnapshotCASResult, error)
}

// RouteSnapshotBootstrapper is a separate capability from ordinary CAS.
// Bootstrap accepts only an empty physical store and revision 1.
//
// Bootstrap runs on the startup path, the moment two controllers are most
// likely to overlap, so it requires its own top-level token rather than an
// inherited one.
type RouteSnapshotBootstrapper interface {
	Bootstrap(context.Context, NestedControlOperationToken, actorrouting.RouteSnapshot) (SnapshotCASResult, error)
}

var (
	_ RouteSnapshotStore         = (*MemoryRouteSnapshotStore)(nil)
	_ RouteSnapshotBootstrapper  = (*MemoryRouteSnapshotStore)(nil)
	_ InitialAuthorityBatchStore = (*MemoryRouteSnapshotStore)(nil)
	_ AuthorityWaveBatchStore    = (*MemoryRouteSnapshotStore)(nil)
)

type SnapshotCASStatus string

const (
	SnapshotCASCommitted        SnapshotCASStatus = "COMMITTED"
	SnapshotCASAlreadyCommitted SnapshotCASStatus = "ALREADY_COMMITTED"
	SnapshotCASRevisionConflict SnapshotCASStatus = "REVISION_CONFLICT"
	SnapshotCASStaleExpectation SnapshotCASStatus = "STALE_EXPECTATION"
)

// Compatibility aliases make the status vocabulary readable at call sites
// without introducing a second status type.
const (
	CASCommitted             = SnapshotCASCommitted
	CASAlreadyCommitted      = SnapshotCASAlreadyCommitted
	CASRevisionConflict      = SnapshotCASRevisionConflict
	CASStaleExpectation      = SnapshotCASStaleExpectation
	SnapshotCommitted        = SnapshotCASCommitted
	SnapshotAlreadyCommitted = SnapshotCASAlreadyCommitted
)

type SnapshotCASResult struct {
	Status SnapshotCASStatus
	Stored StoredRouteSnapshot
}

func (r SnapshotCASResult) Committed() bool {
	return r.Status == SnapshotCASCommitted || r.Status == SnapshotCASAlreadyCommitted
}

// MemoryRouteSnapshotStore is a deterministic bounded reference store used by
// unit/concurrency tests and by callers that need the logical contract without
// Redis. It performs the same strict revision checks as the Redis adapter.
type MemoryRouteSnapshotStore struct {
	mu  sync.Mutex
	now func() int64

	current StoredRouteSnapshot
	found   bool
}

func NewMemoryRouteSnapshotStore() *MemoryRouteSnapshotStore {
	return &MemoryRouteSnapshotStore{now: func() int64 { return time.Now().UnixMilli() }}
}

func NewMemorySnapshotStore() *MemoryRouteSnapshotStore {
	return NewMemoryRouteSnapshotStore()
}

func (s *MemoryRouteSnapshotStore) SetNow(now func() int64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}

func (s *MemoryRouteSnapshotStore) LoadCurrent(ctx context.Context) (StoredRouteSnapshot, bool, error) {
	if err := requireContext(ctx); err != nil {
		return StoredRouteSnapshot{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return StoredRouteSnapshot{}, false, err
	}
	if s == nil {
		return StoredRouteSnapshot{}, false, ErrSnapshotCASConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.found {
		return StoredRouteSnapshot{}, false, nil
	}
	if err := validateStoredRouteSnapshot(s.current); err != nil {
		return StoredRouteSnapshot{}, true, err
	}
	return s.current.Clone(), true, nil
}

func (s *MemoryRouteSnapshotStore) Bootstrap(ctx context.Context, _ NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) (SnapshotCASResult, error) {
	if err := requireContext(ctx); err != nil {
		return SnapshotCASResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return SnapshotCASResult{}, err
	}
	stored, err := CanonicalizeRouteSnapshot(snapshot)
	if err != nil {
		return SnapshotCASResult{}, err
	}
	if stored.Revision != 1 {
		return SnapshotCASResult{Status: SnapshotCASRevisionConflict}, fmt.Errorf("%w: bootstrap revision=%d", ErrSnapshotCASConflict, stored.Revision)
	}
	if s == nil {
		return SnapshotCASResult{}, ErrSnapshotCASConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.found {
		if err := validateStoredRouteSnapshot(s.current); err != nil {
			return SnapshotCASResult{Status: SnapshotCASRevisionConflict}, fmt.Errorf("%w: existing envelope: %v", ErrSnapshotCASConflict, err)
		}
		if stored.EnvelopeEqual(s.current) {
			return SnapshotCASResult{Status: SnapshotCASAlreadyCommitted, Stored: s.current.Clone()}, nil
		}
		return SnapshotCASResult{Status: SnapshotCASRevisionConflict, Stored: s.current.Clone()}, ErrSnapshotCASConflict
	}
	if err := ctx.Err(); err != nil {
		return SnapshotCASResult{}, err
	}
	now := s.nowMs()
	if err := ctx.Err(); err != nil {
		return SnapshotCASResult{}, err
	}
	stored.CommittedAtMs = now
	s.current = stored.Clone()
	s.found = true
	return SnapshotCASResult{Status: SnapshotCASCommitted, Stored: stored.Clone()}, nil
}

func (s *MemoryRouteSnapshotStore) CompareAndSwapInitialAuthorityBatch(ctx context.Context, token NestedControlOperationToken, capability InitialAuthorityBatchCapability) (SnapshotCASResult, error) {
	if err := requireContext(ctx); err != nil {
		return SnapshotCASResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return SnapshotCASResult{}, err
	}
	if !token.Issued() {
		return SnapshotCASResult{}, ErrInitialAuthorityBatchTokenRequired
	}
	expected, nextSnapshot, records, now, err := InitialAuthorityBatchValues(capability)
	if err != nil {
		return SnapshotCASResult{}, err
	}
	if err := ValidateInitialAuthorityBatchAt(expected, nextSnapshot, records, "", now); err != nil {
		return SnapshotCASResult{}, err
	}
	next, err := CanonicalizeRouteSnapshot(nextSnapshot)
	if err != nil {
		return SnapshotCASResult{}, err
	}
	if s == nil {
		return SnapshotCASResult{}, ErrSnapshotCASConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.found {
		return SnapshotCASResult{Status: SnapshotCASRevisionConflict}, ErrSnapshotBootstrapRequired
	}
	current := s.current
	if err := validateStoredRouteSnapshot(current); err != nil {
		return SnapshotCASResult{Status: SnapshotCASRevisionConflict}, err
	}
	if !storedSnapshotExactEqual(current, expected) {
		return SnapshotCASResult{Status: SnapshotCASStaleExpectation, Stored: current.Clone()}, nil
	}
	if err := ctx.Err(); err != nil {
		return SnapshotCASResult{}, err
	}
	nowMs := s.nowMs()
	next.CommittedAtMs = nowMs
	s.current = next.Clone()
	return SnapshotCASResult{Status: SnapshotCASCommitted, Stored: next.Clone()}, nil
}

func (s *MemoryRouteSnapshotStore) CompareAndSwapAuthorityWaveBatch(ctx context.Context, token NestedControlOperationToken, capability AuthorityWaveBatchCapability) (SnapshotCASResult, error) {
	if err := requireContext(ctx); err != nil {
		return SnapshotCASResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return SnapshotCASResult{}, err
	}
	if !token.Issued() {
		return SnapshotCASResult{}, ErrAuthorityWaveBatchTokenRequired
	}
	expected, nextSnapshot, steps, members, _, err := AuthorityWaveBatchValues(capability)
	if err != nil {
		return SnapshotCASResult{}, err
	}
	if err := ValidateAuthorityWaveBatch(expected, steps, members); err != nil {
		return SnapshotCASResult{}, err
	}
	next, err := CanonicalizeRouteSnapshot(nextSnapshot)
	if err != nil {
		return SnapshotCASResult{}, err
	}
	if s == nil {
		return SnapshotCASResult{}, ErrSnapshotCASConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.found {
		return SnapshotCASResult{Status: SnapshotCASRevisionConflict}, ErrSnapshotBootstrapRequired
	}
	current := s.current
	if err := validateStoredRouteSnapshot(current); err != nil {
		return SnapshotCASResult{Status: SnapshotCASRevisionConflict}, err
	}
	if next.EnvelopeEqual(current) {
		return SnapshotCASResult{Status: SnapshotCASAlreadyCommitted, Stored: current.Clone()}, nil
	}
	if !storedSnapshotExactEqual(current, expected) {
		return SnapshotCASResult{Status: SnapshotCASStaleExpectation, Stored: current.Clone()}, nil
	}
	if err := ctx.Err(); err != nil {
		return SnapshotCASResult{}, err
	}
	next.CommittedAtMs = s.nowMs()
	s.current = next.Clone()
	return SnapshotCASResult{Status: SnapshotCASCommitted, Stored: next.Clone()}, nil
}

func storedSnapshotExactEqual(left, right StoredRouteSnapshot) bool {
	return left.EnvelopeEqual(right) && left.CommittedAtMs == right.CommittedAtMs
}

func (s *MemoryRouteSnapshotStore) CompareAndSwap(ctx context.Context, _ NestedControlOperationToken, expectedRevision uint64, snapshot actorrouting.RouteSnapshot) (SnapshotCASResult, error) {
	if err := requireContext(ctx); err != nil {
		return SnapshotCASResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return SnapshotCASResult{}, err
	}
	next, err := CanonicalizeRouteSnapshot(snapshot)
	if err != nil {
		return SnapshotCASResult{}, err
	}
	if s == nil {
		return SnapshotCASResult{}, ErrSnapshotCASConflict
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.found {
		if expectedRevision == 0 && next.Revision == 1 {
			return SnapshotCASResult{Status: SnapshotCASRevisionConflict}, ErrSnapshotBootstrapRequired
		}
		return SnapshotCASResult{Status: SnapshotCASRevisionConflict}, fmt.Errorf("%w: empty store expected=%d next=%d", ErrSnapshotCASConflict, expectedRevision, next.Revision)
	}
	current := s.current
	if next.Revision == current.Revision && next.EnvelopeEqual(current) {
		return SnapshotCASResult{Status: SnapshotCASAlreadyCommitted, Stored: current.Clone()}, nil
	}
	if next.Revision == current.Revision {
		return SnapshotCASResult{Status: SnapshotCASRevisionConflict, Stored: current.Clone()}, nil
	}
	if expectedRevision != current.Revision {
		return SnapshotCASResult{Status: SnapshotCASStaleExpectation, Stored: current.Clone()}, nil
	}
	if current.Revision == math.MaxUint64 {
		return SnapshotCASResult{Status: SnapshotCASRevisionConflict, Stored: current.Clone()}, ErrSnapshotOverflow
	}
	if next.Revision != current.Revision+1 {
		return SnapshotCASResult{Status: SnapshotCASRevisionConflict, Stored: current.Clone()}, nil
	}
	if err := ctx.Err(); err != nil {
		return SnapshotCASResult{}, err
	}
	now := s.nowMs()
	if err := ctx.Err(); err != nil {
		return SnapshotCASResult{}, err
	}
	next.CommittedAtMs = now
	s.current = next.Clone()
	return SnapshotCASResult{Status: SnapshotCASCommitted, Stored: next.Clone()}, nil
}

func (s *MemoryRouteSnapshotStore) nowMs() int64 {
	if s.now == nil {
		return time.Now().UnixMilli()
	}
	now := s.now()
	if now < 0 {
		return 0
	}
	return now
}

func requireContext(ctx context.Context) error {
	if ctx == nil {
		return ErrSnapshotContextRequired
	}
	return nil
}

// SnapshotStore is the compatibility name for the logical in-memory
// reference implementation. The production Redis implementation lives in
// internal/storage/redis/routecontrol.
type SnapshotStore = MemoryRouteSnapshotStore
