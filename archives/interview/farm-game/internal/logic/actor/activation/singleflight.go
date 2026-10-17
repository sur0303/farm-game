package activation

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrInvalidLoadIdentity = errors.New("activation: invalid load identity")
	ErrTooManyLoadWaiters  = errors.New("activation: too many load waiters")
	ErrStaleLoadFuture     = errors.New("activation: stale load future")
	ErrLoadFutureOwner     = errors.New("activation: load owner cannot wait on itself")
)

// LoadIdentity is generated exactly once by the owner of an activation
// flight. Every waiter observes the same identities, including while the
// underlying LoadAndClaim is in CLAIM_UNKNOWN recovery.
type LoadIdentity struct {
	ActivationGeneration uint64
	ClaimOperationID     string
	NewWriterClaimID     string
}

type LoadFutureOptions struct {
	MaxWaitersPerActor int
	MaxTotalWaiters    int
}

func (o LoadFutureOptions) withDefaults() LoadFutureOptions {
	if o.MaxWaitersPerActor <= 0 {
		o.MaxWaitersPerActor = 256
	}
	if o.MaxTotalWaiters <= 0 {
		o.MaxTotalWaiters = 4096
	}
	return o
}

type loadFlight struct {
	identity  LoadIdentity
	done      chan struct{}
	completed bool
	err       error
	waiters   int
}

// LoadFutureGroup only merges activation/loading work. It never merges the
// user commands that arrive behind the load; after activation, each command
// must still enter the Actor mailbox independently.
type LoadFutureGroup struct {
	mu             sync.Mutex
	options        LoadFutureOptions
	flights        map[uint64]*loadFlight
	totalWaiters   int
	joinedTotal    uint64
	startedTotal   uint64
	completedTotal uint64
}

type LoadFutureStats struct {
	ActiveFlights  int
	CurrentWaiters int
	JoinedTotal    uint64
	StartedTotal   uint64
	CompletedTotal uint64
}

// LoadHandle represents either the unique owner or one joined waiter. A
// canceled waiter only detaches itself; it never cancels the shared load.
type LoadHandle struct {
	group   *LoadFutureGroup
	key     uint64
	flight  *loadFlight
	owner   bool
	release sync.Once
}

func NewLoadFutureGroup(options LoadFutureOptions) *LoadFutureGroup {
	return &LoadFutureGroup{
		options: options.withDefaults(),
		flights: make(map[uint64]*loadFlight),
	}
}

// BeginOrJoin creates one load flight per Actor key. identityFactory is called
// only for the owner; joiners neither generate C3 nor replace the original
// claim_operation_id/new_writer_claim_id.
func (g *LoadFutureGroup) BeginOrJoin(key uint64, identityFactory func() (LoadIdentity, error)) (*LoadHandle, bool, error) {
	if g == nil || key == 0 {
		return nil, false, ErrInvalidLoadIdentity
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if current := g.flights[key]; current != nil {
		if current.waiters >= g.options.MaxWaitersPerActor || g.totalWaiters >= g.options.MaxTotalWaiters {
			return nil, false, ErrTooManyLoadWaiters
		}
		current.waiters++
		g.totalWaiters++
		g.joinedTotal++
		return &LoadHandle{group: g, key: key, flight: current}, false, nil
	}
	if identityFactory == nil {
		return nil, false, ErrInvalidLoadIdentity
	}
	identity, err := identityFactory()
	if err != nil {
		return nil, false, err
	}
	if identity.ActivationGeneration == 0 || identity.ClaimOperationID == "" || identity.NewWriterClaimID == "" {
		return nil, false, ErrInvalidLoadIdentity
	}
	flight := &loadFlight{identity: identity, done: make(chan struct{})}
	g.flights[key] = flight
	g.startedTotal++
	return &LoadHandle{group: g, key: key, flight: flight, owner: true}, true, nil
}

func (h *LoadHandle) Identity() LoadIdentity {
	if h == nil || h.flight == nil {
		return LoadIdentity{}
	}
	return h.flight.identity
}

func (h *LoadHandle) IsOwner() bool {
	return h != nil && h.owner
}

// Complete is called once by the owner after the load or its UNKNOWN recovery
// reaches a stable answer. The error is shared by all current waiters.
func (h *LoadHandle) Complete(err error) error {
	if h == nil || h.group == nil || h.flight == nil || !h.owner {
		return ErrStaleLoadFuture
	}
	g := h.group
	g.mu.Lock()
	defer g.mu.Unlock()
	if current := g.flights[h.key]; current != h.flight || current.completed {
		return ErrStaleLoadFuture
	}
	h.flight.err = err
	h.flight.completed = true
	delete(g.flights, h.key)
	g.completedTotal++
	close(h.flight.done)
	return nil
}

// Wait waits for the owner's stable result. Deadline/cancellation releases
// only this waiter and leaves the shared flight and other waiters untouched.
func (h *LoadHandle) Wait(ctx context.Context) error {
	if h == nil || h.group == nil || h.flight == nil {
		return ErrStaleLoadFuture
	}
	if h.owner {
		return ErrLoadFutureOwner
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrStaleLoadFuture)
	}
	select {
	case <-h.flight.done:
		h.releaseWaiter()
		return h.flight.err
	case <-ctx.Done():
		h.releaseWaiter()
		return ctx.Err()
	}
}

func (h *LoadHandle) releaseWaiter() {
	h.release.Do(func() {
		g := h.group
		g.mu.Lock()
		defer g.mu.Unlock()
		if h.flight.waiters > 0 {
			h.flight.waiters--
		}
		if g.totalWaiters > 0 {
			g.totalWaiters--
		}
	})
}

func (g *LoadFutureGroup) Stats() LoadFutureStats {
	if g == nil {
		return LoadFutureStats{}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return LoadFutureStats{
		ActiveFlights:  len(g.flights),
		CurrentWaiters: g.totalWaiters,
		JoinedTotal:    g.joinedTotal,
		StartedTotal:   g.startedTotal,
		CompletedTotal: g.completedTotal,
	}
}
