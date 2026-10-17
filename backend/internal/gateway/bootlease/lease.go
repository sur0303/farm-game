// Package bootlease owns the short-lived liveness lease for one farm-gate
// process boot. It is deliberately separate from per-player SessionRoute
// leases: one boot lease proves that the process which owns local connections
// is still able to reach the shared lease authority.
package bootlease

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var ErrInvalidFence = errors.New("gate boot lease: invalid fence")

// Fence identifies exactly one process lifetime. A restarted process must use
// a new BootID even when it reuses the same GatewayID.
type Fence struct {
	GatewayID string
	BootID    string
}

func (f Fence) Valid() bool { return f.GatewayID != "" && f.BootID != "" }

// Record is the authority result trusted by the local Gate. ExpiresAt is a
// local admission deadline returned only after the Store renew succeeded.
type Record struct {
	Fence     Fence
	ExpiresAt time.Time
}

// Store is intentionally small so the lease can be backed by Redis in shared
// environments and by memory in single-process development.
type Store interface {
	Renew(context.Context, Fence, time.Duration) (Record, error)
	Alive(context.Context, Fence) (bool, error)
}

// Lease tracks the last successfully renewed local trust deadline. A failed
// renew never extends that deadline.
type Lease struct {
	store Store
	fence Fence
	ttl   time.Duration
	now   func() time.Time

	mu             sync.RWMutex
	trustedUntil   time.Time
	renewAttempts  atomic.Uint64
	renewSuccesses atomic.Uint64
	renewFailures  atomic.Uint64
}

func New(store Store, fence Fence, ttl time.Duration) (*Lease, error) {
	if store == nil || !fence.Valid() || ttl <= 0 {
		return nil, ErrInvalidFence
	}
	return &Lease{store: store, fence: fence, ttl: ttl, now: time.Now}, nil
}

func (l *Lease) Fence() Fence       { return l.fence }
func (l *Lease) TTL() time.Duration { return l.ttl }

// Renew performs one authority round trip. The local trust deadline advances
// only when the authority confirms this exact boot fence.
func (l *Lease) Renew(ctx context.Context) error {
	l.renewAttempts.Add(1)
	record, err := l.store.Renew(ctx, l.fence, l.ttl)
	if err != nil {
		l.renewFailures.Add(1)
		return err
	}
	if record.Fence != l.fence || record.ExpiresAt.IsZero() {
		l.renewFailures.Add(1)
		return fmt.Errorf("%w: store returned mismatched record", ErrInvalidFence)
	}
	l.mu.Lock()
	l.trustedUntil = record.ExpiresAt
	l.mu.Unlock()
	l.renewSuccesses.Add(1)
	return nil
}

func (l *Lease) Trusted(now time.Time) bool {
	l.mu.RLock()
	deadline := l.trustedUntil
	l.mu.RUnlock()
	return !deadline.IsZero() && now.Before(deadline)
}

func (l *Lease) TrustedUntil() time.Time {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.trustedUntil
}

type Stats struct {
	RenewAttempts  uint64
	RenewSuccesses uint64
	RenewFailures  uint64
	TrustedUntil   time.Time
}

func (l *Lease) Stats() Stats {
	if l == nil {
		return Stats{}
	}
	return Stats{RenewAttempts: l.renewAttempts.Load(), RenewSuccesses: l.renewSuccesses.Load(),
		RenewFailures: l.renewFailures.Load(), TrustedUntil: l.TrustedUntil()}
}

// MemoryStore is the development/test implementation. It preserves exact
// boot fences and TTL semantics without introducing a second persistence model.
type MemoryStore struct {
	mu      sync.Mutex
	now     func() time.Time
	records map[Fence]time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{now: time.Now, records: make(map[Fence]time.Time)}
}

func (s *MemoryStore) Renew(_ context.Context, fence Fence, ttl time.Duration) (Record, error) {
	if !fence.Valid() || ttl <= 0 {
		return Record{}, ErrInvalidFence
	}
	expiresAt := s.now().Add(ttl)
	s.mu.Lock()
	s.records[fence] = expiresAt
	s.mu.Unlock()
	return Record{Fence: fence, ExpiresAt: expiresAt}, nil
}

func (s *MemoryStore) Alive(_ context.Context, fence Fence) (bool, error) {
	if !fence.Valid() {
		return false, ErrInvalidFence
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	expiresAt, ok := s.records[fence]
	if ok && !s.now().Before(expiresAt) {
		delete(s.records, fence)
		ok = false
	}
	return ok, nil
}
