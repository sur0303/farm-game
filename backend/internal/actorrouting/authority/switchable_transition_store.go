package authority

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var ErrInitialTransitionStoreInvalid = errors.New("route authority: invalid initial transition store")

// SwitchableTransitionStore keeps the bounded initial assignment's recovery
// bookkeeping in memory. The durable adapter is still loaded during Manager
// construction, but no O(N) Redis validation runs for each bounded claim.
// SealDurable is one-way for the lifetime of this Manager composition.
type SwitchableTransitionStore struct {
	mu          sync.RWMutex
	durable     TransitionTombstoneStore
	recovery    TransitionRecoveryStore
	evidence    ExactTransitionEvidenceStore
	memory      *MemoryTransitionTombstoneStore
	memoryLimit int
	buffered    bool
	sealed      bool
}

func NewSwitchableTransitionStore(durable TransitionTombstoneStore, memoryLimit int) (*SwitchableTransitionStore, error) {
	if durable == nil {
		return nil, ErrInitialTransitionStoreInvalid
	}
	recovery, ok := durable.(TransitionRecoveryStore)
	if !ok {
		return nil, fmt.Errorf("%w: durable recovery store required", ErrInitialTransitionStoreInvalid)
	}
	evidence, _ := durable.(ExactTransitionEvidenceStore)
	if memoryLimit <= 0 {
		memoryLimit = 1024
	}
	return &SwitchableTransitionStore{
		durable: durable, recovery: recovery, evidence: evidence,
		memory: NewMemoryTransitionTombstoneStore(memoryLimit), memoryLimit: memoryLimit,
	}, nil
}

// BeginBufferedInitialAuthorityBatch switches the manager-owned transition
// evidence to its bounded in-memory adapter. It must happen before the first
// initial Claim and cannot be repeated after sealing.
func (s *SwitchableTransitionStore) BeginBufferedInitialAuthorityBatch() error {
	if s == nil {
		return ErrInitialTransitionStoreInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buffered || s.sealed {
		return fmt.Errorf("%w: transition store already switched", ErrInitialTransitionStoreInvalid)
	}
	s.buffered = true
	s.memory = NewMemoryTransitionTombstoneStore(s.memoryLimit)
	return nil
}

// SealDurable irreversibly routes all later transition evidence to the
// production adapter. Initial in-memory records are intentionally discarded:
// exact Authority records and the final durable snapshot are the crash-resume
// source of truth for this bounded batch.
func (s *SwitchableTransitionStore) SealDurable() error {
	if s == nil {
		return ErrInitialTransitionStoreInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.buffered || s.sealed {
		return fmt.Errorf("%w: transition store is not in the initial batch", ErrInitialTransitionStoreInvalid)
	}
	s.buffered = false
	s.sealed = true
	s.memory = nil
	return nil
}

func (s *SwitchableTransitionStore) Durable() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return !s.buffered
}

func (s *SwitchableTransitionStore) backend() (*MemoryTransitionTombstoneStore, TransitionTombstoneStore, TransitionRecoveryStore, ExactTransitionEvidenceStore, func()) {
	s.mu.RLock()
	if s.buffered {
		return s.memory, nil, s.memory, s.memory, s.mu.RUnlock
	}
	return nil, s.durable, s.recovery, s.evidence, s.mu.RUnlock
}

func (s *SwitchableTransitionStore) LoadTransitionTombstones(ctx context.Context) ([]TransitionTombstone, error) {
	memory, durable, _, _, release := s.backend()
	defer release()
	if memory != nil {
		return memory.LoadTransitionTombstones(ctx)
	}
	return durable.LoadTransitionTombstones(ctx)
}

func (s *SwitchableTransitionStore) LookupTransitionTombstone(ctx context.Context, id string) (TransitionTombstone, bool, error) {
	memory, durable, _, _, release := s.backend()
	defer release()
	if memory != nil {
		return memory.LookupTransitionTombstone(ctx, id)
	}
	return durable.LookupTransitionTombstone(ctx, id)
}

func (s *SwitchableTransitionStore) SaveTransitionTombstone(ctx context.Context, entry TransitionTombstone) error {
	memory, durable, _, _, release := s.backend()
	defer release()
	if memory != nil {
		return memory.SaveTransitionTombstone(ctx, entry)
	}
	return durable.SaveTransitionTombstone(ctx, entry)
}

func (s *SwitchableTransitionStore) LoadTransitionRecoveries(ctx context.Context) ([]TransitionRecovery, error) {
	memory, _, recovery, _, release := s.backend()
	defer release()
	if memory != nil {
		return memory.LoadTransitionRecoveries(ctx)
	}
	return recovery.LoadTransitionRecoveries(ctx)
}

func (s *SwitchableTransitionStore) LookupTransitionRecovery(ctx context.Context, id string) (TransitionRecovery, bool, error) {
	memory, _, recovery, _, release := s.backend()
	defer release()
	if memory != nil {
		return memory.LookupTransitionRecovery(ctx, id)
	}
	return recovery.LookupTransitionRecovery(ctx, id)
}

func (s *SwitchableTransitionStore) ReserveTransitionRecovery(ctx context.Context, recoveryValue TransitionRecovery) error {
	memory, _, recovery, _, release := s.backend()
	defer release()
	if memory != nil {
		return memory.ReserveTransitionRecovery(ctx, recoveryValue)
	}
	return recovery.ReserveTransitionRecovery(ctx, recoveryValue)
}

func (s *SwitchableTransitionStore) CommitTransitionRecovery(ctx context.Context, recoveryValue TransitionRecovery) error {
	memory, _, recovery, _, release := s.backend()
	defer release()
	if memory != nil {
		return memory.CommitTransitionRecovery(ctx, recoveryValue)
	}
	return recovery.CommitTransitionRecovery(ctx, recoveryValue)
}

func (s *SwitchableTransitionStore) DeleteTransitionRecovery(ctx context.Context, id string) error {
	memory, _, recovery, _, release := s.backend()
	defer release()
	if memory != nil {
		return memory.DeleteTransitionRecovery(ctx, id)
	}
	return recovery.DeleteTransitionRecovery(ctx, id)
}

func (s *SwitchableTransitionStore) ObserveTransitionEvidence(ctx context.Context, id string) (TransitionRecovery, bool, TransitionTombstone, bool, error) {
	memory, _, _, evidence, release := s.backend()
	defer release()
	if memory != nil {
		return memory.ObserveTransitionEvidence(ctx, id)
	}
	if evidence == nil {
		return TransitionRecovery{}, false, TransitionTombstone{}, false, ErrTransitionReconcile
	}
	return evidence.ObserveTransitionEvidence(ctx, id)
}

var (
	_ TransitionTombstoneStore     = (*SwitchableTransitionStore)(nil)
	_ TransitionRecoveryStore      = (*SwitchableTransitionStore)(nil)
	_ ExactTransitionEvidenceStore = (*SwitchableTransitionStore)(nil)
)
