// Package persistence contains the storage-independent per-Actor persistence
// lane. The lane serializes SoftSave, durable barriers, Unknown resolution and
// drain; a later Redis adapter must not bypass it with another writer.
package persistence

import (
	"errors"
	"fmt"
	"sync"
)

type State uint8

const (
	Idle State = iota
	SoftSaveInflight
	DurableSaveInflight
	ResolvingUnknown
	Draining
)

type OperationKind uint8

const (
	OperationUnknown OperationKind = iota
	OperationSoftSave
	OperationDurableSave
)

type Completion uint8

const (
	CompletionUnknown Completion = iota
	CompletionCommitted
	CompletionNotSent
	CompletionTimeoutUnknown
)

var (
	ErrLaneBusy          = errors.New("persistence lane: busy")
	ErrLaneDraining      = errors.New("persistence lane: draining")
	ErrLaneDirty         = errors.New("persistence lane: dirty state requires durable flush")
	ErrSoftSaveDisabled  = errors.New("persistence lane: soft save disabled")
	ErrVersionRegression = errors.New("persistence lane: version regression")
	ErrStaleCompletion   = errors.New("persistence lane: stale completion")
	ErrInvalidCompletion = errors.New("persistence lane: invalid completion")
)

type Options struct {
	EnableSoftSave bool
}

type Token struct {
	id      uint64
	kind    OperationKind
	version uint64
}

type Snapshot struct {
	State                   State
	MemoryStateVersion      uint64
	PersistedThroughVersion uint64
	DurableThroughVersion   uint64
	SaveInflightVersion     uint64
	Dirty                   bool
}

type Lane struct {
	mu                      sync.Mutex
	enableSoft              bool
	state                   State
	nextToken               uint64
	inflight                Token
	memoryStateVersion      uint64
	persistedThroughVersion uint64
	durableThroughVersion   uint64
}

func NewLane(options Options) *Lane {
	return &Lane{enableSoft: options.EnableSoftSave, state: Idle}
}

func (l *Lane) MarkMemoryVersion(version uint64) error {
	if l == nil || version == 0 {
		return ErrVersionRegression
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if version < l.memoryStateVersion {
		return ErrVersionRegression
	}
	l.memoryStateVersion = version
	return nil
}

// RestorePersistedVersion seeds the logical version watermark recovered from
// a persisted actor snapshot. In dirty-writeback mode Redis RecordVersion may
// be lower than the actor's logical memory version because many turns are
// coalesced into one physical write. Restoration is activation-only and may
// not race an in-flight save.
func (l *Lane) RestorePersistedVersion(version uint64) error {
	if l == nil || version == 0 {
		return ErrVersionRegression
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state != Idle {
		return ErrLaneBusy
	}
	if version < l.memoryStateVersion || version < l.persistedThroughVersion {
		return ErrVersionRegression
	}
	l.memoryStateVersion = version
	l.persistedThroughVersion = version
	if version > l.durableThroughVersion {
		l.durableThroughVersion = version
	}
	return nil
}

func (l *Lane) BeginSoft(version uint64) (Token, error) {
	if l == nil {
		return Token{}, ErrLaneBusy
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.enableSoft {
		return Token{}, ErrSoftSaveDisabled
	}
	return l.beginLocked(OperationSoftSave, version)
}

func (l *Lane) BeginDurable(version uint64) (Token, error) {
	if l == nil {
		return Token{}, ErrLaneBusy
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.beginLocked(OperationDurableSave, version)
}

func (l *Lane) Complete(token Token, completion Completion) error {
	if l == nil {
		return ErrStaleCompletion
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.matchesLocked(token) || (l.state != SoftSaveInflight && l.state != DurableSaveInflight) {
		return ErrStaleCompletion
	}
	switch completion {
	case CompletionCommitted:
		if token.version > l.persistedThroughVersion {
			l.persistedThroughVersion = token.version
		}
		if token.kind == OperationDurableSave && token.version > l.durableThroughVersion {
			l.durableThroughVersion = token.version
		}
		l.clearInflightLocked(Idle)
		return nil
	case CompletionNotSent:
		l.clearInflightLocked(Idle)
		return nil
	case CompletionTimeoutUnknown:
		l.state = ResolvingUnknown
		return nil
	default:
		return ErrInvalidCompletion
	}
}

func (l *Lane) ResolveUnknown(token Token, committed bool) error {
	if l == nil {
		return ErrStaleCompletion
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state != ResolvingUnknown || !l.matchesLocked(token) {
		return ErrStaleCompletion
	}
	if committed {
		if token.version > l.persistedThroughVersion {
			l.persistedThroughVersion = token.version
		}
		if token.kind == OperationDurableSave && token.version > l.durableThroughVersion {
			l.durableThroughVersion = token.version
		}
	}
	l.clearInflightLocked(Idle)
	return nil
}

func (l *Lane) BeginDrain() error {
	if l == nil {
		return ErrLaneBusy
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state == Draining {
		return nil
	}
	if l.state != Idle {
		return ErrLaneBusy
	}
	if l.memoryStateVersion > l.persistedThroughVersion {
		return ErrLaneDirty
	}
	l.state = Draining
	return nil
}

// BeginGenerationRetirement terminally discards process-local memory for an
// already fenced authority generation. Unlike BeginDrain, it may abandon a
// dirty in-memory candidate because no operation can use this lane again.
// Callers must first settle or explicitly abandon any ResolvingUnknown token;
// ordinary active-generation drains must continue to use BeginDrain.
func (l *Lane) BeginGenerationRetirement() error {
	if l == nil {
		return ErrLaneBusy
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state == Draining {
		return nil
	}
	if l.state != Idle {
		return ErrLaneBusy
	}
	l.memoryStateVersion = l.persistedThroughVersion
	l.inflight = Token{}
	l.state = Draining
	return nil
}

func (l *Lane) Snapshot() Snapshot {
	if l == nil {
		return Snapshot{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return Snapshot{
		State:                   l.state,
		MemoryStateVersion:      l.memoryStateVersion,
		PersistedThroughVersion: l.persistedThroughVersion,
		DurableThroughVersion:   l.durableThroughVersion,
		SaveInflightVersion:     l.inflight.version,
		Dirty:                   l.memoryStateVersion > l.persistedThroughVersion,
	}
}

func (l *Lane) beginLocked(kind OperationKind, version uint64) (Token, error) {
	if l.state == Draining {
		return Token{}, ErrLaneDraining
	}
	if l.state != Idle {
		return Token{}, ErrLaneBusy
	}
	if version == 0 || version > l.memoryStateVersion || version < l.persistedThroughVersion || (kind == OperationDurableSave && version < l.memoryStateVersion) {
		return Token{}, fmt.Errorf("%w: memory=%d persisted=%d requested=%d", ErrVersionRegression, l.memoryStateVersion, l.persistedThroughVersion, version)
	}
	l.nextToken++
	if l.nextToken == 0 {
		l.nextToken++
	}
	l.inflight = Token{id: l.nextToken, kind: kind, version: version}
	if kind == OperationSoftSave {
		l.state = SoftSaveInflight
	} else {
		l.state = DurableSaveInflight
	}
	return l.inflight, nil
}

func (l *Lane) matchesLocked(token Token) bool {
	return token.id != 0 && token == l.inflight
}

func (l *Lane) clearInflightLocked(next State) {
	l.inflight = Token{}
	l.state = next
}
