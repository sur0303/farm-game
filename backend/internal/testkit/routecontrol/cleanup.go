package routecontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// ExactKeyCleanup is implemented by a caller-owned exact-key adapter. The
// adapter may retain the raw key privately; the registry stores only its
// digest and this interface, never a raw key string.
type ExactKeyCleanup interface {
	DeleteExact(context.Context) error
	ExistsExact(context.Context) (bool, error)
}

// KeyHandle binds an exact-key cleanup adapter to a non-secret digest.
type KeyHandle struct {
	digest  string
	cleanup ExactKeyCleanup
}

// NewKeyHandle constructs a key handle from a precomputed digest.
func NewKeyHandle(digest string, cleanup ExactKeyCleanup) (KeyHandle, error) {
	if !validSHA256(digest) {
		return KeyHandle{}, errors.New("key digest must be 64 lowercase hex characters")
	}
	if cleanup == nil {
		return KeyHandle{}, errors.New("exact-key cleanup adapter is required")
	}
	return KeyHandle{digest: digest, cleanup: cleanup}, nil
}

// KeyDigest returns the SHA-256 digest of a raw key without retaining the raw
// key in this package.
func KeyDigest(rawKey string) string {
	digest := sha256.Sum256([]byte(rawKey))
	return hex.EncodeToString(digest[:])
}

// Digest returns the registered key digest.
func (h KeyHandle) Digest() string {
	return h.digest
}

// ExactKeyRegistry tracks only explicitly registered exact keys. Cleanup
// performs DeleteExact followed by ExistsExact for every key and provides no
// discovery or global flush operation.
type ExactKeyRegistry struct {
	mu      sync.Mutex
	handles map[string]KeyHandle
	state   exactKeyRegistryState
}

type exactKeyRegistryState uint8

const (
	exactKeyRegistryOpen exactKeyRegistryState = iota
	exactKeyRegistryClosing
	exactKeyRegistryClosed
)

// NewExactKeyRegistry constructs an empty registry.
func NewExactKeyRegistry() *ExactKeyRegistry {
	return &ExactKeyRegistry{handles: make(map[string]KeyHandle)}
}

// Register adds one exact key by digest.
func (r *ExactKeyRegistry) Register(handle KeyHandle) error {
	if r == nil {
		return errors.New("exact-key registry is nil")
	}
	if !validSHA256(handle.digest) || handle.cleanup == nil {
		return errors.New("invalid exact-key handle")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	switch r.state {
	case exactKeyRegistryClosing:
		return ErrCleanupInProgress
	case exactKeyRegistryClosed:
		return ErrExactKeyRegistryClosed
	}
	if _, exists := r.handles[handle.digest]; exists {
		return fmt.Errorf("exact key %s already registered", handle.digest)
	}
	r.handles[handle.digest] = handle
	return nil
}

// Digests returns only sorted non-secret identifiers.
func (r *ExactKeyRegistry) Digests() []string {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	digests := make([]string, 0, len(r.handles))
	for digest := range r.handles {
		digests = append(digests, digest)
	}
	sort.Strings(digests)
	return digests
}

// Cleanup deletes and verifies every registered key. All keys are attempted;
// the returned error joins individual failures.
func (r *ExactKeyRegistry) Cleanup(ctx context.Context) error {
	if r == nil {
		return errors.New("exact-key registry is nil")
	}
	if ctx == nil {
		return errors.New("cleanup context is nil")
	}
	r.mu.Lock()
	switch r.state {
	case exactKeyRegistryClosing:
		r.mu.Unlock()
		return ErrCleanupInProgress
	case exactKeyRegistryClosed:
		r.mu.Unlock()
		return nil
	default:
		r.state = exactKeyRegistryClosing
	}
	handles := make([]KeyHandle, 0, len(r.handles))
	for _, handle := range r.handles {
		handles = append(handles, handle)
	}
	r.mu.Unlock()
	sort.Slice(handles, func(i, j int) bool { return handles[i].digest < handles[j].digest })

	var joined error
	for _, handle := range handles {
		if err := ctx.Err(); err != nil {
			joined = errors.Join(joined, err)
			break
		}
		if err := handle.cleanup.DeleteExact(ctx); err != nil {
			joined = errors.Join(joined, fmt.Errorf("%s DEL: %w", handle.digest, err))
		}
		remaining, err := handle.cleanup.ExistsExact(ctx)
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("%s EXISTS: %w", handle.digest, err))
			continue
		}
		if remaining {
			joined = errors.Join(joined, fmt.Errorf("%w: %s", ErrKeyResidue, handle.digest))
		}
	}
	r.mu.Lock()
	if joined == nil {
		r.state = exactKeyRegistryClosed
	} else {
		// A failed cleanup remains retryable, but no registration can race
		// with the current cleanup pass.
		r.state = exactKeyRegistryOpen
	}
	r.mu.Unlock()
	return joined
}

func (h KeyHandle) String() string {
	return strings.TrimSpace(h.digest)
}
