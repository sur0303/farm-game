package ownerstore

import (
	"context"
	"errors"
	"fmt"

	ownercorepb "farmgame/api/gen/ownercore"
)

// CASMutation constructs the next record from the latest authoritative record.
// The record passed to the callback is a clone and may be mutated in place.
type CASMutation func(current *ownercorepb.OwnerCoreRecord, version uint64) (*ownercorepb.OwnerCoreRecord, error)

// UnknownWriteResolver resolves a timeout/unknown CAS result using the
// operation's idempotency key. It must return true only after observing the
// committed operation in authoritative state.
type UnknownWriteResolver func(ctx context.Context, operationID string) (committed bool, err error)

// CompareAndSwapWithRetry retries the whole read/compute/CAS turn on bounded
// version conflicts. It never converts an ambiguous write into success unless
// the caller's resolver proves that the operation committed.
func CompareAndSwapWithRetry(
	ctx context.Context,
	store OwnerStore,
	ownerID uint64,
	operationID string,
	maxAttempts int,
	mutate CASMutation,
	resolve UnknownWriteResolver,
) (*ownercorepb.OwnerCoreRecord, uint64, error) {
	if store == nil {
		return nil, 0, ErrUnavailable
	}
	if mutate == nil {
		return nil, 0, errors.New("ownerstore: nil CAS mutation")
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	current, version, err := store.Get(ctx, ownerID)
	if err != nil {
		return nil, 0, err
	}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		next, err := mutate(Clone(current), version)
		if err != nil {
			return nil, 0, err
		}
		if next == nil {
			return nil, 0, errors.New("ownerstore: CAS mutation returned nil")
		}
		if err := store.CAS(ctx, next, version); err == nil {
			return Clone(next), version + 1, nil
		} else if errors.Is(err, ErrCASConflict) {
			current, version, err = store.Get(ctx, ownerID)
			if err != nil {
				return nil, 0, err
			}
			continue
		} else if errors.Is(err, ErrOwnerEpochStale) {
			return nil, 0, err
		} else if errors.Is(err, ErrTimeoutUnknown) {
			if resolve == nil {
				return nil, 0, err
			}
			committed, resolveErr := resolve(ctx, operationID)
			if resolveErr != nil {
				return nil, 0, resolveErr
			}
			if !committed {
				return nil, 0, err
			}
			// The resolver proved the write. Re-read the authoritative value so
			// callers never return a locally fabricated success.
			return store.Get(ctx, ownerID)
		} else {
			return nil, 0, err
		}
	}
	return nil, 0, fmt.Errorf("%w: %w after %d attempts", ErrUnavailable, ErrCASConflict, maxAttempts)
}
