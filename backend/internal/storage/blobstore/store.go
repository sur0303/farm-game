// Package blobstore provides the CAS byte-record contract shared by the new
// PlayerActor and FarmActor stores. RedisStore is a development/staging adapter,
// not the production asset authority; the same contract is implemented by the
// dedicated Tcaplus repositories before production cutover.
package blobstore

import (
	"context"
	"errors"
)

var (
	ErrNotFound            = errors.New("blobstore: record not found")
	ErrExists              = errors.New("blobstore: record already exists")
	ErrCASConflict         = errors.New("blobstore: record version conflict")
	ErrWriterEpochMismatch = errors.New("blobstore: writer epoch mismatch")
	// ErrWriterFenced is kept as a compatibility alias for existing callers.
	ErrWriterFenced   = ErrWriterEpochMismatch
	ErrTimeoutUnknown = errors.New("blobstore: timeout or unknown result")
	ErrUnavailable    = errors.New("blobstore: unavailable and not sent")
)

type Store interface {
	Get(ctx context.Context, id uint64) (data []byte, version uint64, err error)
	Insert(ctx context.Context, id uint64, data []byte) error
	CAS(ctx context.Context, id uint64, expectVersion uint64, data []byte) error
}

// CASMutation computes the next blob from the latest authoritative blob.
type CASMutation func(current []byte, version uint64) (next []byte, err error)

// UnknownWriteResolver proves whether an operation identified by operationID
// was committed. It must return true only after observing that operation in
// authoritative state; a local copy or a timeout is not proof.
type UnknownWriteResolver func(ctx context.Context, operationID string) (committed bool, err error)

// CompareAndSwapWithRetry retries only known version conflicts. An ambiguous
// write is never reported as success without resolver proof. After proof, it
// re-reads the authoritative record instead of returning locally constructed
// bytes.
func CompareAndSwapWithRetry(
	ctx context.Context,
	store Store,
	id uint64,
	operationID string,
	maxAttempts int,
	mutate CASMutation,
	resolve UnknownWriteResolver,
) ([]byte, uint64, error) {
	if store == nil {
		return nil, 0, ErrUnavailable
	}
	if mutate == nil {
		return nil, 0, errors.New("blobstore: nil CAS mutation")
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	current, version, err := store.Get(ctx, id)
	if err != nil {
		return nil, 0, err
	}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		next, err := mutate(append([]byte(nil), current...), version)
		if err != nil {
			return nil, 0, err
		}
		if next == nil {
			return nil, 0, errors.New("blobstore: CAS mutation returned nil")
		}
		err = store.CAS(ctx, id, version, next)
		switch {
		case err == nil:
			return append([]byte(nil), next...), version + 1, nil
		case errors.Is(err, ErrCASConflict):
			current, version, err = store.Get(ctx, id)
			if err != nil {
				return nil, 0, err
			}
		case errors.Is(err, ErrWriterEpochMismatch):
			return nil, 0, err
		case errors.Is(err, ErrTimeoutUnknown):
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
			return store.Get(ctx, id)
		default:
			return nil, 0, err
		}
	}
	return nil, 0, ErrCASConflict
}

// EpochStore is the stronger multi-holder contract. Fence must be committed
// before a new shard holder serves writes; stale CASEpoch calls then fail even
// when the old process and its CAS version are otherwise still valid.
type EpochStore interface {
	Store
	InsertEpoch(ctx context.Context, id uint64, writerEpoch uint64, data []byte) error
	CASEpoch(ctx context.Context, id uint64, expectVersion, writerEpoch uint64, data []byte) error
	Fence(ctx context.Context, id uint64, writerEpoch uint64) error
}

func InsertAtEpoch(ctx context.Context, store Store, id, writerEpoch uint64, data []byte) error {
	if writerEpoch == 0 {
		return store.Insert(ctx, id, data)
	}
	epochStore, ok := store.(EpochStore)
	if !ok {
		return ErrUnavailable
	}
	return epochStore.InsertEpoch(ctx, id, writerEpoch, data)
}

func CASAtEpoch(ctx context.Context, store Store, id, expectVersion, writerEpoch uint64, data []byte) error {
	if writerEpoch == 0 {
		return store.CAS(ctx, id, expectVersion, data)
	}
	epochStore, ok := store.(EpochStore)
	if !ok {
		return ErrUnavailable
	}
	return epochStore.CASEpoch(ctx, id, expectVersion, writerEpoch, data)
}

func FenceAtEpoch(ctx context.Context, store Store, id, writerEpoch uint64) error {
	if writerEpoch == 0 {
		return nil
	}
	epochStore, ok := store.(EpochStore)
	if !ok {
		return ErrUnavailable
	}
	return epochStore.Fence(ctx, id, writerEpoch)
}
