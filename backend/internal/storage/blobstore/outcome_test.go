package blobstore

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
)

type timeoutStore struct {
	data []byte
}

type conflictStore struct{}

func (conflictStore) Get(context.Context, uint64) ([]byte, uint64, error) {
	return []byte("authoritative"), 1, nil
}

func (conflictStore) Insert(context.Context, uint64, []byte) error { return nil }

func (conflictStore) CAS(context.Context, uint64, uint64, []byte) error {
	return ErrCASConflict
}

func (s *timeoutStore) Get(context.Context, uint64) ([]byte, uint64, error) {
	return append([]byte(nil), s.data...), 1, nil
}

func (*timeoutStore) Insert(context.Context, uint64, []byte) error { return nil }

func (*timeoutStore) CAS(context.Context, uint64, uint64, []byte) error {
	return ErrTimeoutUnknown
}

func TestCompareAndSwapWithRetryExhaustionIsConflictOnly(t *testing.T) {
	_, _, err := CompareAndSwapWithRetry(context.Background(), conflictStore{}, 1, "op-conflict", 2,
		func([]byte, uint64) ([]byte, error) {
			return []byte("next"), nil
		}, nil)
	if !errors.Is(err, ErrCASConflict) || errors.Is(err, ErrUnavailable) {
		t.Fatalf("err=%v, want conflict only", err)
	}
}

func TestCompareAndSwapWithRetryDoesNotClaimUnprovenTimeout(t *testing.T) {
	store := &timeoutStore{data: []byte("authoritative")}
	_, _, err := CompareAndSwapWithRetry(context.Background(), store, 1, "op-1", 3,
		func([]byte, uint64) ([]byte, error) {
			return []byte("local-success"), nil
		},
		func(context.Context, string) (bool, error) {
			return false, nil
		})
	if !errors.Is(err, ErrTimeoutUnknown) {
		t.Fatalf("err=%v, want timeout unknown", err)
	}
}

func TestCompareAndSwapWithRetryRereadsAfterResolverProof(t *testing.T) {
	store := &timeoutStore{data: []byte("authoritative")}
	data, version, err := CompareAndSwapWithRetry(context.Background(), store, 1, "op-2", 3,
		func([]byte, uint64) ([]byte, error) {
			return []byte("locally-fabricated"), nil
		},
		func(context.Context, string) (bool, error) {
			return true, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "authoritative" || version != 1 {
		t.Fatalf("data=%q version=%d, want authoritative version 1", data, version)
	}
}

func TestRedisWriteErrorClassification(t *testing.T) {
	timeout := &net.OpError{Op: "write", Net: "tcp", Err: syscall.ETIMEDOUT}
	if err := classifyRedisError(timeout, true); !errors.Is(err, ErrTimeoutUnknown) ||
		errors.Is(err, ErrUnavailable) {
		t.Fatalf("timeout classification=%v", err)
	}
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	if err := classifyRedisError(refused, true); !errors.Is(err, ErrUnavailable) ||
		errors.Is(err, ErrTimeoutUnknown) {
		t.Fatalf("refused classification=%v", err)
	}
	if err := classifyRedisError(context.DeadlineExceeded, true); !errors.Is(err, ErrTimeoutUnknown) {
		t.Fatalf("deadline classification=%v", err)
	}
}

func TestEpochMismatchIsNotVersionConflict(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	if err := store.InsertEpoch(ctx, 7, 3, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	err := store.CASEpoch(ctx, 7, 1, 2, []byte("stale-holder"))
	if !errors.Is(err, ErrWriterEpochMismatch) || errors.Is(err, ErrCASConflict) {
		t.Fatalf("err=%v, want writer epoch mismatch only", err)
	}
}
