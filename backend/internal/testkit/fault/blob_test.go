package fault

import (
	"context"
	"errors"
	"testing"

	"farmgame/internal/storage/blobstore"
)

func newMemoryBlob(t *testing.T) (*BlobStore, *blobstore.MemoryStore) {
	t.Helper()
	mem := blobstore.NewMemoryStore()
	return NewBlobStore(mem), mem
}

func TestBlobStoreUnavailableNotSentDoesNotMutate(t *testing.T) {
	store, mem := newMemoryBlob(t)
	ctx := context.Background()
	store.Inject(BlobInsert, BlobUnavailableNotSent)
	if err := store.Insert(ctx, 1, []byte("v1")); !errors.Is(err, blobstore.ErrUnavailable) {
		t.Fatalf("err=%v", err)
	}
	if _, _, err := mem.Get(ctx, 1); !errors.Is(err, blobstore.ErrNotFound) {
		t.Fatalf("underlying store must stay untouched, err=%v", err)
	}
}

func TestBlobStoreTimeoutUnknownBeforeApplyDoesNotMutate(t *testing.T) {
	store, mem := newMemoryBlob(t)
	ctx := context.Background()
	if err := mem.Insert(ctx, 1, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	store.Inject(BlobCAS, BlobTimeoutUnknownBeforeApply)
	if err := store.CAS(ctx, 1, 1, []byte("v2")); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("err=%v", err)
	}
	data, version, err := mem.Get(ctx, 1)
	if err != nil || string(data) != "v1" || version != 1 {
		t.Fatalf("before-apply must not mutate: data=%s version=%d err=%v", data, version, err)
	}
}

func TestBlobStoreTimeoutUnknownAfterApplyMutates(t *testing.T) {
	store, mem := newMemoryBlob(t)
	ctx := context.Background()
	if err := mem.Insert(ctx, 1, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	store.Inject(BlobCAS, BlobTimeoutUnknownAfterApply)
	if err := store.CAS(ctx, 1, 1, []byte("v2")); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("err=%v", err)
	}
	data, version, err := mem.Get(ctx, 1)
	if err != nil || string(data) != "v2" || version != 2 {
		t.Fatalf("after-apply must commit then hide response: data=%s version=%d err=%v", data, version, err)
	}
}

func TestBlobStoreEpochAndFencePassthrough(t *testing.T) {
	store, mem := newMemoryBlob(t)
	ctx := context.Background()
	if err := store.InsertEpoch(ctx, 7, 1, []byte("epoch1")); err != nil {
		t.Fatal(err)
	}
	if err := store.Fence(ctx, 7, 2); err != nil {
		t.Fatal(err)
	}
	// 旧 holder 写必须被 fence(与 blobstore.MemoryStore 语义一致)。
	if err := store.CASEpoch(ctx, 7, 1, 1, []byte("stale")); !errors.Is(err, blobstore.ErrWriterFenced) {
		t.Fatalf("fenced write err=%v", err)
	}
	if _, _, err := mem.Get(ctx, 7); err != nil {
		t.Fatal(err)
	}
	if store.Calls(BlobInsertEpoch) != 1 || store.Calls(BlobFence) != 1 || store.Calls(BlobCASEpoch) != 1 {
		t.Fatal("call counters wrong")
	}
}

func TestBlobStoreCrashPanics(t *testing.T) {
	store, _ := newMemoryBlob(t)
	store.Inject(BlobGet, BlobCrash)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	_, _, _ = store.Get(context.Background(), 1)
}
