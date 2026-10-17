package blobstore

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestWriterEpochFencesOldHolderAndLegacyCAS(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.InsertEpoch(ctx, 7, 3, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := store.CASEpoch(ctx, 7, 1, 3, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := store.Fence(ctx, 7, 4); err != nil {
		t.Fatal(err)
	}
	if err := store.CASEpoch(ctx, 7, 2, 3, []byte("stale")); !errors.Is(err, ErrWriterFenced) {
		t.Fatalf("old holder err=%v", err)
	}
	if err := store.CAS(ctx, 7, 2, []byte("legacy")); !errors.Is(err, ErrWriterFenced) {
		t.Fatalf("legacy writer err=%v", err)
	}
	if err := store.CASEpoch(ctx, 7, 2, 4, []byte("v3")); err != nil {
		t.Fatalf("new holder err=%v", err)
	}
}

func TestMeasuredStoreExposesCASAndFenceOutcomes(t *testing.T) {
	registry := prometheus.NewRegistry()
	store := NewMeasuredStore(NewMemoryStore(), registry, "test")
	ctx := context.Background()
	if err := store.Insert(ctx, 8, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := store.CAS(ctx, 8, 99, []byte("bad")); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("cas err=%v", err)
	}
	epochStore := store.(EpochStore)
	if err := epochStore.Fence(ctx, 8, 2); err != nil {
		t.Fatal(err)
	}
	if err := store.CAS(ctx, 8, 1, []byte("stale")); !errors.Is(err, ErrWriterFenced) {
		t.Fatalf("legacy cas err=%v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 2 {
		t.Fatalf("metric families=%d, want latency and outcomes", len(families))
	}
}

func TestMemoryStoreInsertAndCAS(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.Insert(ctx, 7, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := store.Insert(ctx, 7, []byte("duplicate")); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate insert: %v", err)
	}
	data, version, err := store.Get(ctx, 7)
	if err != nil || string(data) != "one" || version != 1 {
		t.Fatalf("get data=%q version=%d err=%v", data, version, err)
	}
	if err := store.CAS(ctx, 7, 99, []byte("bad")); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("stale cas: %v", err)
	}
	if err := store.CAS(ctx, 7, version, []byte("two")); err != nil {
		t.Fatal(err)
	}
	data, version, err = store.Get(ctx, 7)
	if err != nil || string(data) != "two" || version != 2 {
		t.Fatalf("get data=%q version=%d err=%v", data, version, err)
	}
}
