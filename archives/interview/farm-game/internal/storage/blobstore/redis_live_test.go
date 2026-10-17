package blobstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestLiveRedisBlobStore verifies the exact CAS/fence primitives used by
// PlayerActor and FarmActor against the configured company Redis.
func TestLiveRedisBlobStore(t *testing.T) {
	uri := os.Getenv("FARM_REDIS_URL")
	if uri == "" {
		t.Skip("FARM_REDIS_URL not set; skipping live redis blobstore test")
	}
	options, err := redis.ParseURL(uri)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	prefix := fmt.Sprintf("farm:test:blob:%d", time.Now().UnixNano())
	store, err := NewRedisStore(client, prefix)
	if err != nil {
		t.Fatal(err)
	}
	id := uint64(1)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = client.Del(cleanupCtx, store.key(id)).Err()
	})

	if err := store.Insert(ctx, id, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	var winners atomic.Int32
	var wait sync.WaitGroup
	for i := 0; i < 16; i++ {
		wait.Add(1)
		go func(candidate int) {
			defer wait.Done()
			err := store.CAS(ctx, id, 1, []byte(fmt.Sprintf("winner-%d", candidate)))
			if err == nil {
				winners.Add(1)
				return
			}
			if !errors.Is(err, ErrCASConflict) {
				t.Errorf("concurrent CAS: %v", err)
			}
		}(i)
	}
	wait.Wait()
	if winners.Load() != 1 {
		t.Fatalf("CAS winners=%d, want 1", winners.Load())
	}

	// Model a response lost after Redis committed the script: discard the
	// successful return, then resolve uncertainty by reading the authoritative
	// version and marker. Replaying the stale expected version must not commit.
	if err := store.CAS(ctx, id, 2, []byte("operation-marker-uncertain")); err != nil {
		t.Fatal(err)
	}
	data, version, err := store.Get(ctx, id)
	if err != nil || version != 3 || string(data) != "operation-marker-uncertain" {
		t.Fatalf("resolve uncertain write data=%q version=%d err=%v", data, version, err)
	}
	if err := store.CAS(ctx, id, 2, []byte("duplicate")); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("stale retry after uncertain commit=%v", err)
	}

	if err := store.Fence(ctx, id, 7); err != nil {
		t.Fatal(err)
	}
	if err := store.CASEpoch(ctx, id, 3, 6, []byte("old-holder")); !errors.Is(err, ErrWriterFenced) {
		t.Fatalf("old holder write=%v", err)
	}
	if err := store.CASEpoch(ctx, id, 3, 7, []byte("new-holder")); err != nil {
		t.Fatalf("new holder write=%v", err)
	}
}
