package routecontrol

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
)

func testSnapshot(revision uint64) actorrouting.RouteSnapshot {
	return actorrouting.RouteSnapshot{
		Realm:          "realm-a",
		GlobalRevision: revision,
		HashVersion:    actorrouting.HashVersionXXH64V2,
		HashSeed:       202608030001,
		Player: actorrouting.DomainSnapshot{
			ShardCount: 1,
			Routes: []actorrouting.ShardRoute{{
				HolderIndex:         0,
				State:               actorrouting.ShardStateActive,
				ShardRevision:       1,
				ExpectedWriterEpoch: 1,
				ExpectedLeaseEpoch:  1,
				LeaseUntilMs:        1000,
			}},
		},
		Farm: actorrouting.DomainSnapshot{
			ShardCount: 1,
			Routes: []actorrouting.ShardRoute{{
				HolderIndex:         1,
				State:               actorrouting.ShardStateActive,
				ShardRevision:       1,
				ExpectedWriterEpoch: 1,
				ExpectedLeaseEpoch:  1,
				LeaseUntilMs:        1000,
			}},
		},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "player-1", BootID: "boot-player", Endpoint: "player:1", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-1", BootID: "boot-farm", Endpoint: "farm:1", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
}

func TestMemorySnapshotStoreBootstrapIsIndependentFromCAS(t *testing.T) {
	store := NewMemoryRouteSnapshotStore()
	store.SetNow(func() int64 { return 1234 })
	ctx := context.Background()

	if result, err := store.CompareAndSwap(ctx, NestedControlOperationToken{}, 0, testSnapshot(1)); !errors.Is(err, ErrSnapshotBootstrapRequired) || result.Status != SnapshotCASRevisionConflict {
		t.Fatalf("empty ordinary CAS result=%+v err=%v", result, err)
	}
	if result, err := store.Bootstrap(ctx, NestedControlOperationToken{}, testSnapshot(2)); !errors.Is(err, ErrSnapshotCASConflict) || result.Status != SnapshotCASRevisionConflict {
		t.Fatalf("bad bootstrap result=%+v err=%v", result, err)
	}
	first, err := store.Bootstrap(ctx, NestedControlOperationToken{}, testSnapshot(1))
	if err != nil || first.Status != SnapshotCASCommitted || first.Stored.CommittedAtMs != 1234 {
		t.Fatalf("bootstrap result=%+v err=%v", first, err)
	}
	duplicate, err := store.Bootstrap(ctx, NestedControlOperationToken{}, testSnapshot(1))
	if err != nil || duplicate.Status != SnapshotCASAlreadyCommitted {
		t.Fatalf("duplicate bootstrap result=%+v err=%v", duplicate, err)
	}
	found, ok, err := store.LoadCurrent(ctx)
	if err != nil || !ok || found.Revision != 1 {
		t.Fatalf("load after bootstrap=%+v found=%t err=%v", found, ok, err)
	}
}

func TestMemorySnapshotStoreStrictCASAndExactReplay(t *testing.T) {
	store := NewMemoryRouteSnapshotStore()
	store.SetNow(func() int64 { return 10 })
	if _, err := store.Bootstrap(context.Background(), NestedControlOperationToken{}, testSnapshot(1)); err != nil {
		t.Fatal(err)
	}

	store.SetNow(func() int64 { return 20 })
	committed, err := store.CompareAndSwap(context.Background(), NestedControlOperationToken{}, 1, testSnapshot(2))
	if err != nil || committed.Status != SnapshotCASCommitted || committed.Stored.CommittedAtMs != 20 {
		t.Fatalf("commit=%+v err=%v", committed, err)
	}
	replayed, err := store.CompareAndSwap(context.Background(), NestedControlOperationToken{}, 2, testSnapshot(2))
	if err != nil || replayed.Status != SnapshotCASAlreadyCommitted || replayed.Stored.CommittedAtMs != 20 {
		t.Fatalf("replay=%+v err=%v", replayed, err)
	}
	if conflict, err := store.CompareAndSwap(context.Background(), NestedControlOperationToken{}, 2, testSnapshot(2)); err != nil || conflict.Status != SnapshotCASAlreadyCommitted {
		t.Fatalf("same exact replay changed classification=%+v err=%v", conflict, err)
	}
	gap, err := store.CompareAndSwap(context.Background(), NestedControlOperationToken{}, 2, testSnapshot(4))
	if err != nil || gap.Status != SnapshotCASRevisionConflict {
		t.Fatalf("gap=%+v err=%v", gap, err)
	}
	stale, err := store.CompareAndSwap(context.Background(), NestedControlOperationToken{}, 1, testSnapshot(3))
	if err != nil || stale.Status != SnapshotCASStaleExpectation {
		t.Fatalf("stale=%+v err=%v", stale, err)
	}
	different := testSnapshot(2)
	different.Realm = "other-realm"
	conflict, err := store.CompareAndSwap(context.Background(), NestedControlOperationToken{}, 2, different)
	if err != nil || conflict.Status != SnapshotCASRevisionConflict {
		t.Fatalf("same revision conflict=%+v err=%v", conflict, err)
	}
}

func TestMemorySnapshotStoreConcurrentCASHasOneCommit(t *testing.T) {
	store := NewMemoryRouteSnapshotStore()
	if _, err := store.Bootstrap(context.Background(), NestedControlOperationToken{}, testSnapshot(1)); err != nil {
		t.Fatal(err)
	}
	const workers = 64
	results := make(chan SnapshotCASResult, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			result, err := store.CompareAndSwap(context.Background(), NestedControlOperationToken{}, 1, testSnapshot(2))
			results <- result
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)

	var committed, replayed int
	for result := range results {
		switch result.Status {
		case SnapshotCASCommitted:
			committed++
		case SnapshotCASAlreadyCommitted:
			replayed++
		default:
			t.Fatalf("unexpected concurrent result=%+v", result)
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if committed != 1 || replayed != workers-1 {
		t.Fatalf("commit=%d replay=%d", committed, replayed)
	}
}

func TestMemorySnapshotStoreRejectsOverflow(t *testing.T) {
	store := NewMemoryRouteSnapshotStore()
	max := testSnapshot(^uint64(0))
	if _, err := store.Bootstrap(context.Background(), NestedControlOperationToken{}, max); err == nil {
		t.Fatal("bootstrap with max revision should not be accepted as revision 1")
	}
	store.mu.Lock()
	stored, err := CanonicalizeRouteSnapshot(max)
	if err != nil {
		store.mu.Unlock()
		t.Fatal(err)
	}
	stored.CommittedAtMs = time.Now().UnixMilli()
	store.current, store.found = stored, true
	store.mu.Unlock()
	result, err := store.CompareAndSwap(context.Background(), NestedControlOperationToken{}, ^uint64(0), testSnapshot(^uint64(0)))
	if err != nil || result.Status != SnapshotCASAlreadyCommitted {
		t.Fatalf("exact max replay=%+v err=%v", result, err)
	}
}

func TestMemorySnapshotStoreCancellationCannotCommitAfterNowHook(t *testing.T) {
	store := NewMemoryRouteSnapshotStore()
	ctx, cancel := context.WithCancel(context.Background())
	store.SetNow(func() int64 {
		cancel()
		return 99
	})
	result, err := store.Bootstrap(ctx, NestedControlOperationToken{}, testSnapshot(1))
	if !errors.Is(err, context.Canceled) || result.Status != "" {
		t.Fatalf("cancelled bootstrap result=%+v err=%v", result, err)
	}
	if _, found, err := store.LoadCurrent(context.Background()); err != nil || found {
		t.Fatalf("cancelled bootstrap changed store found=%t err=%v", found, err)
	}

	store.SetNow(func() int64 { return 1 })
	if _, err := store.Bootstrap(context.Background(), NestedControlOperationToken{}, testSnapshot(1)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	store.SetNow(func() int64 {
		cancel()
		return 100
	})
	result, err = store.CompareAndSwap(ctx, NestedControlOperationToken{}, 1, testSnapshot(2))
	if !errors.Is(err, context.Canceled) || result.Status != "" {
		t.Fatalf("cancelled CAS result=%+v err=%v", result, err)
	}
	current, found, err := store.LoadCurrent(context.Background())
	if err != nil || !found || current.Revision != 1 {
		t.Fatalf("cancelled CAS changed store=%+v found=%t err=%v", current, found, err)
	}
}
