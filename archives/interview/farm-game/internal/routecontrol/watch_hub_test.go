package routecontrol

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
)

func TestSnapshotWatchHubReconcilesCommittedNotifyFailureForNewAndExistingWatches(t *testing.T) {
	store := NewMemoryRouteSnapshotStore()
	ctx := context.Background()
	if _, err := store.Bootstrap(ctx, NestedControlOperationToken{}, testSnapshot(1)); err != nil {
		t.Fatal(err)
	}
	hub, err := NewSnapshotWatchHub(store, SnapshotWatchHubOptions{MaxClients: 8, QueueSize: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	existing, err := hub.Subscribe(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer existing.Close()

	next := testSnapshot(2)
	if result, err := store.CompareAndSwap(ctx, NestedControlOperationToken{}, 1, next); err != nil || result.Status != SnapshotCASCommitted {
		t.Fatalf("durable commit status=%v err=%v", result.Status, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := hub.Notify(canceled, next); !errors.Is(err, context.Canceled) {
		t.Fatalf("failed notification err=%v, want context canceled", err)
	}

	compensating, err := hub.Subscribe(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer compensating.Close()
	assertRevision(t, existing.Updates, 2)
	assertRevision(t, compensating.Updates, 2)
}

func TestSnapshotWatchHubNotifyRejectsEmptyDurableStoreThenAcceptsBootstrap(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryRouteSnapshotStore()
	hub, err := NewSnapshotWatchHub(store, SnapshotWatchHubOptions{
		ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()

	sub, err := hub.Subscribe(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	candidate := testSnapshot(1)
	if err := hub.Notify(ctx, candidate); !errors.Is(err, ErrSnapshotNotify) {
		t.Fatalf("empty durable Notify err=%v want ErrSnapshotNotify", err)
	}
	select {
	case update := <-sub.Updates:
		t.Fatalf("empty durable Notify fanned out revision=%d", update.Revision)
	default:
	}

	if _, err := store.Bootstrap(ctx, NestedControlOperationToken{}, candidate); err != nil {
		t.Fatal(err)
	}
	if err := hub.Notify(ctx, candidate); err != nil {
		t.Fatalf("post-bootstrap Notify err=%v", err)
	}
	assertRevision(t, sub.Updates, 1)
}

func TestSnapshotWatchHubSubscribeNotifyRaceHasNoLostRevision(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryRouteSnapshotStore()
	if _, err := store.Bootstrap(ctx, NestedControlOperationToken{}, testSnapshot(1)); err != nil {
		t.Fatal(err)
	}
	hub, err := NewSnapshotWatchHub(store, SnapshotWatchHubOptions{MaxClients: 128, QueueSize: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	existing, err := hub.Subscribe(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer existing.Close()

	for revision := uint64(2); revision <= 32; revision++ {
		next := testSnapshot(revision)
		start := make(chan struct{})
		var once sync.Once
		var subscribe *SnapshotSubscription
		var subscribeErr error
		var commitErr error
		var notifyErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			result, err := store.CompareAndSwap(ctx, NestedControlOperationToken{}, revision-1, next)
			if err != nil {
				commitErr = err
				return
			}
			if result.Status != SnapshotCASCommitted {
				commitErr = errors.New("unexpected CAS status")
				return
			}
			notifyErr = hub.Notify(ctx, next)
		}()
		go func() {
			defer wg.Done()
			<-start
			subscribe, subscribeErr = hub.Subscribe(ctx, revision-1)
		}()
		once.Do(func() { close(start) })
		wg.Wait()
		if commitErr != nil || notifyErr != nil || subscribeErr != nil {
			t.Fatalf("revision=%d commit=%v notify=%v subscribe=%v", revision, commitErr, notifyErr, subscribeErr)
		}
		assertRevision(t, existing.Updates, revision)
		assertRevision(t, subscribe.Updates, revision)
		subscribe.Close()
	}
}

func TestSnapshotWatchHubRestartLoadsDurableCurrentAndClones(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryRouteSnapshotStore()
	const targetRevision = uint64(7)
	current := testSnapshot(targetRevision)
	if _, err := store.Bootstrap(ctx, NestedControlOperationToken{}, testSnapshot(1)); err != nil {
		t.Fatal(err)
	}
	for revision := uint64(2); revision <= targetRevision; revision++ {
		current.GlobalRevision = revision
		if _, err := store.CompareAndSwap(ctx, NestedControlOperationToken{}, revision-1, current); err != nil {
			t.Fatal(err)
		}
	}
	hub, err := NewSnapshotWatchHub(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := hub.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewSnapshotWatchHub(store)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	sub, err := restarted.Subscribe(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	stored := receiveStored(t, sub.Updates)
	if stored.Revision != 7 {
		t.Fatalf("restart revision=%d want 7", stored.Revision)
	}
	stored.Snapshot.Holders[0].Endpoint = "mutated"
	stored.CanonicalJSON[0] ^= 0xff
	again, found, err := restarted.Current(ctx)
	if err != nil || !found {
		t.Fatalf("current found=%v err=%v", found, err)
	}
	if again.Snapshot.Holders[0].Endpoint == "mutated" || again.CanonicalJSON[0] == stored.CanonicalJSON[0] {
		t.Fatal("subscription exposed hub-owned mutable state")
	}
}

func TestSnapshotWatchHubDuplicateStaleSlowConsumerAndCapabilitySplit(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryRouteSnapshotStore()
	if _, err := store.Bootstrap(ctx, NestedControlOperationToken{}, testSnapshot(1)); err != nil {
		t.Fatal(err)
	}
	hub, err := NewSnapshotWatchHub(store, SnapshotWatchHubOptions{MaxClients: 8, QueueSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	sub, err := hub.Subscribe(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	if err := hub.Notify(ctx, testSnapshot(1)); err != nil {
		t.Fatalf("duplicate notify err=%v", err)
	}
	select {
	case update := <-sub.Updates:
		t.Fatalf("duplicate unexpectedly delivered revision=%d", update.Revision)
	case <-time.After(20 * time.Millisecond):
	}

	next := testSnapshot(2)
	if _, err := store.CompareAndSwap(ctx, NestedControlOperationToken{}, 1, next); err != nil {
		t.Fatal(err)
	}
	if err := hub.Notify(ctx, next); err != nil {
		t.Fatal(err)
	}
	if got := receiveStored(t, sub.Updates); got.Revision != 2 {
		t.Fatalf("first update revision=%d want 2", got.Revision)
	}
	if err := hub.Notify(ctx, testSnapshot(1)); !errors.Is(err, ErrSnapshotNotify) {
		t.Fatalf("stale notify err=%v, want ErrSnapshotNotify", err)
	}

	third := testSnapshot(3)
	if _, err := store.CompareAndSwap(ctx, NestedControlOperationToken{}, 2, third); err != nil {
		t.Fatal(err)
	}
	if err := hub.Notify(ctx, third); err != nil {
		t.Fatal(err)
	}
	fourth := testSnapshot(4)
	if _, err := store.CompareAndSwap(ctx, NestedControlOperationToken{}, 3, fourth); err != nil {
		t.Fatal(err)
	}
	if err := hub.Notify(ctx, fourth); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(sub.Err(), ErrSnapshotWatchSlowConsumer) {
		t.Fatalf("slow consumer err=%v", sub.Err())
	}
	if got := receiveStored(t, sub.Updates); got.Revision != 3 {
		t.Fatalf("queued slow-consumer revision=%d want 3", got.Revision)
	}
	if _, ok := <-sub.Updates; ok {
		t.Fatal("slow consumer channel remained open")
	}

	ordinary := hub.Notifier()
	if _, authority := ordinary.(AuthoritySnapshotNotifier); authority {
		t.Fatal("ordinary notifier exposes authority capability")
	}
	authority := hub.AuthorityNotifier()
	if _, ordinary := authority.(SnapshotNotifier); ordinary {
		t.Fatal("authority notifier exposes ordinary capability")
	}
}

func TestSnapshotWatchHubContextCloseRemovesClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := NewMemoryRouteSnapshotStore()
	if _, err := store.Bootstrap(context.Background(), NestedControlOperationToken{}, testSnapshot(1)); err != nil {
		t.Fatal(err)
	}
	hub, err := NewSnapshotWatchHub(store)
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	sub, err := hub.Subscribe(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	waitUntil(t, time.Second, func() bool { return hub.ClientCount() == 0 })
	select {
	case _, ok := <-sub.Updates:
		if ok {
			t.Fatal("context-closed subscription produced an update")
		}
	case <-time.After(time.Second):
		t.Fatal("context-closed subscription channel remained open")
	}
	if !errors.Is(sub.Err(), context.Canceled) {
		t.Fatalf("subscription err=%v, want context canceled", sub.Err())
	}
}

func assertRevision(t *testing.T, updates <-chan StoredRouteSnapshot, want uint64) {
	t.Helper()
	got := receiveStored(t, updates)
	if got.Revision != want {
		t.Fatalf("revision=%d want=%d", got.Revision, want)
	}
}

func receiveStored(t *testing.T, updates <-chan StoredRouteSnapshot) StoredRouteSnapshot {
	t.Helper()
	select {
	case got, ok := <-updates:
		if !ok {
			t.Fatal("snapshot channel closed")
		}
		return got
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for snapshot")
		return StoredRouteSnapshot{}
	}
}

func waitUntil(t *testing.T, timeout time.Duration, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func TestSnapshotWatchHubRejectsRollbackSameRevisionConflictAndDisappearance(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*mutableHubStore, StoredRouteSnapshot)
		want      error
		wantCause error
	}{
		{
			name: "rollback",
			mutate: func(store *mutableHubStore, current StoredRouteSnapshot) {
				older, err := CanonicalizeRouteSnapshot(testSnapshot(1))
				if err != nil {
					panic(err)
				}
				older.CommittedAtMs = current.CommittedAtMs
				store.set(older, true)
			},
			want: ErrSnapshotDurableRollback,
		},
		{
			name: "same revision different envelope",
			mutate: func(store *mutableHubStore, current StoredRouteSnapshot) {
				conflictSnapshot := testSnapshot(current.Revision)
				conflictSnapshot.Holders[0].Endpoint = "player-conflict:1"
				conflict, err := CanonicalizeRouteSnapshot(conflictSnapshot)
				if err != nil {
					panic(err)
				}
				conflict.CommittedAtMs = current.CommittedAtMs
				store.set(conflict, true)
			},
			want: ErrSnapshotDurableConflict,
		},
		{
			name: "key disappeared",
			mutate: func(store *mutableHubStore, _ StoredRouteSnapshot) {
				store.set(StoredRouteSnapshot{}, false)
			},
			want: ErrSnapshotDurableMissing,
		},
		{
			name: "topology change",
			mutate: func(store *mutableHubStore, current StoredRouteSnapshot) {
				nextSnapshot := testSnapshot(current.Revision + 1)
				nextSnapshot.HashSeed++
				next, err := CanonicalizeRouteSnapshot(nextSnapshot)
				if err != nil {
					panic(err)
				}
				next.CommittedAtMs = current.CommittedAtMs
				store.set(next, true)
			},
			want:      ErrSnapshotDurableContract,
			wantCause: actorrouting.ErrHashMigrationRequired,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &mutableHubStore{}
			base, err := CanonicalizeRouteSnapshot(testSnapshot(2))
			if err != nil {
				t.Fatal(err)
			}
			base.CommittedAtMs = 22
			store.set(base, true)
			hub, err := NewSnapshotWatchHub(store, SnapshotWatchHubOptions{
				ReconcileInterval: time.Hour,
			})
			if err != nil {
				t.Fatal(err)
			}
			sub, err := hub.Subscribe(context.Background(), base.Revision)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(store, base)
			_, _, err = hub.Current(context.Background())
			if !errors.Is(err, test.want) {
				t.Fatalf("Current err=%v want=%v", err, test.want)
			}
			if test.wantCause != nil && !errors.Is(err, test.wantCause) {
				t.Fatalf("Current err=%v missing cause %v", err, test.wantCause)
			}
			if !errors.Is(sub.Err(), test.want) {
				t.Fatalf("subscription err=%v want=%v", sub.Err(), test.want)
			}
			if _, _, err := hub.Current(context.Background()); !errors.Is(err, test.want) {
				t.Fatalf("terminal Current err=%v want=%v", err, test.want)
			}
			if err := hub.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSnapshotWatchHubAllowsLegalHigherRevisionSkip(t *testing.T) {
	store := &mutableHubStore{}
	base, err := CanonicalizeRouteSnapshot(testSnapshot(1))
	if err != nil {
		t.Fatal(err)
	}
	store.set(base, true)
	hub, err := NewSnapshotWatchHub(store, SnapshotWatchHubOptions{
		ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()

	higher, err := CanonicalizeRouteSnapshot(testSnapshot(3))
	if err != nil {
		t.Fatal(err)
	}
	higher.CommittedAtMs = 33
	store.set(higher, true)
	current, found, err := hub.Current(context.Background())
	if err != nil || !found || current.Revision != 3 {
		t.Fatalf("higher revision current=%+v found=%t err=%v", current, found, err)
	}
}

func TestSnapshotWatchHubAllowsSkippedRevisionWithMultipleActivations(t *testing.T) {
	store := &mutableHubStore{}
	previous := testSnapshot(10)
	previous.Player.ShardCount = 2
	previous.Player.Routes[0].State = actorrouting.ShardStateAssigning
	previous.Player.Routes[0].ShardRevision = 2
	previous.Player.Routes[0].ExpectedWriterEpoch = 2
	previous.Player.Routes[0].ExpectedLeaseEpoch = 2
	previous.Player.Routes[0].LeaseUntilMs = 0
	previous.Holders = append(previous.Holders, actorrouting.HolderRef{
		InstanceID: "player-b", BootID: "player-b-boot", Endpoint: "player-b:9002",
		State: actorrouting.HolderStateAssigning, Weight: 1,
	})
	previous.Player.Routes = append(previous.Player.Routes, actorrouting.ShardRoute{
		HolderIndex: 2, State: actorrouting.ShardStateAssigning,
		ShardRevision: 3, ExpectedWriterEpoch: 3, ExpectedLeaseEpoch: 3,
	})
	previousStored, err := CanonicalizeRouteSnapshot(previous)
	if err != nil {
		t.Fatal(err)
	}
	store.set(previousStored, true)

	hub, err := NewSnapshotWatchHub(store, SnapshotWatchHubOptions{
		ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()

	next := actorrouting.CloneRouteSnapshot(previous)
	next.GlobalRevision = 15
	next.Player.Routes[0].State = actorrouting.ShardStateActive
	next.Player.Routes[0].LeaseUntilMs = time.Now().Add(time.Hour).UnixMilli()
	next.Player.Routes[1].State = actorrouting.ShardStateActive
	next.Player.Routes[1].LeaseUntilMs = time.Now().Add(time.Hour).UnixMilli()
	next.Holders[2].State = actorrouting.HolderStateActive
	nextStored, err := CanonicalizeRouteSnapshot(next)
	if err != nil {
		t.Fatal(err)
	}
	nextStored.CommittedAtMs = 15
	store.set(nextStored, true)

	current, found, err := hub.Current(context.Background())
	if err != nil || !found || current.Revision != 15 {
		t.Fatalf("multiple activation skip current=%+v found=%t err=%v", current, found, err)
	}
}

func TestSnapshotWatchHubPeriodicReconcileRepairsNotifyFailureWithoutRequest(t *testing.T) {
	store := NewMemoryRouteSnapshotStore()
	ctx := context.Background()
	if _, err := store.Bootstrap(ctx, NestedControlOperationToken{}, testSnapshot(1)); err != nil {
		t.Fatal(err)
	}
	hub, err := NewSnapshotWatchHub(store, SnapshotWatchHubOptions{
		QueueSize:         2,
		ReconcileInterval: 5 * time.Millisecond,
		ReconcileTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	sub, err := hub.Subscribe(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	next := testSnapshot(2)
	if _, err := store.CompareAndSwap(ctx, NestedControlOperationToken{}, 1, next); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := hub.Notify(canceled, next); !errors.Is(err, context.Canceled) {
		t.Fatalf("Notify err=%v want context canceled", err)
	}
	assertRevision(t, sub.Updates, 2)
}

func TestSnapshotWatchHubFanoutClonesAtConsumptionAndBoundsPointerQueue(t *testing.T) {
	store := NewMemoryRouteSnapshotStore()
	ctx := context.Background()
	if _, err := store.Bootstrap(ctx, NestedControlOperationToken{}, testSnapshot(1)); err != nil {
		t.Fatal(err)
	}
	hub, err := NewSnapshotWatchHub(store, SnapshotWatchHubOptions{
		QueueSize:         3,
		ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer hub.Close()
	first, err := hub.Subscribe(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := hub.Subscribe(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if cap(first.queue) != 2 || cap(second.queue) != 2 {
		t.Fatalf("pointer queue capacities=%d/%d want 2/2", cap(first.queue), cap(second.queue))
	}
	next := testSnapshot(2)
	if _, err := store.CompareAndSwap(ctx, NestedControlOperationToken{}, 1, next); err != nil {
		t.Fatal(err)
	}
	if err := hub.Notify(ctx, next); err != nil {
		t.Fatal(err)
	}
	firstSnapshot := receiveStored(t, first.Updates)
	secondSnapshot := receiveStored(t, second.Updates)
	firstSnapshot.Snapshot.Holders[0].Endpoint = "mutated"
	firstSnapshot.CanonicalJSON[0] ^= 0xff
	if secondSnapshot.Snapshot.Holders[0].Endpoint == "mutated" ||
		secondSnapshot.CanonicalJSON[0] == firstSnapshot.CanonicalJSON[0] {
		t.Fatal("fanout exposed mutable shared snapshot state")
	}
}

func TestSnapshotWatchHubCloseDoesNotWaitBehindBlockedStoreLoad(t *testing.T) {
	store := &mutableHubStore{}
	base, err := CanonicalizeRouteSnapshot(testSnapshot(1))
	if err != nil {
		t.Fatal(err)
	}
	store.set(base, true)
	hub, err := NewSnapshotWatchHub(store, SnapshotWatchHubOptions{
		ReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	store.blockLoads()
	loadDone := make(chan struct{})
	go func() {
		_, _, _ = hub.Current(context.Background())
		close(loadDone)
	}()
	<-store.loadStarted
	clientCountDone := make(chan struct{})
	go func() {
		_ = hub.ClientCount()
		close(clientCountDone)
	}()
	select {
	case <-clientCountDone:
	case <-time.After(time.Second):
		t.Fatal("diagnostic ClientCount waited behind blocked store load")
	}
	closeDone := make(chan struct{})
	go func() {
		_ = hub.Close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close waited behind blocked store load")
	}
	close(store.loadRelease)
	select {
	case <-loadDone:
	case <-time.After(time.Second):
		t.Fatal("blocked store load did not converge after release")
	}
}

type mutableHubStore struct {
	mu            sync.Mutex
	current       StoredRouteSnapshot
	found         bool
	loadStarted   chan struct{}
	loadRelease   chan struct{}
	loadStartOnce sync.Once
}

func (s *mutableHubStore) LoadCurrent(ctx context.Context) (StoredRouteSnapshot, bool, error) {
	if s.loadStarted != nil {
		s.loadStartOnce.Do(func() { close(s.loadStarted) })
		select {
		case <-s.loadRelease:
		case <-ctx.Done():
			return StoredRouteSnapshot{}, false, ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.found {
		return StoredRouteSnapshot{}, false, nil
	}
	return s.current.Clone(), true, nil
}

func (s *mutableHubStore) CompareAndSwap(context.Context, NestedControlOperationToken, uint64, actorrouting.RouteSnapshot) (SnapshotCASResult, error) {
	return SnapshotCASResult{}, errors.New("mutable hub store: CAS not used")
}

func (s *mutableHubStore) set(current StoredRouteSnapshot, found bool) {
	s.mu.Lock()
	s.current = current.Clone()
	s.found = found
	s.mu.Unlock()
}

func (s *mutableHubStore) blockLoads() {
	s.loadStarted = make(chan struct{})
	s.loadRelease = make(chan struct{})
}
