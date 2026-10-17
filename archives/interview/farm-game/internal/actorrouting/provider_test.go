package actorrouting

import (
	"context"
	"errors"
	"farmgame/internal/controlfence"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStaticAssignmentProviderGetSnapshot(t *testing.T) {
	snap := completeSnapshot()
	provider, err := NewStaticAssignmentProvider(snap)
	if err != nil {
		t.Fatal(err)
	}
	got, err := provider.GetSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Realm != snap.Realm || got.HashVersion != HashVersionXXH64V2 || got.HashSeed != testHashSeed || got.Player.ShardCount != 4 {
		t.Fatalf("GetSnapshot: %+v", got)
	}
	// Invalid snapshot rejected at construction (fail-closed).
	bad := snap
	bad.Farm.Routes = bad.Farm.Routes[:3]
	if _, err := NewStaticAssignmentProvider(bad); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("incomplete snapshot err=%v", err)
	}
}

func TestLegacyDeploymentRouteSnapshotsFailClosedWithoutAuthorityGeneration(t *testing.T) {
	for _, path := range []string{
		"../../deploy/actor-route.static.example.json",
		"../../deploy/route-snapshot.player.json",
	} {
		t.Run(path, func(t *testing.T) {
			if _, err := LoadStaticAssignmentProvider(path); !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("legacy deployment snapshot must fail closed: %v", err)
			}
		})
	}
}

func TestStaticAssignmentProviderAndResolverSnapshotsAreImmutable(t *testing.T) {
	snap := completeSnapshot()
	provider, err := NewStaticAssignmentProvider(snap)
	if err != nil {
		t.Fatal(err)
	}

	// Mutating the caller-owned input after construction must not alter provider.
	snap.Holders[0].InstanceID = "mutated-input"
	got, err := provider.GetSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Holders[0].InstanceID != "player-a" {
		t.Fatalf("provider retained caller slice: %+v", got.Holders[0])
	}

	// Mutating one returned snapshot must not alter the next return.
	got.Holders[0].InstanceID = "mutated-return"
	again, err := provider.GetSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if again.Holders[0].InstanceID != "player-a" {
		t.Fatalf("provider exposed mutable nested slice: %+v", again.Holders[0])
	}

	resolver, err := NewStaticResolver(again)
	if err != nil {
		t.Fatal(err)
	}
	view := resolver.Snapshot()
	view.Farm.Routes[0].ExpectedWriterEpoch = 999
	if resolver.Snapshot().Farm.Routes[0].ExpectedWriterEpoch == 999 {
		t.Fatal("resolver Snapshot exposed mutable active table")
	}
}

func TestStaticAssignmentProviderWatchClosesOnCancel(t *testing.T) {
	provider, err := NewStaticAssignmentProvider(completeSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := provider.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("static Watch must not push snapshots")
		}
	case <-time.After(time.Second):
		t.Fatal("Watch channel did not close after cancel")
	}
}

func TestStaticResolverReplaceRejectsInvalidKeepsOld(t *testing.T) {
	resolver, err := NewStaticResolver(completeSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	before, err := resolver.ResolveActor(context.Background(), DomainPlayer, 10001)
	if err != nil {
		t.Fatal(err)
	}

	bad := completeSnapshot()
	bad.GlobalRevision = 2
	bad.Holders[0].Endpoint = "" // invalid payload, same routing layout
	if err := resolver.Replace(bad); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("Replace invalid err=%v", err)
	}
	after, err := resolver.ResolveActor(context.Background(), DomainPlayer, 10001)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("old table must remain: before=%+v after=%+v", before, after)
	}
}

func TestStaticResolverReplaceRejectsRoutingLayoutMigration(t *testing.T) {
	base := completeSnapshot()
	resolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}

	tests := map[string]func(*RouteSnapshot){
		"seed":    func(s *RouteSnapshot) { s.HashSeed++ },
		"version": func(s *RouteSnapshot) { s.HashVersion++ },
		"player shard count": func(s *RouteSnapshot) {
			s.Player.ShardCount = 2
			s.Player.Routes = append([]ShardRoute(nil), s.Player.Routes[:2]...)
		},
		"farm shard count": func(s *RouteSnapshot) {
			s.Farm.ShardCount = 2
			s.Farm.Routes = append([]ShardRoute(nil), s.Farm.Routes[:2]...)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			next := cloneSnapshot(base)
			next.GlobalRevision++
			mutate(&next)
			if err := resolver.Replace(next); !errors.Is(err, ErrHashMigrationRequired) {
				t.Fatalf("layout change err=%v", err)
			}
		})
	}
}

func TestStaticResolverReplaceRejectsRevisionRollbackAndConflict(t *testing.T) {
	base := completeSnapshot()
	base.GlobalRevision = 5
	resolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}

	stale := cloneSnapshot(base)
	stale.GlobalRevision = 4
	if err := resolver.Replace(stale); !errors.Is(err, ErrStaleRoute) {
		t.Fatalf("revision rollback err=%v", err)
	}

	conflict := cloneSnapshot(base)
	conflict.Holders[0].Endpoint = "conflict:9100"
	if err := resolver.Replace(conflict); !errors.Is(err, ErrSnapshotRevisionConflict) {
		t.Fatalf("same revision conflict err=%v", err)
	}
	if got := resolver.Snapshot(); got.Holders[0].Endpoint != base.Holders[0].Endpoint {
		t.Fatalf("rejected replacement changed active snapshot: %+v", got.Holders[0])
	}
}

func TestSnapshotApplierGapRejectsStaleFullReload(t *testing.T) {
	base := completeSnapshot()
	base.GlobalRevision = 3
	provider := &countingProvider{snapshot: base}
	resolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewSnapshotApplier(provider, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if err := applier.Apply(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	push := cloneSnapshot(base)
	push.GlobalRevision = 7
	if err := applier.Apply(context.Background(), push); !errors.Is(err, ErrStaleRoute) {
		t.Fatalf("stale gap reload err=%v", err)
	}
	if applier.LastGlobalRevision() != 3 {
		t.Fatalf("stale full snapshot downgraded revision to %d", applier.LastGlobalRevision())
	}
}

func TestStaticResolverConcurrentReplaceAndResolve(t *testing.T) {
	base := completeSnapshot()
	base.Player = DomainSnapshot{ShardCount: 1, Routes: []ShardRoute{{HolderIndex: 0, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: activeLeaseUntil()}}}
	base.Farm = DomainSnapshot{ShardCount: 1, Routes: []ShardRoute{{HolderIndex: 2, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: activeLeaseUntil()}}}
	resolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for reader := 0; reader < 8; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				assignment, err := resolver.ResolveActor(context.Background(), DomainFarm, uint64(i+1))
				if err != nil {
					t.Errorf("resolve: %v", err)
					return
				}
				if assignment.HolderPod != "farm-a" && assignment.HolderPod != "farm-b" {
					t.Errorf("torn holder: %+v", assignment)
					return
				}
			}
		}()
	}
	for revision := uint64(2); revision <= 200; revision++ {
		next := cloneSnapshot(base)
		next.GlobalRevision = revision
		next.Farm.Routes[0].HolderIndex = 2 + int(revision%2)
		next.Farm.Routes[0].ShardRevision = revision
		next.Farm.Routes[0].ExpectedWriterEpoch = revision
		next.Farm.Routes[0].ExpectedLeaseEpoch = revision
		if err := resolver.Replace(next); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}

func TestStaticResolverConcurrentWritersNeverRollback(t *testing.T) {
	for iteration := 0; iteration < 500; iteration++ {
		base := completeSnapshot()
		base.GlobalRevision = 1
		resolver, err := NewStaticResolver(base)
		if err != nil {
			t.Fatal(err)
		}

		revision2 := cloneSnapshot(base)
		revision2.GlobalRevision = 2
		revision2.Player.Routes[0].ShardRevision = 2
		revision2.Player.Routes[0].ExpectedWriterEpoch = 2
		revision3 := cloneSnapshot(base)
		revision3.GlobalRevision = 3
		revision3.Player.Routes[0].ShardRevision = 3
		revision3.Player.Routes[0].ExpectedWriterEpoch = 3

		start := make(chan struct{})
		var revision2Err, revision3Err error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			revision2Err = resolver.Replace(revision2)
		}()
		go func() {
			defer wg.Done()
			<-start
			revision3Err = resolver.Replace(revision3)
		}()
		close(start)
		wg.Wait()

		if revision3Err != nil {
			t.Fatalf("iteration %d: newest revision rejected: %v", iteration, revision3Err)
		}
		if revision2Err != nil && !errors.Is(revision2Err, ErrStaleRoute) {
			t.Fatalf("iteration %d: revision 2 err=%v", iteration, revision2Err)
		}
		if got := resolver.Snapshot().GlobalRevision; got != 3 {
			t.Fatalf("iteration %d: concurrent install rolled back to revision %d", iteration, got)
		}
	}
}

func cloneSnapshot(s RouteSnapshot) RouteSnapshot {
	out := s
	out.Holders = append([]HolderRef(nil), s.Holders...)
	out.Player.Routes = append([]ShardRoute(nil), s.Player.Routes...)
	out.Farm.Routes = append([]ShardRoute(nil), s.Farm.Routes...)
	return out
}

func TestStaticResolverReplaceSwapsHolderAndEpoch(t *testing.T) {
	snap := completeSnapshot()
	snap.Player = DomainSnapshot{
		ShardCount: 1,
		Routes:     []ShardRoute{{HolderIndex: 0, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: activeLeaseUntil()}},
	}
	snap.Farm = DomainSnapshot{
		ShardCount: 1,
		Routes:     []ShardRoute{{HolderIndex: 2, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: activeLeaseUntil()}},
	}
	resolver, err := NewStaticResolver(snap)
	if err != nil {
		t.Fatal(err)
	}
	before, err := resolver.ResolveActor(context.Background(), DomainPlayer, 42)
	if err != nil {
		t.Fatal(err)
	}
	if before.HolderPod != "player-a" || before.WriterEpoch != 1 {
		t.Fatalf("before=%+v", before)
	}

	next := cloneSnapshot(snap)
	next.GlobalRevision = 2
	next.Holders[0] = HolderRef{InstanceID: "player-z", BootID: "player-z-boot", Endpoint: "player-z:9100", State: HolderStateActive, Weight: 1}
	next.Player.Routes[0].ExpectedWriterEpoch = 99
	next.Player.Routes[0].ShardRevision = 7
	next.Player.Routes[0].ExpectedLeaseEpoch = 7
	if err := resolver.Replace(next); err != nil {
		t.Fatal(err)
	}
	after, err := resolver.ResolveActor(context.Background(), DomainPlayer, 42)
	if err != nil {
		t.Fatal(err)
	}
	if after.HolderPod != "player-z" || after.WriterEpoch != 99 || after.ShardRevision != 7 {
		t.Fatalf("Replace did not swap table: %+v", after)
	}
}

type countingProvider struct {
	snapshot   RouteSnapshot
	getCalls   atomic.Uint64
	watchSnaps []RouteSnapshot
}

func (p *countingProvider) GetSnapshot(context.Context) (RouteSnapshot, error) {
	p.getCalls.Add(1)
	return p.snapshot, nil
}

func (p *countingProvider) Watch(ctx context.Context) (<-chan RouteSnapshot, error) {
	ch := make(chan RouteSnapshot, len(p.watchSnaps))
	for _, s := range p.watchSnaps {
		ch <- s
	}
	close(ch)
	return ch, nil
}

func TestSnapshotApplierRevisionGapTriggersGetSnapshot(t *testing.T) {
	base := completeSnapshot()
	base.GlobalRevision = 1
	base.Player = DomainSnapshot{
		ShardCount: 1,
		Routes:     []ShardRoute{{HolderIndex: 0, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: activeLeaseUntil()}},
	}
	base.Farm = DomainSnapshot{
		ShardCount: 1,
		Routes:     []ShardRoute{{HolderIndex: 2, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: activeLeaseUntil()}},
	}

	full := cloneSnapshot(base)
	full.GlobalRevision = 5
	full.Holders[0] = HolderRef{InstanceID: "player-gap", BootID: "player-gap-boot", Endpoint: "player-gap:9100", State: HolderStateActive, Weight: 1}
	full.Player.Routes[0].ShardRevision = 55
	full.Player.Routes[0].ExpectedWriterEpoch = 55
	full.Player.Routes[0].ExpectedLeaseEpoch = 55

	provider := &countingProvider{snapshot: full}
	resolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewSnapshotApplier(provider, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if err := applier.Apply(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	if provider.getCalls.Load() != 0 {
		t.Fatalf("bootstrap Apply must not call GetSnapshot, got %d", provider.getCalls.Load())
	}
	if applier.LastGlobalRevision() != 1 {
		t.Fatalf("last=%d", applier.LastGlobalRevision())
	}

	// Gap: jump from 1 to 4 (skipping 2,3) → must reload via GetSnapshot.
	gapPush := base
	gapPush.GlobalRevision = 4
	if err := applier.Apply(context.Background(), gapPush); err != nil {
		t.Fatal(err)
	}
	if provider.getCalls.Load() != 1 {
		t.Fatalf("gap must call GetSnapshot once, got %d", provider.getCalls.Load())
	}
	if applier.GapReloads.Load() != 1 {
		t.Fatalf("GapReloads=%d", applier.GapReloads.Load())
	}
	if applier.LastGlobalRevision() != 5 {
		t.Fatalf("after gap last want 5 got %d", applier.LastGlobalRevision())
	}
	got, err := resolver.ResolveActor(context.Background(), DomainPlayer, 42)
	if err != nil {
		t.Fatal(err)
	}
	if got.HolderPod != "player-gap" || got.WriterEpoch != 55 {
		t.Fatalf("gap reload did not install full snapshot: %+v", got)
	}
}

func TestSnapshotApplierIgnoresStaleAndDuplicate(t *testing.T) {
	base := completeSnapshot()
	base.GlobalRevision = 3
	provider := &countingProvider{snapshot: base}
	resolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewSnapshotApplier(provider, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if err := applier.Apply(context.Background(), base); err != nil {
		t.Fatal(err)
	}

	stale := base
	stale.GlobalRevision = 2
	if err := applier.Apply(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	dup := base
	dup.GlobalRevision = 3
	if err := applier.Apply(context.Background(), dup); err != nil {
		t.Fatal(err)
	}
	if applier.StaleIgnored.Load() != 1 || applier.Duplicates.Load() != 1 {
		t.Fatalf("stale=%d dup=%d", applier.StaleIgnored.Load(), applier.Duplicates.Load())
	}
	if provider.getCalls.Load() != 0 {
		t.Fatalf("stale/dup must not GetSnapshot, got %d", provider.getCalls.Load())
	}
	if applier.LastGlobalRevision() != 3 {
		t.Fatalf("last=%d", applier.LastGlobalRevision())
	}
}

func TestSnapshotApplierOnAppliedFiresAfterSuccessfulApply(t *testing.T) {
	base := completeSnapshot()
	base.GlobalRevision = 4
	provider := &countingProvider{snapshot: base}
	resolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewSnapshotApplier(provider, resolver)
	if err != nil {
		t.Fatal(err)
	}
	var applied []uint64
	applier.OnApplied = func(revision uint64) {
		applied = append(applied, revision)
	}
	if err := applier.Apply(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	next := cloneSnapshot(base)
	next.GlobalRevision = 5
	next.Player.Routes[0].ShardRevision++
	next.Player.Routes[0].ExpectedWriterEpoch++
	next.Player.Routes[0].ExpectedLeaseEpoch++
	if err := applier.Apply(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	stale := cloneSnapshot(base)
	stale.GlobalRevision = 3
	if err := applier.Apply(context.Background(), stale); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(applied, []uint64{4, 5}) {
		t.Fatalf("applied=%v", applied)
	}
}

func TestDynamicAssignmentProviderWatchFromPublishesFullSnapshots(t *testing.T) {
	base := completeSnapshot()
	provider, publisher, err := NewDynamicAssignmentProvider(base, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates, err := provider.WatchFrom(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	initial := <-updates
	if initial.Revision != base.GlobalRevision || !reflect.DeepEqual(initial.Snapshot, base) {
		t.Fatalf("initial full update=%+v", initial)
	}

	next := cloneSnapshot(base)
	next.GlobalRevision = base.GlobalRevision + 1
	next.Player.Routes[0].ShardRevision++
	next.Player.Routes[0].ExpectedWriterEpoch++
	next.Player.Routes[0].ExpectedLeaseEpoch = 9
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, next); err != nil {
		t.Fatal(err)
	}
	update := <-updates
	if update.Revision != next.GlobalRevision || !reflect.DeepEqual(update.Snapshot, next) {
		t.Fatalf("published update=%+v", update)
	}

	// A same-revision replay is idempotent, while a conflicting replay is
	// rejected without mutating the installed full snapshot.
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, next); err != nil {
		t.Fatalf("same snapshot replay: %v", err)
	}
	select {
	case duplicate, ok := <-updates:
		if ok {
			t.Fatalf("identical revision replay notified watcher: %+v", duplicate)
		}
		t.Fatal("identical revision replay closed watcher")
	case <-time.After(20 * time.Millisecond):
	}
	followup := cloneSnapshot(next)
	followup.GlobalRevision++
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, followup); err != nil {
		t.Fatalf("follow-up publish after identical replay: %v", err)
	}
	if update := <-updates; update.Revision != followup.GlobalRevision || !reflect.DeepEqual(update.Snapshot, followup) {
		t.Fatalf("watcher did not survive identical replay: %+v", update)
	}
	conflict := cloneSnapshot(followup)
	conflict.Holders[0].Endpoint = "conflict:9100"
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, conflict); !errors.Is(err, ErrSnapshotRevisionConflict) {
		t.Fatalf("same revision conflict=%v", err)
	}
	got := provider.CurrentSnapshot()
	if got.Holders[0].Endpoint != next.Holders[0].Endpoint {
		t.Fatalf("conflicting publish changed current snapshot=%+v", got)
	}
}

func TestDynamicAssignmentProviderClosesSlowSubscriber(t *testing.T) {
	base := completeSnapshot()
	provider, publisher, err := NewDynamicAssignmentProvider(base, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	updates, err := provider.WatchFrom(ctx, base.GlobalRevision)
	if err != nil {
		t.Fatal(err)
	}
	for revision := base.GlobalRevision + 1; revision <= base.GlobalRevision+2; revision++ {
		next := cloneSnapshot(base)
		next.GlobalRevision = revision
		if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, next); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok := <-updates; !ok {
		t.Fatal("slow subscriber closed before its buffered update was observed")
	}
	select {
	case _, ok := <-updates:
		if ok {
			t.Fatal("slow subscriber must close after queue overflow")
		}
	case <-time.After(time.Second):
		t.Fatal("slow subscriber was not closed")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && provider.state.watcherCleanup.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	if provider.state.watcherCleanup.Load() == 0 {
		t.Fatal("slow subscriber cleanup goroutine did not exit after channel close")
	}
	provider.state.mu.Lock()
	watchCount := len(provider.state.watches)
	provider.state.mu.Unlock()
	if watchCount != 0 {
		t.Fatalf("closed slow subscriber remained registered: %d", watchCount)
	}
}

func TestDynamicAssignmentProviderRejectsPerShardGenerationRollback(t *testing.T) {
	base := completeSnapshot()
	base.Player.Routes[0].ShardRevision = 7
	base.Player.Routes[0].ExpectedWriterEpoch = 9
	base.Player.Routes[0].ExpectedLeaseEpoch = 11
	provider, publisher, err := NewDynamicAssignmentProvider(base, 2)
	if err != nil {
		t.Fatal(err)
	}
	next := cloneSnapshot(base)
	next.GlobalRevision++
	next.Player.Routes[0].ShardRevision = 6
	next.Player.Routes[0].ExpectedWriterEpoch = 8
	next.Player.Routes[0].ExpectedLeaseEpoch = 10
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, next); !errors.Is(err, ErrSnapshotGenerationRollback) {
		t.Fatalf("per-shard rollback err=%v", err)
	}
	if got := provider.CurrentSnapshot(); got.GlobalRevision != base.GlobalRevision || got.Player.Routes[0].ShardRevision != 7 {
		t.Fatalf("rejected rollback mutated provider: %+v", got.Player.Routes[0])
	}
}

func TestDynamicAssignmentProviderRejectsSameGenerationLeaseDeadlineRollback(t *testing.T) {
	base := completeSnapshot()
	provider, publisher, err := NewDynamicAssignmentProvider(base, 2)
	if err != nil {
		t.Fatal(err)
	}
	next := cloneSnapshot(base)
	next.GlobalRevision++
	next.Player.Routes[0].LeaseUntilMs = base.Player.Routes[0].LeaseUntilMs - 1
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, next); !errors.Is(err, ErrSnapshotGenerationRollback) {
		t.Fatalf("same-generation lease deadline rollback err=%v", err)
	}
	if got := provider.CurrentSnapshot(); got.GlobalRevision != base.GlobalRevision || got.Player.Routes[0].LeaseUntilMs != base.Player.Routes[0].LeaseUntilMs {
		t.Fatalf("deadline rollback changed live provider: %+v", got.Player.Routes[0])
	}
}

func TestDynamicAssignmentProviderRejectsSameGenerationActiveToAssigningRollback(t *testing.T) {
	base := completeSnapshot()
	provider, publisher, err := NewDynamicAssignmentProvider(base, 2)
	if err != nil {
		t.Fatal(err)
	}
	next := cloneSnapshot(base)
	next.GlobalRevision++
	next.Player.Routes[0].State = ShardStateAssigning
	next.Player.Routes[0].LeaseUntilMs = 0
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, next); !errors.Is(err, ErrSnapshotGenerationRollback) {
		t.Fatalf("same-generation active->assigning rollback err=%v", err)
	}
	if got := provider.CurrentSnapshot(); got.GlobalRevision != base.GlobalRevision || got.Player.Routes[0].State != ShardStateActive {
		t.Fatalf("state rollback changed live provider: %+v", got.Player.Routes[0])
	}
}

func TestStaticProviderAndSnapshotApplierRejectCanceledContextBeforeInstall(t *testing.T) {
	base := completeSnapshot()
	static, err := NewStaticAssignmentProvider(base)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := static.GetSnapshot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("static GetSnapshot ignored cancellation: %v", err)
	}
	if _, err := static.WatchFrom(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("static WatchFrom ignored cancellation: %v", err)
	}
	dynamic, _, err := NewDynamicAssignmentProvider(base, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dynamic.WatchFrom(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("dynamic WatchFrom ignored cancellation: %v", err)
	}
	resolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewSnapshotApplier(static, resolver)
	if err != nil {
		t.Fatal(err)
	}
	next := cloneSnapshot(base)
	next.GlobalRevision++
	if err := applier.Apply(ctx, next); !errors.Is(err, context.Canceled) {
		t.Fatalf("SnapshotApplier installed after cancellation: %v", err)
	}
	if got := resolver.Snapshot(); got.GlobalRevision != base.GlobalRevision {
		t.Fatalf("canceled applier changed resolver revision: %d", got.GlobalRevision)
	}
}

type cancelAfterProviderReturn struct {
	snapshot RouteSnapshot
	started  chan struct{}
	release  chan struct{}
	cancel   context.CancelFunc
}

func (p *cancelAfterProviderReturn) GetSnapshot(context.Context) (RouteSnapshot, error) {
	close(p.started)
	<-p.release
	p.cancel()
	return cloneSnapshot(p.snapshot), nil
}

func (*cancelAfterProviderReturn) Watch(context.Context) (<-chan RouteSnapshot, error) {
	ch := make(chan RouteSnapshot)
	close(ch)
	return ch, nil
}

func TestNewStaticResolverFromProviderRechecksCancellationAfterProviderReturn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	provider := &cancelAfterProviderReturn{snapshot: completeSnapshot(), started: make(chan struct{}), release: make(chan struct{}), cancel: cancel}
	done := make(chan error, 1)
	go func() {
		_, err := NewStaticResolverFromProvider(ctx, provider)
		done <- err
	}()
	<-provider.started
	close(provider.release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("resolver constructed after provider-return cancellation: %v", err)
	}
}

func TestSnapshotApplierCancellationDuringReplacementDoesNotInstallOrAdvance(t *testing.T) {
	base := completeSnapshot()
	provider, err := NewStaticAssignmentProvider(base)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewSnapshotApplier(provider, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if err := applier.Apply(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	next := cloneSnapshot(base)
	next.GlobalRevision++
	next.Player.Routes[0].LeaseUntilMs++
	installReached := make(chan struct{})
	installRelease := make(chan struct{})
	resolver.beforeInstall = func() {
		close(installReached)
		<-installRelease
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- applier.Apply(ctx, next) }()
	<-installReached
	cancel()
	close(installRelease)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("replacement cancellation err=%v", err)
	}
	if got := resolver.Snapshot().GlobalRevision; got != base.GlobalRevision {
		t.Fatalf("canceled replacement installed revision %d", got)
	}
	if got := applier.LastGlobalRevision(); got != base.GlobalRevision {
		t.Fatalf("canceled replacement advanced applier revision %d", got)
	}
	if got := applier.Applied.Load(); got != 1 {
		t.Fatalf("canceled replacement advanced applied counter to %d", got)
	}
}

func TestSameGenerationActivationRequiresManagerWriteCapability(t *testing.T) {
	base := completeSnapshot()
	base.Player.Routes[0].State = ShardStateAssigning
	base.Player.Routes[0].ShardRevision = 2
	base.Player.Routes[0].ExpectedWriterEpoch = 2
	base.Player.Routes[0].ExpectedLeaseEpoch = 2
	next := cloneSnapshot(base)
	next.GlobalRevision++
	next.Player.Routes[0].State = ShardStateActive

	provider, publisher, err := NewDynamicAssignmentProvider(base, 2)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewSnapshotApplier(provider, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, next); !errors.Is(err, ErrSnapshotGenerationRollback) {
		t.Fatalf("unproved provider activation err=%v", err)
	}
	if err := applier.Apply(context.Background(), next); !errors.Is(err, ErrSnapshotGenerationRollback) {
		t.Fatalf("unproved resolver activation err=%v", err)
	}
	if provider.CurrentSnapshot().GlobalRevision != base.GlobalRevision || resolver.Snapshot().GlobalRevision != base.GlobalRevision {
		t.Fatal("unproved activation changed provider or resolver")
	}

	watchCtx, cancelWatch := context.WithCancel(context.Background())
	defer cancelWatch()
	updates, err := provider.WatchFrom(watchCtx, base.GlobalRevision)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.PublishAuthoritySnapshot(context.Background(), controlfence.NestedControlOperationToken{}, next); err != nil {
		t.Fatal(err)
	}
	update := <-updates
	if err := applier.apply(context.Background(), update.Snapshot, true); err != nil {
		t.Fatal(err)
	}
	if provider.CurrentSnapshot().GlobalRevision != next.GlobalRevision || resolver.Snapshot().GlobalRevision != next.GlobalRevision {
		t.Fatal("manager-owned activation did not traverse provider and resolver")
	}
}

type reconnectReloadProvider struct {
	read          *DynamicAssignmentProvider
	getCalls      atomic.Uint64
	watchCalls    atomic.Uint64
	reloadStarted chan struct{}
	reloadRelease chan struct{}
	reloadOnce    sync.Once
}

func (p *reconnectReloadProvider) GetSnapshot(ctx context.Context) (RouteSnapshot, error) {
	call := p.getCalls.Add(1)
	if call == 2 {
		p.reloadOnce.Do(func() { close(p.reloadStarted) })
		select {
		case <-p.reloadRelease:
		case <-ctx.Done():
			return RouteSnapshot{}, ctx.Err()
		}
	}
	return p.read.GetSnapshot(ctx)
}

func (p *reconnectReloadProvider) Watch(ctx context.Context) (<-chan RouteSnapshot, error) {
	return p.read.Watch(ctx)
}

func (p *reconnectReloadProvider) WatchFrom(ctx context.Context, afterRevision uint64) (<-chan AssignmentUpdate, error) {
	if p.watchCalls.Add(1) == 1 {
		ch := make(chan AssignmentUpdate)
		close(ch)
		return ch, nil
	}
	return p.read.WatchFrom(ctx, afterRevision)
}

func TestSnapshotApplierReconnectFullReloadConvergesAfterMissedActivationAndLaterRevision(t *testing.T) {
	base := completeSnapshot()
	base.Player.Routes[0].State = ShardStateAssigning
	base.Player.Routes[0].ShardRevision = 2
	base.Player.Routes[0].ExpectedWriterEpoch = 2
	base.Player.Routes[0].ExpectedLeaseEpoch = 2
	read, publisher, err := NewDynamicAssignmentProvider(base, 2)
	if err != nil {
		t.Fatal(err)
	}
	provider := &reconnectReloadProvider{
		read: read, reloadStarted: make(chan struct{}), reloadRelease: make(chan struct{}),
	}
	resolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewSnapshotApplier(provider, resolver)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- applier.Run(ctx) }()

	select {
	case <-provider.reloadStarted:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("watch closure did not trigger full reload")
	}
	active := cloneSnapshot(base)
	active.GlobalRevision++
	active.Player.Routes[0].State = ShardStateActive
	active.Player.Routes[0].ShardRevision++
	active.Player.Routes[0].ExpectedWriterEpoch++
	active.Player.Routes[0].ExpectedLeaseEpoch++
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, active); err != nil {
		cancel()
		t.Fatal(err)
	}
	later := cloneSnapshot(active)
	later.GlobalRevision++
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, later); err != nil {
		cancel()
		t.Fatal(err)
	}
	close(provider.reloadRelease)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if got := resolver.Snapshot(); got.GlobalRevision == later.GlobalRevision && got.Player.Routes[0].State == ShardStateActive {
			break
		}
		time.Sleep(time.Millisecond)
	}
	got := resolver.Snapshot()
	if got.GlobalRevision != later.GlobalRevision || got.Player.Routes[0].State != ShardStateActive {
		cancel()
		t.Fatalf("reconnect full reload did not converge: %+v", got.Player.Routes[0])
	}
	if provider.getCalls.Load() < 2 || applier.Reconnects.Load() == 0 {
		cancel()
		t.Fatalf("reconnect did not use GetSnapshot: gets=%d reconnects=%d", provider.getCalls.Load(), applier.Reconnects.Load())
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run exit=%v", err)
	}
}

func TestTrustedProviderBootstrapAndGapReloadActivationWhileDirectRedirectFailsClosed(t *testing.T) {
	base := completeSnapshot()
	base.Player.Routes[0].State = ShardStateAssigning
	base.Player.Routes[0].ShardRevision = 2
	base.Player.Routes[0].ExpectedWriterEpoch = 2
	base.Player.Routes[0].ExpectedLeaseEpoch = 2
	read, publisher, err := NewDynamicAssignmentProvider(base, 2)
	if err != nil {
		t.Fatal(err)
	}
	active := cloneSnapshot(base)
	active.GlobalRevision++
	active.Player.Routes[0].State = ShardStateActive
	if err := publisher.PublishAuthoritySnapshot(context.Background(), controlfence.NestedControlOperationToken{}, active); err != nil {
		t.Fatal(err)
	}
	later := cloneSnapshot(active)
	later.GlobalRevision++
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, later); err != nil {
		t.Fatal(err)
	}

	bootstrapResolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapApplier, err := NewSnapshotApplier(read, bootstrapResolver)
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrapApplier.Bootstrap(context.Background()); err != nil {
		t.Fatalf("trusted Bootstrap: %v", err)
	}
	if got := bootstrapResolver.Snapshot(); got.GlobalRevision != later.GlobalRevision || got.Player.Routes[0].State != ShardStateActive {
		t.Fatalf("Bootstrap did not install current ACTIVE snapshot: %+v", got.Player.Routes[0])
	}

	gapResolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	gapApplier, err := NewSnapshotApplier(read, gapResolver)
	if err != nil {
		t.Fatal(err)
	}
	if err := gapApplier.Apply(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	if err := gapApplier.Apply(context.Background(), later); err != nil {
		t.Fatalf("trusted gap reload: %v", err)
	}
	if got := gapResolver.Snapshot(); got.GlobalRevision != later.GlobalRevision || got.Player.Routes[0].State != ShardStateActive || gapApplier.GapReloads.Load() != 1 {
		t.Fatalf("gap reload did not install current ACTIVE snapshot: gap=%d route=%+v", gapApplier.GapReloads.Load(), got.Player.Routes[0])
	}

	redirectBase := cloneSnapshot(base)
	redirectBase.GlobalRevision = later.GlobalRevision - 1
	redirectResolver, err := NewStaticResolver(redirectBase)
	if err != nil {
		t.Fatal(err)
	}
	redirectApplier, err := NewRedirectApplier(redirectResolver)
	if err != nil {
		t.Fatal(err)
	}
	redirect := RouteRedirect{Domain: DomainPlayer, ShardID: 0, GlobalRevision: later.GlobalRevision, Snapshot: &later}
	if err := redirectApplier.ApplyRedirect(context.Background(), redirect); !errors.Is(err, ErrSnapshotGenerationRollback) {
		t.Fatalf("untrusted redirect installed provider-equivalent activation: %v", err)
	}
	if got := redirectResolver.Snapshot(); got.GlobalRevision != redirectBase.GlobalRevision || got.Player.Routes[0].State != ShardStateAssigning {
		t.Fatalf("failed redirect changed resolver: %+v", got.Player.Routes[0])
	}
}

func TestDynamicAssignmentProviderRejectsGlobalRevisionGap(t *testing.T) {
	base := completeSnapshot()
	base.GlobalRevision = 10
	provider, publisher, err := NewDynamicAssignmentProvider(base, 2)
	if err != nil {
		t.Fatal(err)
	}
	next := cloneSnapshot(base)
	next.GlobalRevision = 12
	if err := publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, next); !errors.Is(err, ErrSnapshotRevisionGap) {
		t.Fatalf("provider accepted 10->12 gap: %v", err)
	}
	if got := provider.CurrentSnapshot().GlobalRevision; got != 10 {
		t.Fatalf("gapped publish changed live revision to %d", got)
	}
}

func TestDynamicAssignmentProviderWatcherCancellationCleansWithoutPublish(t *testing.T) {
	provider, _, err := NewDynamicAssignmentProvider(completeSnapshot(), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	updates, err := provider.WatchFrom(ctx, provider.CurrentSnapshot().GlobalRevision)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-updates:
		if ok {
			t.Fatal("cancelled watcher emitted an update")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled watcher did not close")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && provider.state.watcherCleanup.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	if provider.state.watcherCleanup.Load() == 0 {
		t.Fatal("watcher cleanup waited only on a future publish")
	}
}

type reconnectingProvider struct {
	snapshot   RouteSnapshot
	getCalls   atomic.Uint64
	watchCalls atomic.Uint64
}

func (p *reconnectingProvider) GetSnapshot(context.Context) (RouteSnapshot, error) {
	p.getCalls.Add(1)
	return cloneSnapshot(p.snapshot), nil
}

func (p *reconnectingProvider) Watch(context.Context) (<-chan RouteSnapshot, error) {
	ch := make(chan RouteSnapshot)
	close(ch)
	return ch, nil
}

func (p *reconnectingProvider) WatchFrom(ctx context.Context, _ uint64) (<-chan AssignmentUpdate, error) {
	call := p.watchCalls.Add(1)
	ch := make(chan AssignmentUpdate)
	if call == 1 {
		close(ch)
		return ch, nil
	}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func TestSnapshotApplierRevisionedRunReloadsAfterReconnect(t *testing.T) {
	base := completeSnapshot()
	base.GlobalRevision = 1
	next := cloneSnapshot(base)
	next.GlobalRevision = 2
	next.Player.Routes[0].ShardRevision++
	next.Player.Routes[0].ExpectedWriterEpoch++
	provider := &reconnectingProvider{snapshot: next}
	resolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewSnapshotApplier(provider, resolver)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- applier.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && (applier.LastGlobalRevision() != 2 || applier.Reconnects.Load() != 1) {
		time.Sleep(time.Millisecond)
	}
	if applier.LastGlobalRevision() != 2 || applier.Reconnects.Load() != 1 || provider.getCalls.Load() < 2 || provider.watchCalls.Load() < 2 {
		cancel()
		<-done
		t.Fatalf("revisioned reconnect last=%d reconnects=%d get=%d watch=%d", applier.LastGlobalRevision(), applier.Reconnects.Load(), provider.getCalls.Load(), provider.watchCalls.Load())
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run cancellation err=%v", err)
	}
}

func TestSnapshotApplierRejectsSameRevisionWithDifferentPayload(t *testing.T) {
	base := completeSnapshot()
	base.GlobalRevision = 3
	provider := &countingProvider{snapshot: base}
	resolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewSnapshotApplier(provider, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if err := applier.Apply(context.Background(), base); err != nil {
		t.Fatal(err)
	}

	conflict := cloneSnapshot(base)
	conflict.Holders[0].Endpoint = "different:9100"
	if err := applier.Apply(context.Background(), conflict); !errors.Is(err, ErrSnapshotRevisionConflict) {
		t.Fatalf("same revision conflict err=%v", err)
	}
	if applier.Duplicates.Load() != 0 {
		t.Fatalf("conflict counted as duplicate: %d", applier.Duplicates.Load())
	}
}

func TestNewStaticResolverFromProvider(t *testing.T) {
	provider, err := NewStaticAssignmentProvider(completeSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewStaticResolverFromProvider(context.Background(), provider)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveActor(context.Background(), DomainFarm, 10001); err != nil {
		t.Fatal(err)
	}
}
