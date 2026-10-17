package opportunityrecovery

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/farm/domain"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
)

func routeWorkerSnapshot(t *testing.T, revision uint64, ownerByShard []int) actorrouting.RouteSnapshot {
	t.Helper()
	player := actorrouting.HolderRef{InstanceID: "player", BootID: "player-boot", Endpoint: "player:9100", State: actorrouting.HolderStateActive, Weight: 1}
	farm := actorrouting.HolderRef{InstanceID: "farm", BootID: "farm-boot", Endpoint: "farm:9090", State: actorrouting.HolderStateActive, Weight: 1}
	workers := []actorrouting.HolderRef{
		{InstanceID: "worker-a", BootID: "worker-a-boot", Endpoint: "worker-a:9200", State: actorrouting.HolderStateActive, Weight: 1},
		{InstanceID: "worker-b", BootID: "worker-b-boot", Endpoint: "worker-b:9200", State: actorrouting.HolderStateActive, Weight: 1},
	}
	active := func(holder int) actorrouting.ShardRoute {
		return actorrouting.ShardRoute{HolderIndex: holder, State: actorrouting.ShardStateActive,
			ShardRevision: revision, ExpectedWriterEpoch: revision, ExpectedLeaseEpoch: revision, LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli()}
	}
	farmRoutes := make([]actorrouting.ShardRoute, len(ownerByShard))
	workerRoutes := make([]actorrouting.ShardRoute, len(ownerByShard))
	for shard, owner := range ownerByShard {
		farmRoutes[shard] = active(1)
		workerRoutes[shard] = active(2 + owner)
	}
	return actorrouting.RouteSnapshot{Realm: "test", GlobalRevision: revision, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608120001,
		Player:     actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{active(0)}},
		Farm:       actorrouting.DomainSnapshot{ShardCount: uint32(len(farmRoutes)), Routes: farmRoutes},
		FarmWorker: actorrouting.DomainSnapshot{ShardCount: uint32(len(workerRoutes)), Routes: workerRoutes},
		Holders:    append([]actorrouting.HolderRef{player, farm}, workers...),
	}
}

func TestRouteWorkerDelegatedLeaseViewSurvivesDeadlineOnlySnapshotRefresh(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	initial := routeWorkerSnapshot(t, 1, []int{0})
	initial.FarmWorker.Routes[0].LeaseUntilMs = now.Add(-time.Second).UnixMilli()
	source, err := actorrouting.NewStaticAssignmentProvider(initial)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewRouteWorkerShardAssignmentProviderWithOptions(source, "worker-a", "worker-a-boot", RouteWorkerShardAssignmentOptions{
		LeaseCheckMode: actorrouting.LeaseCheckAuthorityDelegated,
		Now:            func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	if assignment, err := provider.Assignment(context.Background()); err != nil || len(assignment.OwnedShards) != 0 {
		t.Fatalf("delegated provider used stale snapshot lease: assignment=%+v err=%v", assignment, err)
	}
	route := initial.FarmWorker.Routes[0]
	lease := WorkerShardLeaseView{
		Shard: 0, HolderID: "worker-a", HolderBootID: "worker-a-boot",
		AssignmentRevision: route.ShardRevision, WriterEpoch: route.ExpectedWriterEpoch,
		LeaseEpoch: route.ExpectedLeaseEpoch, LeaseUntilMs: now.Add(time.Minute).UnixMilli(),
	}
	if err := provider.ObserveAuthorityLease(lease); err != nil {
		t.Fatal(err)
	}
	assignment, err := provider.Assignment(context.Background())
	if err != nil || assignment.Revision != 1 || len(assignment.OwnedShards) != 1 {
		t.Fatalf("renewed delegated lease not admitted: assignment=%+v err=%v", assignment, err)
	}
	if err := provider.CanClaimShard(context.Background(), 0, assignment.Revision); err != nil {
		t.Fatalf("renewed delegated lease cannot claim: %v", err)
	}

	next := actorrouting.CloneRouteSnapshot(initial)
	next.GlobalRevision++
	next.FarmWorker.Routes[0].LeaseUntilMs = now.Add(-2 * time.Second).UnixMilli()
	if err := provider.install(next, false); err != nil {
		t.Fatal(err)
	}
	assignment, err = provider.Assignment(context.Background())
	if err != nil || assignment.Revision != 1 || len(assignment.OwnedShards) != 1 {
		t.Fatalf("deadline-only snapshot displaced independent lease: assignment=%+v err=%v", assignment, err)
	}
}

func TestRouteWorkerDelegatedLeaseFenceChangeStopsClaims(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	initial := routeWorkerSnapshot(t, 1, []int{0})
	source, err := actorrouting.NewStaticAssignmentProvider(initial)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewRouteWorkerShardAssignmentProviderWithOptions(source, "worker-a", "worker-a-boot", RouteWorkerShardAssignmentOptions{
		LeaseCheckMode: actorrouting.LeaseCheckAuthorityDelegated,
		Now:            func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	route := initial.FarmWorker.Routes[0]
	oldLease := WorkerShardLeaseView{Shard: 0, HolderID: "worker-a", HolderBootID: "worker-a-boot", AssignmentRevision: route.ShardRevision, WriterEpoch: route.ExpectedWriterEpoch, LeaseEpoch: route.ExpectedLeaseEpoch, LeaseUntilMs: now.Add(time.Minute).UnixMilli()}
	if err := provider.ObserveAuthorityLease(oldLease); err != nil {
		t.Fatal(err)
	}
	next := routeWorkerSnapshot(t, 2, []int{0})
	if err := provider.install(next, false); err != nil {
		t.Fatal(err)
	}
	assignment, err := provider.Assignment(context.Background())
	if err != nil || len(assignment.OwnedShards) != 0 {
		t.Fatalf("fence change retained old lease: assignment=%+v err=%v", assignment, err)
	}
	if err := provider.ObserveAuthorityLease(oldLease); !errors.Is(err, ErrWorkerAuthorityFenceMismatch) {
		t.Fatalf("old fence was accepted after route change: %v", err)
	}
	if err := provider.CanClaimShard(context.Background(), 0, assignment.Revision); !errors.Is(err, ErrWorkerAuthorityFenceMismatch) {
		t.Fatalf("route without new authority fence claim err=%v", err)
	}
}

func TestRouteWorkerAssignmentMigrationFencesNewClaims(t *testing.T) {
	initial := routeWorkerSnapshot(t, 1, []int{0, 1, 0, 1})
	source, _, err := actorrouting.NewDynamicAssignmentProvider(initial, 4)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewRouteWorkerShardAssignmentProvider(source, "worker-a", "worker-a-boot")
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	assignment, err := provider.Assignment(context.Background())
	if err != nil || assignment.Revision != 1 || len(assignment.OwnedShards) != 2 {
		t.Fatalf("initial assignment=%+v err=%v", assignment, err)
	}
	next := routeWorkerSnapshot(t, 2, []int{1, 1, 1, 1})
	if err := provider.install(next, false); err != nil {
		t.Fatal(err)
	}
	if err := provider.CanClaimShard(context.Background(), 0, 1); !errors.Is(err, ErrWorkerAssignmentStale) {
		t.Fatalf("old revision admitted: %v", err)
	}
	assignment, err = provider.Assignment(context.Background())
	if err != nil || assignment.Revision != 2 || len(assignment.OwnedShards) != 0 {
		t.Fatalf("migrated assignment=%+v err=%v", assignment, err)
	}
}

type rejectClaimAdmission struct{ err error }

func (a rejectClaimAdmission) CanClaimShard(context.Context, uint32, uint64) error { return a.err }

func TestWorkerRechecksAssignmentBeforeClaim(t *testing.T) {
	bucket := testBucket(t)
	farm, payload := testFarm(t, 91, 2, domain.FarmOpportunityCycleFactV1{PlotID: 1, CycleID: "c", MatureAtMs: 1000, RemainingTheftSlots: 1})
	store := &fakeScheduleStore{due: []authority.ScheduledDueRef{{ScheduleType: WorkType, ScheduleID: "s", DueAtMs: payload.DueAtMs}}, snapshots: map[string]authority.ScheduledWorkSnapshot{}}
	addSnapshot(t, store, "s", payload)
	provider, err := NewStaticWorkerShardAssignmentProvider([]uint32{0})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := New(Config{ScheduleStore: store, FarmReader: &fakeFarmReader{farm: farm}, Publisher: &fakePublisher{},
		Buckets: []rediskey.AuthorityBucketRef{bucket}, ShardAssignment: provider,
		ShardClaimAdmission: rejectClaimAdmission{err: ErrWorkerAssignmentStale},
		Owner:               "worker", Token: "token", BootID: "boot", LeaseTTL: time.Second, MaxConcurrent: 1,
		RoundTimeBudget: time.Second, Now: func() time.Time { return time.UnixMilli(2000) }})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.ClaimRejected != 1 || stats.Claimed != 0 || store.claimCalls != 0 {
		t.Fatalf("stale assignment reached WorkLease claim: stats=%+v claims=%d", stats, store.claimCalls)
	}
}

func TestRouteWorkerAssignmentExpiredLeaseOwnsNoShards(t *testing.T) {
	snapshot := routeWorkerSnapshot(t, 1, []int{0})
	source, err := actorrouting.NewStaticAssignmentProvider(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewRouteWorkerShardAssignmentProvider(source, "worker-a", "worker-a-boot")
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	provider.now = func() time.Time { return time.UnixMilli(snapshot.FarmWorker.Routes[0].LeaseUntilMs + 1) }
	assignment, err := provider.Assignment(context.Background())
	if err != nil || len(assignment.OwnedShards) != 0 {
		t.Fatalf("expired assignment=%+v err=%v", assignment, err)
	}
	if err := provider.CanClaimShard(context.Background(), 0, assignment.Revision); !errors.Is(err, ErrWorkerShardNotOwned) {
		t.Fatalf("expired lease admitted: %v", err)
	}
}

func TestRouteWorkerAssignmentsScaleOneTwoFourEightWithoutDuplicateScanning(t *testing.T) {
	const shardCount = 8
	for _, workerCount := range []int{1, 2, 4, 8} {
		t.Run(string(rune('0'+workerCount)), func(t *testing.T) {
			holders := []actorrouting.HolderRef{
				{InstanceID: "player", BootID: "player-boot", Endpoint: "player:9100", State: actorrouting.HolderStateActive, Weight: 1},
				{InstanceID: "farm", BootID: "farm-boot", Endpoint: "farm:9090", State: actorrouting.HolderStateActive, Weight: 1},
			}
			for worker := 0; worker < workerCount; worker++ {
				holders = append(holders, actorrouting.HolderRef{InstanceID: fmt.Sprintf("worker-%d", worker), BootID: fmt.Sprintf("boot-%d", worker), Endpoint: fmt.Sprintf("worker-%d:9200", worker), State: actorrouting.HolderStateActive, Weight: 1})
			}
			active := func(holder int) actorrouting.ShardRoute {
				return actorrouting.ShardRoute{HolderIndex: holder, State: actorrouting.ShardStateActive, ShardRevision: uint64(workerCount), ExpectedWriterEpoch: uint64(workerCount), ExpectedLeaseEpoch: uint64(workerCount), LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli()}
			}
			farmRoutes := make([]actorrouting.ShardRoute, shardCount)
			workerRoutes := make([]actorrouting.ShardRoute, shardCount)
			for shard := 0; shard < shardCount; shard++ {
				farmRoutes[shard] = active(1)
				workerRoutes[shard] = active(2 + shard%workerCount)
			}
			snapshot := actorrouting.RouteSnapshot{Realm: "test", GlobalRevision: uint64(workerCount), HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608120002,
				Player:     actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{active(0)}},
				Farm:       actorrouting.DomainSnapshot{ShardCount: shardCount, Routes: farmRoutes},
				FarmWorker: actorrouting.DomainSnapshot{ShardCount: shardCount, Routes: workerRoutes}, Holders: holders}
			source, err := actorrouting.NewStaticAssignmentProvider(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			seen := make(map[uint32]string, shardCount)
			for worker := 0; worker < workerCount; worker++ {
				provider, err := NewRouteWorkerShardAssignmentProvider(source, fmt.Sprintf("worker-%d", worker), fmt.Sprintf("boot-%d", worker))
				if err != nil {
					t.Fatal(err)
				}
				if err := provider.Bootstrap(context.Background()); err != nil {
					t.Fatal(err)
				}
				assignment, err := provider.Assignment(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				for _, shard := range assignment.OwnedShards {
					if previous, exists := seen[shard]; exists {
						t.Fatalf("shard %d scanned by %s and worker-%d", shard, previous, worker)
					}
					seen[shard] = fmt.Sprintf("worker-%d", worker)
				}
			}
			if len(seen) != shardCount {
				t.Fatalf("workers=%d covered=%d want=%d", workerCount, len(seen), shardCount)
			}
		})
	}
}

func TestRouteWorkerFullReloadCannotRollBackRevision(t *testing.T) {
	initial := routeWorkerSnapshot(t, 2, []int{0})
	source, err := actorrouting.NewStaticAssignmentProvider(initial)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewRouteWorkerShardAssignmentProvider(source, "worker-a", "worker-a-boot")
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	stale := routeWorkerSnapshot(t, 1, []int{0})
	if err := provider.install(stale, true); !errors.Is(err, ErrWorkerAssignmentStale) {
		t.Fatalf("stale full reload accepted: %v", err)
	}
}

type gapReloadAssignmentSource struct {
	mu            sync.Mutex
	initial       actorrouting.RouteSnapshot
	reloaded      actorrouting.RouteSnapshot
	getCalls      int
	watchCalls    int
	reloadStarted chan struct{}
	reloadRelease chan struct{}
	reloadOnce    sync.Once
}

func (s *gapReloadAssignmentSource) GetSnapshot(ctx context.Context) (actorrouting.RouteSnapshot, error) {
	s.mu.Lock()
	s.getCalls++
	call := s.getCalls
	s.mu.Unlock()
	if call == 1 {
		return actorrouting.CloneRouteSnapshot(s.initial), nil
	}
	s.reloadOnce.Do(func() { close(s.reloadStarted) })
	select {
	case <-s.reloadRelease:
		return actorrouting.CloneRouteSnapshot(s.reloaded), nil
	case <-ctx.Done():
		return actorrouting.RouteSnapshot{}, ctx.Err()
	}
}

func (s *gapReloadAssignmentSource) WatchFrom(ctx context.Context, _ uint64) (<-chan actorrouting.AssignmentUpdate, error) {
	s.mu.Lock()
	s.watchCalls++
	call := s.watchCalls
	s.mu.Unlock()
	if call == 1 {
		updates := make(chan actorrouting.AssignmentUpdate, 1)
		updates <- actorrouting.AssignmentUpdate{Revision: s.reloaded.GlobalRevision, Snapshot: actorrouting.CloneRouteSnapshot(s.reloaded)}
		close(updates)
		return updates, nil
	}
	updates := make(chan actorrouting.AssignmentUpdate)
	go func() {
		<-ctx.Done()
		close(updates)
	}()
	return updates, nil
}

func TestRouteWorkerWatchGapFailsClosedThenFullReloads(t *testing.T) {
	source := &gapReloadAssignmentSource{
		initial:       routeWorkerSnapshot(t, 1, []int{0, 1}),
		reloaded:      routeWorkerSnapshot(t, 3, []int{1, 0}),
		reloadStarted: make(chan struct{}),
		reloadRelease: make(chan struct{}),
	}
	provider, err := NewRouteWorkerShardAssignmentProvider(source, "worker-a", "worker-a-boot")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- provider.Run(ctx) }()

	select {
	case <-source.reloadStarted:
	case <-time.After(time.Second):
		t.Fatal("watch revision gap did not start a full reload")
	}
	if _, err := provider.Assignment(context.Background()); !errors.Is(err, ErrWorkerAssignmentUnavailable) {
		t.Fatalf("new claims were not fenced during reload: %v", err)
	}
	close(source.reloadRelease)

	deadline := time.Now().Add(time.Second)
	for {
		stats := provider.Stats()
		if stats.Ready && stats.Revision == 3 {
			if stats.WatchGaps != 1 || stats.FullReloads != 2 {
				t.Fatalf("unexpected recovery stats: %+v", stats)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("provider did not converge after full reload: %+v", stats)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}
