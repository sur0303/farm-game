package authority

import (
	"context"
	"errors"
	"farmgame/internal/routecontrol"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

type projectionReader struct {
	mu      sync.Mutex
	values  map[authorityKey]authoritystore.ShardWriteAuthority
	err     error
	calls   int
	started chan struct{}
	release <-chan struct{}
}

func (r *projectionReader) ReadShardWriteAuthority(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool, error) {
	r.mu.Lock()
	r.calls++
	started, release, readErr := r.started, r.release, r.err
	if r.started != nil {
		close(r.started)
		r.started = nil
	}
	value, found := r.values[authorityKey{domain: domain, shard: shard}]
	r.mu.Unlock()
	if started != nil && release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return authoritystore.ShardWriteAuthority{}, false, ctx.Err()
		}
	}
	return value, found, readErr
}

func (r *projectionReader) setDeadline(key authorityKey, deadline int64) {
	r.mu.Lock()
	value := r.values[key]
	value.LeaseUntilMs = deadline
	r.values[key] = value
	r.mu.Unlock()
}

func activeProjectionSnapshot(now time.Time) actorrouting.RouteSnapshot {
	deadline := now.Add(30 * time.Second).UnixMilli()
	return actorrouting.RouteSnapshot{
		Realm: "classic-farm", GlobalRevision: 1, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608040001,
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: deadline,
		}}},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: deadline,
		}}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "player-a", BootID: "player-a-boot", Endpoint: "player-a:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-a", BootID: "farm-a-boot", Endpoint: "farm-a:9001", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
}

func projectionAuthorities(snapshot actorrouting.RouteSnapshot) map[authorityKey]authoritystore.ShardWriteAuthority {
	out := make(map[authorityKey]authoritystore.ShardWriteAuthority, 2)
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		routes := snapshot.Player.Routes
		if domain == actorrouting.DomainFarm {
			routes = snapshot.Farm.Routes
		}
		route := routes[0]
		holder := snapshot.Holders[route.HolderIndex]
		out[authorityKey{domain: domain, shard: 0}] = authoritystore.ShardWriteAuthority{
			AuthorityLayoutVersion: "auth-v1", HashVersion: snapshot.HashVersion, HashSeed: snapshot.HashSeed,
			Domain: domain, VirtualShard: 0, HolderID: holder.InstanceID, HolderBootID: holder.BootID,
			AssignmentRevision: route.ShardRevision, WriterEpoch: route.ExpectedWriterEpoch, LeaseEpoch: route.ExpectedLeaseEpoch,
			State: authoritystore.ShardAuthorityActive, LeaseUntilMs: route.LeaseUntilMs,
		}
	}
	return out
}

func newProjectionManager(t *testing.T, now time.Time, reader AuthorityReader, publisher SnapshotPublisher) *RouteAssignmentManager {
	t.Helper()
	calculator, err := actorrouting.NewCalculator("classic-farm", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040001}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{Resolver: calculator, AuthorityLayoutVersion: "auth-v1", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: publisher, AuthorityReader: reader, InitialSnapshot: activeProjectionSnapshot(now),
		AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager", RouteManagerBootID: "route-manager-boot",
		Now: func() time.Time { return now }, TombstoneStore: NewMemoryTransitionTombstoneStore(32),
	})
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func projectionContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(t.Context(), time.Second)
}

func TestProjectFreshLeasesPublishesOneCompleteDeadlineWave(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	snapshot := activeProjectionSnapshot(now)
	reader := &projectionReader{values: projectionAuthorities(snapshot)}
	reader.setDeadline(authorityKey{domain: actorrouting.DomainPlayer}, now.Add(50*time.Second).UnixMilli())
	reader.setDeadline(authorityKey{domain: actorrouting.DomainFarm}, now.Add(55*time.Second).UnixMilli())
	var published []actorrouting.RouteSnapshot
	manager := newProjectionManager(t, now, reader, SnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, value actorrouting.RouteSnapshot) error {
		published = append(published, actorrouting.CloneRouteSnapshot(value))
		return nil
	}))
	ctx, cancel := projectionContext(t)
	defer cancel()
	result, err := manager.ProjectFreshLeases(ctx, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if result.Observed != 2 || result.Changed != 2 || !result.Published || result.GlobalRevision != 2 || len(published) != 1 {
		t.Fatalf("result=%+v publishes=%d", result, len(published))
	}
	current := manager.CurrentSnapshot()
	if current.GlobalRevision != 2 || current.Player.Routes[0].LeaseUntilMs != now.Add(50*time.Second).UnixMilli() ||
		current.Farm.Routes[0].LeaseUntilMs != now.Add(55*time.Second).UnixMilli() {
		t.Fatalf("current=%+v", current)
	}
	result, err = manager.ProjectFreshLeases(ctx, 10*time.Second)
	if err != nil || result.Published || result.Changed != 0 || len(published) != 1 {
		t.Fatalf("no-change result=%+v err=%v publishes=%d", result, err, len(published))
	}
}

func TestProjectFreshLeasesRejectsPartialRollbackAndTooClose(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	for _, mutate := range []func(*projectionReader, actorrouting.RouteSnapshot){
		func(reader *projectionReader, _ actorrouting.RouteSnapshot) {
			reader.err = errors.New("read unavailable")
		},
		func(reader *projectionReader, snapshot actorrouting.RouteSnapshot) {
			reader.setDeadline(authorityKey{domain: actorrouting.DomainFarm}, snapshot.Farm.Routes[0].LeaseUntilMs-1)
		},
		func(reader *projectionReader, _ actorrouting.RouteSnapshot) {
			reader.setDeadline(authorityKey{domain: actorrouting.DomainPlayer}, now.Add(31*time.Second).UnixMilli())
			reader.setDeadline(authorityKey{domain: actorrouting.DomainFarm}, now.Add(31*time.Second).UnixMilli())
		},
	} {
		snapshot := activeProjectionSnapshot(now)
		reader := &projectionReader{values: projectionAuthorities(snapshot)}
		mutate(reader, snapshot)
		publishes := 0
		manager := newProjectionManager(t, now, reader, SnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			publishes++
			return nil
		}))
		ctx, cancel := projectionContext(t)
		minimum := 10 * time.Second
		if reader.err == nil && reader.values[authorityKey{domain: actorrouting.DomainPlayer}].LeaseUntilMs == now.Add(31*time.Second).UnixMilli() {
			minimum = 32 * time.Second
		}
		_, err := manager.ProjectFreshLeases(ctx, minimum)
		cancel()
		if err == nil || publishes != 0 || manager.CurrentSnapshot().GlobalRevision != 1 {
			t.Fatalf("err=%v publishes=%d revision=%d", err, publishes, manager.CurrentSnapshot().GlobalRevision)
		}
	}
}

func TestProjectFreshLeasesStaleWaveCannotOverwriteNewSnapshot(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	snapshot := activeProjectionSnapshot(now)
	started := make(chan struct{})
	release := make(chan struct{})
	reader := &projectionReader{values: projectionAuthorities(snapshot), started: started, release: release}
	reader.setDeadline(authorityKey{domain: actorrouting.DomainPlayer}, now.Add(50*time.Second).UnixMilli())
	reader.setDeadline(authorityKey{domain: actorrouting.DomainFarm}, now.Add(50*time.Second).UnixMilli())
	publishes := 0
	manager := newProjectionManager(t, now, reader, SnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		publishes++
		return nil
	}))
	ctx, cancel := projectionContext(t)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := manager.ProjectFreshLeases(ctx, 10*time.Second); done <- err }()
	<-started
	manager.mu.Lock()
	manager.snapshot.GlobalRevision++
	manager.mu.Unlock()
	close(release)
	if err := <-done; !errors.Is(err, ErrLeaseProjectionStale) || publishes != 0 || manager.CurrentSnapshot().GlobalRevision != 2 {
		t.Fatalf("err=%v publishes=%d revision=%d", err, publishes, manager.CurrentSnapshot().GlobalRevision)
	}
}

func TestProjectFreshLeasesRetriesExactPendingPublicationBeforeNewRead(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	snapshot := activeProjectionSnapshot(now)
	reader := &projectionReader{values: projectionAuthorities(snapshot)}
	reader.setDeadline(authorityKey{domain: actorrouting.DomainPlayer}, now.Add(50*time.Second).UnixMilli())
	reader.setDeadline(authorityKey{domain: actorrouting.DomainFarm}, now.Add(50*time.Second).UnixMilli())
	publishes := 0
	manager := newProjectionManager(t, now, reader, SnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		publishes++
		if publishes == 1 {
			return errors.New("committed notify unknown")
		}
		return nil
	}))
	ctx, cancel := projectionContext(t)
	defer cancel()
	if _, err := manager.ProjectFreshLeases(ctx, 10*time.Second); !errors.Is(err, ErrSnapshotPublish) {
		t.Fatalf("first err=%v", err)
	}
	_, renewErr := manager.Renew(ctx, RenewRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: "player-a", BootID: "player-a-boot"},
		Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: authoritystore.ShardWriteFence{ExpectedHolderID: "player-a", ExpectedHolderBootID: "player-a-boot", ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1},
		RequestedTTL:  30 * time.Second,
	})
	if !errors.Is(renewErr, ErrLeaseProjectionPending) {
		t.Fatalf("renew passed pending projection: %v", renewErr)
	}
	calls := reader.calls
	deadlineResult, deadlineErr := manager.ReconcileRenewedAuthorityDeadlines(ctx, 10*time.Second)
	if !errors.Is(deadlineErr, ErrLeaseProjectionPending) || deadlineResult.FailureStage != "lease_projection_pending" || reader.calls != calls || publishes != 1 {
		t.Fatalf("deadline reconciliation crossed pending all-active projection: result=%+v err=%v reads=%d/%d publishes=%d", deadlineResult, deadlineErr, reader.calls, calls, publishes)
	}
	recoveryErr := manager.RecoverPendingProjections(ctx, nil, nil)
	if !errors.Is(recoveryErr, ErrProjectionRecovery) || !errors.Is(recoveryErr, ErrLeaseProjectionPending) || reader.calls != calls || publishes != 1 {
		t.Fatalf("durable recovery crossed pending all-active projection: err=%v reads=%d/%d publishes=%d", recoveryErr, reader.calls, calls, publishes)
	}

	reader.setDeadline(authorityKey{domain: actorrouting.DomainPlayer}, now.Add(55*time.Second).UnixMilli())
	result, err := manager.ProjectFreshLeases(ctx, 10*time.Second)
	if err != nil || !result.Published || reader.calls != calls || publishes != 2 || manager.CurrentSnapshot().GlobalRevision != 2 {
		t.Fatalf("retry result=%+v err=%v reads=%d/%d publishes=%d", result, err, reader.calls, calls, publishes)
	}
}

func TestProjectFreshLeasesClearsPendingCandidateWhenItsBaseIsStale(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	snapshot := activeProjectionSnapshot(now)
	reader := &projectionReader{values: projectionAuthorities(snapshot)}
	reader.setDeadline(authorityKey{domain: actorrouting.DomainPlayer}, now.Add(50*time.Second).UnixMilli())
	reader.setDeadline(authorityKey{domain: actorrouting.DomainFarm}, now.Add(50*time.Second).UnixMilli())
	publishes := 0
	manager := newProjectionManager(t, now, reader, SnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		publishes++
		return errors.New("definitive CAS failure")
	}))
	ctx, cancel := projectionContext(t)
	defer cancel()
	if _, err := manager.ProjectFreshLeases(ctx, 10*time.Second); !errors.Is(err, ErrSnapshotPublish) {
		t.Fatalf("first err=%v", err)
	}
	manager.mu.Lock()
	manager.snapshot.GlobalRevision++
	manager.mu.Unlock()
	if _, err := manager.ProjectFreshLeases(ctx, 10*time.Second); !errors.Is(err, ErrLeaseProjectionStale) || publishes != 1 {
		t.Fatalf("stale retry err=%v publishes=%d", err, publishes)
	}
	manager.mu.Lock()
	pending := manager.pendingLeaseProjection
	manager.mu.Unlock()
	if pending != nil {
		t.Fatal("stale pending lease projection was not cleared")
	}
}

func TestProjectFreshLeasesAdoptsCommittedSnapshotAndReplaysNotification(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	snapshot := activeProjectionSnapshot(now)
	reader := &projectionReader{values: projectionAuthorities(snapshot)}
	reader.setDeadline(authorityKey{domain: actorrouting.DomainPlayer}, now.Add(50*time.Second).UnixMilli())
	reader.setDeadline(authorityKey{domain: actorrouting.DomainFarm}, now.Add(55*time.Second).UnixMilli())
	publishes := 0
	manager := newProjectionManager(t, now, reader, SnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		publishes++
		if publishes == 1 {
			return committedNotificationTestError{}
		}
		return nil
	}))
	ctx, cancel := projectionContext(t)
	defer cancel()
	result, err := manager.ProjectFreshLeases(ctx, 10*time.Second)
	if !errors.Is(err, ErrSnapshotPublish) || result.Published || manager.CurrentSnapshot().GlobalRevision != 2 {
		t.Fatalf("committed notification result=%+v err=%v revision=%d", result, err, manager.CurrentSnapshot().GlobalRevision)
	}
	manager.mu.Lock()
	leasePending, notifyPending := manager.pendingLeaseProjection, manager.deadlineNotifyPending
	manager.mu.Unlock()
	if leasePending != nil || notifyPending == nil || notifyPending.GlobalRevision != 2 {
		t.Fatalf("committed state lease_pending=%v notify_pending=%+v", leasePending != nil, notifyPending)
	}
	replayed, err := manager.ReconcileRenewedAuthorityDeadlines(ctx, 10*time.Second)
	if err != nil || !replayed.NotificationReplayed || replayed.SnapshotRevision != 2 || publishes != 2 {
		t.Fatalf("notification replay result=%+v err=%v publishes=%d", replayed, err, publishes)
	}
	noop, err := manager.ProjectFreshLeases(ctx, 10*time.Second)
	if err != nil || noop.Observed != 2 || noop.Changed != 0 || noop.Published || publishes != 2 {
		t.Fatalf("post-replay projection result=%+v err=%v publishes=%d", noop, err, publishes)
	}
}
