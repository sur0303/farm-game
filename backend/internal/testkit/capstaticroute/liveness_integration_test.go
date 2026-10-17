package capstaticroute

import (
	"context"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	routeauthority "farmgame/internal/actorrouting/authority"
	"farmgame/internal/routecontrol"
	"farmgame/internal/storage/authoritystore"
)

func TestStaticInitFeedsManagerOwnedFreshProjectionForThreeTTLs(t *testing.T) {
	rig := newRealRig(t, 4)
	rig.initializer.Config.LeaseTTL = time.Second
	rig.initializer.Config.MinimumRemainingLease = 200 * time.Millisecond
	initial, err := rig.initializer.Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if initial.Snapshot.GlobalRevision != 1 {
		t.Fatalf("initial durable revision=%d", initial.Snapshot.GlobalRevision)
	}

	durableStore := routecontrol.NewMemoryRouteSnapshotStore()
	if _, err := durableStore.Bootstrap(t.Context(), routecontrol.NestedControlOperationToken{}, initial.Snapshot); err != nil {
		t.Fatal(err)
	}
	provider, dynamicPublisher, err := actorrouting.NewDynamicAssignmentProvider(initial.Snapshot, 16)
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := routecontrol.NewDurableSnapshotPublisher(
		durableStore,
		routecontrol.SnapshotNotifierFunc(func(ctx context.Context, snapshot actorrouting.RouteSnapshot) error {
			return dynamicPublisher.Publish(ctx, routecontrol.NestedControlOperationToken{}, snapshot)
		}),
		routecontrol.AuthoritySnapshotNotifierFunc(func(ctx context.Context, snapshot actorrouting.RouteSnapshot) error {
			return dynamicPublisher.PublishAuthoritySnapshot(ctx, routecontrol.NestedControlOperationToken{}, snapshot)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := routeauthority.NewRouteAssignmentManager(routeauthority.ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: rig.store, Publisher: publisher, AuthorityReader: rig.reader, InitialSnapshot: initial.Snapshot,
		AuthorityLayoutVersion: rig.initializer.Config.Profile.AuthorityLayoutVersion,
		RouteManagerID:         "route-controller", RouteManagerBootID: "route-controller-functional-g1-test",
		TombstoneStore: routeauthority.NewMemoryTransitionTombstoneStore(64), InitializationContext: t.Context(),
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := actorrouting.NewStaticResolverFromProvider(t.Context(), provider)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := actorrouting.NewSnapshotApplier(provider, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if err := applier.Bootstrap(t.Context()); err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() { runDone <- applier.Run(runCtx) }()
	defer func() {
		cancelRun()
		if err := <-runDone; err != nil && err != context.Canceled {
			t.Errorf("applier exit: %v", err)
		}
	}()

	renewAll := func(ctx context.Context) {
		snapshot := manager.CurrentSnapshot()
		for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
			routes := snapshot.Player.Routes
			if domain == actorrouting.DomainFarm {
				routes = snapshot.Farm.Routes
			}
			for shard, route := range routes {
				holder := snapshot.Holders[route.HolderIndex]
				result, err := rig.store.RenewShardAuthority(ctx, authoritystore.RenewShardAuthorityRequest{
					Caller:                 authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: holder.InstanceID, BootID: holder.BootID},
					AuthorityLayoutVersion: rig.initializer.Config.Profile.AuthorityLayoutVersion,
					Domain:                 domain, VirtualShard: uint32(shard),
					ExpectedFence: authoritystore.ShardWriteFence{
						ExpectedHolderID: holder.InstanceID, ExpectedHolderBootID: holder.BootID,
						ExpectedAssignmentRevision: route.ShardRevision, ExpectedWriterEpoch: route.ExpectedWriterEpoch, ExpectedLeaseEpoch: route.ExpectedLeaseEpoch,
					},
					RequestedTTLms: rig.initializer.Config.LeaseTTL.Milliseconds(),
				})
				if err != nil || result.Status != authoritystore.ShardTransitionApplied && result.Status != authoritystore.ShardTransitionAlreadyApplied {
					t.Fatalf("renew %s/%d status/error=%s/%v", domain, shard, result.Status, err)
				}
			}
		}
	}

	started := time.Now()
	deadline := started.Add(3*rig.initializer.Config.LeaseTTL + 300*time.Millisecond)
	lastRenew := time.Time{}
	published := 0
	waves := 0
	for time.Now().Before(deadline) {
		if lastRenew.IsZero() || time.Since(lastRenew) >= 250*time.Millisecond {
			ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
			renewAll(ctx)
			cancel()
			lastRenew = time.Now()
		}
		ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		result, err := manager.ProjectFreshLeases(ctx, 200*time.Millisecond)
		cancel()
		if err != nil {
			t.Fatalf("projection wave %d: %v", waves, err)
		}
		waves++
		if result.Published {
			published++
		}
		waitUntil := time.Now().Add(200 * time.Millisecond)
		for applier.LastGlobalRevision() < manager.CurrentSnapshot().GlobalRevision && time.Now().Before(waitUntil) {
			time.Sleep(time.Millisecond)
		}
		nowMs := time.Now().UnixMilli()
		observed := resolver.Snapshot()
		if observed.GlobalRevision != manager.CurrentSnapshot().GlobalRevision {
			t.Fatalf("consumer revision=%d manager=%d", observed.GlobalRevision, manager.CurrentSnapshot().GlobalRevision)
		}
		for _, routes := range [][]actorrouting.ShardRoute{observed.Player.Routes, observed.Farm.Routes} {
			for shard, route := range routes {
				if route.State != actorrouting.ShardStateActive || route.LeaseUntilMs <= nowMs {
					t.Fatalf("route expired/non-active during 3xTTL shard=%d route=%+v", shard, route)
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if elapsed := time.Since(started); elapsed < 3*rig.initializer.Config.LeaseTTL || waves < 3 || published < 3 {
		t.Fatalf("duration/waves/publishes=%s/%d/%d", elapsed, waves, published)
	}
	stored, found, err := durableStore.LoadCurrent(t.Context())
	if err != nil || !found || stored.Revision != manager.CurrentSnapshot().GlobalRevision || stored.Revision <= 1 {
		t.Fatalf("durable final found/revision/error=%t/%d/%v", found, stored.Revision, err)
	}
}
