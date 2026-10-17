package main

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
)

type countingAssignmentProvider struct {
	snapshot actorrouting.RouteSnapshot
	gets     atomic.Uint64
}

func (p *countingAssignmentProvider) GetSnapshot(context.Context) (actorrouting.RouteSnapshot, error) {
	p.gets.Add(1)
	return actorrouting.CloneRouteSnapshot(p.snapshot), nil
}

func (p *countingAssignmentProvider) WatchFrom(
	ctx context.Context,
	_ uint64,
) (<-chan actorrouting.AssignmentUpdate, error) {
	updates := make(chan actorrouting.AssignmentUpdate)
	go func() {
		defer close(updates)
		<-ctx.Done()
	}()
	return updates, nil
}

func opportunityRouteSnapshot() actorrouting.RouteSnapshot {
	leaseUntil := time.Now().Add(time.Hour).UnixMilli()
	route := func(holderIndex int) actorrouting.ShardRoute {
		return actorrouting.ShardRoute{
			HolderIndex:         holderIndex,
			State:               actorrouting.ShardStateActive,
			ShardRevision:       1,
			ExpectedWriterEpoch: 1,
			ExpectedLeaseEpoch:  1,
			LeaseUntilMs:        leaseUntil,
		}
	}
	return actorrouting.RouteSnapshot{
		Realm:          "classic-farm",
		GlobalRevision: 7,
		HashVersion:    actorrouting.HashVersionXXH64V2,
		HashSeed:       1,
		Player: actorrouting.DomainSnapshot{
			ShardCount: 4,
			Routes:     []actorrouting.ShardRoute{route(0), route(0), route(0), route(0)},
		},
		Farm: actorrouting.DomainSnapshot{
			ShardCount: 4,
			Routes:     []actorrouting.ShardRoute{route(1), route(1), route(1), route(1)},
		},
		FarmWorker: actorrouting.DomainSnapshot{
			ShardCount: 4,
			Routes:     []actorrouting.ShardRoute{route(2), route(3), route(2), route(3)},
		},
		Holders: []actorrouting.HolderRef{
			{
				InstanceID: "player-a",
				BootID:     "player-a-boot",
				Endpoint:   "player-a:9100",
				State:      actorrouting.HolderStateActive,
				Weight:     1,
			},
			{
				InstanceID: "farm-a",
				BootID:     "farm-a-boot",
				Endpoint:   "farm-a:9090",
				State:      actorrouting.HolderStateActive,
				Weight:     1,
			},
			{
				InstanceID: "farm-worker-a",
				BootID:     "worker-a-boot",
				Endpoint:   "farm-worker-a:9200",
				State:      actorrouting.HolderStateActive,
				Weight:     1,
			},
			{
				InstanceID: "farm-worker-b",
				BootID:     "worker-b-boot",
				Endpoint:   "farm-worker-b:9200",
				State:      actorrouting.HolderStateActive,
				Weight:     1,
			},
		},
	}
}

func TestOpportunityOwnedShardAssignmentRequiresDynamicProviderInRouteMode(t *testing.T) {
	t.Setenv("FARM_OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE", "route")
	t.Setenv("FARM_OPPORTUNITY_WAKEUP_OWNED_SHARDS", "")
	_, _, err := opportunityOwnedShardAssignment(
		[]uint32{0, 1, 2, 3},
		nil,
		"farm-worker-a",
		"worker-a-boot",
	)
	if err == nil || !strings.Contains(err.Error(), "AssignmentComposition.Provider") {
		t.Fatalf("route mode nil provider err=%v", err)
	}
}

func TestOpportunityOwnedShardAssignmentUsesSuppliedDynamicProvider(t *testing.T) {
	t.Setenv("FARM_OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE", "route")
	t.Setenv("FARM_OPPORTUNITY_WAKEUP_OWNED_SHARDS", "")
	source := &countingAssignmentProvider{snapshot: opportunityRouteSnapshot()}
	assignment, routeAssignment, err := opportunityOwnedShardAssignment(
		[]uint32{0, 1, 2, 3},
		source,
		"farm-worker-a",
		"worker-a-boot",
	)
	if err != nil {
		t.Fatal(err)
	}
	if routeAssignment == nil || assignment != routeAssignment {
		t.Fatal("route assignment did not return one shared dynamic provider")
	}
	if source.gets.Load() != 1 {
		t.Fatalf("dynamic source bootstrap calls=%d, want 1", source.gets.Load())
	}
	got, err := assignment.Assignment(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != 7 || len(got.OwnedShards) != 2 ||
		got.OwnedShards[0] != 0 || got.OwnedShards[1] != 2 {
		t.Fatalf("assignment=%+v, want revision 7 shards [0 2]", got)
	}
}
