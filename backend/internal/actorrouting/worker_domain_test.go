package actorrouting

import (
	"context"
	"testing"
)

func workerDomainSnapshot(t *testing.T) RouteSnapshot {
	t.Helper()
	snapshot := completeSnapshot()
	baseHolderCount := len(snapshot.Holders)
	snapshot.Holders = append(snapshot.Holders,
		HolderRef{InstanceID: "worker-a", BootID: "worker-a-boot", Endpoint: "worker-a:9200", State: HolderStateActive, Weight: 1},
		HolderRef{InstanceID: "worker-b", BootID: "worker-b-boot", Endpoint: "worker-b:9200", State: HolderStateActive, Weight: 1},
	)
	routes := make([]ShardRoute, snapshot.Farm.ShardCount)
	for shard := range routes {
		routes[shard] = ShardRoute{HolderIndex: baseHolderCount + shard%2, State: ShardStateActive,
			ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: activeLeaseUntil()}
	}
	snapshot.FarmWorker = DomainSnapshot{ShardCount: snapshot.Farm.ShardCount, Routes: routes}
	return snapshot
}

func TestFarmWorkerUsesFarmShardCalculationButIndependentAssignments(t *testing.T) {
	snapshot := workerDomainSnapshot(t)
	resolver, err := NewStaticResolver(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	calculator, err := NewCalculator(snapshot.Realm, HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed}, snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		t.Fatal(err)
	}
	for _, farmID := range []uint64{1, 42, 9001, 999999} {
		farmShard, err := calculator.ShardFor(DomainFarm, farmID)
		if err != nil {
			t.Fatal(err)
		}
		workerShard, err := calculator.ShardFor(DomainFarmWorker, farmID)
		if err != nil {
			t.Fatal(err)
		}
		if farmShard != workerShard {
			t.Fatalf("farm_id=%d FARM shard=%d FARM_WORKER shard=%d", farmID, farmShard, workerShard)
		}
	}
	farm, err := resolver.Resolve(context.Background(), Address{Realm: snapshot.Realm, Kind: DomainFarm, ID: 42})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := resolver.Resolve(context.Background(), Address{Realm: snapshot.Realm, Kind: DomainFarmWorker, ID: 42})
	if err != nil {
		t.Fatal(err)
	}
	if farm.ShardID != worker.ShardID || farm.HolderPod == worker.HolderPod {
		t.Fatalf("assignments not independent: farm=%+v worker=%+v", farm, worker)
	}
}

func TestFarmWorkerShardCountMustMatchFarm(t *testing.T) {
	snapshot := workerDomainSnapshot(t)
	snapshot.FarmWorker.ShardCount--
	snapshot.FarmWorker.Routes = snapshot.FarmWorker.Routes[:snapshot.FarmWorker.ShardCount]
	if _, err := NewStaticResolver(snapshot); err == nil {
		t.Fatal("mismatched FARM_WORKER shard count accepted")
	}
}

func TestCloneRouteSnapshotClonesFarmWorkerRoutes(t *testing.T) {
	snapshot := workerDomainSnapshot(t)
	clone := CloneRouteSnapshot(snapshot)
	clone.FarmWorker.Routes[0].ShardRevision++
	if clone.FarmWorker.Routes[0].ShardRevision == snapshot.FarmWorker.Routes[0].ShardRevision {
		t.Fatal("FarmWorker routes alias source snapshot")
	}
}
