package actorrouting

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
)

const testHashSeed uint64 = 202608030001

func activeLeaseUntil() int64 { return time.Now().Add(time.Hour).UnixMilli() }

func completeSnapshot() RouteSnapshot {
	leaseUntil := time.Now().Add(time.Hour).UnixMilli()
	return RouteSnapshot{
		Realm: "classic-farm", GlobalRevision: 1,
		HashVersion: HashVersionXXH64V2, HashSeed: testHashSeed,
		Player: DomainSnapshot{
			ShardCount: 4,
			Routes: []ShardRoute{
				{HolderIndex: 0, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: leaseUntil},
				{HolderIndex: 1, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: leaseUntil},
				{HolderIndex: 0, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: leaseUntil},
				{HolderIndex: 1, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: leaseUntil},
			},
		},
		Farm: DomainSnapshot{
			ShardCount: 4,
			Routes: []ShardRoute{
				{HolderIndex: 2, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: leaseUntil},
				{HolderIndex: 3, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: leaseUntil},
				{HolderIndex: 2, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: leaseUntil},
				{HolderIndex: 3, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: leaseUntil},
			},
		},
		Holders: []HolderRef{
			{InstanceID: "player-a", BootID: "player-a-boot", Endpoint: "player-a:9100", State: HolderStateActive, Weight: 1},
			{InstanceID: "player-b", BootID: "player-b-boot", Endpoint: "player-b:9100", State: HolderStateActive, Weight: 1},
			{InstanceID: "farm-a", BootID: "farm-a-boot", Endpoint: "farm-a:9090", State: HolderStateActive, Weight: 1},
			{InstanceID: "farm-b", BootID: "farm-b-boot", Endpoint: "farm-b:9090", State: HolderStateActive, Weight: 1},
		},
	}
}

func TestCalculatorHasFrozenKindSeparatedGolden(t *testing.T) {
	calculator, err := NewCalculator(
		"classic-farm",
		HashProfile{Version: HashVersionXXH64V2, Seed: testHashSeed},
		1024,
		1024,
	)
	if err != nil {
		t.Fatal(err)
	}
	playerInput, err := calculator.canonicalInput(DomainPlayer, 10001)
	if err != nil {
		t.Fatal(err)
	}
	farmInput, err := calculator.canonicalInput(DomainFarm, 10001)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := hex.EncodeToString(playerInput), "4346525400000002000c636c61737369632d6661726d010000000000002711"; got != want {
		t.Fatalf("PLAYER canonical bytes changed: got=%s want=%s", got, want)
	}
	if got, want := hex.EncodeToString(farmInput), "4346525400000002000c636c61737369632d6661726d020000000000002711"; got != want {
		t.Fatalf("FARM canonical bytes changed: got=%s want=%s", got, want)
	}
	playerHasher := xxhash.NewWithSeed(testHashSeed)
	_, _ = playerHasher.Write(playerInput)
	farmHasher := xxhash.NewWithSeed(testHashSeed)
	_, _ = farmHasher.Write(farmInput)
	playerHash := playerHasher.Sum64()
	farmHash := farmHasher.Sum64()
	player, err := calculator.ShardFor(DomainPlayer, 10001)
	if err != nil {
		t.Fatal(err)
	}
	farm, err := calculator.ShardFor(DomainFarm, 10001)
	if err != nil {
		t.Fatal(err)
	}
	// Changing canonical bytes, raw hashes, or shard values is an auth-v1 data
	// migration, not a compatible rolling update.
	if playerHash != 11000288192871273299 || farmHash != 1295869252430091328 || player != 851 || farm != 64 {
		t.Fatalf("routing v2 golden changed: player_hash=%d farm_hash=%d player_shard=%d farm_shard=%d", playerHash, farmHash, player, farm)
	}
	// Address wrapper must stay on the same frozen encoding.
	playerAddr, err := calculator.Shard(Address{Realm: "classic-farm", Kind: KindPlayer, ID: 10001})
	if err != nil {
		t.Fatal(err)
	}
	farmAddr, err := calculator.Shard(Address{Realm: "classic-farm", Kind: KindFarm, ID: 10001})
	if err != nil {
		t.Fatal(err)
	}
	if playerAddr != player || farmAddr != farm {
		t.Fatalf("Address wrapper drifted: player=%d farm=%d", playerAddr, farmAddr)
	}
}

func TestStaticResolverRequiresCompleteAssignments(t *testing.T) {
	// Drop one farm shard while keeping declared shard_count — fail closed.
	snapshot := completeSnapshot()
	snapshot.Farm.Routes = snapshot.Farm.Routes[:3]
	if _, err := NewStaticResolver(snapshot); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("incomplete snapshot err=%v", err)
	}
}

func TestStaticResolverMarksPrototypeProfileAsMigration(t *testing.T) {
	snapshot := completeSnapshot()
	snapshot.HashVersion = 1
	snapshot.HashSeed = 0
	_, err := NewStaticResolver(snapshot)
	if !errors.Is(err, ErrInvalidSnapshot) || !errors.Is(err, ErrHashMigrationRequired) {
		t.Fatalf("prototype profile must be invalid and require migration: %v", err)
	}
}

func TestStaticResolverRejectsLegacyJSONAsMigration(t *testing.T) {
	legacy := `{
	  "realm":"classic-farm","routing_version":1,
	  "domains":[
	    {"actor_kind":"PLAYER","virtual_shard_count":2},
	    {"actor_kind":"FARM","virtual_shard_count":1}
	  ],
	  "assignments":[
	    {"actor_kind":"PLAYER","virtual_shard_id":0,"holder_pod":"p0","endpoint":"p0:1","shard_revision":1,"writer_epoch":1},
	    {"actor_kind":"FARM","virtual_shard_id":0,"holder_pod":"f0","endpoint":"f0:1","shard_revision":1,"writer_epoch":1}
	  ]
	}`
	if _, err := ParseRouteSnapshotJSON([]byte(legacy)); !errors.Is(err, ErrHashMigrationRequired) {
		t.Fatalf("legacy snapshot must require explicit migration, err=%v", err)
	}
}

func TestDomainSeparationSameActorID(t *testing.T) {
	resolver, err := NewStaticResolver(completeSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	player, err := resolver.ResolveActor(context.Background(), DomainPlayer, 10001)
	if err != nil {
		t.Fatal(err)
	}
	farm, err := resolver.ResolveActor(context.Background(), DomainFarm, 10001)
	if err != nil {
		t.Fatal(err)
	}
	if player.Kind != KindPlayer || farm.Kind != KindFarm {
		t.Fatalf("domains mixed: player=%+v farm=%+v", player, farm)
	}
	if player.Endpoint == farm.Endpoint || player.HolderPod == farm.HolderPod {
		t.Fatalf("player/farm must not share holder: player=%+v farm=%+v", player, farm)
	}
}

func TestStaticResolverRejectsCanceledContextBeforeAndAfterLocalResolution(t *testing.T) {
	resolver, err := NewStaticResolver(completeSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.Resolve(ctx, Address{Realm: resolver.Realm(), Kind: KindPlayer, ID: 42}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled resolver call was admitted: %v", err)
	}
	if _, err := resolver.ResolveActor(ctx, DomainPlayer, 42); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ResolveActor call was admitted: %v", err)
	}
}

func TestExpectedWriterEpochSurfaced(t *testing.T) {
	snapshot := completeSnapshot()
	snapshot.Player.Routes[0].ExpectedWriterEpoch = 9
	snapshot.Farm.Routes[0].ExpectedWriterEpoch = 11
	// Force ID 1 into shard 0 for a deterministic epoch check by shrinking to 1 shard.
	snapshot.Player = DomainSnapshot{
		ShardCount: 1,
		Routes:     []ShardRoute{{HolderIndex: 0, State: ShardStateActive, ShardRevision: 2, ExpectedWriterEpoch: 9, ExpectedLeaseEpoch: 9, LeaseUntilMs: activeLeaseUntil()}},
	}
	snapshot.Farm = DomainSnapshot{
		ShardCount: 1,
		Routes:     []ShardRoute{{HolderIndex: 2, State: ShardStateActive, ShardRevision: 3, ExpectedWriterEpoch: 11, ExpectedLeaseEpoch: 11, LeaseUntilMs: activeLeaseUntil()}},
	}
	resolver, err := NewStaticResolver(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	player, err := resolver.ResolveActor(context.Background(), DomainPlayer, 42)
	if err != nil {
		t.Fatal(err)
	}
	farm, err := resolver.ResolveActor(context.Background(), DomainFarm, 42)
	if err != nil {
		t.Fatal(err)
	}
	if player.WriterEpoch != 9 || farm.WriterEpoch != 11 {
		t.Fatalf("ExpectedWriterEpoch not surfaced: player=%d farm=%d", player.WriterEpoch, farm.WriterEpoch)
	}
}

func TestParseRouteSnapshotJSONNewAndRejectsLegacy(t *testing.T) {
	newJSON, err := json.Marshal(completeSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	newSnap, err := ParseRouteSnapshotJSON(newJSON)
	if err != nil {
		t.Fatal(err)
	}
	if newSnap.HashVersion != HashVersionXXH64V2 || newSnap.HashSeed != testHashSeed || newSnap.Player.ShardCount != 4 || len(newSnap.Holders) != 4 {
		t.Fatalf("new snapshot parse: %+v", newSnap)
	}

	legacy := `{
	  "realm":"classic-farm","routing_version":1,
	  "domains":[
	    {"actor_kind":"PLAYER","virtual_shard_count":2},
	    {"actor_kind":"FARM","virtual_shard_count":2}
	  ],
	  "assignments":[
	    {"actor_kind":"PLAYER","virtual_shard_id":0,"holder_pod":"p0","endpoint":"p0:1","shard_revision":1,"writer_epoch":7},
	    {"actor_kind":"PLAYER","virtual_shard_id":1,"holder_pod":"p1","endpoint":"p1:1","shard_revision":1,"writer_epoch":8},
	    {"actor_kind":"FARM","virtual_shard_id":0,"holder_pod":"f0","endpoint":"f0:1","shard_revision":2,"writer_epoch":9},
	    {"actor_kind":"FARM","virtual_shard_id":1,"holder_pod":"f1","endpoint":"f1:1","shard_revision":2,"writer_epoch":10}
	  ]
	}`
	if _, err := ParseRouteSnapshotJSON([]byte(legacy)); !errors.Is(err, ErrHashMigrationRequired) {
		t.Fatalf("legacy snapshot must not be silently migrated: %v", err)
	}
}

func TestRouteSnapshotStateValidationAndResolveFence(t *testing.T) {
	invalidHolder := completeSnapshot()
	invalidHolder.Holders[0].State = HolderState("active")
	if _, err := NewStaticResolver(invalidHolder); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("lowercase holder state accepted: %v", err)
	}

	invalidShard := completeSnapshot()
	invalidShard.Player.Routes[0].State = ShardRouteState("BROKEN")
	if _, err := NewStaticResolver(invalidShard); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("unknown shard state accepted: %v", err)
	}

	drainingShard := completeSnapshot()
	drainingShard.Player = DomainSnapshot{
		ShardCount: 1,
		Routes: []ShardRoute{{
			HolderIndex:         0,
			State:               ShardStateDraining,
			ShardRevision:       2,
			ExpectedWriterEpoch: 1,
		}},
	}
	resolver, err := NewStaticResolver(drainingShard)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveActor(context.Background(), DomainPlayer, 10001); !errors.Is(err, ErrShardDraining) {
		t.Fatalf("draining shard remained routable: %v", err)
	}

	assigningHolder := completeSnapshot()
	assigningHolder.Player = DomainSnapshot{
		ShardCount: 1,
		Routes: []ShardRoute{{
			HolderIndex:         0,
			State:               ShardStateActive,
			ShardRevision:       2,
			ExpectedWriterEpoch: 1,
			ExpectedLeaseEpoch:  1,
			LeaseUntilMs:        activeLeaseUntil(),
		}},
	}
	assigningHolder.Holders[0].State = HolderStateAssigning
	resolver, err = NewStaticResolver(assigningHolder)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveActor(context.Background(), DomainPlayer, 10001); !errors.Is(err, ErrShardDraining) {
		t.Fatalf("assigning holder remained routable: %v", err)
	}
}

func TestWorkerHandoffDrainingIsOneWayWithoutAuthorityGeneration(t *testing.T) {
	previous := completeSnapshot()
	previous.Holders = append(previous.Holders,
		HolderRef{
			InstanceID: "farm-worker-a", BootID: "farm-worker-a-boot",
			Endpoint: "farm-worker-a:9200", State: HolderStateActive, Weight: 1,
		},
		HolderRef{
			InstanceID: "farm-worker-b", BootID: "farm-worker-b-boot",
			Endpoint: "farm-worker-b:9200", State: HolderStateActive, Weight: 1,
		},
	)
	previous.FarmWorker = DomainSnapshot{
		ShardCount: 4,
		Routes: []ShardRoute{
			{
				HolderIndex: 4, State: ShardStateActive, ShardRevision: 3,
				ExpectedWriterEpoch: 4, ExpectedLeaseEpoch: 5,
				LeaseUntilMs: activeLeaseUntil(),
			},
			{
				HolderIndex: 5, State: ShardStateActive, ShardRevision: 3,
				ExpectedWriterEpoch: 4, ExpectedLeaseEpoch: 5,
				LeaseUntilMs: activeLeaseUntil(),
			},
			{
				HolderIndex: 4, State: ShardStateActive, ShardRevision: 3,
				ExpectedWriterEpoch: 4, ExpectedLeaseEpoch: 5,
				LeaseUntilMs: activeLeaseUntil(),
			},
			{
				HolderIndex: 5, State: ShardStateActive, ShardRevision: 3,
				ExpectedWriterEpoch: 4, ExpectedLeaseEpoch: 5,
				LeaseUntilMs: activeLeaseUntil(),
			},
		},
	}
	draining := CloneRouteSnapshot(previous)
	draining.GlobalRevision++
	draining.FarmWorker.Routes[0].State = ShardStateDraining
	if err := ValidateRouteSnapshotUpdate(previous, draining); err != nil {
		t.Fatalf("generic Watch rejected fail-closed draining: %v", err)
	}
	if err := ValidateWorkerHandoffDrainingSnapshotUpdate(previous, draining); err != nil {
		t.Fatalf("worker handoff draining rejected: %v", err)
	}
	reactivated := CloneRouteSnapshot(draining)
	reactivated.GlobalRevision++
	reactivated.FarmWorker.Routes[0].State = ShardStateActive
	if err := ValidateRouteSnapshotUpdate(draining, reactivated); !errors.Is(
		err,
		ErrSnapshotGenerationRollback,
	) {
		t.Fatalf("same-generation reactivation err=%v", err)
	}
	activated := CloneRouteSnapshot(draining)
	activated.GlobalRevision++
	activated.FarmWorker.Routes[0] = ShardRoute{
		HolderIndex:         5,
		State:               ShardStateActive,
		ShardRevision:       4,
		ExpectedWriterEpoch: 5,
		ExpectedLeaseEpoch:  6,
		LeaseUntilMs:        activeLeaseUntil(),
	}
	if err := ValidateWorkerHandoffActivationSnapshotUpdate(
		draining,
		activated,
	); err != nil {
		t.Fatalf("worker handoff activation rejected: %v", err)
	}
	incomplete := CloneRouteSnapshot(activated)
	incomplete.FarmWorker.Routes[0].ExpectedWriterEpoch = 4
	if err := ValidateWorkerHandoffActivationSnapshotUpdate(
		draining,
		incomplete,
	); !errors.Is(err, ErrSnapshotGenerationRollback) &&
		!errors.Is(err, ErrSnapshotHolderGeneration) {
		t.Fatalf("incomplete worker activation err=%v", err)
	}
	two := CloneRouteSnapshot(draining)
	two.FarmWorker.Routes[1].State = ShardStateDraining
	if err := ValidateWorkerHandoffDrainingSnapshotUpdate(previous, two); !errors.Is(
		err,
		ErrSnapshotGenerationRollback,
	) {
		t.Fatalf("multi-shard worker draining err=%v", err)
	}
}

func TestActorRouteClientObservesResolverSwapWithoutActorCache(t *testing.T) {
	snapshot := completeSnapshot()
	snapshot.Player = DomainSnapshot{ShardCount: 1, Routes: []ShardRoute{{HolderIndex: 0, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: activeLeaseUntil()}}}
	snapshot.Farm = DomainSnapshot{ShardCount: 1, Routes: []ShardRoute{{HolderIndex: 2, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: activeLeaseUntil()}}}
	resolver, err := NewStaticResolver(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	client, _ := NewActorRouteClient(resolver, time.Hour)
	address := Address{Realm: "classic-farm", Kind: KindFarm, ID: 10001}
	first, err := client.Resolve(context.Background(), address)
	if err != nil {
		t.Fatal(err)
	}
	next := cloneRouteSnapshot(snapshot)
	next.GlobalRevision = 2
	next.Holders[2] = HolderRef{InstanceID: "farm-z", BootID: "farm-z-boot", Endpoint: "farm-z:9090", State: HolderStateActive, Weight: 1}
	next.Farm.Routes[0].ShardRevision = 2
	next.Farm.Routes[0].ExpectedWriterEpoch = 2
	next.Farm.Routes[0].ExpectedLeaseEpoch = 2
	if err := resolver.Replace(next); err != nil {
		t.Fatal(err)
	}
	second, err := client.Resolve(context.Background(), address)
	if err != nil {
		t.Fatal(err)
	}
	if first.HolderPod == second.HolderPod || second.HolderPod != "farm-z" {
		t.Fatalf("client retained stale actor route: first=%+v second=%+v", first, second)
	}
	client.Invalidate(address, second.ShardRevision) // compatibility no-op
}

func TestRouteSnapshotRejectsIncompleteActiveAuthorityTuple(t *testing.T) {
	badBoot := completeSnapshot()
	badBoot.Holders[0].BootID = ""
	if _, err := NewStaticResolver(badBoot); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("empty active holder boot accepted: %v", err)
	}
	badLease := completeSnapshot()
	badLease.Player.Routes[0].ExpectedLeaseEpoch = 0
	if _, err := NewStaticResolver(badLease); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("zero active lease epoch accepted: %v", err)
	}
	badDeadline := completeSnapshot()
	badDeadline.Player.Routes[0].LeaseUntilMs = 0
	if _, err := NewStaticResolver(badDeadline); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("zero active authority lease deadline accepted: %v", err)
	}
}

func TestRouteSnapshotRejectsSharedHolderIndexAcrossDomains(t *testing.T) {
	bad := completeSnapshot()
	bad.Farm.Routes[0].HolderIndex = bad.Player.Routes[0].HolderIndex
	if _, err := NewStaticResolver(bad); !errors.Is(err, ErrHolderNamespaceConflict) {
		t.Fatalf("shared PLAYER/FARM holder index accepted: %v", err)
	}
}

func TestStaticResolverRejectsHigherGlobalRevisionPerShardRollback(t *testing.T) {
	base := completeSnapshot()
	base.Player.Routes[0].ShardRevision = 7
	base.Player.Routes[0].ExpectedWriterEpoch = 9
	base.Player.Routes[0].ExpectedLeaseEpoch = 11
	resolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	next := cloneRouteSnapshot(base)
	next.GlobalRevision++
	next.Player.Routes[0].ShardRevision = 6
	next.Player.Routes[0].ExpectedWriterEpoch = 8
	next.Player.Routes[0].ExpectedLeaseEpoch = 10
	if err := resolver.Replace(next); !errors.Is(err, ErrSnapshotGenerationRollback) {
		t.Fatalf("higher global revision carried a generation rollback: %v", err)
	}
	if got := resolver.Snapshot(); got.GlobalRevision != base.GlobalRevision {
		t.Fatalf("rollback replacement changed resolver revision to %d", got.GlobalRevision)
	}
}

func TestStaticResolverRejectsHolderSwitchWithoutGenerationProgression(t *testing.T) {
	base := completeSnapshot()
	base.Player = DomainSnapshot{ShardCount: 1, Routes: []ShardRoute{{HolderIndex: 0, State: ShardStateActive, ShardRevision: 4, ExpectedWriterEpoch: 4, ExpectedLeaseEpoch: 4, LeaseUntilMs: activeLeaseUntil()}}}
	base.Farm = DomainSnapshot{ShardCount: 1, Routes: []ShardRoute{{HolderIndex: 2, State: ShardStateActive, ShardRevision: 4, ExpectedWriterEpoch: 4, ExpectedLeaseEpoch: 4, LeaseUntilMs: activeLeaseUntil()}}}
	resolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	next := cloneSnapshot(base)
	next.GlobalRevision++
	next.Player.Routes[0].HolderIndex = 1
	if err := resolver.Replace(next); !errors.Is(err, ErrSnapshotHolderGeneration) {
		t.Fatalf("holder switch with unchanged generation err=%v", err)
	}
	if got := resolver.Snapshot(); got.GlobalRevision != base.GlobalRevision || got.Player.Routes[0].HolderIndex != base.Player.Routes[0].HolderIndex {
		t.Fatalf("rejected holder switch mutated resolver: %+v", got.Player.Routes[0])
	}
}

func TestHolderEpochResolverStaleRoute(t *testing.T) {
	resolver, err := NewStaticResolver(completeSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	// Actor 10001 farm lands on a farm-* holder; wrong pod must be stale.
	fn := resolver.HolderEpochResolverWithContext(context.Background(), KindFarm, "player-a")
	if _, err := fn(10001); !errors.Is(err, ErrStaleRoute) {
		t.Fatalf("expected stale route, got %v", err)
	}
}

func TestStaticResolverLegacyHolderEpochResolverFailsClosedWithoutContext(t *testing.T) {
	resolver, err := NewStaticResolver(completeSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.HolderEpochResolver(KindFarm, "farm-a")(10001); !errors.Is(err, ErrContextRequired) {
		t.Fatalf("legacy holder epoch callback did not fail closed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.HolderEpochResolverWithContext(ctx, KindFarm, "farm-a")(10001); !errors.Is(err, context.Canceled) {
		t.Fatalf("context-aware holder epoch callback ignored cancellation: %v", err)
	}
}
