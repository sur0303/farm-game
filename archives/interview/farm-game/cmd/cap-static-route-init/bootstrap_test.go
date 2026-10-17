package main

import (
	"context"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/testkit/capstaticroute"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestBootstrapDurableSnapshotAndExactReplay(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	namespace, err := rediskey.NewNamespace("l2r2durable", "z1")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Minute).UnixMilli()
	snapshot := actorrouting.RouteSnapshot{
		Realm: "classic-farm-local-l2-r2", GlobalRevision: 1, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608030001,
		Holders: []actorrouting.HolderRef{
			{InstanceID: "player-svr-0", BootID: "player-svr-0-functional-g1-a1b2c3d4e5f60718293a4b5c", Endpoint: "player:9100", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-svr-0", BootID: "farm-svr-0-functional-g1-a1b2c3d4e5f60718293a4b5c", Endpoint: "farm:9090", State: actorrouting.HolderStateActive, Weight: 1},
		},
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: deadline}}},
		Farm:   actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: deadline}}},
	}
	for attempt := 0; attempt < 2; attempt++ {
		stored, bound, err := commitDurableSnapshot(context.Background(), client, namespace, snapshot)
		if err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		if stored.Revision != 1 || bound.GlobalRevision != 1 || stored.ChecksumSHA256 == "" {
			t.Fatalf("stored/bound=%+v/%+v", stored, bound)
		}
	}
}

func TestBootstrapDurableSnapshotFromStaticInitializer(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	cfg, err := buildConfig(options{
		runID: "durable-init", profileID: "functional", generation: 1, shardCount: 4,
		namespaceEnv: "l2r2durableinit", namespaceZone: "z1", leaseTTL: time.Minute, minimumRemaining: time.Second,
	}, runtimeIdentity{RunID: "durable-init", ProfileID: "functional", Generation: 1, ControllerNonce: "a1b2c3d4e5f60718293a4b5c"})
	if err != nil {
		t.Fatal(err)
	}
	calculator, _ := cfg.Calculator()
	registry, _ := redisauthority.NewScriptRegistry(client)
	if err := registry.EnsureLoaded(t.Context()); err != nil {
		t.Fatal(err)
	}
	store, err := redisauthority.NewStore(client, registry, redisauthority.Options{Profile: cfg.Profile, Resolver: calculator, LeaseLimits: authoritystore.DefaultLeaseLimits()})
	if err != nil {
		t.Fatal(err)
	}
	readerOptions := redisauthority.DefaultAuthorityReaderOptions(cfg.Profile, calculator)
	readerOptions.OperationTimeout = time.Second
	reader, _ := redisauthority.NewShardAuthorityReader(client, readerOptions)
	result, err := (&capstaticroute.Initializer{Store: store, Reader: reader, Config: cfg}).Run(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if result.Snapshot.GlobalRevision != 1 {
		t.Fatalf("snapshot revision=%d", result.Snapshot.GlobalRevision)
	}
	if _, _, err := commitDurableSnapshot(t.Context(), client, cfg.Profile.Namespace, result.Snapshot); err != nil {
		t.Fatal(err)
	}
}
