package routecontrol

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"farmgame/internal/actorrouting"
	"farmgame/internal/controlfence"
	"farmgame/internal/holderidentity"
	"farmgame/internal/netendpoint"
	core "farmgame/internal/routecontrol"
	"farmgame/internal/storage/redis/rediskey"
)

func newHolderRedisFixture(t *testing.T, maxHolders int) (*miniredis.Miniredis, *redis.Client, *HolderStore, *core.HolderRegistry, *time.Time) {
	t.Helper()
	now := time.UnixMilli(1_800_000_000_000)
	server := miniredis.RunT(t)
	server.SetTime(now)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	namespace, err := rediskey.NewNamespace("test", "holder")
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultHolderStoreOptions(namespace)
	options.MaxHolders = maxHolders
	options.MaxRecordBytes = 16 << 10
	options.MaxResourceBytes = 4096
	options.RecordTTL = 2 * time.Minute
	options.LeaseTTL = 10 * time.Second
	options.MinLeaseTTL = time.Second
	options.MaxLeaseTTL = time.Minute
	options.Now = func() time.Time { return now }
	store, err := NewHolderStore(client, options)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := core.NewHolderRegistry(store, core.HolderRegistryOptions{
		Now: options.Now, LeaseTTL: options.LeaseTTL, MinLeaseTTL: options.MinLeaseTTL,
		MaxLeaseTTL: options.MaxLeaseTTL, MaxHolderCount: maxHolders,
	})
	if err != nil {
		t.Fatal(err)
	}
	return server, client, store, registry, &now
}

func redisHolderRecord(now time.Time, id, boot string) core.HolderRecord {
	return core.HolderRecord{
		Domain: actorrouting.DomainPlayer, HolderID: id, BootID: boot,
		LogicEndpoint: id + ".internal:9000", HolderControlEndpoint: id + ".internal:9000", TLSDNSIdentity: id + ".internal",
		State: core.HolderStateWarming, HeartbeatAt: now, StartedAt: now,
		LeaseUntil: now.Add(10 * time.Second), Sequence: 1, CapacityWeight: 5,
		ResourceSummary: map[string]string{"cpu": "2"},
	}
}

func holderRegisterArgs(t *testing.T, store *HolderStore, record core.HolderRecord) []any {
	t.Helper()
	recordBytes, resourceWire, recordDigest, err := encodeHolderRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	registry := store.registryOptions()
	return []any{
		store.keys.HashTag(), store.opts.MaxHolders, store.opts.MaxRecordBytes, store.opts.MaxResourceBytes,
		store.opts.RecordTTL.Milliseconds(), store.opts.MinLeaseTTL.Milliseconds(),
		store.opts.MaxLeaseTTL.Milliseconds(), store.opts.ClockSkew.Milliseconds(),
		registry.MaxServingShardCount, registry.MaxCapacityWeight,
		holderRecordSchema, registry.MaxIDBytes, registry.MaxEndpointBytes,
		registry.MaxTLSIdentityBytes, string(record.Domain), record.HolderID, record.BootID,
		record.DeploymentIdentity.PodUID, record.DeploymentIdentity.WorkloadUID,
		ordinalWire(record.DeploymentIdentity), record.LogicEndpoint, record.HolderControlEndpoint,
		record.TLSDNSIdentity, string(record.State),
		boolArg(record.ProcessReady), boolArg(record.Draining), record.ServingShardCount,
		record.SnapshotRevision, record.StartedAt.UnixMilli(), record.HeartbeatAt.UnixMilli(),
		record.LeaseUntil.UnixMilli(), record.Sequence, record.CapacityWeight, resourceWire,
		recordDigest, recordBytes, store.opts.MaxTotalRecordBytes, store.opts.MaxTotalResourceBytes,
	}
}

func rewriteHolderRegisterDerivedArgs(t *testing.T, args []any) {
	t.Helper()
	values := map[string]string{
		"schema_version":          fmt.Sprint(args[10]),
		"domain":                  fmt.Sprint(args[14]),
		"holder_id":               fmt.Sprint(args[15]),
		"boot_id":                 fmt.Sprint(args[16]),
		"pod_uid":                 fmt.Sprint(args[17]),
		"workload_uid":            fmt.Sprint(args[18]),
		"ordinal":                 fmt.Sprint(args[19]),
		"logic_endpoint":          fmt.Sprint(args[20]),
		"holder_control_endpoint": fmt.Sprint(args[21]),
		"tls_dns_identity":        fmt.Sprint(args[22]),
		"state":                   fmt.Sprint(args[23]),
		"process_ready":           fmt.Sprint(args[24]),
		"draining":                fmt.Sprint(args[25]),
		"serving_shard_count":     fmt.Sprint(args[26]),
		"snapshot_revision":       fmt.Sprint(args[27]),
		"started_at_ms":           fmt.Sprint(args[28]),
		"heartbeat_at_ms":         fmt.Sprint(args[29]),
		"lease_until_ms":          fmt.Sprint(args[30]),
		"sequence":                fmt.Sprint(args[31]),
		"capacity_weight":         fmt.Sprint(args[32]),
		"resource_summary":        fmt.Sprint(args[33]),
	}
	digest, err := holderSemanticDigest(values)
	if err != nil {
		t.Fatal(err)
	}
	args[34] = digest
	values["record_digest"] = digest
	recordBytes, err := holderRecordWireBytes(values)
	if err != nil {
		t.Fatal(err)
	}
	args[35] = recordBytes
}

func holderHeartbeatArgs(
	t *testing.T,
	store *HolderStore,
	current core.HolderRecord,
	heartbeat core.HolderHeartbeat,
) []any {
	t.Helper()
	candidate := holderRecordAfterStoreHeartbeat(current, heartbeat)
	recordBytes, resourceWire, recordDigest, err := encodeHolderRecord(candidate)
	if err != nil {
		t.Fatal(err)
	}
	registry := store.registryOptions()
	return []any{
		store.keys.HashTag(), store.opts.MaxHolders, store.opts.MaxRecordBytes, store.opts.MaxResourceBytes,
		store.opts.RecordTTL.Milliseconds(), store.opts.MinLeaseTTL.Milliseconds(),
		store.opts.MaxLeaseTTL.Milliseconds(), store.opts.ClockSkew.Milliseconds(),
		registry.MaxServingShardCount, registry.MaxCapacityWeight,
		holderRecordSchema, registry.MaxIDBytes, registry.MaxEndpointBytes,
		registry.MaxTLSIdentityBytes, string(heartbeat.Domain), heartbeat.HolderID, heartbeat.BootID,
		string(heartbeat.State), boolArg(heartbeat.ProcessReady), boolArg(heartbeat.Draining),
		heartbeat.ServingShardCount, heartbeat.SnapshotRevision, heartbeat.HeartbeatAt.UnixMilli(),
		heartbeat.LeaseUntil.UnixMilli(), heartbeat.Sequence, heartbeat.CapacityWeight,
		resourceWire, recordDigest, recordBytes, store.opts.MaxTotalRecordBytes,
		store.opts.MaxTotalResourceBytes,
	}
}

func holderRecordAfterStoreHeartbeat(current core.HolderRecord, heartbeat core.HolderHeartbeat) core.HolderRecord {
	current.State = heartbeat.State
	current.ProcessReady = heartbeat.ProcessReady
	current.Draining = heartbeat.Draining
	current.ServingShardCount = heartbeat.ServingShardCount
	current.SnapshotRevision = heartbeat.SnapshotRevision
	current.HeartbeatAt = heartbeat.HeartbeatAt
	current.LeaseUntil = heartbeat.LeaseUntil
	current.Sequence = heartbeat.Sequence
	current.CapacityWeight = heartbeat.CapacityWeight
	current.ResourceSummary = heartbeat.ResourceSummary
	return current
}

func rewriteHolderDerivedFields(t *testing.T, client *redis.Client, key rediskey.RedisKey, changes map[string]string) {
	t.Helper()
	values, err := client.HGetAll(context.Background(), key.String()).Result()
	if err != nil {
		t.Fatal(err)
	}
	for field, value := range changes {
		values[field] = value
	}
	digest, err := holderSemanticDigest(values)
	if err != nil {
		t.Fatal(err)
	}
	values["record_digest"] = digest
	recordBytes, err := holderRecordWireBytes(values)
	if err != nil {
		t.Fatal(err)
	}
	values["record_bytes"] = fmt.Sprintf("%d", recordBytes)
	integrity, err := holderRecordIntegrity(values)
	if err != nil {
		t.Fatal(err)
	}
	values["record_integrity"] = integrity
	if err := client.HSet(context.Background(), key.String(), values).Err(); err != nil {
		t.Fatal(err)
	}
}

func TestHolderStoreV5UsesIsolatedV2Keyspace(t *testing.T) {
	_, _, store, _, _ := newHolderRedisFixture(t, 4)
	if !strings.HasSuffix(store.Keys().HashTag(), ":holders:v2") {
		t.Fatalf("hash tag=%q", store.Keys().HashTag())
	}
	if holderRecordSchema != "holder-record-v5" || holderMetaSchema != "holder-registry-v5" {
		t.Fatalf("record=%q meta=%q", holderRecordSchema, holderMetaSchema)
	}
}

func TestHolderStorePersistsExactDeploymentIdentityAndHeartbeatPreservesIt(t *testing.T) {
	_, client, store, registry, now := newHolderRedisFixture(t, 4)
	record := redisHolderRecord(*now, "holder-exact", "boot-exact")
	record.DeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-uid", WorkloadUID: "workload-uid", Ordinal: 2}
	registered, err := registry.Register(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	if registered.DeploymentIdentity != record.DeploymentIdentity {
		t.Fatalf("registered=%+v", registered)
	}
	key, err := store.Keys().Record(record.Domain, record.HolderID)
	if err != nil {
		t.Fatal(err)
	}
	values, err := client.HMGet(context.Background(), key.String(), "schema_version", "pod_uid", "workload_uid", "ordinal").Result()
	if err != nil || !reflect.DeepEqual(values, []any{holderRecordSchema, "pod-uid", "workload-uid", "2"}) {
		t.Fatalf("values=%v err=%v", values, err)
	}
	updated, err := registry.Heartbeat(context.Background(), core.HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: core.HolderStateReadyZeroShard, ProcessReady: true, Sequence: 2,
		HeartbeatAt: now.Add(time.Second), LeaseUntil: now.Add(11 * time.Second), CapacityWeight: record.CapacityWeight,
	})
	if err != nil || updated.DeploymentIdentity != record.DeploymentIdentity {
		t.Fatalf("updated=%+v err=%v", updated, err)
	}
}

func TestHolderStoreRejectsDeploymentIdentityCorruptionAndSameBootDrift(t *testing.T) {
	_, client, store, registry, now := newHolderRedisFixture(t, 4)
	record := redisHolderRecord(*now, "holder-exact", "boot-exact")
	record.DeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-uid", WorkloadUID: "workload-uid", Ordinal: 0}
	if _, err := registry.Register(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	drift := record
	drift.DeploymentIdentity.PodUID = "replacement-pod"
	if _, err := registry.Register(context.Background(), drift); !errors.Is(err, core.ErrHolderBootConflict) {
		t.Fatalf("same boot drift err=%v", err)
	}
	key, err := store.Keys().Record(record.Domain, record.HolderID)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(context.Background(), key.String(), "pod_uid", "corrupt").Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.GetHolder(context.Background(), record.Domain, record.HolderID); !errors.Is(err, ErrHolderStoreCorrupt) {
		t.Fatalf("corruption err=%v", err)
	}
}

func TestHolderStoreRegisterHeartbeatLifecycleAndScriptSHA(t *testing.T) {
	server, client, store, registry, now := newHolderRedisFixture(t, 4)
	record, err := registry.Register(context.Background(), redisHolderRecord(*now, "holder-a", "boot-a"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.ScriptSHA(ScriptRegisterHolder); !ok {
		t.Fatal("register script SHA missing")
	}
	if _, ok := store.ScriptSHA(ScriptHeartbeatHolder); !ok {
		t.Fatal("heartbeat script SHA missing")
	}
	if _, ok := store.ScriptSHA(ScriptGetPlacementExclusion); !ok {
		t.Fatal("placement read script SHA missing")
	}
	if _, ok := store.ScriptSHA(ScriptExcludePlacement); !ok {
		t.Fatal("placement exclusion script SHA missing")
	}
	if err := store.Keys().AssertSameSlot(store.Keys().Meta(), store.Keys().Index()); err != nil {
		t.Fatal(err)
	}
	record, err = registry.Heartbeat(context.Background(), core.HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: core.HolderStateReadyZeroShard, ProcessReady: true,
		HeartbeatAt: now.Add(time.Second), LeaseUntil: now.Add(11 * time.Second),
		Sequence: 2, CapacityWeight: 5, SnapshotRevision: 2,
	})
	if err != nil || record.State != core.HolderStateReadyZeroShard {
		t.Fatalf("ready=%+v err=%v", record, err)
	}
	if _, err := registry.Heartbeat(context.Background(), core.HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: core.HolderStateReadyZeroShard, ProcessReady: true,
		HeartbeatAt: now.Add(2 * time.Second), LeaseUntil: now.Add(12 * time.Second),
		Sequence: 2, CapacityWeight: 5, SnapshotRevision: 2,
	}); !errors.Is(err, core.ErrHolderHeartbeatReplay) {
		t.Fatalf("replay err=%v", err)
	}
	server.FastForward(2 * time.Minute)
	if exists, err := store.ExistsHolderForTest(context.Background(), record.Domain, record.HolderID); err != nil || exists {
		t.Fatalf("physical TTL exists=%t err=%v", exists, err)
	}
	_ = client
}

func TestHolderStorePlacementReleaseIsOwnerCASAndKeepsShape(t *testing.T) {
	_, client, store, registry, now := newHolderRedisFixture(t, 4)
	record := redisHolderRecord(*now, "holder-release", "boot-release")
	if _, err := registry.Register(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	baseline, found, err := store.GetPlacementExclusion(context.Background(), record.Domain, record.HolderID, record.BootID)
	if err != nil || !found || baseline.Excluded || baseline.Revision != 1 {
		t.Fatalf("baseline=%+v found=%t err=%v", baseline, found, err)
	}
	excluded, err := store.ExcludePlacement(context.Background(), record.Domain, record.HolderID, record.BootID,
		core.PlacementExclusionPrecondition{ExpectedRevision: baseline.Revision, OwnerPlanID: "plan-a"})
	if err != nil || !excluded.Committed || !excluded.State.Excluded || excluded.State.Revision != 2 {
		t.Fatalf("exclude=%+v err=%v", excluded, err)
	}
	issuer := controlfence.NewIssuer()
	token := issuer.Issue("release-op", 1)
	released, err := store.ReleasePlacementExclusionForOwner(context.Background(), token, record.Domain, record.HolderID, record.BootID,
		core.PlacementExclusionRelease{OwnerPlanID: "plan-a", ExpectedRevision: 2})
	if err != nil || !released.Committed || released.Outcome != core.PlacementReleaseReleased || released.State.Excluded || released.State.Revision != 3 {
		t.Fatalf("release=%+v err=%v", released, err)
	}
	placementKey, err := store.Keys().PlacementRecord(record.Domain, record.HolderID)
	if err != nil {
		t.Fatal(err)
	}
	if fields := client.HLen(context.Background(), placementKey.String()).Val(); fields != 8 {
		t.Fatalf("placement shape fields=%d", fields)
	}
	replay, err := store.ReleasePlacementExclusionForOwner(context.Background(), token, record.Domain, record.HolderID, record.BootID,
		core.PlacementExclusionRelease{OwnerPlanID: "plan-a", ExpectedRevision: 2})
	if err != nil || replay.Outcome != core.PlacementReleaseAlreadyReleased || replay.State.Revision != 3 {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	reexcluded, err := store.ExcludePlacement(context.Background(), record.Domain, record.HolderID, record.BootID,
		core.PlacementExclusionPrecondition{ExpectedRevision: 3, OwnerPlanID: "plan-b"})
	if err != nil || !reexcluded.Committed {
		t.Fatalf("re-exclude=%+v err=%v", reexcluded, err)
	}
	wrongOwner, err := store.ReleasePlacementExclusionForOwner(context.Background(), token, record.Domain, record.HolderID, record.BootID,
		core.PlacementExclusionRelease{OwnerPlanID: "plan-a", ExpectedRevision: 4})
	if err != nil || wrongOwner.Outcome != core.PlacementReleaseOwnerMismatch || wrongOwner.State.OwnerPlanID != "plan-b" {
		t.Fatalf("wrong owner=%+v err=%v", wrongOwner, err)
	}
	rollover, err := store.ReleasePlacementExclusionForOwner(context.Background(), token, record.Domain, record.HolderID, "new-boot",
		core.PlacementExclusionRelease{OwnerPlanID: "plan-b", ExpectedRevision: 4})
	if err != nil || rollover.Outcome != core.PlacementReleaseByBootRollover {
		t.Fatalf("rollover=%+v err=%v", rollover, err)
	}
}

func TestHolderStoreFarmWorkerLifecycle(t *testing.T) {
	_, _, store, registry, now := newHolderRedisFixture(t, 4)
	input := redisHolderRecord(*now, "farm-worker-a", "worker-a-boot")
	input.Domain = actorrouting.DomainFarmWorker
	input.LogicEndpoint = "farm-worker-a.internal:9201"
	input.HolderControlEndpoint = "farm-worker-a.internal:19201"
	record, err := registry.Register(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	record, err = registry.Heartbeat(context.Background(), core.HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: core.HolderStateReadyZeroShard, ProcessReady: true,
		HeartbeatAt: now.Add(time.Second), LeaseUntil: now.Add(11 * time.Second),
		Sequence: 2, CapacityWeight: record.CapacityWeight, SnapshotRevision: 1,
		ResourceSummary: record.ResourceSummary,
	})
	if err != nil || record.State != core.HolderStateReadyZeroShard {
		t.Fatalf("ready=%+v err=%v", record, err)
	}
	workers, err := store.ListHolders(context.Background(), actorrouting.DomainFarmWorker)
	if err != nil || len(workers) != 1 || workers[0].HolderID != input.HolderID {
		t.Fatalf("workers=%+v err=%v", workers, err)
	}
	farms, err := store.ListHolders(context.Background(), actorrouting.DomainFarm)
	if err != nil || len(farms) != 0 {
		t.Fatalf("FARM holders=%+v err=%v", farms, err)
	}
}

func TestHolderStoreCanonicalEndpointGoLuaParity(t *testing.T) {
	for index, endpoint := range []string{"holder.internal:443", "127.0.0.1:9443", "[::1]:9443", "[2001:db8::1]:65535"} {
		t.Run(endpoint, func(t *testing.T) {
			_, _, store, registry, now := newHolderRedisFixture(t, 4)
			record := redisHolderRecord(*now, fmt.Sprintf("holder-%d", index), fmt.Sprintf("boot-%d", index))
			record.LogicEndpoint = endpoint
			record.HolderControlEndpoint = endpoint
			stored, err := registry.Register(context.Background(), record)
			if err != nil || stored.LogicEndpoint != endpoint || stored.HolderControlEndpoint != endpoint {
				t.Fatalf("endpoint=%q stored=%+v err=%v", endpoint, stored, err)
			}
			loaded, found, err := store.GetHolder(context.Background(), record.Domain, record.HolderID)
			if err != nil || !found || loaded.LogicEndpoint != endpoint {
				t.Fatalf("endpoint=%q loaded=%+v found=%t err=%v", endpoint, loaded, found, err)
			}
		})
	}
}

func TestHolderStoreWrongTypeCapacityAndExpiredBootReplacement(t *testing.T) {
	server, client, store, registry, now := newHolderRedisFixture(t, 1)
	first := redisHolderRecord(*now, "holder-a", "boot-a")
	if _, err := registry.Register(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := redisHolderRecord(*now, "holder-b", "boot-b")
	if _, err := registry.Register(context.Background(), second); !errors.Is(err, core.ErrHolderRegistryCapacity) {
		t.Fatalf("capacity err=%v", err)
	}
	recordKey, err := store.Keys().Record(actorrouting.DomainPlayer, "holder-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Set(context.Background(), recordKey.String(), "wrong", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.GetHolder(context.Background(), actorrouting.DomainPlayer, "holder-a"); !errors.Is(err, ErrHolderStoreCorrupt) {
		t.Fatalf("wrong type err=%v", err)
	}
	if err := store.DeleteHolderForTest(context.Background(), actorrouting.DomainPlayer, "holder-a"); err != nil {
		t.Fatal(err)
	}
	*now = time.UnixMilli(1_800_000_000_000 + int64(2*time.Minute/time.Millisecond))
	server.SetTime(*now)
	replacement := first
	replacement.BootID = "boot-b"
	replacement.StartedAt = *now
	replacement.HeartbeatAt = *now
	replacement.LeaseUntil = now.Add(10 * time.Second)
	if _, err := registry.Register(context.Background(), replacement); err != nil {
		t.Fatalf("replacement after exact cleanup err=%v", err)
	}
}

func TestHolderStoreOldBootCannotOverwriteEitherEndpoint(t *testing.T) {
	server, _, store, registry, now := newHolderRedisFixture(t, 1)
	oldBoot := redisHolderRecord(*now, "holder-a", "boot-a")
	oldBoot.LogicEndpoint = "old.logic:9000"
	oldBoot.HolderControlEndpoint = "old.control:9443"
	if _, err := registry.Register(context.Background(), oldBoot); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(11 * time.Second)
	server.SetTime(*now)
	newBoot := redisHolderRecord(*now, "holder-a", "boot-b")
	newBoot.LogicEndpoint = "new.logic:9000"
	newBoot.HolderControlEndpoint = "new.control:9443"
	if _, err := registry.Register(context.Background(), newBoot); err != nil {
		t.Fatal(err)
	}
	oldBoot.StartedAt = *now
	oldBoot.HeartbeatAt = *now
	oldBoot.LeaseUntil = now.Add(10 * time.Second)
	oldBoot.LogicEndpoint = "overwrite.logic:9000"
	oldBoot.HolderControlEndpoint = "overwrite.control:9443"
	if _, err := store.RegisterHolder(context.Background(), oldBoot); !errors.Is(err, core.ErrHolderBootConflict) {
		t.Fatalf("old boot register err=%v", err)
	}
	stored, found, err := store.GetHolder(context.Background(), newBoot.Domain, newBoot.HolderID)
	if err != nil || !found {
		t.Fatalf("stored found=%v err=%v", found, err)
	}
	if stored.BootID != newBoot.BootID || stored.LogicEndpoint != newBoot.LogicEndpoint ||
		stored.HolderControlEndpoint != newBoot.HolderControlEndpoint {
		t.Fatalf("new boot overwritten: %+v", stored)
	}
}

func TestHolderStoreRejectsLegacyAmbiguousSchema(t *testing.T) {
	_, client, store, registry, now := newHolderRedisFixture(t, 1)
	record := redisHolderRecord(*now, "holder-legacy", "boot-legacy")
	if _, err := registry.Register(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	key, err := store.Keys().Record(record.Domain, record.HolderID)
	if err != nil {
		t.Fatal(err)
	}
	pipe := client.TxPipeline()
	pipe.HSet(context.Background(), key.String(), "schema_version", "holder-record-v3")
	pipe.HDel(context.Background(), key.String(), "holder_control_endpoint")
	if _, err := pipe.Exec(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.GetHolder(context.Background(), record.Domain, record.HolderID); !errors.Is(err, ErrHolderStoreCorrupt) {
		t.Fatalf("legacy ambiguous record err=%v", err)
	}
}

func TestHolderStoreConcurrentExactRegistrationHasOneRecord(t *testing.T) {
	_, _, store, _, now := newHolderRedisFixture(t, 8)
	input := redisHolderRecord(*now, "holder-a", "boot-a")
	const workers = 20
	results := make(chan core.HolderRecord, workers)
	errs := make(chan error, workers)
	var wait sync.WaitGroup
	wait.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wait.Done()
			record, err := store.RegisterHolder(context.Background(), input)
			results <- record
			errs <- err
		}()
	}
	wait.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	count := 0
	for record := range results {
		if record.HolderID == input.HolderID && record.BootID == input.BootID {
			count++
		}
	}
	if count != workers {
		t.Fatalf("successful records=%d want=%d", count, workers)
	}
	records, err := store.ListHolders(context.Background(), actorrouting.DomainPlayer)
	if err != nil || len(records) != 1 {
		t.Fatalf("list=%+v err=%v", records, err)
	}
}

func TestHolderStoreStaleMixedHolderListAndRegisterReclaimsCounters(t *testing.T) {
	server, client, store, registry, now := newHolderRedisFixture(t, 2)
	base := *now
	a := redisHolderRecord(base, "holder-a", "boot-a")
	b := redisHolderRecord(base, "holder-b", "boot-b")
	if _, err := registry.Register(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Register(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Heartbeat(context.Background(), core.HolderHeartbeat{
		Domain: b.Domain, HolderID: b.HolderID, BootID: b.BootID,
		State: core.HolderStateReadyZeroShard, ProcessReady: true,
		HeartbeatAt: base.Add(5 * time.Second), LeaseUntil: base.Add(15 * time.Second),
		Sequence: 2, CapacityWeight: b.CapacityWeight,
	}); err != nil {
		t.Fatal(err)
	}
	aKey, err := store.Keys().Record(a.Domain, a.HolderID)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.PExpire(context.Background(), aKey.String(), time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	server.FastForward(2 * time.Millisecond)
	*now = base.Add(11 * time.Second)
	server.SetTime(*now)

	records, err := registry.List(context.Background(), actorrouting.DomainPlayer)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].HolderID != b.HolderID {
		t.Fatalf("stale list=%+v", records)
	}

	c := redisHolderRecord(*now, "holder-c", "boot-c")
	if _, err := registry.Register(context.Background(), c); err != nil {
		t.Fatalf("register after stale cleanup: %v", err)
	}
	records, err = store.ListHolders(context.Background(), actorrouting.DomainPlayer)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("reclaimed list=%+v", records)
	}
	meta, err := client.HGetAll(context.Background(), store.Keys().Meta().String()).Result()
	if err != nil {
		t.Fatal(err)
	}
	if meta["holder_count"] != "2" {
		t.Fatalf("meta count=%v", meta)
	}
	index, err := client.ZRangeWithScores(context.Background(), store.Keys().Index().String(), 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(index) != 2 {
		t.Fatalf("index=%+v", index)
	}
	for _, entry := range index {
		member, ok := redisMemberString(entry.Member)
		if !ok {
			t.Fatalf("member type=%T", entry.Member)
		}
		domain, holderID, err := parseHolderMember(member)
		if err != nil {
			t.Fatal(err)
		}
		record, found, err := store.GetHolder(context.Background(), domain, holderID)
		if err != nil || !found || !scoreMatchesLease(entry.Score, record.LeaseUntil) {
			t.Fatalf("member=%q record=%+v found=%t err=%v score=%v", member, record, found, err, entry.Score)
		}
	}
}

func TestHolderStoreAggregateResourceLimitIsAcrossRecords(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	server := miniredis.RunT(t)
	server.SetTime(now)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	namespace, err := rediskey.NewNamespace("test", "holder-resource")
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultHolderStoreOptions(namespace)
	options.MaxHolders = 4
	options.MaxRecordBytes = 16 << 10
	options.MaxTotalRecordBytes = 64 << 10
	options.MaxResourceBytes = 128
	options.MaxTotalResourceBytes = 25
	options.RecordTTL = 2 * time.Minute
	options.LeaseTTL = 10 * time.Second
	options.MinLeaseTTL = time.Second
	options.MaxLeaseTTL = time.Minute
	options.Now = func() time.Time { return now }
	store, err := NewHolderStore(client, options)
	if err != nil {
		t.Fatal(err)
	}
	first := redisHolderRecord(now, "holder-a", "boot-a")
	first.ResourceSummary = map[string]string{"cpu": "123456"}
	if _, err := store.RegisterHolder(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := redisHolderRecord(now, "holder-b", "boot-b")
	second.ResourceSummary = map[string]string{"cpu": "654321"}
	if _, err := store.RegisterHolder(context.Background(), second); !errors.Is(err, ErrHolderStoreCapacity) {
		t.Fatalf("aggregate resource err=%v", err)
	}
	records, err := store.ListHolders(context.Background(), actorrouting.DomainPlayer)
	if err != nil || len(records) != 1 {
		t.Fatalf("records=%+v err=%v", records, err)
	}
}

func TestHolderStoreRejectsWrapped2049MaxAccountingInGoAndLua(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("the 2049-value int wrap fixture is specific to 64-bit int")
	}
	now := time.UnixMilli(1_800_000_000_000)
	server := miniredis.RunT(t)
	server.SetTime(now)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	namespace, err := rediskey.NewNamespace("test", "holder-accounting-wrap")
	if err != nil {
		t.Fatal(err)
	}
	const sidecarCount = 2049
	maxValue := int(maxLuaSafeInteger)
	options := DefaultHolderStoreOptions(namespace)
	options.MaxHolders = sidecarCount
	options.MaxRecordBytes = maxValue
	options.MaxTotalRecordBytes = maxValue
	options.MaxResourceBytes = maxValue
	options.MaxTotalResourceBytes = maxValue
	options.Now = func() time.Time { return now }
	store, err := NewHolderStore(client, options)
	if err != nil {
		t.Fatal(err)
	}

	entries := make([]redis.Z, 0, sidecarCount)
	accounting := make(map[string]string, sidecarCount)
	for index := 0; index < sidecarCount; index++ {
		holderID := fmt.Sprintf("wrapped-%04d", index)
		member, err := store.Keys().Member(actorrouting.DomainPlayer, holderID)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, redis.Z{
			Score: float64(now.Add(-time.Second).UnixMilli()), Member: member,
		})
		accounting[member] = holderAccountingValue(maxValue, maxValue)
	}
	if err := client.ZAdd(context.Background(), store.Keys().Index().String(), entries...).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(context.Background(), store.Keys().Accounting().String(), accounting).Err(); err != nil {
		t.Fatal(err)
	}
	// On a 64-bit int, 2049*(2^53-1) wraps modulo 2^64 to
	// (2^53-1)-2048. The forged meta counters model the value that the old
	// unchecked Go additions could mistake for a valid bounded aggregate.
	wrappedTotal := maxValue - (sidecarCount - 1)
	meta := map[string]any{
		"schema_version":    holderMetaSchema,
		"registry_revision": "1",
		"holder_count":      strconv.Itoa(sidecarCount),
		"record_bytes":      strconv.Itoa(wrappedTotal),
		"resource_bytes":    strconv.Itoa(wrappedTotal),
	}
	if err := client.HSet(context.Background(), store.Keys().Meta().String(), meta).Err(); err != nil {
		t.Fatal(err)
	}

	if _, err := store.ListHolders(context.Background(), actorrouting.DomainPlayer); !errors.Is(err, ErrHolderStoreCorrupt) {
		t.Fatalf("Go snapshot accepted wrapped accounting err=%v", err)
	}
	candidate := redisHolderRecord(now, "holder-candidate", "boot-candidate")
	if _, err := store.RegisterHolder(context.Background(), candidate); !errors.Is(err, ErrHolderStoreCorrupt) {
		t.Fatalf("Lua reconcile accepted wrapped accounting err=%v", err)
	}
	if indexed, err := client.ZCard(context.Background(), store.Keys().Index().String()).Result(); err != nil || indexed != sidecarCount {
		t.Fatalf("Lua corrupt return mutated index count=%d err=%v", indexed, err)
	}
	if accounted, err := client.HLen(context.Background(), store.Keys().Accounting().String()).Result(); err != nil || accounted != sidecarCount {
		t.Fatalf("Lua corrupt return mutated accounting count=%d err=%v", accounted, err)
	}
	gotMeta, err := client.HGetAll(context.Background(), store.Keys().Meta().String()).Result()
	if err != nil {
		t.Fatal(err)
	}
	for field, want := range meta {
		if got := gotMeta[field]; got != fmt.Sprint(want) {
			t.Fatalf("Lua corrupt return mutated meta field=%s got=%q want=%q", field, got, want)
		}
	}
	candidateKey, err := store.Keys().Record(candidate.Domain, candidate.HolderID)
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := client.Exists(context.Background(), candidateKey.String()).Result(); err != nil || exists != 0 {
		t.Fatalf("Lua corrupt return created candidate exists=%d err=%v", exists, err)
	}
}

func TestHolderStoreConstructionRejectsRingAndSubMillisecondOptions(t *testing.T) {
	namespace, err := rediskey.NewNamespace("test", "holder-construction")
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultHolderStoreOptions(namespace)
	ring := redis.NewRing(&redis.RingOptions{Addrs: map[string]string{"default": "127.0.0.1:6379"}})
	t.Cleanup(func() { _ = ring.Close() })
	if _, err := NewHolderStore(ring, options); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("ring err=%v", err)
	}
	type unknownUniversalClient struct {
		redis.UniversalClient
	}
	if _, err := NewHolderStore(unknownUniversalClient{}, options); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("unknown universal client err=%v", err)
	}
	options.OperationTimeout = 500 * time.Microsecond
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() { _ = client.Close() })
	if _, err := NewHolderStore(client, options); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("sub-millisecond err=%v", err)
	}
	options = DefaultHolderStoreOptions(namespace)
	options.MaxLeaseTTL = time.Minute
	options.ClockSkew = 5 * time.Second
	options.RecordTTL = options.MaxLeaseTTL + options.ClockSkew
	if _, err := NewHolderStore(client, options); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("unsafe record TTL relation err=%v", err)
	}
}

func TestHolderStoreRejectsWrongDigestScoreMemberAndMeta(t *testing.T) {
	t.Run("digest", func(t *testing.T) {
		_, client, store, registry, now := newHolderRedisFixture(t, 2)
		record := redisHolderRecord(*now, "holder-a", "boot-a")
		if _, err := registry.Register(context.Background(), record); err != nil {
			t.Fatal(err)
		}
		key, err := store.Keys().Record(record.Domain, record.HolderID)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.HSet(context.Background(), key.String(), "record_digest", strings.Repeat("0", 64)).Err(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.GetHolder(context.Background(), record.Domain, record.HolderID); !errors.Is(err, ErrHolderStoreCorrupt) {
			t.Fatalf("wrong digest err=%v", err)
		}
	})
	t.Run("score", func(t *testing.T) {
		_, client, store, registry, now := newHolderRedisFixture(t, 2)
		record := redisHolderRecord(*now, "holder-a", "boot-a")
		if _, err := registry.Register(context.Background(), record); err != nil {
			t.Fatal(err)
		}
		member, err := store.Keys().Member(record.Domain, record.HolderID)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.ZAdd(context.Background(), store.Keys().Index().String(), redis.Z{Score: 1, Member: member}).Err(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := store.GetHolder(context.Background(), record.Domain, record.HolderID); !errors.Is(err, ErrHolderStoreCorrupt) {
			t.Fatalf("wrong score err=%v", err)
		}
	})
	t.Run("fresh-member-missing-record", func(t *testing.T) {
		_, client, store, registry, now := newHolderRedisFixture(t, 2)
		record := redisHolderRecord(*now, "holder-a", "boot-a")
		if _, err := registry.Register(context.Background(), record); err != nil {
			t.Fatal(err)
		}
		key, member, err := store.recordAndMember(record.Domain, record.HolderID)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Del(context.Background(), key.String()).Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := store.ListHolders(context.Background(), record.Domain); !errors.Is(err, ErrHolderStoreCorrupt) {
			t.Fatalf("fresh missing record list err=%v", err)
		}
		if err := client.ZRem(context.Background(), store.Keys().Index().String(), member).Err(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("meta", func(t *testing.T) {
		_, client, store, registry, now := newHolderRedisFixture(t, 2)
		record := redisHolderRecord(*now, "holder-a", "boot-a")
		if _, err := registry.Register(context.Background(), record); err != nil {
			t.Fatal(err)
		}
		if err := client.HSet(context.Background(), store.Keys().Meta().String(), "record_bytes", "18446744073709551615").Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := store.ListHolders(context.Background(), record.Domain); !errors.Is(err, ErrHolderStoreCorrupt) {
			t.Fatalf("wrong meta err=%v", err)
		}
	})
}

func TestHolderStoreHeartbeatRejectsLeaseAndHeartbeatRegression(t *testing.T) {
	_, _, store, registry, now := newHolderRedisFixture(t, 2)
	record := redisHolderRecord(*now, "holder-a", "boot-a")
	if _, err := registry.Register(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	ready := core.HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: core.HolderStateReadyZeroShard, ProcessReady: true,
		HeartbeatAt: now.Add(time.Second), LeaseUntil: now.Add(11 * time.Second),
		Sequence: 2, CapacityWeight: record.CapacityWeight,
	}
	if _, err := registry.Heartbeat(context.Background(), ready); err != nil {
		t.Fatal(err)
	}
	regressed := ready
	regressed.Sequence = 3
	regressed.HeartbeatAt = *now
	if _, err := store.HeartbeatHolder(context.Background(), regressed); !errors.Is(err, ErrHolderStoreStale) {
		t.Fatalf("heartbeat regression err=%v", err)
	}
	regressed = ready
	regressed.Sequence = 3
	regressed.HeartbeatAt = now.Add(2 * time.Second)
	regressed.LeaseUntil = now.Add(10 * time.Second)
	if _, err := store.HeartbeatHolder(context.Background(), regressed); !errors.Is(err, ErrHolderStoreStale) {
		t.Fatalf("lease regression err=%v", err)
	}
}

func TestHolderStoreStaticBootstrapCanHeartbeatWarmingDirectlyToServing(t *testing.T) {
	_, _, store, _, now := newHolderRedisFixture(t, 4)
	registryOptions := core.DefaultHolderRegistryOptions()
	registryOptions.Now = store.opts.Now
	registryOptions.LeaseTTL = store.opts.LeaseTTL
	registryOptions.MinLeaseTTL = store.opts.MinLeaseTTL
	registryOptions.MaxLeaseTTL = store.opts.MaxLeaseTTL
	registryOptions.MaxHolderCount = store.opts.MaxHolders
	registryOptions.InitialServingVerifier = func(context.Context, core.HolderRecord, core.HolderHeartbeat) error { return nil }
	registry, err := core.NewHolderRegistry(store, registryOptions)
	if err != nil {
		t.Fatal(err)
	}
	record := redisHolderRecord(*now, "holder-static", "boot-static")
	if _, err := registry.Register(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	serving, err := registry.Heartbeat(context.Background(), core.HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: core.HolderStateServing, ProcessReady: true, ServingShardCount: 2,
		SnapshotRevision: 7, HeartbeatAt: now.Add(time.Second), LeaseUntil: now.Add(11 * time.Second),
		Sequence: 2, CapacityWeight: record.CapacityWeight,
		ResourceSummary: map[string]string{"cpu": "2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if serving.State != core.HolderStateServing || !serving.ProcessReady || serving.ServingShardCount != 2 {
		t.Fatalf("serving record=%+v", serving)
	}
}

func TestHolderStoreDynamicHolderReturnsToReadyZeroShard(t *testing.T) {
	_, _, store, registry, now := newHolderRedisFixture(t, 4)
	record := redisHolderRecord(*now, "holder-dynamic", "boot-dynamic")
	if _, err := registry.Register(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	ready, err := registry.Heartbeat(context.Background(), core.HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: core.HolderStateReadyZeroShard, ProcessReady: true,
		SnapshotRevision: 1, HeartbeatAt: now.Add(time.Second),
		LeaseUntil: now.Add(11 * time.Second), Sequence: 2,
		CapacityWeight: record.CapacityWeight,
	})
	if err != nil {
		t.Fatal(err)
	}
	serving, err := registry.Heartbeat(context.Background(), core.HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: core.HolderStateServing, ProcessReady: true, ServingShardCount: 1,
		SnapshotRevision: 2, HeartbeatAt: now.Add(2 * time.Second),
		LeaseUntil: now.Add(12 * time.Second), Sequence: 3,
		CapacityWeight: record.CapacityWeight,
	})
	if err != nil {
		t.Fatal(err)
	}
	sameRevision := core.HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: core.HolderStateReadyZeroShard, ProcessReady: true,
		SnapshotRevision: 2, HeartbeatAt: now.Add(3 * time.Second),
		LeaseUntil: now.Add(13 * time.Second), Sequence: 4,
		CapacityWeight: record.CapacityWeight,
	}
	if _, err := store.HeartbeatHolder(
		context.Background(),
		sameRevision,
	); !errors.Is(err, core.ErrHolderStateTransition) {
		t.Fatalf("Lua same-revision zero-shard return err=%v", err)
	}
	if _, err := registry.Heartbeat(
		context.Background(),
		sameRevision,
	); !errors.Is(err, core.ErrHolderStateTransition) {
		t.Fatalf("same-revision zero-shard return err=%v", err)
	}
	returned, err := registry.Heartbeat(context.Background(), core.HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: core.HolderStateReadyZeroShard, ProcessReady: true,
		SnapshotRevision: 3, HeartbeatAt: now.Add(3 * time.Second),
		LeaseUntil: now.Add(13 * time.Second), Sequence: 4,
		CapacityWeight: record.CapacityWeight,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ready.State != core.HolderStateReadyZeroShard ||
		serving.State != core.HolderStateServing ||
		returned.State != core.HolderStateReadyZeroShard ||
		returned.ServingShardCount != 0 {
		t.Fatalf(
			"dynamic lifecycle ready=%+v serving=%+v returned=%+v",
			ready,
			serving,
			returned,
		)
	}
}

func TestHolderStoreFreshNearExpiryCanRenewAndDoesNotBlockOtherMutation(t *testing.T) {
	server, _, store, registry, now := newHolderRedisFixture(t, 4)
	base := *now
	first := redisHolderRecord(base, "holder-a", "boot-a")
	if _, err := registry.Register(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	*now = base.Add(9500 * time.Millisecond)
	server.SetTime(*now)
	if records, err := registry.List(context.Background(), first.Domain); err != nil || len(records) != 1 {
		t.Fatalf("near-expiry list=%+v err=%v", records, err)
	}
	second := redisHolderRecord(*now, "holder-b", "boot-b")
	if _, err := registry.Register(context.Background(), second); err != nil {
		t.Fatalf("near-expiry holder blocked other register: %v", err)
	}
	if _, err := registry.Heartbeat(context.Background(), core.HolderHeartbeat{
		Domain: first.Domain, HolderID: first.HolderID, BootID: first.BootID,
		State: core.HolderStateWarming, HeartbeatAt: *now,
		LeaseUntil: now.Add(10 * time.Second), Sequence: 2, CapacityWeight: first.CapacityWeight,
		ResourceSummary: map[string]string{"cpu": "2"},
	}); err != nil {
		t.Fatalf("near-expiry renewal err=%v", err)
	}
	records, err := store.ListHolders(context.Background(), first.Domain)
	if err != nil || len(records) != 2 {
		t.Fatalf("renewed records=%+v err=%v", records, err)
	}
}

func TestHolderStoreStaleSweepRequiresExactRecordLeaseBinding(t *testing.T) {
	t.Run("expired-score-fresh-record", func(t *testing.T) {
		_, client, store, registry, now := newHolderRedisFixture(t, 4)
		first := redisHolderRecord(*now, "holder-a", "boot-a")
		if _, err := registry.Register(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		member, err := store.Keys().Member(first.Domain, first.HolderID)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.ZAdd(context.Background(), store.Keys().Index().String(), redis.Z{
			Score: float64(now.Add(-time.Second).UnixMilli()), Member: member,
		}).Err(); err != nil {
			t.Fatal(err)
		}
		revision := mustHolderRevision(t, client, store)
		second := redisHolderRecord(*now, "holder-b", "boot-b")
		if _, err := store.RegisterHolder(context.Background(), second); !errors.Is(err, ErrHolderStoreCorrupt) {
			t.Fatalf("register err=%v", err)
		}
		assertHolderMutationRejectedWithoutCleanup(t, client, store, first, second, member, revision)
	})

	t.Run("future-score-expired-record", func(t *testing.T) {
		server, client, store, registry, now := newHolderRedisFixture(t, 4)
		base := *now
		first := redisHolderRecord(base, "holder-a", "boot-a")
		if _, err := registry.Register(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		*now = base.Add(11 * time.Second)
		server.SetTime(*now)
		member, err := store.Keys().Member(first.Domain, first.HolderID)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.ZAdd(context.Background(), store.Keys().Index().String(), redis.Z{
			Score: float64(now.Add(10 * time.Second).UnixMilli()), Member: member,
		}).Err(); err != nil {
			t.Fatal(err)
		}
		revision := mustHolderRevision(t, client, store)
		second := redisHolderRecord(*now, "holder-b", "boot-b")
		if _, err := store.RegisterHolder(context.Background(), second); !errors.Is(err, ErrHolderStoreCorrupt) {
			t.Fatalf("register err=%v", err)
		}
		assertHolderMutationRejectedWithoutCleanup(t, client, store, first, second, member, revision)
	})

	t.Run("corrupt-expired-record", func(t *testing.T) {
		server, client, store, registry, now := newHolderRedisFixture(t, 4)
		base := *now
		first := redisHolderRecord(base, "holder-a", "boot-a")
		if _, err := registry.Register(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		firstKey, err := store.Keys().Record(first.Domain, first.HolderID)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.HSet(context.Background(), firstKey.String(), "unexpected", "field").Err(); err != nil {
			t.Fatal(err)
		}
		*now = base.Add(11 * time.Second)
		server.SetTime(*now)
		member, err := store.Keys().Member(first.Domain, first.HolderID)
		if err != nil {
			t.Fatal(err)
		}
		revision := mustHolderRevision(t, client, store)
		second := redisHolderRecord(*now, "holder-b", "boot-b")
		if _, err := store.RegisterHolder(context.Background(), second); !errors.Is(err, ErrHolderStoreCorrupt) {
			t.Fatalf("register err=%v", err)
		}
		assertHolderMutationRejectedWithoutCleanup(t, client, store, first, second, member, revision)
	})

	t.Run("missing-record", func(t *testing.T) {
		server, client, store, registry, now := newHolderRedisFixture(t, 4)
		base := *now
		first := redisHolderRecord(base, "holder-a", "boot-a")
		if _, err := registry.Register(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		firstKey, member, err := store.recordAndMember(first.Domain, first.HolderID)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Del(context.Background(), firstKey.String()).Err(); err != nil {
			t.Fatal(err)
		}
		second := redisHolderRecord(base, "holder-b", "boot-b")
		if _, err := store.RegisterHolder(context.Background(), second); !errors.Is(err, ErrHolderStoreCorrupt) {
			t.Fatalf("fresh missing record err=%v", err)
		}
		if score, err := client.ZScore(context.Background(), store.Keys().Index().String(), member).Result(); err != nil ||
			score != float64(first.LeaseUntil.UnixMilli()) {
			t.Fatalf("fresh missing index was changed score=%v err=%v", score, err)
		}

		*now = base.Add(11 * time.Second)
		server.SetTime(*now)
		second = redisHolderRecord(*now, "holder-b", "boot-b")
		if _, err := store.RegisterHolder(context.Background(), second); err != nil {
			t.Fatalf("expired missing record cleanup err=%v", err)
		}
		if _, err := client.ZScore(context.Background(), store.Keys().Index().String(), member).Result(); !errors.Is(err, redis.Nil) {
			t.Fatalf("stale orphan member remains err=%v", err)
		}
	})
}

func TestHolderStoreReconcileRejectsAnyExpiredRecordIntegrityDriftWithoutWrites(t *testing.T) {
	tests := []struct {
		name  string
		field string
		value string
	}{
		{name: "wrong-40-hex-digest", field: "record_digest", value: strings.Repeat("f", 40)},
		{name: "fake-record-bytes", field: "record_bytes", value: "1"},
		{name: "resource-whitespace", field: "resource_summary", value: " " + encodeHolderResourceSummary(map[string]string{"cpu": "2"})},
		{name: "resource-duplicate-key", field: "resource_summary", value: "RS1:2:3:cpu1:23:cpu1:2"},
		{name: "resource-order-drift", field: "resource_summary", value: "RS1:2:3:mem1:23:cpu1:2"},
		{name: "resource-altered-value", field: "resource_summary", value: encodeHolderResourceSummary(map[string]string{"cpu": "3"})},
		{name: "zero-sequence", field: "sequence", value: "0"},
		{name: "noncanonical-sequence", field: "sequence", value: "0001"},
		{
			name: "capacity-out-of-range", field: "capacity_weight",
			value: fmt.Sprintf("%d", uint64(core.DefaultHolderMaxCapacityWeight)+1),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, client, store, registry, now := newHolderRedisFixture(t, 4)
			base := *now
			existing := redisHolderRecord(base, "holder-existing", "boot-existing")
			if _, err := registry.Register(context.Background(), existing); err != nil {
				t.Fatal(err)
			}
			existingKey, member, err := store.recordAndMember(existing.Domain, existing.HolderID)
			if err != nil {
				t.Fatal(err)
			}
			if fields, err := client.HLen(context.Background(), existingKey.String()).Result(); err != nil || fields != int64(len(holderRecordFields)) {
				t.Fatalf("record schema fields=%d err=%v", fields, err)
			}
			if err := client.HSet(context.Background(), existingKey.String(), test.field, test.value).Err(); err != nil {
				t.Fatal(err)
			}
			corruptValues, err := client.HGetAll(context.Background(), existingKey.String()).Result()
			if err != nil {
				t.Fatal(err)
			}
			integrity, err := holderRecordIntegrity(corruptValues)
			if err != nil {
				t.Fatal(err)
			}
			if err := client.HSet(
				context.Background(), existingKey.String(), "record_integrity", integrity,
			).Err(); err != nil {
				t.Fatal(err)
			}
			beforeRecord, err := client.HGetAll(context.Background(), existingKey.String()).Result()
			if err != nil {
				t.Fatal(err)
			}
			beforeIndex, err := client.ZRangeWithScores(
				context.Background(), store.Keys().Index().String(), 0, -1,
			).Result()
			if err != nil {
				t.Fatal(err)
			}
			beforeAccounting, err := client.HGetAll(context.Background(), store.Keys().Accounting().String()).Result()
			if err != nil {
				t.Fatal(err)
			}
			beforeMeta, err := client.HGetAll(context.Background(), store.Keys().Meta().String()).Result()
			if err != nil {
				t.Fatal(err)
			}
			revision := beforeMeta["registry_revision"]
			*now = base.Add(11 * time.Second)
			server.SetTime(*now)
			candidate := redisHolderRecord(*now, "holder-candidate", "boot-candidate")
			if _, err := store.RegisterHolder(context.Background(), candidate); !errors.Is(err, ErrHolderStoreCorrupt) {
				t.Fatalf("corrupt expired record mutation err=%v", err)
			}
			afterRecord, err := client.HGetAll(context.Background(), existingKey.String()).Result()
			if err != nil || !reflect.DeepEqual(afterRecord, beforeRecord) {
				t.Fatalf("existing record changed before=%v after=%v err=%v", beforeRecord, afterRecord, err)
			}
			afterIndex, err := client.ZRangeWithScores(
				context.Background(), store.Keys().Index().String(), 0, -1,
			).Result()
			if err != nil || !reflect.DeepEqual(afterIndex, beforeIndex) {
				t.Fatalf("index changed before=%v after=%v err=%v", beforeIndex, afterIndex, err)
			}
			afterAccounting, err := client.HGetAll(context.Background(), store.Keys().Accounting().String()).Result()
			if err != nil || !reflect.DeepEqual(afterAccounting, beforeAccounting) {
				t.Fatalf("accounting changed before=%v after=%v err=%v", beforeAccounting, afterAccounting, err)
			}
			afterMeta, err := client.HGetAll(context.Background(), store.Keys().Meta().String()).Result()
			if err != nil || !reflect.DeepEqual(afterMeta, beforeMeta) || afterMeta["registry_revision"] != revision {
				t.Fatalf("meta changed before=%v after=%v err=%v", beforeMeta, afterMeta, err)
			}
			if _, err := client.ZScore(context.Background(), store.Keys().Index().String(), member).Result(); err != nil {
				t.Fatalf("existing member removed err=%v", err)
			}
			candidateKey, err := store.Keys().Record(candidate.Domain, candidate.HolderID)
			if err != nil {
				t.Fatal(err)
			}
			if exists, err := client.Exists(context.Background(), candidateKey.String()).Result(); err != nil || exists != 0 {
				t.Fatalf("candidate partially written exists=%d err=%v", exists, err)
			}
		})
	}
}

func TestHolderStoreLoadRejectsNoncanonicalResourceRawEvenWithRecomputedIntegrity(t *testing.T) {
	tests := map[string]string{
		"whitespace":    " " + encodeHolderResourceSummary(map[string]string{"cpu": "2"}),
		"duplicate-key": "RS1:2:3:cpu1:23:cpu1:2",
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			_, client, store, _, now := newHolderRedisFixture(t, 2)
			record := redisHolderRecord(*now, "holder-a", "boot-a")
			if _, err := store.RegisterHolder(context.Background(), record); err != nil {
				t.Fatal(err)
			}
			key, err := store.Keys().Record(record.Domain, record.HolderID)
			if err != nil {
				t.Fatal(err)
			}
			values, err := client.HGetAll(context.Background(), key.String()).Result()
			if err != nil {
				t.Fatal(err)
			}
			values["resource_summary"] = raw
			integrity, err := holderRecordIntegrity(values)
			if err != nil {
				t.Fatal(err)
			}
			if err := client.HSet(
				context.Background(), key.String(),
				"resource_summary", raw, "record_integrity", integrity,
			).Err(); err != nil {
				t.Fatal(err)
			}
			if _, _, err := store.GetHolder(
				context.Background(), record.Domain, record.HolderID,
			); !errors.Is(err, ErrHolderStoreCorrupt) {
				t.Fatalf("noncanonical raw accepted err=%v", err)
			}
		})
	}
}

func TestHolderStoreRejectsNonASCIIResourceWithEveryDerivedFieldRecomputedAndLuaWritesNothing(t *testing.T) {
	server, client, store, _, now := newHolderRedisFixture(t, 4)
	base := *now
	record := redisHolderRecord(base, "holder-invalid-resource", "boot-invalid-resource")
	if _, err := store.RegisterHolder(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	key, member, err := store.recordAndMember(record.Domain, record.HolderID)
	if err != nil {
		t.Fatal(err)
	}
	values, err := client.HGetAll(context.Background(), key.String()).Result()
	if err != nil {
		t.Fatal(err)
	}
	raw := "RS1:1:3:cpu1:" + string([]byte{0xff})
	values["resource_summary"] = raw
	digest, err := holderSemanticDigest(values)
	if err != nil {
		t.Fatal(err)
	}
	values["record_digest"] = digest
	recordBytes, err := holderRecordWireBytes(values)
	if err != nil {
		t.Fatal(err)
	}
	values["record_bytes"] = strconv.Itoa(recordBytes)
	integrity, err := holderRecordIntegrity(values)
	if err != nil {
		t.Fatal(err)
	}
	values["record_integrity"] = integrity
	if err := client.HSet(context.Background(), key.String(), values).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(
		context.Background(), store.Keys().Accounting().String(), member,
		holderAccountingValue(recordBytes, len(raw)),
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(
		context.Background(), store.Keys().Meta().String(),
		"record_bytes", strconv.Itoa(recordBytes), "resource_bytes", strconv.Itoa(len(raw)),
	).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.GetHolder(context.Background(), record.Domain, record.HolderID); !errors.Is(err, ErrHolderStoreCorrupt) {
		t.Fatalf("Get accepted non-ASCII resource err=%v", err)
	}

	beforeRecord, _ := client.HGetAll(context.Background(), key.String()).Result()
	beforeIndex, _ := client.ZRangeWithScores(context.Background(), store.Keys().Index().String(), 0, -1).Result()
	beforeAccounting, _ := client.HGetAll(context.Background(), store.Keys().Accounting().String()).Result()
	beforeMeta, _ := client.HGetAll(context.Background(), store.Keys().Meta().String()).Result()
	*now = base.Add(11 * time.Second)
	server.SetTime(*now)
	candidate := redisHolderRecord(*now, "holder-candidate", "boot-candidate")
	if _, err := store.RegisterHolder(context.Background(), candidate); !errors.Is(err, ErrHolderStoreCorrupt) {
		t.Fatalf("Lua accepted non-ASCII resource err=%v", err)
	}
	afterRecord, _ := client.HGetAll(context.Background(), key.String()).Result()
	afterIndex, _ := client.ZRangeWithScores(context.Background(), store.Keys().Index().String(), 0, -1).Result()
	afterAccounting, _ := client.HGetAll(context.Background(), store.Keys().Accounting().String()).Result()
	afterMeta, _ := client.HGetAll(context.Background(), store.Keys().Meta().String()).Result()
	if !reflect.DeepEqual(afterRecord, beforeRecord) || !reflect.DeepEqual(afterIndex, beforeIndex) ||
		!reflect.DeepEqual(afterAccounting, beforeAccounting) || !reflect.DeepEqual(afterMeta, beforeMeta) {
		t.Fatalf("non-ASCII MALFORMED wrote state")
	}
}

func TestHolderStoreRejectsCanonicalNumericAndPersistedStateDriftWithAllDerivedFieldsRecomputed(t *testing.T) {
	tests := []struct {
		name    string
		changes map[string]string
	}{
		{name: "sequence-leading-zero", changes: map[string]string{"sequence": "0001"}},
		{name: "persisted-registering", changes: map[string]string{"state": "REGISTERING"}},
		{name: "ready-not-process-ready", changes: map[string]string{"state": "READY_ZERO_SHARD"}},
		{name: "serving-not-process-ready", changes: map[string]string{
			"state": "SERVING", "serving_shard_count": "1",
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, client, store, _, now := newHolderRedisFixture(t, 4)
			record := redisHolderRecord(*now, "holder-state", "boot-state")
			if _, err := store.RegisterHolder(context.Background(), record); err != nil {
				t.Fatal(err)
			}
			key, member, err := store.recordAndMember(record.Domain, record.HolderID)
			if err != nil {
				t.Fatal(err)
			}
			rewriteHolderDerivedFields(t, client, key, test.changes)
			beforeRecord, _ := client.HGetAll(context.Background(), key.String()).Result()
			beforeIndex, _ := client.ZRangeWithScores(context.Background(), store.Keys().Index().String(), 0, -1).Result()
			beforeAccounting, _ := client.HGetAll(context.Background(), store.Keys().Accounting().String()).Result()
			beforeMeta, _ := client.HGetAll(context.Background(), store.Keys().Meta().String()).Result()

			if _, _, err := store.GetHolder(context.Background(), record.Domain, record.HolderID); !errors.Is(err, ErrHolderStoreCorrupt) {
				t.Fatalf("Get accepted drift err=%v", err)
			}
			if _, err := store.ListHolders(context.Background(), record.Domain); !errors.Is(err, ErrHolderStoreCorrupt) {
				t.Fatalf("List accepted drift err=%v", err)
			}
			*now = now.Add(11 * time.Second)
			server.SetTime(*now)
			candidate := redisHolderRecord(*now, "holder-candidate", "boot-candidate")
			if _, err := store.RegisterHolder(context.Background(), candidate); !errors.Is(err, ErrHolderStoreCorrupt) {
				t.Fatalf("Lua reconcile accepted drift err=%v", err)
			}
			afterRecord, _ := client.HGetAll(context.Background(), key.String()).Result()
			afterIndex, _ := client.ZRangeWithScores(context.Background(), store.Keys().Index().String(), 0, -1).Result()
			afterAccounting, _ := client.HGetAll(context.Background(), store.Keys().Accounting().String()).Result()
			afterMeta, _ := client.HGetAll(context.Background(), store.Keys().Meta().String()).Result()
			if !reflect.DeepEqual(afterRecord, beforeRecord) || !reflect.DeepEqual(afterIndex, beforeIndex) ||
				!reflect.DeepEqual(afterAccounting, beforeAccounting) || !reflect.DeepEqual(afterMeta, beforeMeta) {
				t.Fatalf("malformed mutation wrote state record=%v index=%v accounting=%v meta=%v member=%q",
					afterRecord, afterIndex, afterAccounting, afterMeta, member)
			}
		})
	}
}

func TestHolderRegisterLuaRejectsNoncanonicalCandidateAndWrongSchemaWithoutWrites(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]any)
	}{
		{name: "snapshot-leading-zero", mutate: func(args []any) { args[27] = "01" }},
		{name: "ordinal-over-int32", mutate: func(args []any) {
			args[17], args[18], args[19] = "pod-uid", "workload-uid", "2147483648"
			rewriteHolderRegisterDerivedArgs(t, args)
		}},
		{name: "pod-malformed-utf8", mutate: func(args []any) {
			args[17], args[18], args[19] = string([]byte{0xff}), "workload-uid", "0"
			rewriteHolderRegisterDerivedArgs(t, args)
		}},
		{name: "workload-malformed-utf8", mutate: func(args []any) {
			args[17], args[18], args[19] = "pod-uid", string([]byte{0xff}), "0"
			rewriteHolderRegisterDerivedArgs(t, args)
		}},
		{name: "pod-unicode-whitespace", mutate: func(args []any) {
			args[17], args[18], args[19] = "pod\u00a0uid", "workload-uid", "0"
			rewriteHolderRegisterDerivedArgs(t, args)
		}},
		{name: "workload-unicode-letter", mutate: func(args []any) {
			args[17], args[18], args[19] = "pod-uid", "wörkload-uid", "0"
			rewriteHolderRegisterDerivedArgs(t, args)
		}},
		{name: "pod-internal-ascii-space", mutate: func(args []any) {
			args[17], args[18], args[19] = "pod uid", "workload-uid", "0"
			rewriteHolderRegisterDerivedArgs(t, args)
		}},
		{name: "wrong-schema", mutate: func(args []any) { args[10] = "holder-record-v999" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, client, store, _, now := newHolderRedisFixture(t, 4)
			record := redisHolderRecord(*now, "holder-candidate", "boot-candidate")
			args := holderRegisterArgs(t, store, record)
			test.mutate(args)
			recordKey, _, err := store.recordAndMember(record.Domain, record.HolderID)
			if err != nil {
				t.Fatal(err)
			}
			reply, err := store.scripts.run(context.Background(), ScriptRegisterHolder,
				[]string{store.Keys().Meta().String(), store.Keys().Index().String(),
					store.Keys().Accounting().String(), recordKey.String()}, args...)
			if err != nil {
				t.Fatal(err)
			}
			if len(reply) != 1 || reply[0] != "MALFORMED" {
				t.Fatalf("reply=%v", reply)
			}
			for _, key := range []string{store.Keys().Meta().String(), store.Keys().Index().String(),
				store.Keys().Accounting().String(), recordKey.String()} {
				if exists, err := client.Exists(context.Background(), key).Result(); err != nil || exists != 0 {
					t.Fatalf("candidate validation wrote key=%q exists=%d err=%v", key, exists, err)
				}
			}
		})
	}
}

func TestHolderRegisterLuaEndpointGrammarMatchesGoAndWritesNothing(t *testing.T) {
	invalid := []string{
		"not-a-socket", "Holder.internal:443", " holder.internal:443", "holder.internal:0",
		"holder.internal:01", "holder.internal:65536", "holder..internal:443", "999.0.0.1:443",
		"127.0.0.01:443", "[0:0:0:0:0:0:0:1]:443", "[2001:DB8::1]:443",
		"[fe80::1%eth0]:443", "[::ffff:192.0.2.1]:443", "[::ffff:0:1]:443",
		"[::ffff:c000:201]:443", "[::ffff:0:0]:443", "[::ffff:ffff:ffff]:443",
		"[0:0:0:0:0:ffff:c000:201]:443", "h\u00f6lder:443",
	}
	for _, argument := range []int{20, 21} {
		for _, endpoint := range invalid {
			t.Run(fmt.Sprintf("arg_%d_%s", argument, endpoint), func(t *testing.T) {
				if err := netendpoint.Validate(endpoint, core.DefaultHolderMaxEndpointBytes); err == nil {
					t.Fatalf("Go accepted endpoint expected to be rejected by direct Lua: %q", endpoint)
				}
				_, client, store, _, now := newHolderRedisFixture(t, 4)
				record := redisHolderRecord(*now, "holder-candidate", "boot-candidate")
				args := holderRegisterArgs(t, store, record)
				args[argument] = endpoint
				rewriteHolderRegisterDerivedArgs(t, args)
				recordKey, _, err := store.recordAndMember(record.Domain, record.HolderID)
				if err != nil {
					t.Fatal(err)
				}
				reply, err := store.scripts.run(context.Background(), ScriptRegisterHolder,
					[]string{store.Keys().Meta().String(), store.Keys().Index().String(),
						store.Keys().Accounting().String(), recordKey.String()}, args...)
				if err != nil || len(reply) != 1 || reply[0] != "MALFORMED" {
					t.Fatalf("endpoint=%q reply=%v err=%v", endpoint, reply, err)
				}
				for _, key := range []string{store.Keys().Meta().String(), store.Keys().Index().String(),
					store.Keys().Accounting().String(), recordKey.String()} {
					if exists, err := client.Exists(context.Background(), key).Result(); err != nil || exists != 0 {
						t.Fatalf("invalid endpoint wrote key=%q exists=%d err=%v", key, exists, err)
					}
				}
			})
		}
	}
}

func TestHolderRegisterLuaRejectsUnsafeTTLRelationBeforeAnyWrite(t *testing.T) {
	t.Run("register", func(t *testing.T) {
		_, client, store, _, now := newHolderRedisFixture(t, 4)
		record := redisHolderRecord(*now, "holder-ttl", "boot-ttl")
		args := holderRegisterArgs(t, store, record)
		args[4] = store.opts.MaxLeaseTTL.Milliseconds() + store.opts.ClockSkew.Milliseconds()
		recordKey, _, err := store.recordAndMember(record.Domain, record.HolderID)
		if err != nil {
			t.Fatal(err)
		}
		reply, err := store.scripts.run(context.Background(), ScriptRegisterHolder,
			[]string{store.Keys().Meta().String(), store.Keys().Index().String(),
				store.Keys().Accounting().String(), recordKey.String()}, args...)
		if err != nil {
			t.Fatal(err)
		}
		if len(reply) != 1 || reply[0] != "BAD_NUMBER" {
			t.Fatalf("reply=%v", reply)
		}
		for _, key := range []string{store.Keys().Meta().String(), store.Keys().Index().String(),
			store.Keys().Accounting().String(), recordKey.String()} {
			if exists, err := client.Exists(context.Background(), key).Result(); err != nil || exists != 0 {
				t.Fatalf("unsafe TTL wrote key=%q exists=%d err=%v", key, exists, err)
			}
		}
	})

	t.Run("heartbeat", func(t *testing.T) {
		_, client, store, _, now := newHolderRedisFixture(t, 4)
		current := redisHolderRecord(*now, "holder-ttl", "boot-ttl")
		if _, err := store.RegisterHolder(context.Background(), current); err != nil {
			t.Fatal(err)
		}
		heartbeat := core.HolderHeartbeat{
			Domain: current.Domain, HolderID: current.HolderID, BootID: current.BootID,
			State: core.HolderStateWarming, HeartbeatAt: now.Add(time.Second),
			LeaseUntil: now.Add(11 * time.Second), Sequence: 2,
			CapacityWeight: current.CapacityWeight, ResourceSummary: current.ResourceSummary,
		}
		args := holderHeartbeatArgs(t, store, current, heartbeat)
		args[4] = store.opts.MaxLeaseTTL.Milliseconds() + store.opts.ClockSkew.Milliseconds()
		recordKey, _, err := store.recordAndMember(current.Domain, current.HolderID)
		if err != nil {
			t.Fatal(err)
		}
		beforeRecord, _ := client.HGetAll(context.Background(), recordKey.String()).Result()
		beforeIndex, _ := client.ZRangeWithScores(context.Background(), store.Keys().Index().String(), 0, -1).Result()
		beforeAccounting, _ := client.HGetAll(context.Background(), store.Keys().Accounting().String()).Result()
		beforeMeta, _ := client.HGetAll(context.Background(), store.Keys().Meta().String()).Result()
		reply, err := store.scripts.run(context.Background(), ScriptHeartbeatHolder,
			[]string{store.Keys().Meta().String(), store.Keys().Index().String(),
				store.Keys().Accounting().String(), recordKey.String()}, args...)
		if err != nil {
			t.Fatal(err)
		}
		if len(reply) != 1 || reply[0] != "BAD_NUMBER" {
			t.Fatalf("reply=%v", reply)
		}
		afterRecord, _ := client.HGetAll(context.Background(), recordKey.String()).Result()
		afterIndex, _ := client.ZRangeWithScores(context.Background(), store.Keys().Index().String(), 0, -1).Result()
		afterAccounting, _ := client.HGetAll(context.Background(), store.Keys().Accounting().String()).Result()
		afterMeta, _ := client.HGetAll(context.Background(), store.Keys().Meta().String()).Result()
		if !reflect.DeepEqual(afterRecord, beforeRecord) || !reflect.DeepEqual(afterIndex, beforeIndex) ||
			!reflect.DeepEqual(afterAccounting, beforeAccounting) || !reflect.DeepEqual(afterMeta, beforeMeta) {
			t.Fatal("unsafe heartbeat TTL wrote state")
		}
	})
}

func TestHolderStoreAlreadyRequiresExactMetaCountersAndWritesNothingOnDrift(t *testing.T) {
	_, client, store, _, now := newHolderRedisFixture(t, 4)
	record := redisHolderRecord(*now, "holder-meta", "boot-meta")
	if _, err := store.RegisterHolder(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	key, _, err := store.recordAndMember(record.Domain, record.HolderID)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(context.Background(), store.Keys().Meta().String(), "record_bytes", "1").Err(); err != nil {
		t.Fatal(err)
	}
	beforeRecord, _ := client.HGetAll(context.Background(), key.String()).Result()
	beforeIndex, _ := client.ZRangeWithScores(context.Background(), store.Keys().Index().String(), 0, -1).Result()
	beforeAccounting, _ := client.HGetAll(context.Background(), store.Keys().Accounting().String()).Result()
	beforeMeta, _ := client.HGetAll(context.Background(), store.Keys().Meta().String()).Result()
	if _, err := store.RegisterHolder(context.Background(), record); !errors.Is(err, ErrHolderStoreCorrupt) {
		t.Fatalf("ALREADY accepted forged meta err=%v", err)
	}
	afterRecord, _ := client.HGetAll(context.Background(), key.String()).Result()
	afterIndex, _ := client.ZRangeWithScores(context.Background(), store.Keys().Index().String(), 0, -1).Result()
	afterAccounting, _ := client.HGetAll(context.Background(), store.Keys().Accounting().String()).Result()
	afterMeta, _ := client.HGetAll(context.Background(), store.Keys().Meta().String()).Result()
	if !reflect.DeepEqual(afterRecord, beforeRecord) || !reflect.DeepEqual(afterIndex, beforeIndex) ||
		!reflect.DeepEqual(afterAccounting, beforeAccounting) || !reflect.DeepEqual(afterMeta, beforeMeta) {
		t.Fatalf("ALREADY corruption performed writes")
	}
}

func TestHolderStoreFullExpiredCleanupRequiresExactPrecleanCounters(t *testing.T) {
	server, client, store, _, now := newHolderRedisFixture(t, 4)
	base := *now
	expired := redisHolderRecord(base, "holder-expired", "boot-expired")
	if _, err := store.RegisterHolder(context.Background(), expired); err != nil {
		t.Fatal(err)
	}
	expiredKey, member, err := store.recordAndMember(expired.Domain, expired.HolderID)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(context.Background(), store.Keys().Meta().String(), "resource_bytes", "1").Err(); err != nil {
		t.Fatal(err)
	}
	beforeRecord, _ := client.HGetAll(context.Background(), expiredKey.String()).Result()
	beforeIndex, _ := client.ZRangeWithScores(context.Background(), store.Keys().Index().String(), 0, -1).Result()
	beforeAccounting, _ := client.HGetAll(context.Background(), store.Keys().Accounting().String()).Result()
	beforeMeta, _ := client.HGetAll(context.Background(), store.Keys().Meta().String()).Result()
	*now = base.Add(11 * time.Second)
	server.SetTime(*now)
	candidate := redisHolderRecord(*now, "holder-next", "boot-next")
	if _, err := store.RegisterHolder(context.Background(), candidate); !errors.Is(err, ErrHolderStoreCorrupt) {
		t.Fatalf("full stale cleanup accepted forged pre-counters err=%v", err)
	}
	afterRecord, _ := client.HGetAll(context.Background(), expiredKey.String()).Result()
	afterIndex, _ := client.ZRangeWithScores(context.Background(), store.Keys().Index().String(), 0, -1).Result()
	afterAccounting, _ := client.HGetAll(context.Background(), store.Keys().Accounting().String()).Result()
	afterMeta, _ := client.HGetAll(context.Background(), store.Keys().Meta().String()).Result()
	if !reflect.DeepEqual(afterRecord, beforeRecord) || !reflect.DeepEqual(afterIndex, beforeIndex) ||
		!reflect.DeepEqual(afterAccounting, beforeAccounting) || !reflect.DeepEqual(afterMeta, beforeMeta) {
		t.Fatalf("malformed full stale cleanup wrote state member=%q", member)
	}
}

func TestHolderStoreListRejectsIndexCardinalityBeyondBoundBeforeRange(t *testing.T) {
	_, client, store, _, now := newHolderRedisFixture(t, 1)
	record := redisHolderRecord(*now, "holder-list", "boot-list")
	if _, err := store.RegisterHolder(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	extraMember, err := store.Keys().Member(record.Domain, "holder-extra")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(context.Background(), store.Keys().Index().String(), redis.Z{
		Score: float64(now.Add(time.Second).UnixMilli()), Member: extraMember,
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ListHolders(context.Background(), record.Domain); !errors.Is(err, ErrHolderStoreCorrupt) {
		t.Fatalf("truncated over-limit index err=%v", err)
	}
}

func TestHolderStoreHeartbeatDoesNotRequireStableGlobalSnapshot(t *testing.T) {
	_, _, store, _, now := newHolderRedisFixture(t, 2)
	record := redisHolderRecord(*now, "holder-a", "boot-a")
	if _, err := store.RegisterHolder(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	readAttempts := 0
	store.readAttemptHook = func(int, holderReadStage) {
		readAttempts++
	}
	updated, err := store.HeartbeatHolder(
		context.Background(),
		core.HolderHeartbeat{
			Domain: record.Domain, HolderID: record.HolderID,
			BootID: record.BootID, State: core.HolderStateWarming,
			HeartbeatAt: now.Add(time.Second),
			LeaseUntil:  now.Add(11 * time.Second), Sequence: 2,
			CapacityWeight:  record.CapacityWeight,
			ResourceSummary: record.ResourceSummary,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if readAttempts != 0 {
		t.Fatalf("heartbeat used full registry read attempts=%d", readAttempts)
	}
	if updated.Sequence != 2 {
		t.Fatalf("heartbeat sequence=%d", updated.Sequence)
	}
}

func TestHolderRegistryHeartbeatUsesPointPreflight(t *testing.T) {
	_, _, store, registry, now := newHolderRedisFixture(t, 2)
	record := redisHolderRecord(*now, "holder-a", "boot-a")
	if _, err := store.RegisterHolder(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	readAttempts := 0
	store.readAttemptHook = func(int, holderReadStage) {
		readAttempts++
	}
	updated, err := registry.Heartbeat(context.Background(), core.HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: core.HolderStateWarming, HeartbeatAt: now.Add(time.Second),
		LeaseUntil: now.Add(11 * time.Second), Sequence: 2,
		CapacityWeight: record.CapacityWeight, ResourceSummary: record.ResourceSummary,
	})
	if err != nil {
		t.Fatal(err)
	}
	if readAttempts != 0 {
		t.Fatalf("registry heartbeat used full registry read attempts=%d", readAttempts)
	}
	if updated.Sequence != 2 {
		t.Fatalf("heartbeat sequence=%d", updated.Sequence)
	}
}

func TestHolderStoreMutationReplyUsesPreflightNowOnce(t *testing.T) {
	t.Run("register", func(t *testing.T) {
		_, client, store, _, now := newHolderRedisFixture(t, 2)
		base := *now
		calls := 0
		store.opts.Now = func() time.Time {
			calls++
			if calls == 1 {
				return base
			}
			return base.Add(-time.Hour)
		}
		record, err := store.RegisterHolder(
			context.Background(), redisHolderRecord(base, "holder-a", "boot-a"),
		)
		if err != nil || calls != 1 || record.Sequence != 1 {
			t.Fatalf("register record=%+v calls=%d err=%v", record, calls, err)
		}
		if got := mustHolderRevision(t, client, store); got != "1" {
			t.Fatalf("register revision=%s", got)
		}
	})

	t.Run("heartbeat", func(t *testing.T) {
		server, client, store, _, now := newHolderRedisFixture(t, 2)
		base := *now
		record := redisHolderRecord(base, "holder-a", "boot-a")
		if _, err := store.RegisterHolder(context.Background(), record); err != nil {
			t.Fatal(err)
		}
		preflight := base.Add(time.Second)
		*now = preflight
		server.SetTime(preflight)
		calls := 0
		store.opts.Now = func() time.Time {
			calls++
			if calls == 1 {
				return preflight
			}
			return base.Add(-time.Hour)
		}
		updated, err := store.HeartbeatHolder(context.Background(), core.HolderHeartbeat{
			Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
			State: core.HolderStateWarming, HeartbeatAt: preflight,
			LeaseUntil: preflight.Add(10 * time.Second), Sequence: 2,
			CapacityWeight: record.CapacityWeight, ResourceSummary: record.ResourceSummary,
		})
		if err != nil || calls != 1 || updated.Sequence != 2 {
			t.Fatalf("heartbeat record=%+v calls=%d err=%v", updated, calls, err)
		}
		if got := mustHolderRevision(t, client, store); got != "2" {
			t.Fatalf("heartbeat revision=%s", got)
		}
	})
}

func TestHolderStoreMetaDisappearanceAfterRevisionRetriesToEmpty(t *testing.T) {
	_, client, store, _, now := newHolderRedisFixture(t, 2)
	record := redisHolderRecord(*now, "holder-a", "boot-a")
	if _, err := store.RegisterHolder(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	var hookErr error
	called := false
	store.metaReadHook = func(stage holderMetaReadStage) {
		if called || stage != holderMetaReadAfterRevision {
			return
		}
		called = true
		hookErr = client.Del(
			context.Background(), store.Keys().Meta().String(), store.Keys().Index().String(),
			store.Keys().Accounting().String(),
		).Err()
	}
	records, err := store.ListHolders(context.Background(), record.Domain)
	if hookErr != nil || err != nil || !called || len(records) != 0 {
		t.Fatalf("meta expiry list=%+v called=%t hookErr=%v err=%v", records, called, hookErr, err)
	}
	store.metaReadHook = nil
	if _, found, err := store.GetHolder(context.Background(), record.Domain, record.HolderID); !errors.Is(err, ErrHolderStoreCorrupt) || found {
		t.Fatalf("residual target Get found=%t err=%v", found, err)
	}
	if _, err := store.RegisterHolder(context.Background(), record); !errors.Is(err, ErrHolderStoreCorrupt) {
		t.Fatalf("residual target Register err=%v", err)
	}
	recordKey, err := store.Keys().Record(record.Domain, record.HolderID)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Del(context.Background(), recordKey.String()).Err(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.GetHolder(context.Background(), record.Domain, record.HolderID); err != nil || found {
		t.Fatalf("fully absent Get found=%t err=%v", found, err)
	}
}

func TestHolderStoreGetUsesCompleteBoundedAccountingFence(t *testing.T) {
	_, client, store, _, now := newHolderRedisFixture(t, 4)
	target := redisHolderRecord(*now, "holder-target", "boot-target")
	unrelated := redisHolderRecord(*now, "holder-unrelated", "boot-unrelated")
	if _, err := store.RegisterHolder(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RegisterHolder(context.Background(), unrelated); err != nil {
		t.Fatal(err)
	}
	unrelatedMember, err := store.Keys().Member(unrelated.Domain, unrelated.HolderID)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(
		context.Background(), store.Keys().Accounting().String(), unrelatedMember, "1:1",
	).Err(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.GetHolder(context.Background(), target.Domain, target.HolderID); !errors.Is(err, ErrHolderStoreCorrupt) || found {
		t.Fatalf("Get ignored unrelated accounting drift found=%t err=%v", found, err)
	}
}

func TestHolderStoreNaturalTTLExpiresRecordBeforeRegistryKeys(t *testing.T) {
	server, client, store, _, now := newHolderRedisFixture(t, 4)
	record := redisHolderRecord(*now, "holder-natural-ttl", "boot-natural-ttl")
	if _, err := store.RegisterHolder(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	recordKey, err := store.Keys().Record(record.Domain, record.HolderID)
	if err != nil {
		t.Fatal(err)
	}
	recordTTL, err := client.PTTL(context.Background(), recordKey.String()).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, registryKey := range []rediskey.RedisKey{
		store.Keys().Meta(), store.Keys().Index(), store.Keys().Accounting(),
	} {
		registryTTL, err := client.PTTL(context.Background(), registryKey.String()).Result()
		if err != nil || registryTTL <= recordTTL {
			t.Fatalf("registry TTL key=%s ttl=%v record=%v err=%v", registryKey, registryTTL, recordTTL, err)
		}
	}
	server.FastForward(store.opts.RecordTTL)
	*now = now.Add(store.opts.RecordTTL)
	server.SetTime(*now)
	if exists, err := client.Exists(context.Background(), recordKey.String()).Result(); err != nil || exists != 0 {
		t.Fatalf("record did not expire first exists=%d err=%v", exists, err)
	}
	if exists, err := client.Exists(context.Background(), store.Keys().Meta().String()).Result(); err != nil || exists != 1 {
		t.Fatalf("meta did not outlive record exists=%d err=%v", exists, err)
	}
	if _, found, err := store.GetHolder(context.Background(), record.Domain, record.HolderID); err != nil || found {
		t.Fatalf("natural record expiry Get found=%t err=%v", found, err)
	}
	server.FastForward(time.Duration(holderRegistryTTLMarginMillis) * time.Millisecond)
	if _, found, err := store.GetHolder(context.Background(), record.Domain, record.HolderID); err != nil || found {
		t.Fatalf("natural complete expiry Get found=%t err=%v", found, err)
	}
}

func TestHolderStoreRegistryRevisionStringIncrementReplayAndCleanup(t *testing.T) {
	_, client, store, _, now := newHolderRedisFixture(t, 8)
	record := redisHolderRecord(*now, "holder-a", "boot-a")
	if _, err := store.RegisterHolder(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if got := mustHolderRevision(t, client, store); got != "1" {
		t.Fatalf("register revision=%s", got)
	}
	if _, err := store.RegisterHolder(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if got := mustHolderRevision(t, client, store); got != "1" {
		t.Fatalf("ALREADY incremented revision=%s", got)
	}
	heartbeat := core.HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: core.HolderStateWarming, HeartbeatAt: now.Add(time.Second),
		LeaseUntil: now.Add(11 * time.Second), Sequence: 2, CapacityWeight: record.CapacityWeight,
		ResourceSummary: map[string]string{"cpu": "2"},
	}
	if _, err := store.HeartbeatHolder(context.Background(), heartbeat); err != nil {
		t.Fatal(err)
	}
	if got := mustHolderRevision(t, client, store); got != "2" {
		t.Fatalf("heartbeat revision=%s", got)
	}
	if _, err := store.HeartbeatHolder(context.Background(), heartbeat); !errors.Is(err, ErrHolderStoreReplay) {
		t.Fatalf("replay err=%v", err)
	}
	if got := mustHolderRevision(t, client, store); got != "2" {
		t.Fatalf("replay incremented revision=%s", got)
	}

	if err := client.HSet(
		context.Background(), store.Keys().Meta().String(),
		"registry_revision", "9007199254740992",
	).Err(); err != nil {
		t.Fatal(err)
	}
	heartbeat.Sequence = 3
	heartbeat.HeartbeatAt = now.Add(2 * time.Second)
	heartbeat.LeaseUntil = now.Add(12 * time.Second)
	if _, err := store.HeartbeatHolder(context.Background(), heartbeat); err != nil {
		t.Fatal(err)
	}
	if got := mustHolderRevision(t, client, store); got != "9007199254740993" {
		t.Fatalf("string-safe revision=%s", got)
	}

	if err := client.HSet(
		context.Background(), store.Keys().Meta().String(),
		"registry_revision", maxRegistryRevision,
	).Err(); err != nil {
		t.Fatal(err)
	}
	heartbeat.Sequence = 4
	heartbeat.HeartbeatAt = now.Add(3 * time.Second)
	heartbeat.LeaseUntil = now.Add(13 * time.Second)
	if _, err := store.HeartbeatHolder(context.Background(), heartbeat); !errors.Is(err, ErrHolderStoreUnavailable) {
		t.Fatalf("revision exhaustion err=%v", err)
	}
	if got := mustHolderRevision(t, client, store); got != maxRegistryRevision {
		t.Fatalf("revision wrapped=%s", got)
	}
	current, found, err := store.GetHolder(context.Background(), record.Domain, record.HolderID)
	if err != nil || !found || current.Sequence != 3 {
		t.Fatalf("exhausted mutation changed record=%+v found=%t err=%v", current, found, err)
	}

	_, cleanupClient, cleanupStore, _, cleanupNow := newHolderRedisFixture(t, 8)
	cleanupRecord := redisHolderRecord(*cleanupNow, "holder-cleanup", "boot-cleanup")
	if _, err := cleanupStore.RegisterHolder(context.Background(), cleanupRecord); err != nil {
		t.Fatal(err)
	}
	staleMember, err := cleanupStore.Keys().Member(cleanupRecord.Domain, "stale-orphan")
	if err != nil {
		t.Fatal(err)
	}
	if err := cleanupClient.ZAdd(context.Background(), cleanupStore.Keys().Index().String(), redis.Z{
		Score: float64(cleanupNow.Add(-time.Second).UnixMilli()), Member: staleMember,
	}).Err(); err != nil {
		t.Fatal(err)
	}
	// The exact accounting sidecar keeps this synthetic expired missing record
	// provable even though its record hash is absent.
	missingResourceBytes := len(encodeHolderResourceSummary(nil))
	if err := cleanupClient.HSet(
		context.Background(), cleanupStore.Keys().Accounting().String(), staleMember,
		holderAccountingValue(1, missingResourceBytes),
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := cleanupClient.HIncrBy(
		context.Background(), cleanupStore.Keys().Meta().String(), "holder_count", 1,
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := cleanupClient.HIncrBy(
		context.Background(), cleanupStore.Keys().Meta().String(), "record_bytes", 1,
	).Err(); err != nil {
		t.Fatal(err)
	}
	if err := cleanupClient.HIncrBy(
		context.Background(), cleanupStore.Keys().Meta().String(), "resource_bytes",
		int64(missingResourceBytes),
	).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := cleanupStore.RegisterHolder(context.Background(), cleanupRecord); err != nil {
		t.Fatalf("ALREADY stale cleanup err=%v", err)
	}
	if got := mustHolderRevision(t, cleanupClient, cleanupStore); got != "2" {
		t.Fatalf("cleanup revision=%s", got)
	}
	if _, err := cleanupStore.RegisterHolder(context.Background(), cleanupRecord); err != nil {
		t.Fatal(err)
	}
	if got := mustHolderRevision(t, cleanupClient, cleanupStore); got != "2" {
		t.Fatalf("post-cleanup ALREADY incremented revision=%s", got)
	}
}

func TestHolderStoreRevisionFencedReadsRetryAndExhaustUnavailable(t *testing.T) {
	_, _, store, _, now := newHolderRedisFixture(t, holderReadMaxAttempts+8)
	first := redisHolderRecord(*now, "holder-a", "boot-a")
	if _, err := store.RegisterHolder(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	var hookErr error
	store.readAttemptHook = func(attempt int, stage holderReadStage) {
		if attempt != 1 || stage != holderReadAfterMeta {
			return
		}
		_, hookErr = store.RegisterHolder(
			context.Background(), redisHolderRecord(*now, "holder-b", "boot-b"),
		)
	}
	records, err := store.ListHolders(context.Background(), first.Domain)
	if hookErr != nil || err != nil || len(records) != 2 {
		t.Fatalf("revision retry list=%+v hookErr=%v err=%v", records, hookErr, err)
	}

	hookErr = nil
	store.readAttemptHook = func(attempt int, stage holderReadStage) {
		if attempt != 1 || stage != holderReadAfterMeta {
			return
		}
		_, hookErr = store.RegisterHolder(
			context.Background(), redisHolderRecord(*now, "holder-c", "boot-c"),
		)
	}
	got, found, err := store.GetHolder(context.Background(), first.Domain, first.HolderID)
	if hookErr != nil || err != nil || !found || got.HolderID != first.HolderID {
		t.Fatalf("revision retry Get=%+v found=%t hookErr=%v err=%v", got, found, hookErr, err)
	}

	hookErr = nil
	store.readAttemptHook = func(attempt int, stage holderReadStage) {
		if stage != holderReadAfterMeta {
			return
		}
		holderID := fmt.Sprintf("holder-churn-%d", attempt)
		_, hookErr = store.RegisterHolder(
			context.Background(), redisHolderRecord(*now, holderID, "boot-"+holderID),
		)
	}
	_, err = store.ListHolders(context.Background(), first.Domain)
	if hookErr != nil {
		t.Fatalf("hook mutation err=%v", hookErr)
	}
	if !errors.Is(err, ErrHolderStoreUnavailable) || errors.Is(err, ErrHolderStoreCorrupt) {
		t.Fatalf("retry exhaustion err=%v", err)
	}
	store.readAttemptHook = nil
}

func TestHolderStoreRevisionFencedReadSurvivesHeartbeatSizedChurn(t *testing.T) {
	_, _, store, _, now := newHolderRedisFixture(t, 16)
	first := redisHolderRecord(*now, "holder-a", "boot-a")
	if _, err := store.RegisterHolder(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	const churnAttempts = 8
	var hookErr error
	store.readAttemptHook = func(attempt int, stage holderReadStage) {
		if stage != holderReadAfterMeta || attempt > churnAttempts {
			return
		}
		holderID := fmt.Sprintf("holder-churn-%d", attempt)
		_, hookErr = store.RegisterHolder(
			context.Background(), redisHolderRecord(*now, holderID, "boot-"+holderID),
		)
	}
	records, err := store.ListHolders(context.Background(), first.Domain)
	if hookErr != nil || err != nil {
		t.Fatalf("heartbeat-sized churn hookErr=%v err=%v", hookErr, err)
	}
	if got, want := len(records), churnAttempts+1; got != want {
		t.Fatalf("records=%d want=%d", got, want)
	}
}

func TestHolderStoreRejectsNonZeroWarmingAtPreflightAndLua(t *testing.T) {
	_, _, store, _, now := newHolderRedisFixture(t, 4)
	tests := map[string]func(*core.HolderRecord){
		"process-ready": func(record *core.HolderRecord) { record.ProcessReady = true },
		"draining":      func(record *core.HolderRecord) { record.Draining = true },
		"serving":       func(record *core.HolderRecord) { record.ServingShardCount = 1 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			bad := redisHolderRecord(*now, "holder-"+name, "boot-"+name)
			mutate(&bad)
			if _, err := store.RegisterHolder(context.Background(), bad); !errors.Is(err, core.ErrHolderRegistryInvalid) {
				t.Fatalf("store preflight err=%v", err)
			}
			recordBytes, resourceWire, recordDigest, err := encodeHolderRecord(bad)
			if err != nil {
				t.Fatal(err)
			}
			registry := store.registryOptions()
			args := []any{
				store.keys.HashTag(), store.opts.MaxHolders, store.opts.MaxRecordBytes, store.opts.MaxResourceBytes,
				store.opts.RecordTTL.Milliseconds(), store.opts.MinLeaseTTL.Milliseconds(),
				store.opts.MaxLeaseTTL.Milliseconds(), store.opts.ClockSkew.Milliseconds(),
				registry.MaxServingShardCount, registry.MaxCapacityWeight,
				holderRecordSchema, registry.MaxIDBytes, registry.MaxEndpointBytes,
				registry.MaxTLSIdentityBytes, string(bad.Domain), bad.HolderID, bad.BootID,
				bad.DeploymentIdentity.PodUID, bad.DeploymentIdentity.WorkloadUID,
				ordinalWire(bad.DeploymentIdentity), bad.LogicEndpoint, bad.HolderControlEndpoint,
				bad.TLSDNSIdentity, string(bad.State),
				boolArg(bad.ProcessReady), boolArg(bad.Draining), bad.ServingShardCount,
				bad.SnapshotRevision, bad.StartedAt.UnixMilli(), bad.HeartbeatAt.UnixMilli(),
				bad.LeaseUntil.UnixMilli(), bad.Sequence, bad.CapacityWeight, resourceWire,
				recordDigest, recordBytes, store.opts.MaxTotalRecordBytes,
				store.opts.MaxTotalResourceBytes,
			}
			recordKey, _, err := store.recordAndMember(bad.Domain, bad.HolderID)
			if err != nil {
				t.Fatal(err)
			}
			reply, err := store.scripts.run(context.Background(), ScriptRegisterHolder,
				[]string{store.keys.Meta().String(), store.keys.Index().String(),
					store.keys.Accounting().String(), recordKey.String()}, args...)
			if err != nil {
				t.Fatal(err)
			}
			if len(reply) != 1 || reply[0] != "BAD_STATE" {
				t.Fatalf("Lua WARMING reply=%v", reply)
			}
		})
	}
}

func TestHolderStoreRejectsLargeExtraHashFieldsBeforeMaterialization(t *testing.T) {
	_, client, store, _, now := newHolderRedisFixture(t, 4)
	record := redisHolderRecord(*now, "holder-a", "boot-a")
	if _, err := store.RegisterHolder(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	extra := make(map[string]any, 2048)
	for index := 0; index < 2048; index++ {
		extra[fmt.Sprintf("extra_%04d", index)] = strings.Repeat("x", 128)
	}
	if err := client.HSet(context.Background(), store.Keys().Meta().String(), extra).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ListHolders(context.Background(), record.Domain); !errors.Is(err, ErrHolderStoreCorrupt) {
		t.Fatalf("extra meta fields err=%v", err)
	}

	_, client, store, _, now = newHolderRedisFixture(t, 4)
	record = redisHolderRecord(*now, "holder-a", "boot-a")
	if _, err := store.RegisterHolder(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if err := client.HDel(context.Background(), store.Keys().Meta().String(), "resource_bytes").Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(
		context.Background(), store.Keys().Meta().String(),
		"unexpected_same_count", strings.Repeat("x", 1<<20),
	).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ListHolders(context.Background(), record.Domain); !errors.Is(err, ErrHolderStoreCorrupt) {
		t.Fatalf("same-count substituted meta err=%v", err)
	}

	_, client, store, _, now = newHolderRedisFixture(t, 4)
	record = redisHolderRecord(*now, "holder-a", "boot-a")
	if _, err := store.RegisterHolder(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	recordKey, err := store.Keys().Record(record.Domain, record.HolderID)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.HSet(context.Background(), recordKey.String(), extra).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.GetHolder(context.Background(), record.Domain, record.HolderID); !errors.Is(err, ErrHolderStoreCorrupt) {
		t.Fatalf("extra record fields err=%v", err)
	}
}

func mustHolderRevision(t *testing.T, client *redis.Client, store *HolderStore) string {
	t.Helper()
	revision, err := client.HGet(
		context.Background(), store.Keys().Meta().String(), "registry_revision",
	).Result()
	if err != nil {
		t.Fatal(err)
	}
	return revision
}

func assertHolderMutationRejectedWithoutCleanup(
	t *testing.T,
	client *redis.Client,
	store *HolderStore,
	existing core.HolderRecord,
	candidate core.HolderRecord,
	existingMember string,
	revision string,
) {
	t.Helper()
	existingKey, err := store.Keys().Record(existing.Domain, existing.HolderID)
	if err != nil {
		t.Fatal(err)
	}
	candidateKey, err := store.Keys().Record(candidate.Domain, candidate.HolderID)
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := client.Exists(context.Background(), existingKey.String()).Result(); err != nil || exists != 1 {
		t.Fatalf("existing record deleted exists=%d err=%v", exists, err)
	}
	if exists, err := client.Exists(context.Background(), candidateKey.String()).Result(); err != nil || exists != 0 {
		t.Fatalf("candidate partially written exists=%d err=%v", exists, err)
	}
	if _, err := client.ZScore(context.Background(), store.Keys().Index().String(), existingMember).Result(); err != nil {
		t.Fatalf("existing index member removed err=%v", err)
	}
	if got := mustHolderRevision(t, client, store); got != revision {
		t.Fatalf("revision changed got=%s want=%s", got, revision)
	}
}
