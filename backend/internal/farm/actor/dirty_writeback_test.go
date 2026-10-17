package actor

import (
	"context"
	"testing"
	"time"

	farmpb "farmgame/api/gen/farm"
	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/logic/actor/dirtywriteback"
	actorruntime "farmgame/internal/logic/actor/runtime"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/protobuf/proto"
)

func newSoftFarmRegistry(t *testing.T, farmID uint64) (*Registry, authoritystore.ModelFieldBatchReader) {
	registry, reader, _ := newSoftFarmRegistryWithGate(t, farmID, nil)
	return registry, reader
}

func TestSoftFarmRegistrySkipsDetachedParticipantCapacityProjection(t *testing.T) {
	registry, _ := newSoftFarmRegistry(t, 98_001)
	if registry.ParticipantCapacityProjectionRequired() {
		t.Fatal("soft dirty writeback should not require detached participant capacity projection")
	}
}

func newSoftFarmRegistryWithGate(t *testing.T, farmID uint64, gate actorruntime.AdmissionGate) (*Registry, authoritystore.ModelFieldBatchReader, *actorrouting.Calculator) {
	t.Helper()
	calculator := ar1Calculator(t)
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver: calculator, AuthorityLayoutVersion: "v1",
		Now: func() time.Time { return ar1Now },
	})
	if err != nil {
		t.Fatal(err)
	}
	shard, err := calculator.ShardFor(actorrouting.DomainFarm, farmID)
	if err != nil {
		t.Fatal(err)
	}
	writeAuthority := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainFarm, VirtualShard: shard,
		HolderID: "farm-soft-holder", HolderBootID: "farm-soft-boot",
		AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1,
		State: authoritystore.ShardAuthorityActive, LeaseUntilMs: ar1Now.Add(time.Minute).UnixMilli(),
	}
	if err := store.SetShardWriteAuthority(writeAuthority); err != nil {
		t.Fatal(err)
	}
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store: store,
		Fence: authority.StaticFenceSource{Fence: authoritystore.ShardWriteFence{
			ExpectedHolderID: "farm-soft-holder", ExpectedHolderBootID: "farm-soft-boot",
			ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
		}},
		AuthorityLayoutVersion: "v1", BootID: "farm-soft-boot", EnableSoftSave: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewAuthorityRegistry(provider, RegistryOptions{
		MailboxSize:   8,
		AdmissionGate: gate,
		Passivation:   PassivationOptions{Disable: true},
		DirtyWriteback: dirtywriteback.Options{
			FlushInterval: time.Hour, MaxDirtyAge: 2 * time.Hour,
			RetryDelay: time.Second, FlushTimeout: time.Second,
			Workers: 1, QueueSize: 16,
		},
	})
	t.Cleanup(registry.Stop)
	return registry, store, calculator
}

func TestFarmShardDrainFlushesDirtyStateBeforeEviction(t *testing.T) {
	const farmID = uint64(98_002)
	calculator := ar1Calculator(t)
	gate, err := actorruntime.NewShardDrainGate(actorruntime.ShardDrainGateOptions{
		ShardCount: 64,
		ShardFor: func(actorID uint64) (uint32, error) {
			return calculator.ShardFor(actorrouting.DomainFarm, actorID)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	registry, reader, _ := newSoftFarmRegistryWithGate(t, farmID, gate)
	if _, err := ar1PlantAt(context.Background(), registry, farmID, 0, "soft-drain-plant", []byte("soft-drain")); err != nil {
		t.Fatal(err)
	}
	shard, err := calculator.ShardFor(actorrouting.DomainFarm, farmID)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.StopNewActivationAndTurns(context.Background(), shard); err != nil {
		t.Fatal(err)
	}
	if err := registry.WaitCurrentTurns(context.Background(), shard); err != nil {
		t.Fatal(err)
	}
	barrier, err := registry.NewShardFactsBarrier(func(actorID uint64) (uint32, error) {
		return calculator.ShardFor(actorrouting.DomainFarm, actorID)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := barrier.ConfirmFactsCommitted(context.Background(), shard); err != nil {
		t.Fatal(err)
	}
	after, physicalVersion := readPersistedFarm(t, reader, farmID)
	if physicalVersion != 2 || after.GetCore().GetFarmVersion() != 1 || len(after.GetCore().GetCropCycles()) != 1 {
		t.Fatalf("drain did not flush farm: physicalVersion=%d record=%+v", physicalVersion, after.GetCore())
	}
	registry.mu.Lock()
	remaining := len(registry.actors) + len(registry.retiredBindings)
	registry.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("drain retained %d farm generations", remaining)
	}
}

func readPersistedFarm(t *testing.T, reader authoritystore.ModelFieldBatchReader, farmID uint64) (*farmpb.FarmRecord, uint64) {
	t.Helper()
	rows, err := reader.BatchReadModelFields(context.Background(), actorrouting.DomainFarm,
		[]uint64{farmID}, []string{authority.RecordModelField})
	if err != nil || len(rows) != 1 || rows[0].Err != nil || !rows[0].Found {
		t.Fatalf("persisted row=%+v err=%v", rows, err)
	}
	record := &farmpb.FarmRecord{}
	if err := proto.Unmarshal(rows[0].ModelFields[authority.RecordModelField], record); err != nil {
		t.Fatal(err)
	}
	return record, rows[0].RecordVersion
}

func TestFarmDirtyWritebackCoalescesTurnsAndFlushesLatestState(t *testing.T) {
	const farmID = uint64(98001)
	registry, reader := newSoftFarmRegistry(t, farmID)
	ctx := context.Background()

	if _, err := ar1PlantAt(ctx, registry, farmID, 0, "soft-plant-a", []byte("soft-a")); err != nil {
		t.Fatal(err)
	}
	if _, err := ar1PlantAt(ctx, registry, farmID, 1, "soft-plant-b", []byte("soft-b")); err != nil {
		t.Fatal(err)
	}
	before, persistedVersion := readPersistedFarm(t, reader, farmID)
	if persistedVersion != 1 || before.GetCore().GetFarmVersion() != 0 {
		t.Fatalf("Redis changed before flush: record_version=%d farm_version=%d", persistedVersion, before.GetCore().GetFarmVersion())
	}
	stats := registry.DirtyWritebackStats()
	if stats.QueueDepth != 1 || stats.Marks < 2 || stats.CoalescedMarks < 1 {
		t.Fatalf("dirty queue did not coalesce: %+v", stats)
	}

	if err := registry.writeback.FlushNow(ctx, farmID); err != nil {
		t.Fatal(err)
	}
	after, persistedVersion := readPersistedFarm(t, reader, farmID)
	if persistedVersion != 2 || after.GetCore().GetFarmVersion() != 2 || len(after.GetCore().GetCropCycles()) != 2 {
		t.Fatalf("latest state not flushed: record_version=%d record=%+v", persistedVersion, after.GetCore())
	}
	stats = registry.DirtyWritebackStats()
	if stats.QueueDepth != 0 || stats.FlushSucceeded < 1 {
		t.Fatalf("flush metrics=%+v", stats)
	}
}
