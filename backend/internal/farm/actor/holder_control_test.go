package actor

import (
	"context"
	"errors"
	"testing"

	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/authority"
	actorruntime "farmgame/internal/logic/actor/runtime"
	"farmgame/internal/storage/blobstore"
)

func newGatedFarmFactsRegistry(t *testing.T, farmID uint64) (*Registry, *actorruntime.ShardDrainGate, *actorrouting.Calculator, *ar1Rig) {
	t.Helper()
	base := ar1MemoryRig(t, farmID)
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
	registry := NewAuthorityRegistry(base.provider, RegistryOptions{
		MailboxSize:   8,
		AdmissionGate: gate,
	})
	t.Cleanup(registry.Stop)
	return registry, gate, calculator, base
}

func stopAndWaitFarmShard(t *testing.T, registry *Registry, gate *actorruntime.ShardDrainGate, calculator *actorrouting.Calculator, farmID uint64) (*ShardFactsBarrier, uint32) {
	t.Helper()
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
	status, _ := gate.Status(shard)
	if !status.WaitComplete || status.AcceptedJobs != 0 {
		t.Fatalf("gate status=%+v", status)
	}
	return barrier, shard
}

func TestFarmFactsBarrierRequiresAuthorityAndProvesEmptyEnumeration(t *testing.T) {
	registry, gate, calculator, _ := newGatedFarmFactsRegistry(t, 1101)
	if err := registry.ConfirmShardFactsCommitted(context.Background(), 0, func(actorID uint64) (uint32, error) {
		return calculator.ShardFor(actorrouting.DomainFarm, actorID)
	}); !errors.Is(err, ErrFactsBarrierPrecondition) {
		t.Fatalf("facts before Stop/Wait err=%v", err)
	}
	barrier, shard := stopAndWaitFarmShard(t, registry, gate, calculator, 1101)
	if err := barrier.ConfirmFactsCommitted(context.Background(), shard); err != nil {
		t.Fatalf("empty authority enumeration failed: %v", err)
	}

	legacyCalculator := ar1Calculator(t)
	legacyGate, err := actorruntime.NewShardDrainGate(actorruntime.ShardDrainGateOptions{
		ShardCount: 64,
		ShardFor: func(actorID uint64) (uint32, error) {
			return legacyCalculator.ShardFor(actorrouting.DomainFarm, actorID)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	legacy := NewRegistryWithOptions(blobstore.NewMemoryStore(), RegistryOptions{AdmissionGate: legacyGate})
	t.Cleanup(legacy.Stop)
	legacyShard, err := legacyCalculator.ShardFor(actorrouting.DomainFarm, 1102)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.StopNewActivationAndTurns(context.Background(), legacyShard); err != nil {
		t.Fatal(err)
	}
	if err := legacy.WaitCurrentTurns(context.Background(), legacyShard); err != nil {
		t.Fatal(err)
	}
	legacyBarrier, err := legacy.NewShardFactsBarrier(func(actorID uint64) (uint32, error) {
		return legacyCalculator.ShardFor(actorrouting.DomainFarm, actorID)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := legacyBarrier.ConfirmFactsCommitted(context.Background(), legacyShard); !errors.Is(err, ErrFactsBarrierLegacy) {
		t.Fatalf("legacy facts err=%v", err)
	}
}

func TestFarmHolderShardRuntimeRearmsCompletedDrain(t *testing.T) {
	registry, gate, calculator, _ := newGatedFarmFactsRegistry(t, 1110)
	_, shard := stopAndWaitFarmShard(t, registry, gate, calculator, 1110)
	runtime, err := NewHolderShardRuntime(
		registry,
		func(actorID uint64) (uint32, error) {
			return calculator.ShardFor(actorrouting.DomainFarm, actorID)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.RearmShard(context.Background(), shard); err != nil {
		t.Fatal(err)
	}
	status, err := runtime.GetShardStatus(context.Background(), shard)
	if err != nil || status.Phase != actorruntime.ShardDrainPhaseOpen ||
		status.WaitComplete {
		t.Fatalf("rearmed status=%+v err=%v", status, err)
	}
}

func TestFarmFactsBarrierRejectsDirtyLaneAndConcurrentActorReplacement(t *testing.T) {
	registry, gate, calculator, _ := newGatedFarmFactsRegistry(t, 1103)
	if _, err := registry.Submit(context.Background(), 1103, func(_ context.Context, _ *FarmActor) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	actor := registry.actors[1103]
	registry.mu.Unlock()
	if actor == nil {
		t.Fatal("actor missing")
	}
	binding, ok := actor.store.(*authority.Binding)
	if !ok || binding == nil {
		t.Fatal("authority binding missing")
	}
	if err := binding.Lane().MarkMemoryVersion(binding.Version() + 1); err != nil {
		t.Fatal(err)
	}
	barrier, shard := stopAndWaitFarmShard(t, registry, gate, calculator, 1103)
	if err := barrier.ConfirmFactsCommitted(context.Background(), shard); !errors.Is(err, ErrFactsBarrierPrecondition) {
		t.Fatalf("dirty farm lane err=%v", err)
	}

	registry2, gate2, calculator2, _ := newGatedFarmFactsRegistry(t, 1104)
	if _, err := registry2.Submit(context.Background(), 1104, func(_ context.Context, _ *FarmActor) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	barrier2, shard2 := stopAndWaitFarmShard(t, registry2, gate2, calculator2, 1104)
	targets, err := barrier2.snapshotTargets(shard2)
	if err != nil || len(targets) != 1 {
		t.Fatalf("targets=%+v err=%v", targets, err)
	}
	registry2.mu.Lock()
	delete(registry2.actors, 1104)
	registry2.mu.Unlock()
	if err := registry2.drainActorForFacts(context.Background(), targets[0]); !errors.Is(err, ErrFactsActorChanged) {
		t.Fatalf("actor replacement err=%v", err)
	}
}
