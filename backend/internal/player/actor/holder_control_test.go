package actor

import (
	"context"
	"errors"
	"testing"

	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/logic/actor/persistence"
	actorruntime "farmgame/internal/logic/actor/runtime"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
)

func newGatedPlayerFactsRegistry(t *testing.T, playerID uint64) (*Registry, *actorruntime.ShardDrainGate, *actorrouting.Calculator, *ar1pRig) {
	t.Helper()
	base := ar1pMemoryRig(t, playerID)
	calculator := ar1pCalculator(t)
	gate, err := actorruntime.NewShardDrainGate(actorruntime.ShardDrainGateOptions{
		ShardCount: 64,
		ShardFor: func(actorID uint64) (uint32, error) {
			return calculator.ShardFor(actorrouting.DomainPlayer, actorID)
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

func stopAndWaitPlayerShard(t *testing.T, registry *Registry, gate *actorruntime.ShardDrainGate, calculator *actorrouting.Calculator, playerID uint64) (*ShardFactsBarrier, uint32) {
	t.Helper()
	shard, err := calculator.ShardFor(actorrouting.DomainPlayer, playerID)
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
		return calculator.ShardFor(actorrouting.DomainPlayer, actorID)
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

func TestPlayerFactsBarrierRequiresStopWaitAndProvesEmptyEnumeration(t *testing.T) {
	registry, gate, calculator, _ := newGatedPlayerFactsRegistry(t, 1001)
	if err := registry.ConfirmShardFactsCommitted(context.Background(), 0, nil); !errors.Is(err, ErrHolderControlUnavailable) {
		t.Fatalf("nil mapper err=%v", err)
	}
	barrier, shard := stopAndWaitPlayerShard(t, registry, gate, calculator, 1001)
	if err := barrier.ConfirmFactsCommitted(context.Background(), shard); err != nil {
		t.Fatalf("complete authority enumeration with no bindings failed: %v", err)
	}
}

func TestPlayerHolderShardRuntimeRearmsCompletedDrain(t *testing.T) {
	registry, gate, calculator, _ := newGatedPlayerFactsRegistry(t, 1010)
	_, shard := stopAndWaitPlayerShard(t, registry, gate, calculator, 1010)
	runtime, err := NewHolderShardRuntime(
		registry,
		func(actorID uint64) (uint32, error) {
			return calculator.ShardFor(actorrouting.DomainPlayer, actorID)
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

func TestPlayerFactsBarrierFailsClosedForLegacyAndDirtyInflight(t *testing.T) {
	calculator := ar1pCalculator(t)
	legacyGate, err := actorruntime.NewShardDrainGate(actorruntime.ShardDrainGateOptions{
		ShardCount: 64,
		ShardFor: func(actorID uint64) (uint32, error) {
			return calculator.ShardFor(actorrouting.DomainPlayer, actorID)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	legacy := NewRegistryWithOptions(blobstore.NewMemoryStore(), RegistryOptions{
		MailboxSize:   4,
		AdmissionGate: legacyGate,
	})
	t.Cleanup(legacy.Stop)
	shard, err := calculator.ShardFor(actorrouting.DomainPlayer, 1002)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.StopNewActivationAndTurns(context.Background(), shard); err != nil {
		t.Fatal(err)
	}
	if err := legacy.WaitCurrentTurns(context.Background(), shard); err != nil {
		t.Fatal(err)
	}
	legacyBarrier, err := legacy.NewShardFactsBarrier(func(actorID uint64) (uint32, error) {
		return calculator.ShardFor(actorrouting.DomainPlayer, actorID)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := legacyBarrier.ConfirmFactsCommitted(context.Background(), shard); !errors.Is(err, ErrFactsBarrierLegacy) {
		t.Fatalf("legacy barrier err=%v", err)
	}

	registry, gate, calculator, base := newGatedPlayerFactsRegistry(t, 1003)
	if _, _, err := registry.Ensure(context.Background(), 1003, "facts-dirty"); err != nil {
		t.Fatal(err)
	}
	registry.bindingsMu.Lock()
	binding := registry.bindings[1003]
	registry.bindingsMu.Unlock()
	if binding == nil {
		t.Fatal("binding missing")
	}
	if err := binding.Lane().MarkMemoryVersion(binding.Version() + 1); err != nil {
		t.Fatal(err)
	}
	barrier, shard := stopAndWaitPlayerShard(t, registry, gate, calculator, 1003)
	if err := barrier.ConfirmFactsCommitted(context.Background(), shard); !errors.Is(err, ErrFactsBarrierPrecondition) {
		t.Fatalf("dirty lane err=%v", err)
	}
	_ = base

	registry2, gate2, calculator2, base2 := newGatedPlayerFactsRegistry(t, 1004)
	if _, _, err := registry2.Ensure(context.Background(), 1004, "facts-inflight"); err != nil {
		t.Fatal(err)
	}
	registry2.bindingsMu.Lock()
	binding2 := registry2.bindings[1004]
	registry2.bindingsMu.Unlock()
	if binding2 == nil {
		t.Fatal("inflight binding missing")
	}
	if err := binding2.Lane().MarkMemoryVersion(binding2.Version() + 1); err != nil {
		t.Fatal(err)
	}
	token, err := binding2.Lane().BeginDurable(binding2.Version() + 1)
	if err != nil {
		t.Fatal(err)
	}
	barrier2, shard2 := stopAndWaitPlayerShard(t, registry2, gate2, calculator2, 1004)
	if err := barrier2.ConfirmFactsCommitted(context.Background(), shard2); !errors.Is(err, ErrFactsBarrierPrecondition) {
		t.Fatalf("inflight lane err=%v", err)
	}
	if err := binding2.Lane().Complete(token, persistence.CompletionNotSent); err != nil {
		t.Fatal(err)
	}
	_ = base2
}

func TestPlayerFactsBarrierResolvesPendingUnknownWithStableBinding(t *testing.T) {
	registry, gate, calculator, base := newGatedPlayerFactsRegistry(t, 1005)
	if _, _, err := registry.Ensure(context.Background(), 1005, "facts-unknown"); err != nil {
		t.Fatal(err)
	}
	registry.bindingsMu.Lock()
	binding := registry.bindings[1005]
	registry.bindingsMu.Unlock()
	if binding == nil {
		t.Fatal("binding missing")
	}
	data, version, err := binding.Get(context.Background(), 1005)
	if err != nil {
		t.Fatal(err)
	}
	base.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
	if err := binding.CASEpoch(context.Background(), 1005, version, 0, data); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("unknown commit err=%v", err)
	}
	if !binding.HasPendingUnknown() {
		t.Fatal("unknown commit did not leave pending marker")
	}
	barrier, shard := stopAndWaitPlayerShard(t, registry, gate, calculator, 1005)
	if err := barrier.ConfirmFactsCommitted(context.Background(), shard); err != nil {
		t.Fatalf("unknown resolution barrier err=%v", err)
	}
	if binding.HasPendingUnknown() || binding.Lane().Snapshot().State != persistence.Draining {
		t.Fatalf("unknown lane not terminal: pending=%v snapshot=%+v", binding.HasPendingUnknown(), binding.Lane().Snapshot())
	}
}

func TestPlayerFactsBarrierRejectsConcurrentBindingIdentityChange(t *testing.T) {
	registry, gate, calculator, _ := newGatedPlayerFactsRegistry(t, 1006)
	if _, _, err := registry.Ensure(context.Background(), 1006, "facts-binding-change"); err != nil {
		t.Fatal(err)
	}
	barrier, shard := stopAndWaitPlayerShard(t, registry, gate, calculator, 1006)
	targets, err := barrier.snapshotTargets(shard)
	if err != nil || len(targets) != 1 {
		t.Fatalf("targets=%+v err=%v", targets, err)
	}
	registry.bindingsMu.Lock()
	registry.bindings[1006] = &authority.Binding{}
	registry.bindingsMu.Unlock()
	if err := registry.drainBindingForFacts(context.Background(), targets[0]); !errors.Is(err, ErrFactsBindingChanged) {
		t.Fatalf("binding replacement err=%v", err)
	}
}
