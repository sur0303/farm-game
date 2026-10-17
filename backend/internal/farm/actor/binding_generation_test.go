package actor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	farmpb "farmgame/api/gen/farm"
	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/activation"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/logic/actor/persistence"
	actorruntime "farmgame/internal/logic/actor/runtime"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
)

type mutableFarmFenceSource struct {
	mu    sync.RWMutex
	fence authoritystore.ShardWriteFence
	err   error
}

func (s *mutableFarmFenceSource) WriteFence(context.Context, authoritystore.ActorAddress) (authoritystore.ShardWriteFence, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fence, s.err
}

func (s *mutableFarmFenceSource) Set(fence authoritystore.ShardWriteFence) {
	s.mu.Lock()
	s.fence, s.err = fence, nil
	s.mu.Unlock()
}

type farmBindingGenerationHarness struct {
	farmID     uint64
	calculator *actorrouting.Calculator
	store      *authoritystore.MemoryStore
	fault      *authoritystore.FaultStore
	source     *mutableFarmFenceSource
	registry   *Registry
	runtime    *HolderShardRuntime
	shard      uint32
}

func newFarmBindingGenerationHarness(t *testing.T, farmID uint64) *farmBindingGenerationHarness {
	t.Helper()
	calculator := ar1Calculator(t)
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver: calculator, AuthorityLayoutVersion: "v1", Now: func() time.Time { return ar1Now },
	})
	if err != nil {
		t.Fatal(err)
	}
	shard, err := calculator.ShardFor(actorrouting.DomainFarm, farmID)
	if err != nil {
		t.Fatal(err)
	}
	h := &farmBindingGenerationHarness{farmID: farmID, calculator: calculator, store: store, shard: shard}
	first := h.setAuthority(t, "farm-binding-holder-a", 1)
	h.source = &mutableFarmFenceSource{fence: first}
	h.fault = authoritystore.NewFaultStore(store)
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store: h.fault, Fence: h.source, AuthorityLayoutVersion: "v1", BootID: "farm-binding-process",
	})
	if err != nil {
		t.Fatal(err)
	}
	gate, err := actorruntime.NewShardDrainGate(actorruntime.ShardDrainGateOptions{
		ShardCount: 64,
		ShardFor: func(actorID uint64) (uint32, error) {
			return calculator.ShardFor(actorrouting.DomainFarm, actorID)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.registry = NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 32, AdmissionGate: gate})
	t.Cleanup(h.registry.Stop)
	h.runtime, err = NewHolderShardRuntime(h.registry, func(actorID uint64) (uint32, error) {
		return calculator.ShardFor(actorrouting.DomainFarm, actorID)
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *farmBindingGenerationHarness) setAuthority(t *testing.T, holder string, generation uint64) authoritystore.ShardWriteFence {
	t.Helper()
	authorityValue := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "v1",
		Domain:                 actorrouting.DomainFarm,
		VirtualShard:           h.shard,
		HolderID:               holder,
		HolderBootID:           holder + "-boot",
		AssignmentRevision:     generation,
		WriterEpoch:            generation,
		LeaseEpoch:             generation,
		State:                  authoritystore.ShardAuthorityActive,
		LeaseUntilMs:           ar1Now.Add(time.Minute).UnixMilli(),
	}
	if err := h.store.SetShardWriteAuthority(authorityValue); err != nil {
		t.Fatal(err)
	}
	return authoritystore.ShardWriteFence{
		ExpectedHolderID:           authorityValue.HolderID,
		ExpectedHolderBootID:       authorityValue.HolderBootID,
		ExpectedAssignmentRevision: generation,
		ExpectedWriterEpoch:        generation,
		ExpectedLeaseEpoch:         generation,
	}
}

func (h *farmBindingGenerationHarness) mutateAtGeneration(t *testing.T, fence authoritystore.ShardWriteFence, plot uint32, operationID string) {
	t.Helper()
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store: h.store, Fence: authority.StaticFenceSource{Fence: fence}, AuthorityLayoutVersion: "v1",
		BootID: fence.ExpectedHolderBootID,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	if _, err := ar1PlantAt(context.Background(), registry, h.farmID, plot, operationID, []byte(operationID)); err != nil {
		t.Fatal(err)
	}
}

func (h *farmBindingGenerationHarness) currentActorBinding(t *testing.T) (*FarmActor, *authority.Binding) {
	t.Helper()
	h.registry.mu.Lock()
	defer h.registry.mu.Unlock()
	actor := h.registry.actors[h.farmID]
	binding := authorityBindingForFarmActor(actor)
	if actor == nil || binding == nil {
		t.Fatal("farm authority actor/binding missing")
	}
	return actor, binding
}

func TestFarmBindingDrainMoveMutateReturnReloadsEveryReadPath(t *testing.T) {
	const farmID = uint64(43_001)
	h := newFarmBindingGenerationHarness(t, farmID)
	ctx := context.Background()
	if _, err := ar1PlantAt(ctx, h.registry, farmID, 0, "farm-binding-first", []byte("farm-binding-first")); err != nil {
		t.Fatal(err)
	}
	_, oldBinding := h.currentActorBinding(t)

	if err := h.runtime.StopNewActivationAndTurns(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
	if err := h.runtime.WaitCurrentTurns(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
	if err := h.runtime.ConfirmFactsCommitted(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
	if !oldBinding.Retired() || oldBinding.CurrentModelSnapshot().RecordVersion != 0 {
		t.Fatal("drained Farm binding remained readable")
	}
	h.registry.mu.Lock()
	active, tombstones := len(h.registry.actors), len(h.registry.retiredBindings)
	h.registry.mu.Unlock()
	if active != 0 || tombstones != 0 {
		t.Fatalf("facts retained active=%d tombstones=%d", active, tombstones)
	}

	movedFence := h.setAuthority(t, "farm-binding-holder-b", 2)
	h.mutateAtGeneration(t, movedFence, 1, "farm-binding-moved")
	returnFence := h.setAuthority(t, "farm-binding-holder-a", 3)
	h.source.Set(returnFence)
	if err := h.runtime.RearmShard(ctx, h.shard); err != nil {
		t.Fatal(err)
	}

	value, err := h.registry.Submit(ctx, farmID, func(_ context.Context, actor *FarmActor) (any, error) {
		return actor.Snapshot(time.UnixMilli(1_800_000_000_000)), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if planted := ar1PlantedPlots(value.(*farmpb.FarmSnapshot)); planted != 2 {
		t.Fatalf("no-change snapshot used stale generation: planted=%d", planted)
	}
	replayed, err := ar1PlantAt(ctx, h.registry, farmID, 1, "farm-binding-moved", []byte("farm-binding-moved"))
	if err != nil || !replayed.Replayed {
		t.Fatalf("replay after return=%+v err=%v", replayed, err)
	}
	record, version, err := h.registry.ReadOutsideMailbox(ctx, farmID)
	if err != nil || record == nil || version < 3 {
		t.Fatalf("outside-mailbox read record=%v version=%d err=%v", record, version, err)
	}
}

func TestFarmRearmRejectsUndrainedActiveGeneration(t *testing.T) {
	const farmID = uint64(43_006)
	h := newFarmBindingGenerationHarness(t, farmID)
	ctx := context.Background()
	if _, err := h.registry.Submit(ctx, farmID, func(context.Context, *FarmActor) (any, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	if err := h.runtime.StopNewActivationAndTurns(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
	if err := h.runtime.WaitCurrentTurns(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
	if err := h.runtime.RearmShard(ctx, h.shard); !errors.Is(err, ErrFactsBarrierPrecondition) {
		t.Fatalf("premature Farm rearm err=%v", err)
	}
	if err := h.runtime.ConfirmFactsCommitted(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
	if err := h.runtime.RearmShard(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
}

func TestFarmRetiredUnknownLeaseLossSettlesAndFactsEvicts(t *testing.T) {
	const farmID = uint64(43_002)
	h := newFarmBindingGenerationHarness(t, farmID)
	h.registry.SetAutoResolveUnknown(false)
	ctx := context.Background()
	h.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
	if _, err := ar1Plant(ctx, h.registry, farmID, "farm-binding-unknown", []byte("farm-binding-unknown")); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("ambiguous Farm commit err=%v", err)
	}
	_, binding := h.currentActorBinding(t)
	if !binding.HasPendingUnknown() {
		t.Fatal("Farm UNKNOWN evidence was not retained")
	}
	if err := h.registry.NotifyLeaseLost(ctx, farmID); err != nil {
		t.Fatal(err)
	}
	if !binding.Retired() || binding.HasPendingUnknown() {
		t.Fatalf("retired Farm UNKNOWN pending=%t retired=%t", binding.HasPendingUnknown(), binding.Retired())
	}
	if lane := binding.Lane().Snapshot(); lane.State != persistence.Draining || lane.Dirty {
		t.Fatalf("retired Farm UNKNOWN lane=%+v", lane)
	}
	if err := h.runtime.StopNewActivationAndTurns(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
	if err := h.runtime.WaitCurrentTurns(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
	if err := h.runtime.ConfirmFactsCommitted(ctx, h.shard); err != nil {
		t.Fatalf("Farm facts wedged on retired UNKNOWN: %v", err)
	}
	h.registry.mu.Lock()
	remaining := len(h.registry.actors) + len(h.registry.retiredBindings)
	h.registry.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("Farm facts retained %d generation identities", remaining)
	}
}

func TestFarmResolutionReconcileIsTerminalGenerationLoss(t *testing.T) {
	const farmID = uint64(43_003)
	h := newFarmBindingGenerationHarness(t, farmID)
	h.registry.SetAutoResolveUnknown(false)
	ctx := context.Background()
	h.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownBeforeApply)
	if _, err := ar1Plant(ctx, h.registry, farmID, "farm-binding-reconcile", []byte("farm-binding-reconcile")); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("ambiguous Farm commit err=%v", err)
	}
	_, oldBinding := h.currentActorBinding(t)
	opID, claimID := h.registry.authority.NewClaimIdentity(farmID)
	if _, status, err := h.registry.authority.Claim(ctx, actorrouting.DomainFarm, farmID, opID, claimID, nil); err != nil || status != authority.ClaimAcquired {
		t.Fatalf("competing claim status=%v err=%v", status, err)
	}
	resolution, err := h.registry.ResolveUnknown(ctx, farmID)
	if resolution != UnknownStillUnknown || !errors.Is(err, activation.ErrStaleRoute) || errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("reconcile resolution=%v err=%v", resolution, err)
	}
	if !oldBinding.Retired() || oldBinding.HasPendingUnknown() {
		t.Fatalf("reconciled binding pending=%t retired=%t", oldBinding.HasPendingUnknown(), oldBinding.Retired())
	}
	if state := h.registry.Activation().StateOf(farmID); state != activation.Absent {
		t.Fatalf("reconcile activation=%s", state)
	}
	if _, err := h.registry.Submit(ctx, farmID, func(context.Context, *FarmActor) (any, error) {
		return nil, nil
	}); !errors.Is(err, authority.ErrBindingRetired) {
		t.Fatalf("same-generation submit after reconcile err=%v", err)
	}
}

func TestFarmOutsideMailboxReadRejectsChangedGeneration(t *testing.T) {
	const farmID = uint64(43_007)
	h := newFarmBindingGenerationHarness(t, farmID)
	ctx := context.Background()
	if _, err := h.registry.Submit(ctx, farmID, func(context.Context, *FarmActor) (any, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	_, oldBinding := h.currentActorBinding(t)
	next := h.setAuthority(t, "farm-binding-holder-b", 2)
	h.source.Set(next)
	if _, _, err := h.registry.ReadOutsideMailbox(ctx, farmID); !errors.Is(err, blobstore.ErrWriterEpochMismatch) {
		t.Fatalf("stale outside-mailbox read err=%v", err)
	}
	if !oldBinding.Retired() || oldBinding.CurrentModelSnapshot().RecordVersion != 0 {
		t.Fatal("outside-mailbox generation loss retained readable state")
	}
	h.registry.mu.Lock()
	actor, tombstone := h.registry.actors[farmID], h.registry.retiredBindings[farmID]
	h.registry.mu.Unlock()
	if actor != nil || tombstone != oldBinding {
		t.Fatalf("outside read cleanup actor=%p tombstone=%p old=%p", actor, tombstone, oldBinding)
	}
}

func TestFarmCallbackErrorStillValidatesGeneration(t *testing.T) {
	const farmID = uint64(43_004)
	h := newFarmBindingGenerationHarness(t, farmID)
	ctx := context.Background()
	if _, err := h.registry.Submit(ctx, farmID, func(context.Context, *FarmActor) (any, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	_, oldBinding := h.currentActorBinding(t)
	callbackErr := errors.New("farm callback rejected candidate")
	_, err := h.registry.Submit(ctx, farmID, func(context.Context, *FarmActor) (any, error) {
		next := h.setAuthority(t, "farm-binding-holder-b", 2)
		h.source.Set(next)
		return nil, callbackErr
	})
	if !errors.Is(err, callbackErr) || !errors.Is(err, blobstore.ErrWriterEpochMismatch) {
		t.Fatalf("Farm callback generation outcome err=%v", err)
	}
	if !oldBinding.Retired() || oldBinding.CurrentModelSnapshot().RecordVersion != 0 {
		t.Fatal("Farm callback error retained readable old-generation state")
	}
	if _, err := h.registry.Submit(ctx, farmID, func(context.Context, *FarmActor) (any, error) { return nil, nil }); err != nil {
		t.Fatalf("fresh Farm generation after callback error: %v", err)
	}
}

func TestFarmQueuedUserCannotRunAfterLeaseCleanup(t *testing.T) {
	const farmID = uint64(43_005)
	h := newFarmBindingGenerationHarness(t, farmID)
	ctx := context.Background()
	if _, err := h.registry.Submit(ctx, farmID, func(context.Context, *FarmActor) (any, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := h.registry.Submit(ctx, farmID, func(context.Context, *FarmActor) (any, error) {
			close(started)
			<-release
			return nil, nil
		})
		firstDone <- err
	}()
	<-started
	var queuedRan atomic.Bool
	queuedDone := make(chan error, 1)
	go func() {
		_, err := h.registry.Submit(ctx, farmID, func(context.Context, *FarmActor) (any, error) {
			queuedRan.Store(true)
			return nil, nil
		})
		queuedDone <- err
	}()
	waitFarmMailboxQueues(t, h.registry, farmID, func(snapshot actorruntime.MailboxSnapshot) bool { return snapshot.UserQueued > 0 })
	leaseDone := make(chan error, 1)
	go func() { leaseDone <- h.registry.NotifyLeaseLost(ctx, farmID) }()
	waitFarmMailboxQueues(t, h.registry, farmID, func(snapshot actorruntime.MailboxSnapshot) bool { return snapshot.SystemQueued > 0 })
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Farm user: %v", err)
	}
	if err := <-leaseDone; err != nil {
		t.Fatalf("Farm lease cleanup: %v", err)
	}
	if err := <-queuedDone; !errors.Is(err, authority.ErrBindingRetired) {
		t.Fatalf("queued Farm user err=%v", err)
	}
	if queuedRan.Load() {
		t.Fatal("queued Farm callback ran after lease cleanup")
	}
}

func waitFarmMailboxQueues(t *testing.T, registry *Registry, farmID uint64, ready func(actorruntime.MailboxSnapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if snapshot, ok := registry.exec.MailboxSnapshot(farmID); ok && ready(snapshot) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Farm mailbox queue condition was not reached")
}
