package actor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	playerpb "farmgame/api/gen/player"
	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/logic/actor/persistence"
	actorruntime "farmgame/internal/logic/actor/runtime"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
)

type mutablePlayerFenceSource struct {
	mu    sync.RWMutex
	fence authoritystore.ShardWriteFence
	err   error
}

func (s *mutablePlayerFenceSource) WriteFence(context.Context, authoritystore.ActorAddress) (authoritystore.ShardWriteFence, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fence, s.err
}

func (s *mutablePlayerFenceSource) Set(fence authoritystore.ShardWriteFence) {
	s.mu.Lock()
	s.fence, s.err = fence, nil
	s.mu.Unlock()
}

type playerBindingGenerationHarness struct {
	playerID   uint64
	calculator *actorrouting.Calculator
	store      *authoritystore.MemoryStore
	fault      *authoritystore.FaultStore
	source     *mutablePlayerFenceSource
	registry   *Registry
	runtime    *HolderShardRuntime
	shard      uint32
}

func newPlayerBindingGenerationHarness(t *testing.T, playerID uint64) *playerBindingGenerationHarness {
	t.Helper()
	calculator := ar1pCalculator(t)
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver: calculator, AuthorityLayoutVersion: "v1", Now: func() time.Time { return ar1pNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	shard, err := calculator.ShardFor(actorrouting.DomainPlayer, playerID)
	if err != nil {
		t.Fatal(err)
	}
	h := &playerBindingGenerationHarness{playerID: playerID, calculator: calculator, store: store, shard: shard}
	first := h.setAuthority(t, "binding-holder-a", 1)
	h.source = &mutablePlayerFenceSource{fence: first}
	h.fault = authoritystore.NewFaultStore(store)
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store: h.fault, Fence: h.source, AuthorityLayoutVersion: "v1", BootID: "binding-process-boot",
	})
	if err != nil {
		t.Fatal(err)
	}
	gate, err := actorruntime.NewShardDrainGate(actorruntime.ShardDrainGateOptions{
		ShardCount: 64,
		ShardFor: func(actorID uint64) (uint32, error) {
			return calculator.ShardFor(actorrouting.DomainPlayer, actorID)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.registry = NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 64, AdmissionGate: gate})
	t.Cleanup(h.registry.Stop)
	h.runtime, err = NewHolderShardRuntime(h.registry, func(actorID uint64) (uint32, error) {
		return calculator.ShardFor(actorrouting.DomainPlayer, actorID)
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *playerBindingGenerationHarness) setAuthority(t *testing.T, holder string, generation uint64) authoritystore.ShardWriteFence {
	t.Helper()
	authorityValue := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "v1",
		Domain:                 actorrouting.DomainPlayer,
		VirtualShard:           h.shard,
		HolderID:               holder,
		HolderBootID:           holder + "-boot",
		AssignmentRevision:     generation,
		WriterEpoch:            generation,
		LeaseEpoch:             generation,
		State:                  authoritystore.ShardAuthorityActive,
		LeaseUntilMs:           ar1pNow.Add(time.Minute).UnixMilli(),
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

func (h *playerBindingGenerationHarness) mutateAtGeneration(t *testing.T, fence authoritystore.ShardWriteFence, coin int64) {
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
	if _, err := registry.Submit(context.Background(), h.playerID, func(record *playerpb.PlayerRecord) (any, bool, error) {
		record.Wallet.Coin = coin
		record.EconomyVersion++
		return nil, true, nil
	}); err != nil {
		t.Fatal(err)
	}
}

func (h *playerBindingGenerationHarness) currentBinding(t *testing.T) *authority.Binding {
	t.Helper()
	h.registry.bindingsMu.Lock()
	defer h.registry.bindingsMu.Unlock()
	binding := h.registry.bindings[h.playerID]
	if binding == nil {
		t.Fatal("player binding missing")
	}
	return binding
}

func TestPlayerBindingDrainMoveMutateRearmDoesNotReuseSnapshot(t *testing.T) {
	const playerID = uint64(42_001)
	const movedCoin = int64(713)
	h := newPlayerBindingGenerationHarness(t, playerID)
	ctx := context.Background()
	if _, _, err := h.registry.Ensure(ctx, playerID, "binding-drain"); err != nil {
		t.Fatal(err)
	}
	oldBinding := h.currentBinding(t)

	if err := h.runtime.StopNewActivationAndTurns(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
	if err := h.runtime.WaitCurrentTurns(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
	if err := h.runtime.ConfirmFactsCommitted(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
	if oldBinding.Lane().Snapshot().State != persistence.Draining || oldBinding.Usable() {
		t.Fatalf("drained binding remained usable: lane=%+v usable=%t", oldBinding.Lane().Snapshot(), oldBinding.Usable())
	}
	if snapshot := oldBinding.CurrentModelSnapshot(); snapshot.RecordVersion != 0 || snapshot.ModelFields != nil {
		t.Fatalf("drained binding exposed cached snapshot: %+v", snapshot)
	}
	if _, _, err := oldBinding.Get(ctx, playerID); err == nil {
		t.Fatal("drained binding Get unexpectedly succeeded")
	}
	if bindings, _ := h.registry.AuthorityStats(); bindings != 0 {
		t.Fatalf("facts barrier retained %d usable bindings", bindings)
	}
	h.registry.bindingsMu.Lock()
	remainingBindings := len(h.registry.bindings)
	h.registry.bindingsMu.Unlock()
	if remainingBindings != 0 {
		t.Fatalf("facts barrier retained %d binding identities", remainingBindings)
	}

	movedFence := h.setAuthority(t, "binding-holder-b", 2)
	h.mutateAtGeneration(t, movedFence, movedCoin)
	returnFence := h.setAuthority(t, "binding-holder-a", 3)
	h.source.Set(returnFence)
	if err := h.runtime.RearmShard(ctx, h.shard); err != nil {
		t.Fatal(err)
	}

	record, version, err := h.registry.ReadRoutedCurrent(ctx, playerID)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetWallet().GetCoin() != movedCoin || version != 2 {
		t.Fatalf("rearmed routed read returned stale state: coin=%d version=%d", record.GetWallet().GetCoin(), version)
	}
	result, err := h.registry.SubmitVersioned(ctx, playerID, func(record *playerpb.PlayerRecord, _ uint64) (any, bool, error) {
		return record.GetWallet().GetCoin(), false, nil
	})
	if err != nil || result.Value != movedCoin || result.CommittedRecordVersion != version {
		t.Fatalf("no-change turn result=%+v err=%v", result, err)
	}
	result, err = h.registry.SubmitDurableCommand(ctx, playerID, func(record *playerpb.PlayerRecord, _ uint64) (any, bool, authoritystore.StoredOperationOutcome, []authoritystore.AttachedOrder, error) {
		return record.GetWallet().GetCoin(), false, authoritystore.StoredOperationOutcome{}, nil, nil
	})
	if err != nil || result.Value != movedCoin || result.CommittedRecordVersion != version {
		t.Fatalf("replay turn result=%+v err=%v", result, err)
	}
}

func TestPlayerBindingRearmRejectsUndrainedBinding(t *testing.T) {
	const playerID = uint64(42_004)
	h := newPlayerBindingGenerationHarness(t, playerID)
	ctx := context.Background()
	if _, _, err := h.registry.Ensure(ctx, playerID, "binding-premature-rearm"); err != nil {
		t.Fatal(err)
	}
	if err := h.runtime.StopNewActivationAndTurns(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
	if err := h.runtime.WaitCurrentTurns(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
	if err := h.registry.RearmShard(ctx, h.shard); !errors.Is(err, ErrFactsBarrierPrecondition) {
		t.Fatalf("premature registry rearm err=%v", err)
	}
	if err := h.runtime.RearmShard(ctx, h.shard); !errors.Is(err, ErrFactsBarrierPrecondition) {
		t.Fatalf("premature runtime rearm err=%v", err)
	}
	status, err := h.runtime.GetShardStatus(ctx, h.shard)
	if err != nil || status.Phase != actorruntime.ShardDrainPhaseDraining {
		t.Fatalf("premature rearm opened shard: status=%+v err=%v", status, err)
	}
	if err := h.runtime.ConfirmFactsCommitted(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
	if err := h.runtime.RearmShard(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
}

func TestPlayerBindingLeaseLossBlocksSameGenerationAndReloadsNewGeneration(t *testing.T) {
	const playerID = uint64(42_002)
	const movedCoin = int64(811)
	h := newPlayerBindingGenerationHarness(t, playerID)
	ctx := context.Background()
	if _, _, err := h.registry.Ensure(ctx, playerID, "binding-lease-loss"); err != nil {
		t.Fatal(err)
	}
	oldBinding := h.currentBinding(t)
	if err := h.registry.NotifyLeaseLost(ctx, playerID); err != nil {
		t.Fatal(err)
	}
	if oldBinding.Usable() || oldBinding.CurrentModelSnapshot().RecordVersion != 0 {
		t.Fatal("lease-lost binding retained a usable cached snapshot")
	}
	// Mailbox retirement must not erase the terminal generation tombstone and
	// permit a same-generation LoadAndClaim.
	_ = h.registry.tryRetireIdle(ctx, playerID)
	h.registry.bindingsMu.Lock()
	retained := h.registry.bindings[playerID]
	h.registry.bindingsMu.Unlock()
	if retained != oldBinding {
		t.Fatal("idle retirement erased the lease-loss generation tombstone")
	}
	if _, _, err := h.registry.ReadRoutedCurrent(ctx, playerID); !errors.Is(err, authority.ErrBindingRetired) || !errors.Is(err, blobstore.ErrWriterEpochMismatch) {
		t.Fatalf("same-generation routed read err=%v", err)
	}
	if _, err := h.registry.SubmitVersioned(ctx, playerID, func(record *playerpb.PlayerRecord, _ uint64) (any, bool, error) {
		return record.GetWallet().GetCoin(), false, nil
	}); !errors.Is(err, authority.ErrBindingRetired) {
		t.Fatalf("same-generation no-change turn err=%v", err)
	}

	movedFence := h.setAuthority(t, "binding-holder-b", 2)
	h.mutateAtGeneration(t, movedFence, movedCoin)
	returnFence := h.setAuthority(t, "binding-holder-a", 3)
	h.source.Set(returnFence)
	result, err := h.registry.SubmitVersioned(ctx, playerID, func(record *playerpb.PlayerRecord, _ uint64) (any, bool, error) {
		return record.GetWallet().GetCoin(), false, nil
	})
	if err != nil || result.Value != movedCoin || result.CommittedRecordVersion != 2 {
		t.Fatalf("new-generation no-change turn result=%+v err=%v", result, err)
	}
}

func TestPlayerRetiredUnknownLeaseLossSettlesAndFactsEvicts(t *testing.T) {
	const playerID = uint64(42_005)
	h := newPlayerBindingGenerationHarness(t, playerID)
	ctx := context.Background()
	if _, _, err := h.registry.Ensure(ctx, playerID, "binding-retired-unknown"); err != nil {
		t.Fatal(err)
	}
	h.fault.Inject(authoritystore.MethodCommit, authoritystore.FaultTimeoutUnknownAfterApply)
	h.fault.Inject(authoritystore.MethodFenceAndClaim, authoritystore.FaultTimeoutUnknownBeforeApply)
	if _, err := h.registry.SubmitVersioned(ctx, playerID, func(record *playerpb.PlayerRecord, _ uint64) (any, bool, error) {
		record.Wallet.Coin++
		return nil, true, nil
	}); err == nil {
		t.Fatal("ambiguous commit unexpectedly resolved in the user turn")
	}
	binding := h.currentBinding(t)
	if !binding.HasPendingUnknown() || binding.Retired() {
		t.Fatalf("pending binding before lease loss: pending=%t retired=%t", binding.HasPendingUnknown(), binding.Retired())
	}

	if err := h.registry.NotifyLeaseLost(ctx, playerID); err != nil {
		t.Fatal(err)
	}
	if binding.HasPendingUnknown() || !binding.Retired() {
		t.Fatalf("lease-loss lifecycle settlement: pending=%t retired=%t", binding.HasPendingUnknown(), binding.Retired())
	}
	if lane := binding.Lane().Snapshot(); lane.State != persistence.Draining || lane.Dirty {
		t.Fatalf("retired UNKNOWN lane=%+v", lane)
	}

	if err := h.runtime.StopNewActivationAndTurns(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
	if err := h.runtime.WaitCurrentTurns(ctx, h.shard); err != nil {
		t.Fatal(err)
	}
	if err := h.runtime.ConfirmFactsCommitted(ctx, h.shard); err != nil {
		t.Fatalf("facts barrier wedged on retired UNKNOWN: %v", err)
	}
	h.registry.bindingsMu.Lock()
	remaining := len(h.registry.bindings)
	h.registry.bindingsMu.Unlock()
	if remaining != 0 {
		t.Fatalf("facts retained %d player tombstones", remaining)
	}
}

func TestPlayerCallbackErrorStillValidatesGeneration(t *testing.T) {
	const playerID = uint64(42_006)
	h := newPlayerBindingGenerationHarness(t, playerID)
	ctx := context.Background()
	if _, _, err := h.registry.Ensure(ctx, playerID, "binding-error-generation"); err != nil {
		t.Fatal(err)
	}
	oldBinding := h.currentBinding(t)
	callbackErr := errors.New("callback rejected candidate")
	_, err := h.registry.SubmitVersioned(ctx, playerID, func(*playerpb.PlayerRecord, uint64) (any, bool, error) {
		next := h.setAuthority(t, "binding-holder-b", 2)
		h.source.Set(next)
		return nil, false, callbackErr
	})
	if !errors.Is(err, callbackErr) || !errors.Is(err, blobstore.ErrWriterEpochMismatch) {
		t.Fatalf("callback generation outcome err=%v", err)
	}
	if !oldBinding.Retired() || oldBinding.CurrentModelSnapshot().RecordVersion != 0 {
		t.Fatal("callback error exposed or retained the old generation")
	}
	// The changed complete fence permits a fresh claim; the callback error did
	// not leave activation state able to erase that replacement.
	if _, err := h.registry.SubmitVersioned(ctx, playerID, func(record *playerpb.PlayerRecord, _ uint64) (any, bool, error) {
		return record.GetWallet().GetCoin(), false, nil
	}); err != nil {
		t.Fatalf("fresh generation after callback error: %v", err)
	}
}

func TestPlayerQueuedUserCannotRunAfterLeaseCleanup(t *testing.T) {
	const playerID = uint64(42_007)
	h := newPlayerBindingGenerationHarness(t, playerID)
	ctx := context.Background()
	if _, _, err := h.registry.Ensure(ctx, playerID, "binding-queued-cleanup"); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := h.registry.SubmitVersioned(ctx, playerID, func(*playerpb.PlayerRecord, uint64) (any, bool, error) {
			close(started)
			<-release
			return nil, false, nil
		})
		firstDone <- err
	}()
	<-started

	var queuedRan atomic.Bool
	queuedDone := make(chan error, 1)
	go func() {
		_, err := h.registry.SubmitVersioned(ctx, playerID, func(*playerpb.PlayerRecord, uint64) (any, bool, error) {
			queuedRan.Store(true)
			return nil, false, nil
		})
		queuedDone <- err
	}()
	waitPlayerMailboxQueues(t, h.registry, playerID, func(snapshot actorruntime.MailboxSnapshot) bool {
		return snapshot.UserQueued > 0
	})
	leaseDone := make(chan error, 1)
	go func() { leaseDone <- h.registry.NotifyLeaseLost(ctx, playerID) }()
	waitPlayerMailboxQueues(t, h.registry, playerID, func(snapshot actorruntime.MailboxSnapshot) bool {
		return snapshot.SystemQueued > 0
	})
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first user turn: %v", err)
	}
	if err := <-leaseDone; err != nil {
		t.Fatalf("queued lease cleanup: %v", err)
	}
	if err := <-queuedDone; !errors.Is(err, authority.ErrBindingRetired) {
		t.Fatalf("queued user err=%v, want retired generation", err)
	}
	if queuedRan.Load() {
		t.Fatal("queued user callback ran after lease cleanup")
	}
}

func waitPlayerMailboxQueues(t *testing.T, registry *Registry, playerID uint64, ready func(actorruntime.MailboxSnapshot) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if snapshot, ok := registry.exec.MailboxSnapshot(playerID); ok && ready(snapshot) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("player mailbox queue condition was not reached")
}

func TestPlayerBindingConcurrentLeaseLossCurrentReadsFailClosed(t *testing.T) {
	const playerID = uint64(42_003)
	h := newPlayerBindingGenerationHarness(t, playerID)
	ctx := context.Background()
	if _, _, err := h.registry.Ensure(ctx, playerID, "binding-lease-race"); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	errs := make(chan error, 33)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, err := h.registry.ReadRoutedCurrent(ctx, playerID)
			if err != nil && !errors.Is(err, authority.ErrBindingRetired) {
				errs <- err
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		errs <- h.registry.NotifyLeaseLost(ctx, playerID)
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent lease-loss path err=%v", err)
		}
	}
	if _, _, err := h.registry.ReadRoutedCurrent(ctx, playerID); !errors.Is(err, authority.ErrBindingRetired) {
		t.Fatalf("post-lease-loss routed read err=%v", err)
	}
}
