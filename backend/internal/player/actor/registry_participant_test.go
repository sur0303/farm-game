package actor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/authority"
	actorruntime "farmgame/internal/logic/actor/runtime"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
)

func TestReadOutsideMailboxDoesNotSubmitLegacyTurn(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	t.Cleanup(registry.Stop)
	if _, _, err := registry.Ensure(context.Background(), 9401, "participant-read"); err != nil {
		t.Fatal(err)
	}
	before := registry.RuntimeStats().Submitted
	if _, _, err := registry.ReadOutsideMailbox(context.Background(), 9401); !errors.Is(err, ErrParticipantReceiptAuthorityUnavailable) {
		t.Fatalf("legacy registry must fail closed for participant receipt read: %v", err)
	}
	if after := registry.RuntimeStats().Submitted; after != before {
		t.Fatalf("mailbox-free read submitted an actor turn: %d -> %d", before, after)
	}
}

type legacyReceiptReadTrapStore struct {
	fenceCalls atomic.Int32
	getCalls   atomic.Int32
}

func (s *legacyReceiptReadTrapStore) Get(context.Context, uint64) ([]byte, uint64, error) {
	s.getCalls.Add(1)
	return nil, 0, blobstore.ErrUnavailable
}

func (s *legacyReceiptReadTrapStore) Insert(context.Context, uint64, []byte) error {
	return blobstore.ErrUnavailable
}

func (s *legacyReceiptReadTrapStore) CAS(context.Context, uint64, uint64, []byte) error {
	return blobstore.ErrUnavailable
}

func (s *legacyReceiptReadTrapStore) Fence(context.Context, uint64, uint64) error {
	s.fenceCalls.Add(1)
	return blobstore.ErrWriterEpochMismatch
}

func (s *legacyReceiptReadTrapStore) InsertEpoch(context.Context, uint64, uint64, []byte) error {
	return blobstore.ErrUnavailable
}

func (s *legacyReceiptReadTrapStore) CASEpoch(context.Context, uint64, uint64, uint64, []byte) error {
	return blobstore.ErrUnavailable
}

func TestLegacyReceiptReadNeverRunsFenceThenGetTOCTOU(t *testing.T) {
	store := &legacyReceiptReadTrapStore{}
	registry := NewRegistry(store, 8)
	t.Cleanup(registry.Stop)
	var epochCalls atomic.Int32
	registry.SetWriterEpochResolver(func(uint64) (uint64, error) {
		epochCalls.Add(1)
		return 1, nil
	})
	if _, _, err := registry.ReadOutsideMailbox(context.Background(), 9403); !errors.Is(err, ErrParticipantReceiptAuthorityUnavailable) {
		t.Fatalf("legacy receipt read did not fail closed: %v", err)
	}
	if epochCalls.Load() != 0 || store.fenceCalls.Load() != 0 || store.getCalls.Load() != 0 {
		t.Fatalf("legacy receipt read attempted a non-atomic fence/get path: epoch=%d fence=%d get=%d", epochCalls.Load(), store.fenceCalls.Load(), store.getCalls.Load())
	}
}

func TestReadOutsideMailboxDoesNotSubmitAuthorityTurn(t *testing.T) {
	rig := ar1pMemoryRig(t, 9402)
	if _, _, err := rig.registry.Ensure(context.Background(), 9402, "participant-read-authority"); err != nil {
		t.Fatal(err)
	}
	before := rig.registry.RuntimeStats().Submitted
	if _, _, err := rig.registry.ReadOutsideMailbox(context.Background(), 9402); err != nil {
		t.Fatal(err)
	}
	if after := rig.registry.RuntimeStats().Submitted; after != before {
		t.Fatalf("authority mailbox-free read submitted an actor turn: %d -> %d", before, after)
	}
}

type heldAdmissionGate struct {
	inner *actorruntime.ShardDrainGate

	mu      sync.Mutex
	hold    <-chan struct{}
	entered chan struct{}
}

func (g *heldAdmissionGate) holdNext() (chan struct{}, <-chan struct{}) {
	hold := make(chan struct{})
	entered := make(chan struct{})
	g.mu.Lock()
	g.hold = hold
	g.entered = entered
	g.mu.Unlock()
	return hold, entered
}

func (g *heldAdmissionGate) Admit(ctx context.Context, actorID uint64) (func(), error) {
	release, err := g.inner.Admit(ctx, actorID)
	if err != nil {
		return nil, err
	}
	g.mu.Lock()
	hold, entered := g.hold, g.entered
	g.mu.Unlock()
	if hold == nil || entered == nil {
		release()
		return nil, errors.New("held admission gate was not armed")
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			close(entered)
			<-hold
			release()
		})
	}, nil
}

func (g *heldAdmissionGate) BeginDrain(shard uint32) error {
	return g.inner.BeginDrain(shard)
}

func (g *heldAdmissionGate) Wait(ctx context.Context, shard uint32) error {
	return g.inner.Wait(ctx, shard)
}

func (g *heldAdmissionGate) Status(shard uint32) (actorruntime.ShardDrainStatus, bool) {
	return g.inner.Status(shard)
}

var _ actorruntime.ShardDrainController = (*heldAdmissionGate)(nil)

func TestPlayerMailboxFreeAuthorityAdmissionSharesGateAndFactsBarrier(t *testing.T) {
	const playerID = uint64(9410)
	rig := ar1pMemoryRig(t, playerID)
	if _, _, err := rig.registry.Ensure(context.Background(), playerID, "mailbox-free-admission"); err != nil {
		t.Fatal(err)
	}
	calculator := ar1pCalculator(t)
	inner, err := actorruntime.NewShardDrainGate(actorruntime.ShardDrainGateOptions{
		ShardCount: 64,
		ShardFor: func(actorID uint64) (uint32, error) {
			return calculator.ShardFor(actorrouting.DomainPlayer, actorID)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	gate := &heldAdmissionGate{inner: inner}
	registry := NewAuthorityRegistry(rig.provider, RegistryOptions{
		MailboxSize:   8,
		AdmissionGate: gate,
	})
	t.Cleanup(registry.Stop)

	shard, err := calculator.ShardFor(actorrouting.DomainPlayer, playerID)
	if err != nil {
		t.Fatal(err)
	}
	readHold, readEntered := gate.holdNext()
	readDone := make(chan error, 1)
	go func() {
		_, _, err := registry.ReadOutsideMailbox(context.Background(), playerID)
		readDone <- err
	}()
	<-readEntered
	status, ok := gate.Status(shard)
	if !ok || status.AcceptedJobs != 1 {
		t.Fatalf("mailbox-free read was not admitted into shard barrier: %+v ok=%v", status, ok)
	}
	if got := registry.RuntimeStats().Submitted; got != 0 {
		t.Fatalf("mailbox-free read submitted runtime turn: %d", got)
	}
	close(readHold)
	if err := <-readDone; err != nil {
		t.Fatalf("ReadOutsideMailbox: %v", err)
	}

	registry.bindingsMu.Lock()
	binding := registry.bindings[playerID]
	registry.bindingsMu.Unlock()
	if binding == nil {
		t.Fatal("mailbox-free read did not publish binding before release")
	}
	data, version, err := binding.Get(context.Background(), playerID)
	if err != nil {
		t.Fatal(err)
	}

	capacityHold, capacityEntered := gate.holdNext()
	capacityDone := make(chan error, 1)
	go func() {
		capacityDone <- registry.CheckParticipantAuthorityCapacity(
			context.Background(), playerID, version,
			len(authority.RecordModelField)+len(data),
		)
	}()
	<-capacityEntered
	status, ok = gate.Status(shard)
	if !ok || status.AcceptedJobs != 1 {
		t.Fatalf("capacity preflight did not share shard admission: %+v ok=%v", status, ok)
	}
	close(capacityHold)
	if err := <-capacityDone; err != nil {
		t.Fatalf("capacity preflight: %v", err)
	}

	if err := registry.StopNewActivationAndTurns(context.Background(), shard); err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- registry.WaitCurrentTurns(context.Background(), shard)
	}()
	if err := <-waitDone; err != nil {
		t.Fatal(err)
	}
	barrier, err := registry.NewShardFactsBarrier(func(actorID uint64) (uint32, error) {
		return calculator.ShardFor(actorrouting.DomainPlayer, actorID)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := barrier.ConfirmFactsCommitted(context.Background(), shard); err != nil {
		t.Fatalf("facts barrier did not see admitted binding: %v", err)
	}

	registry.Stop()
	if _, _, err := registry.ReadOutsideMailbox(context.Background(), playerID); !errors.Is(err, actorruntime.ErrStopped) {
		t.Fatalf("ReadOutsideMailbox after Stop err=%v, want ErrStopped", err)
	}
}

type authorityStoreWithoutPhysicalCapabilities struct {
	authoritystore.ActorAuthorityStore
}

type authorityStoreWithoutPhysicalProjector struct {
	authoritystore.ActorAuthorityStore
}

func (s *authorityStoreWithoutPhysicalProjector) PhysicalCapabilities() (authoritystore.PhysicalCapabilities, error) {
	return s.ActorAuthorityStore.(authoritystore.PhysicalCapabilitiesProvider).PhysicalCapabilities()
}

func TestAuthorityParticipantFailsClosedWithoutConcreteStoreCapabilities(t *testing.T) {
	rig := ar1pMemoryRig(t, 9404)
	fence := rig.reassign(t, "player-holder-a", 2, 2)
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store:                  &authorityStoreWithoutPhysicalCapabilities{ActorAuthorityStore: rig.fault},
		Fence:                  authority.StaticFenceSource{Fence: fence},
		AuthorityLayoutVersion: "v1",
		BootID:                 "player-holder-a-boot-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	if err := registry.ParticipantAdmissionError(); err == nil {
		t.Fatal("Participant admission accepted an authority Store without physical capabilities")
	}
}

func TestAuthorityParticipantFailsClosedWithoutConcreteStoreProjector(t *testing.T) {
	trig := ar1pMemoryRig(t, 9405)
	fence := trig.reassign(t, "player-holder-a", 2, 2)
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store:                  &authorityStoreWithoutPhysicalProjector{ActorAuthorityStore: trig.fault},
		Fence:                  authority.StaticFenceSource{Fence: fence},
		AuthorityLayoutVersion: "v1",
		BootID:                 "player-holder-a-boot-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	if err := registry.ParticipantAdmissionError(); err == nil {
		t.Fatal("Participant admission accepted an authority Store without a physical commit projector")
	}
}
