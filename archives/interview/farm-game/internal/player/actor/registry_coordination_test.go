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
	"farmgame/internal/storage/authoritystore"
)

type countingPlayerLoadStore struct {
	authoritystore.ActorAuthorityStore
	loads atomic.Uint64
}

func (s *countingPlayerLoadStore) LoadAndClaim(ctx context.Context, req authoritystore.LoadAndClaimRequest) (authoritystore.LoadAndClaimResult, error) {
	s.loads.Add(1)
	return s.ActorAuthorityStore.LoadAndClaim(ctx, req)
}

func (s *countingPlayerLoadStore) PhysicalCapabilities() (authoritystore.PhysicalCapabilities, error) {
	return s.ActorAuthorityStore.(authoritystore.PhysicalCapabilitiesProvider).PhysicalCapabilities()
}

func (s *countingPlayerLoadStore) ProjectPhysicalCommit(req authoritystore.AuthorityCommitRequest) (authoritystore.PhysicalCommitUsage, error) {
	return s.ActorAuthorityStore.(authoritystore.PhysicalCommitUsageProjector).ProjectPhysicalCommit(req)
}

func (s *countingPlayerLoadStore) AdmissionSnapshotReuseSafe() bool { return true }

type blockingPlayerLoadStore struct {
	*authoritystore.FaultStore
	blockPlayerID uint64
	started       chan struct{}
	release       chan struct{}
	startOnce     sync.Once
}

func (s *blockingPlayerLoadStore) LoadAndClaim(ctx context.Context, req authoritystore.LoadAndClaimRequest) (authoritystore.LoadAndClaimResult, error) {
	if req.ActorAddress.Domain == actorrouting.DomainPlayer && req.ActorAddress.ActorID == s.blockPlayerID {
		s.startOnce.Do(func() { close(s.started) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return authoritystore.LoadAndClaimResult{}, ctx.Err()
		}
	}
	return s.FaultStore.LoadAndClaim(ctx, req)
}

func TestAuthorityHotWritesReuseCurrentBindingSnapshot(t *testing.T) {
	const playerID = uint64(40_000)
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
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID: "player-hot-holder", ExpectedHolderBootID: "player-hot-holder-boot",
		ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
	}
	if err := store.SetShardWriteAuthority(authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer, VirtualShard: shard,
		HolderID: fence.ExpectedHolderID, HolderBootID: fence.ExpectedHolderBootID,
		AssignmentRevision: fence.ExpectedAssignmentRevision, WriterEpoch: fence.ExpectedWriterEpoch,
		LeaseEpoch: fence.ExpectedLeaseEpoch, State: authoritystore.ShardAuthorityActive,
		LeaseUntilMs: ar1pNow.Add(time.Minute).UnixMilli(),
	}); err != nil {
		t.Fatal(err)
	}
	counting := &countingPlayerLoadStore{ActorAuthorityStore: store}
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store: counting, Fence: authority.StaticFenceSource{Fence: fence},
		AuthorityLayoutVersion: "v1", BootID: fence.ExpectedHolderBootID,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	if _, _, err := registry.Ensure(context.Background(), playerID, "hot-bootstrap"); err != nil {
		t.Fatal(err)
	}
	before := counting.loads.Load()
	for i := 0; i < 2; i++ {
		if _, err := registry.Submit(context.Background(), playerID, ar1pBuyTurn); err != nil {
			t.Fatal(err)
		}
	}
	if got := counting.loads.Load(); got != before {
		t.Fatalf("hot writes issued LoadAndClaim calls: before=%d after=%d", before, got)
	}
	if _, _, err := registry.Read(context.Background(), playerID); err != nil {
		t.Fatal(err)
	}
	if got := counting.loads.Load(); got != before+1 {
		t.Fatalf("writer-fenced public read loads=%d, want %d", got, before+1)
	}
	beforeRoutedRead := counting.loads.Load()
	if _, _, err := registry.ReadRoutedCurrent(context.Background(), playerID); err != nil {
		t.Fatal(err)
	}
	if got := counting.loads.Load(); got != beforeRoutedRead {
		t.Fatalf("routed hot read reloaded: before=%d after=%d", beforeRoutedRead, got)
	}

	beforeAdmissionRead := counting.loads.Load()
	_, admissionVersion, err := registry.ReadOutsideMailbox(context.Background(), playerID)
	if err != nil {
		t.Fatal(err)
	}
	if got := counting.loads.Load(); got != beforeAdmissionRead {
		t.Fatalf("hot outside-mailbox admission reloaded: before=%d after=%d", beforeAdmissionRead, got)
	}
	beforeParticipant := counting.loads.Load()
	if _, err := registry.SubmitParticipantVersionedFromAdmission(context.Background(), playerID, admissionVersion,
		func(rec *playerpb.PlayerRecord, _ uint64, _ ParticipantCapacityGuard) (any, bool, error) {
			rec.Wallet.Coin--
			return nil, true, nil
		}); err != nil {
		t.Fatal(err)
	}
	if got := counting.loads.Load(); got != beforeParticipant {
		t.Fatalf("matching admission version reloaded: before=%d after=%d", beforeParticipant, got)
	}

	_, staleAdmissionVersion, err := registry.ReadOutsideMailbox(context.Background(), playerID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Submit(context.Background(), playerID, ar1pBuyTurn); err != nil {
		t.Fatal(err)
	}
	beforeStale := counting.loads.Load()
	businessErr := errors.New("stop after stale admission refresh")
	if _, err := registry.SubmitParticipantVersionedFromAdmission(context.Background(), playerID, staleAdmissionVersion,
		func(*playerpb.PlayerRecord, uint64, ParticipantCapacityGuard) (any, bool, error) {
			return nil, false, businessErr
		}); !errors.Is(err, businessErr) {
		t.Fatalf("stale admission turn err=%v", err)
	}
	if got := counting.loads.Load(); got != beforeStale+1 {
		t.Fatalf("stale admission loads=%d, want %d", got, beforeStale+1)
	}
}

func TestAuthorityCoordinationDifferentPlayersDoNotShareSlowAuthorityGate(t *testing.T) {
	const blockedPlayerID = uint64(40_001)
	const otherPlayerID = uint64(40_002)
	calculator := ar1pCalculator(t)
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver:               calculator,
		AuthorityLayoutVersion: "v1",
		Now:                    func() time.Time { return ar1pNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID:           "player-coord-holder",
		ExpectedHolderBootID:       "player-coord-holder-boot",
		ExpectedAssignmentRevision: 1,
		ExpectedWriterEpoch:        1,
		ExpectedLeaseEpoch:         1,
	}
	for _, playerID := range []uint64{blockedPlayerID, otherPlayerID} {
		shard, err := calculator.ShardFor(actorrouting.DomainPlayer, playerID)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.SetShardWriteAuthority(authoritystore.ShardWriteAuthority{
			AuthorityLayoutVersion: "v1",
			Domain:                 actorrouting.DomainPlayer,
			VirtualShard:           shard,
			HolderID:               fence.ExpectedHolderID,
			HolderBootID:           fence.ExpectedHolderBootID,
			AssignmentRevision:     fence.ExpectedAssignmentRevision,
			WriterEpoch:            fence.ExpectedWriterEpoch,
			LeaseEpoch:             fence.ExpectedLeaseEpoch,
			State:                  authoritystore.ShardAuthorityActive,
			LeaseUntilMs:           ar1pNow.Add(time.Minute).UnixMilli(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	blocking := &blockingPlayerLoadStore{
		FaultStore:    authoritystore.NewFaultStore(store),
		blockPlayerID: blockedPlayerID,
		started:       make(chan struct{}),
		release:       make(chan struct{}),
	}
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store:                  blocking,
		Fence:                  authority.StaticFenceSource{Fence: fence},
		AuthorityLayoutVersion: "v1",
		BootID:                 fence.ExpectedHolderBootID,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)

	aDone := make(chan error, 1)
	go func() {
		_, _, err := registry.EnsureWithProfile(context.Background(), blockedPlayerID, "boot-a", "", 0)
		aDone <- err
	}()
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("player A authority access did not block")
	}

	bDone := make(chan error, 1)
	go func() {
		_, _, err := registry.EnsureWithProfile(context.Background(), otherPlayerID, "boot-b", "", 0)
		bDone <- err
	}()
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("player B bootstrap failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("player B bootstrap was blocked by player A authority I/O")
	}

	close(blocking.release)
	select {
	case err := <-aDone:
		if err != nil {
			t.Fatalf("player A bootstrap failed after authority release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("player A bootstrap did not finish after authority release")
	}
}
