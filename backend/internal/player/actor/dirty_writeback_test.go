package actor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	economypb "farmgame/api/gen/economy"
	playerpb "farmgame/api/gen/player"
	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/logic/actor/dirtywriteback"
	actorruntime "farmgame/internal/logic/actor/runtime"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/protobuf/proto"
)

type blockingPlayerCommitStore struct {
	*authoritystore.MemoryStore
	block   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingPlayerCommitStore) Commit(ctx context.Context, req authoritystore.AuthorityCommitRequest) (authoritystore.CommitResult, error) {
	if s.block.CompareAndSwap(true, false) {
		s.once.Do(func() { close(s.entered) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return authoritystore.CommitResult{}, ctx.Err()
		}
	}
	return s.MemoryStore.Commit(ctx, req)
}

func newSoftPlayerRegistry(t *testing.T, playerID uint64, blockCommit bool, gate actorruntime.AdmissionGate) (*Registry, authoritystore.ModelFieldBatchReader, *blockingPlayerCommitStore) {
	t.Helper()
	calculator := ar1pCalculator(t)
	memory, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver: calculator, AuthorityLayoutVersion: "v1",
		Now: func() time.Time { return ar1pNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &blockingPlayerCommitStore{
		MemoryStore: memory, entered: make(chan struct{}), release: make(chan struct{}),
	}
	store.block.Store(blockCommit)
	shard, err := calculator.ShardFor(actorrouting.DomainPlayer, playerID)
	if err != nil {
		t.Fatal(err)
	}
	writeAuthority := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer, VirtualShard: shard,
		HolderID: "player-soft-holder", HolderBootID: "player-soft-boot",
		AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1,
		State: authoritystore.ShardAuthorityActive, LeaseUntilMs: ar1pNow.Add(time.Minute).UnixMilli(),
	}
	if err := store.SetShardWriteAuthority(writeAuthority); err != nil {
		t.Fatal(err)
	}
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store: store,
		Fence: authority.StaticFenceSource{Fence: authoritystore.ShardWriteFence{
			ExpectedHolderID: "player-soft-holder", ExpectedHolderBootID: "player-soft-boot",
			ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
		}},
		AuthorityLayoutVersion: "v1", BootID: "player-soft-boot", EnableSoftSave: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewAuthorityRegistry(provider, RegistryOptions{
		MailboxSize:   8,
		Idle:          IdleOptions{Disable: true},
		AdmissionGate: gate,
		DirtyWriteback: dirtywriteback.Options{
			FlushInterval: time.Hour, MaxDirtyAge: 2 * time.Hour,
			RetryDelay: time.Second, FlushTimeout: time.Second,
			Workers: 1, QueueSize: 16,
		},
	})
	t.Cleanup(registry.Stop)
	return registry, memory, store
}

func TestSoftPlayerRegistrySkipsDetachedParticipantCapacityProjection(t *testing.T) {
	registry, _, _ := newSoftPlayerRegistry(t, 97_001, false, nil)
	if registry.ParticipantCapacityProjectionRequired() {
		t.Fatal("soft dirty writeback should not require detached participant capacity projection")
	}
	guard := ParticipantCapacityGuard{bestEffort: true}
	if guard.PhysicalProjectionRequired() {
		t.Fatal("best-effort participant guard unexpectedly requires a physical projection")
	}
}

func readPersistedPlayer(t *testing.T, reader authoritystore.ModelFieldBatchReader, playerID uint64) (*playerpb.PlayerRecord, uint64) {
	t.Helper()
	rows, err := reader.BatchReadModelFields(context.Background(), actorrouting.DomainPlayer,
		[]uint64{playerID}, []string{authority.RecordModelField})
	if err != nil || len(rows) != 1 || rows[0].Err != nil || !rows[0].Found {
		t.Fatalf("persisted row=%+v err=%v", rows, err)
	}
	record := &playerpb.PlayerRecord{}
	if err := proto.Unmarshal(rows[0].ModelFields[authority.RecordModelField], record); err != nil {
		t.Fatal(err)
	}
	return record, rows[0].RecordVersion
}

func addPlayerCoinResult(registry *Registry, playerID uint64, delta int64) (int64, error) {
	result, err := registry.SubmitVersioned(context.Background(), playerID, func(record *playerpb.PlayerRecord, _ uint64) (any, bool, error) {
		record.Wallet.Coin += delta
		return record.Wallet.Coin, true, nil
	})
	if err != nil {
		return 0, err
	}
	return result.Value.(int64), nil
}

func addPlayerCoin(t *testing.T, registry *Registry, playerID uint64, delta int64) int64 {
	t.Helper()
	coin, err := addPlayerCoinResult(registry, playerID, delta)
	if err != nil {
		t.Fatal(err)
	}
	return coin
}

func TestPlayerDirtyWritebackCoalescesWithoutReloadingPersistedState(t *testing.T) {
	const playerID = uint64(98_101)
	registry, reader, _ := newSoftPlayerRegistry(t, playerID, false, nil)
	initial, _, err := registry.Ensure(context.Background(), playerID, "soft-player-bootstrap")
	if err != nil {
		t.Fatal(err)
	}
	want := initial.GetWallet().GetCoin() + 2
	addPlayerCoin(t, registry, playerID, 1)
	if got := addPlayerCoin(t, registry, playerID, 1); got != want {
		t.Fatalf("second in-memory turn lost the first mutation: got=%d want=%d", got, want)
	}
	before, physicalVersion := readPersistedPlayer(t, reader, playerID)
	if before.GetWallet().GetCoin() != initial.GetWallet().GetCoin() || physicalVersion != 1 {
		t.Fatalf("Redis changed before flush: coin=%d physicalVersion=%d", before.GetWallet().GetCoin(), physicalVersion)
	}
	stats := registry.DirtyWritebackStats()
	if stats.QueueDepth != 1 || stats.CoalescedMarks < 1 {
		t.Fatalf("dirty queue did not coalesce: %+v", stats)
	}
	if err := registry.writeback.FlushNow(context.Background(), playerID); err != nil {
		t.Fatal(err)
	}
	after, physicalVersion := readPersistedPlayer(t, reader, playerID)
	if after.GetWallet().GetCoin() != want || physicalVersion != 2 || after.GetActorMemoryVersion() != 3 {
		t.Fatalf("flushed player coin=%d actorMemoryVersion=%d physicalVersion=%d wantCoin=%d",
			after.GetWallet().GetCoin(), after.GetActorMemoryVersion(), physicalVersion, want)
	}
}

func TestPlayerResidentRecordIsolatedAcrossTurnsAndRetirement(t *testing.T) {
	const playerID = uint64(98_111)
	registry, _, _ := newSoftPlayerRegistry(t, playerID, false, nil)
	initial, _, err := registry.Ensure(context.Background(), playerID, "resident-player")
	if err != nil {
		t.Fatal(err)
	}
	want := initial.GetWallet().GetCoin() + 1
	if got := addPlayerCoin(t, registry, playerID, 1); got != want {
		t.Fatalf("coin=%d want=%d", got, want)
	}
	if stats := registry.ResidentRecordStats(); stats.Hits == 0 {
		t.Fatalf("resident cache was not used: %+v", stats)
	}

	turnErr := errors.New("reject after accidental mutation")
	if _, err := registry.SubmitVersioned(context.Background(), playerID, func(record *playerpb.PlayerRecord, _ uint64) (any, bool, error) {
		record.Wallet.Coin += 10_000
		return nil, false, turnErr
	}); !errors.Is(err, turnErr) {
		t.Fatalf("turn err=%v", err)
	}
	firstRead, version, err := registry.ReadRoutedCurrent(context.Background(), playerID)
	if err != nil || firstRead.GetWallet().GetCoin() != want {
		t.Fatalf("failed turn leaked: coin=%d version=%d err=%v", firstRead.GetWallet().GetCoin(), version, err)
	}
	firstRead.Wallet.Coin = -1
	secondRead, _, err := registry.ReadRoutedCurrent(context.Background(), playerID)
	if err != nil || secondRead.GetWallet().GetCoin() != want {
		t.Fatalf("returned read aliased resident state: coin=%d err=%v", secondRead.GetWallet().GetCoin(), err)
	}

	registry.bindingsMu.Lock()
	binding := registry.bindings[playerID]
	registry.bindingsMu.Unlock()
	if binding == nil || !registry.retireBindingIfCurrent(playerID, binding) {
		t.Fatal("failed to retire resident binding")
	}
	if _, ok := registry.residentRecords.Load(playerID); ok {
		t.Fatal("retired binding retained decoded resident state")
	}
}

func TestPlayerResidentRecordDetachesSuccessfulProtoResult(t *testing.T) {
	const playerID = uint64(98_112)
	registry, _, _ := newSoftPlayerRegistry(t, playerID, false, nil)
	initial, _, err := registry.Ensure(context.Background(), playerID, "resident-result-player")
	if err != nil {
		t.Fatal(err)
	}
	want := initial.GetWallet().GetCoin() + 1
	result, err := registry.SubmitVersioned(context.Background(), playerID, func(record *playerpb.PlayerRecord, _ uint64) (any, bool, error) {
		record.Wallet.Coin++
		return record.Wallet, true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	wallet, ok := result.Value.(*economypb.Wallet)
	if !ok || wallet == nil || wallet.GetCoin() != want {
		t.Fatalf("result wallet=%T/%v wantCoin=%d", result.Value, wallet, want)
	}
	wallet.Coin = -1
	read, _, err := registry.ReadRoutedCurrent(context.Background(), playerID)
	if err != nil || read.GetWallet().GetCoin() != want {
		t.Fatalf("successful result aliased resident state: coin=%d err=%v", read.GetWallet().GetCoin(), err)
	}
}

func TestPlayerDirtyWritebackQueueFullFlushFailureRemainsTracked(t *testing.T) {
	const playerID = uint64(98_113)
	registry, _, store := newSoftPlayerRegistry(t, playerID, false, nil)
	initial, _, err := registry.Ensure(context.Background(), playerID, "queue-full-player")
	if err != nil {
		t.Fatal(err)
	}
	// Fill the ordinary unique-actor queue without creating bindings for the
	// placeholder IDs. The tested player's post-stage Mark must therefore take
	// the emergency FlushNow path.
	for index := 0; index < 16; index++ {
		if err := registry.writeback.Mark(uint64(200_000 + index)); err != nil {
			t.Fatal(err)
		}
	}
	store.block.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = registry.SubmitVersioned(ctx, playerID, func(record *playerpb.PlayerRecord, _ uint64) (any, bool, error) {
		record.Wallet.Coin++
		return record.Wallet.Coin, true, nil
	})
	if err == nil {
		t.Fatal("expected emergency flush failure")
	}
	if stats := registry.DirtyWritebackStats(); stats.QueueDepth != 17 {
		t.Fatalf("staged actor was not retained for retry: %+v", stats)
	}
	read, _, readErr := registry.ReadRoutedCurrent(context.Background(), playerID)
	if readErr != nil || read.GetWallet().GetCoin() != initial.GetWallet().GetCoin()+1 {
		t.Fatalf("staged state missing after tracked flush failure: coin=%d err=%v", read.GetWallet().GetCoin(), readErr)
	}
}

func TestPlayerRedisFlushDoesNotBlockMailboxAndKeepsTrailingTurn(t *testing.T) {
	const playerID = uint64(98_102)
	registry, reader, store := newSoftPlayerRegistry(t, playerID, false, nil)
	initial, _, err := registry.Ensure(context.Background(), playerID, "soft-player-mailbox")
	if err != nil {
		t.Fatal(err)
	}
	addPlayerCoin(t, registry, playerID, 1)
	store.block.Store(true)
	flushDone := make(chan error, 1)
	go func() { flushDone <- registry.writeback.FlushNow(context.Background(), playerID) }()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("flush never reached the blocking Store")
	}

	type turnResult struct {
		coin int64
		err  error
	}
	turnDone := make(chan turnResult, 1)
	go func() {
		coin, err := addPlayerCoinResult(registry, playerID, 1)
		turnDone <- turnResult{coin: coin, err: err}
	}()
	select {
	case result := <-turnDone:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if want := initial.GetWallet().GetCoin() + 2; result.coin != want {
			t.Fatalf("trailing turn coin=%d want=%d", result.coin, want)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Actor mailbox was blocked by external Redis flush")
	}
	close(store.release)
	if err := <-flushDone; err != nil {
		t.Fatal(err)
	}
	if err := registry.writeback.FlushNow(context.Background(), playerID); err != nil {
		t.Fatal(err)
	}
	after, _ := readPersistedPlayer(t, reader, playerID)
	if want := initial.GetWallet().GetCoin() + 2; after.GetWallet().GetCoin() != want {
		t.Fatalf("trailing turn was not persisted: coin=%d want=%d", after.GetWallet().GetCoin(), want)
	}
}

func TestPlayerShardDrainFlushesDirtyStateBeforeEviction(t *testing.T) {
	const playerID = uint64(98_103)
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
	registry, reader, _ := newSoftPlayerRegistry(t, playerID, false, gate)
	initial, _, err := registry.Ensure(context.Background(), playerID, "soft-player-drain")
	if err != nil {
		t.Fatal(err)
	}
	addPlayerCoin(t, registry, playerID, 7)
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
	if err := barrier.ConfirmFactsCommitted(context.Background(), shard); err != nil {
		t.Fatal(err)
	}
	after, _ := readPersistedPlayer(t, reader, playerID)
	if want := initial.GetWallet().GetCoin() + 7; after.GetWallet().GetCoin() != want {
		t.Fatalf("drain persisted coin=%d want=%d", after.GetWallet().GetCoin(), want)
	}
	registry.bindingsMu.Lock()
	remaining := len(registry.bindings)
	registry.bindingsMu.Unlock()
	if remaining != 0 {
		t.Fatalf("drain retained %d player bindings", remaining)
	}
}
