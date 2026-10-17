package actor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/activation"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/logic/actor/idle"
	"farmgame/internal/storage/authoritystore"
)

// blockingAuthorityLoadStore blocks only one farm before delegating to the
// real authority fake. This makes a slow authority access observable without
// moving the test into the Actor turn.
type blockingAuthorityLoadStore struct {
	*authoritystore.FaultStore
	blockFarmID uint64
	started     chan struct{}
	release     chan struct{}
	startOnce   sync.Once
	loadCalls   atomic.Int32
}

func (s *blockingAuthorityLoadStore) LoadAndClaim(ctx context.Context, req authoritystore.LoadAndClaimRequest) (authoritystore.LoadAndClaimResult, error) {
	s.loadCalls.Add(1)
	if req.ActorAddress.Domain == actorrouting.DomainFarm && req.ActorAddress.ActorID == s.blockFarmID {
		s.startOnce.Do(func() { close(s.started) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return authoritystore.LoadAndClaimResult{}, ctx.Err()
		}
	}
	return s.FaultStore.LoadAndClaim(ctx, req)
}

func TestParticipantAdmissionUsesActivatedSnapshotWithoutLoadAndClaim(t *testing.T) {
	const farmID = uint64(30_005)
	provider, blocking := newCoordinationTestProvider(t, farmID, 30_006)
	close(blocking.release)
	registry := NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	if _, err := registry.Submit(context.Background(), farmID, func(context.Context, *FarmActor) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	blocking.loadCalls.Store(0)
	record, version, err := registry.ReadOutsideMailbox(context.Background(), farmID)
	if err != nil || record == nil || version == 0 {
		t.Fatalf("activated admission snapshot=%v/%d err=%v", record, version, err)
	}
	if err := registry.CheckParticipantAuthorityCapacity(context.Background(), farmID, version, len("record")+1); err != nil {
		t.Fatalf("activated capacity snapshot err=%v", err)
	}
	if got := blocking.loadCalls.Load(); got != 0 {
		t.Fatalf("mailbox-free admission called LoadAndClaim %d times", got)
	}
}

func TestParticipantAdmissionDoesNotWaitBehindSameFarmAuthorityIO(t *testing.T) {
	const farmID = uint64(30_011)
	provider, blocking := newCoordinationTestProvider(t, farmID, 30_012)
	close(blocking.release)
	registry := NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	if _, err := registry.Submit(context.Background(), farmID, func(context.Context, *FarmActor) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	hold, err := registry.acquireAuthorityCoord(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, _, err = registry.ReadOutsideMailbox(context.Background(), farmID)
	elapsed := time.Since(start)
	hold()
	if !errors.Is(err, ErrParticipantAdmissionBusy) {
		t.Fatalf("mailbox-free admission err=%v, want nonblocking busy", err)
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("mailbox-free admission waited %s behind the farm gate", elapsed)
	}
}

func TestParticipantAdmissionColdFarmFailsClosedWithoutActivation(t *testing.T) {
	const farmID = uint64(30_007)
	provider, blocking := newCoordinationTestProvider(t, farmID, 30_008)
	close(blocking.release)
	registry := NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	_, _, err := registry.ReadOutsideMailbox(context.Background(), farmID)
	if err == nil || !errors.Is(err, ErrParticipantReceiptAuthorityUnavailable) {
		t.Fatalf("cold admission err=%v, want fail-closed unavailable", err)
	}
	if got := blocking.loadCalls.Load(); got != 0 {
		t.Fatalf("cold mailbox-free admission activated through LoadAndClaim %d times", got)
	}
	if got := registry.Activation().StateOf(farmID); got != activation.Absent {
		t.Fatalf("cold admission changed activation to %s", got)
	}
}

func TestParticipantAdmissionBlocksIdlePassivationOnSameFarm(t *testing.T) {
	const farmID = uint64(30_009)
	provider, blocking := newCoordinationTestProvider(t, farmID, 30_010)
	close(blocking.release)
	registry := NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	if _, err := registry.Submit(context.Background(), farmID, func(context.Context, *FarmActor) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	registry.passivation.IdleTimeout = time.Nanosecond
	registry.passivation.Now = func() time.Time { return time.Now().Add(time.Hour) }
	release, err := registry.acquireParticipantAdmissionCoord(context.Background(), farmID)
	if err != nil {
		t.Fatal(err)
	}
	result := registry.tryPassivateIdle(context.Background(), farmID)
	release()
	if result.Decision != idle.DecisionBusy || !containsString(result.Reasons, BlockerParticipantAdmission) {
		t.Fatalf("passivation result=%+v, want participant admission blocker", result)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func newCoordinationTestProvider(t *testing.T, blockedFarmID, otherFarmID uint64) (*authority.Provider, *blockingAuthorityLoadStore) {
	t.Helper()
	calculator := ar1Calculator(t)
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver:               calculator,
		AuthorityLayoutVersion: "v1",
		Now:                    func() time.Time { return ar1Now },
	})
	if err != nil {
		t.Fatal(err)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID:           "coord-holder",
		ExpectedHolderBootID:       "coord-holder-boot",
		ExpectedAssignmentRevision: 1,
		ExpectedWriterEpoch:        1,
		ExpectedLeaseEpoch:         1,
	}
	for _, farmID := range []uint64{blockedFarmID, otherFarmID} {
		shard, err := calculator.ShardFor(actorrouting.DomainFarm, farmID)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.SetShardWriteAuthority(authoritystore.ShardWriteAuthority{
			AuthorityLayoutVersion: "v1",
			Domain:                 actorrouting.DomainFarm,
			VirtualShard:           shard,
			HolderID:               fence.ExpectedHolderID,
			HolderBootID:           fence.ExpectedHolderBootID,
			AssignmentRevision:     fence.ExpectedAssignmentRevision,
			WriterEpoch:            fence.ExpectedWriterEpoch,
			LeaseEpoch:             fence.ExpectedLeaseEpoch,
			State:                  authoritystore.ShardAuthorityActive,
			LeaseUntilMs:           ar1Now.Add(time.Minute).UnixMilli(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	base := authoritystore.NewFaultStore(store)
	blocking := &blockingAuthorityLoadStore{
		FaultStore:  base,
		blockFarmID: blockedFarmID,
		started:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store:                  blocking,
		Fence:                  authority.StaticFenceSource{Fence: fence},
		AuthorityLayoutVersion: "v1",
		BootID:                 "coord-holder-boot",
	})
	if err != nil {
		t.Fatal(err)
	}
	return provider, blocking
}

func TestAuthorityCoordinationDifferentFarmsDoNotShareSlowAuthorityGate(t *testing.T) {
	const blockedFarmID = 30_001
	const otherFarmID = 30_002
	provider, blocking := newCoordinationTestProvider(t, blockedFarmID, otherFarmID)
	registry := NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)

	aDone := make(chan error, 1)
	go func() {
		_, err := registry.Submit(context.Background(), blockedFarmID, func(context.Context, *FarmActor) (any, error) {
			return nil, nil
		})
		aDone <- err
	}()
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("farm A authority access did not block")
	}

	// This is a Participant mailbox turn for farm B. A Registry-wide authority
	// mutex would leave it waiting behind farm A's blocked LoadAndClaim.
	bDone := make(chan error, 1)
	go func() {
		_, err := registry.SubmitParticipant(context.Background(), otherFarmID, func(_ context.Context, actor *FarmActor, _ ParticipantCapacityGuard) (any, error) {
			return actor.Snapshot(ar1Now), nil
		})
		bDone <- err
	}()
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatalf("farm B participant turn failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("farm B participant turn was blocked by farm A authority I/O")
	}

	close(blocking.release)
	select {
	case err := <-aDone:
		if err != nil {
			t.Fatalf("farm A turn failed after authority release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("farm A turn did not finish after authority release")
	}
}

func TestAuthorityCoordinationSerializesSameFarmAndReclaimsGate(t *testing.T) {
	registry := newRegistryShared(RegistryOptions{MailboxSize: 2})
	t.Cleanup(registry.Stop)

	releaseFirst, err := registry.acquireAuthorityCoord(context.Background(), 30_003)
	if err != nil {
		t.Fatal(err)
	}
	acquiredSecond := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		releaseSecond, err := registry.acquireAuthorityCoord(context.Background(), 30_003)
		if err != nil {
			secondDone <- err
			return
		}
		close(acquiredSecond)
		releaseSecond()
		secondDone <- nil
	}()

	select {
	case <-acquiredSecond:
		t.Fatal("same-farm authority gate admitted a concurrent operation")
	case <-time.After(25 * time.Millisecond):
	}
	releaseFirst()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("same-farm authority operation did not proceed after release")
	}

	registry.authorityCoordMu.Lock()
	deferred := len(registry.authorityCoords)
	registry.authorityCoordMu.Unlock()
	if deferred != 0 {
		t.Fatalf("authority coordination entries leaked: %d", deferred)
	}
}

func TestAuthorityCoordinationContextCancellationReclaimsWaiter(t *testing.T) {
	registry := newRegistryShared(RegistryOptions{MailboxSize: 2})
	t.Cleanup(registry.Stop)

	release, err := registry.acquireAuthorityCoord(context.Background(), 30_004)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := registry.acquireAuthorityCoord(ctx, 30_004); err == nil {
		t.Fatal("expected same-farm waiter cancellation")
	}
	release()

	registry.authorityCoordMu.Lock()
	deferred := len(registry.authorityCoords)
	registry.authorityCoordMu.Unlock()
	if deferred != 0 {
		t.Fatalf("cancelled coordination waiter leaked entry: %d", deferred)
	}
}
