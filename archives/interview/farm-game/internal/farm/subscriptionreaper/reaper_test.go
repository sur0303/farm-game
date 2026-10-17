package subscriptionreaper

import (
	"context"
	"sync"
	"testing"
	"time"

	farmactor "farmgame/internal/farm/actor"
	"farmgame/internal/gateway/bootlease"
	"farmgame/internal/storage/blobstore"
)

type fakeLeaseStore struct {
	mu    sync.Mutex
	alive map[bootlease.Fence]bool
}

func (s *fakeLeaseStore) Renew(_ context.Context, fence bootlease.Fence, ttl time.Duration) (bootlease.Record, error) {
	s.mu.Lock()
	s.alive[fence] = true
	s.mu.Unlock()
	return bootlease.Record{Fence: fence, ExpiresAt: time.Now().Add(ttl)}, nil
}
func (s *fakeLeaseStore) Alive(_ context.Context, fence bootlease.Fence) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.alive[fence], nil
}

func TestSweepChecksOnlyHubBootsAndReapsExactDeadFence(t *testing.T) {
	registry := farmactor.NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	join := func(farmID, playerID uint64, boot string) {
		_, err := registry.Submit(context.Background(), farmID, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
			return actor.Join(farmactor.JoinRequest{
				OperationID: "join-" + boot, SubscriptionID: "sub-" + boot, ViewerPlayerID: playerID,
				SessionEpoch: 1, GatewayID: "gw", GatewayBootID: boot, ConnectionID: "conn-" + boot,
				TransportGeneration: 1, RouteRevision: 1,
			}, time.Now())
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	join(10, 100, "alive")
	join(20, 200, "dead")

	store := &fakeLeaseStore{alive: map[bootlease.Fence]bool{{GatewayID: "gw", BootID: "alive"}: true}}
	reaper, err := New(registry.SubscriptionHub(), store, time.Second, time.Second, ModeEnabled)
	if err != nil {
		t.Fatal(err)
	}
	if err := reaper.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := reaper.Stats().BootsChecked.Load(); got != 2 {
		t.Fatalf("boots checked=%d, want only the 2 local Hub boots", got)
	}
	if count, _ := registry.SubscriptionHub().PresenceCount(context.Background(), 10); count != 1 {
		t.Fatalf("alive boot presence count=%d, want 1", count)
	}
	if count, _ := registry.SubscriptionHub().PresenceCount(context.Background(), 20); count != 0 {
		t.Fatalf("dead boot presence count=%d, want 0", count)
	}
}

func TestShadowModeCountsWouldReapWithoutRemoving(t *testing.T) {
	registry := farmactor.NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	_, err := registry.Submit(context.Background(), 20, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		return actor.Join(farmactor.JoinRequest{
			OperationID: "join-dead", SubscriptionID: "sub-dead", ViewerPlayerID: 200,
			SessionEpoch: 1, GatewayID: "gw", GatewayBootID: "dead", ConnectionID: "conn-dead",
			TransportGeneration: 1, RouteRevision: 1,
		}, time.Now())
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeLeaseStore{alive: map[bootlease.Fence]bool{}}
	reaper, err := New(registry.SubscriptionHub(), store, time.Second, time.Second, ModeShadow)
	if err != nil {
		t.Fatal(err)
	}
	if err := reaper.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := reaper.Stats().DeadBoots.Load(); got != 1 {
		t.Fatalf("dead boots=%d, want 1", got)
	}
	if got := reaper.Stats().WouldReap.Load(); got != 1 {
		t.Fatalf("would reap=%d, want 1", got)
	}
	if got := reaper.Stats().Removed.Load(); got != 0 {
		t.Fatalf("shadow mode removed=%d, want 0", got)
	}
	if count, _ := registry.SubscriptionHub().PresenceCount(context.Background(), 20); count != 1 {
		t.Fatalf("shadow mode must not remove presence, count=%d", count)
	}
}

func TestDisabledModeSkipsGateBootChecks(t *testing.T) {
	registry := farmactor.NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	_, err := registry.Submit(context.Background(), 20, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		return actor.Join(farmactor.JoinRequest{
			OperationID: "join-dead", SubscriptionID: "sub-dead", ViewerPlayerID: 200,
			SessionEpoch: 1, GatewayID: "gw", GatewayBootID: "dead", ConnectionID: "conn-dead",
			TransportGeneration: 1, RouteRevision: 1,
		}, time.Now())
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeLeaseStore{alive: map[bootlease.Fence]bool{}}
	reaper, err := New(registry.SubscriptionHub(), store, time.Second, time.Second, ModeDisabled)
	if err != nil {
		t.Fatal(err)
	}
	if err := reaper.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := reaper.Stats().BootsChecked.Load(); got != 0 {
		t.Fatalf("disabled mode boots checked=%d, want 0", got)
	}
	if count, _ := registry.SubscriptionHub().PresenceCount(context.Background(), 20); count != 1 {
		t.Fatalf("disabled mode must not reap, count=%d", count)
	}
}

func TestNewRejectsInvalidMode(t *testing.T) {
	registry := farmactor.NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	store := &fakeLeaseStore{alive: map[bootlease.Fence]bool{}}
	if _, err := New(registry.SubscriptionHub(), store, time.Second, time.Second, Mode("bogus")); err == nil {
		t.Fatal("invalid mode must be rejected")
	}
}
