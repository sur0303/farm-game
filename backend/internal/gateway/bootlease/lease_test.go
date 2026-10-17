package bootlease

import (
	"context"
	"errors"
	"testing"
	"time"
)

type failingStore struct{ err error }

func (s failingStore) Renew(context.Context, Fence, time.Duration) (Record, error) {
	return Record{}, s.err
}
func (s failingStore) Alive(context.Context, Fence) (bool, error) { return false, s.err }

func TestLeaseTrustAdvancesOnlyAfterSuccessfulRenew(t *testing.T) {
	store := NewMemoryStore()
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	lease, err := New(store, Fence{GatewayID: "gw-a", BootID: "boot-a"}, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Trusted(now) {
		t.Fatal("unrenewed boot must not be trusted")
	}
	if err := lease.Renew(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !lease.Trusted(now.Add(14*time.Second)) || lease.Trusted(now.Add(15*time.Second)) {
		t.Fatalf("unexpected trusted deadline %v", lease.TrustedUntil())
	}
	alive, err := store.Alive(context.Background(), lease.Fence())
	if err != nil || !alive {
		t.Fatalf("renewed fence should be alive: alive=%v err=%v", alive, err)
	}
}

func TestFailedRenewDoesNotInventTrust(t *testing.T) {
	want := errors.New("authority unavailable")
	lease, err := New(failingStore{err: want}, Fence{GatewayID: "gw-a", BootID: "boot-a"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Renew(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Renew error=%v want=%v", err, want)
	}
	if lease.Trusted(time.Now()) {
		t.Fatal("failed initial renew must remain untrusted")
	}
}

func TestMemoryStoreExpiresExactBootFence(t *testing.T) {
	store := NewMemoryStore()
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	a := Fence{GatewayID: "gw", BootID: "boot-a"}
	b := Fence{GatewayID: "gw", BootID: "boot-b"}
	if _, err := store.Renew(context.Background(), a, time.Second); err != nil {
		t.Fatal(err)
	}
	if alive, _ := store.Alive(context.Background(), b); alive {
		t.Fatal("a different boot fence must not be alive")
	}
	now = now.Add(time.Second)
	if alive, _ := store.Alive(context.Background(), a); alive {
		t.Fatal("expired boot fence must not be alive")
	}
}
