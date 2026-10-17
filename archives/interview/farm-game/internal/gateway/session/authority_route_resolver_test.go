package session

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"farmgame/internal/storage/redis/rediskey"
)

func TestBatchGetActiveRoutesMemoryDeduplicatesAndOmitsExpiredRoutes(t *testing.T) {
	ctx := context.Background()
	authority := New(Config{Secret: []byte("route-test-secret"), AuthorityEpoch: 1})
	if _, err := authority.Begin(ctx, 701, "gate-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := authority.Begin(ctx, 702, "gate-b"); err != nil {
		t.Fatal(err)
	}

	memory := authority.(*memoryAuthority)
	memory.mu.Lock()
	expired := memory.routes[702]
	expired.LeaseExpiresAt = time.Now().Add(-time.Second)
	memory.routes[702] = expired
	memory.mu.Unlock()

	resolver := authority.(SessionRouteResolver)
	got, err := resolver.BatchGetActiveRoutes(ctx, []uint64{701, 702, 701, 703})
	if err != nil {
		t.Fatalf("batch routes: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("routes=%v, want only one active route", got)
	}
	route, ok := got[701]
	if !ok || route.GatewayID != "gate-a" || route.ConnectionID == "" ||
		route.ConnectionGeneration == 0 || route.SessionEpoch == 0 || route.RouteRevision == 0 {
		t.Fatalf("valid route=%+v", route)
	}
	if _, ok := got[702]; ok {
		t.Fatal("expired route must be omitted")
	}

	ids, err := normalizeActiveRouteIDs([]uint64{9, 7, 9, 8})
	if err != nil || !reflect.DeepEqual(ids, []uint64{9, 7, 8}) {
		t.Fatalf("stable de-dup ids=%v err=%v", ids, err)
	}
	tooMany := make([]uint64, MaxBatchGetActiveRoutes+1)
	for i := range tooMany {
		tooMany[i] = uint64(i + 1)
	}
	if _, err := resolver.BatchGetActiveRoutes(ctx, tooMany); !errors.Is(err, ErrInvalidActiveRouteBatch) {
		t.Fatalf("too-large batch err=%v", err)
	}
}

func TestRedisBatchGetActiveRoutesValidatesFenceAndLease(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	authority := NewRedis(client, Config{Secret: []byte("route-test-secret"), AuthorityEpoch: 1})
	ctx := context.Background()
	now := time.Now()

	setRoute := func(playerID uint64, lease time.Time, generation string) {
		t.Helper()
		err := client.HSet(ctx, rediskey.Session(playerID), map[string]interface{}{
			"active_epoch":                "7",
			"session_id":                  "session-" + string(rune('a'+playerID-701)),
			"route_gate_id":               "gate-a",
			"route_gate_boot_id":          "boot-a",
			"route_connection_id":         "connection-" + string(rune('a'+playerID-701)),
			"route_connection_generation": generation,
			"route_revision":              "3",
			"route_lease_expire_at_ms":    lease.UnixMilli(),
		}).Err()
		if err != nil {
			t.Fatal(err)
		}
	}
	setRoute(701, now.Add(time.Minute), "11")
	setRoute(702, now.Add(-time.Minute), "12")
	setRoute(703, now.Add(time.Minute), "bad")

	got, err := authority.BatchGetActiveRoutes(ctx, []uint64{703, 701, 702, 701, 704})
	if err != nil {
		t.Fatalf("redis batch routes: %v", err)
	}
	if len(got) != 1 || got[701].PlayerID != 701 || got[701].ConnectionGeneration != 11 {
		t.Fatalf("redis routes=%+v", got)
	}
	if _, ok := got[702]; ok {
		t.Fatal("expired redis route must be omitted")
	}
	if _, ok := got[703]; ok {
		t.Fatal("malformed redis route must be omitted")
	}
}
