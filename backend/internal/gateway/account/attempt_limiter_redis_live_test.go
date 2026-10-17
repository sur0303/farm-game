package account

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// This test is opt-in because it writes short-lived isolated keys to the
// configured Redis. It never scans or flushes the database and deletes only
// keys derived from its random run prefix.
func TestRedisAttemptLimiterSharedAcrossGateInstancesLive(t *testing.T) {
	if os.Getenv("FARM_REDIS_LIVE_TEST") != "1" {
		t.Skip("set FARM_REDIS_LIVE_TEST=1 to run the isolated Redis limiter probe")
	}
	redisURL := os.Getenv("FARM_REDIS_URL")
	if redisURL == "" {
		t.Skip("FARM_REDIS_URL not set")
	}
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })

	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	cfg := AttemptLimiterConfig{
		Prefix:  "farm:test:web-login-limit:" + hex.EncodeToString(random[:]),
		HashKey: []byte("0123456789abcdef0123456789abcdef"),
		Account: LimitPolicy{Burst: 2, Window: 2 * time.Minute},
		IP:      LimitPolicy{Burst: 20, Window: time.Minute},
		Device:  LimitPolicy{Burst: 20, Window: time.Minute},
	}
	gateA, err := NewRedisAttemptLimiter(client, cfg)
	if err != nil {
		t.Fatal(err)
	}
	gateB, err := NewRedisAttemptLimiter(client, cfg)
	if err != nil {
		t.Fatal(err)
	}
	attempts := []LoginAttempt{
		{Username: "live_farmer", ClientIP: "192.0.2.1", DeviceID: "device-a"},
		{Username: "live_farmer", ClientIP: "192.0.2.2", DeviceID: "device-b"},
		{Username: "live_farmer", ClientIP: "192.0.2.3", DeviceID: "device-c"},
	}
	keys := make([]string, 0, len(attempts)*3)
	for _, attempt := range attempts {
		for _, dimension := range attemptDimensions(attempt, cfg) {
			keys = append(keys, attemptKey(cfg, dimension.name, dimension.identifier))
		}
	}
	t.Cleanup(func() {
		_ = client.Del(context.Background(), keys...).Err()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if decision, err := gateA.Allow(ctx, attempts[0]); err != nil || !decision.Allowed {
		t.Fatalf("Gate A first attempt = %+v, %v", decision, err)
	}
	if decision, err := gateB.Allow(ctx, attempts[1]); err != nil || !decision.Allowed {
		t.Fatalf("Gate B second attempt = %+v, %v", decision, err)
	}
	if decision, err := gateA.Allow(ctx, attempts[2]); err != nil || decision.Allowed || decision.RetryAfter <= 0 {
		t.Fatalf("shared account bucket did not reject = %+v, %v", decision, err)
	}
	if err := gateB.ResetAccount(ctx, attempts[0].Username); err != nil {
		t.Fatal(err)
	}
	if decision, err := gateA.Allow(ctx, attempts[2]); err != nil || !decision.Allowed {
		t.Fatalf("account reset did not propagate across Gates = %+v, %v", decision, err)
	}
}

