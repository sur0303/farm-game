package account

import (
	"context"
	"strings"
	"testing"
	"time"
)

func testAttemptLimiterConfig() AttemptLimiterConfig {
	return AttemptLimiterConfig{
		Prefix:  "farm:test:login-limit",
		HashKey: []byte("0123456789abcdef0123456789abcdef"),
		Account: LimitPolicy{Burst: 2, Window: 2 * time.Minute},
		IP:      LimitPolicy{Burst: 3, Window: 3 * time.Minute},
		Device:  LimitPolicy{Burst: 4, Window: 4 * time.Minute},
	}
}

func TestMemoryAttemptLimiterEnforcesIndependentDimensions(t *testing.T) {
	limiter, err := NewMemoryAttemptLimiter(testAttemptLimiterConfig())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_000, 0)
	limiter.now = func() time.Time { return now }
	ctx := context.Background()

	attempt := LoginAttempt{Username: "Farmer_One", ClientIP: "192.0.2.1", DeviceID: "browser-a"}
	for index := 0; index < 2; index++ {
		decision, err := limiter.Allow(ctx, attempt)
		if err != nil || !decision.Allowed {
			t.Fatalf("account attempt %d = %+v, %v", index, decision, err)
		}
	}
	decision, err := limiter.Allow(ctx, LoginAttempt{
		Username: "farmer_one", ClientIP: "192.0.2.2", DeviceID: "browser-b",
	})
	if err != nil || decision.Allowed || decision.RetryAfter != time.Minute {
		t.Fatalf("account dimension did not limit globally: %+v, %v", decision, err)
	}

	if err := limiter.ResetAccount(ctx, " FARMER_ONE "); err != nil {
		t.Fatal(err)
	}
	decision, err = limiter.Allow(ctx, LoginAttempt{
		Username: "farmer_one", ClientIP: "192.0.2.3", DeviceID: "browser-c",
	})
	if err != nil || !decision.Allowed {
		t.Fatalf("account reset did not admit: %+v, %v", decision, err)
	}

	for _, username := range []string{"other_one", "other_two"} {
		decision, err = limiter.Allow(ctx, LoginAttempt{
			Username: username, ClientIP: "198.51.100.1", DeviceID: username,
		})
		if err != nil || !decision.Allowed {
			t.Fatalf("IP setup attempt = %+v, %v", decision, err)
		}
	}
	decision, err = limiter.Allow(ctx, LoginAttempt{
		Username: "other_three", ClientIP: "198.51.100.1", DeviceID: "other-three",
	})
	if err != nil || !decision.Allowed {
		t.Fatalf("third IP attempt = %+v, %v", decision, err)
	}
	decision, err = limiter.Allow(ctx, LoginAttempt{
		Username: "other_four", ClientIP: "198.51.100.1", DeviceID: "other-four",
	})
	if err != nil || decision.Allowed || decision.RetryAfter != time.Minute {
		t.Fatalf("IP dimension did not limit: %+v, %v", decision, err)
	}
}

func TestMemoryAttemptLimiterRefillsAndDeviceIsSupplemental(t *testing.T) {
	cfg := testAttemptLimiterConfig()
	cfg.Account = LimitPolicy{}
	cfg.IP = LimitPolicy{}
	cfg.Device = LimitPolicy{Burst: 1, Window: time.Minute}
	limiter, err := NewMemoryAttemptLimiter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(2_000, 0)
	limiter.now = func() time.Time { return now }
	ctx := context.Background()
	attempt := LoginAttempt{Username: "one", ClientIP: "192.0.2.1", DeviceID: "same-device"}

	if decision, err := limiter.Allow(ctx, attempt); err != nil || !decision.Allowed {
		t.Fatalf("first device attempt = %+v, %v", decision, err)
	}
	if decision, err := limiter.Allow(ctx, LoginAttempt{
		Username: "two", ClientIP: "192.0.2.2", DeviceID: "same-device",
	}); err != nil || decision.Allowed || decision.RetryAfter != time.Minute {
		t.Fatalf("device dimension did not limit: %+v, %v", decision, err)
	}
	now = now.Add(time.Minute)
	if decision, err := limiter.Allow(ctx, attempt); err != nil || !decision.Allowed {
		t.Fatalf("device bucket did not refill: %+v, %v", decision, err)
	}
}

func TestAttemptKeysArePseudonymizedAndSeparated(t *testing.T) {
	cfg := testAttemptLimiterConfig()
	accountKey := attemptKey(cfg, "account", "farmer_one")
	ipKey := attemptKey(cfg, "ip", "192.0.2.1")
	deviceKey := attemptKey(cfg, "device", "browser-a")
	for _, key := range []string{accountKey, ipKey, deviceKey} {
		if strings.Contains(key, "farmer_one") || strings.Contains(key, "192.0.2.1") ||
			strings.Contains(key, "browser-a") {
			t.Fatalf("raw identifier leaked into Redis key: %q", key)
		}
	}
	if accountKey == ipKey || accountKey == deviceKey || ipKey == deviceKey {
		t.Fatal("risk dimensions shared a key")
	}
}

func TestAttemptLimiterRejectsUnsafeConfiguration(t *testing.T) {
	cfg := testAttemptLimiterConfig()
	cfg.HashKey = []byte("short")
	if _, err := NewMemoryAttemptLimiter(cfg); err == nil {
		t.Fatal("short pseudonymization key accepted")
	}
	cfg = testAttemptLimiterConfig()
	cfg.IP = LimitPolicy{Burst: 1}
	if _, err := NewMemoryAttemptLimiter(cfg); err == nil {
		t.Fatal("partial IP policy accepted")
	}
	cfg = testAttemptLimiterConfig()
	cfg.Device = LimitPolicy{Burst: 1, Window: time.Nanosecond}
	if _, err := NewMemoryAttemptLimiter(cfg); err == nil {
		t.Fatal("sub-millisecond device window accepted")
	}
}
