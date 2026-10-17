package account

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

// LoginAttempt contains risk dimensions, not authenticated identity. DeviceID
// is client supplied and therefore may only be used as a supplemental rate
// dimension; it must never become an authentication or ownership proof.
type LoginAttempt struct {
	Username string
	ClientIP string
	DeviceID string
}

type LimitPolicy struct {
	Burst  int
	Window time.Duration
}

type AttemptLimiterConfig struct {
	Prefix  string
	HashKey []byte
	Account LimitPolicy
	IP      LimitPolicy
	Device  LimitPolicy
}

type AttemptDecision struct {
	Allowed    bool
	RetryAfter time.Duration
	LimitedBy  []string
}

// AttemptLimiter performs admission before the expensive password KDF.
// ResetAccount is called only after a successful credential or registration
// flow. IP and device buckets deliberately remain consumed by successful
// requests because they protect total KDF/registration load.
type AttemptLimiter interface {
	Allow(ctx context.Context, attempt LoginAttempt) (AttemptDecision, error)
	ResetAccount(ctx context.Context, username string) error
	Close() error
}

type limitDimension struct {
	name       string
	identifier string
	policy     LimitPolicy
}

func validateAttemptLimiterConfig(cfg *AttemptLimiterConfig) error {
	if cfg == nil {
		return errors.New("account: attempt limiter config required")
	}
	cfg.Prefix = strings.TrimSuffix(strings.TrimSpace(cfg.Prefix), ":")
	if cfg.Prefix == "" {
		return errors.New("account: attempt limiter prefix required")
	}
	if len(cfg.HashKey) < 32 {
		return errors.New("account: attempt limiter hash key must contain at least 32 bytes")
	}
	for name, policy := range map[string]LimitPolicy{
		"account": cfg.Account,
		"ip":      cfg.IP,
		"device":  cfg.Device,
	} {
		if policy.Burst < 0 || policy.Window < 0 {
			return fmt.Errorf("account: invalid %s attempt limit", name)
		}
		if (policy.Burst == 0) != (policy.Window == 0) {
			return fmt.Errorf("account: %s attempt burst and window must both be set or disabled", name)
		}
		if policy.Burst > 0 && policy.Window < time.Millisecond {
			return fmt.Errorf("account: %s attempt window must be at least 1ms", name)
		}
	}
	return nil
}

// DeriveAttemptHashKey creates a purpose-separated key for pseudonymizing rate
// dimensions. Production may inject a dedicated high-entropy base secret; the
// derivation also safely separates it from an existing Gate secret.
func DeriveAttemptHashKey(baseSecret []byte) []byte {
	mac := hmac.New(sha256.New, baseSecret)
	_, _ = mac.Write([]byte("farm-game/account-attempt-limit/v1"))
	return mac.Sum(nil)
}

func attemptDimensions(attempt LoginAttempt, cfg AttemptLimiterConfig) []limitDimension {
	return []limitDimension{
		{name: "account", identifier: normalizeRiskUsername(attempt.Username), policy: cfg.Account},
		{name: "ip", identifier: normalizeRiskIP(attempt.ClientIP), policy: cfg.IP},
		{name: "device", identifier: normalizeRiskDevice(attempt.DeviceID), policy: cfg.Device},
	}
}

func normalizeRiskUsername(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeRiskIP(value string) string {
	value = strings.TrimSpace(value)
	if parsed := net.ParseIP(value); parsed != nil {
		return parsed.String()
	}
	return strings.ToLower(value)
}

func normalizeRiskDevice(value string) string {
	return strings.TrimSpace(value)
}

func attemptKey(cfg AttemptLimiterConfig, dimension, identifier string) string {
	if identifier == "" {
		return ""
	}
	mac := hmac.New(sha256.New, cfg.HashKey)
	_, _ = mac.Write([]byte(dimension))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(identifier))
	digest := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return cfg.Prefix + ":" + dimension + ":" + digest + ":hash"
}

type memoryAttemptBucket struct {
	tokens    float64
	updatedAt time.Time
	expiresAt time.Time
}

type MemoryAttemptLimiter struct {
	cfg     AttemptLimiterConfig
	now     func() time.Time
	mu      sync.Mutex
	buckets map[string]memoryAttemptBucket
	calls   uint64
}

func NewMemoryAttemptLimiter(cfg AttemptLimiterConfig) (*MemoryAttemptLimiter, error) {
	if err := validateAttemptLimiterConfig(&cfg); err != nil {
		return nil, err
	}
	return &MemoryAttemptLimiter{
		cfg: cfg, now: time.Now, buckets: make(map[string]memoryAttemptBucket),
	}, nil
}

func (l *MemoryAttemptLimiter) Allow(_ context.Context, attempt LoginAttempt) (AttemptDecision, error) {
	if l == nil {
		return AttemptDecision{}, errors.New("account: attempt limiter unavailable")
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()

	decision := AttemptDecision{Allowed: true}
	for _, dimension := range attemptDimensions(attempt, l.cfg) {
		if dimension.identifier == "" || dimension.policy.Burst == 0 {
			continue
		}
		key := attemptKey(l.cfg, dimension.name, dimension.identifier)
		bucket := l.buckets[key]
		allowed, retryAfter, next := consumeAttemptBucket(bucket, dimension.policy, now)
		l.buckets[key] = next
		if !allowed {
			decision.Allowed = false
			decision.LimitedBy = append(decision.LimitedBy, dimension.name)
			if retryAfter > decision.RetryAfter {
				decision.RetryAfter = retryAfter
			}
		}
	}
	l.calls++
	if l.calls%256 == 0 {
		for key, bucket := range l.buckets {
			if !bucket.expiresAt.After(now) {
				delete(l.buckets, key)
			}
		}
	}
	return decision, nil
}

func consumeAttemptBucket(current memoryAttemptBucket, policy LimitPolicy, now time.Time) (bool, time.Duration, memoryAttemptBucket) {
	tokens := current.tokens
	updatedAt := current.updatedAt
	if updatedAt.IsZero() || !current.expiresAt.After(now) {
		tokens = float64(policy.Burst)
		updatedAt = now
	}
	if elapsed := now.Sub(updatedAt); elapsed > 0 {
		tokens += elapsed.Seconds() * float64(policy.Burst) / policy.Window.Seconds()
		if tokens > float64(policy.Burst) {
			tokens = float64(policy.Burst)
		}
	}
	next := memoryAttemptBucket{
		tokens: tokens, updatedAt: now, expiresAt: now.Add(2 * policy.Window),
	}
	if tokens >= 1 {
		next.tokens--
		return true, 0, next
	}
	retryAfter := time.Duration((1 - tokens) * float64(policy.Window) / float64(policy.Burst))
	if retryAfter < time.Millisecond {
		retryAfter = time.Millisecond
	}
	return false, retryAfter, next
}

func (l *MemoryAttemptLimiter) ResetAccount(_ context.Context, username string) error {
	if l == nil {
		return nil
	}
	key := attemptKey(l.cfg, "account", normalizeRiskUsername(username))
	if key == "" {
		return nil
	}
	l.mu.Lock()
	delete(l.buckets, key)
	l.mu.Unlock()
	return nil
}

func (l *MemoryAttemptLimiter) Close() error { return nil }
