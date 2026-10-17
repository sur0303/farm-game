package friend

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/storage/redis/rediskey"
)

// TestRedisFriendAuthorityRealRedisProbe is intentionally opt-in.  A local
// miniredis run is never reported as company/REAL_REDIS_PASS evidence.
// CI or a controlled probe environment supplies FRIEND_REDIS_LIVE_ADDR (and,
// when needed, FRIEND_REDIS_LIVE_PASSWORD/FRIEND_REDIS_LIVE_DB).
func liveFriendClient(t *testing.T) *redis.Client {
	t.Helper()
	address := os.Getenv("FRIEND_REDIS_LIVE_ADDR")
	if address == "" {
		t.Skip("PENDING: FRIEND_REDIS_LIVE_ADDR is not configured; no real Redis evidence")
	}
	client := redis.NewClient(&redis.Options{Addr: address, Password: os.Getenv("FRIEND_REDIS_LIVE_PASSWORD")})
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("REAL_REDIS probe ping failed: %v", err)
	}
	return client
}

func TestRedisFriendAuthorityRealRedisProbe(t *testing.T) {
	client := liveFriendClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	namespace, err := rediskey.NewNamespace("probe", "friend")
	if err != nil {
		t.Fatal(err)
	}
	repositories, err := NewRedisFriendshipRepositoriesForNamespace(client, namespace)
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.EnsureScriptsLoaded(ctx); err != nil {
		t.Fatalf("REAL_REDIS script load failed: %v", err)
	}
	t.Log("REAL_REDIS_PASS: ping and all friendship scripts loaded; no production data mutation performed")
}

func cleanupLiveFriendNamespace(t *testing.T, client *redis.Client, namespace rediskey.Namespace) {
	t.Helper()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		var cursor uint64
		pattern := fmt.Sprintf("classic-farm:%s:%s:*", namespace.Environment, namespace.Zone)
		for {
			keys, next, scanErr := client.Scan(cleanupCtx, cursor, pattern, 256).Result()
			if scanErr != nil {
				return
			}
			if len(keys) != 0 {
				_ = client.Del(cleanupCtx, keys...).Err()
			}
			cursor = next
			if cursor == 0 {
				return
			}
		}
	})
}

func TestRedisFriendAtomicEnvelopeRealRedisPhaseBarrier(t *testing.T) {
	client := liveFriendClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	zone := fmt.Sprintf("env%d", time.Now().UnixNano()%1_000_000_000)
	namespace, err := rediskey.NewNamespace("review", zone)
	if err != nil {
		t.Fatal(err)
	}
	profile := DefaultFriendRedisProfile(namespace)
	profile.PairBucketCount, profile.QuotaBucketCount, profile.ShareBucketCount = 64, 64, 64
	repositories, err := NewRedisFriendshipRepositories(client, RedisFriendshipOptions{Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.EnsureScriptsLoaded(ctx); err != nil {
		t.Fatal(err)
	}
	cleanupLiveFriendNamespace(t, client, namespace)
	repeats := 1
	if raw := os.Getenv("FRIEND_ENVELOPE_LIVE_REPEAT"); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed < 1 || parsed > 100 {
			t.Fatalf("FRIEND_ENVELOPE_LIVE_REPEAT=%q", raw)
		}
		repeats = parsed
	}
	sha, ok := repositories.ScriptSHA(scriptFriendReadCompleteSnapshot)
	if !ok {
		t.Fatal("atomic envelope script is not registered")
	}
	for iteration := 0; iteration < repeats; iteration++ {
		base := uint64(10_000_000 + iteration*3_000_000)
		seed := envelopePairAt(t, repositories.authority.keys, base)
		target := envelopePairInBucket(t, repositories.authority.keys, seed.ref.BucketID, base+1_000_000)
		writer := envelopePairInBucket(t, repositories.authority.keys, seed.ref.BucketID, base+2_000_000)
		label := strconv.Itoa(iteration)
		if outcome, activateErr := envelopeActivate(ctx, repositories, seed, "live-seed-"+label); activateErr != nil || outcome.Status != StatusSucceeded {
			t.Fatalf("iteration %d seed=%+v err=%v", iteration, outcome, activateErr)
		}
		hook := &envelopeReadPhaseHook{sha: sha, entered: make(chan struct{}), release: make(chan struct{})}
		client.AddHook(hook)
		type result struct {
			outcome Outcome
			err     error
		}
		done := make(chan result, 1)
		go func() {
			outcome, activateErr := envelopeActivate(context.WithValue(ctx, envelopePhaseContextKey{}, true), repositories, target, "live-target-"+label)
			done <- result{outcome: outcome, err: activateErr}
		}()
		select {
		case <-hook.entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d did not enter PhaseBarrier", iteration)
		}
		writerOutcome, writerErr := envelopeActivate(ctx, repositories, writer, "live-writer-"+label)
		if writerErr != nil || writerOutcome.Status != StatusSucceeded {
			t.Fatalf("iteration %d writer=%+v err=%v", iteration, writerOutcome, writerErr)
		}
		close(hook.release)
		targetResult := <-done
		if targetResult.err != nil || targetResult.outcome.Status != StatusSucceeded {
			t.Fatalf("iteration %d target=%+v err=%v", iteration, targetResult.outcome, targetResult.err)
		}
	}
	metrics := repositories.EnvelopeReadMetrics()
	if metrics.ReconcileTotal != 0 || metrics.TechnicalFailureTotal != 0 {
		t.Fatalf("REAL_REDIS atomic envelope metrics=%+v", metrics)
	}
	t.Logf("REAL_REDIS_PASS: atomic same-bucket PhaseBarrier repeats=%d metrics=%+v", repeats, metrics)
}

func TestRedisShareSameBucketDueFilterRealRedis(t *testing.T) {
	client := liveFriendClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	zone := fmt.Sprintf("shr%d", time.Now().UnixNano()%1_000_000_000)
	namespace, err := rediskey.NewNamespace("review", zone)
	if err != nil {
		t.Fatal(err)
	}
	profile := DefaultFriendRedisProfile(namespace)
	profile.PairBucketCount, profile.QuotaBucketCount, profile.ShareBucketCount = 64, 64, 64
	repositories, err := NewRedisFriendshipRepositories(client, RedisFriendshipOptions{Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.EnsureScriptsLoaded(ctx); err != nil {
		t.Fatal(err)
	}
	cleanupLiveFriendNamespace(t, client, namespace)
	for iteration := 0; iteration < 20; iteration++ {
		first := uint64(150_000_000 + iteration*200_000)
		firstRef, refErr := repositories.authority.keys.BucketFor(FriendShareIssuerAuthority, strconv.FormatUint(first, 10))
		if refErr != nil {
			t.Fatal(refErr)
		}
		second := first + 1
		for ; second < first+200_000; second++ {
			ref, candidateErr := repositories.authority.keys.BucketFor(FriendShareIssuerAuthority, strconv.FormatUint(second, 10))
			if candidateErr != nil {
				t.Fatal(candidateErr)
			}
			if ref.BucketID == firstRef.BucketID {
				break
			}
		}
		if second == first+200_000 {
			t.Fatalf("iteration %d could not find same-bucket issuer", iteration)
		}
		for _, inviter := range []uint64{first, second} {
			label := fmt.Sprintf("live-share-%d-%d", iteration, inviter)
			identity := redisFriendPublic(inviter, "GENERATE_SHARE", label)
			if _, outcome, generateErr := repositories.Share.GenerateShareToken(ctx, inviter, label, identity, redisFriendHash(label), 100); generateErr != nil || outcome.Status != StatusSucceeded {
				t.Fatalf("iteration %d inviter=%d outcome=%+v err=%v", iteration, inviter, outcome, generateErr)
			}
		}
		for _, inviter := range []uint64{first, second} {
			if _, readErr := repositories.Share.GetIssuer(ctx, inviter); readErr != nil {
				t.Fatalf("iteration %d inviter=%d read err=%v", iteration, inviter, readErr)
			}
		}
	}
	t.Log("REAL_REDIS_PASS: Share GetIssuer filtered foreign same-bucket Due entries repeats=20")
}
