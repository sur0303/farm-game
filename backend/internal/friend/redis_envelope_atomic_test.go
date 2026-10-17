package friend

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"farmgame/internal/storage/redis/rediskey"
)

type envelopePhaseContextKey struct{}

type envelopeReadPhaseHook struct {
	sha     string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *envelopeReadPhaseHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *envelopeReadPhaseHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *envelopeReadPhaseHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		args := cmd.Args()
		if ctx.Value(envelopePhaseContextKey{}) == true && strings.EqualFold(cmd.Name(), "evalsha") &&
			len(args) > 1 && fmt.Sprint(args[1]) == h.sha {
			h.once.Do(func() { close(h.entered) })
			<-h.release
		}
		return err
	}
}

type envelopeTestPair struct {
	low, high uint64
	pairID    string
	ref       FriendshipBucketRef
}

func envelopePairAt(t *testing.T, builder FriendshipKeyBuilder, start uint64) envelopeTestPair {
	t.Helper()
	pairID, err := RelationIDFor(start, start+1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := builder.BucketFor(FriendPairAuthority, pairID)
	if err != nil {
		t.Fatal(err)
	}
	return envelopeTestPair{low: start, high: start + 1_000_000, pairID: pairID, ref: ref}
}

func envelopePairInBucket(t *testing.T, builder FriendshipKeyBuilder, bucket uint32, start uint64) envelopeTestPair {
	t.Helper()
	for player := start; player < start+1_000_000; player++ {
		candidate := envelopePairAt(t, builder, player)
		if candidate.ref.BucketID == bucket {
			return candidate
		}
	}
	t.Fatalf("no Pair found in bucket %d", bucket)
	return envelopeTestPair{}
}

func envelopeActivate(ctx context.Context, repositories *RedisFriendshipRepositories, pair envelopeTestPair, label string) (Outcome, error) {
	identity := CommitIdentity{WorkAddressID: pair.pairID, ParticipantInvocationID: "envelope-invocation-" + label, ParticipantStep: "ACTIVATE"}
	payload := sha256.Sum256([]byte(label))
	return repositories.Pair.ActivateFriendRelation(ctx, ActivateCommand{
		InviterPlayerID: pair.low, RedeemerPlayerID: pair.high, TargetRelationEpoch: 1,
		TokenID: "envelope-token-" + label, TokenGeneration: 1,
		OperationID: "envelope-operation-" + label, NowMs: time.Now().UnixMilli(),
	}, identity, payload[:])
}

func TestAtomicEnvelopeReadDifferentPairSameBucketPhaseBarrier(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	if err := repositories.EnsureScriptsLoaded(ctx); err != nil {
		t.Fatal(err)
	}
	builder := repositories.authority.keys
	seed := envelopePairAt(t, builder, 200_000)
	target := envelopePairInBucket(t, builder, seed.ref.BucketID, 300_000)
	writer := envelopePairInBucket(t, builder, seed.ref.BucketID, 1_400_000)
	if seed.pairID == target.pairID || seed.pairID == writer.pairID || target.pairID == writer.pairID {
		t.Fatal("PhaseBarrier requires three distinct Pairs")
	}
	if outcome, err := envelopeActivate(ctx, repositories, seed, "seed"); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("seed outcome=%+v err=%v", outcome, err)
	}
	sha, ok := repositories.ScriptSHA(scriptFriendReadCompleteSnapshot)
	if !ok {
		t.Fatal("atomic envelope read script is not registered")
	}
	client, ok := repositories.authority.client.(*redis.Client)
	if !ok {
		t.Fatalf("test client type=%T", repositories.authority.client)
	}
	hook := &envelopeReadPhaseHook{sha: sha, entered: make(chan struct{}), release: make(chan struct{})}
	client.AddHook(hook)
	type activationResult struct {
		outcome Outcome
		err     error
	}
	done := make(chan activationResult, 1)
	go func() {
		outcome, err := envelopeActivate(context.WithValue(ctx, envelopePhaseContextKey{}, true), repositories, target, "target")
		done <- activationResult{outcome: outcome, err: err}
	}()
	select {
	case <-hook.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("target did not reach post-atomic-read barrier")
	}
	writerOutcome, err := envelopeActivate(ctx, repositories, writer, "writer")
	if err != nil || writerOutcome.Status != StatusSucceeded {
		t.Fatalf("writer outcome=%+v err=%v", writerOutcome, err)
	}
	close(hook.release)
	targetResult := <-done
	if targetResult.err != nil || targetResult.outcome.Status != StatusSucceeded {
		t.Fatalf("target outcome=%+v err=%v", targetResult.outcome, targetResult.err)
	}
	for _, pair := range []envelopeTestPair{target, writer} {
		snapshot, err := repositories.Pair.GetPair(ctx, pair.low, pair.high)
		if err != nil || snapshot.Relation.Status != RelationStatusActive || snapshot.Relation.RelationEpoch != 1 {
			t.Fatalf("Pair %s snapshot=%+v err=%v", pair.pairID, snapshot, err)
		}
	}
	metrics := repositories.EnvelopeReadMetrics()
	if metrics.AtomicReadTotal == 0 || metrics.ReconcileTotal != 0 || metrics.TechnicalFailureTotal != 0 ||
		metrics.AtomicReadTotal != metrics.PresentTotal+metrics.AbsentTotal {
		t.Fatalf("envelope metrics=%+v", metrics)
	}
}

func TestAtomicEnvelopeReadSameOperationConcurrentReplayConverges(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	if err := repositories.EnsureScriptsLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}
	pair := envelopePairAt(t, repositories.authority.keys, 2_000_000)
	identity := redisFriendPublic(pair.low, "BLOCK_PLAYER", "atomic-envelope-same-operation")
	payload := redisFriendHash("atomic-envelope-same-operation")
	invoke := func() (Outcome, error) {
		return repositories.Pair.BlockPlayer(context.Background(), PairMutationCommand{Identity: identity,
			PayloadHash: payload, CallerPlayerID: pair.low, PeerPlayerID: pair.high, NowMs: time.Now().UnixMilli()})
	}
	first, err := invoke()
	if err != nil || first.Status != StatusSucceeded {
		t.Fatalf("initial same operation=%+v err=%v", first, err)
	}
	const concurrency = 20
	start := make(chan struct{})
	results := make(chan error, concurrency)
	for index := 0; index < concurrency; index++ {
		go func() {
			<-start
			outcome, err := invoke()
			if err == nil && outcome.Status != StatusSucceeded {
				err = fmt.Errorf("status=%d reason=%s", outcome.Status, outcome.Reason)
			}
			results <- err
		}()
	}
	close(start)
	for index := 0; index < concurrency; index++ {
		if err := <-results; err != nil {
			t.Fatalf("same operation replay %d: %v", index, err)
		}
	}
	// Concurrent replays of one settled operation must converge to its one
	// receipt without a second Pair mutation.
	replay, err := invoke()
	if err != nil || replay.Status != StatusSucceeded {
		t.Fatalf("settled same-operation replay=%+v err=%v", replay, err)
	}
	snapshot, err := repositories.Pair.GetPair(context.Background(), pair.low, pair.high)
	if err != nil || snapshot.LowBlocksHigh.Status != BlockStatusActive || snapshot.RecordVersion != 1 {
		t.Fatalf("same operation snapshot=%+v err=%v", snapshot, err)
	}
	group, err := repositories.authority.group(FriendPairAuthority, pair.pairID, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if count, err := repositories.authority.client.ZCard(context.Background(), group.receiptDue).Result(); err != nil || count != 1 {
		t.Fatalf("same operation receipt due=%d err=%v", count, err)
	}
}

func runAtomicEnvelopeSameBucketDistinctPairs(t *testing.T, concurrency, total int) {
	t.Helper()
	server := miniredis.RunT(t)
	namespace, err := rediskey.NewNamespace("test", fmt.Sprintf("envelopec%d", concurrency))
	if err != nil {
		t.Fatal(err)
	}
	profile := DefaultFriendRedisProfile(namespace)
	profile.PairBucketCount, profile.QuotaBucketCount, profile.ShareBucketCount = 64, 64, 64
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), PoolSize: max(32, concurrency+8)})
	defer client.Close()
	repositories, err := NewRedisFriendshipRepositories(client, RedisFriendshipOptions{Profile: profile})
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.EnsureScriptsLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}
	builder := repositories.authority.keys
	seed := envelopePairAt(t, builder, 6_000_000)
	pairs := make([]envelopeTestPair, 0, total)
	for player := uint64(7_000_000); len(pairs) < cap(pairs); player++ {
		candidate := envelopePairAt(t, builder, player)
		if candidate.ref.BucketID == seed.ref.BucketID {
			pairs = append(pairs, candidate)
		}
	}
	jobs := make(chan int)
	errorsByIndex := make([]error, len(pairs))
	var workers sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				outcome, activateErr := envelopeActivate(context.Background(), repositories, pairs[index], fmt.Sprintf("c%d-%d", concurrency, index))
				if activateErr != nil {
					errorsByIndex[index] = activateErr
				} else if outcome.Status != StatusSucceeded {
					errorsByIndex[index] = fmt.Errorf("status=%d reason=%s", outcome.Status, outcome.Reason)
				}
			}
		}()
	}
	for index := range pairs {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	for index, operationErr := range errorsByIndex {
		if operationErr != nil {
			t.Fatalf("operation %d/%s: %v", index, pairs[index].pairID, operationErr)
		}
	}
	metrics := repositories.EnvelopeReadMetrics()
	if metrics.ReconcileTotal != 0 || metrics.TechnicalFailureTotal != 0 {
		t.Fatalf("concurrency=%d metrics=%+v", concurrency, metrics)
	}
}

func TestAtomicEnvelopeReadSameBucketC10Completes300WithoutFalseReconcile(t *testing.T) {
	runAtomicEnvelopeSameBucketDistinctPairs(t, 10, 300)
}

func TestAtomicEnvelopeReadSameBucketDistinctPairConcurrencyMatrix(t *testing.T) {
	for _, concurrency := range []int{1, 10, 50, 100} {
		t.Run(fmt.Sprintf("c%d", concurrency), func(t *testing.T) {
			runAtomicEnvelopeSameBucketDistinctPairs(t, concurrency, 300)
		})
	}
}

func TestAtomicEnvelopeReadConcurrentAttemptCompactionHasStableSemantics(t *testing.T) {
	repositories, server := newFriendRedisTestRepositoriesWithLimits(t, FriendRedisLimits{MaxFriends: 300, AttemptRetentionMs: 1})
	defer server.Close()
	ctx := context.Background()
	if err := repositories.EnsureScriptsLoaded(ctx); err != nil {
		t.Fatal(err)
	}
	builder := repositories.authority.keys
	seed := envelopePairAt(t, builder, 2_500_000)
	target := envelopePairInBucket(t, builder, seed.ref.BucketID, 2_600_000)
	if outcome, err := envelopeActivate(ctx, repositories, seed, "compact-seed"); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("seed outcome=%+v err=%v", outcome, err)
	}
	server.FastForward(5 * time.Millisecond)
	sha, _ := repositories.ScriptSHA(scriptFriendReadCompleteSnapshot)
	client := repositories.authority.client.(*redis.Client)
	hook := &envelopeReadPhaseHook{sha: sha, entered: make(chan struct{}), release: make(chan struct{})}
	client.AddHook(hook)
	type activationResult struct {
		outcome Outcome
		err     error
	}
	done := make(chan activationResult, 1)
	go func() {
		outcome, err := envelopeActivate(context.WithValue(ctx, envelopePhaseContextKey{}, true), repositories, target, "compact-target")
		done <- activationResult{outcome: outcome, err: err}
	}()
	select {
	case <-hook.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("target did not reach compaction barrier")
	}
	compacted, err := repositories.CompactCompletedAttemptMarkers(ctx, FriendPairAuthority, seed.pairID, 8)
	if err != nil || compacted != 1 {
		t.Fatalf("compaction=%d err=%v", compacted, err)
	}
	close(hook.release)
	result := <-done
	if result.err != nil || result.outcome.Status != StatusSucceeded {
		t.Fatalf("target after compaction outcome=%+v err=%v", result.outcome, result.err)
	}
}

func TestAtomicEnvelopeReadDifferentBucketIsolationAndRealCorruption(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	if err := repositories.EnsureScriptsLoaded(ctx); err != nil {
		t.Fatal(err)
	}
	builder := repositories.authority.keys
	first := envelopePairAt(t, builder, 3_000_000)
	second := envelopePairAt(t, builder, 4_000_000)
	for second.ref.BucketID == first.ref.BucketID {
		second = envelopePairAt(t, builder, second.low+1)
	}
	if outcome, err := envelopeActivate(ctx, repositories, first, "first-bucket"); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("first outcome=%+v err=%v", outcome, err)
	}
	if outcome, err := envelopeActivate(ctx, repositories, second, "second-bucket"); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("second outcome=%+v err=%v", outcome, err)
	}
	for _, pair := range []envelopeTestPair{first, second} {
		if _, err := repositories.Pair.GetPair(ctx, pair.low, pair.high); err != nil {
			t.Fatalf("different bucket Pair %s: %v", pair.pairID, err)
		}
	}
	group, err := repositories.authority.group(FriendPairAuthority, first.pairID, [32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	if err := repositories.authority.client.HIncrBy(ctx, group.meta, "receipt_count", 1).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := repositories.Pair.GetPair(ctx, first.low, first.high); !errors.Is(err, ErrFriendReconcile) {
		t.Fatalf("stable corruption err=%v want ErrFriendReconcile", err)
	}
	if repositories.EnvelopeReadMetrics().ReconcileTotal == 0 {
		t.Fatal("real corruption did not increment reconcile metric")
	}
}

func TestAtomicEnvelopeReadNOSCRIPTReloadAndBoundedSource(t *testing.T) {
	repositories, server := newFriendRedisTestRepositories(t, 300)
	defer server.Close()
	ctx := context.Background()
	if err := repositories.EnsureScriptsLoaded(ctx); err != nil {
		t.Fatal(err)
	}
	pair := envelopePairAt(t, repositories.authority.keys, 5_000_000)
	if outcome, err := envelopeActivate(ctx, repositories, pair, "noscript-seed"); err != nil || outcome.Status != StatusSucceeded {
		t.Fatalf("seed outcome=%+v err=%v", outcome, err)
	}
	if err := repositories.authority.client.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := repositories.Pair.GetPair(ctx, pair.low, pair.high); err != nil {
		t.Fatalf("atomic read after SCRIPT FLUSH: %v", err)
	}
	source := friendScriptSources()[scriptFriendReadCompleteSnapshot]
	for _, forbidden := range []string{"redis.call('KEYS'", "redis.call(\"KEYS\"", "HGETALL", "SCAN"} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("atomic read source contains forbidden %q", forbidden)
		}
	}
	if !strings.Contains(source, "READ_FRIEND_AUTHORITY_COMPLETE_SNAPSHOT_V2") ||
		strings.Count(source, "redis.call('HMGET'") != 1 || strings.Count(source, "redis.call('HLEN'") != 1 ||
		strings.Count(source, "redis.call('ZCARD'") != 1 {
		t.Fatalf("complete snapshot source lost its bounded helper shape")
	}
}
