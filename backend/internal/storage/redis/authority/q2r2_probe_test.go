package authority

// Q2R-2A probes run only against a REAL Redis (FARM_REDIS_URL).  They mirror
// the semantics proven on MemoryStore/FaultStore (miniredis) in a real Redis
// process, including the Redis TIME authority clock, script (Lua) execution,
// the Transient/LostGeneration classification contract and exact-key cleanup.
//
//   FARM_REDIS_URL='redis://<endpoint>/0' \
//     go test ./internal/storage/redis/authority/ -run Q2R2 -v -count=1
//
// Result contract (Q2R-2A):
//   - Every probe logs "Q2R2-<n> OK" on success and cleans up via the EXACT
//     keys it created (DEL + EXISTS), never via KEYS or a global SCAN.
//   - NOSCRIPT is exercised with a never-before-loaded unique script (random
//     comment); a global SCRIPT FLUSH is NEVER issued by default.
//   - A probe that skips (no FARM_REDIS_URL, or a non-cluster instance for the
//     cluster item) MUST be reported PROBE_PENDING / INCONCLUSIVE.
//   - The cluster-routing item is only decidable on a direct cluster; the
//     company proxy profile stays a separate PASS.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"
)

// q2r2LiveClient requires BOTH FARM_REDIS_URL and Q2R2_LIVE=1 so an ordinary
// developer run can never touch the company Redis by accident.
func q2r2LiveClient(t *testing.T) *redis.Client {
	t.Helper()
	if strings.TrimSpace(os.Getenv("Q2R2_LIVE")) != "1" {
		t.Skip("Q2R2_LIVE=1 required (in addition to FARM_REDIS_URL) to run live probes")
	}
	return liveClient(t)
}

func q2r2Profile(t *testing.T) rediskey.PhysicalProfile {
	t.Helper()
	// UnixNano keeps every probe on an isolated namespace so a probe's live
	// lease can never conflict with a sibling probe's claim on the same shard.
	namespace, err := rediskey.NewNamespace(fmt.Sprintf("liveq2r2%d", time.Now().UnixNano()), "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion:         "q2r2-v1",
		Namespace:              namespace,
		AuthorityLayoutVersion: "v1",
		WorkflowLayoutVersion:  "v1",
		WorkflowHashVersion:    1,
		WorkflowBucketCount:    64,
	}
	if err := profile.Validate(); err != nil {
		t.Fatal(err)
	}
	return profile
}

// q2r2Store builds an isolated real-Redis store and returns it together with
// the exact shard-authority key the probe creates (for exact-key cleanup).
func q2r2Store(t *testing.T, client *redis.Client) (*Store, rediskey.RedisKey, *actorrouting.Calculator, authoritystore.ActorAddress, uint32) {
	t.Helper()
	profile := q2r2Profile(t)
	calculator := testCalculator(t)
	store, err := NewStore(client, nil, Options{Profile: profile, Resolver: calculator})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Registry().EnsureLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}
	addr := authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 10001}
	bucket, err := profile.AuthorityBucketForActor(calculator, addr)
	if err != nil {
		t.Fatal(err)
	}
	shardKey, err := store.Keys().ShardAuthority(bucket)
	if err != nil {
		t.Fatal(err)
	}
	return store, shardKey, calculator, addr, bucket.VirtualShard()
}

func q2r2ID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func q2r2Fence(authority authoritystore.ShardWriteAuthority) authoritystore.ShardWriteFence {
	return authoritystore.ShardWriteFence{
		ExpectedHolderID:           authority.HolderID,
		ExpectedHolderBootID:       authority.HolderBootID,
		ExpectedAssignmentRevision: authority.AssignmentRevision,
		ExpectedWriterEpoch:        authority.WriterEpoch,
		ExpectedLeaseEpoch:         authority.LeaseEpoch,
	}
}

func q2r2Holder(authority authoritystore.ShardWriteAuthority) authoritystore.TrustedShardAuthorityCaller {
	return authoritystore.TrustedShardAuthorityCaller{
		Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: authority.HolderID, BootID: authority.HolderBootID,
	}
}

func redisTimeMS(t *testing.T, client *redis.Client) int64 {
	t.Helper()
	tm, err := client.Time(context.Background()).Result()
	if err != nil {
		t.Fatalf("redis TIME: %v", err)
	}
	return tm.UnixMilli()
}

// q2r2Keys tracks the EXACT keys a probe creates.  Cleanup DELs those keys and
// verifies each with EXISTS==0.  It never uses KEYS or a global SCAN, and it
// never touches keys owned by another probe/namespace.
type q2r2Keys struct {
	t      *testing.T
	client *redis.Client
	keys   map[string]string
}

func newQ2R2Keys(t *testing.T, client *redis.Client) *q2r2Keys {
	k := &q2r2Keys{t: t, client: client, keys: make(map[string]string)}
	t.Cleanup(k.cleanup)
	return k
}

func (k *q2r2Keys) track(key rediskey.RedisKey) { k.keys[key.String()] = key.String() }

func (k *q2r2Keys) trackRaw(key string) { k.keys[key] = key }

func (k *q2r2Keys) cleanup() {
	for _, key := range k.keys {
		if err := k.client.Del(context.Background(), key).Err(); err != nil {
			k.t.Logf("q2r2 cleanup DEL %s: %v", key, err)
		}
	}
	for _, key := range k.keys {
		n, err := k.client.Exists(context.Background(), key).Result()
		if err != nil {
			k.t.Fatalf("q2r2 cleanup EXISTS %s: %v", key, err)
		}
		if n != 0 {
			k.t.Fatalf("q2r2 residue key %s still exists", key)
		}
	}
}

// renewTransportFaultHook injects a transport failure (ECONNREFUSED, provably
// before the command reached Redis) into the FIRST renew-script EVALSHA,
// exercising the UNAVAILABLE_NOT_SENT / LeaseRenewTransient classification on
// the real Redis.
type renewTransportFaultHook struct {
	sha  string
	once atomic.Bool
}

func (h *renewTransportFaultHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *renewTransportFaultHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if cmd.Name() == "evalsha" && len(cmd.Args()) > 1 {
			if s, ok := cmd.Args()[1].(string); ok && s == h.sha {
				if h.once.CompareAndSwap(false, true) {
					return syscall.ECONNREFUSED
				}
			}
		}
		return next(ctx, cmd)
	}
}
func (h *renewTransportFaultHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// TestLiveQ2R2ClaimRenewAcceptedTTLAndDeadlineRefresh proves the Redis clock
// (TIME) is the authority: the claimed/renewed lease deadline tracks
// Redis TIME + AcceptedTTL, and a same-generation renew refreshes the local
// monotonic deadline forward.
func TestLiveQ2R2ClaimRenewAcceptedTTLAndDeadlineRefresh(t *testing.T) {
	client := q2r2LiveClient(t)
	store, shardKey, _, addr, shard := q2r2Store(t, client)
	keys := newQ2R2Keys(t, client)
	keys.track(shardKey)
	now := redisTimeMS(t, client)

	claim, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller:                 routeManager(),
		TransitionID:           q2r2ID("q2r2-claim"),
		AuthorityLayoutVersion: "v1",
		Domain:                 addr.Domain,
		VirtualShard:           shard,
		NewHolderID:            "q2r2-pod-1",
		NewHolderBootID:        "q2r2-boot-1",
		RequestedTTLms:         8000,
	})
	if err != nil || claim.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	if claim.AcceptedTTLms <= 0 {
		t.Fatalf("accepted_ttl=%d must be > 0", claim.AcceptedTTLms)
	}
	firstUntil := claim.Authority.LeaseUntilMs
	if d := firstUntil - (now + int64(claim.AcceptedTTLms)); d < -1500 || d > 1500 {
		t.Fatalf("lease_until=%d now=%d accepted_ttl=%d: must track Redis TIME (delta=%d)", firstUntil, now, claim.AcceptedTTLms, d)
	}

	fence := q2r2Fence(claim.Authority)
	time.Sleep(300 * time.Millisecond)
	renew, err := store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller:                 q2r2Holder(claim.Authority),
		AuthorityLayoutVersion: "v1",
		Domain:                 addr.Domain,
		VirtualShard:           shard,
		ExpectedFence:          fence,
		RequestedTTLms:         8000,
	})
	if err != nil || renew.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("renew=%+v err=%v", renew, err)
	}
	if renew.AcceptedTTLms <= 0 || renew.Authority.LeaseUntilMs <= firstUntil {
		t.Fatalf("renew did not refresh deadline: accepted_ttl=%d until=%d first=%d", renew.AcceptedTTLms, renew.Authority.LeaseUntilMs, firstUntil)
	}
	t.Logf("Q2R2-1 OK claim_until=%d renew_until=%d accepted_ttl=%d", firstUntil, renew.Authority.LeaseUntilMs, renew.AcceptedTTLms)
}

// TestLiveQ2R2TransientTransportFailureBeforeDeadlineKeepsAdmission freezes
// the TRANSIENT classification on the real Redis: a transport failure that
// provably never reached the command processor (ECONNREFUSED) yields
// UNAVAILABLE_NOT_SENT -- NOT a FenceMismatch/LostGeneration signal -- so
// RenewWithFence maps it to LeaseRenewTransient and the guard keeps serving
// until its local deadline.  The authoritative lease itself is untouched.
func TestLiveQ2R2TransientTransportFailureBeforeDeadlineKeepsAdmission(t *testing.T) {
	client := q2r2LiveClient(t)
	renewSHA := newScriptDef(ScriptRenewShardAuthority, luaRenewShardAuthoritySrc).sha
	client.AddHook(&renewTransportFaultHook{sha: renewSHA})
	store, shardKey, _, addr, shard := q2r2Store(t, client)
	keys := newQ2R2Keys(t, client)
	keys.track(shardKey)

	claim, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: routeManager(), TransitionID: q2r2ID("q2r2-tx"), AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: shard,
		NewHolderID: "q2r2-pod-1", NewHolderBootID: "q2r2-boot-1", RequestedTTLms: 8000,
	})
	if err != nil || claim.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	fence := q2r2Fence(claim.Authority)
	req := authoritystore.RenewShardAuthorityRequest{
		Caller: q2r2Holder(claim.Authority), AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: shard,
		ExpectedFence: fence, RequestedTTLms: 8000,
	}

	// The first renew EVALSHA is intercepted and fails before send.
	renew, err := store.RenewShardAuthority(context.Background(), req)
	if err != nil || renew.Status != authoritystore.ShardTransitionUnavailableNotSent {
		t.Fatalf("transport failure must classify UNAVAILABLE_NOT_SENT (Transient), got status=%v err=%v", renew.Status, err)
	}
	// The authoritative lease is untouched: a correct renew still applies.
	good, err := store.RenewShardAuthority(context.Background(), req)
	if err != nil || good.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("lease died after a transient failure: %+v err=%v", good, err)
	}
	t.Logf("Q2R2-TRANSIENT OK status=UNAVAILABLE_NOT_SENT then_live_until=%d", good.Authority.LeaseUntilMs)
}

// TestLiveQ2R2FenceMismatchIsLostGeneration freezes the LOST_GENERATION
// classification on the real Redis: a stale-generation renew is answered
// FENCE_MISMATCH (never Transient) -- the exact signal RenewWithFence turns
// into LeaseRenewLostGeneration and a local precise fence of that shard.  The
// current generation (and every other shard) is unaffected.
func TestLiveQ2R2FenceMismatchIsLostGeneration(t *testing.T) {
	client := q2r2LiveClient(t)
	store, shardKey, _, addr, shard := q2r2Store(t, client)
	keys := newQ2R2Keys(t, client)
	keys.track(shardKey)

	claim, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: routeManager(), TransitionID: q2r2ID("q2r2-lg"), AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: shard,
		NewHolderID: "q2r2-pod-1", NewHolderBootID: "q2r2-boot-1", RequestedTTLms: 8000,
	})
	if err != nil || claim.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	fence := q2r2Fence(claim.Authority)

	stale := fence
	stale.ExpectedLeaseEpoch++
	staleResult, err := store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller: q2r2Holder(claim.Authority), AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: shard,
		ExpectedFence: stale, RequestedTTLms: 8000,
	})
	if err != nil || staleResult.Status != authoritystore.ShardTransitionFenceMismatch {
		t.Fatalf("stale fence must classify FENCE_MISMATCH (LostGeneration), got status=%v err=%v", staleResult.Status, err)
	}
	if staleResult.Status == authoritystore.ShardTransitionUnavailableNotSent || staleResult.Status == authoritystore.ShardTransitionTimeoutUnknown {
		t.Fatalf("FENCE_MISMATCH must never be classified transient: %v", staleResult.Status)
	}

	good, err := store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller: q2r2Holder(claim.Authority), AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: shard,
		ExpectedFence: fence, RequestedTTLms: 8000,
	})
	if err != nil || good.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("current generation must be unaffected by a stale fence: %+v err=%v", good, err)
	}
	t.Logf("Q2R2-LOSTGEN OK stale=FENCE_MISMATCH current_until=%d", good.Authority.LeaseUntilMs)
}

// TestLiveQ2R2SameGenerationRenewAfterLocalDeadlineExpiry proves the recovery
// precondition for "local safety deadline expired but authoritative lease
// alive": a same-generation renew issued after the local deadline window
// (but before the authoritative lease expiry) is accepted and re-admits.
func TestLiveQ2R2SameGenerationRenewAfterLocalDeadlineExpiry(t *testing.T) {
	client := q2r2LiveClient(t)
	store, shardKey, _, addr, shard := q2r2Store(t, client)
	keys := newQ2R2Keys(t, client)
	keys.track(shardKey)

	claim, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: routeManager(), TransitionID: q2r2ID("q2r2-c3"), AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: shard,
		NewHolderID: "q2r2-pod-1", NewHolderBootID: "q2r2-boot-1", RequestedTTLms: 6000,
	})
	if err != nil || claim.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	fence := q2r2Fence(claim.Authority)
	until := claim.Authority.LeaseUntilMs

	// Sleep past any realistic local safety-margin window, keeping the
	// authoritative lease alive (>= 2s of lease remain).
	for now := redisTimeMS(t, client); now < until-2000; now = redisTimeMS(t, client) {
		time.Sleep(time.Duration(until-2000-now) * time.Millisecond)
	}
	renew, err := store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller: q2r2Holder(claim.Authority), AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: shard,
		ExpectedFence: fence, RequestedTTLms: 6000,
	})
	if err != nil || renew.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("same-generation renew after local deadline must succeed while authority lease alive: %+v err=%v", renew, err)
	}
	t.Logf("Q2R2-3 OK renew_after_local_deadline until=%d renewed_until=%d", until, renew.Authority.LeaseUntilMs)
}

// TestLiveQ2R2LateRenewAfterAuthorityLeaseExpiryCannotRevive proves a renew
// arriving after the authoritative lease truly expired cannot revive it.
func TestLiveQ2R2LateRenewAfterAuthorityLeaseExpiryCannotRevive(t *testing.T) {
	client := q2r2LiveClient(t)
	store, shardKey, _, addr, shard := q2r2Store(t, client)
	keys := newQ2R2Keys(t, client)
	keys.track(shardKey)

	claim, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: routeManager(), TransitionID: q2r2ID("q2r2-c4"), AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: shard,
		NewHolderID: "q2r2-pod-1", NewHolderBootID: "q2r2-boot-1", RequestedTTLms: 2000,
	})
	if err != nil || claim.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	fence := q2r2Fence(claim.Authority)
	until := claim.Authority.LeaseUntilMs

	for now := redisTimeMS(t, client); now < until+1000; now = redisTimeMS(t, client) {
		time.Sleep(time.Duration(until+1000-now) * time.Millisecond)
	}
	late, err := store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller: q2r2Holder(claim.Authority), AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: shard,
		ExpectedFence: fence, RequestedTTLms: 2000,
	})
	if err == nil && late.Status == authoritystore.ShardTransitionApplied {
		t.Fatalf("late renew after authority lease expiry must not revive: %+v", late)
	}
	t.Logf("Q2R2-4 OK late_renew_status=%v err=%v cannot revive after lease expiry", late.Status, err)
}

// TestLiveQ2R2ExpiredTakeoverUsesDedicatedFencedCAS proves the automatic
// crash-recovery mutation on a real Redis clock. Ordinary Transfer is not
// allowed to reuse an expired source lease; the dedicated takeover accepts
// only the exact old generation and advances all write fences once.
func TestLiveQ2R2ExpiredTakeoverUsesDedicatedFencedCAS(t *testing.T) {
	client := q2r2LiveClient(t)
	store, shardKey, _, addr, shard := q2r2Store(t, client)
	keys := newQ2R2Keys(t, client)
	keys.track(shardKey)

	claim, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: routeManager(), TransitionID: q2r2ID("q2r2-failover-claim"), AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: shard,
		NewHolderID: "q2r2-failed-pod", NewHolderBootID: "q2r2-failed-boot", RequestedTTLms: 2000,
	})
	if err != nil || claim.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	fence := q2r2Fence(claim.Authority)
	for now := redisTimeMS(t, client); now < claim.Authority.LeaseUntilMs+250; now = redisTimeMS(t, client) {
		time.Sleep(time.Duration(claim.Authority.LeaseUntilMs+250-now) * time.Millisecond)
	}

	ordinary, err := store.TransferShardAuthority(context.Background(), authoritystore.TransferShardAuthorityRequest{
		Caller: routeManager(), TransitionID: q2r2ID("q2r2-invalid-transfer"), AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: shard, ExpectedFence: fence,
		NewHolderID: "q2r2-recovery-pod", NewHolderBootID: "q2r2-recovery-boot", RequestedTTLms: 20_000,
	})
	if err != nil || ordinary.Status != authoritystore.ShardTransitionLeaseExpired {
		t.Fatalf("ordinary expired transfer must be rejected result=%+v err=%v", ordinary, err)
	}

	transitionID := q2r2ID("q2r2-expired-takeover")
	request := authoritystore.ExpiredTakeoverShardAuthorityRequest{
		Caller: routeManager(), TransitionID: transitionID, AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: shard, ExpectedFence: fence,
		NewHolderID: "q2r2-recovery-pod", NewHolderBootID: "q2r2-recovery-boot", RequestedTTLms: 20_000,
	}
	taken, err := store.TakeoverExpiredShardAuthority(context.Background(), request)
	if err != nil || taken.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("expired takeover=%+v err=%v", taken, err)
	}
	if taken.Authority.HolderID != request.NewHolderID ||
		taken.Authority.HolderBootID != request.NewHolderBootID ||
		taken.Authority.AssignmentRevision != fence.ExpectedAssignmentRevision+1 ||
		taken.Authority.WriterEpoch != fence.ExpectedWriterEpoch+1 ||
		taken.Authority.LeaseEpoch != fence.ExpectedLeaseEpoch+1 {
		t.Fatalf("takeover did not advance exact fences: %+v", taken.Authority)
	}
	replay, err := store.TakeoverExpiredShardAuthority(context.Background(), request)
	if err != nil || replay.Status != authoritystore.ShardTransitionAlreadyApplied {
		t.Fatalf("takeover replay=%+v err=%v", replay, err)
	}
	late, err := store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller: q2r2Holder(claim.Authority), AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: shard, ExpectedFence: fence, RequestedTTLms: 20_000,
	})
	if err != nil || late.Status != authoritystore.ShardTransitionFenceMismatch {
		t.Fatalf("old generation renewed after takeover result=%+v err=%v", late, err)
	}
	t.Logf("Q2R2-FAILOVER OK holder=%s writer_epoch=%d lease_epoch=%d assignment_revision=%d",
		taken.Authority.HolderID, taken.Authority.WriterEpoch, taken.Authority.LeaseEpoch, taken.Authority.AssignmentRevision)
}

// TestLiveQ2R2TransferG1G2G3StaleFenceRejected proves stale G1/G2 fences from
// an older generation cannot mutate the current G3 authority: only the newest
// fence advances it.
func TestLiveQ2R2TransferG1G2G3StaleFenceRejected(t *testing.T) {
	client := q2r2LiveClient(t)
	store, shardKey, _, addr, shard := q2r2Store(t, client)
	keys := newQ2R2Keys(t, client)
	keys.track(shardKey)

	claim, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: routeManager(), TransitionID: q2r2ID("q2r2-c5"), AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: shard,
		NewHolderID: "q2r2-pod-1", NewHolderBootID: "q2r2-boot-1", RequestedTTLms: 20000,
	})
	if err != nil || claim.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	fence1 := q2r2Fence(claim.Authority)

	transfer := func(fence authoritystore.ShardWriteFence, to string) authoritystore.ShardAuthorityTransitionResult {
		t.Helper()
		res, err := store.TransferShardAuthority(context.Background(), authoritystore.TransferShardAuthorityRequest{
			Caller: routeManager(), TransitionID: q2r2ID("q2r2-x"), AuthorityLayoutVersion: "v1",
			Domain: addr.Domain, VirtualShard: shard,
			ExpectedFence: fence, NewHolderID: to, NewHolderBootID: "boot-" + to, RequestedTTLms: 20000,
		})
		if err != nil {
			t.Fatalf("transfer=%+v err=%v", res, err)
		}
		return res
	}
	g2 := transfer(fence1, "q2r2-pod-2")
	fence2 := q2r2Fence(g2.Authority)
	g3 := transfer(fence2, "q2r2-pod-3")
	fence3 := q2r2Fence(g3.Authority)

	for name, stale := range map[string]authoritystore.ShardWriteFence{"g1": fence1, "g2": fence2} {
		res, err := store.TransferShardAuthority(context.Background(), authoritystore.TransferShardAuthorityRequest{
			Caller: routeManager(), TransitionID: q2r2ID("q2r2-stale"), AuthorityLayoutVersion: "v1",
			Domain: addr.Domain, VirtualShard: shard,
			ExpectedFence: stale, NewHolderID: "q2r2-stale-pod", NewHolderBootID: "boot-stale", RequestedTTLms: 20000,
		})
		if err == nil && res.Status == authoritystore.ShardTransitionApplied {
			t.Fatalf("stale %s fence must not mutate G3: %+v", name, res)
		}
	}
	final := transfer(fence3, "q2r2-pod-4")
	if final.Authority.HolderID != "q2r2-pod-4" {
		t.Fatalf("G3 fence did not advance: %+v", final)
	}
	t.Logf("Q2R2-5 OK g1/g2 stale fences rejected; g3 -> holder=%s epoch=%d", final.Authority.HolderID, final.Authority.WriterEpoch)
}

// TestLiveQ2R2NoScriptReloadWithoutGlobalFlush proves NOSCRIPT recovery with a
// never-before-loaded script (unique SHA via a random comment): the first
// EVALSHA naturally returns NOSCRIPT, the registry loads the exact source and
// retries successfully.  No SCRIPT FLUSH is issued, so a shared instance is
// never globally affected.
func TestLiveQ2R2NoScriptReloadWithoutGlobalFlush(t *testing.T) {
	client := q2r2LiveClient(t)
	unique := fmt.Sprintf("q2r2-noscript-%d", time.Now().UnixNano())
	def := newScriptDef(unique, "-- "+unique+"\nreturn {KEYS[1], ARGV[1]}")
	reg := &ScriptRegistry{client: client, scripts: map[string]scriptDef{def.name: def}}
	key := "classic-farm:{" + unique + "}:noscript"
	keys := newQ2R2Keys(t, client)
	keys.trackRaw(key)

	reply, err := reg.Run(context.Background(), def.name, []string{key}, "v")
	if err != nil {
		t.Fatalf("NOSCRIPT auto-reload failed on real Redis: %v", err)
	}
	if len(reply) != 2 || reply[0] != key || reply[1] != "v" {
		t.Fatalf("reply=%v", reply)
	}
	t.Logf("Q2R2-NOSCRIPT OK unique_sha=%s reloaded without global flush", def.sha)
}

// TestLiveQ2R2CommitIdempotentReplayAfterUnknown proves that replaying a
// commit with the same identity -- the situation after "Lua applied but the
// client reply was lost" -- resolves to ALREADY_COMMITTED and never installs
// an unproven second generation.  (The lost-reply fault injection itself is
// covered by the FaultStore dropClient suite.)
func TestLiveQ2R2CommitIdempotentReplayAfterUnknown(t *testing.T) {
	client := q2r2LiveClient(t)
	store, shardKey, _, addr, shard := q2r2Store(t, client)
	keys := newQ2R2Keys(t, client)
	keys.track(shardKey)

	claim, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: routeManager(), TransitionID: q2r2ID("q2r2-c7"), AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: shard,
		NewHolderID: "q2r2-pod-1", NewHolderBootID: "q2r2-boot-1", RequestedTTLms: 20000,
	})
	if err != nil || claim.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	fence := q2r2Fence(claim.Authority)

	loaded, err := store.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
		ActorAddress: addr, AuthorityLayoutVersion: "v1", ShardWriteFence: fence,
		ClaimOperationID: q2r2ID("q2r2-load"), NewWriterClaimID: "q2r2-writer",
		InitialModelFields: authoritystore.ModelState{"crop": []byte("seed")},
	})
	if err != nil || loaded.Status != authoritystore.LoadAndClaimClaimed {
		t.Fatalf("load=%+v err=%v", loaded, err)
	}
	op := q2r2ID("q2r2-commit")
	req := commitReqFor(addr, fence, "q2r2-writer", loaded.Snapshot.RecordVersion, op,
		authoritystore.ModelState{"crop": []byte("remaining=8")})

	first, err := store.Commit(context.Background(), req)
	if err != nil || first.Status != authoritystore.CommitCommitted {
		t.Fatalf("first commit=%+v err=%v", first, err)
	}
	replay, err := store.Commit(context.Background(), req)
	if err != nil {
		t.Fatalf("replay commit err=%v", err)
	}
	if replay.Status != authoritystore.CommitAlreadyCommitted {
		t.Fatalf("replay status=%s want ALREADY_COMMITTED", replay.Status)
	}
	lookup, err := store.LookupIdempotencyOutcome(context.Background(), authoritystore.IdempotencyLookupRequest{
		ActorAddress: addr, IdempotencyKey: req.IdempotencyKey,
		CanonicalPayloadHash: req.CanonicalPayloadHash,
	})
	if err != nil || lookup.Status != authoritystore.IdempotencyFoundMatch {
		t.Fatalf("lookup=%+v err=%v", lookup, err)
	}
	t.Logf("Q2R2-7 OK commit then same-identity replay resolved ALREADY_COMMITTED; lookup status=%d", lookup.Status)
}

// TestLiveQ2R2CleanupUsesExactKeys proves cleanup deletes only the EXACT keys
// the probe created (never KEYS / global SCAN) and verifies each via
// EXISTS==0.
func TestLiveQ2R2CleanupUsesExactKeys(t *testing.T) {
	client := q2r2LiveClient(t)
	store, shardKey, _, addr, shard := q2r2Store(t, client)
	keys := newQ2R2Keys(t, client)
	keys.track(shardKey)

	claim, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: routeManager(), TransitionID: q2r2ID("q2r2-clean"), AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: shard,
		NewHolderID: "q2r2-pod-1", NewHolderBootID: "q2r2-boot-1", RequestedTTLms: 8000,
	})
	if err != nil || claim.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	n, err := client.Exists(context.Background(), shardKey.String()).Result()
	if err != nil || n != 1 {
		t.Fatalf("exact shard key must exist after claim: n=%d err=%v", n, err)
	}
	if err := client.Del(context.Background(), shardKey.String()).Err(); err != nil {
		t.Fatal(err)
	}
	n, err = client.Exists(context.Background(), shardKey.String()).Result()
	if err != nil || n != 0 {
		t.Fatalf("exact key must be gone after DEL: n=%d err=%v", n, err)
	}
	t.Logf("Q2R2-CLEANUP OK exact-key DEL + EXISTS == 0 (no KEYS/SCAN)")
}

// TestLiveQ2R2ClusterKeySlotReachability determines whether the connected
// Redis is a direct cluster.  On a proxy profile this is INCONCLUSIVE
// (CLUSTER commands are hidden): MOVED/ASK and primary routing stay
// PROBE_PENDING until a direct company cluster is available.
func TestLiveQ2R2ClusterKeySlotReachability(t *testing.T) {
	client := q2r2LiveClient(t)
	info, err := client.ClusterInfo(context.Background()).Result()
	if err != nil {
		t.Logf("Q2R2-8 cluster routing unavailable on this profile: %v", err)
		t.Skipf("CLUSTER_INFO unavailable (proxy/non-cluster profile): %v", err)
	}
	_ = info
	slotA, err := client.Do(context.Background(), "CLUSTER", "KEYSLOT", "classic-farm:{q2r2:10001}:a").Int()
	if err != nil {
		t.Skipf("CLUSTER KEYSLOT unavailable: %v", err)
	}
	slotB, err := client.Do(context.Background(), "CLUSTER", "KEYSLOT", "classic-farm:{q2r2:10001}:b").Int()
	if err != nil {
		t.Skipf("CLUSTER KEYSLOT unavailable: %v", err)
	}
	if slotA != slotB {
		t.Fatalf("same hash tag must share a slot: a=%d b=%d", slotA, slotB)
	}
	t.Logf("Q2R2-8 OK cluster reachable; slot=%d for hash-tagged key", slotA)
}
