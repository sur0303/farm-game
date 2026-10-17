package authority

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

type noRetryInspectClient struct {
	redis.UniversalClient

	mu         sync.Mutex
	commands   int
	allNoRetry bool
}

func (c *noRetryInspectClient) Process(ctx context.Context, command redis.Cmder) error {
	c.mu.Lock()
	c.commands++
	if !command.NoRetry() {
		c.allNoRetry = false
	}
	c.mu.Unlock()
	return c.UniversalClient.Process(ctx, command)
}

func (c *noRetryInspectClient) observation() (commands int, allNoRetry bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.commands, c.allNoRetry
}

func (c *noRetryInspectClient) resetObservation() {
	c.mu.Lock()
	c.commands = 0
	c.allNoRetry = true
	c.mu.Unlock()
}

func TestShardTransitionCommandsDisableAutomaticRedisRetries(t *testing.T) {
	rig := newTestRig(t)
	client := &noRetryInspectClient{UniversalClient: rig.client, allNoRetry: true}
	store, err := NewStore(client, nil, Options{
		Profile:  testProfile(t),
		Resolver: testCalculator(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Registry().EnsureLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}
	client.resetObservation()

	result, err := store.TransferShardAuthority(context.Background(), authoritystore.TransferShardAuthorityRequest{
		Caller:                 routeManager(),
		TransitionID:           "transfer-no-client-retry",
		AuthorityLayoutVersion: "v1",
		Domain:                 rig.addr.Domain,
		VirtualShard:           rig.bucket.VirtualShard(),
		ExpectedFence:          rig.fence,
		NewHolderID:            "player-pod-no-retry",
		NewHolderBootID:        "boot-no-retry",
		RequestedTTLms:         60_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("status=%s", result.Status)
	}
	if commands, allNoRetry := client.observation(); commands != 1 || !allNoRetry {
		t.Fatalf("commands=%d all_no_retry=%t", commands, allNoRetry)
	}
}

func TestShardTransitionsReportCanceledPreSendAsDefinitiveNotSent(t *testing.T) {
	rig := newTestRig(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	operations := map[string]func() (authoritystore.ShardAuthorityTransitionResult, error){
		"claim": func() (authoritystore.ShardAuthorityTransitionResult, error) {
			return rig.store.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{})
		},
		"renew": func() (authoritystore.ShardAuthorityTransitionResult, error) {
			return rig.store.RenewShardAuthority(ctx, authoritystore.RenewShardAuthorityRequest{})
		},
		"transfer": func() (authoritystore.ShardAuthorityTransitionResult, error) {
			return rig.store.TransferShardAuthority(ctx, authoritystore.TransferShardAuthorityRequest{})
		},
		"expired-takeover": func() (authoritystore.ShardAuthorityTransitionResult, error) {
			return rig.store.TakeoverExpiredShardAuthority(ctx, authoritystore.ExpiredTakeoverShardAuthorityRequest{})
		},
		"unassign": func() (authoritystore.ShardAuthorityTransitionResult, error) {
			return rig.store.UnassignShardAuthority(ctx, authoritystore.UnassignShardAuthorityRequest{})
		},
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			result, err := operation()
			if result.Status != authoritystore.ShardTransitionUnavailableNotSent || !errors.Is(err, context.Canceled) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
}

func TestShardAuthorityPreMutationLayoutMismatchIsDefinitiveNotSent(t *testing.T) {
	operations := map[string]func(*testRig) (authoritystore.ShardAuthorityTransitionResult, error){
		"claim": func(rig *testRig) (authoritystore.ShardAuthorityTransitionResult, error) {
			return rig.store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
				Caller: routeManager(), TransitionID: "layout-claim", AuthorityLayoutVersion: "v1",
				Domain: rig.addr.Domain, VirtualShard: rig.bucket.VirtualShard(), NewHolderID: "holder-layout", NewHolderBootID: "boot-layout", RequestedTTLms: 60_000,
			})
		},
		"transfer": func(rig *testRig) (authoritystore.ShardAuthorityTransitionResult, error) {
			return rig.store.TransferShardAuthority(context.Background(), authoritystore.TransferShardAuthorityRequest{
				Caller: routeManager(), TransitionID: "layout-transfer", AuthorityLayoutVersion: "v1",
				Domain: rig.addr.Domain, VirtualShard: rig.bucket.VirtualShard(), ExpectedFence: rig.fence,
				NewHolderID: "holder-layout", NewHolderBootID: "boot-layout", RequestedTTLms: 60_000,
			})
		},
		"expired-takeover": func(rig *testRig) (authoritystore.ShardAuthorityTransitionResult, error) {
			return rig.store.TakeoverExpiredShardAuthority(context.Background(), authoritystore.ExpiredTakeoverShardAuthorityRequest{
				Caller: routeManager(), TransitionID: "layout-expired-takeover", AuthorityLayoutVersion: "v1",
				Domain: rig.addr.Domain, VirtualShard: rig.bucket.VirtualShard(), ExpectedFence: rig.fence,
				NewHolderID: "holder-layout", NewHolderBootID: "boot-layout", RequestedTTLms: 60_000,
			})
		},
		"unassign": func(rig *testRig) (authoritystore.ShardAuthorityTransitionResult, error) {
			return rig.store.UnassignShardAuthority(context.Background(), authoritystore.UnassignShardAuthorityRequest{
				Caller: routeManager(), TransitionID: "layout-unassign", AuthorityLayoutVersion: "v1",
				Domain: rig.addr.Domain, VirtualShard: rig.bucket.VirtualShard(), ExpectedFence: rig.fence,
			})
		},
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			rig := newTestRig(t)
			key, err := rig.store.Keys().ShardAuthority(rig.bucket)
			if err != nil {
				t.Fatal(err)
			}
			if err := rig.client.HSet(context.Background(), key.String(), "authority_layout_version", "legacy-layout").Err(); err != nil {
				t.Fatal(err)
			}
			result, err := operation(rig)
			if result.Status != authoritystore.ShardTransitionUnavailableNotSent || !errors.Is(err, authoritystore.ErrAuthorityAddressMismatch) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if got, getErr := rig.client.HGet(context.Background(), key.String(), "last_transition_id").Result(); getErr != nil || got != "transition-1" {
				t.Fatalf("pre-mutation mismatch changed authority transition=%q err=%v", got, getErr)
			}
		})
	}
}

func TestDecodeShardReplyDoesNotInferNotSentFromMalformedReply(t *testing.T) {
	rig := newTestRig(t)
	for _, reply := range [][]string{{"UNAVAILABLE_NOT_SENT"}, {"UNAVAILABLE_NOT_SENT", "UNKNOWN"}, {"APPLIED"}} {
		result, err := rig.store.decodeShardReply(rig.addr.Domain, rig.bucket.VirtualShard(), reply)
		if err == nil || result.Status == authoritystore.ShardTransitionUnavailableNotSent {
			t.Fatalf("reply=%q result=%+v err=%v", reply, result, err)
		}
	}
}

func routeManager() authoritystore.TrustedShardAuthorityCaller {
	return authoritystore.TrustedShardAuthorityCaller{
		Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "rm-1", BootID: "rm-boot",
	}
}

func TestFarmWorkerShardAuthorityUsesIndependentRedisKeyspace(t *testing.T) {
	rig := newTestRig(t)
	worker, err := rig.store.ClaimShardAuthority(
		context.Background(),
		authoritystore.ClaimShardAuthorityRequest{
			Caller:                 routeManager(),
			TransitionID:           "worker-claim",
			AuthorityLayoutVersion: "v1",
			Domain:                 actorrouting.DomainFarmWorker,
			VirtualShard:           rig.bucket.VirtualShard(),
			NewHolderID:            "farm-worker-a",
			NewHolderBootID:        "worker-a-boot",
			RequestedTTLms:         60_000,
		},
	)
	if err != nil || worker.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("worker claim=%+v err=%v", worker, err)
	}
	workerBucket, err := rig.store.opts.Profile.AuthorityBucketForShard(
		rig.store.opts.Resolver,
		actorrouting.DomainFarmWorker,
		rig.bucket.VirtualShard(),
	)
	if err != nil {
		t.Fatal(err)
	}
	workerKey, err := rig.store.Keys().ShardAuthority(workerBucket)
	if err != nil {
		t.Fatal(err)
	}
	actorKey, err := rig.store.Keys().ShardAuthority(rig.bucket)
	if err != nil {
		t.Fatal(err)
	}
	if workerKey == actorKey {
		t.Fatal("FARM_WORKER and actor shard-authority keys are identical")
	}
	if got, err := rig.client.HGet(
		context.Background(),
		workerKey.String(),
		"holder_instance_id",
	).Result(); err != nil || got != "farm-worker-a" {
		t.Fatalf("worker holder=%q err=%v", got, err)
	}
	if got, err := rig.client.HGet(
		context.Background(),
		actorKey.String(),
		"holder_instance_id",
	).Result(); err != nil || got != rig.fence.ExpectedHolderID {
		t.Fatalf("actor holder changed=%q err=%v", got, err)
	}
}

func TestShardClaimRenewTransferUnassignLifecycle(t *testing.T) {
	rig := newTestRig(t) // shard already claimed by rig setup (holder player-pod-1)
	fence := rig.fence

	// Holder renew extends the lease.
	renew, err := rig.store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: fence.ExpectedHolderID, BootID: fence.ExpectedHolderBootID,
		},
		AuthorityLayoutVersion: "v1",
		Domain:                 rig.addr.Domain,
		VirtualShard:           rig.bucket.VirtualShard(),
		ExpectedFence:          fence,
		RequestedTTLms:         30_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if renew.Status != authoritystore.ShardTransitionApplied || renew.AcceptedTTLms <= 0 {
		t.Fatalf("renew=%+v", renew)
	}

	// Non-holder renew is rejected without touching state.
	stranger, err := rig.store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller:                 authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: "other", BootID: "x"},
		AuthorityLayoutVersion: "v1",
		Domain:                 rig.addr.Domain,
		VirtualShard:           rig.bucket.VirtualShard(),
		ExpectedFence:          fence,
		RequestedTTLms:         30_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stranger.Status != authoritystore.ShardTransitionUnauthorized {
		t.Fatalf("stranger=%s", stranger.Status)
	}

	// Route manager transfers to a new holder generation.
	transfer, err := rig.store.TransferShardAuthority(context.Background(), authoritystore.TransferShardAuthorityRequest{
		Caller:                 routeManager(),
		TransitionID:           "transfer-1",
		AuthorityLayoutVersion: "v1",
		Domain:                 rig.addr.Domain,
		VirtualShard:           rig.bucket.VirtualShard(),
		ExpectedFence:          fence,
		NewHolderID:            "player-pod-2",
		NewHolderBootID:        "boot-2",
		RequestedTTLms:         60_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if transfer.Status != authoritystore.ShardTransitionApplied ||
		transfer.Authority.HolderID != "player-pod-2" ||
		transfer.Authority.WriterEpoch != fence.ExpectedWriterEpoch+1 {
		t.Fatalf("transfer=%+v", transfer)
	}

	// Idempotent replay of the same transition.
	replay, err := rig.store.TransferShardAuthority(context.Background(), authoritystore.TransferShardAuthorityRequest{
		Caller:                 routeManager(),
		TransitionID:           "transfer-1",
		AuthorityLayoutVersion: "v1",
		Domain:                 rig.addr.Domain,
		VirtualShard:           rig.bucket.VirtualShard(),
		ExpectedFence:          fence,
		NewHolderID:            "player-pod-2",
		NewHolderBootID:        "boot-2",
		RequestedTTLms:         60_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != authoritystore.ShardTransitionAlreadyApplied {
		t.Fatalf("replay=%s", replay.Status)
	}

	// Old holder's fence is stale now.
	staleCommit := rig.commitReq("claim-x", 1, "stale", authoritystore.ModelState{"a": []byte("b")})
	staleCommit.ShardWriteFence = fence
	stale, err := rig.store.Commit(context.Background(), staleCommit)
	if err != nil {
		t.Fatal(err)
	}
	if stale.Status != authoritystore.CommitWriterEpochMismatch {
		t.Fatalf("stale fence commit=%s", stale.Status)
	}

	// Unassign by route manager with the new fence.
	newFence := authoritystore.ShardWriteFence{
		ExpectedHolderID:           transfer.Authority.HolderID,
		ExpectedHolderBootID:       transfer.Authority.HolderBootID,
		ExpectedAssignmentRevision: transfer.Authority.AssignmentRevision,
		ExpectedWriterEpoch:        transfer.Authority.WriterEpoch,
		ExpectedLeaseEpoch:         transfer.Authority.LeaseEpoch,
	}
	unassign, err := rig.store.UnassignShardAuthority(context.Background(), authoritystore.UnassignShardAuthorityRequest{
		Caller:                 routeManager(),
		TransitionID:           "unassign-1",
		AuthorityLayoutVersion: "v1",
		Domain:                 rig.addr.Domain,
		VirtualShard:           rig.bucket.VirtualShard(),
		ExpectedFence:          newFence,
	})
	if err != nil {
		t.Fatal(err)
	}
	if unassign.Status != authoritystore.ShardTransitionApplied ||
		unassign.Authority.State != authoritystore.ShardAuthorityUnassigned {
		t.Fatalf("unassign=%+v", unassign)
	}
}

func TestShardClaimConflictsActiveLeaseAndAllowsExpiredTakeover(t *testing.T) {
	rig := newTestRig(t)
	// Second claim while ACTIVE and unexpired: conflict.
	second, err := rig.store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller:                 routeManager(),
		TransitionID:           "claim-2",
		AuthorityLayoutVersion: "v1",
		Domain:                 rig.addr.Domain,
		VirtualShard:           rig.bucket.VirtualShard(),
		NewHolderID:            "pod-2",
		NewHolderBootID:        "boot-2",
		RequestedTTLms:         60_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Status != authoritystore.ShardTransitionActiveLeaseConflict {
		t.Fatalf("second=%s", second.Status)
	}

	// Lease expires (miniredis virtual time) → takeover allowed, epochs bump.
	rig.server.SetTime(testNow.Add(2 * time.Minute))
	takeover, err := rig.store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller:                 routeManager(),
		TransitionID:           "claim-3",
		AuthorityLayoutVersion: "v1",
		Domain:                 rig.addr.Domain,
		VirtualShard:           rig.bucket.VirtualShard(),
		NewHolderID:            "pod-2",
		NewHolderBootID:        "boot-2",
		RequestedTTLms:         60_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if takeover.Status != authoritystore.ShardTransitionApplied ||
		takeover.Authority.WriterEpoch != rig.fence.ExpectedWriterEpoch+1 {
		t.Fatalf("takeover=%+v", takeover)
	}

	// The old holder's fence is now stale; writes must fail closed.
	req := rig.commitReq("claim-1", 1, "old-epoch", authoritystore.ModelState{"a": []byte("b")})
	stale, err := rig.store.Commit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if stale.Status != authoritystore.CommitWriterEpochMismatch {
		t.Fatalf("stale=%s", stale.Status)
	}

	// Renew on an expired lease reports LEASE_EXPIRED.
	rig.server.SetTime(testNow.Add(4 * time.Minute))
	expired, err := rig.store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: "pod-2", BootID: "boot-2",
		},
		AuthorityLayoutVersion: "v1",
		Domain:                 rig.addr.Domain,
		VirtualShard:           rig.bucket.VirtualShard(),
		ExpectedFence: authoritystore.ShardWriteFence{
			ExpectedHolderID:           takeover.Authority.HolderID,
			ExpectedHolderBootID:       takeover.Authority.HolderBootID,
			ExpectedAssignmentRevision: takeover.Authority.AssignmentRevision,
			ExpectedWriterEpoch:        takeover.Authority.WriterEpoch,
			ExpectedLeaseEpoch:         takeover.Authority.LeaseEpoch,
		},
		RequestedTTLms: 60_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if expired.Status != authoritystore.ShardTransitionLeaseExpired {
		t.Fatalf("expired=%s", expired.Status)
	}
}

func TestExplicitExpiredTakeoverRequiresLeaseExpiryAndExactFence(t *testing.T) {
	rig := newTestRig(t)
	req := authoritystore.ExpiredTakeoverShardAuthorityRequest{
		Caller: routeManager(), TransitionID: "explicit-expired-takeover", AuthorityLayoutVersion: "v1",
		Domain: rig.addr.Domain, VirtualShard: rig.bucket.VirtualShard(), ExpectedFence: rig.fence,
		NewHolderID: "pod-recovery", NewHolderBootID: "boot-recovery", RequestedTTLms: 60_000,
	}

	active, err := rig.store.TakeoverExpiredShardAuthority(context.Background(), req)
	if err != nil || active.Status != authoritystore.ShardTransitionActiveLeaseConflict {
		t.Fatalf("active takeover=%+v err=%v", active, err)
	}

	rig.server.SetTime(testNow.Add(2 * time.Minute))
	wrong := req
	wrong.TransitionID = "explicit-expired-wrong-fence"
	wrong.ExpectedFence.ExpectedWriterEpoch++
	wrongResult, err := rig.store.TakeoverExpiredShardAuthority(context.Background(), wrong)
	if err != nil || wrongResult.Status != authoritystore.ShardTransitionFenceMismatch {
		t.Fatalf("wrong fence=%+v err=%v", wrongResult, err)
	}

	taken, err := rig.store.TakeoverExpiredShardAuthority(context.Background(), req)
	if err != nil || taken.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("expired takeover=%+v err=%v", taken, err)
	}
	if taken.Authority.HolderID != "pod-recovery" || taken.Authority.HolderBootID != "boot-recovery" ||
		taken.Authority.WriterEpoch != rig.fence.ExpectedWriterEpoch+1 ||
		taken.Authority.LeaseEpoch != rig.fence.ExpectedLeaseEpoch+1 ||
		taken.Authority.AssignmentRevision != rig.fence.ExpectedAssignmentRevision+1 {
		t.Fatalf("taken=%+v", taken.Authority)
	}
	replay, err := rig.store.TakeoverExpiredShardAuthority(context.Background(), req)
	if err != nil || replay.Status != authoritystore.ShardTransitionAlreadyApplied {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}

	stale, err := rig.store.Commit(context.Background(), rig.commitReq("claim-after-failover", 1, "old-writer", authoritystore.ModelState{"a": []byte("b")}))
	if err != nil || stale.Status != authoritystore.CommitWriterEpochMismatch {
		t.Fatalf("old writer commit=%+v err=%v", stale, err)
	}
}

func TestShardRenewNotFoundAndLayoutGuard(t *testing.T) {
	rig := newTestRig(t)
	// A shard nobody claimed: NOT_FOUND.
	otherShard := (rig.bucket.VirtualShard() + 1) % 64
	renew, err := rig.store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: "pod-x", BootID: "boot-x",
		},
		AuthorityLayoutVersion: "v1",
		Domain:                 actorrouting.DomainPlayer,
		VirtualShard:           otherShard,
		ExpectedFence: authoritystore.ShardWriteFence{
			ExpectedHolderID: "pod-x", ExpectedHolderBootID: "boot-x",
		},
		RequestedTTLms: 60_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if renew.Status != authoritystore.ShardTransitionNotFound {
		t.Fatalf("renew=%s", renew.Status)
	}

	// Wrong layout version fails closed as an address error.
	if _, err := rig.store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller:                 routeManager(),
		TransitionID:           "claim-bad-layout",
		AuthorityLayoutVersion: "v2",
		Domain:                 rig.addr.Domain,
		VirtualShard:           rig.bucket.VirtualShard(),
		NewHolderID:            "pod-2",
		NewHolderBootID:        "boot-2",
		RequestedTTLms:         60_000,
	}); err == nil {
		t.Fatal("layout mismatch must fail closed")
	}
}
