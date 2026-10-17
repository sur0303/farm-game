package authority

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/redis/go-redis/v9"
)

// TestLiveRedisAttachedScheduleProbe is opt-in and uses an isolated namespace.
// It is intentionally skipped in normal CI. Run with:
//
//	FARM_REDIS_LIVE_PROBE=1 FARM_REDIS_URL=... go test -run TestLiveRedisAttachedScheduleProbe ./internal/storage/redis/authority
//
// The probe deletes only the five exact keys it derives in its unique
// codexprobe namespace; it never scans or flushes the shared Redis instance.
func TestLiveRedisAttachedScheduleProbe(t *testing.T) {
	if os.Getenv("FARM_REDIS_LIVE_PROBE") != "1" {
		t.Skip("set FARM_REDIS_LIVE_PROBE=1 to use an explicitly supplied Redis")
	}
	rawURL := os.Getenv("FARM_REDIS_URL")
	if rawURL == "" {
		t.Fatal("FARM_REDIS_URL is required")
	}
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	zone := "p" + strconv.FormatInt(time.Now().Unix(), 10)
	namespace, err := rediskey.NewNamespace("codexprobe", zone)
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "live-probe-v1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 1,
	}
	calculator, err := actorrouting.NewCalculator("live-probe",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: testHashSeed}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(client, nil, Options{Profile: profile, Resolver: calculator})
	if err != nil {
		t.Fatal(err)
	}
	actorID := uint64(9_990_001)
	address := authoritystore.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: actorID}
	bucket, err := profile.AuthorityBucketForActor(calculator, address)
	if err != nil {
		t.Fatal(err)
	}
	schedule := testSchedule(t, "live-opportunity", 1, time.Now().UnixMilli(), "live-wakeup")
	keys := store.Keys()
	shardKey, _ := keys.ShardAuthority(bucket)
	actorKey, _ := keys.ActorRecord(bucket, actorID)
	scheduleKey, _ := keys.SourceSchedule(bucket, schedule.ScheduleType, schedule.ScheduleID)
	dueKey, _ := keys.SourceDueIndex(bucket)
	metaKey, _ := keys.BucketMeta(bucket)
	exactKeys := []string{shardKey.String(), actorKey.String(), scheduleKey.String(), dueKey.String(), metaKey.String()}
	cleanup := func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := client.Del(cleanupCtx, exactKeys...).Err(); err != nil {
			t.Logf("live probe exact-key cleanup: %v", err)
		}
	}
	cleanup()
	defer cleanup()

	claim, err := store.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "live-route-manager", BootID: "live-route-boot",
		},
		TransitionID: "live-transition", AuthorityLayoutVersion: "v1", Domain: address.Domain,
		VirtualShard: bucket.VirtualShard(), NewHolderID: "live-farm-holder", NewHolderBootID: "live-farm-boot",
		RequestedTTLms: 60_000,
	})
	if err != nil || claim.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("claim shard status=%s err=%v", claim.Status, err)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID: claim.Authority.HolderID, ExpectedHolderBootID: claim.Authority.HolderBootID,
		ExpectedAssignmentRevision: claim.Authority.AssignmentRevision, ExpectedWriterEpoch: claim.Authority.WriterEpoch,
		ExpectedLeaseEpoch: claim.Authority.LeaseEpoch,
	}
	loaded, err := store.LoadAndClaim(ctx, authoritystore.LoadAndClaimRequest{
		ActorAddress: address, AuthorityLayoutVersion: "v1", ShardWriteFence: fence,
		ClaimOperationID: "live-actor-claim", NewWriterClaimID: "live-writer-claim",
		InitialModelFields: authoritystore.ModelState{"initial": []byte("live")},
	})
	if err != nil || loaded.Status != authoritystore.LoadAndClaimClaimed {
		t.Fatalf("load and claim status=%s err=%v", loaded.Status, err)
	}
	request := commitReqFor(address, fence, "live-writer-claim", loaded.Snapshot.RecordVersion,
		"live-schedule-commit", authoritystore.ModelState{"state": []byte("live-v1")})
	request.AttachedSchedules = []authoritystore.AttachedSchedule{schedule}
	committed, err := store.Commit(ctx, request)
	if err != nil || committed.Status != authoritystore.CommitCommitted {
		t.Fatalf("commit schedule status=%s err=%v", committed.Status, err)
	}
	lease := store.WorkLease()
	nowMs, err := lease.TrustedNowMs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	due, err := lease.ListDueScheduledWork(ctx, bucket, nowMs+1_000, 8)
	if err != nil || len(due) != 1 {
		t.Fatalf("due=%+v err=%v", due, err)
	}
	workClaim, err := lease.ClaimScheduledWorkWithBoot(ctx, bucket, schedule.ScheduleType, schedule.ScheduleID,
		"live-worker", "live-worker-boot", "live-token", 10_000)
	if err != nil || workClaim.Status != WorkClaimClaimed {
		t.Fatalf("claim schedule=%+v err=%v", workClaim, err)
	}
	completed, err := lease.CompleteScheduledWork(ctx, bucket, schedule.ScheduleType, schedule.ScheduleID,
		"live-worker", "live-worker-boot", workClaim.LeaseEpoch, workClaim.WorkVersion, workClaim.Generation, "live-token")
	if err != nil || completed.Status != WorkLeaseCompleted {
		t.Fatalf("complete schedule=%+v err=%v", completed, err)
	}
}
