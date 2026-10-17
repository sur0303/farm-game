package authority

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

// Live tests run only when FARM_REDIS_URL points at a real Redis, e.g.:
//
//	FARM_REDIS_URL='redis://:developer_003@example.invalid:6380/0' \
//	  go test ./internal/storage/redis/authority/ -run Live -v
//
// Passing here is MINIREDIS_PASS/UNIT evidence only; REAL_REDIS_PASS requires
// running these against the company platform Redis with raw output archived.
func liveClient(t *testing.T) *redis.Client {
	t.Helper()
	uri := os.Getenv("FARM_REDIS_URL")
	if uri == "" {
		t.Skip("FARM_REDIS_URL not set; skipping live redis authority test")
	}
	options, err := redis.ParseURL(uri)
	if err != nil {
		t.Fatalf("parse FARM_REDIS_URL: %v", err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return client
}

func TestLiveAuthorityRoundTrip(t *testing.T) {
	client := liveClient(t)
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
	addr := authoritystore.ActorAddress{Domain: "PLAYER", ActorID: 99000001}
	bucket, err := testProfile(t).AuthorityBucketForActor(testCalculator(t), addr)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "rm-live", BootID: "rm-live-boot",
		},
		TransitionID:           "live-transition-1",
		AuthorityLayoutVersion: "v1",
		Domain:                 addr.Domain,
		VirtualShard:           bucket.VirtualShard(),
		NewHolderID:            "live-pod-1",
		NewHolderBootID:        "live-boot-1",
		RequestedTTLms:         30_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if claim.Status != authoritystore.ShardTransitionApplied &&
		claim.Status != authoritystore.ShardTransitionAlreadyApplied {
		t.Fatalf("claim=%s", claim.Status)
	}
	t.Logf("live shard claim: status=%s writer_epoch=%d slot bucket=%d",
		claim.Status, claim.Authority.WriterEpoch, bucket.VirtualShard())
}

// TestLiveCareWorkLeaseClaimComplete exercises Care SourceOrder + DueIndex +
// ClaimWorkV5 + CompleteWorkV5 against company Redis TIME/EVALSHA (not miniredis).
func TestLiveCareWorkLeaseClaimComplete(t *testing.T) {
	client := liveClient(t)
	namespace, err := rediskey.NewNamespace(fmt.Sprintf("live%d", time.Now().Unix()%1_000_000), "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "live-care-v1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 64,
	}
	if err := profile.Validate(); err != nil {
		t.Fatal(err)
	}
	calculator, err := actorrouting.NewCalculator(
		"live-care-realm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: testHashSeed},
		64, 64,
	)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(client, nil, Options{Profile: profile, Resolver: calculator})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Registry().EnsureLoaded(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Mirror unit-test subject root (player 10001) under an isolated live namespace.
	addr := authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 10001}
	bucket, err := profile.AuthorityBucketForActor(calculator, addr)
	if err != nil {
		t.Fatal(err)
	}
	shardClaim, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "rm-live-care", BootID: "rm-live-care-boot",
		},
		TransitionID:           fmt.Sprintf("live-care-transition-%d", time.Now().UnixNano()),
		AuthorityLayoutVersion: "v1",
		Domain:                 addr.Domain,
		VirtualShard:           bucket.VirtualShard(),
		NewHolderID:            "live-care-pod",
		NewHolderBootID:        "live-care-boot",
		RequestedTTLms:         60_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if shardClaim.Status != authoritystore.ShardTransitionApplied &&
		shardClaim.Status != authoritystore.ShardTransitionAlreadyApplied {
		t.Fatalf("shard claim=%s", shardClaim.Status)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID:           shardClaim.Authority.HolderID,
		ExpectedHolderBootID:       shardClaim.Authority.HolderBootID,
		ExpectedAssignmentRevision: shardClaim.Authority.AssignmentRevision,
		ExpectedWriterEpoch:        shardClaim.Authority.WriterEpoch,
		ExpectedLeaseEpoch:         shardClaim.Authority.LeaseEpoch,
	}
	loaded, err := store.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
		ActorAddress: addr, AuthorityLayoutVersion: "v1", ShardWriteFence: fence,
		ClaimOperationID: "live-care-load", NewWriterClaimID: "live-care-writer",
		InitialModelFields: authoritystore.ModelState{"seed": []byte("live-care")},
	})
	if err != nil || loaded.Status != authoritystore.LoadAndClaimClaimed {
		t.Fatalf("load=%+v err=%v", loaded, err)
	}

	operationID := fmt.Sprintf("live-care-op-%d", time.Now().UnixNano())
	workID, err := workidentity.StableOpaqueID("livecare", "CARE_REWARD", operationID)
	if err != nil {
		t.Fatal(err)
	}
	order := authoritystore.AttachedOrder{
		OrderID: workID, OrderType: "CARE_REWARD",
		Payload: []byte("payload:" + operationID), DueAtMs: time.Now().UnixMilli() - 1,
	}
	commit := commitReqFor(addr, fence, "live-care-writer", loaded.Snapshot.RecordVersion, operationID,
		authoritystore.ModelState{"care": []byte("resolved")})
	commit.AttachedOrders = []authoritystore.AttachedOrder{order}
	committed, err := store.Commit(context.Background(), commit)
	if err != nil || committed.Status != authoritystore.CommitCommitted {
		t.Fatalf("commit=%+v err=%v", committed, err)
	}

	lease := store.WorkLease()
	dueAt, found, err := lease.DueIndexScore(context.Background(), bucket, order.OrderType, order.OrderID)
	if err != nil || !found || dueAt != order.DueAtMs {
		t.Fatalf("due index score=%d found=%v err=%v", dueAt, found, err)
	}
	claim, err := lease.ClaimWorkWithBoot(context.Background(), bucket, order.OrderType, order.OrderID,
		"live-worker", "live-worker-boot", "live-token", 30_000)
	if err != nil || claim.Status != WorkClaimClaimed {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	payloadHash := sha256.Sum256(order.Payload)
	proof := WorkCompletionProof{
		SourceAuthorityKind: "PLAYER_ACTOR", CanonicalSourceAuthorityID: fmt.Sprint(addr.ActorID),
		AuthorityLayoutVersion: "v1", ActorRouteDomain: "PLAYER",
		PayloadHash: fmt.Sprintf("%x", payloadHash[:]), InvocationID: "live-invocation",
		ParticipantStep: "APPLY_CARE_REWARD", ParticipantOutcome: "APPLIED",
		ParticipantResultHash: fmt.Sprintf("%064x", 1), ParticipantResultVersion: 1,
	}
	proof.ResultProofHash = WorkCompletionProofHash(proof)
	complete, err := lease.CompleteWorkWithProof(context.Background(), bucket, order.OrderType, order.OrderID,
		"live-worker", "live-worker-boot", claim.LeaseEpoch, claim.WorkVersion, "live-token", proof)
	if err != nil || complete.Status != WorkLeaseCompleted {
		t.Fatalf("complete=%+v err=%v", complete, err)
	}
	snapshot, err := lease.GetSourceOrder(context.Background(), bucket, order.OrderType, order.OrderID)
	if err != nil || snapshot.State != "COMPLETED" {
		t.Fatalf("source order after complete=%+v err=%v", snapshot, err)
	}
	if _, found, err := lease.DueIndexScore(context.Background(), bucket, order.OrderType, order.OrderID); err != nil || found {
		t.Fatalf("completed work must leave due index found=%v err=%v", found, err)
	}
	t.Logf("live Care lease: ns=%s actor=%d work=%s claim_epoch=%d completed via Redis TIME/EVALSHA",
		namespace.Environment, addr.ActorID, order.OrderID, claim.LeaseEpoch)
}

// TestLiveActorModelV2Migration exercises the exact migration Lua against a
// real Redis when explicitly configured. It uses a unique namespace and
// deletes only its exact keys; it never uses FLUSHALL or a broad key scan.
func TestLiveActorModelV2Migration(t *testing.T) {
	client := liveClient(t)
	ctx := context.Background()
	namespace, err := rediskey.NewNamespace(fmt.Sprintf("m1a%d", time.Now().UnixNano()%1_000_000_000), "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "live-m1a-v1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 64,
	}
	calculator, err := actorrouting.NewCalculator("live-m1a-realm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: testHashSeed}, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(client, nil, Options{Profile: profile, Resolver: calculator})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Registry().EnsureLoaded(ctx); err != nil {
		t.Fatal(err)
	}
	address := authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 10001}
	bucket, err := profile.AuthorityBucketForActor(calculator, address)
	if err != nil {
		t.Fatal(err)
	}
	shardKey, _ := store.Keys().ShardAuthority(bucket)
	actorKey, _ := store.Keys().ActorRecord(bucket, address.ActorID)
	dueKey, _ := store.Keys().SourceDueIndex(bucket)
	receiptDueKey, _ := store.Keys().ReceiptDueIndex(bucket)
	bucketMetaKey, _ := store.Keys().BucketMeta(bucket)
	t.Cleanup(func() {
		_ = client.Del(context.Background(), shardKey.String(), actorKey.String(), dueKey.String(), receiptDueKey.String(), bucketMetaKey.String()).Err()
	})

	claim, err := store.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "rm-live-m1a", BootID: "rm-live-m1a-boot",
		},
		TransitionID: "live-m1a-transition", AuthorityLayoutVersion: "v1",
		Domain: address.Domain, VirtualShard: bucket.VirtualShard(),
		NewHolderID: "live-m1a-pod", NewHolderBootID: "live-m1a-boot", RequestedTTLms: 60_000,
	})
	if err != nil || claim.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID: claim.Authority.HolderID, ExpectedHolderBootID: claim.Authority.HolderBootID,
		ExpectedAssignmentRevision: claim.Authority.AssignmentRevision,
		ExpectedWriterEpoch:        claim.Authority.WriterEpoch, ExpectedLeaseEpoch: claim.Authority.LeaseEpoch,
	}
	loaded, err := store.LoadAndClaim(ctx, authoritystore.LoadAndClaimRequest{
		ActorAddress: address, AuthorityLayoutVersion: "v1", ShardWriteFence: fence,
		ClaimOperationID: "live-m1a-load", NewWriterClaimID: "live-m1a-writer",
		InitialModelFields: authoritystore.ModelState{"record": []byte("live-v1-record")},
	})
	if err != nil || loaded.Status != authoritystore.LoadAndClaimClaimed {
		t.Fatalf("load=%+v err=%v", loaded, err)
	}
	candidate := redisPlayerV2DummyState()
	mutation, err := authoritystore.BuildActorModelMutation(authoritystore.ModelMutationIntentMigrateV1ToV2,
		authoritystore.ModelLayoutV2, candidate, candidate, []string{"record"})
	if err != nil {
		t.Fatal(err)
	}
	commit := commitReqFor(address, fence, "live-m1a-writer", loaded.Snapshot.RecordVersion, "live-m1a-migrate", candidate)
	commit.ModelMutation = mutation
	committed, err := store.Commit(ctx, commit)
	if err != nil || committed.Status != authoritystore.CommitCommitted ||
		committed.Snapshot.ModelLayout != authoritystore.ModelLayoutV2 ||
		committed.Snapshot.RecordVersion != loaded.Snapshot.RecordVersion+1 {
		t.Fatalf("commit=%+v err=%v", committed, err)
	}
	if exists, existsErr := client.HExists(ctx, actorKey.String(), "model:record").Result(); existsErr != nil || exists {
		t.Fatalf("model:record exists=%v err=%v", exists, existsErr)
	}
	names, err := fieldv2.ExactNames(actorrouting.DomainPlayer)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := store.BatchReadModelFields(ctx, actorrouting.DomainPlayer, []uint64{address.ActorID}, names)
	if err != nil || len(rows) != 1 || rows[0].Err != nil {
		t.Fatalf("v2 batch=%+v err=%v", rows, err)
	}
	t.Logf("live model migration: ns=%s actor=%d record_version=%d", namespace.Environment, address.ActorID, committed.Snapshot.RecordVersion)
}
