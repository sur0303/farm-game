package maildeliveryrecovery

import (
	"context"
	"crypto/sha256"
	"fmt"
	"reflect"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/identity"
	"farmgame/internal/player/mail"
	"farmgame/internal/storage/authoritystore"
	authority "farmgame/internal/storage/redis/authority"
	redisplayermail "farmgame/internal/storage/redis/playermail"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisMailDeliveryOrderRecovery(t *testing.T) {
	const playerID = uint64(10001)
	now := time.UnixMilli(1_777_000_000_000)
	server := miniredis.RunT(t)
	server.SetTime(now)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()

	namespace, err := rediskey.NewNamespace("maildelivery", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "mail-delivery-test-v1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 64,
	}
	calculator, err := actorrouting.NewCalculator("mail-delivery-test",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608130101}, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	authorityStore, err := authority.NewStore(client, nil, authority.Options{Profile: profile, Resolver: calculator})
	if err != nil {
		t.Fatal(err)
	}
	address := authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: playerID}
	bucket, err := profile.AuthorityBucketForActor(calculator, address)
	if err != nil {
		t.Fatal(err)
	}
	shardClaim, err := authorityStore.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-mail-delivery", BootID: "route-manager-mail-delivery-boot",
		},
		TransitionID: "mail-delivery-transition", AuthorityLayoutVersion: "v1",
		Domain: actorrouting.DomainPlayer, VirtualShard: bucket.VirtualShard(),
		NewHolderID: "player-mail-holder", NewHolderBootID: "player-mail-holder-boot", RequestedTTLms: 60_000,
	})
	if err != nil || shardClaim.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("claim=%+v err=%v", shardClaim, err)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID: shardClaim.Authority.HolderID, ExpectedHolderBootID: shardClaim.Authority.HolderBootID,
		ExpectedAssignmentRevision: shardClaim.Authority.AssignmentRevision,
		ExpectedWriterEpoch:        shardClaim.Authority.WriterEpoch, ExpectedLeaseEpoch: shardClaim.Authority.LeaseEpoch,
	}
	loaded, err := authorityStore.LoadAndClaim(ctx, authoritystore.LoadAndClaimRequest{
		ActorAddress: address, AuthorityLayoutVersion: "v1", ShardWriteFence: fence,
		ClaimOperationID: "mail-delivery-load", NewWriterClaimID: "mail-delivery-writer",
		InitialModelFields: authoritystore.ModelState{"player": []byte("initial")},
	})
	if err != nil || loaded.Status != authoritystore.LoadAndClaimClaimed {
		t.Fatalf("load=%+v err=%v", loaded, err)
	}

	mailStore, err := redisplayermail.NewStore(client, redisplayermail.Profile{Namespace: namespace, BucketCount: 64}, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := mailStore.EnsureLoaded(ctx); err != nil {
		t.Fatal(err)
	}
	entry, err := mail.BuildMailEntry(mail.DeliveryParams{
		RecipientPlayerID: playerID, DeliveryID: "SYSTEM:10001:welcome:1",
		SourceType: mail.SourceTypeSystem, SourceID: "welcome-v1",
		TitleSnapshot: "Welcome", BodySnapshot: "Frozen welcome body",
		Reward: mail.RewardBundle{Coin: 10, Exp: 2}, TemplateVersion: "welcome-v1",
		RewardConfigVersion: "mail-reward-v1", CreatedAtMs: now.UnixMilli(), VisibleAtMs: now.UnixMilli(),
	}, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := mail.EncodeMailEntry(entry, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	workID, err := identity.SourceOrderID(identity.WorkTypeMailDeliveryOrder, identity.SourceKindPlayerActor,
		fmt.Sprint(playerID), entry.DeliveryID, 1)
	if err != nil {
		t.Fatal(err)
	}
	payloadHash := sha256.Sum256(raw)
	operationID := "mail-delivery-operation"
	idempotency := authoritystore.IdempotencyKey{
		SubjectPlayerID: playerID, StableCommandTypeName: "WELCOME_MAIL", OperationID: operationID,
		Root: authoritystore.OperationRootRef{Kind: authoritystore.RootKindPlayerActor, CanonicalID: fmt.Sprint(playerID)},
	}
	canonicalPayloadHash := authoritystore.PayloadHash(sha256.Sum256([]byte("welcome-mail-command")))
	storedOutcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded, Payload: []byte("created")}
	commit, err := authorityStore.Commit(ctx, authoritystore.AuthorityCommitRequest{
		CommitRequestID: "mail-delivery-commit", ActorAddress: address, AuthorityLayoutVersion: "v1",
		ShardWriteFence: fence, ExpectedWriterClaimID: "mail-delivery-writer", ExpectedRecordVersion: loaded.Snapshot.RecordVersion,
		IdempotencyKey: idempotency, CanonicalPayloadHash: canonicalPayloadHash,
		CandidateState: authoritystore.ModelState{"player": []byte("welcome-attached")},
		Receipt: authoritystore.AuthorityReceipt{ReceiptID: "mail-delivery-receipt", IdempotencyKey: idempotency,
			CanonicalPayloadHash: canonicalPayloadHash, Outcome: storedOutcome},
		OperationResult: storedOutcome,
		AttachedOrders: []authoritystore.AttachedOrder{{
			OrderID: workID, OrderType: FamilyMailDeliveryOrder, Payload: raw, PayloadHash: payloadHash,
			DueAtMs: now.UnixMilli(), SubjectPlayerID: playerID,
		}},
	})
	if err != nil || commit.Status != authoritystore.CommitCommitted {
		t.Fatalf("commit=%+v err=%v", commit, err)
	}

	worker, err := NewWorker(Config{
		AuthorityStore: authorityStore, MailEntries: mailStore, Limits: mail.LimitsV1(),
		Buckets: []rediskey.AuthorityBucketRef{bucket}, OwnedBuckets: []rediskey.AuthorityBucketRef{bucket},
		Owner: "mail-delivery-worker", Token: "mail-delivery-token", BootID: "mail-delivery-boot",
		ClaimLimit: 8, MaxConcurrent: 1, LeaseTTL: 10 * time.Second, SafetyMargin: time.Second,
		OperationTimeout: 2 * time.Second, RetryDelay: time.Millisecond, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := worker.RunOnce(ctx)
	if err != nil || stats.Inserted != 1 || stats.Completed != 1 || stats.Rescheduled != 0 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	got, _, err := mailStore.GetEntry(ctx, playerID, entry.MailID)
	if err != nil || !reflect.DeepEqual(got, entry) {
		t.Fatalf("entry=%+v want=%+v err=%v", got, entry, err)
	}
	source, err := authorityStore.WorkLease().GetSourceOrder(ctx, bucket, FamilyMailDeliveryOrder, workID)
	if err != nil || source.State != "COMPLETED" {
		t.Fatalf("source=%+v err=%v", source, err)
	}
	due, err := worker.ObserveDue(ctx, 8)
	if err != nil || !due.Available || due.Depth != 0 {
		t.Fatalf("due=%+v err=%v", due, err)
	}
}
