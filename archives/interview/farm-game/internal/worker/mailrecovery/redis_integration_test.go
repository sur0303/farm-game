package mailrecovery

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/actorrouting"
	"farmgame/internal/identity"
	"farmgame/internal/player/mail"
	"farmgame/internal/storage/authoritystore"
	authority "farmgame/internal/storage/redis/authority"
	redisplayermail "farmgame/internal/storage/redis/playermail"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// TestRedisAuthorityOrderToMailboxProjection exercises the physical recovery
// path with real Redis scripts: AuthorityCommit + Source DueIndex + WorkLease
// + PlayerMail projection + completion proof. It catches contract drift that
// isolated fake repositories cannot detect, especially payload_hash encoding.
func TestRedisAuthorityOrderToMailboxProjection(t *testing.T) {
	const playerID = uint64(10001)
	now := time.UnixMilli(1_777_000_000_000)
	server := miniredis.RunT(t)
	server.SetTime(now)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()

	namespace, err := rediskey.NewNamespace("mailrecover", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "mail-recovery-test-v1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 64,
	}
	calculator, err := actorrouting.NewCalculator("mail-recovery-test",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608120101}, 64, 64)
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
			Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-mail-test", BootID: "route-manager-mail-boot",
		},
		TransitionID: "mail-recovery-transition", AuthorityLayoutVersion: "v1",
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
		ClaimOperationID: "mail-recovery-load", NewWriterClaimID: "mail-recovery-writer",
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
		RecipientPlayerID: playerID, DeliveryID: "mail-recovery-delivery", SourceType: mail.SourceTypeTest,
		SourceID: "mail-recovery-source", TitleSnapshot: "recovery reward", BodySnapshot: "recover the projection",
		Reward: mail.RewardBundle{Coin: 10}, TemplateVersion: "mail-template-v1", RewardConfigVersion: "mail-reward-v1",
		CreatedAtMs: now.UnixMilli(), VisibleAtMs: now.UnixMilli(),
	}, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	if inserted, insertErr := mailStore.InsertMailIfAbsent(ctx, entry); insertErr != nil || inserted.Status != mail.InsertInserted {
		t.Fatalf("insert=%+v err=%v", inserted, insertErr)
	}
	receiptRef, err := workidentity.StableOpaqueID("mail-claim-receipt", fmt.Sprint(playerID), entry.MailID)
	if err != nil {
		t.Fatal(err)
	}
	workID, err := identity.SourceOrderID(identity.WorkTypeMailClaimProjectionOrder, identity.SourceKindPlayerActor,
		fmt.Sprint(playerID), entry.MailID, 1)
	if err != nil {
		t.Fatal(err)
	}
	payload := &contractspb.ApplyMailClaimProjectionPayload{
		RecipientPlayerId: playerID, MailId: entry.MailID, ClaimReceiptRef: receiptRef,
		ClaimMaterialHash: entry.ClaimMaterialHash, ClaimedAtMs: now.UnixMilli(),
	}
	rawPayload, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	payloadHash := sha256.Sum256(rawPayload)
	operationID := "mail-recovery-operation"
	idempotency := authoritystore.IdempotencyKey{
		SubjectPlayerID: playerID, StableCommandTypeName: "CLAIM_MAIL_ATTACHMENT", OperationID: operationID,
		Root: authoritystore.OperationRootRef{Kind: authoritystore.RootKindPlayerActor, CanonicalID: fmt.Sprint(playerID)},
	}
	canonicalPayloadHash := authoritystore.PayloadHash(sha256.Sum256([]byte("claim-mail-command")))
	storedOutcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded, Payload: []byte("claimed")}
	commit, err := authorityStore.Commit(ctx, authoritystore.AuthorityCommitRequest{
		CommitRequestID: "mail-recovery-commit", ActorAddress: address, AuthorityLayoutVersion: "v1",
		ShardWriteFence: fence, ExpectedWriterClaimID: "mail-recovery-writer", ExpectedRecordVersion: loaded.Snapshot.RecordVersion,
		IdempotencyKey: idempotency, CanonicalPayloadHash: canonicalPayloadHash,
		CandidateState: authoritystore.ModelState{"player": []byte("attachment-granted")},
		Receipt: authoritystore.AuthorityReceipt{ReceiptID: "mail-recovery-receipt", IdempotencyKey: idempotency,
			CanonicalPayloadHash: canonicalPayloadHash, Outcome: storedOutcome},
		OperationResult: storedOutcome,
		AttachedOrders: []authoritystore.AttachedOrder{{
			OrderID: workID, OrderType: FamilyMailClaimProjection, Payload: rawPayload, PayloadHash: payloadHash,
			DueAtMs: now.UnixMilli(), SubjectPlayerID: playerID,
		}},
	})
	if err != nil || commit.Status != authoritystore.CommitCommitted {
		t.Fatalf("commit=%+v err=%v", commit, err)
	}

	worker, err := NewWorker(Config{
		AuthorityStore: authorityStore, MailEntries: mailStore, Buckets: []rediskey.AuthorityBucketRef{bucket},
		Owner: "mail-recovery-worker", Token: "mail-recovery-token", BootID: "mail-recovery-boot",
		ClaimLimit: 8, MaxConcurrent: 1, LeaseTTL: 10 * time.Second, SafetyMargin: time.Second,
		OperationTimeout: 2 * time.Second, RetryDelay: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := worker.RunOnce(ctx)
	if err != nil || stats.Applied != 1 || stats.Completed != 1 || stats.Rescheduled != 0 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	_, state, err := mailStore.GetEntry(ctx, playerID, entry.MailID)
	if err != nil || !state.ClaimedProjection || state.ClaimReceiptRef != receiptRef {
		t.Fatalf("state=%+v err=%v", state, err)
	}
	source, err := authorityStore.WorkLease().GetSourceOrder(ctx, bucket, FamilyMailClaimProjection, workID)
	if err != nil || source.State != "COMPLETED" {
		t.Fatalf("source=%+v err=%v", source, err)
	}
	if due, err := worker.ObserveDue(ctx, 8); err != nil || !due.Available || due.Depth != 0 {
		t.Fatalf("due=%+v err=%v", due, err)
	}
}
