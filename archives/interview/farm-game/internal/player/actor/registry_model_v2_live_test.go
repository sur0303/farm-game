package actor

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/redis/go-redis/v9"
)

func TestLivePlayerActorModelV2RuntimeMigration(t *testing.T) {
	rawURL := os.Getenv("REDIS_AUTHORITY_LIVE_URL")
	if rawURL == "" {
		t.Skip("REDIS_AUTHORITY_LIVE_URL is unset")
	}
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	zone := fmt.Sprintf("m1b%d", time.Now().UnixNano())
	namespace, err := rediskey.NewNamespace("m1blive", zone)
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "p1", Namespace: namespace, AuthorityLayoutVersion: "v1",
		WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 64,
	}
	calculator, err := actorrouting.NewCalculator("m1b-live", actorrouting.HashProfile{
		Version: actorrouting.HashVersionXXH64V2, Seed: 2026081002,
	}, 64, 64)
	if err != nil {
		t.Fatal(err)
	}
	const playerID = uint64(39_201)
	keyBuilder, err := rediskey.NewAuthorityKeys(profile, calculator)
	if err != nil {
		t.Fatal(err)
	}
	address := actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: playerID}
	bucket, err := profile.AuthorityBucketForActor(calculator, address)
	if err != nil {
		t.Fatal(err)
	}
	shardKey, err := keyBuilder.ShardAuthority(bucket)
	if err != nil {
		t.Fatal(err)
	}
	actorKey, err := keyBuilder.ActorRecordFor(address)
	if err != nil {
		t.Fatal(err)
	}
	dueKey, err := keyBuilder.SourceDueIndex(bucket)
	if err != nil {
		t.Fatal(err)
	}
	receiptDueKey, err := keyBuilder.ReceiptDueIndex(bucket)
	if err != nil {
		t.Fatal(err)
	}
	bucketMetaKey, err := keyBuilder.BucketMeta(bucket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Unlink(context.Background(), shardKey.String(), actorKey.String(), dueKey.String(),
			receiptDueKey.String(), bucketMetaKey.String()).Err()
	})
	store, err := redisauthority.NewStore(client, nil, redisauthority.Options{Profile: profile, Resolver: calculator})
	if err != nil {
		t.Fatal(err)
	}
	shard, err := calculator.ShardFor(actorrouting.DomainPlayer, playerID)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager,
			InstanceID: "m1b-live-route-manager", BootID: "m1b-live-route-manager-boot"},
		TransitionID: "m1b-live-player-claim", AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer,
		VirtualShard: shard, NewHolderID: "m1b-live-holder", NewHolderBootID: "m1b-live-holder-boot", RequestedTTLms: 60_000,
	})
	if err != nil || claim.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID: claim.Authority.HolderID, ExpectedHolderBootID: claim.Authority.HolderBootID,
		ExpectedAssignmentRevision: claim.Authority.AssignmentRevision, ExpectedWriterEpoch: claim.Authority.WriterEpoch,
		ExpectedLeaseEpoch: claim.Authority.LeaseEpoch,
	}
	provider, err := authority.NewProvider(authority.ProviderOptions{Store: store,
		Fence: authority.StaticFenceSource{Fence: fence}, AuthorityLayoutVersion: "v1", BootID: "m1b-live-holder-boot"})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewAuthorityRegistry(provider, RegistryOptions{MailboxSize: 8, ModelFieldV2Conversion: enabledPlayerV2Policy()})
	t.Cleanup(registry.Stop)
	if _, version, err := registry.EnsureWithProfile(ctx, playerID, "m1b-live-bootstrap", "m1b_live_farmer", 1); err != nil || version != 2 {
		t.Fatalf("version=%d err=%v", version, err)
	}
	assertPlayerV2PhysicalTruth(t, store, playerID, 2)
}
