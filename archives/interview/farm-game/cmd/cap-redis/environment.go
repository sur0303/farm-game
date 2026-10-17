package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/friend"
	"farmgame/internal/gateway/session"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/redis/go-redis/v9"
)

const hashSeed uint64 = 202608030001

type benchmarkEnvironment struct {
	client       *redis.Client
	hook         *redisMetricHook
	namespace    rediskey.Namespace
	profile      rediskey.PhysicalProfile
	calculator   *actorrouting.Calculator
	authority    *redisauthority.Store
	friends      *friend.RedisFriendshipRepositories
	friendKeys   friend.FriendshipKeyBuilder
	resultDir    *redisauthority.ResultDirectoryStore
	sessions     *session.RedisAuthority
	workflowBase string
}

func newBenchmarkEnvironment(ctx context.Context, client *redis.Client, hook *redisMetricHook, environment, zone string) (*benchmarkEnvironment, error) {
	return newBenchmarkEnvironmentForTask(ctx, client, hook, environment, zone, "CAP-REDIS-L1-R1")
}

func newBenchmarkEnvironmentForTask(ctx context.Context, client *redis.Client, hook *redisMetricHook, environment, zone, taskID string) (*benchmarkEnvironment, error) {
	namespace, err := rediskey.NewNamespace(environment, zone)
	if err != nil {
		return nil, err
	}
	profileID := strings.ToLower(taskID)
	profile := rediskey.PhysicalProfile{
		ProfileVersion: profileID, Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 64,
	}
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	calculator, err := actorrouting.NewCalculator(profileID+"-realm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: hashSeed}, 64, 64)
	if err != nil {
		return nil, err
	}
	store, err := redisauthority.NewStore(client, nil, redisauthority.Options{Profile: profile, Resolver: calculator})
	if err != nil {
		return nil, err
	}
	if err := store.Registry().EnsureLoaded(ctx); err != nil {
		return nil, fmt.Errorf("load authority scripts: %w", err)
	}
	friendProfile := friend.DefaultFriendRedisProfile(namespace)
	friendProfile.PairBucketCount = 64
	friendProfile.QuotaBucketCount = 64
	friendProfile.ShareBucketCount = 64
	friends, err := friend.NewRedisFriendshipRepositories(client, friend.RedisFriendshipOptions{Profile: friendProfile})
	if err != nil {
		return nil, err
	}
	friendKeys, err := friend.NewFriendshipKeyBuilder(friendProfile)
	if err != nil {
		return nil, err
	}
	if err := friends.EnsureScriptsLoaded(ctx); err != nil {
		return nil, fmt.Errorf("load friendship scripts: %w", err)
	}
	resultDir, err := redisauthority.NewResultDirectoryStore(client, store.Registry(), namespace, 0)
	if err != nil {
		return nil, err
	}
	sessions := session.NewRedis(client, session.Config{Secret: []byte(profileID + "-test-only-secret"), AuthorityEpoch: 1, EpochCacheTTL: 0})
	env := &benchmarkEnvironment{client: client, hook: hook, namespace: namespace, profile: profile,
		calculator: calculator, authority: store, friends: friends, friendKeys: friendKeys, resultDir: resultDir, sessions: sessions,
		workflowBase: "classic-farm:" + environment + ":" + zone + ":capwf"}
	if err := env.claimAllActorShards(ctx); err != nil {
		return nil, err
	}
	return env, nil
}

func namespaceKeyPatterns(namespace rediskey.Namespace) []string {
	prefix := fmt.Sprintf("classic-farm:%s:%s:", namespace.Environment, namespace.Zone)
	return []string{prefix + "*", "{" + prefix + "*"}
}

func (e *benchmarkEnvironment) deleteNamespace(ctx context.Context) error {
	for _, pattern := range namespaceKeyPatterns(e.namespace) {
		var cursor uint64
		for {
			keys, next, err := e.client.Scan(ctx, cursor, pattern, 512).Result()
			if err != nil {
				return err
			}
			if len(keys) != 0 {
				if err := e.client.Del(ctx, keys...).Err(); err != nil {
					return err
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
	return nil
}

func (e *benchmarkEnvironment) resetNamespace(ctx context.Context) error {
	if err := e.deleteNamespace(ctx); err != nil {
		return err
	}
	return e.claimAllActorShards(ctx)
}

func (e *benchmarkEnvironment) namespaceResidual(ctx context.Context) (int64, error) {
	var count int64
	for _, pattern := range namespaceKeyPatterns(e.namespace) {
		var cursor uint64
		for {
			keys, next, err := e.client.Scan(ctx, cursor, pattern, 512).Result()
			if err != nil {
				return 0, err
			}
			count += int64(len(keys))
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
	return count, nil
}

func (e *benchmarkEnvironment) claimAllActorShards(ctx context.Context) error {
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		for shard := uint32(0); shard < 64; shard++ {
			result, err := e.authority.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
				Caller: authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager,
					InstanceID: "cap-route-manager", BootID: "cap-route-manager-boot"},
				TransitionID:           fmt.Sprintf("cap-claim-%s-%d", strings.ToLower(string(domain)), shard),
				AuthorityLayoutVersion: "v1", Domain: domain, VirtualShard: shard,
				NewHolderID: "cap-" + strings.ToLower(string(domain)) + "-pod", NewHolderBootID: "cap-pod-boot",
				RequestedTTLms: int64((4 * time.Hour).Milliseconds()),
			})
			if err != nil {
				return fmt.Errorf("claim %s shard %d: %w", domain, shard, err)
			}
			if result.Status != authoritystore.ShardTransitionApplied && result.Status != authoritystore.ShardTransitionAlreadyApplied {
				return fmt.Errorf("claim %s shard %d status=%s", domain, shard, result.Status)
			}
		}
	}
	return nil
}

func (e *benchmarkEnvironment) fenceFor(ctx context.Context, address authoritystore.ActorAddress) (authoritystore.ShardWriteFence, rediskey.AuthorityBucketRef, error) {
	bucket, err := e.profile.AuthorityBucketForActor(e.calculator, address)
	if err != nil {
		return authoritystore.ShardWriteFence{}, rediskey.AuthorityBucketRef{}, err
	}
	// The frozen fixture uses one known holder/fence. Claim is idempotent and
	// deliberately outside every measured cell.
	result, err := e.authority.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager,
			InstanceID: "cap-route-manager", BootID: "cap-route-manager-boot"},
		TransitionID:           fmt.Sprintf("cap-claim-%s-%d", strings.ToLower(string(address.Domain)), bucket.VirtualShard()),
		AuthorityLayoutVersion: "v1", Domain: address.Domain, VirtualShard: bucket.VirtualShard(),
		NewHolderID: "cap-" + strings.ToLower(string(address.Domain)) + "-pod", NewHolderBootID: "cap-pod-boot",
		RequestedTTLms: int64((4 * time.Hour).Milliseconds()),
	})
	if err != nil {
		return authoritystore.ShardWriteFence{}, rediskey.AuthorityBucketRef{}, err
	}
	if result.Status != authoritystore.ShardTransitionApplied && result.Status != authoritystore.ShardTransitionAlreadyApplied {
		return authoritystore.ShardWriteFence{}, rediskey.AuthorityBucketRef{}, fmt.Errorf("fence claim status=%s", result.Status)
	}
	return authoritystore.ShardWriteFence{
		ExpectedHolderID: result.Authority.HolderID, ExpectedHolderBootID: result.Authority.HolderBootID,
		ExpectedAssignmentRevision: result.Authority.AssignmentRevision, ExpectedWriterEpoch: result.Authority.WriterEpoch,
		ExpectedLeaseEpoch: result.Authority.LeaseEpoch,
	}, bucket, nil
}
