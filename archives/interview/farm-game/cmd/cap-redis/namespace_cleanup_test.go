package main

import (
	"context"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestNamespaceCleanupDeletesTaggedAndUntaggedKeysOnly(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	namespace, err := rediskey.NewNamespace("cleanup", "zone")
	if err != nil {
		t.Fatal(err)
	}
	environment := &benchmarkEnvironment{client: client, namespace: namespace}
	owned := []string{
		"classic-farm:cleanup:zone:plain-key",
		"{classic-farm:cleanup:zone:auth:player:v1:s:1}:actor:1",
		"{classic-farm:cleanup:zone:wf:v1:b:1}:work:fixture",
	}
	foreign := []string{
		"classic-farm:cleanup:other:plain-key",
		"{classic-farm:cleanup:other:auth:player:v1:s:1}:actor:1",
		"{classic-farm:other:zone:auth:player:v1:s:1}:actor:1",
		"{classic-farm:cleanupx:zone:auth:player:v1:s:1}:actor:1",
	}
	ctx := context.Background()
	for _, key := range append(append([]string{}, owned...), foreign...) {
		if err := client.Set(ctx, key, "fixture", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if residual, err := environment.namespaceResidual(ctx); err != nil || residual != int64(len(owned)) {
		t.Fatalf("residual before=%d err=%v", residual, err)
	}
	if err := environment.deleteNamespace(ctx); err != nil {
		t.Fatal(err)
	}
	if residual, err := environment.namespaceResidual(ctx); err != nil || residual != 0 {
		t.Fatalf("residual after=%d err=%v", residual, err)
	}
	for _, key := range owned {
		if exists := client.Exists(ctx, key).Val(); exists != 0 {
			t.Fatalf("owned key remains: %s", key)
		}
	}
	for _, key := range foreign {
		if exists := client.Exists(ctx, key).Val(); exists != 1 {
			t.Fatalf("foreign key deleted: %s", key)
		}
	}
}

func TestNamespaceResetRecreatesFreshRenewableShardLease(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	hook := newRedisMetricHook()
	environment, err := newBenchmarkEnvironment(context.Background(), client, hook, "cleanup", "lease")
	if err != nil {
		t.Fatal(err)
	}
	server.FastForward(2 * time.Minute)
	if err := environment.resetNamespace(context.Background()); err != nil {
		t.Fatal(err)
	}
	address := authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 160_000_001}
	fence, bucket, err := environment.fenceFor(context.Background(), address)
	if err != nil {
		t.Fatal(err)
	}
	result, err := environment.authority.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder,
			InstanceID: fence.ExpectedHolderID, BootID: fence.ExpectedHolderBootID},
		AuthorityLayoutVersion: "v1", Domain: address.Domain, VirtualShard: bucket.VirtualShard(),
		ExpectedFence: fence, RequestedTTLms: int64(time.Minute.Milliseconds()),
	})
	if err != nil || result.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("renew after reset status=%s err=%v", result.Status, err)
	}
}
