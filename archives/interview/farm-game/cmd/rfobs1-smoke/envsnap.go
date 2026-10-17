package main

// Environment, snapshot and authority helpers for the Q2R-2B2 smoke.  The
// business process points FARM_REDIS_URL at its own fault proxy; the observer
// uses a direct client to the company Redis.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
)

const (
	playerHolder = "rfobs1-player"
	playerBoot   = "rfobs1-player-boot"
	farmHolder   = "rfobs1-farm"
	farmBoot     = "rfobs1-farm-boot"
	// TTL 20s with a 5s safety margin: crossing the LOCAL deadline leaves a
	// ~4.3s recovery window before the authoritative lease expires, giving the
	// same-generation renew enough time to re-admit after the proxy recovers.
	leaseTTLMs   = 20000
	safetyMargin = 5000 * time.Millisecond
	renewInterval = 300 * time.Millisecond
	hashSeed     = uint64(0x5EEDB1)
)

// proxyURL wraps the proxy address into a redis URL for the business process,
// preserving the original credentials (the proxy is a transparent byte relay).
// proxyAddr is already "127.0.0.1:port".
func proxyURL(rawURL, proxyAddr string) string {
	orig, err := url.Parse(rawURL)
	if err != nil {
		return "redis://" + proxyAddr + "/0"
	}
	u := &url.URL{Scheme: "redis", Host: proxyAddr, Path: "/0"}
	if orig.User != nil {
		u.User = orig.User
	}
	return u.String()
}

func baseEnv(rawURL, proxyAddr, ns string) []string {
	return []string{
		"FARM_REDIS_URL=" + proxyURL(rawURL, proxyAddr),
		"ACTOR_AUTHORITY_MODE=authority",
		"AUTHORITY_MEMBERSHIP_MODE=true",
		"Q2R2_LIVE=1", // test-only diagnostics double-gate; no effect on the business path
		"REDIS_NAMESPACE_ENVIRONMENT=" + ns,
		"REDIS_NAMESPACE_ZONE=z1",
		"REDIS_PHYSICAL_PROFILE_VERSION=rfobs1-v1",
		"REDIS_AUTHORITY_LAYOUT_VERSION=v1",
		"REDIS_WORKFLOW_LAYOUT_VERSION=v1",
		"REDIS_WORKFLOW_HASH_VERSION=1",
		"REDIS_WORKFLOW_BUCKET_COUNT=64",
		"ACTOR_AUTHORITY_LEASE_TTL=20s",
		"ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN=5s",
		"ACTOR_AUTHORITY_LEASE_RENEW_INTERVAL=300ms",
	}
}

func playerEnv(rawURL, ns, proxyAddr, snapshotPath, holder, boot string, healthPort int) []string {
	return append(append([]string{}, baseEnv(rawURL, proxyAddr, ns)...),
		"PLAYER_LISTEN=127.0.0.1:"+strconv.Itoa(nextPort()),
		"PLAYER_HEALTH_LISTEN=127.0.0.1:"+strconv.Itoa(healthPort),
		"ACTOR_ROUTE_SNAPSHOT="+snapshotPath,
		"ACTOR_HOLDER_POD="+holder,
		"ACTOR_HOLDER_BOOT_ID="+boot,
		"PLAYER_STORE_PREFIX=rfobs1:player",
	)
}

func farmEnv(rawURL, ns, proxyAddr, snapshotPath, holder, boot string, healthPort int) []string {
	return append(append([]string{}, baseEnv(rawURL, proxyAddr, ns)...),
		"FARM_LISTEN=127.0.0.1:"+strconv.Itoa(nextPort()),
		"FARM_HEALTH_LISTEN=127.0.0.1:"+strconv.Itoa(healthPort),
		"ACTOR_ROUTE_SNAPSHOT="+snapshotPath,
		"ACTOR_HOLDER_POD="+holder,
		"ACTOR_HOLDER_BOOT_ID="+boot,
		"PERMIT_SECRET=rfobs1-permit-secret-0123456789abcdef",
		"THEFT_PRF_MODE=test",
		"FARM_STORE_PREFIX=rfobs1:farm",
		"WORKFLOW_STORE_PREFIX=rfobs1:workflow",
		"THEFT_GUARD_SNAPSHOT_PREFIX=rfobs1:theft-guard",
		"THEFT_TARGET_BINDING_PREFIX=rfobs1:theft-target",
	)
}

var portCounter = 17000

func nextPort() int {
	portCounter++
	return portCounter
}

func writeSnapshot(dir, ns string, playerRoute, farmRoute actorrouting.ShardRoute) (string, error) {
	holders := []actorrouting.HolderRef{
		{InstanceID: playerHolder, BootID: playerBoot, Endpoint: "player.local:17100", State: actorrouting.HolderStateActive, Weight: 1},
		{InstanceID: farmHolder, BootID: farmBoot, Endpoint: "farm.local:17101", State: actorrouting.HolderStateActive, Weight: 1},
		{InstanceID: playerHolder + "-other", BootID: "rfobs1-player-other-boot", Endpoint: "player-other.local:17102", State: actorrouting.HolderStateActive, Weight: 1},
		{InstanceID: farmHolder + "-other", BootID: "rfobs1-farm-other-boot", Endpoint: "farm-other.local:17103", State: actorrouting.HolderStateActive, Weight: 1},
	}
	snapshot := actorrouting.RouteSnapshot{
		Realm: "rfobs1-" + ns, GlobalRevision: 1,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: hashSeed,
		Holders: holders,
		Player:  actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{playerRoute}},
		Farm:    actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{farmRoute}},
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "route-"+ns+".json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		return "", err
	}
	return path, nil
}

func routeFor(authority *authoritystore.ShardWriteAuthority, holderIndex int) actorrouting.ShardRoute {
	route := actorrouting.ShardRoute{
		HolderIndex: holderIndex, State: actorrouting.ShardStateActive,
		ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
		LeaseUntilMs: time.Now().Add(time.Duration(leaseTTLMs) * time.Millisecond).UnixMilli(),
	}
	if authority != nil {
		route.ShardRevision = authority.AssignmentRevision
		route.ExpectedWriterEpoch = authority.WriterEpoch
		route.ExpectedLeaseEpoch = authority.LeaseEpoch
		route.LeaseUntilMs = authority.LeaseUntilMs
	}
	return route
}

func calculatorFor(ns string) (*actorrouting.Calculator, error) {
	return actorrouting.NewCalculator("rfobs1-"+ns,
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: hashSeed}, 1, 1)
}

func profileFor(ns string) rediskey.PhysicalProfile {
	ns2, err := rediskey.NewNamespace(ns, "z1")
	if err != nil {
		panic(err)
	}
	return rediskey.PhysicalProfile{
		ProfileVersion: "rfobs1-v1", Namespace: ns2,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 64,
	}
}

// claimShard pre-seeds the authoritative lease on the company Redis (direct
// client) and returns the result plus the exact shard key.
func claimShard(client *redis.Client, domain actorrouting.RouteDomain, actorID uint64, ns, holder, boot string) (authoritystore.ShardAuthorityTransitionResult, rediskey.RedisKey, error) {
	calculator, err := calculatorFor(ns)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, "", err
	}
	store, err := redisauthority.NewStore(client, nil, redisauthority.Options{Profile: profileFor(ns), Resolver: calculator})
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, "", err
	}
	if err := store.Registry().EnsureLoaded(context.Background()); err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, "", err
	}
	addr := authoritystore.ActorAddress{Domain: domain, ActorID: actorID}
	// The static snapshot has ShardCount=1 with the sole route at shard 0, so
	// the pre-seeded claim must land on shard 0 (not an actor-derived shard) to
	// match the process's serving shard and its renew key.
	bucket, err := profileFor(ns).AuthorityBucketForShard(calculator, domain, 0)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, "", err
	}
	claim, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "rfobs1-rm", BootID: "rfobs1-rm-" + ns,
		},
		TransitionID: "claim-" + ns, AuthorityLayoutVersion: "v1",
		Domain: addr.Domain, VirtualShard: bucket.VirtualShard(),
		NewHolderID: holder, NewHolderBootID: boot,
		RequestedTTLms: leaseTTLMs,
	})
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, "", err
	}
	if claim.Status != authoritystore.ShardTransitionApplied {
		return authoritystore.ShardAuthorityTransitionResult{}, "", fmt.Errorf("claim status=%s", claim.Status)
	}
	key, err := store.Keys().ShardAuthority(bucket)
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, "", err
	}
	return claim, key, nil
}

// leaseDeadline reads the authoritative lease deadline field written by the
// claim/renew Lua on the exact shard key.
func leaseDeadline(client *redis.Client, key rediskey.RedisKey) (int64, error) {
	return client.HGet(context.Background(), key.String(), "lease_expire_at_ms").Int64()
}

func redisTimeMS(ctx context.Context, client *redis.Client) int64 {
	tm, err := client.Time(ctx).Result()
	if err != nil {
		return -1
	}
	return tm.UnixMilli()
}
