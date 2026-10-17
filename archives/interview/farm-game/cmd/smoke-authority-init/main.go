// Command smoke-authority-init claims ShardWriteAuthority for player and farm
// domains in a clean namespace, then writes the matching route-snapshot.json.
// It must run before any player-svr / farm-svr in authority mode.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/redis/go-redis/v9"
)

func main() {
	redisURL := flag.String("redis-url", "", "Redis connection URL")
	namespaceEnv := flag.String("namespace-env", "smoke-a", "Redis namespace environment")
	namespaceZone := flag.String("namespace-zone", "local", "Redis namespace zone")
	profile := flag.String("profile", "redis-physical-v1", "Redis physical profile version")
	realm := flag.String("realm", "smoke-premerge-a", "Route realm")
	hashSeed := flag.Uint64("hash-seed", 202608060001, "Route hash seed")
	runID := flag.String("run-id", "", "Unique run identifier")
	playerHolder := flag.String("player-holder", "smoke-player", "Player holder instance ID")
	playerBoot := flag.String("player-boot", "", "Player holder boot ID")
	playerEndpoint := flag.String("player-endpoint", "127.0.0.1:19600", "Player holder endpoint")
	farmHolder := flag.String("farm-holder", "smoke-farm", "Farm holder instance ID")
	farmBoot := flag.String("farm-boot", "", "Farm holder boot ID")
	farmEndpoint := flag.String("farm-endpoint", "127.0.0.1:19500", "Farm holder endpoint")
	routeOut := flag.String("route-out", "", "Output path for route-snapshot.json")
	flag.Parse()

	if *redisURL == "" || *routeOut == "" || *runID == "" {
		log.Fatal("--redis-url, --route-out, and --run-id are required")
	}
	if *playerBoot == "" {
		*playerBoot = *playerHolder + "-boot-" + *runID
	}
	if *farmBoot == "" {
		*farmBoot = *farmHolder + "-boot-" + *runID
	}

	opt, err := redis.ParseURL(*redisURL)
	if err != nil {
		log.Fatalf("parse redis URL: %v", err)
	}
	client := redis.NewClient(opt)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	namespace, err := rediskey.NewNamespace(*namespaceEnv, *namespaceZone)
	if err != nil {
		log.Fatalf("build namespace: %v", err)
	}
	physicalProfile := rediskey.PhysicalProfile{
		ProfileVersion:         *profile,
		Namespace:              namespace,
		AuthorityLayoutVersion: "v1",
		WorkflowLayoutVersion:  "v1",
		WorkflowHashVersion:    1,
		WorkflowBucketCount:    256,
	}
	calculator, err := actorrouting.NewCalculator(*realm, actorrouting.HashProfile{
		Version: actorrouting.HashVersionXXH64V2,
		Seed:    *hashSeed,
	}, 1, 1)
	if err != nil {
		log.Fatalf("build calculator: %v", err)
	}

	scripts, err := redisauthority.NewScriptRegistry(client)
	if err != nil {
		log.Fatalf("build script registry: %v", err)
	}
	if err := scripts.EnsureLoaded(ctx); err != nil {
		log.Fatalf("load scripts: %v", err)
	}
	store, err := redisauthority.NewStore(client, scripts, redisauthority.Options{
		Profile: physicalProfile, Resolver: calculator, LeaseLimits: authoritystore.DefaultLeaseLimits(),
	})
	if err != nil {
		log.Fatalf("build authority store: %v", err)
	}

	leaseTTL := 55 * time.Second
	claim := func(domain actorrouting.RouteDomain, holder, boot string) authoritystore.ShardAuthorityTransitionResult {
		result, err := store.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
			Caller: authoritystore.TrustedShardAuthorityCaller{
				Kind:       authoritystore.ShardAuthorityCallerRouteManager,
				InstanceID: "smoke-route-manager",
				BootID:     "smoke-route-manager-" + *runID,
			},
			TransitionID:           fmt.Sprintf("claim-%s-%s", domain, *runID),
			AuthorityLayoutVersion: "v1",
			Domain:                 domain,
			VirtualShard:           0,
			NewHolderID:            holder,
			NewHolderBootID:        boot,
			RequestedTTLms:         leaseTTL.Milliseconds(),
		})
		if err != nil {
			log.Fatalf("claim %s shard: %v", domain, err)
		}
		if result.Status != authoritystore.ShardTransitionApplied {
			log.Fatalf("claim %s shard: unexpected status %s", domain, result.Status)
		}
		log.Printf("claimed %s shard 0: holder=%s epoch=%d assignment_rev=%d lease_until=%d",
			domain, holder, result.Authority.WriterEpoch, result.Authority.AssignmentRevision, result.Authority.LeaseUntilMs)
		return result
	}

	playerClaim := claim(actorrouting.DomainPlayer, *playerHolder, *playerBoot)
	farmClaim := claim(actorrouting.DomainFarm, *farmHolder, *farmBoot)

	leaseUntil := playerClaim.Authority.LeaseUntilMs
	if farmClaim.Authority.LeaseUntilMs < leaseUntil {
		leaseUntil = farmClaim.Authority.LeaseUntilMs
	}

	snapshot := actorrouting.RouteSnapshot{
		Realm:          *realm,
		GlobalRevision: 1,
		HashVersion:    actorrouting.HashVersionXXH64V2,
		HashSeed:       *hashSeed,
		Holders: []actorrouting.HolderRef{
			{InstanceID: *playerHolder, BootID: *playerBoot, Endpoint: *playerEndpoint, State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: *farmHolder, BootID: *farmBoot, Endpoint: *farmEndpoint, State: actorrouting.HolderStateActive, Weight: 1},
		},
		Player: actorrouting.DomainSnapshot{
			ShardCount: 1,
			Routes: []actorrouting.ShardRoute{{
				HolderIndex:         0,
				State:               actorrouting.ShardStateActive,
				ShardRevision:       playerClaim.Authority.AssignmentRevision,
				ExpectedWriterEpoch: playerClaim.Authority.WriterEpoch,
				ExpectedLeaseEpoch:  playerClaim.Authority.LeaseEpoch,
				LeaseUntilMs:        leaseUntil,
			}},
		},
		Farm: actorrouting.DomainSnapshot{
			ShardCount: 1,
			Routes: []actorrouting.ShardRoute{{
				HolderIndex:         1,
				State:               actorrouting.ShardStateActive,
				ShardRevision:       farmClaim.Authority.AssignmentRevision,
				ExpectedWriterEpoch: farmClaim.Authority.WriterEpoch,
				ExpectedLeaseEpoch:  farmClaim.Authority.LeaseEpoch,
				LeaseUntilMs:        leaseUntil,
			}},
		},
	}

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		log.Fatalf("marshal snapshot: %v", err)
	}
	if err := os.WriteFile(*routeOut, append(data, '\n'), 0o600); err != nil {
		log.Fatalf("write route snapshot: %v", err)
	}
	log.Printf("route snapshot written: %s", *routeOut)
	log.Printf("player boot=%s farm boot=%s", *playerBoot, *farmBoot)
}
