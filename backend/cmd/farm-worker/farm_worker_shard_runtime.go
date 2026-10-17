package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/routecontrol"
	"farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	redisroutecontrol "farmgame/internal/storage/redis/routecontrol"
	"farmgame/internal/worker/opportunityrecovery"

	"github.com/redis/go-redis/v9"
)

// farmWorkerShardRuntime is the one process-wide FARM_WORKER ownership view.
// Opportunity wakeups and SourceOrder recovery share it, so one farm-worker
// process never runs duplicate assignment watchers or authority renewers.
type farmWorkerShardRuntime struct {
	Provider  opportunityrecovery.WorkerShardAssignmentProvider
	Admission opportunityrecovery.WorkerShardClaimAdmission
	Route     *opportunityrecovery.RouteWorkerShardAssignmentProvider
	Renewer   *farmWorkerAuthorityRenewer
}

func buildFarmWorkerShardRuntime(
	authorityClient redis.UniversalClient,
	profile rediskey.PhysicalProfile,
	resolver *actorrouting.StaticResolver,
	assignmentSource actorrouting.RevisionedAssignmentProvider,
	owner, bootID string,
) (*farmWorkerShardRuntime, error) {
	if authorityClient == nil || resolver == nil {
		return nil, errors.New("FARM_WORKER shard runtime requires Authority Redis and route snapshot")
	}
	snapshot := resolver.Snapshot()
	calculator, err := actorrouting.NewCalculator(snapshot.Realm,
		actorrouting.HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed},
		snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		return nil, fmt.Errorf("FARM_WORKER route calculator: %w", err)
	}
	farmShards, err := calculator.ShardCount(actorrouting.DomainFarm)
	if err != nil || farmShards == 0 {
		return nil, fmt.Errorf("FARM_WORKER FARM shard count: %w", err)
	}
	configured := make([]uint32, farmShards)
	for shard := uint32(0); shard < farmShards; shard++ {
		configured[shard] = shard
	}
	routeOptions, err := farmWorkerRouteAssignmentOptionsFromEnvironment()
	if err != nil {
		return nil, err
	}
	provider, routeProvider, err := opportunityOwnedShardAssignmentWithOptions(configured, assignmentSource, owner, bootID, routeOptions)
	if err != nil {
		return nil, err
	}
	admission, ok := provider.(opportunityrecovery.WorkerShardClaimAdmission)
	if !ok {
		return nil, errors.New("FARM_WORKER assignment provider lacks claim admission")
	}
	runtime := &farmWorkerShardRuntime{Provider: provider, Admission: admission, Route: routeProvider}
	if routeProvider == nil {
		return runtime, nil
	}

	scheduleDueIndexMode, scheduleDueIndexReady, err := scheduleDueIndexModeFromEnvironment()
	if err != nil {
		return nil, err
	}
	authorityStore, err := authority.NewStore(authorityClient, nil, authority.Options{
		Profile: profile, Resolver: calculator, ScheduleDueIndexMode: scheduleDueIndexMode,
		ScheduleDueIndexDedicatedReady: scheduleDueIndexReady,
	})
	if err != nil {
		return nil, fmt.Errorf("FARM_WORKER authority store: %w", err)
	}
	warmupCtx, cancelWarmup := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWarmup()
	if err = authorityStore.Registry().EnsureLoaded(warmupCtx); err != nil {
		return nil, fmt.Errorf("FARM_WORKER authority scripts: %w", err)
	}
	holderStore, err := redisroutecontrol.NewHolderStore(
		authorityClient,
		redisroutecontrol.DefaultHolderStoreOptions(profile.Namespace),
	)
	if err != nil {
		return nil, fmt.Errorf("FARM_WORKER holder store: %w", err)
	}
	if err = holderStore.EnsureLoaded(warmupCtx); err != nil {
		return nil, fmt.Errorf("FARM_WORKER holder scripts: %w", err)
	}
	holderRegistry, err := core.NewHolderRegistry(holderStore, core.DefaultHolderRegistryOptions())
	if err != nil {
		return nil, fmt.Errorf("FARM_WORKER holder registry: %w", err)
	}
	runtime.Renewer, err = newFarmWorkerAuthorityRenewer(
		assignmentSource,
		authorityStore,
		holderRegistry,
		owner,
		bootID,
		env("FARM_WORKER_LOGIC_ENDPOINT", owner+":9201"),
		env("FARM_WORKER_HOLDER_CONTROL_ENDPOINT", owner+":9201"),
		env("FARM_WORKER_HOLDER_TLS_DNS_IDENTITY", "holder-control.farm-route-dynamic.svc.cluster.local"),
		profile.AuthorityLayoutVersion,
		parseDuration("FARM_WORKER_AUTHORITY_RENEW_TTL", time.Minute),
		parseDuration("FARM_WORKER_HOLDER_LEASE_TTL", 30*time.Second),
		parseDuration("FARM_WORKER_AUTHORITY_RENEW_INTERVAL", 10*time.Second),
		parseDuration("FARM_WORKER_AUTHORITY_RENEW_TIMEOUT", 2*time.Second),
	)
	if err != nil {
		return nil, err
	}
	if err = runtime.Renewer.SetLeaseCheckMode(routeOptions.LeaseCheckMode); err != nil {
		return nil, err
	}
	runtime.Renewer.SetLeaseObserver(routeProvider)
	if err = runtime.Renewer.bootstrap(context.Background()); err != nil {
		return nil, fmt.Errorf("FARM_WORKER authority bootstrap: %w", err)
	}
	gated := &renewalGatedWorkerAssignment{
		provider: routeProvider, admission: routeProvider, renewer: runtime.Renewer,
	}
	runtime.Provider = gated
	runtime.Admission = gated
	return runtime, nil
}

func farmWorkerRouteAssignmentOptionsFromEnvironment() (opportunityrecovery.RouteWorkerShardAssignmentOptions, error) {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("ACTOR_ROUTE_LEASE_CHECK_MODE")))
	if raw == "" {
		raw = string(actorrouting.LeaseCheckSnapshot)
	}
	mode := actorrouting.LeaseCheckMode(raw)
	if mode != actorrouting.LeaseCheckSnapshot && mode != actorrouting.LeaseCheckAuthorityDelegated {
		return opportunityrecovery.RouteWorkerShardAssignmentOptions{}, fmt.Errorf("ACTOR_ROUTE_LEASE_CHECK_MODE: unsupported worker lease check mode %q", raw)
	}
	return opportunityrecovery.RouteWorkerShardAssignmentOptions{
		LeaseCheckMode: mode,
		SafetyMargin:   parseDuration("ACTOR_AUTHORITY_LEASE_SAFETY_MARGIN", 10*time.Second),
	}, nil
}

func (r *farmWorkerShardRuntime) Start(ctx context.Context) {
	if r == nil {
		return
	}
	if r.Route != nil {
		go func() {
			if err := r.Route.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("FARM_WORKER assignment watch stopped fail-closed: %v", err)
			}
		}()
	}
	if r.Renewer != nil {
		go func() {
			if err := r.Renewer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("FARM_WORKER authority renewer stopped fail-closed: %v", err)
			}
		}()
	}
}
