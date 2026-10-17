// Command farm-svr runs the farm_id keyed FarmActor authority and coordinates
// PlayerActor participant steps for plant/harvest/theft workflows.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"farmgame"
	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	"farmgame/internal/authoritycomposition"
	"farmgame/internal/contracts"
	"farmgame/internal/dynamicconfig"
	farmactor "farmgame/internal/farm/actor"
	farmingress "farmgame/internal/farm/ingress"
	"farmgame/internal/farm/readpath"
	"farmgame/internal/farm/subscriptionreaper"
	"farmgame/internal/friend"
	"farmgame/internal/gateway/bootlease"
	"farmgame/internal/gatewayapp"
	"farmgame/internal/grpcmetrics"
	"farmgame/internal/holdercontrol"
	"farmgame/internal/holderkey"
	"farmgame/internal/identity"
	"farmgame/internal/logic/actor/activation"
	"farmgame/internal/logic/actor/idle"
	actorruntime "farmgame/internal/logic/actor/runtime"
	logicdownlink "farmgame/internal/logic/downlink"
	"farmgame/internal/observability/actorlayout"
	"farmgame/internal/observability/redisworkload"
	"farmgame/internal/permitsecret"
	"farmgame/internal/plantonline"
	"farmgame/internal/redisdomains"
	routecontrol "farmgame/internal/routecontrol"
	"farmgame/internal/routecontrol/transport"
	plantscenario "farmgame/internal/scenario/plant"
	theftscenario "farmgame/internal/scenario/theft"
	"farmgame/internal/sourceordermetrics"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
	"farmgame/internal/storage/redis/rediskey"
	redisroutecontrol "farmgame/internal/storage/redis/routecontrol"
	"farmgame/internal/theftonline"
	"farmgame/internal/worker/carerecovery"
	"farmgame/internal/worker/sourceorder"
	"farmgame/internal/worker/theftrecovery"
	"farmgame/internal/workflow"
	"farmgame/internal/workflow/participantproof"
	"farmgame/internal/workidentity"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type targetedFriendOpportunityPublisher struct {
	bus   *friend.RedisOpportunityHintBus
	index friend.FriendIndexRepository
}

func (p targetedFriendOpportunityPublisher) PublishFriendOpportunityHint(ctx context.Context,
	ownerID, sourceFarmVersion, opportunityRevision uint64) error {
	viewers, err := friend.CollectTargetedOpportunityViewers(ctx, p.index, ownerID)
	if err != nil || len(viewers) == 0 {
		return err
	}
	return p.bus.PublishTargetedOpportunityHint(ctx, friend.TargetedOpportunityHint{
		TargetOwnerID: ownerID, SourceFarmVersion: sourceFarmVersion,
		OpportunityRevision: opportunityRevision, ViewerPlayerIDs: viewers,
	})
}

func buildFarmParticipantServer(registry *farmactor.Registry, repository workflow.WorkRepository, resolver *actorrouting.StaticResolver, observers ...farmingress.CommitPlantObserver) *farmingress.Server {
	if registry == nil {
		return nil
	}
	var commitPlantObserver farmingress.CommitPlantObserver
	if len(observers) > 0 {
		commitPlantObserver = observers[0]
	}
	var proof *participantproof.Verifier
	if repository != nil && resolver != nil {
		holderID := os.Getenv("ACTOR_HOLDER_POD")
		bootID := os.Getenv("ACTOR_HOLDER_BOOT_ID")
		fence, fenceErr := plantonline.NewRouteFence(resolver, resolver.Realm(), actorrouting.DomainFarm, holderID, bootID)
		if fenceErr == nil {
			placement, placementErr := plantonline.NewPlacementResolver(resolver.Snapshot(), productionPlacementConfig())
			if placementErr == nil {
				proof, _ = participantproof.New(repository,
					participantproof.WithBucketResolver(placement),
					participantproof.WithRouteFenceVerifier(fence),
					participantproof.WithClock(time.Now),
				)
			} else {
				log.Printf("typed Farm Participant disabled: %v", placementErr)
			}
		} else {
			log.Printf("typed Farm Participant disabled: %v", fenceErr)
		}
	} else {
		log.Printf("typed Farm Participant disabled: WorkRepository and route snapshot are required")
	}
	server := farmingress.NewServer(farmingress.Options{
		Registry:                   registry,
		WorkVersionVerifier:        proof,
		WorkLeaseVerifier:          proof,
		RouteFenceVerifier:         proof,
		WorkAddressVerifier:        proof,
		IdentityBindingVerifier:    proof,
		CommitPlantDependency:      proof,
		CommitUnlockDependency:     proof,
		CommitHarvestDependency:    proof,
		DecideTheftDependency:      proof,
		RequireRouteLeaseProof:     envBool("PARTICIPANT_ROUTE_LEASE_PROOF_STRICT", false),
		CommitPlantObserver:        commitPlantObserver,
		CommitHarvestObserver:      commitPlantObserver,
		DecideTheftObserver:        commitPlantObserver,
		ApplyGuardSnapshotObserver: commitPlantObserver,
		Now:                        time.Now,
	})
	if err := registry.ParticipantAdmissionError(); err != nil {
		log.Printf("typed Farm Participant fail-closed until authority-backed registry is configured: %v", err)
	}
	return server
}

func buildFarmParticipantServerChecked(registry *farmactor.Registry, repository workflow.WorkRepository, resolver *actorrouting.StaticResolver, routeFence participantproof.RouteFenceVerifier, placement workidentity.BucketResolver, sourceWork authoritystore.SourceWorkReader, playerSource farmingress.PlayerSourceOrderVerifier, theftRecovery farmingress.DecideTheftRecoveryVerifier, observers ...farmingress.CommitPlantObserver) (*farmingress.Server, error) {
	if registry == nil || repository == nil || resolver == nil || routeFence == nil || placement == nil || sourceWork == nil || theftRecovery == nil {
		return nil, errors.New("farm participant authority composition is incomplete")
	}
	proof, err := participantproof.New(repository,
		participantproof.WithBucketResolver(placement),
		participantproof.WithSourceWorkReader(sourceWork),
		participantproof.WithRouteFenceVerifier(routeFence),
		participantproof.WithClock(time.Now),
	)
	if err != nil {
		return nil, fmt.Errorf("build farm Participant proof: %w", err)
	}
	var commitPlantObserver farmingress.CommitPlantObserver
	if len(observers) > 0 {
		commitPlantObserver = observers[0]
	}
	return farmingress.NewServerChecked(farmingress.Options{
		Registry:                   registry,
		WorkVersionVerifier:        proof,
		WorkLeaseVerifier:          proof,
		RouteFenceVerifier:         proof,
		WorkAddressVerifier:        proof,
		IdentityBindingVerifier:    proof,
		CommitPlantDependency:      proof,
		CommitUnlockDependency:     proof,
		CommitHarvestDependency:    proof,
		DecideTheftDependency:      proof,
		DecideTheftRecovery:        theftRecovery,
		RequireRouteLeaseProof:     envBool("PARTICIPANT_ROUTE_LEASE_PROOF_STRICT", false),
		CommitPlantObserver:        commitPlantObserver,
		CommitHarvestObserver:      commitPlantObserver,
		DecideTheftObserver:        commitPlantObserver,
		ApplyGuardSnapshotObserver: commitPlantObserver,
		PlayerSourceVerifier:       playerSource,
		Now:                        time.Now,
	})
}

func typedPlayerClientMap(clients map[string]contractspb.CoordinatorParticipantServiceClient) map[string]plantscenario.CoordinatorParticipantClient {
	result := make(map[string]plantscenario.CoordinatorParticipantClient, len(clients))
	for endpoint, client := range clients {
		result[endpoint] = client
	}
	return result
}

func productionPlacementConfig() plantonline.PlacementConfig {
	return plantonline.PlacementConfig{
		ProfileVersion:         env("REDIS_PHYSICAL_PROFILE_VERSION", "redis-physical-v1"),
		NamespaceEnvironment:   env("REDIS_NAMESPACE_ENVIRONMENT", "dev"),
		NamespaceZone:          env("REDIS_NAMESPACE_ZONE", "z1"),
		AuthorityLayoutVersion: env("REDIS_AUTHORITY_LAYOUT_VERSION", "v1"),
		WorkflowLayoutVersion:  env("REDIS_WORKFLOW_LAYOUT_VERSION", "v1"),
		WorkflowHashVersion:    uint32(envInt("REDIS_WORKFLOW_HASH_VERSION", 1)),
		WorkflowBucketCount:    uint32(envInt("REDIS_WORKFLOW_BUCKET_COUNT", 256)),
	}
}

func run() error {
	modelFieldV2Conversion, conversionErr := fieldv2.ProductionConversionPolicy(os.LookupEnv)
	if conversionErr != nil {
		return fmt.Errorf("validate Actor Model Field V2 conversion policy: %w", conversionErr)
	}
	addr := env("FARM_LISTEN", ":9090")
	healthAddr := env("FARM_HEALTH_LISTEN", ":9091")
	mode, modeErr := authoritycomposition.ModeFromEnvironment()
	if modeErr != nil {
		return modeErr
	}
	membershipMode, membershipModeErr := authoritycomposition.MembershipModeFromEnvironment()
	if membershipModeErr != nil {
		return membershipModeErr
	}
	assignmentMode, assignmentModeErr := authoritycomposition.AssignmentModeFromEnvironment()
	if assignmentModeErr != nil {
		return assignmentModeErr
	}
	if err := authoritycomposition.ValidateModeCombination(membershipMode, assignmentMode); err != nil {
		return err
	}
	keyspaceTuple, err := requiredRemoteKeyspaceTuple(membershipMode, assignmentMode)
	if err != nil {
		return err
	}
	permitConfig, permitConfigErr := permitsecret.Load(os.Getenv)
	if permitConfigErr != nil {
		return fmt.Errorf("validate permit secret configuration: %w", permitConfigErr)
	}
	log.Printf("permit secret policy=%s insecure_development=%t development_switch=%t",
		permitConfig.Policy(), permitConfig.InsecureDevelopment(), permitConfig.DevelopmentSwitchEnabled())
	if permitConfig.InsecureDevelopment() {
		log.Printf("WARNING: insecure permit secret development mode is active; never use this composition for production traffic")
	}
	permitSecret := permitConfig.Secret()
	theftPRF, theftPRFErr := theftPRFFromConfig(os.Getenv)
	if theftPRFErr != nil {
		return theftPRFErr
	}
	redisOpenCtx, redisOpenCancel := context.WithTimeout(context.Background(), 5*time.Second)
	redisClients, redisErr := redisdomains.OpenFromEnv(redisOpenCtx, os.Getenv, redisdomains.SelectAll)
	redisOpenCancel()
	if redisErr != nil {
		return redisErr
	}
	defer redisClients.Close()
	friendshipRepositories, workflows, workRepository, closeStores := openStores(redisClients, keyspaceTuple)
	defer closeStores()
	metricsRegistry := prometheus.NewRegistry()
	metricsRegistry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	sourceordermetrics.Register(metricsRegistry)
	if err := contracts.RegisterParticipantRouteLeaseProofMetrics(metricsRegistry); err != nil {
		return err
	}
	commitProofMode, err := sourceorder.CommitProofModeFromEnvironment(os.Getenv)
	if err != nil {
		return fmt.Errorf("farm-svr commit proof mode: %w", err)
	}
	if err := commitProofMode.ValidatePreferRollout(); err != nil {
		return fmt.Errorf("farm-svr commit proof rollout: %w", err)
	}
	if _, err := redisworkload.AttachTargetsFromEnvironment(redisworkload.DependencyTargets{
		Authority: redisClients.Authority, Runtime: redisClients.Runtime, Projection: redisClients.Projection,
	}, metricsRegistry, os.Getenv("REDIS_WORKLOAD_METRICS")); err != nil {
		return err
	}
	if _, err := redisworkload.AttachDependencyHealth(metricsRegistry, redisworkload.DependencyTargets{
		Authority:  redisClients.Authority,
		Runtime:    redisClients.Runtime,
		Projection: redisClients.Projection,
	}); err != nil {
		return err
	}
	layoutMetrics := actorlayout.NewMetrics(metricsRegistry)
	routeMetrics := actorrouting.RegisterRouteMetrics(metricsRegistry)
	actOpts := activation.DefaultOptions()
	actOpts.MaxConcurrentLoads = envInt("MAX_CONCURRENT_ACTOR_LOADS", actOpts.MaxConcurrentLoads)
	actOpts.MaxPendingLoads = envInt("MAX_PENDING_ACTOR_LOADS", actOpts.MaxPendingLoads)
	if rawRate := os.Getenv("ACTIVATION_RATE_PER_SEC"); rawRate != "" {
		if parsed, parseErr := strconv.ParseFloat(rawRate, 64); parseErr == nil && parsed >= 0 {
			actOpts.ActivationRatePerSec = parsed
		}
	}
	registryOptions := farmactor.RegistryOptions{
		MailboxSize:              envInt("FARM_MAILBOX_MAX_SIZE", 64),
		MailboxInitialSize:       envInt("FARM_MAILBOX_INITIAL_SIZE", 16),
		FarmEventLaneSize:        envInt("FARM_EVENT_LANE_SIZE", 128),
		Activation:               actOpts,
		TheftPRF:                 theftPRF,
		ModelFieldV2Conversion:   modelFieldV2Conversion,
		NewModelLayout:           authoritystore.ModelLayoutV2,
		OpportunityWakeupEnabled: envBool("FARM_OPPORTUNITY_WAKEUP_ATTACH_ENABLED", false),
		Passivation: farmactor.PassivationOptions{
			IdleTimeout:       envDuration("FARM_ACTOR_IDLE_TIMEOUT", 5*time.Minute),
			BucketWidth:       envDuration("FARM_ACTOR_IDLE_BUCKET_WIDTH", 30*time.Second),
			SweepInterval:     envDuration("FARM_ACTOR_IDLE_SWEEP_INTERVAL", 30*time.Second),
			MaxPerSweep:       envInt("FARM_ACTOR_IDLE_MAX_PER_SWEEP", 256),
			BlockedRetryDelay: envDuration("FARM_ACTOR_IDLE_BLOCKED_RETRY_DELAY", 5*time.Second),
		},
	}
	guardSnapshotStore, guardSnapshotErr := theftonline.NewRedisGuardSnapshotStore(redisClients.Authority,
		env("THEFT_GUARD_SNAPSHOT_PREFIX", "farm:v121:m1:theft-guard"))
	if guardSnapshotErr != nil {
		return guardSnapshotErr
	}
	registryOptions.ParticipantGuardSnapshotReader = guardSnapshotStore
	var registry *farmactor.Registry
	var authorityRuntime *authoritycomposition.Runtime
	var assignment *authoritycomposition.AssignmentComposition
	var assignmentClient interface{ Close() error }
	var drainGate *actorruntime.ShardDrainGate
	var holderProcess *gatewayapp.HolderProcess
	var holderAssignmentMonitorDone <-chan struct{}
	var assignmentRunDone <-chan struct{}
	var draining atomic.Bool
	var authorityShardCount uint32
	var plantRouteResolver *actorrouting.StaticResolver
	var routeFence participantproof.RouteFenceVerifier
	var commitProofSigner *sourceorder.CommitProofSigner
	var commitProofPublication holderkey.Publication
	if mode == authoritycomposition.ModeAuthority {
		var authorityOptions authoritycomposition.Options
		if keyspaceTuple != nil {
			authorityOptions, err = authoritycomposition.OptionsFromEnvironmentWithProfile(redisClients.Authority, actorrouting.DomainFarm, keyspaceTuple.Profile())
		} else {
			authorityOptions, err = authoritycomposition.OptionsFromEnvironment(redisClients.Authority, actorrouting.DomainFarm)
		}
		if err != nil {
			return err
		}
		authorityOptions.OnFatal = func(err error) { log.Printf("farm-svr authority runtime fatal: %v", err) }
		redisClients.Authority.AddHook(layoutMetrics)
		authorityOptions.CommitObserver = layoutMetrics
		if authoritycomposition.HolderCompositionEnabled(
			membershipMode,
			assignmentMode,
		) {
			authorityOptions.MembershipInstallerDecorator = func(
				inner authoritycomposition.MembershipInstaller,
			) (authoritycomposition.MembershipInstaller, error) {
				return holdercontrol.NewPrepareOnlyMembershipInstaller(
					holdercontrol.PrepareOnlyMembershipInstallerOptions{
						Inner: inner,
						MaxEntries: envInt(
							"HOLDER_PREPARED_MAX_ENTRIES",
							holdercontrol.DefaultPreparedMembershipMaxEntries,
						),
						MaxBytes: envInt(
							"HOLDER_PREPARED_MAX_BYTES",
							holdercontrol.DefaultPreparedMembershipMaxBytes,
						),
						Retention: envDuration(
							"HOLDER_PREPARED_RETENTION",
							holdercontrol.DefaultPreparedMembershipRetention,
						),
					},
				)
			}
		}
		if assignmentMode == authoritycomposition.AssignmentModeRemoteWatch {
			dialCtx, dialCancel := context.WithTimeout(context.Background(), authoritycomposition.RemoteDialTimeoutFromEnvironment())
			assignmentClient, err = authoritycomposition.NewRemoteSnapshotClientFromEnvironment(dialCtx)
			dialCancel()
			if err != nil {
				return err
			}
			defer assignmentClient.Close()
			remoteSource, ok := assignmentClient.(interface {
				FetchFullSnapshot(context.Context) (actorrouting.RouteSnapshot, error)
				WatchFrom(context.Context, uint64) (<-chan actorrouting.RouteSnapshot, error)
			})
			if !ok {
				return errors.New("farm-svr remote assignment client does not implement snapshot source")
			}
			initialCtx, initialCancel := context.WithTimeout(context.Background(), authoritycomposition.RemoteInitialFetchTimeoutFromEnvironment())
			assignment, err = authoritycomposition.NewRemoteAssignmentComposition(initialCtx, remoteSource, authoritycomposition.RemoteAssignmentOptionsFromEnvironment())
			initialCancel()
			if err != nil {
				return err
			}
			if err := assignment.ConfigureEndpointConnections(authoritycomposition.EndpointConnectionOptionsFromEnvironment()); err != nil {
				return err
			}
			authorityOptions.AssignmentMode = assignmentMode
		}
		// RT1B-3C-B: composed mode (AUTHORITY_MEMBERSHIP_MODE=true) composes
		// NewMembershipRuntime (per-shard scheduler is the sole renew owner,
		// legacy runRenewal never starts); legacy mode keeps the old New path.
		// A construction failure is fatal -- there is no silent fallback.
		if assignment != nil {
			runtimeStartupCtx, runtimeStartupCancel := context.WithTimeout(context.Background(), authoritycomposition.RemoteStartupTimeoutFromEnvironment())
			authorityRuntime, err = authoritycomposition.NewRuntimeForModeWithAssignment(runtimeStartupCtx, authorityOptions, membershipMode, assignment)
			runtimeStartupCancel()
		} else {
			authorityRuntime, err = authoritycomposition.NewRuntimeForMode(context.Background(), authorityOptions, membershipMode)
		}
		if err != nil {
			return err
		}
		defer authorityRuntime.Close()
		if assignment != nil {
			defer assignment.Close()
		}
		authorityRuntime.Resolver.SetMetrics(routeMetrics)
		if assignment != nil {
			assignment.Pump.SetMetrics(routeMetrics)
		}
		plantRouteResolver = authorityRuntime.Resolver
		routeFence, err = authoritycomposition.NewParticipantRouteFence(authorityRuntime)
		if err != nil {
			return err
		}
		shardCount, err := authorityRuntime.Calculator.ShardCount(authorityOptions.Domain)
		if err != nil {
			return fmt.Errorf("farm-svr shard count: %w", err)
		}
		authorityShardCount = shardCount
		drainGate, err = actorruntime.NewShardDrainGate(actorruntime.ShardDrainGateOptions{
			ShardCount: shardCount,
			ShardFor: func(actorID uint64) (uint32, error) {
				return authorityRuntime.Calculator.ShardFor(authorityOptions.Domain, actorID)
			},
		})
		if err != nil {
			return fmt.Errorf("farm-svr shard drain gate: %w", err)
		}
		registryOptions.AdmissionGate = drainGate
		registry = farmactor.NewAuthorityRegistry(authorityRuntime.Provider, registryOptions)
		log.Printf("farm-svr authority mode enabled holder=%s boot=%s snapshot=%s membership=%s", authorityOptions.HolderID, authorityOptions.HolderBootID, authorityOptions.RouteSnapshotPath, membershipMode)
		if rb := authorityRuntime.RecoveryBudget(); rb != (authoritycomposition.RecoveryBudgetInfo{}) {
			log.Printf("farm-svr lease recovery budget margin_ms=%d base_ms=%d max_ms=%d jitter_ms=%d attempt_timeout_ms=%d slack_ms=%d completion_budget_ms=%d satisfied=%v",
				rb.SafetyMargin.Milliseconds(), rb.RecoveryBaseDelay.Milliseconds(), rb.RecoveryMaxDelay.Milliseconds(), rb.RecoveryJitterMax.Milliseconds(), rb.RecoveryAttemptTimeout.Milliseconds(), rb.RecoverySchedulerSlack.Milliseconds(), rb.CompletionBudget.Milliseconds(), rb.Satisfied)
		}
	} else {
		var legacyStore blobstore.Store
		var err error
		legacyStore, err = blobstore.NewRedisStore(redisClients.Authority, env("FARM_STORE_PREFIX", "farm:v118:dev:farm"))
		if err != nil {
			return err
		}
		legacyStore = blobstore.NewMeasuredStore(legacyStore, metricsRegistry, "farm")
		registry = farmactor.NewRegistryWithOptions(legacyStore, registryOptions)
		plantRouteResolver = configureActorRoute(registry, routeMetrics)
		log.Printf("farm-svr actor authority mode=%s", mode)
	}
	registry.SubscriptionHub().SetSubscriptionLeaseConfig(
		envDuration("FARM_SUBSCRIPTION_LEASE_INITIAL_TTL", 30*time.Second),
		envDuration("FARM_SUBSCRIPTION_LEASE_MAX_EXTENSION", 30*time.Second),
	)
	defer registry.Stop()
	if authorityRuntime != nil && authoritycomposition.HolderCompositionEnabled(membershipMode, assignmentMode) {
		var holderRegistry *routecontrol.HolderRegistry
		var err error
		commitProofSigner, commitProofPublication, err = sourceorder.NewEphemeralHolderBootSigner(
			plantRouteResolver.Realm(), authorityRuntime.HolderID, authorityRuntime.HolderBootID)
		if err != nil {
			return fmt.Errorf("farm-svr ephemeral holder-boot signer: %w", err)
		}
		holderProcess, holderRegistry, err = buildFarmHolderProcess(redisClients.Authority, authorityRuntime, assignment, registry, drainGate, &draining, commitProofPublication)
		if err != nil {
			return err
		}
		defer holderRegistry.Close()
	}
	registry.StartIdleSweeper(context.Background())
	reaperCtx, stopReaper := context.WithCancel(context.Background())
	defer stopReaper()
	// The GateBootReaper watches boot fences Renewed by farm-gate. Runtime is
	// the bounded live-state domain; the boot lease must never touch Authority.
	leaseStore, storeErr := bootlease.NewRedisStore(redisClients.Runtime, env("GATE_BOOT_LEASE_PREFIX", bootlease.DefaultRedisPrefix))
	if storeErr != nil {
		return fmt.Errorf("build GateBootReaper lease store: %w", storeErr)
	}
	reaper, reaperErr := subscriptionreaper.New(registry.SubscriptionHub(), leaseStore,
		envDuration("GATE_BOOT_REAPER_INTERVAL", 5*time.Second),
		envDuration("GATE_BOOT_REAPER_CHECK_TIMEOUT", 2*time.Second),
		subscriptionreaper.Mode(env("GATE_BOOT_REAPER_MODE", string(subscriptionreaper.ModeEnabled))))
	if reaperErr != nil {
		return fmt.Errorf("build GateBootReaper: %w", reaperErr)
	}
	subscriptionreaper.RegisterMetrics(metricsRegistry, reaper)
	go reaper.Run(reaperCtx)
	if reaper.Mode() == subscriptionreaper.ModeDisabled {
		log.Printf("GateBootReaper mode=disabled; subscription lifecycle owned by BatchRenew leases (contract 26 section 11.5)")
	} else {
		log.Printf("GateBootReaper mode=%s; checking only boot fences present in this holder's SubscriptionHub", reaper.Mode())
	}
	metricsRegistry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: "subscription_cleanup_actor_scan_total",
		Help: "Legacy connection cleanup invocations that scan every active FarmActor; exact disconnect cleanup must keep this at zero.",
	}, func() float64 { return float64(registry.CleanupActorScanTotal()) }))
	hub := registry.SubscriptionHub()
	metricsRegistry.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "farm_subscription_hub_active_lanes", Help: "FarmEventLanes currently owned by this holder's SubscriptionHub."}, func() float64 { return float64(hub.Stats().ActiveLanes) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "farm_subscription_hub_boot_fences", Help: "Distinct GateBoot fences represented by local Presence."}, func() float64 { return float64(hub.Stats().BootFences) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "farm_event_lane_full_total", Help: "FarmEventLane bounded admission failures."}, func() float64 { return float64(hub.Stats().LaneFull) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "farm_event_lane_resync_total", Help: "Presence generations forced to RESYNC_REQUIRED after a committed change could not enter its FarmEventLane."}, func() float64 { return float64(hub.Stats().LaneResync) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "farm_subscription_hub_lanes_retired_total", Help: "Empty FarmEventLanes independently retired by SubscriptionHub."}, func() float64 { return float64(hub.Stats().LanesRetired) }),
		// W1A(26 §12 Join 状态机观测):各阶段订阅数 + post-ready 缓冲长度。
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "farm_subscription_hub_subscription_state", Help: "Subscriptions per join state (26 section 12).", ConstLabels: prometheus.Labels{"state": "joining_unarmed"}}, func() float64 {
			return float64(hub.StateCounts(context.Background()).JoiningUnarmed)
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "farm_subscription_hub_subscription_state", Help: "Subscriptions per join state (26 section 12).", ConstLabels: prometheus.Labels{"state": "joining_armed"}}, func() float64 {
			return float64(hub.StateCounts(context.Background()).JoiningArmed)
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "farm_subscription_hub_subscription_state", Help: "Subscriptions per join state (26 section 12).", ConstLabels: prometheus.Labels{"state": "flushing"}}, func() float64 {
			return float64(hub.StateCounts(context.Background()).Flushing)
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "farm_subscription_hub_subscription_state", Help: "Subscriptions per join state (26 section 12).", ConstLabels: prometheus.Labels{"state": "activating"}}, func() float64 {
			return float64(hub.StateCounts(context.Background()).Activating)
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "farm_subscription_hub_subscription_state", Help: "Subscriptions per join state (26 section 12).", ConstLabels: prometheus.Labels{"state": "in_farm"}}, func() float64 {
			return float64(hub.StateCounts(context.Background()).InFarm)
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "farm_subscription_hub_post_ready_buffer_len", Help: "Buffered post-ready changes waiting behind SUBSCRIPTION_READY."}, func() float64 {
			return float64(hub.StateCounts(context.Background()).PostReadyLen)
		}),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "farm_subscription_hub_joining_buffer_dropped_total", Help: "Subscriptions invalidated by joining/post-ready buffer overflow."}, func() float64 { return float64(hub.JoiningBufferDropped()) }),
		// Hard correctness gate: a change the subscriber was owed never arrived.
		// Registered here so that a healthy run reports an explicit zero rather
		// than an absent series the capacity oracle would read as unobserved.
		prometheus.NewCounterFunc(prometheus.CounterOpts{Name: "farm_change_version_gap_total", Help: "Breaks observed in the delivered FarmChange version sequence."}, func() float64 { return float64(hub.VersionGaps()) }),
		// W1B(27 §5.17):level-triggered 实时失效状态。
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "farm_realtime_invalid_farms", Help: "Farms whose realtime stream is currently marked invalid."}, func() float64 {
			return float64(hub.RealtimeCounts(context.Background()).InvalidFarms)
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "farm_realtime_delivery_generation_sum", Help: "Sum of delivery generations across farm lanes."}, func() float64 {
			return float64(hub.RealtimeCounts(context.Background()).DeliveryGenerationSum)
		}),
	)
	realtimeInvalid := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "farm_realtime_invalid_total",
		Help: "Realtime invalidations by reason (level-triggered, cleared only by fresh baseline).",
	}, []string{"reason"})
	metricsRegistry.MustRegister(realtimeInvalid)
	hub.SetInvalidObserver(func(reason string) { realtimeInvalid.WithLabelValues(reason).Inc() })
	// W5B(26 §11.5):Hub 侧逐订阅续租结果与租约过期移除统计。
	subscriptionLeaseResults := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "farm_subscription_lease_results_total",
		Help: "Hub-side subscription lease renew results by status (26 section 11.5).",
	}, []string{"status"})
	metricsRegistry.MustRegister(subscriptionLeaseResults)
	hub.SetLeaseObserver(func(status string) { subscriptionLeaseResults.WithLabelValues(status).Inc() })
	subscriptionLeaseExpired := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "farm_subscription_lease_expired_total",
		Help: "Presences removed because their renewable subscription lease expired.",
	})
	metricsRegistry.MustRegister(subscriptionLeaseExpired)
	hub.SetLeaseExpiredObserver(func() { subscriptionLeaseExpired.Inc() })
	registry.SetRouteMetrics(routeMetrics)
	turnLatency := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "farm", Subsystem: "actor", Name: "turn_latency_seconds",
		Help:        "Actor turn execution latency (task only, queue wait excluded).",
		ConstLabels: prometheus.Labels{"actor_kind": "farm"},
		Buckets:     []float64{.00005, .0001, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	})
	metricsRegistry.MustRegister(turnLatency)
	registry.SetTurnObserver(func(_ uint64, duration time.Duration) { turnLatency.Observe(duration.Seconds()) })

	var player farmingress.PlayerParticipant
	var typedPlayerClients map[string]contractspb.CoordinatorParticipantServiceClient
	var participantRouteResolver actorrouting.Resolver
	var playerConn *grpc.ClientConn
	var playerRouteConns []*grpc.ClientConn
	if plantRouteResolver != nil {
		resolver := plantRouteResolver
		var routeErr error
		maxEndpoints := authoritycomposition.ActorRouteEndpointLimitFromEnvironment()
		if len(resolver.Endpoints()) > maxEndpoints {
			return fmt.Errorf("farm-svr route snapshot has %d endpoints, exceeds ACTOR_ROUTE_MAX_ENDPOINTS=%d", len(resolver.Endpoints()), maxEndpoints)
		}
		participantRouteResolver = resolver
		var pooledConn grpc.ClientConnInterface
		if assignment != nil {
			participantRouteResolver, routeErr = authoritycomposition.NewPooledResolverForDomains(
				resolver, assignment.Endpoints, actorrouting.DomainPlayer,
			)
			if routeErr == nil {
				pooledConn, routeErr = authoritycomposition.NewPooledClientConn(resolver, assignment.Endpoints)
			}
		}
		routes, routeErr := actorrouting.NewActorRouteClient(participantRouteResolver, time.Second)
		if routeErr != nil {
			return routeErr
		}
		clients := make(map[string]logicrpcpb.PlayerParticipantServiceClient)
		typedPlayerClients = make(map[string]contractspb.CoordinatorParticipantServiceClient)
		if pooledConn != nil {
			clients[authoritycomposition.PooledEndpoint] = logicrpcpb.NewPlayerParticipantServiceClient(pooledConn)
			typedPlayerClients[authoritycomposition.PooledEndpoint] = contractspb.NewCoordinatorParticipantServiceClient(pooledConn)
		} else {
			for _, endpoint := range resolver.EndpointsFor(actorrouting.KindPlayer) {
				connection, dialErr := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if dialErr != nil {
					for _, conn := range playerRouteConns {
						_ = conn.Close()
					}
					return fmt.Errorf("dial player holder %s: %w", endpoint, dialErr)
				}
				playerRouteConns = append(playerRouteConns, connection)
				clients[endpoint] = logicrpcpb.NewPlayerParticipantServiceClient(connection)
				typedPlayerClients[endpoint] = contractspb.NewCoordinatorParticipantServiceClient(connection)
			}
		}
		for _, connection := range playerRouteConns {
			defer connection.Close()
		}
		player, routeErr = farmingress.NewRoutedPlayerParticipant(resolver.Realm(), routes, clients)
		if routeErr != nil {
			return routeErr
		}
		log.Printf("using routed player participants across %d endpoints", len(resolver.EndpointsFor(actorrouting.KindPlayer)))
	} else if playerAddr := os.Getenv("PLAYER_ADDR"); playerAddr != "" {
		var err error
		playerConn, err = grpc.Dial(playerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial player-svr %s: %w", playerAddr, err)
		}
		defer playerConn.Close()
		player = farmingress.NewGRPCPlayerParticipant(logicrpcpb.NewPlayerParticipantServiceClient(playerConn))
		log.Printf("using player-svr participant at %s", playerAddr)
	} else {
		log.Println("PLAYER_ADDR is empty; read/join work but cross-authority farm workflows fail closed")
	}
	service := farmingress.NewService(registry, player)
	friendNamespace, err := rediskey.NewNamespace(env("REDIS_NAMESPACE_ENVIRONMENT", "dev"), env("REDIS_NAMESPACE_ZONE", "z1"))
	if err != nil {
		return fmt.Errorf("build friend opportunity hint namespace: %w", err)
	}
	friendIndex, err := friend.NewRedisFriendIndexRepository(redisClients.Projection, friend.DefaultFriendRedisProfile(friendNamespace))
	if err != nil {
		return fmt.Errorf("build friend opportunity index projection: %w", err)
	}
	navigation, err := friend.NewRedisNavigationVisitedRepository(redisClients.Projection,
		friend.DefaultFriendRedisProfile(friendNamespace), friend.DefaultNavigationVisitedTTL)
	if err != nil {
		return fmt.Errorf("build friend navigation projection: %w", err)
	}
	friendOpportunityHints, err := friend.NewRedisOpportunityHintBus(redisClients.Projection, friendNamespace)
	if err != nil {
		return fmt.Errorf("build friend opportunity hint publisher: %w", err)
	}
	service.SetFriendOpportunityHintPublisher(targetedFriendOpportunityPublisher{
		bus: friendOpportunityHints, index: friendIndex,
	})
	service.SetFriendNavigationVisitedRepository(navigation)
	var headerReadModeWatcher *dynamicconfig.FileValue
	var stopHeaderReadModeWatcher context.CancelFunc
	defer func() {
		if stopHeaderReadModeWatcher != nil {
			stopHeaderReadModeWatcher()
		}
	}()
	if authorityRuntime != nil {
		headerReadMode, modeErr := readpath.ParseHeaderReadMode(os.Getenv("FARM_OPPORTUNITY_HEADER_READ_MODE"))
		if modeErr != nil {
			return fmt.Errorf("parse friend opportunity header read mode: %w", modeErr)
		}
		headerReadMetrics := &readpath.HeaderReadMetrics{}
		readpath.RegisterHeaderReadMetrics(metricsRegistry, headerReadMetrics)
		friendOpportunityReader, readErr := readpath.NewProviderWithHeaderReadOptions(authorityRuntime.Store, readpath.HeaderReadOptions{
			Mode:            headerReadMode,
			NarrowReader:    authorityRuntime.Store,
			MaxFullFallback: envInt("FARM_OPPORTUNITY_HEADER_FULL_FALLBACK", 8),
			Observer:        headerReadMetrics,
		})
		if readErr != nil {
			return fmt.Errorf("build friend opportunity authority reader: %w", readErr)
		}
		readpath.RegisterHeaderReadModeMetrics(
			metricsRegistry,
			friendOpportunityReader,
		)
		modeFile := strings.TrimSpace(os.Getenv("FARM_OPPORTUNITY_HEADER_MODE_FILE"))
		if modeFile != "" {
			headerReadModeWatcher, readErr = dynamicconfig.NewFileValue(
				modeFile,
				envDuration(
					"FARM_OPPORTUNITY_HEADER_MODE_POLL_INTERVAL",
					time.Second,
				),
				func(raw string) error {
					mode, parseErr := readpath.ParseHeaderReadMode(raw)
					if parseErr != nil {
						return parseErr
					}
					return friendOpportunityReader.SetHeaderReadMode(mode)
				},
			)
			if readErr != nil {
				return fmt.Errorf(
					"build friend opportunity header mode watcher: %w",
					readErr,
				)
			}
			// The mode file is the live operator-owned value and the watcher has
			// already applied it to the reader. Refusing to boot on a difference
			// would leave every process unable to restart once the mode ladder
			// has moved past the environment seed.
			if current := headerReadModeWatcher.Current(); current != string(headerReadMode) {
				log.Printf(
					"friend opportunity header boot mode adopted from file: env=%s file=%s",
					headerReadMode,
					current,
				)
			}
			watchContext, cancelWatch := context.WithCancel(context.Background())
			stopHeaderReadModeWatcher = cancelWatch
			go func() {
				err := headerReadModeWatcher.Run(
					watchContext,
					func(reloadErr error) {
						log.Printf(
							"friend opportunity header mode reload failed closed: %v",
							reloadErr,
						)
					},
				)
				if err != nil && !errors.Is(err, context.Canceled) {
					log.Printf(
						"friend opportunity header mode watcher stopped fail-closed: %v",
						err,
					)
				}
			}()
		}
		service.SetFriendOpportunityReader(friendOpportunityReader)
		log.Printf("friend opportunity header read mode=%s full_fallback=%d", headerReadMode, envInt("FARM_OPPORTUNITY_HEADER_FULL_FALLBACK", 8))
	}
	if mode == authoritycomposition.ModeLegacy && envBool("CARE_LEGACY_ROLLBACK_ENABLED", false) {
		service.SetLegacyCareEnabled(true)
		log.Printf("legacy CARE rollback switch enabled explicitly")
	} else if mode == authoritycomposition.ModeLegacy {
		log.Printf("legacy CARE disabled by default; set CARE_LEGACY_ROLLBACK_ENABLED=true only for rollback/test")
	}
	var commitProofIssuer *sourceorder.CommitProofIssuer
	if authorityRuntime != nil && commitProofMode != sourceorder.CommitProofModeOff {
		var signerErr error
		if commitProofSigner == nil {
			signerErr = sourceorder.ErrCommitProofMissing
		} else {
			commitProofIssuer, signerErr = sourceorder.NewPublishedCommitProofIssuer(commitProofSigner, commitProofPublication,
				plantRouteResolver, plantRouteResolver.Realm(), envDuration(sourceorder.CommitProofTTLEnv, sourceorder.DefaultCommitProofTTL),
				envDuration(sourceorder.CommitProofLeaseSafetyEnv, sourceorder.DefaultCommitProofSafetyMargin), time.Now)
		}
		if signerErr != nil {
			if commitProofMode == sourceorder.CommitProofModePrefer || !errors.Is(signerErr, sourceorder.ErrCommitProofMissing) {
				return fmt.Errorf("farm-svr holder-boot commit proof signer: %w", signerErr)
			}
			log.Printf("source-order commit proof shadow signer unavailable; Redis remains authoritative: %v", signerErr)
		}
	}
	var careDirect *carerecovery.DirectDispatcher
	careDirectShutdownAttempted := false
	shutdownCareDirect := func() error {
		if careDirect == nil || careDirectShutdownAttempted {
			return nil
		}
		careDirectShutdownAttempted = true
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), directShutdownTimeout())
		defer shutdownCancel()
		return careDirect.Shutdown(shutdownCtx)
	}
	if authorityRuntime != nil && participantRouteResolver != nil && len(typedPlayerClients) != 0 {
		route, routeErr := carerecovery.NewPlayerRouteProvider(participantRouteResolver, plantRouteResolver.Realm())
		if routeErr == nil {
			coordinatorClient, clientErr := plantonline.NewRoutedCoordinatorParticipantClient(
				participantRouteResolver, plantRouteResolver.Realm(), actorrouting.DomainPlayer, typedPlayerClientMap(typedPlayerClients))
			if clientErr == nil {
				careDirect, clientErr = carerecovery.NewDirectDispatcher(carerecovery.DirectConfig{
					WorkLease: redisworkload.NewAttributedWorkLease(authorityRuntime.Store.WorkLease()), SourceOrders: authorityRuntime.Store.WorkLease(),
					Route: route, Player: coordinatorClient, Owner: authorityRuntime.HolderID,
					Token: env("CARE_DIRECT_LEASE_TOKEN", "care-direct-"+authorityRuntime.HolderBootID), BootID: authorityRuntime.HolderBootID,
					BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) {
						return authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
							actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
					},
					BucketForPlayer: func(playerID uint64) (rediskey.AuthorityBucketRef, error) {
						return authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
							actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: playerID})
					},
					LeaseTTL:            envDuration("CARE_DIRECT_LEASE", 10*time.Second),
					SafetyMargin:        envDuration("CARE_DIRECT_LEASE_SAFETY_MARGIN", 2*time.Second),
					ItemDeadline:        envDuration("CARE_DIRECT_OPERATION_TIMEOUT", 3*time.Second),
					ParticipantBudgetMs: uint32(envInt("CARE_DIRECT_PARTICIPANT_BUDGET_MS", 5000)),
					MaxItems:            envInt("CARE_DIRECT_QUEUE_SIZE", 1024), MaxBytes: envInt("CARE_DIRECT_QUEUE_BYTES", 4<<20),
					Workers:             envInt("CARE_DIRECT_WORKERS", 4),
					CompletionBatchSize: envInt("CARE_DIRECT_COMPLETION_BATCH_SIZE", 64),
					CompletionBatchWait: envDuration("CARE_DIRECT_COMPLETION_BATCH_WAIT", time.Millisecond), Now: time.Now,
					CommitProofMode: commitProofMode, CommitProofIssuer: commitProofIssuer,
				})
			}
			if clientErr != nil {
				log.Printf("Care direct dispatcher disabled: %v", clientErr)
			} else {
				registerSourceOrderDirectMetrics(metricsRegistry, careDirect)
				// Early-return cleanup still runs before the previously registered
				// Authority/Redis defers. The normal signal path performs this step
				// explicitly before beginning runtime drain.
				defer func() {
					if shutdownErr := shutdownCareDirect(); shutdownErr != nil {
						log.Printf("farm-svr Direct early shutdown: %v", shutdownErr)
					}
				}()
				service.SetSourceCommitEvidenceEnabled(commitProofMode != sourceorder.CommitProofModeOff)
				service.SetCareOrderEnqueuer(func(ctx context.Context, farmID uint64, order authoritystore.AttachedOrder, evidence *authoritystore.ExactCommitEvidence) error {
					bucket, err := authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
						actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
					if err != nil {
						return err
					}
					address, err := carerecovery.CareWorkAddress(bucket, order.OrderID, farmID)
					if err != nil {
						return err
					}
					return careDirect.EnqueueCommittedWithEvidence(address, order, evidence)
				})
				service.SetHarvestOrderEnqueuer(func(ctx context.Context, farmID uint64, order authoritystore.AttachedOrder, evidence *authoritystore.ExactCommitEvidence) error {
					bucket, err := authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
						actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
					if err != nil {
						return err
					}
					address, err := sourceorder.WorkAddress(bucket, string(identity.WorkTypeHarvest), order.OrderID, workidentity.RouteDomainFarm, farmID)
					if err != nil {
						return err
					}
					return careDirect.EnqueueCommittedWithEvidence(address, order, evidence)
				})
				service.SetPlayerCoordinator(func(ctx context.Context, req *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error) {
					return coordinatorClient.ExecuteStep(ctx, req)
				}, func(ctx context.Context, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
					return route(ctx, actorID)
				})
				service.SetPlayerBucketResolver(func(playerID uint64) (rediskey.AuthorityBucketRef, error) {
					return authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
						actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: playerID})
				})
				service.SetPlantOrderDispatcher(func(ctx context.Context, playerID uint64, orderID string) error {
					bucket, err := authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
						actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: playerID})
					if err != nil {
						return err
					}
					address, err := sourceorder.WorkAddress(bucket, string(identity.WorkTypePlant), orderID, workidentity.RouteDomainPlayer, playerID)
					if err != nil {
						return err
					}
					return careDirect.DispatchOnce(ctx, address)
				})
				service.SetUnlockOrderDispatcher(func(ctx context.Context, playerID uint64, orderID string) error {
					bucket, err := authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
						actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: playerID})
					if err != nil {
						return err
					}
					address, err := sourceorder.WorkAddress(bucket, string(identity.WorkTypeUnlockPlot), orderID, workidentity.RouteDomainPlayer, playerID)
					if err != nil {
						return err
					}
					return careDirect.DispatchOnce(ctx, address)
				})
				service.SetPlantCommittedOrderDispatcher(func(ctx context.Context, playerID uint64, order authoritystore.AttachedOrder) error {
					bucket, err := authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
						actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: playerID})
					if err != nil {
						return err
					}
					address, err := sourceorder.WorkAddress(bucket, string(identity.WorkTypePlant), order.OrderID, workidentity.RouteDomainPlayer, playerID)
					if err != nil {
						return err
					}
					return careDirect.DispatchCommittedOnce(ctx, address, order)
				})
				service.SetUnlockCommittedOrderDispatcher(func(ctx context.Context, playerID uint64, order authoritystore.AttachedOrder) error {
					bucket, err := authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
						actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: playerID})
					if err != nil {
						return err
					}
					address, err := sourceorder.WorkAddress(bucket, string(identity.WorkTypeUnlockPlot), order.OrderID, workidentity.RouteDomainPlayer, playerID)
					if err != nil {
						return err
					}
					return careDirect.DispatchCommittedOnce(ctx, address, order)
				})
				service.SetTheftOrderEnqueuer(func(ctx context.Context, farmID uint64, orderID string) error {
					bucket, err := authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
						actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
					if err != nil {
						return err
					}
					address, err := sourceorder.WorkAddress(bucket, string(identity.WorkTypeTheftVisitor), orderID, workidentity.RouteDomainFarm, farmID)
					if err != nil {
						return err
					}
					return careDirect.Enqueue(address, 0)
				}, func(ctx context.Context, farmID uint64, orderID string) error {
					bucket, err := authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
						actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
					if err != nil {
						return err
					}
					address, err := sourceorder.WorkAddress(bucket, string(identity.WorkTypeTheftGuard), orderID, workidentity.RouteDomainFarm, farmID)
					if err != nil {
						return err
					}
					return careDirect.Enqueue(address, 0)
				})
				log.Printf("Care/Harvest/Plant/Unlock/Theft direct dispatcher enabled on shared Authority SourceOrder/WorkLease")
			}
		} else {
			log.Printf("Care direct dispatcher disabled: %v", routeErr)
		}
	} else if authorityRuntime != nil {
		log.Printf("Care direct dispatcher disabled: Authority route and typed Player Coordinator clients are required; DueIndex recovery remains enabled")
	}
	var typedFarmParticipant *farmingress.Server
	if mode == authoritycomposition.ModeAuthority {
		var playerSource farmingress.PlayerSourceOrderVerifier
		if authorityRuntime != nil {
			verifier, verifierErr := farmingress.NewRedisPlayerSourceVerifier(authorityRuntime.Store,
				func(playerID uint64) (rediskey.AuthorityBucketRef, error) {
					return authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
						actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: playerID})
				})
			if verifierErr != nil {
				log.Printf("farm Player SourceOrder verifier disabled: %v", verifierErr)
			} else {
				playerSource = verifier
			}
		}
		if friendshipRepositories == nil {
			return errors.New("farm participant Pair recovery authority is unavailable")
		}
		pairLeases, err := theftrecovery.NewFriendPairWorkLeaseRepository(friendshipRepositories, 10*time.Millisecond, 2*time.Minute)
		if err != nil {
			return fmt.Errorf("build Pair recovery lease verifier: %w", err)
		}
		theftRecovery, err := theftonline.NewPairDecideRecoveryVerifier(friendshipRepositories, pairLeases,
			func(farmID uint64) (rediskey.AuthorityBucketRef, error) {
				return authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
					actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
			})
		if err != nil {
			return fmt.Errorf("build Pair DECIDE_THEFT recovery verifier: %w", err)
		}
		typedFarmParticipant, err = buildFarmParticipantServerChecked(registry, workRepository, plantRouteResolver, routeFence, authorityRuntime.Placement, authorityRuntime.Store, playerSource, theftRecovery, service.EmitChange)
		if err != nil {
			return err
		}
	} else {
		typedFarmParticipant = buildFarmParticipantServer(registry, workRepository, plantRouteResolver, service.EmitChange)
	}
	if typedFarmParticipant != nil {
		// COMMIT_PLANT's typed Participant observer now owns the single
		// post-CAS FarmChange handoff. The public PLANT command path must not
		// emit the same ActionResult a second time.
		service.SetPlantParticipantObserverConfigured()
		if careDirect != nil && participantRouteResolver != nil && plantRouteResolver != nil {
			if farmRoute, farmRouteErr := carerecovery.NewFarmRouteProvider(participantRouteResolver, plantRouteResolver.Realm()); farmRouteErr != nil {
				log.Printf("Plant/Unlock Farm dest route disabled: %v", farmRouteErr)
			} else {
				careDirect.SetFarmDest(plantonline.LocalCoordinatorClient{Server: typedFarmParticipant}, farmRoute,
					func(playerID uint64) (rediskey.AuthorityBucketRef, error) {
						return authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
							actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: playerID})
					})
			}
		}
	}
	// Start only after the local Farm participant destination has been attached;
	// this keeps mutable composition out of the dispatch/shutdown race surface.
	if careDirect != nil {
		careDirect.Start(context.Background())
	}
	var theftTargetStore theftonline.TargetBindingStore
	var theftTargetErr error
	theftTargetStore, theftTargetErr = theftonline.NewRedisTargetBindingStore(redisClients.Projection,
		env("THEFT_TARGET_BINDING_PREFIX", "farm:v121:m1:theft-target"),
		envDuration("THEFT_TARGET_BINDING_TTL", 7*24*time.Hour))
	if theftTargetErr != nil {
		log.Printf("public THEFT target binding sidecar unavailable: %v", theftTargetErr)
	}
	var theftDispatch farmingress.TheftDispatchPort
	var theftAsyncDispatch *theftonline.AsyncDispatch
	var theftResultProcessor *theftonline.ResultOrderProcessor
	if friendshipRepositories != nil && authorityRuntime != nil {
		resultPublisher, publisherErr := newFarmResultPublisher(redisClients.Projection, keyspaceTuple, os.Getenv)
		if publisherErr != nil {
			log.Printf("public THEFT result fast path unavailable: %v", publisherErr)
		} else {
			theftResultProcessor, publisherErr = theftonline.NewResultOrderProcessor(
				friendshipRepositories, resultPublisher, authorityRuntime.Store)
			if publisherErr != nil {
				log.Printf("public THEFT result fast path unavailable: %v", publisherErr)
			}
		}
	}
	if theftTargetStore != nil && friendshipRepositories != nil && registry != nil && authorityRuntime != nil {
		var rawTheftDispatch *theftonline.Dispatch
		rawTheftDispatch, theftTargetErr = theftonline.NewDispatch(theftonline.DispatchConfig{
			FastPathHolderID:   authorityRuntime.HolderID,
			FastPathBootID:     authorityRuntime.HolderBootID,
			FastPathLeaseToken: env("THEFT_DIRECT_LEASE_TOKEN", "theft-direct-"+authorityRuntime.HolderBootID),
			FastPathLeaseTTL:   envDuration("THEFT_DIRECT_LEASE_TTL", 5*time.Second),
			ResultProcessor:    theftResultProcessor,
			FinalizeNoChange: func(ctx context.Context, order theftrecovery.DispatchOrder) error {
				if theftResultProcessor == nil {
					return fmt.Errorf("theft result processor unavailable")
				}
				return theftResultProcessor.FinalizeNoChange(ctx, order)
			},
			PairRepositories: friendshipRepositories, TargetStore: theftTargetStore,
			MaxPairBuckets: envInt("THEFT_PAIR_LOOKUP_MAX_BUCKETS", int(friendshipRepositories.PairBucketCount())),
			SourceCommit: func(ctx context.Context, input theftscenario.Input) (string, string, error) {
				if registry == nil || authorityRuntime == nil {
					return "", "", fmt.Errorf("theft source-order farm ports are unavailable")
				}
				return farmingress.CommitTheftSource(ctx, registry, func(farmID uint64) (rediskey.AuthorityBucketRef, error) {
					return authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
						actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
				}, input, time.Now().UnixMilli(), service.EmitChange)
			},
			EnqueueVisitor: func(ctx context.Context, farmID uint64, orderID string) error {
				if careDirect == nil {
					return nil
				}
				bucket, err := authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
					actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
				if err != nil {
					return err
				}
				address, err := sourceorder.WorkAddress(bucket, string(identity.WorkTypeTheftVisitor), orderID, workidentity.RouteDomainFarm, farmID)
				if err != nil {
					return err
				}
				return careDirect.Enqueue(address, 0)
			},
			EnqueueGuard: func(ctx context.Context, farmID uint64, orderID string) error {
				if careDirect == nil {
					return nil
				}
				bucket, err := authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
					actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
				if err != nil {
					return err
				}
				address, err := sourceorder.WorkAddress(bucket, string(identity.WorkTypeTheftGuard), orderID, workidentity.RouteDomainFarm, farmID)
				if err != nil {
					return err
				}
				return careDirect.Enqueue(address, 0)
			},
		})
		if theftTargetErr != nil {
			log.Printf("public THEFT dispatch unavailable: %v", theftTargetErr)
		} else if envBool("THEFT_ASYNC_FASTPATH_ENABLED", true) {
			theftAsyncDispatch, theftTargetErr = theftonline.NewAsyncDispatch(theftonline.AsyncDispatchConfig{
				Delegate:    rawTheftDispatch,
				MaxItems:    envInt("THEFT_ASYNC_FASTPATH_MAX_ITEMS", 4096),
				MaxBytes:    int64(envInt("THEFT_ASYNC_FASTPATH_MAX_BYTES", 8<<20)),
				Workers:     envInt("THEFT_ASYNC_FASTPATH_WORKERS", 16),
				ItemTimeout: envDuration("THEFT_ASYNC_FASTPATH_ITEM_TIMEOUT", 5*time.Second),
			})
			if theftTargetErr != nil {
				log.Printf("public THEFT async fast path unavailable: %v", theftTargetErr)
			} else {
				theftDispatch = theftAsyncDispatch
			}
		} else {
			theftDispatch = rawTheftDispatch
		}
	}
	visitAuthorizer, err := friend.NewVisitAuthorizer(friendshipRepositories.Pair)
	if err != nil {
		return fmt.Errorf("build canonical friend VisitPermit authorizer: %w", err)
	}
	service.SetFriendVisitAuthorizer(visitAuthorizer)
	friendPermits, err := friend.NewPermitService(friendshipRepositories.Pair, permitSecret, friend.DefaultActionPermitTTL)
	if err != nil {
		return fmt.Errorf("build friend action permit service: %w", err)
	}
	service.SetFriendPermitAuthority(friendPermits)
	service.SetLegacyTheftRollbackEnabled(envBool("FARM_THEFT_LEGACY_ROLLBACK_ENABLED", false))
	if err := configureTheftIngress(service, friendshipRepositories, theftDispatch); err != nil {
		log.Printf("public THEFT ingress unavailable: %v", err)
	}
	if mode == authoritycomposition.ModeAuthority {
		// openStores already constructed and warmed the same namespace-backed Pair
		// repositories used by Theft. Care shares that authority instead of
		// installing a second repository instance over the service seam.
		service.SetFriendActionAdmission(friendshipRepositories.Pair)
		log.Printf("friend Pair Authority ActionAdmission enabled for CARE")
	}
	service.SetWorkflowStore(workflows)
	if duration, err := time.ParseDuration(env("FARM_CROP_MATURE_AFTER", "10s")); err == nil {
		service.SetMatureAfter(duration)
	}

	var dispatcher *logicdownlink.Dispatcher
	gatewayClient, gatewayConns, gatewayErr := openGatewayDownlinks()
	if gatewayErr != nil {
		return gatewayErr
	}
	for _, connection := range gatewayConns {
		defer connection.Close()
	}
	if gatewayClient != nil {
		dispatchConfig := logicdownlink.DefaultDispatcherConfig()
		dispatchConfig.LaneQueueSize = envInt("DOWNLINK_LANE_QUEUE_SIZE", dispatchConfig.LaneQueueSize)
		dispatchConfig.BatchSize = envInt("DOWNLINK_BATCH_SIZE", dispatchConfig.BatchSize)
		dispatchConfig.MaxConcurrent = envInt("DOWNLINK_MAX_CONCURRENT_RPCS", dispatchConfig.MaxConcurrent)
		dispatchConfig.MaxLanes = envInt("DOWNLINK_MAX_LANES", dispatchConfig.MaxLanes)
		dispatchConfig.LaneWorkers = envInt("DOWNLINK_LANE_WORKERS", dispatchConfig.LaneWorkers)
		if parsed, parseErr := time.ParseDuration(env("DOWNLINK_RPC_TIMEOUT", dispatchConfig.RPCTimeout.String())); parseErr == nil {
			dispatchConfig.RPCTimeout = parsed
		}
		if parsed, parseErr := time.ParseDuration(env("DOWNLINK_LANE_IDLE_TIMEOUT", dispatchConfig.LaneIdleTimeout.String())); parseErr == nil {
			dispatchConfig.LaneIdleTimeout = parsed
		}
		dispatchConfig.TransientMaxAttempts = envInt("DOWNLINK_TRANSIENT_MAX_ATTEMPTS", dispatchConfig.TransientMaxAttempts)
		if parsed, parseErr := time.ParseDuration(env("DOWNLINK_TRANSIENT_BACKOFF", dispatchConfig.TransientBackoff.String())); parseErr == nil {
			dispatchConfig.TransientBackoff = parsed
		}
		if parsed, parseErr := time.ParseDuration(env("DOWNLINK_TRANSIENT_BUDGET", dispatchConfig.TransientBudget.String())); parseErr == nil {
			dispatchConfig.TransientBudget = parsed
		}
		dispatcher = logicdownlink.NewDispatcherWithConfig(gatewayClient, dispatchConfig)
		defer dispatcher.Close()
		logicdownlink.RegisterMetrics(metricsRegistry, dispatcher)
		service.SetDownlinkEnqueuer(dispatcher)
	}

	rpcMetrics := grpcmetrics.New(metricsRegistry)
	server := grpc.NewServer(grpc.UnaryInterceptor(rpcMetrics.UnaryServerInterceptor()))
	defer server.Stop()
	logicrpcpb.RegisterLogicServiceServer(server, service)
	logicrpcpb.RegisterFarmWorkflowServiceServer(server, service)
	if typedFarmParticipant != nil {
		registerFarmParticipantServices(server, typedFarmParticipant)
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		if shutdownErr := shutdownCareDirect(); shutdownErr != nil {
			log.Printf("farm-svr Direct listen-failure shutdown: %v", shutdownErr)
		}
		return fmt.Errorf("farm-svr listen %s: %w", addr, err)
	}
	go func() {
		if err := server.Serve(listener); err != nil {
			log.Printf("farm-svr serve: %v", err)
		}
	}()
	if holderProcess != nil {
		if err := holderProcess.StartControlServer(context.Background()); err != nil {
			if shutdownErr := shutdownCareDirect(); shutdownErr != nil {
				log.Printf("farm-svr Direct HolderControl-start-failure shutdown: %v", shutdownErr)
			}
			return err
		}
		log.Printf("farm-svr HolderControl WARMING endpoint listening on %s", holderProcess.Listener.Addr())
	}
	if assignment != nil {
		runDone := make(chan struct{})
		assignmentRunDone = runDone
		go func() {
			defer close(runDone)
			if err := assignment.Run(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("farm-svr remote assignment composition terminal: %v", err)
			}
		}()
		defer func() {
			_ = assignment.Close()
			<-runDone
		}()
		startupCtx, startupCancel := context.WithTimeout(context.Background(), authoritycomposition.RemoteStartupTimeoutFromEnvironment())
		err := assignment.WaitHealthy(startupCtx)
		startupCancel()
		if err != nil {
			if shutdownErr := shutdownCareDirect(); shutdownErr != nil {
				log.Printf("farm-svr Direct assignment-startup-failure shutdown: %v", shutdownErr)
			}
			if holderProcess != nil {
				stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = holderProcess.GracefulStop(stopCtx)
				stopCancel()
			}
			return fmt.Errorf("farm-svr remote assignment startup: %w", err)
		}
	}
	if commitProofMode == sourceorder.CommitProofModePrefer {
		if err := sourceorder.ValidateProductionPreferComposition(commitProofMode, assignment != nil, commitProofSigner != nil); err != nil {
			return fmt.Errorf("farm-svr commit proof prefer composition: %w", err)
		}
		preferCtx, preferCancel := context.WithTimeout(context.Background(), authoritycomposition.RemoteStartupTimeoutFromEnvironment())
		err := sourceorder.WaitForCommitProofRouteReadiness(preferCtx, plantRouteResolver.Snapshot, func(snapshot actorrouting.RouteSnapshot) error {
			return sourceorder.ValidateLocalCommitProofSignerPublication(snapshot, actorrouting.DomainFarm,
				authorityRuntime.HolderID, authorityRuntime.HolderBootID, commitProofPublication)
		})
		preferCancel()
		if err != nil {
			if shutdownErr := shutdownCareDirect(); shutdownErr != nil {
				log.Printf("farm-svr Direct prefer-publication-failure shutdown: %v", shutdownErr)
			}
			return fmt.Errorf("farm-svr exact local signer publication startup: %w", err)
		}
	}
	if holderProcess != nil {
		if err := holderProcess.StartAgent(context.Background()); err != nil {
			if shutdownErr := shutdownCareDirect(); shutdownErr != nil {
				log.Printf("farm-svr Direct HolderAgent-start-failure shutdown: %v", shutdownErr)
			}
			return err
		}
		log.Printf("farm-svr HolderControl sealed server listening on %s", holderProcess.Listener.Addr())
		if assignment != nil {
			monitorDone := make(chan struct{})
			holderAssignmentMonitorDone = monitorDone
			go func() {
				defer close(monitorDone)
				<-assignment.Done()
				if err := assignment.TerminalError(); err != nil {
					holderProcess.FailClosed(err)
				}
			}()
			defer func() {
				_ = assignment.Close()
				<-monitorDone
			}()
		}
	}
	log.Printf("farm-svr listening on %s, version=%s", addr, farmgame.FullVersion())

	actorruntime.RegisterMetrics(metricsRegistry, "farm", registry.RuntimeStats)
	theftonline.RegisterAsyncDispatchMetrics(metricsRegistry, theftAsyncDispatch)
	activation.RegisterActivationMetrics(metricsRegistry, "farm", registry.ActivationStats)
	idle.RegisterMetrics(metricsRegistry, "farm", registry.IdleSweeperStats, []string{
		farmactor.BlockerTurnRunning, farmactor.BlockerUserQueueNonempty, farmactor.BlockerSystemQueueNonempty,
		farmactor.BlockerResolvingUnknown, farmactor.BlockerPersistenceInFlight,
		farmactor.BlockerDirtyUnhanded, farmactor.BlockerParticipantUnanchor,
	})
	ready := func() bool {
		if authorityRuntime != nil && !authorityRuntime.Ready() {
			return false
		}
		if assignment != nil && !assignment.Healthy() {
			return false
		}
		if holderProcess != nil && !holderProcess.Ready() {
			return false
		}
		if headerReadModeWatcher != nil && !headerReadModeWatcher.Healthy() {
			return false
		}
		return service.TheftIngressReady()
	}
	routeEvidence := func() map[string]any {
		if authorityRuntime == nil || assignment == nil {
			return nil
		}
		assignmentError := ""
		if err := assignment.Pump.LastError(); err != nil {
			assignmentError = err.Error()
		}
		agentStatus := holdercontrol.AgentStatus{State: holdercontrol.AgentStateClosed, Unhealthy: true}
		if holderProcess != nil && holderProcess.Agent != nil {
			agentStatus = holderProcess.Agent.Status()
		}
		agentError := ""
		if agentStatus.LastError != nil {
			agentError = agentStatus.LastError.Error()
		}
		return map[string]any{
			"domain": authorityRuntime.Domain, "holder_id": authorityRuntime.HolderID, "holder_boot_id": authorityRuntime.HolderBootID,
			"process_ready": authorityRuntime.ProcessReady(), "serving_shard_count": authorityRuntime.ServingShardCount(),
			"lease_scheduler_entries": authorityRuntime.LeaseSchedulerEntryCount(), "fatal_error_present": authorityRuntime.FatalErr() != nil,
			"assignment_healthy": assignment.Healthy(), "snapshot_revision": assignment.Resolver.Snapshot().GlobalRevision,
			"assignment_state": assignment.Pump.State(), "assignment_last_error": assignmentError,
			"assignment_reconnects": assignment.Pump.Reconnects.Load(), "assignment_resyncs": assignment.Pump.Resyncs.Load(),
			"holder_agent_state": agentStatus.State, "holder_agent_unhealthy": agentStatus.Unhealthy,
			"holder_agent_consecutive_failures": agentStatus.ConsecutiveFailures, "holder_agent_last_error": agentError,
			"membership": authorityRuntime.MembershipEvidenceStats(),
		}
	}
	health := newHealthServer(healthAddr, metricsRegistry, ready, routeEvidence)
	go func() {
		if err := health.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("farm-svr health: %v", err)
		}
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	careDirectShutdownAttempted = true
	if err := shutdownDirectBeforeFarmRuntime(careDirect, directShutdownTimeout(), func() {
		// Only after Direct has drained (or its bounded context has cancelled
		// in-flight acceleration) may holder admission, the local Participant
		// registry, route assignment, or Redis authority begin fencing.
		draining.Store(true)
		if drainGate != nil {
			drainFarmShards(drainGate, authorityShardCount)
		}
		registry.Stop()
	}); err != nil {
		log.Printf("farm-svr Direct shutdown timed out; durable DueIndex recovery retained: %v", err)
	}
	if holderProcess != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), gatewayapp.BoundedHolderTimeout(envDuration("HOLDER_CLOSE_TIMEOUT", 10*time.Second), 10*time.Second))
		if err := holderProcess.Agent.Close(closeCtx); err != nil {
			log.Printf("farm-svr HolderAgent close: %v", err)
		}
		closeCancel()
	}
	if assignment != nil {
		_ = assignment.Close()
	}
	if assignmentRunDone != nil {
		<-assignmentRunDone
	}
	if holderAssignmentMonitorDone != nil {
		<-holderAssignmentMonitorDone
	}
	serverStopCtx, serverStopCancel := context.WithTimeout(context.Background(), gatewayapp.BoundedHolderTimeout(envDuration("BUSINESS_GRPC_CLOSE_TIMEOUT", 5*time.Second), 5*time.Second))
	if err := gatewayapp.BoundedGRPCGracefulStop(serverStopCtx, server); err != nil {
		log.Printf("farm-svr business gRPC forced stop: %v", err)
	}
	serverStopCancel()
	if holderProcess != nil {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), gatewayapp.BoundedHolderTimeout(envDuration("HOLDER_SERVER_CLOSE_TIMEOUT", 5*time.Second), 5*time.Second))
		if err := holderProcess.GracefulStop(stopCtx); err != nil {
			log.Printf("farm-svr HolderControl server stop: %v", err)
		}
		stopCancel()
	}
	if authorityRuntime != nil {
		_ = authorityRuntime.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if theftAsyncDispatch != nil {
		_ = theftAsyncDispatch.Close(ctx)
	}
	_ = health.Shutdown(ctx)
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func buildFarmHolderProcess(client redis.UniversalClient, runtime *authoritycomposition.Runtime, assignment *authoritycomposition.AssignmentComposition, registry *farmactor.Registry, drainGate *actorruntime.ShardDrainGate, draining *atomic.Bool, commitProofKey holderkey.Publication) (*gatewayapp.HolderProcess, *routecontrol.HolderRegistry, error) {
	if runtime == nil || assignment == nil || registry == nil || drainGate == nil || draining == nil {
		return nil, nil, errors.New("farm-svr HolderControl requires authority runtime, registry, and drain gate")
	}
	namespace := runtime.Profile.Namespace
	if err := namespace.Validate(); err != nil {
		return nil, nil, fmt.Errorf("farm-svr HolderRegistry namespace: %w", err)
	}
	leaseTTL := envDuration("HOLDER_AGENT_LEASE_TTL", 30*time.Second)
	holderStoreOptions := redisroutecontrol.DefaultHolderStoreOptions(namespace)
	holderStoreOptions.OperationTimeout = gatewayapp.BoundedHolderTimeout(envDuration("HOLDER_REGISTRY_OPERATION_TIMEOUT", 2*time.Second), 2*time.Second)
	holderStoreOptions.LeaseTTL = leaseTTL
	holderStoreOptions.MinLeaseTTL = time.Millisecond
	holderStore, err := redisroutecontrol.NewHolderStore(client, holderStoreOptions)
	if err != nil {
		return nil, nil, fmt.Errorf("farm-svr HolderRegistry store: %w", err)
	}
	registryOptions := routecontrol.DefaultHolderRegistryOptions()
	registryOptions.LeaseTTL = leaseTTL
	registryOptions.MinLeaseTTL = time.Millisecond
	allowInitialServing := os.Getenv("HOLDER_ALLOW_INITIAL_SERVING") == "true"
	if allowInitialServing {
		registryOptions.InitialServingVerifier, err = routecontrol.NewStaticInitialServingVerifier(routecontrol.StaticInitialServingVerifierOptions{
			Domain: runtime.Domain, HolderID: runtime.HolderID, HolderBootID: runtime.HolderBootID,
			LogicEndpoint: os.Getenv("FARM_LOGIC_ENDPOINT"), Snapshot: assignment.Provider.GetSnapshot,
			ProcessReady: runtime.ProcessReady, AssignmentHealthy: assignment.Healthy,
			ShardOpen: func(shard uint32) bool {
				status, ok := drainGate.Status(shard)
				return ok && status.Phase == actorruntime.ShardDrainPhaseOpen
			},
			CanServeShard: runtime.CanServeShard, Now: time.Now,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("farm-svr static Holder serving admission: %w", err)
		}
	}
	holderRegistry, err := routecontrol.NewHolderRegistry(holderStore, registryOptions)
	if err != nil {
		return nil, nil, fmt.Errorf("farm-svr HolderRegistry: %w", err)
	}

	shardFor := func(actorID uint64) (uint32, error) {
		return runtime.Calculator.ShardFor(runtime.Domain, actorID)
	}
	holderRuntime, err := farmactor.NewHolderShardRuntime(registry, shardFor)
	if err != nil {
		_ = holderRegistry.Close()
		return nil, nil, fmt.Errorf("farm-svr HolderShardRuntime: %w", err)
	}
	var preparer holdercontrol.ShardGenerationPreparer
	if inner := runtime.MembershipInstaller(); inner != nil {
		prepared, ok := inner.(*holdercontrol.PrepareOnlyMembershipInstaller)
		if !ok {
			_ = holderRegistry.Close()
			return nil, nil, errors.New(
				"farm-svr Holder prepare seam is not shared with membership reconciliation",
			)
		}
		preparer = &holdercontrol.DomainGenerationPreparer{Domain: runtime.Domain, Installer: prepared}
	}
	controllerIdentity := env("HOLDER_CONTROL_CONTROLLER_IDENTITY", "route-controller")
	rules := holdercontrol.HolderControlMethodACL([]string{controllerIdentity})
	if err := holdercontrol.ValidateHolderControlMethodACL(rules); err != nil {
		_ = holderRegistry.Close()
		return nil, nil, fmt.Errorf("farm-svr Holder ACL: %w", err)
	}
	authorizer, err := transport.NewMethodAuthorizer(rules)
	if err != nil {
		_ = holderRegistry.Close()
		return nil, nil, fmt.Errorf("farm-svr Holder method authorizer: %w", err)
	}
	statusSource := holdercontrol.ProcessStatusSourceFunc(func(ctx context.Context) (holdercontrol.ProcessStatus, error) {
		if ctx == nil {
			return holdercontrol.ProcessStatus{}, errors.New("farm-svr Holder status context is required")
		}
		if err := ctx.Err(); err != nil {
			return holdercontrol.ProcessStatus{}, err
		}
		serving := uint32(0)
		for shard := uint32(0); shard < drainGate.ShardCount(); shard++ {
			state, ok := drainGate.Status(shard)
			if ok && state.Phase == actorruntime.ShardDrainPhaseOpen && runtime.CanServeShard(runtime.Domain, shard) {
				serving++
			}
		}
		runtimeHealthy := runtime.ProcessReady()
		assignmentHealthy := assignment.Healthy()
		shutdownDraining := draining.Load()
		appliedRevision, desiredShards := assignment.AppliedSnapshotRevisionForHolder(runtime.Domain, runtime.HolderID, runtime.HolderBootID)
		return gatewayapp.HolderRuntimeProcessStatus(holdercontrol.ProcessStatus{
			ServingShardCount: serving,
			SnapshotRevision:  gatewayapp.HolderSnapshotRevisionEvidence(appliedRevision, serving, desiredShards),
			CapacityWeight:    uint32(envInt("HOLDER_CAPACITY_WEIGHT", 1)),
		}, runtimeHealthy, assignmentHealthy, shutdownDraining), nil
	})
	localService, err := holdercontrol.NewLocalService(holdercontrol.LocalServiceOptions{
		Authorizer:                  authorizer,
		LocalHolderID:               runtime.HolderID,
		LocalBootID:                 runtime.HolderBootID,
		DeploymentIdentity:          runtime.DeploymentIdentity,
		ProcessStatusSource:         statusSource,
		WorkloadIdentityTokenSource: holdercontrol.FileWorkloadIdentityTokenSource{Path: os.Getenv("HOLDER_WORKLOAD_IDENTITY_TOKEN_FILE")},
		CommitProofKey:              commitProofKey,
		Now:                         time.Now,
		OperationTimeout:            gatewayapp.BoundedHolderTimeout(envDuration("HOLDER_CONTROL_OPERATION_TIMEOUT", 5*time.Second), 5*time.Second),
		Farm: holdercontrol.DomainRuntimeOptions{
			Runtime: holderRuntime, Preparer: preparer, ShardCount: drainGate.ShardCount(),
		},
	})
	if err != nil {
		_ = holderRegistry.Close()
		return nil, nil, fmt.Errorf("farm-svr Holder local service: %w", err)
	}
	agentOptions := holdercontrol.DefaultHolderAgentOptions()
	agentIdentity := farmHolderAgentIdentityOptions(runtime)
	agentOptions.Domain = runtime.Domain
	agentOptions.HolderID = agentIdentity.HolderID
	agentOptions.DeploymentIdentity = agentIdentity.DeploymentIdentity
	agentOptions.LogicEndpoint = os.Getenv("FARM_LOGIC_ENDPOINT")
	agentOptions.HolderControlEndpoint = os.Getenv("FARM_HOLDER_ENDPOINT")
	agentOptions.TLSDNSIdentity = os.Getenv("FARM_HOLDER_TLS_DNS_IDENTITY")
	agentOptions.CapacityWeight = uint32(envInt("HOLDER_CAPACITY_WEIGHT", 1))
	agentOptions.AllowInitialServing = allowInitialServing
	agentOptions.HeartbeatInterval = gatewayapp.BoundedHolderTimeout(envDuration("HOLDER_HEARTBEAT_INTERVAL", agentOptions.HeartbeatInterval), agentOptions.HeartbeatInterval)
	agentOptions.HeartbeatTimeout = gatewayapp.BoundedHolderTimeout(envDuration("HOLDER_HEARTBEAT_TIMEOUT", agentOptions.HeartbeatTimeout), agentOptions.HeartbeatTimeout)
	agentOptions.CloseTimeout = gatewayapp.BoundedHolderTimeout(envDuration("HOLDER_CLOSE_TIMEOUT", agentOptions.CloseTimeout), agentOptions.CloseTimeout)
	if n := envInt("HOLDER_HEARTBEAT_FAILURE_THRESHOLD", 0); n > 0 {
		agentOptions.ConsecutiveFailureThreshold = n
	}
	agentOptions.LeaseTTL = leaseTTL
	agentOptions.BootID = agentIdentity.BootID
	holderProcess, err := gatewayapp.NewHolderProcess(gatewayapp.HolderProcessOptions{
		ListenAddr:          env("FARM_HOLDER_CONTROL_LISTEN", ":19101"),
		ServerCertFile:      os.Getenv("FARM_HOLDER_TLS_CERT_FILE"),
		ServerKeyFile:       os.Getenv("FARM_HOLDER_TLS_KEY_FILE"),
		ClientCAFile:        os.Getenv("FARM_HOLDER_CLIENT_CA_FILE"),
		ControllerIdentity:  controllerIdentity,
		LocalService:        localService,
		Registry:            holderRegistry,
		ProcessStatus:       statusSource,
		StartupReadyTimeout: gatewayapp.BoundedHolderTimeout(envDuration("HOLDER_STARTUP_READY_TIMEOUT", 20*time.Second), 20*time.Second),
		AgentOptions:        agentOptions,
		MaxMessageBytes:     gatewayapp.BoundedHolderMessageBytes(envInt("HOLDER_CONTROL_MAX_MESSAGE_BYTES", 4<<20), 4<<20),
	})
	if err != nil {
		_ = holderRegistry.Close()
		return nil, nil, err
	}
	bindSnapshotAppliedHeartbeat(assignment, holderProcess)
	return holderProcess, holderRegistry, nil
}

func bindSnapshotAppliedHeartbeat(assignment *authoritycomposition.AssignmentComposition, holderProcess *gatewayapp.HolderProcess) {
	if assignment == nil || assignment.Applier == nil || holderProcess == nil || holderProcess.Agent == nil {
		return
	}
	agent := holderProcess.Agent
	assignment.Applier.OnApplied = func(uint64) {
		agent.NotifySnapshotApplied()
	}
}

func farmHolderAgentIdentityOptions(runtime *authoritycomposition.Runtime) holdercontrol.HolderAgentOptions {
	if runtime == nil {
		return holdercontrol.HolderAgentOptions{}
	}
	bootID := runtime.HolderBootID
	return holdercontrol.HolderAgentOptions{
		HolderID: runtime.HolderID, DeploymentIdentity: runtime.DeploymentIdentity,
		BootID: func() (string, error) { return bootID, nil },
	}
}

type farmDirectShutdown interface {
	Shutdown(context.Context) error
}

func directShutdownTimeout() time.Duration {
	return gatewayapp.BoundedHolderTimeout(envDuration("CARE_DIRECT_SHUTDOWN_TIMEOUT", 5*time.Second), 5*time.Second)
}

// shutdownDirectBeforeFarmRuntime is the production ordering boundary. The
// callback owns every runtime-fencing action and is invoked only after Direct
// has stopped admission and either drained or consumed its bounded budget.
func shutdownDirectBeforeFarmRuntime(direct farmDirectShutdown, timeout time.Duration, fenceRuntime func()) error {
	var err error
	if direct != nil {
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err = direct.Shutdown(ctx)
		cancel()
	}
	if fenceRuntime != nil {
		fenceRuntime()
	}
	return err
}

func drainFarmShards(gate *actorruntime.ShardDrainGate, shardCount uint32) {
	if gate == nil {
		return
	}
	for shard := uint32(0); shard < shardCount; shard++ {
		if err := gate.BeginDrain(shard); err != nil {
			log.Printf("farm-svr begin shard %d drain: %v", shard, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), gatewayapp.BoundedHolderTimeout(envDuration("HOLDER_DRAIN_TIMEOUT", 10*time.Second), 10*time.Second))
	defer cancel()
	for shard := uint32(0); shard < shardCount; shard++ {
		if err := gate.Wait(ctx, shard); err != nil {
			log.Printf("farm-svr wait shard %d drain barrier: %v", shard, err)
		}
	}
}

const explicitLocalTheftPRFSecret = "farmgame-explicit-test-demo-theft-prf-v1"

// theftPRFFromConfig is the fail-closed deployment boundary for FarmActor's
// Theft randomness. Production is the default mode and always requires a
// deployment-provided secret of at least 32 bytes. The fixed local key is
// reachable only after explicitly selecting test or demo mode.
func theftPRFFromConfig(getenv func(string) string) (farmactor.TheftPRF, error) {
	if getenv == nil {
		return nil, errors.New("THEFT PRF configuration source is required")
	}
	mode := strings.ToLower(strings.TrimSpace(getenv("THEFT_PRF_MODE")))
	if mode == "" {
		mode = "production"
	}
	secret := []byte(getenv("THEFT_PRF_HMAC_SECRET"))
	switch mode {
	case "production":
		if len(secret) < 32 {
			return nil, errors.New("THEFT_PRF_HMAC_SECRET must contain at least 32 bytes in production mode")
		}
	case "test", "demo":
		if len(secret) == 0 {
			secret = []byte(explicitLocalTheftPRFSecret)
		}
		if len(secret) < 16 {
			return nil, errors.New("THEFT_PRF_HMAC_SECRET must contain at least 16 bytes in test/demo mode")
		}
	default:
		return nil, fmt.Errorf("THEFT_PRF_MODE must be production, test, or demo, got %q", mode)
	}
	return farmactor.HMACTheftPRF(secret), nil
}

func registerFarmParticipantServices(registrar grpc.ServiceRegistrar, participant *farmingress.Server) {
	if registrar == nil || participant == nil {
		return
	}
	contractspb.RegisterCoordinatorParticipantServiceServer(registrar, participant)
	contractspb.RegisterWorkerRecoveryIngressServer(registrar, participant)
}

func configureActorRoute(registry *farmactor.Registry, routeMetrics *actorrouting.Metrics) *actorrouting.StaticResolver {
	path := os.Getenv("ACTOR_ROUTE_SNAPSHOT")
	if path == "" {
		return nil
	}
	holder := os.Getenv("ACTOR_HOLDER_POD")
	if holder == "" {
		log.Fatal("ACTOR_HOLDER_POD is required with ACTOR_ROUTE_SNAPSHOT")
	}
	resolver, err := actorrouting.LoadStaticResolver(path)
	if err != nil {
		log.Fatalf("load actor route snapshot: %v", err)
	}
	resolver.SetMetrics(routeMetrics)
	registry.SetWriterEpochResolver(resolver.HolderEpochResolver(actorrouting.KindFarm, holder))
	log.Printf("farm holder fencing enabled pod=%s snapshot=%s", holder, path)
	return resolver
}

func openGatewayDownlinks() (logicdownlink.GatewayClient, []*grpc.ClientConn, error) {
	if rawRoutes := strings.TrimSpace(os.Getenv("GATEWAY_DOWNLINK_ROUTES")); rawRoutes != "" {
		clients := make(map[string]logicdownlink.GatewayClient)
		var connections []*grpc.ClientConn
		closeAll := func() {
			for _, conn := range connections {
				_ = conn.Close()
			}
		}
		for _, rawRoute := range strings.Split(rawRoutes, ",") {
			parts := strings.SplitN(strings.TrimSpace(rawRoute), "=", 2)
			if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
				closeAll()
				return nil, nil, fmt.Errorf("invalid GATEWAY_DOWNLINK_ROUTES entry %q; want gateway-id=host:port", rawRoute)
			}
			gatewayID, address := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
			if _, exists := clients[gatewayID]; exists {
				closeAll()
				return nil, nil, fmt.Errorf("duplicate gateway downlink route %q", gatewayID)
			}
			connection, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				closeAll()
				return nil, nil, fmt.Errorf("dial farm-gate %s downlink %s: %w", gatewayID, address, err)
			}
			connections = append(connections, connection)
			clients[gatewayID] = logicrpcpb.NewGatewayDownlinkServiceClient(connection)
		}
		routed, err := logicdownlink.NewRoutedGatewayClient(clients, nil)
		if err != nil {
			closeAll()
			return nil, nil, err
		}
		log.Printf("using %d gateway-aware downlink routes", len(clients))
		return routed, connections, nil
	}
	if gatewayAddr := strings.TrimSpace(os.Getenv("GATEWAY_DOWNLINK_ADDR")); gatewayAddr != "" {
		connection, err := grpc.Dial(gatewayAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, fmt.Errorf("dial farm-gate downlink %s: %w", gatewayAddr, err)
		}
		log.Printf("using single farm-gate downlink at %s", gatewayAddr)
		return logicrpcpb.NewGatewayDownlinkServiceClient(connection), []*grpc.ClientConn{connection}, nil
	}
	return nil, nil, nil
}

func openStores(redisClients *redisdomains.Clients, keyspaceTuple *rediskey.KeyspaceTuple) (*friend.RedisFriendshipRepositories, workflow.Store, workflow.WorkRepository, func()) {
	if redisClients == nil || redisClients.Authority == nil || redisClients.Projection == nil {
		log.Fatal("redis domain clients are required; farm-svr has no in-memory runtime fallback")
	}
	namespace, err := farmRedisNamespace(keyspaceTuple, os.Getenv)
	if err != nil {
		log.Fatalf("build friendship Redis namespace: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Authority owns the canonical Friendship Pair/Quota/Share state used by
	// THEFT admission and CARE authority. The FriendIndex and Navigation
	// sidecars are rebuildable projections and therefore live on Projection.
	friendshipRepositories, err := friend.NewRedisFriendshipRepositoriesForDomains(
		redisClients.Authority,
		redisClients.Projection,
		friend.RedisFriendshipOptions{Profile: friend.DefaultFriendRedisProfile(namespace)},
	)
	if err != nil {
		log.Fatalf("build Redis Friendship repositories: %v", err)
	}
	if err := friendshipRepositories.EnsureScriptsLoaded(ctx); err != nil {
		log.Fatalf("load Redis Friendship scripts: %v", err)
	}
	workflowStore, err := workflow.NewRedisStore(redisClients.Authority, env("WORKFLOW_STORE_PREFIX", "farm:v121:m1:workflow"))
	if err != nil {
		log.Fatal(err)
	}
	workRepository, err := workflow.NewRedisWorkRepository(redisClients.Authority, env("WORKFLOW_STORE_PREFIX", "farm:v121:m1:workflow"))
	if err != nil {
		log.Fatal(err)
	}
	log.Println("farm-svr using persistent canonical Friendship Pair/Quota/Share repositories")
	return friendshipRepositories, workflowStore, workRepository, func() {}
}

func farmRedisNamespace(keyspaceTuple *rediskey.KeyspaceTuple, getenv func(string) string) (rediskey.Namespace, error) {
	if keyspaceTuple != nil {
		namespace := keyspaceTuple.Namespace()
		return namespace, namespace.Validate()
	}
	if getenv == nil {
		return rediskey.Namespace{}, errors.New("farm-svr Redis namespace environment reader is required")
	}
	environment := getenv(rediskey.RedisNamespaceEnvironmentEnv)
	if environment == "" {
		environment = "dev"
	}
	zone := getenv(rediskey.RedisNamespaceZoneEnv)
	if zone == "" {
		zone = "z1"
	}
	return rediskey.NewNamespace(environment, zone)
}

func newFarmResultPublisher(client redis.UniversalClient, keyspaceTuple *rediskey.KeyspaceTuple, getenv func(string) string) (*theftonline.RedisResultPublisher, error) {
	namespace, err := farmRedisNamespace(keyspaceTuple, getenv)
	if err != nil {
		return nil, err
	}
	return theftonline.NewRedisResultPublisher(client, namespace)
}

func requiredRemoteKeyspaceTuple(membership authoritycomposition.MembershipMode, assignment authoritycomposition.AssignmentMode) (*rediskey.KeyspaceTuple, error) {
	if !authoritycomposition.HolderCompositionEnabled(membership, assignment) {
		return nil, nil
	}
	tuple, err := rediskey.ParseKeyspaceTupleEnvironment(os.Getenv)
	if err != nil {
		return nil, fmt.Errorf("farm-svr remote Redis keyspace tuple: %w", err)
	}
	fingerprint, err := tuple.Fingerprint()
	if err != nil {
		return nil, fmt.Errorf("farm-svr remote Redis keyspace fingerprint: %w", err)
	}
	log.Printf("farm-svr Redis keyspace fingerprint=%s", fingerprint)
	return &tuple, nil
}

// configureTheftIngress is kept explicit so production composition cannot
// accidentally satisfy the new ingress with memory/mock dependencies or fall
// back to PrepareTheft. The concrete Dispatch also owns the durable target
// sidecar recorder used to reconstruct the same Work after response loss.
func configureTheftIngress(service *farmingress.Service, repositories *friend.RedisFriendshipRepositories, dispatch farmingress.TheftDispatchPort) error {
	if service == nil {
		return errors.New("THEFT ingress service is nil")
	}
	if repositories == nil || repositories.Pair == nil {
		return errors.New("real Redis Friendship Pair repository is required")
	}
	if dispatch == nil {
		return errors.New("concrete Theft Workflow dispatch port is not composed")
	}
	recorder, ok := dispatch.(farmingress.TheftTargetBindingRecorder)
	if !ok || recorder == nil {
		return errors.New("concrete Theft dispatch must persist target bindings")
	}
	service.SetTheftPairRepository(repositories.Pair)
	service.SetTheftDispatchPort(dispatch)
	service.SetTheftTargetBindingRecorder(recorder)
	return nil
}

func newHealthServer(addr string, metricsRegistry *prometheus.Registry, ready func() bool, evidence ...func() map[string]any) *http.Server {
	if ready == nil {
		ready = func() bool { return true }
	}
	routeEvidence := func() map[string]any { return nil }
	if len(evidence) > 0 && evidence[0] != nil {
		routeEvidence = evidence[0]
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"status":"ok"}`)) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if !ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "not_ready", "route_runtime": routeEvidence()})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ready", "route_runtime": routeEvidence()})
	})
	mux.Handle("/metrics", promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{}))
	if os.Getenv("HOLDER_PPROF_ENABLED") == "1" {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		log.Printf("WARNING: pprof is enabled on the farm-svr health listener")
	}
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, parseErr := strconv.Atoi(value); parseErr == nil {
			return parsed
		}
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	switch value {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}
