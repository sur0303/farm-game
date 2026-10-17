// Command player-svr runs the player_id keyed PlayerActor authority.
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
	friendshiprpcpb "farmgame/api/gen/friendshiprpc"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	"farmgame/internal/authoritycomposition"
	"farmgame/internal/contracts"
	"farmgame/internal/friend"
	"farmgame/internal/gatewayapp"
	"farmgame/internal/grpcmetrics"
	"farmgame/internal/holdercontrol"
	"farmgame/internal/logic/actor/activation"
	routeauthority "farmgame/internal/logic/actor/authority"
	"farmgame/internal/logic/actor/datamode"
	"farmgame/internal/logic/actor/dirtywriteback"
	"farmgame/internal/logic/actor/idle"
	actorruntime "farmgame/internal/logic/actor/runtime"
	"farmgame/internal/observability/actorlayout"
	"farmgame/internal/observability/redisworkload"
	"farmgame/internal/plantonline"
	playeractor "farmgame/internal/player/actor"
	playercompendium "farmgame/internal/player/compendium"
	playeringress "farmgame/internal/player/ingress"
	playermail "farmgame/internal/player/mail"
	playerparticipant "farmgame/internal/player/participant"
	playerpet "farmgame/internal/player/pet"
	playertask "farmgame/internal/player/task"
	"farmgame/internal/redisdomains"
	routecontrol "farmgame/internal/routecontrol"
	"farmgame/internal/routecontrol/transport"
	"farmgame/internal/socialprofile"
	"farmgame/internal/sourceordermetrics"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
	redisauthority "farmgame/internal/storage/redis/authority"
	redisplayermail "farmgame/internal/storage/redis/playermail"
	"farmgame/internal/storage/redis/rediskey"
	redisroutecontrol "farmgame/internal/storage/redis/routecontrol"
	"farmgame/internal/worker/guardrecovery"
	"farmgame/internal/worker/sourceorder"
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

func run() error {
	addr := env("PLAYER_LISTEN", ":9100")
	healthAddr := env("PLAYER_HEALTH_LISTEN", ":9101")
	mode, modeErr := authoritycomposition.ModeFromEnvironment()
	if modeErr != nil {
		return modeErr
	}
	dataMode, dataModeErr := datamode.FromLookup(os.LookupEnv)
	if dataModeErr != nil {
		return dataModeErr
	}
	if dataMode.DirtyWritebackEnabled() && mode != authoritycomposition.ModeAuthority {
		return errors.New("player-svr: dirty_best_effort requires ACTOR_AUTHORITY_MODE=authority")
	}
	if dataMode.DirtyWritebackEnabled() {
		log.Printf("WARNING: player-svr data_mode=%s accepts bounded crash loss and disables durable cross-actor outcomes", dataMode)
	}
	modelFieldV2Conversion, conversionErr := fieldv2.ProductionConversionPolicy(os.LookupEnv)
	if conversionErr != nil {
		return fmt.Errorf("validate Actor Model Field V2 conversion policy: %w", conversionErr)
	}
	membershipMode, membershipModeErr := authoritycomposition.MembershipModeFromEnvironment()
	if membershipModeErr != nil {
		return membershipModeErr
	}
	if _, err := socialprofile.ResolveEnvironment(os.LookupEnv); err != nil {
		return fmt.Errorf("validate social product profile: %w", err)
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
	authorityClient, projectionClient, workRepository, friendshipRepositories, closeStore := openStore(keyspaceTuple)
	defer closeStore()
	metricsRegistry := prometheus.NewRegistry()
	metricsRegistry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	sourceordermetrics.Register(metricsRegistry)
	if err := contracts.RegisterParticipantRouteLeaseProofMetrics(metricsRegistry); err != nil {
		return err
	}
	commitProofMode, err := sourceorder.CommitProofModeFromEnvironment(os.Getenv)
	if err != nil {
		return fmt.Errorf("player-svr commit proof mode: %w", err)
	}
	if dataMode.DirtyWritebackEnabled() && commitProofMode != sourceorder.CommitProofModeOff {
		log.Printf("WARNING: player-svr disables durable SourceOrder commit proof in data_mode=%s", dataMode)
		commitProofMode = sourceorder.CommitProofModeOff
	}
	if err := commitProofMode.ValidatePreferRollout(); err != nil {
		return fmt.Errorf("player-svr commit proof rollout: %w", err)
	}
	commitProofMetrics := &playerparticipant.CommitProofMetrics{}
	if err := playerparticipant.RegisterCommitProofMetrics(metricsRegistry, commitProofMetrics, commitProofMode); err != nil {
		return err
	}
	if _, err := redisworkload.AttachTargetsFromEnvironment(redisworkload.DependencyTargets{
		Authority: authorityClient, Projection: projectionClient,
	}, metricsRegistry, os.Getenv("REDIS_WORKLOAD_METRICS")); err != nil {
		return err
	}
	// player-svr does not open the runtime domain; that series is still
	// exported as 0 so the capacity harness always finds the metric.
	if _, err := redisworkload.AttachDependencyHealth(metricsRegistry, redisworkload.DependencyTargets{
		Authority:  authorityClient,
		Projection: projectionClient,
	}); err != nil {
		return err
	}
	layoutMetrics := actorlayout.NewMetrics(metricsRegistry)
	routeMetrics := actorrouting.RegisterRouteMetrics(metricsRegistry)
	var registry *playeractor.Registry
	var routeResolver *actorrouting.StaticResolver
	var authorityRuntime *authoritycomposition.Runtime
	var assignment *authoritycomposition.AssignmentComposition
	var assignmentClient interface{ Close() error }
	var drainGate *actorruntime.ShardDrainGate
	var holderProcess *gatewayapp.HolderProcess
	var holderAssignmentMonitorDone <-chan struct{}
	var assignmentRunDone <-chan struct{}
	var draining atomic.Bool
	var authorityShardCount uint32
	var routeFence participantproof.RouteFenceVerifier
	var careVerifier playerparticipant.CareDependencyVerifier
	var harvestSourceVerifier playerparticipant.HarvestSourceVerifier
	var commitProofValidator *sourceorder.CommitProofValidator
	var commitProofSourceRealm string
	if mode == authoritycomposition.ModeAuthority {
		var authorityOptions authoritycomposition.Options
		if keyspaceTuple != nil {
			authorityOptions, err = authoritycomposition.OptionsFromEnvironmentWithProfile(authorityClient, actorrouting.DomainPlayer, keyspaceTuple.Profile())
		} else {
			authorityOptions, err = authoritycomposition.OptionsFromEnvironment(authorityClient, actorrouting.DomainPlayer)
		}
		if err != nil {
			return err
		}
		authorityOptions.OnFatal = func(err error) { log.Printf("player-svr authority runtime fatal: %v", err) }
		authorityOptions.EnableSoftSave = dataMode.DirtyWritebackEnabled()
		authorityClient.AddHook(layoutMetrics)
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
				return errors.New("player-svr remote assignment client does not implement snapshot source")
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
		routeResolver = authorityRuntime.Resolver
		// RF-OBS-1: test-only local read-only diagnostics.  Enabled only when
		// Q2R2_LIVE=1 && Q2R2_DIAGNOSTICS=1, loopback-only, derived directly
		// from Runtime serving/canServe, and shut down with run().
		diagAddr, diagShutdown, diagErr := startQ2R2Diagnostics(authorityRuntime, authorityOptions.Domain, "127.0.0.1:"+env("Q2R2_DIAGNOSTICS_PORT", "0"))
		if diagErr != nil {
			return diagErr
		}
		if diagShutdown != nil {
			defer diagShutdown()
			log.Printf("player-svr q2r2 diagnostics on %s", diagAddr)
		}
		routeFence, err = authoritycomposition.NewParticipantRouteFence(authorityRuntime)
		if err != nil {
			return err
		}
		commitProofSourceRealm = routeResolver.Realm()
		if commitProofMode != sourceorder.CommitProofModeOff {
			commitProofValidator, err = sourceorder.NewRouteCommitProofValidator(routeResolver, commitProofSourceRealm,
				envDuration(sourceorder.CommitProofTTLEnv, sourceorder.DefaultCommitProofTTL), time.Now)
			if err != nil {
				return fmt.Errorf("player-svr route commit proof keyring: %w", err)
			}
		}
		careVerifier, err = playerparticipant.NewRedisCareDependencyVerifier(authorityRuntime.Store,
			func(farmID uint64) (rediskey.AuthorityBucketRef, error) {
				return authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
					actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
			})
		if err != nil {
			return err
		}
		harvestSourceVerifier, err = playerparticipant.NewRedisHarvestSourceVerifier(authorityRuntime.Store,
			func(farmID uint64) (rediskey.AuthorityBucketRef, error) {
				return authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
					actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
			})
		if err != nil {
			return err
		}
		shardCount, err := authorityRuntime.Calculator.ShardCount(authorityOptions.Domain)
		if err != nil {
			return fmt.Errorf("player-svr shard count: %w", err)
		}
		authorityShardCount = shardCount
		drainGate, err = actorruntime.NewShardDrainGate(actorruntime.ShardDrainGateOptions{
			ShardCount: shardCount,
			ShardFor: func(actorID uint64) (uint32, error) {
				return authorityRuntime.Calculator.ShardFor(authorityOptions.Domain, actorID)
			},
		})
		if err != nil {
			return fmt.Errorf("player-svr shard drain gate: %w", err)
		}
		actOpts := activation.DefaultOptions()
		actOpts.MaxConcurrentLoads = envInt("MAX_CONCURRENT_ACTOR_LOADS", 2000)
		actOpts.MaxPendingLoads = envInt("MAX_PENDING_ACTOR_LOADS", 4096)
		if rawRate := os.Getenv("ACTIVATION_RATE_PER_SEC"); rawRate != "" {
			if parsed, parseErr := strconv.ParseFloat(rawRate, 64); parseErr == nil && parsed >= 0 {
				actOpts.ActivationRatePerSec = parsed
			}
		}
		registry = playeractor.NewAuthorityRegistry(authorityRuntime.Provider, playeractor.RegistryOptions{
			MailboxSize:            envInt("PLAYER_MAILBOX_MAX_SIZE", 64),
			MailboxInitialSize:     envInt("PLAYER_MAILBOX_INITIAL_SIZE", 16),
			AdmissionGate:          drainGate,
			Activation:             actOpts,
			ModelFieldV2Conversion: modelFieldV2Conversion,
			NewModelLayout:         authoritystore.ModelLayoutV2,
			DirtyWriteback: dirtywriteback.Options{
				FlushInterval: envDuration("ACTOR_DIRTY_FLUSH_INTERVAL", 3*time.Second),
				Jitter:        envDuration("ACTOR_DIRTY_FLUSH_JITTER", 600*time.Millisecond),
				MaxDirtyAge:   envDuration("ACTOR_DIRTY_MAX_AGE", 10*time.Second),
				RetryDelay:    envDuration("ACTOR_DIRTY_RETRY_DELAY", time.Second),
				FlushTimeout:  envDuration("ACTOR_DIRTY_FLUSH_TIMEOUT", 2*time.Second),
				Workers:       envInt("ACTOR_DIRTY_FLUSH_WORKERS", 4),
				QueueSize:     envInt("ACTOR_DIRTY_QUEUE_SIZE", 1024),
			},
			DirtyDrainTimeout: envDuration("ACTOR_DIRTY_DRAIN_TIMEOUT", 8*time.Second),
			Idle: playeractor.IdleOptions{
				IdleTimeout:       envDuration("PLAYER_MAILBOX_IDLE_TIMEOUT", 5*time.Minute),
				BucketWidth:       envDuration("PLAYER_MAILBOX_IDLE_BUCKET_WIDTH", 30*time.Second),
				SweepInterval:     envDuration("PLAYER_MAILBOX_IDLE_SWEEP_INTERVAL", 30*time.Second),
				MaxPerSweep:       envInt("PLAYER_MAILBOX_IDLE_MAX_PER_SWEEP", 256),
				BlockedRetryDelay: envDuration("PLAYER_MAILBOX_IDLE_BLOCKED_RETRY_DELAY", 5*time.Second),
			},
		})
		log.Printf("player-svr authority mode enabled holder=%s boot=%s snapshot=%s membership=%s domain=%s snapshot_revision=%d", authorityOptions.HolderID, authorityOptions.HolderBootID, authorityOptions.RouteSnapshotPath, membershipMode, authorityOptions.Domain, authorityRuntime.AssignmentProvider.Snapshot().GlobalRevision)
		if rb := authorityRuntime.RecoveryBudget(); rb != (authoritycomposition.RecoveryBudgetInfo{}) {
			log.Printf("player-svr lease recovery budget margin_ms=%d base_ms=%d max_ms=%d jitter_ms=%d attempt_timeout_ms=%d slack_ms=%d completion_budget_ms=%d satisfied=%v",
				rb.SafetyMargin.Milliseconds(), rb.RecoveryBaseDelay.Milliseconds(), rb.RecoveryMaxDelay.Milliseconds(), rb.RecoveryJitterMax.Milliseconds(), rb.RecoveryAttemptTimeout.Milliseconds(), rb.RecoverySchedulerSlack.Milliseconds(), rb.CompletionBudget.Milliseconds(), rb.Satisfied)
		}
	} else {
		var legacyStore blobstore.Store
		var err error
		legacyStore, err = blobstore.NewRedisStore(authorityClient, env("PLAYER_STORE_PREFIX", "farm:v118:dev:player"))
		if err != nil {
			return err
		}
		legacyStore = blobstore.NewMeasuredStore(legacyStore, metricsRegistry, "player")
		registry = playeractor.NewRegistryWithOptions(legacyStore, playeractor.RegistryOptions{
			MailboxSize:        envInt("PLAYER_MAILBOX_MAX_SIZE", 64),
			MailboxInitialSize: envInt("PLAYER_MAILBOX_INITIAL_SIZE", 16),
			Idle: playeractor.IdleOptions{
				IdleTimeout:       envDuration("PLAYER_MAILBOX_IDLE_TIMEOUT", 5*time.Minute),
				BucketWidth:       envDuration("PLAYER_MAILBOX_IDLE_BUCKET_WIDTH", 30*time.Second),
				SweepInterval:     envDuration("PLAYER_MAILBOX_IDLE_SWEEP_INTERVAL", 30*time.Second),
				MaxPerSweep:       envInt("PLAYER_MAILBOX_IDLE_MAX_PER_SWEEP", 256),
				BlockedRetryDelay: envDuration("PLAYER_MAILBOX_IDLE_BLOCKED_RETRY_DELAY", 5*time.Second),
			},
		})
		routeResolver = configureActorRoute(registry, routeMetrics)
		log.Printf("player-svr actor authority mode=%s", mode)
	}
	defer registry.Stop()
	if authorityRuntime != nil && authoritycomposition.HolderCompositionEnabled(membershipMode, assignmentMode) {
		var holderRegistry *routecontrol.HolderRegistry
		var err error
		holderProcess, holderRegistry, err = buildPlayerHolderProcess(authorityClient, authorityRuntime, assignment, registry, drainGate, &draining)
		if err != nil {
			return err
		}
		defer holderRegistry.Close()
	}
	registry.StartIdleSweeper(context.Background())
	service := playeringress.NewService(registry)
	if authorityRuntime != nil {
		privateStateLease, admissionErr := routeauthority.NewLeaseGuardAdmission(authorityRuntime.Guards)
		if admissionErr != nil {
			return fmt.Errorf("compose GetPrivateState route admission: %w", admissionErr)
		}
		if admissionErr := service.SetPrivateStateRouteAdmission(routeResolver, privateStateLease, routeResolver.Realm()); admissionErr != nil {
			return fmt.Errorf("compose GetPrivateState route admission: %w", admissionErr)
		}
	}
	namespace, err := playerRedisNamespace(keyspaceTuple, os.Getenv)
	if err != nil {
		return fmt.Errorf("derive PlayerMail Redis namespace: %w", err)
	}
	mailStore, err := redisplayermail.NewStore(authorityClient, redisplayermail.Profile{
		Namespace:   namespace,
		BucketCount: uint32(envInt("PLAYER_MAIL_BUCKET_COUNT", int(redisplayermail.DefaultBucketCount))),
	}, playermail.LimitsV1())
	if err != nil {
		return fmt.Errorf("compose PlayerMail store: %w", err)
	}
	mailLoadCtx, mailLoadCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = mailStore.EnsureLoaded(mailLoadCtx)
	mailLoadCancel()
	if err != nil {
		return fmt.Errorf("preload PlayerMail scripts: %w", err)
	}
	if err := service.SetMailRepositories(mailStore, mailStore); err != nil {
		return fmt.Errorf("compose PlayerMail ingress: %w", err)
	}
	if authorityRuntime != nil && os.Getenv("FARM_FRIEND_PUBLIC_PROFILE_MODEL_READ_ENABLED") != "0" {
		service.SetPublicProfileModelReader(authorityRuntime.Store)
	}
	friendshipRPC, stopFriendship, err := buildPlayerFriendship(projectionClient, friendshipRepositories, service, metricsRegistry)
	if err != nil {
		return fmt.Errorf("compose player FriendshipService: %w", err)
	}
	defer stopFriendship()
	// GET_PLAYER_BOOTSTRAP (PLAYER-BOOTSTRAP-AUTHORITY-A2, 28 §3.7): wire the
	// server-owned immutable TaskConfigRepository-backed TaskDomain so the
	// command can run EnsureDailyPeriod + RecordLogin in the same PlayerActor
	// Turn. A missing composition stays fail-closed in Service and never falls
	// back to a legacy EconomyVersion view.
	if err := service.SetBootstrapTaskDomain(playertask.MustNewP0Repository(), playertask.DefaultGameTimezone,
		func() int64 { return time.Now().UnixMilli() }); err != nil {
		log.Fatalf("GET_PLAYER_BOOTSTRAP composition failed: %v", err)
	}
	compendiumCatalog, err := playercompendium.BuildDefaultCatalogV1()
	if err != nil {
		return fmt.Errorf("build P0 compendium catalog: %w", err)
	}
	compendiumProvider, err := playercompendium.NewAtomicCatalogProvider(compendiumCatalog, playercompendium.DefaultLimitsV1())
	if err != nil {
		return fmt.Errorf("compose compendium catalog provider: %w", err)
	}
	if err := service.SetCompendiumCatalogProvider(compendiumProvider, playercompendium.DefaultLimitsV1()); err != nil {
		return fmt.Errorf("compose compendium ingress: %w", err)
	}
	petCatalog, err := playerpet.NewP0Repository()
	if err != nil {
		return fmt.Errorf("build P0 pet catalog: %w", err)
	}
	if err := service.SetPetCatalogProvider(petCatalog); err != nil {
		return fmt.Errorf("compose pet ingress: %w", err)
	}
	if dataMode.DirtyWritebackEnabled() {
		if authorityRuntime == nil || routeResolver == nil {
			return errors.New("player-svr: dirty GuardSync requires authority runtime and FARM route resolver")
		}
		var guardRouteResolver actorrouting.Resolver = routeResolver
		guardClients := make(map[string]contractspb.CoordinatorParticipantServiceClient)
		var guardRouteConns []*grpc.ClientConn
		if assignment != nil {
			guardRouteResolver, err = authoritycomposition.NewPooledResolverForDomains(
				routeResolver, assignment.Endpoints, actorrouting.DomainFarm,
			)
			if err != nil {
				return fmt.Errorf("compose dirty GuardSync pooled resolver: %w", err)
			}
			pooledConn, poolErr := authoritycomposition.NewPooledClientConn(routeResolver, assignment.Endpoints)
			if poolErr != nil {
				return fmt.Errorf("compose dirty GuardSync pooled client: %w", poolErr)
			}
			guardClients[authoritycomposition.PooledEndpoint] = contractspb.NewCoordinatorParticipantServiceClient(pooledConn)
		} else {
			endpoints := routeResolver.EndpointsFor(actorrouting.KindFarm)
			if len(endpoints) == 0 {
				return errors.New("player-svr: dirty GuardSync has no FARM route endpoints")
			}
			maxEndpoints := authoritycomposition.ActorRouteEndpointLimitFromEnvironment()
			if len(endpoints) > maxEndpoints {
				return fmt.Errorf("player-svr dirty GuardSync route snapshot has %d FARM endpoints, exceeds ACTOR_ROUTE_MAX_ENDPOINTS=%d", len(endpoints), maxEndpoints)
			}
			for _, endpoint := range endpoints {
				connection, dialErr := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if dialErr != nil {
					for _, conn := range guardRouteConns {
						_ = conn.Close()
					}
					return fmt.Errorf("dial dirty GuardSync FARM holder %s: %w", endpoint, dialErr)
				}
				guardRouteConns = append(guardRouteConns, connection)
				guardClients[endpoint] = contractspb.NewCoordinatorParticipantServiceClient(connection)
			}
		}
		for _, connection := range guardRouteConns {
			defer connection.Close()
		}
		guardRoute, routeErr := guardrecovery.NewFarmRouteProvider(guardRouteResolver, routeResolver.Realm())
		if routeErr != nil {
			return fmt.Errorf("compose dirty GuardSync route provider: %w", routeErr)
		}
		guardClient, routeErr := guardrecovery.NewRoutedCoordinatorClient(guardRouteResolver, routeResolver.Realm(), guardClients)
		if routeErr != nil {
			return fmt.Errorf("compose dirty GuardSync routed client: %w", routeErr)
		}
		guardDispatcher, routeErr := guardrecovery.NewBestEffortDispatcher(guardrecovery.BestEffortConfig{
			Route: guardRoute,
			Farm:  guardClient,
			BucketForPlayer: func(playerID uint64) (rediskey.AuthorityBucketRef, error) {
				return authorityRuntime.Profile.AuthorityBucketForActor(authorityRuntime.Calculator,
					actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: playerID})
			},
			OperationTimeout: envDuration("PET_GUARD_SYNC_TIMEOUT", 3*time.Second),
		})
		if routeErr != nil {
			return fmt.Errorf("compose dirty GuardSync dispatcher: %w", routeErr)
		}
		if err := service.SetBestEffortGuardSyncDispatcher(guardDispatcher); err != nil {
			return fmt.Errorf("compose dirty GuardSync ingress: %w", err)
		}
	}
	// GET_OPERATION_RESULT is a read-only view over the same generic WorkRecord
	// used by the coordinators. Keep the W31 placement object shared
	// with the typed Participant proof so the two paths cannot address different
	// buckets. A missing composition stays fail-closed in Service; in
	// particular it must not be reported as NOT_FOUND.
	var operationResultPlacement workidentity.BucketResolver
	if authorityRuntime != nil {
		operationResultPlacement = authorityRuntime.Placement
	} else if routeResolver != nil {
		var placementErr error
		operationResultPlacement, placementErr = plantonline.NewPlacementResolver(routeResolver.Snapshot(), playerPlacementConfig())
		if placementErr != nil {
			log.Printf("GET_OPERATION_RESULT disabled: W31 placement unavailable: %v", placementErr)
		}
	}
	if operationResultPlacement == nil {
		log.Printf("GET_OPERATION_RESULT disabled fail-closed: WorkRepository and W31 placement are required")
	} else {
		var resultDirectory playeringress.ResultDirectoryLookup
		resultDirectory, namespaceErr := newPlayerResultDirectory(projectionClient, keyspaceTuple, os.Getenv)
		if namespaceErr != nil {
			log.Printf("GET_OPERATION_RESULT ResultDirectory projection unavailable; authority fallback remains enabled: %v", namespaceErr)
		}
		if err := service.SetOperationResultSource(playeringress.OperationResultReaderConfig{
			Repository:           workRepository,
			BucketResolver:       operationResultPlacement,
			BucketVersion:        1,
			RetryAfter:           50 * time.Millisecond,
			TheftAdmissionLookup: friendshipRepositories,
			ResultDirectory:      resultDirectory,
		}); err != nil {
			log.Printf("GET_OPERATION_RESULT disabled fail-closed: %v", err)
		}
	}
	var typedParticipant *playerparticipant.Server
	if mode == authoritycomposition.ModeAuthority {
		var err error
		typedParticipant, err = buildPlayerParticipantServerChecked(registry, workRepository, routeResolver, routeFence, authorityRuntime.Placement, careVerifier, harvestSourceVerifier,
			commitProofMode, commitProofValidator, commitProofSourceRealm, commitProofMetrics, dataMode.DirtyWritebackEnabled())
		if err != nil {
			log.Fatal(err)
		}
	} else {
		typedParticipant = buildPlayerParticipantServer(registry, workRepository, routeResolver, operationResultPlacement)
	}
	turnLatency := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "farm", Subsystem: "actor", Name: "turn_latency_seconds",
		Help:        "Actor turn execution latency (task only, queue wait excluded).",
		ConstLabels: prometheus.Labels{"actor_kind": "player"},
		Buckets:     []float64{.00005, .0001, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	})
	metricsRegistry.MustRegister(turnLatency)
	registry.SetTurnObserver(func(_ uint64, duration time.Duration) { turnLatency.Observe(duration.Seconds()) })

	rpcMetrics := grpcmetrics.New(metricsRegistry)
	server := grpc.NewServer(grpc.UnaryInterceptor(rpcMetrics.UnaryServerInterceptor()))
	defer server.Stop()
	logicrpcpb.RegisterLogicServiceServer(server, service)
	logicrpcpb.RegisterBootstrapServiceServer(server, service)
	logicrpcpb.RegisterPlayerParticipantServiceServer(server, service)
	friendshiprpcpb.RegisterFriendshipServiceServer(server, friendshipRPC)
	registerPlayerParticipantServices(server, typedParticipant)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("player-svr listen %s: %w", addr, err)
	}
	go func() {
		if err := server.Serve(listener); err != nil {
			log.Printf("player-svr serve: %v", err)
		}
	}()
	if holderProcess != nil {
		if err := holderProcess.StartControlServer(context.Background()); err != nil {
			return err
		}
		log.Printf("player-svr HolderControl WARMING endpoint listening on %s", holderProcess.Listener.Addr())
	}
	if assignment != nil {
		runDone := make(chan struct{})
		assignmentRunDone = runDone
		go func() {
			defer close(runDone)
			if err := assignment.Run(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("player-svr remote assignment composition terminal: %v", err)
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
			if holderProcess != nil {
				stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = holderProcess.GracefulStop(stopCtx)
				stopCancel()
			}
			return fmt.Errorf("player-svr remote assignment startup: %w", err)
		}
	}
	if commitProofMode == sourceorder.CommitProofModePrefer {
		if err := sourceorder.ValidateProductionPreferComposition(commitProofMode, assignment != nil, true); err != nil {
			return fmt.Errorf("player-svr commit proof prefer composition: %w", err)
		}
		preferCtx, preferCancel := context.WithTimeout(context.Background(), authoritycomposition.RemoteStartupTimeoutFromEnvironment())
		err := sourceorder.WaitForCommitProofRouteReadiness(preferCtx, routeResolver.Snapshot, func(snapshot actorrouting.RouteSnapshot) error {
			return sourceorder.ValidateRouteCommitProofKeys(snapshot, actorrouting.DomainFarm)
		})
		preferCancel()
		if err != nil {
			return fmt.Errorf("player-svr destination route key startup: %w", err)
		}
	}
	if holderProcess != nil {
		if err := holderProcess.StartAgent(context.Background()); err != nil {
			return err
		}
		log.Printf("player-svr HolderControl sealed server listening on %s", holderProcess.Listener.Addr())
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
	log.Printf("player-svr listening on %s, version=%s", addr, farmgame.FullVersion())

	actorruntime.RegisterMetrics(metricsRegistry, "player", registry.RuntimeStats)
	dirtywriteback.RegisterMetrics(metricsRegistry, "player", registry.DirtyWritebackStats)
	metricsRegistry.MustRegister(
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "farm", Subsystem: "player_admission_snapshot", Name: "reuse_total", Help: "Participant mailbox turns that reused the exact audited outside-mailbox admission version."}, func() float64 { return float64(registry.AdmissionSnapshotStats().Reused) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "farm", Subsystem: "player_admission_snapshot", Name: "refresh_total", Help: "Participant mailbox turns that required a conservative Redis snapshot refresh."}, func() float64 { return float64(registry.AdmissionSnapshotStats().Refreshed) }),
	)
	activation.RegisterActivationMetrics(metricsRegistry, "player", registry.ActivationStats)
	idle.RegisterMetrics(metricsRegistry, "player", registry.IdleSweeperStats,
		[]string{"turn_running", "user_queue_nonempty", "system_queue_nonempty"})
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
		return true
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
			log.Printf("player-svr health: %v", err)
		}
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	draining.Store(true)
	if drainGate != nil {
		drainPlayerShards(drainGate, authorityShardCount)
	}
	registry.Stop()
	if holderProcess != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), gatewayapp.BoundedHolderTimeout(envDuration("HOLDER_CLOSE_TIMEOUT", 10*time.Second), 10*time.Second))
		if err := holderProcess.Agent.Close(closeCtx); err != nil {
			log.Printf("player-svr HolderAgent close: %v", err)
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
		log.Printf("player-svr business gRPC forced stop: %v", err)
	}
	serverStopCancel()
	if holderProcess != nil {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), gatewayapp.BoundedHolderTimeout(envDuration("HOLDER_SERVER_CLOSE_TIMEOUT", 5*time.Second), 5*time.Second))
		if err := holderProcess.GracefulStop(stopCtx); err != nil {
			log.Printf("player-svr HolderControl server stop: %v", err)
		}
		stopCancel()
	}
	if authorityRuntime != nil {
		_ = authorityRuntime.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = health.Shutdown(ctx)
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func buildPlayerHolderProcess(client redis.UniversalClient, runtime *authoritycomposition.Runtime, assignment *authoritycomposition.AssignmentComposition, registry *playeractor.Registry, drainGate *actorruntime.ShardDrainGate, draining *atomic.Bool) (*gatewayapp.HolderProcess, *routecontrol.HolderRegistry, error) {
	if runtime == nil || assignment == nil || registry == nil || drainGate == nil || draining == nil {
		return nil, nil, errors.New("player-svr HolderControl requires authority runtime, registry, and drain gate")
	}
	namespace := runtime.Profile.Namespace
	if err := namespace.Validate(); err != nil {
		return nil, nil, fmt.Errorf("player-svr HolderRegistry namespace: %w", err)
	}
	leaseTTL := envDuration("HOLDER_AGENT_LEASE_TTL", 30*time.Second)
	holderStoreOptions := redisroutecontrol.DefaultHolderStoreOptions(namespace)
	holderStoreOptions.OperationTimeout = gatewayapp.BoundedHolderTimeout(envDuration("HOLDER_REGISTRY_OPERATION_TIMEOUT", 2*time.Second), 2*time.Second)
	holderStoreOptions.LeaseTTL = leaseTTL
	holderStoreOptions.MinLeaseTTL = time.Millisecond
	holderClient := client
	holderCtx, holderCancel := context.WithTimeout(context.Background(), 5*time.Second)
	dedicatedHolderClient, holderDialErr := redisdomains.OpenEndpoint(holderCtx, os.Getenv("FARM_REDIS_URL"), os.Getenv("FARM_REDIS_CLUSTER_URLS"))
	holderCancel()
	if holderDialErr != nil {
		log.Printf("player-svr Holder heartbeat redis isolation unavailable: %v", holderDialErr)
	} else {
		holderClient = dedicatedHolderClient
		log.Printf("player-svr Holder heartbeat using isolated redis pool")
	}
	holderStore, err := redisroutecontrol.NewHolderStore(holderClient, holderStoreOptions)
	if err != nil {
		return nil, nil, fmt.Errorf("player-svr HolderRegistry store: %w", err)
	}
	registryOptions := routecontrol.DefaultHolderRegistryOptions()
	registryOptions.LeaseTTL = leaseTTL
	registryOptions.MinLeaseTTL = time.Millisecond
	allowInitialServing := os.Getenv("HOLDER_ALLOW_INITIAL_SERVING") == "true"
	if allowInitialServing {
		registryOptions.InitialServingVerifier, err = routecontrol.NewStaticInitialServingVerifier(routecontrol.StaticInitialServingVerifierOptions{
			Domain: runtime.Domain, HolderID: runtime.HolderID, HolderBootID: runtime.HolderBootID,
			LogicEndpoint: os.Getenv("PLAYER_LOGIC_ENDPOINT"), Snapshot: assignment.Provider.GetSnapshot,
			ProcessReady: runtime.ProcessReady, AssignmentHealthy: assignment.Healthy,
			ShardOpen: func(shard uint32) bool {
				status, ok := drainGate.Status(shard)
				return ok && status.Phase == actorruntime.ShardDrainPhaseOpen
			},
			CanServeShard: runtime.CanServeShard, Now: time.Now,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("player-svr static Holder serving admission: %w", err)
		}
	}
	holderRegistry, err := routecontrol.NewHolderRegistry(holderStore, registryOptions)
	if err != nil {
		return nil, nil, fmt.Errorf("player-svr HolderRegistry: %w", err)
	}

	shardFor := func(actorID uint64) (uint32, error) {
		return runtime.Calculator.ShardFor(runtime.Domain, actorID)
	}
	holderRuntime, err := playeractor.NewHolderShardRuntime(registry, shardFor)
	if err != nil {
		_ = holderRegistry.Close()
		return nil, nil, fmt.Errorf("player-svr HolderShardRuntime: %w", err)
	}
	var preparer holdercontrol.ShardGenerationPreparer
	if inner := runtime.MembershipInstaller(); inner != nil {
		prepared, ok := inner.(*holdercontrol.PrepareOnlyMembershipInstaller)
		if !ok {
			_ = holderRegistry.Close()
			return nil, nil, errors.New(
				"player-svr Holder prepare seam is not shared with membership reconciliation",
			)
		}
		preparer = &holdercontrol.DomainGenerationPreparer{Domain: runtime.Domain, Installer: prepared}
	}
	controllerIdentity := env("HOLDER_CONTROL_CONTROLLER_IDENTITY", "route-controller")
	rules := holdercontrol.HolderControlMethodACL([]string{controllerIdentity})
	if err := holdercontrol.ValidateHolderControlMethodACL(rules); err != nil {
		_ = holderRegistry.Close()
		return nil, nil, fmt.Errorf("player-svr Holder ACL: %w", err)
	}
	authorizer, err := transport.NewMethodAuthorizer(rules)
	if err != nil {
		_ = holderRegistry.Close()
		return nil, nil, fmt.Errorf("player-svr Holder method authorizer: %w", err)
	}
	statusSource := holdercontrol.ProcessStatusSourceFunc(func(ctx context.Context) (holdercontrol.ProcessStatus, error) {
		if ctx == nil {
			return holdercontrol.ProcessStatus{}, errors.New("player-svr Holder status context is required")
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
		Now:                         time.Now,
		OperationTimeout:            gatewayapp.BoundedHolderTimeout(envDuration("HOLDER_CONTROL_OPERATION_TIMEOUT", 5*time.Second), 5*time.Second),
		Player: holdercontrol.DomainRuntimeOptions{
			Runtime: holderRuntime, Preparer: preparer, ShardCount: drainGate.ShardCount(),
		},
	})
	if err != nil {
		_ = holderRegistry.Close()
		return nil, nil, fmt.Errorf("player-svr Holder local service: %w", err)
	}
	agentOptions := holdercontrol.DefaultHolderAgentOptions()
	agentIdentity := playerHolderAgentIdentityOptions(runtime)
	agentOptions.Domain = runtime.Domain
	agentOptions.HolderID = agentIdentity.HolderID
	agentOptions.DeploymentIdentity = agentIdentity.DeploymentIdentity
	agentOptions.LogicEndpoint = os.Getenv("PLAYER_LOGIC_ENDPOINT")
	agentOptions.HolderControlEndpoint = os.Getenv("PLAYER_HOLDER_ENDPOINT")
	agentOptions.TLSDNSIdentity = os.Getenv("PLAYER_HOLDER_TLS_DNS_IDENTITY")
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
		ListenAddr:          env("PLAYER_HOLDER_CONTROL_LISTEN", ":19100"),
		ServerCertFile:      os.Getenv("PLAYER_HOLDER_TLS_CERT_FILE"),
		ServerKeyFile:       os.Getenv("PLAYER_HOLDER_TLS_KEY_FILE"),
		ClientCAFile:        os.Getenv("PLAYER_HOLDER_CLIENT_CA_FILE"),
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

func playerHolderAgentIdentityOptions(runtime *authoritycomposition.Runtime) holdercontrol.HolderAgentOptions {
	if runtime == nil {
		return holdercontrol.HolderAgentOptions{}
	}
	bootID := runtime.HolderBootID
	return holdercontrol.HolderAgentOptions{
		HolderID: runtime.HolderID, DeploymentIdentity: runtime.DeploymentIdentity,
		BootID: func() (string, error) { return bootID, nil },
	}
}

func drainPlayerShards(gate *actorruntime.ShardDrainGate, shardCount uint32) {
	if gate == nil {
		return
	}
	for shard := uint32(0); shard < shardCount; shard++ {
		if err := gate.BeginDrain(shard); err != nil {
			log.Printf("player-svr begin shard %d drain: %v", shard, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), gatewayapp.BoundedHolderTimeout(envDuration("HOLDER_DRAIN_TIMEOUT", 10*time.Second), 10*time.Second))
	defer cancel()
	for shard := uint32(0); shard < shardCount; shard++ {
		if err := gate.Wait(ctx, shard); err != nil {
			log.Printf("player-svr wait shard %d drain barrier: %v", shard, err)
		}
	}
}

func registerPlayerParticipantServices(registrar grpc.ServiceRegistrar, participant *playerparticipant.Server) {
	if registrar == nil || participant == nil {
		return
	}
	contractspb.RegisterCoordinatorParticipantServiceServer(registrar, participant)
	contractspb.RegisterWorkerRecoveryIngressServer(registrar, participant)
}

func configureActorRoute(registry *playeractor.Registry, routeMetrics *actorrouting.Metrics) *actorrouting.StaticResolver {
	path := os.Getenv("ACTOR_ROUTE_SNAPSHOT")
	if path == "" {
		log.Printf("typed Player Participant disabled: ACTOR_ROUTE_SNAPSHOT is required")
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
	registry.SetWriterEpochResolver(resolver.HolderEpochResolver(actorrouting.KindPlayer, holder))
	log.Printf("player holder fencing enabled pod=%s snapshot=%s", holder, path)
	return resolver
}

func buildPlayerParticipantServer(registry *playeractor.Registry, repository workflow.WorkRepository, resolver *actorrouting.StaticResolver, placementOverrides ...workidentity.BucketResolver) *playerparticipant.Server {
	var proof *participantproof.Verifier
	if repository != nil && resolver != nil {
		holderID := os.Getenv("ACTOR_HOLDER_POD")
		bootID := os.Getenv("ACTOR_HOLDER_BOOT_ID")
		fence, fenceErr := plantonline.NewRouteFence(resolver, resolver.Realm(), actorrouting.DomainPlayer, holderID, bootID)
		if fenceErr == nil {
			var placement workidentity.BucketResolver
			if len(placementOverrides) > 0 {
				placement = placementOverrides[0]
			}
			var placementErr error
			if placement == nil {
				placement, placementErr = plantonline.NewPlacementResolver(resolver.Snapshot(), playerPlacementConfig())
			}
			if placementErr == nil {
				proof, _ = participantproof.New(repository,
					participantproof.WithBucketResolver(placement),
					participantproof.WithRouteFenceVerifier(fence),
					participantproof.WithClock(time.Now),
				)
			} else {
				log.Printf("typed Player Participant disabled: %v", placementErr)
			}
		} else {
			log.Printf("typed Player Participant disabled: %v", fenceErr)
		}
	} else {
		log.Printf("typed Player Participant disabled: WorkRepository and route snapshot are required")
	}
	server := playerparticipant.NewServer(playerparticipant.Options{
		Registry:                  registry,
		WorkVersionVerifier:       proof,
		WorkLeaseVerifier:         proof,
		RouteFenceVerifier:        proof,
		WorkAddressVerifier:       proof,
		IdentityBindingVerifier:   proof,
		PlantDependencyVerifier:   proof,
		UnlockDependencyVerifier:  proof,
		HarvestDependencyVerifier: proof,
		TheftDependencyVerifier:   proof,
		RequireRouteLeaseProof:    envBool("PARTICIPANT_ROUTE_LEASE_PROOF_STRICT", false),
		TaskConfigRepository:      playertask.MustNewP0Repository(),
		TaskTimezone:              playertask.DefaultGameTimezone,
		NowMs:                     func() int64 { return time.Now().UnixMilli() },
		Now:                       time.Now,
	})
	if err := registry.ParticipantAdmissionError(); err != nil {
		log.Printf("typed Player Participant fail-closed until authority-backed registry is configured: %v", err)
	}
	return server
}

func buildPlayerParticipantServerChecked(registry *playeractor.Registry, repository workflow.WorkRepository, resolver *actorrouting.StaticResolver, routeFence participantproof.RouteFenceVerifier, placement workidentity.BucketResolver, careVerifier playerparticipant.CareDependencyVerifier, harvestSource playerparticipant.HarvestSourceVerifier, commitProofMode sourceorder.CommitProofMode, commitProofValidator *sourceorder.CommitProofValidator, commitProofSourceRealm string, commitProofMetrics *playerparticipant.CommitProofMetrics, bestEffortNoSourcePersistence bool) (*playerparticipant.Server, error) {
	if registry == nil || repository == nil || resolver == nil || routeFence == nil || placement == nil || careVerifier == nil {
		return nil, errors.New("player participant authority composition is incomplete")
	}
	proof, err := participantproof.New(repository,
		participantproof.WithBucketResolver(placement),
		participantproof.WithRouteFenceVerifier(routeFence),
		participantproof.WithClock(time.Now),
	)
	if err != nil {
		return nil, fmt.Errorf("build player Participant proof: %w", err)
	}
	return playerparticipant.NewServerChecked(playerparticipant.Options{
		Registry:                      registry,
		WorkVersionVerifier:           proof,
		WorkLeaseVerifier:             proof,
		RouteFenceVerifier:            proof,
		WorkAddressVerifier:           proof,
		IdentityBindingVerifier:       proof,
		PlantDependencyVerifier:       proof,
		UnlockDependencyVerifier:      proof,
		HarvestDependencyVerifier:     proof,
		HarvestSourceVerifier:         harvestSource,
		TheftSourceVerifier:           theftSourceFrom(harvestSource),
		TheftDependencyVerifier:       proof,
		CareDependencyVerifier:        careVerifier,
		BestEffortNoSourcePersistence: bestEffortNoSourcePersistence,
		CommitProofMode:               commitProofMode,
		CommitProofValidator:          commitProofValidator,
		CommitProofSourceRealm:        commitProofSourceRealm,
		CommitProofMetrics:            commitProofMetrics,
		RequireRouteLeaseProof:        envBool("PARTICIPANT_ROUTE_LEASE_PROOF_STRICT", false),
		TaskConfigRepository:          playertask.MustNewP0Repository(),
		TaskTimezone:                  playertask.DefaultGameTimezone,
		NowMs:                         func() int64 { return time.Now().UnixMilli() },
		Now:                           time.Now,
	})
}

func theftSourceFrom(harvestSource playerparticipant.HarvestSourceVerifier) playerparticipant.TheftSourceVerifier {
	if source, ok := harvestSource.(playerparticipant.TheftSourceVerifier); ok {
		return source
	}
	return nil
}

func playerPlacementConfig() plantonline.PlacementConfig {
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

func openStore(keyspaceTuple *rediskey.KeyspaceTuple) (redis.UniversalClient, redis.UniversalClient, workflow.WorkRepository, *friend.RedisFriendshipRepositories, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	clients, err := redisdomains.OpenFromEnv(ctx, os.Getenv,
		redisdomains.SelectAuthority|redisdomains.SelectProjection)
	cancel()
	if err != nil {
		log.Fatal(err)
	}
	authorityClient := clients.Client(redisdomains.Authority)
	projectionClient := clients.Client(redisdomains.Projection)
	if authorityClient == nil || projectionClient == nil {
		_ = clients.Close()
		log.Fatal("redisdomains: selected domain client is missing")
	}
	workRepository, err := workflow.NewRedisWorkRepository(authorityClient, env("WORKFLOW_STORE_PREFIX", "farm:v121:m1:workflow"))
	if err != nil {
		_ = clients.Close()
		log.Fatal(err)
	}
	namespace, err := playerRedisNamespace(keyspaceTuple, os.Getenv)
	if err != nil {
		_ = clients.Close()
		log.Fatal(err)
	}
	social := socialprofile.Current()
	friendshipRepositories, err := friend.NewRedisFriendshipRepositoriesForDomains(authorityClient, projectionClient, friend.RedisFriendshipOptions{
		Profile: friend.DefaultFriendRedisProfile(namespace),
		Limits:  friend.FriendRedisLimits{MaxFriends: social.EffectiveMaxFriends, ShareTokenTTLMs: social.ShareTokenTTL.Milliseconds()},
	})
	if err != nil {
		_ = clients.Close()
		log.Fatal(err)
	}
	log.Println("player-svr using authority Redis for workflow records and Friendship authorities; projection Redis for Friendship reads")
	return authorityClient, projectionClient, workRepository, friendshipRepositories, func() { _ = clients.Close() }
}

func playerRedisNamespace(keyspaceTuple *rediskey.KeyspaceTuple, getenv func(string) string) (rediskey.Namespace, error) {
	if keyspaceTuple != nil {
		namespace := keyspaceTuple.Namespace()
		return namespace, namespace.Validate()
	}
	if getenv == nil {
		return rediskey.Namespace{}, errors.New("player-svr Redis namespace environment reader is required")
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

func newPlayerResultDirectory(client redis.UniversalClient, keyspaceTuple *rediskey.KeyspaceTuple, getenv func(string) string) (*redisauthority.ResultDirectoryStore, error) {
	namespace, err := playerRedisNamespace(keyspaceTuple, getenv)
	if err != nil {
		return nil, err
	}
	return redisauthority.NewResultDirectoryStore(client, nil, namespace, 0)
}

func requiredRemoteKeyspaceTuple(membership authoritycomposition.MembershipMode, assignment authoritycomposition.AssignmentMode) (*rediskey.KeyspaceTuple, error) {
	if !authoritycomposition.HolderCompositionEnabled(membership, assignment) {
		return nil, nil
	}
	tuple, err := rediskey.ParseKeyspaceTupleEnvironment(os.Getenv)
	if err != nil {
		return nil, fmt.Errorf("player-svr remote Redis keyspace tuple: %w", err)
	}
	fingerprint, err := tuple.Fingerprint()
	if err != nil {
		return nil, fmt.Errorf("player-svr remote Redis keyspace fingerprint: %w", err)
	}
	log.Printf("player-svr Redis keyspace fingerprint=%s", fingerprint)
	return &tuple, nil
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
		log.Printf("WARNING: pprof is enabled on the player-svr health listener")
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
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
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
