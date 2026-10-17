// Command farm-worker advances durable cross-authority farm workflows.
package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"farmgame"
	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/authoritycomposition"
	"farmgame/internal/friend"
	"farmgame/internal/identity"
	"farmgame/internal/logic/actor/datamode"
	"farmgame/internal/observability/redisworkload"
	playermail "farmgame/internal/player/mail"
	"farmgame/internal/redisdomains"
	theftscenario "farmgame/internal/scenario/theft"
	"farmgame/internal/sourceordermetrics"
	"farmgame/internal/storage/redis/authority"
	redisplayermail "farmgame/internal/storage/redis/playermail"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/theftonline"
	"farmgame/internal/worker/carerecovery"
	"farmgame/internal/worker/guardrecovery"
	"farmgame/internal/worker/maildeliveryrecovery"
	"farmgame/internal/worker/mailmaintenance"
	"farmgame/internal/worker/mailrecovery"
	"farmgame/internal/worker/opportunityrecovery"
	workerreadiness "farmgame/internal/worker/readiness"
	"farmgame/internal/worker/theftrecovery"
	"farmgame/internal/workflow"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var errPairCareRecoveryNotReady = errors.New("shared Pair CARE recovery is not ready")

type farmParticipant struct {
	client logicrpcpb.FarmWorkflowServiceClient
}

func theftRecoveryClients(clients map[string]contractspb.WorkerRecoveryIngressClient) map[string]theftscenario.WorkerRecoveryParticipantClient {
	result := make(map[string]theftscenario.WorkerRecoveryParticipantClient, len(clients))
	for endpoint, client := range clients {
		result[endpoint] = client
	}
	return result
}

func careRecoveryClients(clients map[string]contractspb.WorkerRecoveryIngressClient) map[string]carerecovery.WorkerRecoveryParticipantClient {
	result := make(map[string]carerecovery.WorkerRecoveryParticipantClient, len(clients))
	for endpoint, client := range clients {
		result[endpoint] = client
	}
	return result
}

func guardRecoveryClients(clients map[string]contractspb.WorkerRecoveryIngressClient) map[string]guardrecovery.WorkerRecoveryParticipantClient {
	result := make(map[string]guardrecovery.WorkerRecoveryParticipantClient, len(clients))
	for endpoint, client := range clients {
		result[endpoint] = client
	}
	return result
}

func runSourceOrderRecoveryLoop(ctx context.Context, pollInterval, idleMaxInterval time.Duration, family string, worker *carerecovery.Worker) {
	if worker == nil {
		return
	}
	if pollInterval <= 0 {
		pollInterval = time.Second
	}
	if idleMaxInterval < pollInterval {
		idleMaxInterval = pollInterval
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	var sweep adaptiveShardSweep
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			stats, scanErr := worker.RunOnce(ctx)
			if scanErr != nil {
				log.Printf("recover %s SourceOrder: %v", family, scanErr)
			}
			timer.Reset(sweep.next(pollInterval, idleMaxInterval, stats.Revision, stats.OwnedBuckets, stats.Buckets, stats.Candidates, scanErr))
		}
	}
}

// buildCareRecovery composes SourceOrder recovery against the existing
// auth-v1 authority bucket layout. THEFT dest uses THEFT_VISITOR/THEFT_GUARD
// SourceOrders; Pair dispatch recovery SourceCommits those orders separately.
func buildCareRecovery(redisClient redis.UniversalClient, profile rediskey.PhysicalProfile, staticResolver *actorrouting.StaticResolver, routeResolver actorrouting.Resolver, playerClients map[string]contractspb.WorkerRecoveryIngressClient, farmClients map[string]contractspb.WorkerRecoveryIngressClient, shardRuntime *farmWorkerShardRuntime, sharedDue *carerecovery.SharedDueIndexCache, owner, token, bootID, family string) (*carerecovery.Composition, error) {
	if staticResolver == nil || routeResolver == nil || shardRuntime == nil {
		return nil, fmt.Errorf("care recovery requires a static Actor route snapshot")
	}
	snapshot := staticResolver.Snapshot()
	calculator, err := actorrouting.NewCalculator(snapshot.Realm,
		actorrouting.HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed},
		snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		return nil, fmt.Errorf("care recovery route calculator: %w", err)
	}
	if err := profile.Validate(); err != nil {
		return nil, fmt.Errorf("care recovery Redis keyspace tuple: %w", err)
	}
	scheduleDueIndexMode, scheduleDueIndexReady, err := scheduleDueIndexModeFromEnvironment()
	if err != nil {
		return nil, err
	}
	authorityStore, err := authority.NewStore(redisClient, nil, authority.Options{
		Profile: profile, Resolver: calculator, ScheduleDueIndexMode: scheduleDueIndexMode,
		ScheduleDueIndexDedicatedReady: scheduleDueIndexReady,
	})
	if err != nil {
		return nil, fmt.Errorf("care recovery authority store: %w", err)
	}
	scanDomain := actorrouting.DomainFarm
	switch strings.TrimSpace(family) {
	case string(identity.WorkTypePlant), string(identity.WorkTypeUnlockPlot):
		scanDomain = actorrouting.DomainPlayer
	}
	shardCount, err := calculator.ShardCount(scanDomain)
	if err != nil {
		return nil, fmt.Errorf("care recovery shard count: %w", err)
	}
	buckets := make([]rediskey.AuthorityBucketRef, 0, shardCount)
	configuredShards := make([]uint32, 0, shardCount)
	for shard := uint32(0); shard < shardCount; shard++ {
		bucket, bucketErr := profile.AuthorityBucketForShard(calculator, scanDomain, shard)
		if bucketErr != nil {
			return nil, fmt.Errorf("care recovery bucket %d: %w", shard, bucketErr)
		}
		buckets = append(buckets, bucket)
		configuredShards = append(configuredShards, shard)
	}
	workerShardCount := snapshot.FarmWorker.ShardCount
	if workerShardCount == 0 {
		workerShardCount = snapshot.Farm.ShardCount
	}
	shardAssignment, err := newMappedSourceOrderShardAssignment(
		shardRuntime.Provider,
		shardRuntime.Admission,
		configuredShards,
		workerShardCount,
	)
	if err != nil {
		return nil, fmt.Errorf("care recovery shard assignment: %w", err)
	}
	workLease := redisworkload.NewAttributedWorkLease(authorityStore.WorkLease())
	dueIndex := carerecovery.DueIndexScanner(workLease)
	var dueObserver carerecovery.DueIndexObserver
	if sharedDue != nil {
		// Shared family caching is permitted only for the proof-carrying WorkLease
		// page API. A decoded-ref-only adapter cannot prove that schedules,
		// malformed members, or another family did not consume its prefix.
		if _, ok := dueIndex.(carerecovery.DueIndexPageScanner); !ok {
			return nil, fmt.Errorf("care recovery shared DueIndex requires completeness metadata")
		}
		sharedScanner := sharedDue.ForFamily(family, dueIndex)
		dueIndex = sharedScanner
		dueObserver = sharedScanner
	}
	composition, err := carerecovery.New(carerecovery.Config{
		AuthorityStore: authorityStore, WorkLease: workLease, DueIndex: dueIndex, DueObserver: dueObserver,
		ScanLeases: workLease, Buckets: buckets,
		ShardAssignment: shardAssignment, ClaimAdmission: shardAssignment,
		BucketsPerRound:        parseInt("SOURCE_ORDER_WORKER_BUCKETS_PER_ROUND", 256),
		DueObservationInterval: parseDuration("SOURCE_ORDER_DUE_OBSERVE_INTERVAL", 10*time.Second),
		RecoveryGrace:          parseDuration("SOURCE_ORDER_RECOVERY_GRACE", 0),
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) {
			return profile.AuthorityBucketForActor(calculator, actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
		},
		BucketForPlayer: func(playerID uint64) (rediskey.AuthorityBucketRef, error) {
			return profile.AuthorityBucketForActor(calculator, actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: playerID})
		},
		RouteResolver: routeResolver, Realm: staticResolver.Realm(),
		PlayerClients: careRecoveryClients(playerClients), FarmClients: careRecoveryClients(farmClients),
		Owner: owner, Token: token, BootID: bootID,
		ClaimLimit: parseInt("CARE_WORKER_CLAIM_LIMIT", 32), MaxConcurrent: parseInt("CARE_WORKER_MAX_CONCURRENT", 4),
		LeaseTTL:            parseDuration("CARE_WORKER_LEASE", 10*time.Second),
		SafetyMargin:        parseDuration("CARE_WORKER_LEASE_SAFETY_MARGIN", 2*time.Second),
		OperationTimeout:    parseDuration("CARE_WORKER_OPERATION_TIMEOUT", 3*time.Second),
		ParticipantBudgetMs: uint32(parseInt("CARE_WORKER_PARTICIPANT_BUDGET_MS", int(5000))),
		ScanLeaseTTL:        parseDuration("CARE_WORKER_SCAN_LEASE", 10*time.Second),
		// Bucket scan leases are only a duplicate-discovery optimization. The
		// shard assignment is disjoint and every execution still requires the
		// per-order WorkLease, so high-throughput deployments may disable them.
		UseScanLease: parseBool("SOURCE_ORDER_SCAN_LEASE_ENABLED", true),
		Family:       family,
	})
	if err != nil {
		return nil, err
	}
	return composition, nil
}

// buildMailRecovery composes the PLAYER source-order recovery lane and the
// rebuildable mailbox projection against the same Redis namespace. It does
// not activate PlayerActor and does not require a participant RPC.
func buildMailRecovery(redisClient redis.UniversalClient, profile rediskey.PhysicalProfile, staticResolver *actorrouting.StaticResolver, owner, token, bootID string) (*mailrecovery.Worker, error) {
	if redisClient == nil || staticResolver == nil {
		return nil, fmt.Errorf("mail recovery requires Authority Redis and an Actor route snapshot")
	}
	snapshot := staticResolver.Snapshot()
	calculator, err := actorrouting.NewCalculator(snapshot.Realm,
		actorrouting.HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed},
		snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		return nil, fmt.Errorf("mail recovery route calculator: %w", err)
	}
	if err := profile.Validate(); err != nil {
		return nil, fmt.Errorf("mail recovery Redis keyspace tuple: %w", err)
	}
	scheduleDueIndexMode, scheduleDueIndexReady, err := scheduleDueIndexModeFromEnvironment()
	if err != nil {
		return nil, err
	}
	authorityStore, err := authority.NewStore(redisClient, nil, authority.Options{
		Profile: profile, Resolver: calculator, ScheduleDueIndexMode: scheduleDueIndexMode,
		ScheduleDueIndexDedicatedReady: scheduleDueIndexReady,
	})
	if err != nil {
		return nil, fmt.Errorf("mail recovery authority store: %w", err)
	}
	playerShards, err := calculator.ShardCount(actorrouting.DomainPlayer)
	if err != nil {
		return nil, fmt.Errorf("mail recovery PLAYER shard count: %w", err)
	}
	buckets := make([]rediskey.AuthorityBucketRef, 0, playerShards)
	for shard := uint32(0); shard < playerShards; shard++ {
		bucket, bucketErr := profile.AuthorityBucketForShard(calculator, actorrouting.DomainPlayer, shard)
		if bucketErr != nil {
			return nil, fmt.Errorf("mail recovery PLAYER bucket %d: %w", shard, bucketErr)
		}
		buckets = append(buckets, bucket)
	}
	mailBucketCount := parseInt("PLAYER_MAIL_BUCKET_COUNT", int(redisplayermail.DefaultBucketCount))
	if mailBucketCount <= 0 {
		return nil, fmt.Errorf("mail recovery PLAYER_MAIL_BUCKET_COUNT must be positive")
	}
	mailStore, err := redisplayermail.NewStore(redisClient, redisplayermail.Profile{
		Namespace: profile.Namespace, BucketCount: uint32(mailBucketCount),
	}, playermail.LimitsV1())
	if err != nil {
		return nil, fmt.Errorf("mail recovery projection store: %w", err)
	}
	loadCtx, loadCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = mailStore.EnsureLoaded(loadCtx)
	loadCancel()
	if err != nil {
		return nil, fmt.Errorf("mail recovery projection scripts: %w", err)
	}
	workLease := redisworkload.NewAttributedWorkLease(authorityStore.WorkLease())
	worker, err := mailrecovery.NewWorker(mailrecovery.Config{
		AuthorityStore: authorityStore, WorkLease: workLease, DueIndex: workLease, MailEntries: mailStore, Buckets: buckets,
		Owner: owner, Token: token, BootID: bootID,
		ClaimLimit: parseInt("MAIL_WORKER_CLAIM_LIMIT", 32), MaxConcurrent: parseInt("MAIL_WORKER_MAX_CONCURRENT", 4),
		LeaseTTL:         parseDuration("MAIL_WORKER_LEASE", 10*time.Second),
		SafetyMargin:     parseDuration("MAIL_WORKER_LEASE_SAFETY_MARGIN", 2*time.Second),
		OperationTimeout: parseDuration("MAIL_WORKER_OPERATION_TIMEOUT", 3*time.Second),
		RetryDelay:       parseDuration("MAIL_WORKER_RETRY_DELAY", 250*time.Millisecond),
	})
	if err != nil {
		return nil, err
	}
	return worker, nil
}

func buildGuardRecovery(redisClient redis.UniversalClient, profile rediskey.PhysicalProfile, staticResolver *actorrouting.StaticResolver, routeResolver actorrouting.Resolver, farmClients map[string]contractspb.WorkerRecoveryIngressClient, owner, token, bootID string) (*guardrecovery.Worker, error) {
	if redisClient == nil || staticResolver == nil || routeResolver == nil || len(farmClients) == 0 {
		return nil, fmt.Errorf("guard recovery requires Authority Redis, Actor routing and FARM Recovery clients")
	}
	snapshot := staticResolver.Snapshot()
	calculator, err := actorrouting.NewCalculator(snapshot.Realm,
		actorrouting.HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed},
		snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		return nil, fmt.Errorf("guard recovery route calculator: %w", err)
	}
	if err := profile.Validate(); err != nil {
		return nil, fmt.Errorf("guard recovery Redis keyspace tuple: %w", err)
	}
	scheduleDueIndexMode, scheduleDueIndexReady, err := scheduleDueIndexModeFromEnvironment()
	if err != nil {
		return nil, err
	}
	authorityStore, err := authority.NewStore(redisClient, nil, authority.Options{
		Profile: profile, Resolver: calculator, ScheduleDueIndexMode: scheduleDueIndexMode,
		ScheduleDueIndexDedicatedReady: scheduleDueIndexReady,
	})
	if err != nil {
		return nil, fmt.Errorf("guard recovery authority store: %w", err)
	}
	playerShards, err := calculator.ShardCount(actorrouting.DomainPlayer)
	if err != nil {
		return nil, fmt.Errorf("guard recovery PLAYER shard count: %w", err)
	}
	buckets := make([]rediskey.AuthorityBucketRef, 0, playerShards)
	for shard := uint32(0); shard < playerShards; shard++ {
		bucket, bucketErr := profile.AuthorityBucketForShard(calculator, actorrouting.DomainPlayer, shard)
		if bucketErr != nil {
			return nil, fmt.Errorf("guard recovery PLAYER bucket %d: %w", shard, bucketErr)
		}
		buckets = append(buckets, bucket)
	}
	workLease := redisworkload.NewAttributedWorkLease(authorityStore.WorkLease())
	return guardrecovery.NewWorker(guardrecovery.Config{
		AuthorityStore: authorityStore, WorkLease: workLease, DueIndex: workLease, Buckets: buckets,
		RouteResolver: routeResolver, Realm: staticResolver.Realm(), FarmClients: guardRecoveryClients(farmClients),
		Owner: owner, Token: token, BootID: bootID,
		ClaimLimit: parseInt("GUARD_SYNC_WORKER_CLAIM_LIMIT", 32), MaxConcurrent: parseInt("GUARD_SYNC_WORKER_MAX_CONCURRENT", 4),
		LeaseTTL:            parseDuration("GUARD_SYNC_WORKER_LEASE", 10*time.Second),
		SafetyMargin:        parseDuration("GUARD_SYNC_WORKER_LEASE_SAFETY_MARGIN", 2*time.Second),
		OperationTimeout:    parseDuration("GUARD_SYNC_WORKER_OPERATION_TIMEOUT", 3*time.Second),
		ParticipantBudgetMs: uint32(parseInt("GUARD_SYNC_WORKER_PARTICIPANT_BUDGET_MS", 5000)),
		RetryDelay:          parseDuration("GUARD_SYNC_WORKER_RETRY_DELAY", 250*time.Millisecond),
	})
}

func (p farmParticipant) InspectFarmOperation(ctx context.Context, request *logicrpcpb.InspectFarmOperationRequest) (*logicrpcpb.InspectFarmOperationResponse, error) {
	return p.client.InspectFarmOperation(ctx, request)
}
func (p farmParticipant) CompleteFarmOperation(ctx context.Context, request *logicrpcpb.CompleteFarmOperationRequest) (*logicrpcpb.CompleteFarmOperationResponse, error) {
	return p.client.CompleteFarmOperation(ctx, request)
}

func main() {
	dataMode, dataModeErr := datamode.FromLookup(os.LookupEnv)
	if dataModeErr != nil {
		log.Fatal(dataModeErr)
	}
	businessRecoveryEnabled := !dataMode.DirtyWritebackEnabled()
	if !businessRecoveryEnabled {
		log.Printf("WARNING: %s=%s: durable Plant/Unlock/Harvest/Care/Theft/Guard recovery is disabled; mail, opportunity wakeup, route ownership and health loops remain enabled", datamode.EnvKey, dataMode)
	}
	authorityMode, modeErr := authoritycomposition.ModeFromEnvironment()
	if modeErr != nil {
		log.Fatal(modeErr)
	}
	membershipMode, membershipErr := authoritycomposition.MembershipModeFromEnvironment()
	if membershipErr != nil {
		log.Fatal(membershipErr)
	}
	assignmentMode, assignmentErr := authoritycomposition.AssignmentModeFromEnvironment()
	if assignmentErr != nil {
		log.Fatal(assignmentErr)
	}
	if err := authoritycomposition.ValidateModeCombination(membershipMode, assignmentMode); err != nil {
		log.Fatal(err)
	}
	keyspaceTuple, err := requiredRemoteKeyspaceTuple(membershipMode, assignmentMode)
	if err != nil {
		log.Fatal(err)
	}
	workerProfile, err := workerPhysicalProfile(keyspaceTuple)
	if err != nil {
		log.Fatal(err)
	}
	redisOpenCtx, redisOpenCancel := context.WithTimeout(context.Background(), 5*time.Second)
	redisClients, redisErr := redisdomains.OpenFromEnv(redisOpenCtx, os.Getenv, redisdomains.SelectAll)
	redisOpenCancel()
	if redisErr != nil {
		log.Fatal(redisErr)
	}
	defer redisClients.Close()
	processCtx, processCancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer processCancel()

	var farm workflow.FarmParticipant
	var routeConns []*grpc.ClientConn
	var playerHolderConns []holderConnection
	var farmHolderConns []holderConnection
	var routeResolver actorrouting.Resolver
	var staticRouteResolver *actorrouting.StaticResolver
	var routeRealm string
	var playerRecoveryClients map[string]contractspb.WorkerRecoveryIngressClient
	var farmRecoveryClients map[string]contractspb.WorkerRecoveryIngressClient
	var assignment *authoritycomposition.AssignmentComposition
	var assignmentClient interface{ Close() error }
	if assignmentMode == authoritycomposition.AssignmentModeRemoteWatch {
		dialCtx, dialCancel := context.WithTimeout(processCtx, authoritycomposition.RemoteDialTimeoutFromEnvironment())
		assignmentClient, err = authoritycomposition.NewRemoteSnapshotClientFromEnvironment(dialCtx)
		dialCancel()
		if err != nil {
			log.Fatal(err)
		}
		defer assignmentClient.Close()
		remoteSource, ok := assignmentClient.(interface {
			FetchFullSnapshot(context.Context) (actorrouting.RouteSnapshot, error)
			WatchFrom(context.Context, uint64) (<-chan actorrouting.RouteSnapshot, error)
		})
		if !ok {
			log.Fatal("farm-worker remote assignment client does not implement snapshot source")
		}
		initialCtx, initialCancel := context.WithTimeout(processCtx, authoritycomposition.RemoteInitialFetchTimeoutFromEnvironment())
		assignment, err = authoritycomposition.NewRemoteAssignmentComposition(initialCtx, remoteSource, authoritycomposition.RemoteAssignmentOptionsFromEnvironment())
		initialCancel()
		if err != nil {
			log.Fatal(err)
		}
		if err := assignment.ConfigureEndpointConnections(authoritycomposition.EndpointConnectionOptionsFromEnvironment()); err != nil {
			log.Fatal(err)
		}
		runDone := make(chan struct{})
		go func() {
			defer close(runDone)
			if err := assignment.Run(processCtx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("farm-worker remote assignment composition terminal: %v", err)
			}
		}()
		defer func() {
			_ = assignment.Close()
			<-runDone
		}()
	}
	if assignment != nil || os.Getenv("ACTOR_ROUTE_SNAPSHOT") != "" {
		var resolver *actorrouting.StaticResolver
		var routeErr error
		if assignment != nil {
			resolver = assignment.Resolver
		} else {
			resolver, routeErr = actorrouting.LoadStaticResolver(os.Getenv("ACTOR_ROUTE_SNAPSHOT"))
			if routeErr != nil {
				log.Fatalf("load actor route snapshot: %v", routeErr)
			}
		}
		maxEndpoints := authoritycomposition.ActorRouteEndpointLimitFromEnvironment()
		if len(resolver.Endpoints()) > maxEndpoints {
			log.Fatalf("actor route snapshot has %d endpoints, exceeds ACTOR_ROUTE_MAX_ENDPOINTS=%d", len(resolver.Endpoints()), maxEndpoints)
		}
		staticRouteResolver = resolver
		clientRouteResolver := actorrouting.Resolver(resolver)
		var pooledConn grpc.ClientConnInterface
		if assignment != nil {
			clientRouteResolver, routeErr = authoritycomposition.NewPooledResolver(resolver, assignment.Endpoints)
			if routeErr == nil {
				pooledConn, routeErr = authoritycomposition.NewPooledClientConn(resolver, assignment.Endpoints)
			}
		}
		routes, routeErr := actorrouting.NewActorRouteClient(clientRouteResolver, time.Second)
		if routeErr != nil {
			log.Fatal(routeErr)
		}
		routeResolver = clientRouteResolver
		routeRealm = resolver.Realm()
		playerRecoveryClients = make(map[string]contractspb.WorkerRecoveryIngressClient)
		farmClients := make(map[string]logicrpcpb.FarmWorkflowServiceClient)
		farmRecoveryClients = make(map[string]contractspb.WorkerRecoveryIngressClient)
		if pooledConn != nil {
			playerRecoveryClients[authoritycomposition.PooledEndpoint] = contractspb.NewWorkerRecoveryIngressClient(pooledConn)
			farmClients[authoritycomposition.PooledEndpoint] = logicrpcpb.NewFarmWorkflowServiceClient(pooledConn)
			farmRecoveryClients[authoritycomposition.PooledEndpoint] = contractspb.NewWorkerRecoveryIngressClient(pooledConn)
			pooledHealthy := assignment.DataPlaneHealthy
			playerHolderConns = append(playerHolderConns, &pooledParticipantConnection{healthy: pooledHealthy})
			farmHolderConns = append(farmHolderConns, &pooledParticipantConnection{healthy: pooledHealthy})
		} else {
			for _, endpoint := range resolver.EndpointsFor(actorrouting.KindPlayer) {
				connection, dialErr := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if dialErr != nil {
					log.Fatalf("dial player holder %s: %v", endpoint, dialErr)
				}
				routeConns = append(routeConns, connection)
				playerHolderConns = append(playerHolderConns, connection)
				playerRecoveryClients[endpoint] = contractspb.NewWorkerRecoveryIngressClient(connection)
			}
			for _, endpoint := range resolver.EndpointsFor(actorrouting.KindFarm) {
				connection, dialErr := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if dialErr != nil {
					log.Fatalf("dial farm holder %s: %v", endpoint, dialErr)
				}
				routeConns = append(routeConns, connection)
				farmHolderConns = append(farmHolderConns, connection)
				farmClients[endpoint] = logicrpcpb.NewFarmWorkflowServiceClient(connection)
				farmRecoveryClients[endpoint] = contractspb.NewWorkerRecoveryIngressClient(connection)
			}
		}
		for _, connection := range routeConns {
			defer connection.Close()
		}
		farm, routeErr = workflow.NewRoutedFarmParticipant(resolver.Realm(), routes, farmClients)
		if routeErr != nil {
			log.Fatal(routeErr)
		}
		log.Printf("worker actor routing enabled with %d endpoints", len(resolver.Endpoints()))
	} else {
		farmConn, dialErr := grpc.Dial(env("FARM_ADDR", "farm-svr:9090"), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if dialErr != nil {
			log.Fatalf("dial farm-svr: %v", dialErr)
		}
		defer farmConn.Close()
		farm = farmParticipant{client: logicrpcpb.NewFarmWorkflowServiceClient(farmConn)}
	}
	hostname, _ := os.Hostname()
	workerID := env("WORKER_ID", hostname)
	if workerID == "" {
		workerID = "farm-worker"
	}
	bootID := env("WORKER_BOOT_ID", randomIdentity("boot"))
	leaseToken := env("WORKER_LEASE_TOKEN", randomIdentity("lease"))
	var workerAssignmentSource actorrouting.RevisionedAssignmentProvider
	if assignment != nil {
		workerAssignmentSource = assignment.Provider
	}
	var farmWorkerShards *farmWorkerShardRuntime
	if staticRouteResolver != nil {
		farmWorkerShards, err = buildFarmWorkerShardRuntime(
			redisClients.Authority,
			workerProfile,
			staticRouteResolver,
			workerAssignmentSource,
			workerID,
			bootID,
		)
		if err != nil {
			log.Fatalf("FARM_WORKER production shard ownership: %v", err)
		}
		farmWorkerShards.Start(processCtx)
	}
	// Shared Source DueIndex caching is intentionally disabled. Safe paging is
	// family-local: sharing physical pages would require exact client/keyspace
	// identity, independent consumption positions for all six families, bounded
	// retained bytes with lag-aware eviction, and a cache-wide command budget.
	// WorkLease's mutation-stable page API is used directly until those cache
	// invariants have a separately reviewed implementation.
	var sharedSourceDue *carerecovery.SharedDueIndexCache
	var careRecovery *carerecovery.Composition
	var harvestSourceRecovery *carerecovery.Composition
	var plantSourceRecovery *carerecovery.Composition
	var unlockSourceRecovery *carerecovery.Composition
	var theftVisitorSourceRecovery *carerecovery.Composition
	var theftGuardSourceRecovery *carerecovery.Composition
	if businessRecoveryEnabled && routeResolver != nil && len(playerRecoveryClients) != 0 {
		careRecovery, err = buildCareRecovery(redisClients.Authority, workerProfile, staticRouteResolver, routeResolver, playerRecoveryClients, farmRecoveryClients, farmWorkerShards, sharedSourceDue, workerID, leaseToken, bootID, "")
		if err != nil {
			if authorityMode == authoritycomposition.ModeAuthority {
				log.Fatalf("CareRewardOrder authority recovery required: %v", err)
			}
			log.Printf("CareRewardOrder authority recovery disabled: %v", err)
		} else {
			log.Printf("CareRewardOrder authority recovery enabled with bounded FARM bucket scans")
		}
		harvestSourceRecovery, err = buildCareRecovery(redisClients.Authority, workerProfile, staticRouteResolver, routeResolver, playerRecoveryClients, farmRecoveryClients, farmWorkerShards, sharedSourceDue, workerID, leaseToken, bootID, "HARVEST")
		if err != nil {
			log.Printf("HARVEST SourceOrder recovery disabled: %v", err)
			harvestSourceRecovery = nil
		} else {
			log.Printf("HARVEST SourceOrder recovery enabled with bounded FARM bucket scans")
		}
		if len(farmRecoveryClients) != 0 {
			plantSourceRecovery, err = buildCareRecovery(redisClients.Authority, workerProfile, staticRouteResolver, routeResolver, playerRecoveryClients, farmRecoveryClients, farmWorkerShards, sharedSourceDue, workerID, leaseToken, bootID, "PLANT")
			if err != nil {
				log.Printf("PLANT SourceOrder recovery disabled: %v", err)
				plantSourceRecovery = nil
			} else {
				log.Printf("PLANT SourceOrder recovery enabled with bounded PLAYER bucket scans")
			}
			unlockSourceRecovery, err = buildCareRecovery(redisClients.Authority, workerProfile, staticRouteResolver, routeResolver, playerRecoveryClients, farmRecoveryClients, farmWorkerShards, sharedSourceDue, workerID, leaseToken, bootID, "UNLOCK_PLOT")
			if err != nil {
				log.Printf("UNLOCK_PLOT SourceOrder recovery disabled: %v", err)
				unlockSourceRecovery = nil
			} else {
				log.Printf("UNLOCK_PLOT SourceOrder recovery enabled with bounded PLAYER bucket scans")
			}
			theftVisitorSourceRecovery, err = buildCareRecovery(redisClients.Authority, workerProfile, staticRouteResolver, routeResolver, playerRecoveryClients, farmRecoveryClients, farmWorkerShards, sharedSourceDue, workerID, leaseToken, bootID, "THEFT_VISITOR")
			if err != nil {
				log.Printf("THEFT_VISITOR SourceOrder recovery disabled: %v", err)
				theftVisitorSourceRecovery = nil
			} else {
				log.Printf("THEFT_VISITOR SourceOrder recovery enabled with bounded FARM bucket scans")
			}
			theftGuardSourceRecovery, err = buildCareRecovery(redisClients.Authority, workerProfile, staticRouteResolver, routeResolver, playerRecoveryClients, farmRecoveryClients, farmWorkerShards, sharedSourceDue, workerID, leaseToken, bootID, "THEFT_GUARD")
			if err != nil {
				log.Printf("THEFT_GUARD SourceOrder recovery disabled: %v", err)
				theftGuardSourceRecovery = nil
			} else {
				log.Printf("THEFT_GUARD SourceOrder recovery enabled with bounded FARM bucket scans")
			}
		}
	} else if businessRecoveryEnabled && authorityMode == authoritycomposition.ModeAuthority {
		log.Fatal("CareRewardOrder authority recovery required: Actor route snapshot and PLAYER Recovery clients are required")
	} else if businessRecoveryEnabled {
		log.Printf("CareRewardOrder authority recovery disabled: Actor route snapshot and PLAYER Recovery clients are required")
	}
	// SourceOrder-backed welcome mail must be recoverable in the authority
	// worker by default. Deployments may still turn it off explicitly while
	// running a holder-only canary; the authority path must not silently leave
	// a committed delivery order without a consumer.
	mailDeliveryWorkerEnabled := parseBool("MAIL_DELIVERY_WORKER_ENABLED", authorityMode == authoritycomposition.ModeAuthority)
	mailWorkerEnabled := parseBool("MAIL_WORKER_ENABLED", false)
	mailMaintenanceEnabled := parseBool("MAIL_MAINTENANCE_ENABLED", false)
	guardWorkerEnabled, err := guardSyncEnabledForDataMode(dataMode, authorityMode, os.LookupEnv)
	if err != nil {
		log.Fatal(err)
	}
	var mailRecovery *mailrecovery.Worker
	if mailWorkerEnabled {
		mailRecovery, err = buildMailRecovery(redisClients.Authority, workerProfile, staticRouteResolver, workerID, leaseToken, bootID)
		if err != nil {
			log.Fatalf("MAIL recovery explicitly enabled but unavailable: %v", err)
		}
		log.Printf("MAIL claim projection recovery enabled with bounded PLAYER bucket scans")
	}
	var mailDeliveryRecovery *maildeliveryrecovery.Worker
	if mailDeliveryWorkerEnabled {
		mailDeliveryRecovery, err = buildMailDeliveryRecovery(redisClients.Authority, workerProfile, staticRouteResolver, workerID, leaseToken, bootID)
		if err != nil {
			log.Fatalf("MAIL delivery recovery explicitly enabled but unavailable: %v", err)
		}
		log.Printf("MAIL delivery recovery enabled with bounded owned PLAYER bucket scans")
	}
	var mailMaintenance *mailmaintenance.Worker
	if mailMaintenanceEnabled {
		mailMaintenance, err = buildMailMaintenance(redisClients.Authority, workerProfile, workerID, bootID)
		if err != nil {
			log.Fatalf("MAIL maintenance explicitly enabled but unavailable: %v", err)
		}
		ownership := mailMaintenance.Ownership()
		log.Printf("MAIL expiry and receipt maintenance enabled with static shard=%d/%d owned_buckets=%d total_buckets=%d advisory_scan_lease=true",
			ownership.WorkerIndex, ownership.WorkerCount, ownership.OwnedBuckets, ownership.TotalBuckets)
	}
	var guardRecovery *guardrecovery.Worker
	if guardWorkerEnabled {
		guardRecovery, err = buildGuardRecovery(redisClients.Authority, workerProfile, staticRouteResolver, routeResolver, farmRecoveryClients, workerID, leaseToken, bootID)
		if err != nil {
			log.Fatalf("GUARD_SYNC recovery explicitly enabled but unavailable: %v", err)
		}
		log.Printf("GUARD_SYNC recovery enabled with bounded PLAYER bucket scans and FARM Participant routing")
	}
	var theftWorker *theftrecovery.Worker
	if businessRecoveryEnabled {
		log.Printf("Plant/Unlock/Harvest FirstHop recovery is SourceOrder; old {work-v1} compatibility loop is not started")
		var theftErr error
		theftWorker, theftErr = buildTheftWorker(
			redisClients.Authority,
			redisClients.Projection,
			workerProfile,
			farm,
			staticRouteResolver,
			routeResolver,
			routeRealm,
			farmRecoveryClients,
			workerID,
			bootID,
			leaseToken,
		)
		if theftErr != nil {
			log.Printf("Theft recovery fail-closed and worker readiness is false: %v", theftErr)
		} else {
			log.Printf("farm-worker durable Theft recovery enabled from Pair DueIndex")
		}
	}
	routeHolderOnly := parseBool("FARM_WORKER_ROUTE_HOLDER_ONLY", false)
	var opportunityWorkerEnabled = parseBool("FARM_OPPORTUNITY_WAKEUP_ENABLED", false)
	var opportunityWorker = (*opportunityrecovery.Worker)(nil)
	var opportunityRuntime *opportunityRecoveryRuntime
	if opportunityWorkerEnabled {
		opportunityRuntime, err = buildOpportunityRecovery(
			redisClients.Authority,
			redisClients.Runtime,
			redisClients.Projection,
			staticRouteResolver,
			farmWorkerShards,
			workerID,
			leaseToken,
			bootID,
			parseBool("FARM_OPPORTUNITY_WAKEUP_SHADOW", false),
		)
		if err != nil {
			log.Fatalf("FARM opportunity wakeup explicitly enabled but unavailable: %v", err)
		}
		defer opportunityRuntime.Close()
		opportunityWorker = opportunityRuntime.Worker
		log.Printf("FARM opportunity wakeup enabled shadow=%t transport=%s",
			parseBool("FARM_OPPORTUNITY_WAKEUP_SHADOW", false), opportunityRuntime.TransportMode)
	}
	if routeHolderOnly && (farmWorkerShards == nil || farmWorkerShards.Renewer == nil) {
		log.Fatal("FARM_WORKER_ROUTE_HOLDER_ONLY requires dynamic FARM_WORKER assignment and authority renewal")
	}

	metricsRegistry := prometheus.NewRegistry()
	metricsRegistry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	sourceordermetrics.Register(metricsRegistry)
	metricsRegistry.MustRegister(
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "farm", Subsystem: "source_order_due_cache", Name: "hits_total", Help: "Family DueIndex reads served from the shared per-bucket cache."}, func() float64 { return float64(sharedSourceDue.Stats().Hits) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "farm", Subsystem: "source_order_due_cache", Name: "misses_total", Help: "Physical DueIndex reads performed by the shared per-bucket cache."}, func() float64 { return float64(sharedSourceDue.Stats().Misses) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "farm", Subsystem: "source_order_due_cache", Name: "waits_total", Help: "Concurrent family scans joined to one in-flight physical DueIndex read."}, func() float64 { return float64(sharedSourceDue.Stats().Waits) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "farm", Subsystem: "source_order_due_cache", Name: "entries", Help: "Physical source buckets currently retained by the bounded shared cache."}, func() float64 { return float64(sharedSourceDue.Stats().Entries) }),
	)
	routeMetrics := actorrouting.RegisterRouteMetrics(metricsRegistry)
	if assignment != nil {
		assignment.Resolver.SetMetrics(routeMetrics)
		assignment.Pump.SetMetrics(routeMetrics)
		registerRemoteAssignmentMetrics(metricsRegistry, assignment)
	} else if staticRouteResolver != nil {
		staticRouteResolver.SetMetrics(routeMetrics)
	}
	if _, err := redisworkload.AttachTargetsFromEnvironment(redisworkload.DependencyTargets{
		Authority: redisClients.Authority, Runtime: redisClients.Runtime, Projection: redisClients.Projection,
	}, metricsRegistry, os.Getenv("REDIS_WORKLOAD_METRICS")); err != nil {
		log.Fatal(err)
	}
	if _, err := redisworkload.AttachDependencyHealth(metricsRegistry, redisworkload.DependencyTargets{
		Authority:  redisClients.Authority,
		Runtime:    redisClients.Runtime,
		Projection: redisClients.Projection,
	}); err != nil {
		log.Fatal(err)
	}
	claimErrors := prometheus.NewCounter(prometheus.CounterOpts{Namespace: "farm", Subsystem: "workflow", Name: "claim_error_total", Help: "Workflow due-queue claim errors."})
	claimBatch := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "farm", Subsystem: "workflow", Name: "claim_batch_size", Help: "Most recent due claim batch size."})
	backlog := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "farm", Subsystem: "workflow", Name: "backlog", Help: "Due workflows pending claim at last poll (recovery storm backlog)."})
	metricsRegistry.MustRegister(claimErrors, claimBatch, backlog)
	mailMaintenanceRounds := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "farm", Subsystem: "player_mail_maintenance", Name: "round_total",
		Help: "Bounded PlayerMail maintenance rounds.",
	})
	mailMaintenanceCandidates := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "farm", Subsystem: "player_mail_maintenance", Name: "candidate_total",
		Help: "PlayerMail expiry, mail-GC and receipt-GC candidates discovered.",
	})
	mailMaintenanceExpired := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "farm", Subsystem: "player_mail_maintenance", Name: "expired_total",
		Help: "PlayerMail visibility expiries applied.",
	})
	mailMaintenanceGC := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "farm", Subsystem: "player_mail_maintenance", Name: "gc_total",
		Help: "PlayerMail physical garbage collections applied.",
	})
	mailMaintenanceReceiptGC := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "farm", Subsystem: "player_mail_maintenance", Name: "receipt_gc_total",
		Help: "PlayerMail mutation/projection receipt garbage collections applied.",
	})
	mailMaintenanceErrors := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "farm", Subsystem: "player_mail_maintenance", Name: "error_total",
		Help: "PlayerMail maintenance rounds with an error or unknown outcome.",
	})
	mailMaintenanceDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "farm", Subsystem: "player_mail_maintenance", Name: "round_duration_seconds",
		Help:    "Duration of one bounded PlayerMail maintenance round.",
		Buckets: prometheus.DefBuckets,
	})
	mailMaintenanceOwnedBuckets := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "farm", Subsystem: "player_mail_maintenance", Name: "owned_buckets",
		Help: "Static PlayerMail buckets owned by this maintenance instance.",
	})
	mailMaintenanceBuckets := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "farm", Subsystem: "player_mail_maintenance", Name: "bucket_total",
		Help: "Owned PlayerMail buckets attempted by bounded rotating rounds.",
	})
	mailMaintenanceRoundDeadlines := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "farm", Subsystem: "player_mail_maintenance", Name: "round_deadline_total",
		Help: "PlayerMail maintenance rounds stopped by their total deadline.",
	})
	metricsRegistry.MustRegister(mailMaintenanceRounds, mailMaintenanceCandidates, mailMaintenanceExpired,
		mailMaintenanceGC, mailMaintenanceReceiptGC, mailMaintenanceErrors, mailMaintenanceDuration,
		mailMaintenanceOwnedBuckets, mailMaintenanceBuckets, mailMaintenanceRoundDeadlines)
	if mailMaintenance != nil {
		mailMaintenanceOwnedBuckets.Set(float64(mailMaintenance.Ownership().OwnedBuckets))
	}
	theftRounds := prometheus.NewCounter(prometheus.CounterOpts{Namespace: "farm", Subsystem: "theft_recovery", Name: "round_total", Help: "Dedicated Theft recovery rounds."})
	theftDiscovered := prometheus.NewCounter(prometheus.CounterOpts{Namespace: "farm", Subsystem: "theft_recovery", Name: "discovered_total", Help: "Pair DueIndex candidates discovered by Theft recovery."})
	theftCompleted := prometheus.NewCounter(prometheus.CounterOpts{Namespace: "farm", Subsystem: "theft_recovery", Name: "completed_total", Help: "Theft dispatch orders completed by recovery."})
	theftResultsCompleted := prometheus.NewCounter(prometheus.CounterOpts{Namespace: "farm", Subsystem: "theft_recovery", Name: "result_completed_total", Help: "Theft result publish orders completed and released."})
	theftRoundErrors := prometheus.NewCounter(prometheus.CounterOpts{Namespace: "farm", Subsystem: "theft_recovery", Name: "round_error_total", Help: "Dedicated Theft recovery round failures."})
	theftRoundDuration := prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "farm", Subsystem: "theft_recovery", Name: "round_duration_seconds", Help: "Dedicated Theft recovery round duration.", Buckets: prometheus.DefBuckets})
	metricsRegistry.MustRegister(theftRounds, theftDiscovered, theftCompleted, theftResultsCompleted, theftRoundErrors, theftRoundDuration)
	var opportunityMetrics *opportunityRecoveryMetrics
	if opportunityWorker != nil {
		opportunityMetrics = newOpportunityRecoveryMetrics(metricsRegistry)
		if opportunityRuntime != nil && opportunityRuntime.PubSubTransport != nil {
			friend.RegisterPubSubOpportunityHintMetrics(
				metricsRegistry,
				opportunityRuntime.PubSubTransport,
			)
		}
		if opportunityRuntime != nil && opportunityRuntime.DirectTransport != nil {
			friend.RegisterDirectOpportunityHintMetrics(metricsRegistry, opportunityRuntime.DirectTransport)
		}
		if opportunityRuntime != nil && opportunityRuntime.DynamicTransport != nil {
			friend.RegisterSwitchableOpportunityHintMetrics(
				metricsRegistry,
				opportunityRuntime.DynamicTransport,
			)
		}
	}
	freshness := parseDuration("WORKER_READINESS_MAX_SCAN_AGE_V1", 5*time.Second)
	familyConfigs := workerFamilyConfigs(
		freshness,
		businessRecoveryEnabled,
		guardWorkerEnabled,
		mailWorkerEnabled || mailMaintenanceEnabled || mailDeliveryWorkerEnabled,
	)
	if routeHolderOnly {
		for index := range familyConfigs {
			familyConfigs[index].Enabled = false
			familyConfigs[index].Critical = false
			familyConfigs[index].RequireDueProof = false
		}
	}
	familyRegistry, err := workerreadiness.NewRegistry(familyConfigs, nil)
	if err != nil {
		log.Fatal(err)
	}
	routeConfigured := routeResolver != nil
	dependencies := map[workerreadiness.Family]workerreadiness.Dependencies{
		workerreadiness.FamilyPlant:   {RepositoryInitialized: plantSourceRecovery != nil && plantSourceRecovery.Worker != nil, RouteProviderUsable: routeConfigured},
		workerreadiness.FamilyHarvest: {RepositoryInitialized: harvestSourceRecovery != nil && harvestSourceRecovery.Worker != nil, RouteProviderUsable: routeConfigured},
		// CARE has two durable sources: authority CareRewardOrder and CARE-scoped
		// Pair orders processed by the shared Theft/Pair scanner. Both must be
		// recoverable before the Family can be READY.
		workerreadiness.FamilyCare:  {RepositoryInitialized: careRecovery != nil && careRecovery.Worker != nil && theftWorker != nil, RouteProviderUsable: routeConfigured},
		workerreadiness.FamilyTheft: {RepositoryInitialized: theftWorker != nil, RouteProviderUsable: routeConfigured},
		// FRIEND_PROJECTION remains enabled and visibly unavailable, but its
		// FriendIndex repair worker is externally owned by player-svr after F1.
		// Do not alias Theft result publication into a false READY state.
		workerreadiness.FamilyFriendProjection: {},
		workerreadiness.FamilyGuardSync: {
			RepositoryInitialized:   guardRecovery != nil,
			RouteProviderUsable:     routeConfigured,
			ParticipantClientUsable: guardRecovery != nil && len(farmRecoveryClients) != 0,
		},
		workerreadiness.FamilyMail: {
			RepositoryInitialized: (!mailWorkerEnabled || mailRecovery != nil) &&
				(!mailMaintenanceEnabled || mailMaintenance != nil) &&
				(!mailDeliveryWorkerEnabled || mailDeliveryRecovery != nil),
			RouteProviderUsable: (!mailWorkerEnabled && !mailDeliveryWorkerEnabled) || staticRouteResolver != nil,
			// Mail projection recovery writes a rebuildable Redis read model and
			// maintenance also uses only bounded Redis repository calls and
			// deliberately has no Actor participant RPC.
			ParticipantClientUsable: (!mailWorkerEnabled || mailRecovery != nil) &&
				(!mailMaintenanceEnabled || mailMaintenance != nil) &&
				(!mailDeliveryWorkerEnabled || mailDeliveryRecovery != nil),
		},
	}
	for family, dependency := range dependencies {
		if err := familyRegistry.SetDependencies(family, dependency); err != nil {
			log.Fatal(err)
		}
	}
	workerMetrics, err := workerreadiness.NewMetrics(metricsRegistry, familyRegistry)
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if opportunityRuntime != nil {
		opportunityRuntime.StartModeWatch(ctx)
	}
	if !routeHolderOnly {
		pollInterval := parseDuration("WORKER_POLL_INTERVAL", time.Second)
		sourceOrderIdleMaxInterval := parseDuration("SOURCE_ORDER_WORKER_IDLE_MAX_INTERVAL", 4*time.Second)
		sourceOrderDueObserveInterval := parseDuration("SOURCE_ORDER_DUE_OBSERVE_INTERVAL", 10*time.Second)
		mailMaintenancePollInterval := time.Second
		if mailMaintenance != nil {
			var intervalErr error
			mailMaintenancePollInterval, intervalErr = mailMaintenanceEnvDuration("MAIL_MAINTENANCE_POLL_INTERVAL", time.Second)
			if intervalErr != nil {
				log.Fatal(intervalErr)
			}
		}
		dueLimit := boundedDueObservationLimit(parseInt("WORKER_DUE_OBSERVE_LIMIT", 4096))
		barriers := make([]<-chan struct{}, 0, 8)
		if businessRecoveryEnabled {
			participantMonitor, monitorErr := newParticipantConnectionMonitor(familyRegistry, dependencies, playerHolderConns, farmHolderConns)
			if monitorErr != nil {
				log.Fatal(monitorErr)
			}
			barriers = append(barriers, startParticipantConnectionMonitor(ctx, participantMonitor))
		}
		if plantSourceRecovery != nil && plantSourceRecovery.Worker != nil {
			var sweep adaptiveShardSweep
			barriers = append(barriers, startObservedLoop(ctx, pollInterval, familyRegistry, workerMetrics,
				[]workerreadiness.Family{workerreadiness.FamilyPlant}, func(scanCtx context.Context) observedRound {
					stats, scanErr := plantSourceRecovery.Worker.RunOnce(scanCtx)
					dueObs, dueErr := plantSourceRecovery.Worker.ObserveDue(scanCtx, dueLimit)
					scanErr = errors.Join(scanErr, dueErr)
					due := provenDue(dueObs.Depth, dueObs.OldestDueAtMs, dueObs.Available)
					if unlockSourceRecovery != nil && unlockSourceRecovery.Worker != nil {
						unlockStats, unlockErr := unlockSourceRecovery.Worker.RunOnce(scanCtx)
						unlockDue, unlockDueErr := unlockSourceRecovery.Worker.ObserveDue(scanCtx, dueLimit)
						scanErr = errors.Join(scanErr, unlockErr, unlockDueErr)
						if stats.Revision != 0 && unlockStats.Revision != 0 && stats.Revision != unlockStats.Revision {
							scanErr = errors.Join(scanErr, fmt.Errorf("Plant/Unlock assignment revision changed %d -> %d", stats.Revision, unlockStats.Revision))
						}
						stats.Claimed += unlockStats.Claimed
						stats.Completed += unlockStats.Completed
						stats.Rescheduled += unlockStats.Rescheduled
						stats.Unknown += unlockStats.Unknown
						stats.OwnedBuckets += unlockStats.OwnedBuckets
						stats.Buckets += unlockStats.Buckets
						stats.Scanned += unlockStats.Scanned
						stats.Candidates += unlockStats.Candidates
						if unlockStats.Revision > stats.Revision {
							stats.Revision = unlockStats.Revision
						}
						due = mergeDue(due, provenDue(unlockDue.Depth, unlockDue.OldestDueAtMs, unlockDue.Available))
					}
					scanErr = suppressExpectedOnly(scanErr, carerecovery.ErrRetryDeferred)
					claimBatch.Set(float64(stats.Claimed))
					if scanErr != nil {
						claimErrors.Inc()
						log.Printf("recover Plant/Unlock SourceOrder: %v", scanErr)
					}
					return observedRound{stats: map[workerreadiness.Family]workerreadiness.ScanStats{
						workerreadiness.FamilyPlant: {OwnedBuckets: stats.OwnedBuckets, AssignmentRevision: stats.Revision, BucketsScanned: stats.Buckets, Claimed: stats.Claimed, Completed: stats.Completed, Retried: stats.Rescheduled, StepUnknown: stats.Unknown},
					}, due: map[workerreadiness.Family]workerreadiness.DueObservation{workerreadiness.FamilyPlant: due}, err: scanErr,
						nextDelay: sweep.next(pollInterval, sourceOrderIdleMaxInterval, stats.Revision, stats.OwnedBuckets, stats.Buckets, stats.Candidates, scanErr)}
				}))
		}
		if harvestSourceRecovery != nil && harvestSourceRecovery.Worker != nil {
			var sweep adaptiveShardSweep
			barriers = append(barriers, startObservedLoop(ctx, pollInterval, familyRegistry, workerMetrics,
				[]workerreadiness.Family{workerreadiness.FamilyHarvest}, func(scanCtx context.Context) observedRound {
					stats, scanErr := harvestSourceRecovery.Worker.RunOnce(scanCtx)
					dueObs, dueErr := harvestSourceRecovery.Worker.ObserveDue(scanCtx, dueLimit)
					scanErr = errors.Join(scanErr, dueErr)
					scanErr = suppressExpectedOnly(scanErr, carerecovery.ErrRetryDeferred)
					claimBatch.Set(float64(stats.Claimed))
					if scanErr != nil {
						claimErrors.Inc()
						log.Printf("recover Harvest SourceOrder: %v", scanErr)
					}
					return observedRound{stats: map[workerreadiness.Family]workerreadiness.ScanStats{
						workerreadiness.FamilyHarvest: {OwnedBuckets: stats.OwnedBuckets, AssignmentRevision: stats.Revision, BucketsScanned: stats.Buckets, Claimed: stats.Claimed, Completed: stats.Completed, Retried: stats.Rescheduled, StepUnknown: stats.Unknown},
					}, due: map[workerreadiness.Family]workerreadiness.DueObservation{
						workerreadiness.FamilyHarvest: provenDue(dueObs.Depth, dueObs.OldestDueAtMs, dueObs.Available),
					}, err: scanErr, nextDelay: sweep.next(pollInterval, sourceOrderIdleMaxInterval, stats.Revision, stats.OwnedBuckets, stats.Buckets, stats.Candidates, scanErr)}
				}))
		}
		if careRecovery != nil && careRecovery.Worker != nil {
			var sweep adaptiveShardSweep
			barriers = append(barriers, startObservedLoop(ctx, pollInterval, familyRegistry, workerMetrics,
				[]workerreadiness.Family{workerreadiness.FamilyCare}, func(scanCtx context.Context) observedRound {
					stats, scanErr := careRecovery.Worker.RunOnce(scanCtx)
					authorityDue, dueErr := careRecovery.Worker.ObserveDue(scanCtx, dueLimit)
					scanErr = errors.Join(scanErr, dueErr)
					due := provenDue(authorityDue.Depth, authorityDue.OldestDueAtMs, authorityDue.Available)
					if theftWorker == nil {
						scanErr = errors.Join(scanErr, errPairCareRecoveryNotReady)
						due = workerreadiness.DueObservation{}
					} else {
						pairDue, pairDueErr := theftWorker.ObserveDue(scanCtx, dueLimit)
						scanErr = errors.Join(scanErr, pairDueErr)
						due = mergeDue(due, provenDue(pairDue.Care.Depth, pairDue.Care.OldestDueAtMs, pairDue.Available))
						if familyState(familyRegistry.Snapshot(), workerreadiness.FamilyTheft) != workerreadiness.StateReady {
							scanErr = errors.Join(scanErr, errPairCareRecoveryNotReady)
						}
					}
					scanErr = suppressExpectedOnly(scanErr, carerecovery.ErrRetryDeferred)
					claimBatch.Set(float64(stats.Claimed))
					if scanErr != nil {
						claimErrors.Inc()
						log.Printf("recover CareRewardOrder: %v", scanErr)
					}
					if stats.Claimed != 0 || stats.Completed != 0 || stats.Rescheduled != 0 {
						log.Printf("recovery round care=%+v", stats)
					}
					return observedRound{stats: map[workerreadiness.Family]workerreadiness.ScanStats{workerreadiness.FamilyCare: {
						OwnedBuckets: stats.OwnedBuckets, AssignmentRevision: stats.Revision, BucketsScanned: stats.Buckets,
						Claimed: stats.Claimed, Completed: stats.Completed, Retried: stats.Rescheduled, StepUnknown: stats.Unknown,
					}}, due: map[workerreadiness.Family]workerreadiness.DueObservation{workerreadiness.FamilyCare: due}, err: scanErr,
						nextDelay: sweep.next(pollInterval, sourceOrderIdleMaxInterval, stats.Revision, stats.OwnedBuckets, stats.Buckets, stats.Candidates, scanErr)}
				}))
		}
		if theftVisitorSourceRecovery != nil && theftVisitorSourceRecovery.Worker != nil {
			go runSourceOrderRecoveryLoop(ctx, pollInterval, sourceOrderIdleMaxInterval, "THEFT_VISITOR", theftVisitorSourceRecovery.Worker)
		}
		if theftGuardSourceRecovery != nil && theftGuardSourceRecovery.Worker != nil {
			go runSourceOrderRecoveryLoop(ctx, pollInterval, sourceOrderIdleMaxInterval, "THEFT_GUARD", theftGuardSourceRecovery.Worker)
		}
		if mailRecovery != nil || mailMaintenance != nil || mailDeliveryRecovery != nil {
			mailSources := make([]mailObservedSource, 0, 3)
			if mailRecovery != nil {
				mailSources = append(mailSources, mailObservedSource{
					name: "claim_projection", interval: pollInterval, staleAfter: freshness,
					scan: func(scanCtx context.Context) observedRound {
						stats, recoveryErr := mailRecovery.RunOnce(scanCtx)
						recoveryDue, dueErr := mailRecovery.ObserveDue(scanCtx, dueLimit)
						scanErr := errors.Join(recoveryErr, dueErr)
						readinessStats := workerreadiness.ScanStats{
							Claimed: stats.Claimed, Completed: stats.Completed,
							Retried: stats.Rescheduled, StepUnknown: stats.Unknown,
						}
						claimBatch.Set(float64(readinessStats.Claimed))
						if scanErr != nil {
							claimErrors.Inc()
							log.Printf("recover MAIL claim projections: %v", scanErr)
						}
						if stats.Claimed != 0 || stats.Completed != 0 || stats.Rescheduled != 0 {
							log.Printf("recovery round mail=%+v", stats)
						}
						return observedRound{stats: map[workerreadiness.Family]workerreadiness.ScanStats{
							workerreadiness.FamilyMail: readinessStats,
						}, due: map[workerreadiness.Family]workerreadiness.DueObservation{
							workerreadiness.FamilyMail: provenDue(recoveryDue.Depth, recoveryDue.OldestDueAtMs, recoveryDue.Available),
						}, err: scanErr}
					},
				})
			}
			if mailMaintenance != nil {
				mailSources = append(mailSources, mailObservedSource{
					name: "maintenance", interval: mailMaintenancePollInterval, staleAfter: freshness,
					scan: func(scanCtx context.Context) observedRound {
						started := time.Now()
						stats, scanErr := mailMaintenance.RunOnce(scanCtx)
						mailMaintenanceRounds.Inc()
						mailMaintenanceBuckets.Add(float64(stats.Buckets))
						mailMaintenanceCandidates.Add(float64(stats.Candidates()))
						mailMaintenanceExpired.Add(float64(stats.Expired))
						mailMaintenanceGC.Add(float64(stats.GcPurged))
						mailMaintenanceReceiptGC.Add(float64(stats.ReceiptPurged))
						mailMaintenanceDuration.Observe(time.Since(started).Seconds())
						if stats.DeadlineExceeded {
							mailMaintenanceRoundDeadlines.Inc()
						}
						if scanErr != nil {
							mailMaintenanceErrors.Inc()
							claimErrors.Inc()
							log.Printf("maintain MAIL expiry/GC: %v", scanErr)
						}
						readinessStats := workerreadiness.ScanStats{
							Claimed: stats.Candidates(), Completed: stats.Completed(),
							Retried: stats.Retried, StepUnknown: stats.Unknown,
						}
						claimBatch.Set(float64(readinessStats.Claimed))
						if stats.Candidates() != 0 || stats.Completed() != 0 || stats.Retried != 0 || stats.Failed != 0 {
							log.Printf("maintenance round mail=%+v", stats)
						}
						return observedRound{stats: map[workerreadiness.Family]workerreadiness.ScanStats{
							workerreadiness.FamilyMail: readinessStats,
						}, due: map[workerreadiness.Family]workerreadiness.DueObservation{
							workerreadiness.FamilyMail: provenDue(stats.Due.Depth, stats.Due.OldestDueAtMs, stats.Due.Available),
						}, err: scanErr}
					},
				})
			}
			if mailDeliveryRecovery != nil {
				mailSources = append(mailSources, mailObservedSource{
					name: "delivery", interval: pollInterval, staleAfter: freshness,
					scan: func(scanCtx context.Context) observedRound {
						stats, recoveryErr := mailDeliveryRecovery.RunOnce(scanCtx)
						recoveryDue, dueErr := mailDeliveryRecovery.ObserveDue(scanCtx, dueLimit)
						scanErr := errors.Join(recoveryErr, dueErr)
						readinessStats := workerreadiness.ScanStats{
							Claimed: stats.Claimed, Completed: stats.Completed,
							Retried: stats.Rescheduled, StepUnknown: stats.Unknown,
						}
						claimBatch.Set(float64(readinessStats.Claimed))
						if scanErr != nil {
							claimErrors.Inc()
							log.Printf("recover MAIL delivery: %v", scanErr)
						}
						if stats.Claimed != 0 || stats.Completed != 0 || stats.Rescheduled != 0 {
							log.Printf("recovery round mail_delivery=%+v", stats)
						}
						return observedRound{stats: map[workerreadiness.Family]workerreadiness.ScanStats{
							workerreadiness.FamilyMail: readinessStats,
						}, due: map[workerreadiness.Family]workerreadiness.DueObservation{
							workerreadiness.FamilyMail: provenDue(recoveryDue.Depth, recoveryDue.OldestDueAtMs, recoveryDue.Available),
						}, err: scanErr}
					},
				})
			}
			barriers = append(barriers, startMailObservedLoops(ctx, familyRegistry, workerMetrics, mailSources))
		}
		if guardRecovery != nil {
			barriers = append(barriers, startObservedLoop(ctx, pollInterval, familyRegistry, workerMetrics,
				[]workerreadiness.Family{workerreadiness.FamilyGuardSync}, func(scanCtx context.Context) observedRound {
					stats, scanErr := guardRecovery.RunOnce(scanCtx)
					due, dueErr := guardRecovery.ObserveDue(scanCtx, dueLimit)
					scanErr = errors.Join(scanErr, dueErr)
					claimBatch.Set(float64(stats.Claimed))
					if scanErr != nil {
						claimErrors.Inc()
						log.Printf("recover GUARD_SYNC orders: %v", scanErr)
					}
					if stats.Claimed != 0 || stats.Completed != 0 || stats.Rescheduled != 0 {
						log.Printf("recovery round guard_sync=%+v", stats)
					}
					return observedRound{stats: map[workerreadiness.Family]workerreadiness.ScanStats{
						workerreadiness.FamilyGuardSync: {Claimed: stats.Claimed, Completed: stats.Completed, Retried: stats.Rescheduled, StepUnknown: stats.Unknown},
					}, due: map[workerreadiness.Family]workerreadiness.DueObservation{
						workerreadiness.FamilyGuardSync: provenDue(due.Depth, due.OldestDueAtMs, due.Available),
					}, err: scanErr}
				}))
		}
		if theftWorker != nil {
			theftPollInterval := parseDuration("THEFT_WORKER_POLL_INTERVAL", 100*time.Millisecond)
			theftIdleMaxInterval := parseDuration("THEFT_WORKER_IDLE_MAX_INTERVAL", 500*time.Millisecond)
			emptyPairSweeps := 0
			barriers = append(barriers, startObservedLoop(ctx, theftPollInterval, familyRegistry, workerMetrics,
				[]workerreadiness.Family{workerreadiness.FamilyTheft}, func(scanCtx context.Context) observedRound {
					started := time.Now()
					stats, scanErr := theftWorker.RunOnce(scanCtx)
					due, dueErr := theftWorker.ObserveDue(scanCtx, dueLimit)
					scanErr = errors.Join(scanErr, dueErr)
					theftRoundDuration.Observe(time.Since(started).Seconds())
					theftRounds.Inc()
					theftDiscovered.Add(float64(stats.Discovered))
					theftCompleted.Add(float64(stats.Completed))
					theftResultsCompleted.Add(float64(stats.ResultsCompleted))
					workerMetrics.ObserveCounts(workerreadiness.FamilyCare, workerreadiness.ScanStats{Completed: uint64(stats.CareCompleted)})
					if scanErr != nil {
						theftRoundErrors.Inc()
						log.Printf("recover Theft workflows: %v", scanErr)
					}
					if stats.Claimed != 0 || stats.Completed != 0 || stats.ResultsCompleted != 0 || stats.CareCompleted != 0 || stats.Retried != 0 || stats.Reconciled != 0 || stats.Errors != 0 {
						log.Printf("recovery round theft=%+v", stats)
					}
					nextDelay := time.Duration(0)
					if stats.SweepExhausted {
						if stats.Discovered == 0 && stats.Errors == 0 {
							emptyPairSweeps++
							nextDelay = emptySweepBackoff(theftPollInterval, theftIdleMaxInterval, emptyPairSweeps)
						} else {
							emptyPairSweeps = 0
						}
					} else if stats.Discovered != 0 || stats.Errors != 0 {
						emptyPairSweeps = 0
					}
					return observedRound{stats: map[workerreadiness.Family]workerreadiness.ScanStats{
						workerreadiness.FamilyTheft: {Claimed: uint64(stats.Claimed), Completed: uint64(stats.Completed + stats.ResultsCompleted), Retried: uint64(stats.Retried), StepUnknown: uint64(stats.StepUnknown)},
					}, due: map[workerreadiness.Family]workerreadiness.DueObservation{
						workerreadiness.FamilyTheft: provenDue(due.Theft.Depth, due.Theft.OldestDueAtMs, due.Available),
					}, terminalLatencies: map[workerreadiness.Family][]time.Duration{workerreadiness.FamilyTheft: stats.TerminalLatencies}, err: scanErr,
						nextDelay: nextDelay}
				}))
		}
		if opportunityWorker != nil {
			barriers = append(barriers, startOpportunityRecoveryLoop(ctx,
				opportunityWorker, opportunityRuntime.Assignment,
				parseDuration("FARM_OPPORTUNITY_WAKEUP_POLL_INTERVAL", time.Second),
				parseDuration("FARM_OPPORTUNITY_WAKEUP_IDLE_MAX_INTERVAL", 5*time.Second), opportunityMetrics))
		}
		if businessRecoveryEnabled {
			go runBacklogObservationLoop(ctx, sourceOrderDueObserveInterval, careRecovery, backlog, dueLimit)
		} else {
			backlog.Set(0)
		}
		waitLoopStarts(barriers...)
	} else {
		log.Printf("farm-worker route-holder-only mode: business recovery loops are disabled")
	}

	var routeProjectionReady func() bool
	if assignment != nil {
		routeProjectionReady = func() bool {
			return assignment.DataPlaneHealthy() &&
				(farmWorkerShards == nil ||
					farmWorkerShards.Renewer == nil ||
					farmWorkerShards.Renewer.Healthy()) &&
				(opportunityRuntime == nil || opportunityRuntime.ModeHealthy())
		}
	}
	health := newHealthServer(env("WORKER_HEALTH_LISTEN", ":9201"), metricsRegistry, familyRegistry, routeProjectionReady)
	go func() {
		if err := health.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("worker health: %v", err)
		}
	}()
	log.Printf("farm-worker running, worker_id=%s boot_id=%s data_mode=%s business_recovery=%t version=%s readiness=%s", workerID, bootID, dataMode, businessRecoveryEnabled, farmgame.FullVersion(), workerreadiness.ContractVersion)
	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = health.Shutdown(shutdownCtx)
	log.Println("farm-worker stopped")
}

func workerFamilyConfigs(freshness time.Duration, businessRecoveryEnabled, guardEnabled, mailEnabled bool) []workerreadiness.FamilyConfig {
	configs := make([]workerreadiness.FamilyConfig, 0, workerreadiness.MaxFamilies)
	for _, family := range workerreadiness.AllFamilies() {
		enabled := true
		critical := true
		switch family {
		case workerreadiness.FamilyPlant, workerreadiness.FamilyHarvest, workerreadiness.FamilyCare, workerreadiness.FamilyTheft:
			enabled = businessRecoveryEnabled
			critical = enabled
		case workerreadiness.FamilyFriendProjection:
			critical = false
		case workerreadiness.FamilyGuardSync:
			enabled = guardEnabled
			critical = enabled
		case workerreadiness.FamilyMail:
			enabled = mailEnabled
			critical = enabled
		}
		configs = append(configs, workerreadiness.FamilyConfig{Family: family, Enabled: enabled, Critical: critical,
			FreshnessThreshold: freshness, RequireDueProof: enabled && critical})
	}
	return configs
}

func boundedDueObservationLimit(limit int) int {
	if limit <= 0 {
		return 4096
	}
	if limit > 4096 {
		return 4096
	}
	return limit
}

func workflowReadinessStats(values ...workflow.RecoveryStats) workerreadiness.ScanStats {
	var result workerreadiness.ScanStats
	for _, value := range values {
		result.Claimed += uint64(value.Claimed)
		result.Completed += uint64(value.Completed)
		result.Retried += uint64(value.Retried)
	}
	return result
}

func workflowTerminalLatencies(values ...workflow.RecoveryStats) []time.Duration {
	var result []time.Duration
	for _, value := range values {
		result = append(result, value.TerminalLatencies...)
	}
	return result
}

func familyState(snapshot workerreadiness.Snapshot, family workerreadiness.Family) workerreadiness.State {
	for _, item := range snapshot.Families {
		if item.Family == family {
			return item.State
		}
	}
	return workerreadiness.StateFailed
}

func runBacklogObservationLoop(ctx context.Context, interval time.Duration,
	care *carerecovery.Composition, backlog prometheus.Gauge, limit int) {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if care == nil {
				backlog.Set(0)
				timer.Reset(interval)
				continue
			}
			observation, observationErr := care.Worker.ObserveDue(ctx, limit)
			if observationErr != nil || !observation.Available {
				backlog.Set(-1)
			} else {
				backlog.Set(float64(observation.Depth))
			}
			timer.Reset(interval)
		}
	}
}

func buildTheftWorker(
	authorityClient, projectionClient redis.UniversalClient,
	profile rediskey.PhysicalProfile,
	farmInspector carerecovery.PairCareFarmInspector,
	resolver *actorrouting.StaticResolver,
	routeResolver actorrouting.Resolver,
	realm string,
	farmClients map[string]contractspb.WorkerRecoveryIngressClient,
	workerID, bootID, leaseToken string,
) (*theftrecovery.Worker, error) {
	if authorityClient == nil || projectionClient == nil || farmInspector == nil || resolver == nil || routeResolver == nil ||
		strings.TrimSpace(realm) == "" || len(farmClients) == 0 || strings.TrimSpace(workerID) == "" ||
		strings.TrimSpace(bootID) == "" || strings.TrimSpace(leaseToken) == "" {
		return nil, errors.New("THEFT worker requires Pair Redis, Farm recovery clients, static route snapshot and worker identity")
	}
	if err := profile.Validate(); err != nil {
		return nil, fmt.Errorf("THEFT worker Redis keyspace tuple: %w", err)
	}
	namespace := profile.Namespace
	pairRepositories, err := friend.NewRedisFriendshipRepositoriesForNamespace(authorityClient, namespace)
	if err != nil {
		return nil, fmt.Errorf("THEFT worker Pair repositories: %w", err)
	}
	loadCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pairRepositories.EnsureScriptsLoaded(loadCtx); err != nil {
		return nil, fmt.Errorf("THEFT worker Pair scripts: %w", err)
	}
	targetStore, err := theftonline.NewRedisTargetBindingStore(projectionClient,
		env("THEFT_TARGET_BINDING_PREFIX", "farm:v121:m1:theft-target"),
		parseDuration("THEFT_TARGET_BINDING_TTL", 7*24*time.Hour))
	if err != nil {
		return nil, fmt.Errorf("THEFT worker target binding store: %w", err)
	}
	resultPublisher, err := theftonline.NewRedisResultPublisher(projectionClient, namespace)
	if err != nil {
		return nil, fmt.Errorf("THEFT worker ResultPublisher: %w", err)
	}
	rawPairDispatch, err := theftrecovery.NewFriendPairDispatchRepository(theftrecovery.FriendPairDispatchRepositoryConfig{
		Repository: pairRepositories, TargetDecode: theftonline.NewFriendPairTargetDecoder(targetStore),
		MaxBuckets: parseInt("THEFT_PAIR_DISCOVERY_MAX_BUCKETS", 128),
	})
	if err != nil {
		return nil, fmt.Errorf("THEFT worker Pair DueIndex: %w", err)
	}
	pairDispatch := &attributedPairDispatchRepository{FriendPairDispatchRepository: rawPairDispatch}
	leaseTTL := parseDuration("THEFT_WORK_LEASE", 30*time.Second)
	rawPairLeases, err := theftrecovery.NewFriendPairWorkLeaseRepository(pairRepositories,
		parseDuration("THEFT_WORK_LEASE_MIN", 100*time.Millisecond),
		parseDuration("THEFT_WORK_LEASE_MAX", 2*time.Minute))
	if err != nil {
		return nil, fmt.Errorf("THEFT worker Pair WorkLease: %w", err)
	}
	pairLeases := &attributedPairWorkLease{RedisWorkLeaseRepository: rawPairLeases}
	snapshot := resolver.Snapshot()
	calculator, err := actorrouting.NewCalculator(snapshot.Realm,
		actorrouting.HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed},
		snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		return nil, fmt.Errorf("THEFT worker route calculator: %w", err)
	}
	sourceOrderStore, err := authority.NewStore(authorityClient, nil, authority.Options{
		Profile:  profile,
		Resolver: calculator,
	})
	if err != nil {
		return nil, fmt.Errorf("THEFT worker source-order reader: %w", err)
	}
	farmRoute, err := carerecovery.NewFarmRouteProvider(routeResolver, realm)
	if err != nil {
		return nil, fmt.Errorf("THEFT worker Farm route: %w", err)
	}
	farmRPC, err := theftonline.NewRoutedFarmRecoveryClient(routeResolver, realm, theftRecoveryClients(farmClients))
	if err != nil {
		return nil, fmt.Errorf("THEFT worker Farm recovery client: %w", err)
	}
	var resultProcessor *theftonline.ResultOrderProcessor
	advancer, err := theftonline.NewSourceOrderAdvancer(theftonline.SourceOrderAdvancerConfig{
		SourceCommit: theftonline.NewRecoverySourceCommit(theftonline.RecoverySourceCommitConfig{
			Farm:  farmRPC,
			Route: theftonline.FarmRouteResolver(farmRoute),
			BucketFor: func(farmID uint64) (rediskey.AuthorityBucketRef, error) {
				return profile.AuthorityBucketForActor(calculator, actorrouting.ActorAddress{
					Domain: actorrouting.DomainFarm, ActorID: farmID,
				})
			},
		}),
		FinalizeNoChange: func(ctx context.Context, order theftrecovery.DispatchOrder) error {
			if resultProcessor == nil {
				return fmt.Errorf("THEFT worker result-order finalizer is unavailable")
			}
			return resultProcessor.FinalizeNoChange(ctx, order)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("THEFT worker source-order advancer: %w", err)
	}
	resultProcessor, err = theftonline.NewResultOrderProcessor(pairRepositories, resultPublisher, sourceOrderStore)
	if err != nil {
		return nil, fmt.Errorf("THEFT worker result-order consumer: %w", err)
	}
	careProcessor, err := carerecovery.NewPairCareProcessor(carerecovery.PairCareProcessorConfig{
		Pair: pairRepositories, Dispatch: rawPairDispatch, Leases: pairLeases, Farm: farmInspector,
		HolderID: workerID, BootID: bootID, LeaseToken: leaseToken, LeaseTTL: leaseTTL,
	})
	if err != nil {
		return nil, fmt.Errorf("CARE Pair result-order consumer: %w", err)
	}
	worker, err := theftrecovery.New(theftrecovery.Config{
		PairRepository: pairDispatch, Leases: pairLeases, Advancer: advancer,
		ResultProcessor: resultProcessor, CareProcessor: careProcessor,
		HolderID: workerID, BootID: bootID, LeaseToken: leaseToken,
		ClaimLimit:     parseInt("THEFT_CLAIM_LIMIT", theftrecovery.DefaultClaimLimit),
		DiscoveryLimit: parseInt("THEFT_DISCOVERY_LIMIT", theftrecovery.DefaultDiscoveryLimit),
		LeaseTTL:       leaseTTL, OperationTimeout: parseDuration("THEFT_OPERATION_TIMEOUT", 3*time.Second),
		MaxAttempts: theftrecovery.DefaultMaxAttempts, Now: time.Now,
		DueObservationCacheTTL: parseDuration("THEFT_DUE_OBSERVE_INTERVAL", 10*time.Second),
	})
	if err != nil {
		return nil, fmt.Errorf("THEFT worker: %w", err)
	}
	return worker, nil
}

func requiredRemoteKeyspaceTuple(membership authoritycomposition.MembershipMode, assignment authoritycomposition.AssignmentMode) (*rediskey.KeyspaceTuple, error) {
	if !authoritycomposition.HolderCompositionEnabled(membership, assignment) {
		return nil, nil
	}
	tuple, err := rediskey.ParseKeyspaceTupleEnvironment(os.Getenv)
	if err != nil {
		return nil, fmt.Errorf("farm-worker remote Redis keyspace tuple: %w", err)
	}
	fingerprint, err := tuple.Fingerprint()
	if err != nil {
		return nil, fmt.Errorf("farm-worker remote Redis keyspace fingerprint: %w", err)
	}
	log.Printf("farm-worker Redis keyspace fingerprint=%s", fingerprint)
	return &tuple, nil
}

func workerPhysicalProfile(keyspaceTuple *rediskey.KeyspaceTuple) (rediskey.PhysicalProfile, error) {
	if keyspaceTuple != nil {
		profile := keyspaceTuple.Profile()
		return profile, profile.Validate()
	}
	namespace, err := rediskey.NewNamespace(env("REDIS_NAMESPACE_ENVIRONMENT", "dev"), env("REDIS_NAMESPACE_ZONE", "z1"))
	if err != nil {
		return rediskey.PhysicalProfile{}, err
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: env("REDIS_PHYSICAL_PROFILE_VERSION", "redis-physical-v1"), Namespace: namespace,
		AuthorityLayoutVersion: env("REDIS_AUTHORITY_LAYOUT_VERSION", "v1"),
		WorkflowLayoutVersion:  env("REDIS_WORKFLOW_LAYOUT_VERSION", "v1"),
		WorkflowHashVersion:    uint32(parseInt("REDIS_WORKFLOW_HASH_VERSION", 1)),
		WorkflowBucketCount:    uint32(parseInt("REDIS_WORKFLOW_BUCKET_COUNT", 256)),
	}
	return profile, profile.Validate()
}

func registerRemoteAssignmentMetrics(
	registerer prometheus.Registerer,
	assignment *authoritycomposition.AssignmentComposition,
) {
	if registerer == nil || assignment == nil || assignment.Pump == nil {
		return
	}
	registerer.MustRegister(
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm",
			Subsystem: "actor_route",
			Name:      "assignment_reconnects_total",
			Help:      "Remote route Watch reconnects observed by this process.",
		}, func() float64 {
			return float64(assignment.Pump.Reconnects.Load())
		}),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Namespace: "farm",
			Subsystem: "actor_route",
			Name:      "assignment_resyncs_total",
			Help:      "Remote route full resyncs observed by this process.",
		}, func() float64 {
			return float64(assignment.Pump.Resyncs.Load())
		}),
	)
}

func newHealthServer(addr string, metricsRegistry *prometheus.Registry, registry *workerreadiness.Registry, routeReady ...func() bool) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		snapshot := workerreadiness.Snapshot{Version: workerreadiness.ContractVersion, Ready: true, State: workerreadiness.StateReady}
		if registry != nil {
			snapshot = registry.Snapshot()
		}
		routeRequired := len(routeReady) > 0
		routeFresh := true
		for _, check := range routeReady {
			if check == nil || !check() {
				routeFresh = false
			}
		}
		if !routeFresh {
			snapshot.Ready = false
			snapshot.State = workerreadiness.StateFailed
		}
		if !snapshot.Ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(struct {
			workerreadiness.Snapshot
			RouteProjectionRequired bool `json:"route_projection_required"`
			RouteProjectionFresh    bool `json:"route_projection_fresh"`
		}{Snapshot: snapshot, RouteProjectionRequired: routeRequired, RouteProjectionFresh: routeFresh})
	})
	mux.Handle("/metrics", promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{}))
	if os.Getenv("WORKER_PPROF_ENABLED") == "1" {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func scheduleDueIndexModeFromEnvironment() (authority.ScheduleDueIndexMode, bool, error) {
	mode, err := authority.ParseScheduleDueIndexMode(os.Getenv("FARM_SCHEDULE_DUE_INDEX_MODE"))
	if err != nil {
		return "", false, fmt.Errorf("FARM_SCHEDULE_DUE_INDEX_MODE: %w", err)
	}
	rawReady := strings.ToLower(strings.TrimSpace(os.Getenv("FARM_SCHEDULE_DUE_INDEX_DEDICATED_READY")))
	ready := rawReady == "1" || rawReady == "true" || rawReady == "yes" || rawReady == "on"
	if rawReady != "" && !ready && rawReady != "0" && rawReady != "false" && rawReady != "no" && rawReady != "off" {
		return "", false, fmt.Errorf("FARM_SCHEDULE_DUE_INDEX_DEDICATED_READY must be boolean")
	}
	return mode, ready, nil
}

func parseDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func parseInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed := 0
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

// guardSyncEnabled 决定是否启动守卫同步 worker。
//
// 这里默认开是有历史的:GUARD_SYNC 最初默认关,因为那时它根本没有实现
// (r2-w1 worker readiness handoff 的 readiness 表里写的是 "No implementation
// at this base")。实现补齐之后默认值没跟着改,后果是宠物防卫在生产上静默
// 失效——玩家喂了狗、Player 侧 guard_until 也涨了,但 GuardSyncOrder 没有
// 消费者,FarmActor 永远不知道这只狗存在,偷菜照常成功。防卫是宠物唯一的
// 用途,静默失效比起不来更糟。
//
// Authority 模式下它与 CareReward 同级:属于必需恢复链,依赖缺失时
// buildGuardRecovery 会 fail-closed 而不是悄悄跳过。非 Authority 模式没有
// 这条链的依赖,保持关闭。运维显式设置环境变量始终优先。
func guardSyncEnabled(mode authoritycomposition.Mode) bool {
	return parseBool("GUARD_SYNC_WORKER_ENABLED", mode == authoritycomposition.ModeAuthority)
}

func guardSyncEnabledForDataMode(dataMode datamode.Mode, mode authoritycomposition.Mode, lookup func(string) (string, bool)) (bool, error) {
	if !dataMode.DirtyWritebackEnabled() {
		return guardSyncEnabled(mode), nil
	}
	if lookup != nil {
		if raw, present := lookup("GUARD_SYNC_WORKER_ENABLED"); present {
			switch strings.ToLower(strings.TrimSpace(raw)) {
			case "1", "true", "yes", "on":
				return false, fmt.Errorf("GUARD_SYNC_WORKER_ENABLED cannot be enabled when %s=%s: dirty mode does not create durable GuardSyncOrder", datamode.EnvKey, dataMode)
			case "", "0", "false", "no", "off":
			default:
				return false, fmt.Errorf("GUARD_SYNC_WORKER_ENABLED must be boolean")
			}
		}
	}
	return false, nil
}

func parseBool(key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	switch value {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	case "":
		return fallback
	default:
		return fallback
	}
}

func randomIdentity(prefix string) string {
	bytes := make([]byte, 16)
	if _, err := cryptorand.Read(bytes); err != nil {
		log.Fatalf("generate %s identity: %v", prefix, err)
	}
	return prefix + "-" + hex.EncodeToString(bytes)
}

// buildMailDeliveryRecovery composes the frozen MailEntry delivery recovery
// lane. It only reads PLAYER Authority SourceOrders and writes PlayerMail;
// it never activates PlayerActor.
func buildMailDeliveryRecovery(redisClient redis.UniversalClient, profile rediskey.PhysicalProfile, staticResolver *actorrouting.StaticResolver, owner, token, bootID string) (*maildeliveryrecovery.Worker, error) {
	if redisClient == nil || staticResolver == nil {
		return nil, fmt.Errorf("mail delivery recovery requires Authority Redis and an Actor route snapshot")
	}
	snapshot := staticResolver.Snapshot()
	calculator, err := actorrouting.NewCalculator(snapshot.Realm,
		actorrouting.HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed},
		snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		return nil, fmt.Errorf("mail delivery recovery route calculator: %w", err)
	}
	if err := profile.Validate(); err != nil {
		return nil, fmt.Errorf("mail delivery recovery Redis keyspace tuple: %w", err)
	}
	scheduleDueIndexMode, scheduleDueIndexReady, err := scheduleDueIndexModeFromEnvironment()
	if err != nil {
		return nil, err
	}
	authorityStore, err := authority.NewStore(redisClient, nil, authority.Options{
		Profile: profile, Resolver: calculator, ScheduleDueIndexMode: scheduleDueIndexMode,
		ScheduleDueIndexDedicatedReady: scheduleDueIndexReady,
	})
	if err != nil {
		return nil, fmt.Errorf("mail delivery recovery authority store: %w", err)
	}
	playerShards, err := calculator.ShardCount(actorrouting.DomainPlayer)
	if err != nil {
		return nil, fmt.Errorf("mail delivery recovery PLAYER shard count: %w", err)
	}
	buckets := make([]rediskey.AuthorityBucketRef, 0, playerShards)
	for shard := uint32(0); shard < playerShards; shard++ {
		bucket, bucketErr := profile.AuthorityBucketForShard(calculator, actorrouting.DomainPlayer, shard)
		if bucketErr != nil {
			return nil, fmt.Errorf("mail delivery recovery PLAYER bucket %d: %w", shard, bucketErr)
		}
		buckets = append(buckets, bucket)
	}
	ownedBuckets, err := mailDeliveryOwnedBuckets(buckets)
	if err != nil {
		return nil, err
	}
	mailBucketCount := parseInt("PLAYER_MAIL_BUCKET_COUNT", int(redisplayermail.DefaultBucketCount))
	if mailBucketCount <= 0 {
		return nil, fmt.Errorf("mail delivery recovery PLAYER_MAIL_BUCKET_COUNT must be positive")
	}
	mailStore, err := redisplayermail.NewStore(redisClient, redisplayermail.Profile{
		Namespace: profile.Namespace, BucketCount: uint32(mailBucketCount),
	}, playermail.LimitsV1())
	if err != nil {
		return nil, fmt.Errorf("mail delivery recovery projection store: %w", err)
	}
	loadCtx, loadCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = mailStore.EnsureLoaded(loadCtx)
	loadCancel()
	if err != nil {
		return nil, fmt.Errorf("mail delivery recovery projection scripts: %w", err)
	}
	workLease := redisworkload.NewAttributedWorkLease(authorityStore.WorkLease())
	worker, err := maildeliveryrecovery.NewWorker(maildeliveryrecovery.Config{
		AuthorityStore: authorityStore, WorkLease: workLease, DueIndex: workLease, MailEntries: mailStore, Limits: playermail.LimitsV1(),
		Buckets: buckets, OwnedBuckets: ownedBuckets, Owner: owner, Token: token, BootID: bootID,
		BucketsPerRound:  parseInt("MAIL_DELIVERY_WORKER_BUCKETS_PER_ROUND", 16),
		RoundTimeBudget:  parseDuration("MAIL_DELIVERY_WORKER_ROUND_TIME_BUDGET", 3*time.Second),
		ClaimLimit:       parseInt("MAIL_DELIVERY_WORKER_CLAIM_LIMIT", 32),
		MaxConcurrent:    parseInt("MAIL_DELIVERY_WORKER_MAX_CONCURRENT", 4),
		LeaseTTL:         parseDuration("MAIL_DELIVERY_WORKER_LEASE", 10*time.Second),
		SafetyMargin:     parseDuration("MAIL_DELIVERY_WORKER_LEASE_SAFETY_MARGIN", 2*time.Second),
		OperationTimeout: parseDuration("MAIL_DELIVERY_WORKER_OPERATION_TIMEOUT", 3*time.Second),
		RetryDelay:       parseDuration("MAIL_DELIVERY_WORKER_RETRY_DELAY", 250*time.Millisecond),
	})
	if err != nil {
		return nil, fmt.Errorf("mail delivery recovery worker: %w", err)
	}
	return worker, nil
}
