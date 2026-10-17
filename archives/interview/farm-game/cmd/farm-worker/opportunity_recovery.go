package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/dynamicconfig"
	"farmgame/internal/friend"
	"farmgame/internal/gateway/session"
	logicdownlink "farmgame/internal/logic/downlink"
	"farmgame/internal/permitsecret"
	"farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/opportunityrecovery"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type opportunityRecoveryRuntime struct {
	Worker           *opportunityrecovery.Worker
	Assignment       *opportunityrecovery.RouteWorkerShardAssignmentProvider
	PubSubTransport  *friend.PubSubOpportunityHintTransport
	DirectTransport  *friend.DirectOpportunityHintTransport
	DynamicTransport *friend.SwitchableOpportunityHintTransport
	ModeWatcher      *dynamicconfig.FileValue
	TransportMode    friend.OpportunityHintTransportMode
	dispatcher       *logicdownlink.Dispatcher
	connections      []*grpc.ClientConn
}

type renewalGatedWorkerAssignment struct {
	provider  opportunityrecovery.WorkerShardAssignmentProvider
	admission opportunityrecovery.WorkerShardClaimAdmission
	renewer   *farmWorkerAuthorityRenewer
}

func (a *renewalGatedWorkerAssignment) Assignment(
	ctx context.Context,
) (opportunityrecovery.WorkerShardAssignment, error) {
	if a == nil || a.provider == nil || a.renewer == nil || !a.renewer.Healthy() {
		return opportunityrecovery.WorkerShardAssignment{}, errFarmWorkerAuthorityRenewal
	}
	return a.provider.Assignment(ctx)
}

func (a *renewalGatedWorkerAssignment) CanClaimShard(
	ctx context.Context,
	shard uint32,
	revision uint64,
) error {
	if a == nil || a.admission == nil || a.renewer == nil || !a.renewer.Healthy() {
		return errFarmWorkerAuthorityRenewal
	}
	return a.admission.CanClaimShard(ctx, shard, revision)
}

func (r *opportunityRecoveryRuntime) StartModeWatch(ctx context.Context) {
	if r == nil {
		return
	}
	if r.ModeWatcher != nil {
		go func() {
			err := r.ModeWatcher.Run(ctx, func(reloadErr error) {
				log.Printf(
					"FARM opportunity Hint mode reload failed closed: %v",
					reloadErr,
				)
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("FARM opportunity Hint mode watch stopped fail-closed: %v", err)
			}
		}()
	}
}

func (r *opportunityRecoveryRuntime) Close() {
	if r == nil {
		return
	}
	if r.dispatcher != nil {
		r.dispatcher.Close()
	}
	for _, connection := range r.connections {
		_ = connection.Close()
	}
}

func (r *opportunityRecoveryRuntime) ModeHealthy() bool {
	return r == nil || r.ModeWatcher == nil || r.ModeWatcher.Healthy()
}

// buildOpportunityRecovery composes the optional farm-global opportunity
// wakeup experiment. The schedule is colocated with FARM authority; the
// worker only reads Header/Facts and publishes an invalidation Hint. It never
// mutates CropCycle maturity or authorizes a viewer action.
func buildOpportunityRecovery(authorityClient, runtimeClient, projectionClient redis.UniversalClient,
	resolver *actorrouting.StaticResolver, shardRuntime *farmWorkerShardRuntime,
	owner, token, bootID string, shadow bool) (*opportunityRecoveryRuntime, error) {
	if authorityClient == nil || runtimeClient == nil || projectionClient == nil || resolver == nil || shardRuntime == nil {
		return nil, errors.New("opportunity wakeup requires Redis domains and a static Actor route snapshot")
	}
	snapshot := resolver.Snapshot()
	calculator, err := actorrouting.NewCalculator(snapshot.Realm,
		actorrouting.HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed},
		snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		return nil, fmt.Errorf("opportunity wakeup route calculator: %w", err)
	}
	namespace, err := rediskey.NewNamespace(env("REDIS_NAMESPACE_ENVIRONMENT", "dev"), env("REDIS_NAMESPACE_ZONE", "z1"))
	if err != nil {
		return nil, fmt.Errorf("opportunity wakeup Redis namespace: %w", err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion:         env("REDIS_PHYSICAL_PROFILE_VERSION", "redis-physical-v1"),
		Namespace:              namespace,
		AuthorityLayoutVersion: env("REDIS_AUTHORITY_LAYOUT_VERSION", "v1"),
		WorkflowLayoutVersion:  env("REDIS_WORKFLOW_LAYOUT_VERSION", "v1"),
		WorkflowHashVersion:    uint32(parseInt("REDIS_WORKFLOW_HASH_VERSION", 1)),
		WorkflowBucketCount:    uint32(parseInt("REDIS_WORKFLOW_BUCKET_COUNT", 256)),
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
		return nil, fmt.Errorf("opportunity wakeup authority store: %w", err)
	}
	workLease := authorityStore.WorkLease()
	scheduleStore, err := opportunityrecovery.NewRedisScheduleStore(workLease)
	if err != nil {
		return nil, err
	}
	farmReader, err := opportunityrecovery.NewFarmOpportunityReader(authorityStore)
	if err != nil {
		return nil, err
	}
	repositories, err := friend.NewRedisFriendshipRepositoriesForNamespace(projectionClient, namespace)
	if err != nil {
		return nil, fmt.Errorf("opportunity wakeup friendship repositories: %w", err)
	}
	hintBus, err := friend.NewRedisOpportunityHintBus(projectionClient, namespace)
	if err != nil {
		return nil, fmt.Errorf("opportunity wakeup Hint bus: %w", err)
	}
	pubsubTransport, err := friend.NewPubSubOpportunityHintTransport(hintBus, repositories.Index)
	if err != nil {
		return nil, err
	}
	transportMode, err := friend.ParseOpportunityHintTransportMode(env("FARM_OPPORTUNITY_HINT_TRANSPORT", string(friend.OpportunityHintTransportPubSub)))
	if err != nil {
		return nil, err
	}
	runtime := &opportunityRecoveryRuntime{
		PubSubTransport: pubsubTransport,
		TransportMode:   transportMode,
	}
	keepRuntime := false
	defer func() {
		if !keepRuntime {
			runtime.Close()
		}
	}()
	modeFile := strings.TrimSpace(os.Getenv("FARM_OPPORTUNITY_HINT_MODE_FILE"))
	var directTransport *friend.DirectOpportunityHintTransport
	if transportMode == friend.OpportunityHintTransportDirect ||
		transportMode == friend.OpportunityHintTransportShadowDirect ||
		modeFile != "" {
		permitConfig, permitErr := permitsecret.Load(os.Getenv)
		if permitErr != nil {
			return nil, fmt.Errorf("opportunity direct SessionRoute resolver: %w", permitErr)
		}
		routeResolver := session.NewRedis(runtimeClient, session.Config{Secret: permitConfig.Secret(), AuthorityEpoch: 1})
		gatewayClient, connections, gatewayErr := openOpportunityGatewayDownlinks()
		if gatewayErr != nil {
			return nil, gatewayErr
		}
		if gatewayClient == nil {
			return nil, errors.New("opportunity direct transport requires GATEWAY_DOWNLINK_ROUTES or GATEWAY_DOWNLINK_ADDR")
		}
		dispatcherConfig := logicdownlink.DefaultDispatcherConfig()
		dispatcherConfig.LaneQueueSize = parseInt("FARM_OPPORTUNITY_DIRECT_LANE_QUEUE_SIZE", dispatcherConfig.LaneQueueSize)
		dispatcherConfig.BatchSize = parseInt("FARM_OPPORTUNITY_DIRECT_BATCH_SIZE", dispatcherConfig.BatchSize)
		dispatcherConfig.RPCTimeout = parseDuration("FARM_OPPORTUNITY_DIRECT_RPC_TIMEOUT", dispatcherConfig.RPCTimeout)
		dispatcherConfig.MaxConcurrent = parseInt("FARM_OPPORTUNITY_DIRECT_MAX_CONCURRENT", dispatcherConfig.MaxConcurrent)
		runtime.dispatcher = logicdownlink.NewDispatcherWithConfig(gatewayClient, dispatcherConfig)
		runtime.connections = connections
		directTransport, err = friend.NewDirectOpportunityHintTransport(repositories.Index, routeResolver, runtime.dispatcher)
		if err != nil {
			runtime.Close()
			return nil, err
		}
		runtime.DirectTransport = directTransport
	}
	var selectedTransport friend.TargetedOpportunityHintTransport
	if modeFile == "" {
		selectedTransport, err = friend.SelectOpportunityHintTransport(
			string(transportMode),
			pubsubTransport,
			directTransport,
		)
		if err != nil {
			runtime.Close()
			return nil, err
		}
	} else {
		runtime.DynamicTransport, err = friend.NewSwitchableOpportunityHintTransport(
			pubsubTransport,
			directTransport,
			transportMode,
		)
		if err != nil {
			return nil, err
		}
		runtime.ModeWatcher, err = dynamicconfig.NewFileValue(
			modeFile,
			parseDuration("FARM_OPPORTUNITY_HINT_MODE_POLL_INTERVAL", time.Second),
			func(raw string) error {
				mode, parseErr := friend.ParseOpportunityHintTransportMode(raw)
				if parseErr != nil {
					return parseErr
				}
				return runtime.DynamicTransport.SetMode(mode)
			},
		)
		if err != nil {
			return nil, fmt.Errorf("opportunity Hint mode file: %w", err)
		}
		// The mode file is the live operator-owned value and the watcher has
		// already applied it. Refusing to boot on a difference would leave every
		// process unable to restart once the mode ladder has moved past the
		// environment seed, so adopt the file and record the adoption instead.
		if current := runtime.ModeWatcher.Current(); current != string(transportMode) {
			log.Printf(
				"opportunity Hint boot mode adopted from file: env=%s file=%s",
				transportMode,
				current,
			)
		}
		selectedTransport = runtime.DynamicTransport
	}
	farmShards, err := calculator.ShardCount(actorrouting.DomainFarm)
	if err != nil {
		return nil, fmt.Errorf("opportunity wakeup FARM shard count: %w", err)
	}
	buckets := make([]rediskey.AuthorityBucketRef, 0, farmShards)
	for shard := uint32(0); shard < farmShards; shard++ {
		bucket, bucketErr := profile.AuthorityBucketForShard(calculator, actorrouting.DomainFarm, shard)
		if bucketErr != nil {
			return nil, fmt.Errorf("opportunity wakeup FARM bucket %d: %w", shard, bucketErr)
		}
		buckets = append(buckets, bucket)
	}
	shardAssignment := shardRuntime.Provider
	runtime.Assignment = shardRuntime.Route
	worker, err := opportunityrecovery.New(opportunityrecovery.Config{
		ScheduleStore: scheduleStore, FarmReader: farmReader, Publisher: selectedTransport, Buckets: buckets,
		ShardAssignment: shardAssignment,
		Owner:           owner, Token: token, BootID: bootID,
		ClaimLimit:      parseInt("FARM_OPPORTUNITY_WAKEUP_CLAIM_LIMIT", opportunityrecovery.DefaultClaimLimit),
		DiscoveryLimit:  parseInt("FARM_OPPORTUNITY_WAKEUP_DISCOVERY_LIMIT", opportunityrecovery.DefaultDiscoveryLimit),
		LeaseTTL:        parseDuration("FARM_OPPORTUNITY_WAKEUP_LEASE", opportunityrecovery.DefaultLeaseTTL),
		MaxConcurrent:   parseInt("FARM_OPPORTUNITY_WAKEUP_MAX_CONCURRENT", opportunityrecovery.DefaultMaxConcurrent),
		RoundTimeBudget: parseDuration("FARM_OPPORTUNITY_WAKEUP_ROUND_TIME_BUDGET", opportunityrecovery.DefaultRoundTimeBudget),
		Shadow:          shadow,
		TrustedNowMs:    workLease.TrustedNowMs,
	})
	if err != nil {
		runtime.Close()
		return nil, err
	}
	runtime.Worker = worker
	keepRuntime = true
	return runtime, nil
}

func openOpportunityGatewayDownlinks() (logicdownlink.GatewayClient, []*grpc.ClientConn, error) {
	if rawRoutes := strings.TrimSpace(os.Getenv("GATEWAY_DOWNLINK_ROUTES")); rawRoutes != "" {
		clients := make(map[string]logicdownlink.GatewayClient)
		connections := make([]*grpc.ClientConn, 0)
		closeAll := func() {
			for _, connection := range connections {
				_ = connection.Close()
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
				return nil, nil, fmt.Errorf("duplicate opportunity gateway route %q", gatewayID)
			}
			connection, dialErr := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if dialErr != nil {
				closeAll()
				return nil, nil, fmt.Errorf("dial opportunity gate %s at %s: %w", gatewayID, address, dialErr)
			}
			connections = append(connections, connection)
			clients[gatewayID] = logicrpcpb.NewGatewayDownlinkServiceClient(connection)
		}
		routed, routeErr := logicdownlink.NewRoutedGatewayClient(clients, nil)
		if routeErr != nil {
			closeAll()
			return nil, nil, routeErr
		}
		return routed, connections, nil
	}
	if gatewayAddr := strings.TrimSpace(os.Getenv("GATEWAY_DOWNLINK_ADDR")); gatewayAddr != "" {
		connection, err := grpc.Dial(gatewayAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, fmt.Errorf("dial opportunity gate %s: %w", gatewayAddr, err)
		}
		return logicrpcpb.NewGatewayDownlinkServiceClient(connection), []*grpc.ClientConn{connection}, nil
	}
	return nil, nil, nil
}

// opportunityOwnedShardAssignment defaults to every configured FARM shard for
// static compatibility deployments. Tests and load tools may set
// FARM_OPPORTUNITY_WAKEUP_OWNED_SHARDS to an explicit validated subset.
func opportunityOwnedShardAssignment(
	configured []uint32,
	assignmentSource actorrouting.RevisionedAssignmentProvider,
	workerID string,
	bootID string,
) (
	opportunityrecovery.WorkerShardAssignmentProvider,
	*opportunityrecovery.RouteWorkerShardAssignmentProvider,
	error,
) {
	return opportunityOwnedShardAssignmentWithOptions(configured, assignmentSource, workerID, bootID, opportunityrecovery.RouteWorkerShardAssignmentOptions{})
}

func opportunityOwnedShardAssignmentWithOptions(
	configured []uint32,
	assignmentSource actorrouting.RevisionedAssignmentProvider,
	workerID string,
	bootID string,
	routeOptions opportunityrecovery.RouteWorkerShardAssignmentOptions,
) (
	opportunityrecovery.WorkerShardAssignmentProvider,
	*opportunityrecovery.RouteWorkerShardAssignmentProvider,
	error,
) {
	raw := strings.TrimSpace(os.Getenv("FARM_WORKER_OWNED_SHARDS"))
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv("FARM_OPPORTUNITY_WAKEUP_OWNED_SHARDS"))
	}
	modeRaw := strings.TrimSpace(os.Getenv("FARM_WORKER_ASSIGNMENT_MODE"))
	if modeRaw == "" {
		modeRaw = strings.TrimSpace(os.Getenv("FARM_OPPORTUNITY_WAKEUP_ASSIGNMENT_MODE"))
	}
	if modeRaw == "" {
		if assignmentSource != nil {
			modeRaw = "route"
		} else {
			modeRaw = "static"
		}
	}
	mode := strings.ToLower(modeRaw)
	if mode == "route" {
		if raw != "" {
			return nil, nil, errors.New("route assignment mode cannot use FARM_WORKER_OWNED_SHARDS")
		}
		if assignmentSource == nil {
			return nil, nil, errors.New(
				"route assignment mode requires AssignmentComposition.Provider",
			)
		}
		provider, err := opportunityrecovery.NewRouteWorkerShardAssignmentProviderWithOptions(
			assignmentSource,
			workerID,
			bootID,
			routeOptions,
		)
		if err != nil {
			return nil, nil, err
		}
		if err := provider.Bootstrap(context.Background()); err != nil {
			return nil, nil, err
		}
		return provider, provider, nil
	}
	if mode != "static" {
		return nil, nil, fmt.Errorf("unsupported FARM_WORKER_ASSIGNMENT_MODE %q", mode)
	}
	if raw == "" {
		provider, err := opportunityrecovery.NewAllConfiguredShardsProvider(configured)
		return provider, nil, err
	}
	parts := strings.Split(raw, ",")
	owned := make([]uint32, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		shard64, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return nil, nil, fmt.Errorf("opportunity wakeup owned shard %q: %w", part, err)
		}
		owned = append(owned, uint32(shard64))
	}
	provider, err := opportunityrecovery.NewStaticWorkerShardAssignmentProvider(owned)
	return provider, nil, err
}

type opportunityRecoveryMetrics struct {
	rounds, buckets, scanned, candidates, claimed, published, advanced, completed, stale, failed prometheus.Counter
	emptyShards, budgetExhausted                                                                 prometheus.Counter
	ownedShards, dueBacklog, dueLagMs                                                            prometheus.Gauge
	assignmentRevision, assignmentReady, assignmentWatchGaps, assignmentFullReloads              prometheus.Gauge
	claimRejected                                                                                prometheus.Counter
	duration                                                                                     prometheus.Histogram
}

func newOpportunityRecoveryMetrics(registry *prometheus.Registry) *opportunityRecoveryMetrics {
	metric := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Namespace: "farm", Subsystem: "opportunity_wakeup", Name: name, Help: help})
	}
	gauge := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Namespace: "farm", Subsystem: "opportunity_wakeup", Name: name, Help: help})
	}
	m := &opportunityRecoveryMetrics{
		rounds:                metric("round_total", "Bounded opportunity wakeup rounds."),
		buckets:               metric("bucket_scan_total", "Owned FARM authority buckets inspected by opportunity wakeup discovery."),
		scanned:               metric("due_ref_scanned_total", "Due references returned by opportunity wakeup discovery."),
		candidates:            metric("candidate_total", "Due schedule candidates inspected."),
		claimed:               metric("claimed_total", "Opportunity schedules claimed."),
		published:             metric("hint_published_total", "Targeted invalidation Hints published."),
		advanced:              metric("advanced_total", "Schedules advanced to their next farm boundary."),
		completed:             metric("completed_total", "Schedules completed because no future boundary remains."),
		stale:                 metric("stale_generation_total", "Stale schedule generations fenced without publishing."),
		failed:                metric("failed_total", "Opportunity wakeup item failures."),
		emptyShards:           metric("empty_shard_total", "Owned FARM shards with an empty due index in a round."),
		budgetExhausted:       metric("round_budget_exhausted_total", "Rounds that stopped early because RoundTimeBudget elapsed."),
		ownedShards:           gauge("owned_shards", "FARM shards owned by this opportunity wakeup worker."),
		dueBacklog:            gauge("due_backlog", "Due schedule references observed across owned shards in the latest round."),
		dueLagMs:              gauge("due_lag_ms", "Oldest due lag in milliseconds observed in the latest round."),
		assignmentRevision:    gauge("assignment_revision", "Current FARM_WORKER assignment revision used by the latest round."),
		assignmentReady:       gauge("assignment_ready", "Whether the local FARM_WORKER assignment snapshot can admit new claims."),
		assignmentWatchGaps:   gauge("assignment_watch_gaps", "Cumulative FARM_WORKER watch gaps or reconnect reloads observed by this process."),
		assignmentFullReloads: gauge("assignment_full_reloads", "Cumulative trusted full FARM_WORKER assignment reloads observed by this process."),
		claimRejected:         metric("claim_rejected_total", "New claims rejected after FARM_WORKER assignment changed."),
		duration:              prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "farm", Subsystem: "opportunity_wakeup", Name: "round_duration_seconds", Help: "Bounded opportunity wakeup round duration.", Buckets: prometheus.DefBuckets}),
	}
	registry.MustRegister(m.rounds, m.buckets, m.scanned, m.candidates, m.claimed, m.published, m.advanced, m.completed, m.stale, m.failed,
		m.emptyShards, m.budgetExhausted, m.claimRejected, m.ownedShards, m.dueBacklog, m.dueLagMs,
		m.assignmentRevision, m.assignmentReady, m.assignmentWatchGaps, m.assignmentFullReloads, m.duration)
	return m
}

func (m *opportunityRecoveryMetrics) observe(stats opportunityrecovery.RoundStats, duration time.Duration) {
	if m == nil {
		return
	}
	m.rounds.Inc()
	m.buckets.Add(float64(stats.Buckets))
	m.scanned.Add(float64(stats.Scanned))
	m.candidates.Add(float64(stats.Candidates))
	m.claimed.Add(float64(stats.Claimed))
	m.published.Add(float64(stats.Published))
	m.advanced.Add(float64(stats.Advanced))
	m.completed.Add(float64(stats.Completed))
	m.stale.Add(float64(stats.StaleGeneration))
	m.failed.Add(float64(stats.Failed))
	m.emptyShards.Add(float64(stats.EmptyShards))
	m.budgetExhausted.Add(float64(stats.BudgetExhausted))
	m.claimRejected.Add(float64(stats.ClaimRejected))
	m.ownedShards.Set(float64(stats.OwnedShards))
	m.assignmentRevision.Set(float64(stats.AssignmentRevision))
	m.dueBacklog.Set(float64(stats.DueBacklog))
	m.dueLagMs.Set(float64(stats.MaxDueLagMs))
	m.duration.Observe(duration.Seconds())
}

func (m *opportunityRecoveryMetrics) observeAssignment(provider *opportunityrecovery.RouteWorkerShardAssignmentProvider) {
	if m == nil || provider == nil {
		return
	}
	stats := provider.Stats()
	if stats.Ready {
		m.assignmentReady.Set(1)
	} else {
		m.assignmentReady.Set(0)
	}
	m.assignmentRevision.Set(float64(stats.AssignmentRevision))
	m.assignmentWatchGaps.Set(float64(stats.WatchGaps))
	m.assignmentFullReloads.Set(float64(stats.FullReloads))
}

func startOpportunityRecoveryLoop(ctx context.Context, worker *opportunityrecovery.Worker, assignment *opportunityrecovery.RouteWorkerShardAssignmentProvider,
	interval, idleMaxInterval time.Duration, metrics *opportunityRecoveryMetrics) <-chan struct{} {
	started := make(chan struct{})
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Printf("farm-worker opportunity wakeup loop panic: %v", recovered)
			}
		}()
		if interval <= 0 {
			interval = time.Second
		}
		if idleMaxInterval < interval {
			idleMaxInterval = interval
		}
		var sweep adaptiveShardSweep
		timer := time.NewTimer(0)
		defer timer.Stop()
		first := true
		for {
			select {
			case <-ctx.Done():
				if first {
					close(started)
				}
				return
			case <-timer.C:
				begin := time.Now()
				stats, err := worker.RunOnce(ctx)
				metrics.observe(stats, time.Since(begin))
				metrics.observeAssignment(assignment)
				if first {
					close(started)
					first = false
				}
				if err != nil {
					log.Printf("recover FARM opportunity wakeups: %v", err)
				}
				if stats.Claimed != 0 || stats.Published != 0 || stats.Advanced != 0 || stats.Completed != 0 ||
					stats.Failed != 0 || stats.BudgetExhausted != 0 {
					log.Printf("opportunity wakeup round=%+v", stats)
				}
				// A complete empty traversal backs off exponentially, while any
				// candidate or error immediately returns to the base interval. Direct
				// hints still wake the live path; this loop is bounded recovery.
				next := sweep.next(interval, idleMaxInterval, stats.AssignmentRevision, stats.OwnedShards, stats.Buckets, stats.Candidates, err)
				timer.Reset(next)
			}
		}
	}()
	return started
}
