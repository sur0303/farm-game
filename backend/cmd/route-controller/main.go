// Command route-controller serves the authenticated RouteSnapshot and
// internal RoutePlan services. It remains one single-controller process; no
// HA/leader election or public mutation surface is composed here.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	routeplanpb "farmgame/api/gen/routeplan"
	"farmgame/internal/actorrouting"
	"farmgame/internal/observability/redisworkload"
	"farmgame/internal/redisdomains"
	core "farmgame/internal/routecontrol"
	"farmgame/internal/routecontrol/gateverification"
	"farmgame/internal/routecontrol/transport"
	"farmgame/internal/routeplan"
	scaleplancore "farmgame/internal/scaleplan"
	"farmgame/internal/storage/redis/rediskey"
	redisroutecontrol "farmgame/internal/storage/redis/routecontrol"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

const (
	defaultShutdownTimeout            = 5 * time.Second
	defaultStartupTimeout             = 10 * time.Second
	defaultRedisTimeout               = 2 * time.Second
	defaultPlanOperationTimeout       = 30 * time.Second
	defaultPlanTTL                    = 24 * time.Hour
	defaultLeaseProjectionInterval    = 5 * time.Second
	defaultLeaseProjectionMinimum     = 15 * time.Second
	defaultLeaseProjectionWaveTimeout = 30 * time.Second
	defaultRouteFailoverInterval      = time.Second
	defaultRouteFailoverMaxPlans      = 32
	defaultRouteFailoverMaxReads      = 128
	defaultScalePlanParallelism       = 8
	defaultHolderControlMaxClients    = 64
	maxHolderControlMaxClients        = 256
	initialAssignmentModeEnv          = "ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_MODE"
	initialAssignmentTimeoutEnv       = "ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_TIMEOUT"
	initialAssignmentPlayerCountEnv   = "ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_EXPECTED_PLAYER_HOLDERS"
	initialAssignmentFarmCountEnv     = "ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_EXPECTED_FARM_HOLDERS"
	leaseProjectionModeDisabled       = "disabled"
	leaseProjectionModeAllActive      = "all-active"
	deadlineRefreshModeSnapshot       = "snapshot"
	deadlineRefreshModeAuthority      = "authority-delegated"
	// A 1024-shard scale-out may require eleven bounded 32-shard waves. Keep
	// the per-wave parallelism capped, but allow the owning operation enough
	// time to finish the complete, deliberately serialized wave sequence.
	maxPlanOperationTimeout      = 30 * time.Minute
	maxPlanTTL                   = 7 * 24 * time.Hour
	productionPlanAuditRetention = 38 * 24 * time.Hour
	// Keep enough bounded RoutePlan history for four complete 2048-shard
	// topology generations. The generic Redis store defaults (1024 items,
	// 8 MiB) are intentionally small for library users, but cannot represent
	// even one full production failover of the current PLAYER + FARM layout.
	productionRoutePlanMaxItems      = 8192
	productionRoutePlanMaxBytes      = 128 << 20
	planExecutorTTLSafety            = time.Second
	targetSnapshotConvergenceTimeout = 15 * time.Second
	recoveryStartupHeadroom          = 5 * time.Second
	defaultDeadlineRefreshInterval   = time.Second
	defaultDeadlineRefreshLead       = 30 * time.Second
	maxDeadlineRefreshInterval       = 30 * time.Second
	maxDeadlineRefreshLead           = 59 * time.Second
	deadlineRefreshApplyHeadroom     = 5 * time.Second
)

type config struct {
	listen                                 string
	controllerID                           string
	boot                                   string
	realm                                  string
	redisURL                               string
	redisClusterURLs                       string
	namespaceEnvironment                   string
	namespaceZone                          string
	redisOperation                         time.Duration
	metricsListen                          string
	maxSnapshotBytes                       int
	maxClients                             int
	queueSize                              int
	maxMessageBytes                        int
	tlsCert                                string
	tlsKey                                 string
	tlsCA                                  string
	holderClientCert                       string
	holderClientKey                        string
	holderClientCA                         string
	holderDiscoveryMode                    string
	holderDiscoveryAudience                string
	holderDiscoveryResync                  time.Duration
	holderDiscoveryVerifiedTTL             time.Duration
	holderDiscoveryPlayerLogicPort         int
	holderDiscoveryPlayerControlPort       int
	holderDiscoveryFarmLogicPort           int
	holderDiscoveryFarmControlPort         int
	holderDiscoveryPlayerTLSIdentity       string
	holderDiscoveryFarmTLSIdentity         string
	gateVerifierClientCert                 string
	gateVerifierClientKey                  string
	gateVerifierClientCA                   string
	gateVerifierPeers                      []gateverification.Peer
	gateVerifierTimeout                    time.Duration
	planOperation                          time.Duration
	planTTL                                time.Duration
	scalePlanMode                          string
	scalePlanRoutePlanParallelism          int
	holderControlMaxClients                int
	initialAssignmentMode                  string
	initialAssignmentTimeout               time.Duration
	initialAssignmentExpectedPlayerHolders int
	initialAssignmentExpectedFarmHolders   int
	// These shorter names are retained as composition aliases for tests and
	// callers that construct config directly; parsing populates both forms.
	initialAssignmentPlayerHolders   int
	initialAssignmentFarmHolders     int
	scalePlanPlayerNamespace         string
	scalePlanPlayerStatefulSet       string
	scalePlanFarmNamespace           string
	scalePlanFarmStatefulSet         string
	scalePlanAutoInterval            time.Duration
	scalePlanSafeDesiredAnnotation   string
	scalePlanRolloutEnabled          bool
	scalePlanRolloutAnnotation       string
	scalePlanRolloutIntentAnnotation string
	routeFailoverInterval            time.Duration
	routeFailoverMaxPlans            int
	routeFailoverMaxAuthorityReads   int
	leaseProjectionMode              string
	deadlineRefreshMode              string
	leaseProjectionInterval          time.Duration
	leaseProjectionMinimum           time.Duration
	leaseProjectionTimeout           time.Duration
	healthListen                     string
	deadlineRefreshInterval          time.Duration
	deadlineRefreshLead              time.Duration
	physicalProfile                  string
	authorityLayout                  string
	workflowLayout                   string
	workflowHashVersion              string
	workflowBucketCount              string
	allowedClientIdentity            []string
	planReadIdentity                 []string
	operatorIdentity                 []string
	singleControllerContract         string
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		return err
	}
	if err := cfg.validate(); err != nil {
		return err
	}

	redisOpenCtx, redisOpenCancel := context.WithTimeout(context.Background(), 5*time.Second)
	redisClient, err := redisdomains.OpenEndpoint(redisOpenCtx, cfg.redisURL, cfg.redisClusterURLs)
	redisOpenCancel()
	if err != nil {
		return errors.New("route-controller: Redis endpoint unavailable")
	}
	defer redisClient.Close()

	keyspaceTuple, err := cfg.keyspaceTuple()
	if err != nil {
		return errors.New("route-controller: invalid Redis keyspace tuple")
	}
	fingerprint, err := keyspaceTuple.Fingerprint()
	if err != nil {
		return errors.New("route-controller: invalid Redis keyspace fingerprint")
	}
	log.Printf("route-controller Redis keyspace fingerprint=%s", fingerprint)
	namespace := keyspaceTuple.Namespace()
	storeOptions := redisroutecontrol.DefaultOptions(namespace)
	storeOptions.OperationTimeout = cfg.redisOperation
	storeOptions.MaxSnapshotBytes = cfg.maxSnapshotBytes
	store, err := redisroutecontrol.NewSnapshotStore(redisClient, storeOptions)
	if err != nil {
		return errors.New("route-controller: invalid Redis snapshot store options")
	}

	startupCtx, startupCancel := context.WithTimeout(context.Background(), defaultStartupTimeout)
	hub, err := core.NewSnapshotWatchHubWithContext(startupCtx, store, core.SnapshotWatchHubOptions{
		MaxClients:       cfg.maxClients,
		QueueSize:        cfg.queueSize,
		MaxSnapshotBytes: cfg.maxSnapshotBytes,
	})
	startupCancel()
	if err != nil {
		return errors.New("route-controller: snapshot store startup failed")
	}
	defer hub.Close()

	stored, found, currentErr := hub.Current(context.Background())
	if currentErr != nil {
		return errors.New("route-controller: snapshot reconciliation failed")
	} else if err := requireDurableSnapshot(found); err != nil {
		return err
	} else if cfg.realm != "" && stored.Realm != cfg.realm {
		return errors.New("route-controller: configured realm does not match durable snapshot")
	}

	// Holder Pods may be gated on Route Controller /healthz before their main
	// process starts.  The full health server cannot be constructed until the
	// plan runtime and discovery dependencies exist, so publish only the
	// liveness endpoint during this narrow bootstrap window.  /readyz remains
	// 503 until the normal startup sequence has completed; this releases the
	// Holder init barrier without weakening route or identity readiness.
	bootstrapHealthServer := newBootstrapHealthServer()
	bootstrapHealthListener, err := net.Listen("tcp", cfg.healthListen)
	if err != nil {
		return errors.New("route-controller: bootstrap health listener setup failed")
	}
	bootstrapHealthServerDone := make(chan error, 1)
	go func() { bootstrapHealthServerDone <- bootstrapHealthServer.Serve(bootstrapHealthListener) }()
	defer func() {
		if bootstrapHealthServer != nil {
			shutdownHTTPServer(bootstrapHealthServer)
		}
	}()

	runtimeCtx, runtimeCancel := context.WithTimeout(context.Background(), defaultStartupTimeout)
	metricsRegistry := prometheus.NewRegistry()
	metricsRegistry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	if _, err := redisworkload.AttachFromEnvironmentWithComponent(redisClient, metricsRegistry,
		os.Getenv("REDIS_WORKLOAD_METRICS"), redisworkload.ComponentRouteControl); err != nil {
		return fmt.Errorf("route-controller: Redis workload metrics: %w", err)
	}
	if _, err := redisworkload.AttachDependencyHealth(metricsRegistry, redisworkload.DependencyTargets{Authority: redisClient}); err != nil {
		return fmt.Errorf("route-controller: Redis dependency metrics: %w", err)
	}
	controlMutation, err := newRouteControllerControlMutation(cfg)
	if err != nil {
		runtimeCancel()
		return errors.New("route-controller: control mutation composition failed")
	}
	planRuntime, err := newPlanRuntimeWithControl(runtimeCtx, cfg, redisClient, keyspaceTuple, store, hub, stored, metricsRegistry, controlMutation)
	runtimeCancel()
	if err != nil {
		return fmt.Errorf("route-controller: plan runtime startup failed: %w", err)
	}
	defer planRuntime.Close()
	discoveryHooks, closeDiscovery, err := newKubernetesHolderDiscoveryHooks(context.Background(), cfg, planRuntime)
	if err != nil {
		return fmt.Errorf("route-controller: holder discovery startup failed: %w", err)
	}
	scalePlanRuntime, err := newScalePlanRuntime(context.Background(), cfg, redisClient, keyspaceTuple, planRuntime, discoveryHooks)
	if err != nil {
		closeDiscovery()
		return fmt.Errorf("route-controller: ScalePlan runtime startup failed: %w", err)
	}
	defer scalePlanRuntime.Close()
	initialAssignmentRuntime, err := newInitialAssignmentRuntime(cfg, planRuntime, discoveryHooks.VerifiedHolderView)
	if err != nil {
		closeDiscovery()
		return fmt.Errorf("route-controller: initial assignment runtime startup failed: %w", err)
	}
	tlsConfig, err := transport.LoadServerTLSConfig(cfg.tlsCert, cfg.tlsKey, cfg.tlsCA)
	if err != nil {
		return errors.New("route-controller: TLS configuration failed")
	}
	service, err := transport.NewRouteSnapshotServer(hub, transport.ServerOptions{
		ControllerID:          cfg.controllerID,
		Boot:                  cfg.boot,
		Realm:                 cfg.realm,
		AllowedClientIdentity: cfg.allowedClientIdentity,
		MaxMessageBytes:       cfg.maxMessageBytes,
	})
	if err != nil {
		return errors.New("route-controller: service configuration failed")
	}
	grpcServer, err := transport.NewMultiServiceGRPCServer(service, tlsConfig, transport.GRPCServerOptions{
		MaxMessageBytes:  cfg.maxMessageBytes,
		MethodAuthorizer: planRuntime.ServerAuthorizer,
	}, func(registrar grpc.ServiceRegistrar) {
		routeplanpb.RegisterRoutePlanServiceServer(registrar, planRuntime.Service)
		scalePlanRuntime.Register(registrar)
	})
	if err != nil {
		return errors.New("route-controller: gRPC TLS setup failed")
	}
	listener, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return errors.New("route-controller: listener setup failed")
	}
	defer listener.Close()
	// Replace the bootstrap-only listener with the full readiness server once
	// all dependencies have been constructed.  The bootstrap server is closed
	// before rebinding the same address; the brief handoff occurs before the
	// Holder main process can complete its route startup.
	shutdownHTTPServer(bootstrapHealthServer)
	bootstrapHealthServer = nil
	_ = bootstrapHealthListener.Close()
	select {
	case <-bootstrapHealthServerDone:
	default:
	}
	healthListener, err := net.Listen("tcp", cfg.healthListen)
	if err != nil {
		return errors.New("route-controller: health listener setup failed")
	}
	defer healthListener.Close()
	healthServer := newRouteControllerHealthServer(planRuntime.LeaseProjection, planRuntime.HolderReader, planRuntime.StartupReady)

	serveErr := make(chan error, 2)
	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()
	go func() {
		serveErr <- healthServer.Serve(healthListener)
	}()
	var metricsServer *http.Server
	var metricsErr <-chan error
	if cfg.metricsListen != "" {
		metricsListener, listenErr := net.Listen("tcp", cfg.metricsListen)
		if listenErr != nil {
			shutdownHTTPServer(healthServer)
			gracefulStop(grpcServer, defaultShutdownTimeout)
			return errors.New("route-controller: metrics listener setup failed")
		}
		defer metricsListener.Close()
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{}))
		metricsServer = &http.Server{Handler: metricsMux, ReadHeaderTimeout: 2 * time.Second}
		result := make(chan error, 1)
		metricsErr = result
		go func() { result <- metricsServer.Serve(metricsListener) }()
	}

	// The durable RouteSnapshot service must be reachable before Holder
	// discovery waits for the data plane. During an all-domain cold restart,
	// holders need that snapshot to construct their runtime and expose
	// HolderControl; discovery then verifies those exact process generations.
	// Mutations and readiness remain closed until this initial informer view,
	// projection recovery, and ScalePlan recovery have all completed.
	if discoveryHooks.WaitInitial != nil {
		discoveryCtx, discoveryCancel := context.WithTimeout(context.Background(), cfg.planOperation)
		err = discoveryHooks.WaitInitial(discoveryCtx)
		discoveryCancel()
		if err != nil {
			shutdownHTTPServer(healthServer)
			gracefulStop(grpcServer, defaultShutdownTimeout)
			shutdownMetricsServer(metricsServer)
			return fmt.Errorf("route-controller: holder discovery initial synchronization failed: %w", err)
		}
	}
	// HolderRegistry placement records are short-lived liveness facts, not
	// Shard authority. A controller outage can therefore outlive those records
	// while Kubernetes and HolderControl still prove the exact same Pod/boot.
	// Re-materialize only the missing non-excluded placement candidates before
	// replaying durable RoutePlans. Writer ownership is unchanged here and is
	// still fenced exclusively by the Authority store and writer_epoch.
	if discoveryHooks.PreparePlanRecovery != nil {
		prepareCtx, prepareCancel := context.WithTimeout(context.Background(), cfg.planOperation)
		err = discoveryHooks.PreparePlanRecovery(prepareCtx)
		prepareCancel()
		if err != nil {
			shutdownHTTPServer(healthServer)
			gracefulStop(grpcServer, defaultShutdownTimeout)
			shutdownMetricsServer(metricsServer)
			return fmt.Errorf("route-controller: holder placement recovery preparation failed: %w", err)
		}
	}

	// Watch must be served before Manager recovery publishes ASSIGNING and waits
	// for holders to heartbeat that exact SnapshotRevision. The periodic
	// recovery owner starts only after the buffered initial batch is committed:
	// while that batch is open Manager deliberately rejects ordinary recovery,
	// and starting the loop here would leave rejected control operations active
	// until their lease expires and poison the coordinator.

	// Plan status remains Ready=false and mutations fail Unavailable until
	// RoutePlan-owned waves, orphan Manager projections, initial PLAYER/FARM
	// assignment, renewed deadline, and complete lease projection all finish.
	//
	// Refresh only monotonic deadlines for already-ACTIVE generations before
	// RoutePlan recovery. A long sliding window can outlive the projected
	// deadlines while a child owns one ASSIGNING shard; refreshing the unrelated
	// ACTIVE routes first prevents that shard from poisoning Holder readiness.
	//
	// A controller restart can also observe the opposite durable cut: Authority
	// has advanced but the matching Snapshot publication was interrupted. In
	// that case the strict all-or-nothing refresh reports generation drift. Do
	// not turn that recoverable cut into a startup loop: remember the failure,
	// let the existing RoutePlan/Manager recovery owners close the projection,
	// then require one strict refresh before Ready opens.
	initialDeadlineCtx, initialDeadlineCancel := context.WithTimeout(context.Background(), cfg.planOperation)
	initialDeadlineErr := planRuntime.Recovery.RefreshDeadlines(initialDeadlineCtx)
	initialDeadlineCancel()
	if initialDeadlineErr != nil {
		log.Printf("route-controller initial deadline refresh deferred until projection recovery err=%v", initialDeadlineErr)
	}

	// RoutePlan recovery must run first. A two-stage wave can durably contain
	// ACTIVE Authority plus an ASSIGNING Snapshot while its shared ACTIVE CAS is
	// pending. Only PlanExecutor owns enough wave identity to reconstruct every
	// member and publish that CAS atomically. Generic Manager recovery runs
	// afterwards to finish projections that are not owned by an incomplete Plan.
	planRecoveryCtx, planRecoveryCancel := context.WithTimeout(context.Background(), cfg.planOperation)
	err = recoverRoutePlansUntilReady(planRecoveryCtx, planRuntime.Executor.Recover)
	planRecoveryCancel()
	if err != nil {
		shutdownHTTPServer(healthServer)
		gracefulStop(grpcServer, defaultShutdownTimeout)
		shutdownMetricsServer(metricsServer)
		return fmt.Errorf("route-controller: plan recovery failed: %w", err)
	}
	initialRecoveryCtx, initialRecoveryCancel := context.WithTimeout(context.Background(), cfg.planOperation)
	err = planRuntime.Recovery.Recover(initialRecoveryCtx)
	initialRecoveryCancel()
	if err != nil {
		shutdownHTTPServer(healthServer)
		gracefulStop(grpcServer, defaultShutdownTimeout)
		shutdownMetricsServer(metricsServer)
		return errors.New("route-controller: pending projection recovery failed")
	}
	if initialDeadlineErr != nil {
		retryDeadlineCtx, retryDeadlineCancel := context.WithTimeout(context.Background(), cfg.planOperation)
		err = planRuntime.Recovery.RefreshDeadlines(retryDeadlineCtx)
		retryDeadlineCancel()
		if err != nil {
			shutdownHTTPServer(healthServer)
			gracefulStop(grpcServer, defaultShutdownTimeout)
			shutdownMetricsServer(metricsServer)
			return fmt.Errorf("route-controller: initial deadline refresh after projection recovery failed: %w", err)
		}
	}
	if initialAssignmentRuntime != nil {
		initialAssignmentCtx, initialAssignmentCancel := context.WithTimeout(context.Background(), effectiveInitialAssignmentTimeout(cfg.initialAssignmentTimeout))
		err = initialAssignmentRuntime.Run(initialAssignmentCtx)
		initialAssignmentCancel()
		if err != nil {
			shutdownHTTPServer(healthServer)
			gracefulStop(grpcServer, defaultShutdownTimeout)
			shutdownMetricsServer(metricsServer)
			return fmt.Errorf("route-controller: initial PLAYER/FARM assignment failed: %w", err)
		}
	}
	if planRuntime.LeaseProjection != nil {
		leaseProjectionCtx, leaseProjectionCancel := context.WithTimeout(context.Background(), cfg.leaseProjectionTimeout)
		err = planRuntime.BootstrapLeaseProjection(leaseProjectionCtx)
		leaseProjectionCancel()
		if err != nil {
			shutdownHTTPServer(healthServer)
			gracefulStop(grpcServer, defaultShutdownTimeout)
			shutdownMetricsServer(metricsServer)
			return errors.New("route-controller: initial lease projection failed")
		}
	}
	if scalePlanRuntime != nil {
		scalePlanCtx, scalePlanCancel := context.WithTimeout(context.Background(), cfg.planOperation)
		err = scalePlanRuntime.Startup(scalePlanCtx)
		scalePlanCancel()
		if err != nil {
			shutdownHTTPServer(healthServer)
			gracefulStop(grpcServer, defaultShutdownTimeout)
			shutdownMetricsServer(metricsServer)
			return errors.New("route-controller: ScalePlan startup failed")
		}
	}
	if initialAssignmentRuntime != nil {
		readyCheckCtx, readyCheckCancel := context.WithTimeout(context.Background(), cfg.planOperation)
		err = initialAssignmentRuntime.VerifyReady(readyCheckCtx)
		readyCheckCancel()
		if err != nil {
			shutdownHTTPServer(healthServer)
			gracefulStop(grpcServer, defaultShutdownTimeout)
			shutdownMetricsServer(metricsServer)
			return fmt.Errorf("route-controller: initial assignment readiness verification failed: %w", err)
		}
	}
	// Initial assignment has now sealed both publisher and transition evidence
	// onto their durable paths, so normal recovery can safely begin exactly once.
	planRuntime.StartRecovery(context.Background())
	if planRuntime.LeaseProjection != nil {
		if err := planRuntime.StartLeaseProjection(context.Background()); err != nil {
			shutdownHTTPServer(healthServer)
			gracefulStop(grpcServer, defaultShutdownTimeout)
			shutdownMetricsServer(metricsServer)
			return errors.New("route-controller: lease projection loop startup failed")
		}
	}
	planRuntime.MarkStartupReady()
	if scalePlanRuntime != nil {
		scalePlanRuntime.MarkReady()
	}

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	select {
	case err := <-serveErr:
		_ = hub.Close()
		shutdownHTTPServer(healthServer)
		gracefulStop(grpcServer, defaultShutdownTimeout)
		shutdownMetricsServer(metricsServer)
		if err == nil || errors.Is(err, grpc.ErrServerStopped) || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case err := <-metricsErr:
		_ = hub.Close()
		shutdownHTTPServer(healthServer)
		gracefulStop(grpcServer, defaultShutdownTimeout)
		shutdownMetricsServer(metricsServer)
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-signalCtx.Done():
		_ = hub.Close()
		shutdownHTTPServer(healthServer)
		gracefulStop(grpcServer, defaultShutdownTimeout)
		shutdownMetricsServer(metricsServer)
		return nil
	}
}

func newBootstrapHealthServer() *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","phase":"bootstrap"}`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"not_ready","phase":"bootstrap"}`))
	})
	return &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

func newRouteControllerHealthServer(projection *leaseProjectionRuntime, registry routeplan.HolderReader, startupChecks ...func() bool) *http.Server {
	startupReady := func() bool { return true }
	if len(startupChecks) == 1 && startupChecks[0] != nil {
		startupReady = startupChecks[0]
	} else if len(startupChecks) > 1 {
		startupReady = func() bool { return false }
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		status := projection.Status()
		status.StartupReady = startupReady()
		projectionReady := projection == nil || projection.Ready()
		if projection == nil {
			status.Mode = leaseProjectionModeDisabled
			status.FailureClass = ""
		}
		if !status.StartupReady || !projectionReady {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(status)
	})
	mux.HandleFunc("/evidencez", func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if projection == nil || registry == nil || !startupReady() || !projection.Ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "not_ready"})
			return
		}
		status := projection.Status()
		status.StartupReady = true
		snapshot := projection.Snapshot()
		if !status.KnownFresh || snapshot.GlobalRevision == 0 || snapshot.GlobalRevision != status.GlobalRevision {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "projection_snapshot_race"})
			return
		}
		ctx, cancel := context.WithTimeout(request.Context(), defaultRedisTimeout)
		defer cancel()
		players, playerErr := registry.List(ctx, actorrouting.DomainPlayer)
		farms, farmErr := registry.List(ctx, actorrouting.DomainFarm)
		if playerErr != nil || farmErr != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "holder_registry_unavailable"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"schema_version": "farm-local-route-controller-evidence.v1",
			"status":         "fresh", "single_controller_publisher_count": 1,
			"projection": status, "snapshot": snapshot,
			"holders": map[string]any{"PLAYER": players, "FARM": farms},
		})
	})
	return &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}

func shutdownHTTPServer(server *http.Server) {
	if server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer cancel()
	_ = server.Shutdown(ctx)
}

func shutdownMetricsServer(server *http.Server) {
	if server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer cancel()
	_ = server.Shutdown(ctx)
}

func requireDurableSnapshot(found bool) error {
	if !found {
		return errors.New("route-controller: durable snapshot is empty; bootstrap is required before startup")
	}
	return nil
}

type serverLifecycle interface {
	GracefulStop()
	Stop()
}

func gracefulStop(server serverLifecycle, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		server.Stop()
	}
}

func parseConfig(args []string) (config, error) {
	rolloutEnabled, err := strictBoolEnv("SCALEPLAN_ROLLOUT_ENABLED", false)
	if err != nil {
		return config{}, err
	}
	gateVerifierTimeout, err := strictDurationEnv("ROUTE_CONTROLLER_GATE_VERIFIER_TIMEOUT", 5*time.Second, 30*time.Second)
	if err != nil {
		return config{}, err
	}
	planTTL, err := strictDurationEnv("ROUTE_CONTROLLER_PLAN_TTL", defaultPlanTTL, maxPlanTTL)
	if err != nil {
		return config{}, err
	}
	deadlineRefreshInterval, err := strictDurationEnv("ROUTE_CONTROLLER_DEADLINE_REFRESH_INTERVAL", defaultDeadlineRefreshInterval, maxDeadlineRefreshInterval)
	if err != nil {
		return config{}, err
	}
	deadlineRefreshLead, err := strictDurationEnv("ROUTE_CONTROLLER_DEADLINE_REFRESH_LEAD", defaultDeadlineRefreshLead, maxDeadlineRefreshLead)
	if err != nil {
		return config{}, err
	}
	deadlineRefreshMode := stringOrDefault("ROUTE_CONTROLLER_DEADLINE_REFRESH_MODE", deadlineRefreshModeSnapshot)
	if err := validateDeadlineRefreshMode(deadlineRefreshMode); err != nil {
		return config{}, err
	}
	leaseProjectionMode := leaseProjectionModeDisabled
	if value, present := os.LookupEnv("ROUTE_CONTROLLER_LEASE_PROJECTION_MODE"); present {
		if err := validateLeaseProjectionMode(value); err != nil {
			return config{}, err
		}
		leaseProjectionMode = value
	}
	scalePlanMode := scalePlanModeOff
	initialAssignmentMode := initialAssignmentModeOff
	if value, present := os.LookupEnv(initialAssignmentModeEnv); present {
		if err := validateInitialAssignmentMode(value); err != nil {
			return config{}, err
		}
		initialAssignmentMode = value
	}
	initialAssignmentTimeout, err := strictDurationEnv(initialAssignmentTimeoutEnv, defaultInitialAssignmentTimeout, maxInitialAssignmentTimeout)
	if err != nil {
		return config{}, err
	}
	initialAssignmentPlayerHolders, err := strictIntEnvAny([]string{initialAssignmentPlayerCountEnv, "ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_EXPECTED_PLAYER_HOLDER_COUNT", "ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_EXPECTED_PLAYER_COUNT", "ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_PLAYER_HOLDER_COUNT", "ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_PLAYER_COUNT", "ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_PLAYER_HOLDERS"}, defaultInitialAssignmentHolders, 1, maxInitialAssignmentHolders)
	if err != nil {
		return config{}, err
	}
	initialAssignmentFarmHolders, err := strictIntEnvAny([]string{initialAssignmentFarmCountEnv, "ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_EXPECTED_FARM_HOLDER_COUNT", "ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_EXPECTED_FARM_COUNT", "ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_FARM_HOLDER_COUNT", "ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_FARM_COUNT", "ROUTE_CONTROLLER_INITIAL_ASSIGNMENT_FARM_HOLDERS"}, defaultInitialAssignmentHolders, 1, maxInitialAssignmentHolders)
	if err != nil {
		return config{}, err
	}
	scalePlanParallelism, err := strictIntEnvAny([]string{"SCALEPLAN_ROUTEPLAN_PARALLELISM"}, defaultScalePlanParallelism, 1, int(scaleplancore.ProductionMaxRoutePlansInflight))
	if err != nil {
		return config{}, err
	}
	holderControlMaxClients, err := strictIntEnvAny([]string{"ROUTE_CONTROLLER_HOLDER_CONTROL_MAX_CLIENTS"}, defaultHolderControlMaxClients, 2, maxHolderControlMaxClients)
	if err != nil {
		return config{}, err
	}
	if value, present := os.LookupEnv("SCALEPLAN_MODE"); present {
		if err := validateScalePlanMode(value); err != nil {
			return config{}, err
		}
		scalePlanMode = value
	}
	fs := flag.NewFlagSet("route-controller", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var cfg config
	fs.StringVar(&cfg.listen, "listen", envOrEmpty("ROUTE_CONTROLLER_LISTEN"), "internal listen address")
	fs.StringVar(&cfg.controllerID, "controller-id", envOrEmpty("ROUTE_CONTROLLER_ID"), "controller identity")
	fs.StringVar(&cfg.boot, "boot", envOrEmpty("ROUTE_CONTROLLER_BOOT"), "controller boot identity")
	fs.StringVar(&cfg.realm, "realm", envOrEmpty("ROUTE_CONTROLLER_REALM"), "optional route realm pin")
	fs.StringVar(&cfg.redisURL, "redis-url", envOrEmpty("FARM_REDIS_URL"), "Redis connection URL")
	fs.StringVar(&cfg.redisClusterURLs, "redis-cluster-urls", envOrEmpty(redisdomains.LegacyClusterURLEnv), "comma-separated Redis Cluster seed URLs")
	fs.StringVar(&cfg.namespaceEnvironment, "redis-environment", os.Getenv(rediskey.RedisNamespaceEnvironmentEnv), "Redis namespace environment")
	fs.StringVar(&cfg.namespaceZone, "redis-zone", os.Getenv(rediskey.RedisNamespaceZoneEnv), "Redis namespace zone")
	fs.DurationVar(&cfg.redisOperation, "redis-operation-timeout", durationEnv("ROUTE_CONTROLLER_REDIS_OPERATION_TIMEOUT", defaultRedisTimeout), "bounded Redis operation timeout")
	fs.StringVar(&cfg.metricsListen, "metrics-listen", envOrEmpty("ROUTE_CONTROLLER_METRICS_LISTEN"), "optional Prometheus metrics listen address")
	fs.IntVar(&cfg.maxSnapshotBytes, "max-snapshot-bytes", intEnv("ROUTE_CONTROLLER_MAX_SNAPSHOT_BYTES", 4<<20), "maximum canonical snapshot bytes")
	fs.IntVar(&cfg.maxClients, "max-watch-clients", intEnv("ROUTE_CONTROLLER_MAX_WATCH_CLIENTS", 128), "maximum concurrent watch clients")
	fs.IntVar(&cfg.queueSize, "watch-queue-size", intEnv("ROUTE_CONTROLLER_WATCH_QUEUE_SIZE", 8), "per-watch bounded queue size")
	fs.IntVar(&cfg.maxMessageBytes, "max-message-bytes", intEnv("ROUTE_CONTROLLER_MAX_MESSAGE_BYTES", 4<<20), "maximum gRPC message bytes")
	fs.StringVar(&cfg.tlsCert, "tls-cert", envOrEmpty("ROUTE_CONTROLLER_TLS_CERT"), "server TLS certificate")
	fs.StringVar(&cfg.tlsKey, "tls-key", envOrEmpty("ROUTE_CONTROLLER_TLS_KEY"), "server TLS key")
	fs.StringVar(&cfg.tlsCA, "tls-ca", envOrEmpty("ROUTE_CONTROLLER_TLS_CA"), "client CA certificate")
	fs.StringVar(&cfg.holderClientCert, "holder-client-cert", envOrEmpty("ROUTE_CONTROLLER_HOLDER_CLIENT_CERT"), "holder-control client TLS certificate")
	fs.StringVar(&cfg.holderClientKey, "holder-client-key", envOrEmpty("ROUTE_CONTROLLER_HOLDER_CLIENT_KEY"), "holder-control client TLS key")
	fs.StringVar(&cfg.holderClientCA, "holder-client-ca", envOrEmpty("ROUTE_CONTROLLER_HOLDER_CLIENT_CA"), "holder-control server CA certificate")
	fs.StringVar(&cfg.holderDiscoveryMode, "holder-discovery-mode", stringOrDefault("HOLDER_DISCOVERY_MODE", holderDiscoveryModeLegacy), "holder discovery mode: legacy, shadow, k8s-canary or k8s")
	fs.StringVar(&cfg.holderDiscoveryAudience, "holder-discovery-audience", stringOrDefault("HOLDER_DISCOVERY_AUDIENCE", defaultHolderDiscoveryAudience), "bound service-account token audience")
	fs.DurationVar(&cfg.holderDiscoveryResync, "holder-discovery-resync", durationEnv("HOLDER_DISCOVERY_RESYNC", defaultHolderDiscoveryResync), "periodic exact holder reverification interval")
	fs.DurationVar(&cfg.holderDiscoveryVerifiedTTL, "holder-discovery-verified-ttl", durationEnv("HOLDER_DISCOVERY_VERIFIED_TTL", defaultHolderDiscoveryVerifiedTTL), "verified holder freshness lifetime")
	fs.IntVar(&cfg.holderDiscoveryPlayerLogicPort, "holder-discovery-player-logic-port", intEnv("HOLDER_DISCOVERY_PLAYER_LOGIC_PORT", defaultPlayerHolderLogicPort), "player holder logic port")
	fs.IntVar(&cfg.holderDiscoveryPlayerControlPort, "holder-discovery-player-control-port", intEnv("HOLDER_DISCOVERY_PLAYER_CONTROL_PORT", defaultPlayerHolderControlPort), "player holder-control port")
	fs.IntVar(&cfg.holderDiscoveryFarmLogicPort, "holder-discovery-farm-logic-port", intEnv("HOLDER_DISCOVERY_FARM_LOGIC_PORT", defaultFarmHolderLogicPort), "farm holder logic port")
	fs.IntVar(&cfg.holderDiscoveryFarmControlPort, "holder-discovery-farm-control-port", intEnv("HOLDER_DISCOVERY_FARM_CONTROL_PORT", defaultFarmHolderControlPort), "farm holder-control port")
	fs.StringVar(&cfg.holderDiscoveryPlayerTLSIdentity, "holder-discovery-player-tls-identity", envOrEmpty("HOLDER_DISCOVERY_PLAYER_TLS_IDENTITY"), "exact player holder-control TLS DNS identity")
	fs.StringVar(&cfg.holderDiscoveryFarmTLSIdentity, "holder-discovery-farm-tls-identity", envOrEmpty("HOLDER_DISCOVERY_FARM_TLS_IDENTITY"), "exact farm holder-control TLS DNS identity")
	fs.StringVar(&cfg.gateVerifierClientCert, "gate-verifier-client-cert", envOrEmpty("ROUTE_CONTROLLER_GATE_VERIFIER_CLIENT_CERT"), "Gate verifier client TLS certificate")
	fs.StringVar(&cfg.gateVerifierClientKey, "gate-verifier-client-key", envOrEmpty("ROUTE_CONTROLLER_GATE_VERIFIER_CLIENT_KEY"), "Gate verifier client TLS key")
	fs.StringVar(&cfg.gateVerifierClientCA, "gate-verifier-client-ca", envOrEmpty("ROUTE_CONTROLLER_GATE_VERIFIER_CLIENT_CA"), "Gate verifier server CA certificate")
	fs.DurationVar(&cfg.gateVerifierTimeout, "gate-verifier-timeout", gateVerifierTimeout, "bounded per-Gate verification RPC timeout")
	fs.DurationVar(&cfg.planOperation, "plan-operation-timeout", durationEnv("ROUTE_CONTROLLER_PLAN_OPERATION_TIMEOUT", defaultPlanOperationTimeout), "bounded RoutePlan operation timeout")
	fs.DurationVar(&cfg.planTTL, "plan-ttl", planTTL, "bounded RoutePlan logical expiry horizon")
	fs.StringVar(&cfg.scalePlanMode, "scaleplan-mode", scalePlanMode, "ScalePlan runtime mode: off, manual or auto")
	fs.IntVar(&cfg.scalePlanRoutePlanParallelism, "scaleplan-routeplan-parallelism", scalePlanParallelism, "persisted ScalePlan RoutePlan concurrency profile: 1, 8, 16, or 32")
	fs.IntVar(&cfg.holderControlMaxClients, "holder-control-max-clients", holderControlMaxClients, "maximum pooled HolderControl client connections")
	fs.StringVar(&cfg.initialAssignmentMode, "initial-assignment-mode", initialAssignmentMode, "initial PLAYER/FARM assignment runtime mode: off or auto")
	fs.DurationVar(&cfg.initialAssignmentTimeout, "initial-assignment-timeout", initialAssignmentTimeout, "bounded initial PLAYER/FARM assignment wait/claim timeout")
	fs.IntVar(&cfg.initialAssignmentExpectedPlayerHolders, "initial-assignment-expected-player-holders", initialAssignmentPlayerHolders, "expected initial eligible PLAYER holder count")
	fs.IntVar(&cfg.initialAssignmentExpectedPlayerHolders, "initial-assignment-player-holder-count", initialAssignmentPlayerHolders, "alias for expected initial PLAYER holder count")
	fs.IntVar(&cfg.initialAssignmentExpectedPlayerHolders, "initial-assignment-player-holders", initialAssignmentPlayerHolders, "alias for expected initial PLAYER holder count")
	fs.IntVar(&cfg.initialAssignmentExpectedPlayerHolders, "initial-assignment-player-count", initialAssignmentPlayerHolders, "alias for expected initial PLAYER holder count")
	fs.IntVar(&cfg.initialAssignmentExpectedFarmHolders, "initial-assignment-expected-farm-holders", initialAssignmentFarmHolders, "expected initial eligible FARM holder count")
	fs.IntVar(&cfg.initialAssignmentExpectedFarmHolders, "initial-assignment-farm-holder-count", initialAssignmentFarmHolders, "alias for expected initial FARM holder count")
	fs.IntVar(&cfg.initialAssignmentExpectedFarmHolders, "initial-assignment-farm-holders", initialAssignmentFarmHolders, "alias for expected initial FARM holder count")
	fs.IntVar(&cfg.initialAssignmentExpectedFarmHolders, "initial-assignment-farm-count", initialAssignmentFarmHolders, "alias for expected initial FARM holder count")
	fs.StringVar(&cfg.scalePlanPlayerNamespace, "scaleplan-player-namespace", envOrEmpty("SCALEPLAN_PLAYER_NAMESPACE"), "ScalePlan player StatefulSet namespace")
	fs.StringVar(&cfg.scalePlanPlayerStatefulSet, "scaleplan-player-statefulset", envOrEmpty("SCALEPLAN_PLAYER_STATEFULSET"), "ScalePlan player StatefulSet name")
	fs.StringVar(&cfg.scalePlanFarmNamespace, "scaleplan-farm-namespace", envOrEmpty("SCALEPLAN_FARM_NAMESPACE"), "ScalePlan farm StatefulSet namespace")
	fs.StringVar(&cfg.scalePlanFarmStatefulSet, "scaleplan-farm-statefulset", envOrEmpty("SCALEPLAN_FARM_STATEFULSET"), "ScalePlan farm StatefulSet name")
	fs.DurationVar(&cfg.scalePlanAutoInterval, "scaleplan-auto-interval", durationEnv("SCALEPLAN_AUTO_RECONCILE_INTERVAL", 5*time.Second), "periodic auto-rebalance recovery interval")
	fs.StringVar(&cfg.scalePlanSafeDesiredAnnotation, "scaleplan-safe-desired-annotation", stringOrDefault("SCALEPLAN_SAFE_DESIRED_ANNOTATION", "scaleplan.farm/safe-desired-replicas"), "StatefulSet annotation carrying the safe controller-owned replica target")
	fs.BoolVar(&cfg.scalePlanRolloutEnabled, "scaleplan-rollout-enabled", rolloutEnabled, "enable safe OnDelete StatefulSet holder rollout")
	fs.StringVar(&cfg.scalePlanRolloutAnnotation, "scaleplan-rollout-annotation", stringOrDefault("SCALEPLAN_ROLLOUT_ANNOTATION", "scaleplan.farm/managed-rollout-state"), "StatefulSet annotation carrying the restartable rollout checkpoint")
	fs.StringVar(&cfg.scalePlanRolloutIntentAnnotation, "scaleplan-rollout-intent-annotation", stringOrDefault("SCALEPLAN_ROLLOUT_INTENT_ANNOTATION", "scaleplan.farm/rollout-intent"), "StatefulSet annotation carrying the explicit immutable rollout intent")
	fs.DurationVar(&cfg.routeFailoverInterval, "route-failover-interval", durationEnv("ROUTE_FAILOVER_RECONCILE_INTERVAL", defaultRouteFailoverInterval), "automatic crashed-holder recovery interval")
	fs.IntVar(&cfg.routeFailoverMaxPlans, "route-failover-max-plans", intEnv("ROUTE_FAILOVER_MAX_PLANS_PER_PASS", defaultRouteFailoverMaxPlans), "maximum concurrent crash-takeover Shard slots")
	fs.IntVar(&cfg.routeFailoverMaxAuthorityReads, "route-failover-max-authority-reads", intEnv("ROUTE_FAILOVER_MAX_AUTHORITY_READS_PER_PASS", defaultRouteFailoverMaxReads), "maximum exact Authority reads per failover pass")
	fs.StringVar(&cfg.leaseProjectionMode, "lease-projection-mode", leaseProjectionMode, "complete Authority lease projection mode: disabled or all-active")
	fs.DurationVar(&cfg.leaseProjectionInterval, "lease-projection-interval", durationEnv("ROUTE_CONTROLLER_LEASE_PROJECTION_INTERVAL", defaultLeaseProjectionInterval), "complete authority lease projection interval")
	fs.DurationVar(&cfg.leaseProjectionMinimum, "lease-projection-minimum-remaining", durationEnv("ROUTE_CONTROLLER_LEASE_PROJECTION_MINIMUM_REMAINING", defaultLeaseProjectionMinimum), "minimum admitted projected lease budget")
	fs.DurationVar(&cfg.leaseProjectionTimeout, "lease-projection-wave-timeout", durationEnv("ROUTE_CONTROLLER_LEASE_PROJECTION_WAVE_TIMEOUT", defaultLeaseProjectionWaveTimeout), "whole all-shard projection wave timeout")
	fs.StringVar(&cfg.healthListen, "health-listen", stringOrDefault("ROUTE_CONTROLLER_HEALTH_LISTEN", ":8081"), "health and freshness readiness listen address")
	fs.StringVar(&cfg.deadlineRefreshMode, "deadline-refresh-mode", deadlineRefreshMode, "deadline refresh mode: snapshot or authority-delegated")
	fs.DurationVar(&cfg.deadlineRefreshInterval, "deadline-refresh-interval", deadlineRefreshInterval, "bounded ACTIVE Authority deadline reconciliation cadence")
	fs.DurationVar(&cfg.deadlineRefreshLead, "deadline-refresh-lead", deadlineRefreshLead, "minimum projected lease lead that triggers complete Snapshot reconciliation")
	fs.StringVar(&cfg.physicalProfile, "redis-physical-profile", os.Getenv(rediskey.RedisPhysicalProfileEnv), "immutable Redis physical profile")
	fs.StringVar(&cfg.authorityLayout, "authority-layout-version", os.Getenv(rediskey.RedisAuthorityLayoutEnv), "immutable authority layout version")
	fs.StringVar(&cfg.workflowLayout, "workflow-layout-version", os.Getenv(rediskey.RedisWorkflowLayoutEnv), "immutable workflow layout version")
	fs.StringVar(&cfg.workflowHashVersion, "workflow-hash-version", os.Getenv(rediskey.RedisWorkflowHashEnv), "immutable workflow hash version")
	fs.StringVar(&cfg.workflowBucketCount, "workflow-bucket-count", os.Getenv(rediskey.RedisWorkflowBucketCountEnv), "immutable workflow bucket count")
	fs.StringVar(&cfg.singleControllerContract, "single-controller-attestation", envOrEmpty("ROUTE_CONTROLLER_SINGLE_CONTROLLER_ATTESTATION"), "required immutable single-replica Recreate attestation")
	var allowed stringSliceFlag
	if value := os.Getenv("ROUTE_CONTROLLER_ALLOWED_CLIENT_IDENTITIES"); value != "" {
		allowed = stringSliceFlag(splitComma(value))
	}
	fs.Var(&allowed, "allowed-client-identity", "required client certificate identity; repeat or comma-separate")
	var planReaders stringSliceFlag
	if value := os.Getenv("ROUTE_CONTROLLER_PLAN_READ_IDENTITIES"); value != "" {
		planReaders = stringSliceFlag(splitComma(value))
	}
	fs.Var(&planReaders, "plan-read-identity", "Plan read-only client identity; repeat or comma-separate")
	var operators stringSliceFlag
	if value := os.Getenv("ROUTE_CONTROLLER_OPERATOR_IDENTITIES"); value != "" {
		operators = stringSliceFlag(splitComma(value))
	}
	fs.Var(&operators, "operator-identity", "required Plan mutation operator identity; repeat or comma-separate")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() != 0 {
		return config{}, errors.New("route-controller: unexpected positional arguments")
	}
	if cfg.gateVerifierTimeout <= 0 || cfg.gateVerifierTimeout > 30*time.Second {
		return config{}, errors.New("route-controller: Gate verifier timeout must be greater than zero and at most 30s")
	}
	if err := validateLeaseProjectionMode(cfg.leaseProjectionMode); err != nil {
		return config{}, err
	}
	if err := validateDeadlineRefreshMode(cfg.deadlineRefreshMode); err != nil {
		return config{}, err
	}
	if err := validateScalePlanMode(cfg.scalePlanMode); err != nil {
		return config{}, err
	}
	if _, err := scaleplancore.FunctionalLimitsForParallelism(uint32(cfg.scalePlanRoutePlanParallelism)); err != nil {
		return config{}, fmt.Errorf("route-controller: ScalePlan RoutePlan parallelism: %w", err)
	}
	if cfg.holderControlMaxClients < 2 || cfg.holderControlMaxClients > maxHolderControlMaxClients {
		return config{}, fmt.Errorf("route-controller: HolderControl max clients must be between 2 and %d", maxHolderControlMaxClients)
	}
	if cfg.initialAssignmentTimeout <= 0 || cfg.initialAssignmentTimeout > maxInitialAssignmentTimeout {
		return config{}, fmt.Errorf("route-controller: initial assignment timeout must be greater than zero and at most %s", maxInitialAssignmentTimeout)
	}
	if cfg.initialAssignmentExpectedPlayerHolders <= 0 || cfg.initialAssignmentExpectedPlayerHolders > maxInitialAssignmentHolders ||
		cfg.initialAssignmentExpectedFarmHolders <= 0 || cfg.initialAssignmentExpectedFarmHolders > maxInitialAssignmentHolders {
		return config{}, fmt.Errorf("route-controller: initial assignment expected holder counts must be between 1 and %d", maxInitialAssignmentHolders)
	}
	if err := validateInitialAssignmentConfig(cfg); err != nil {
		return config{}, err
	}
	if err := validateHolderDiscoveryMode(cfg.holderDiscoveryMode); err != nil {
		return config{}, err
	}
	if err := validatePlanTiming(cfg.planOperation, cfg.planTTL); err != nil {
		return config{}, err
	}
	if err := validateDeadlineRefreshTiming(cfg.deadlineRefreshInterval, cfg.deadlineRefreshLead, cfg.redisOperation); err != nil {
		return config{}, err
	}
	peers, err := parseGateVerifierPeers(envOrEmpty("ROUTE_CONTROLLER_GATE_VERIFIER_PEERS"))
	if err != nil {
		return config{}, err
	}
	cfg.gateVerifierPeers = peers
	cfg.initialAssignmentPlayerHolders = cfg.initialAssignmentExpectedPlayerHolders
	cfg.initialAssignmentFarmHolders = cfg.initialAssignmentExpectedFarmHolders
	cfg.allowedClientIdentity = append([]string(nil), allowed...)
	cfg.operatorIdentity = append([]string(nil), operators...)
	if len(planReaders) == 0 {
		cfg.planReadIdentity = append([]string(nil), operators...)
	} else {
		cfg.planReadIdentity = append([]string(nil), planReaders...)
	}
	return cfg, nil
}

func effectiveScalePlanParallelism(value int) int {
	if value == 0 {
		return defaultScalePlanParallelism
	}
	return value
}

func (c config) validate() error {
	required := map[string]string{
		"listen": c.listen, "controller id": c.controllerID, "boot": c.boot,
		"Redis namespace environment": c.namespaceEnvironment,
		"Redis namespace zone":        c.namespaceZone, "TLS certificate": c.tlsCert,
		"TLS key": c.tlsKey, "TLS CA": c.tlsCA,
		"Redis physical profile": c.physicalProfile, "Redis authority layout": c.authorityLayout,
		"Redis workflow layout": c.workflowLayout, "Redis workflow hash version": c.workflowHashVersion,
		"Redis workflow bucket count":      c.workflowBucketCount,
		"Gate verifier client certificate": c.gateVerifierClientCert,
		"Gate verifier client key":         c.gateVerifierClientKey,
		"Gate verifier client CA":          c.gateVerifierClientCA,
	}
	for name, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("route-controller: %s is required", name)
		}
	}
	if strings.TrimSpace(c.redisURL) == "" && strings.TrimSpace(c.redisClusterURLs) == "" {
		return errors.New("route-controller: Redis URL or Redis Cluster seed URLs are required")
	}
	if _, err := transport.ValidateAllowedClientIdentities(c.allowedClientIdentity); err != nil {
		return fmt.Errorf("route-controller: allowed client identity: %w", err)
	}
	if _, err := transport.ValidateAllowedClientIdentities(c.operatorIdentity); err != nil {
		return fmt.Errorf("route-controller: operator identity: %w", err)
	}
	if _, err := transport.ValidateAllowedClientIdentities(c.planReadIdentity); err != nil {
		return fmt.Errorf("route-controller: plan read identity: %w", err)
	}
	if c.singleControllerContract != routeplan.SingleControllerContract {
		return errors.New("route-controller: explicit single-controller attestation is required")
	}
	planOperation := effectivePlanOperationTimeout(c.planOperation)
	if err := validateLeaseProjectionMode(c.leaseProjectionMode); err != nil {
		return err
	}
	if err := validateDeadlineRefreshMode(c.deadlineRefreshMode); err != nil {
		return err
	}
	projectionInterval, projectionMinimum, projectionTimeout := effectiveLeaseProjectionConfig(c)
	if c.redisOperation <= 0 || c.maxSnapshotBytes <= 0 || c.maxClients <= 0 ||
		c.queueSize <= 0 || c.maxMessageBytes <= 0 || planOperation <= 0 || c.gateVerifierTimeout <= 0 ||
		c.gateVerifierTimeout > 30*time.Second || projectionInterval <= 0 || projectionInterval > time.Minute ||
		projectionMinimum <= 0 || projectionMinimum > time.Minute || projectionTimeout <= 0 || projectionTimeout > 2*time.Minute ||
		len(c.gateVerifierPeers) == 0 || len(c.gateVerifierPeers) > gateverification.MaxPeers {
		return errors.New("route-controller: Redis and transport bounds must be positive")
	}
	if err := validatePlanTiming(planOperation, c.planTTL); err != nil {
		return err
	}
	if err := validateDeadlineRefreshTiming(effectiveDeadlineRefreshInterval(c.deadlineRefreshInterval), effectiveDeadlineRefreshLead(c.deadlineRefreshLead), c.redisOperation); err != nil {
		return err
	}
	if _, err := c.keyspaceTuple(); err != nil {
		return errors.New("route-controller: Redis keyspace tuple is invalid")
	}
	if err := validateInitialAssignmentConfig(c); err != nil {
		return err
	}
	if err := validateScalePlanConfig(c); err != nil {
		return err
	}
	if err := validateHolderDiscoveryConfig(c); err != nil {
		return err
	}
	return nil
}

func parseGateVerifierPeers(raw string) ([]gateverification.Peer, error) {
	if raw == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var peers []gateverification.Peer
	if err := decoder.Decode(&peers); err != nil {
		return nil, errors.New("route-controller: invalid Gate verifier peers")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("route-controller: invalid Gate verifier peers")
	}
	if len(peers) == 0 || len(peers) > gateverification.MaxPeers {
		return nil, errors.New("route-controller: invalid Gate verifier peer count")
	}
	return peers, nil
}

func (c config) keyspaceTuple() (rediskey.KeyspaceTuple, error) {
	return rediskey.ParseKeyspaceTuple(rediskey.KeyspaceTupleInput{
		NamespaceEnvironment: c.namespaceEnvironment,
		NamespaceZone:        c.namespaceZone,
		PhysicalProfile:      c.physicalProfile,
		AuthorityLayout:      c.authorityLayout,
		WorkflowLayout:       c.workflowLayout,
		WorkflowHashVersion:  c.workflowHashVersion,
		WorkflowBucketCount:  c.workflowBucketCount,
	})
}

type stringSliceFlag []string

func (s *stringSliceFlag) String() string {
	if s == nil {
		return ""
	}
	return strings.Join(*s, ",")
}

func (s *stringSliceFlag) Set(value string) error {
	*s = append(*s, splitComma(value)...)
	return nil
}

func splitComma(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		result = append(result, part)
	}
	return result
}

func envOrEmpty(name string) string {
	return strings.TrimSpace(os.Getenv(name))
}

func stringOrDefault(name, fallback string) string {
	if value := envOrEmpty(name); value != "" {
		return value
	}
	return fallback
}

func strictBoolEnv(name string, fallback bool) (bool, error) {
	value, found := os.LookupEnv(name)
	if !found {
		return fallback, nil
	}
	if value != "true" && value != "false" {
		return false, fmt.Errorf("route-controller: %s must be true or false", name)
	}
	return value == "true", nil
}

func intEnv(name string, fallback int) int {
	value := envOrEmpty(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func durationEnv(name string, fallback time.Duration) time.Duration {
	value := envOrEmpty(name)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func strictDurationEnv(name string, fallback, maximum time.Duration) (time.Duration, error) {
	value, present := os.LookupEnv(name)
	if !present {
		return fallback, nil
	}
	if value == "" || strings.TrimSpace(value) != value {
		return 0, fmt.Errorf("route-controller: %s must be a canonical duration", name)
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 || parsed > maximum {
		return 0, fmt.Errorf("route-controller: %s must be greater than zero and at most %s", name, maximum)
	}
	return parsed, nil
}

func strictIntEnvAny(names []string, fallback, minimum, maximum int) (int, error) {
	for _, name := range names {
		if value, present := os.LookupEnv(name); present {
			if value == "" || strings.TrimSpace(value) != value {
				return 0, fmt.Errorf("route-controller: %s must be a canonical integer", name)
			}
			parsed, err := strconv.Atoi(value)
			if err != nil || parsed < minimum || parsed > maximum {
				return 0, fmt.Errorf("route-controller: %s must be between %d and %d", name, minimum, maximum)
			}
			return parsed, nil
		}
	}
	return fallback, nil
}

func validateLeaseProjectionMode(mode string) error {
	if mode != leaseProjectionModeDisabled && mode != leaseProjectionModeAllActive {
		return errors.New("route-controller: lease projection mode must be disabled or all-active")
	}
	return nil
}

func validateDeadlineRefreshMode(mode string) error {
	if mode == "" {
		return nil
	}
	if mode != deadlineRefreshModeSnapshot && mode != deadlineRefreshModeAuthority {
		return errors.New("route-controller: deadline refresh mode must be snapshot or authority-delegated")
	}
	return nil
}

func effectiveLeaseProjectionConfig(cfg config) (time.Duration, time.Duration, time.Duration) {
	interval, minimum, timeout := cfg.leaseProjectionInterval, cfg.leaseProjectionMinimum, cfg.leaseProjectionTimeout
	if interval == 0 {
		interval = defaultLeaseProjectionInterval
	}
	if minimum == 0 {
		minimum = defaultLeaseProjectionMinimum
	}
	if timeout == 0 {
		timeout = defaultLeaseProjectionWaveTimeout
	}
	return interval, minimum, timeout
}

func effectivePlanOperationTimeout(value time.Duration) time.Duration {
	if value == 0 {
		return defaultPlanOperationTimeout
	}
	return value
}

func validatePlanTiming(operationTimeout, planTTL time.Duration) error {
	operationTimeout = effectivePlanOperationTimeout(operationTimeout)
	if operationTimeout <= 0 {
		return errors.New("route-controller: Plan operation timeout must be greater than zero")
	}
	required, ok := checkedAddDuration(operationTimeout, planExecutorTTLSafety)
	if !ok {
		return errors.New("route-controller: Plan operation timeout plus executor safety overflows")
	}
	if operationTimeout > maxPlanOperationTimeout {
		return fmt.Errorf("route-controller: Plan operation timeout must be at most %s", maxPlanOperationTimeout)
	}
	minimumRecoveryBudget, ok := checkedAddDuration(targetSnapshotConvergenceTimeout, recoveryStartupHeadroom)
	if !ok || operationTimeout < minimumRecoveryBudget {
		return fmt.Errorf("route-controller: Plan operation timeout must be at least %s for pending projection recovery", minimumRecoveryBudget)
	}
	if planTTL <= 0 || planTTL > maxPlanTTL {
		return fmt.Errorf("route-controller: Plan TTL must be greater than zero and at most %s", maxPlanTTL)
	}
	requiredMilliseconds, ok := ceilDurationMilliseconds(required)
	if !ok || planTTL.Milliseconds() <= requiredMilliseconds {
		return errors.New("route-controller: Plan TTL must exceed the Plan operation timeout plus 1s executor safety at millisecond precision")
	}
	return nil
}

func effectiveDeadlineRefreshInterval(value time.Duration) time.Duration {
	if value == 0 {
		return defaultDeadlineRefreshInterval
	}
	return value
}

func effectiveDeadlineRefreshLead(value time.Duration) time.Duration {
	if value == 0 {
		return defaultDeadlineRefreshLead
	}
	return value
}

func validateDeadlineRefreshTiming(interval, lead, redisOperation time.Duration) error {
	if interval <= 0 || interval > maxDeadlineRefreshInterval {
		return fmt.Errorf("route-controller: deadline refresh interval must be greater than zero and at most %s", maxDeadlineRefreshInterval)
	}
	if lead <= 0 || lead > maxDeadlineRefreshLead {
		return fmt.Errorf("route-controller: deadline refresh lead must be greater than zero and at most %s", maxDeadlineRefreshLead)
	}
	if redisOperation <= 0 {
		return errors.New("route-controller: Redis operation timeout must be greater than zero")
	}
	minimumLead, ok := checkedAddDuration(interval, redisOperation)
	if !ok {
		return errors.New("route-controller: deadline refresh timing overflows")
	}
	minimumLead, ok = checkedAddDuration(minimumLead, deadlineRefreshApplyHeadroom)
	if !ok || lead <= minimumLead {
		return errors.New("route-controller: deadline refresh lead must exceed interval plus Redis operation timeout plus apply headroom")
	}
	return nil
}

func checkedAddDuration(left, right time.Duration) (time.Duration, bool) {
	const (
		maxDuration = time.Duration(1<<63 - 1)
		minDuration = -maxDuration - 1
	)
	if (right > 0 && left > maxDuration-right) || (right < 0 && left < minDuration-right) {
		return 0, false
	}
	return left + right, true
}

func ceilDurationMilliseconds(value time.Duration) (int64, bool) {
	if value < 0 {
		return 0, false
	}
	milliseconds := int64(value / time.Millisecond)
	if value%time.Millisecond == 0 {
		return milliseconds, true
	}
	if milliseconds == int64(^uint64(0)>>1) {
		return 0, false
	}
	return milliseconds + 1, true
}
