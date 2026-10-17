// Command farm-gate 是 farm-game 的应用接入进程(S2 阶段)。
//
// 本入口实现并暴露登录模块的最小可运行服务:
//   - GET  /version                      打印构建版本
//   - POST /v1/auth/login                HTTPS 测试登录 → 会话代次 → 建档 → 一次性票据
//   - GATEPROXY_SOCKET enables the in-process GateStream transport.
//
// 凭据通过环境变量注入;未配置 Redis 时使用内存实现(单节点本地开发/演示)。
package gatewayapp

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"farmgame"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/authoritycomposition"
	"farmgame/internal/friendshipservice"
	"farmgame/internal/gateway/account"
	"farmgame/internal/gateway/auth"
	"farmgame/internal/gateway/auth/ticket"
	"farmgame/internal/gateway/bootlease"
	"farmgame/internal/gateway/command"
	"farmgame/internal/gateway/commandcatalog"
	"farmgame/internal/gateway/downlink"
	"farmgame/internal/gateway/leasebatch"
	"farmgame/internal/gateway/preauth"
	"farmgame/internal/gateway/session"
	"farmgame/internal/gateway/transport/gateproxy"
	wstransport "farmgame/internal/gateway/transport/websocket"
	"farmgame/internal/grpcmetrics"
	"farmgame/internal/observability/redisworkload"
	"farmgame/internal/permitsecret"
	"farmgame/internal/publicprofile"
	"farmgame/internal/redisdomains"
	"farmgame/internal/socialprofile"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"git.internal.example.invalid/mfcn/gateproxy/go-client/pkg/proto/proxypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

type appProfile string

const (
	appProfileLocal    appProfile = "local"
	appProfileTest     appProfile = "test"
	appProfileShowcase appProfile = "showcase"
)

func parseAppProfile(raw string) (appProfile, error) {
	switch value := strings.ToLower(strings.TrimSpace(raw)); value {
	case "", string(appProfileLocal):
		return appProfileLocal, nil
	case string(appProfileTest):
		return appProfileTest, nil
	case string(appProfileShowcase):
		return appProfileShowcase, nil
	default:
		return appProfile(value), fmt.Errorf("FARM_APP_PROFILE must be one of local, test, showcase; got %q", raw)
	}
}

func (p appProfile) allowsTestAuth() bool {
	return p == appProfileLocal || p == appProfileTest
}

func (p appProfile) isShowcase() bool { return p == appProfileShowcase }

// demoVerifier 是 S2 演示用平台票据校验:固定 token → 固定 player_id。
// 本地双浏览器联调可配置第二组 token/player_id；生产必须替换为真实平台校验。
// 生产应替换为真实平台 SSO/票据校验(见 docs/design/41-sequences.md §1)。
type demoVerifier struct {
	token             string
	playerID          uint64
	secondaryToken    string
	secondaryPlayerID uint64
	loadTestSecret    []byte
}

func (v *demoVerifier) Verify(_ context.Context, token string) (uint64, error) {
	switch {
	case token == v.token:
		return v.playerID, nil
	case v.secondaryToken != "" && v.secondaryPlayerID != 0 && token == v.secondaryToken:
		return v.secondaryPlayerID, nil
	case len(v.loadTestSecret) > 0:
		parts := strings.Split(token, ":")
		if len(parts) != 3 || parts[0] != "load" {
			return 0, errBadToken
		}
		playerID, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil || playerID == 0 {
			return 0, errBadToken
		}
		signature, err := hex.DecodeString(parts[2])
		if err != nil {
			return 0, errBadToken
		}
		mac := hmac.New(sha256.New, v.loadTestSecret)
		_, _ = mac.Write([]byte(parts[1]))
		if !hmac.Equal(signature, mac.Sum(nil)) {
			return 0, errBadToken
		}
		return playerID, nil
	default:
		return 0, errBadToken
	}
}

var errBadToken = credentialError("bad platform token")

type disabledPlatformVerifier struct{}

func (disabledPlatformVerifier) Verify(context.Context, string) (uint64, error) {
	return 0, credentialError("platform token login is disabled for showcase")
}

type credentialError string

func (e credentialError) Error() string { return string(e) }

// RunOptions is the minimal construction seam for a process that already
// owns a complete local assignment composition. The gateway consumes the
// supplied resolver and endpoint table; it never creates a second remote
// source, client, provider, or Pump.
type RunOptions struct {
	Assignment *authoritycomposition.AssignmentComposition
	// RPCMetrics may be constructed by the process entrypoint before the
	// dynamic endpoint pool is configured, so pooled holder dials and ordinary
	// Gate dials share one registered client-metrics collection.
	RPCMetrics *grpcmetrics.Metrics
}

func Run() {
	RunWithOptions(RunOptions{})
}

type gateRedisScriptLoader interface {
	EnsureScriptsLoaded(context.Context) error
}

func preloadGateRedisScripts(ctx context.Context, loaders ...gateRedisScriptLoader) error {
	for index, loader := range loaders {
		if loader == nil {
			return fmt.Errorf("Gate Redis script family %d is unavailable", index)
		}
		if err := loader.EnsureScriptsLoaded(ctx); err != nil {
			return fmt.Errorf("Gate Redis script family %d: %w", index, err)
		}
	}
	return nil
}

func RunWithOptions(options RunOptions) {
	log.Printf("farm-gate starting, version=%s", farmgame.FullVersion())

	cfg := loadConfig()
	if err := cfg.validateProfile(); err != nil {
		log.Fatalf("validate application profile: %v", err)
	}
	friendshipMode, err := friendshipservice.ModeFromEnvironment(os.LookupEnv)
	if err != nil {
		log.Fatalf("validate FriendshipService mode: %v", err)
	}
	log.Printf("FriendshipService mode=%s", friendshipMode)
	leaseRenewalCfg, err := cfg.leaseRenewalConfig()
	if err != nil {
		log.Fatalf("validate lease renewal batching: %v", err)
	}
	permitConfig, err := permitsecret.Load(os.Getenv)
	if err != nil {
		log.Fatalf("validate permit secret configuration: %v", err)
	}
	log.Printf("permit secret policy=%s insecure_development=%t development_switch=%t",
		permitConfig.Policy(), permitConfig.InsecureDevelopment(), permitConfig.DevelopmentSwitchEnabled())
	if permitConfig.InsecureDevelopment() {
		log.Printf("WARNING: insecure permit secret development mode is active; never use this composition for production traffic")
	}
	permitSecret := permitConfig.Secret()
	redisOpenCtx, redisOpenCancel := context.WithTimeout(context.Background(), 5*time.Second)
	clients, err := redisdomains.OpenFromEnv(redisOpenCtx, os.Getenv,
		redisdomains.SelectAuthority|redisdomains.SelectRuntime|redisdomains.SelectProjection)
	redisOpenCancel()
	if err != nil {
		log.Fatal(err)
	}
	defer clients.Close()
	authorityClient := clients.Client(redisdomains.Authority)
	runtimeClient := clients.Client(redisdomains.Runtime)
	projectionClient := clients.Client(redisdomains.Projection)
	if authorityClient == nil || runtimeClient == nil || projectionClient == nil {
		log.Fatal("redisdomains: selected domain client is missing")
	}
	if _, err := redisworkload.AttachTargetsFromEnvironment(redisworkload.DependencyTargets{
		Authority: authorityClient, Runtime: runtimeClient, Projection: projectionClient,
	}, prometheus.DefaultRegisterer, os.Getenv("REDIS_WORKLOAD_METRICS")); err != nil {
		log.Fatalf("attach Redis workload metrics: %v", err)
	}
	if _, err := redisworkload.AttachDependencyHealth(prometheus.DefaultRegisterer, redisworkload.DependencyTargets{
		Authority:  authorityClient,
		Runtime:    runtimeClient,
		Projection: projectionClient,
	}); err != nil {
		log.Fatalf("attach Redis dependency health: %v", err)
	}
	epochCacheTTL := parseDurationEnv("GATE_SESSION_EPOCH_CACHE_TTL", "0s")
	if epochCacheTTL > 0 {
		log.Printf("WARNING: GATE_SESSION_EPOCH_CACHE_TTL=%s caches per-player session epochs and relaxes immediate cross-gate fencing; use only in isolated load tests", epochCacheTTL)
	}
	redisSessionAuthority := session.NewRedis(runtimeClient, session.Config{
		Secret:         permitSecret,
		AuthorityEpoch: 1,
		EpochCacheTTL:  epochCacheTTL,
	})
	var authority session.Authority = redisSessionAuthority
	redisTicketStore := ticket.NewRedisStore(runtimeClient)
	redisTicketStore.SetMetrics(ticket.NewMetrics(nil))
	var ticketStore ticket.Store = redisTicketStore
	webAccountStore, err := account.NewRedisStore(authorityClient, cfg.webAccountStorePrefix, cfg.webAccountPlayerIDBase)
	if err != nil {
		log.Fatalf("build persistent Redis web account store: %v", err)
	}
	log.Println("using authority Redis for account credentials; runtime Redis for session authority and connection tickets")

	var logicClient command.LogicClient
	tickets := ticket.New(ticketStore, ticket.WithTTL(cfg.ticketTTL))
	cookieActiveVersion, err := parseCookieActiveVersion(cfg.httpCookieActiveVersion)
	if err != nil {
		log.Fatalf("parse active HTTP session Cookie key version: %v", err)
	}
	cookieKeyRing, err := parseCookieKeyRing(cfg.httpCookieKeyRing)
	if err != nil {
		log.Fatalf("parse HTTP session Cookie key ring: %v", err)
	}

	var platformVerifier auth.PlatformVerifier = disabledPlatformVerifier{}
	if cfg.profile.allowsTestAuth() {
		platformVerifier = &demoVerifier{
			token:             cfg.demoToken,
			playerID:          cfg.demoPlayerID,
			secondaryToken:    cfg.demoSecondaryToken,
			secondaryPlayerID: cfg.demoSecondaryPlayerID,
			loadTestSecret:    []byte(cfg.loadTestTokenSecret),
		}
	}
	svc, err := auth.New(
		platformVerifier,
		authority,
		tickets,
		auth.Config{
			GatewayID:          cfg.gatewayID,
			WssEndpoint:        cfg.wssEndpoint,
			InitialRuleVersion: cfg.initialRuleVersion,
			TicketTTL:          cfg.ticketTTL,
			HTTPSessionTTL:     cfg.httpSessionTTL,
			PermitSecret:       permitSecret,
			RouteLeaseTTL:      cfg.routeLeaseTTL,
			CookieKeyVersion:   cookieActiveVersion,
			CookieKeyRing:      cookieKeyRing,
		},
	)
	if err != nil {
		log.Fatalf("build login service: %v", err)
	}
	webAccounts, err := account.NewService(webAccountStore, account.Config{
		PBKDF2Iterations: cfg.webAccountPBKDF2Iterations,
		MaxFailures:      cfg.webAccountMaxFailures,
		FailureWindow:    cfg.webAccountFailureWindow,
		LockDuration:     cfg.webAccountLockDuration,
	})
	if err != nil {
		log.Fatalf("build web account service: %v", err)
	}
	attemptHashSecret := []byte(cfg.webLoginRateLimitSecret)
	if len(attemptHashSecret) == 0 {
		// Backward-compatible purpose separation. Production may inject a
		// dedicated secret; its raw value is never logged or stored in Redis.
		attemptHashSecret = append([]byte(nil), permitSecret...)
	} else if len(attemptHashSecret) < 32 {
		log.Fatal("WEB_LOGIN_RATE_LIMIT_SECRET must contain at least 32 bytes")
	}
	attemptLimiterConfig := account.AttemptLimiterConfig{
		Prefix:  cfg.webLoginRateLimitPrefix,
		HashKey: account.DeriveAttemptHashKey(attemptHashSecret),
		Account: account.LimitPolicy{
			Burst: cfg.webAccountMaxFailures, Window: cfg.webAccountFailureWindow,
		},
		IP: account.LimitPolicy{
			Burst: cfg.webLoginIPBurst, Window: cfg.webLoginIPWindow,
		},
		Device: account.LimitPolicy{
			Burst: cfg.webLoginDeviceBurst, Window: cfg.webLoginDeviceWindow,
		},
	}
	attemptLimiter, err := account.NewRedisAttemptLimiter(runtimeClient, attemptLimiterConfig)
	if err != nil {
		log.Fatalf("build web account attempt limiter: %v", err)
	}
	defer attemptLimiter.Close()
	scriptCtx, scriptCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = preloadGateRedisScripts(scriptCtx, redisSessionAuthority, redisTicketStore, webAccountStore, attemptLimiter)
	if err == nil {
		// Raise/cache the process-wide disaster fence during bounded startup,
		// rather than charging an extra script to the first real login.
		_, err = redisSessionAuthority.AuthorityEpoch(scriptCtx)
	}
	scriptCancel()
	if err != nil {
		log.Fatalf("preload Gate Redis scripts: %v", err)
	}
	var logicConn *grpc.ClientConn
	var playerConn *grpc.ClientConn
	var routeConns []*grpc.ClientConn
	var profileReader publicprofile.Reader
	rpcMetrics := options.RPCMetrics
	if rpcMetrics == nil {
		rpcMetrics = grpcmetrics.New(prometheus.DefaultRegisterer)
	}
	rpcClientOption := grpc.WithUnaryInterceptor(rpcMetrics.UnaryClientInterceptor())
	assignmentReady := func() bool { return true }
	if options.Assignment != nil {
		if options.Assignment.Provider == nil || options.Assignment.Resolver == nil || options.Assignment.Applier == nil ||
			options.Assignment.Pump == nil || options.Assignment.Endpoints == nil {
			log.Fatal("remote assignment gateway composition is incomplete")
		}
		cfg.actorRouteSnapshot = "<remote-watch>"
		routeMetrics := actorrouting.RegisterRouteMetrics(prometheus.DefaultRegisterer)
		options.Assignment.Resolver.SetMetrics(routeMetrics)
		options.Assignment.Pump.SetMetrics(routeMetrics)
		assignmentReady = options.Assignment.DataPlaneHealthy
		pumpContext, pumpCancel := context.WithCancel(context.Background())
		assignmentRunDone := make(chan struct{})
		go func() {
			defer close(assignmentRunDone)
			if err := options.Assignment.Run(pumpContext); err != nil && !errors.Is(err, context.Canceled) {
				// The composition retains the exact terminal error for diagnostics;
				// do not copy a transport error into ordinary logs.
				log.Printf("remote actor route composition terminal")
			}
		}()
		defer func() {
			pumpCancel()
			_ = options.Assignment.Close()
			<-assignmentRunDone
		}()
	}
	if cfg.actorRouteSnapshot == "" && (cfg.playerAddr == "") != (cfg.farmAddr == "") {
		log.Fatal("PLAYER_ADDR and FARM_ADDR must be configured together without ACTOR_ROUTE_SNAPSHOT")
	}
	if cfg.actorRouteSnapshot == "" && cfg.playerAddr != "" {
		playerConn, err = grpc.Dial(cfg.playerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), rpcClientOption)
		if err != nil {
			log.Fatalf("dial player-svr %s: %v", cfg.playerAddr, err)
		}
		defer playerConn.Close()
		logicConn, err = grpc.Dial(cfg.farmAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), rpcClientOption)
		if err != nil {
			log.Fatalf("dial farm-svr %s: %v", cfg.farmAddr, err)
		}
		defer logicConn.Close()
		logicClient = command.NewRoutedLogicClient(
			command.NewGRPCLogicClient(logicrpcpb.NewLogicServiceClient(playerConn)),
			command.NewGRPCLogicClient(logicrpcpb.NewLogicServiceClient(logicConn)),
		)
		profileReader = publicprofile.NewDirectReader(logicrpcpb.NewPlayerParticipantServiceClient(playerConn))
		svc.SetBootstrapper(auth.NewGRPCBootstrapper(logicrpcpb.NewBootstrapServiceClient(playerConn), cfg.initialRuleVersion))
		log.Printf("using player-svr at %s and farm-svr at %s", cfg.playerAddr, cfg.farmAddr)
	} else if options.Assignment != nil || cfg.actorRouteSnapshot != "" {
		var resolver *actorrouting.StaticResolver
		if options.Assignment != nil {
			resolver = options.Assignment.Resolver
		}
		var routeErr error
		if resolver == nil {
			resolver, routeErr = actorrouting.LoadStaticResolver(cfg.actorRouteSnapshot)
			if routeErr != nil {
				log.Fatalf("load actor route snapshot: %v", routeErr)
			}
		}
		if cfg.actorRouteMaxEndpoints > 0 && len(resolver.Endpoints()) > cfg.actorRouteMaxEndpoints {
			log.Fatalf("actor route snapshot has %d endpoints, exceeds ACTOR_ROUTE_MAX_ENDPOINTS=%d", len(resolver.Endpoints()), cfg.actorRouteMaxEndpoints)
		}
		var clientResolver actorrouting.Resolver = resolver
		var pooledConn grpc.ClientConnInterface
		if options.Assignment != nil {
			clientResolver, routeErr = authoritycomposition.NewPooledResolver(resolver, options.Assignment.Endpoints)
			if routeErr == nil {
				pooledConn, routeErr = authoritycomposition.NewPooledClientConn(resolver, options.Assignment.Endpoints)
			}
			if routeErr != nil {
				log.Fatalf("build dynamic actor endpoint pool client: %v", routeErr)
			}
		}
		routes, routeErr := actorrouting.NewActorRouteClient(clientResolver, cfg.actorRouteCacheTTL)
		if routeErr != nil {
			log.Fatalf("build actor route client: %v", routeErr)
		}
		logicClients := make(map[string]command.LogicClient)
		bootstrapClients := make(map[string]logicrpcpb.BootstrapServiceClient)
		profileClients := make(map[string]logicrpcpb.PlayerParticipantServiceClient)
		if pooledConn != nil {
			logicClients[authoritycomposition.PooledEndpoint] = command.NewGRPCLogicClient(logicrpcpb.NewLogicServiceClient(pooledConn))
			bootstrapClients[authoritycomposition.PooledEndpoint] = logicrpcpb.NewBootstrapServiceClient(pooledConn)
			profileClients[authoritycomposition.PooledEndpoint] = logicrpcpb.NewPlayerParticipantServiceClient(pooledConn)
		} else {
			for _, endpoint := range resolver.Endpoints() {
				connection, dialErr := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), rpcClientOption)
				if dialErr != nil {
					log.Fatalf("dial actor holder %s: %v", endpoint, dialErr)
				}
				routeConns = append(routeConns, connection)
				logicClients[endpoint] = command.NewGRPCLogicClient(logicrpcpb.NewLogicServiceClient(connection))
				bootstrapClients[endpoint] = logicrpcpb.NewBootstrapServiceClient(connection)
				profileClients[endpoint] = logicrpcpb.NewPlayerParticipantServiceClient(connection)
			}
		}
		for _, connection := range routeConns {
			defer connection.Close()
		}
		serviceLBClients := make(map[commandcatalog.FirstHopRole]command.LogicClient, 2)
		if cfg.playerAddr != "" {
			playerConn, err = grpc.Dial(cfg.playerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), rpcClientOption)
			if err != nil {
				log.Fatalf("dial player-svr ServiceLB %s: %v", cfg.playerAddr, err)
			}
			defer playerConn.Close()
			serviceLBClients[commandcatalog.FirstHopPlayerSvr] = command.NewGRPCLogicClient(logicrpcpb.NewLogicServiceClient(playerConn))
		}
		if cfg.farmAddr != "" {
			logicConn, err = grpc.Dial(cfg.farmAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), rpcClientOption)
			if err != nil {
				log.Fatalf("dial farm-svr ServiceLB %s: %v", cfg.farmAddr, err)
			}
			defer logicConn.Close()
			serviceLBClients[commandcatalog.FirstHopFarmSvr] = command.NewGRPCLogicClient(logicrpcpb.NewLogicServiceClient(logicConn))
		}
		logicClient, routeErr = command.NewShardedLogicClientWithServiceLB(resolver.Realm(), routes, logicClients, serviceLBClients)
		if routeErr != nil {
			log.Fatal(routeErr)
		}
		profileReader, routeErr = publicprofile.NewRoutedReader(resolver.Realm(), routes, profileClients)
		if routeErr != nil {
			log.Fatal(routeErr)
		}
		bootstrapper, routeErr := auth.NewShardedBootstrapper(resolver.Realm(), routes, bootstrapClients)
		if routeErr != nil {
			log.Fatal(routeErr)
		}
		svc.SetBootstrapper(bootstrapper)
		log.Printf("using actor route snapshot %s with %d holder endpoints and %d ServiceLB destinations", cfg.actorRouteSnapshot, len(resolver.Endpoints()), len(serviceLBClients))
	} else {
		log.Fatal("PLAYER_ADDR/FARM_ADDR or ACTOR_ROUTE_SNAPSHOT must configure a backend")
	}
	gate := command.NewGate(svc, logicClient, cfg.maxFrame)
	downlinkCapacity, err := cfg.validatedDownlinkCapacity()
	if err != nil {
		log.Fatalf("validate per-connection downlink capacity: %v", err)
	}
	if err := gate.SetDownlinkWriterCapacity(downlinkCapacity); err != nil {
		log.Fatalf("configure per-connection downlink capacity: %v", err)
	}
	if err := gate.SetRouteLeaseSafetyMargin(cfg.routeLeaseSafetyMargin); err != nil {
		log.Fatalf("configure SessionRoute lease safety margin: %v", err)
	}
	if err := gate.SetSubscriptionLeaseConfig(cfg.subscriptionLeaseTTL, cfg.subscriptionLeaseSafetyMargin); err != nil {
		log.Fatalf("configure FarmSubscription lease: %v", err)
	}
	if err := gate.SetLeaseRenewalConfig(leaseRenewalCfg); err != nil {
		log.Fatalf("configure bounded lease renewal: %v", err)
	}
	bootLeaseStore, err := bootlease.NewRedisStore(runtimeClient, cfg.gateBootLeasePrefix)
	if err != nil {
		log.Fatalf("build GateBoot lease store: %v", err)
	}
	gateBootLease, err := bootlease.New(bootLeaseStore, bootlease.Fence{
		GatewayID: svc.GatewayID(), BootID: svc.GatewayBootID(),
	}, cfg.gateBootLeaseTTL)
	if err != nil {
		log.Fatalf("build GateBoot lease: %v", err)
	}
	gate.SetBootLease(gateBootLease)
	initialLeaseCtx, initialLeaseCancel := context.WithTimeout(context.Background(), cfg.gateBootLeaseRenewTimeout)
	if err := gateBootLease.Renew(initialLeaseCtx); err != nil {
		// Start in RECOVERING rather than silently admitting traffic. The renew
		// loop below can restore readiness without restarting this process.
		log.Printf("GateBoot lease initial renew failed; starting RECOVERING: %v", err)
	}
	initialLeaseCancel()
	gate.SetBackendPool(cfg.backendMaxInflight, cfg.backendRPCTimeout)
	gate.SetBackendAdmissionWait(cfg.backendAdmissionWait)
	gate.SetCrossGateKickTimeout(cfg.gateControlKickTimeout)
	var preAuthMetrics *preauth.Metrics
	if cfg.wsMetricsEnabled {
		preAuthMetrics = preauth.NewMetrics(prometheus.DefaultRegisterer)
		gate.SetMetrics(command.NewMetrics(prometheus.DefaultRegisterer))
		bootlease.RegisterMetrics(prometheus.DefaultRegisterer, gateBootLease)
	}
	gate.SetPreAuthController(preauth.New(preauth.Config{
		MaxConnections:         cfg.preAuthMaxConnections,
		MaxConnectionsPerIP:    cfg.preAuthMaxConnectionsPerIP,
		MaxFramesPerConnection: cfg.preAuthMaxFramesPerConnection,
		MaxBytesPerConnection:  cfg.preAuthMaxBytesPerConnection,
		MaxBytesPerIP:          cfg.preAuthMaxBytesPerIP,
		MaxBytesPod:            cfg.preAuthMaxBytesPod,
		Metrics:                preAuthMetrics,
	}))
	controlKicker, controlConnections, err := newStaticGateControlKicker(cfg.gateControlTargets)
	if err != nil {
		log.Fatalf("build GateControl clients: %v", err)
	}
	for _, connection := range controlConnections {
		defer connection.Close()
	}
	if len(cfg.gateControlTargets) > 0 {
		gate.SetCrossGateKicker(controlKicker)
	}
	leaseStop := make(chan struct{})
	var leaseStopOnce sync.Once
	var leaseWG sync.WaitGroup
	stopLeaseRenewals := func() {
		leaseStopOnce.Do(func() {
			close(leaseStop)
			gate.StopLeaseRenewalSchedulers()
			leaseWG.Wait()
		})
	}
	defer stopLeaseRenewals()
	bootLeaseInterval := cfg.gateBootLeaseTTL / 3
	if bootLeaseInterval < time.Second {
		bootLeaseInterval = time.Second
	}
	leaseWG.Add(1)
	go func() {
		defer leaseWG.Done()
		ticker := time.NewTicker(bootLeaseInterval)
		defer ticker.Stop()
		for {
			select {
			case <-leaseStop:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), cfg.gateBootLeaseRenewTimeout)
				err := gateBootLease.Renew(ctx)
				cancel()
				if err != nil {
					log.Printf("GateBoot lease renew failed trusted_until=%s trusted=%v: %v",
						gateBootLease.TrustedUntil().Format(time.RFC3339Nano), gate.BootLeaseTrusted(), err)
				}
			}
		}
	}()
	leaseWG.Add(1)
	go func() {
		defer leaseWG.Done()
		ticker := time.NewTicker(cfg.leaseSchedulerTick)
		defer ticker.Stop()
		for {
			select {
			case <-leaseStop:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), cfg.routeLeaseMaxDispatchDuration)
				if err := gate.RenewDueSessionLeases(ctx); err != nil {
					log.Printf("session route lease renew failed: %v", err)
				}
				cancel()
			}
		}
	}()
	downlinkRegistry := downlink.NewConnectionRegistry()
	downlinkMetrics := downlink.NewMetrics(nil, downlinkRegistry)
	downlinkRegistry.SetMetrics(downlinkMetrics)
	gate.SetDownlinkRegistry(downlinkRegistry)
	targetedOpportunityHintCleanup, err := startGatewayTargetedOpportunityHintSubscriber(projectionClient, downlinkRegistry)
	if err != nil {
		log.Fatalf("start targeted friend opportunity Hint subscriber: %v", err)
	}
	defer targetedOpportunityHintCleanup()
	leaseWG.Add(1)
	go func() {
		defer leaseWG.Done()
		ticker := time.NewTicker(cfg.leaseSchedulerTick)
		defer ticker.Stop()
		for {
			select {
			case <-leaseStop:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), cfg.subscriptionLeaseRenewTimeout)
				if err := gate.RenewDueSubscriptionLeases(ctx); err != nil {
					log.Printf("farm subscription lease renew failed: %v", err)
				}
				cancel()
			}
		}
	}()
	controlListener, err := net.Listen("tcp", cfg.gateControlListenAddr)
	if err != nil {
		log.Fatalf("listen GateControl %s: %v", cfg.gateControlListenAddr, err)
	}
	controlGRPC := grpc.NewServer()
	logicrpcpb.RegisterGateControlServiceServer(controlGRPC, gate)
	go func() {
		if err := controlGRPC.Serve(controlListener); err != nil {
			log.Printf("GateControl serve: %v", err)
		}
	}()
	defer controlGRPC.Stop()
	log.Printf("GateControl listening on %s", cfg.gateControlListenAddr)
	gateproxyReady := &atomic.Bool{}
	// Transport 选择:GATE_TRANSPORT=websocket(生产默认) 或 gateproxy(兼容探针)。
	// 同一进程只开放一种公网接入。
	transportMode := cfg.gateTransport
	if transportMode == "" {
		transportMode = "websocket"
	}
	var wsServer *wstransport.Server
	var commandResultFault *e2eCommandResultDropper
	switch transportMode {
	case "websocket":
		// 直连 WebSocket:复用 command.Gate 作为 FrameHandler。
		var wsMetrics *wstransport.Metrics
		if cfg.wsMetricsEnabled {
			wsMetrics = wstransport.NewMetrics("farmgate", "ws", nil)
		}
		var frameHandler wstransport.FrameHandler = gate
		if cfg.faultEndpointEnabled {
			commandResultFault = &e2eCommandResultDropper{inner: gate}
			frameHandler = commandResultFault
			log.Printf("WARNING: smoke-only CommandResult drop controller enabled")
		}
		wsServer = wstransport.NewServer(frameHandler, wstransport.Config{
			ReadLimit:            int64(cfg.maxFrame),
			SendQueueSize:        256,
			CommandQueueSize:     cfg.wsCommandQueueSize,
			MaxConns:             cfg.wsMaxConns,
			HelloTimeout:         cfg.preAuthHelloTimeout,
			RateLimitPerSec:      cfg.wsRateLimitPerSec,
			RateLimitBurst:       cfg.wsRateLimitBurst,
			AllowedOrigins:       cfg.wsAllowedOrigins,
			TrustedProxies:       cfg.wsTrustedProxies,
			Downlink:             downlinkRegistry,
			DownlinkDrainQuantum: cfg.downlinkDrainQuantum,
			Metrics:              wsMetrics,
			Logger:               log.Default(),
			AdmissionReady:       gate.BootLeaseTrusted,
		})
		gateproxyReady.Store(true)
		log.Printf("gate transport: websocket (direct, maxConns=%d rate=%.1f/s burst=%d origins=%v trustedProxies=%v metrics=%v)",
			cfg.wsMaxConns, cfg.wsRateLimitPerSec, cfg.wsRateLimitBurst, cfg.wsAllowedOrigins, cfg.wsTrustedProxies, cfg.wsMetricsEnabled)
	case "gateproxy":
		fallthrough
	default:
		if cfg.gateproxySocket != "" {
			go runGateproxy(context.Background(), cfg, gate, downlinkRegistry, gateproxyReady)
			log.Printf("gate transport: gateproxy (TCONND)")
		} else {
			log.Printf("gate transport: gateproxy selected but GATEPROXY_SOCKET is empty, transport disabled")
		}
	}
	downlinkListener, err := net.Listen("tcp", cfg.downlinkListenAddr)
	if err != nil {
		log.Fatalf("listen gateway downlink %s: %v", cfg.downlinkListenAddr, err)
	}
	downlinkGRPC := grpc.NewServer()
	logicrpcpb.RegisterGatewayDownlinkServiceServer(downlinkGRPC, downlinkRegistry)
	go func() {
		if err := downlinkGRPC.Serve(downlinkListener); err != nil {
			log.Printf("gateway downlink serve: %v", err)
		}
	}()
	defer downlinkGRPC.Stop()
	log.Printf("gateway downlink listening on %s", cfg.downlinkListenAddr)

	mux := http.NewServeMux()
	if commandResultFault != nil {
		mux.Handle("/api/v1/e2e/fault/drop-command-result", commandResultFault)
	}
	mux.Handle("/version", versionHandler())
	if cfg.profile.allowsTestAuth() {
		registerTestAuthRoutes(mux, cfg.profile, auth.NewHandler(svc, cfg.httpCookieSecure))
	}
	accountLoginHandler := auth.NewAccountLoginHandler(webAccounts, svc, cfg.httpCookieSecure)
	accountLoginHandler.SetAttemptLimiter(attemptLimiter, func(r *http.Request) string {
		return wstransport.ClientIP(r, cfg.httpTrustedProxies)
	})
	for _, path := range []string{"/v1/session/login", "/api/v1/session/login"} {
		mux.Handle(path, accountLoginHandler)
	}
	accountRegisterHandler := auth.NewAccountRegisterHandler(webAccounts, svc, cfg.httpCookieSecure)
	accountRegisterHandler.SetAttemptLimiter(attemptLimiter, func(r *http.Request) string {
		return wstransport.ClientIP(r, cfg.httpTrustedProxies)
	})
	changePasswordHandler := auth.NewChangePasswordHandler(webAccounts, svc, cfg.httpCookieSecure)
	changePasswordHandler.SetAttemptLimiter(attemptLimiter, func(r *http.Request) string {
		return wstransport.ClientIP(r, cfg.httpTrustedProxies)
	})
	if cfg.wsMetricsEnabled {
		attemptMetrics := account.NewAttemptMetrics(prometheus.DefaultRegisterer)
		accountLoginHandler.SetAttemptMetrics(attemptMetrics)
		accountRegisterHandler.SetAttemptMetrics(attemptMetrics)
		changePasswordHandler.SetAttemptMetrics(attemptMetrics)
	}
	for _, path := range []string{"/v1/accounts/register", "/api/v1/accounts/register"} {
		mux.Handle(path, accountRegisterHandler)
	}
	for _, path := range []string{"/v1/accounts/change-password", "/api/v1/accounts/change-password"} {
		mux.Handle(path, changePasswordHandler)
	}
	if cfg.webAccountAdminToken != "" {
		adminDisableHandler := auth.NewAdminDisableHandler(webAccounts, svc, cfg.webAccountAdminToken)
		for _, path := range []string{"/v1/admin/accounts/disable", "/api/v1/admin/accounts/disable"} {
			mux.Handle(path, adminDisableHandler)
		}
	}
	renewHandler := auth.NewRenewHandler(svc, cfg.httpCookieSecure)
	for _, path := range []string{"/v1/auth/renew", "/api/v1/auth/renew"} {
		mux.Handle(path, renewHandler)
	}
	revokeAllHandler := auth.NewRevokeAllHandler(svc, cfg.httpCookieSecure)
	for _, path := range []string{"/v1/auth/logout-all", "/api/v1/auth/logout-all"} {
		mux.Handle(path, revokeAllHandler)
	}
	// WeChat Mini-Program login endpoint uses the development verifier in this
	// repository. Keep it out of the showcase listener until a real WeChat
	// provider is wired; local/test retain the existing explicit test path.
	if cfg.profile.allowsTestAuth() {
		mux.Handle("/api/v1/auth/wx-miniprogram-login", auth.NewWeChatHandler(svc, cfg.httpCookieSecure))
	}
	// Public Friendship routes remain an HTTP/session adapter. Mode selection
	// is structural: remote_player never invokes the local authority factory.
	shareHandler, friendshipCleanup, err := buildGatewayFriendship(gatewayFriendshipOptions{
		Mode: friendshipMode, Authority: authorityClient, Projection: projectionClient, Identities: svc, Profiles: profileReader,
		FarmConnection: logicConn, PlayerConnection: playerConn, Notifier: downlinkRegistry,
		ShareStorePrefix: cfg.shareStorePrefix,
		FriendLimit:      cfg.friendLimit, ShareTokenTTL: cfg.shareTokenTTL,
		RPCDeadline: parseDurationEnv("FRIENDSHIP_RPC_TIMEOUT", "2s"),
	})
	if err != nil {
		log.Fatalf("build Gateway Friendship adapter: %v", err)
	}
	defer friendshipCleanup()
	friendshipLimiter := wstransport.NewIPLimiter(
		parseFloat("GATE_FRIENDSHIP_HTTP_RATE_PER_SEC", 20),
		parseInt("GATE_FRIENDSHIP_HTTP_RATE_BURST", 40),
	)
	defer friendshipLimiter.Close()
	friendshipHTTP := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !friendshipLimiter.AllowRequest(r, cfg.httpTrustedProxies) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"rate limit exceeded","error_code":"ERR_RATE_LIMITED"}`))
			return
		}
		shareHandler.ServeHTTP(w, r)
	})
	// 机会弹窗独立限流:不再与其他 Friendship HTTP 共用模糊限流,避免登录洪峰
	// 在请求进入 Player/Farm 后才在 2s 截止时变成 503。过载直接 429 + Retry-After,
	// 客户端异步重试;429 绝不转 NONE。
	opportunityLimiter := wstransport.NewIPLimiter(
		parseFloat("GATE_OPPORTUNITY_HTTP_RATE_PER_SEC", 20),
		parseInt("GATE_OPPORTUNITY_HTTP_RATE_BURST", 40),
	)
	defer opportunityLimiter.Close()
	opportunityMaxInflight := parseInt("GATE_OPPORTUNITY_HTTP_MAX_INFLIGHT", 256)
	var opportunitySlots chan struct{}
	if opportunityMaxInflight > 0 {
		opportunitySlots = make(chan struct{}, opportunityMaxInflight)
	}
	opportunityHTTP := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !opportunityLimiter.AllowRequest(r, cfg.httpTrustedProxies) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"opportunity rate limit exceeded","error_code":"ERR_RATE_LIMITED"}`))
			return
		}
		if opportunitySlots != nil {
			select {
			case opportunitySlots <- struct{}{}:
				defer func() { <-opportunitySlots }()
			default:
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":"opportunity concurrency limit exceeded","error_code":"ERR_OPPORTUNITY_BUSY"}`))
				return
			}
		}
		shareHandler.ServeHTTP(w, r)
	})
	for _, path := range []string{"/api/v1/share/create", "/api/v1/share/resolve", "/api/v1/share/accept", "/api/v1/friends", "/api/v1/friends/revoke"} {
		mux.Handle(path, friendshipHTTP)
	}
	mux.Handle("/api/v1/friends/opportunity", opportunityHTTP)
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		readinessConns := append([]*grpc.ClientConn{playerConn}, routeConns...)
		if !gate.BootLeaseTrusted() || !assignmentReady() || (wsServer != nil && wsServer.IsDraining()) ||
			!gatewayReady(logicConn, cfg.logicHealthAddr, cfg.gateproxySocket, gateproxyReady, readinessConns...) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"not_ready"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	// Prometheus 指标暴露(仅 websocket 模式启用)
	if wsServer != nil && cfg.wsMetricsEnabled {
		mux.Handle("/metrics", promhttp.Handler())
		log.Printf("prometheus metrics: /metrics")
	}

	// pprof is a privileged diagnostics surface. It intentionally remains
	// opt-in until an isolated admin listener exists.
	if os.Getenv("GATE_PPROF_ENABLED") == "1" {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		log.Printf("WARNING: pprof is enabled on the FarmGate HTTP listener")
	}

	// 直连 WebSocket transport 挂载点。
	if wsServer != nil {
		wsPath := cfg.wsPath
		if wsPath == "" {
			wsPath = "/ws"
		}
		mux.Handle(wsPath, wsServer.Handler())
		log.Printf("websocket endpoint: %s", wsPath)
	}

	srv := &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// 优雅关闭:SIGTERM/SIGINT 触发 WebSocket drain + HTTP shutdown。
	if wsServer != nil {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
		go func() {
			sig := <-sigCh
			log.Printf("received %v, entering graceful shutdown", sig)
			stopLeaseRenewals()
			wsServer.Drain(cfg.wsDrainWait)
			ctx, cancel := context.WithTimeout(context.Background(), cfg.wsShutdownTimeout)
			defer cancel()
			_ = srv.Shutdown(ctx)
		}()
	}

	// 如果配了独立的 WS 监听端口,主端口仍跑 HTTP,WS 端口跑专用 mux。
	if wsServer != nil && cfg.wsListenAddr != "" && cfg.wsListenAddr != cfg.listenAddr {
		wsMux := http.NewServeMux()
		wsPath := cfg.wsPath
		if wsPath == "" {
			wsPath = "/ws"
		}
		wsMux.Handle(wsPath, wsServer.Handler())
		wsMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		})
		wsMux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
			reason := ""
			readinessConns := append([]*grpc.ClientConn{playerConn}, routeConns...)
			switch {
			case wsServer.IsDraining():
				reason = "draining"
			case !gate.BootLeaseTrusted():
				reason = "boot_lease_not_trusted"
			case !assignmentReady():
				reason = "route_projection_not_fresh"
			case !gatewayReady(logicConn, cfg.logicHealthAddr, cfg.gateproxySocket, gateproxyReady, readinessConns...):
				reason = "downstream_not_ready"
			}
			if reason != "" {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = fmt.Fprintf(w, `{"status":"not_ready","reason":%q}`, reason)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ready"}`))
		})
		wsSrv := &http.Server{Addr: cfg.wsListenAddr, Handler: wsMux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			log.Printf("websocket listening on %s", cfg.wsListenAddr)
			if err := wsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("websocket server: %v", err)
			}
		}()
		defer wsSrv.Close()
	}

	log.Printf("listening on %s", cfg.listenAddr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("http server: %v", err)
	}
}

type config struct {
	profile                        appProfile
	profileErr                     error
	listenAddr                     string
	gatewayID                      string
	wssEndpoint                    string
	initialRuleVersion             int32
	ticketTTL                      time.Duration
	routeLeaseTTL                  time.Duration
	routeLeaseSafetyMargin         time.Duration
	routeLeaseRenewInterval        time.Duration
	routeLeaseBucketCount          int
	routeLeaseMaxJitter            time.Duration
	routeLeaseBatchMaxItems        int
	routeLeaseBatchMaxBytes        int
	routeLeaseMaxDispatchDuration  time.Duration
	routeLeaseTransientBackoff     time.Duration
	subscriptionLeaseTTL           time.Duration
	subscriptionLeaseSafetyMargin  time.Duration
	subscriptionLeaseRenewInterval time.Duration
	subscriptionLeaseRenewTimeout  time.Duration
	subscriptionLeaseBucketCount   int
	subscriptionLeaseMaxJitter     time.Duration
	subscriptionLeaseBatchMaxItems int
	subscriptionLeaseBatchMaxBytes int
	subscriptionLeaseBackoff       time.Duration
	leaseSchedulerTick             time.Duration
	webAccountStorePrefix          string
	webAccountPlayerIDBase         uint64
	webAccountPBKDF2Iterations     int
	webAccountMaxFailures          int
	webAccountFailureWindow        time.Duration
	webAccountLockDuration         time.Duration
	webAccountAdminToken           string
	webLoginRateLimitPrefix        string
	webLoginRateLimitSecret        string
	webLoginIPBurst                int
	webLoginIPWindow               time.Duration
	webLoginDeviceBurst            int
	webLoginDeviceWindow           time.Duration
	playerAddr                     string
	farmAddr                       string
	logicHealthAddr                string
	gateproxySocket                string
	gateproxyRetry                 time.Duration
	gateproxyHeartbeat             time.Duration
	gateproxyTimeout               time.Duration
	downlinkListenAddr             string
	gateControlListenAddr          string
	gateControlTargets             map[string]string
	gateControlKickTimeout         time.Duration
	maxFrame                       int
	backendMaxInflight             int
	backendRPCTimeout              time.Duration
	backendAdmissionWait           time.Duration
	demoToken                      string
	demoPlayerID                   uint64
	demoSecondaryToken             string
	demoSecondaryPlayerID          uint64
	httpSessionTTL                 time.Duration
	httpCookieSecure               bool
	httpCookieActiveVersion        string
	httpCookieKeyRing              string
	shareStorePrefix               string
	shareTokenTTL                  time.Duration
	friendLimit                    int
	// gateTransport 选择接入层实现:websocket(默认) 或 gateproxy(兼容探针)。
	gateTransport string
	// wsListenAddr 是 WebSocket transport 的独立监听地址。
	// 留空时复用 listenAddr,在同一 HTTP 端口挂 /ws 路由。
	wsListenAddr string
	// wsPath 是 WebSocket Upgrade 路径(默认 /ws)。
	wsPath string
	// wsRateLimitPerSec 是每 IP 每秒允许的新连接数。
	wsRateLimitPerSec float64
	// wsRateLimitBurst 是每 IP 突发最大连接数。
	wsRateLimitBurst int
	// wsMaxConns 是单实例最大并发连接数(水位),0 = 不限。
	wsMaxConns int
	// wsCommandQueueSize bounds frames waiting behind the one in-flight
	// command in a connection's SessionLane.
	wsCommandQueueSize int
	// Per-connection asynchronous downlink budgets. Hint quotas are soft
	// reservations; total limits continue to protect process memory.
	downlinkWriterMaxItems int
	downlinkWriterMaxBytes int
	downlinkHintMaxItems   int
	downlinkHintMaxBytes   int
	downlinkDrainQuantum   int
	// PRE_AUTH budgets cap unauthenticated transport resource ownership. These
	// are operational defaults, not frozen 3000w-DAU capacity conclusions.
	preAuthMaxConnections         int
	preAuthMaxConnectionsPerIP    int
	preAuthMaxFramesPerConnection int
	preAuthMaxBytesPerConnection  int64
	preAuthMaxBytesPerIP          int64
	preAuthMaxBytesPod            int64
	preAuthHelloTimeout           time.Duration
	// wsAllowedOrigins 是 Origin 白名单(host 或 host:port);空 = gorilla same-origin 默认。
	wsAllowedOrigins []string
	// wsTrustedProxies 是可信代理 CIDR;仅命中时采纳 XFF/X-Real-IP。
	wsTrustedProxies []*net.IPNet
	// httpTrustedProxies applies the same anti-spoofing rule to HTTP login
	// limits. It defaults to wsTrustedProxies when not configured separately.
	httpTrustedProxies []*net.IPNet
	// wsMetricsEnabled 开启 Prometheus 指标。
	wsMetricsEnabled          bool
	wsDrainWait               time.Duration
	wsShutdownTimeout         time.Duration
	actorRouteSnapshot        string
	actorRouteCacheTTL        time.Duration
	actorRouteMaxEndpoints    int
	loadTestTokenSecret       string
	faultEndpointEnabled      bool
	gateBootLeasePrefix       string
	gateBootLeaseTTL          time.Duration
	gateBootLeaseRenewTimeout time.Duration
}

func (c config) validatedDownlinkCapacity() (downlink.WriterCapacity, error) {
	capacity := downlink.WriterCapacity{
		MaxItems: c.downlinkWriterMaxItems,
		MaxBytes: c.downlinkWriterMaxBytes,
		Hint: downlink.HintPolicy{
			MaxHintItems: c.downlinkHintMaxItems,
			MaxHintBytes: c.downlinkHintMaxBytes,
		},
	}
	if capacity.MaxItems <= 0 || capacity.MaxBytes <= 0 {
		return downlink.WriterCapacity{}, errors.New("writer item and byte limits must be positive")
	}
	if capacity.Hint.MaxHintItems < 0 || capacity.Hint.MaxHintBytes < 0 ||
		capacity.Hint.MaxHintItems > capacity.MaxItems || capacity.Hint.MaxHintBytes > capacity.MaxBytes {
		return downlink.WriterCapacity{}, errors.New("hint quotas must be non-negative and bounded by writer limits")
	}
	if c.downlinkDrainQuantum <= 0 {
		return downlink.WriterCapacity{}, errors.New("drain quantum must be positive")
	}
	return capacity, nil
}

func loadConfig() config {
	get := func(k, def string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return def
	}
	profile, profileErr := parseAppProfile(os.Getenv("FARM_APP_PROFILE"))
	devDefaults := profile.allowsTestAuth()
	demoTokenDefault := ""
	demoPlayerDefault := "0"
	sharePrefixDefault := "farm:v121:m2:primary:share"
	if devDefaults {
		demoTokenDefault = "demo-platform-token"
		demoPlayerDefault = "100001"
		sharePrefixDefault = "farm:v121:m2:dev:share"
	}
	wssEndpointDefault := "ws://localhost:8080/ws"
	if profile.isShowcase() {
		wssEndpointDefault = ""
	}
	httpCookieSecure := os.Getenv("HTTP_SESSION_COOKIE_SECURE") == "1"
	if profile.isShowcase() && strings.TrimSpace(os.Getenv("HTTP_SESSION_COOKIE_SECURE")) == "" {
		httpCookieSecure = true
	}
	ttl, _ := time.ParseDuration(get("GATEWAY_TICKET_TTL", "30s"))
	rule, _ := strconv.Atoi(get("INITIAL_RULE_VERSION", "1"))
	player, _ := strconv.ParseUint(get("DEMO_PLAYER_ID", demoPlayerDefault), 10, 64)
	secondaryPlayer, _ := strconv.ParseUint(os.Getenv("DEMO_SECONDARY_PLAYER_ID"), 10, 64)
	webAccountPlayerIDBase, _ := strconv.ParseUint(get("WEB_ACCOUNT_PLAYER_ID_BASE", "1000000"), 10, 64)
	maxFrame, _ := strconv.Atoi(get("MAX_FRAME_BYTES", strconv.Itoa(1<<20)))
	wsTrustedProxies := parseCIDRs("GATE_WS_TRUSTED_PROXIES")
	httpTrustedProxies := parseCIDRs("GATE_HTTP_TRUSTED_PROXIES")
	if strings.TrimSpace(os.Getenv("GATE_HTTP_TRUSTED_PROXIES")) == "" {
		httpTrustedProxies = wsTrustedProxies
	}
	routeLeaseTTL := parseDurationEnv("GATE_ROUTE_LEASE_TTL", "30s")
	defaultRouteLeaseSafetyMargin := routeLeaseTTL / 10
	if defaultRouteLeaseSafetyMargin > time.Second {
		defaultRouteLeaseSafetyMargin = time.Second
	}
	subscriptionLeaseTTL := parseDurationEnv("GATE_SUBSCRIPTION_LEASE_TTL", "30s")
	defaultSubscriptionLeaseSafetyMargin := subscriptionLeaseTTL / 10
	if defaultSubscriptionLeaseSafetyMargin > time.Second {
		defaultSubscriptionLeaseSafetyMargin = time.Second
	}
	defaultSubscriptionLeaseRenewInterval := subscriptionLeaseTTL / 3
	if defaultSubscriptionLeaseRenewInterval < time.Second {
		defaultSubscriptionLeaseRenewInterval = time.Second
	}
	defaultRouteLeaseRenewInterval := routeLeaseTTL / 3
	if defaultRouteLeaseRenewInterval <= 0 {
		defaultRouteLeaseRenewInterval = 10 * time.Second
	}
	routeLeaseRenewInterval := parseDurationEnv("GATE_ROUTE_LEASE_RENEW_INTERVAL", defaultRouteLeaseRenewInterval.String())
	subscriptionLeaseRenewInterval := parseDurationEnv("GATE_SUBSCRIPTION_LEASE_RENEW_INTERVAL", defaultSubscriptionLeaseRenewInterval.String())
	defaultRouteJitter := routeLeaseRenewInterval / 128
	defaultSubscriptionJitter := subscriptionLeaseRenewInterval / 128
	socialProductProfile := socialprofile.Current()
	actorRouteMaxEndpoints := authoritycomposition.ActorRouteEndpointLimitFromEnvironment()
	return config{
		profile:                        profile,
		profileErr:                     profileErr,
		listenAddr:                     get("GATEWAY_LISTEN", ":8080"),
		gatewayID:                      get("GATEWAY_ID", "farm-gate-local"),
		wssEndpoint:                    get("WSS_ENDPOINT", wssEndpointDefault),
		initialRuleVersion:             int32(rule),
		ticketTTL:                      ttl,
		routeLeaseTTL:                  routeLeaseTTL,
		routeLeaseSafetyMargin:         parseDurationEnv("GATE_ROUTE_LEASE_SAFETY_MARGIN", defaultRouteLeaseSafetyMargin.String()),
		routeLeaseRenewInterval:        routeLeaseRenewInterval,
		routeLeaseBucketCount:          parseInt("GATE_ROUTE_LEASE_BUCKETS", 64),
		routeLeaseMaxJitter:            parseDurationEnvAllowZero("GATE_ROUTE_LEASE_MAX_JITTER", defaultRouteJitter.String()),
		routeLeaseBatchMaxItems:        parseInt("GATE_ROUTE_LEASE_BATCH_MAX_ITEMS", 256),
		routeLeaseBatchMaxBytes:        parseInt("GATE_ROUTE_LEASE_BATCH_MAX_BYTES", 512<<10),
		routeLeaseMaxDispatchDuration:  parseDurationEnv("GATE_ROUTE_LEASE_MAX_DISPATCH_DURATION", "3s"),
		routeLeaseTransientBackoff:     parseDurationEnv("GATE_ROUTE_LEASE_TRANSIENT_BACKOFF", "250ms"),
		subscriptionLeaseTTL:           subscriptionLeaseTTL,
		subscriptionLeaseSafetyMargin:  parseDurationEnv("GATE_SUBSCRIPTION_LEASE_SAFETY_MARGIN", defaultSubscriptionLeaseSafetyMargin.String()),
		subscriptionLeaseRenewInterval: subscriptionLeaseRenewInterval,
		subscriptionLeaseRenewTimeout:  parseDurationEnv("GATE_SUBSCRIPTION_LEASE_RENEW_TIMEOUT", "3s"),
		subscriptionLeaseBucketCount:   parseInt("GATE_SUBSCRIPTION_LEASE_BUCKETS", 64),
		subscriptionLeaseMaxJitter:     parseDurationEnvAllowZero("GATE_SUBSCRIPTION_LEASE_MAX_JITTER", defaultSubscriptionJitter.String()),
		subscriptionLeaseBatchMaxItems: parseInt("GATE_SUBSCRIPTION_LEASE_BATCH_MAX_ITEMS", 256),
		subscriptionLeaseBatchMaxBytes: parseInt("GATE_SUBSCRIPTION_LEASE_BATCH_MAX_BYTES", 512<<10),
		subscriptionLeaseBackoff:       parseDurationEnv("GATE_SUBSCRIPTION_LEASE_TRANSIENT_BACKOFF", "250ms"),
		leaseSchedulerTick:             parseDurationEnv("GATE_LEASE_SCHEDULER_TICK", "10ms"),
		webAccountStorePrefix:          get("WEB_ACCOUNT_STORE_PREFIX", "farm:v122:web-account"),
		webAccountPlayerIDBase:         webAccountPlayerIDBase,
		webAccountPBKDF2Iterations:     parseInt("WEB_ACCOUNT_PBKDF2_ITERATIONS", account.DefaultPBKDF2Iterations),
		webAccountMaxFailures:          parseInt("WEB_ACCOUNT_MAX_FAILURES", 5),
		webAccountFailureWindow:        parseDurationEnv("WEB_ACCOUNT_FAILURE_WINDOW", "5m"),
		webAccountLockDuration:         parseDurationEnv("WEB_ACCOUNT_LOCK_DURATION", "1m"),
		webAccountAdminToken:           strings.TrimSpace(os.Getenv("WEB_ACCOUNT_ADMIN_TOKEN")),
		webLoginRateLimitPrefix:        get("WEB_LOGIN_RATE_LIMIT_PREFIX", "farm:v03:web-login-limit"),
		webLoginRateLimitSecret:        os.Getenv("WEB_LOGIN_RATE_LIMIT_SECRET"),
		webLoginIPBurst:                parseInt("WEB_LOGIN_IP_BURST", 30),
		webLoginIPWindow:               parseDurationEnv("WEB_LOGIN_IP_WINDOW", "1m"),
		webLoginDeviceBurst:            parseInt("WEB_LOGIN_DEVICE_BURST", 10),
		webLoginDeviceWindow:           parseDurationEnv("WEB_LOGIN_DEVICE_WINDOW", "1m"),
		playerAddr:                     os.Getenv("PLAYER_ADDR"),
		farmAddr:                       os.Getenv("FARM_ADDR"),
		logicHealthAddr:                os.Getenv("LOGIC_HEALTH_ADDR"),
		gateproxySocket:                os.Getenv("GATEPROXY_SOCKET"),
		gateproxyRetry:                 parseDurationEnv("GATEPROXY_RETRY_INTERVAL", "1s"),
		gateproxyHeartbeat:             parseDurationEnv("GATEPROXY_HEARTBEAT_INTERVAL", "1s"),
		gateproxyTimeout:               parseDurationEnv("GATEPROXY_HEARTBEAT_TIMEOUT", "5s"),
		downlinkListenAddr:             get("GATEWAY_DOWNLINK_LISTEN", ":9092"),
		gateControlListenAddr:          get("GATE_CONTROL_LISTEN", ":9093"),
		gateControlTargets:             parseGateControlTargets(os.Getenv("GATE_CONTROL_TARGETS")),
		gateControlKickTimeout:         parseDurationEnv("GATE_CONTROL_KICK_TIMEOUT", "3s"),
		maxFrame:                       maxFrame,
		backendMaxInflight:             parseInt("GATE_BACKEND_MAX_INFLIGHT", 1024),
		backendRPCTimeout:              parseDurationEnv("GATE_BACKEND_RPC_TIMEOUT", "3s"),
		backendAdmissionWait:           parseDurationEnv("GATE_BACKEND_ADMISSION_WAIT", "25ms"),
		demoToken:                      get("DEMO_PLATFORM_TOKEN", demoTokenDefault),
		demoPlayerID:                   player,
		demoSecondaryToken:             os.Getenv("DEMO_SECONDARY_PLATFORM_TOKEN"),
		demoSecondaryPlayerID:          secondaryPlayer,
		httpSessionTTL:                 parseDurationEnv("HTTP_SESSION_TTL", "2h"),
		httpCookieSecure:               httpCookieSecure,
		httpCookieActiveVersion:        get("HTTP_SESSION_COOKIE_ACTIVE_VERSION", "1"),
		httpCookieKeyRing:              os.Getenv("HTTP_SESSION_COOKIE_KEYRING"),
		shareStorePrefix:               get("SHARE_STORE_PREFIX", sharePrefixDefault),
		shareTokenTTL:                  parseDurationEnv("SHARE_TOKEN_TTL", socialProductProfile.ShareTokenTTL.String()),
		friendLimit:                    parseInt("FRIEND_LIMIT", socialProductProfile.EffectiveMaxFriends),
		gateTransport:                  get("GATE_TRANSPORT", "websocket"),
		wsListenAddr:                   os.Getenv("GATE_WS_LISTEN"),
		wsPath:                         get("GATE_WS_PATH", "/ws"),
		wsRateLimitPerSec:              parseFloat("GATE_WS_RATE_PER_SEC", 10),
		wsRateLimitBurst:               parseInt("GATE_WS_RATE_BURST", 20),
		wsMaxConns:                     parseInt("GATE_WS_MAX_CONNS", 0),
		wsCommandQueueSize:             parseInt("GATE_WS_COMMAND_QUEUE_SIZE", 16),
		downlinkWriterMaxItems:         parseInt("GATE_DOWNLINK_WRITER_MAX_ITEMS", 256),
		downlinkWriterMaxBytes:         parseInt("GATE_DOWNLINK_WRITER_MAX_BYTES", 1<<20),
		downlinkHintMaxItems:           parseInt("GATE_DOWNLINK_HINT_MAX_ITEMS", 32),
		downlinkHintMaxBytes:           parseInt("GATE_DOWNLINK_HINT_MAX_BYTES", 64<<10),
		downlinkDrainQuantum:           parseInt("GATE_DOWNLINK_DRAIN_QUANTUM", 64),
		preAuthMaxConnections:          parseInt("GATE_PREAUTH_MAX_CONNECTIONS", 4096),
		preAuthMaxConnectionsPerIP:     parseInt("GATE_PREAUTH_MAX_CONNECTIONS_PER_IP", 64),
		preAuthMaxFramesPerConnection: parseInt(
			"GATE_PREAUTH_MAX_FRAMES_PER_CONNECTION", 1,
		),
		preAuthMaxBytesPerConnection: parseInt64("GATE_PREAUTH_MAX_BYTES_PER_CONNECTION", 64<<10),
		preAuthMaxBytesPerIP:         parseInt64("GATE_PREAUTH_MAX_BYTES_PER_IP", 1<<20),
		preAuthMaxBytesPod:           parseInt64("GATE_PREAUTH_MAX_BYTES_POD", 64<<20),
		preAuthHelloTimeout:          parseDurationEnv("GATE_PREAUTH_HELLO_TIMEOUT", "10s"),
		wsAllowedOrigins:             parseCSV("GATE_WS_ALLOWED_ORIGINS"),
		wsTrustedProxies:             wsTrustedProxies,
		httpTrustedProxies:           httpTrustedProxies,
		wsMetricsEnabled:             os.Getenv("GATE_WS_METRICS") == "1",
		wsDrainWait:                  parseDurationEnv("GATE_WS_DRAIN_WAIT", "5s"),
		wsShutdownTimeout:            parseDurationEnv("GATE_WS_SHUTDOWN_TIMEOUT", "10s"),
		actorRouteSnapshot:           os.Getenv("ACTOR_ROUTE_SNAPSHOT"),
		actorRouteCacheTTL:           parseDurationEnv("ACTOR_ROUTE_CACHE_TTL", "1s"),
		actorRouteMaxEndpoints:       actorRouteMaxEndpoints,
		loadTestTokenSecret:          os.Getenv("LOAD_TEST_TOKEN_SECRET"),
		faultEndpointEnabled:         !profile.isShowcase() && os.Getenv("GATE_E2E_COMMAND_RESULT_FAULTS") == "1",
		gateBootLeasePrefix:          get("GATE_BOOT_LEASE_PREFIX", bootlease.DefaultRedisPrefix),
		gateBootLeaseTTL:             parseDurationEnv("GATE_BOOT_LEASE_TTL", "15s"),
		gateBootLeaseRenewTimeout:    parseDurationEnv("GATE_BOOT_LEASE_RENEW_TIMEOUT", "3s"),
	}
}

func registerTestAuthRoutes(mux *http.ServeMux, profile appProfile, handler http.Handler) {
	if !profile.allowsTestAuth() {
		return
	}
	for _, path := range []string{"/v1/auth/login", "/v1/dev/session", "/api/v1/auth/login", "/api/v1/dev/session"} {
		mux.Handle(path, handler)
	}
}

func envEnabled(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func envConfigured(key string) bool {
	return strings.TrimSpace(os.Getenv(key)) != ""
}

func providerIsUnsafe(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "mock", "fake", "stub", "demo", "fixture", "test":
		return true
	default:
		return false
	}
}

func (cfg config) validateProfile() error {
	if cfg.profileErr != nil {
		return cfg.profileErr
	}
	if !cfg.profile.isShowcase() {
		return nil
	}
	for _, key := range []string{
		"DEMO_PLATFORM_TOKEN",
		"DEMO_PLAYER_ID",
		"DEMO_SECONDARY_PLATFORM_TOKEN",
		"DEMO_SECONDARY_PLAYER_ID",
		"LOAD_TEST_TOKEN_SECRET",
		"GATE_SESSION_EPOCH_CACHE_TTL",
		"GATE_BLOCK_PROFILE_RATE",
	} {
		if envConfigured(key) {
			return fmt.Errorf("%s is not allowed when FARM_APP_PROFILE=showcase", key)
		}
	}
	for _, key := range []string{
		"GATE_E2E_COMMAND_RESULT_FAULTS",
		"GATE_PPROF_ENABLED",
		"ENABLE_DEV_SESSION",
		"FARM_DEV_SESSION",
		"GATE_DEV_SESSION",
		"PUBLIC_FAULT_ENDPOINT",
		"GATE_FAULT_ENDPOINT",
	} {
		if envEnabled(key) {
			return fmt.Errorf("%s is not allowed when FARM_APP_PROFILE=showcase", key)
		}
	}
	for _, key := range []string{
		"AUTH_PROVIDER",
		"FRIEND_PROVIDER",
		"FARM_AUTH_PROVIDER",
		"FARM_FRIEND_PROVIDER",
		"MOCK_AUTH_PROVIDER",
		"MOCK_FRIEND_PROVIDER",
	} {
		if providerIsUnsafe(key) {
			return fmt.Errorf("unsafe provider %s is not allowed when FARM_APP_PROFILE=showcase", key)
		}
	}
	if cfg.demoToken != "" || cfg.demoPlayerID != 0 || cfg.demoSecondaryToken != "" || cfg.demoSecondaryPlayerID != 0 {
		return errors.New("demo platform identity is enabled when FARM_APP_PROFILE=showcase")
	}
	if !cfg.httpCookieSecure {
		return errors.New("HTTP_SESSION_COOKIE_SECURE must be enabled when FARM_APP_PROFILE=showcase")
	}
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(cfg.wssEndpoint)), "wss://") {
		return errors.New("WSS_ENDPOINT must be an explicit wss:// URL when FARM_APP_PROFILE=showcase")
	}
	if strings.EqualFold(strings.TrimSpace(cfg.gateTransport), "gateproxy") {
		return errors.New("GATE_TRANSPORT=gateproxy is not allowed when FARM_APP_PROFILE=showcase")
	}
	return nil
}

func (cfg config) leaseRenewalConfig() (command.LeaseRenewalConfig, error) {
	var result command.LeaseRenewalConfig
	if cfg.leaseSchedulerTick < time.Millisecond || cfg.leaseSchedulerTick > time.Second {
		return result, fmt.Errorf("GATE_LEASE_SCHEDULER_TICK=%s must be in [1ms,1s]", cfg.leaseSchedulerTick)
	}
	build := func(lane string, ttl, margin, interval time.Duration, buckets, maxItems, maxBytes int,
		jitter, dispatch, backoff time.Duration) (command.LeaseRenewalLaneConfig, error) {
		var laneConfig command.LeaseRenewalLaneConfig
		if buckets < 1 || buckets > 4096 {
			return laneConfig, fmt.Errorf("%s bucket count %d must be in [1,4096]", lane, buckets)
		}
		if maxItems < 1 || maxItems > 4096 {
			return laneConfig, fmt.Errorf("%s max_items %d must be in [1,4096]", lane, maxItems)
		}
		if maxBytes < 256 || maxBytes > 64<<20 {
			return laneConfig, fmt.Errorf("%s max_bytes %d must be in [256,67108864]", lane, maxBytes)
		}
		laneConfig = command.LeaseRenewalLaneConfig{
			Batch: leasebatch.Config{
				Limits:      leasebatch.Limits{MaxItems: maxItems, MaxBytes: maxBytes, MaxDispatchDuration: dispatch},
				Schedule:    leasebatch.Schedule{Interval: interval, BucketCount: uint32(buckets), MaxJitter: jitter},
				MaxPlanScan: maxItems*4 + 64,
			},
			TransientBackoff: backoff,
		}
		if err := laneConfig.Batch.ValidateSafety(ttl, margin); err != nil {
			return laneConfig, fmt.Errorf("%s: %w", lane, err)
		}
		if backoff <= 0 || backoff >= ttl-margin {
			return laneConfig, fmt.Errorf("%s transient backoff %s outside trusted window", lane, backoff)
		}
		return laneConfig, nil
	}
	var err error
	result.SessionRoute, err = build("session_route", cfg.routeLeaseTTL, cfg.routeLeaseSafetyMargin,
		cfg.routeLeaseRenewInterval, cfg.routeLeaseBucketCount, cfg.routeLeaseBatchMaxItems,
		cfg.routeLeaseBatchMaxBytes, cfg.routeLeaseMaxJitter, cfg.routeLeaseMaxDispatchDuration,
		cfg.routeLeaseTransientBackoff)
	if err != nil {
		return result, err
	}
	result.Subscription, err = build("subscription", cfg.subscriptionLeaseTTL, cfg.subscriptionLeaseSafetyMargin,
		cfg.subscriptionLeaseRenewInterval, cfg.subscriptionLeaseBucketCount,
		cfg.subscriptionLeaseBatchMaxItems, cfg.subscriptionLeaseBatchMaxBytes,
		cfg.subscriptionLeaseMaxJitter, cfg.subscriptionLeaseRenewTimeout, cfg.subscriptionLeaseBackoff)
	if err != nil {
		return result, err
	}
	return result, nil
}

func parseCookieActiveVersion(value string) (uint32, error) {
	version, err := strconv.ParseUint(strings.TrimSpace(value), 10, 32)
	if err != nil || version == 0 {
		return 0, errors.New("active Cookie key version must be a positive uint32")
	}
	return uint32(version), nil
}

// parseCookieKeyRing accepts "2=<base64url>,1=<base64url>". The active
// version is configured separately. Values are never logged or persisted.
func parseCookieKeyRing(value string) (map[uint32][]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	result := make(map[uint32][]byte)
	for _, item := range strings.Split(value, ",") {
		versionText, encoded, ok := strings.Cut(strings.TrimSpace(item), "=")
		version, err := strconv.ParseUint(strings.TrimSpace(versionText), 10, 32)
		if !ok || err != nil || version == 0 || strings.TrimSpace(encoded) == "" {
			return nil, errors.New("expected comma-separated version=base64url entries")
		}
		if _, exists := result[uint32(version)]; exists {
			return nil, fmt.Errorf("duplicate Cookie key version %d", version)
		}
		encoded = strings.TrimSpace(encoded)
		key, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil {
			key, err = base64.URLEncoding.DecodeString(encoded)
		}
		if err != nil || len(key) < 32 {
			return nil, fmt.Errorf("Cookie key version %d must be base64url and decode to at least 32 bytes", version)
		}
		result[uint32(version)] = key
	}
	return result, nil
}

// parseCSV 解析逗号分隔的环境变量为字符串列表(去空白、去空项)。
func parseCSV(key string) []string {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(v, ",") {
		if s := strings.TrimSpace(part); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// parseCIDRs 解析逗号分隔的 CIDR 列表;裸 IP 自动补 /32 或 /128。
// 解析失败的项会被忽略并记日志,不会中断启动。
func parseCIDRs(key string) []*net.IPNet {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	var out []*net.IPNet
	for _, part := range strings.Split(v, ",") {
		s := strings.TrimSpace(part)
		if s == "" {
			continue
		}
		if _, cidr, err := net.ParseCIDR(s); err == nil {
			out = append(out, cidr)
			continue
		}
		if ip := net.ParseIP(s); ip != nil {
			bits := 32
			if ip.To4() == nil {
				bits = 128
			}
			out = append(out, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
			continue
		}
		log.Printf("ignoring invalid entry in %s: %q", key, s)
	}
	return out
}

func parseInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func parseInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func parseFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			return n
		}
	}
	return def
}

func gatewayReady(logicConn *grpc.ClientConn, logicHealthAddr, gateproxySocket string, gateproxyReady *atomic.Bool, extraLogicConns ...*grpc.ClientConn) bool {
	if logicConn == nil && logicHealthAddr == "" && gateproxySocket == "" {
		return true
	}
	if logicConn != nil {
		state := logicConn.GetState()
		if state == connectivity.Shutdown {
			return false
		}
		// grpc.Dial is intentionally non-blocking in the process entrypoint.
		// Readiness must not report success while the RPC channel is still Idle
		// or retrying; otherwise the first HTTP login can race the Logic dial and
		// be mapped to a misleading 401 by the login handler.
		if state != connectivity.Ready {
			logicConn.Connect()
			return false
		}
	}
	for _, connection := range extraLogicConns {
		if connection == nil {
			continue
		}
		state := connection.GetState()
		if state == connectivity.Shutdown {
			return false
		}
		if state != connectivity.Ready {
			connection.Connect()
			return false
		}
	}
	if logicHealthAddr == "" {
		if logicConn == nil && gateproxySocket == "" {
			return true
		}
	} else {
		if !healthEndpointReady(logicHealthAddr) {
			return false
		}
	}
	if gateproxySocket != "" && (gateproxyReady == nil || !gateproxyReady.Load()) {
		return false
	}
	return logicConn == nil || logicConn.GetState() == connectivity.Ready
}

func parseDurationEnv(key, fallback string) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		value = fallback
	}
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		d, _ = time.ParseDuration(fallback)
	}
	return d
}

func parseDurationEnvAllowZero(key, fallback string) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		value = fallback
	}
	d, err := time.ParseDuration(value)
	if err != nil || d < 0 {
		d, _ = time.ParseDuration(fallback)
	}
	return d
}

func healthEndpointReady(endpoint string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/readyz", nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// runGateproxy is the TCONND + gateproxy transport path. It keeps the
// platform GateStream inside farm-gate. A stream outage makes the
// process NotReady and invalidates all affected generations through the
// transport close behavior; it never turns a network write into a business commit.
func runGateproxy(ctx context.Context, cfg config, gate *command.Gate, registry *downlink.ConnectionRegistry, ready *atomic.Bool) {
	client := &localGateClient{gate: gate, registry: registry}
	for {
		if ctx.Err() != nil {
			ready.Store(false)
			return
		}
		dialCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		conn, err := grpc.DialContext(dialCtx, "unix:"+cfg.gateproxySocket,
			grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancel()
		if err != nil {
			ready.Store(false)
			log.Printf("gateproxy dial %s: %v", cfg.gateproxySocket, err)
			if !sleepContext(ctx, cfg.gateproxyRetry) {
				return
			}
			continue
		}
		stream, err := logicrpcpbGateStream(ctx, conn)
		if err != nil {
			_ = conn.Close()
			ready.Store(false)
			log.Printf("gateproxy GateStream: %v", err)
			if !sleepContext(ctx, cfg.gateproxyRetry) {
				return
			}
			continue
		}
		ready.Store(true)
		runner := gateproxy.New(client, gateproxy.Config{
			MaxFrameBytes:     cfg.maxFrame,
			HelloTimeout:      cfg.preAuthHelloTimeout,
			HeartbeatInterval: cfg.gateproxyHeartbeat,
			HeartbeatTimeout:  cfg.gateproxyTimeout,
			Logger:            log.Default(),
		})
		err = runner.Run(stream)
		ready.Store(false)
		_ = stream.CloseSend()
		_ = conn.Close()
		if ctx.Err() != nil {
			return
		}
		if !errors.Is(err, gateproxy.ErrStreamClosed) {
			log.Printf("gateproxy stream stopped: %v", err)
		}
		if !sleepContext(ctx, cfg.gateproxyRetry) {
			return
		}
	}
}

// logicrpcpbGateStream is kept as a tiny helper so all GateStream construction
// stays in this process entry point; the transport package only sees its
// narrow stream interface.
func logicrpcpbGateStream(ctx context.Context, conn *grpc.ClientConn) (gateproxy.GateStream, error) {
	return proxypb.NewConnectorServiceClient(conn).GateStream(ctx)
}

type localGateClient struct {
	gate     *command.Gate
	registry *downlink.ConnectionRegistry
}

func (c *localGateClient) HandleFrame(ctx context.Context, connectionID string, generation uint64, body []byte) ([]byte, error) {
	return c.gate.HandleBinary(ctx, connectionID, generation, body)
}

func (c *localGateClient) PollDownlink(_ context.Context, connectionID string, generation uint64) ([]byte, bool, error) {
	frame, ok := c.registry.Dequeue(connectionID, generation)
	return frame, ok, nil
}

func (c *localGateClient) Disconnect(_ context.Context, connectionID string, generation uint64) error {
	c.gate.Disconnect(connectionID, generation)
	return nil
}

func (c *localGateClient) BeginPreAuth(connectionID string, generation uint64, clientIP string) error {
	return c.gate.BeginPreAuth(connectionID, generation, clientIP)
}

func (c *localGateClient) AdmitPreAuth(connectionID string, generation uint64, frameBytes int) error {
	return c.gate.AdmitPreAuth(connectionID, generation, frameBytes)
}

func (c *localGateClient) IsAuthenticated(connectionID string, generation uint64) bool {
	return c.gate.IsAuthenticated(connectionID, generation)
}

func (c *localGateClient) TimeoutPreAuth(connectionID string, generation uint64) bool {
	return c.gate.TimeoutPreAuth(connectionID, generation)
}

func (c *localGateClient) PreAuthMaxFrameBytes() int64 {
	return c.gate.PreAuthMaxFrameBytes()
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func versionHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":"` + farmgame.FullVersion() + `"}`))
	})
}
