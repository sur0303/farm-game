// Command live-bot-fixture provisions the players a sustained load run needs.
//
// Provisioning is deliberately route-aware. A Player/Farm Service is an
// ordinary load-balanced first hop and is not a safe writer for a sharded
// actor. The fixture loads the durable route snapshot, verifies that every
// referenced Holder is fresh and serving that snapshot, and sends each write
// through the exact Holder endpoint together with the current route fence.
//
// A freshly created player has 100 coins, level 1, and five unlocked plots.
// This grants the coins and experience of a developed player. Farm plot
// records are still produced by the game protocol; this command must never
// create a partial or fake FarmRecord that the game itself would reject.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/friend"
	"farmgame/internal/redisdomains"
	"farmgame/internal/storage/redis/rediskey"
	redisroutecontrol "farmgame/internal/storage/redis/routecontrol"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// expPerLevel mirrors the player service's own curve, level = exp/20 + 1. It
// is duplicated rather than imported because this tool must ask for a delta
// over the wire, and the service is the only thing that applies it.
const expPerLevel = 20

// bootstrapPrefix must match the identity livefarmbot logs in with, because
// the first bootstrap binds the record. Provisioning under a different one
// would create the player this run never uses.
const bootstrapPrefix = "live-farm-bot-"

type config struct {
	// playerAddr is retained for CLI compatibility with the original fixture.
	// It is intentionally not used for redis-current provisioning: a Service
	// address would reintroduce arbitrary load-balanced sharded writes.
	playerAddr         string
	playerStart        uint64
	players            int
	coins              int64
	level              int
	concurrency        int
	timeout            time.Duration
	routePath          string
	namespaceEnv       string
	zone               string
	holderReadyTimeout time.Duration
	friendRing         bool
	friendRingOffset   int
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "live-bot-fixture: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	cfg := config{
		playerAddr:         "127.0.0.1:9100",
		playerStart:        930000001,
		players:            200,
		coins:              50000,
		level:              19,
		concurrency:        16,
		timeout:            2 * time.Minute,
		routePath:          "redis-current",
		namespaceEnv:       "uctestscaleplanqqfarm20260816",
		zone:               "z1",
		holderReadyTimeout: 90 * time.Second,
		friendRingOffset:   1,
	}
	flags := flag.NewFlagSet("live-bot-fixture", flag.ContinueOnError)
	flags.StringVar(&cfg.playerAddr, "player-addr", cfg.playerAddr, "deprecated compatibility flag; ignored for redis-current routing")
	flags.Uint64Var(&cfg.playerStart, "player-start", cfg.playerStart, "first player id")
	flags.IntVar(&cfg.players, "players", cfg.players, "how many consecutive players to provision")
	flags.Int64Var(&cfg.coins, "coins", cfg.coins, "coin balance to grant; unlocking all 24 plots costs 19000")
	flags.IntVar(&cfg.level, "level", cfg.level, "level to grant; the last plot requires 19")
	flags.IntVar(&cfg.concurrency, "concurrency", cfg.concurrency, "players provisioned in parallel")
	flags.DurationVar(&cfg.timeout, "timeout", cfg.timeout, "bound on the whole run")
	flags.StringVar(&cfg.routePath, "route", cfg.routePath, "redis-current or a verified static route snapshot path")
	flags.StringVar(&cfg.namespaceEnv, "namespace-env", cfg.namespaceEnv, "Redis namespace environment for the durable route snapshot")
	flags.StringVar(&cfg.zone, "zone", cfg.zone, "Redis namespace zone for the durable route snapshot")
	flags.DurationVar(&cfg.holderReadyTimeout, "holder-ready-timeout", cfg.holderReadyTimeout, "maximum wait for every referenced Holder to report the loaded snapshot")
	flags.BoolVar(&cfg.friendRing, "friend-ring", cfg.friendRing, "initialize a canonical friend ring across all provisioned players")
	flags.IntVar(&cfg.friendRingOffset, "friend-ring-offset", cfg.friendRingOffset, "friend ring hop, between 1 and players-1")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := cfg.validate(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	snapshot, routeClient, err := loadRouteSnapshot(ctx, cfg)
	if err != nil {
		return err
	}
	if routeClient != nil {
		defer routeClient.Close()
	}
	if err := validateSnapshotForFixture(snapshot); err != nil {
		return err
	}
	if err := waitForFreshHolders(ctx, snapshot, cfg.holderReadyTimeout); err != nil {
		return err
	}

	fixtureRouter, err := newFixtureRouter(ctx, snapshot)
	if err != nil {
		return err
	}
	defer fixtureRouter.Close()

	var initialized, granted, unchanged atomic.Uint64
	var failure error
	var failureOnce sync.Once

	work := make(chan uint64)
	var workers sync.WaitGroup
	for worker := 0; worker < cfg.concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for playerID := range work {
				if err := provision(ctx, fixtureRouter, cfg, playerID, &initialized, &granted, &unchanged); err != nil {
					failureOnce.Do(func() { failure = err })
					cancel()
					return
				}
			}
		}()
	}
	for offset := 0; offset < cfg.players; offset++ {
		select {
		case work <- cfg.playerStart + uint64(offset):
		case <-ctx.Done():
		}
	}
	close(work)
	workers.Wait()
	if failure != nil {
		return failure
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	var friendPairs, friendIndexRows, friendQuotaSlots uint64
	if cfg.friendRing {
		friendClients, err := redisdomains.OpenFromEnv(ctx, os.Getenv, redisdomains.SelectAuthority|redisdomains.SelectProjection)
		if err != nil {
			return fmt.Errorf("open friend Redis domains: %w", err)
		}
		defer friendClients.Close()
		namespace, err := rediskey.NewNamespace(cfg.namespaceEnv, cfg.zone)
		if err != nil {
			return err
		}
		repositories, err := friend.NewRedisFriendshipRepositoriesForDomains(
			friendClients.Authority, friendClients.Projection,
			friend.RedisFriendshipOptions{Profile: friend.DefaultFriendRedisProfile(namespace)},
		)
		if err != nil {
			return fmt.Errorf("build friend repositories: %w", err)
		}
		if err := repositories.EnsureScriptsLoaded(ctx); err != nil {
			return fmt.Errorf("load friend scripts: %w", err)
		}
		friendPairs, friendIndexRows, friendQuotaSlots, err = initializeFriendRing(ctx, repositories, cfg)
		if err != nil {
			return err
		}
	}

	fmt.Fprintf(stdout, "{\"players\":%d,\"initialized\":%d,\"granted\":%d,\"already_provisioned\":%d,\"friend_pairs\":%d,\"friend_index_rows\":%d,\"friend_quota_slots\":%d,\"coins\":%d,\"level\":%d,\"route_revision\":%d,\"route_mode\":\"%s\"}\n",
		cfg.players, initialized.Load(), granted.Load(), unchanged.Load(), friendPairs, friendIndexRows, friendQuotaSlots,
		cfg.coins, cfg.level, snapshot.GlobalRevision, cfg.routePath)
	return nil
}

func (cfg config) validate() error {
	if cfg.players <= 0 {
		return errors.New("players must be positive")
	}
	if cfg.playerStart == 0 {
		return errors.New("player-start must not be zero")
	}
	if cfg.coins < 0 {
		return errors.New("coins must not be negative")
	}
	if cfg.level < 1 {
		return errors.New("level must be at least 1")
	}
	if cfg.concurrency <= 0 {
		return errors.New("concurrency must be positive")
	}
	if strings.TrimSpace(cfg.routePath) == "" {
		return errors.New("route must not be blank")
	}
	if strings.TrimSpace(cfg.namespaceEnv) == "" || strings.TrimSpace(cfg.zone) == "" {
		return errors.New("namespace-env and zone are required")
	}
	if cfg.timeout <= 0 || cfg.holderReadyTimeout <= 0 {
		return errors.New("timeouts must be positive")
	}
	if cfg.friendRing && (cfg.players < 2 || cfg.friendRingOffset < 1 || cfg.friendRingOffset >= cfg.players) {
		return errors.New("friend-ring requires at least two players and an offset between 1 and players-1")
	}
	return nil
}

func loadRouteSnapshot(ctx context.Context, cfg config) (actorrouting.RouteSnapshot, redis.UniversalClient, error) {
	if cfg.routePath != "redis-current" {
		resolver, err := actorrouting.LoadStaticResolver(cfg.routePath)
		if err != nil {
			return actorrouting.RouteSnapshot{}, nil, fmt.Errorf("load route snapshot %s: %w", cfg.routePath, err)
		}
		return resolver.Snapshot(), nil, nil
	}

	rawURL := strings.TrimSpace(os.Getenv(redisdomains.LegacyURLEnv))
	clusterURLs := strings.TrimSpace(os.Getenv(redisdomains.LegacyClusterURLEnv))
	if rawURL == "" && clusterURLs == "" {
		return actorrouting.RouteSnapshot{}, nil, errors.New("redis-current requires FARM_REDIS_URL or FARM_REDIS_CLUSTER_URLS")
	}
	client, err := redisdomains.OpenEndpoint(ctx, rawURL, clusterURLs)
	if err != nil {
		return actorrouting.RouteSnapshot{}, nil, fmt.Errorf("connect Redis for route snapshot: %w", err)
	}
	namespace, err := rediskey.NewNamespace(cfg.namespaceEnv, cfg.zone)
	if err != nil {
		client.Close()
		return actorrouting.RouteSnapshot{}, nil, err
	}
	store, err := redisroutecontrol.NewSnapshotStore(client, redisroutecontrol.DefaultOptions(namespace))
	if err != nil {
		client.Close()
		return actorrouting.RouteSnapshot{}, nil, fmt.Errorf("build route snapshot store: %w", err)
	}
	stored, found, err := store.LoadCurrent(ctx)
	if err != nil {
		client.Close()
		return actorrouting.RouteSnapshot{}, nil, fmt.Errorf("load current route snapshot: %w", err)
	}
	if !found {
		client.Close()
		return actorrouting.RouteSnapshot{}, nil, errors.New("current route snapshot is absent")
	}
	return stored.Snapshot, client, nil
}

func validateSnapshotForFixture(snapshot actorrouting.RouteSnapshot) error {
	if snapshot.Realm == "" || snapshot.GlobalRevision == 0 || snapshot.HashVersion != actorrouting.HashVersionXXH64V2 || snapshot.HashSeed == 0 {
		return errors.New("route snapshot is not a complete verified snapshot")
	}
	if snapshot.Player.ShardCount == 0 || len(snapshot.Player.Routes) != int(snapshot.Player.ShardCount) ||
		snapshot.Farm.ShardCount == 0 || len(snapshot.Farm.Routes) != int(snapshot.Farm.ShardCount) {
		return errors.New("route snapshot has incomplete Player/Farm shard tables")
	}
	if len(snapshot.Holders) == 0 {
		return errors.New("route snapshot has no Holder identities")
	}
	now := time.Now().UnixMilli()
	for domain, table := range map[actorrouting.RouteDomain]actorrouting.DomainSnapshot{
		actorrouting.DomainPlayer: snapshot.Player,
		actorrouting.DomainFarm:   snapshot.Farm,
	} {
		for shard, route := range table.Routes {
			if route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
				return fmt.Errorf("route snapshot %s shard %d has invalid Holder index", domain, shard)
			}
			if route.State != actorrouting.ShardStateActive || route.ExpectedWriterEpoch == 0 || route.LeaseUntilMs <= now {
				return fmt.Errorf("route snapshot %s shard %d is not active/fresh", domain, shard)
			}
			holder := snapshot.Holders[route.HolderIndex]
			if holder.State != actorrouting.HolderStateActive || holder.InstanceID == "" || holder.BootID == "" || holder.Endpoint == "" {
				return fmt.Errorf("route snapshot %s shard %d references an incomplete Holder", domain, shard)
			}
		}
	}
	return nil
}

type holderExpectation struct {
	domain         actorrouting.RouteDomain
	holder         actorrouting.HolderRef
	expectedShards int
}

func holderExpectations(snapshot actorrouting.RouteSnapshot) ([]holderExpectation, error) {
	type key struct {
		domain actorrouting.RouteDomain
		index  int
	}
	counts := make(map[key]int)
	for domain, table := range map[actorrouting.RouteDomain]actorrouting.DomainSnapshot{
		actorrouting.DomainPlayer: snapshot.Player,
		actorrouting.DomainFarm:   snapshot.Farm,
	} {
		for _, route := range table.Routes {
			counts[key{domain: domain, index: route.HolderIndex}]++
		}
	}
	result := make([]holderExpectation, 0, len(counts))
	for value, count := range counts {
		if value.index < 0 || value.index >= len(snapshot.Holders) {
			return nil, fmt.Errorf("route snapshot %s references invalid Holder %d", value.domain, value.index)
		}
		result = append(result, holderExpectation{domain: value.domain, holder: snapshot.Holders[value.index], expectedShards: count})
	}
	return result, nil
}

type holderReadyResponse struct {
	Status       string `json:"status"`
	RouteRuntime struct {
		Domain            string `json:"domain"`
		HolderID          string `json:"holder_id"`
		HolderBootID      string `json:"holder_boot_id"`
		ProcessReady      bool   `json:"process_ready"`
		ServingShardCount int    `json:"serving_shard_count"`
		FatalErrorPresent bool   `json:"fatal_error_present"`
		AssignmentHealthy bool   `json:"assignment_healthy"`
		SnapshotRevision  uint64 `json:"snapshot_revision"`
		Membership        struct {
			InstalledMemberships int `json:"installed_memberships"`
			PendingActions       int `json:"pending_actions"`
			DegradedShards       int `json:"degraded_shards"`
		} `json:"membership"`
	} `json:"route_runtime"`
}

func waitForFreshHolders(parent context.Context, snapshot actorrouting.RouteSnapshot, timeout time.Duration) error {
	expectations, err := holderExpectations(snapshot)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	client := &http.Client{Timeout: 2 * time.Second}
	var lastErr error
	for {
		allReady := true
		for _, expected := range expectations {
			if err := probeHolder(ctx, client, snapshot.GlobalRevision, expected); err != nil {
				allReady = false
				lastErr = err
			}
		}
		if allReady {
			return nil
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("Holder freshness gate failed: %w", lastErr)
			}
			return fmt.Errorf("Holder freshness gate failed: %w", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func probeHolder(ctx context.Context, client *http.Client, snapshotRevision uint64, expected holderExpectation) error {
	healthURL, err := holderHealthURL(expected.domain, expected.holder.Endpoint)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("probe %s Holder %s: %w", expected.domain, expected.holder.InstanceID, err)
	}
	defer resp.Body.Close()
	var body holderReadyResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return fmt.Errorf("decode %s Holder %s readiness: %w", expected.domain, expected.holder.InstanceID, err)
	}
	r := body.RouteRuntime
	if resp.StatusCode != http.StatusOK || body.Status != "ready" || !r.ProcessReady || !r.AssignmentHealthy || r.FatalErrorPresent ||
		r.SnapshotRevision != snapshotRevision || r.HolderID != expected.holder.InstanceID || r.HolderBootID != expected.holder.BootID ||
		r.ServingShardCount != expected.expectedShards || r.Membership.InstalledMemberships != expected.expectedShards ||
		r.Membership.PendingActions != 0 || r.Membership.DegradedShards != 0 {
		return fmt.Errorf("Holder %s is not fresh: http=%d status=%s domain=%s holder=%s boot=%s snapshot=%d serving=%d installed=%d pending=%d degraded=%d",
			expected.holder.InstanceID, resp.StatusCode, body.Status, r.Domain, r.HolderID, r.HolderBootID, r.SnapshotRevision,
			r.ServingShardCount, r.Membership.InstalledMemberships, r.Membership.PendingActions, r.Membership.DegradedShards)
	}
	return nil
}

func holderHealthURL(domain actorrouting.RouteDomain, endpoint string) (string, error) {
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		return "", fmt.Errorf("Holder endpoint %q is not host:port: %w", endpoint, err)
	}
	port := ""
	switch domain {
	case actorrouting.DomainPlayer:
		port = "9101"
	case actorrouting.DomainFarm:
		port = "9091"
	default:
		return "", fmt.Errorf("unsupported Holder health domain %s", domain)
	}
	return "http://" + net.JoinHostPort(host, port) + "/readyz", nil
}

type fixtureRouter struct {
	resolver    *actorrouting.StaticResolver
	bootstrap   map[string]logicrpcpb.BootstrapServiceClient
	participant map[string]logicrpcpb.PlayerParticipantServiceClient
	connections []*grpc.ClientConn
}

func newFixtureRouter(ctx context.Context, snapshot actorrouting.RouteSnapshot) (*fixtureRouter, error) {
	resolver, err := actorrouting.NewStaticResolver(snapshot)
	if err != nil {
		return nil, fmt.Errorf("build fixture route resolver: %w", err)
	}
	endpoints := make(map[string]struct{})
	for _, route := range snapshot.Player.Routes {
		endpoints[snapshot.Holders[route.HolderIndex].Endpoint] = struct{}{}
	}
	router := &fixtureRouter{
		resolver:    resolver,
		bootstrap:   make(map[string]logicrpcpb.BootstrapServiceClient, len(endpoints)),
		participant: make(map[string]logicrpcpb.PlayerParticipantServiceClient, len(endpoints)),
	}
	for endpoint := range endpoints {
		dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		connection, dialErr := grpc.DialContext(dialCtx, endpoint,
			grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancel()
		if dialErr != nil {
			router.Close()
			return nil, fmt.Errorf("dial exact Player Holder %s: %w", endpoint, dialErr)
		}
		router.connections = append(router.connections, connection)
		router.bootstrap[endpoint] = logicrpcpb.NewBootstrapServiceClient(connection)
		router.participant[endpoint] = logicrpcpb.NewPlayerParticipantServiceClient(connection)
	}
	return router, nil
}

func (r *fixtureRouter) Close() {
	if r == nil {
		return
	}
	for _, connection := range r.connections {
		_ = connection.Close()
	}
}

// provision makes one player exist and then brings it up to the configured
// balance and level. Both calls are idempotent, so re-running the fixture over
// a range that is already provisioned is a no-op rather than a second grant.
func provision(ctx context.Context, router *fixtureRouter, cfg config, playerID uint64,
	initialized, granted, unchanged *atomic.Uint64) error {
	address := actorrouting.Address{Realm: router.resolver.Realm(), Kind: actorrouting.KindPlayer, ID: playerID}
	assignment, err := router.resolver.Resolve(ctx, address)
	if err != nil {
		return fmt.Errorf("resolve player %d: %w", playerID, err)
	}
	bootstrap := router.bootstrap[assignment.Endpoint]
	participant := router.participant[assignment.Endpoint]
	if bootstrap == nil || participant == nil {
		return fmt.Errorf("player %d resolved to unconfigured Holder %s", playerID, assignment.Endpoint)
	}
	bootstrapID := bootstrapPrefix + strconv.FormatUint(playerID, 10)
	owner, err := bootstrap.EnsureOwnerInitialized(actorrouting.WithRoute(ctx, address, assignment), &logicrpcpb.EnsureOwnerInitializedRequest{
		PlayerId: playerID, BootstrapId: bootstrapID,
	})
	if err != nil {
		return fmt.Errorf("initialize player %d via %s: %w", playerID, assignment.HolderPod, err)
	}
	if owner.GetOwner() == nil {
		return fmt.Errorf("initialize player %d: no owner in response", playerID)
	}
	initialized.Add(1)

	// One operation id per player and grant shape, so a repeated run resolves
	// to the stored receipt instead of granting twice.
	operationID := fmt.Sprintf("fixture-%d-c%d-l%d", playerID, cfg.coins, cfg.level)
	digest := sha256.Sum256([]byte(operationID))
	reward, err := participant.ApplyReward(actorrouting.WithRoute(ctx, address, assignment), &logicrpcpb.ApplyRewardRequest{
		PlayerId: playerID, OperationId: operationID, CoinDelta: cfg.coins,
		ExpDelta: int64(cfg.level-1) * expPerLevel, PayloadHash: digest[:],
	})
	if err != nil {
		return fmt.Errorf("grant player %d via %s: %w", playerID, assignment.HolderPod, err)
	}
	switch reward.GetStatus() {
	case commonpb.CommandStatus_SUCCEEDED:
		granted.Add(1)
	case commonpb.CommandStatus_NO_CHANGE:
		unchanged.Add(1)
	default:
		return fmt.Errorf("grant player %d: status=%s code=%s", playerID, reward.GetStatus(), reward.GetErrorCode())
	}
	return nil
}

type fixtureFriendPair struct {
	low  uint64
	high uint64
}

// friendRingPairs returns the unique canonical edges installed by both the
// fixture and live-farm-bot's ring target calculation. Offsets such as N/2
// naturally produce the same undirected edge twice, so canonical de-dup is
// required before issuing idempotent repository commands.
func friendRingPairs(start uint64, players, offset int) []fixtureFriendPair {
	if start == 0 || players < 2 || offset < 1 || offset >= players {
		return nil
	}
	unique := make(map[fixtureFriendPair]struct{}, players)
	for index := 0; index < players; index++ {
		principal := start + uint64(index)
		peer := start + uint64((index+offset)%players)
		pair := fixtureFriendPair{low: principal, high: peer}
		if pair.low > pair.high {
			pair.low, pair.high = pair.high, pair.low
		}
		if pair.low != pair.high {
			unique[pair] = struct{}{}
		}
	}
	pairs := make([]fixtureFriendPair, 0, len(unique))
	for pair := range unique {
		pairs = append(pairs, pair)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].low == pairs[j].low {
			return pairs[i].high < pairs[j].high
		}
		return pairs[i].low < pairs[j].low
	})
	return pairs
}

func initializeFriendRing(ctx context.Context, repositories *friend.RedisFriendshipRepositories, cfg config) (uint64, uint64, uint64, error) {
	if repositories == nil || repositories.Pair == nil || repositories.Quota == nil || repositories.Index == nil {
		return 0, 0, 0, errors.New("friend ring repositories are unavailable")
	}
	pairs := friendRingPairs(cfg.playerStart, cfg.players, cfg.friendRingOffset)
	if len(pairs) == 0 {
		return 0, 0, 0, errors.New("friend ring produced no canonical pairs")
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type pairJob struct {
		ordinal int
		pair    fixtureFriendPair
	}
	jobs := make(chan pairJob)
	var workers sync.WaitGroup
	var firstErr error
	var firstErrOnce sync.Once
	workerCount := cfg.concurrency
	if workerCount > len(pairs) {
		workerCount = len(pairs)
	}
	for worker := 0; worker < workerCount; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobs {
				if err := activateFixtureFriendPair(workerCtx, repositories, job.pair, job.ordinal); err != nil {
					firstErrOnce.Do(func() {
						firstErr = err
						cancel()
					})
					return
				}
			}
		}()
	}
	for ordinal, pair := range pairs {
		select {
		case jobs <- pairJob{ordinal: ordinal, pair: pair}:
		case <-workerCtx.Done():
			break
		}
		if workerCtx.Err() != nil {
			break
		}
	}
	close(jobs)
	workers.Wait()
	if firstErr != nil {
		return 0, 0, 0, firstErr
	}
	if err := ctx.Err(); err != nil {
		return 0, 0, 0, err
	}
	quotaSlots, err := initializeFriendRingQuotas(ctx, repositories.Quota, pairs, cfg.concurrency)
	if err != nil {
		return 0, 0, 0, err
	}
	return uint64(len(pairs)), uint64(len(pairs) * 2), quotaSlots, nil
}

type fixtureQuotaRelation struct {
	relationID string
	ordinal    int
}

func friendRingQuotaRelations(pairs []fixtureFriendPair) (map[uint64][]fixtureQuotaRelation, error) {
	byPlayer := make(map[uint64][]fixtureQuotaRelation, len(pairs))
	for ordinal, pair := range pairs {
		relationID, err := friend.RelationIDFor(pair.low, pair.high)
		if err != nil {
			return nil, err
		}
		relation := fixtureQuotaRelation{relationID: relationID, ordinal: ordinal}
		byPlayer[pair.low] = append(byPlayer[pair.low], relation)
		byPlayer[pair.high] = append(byPlayer[pair.high], relation)
	}
	for playerID := range byPlayer {
		sort.Slice(byPlayer[playerID], func(left, right int) bool {
			return byPlayer[playerID][left].relationID < byPlayer[playerID][right].relationID
		})
	}
	return byPlayer, nil
}

func initializeFriendRingQuotas(ctx context.Context, repository *friend.RedisQuotaRepository, pairs []fixtureFriendPair, concurrency int) (uint64, error) {
	if repository == nil {
		return 0, errors.New("friend quota repository is unavailable")
	}
	relations, err := friendRingQuotaRelations(pairs)
	if err != nil {
		return 0, err
	}
	playerIDs := make([]uint64, 0, len(relations))
	for playerID := range relations {
		playerIDs = append(playerIDs, playerID)
	}
	sort.Slice(playerIDs, func(left, right int) bool { return playerIDs[left] < playerIDs[right] })

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan uint64)
	var workers sync.WaitGroup
	var completed atomic.Uint64
	var firstErr error
	var firstErrOnce sync.Once
	workerCount := concurrency
	if workerCount > len(playerIDs) {
		workerCount = len(playerIDs)
	}
	for worker := 0; worker < workerCount; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for playerID := range jobs {
				for _, relation := range relations[playerID] {
					if err := initializeFixtureQuotaSlot(workerCtx, repository, playerID, relation); err != nil {
						firstErrOnce.Do(func() {
							firstErr = err
							cancel()
						})
						return
					}
					completed.Add(1)
				}
			}
		}()
	}
	for _, playerID := range playerIDs {
		select {
		case jobs <- playerID:
		case <-workerCtx.Done():
		}
		if workerCtx.Err() != nil {
			break
		}
	}
	close(jobs)
	workers.Wait()
	if firstErr != nil {
		return completed.Load(), firstErr
	}
	if err := ctx.Err(); err != nil {
		return completed.Load(), err
	}
	return completed.Load(), nil
}

func initializeFixtureQuotaSlot(ctx context.Context, repository *friend.RedisQuotaRepository, playerID uint64, relation fixtureQuotaRelation) error {
	workflowID := "live-bot-friend-ring-quota-" + relation.relationID
	reservationID, err := friend.QuotaReservationID(workflowID, playerID, relation.relationID)
	if err != nil {
		return err
	}
	reservation := friend.QuotaReservation{
		ReservationID: reservationID, WorkflowID: workflowID,
		RelationID: relation.relationID, TargetRelationEpoch: 1,
	}
	payloadHash := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:1", relation.relationID, playerID)))
	activatedAtMs := int64(1_780_000_000_000) + int64(relation.ordinal)
	reserve, err := repository.ReserveFriendSlot(ctx, playerID, reservation,
		fixtureQuotaIdentity(workflowID, playerID, "RESERVE"), payloadHash[:], activatedAtMs)
	if err != nil {
		return fmt.Errorf("reserve friend quota player=%d relation=%s: %w", playerID, relation.relationID, err)
	}
	if reserve.Status == friend.StatusNoChange && reserve.Reason == friend.ReasonAlreadyFriends {
		return nil
	}
	if reserve.Status != friend.StatusSucceeded {
		return fmt.Errorf("reserve friend quota player=%d relation=%s: status=%v reason=%v", playerID, relation.relationID, reserve.Status, reserve.Reason)
	}
	finalize, err := repository.FinalizeFriendSlot(ctx, playerID, reservation,
		fixtureQuotaIdentity(workflowID, playerID, "FINALIZE"), payloadHash[:], activatedAtMs)
	if err != nil {
		return fmt.Errorf("finalize friend quota player=%d relation=%s: %w", playerID, relation.relationID, err)
	}
	if finalize.Status != friend.StatusSucceeded {
		return fmt.Errorf("finalize friend quota player=%d relation=%s: status=%v reason=%v", playerID, relation.relationID, finalize.Status, finalize.Reason)
	}
	return nil
}

func fixtureQuotaIdentity(workflowID string, playerID uint64, step string) friend.CommitIdentity {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%s", workflowID, playerID, step)))
	return friend.CommitIdentity{
		WorkAddressID: workflowID, ParticipantInvocationID: fmt.Sprintf("fixture-%x", digest[:12]),
		ParticipantStep: step,
	}
}

func activateFixtureFriendPair(ctx context.Context, repositories *friend.RedisFriendshipRepositories, pair fixtureFriendPair, ordinal int) error {
	relationID, err := friend.RelationIDFor(pair.low, pair.high)
	if err != nil {
		return err
	}
	operationID := fmt.Sprintf("live-bot-friend-ring-%d-%d", pair.low, pair.high)
	payloadHash := sha256.Sum256([]byte(operationID))
	// A deterministic timestamp keeps a repeated fixture invocation an exact
	// idempotent replay instead of silently changing friend-list ordering.
	activatedAtMs := int64(1_780_000_000_000) + int64(ordinal)
	outcome, err := repositories.Pair.ActivateFriendRelation(ctx, friend.ActivateCommand{
		InviterPlayerID: pair.low, RedeemerPlayerID: pair.high, TargetRelationEpoch: 1,
		TokenID: operationID, TokenGeneration: 1, OperationID: operationID, NowMs: activatedAtMs,
	}, friend.CommitIdentity{
		WorkAddressID: relationID, ParticipantInvocationID: operationID, ParticipantStep: "ACTIVATE",
	}, payloadHash[:])
	if err != nil {
		return fmt.Errorf("activate friend pair %s: %w", relationID, err)
	}
	if outcome.Status != friend.StatusSucceeded && outcome.Reason != friend.ReasonAlreadyFriends {
		return fmt.Errorf("activate friend pair %s: status=%v reason=%v", relationID, outcome.Status, outcome.Reason)
	}
	for viewer, peer := range map[uint64]uint64{pair.low: pair.high, pair.high: pair.low} {
		if _, err := repositories.Index.Apply(ctx, viewer, friend.FriendIndexEntry{
			FriendPlayerID: peer, RelationID: relationID, RelationEpoch: 1, ActivatedAtMs: activatedAtMs,
		}); err != nil {
			return fmt.Errorf("apply friend index %d->%d: %w", viewer, peer, err)
		}
	}
	return nil
}
