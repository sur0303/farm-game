// Command friend-opportunity-perf-fixture prepares a bounded, isolated friend
// roster for the real A/B performance experiment. It is test-only: canonical
// Pair authorities and FriendIndex rows are created through the production
// repositories, while non-interactive farm Header/Facts records are installed
// directly so the real read path can evaluate a 20/100/300-friend roster
// without creating hundreds of browser sessions.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/farm/domain"
	"farmgame/internal/farm/readpath"
	"farmgame/internal/friend"
	"farmgame/internal/redisdomains"
	"farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	redisroutecontrol "farmgame/internal/storage/redis/routecontrol"
	"farmgame/internal/workidentity"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type config struct {
	viewer, targetOwner                                   uint64
	viewerCount, friendCount, densityPercent, concurrency int
	fakeBase                                              uint64
	playerAddr, farmAddr, routePath, namespaceEnv, zone   string
	runID, qqFarmConfirm                                  string
}

type result struct {
	RunID             string `json:"run_id"`
	Namespace         string `json:"namespace"`
	Zone              string `json:"zone"`
	Viewer            uint64 `json:"viewer_player_id"`
	ViewerCount       int    `json:"viewer_count,omitempty"`
	ViewerEnd         uint64 `json:"viewer_end,omitempty"`
	TargetOwner       uint64 `json:"target_owner_id"`
	FriendCount       int    `json:"friend_count"`
	VerifiedCount     int    `json:"verified_friend_count"`
	FakePlayers       int    `json:"fake_players"`
	DensityPercent    int    `json:"density_percent"`
	Initialized       uint64 `json:"initialized_players"`
	ActivatedPairs    uint64 `json:"activated_pairs"`
	IndexRowsApplied  uint64 `json:"index_rows_applied"`
	FarmRecordsSeeded uint64 `json:"farm_records_seeded"`
	ElapsedMs         int64  `json:"elapsed_ms"`
	CleanupHint       string `json:"cleanup_hint"`
	CleanupScope      string `json:"cleanup_scope"`
}

var isolatedNamespacePattern = regexp.MustCompile(`^foperf-[a-z0-9][a-z0-9-]{4,24}$`)

const qqFarmLoadtestConfirmation = "uc-test-plus/developer_026-qqfarm/uctestscaleplanqqfarm20260816/z1"

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

func parseFlags() config {
	var cfg config
	flag.Uint64Var(&cfg.viewer, "viewer", 0, "real viewer player id")
	flag.IntVar(&cfg.viewerCount, "viewer-count", 1, "number of consecutive viewers; qqfarm load-test mode only")
	flag.Uint64Var(&cfg.targetOwner, "target-owner", 0, "real target farm owner id")
	flag.IntVar(&cfg.friendCount, "friend-count", 300, "final friend count for viewer and target owner")
	flag.IntVar(&cfg.densityPercent, "density", 0, "actionable fake-farm percentage")
	flag.IntVar(&cfg.concurrency, "concurrency", 16, "bounded setup concurrency")
	flag.Uint64Var(&cfg.fakeBase, "fake-base", 7_000_000_000, "first fake player id")
	flag.StringVar(&cfg.playerAddr, "player-addr", "127.0.0.1:19100", "player-svr gRPC address")
	flag.StringVar(&cfg.farmAddr, "farm-addr", "127.0.0.1:19090", "farm-svr gRPC address used to verify the production opportunity read path")
	flag.StringVar(&cfg.routePath, "route", "deploy/route-snapshot.player.json", "Actor route snapshot")
	flag.StringVar(&cfg.namespaceEnv, "namespace-env", env("REDIS_NAMESPACE_ENVIRONMENT", ""), "Redis namespace environment; must be foperf-<run>")
	flag.StringVar(&cfg.zone, "zone", env("REDIS_NAMESPACE_ZONE", "local"), "Redis namespace zone")
	flag.StringVar(&cfg.runID, "run-id", env("FARM_PERF_RUN_ID", ""), "isolated run id; required; forms the Redis namespace")
	flag.StringVar(&cfg.qqFarmConfirm, "qqfarm-loadtest-confirm", "", "exact fixed-scope confirmation for the dedicated qqfarm load-test namespace")
	flag.Parse()
	return cfg
}

func run(cfg config) error {
	if cfg.viewer == 0 || cfg.viewerCount < 1 || cfg.viewerCount > friend.MaxTargetedOpportunityViewers || cfg.friendCount < 1 ||
		cfg.friendCount > friend.MaxTargetedOpportunityViewers || cfg.densityPercent < 0 || cfg.densityPercent > 100 ||
		cfg.concurrency < 1 || cfg.concurrency > 64 || cfg.fakeBase == 0 {
		return errorsNew("invalid fixture configuration")
	}
	if cfg.qqFarmConfirm == "" && (cfg.viewerCount != 1 || cfg.targetOwner == 0 || cfg.viewer == cfg.targetOwner) {
		return errorsNew("isolated single-viewer mode requires a distinct target-owner")
	}
	if cfg.qqFarmConfirm != "" && cfg.targetOwner != 0 {
		return errorsNew("viewer-range mode derives friends from fake-base and forbids target-owner")
	}
	viewerEnd, ok := boundedRangeEnd(cfg.viewer, cfg.viewerCount)
	if !ok {
		return errorsNew("viewer range overflows uint64")
	}
	fakeEnd, ok := boundedRangeEnd(cfg.fakeBase, cfg.friendCount)
	if !ok || rangesOverlap(cfg.viewer, viewerEnd, cfg.fakeBase, fakeEnd) {
		return errorsNew("fake friend range is invalid or overlaps viewer range")
	}
	namespaceEnv, runID, err := resolveFixtureNamespace(cfg)
	if err != nil {
		return err
	}
	cfg.namespaceEnv = namespaceEnv
	cfg.runID = runID
	if strings.TrimSpace(cfg.zone) == "" {
		cfg.zone = "local"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rawURL := strings.TrimSpace(os.Getenv(redisdomains.LegacyURLEnv))
	clusterURLs := strings.TrimSpace(os.Getenv(redisdomains.LegacyClusterURLEnv))
	if rawURL == "" && clusterURLs == "" {
		return errorsNew("FARM_REDIS_URL or FARM_REDIS_CLUSTER_URLS is required")
	}
	client, err := redisdomains.OpenEndpoint(ctx, rawURL, clusterURLs)
	if err != nil {
		return fmt.Errorf("connect Redis: %w", err)
	}

	namespace, err := rediskey.NewNamespace(cfg.namespaceEnv, cfg.zone)
	if err != nil {
		return err
	}
	repositories, err := friend.NewRedisFriendshipRepositoriesForNamespace(client, namespace)
	if err != nil {
		return err
	}
	if err := repositories.EnsureScriptsLoaded(ctx); err != nil {
		return err
	}
	snapshot, err := loadFixtureSnapshot(ctx, client, namespace, cfg.routePath)
	if err != nil {
		return err
	}
	calculator, err := actorrouting.NewCalculator(snapshot.Realm,
		actorrouting.HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed},
		snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		return err
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: env("REDIS_PHYSICAL_PROFILE_VERSION", "redis-physical-v1"), Namespace: namespace,
		AuthorityLayoutVersion: env("REDIS_AUTHORITY_LAYOUT_VERSION", "v1"),
		WorkflowLayoutVersion:  env("REDIS_WORKFLOW_LAYOUT_VERSION", "v1"),
		WorkflowHashVersion:    uint32(envInt("REDIS_WORKFLOW_HASH_VERSION", 1)),
		WorkflowBucketCount:    uint32(envInt("REDIS_WORKFLOW_BUCKET_COUNT", 256)),
	}
	authorityStore, err := authority.NewStore(client, nil, authority.Options{Profile: profile, Resolver: calculator})
	if err != nil {
		return err
	}
	connection, err := grpc.Dial(cfg.playerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer connection.Close()
	players := logicrpcpb.NewBootstrapServiceClient(connection)

	started := time.Now()
	var initialized, pairs, indexes, farms atomic.Uint64
	if cfg.qqFarmConfirm != "" {
		report, err := prepareViewerRange(ctx, players, client, authorityStore, repositories, cfg, started,
			&initialized, &pairs, &indexes, &farms)
		if err != nil {
			return err
		}
		if err := verifySeededFarmReadPath(ctx, authorityStore, cfg); err != nil {
			return err
		}
		if err := verifyFarmOpportunityRPC(ctx, cfg); err != nil {
			return err
		}
		return printResult(report)
	}

	// Fail closed: the viewer↔targetOwner canonical pair must exist before any
	// fake friends are counted toward friend-count.
	nowMs := time.Now().UnixMilli()
	if err := activateAndIndex(ctx, repositories, cfg, cfg.viewer, cfg.targetOwner, nowMs); err != nil {
		return fmt.Errorf("activate viewer-target pair: %w", err)
	}
	pairs.Add(1)
	indexes.Add(2)
	// Seed an actionable opportunity on the real target owner so next-opportunity
	// / Hint paths have a deterministic FOUND candidate on a fresh namespace.
	if err := seedFarmOpportunity(ctx, client, authorityStore, cfg.targetOwner, nowMs, true); err != nil {
		return fmt.Errorf("seed target-owner opportunity: %w", err)
	}
	farms.Add(1)

	fakeCount := cfg.friendCount - 1
	jobs := make(chan int)
	var firstErr atomic.Value
	var workers sync.WaitGroup
	for worker := 0; worker < cfg.concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for ordinal := range jobs {
				if firstErr.Load() != nil {
					continue
				}
				fakeID := cfg.fakeBase + uint64(ordinal)
				if fakeID == cfg.viewer || fakeID == cfg.targetOwner {
					fakeID += uint64(cfg.friendCount + 1)
				}
				if err := prepareFake(ctx, players, client, authorityStore, repositories, cfg, fakeID, ordinal,
					&initialized, &pairs, &indexes, &farms); err != nil {
					firstErr.CompareAndSwap(nil, err)
				}
			}
		}()
	}
	for ordinal := 0; ordinal < fakeCount; ordinal++ {
		jobs <- ordinal
	}
	close(jobs)
	workers.Wait()
	if value := firstErr.Load(); value != nil {
		return value.(error)
	}

	verifiedCount, err := verifyViewerRoster(ctx, repositories.Index, cfg.viewer, cfg.targetOwner, cfg.friendCount)
	if err != nil {
		return err
	}

	cleanupScope := fmt.Sprintf("%s:%s", cfg.namespaceEnv, cfg.zone)
	report := result{
		RunID: cfg.runID, Namespace: cfg.namespaceEnv, Zone: cfg.zone,
		Viewer: cfg.viewer, TargetOwner: cfg.targetOwner, FriendCount: cfg.friendCount,
		VerifiedCount: verifiedCount, FakePlayers: fakeCount, DensityPercent: cfg.densityPercent,
		Initialized: initialized.Load(), ActivatedPairs: pairs.Load(), IndexRowsApplied: indexes.Load(),
		FarmRecordsSeeded: farms.Load(), ElapsedMs: time.Since(started).Milliseconds(),
		CleanupScope: cleanupScope,
		CleanupHint:  "exact run-scoped cleanup only for namespace " + cleanupScope + "; never FLUSH, CONFIG, SLOWLOG, or broad SCAN+DEL",
	}
	return printResult(report)
}

func resolveFixtureNamespace(cfg config) (string, string, error) {
	if cfg.qqFarmConfirm == "" {
		if cfg.viewerCount != 1 {
			return "", "", errorsNew("viewer-range mode requires the fixed qqfarm confirmation")
		}
		return resolveIsolatedNamespace(cfg.runID, cfg.namespaceEnv)
	}
	if cfg.qqFarmConfirm != qqFarmLoadtestConfirmation || cfg.namespaceEnv != "uctestscaleplanqqfarm20260816" || cfg.zone != "z1" {
		return "", "", errorsNew("qqfarm confirmation or namespace tuple mismatch")
	}
	if cfg.routePath != "redis-current" {
		return "", "", errorsNew("qqfarm mode requires the current durable Redis route snapshot")
	}
	runID := sanitizeRunID(cfg.runID)
	if runID == "" {
		return "", "", errorsNew("run-id is required")
	}
	return cfg.namespaceEnv, runID, nil
}

func loadFixtureSnapshot(ctx context.Context, client redis.UniversalClient, namespace rediskey.Namespace, routePath string) (actorrouting.RouteSnapshot, error) {
	if routePath != "redis-current" {
		resolver, err := actorrouting.LoadStaticResolver(routePath)
		if err != nil {
			return actorrouting.RouteSnapshot{}, err
		}
		return resolver.Snapshot(), nil
	}
	store, err := redisroutecontrol.NewSnapshotStore(client, redisroutecontrol.DefaultOptions(namespace))
	if err != nil {
		return actorrouting.RouteSnapshot{}, err
	}
	stored, found, err := store.LoadCurrent(ctx)
	if err != nil {
		return actorrouting.RouteSnapshot{}, err
	}
	if !found {
		return actorrouting.RouteSnapshot{}, errorsNew("current route snapshot is absent")
	}
	return stored.Snapshot, nil
}

func prepareViewerRange(ctx context.Context, players logicrpcpb.BootstrapServiceClient, client redis.UniversalClient,
	store *authority.Store, repositories *friend.RedisFriendshipRepositories, cfg config, started time.Time,
	initialized, pairs, indexes, farms *atomic.Uint64) (result, error) {
	viewerEnd, _ := boundedRangeEnd(cfg.viewer, cfg.viewerCount)
	nowMs := time.Now().UnixMilli()

	// The friend pool is shared by at most 300 viewers, so both sides remain
	// inside the production 300-friend quota. Fake Player/Farm records are
	// initialized once, while every viewer receives an independent canonical
	// relation and FriendIndex row for all requested friends.
	if err := runBoundedJobs(ctx, cfg.friendCount, cfg.concurrency, func(ordinal int) error {
		fakeID := cfg.fakeBase + uint64(ordinal)
		response, err := players.EnsureOwnerInitialized(ctx, &logicrpcpb.EnsureOwnerInitializedRequest{
			PlayerId: fakeID, BootstrapId: fmt.Sprintf("qqfarm-friend-%s-%d", cfg.runID, fakeID),
		})
		if err != nil || response == nil || response.GetOwner() == nil {
			return fmt.Errorf("initialize shared friend %d: %w", fakeID, err)
		}
		initialized.Add(1)
		actionable := ordinal*100 < cfg.friendCount*cfg.densityPercent
		if err := seedFarmOpportunity(ctx, client, store, fakeID, nowMs, actionable); err != nil {
			return fmt.Errorf("seed shared friend farm %d: %w", fakeID, err)
		}
		farms.Add(1)
		return nil
	}); err != nil {
		return result{}, err
	}

	// Returning-player load tests must Ensure viewers before traffic. Otherwise
	// HTTP login is first-time ActorRecord INSERT, not LoadAndClaim of an
	// existing account. Viewer farms are left to production Join; a partial
	// Header/Facts HSET would make later LoadAndClaim see MALFORMED_RECORD.
	if err := runBoundedJobs(ctx, cfg.viewerCount, cfg.concurrency, func(ordinal int) error {
		viewerID := cfg.viewer + uint64(ordinal)
		response, err := players.EnsureOwnerInitialized(ctx, &logicrpcpb.EnsureOwnerInitializedRequest{
			PlayerId: viewerID, BootstrapId: fmt.Sprintf("qqfarm-viewer-%s-%d", cfg.runID, viewerID),
		})
		if err != nil || response == nil || response.GetOwner() == nil {
			return fmt.Errorf("initialize viewer %d: %w", viewerID, err)
		}
		initialized.Add(1)
		return nil
	}); err != nil {
		return result{}, err
	}

	totalPairs := cfg.viewerCount * cfg.friendCount
	if err := runBoundedJobs(ctx, totalPairs, cfg.concurrency, func(job int) error {
		viewerOrdinal := job / cfg.friendCount
		friendOrdinal := job % cfg.friendCount
		viewer := cfg.viewer + uint64(viewerOrdinal)
		peer := cfg.fakeBase + uint64(friendOrdinal)
		// Lower friend ordinals are the actionable density set and receive the
		// newest activation timestamps, so the top-k opportunity window sees
		// them deterministically.
		activatedAt := nowMs + int64(cfg.friendCount-friendOrdinal)
		if err := activateAndIndex(ctx, repositories, cfg, viewer, peer, activatedAt); err != nil {
			return fmt.Errorf("activate viewer=%d friend=%d: %w", viewer, peer, err)
		}
		pairs.Add(1)
		indexes.Add(2)
		return nil
	}); err != nil {
		return result{}, err
	}

	for ordinal := 0; ordinal < cfg.viewerCount; ordinal++ {
		viewer := cfg.viewer + uint64(ordinal)
		if _, err := verifyViewerRoster(ctx, repositories.Index, viewer, 0, cfg.friendCount); err != nil {
			return result{}, err
		}
		if err := verifyCanonicalFriendships(ctx, repositories.Pair, viewer, consecutiveIDs(cfg.fakeBase, cfg.friendCount)); err != nil {
			return result{}, err
		}
	}
	cleanupScope := fmt.Sprintf("%s:%s", cfg.namespaceEnv, cfg.zone)
	return result{
		RunID: cfg.runID, Namespace: cfg.namespaceEnv, Zone: cfg.zone,
		Viewer: cfg.viewer, ViewerCount: cfg.viewerCount, ViewerEnd: viewerEnd,
		FriendCount: cfg.friendCount, VerifiedCount: cfg.friendCount, FakePlayers: cfg.friendCount,
		DensityPercent: cfg.densityPercent, Initialized: initialized.Load(), ActivatedPairs: pairs.Load(),
		IndexRowsApplied: indexes.Load(), FarmRecordsSeeded: farms.Load(), ElapsedMs: time.Since(started).Milliseconds(),
		CleanupScope: cleanupScope,
		CleanupHint:  "dedicated qqfarm load-test data; clear only through prepare-qqfarm-loadtest clean reset",
	}, nil
}

func verifyCanonicalFriendships(ctx context.Context, pairs *friend.RedisPairRepository, viewer uint64, peers []uint64) error {
	snapshots, err := pairs.BatchGetPairs(ctx, viewer, peers)
	if err != nil {
		return fmt.Errorf("verify canonical friendships viewer=%d: %w", viewer, err)
	}
	if len(snapshots) != len(peers) {
		return errorsNew("canonical friendship verification cardinality mismatch")
	}
	for ordinal, snapshot := range snapshots {
		relationID, err := friend.RelationIDFor(viewer, peers[ordinal])
		if err != nil {
			return err
		}
		if snapshot.Relation.Status != friend.RelationStatusActive || snapshot.Relation.RelationID != relationID ||
			snapshot.Relation.RelationEpoch != 1 || !snapshot.CanIssuePermit(viewer, peers[ordinal]) {
			return fmt.Errorf("canonical friendship viewer=%d peer=%d is not active/permit-capable", viewer, peers[ordinal])
		}
	}
	return nil
}

func verifySeededFarmReadPath(ctx context.Context, store *authority.Store, cfg config) error {
	provider, err := readpath.NewProvider(store)
	if err != nil {
		return err
	}
	friendIDs := consecutiveIDs(cfg.fakeBase, cfg.friendCount)
	headers, err := provider.BatchReadHeaders(ctx, friendIDs)
	if err != nil {
		return fmt.Errorf("verify direct opportunity headers: %w", err)
	}
	evaluations, err := provider.BatchEvaluate(ctx, cfg.viewer, friendIDs, time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("verify direct opportunity facts: %w", err)
	}
	for ordinal := range friendIDs {
		if headers[ordinal].Status != readpath.StatusFound || headers[ordinal].Err != nil {
			return fmt.Errorf("direct opportunity header friend=%d status=%d err=%v", friendIDs[ordinal], headers[ordinal].Status, headers[ordinal].Err)
		}
		if evaluations[ordinal].Status != readpath.StatusFound || evaluations[ordinal].Err != nil {
			return fmt.Errorf("direct opportunity facts friend=%d status=%d err=%v", friendIDs[ordinal], evaluations[ordinal].Status, evaluations[ordinal].Err)
		}
	}
	return nil
}

func verifyFarmOpportunityRPC(ctx context.Context, cfg config) error {
	connection, err := grpc.Dial(cfg.farmAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial farm opportunity read path: %w", err)
	}
	defer connection.Close()
	client := logicrpcpb.NewLogicServiceClient(connection)
	evaluator, err := friend.NewGRPCOpportunityEvaluator(client, 2*time.Second)
	if err != nil {
		return err
	}
	friendIDs := consecutiveIDs(cfg.fakeBase, cfg.friendCount)
	for start := 0; start < len(friendIDs); start += readpath.MaxBatchFarmIDs {
		end := start + readpath.MaxBatchFarmIDs
		if end > len(friendIDs) {
			end = len(friendIDs)
		}
		batch := friendIDs[start:end]
		headers, err := client.BatchReadFarmOpportunityHeader(ctx, &logicrpcpb.BatchReadFarmOpportunityHeaderRequest{OwnerPlayerIds: batch})
		if err != nil {
			return fmt.Errorf("verify farm opportunity header RPC: %w", err)
		}
		evaluations, err := evaluator.BatchEvaluateFriendOpportunity(ctx, cfg.viewer, batch, time.Now().UnixMilli())
		if err != nil {
			return fmt.Errorf("verify farm opportunity exact RPC: %w", err)
		}
		if len(headers.GetResults()) != len(batch) || len(evaluations) != len(batch) {
			return errorsNew("farm opportunity verification cardinality mismatch")
		}
		for ordinal := range batch {
			if headers.GetResults()[ordinal].GetStatus() != logicrpcpb.FarmOpportunityReadStatus_FARM_OPPORTUNITY_READ_STATUS_FOUND {
				return fmt.Errorf("farm opportunity header RPC friend=%d status=%s", batch[ordinal], headers.GetResults()[ordinal].GetStatus())
			}
			if evaluations[ordinal].Status != friend.OpportunityReadReady {
				return fmt.Errorf("farm opportunity exact RPC friend=%d status=%d", batch[ordinal], evaluations[ordinal].Status)
			}
		}
	}
	return nil
}

func consecutiveIDs(start uint64, count int) []uint64 {
	ids := make([]uint64, count)
	for ordinal := range ids {
		ids[ordinal] = start + uint64(ordinal)
	}
	return ids
}

func runBoundedJobs(ctx context.Context, count, concurrency int, fn func(int) error) error {
	jobs := make(chan int)
	var firstErr error
	var firstErrMu sync.Mutex
	var workers sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for ordinal := range jobs {
				if err := fn(ordinal); err != nil {
					firstErrMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					firstErrMu.Unlock()
				}
			}
		}()
	}
	for ordinal := 0; ordinal < count; ordinal++ {
		select {
		case jobs <- ordinal:
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return ctx.Err()
		}
	}
	close(jobs)
	workers.Wait()
	return firstErr
}

func boundedRangeEnd(start uint64, count int) (uint64, bool) {
	if start == 0 || count < 1 || uint64(count-1) > ^uint64(0)-start {
		return 0, false
	}
	return start + uint64(count-1), true
}

func rangesOverlap(aStart, aEnd, bStart, bEnd uint64) bool {
	return aStart <= bEnd && bStart <= aEnd
}

func printResult(report result) error {
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return nil
}

// resolveIsolatedNamespace enforces that Redis writes happen only under an
// isolated foperf-* environment. run-id is always required for operation and
// bootstrap identity. When namespace-env is omitted it is derived as
// foperf-<run-id>; when provided it must already be an isolated foperf-*
// namespace (matrix cells may share one Redis namespace while using distinct
// cell-scoped run-id values for operation identity).
func resolveIsolatedNamespace(runID, namespaceEnv string) (resolvedEnv, resolvedRun string, err error) {
	runID = sanitizeRunID(runID)
	if runID == "" {
		return "", "", errorsNew("run-id is required and must isolate operation/bootstrap identity")
	}
	namespaceEnv = strings.ToLower(strings.TrimSpace(namespaceEnv))
	switch {
	case namespaceEnv == "":
		if strings.HasPrefix(runID, "foperf-") {
			resolvedEnv = runID
			resolvedRun = strings.TrimPrefix(runID, "foperf-")
		} else {
			resolvedRun = runID
			resolvedEnv = "foperf-" + runID
		}
	default:
		resolvedEnv = namespaceEnv
		resolvedRun = runID
		if strings.HasPrefix(resolvedRun, "foperf-") {
			resolvedRun = strings.TrimPrefix(resolvedRun, "foperf-")
		}
	}
	if len(resolvedEnv) > 32 {
		return "", "", errorsNew("namespace longer than 32 characters")
	}
	if !isolatedNamespacePattern.MatchString(resolvedEnv) {
		return "", "", fmt.Errorf("friend opportunity perf fixture: namespace %q is not an isolated foperf-* environment", resolvedEnv)
	}
	if _, err := rediskey.NewNamespace(resolvedEnv, "local"); err != nil {
		return "", "", fmt.Errorf("friend opportunity perf fixture: invalid isolated namespace: %w", err)
	}
	return resolvedEnv, resolvedRun, nil
}

func sanitizeRunID(runID string) string {
	runID = strings.ToLower(strings.TrimSpace(runID))
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z':
			return r
		case r >= '0' && r <= '9':
			return r
		case r == '-' || r == '_':
			return r
		default:
			return -1
		}
	}, runID)
}

func verifyViewerRoster(ctx context.Context, index *friend.RedisFriendIndexRepository, viewer, targetOwner uint64, wantCount int) (int, error) {
	if index == nil {
		return 0, errorsNew("friend index repository is nil")
	}
	seen := make(map[uint64]struct{}, wantCount)
	cursor := ""
	for {
		scan, err := index.Scan(ctx, viewer, cursor, friend.MaxPageSize)
		if err != nil {
			return 0, fmt.Errorf("verify roster scan: %w", err)
		}
		for _, entry := range scan.Entries {
			seen[entry.FriendPlayerID] = struct{}{}
		}
		if !scan.HasMore || len(scan.Entries) == 0 {
			break
		}
		last := scan.Entries[len(scan.Entries)-1]
		cursor = encodeVerifyCursor(viewer, scan.RosterRevision, last)
	}
	if targetOwner != 0 {
		if _, ok := seen[targetOwner]; !ok {
			return len(seen), fmt.Errorf("friend opportunity perf fixture: viewer %d missing required target owner %d after seeding", viewer, targetOwner)
		}
	}
	if len(seen) != wantCount {
		return len(seen), fmt.Errorf("friend opportunity perf fixture: viewer %d verified friend count %d != requested %d", viewer, len(seen), wantCount)
	}
	return len(seen), nil
}

func encodeVerifyCursor(viewer, revision uint64, entry friend.FriendIndexEntry) string {
	// DecodeListCursor-compatible cursor built from the last scanned entry so
	// verification pages the full roster without inventing a second pagination API.
	return friend.ListCursor{
		ViewerPlayerID: viewer, SortMode: friend.SortModeActivatedDesc, RosterRevision: revision,
		LastActivatedAtMs: entry.ActivatedAtMs, LastFriendPlayerID: entry.FriendPlayerID,
	}.Encode()
}

func prepareFake(ctx context.Context, players logicrpcpb.BootstrapServiceClient, client redis.UniversalClient,
	store *authority.Store, repositories *friend.RedisFriendshipRepositories, cfg config, fakeID uint64, ordinal int,
	initialized, pairs, indexes, farms *atomic.Uint64) error {
	bootstrapID := fmt.Sprintf("fo-perf-%s-%d", cfg.runID, fakeID)
	response, err := players.EnsureOwnerInitialized(ctx, &logicrpcpb.EnsureOwnerInitializedRequest{
		PlayerId: fakeID, BootstrapId: bootstrapID,
	})
	if err != nil || response == nil || response.GetOwner() == nil {
		return fmt.Errorf("initialize fake player %d: %w", fakeID, err)
	}
	initialized.Add(1)
	nowMs := time.Now().UnixMilli()
	for _, principal := range []uint64{cfg.viewer, cfg.targetOwner} {
		if err := activateAndIndex(ctx, repositories, cfg, principal, fakeID, nowMs+int64(ordinal+1)); err != nil {
			return err
		}
		pairs.Add(1)
		indexes.Add(2)
	}
	actionable := ordinal*100 < (cfg.friendCount-1)*cfg.densityPercent
	if err := seedFarmOpportunity(ctx, client, store, fakeID, nowMs, actionable); err != nil {
		return err
	}
	farms.Add(1)
	return nil
}

func activateAndIndex(ctx context.Context, repositories *friend.RedisFriendshipRepositories,
	cfg config, principal, peer uint64, activatedAtMs int64) error {
	relationID, err := friend.RelationIDFor(principal, peer)
	if err != nil {
		return err
	}
	operationID := fmt.Sprintf("fo-perf-%s-activate-%d-%d", cfg.runID, principal, peer)
	payloadHash := sha256.Sum256([]byte(operationID))
	outcome, err := repositories.Pair.ActivateFriendRelation(ctx, friend.ActivateCommand{
		InviterPlayerID: principal, RedeemerPlayerID: peer, TargetRelationEpoch: 1,
		TokenID: operationID, TokenGeneration: 1, OperationID: operationID, NowMs: activatedAtMs,
	}, friend.CommitIdentity{
		WorkAddressID: relationID, ParticipantInvocationID: operationID, ParticipantStep: "ACTIVATE",
	}, payloadHash[:])
	if err != nil {
		return err
	}
	if outcome.Status != friend.StatusSucceeded && outcome.Reason != friend.ReasonAlreadyFriends {
		return fmt.Errorf("activate pair %s status=%v reason=%v", relationID, outcome.Status, outcome.Reason)
	}
	entryForPrincipal := friend.FriendIndexEntry{FriendPlayerID: peer, RelationID: relationID,
		RelationEpoch: 1, ActivatedAtMs: activatedAtMs}
	entryForPeer := friend.FriendIndexEntry{FriendPlayerID: principal, RelationID: relationID,
		RelationEpoch: 1, ActivatedAtMs: activatedAtMs}
	if _, err := repositories.Index.Apply(ctx, principal, entryForPrincipal); err != nil {
		return err
	}
	if _, err := repositories.Index.Apply(ctx, peer, entryForPeer); err != nil {
		return err
	}
	return nil
}

func seedFarmOpportunity(ctx context.Context, client redis.UniversalClient, store *authority.Store,
	farmID uint64, nowMs int64, actionable bool) error {
	dueAt := nowMs + int64(time.Hour/time.Millisecond)
	if actionable {
		dueAt = nowMs - int64(time.Minute/time.Millisecond)
	}
	header := domain.FarmOpportunityHeaderV2{
		SchemaVersion: domain.FarmOpportunityHeaderSchemaV2, SourceFarmVersion: 1,
		NextStealableAtMs: dueAt, NextOpportunityAtMs: dueAt,
	}
	facts := domain.FarmOpportunityFactsV1{
		SchemaVersion: domain.FarmOpportunityFactsSchemaV1, SourceFarmVersion: 1,
		Cycles: []domain.FarmOpportunityCycleFactV1{{
			PlotID:     1,
			MatureAtMs: dueAt, RemainingTheftSlots: 10,
		}},
	}
	cycleID, err := workidentity.StableOpaqueID("friend-opportunity-perf-cycle", strconv.FormatUint(farmID, 10))
	if err != nil {
		return err
	}
	facts.Cycles[0].CycleID = cycleID
	headerBytes, err := domain.MarshalFarmOpportunityHeaderV2(header)
	if err != nil {
		return err
	}
	factsBytes, err := domain.MarshalFarmOpportunityFactsV1(facts)
	if err != nil {
		return err
	}
	actorKey, err := store.Keys().ActorRecordFor(actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
	if err != nil {
		return err
	}
	return client.HSet(ctx, actorKey.String(), map[string]any{
		"_meta:record_version":                            "1",
		"model:" + domain.FarmOpportunityHeaderModelField: headerBytes,
		"model:" + domain.FarmOpportunityFactsModelField:  factsBytes,
	}).Err()
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err == nil && value > 0 {
		return value
	}
	return fallback
}

func errorsNew(message string) error {
	return fmt.Errorf("friend opportunity perf fixture: %s", message)
}
