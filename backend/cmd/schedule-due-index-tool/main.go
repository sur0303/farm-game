// Command schedule-due-index-tool provides the bounded C5 migration rebuild
// and an isolated shared-vs-dedicated Redis probe. It never prints Redis URLs,
// credentials, keys, actor IDs, or schedule payloads.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/actorrouting"
	"farmgame/internal/redisdomains"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

type config struct {
	command             string
	redisURL            string
	redisClusterURLs    string
	output              string
	environment         string
	zone                string
	profileVersion      string
	authorityLayout     string
	workflowLayout      string
	workflowHash        uint
	workflowBuckets     uint
	routeSnapshot       string
	pageSize            int64
	maxPagesPerShard    int
	confirmBackfillMode string
	ordinaryCount       int
	iterations          int
}

type commandCounter struct{ commands atomic.Uint64 }

func (h *commandCounter) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *commandCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.commands.Add(1)
		return next(ctx, cmd)
	}
}
func (h *commandCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.commands.Add(uint64(len(cmds)))
		return next(ctx, cmds)
	}
}

type probeCell struct {
	Mode                 string  `json:"mode"`
	FoundPerIteration    int     `json:"found_per_iteration"`
	CommandsTotal        uint64  `json:"commands_total"`
	CommandsPerIteration float64 `json:"commands_per_iteration"`
	P50Ms                float64 `json:"p50_ms"`
	P95Ms                float64 `json:"p95_ms"`
	P99Ms                float64 `json:"p99_ms"`
}

type probeReport struct {
	SchemaVersion   string      `json:"schema_version"`
	Purpose         string      `json:"purpose"`
	OrdinaryMembers int         `json:"ordinary_members"`
	Iterations      int         `json:"iterations"`
	Cells           []probeCell `json:"cells"`
	Supports30MDAU  bool        `json:"supports_30m_dau"`
	Note            string      `json:"note"`
}

type backfillReport struct {
	SchemaVersion  string `json:"schema_version"`
	Purpose        string `json:"purpose"`
	Buckets        int    `json:"buckets"`
	Pages          int    `json:"pages"`
	Scanned        int    `json:"scanned"`
	Repaired       int    `json:"repaired"`
	Skipped        int    `json:"skipped"`
	Complete       bool   `json:"complete"`
	Supports30MDAU bool   `json:"supports_30m_dau"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "SCHEDULE_DUE_INDEX_TOOL_ERROR:", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg config
	flag.StringVar(&cfg.command, "command", "probe", "probe or backfill")
	flag.StringVar(&cfg.redisURL, "redis-url", os.Getenv("SCHEDULE_DUE_REDIS_URL"), "Redis URL; never emitted")
	flag.StringVar(&cfg.redisClusterURLs, "redis-cluster-urls", os.Getenv("SCHEDULE_DUE_REDIS_CLUSTER_URLS"), "Redis Cluster seed URLs; never emitted")
	flag.StringVar(&cfg.output, "output", "", "optional JSON output file")
	flag.StringVar(&cfg.environment, "environment", "c5probe", "isolated namespace environment")
	flag.StringVar(&cfg.zone, "zone", fmt.Sprintf("z%d", time.Now().UnixNano()), "isolated namespace zone")
	flag.StringVar(&cfg.profileVersion, "profile-version", "redis-physical-v1", "physical profile version")
	flag.StringVar(&cfg.authorityLayout, "authority-layout", "v1", "authority layout version")
	flag.StringVar(&cfg.workflowLayout, "workflow-layout", "v1", "workflow layout version")
	flag.UintVar(&cfg.workflowHash, "workflow-hash-version", 1, "workflow hash version")
	flag.UintVar(&cfg.workflowBuckets, "workflow-bucket-count", 256, "workflow bucket count")
	flag.StringVar(&cfg.routeSnapshot, "route-snapshot", "", "required for backfill")
	flag.Int64Var(&cfg.pageSize, "page-size", 128, "bounded ZSCAN count hint")
	flag.IntVar(&cfg.maxPagesPerShard, "max-pages-per-shard", 100000, "fail-closed page guard")
	flag.StringVar(&cfg.confirmBackfillMode, "confirm-backfill-mode", "", "required migration mode confirmation: dual_shadow or dedicated_fallback")
	flag.IntVar(&cfg.ordinaryCount, "ordinary-count", 100000, "probe shared-index wr1_ members")
	flag.IntVar(&cfg.iterations, "iterations", 10, "probe iterations per mode")
	flag.Parse()
	if cfg.redisURL == "" && cfg.redisClusterURLs == "" {
		return errors.New("redis-url or redis-cluster-urls is required")
	}
	counter := &commandCounter{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	client, err := redisdomains.OpenEndpoint(ctx, cfg.redisURL, cfg.redisClusterURLs)
	if err != nil {
		return fmt.Errorf("open Redis endpoint: %w", err)
	}
	client.AddHook(counter)
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("Redis ping: %w", err)
	}
	profile, err := physicalProfile(cfg)
	if err != nil {
		return err
	}
	var report any
	switch cfg.command {
	case "probe":
		report, err = runProbe(ctx, client, counter, profile, cfg)
	case "backfill":
		report, err = runBackfill(ctx, client, profile, cfg)
	default:
		return fmt.Errorf("unknown command %q", cfg.command)
	}
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if cfg.output != "" {
		if err := os.WriteFile(cfg.output, append(data, '\n'), 0o600); err != nil {
			return err
		}
	}
	fmt.Println(string(data))
	return nil
}

func physicalProfile(cfg config) (rediskey.PhysicalProfile, error) {
	namespace, err := rediskey.NewNamespace(cfg.environment, cfg.zone)
	if err != nil {
		return rediskey.PhysicalProfile{}, err
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: cfg.profileVersion, Namespace: namespace,
		AuthorityLayoutVersion: cfg.authorityLayout, WorkflowLayoutVersion: cfg.workflowLayout,
		WorkflowHashVersion: uint32(cfg.workflowHash), WorkflowBucketCount: uint32(cfg.workflowBuckets),
	}
	return profile, profile.Validate()
}

func runProbe(ctx context.Context, client redis.UniversalClient, counter *commandCounter, profile rediskey.PhysicalProfile, cfg config) (probeReport, error) {
	if cfg.ordinaryCount < 0 || cfg.iterations <= 0 {
		return probeReport{}, errors.New("ordinary-count and iterations must be non-negative/positive")
	}
	calculator, err := actorrouting.NewCalculator("schedule-due-c5-probe",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608120001}, 64, 64)
	if err != nil {
		return probeReport{}, err
	}
	bucket, err := profile.AuthorityBucketForShard(calculator, actorrouting.DomainFarm, 0)
	if err != nil {
		return probeReport{}, err
	}
	keys, err := rediskey.NewAuthorityKeys(profile, calculator)
	if err != nil {
		return probeReport{}, err
	}
	shared, _ := keys.SourceDueIndex(bucket)
	dedicated, _ := keys.SourceScheduleDueIndex(bucket)
	defer client.Del(context.Background(), shared.String(), dedicated.String())
	if err := client.Del(ctx, shared.String(), dedicated.String()).Err(); err != nil {
		return probeReport{}, err
	}
	nowMs := time.Now().UnixMilli()
	for start := 0; start < cfg.ordinaryCount; start += 1000 {
		end := start + 1000
		if end > cfg.ordinaryCount {
			end = cfg.ordinaryCount
		}
		batch := make([]redis.Z, 0, end-start)
		for index := start; index < end; index++ {
			batch = append(batch, redis.Z{Score: float64(nowMs - 1), Member: fmt.Sprintf("wr1_probe_%09d", index)})
		}
		if err := client.ZAdd(ctx, shared.String(), batch...).Err(); err != nil {
			return probeReport{}, err
		}
	}
	scheduleID, err := workidentity.StableOpaqueID("schedule", "FRIEND_OPPORTUNITY", "c5-probe")
	if err != nil {
		return probeReport{}, err
	}
	member, err := workidentity.EncodeScheduleRefV1("FRIEND_OPPORTUNITY", scheduleID)
	if err != nil {
		return probeReport{}, err
	}
	if err := client.ZAdd(ctx, shared.String(), redis.Z{Score: float64(nowMs), Member: member}).Err(); err != nil {
		return probeReport{}, err
	}
	if err := client.ZAdd(ctx, dedicated.String(), redis.Z{Score: float64(nowMs), Member: member}).Err(); err != nil {
		return probeReport{}, err
	}
	report := probeReport{SchemaVersion: "schedule-due-index-probe-v1", Purpose: "C5 shared versus dedicated discovery",
		OrdinaryMembers: cfg.ordinaryCount, Iterations: cfg.iterations, Supports30MDAU: false,
		Note: "Repository-level isolated Redis evidence only; not a 30M DAU capacity proof."}
	for _, mode := range []authority.ScheduleDueIndexMode{authority.ScheduleDueIndexShared, authority.ScheduleDueIndexDedicatedFallback, authority.ScheduleDueIndexDedicatedOnly} {
		store, storeErr := authority.NewStore(client, nil, authority.Options{
			Profile: profile, Resolver: calculator, ScheduleDueIndexMode: mode,
			ScheduleDueIndexDedicatedReady: mode == authority.ScheduleDueIndexDedicatedOnly,
		})
		if storeErr != nil {
			return report, storeErr
		}
		counter.commands.Store(0)
		latencies := make([]time.Duration, 0, cfg.iterations)
		found := 0
		for iteration := 0; iteration < cfg.iterations; iteration++ {
			started := time.Now()
			refs, listErr := store.WorkLease().ListDueScheduledWork(ctx, bucket, nowMs, 1)
			latencies = append(latencies, time.Since(started))
			if listErr != nil {
				return report, listErr
			}
			if len(refs) == 1 && refs[0].Member == member {
				found++
			}
		}
		commands := counter.commands.Load()
		report.Cells = append(report.Cells, probeCell{Mode: mode.String(), FoundPerIteration: found,
			CommandsTotal: commands, CommandsPerIteration: float64(commands) / float64(cfg.iterations),
			P50Ms: percentileMs(latencies, 0.50), P95Ms: percentileMs(latencies, 0.95), P99Ms: percentileMs(latencies, 0.99)})
	}
	return report, nil
}

func runBackfill(ctx context.Context, client redis.UniversalClient, profile rediskey.PhysicalProfile, cfg config) (backfillReport, error) {
	if cfg.routeSnapshot == "" || cfg.pageSize <= 0 || cfg.pageSize > 1024 || cfg.maxPagesPerShard <= 0 {
		return backfillReport{}, errors.New("route-snapshot, valid page-size, and max-pages-per-shard are required")
	}
	mode, err := authority.ParseScheduleDueIndexMode(cfg.confirmBackfillMode)
	if err != nil || (mode != authority.ScheduleDueIndexDualShadow && mode != authority.ScheduleDueIndexDedicatedFallback) {
		return backfillReport{}, errors.New("confirm-backfill-mode must explicitly be dual_shadow or dedicated_fallback")
	}
	provider, err := actorrouting.LoadStaticAssignmentProvider(cfg.routeSnapshot)
	if err != nil {
		return backfillReport{}, err
	}
	snapshot, err := provider.GetSnapshot(ctx)
	if err != nil {
		return backfillReport{}, err
	}
	calculator, err := actorrouting.NewCalculator(snapshot.Realm,
		actorrouting.HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed}, snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		return backfillReport{}, err
	}
	store, err := authority.NewStore(client, nil, authority.Options{
		Profile: profile, Resolver: calculator, ScheduleDueIndexMode: mode,
	})
	if err != nil {
		return backfillReport{}, err
	}
	report := backfillReport{SchemaVersion: "schedule-due-index-backfill-v1", Purpose: "bounded C5 shared-to-dedicated rebuild", Supports30MDAU: false}
	for shard := uint32(0); shard < snapshot.Farm.ShardCount; shard++ {
		bucket, bucketErr := profile.AuthorityBucketForShard(calculator, actorrouting.DomainFarm, shard)
		if bucketErr != nil {
			return report, bucketErr
		}
		report.Buckets++
		cursor := uint64(0)
		for page := 0; ; page++ {
			if page >= cfg.maxPagesPerShard {
				return report, fmt.Errorf("bucket %d exceeded max-pages-per-shard", shard)
			}
			stats, pageErr := store.WorkLease().BackfillScheduleDueIndexPage(ctx, bucket, cursor, cfg.pageSize)
			if pageErr != nil {
				return report, pageErr
			}
			report.Pages++
			report.Scanned += stats.Scanned
			report.Repaired += stats.Repaired
			report.Skipped += stats.Skipped
			cursor = stats.Cursor
			if cursor == 0 {
				break
			}
		}
	}
	report.Complete = true
	return report, nil
}

func percentileMs(values []time.Duration, quantile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	copyValues := append([]time.Duration(nil), values...)
	sort.Slice(copyValues, func(i, j int) bool { return copyValues[i] < copyValues[j] })
	index := int(float64(len(copyValues)-1) * quantile)
	return float64(copyValues[index]) / float64(time.Millisecond)
}
