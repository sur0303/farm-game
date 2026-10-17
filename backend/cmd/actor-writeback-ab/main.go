// Command actor-writeback-ab compares the existing per-turn durable CAS path
// with the explicit dirty_best_effort actor path against an isolated Redis
// namespace. It prints no endpoint or credential and deletes every probe key.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	playerpb "farmgame/api/gen/player"
	"farmgame/internal/actorrouting"
	"farmgame/internal/logic/actor/authority"
	"farmgame/internal/logic/actor/dirtywriteback"
	playeractor "farmgame/internal/player/actor"
	"farmgame/internal/redisdomains"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

var turns = flag.Int("turns", 200, "sequential wallet mutations per mode")

type commandCounter struct {
	total atomic.Uint64
	mu    sync.Mutex
	by    map[string]uint64
}

func newCommandCounter() *commandCounter { return &commandCounter{by: make(map[string]uint64)} }

func (c *commandCounter) DialHook(next redis.DialHook) redis.DialHook { return next }
func (c *commandCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		c.observe(cmd.Name())
		return next(ctx, cmd)
	}
}
func (c *commandCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			c.observe(cmd.Name())
		}
		return next(ctx, cmds)
	}
}
func (c *commandCounter) observe(name string) {
	c.total.Add(1)
	c.mu.Lock()
	c.by[strings.ToUpper(name)]++
	c.mu.Unlock()
}
func (c *commandCounter) reset() {
	c.total.Store(0)
	c.mu.Lock()
	c.by = make(map[string]uint64)
	c.mu.Unlock()
}
func (c *commandCounter) snapshot() (uint64, map[string]uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	by := make(map[string]uint64, len(c.by))
	for name, count := range c.by {
		by[name] = count
	}
	return c.total.Load(), by
}

type latencyReport struct {
	P50US int64 `json:"p50_us"`
	P95US int64 `json:"p95_us"`
	P99US int64 `json:"p99_us"`
	MaxUS int64 `json:"max_us"`
}

type modeReport struct {
	Mode                    string            `json:"mode"`
	Turns                   int               `json:"turns"`
	TurnElapsedMS           int64             `json:"turn_elapsed_ms"`
	TurnThroughputPerSec    float64           `json:"turn_throughput_per_sec"`
	TurnLatency             latencyReport     `json:"turn_latency"`
	HotPathRedisCommands    uint64            `json:"hot_path_redis_commands"`
	HotPathCommandsByName   map[string]uint64 `json:"hot_path_commands_by_name"`
	FinalFlushMS            int64             `json:"final_flush_ms"`
	FinalFlushUS            int64             `json:"final_flush_us"`
	FinalFlushRedisCommands uint64            `json:"final_flush_redis_commands"`
	PersistedRecordVersion  uint64            `json:"persisted_record_version"`
	PersistedMemoryVersion  uint64            `json:"persisted_actor_memory_version"`
	VerifiedCoinDelta       int64             `json:"verified_coin_delta"`
}

type report struct {
	TimestampUTC string       `json:"timestamp_utc"`
	RedisMode    string       `json:"redis_mode"`
	Namespace    string       `json:"namespace"`
	Results      []modeReport `json:"results"`
}

func main() {
	flag.Parse()
	if *turns <= 0 || *turns > 1000 {
		log.Fatal("--turns must be within 1..1000")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	clients, err := redisdomains.OpenFromEnv(ctx, os.Getenv, redisdomains.SelectAuthority)
	if err != nil {
		log.Fatal(err)
	}
	defer clients.Close()
	client := clients.Authority
	counter := newCommandCounter()
	if hookable, ok := client.(interface{ AddHook(redis.Hook) }); ok {
		hookable.AddHook(counter)
	} else {
		log.Fatal("Redis client does not expose command hooks")
	}

	environment := fmt.Sprintf("ab%d", time.Now().Unix()%1_000_000_000)
	namespace, err := rediskey.NewNamespace(environment, "z1")
	if err != nil {
		log.Fatal(err)
	}
	prefix := namespace.KeyPrefix()
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := cleanupNamespace(cleanupCtx, client, "*"+prefix+"*"); err != nil {
			log.Printf("isolated cleanup incomplete: %v", err)
		}
	}()

	calculator, err := actorrouting.NewCalculator(
		"actor-writeback-ab",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608240001},
		64, 64,
	)
	if err != nil {
		log.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "p1", Namespace: namespace,
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1",
		WorkflowHashVersion: 1, WorkflowBucketCount: 64,
	}
	store, err := redisauthority.NewStore(client, nil, redisauthority.Options{Profile: profile, Resolver: calculator})
	if err != nil {
		log.Fatal(err)
	}
	baseID := uint64(time.Now().UnixNano()&0x1fffffffffffffff) + 1_000_000
	results := make([]modeReport, 0, 2)
	for index, mode := range []string{"durable_cas", "dirty_best_effort"} {
		result, runErr := runMode(ctx, store, calculator, counter, baseID+uint64(index+1), mode, *turns)
		if runErr != nil {
			log.Fatalf("%s: %v", mode, runErr)
		}
		results = append(results, result)
	}
	redisMode := "standalone"
	if _, ok := client.(*redis.ClusterClient); ok {
		redisMode = "cluster"
	}
	encoded, _ := json.MarshalIndent(report{
		TimestampUTC: time.Now().UTC().Format(time.RFC3339), RedisMode: redisMode,
		Namespace: prefix, Results: results,
	}, "", "  ")
	fmt.Println(string(encoded))
}

func runMode(ctx context.Context, store *redisauthority.Store, calculator *actorrouting.Calculator, counter *commandCounter, playerID uint64, mode string, count int) (modeReport, error) {
	shard, err := calculator.ShardFor(actorrouting.DomainPlayer, playerID)
	if err != nil {
		return modeReport{}, err
	}
	holderID := "ab-" + strings.ReplaceAll(mode, "_", "-")
	bootID := holderID + "-boot"
	claim, err := store.ClaimShardAuthority(ctx, authoritystore.ClaimShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "actor-writeback-ab", BootID: "actor-writeback-ab-boot",
		},
		TransitionID:           fmt.Sprintf("ab-claim-%s-%d", mode, playerID),
		AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer, VirtualShard: shard,
		NewHolderID: holderID, NewHolderBootID: bootID, RequestedTTLms: 120_000,
	})
	if err != nil || claim.Status != authoritystore.ShardTransitionApplied {
		return modeReport{}, fmt.Errorf("claim shard status=%s: %w", claim.Status, err)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID: claim.Authority.HolderID, ExpectedHolderBootID: claim.Authority.HolderBootID,
		ExpectedAssignmentRevision: claim.Authority.AssignmentRevision,
		ExpectedWriterEpoch:        claim.Authority.WriterEpoch, ExpectedLeaseEpoch: claim.Authority.LeaseEpoch,
	}
	soft := mode == "dirty_best_effort"
	provider, err := authority.NewProvider(authority.ProviderOptions{
		Store: store, Fence: authority.StaticFenceSource{Fence: fence},
		AuthorityLayoutVersion: "v1", BootID: bootID, EnableSoftSave: soft,
	})
	if err != nil {
		return modeReport{}, err
	}
	registry := playeractor.NewAuthorityRegistry(provider, playeractor.RegistryOptions{
		MailboxSize: 64, Idle: playeractor.IdleOptions{Disable: true},
		DirtyWriteback: dirtywriteback.Options{
			FlushInterval: time.Hour, MaxDirtyAge: 2 * time.Hour,
			RetryDelay: time.Second, FlushTimeout: 5 * time.Second,
			Workers: 1, QueueSize: 16,
		},
		DirtyDrainTimeout: 10 * time.Second,
	})
	initial, _, err := registry.Ensure(ctx, playerID, fmt.Sprintf("ab-bootstrap-%s-%d", mode, playerID))
	if err != nil {
		registry.Stop()
		return modeReport{}, err
	}
	initialCoin := initial.GetWallet().GetCoin()
	counter.reset()
	latencies := make([]time.Duration, 0, count)
	started := time.Now()
	for i := 0; i < count; i++ {
		turnStarted := time.Now()
		_, err := registry.SubmitVersioned(ctx, playerID, func(record *playerpb.PlayerRecord, _ uint64) (any, bool, error) {
			record.Wallet.Coin++
			return nil, true, nil
		})
		latencies = append(latencies, time.Since(turnStarted))
		if err != nil {
			registry.Stop()
			return modeReport{}, fmt.Errorf("turn %d: %w", i, err)
		}
	}
	turnElapsed := time.Since(started)
	hotCommands, hotByName := counter.snapshot()
	counter.reset()
	flushStarted := time.Now()
	stopCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	err = registry.StopWithContext(stopCtx)
	cancel()
	flushElapsed := time.Since(flushStarted)
	if err != nil {
		return modeReport{}, fmt.Errorf("final flush: %w", err)
	}
	flushCommands, _ := counter.snapshot()

	rows, err := store.BatchReadModelFields(ctx, actorrouting.DomainPlayer, []uint64{playerID}, []string{authority.RecordModelField})
	if err != nil || len(rows) != 1 || rows[0].Err != nil || !rows[0].Found {
		return modeReport{}, fmt.Errorf("verify row=%+v: %w", rows, err)
	}
	persisted := &playerpb.PlayerRecord{}
	if err := proto.Unmarshal(rows[0].ModelFields[authority.RecordModelField], persisted); err != nil {
		return modeReport{}, err
	}
	delta := persisted.GetWallet().GetCoin() - initialCoin
	if delta != int64(count) {
		return modeReport{}, fmt.Errorf("persisted coin delta=%d want=%d", delta, count)
	}
	throughput := float64(count) / turnElapsed.Seconds()
	return modeReport{
		Mode: mode, Turns: count, TurnElapsedMS: turnElapsed.Milliseconds(),
		TurnThroughputPerSec: throughput, TurnLatency: summarizeLatency(latencies),
		HotPathRedisCommands: hotCommands, HotPathCommandsByName: hotByName,
		FinalFlushMS: flushElapsed.Milliseconds(), FinalFlushUS: flushElapsed.Microseconds(), FinalFlushRedisCommands: flushCommands,
		PersistedRecordVersion: rows[0].RecordVersion,
		PersistedMemoryVersion: persisted.GetActorMemoryVersion(), VerifiedCoinDelta: delta,
	}, nil
}

func summarizeLatency(values []time.Duration) latencyReport {
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pick := func(percent float64) int64 {
		if len(sorted) == 0 {
			return 0
		}
		index := int(float64(len(sorted)-1) * percent)
		return sorted[index].Microseconds()
	}
	return latencyReport{P50US: pick(.50), P95US: pick(.95), P99US: pick(.99), MaxUS: sorted[len(sorted)-1].Microseconds()}
}

func cleanupNamespace(ctx context.Context, client redis.UniversalClient, pattern string) error {
	cleanup := func(ctx context.Context, node *redis.Client) error {
		var cursor uint64
		for {
			keys, next, err := node.Scan(ctx, cursor, pattern, 256).Result()
			if err != nil {
				return err
			}
			if len(keys) > 0 {
				if err := node.Del(ctx, keys...).Err(); err != nil {
					return err
				}
			}
			cursor = next
			if cursor == 0 {
				return nil
			}
		}
	}
	switch typed := client.(type) {
	case *redis.ClusterClient:
		return typed.ForEachMaster(ctx, cleanup)
	case *redis.Client:
		return cleanup(ctx, typed)
	default:
		return errors.New("unsupported Redis client")
	}
}
