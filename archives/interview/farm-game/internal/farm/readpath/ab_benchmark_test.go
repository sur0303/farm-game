package readpath

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/farm/domain"
	"farmgame/internal/socialprofile"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// This benchmark is a deterministic local comparison of Redis command shape,
// allocations and algorithm cost. It is not CAPACITY_PASS evidence; the real
// decision still requires the ws-stress/Prometheus A/B/C matrix.
func BenchmarkFriendOpportunityReadPaths(b *testing.B) {
	for _, friends := range []int{20, 100, socialprofile.Current().EffectiveMaxFriends} {
		for _, density := range []int{0, 5, 20, 60} {
			name := fmt.Sprintf("friends=%d/density=%d", friends, density)
			b.Run(name+"/A_authority_header_facts", func(b *testing.B) {
				fixture := newABFixture(b, friends, density)
				fixture.resetCounters()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := fixture.queryAuthority(); err != nil {
						b.Fatal(err)
					}
				}
				fixture.reportRedisMetrics(b)
			})
			b.Run(name+"/A_narrow_header_then_authority_facts", func(b *testing.B) {
				fixture := newABFixture(b, friends, density)
				fixture.resetCounters()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := fixture.queryNarrowAuthority(); err != nil {
						b.Fatal(err)
					}
				}
				fixture.reportRedisMetrics(b)
			})
			b.Run(name+"/B_summary_portable_pipeline_then_authority", func(b *testing.B) {
				fixture := newABFixture(b, friends, density)
				fixture.resetCounters()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := fixture.querySummaryPortable(); err != nil {
						b.Fatal(err)
					}
				}
				fixture.reportRedisMetrics(b)
			})
			b.Run(name+"/B_summary_cross_key_mget_optimistic", func(b *testing.B) {
				fixture := newABFixture(b, friends, density)
				fixture.resetCounters()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := fixture.querySummaryMGetOptimistic(); err != nil {
						b.Fatal(err)
					}
				}
				fixture.reportRedisMetrics(b)
			})
		}
	}
}

func BenchmarkFriendOpportunitySummaryRefreshWrite(b *testing.B) {
	fixture := newABFixture(b, socialprofile.Current().EffectiveMaxFriends, 20)
	value := encodeSummary(true, fixture.nowMs-1)
	fixture.resetCounters()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fixture.summaryKeys[i%len(fixture.summaryKeys)]
		if err := fixture.client.Set(fixture.ctx, key, value, 0).Err(); err != nil {
			b.Fatal(err)
		}
	}
	fixture.reportRedisMetrics(b)
}

type benchmarkRedisHook struct {
	commands   atomic.Uint64
	roundTrips atomic.Uint64
}

func (h *benchmarkRedisHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *benchmarkRedisHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.commands.Add(1)
		h.roundTrips.Add(1)
		return next(ctx, cmd)
	}
}
func (h *benchmarkRedisHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.commands.Add(uint64(len(cmds)))
		h.roundTrips.Add(1)
		return next(ctx, cmds)
	}
}

type abFixture struct {
	ctx         context.Context
	client      *redis.Client
	provider    *Provider
	narrow      *Provider
	hook        *benchmarkRedisHook
	farmIDs     []uint64
	summaryKeys []string
	nowMs       int64
}

func newABFixture(b *testing.B, friendCount, density int) *abFixture {
	b.Helper()
	server := miniredis.RunT(b)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	b.Cleanup(func() { _ = client.Close() })
	hook := &benchmarkRedisHook{}
	client.AddHook(hook)
	namespace, err := rediskey.NewNamespace("bench", "z1")
	if err != nil {
		b.Fatal(err)
	}
	calculator, err := actorrouting.NewCalculator("bench-realm", actorrouting.HashProfile{
		Version: actorrouting.HashVersionXXH64V2, Seed: 202608080001,
	}, 4096, 4096)
	if err != nil {
		b.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "p1", Namespace: namespace, AuthorityLayoutVersion: "v1",
		WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 64,
	}
	store, err := redisauthority.NewStore(client, nil, redisauthority.Options{Profile: profile, Resolver: calculator})
	if err != nil {
		b.Fatal(err)
	}
	provider, err := NewProvider(store)
	if err != nil {
		b.Fatal(err)
	}
	narrow, err := NewProviderWithHeaderReadOptions(store, HeaderReadOptions{
		Mode: HeaderReadNarrow, NarrowReader: store,
	})
	if err != nil {
		b.Fatal(err)
	}
	projectionKeys, err := rediskey.NewProjectionKeys(namespace)
	if err != nil {
		b.Fatal(err)
	}
	fixture := &abFixture{ctx: context.Background(), client: client, provider: provider, narrow: narrow, hook: hook, nowMs: time.Unix(1_800_000_000, 0).UnixMilli()}
	for index := 0; index < friendCount; index++ {
		farmID := uint64(10_000 + index)
		candidate := index*100 < friendCount*density
		dueAt := fixture.nowMs + int64(index+1)*1000
		if candidate {
			dueAt = fixture.nowMs - int64(index+1)*1000
		}
		header := domain.FarmOpportunityHeaderV2{
			SchemaVersion: domain.FarmOpportunityHeaderSchemaV2, SourceFarmVersion: 7,
			NextStealableAtMs: dueAt, NextOpportunityAtMs: dueAt,
		}
		facts := domain.FarmOpportunityFactsV1{
			SchemaVersion: domain.FarmOpportunityFactsSchemaV1, SourceFarmVersion: 7,
			Cycles: []domain.FarmOpportunityCycleFactV1{{
				PlotID: 1, CycleID: fmt.Sprintf("cycle-%d", farmID), MatureAtMs: dueAt, RemainingTheftSlots: 10,
			}},
		}
		headerBytes, _ := domain.MarshalFarmOpportunityHeaderV2(header)
		factsBytes, _ := domain.MarshalFarmOpportunityFactsV1(facts)
		actorKey, keyErr := store.Keys().ActorRecordFor(actorrouting.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: farmID})
		if keyErr != nil {
			b.Fatal(keyErr)
		}
		if err := client.HSet(fixture.ctx, actorKey.String(), map[string]any{
			"_meta:record_version":                            "1",
			"model:" + domain.FarmOpportunityHeaderModelField: headerBytes,
			"model:" + domain.FarmOpportunityFactsModelField:  factsBytes,
		}).Err(); err != nil {
			b.Fatal(err)
		}
		summaryKey, keyErr := projectionKeys.FarmOpportunity(farmID)
		if keyErr != nil {
			b.Fatal(keyErr)
		}
		if err := client.Set(fixture.ctx, summaryKey.String(), encodeSummary(candidate, dueAt), 0).Err(); err != nil {
			b.Fatal(err)
		}
		fixture.farmIDs = append(fixture.farmIDs, farmID)
		fixture.summaryKeys = append(fixture.summaryKeys, summaryKey.String())
	}
	return fixture
}

func (f *abFixture) queryAuthority() ([]EvaluationResult, error) {
	return f.queryHeadersThenEvaluate(f.provider)
}

func (f *abFixture) queryNarrowAuthority() ([]EvaluationResult, error) {
	return f.queryHeadersThenEvaluate(f.narrow)
}

func (f *abFixture) queryHeadersThenEvaluate(headerProvider *Provider) ([]EvaluationResult, error) {
	var candidates []opportunityCandidate
	for start := 0; start < len(f.farmIDs); start += MaxBatchFarmIDs {
		end := start + MaxBatchFarmIDs
		if end > len(f.farmIDs) {
			end = len(f.farmIDs)
		}
		rows, err := headerProvider.BatchReadHeaders(f.ctx, f.farmIDs[start:end])
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if row.IsCandidate(f.nowMs) {
				candidates = append(candidates, opportunityCandidate{farmID: row.FarmID, atMs: row.Header.NextOpportunityAtMs})
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].atMs > candidates[j].atMs })
	return f.evaluateTop(candidates)
}

// querySummaryPortable issues one GET per farm in bounded pipelines. This is
// portable to native Redis Cluster because go-redis can group pipeline items
// by owner node. It does not assume cross-slot MGET support from a proxy.
func (f *abFixture) querySummaryPortable() ([]EvaluationResult, error) {
	values := make([]string, len(f.summaryKeys))
	for start := 0; start < len(f.summaryKeys); start += MaxBatchFarmIDs {
		end := start + MaxBatchFarmIDs
		if end > len(f.summaryKeys) {
			end = len(f.summaryKeys)
		}
		cmds := make([]*redis.StringCmd, end-start)
		_, err := f.client.Pipelined(f.ctx, func(pipe redis.Pipeliner) error {
			for index := start; index < end; index++ {
				cmds[index-start] = pipe.Get(f.ctx, f.summaryKeys[index])
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		for index, cmd := range cmds {
			value, err := cmd.Result()
			if err != nil {
				return nil, err
			}
			values[start+index] = value
		}
	}
	return f.evaluateSummaryValues(values)
}

// querySummaryMGetOptimistic is a lower bound for managed proxies that prove
// cross-slot MGET fanout. Native Redis Cluster users must not select this path.
func (f *abFixture) querySummaryMGetOptimistic() ([]EvaluationResult, error) {
	values, err := f.client.MGet(f.ctx, f.summaryKeys...).Result()
	if err != nil {
		return nil, err
	}
	strings := make([]string, len(values))
	for index, value := range values {
		text, ok := value.(string)
		if ok {
			strings[index] = text
		}
	}
	return f.evaluateSummaryValues(strings)
}

func (f *abFixture) evaluateSummaryValues(values []string) ([]EvaluationResult, error) {
	var candidates []opportunityCandidate
	for index, text := range values {
		if len(text) != 9 || text[0] == 0 {
			continue
		}
		candidates = append(candidates, opportunityCandidate{farmID: f.farmIDs[index], atMs: int64(binary.BigEndian.Uint64([]byte(text[1:])))})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].atMs > candidates[j].atMs })
	return f.evaluateTop(candidates)
}

func (f *abFixture) evaluateTop(candidates []opportunityCandidate) ([]EvaluationResult, error) {
	limit := len(candidates)
	if limit > 8 {
		limit = 8
	}
	if limit == 0 {
		return nil, nil
	}
	farmIDs := make([]uint64, limit)
	for index := 0; index < limit; index++ {
		farmIDs[index] = candidates[index].farmID
	}
	return f.provider.BatchEvaluate(f.ctx, 99_999, farmIDs, f.nowMs)
}

type opportunityCandidate struct {
	farmID uint64
	atMs   int64
}

func encodeSummary(candidate bool, atMs int64) []byte {
	data := make([]byte, 9)
	if candidate {
		data[0] = 1
	}
	binary.BigEndian.PutUint64(data[1:], uint64(atMs))
	return data
}

func (f *abFixture) resetCounters() {
	f.hook.commands.Store(0)
	f.hook.roundTrips.Store(0)
}

func (f *abFixture) reportRedisMetrics(b *testing.B) {
	b.Helper()
	if b.N == 0 {
		return
	}
	b.ReportMetric(float64(f.hook.commands.Load())/float64(b.N), "redis_cmd/op")
	b.ReportMetric(float64(f.hook.roundTrips.Load())/float64(b.N), "redis_rtt/op")
}
