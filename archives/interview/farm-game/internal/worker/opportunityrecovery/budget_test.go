package opportunityrecovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/farm/domain"
	"farmgame/internal/farm/opportunitywakeup"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
)

func TestWorkerScansOnlyOwnedShards(t *testing.T) {
	bucket0 := testBucketForShard(t, 0, 2)
	bucket1 := testBucketForShard(t, 1, 2)
	farm0, payload0 := testFarm(t, 40, 2, domain.FarmOpportunityCycleFactV1{
		PlotID: 1, CycleID: "c0", MatureAtMs: 2000, RemainingTheftSlots: 0,
		Pest: domain.OpportunityIncidentFactV1{IncidentID: "p0", DueAtMs: 1000},
	})
	farm1, payload1 := testFarm(t, 41, 2, domain.FarmOpportunityCycleFactV1{
		PlotID: 1, CycleID: "c1", MatureAtMs: 2000, RemainingTheftSlots: 0,
		Pest: domain.OpportunityIncidentFactV1{IncidentID: "p1", DueAtMs: 1000},
	})
	store := &fakeScheduleStore{
		dueByShard: map[uint32][]authority.ScheduledDueRef{
			0: {{ScheduleType: WorkType, ScheduleID: "s0", DueAtMs: payload0.DueAtMs}},
			1: {{ScheduleType: WorkType, ScheduleID: "s1", DueAtMs: payload1.DueAtMs}},
		},
		snapshots: map[string]authority.ScheduledWorkSnapshot{},
	}
	addSnapshot(t, store, "s0", payload0)
	addSnapshot(t, store, "s1", payload1)
	provider, err := NewStaticWorkerShardAssignmentProvider([]uint32{1})
	if err != nil {
		t.Fatal(err)
	}
	reader := &ownedFarmReader{farms: map[uint64]FarmOpportunity{40: farm0, 41: farm1}}
	worker, err := New(Config{
		ScheduleStore: store, FarmReader: reader, Publisher: &fakePublisher{},
		Buckets: []rediskey.AuthorityBucketRef{bucket0, bucket1}, ShardAssignment: provider,
		Owner: "opportunity-worker", Token: "lease-token", BootID: "boot-1", LeaseTTL: time.Second,
		MaxConcurrent: 1, RoundTimeBudget: time.Minute, ClaimLimit: 8,
		Now: func() time.Time { return time.UnixMilli(3000) },
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.OwnedShards != 1 || stats.Buckets != 1 || stats.Claimed != 1 || store.claimCalls != 1 {
		t.Fatalf("owned-only stats=%+v list=%d claim=%d", stats, store.listCalls, store.claimCalls)
	}
	if store.snapshots[scheduleKey(WorkType, "s0")].State == "COMPLETED" || store.snapshots[scheduleKey(WorkType, "s1")].State != "COMPLETED" {
		t.Fatalf("unexpected schedule states=%+v", store.snapshots)
	}
}

func TestWorkerObservesDueLagBacklogAndEmptyShards(t *testing.T) {
	bucket0 := testBucketForShard(t, 0, 2)
	bucket1 := testBucketForShard(t, 1, 2)
	farm, payload := testFarm(t, 50, 3, domain.FarmOpportunityCycleFactV1{
		PlotID: 1, CycleID: "c", MatureAtMs: 5000, RemainingTheftSlots: 2,
		Water: domain.OpportunityIncidentFactV1{IncidentID: "w", DueAtMs: 1000},
	})
	store := &fakeScheduleStore{
		dueByShard: map[uint32][]authority.ScheduledDueRef{
			0: {},
			1: {{ScheduleType: WorkType, ScheduleID: "lagging", DueAtMs: 1000}},
		},
		snapshots: map[string]authority.ScheduledWorkSnapshot{},
	}
	addSnapshot(t, store, "lagging", payload)
	worker, err := New(Config{
		ScheduleStore: store, FarmReader: &fakeFarmReader{farm: farm}, Publisher: &fakePublisher{},
		Buckets: []rediskey.AuthorityBucketRef{bucket0, bucket1},
		Owner:   "opportunity-worker", Token: "lease-token", BootID: "boot-1", LeaseTTL: time.Second,
		MaxConcurrent: 1, RoundTimeBudget: time.Minute,
		Now: func() time.Time { return time.UnixMilli(4000) },
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.EmptyShards != 1 || stats.DueBacklog != 1 || stats.MaxDueLagMs != 3000 || stats.Claimed != 1 {
		t.Fatalf("observability stats=%+v", stats)
	}
}

func TestWorkerRoundTimeBudgetStopsDiscovery(t *testing.T) {
	bucket0 := testBucketForShard(t, 0, 2)
	bucket1 := testBucketForShard(t, 1, 2)
	store := &fakeScheduleStore{
		listDelay: 30 * time.Millisecond,
		dueByShard: map[uint32][]authority.ScheduledDueRef{
			0: {},
			1: {},
		},
		snapshots: map[string]authority.ScheduledWorkSnapshot{},
	}
	worker, err := New(Config{
		ScheduleStore: store, FarmReader: &fakeFarmReader{}, Publisher: &fakePublisher{},
		Buckets: []rediskey.AuthorityBucketRef{bucket0, bucket1},
		Owner:   "opportunity-worker", Token: "lease-token", BootID: "boot-1", LeaseTTL: time.Second,
		MaxConcurrent: 1, RoundTimeBudget: 20 * time.Millisecond,
		Now: func() time.Time { return time.UnixMilli(1000) },
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := worker.RunOnce(context.Background())
	if stats.BudgetExhausted == 0 || store.listCalls != 1 {
		t.Fatalf("budget stats=%+v err=%v listCalls=%d", stats, err, store.listCalls)
	}
}

func TestWorkerMaxConcurrentBoundsInFlightClaims(t *testing.T) {
	bucket := testBucket(t)
	store := &fakeScheduleStore{due: nil, snapshots: map[string]authority.ScheduledWorkSnapshot{}}
	var active, maxActive atomic.Int32
	reader := &blockingFarmReader{
		active: &active, maxActive: &maxActive,
		block: make(chan struct{}),
		farm: FarmOpportunity{
			FarmID: 60, OwnerID: 1060,
			Header: domain.FarmOpportunityHeaderV2{SchemaVersion: domain.FarmOpportunityHeaderSchemaV2, SourceFarmVersion: 1},
			Facts: domain.FarmOpportunityFactsV1{SchemaVersion: domain.FarmOpportunityFactsSchemaV1, SourceFarmVersion: 1,
				Cycles: []domain.FarmOpportunityCycleFactV1{{PlotID: 1, CycleID: "c", MatureAtMs: 5000, RemainingTheftSlots: 0,
					Pest: domain.OpportunityIncidentFactV1{IncidentID: "p", DueAtMs: 1000}}}},
		},
	}
	for i := 0; i < 4; i++ {
		payload, found, err := opportunitywakeup.FromFacts(60, reader.farm.Facts, 0)
		if err != nil || !found {
			t.Fatalf("payload found=%v err=%v", found, err)
		}
		payload.FarmID = 60
		id := fmt.Sprintf("sched-%d", i)
		store.due = append(store.due, authority.ScheduledDueRef{ScheduleType: WorkType, ScheduleID: id, DueAtMs: payload.DueAtMs})
		addSnapshot(t, store, id, payload)
	}
	worker, err := New(Config{
		ScheduleStore: store, FarmReader: reader, Publisher: &fakePublisher{},
		Buckets: []rediskey.AuthorityBucketRef{bucket},
		Owner:   "opportunity-worker", Token: "lease-token", BootID: "boot-1", LeaseTTL: time.Second,
		MaxConcurrent: 2, ClaimLimit: 4, RoundTimeBudget: time.Minute,
		Now: func() time.Time { return time.UnixMilli(3000) },
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan RoundStats, 1)
	go func() {
		stats, _ := worker.RunOnce(context.Background())
		done <- stats
	}()
	deadline := time.After(2 * time.Second)
	for {
		if maxActive.Load() >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("never reached concurrency 2; max=%d", maxActive.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
	if maxActive.Load() > 2 {
		t.Fatalf("unbounded concurrency max=%d", maxActive.Load())
	}
	close(reader.block)
	stats := <-done
	if stats.Claimed != 4 || maxActive.Load() > 2 {
		t.Fatalf("stats=%+v maxActive=%d", stats, maxActive.Load())
	}
}

type ownedFarmReader struct {
	farms map[uint64]FarmOpportunity
}

func (r *ownedFarmReader) ReadFarmOpportunity(_ context.Context, farmID uint64) (FarmOpportunity, error) {
	farm, ok := r.farms[farmID]
	if !ok {
		return FarmOpportunity{}, ErrFarmNotFound
	}
	return farm, nil
}

type blockingFarmReader struct {
	farm      FarmOpportunity
	block     chan struct{}
	active    *atomic.Int32
	maxActive *atomic.Int32
}

func (r *blockingFarmReader) ReadFarmOpportunity(_ context.Context, _ uint64) (FarmOpportunity, error) {
	cur := r.active.Add(1)
	for {
		prev := r.maxActive.Load()
		if cur <= prev || r.maxActive.CompareAndSwap(prev, cur) {
			break
		}
	}
	defer r.active.Add(-1)
	<-r.block
	return r.farm, nil
}

func addSnapshot(t *testing.T, store *fakeScheduleStore, scheduleID string, payload opportunitywakeup.Payload) {
	t.Helper()
	encoded, err := opportunitywakeup.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(encoded)
	store.snapshots[scheduleKey(WorkType, scheduleID)] = authority.ScheduledWorkSnapshot{
		ScheduleType: WorkType, ScheduleID: scheduleID, Generation: payload.ScheduleGeneration,
		State: "PENDING", WorkVersion: 1, SubjectPlayerID: payload.FarmID,
		PayloadHash: hex.EncodeToString(hash[:]), Payload: encoded, NextDueAtMs: payload.DueAtMs,
	}
}

type mutableShardAssignmentProvider struct {
	mu     sync.RWMutex
	shards []uint32
}

func (p *mutableShardAssignmentProvider) Assignment(_ context.Context) (WorkerShardAssignment, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	shards := append([]uint32(nil), p.shards...)
	return WorkerShardAssignment{OwnedShards: shards}, nil
}

func (p *mutableShardAssignmentProvider) Set(shards []uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.shards = append([]uint32(nil), shards...)
}

func TestWorkerRoundRobinCursorCoversShardsAcrossBudgetExhaustion(t *testing.T) {
	buckets := []rediskey.AuthorityBucketRef{
		testBucketForShard(t, 1, 100),
		testBucketForShard(t, 7, 100),
		testBucketForShard(t, 99, 100),
	}
	store := &fakeScheduleStore{
		listDelay: 100 * time.Millisecond,
		dueByShard: map[uint32][]authority.ScheduledDueRef{
			1:  {},
			7:  {},
			99: {},
		},
		snapshots: map[string]authority.ScheduledWorkSnapshot{},
	}
	provider, err := NewStaticWorkerShardAssignmentProvider([]uint32{99, 1, 7})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := New(Config{
		ScheduleStore: store, FarmReader: &fakeFarmReader{}, Publisher: &fakePublisher{},
		Buckets: buckets, ShardAssignment: provider,
		Owner: "opportunity-worker", Token: "lease-token", BootID: "boot-1", LeaseTTL: time.Second,
		MaxConcurrent: 1, ClaimLimit: 1, DiscoveryLimit: 1, RoundTimeBudget: 20 * time.Millisecond,
		Now: func() time.Time { return time.UnixMilli(1000) },
	})
	if err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 4; round++ {
		stats, err := worker.RunOnce(context.Background())
		if err == nil || stats.BudgetExhausted == 0 {
			t.Fatalf("round=%d stats=%+v err=%v", round, stats, err)
		}
	}

	store.mu.Lock()
	got := append([]uint32(nil), store.listShards...)
	store.mu.Unlock()
	want := []uint32{1, 7, 99, 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round-robin list order=%v want=%v", got, want)
	}
}

func TestWorkerRoundRobinCursorHandlesAssignmentChanges(t *testing.T) {
	buckets := []rediskey.AuthorityBucketRef{
		testBucketForShard(t, 1, 100),
		testBucketForShard(t, 7, 100),
		testBucketForShard(t, 99, 100),
	}
	store := &fakeScheduleStore{
		listDelay: 100 * time.Millisecond,
		dueByShard: map[uint32][]authority.ScheduledDueRef{
			1:  {},
			7:  {},
			99: {},
		},
		snapshots: map[string]authority.ScheduledWorkSnapshot{},
	}
	provider := &mutableShardAssignmentProvider{shards: []uint32{99, 1, 7}}
	worker, err := New(Config{
		ScheduleStore: store, FarmReader: &fakeFarmReader{}, Publisher: &fakePublisher{},
		Buckets: buckets, ShardAssignment: provider,
		Owner: "opportunity-worker", Token: "lease-token", BootID: "boot-1", LeaseTTL: time.Second,
		MaxConcurrent: 1, ClaimLimit: 1, DiscoveryLimit: 1, RoundTimeBudget: 20 * time.Millisecond,
		Now: func() time.Time { return time.UnixMilli(1000) },
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := worker.RunOnce(context.Background())
	if err == nil || first.BudgetExhausted == 0 {
		t.Fatalf("first assignment round stats=%+v err=%v", first, err)
	}
	provider.Set([]uint32{99, 1})
	second, err := worker.RunOnce(context.Background())
	if err == nil || second.BudgetExhausted == 0 || second.OwnedShards != 2 {
		t.Fatalf("changed assignment round stats=%+v err=%v", second, err)
	}

	store.mu.Lock()
	got := append([]uint32(nil), store.listShards...)
	store.mu.Unlock()
	want := []uint32{1, 99}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("assignment-change list order=%v want=%v", got, want)
	}
}

func TestWorkerConcurrentRunOnceIsSerialized(t *testing.T) {
	buckets := []rediskey.AuthorityBucketRef{
		testBucketForShard(t, 1, 100),
		testBucketForShard(t, 7, 100),
		testBucketForShard(t, 99, 100),
	}
	due := authority.ScheduledDueRef{ScheduleType: "other", ScheduleID: "invalid", DueAtMs: 1}
	store := &fakeScheduleStore{
		dueByShard: map[uint32][]authority.ScheduledDueRef{
			1:  []authority.ScheduledDueRef{due},
			7:  []authority.ScheduledDueRef{due},
			99: []authority.ScheduledDueRef{due},
		},
		snapshots: map[string]authority.ScheduledWorkSnapshot{},
	}
	provider, err := NewStaticWorkerShardAssignmentProvider([]uint32{99, 1, 7})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := New(Config{
		ScheduleStore: store, FarmReader: &fakeFarmReader{}, Publisher: &fakePublisher{},
		Buckets: buckets, ShardAssignment: provider,
		Owner: "opportunity-worker", Token: "lease-token", BootID: "boot-1", LeaseTTL: time.Second,
		MaxConcurrent: 1, ClaimLimit: 1, DiscoveryLimit: 1, RoundTimeBudget: time.Minute,
		Now: func() time.Time { return time.UnixMilli(1000) },
	})
	if err != nil {
		t.Fatal(err)
	}
	const runs = 12
	errs := make(chan error, runs)
	var wg sync.WaitGroup
	wg.Add(runs)
	for i := 0; i < runs; i++ {
		go func() {
			defer wg.Done()
			_, err := worker.RunOnce(context.Background())
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	store.mu.Lock()
	got := append([]uint32(nil), store.listShards...)
	store.mu.Unlock()
	sequence := []uint32{1, 7, 99}
	want := make([]uint32, runs)
	for i := range want {
		want[i] = sequence[i%len(sequence)]
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("concurrent list order=%v want=%v", got, want)
	}
}
