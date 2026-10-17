package opportunityrecovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/farm/domain"
	"farmgame/internal/farm/opportunitywakeup"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
)

type testShardResolver struct{ shards uint32 }

func (r testShardResolver) ShardFor(actorrouting.RouteDomain, uint64) (uint32, error) { return 0, nil }
func (r testShardResolver) ShardCount(actorrouting.RouteDomain) (uint32, error) {
	if r.shards == 0 {
		return 1, nil
	}
	return r.shards, nil
}
func (testShardResolver) Profile() actorrouting.HashProfile {
	return actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040001}
}

func testBucket(t *testing.T) rediskey.AuthorityBucketRef {
	t.Helper()
	return testBucketForShard(t, 0, 1)
}

func testBucketForShard(t *testing.T, shard, shardCount uint32) rediskey.AuthorityBucketRef {
	t.Helper()
	if shardCount == 0 {
		shardCount = 1
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion: "test-profile", Namespace: rediskey.Namespace{Environment: "test", Zone: "z1"},
		AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 1,
	}
	bucket, err := profile.AuthorityBucketForShard(testShardResolver{shards: shardCount}, actorrouting.DomainFarm, shard)
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}

type fakeScheduleStore struct {
	mu sync.Mutex

	dueByShard map[uint32][]authority.ScheduledDueRef
	due        []authority.ScheduledDueRef
	snapshots  map[string]authority.ScheduledWorkSnapshot
	claims     map[string]authority.WorkClaim

	leaseActive bool
	nextEpoch   uint64

	listCalls     int
	listShards    []uint32
	claimCalls    int
	getCalls      int
	advanceCalls  int
	completeCalls int
	listDelay     time.Duration
	maxInFlight   int
	inFlight      int
	lastClaim     authority.WorkClaim
	lastAdvance   authority.ScheduledWorkAdvanceResult
	lastComplete  authority.WorkCompleteResult

	claimErr       error
	getErr         error
	advanceErr     error
	completeErr    error
	advanceStatus  authority.WorkLeaseStatus
	completeStatus authority.WorkLeaseStatus
}

func scheduleKey(scheduleType, scheduleID string) string { return scheduleType + "|" + scheduleID }

func (s *fakeScheduleStore) ListDueScheduledWork(ctx context.Context, bucket rediskey.AuthorityBucketRef, _ int64, limit int) ([]authority.ScheduledDueRef, error) {
	s.mu.Lock()
	s.listCalls++
	s.listShards = append(s.listShards, bucket.VirtualShard())
	if s.listDelay > 0 {
		delay := s.listDelay
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		s.mu.Lock()
	}
	due := s.due
	if s.dueByShard != nil {
		due = s.dueByShard[bucket.VirtualShard()]
	}
	if limit < len(due) {
		out := append([]authority.ScheduledDueRef(nil), due[:limit]...)
		s.mu.Unlock()
		return out, nil
	}
	out := append([]authority.ScheduledDueRef(nil), due...)
	s.mu.Unlock()
	return out, nil
}

func (s *fakeScheduleStore) ClaimScheduledWorkWithBoot(_ context.Context, _ rediskey.AuthorityBucketRef, scheduleType, scheduleID, holderID, bootID, token string, _ int64) (authority.WorkClaim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimCalls++
	s.inFlight++
	if s.inFlight > s.maxInFlight {
		s.maxInFlight = s.inFlight
	}
	defer func() { s.inFlight-- }()
	if s.claimErr != nil {
		return authority.WorkClaim{}, s.claimErr
	}
	key := scheduleKey(scheduleType, scheduleID)
	snapshot, ok := s.snapshots[key]
	if !ok || snapshot.State == "COMPLETED" {
		return authority.WorkClaim{Status: authority.WorkClaimNotFound}, nil
	}
	if snapshot.State == "CLAIMED" {
		return authority.WorkClaim{Status: authority.WorkClaimLeaseHeld}, nil
	}
	s.nextEpoch++
	if s.nextEpoch == 0 {
		s.nextEpoch = 1
	}
	snapshot.State = "CLAIMED"
	snapshot.LeaseHolderID = holderID
	snapshot.LeaseHolderBootID = bootID
	snapshot.LeaseEpoch = s.nextEpoch
	s.snapshots[key] = snapshot
	s.leaseActive = true
	claim := authority.WorkClaim{
		Status: authority.WorkClaimClaimed, WorkID: scheduleID, WorkType: scheduleType,
		SubjectPlayerID: snapshot.SubjectPlayerID, Payload: append([]byte(nil), snapshot.Payload...),
		PayloadHash: snapshot.PayloadHash, WorkVersion: snapshot.WorkVersion, Generation: snapshot.Generation,
		LeaseEpoch: s.nextEpoch, LeaseHolderBootID: bootID,
	}
	s.lastClaim = claim
	return claim, nil
}

func (s *fakeScheduleStore) GetScheduledWork(_ context.Context, _ rediskey.AuthorityBucketRef, scheduleType, scheduleID string) (authority.ScheduledWorkSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getCalls++
	if s.getErr != nil {
		return authority.ScheduledWorkSnapshot{}, s.getErr
	}
	snapshot, ok := s.snapshots[scheduleKey(scheduleType, scheduleID)]
	if !ok {
		return authority.ScheduledWorkSnapshot{}, authority.ErrSourceOrderNotFound
	}
	snapshot.Payload = append([]byte(nil), snapshot.Payload...)
	return snapshot, nil
}

func (s *fakeScheduleStore) AdvanceScheduledWork(_ context.Context, _ rediskey.AuthorityBucketRef, scheduleType, scheduleID, holderID, bootID string, leaseEpoch, workVersion, generation uint64, _ string, nextDueAtMs int64, payload []byte, payloadHash string) (authority.ScheduledWorkAdvanceResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advanceCalls++
	if s.advanceErr != nil {
		return authority.ScheduledWorkAdvanceResult{}, s.advanceErr
	}
	if s.advanceStatus != authority.WorkLeaseUnknown {
		return authority.ScheduledWorkAdvanceResult{Status: s.advanceStatus}, nil
	}
	key := scheduleKey(scheduleType, scheduleID)
	snapshot := s.snapshots[key]
	snapshot.State = "PENDING"
	snapshot.Payload = append([]byte(nil), payload...)
	snapshot.PayloadHash = payloadHash
	snapshot.NextDueAtMs = nextDueAtMs
	snapshot.WorkVersion = workVersion + 1
	snapshot.Generation = generation
	snapshot.LeaseHolderID = ""
	snapshot.LeaseHolderBootID = ""
	snapshot.LeaseEpoch = 0
	s.snapshots[key] = snapshot
	s.leaseActive = false
	s.lastAdvance = authority.ScheduledWorkAdvanceResult{Status: authority.WorkLeaseRenewed, WorkVersion: snapshot.WorkVersion, Generation: generation, NextDueAtMs: nextDueAtMs}
	return s.lastAdvance, nil
}

func (s *fakeScheduleStore) CompleteScheduledWork(_ context.Context, _ rediskey.AuthorityBucketRef, scheduleType, scheduleID, holderID, bootID string, leaseEpoch, workVersion, generation uint64, _ string) (authority.WorkCompleteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completeCalls++
	if s.completeErr != nil {
		return authority.WorkCompleteResult{}, s.completeErr
	}
	if s.completeStatus != authority.WorkLeaseUnknown {
		return authority.WorkCompleteResult{Status: s.completeStatus}, nil
	}
	key := scheduleKey(scheduleType, scheduleID)
	snapshot := s.snapshots[key]
	snapshot.State = "COMPLETED"
	snapshot.LeaseHolderID = holderID
	snapshot.LeaseHolderBootID = bootID
	snapshot.LeaseEpoch = leaseEpoch
	snapshot.WorkVersion = workVersion
	snapshot.Generation = generation
	s.snapshots[key] = snapshot
	s.leaseActive = false
	s.lastComplete = authority.WorkCompleteResult{Status: authority.WorkLeaseCompleted}
	return s.lastComplete, nil
}

func (s *fakeScheduleStore) expireLease() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leaseActive = false
	for key, snapshot := range s.snapshots {
		if snapshot.State == "CLAIMED" {
			snapshot.State = "PENDING"
			snapshot.LeaseHolderID = ""
			snapshot.LeaseHolderBootID = ""
			snapshot.LeaseEpoch = 0
			s.snapshots[key] = snapshot
		}
	}
}

type fakeFarmReader struct {
	farm  FarmOpportunity
	err   error
	calls int
}

func (r *fakeFarmReader) ReadFarmOpportunity(_ context.Context, _ uint64) (FarmOpportunity, error) {
	r.calls++
	if r.err != nil {
		return FarmOpportunity{}, r.err
	}
	return r.farm, nil
}

type fakePublisher struct {
	ownerID, farmID, version, revision uint64
	calls                              int
	err                                error
}

func (p *fakePublisher) PublishTargetedHint(_ context.Context, ownerID, farmID, version, revision uint64) error {
	p.calls++
	p.ownerID, p.farmID, p.version, p.revision = ownerID, farmID, version, revision
	return p.err
}

func testFarm(t *testing.T, farmID, version uint64, cycle domain.FarmOpportunityCycleFactV1) (FarmOpportunity, opportunitywakeup.Payload) {
	t.Helper()
	facts := domain.FarmOpportunityFactsV1{SchemaVersion: domain.FarmOpportunityFactsSchemaV1, SourceFarmVersion: version}
	if cycle.PlotID != 0 {
		facts.Cycles = []domain.FarmOpportunityCycleFactV1{cycle}
	}
	boundaryPayload, found, err := opportunitywakeup.FromFacts(farmID, facts, 0)
	if err != nil || !found {
		t.Fatalf("initial wakeup payload found=%v err=%v", found, err)
	}
	header := domain.FarmOpportunityHeaderV2{SchemaVersion: domain.FarmOpportunityHeaderSchemaV2, SourceFarmVersion: version}
	return FarmOpportunity{FarmID: farmID, OwnerID: farmID + 1000, Header: header, Facts: facts}, boundaryPayload
}

func newFakeStore(t *testing.T, payload opportunitywakeup.Payload) (*fakeScheduleStore, rediskey.AuthorityBucketRef) {
	t.Helper()
	bucket := testBucket(t)
	encoded, err := opportunitywakeup.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(encoded)
	scheduleID := "schedule-from-due-ref"
	store := &fakeScheduleStore{
		due: []authority.ScheduledDueRef{{ScheduleType: WorkType, ScheduleID: scheduleID, DueAtMs: payload.DueAtMs}},
		snapshots: map[string]authority.ScheduledWorkSnapshot{
			scheduleKey(WorkType, scheduleID): {
				ScheduleType: WorkType, ScheduleID: scheduleID, Generation: payload.ScheduleGeneration,
				State: "PENDING", WorkVersion: 7, SubjectPlayerID: payload.FarmID,
				PayloadHash: hex.EncodeToString(hash[:]), Payload: encoded, NextDueAtMs: payload.DueAtMs,
			},
		},
	}
	return store, bucket
}

func newWorkerForTest(t *testing.T, store ScheduleStore, reader FarmOpportunityReader, publisher TargetedHintPublisher, bucket rediskey.AuthorityBucketRef, nowMs int64) *Worker {
	t.Helper()
	worker, err := New(Config{
		ScheduleStore: store, FarmReader: reader, Publisher: publisher, Buckets: []rediskey.AuthorityBucketRef{bucket},
		Owner: "opportunity-worker", Token: "lease-token", BootID: "boot-1", LeaseTTL: time.Second,
		MaxConcurrent: 1, RoundTimeBudget: time.Minute,
		Now: func() time.Time { return time.UnixMilli(nowMs) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func TestWorkerPublishesGlobalHintAndAdvancesStrictlyAfterTrustedNow(t *testing.T) {
	farm, payload := testFarm(t, 41, 9, domain.FarmOpportunityCycleFactV1{
		PlotID: 1, CycleID: "cycle-1", MatureAtMs: 2000, RemainingTheftSlots: 2,
		Water: domain.OpportunityIncidentFactV1{IncidentID: "water-1", DueAtMs: 1000},
	})
	store, bucket := newFakeStore(t, payload)
	reader := &fakeFarmReader{farm: farm}
	publisher := &fakePublisher{}
	worker := newWorkerForTest(t, store, reader, publisher, bucket, 1000)

	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Claimed != 1 || stats.Published != 1 || stats.Advanced != 1 || stats.Completed != 0 || stats.Failed != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	if publisher.calls != 1 || publisher.ownerID != farm.OwnerID || publisher.farmID != farm.FarmID || publisher.version != farm.Facts.SourceFarmVersion {
		t.Fatalf("publisher=%+v", publisher)
	}
	if store.advanceCalls != 1 || store.completeCalls != 0 {
		t.Fatalf("schedule lifecycle advance=%d complete=%d", store.advanceCalls, store.completeCalls)
	}
	advancedPayload, err := opportunitywakeup.Unmarshal(store.snapshots[scheduleKey(WorkType, store.due[0].ScheduleID)].Payload)
	if err != nil {
		t.Fatal(err)
	}
	if advancedPayload.DueAtMs != 2000 || advancedPayload.BoundaryMask != domain.OpportunityBoundaryStealable {
		t.Fatalf("strict next payload=%+v", advancedPayload)
	}
}

func TestWorkerCompletesWhenNoFutureBoundaryRemains(t *testing.T) {
	farm, payload := testFarm(t, 42, 3, domain.FarmOpportunityCycleFactV1{
		PlotID: 1, CycleID: "cycle-1", MatureAtMs: 2000, RemainingTheftSlots: 0,
		Pest: domain.OpportunityIncidentFactV1{IncidentID: "pest-1", DueAtMs: 1000},
	})
	store, bucket := newFakeStore(t, payload)
	reader := &fakeFarmReader{farm: farm}
	publisher := &fakePublisher{}
	worker := newWorkerForTest(t, store, reader, publisher, bucket, 3000)

	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Claimed != 1 || stats.Completed != 1 || stats.Advanced != 0 || stats.Published != 0 || store.completeCalls != 1 {
		t.Fatalf("stats=%+v store=%+v", stats, store)
	}
}

func TestWorkerShadowStillAdvancesWithoutPublishing(t *testing.T) {
	farm, payload := testFarm(t, 43, 4, domain.FarmOpportunityCycleFactV1{
		PlotID: 1, CycleID: "cycle-1", MatureAtMs: 2000, RemainingTheftSlots: 2,
		Water: domain.OpportunityIncidentFactV1{IncidentID: "water-1", DueAtMs: 1000},
	})
	store, bucket := newFakeStore(t, payload)
	reader := &fakeFarmReader{farm: farm}
	worker, err := New(Config{
		ScheduleStore: store, FarmReader: reader, Buckets: []rediskey.AuthorityBucketRef{bucket}, Shadow: true,
		Owner: "opportunity-worker", Token: "lease-token", BootID: "boot-1", LeaseTTL: time.Second,
		MaxConcurrent: 1, RoundTimeBudget: time.Minute,
		Now: func() time.Time { return time.UnixMilli(1000) },
	})
	if err != nil {
		t.Fatal(err)
	}
	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !stats.Shadow || stats.Published != 0 || stats.Advanced != 1 || store.advanceCalls != 1 {
		t.Fatalf("shadow stats=%+v", stats)
	}
}

func TestWorkerStaleGenerationCompletesOnlyThroughFence(t *testing.T) {
	oldFarm, payload := testFarm(t, 44, 5, domain.FarmOpportunityCycleFactV1{
		PlotID: 1, CycleID: "cycle-1", MatureAtMs: 2000, RemainingTheftSlots: 2,
	})
	store, bucket := newFakeStore(t, payload)
	newFarm, _ := testFarm(t, 44, 6, domain.FarmOpportunityCycleFactV1{
		PlotID: 2, CycleID: "cycle-2", MatureAtMs: 4000, RemainingTheftSlots: 2,
	})
	reader := &fakeFarmReader{farm: newFarm}
	publisher := &fakePublisher{}
	worker := newWorkerForTest(t, store, reader, publisher, bucket, 2000)

	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.StaleGeneration != 1 || stats.Published != 0 || stats.Advanced != 0 || stats.Completed != 1 || store.completeCalls != 1 {
		t.Fatalf("stale stats=%+v oldFarm=%+v", stats, oldFarm)
	}
	if store.lastClaim.LeaseEpoch == 0 {
		t.Fatal("stale completion did not use a claimed lease fence")
	}
}

func TestWorkerFenceMismatchDoesNotCompleteAndReturnsLeaseLost(t *testing.T) {
	farm, payload := testFarm(t, 45, 7, domain.FarmOpportunityCycleFactV1{
		PlotID: 1, CycleID: "cycle-1", MatureAtMs: 2000, RemainingTheftSlots: 2,
		Pest: domain.OpportunityIncidentFactV1{IncidentID: "pest-later", DueAtMs: 3000},
	})
	store, bucket := newFakeStore(t, payload)
	store.advanceStatus = authority.WorkLeaseFenceMismatch
	worker := newWorkerForTest(t, store, &fakeFarmReader{farm: farm}, &fakePublisher{}, bucket, 2000)

	stats, err := worker.RunOnce(context.Background())
	if !errors.Is(err, ErrLeaseLost) || stats.LeaseLost != 1 || store.completeCalls != 0 {
		t.Fatalf("fence stats=%+v err=%v complete=%d", stats, err, store.completeCalls)
	}
}

func TestWorkerReadFailureLeavesClaimForLaterRetry(t *testing.T) {
	farm, payload := testFarm(t, 46, 8, domain.FarmOpportunityCycleFactV1{
		PlotID: 1, CycleID: "cycle-1", MatureAtMs: 2000, RemainingTheftSlots: 2,
		Pest: domain.OpportunityIncidentFactV1{IncidentID: "pest-later", DueAtMs: 3000},
	})
	store, bucket := newFakeStore(t, payload)
	reader := &fakeFarmReader{farm: farm, err: errors.New("temporary model read")}
	publisher := &fakePublisher{}
	worker := newWorkerForTest(t, store, reader, publisher, bucket, 2000)

	first, err := worker.RunOnce(context.Background())
	if err == nil || first.Claimed != 1 || first.Failed != 1 || store.advanceCalls != 0 || store.completeCalls != 0 {
		t.Fatalf("first retry round stats=%+v err=%v store=%+v", first, err, store)
	}
	secondHeld, err := worker.RunOnce(context.Background())
	if err != nil || secondHeld.LeaseHeld != 1 || reader.calls != 1 {
		t.Fatalf("lease-held retry unexpectedly processed before lease expiry: stats=%+v err=%v", secondHeld, err)
	}
	reader.err = nil
	store.expireLease()
	second, err := worker.RunOnce(context.Background())
	if err != nil || second.Claimed != 1 || second.Advanced != 1 || reader.calls != 2 {
		t.Fatalf("second retry round stats=%+v err=%v readerCalls=%d", second, err, reader.calls)
	}
}

func TestWorkerUsesScheduleIDFromDueRef(t *testing.T) {
	farm, payload := testFarm(t, 47, 10, domain.FarmOpportunityCycleFactV1{
		PlotID: 1, CycleID: "cycle-1", MatureAtMs: 2000, RemainingTheftSlots: 0,
		Pest: domain.OpportunityIncidentFactV1{IncidentID: "pest-1", DueAtMs: 1000},
	})
	store, bucket := newFakeStore(t, payload)
	store.due[0].ScheduleID = "farm-specific-schedule-id"
	oldKey := scheduleKey(WorkType, "schedule-from-due-ref")
	newKey := scheduleKey(WorkType, store.due[0].ScheduleID)
	snapshot := store.snapshots[oldKey]
	snapshot.ScheduleID = store.due[0].ScheduleID
	store.snapshots[newKey] = snapshot
	delete(store.snapshots, oldKey)
	worker := newWorkerForTest(t, store, &fakeFarmReader{farm: farm}, &fakePublisher{}, bucket, 3000)

	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.claimCalls != 1 || store.completeCalls != 1 || store.snapshots[newKey].State != "COMPLETED" {
		t.Fatalf("DueRef schedule ID was not used: calls claim=%d complete=%d snapshots=%+v", store.claimCalls, store.completeCalls, store.snapshots)
	}
}

func TestValidatePayloadRejectsFarmAndGenerationDrift(t *testing.T) {
	facts := domain.FarmOpportunityFactsV1{SchemaVersion: domain.FarmOpportunityFactsSchemaV1, SourceFarmVersion: 2,
		Cycles: []domain.FarmOpportunityCycleFactV1{{PlotID: 1, CycleID: "cycle", MatureAtMs: 1000, RemainingTheftSlots: 1}}}
	payload, found, err := opportunitywakeup.FromFacts(99, facts, 0)
	if err != nil || !found {
		t.Fatal(err)
	}
	encoded, _ := opportunitywakeup.Marshal(payload)
	hash := sha256.Sum256(encoded)
	snapshot := authority.ScheduledWorkSnapshot{ScheduleID: "s", Generation: 3, NextDueAtMs: payload.DueAtMs, Payload: encoded, PayloadHash: hex.EncodeToString(hash[:]), SubjectPlayerID: 99}
	claim := authority.WorkClaim{Generation: 3, Payload: encoded, PayloadHash: hex.EncodeToString(hash[:])}
	if err := validatePayload(payload, snapshot, claim); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("generation drift err=%v", err)
	}
	payload.FarmID = 100
	if err := validatePayload(payload, authority.ScheduledWorkSnapshot{ScheduleID: "s", Generation: 2, NextDueAtMs: payload.DueAtMs, Payload: encoded, PayloadHash: hex.EncodeToString(hash[:]), SubjectPlayerID: 99}, authority.WorkClaim{Generation: 2, Payload: encoded, PayloadHash: hex.EncodeToString(hash[:])}); err == nil {
		t.Fatal("FarmID/generation drift accepted")
	}
}

func TestTargetedHintPublisherFuncAndFarmReadAreSmallPorts(t *testing.T) {
	called := false
	publisher := TargetedHintPublisherFunc(func(_ context.Context, ownerID, farmID, version, revision uint64) error {
		called = ownerID == 1 && farmID == 2 && version == 3 && revision == 4
		return nil
	})
	if err := publisher.PublishTargetedHint(context.Background(), 1, 2, 3, 4); err != nil || !called {
		t.Fatalf("publisher func err=%v called=%v", err, called)
	}
	if reflect.TypeOf(publisher).Kind() != reflect.Func {
		t.Fatal("publisher adapter lost function shape")
	}
}
