package routecontrol

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderidentity"
)

type memoryHolderStore struct {
	mu         sync.Mutex
	records    map[string]HolderRecord
	exclusions map[string]PlacementExclusion
}

func newMemoryHolderStore() *memoryHolderStore {
	return &memoryHolderStore{records: make(map[string]HolderRecord), exclusions: make(map[string]PlacementExclusion)}
}

func (s *memoryHolderStore) key(domain actorrouting.RouteDomain, holderID string) string {
	return string(domain) + "\x00" + holderID
}

func (s *memoryHolderStore) RegisterHolder(_ context.Context, input HolderRegistration) (HolderRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := s.key(input.Domain, input.HolderID)
	if current, ok := s.records[key]; ok {
		if current.BootID == input.BootID && HolderRecordEqual(current, input) {
			return cloneHolderRecord(current), nil
		}
		if current.BootID == input.BootID || current.LeaseUntil.After(input.HeartbeatAt) {
			return HolderRecord{}, ErrHolderBootConflict
		}
	}
	s.records[key] = cloneHolderRecord(input)
	return cloneHolderRecord(input), nil
}

func (s *memoryHolderStore) HeartbeatHolder(_ context.Context, input HolderHeartbeat) (HolderRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := s.key(input.Domain, input.HolderID)
	current, ok := s.records[key]
	if !ok {
		return HolderRecord{}, ErrHolderNotFound
	}
	if current.BootID != input.BootID {
		return HolderRecord{}, ErrHolderBootConflict
	}
	next := current
	next.State = input.State
	next.ProcessReady = input.ProcessReady
	next.Draining = input.Draining
	next.ServingShardCount = input.ServingShardCount
	next.SnapshotRevision = input.SnapshotRevision
	next.HeartbeatAt = input.HeartbeatAt
	next.LeaseUntil = input.LeaseUntil
	next.Sequence = input.Sequence
	next.CapacityWeight = input.CapacityWeight
	next.ResourceSummary = cloneResourceSummary(input.ResourceSummary)
	s.records[key] = next
	return cloneHolderRecord(next), nil
}

func (s *memoryHolderStore) GetHolder(_ context.Context, domain actorrouting.RouteDomain, holderID string) (HolderRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[s.key(domain, holderID)]
	return cloneHolderRecord(record), ok, nil
}

func (s *memoryHolderStore) ListHolders(_ context.Context, domain actorrouting.RouteDomain) ([]HolderRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []HolderRecord
	for _, record := range s.records {
		if record.Domain == domain {
			result = append(result, cloneHolderRecord(record))
		}
	}
	return result, nil
}

func (s *memoryHolderStore) GetPlacementExclusion(_ context.Context, domain actorrouting.RouteDomain, holderID, bootID string) (PlacementExclusion, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, found := s.records[s.key(domain, holderID)]
	if !found || record.BootID != bootID {
		return PlacementExclusion{}, false, nil
	}
	if value, ok := s.exclusions[s.key(domain, holderID)]; ok && value.BootID == bootID {
		return value, true, nil
	}
	revision := uint64(1)
	if stale, ok := s.exclusions[s.key(domain, holderID)]; ok {
		if stale.Revision == ^uint64(0) {
			return PlacementExclusion{}, false, ErrPlacementExclusionUnavailable
		}
		revision = stale.Revision + 1
	}
	return PlacementExclusion{Domain: domain, HolderID: holderID, BootID: bootID, Revision: revision}, true, nil
}

func (s *memoryHolderStore) ExcludePlacement(_ context.Context, domain actorrouting.RouteDomain, holderID, bootID string, precondition PlacementExclusionPrecondition) (PlacementExclusionResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, found := s.records[s.key(domain, holderID)]
	if !found || record.BootID != bootID {
		return PlacementExclusionResult{}, ErrHolderNotFound
	}
	current, ok := s.exclusions[s.key(domain, holderID)]
	if !ok || current.BootID != bootID {
		revision := uint64(1)
		if ok {
			if current.Revision == ^uint64(0) {
				return PlacementExclusionResult{}, ErrPlacementExclusionUnavailable
			}
			revision = current.Revision + 1
		}
		current = PlacementExclusion{Domain: domain, HolderID: holderID, BootID: bootID, Revision: revision}
	}
	if current.Excluded {
		if current.OwnerPlanID == precondition.OwnerPlanID {
			return PlacementExclusionResult{State: current}, nil
		}
		return PlacementExclusionResult{}, ErrPlacementExclusionConflict
	}
	if current.Revision != precondition.ExpectedRevision || precondition.ExpectedExcluded || current.Revision == ^uint64(0) {
		return PlacementExclusionResult{}, ErrPlacementExclusionConflict
	}
	current.Excluded = true
	current.Revision++
	current.OwnerPlanID = precondition.OwnerPlanID
	s.exclusions[s.key(domain, holderID)] = current
	return PlacementExclusionResult{Committed: true, State: current}, nil
}

func (s *memoryHolderStore) DeleteHolderForTest(_ context.Context, domain actorrouting.RouteDomain, holderID string) error {
	s.mu.Lock()
	delete(s.records, s.key(domain, holderID))
	s.mu.Unlock()
	return nil
}

func (s *memoryHolderStore) ExistsHolderForTest(_ context.Context, domain actorrouting.RouteDomain, holderID string) (bool, error) {
	s.mu.Lock()
	_, ok := s.records[s.key(domain, holderID)]
	s.mu.Unlock()
	return ok, nil
}

func holderRegistryTestRecord(now time.Time) HolderRecord {
	return HolderRecord{
		Domain: actorrouting.DomainPlayer, HolderID: "holder-a", BootID: "boot-a",
		LogicEndpoint: "holder-a.internal:9000", HolderControlEndpoint: "holder-a.internal:9443", TLSDNSIdentity: "holder-a.internal",
		State: HolderStateWarming, ProcessReady: false, ServingShardCount: 0,
		StartedAt: now, HeartbeatAt: now, LeaseUntil: now.Add(10 * time.Second),
		Sequence: 1, CapacityWeight: 10,
		ResourceSummary: map[string]string{"cpu": "2"},
	}
}

func TestHolderRegistryDeploymentIdentityIsImmutableAndHeartbeatPreservesIt(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	store := newMemoryHolderStore()
	registry, err := NewHolderRegistry(store, HolderRegistryOptions{Now: nowFunc(&now), LeaseTTL: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	record := holderRegistryTestRecord(now)
	record.DeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-uid-a", WorkloadUID: "workload-uid", Ordinal: 0}
	registered, err := registry.Register(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	if registered.DeploymentIdentity != record.DeploymentIdentity {
		t.Fatalf("registered identity=%+v", registered.DeploymentIdentity)
	}
	drift := record
	drift.DeploymentIdentity.PodUID = "replacement-pod-uid"
	if _, err := registry.Register(context.Background(), drift); !errors.Is(err, ErrHolderBootConflict) {
		t.Fatalf("same-boot deployment drift err=%v", err)
	}
	updated, err := registry.Heartbeat(context.Background(), HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: HolderStateReadyZeroShard, ProcessReady: true, Sequence: 2,
		HeartbeatAt: now.Add(time.Second), LeaseUntil: now.Add(11 * time.Second), CapacityWeight: record.CapacityWeight,
	})
	if err != nil || updated.DeploymentIdentity != record.DeploymentIdentity {
		t.Fatalf("updated=%+v err=%v", updated, err)
	}
	for name, identity := range map[string]holderidentity.DeploymentIdentity{
		"partial":  {PodUID: "pod-only"},
		"negative": {PodUID: "pod", WorkloadUID: "workload", Ordinal: -1},
	} {
		t.Run(name, func(t *testing.T) {
			bad := holderRegistryTestRecord(now)
			bad.HolderID, bad.BootID = "holder-"+name, "boot-"+name
			bad.LogicEndpoint, bad.HolderControlEndpoint, bad.TLSDNSIdentity = "holder.internal:9000", "holder.internal:9443", "holder.internal"
			bad.DeploymentIdentity = identity
			if _, err := registry.Register(context.Background(), bad); !errors.Is(err, ErrHolderRegistryInvalid) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestHolderRecordDigestBindsEveryDeploymentIdentityField(t *testing.T) {
	base := holderRegistryTestRecord(time.UnixMilli(1_800_000_000_000))
	base.DeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-a", WorkloadUID: "workload-a", Ordinal: 1}
	for name, mutate := range map[string]func(*HolderRecord){
		"pod":      func(record *HolderRecord) { record.DeploymentIdentity.PodUID = "pod-b" },
		"workload": func(record *HolderRecord) { record.DeploymentIdentity.WorkloadUID = "workload-b" },
		"ordinal":  func(record *HolderRecord) { record.DeploymentIdentity.Ordinal++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			if HolderRecordEqual(base, changed) || HolderRecordDigest(base) == HolderRecordDigest(changed) {
				t.Fatal("deployment identity drift omitted from digest")
			}
		})
	}
}

func TestStaticBootstrapMayReportFactualServingFromWarming(t *testing.T) {
	if !ValidHolderStateTransition(HolderStateWarming, HolderStateServing) {
		t.Fatal("explicit static-bootstrap WARMING to SERVING transition rejected")
	}
	if ValidHolderStateTransition(HolderStateRegistering, HolderStateServing) {
		t.Fatal("REGISTERING bypassed mandatory WARMING state")
	}
}

func TestDynamicHolderMayReturnToReadyZeroShard(t *testing.T) {
	if !ValidHolderStateTransition(HolderStateServing, HolderStateReadyZeroShard) {
		t.Fatal("dynamic SERVING to READY_ZERO_SHARD transition rejected")
	}
	if ValidHolderStateTransition(HolderStateServing, HolderStateWarming) {
		t.Fatal("SERVING holder regressed to WARMING")
	}
}

func TestHolderRegistryStaticInitialServingRequiresServerVerifier(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	store := newMemoryHolderStore()
	registry, err := NewHolderRegistry(store, HolderRegistryOptions{Now: nowFunc(&now)})
	if err != nil {
		t.Fatal(err)
	}
	record := holderRegistryTestRecord(now)
	if _, err := registry.Register(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	heartbeat := HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: HolderStateServing, ProcessReady: true, ServingShardCount: 1,
		SnapshotRevision: 2, HeartbeatAt: now.Add(time.Second), LeaseUntil: now.Add(11 * time.Second),
		Sequence: 2, CapacityWeight: record.CapacityWeight, ResourceSummary: record.ResourceSummary,
	}
	if _, err := registry.Heartbeat(context.Background(), heartbeat); !errors.Is(err, ErrHolderInitialServingAdmission) {
		t.Fatalf("unverified initial serving err=%v", err)
	}
	durable, found, err := store.GetHolder(context.Background(), record.Domain, record.HolderID)
	if err != nil || !found || durable.State != HolderStateWarming || durable.Sequence != 1 {
		t.Fatalf("rejected transition mutated durable record=%+v found=%v err=%v", durable, found, err)
	}

	verificationCalls := 0
	options := HolderRegistryOptions{Now: nowFunc(&now)}
	options.InitialServingVerifier = func(_ context.Context, previous HolderRecord, candidate HolderHeartbeat) error {
		if previous.State != HolderStateWarming || candidate.State != HolderStateServing {
			t.Fatalf("verification previous=%s candidate=%s", previous.State, candidate.State)
		}
		verificationCalls++
		return nil
	}
	admitting, err := NewHolderRegistry(store, options)
	if err != nil {
		t.Fatal(err)
	}
	serving, err := admitting.Heartbeat(context.Background(), heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	if verificationCalls != 2 || serving.State != HolderStateServing || serving.ServingShardCount != 1 {
		t.Fatalf("verification_calls=%d serving=%+v", verificationCalls, serving)
	}
}

func TestHolderRegistryStaticVerifierCoversReadyZeroShardToServing(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	store := newMemoryHolderStore()
	calls := 0
	options := HolderRegistryOptions{Now: nowFunc(&now)}
	options.InitialServingVerifier = func(_ context.Context, previous HolderRecord, candidate HolderHeartbeat) error {
		if previous.State != HolderStateReadyZeroShard || candidate.State != HolderStateServing {
			t.Fatalf("verification previous=%s candidate=%s", previous.State, candidate.State)
		}
		calls++
		return nil
	}
	registry, err := NewHolderRegistry(store, options)
	if err != nil {
		t.Fatal(err)
	}
	record := holderRegistryTestRecord(now)
	if _, err := registry.Register(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	ready := HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: HolderStateReadyZeroShard, ProcessReady: true, SnapshotRevision: 2,
		HeartbeatAt: now.Add(time.Second), LeaseUntil: now.Add(11 * time.Second), Sequence: 2,
		CapacityWeight: record.CapacityWeight, ResourceSummary: record.ResourceSummary,
	}
	if _, err := registry.Heartbeat(context.Background(), ready); err != nil {
		t.Fatal(err)
	}
	serving := ready
	serving.State = HolderStateServing
	serving.ServingShardCount = 1
	serving.SnapshotRevision = 3
	serving.HeartbeatAt = now.Add(2 * time.Second)
	serving.LeaseUntil = now.Add(12 * time.Second)
	serving.Sequence = 3
	if _, err := registry.Heartbeat(context.Background(), serving); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("verification calls=%d, want pre/post commit", calls)
	}
}

func TestHolderRegistryStaticVerifierDetectsPostCommitRuntimeRace(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	store := newMemoryHolderStore()
	calls := 0
	options := HolderRegistryOptions{Now: nowFunc(&now)}
	options.InitialServingVerifier = func(context.Context, HolderRecord, HolderHeartbeat) error {
		calls++
		if calls == 2 {
			return errors.New("guard fenced during durable commit")
		}
		return nil
	}
	registry, err := NewHolderRegistry(store, options)
	if err != nil {
		t.Fatal(err)
	}
	record := holderRegistryTestRecord(now)
	if _, err := registry.Register(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	heartbeat := HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: HolderStateServing, ProcessReady: true, ServingShardCount: 1,
		SnapshotRevision: 2, HeartbeatAt: now.Add(time.Second), LeaseUntil: now.Add(11 * time.Second),
		Sequence: 2, CapacityWeight: record.CapacityWeight, ResourceSummary: record.ResourceSummary,
	}
	if _, err := registry.Heartbeat(context.Background(), heartbeat); !errors.Is(err, ErrHolderInitialServingAdmission) {
		t.Fatalf("post-commit race err=%v", err)
	}
	durable, found, err := store.GetHolder(context.Background(), record.Domain, record.HolderID)
	if err != nil || !found || durable.State != HolderStateServing || durable.Sequence != 2 {
		t.Fatalf("durable commit must remain detectable for Agent reconciliation record=%+v found=%v err=%v", durable, found, err)
	}
}

func TestHolderRegistryServingMayReturnToReadyZeroShardAndServeAgain(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	store := newMemoryHolderStore()
	verifications := 0
	registry, err := NewHolderRegistry(store, HolderRegistryOptions{
		Now: nowFunc(&now), LeaseTTL: 10 * time.Second, MinLeaseTTL: time.Second, MaxLeaseTTL: time.Minute,
		InitialServingVerifier: func(_ context.Context, previous HolderRecord, candidate HolderHeartbeat) error {
			if previous.State != HolderStateReadyZeroShard || candidate.State != HolderStateServing {
				t.Fatalf("unexpected serving admission %s -> %s", previous.State, candidate.State)
			}
			verifications++
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	record, err := registry.Register(context.Background(), holderRegistryTestRecord(now))
	if err != nil {
		t.Fatal(err)
	}
	heartbeat := HolderHeartbeat{Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: HolderStateReadyZeroShard, ProcessReady: true, SnapshotRevision: 1,
		HeartbeatAt: now.Add(time.Second), LeaseUntil: now.Add(11 * time.Second), Sequence: 2,
		CapacityWeight: 10, ResourceSummary: map[string]string{"cpu": "2"}}
	record, err = registry.Heartbeat(context.Background(), heartbeat)
	if err != nil {
		t.Fatal(err)
	}
	heartbeat.State, heartbeat.ServingShardCount, heartbeat.SnapshotRevision = HolderStateServing, 1, 2
	heartbeat.HeartbeatAt, heartbeat.LeaseUntil, heartbeat.Sequence = now.Add(2*time.Second), now.Add(12*time.Second), 3
	record, err = registry.Heartbeat(context.Background(), heartbeat)
	if err != nil || record.State != HolderStateServing {
		t.Fatalf("first serving=%+v err=%v", record, err)
	}
	// The local membership reconciler may remove the final guard after it has
	// already reported SERVING for this same installed snapshot. Zero-shard is
	// a per-holder fact and must not wait for an unrelated global revision.
	heartbeat.State, heartbeat.ServingShardCount = HolderStateReadyZeroShard, 0
	heartbeat.HeartbeatAt, heartbeat.LeaseUntil, heartbeat.Sequence = now.Add(3*time.Second), now.Add(13*time.Second), 4
	record, err = registry.Heartbeat(context.Background(), heartbeat)
	if err != nil || record.State != HolderStateReadyZeroShard || !record.ProcessReady || record.Draining || record.ServingShardCount != 0 {
		t.Fatalf("post-migration zero shard=%+v err=%v", record, err)
	}
	heartbeat.State, heartbeat.ServingShardCount, heartbeat.SnapshotRevision = HolderStateServing, 1, 3
	heartbeat.HeartbeatAt, heartbeat.LeaseUntil, heartbeat.Sequence = now.Add(4*time.Second), now.Add(14*time.Second), 5
	record, err = registry.Heartbeat(context.Background(), heartbeat)
	if err != nil || record.State != HolderStateServing || verifications != 4 {
		t.Fatalf("second serving=%+v verifications=%d err=%v", record, verifications, err)
	}
}

func TestHolderRegistryServingToReadyZeroShardRequiresExactFlags(t *testing.T) {
	if !ValidHolderStateTransition(HolderStateServing, HolderStateReadyZeroShard) {
		t.Fatal("SERVING -> READY_ZERO_SHARD structural transition rejected")
	}
	now := time.UnixMilli(1_800_000_000_000)
	base := HolderRecord{Domain: actorrouting.DomainPlayer, HolderID: "holder-a", BootID: "boot-a",
		LogicEndpoint: "holder-a.internal:9000", HolderControlEndpoint: "holder-a.internal:9443", TLSDNSIdentity: "holder-a.internal",
		State: HolderStateReadyZeroShard, ProcessReady: true, StartedAt: now, HeartbeatAt: now.Add(time.Second),
		LeaseUntil: now.Add(11 * time.Second), Sequence: 2, CapacityWeight: 10}
	for _, test := range []struct {
		name   string
		mutate func(*HolderRecord)
	}{
		{name: "process-unready", mutate: func(value *HolderRecord) { value.ProcessReady = false }},
		{name: "draining", mutate: func(value *HolderRecord) { value.Draining = true }},
		{name: "nonzero-shards", mutate: func(value *HolderRecord) { value.ServingShardCount = 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := base
			test.mutate(&candidate)
			if err := ValidateHolderRecordCandidate(candidate, now, HolderRegistryOptions{}); !errors.Is(err, ErrHolderRegistryInvalid) {
				t.Fatalf("invalid READY_ZERO_SHARD flags accepted: %v", err)
			}
		})
	}
}

func TestHolderRegistryLifecycleAndTargetPredicate(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	store := newMemoryHolderStore()
	registry, err := NewHolderRegistry(store, HolderRegistryOptions{
		Now: nowFunc(&now), LeaseTTL: 10 * time.Second,
		MinLeaseTTL: time.Second, MaxLeaseTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	record, err := registry.Register(context.Background(), holderRegistryTestRecord(now))
	if err != nil || record.State != HolderStateWarming {
		t.Fatalf("register=%+v err=%v", record, err)
	}
	record, err = registry.Heartbeat(context.Background(), HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: HolderStateReadyZeroShard, ProcessReady: true, SnapshotRevision: 1,
		HeartbeatAt: now.Add(time.Second), LeaseUntil: now.Add(11 * time.Second),
		Sequence: 2, CapacityWeight: 10, ResourceSummary: map[string]string{"cpu": "2"},
	})
	if err != nil || record.State != HolderStateReadyZeroShard {
		t.Fatalf("ready=%+v err=%v", record, err)
	}
	target := HolderTarget{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		LogicEndpoint: record.LogicEndpoint, HolderControlEndpoint: record.HolderControlEndpoint, TLSDNSIdentity: record.TLSDNSIdentity,
		MinimumCapacityWeight: 10,
	}
	if _, err := registry.FindTarget(context.Background(), target); err != nil {
		t.Fatalf("ready target err=%v", err)
	}
	record.ResourceSummary["cpu"] = "mutated"
	stored, _, err := registry.Get(context.Background(), record.Domain, record.HolderID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ResourceSummary["cpu"] != "2" {
		t.Fatalf("store input map was not copied: %+v", stored.ResourceSummary)
	}
	if _, err := registry.Heartbeat(context.Background(), HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: HolderStateDrained, ProcessReady: false, Draining: true,
		HeartbeatAt: now.Add(2 * time.Second), LeaseUntil: now.Add(12 * time.Second),
		SnapshotRevision: 1,
		Sequence:         3, CapacityWeight: 10,
	}); !errors.Is(err, ErrHolderStateTransition) {
		t.Fatalf("skipped state transition err=%v", err)
	}
}

func TestHolderRegistryAcceptsIndependentFarmWorkerDomain(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	store := newMemoryHolderStore()
	registry, err := NewHolderRegistry(store, HolderRegistryOptions{
		Now:         nowFunc(&now),
		LeaseTTL:    10 * time.Second,
		MinLeaseTTL: time.Second,
		MaxLeaseTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	record := holderRegistryTestRecord(now)
	record.Domain = actorrouting.DomainFarmWorker
	record.HolderID = "farm-worker-a"
	record.BootID = "worker-a-boot"
	record.LogicEndpoint = "farm-worker-a.internal:9200"
	record.HolderControlEndpoint = "farm-worker-a.internal:19201"
	if _, err := registry.Register(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	workers, err := registry.List(context.Background(), actorrouting.DomainFarmWorker)
	if err != nil {
		t.Fatal(err)
	}
	if len(workers) != 1 || workers[0].Domain != actorrouting.DomainFarmWorker ||
		workers[0].HolderID != record.HolderID {
		t.Fatalf("workers=%+v", workers)
	}
	farms, err := registry.List(context.Background(), actorrouting.DomainFarm)
	if err != nil {
		t.Fatal(err)
	}
	if len(farms) != 0 {
		t.Fatalf("FARM_WORKER leaked into FARM holders: %+v", farms)
	}
}

func TestHolderRegistryRejectsReplayFreshBootAndExpiredTarget(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	store := newMemoryHolderStore()
	registry, err := NewHolderRegistry(store, HolderRegistryOptions{
		Now: nowFunc(&now), LeaseTTL: 5 * time.Second,
		MinLeaseTTL: time.Second, MaxLeaseTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	record := holderRegistryTestRecord(now)
	record.LeaseUntil = now.Add(5 * time.Second)
	if _, err := registry.Register(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	heartbeat := HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: HolderStateReadyZeroShard, ProcessReady: true,
		HeartbeatAt: now.Add(time.Second), LeaseUntil: now.Add(6 * time.Second),
		Sequence: 2, CapacityWeight: 1,
	}
	if _, err := registry.Heartbeat(context.Background(), heartbeat); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Heartbeat(context.Background(), heartbeat); !errors.Is(err, ErrHolderHeartbeatReplay) {
		t.Fatalf("replay err=%v", err)
	}
	replacement := record
	replacement.BootID = "boot-b"
	if _, err := registry.Register(context.Background(), replacement); !errors.Is(err, ErrHolderBootConflict) {
		t.Fatalf("fresh boot conflict err=%v", err)
	}
	now = now.Add(7 * time.Second)
	replacement.StartedAt = now
	replacement.HeartbeatAt = now
	replacement.LeaseUntil = now.Add(5 * time.Second)
	if _, err := registry.Register(context.Background(), replacement); err != nil {
		t.Fatalf("expired boot replacement err=%v", err)
	}
	target := HolderTarget{
		Domain: replacement.Domain, HolderID: replacement.HolderID, BootID: replacement.BootID,
		LogicEndpoint: replacement.LogicEndpoint, HolderControlEndpoint: replacement.HolderControlEndpoint, TLSDNSIdentity: replacement.TLSDNSIdentity,
		MinimumCapacityWeight: 2,
	}
	if _, err := registry.FindTarget(context.Background(), target); !errors.Is(err, ErrHolderTargetUnavailable) {
		t.Fatalf("capacity target err=%v", err)
	}
}

func TestHolderRegistryEndpointRolesAreMandatoryImmutableAndBootFenced(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	store := newMemoryHolderStore()
	registry, err := NewHolderRegistry(store, HolderRegistryOptions{
		Now: nowFunc(&now), LeaseTTL: 5 * time.Second,
		MinLeaseTTL: time.Second, MaxLeaseTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	missingControl := holderRegistryTestRecord(now)
	missingControl.HolderControlEndpoint = ""
	if _, err := registry.Register(context.Background(), missingControl); !errors.Is(err, ErrHolderRegistryInvalid) {
		t.Fatalf("missing HolderControl endpoint err=%v", err)
	}

	oldBoot := holderRegistryTestRecord(now)
	oldBoot.LeaseUntil = now.Add(5 * time.Second)
	if _, err := registry.Register(context.Background(), oldBoot); err != nil {
		t.Fatal(err)
	}
	sameBootDrift := oldBoot
	sameBootDrift.LogicEndpoint = "drifted-logic.internal:9000"
	sameBootDrift.HolderControlEndpoint = "drifted-control.internal:9443"
	if _, err := registry.Register(context.Background(), sameBootDrift); !errors.Is(err, ErrHolderBootConflict) {
		t.Fatalf("same-boot endpoint drift err=%v", err)
	}

	now = now.Add(6 * time.Second)
	newBoot := holderRegistryTestRecord(now)
	newBoot.BootID = "boot-b"
	newBoot.LogicEndpoint = "holder-b.logic:9000"
	newBoot.HolderControlEndpoint = "holder-b.control:9443"
	newBoot.StartedAt = now
	newBoot.HeartbeatAt = now
	newBoot.LeaseUntil = now.Add(5 * time.Second)
	if _, err := registry.Register(context.Background(), newBoot); err != nil {
		t.Fatal(err)
	}
	oldBoot.StartedAt = now
	oldBoot.HeartbeatAt = now
	oldBoot.LeaseUntil = now.Add(5 * time.Second)
	oldBoot.LogicEndpoint = "old-overwrite.logic:9000"
	oldBoot.HolderControlEndpoint = "old-overwrite.control:9443"
	if _, err := registry.Register(context.Background(), oldBoot); !errors.Is(err, ErrHolderBootConflict) {
		t.Fatalf("old boot overwrote newer boot err=%v", err)
	}
	stored, found, err := registry.Get(context.Background(), newBoot.Domain, newBoot.HolderID)
	if err != nil || !found {
		t.Fatalf("new boot record found=%v err=%v", found, err)
	}
	if stored.BootID != newBoot.BootID || stored.LogicEndpoint != newBoot.LogicEndpoint ||
		stored.HolderControlEndpoint != newBoot.HolderControlEndpoint {
		t.Fatalf("new boot endpoints changed: %+v", stored)
	}
}

func TestHolderRegistryRejectsEveryNoncanonicalSocketEndpoint(t *testing.T) {
	invalid := []string{
		"not-a-socket", "Holder.internal:443", " holder.internal:443", "holder.internal:0",
		"holder.internal:01", "holder.internal:65536", "holder..internal:443", "999.0.0.1:443",
		"127.0.0.01:443", "[0:0:0:0:0:0:0:1]:443", "[fe80::1%eth0]:443", "h\u00f6lder:443",
	}
	for _, field := range []string{"logic", "control"} {
		for _, endpoint := range invalid {
			t.Run(field+"_"+endpoint, func(t *testing.T) {
				now := time.UnixMilli(1_800_000_000_000)
				store := newMemoryHolderStore()
				registry, err := NewHolderRegistry(store, HolderRegistryOptions{
					Now: nowFunc(&now), LeaseTTL: 10 * time.Second,
					MinLeaseTTL: time.Second, MaxLeaseTTL: time.Minute,
				})
				if err != nil {
					t.Fatal(err)
				}
				record := holderRegistryTestRecord(now)
				if field == "logic" {
					record.LogicEndpoint = endpoint
				} else {
					record.HolderControlEndpoint = endpoint
				}
				if _, err := registry.Register(context.Background(), record); !errors.Is(err, ErrHolderRegistryInvalid) {
					t.Fatalf("endpoint=%q err=%v", endpoint, err)
				}
				if len(store.records) != 0 {
					t.Fatal("invalid endpoint reached durable registration")
				}
			})
		}
	}
}

func TestHolderRegistryTerminalRecordCannotReviveByHeartbeat(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	store := newMemoryHolderStore()
	record := holderRegistryTestRecord(now)
	record.State = HolderStateDrained
	record.Draining = true
	store.records[store.key(record.Domain, record.HolderID)] = record
	registry, err := NewHolderRegistry(store, HolderRegistryOptions{
		Now: nowFunc(&now), LeaseTTL: 10 * time.Second,
		MinLeaseTTL: time.Second, MaxLeaseTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.Heartbeat(context.Background(), HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: HolderStateDrained, Draining: true, HeartbeatAt: now.Add(time.Second),
		LeaseUntil: now.Add(11 * time.Second), Sequence: 2, CapacityWeight: 1,
	})
	if !errors.Is(err, ErrHolderStateTransition) {
		t.Fatalf("terminal heartbeat err=%v", err)
	}
}

func TestHolderRegistryRejectsHeartbeatTimeRegression(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	store := newMemoryHolderStore()
	registry, err := NewHolderRegistry(store, HolderRegistryOptions{
		Now: nowFunc(&now), LeaseTTL: 10 * time.Second,
		MinLeaseTTL: time.Second, MaxLeaseTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	record := holderRegistryTestRecord(now)
	if _, err := registry.Register(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	ready := HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: HolderStateReadyZeroShard, ProcessReady: true,
		HeartbeatAt: now.Add(time.Second), LeaseUntil: now.Add(11 * time.Second),
		Sequence: 2, CapacityWeight: record.CapacityWeight,
	}
	if _, err := registry.Heartbeat(context.Background(), ready); err != nil {
		t.Fatal(err)
	}
	heartbeatRegression := ready
	heartbeatRegression.Sequence = 3
	heartbeatRegression.HeartbeatAt = now
	if _, err := registry.Heartbeat(context.Background(), heartbeatRegression); !errors.Is(err, ErrHolderHeartbeatStale) {
		t.Fatalf("heartbeat regression err=%v", err)
	}
	leaseRegression := ready
	leaseRegression.Sequence = 3
	leaseRegression.HeartbeatAt = now.Add(2 * time.Second)
	leaseRegression.LeaseUntil = now.Add(10 * time.Second)
	if _, err := registry.Heartbeat(context.Background(), leaseRegression); !errors.Is(err, ErrHolderHeartbeatStale) {
		t.Fatalf("lease regression err=%v", err)
	}
}

func TestHolderRegistryFreshNearExpiryRemainsReadableAndRenewable(t *testing.T) {
	base := time.UnixMilli(1_800_000_000_000)
	now := base
	store := newMemoryHolderStore()
	registry, err := NewHolderRegistry(store, HolderRegistryOptions{
		Now: nowFunc(&now), LeaseTTL: 10 * time.Second,
		MinLeaseTTL: time.Second, MaxLeaseTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	record := holderRegistryTestRecord(base)
	if _, err := registry.Register(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	now = base.Add(9500 * time.Millisecond)
	current, found, err := registry.Get(context.Background(), record.Domain, record.HolderID)
	if err != nil || !found {
		t.Fatalf("near-expiry Get record=%+v found=%t err=%v", current, found, err)
	}
	renewed, err := registry.Heartbeat(context.Background(), HolderHeartbeat{
		Domain: record.Domain, HolderID: record.HolderID, BootID: record.BootID,
		State: HolderStateWarming, HeartbeatAt: now,
		LeaseUntil: now.Add(10 * time.Second), Sequence: 2, CapacityWeight: record.CapacityWeight,
		ResourceSummary: map[string]string{"cpu": "2"},
	})
	if err != nil {
		t.Fatalf("near-expiry renewal err=%v", err)
	}
	if !renewed.LeaseUntil.Equal(now.Add(10 * time.Second)) {
		t.Fatalf("renewed lease=%v", renewed.LeaseUntil)
	}
}

func TestHolderRegistryRejectsNonZeroWarmingFlags(t *testing.T) {
	base := time.UnixMilli(1_800_000_000_000)
	tests := map[string]func(*HolderRecord){
		"process-ready": func(record *HolderRecord) { record.ProcessReady = true },
		"draining":      func(record *HolderRecord) { record.Draining = true },
		"serving":       func(record *HolderRecord) { record.ServingShardCount = 1 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			record := holderRegistryTestRecord(base)
			mutate(&record)
			registry, err := NewHolderRegistry(newMemoryHolderStore(), HolderRegistryOptions{
				Now: func() time.Time { return base }, LeaseTTL: 10 * time.Second,
				MinLeaseTTL: time.Second, MaxLeaseTTL: time.Minute,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := registry.Register(context.Background(), record); !errors.Is(err, ErrHolderRegistryInvalid) {
				t.Fatalf("Register err=%v", err)
			}
			if err := ValidateHolderRecord(record, base, registry.opts); !errors.Is(err, ErrHolderRegistryInvalid) {
				t.Fatalf("ValidateHolderRecord err=%v", err)
			}
		})
	}
}

func TestValidateHolderRecordRejectsNonPersistedAndIncompleteReadyStates(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	base := holderRegistryTestRecord(now)
	tests := []struct {
		name   string
		mutate func(*HolderRecord)
	}{
		{name: "registering", mutate: func(record *HolderRecord) { record.State = HolderStateRegistering }},
		{name: "ready-zero-not-ready", mutate: func(record *HolderRecord) { record.State = HolderStateReadyZeroShard }},
		{name: "serving-not-ready", mutate: func(record *HolderRecord) {
			record.State = HolderStateServing
			record.ServingShardCount = 1
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := base
			test.mutate(&record)
			if err := ValidateHolderRecord(record, now, DefaultHolderRegistryOptions()); !errors.Is(err, ErrHolderRegistryInvalid) {
				t.Fatalf("persisted state accepted record=%+v err=%v", record, err)
			}
		})
	}
}

func TestHolderResourceSummaryUsesCanonicalRS1PrintableASCIIContract(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	base := holderRegistryTestRecord(now)
	base.ResourceSummary = map[string]string{`quote"key`: `path\with internal space`}
	wire := CanonicalHolderResourceSummaryWire(base.ResourceSummary)
	if wire != `RS1:1:9:quote"key24:path\with internal space` {
		t.Fatalf("wire=%q", wire)
	}
	opts := DefaultHolderRegistryOptions()
	opts.MaxResourceSummaryBytes = len(wire)
	if err := ValidateHolderRecord(base, now, opts); err != nil {
		t.Fatalf("exact RS1 capacity rejected: %v", err)
	}
	opts.MaxResourceSummaryBytes--
	if err := ValidateHolderRecord(base, now, opts); !errors.Is(err, ErrHolderRegistryInvalid) {
		t.Fatalf("under-sized RS1 capacity err=%v", err)
	}

	tests := map[string]map[string]string{
		"leading-space":  {" cpu": "2"},
		"trailing-space": {"cpu": "2 "},
		"tab":            {"cpu": "two\tunits"},
		"unicode":        {"cpu": "二"},
		"invalid-byte":   {"cpu": string([]byte{0xff})},
	}
	for name, summary := range tests {
		t.Run(name, func(t *testing.T) {
			record := holderRegistryTestRecord(now)
			record.ResourceSummary = summary
			if err := ValidateHolderRecord(record, now, DefaultHolderRegistryOptions()); !errors.Is(err, ErrHolderRegistryInvalid) {
				t.Fatalf("invalid resource text accepted err=%v", err)
			}
		})
	}
	internalSpace := holderRegistryTestRecord(now)
	internalSpace.ResourceSummary = map[string]string{"cpu class": "two units"}
	if err := ValidateHolderRecord(internalSpace, now, DefaultHolderRegistryOptions()); err != nil {
		t.Fatalf("internal ASCII space rejected: %v", err)
	}
}

func nowFunc(value *time.Time) func() time.Time {
	return func() time.Time { return *value }
}

// The registry discovers this capability with a runtime type assertion rather
// than a compile-time one, so losing a method here does not fail the build --
// it fails registry construction at run time instead. The assertion below
// converts that back into a compile error.
var _ PlacementExclusionStore = (*memoryHolderStore)(nil)

func (s *memoryHolderStore) ReleasePlacementExclusionForOwner(_ context.Context, token NestedControlOperationToken, domain actorrouting.RouteDomain, holderID, bootID string, release PlacementExclusionRelease) (PlacementExclusionResult, error) {
	if !token.Issued() {
		return PlacementExclusionResult{}, ErrPlacementExclusionInvalid
	}
	if err := validatePlacementExclusionRelease(release, DefaultHolderMaxIDBytes); err != nil {
		return PlacementExclusionResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	record, found := s.records[s.key(domain, holderID)]
	if !found {
		// Absence of the Holder record does not prove the process is gone; it
		// may merely have outlived its TTL. Blocking is the only safe answer.
		return PlacementExclusionResult{Outcome: PlacementReleaseNotFound}, nil
	}
	if record.BootID != bootID {
		// The boot we excluded is provably gone, and load_placement will
		// synthesize an unexcluded baseline for the new one, so there is
		// nothing to write.
		return PlacementExclusionResult{Outcome: PlacementReleaseByBootRollover}, nil
	}
	current, ok := s.exclusions[s.key(domain, holderID)]
	if !ok || current.BootID != bootID {
		return PlacementExclusionResult{Outcome: PlacementReleaseAlreadyReleased}, nil
	}
	if err := validatePlacementExclusion(current, DefaultHolderMaxIDBytes); err != nil {
		return PlacementExclusionResult{State: current, Outcome: PlacementReleaseMalformed}, nil
	}
	if !current.Excluded {
		return PlacementExclusionResult{State: current, Outcome: PlacementReleaseAlreadyReleased}, nil
	}
	if current.OwnerPlanID != release.OwnerPlanID {
		return PlacementExclusionResult{State: current, Outcome: PlacementReleaseOwnerMismatch}, nil
	}
	if current.Revision != release.ExpectedRevision {
		return PlacementExclusionResult{State: current, Outcome: PlacementReleaseRevisionConflict}, nil
	}
	if current.Revision == ^uint64(0) {
		return PlacementExclusionResult{State: current}, ErrPlacementExclusionUnavailable
	}
	current.Excluded = false
	current.OwnerPlanID = ""
	current.Revision++
	s.exclusions[s.key(domain, holderID)] = current
	return PlacementExclusionResult{Committed: true, State: current, Outcome: PlacementReleaseReleased}, nil
}
