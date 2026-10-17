package authority

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/routecontrol"
	"farmgame/internal/storage/authoritystore"
)

func TestTargetPinnedCrashTakeoverIdentityContract(t *testing.T) {
	request := transitionRequest{
		kind:         transitionTransfer,
		transitionID: "target-pinned-crash",
		domain:       actorrouting.DomainPlayer,
		virtualShard: 7,
		expectedFence: authoritystore.ShardWriteFence{
			ExpectedHolderID: "player-a", ExpectedHolderBootID: "boot-a",
			ExpectedAssignmentRevision: 11, ExpectedWriterEpoch: 12, ExpectedLeaseEpoch: 13,
		},
		newHolderID: "player-b", newHolderBoot: "boot-b",
		targetPodUID: "pod-b", targetWorkloadUID: "workload", targetOrdinal: 1,
		deploymentIdentityPresent: true,
		endpoint:                  "player-b:9000",
		ttl:                       time.Minute,
		mode:                      TransferModeCrashTakeover,
		proof: TransferTakeoverProof{
			ExpectedHolderBootID: "boot-a", HolderUnavailable: true, Evidence: "source unavailable",
		},
	}
	if err := validateTransitionRequest(request); err != nil {
		t.Fatalf("target-pinned crash takeover rejected: %v", err)
	}
	fingerprint := transitionFingerprint(request)
	if !strings.HasPrefix(fingerprint, "route-transition-fingerprint-v2:") {
		t.Fatalf("target deployment identity was not bound into fingerprint: %q", fingerprint)
	}
	recovery := recoveryForRequest(request, fingerprint, authoritystore.ShardAuthorityTransitionResult{}, time.Now())
	restored, err := requestFromRecovery(recovery)
	if err != nil || !reflect.DeepEqual(restored, request) {
		t.Fatalf("target-pinned recovery restored=%+v want=%+v err=%v", restored, request, err)
	}

	partial := request
	partial.targetWorkloadUID = ""
	if err := validateTransitionRequest(partial); err == nil {
		t.Fatal("partial crash target deployment identity was accepted")
	}
	inventedSource := request
	inventedSource.sourcePodUID, inventedSource.sourceWorkloadUID = "pod-a", "workload"
	if err := validateTransitionRequest(inventedSource); err == nil {
		t.Fatal("crash takeover invented a source deployment identity")
	}
}

// testSnapshotPublisherFunc is an explicit test-only authority publisher. The
// production SnapshotPublisherFunc intentionally has no manager activation
// method, so it cannot serve as a public bootstrap/finalization bypass.
type testSnapshotPublisherFunc func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error

type committedNotificationTestError struct{}

func (committedNotificationTestError) Error() string           { return "durable commit notification failed" }
func (committedNotificationTestError) SnapshotCommitted() bool { return true }

func (f testSnapshotPublisherFunc) Publish(ctx context.Context, token routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	if f == nil {
		return ErrSnapshotPublish
	}
	return f(ctx, token, snapshot)
}

type fakePlacementExclusions struct {
	mu     sync.Mutex
	states map[string]routecontrol.PlacementExclusion
	calls  int
}

func placementTestKey(domain actorrouting.RouteDomain, holderID, bootID string) string {
	return string(domain) + "\x00" + holderID + "\x00" + bootID
}

func newFakePlacementExclusions(states ...routecontrol.PlacementExclusion) *fakePlacementExclusions {
	result := &fakePlacementExclusions{states: make(map[string]routecontrol.PlacementExclusion)}
	for _, state := range states {
		result.states[placementTestKey(state.Domain, state.HolderID, state.BootID)] = state
	}
	return result
}

func (f *fakePlacementExclusions) GetPlacementExclusion(_ context.Context, domain actorrouting.RouteDomain, holderID, bootID string) (routecontrol.PlacementExclusion, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	state, found := f.states[placementTestKey(domain, holderID, bootID)]
	return state, found, nil
}

func (f *fakePlacementExclusions) ExcludePlacement(_ context.Context, domain actorrouting.RouteDomain, holderID, bootID string, precondition routecontrol.PlacementExclusionPrecondition) (routecontrol.PlacementExclusionResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := placementTestKey(domain, holderID, bootID)
	state, found := f.states[key]
	if !found || state.Revision != precondition.ExpectedRevision || state.Excluded {
		return routecontrol.PlacementExclusionResult{}, routecontrol.ErrPlacementExclusionConflict
	}
	state.Excluded, state.Revision, state.OwnerPlanID = true, state.Revision+1, precondition.OwnerPlanID
	f.states[key] = state
	return routecontrol.PlacementExclusionResult{Committed: true, State: state}, nil
}

func (f *fakePlacementExclusions) ValidatePlacementTarget(_ context.Context, domain actorrouting.RouteDomain, holderID, bootID string) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	state, found := f.states[placementTestKey(domain, holderID, bootID)]
	if !found {
		return 0, routecontrol.ErrPlacementExclusionUnavailable
	}
	if state.Excluded {
		return state.Revision, routecontrol.ErrPlacementExcluded
	}
	return state.Revision, nil
}

func (f *fakePlacementExclusions) set(state routecontrol.PlacementExclusion) {
	f.mu.Lock()
	f.states[placementTestKey(state.Domain, state.HolderID, state.BootID)] = state
	f.mu.Unlock()
}

func (f *fakePlacementExclusions) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f testSnapshotPublisherFunc) PublishAuthoritySnapshot(ctx context.Context, token routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	return f.Publish(ctx, token, snapshot)
}

func (f testSnapshotPublisherFunc) PublishAuthorityWaveSnapshot(ctx context.Context, token routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	return f.Publish(ctx, token, snapshot)
}

func managerSnapshot() actorrouting.RouteSnapshot {
	return actorrouting.RouteSnapshot{
		Realm: "classic-farm", GlobalRevision: 1,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608040001,
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateAssigning, ShardRevision: 1,
			ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: time.Now().Add(time.Minute).UnixMilli(),
		}}},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 1, State: actorrouting.ShardStateAssigning, ShardRevision: 1,
			ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: time.Now().Add(time.Minute).UnixMilli(),
		}}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "player-a", BootID: "player-a-boot", Endpoint: "player-a:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-a", BootID: "farm-a-boot", Endpoint: "farm-a:9001", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
}

func managerFarmWorkerSnapshot() actorrouting.RouteSnapshot {
	snapshot := managerSnapshot()
	snapshot.Holders = append(snapshot.Holders, actorrouting.HolderRef{
		InstanceID: "farm-worker-a",
		BootID:     "farm-worker-a-boot",
		Endpoint:   "farm-worker-a:9200",
		State:      actorrouting.HolderStateActive,
		Weight:     1,
	})
	snapshot.FarmWorker = actorrouting.DomainSnapshot{
		ShardCount: 1,
		Routes: []actorrouting.ShardRoute{{
			HolderIndex:         2,
			State:               actorrouting.ShardStateAssigning,
			ShardRevision:       1,
			ExpectedWriterEpoch: 1,
			ExpectedLeaseEpoch:  1,
			LeaseUntilMs:        time.Now().Add(time.Minute).UnixMilli(),
		}},
	}
	return snapshot
}

type recordingAuthorityStore struct {
	inner         authoritystore.ShardWriteAuthorityStore
	mu            sync.Mutex
	claim         int
	trans         int
	unassign      int
	call          func()
	renewStarted  chan struct{}
	renewRelease  <-chan struct{}
	transferDelay time.Duration
	transferLive  atomic.Int32
	transferPeak  atomic.Int32
	digests       map[authorityKey]string
}

func (s *recordingAuthorityStore) ClaimShardAuthority(ctx context.Context, req authoritystore.ClaimShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	s.mu.Lock()
	s.claim++
	if s.call != nil {
		s.call()
	}
	s.mu.Unlock()
	result, err := s.inner.ClaimShardAuthority(ctx, req)
	if err == nil && (result.Status == authoritystore.ShardTransitionApplied || result.Status == authoritystore.ShardTransitionAlreadyApplied) {
		s.rememberDigest(req.Domain, req.VirtualShard, authoritystore.ShardAuthorityTransitionIntentDigest("claim", req.AuthorityLayoutVersion, string(req.Domain), req.VirtualShard, authoritystore.ShardWriteFence{}, req.NewHolderID, req.NewHolderBootID, req.RequestedTTLms))
	}
	return result, err
}

func (s *recordingAuthorityStore) RenewShardAuthority(ctx context.Context, req authoritystore.RenewShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	if s.renewStarted != nil {
		close(s.renewStarted)
		s.renewStarted = nil
	}
	if s.renewRelease != nil {
		<-s.renewRelease
	}
	return s.inner.RenewShardAuthority(ctx, req)
}

func (s *recordingAuthorityStore) TransferShardAuthority(ctx context.Context, req authoritystore.TransferShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	live := s.transferLive.Add(1)
	for {
		peak := s.transferPeak.Load()
		if live <= peak || s.transferPeak.CompareAndSwap(peak, live) {
			break
		}
	}
	if s.transferDelay > 0 {
		time.Sleep(s.transferDelay)
	}
	defer s.transferLive.Add(-1)
	s.mu.Lock()
	s.trans++
	if s.call != nil {
		s.call()
	}
	s.mu.Unlock()
	result, err := s.inner.TransferShardAuthority(ctx, req)
	if err == nil && (result.Status == authoritystore.ShardTransitionApplied || result.Status == authoritystore.ShardTransitionAlreadyApplied) {
		s.rememberDigest(req.Domain, req.VirtualShard, authoritystore.ShardAuthorityTransitionIntentDigest("transfer", req.AuthorityLayoutVersion, string(req.Domain), req.VirtualShard, req.ExpectedFence, req.NewHolderID, req.NewHolderBootID, req.RequestedTTLms))
	}
	return result, err
}

func (s *recordingAuthorityStore) TakeoverExpiredShardAuthority(ctx context.Context, req authoritystore.ExpiredTakeoverShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	s.mu.Lock()
	s.trans++
	if s.call != nil {
		s.call()
	}
	s.mu.Unlock()
	store, ok := s.inner.(authoritystore.ExpiredTakeoverShardAuthorityStore)
	if !ok {
		return authoritystore.ShardAuthorityTransitionResult{}, authoritystore.ErrInvalidRequest
	}
	result, err := store.TakeoverExpiredShardAuthority(ctx, req)
	if err == nil && (result.Status == authoritystore.ShardTransitionApplied || result.Status == authoritystore.ShardTransitionAlreadyApplied) {
		s.rememberDigest(req.Domain, req.VirtualShard, authoritystore.ShardAuthorityTransitionIntentDigest("transfer", req.AuthorityLayoutVersion, string(req.Domain), req.VirtualShard, req.ExpectedFence, req.NewHolderID, req.NewHolderBootID, req.RequestedTTLms))
	}
	return result, err
}

func (s *recordingAuthorityStore) UnassignShardAuthority(ctx context.Context, req authoritystore.UnassignShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	s.mu.Lock()
	s.unassign++
	s.mu.Unlock()
	result, err := s.inner.UnassignShardAuthority(ctx, req)
	if err == nil && (result.Status == authoritystore.ShardTransitionApplied || result.Status == authoritystore.ShardTransitionAlreadyApplied) {
		s.rememberDigest(req.Domain, req.VirtualShard, authoritystore.ShardAuthorityTransitionIntentDigest("unassign", req.AuthorityLayoutVersion, string(req.Domain), req.VirtualShard, req.ExpectedFence, "", "", 0))
	}
	return result, err
}

func (s *recordingAuthorityStore) rememberDigest(domain actorrouting.RouteDomain, shard uint32, digest string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.digests == nil {
		s.digests = make(map[authorityKey]string)
	}
	s.digests[authorityKey{domain: domain, shard: shard}] = digest
}

func (s *recordingAuthorityStore) InspectShardWriteAuthority(domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool) {
	inspector, ok := s.inner.(interface {
		InspectShardWriteAuthority(actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool)
	})
	if !ok {
		return authoritystore.ShardWriteAuthority{}, false
	}
	return inspector.InspectShardWriteAuthority(domain, shard)
}

func (s *recordingAuthorityStore) ReadShardWriteAuthority(_ context.Context, domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool, error) {
	authority, found := s.InspectShardWriteAuthority(domain, shard)
	return authority, found, nil
}

func (s *recordingAuthorityStore) ReadShardWriteAuthorityObservation(_ context.Context, domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthorityObservation, bool, error) {
	authority, found := s.InspectShardWriteAuthority(domain, shard)
	if !found {
		return authoritystore.ShardWriteAuthorityObservation{}, false, nil
	}
	s.mu.Lock()
	digest := s.digests[authorityKey{domain: domain, shard: shard}]
	s.mu.Unlock()
	return authoritystore.ShardWriteAuthorityObservation{Authority: authority, LastTransitionDigest: digest}, true, nil
}

type countingBatchAuthorityReader struct {
	store   *recordingAuthorityStore
	batches int
	singles int
}

func (r *countingBatchAuthorityReader) ReadShardWriteAuthority(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool, error) {
	r.singles++
	return r.store.ReadShardWriteAuthority(ctx, domain, shard)
}

func (r *countingBatchAuthorityReader) ReadShardWriteAuthorityBatch(ctx context.Context, domains []actorrouting.RouteDomain, shards []uint32) ([]authoritystore.ShardWriteAuthority, []bool, error) {
	r.batches++
	values := make([]authoritystore.ShardWriteAuthority, len(domains))
	found := make([]bool, len(domains))
	for index := range domains {
		var err error
		values[index], found[index], err = r.store.ReadShardWriteAuthority(ctx, domains[index], shards[index])
		if err != nil {
			return nil, nil, err
		}
	}
	return values, found, nil
}

func newManagerForTest(t *testing.T, publisher SnapshotPublisher) (*RouteAssignmentManager, *recordingAuthorityStore) {
	return newManagerForTestWithOptions(t, publisher, ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions()})
}

func newManagerForTestWithOptions(t *testing.T, publisher SnapshotPublisher, extra ManagerOptions) (*RouteAssignmentManager, *recordingAuthorityStore) {
	t.Helper()
	now := extra.Now
	if now == nil {
		now = func() time.Time { return time.UnixMilli(1_900_000_000_000) }
	}
	calc, err := actorrouting.NewCalculator("classic-farm", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040001}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	inner, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{Resolver: calc, AuthorityLayoutVersion: "auth-v1", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	store := &recordingAuthorityStore{inner: inner}
	opts := ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: publisher, InitialSnapshot: managerSnapshot(),
		AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-a", RouteManagerBootID: "route-manager-boot",
		Now: now, AuthorityReader: store,
		TargetConvergenceTimeout:           extra.TargetConvergenceTimeout,
		TargetConvergenceBackoff:           extra.TargetConvergenceBackoff,
		After:                              extra.After,
		DisableDeadlineSnapshotPublication: extra.DisableDeadlineSnapshotPublication,
	}
	if extra.SnapshotCoalesceWindow != 0 {
		opts.SnapshotCoalesceWindow = extra.SnapshotCoalesceWindow
	}
	if extra.SnapshotCoalesceBatch != 0 {
		opts.SnapshotCoalesceBatch = extra.SnapshotCoalesceBatch
	}
	if extra.SnapshotCoalesceMaxWait != 0 {
		opts.SnapshotCoalesceMaxWait = extra.SnapshotCoalesceMaxWait
	}
	if extra.MemoLimit != 0 {
		opts.MemoLimit = extra.MemoLimit
	}
	if extra.MemoRetention != 0 {
		opts.MemoRetention = extra.MemoRetention
	}
	if extra.AuthorityReader != nil {
		opts.AuthorityReader = extra.AuthorityReader
	}
	if extra.TombstoneStore != nil {
		opts.TombstoneStore = extra.TombstoneStore
	}
	if extra.PlacementExclusions != nil {
		opts.PlacementExclusions = extra.PlacementExclusions
	}
	if opts.TombstoneStore == nil {
		// Durable replay evidence has an independent bound and intentionally
		// outlives the hot memo bound.
		opts.TombstoneStore = NewMemoryTransitionTombstoneStore(1024)
	}
	manager, err := NewRouteAssignmentManager(opts)
	if err != nil {
		t.Fatal(err)
	}
	return manager, store
}

func newRawManagerForTest(t *testing.T, store authoritystore.ShardWriteAuthorityStore, publisher SnapshotPublisher, extra ManagerOptions) *RouteAssignmentManager {
	t.Helper()
	now := extra.Now
	if now == nil {
		now = func() time.Time { return time.UnixMilli(1_900_000_000_000) }
	}
	reader := extra.AuthorityReader
	if reader == nil {
		if _, ok := store.(shardAuthorityInspector); !ok {
			reader = AuthorityReaderFunc(func(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error) {
				return authoritystore.ShardWriteAuthority{}, false, nil
			})
		}
	}
	placements := extra.PlacementExclusions
	if placements == nil {
		placements = newAllowAllPlacementExclusions()
	}
	opts := ManagerOptions{PlacementExclusions: placements,
		Store: store, Publisher: publisher, InitialSnapshot: managerSnapshot(),
		AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-a", RouteManagerBootID: "route-manager-boot", Now: now,
		MemoLimit: extra.MemoLimit, MemoRetention: extra.MemoRetention, AuthorityReader: reader,
		TombstoneStore:                     extra.TombstoneStore,
		DisableDeadlineSnapshotPublication: extra.DisableDeadlineSnapshotPublication,
	}
	if extra.SnapshotCoalesceWindow != 0 {
		opts.SnapshotCoalesceWindow = extra.SnapshotCoalesceWindow
	}
	if extra.SnapshotCoalesceBatch != 0 {
		opts.SnapshotCoalesceBatch = extra.SnapshotCoalesceBatch
	}
	if extra.SnapshotCoalesceMaxWait != 0 {
		opts.SnapshotCoalesceMaxWait = extra.SnapshotCoalesceMaxWait
	}
	if opts.TombstoneStore == nil {
		limit := opts.MemoLimit
		if limit <= 0 {
			limit = 1024
		}
		opts.TombstoneStore = NewMemoryTransitionTombstoneStore(limit)
	}
	manager, err := NewRouteAssignmentManager(opts)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func newMemoryAuthorityStoreForTest(t *testing.T) authoritystore.ShardWriteAuthorityStore {
	t.Helper()
	calc, err := actorrouting.NewCalculator("classic-farm", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040001}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver: calc, AuthorityLayoutVersion: "auth-v1", Now: func() time.Time { return time.UnixMilli(1_900_000_000_000) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestTransitionFingerprintPreservesLegacyV1AndBindsExactV2Identity(t *testing.T) {
	legacy := transitionRequest{kind: transitionTransfer, transitionID: "identity-fingerprint", domain: actorrouting.DomainPlayer,
		virtualShard: 1, expectedFence: authoritystore.ShardWriteFence{ExpectedHolderID: "holder-a", ExpectedHolderBootID: "boot-a", ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1},
		newHolderID: "holder-b", newHolderBoot: "boot-b", endpoint: "holder-b:9000", ttl: time.Second,
		mode: TransferModeCrashTakeover, proof: TransferTakeoverProof{ExpectedHolderBootID: "boot-a", LeaseExpired: true, Evidence: "expired"}}
	if got := transitionFingerprint(legacy); !strings.HasPrefix(got, "route-transition-fingerprint-v1:") {
		t.Fatalf("legacy fingerprint=%s", got)
	}
	exact := legacy
	exact.mode, exact.proof, exact.internalDrain = TransferModeDrainHandoff, TransferTakeoverProof{}, true
	exact.sourcePodUID, exact.sourceWorkloadUID, exact.sourceOrdinal = "pod-a", "workload", 0
	exact.targetPodUID, exact.targetWorkloadUID, exact.targetOrdinal = "pod-b", "workload", 1
	exact.deploymentIdentityPresent = true
	fingerprint := transitionFingerprint(exact)
	if !strings.HasPrefix(fingerprint, "route-transition-fingerprint-v2:") || fingerprint == transitionFingerprintV1(exact) {
		t.Fatalf("exact fingerprint=%s legacy=%s", fingerprint, transitionFingerprintV1(exact))
	}
	for _, mutate := range []func(*transitionRequest){
		func(value *transitionRequest) { value.sourcePodUID = "other-source" },
		func(value *transitionRequest) { value.sourceWorkloadUID = "other-workload" },
		func(value *transitionRequest) { value.sourceOrdinal++ },
		func(value *transitionRequest) { value.targetPodUID = "other-target" },
		func(value *transitionRequest) { value.targetWorkloadUID = "other-workload" },
		func(value *transitionRequest) { value.targetOrdinal++ },
	} {
		candidate := exact
		mutate(&candidate)
		if transitionFingerprint(candidate) == fingerprint {
			t.Fatal("deployment identity forgery retained fingerprint")
		}
	}
}

func TestRouteAssignmentManagerAuthorityBeforeSnapshotAndReplay(t *testing.T) {
	var authorityCalls int
	var publishes []actorrouting.RouteSnapshot
	var manager *RouteAssignmentManager
	publisher := testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		if authorityCalls == 0 {
			t.Fatal("snapshot published before authority commit")
		}
		publishes = append(publishes, actorrouting.CloneRouteSnapshot(snapshot))
		return nil
	})
	manager, store := newManagerForTest(t, publisher)
	store.call = func() { authorityCalls++ }
	req := ClaimRequest{TransitionID: "claim-player-b", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-b", NewHolderBootID: "player-b-boot", Endpoint: "player-b:9000", RequestedTTL: 10 * time.Second}
	first, err := manager.Claim(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != authoritystore.ShardTransitionApplied || len(publishes) != 1 {
		t.Fatalf("first result=%+v publishes=%d", first, len(publishes))
	}
	if route := publishes[0].Player.Routes[0]; route.ShardRevision != first.Authority.AssignmentRevision || route.ExpectedWriterEpoch != first.Authority.WriterEpoch || route.ExpectedLeaseEpoch != first.Authority.LeaseEpoch {
		t.Fatalf("published tuple does not match authority: route=%+v authority=%+v", route, first.Authority)
	}
	if got := publishes[0].Holders[publishes[0].Player.Routes[0].HolderIndex]; got.InstanceID != first.Authority.HolderID || got.BootID != first.Authority.HolderBootID {
		t.Fatalf("published route=%+v holders=%+v holder=%+v authority=%+v", publishes[0].Player.Routes[0], publishes[0].Holders, got, first.Authority)
	}

	replayed, err := manager.Claim(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, replayed) || len(publishes) != 1 || store.claim != 1 {
		t.Fatalf("replay changed result/published twice: first=%+v replay=%+v publishes=%d calls=%d", first, replayed, len(publishes), store.claim)
	}
	conflict := req
	conflict.NewHolderID = "player-c"
	if _, err := manager.Claim(context.Background(), conflict); !errors.Is(err, ErrTransitionReconcile) {
		t.Fatalf("conflicting transition err=%v", err)
	}
}

func TestRouteAssignmentManagerCrashTakeoverAfterExpiryUsesAtomicExpiredCAS(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	var published []actorrouting.RouteSnapshot
	manager, _ := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
		return nil
	}), ManagerOptions{Now: func() time.Time { return now }})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "expired-manager-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "player-a-boot", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil || claimed.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("claim=%+v err=%v", claimed, err)
	}
	now = time.UnixMilli(claimed.Authority.LeaseUntilMs)
	taken, err := manager.Transfer(context.Background(), TransferRequest{
		TransitionID: "expired-manager-takeover", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "player-b-boot",
		Endpoint: "player-b:9000", RequestedTTL: time.Second, Mode: TransferModeCrashTakeover,
		TakeoverProof: TransferTakeoverProof{ExpectedHolderBootID: "player-a-boot", LeaseExpired: true, Evidence: "lease ended"},
	})
	if err != nil || taken.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("takeover=%+v err=%v", taken, err)
	}
	if taken.Authority.HolderID != "player-b" || taken.Authority.WriterEpoch != claimed.Authority.WriterEpoch+1 ||
		taken.Authority.LeaseEpoch != claimed.Authority.LeaseEpoch+1 ||
		taken.Authority.AssignmentRevision != claimed.Authority.AssignmentRevision+1 {
		t.Fatalf("taken authority=%+v", taken.Authority)
	}
	if len(published) != 2 {
		t.Fatalf("published snapshots=%d", len(published))
	}
	last := published[len(published)-1]
	route := last.Player.Routes[0]
	holder := last.Holders[route.HolderIndex]
	if holder.InstanceID != "player-b" || holder.BootID != "player-b-boot" ||
		route.ExpectedWriterEpoch != taken.Authority.WriterEpoch || route.ShardRevision != taken.Authority.AssignmentRevision {
		t.Fatalf("route=%+v holder=%+v authority=%+v", route, holder, taken.Authority)
	}
}

func TestRouteAssignmentManagerClaimsFarmWorkerWithoutChangingFarm(t *testing.T) {
	initial := managerFarmWorkerSnapshot()
	var published actorrouting.RouteSnapshot
	publisher := testSnapshotPublisherFunc(func(
		_ context.Context,
		_ routecontrol.NestedControlOperationToken,
		snapshot actorrouting.RouteSnapshot,
	) error {
		published = actorrouting.CloneRouteSnapshot(snapshot)
		return nil
	})
	store := &recordingAuthorityStore{inner: newMemoryAuthorityStoreForTest(t)}
	manager, err := NewRouteAssignmentManager(ManagerOptions{
		PlacementExclusions:    newAllowAllPlacementExclusions(),
		Store:                  store,
		Publisher:              publisher,
		InitialSnapshot:        initial,
		AuthorityLayoutVersion: "auth-v1",
		RouteManagerID:         "route-manager-worker",
		RouteManagerBootID:     "route-manager-worker-boot",
		Now:                    func() time.Time { return time.UnixMilli(1_900_000_000_000) },
		AuthorityReader:        store,
		TombstoneStore:         NewMemoryTransitionTombstoneStore(8),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID:    "claim-farm-worker-b",
		Domain:          actorrouting.DomainFarmWorker,
		VirtualShard:    0,
		NewHolderID:     "farm-worker-b",
		NewHolderBootID: "farm-worker-b-boot",
		Endpoint:        "farm-worker-b:9200",
		RequestedTTL:    10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != authoritystore.ShardTransitionApplied ||
		result.Authority.Domain != actorrouting.DomainFarmWorker {
		t.Fatalf("worker claim=%+v", result)
	}
	if !reflect.DeepEqual(published.Farm, initial.Farm) {
		t.Fatalf(
			"FARM routes changed by FARM_WORKER claim: before=%+v after=%+v",
			initial.Farm,
			published.Farm,
		)
	}
	workerRoute := published.FarmWorker.Routes[0]
	workerHolder := published.Holders[workerRoute.HolderIndex]
	if workerRoute.State != actorrouting.ShardStateActive ||
		workerHolder.InstanceID != "farm-worker-b" ||
		workerHolder.BootID != "farm-worker-b-boot" {
		t.Fatalf("published worker route=%+v holder=%+v", workerRoute, workerHolder)
	}
	_, err = manager.Drain(context.Background(), TransferRequest{
		TransitionID:    "reject-worker-actor-drain",
		Domain:          actorrouting.DomainFarmWorker,
		VirtualShard:    0,
		ExpectedFence:   managerFence(result.Authority),
		NewHolderID:     "farm-worker-a",
		NewHolderBootID: "farm-worker-a-next-boot",
		Endpoint:        "farm-worker-a:9200",
		RequestedTTL:    time.Second,
	}, &recordingDrainController{})
	if !errors.Is(err, ErrInvalidManager) {
		t.Fatalf("FARM_WORKER actor drain err=%v", err)
	}
}

func TestRouteAssignmentManagerFarmWorkerHandoffStopsClaimsBeforeFenceTransfer(t *testing.T) {
	initial := managerFarmWorkerSnapshot()
	var published []actorrouting.RouteSnapshot
	publisher := testSnapshotPublisherFunc(func(
		_ context.Context,
		_ routecontrol.NestedControlOperationToken,
		snapshot actorrouting.RouteSnapshot,
	) error {
		published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
		return nil
	})
	store := &recordingAuthorityStore{inner: newMemoryAuthorityStoreForTest(t)}
	manager, err := NewRouteAssignmentManager(ManagerOptions{
		PlacementExclusions:    newAllowAllPlacementExclusions(),
		Store:                  store,
		Publisher:              publisher,
		InitialSnapshot:        initial,
		AuthorityLayoutVersion: "auth-v1",
		RouteManagerID:         "route-manager-worker",
		RouteManagerBootID:     "route-manager-worker-boot",
		Now:                    func() time.Time { return time.UnixMilli(1_900_000_000_000) },
		AuthorityReader:        store,
		TombstoneStore:         NewMemoryTransitionTombstoneStore(8),
	})
	if err != nil {
		t.Fatal(err)
	}
	source, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID:    "claim-worker-source",
		Domain:          actorrouting.DomainFarmWorker,
		VirtualShard:    0,
		NewHolderID:     "farm-worker-a",
		NewHolderBootID: "farm-worker-a-boot",
		Endpoint:        "farm-worker-a:9200",
		RequestedTTL:    10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	// The final controller may have observed the initial ASSIGNING placeholder
	// before external bootstrap installed ACTIVE. Worker handoff must use its
	// own DRAINING -> ACTIVE validator even if that stale bootstrap bit remains.
	manager.mu.Lock()
	manager.bootstrap[authorityKey{
		domain: actorrouting.DomainFarmWorker,
		shard:  0,
	}] = true
	manager.mu.Unlock()
	published = nil
	request := TransferRequest{
		TransitionID:    "handoff-worker-a-to-b",
		Domain:          actorrouting.DomainFarmWorker,
		VirtualShard:    0,
		ExpectedFence:   managerFence(source.Authority),
		NewHolderID:     "farm-worker-b",
		NewHolderBootID: "farm-worker-b-boot",
		Endpoint:        "farm-worker-b:9200",
		RequestedTTL:    10 * time.Second,
	}
	sourceValidated := false
	result, err := manager.HandoffWorkerValidated(
		context.Background(),
		request,
		func(_ context.Context, observation WorkerHandoffSourceObservation) error {
			sourceValidated = true
			if store.trans != 0 {
				t.Fatal("worker Authority transferred before source stop-new-claim acknowledgement")
			}
			snapshot := manager.CurrentSnapshot()
			route := snapshot.FarmWorker.Routes[0]
			holder := snapshot.Holders[route.HolderIndex]
			if route.State != actorrouting.ShardStateDraining ||
				holder.InstanceID != "farm-worker-a" ||
				observation.SnapshotRevision != snapshot.GlobalRevision ||
				observation.ExpectedServingShards != 0 {
				t.Fatalf("draining snapshot=%+v observation=%+v", snapshot, observation)
			}
			return nil
		},
		func(context.Context, TransitionRecovery, uint64) error { return nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if !sourceValidated || store.trans != 1 ||
		result.Authority.HolderID != "farm-worker-b" ||
		len(published) != 2 {
		t.Fatalf(
			"validated=%t transfers=%d result=%+v publishes=%d",
			sourceValidated,
			store.trans,
			result,
			len(published),
		)
	}
	if published[0].FarmWorker.Routes[0].State != actorrouting.ShardStateDraining {
		t.Fatalf("first handoff snapshot=%+v", published[0].FarmWorker.Routes[0])
	}
	activeRoute := published[1].FarmWorker.Routes[0]
	activeHolder := published[1].Holders[activeRoute.HolderIndex]
	if activeRoute.State != actorrouting.ShardStateActive ||
		activeHolder.InstanceID != "farm-worker-b" ||
		activeHolder.BootID != "farm-worker-b-boot" {
		t.Fatalf("active route=%+v holder=%+v", activeRoute, activeHolder)
	}
	direct := request
	direct.TransitionID = "forged-worker-handoff"
	direct.Mode = TransferModeWorkerHandoff
	if _, err := manager.TransferValidated(
		context.Background(),
		direct,
		func(context.Context, TransitionRecovery, uint64) error { return nil },
	); !errors.Is(err, ErrTransferTakeoverNeeded) {
		t.Fatalf("public worker handoff mode err=%v", err)
	}
}

func TestRouteAssignmentManagerActivationTraversesDynamicProviderAndStaticResolver(t *testing.T) {
	initial := managerSnapshot()
	now := func() time.Time { return time.UnixMilli(1_900_000_000_000) }
	calc, err := actorrouting.NewCalculator(initial.Realm, actorrouting.HashProfile{Version: initial.HashVersion, Seed: initial.HashSeed}, initial.Player.ShardCount, initial.Farm.ShardCount)
	if err != nil {
		t.Fatal(err)
	}
	inner, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{Resolver: calc, AuthorityLayoutVersion: "auth-v1", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	store := &recordingAuthorityStore{inner: inner}
	provider, publisher, err := actorrouting.NewDynamicAssignmentProvider(initial, 8)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := actorrouting.NewStaticResolver(initial)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := actorrouting.NewSnapshotApplier(provider, resolver)
	if err != nil {
		t.Fatal(err)
	}
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	watchDone := make(chan error, 1)
	go func() { watchDone <- applier.Run(watchCtx) }()
	defer func() {
		cancelWatch()
		if runErr := <-watchDone; !errors.Is(runErr, context.Canceled) {
			t.Errorf("snapshot applier exit=%v", runErr)
		}
	}()
	waitForResolverRoute(t, resolver, applier, 1, actorrouting.ShardStateAssigning)

	manager, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: publisher, InitialSnapshot: initial,
		AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-integration", RouteManagerBootID: "integration-boot",
		Now: now, TombstoneStore: NewMemoryTransitionTombstoneStore(8),
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "pipeline-bootstrap", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-bootstrap", NewHolderBootID: "bootstrap-boot", Endpoint: "player-bootstrap:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForResolverRoute(t, resolver, applier, 2, actorrouting.ShardStateActive)
	bootstrapRoute := resolver.Snapshot().Player.Routes[0]
	if bootstrapRoute.ShardRevision != claimed.Authority.AssignmentRevision || bootstrapRoute.ExpectedWriterEpoch != claimed.Authority.WriterEpoch || bootstrapRoute.ExpectedLeaseEpoch != claimed.Authority.LeaseEpoch {
		t.Fatalf("bootstrap activation tuple=%+v authority=%+v", bootstrapRoute, claimed.Authority)
	}

	drained, err := manager.Drain(context.Background(), TransferRequest{
		TransitionID: "pipeline-drain", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority),
		NewHolderID: "player-b", NewHolderBootID: "drained-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
	}, &recordingDrainController{})
	if err != nil {
		t.Fatal(err)
	}
	waitForResolverRoute(t, resolver, applier, 4, actorrouting.ShardStateActive)
	finalSnapshot := resolver.Snapshot()
	finalRoute := finalSnapshot.Player.Routes[0]
	finalHolder := finalSnapshot.Holders[finalRoute.HolderIndex]
	if finalRoute.ShardRevision != drained.Authority.AssignmentRevision || finalRoute.ExpectedWriterEpoch != drained.Authority.WriterEpoch || finalRoute.ExpectedLeaseEpoch != drained.Authority.LeaseEpoch || finalHolder.InstanceID != drained.Authority.HolderID || finalHolder.BootID != drained.Authority.HolderBootID {
		t.Fatalf("Drain final activation route=%+v holder=%+v authority=%+v", finalRoute, finalHolder, drained.Authority)
	}
}

func TestInitialPlaceholderActivationRejectsOrdinarySnapshotPublisher(t *testing.T) {
	var ordinaryPublishes int
	manager, store := newManagerForTest(t, SnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		ordinaryPublishes++
		return nil
	}))
	result, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "ordinary-bootstrap-denied", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-b", NewHolderBootID: "player-b-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
	})
	if !errors.Is(err, ErrSnapshotPublish) || result.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("ordinary publisher bootstrap result=%+v err=%v", result, err)
	}
	if ordinaryPublishes != 0 || store.claim != 1 {
		t.Fatalf("ordinary publisher reached activation path: publishes=%d authority_calls=%d", ordinaryPublishes, store.claim)
	}
	if got := manager.CurrentSnapshot().Player.Routes[0]; got.State != actorrouting.ShardStateAssigning || got.ShardRevision != 1 {
		t.Fatalf("ordinary publisher changed initial placeholder: %+v", got)
	}
}

type recordingDynamicPublisher struct {
	inner *actorrouting.DynamicAssignmentPublisher
	mu    sync.Mutex
	seen  []actorrouting.RouteSnapshot
}

func (p *recordingDynamicPublisher) Publish(ctx context.Context, token routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	if err := p.inner.Publish(ctx, token, snapshot); err != nil {
		return err
	}
	p.record(snapshot)
	return nil
}

func (p *recordingDynamicPublisher) PublishAuthoritySnapshot(ctx context.Context, token routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	if err := p.inner.PublishAuthoritySnapshot(ctx, token, snapshot); err != nil {
		return err
	}
	p.record(snapshot)
	return nil
}

func (p *recordingDynamicPublisher) PublishAuthorityWaveSnapshot(ctx context.Context, token routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	if err := p.inner.PublishAuthorityWaveSnapshot(ctx, token, snapshot); err != nil {
		return err
	}
	p.record(snapshot)
	return nil
}

func (p *recordingDynamicPublisher) record(snapshot actorrouting.RouteSnapshot) {
	p.mu.Lock()
	p.seen = append(p.seen, actorrouting.CloneRouteSnapshot(snapshot))
	p.mu.Unlock()
}

func (p *recordingDynamicPublisher) snapshots() []actorrouting.RouteSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := make([]actorrouting.RouteSnapshot, len(p.seen))
	for i := range p.seen {
		result[i] = actorrouting.CloneRouteSnapshot(p.seen[i])
	}
	return result
}

type failNextDeleteRecoveryStore struct {
	*MemoryTransitionTombstoneStore
	mu         sync.Mutex
	failDelete bool
	failCommit bool
}

func (s *failNextDeleteRecoveryStore) armCommitFailure() {
	s.mu.Lock()
	s.failCommit = true
	s.mu.Unlock()
}

func (s *failNextDeleteRecoveryStore) CommitTransitionRecovery(_ context.Context, recovery TransitionRecovery) error {
	s.mu.Lock()
	if s.failCommit && recovery.Published && recovery.LastState == actorrouting.ShardStateActive {
		s.failCommit = false
		s.mu.Unlock()
		return errors.New("simulated crash before complete recovery commit")
	}
	s.mu.Unlock()
	return s.MemoryTransitionTombstoneStore.CommitTransitionRecovery(context.Background(), recovery)
}

func (s *failNextDeleteRecoveryStore) armDeleteFailure() {
	s.mu.Lock()
	s.failDelete = true
	s.mu.Unlock()
}

func (s *failNextDeleteRecoveryStore) DeleteTransitionRecovery(_ context.Context, id string) error {
	s.mu.Lock()
	if s.failDelete {
		s.failDelete = false
		s.mu.Unlock()
		return errors.New("simulated crash before recovery cleanup")
	}
	s.mu.Unlock()
	return s.MemoryTransitionTombstoneStore.DeleteTransitionRecovery(context.Background(), id)
}

func TestDrainFinalPublicationReplayIsIdempotentAfterRecoveryCleanupCrash(t *testing.T) {
	initial := managerSnapshot()
	clock := time.UnixMilli(1_900_000_000_000)
	now := func() time.Time { return clock }
	calc, err := actorrouting.NewCalculator(initial.Realm, actorrouting.HashProfile{Version: initial.HashVersion, Seed: initial.HashSeed}, initial.Player.ShardCount, initial.Farm.ShardCount)
	if err != nil {
		t.Fatal(err)
	}
	inner, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{Resolver: calc, AuthorityLayoutVersion: "auth-v1", Now: now})
	if err != nil {
		t.Fatal(err)
	}
	store := &recordingAuthorityStore{inner: inner}
	read, write, err := actorrouting.NewDynamicAssignmentProvider(initial, 8)
	if err != nil {
		t.Fatal(err)
	}
	publisher := &recordingDynamicPublisher{inner: write}
	tombstones := &failNextDeleteRecoveryStore{MemoryTransitionTombstoneStore: NewMemoryTransitionTombstoneStore(8)}
	manager, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: publisher, InitialSnapshot: initial,
		AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-first", RouteManagerBootID: "first-boot",
		Now: now, TombstoneStore: tombstones,
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := actorrouting.NewStaticResolver(initial)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := actorrouting.NewSnapshotApplier(read, resolver)
	if err != nil {
		t.Fatal(err)
	}
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	watchDone := make(chan error, 1)
	go func() { watchDone <- applier.Run(watchCtx) }()
	defer func() {
		cancelWatch()
		if runErr := <-watchDone; !errors.Is(runErr, context.Canceled) {
			t.Errorf("snapshot applier exit=%v", runErr)
		}
	}()
	waitForResolverRoute(t, resolver, applier, initial.GlobalRevision, actorrouting.ShardStateAssigning)

	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "cleanup-crash-bootstrap", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "player-a-new-boot", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForResolverRoute(t, resolver, applier, 2, actorrouting.ShardStateActive)
	drainRequest := TransferRequest{
		TransitionID: "cleanup-crash-drain", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority),
		NewHolderID: "player-b", NewHolderBootID: "player-b-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
	}
	tombstones.armCommitFailure()
	firstResult, err := manager.Drain(context.Background(), drainRequest, &recordingDrainController{})
	if !errors.Is(err, ErrSnapshotPublish) || firstResult.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("final cleanup crash result=%+v err=%v", firstResult, err)
	}
	waitForResolverRoute(t, resolver, applier, 4, actorrouting.ShardStateActive)
	seen := publisher.snapshots()
	if len(seen) != 3 || seen[1].GlobalRevision != 3 || seen[1].Player.Routes[0].State != actorrouting.ShardStateAssigning || seen[2].GlobalRevision != 4 || seen[2].Player.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("unexpected publication sequence: %+v", seen)
	}
	recoveries, err := tombstones.LoadTransitionRecoveries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !containsRecovery(recoveries, drainRequest.TransitionID, actorrouting.ShardStateAssigning, true) {
		t.Fatalf("stale ASSIGNING recovery not retained: %+v", recoveries)
	}
	placement := newFakePlacementExclusions(routecontrol.PlacementExclusion{Domain: actorrouting.DomainPlayer,
		HolderID: drainRequest.NewHolderID, BootID: drainRequest.NewHolderBootID, Excluded: true, Revision: 2, OwnerPlanID: "scale-plan-a"})
	manager.placementExclusions = placement
	clock = clock.Add(2 * time.Second) // exact installed evidence outlives the authority lease
	// Same-process replay must treat the exact already-published ACTIVE route as
	// terminal evidence even though recovery-marker persistence failed.
	sameProcessController := &recordingDrainController{}
	sameProcess, err := manager.Drain(context.Background(), drainRequest, sameProcessController)
	if err != nil || !reflect.DeepEqual(firstResult, sameProcess) || len(sameProcessController.steps) != 0 || store.trans != 1 {
		t.Fatalf("same-process terminal replay=%+v err=%v steps=%v transfers=%d", sameProcess, err, sameProcessController.steps, store.trans)
	}

	restarted, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: placement,
		Store: store, Publisher: publisher, InitialSnapshot: seen[2],
		AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-restart", RouteManagerBootID: "restart-boot",
		Now: now, TombstoneStore: tombstones,
	})
	if err != nil {
		t.Fatal(err)
	}
	replayController := &recordingDrainController{}
	replayed, err := restarted.Drain(context.Background(), drainRequest, replayController)
	if err != nil || !reflect.DeepEqual(firstResult, replayed) {
		t.Fatalf("idempotent final replay result=%+v want=%+v err=%v", replayed, firstResult, err)
	}
	if len(replayController.steps) != 0 || store.trans != 1 {
		t.Fatalf("replay repeated Drain side effects: steps=%v transfers=%d", replayController.steps, store.trans)
	}
	if got := read.CurrentSnapshot(); got.GlobalRevision != 4 || got.Player.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("provider left target after replay: %+v", got.Player.Routes[0])
	}
	if got := resolver.Snapshot(); got.GlobalRevision != 4 || got.Player.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("resolver left target after replay: %+v", got.Player.Routes[0])
	}
	if got := restarted.CurrentSnapshot(); got.GlobalRevision != 4 || got.Player.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("manager did not advance stale recovery to target: %+v", got.Player.Routes[0])
	}
	recoveries, err = tombstones.LoadTransitionRecoveries(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !containsRecovery(recoveries, drainRequest.TransitionID, actorrouting.ShardStateActive, true) {
		t.Fatalf("successful replay did not retain complete recovery: %+v", recoveries)
	}
}

func containsRecovery(recoveries []TransitionRecovery, id string, state actorrouting.ShardRouteState, published bool) bool {
	for _, recovery := range recoveries {
		if recovery.TransitionID == id && recovery.LastState == state && recovery.Published == published {
			return true
		}
	}
	return false
}

func TestManagerObserveTransitionRetainedAcrossRestartAndAtomicEviction(t *testing.T) {
	clock := time.UnixMilli(1_900_000_000_000)
	now := func() time.Time { return clock }
	evidence := NewMemoryTransitionTombstoneStore(32)
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		TombstoneStore: evidence, MemoLimit: 4, Now: now,
	})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "observe-source", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "observe-source-boot", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	drain := TransferRequest{
		TransitionID: "observe-drain", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority),
		NewHolderID: "player-b", NewHolderBootID: "observe-target-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
	}
	result, err := manager.Drain(context.Background(), drain, &recordingDrainController{})
	if err != nil {
		t.Fatal(err)
	}
	request := TransitionObservationRequest{Transfer: drain, PlannedDrain: true}
	observe := func(target *RouteAssignmentManager) TransitionObservation {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		observed, observeErr := target.ObserveTransition(ctx, request)
		if observeErr != nil {
			t.Fatalf("observe: %v", observeErr)
		}
		return observed
	}
	if observed := observe(manager); observed.Class != TransitionEvidenceCompleteRecovery || observed.Result != result ||
		observed.Fingerprint == "" || observed.Recovery.TransitionID != drain.TransitionID || observed.Recovery.Fingerprint != observed.Fingerprint ||
		observed.Recovery.Result != result || observed.Tombstone != (TransitionTombstone{}) {
		t.Fatalf("hot observation=%+v", observed)
	}
	restarted, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, AuthorityReader: store, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			return nil
		}),
		InitialSnapshot: manager.CurrentSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "observe-restart",
		RouteManagerBootID: "observe-restart-boot", TombstoneStore: evidence, MemoLimit: 4, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if observed := observe(restarted); observed.Class != TransitionEvidenceCompleteRecovery || observed.Result != result ||
		observed.Recovery.TransitionID != drain.TransitionID || observed.Recovery.Fingerprint != observed.Fingerprint || observed.Tombstone != (TransitionTombstone{}) {
		t.Fatalf("restart observation=%+v", observed)
	}

	restarted.mu.Lock()
	memo := restarted.transitions[drain.TransitionID]
	restarted.moveToRecoveryLocked(drain.TransitionID, memo)
	clock = clock.Add(time.Second)
	err = restarted.expireMemoLocked(drain.TransitionID, memo, clock)
	restarted.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if observed := observe(restarted); observed.Class != TransitionEvidenceTombstone || observed.Result != result ||
		observed.Tombstone.TransitionID != drain.TransitionID || observed.Tombstone.Fingerprint != observed.Fingerprint ||
		observed.Tombstone.Result != result || observed.Tombstone.ExpiredAt.IsZero() || observed.Recovery != (TransitionRecovery{}) {
		t.Fatalf("evicted observation=%+v", observed)
	}
	if _, found, _ := evidence.LookupTransitionRecovery(context.Background(), drain.TransitionID); found {
		t.Fatal("atomic eviction retained recovery overlap")
	}
}

func TestManagerObserveTransitionRejectsEachMismatchAndEvidenceGap(t *testing.T) {
	evidence := NewMemoryTransitionTombstoneStore(32)
	manager, _ := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), TombstoneStore: evidence})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "observe-crash-source", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "observe-crash-source-boot", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	fence := managerFence(claimed.Authority)
	crash := TransferRequest{
		TransitionID: "observe-crash", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: fence,
		NewHolderID: "player-b", NewHolderBootID: "observe-crash-target-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
		Mode: TransferModeCrashTakeover, TakeoverProof: takeoverProofFor(fence),
	}
	if _, err := manager.Transfer(context.Background(), crash); err != nil {
		t.Fatal(err)
	}
	assertObserve := func(request TransitionObservationRequest, wantOK bool) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, observeErr := manager.ObserveTransition(ctx, request)
		if (observeErr == nil) != wantOK {
			t.Fatalf("request=%+v err=%v want_ok=%t", request, observeErr, wantOK)
		}
	}
	assertObserve(TransitionObservationRequest{Transfer: crash}, true)
	mutations := []func(*TransitionObservationRequest){
		func(v *TransitionObservationRequest) { v.PlannedDrain = true },
		func(v *TransitionObservationRequest) { v.Transfer.TransitionID += "-different" },
		func(v *TransitionObservationRequest) { v.Transfer.Domain = actorrouting.DomainFarm },
		func(v *TransitionObservationRequest) { v.Transfer.VirtualShard++ },
		func(v *TransitionObservationRequest) { v.Transfer.ExpectedFence.ExpectedHolderID += "-different" },
		func(v *TransitionObservationRequest) { v.Transfer.ExpectedFence.ExpectedHolderBootID += "-different" },
		func(v *TransitionObservationRequest) { v.Transfer.ExpectedFence.ExpectedAssignmentRevision++ },
		func(v *TransitionObservationRequest) { v.Transfer.ExpectedFence.ExpectedWriterEpoch++ },
		func(v *TransitionObservationRequest) { v.Transfer.ExpectedFence.ExpectedLeaseEpoch++ },
		func(v *TransitionObservationRequest) { v.Transfer.NewHolderID += "-different" },
		func(v *TransitionObservationRequest) { v.Transfer.NewHolderBootID += "-different" },
		func(v *TransitionObservationRequest) { v.Transfer.Endpoint += "-different" },
		func(v *TransitionObservationRequest) { v.Transfer.RequestedTTL += time.Millisecond },
		func(v *TransitionObservationRequest) { v.Transfer.Mode = TransferModeUnspecified },
		func(v *TransitionObservationRequest) { v.Transfer.TakeoverProof.ExpectedHolderBootID += "-different" },
		func(v *TransitionObservationRequest) {
			v.Transfer.TakeoverProof.HolderUnavailable = !v.Transfer.TakeoverProof.HolderUnavailable
		},
		func(v *TransitionObservationRequest) { v.Transfer.TakeoverProof.Evidence += "-different" },
		func(v *TransitionObservationRequest) {
			v.Transfer.TakeoverProof.LeaseExpired = !v.Transfer.TakeoverProof.LeaseExpired
		},
	}
	for _, mutate := range mutations {
		candidate := TransitionObservationRequest{Transfer: crash}
		mutate(&candidate)
		assertObserve(candidate, false)
	}

	complete, found, err := evidence.LookupTransitionRecovery(context.Background(), crash.TransitionID)
	if err != nil || !found {
		t.Fatalf("complete recovery found=%t err=%v", found, err)
	}
	evidence.mu.Lock()
	incomplete := complete
	incomplete.Published = false
	evidence.recoveries[crash.TransitionID] = incomplete
	evidence.mu.Unlock()
	assertObserve(TransitionObservationRequest{Transfer: crash}, false)
	evidence.mu.Lock()
	evidence.recoveries[crash.TransitionID] = complete
	evidence.mu.Unlock()

	manager.mu.Lock()
	manager.transitions[crash.TransitionID].commitAmbiguous = true
	manager.mu.Unlock()
	assertObserve(TransitionObservationRequest{Transfer: crash}, false)
	manager.mu.Lock()
	manager.transitions[crash.TransitionID].commitAmbiguous = false
	manager.mu.Unlock()

	evidence.mu.Lock()
	legacy := complete
	legacy.AuthorityAttemptedPresent = false
	legacy.RecoveryWireVersion = TransitionRecoveryWireLegacy
	evidence.recoveries[crash.TransitionID] = legacy
	evidence.mu.Unlock()
	assertObserve(TransitionObservationRequest{Transfer: crash}, false)
	evidence.mu.Lock()
	evidence.recoveries[crash.TransitionID] = complete
	evidence.mu.Unlock()

	manager.mu.Lock()
	memo := manager.transitions[crash.TransitionID]
	manager.moveToRecoveryLocked(crash.TransitionID, memo)
	if err := manager.expireMemoLocked(crash.TransitionID, memo, time.Now()); err != nil {
		manager.mu.Unlock()
		t.Fatal(err)
	}
	manager.mu.Unlock()
	tombstone, found, err := evidence.LookupTransitionTombstone(context.Background(), crash.TransitionID)
	if err != nil || !found {
		t.Fatalf("tombstone found=%t err=%v", found, err)
	}
	evidence.mu.Lock()
	generationMismatch := tombstone
	generationMismatch.Result.Authority.WriterEpoch++
	evidence.entries[crash.TransitionID] = generationMismatch
	evidence.mu.Unlock()
	assertObserve(TransitionObservationRequest{Transfer: crash}, false)
	evidence.mu.Lock()
	resultMismatch := tombstone
	resultMismatch.Result.Authority.HolderBootID += "-different"
	evidence.entries[crash.TransitionID] = resultMismatch
	evidence.mu.Unlock()
	assertObserve(TransitionObservationRequest{Transfer: crash}, false)
	evidence.mu.Lock()
	evidence.entries[crash.TransitionID] = tombstone
	evidence.mu.Unlock()
	assertObserve(TransitionObservationRequest{Transfer: crash}, true)
	evidence.mu.Lock()
	delete(evidence.entries, crash.TransitionID)
	evidence.mu.Unlock()
	assertObserve(TransitionObservationRequest{Transfer: crash}, false)
	evidence.mu.Lock()
	evidence.entries[crash.TransitionID] = tombstone
	evidence.recoveries[crash.TransitionID] = recoveryForMemo(memo)
	evidence.mu.Unlock()
	assertObserve(TransitionObservationRequest{Transfer: crash}, false)
}

func waitForResolverRoute(t *testing.T, resolver *actorrouting.StaticResolver, applier *actorrouting.SnapshotApplier, revision uint64, state actorrouting.ShardRouteState) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot := resolver.Snapshot()
		if snapshot.GlobalRevision == revision && applier.LastGlobalRevision() == revision && snapshot.Player.Routes[0].State == state {
			return
		}
		time.Sleep(time.Millisecond)
	}
	snapshot := resolver.Snapshot()
	t.Fatalf("resolver did not reach revision=%d state=%s: last=%d snapshot=%+v", revision, state, applier.LastGlobalRevision(), snapshot)
}

func TestRouteAssignmentManagerUnknownDoesNotPublish(t *testing.T) {
	var publishes int
	store := &unknownTransitionStore{}
	manager, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			publishes++
			return nil
		}),
		InitialSnapshot: managerSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-a", TombstoneStore: NewMemoryTransitionTombstoneStore(1024),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "unknown-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-b", NewHolderBootID: "boot-b", Endpoint: "player-b:9000", RequestedTTL: time.Second})
	if !errors.Is(err, ErrAuthorityCommitUnknown) || result.Status != authoritystore.ShardTransitionTimeoutUnknown || publishes != 0 {
		t.Fatalf("unknown result=%+v err=%v publishes=%d", result, err, publishes)
	}
}

func TestRouteAssignmentManagerInitializationAuthorityReadsAreCancellable(t *testing.T) {
	newOptions := func(ctx context.Context, reader AuthorityReader, tombstones TransitionTombstoneStore) ManagerOptions {
		return ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
			Store: newMemoryAuthorityStoreForTest(t), Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
				return nil
			}),
			AuthorityReader: reader, InitialSnapshot: managerSnapshot(), AuthorityLayoutVersion: "auth-v1",
			RouteManagerID: "route-manager-cancel", RouteManagerBootID: "cancel-boot", TombstoneStore: tombstones,
			InitializationContext: ctx,
		}
	}
	blockingReader := func(started chan<- struct{}) AuthorityReader {
		var once sync.Once
		return AuthorityReaderFunc(func(ctx context.Context, _ actorrouting.RouteDomain, _ uint32) (authoritystore.ShardWriteAuthority, bool, error) {
			once.Do(func() { close(started) })
			<-ctx.Done()
			return authoritystore.ShardWriteAuthority{}, false, ctx.Err()
		})
	}

	t.Run("bootstrap absence read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			_, err := NewRouteAssignmentManager(newOptions(ctx, blockingReader(started), NewMemoryTransitionTombstoneStore(8)))
			done <- err
		}()
		<-started
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("bootstrap authority read cancellation err=%v", err)
		}
	})

	t.Run("precommit recovery read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan struct{})
		tombstones := NewMemoryTransitionTombstoneStore(8)
		request := transitionRequest{
			kind: transitionClaim, transitionID: "cancel-recovery", domain: actorrouting.DomainPlayer, virtualShard: 0,
			newHolderID: "player-recovery", newHolderBoot: "recovery-boot", endpoint: "recovery:9000", ttl: time.Second,
		}
		fingerprint := transitionFingerprint(request)
		recovery := recoveryForRequest(request, fingerprint, authoritystore.ShardAuthorityTransitionResult{}, time.Now())
		if err := tombstones.ReserveTransitionRecovery(context.Background(), recovery); err != nil {
			t.Fatal(err)
		}
		recovery.AuthorityAttempted = true
		recovery.Committed = true // physical update stage; result remains uncommitted
		if err := tombstones.CommitTransitionRecovery(context.Background(), recovery); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, err := NewRouteAssignmentManager(newOptions(ctx, blockingReader(started), tombstones))
			done <- err
		}()
		<-started
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("recovery authority read cancellation err=%v", err)
		}
	})
}

func TestRouteAssignmentManagerRetriesPublishWithoutRepeatingAuthority(t *testing.T) {
	var publishCalls int
	var fail bool = true
	manager, store := newManagerForTest(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, _ actorrouting.RouteSnapshot) error {
		publishCalls++
		if fail {
			fail = false
			return errors.New("publisher unavailable")
		}
		return nil
	}))
	req := ClaimRequest{TransitionID: "publish-retry", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-b", NewHolderBootID: "player-b-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second}
	first, err := manager.Claim(context.Background(), req)
	if first.Status != authoritystore.ShardTransitionApplied || !errors.Is(err, ErrSnapshotPublish) || publishCalls != 1 || store.claim != 1 {
		t.Fatalf("first failure result=%+v err=%v publishes=%d claims=%d", first, err, publishCalls, store.claim)
	}
	second, err := manager.Claim(context.Background(), req)
	if err != nil || !reflect.DeepEqual(first, second) || publishCalls != 2 || store.claim != 1 {
		t.Fatalf("retry repeated authority or changed result: first=%+v second=%+v err=%v publishes=%d claims=%d", first, second, err, publishCalls, store.claim)
	}
	if got := manager.CurrentSnapshot().Holders[manager.CurrentSnapshot().Player.Routes[0].HolderIndex]; got.InstanceID != "player-b" {
		t.Fatalf("successful publish did not install committed holder=%+v", got)
	}
}

func TestRouteAssignmentManagerBootstrapOnlyInitialPlaceholderAndRestartRecovery(t *testing.T) {
	now := func() time.Time { return time.UnixMilli(1_900_000_000_000) }
	inner := newMemoryAuthorityStoreForTest(t)
	store := &recordingAuthorityStore{inner: inner}
	tombstones := NewMemoryTransitionTombstoneStore(8)
	firstPublisher := &alwaysFailPublisher{}
	manager, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: firstPublisher, InitialSnapshot: managerSnapshot(), AuthorityLayoutVersion: "auth-v1",
		RouteManagerID: "route-manager-a", RouteManagerBootID: "boot-a", Now: now, TombstoneStore: tombstones,
	})
	if err != nil {
		t.Fatal(err)
	}
	replayRequest := ClaimRequest{TransitionID: "restart-recover-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-recover", NewHolderBootID: "recover-boot", Endpoint: "recover:9000", RequestedTTL: time.Second}
	committed, err := manager.Claim(context.Background(), replayRequest)
	if !errors.Is(err, ErrSnapshotPublish) || committed.Status != authoritystore.ShardTransitionApplied || store.claim != 1 {
		t.Fatalf("initial placeholder claim did not commit before failed projection: result=%+v err=%v claims=%d", committed, err, store.claim)
	}

	restarted, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			return nil
		}),
		InitialSnapshot: manager.CurrentSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-b", RouteManagerBootID: "boot-b", Now: now,
		TombstoneStore: tombstones,
	})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := restarted.Claim(context.Background(), replayRequest)
	if err != nil || !reflect.DeepEqual(committed, replayed) || store.claim != 1 {
		t.Fatalf("restart replay did not resume the committed projection: committed=%+v replay=%+v err=%v claims=%d", committed, replayed, err, store.claim)
	}

	if _, err := restarted.Unassign(context.Background(), UnassignRequest{TransitionID: "restart-recover-unassign", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(committed.Authority)}); err != nil {
		t.Fatal(err)
	}
	inactive := restarted.CurrentSnapshot().Player.Routes[0]
	malformed := malformedAuthorityStore{inner: store, mutateClaim: func(result *authoritystore.ShardAuthorityTransitionResult) {
		// Simulate an adapter replay after the route manager restarted from an
		// ASSIGNING/UNASSIGNED checkpoint: the holder is exact, but the result
		// illegally retains the placeholder generation.
		result.Authority.AssignmentRevision = inactive.ShardRevision
		result.Authority.WriterEpoch = inactive.ExpectedWriterEpoch
		result.Authority.LeaseEpoch = inactive.ExpectedLeaseEpoch
		result.Authority.LeaseUntilMs = now().Add(time.Second).UnixMilli()
	}}
	var publishes int
	postUnassign, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: malformed, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			publishes++
			return nil
		}),
		InitialSnapshot: restarted.CurrentSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-c", RouteManagerBootID: "boot-c", Now: now,
		TombstoneStore: NewMemoryTransitionTombstoneStore(8),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := postUnassign.Claim(context.Background(), ClaimRequest{TransitionID: "restart-malformed-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-new", NewHolderBootID: "new-boot", Endpoint: "new:9000", RequestedTTL: time.Second}); !errors.Is(err, ErrAuthorityGeneration) || result.Status != authoritystore.ShardTransitionApplied || publishes != 0 {
		t.Fatalf("restarted ASSIGNING route regained bootstrap semantics: result=%+v err=%v publishes=%d", result, err, publishes)
	}
}

type failingRecoveryStore struct {
	*MemoryTransitionTombstoneStore
	failCommit bool
}

type failColdLoadCheckpointStore struct {
	*MemoryTransitionTombstoneStore
	fail bool
}

func (s *failColdLoadCheckpointStore) CommitTransitionRecovery(_ context.Context, recovery TransitionRecovery) error {
	if s.fail && recovery.ColdLoaded && recovery.LastState == actorrouting.ShardStateAssigning {
		s.fail = false
		return errors.New("cold-load checkpoint unavailable")
	}
	return s.MemoryTransitionTombstoneStore.CommitTransitionRecovery(context.Background(), recovery)
}

func (s *failingRecoveryStore) CommitTransitionRecovery(_ context.Context, recovery TransitionRecovery) error {
	if s.failCommit && recoveryAuthorityCommitted(recovery) {
		s.failCommit = false
		return errors.New("recovery marker unavailable")
	}
	return s.MemoryTransitionTombstoneStore.CommitTransitionRecovery(context.Background(), recovery)
}

func TestRouteAssignmentManagerRecoversAuthorityAfterRecoveryMarkerFailure(t *testing.T) {
	tombstones := &failingRecoveryStore{MemoryTransitionTombstoneStore: NewMemoryTransitionTombstoneStore(8), failCommit: true}
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), TombstoneStore: tombstones})
	req := ClaimRequest{TransitionID: "recovery-marker-failure", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "marker-holder", NewHolderBootID: "marker-boot", Endpoint: "marker:9000", RequestedTTL: time.Second}
	result, err := manager.Claim(context.Background(), req)
	if !errors.Is(err, ErrSnapshotPublish) || result.Status != authoritystore.ShardTransitionApplied || store.claim != 1 {
		t.Fatalf("authority marker failure result=%+v err=%v claims=%d", result, err, store.claim)
	}
	restarted, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			return nil
		}),
		InitialSnapshot: manager.CurrentSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-restart", RouteManagerBootID: "restart-boot", TombstoneStore: tombstones,
		AuthorityReader: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := restarted.Claim(context.Background(), req)
	if err != nil || !reflect.DeepEqual(result, replayed) || store.claim != 1 || restarted.CurrentSnapshot().Player.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("restart lost committed authority after marker failure: result=%+v replay=%+v err=%v claims=%d snapshot=%+v", result, replayed, err, store.claim, restarted.CurrentSnapshot())
	}
}

func TestRouteAssignmentManagerRejectsRouteManagerRenew(t *testing.T) {
	manager, store := newManagerForTest(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}))
	_, err := manager.Renew(context.Background(), RenewRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-a"},
		Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: authoritystore.ShardWriteFence{ExpectedHolderID: "player-a", ExpectedHolderBootID: "player-a-boot", ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1},
		RequestedTTL:  time.Second,
	})
	if !errors.Is(err, ErrHolderRenewOnly) || store.claim != 0 || store.trans != 0 {
		t.Fatalf("route manager renew was not rejected: err=%v claims=%d transfers=%d", err, store.claim, store.trans)
	}
}

type unknownTransitionStore struct{}

type commitThenLoseResponseStore struct {
	inner *recordingAuthorityStore
	mu    sync.Mutex
	lose  bool
}

func (s *commitThenLoseResponseStore) ClaimShardAuthority(ctx context.Context, req authoritystore.ClaimShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	result, err := s.inner.ClaimShardAuthority(ctx, req)
	s.mu.Lock()
	lose := s.lose
	s.lose = false
	s.mu.Unlock()
	if err == nil && lose {
		return authoritystore.ShardAuthorityTransitionResult{}, errors.New("response lost after commit")
	}
	return result, err
}
func (s *commitThenLoseResponseStore) RenewShardAuthority(ctx context.Context, req authoritystore.RenewShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.inner.RenewShardAuthority(ctx, req)
}
func (s *commitThenLoseResponseStore) TransferShardAuthority(ctx context.Context, req authoritystore.TransferShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.inner.TransferShardAuthority(ctx, req)
}
func (s *commitThenLoseResponseStore) UnassignShardAuthority(ctx context.Context, req authoritystore.UnassignShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.inner.UnassignShardAuthority(ctx, req)
}

func TestRouteAssignmentManagerCommitThenLostResponseRecoversWithoutMutation(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%t", restart), func(t *testing.T) {
			now := func() time.Time { return time.UnixMilli(1_900_000_000_000) }
			inner := &recordingAuthorityStore{inner: newMemoryAuthorityStoreForTest(t)}
			store := &commitThenLoseResponseStore{inner: inner, lose: true}
			tombstones := NewMemoryTransitionTombstoneStore(8)
			publishes := 0
			newManager := func(snapshot actorrouting.RouteSnapshot) *RouteAssignmentManager {
				manager, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
					Store: store, AuthorityReader: inner, Publisher: testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, next actorrouting.RouteSnapshot) error {
						publishes++
						if next.Player.Routes[0].State != actorrouting.ShardStateActive {
							t.Fatalf("published non-active snapshot: %+v", next.Player.Routes[0])
						}
						return nil
					}), InitialSnapshot: snapshot, AuthorityLayoutVersion: "auth-v1",
					RouteManagerID: "lost-response-manager", RouteManagerBootID: "lost-response-boot",
					TombstoneStore: tombstones, Now: now,
				})
				if err != nil {
					t.Fatal(err)
				}
				return manager
			}
			manager := newManager(managerSnapshot())
			req := ClaimRequest{TransitionID: "lost-response", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "holder-b", NewHolderBootID: "boot-b", Endpoint: "holder-b:9000", RequestedTTL: time.Second}
			if result, err := manager.Claim(context.Background(), req); !errors.Is(err, ErrAuthorityCommitUnknown) || result.Status != authoritystore.ShardTransitionUnknown || inner.claim != 1 || publishes != 0 {
				t.Fatalf("first result=%+v err=%v mutations=%d publishes=%d", result, err, inner.claim, publishes)
			}
			if restart {
				manager = newManager(manager.CurrentSnapshot())
			}
			result, err := manager.Claim(context.Background(), req)
			if err != nil || result.Status != authoritystore.ShardTransitionApplied || inner.claim != 1 || publishes != 1 || result.Authority.HolderID != "holder-b" || result.Authority.HolderBootID != "boot-b" {
				t.Fatalf("recovered result=%+v err=%v mutations=%d publishes=%d", result, err, inner.claim, publishes)
			}
		})
	}
}

func TestRouteAssignmentManagerLegacyMissingAttemptBoundaryReconcilesCommittedAuthority(t *testing.T) {
	now := func() time.Time { return time.UnixMilli(1_900_000_000_000) }
	inner := &recordingAuthorityStore{inner: newMemoryAuthorityStoreForTest(t)}
	store := &commitThenLoseResponseStore{inner: inner, lose: true}
	tombstones := NewMemoryTransitionTombstoneStore(8)
	publishes := 0
	newManager := func(snapshot actorrouting.RouteSnapshot) *RouteAssignmentManager {
		manager, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
			Store: store, AuthorityReader: inner, Publisher: testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, next actorrouting.RouteSnapshot) error {
				publishes++
				if next.Player.Routes[0].State != actorrouting.ShardStateActive {
					t.Fatalf("published non-active snapshot: %+v", next.Player.Routes[0])
				}
				return nil
			}), InitialSnapshot: snapshot, AuthorityLayoutVersion: "auth-v1",
			RouteManagerID: "legacy-attempt-manager", RouteManagerBootID: "legacy-attempt-boot",
			TombstoneStore: tombstones, Now: now,
		})
		if err != nil {
			t.Fatal(err)
		}
		return manager
	}
	manager := newManager(managerSnapshot())
	req := ClaimRequest{TransitionID: "legacy-missing-attempt", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "holder-b", NewHolderBootID: "boot-b", Endpoint: "holder-b:9000", RequestedTTL: time.Second}
	if result, err := manager.Claim(context.Background(), req); !errors.Is(err, ErrAuthorityCommitUnknown) || result.Status != authoritystore.ShardTransitionUnknown || inner.claim != 1 {
		t.Fatalf("first result=%+v err=%v mutations=%d", result, err, inner.claim)
	}

	// Simulate the canonical parent wire: it had no AuthorityAttempted field,
	// and therefore decoded false is UNKNOWN rather than definitive NOT_SENT.
	tombstones.mu.Lock()
	legacy := tombstones.recoveries[req.TransitionID]
	legacy.AuthorityAttempted = false
	legacy.AuthorityAttemptedPresent = false
	legacy.RecoveryWireVersion = TransitionRecoveryWireLegacy
	legacy.Committed = false
	legacy.Result = authoritystore.ShardAuthorityTransitionResult{}
	tombstones.recoveries[req.TransitionID] = legacy
	tombstones.mu.Unlock()

	manager = newManager(manager.CurrentSnapshot())
	result, err := manager.Claim(context.Background(), req)
	if err != nil || result.Status != authoritystore.ShardTransitionApplied || inner.claim != 1 || publishes != 1 || result.Authority.LastTransitionID != req.TransitionID {
		t.Fatalf("legacy recovery result=%+v err=%v mutations=%d publishes=%d", result, err, inner.claim, publishes)
	}
}

type fixedIntentAuthorityReader struct {
	observation authoritystore.ShardWriteAuthorityObservation
	found       bool
}

func (r fixedIntentAuthorityReader) ReadShardWriteAuthority(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error) {
	return r.observation.Authority, r.found, nil
}
func (r fixedIntentAuthorityReader) ReadShardWriteAuthorityObservation(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthorityObservation, bool, error) {
	return r.observation, r.found, nil
}

func TestRouteAssignmentManagerWrongLegalIntentDigestCannotPromote(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	tombstones := NewMemoryTransitionTombstoneStore(8)
	request := transitionRequest{kind: transitionClaim, transitionID: "wrong-digest", domain: actorrouting.DomainPlayer, virtualShard: 0, newHolderID: "holder-b", newHolderBoot: "boot-b", endpoint: "holder-b:9000", ttl: time.Second}
	fingerprint := transitionFingerprint(request)
	recovery := recoveryForRequest(request, fingerprint, authoritystore.ShardAuthorityTransitionResult{}, now)
	if err := tombstones.ReserveTransitionRecovery(context.Background(), recovery); err != nil {
		t.Fatal(err)
	}
	recovery.AuthorityAttempted = true
	recovery.Committed = true // physical update stage; result remains uncommitted
	if err := tombstones.CommitTransitionRecovery(context.Background(), recovery); err != nil {
		t.Fatal(err)
	}
	authorityValue := authoritystore.ShardWriteAuthority{AuthorityLayoutVersion: "auth-v1", HashVersion: 2, HashSeed: 202608040001, Domain: actorrouting.DomainPlayer, VirtualShard: 0, HolderID: "holder-b", HolderBootID: "boot-b", AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1, State: authoritystore.ShardAuthorityActive, LeaseUntilMs: now.Add(time.Second).UnixMilli(), LastTransitionID: request.transitionID, LastTransitionAtMs: now.UnixMilli()}
	reader := fixedIntentAuthorityReader{observation: authoritystore.ShardWriteAuthorityObservation{Authority: authorityValue, LastTransitionDigest: strings.Repeat("a", 64)}, found: true}
	manager, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: unknownTransitionStore{}, AuthorityReader: reader, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			t.Fatal("wrong digest published")
			return nil
		}),
		InitialSnapshot: managerSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "digest-manager", RouteManagerBootID: "digest-boot", TombstoneStore: tombstones, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = manager.RecoverPendingProjections(ctx, acceptPendingProjectionTarget, nil)
	cancel()
	if !errors.Is(err, ErrStaleTransition) {
		t.Fatalf("wrong digest recovery err=%v", err)
	}
	reader.observation.LastTransitionDigest = transitionIntentDigest("auth-v1", request, time.Second.Milliseconds())
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = manager.RecoverPendingProjections(ctx, acceptPendingProjectionTarget, nil)
	cancel()
	if !errors.Is(err, ErrStaleTransition) {
		t.Fatalf("digest mismatch was not sticky: %v", err)
	}
}

func TestRouteAssignmentManagerUnattemptedRecoveryCleanupIsDurableAndRetryable(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	tombstones := &failNextDeleteRecoveryStore{MemoryTransitionTombstoneStore: NewMemoryTransitionTombstoneStore(8)}
	request := transitionRequest{
		kind: transitionClaim, transitionID: "unattempted-cleanup", domain: actorrouting.DomainPlayer, virtualShard: 0,
		newHolderID: "holder-b", newHolderBoot: "boot-b", endpoint: "holder-b:9000", ttl: time.Second,
	}
	recovery := recoveryForRequest(request, transitionFingerprint(request), authoritystore.ShardAuthorityTransitionResult{}, now)
	if err := tombstones.ReserveTransitionRecovery(context.Background(), recovery); err != nil {
		t.Fatal(err)
	}
	tombstones.armDeleteFailure()
	store := &recordingAuthorityStore{inner: newMemoryAuthorityStoreForTest(t)}
	options := ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, AuthorityReader: store,
		Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			return nil
		}),
		InitialSnapshot: managerSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "cleanup-manager",
		RouteManagerBootID: "cleanup-boot", TombstoneStore: tombstones, Now: func() time.Time { return now },
	}
	if _, err := NewRouteAssignmentManager(options); !errors.Is(err, ErrInvalidManager) {
		t.Fatalf("cleanup failure startup err=%v", err)
	}
	if recoveries, err := tombstones.LoadTransitionRecoveries(context.Background()); err != nil || len(recoveries) != 1 || recoveries[0].AuthorityAttempted {
		t.Fatalf("cleanup failure lost unattempted recovery=%+v err=%v", recoveries, err)
	}
	if _, err := NewRouteAssignmentManager(options); err != nil {
		t.Fatalf("cleanup retry startup err=%v", err)
	}
	if recoveries, err := tombstones.LoadTransitionRecoveries(context.Background()); err != nil || len(recoveries) != 0 {
		t.Fatalf("cleanup retry retained recovery=%+v err=%v", recoveries, err)
	}
}

type fullTombstoneStore struct {
	mu    sync.Mutex
	saves int
}

func (s *fullTombstoneStore) LoadTransitionTombstones(_ context.Context) ([]TransitionTombstone, error) {
	return nil, nil
}

func (s *fullTombstoneStore) LookupTransitionTombstone(context.Context, string) (TransitionTombstone, bool, error) {
	return TransitionTombstone{}, false, nil
}

func (s *fullTombstoneStore) SaveTransitionTombstone(context.Context, TransitionTombstone) error {
	s.mu.Lock()
	s.saves++
	s.mu.Unlock()
	return ErrTransitionTombstoneCap
}

func (s *fullTombstoneStore) LoadTransitionRecoveries(_ context.Context) ([]TransitionRecovery, error) {
	return nil, nil
}
func (s *fullTombstoneStore) LookupTransitionRecovery(context.Context, string) (TransitionRecovery, bool, error) {
	return TransitionRecovery{}, false, nil
}
func (s *fullTombstoneStore) ReserveTransitionRecovery(context.Context, TransitionRecovery) error {
	return nil
}
func (s *fullTombstoneStore) CommitTransitionRecovery(context.Context, TransitionRecovery) error {
	return nil
}
func (s *fullTombstoneStore) DeleteTransitionRecovery(context.Context, string) error {
	return nil
}

type rejectingAuthorityStore struct {
	inner  authoritystore.ShardWriteAuthorityStore
	mu     sync.Mutex
	reject bool
	claim  int
}

type definitiveNotSentErrorStore struct {
	inner        authoritystore.ShardWriteAuthorityStore
	failClaim    bool
	failTransfer bool
}

func (s *definitiveNotSentErrorStore) ClaimShardAuthority(ctx context.Context, req authoritystore.ClaimShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	if s.failClaim {
		return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionUnavailableNotSent}, context.DeadlineExceeded
	}
	return s.inner.ClaimShardAuthority(ctx, req)
}
func (s *definitiveNotSentErrorStore) RenewShardAuthority(ctx context.Context, req authoritystore.RenewShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.inner.RenewShardAuthority(ctx, req)
}
func (s *definitiveNotSentErrorStore) TransferShardAuthority(ctx context.Context, req authoritystore.TransferShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	if s.failTransfer {
		return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionUnavailableNotSent}, context.DeadlineExceeded
	}
	return s.inner.TransferShardAuthority(ctx, req)
}
func (s *definitiveNotSentErrorStore) UnassignShardAuthority(ctx context.Context, req authoritystore.UnassignShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.inner.UnassignShardAuthority(ctx, req)
}

func TestRouteAssignmentManagerDefinitiveNotSentErrorReleasesReservation(t *testing.T) {
	t.Run("claim", func(t *testing.T) {
		tombstones := NewMemoryTransitionTombstoneStore(8)
		store := &definitiveNotSentErrorStore{inner: newMemoryAuthorityStoreForTest(t), failClaim: true}
		manager := newRawManagerForTest(t, store, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			return nil
		}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), TombstoneStore: tombstones})
		req := ClaimRequest{TransitionID: "definitive-not-sent-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "holder-a", NewHolderBootID: "not-sent-boot", Endpoint: "holder-a:9000", RequestedTTL: time.Second}
		if _, err := manager.Claim(context.Background(), req); !errors.Is(err, ErrAuthorityUnavailable) {
			t.Fatalf("not-sent claim err=%v", err)
		}
		if manager.reservedSlots != 0 || len(manager.reservations) != 0 || manager.transitionMemoLocked(req.TransitionID) != nil {
			t.Fatalf("not-sent claim retained state slots=%d reservations=%d", manager.reservedSlots, len(manager.reservations))
		}
		if _, found, err := tombstones.LookupTransitionRecovery(context.Background(), req.TransitionID); err != nil || found {
			t.Fatalf("not-sent durable recovery found=%v err=%v", found, err)
		}
		store.failClaim = false
		if result, err := manager.Claim(context.Background(), req); err != nil || result.Status != authoritystore.ShardTransitionApplied {
			t.Fatalf("released claim retry result=%+v err=%v", result, err)
		}
	})

	t.Run("drain", func(t *testing.T) {
		tombstones := NewMemoryTransitionTombstoneStore(8)
		store := &definitiveNotSentErrorStore{inner: newMemoryAuthorityStoreForTest(t)}
		manager := newRawManagerForTest(t, store, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			return nil
		}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), TombstoneStore: tombstones})
		claimed, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "definitive-not-sent-source", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "holder-a", NewHolderBootID: "source-boot", Endpoint: "holder-a:9000", RequestedTTL: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		store.failTransfer = true
		req := TransferRequest{TransitionID: "definitive-not-sent-drain", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority), NewHolderID: "holder-b", NewHolderBootID: "target-boot", Endpoint: "holder-b:9000", RequestedTTL: time.Second}
		if _, err := manager.Drain(context.Background(), req, &recordingDrainController{}); !errors.Is(err, ErrAuthorityUnavailable) {
			t.Fatalf("not-sent drain err=%v", err)
		}
		if manager.reservedSlots != 0 || len(manager.reservations) != 0 || manager.transitionMemoLocked(req.TransitionID) != nil {
			t.Fatalf("not-sent drain retained state slots=%d reservations=%d", manager.reservedSlots, len(manager.reservations))
		}
		if _, found, err := tombstones.LookupTransitionRecovery(context.Background(), req.TransitionID); err != nil || found {
			t.Fatalf("not-sent drain durable recovery found=%v err=%v", found, err)
		}
	})
}

func (s *rejectingAuthorityStore) ClaimShardAuthority(ctx context.Context, req authoritystore.ClaimShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	s.mu.Lock()
	s.claim++
	reject := s.reject
	s.mu.Unlock()
	if reject {
		return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionFenceMismatch}, nil
	}
	return s.inner.ClaimShardAuthority(ctx, req)
}

func (s *rejectingAuthorityStore) RenewShardAuthority(ctx context.Context, req authoritystore.RenewShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.inner.RenewShardAuthority(ctx, req)
}
func (s *rejectingAuthorityStore) TransferShardAuthority(ctx context.Context, req authoritystore.TransferShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.inner.TransferShardAuthority(ctx, req)
}
func (s *rejectingAuthorityStore) UnassignShardAuthority(ctx context.Context, req authoritystore.UnassignShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.inner.UnassignShardAuthority(ctx, req)
}

func TestRouteAssignmentManagerReservesMemoCapacityBeforeAuthorityCommit(t *testing.T) {
	publisher := &alwaysFailPublisher{}
	tombstones := &fullTombstoneStore{}
	manager, store := newManagerForTestWithOptions(t, publisher, ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), MemoLimit: 2, MemoRetention: time.Hour, TombstoneStore: tombstones})
	firstReq := ClaimRequest{TransitionID: "reserve-cap-first", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "holder-a", NewHolderBootID: "reserve-a", Endpoint: "holder-a:9000", RequestedTTL: time.Second}
	first, err := manager.Claim(context.Background(), firstReq)
	if !errors.Is(err, ErrSnapshotPublish) {
		t.Fatalf("first publication failure=%v result=%+v", err, first)
	}
	second, err := manager.Transfer(context.Background(), TransferRequest{TransitionID: "reserve-cap-second", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(first.Authority), NewHolderID: "holder-b", NewHolderBootID: "reserve-b", Endpoint: "holder-b:9000", RequestedTTL: time.Second, Mode: TransferModeCrashTakeover, TakeoverProof: takeoverProofFor(managerFence(first.Authority))})
	if !errors.Is(err, ErrSnapshotPublish) {
		t.Fatalf("second publication failure=%v result=%+v", err, second)
	}
	thirdFence := managerFence(second.Authority)
	third, err := manager.Transfer(context.Background(), TransferRequest{TransitionID: "reserve-cap-third", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: thirdFence, NewHolderID: "holder-c", NewHolderBootID: "reserve-c", Endpoint: "holder-c:9000", RequestedTTL: time.Second, Mode: TransferModeCrashTakeover, TakeoverProof: takeoverProofFor(thirdFence)})
	if !errors.Is(err, ErrTransitionTombstoneCap) || third.Status != authoritystore.ShardTransitionUnknown || store.trans != 1 || manager.reservedSlots != 0 {
		t.Fatalf("capacity failure happened after Authority or leaked reservation: result=%+v err=%v transfers=%d reserved=%d", third, err, store.trans, manager.reservedSlots)
	}
	if tombstones.saves != 0 {
		t.Fatalf("pinned incomplete memo was unexpectedly evicted: saves=%d", tombstones.saves)
	}
}

func TestRouteAssignmentManagerReleasesRejectedButRetainsUnknownAuthority(t *testing.T) {
	inner := newMemoryAuthorityStoreForTest(t)
	store := &rejectingAuthorityStore{inner: inner, reject: true}
	manager := newRawManagerForTest(t, store, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions()})
	firstReq := ClaimRequest{TransitionID: "reserve-release-rejected", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "holder-a", NewHolderBootID: "reserve-release-a", Endpoint: "holder-a:9000", RequestedTTL: time.Second}
	if _, err := manager.Claim(context.Background(), firstReq); !errors.Is(err, ErrAuthorityRejected) || manager.reservedSlots != 0 || len(manager.reservations) != 0 {
		t.Fatalf("rejected authority leaked reservation: err=%v reserved=%d reservations=%d", err, manager.reservedSlots, len(manager.reservations))
	}
	store.mu.Lock()
	store.reject = false
	store.mu.Unlock()
	if result, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "reserve-release-success", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "holder-a", NewHolderBootID: "reserve-release-a", Endpoint: "holder-a:9000", RequestedTTL: time.Second}); err != nil || result.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("released reservation was not reusable: result=%+v err=%v", result, err)
	}

	// An UNKNOWN response may have committed and therefore keeps the unique
	// durable reservation until exact intent-aware reconciliation.
	unknownManager := newRawManagerForTest(t, unknownTransitionStore{}, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions()})
	if _, err := unknownManager.Claim(context.Background(), ClaimRequest{TransitionID: "reserve-release-unknown", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "holder-a", NewHolderBootID: "reserve-unknown", Endpoint: "holder-a:9000", RequestedTTL: time.Second}); !errors.Is(err, ErrAuthorityCommitUnknown) || unknownManager.reservedSlots != 1 || len(unknownManager.reservations) != 1 {
		t.Fatalf("unknown authority leaked reservation: err=%v reserved=%d reservations=%d", err, unknownManager.reservedSlots, len(unknownManager.reservations))
	}
}

func (unknownTransitionStore) ClaimShardAuthority(context.Context, authoritystore.ClaimShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionTimeoutUnknown}, nil
}
func (unknownTransitionStore) RenewShardAuthority(context.Context, authoritystore.RenewShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return authoritystore.ShardAuthorityTransitionResult{}, nil
}
func (unknownTransitionStore) TransferShardAuthority(context.Context, authoritystore.TransferShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionTimeoutUnknown}, nil
}
func (unknownTransitionStore) UnassignShardAuthority(context.Context, authoritystore.UnassignShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionTimeoutUnknown}, nil
}

type opaqueAuthorityStore struct {
	inner authoritystore.ShardWriteAuthorityStore
}

func (s opaqueAuthorityStore) ClaimShardAuthority(ctx context.Context, req authoritystore.ClaimShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.inner.ClaimShardAuthority(ctx, req)
}
func (s opaqueAuthorityStore) RenewShardAuthority(ctx context.Context, req authoritystore.RenewShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.inner.RenewShardAuthority(ctx, req)
}
func (s opaqueAuthorityStore) TransferShardAuthority(ctx context.Context, req authoritystore.TransferShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.inner.TransferShardAuthority(ctx, req)
}
func (s opaqueAuthorityStore) UnassignShardAuthority(ctx context.Context, req authoritystore.UnassignShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.inner.UnassignShardAuthority(ctx, req)
}

type deadlineRegressionStore struct {
	inner  authoritystore.ShardWriteAuthorityStore
	result authoritystore.ShardAuthorityTransitionResult
}

func (s deadlineRegressionStore) ClaimShardAuthority(ctx context.Context, req authoritystore.ClaimShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.inner.ClaimShardAuthority(ctx, req)
}
func (s deadlineRegressionStore) RenewShardAuthority(context.Context, authoritystore.RenewShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.result, nil
}
func (s deadlineRegressionStore) TransferShardAuthority(ctx context.Context, req authoritystore.TransferShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.inner.TransferShardAuthority(ctx, req)
}
func (s deadlineRegressionStore) UnassignShardAuthority(ctx context.Context, req authoritystore.UnassignShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.inner.UnassignShardAuthority(ctx, req)
}

type malformedAuthorityStore struct {
	inner          authoritystore.ShardWriteAuthorityStore
	mutateClaim    func(*authoritystore.ShardAuthorityTransitionResult)
	mutateTransfer func(*authoritystore.ShardAuthorityTransitionResult)
	mutateUnassign func(*authoritystore.ShardAuthorityTransitionResult)
}

func (s malformedAuthorityStore) ClaimShardAuthority(ctx context.Context, req authoritystore.ClaimShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	result, err := s.inner.ClaimShardAuthority(ctx, req)
	if err == nil && s.mutateClaim != nil {
		s.mutateClaim(&result)
	}
	return result, err
}

func (s malformedAuthorityStore) RenewShardAuthority(ctx context.Context, req authoritystore.RenewShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.inner.RenewShardAuthority(ctx, req)
}

func (s malformedAuthorityStore) TransferShardAuthority(ctx context.Context, req authoritystore.TransferShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	result, err := s.inner.TransferShardAuthority(ctx, req)
	if err == nil && s.mutateTransfer != nil {
		s.mutateTransfer(&result)
	}
	return result, err
}

func (s malformedAuthorityStore) UnassignShardAuthority(ctx context.Context, req authoritystore.UnassignShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	result, err := s.inner.UnassignShardAuthority(ctx, req)
	if err == nil && s.mutateUnassign != nil {
		s.mutateUnassign(&result)
	}
	return result, err
}

func TestRouteAssignmentManagerRequiresRecoverableTransitionFloor(t *testing.T) {
	_, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: unknownTransitionStore{}, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			return nil
		}),
		InitialSnapshot: managerSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-a",
	})
	if !errors.Is(err, ErrInvalidManager) {
		t.Fatalf("manager accepted missing transition tombstone store: %v", err)
	}
}

func TestRouteAssignmentManagerRejectsSameGenerationReplayIdentityDrift(t *testing.T) {
	var claimed authoritystore.ShardWriteAuthority
	publisher := &failOncePublisher{fail: true}
	manager, _ := newManagerForTestWithOptions(t, publisher, ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		AuthorityReader: AuthorityReaderFunc(func(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error) {
			observed := claimed
			observed.LastTransitionID = "different-transition"
			return observed, true, nil
		}),
	})
	req := ClaimRequest{TransitionID: "replay-identity-drift", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-b", NewHolderBootID: "replay-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second}
	var err error
	claimedResult, err := manager.Claim(context.Background(), req)
	if !errors.Is(err, ErrSnapshotPublish) {
		t.Fatalf("initial publication err=%v result=%+v", err, claimedResult)
	}
	claimed = claimedResult.Authority
	before := len(publisher.publishes)
	if _, err := manager.Claim(context.Background(), req); !errors.Is(err, ErrStaleTransition) || len(publisher.publishes) != before {
		t.Fatalf("same-generation identity drift was published: err=%v publishes=%d", err, len(publisher.publishes))
	}
}

func TestRouteAssignmentManagerRejectsMalformedAuthorityAddressBeforeProjection(t *testing.T) {
	calc, err := actorrouting.NewCalculator("classic-farm", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040001}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	inner, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{Resolver: calc, AuthorityLayoutVersion: "auth-v1", Now: func() time.Time { return time.UnixMilli(1_900_000_000_000) }})
	if err != nil {
		t.Fatal(err)
	}
	store := malformedAuthorityStore{inner: inner, mutateClaim: func(result *authoritystore.ShardAuthorityTransitionResult) {
		result.Authority.VirtualShard = 1
	}}
	publishes := 0
	manager, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			publishes++
			return nil
		}),
		InitialSnapshot: managerSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-a", TombstoneStore: NewMemoryTransitionTombstoneStore(1024),
		Now: func() time.Time { return time.UnixMilli(1_900_000_000_000) },
		AuthorityReader: AuthorityReaderFunc(func(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error) {
			return authoritystore.ShardWriteAuthority{}, false, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "malformed-address", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-b", NewHolderBootID: "bad-address-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second})
	if !errors.Is(err, ErrAuthorityAddress) || result.Status != authoritystore.ShardTransitionApplied || publishes != 0 {
		t.Fatalf("malformed authority address was projected: result=%+v err=%v publishes=%d", result, err, publishes)
	}
}

func TestRouteAssignmentManagerRejectsMalformedTransferGenerationBeforeProjection(t *testing.T) {
	calc, err := actorrouting.NewCalculator("classic-farm", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040001}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	inner, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{Resolver: calc, AuthorityLayoutVersion: "auth-v1", Now: func() time.Time { return time.UnixMilli(1_900_000_000_000) }})
	if err != nil {
		t.Fatal(err)
	}
	store := malformedAuthorityStore{inner: inner, mutateTransfer: func(result *authoritystore.ShardAuthorityTransitionResult) {
		result.Authority.WriterEpoch = result.Authority.WriterEpoch - 1
	}}
	publishes := 0
	manager, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			publishes++
			return nil
		}),
		InitialSnapshot: managerSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-a", TombstoneStore: NewMemoryTransitionTombstoneStore(1024),
		Now: func() time.Time { return time.UnixMilli(1_900_000_000_000) },
		AuthorityReader: AuthorityReaderFunc(func(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error) {
			return authoritystore.ShardWriteAuthority{}, false, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "malformed-transfer-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-a", NewHolderBootID: "malformed-transfer-a", Endpoint: "player-a:9000", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	fence := managerFence(claimed.Authority)
	result, err := manager.Transfer(context.Background(), TransferRequest{TransitionID: "malformed-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: fence, NewHolderID: "player-b", NewHolderBootID: "malformed-transfer-b", Endpoint: "player-b:9000", RequestedTTL: time.Second, Mode: TransferModeCrashTakeover, TakeoverProof: takeoverProofFor(fence)})
	if !errors.Is(err, ErrAuthorityGeneration) || result.Status != authoritystore.ShardTransitionApplied || publishes != 1 {
		t.Fatalf("malformed transfer generation was projected: result=%+v err=%v publishes=%d", result, err, publishes)
	}
}

func TestRouteAssignmentManagerRequiresExactAuthorityHolderBinding(t *testing.T) {
	for name, mutate := range map[string]func(*authoritystore.ShardAuthorityTransitionResult){
		"claim holder": func(result *authoritystore.ShardAuthorityTransitionResult) { result.Authority.HolderID = "holder-c" },
		"claim boot":   func(result *authoritystore.ShardAuthorityTransitionResult) { result.Authority.HolderBootID = "boot-c" },
	} {
		t.Run(name, func(t *testing.T) {
			store := malformedAuthorityStore{inner: newMemoryAuthorityStoreForTest(t), mutateClaim: mutate}
			publishes := 0
			manager := newRawManagerForTest(t, store, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
				publishes++
				return nil
			}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions()})
			id := "exact-holder-" + name
			result, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: id, Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "holder-b", NewHolderBootID: "boot-b", Endpoint: "holder-b:9000", RequestedTTL: time.Second})
			if !errors.Is(err, ErrAuthorityAddress) || result.Status != authoritystore.ShardTransitionApplied || publishes != 0 {
				t.Fatalf("malformed claim holder was projected: result=%+v err=%v publishes=%d", result, err, publishes)
			}
			if memo := manager.transitionMemoLocked(id); memo == nil || !memo.commitAmbiguous {
				t.Fatalf("malformed committed claim did not retain ambiguity: %+v", memo)
			}
		})
	}

	for name, mutate := range map[string]func(*authoritystore.ShardAuthorityTransitionResult){
		"transfer holder": func(result *authoritystore.ShardAuthorityTransitionResult) { result.Authority.HolderID = "holder-c" },
		"transfer boot":   func(result *authoritystore.ShardAuthorityTransitionResult) { result.Authority.HolderBootID = "boot-c" },
	} {
		t.Run(name, func(t *testing.T) {
			store := malformedAuthorityStore{inner: newMemoryAuthorityStoreForTest(t), mutateTransfer: mutate}
			publishes := 0
			manager := newRawManagerForTest(t, store, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
				publishes++
				return nil
			}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions()})
			claimed, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "exact-transfer-claim-" + name, Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "holder-a", NewHolderBootID: "boot-a", Endpoint: "holder-a:9000", RequestedTTL: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			id := "exact-transfer-" + name
			result, err := manager.Transfer(context.Background(), TransferRequest{TransitionID: id, Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority), NewHolderID: "holder-b", NewHolderBootID: "boot-b", Endpoint: "holder-b:9000", RequestedTTL: time.Second, Mode: TransferModeCrashTakeover, TakeoverProof: takeoverProofFor(managerFence(claimed.Authority))})
			if !errors.Is(err, ErrAuthorityAddress) || result.Status != authoritystore.ShardTransitionApplied || publishes != 1 {
				t.Fatalf("malformed transfer holder was projected: result=%+v err=%v publishes=%d", result, err, publishes)
			}
			if memo := manager.transitionMemoLocked(id); memo == nil || !memo.commitAmbiguous {
				t.Fatalf("malformed committed transfer did not retain ambiguity: %+v", memo)
			}
		})
	}

	store := malformedAuthorityStore{inner: newMemoryAuthorityStoreForTest(t), mutateUnassign: func(result *authoritystore.ShardAuthorityTransitionResult) {
		result.Authority.HolderID = "stale-holder"
	}}
	publishes := 0
	manager := newRawManagerForTest(t, store, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		publishes++
		return nil
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions()})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "exact-unassign-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "holder-a", NewHolderBootID: "boot-a", Endpoint: "holder-a:9000", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	id := "exact-unassign"
	result, err := manager.Unassign(context.Background(), UnassignRequest{TransitionID: id, Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority)})
	if !errors.Is(err, ErrAuthorityAddress) || result.Status != authoritystore.ShardTransitionApplied || publishes != 1 {
		t.Fatalf("malformed unassign holder was projected: result=%+v err=%v publishes=%d", result, err, publishes)
	}
	if memo := manager.transitionMemoLocked(id); memo == nil || !memo.commitAmbiguous {
		t.Fatalf("malformed committed unassign did not retain ambiguity: %+v", memo)
	}
}

func TestRouteAssignmentManagerReplayWithoutAuthoritativeReadFailsClosed(t *testing.T) {
	calc, err := actorrouting.NewCalculator("classic-farm", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040001}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	inner, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{Resolver: calc, AuthorityLayoutVersion: "auth-v1", Now: func() time.Time { return time.UnixMilli(1_900_000_000_000) }})
	if err != nil {
		t.Fatal(err)
	}
	store := opaqueAuthorityStore{inner: inner}
	publisher := &failOncePublisher{fail: true}
	bootstrapReads := 0
	manager, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: publisher, InitialSnapshot: managerSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-a", RouteManagerBootID: "route-manager-boot",
		Now: func() time.Time { return time.UnixMilli(1_900_000_000_000) }, TombstoneStore: NewMemoryTransitionTombstoneStore(1024),
		AuthorityReader: AuthorityReaderFunc(func(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error) {
			if bootstrapReads < 2 {
				bootstrapReads++
				return authoritystore.ShardWriteAuthority{}, false, nil
			}
			return authoritystore.ShardWriteAuthority{}, false, ErrAuthorityReconcile
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	req := ClaimRequest{TransitionID: "opaque-stale-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-b", NewHolderBootID: "boot-b", Endpoint: "player-b:9000", RequestedTTL: time.Second}
	claimed, err := manager.Claim(context.Background(), req)
	if !errors.Is(err, ErrSnapshotPublish) {
		t.Fatalf("initial publish err=%v result=%+v", err, claimed)
	}
	_, err = inner.TransferShardAuthority(context.Background(), authoritystore.TransferShardAuthorityRequest{
		Caller:       authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "external-manager"},
		TransitionID: "external-move", AuthorityLayoutVersion: "auth-v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-c", NewHolderBootID: "boot-c", RequestedTTLms: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	before := len(publisher.publishes)
	replayed, err := manager.Claim(context.Background(), req)
	if !errors.Is(err, ErrAuthorityReconcile) || !reflect.DeepEqual(replayed, claimed) || len(publisher.publishes) != before {
		t.Fatalf("opaque stale replay was not fail-closed: replay=%+v err=%v publishes=%d", replayed, err, len(publisher.publishes))
	}
}

func TestRouteAssignmentManagerDomainsHaveIndependentGenerations(t *testing.T) {
	var snapshots []actorrouting.RouteSnapshot
	manager, _ := newManagerForTest(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		snapshots = append(snapshots, actorrouting.CloneRouteSnapshot(snapshot))
		return nil
	}))
	player, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "same-id-player", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-same", NewHolderBootID: "player-boot", Endpoint: "player:9000", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	farm, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "same-id-farm", Domain: actorrouting.DomainFarm, VirtualShard: 0, NewHolderID: "farm-same", NewHolderBootID: "farm-boot", Endpoint: "farm:9001", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if player.Authority.Domain != actorrouting.DomainPlayer || farm.Authority.Domain != actorrouting.DomainFarm || player.Authority.WriterEpoch != 1 || farm.Authority.WriterEpoch != 1 || player.Authority.AssignmentRevision != 1 || farm.Authority.AssignmentRevision != 1 {
		t.Fatalf("domain generations shared: player=%+v farm=%+v", player.Authority, farm.Authority)
	}
	if len(snapshots) != 2 || snapshots[1].Player.Routes[0].ExpectedWriterEpoch != 1 || snapshots[1].Farm.Routes[0].ExpectedWriterEpoch != 1 {
		t.Fatalf("domain snapshot tuples=%+v", snapshots)
	}
}

type failOncePublisher struct {
	mu        sync.Mutex
	fail      bool
	publishes []actorrouting.RouteSnapshot
}

type alwaysFailPublisher struct {
	mu    sync.Mutex
	calls int
}

func (p *alwaysFailPublisher) Publish(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	return errors.New("publisher permanently unavailable")
}

func (p *alwaysFailPublisher) PublishAuthoritySnapshot(ctx context.Context, token routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	return p.Publish(ctx, token, snapshot)
}

func (p *alwaysFailPublisher) PublishAuthorityWaveSnapshot(ctx context.Context, token routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	return p.Publish(ctx, token, snapshot)
}

func (p *failOncePublisher) Publish(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail {
		p.fail = false
		return errors.New("publisher unavailable")
	}
	p.publishes = append(p.publishes, actorrouting.CloneRouteSnapshot(snapshot))
	return nil
}

func (p *failOncePublisher) PublishAuthoritySnapshot(ctx context.Context, token routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	return p.Publish(ctx, token, snapshot)
}

func (p *failOncePublisher) PublishAuthorityWaveSnapshot(ctx context.Context, token routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
	return p.Publish(ctx, token, snapshot)
}

func TestRouteAssignmentManagerRetriesOnlySnapshotAfterPublishFailure(t *testing.T) {
	publisher := &failOncePublisher{fail: true}
	manager, store := newManagerForTest(t, publisher)
	req := ClaimRequest{TransitionID: "claim-publish-retry", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-b", NewHolderBootID: "boot-b", Endpoint: "player-b:9000", RequestedTTL: time.Second}
	first, err := manager.Claim(context.Background(), req)
	if !errors.Is(err, ErrSnapshotPublish) || first.Status != authoritystore.ShardTransitionApplied || store.claim != 1 {
		t.Fatalf("first publish failure result=%+v err=%v calls=%d", first, err, store.claim)
	}
	if manager.CurrentSnapshot().GlobalRevision != 1 {
		t.Fatal("local route advanced when publisher failed")
	}
	retried, err := manager.Claim(context.Background(), req)
	if err != nil || !reflect.DeepEqual(first, retried) || store.claim != 1 || len(publisher.publishes) != 1 {
		t.Fatalf("retry=%+v err=%v calls=%d publishes=%d", retried, err, store.claim, len(publisher.publishes))
	}
	if manager.CurrentSnapshot().GlobalRevision != 2 {
		t.Fatalf("retry did not install snapshot: %+v", manager.CurrentSnapshot())
	}
}

func TestRouteAssignmentManagerUnassignPublishesInactiveAndReplays(t *testing.T) {
	publisher := &failOncePublisher{}
	manager, store := newManagerForTest(t, publisher)
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "unassign-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-b", NewHolderBootID: "player-b-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := UnassignRequest{TransitionID: "unassign-active", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority)}
	setPublisherFailure(publisher, true)
	first, err := manager.Unassign(context.Background(), req)
	if first.Status != authoritystore.ShardTransitionApplied || !errors.Is(err, ErrSnapshotPublish) || store.unassign != 1 {
		t.Fatalf("unassign commit/publish=%+v err=%v calls=%d", first, err, store.unassign)
	}
	if got := manager.CurrentSnapshot().Player.Routes[0]; got.State != actorrouting.ShardStateActive {
		t.Fatalf("failed unassign changed local route: %+v", got)
	}
	second, err := manager.Unassign(context.Background(), req)
	if err != nil || !reflect.DeepEqual(first, second) || store.unassign != 1 || len(publisher.publishes) != 2 {
		t.Fatalf("unassign replay=%+v err=%v calls=%d publishes=%d", second, err, store.unassign, len(publisher.publishes))
	}
	route := manager.CurrentSnapshot().Player.Routes[0]
	if route.State != actorrouting.ShardStateAssigning || route.LeaseUntilMs != 0 || route.ShardRevision != first.Authority.AssignmentRevision || route.ExpectedWriterEpoch != first.Authority.WriterEpoch || route.ExpectedLeaseEpoch != first.Authority.LeaseEpoch {
		t.Fatalf("published inactive route does not match unassign authority: route=%+v authority=%+v", route, first.Authority)
	}
	current, ok := store.inner.(interface {
		InspectShardWriteAuthority(actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool)
	}).InspectShardWriteAuthority(actorrouting.DomainPlayer, 0)
	if !ok || current.State != authoritystore.ShardAuthorityUnassigned || current.HolderID != "" || current.LeaseUntilMs != 0 {
		t.Fatalf("store unassign state=%+v found=%v", current, ok)
	}
}

func TestRouteAssignmentManagerSerializesDelayedRenewBeforeTransfer(t *testing.T) {
	manager, store := newManagerForTest(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}))
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "renew-race-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "renew-race-boot", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	store.renewStarted = started
	store.renewRelease = release
	renewDone := make(chan error, 1)
	go func() {
		_, renewErr := manager.Renew(context.Background(), RenewRequest{
			Caller: authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: "player-a", BootID: "renew-race-boot"},
			Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority), RequestedTTL: time.Second,
		})
		renewDone <- renewErr
	}()
	<-started
	transferDone := make(chan error, 1)
	go func() {
		_, transferErr := manager.Transfer(context.Background(), TransferRequest{
			TransitionID: "renew-race-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
			ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "renew-race-b-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
			Mode: TransferModeCrashTakeover, TakeoverProof: takeoverProofFor(managerFence(claimed.Authority)),
		})
		transferDone <- transferErr
	}()
	select {
	case err := <-transferDone:
		t.Fatalf("transfer bypassed in-flight renew: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-renewDone; err != nil {
		t.Fatal(err)
	}
	if err := <-transferDone; err != nil {
		t.Fatal(err)
	}
	current := manager.CurrentSnapshot()
	route := current.Player.Routes[0]
	holder := current.Holders[route.HolderIndex]
	if holder.InstanceID != "player-b" || route.ShardRevision != 2 || route.ExpectedWriterEpoch != 2 || route.ExpectedLeaseEpoch != 2 {
		t.Fatalf("delayed renew regressed transfer snapshot: holder=%+v route=%+v", holder, route)
	}
	if cached := manager.committed[authorityKey{domain: actorrouting.DomainPlayer, shard: 0}]; cached.HolderID != "player-b" || cached.AssignmentRevision != 2 {
		t.Fatalf("delayed renew regressed committed cache: %+v", cached)
	}
}

func TestRouteAssignmentManagerRenewAfterRestartRejectsUnprovenDeadlineRegression(t *testing.T) {
	now := func() time.Time { return time.UnixMilli(1_900_000_000_000) }
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), Now: now})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "renew-restart-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "restart-holder", NewHolderBootID: "restart-boot", Endpoint: "restart:9000", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	regressed := claimed
	regressed.Authority.LeaseUntilMs--
	regressed.AcceptedTTLms = 1
	storeWithoutReader := deadlineRegressionStore{inner: store.inner, result: regressed}
	publishes := 0
	restarted, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: storeWithoutReader, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			publishes++
			return nil
		}),
		InitialSnapshot: manager.CurrentSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-restarted", RouteManagerBootID: "restart-boot", Now: now,
		TombstoneStore: NewMemoryTransitionTombstoneStore(16),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := restarted.Renew(context.Background(), RenewRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: "restart-holder", BootID: "restart-boot"},
		Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority), RequestedTTL: time.Second,
	})
	if !errors.Is(err, ErrAuthorityReconcile) || result.Status != authoritystore.ShardTransitionApplied || publishes != 0 {
		t.Fatalf("restart accepted unproven deadline regression: result=%+v err=%v publishes=%d", result, err, publishes)
	}
}

type recordingDrainController struct {
	steps           []string
	coldAuthorities []authoritystore.ShardWriteAuthority
	coldErr         error
	prepared        *authoritystore.ShardWriteAuthority
	preparedErr     error
	preparedHook    func()
	coldHook        func(context.Context, authoritystore.ShardWriteAuthority) error
}

func acceptPendingProjectionTarget(context.Context, TransitionRecovery, uint64) error { return nil }

func (c *recordingDrainController) StopNewActivationAndTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	c.steps = append(c.steps, "stop")
	return nil
}
func (c *recordingDrainController) WaitCurrentTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	c.steps = append(c.steps, "wait")
	return nil
}
func (c *recordingDrainController) ConfirmFactsCommitted(context.Context, actorrouting.RouteDomain, uint32) error {
	c.steps = append(c.steps, "facts")
	return nil
}
func (c *recordingDrainController) ColdLoadNewHolder(ctx context.Context, _ actorrouting.RouteDomain, _ uint32, authority authoritystore.ShardWriteAuthority) error {
	if authority.HolderID != "player-b" {
		return errors.New("cold load saw wrong holder")
	}
	c.steps = append(c.steps, "cold-load")
	c.coldAuthorities = append(c.coldAuthorities, authority)
	if c.coldHook != nil {
		if err := c.coldHook(ctx, authority); err != nil {
			return err
		}
	}
	return c.coldErr
}

func (c *recordingDrainController) ColdLoadPrepared(_ context.Context, domain actorrouting.RouteDomain, shard uint32, value authoritystore.ShardWriteAuthority) (bool, error) {
	if c.preparedHook != nil {
		c.preparedHook()
	}
	if c.preparedErr != nil {
		return false, c.preparedErr
	}
	if c.prepared == nil {
		return false, nil
	}
	if c.prepared.Domain != domain || c.prepared.VirtualShard != shard || !sameAuthorityGeneration(*c.prepared, value) || c.prepared.LeaseUntilMs < value.LeaseUntilMs {
		return false, errors.New("prepared generation mismatch")
	}
	return true, nil
}

type blockingDrainController struct {
	mu          sync.Mutex
	steps       []string
	coldStarted chan struct{}
	coldRelease chan struct{}
	coldOnce    sync.Once
}

func (c *blockingDrainController) add(step string) {
	c.mu.Lock()
	c.steps = append(c.steps, step)
	c.mu.Unlock()
}

func (c *blockingDrainController) StopNewActivationAndTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	c.add("stop")
	return nil
}

func (c *blockingDrainController) WaitCurrentTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	c.add("wait")
	return nil
}

func (c *blockingDrainController) ConfirmFactsCommitted(context.Context, actorrouting.RouteDomain, uint32) error {
	c.add("facts")
	return nil
}

func (c *blockingDrainController) ColdLoadNewHolder(ctx context.Context, _ actorrouting.RouteDomain, _ uint32, authority authoritystore.ShardWriteAuthority) error {
	if authority.HolderID != "player-b" {
		return errors.New("cold load saw wrong holder")
	}
	c.add("cold-load")
	c.coldOnce.Do(func() { close(c.coldStarted) })
	select {
	case <-c.coldRelease:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *blockingDrainController) Steps() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.steps...)
}

func TestRouteAssignmentManagerDrainReconcilesPrepareRenewedDeadlineBeforeActive(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}), ManagerOptions{Now: func() time.Time { return now }})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "drain-prepare-renew-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "prepare-renew-a", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	controller := &recordingDrainController{}
	controller.coldHook = func(ctx context.Context, authority authoritystore.ShardWriteAuthority) error {
		now = now.Add(100 * time.Millisecond)
		renewed, renewErr := store.RenewShardAuthority(ctx, authoritystore.RenewShardAuthorityRequest{
			Caller:                 authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: authority.HolderID, BootID: authority.HolderBootID},
			AuthorityLayoutVersion: authority.AuthorityLayoutVersion, Domain: authority.Domain, VirtualShard: authority.VirtualShard,
			ExpectedFence: managerFence(authority), RequestedTTLms: time.Second.Milliseconds(),
		})
		if renewErr != nil {
			return renewErr
		}
		if renewed.Authority.LeaseUntilMs <= authority.LeaseUntilMs {
			return errors.New("prepare renew did not extend deadline")
		}
		return nil
	}
	result, err := manager.Drain(context.Background(), TransferRequest{
		TransitionID: "drain-prepare-renew-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "prepare-renew-b",
		Endpoint: "player-b:9000", RequestedTTL: time.Second,
	}, controller)
	if err != nil {
		t.Fatal(err)
	}
	if len(controller.coldAuthorities) != 1 || result.Authority.LeaseUntilMs <= controller.coldAuthorities[0].LeaseUntilMs {
		t.Fatalf("Drain did not return reconciled Prepare deadline: cold=%+v result=%+v", controller.coldAuthorities, result.Authority)
	}
	route := manager.CurrentSnapshot().Player.Routes[0]
	if route.State != actorrouting.ShardStateActive || route.LeaseUntilMs != result.Authority.LeaseUntilMs {
		t.Fatalf("ACTIVE snapshot deadline=%d result=%d state=%s", route.LeaseUntilMs, result.Authority.LeaseUntilMs, route.State)
	}
}

func TestRouteAssignmentManagerDrainSingleFlightAndReservation(t *testing.T) {
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), MemoLimit: 1, MemoRetention: time.Millisecond})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "drain-flight-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-a", NewHolderBootID: "drain-flight-a", Endpoint: "player-a:9000", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	controller := &blockingDrainController{coldStarted: make(chan struct{}), coldRelease: make(chan struct{})}
	req := TransferRequest{TransitionID: "drain-flight-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "drain-flight-b", Endpoint: "player-b:9000", RequestedTTL: time.Second}
	firstDone := make(chan struct{})
	var first authoritystore.ShardAuthorityTransitionResult
	var firstErr error
	go func() {
		first, firstErr = manager.Drain(context.Background(), req, controller)
		close(firstDone)
	}()
	select {
	case <-controller.coldStarted:
	case <-time.After(time.Second):
		t.Fatal("Drain did not reach the bounded cold-load checkpoint")
	}

	// The fixed reservation is held even after MemoRetention. A concurrent
	// transition cannot commit after discovering that no memo/tombstone room
	// remains, and a conflicting Drain cannot start holder side effects.
	if _, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "drain-flight-capacity", Domain: actorrouting.DomainFarm, VirtualShard: 0, NewHolderID: "farm-b", NewHolderBootID: "drain-flight-farm", Endpoint: "farm-b:9001", RequestedTTL: time.Second}); !errors.Is(err, ErrTransitionTombstoneCap) || store.claim != 1 {
		t.Fatalf("in-flight reservation did not fail closed before Authority: err=%v claims=%d", err, store.claim)
	}
	conflict := req
	conflict.TransitionID = "drain-flight-conflict"
	conflict.NewHolderID = "player-c"
	conflict.NewHolderBootID = "drain-flight-c"
	if _, err := manager.Drain(context.Background(), conflict, &recordingDrainController{}); !errors.Is(err, ErrDrainInFlight) {
		t.Fatalf("conflicting Drain was not single-flight rejected: %v", err)
	}

	secondController := &recordingDrainController{}
	secondDone := make(chan struct{})
	var second authoritystore.ShardAuthorityTransitionResult
	var secondErr error
	go func() {
		second, secondErr = manager.Drain(context.Background(), req, secondController)
		close(secondDone)
	}()
	select {
	case <-secondDone:
		t.Fatal("same Drain replay returned before the owner completed")
	case <-time.After(20 * time.Millisecond):
	}
	// Force the retention clock past the memo lifetime while cold load is
	// blocked; the pinned placeholder/result must survive this interval.
	time.Sleep(5 * time.Millisecond)
	close(controller.coldRelease)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("owner Drain did not complete")
	}
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("same Drain waiter did not complete")
	}
	if firstErr != nil || secondErr != nil || !reflect.DeepEqual(first, second) || store.trans != 1 {
		t.Fatalf("single-flight results/store calls first=%+v/%v second=%+v/%v transfers=%d", first, firstErr, second, secondErr, store.trans)
	}
	if got := controller.Steps(); !reflect.DeepEqual(got, []string{"stop", "wait", "facts", "cold-load"}) || len(secondController.steps) != 0 {
		t.Fatalf("Drain side effects duplicated or lost: owner=%v waiter=%v", got, secondController.steps)
	}
	third, err := manager.Drain(context.Background(), req, &recordingDrainController{})
	if err != nil || !reflect.DeepEqual(third, first) || store.trans != 1 {
		t.Fatalf("completed Drain replay was not terminal: result=%+v err=%v transfers=%d", third, err, store.trans)
	}
}

func TestRouteAssignmentManagerDrainRetentionDoesNotPrunePinnedMemo(t *testing.T) {
	manager, _ := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), MemoLimit: 2, MemoRetention: time.Millisecond})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "drain-pinned-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-a", NewHolderBootID: "drain-pinned-a", Endpoint: "player-a:9000", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	controller := &blockingDrainController{coldStarted: make(chan struct{}), coldRelease: make(chan struct{})}
	req := TransferRequest{TransitionID: "drain-pinned-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "drain-pinned-b", Endpoint: "player-b:9000", RequestedTTL: time.Second}
	done := make(chan error, 1)
	go func() {
		_, drainErr := manager.Drain(context.Background(), req, controller)
		done <- drainErr
	}()
	select {
	case <-controller.coldStarted:
	case <-time.After(time.Second):
		t.Fatal("Drain did not reach cold load")
	}
	time.Sleep(5 * time.Millisecond)
	if _, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "drain-pinned-farm", Domain: actorrouting.DomainFarm, VirtualShard: 0, NewHolderID: "farm-b", NewHolderBootID: "drain-pinned-farm-boot", Endpoint: "farm-b:9001", RequestedTTL: time.Second}); err != nil {
		t.Fatalf("unrelated domain transition failed while pinned Drain was cold-loading: %v", err)
	}
	close(controller.coldRelease)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if memo := manager.transitionMemoLocked(req.TransitionID); memo == nil || memo.result.Authority.HolderID != "player-b" {
		t.Fatalf("pinned Drain memo was pruned during cold load: %+v", memo)
	}
}

func TestRouteAssignmentManagerDrainPublishesAssigningThenActive(t *testing.T) {
	var published []actorrouting.RouteSnapshot
	manager, store := newManagerForTest(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
		return nil
	}))
	claimed, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "drain-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-a", NewHolderBootID: "player-a-boot", Endpoint: "player-a:9000", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	controller := &recordingDrainController{}
	result, err := manager.Drain(context.Background(), TransferRequest{
		TransitionID: "drain-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: authoritystore.ShardWriteFence{ExpectedHolderID: claimed.Authority.HolderID, ExpectedHolderBootID: claimed.Authority.HolderBootID, ExpectedAssignmentRevision: claimed.Authority.AssignmentRevision, ExpectedWriterEpoch: claimed.Authority.WriterEpoch, ExpectedLeaseEpoch: claimed.Authority.LeaseEpoch},
		NewHolderID:   "player-b", NewHolderBootID: "player-b-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
	}, controller)
	if err != nil {
		t.Fatal(err)
	}
	if result.Authority.HolderID != "player-b" || manager.DrainState(actorrouting.DomainPlayer, 0) != DrainStageActiveNew || len(controller.steps) != 4 || !reflect.DeepEqual(controller.steps, []string{"stop", "wait", "facts", "cold-load"}) {
		t.Fatalf("drain result=%+v state=%d steps=%v", result, manager.DrainState(actorrouting.DomainPlayer, 0), controller.steps)
	}
	if store.trans != 1 || len(published) != 3 || published[1].Player.Routes[0].State != actorrouting.ShardStateAssigning || published[2].Player.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("drain authority/publish order trans=%d publishes=%+v", store.trans, published)
	}
}

func TestRouteAssignmentManagerCoalescesConcurrentDrainSnapshots(t *testing.T) {
	var mu sync.Mutex
	var published []actorrouting.RouteSnapshot
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		mu.Lock()
		published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
		mu.Unlock()
		return nil
	}), ManagerOptions{SnapshotCoalesceWindow: 50 * time.Millisecond})
	playerClaimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "coalesce-player-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "player-a-boot", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	farmClaimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "coalesce-farm-claim", Domain: actorrouting.DomainFarm, VirtualShard: 0,
		NewHolderID: "farm-a", NewHolderBootID: "farm-a-boot", Endpoint: "farm-a:9001", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	var facts sync.WaitGroup
	facts.Add(2)
	var cold sync.WaitGroup
	cold.Add(2)
	type result struct {
		authority authoritystore.ShardAuthorityTransitionResult
		err       error
	}
	playerDone := make(chan result, 1)
	farmDone := make(chan result, 1)
	go func() {
		got, drainErr := manager.Drain(context.Background(), TransferRequest{
			TransitionID: "coalesce-player-drain", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
			ExpectedFence: managerFence(playerClaimed.Authority),
			NewHolderID:   "player-b", NewHolderBootID: "player-b-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
		}, &coalescingDrainController{holderID: "player-b", facts: &facts, cold: &cold})
		playerDone <- result{got, drainErr}
	}()
	go func() {
		got, drainErr := manager.Drain(context.Background(), TransferRequest{
			TransitionID: "coalesce-farm-drain", Domain: actorrouting.DomainFarm, VirtualShard: 0,
			ExpectedFence: managerFence(farmClaimed.Authority),
			NewHolderID:   "farm-b", NewHolderBootID: "farm-b-boot", Endpoint: "farm-b:9001", RequestedTTL: time.Second,
		}, &coalescingDrainController{holderID: "farm-b", facts: &facts, cold: &cold})
		farmDone <- result{got, drainErr}
	}()
	playerResult := <-playerDone
	farmResult := <-farmDone
	if playerResult.err != nil || farmResult.err != nil {
		t.Fatalf("concurrent drain player=%v farm=%v", playerResult.err, farmResult.err)
	}
	if playerResult.authority.Authority.HolderID != "player-b" || farmResult.authority.Authority.HolderID != "farm-b" {
		t.Fatalf("drain holders player=%+v farm=%+v", playerResult.authority.Authority, farmResult.authority.Authority)
	}
	if store.trans != 2 {
		t.Fatalf("authority transfers=%d", store.trans)
	}
	mu.Lock()
	got := published
	mu.Unlock()
	if len(got) != 4 {
		t.Fatalf("expected 2 claims + 1 coalesced ASSIGNING + 1 coalesced ACTIVE, got %d publishes=%+v", len(got), got)
	}
	assigning := got[2]
	active := got[3]
	if assigning.Player.Routes[0].State != actorrouting.ShardStateAssigning || assigning.Farm.Routes[0].State != actorrouting.ShardStateAssigning {
		t.Fatalf("coalesced ASSIGNING=%+v", assigning)
	}
	if active.Player.Routes[0].State != actorrouting.ShardStateActive || active.Farm.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("coalesced ACTIVE=%+v", active)
	}
	if assigning.GlobalRevision != got[1].GlobalRevision+1 || active.GlobalRevision != assigning.GlobalRevision+1 {
		t.Fatalf("revisions claim=%d assigning=%d active=%d", got[1].GlobalRevision, assigning.GlobalRevision, active.GlobalRevision)
	}
}

func TestRouteAssignmentManagerCoalescedDrainFlushesWhenBatchIsFull(t *testing.T) {
	var mu sync.Mutex
	var published []actorrouting.RouteSnapshot
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		mu.Lock()
		published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
		mu.Unlock()
		return nil
	}), ManagerOptions{SnapshotCoalesceWindow: time.Second, SnapshotCoalesceBatch: 2})
	playerClaimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "batch-player-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "player-a-boot", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	farmClaimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "batch-farm-claim", Domain: actorrouting.DomainFarm, VirtualShard: 0,
		NewHolderID: "farm-a", NewHolderBootID: "farm-a-boot", Endpoint: "farm-a:9001", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	var facts sync.WaitGroup
	facts.Add(2)
	var cold sync.WaitGroup
	cold.Add(2)
	type result struct {
		err error
	}
	started := time.Now()
	playerDone := make(chan result, 1)
	farmDone := make(chan result, 1)
	go func() {
		_, drainErr := manager.Drain(context.Background(), TransferRequest{
			TransitionID: "batch-player-drain", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
			ExpectedFence: managerFence(playerClaimed.Authority),
			NewHolderID:   "player-b", NewHolderBootID: "player-b-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
		}, &coalescingDrainController{holderID: "player-b", facts: &facts, cold: &cold})
		playerDone <- result{drainErr}
	}()
	go func() {
		_, drainErr := manager.Drain(context.Background(), TransferRequest{
			TransitionID: "batch-farm-drain", Domain: actorrouting.DomainFarm, VirtualShard: 0,
			ExpectedFence: managerFence(farmClaimed.Authority),
			NewHolderID:   "farm-b", NewHolderBootID: "farm-b-boot", Endpoint: "farm-b:9001", RequestedTTL: time.Second,
		}, &coalescingDrainController{holderID: "farm-b", facts: &facts, cold: &cold})
		farmDone <- result{drainErr}
	}()
	playerResult := <-playerDone
	farmResult := <-farmDone
	elapsed := time.Since(started)
	if playerResult.err != nil || farmResult.err != nil {
		t.Fatalf("concurrent drain player=%v farm=%v", playerResult.err, farmResult.err)
	}
	if elapsed >= 800*time.Millisecond {
		t.Fatalf("full batch waited out the 1s window: elapsed=%s transfers=%d", elapsed, store.trans)
	}
	mu.Lock()
	got := published
	mu.Unlock()
	if len(got) != 4 {
		t.Fatalf("expected 2 claims + 1 coalesced ASSIGNING + 1 coalesced ACTIVE, got %d", len(got))
	}
}

func TestRouteAssignmentManagerCoalescedDrainGathersLateSibling(t *testing.T) {
	var mu sync.Mutex
	var published []actorrouting.RouteSnapshot
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		mu.Lock()
		published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
		mu.Unlock()
		return nil
	}), ManagerOptions{
		SnapshotCoalesceWindow:  150 * time.Millisecond,
		SnapshotCoalesceBatch:   8,
		SnapshotCoalesceMaxWait: time.Second,
	})
	playerClaimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "late-player-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "player-a-boot", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	farmClaimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "late-farm-claim", Domain: actorrouting.DomainFarm, VirtualShard: 0,
		NewHolderID: "farm-a", NewHolderBootID: "farm-a-boot", Endpoint: "farm-a:9001", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	var cold sync.WaitGroup
	cold.Add(2)
	done := make(chan error, 2)
	go func() {
		_, drainErr := manager.Drain(context.Background(), TransferRequest{
			TransitionID: "late-player-drain", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
			ExpectedFence: managerFence(playerClaimed.Authority),
			NewHolderID:   "player-b", NewHolderBootID: "player-b-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
		}, &coalescingDrainController{holderID: "player-b", cold: &cold})
		done <- drainErr
	}()
	go func() {
		_, drainErr := manager.Drain(context.Background(), TransferRequest{
			TransitionID: "late-farm-drain", Domain: actorrouting.DomainFarm, VirtualShard: 0,
			ExpectedFence: managerFence(farmClaimed.Authority),
			NewHolderID:   "farm-b", NewHolderBootID: "farm-b-boot", Endpoint: "farm-b:9001", RequestedTTL: time.Second,
		}, &coalescingDrainController{holderID: "farm-b", cold: &cold, factsDelay: 80 * time.Millisecond})
		done <- drainErr
	}()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if store.trans != 2 {
		t.Fatalf("authority transfers=%d", store.trans)
	}
	mu.Lock()
	got := published
	mu.Unlock()
	if len(got) != 4 {
		t.Fatalf("expected 2 claims + 1 coalesced ASSIGNING + 1 coalesced ACTIVE, got %d publishes=%+v", len(got), got)
	}
	assigning := got[2]
	if assigning.Player.Routes[0].State != actorrouting.ShardStateAssigning || assigning.Farm.Routes[0].State != actorrouting.ShardStateAssigning {
		t.Fatalf("late sibling ASSIGNING=%+v", assigning)
	}
}

func TestRouteAssignmentManagerCoalescedDrainFlushesAtMaxWait(t *testing.T) {
	manager, _ := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}), ManagerOptions{
		SnapshotCoalesceWindow:  time.Second,
		SnapshotCoalesceBatch:   8,
		SnapshotCoalesceMaxWait: 80 * time.Millisecond,
	})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "maxwait-player-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "player-a-boot", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err := manager.Drain(context.Background(), TransferRequest{
		TransitionID: "maxwait-player-drain", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(claimed.Authority),
		NewHolderID:   "player-b", NewHolderBootID: "player-b-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
	}, &coalescingDrainController{holderID: "player-b"}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed >= 800*time.Millisecond {
		t.Fatalf("max wait did not bound the 1s quiet period: elapsed=%s", elapsed)
	}
}

func TestRouteAssignmentManagerCoalescedDrainKeepsStaggeredWaveTogether(t *testing.T) {
	var mu sync.Mutex
	var published []actorrouting.RouteSnapshot
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		mu.Lock()
		published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
		mu.Unlock()
		return nil
	}), ManagerOptions{
		SnapshotCoalesceWindow: 700 * time.Millisecond,
		SnapshotCoalesceBatch:  8,
	})
	playerClaimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "stagger-player-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "player-a-boot", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	farmClaimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "stagger-farm-claim", Domain: actorrouting.DomainFarm, VirtualShard: 0,
		NewHolderID: "farm-a", NewHolderBootID: "farm-a-boot", Endpoint: "farm-a:9001", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	var cold sync.WaitGroup
	cold.Add(2)
	done := make(chan error, 2)
	go func() {
		_, drainErr := manager.Drain(context.Background(), TransferRequest{
			TransitionID: "stagger-player-drain", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
			ExpectedFence: managerFence(playerClaimed.Authority),
			NewHolderID:   "player-b", NewHolderBootID: "player-b-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
		}, &coalescingDrainController{holderID: "player-b", cold: &cold})
		done <- drainErr
	}()
	time.Sleep(550 * time.Millisecond)
	go func() {
		_, drainErr := manager.Drain(context.Background(), TransferRequest{
			TransitionID: "stagger-farm-drain", Domain: actorrouting.DomainFarm, VirtualShard: 0,
			ExpectedFence: managerFence(farmClaimed.Authority),
			NewHolderID:   "farm-b", NewHolderBootID: "farm-b-boot", Endpoint: "farm-b:9001", RequestedTTL: time.Second,
		}, &coalescingDrainController{holderID: "farm-b", cold: &cold})
		done <- drainErr
	}()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if store.trans != 2 {
		t.Fatalf("authority transfers=%d", store.trans)
	}
	mu.Lock()
	got := published
	mu.Unlock()
	if len(got) != 4 {
		t.Fatalf("first-waiter maxWait split the wave: publishes=%d snapshots=%+v", len(got), got)
	}
	assigning := got[2]
	if assigning.Player.Routes[0].State != actorrouting.ShardStateAssigning || assigning.Farm.Routes[0].State != actorrouting.ShardStateAssigning {
		t.Fatalf("staggered ASSIGNING=%+v", assigning)
	}
}

func TestRouteAssignmentManagerDrainAuthorityCommitsOverlap(t *testing.T) {
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}), ManagerOptions{SnapshotCoalesceWindow: 50 * time.Millisecond})
	store.transferDelay = 40 * time.Millisecond
	playerClaimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "overlap-player-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "player-a-boot", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	farmClaimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "overlap-farm-claim", Domain: actorrouting.DomainFarm, VirtualShard: 0,
		NewHolderID: "farm-a", NewHolderBootID: "farm-a-boot", Endpoint: "farm-a:9001", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	var facts sync.WaitGroup
	facts.Add(2)
	var cold sync.WaitGroup
	cold.Add(2)
	done := make(chan error, 2)
	go func() {
		_, drainErr := manager.Drain(context.Background(), TransferRequest{
			TransitionID: "overlap-player-drain", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
			ExpectedFence: managerFence(playerClaimed.Authority),
			NewHolderID:   "player-b", NewHolderBootID: "player-b-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
		}, &coalescingDrainController{holderID: "player-b", facts: &facts, cold: &cold})
		done <- drainErr
	}()
	go func() {
		_, drainErr := manager.Drain(context.Background(), TransferRequest{
			TransitionID: "overlap-farm-drain", Domain: actorrouting.DomainFarm, VirtualShard: 0,
			ExpectedFence: managerFence(farmClaimed.Authority),
			NewHolderID:   "farm-b", NewHolderBootID: "farm-b-boot", Endpoint: "farm-b:9001", RequestedTTL: time.Second,
		}, &coalescingDrainController{holderID: "farm-b", facts: &facts, cold: &cold})
		done <- drainErr
	}()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if store.transferPeak.Load() < 2 {
		t.Fatalf("authority transfers stayed serial under Manager.mu: peak=%d", store.transferPeak.Load())
	}
}

type coalescingDrainController struct {
	holderID   string
	facts      *sync.WaitGroup
	cold       *sync.WaitGroup
	factsDelay time.Duration
}

func (c *coalescingDrainController) StopNewActivationAndTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	return nil
}
func (c *coalescingDrainController) WaitCurrentTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	return nil
}
func (c *coalescingDrainController) ConfirmFactsCommitted(context.Context, actorrouting.RouteDomain, uint32) error {
	if c.facts != nil {
		c.facts.Done()
		c.facts.Wait()
	}
	if c.factsDelay > 0 {
		time.Sleep(c.factsDelay)
	}
	return nil
}
func (c *coalescingDrainController) ColdLoadNewHolder(_ context.Context, _ actorrouting.RouteDomain, _ uint32, authority authoritystore.ShardWriteAuthority) error {
	if authority.HolderID != c.holderID {
		return fmt.Errorf("cold load saw holder %q want %q", authority.HolderID, c.holderID)
	}
	if c.cold != nil {
		c.cold.Done()
		c.cold.Wait()
	}
	return nil
}
func (c *coalescingDrainController) ColdLoadPrepared(context.Context, actorrouting.RouteDomain, uint32, authoritystore.ShardWriteAuthority) (bool, error) {
	return false, nil
}

func TestRouteAssignmentManagerValidatedDrainRevalidatesAfterAuthorityBeforeColdLoadAndActive(t *testing.T) {
	allowTarget := true
	var published []actorrouting.RouteSnapshot
	manager, store := newManagerForTest(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
		if len(published) > 1 && snapshot.Player.Routes[0].State == actorrouting.ShardStateAssigning {
			// Simulate the target becoming ineligible immediately after the
			// Authority-backed ASSIGNING snapshot is durably published.
			allowTarget = false
		}
		return nil
	}))
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "validated-drain-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "validated-drain-a", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := TransferRequest{
		TransitionID: "validated-drain-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "validated-drain-b",
		Endpoint: "player-b:9000", RequestedTTL: time.Second,
	}
	validator := func(_ context.Context, recovery TransitionRecovery, _ uint64) error {
		if recovery.TransitionID != req.TransitionID || recovery.NewHolderID != req.NewHolderID ||
			recovery.NewHolderBootID != req.NewHolderBootID || recovery.Endpoint != req.Endpoint || !recovery.InternalDrain {
			return errors.New("wrong validated drain identity")
		}
		if !allowTarget {
			return errors.New("target became ineligible")
		}
		return nil
	}
	firstController := &recordingDrainController{}
	if _, err := manager.DrainValidated(context.Background(), req, firstController, validator); !errors.Is(err, ErrProjectionRecovery) {
		t.Fatalf("post-Authority target drift did not fail closed: %v", err)
	}
	if store.trans != 1 || !reflect.DeepEqual(firstController.steps, []string{"stop", "wait", "facts"}) {
		t.Fatalf("unexpected first pass transfers=%d steps=%v", store.trans, firstController.steps)
	}
	if got := manager.CurrentSnapshot().Player.Routes[0].State; got != actorrouting.ShardStateAssigning {
		t.Fatalf("invalid target was advertised ACTIVE: state=%v", got)
	}

	allowTarget = true
	retryController := &recordingDrainController{}
	if _, err := manager.DrainValidated(context.Background(), req, retryController, validator); err != nil {
		t.Fatal(err)
	}
	if store.trans != 1 || !reflect.DeepEqual(retryController.steps, []string{"cold-load"}) {
		t.Fatalf("retry repeated Authority/source callbacks: transfers=%d steps=%v", store.trans, retryController.steps)
	}
	if got := manager.CurrentSnapshot().Player.Routes[0].State; got != actorrouting.ShardStateActive {
		t.Fatalf("eligible retry did not publish ACTIVE: state=%v", got)
	}
}

func TestRouteAssignmentManagerDrainWaitsForPostAssigningTargetSnapshotConvergence(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	afterCh := make(chan time.Time, 8)
	postAssignAttempts := 0
	wantAssignRevision := uint64(0)
	enteredWait := make(chan struct{}, 1)
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		if snapshot.Player.Routes[0].State == actorrouting.ShardStateAssigning {
			wantAssignRevision = snapshot.GlobalRevision
		}
		return nil
	}), ManagerOptions{
		Now:                      func() time.Time { return now },
		TargetConvergenceTimeout: time.Second,
		TargetConvergenceBackoff: 10 * time.Millisecond,
		After: func(d time.Duration) <-chan time.Time {
			if d <= 0 {
				ch := make(chan time.Time, 1)
				ch <- now
				return ch
			}
			select {
			case enteredWait <- struct{}{}:
			default:
			}
			return afterCh
		},
	})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "converge-drain-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "converge-a", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := TransferRequest{
		TransitionID: "converge-drain-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "converge-b",
		Endpoint: "player-b:9000", RequestedTTL: time.Second,
	}
	validator := func(_ context.Context, recovery TransitionRecovery, revision uint64) error {
		if recovery.TransitionID != req.TransitionID || recovery.NewHolderID != req.NewHolderID ||
			recovery.NewHolderBootID != req.NewHolderBootID || recovery.Endpoint != req.Endpoint || !recovery.InternalDrain {
			return errors.New("wrong converge drain identity")
		}
		if wantAssignRevision == 0 || revision != wantAssignRevision {
			return nil
		}
		if recovery.PublishedSnapshotRevision != wantAssignRevision {
			return fmt.Errorf("published shard revision=%d want=%d", recovery.PublishedSnapshotRevision, wantAssignRevision)
		}
		postAssignAttempts++
		// SnapshotRevision>=N is required; first post-publish attempts lag.
		if postAssignAttempts < 3 {
			return errors.New("target SnapshotRevision lagging published ASSIGNING")
		}
		return nil
	}
	controller := &recordingDrainController{}
	done := make(chan error, 1)
	go func() {
		_, drainErr := manager.DrainValidated(context.Background(), req, controller, validator)
		done <- drainErr
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-enteredWait:
		case err := <-done:
			t.Fatalf("drain finished before wait seam: %v", err)
		case <-time.After(time.Second):
			t.Fatal("barrier did not enter After seam")
		}
		afterCh <- now.Add(time.Duration(i+1) * 10 * time.Millisecond)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("converged drain failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("drain did not finish after target converged")
	}
	if store.trans != 1 {
		t.Fatalf("Authority mutated more than once: transfers=%d", store.trans)
	}
	if !reflect.DeepEqual(controller.steps, []string{"stop", "wait", "facts", "cold-load"}) {
		t.Fatalf("unexpected drain steps=%v", controller.steps)
	}
	if wantAssignRevision == 0 || postAssignAttempts < 3 {
		t.Fatalf("convergence barrier did not revalidate: assignRev=%d postAssign=%d", wantAssignRevision, postAssignAttempts)
	}
	manager.mu.Lock()
	frozenRevision := manager.transitions[req.TransitionID].publishedSnapshotRevision
	manager.mu.Unlock()
	if frozenRevision != wantAssignRevision {
		t.Fatalf("durable shard revision=%d want=%d", frozenRevision, wantAssignRevision)
	}
	if got := manager.CurrentSnapshot().Player.Routes[0].State; got != actorrouting.ShardStateActive {
		t.Fatalf("converged drain state=%v", got)
	}
}

func TestRouteAssignmentManagerDrainConvergenceTimeoutKeepsAssigningWithoutRepeatingAuthority(t *testing.T) {
	base := time.UnixMilli(1_900_000_000_000)
	var clockMu sync.Mutex
	now := base
	enteredWait := make(chan struct{}, 1)
	assignPublished := false
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		if snapshot.Player.Routes[0].State == actorrouting.ShardStateAssigning {
			assignPublished = true
		}
		return nil
	}), ManagerOptions{
		Now: func() time.Time {
			clockMu.Lock()
			defer clockMu.Unlock()
			return now
		},
		TargetConvergenceTimeout: 30 * time.Millisecond,
		TargetConvergenceBackoff: 10 * time.Millisecond,
		After: func(d time.Duration) <-chan time.Time {
			clockMu.Lock()
			now = now.Add(d + time.Millisecond)
			tick := now
			clockMu.Unlock()
			select {
			case enteredWait <- struct{}{}:
			default:
			}
			ch := make(chan time.Time, 1)
			ch <- tick
			return ch
		},
	})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "timeout-drain-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "timeout-a", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := TransferRequest{
		TransitionID: "timeout-drain-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "timeout-b",
		Endpoint: "player-b:9000", RequestedTTL: time.Second,
	}
	validator := func(_ context.Context, _ TransitionRecovery, _ uint64) error {
		// Pre-publish validates still see the prior GlobalRevision; lag only
		// after ASSIGNING publication (when exact N must be observed).
		if assignPublished {
			return errors.New("target SnapshotRevision never converged")
		}
		return nil
	}
	controller := &recordingDrainController{}
	_, err = manager.DrainValidated(context.Background(), req, controller, validator)
	if !errors.Is(err, ErrProjectionRecovery) {
		t.Fatalf("timeout drain err=%v", err)
	}
	if store.trans != 1 {
		t.Fatalf("timeout path mutated Authority: transfers=%d", store.trans)
	}
	if !reflect.DeepEqual(controller.steps, []string{"stop", "wait", "facts"}) {
		t.Fatalf("timeout path repeated holder callbacks: %v", controller.steps)
	}
	if got := manager.CurrentSnapshot().Player.Routes[0].State; got != actorrouting.ShardStateAssigning {
		t.Fatalf("timeout path left state=%v", got)
	}
	select {
	case <-enteredWait:
	default:
		t.Fatal("timeout path never entered After seam")
	}
}

func TestRouteAssignmentManagerDrainConvergenceCancelPreservesRecoverableAssigning(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	afterCh := make(chan time.Time)
	enteredWait := make(chan struct{})
	assignPublished := false
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		if snapshot.Player.Routes[0].State == actorrouting.ShardStateAssigning {
			assignPublished = true
		}
		return nil
	}), ManagerOptions{
		Now:                      func() time.Time { return now },
		TargetConvergenceTimeout: time.Second,
		TargetConvergenceBackoff: 50 * time.Millisecond,
		After: func(time.Duration) <-chan time.Time {
			select {
			case <-enteredWait:
			default:
				close(enteredWait)
			}
			return afterCh
		},
	})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "cancel-drain-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "cancel-a", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := TransferRequest{
		TransitionID: "cancel-drain-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "cancel-b",
		Endpoint: "player-b:9000", RequestedTTL: time.Second,
	}
	validator := func(_ context.Context, _ TransitionRecovery, _ uint64) error {
		if assignPublished {
			return errors.New("target SnapshotRevision lagging")
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	controller := &recordingDrainController{}
	done := make(chan error, 1)
	go func() {
		_, drainErr := manager.DrainValidated(ctx, req, controller, validator)
		done <- drainErr
	}()
	select {
	case <-enteredWait:
	case err := <-done:
		t.Fatalf("drain finished before wait: %v", err)
	case <-time.After(time.Second):
		t.Fatal("barrier did not enter After seam")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, ErrProjectionRecovery) || !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel drain err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled drain did not return")
	}
	if store.trans != 1 || !reflect.DeepEqual(controller.steps, []string{"stop", "wait", "facts"}) {
		t.Fatalf("cancel path transfers=%d steps=%v", store.trans, controller.steps)
	}
	if got := manager.CurrentSnapshot().Player.Routes[0].State; got != actorrouting.ShardStateAssigning {
		t.Fatalf("cancel path left state=%v", got)
	}
}

func TestRouteAssignmentManagerValidatedCrashTransferRevalidatesBeforePublication(t *testing.T) {
	var published []actorrouting.RouteSnapshot
	manager, store := newManagerForTest(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
		return nil
	}))
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "validated-crash-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "validated-crash-a", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := TransferRequest{
		TransitionID: "validated-crash-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "validated-crash-b",
		Endpoint: "player-b:9000", RequestedTTL: time.Second, Mode: TransferModeCrashTakeover,
		TakeoverProof: takeoverProofFor(managerFence(claimed.Authority)),
	}
	validatorCalls := 0
	validator := func(_ context.Context, recovery TransitionRecovery, _ uint64) error {
		validatorCalls++
		if recovery.TransitionID != req.TransitionID || recovery.InternalDrain || recovery.Mode != TransferModeCrashTakeover {
			return errors.New("wrong validated crash identity")
		}
		if validatorCalls == 2 {
			return errors.New("target changed after Authority")
		}
		return nil
	}
	first, err := manager.TransferValidated(context.Background(), req, validator)
	if !errors.Is(err, ErrProjectionRecovery) || first.Status != authoritystore.ShardTransitionApplied || store.trans != 1 || len(published) != 1 {
		t.Fatalf("post-Authority crash fence did not hold: result=%+v err=%v transfers=%d publishes=%d", first, err, store.trans, len(published))
	}
	replayed, err := manager.TransferValidated(context.Background(), req, validator)
	if err != nil || !reflect.DeepEqual(replayed, first) || store.trans != 1 || len(published) != 2 {
		t.Fatalf("validated replay mutated Authority or missed publication: result=%+v err=%v transfers=%d publishes=%d", replayed, err, store.trans, len(published))
	}
}

func managerFence(authority authoritystore.ShardWriteAuthority) authoritystore.ShardWriteFence {
	return authoritystore.ShardWriteFence{
		ExpectedHolderID: authority.HolderID, ExpectedHolderBootID: authority.HolderBootID,
		ExpectedAssignmentRevision: authority.AssignmentRevision,
		ExpectedWriterEpoch:        authority.WriterEpoch, ExpectedLeaseEpoch: authority.LeaseEpoch,
	}
}

func takeoverProofFor(fence authoritystore.ShardWriteFence) TransferTakeoverProof {
	return TransferTakeoverProof{ExpectedHolderBootID: fence.ExpectedHolderBootID, HolderUnavailable: true, Evidence: "test-crash-proof"}
}

func setPublisherFailure(publisher *failOncePublisher, fail bool) {
	publisher.mu.Lock()
	publisher.fail = fail
	publisher.mu.Unlock()
}

func TestRouteAssignmentManagerDoesNotReplayClaimOverNewTransfer(t *testing.T) {
	publisher := &failOncePublisher{fail: true}
	manager, store := newManagerForTest(t, publisher)
	claimRequest := ClaimRequest{TransitionID: "claim-stale-before-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-b", NewHolderBootID: "player-b-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second}
	claimed, err := manager.Claim(context.Background(), claimRequest)
	if !errors.Is(err, ErrSnapshotPublish) {
		t.Fatalf("claim publish failure err=%v", err)
	}
	transferred, err := manager.Transfer(context.Background(), TransferRequest{
		TransitionID: "transfer-after-claim-publish-failure", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-c", NewHolderBootID: "player-c-boot", Endpoint: "player-c:9000", RequestedTTL: time.Second,
		Mode: TransferModeCrashTakeover, TakeoverProof: takeoverProofFor(managerFence(claimed.Authority)),
	})
	if err != nil || transferred.Authority.HolderID != "player-c" {
		t.Fatalf("transfer after failed claim publish result=%+v err=%v", transferred, err)
	}
	before := len(publisher.publishes)
	replayed, err := manager.Claim(context.Background(), claimRequest)
	if !errors.Is(err, ErrStaleTransition) || !reflect.DeepEqual(replayed, claimed) || len(publisher.publishes) != before || store.claim != 1 || store.trans != 1 {
		t.Fatalf("stale claim replay published or changed authority: replay=%+v err=%v publishes=%d claims=%d transfers=%d", replayed, err, len(publisher.publishes), store.claim, store.trans)
	}
	current := manager.CurrentSnapshot()
	if got := current.Holders[current.Player.Routes[0].HolderIndex]; got.InstanceID != "player-c" || got.BootID != "player-c-boot" {
		t.Fatalf("stale replay replaced current holder: %+v", got)
	}
}

func TestRouteAssignmentManagerDoesNotReplayTransferOverNewTransfer(t *testing.T) {
	publisher := &failOncePublisher{}
	manager, _ := newManagerForTest(t, publisher)
	claimed, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "transfer-interleave-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-a", NewHolderBootID: "player-a-new-boot", Endpoint: "player-a:9000", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	setPublisherFailure(publisher, true)
	firstFence := managerFence(claimed.Authority)
	firstTransfer := TransferRequest{TransitionID: "transfer-interleave-first", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: firstFence, NewHolderID: "player-b", NewHolderBootID: "player-b-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second, Mode: TransferModeCrashTakeover, TakeoverProof: takeoverProofFor(firstFence)}
	transferred, err := manager.Transfer(context.Background(), firstTransfer)
	if !errors.Is(err, ErrSnapshotPublish) {
		t.Fatalf("first transfer publish failure result=%+v err=%v", transferred, err)
	}
	secondFence := managerFence(transferred.Authority)
	second, err := manager.Transfer(context.Background(), TransferRequest{TransitionID: "transfer-interleave-second", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: secondFence, NewHolderID: "player-c", NewHolderBootID: "player-c-boot", Endpoint: "player-c:9000", RequestedTTL: time.Second, Mode: TransferModeCrashTakeover, TakeoverProof: takeoverProofFor(secondFence)})
	if err != nil || second.Authority.HolderID != "player-c" {
		t.Fatalf("second transfer result=%+v err=%v", second, err)
	}
	before := len(publisher.publishes)
	replayed, err := manager.Transfer(context.Background(), firstTransfer)
	if !errors.Is(err, ErrStaleTransition) || !reflect.DeepEqual(replayed, transferred) || len(publisher.publishes) != before {
		t.Fatalf("stale transfer replay was accepted: replay=%+v err=%v publishes=%d before=%d", replayed, err, len(publisher.publishes), before)
	}
}

func TestRouteAssignmentManagerDrainReplayCannotPublishAfterLaterTransfer(t *testing.T) {
	publisher := &failOncePublisher{}
	manager, _ := newManagerForTest(t, publisher)
	claimed, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "drain-interleave-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-a", NewHolderBootID: "player-a-drain-boot", Endpoint: "player-a:9000", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	setPublisherFailure(publisher, true)
	drainRequest := TransferRequest{TransitionID: "drain-interleave-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "player-b-drain-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second}
	firstController := &recordingDrainController{}
	transferred, err := manager.Drain(context.Background(), drainRequest, firstController)
	if !errors.Is(err, ErrSnapshotPublish) || transferred.Authority.HolderID != "player-b" {
		t.Fatalf("drain transfer publish failure result=%+v err=%v", transferred, err)
	}
	if len(publisher.publishes) != 1 || publisher.publishes[0].Player.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("failed drain unexpectedly published assigning route: %+v", publisher.publishes)
	}
	laterFence := managerFence(transferred.Authority)
	if _, err := manager.Transfer(context.Background(), TransferRequest{TransitionID: "drain-interleave-later-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: laterFence, NewHolderID: "player-c", NewHolderBootID: "player-c-drain-boot", Endpoint: "player-c:9000", RequestedTTL: time.Second, Mode: TransferModeCrashTakeover, TakeoverProof: takeoverProofFor(laterFence)}); err != nil {
		t.Fatal(err)
	}
	before := len(publisher.publishes)
	replayController := &recordingDrainController{}
	replayed, err := manager.Drain(context.Background(), drainRequest, replayController)
	if !errors.Is(err, ErrStaleTransition) || !reflect.DeepEqual(replayed, transferred) || len(publisher.publishes) != before || len(replayController.steps) != 0 {
		t.Fatalf("stale drain replay was accepted: result=%+v err=%v publishes=%d steps=%v", replayed, err, len(publisher.publishes), replayController.steps)
	}
}

type expiredAlreadyAppliedStore struct{}

func (expiredAlreadyAppliedStore) ClaimShardAuthority(context.Context, authoritystore.ClaimShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionAlreadyApplied, Authority: authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "auth-v1", HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608040001, Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		HolderID: "player-b", HolderBootID: "player-b-boot", AssignmentRevision: 2, WriterEpoch: 2, LeaseEpoch: 2, State: authoritystore.ShardAuthorityActive, LeaseUntilMs: time.Now().Add(-time.Second).UnixMilli(), LastTransitionID: "expired-already-applied",
	}, AcceptedTTLms: 0}, nil
}
func (expiredAlreadyAppliedStore) RenewShardAuthority(context.Context, authoritystore.RenewShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return authoritystore.ShardAuthorityTransitionResult{}, nil
}
func (expiredAlreadyAppliedStore) TransferShardAuthority(context.Context, authoritystore.TransferShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return authoritystore.ShardAuthorityTransitionResult{}, nil
}
func (expiredAlreadyAppliedStore) UnassignShardAuthority(context.Context, authoritystore.UnassignShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return authoritystore.ShardAuthorityTransitionResult{}, nil
}

func TestRouteAssignmentManagerRejectsExpiredAlreadyAppliedLease(t *testing.T) {
	publishes := 0
	manager, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: expiredAlreadyAppliedStore{}, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			publishes++
			return nil
		}),
		InitialSnapshot: managerSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-a", Now: time.Now, TombstoneStore: NewMemoryTransitionTombstoneStore(1024),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "expired-already-applied", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-b", NewHolderBootID: "player-b-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second})
	if !errors.Is(err, ErrAuthorityLeaseExpired) || result.Status != authoritystore.ShardTransitionAlreadyApplied || publishes != 0 {
		t.Fatalf("expired ALREADY_APPLIED was published: result=%+v err=%v publishes=%d", result, err, publishes)
	}
}

func TestRouteAssignmentManagerHolderNamespaceIncludesDomain(t *testing.T) {
	var published []actorrouting.RouteSnapshot
	manager, _ := newManagerForTest(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
		return nil
	}))
	if _, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "same-holder-player", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "shared-instance", NewHolderBootID: "player-boot-new", Endpoint: "player:9000", RequestedTTL: time.Second}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "same-holder-farm", Domain: actorrouting.DomainFarm, VirtualShard: 0, NewHolderID: "shared-instance", NewHolderBootID: "farm-boot-new", Endpoint: "farm:9001", RequestedTTL: time.Second}); err != nil {
		t.Fatal(err)
	}
	last := published[len(published)-1]
	playerHolder := last.Holders[last.Player.Routes[0].HolderIndex]
	farmHolder := last.Holders[last.Farm.Routes[0].HolderIndex]
	if last.Player.Routes[0].HolderIndex == last.Farm.Routes[0].HolderIndex || playerHolder.BootID != "player-boot-new" || farmHolder.BootID != "farm-boot-new" {
		t.Fatalf("domain holder replacement shared namespace: player=%+v farm=%+v snapshot=%+v", playerHolder, farmHolder, last)
	}
}

func TestRouteAssignmentManagerBoundsPublishedTransitionMemos(t *testing.T) {
	manager, _ := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), MemoLimit: 1, MemoRetention: time.Hour})
	first, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "memo-bound-first", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-b", NewHolderBootID: "memo-b", Endpoint: "player-b:9000", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	fence := managerFence(first.Authority)
	if _, err := manager.Transfer(context.Background(), TransferRequest{TransitionID: "memo-bound-second", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: fence, NewHolderID: "player-c", NewHolderBootID: "memo-c", Endpoint: "player-c:9000", RequestedTTL: time.Second, Mode: TransferModeCrashTakeover, TakeoverProof: takeoverProofFor(fence)}); err != nil {
		t.Fatal(err)
	}
	if len(manager.transitions) > 1 {
		t.Fatalf("published transition memo limit not enforced: %d", len(manager.transitions))
	}
}

func TestRouteAssignmentManagerBoundsFailedPublicationRecoveryMemos(t *testing.T) {
	publisher := &alwaysFailPublisher{}
	manager, store := newManagerForTestWithOptions(t, publisher, ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), MemoLimit: 3, MemoRetention: time.Hour})
	firstReq := ClaimRequest{TransitionID: "failed-memo-a", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-b", NewHolderBootID: "memo-a", Endpoint: "player-b:9000", RequestedTTL: time.Second}
	first, err := manager.Claim(context.Background(), firstReq)
	if !errors.Is(err, ErrSnapshotPublish) {
		t.Fatalf("first failure=%v result=%+v", err, first)
	}
	secondFence := managerFence(first.Authority)
	second, err := manager.Transfer(context.Background(), TransferRequest{TransitionID: "failed-memo-b", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: secondFence, NewHolderID: "player-c", NewHolderBootID: "memo-b", Endpoint: "player-c:9000", RequestedTTL: time.Second, Mode: TransferModeCrashTakeover, TakeoverProof: takeoverProofFor(secondFence)})
	if !errors.Is(err, ErrSnapshotPublish) {
		t.Fatalf("second failure=%v result=%+v", err, second)
	}
	thirdFence := managerFence(second.Authority)
	third, err := manager.Transfer(context.Background(), TransferRequest{TransitionID: "failed-memo-c", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: thirdFence, NewHolderID: "player-d", NewHolderBootID: "memo-c", Endpoint: "player-d:9000", RequestedTTL: time.Second, Mode: TransferModeCrashTakeover, TakeoverProof: takeoverProofFor(thirdFence)})
	if !errors.Is(err, ErrSnapshotPublish) {
		t.Fatalf("third failure=%v result=%+v", err, third)
	}
	if len(manager.transitions) > 3 || len(manager.recoveries) > 3 {
		t.Fatalf("failed publication memos exceeded bounded active/recovery retention: active=%d recovery=%d", len(manager.transitions), len(manager.recoveries))
	}
	claimsBefore := store.claim
	transfersBefore := store.trans
	if _, err := manager.Claim(context.Background(), firstReq); !errors.Is(err, ErrStaleTransition) {
		t.Fatalf("recoverable stale memo replay err=%v", err)
	}
	if store.claim != claimsBefore || store.trans != transfersBefore {
		t.Fatalf("replayed failed memo repeated authority: claims %d->%d transfers %d->%d", claimsBefore, store.claim, transfersBefore, store.trans)
	}
}

func TestRouteAssignmentManagerRejectsHealthyDirectTransfer(t *testing.T) {
	manager, store := newManagerForTest(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}))
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "healthy-transfer-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "healthy-boot", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := manager.Transfer(context.Background(), TransferRequest{
		TransitionID: "healthy-direct-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "player-b-boot",
		Endpoint: "player-b:9000", RequestedTTL: time.Second,
	})
	if !errors.Is(err, ErrTransferTakeoverNeeded) || result.Status != authoritystore.ShardTransitionUnknown || store.trans != 0 {
		t.Fatalf("healthy holder received unrestricted transfer: result=%+v err=%v transfers=%d", result, err, store.trans)
	}
}

func TestRouteAssignmentManagerDrainReplayIsTerminalAndColdLoadOnce(t *testing.T) {
	var publishCalls int
	var published []actorrouting.RouteSnapshot
	manager, store := newManagerForTest(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		publishCalls++
		if publishCalls == 3 {
			return errors.New("active projection unavailable")
		}
		published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
		return nil
	}))
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "drain-replay-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "drain-replay-a", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := TransferRequest{
		TransitionID: "drain-replay-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "drain-replay-b",
		Endpoint: "player-b:9000", RequestedTTL: time.Second,
	}
	firstController := &recordingDrainController{}
	result, err := manager.Drain(context.Background(), req, firstController)
	if !errors.Is(err, ErrSnapshotPublish) || store.trans != 1 || !reflect.DeepEqual(firstController.steps, []string{"stop", "wait", "facts", "cold-load"}) || len(published) != 2 || published[1].Player.Routes[0].State != actorrouting.ShardStateAssigning {
		t.Fatalf("drain failure did not leave resumable assigning state: result=%+v err=%v steps=%v publishes=%d", result, err, firstController.steps, len(published))
	}
	secondController := &recordingDrainController{}
	replayed, err := manager.Drain(context.Background(), req, secondController)
	if err != nil || !reflect.DeepEqual(result, replayed) || len(secondController.steps) != 0 || store.trans != 1 || len(published) != 3 || published[2].Player.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("publisher-failure replay repeated drain side effects: result=%+v replay=%+v err=%v steps=%v publishes=%d", result, replayed, err, secondController.steps, len(published))
	}
	thirdController := &recordingDrainController{}
	completed, err := manager.Drain(context.Background(), req, thirdController)
	if err != nil || !reflect.DeepEqual(result, completed) || len(thirdController.steps) != 0 || store.trans != 1 || len(published) != 3 {
		t.Fatalf("completed drain replay was not terminal: result=%+v completed=%+v err=%v steps=%v publishes=%d", result, completed, err, thirdController.steps, len(published))
	}
}

func TestRouteAssignmentManagerDrainPublicationFailureResumesAfterAssigningCheckpoint(t *testing.T) {
	publishCalls := 0
	manager, store := newManagerForTest(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		publishCalls++
		if publishCalls == 2 {
			return errors.New("assigning projection unavailable")
		}
		return nil
	}))
	claimed, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "drain-checkpoint-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-a", NewHolderBootID: "drain-checkpoint-a", Endpoint: "player-a:9000", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	req := TransferRequest{TransitionID: "drain-checkpoint-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "drain-checkpoint-b", Endpoint: "player-b:9000", RequestedTTL: time.Second}
	firstController := &recordingDrainController{}
	result, err := manager.Drain(context.Background(), req, firstController)
	if !errors.Is(err, ErrSnapshotPublish) || store.trans != 1 || !reflect.DeepEqual(firstController.steps, []string{"stop", "wait", "facts"}) {
		t.Fatalf("assigning publication failure did not checkpoint safely: result=%+v err=%v steps=%v transfers=%d", result, err, firstController.steps, store.trans)
	}
	secondController := &recordingDrainController{}
	replayed, err := manager.Drain(context.Background(), req, secondController)
	if err != nil || !reflect.DeepEqual(result, replayed) || !reflect.DeepEqual(secondController.steps, []string{"cold-load"}) || store.trans != 1 {
		t.Fatalf("assigning retry repeated unsafe side effects: result=%+v replay=%+v err=%v steps=%v transfers=%d", result, replayed, err, secondController.steps, store.trans)
	}
}

func TestRouteAssignmentManagerRecoversCommittedProjectionWithoutAuthorityMutation(t *testing.T) {
	tombstones := NewMemoryTransitionTombstoneStore(8)
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return errors.New("projection unavailable")
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), TombstoneStore: tombstones})
	result, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "manager-owned-recovery-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-b", NewHolderBootID: "recovery-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
	})
	if !errors.Is(err, ErrSnapshotPublish) || result.Status != authoritystore.ShardTransitionApplied || store.claim != 1 {
		t.Fatalf("initial result=%+v err=%v claims=%d", result, err, store.claim)
	}
	var published []actorrouting.RouteSnapshot
	restarted, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
			published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
			return nil
		}),
		InitialSnapshot: managerSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-recovery",
		RouteManagerBootID: "recovery-boot", TombstoneStore: tombstones,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := restarted.RecoverPendingProjections(ctx, acceptPendingProjectionTarget, nil); err != nil {
		t.Fatalf("recover err=%v", err)
	}
	if store.claim != 1 || store.trans != 0 || store.unassign != 0 {
		t.Fatalf("recovery mutated authority claims=%d transfers=%d unassigns=%d", store.claim, store.trans, store.unassign)
	}
	if len(published) != 1 || restarted.CurrentSnapshot().Player.Routes[0].State != actorrouting.ShardStateActive ||
		restarted.CurrentSnapshot().Player.Routes[0].ExpectedWriterEpoch != result.Authority.WriterEpoch {
		t.Fatalf("published=%v snapshot=%+v", published, restarted.CurrentSnapshot().Player.Routes[0])
	}
	recoveries, err := tombstones.LoadTransitionRecoveries(context.Background())
	if err != nil || len(recoveries) != 1 || recoveries[0].TransitionID != "manager-owned-recovery-claim" || !recoveries[0].Published {
		t.Fatalf("recoveries=%+v err=%v", recoveries, err)
	}
}

func TestRouteAssignmentManagerClaimRecoveryAdoptsFreshSameGenerationRenewal(t *testing.T) {
	clock := time.UnixMilli(1_900_000_000_000)
	now := func() time.Time { return clock }
	tombstones := NewMemoryTransitionTombstoneStore(8)
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return errors.New("projection unavailable")
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), TombstoneStore: tombstones, Now: now})
	committed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "renewed-claim-recovery", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-b", NewHolderBootID: "renewed-claim-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
	})
	if !errors.Is(err, ErrSnapshotPublish) || committed.Status != authoritystore.ShardTransitionApplied || store.claim != 1 {
		t.Fatalf("initial claim=%+v err=%v claims=%d", committed, err, store.claim)
	}
	oldLease := committed.Authority.LeaseUntilMs
	renewed, err := store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: "player-b", BootID: "renewed-claim-boot",
		},
		AuthorityLayoutVersion: "auth-v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(committed.Authority), RequestedTTLms: 5 * time.Second.Milliseconds(),
	})
	if err != nil || renewed.Status != authoritystore.ShardTransitionApplied || renewed.Authority.LeaseUntilMs <= oldLease {
		t.Fatalf("renewed=%+v err=%v old_lease=%d", renewed, err, oldLease)
	}
	clock = clock.Add(2 * time.Second)
	if oldLease >= clock.UnixMilli() || renewed.Authority.LeaseUntilMs <= clock.UnixMilli() {
		t.Fatalf("invalid test window old=%d now=%d renewed=%d", oldLease, clock.UnixMilli(), renewed.Authority.LeaseUntilMs)
	}

	publishes := 0
	restarted, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, AuthorityReader: store, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			publishes++
			return nil
		}),
		InitialSnapshot: managerSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-renewed-claim",
		RouteManagerBootID: "route-manager-renewed-claim-boot", TombstoneStore: tombstones, Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = restarted.RecoverPendingProjections(ctx, acceptPendingProjectionTarget, nil)
	cancel()
	if err != nil || publishes != 1 || store.claim != 1 || store.trans != 0 || store.unassign != 0 {
		t.Fatalf("recovery err=%v publishes=%d calls=%d/%d/%d", err, publishes, store.claim, store.trans, store.unassign)
	}
	route := restarted.CurrentSnapshot().Player.Routes[0]
	if route.State != actorrouting.ShardStateActive || route.LeaseUntilMs != renewed.Authority.LeaseUntilMs ||
		route.ExpectedWriterEpoch != renewed.Authority.WriterEpoch {
		t.Fatalf("route=%+v renewed=%+v", route, renewed.Authority)
	}
	recoveries, loadErr := tombstones.LoadTransitionRecoveries(context.Background())
	if loadErr != nil || len(recoveries) != 1 || recoveries[0].Result.Authority.LeaseUntilMs != renewed.Authority.LeaseUntilMs || !recoveries[0].Published {
		t.Fatalf("recoveries=%+v err=%v", recoveries, loadErr)
	}
}

func TestRouteAssignmentManagerPendingProjectionRequiresFreshTargetFactsAndRetries(t *testing.T) {
	tombstones := NewMemoryTransitionTombstoneStore(8)
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return errors.New("projection unavailable")
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), TombstoneStore: tombstones})
	result, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "manager-target-validation-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-b", NewHolderBootID: "target-validation-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
	})
	if !errors.Is(err, ErrSnapshotPublish) || store.claim != 1 {
		t.Fatalf("claim result=%+v err=%v claims=%d", result, err, store.claim)
	}
	publishes := 0
	restarted, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			publishes++
			return nil
		}),
		InitialSnapshot: managerSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-target-validation",
		RouteManagerBootID: "target-validation-manager-boot", TombstoneStore: tombstones,
	})
	if err != nil {
		t.Fatal(err)
	}
	validatorCalls := 0
	validator := PendingProjectionTargetValidator(func(_ context.Context, recovery TransitionRecovery, revision uint64) error {
		validatorCalls++
		if recovery.TransitionID != "manager-target-validation-claim" || recovery.NewHolderID != "player-b" ||
			recovery.NewHolderBootID != "target-validation-boot" || recovery.Endpoint != "player-b:9000" || revision != 1 {
			t.Fatalf("validator recovery=%+v revision=%d", recovery, revision)
		}
		if validatorCalls == 1 {
			return errors.New("target heartbeat is stale")
		}
		return nil
	})
	firstCtx, firstCancel := context.WithTimeout(context.Background(), time.Second)
	err = restarted.RecoverPendingProjections(firstCtx, validator, nil)
	firstCancel()
	if !errors.Is(err, ErrProjectionRecovery) || publishes != 0 || store.claim != 1 || store.trans != 0 || store.unassign != 0 {
		t.Fatalf("failed target validation err=%v publishes=%d calls=%d/%d/%d", err, publishes, store.claim, store.trans, store.unassign)
	}
	secondCtx, secondCancel := context.WithTimeout(context.Background(), time.Second)
	err = restarted.RecoverPendingProjections(secondCtx, validator, nil)
	secondCancel()
	if err != nil || publishes != 1 || validatorCalls != 2 || store.claim != 1 || store.trans != 0 || store.unassign != 0 {
		t.Fatalf("target retry err=%v publishes=%d validator=%d calls=%d/%d/%d", err, publishes, validatorCalls, store.claim, store.trans, store.unassign)
	}
}

func TestRouteAssignmentManagerProjectionAuthorityMismatchIsSticky(t *testing.T) {
	tombstones := NewMemoryTransitionTombstoneStore(8)
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return errors.New("projection unavailable")
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), TombstoneStore: tombstones})
	result, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "sticky-projection-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-b", NewHolderBootID: "sticky-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
	})
	if !errors.Is(err, ErrSnapshotPublish) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	observed := result.Authority
	observed.HolderBootID = "different-boot"
	publishes := 0
	restarted, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			publishes++
			return nil
		}),
		AuthorityReader: AuthorityReaderFunc(func(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error) {
			return observed, true, nil
		}),
		InitialSnapshot: managerSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-sticky",
		RouteManagerBootID: "sticky-manager-boot", TombstoneStore: tombstones,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, firstCancel := context.WithTimeout(context.Background(), time.Second)
	err = restarted.RecoverPendingProjections(firstCtx, acceptPendingProjectionTarget, nil)
	firstCancel()
	if !errors.Is(err, ErrStaleTransition) || publishes != 0 {
		t.Fatalf("first recovery err=%v publishes=%d", err, publishes)
	}
	observed = result.Authority
	secondCtx, secondCancel := context.WithTimeout(context.Background(), time.Second)
	err = restarted.RecoverPendingProjections(secondCtx, acceptPendingProjectionTarget, nil)
	secondCancel()
	if !errors.Is(err, ErrStaleTransition) || publishes != 0 || store.claim != 1 || store.trans != 0 || store.unassign != 0 {
		t.Fatalf("sticky recovery err=%v publishes=%d claims=%d transfers=%d unassigns=%d", err, publishes, store.claim, store.trans, store.unassign)
	}
}

func TestRouteAssignmentManagerPendingDrainRecoveryUsesDurableCheckpoint(t *testing.T) {
	tombstones := NewMemoryTransitionTombstoneStore(8)
	publishCalls := 0
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		publishCalls++
		if publishCalls == 2 {
			return errors.New("assigning projection unavailable")
		}
		return nil
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), TombstoneStore: tombstones})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "manager-recovery-drain-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "manager-recovery-source", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	drainRequest := TransferRequest{
		TransitionID: "manager-recovery-drain-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "manager-recovery-target",
		Endpoint: "player-b:9000", RequestedTTL: time.Second,
	}
	initialController := &recordingDrainController{}
	result, err := manager.Drain(context.Background(), drainRequest, initialController)
	if !errors.Is(err, ErrSnapshotPublish) || store.trans != 1 ||
		!reflect.DeepEqual(initialController.steps, []string{"stop", "wait", "facts"}) {
		t.Fatalf("initial result=%+v err=%v steps=%v transfers=%d", result, err, initialController.steps, store.trans)
	}

	activeFailures := 1
	restarted, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
			if snapshot.Player.Routes[0].State == actorrouting.ShardStateActive && activeFailures > 0 {
				activeFailures--
				return errors.New("active projection unavailable")
			}
			return nil
		}),
		InitialSnapshot: manager.CurrentSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-drain-recovery",
		RouteManagerBootID: "drain-recovery-boot", TombstoneStore: tombstones,
	})
	if err != nil {
		t.Fatal(err)
	}
	recoveryController := &recordingDrainController{}
	factoryCalls := 0
	factory := PendingDrainControllerFactory(func(context.Context, TransitionRecovery) (DrainController, func() error, error) {
		factoryCalls++
		return recoveryController, nil, nil
	})
	firstCtx, firstCancel := context.WithTimeout(context.Background(), time.Second)
	err = restarted.RecoverPendingProjections(firstCtx, acceptPendingProjectionTarget, factory)
	firstCancel()
	if !errors.Is(err, ErrProjectionRecovery) || store.trans != 1 || factoryCalls != 1 ||
		!reflect.DeepEqual(recoveryController.steps, []string{"cold-load"}) {
		t.Fatalf("first recovery err=%v transfers=%d factory=%d steps=%v", err, store.trans, factoryCalls, recoveryController.steps)
	}
	recoveries, err := tombstones.LoadTransitionRecoveries(context.Background())
	if err != nil || !containsRecovery(recoveries, drainRequest.TransitionID, actorrouting.ShardStateAssigning, true) {
		t.Fatalf("checkpoint recoveries=%+v err=%v", recoveries, err)
	}
	secondCtx, secondCancel := context.WithTimeout(context.Background(), time.Second)
	err = restarted.RecoverPendingProjections(secondCtx, acceptPendingProjectionTarget, factory)
	secondCancel()
	if err != nil || store.trans != 1 || factoryCalls != 1 ||
		!reflect.DeepEqual(recoveryController.steps, []string{"cold-load"}) ||
		restarted.CurrentSnapshot().Player.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("second recovery err=%v transfers=%d factory=%d steps=%v snapshot=%+v", err, store.trans, factoryCalls, recoveryController.steps, restarted.CurrentSnapshot().Player.Routes[0])
	}
	recoveries, err = tombstones.LoadTransitionRecoveries(context.Background())
	if err != nil || !containsRecovery(recoveries, drainRequest.TransitionID, actorrouting.ShardStateActive, true) {
		t.Fatalf("final recoveries=%+v err=%v", recoveries, err)
	}
}

func TestRouteAssignmentManagerPreparedGenerationClosesColdLoadMarkerCrashWindow(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		t.Run(fmt.Sprintf("mismatch=%t", mismatch), func(t *testing.T) {
			tombstones := NewMemoryTransitionTombstoneStore(8)
			manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
				return nil
			}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), TombstoneStore: tombstones})
			claimed, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "prepared-window-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-a", NewHolderBootID: "prepared-source", Endpoint: "player-a:9000", RequestedTTL: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			req := TransferRequest{TransitionID: "prepared-window-drain", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "prepared-target", Endpoint: "player-b:9000", RequestedTTL: time.Second}
			initial := &recordingDrainController{coldErr: errors.New("crash before durable marker")}
			committed, err := manager.Drain(context.Background(), req, initial)
			if err == nil || store.trans != 1 || len(initial.coldAuthorities) != 1 {
				t.Fatalf("checkpoint result=%+v err=%v transfers=%d cold=%d", committed, err, store.trans, len(initial.coldAuthorities))
			}
			restarted, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
				Store: store, AuthorityReader: store, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
					return nil
				}),
				InitialSnapshot: manager.CurrentSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "prepared-restart", RouteManagerBootID: "prepared-restart-boot", TombstoneStore: tombstones,
			})
			if err != nil {
				t.Fatal(err)
			}
			prepared := committed.Authority
			if mismatch {
				prepared.LeaseEpoch++
			}
			recoveryController := &recordingDrainController{prepared: &prepared}
			var renewed authoritystore.ShardAuthorityTransitionResult
			if !mismatch {
				recoveryController.preparedHook = func() {
					recoveryController.preparedHook = nil
					var renewErr error
					renewed, renewErr = store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
						Caller:                 authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: "player-b", BootID: "prepared-target"},
						AuthorityLayoutVersion: "auth-v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
						ExpectedFence: managerFence(committed.Authority), RequestedTTLms: 5 * time.Second.Milliseconds(),
					})
					if renewErr != nil {
						recoveryController.preparedErr = renewErr
						return
					}
					recoveryController.prepared = &renewed.Authority
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err = restarted.RecoverPendingProjections(ctx, acceptPendingProjectionTarget, func(context.Context, TransitionRecovery) (DrainController, func() error, error) {
				return recoveryController, nil, nil
			})
			cancel()
			if mismatch {
				if err == nil || len(recoveryController.coldAuthorities) != 0 || store.trans != 1 {
					t.Fatalf("mismatch err=%v cold=%d transfers=%d", err, len(recoveryController.coldAuthorities), store.trans)
				}
				return
			}
			route := restarted.CurrentSnapshot().Player.Routes[0]
			if err != nil || len(recoveryController.coldAuthorities) != 0 || store.trans != 1 ||
				route.State != actorrouting.ShardStateActive || renewed.Authority.LeaseUntilMs <= committed.Authority.LeaseUntilMs ||
				route.LeaseUntilMs != renewed.Authority.LeaseUntilMs {
				t.Fatalf("prepared recovery err=%v cold=%d transfers=%d snapshot=%+v renewed=%+v", err, len(recoveryController.coldAuthorities), store.trans, route, renewed.Authority)
			}
			durableRecoveries, loadErr := tombstones.LoadTransitionRecoveries(context.Background())
			if loadErr != nil || len(durableRecoveries) == 0 {
				t.Fatalf("load completed recovery=%+v err=%v", durableRecoveries, loadErr)
			}
			var durableRecovery TransitionRecovery
			for _, candidate := range durableRecoveries {
				if candidate.TransitionID == req.TransitionID {
					durableRecovery = candidate
				}
			}
			restarted.mu.Lock()
			memo := restarted.transitionMemoLocked(req.TransitionID)
			if memo == nil || !transitionRecoveryEvidenceEqual(recoveryForMemo(memo), durableRecovery) {
				restarted.mu.Unlock()
				t.Fatalf("completed memo does not match durable recovery: memo=%+v durable=%+v", memo, durableRecovery)
			}
			restarted.mu.Unlock()
			observedCtx, observedCancel := context.WithTimeout(context.Background(), time.Second)
			observation, observeErr := restarted.ObserveTransition(observedCtx, TransitionObservationRequest{Transfer: req, PlannedDrain: true})
			observedCancel()
			if observeErr != nil || observation.Class != TransitionEvidenceCompleteRecovery || observation.Recovery.Result.Authority.LeaseUntilMs != renewed.Authority.LeaseUntilMs {
				t.Fatalf("prepared recovery observation=%+v err=%v", observation, observeErr)
			}
		})
	}
}

func TestRouteAssignmentManagerRecoveryFinalizesExactActiveSnapshotWithoutCallbacks(t *testing.T) {
	tombstones := &failNextDeleteRecoveryStore{MemoryTransitionTombstoneStore: NewMemoryTransitionTombstoneStore(8)}
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), TombstoneStore: tombstones})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "active-recovery-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "active-recovery-source", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	tombstones.armCommitFailure()
	drainRequest := TransferRequest{
		TransitionID: "active-recovery-drain", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "active-recovery-target",
		Endpoint: "player-b:9000", RequestedTTL: time.Second,
	}
	initialController := &recordingDrainController{}
	result, err := manager.Drain(context.Background(), drainRequest, initialController)
	if !errors.Is(err, ErrSnapshotPublish) || result.Status != authoritystore.ShardTransitionApplied ||
		!reflect.DeepEqual(initialController.steps, []string{"stop", "wait", "facts", "cold-load"}) ||
		manager.CurrentSnapshot().Player.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("initial drain result=%+v err=%v steps=%v snapshot=%+v", result, err, initialController.steps, manager.CurrentSnapshot().Player.Routes[0])
	}
	publishes := 0
	restarted, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			publishes++
			return nil
		}),
		InitialSnapshot: manager.CurrentSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-active-recovery",
		RouteManagerBootID: "active-recovery-manager-boot", TombstoneStore: tombstones,
	})
	if err != nil {
		t.Fatal(err)
	}
	validatorCalls := 0
	validator := PendingProjectionTargetValidator(func(_ context.Context, recovery TransitionRecovery, revision uint64) error {
		validatorCalls++
		if recovery.Endpoint != "player-b:9000" || revision != manager.CurrentSnapshot().GlobalRevision {
			t.Fatalf("recovery=%+v revision=%d", recovery, revision)
		}
		return nil
	})
	factoryCalls := 0
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = restarted.RecoverPendingProjections(ctx, validator, func(context.Context, TransitionRecovery) (DrainController, func() error, error) {
		factoryCalls++
		return nil, nil, errors.New("factory must not be called for exact ACTIVE")
	})
	cancel()
	if err != nil || publishes != 0 || validatorCalls != 0 || factoryCalls != 0 || store.claim != 1 || store.trans != 1 || store.unassign != 0 {
		t.Fatalf("exact ACTIVE recovery err=%v publishes=%d validator=%d factory=%d calls=%d/%d/%d", err, publishes, validatorCalls, factoryCalls, store.claim, store.trans, store.unassign)
	}
	recoveries, err := tombstones.LoadTransitionRecoveries(context.Background())
	if err != nil || !containsRecovery(recoveries, drainRequest.TransitionID, actorrouting.ShardStateActive, true) {
		t.Fatalf("recoveries=%+v err=%v", recoveries, err)
	}
}

func TestRouteAssignmentManagerClaimRecoveryFinalizesExactActiveSnapshotWithoutAuthorityMutation(t *testing.T) {
	tombstones := &failNextDeleteRecoveryStore{MemoryTransitionTombstoneStore: NewMemoryTransitionTombstoneStore(8)}
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), TombstoneStore: tombstones})
	tombstones.armCommitFailure()
	result, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "active-recovery-claim-only", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-b", NewHolderBootID: "active-recovery-claim-boot", Endpoint: "player-b:9000", RequestedTTL: time.Second,
	})
	if !errors.Is(err, ErrSnapshotPublish) || result.Status != authoritystore.ShardTransitionApplied ||
		manager.CurrentSnapshot().Player.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("initial claim result=%+v err=%v snapshot=%+v", result, err, manager.CurrentSnapshot().Player.Routes[0])
	}
	publishes := 0
	restarted, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
			publishes++
			return nil
		}),
		InitialSnapshot: manager.CurrentSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-active-claim-recovery",
		RouteManagerBootID: "active-claim-recovery-manager-boot", TombstoneStore: tombstones,
	})
	if err != nil {
		t.Fatal(err)
	}
	validatorCalls := 0
	validator := PendingProjectionTargetValidator(func(_ context.Context, recovery TransitionRecovery, revision uint64) error {
		validatorCalls++
		if recovery.TransitionID != "active-recovery-claim-only" || recovery.Endpoint != "player-b:9000" ||
			revision != manager.CurrentSnapshot().GlobalRevision {
			t.Fatalf("recovery=%+v revision=%d", recovery, revision)
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = restarted.RecoverPendingProjections(ctx, validator, nil)
	cancel()
	if err != nil || publishes != 0 || validatorCalls != 0 || store.claim != 1 || store.trans != 0 || store.unassign != 0 {
		t.Fatalf("exact ACTIVE claim recovery err=%v publishes=%d validator=%d calls=%d/%d/%d", err, publishes, validatorCalls, store.claim, store.trans, store.unassign)
	}
	recoveries, err := tombstones.LoadTransitionRecoveries(context.Background())
	if err != nil || !containsRecovery(recoveries, "active-recovery-claim-only", actorrouting.ShardStateActive, true) {
		t.Fatalf("recoveries=%+v err=%v", recoveries, err)
	}
}

func TestRouteAssignmentManagerPendingDrainRecoveryUsesFreshRenewedAuthority(t *testing.T) {
	for _, checkpoint := range []int{2, 3} {
		t.Run(fmt.Sprintf("checkpoint-%d", checkpoint), func(t *testing.T) {
			clock := time.UnixMilli(1_900_000_000_000)
			now := func() time.Time { return clock }
			tombstones := NewMemoryTransitionTombstoneStore(8)
			failActive := false
			manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
				if failActive && snapshot.Player.Routes[0].State == actorrouting.ShardStateActive {
					failActive = false
					return errors.New("active projection unavailable")
				}
				return nil
			}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), TombstoneStore: tombstones, Now: now})
			claimed, err := manager.Claim(context.Background(), ClaimRequest{
				TransitionID: "renewed-recovery-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
				NewHolderID: "player-a", NewHolderBootID: "renewed-recovery-source", Endpoint: "player-a:9000", RequestedTTL: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			req := TransferRequest{
				TransitionID: fmt.Sprintf("renewed-recovery-drain-%d", checkpoint), Domain: actorrouting.DomainPlayer, VirtualShard: 0,
				ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "renewed-recovery-target",
				Endpoint: "player-b:9000", RequestedTTL: time.Second,
			}
			initialController := &recordingDrainController{}
			if checkpoint == 2 {
				initialController.coldErr = errors.New("cold load unavailable")
			} else {
				failActive = true
			}
			committed, err := manager.Drain(context.Background(), req, initialController)
			if err == nil || store.trans != 1 {
				t.Fatalf("initial checkpoint=%d result=%+v err=%v transfers=%d", checkpoint, committed, err, store.trans)
			}
			oldLease := committed.Authority.LeaseUntilMs
			renewed, err := store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
				Caller:                 authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: "player-b", BootID: "renewed-recovery-target"},
				AuthorityLayoutVersion: "auth-v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
				ExpectedFence: managerFence(committed.Authority), RequestedTTLms: 5 * time.Second.Milliseconds(),
			})
			if err != nil || renewed.Status != authoritystore.ShardTransitionApplied || renewed.Authority.LeaseUntilMs <= oldLease {
				t.Fatalf("renewed=%+v err=%v old_lease=%d", renewed, err, oldLease)
			}
			clock = clock.Add(2 * time.Second)
			if oldLease >= clock.UnixMilli() || renewed.Authority.LeaseUntilMs <= clock.UnixMilli() {
				t.Fatalf("test lease window old=%d now=%d renewed=%d", oldLease, clock.UnixMilli(), renewed.Authority.LeaseUntilMs)
			}

			restarted, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
				Store: store, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
					return nil
				}),
				InitialSnapshot: manager.CurrentSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-renewed-recovery",
				RouteManagerBootID: fmt.Sprintf("renewed-recovery-boot-%d", checkpoint), TombstoneStore: tombstones, Now: now,
			})
			if err != nil {
				t.Fatal(err)
			}
			recoveryController := &recordingDrainController{}
			factoryCalls := 0
			factory := PendingDrainControllerFactory(func(context.Context, TransitionRecovery) (DrainController, func() error, error) {
				factoryCalls++
				return recoveryController, nil, nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err = restarted.RecoverPendingProjections(ctx, acceptPendingProjectionTarget, factory)
			cancel()
			if err != nil || store.trans != 1 {
				t.Fatalf("recovery checkpoint=%d err=%v transfers=%d", checkpoint, err, store.trans)
			}
			route := restarted.CurrentSnapshot().Player.Routes[0]
			if route.State != actorrouting.ShardStateActive || route.LeaseUntilMs != renewed.Authority.LeaseUntilMs {
				t.Fatalf("route=%+v renewed=%+v", route, renewed.Authority)
			}
			expectedFactoryCalls := 1
			expectedColdLoads := 1
			if checkpoint == 3 {
				// ColdLoad completed before the first controller stopped. Recovery
				// must accept the same generation's authenticated lease extension,
				// but must not repeat the remote prepare side effect.
				expectedFactoryCalls = 0
				expectedColdLoads = 0
			}
			if factoryCalls != expectedFactoryCalls || len(recoveryController.coldAuthorities) != expectedColdLoads ||
				(expectedColdLoads == 1 && recoveryController.coldAuthorities[0].LeaseUntilMs != renewed.Authority.LeaseUntilMs) {
				t.Fatalf("checkpoint=%d factory=%d cold=%+v renewed=%+v", checkpoint, factoryCalls, recoveryController.coldAuthorities, renewed.Authority)
			}
			recoveries, loadErr := tombstones.LoadTransitionRecoveries(context.Background())
			if loadErr != nil || !containsRecovery(recoveries, req.TransitionID, actorrouting.ShardStateActive, true) {
				t.Fatalf("completed recovery=%+v err=%v", recoveries, loadErr)
			}
			if checkpoint == 3 {
				// A later controller can observe the exact ACTIVE generation in a
				// Snapshot whose deadline still trails the holder-renewed durable
				// Authority. That is terminal publication evidence, not a reason to
				// demote the drain to checkpoint zero and repeat side effects.
				laggingSnapshot := restarted.CurrentSnapshot()
				laggingSnapshot.Player.Routes[0].LeaseUntilMs = oldLease
				terminalPublishes := 0
				terminal, terminalErr := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
					Store: store, Publisher: testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
						terminalPublishes++
						return nil
					}),
					InitialSnapshot: laggingSnapshot, AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-renewed-terminal",
					RouteManagerBootID: "renewed-terminal-boot", TombstoneStore: tombstones, Now: now,
				})
				if terminalErr != nil {
					t.Fatal(terminalErr)
				}
				terminalCtx, terminalCancel := context.WithTimeout(context.Background(), time.Second)
				terminalErr = terminal.RecoverPendingProjections(terminalCtx, nil, nil)
				terminalCancel()
				if terminalErr != nil || terminalPublishes != 0 || terminal.CurrentSnapshot().Player.Routes[0].LeaseUntilMs != oldLease {
					t.Fatalf("terminal deadline-lag recovery err=%v publishes=%d route=%+v", terminalErr, terminalPublishes, terminal.CurrentSnapshot().Player.Routes[0])
				}
			}
		})
	}
}

func TestRouteAssignmentManagerRecoveryPersistsCompletedColdLoadWithoutRepeatingCallback(t *testing.T) {
	tombstones := &failColdLoadCheckpointStore{MemoryTransitionTombstoneStore: NewMemoryTransitionTombstoneStore(8), fail: true}
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), TombstoneStore: tombstones})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "cold-checkpoint-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "cold-checkpoint-source", Endpoint: "player-a:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := TransferRequest{
		TransitionID: "cold-checkpoint-drain", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "cold-checkpoint-target",
		Endpoint: "player-b:9000", RequestedTTL: time.Second,
	}
	controller := &recordingDrainController{}
	result, err := manager.Drain(context.Background(), req, controller)
	if !errors.Is(err, ErrSnapshotPublish) || result.Status != authoritystore.ShardTransitionApplied ||
		!reflect.DeepEqual(controller.steps, []string{"stop", "wait", "facts", "cold-load"}) || store.trans != 1 {
		t.Fatalf("initial result=%+v err=%v steps=%v transfers=%d", result, err, controller.steps, store.trans)
	}
	factoryCalls := 0
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = manager.RecoverPendingProjections(ctx, acceptPendingProjectionTarget, func(context.Context, TransitionRecovery) (DrainController, func() error, error) {
		factoryCalls++
		return nil, nil, errors.New("completed cold load must not request a controller")
	})
	cancel()
	if err != nil || factoryCalls != 0 || store.trans != 1 ||
		!reflect.DeepEqual(controller.steps, []string{"stop", "wait", "facts", "cold-load"}) ||
		manager.CurrentSnapshot().Player.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("retry err=%v factory=%d transfers=%d steps=%v snapshot=%+v", err, factoryCalls, store.trans, controller.steps, manager.CurrentSnapshot().Player.Routes[0])
	}
}

func TestRouteAssignmentManagerCommittedDrainRecoverySurvivesRestartAndCapacityPressure(t *testing.T) {
	var publishCalls int
	var failFinal bool
	var published []actorrouting.RouteSnapshot
	publisher := testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		publishCalls++
		if failFinal && snapshot.Player.Routes[0].State == actorrouting.ShardStateActive && publishCalls > 1 {
			return errors.New("final projection unavailable")
		}
		published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
		return nil
	})
	tombstones := NewMemoryTransitionTombstoneStore(8)
	manager, store := newManagerForTestWithOptions(t, publisher, ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), MemoLimit: 1, TombstoneStore: tombstones})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "drain-recovery-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-a", NewHolderBootID: "drain-recovery-a", Endpoint: "player-a:9000", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	req := TransferRequest{TransitionID: "drain-recovery-transfer", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority), NewHolderID: "player-b", NewHolderBootID: "drain-recovery-b", Endpoint: "player-b:9000", RequestedTTL: time.Second}
	failFinal = true
	controller := &recordingDrainController{}
	result, err := manager.Drain(context.Background(), req, controller)
	if !errors.Is(err, ErrSnapshotPublish) || store.trans != 1 || !reflect.DeepEqual(controller.steps, []string{"stop", "wait", "facts", "cold-load"}) || manager.CurrentSnapshot().Player.Routes[0].State != actorrouting.ShardStateAssigning {
		t.Fatalf("committed Drain was not retained at safe checkpoint: result=%+v err=%v steps=%v snapshot=%+v", result, err, controller.steps, manager.CurrentSnapshot())
	}
	if _, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "drain-recovery-capacity", Domain: actorrouting.DomainFarm, VirtualShard: 0, NewHolderID: "farm-b", NewHolderBootID: "drain-recovery-farm", Endpoint: "farm-b:9001", RequestedTTL: time.Second}); !errors.Is(err, ErrTransitionTombstoneCap) || store.claim != 1 {
		t.Fatalf("incomplete committed projection was evicted under capacity pressure: err=%v claims=%d", err, store.claim)
	}

	restarted, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
			published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
			return nil
		}),
		InitialSnapshot: manager.CurrentSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-restarted", RouteManagerBootID: "drain-recovery-restart", TombstoneStore: tombstones,
		MemoLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Claim(context.Background(), ClaimRequest{TransitionID: "drain-recovery-post-restart-pressure", Domain: actorrouting.DomainFarm, VirtualShard: 0, NewHolderID: "farm-pressure", NewHolderBootID: "farm-pressure-boot", Endpoint: "farm-pressure:9001", RequestedTTL: time.Second}); !errors.Is(err, ErrTransitionTombstoneCap) || store.claim != 1 {
		t.Fatalf("restarted ASSIGNING Drain lost its capacity pin: err=%v claims=%d", err, store.claim)
	}
	if tombstone, found, lookupErr := tombstones.LookupTransitionTombstone(context.Background(), req.TransitionID); lookupErr != nil || found {
		t.Fatalf("pending Drain expired into tombstone under restart pressure: tombstone=%+v found=%t err=%v", tombstone, found, lookupErr)
	}
	replayController := &recordingDrainController{}
	replayed, err := restarted.Drain(context.Background(), req, replayController)
	if err != nil || !reflect.DeepEqual(result, replayed) || len(replayController.steps) != 0 || store.trans != 1 || restarted.CurrentSnapshot().Player.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("restart did not resume committed Drain projection: result=%+v replay=%+v err=%v steps=%v transfers=%d snapshot=%+v", result, replayed, err, replayController.steps, store.trans, restarted.CurrentSnapshot())
	}
}

func TestRouteAssignmentManagerRenewRefreshesProjectionWithoutGenerationChange(t *testing.T) {
	manager, _ := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}), ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), Now: time.Now})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "renew-projection-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "player-a", NewHolderBootID: "renew-projection-boot", Endpoint: "player-a:9000", RequestedTTL: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	oldDeadline := manager.CurrentSnapshot().Player.Routes[0].LeaseUntilMs
	renewed, err := manager.Renew(context.Background(), RenewRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: "player-a", BootID: "renew-projection-boot"},
		Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority), RequestedTTL: 400 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	route := manager.CurrentSnapshot().Player.Routes[0]
	if route.LeaseUntilMs != renewed.Authority.LeaseUntilMs || route.LeaseUntilMs <= oldDeadline || route.ShardRevision != claimed.Authority.AssignmentRevision || route.ExpectedWriterEpoch != claimed.Authority.WriterEpoch || route.ExpectedLeaseEpoch != claimed.Authority.LeaseEpoch {
		t.Fatalf("renew changed or failed to project generation: route=%+v renewed=%+v old_deadline=%d", route, renewed.Authority, oldDeadline)
	}
	resolver, err := actorrouting.NewStaticResolver(manager.CurrentSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if _, err := resolver.ResolveActor(context.Background(), actorrouting.DomainPlayer, 42); err != nil {
		t.Fatalf("route expired at old deadline instead of renewed deadline: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, err := resolver.ResolveActor(context.Background(), actorrouting.DomainPlayer, 42); !errors.Is(err, actorrouting.ErrLeaseExpired) {
		t.Fatalf("route remained admitted after renewed deadline: %v", err)
	}
}

func TestRouteAssignmentManagerEvictedTransitionTombstoneSurvivesReload(t *testing.T) {
	publisher := testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	})
	tombstones := NewMemoryTransitionTombstoneStore(8)
	nowMillis := int64(1_900_000_000_000)
	now := func() time.Time {
		nowMillis++
		return time.UnixMilli(nowMillis)
	}
	manager, store := newManagerForTestWithOptions(t, publisher, ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(), MemoLimit: 1, MemoRetention: time.Hour, TombstoneStore: tombstones, Now: now})
	firstReq := ClaimRequest{TransitionID: "evicted-replay-id", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "player-b", NewHolderBootID: "evicted-b", Endpoint: "player-b:9000", RequestedTTL: time.Second}
	first, err := manager.Claim(context.Background(), firstReq)
	if err != nil {
		t.Fatalf("first terminal publication=%v result=%+v", err, first)
	}
	fenceB := managerFence(first.Authority)
	second, err := manager.Transfer(context.Background(), TransferRequest{TransitionID: "evicted-replay-b", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: fenceB, NewHolderID: "player-c", NewHolderBootID: "evicted-c", Endpoint: "player-c:9000", RequestedTTL: time.Second, Mode: TransferModeCrashTakeover, TakeoverProof: takeoverProofFor(fenceB)})
	if err != nil {
		t.Fatalf("second terminal publication=%v result=%+v", err, second)
	}
	fenceC := managerFence(second.Authority)
	if _, err := manager.Transfer(context.Background(), TransferRequest{TransitionID: "evicted-replay-c", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: fenceC, NewHolderID: "player-d", NewHolderBootID: "evicted-d", Endpoint: "player-d:9000", RequestedTTL: time.Second, Mode: TransferModeCrashTakeover, TakeoverProof: takeoverProofFor(fenceC)}); err != nil {
		t.Fatalf("third terminal publication=%v", err)
	}
	latest, ok := manager.committed[authorityKey{domain: actorrouting.DomainPlayer, shard: 0}]
	if !ok {
		t.Fatal("latest committed authority missing")
	}
	fenceD := managerFence(latest)
	if _, err := manager.Transfer(context.Background(), TransferRequest{TransitionID: "evicted-replay-d", Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: fenceD, NewHolderID: "player-e", NewHolderBootID: "evicted-e", Endpoint: "player-e:9000", RequestedTTL: time.Second, Mode: TransferModeCrashTakeover, TakeoverProof: takeoverProofFor(fenceD)}); err != nil {
		t.Fatalf("fourth terminal publication=%v", err)
	}
	claimsBefore := store.claim
	replayed, err := manager.Claim(context.Background(), firstReq)
	if !errors.Is(err, ErrTransitionMemoExpired) || !reflect.DeepEqual(replayed, first) || store.claim != claimsBefore {
		t.Fatalf("evicted transition acquired a new generation: replay=%+v err=%v claims=%d before=%d", replayed, err, store.claim, claimsBefore)
	}
	conflict := firstReq
	conflict.NewHolderID = "different-digest-holder"
	if _, err := manager.Claim(context.Background(), conflict); !errors.Is(err, ErrTransitionReconcile) {
		t.Fatalf("evicted same-id different digest was not rejected: %v", err)
	}
	reloaded, err := NewRouteAssignmentManager(ManagerOptions{PlacementExclusions: newAllowAllPlacementExclusions(),
		Store: store, Publisher: publisher, InitialSnapshot: manager.CurrentSnapshot(), AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager-a", RouteManagerBootID: "route-manager-boot", Now: func() time.Time { return time.UnixMilli(1_900_000_000_000) }, MemoLimit: 1, TombstoneStore: tombstones,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reloaded.Claim(context.Background(), firstReq); !errors.Is(err, ErrTransitionMemoExpired) {
		t.Fatalf("reloaded manager forgot expired transition id: %v", err)
	}
}

func TestRouteAssignmentManagerRejectsAddressAndGenerationOverflowBeforeStore(t *testing.T) {
	manager, store := newManagerForTest(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}))
	if _, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "out-of-range", Domain: actorrouting.DomainPlayer, VirtualShard: 9, NewHolderID: "holder", NewHolderBootID: "boot", Endpoint: "holder:9000", RequestedTTL: time.Second}); !errors.Is(err, ErrAuthorityAddress) || store.claim != 0 {
		t.Fatalf("out-of-range authority reached store: err=%v claims=%d", err, store.claim)
	}
	manager.snapshot.GlobalRevision = ^uint64(0)
	if _, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "global-overflow", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "holder", NewHolderBootID: "boot", Endpoint: "holder:9000", RequestedTTL: time.Second}); !errors.Is(err, ErrAuthorityOverflow) || store.claim != 0 {
		t.Fatalf("global overflow reached store: err=%v claims=%d", err, store.claim)
	}
}

func TestRouteAssignmentManagerReconcilesRenewedAuthorityDeadlinesInOneCompleteSnapshot(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	var published []actorrouting.RouteSnapshot
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
		return nil
	}), ManagerOptions{Now: func() time.Time { return now }})

	claim := func(domain actorrouting.RouteDomain, transitionID, holderID, bootID, endpoint string) authoritystore.ShardWriteAuthority {
		t.Helper()
		result, err := manager.Claim(context.Background(), ClaimRequest{
			TransitionID: transitionID, Domain: domain, VirtualShard: 0,
			NewHolderID: holderID, NewHolderBootID: bootID, Endpoint: endpoint, RequestedTTL: time.Second,
		})
		if err != nil {
			t.Fatalf("claim %s: %v", domain, err)
		}
		return result.Authority
	}
	player := claim(actorrouting.DomainPlayer, "deadline-player", "player-refresh", "player-refresh-boot", "player-refresh:9000")
	farm := claim(actorrouting.DomainFarm, "deadline-farm", "farm-refresh", "farm-refresh-boot", "farm-refresh:9001")
	before := manager.CurrentSnapshot()
	beforePublishes := len(published)

	for _, current := range []authoritystore.ShardWriteAuthority{player, farm} {
		result, err := store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
			Caller:                 authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: current.HolderID, BootID: current.HolderBootID},
			AuthorityLayoutVersion: "auth-v1", Domain: current.Domain, VirtualShard: current.VirtualShard,
			ExpectedFence: managerFence(current), RequestedTTLms: int64((5 * time.Second) / time.Millisecond),
		})
		if err != nil {
			t.Fatalf("renew %s: %v", current.Domain, err)
		}
		if result.Authority.LeaseUntilMs <= current.LeaseUntilMs {
			t.Fatalf("renew did not advance %s deadline", current.Domain)
		}
	}

	batchReader := &countingBatchAuthorityReader{store: store}
	manager.reader = batchReader
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := manager.ReconcileRenewedAuthorityDeadlines(ctx, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Changed || result.ChangedShards != 2 || result.SnapshotRevision != before.GlobalRevision+1 || len(published) != beforePublishes+1 {
		t.Fatalf("refresh=%+v publishes=%d before=%d", result, len(published), beforePublishes)
	}
	if batchReader.batches != 1 || batchReader.singles != 0 {
		t.Fatalf("deadline reads batches=%d singles=%d, want 1/0", batchReader.batches, batchReader.singles)
	}
	after := manager.CurrentSnapshot()
	if after.GlobalRevision != before.GlobalRevision+1 || after.Player.Routes[0].LeaseUntilMs != now.Add(5*time.Second).UnixMilli() || after.Farm.Routes[0].LeaseUntilMs != now.Add(5*time.Second).UnixMilli() {
		t.Fatalf("deadlines were not atomically projected: before=%+v after=%+v", before, after)
	}
	if after.Player.Routes[0].ShardRevision != before.Player.Routes[0].ShardRevision || after.Player.Routes[0].ExpectedWriterEpoch != before.Player.Routes[0].ExpectedWriterEpoch || after.Player.Routes[0].ExpectedLeaseEpoch != before.Player.Routes[0].ExpectedLeaseEpoch ||
		after.Farm.Routes[0].ShardRevision != before.Farm.Routes[0].ShardRevision || after.Farm.Routes[0].ExpectedWriterEpoch != before.Farm.Routes[0].ExpectedWriterEpoch || after.Farm.Routes[0].ExpectedLeaseEpoch != before.Farm.Routes[0].ExpectedLeaseEpoch {
		t.Fatal("deadline refresh changed a generation field")
	}

	beforePublishes = len(published)
	noop, err := manager.ReconcileRenewedAuthorityDeadlines(ctx, 2*time.Second)
	if err != nil || noop.Changed || noop.ChangedShards != 0 || len(published) != beforePublishes {
		t.Fatalf("far-from-lead pass was not a no-op: result=%+v err=%v publishes=%d", noop, err, len(published))
	}
	if batchReader.batches != 1 || batchReader.singles != 0 {
		t.Fatalf("far-from-lead pass unexpectedly read Authority: batches=%d singles=%d", batchReader.batches, batchReader.singles)
	}
}

func TestRouteAssignmentManagerDeadlineRefreshCancellationProvesNoPublicationAttempt(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}), ManagerOptions{Now: func() time.Time { return now }})
	claim := func(domain actorrouting.RouteDomain, transitionID, holder, boot, endpoint string) authoritystore.ShardWriteAuthority {
		t.Helper()
		result, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: transitionID, Domain: domain, VirtualShard: 0, NewHolderID: holder, NewHolderBootID: boot, Endpoint: endpoint, RequestedTTL: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		return result.Authority
	}
	player := claim(actorrouting.DomainPlayer, "deadline-cancel-player", "deadline-cancel-player", "deadline-cancel-player-boot", "deadline-cancel-player:9000")
	farm := claim(actorrouting.DomainFarm, "deadline-cancel-farm", "deadline-cancel-farm", "deadline-cancel-farm-boot", "deadline-cancel-farm:9001")
	for _, current := range []authoritystore.ShardWriteAuthority{player, farm} {
		if _, err := store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
			Caller: authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: current.HolderID, BootID: current.HolderBootID}, AuthorityLayoutVersion: "auth-v1",
			Domain: current.Domain, VirtualShard: current.VirtualShard, ExpectedFence: managerFence(current), RequestedTTLms: int64((5 * time.Second) / time.Millisecond),
		}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	manager.reader = AuthorityReaderFunc(func(readCtx context.Context, domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool, error) {
		calls++
		current, found, err := store.ReadShardWriteAuthority(readCtx, domain, shard)
		if calls == 1 {
			cancel()
		}
		return current, found, err
	})
	result, err := manager.ReconcileRenewedAuthorityDeadlines(ctx, 2*time.Second)
	if !errors.Is(err, context.Canceled) || result.FailureStage != "cancelled" || result.PublicationAttempted || result.Changed || calls != 1 {
		t.Fatalf("cancelled refresh result=%+v err=%v reads=%d", result, err, calls)
	}
}

func TestRouteAssignmentManagerAuthorityDelegatedDeadlineRefreshDoesNotPublishSnapshot(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	var published []actorrouting.RouteSnapshot
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
		return nil
	}), ManagerOptions{Now: func() time.Time { return now }, DisableDeadlineSnapshotPublication: true})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{
		TransitionID: "authority-delegated-deadline-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		NewHolderID: "authority-delegated-player", NewHolderBootID: "authority-delegated-boot", Endpoint: "authority-delegated:9000", RequestedTTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	before := manager.CurrentSnapshot()
	beforePublishes := len(published)
	if _, err := store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller:                 authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: claimed.Authority.HolderID, BootID: claimed.Authority.HolderBootID},
		AuthorityLayoutVersion: "auth-v1", Domain: claimed.Authority.Domain, VirtualShard: claimed.Authority.VirtualShard,
		ExpectedFence: managerFence(claimed.Authority), RequestedTTLms: int64((5 * time.Second) / time.Millisecond),
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := manager.ReconcileRenewedAuthorityDeadlines(ctx, 2*time.Second)
	if err != nil || !result.Changed || result.Published {
		t.Fatalf("authority-delegated refresh=%+v err=%v", result, err)
	}
	if got := manager.CurrentSnapshot(); !reflect.DeepEqual(got, before) || len(published) != beforePublishes {
		t.Fatalf("deadline-only refresh published Assignment Snapshot: before=%+v after=%+v publishes=%d/%d", before, got, len(published), beforePublishes)
	}
	projected, ok := manager.LeaseProjection(actorrouting.DomainPlayer, 0)
	if !ok || projected.LeaseUntilMs <= claimed.Authority.LeaseUntilMs {
		t.Fatalf("independent lease projection missing or not advanced: projected=%+v ok=%t claimed=%+v", projected, ok, claimed.Authority)
	}
}

func TestRouteAssignmentManagerDeadlineRefreshIsolatesExpiredAuthorityFromHealthyShards(t *testing.T) {
	base := time.UnixMilli(1_900_000_000_000)
	now := base
	var published []actorrouting.RouteSnapshot
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
		return nil
	}), ManagerOptions{Now: func() time.Time { return now }})

	claim := func(domain actorrouting.RouteDomain, transitionID, holderID, bootID, endpoint string) authoritystore.ShardWriteAuthority {
		t.Helper()
		result, err := manager.Claim(context.Background(), ClaimRequest{
			TransitionID: transitionID, Domain: domain, VirtualShard: 0,
			NewHolderID: holderID, NewHolderBootID: bootID, Endpoint: endpoint, RequestedTTL: time.Second,
		})
		if err != nil {
			t.Fatalf("claim %s: %v", domain, err)
		}
		return result.Authority
	}
	crashed := claim(actorrouting.DomainPlayer, "stalled-player", "stalled-player", "stalled-player-boot", "stalled-player:9000")
	healthy := claim(actorrouting.DomainFarm, "healthy-farm", "healthy-farm", "healthy-farm-boot", "healthy-farm:9001")
	renewed, err := store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller:                 authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: healthy.HolderID, BootID: healthy.HolderBootID},
		AuthorityLayoutVersion: "auth-v1", Domain: healthy.Domain, VirtualShard: healthy.VirtualShard,
		ExpectedFence: managerFence(healthy), RequestedTTLms: int64((5 * time.Second) / time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}

	// The crashed holder stops renewing, so its durable Authority lapses while
	// every other shard keeps a live lease.
	now = base.Add(2 * time.Second)
	before := manager.CurrentSnapshot()
	beforePublishes := len(published)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := manager.ReconcileRenewedAuthorityDeadlines(ctx, 2*time.Second)
	if err != nil {
		t.Fatalf("one expired shard failed the whole pass: %v", err)
	}
	if !result.Changed || result.ChangedShards != 1 || result.StalledShards != 1 || len(published) != beforePublishes+1 {
		t.Fatalf("refresh=%+v publishes=%d before=%d", result, len(published), beforePublishes)
	}
	after := manager.CurrentSnapshot()
	if after.Farm.Routes[0].LeaseUntilMs != renewed.Authority.LeaseUntilMs {
		t.Fatalf("healthy shard deadline was not refreshed: %+v", after.Farm.Routes[0])
	}
	if after.Player.Routes[0].LeaseUntilMs != before.Player.Routes[0].LeaseUntilMs ||
		after.Player.Routes[0].ShardRevision != before.Player.Routes[0].ShardRevision {
		t.Fatalf("stalled shard was projected: before=%+v after=%+v", before.Player.Routes[0], after.Player.Routes[0])
	}
	if crashed.HolderID != after.Holders[after.Player.Routes[0].HolderIndex].InstanceID {
		t.Fatal("stalled shard changed holder")
	}
}

func TestRouteAssignmentManagerDeadlineRefreshFailsClosedWithoutPartialPublication(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	publishCalls := 0
	var corrupt bool
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		publishCalls++
		return nil
	}), ManagerOptions{Now: func() time.Time { return now }})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "deadline-fail-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "deadline-fail", NewHolderBootID: "deadline-fail-boot", Endpoint: "deadline-fail:9000", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller:                 authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: claimed.Authority.HolderID, BootID: claimed.Authority.HolderBootID},
		AuthorityLayoutVersion: "auth-v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(claimed.Authority), RequestedTTLms: int64((5 * time.Second) / time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	manager.reader = AuthorityReaderFunc(func(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool, error) {
		current, found, readErr := store.ReadShardWriteAuthority(ctx, domain, shard)
		if corrupt && found && domain == actorrouting.DomainPlayer {
			current.LeaseUntilMs = claimed.Authority.LeaseUntilMs - 1
		}
		return current, found, readErr
	})
	before := manager.CurrentSnapshot()
	beforePublishes := publishCalls
	corrupt = true
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if result, refreshErr := manager.ReconcileRenewedAuthorityDeadlines(ctx, 2*time.Second); !errors.Is(refreshErr, ErrDeadlineProjection) || result.Changed || result.PublicationAttempted || publishCalls != beforePublishes || !reflect.DeepEqual(manager.CurrentSnapshot(), before) {
		t.Fatalf("regression partially published: result=%+v err=%v publishes=%d", result, refreshErr, publishCalls)
	}

	corrupt = false
	manager.reader = AuthorityReaderFunc(func(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error) {
		return authoritystore.ShardWriteAuthority{}, false, errors.New("read unavailable")
	})
	if result, refreshErr := manager.ReconcileRenewedAuthorityDeadlines(ctx, 2*time.Second); !errors.Is(refreshErr, ErrDeadlineProjection) || result.Changed || result.PublicationAttempted || publishCalls != beforePublishes || !reflect.DeepEqual(manager.CurrentSnapshot(), before) {
		t.Fatalf("read failure partially published: result=%+v err=%v publishes=%d", result, refreshErr, publishCalls)
	}
}

func TestRouteAssignmentManagerDeadlineRefreshExcludesAssigningAndRequiresBoundedContext(t *testing.T) {
	reads, publishes := 0, 0
	manager, _ := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		publishes++
		return nil
	}), ManagerOptions{AuthorityReader: AuthorityReaderFunc(func(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error) {
		reads++
		return authoritystore.ShardWriteAuthority{}, false, errors.New("ASSIGNING route was read")
	})})
	reads = 0 // Constructor bootstrap-absence checks are outside the refresh pass.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := manager.ReconcileRenewedAuthorityDeadlines(ctx, time.Hour)
	if err != nil || result.Changed || result.ChangedShards != 0 || reads != 0 || publishes != 0 {
		t.Fatalf("ASSIGNING refresh result=%+v err=%v reads=%d publishes=%d", result, err, reads, publishes)
	}
	if _, err := manager.ReconcileRenewedAuthorityDeadlines(context.Background(), time.Second); !errors.Is(err, ErrInvalidManager) {
		t.Fatalf("unbounded context err=%v", err)
	}
	cancelled, stop := context.WithTimeout(context.Background(), time.Second)
	stop()
	if _, err := manager.ReconcileRenewedAuthorityDeadlines(cancelled, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context err=%v", err)
	}
}

func TestRouteAssignmentManagerDeadlineRefreshPublishFailureLeavesLocalSnapshotUnchanged(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	failRefresh := false
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		if failRefresh {
			return errors.New("snapshot CAS conflict")
		}
		return nil
	}), ManagerOptions{Now: func() time.Time { return now }})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "deadline-publish-failure", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "deadline-publish", NewHolderBootID: "deadline-publish-boot", Endpoint: "deadline-publish:9000", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: claimed.Authority.HolderID, BootID: claimed.Authority.HolderBootID}, AuthorityLayoutVersion: "auth-v1",
		Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority), RequestedTTLms: int64((5 * time.Second) / time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	before := manager.CurrentSnapshot()
	failRefresh = true
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, refreshErr := manager.ReconcileRenewedAuthorityDeadlines(ctx, 2*time.Second)
	if !errors.Is(refreshErr, ErrDeadlineProjection) || result.Changed || !result.PublicationAttempted || !reflect.DeepEqual(manager.CurrentSnapshot(), before) {
		t.Fatalf("failed publish changed local Snapshot: result=%+v err=%v", result, refreshErr)
	}
}

func TestRouteAssignmentManagerDeadlineRefreshRetriesCommittedNotificationWithoutNewRevision(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	failNotification := false
	var published []actorrouting.RouteSnapshot
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(_ context.Context, _ routecontrol.NestedControlOperationToken, snapshot actorrouting.RouteSnapshot) error {
		published = append(published, actorrouting.CloneRouteSnapshot(snapshot))
		if failNotification {
			failNotification = false
			return committedNotificationTestError{}
		}
		return nil
	}), ManagerOptions{Now: func() time.Time { return now }})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "deadline-notify-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "deadline-notify", NewHolderBootID: "deadline-notify-boot", Endpoint: "deadline-notify:9000", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: claimed.Authority.HolderID, BootID: claimed.Authority.HolderBootID}, AuthorityLayoutVersion: "auth-v1",
		Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: managerFence(claimed.Authority), RequestedTTLms: int64((5 * time.Second) / time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	failNotification = true
	committed, publishErr := manager.ReconcileRenewedAuthorityDeadlines(ctx, 2*time.Second)
	if !errors.Is(publishErr, ErrDeadlineProjection) || !committed.Changed || committed.SnapshotRevision != 3 || manager.CurrentSnapshot().GlobalRevision != 3 {
		t.Fatalf("committed notification failure result=%+v err=%v current=%d", committed, publishErr, manager.CurrentSnapshot().GlobalRevision)
	}
	replayed, err := manager.ReconcileRenewedAuthorityDeadlines(ctx, 2*time.Second)
	if err != nil || replayed.Changed || !replayed.NotificationReplayed || replayed.SnapshotRevision != 3 || manager.CurrentSnapshot().GlobalRevision != 3 {
		t.Fatalf("notification replay result=%+v err=%v current=%d", replayed, err, manager.CurrentSnapshot().GlobalRevision)
	}
	if len(published) != 3 || published[1].GlobalRevision != 3 || !reflect.DeepEqual(published[1], published[2]) {
		t.Fatalf("notification retry emitted a new revision: %+v", published)
	}
}

func TestRouteAssignmentManagerDeadlineRefreshNoopReadDoesNotBlockConcurrentTransition(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	publishCalls := 0
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		publishCalls++
		return nil
	}), ManagerOptions{Now: func() time.Time { return now }})
	if _, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "deadline-noop-player", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "deadline-noop", NewHolderBootID: "deadline-noop-boot", Endpoint: "deadline-noop:9000", RequestedTTL: time.Second}); err != nil {
		t.Fatal(err)
	}
	readStarted := make(chan struct{})
	releaseRead := make(chan struct{})
	var once sync.Once
	manager.reader = AuthorityReaderFunc(func(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool, error) {
		once.Do(func() { close(readStarted) })
		select {
		case <-releaseRead:
		case <-ctx.Done():
			return authoritystore.ShardWriteAuthority{}, false, ctx.Err()
		}
		return store.ReadShardWriteAuthority(ctx, domain, shard)
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	refreshDone := make(chan struct {
		result DeadlineProjectionResult
		err    error
	}, 1)
	go func() {
		result, refreshErr := manager.ReconcileRenewedAuthorityDeadlines(ctx, 2*time.Second)
		refreshDone <- struct {
			result DeadlineProjectionResult
			err    error
		}{result: result, err: refreshErr}
	}()
	<-readStarted

	beforeTransitionPublishes := publishCalls
	if _, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "deadline-noop-farm", Domain: actorrouting.DomainFarm, VirtualShard: 0, NewHolderID: "deadline-noop-farm", NewHolderBootID: "deadline-noop-farm-boot", Endpoint: "deadline-noop-farm:9001", RequestedTTL: time.Second}); err != nil {
		t.Fatalf("Authority read wave blocked concurrent generation mutation: %v", err)
	}
	if publishCalls != beforeTransitionPublishes+1 {
		t.Fatalf("concurrent transition publishes=%d, want %d", publishCalls, beforeTransitionPublishes+1)
	}
	close(releaseRead)
	refresh := <-refreshDone
	if refresh.err != nil || refresh.result.Changed || refresh.result.ChangedShards != 0 {
		t.Fatalf("no-op refresh result=%+v err=%v", refresh.result, refresh.err)
	}
	if publishCalls != beforeTransitionPublishes+1 {
		t.Fatalf("no-op refresh published a Snapshot: publishes=%d", publishCalls)
	}
	current := manager.CurrentSnapshot()
	if current.GlobalRevision != 3 || current.Player.Routes[0].State != actorrouting.ShardStateActive ||
		current.Farm.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("concurrent transition/no-op refresh result is incomplete: %+v", current)
	}
}

func TestRouteAssignmentManagerDeadlineRefreshReadDoesNotBlockTransitionAndRevalidatesBeforePublish(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	publishCalls := 0
	manager, store := newManagerForTestWithOptions(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		publishCalls++
		return nil
	}), ManagerOptions{Now: func() time.Time { return now }})
	claimed, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "deadline-race-player", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "deadline-race", NewHolderBootID: "deadline-race-boot", Endpoint: "deadline-race:9000", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.RenewShardAuthority(context.Background(), authoritystore.RenewShardAuthorityRequest{
		Caller:                 authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: claimed.Authority.HolderID, BootID: claimed.Authority.HolderBootID},
		AuthorityLayoutVersion: "auth-v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: managerFence(claimed.Authority), RequestedTTLms: int64((5 * time.Second) / time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	readStarted := make(chan struct{})
	releaseRead := make(chan struct{})
	var once sync.Once
	manager.reader = AuthorityReaderFunc(func(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool, error) {
		once.Do(func() { close(readStarted) })
		select {
		case <-releaseRead:
		case <-ctx.Done():
			return authoritystore.ShardWriteAuthority{}, false, ctx.Err()
		}
		return store.ReadShardWriteAuthority(ctx, domain, shard)
	})
	beforePublishes := publishCalls
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	refreshDone := make(chan struct {
		result DeadlineProjectionResult
		err    error
	}, 1)
	go func() {
		result, refreshErr := manager.ReconcileRenewedAuthorityDeadlines(ctx, 2*time.Second)
		refreshDone <- struct {
			result DeadlineProjectionResult
			err    error
		}{result: result, err: refreshErr}
	}()
	<-readStarted

	beforeConcurrentTransition := publishCalls
	if _, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "deadline-race-farm", Domain: actorrouting.DomainFarm, VirtualShard: 0, NewHolderID: "deadline-race-farm", NewHolderBootID: "deadline-race-farm-boot", Endpoint: "deadline-race-farm:9001", RequestedTTL: time.Second}); err != nil {
		t.Fatalf("Authority read wave blocked concurrent generation mutation: %v", err)
	}
	if publishCalls != beforeConcurrentTransition+1 {
		t.Fatalf("concurrent transition publishes=%d, want %d", publishCalls, beforeConcurrentTransition+1)
	}
	// CurrentSnapshot takes only the state mutex. It must remain locally
	// readable while the exact Authority network read is blocked.
	if current := manager.CurrentSnapshot(); current.GlobalRevision != 3 {
		t.Fatalf("state mutex was not independently readable: revision=%d", current.GlobalRevision)
	}
	close(releaseRead)
	refresh := <-refreshDone
	if !errors.Is(refresh.err, ErrDeadlineProjection) || refresh.result.FailureStage != "revalidate" ||
		refresh.result.Changed || refresh.result.ChangedShards != 1 {
		t.Fatalf("stale refresh result=%+v err=%v", refresh.result, refresh.err)
	}
	if publishCalls != beforePublishes+1 {
		t.Fatalf("stale refresh published a Snapshot: publishes=%d before=%d", publishCalls, beforePublishes)
	}

	retried, err := manager.ReconcileRenewedAuthorityDeadlines(ctx, 2*time.Second)
	if err != nil || !retried.Changed || retried.ChangedShards != 1 {
		t.Fatalf("retried refresh result=%+v err=%v", retried, err)
	}
	current := manager.CurrentSnapshot()
	if current.GlobalRevision != 4 || current.Player.Routes[0].LeaseUntilMs != now.Add(5*time.Second).UnixMilli() || current.Farm.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("revalidated transition/refresh result is incomplete: %+v", current)
	}
}

func TestRouteAssignmentManagerCompactsUnreferencedHolderRefsDeterministically(t *testing.T) {
	manager, _ := newManagerForTest(t, testSnapshotPublisherFunc(func(context.Context, routecontrol.NestedControlOperationToken, actorrouting.RouteSnapshot) error {
		return nil
	}))
	claimedResult, err := manager.Claim(context.Background(), ClaimRequest{TransitionID: "compact-claim", Domain: actorrouting.DomainPlayer, VirtualShard: 0, NewHolderID: "compact-initial", NewHolderBootID: "compact-initial-boot", Endpoint: "compact-initial:9000", RequestedTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	claimed := claimedResult.Authority
	current := manager.CurrentSnapshot()
	for i := 0; i < 8; i++ {
		fence := managerFence(claimed)
		id := fmt.Sprintf("compact-transfer-%d", i)
		result, err := manager.Transfer(context.Background(), TransferRequest{TransitionID: id, Domain: actorrouting.DomainPlayer, VirtualShard: 0, ExpectedFence: fence, NewHolderID: fmt.Sprintf("compact-holder-%d", i), NewHolderBootID: fmt.Sprintf("compact-boot-%d", i), Endpoint: fmt.Sprintf("compact-%d:9000", i), RequestedTTL: time.Second, Mode: TransferModeCrashTakeover, TakeoverProof: takeoverProofFor(fence)})
		if err != nil {
			t.Fatal(err)
		}
		claimed = result.Authority
		current = manager.CurrentSnapshot()
		if len(current.Holders) > 2 {
			t.Fatalf("unreferenced holder refs grew without compaction at %d: %d", i, len(current.Holders))
		}
	}
	if len(current.Holders) != 2 {
		t.Fatalf("unexpected deterministic holder table size=%d snapshot=%+v", len(current.Holders), current)
	}
}

// ReleasePlacementExclusionForOwner produces every row of the section 8.3
// outcome table. It is the reference classifier the A1R Lua must agree with,
// so the ordering matters: boot rollover is checked before absence, because a
// record missing for the boot we named while another boot is registered means
// the process we excluded is provably gone, whereas plain absence proves only
// that the record is not there right now.
func (f *fakePlacementExclusions) ReleasePlacementExclusionForOwner(_ context.Context, token routecontrol.NestedControlOperationToken, domain actorrouting.RouteDomain, holderID, bootID string, release routecontrol.PlacementExclusionRelease) (routecontrol.PlacementExclusionResult, error) {
	if !token.Issued() {
		return routecontrol.PlacementExclusionResult{}, routecontrol.ErrPlacementExclusionInvalid
	}
	if release.OwnerPlanID == "" || release.ExpectedRevision == 0 {
		return routecontrol.PlacementExclusionResult{}, routecontrol.ErrPlacementExclusionInvalid
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++

	outcome := func(o routecontrol.PlacementReleaseOutcome, state routecontrol.PlacementExclusion, committed bool) (routecontrol.PlacementExclusionResult, error) {
		return routecontrol.PlacementExclusionResult{Committed: committed, State: state, Outcome: o}, nil
	}

	state, found := f.states[placementTestKey(domain, holderID, bootID)]
	if !found {
		for key, other := range f.states {
			if strings.HasPrefix(key, string(domain)+"\x00"+holderID+"\x00") && other.BootID != bootID {
				return outcome(routecontrol.PlacementReleaseByBootRollover, other, false)
			}
		}
		return outcome(routecontrol.PlacementReleaseNotFound, routecontrol.PlacementExclusion{}, false)
	}
	if err := routecontrol.ValidatePlacementExclusion(state); err != nil {
		return outcome(routecontrol.PlacementReleaseMalformed, state, false)
	}
	if !state.Excluded {
		// Whether this owner released it or it was never excluded, the caller's
		// goal already holds. Distinguishing the two would give a retry a way
		// to fail on a state it was trying to reach.
		return outcome(routecontrol.PlacementReleaseAlreadyReleased, state, false)
	}
	if state.OwnerPlanID != release.OwnerPlanID {
		return outcome(routecontrol.PlacementReleaseOwnerMismatch, state, false)
	}
	if state.Revision != release.ExpectedRevision {
		return outcome(routecontrol.PlacementReleaseRevisionConflict, state, false)
	}
	state.Excluded = false
	state.OwnerPlanID = ""
	state.Revision++
	f.states[placementTestKey(domain, holderID, bootID)] = state
	return outcome(routecontrol.PlacementReleaseReleased, state, true)
}
