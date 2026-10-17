package authoritycomposition

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	remoteroute "farmgame/internal/actorrouting/remote"
	"farmgame/internal/holderidentity"
	"farmgame/internal/routecontrol/transport"

	"google.golang.org/grpc/connectivity"
)

type assignmentTestSource struct {
	snapshot   actorrouting.RouteSnapshot
	fetchErr   error
	fetchCount atomic.Int32
	watch      chan actorrouting.RouteSnapshot
}

type assignmentRolloverWatch struct {
	after   uint64
	updates chan actorrouting.RouteSnapshot
}

type countingShardAssignmentSource struct {
	provider      *actorrouting.DynamicAssignmentProvider
	snapshotCalls atomic.Int32
	lookupCalls   atomic.Int32
}

func (s *countingShardAssignmentSource) Snapshot() actorrouting.RouteSnapshot {
	s.snapshotCalls.Add(1)
	return s.provider.CurrentSnapshot()
}

func (s *countingShardAssignmentSource) LookupShard(domain actorrouting.RouteDomain, shard uint32) (actorrouting.ShardRouteSnapshot, error) {
	s.lookupCalls.Add(1)
	return s.provider.LookupShard(domain, shard)
}

type countingSnapshotOnlyAssignmentSource struct {
	snapshot      actorrouting.RouteSnapshot
	snapshotCalls atomic.Int32
}

func (s *countingSnapshotOnlyAssignmentSource) Snapshot() actorrouting.RouteSnapshot {
	s.snapshotCalls.Add(1)
	return actorrouting.CloneRouteSnapshot(s.snapshot)
}

type assignmentRolloverRequest struct {
	baseline remoteroute.ResyncBaseline
	respond  chan assignmentRolloverResponse
}

type assignmentRolloverResponse struct {
	snapshot actorrouting.RouteSnapshot
	err      error
}

type assignmentRolloverSource struct {
	initial     actorrouting.RouteSnapshot
	fetchCount  atomic.Int32
	watchCalls  chan assignmentRolloverWatch
	resyncCalls chan assignmentRolloverRequest
}

func TestSnapshotResolverUsesCoherentShardLookupWithoutFullSnapshotCopy(t *testing.T) {
	const seed = uint64(0x42)
	calc, err := actorrouting.NewCalculator("test-realm", actorrouting.HashProfile{
		Version: actorrouting.HashVersionXXH64V2, Seed: seed,
	}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := actorrouting.RouteSnapshot{
		Realm: "test-realm", GlobalRevision: 7,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: seed,
		Holders: []actorrouting.HolderRef{
			{InstanceID: "player-1", BootID: "boot-1", Endpoint: "player-1:9100", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-1", BootID: "farm-boot-1", Endpoint: "farm-1:9090", State: actorrouting.HolderStateActive, Weight: 1},
		},
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateActive,
			ShardRevision: 3, ExpectedWriterEpoch: 4, ExpectedLeaseEpoch: 5,
			LeaseUntilMs: time.Now().Add(time.Minute).UnixMilli(),
		}}},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 1, State: actorrouting.ShardStateActive,
			ShardRevision: 3, ExpectedWriterEpoch: 4, ExpectedLeaseEpoch: 5,
			LeaseUntilMs: time.Now().Add(time.Minute).UnixMilli(),
		}}},
	}
	provider, _, err := actorrouting.NewDynamicAssignmentProvider(snapshot, 1)
	if err != nil {
		t.Fatal(err)
	}
	fastSource := &countingShardAssignmentSource{provider: provider}
	resolver := &snapshotResolver{source: fastSource, calculator: calc}
	got, err := resolver.Resolve(context.Background(), actorrouting.Address{
		Realm: "test-realm", Kind: actorrouting.KindPlayer, ID: 99,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.HolderPod != "player-1" || got.SnapshotRevision != 7 || got.WriterEpoch != 4 || got.LeaseEpoch != 5 {
		t.Fatalf("assignment=%+v", got)
	}
	if fastSource.lookupCalls.Load() != 1 || fastSource.snapshotCalls.Load() != 0 {
		t.Fatalf("lookup calls=%d snapshot calls=%d", fastSource.lookupCalls.Load(), fastSource.snapshotCalls.Load())
	}

	fallback := &countingSnapshotOnlyAssignmentSource{snapshot: snapshot}
	fallbackResolver := &snapshotResolver{source: fallback, calculator: calc}
	fallbackGot, err := fallbackResolver.Resolve(context.Background(), actorrouting.Address{
		Realm: "test-realm", Kind: actorrouting.KindPlayer, ID: 99,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, fallbackGot) || fallback.snapshotCalls.Load() != 1 {
		t.Fatalf("fast=%+v fallback=%+v snapshot calls=%d", got, fallbackGot, fallback.snapshotCalls.Load())
	}
}

func newAssignmentRolloverSource(initial actorrouting.RouteSnapshot) *assignmentRolloverSource {
	return &assignmentRolloverSource{
		initial:     actorrouting.CloneRouteSnapshot(initial),
		watchCalls:  make(chan assignmentRolloverWatch, 2),
		resyncCalls: make(chan assignmentRolloverRequest, 1),
	}
}

func (s *assignmentRolloverSource) FetchFullSnapshot(context.Context) (actorrouting.RouteSnapshot, error) {
	s.fetchCount.Add(1)
	return actorrouting.CloneRouteSnapshot(s.initial), nil
}

func (s *assignmentRolloverSource) WatchFrom(ctx context.Context, revision uint64) (<-chan actorrouting.RouteSnapshot, error) {
	updates := make(chan actorrouting.RouteSnapshot)
	select {
	case s.watchCalls <- assignmentRolloverWatch{after: revision, updates: updates}:
		return updates, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *assignmentRolloverSource) FetchFullSnapshotForResync(ctx context.Context, baseline remoteroute.ResyncBaseline) (actorrouting.RouteSnapshot, error) {
	request := assignmentRolloverRequest{baseline: baseline, respond: make(chan assignmentRolloverResponse, 1)}
	select {
	case s.resyncCalls <- request:
	case <-ctx.Done():
		return actorrouting.RouteSnapshot{}, ctx.Err()
	}
	select {
	case response := <-request.respond:
		return actorrouting.CloneRouteSnapshot(response.snapshot), response.err
	case <-ctx.Done():
		return actorrouting.RouteSnapshot{}, ctx.Err()
	}
}

func (s *assignmentTestSource) FetchFullSnapshot(context.Context) (actorrouting.RouteSnapshot, error) {
	s.fetchCount.Add(1)
	if s.fetchErr != nil {
		return actorrouting.RouteSnapshot{}, s.fetchErr
	}
	return actorrouting.CloneRouteSnapshot(s.snapshot), nil
}

func (s *assignmentTestSource) WatchFrom(ctx context.Context, _ uint64) (<-chan actorrouting.RouteSnapshot, error) {
	if s.watch == nil {
		s.watch = make(chan actorrouting.RouteSnapshot)
	}
	return s.watch, nil
}

func TestAssignmentModeMatrixAndStrictParser(t *testing.T) {
	cases := []struct {
		raw     string
		want    AssignmentMode
		wantErr bool
	}{
		{raw: "", want: AssignmentModeStatic},
		{raw: "static", want: AssignmentModeStatic},
		{raw: "remote-watch", want: AssignmentModeRemoteWatch},
		{raw: "REMOTE-WATCH", want: AssignmentModeRemoteWatch},
		{raw: "true", wantErr: true},
		{raw: "watch", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			t.Setenv("ACTOR_ASSIGNMENT_MODE", tc.raw)
			got, err := AssignmentModeFromEnvironment()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("raw=%q unexpectedly parsed as %q", tc.raw, got)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("raw=%q got=%q err=%v want=%q", tc.raw, got, err, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		membership MembershipMode
		assignment AssignmentMode
		wantErr    bool
	}{
		{MembershipModeLegacy, AssignmentModeStatic, false},
		{MembershipModeComposed, AssignmentModeStatic, false},
		{MembershipModeComposed, AssignmentModeRemoteWatch, false},
		{MembershipModeLegacy, AssignmentModeRemoteWatch, true},
	} {
		err := ValidateModeCombination(tc.membership, tc.assignment)
		if (err != nil) != tc.wantErr {
			t.Fatalf("membership=%s assignment=%s err=%v wantErr=%v", tc.membership, tc.assignment, err, tc.wantErr)
		}
		wantHolder := tc.membership == MembershipModeComposed && tc.assignment == AssignmentModeRemoteWatch
		if got := HolderCompositionEnabled(tc.membership, tc.assignment); got != wantHolder {
			t.Fatalf("membership=%s assignment=%s HolderCompositionEnabled=%v want %v", tc.membership, tc.assignment, got, wantHolder)
		}
	}
}

func TestRemoteCompositionDefaultsCoverBoundedControllerRecreate(t *testing.T) {
	for _, key := range []string{
		"ACTOR_ROUTE_MAX_RESYNC_ATTEMPTS", "ACTOR_ROUTE_MAX_WATCH_FAILURES",
		"ACTOR_ROUTE_RETRY_BACKOFF", "ACTOR_ROUTE_MAX_RETRY_BACKOFF",
	} {
		t.Setenv(key, "")
	}
	options := RemoteAssignmentOptionsFromEnvironment()
	if options.MaxResyncAttempts != defaultRemoteResyncAttempts || options.MaxWatchFailures != defaultRemoteWatchFailures ||
		options.RetryBackoff != defaultRemoteRetryBackoff || options.MaxRetryBackoff != defaultRemoteMaxRetryBackoff ||
		!options.ContinuousReconnect {
		t.Fatalf("remote controller-recreate defaults=%+v", options)
	}
	// This lower bound excludes RPC duration and proves that a transient
	// multi-second Recreate outage cannot consume the retry owner in the old
	// three-attempt sub-second envelope.
	delay := options.RetryBackoff
	var backoffHorizon time.Duration
	for attempt := 1; attempt < options.MaxResyncAttempts; attempt++ {
		backoffHorizon += delay
		if delay < options.MaxRetryBackoff {
			delay *= 2
			if delay > options.MaxRetryBackoff {
				delay = options.MaxRetryBackoff
			}
		}
	}
	if backoffHorizon < 20*time.Second || backoffHorizon > 30*time.Second {
		t.Fatalf("bounded default reconnect horizon=%s, want [20s,30s]", backoffHorizon)
	}
}

func TestRemoteCompositionConfigurationHasHardBounds(t *testing.T) {
	t.Setenv("ACTOR_ROUTE_WATCH_BUFFER", "999999999")
	t.Setenv("ACTOR_ROUTE_MAX_RESYNC_ATTEMPTS", "999999999")
	t.Setenv("ACTOR_ROUTE_MAX_WATCH_FAILURES", "999999999")
	t.Setenv("ACTOR_ROUTE_RETRY_BACKOFF", "10m")
	t.Setenv("ACTOR_ROUTE_MAX_RETRY_BACKOFF", "10m")
	options := RemoteAssignmentOptionsFromEnvironment()
	if options.Buffer != maxRemoteAssignmentBuffer || options.MaxResyncAttempts != maxRemoteRetryAttempts || options.MaxWatchFailures != maxRemoteRetryAttempts {
		t.Fatalf("remote queue/retry bounds=%+v", options)
	}
	if options.RetryBackoff > maxRemoteTimeout || options.MaxRetryBackoff > maxRemoteTimeout {
		t.Fatalf("remote backoff bounds=%+v", options)
	}
	t.Setenv("ACTOR_ROUTE_MAX_ENDPOINTS", "999999999")
	if got := ActorRouteEndpointLimitFromEnvironment(); got != maxActorRouteEndpoints {
		t.Fatalf("endpoint limit=%d want %d", got, maxActorRouteEndpoints)
	}
	t.Setenv("ROUTE_CONTROL_DIAL_TIMEOUT", "10m")
	t.Setenv("ACTOR_ROUTE_INITIAL_FETCH_TIMEOUT", "10m")
	t.Setenv("ACTOR_ROUTE_STARTUP_TIMEOUT", "10m")
	if got := RemoteDialTimeoutFromEnvironment(); got != maxRemoteTimeout {
		t.Fatalf("dial timeout=%s want %s", got, maxRemoteTimeout)
	}
	if got := RemoteInitialFetchTimeoutFromEnvironment(); got != maxRemoteTimeout {
		t.Fatalf("initial fetch timeout=%s want %s", got, maxRemoteTimeout)
	}
	if got := RemoteStartupTimeoutFromEnvironment(); got != maxRemoteStartupTimeout {
		t.Fatalf("runtime startup timeout=%s want %s", got, maxRemoteStartupTimeout)
	}
	t.Setenv("ROUTE_CONTROL_DIAL_TIMEOUT", "1us")
	t.Setenv("ACTOR_ROUTE_INITIAL_FETCH_TIMEOUT", "1us")
	t.Setenv("ACTOR_ROUTE_STARTUP_TIMEOUT", "1us")
	if got := RemoteDialTimeoutFromEnvironment(); got != time.Millisecond {
		t.Fatalf("minimum dial timeout=%s want %s", got, time.Millisecond)
	}
	if got := RemoteInitialFetchTimeoutFromEnvironment(); got != time.Millisecond {
		t.Fatalf("minimum initial fetch timeout=%s want %s", got, time.Millisecond)
	}
	if got := RemoteStartupTimeoutFromEnvironment(); got != time.Millisecond {
		t.Fatalf("minimum runtime startup timeout=%s want %s", got, time.Millisecond)
	}
	t.Setenv("ACTOR_ROUTE_ENDPOINT_DIAL_TIMEOUT", "1us")
	t.Setenv("ACTOR_ROUTE_ENDPOINT_RETIRE_GRACE", "10m")
	endpointOptions := EndpointConnectionOptionsFromEnvironment()
	if endpointOptions.DialTimeout != time.Millisecond || endpointOptions.RetireGrace != maxRemoteTimeout {
		t.Fatalf("endpoint timeout bounds=%+v", endpointOptions)
	}
}

func TestRemoteRetiredBootLimitEnvironmentIsStrictAndBounded(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		present bool
		want    int
		wantErr bool
	}{
		{name: "unset default", want: defaultRemoteRetiredBootLimit},
		{name: "minimum", value: "1", present: true, want: 1},
		{name: "maximum", value: "64", present: true, want: maxRemoteRetiredBootLimit},
		{name: "empty present", value: "", present: true, wantErr: true},
		{name: "malformed", value: "eight", present: true, wantErr: true},
		{name: "zero", value: "0", present: true, wantErr: true},
		{name: "negative", value: "-1", present: true, wantErr: true},
		{name: "above maximum", value: "65", present: true, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := remoteRetiredBootLimitFromEnvironment(func(key string) (string, bool) {
				if key != "ROUTE_CONTROL_RETIRED_BOOT_LIMIT" {
					t.Fatalf("unexpected environment key %q", key)
				}
				return test.value, test.present
			})
			if test.wantErr {
				if err == nil {
					t.Fatalf("value=%q unexpectedly parsed as %d", test.value, got)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("value=%q got=%d err=%v want=%d", test.value, got, err, test.want)
			}
		})
	}
}

func TestRemoteSnapshotClientRejectsPresentInvalidRetiredBootLimitBeforeTLS(t *testing.T) {
	t.Setenv("ROUTE_CONTROL_RETIRED_BOOT_LIMIT", "0")
	client, err := NewRemoteSnapshotClientFromEnvironment(context.Background())
	if client != nil || err == nil || !strings.Contains(err.Error(), "ROUTE_CONTROL_RETIRED_BOOT_LIMIT") {
		t.Fatalf("client=%v err=%v", client, err)
	}
}

func TestRemoteCompositionHealthyWithNoActiveRoutes(t *testing.T) {
	fixture := newCompositionFixture(t)
	snapshot := actorrouting.CloneRouteSnapshot(fixture.snap)
	for index := range snapshot.Holders {
		snapshot.Holders[index].State = actorrouting.HolderStateAssigning
	}
	for index := range snapshot.Player.Routes {
		snapshot.Player.Routes[index].State = actorrouting.ShardStateAssigning
	}
	for index := range snapshot.Farm.Routes {
		snapshot.Farm.Routes[index].State = actorrouting.ShardStateDraining
	}
	source := &assignmentTestSource{snapshot: snapshot, watch: make(chan actorrouting.RouteSnapshot)}
	var connections sync.Map
	composition, err := NewRemoteAssignmentComposition(context.Background(), source, DefaultRemoteOptionsForTest())
	if err != nil {
		t.Fatal(err)
	}
	if err := composition.ConfigureEndpointConnections(EndpointConnectionOptions{
		DialTimeout: time.Second,
		RetireGrace: time.Second,
		Dial: func(_ context.Context, endpoint string) (ManagedEndpointConnection, error) {
			connection := newEndpointPoolTestConn()
			connections.Store(endpoint, connection)
			return connection, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- composition.Run(ctx) }()
	healthyCtx, healthyCancel := context.WithTimeout(context.Background(), time.Second)
	if err := composition.WaitHealthy(healthyCtx); err != nil {
		healthyCancel()
		cancel()
		_ = composition.Close()
		t.Fatalf("all-inactive complete snapshot never became ready: %v", err)
	}
	healthyCancel()
	if !composition.Healthy() || !composition.DataPlaneHealthy() {
		t.Fatal("complete all-inactive snapshot is not healthy")
	}
	if got := composition.AppliedSnapshotRevision(); got != snapshot.GlobalRevision {
		t.Fatalf("applied revision=%d want=%d", got, snapshot.GlobalRevision)
	}
	var changed *endpointPoolTestConn
	connections.Range(func(_, value any) bool {
		changed = value.(*endpointPoolTestConn)
		return false
	})
	changed.state.Store(int32(connectivity.TransientFailure))
	if !composition.Healthy() || composition.DataPlaneHealthy() {
		t.Fatal("connection transient poisoned control-plane health or left data-plane health open")
	}
	changed.state.Store(int32(connectivity.Idle))
	if !composition.Healthy() || composition.DataPlaneHealthy() {
		t.Fatal("idle connection poisoned control-plane health or reported data-plane ready while waking")
	}
	if changed.connects.Load() != 1 || !composition.DataPlaneHealthy() {
		t.Fatal("data-plane health did not recover after waking the idle pooled connection")
	}
	cancel()
	_ = composition.Close()
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("composition goroutines did not join")
	}
	if got := composition.AppliedSnapshotRevision(); got != 0 {
		t.Fatalf("closed composition retained applied revision=%d", got)
	}
}

func TestAppliedSnapshotRevisionForHolderPairsOneRevisionWithItsOwnDesiredShards(t *testing.T) {
	fixture := newCompositionFixture(t)
	source := &assignmentTestSource{snapshot: fixture.snap, watch: make(chan actorrouting.RouteSnapshot, 1)}
	composition, err := NewRemoteAssignmentComposition(context.Background(), source, DefaultRemoteOptionsForTest())
	if err != nil {
		t.Fatal(err)
	}
	if err := composition.ConfigureEndpointConnections(EndpointConnectionOptions{
		DialTimeout: time.Second, RetireGrace: time.Second,
		Dial: func(context.Context, string) (ManagedEndpointConnection, error) {
			return newEndpointPoolTestConn(), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- composition.Run(runCtx) }()
	defer func() {
		cancelRun()
		_ = composition.Close()
		select {
		case <-runDone:
		case <-time.After(time.Second):
			t.Fatal("composition goroutines did not join")
		}
	}()
	healthyCtx, cancelHealthy := context.WithTimeout(context.Background(), time.Second)
	if err := composition.WaitHealthy(healthyCtx); err != nil {
		cancelHealthy()
		t.Fatal(err)
	}
	cancelHealthy()

	revision, desired := composition.AppliedSnapshotRevisionForHolder(actorrouting.DomainFarm, "farm-pod-1", "farm-boot-1")
	if revision != fixture.snap.GlobalRevision || desired != 1 {
		t.Fatalf("local holder revision=%d desired=%d want revision=%d desired=1", revision, desired, fixture.snap.GlobalRevision)
	}
	revision, desired = composition.AppliedSnapshotRevisionForHolder(actorrouting.DomainFarm, "farm-pod-1", "farm-boot-2")
	if revision != fixture.snap.GlobalRevision || desired != 0 {
		t.Fatalf("replaced boot revision=%d desired=%d want revision=%d desired=0", revision, desired, fixture.snap.GlobalRevision)
	}
	revision, desired = composition.AppliedSnapshotRevisionForHolder(actorrouting.DomainPlayer, "farm-pod-1", "farm-boot-1")
	if revision != fixture.snap.GlobalRevision || desired != 0 {
		t.Fatalf("foreign domain revision=%d desired=%d want revision=%d desired=0", revision, desired, fixture.snap.GlobalRevision)
	}
	if revision, desired := composition.AppliedSnapshotRevisionForHolder(actorrouting.DomainFarm, "", ""); revision != 0 || desired != 0 {
		t.Fatalf("unidentified holder revision=%d desired=%d", revision, desired)
	}
}

func TestAssignmentCompositionObserveShardUsesExactHotResolverAndHealthRevision(t *testing.T) {
	fixture := newCompositionFixture(t)
	fixture.snap.Holders = append(
		fixture.snap.Holders,
		actorrouting.HolderRef{
			InstanceID: "farm-worker-a",
			BootID:     "farm-worker-a-boot",
			Endpoint:   "farm-worker-a:9201",
			State:      actorrouting.HolderStateActive,
			Weight:     1,
		},
	)
	fixture.snap.FarmWorker = actorrouting.DomainSnapshot{
		ShardCount: 1,
		Routes: []actorrouting.ShardRoute{{
			HolderIndex:         2,
			State:               actorrouting.ShardStateActive,
			ShardRevision:       1,
			ExpectedWriterEpoch: 1,
			ExpectedLeaseEpoch:  1,
			LeaseUntilMs:        time.Now().Add(time.Minute).UnixMilli(),
		}},
	}
	source := &assignmentTestSource{snapshot: fixture.snap, watch: make(chan actorrouting.RouteSnapshot, 1)}
	composition, err := NewRemoteAssignmentComposition(context.Background(), source, DefaultRemoteOptionsForTest())
	if err != nil {
		t.Fatal(err)
	}
	if err := composition.ConfigureEndpointConnections(EndpointConnectionOptions{
		DialTimeout: time.Second, RetireGrace: time.Second,
		Dial: func(context.Context, string) (ManagedEndpointConnection, error) {
			return newEndpointPoolTestConn(), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- composition.Run(runCtx) }()
	healthyCtx, cancelHealthy := context.WithTimeout(context.Background(), time.Second)
	if err := composition.WaitHealthy(healthyCtx); err != nil {
		cancelHealthy()
		cancelRun()
		t.Fatal(err)
	}
	cancelHealthy()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	observed, err := composition.ObserveShard(ctx, fixture.snap.Realm, actorrouting.DomainPlayer, 0)
	cancel()
	route := fixture.snap.Player.Routes[0]
	holder := fixture.snap.Holders[route.HolderIndex]
	if err != nil || observed.Realm != fixture.snap.Realm || observed.Domain != actorrouting.DomainPlayer ||
		observed.VirtualShard != 0 || observed.HolderID != holder.InstanceID || observed.HolderBootID != holder.BootID ||
		observed.LogicEndpoint != holder.Endpoint || observed.State != actorrouting.ShardStateActive ||
		observed.AssignmentRevision != route.ShardRevision || observed.WriterEpoch != route.ExpectedWriterEpoch ||
		observed.LeaseEpoch != route.ExpectedLeaseEpoch || observed.LeaseUntilMs != route.LeaseUntilMs ||
		observed.SnapshotRevision != fixture.snap.GlobalRevision {
		t.Fatalf("observation=%+v err=%v", observed, err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	observed, err = composition.ObserveShard(
		ctx,
		fixture.snap.Realm,
		actorrouting.DomainFarmWorker,
		0,
	)
	cancel()
	workerRoute := fixture.snap.FarmWorker.Routes[0]
	workerHolder := fixture.snap.Holders[workerRoute.HolderIndex]
	if err != nil ||
		observed.Domain != actorrouting.DomainFarmWorker ||
		observed.HolderID != workerHolder.InstanceID ||
		observed.LogicEndpoint != workerHolder.Endpoint ||
		observed.AssignmentRevision != workerRoute.ShardRevision {
		t.Fatalf("worker observation=%+v err=%v", observed, err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	_, err = composition.ObserveShard(ctx, "different-realm", actorrouting.DomainPlayer, 0)
	cancel()
	if err == nil {
		t.Fatal("realm mismatch observation passed")
	}

	next := actorrouting.CloneRouteSnapshot(fixture.snap)
	next.GlobalRevision++
	next.Player.Routes[0].ShardRevision++
	watchCtx, cancelWatch := context.WithTimeout(context.Background(), time.Second)
	updates, err := composition.Provider.WatchFrom(watchCtx, fixture.snap.GlobalRevision)
	if err != nil {
		cancelWatch()
		t.Fatal(err)
	}
	source.watch <- next
	select {
	case update := <-updates:
		if update.Revision != next.GlobalRevision {
			cancelWatch()
			t.Fatalf("provider barrier revision=%d want=%d", update.Revision, next.GlobalRevision)
		}
	case <-watchCtx.Done():
		cancelWatch()
		t.Fatal("provider revision barrier timed out")
	}
	cancelWatch()
	advancedCtx, cancelAdvanced := context.WithTimeout(context.Background(), time.Second)
	if err := composition.WaitHealthy(advancedCtx); err != nil {
		cancelAdvanced()
		t.Fatal(err)
	}
	cancelAdvanced()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	observed, err = composition.ObserveShard(ctx, next.Realm, actorrouting.DomainPlayer, 0)
	cancel()
	if err != nil || observed.SnapshotRevision != next.GlobalRevision || observed.AssignmentRevision != next.Player.Routes[0].ShardRevision {
		t.Fatalf("advanced observation=%+v err=%v", observed, err)
	}

	cancelRun()
	_ = composition.Close()
	<-runDone
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	_, err = composition.ObserveShard(ctx, next.Realm, actorrouting.DomainPlayer, 0)
	cancel()
	if err == nil {
		t.Fatal("unhealthy composition observation passed")
	}
}

func TestAssignmentCompositionHealthIsIndependentOfUnrelatedExpiredShard(t *testing.T) {
	fixture := newCompositionFixture(t)
	fixture.snap.Player.Routes[0].LeaseUntilMs = time.Now().Add(-time.Second).UnixMilli()
	fixture.snap.Farm.Routes[0].LeaseUntilMs = time.Now().Add(time.Minute).UnixMilli()
	source := &assignmentTestSource{snapshot: fixture.snap, watch: make(chan actorrouting.RouteSnapshot, 1)}
	composition, err := NewRemoteAssignmentComposition(context.Background(), source, DefaultRemoteOptionsForTest())
	if err != nil {
		t.Fatal(err)
	}
	if err := composition.ConfigureEndpointConnections(EndpointConnectionOptions{
		DialTimeout: time.Second, RetireGrace: time.Second,
		Dial: func(context.Context, string) (ManagedEndpointConnection, error) {
			return newEndpointPoolTestConn(), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- composition.Run(runCtx) }()
	healthyCtx, cancelHealthy := context.WithTimeout(context.Background(), time.Second)
	if err := composition.WaitHealthy(healthyCtx); err != nil {
		cancelHealthy()
		cancelRun()
		t.Fatal(err)
	}
	cancelHealthy()
	if !composition.Healthy() {
		t.Fatal("one expired shard poisoned process health")
	}
	if _, err := composition.Resolver.Resolve(context.Background(), actorrouting.Address{
		Realm: fixture.snap.Realm, Kind: actorrouting.KindPlayer, ID: 1,
	}); !errors.Is(err, actorrouting.ErrLeaseExpired) {
		t.Fatalf("expired shard resolve err=%v want=%v", err, actorrouting.ErrLeaseExpired)
	}
	if _, err := composition.Resolver.Resolve(context.Background(), actorrouting.Address{
		Realm: fixture.snap.Realm, Kind: actorrouting.KindFarm, ID: 1,
	}); err != nil {
		t.Fatalf("healthy unrelated shard resolve failed: %v", err)
	}
	cancelRun()
	_ = composition.Close()
	<-runDone
}

func TestAssignmentCompositionDelegatedModeDoesNotUseSnapshotDeadlineForObservation(t *testing.T) {
	t.Setenv("ACTOR_ROUTE_LEASE_CHECK_MODE", string(actorrouting.LeaseCheckAuthorityDelegated))
	fixture := newCompositionFixture(t)
	fixture.snap.Player.Routes[0].LeaseUntilMs = time.Now().Add(-time.Second).UnixMilli()
	source := &assignmentTestSource{snapshot: fixture.snap, watch: make(chan actorrouting.RouteSnapshot, 1)}
	composition, err := NewRemoteAssignmentComposition(context.Background(), source, DefaultRemoteOptionsForTest())
	if err != nil {
		t.Fatal(err)
	}
	if !composition.LeaseAuthorityDelegated() {
		t.Fatal("delegated mode was not retained by AssignmentComposition")
	}
	if err := composition.ConfigureEndpointConnections(EndpointConnectionOptions{
		DialTimeout: time.Second, RetireGrace: time.Second,
		Dial: func(context.Context, string) (ManagedEndpointConnection, error) {
			return newEndpointPoolTestConn(), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- composition.Run(runCtx) }()
	healthyCtx, cancelHealthy := context.WithTimeout(context.Background(), time.Second)
	if err := composition.WaitHealthy(healthyCtx); err != nil {
		cancelHealthy()
		cancelRun()
		t.Fatal(err)
	}
	cancelHealthy()
	if _, err := composition.Resolver.Resolve(context.Background(), actorrouting.Address{Realm: fixture.snap.Realm, Kind: actorrouting.KindPlayer, ID: 1}); err != nil {
		t.Fatalf("delegated resolver rejected stale snapshot deadline: %v", err)
	}
	observed, err := composition.ObserveShard(context.Background(), fixture.snap.Realm, actorrouting.DomainPlayer, 0)
	if err != nil || observed.LeaseUntilMs != fixture.snap.Player.Routes[0].LeaseUntilMs {
		t.Fatalf("delegated observation=%+v err=%v", observed, err)
	}
	cancelRun()
	_ = composition.Close()
	<-runDone
}

func TestAssignmentCompositionObserveShardLeaseExpiresInsideHealthRevisionBracket(t *testing.T) {
	fixture := newCompositionFixture(t)
	source := &assignmentTestSource{snapshot: fixture.snap, watch: make(chan actorrouting.RouteSnapshot, 1)}
	composition, err := NewRemoteAssignmentComposition(context.Background(), source, DefaultRemoteOptionsForTest())
	if err != nil {
		t.Fatal(err)
	}
	if err := composition.ConfigureEndpointConnections(EndpointConnectionOptions{
		DialTimeout: time.Second, RetireGrace: time.Second,
		Dial: func(context.Context, string) (ManagedEndpointConnection, error) {
			return newEndpointPoolTestConn(), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- composition.Run(runCtx) }()
	healthyCtx, cancelHealthy := context.WithTimeout(context.Background(), time.Second)
	if err := composition.WaitHealthy(healthyCtx); err != nil {
		cancelHealthy()
		cancelRun()
		t.Fatal(err)
	}
	cancelHealthy()
	deadline := time.UnixMilli(fixture.snap.Player.Routes[0].LeaseUntilMs)
	clockCalls := 0
	composition.now = func() time.Time {
		clockCalls++
		if clockCalls == 1 {
			return deadline.Add(-time.Millisecond)
		}
		return deadline
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err = composition.ObserveShard(ctx, fixture.snap.Realm, actorrouting.DomainPlayer, 0)
	cancel()
	if err == nil || clockCalls < 2 {
		t.Fatalf("lease expiry inside bracket passed err=%v clock_calls=%d", err, clockCalls)
	}
	cancelRun()
	_ = composition.Close()
	<-runDone
}

func TestRemoteCompositionPumpTerminalDegradesAndJoinsAllComponents(t *testing.T) {
	fixture := newCompositionFixture(t)
	source := &assignmentTestSource{snapshot: fixture.snap, watch: make(chan actorrouting.RouteSnapshot, 1)}
	composition, err := NewRemoteAssignmentComposition(context.Background(), source, DefaultRemoteOptionsForTest())
	if err != nil {
		t.Fatal(err)
	}
	if err := composition.ConfigureEndpointConnections(EndpointConnectionOptions{
		DialTimeout: time.Second, RetireGrace: time.Second,
		Dial: func(context.Context, string) (ManagedEndpointConnection, error) {
			return newEndpointPoolTestConn(), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- composition.Run(ctx) }()
	healthyCtx, healthyCancel := context.WithTimeout(context.Background(), time.Second)
	if err := composition.WaitHealthy(healthyCtx); err != nil {
		healthyCancel()
		t.Fatal(err)
	}
	healthyCancel()

	invalid := actorrouting.CloneRouteSnapshot(fixture.snap)
	invalid.GlobalRevision++
	invalid.HashSeed++
	source.watch <- invalid
	select {
	case err := <-runDone:
		if err == nil || !errors.Is(err, actorrouting.ErrHashMigrationRequired) {
			t.Fatalf("composition terminal err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Pump terminal did not stop the composition")
	}
	if composition.TerminalError() == nil || composition.Healthy() {
		t.Fatalf("terminal composition error=%v healthy=%v", composition.TerminalError(), composition.Healthy())
	}
	if err := composition.Close(); err != nil {
		t.Fatal(err)
	}
	if err := composition.Close(); err != nil {
		t.Fatalf("idempotent composition Close: %v", err)
	}
	select {
	case <-composition.Done():
	default:
		t.Fatal("composition Done remained open after joined terminal exit")
	}
}

func TestApplierOwnershipAndTerminalJoinAreExactByAssignmentMode(t *testing.T) {
	fixture := newCompositionFixture(t)
	staticRuntime, err := NewRuntimeForMode(context.Background(),
		membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"), MembershipModeComposed)
	if err != nil {
		t.Fatal(err)
	}
	if !staticRuntime.ownsApplier {
		t.Fatal("static runtime must own its SnapshotApplier watch")
	}
	if err := staticRuntime.Close(); err != nil {
		t.Fatal(err)
	}

	source := &assignmentTestSource{snapshot: fixture.snap, watch: make(chan actorrouting.RouteSnapshot)}
	composition, err := NewRemoteAssignmentComposition(context.Background(), source, DefaultRemoteOptionsForTest())
	if err != nil {
		t.Fatal(err)
	}
	if err := composition.ConfigureEndpointConnections(EndpointConnectionOptions{
		DialTimeout: time.Second, RetireGrace: time.Second,
		Dial: func(context.Context, string) (ManagedEndpointConnection, error) {
			return newEndpointPoolTestConn(), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	opts := membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1")
	opts.AssignmentMode = AssignmentModeRemoteWatch
	opts.DeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-uid-1", WorkloadUID: "workload-uid", Ordinal: 0}
	remoteRuntime, err := NewRuntimeForModeWithAssignment(context.Background(), opts, MembershipModeComposed, composition)
	if err != nil {
		t.Fatal(err)
	}
	if remoteRuntime.ownsApplier {
		t.Fatal("remote runtime must leave the shared SnapshotApplier to AssignmentComposition")
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- composition.Run(runCtx) }()
	healthyCtx, cancelHealthy := context.WithTimeout(context.Background(), time.Second)
	if err := composition.WaitHealthy(healthyCtx); err != nil {
		cancelHealthy()
		cancelRun()
		_ = composition.Close()
		_ = remoteRuntime.Close()
		t.Fatal(err)
	}
	cancelHealthy()
	if got := composition.Applier.Duplicates.Load(); got != 1 {
		t.Fatalf("shared remote Applier bootstrap owners=%d want exactly 1", got)
	}

	cancelRun()
	if err := composition.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("AssignmentComposition did not join its Applier owner")
	}
	if err := remoteRuntime.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteRuntimeConstructionHonorsStartupContext(t *testing.T) {
	fixture := newCompositionFixture(t)
	composition, err := NewRemoteAssignmentComposition(context.Background(),
		&assignmentTestSource{snapshot: fixture.snap}, DefaultRemoteOptionsForTest())
	if err != nil {
		t.Fatal(err)
	}
	defer composition.Close()
	opts := membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1")
	opts.AssignmentMode = AssignmentModeRemoteWatch
	opts.DeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-uid-1", WorkloadUID: "workload-uid", Ordinal: 0}
	startupCtx, cancelStartup := context.WithCancel(context.Background())
	cancelStartup()
	runtime, err := NewRuntimeForModeWithAssignment(startupCtx, opts, MembershipModeComposed, composition)
	if runtime != nil || !errors.Is(err, context.Canceled) {
		if runtime != nil {
			_ = runtime.Close()
		}
		t.Fatalf("expired remote runtime startup context runtime=%v err=%v", runtime, err)
	}
}

func TestRemoteAssignmentCompositionFailsClosedBeforeProviderOnInitialFetchError(t *testing.T) {
	source := &assignmentTestSource{fetchErr: errors.New("source unavailable")}
	composition, err := NewRemoteAssignmentComposition(context.Background(), source, DefaultRemoteOptionsForTest())
	if err == nil || composition != nil {
		t.Fatalf("initial fetch failure composition=%v err=%v", composition, err)
	}
	if source.fetchCount.Load() != 1 {
		t.Fatalf("initial fetch count=%d want 1", source.fetchCount.Load())
	}
}

func TestRemoteAssignmentCompositionEnforcesEndpointBoundBeforeInstall(t *testing.T) {
	fixture := newCompositionFixture(t)
	t.Setenv("ACTOR_ROUTE_MAX_ENDPOINTS", "1")
	source := &assignmentTestSource{snapshot: fixture.snap}
	composition, err := NewRemoteAssignmentComposition(context.Background(), source, DefaultRemoteOptionsForTest())
	if err == nil || composition != nil {
		t.Fatalf("endpoint-bound composition=%v err=%v", composition, err)
	}
	if source.fetchCount.Load() != 1 {
		t.Fatalf("endpoint-bound fetch count=%d want 1", source.fetchCount.Load())
	}
}

func TestRemoteAssignmentCompositionUsesOneProviderAndInstallsWatchSnapshots(t *testing.T) {
	fixture := newCompositionFixture(t)
	source := &assignmentTestSource{snapshot: fixture.snap, watch: make(chan actorrouting.RouteSnapshot, 1)}
	composition, err := NewRemoteAssignmentComposition(context.Background(), source, DefaultRemoteOptionsForTest())
	if err != nil {
		t.Fatal(err)
	}
	defer composition.Close()
	if composition.Provider == nil || composition.Resolver == nil || composition.Applier == nil || composition.Pump == nil {
		t.Fatalf("incomplete composition: %+v", composition)
	}
	if got := source.fetchCount.Load(); got != 1 {
		t.Fatalf("startup fetch count=%d want 1", got)
	}
	if _, err := composition.Resolver.Resolve(context.Background(), actorrouting.Address{Realm: fixture.snap.Realm, Kind: actorrouting.KindPlayer, ID: 1}); err != nil {
		t.Fatalf("local resolve failed: %v", err)
	}
	if got := source.fetchCount.Load(); got != 1 {
		t.Fatalf("local resolve triggered remote fetch count=%d", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- composition.Pump.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for composition.Pump.State() != "WATCHING" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	next := actorrouting.CloneRouteSnapshot(fixture.snap)
	next.GlobalRevision = fixture.snap.GlobalRevision + 1
	source.watch <- next
	for composition.Provider.Snapshot().GlobalRevision != next.GlobalRevision && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := composition.Provider.Snapshot().GlobalRevision; got != next.GlobalRevision {
		t.Fatalf("watch snapshot revision=%d want %d", got, next.GlobalRevision)
	}
	cancel()
	_ = composition.Close()
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("remote Pump did not stop after close")
	}
}

func TestAssignmentCompositionRemainsHealthyAfterRestartAwareRollover(t *testing.T) {
	for _, higher := range []bool{false, true} {
		name := "equal"
		if higher {
			name = "higher"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newCompositionFixture(t)
			source := newAssignmentRolloverSource(fixture.snap)
			composition := newRolloverComposition(t, source)
			runCtx, cancelRun := context.WithCancel(context.Background())
			runDone := make(chan error, 1)
			go func() { runDone <- composition.Run(runCtx) }()

			watch0 := receiveAssignmentWatch(t, source)
			waitAssignmentHealthy(t, composition)
			close(watch0.updates)
			request := receiveAssignmentRollover(t, source)
			if request.baseline.Revision != fixture.snap.GlobalRevision ||
				!reflect.DeepEqual(request.baseline.Snapshot, fixture.snap) {
				t.Fatalf("rollover baseline revision=%d snapshot=%+v", request.baseline.Revision, request.baseline.Snapshot)
			}
			next := actorrouting.CloneRouteSnapshot(fixture.snap)
			if higher {
				next.GlobalRevision++
				next.Player.Routes[0].ShardRevision++
				next.Player.Routes[0].ExpectedWriterEpoch++
				next.Player.Routes[0].ExpectedLeaseEpoch++
				next.Player.Routes[0].LeaseUntilMs++
			}
			request.respond <- assignmentRolloverResponse{snapshot: next}
			watch1 := receiveAssignmentWatch(t, source)
			if watch0.after != fixture.snap.GlobalRevision || watch1.after != next.GlobalRevision {
				t.Fatalf("Watch revisions=%d/%d want=%d/%d", watch0.after, watch1.after, fixture.snap.GlobalRevision, next.GlobalRevision)
			}
			waitAssignmentHealthy(t, composition)
			if !composition.Healthy() || composition.TerminalError() != nil || composition.Provider.Snapshot().GlobalRevision != next.GlobalRevision {
				t.Fatalf("post-rollover healthy=%v terminal=%v revision=%d", composition.Healthy(), composition.TerminalError(), composition.Provider.Snapshot().GlobalRevision)
			}
			if source.fetchCount.Load() != 1 {
				t.Fatalf("composition performed ordinary resync fetches=%d", source.fetchCount.Load())
			}

			cancelRun()
			_ = composition.Close()
			select {
			case <-runDone:
			case <-time.After(time.Second):
				t.Fatal("composition did not join after rollover")
			}
		})
	}
}

func TestAssignmentCompositionEveryInvalidRolloverFailsClosed(t *testing.T) {
	tests := []struct {
		name     string
		rollover error
		response func(actorrouting.RouteSnapshot) actorrouting.RouteSnapshot
		want     error
	}{
		{name: "retired boot", rollover: transport.ErrEnvelopeBootRetired, want: transport.ErrEnvelopeBootRetired},
		{name: "rollback", rollover: transport.ErrBootRolloverRollback, want: transport.ErrBootRolloverRollback},
		{name: "equal mismatch", rollover: transport.ErrBootRolloverMismatch, want: transport.ErrBootRolloverMismatch},
		{name: "overlap", rollover: transport.ErrBootRolloverOverlap, want: transport.ErrBootRolloverOverlap},
		{name: "capacity", rollover: transport.ErrRetiredBootCapacity, want: transport.ErrRetiredBootCapacity},
		{name: "contract", rollover: transport.ErrBootRolloverContract, want: transport.ErrBootRolloverContract},
		{name: "controller", rollover: transport.ErrEnvelopeControllerMismatch, want: transport.ErrEnvelopeControllerMismatch},
		{name: "realm", rollover: transport.ErrEnvelopeRealmMismatch, want: transport.ErrEnvelopeRealmMismatch},
		{name: "topology", rollover: transport.ErrEnvelopeTopologyDrift, want: transport.ErrEnvelopeTopologyDrift},
		{
			name: "higher transition rejected after transport acceptance",
			response: func(snapshot actorrouting.RouteSnapshot) actorrouting.RouteSnapshot {
				snapshot.GlobalRevision++
				snapshot.HashSeed++
				return snapshot
			},
			want: actorrouting.ErrHashMigrationRequired,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCompositionFixture(t)
			source := newAssignmentRolloverSource(fixture.snap)
			composition := newRolloverComposition(t, source)
			runDone := make(chan error, 1)
			go func() { runDone <- composition.Run(context.Background()) }()

			watch := receiveAssignmentWatch(t, source)
			waitAssignmentHealthy(t, composition)
			close(watch.updates)
			request := receiveAssignmentRollover(t, source)
			response := assignmentRolloverResponse{err: test.rollover}
			if test.response != nil {
				response.snapshot = test.response(actorrouting.CloneRouteSnapshot(fixture.snap))
			}
			request.respond <- response
			select {
			case err := <-runDone:
				if !errors.Is(err, test.want) {
					t.Fatalf("composition terminal err=%v want=%v", err, test.want)
				}
			case <-time.After(time.Second):
				t.Fatal("invalid rollover did not terminate composition")
			}
			if composition.Healthy() || composition.TerminalError() == nil {
				t.Fatalf("invalid rollover healthy=%v terminal=%v", composition.Healthy(), composition.TerminalError())
			}
			if got := composition.Provider.Snapshot().GlobalRevision; got != fixture.snap.GlobalRevision {
				t.Fatalf("invalid rollover changed installed revision=%d want=%d", got, fixture.snap.GlobalRevision)
			}
			_ = composition.Close()
		})
	}
}

func newRolloverComposition(t *testing.T, source *assignmentRolloverSource) *AssignmentComposition {
	t.Helper()
	composition, err := NewRemoteAssignmentComposition(context.Background(), source, DefaultRemoteOptionsForTest())
	if err != nil {
		t.Fatal(err)
	}
	if err := composition.ConfigureEndpointConnections(EndpointConnectionOptions{
		DialTimeout: time.Second,
		RetireGrace: time.Second,
		Dial: func(context.Context, string) (ManagedEndpointConnection, error) {
			return newEndpointPoolTestConn(), nil
		},
	}); err != nil {
		_ = composition.Close()
		t.Fatal(err)
	}
	return composition
}

func receiveAssignmentWatch(t *testing.T, source *assignmentRolloverSource) assignmentRolloverWatch {
	t.Helper()
	select {
	case watch := <-source.watchCalls:
		return watch
	case <-time.After(time.Second):
		t.Fatal("composition Watch did not start")
		return assignmentRolloverWatch{}
	}
}

func receiveAssignmentRollover(t *testing.T, source *assignmentRolloverSource) assignmentRolloverRequest {
	t.Helper()
	select {
	case request := <-source.resyncCalls:
		return request
	case <-time.After(time.Second):
		t.Fatal("composition restart-aware resync did not start")
		return assignmentRolloverRequest{}
	}
}

func waitAssignmentHealthy(t *testing.T, composition *AssignmentComposition) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := composition.WaitHealthy(ctx); err != nil {
		t.Fatalf("composition did not become healthy: %v", err)
	}
}

func DefaultRemoteOptionsForTest() remoteroute.Options {
	return remoteroute.Options{Buffer: 4, MaxResyncAttempts: 1, MaxWatchFailures: 1, RetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond}
}
