package remote

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
)

type fetchFunc func(context.Context, uint64) (actorrouting.RouteSnapshot, error)
type watchFunc func(context.Context, uint64, uint64) (<-chan actorrouting.RouteSnapshot, error)

type fakeSource struct {
	fetchFn fetchFunc
	watchFn watchFunc

	fetchCalls atomic.Uint64
	watchCalls atomic.Uint64

	mu             sync.Mutex
	watchRevisions []uint64
}

func (s *fakeSource) FetchFullSnapshot(ctx context.Context) (actorrouting.RouteSnapshot, error) {
	call := s.fetchCalls.Add(1)
	if s.fetchFn == nil {
		return actorrouting.RouteSnapshot{}, errors.New("fake source: fetch not configured")
	}
	return s.fetchFn(ctx, call)
}

func (s *fakeSource) WatchFrom(ctx context.Context, revision uint64) (<-chan actorrouting.RouteSnapshot, error) {
	call := s.watchCalls.Add(1)
	s.mu.Lock()
	s.watchRevisions = append(s.watchRevisions, revision)
	s.mu.Unlock()
	if s.watchFn == nil {
		return nil, errors.New("fake source: watch not configured")
	}
	return s.watchFn(ctx, revision, call)
}

func (s *fakeSource) revisions() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.watchRevisions...)
}

type permissiveInstaller struct {
	mu       sync.Mutex
	snapshot actorrouting.RouteSnapshot
	calls    atomic.Uint64
}

type restartAwareFakeSource struct {
	*fakeSource
	resyncFn    func(context.Context, ResyncBaseline, uint64) (actorrouting.RouteSnapshot, error)
	resyncCalls atomic.Uint64
}

func (s *restartAwareFakeSource) FetchFullSnapshotForResync(ctx context.Context, baseline ResyncBaseline) (actorrouting.RouteSnapshot, error) {
	call := s.resyncCalls.Add(1)
	if s.resyncFn == nil {
		return actorrouting.RouteSnapshot{}, errors.New("restart-aware fake source: resync not configured")
	}
	return s.resyncFn(ctx, baseline, call)
}

func (i *permissiveInstaller) InstallFullSnapshot(_ context.Context, snapshot actorrouting.RouteSnapshot) error {
	i.calls.Add(1)
	i.mu.Lock()
	i.snapshot = actorrouting.CloneRouteSnapshot(snapshot)
	i.mu.Unlock()
	return nil
}

func TestRemoteCapabilitiesAreSeparateAndConstructorIsExplicit(t *testing.T) {
	provider, installer, err := actorrouting.NewRemoteAssignmentProvider(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := any(provider).(actorrouting.RemoteAssignmentInstaller); ok {
		t.Fatal("read provider can derive the Pump-only installer")
	}
	if _, ok := any(installer).(actorrouting.AssignmentProvider); ok {
		t.Fatal("installer exposes read provider capability")
	}
	if _, ok := any(installer).(interface {
		Publish(context.Context, actorrouting.RouteSnapshot) error
	}); ok {
		t.Fatal("installer exposes ordinary manager publisher capability")
	}
	if _, ok := any(installer).(interface {
		PublishAuthoritySnapshot(context.Context, actorrouting.RouteSnapshot) error
	}); ok {
		t.Fatal("installer exposes authority publisher capability")
	}
	for _, method := range []string{"InstallFullSnapshot"} {
		if _, exists := reflect.TypeOf(provider).MethodByName(method); exists {
			t.Fatalf("provider exports installer method %s", method)
		}
	}
	for _, method := range []string{"GetSnapshot", "Watch", "WatchFrom", "Publish", "PublishAuthoritySnapshot"} {
		if _, exists := reflect.TypeOf(installer).MethodByName(method); exists {
			t.Fatalf("installer exports unrelated method %s", method)
		}
	}

	source := &fakeSource{
		fetchFn: func(context.Context, uint64) (actorrouting.RouteSnapshot, error) {
			return testSnapshot(10), nil
		},
		watchFn: func(ctx context.Context, _ uint64, _ uint64) (<-chan actorrouting.RouteSnapshot, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	remoteProvider, pump, err := NewRemoteAssignmentProvider(source)
	if err != nil {
		t.Fatal(err)
	}
	if remoteProvider == nil || pump == nil {
		t.Fatal("explicit remote constructor returned nil capability")
	}
	if _, ok := any(pump.installer).(interface {
		Publish(context.Context, actorrouting.RouteSnapshot) error
	}); ok {
		t.Fatal("remote pump received a manager publisher")
	}
}

func TestRemoteProviderFailsClosedBeforeFirstSnapshotAndHasZeroRemoteHotPath(t *testing.T) {
	provider, installer, err := actorrouting.NewRemoteAssignmentProvider(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.GetSnapshot(context.Background()); !errors.Is(err, actorrouting.ErrNoAssignment) {
		t.Fatalf("uninitialized GetSnapshot err=%v", err)
	}
	if _, err := provider.WatchFrom(context.Background(), 0); !errors.Is(err, actorrouting.ErrNoAssignment) {
		t.Fatalf("uninitialized WatchFrom err=%v", err)
	}
	if _, err := actorrouting.NewStaticResolverFromProvider(context.Background(), provider); !errors.Is(err, actorrouting.ErrNoAssignment) {
		t.Fatalf("uninitialized resolver construction err=%v", err)
	}
	if got := provider.Snapshot(); !reflect.DeepEqual(got, actorrouting.RouteSnapshot{}) {
		t.Fatalf("uninitialized Snapshot=%+v", got)
	}

	source := &fakeSource{}
	pump, err := NewRemoteAssignmentPump(source, installer, Options{})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := testSnapshot(10)
	if err := installer.InstallFullSnapshot(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := provider.GetSnapshot(context.Background()); err != nil {
			t.Fatal(err)
		}
		_ = provider.Snapshot()
		_ = pump.Snapshot()
	}
	if source.fetchCalls.Load() != 0 || source.watchCalls.Load() != 0 {
		t.Fatalf("local provider hot path contacted source: fetch=%d watch=%d", source.fetchCalls.Load(), source.watchCalls.Load())
	}
}

func TestRemoteInstallerAllowsFirstArbitraryRevisionJumpAndSameGenerationActivation(t *testing.T) {
	provider, installer, err := actorrouting.NewRemoteAssignmentProvider(2)
	if err != nil {
		t.Fatal(err)
	}
	base := assigningSnapshot(10)
	if err := installer.InstallFullSnapshot(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	if got := provider.CurrentSnapshot().GlobalRevision; got != 10 {
		t.Fatalf("first remote revision=%d, want 10", got)
	}
	active := actorrouting.CloneRouteSnapshot(base)
	active.GlobalRevision = 12
	active.Player.Routes[0].State = actorrouting.ShardStateActive
	if err := installer.InstallFullSnapshot(context.Background(), active); err != nil {
		t.Fatalf("direct R10->R12 same-generation activation: %v", err)
	}
	got := provider.CurrentSnapshot()
	if got.GlobalRevision != 12 || got.Player.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("activation was not installed: %+v", got)
	}

	reverse := actorrouting.CloneRouteSnapshot(active)
	reverse.GlobalRevision = 13
	reverse.Player.Routes[0].State = actorrouting.ShardStateAssigning
	if err := installer.InstallFullSnapshot(context.Background(), reverse); !errors.Is(err, actorrouting.ErrSnapshotGenerationRollback) {
		t.Fatalf("same-generation ACTIVE->ASSIGNING err=%v", err)
	}
	if got := provider.CurrentSnapshot().GlobalRevision; got != 12 {
		t.Fatalf("rejected reverse transition changed revision to %d", got)
	}
}

func TestRemotePumpAllowsAssigningToActiveTargetHolderChange(t *testing.T) {
	provider, installer, err := actorrouting.NewRemoteAssignmentProvider(1)
	if err != nil {
		t.Fatal(err)
	}
	pump, err := NewRemoteAssignmentPump(&fakeSource{}, installer)
	if err != nil {
		t.Fatal(err)
	}

	placeholder := testSnapshot(10)
	placeholder.Player.Routes[0].State = actorrouting.ShardStateAssigning
	placeholder.Player.Routes[0].LeaseUntilMs = 0
	if err := pump.install(context.Background(), placeholder); err != nil {
		t.Fatal(err)
	}
	target := actorrouting.CloneRouteSnapshot(placeholder)
	target.GlobalRevision = 11
	target.Holders = append(target.Holders, actorrouting.HolderRef{
		InstanceID: "player-b", BootID: "player-b-boot", Endpoint: "player-b:9002",
		State: actorrouting.HolderStateActive, Weight: 1,
	})
	target.Player.Routes[0].HolderIndex = 2
	target.Player.Routes[0].State = actorrouting.ShardStateActive
	target.Player.Routes[0].LeaseUntilMs = time.Now().Add(time.Hour).UnixMilli()
	if err := pump.install(context.Background(), target); err != nil {
		t.Fatalf("initial placeholder activation with new holder: %v", err)
	}
	if got := provider.CurrentSnapshot(); got.GlobalRevision != 11 || got.Player.Routes[0].HolderIndex != 2 {
		t.Fatalf("initial activation did not install target holder: %+v", got.Player.Routes[0])
	}

	nonInitialProvider, nonInitialInstaller, err := actorrouting.NewRemoteAssignmentProvider(1)
	if err != nil {
		t.Fatal(err)
	}
	nonInitialPump, err := NewRemoteAssignmentPump(&fakeSource{}, nonInitialInstaller)
	if err != nil {
		t.Fatal(err)
	}
	nonInitial := assigningSnapshot(20)
	if err := nonInitialPump.install(context.Background(), nonInitial); err != nil {
		t.Fatal(err)
	}
	nonInitialTarget := actorrouting.CloneRouteSnapshot(nonInitial)
	nonInitialTarget.GlobalRevision = 21
	nonInitialTarget.Holders = append(nonInitialTarget.Holders, actorrouting.HolderRef{
		InstanceID: "player-b", BootID: "player-b-boot", Endpoint: "player-b:9002",
		State: actorrouting.HolderStateActive, Weight: 1,
	})
	nonInitialTarget.Player.Routes[0].HolderIndex = 2
	nonInitialTarget.Player.Routes[0].State = actorrouting.ShardStateActive
	nonInitialTarget.Player.Routes[0].LeaseUntilMs = time.Now().Add(time.Hour).UnixMilli()
	if err := nonInitialPump.install(context.Background(), nonInitialTarget); err != nil {
		t.Fatalf("planned migration activation with target holder: %v", err)
	}
	if got := nonInitialProvider.CurrentSnapshot(); got.GlobalRevision != 21 || got.Player.Routes[0].HolderIndex != 2 || got.Player.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("planned migration activation did not install target: %+v", got.Player.Routes[0])
	}
}

func TestRemotePumpInstallsFarmWorkerAssignmentTransition(t *testing.T) {
	provider, installer, err := actorrouting.NewRemoteAssignmentProvider(1)
	if err != nil {
		t.Fatal(err)
	}
	pump, err := NewRemoteAssignmentPump(&fakeSource{}, installer)
	if err != nil {
		t.Fatal(err)
	}
	placeholder := testSnapshot(30)
	placeholder.Holders = append(placeholder.Holders, actorrouting.HolderRef{
		InstanceID: "farm-worker-a",
		BootID:     "worker-a-boot",
		Endpoint:   "farm-worker-a:9201",
		State:      actorrouting.HolderStateActive,
		Weight:     1,
	})
	placeholder.FarmWorker = actorrouting.DomainSnapshot{
		ShardCount: 1,
		Routes: []actorrouting.ShardRoute{{
			HolderIndex:         2,
			State:               actorrouting.ShardStateAssigning,
			ShardRevision:       1,
			ExpectedWriterEpoch: 1,
			ExpectedLeaseEpoch:  1,
		}},
	}
	if err := pump.install(context.Background(), placeholder); err != nil {
		t.Fatal(err)
	}
	active := actorrouting.CloneRouteSnapshot(placeholder)
	active.GlobalRevision = 31
	active.FarmWorker.Routes[0].State = actorrouting.ShardStateActive
	active.FarmWorker.Routes[0].LeaseUntilMs = time.Now().Add(time.Hour).UnixMilli()
	if err := pump.install(context.Background(), active); err != nil {
		t.Fatalf("FARM_WORKER activation: %v", err)
	}
	got := provider.CurrentSnapshot()
	if got.GlobalRevision != 31 ||
		got.FarmWorker.Routes[0].State != actorrouting.ShardStateActive {
		t.Fatalf("worker snapshot=%+v", got.FarmWorker)
	}
	topologyDrift := actorrouting.CloneRouteSnapshot(active)
	topologyDrift.GlobalRevision = 32
	topologyDrift.FarmWorker.ShardCount = 0
	topologyDrift.FarmWorker.Routes = nil
	if err := pump.install(context.Background(), topologyDrift); !errors.Is(err, actorrouting.ErrHashMigrationRequired) {
		t.Fatalf("FARM_WORKER topology drift err=%v", err)
	}
}

func TestRemoteInstallerRejectsDuplicateStaleConflictHolderGenerationAndTopologyRollback(t *testing.T) {
	_, installer, err := actorrouting.NewRemoteAssignmentProvider(1)
	if err != nil {
		t.Fatal(err)
	}
	base := testSnapshot(10)
	if err := installer.InstallFullSnapshot(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	if err := installer.InstallFullSnapshot(context.Background(), actorrouting.CloneRouteSnapshot(base)); err != nil {
		t.Fatalf("exact duplicate err=%v", err)
	}

	conflict := actorrouting.CloneRouteSnapshot(base)
	conflict.Holders[0].Endpoint = "player-conflict:9000"
	if err := installer.InstallFullSnapshot(context.Background(), conflict); !errors.Is(err, actorrouting.ErrSnapshotRevisionConflict) {
		t.Fatalf("same-revision conflict err=%v", err)
	}

	stale := actorrouting.CloneRouteSnapshot(base)
	stale.GlobalRevision = 9
	if err := installer.InstallFullSnapshot(context.Background(), stale); !errors.Is(err, actorrouting.ErrStaleRoute) {
		t.Fatalf("stale callback err=%v", err)
	}

	holderRollback := actorrouting.CloneRouteSnapshot(base)
	holderRollback.GlobalRevision = 11
	holderRollback.Holders = append(holderRollback.Holders, actorrouting.HolderRef{
		InstanceID: "player-b", BootID: "player-b-boot", Endpoint: "player-b:9002",
		State: actorrouting.HolderStateActive, Weight: 1,
	})
	holderRollback.Player.Routes[0].HolderIndex = 2
	if err := installer.InstallFullSnapshot(context.Background(), holderRollback); !errors.Is(err, actorrouting.ErrSnapshotHolderGeneration) {
		t.Fatalf("holder identity rollback err=%v", err)
	}

	generationRollback := actorrouting.CloneRouteSnapshot(base)
	generationRollback.GlobalRevision = 11
	generationRollback.Player.Routes[0].ShardRevision = 0
	if err := installer.InstallFullSnapshot(context.Background(), generationRollback); !errors.Is(err, actorrouting.ErrInvalidSnapshot) {
		t.Fatalf("zero generation should fail shape validation, err=%v", err)
	}

	generationProvider, generationInstaller, err := actorrouting.NewRemoteAssignmentProvider(1)
	if err != nil {
		t.Fatal(err)
	}
	generationBase := testSnapshot(20)
	generationBase.Player.Routes[0].ShardRevision = 2
	generationBase.Player.Routes[0].ExpectedWriterEpoch = 2
	generationBase.Player.Routes[0].ExpectedLeaseEpoch = 2
	if err := generationInstaller.InstallFullSnapshot(context.Background(), generationBase); err != nil {
		t.Fatal(err)
	}
	generationRollback = actorrouting.CloneRouteSnapshot(generationBase)
	generationRollback.GlobalRevision = 21
	generationRollback.Player.Routes[0].ShardRevision = 1
	if err := generationInstaller.InstallFullSnapshot(context.Background(), generationRollback); !errors.Is(err, actorrouting.ErrSnapshotGenerationRollback) {
		t.Fatalf("generation rollback err=%v", err)
	}
	if got := generationProvider.CurrentSnapshot().GlobalRevision; got != 20 {
		t.Fatalf("generation rollback changed revision to %d", got)
	}

	topologyRollback := actorrouting.CloneRouteSnapshot(base)
	topologyRollback.GlobalRevision = 11
	topologyRollback.HashSeed++
	if err := installer.InstallFullSnapshot(context.Background(), topologyRollback); !errors.Is(err, actorrouting.ErrHashMigrationRequired) {
		t.Fatalf("topology rollback err=%v", err)
	}
}

func TestRemoteInstallerAtomicallyAppliesSameGenerationDeadlineExtensionAndRejectsRollback(t *testing.T) {
	provider, installer, err := actorrouting.NewRemoteAssignmentProvider(1)
	if err != nil {
		t.Fatal(err)
	}
	oldDeadline := time.Now().Add(75 * time.Millisecond).UnixMilli()
	base := testSnapshot(10)
	base.Player.Routes[0].LeaseUntilMs = oldDeadline
	base.Farm.Routes[0].LeaseUntilMs = time.Now().Add(time.Hour).UnixMilli()
	if err := installer.InstallFullSnapshot(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	extended := actorrouting.CloneRouteSnapshot(base)
	extended.GlobalRevision++
	extended.Player.Routes[0].LeaseUntilMs = time.Now().Add(time.Hour).UnixMilli()
	if err := installer.InstallFullSnapshot(context.Background(), extended); err != nil {
		t.Fatalf("same-generation deadline extension: %v", err)
	}
	installed := provider.CurrentSnapshot()
	if installed.GlobalRevision != extended.GlobalRevision || installed.Player.Routes[0].LeaseUntilMs != extended.Player.Routes[0].LeaseUntilMs || installed.Player.Routes[0].ShardRevision != base.Player.Routes[0].ShardRevision || installed.Player.Routes[0].ExpectedWriterEpoch != base.Player.Routes[0].ExpectedWriterEpoch || installed.Player.Routes[0].ExpectedLeaseEpoch != base.Player.Routes[0].ExpectedLeaseEpoch {
		t.Fatalf("deadline extension was not installed atomically: %+v", installed.Player.Routes[0])
	}
	rollback := actorrouting.CloneRouteSnapshot(extended)
	rollback.GlobalRevision++
	rollback.Player.Routes[0].LeaseUntilMs = oldDeadline
	if err := installer.InstallFullSnapshot(context.Background(), rollback); !errors.Is(err, actorrouting.ErrSnapshotGenerationRollback) {
		t.Fatalf("deadline rollback err=%v", err)
	}
	if got := provider.CurrentSnapshot(); got.GlobalRevision != extended.GlobalRevision || got.Player.Routes[0].LeaseUntilMs != extended.Player.Routes[0].LeaseUntilMs {
		t.Fatalf("deadline rollback changed installed Snapshot: %+v", got.Player.Routes[0])
	}

	wait := time.Until(time.UnixMilli(oldDeadline)) + 10*time.Millisecond
	if wait > 0 {
		time.Sleep(wait)
	}
	resolver, err := actorrouting.NewStaticResolver(provider.CurrentSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.Resolve(context.Background(), actorrouting.Address{Realm: base.Realm, Kind: actorrouting.KindPlayer, ID: 42}); err != nil {
		t.Fatalf("local Resolve failed after superseded deadline: %v", err)
	}
}

func TestRemotePumpBootstrapWatchJumpAndStaleCallback(t *testing.T) {
	base := testSnapshot(10)
	next := actorrouting.CloneRouteSnapshot(base)
	next.GlobalRevision = 12
	next.Player.Routes[0].ShardRevision++
	next.Player.Routes[0].ExpectedWriterEpoch++
	next.Player.Routes[0].ExpectedLeaseEpoch++
	next.Player.Routes[0].LeaseUntilMs++
	watchCh := make(chan actorrouting.RouteSnapshot, 2)
	source := &fakeSource{
		fetchFn: func(context.Context, uint64) (actorrouting.RouteSnapshot, error) {
			return base, nil
		},
		watchFn: func(ctx context.Context, _ uint64, _ uint64) (<-chan actorrouting.RouteSnapshot, error) {
			go func() {
				<-ctx.Done()
			}()
			return watchCh, nil
		},
	}
	provider, pump, err := NewRemoteAssignmentProvider(source, Options{Backoff: func(int) time.Duration { return 0 }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pump.Run(ctx) }()
	waitFor(t, time.Second, func() bool { return source.watchCalls.Load() == 1 })
	watchCh <- next
	waitFor(t, time.Second, func() bool { return provider.CurrentSnapshot().GlobalRevision == 12 })
	watchCh <- next
	waitFor(t, time.Second, func() bool { return pump.Duplicates.Load() == 1 })
	stale := actorrouting.CloneRouteSnapshot(next)
	stale.GlobalRevision = 11
	watchCh <- stale
	waitFor(t, time.Second, func() bool { return pump.StaleIgnored.Load() == 1 })
	if got := provider.CurrentSnapshot().GlobalRevision; got != 12 {
		t.Fatalf("stale callback changed local revision to %d", got)
	}
	revisions := source.revisions()
	if len(revisions) != 1 || revisions[0] != 10 {
		t.Fatalf("watch started from %v, want [10]", revisions)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("external cancellation err=%v", err)
	}
	if err := pump.Run(context.Background()); err != nil {
		t.Fatalf("Run after terminal context cancellation err=%v", err)
	}
	if source.fetchCalls.Load() != 1 {
		t.Fatalf("terminal CLOSED pump restarted remote fetch: fetches=%d", source.fetchCalls.Load())
	}
}

func TestRemotePumpWatchSnapshotContractErrorIsTerminal(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*actorrouting.RouteSnapshot)
		want   error
	}{
		{
			name: "topology change",
			mutate: func(snapshot *actorrouting.RouteSnapshot) {
				snapshot.GlobalRevision = 11
				snapshot.HashSeed++
			},
			want: actorrouting.ErrHashMigrationRequired,
		},
		{
			name: "same revision conflict",
			mutate: func(snapshot *actorrouting.RouteSnapshot) {
				snapshot.Holders[0].Endpoint = "player-conflict:9000"
			},
			want: actorrouting.ErrSnapshotRevisionConflict,
		},
		{
			name: "invalid snapshot shape",
			mutate: func(snapshot *actorrouting.RouteSnapshot) {
				snapshot.GlobalRevision = 11
				snapshot.Player.Routes = nil
			},
			want: actorrouting.ErrInvalidSnapshot,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := testSnapshot(10)
			legalFetch := actorrouting.CloneRouteSnapshot(base)
			legalFetch.GlobalRevision = 12
			legalFetch.Player.Routes[0].ShardRevision++
			legalFetch.Player.Routes[0].ExpectedWriterEpoch++
			legalFetch.Player.Routes[0].ExpectedLeaseEpoch++
			legalFetch.Player.Routes[0].LeaseUntilMs++

			watchCh := make(chan actorrouting.RouteSnapshot, 1)
			source := &fakeSource{
				fetchFn: func(_ context.Context, call uint64) (actorrouting.RouteSnapshot, error) {
					if call == 1 {
						return base, nil
					}
					return legalFetch, nil
				},
				watchFn: func(_ context.Context, _ uint64, _ uint64) (<-chan actorrouting.RouteSnapshot, error) {
					return watchCh, nil
				},
			}
			provider, pump, err := NewRemoteAssignmentProvider(source, Options{
				Backoff: func(int) time.Duration { return 0 },
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- pump.Run(ctx) }()
			waitFor(t, time.Second, func() bool { return source.watchCalls.Load() == 1 })

			invalid := actorrouting.CloneRouteSnapshot(base)
			test.mutate(&invalid)
			watchCh <- invalid

			var runErr error
			select {
			case runErr = <-done:
			case <-time.After(time.Second):
				t.Fatal("snapshot contract error was treated as watch loss and resync")
			}
			if !errors.Is(runErr, test.want) {
				t.Fatalf("run error=%v, want %v", runErr, test.want)
			}
			if source.fetchCalls.Load() != 1 {
				t.Fatalf("contract error triggered a second FetchFullSnapshot: fetches=%d", source.fetchCalls.Load())
			}
			if source.watchCalls.Load() != 1 || pump.Reconnects.Load() != 0 || pump.Resyncs.Load() != 0 {
				t.Fatalf("contract error entered reconnect path: watches=%d reconnects=%d resyncs=%d",
					source.watchCalls.Load(), pump.Reconnects.Load(), pump.Resyncs.Load())
			}
			if got := provider.CurrentSnapshot(); !reflect.DeepEqual(got, base) {
				t.Fatalf("contract error changed last legal local snapshot: got revision=%d want revision=%d",
					got.GlobalRevision, base.GlobalRevision)
			}
			if pump.State() != StateDegraded {
				t.Fatalf("state=%s, want %s", pump.State(), StateDegraded)
			}
		})
	}
}

func TestRemotePumpWatchLivenessFailuresAreBoundedForErrorNilAndClosed(t *testing.T) {
	tests := []struct {
		name  string
		watch watchFunc
	}{
		{
			name: "watch error",
			watch: func(context.Context, uint64, uint64) (<-chan actorrouting.RouteSnapshot, error) {
				return nil, errors.New("watch unavailable")
			},
		},
		{
			name: "nil channel",
			watch: func(context.Context, uint64, uint64) (<-chan actorrouting.RouteSnapshot, error) {
				return nil, nil
			},
		},
		{
			name: "closed channel",
			watch: func(context.Context, uint64, uint64) (<-chan actorrouting.RouteSnapshot, error) {
				ch := make(chan actorrouting.RouteSnapshot)
				close(ch)
				return ch, nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := testSnapshot(10)
			source := &fakeSource{
				fetchFn: func(context.Context, uint64) (actorrouting.RouteSnapshot, error) {
					return actorrouting.CloneRouteSnapshot(base), nil
				},
				watchFn: test.watch,
			}
			provider, pump, err := NewRemoteAssignmentProvider(source, Options{
				MaxResyncAttempts: 1,
				MaxWatchFailures:  2,
				Backoff:           func(int) time.Duration { return 0 },
			})
			if err != nil {
				t.Fatal(err)
			}
			err = pump.Run(context.Background())
			if !errors.Is(err, ErrWatchLivenessExhausted) {
				t.Fatalf("Run error=%v, want ErrWatchLivenessExhausted", err)
			}
			if source.watchCalls.Load() != 3 || source.fetchCalls.Load() != 3 {
				t.Fatalf("liveness calls not bounded: watch=%d fetch=%d, want 3/3",
					source.watchCalls.Load(), source.fetchCalls.Load())
			}
			if pump.Resyncs.Load() != 2 || pump.State() != StateDegraded {
				t.Fatalf("liveness state/counters: resyncs=%d state=%s", pump.Resyncs.Load(), pump.State())
			}
			if got := provider.CurrentSnapshot().GlobalRevision; got != 10 {
				t.Fatalf("duplicate resync changed local revision to %d", got)
			}
		})
	}
}

func TestRemotePumpWatchLivenessBackoffIsInterruptibleByContextAndClose(t *testing.T) {
	t.Run("context", func(t *testing.T) {
		source := &fakeSource{
			fetchFn: func(context.Context, uint64) (actorrouting.RouteSnapshot, error) {
				return testSnapshot(10), nil
			},
			watchFn: func(context.Context, uint64, uint64) (<-chan actorrouting.RouteSnapshot, error) {
				return nil, errors.New("watch unavailable")
			},
		}
		_, pump, err := NewRemoteAssignmentProvider(source, Options{
			MaxWatchFailures: 100,
			Backoff:          func(int) time.Duration { return time.Hour },
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- pump.Run(ctx) }()
		waitFor(t, time.Second, func() bool { return source.watchCalls.Load() == 1 && pump.State() == StateResyncing })
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("context cancellation error=%v", err)
		}
		if source.fetchCalls.Load() != 1 {
			t.Fatalf("context cancellation allowed resync fetch=%d", source.fetchCalls.Load())
		}
	})

	t.Run("close", func(t *testing.T) {
		source := &fakeSource{
			fetchFn: func(context.Context, uint64) (actorrouting.RouteSnapshot, error) {
				return testSnapshot(10), nil
			},
			watchFn: func(context.Context, uint64, uint64) (<-chan actorrouting.RouteSnapshot, error) {
				return nil, errors.New("watch unavailable")
			},
		}
		_, pump, err := NewRemoteAssignmentProvider(source, Options{
			MaxWatchFailures: 100,
			Backoff:          func(int) time.Duration { return time.Hour },
		})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- pump.Run(context.Background()) }()
		waitFor(t, time.Second, func() bool { return source.watchCalls.Load() == 1 && pump.State() == StateResyncing })
		if err := pump.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatalf("Close error=%v", err)
		}
		if source.fetchCalls.Load() != 1 || pump.State() != StateClosed {
			t.Fatalf("Close did not stop liveness retry: fetch=%d state=%s", source.fetchCalls.Load(), pump.State())
		}
	})
}

func TestRemotePumpValidWatchUpdateResetsLivenessBudget(t *testing.T) {
	base := testSnapshot(10)
	next := actorrouting.CloneRouteSnapshot(base)
	next.GlobalRevision = 12
	next.Player.Routes[0].ShardRevision++
	next.Player.Routes[0].ExpectedWriterEpoch++
	next.Player.Routes[0].ExpectedLeaseEpoch++
	next.Player.Routes[0].LeaseUntilMs++
	updateCh := make(chan actorrouting.RouteSnapshot, 1)
	source := &fakeSource{
		fetchFn: func(_ context.Context, call uint64) (actorrouting.RouteSnapshot, error) {
			if call == 1 || call == 2 {
				return base, nil
			}
			return next, nil
		},
		watchFn: func(ctx context.Context, _ uint64, call uint64) (<-chan actorrouting.RouteSnapshot, error) {
			switch call {
			case 1:
				return nil, errors.New("watch unavailable")
			case 2:
				updateCh <- next
				close(updateCh)
				return updateCh, nil
			default:
				ch := make(chan actorrouting.RouteSnapshot)
				go func() {
					<-ctx.Done()
					close(ch)
				}()
				return ch, nil
			}
		},
	}
	provider, pump, err := NewRemoteAssignmentProvider(source, Options{
		MaxResyncAttempts: 1,
		MaxWatchFailures:  1,
		Backoff:           func(int) time.Duration { return 0 },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pump.Run(ctx) }()
	waitFor(t, time.Second, func() bool {
		return provider.CurrentSnapshot().GlobalRevision == 12 && source.watchCalls.Load() == 3
	})
	if pump.State() != StateWatching || pump.Reconnects.Load() != 2 || pump.Resyncs.Load() != 2 {
		t.Fatalf("liveness strategy was not reset: state=%s reconnects=%d resyncs=%d",
			pump.State(), pump.Reconnects.Load(), pump.Resyncs.Load())
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation after reset error=%v", err)
	}
}

func TestRemotePumpSurvivesControllerOutageBeyondLegacyRetryBudget(t *testing.T) {
	base := testSnapshot(10)
	next := actorrouting.CloneRouteSnapshot(base)
	next.GlobalRevision = 11
	next.Player.Routes[0].LeaseUntilMs++
	next.Farm.Routes[0].LeaseUntilMs++
	source := &restartAwareFakeSource{fakeSource: &fakeSource{
		fetchFn: func(context.Context, uint64) (actorrouting.RouteSnapshot, error) {
			return base, nil
		},
		watchFn: func(ctx context.Context, _ uint64, call uint64) (<-chan actorrouting.RouteSnapshot, error) {
			ch := make(chan actorrouting.RouteSnapshot)
			if call == 1 {
				close(ch)
				return ch, nil
			}
			go func() {
				<-ctx.Done()
				close(ch)
			}()
			return ch, nil
		},
	}}
	source.resyncFn = func(_ context.Context, baseline ResyncBaseline, call uint64) (actorrouting.RouteSnapshot, error) {
		if baseline.Revision != 10 {
			return actorrouting.RouteSnapshot{}, fmt.Errorf("unexpected baseline revision %d", baseline.Revision)
		}
		if call <= 8 {
			return actorrouting.RouteSnapshot{}, errors.New("controller recreate in progress")
		}
		return next, nil
	}
	provider, pump, err := NewRemoteAssignmentProvider(source, Options{
		MaxResyncAttempts: 32,
		MaxWatchFailures:  32,
		Backoff:           func(int) time.Duration { return 0 },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pump.Run(ctx) }()
	waitFor(t, time.Second, func() bool {
		return source.resyncCalls.Load() == 9 && provider.CurrentSnapshot().GlobalRevision == 11 && pump.State() == StateWatching
	})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("pump cancellation after controller recovery err=%v", err)
	}
}

func TestRemotePumpContinuousReconnectSurvivesMultipleExhaustedBursts(t *testing.T) {
	base := testSnapshot(10)
	next := actorrouting.CloneRouteSnapshot(base)
	next.GlobalRevision = 11
	next.Player.Routes[0].LeaseUntilMs++
	next.Farm.Routes[0].LeaseUntilMs++
	source := &restartAwareFakeSource{fakeSource: &fakeSource{
		fetchFn: func(context.Context, uint64) (actorrouting.RouteSnapshot, error) {
			return base, nil
		},
		watchFn: func(ctx context.Context, _ uint64, call uint64) (<-chan actorrouting.RouteSnapshot, error) {
			ch := make(chan actorrouting.RouteSnapshot)
			if call == 1 {
				close(ch)
				return ch, nil
			}
			go func() {
				<-ctx.Done()
				close(ch)
			}()
			return ch, nil
		},
	}}
	source.resyncFn = func(_ context.Context, baseline ResyncBaseline, call uint64) (actorrouting.RouteSnapshot, error) {
		if baseline.Revision != 10 {
			return actorrouting.RouteSnapshot{}, fmt.Errorf("unexpected baseline revision %d", baseline.Revision)
		}
		if call <= 5 {
			return actorrouting.RouteSnapshot{}, errors.New("controller still unavailable")
		}
		return next, nil
	}
	provider, pump, err := NewRemoteAssignmentProvider(source, Options{
		MaxResyncAttempts:   2,
		MaxWatchFailures:    2,
		ContinuousReconnect: true,
		Backoff:             func(int) time.Duration { return 0 },
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pump.Run(ctx) }()
	waitFor(t, time.Second, func() bool {
		return source.resyncCalls.Load() == 6 && provider.CurrentSnapshot().GlobalRevision == 11 && pump.State() == StateWatching
	})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("pump cancellation after continuous reconnect err=%v", err)
	}
}

func TestRemotePumpStartupFetchFailureLeavesProviderFailClosed(t *testing.T) {
	startupErr := errors.New("startup fetch failed")
	source := &fakeSource{
		fetchFn: func(context.Context, uint64) (actorrouting.RouteSnapshot, error) {
			return actorrouting.RouteSnapshot{}, startupErr
		},
	}
	provider, pump, err := NewRemoteAssignmentProvider(source)
	if err != nil {
		t.Fatal(err)
	}
	if err := pump.Run(context.Background()); !errors.Is(err, startupErr) {
		t.Fatalf("startup fetch err=%v", err)
	}
	if pump.State() != StateDegraded {
		t.Fatalf("startup failure state=%s, want %s", pump.State(), StateDegraded)
	}
	if _, err := provider.GetSnapshot(context.Background()); !errors.Is(err, actorrouting.ErrNoAssignment) {
		t.Fatalf("startup failure provider err=%v", err)
	}
}

func TestRemotePumpResyncsAfterWatchCloseAndKeepsLocalSnapshotOnExhaustion(t *testing.T) {
	base := testSnapshot(10)
	next := actorrouting.CloneRouteSnapshot(base)
	next.GlobalRevision = 12
	next.Player.Routes[0].ShardRevision++
	next.Player.Routes[0].ExpectedWriterEpoch++
	next.Player.Routes[0].ExpectedLeaseEpoch++
	next.Player.Routes[0].LeaseUntilMs++

	source := &fakeSource{
		fetchFn: func(_ context.Context, call uint64) (actorrouting.RouteSnapshot, error) {
			switch call {
			case 1:
				return base, nil
			case 2:
				return actorrouting.RouteSnapshot{}, errors.New("temporary fetch failure")
			default:
				return actorrouting.RouteSnapshot{}, errors.New("temporary fetch failure")
			}
		},
		watchFn: func(_ context.Context, _ uint64, call uint64) (<-chan actorrouting.RouteSnapshot, error) {
			if call == 1 {
				ch := make(chan actorrouting.RouteSnapshot)
				close(ch)
				return ch, nil
			}
			ch := make(chan actorrouting.RouteSnapshot)
			return ch, nil
		},
	}
	provider, pump, err := NewRemoteAssignmentProvider(source, Options{
		MaxResyncAttempts: 2,
		Backoff:           func(int) time.Duration { return 0 },
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- pump.Run(context.Background()) }()
	err = <-done
	if !errors.Is(err, ErrResyncExhausted) {
		t.Fatalf("resync exhaustion err=%v", err)
	}
	if pump.State() != StateDegraded {
		t.Fatalf("state after bounded resync=%s, want %s", pump.State(), StateDegraded)
	}
	if got := provider.CurrentSnapshot().GlobalRevision; got != 10 {
		t.Fatalf("failed resync discarded local revision %d", got)
	}
	if source.fetchCalls.Load() != 3 {
		t.Fatalf("fetch attempts=%d, want initial plus two bounded resync attempts", source.fetchCalls.Load())
	}
}

func TestRemotePumpResyncConvergesToLaterFullSnapshotAndUsesAppliedRevision(t *testing.T) {
	base := testSnapshot(10)
	next := actorrouting.CloneRouteSnapshot(base)
	next.GlobalRevision = 12
	next.Player.Routes[0].ShardRevision++
	next.Player.Routes[0].ExpectedWriterEpoch++
	next.Player.Routes[0].ExpectedLeaseEpoch++
	next.Player.Routes[0].LeaseUntilMs++
	resyncWatch := make(chan actorrouting.RouteSnapshot)
	source := &fakeSource{
		fetchFn: func(_ context.Context, call uint64) (actorrouting.RouteSnapshot, error) {
			if call == 1 {
				return base, nil
			}
			return next, nil
		},
		watchFn: func(ctx context.Context, _ uint64, call uint64) (<-chan actorrouting.RouteSnapshot, error) {
			if call == 1 {
				ch := make(chan actorrouting.RouteSnapshot)
				close(ch)
				return ch, nil
			}
			go func() {
				<-ctx.Done()
				close(resyncWatch)
			}()
			return resyncWatch, nil
		},
	}
	provider, pump, err := NewRemoteAssignmentProvider(source, Options{Backoff: func(int) time.Duration { return 0 }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pump.Run(ctx) }()
	waitFor(t, time.Second, func() bool {
		return provider.CurrentSnapshot().GlobalRevision == 12 && source.watchCalls.Load() == 2
	})
	revisions := source.revisions()
	if len(revisions) != 2 || revisions[0] != 10 || revisions[1] != 12 {
		t.Fatalf("watch revisions=%v, want [10 12]", revisions)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation after resync err=%v", err)
	}
}

func TestRemotePumpRestartAwareResyncIsOnlyAfterClosedWatchAndBindsInstalledSnapshot(t *testing.T) {
	base := testSnapshot(10)
	secondWatchStarted := make(chan struct{})
	source := &restartAwareFakeSource{fakeSource: &fakeSource{}}
	source.fetchFn = func(context.Context, uint64) (actorrouting.RouteSnapshot, error) {
		return actorrouting.CloneRouteSnapshot(base), nil
	}
	source.watchFn = func(ctx context.Context, _ uint64, call uint64) (<-chan actorrouting.RouteSnapshot, error) {
		watch := make(chan actorrouting.RouteSnapshot)
		if call == 1 {
			close(watch)
			return watch, nil
		}
		close(secondWatchStarted)
		go func() {
			<-ctx.Done()
			close(watch)
		}()
		return watch, nil
	}
	source.resyncFn = func(_ context.Context, baseline ResyncBaseline, call uint64) (actorrouting.RouteSnapshot, error) {
		if call != 1 || baseline.Revision != base.GlobalRevision || !reflect.DeepEqual(baseline.Snapshot, base) {
			return actorrouting.RouteSnapshot{}, fmt.Errorf("restart-aware baseline call=%d revision=%d", call, baseline.Revision)
		}
		return actorrouting.CloneRouteSnapshot(base), nil
	}
	provider, pump, err := NewRemoteAssignmentProvider(source, Options{Backoff: func(int) time.Duration { return 0 }})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pump.Run(ctx) }()
	select {
	case <-secondWatchStarted:
	case <-time.After(time.Second):
		t.Fatal("Pump did not start the post-resync Watch")
	}
	if source.fetchCalls.Load() != 1 || source.resyncCalls.Load() != 1 {
		t.Fatalf("fetch paths ordinary=%d restart-aware=%d want 1/1", source.fetchCalls.Load(), source.resyncCalls.Load())
	}
	if got := provider.CurrentSnapshot(); !reflect.DeepEqual(got, base) || pump.State() != StateWatching {
		t.Fatalf("post-resync snapshot revision=%d state=%s", got.GlobalRevision, pump.State())
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Pump cancellation err=%v", err)
	}
}

func TestRemotePumpWatchStartFailureAndLegacySourceKeepOrdinaryResync(t *testing.T) {
	base := testSnapshot(10)

	t.Run("optional source is not used before a Watch exists", func(t *testing.T) {
		secondWatchStarted := make(chan struct{})
		source := &restartAwareFakeSource{fakeSource: &fakeSource{}}
		source.fetchFn = func(context.Context, uint64) (actorrouting.RouteSnapshot, error) {
			return actorrouting.CloneRouteSnapshot(base), nil
		}
		source.watchFn = func(ctx context.Context, _ uint64, call uint64) (<-chan actorrouting.RouteSnapshot, error) {
			if call == 1 {
				return nil, errors.New("watch start failed")
			}
			watch := make(chan actorrouting.RouteSnapshot)
			close(secondWatchStarted)
			go func() {
				<-ctx.Done()
				close(watch)
			}()
			return watch, nil
		}
		source.resyncFn = func(context.Context, ResyncBaseline, uint64) (actorrouting.RouteSnapshot, error) {
			return actorrouting.RouteSnapshot{}, errors.New("restart-aware resync called without a closed prior Watch")
		}
		_, pump, err := NewRemoteAssignmentProvider(source, Options{Backoff: func(int) time.Duration { return 0 }})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- pump.Run(ctx) }()
		select {
		case <-secondWatchStarted:
		case <-time.After(time.Second):
			t.Fatal("Pump did not reconnect after Watch start failure")
		}
		if source.fetchCalls.Load() != 2 || source.resyncCalls.Load() != 0 {
			t.Fatalf("fetch paths ordinary=%d restart-aware=%d want 2/0", source.fetchCalls.Load(), source.resyncCalls.Load())
		}
		cancel()
		<-done
	})

	t.Run("transport-neutral legacy source", func(t *testing.T) {
		secondWatchStarted := make(chan struct{})
		source := &fakeSource{
			fetchFn: func(context.Context, uint64) (actorrouting.RouteSnapshot, error) {
				return actorrouting.CloneRouteSnapshot(base), nil
			},
			watchFn: func(ctx context.Context, _ uint64, call uint64) (<-chan actorrouting.RouteSnapshot, error) {
				watch := make(chan actorrouting.RouteSnapshot)
				if call == 1 {
					close(watch)
					return watch, nil
				}
				close(secondWatchStarted)
				go func() {
					<-ctx.Done()
					close(watch)
				}()
				return watch, nil
			},
		}
		if _, ok := any(source).(RestartAwareRemoteSnapshotSource); ok {
			t.Fatal("legacy fake unexpectedly implements restart-aware capability")
		}
		_, pump, err := NewRemoteAssignmentProvider(source, Options{Backoff: func(int) time.Duration { return 0 }})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- pump.Run(ctx) }()
		select {
		case <-secondWatchStarted:
		case <-time.After(time.Second):
			t.Fatal("legacy source did not reconnect")
		}
		if source.fetchCalls.Load() != 2 {
			t.Fatalf("legacy full fetch calls=%d want 2", source.fetchCalls.Load())
		}
		cancel()
		<-done
	})
}

func TestRemotePumpCloseCancelsBlockedFetchWatchAndBackoffWithoutFatal(t *testing.T) {
	t.Run("fetch", func(t *testing.T) {
		started := make(chan struct{})
		source := &fakeSource{
			fetchFn: func(ctx context.Context, _ uint64) (actorrouting.RouteSnapshot, error) {
				close(started)
				<-ctx.Done()
				return actorrouting.RouteSnapshot{}, ctx.Err()
			},
		}
		_, pump, err := NewRemoteAssignmentProvider(source)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- pump.Run(context.Background()) }()
		<-started
		if err := pump.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatalf("Close blocked fetch returned fatal error: %v", err)
		}
		if pump.State() != StateClosed || pump.LastError() != nil {
			t.Fatalf("closed fetch state=%s err=%v", pump.State(), pump.LastError())
		}
	})

	t.Run("watch", func(t *testing.T) {
		started := make(chan struct{})
		source := &fakeSource{
			fetchFn: func(context.Context, uint64) (actorrouting.RouteSnapshot, error) {
				return testSnapshot(10), nil
			},
			watchFn: func(ctx context.Context, _ uint64, _ uint64) (<-chan actorrouting.RouteSnapshot, error) {
				close(started)
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}
		_, pump, err := NewRemoteAssignmentProvider(source)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- pump.Run(context.Background()) }()
		<-started
		if err := pump.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatalf("Close blocked watch returned fatal error: %v", err)
		}
	})

	t.Run("backoff", func(t *testing.T) {
		source := &fakeSource{
			fetchFn: func(_ context.Context, call uint64) (actorrouting.RouteSnapshot, error) {
				if call == 1 {
					return testSnapshot(10), nil
				}
				return actorrouting.RouteSnapshot{}, errors.New("fetch unavailable")
			},
			watchFn: func(_ context.Context, _ uint64, _ uint64) (<-chan actorrouting.RouteSnapshot, error) {
				ch := make(chan actorrouting.RouteSnapshot)
				close(ch)
				return ch, nil
			},
		}
		_, pump, err := NewRemoteAssignmentProvider(source, Options{
			MaxResyncAttempts: 3,
			Backoff:           func(int) time.Duration { return time.Hour },
		})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- pump.Run(context.Background()) }()
		waitFor(t, time.Second, func() bool { return pump.State() == StateResyncing })
		if err := pump.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatalf("Close blocked backoff returned fatal error: %v", err)
		}
	})
}

func TestRemoteInstallerSerializesConcurrentInstallAndCannotBeStaleOverwritten(t *testing.T) {
	provider, installer, err := actorrouting.NewRemoteAssignmentProvider(1)
	if err != nil {
		t.Fatal(err)
	}
	base := testSnapshot(10)
	if err := installer.InstallFullSnapshot(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	next11 := actorrouting.CloneRouteSnapshot(base)
	next11.GlobalRevision = 11
	next11.Player.Routes[0].ShardRevision++
	next11.Player.Routes[0].ExpectedWriterEpoch++
	next11.Player.Routes[0].ExpectedLeaseEpoch++
	next11.Player.Routes[0].LeaseUntilMs++
	next12 := actorrouting.CloneRouteSnapshot(next11)
	next12.GlobalRevision = 12
	next12.Player.Routes[0].ShardRevision++
	next12.Player.Routes[0].ExpectedWriterEpoch++
	next12.Player.Routes[0].ExpectedLeaseEpoch++
	next12.Player.Routes[0].LeaseUntilMs++

	if err := installer.InstallFullSnapshot(context.Background(), next12); err != nil {
		t.Fatal(err)
	}

	const staleAttempts = 8
	var wg sync.WaitGroup
	errs := make(chan error, staleAttempts)
	for attempt := 0; attempt < staleAttempts; attempt++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- installer.InstallFullSnapshot(context.Background(), next11)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, actorrouting.ErrStaleRoute) {
			t.Fatalf("concurrent stale install err=%v", err)
		}
	}
	if got := provider.CurrentSnapshot().GlobalRevision; got != 12 {
		t.Fatalf("concurrent install final revision=%d, want 12", got)
	}
}

func TestRemotePumpSecondValidatesBeforePermissiveInstaller(t *testing.T) {
	source := &fakeSource{}
	installer := &permissiveInstaller{}
	pump, err := NewRemoteAssignmentPump(source, installer)
	if err != nil {
		t.Fatal(err)
	}
	base := testSnapshot(10)
	if err := pump.install(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	topologyChange := actorrouting.CloneRouteSnapshot(base)
	topologyChange.GlobalRevision = 12
	topologyChange.HashSeed++
	if err := pump.install(context.Background(), topologyChange); !errors.Is(err, actorrouting.ErrHashMigrationRequired) {
		t.Fatalf("pump accepted topology change before permissive installer: %v", err)
	}
	if installer.calls.Load() != 1 {
		t.Fatalf("permissive installer calls=%d, want 1", installer.calls.Load())
	}
}

func testSnapshot(revision uint64) actorrouting.RouteSnapshot {
	leaseUntil := time.Now().Add(time.Hour).UnixMilli()
	return actorrouting.RouteSnapshot{
		Realm: "remote-test", GlobalRevision: revision,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608080001,
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1,
			ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: leaseUntil,
		}}},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: 1,
			ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: leaseUntil,
		}}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "player-a", BootID: "player-a-boot", Endpoint: "player-a:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-a", BootID: "farm-a-boot", Endpoint: "farm-a:9001", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
}

func assigningSnapshot(revision uint64) actorrouting.RouteSnapshot {
	snapshot := testSnapshot(revision)
	snapshot.Player.Routes[0].State = actorrouting.ShardStateAssigning
	snapshot.Player.Routes[0].ShardRevision = 2
	snapshot.Player.Routes[0].ExpectedWriterEpoch = 2
	snapshot.Player.Routes[0].ExpectedLeaseEpoch = 2
	return snapshot
}

func waitFor(t *testing.T, timeout time.Duration, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}
