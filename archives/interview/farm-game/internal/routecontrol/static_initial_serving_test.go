package routecontrol

import (
	"context"
	"errors"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
)

type staticServingFixture struct {
	now               time.Time
	snapshot          actorrouting.RouteSnapshot
	processReady      bool
	assignmentHealthy bool
	open              map[uint32]bool
	servable          map[uint32]bool
	previous          HolderRecord
	heartbeat         HolderHeartbeat
}

func newStaticServingFixture() *staticServingFixture {
	now := time.UnixMilli(1_800_000_000_000)
	fixture := &staticServingFixture{
		now: now, processReady: true, assignmentHealthy: true,
		open: map[uint32]bool{0: true}, servable: map[uint32]bool{0: true},
	}
	fixture.snapshot = actorrouting.RouteSnapshot{
		Realm: "static-serving", GlobalRevision: 7,
		Holders: []actorrouting.HolderRef{{
			InstanceID: "player-svr-0", BootID: "player-svr-0-functional-g18-nonce",
			Endpoint: "player-svr-0.player-svr-headless:9100", State: actorrouting.HolderStateActive, Weight: 1,
		}},
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1,
			ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: now.Add(time.Minute).UnixMilli(),
		}}},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1,
			ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: now.Add(time.Minute).UnixMilli(),
		}}},
	}
	fixture.previous = HolderRecord{
		Domain: actorrouting.DomainPlayer, HolderID: "player-svr-0", BootID: "player-svr-0-functional-g18-nonce",
		LogicEndpoint: "player-svr-0.player-svr-headless:9100", State: HolderStateWarming,
	}
	fixture.heartbeat = HolderHeartbeat{
		Domain: fixture.previous.Domain, HolderID: fixture.previous.HolderID, BootID: fixture.previous.BootID,
		State: HolderStateServing, ProcessReady: true, ServingShardCount: 1, SnapshotRevision: 7,
	}
	return fixture
}

func (f *staticServingFixture) verifier(t *testing.T) InitialServingVerifier {
	t.Helper()
	verifier, err := NewStaticInitialServingVerifier(StaticInitialServingVerifierOptions{
		Domain: f.previous.Domain, HolderID: f.previous.HolderID, HolderBootID: f.previous.BootID,
		LogicEndpoint: f.previous.LogicEndpoint,
		Snapshot: func(context.Context) (actorrouting.RouteSnapshot, error) {
			return actorrouting.CloneRouteSnapshot(f.snapshot), nil
		},
		ProcessReady: func() bool { return f.processReady }, AssignmentHealthy: func() bool { return f.assignmentHealthy },
		ShardOpen: func(shard uint32) bool { return f.open[shard] },
		CanServeShard: func(domain actorrouting.RouteDomain, shard uint32) bool {
			return domain == f.previous.Domain && f.servable[shard]
		},
		Now: func() time.Time { return f.now }, MinimumRemainingLease: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return verifier
}

func TestStaticInitialServingVerifierRequiresExactInstalledRuntimeFacts(t *testing.T) {
	fixture := newStaticServingFixture()
	if err := fixture.verifier(t)(context.Background(), fixture.previous, fixture.heartbeat); err != nil {
		t.Fatal(err)
	}
	fixture.previous.State = HolderStateReadyZeroShard
	fixture.previous.ProcessReady = true
	if err := fixture.verifier(t)(context.Background(), fixture.previous, fixture.heartbeat); err != nil {
		t.Fatalf("READY_ZERO_SHARD static admission: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*staticServingFixture)
	}{
		{"process-not-ready", func(f *staticServingFixture) { f.processReady = false }},
		{"assignment-not-healthy", func(f *staticServingFixture) { f.assignmentHealthy = false }},
		{"candidate-not-ready", func(f *staticServingFixture) { f.heartbeat.ProcessReady = false }},
		{"candidate-zero-shard", func(f *staticServingFixture) { f.heartbeat.ServingShardCount = 0 }},
		{"candidate-count-mismatch", func(f *staticServingFixture) { f.heartbeat.ServingShardCount = 2 }},
		{"candidate-revision-zero", func(f *staticServingFixture) { f.heartbeat.SnapshotRevision = 0 }},
		{"candidate-revision-not-installed", func(f *staticServingFixture) { f.heartbeat.SnapshotRevision = 8 }},
		{"holder-boot-mismatch", func(f *staticServingFixture) { f.snapshot.Holders[0].BootID = "different-boot" }},
		{"holder-endpoint-mismatch", func(f *staticServingFixture) { f.snapshot.Holders[0].Endpoint = "other:9100" }},
		{"holder-not-active", func(f *staticServingFixture) { f.snapshot.Holders[0].State = actorrouting.HolderStateDraining }},
		{"route-not-active", func(f *staticServingFixture) { f.snapshot.Player.Routes[0].State = actorrouting.ShardStateDraining }},
		{"route-expired", func(f *staticServingFixture) { f.snapshot.Player.Routes[0].LeaseUntilMs = f.now.UnixMilli() }},
		{"shard-gate-closed", func(f *staticServingFixture) { f.open[0] = false }},
		{"authority-guard-not-servable", func(f *staticServingFixture) { f.servable[0] = false }},
		{"runtime-serves-foreign-assignment", func(f *staticServingFixture) { f.snapshot.Holders[0].InstanceID = "player-svr-1" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := newStaticServingFixture()
			test.mutate(candidate)
			err := candidate.verifier(t)(context.Background(), candidate.previous, candidate.heartbeat)
			if !errors.Is(err, ErrHolderInitialServingAdmission) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestStaticInitialServingVerifierHonorsContextAndConstructionBounds(t *testing.T) {
	fixture := newStaticServingFixture()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := fixture.verifier(t)(ctx, fixture.previous, fixture.heartbeat); !errors.Is(err, ErrHolderInitialServingAdmission) {
		t.Fatalf("canceled err=%v", err)
	}
	if _, err := NewStaticInitialServingVerifier(StaticInitialServingVerifierOptions{}); !errors.Is(err, ErrHolderInitialServingAdmission) {
		t.Fatalf("construction err=%v", err)
	}
}
