package routecontrol

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/controlfence"
)

func initialBatchTestPlaceholder() actorrouting.RouteSnapshot {
	snapshot := actorrouting.RouteSnapshot{
		Realm: "batch-realm", GlobalRevision: 1,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608140001,
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 0, State: actorrouting.ShardStateAssigning, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1}}},
		Farm:   actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 1, State: actorrouting.ShardStateAssigning, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1}}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "player-0", BootID: "player-boot", Endpoint: "player.internal:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-0", BootID: "farm-boot", Endpoint: "farm.internal:9000", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
	return snapshot
}

func initialBatchTestFinal(previous actorrouting.RouteSnapshot) actorrouting.RouteSnapshot {
	next := actorrouting.CloneRouteSnapshot(previous)
	next.GlobalRevision = 3
	next.Player.Routes[0] = actorrouting.ShardRoute{HolderIndex: 2, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: 10_000}
	next.Farm.Routes[0] = actorrouting.ShardRoute{HolderIndex: 3, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: 10_000}
	next.Holders = append(next.Holders,
		actorrouting.HolderRef{InstanceID: "player-target", BootID: "player-target-boot", Endpoint: "player-target.internal:9000", State: actorrouting.HolderStateActive, Weight: 1},
		actorrouting.HolderRef{InstanceID: "farm-target", BootID: "farm-target-boot", Endpoint: "farm-target.internal:9000", State: actorrouting.HolderStateActive, Weight: 1},
	)
	return next
}

func initialBatchTestRecords(now int64) []InitialAuthorityBatchRecord {
	return []InitialAuthorityBatchRecord{
		{Realm: "batch-realm", Domain: actorrouting.DomainPlayer, VirtualShard: 0, HolderID: "player-target", HolderBootID: "player-target-boot", Endpoint: "player-target.internal:9000", AuthorityLayoutVersion: "auth-v1", HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608140001, AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1, LeaseUntilMs: 10_000, LastTransitionID: InitialAuthorityTransitionID("batch-realm", actorrouting.DomainPlayer, 0, "player-target", "player-target-boot", "player-target.internal:9000"), AcceptedTTLms: 60_000},
		{Realm: "batch-realm", Domain: actorrouting.DomainFarm, VirtualShard: 0, HolderID: "farm-target", HolderBootID: "farm-target-boot", Endpoint: "farm-target.internal:9000", AuthorityLayoutVersion: "auth-v1", HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608140001, AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1, LeaseUntilMs: 10_000, LastTransitionID: InitialAuthorityTransitionID("batch-realm", actorrouting.DomainFarm, 0, "farm-target", "farm-target-boot", "farm-target.internal:9000"), AcceptedTTLms: 60_000},
	}
}

func TestInitialAuthorityTransitionIDIsBounded(t *testing.T) {
	id := InitialAuthorityTransitionID(strings.Repeat("realm", 100), actorrouting.DomainPlayer, 0, "holder", "boot", "endpoint:9000")
	if len(id) > 256 {
		t.Fatalf("transition id length=%d, want <=256: %q", len(id), id)
	}
}

func TestInitialAuthorityBatchValidatorAndCommitAreSealed(t *testing.T) {
	previous := initialBatchTestPlaceholder()
	final := initialBatchTestFinal(previous)
	stored, err := CanonicalizeRouteSnapshot(previous)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateInitialAuthorityBatchAt(stored, final, initialBatchTestRecords(500), "auth-v1", time.UnixMilli(500)); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*actorrouting.RouteSnapshot){
		"partial": func(snapshot *actorrouting.RouteSnapshot) {
			snapshot.Farm.Routes[0].State = actorrouting.ShardStateAssigning
			snapshot.Farm.Routes[0].LeaseUntilMs = 0
		},
		"farm-worker-drift": func(snapshot *actorrouting.RouteSnapshot) {
			snapshot.FarmWorker = actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 0, State: actorrouting.ShardStateAssigning, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1}}}
		},
		"stale-lease":     func(snapshot *actorrouting.RouteSnapshot) { snapshot.Player.Routes[0].LeaseUntilMs = 1 },
		"holder-rollback": func(snapshot *actorrouting.RouteSnapshot) { snapshot.Player.Routes[0].ExpectedWriterEpoch = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := actorrouting.CloneRouteSnapshot(final)
			mutate(&candidate)
			if err := ValidateInitialAuthorityBatchAt(stored, candidate, initialBatchTestRecords(500), "auth-v1", time.UnixMilli(500)); !errors.Is(err, ErrInitialAuthorityBatchInvalid) {
				t.Fatalf("err=%v, want invalid batch", err)
			}
		})
	}

	store := NewMemoryRouteSnapshotStore()
	store.SetNow(func() int64 { return 2_000 })
	if _, err := store.Bootstrap(context.Background(), NestedControlOperationToken{}, previous); err != nil {
		t.Fatal(err)
	}
	notifications := 0
	durable, err := NewDurableSnapshotPublisher(store, nil, AuthoritySnapshotNotifierFunc(func(context.Context, actorrouting.RouteSnapshot) error {
		notifications++
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	buffered, err := NewInitialAuthorityBatchPublisher(durable, func() time.Time { return time.UnixMilli(500) })
	if err != nil {
		t.Fatal(err)
	}
	if err := buffered.BeginInitialAuthorityBatch(context.Background(), previous); err != nil {
		t.Fatal(err)
	}
	intermediate := actorrouting.CloneRouteSnapshot(final)
	intermediate.GlobalRevision = 2
	intermediate.Farm.Routes[0] = previous.Farm.Routes[0]
	if err := buffered.PublishAuthoritySnapshot(context.Background(), NestedControlOperationToken{}, intermediate); err != nil {
		t.Fatal(err)
	}
	if err := buffered.PublishAuthoritySnapshot(context.Background(), NestedControlOperationToken{}, final); err != nil {
		t.Fatal(err)
	}
	capability, err := buffered.SealInitialAuthorityBatch(initialBatchTestRecords(500))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := buffered.CommitInitialAuthorityBatch(context.Background(), NestedControlOperationToken{}, capability); !errors.Is(err, ErrInitialAuthorityBatchTokenRequired) {
		t.Fatalf("zero token err=%v", err)
	}
	token := controlfence.NewIssuer().Issue("initial-batch", 1)
	result, err := buffered.CommitInitialAuthorityBatch(context.Background(), token, capability)
	if err != nil || result.Status != SnapshotCASCommitted {
		t.Fatalf("commit result=%+v err=%v", result, err)
	}
	if notifications != 1 || !buffered.InitialAuthorityBatchSealed() {
		t.Fatalf("notifications=%d sealed=%t", notifications, buffered.InitialAuthorityBatchSealed())
	}
	current, found, err := store.LoadCurrent(context.Background())
	if err != nil || !found || !reflect.DeepEqual(current.Snapshot, final) {
		t.Fatalf("current=%+v found=%t err=%v equal=%t", current, found, err, found && reflect.DeepEqual(current.Snapshot, final))
	}
}
