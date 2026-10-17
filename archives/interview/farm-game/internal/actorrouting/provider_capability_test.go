package actorrouting_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/controlfence"
)

var (
	_ actorrouting.AssignmentProvider           = (*actorrouting.DynamicAssignmentProvider)(nil)
	_ actorrouting.RevisionedAssignmentProvider = (*actorrouting.DynamicAssignmentProvider)(nil)
)

// These shapes must track the publisher signature exactly. They are matched
// with a runtime type assertion, so a drift here does not fail the build; it
// silently turns "the write handle can publish" into "no handle can publish"
// and the test reports the wrong reason.
type ordinaryPublishCapability interface {
	Publish(context.Context, controlfence.NestedControlOperationToken, actorrouting.RouteSnapshot) error
}

type authorityPublishCapability interface {
	PublishAuthoritySnapshot(context.Context, controlfence.NestedControlOperationToken, actorrouting.RouteSnapshot) error
}

// overridingDynamicProvider models an external adapter that embeds the real
// read handle but overrides the snapshot source. Embedding also promoted the
// old package-private trust marker, which is why provenance cannot be based on
// a marker interface.
type overridingDynamicProvider struct {
	*actorrouting.DynamicAssignmentProvider
	snapshot actorrouting.RouteSnapshot
}

func (p *overridingDynamicProvider) GetSnapshot(context.Context) (actorrouting.RouteSnapshot, error) {
	return actorrouting.CloneRouteSnapshot(p.snapshot), nil
}

func TestDynamicAssignmentReadCapabilityHasNoPublicationAPI(t *testing.T) {
	read, write, err := actorrouting.NewDynamicAssignmentProvider(capabilitySnapshot(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := any(read).(ordinaryPublishCapability); ok {
		t.Fatal("read capability implements ordinary publication")
	}
	if _, ok := any(read).(authorityPublishCapability); ok {
		t.Fatal("read capability implements manager-only authority publication")
	}
	readType := reflect.TypeOf(read)
	for _, method := range []string{"Publish", "PublishAuthoritySnapshot"} {
		if _, exists := readType.MethodByName(method); exists {
			t.Fatalf("read capability exports %s", method)
		}
	}
	if _, ok := any(write).(ordinaryPublishCapability); !ok {
		t.Fatal("paired write capability lacks ordinary publication")
	}
	if _, ok := any(write).(authorityPublishCapability); !ok {
		t.Fatal("paired write capability lacks manager authority publication")
	}
}

func TestSnapshotApplierDoesNotTrustEmbeddedDynamicProviderOverride(t *testing.T) {
	base := capabilitySnapshot()
	base.Player.Routes[0].State = actorrouting.ShardStateAssigning
	read, _, err := actorrouting.NewDynamicAssignmentProvider(base, 1)
	if err != nil {
		t.Fatal(err)
	}
	forged, err := read.GetSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	forged.GlobalRevision++
	forged.Player.Routes[0].State = actorrouting.ShardStateActive
	wrapper := &overridingDynamicProvider{DynamicAssignmentProvider: read, snapshot: forged}

	resolver, err := actorrouting.NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := actorrouting.NewSnapshotApplier(wrapper, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if err := applier.Bootstrap(context.Background()); !errors.Is(err, actorrouting.ErrSnapshotGenerationRollback) {
		t.Fatalf("embedded provider override received trusted replace: %v", err)
	}
	got := resolver.Snapshot()
	if got.GlobalRevision != base.GlobalRevision || got.Player.Routes[0].State != actorrouting.ShardStateAssigning {
		t.Fatalf("forged ACTIVE snapshot was installed: revision=%d route=%+v", got.GlobalRevision, got.Player.Routes[0])
	}
}

func capabilitySnapshot() actorrouting.RouteSnapshot {
	leaseUntil := time.Now().Add(time.Minute).UnixMilli()
	return actorrouting.RouteSnapshot{
		Realm: "classic-farm", GlobalRevision: 1,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608040001,
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
