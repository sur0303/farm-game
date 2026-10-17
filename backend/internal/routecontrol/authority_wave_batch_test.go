package routecontrol

import (
	"context"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/controlfence"
)

func TestAuthorityWaveBatchPublishesAssigningBeforeActive(t *testing.T) {
	ctx := context.Background()
	base := initialBatchTestFinal(initialBatchTestPlaceholder())
	base.GlobalRevision = 1
	store := NewMemoryRouteSnapshotStore()
	if _, err := store.Bootstrap(ctx, NestedControlOperationToken{}, base); err != nil {
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
	publisher, err := NewInitialAuthorityBatchPublisher(durable, func() time.Time { return time.UnixMilli(500) })
	if err != nil {
		t.Fatal(err)
	}
	members := []AuthorityWaveMember{{Domain: actorrouting.DomainPlayer, VirtualShard: 0}, {Domain: actorrouting.DomainFarm, VirtualShard: 0}}
	if err := publisher.BeginAuthorityWaveBatch(ctx, "wave-1", base, members); err != nil {
		t.Fatal(err)
	}
	token := controlfence.NewIssuer().Issue("wave", 1)
	playerAssigning := actorrouting.CloneRouteSnapshot(base)
	playerAssigning.GlobalRevision++
	playerAssigning.Player.Routes[0] = actorrouting.ShardRoute{HolderIndex: 0, State: actorrouting.ShardStateAssigning, ShardRevision: 2, ExpectedWriterEpoch: 2, ExpectedLeaseEpoch: 2, LeaseUntilMs: 20_000}
	if err := publisher.Publish(ctx, token, playerAssigning); err != nil {
		t.Fatal(err)
	}
	farmAssigning := actorrouting.CloneRouteSnapshot(playerAssigning)
	farmAssigning.GlobalRevision++
	farmAssigning.Farm.Routes[0] = actorrouting.ShardRoute{HolderIndex: 1, State: actorrouting.ShardStateAssigning, ShardRevision: 2, ExpectedWriterEpoch: 2, ExpectedLeaseEpoch: 2, LeaseUntilMs: 20_000}
	if err := publisher.Publish(ctx, token, farmAssigning); err != nil {
		t.Fatal(err)
	}
	capability, count, err := publisher.SealAuthorityWaveBatch("wave-1")
	if err != nil || count != 2 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	result, err := publisher.CommitAuthorityWaveBatch(ctx, token, capability)
	if err != nil || !result.Committed() || notifications != 1 || result.Stored.Revision != base.GlobalRevision+2 {
		t.Fatalf("result=%+v notifications=%d err=%v", result, notifications, err)
	}

	if err := publisher.BeginAuthorityWaveBatch(ctx, "wave-1", farmAssigning, members); err != nil {
		t.Fatal(err)
	}
	playerActive := actorrouting.CloneRouteSnapshot(farmAssigning)
	playerActive.GlobalRevision++
	playerActive.Player.Routes[0].State = actorrouting.ShardStateActive
	if err := publisher.PublishAuthoritySnapshot(ctx, token, playerActive); err != nil {
		t.Fatal(err)
	}
	farmActive := actorrouting.CloneRouteSnapshot(playerActive)
	farmActive.GlobalRevision++
	farmActive.Farm.Routes[0].State = actorrouting.ShardStateActive
	if err := publisher.PublishAuthoritySnapshot(ctx, token, farmActive); err != nil {
		t.Fatal(err)
	}
	capability, count, err = publisher.SealAuthorityWaveBatch("wave-1")
	if err != nil || count != 2 {
		t.Fatalf("active count=%d err=%v", count, err)
	}
	result, err = publisher.CommitAuthorityWaveBatch(ctx, token, capability)
	if err != nil || !result.Committed() || notifications != 2 || result.Stored.Revision != base.GlobalRevision+4 {
		t.Fatalf("active result=%+v notifications=%d err=%v", result, notifications, err)
	}
	stored, found, err := store.LoadCurrent(ctx)
	if err != nil || !found || stored.Revision != farmActive.GlobalRevision {
		t.Fatalf("stored=%+v found=%t err=%v", stored, found, err)
	}
}

func TestAuthorityWaveChangedMemberIgnoresCanonicalHolderReindex(t *testing.T) {
	previous := actorrouting.RouteSnapshot{
		Player: actorrouting.DomainSnapshot{ShardCount: 2, Routes: []actorrouting.ShardRoute{
			{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1},
			{HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1},
		}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "holder-a", BootID: "boot-a", Endpoint: "a:1", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "holder-b", BootID: "boot-b", Endpoint: "b:1", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
	next := actorrouting.CloneRouteSnapshot(previous)
	next.Holders = []actorrouting.HolderRef{
		{InstanceID: "holder-new", BootID: "boot-new", Endpoint: "new:1", State: actorrouting.HolderStateActive, Weight: 1},
		previous.Holders[0],
		previous.Holders[1],
	}
	next.Player.Routes[0] = actorrouting.ShardRoute{
		HolderIndex: 0, State: actorrouting.ShardStateAssigning,
		ShardRevision: 2, ExpectedWriterEpoch: 2, ExpectedLeaseEpoch: 2,
	}
	// Shard 1 still points to holder-b; only the HolderIndex encoding changed.
	next.Player.Routes[1].HolderIndex = 2

	member, changed := authorityWaveChangedMember(previous, next)
	if !changed || member.Domain != actorrouting.DomainPlayer || member.VirtualShard != 0 {
		t.Fatalf("member=%+v changed=%t", member, changed)
	}
}
