package routecontrol

import (
	"context"
	"fmt"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/controlfence"
)

func TestAuthorityWaveThirtyTwoShardsPublishesTwoBoundedStages(t *testing.T) {
	ctx := context.Background()
	routes := make([]actorrouting.ShardRoute, MaxAuthorityWaveBatchSize)
	for index := range routes {
		routes[index] = actorrouting.ShardRoute{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: 10_000}
	}
	base := actorrouting.RouteSnapshot{
		Realm: "wave-32", GlobalRevision: 1, HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 42,
		Player: actorrouting.DomainSnapshot{ShardCount: MaxAuthorityWaveBatchSize, Routes: routes},
		Farm:   actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 2, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: 10_000}}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "source", BootID: "source-boot", Endpoint: "source.internal:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "target", BootID: "target-boot", Endpoint: "target.internal:9000", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm", BootID: "farm-boot", Endpoint: "farm.internal:9000", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
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
	members := make([]AuthorityWaveMember, MaxAuthorityWaveBatchSize)
	for shard := range members {
		members[shard] = AuthorityWaveMember{Domain: actorrouting.DomainPlayer, VirtualShard: uint32(shard)}
	}
	if err := publisher.BeginAuthorityWaveBatch(ctx, "wave-32", base, members); err != nil {
		t.Fatal(err)
	}
	token := controlfence.NewIssuer().Issue("wave-32", 1)
	current := base
	for shard := uint32(0); shard < MaxAuthorityWaveBatchSize; shard++ {
		assigning := actorrouting.CloneRouteSnapshot(current)
		assigning.GlobalRevision++
		assigning.Player.Routes[shard] = actorrouting.ShardRoute{HolderIndex: 1, State: actorrouting.ShardStateAssigning, ShardRevision: 2, ExpectedWriterEpoch: 2, ExpectedLeaseEpoch: 2, LeaseUntilMs: 20_000}
		if err := publisher.Publish(ctx, token, assigning); err != nil {
			t.Fatalf("assigning shard %d: %v", shard, err)
		}
		current = assigning
	}
	capability, steps, err := publisher.SealAuthorityWaveBatch("wave-32")
	if err != nil || steps != 32 {
		t.Fatalf("steps=%d err=%v", steps, err)
	}
	result, err := publisher.CommitAuthorityWaveBatch(ctx, token, capability)
	if err != nil || !result.Committed() || result.Stored.Revision != 33 || notifications != 1 {
		t.Fatalf("result=%+v notifications=%d err=%v", result, notifications, err)
	}
	if err := publisher.BeginAuthorityWaveBatch(ctx, "wave-32", current, members); err != nil {
		t.Fatal(err)
	}
	for shard := uint32(0); shard < MaxAuthorityWaveBatchSize; shard++ {
		active := actorrouting.CloneRouteSnapshot(current)
		active.GlobalRevision++
		active.Player.Routes[shard].State = actorrouting.ShardStateActive
		if err := publisher.PublishAuthoritySnapshot(ctx, token, active); err != nil {
			t.Fatalf("active shard %d: %v", shard, err)
		}
		current = active
	}
	capability, steps, err = publisher.SealAuthorityWaveBatch("wave-32")
	if err != nil || steps != 32 {
		t.Fatalf("active steps=%d err=%v", steps, err)
	}
	result, err = publisher.CommitAuthorityWaveBatch(ctx, token, capability)
	if err != nil || !result.Committed() || result.Stored.Revision != 65 || notifications != 2 {
		t.Fatalf("active result=%+v notifications=%d err=%v", result, notifications, err)
	}
	if got := fmt.Sprint(result.Stored.Snapshot.Player.Routes[31].State); got != fmt.Sprint(actorrouting.ShardStateActive) {
		t.Fatalf("last shard state=%s", got)
	}
}
