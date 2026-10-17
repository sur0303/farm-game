package actorrouting

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestRouteMetadataCarriesHolderBootLeaseAndSnapshotRevision(t *testing.T) {
	ctx := WithRoute(context.Background(), Address{Realm: "classic-farm", Kind: DomainPlayer, ID: 42}, Assignment{
		Kind: DomainPlayer, ShardID: 3, HolderPod: "player-a", HolderBootID: "boot-a", ShardRevision: 8,
		WriterEpoch: 12, LeaseEpoch: 15, SnapshotRevision: 21,
	})
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("route metadata missing")
	}
	routed, err := FromIncomingContext(metadata.NewIncomingContext(context.Background(), md))
	if err != nil {
		t.Fatal(err)
	}
	if routed.Address.Kind != DomainPlayer || routed.Address.ID != 42 || routed.Assignment.HolderBootID != "boot-a" || routed.Assignment.LeaseEpoch != 15 || routed.Assignment.SnapshotRevision != 21 {
		t.Fatalf("routed=%+v", routed)
	}
}
