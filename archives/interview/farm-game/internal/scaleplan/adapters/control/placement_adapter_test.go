package control

import (
	"errors"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	scaleplancore "farmgame/internal/scaleplan"
	"farmgame/internal/storage/authoritystore"
)

func TestPlacementAuthorityProjectionDelegatedUsesRedisDeadline(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	route := actorrouting.ShardRoute{
		State:               actorrouting.ShardStateActive,
		ShardRevision:       4,
		ExpectedWriterEpoch: 5,
		ExpectedLeaseEpoch:  6,
		LeaseUntilMs:        now.Add(-time.Second).UnixMilli(),
	}
	ref := scaleplancore.HolderRef{Domain: actorrouting.DomainFarm, HolderID: "farm-a", BootID: "farm-boot"}
	value := authoritystore.ShardWriteAuthority{
		Domain: actorrouting.DomainFarm, VirtualShard: 0,
		HolderID: ref.HolderID, HolderBootID: ref.BootID,
		AssignmentRevision: route.ShardRevision, WriterEpoch: route.ExpectedWriterEpoch,
		LeaseEpoch: route.ExpectedLeaseEpoch, LeaseUntilMs: now.Add(time.Minute).UnixMilli(),
		State: authoritystore.ShardAuthorityActive,
	}
	if err := validateAuthorityProjection(value, actorrouting.DomainFarm, 0, ref, route, actorrouting.LeaseCheckSnapshot, now); !errors.Is(err, ErrPlacementAdapterDrift) {
		t.Fatalf("snapshot mode accepted expired route deadline: %v", err)
	}
	if err := validateAuthorityProjection(value, actorrouting.DomainFarm, 0, ref, route, actorrouting.LeaseCheckAuthorityDelegated, now); err != nil {
		t.Fatalf("delegated mode rejected fresh Redis authority: %v", err)
	}
	if got := effectiveLeaseUntil(route.LeaseUntilMs, value.LeaseUntilMs, actorrouting.LeaseCheckAuthorityDelegated); got.UnixMilli() != value.LeaseUntilMs {
		t.Fatalf("delegated effective lease=%d want authority=%d", got.UnixMilli(), value.LeaseUntilMs)
	}
}
