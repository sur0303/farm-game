package friend

import (
	"context"
	"errors"
	"testing"
	"time"

	friendpb "farmgame/api/gen/friend"
)

type permitPairReader struct{ snapshot PairSnapshot }

func (r *permitPairReader) GetPair(context.Context, uint64, uint64) (PairSnapshot, error) {
	return r.snapshot, nil
}

func TestActionPermitIsFencedByRelationEpochAndExpiry(t *testing.T) {
	ctx := context.Background()
	relationID, err := RelationIDFor(9, 10)
	if err != nil {
		t.Fatal(err)
	}
	pairs := &permitPairReader{snapshot: PairSnapshot{LowPlayerID: 9, HighPlayerID: 10,
		Relation: PairRelation{RelationID: relationID, RelationEpoch: 1, Status: RelationStatusActive}}}
	service, err := NewPermitService(pairs, []byte("0123456789abcdef0123456789abcdef"), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(1_800_000_000_000)
	service.now = func() time.Time { return now }
	permit, err := service.Issue(ctx, 10, 9, "theft-1", friendpb.FriendAction_THEFT)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Verify(ctx, permit, 10, 9, "theft-1", friendpb.FriendAction_THEFT); err != nil {
		t.Fatalf("fresh permit rejected: %v", err)
	}
	if err := service.Verify(ctx, permit, 10, 9, "care-1", friendpb.FriendAction_CARE); !errors.Is(err, ErrPermitInvalid) {
		t.Fatalf("permit was not bound to action and operation: %v", err)
	}
	pairs.snapshot.LowBlocksHigh = BlockRelation{Status: BlockStatusActive, BlockEpoch: 1}
	if _, err := service.Issue(ctx, 10, 9, "theft-blocked", friendpb.FriendAction_THEFT); !errors.Is(err, ErrPermitInvalid) {
		t.Fatalf("active Pair with a block issued permit: %v", err)
	}
	if err := service.Verify(ctx, permit, 10, 9, "theft-1", friendpb.FriendAction_THEFT); !errors.Is(err, ErrPermitInvalid) {
		t.Fatalf("block did not fence existing permit: %v", err)
	}
	pairs.snapshot.LowBlocksHigh = BlockRelation{}

	pairs.snapshot.Relation.Status = RelationStatusDeleted
	pairs.snapshot.Relation.RelationEpoch = 2
	if err := service.Verify(ctx, permit, 10, 9, "theft-1", friendpb.FriendAction_THEFT); !errors.Is(err, ErrPermitInvalid) {
		t.Fatalf("revoked relation left permit valid: %v", err)
	}
	pairs.snapshot.Relation.Status = RelationStatusActive
	pairs.snapshot.Relation.RelationEpoch = 3
	if err := service.Verify(ctx, permit, 10, 9, "theft-1", friendpb.FriendAction_THEFT); !errors.Is(err, ErrPermitInvalid) {
		t.Fatalf("re-friend reused stale epoch: %v", err)
	}
	newPermit, err := service.Issue(ctx, 10, 9, "theft-2", friendpb.FriendAction_THEFT)
	if err != nil || newPermit.RelationEpoch <= permit.RelationEpoch {
		t.Fatalf("new permit=%v err=%v", newPermit, err)
	}
	now = now.Add(2 * time.Minute)
	if err := service.Verify(ctx, newPermit, 10, 9, "theft-2", friendpb.FriendAction_THEFT); !errors.Is(err, ErrPermitInvalid) {
		t.Fatalf("expired permit accepted: %v", err)
	}
}
