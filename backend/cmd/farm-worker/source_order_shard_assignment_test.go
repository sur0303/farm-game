package main

import (
	"context"
	"errors"
	"testing"

	"farmgame/internal/worker/opportunityrecovery"
)

type sourceAssignmentFixture struct {
	assignment opportunityrecovery.WorkerShardAssignment
	claimShard uint32
	claimRev   uint64
	claimErr   error
}

func (f *sourceAssignmentFixture) Assignment(context.Context) (opportunityrecovery.WorkerShardAssignment, error) {
	return f.assignment, nil
}

func (f *sourceAssignmentFixture) CanClaimShard(_ context.Context, shard uint32, revision uint64) error {
	f.claimShard, f.claimRev = shard, revision
	return f.claimErr
}

func TestMappedSourceOrderAssignmentUsesStableWorkerShard(t *testing.T) {
	base := &sourceAssignmentFixture{assignment: opportunityrecovery.WorkerShardAssignment{
		Revision: 17, OwnedShards: []uint32{1, 3},
	}}
	mapped, err := newMappedSourceOrderShardAssignment(base, base, []uint32{0, 1, 2, 3, 4, 5, 6, 7}, 4)
	if err != nil {
		t.Fatal(err)
	}
	assignment, err := mapped.Assignment(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []uint32{1, 3, 5, 7}
	if assignment.Revision != 17 || len(assignment.OwnedShards) != len(want) {
		t.Fatalf("assignment=%+v", assignment)
	}
	for index := range want {
		if assignment.OwnedShards[index] != want[index] {
			t.Fatalf("owned=%v want=%v", assignment.OwnedShards, want)
		}
	}
	if err := mapped.CanClaimShard(context.Background(), 5, 17); err != nil {
		t.Fatal(err)
	}
	if base.claimShard != 1 || base.claimRev != 17 {
		t.Fatalf("claim mapped to shard=%d revision=%d", base.claimShard, base.claimRev)
	}
}

func TestMappedSourceOrderAssignmentPropagatesClaimFence(t *testing.T) {
	want := errors.New("stale assignment")
	base := &sourceAssignmentFixture{
		assignment: opportunityrecovery.WorkerShardAssignment{Revision: 4, OwnedShards: []uint32{0}},
		claimErr:   want,
	}
	mapped, err := newMappedSourceOrderShardAssignment(base, base, []uint32{0, 1}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := mapped.CanClaimShard(context.Background(), 0, 3); !errors.Is(err, want) {
		t.Fatalf("claim err=%v want=%v", err, want)
	}
}
