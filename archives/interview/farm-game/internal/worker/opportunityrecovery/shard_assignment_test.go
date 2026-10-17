package opportunityrecovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"farmgame/internal/storage/redis/rediskey"
)

func TestStaticWorkerShardAssignmentProviderOwnsExplicitSubset(t *testing.T) {
	_, err := NewStaticWorkerShardAssignmentProvider([]uint32{3, 1, 2, 1})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("duplicate shards err=%v", err)
	}
	provider, err := NewStaticWorkerShardAssignmentProvider([]uint32{3, 1, 2})
	if err != nil {
		t.Fatal(err)
	}
	assignment, err := provider.Assignment(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(assignment.OwnedShards) != 3 || assignment.OwnedShards[0] != 1 || assignment.OwnedShards[2] != 3 {
		t.Fatalf("owned=%v", assignment.OwnedShards)
	}
}

func TestAllConfiguredShardsProviderIsOneWorkerDefault(t *testing.T) {
	provider, err := NewAllConfiguredShardsProvider([]uint32{0, 1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	assignment, err := provider.Assignment(context.Background())
	if err != nil || len(assignment.OwnedShards) != 4 {
		t.Fatalf("assignment=%+v err=%v", assignment, err)
	}
}

func TestNewRejectsOwnedShardOutsideConfiguredBuckets(t *testing.T) {
	bucket0 := testBucketForShard(t, 0, 2)
	bucket1 := testBucketForShard(t, 1, 2)
	provider, err := NewStaticWorkerShardAssignmentProvider([]uint32{0, 2})
	if err != nil {
		t.Fatal(err)
	}
	_, err = New(Config{
		ScheduleStore: &fakeScheduleStore{}, FarmReader: &fakeFarmReader{}, Publisher: &fakePublisher{},
		Buckets: []rediskey.AuthorityBucketRef{bucket0, bucket1}, ShardAssignment: provider,
		Owner: "w", Token: "t", BootID: "b", MaxConcurrent: 1, RoundTimeBudget: time.Minute,
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("outside shard err=%v", err)
	}
}
