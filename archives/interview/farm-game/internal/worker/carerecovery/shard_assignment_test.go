package carerecovery

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

type recoveryAssignmentFixture struct {
	mu         sync.Mutex
	assignment ShardAssignment
	claimCalls int
	claimErrAt int
	claimErr   error
}

func (f *recoveryAssignmentFixture) Assignment(context.Context) (ShardAssignment, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return ShardAssignment{Revision: f.assignment.Revision, OwnedShards: append([]uint32(nil), f.assignment.OwnedShards...)}, nil
}

func (f *recoveryAssignmentFixture) CanClaimShard(context.Context, uint32, uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimCalls++
	if f.claimErrAt != 0 && f.claimCalls >= f.claimErrAt {
		return f.claimErr
	}
	return nil
}

type bucketRecordingDue struct {
	mu      sync.Mutex
	buckets []uint32
	refs    []authority.DueWorkRef
}

type bucketObservationFixture struct {
	mu      sync.Mutex
	buckets [][]uint32
}

func (f *bucketObservationFixture) ObserveDue(context.Context, int) (DueIndexObservation, error) {
	return DueIndexObservation{}, errors.New("production observer must use owned buckets")
}

func (f *bucketObservationFixture) ObserveBuckets(_ context.Context, buckets []rediskey.AuthorityBucketRef, _ int) (DueIndexObservation, error) {
	shards := make([]uint32, 0, len(buckets))
	for _, bucket := range buckets {
		shards = append(shards, bucket.VirtualShard())
	}
	f.mu.Lock()
	f.buckets = append(f.buckets, shards)
	f.mu.Unlock()
	return DueIndexObservation{Depth: int64(len(shards)), Available: true}, nil
}

func (d *bucketRecordingDue) ListDueWork(_ context.Context, bucket rediskey.AuthorityBucketRef, limit int) ([]authority.DueWorkRef, error) {
	d.mu.Lock()
	d.buckets = append(d.buckets, bucket.VirtualShard())
	d.mu.Unlock()
	if len(d.refs) > limit {
		return append([]authority.DueWorkRef(nil), d.refs[:limit]...), nil
	}
	return append([]authority.DueWorkRef(nil), d.refs...), nil
}

func testFarmBuckets(t *testing.T, count uint32) (*actorrouting.Calculator, []rediskey.AuthorityBucketRef) {
	t.Helper()
	ns, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{ProfileVersion: "redis-physical-v1", Namespace: ns, AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 256}
	calculator, err := actorrouting.NewCalculator("classic-farm", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040001}, count, count)
	if err != nil {
		t.Fatal(err)
	}
	buckets := make([]rediskey.AuthorityBucketRef, 0, count)
	for shard := uint32(0); shard < count; shard++ {
		bucket, bucketErr := profile.AuthorityBucketForShard(calculator, actorrouting.DomainFarm, shard)
		if bucketErr != nil {
			t.Fatal(bucketErr)
		}
		buckets = append(buckets, bucket)
	}
	return calculator, buckets
}

func TestWorkerScansOnlyOwnedShardsWithRotatingBound(t *testing.T) {
	calculator, buckets := testFarmBuckets(t, 4)
	due := &bucketRecordingDue{}
	assignment := &recoveryAssignmentFixture{assignment: ShardAssignment{Revision: 7, OwnedShards: []uint32{1, 3}}}
	worker, err := NewWorker(Config{
		WorkLease: newFakeLease(), DueIndex: due, Buckets: buckets,
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		Route:         testRoute, Player: &fakeParticipant{}, Owner: "worker-1", Token: "token-1", BootID: "boot-1",
		ShardAssignment: assignment, ClaimAdmission: assignment, BucketsPerRound: 1,
		OperationTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.OwnedBuckets != 2 || first.Buckets != 1 || first.Revision != 7 || second.Buckets != 1 {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	due.mu.Lock()
	got := append([]uint32(nil), due.buckets...)
	due.mu.Unlock()
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("scanned=%v want=[1 3]", got)
	}
}

func TestWorkerRechecksAssignmentBeforeWorkClaim(t *testing.T) {
	calculator, buckets := testFarmBuckets(t, 1)
	workID, err := workidentity.StableOpaqueID("care-order-fenced", "assignment", "9")
	if err != nil {
		t.Fatal(err)
	}
	due := &bucketRecordingDue{refs: []authority.DueWorkRef{dueRef(t, workID)}}
	want := errors.New("assignment changed")
	assignment := &recoveryAssignmentFixture{
		assignment: ShardAssignment{Revision: 9, OwnedShards: []uint32{0}}, claimErrAt: 2, claimErr: want,
	}
	lease := newFakeLease()
	worker, err := NewWorker(Config{
		WorkLease: lease, DueIndex: due, Buckets: buckets,
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		Route:         testRoute, Player: &fakeParticipant{}, Owner: "worker-1", Token: "token-1", BootID: "boot-1",
		ShardAssignment: assignment, ClaimAdmission: assignment, BucketsPerRound: 1,
		OperationTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.RunOnce(context.Background()); !errors.Is(err, want) {
		t.Fatalf("RunOnce err=%v want=%v", err, want)
	}
	if lease.claimCalls != 0 {
		t.Fatalf("stale assignment issued %d WorkLease claims", lease.claimCalls)
	}
}

func TestObserveDueUsesOwnedShardsAndInvalidatesOnRevision(t *testing.T) {
	calculator, buckets := testFarmBuckets(t, 4)
	assignment := &recoveryAssignmentFixture{assignment: ShardAssignment{Revision: 3, OwnedShards: []uint32{1, 3}}}
	observer := &bucketObservationFixture{}
	now := time.UnixMilli(1_800_000_000_000)
	worker, err := NewWorker(Config{
		WorkLease: newFakeLease(), DueIndex: &bucketRecordingDue{}, DueObserver: observer, Buckets: buckets,
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		Route:         testRoute, Player: &fakeParticipant{}, Owner: "worker-1", Token: "token-1", BootID: "boot-1",
		ShardAssignment: assignment, ClaimAdmission: assignment, DueObservationInterval: 10 * time.Second,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := worker.ObserveDue(context.Background(), 32)
	if err != nil || first.Depth != 2 || !first.Available {
		t.Fatalf("first observation=%+v err=%v", first, err)
	}
	if _, err = worker.ObserveDue(context.Background(), 32); err != nil {
		t.Fatal(err)
	}
	assignment.mu.Lock()
	assignment.assignment = ShardAssignment{Revision: 4, OwnedShards: []uint32{2}}
	assignment.mu.Unlock()
	second, err := worker.ObserveDue(context.Background(), 32)
	if err != nil || second.Depth != 1 || !second.Available {
		t.Fatalf("second observation=%+v err=%v", second, err)
	}
	observer.mu.Lock()
	got := append([][]uint32(nil), observer.buckets...)
	observer.mu.Unlock()
	if !reflect.DeepEqual(got, [][]uint32{{1, 3}, {2}}) {
		t.Fatalf("observed shards=%v", got)
	}
}
