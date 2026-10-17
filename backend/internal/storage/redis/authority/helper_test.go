package authority

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

const testHashSeed uint64 = 202608030001

var testNow = time.UnixMilli(1_770_000_000_000)

func testProfile(t *testing.T) rediskey.PhysicalProfile {
	t.Helper()
	namespace, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion:         "p1",
		Namespace:              namespace,
		AuthorityLayoutVersion: "v1",
		WorkflowLayoutVersion:  "v1",
		WorkflowHashVersion:    1,
		WorkflowBucketCount:    64,
	}
	if err := profile.Validate(); err != nil {
		t.Fatal(err)
	}
	return profile
}

func testCalculator(t *testing.T) *actorrouting.Calculator {
	t.Helper()
	calculator, err := actorrouting.NewCalculator(
		"test-realm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: testHashSeed},
		64, 64,
	)
	if err != nil {
		t.Fatal(err)
	}
	return calculator
}

func newTestClientOnly(t *testing.T) *redis.Client {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

type testRig struct {
	server *miniredis.Miniredis
	client *redis.Client
	store  *Store
	bucket rediskey.AuthorityBucketRef
	addr   authoritystore.ActorAddress
	fence  authoritystore.ShardWriteFence
}

func newTestRig(t *testing.T) *testRig {
	return newTestRigWithScheduleDueIndexMode(t, ScheduleDueIndexShared)
}

func newTestRigWithScheduleDueIndexMode(t *testing.T, mode ScheduleDueIndexMode) *testRig {
	t.Helper()
	server := miniredis.RunT(t)
	server.SetTime(testNow)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	calculator := testCalculator(t)
	profile := testProfile(t)
	store, err := NewStore(client, nil, Options{
		Profile: profile, Resolver: calculator, ScheduleDueIndexMode: mode,
		ScheduleDueIndexDedicatedReady: mode == ScheduleDueIndexDedicatedOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	addr := authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 10001}
	bucket, err := profile.AuthorityBucketForActor(calculator, addr)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind:       authoritystore.ShardAuthorityCallerRouteManager,
			InstanceID: "route-manager-1",
			BootID:     "rm-boot-1",
		},
		TransitionID:           "transition-1",
		AuthorityLayoutVersion: "v1",
		Domain:                 addr.Domain,
		VirtualShard:           bucket.VirtualShard(),
		NewHolderID:            "player-pod-1",
		NewHolderBootID:        "boot-1",
		RequestedTTLms:         60_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if claim.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("shard claim status=%s", claim.Status)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID:           claim.Authority.HolderID,
		ExpectedHolderBootID:       claim.Authority.HolderBootID,
		ExpectedAssignmentRevision: claim.Authority.AssignmentRevision,
		ExpectedWriterEpoch:        claim.Authority.WriterEpoch,
		ExpectedLeaseEpoch:         claim.Authority.LeaseEpoch,
	}
	return &testRig{server: server, client: client, store: store, bucket: bucket, addr: addr, fence: fence}
}

func (r *testRig) load(t *testing.T, claim string) authoritystore.AuthoritySnapshot {
	t.Helper()
	result, err := r.store.LoadAndClaim(context.Background(), authoritystore.LoadAndClaimRequest{
		ActorAddress:           r.addr,
		AuthorityLayoutVersion: "v1",
		ShardWriteFence:        r.fence,
		ClaimOperationID:       "claim-op-" + claim,
		NewWriterClaimID:       claim,
		InitialModelFields:     authoritystore.ModelState{"initial": []byte("state")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != authoritystore.LoadAndClaimClaimed {
		t.Fatalf("load status=%s", result.Status)
	}
	return result.Snapshot
}

func (r *testRig) commitReq(claim string, version uint64, operationID string, state authoritystore.ModelState) authoritystore.AuthorityCommitRequest {
	return commitReqFor(r.addr, r.fence, claim, version, operationID, state)
}

// testRootPlayer10001 is the explicit test root; tests must always fill the
// complete root, never rely on a default.
var testRootPlayer10001 = authoritystore.OperationRootRef{Kind: authoritystore.RootKindPlayerActor, CanonicalID: "10001"}

func commitReqFor(addr authoritystore.ActorAddress, fence authoritystore.ShardWriteFence, claim string, version uint64, operationID string, state authoritystore.ModelState) authoritystore.AuthorityCommitRequest {
	key := authoritystore.IdempotencyKey{SubjectPlayerID: 10001, StableCommandTypeName: "PLANT_CROP", OperationID: operationID, Root: testRootPlayer10001}
	hash := authoritystore.PayloadHash{}
	for i := range hash {
		hash[i] = byte(len(operationID) + i)
	}
	outcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded, Payload: []byte("ok:" + operationID)}
	return authoritystore.AuthorityCommitRequest{
		CommitRequestID:        "commit-" + operationID,
		ActorAddress:           addr,
		AuthorityLayoutVersion: "v1",
		ShardWriteFence:        fence,
		ExpectedWriterClaimID:  claim,
		ExpectedRecordVersion:  version,
		IdempotencyKey:         key,
		CanonicalPayloadHash:   hash,
		CandidateState:         state,
		Receipt: authoritystore.AuthorityReceipt{
			ReceiptID:            "receipt-" + operationID,
			IdempotencyKey:       key,
			CanonicalPayloadHash: hash,
			Outcome:              outcome,
		},
		OperationResult: outcome,
	}
}

// dueOrder builds a contract-valid attached order (StableOpaqueID work id).
func dueOrder(t *testing.T, orderType, operationID string, dueAtMs int64) authoritystore.AttachedOrder {
	t.Helper()
	workID, err := workidentity.StableOpaqueID("testwork", orderType, operationID)
	if err != nil {
		t.Fatal(err)
	}
	return authoritystore.AttachedOrder{
		OrderID:   workID,
		OrderType: orderType,
		Payload:   []byte("payload:" + operationID),
		DueAtMs:   dueAtMs,
	}
}

func workRefOf(order authoritystore.AttachedOrder) (string, error) {
	return workidentity.EncodeWorkRefV1(order.OrderType, order.OrderID)
}
