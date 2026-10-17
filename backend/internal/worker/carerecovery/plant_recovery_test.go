package carerecovery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/cropcatalog"
	"farmgame/internal/identity"
	"farmgame/internal/storage/authoritystore"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/sourceorder"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

func plantFamily() string { return string(identity.WorkTypePlant) }

func testPlayerBucket(t *testing.T) (*actorrouting.Calculator, rediskey.AuthorityBucketRef) {
	t.Helper()
	ns, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{ProfileVersion: "redis-physical-v1", Namespace: ns, AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 256}
	calculator, err := actorrouting.NewCalculator("classic-farm", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040001}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := profile.AuthorityBucketForActor(calculator, actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: testFarmID})
	if err != nil {
		t.Fatal(err)
	}
	return calculator, bucket
}

func testBucketForPlayer(calculator *actorrouting.Calculator, playerID uint64) (rediskey.AuthorityBucketRef, error) {
	ns, err := rediskey.NewNamespace("test", "z1")
	if err != nil {
		return rediskey.AuthorityBucketRef{}, err
	}
	profile := rediskey.PhysicalProfile{ProfileVersion: "redis-physical-v1", Namespace: ns, AuthorityLayoutVersion: "v1", WorkflowLayoutVersion: "v1", WorkflowHashVersion: 1, WorkflowBucketCount: 256}
	return profile.AuthorityBucketForActor(calculator, actorrouting.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: playerID})
}

func testFarmRoute(_ context.Context, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
	return &logicrpcpb.ActorRouteHeader{
		RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: actorID, VirtualShard: 0,
		ShardRevision: 1, ExpectedHolderId: "farm-1", ExpectedHolderBootId: "boot-1", ExpectedWriterEpoch: 1,
	}, nil
}

func plantDueRef(t *testing.T, workID string) authority.DueWorkRef {
	t.Helper()
	member, err := workidentity.EncodeWorkRefV1(plantFamily(), workID)
	if err != nil {
		t.Fatal(err)
	}
	return authority.DueWorkRef{Ref: workidentity.WorkRef{WorkType: plantFamily(), WorkID: workID}, DueAtMs: time.Now().UnixMilli(), Member: member}
}

func plantOrderID(t *testing.T, name string) string {
	t.Helper()
	value, err := sourceorder.PlantSourceOrderID(testFarmID, "plant-"+name)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func plantPayload(t *testing.T, orderID string) ([]byte, *contractspb.CommitPlantPayload) {
	t.Helper()
	definition, ok := cropcatalog.ByDefinitionID(cropcatalog.CarrotDefinitionID)
	if !ok {
		t.Fatal("carrot definition missing")
	}
	snapshot, err := cropcatalog.BuildCropRuleSnapshot(definition)
	if err != nil {
		t.Fatal(err)
	}
	snapshotHash, err := contracts.CanonicalHash(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	cycleID, err := identity.CropCycleID(orderID)
	if err != nil {
		t.Fatal(err)
	}
	reservationID, err := identity.SeedReservationID(orderID)
	if err != nil {
		t.Fatal(err)
	}
	payload := &contractspb.CommitPlantPayload{
		OwnerPlayerId: testFarmID, FarmId: testFarmID, PlotId: 3, ExpectedPlotVersion: 1,
		ReservationId: reservationID, ReserveSeedResultHash: bytes.Repeat([]byte{9}, 32),
		CropCycleId: cycleID, CropRuleSnapshot: snapshot, CropRuleSnapshotHash: append([]byte(nil), snapshotHash[:]...),
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sourceorder.DecodeCommitPlantPayload(orderID, raw); err != nil {
		t.Fatal(err)
	}
	return raw, payload
}

func plantResponse(t *testing.T, orderID string, outcome contractspb.ParticipantOutcome) *contractspb.ParticipantResponse {
	t.Helper()
	invocation, err := sourceorder.PlantInvocation(orderID)
	if err != nil {
		t.Fatal(err)
	}
	result := &contractspb.CommitPlantResult{
		FarmId: testFarmID, PlotId: 3, CropCycleId: "cycle", CropDefinitionId: 1,
		AppliedCropRuleVersion: 1, PlantAtMs: 1, MatureAtMs: 2,
		CommittedPlotVersion: 2, CommittedFarmVersion: 3, PlantDecisionHash: bytes.Repeat([]byte{4}, 32),
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT)
	if err != nil {
		t.Fatal(err)
	}
	typed, hash, err := contracts.ValidateTypedResult(spec, result)
	if err != nil {
		t.Fatal(err)
	}
	return &contractspb.ParticipantResponse{
		ParticipantCommand: spec.Command, ParticipantInvocationId: invocation,
		Outcome: outcome, TypedResult: typed, ParticipantResultHash: hash, CommittedRecordVersion: 9,
	}
}

func makePlantWorker(t *testing.T, lease *fakeLease, due fakeDue, farm *fakeParticipant, payloads map[string][]byte, bucket rediskey.AuthorityBucketRef, calculator *actorrouting.Calculator) *Worker {
	t.Helper()
	lease.payload = payloads
	lease.subjectPlayerID = testFarmID
	worker, err := NewWorker(Config{
		WorkLease: lease, DueIndex: due, Buckets: []rediskey.AuthorityBucketRef{bucket},
		BucketForPlayer: func(playerID uint64) (rediskey.AuthorityBucketRef, error) {
			return testBucketForPlayer(calculator, playerID)
		},
		Route: testRoute, Farm: farm, FarmRoute: testFarmRoute, Owner: "worker-1", Token: "token-1", BootID: "boot-1",
		ClaimLimit: 32, MaxConcurrent: 4, LeaseTTL: 2 * time.Second, SafetyMargin: 100 * time.Millisecond,
		OperationTimeout: time.Second, Backoff: workflow.BackoffConfig{Base: time.Millisecond, Max: 5 * time.Millisecond},
		Now: time.Now, Family: plantFamily(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func TestPlantWorkerSingleSuccessCompletesAfterFarmProof(t *testing.T) {
	calculator, bucket := testPlayerBucket(t)
	orderID := plantOrderID(t, "success")
	payload, _ := plantPayload(t, orderID)
	lease := newFakeLease()
	farm := &fakeParticipant{coordinator: true, responses: []*contractspb.ParticipantResponse{plantResponse(t, orderID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}}
	worker := makePlantWorker(t, lease, fakeDue{refs: []authority.DueWorkRef{plantDueRef(t, orderID)}}, farm, map[string][]byte{orderID: payload}, bucket, calculator)
	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Claimed != 1 || stats.Completed != 1 || lease.completeCalls != 1 {
		t.Fatalf("stats=%+v complete=%d", stats, lease.completeCalls)
	}
	if len(farm.requests) != 1 || farm.requests[0].GetParticipantCommand() != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT {
		t.Fatalf("requests=%v", farm.requests)
	}
	if farm.requests[0].GetRouteHeader().GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM {
		t.Fatalf("route=%+v", farm.requests[0].GetRouteHeader())
	}
}

func TestDirectDispatcherCommittedPlantSkipsSourceOrderReadback(t *testing.T) {
	calculator, bucket := testPlayerBucket(t)
	orderID := plantOrderID(t, "committed-direct")
	payload, decoded := plantPayload(t, orderID)
	hash, err := sourceorder.PlantPayloadHash(decoded)
	if err != nil {
		t.Fatal(err)
	}
	lease := newFakeLease()
	lease.subjectPlayerID = testFarmID
	lease.payload = map[string][]byte{orderID: payload}
	farm := &fakeParticipant{coordinator: true, responses: []*contractspb.ParticipantResponse{
		plantResponse(t, orderID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED),
	}}
	direct, err := NewDirectDispatcher(DirectConfig{
		WorkLease: lease, SourceOrders: failingSourceOrders{}, Route: testRoute, Player: &fakeParticipant{},
		Owner: "direct-plant", Token: "direct-token", BootID: "direct-boot",
		BucketForPlayer: func(playerID uint64) (rediskey.AuthorityBucketRef, error) {
			return testBucketForPlayer(calculator, playerID)
		},
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		MaxItems:      16, CompletionBatchSize: 8, CompletionBatchWait: 20 * time.Millisecond, ItemDeadline: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	direct.SetFarmDest(farm, testFarmRoute, func(playerID uint64) (rediskey.AuthorityBucketRef, error) {
		return testBucketForPlayer(calculator, playerID)
	})
	direct.Start(context.Background())
	defer direct.Close()
	address, err := sourceorder.WorkAddress(bucket, plantFamily(), orderID, workidentity.RouteDomainPlayer, testFarmID)
	if err != nil {
		t.Fatal(err)
	}
	attached := authoritystore.AttachedOrder{
		OrderID: orderID, OrderType: plantFamily(), Payload: payload,
		PayloadHash: authoritystore.PayloadHash(hash), DueAtMs: 1000,
		OperationID: "plant-committed-op", SubjectPlayerID: testFarmID,
	}
	if err := direct.DispatchCommittedOnce(context.Background(), address, attached); err != nil {
		t.Fatal(err)
	}
	if len(farm.requests) != 1 || farm.requests[0].GetParticipantCommand() != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT {
		t.Fatalf("requests=%v", farm.requests)
	}
	if lease.completeCalls != 1 {
		t.Fatalf("completion calls=%d", lease.completeCalls)
	}
}

func TestPlantWorkerTimeoutUnknownDoesNotComplete(t *testing.T) {
	calculator, bucket := testPlayerBucket(t)
	orderID := plantOrderID(t, "timeout")
	payload, _ := plantPayload(t, orderID)
	lease := newFakeLease()
	lease.releaseOnRenew = true
	farm := &fakeParticipant{responses: []*contractspb.ParticipantResponse{
		{ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT, ParticipantInvocationId: mustPlantInvocation(t, orderID), Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN},
		plantResponse(t, orderID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED),
	}}
	worker := makePlantWorker(t, lease, fakeDue{refs: []authority.DueWorkRef{plantDueRef(t, orderID)}}, farm, map[string][]byte{orderID: payload}, bucket, calculator)
	first, firstErr := worker.RunOnce(context.Background())
	if first.Completed != 0 || first.Rescheduled != 1 || firstErr == nil {
		t.Fatalf("first stats=%+v err=%v", first, firstErr)
	}
	worker = makePlantWorker(t, lease, fakeDue{refs: []authority.DueWorkRef{plantDueRef(t, orderID)}}, farm, map[string][]byte{orderID: payload}, bucket, calculator)
	second, secondErr := worker.RunOnce(context.Background())
	if secondErr != nil || second.Completed != 1 || lease.completeCalls != 1 {
		t.Fatalf("second stats=%+v err=%v complete=%d", second, secondErr, lease.completeCalls)
	}
}

func TestPlantWorkerBadPayloadIsolatedFromOtherOrder(t *testing.T) {
	calculator, bucket := testPlayerBucket(t)
	goodID, badID := plantOrderID(t, "good"), plantOrderID(t, "bad")
	goodPayload, _ := plantPayload(t, goodID)
	lease := newFakeLease()
	farm := &fakeParticipant{responses: []*contractspb.ParticipantResponse{plantResponse(t, goodID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}}
	worker := makePlantWorker(t, lease, fakeDue{refs: []authority.DueWorkRef{plantDueRef(t, badID), plantDueRef(t, goodID)}}, farm, map[string][]byte{badID: []byte("not-protobuf"), goodID: goodPayload}, bucket, calculator)
	stats, err := worker.RunOnce(context.Background())
	if err == nil || stats.BadPayload != 1 || stats.Completed != 1 || len(farm.requests) != 1 {
		t.Fatalf("stats=%+v err=%v requests=%d", stats, err, len(farm.requests))
	}
}

func TestPlantWorkerConcurrentClaimsFenceOneFarmCall(t *testing.T) {
	calculator, bucket := testPlayerBucket(t)
	orderID := plantOrderID(t, "race")
	payload, _ := plantPayload(t, orderID)
	lease := newFakeLease()
	lease.subjectPlayerID = testFarmID
	lease.payload = map[string][]byte{orderID: payload}
	farm := &fakeParticipant{responses: []*contractspb.ParticipantResponse{plantResponse(t, orderID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}}
	config := Config{
		WorkLease: lease, DueIndex: fakeDue{refs: []authority.DueWorkRef{plantDueRef(t, orderID)}}, Buckets: []rediskey.AuthorityBucketRef{bucket},
		BucketForPlayer: func(playerID uint64) (rediskey.AuthorityBucketRef, error) {
			return testBucketForPlayer(calculator, playerID)
		},
		Route: testRoute, Farm: farm, FarmRoute: testFarmRoute, Owner: "worker-1", Token: "token-1", BootID: "boot-1",
		MaxConcurrent: 2, LeaseTTL: 2 * time.Second, SafetyMargin: 100 * time.Millisecond, OperationTimeout: time.Second,
		Now: time.Now, Family: plantFamily(),
	}
	workerA, err := NewWorker(config)
	if err != nil {
		t.Fatal(err)
	}
	workerB, err := NewWorker(config)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	var statsA, statsB RoundStats
	go func() { defer wg.Done(); statsA, _ = workerA.RunOnce(context.Background()) }()
	go func() { defer wg.Done(); statsB, _ = workerB.RunOnce(context.Background()) }()
	wg.Wait()
	if statsA.Completed+statsB.Completed != 1 || len(farm.requests) != 1 || lease.completeCalls != 1 {
		t.Fatalf("A=%+v B=%+v requests=%d completes=%d", statsA, statsB, len(farm.requests), lease.completeCalls)
	}
}

func TestPlantDirectDispatcherRPCFailureDoesNotClaimOrComplete(t *testing.T) {
	calculator, bucket := testPlayerBucket(t)
	orderID := plantOrderID(t, "direct-failure")
	payload, decoded := plantPayload(t, orderID)
	lease := newFakeLease()
	lease.payload[orderID] = payload
	lease.subjectPlayerID = testFarmID
	farm := &fakeParticipant{errs: []error{errors.New("Farm RPC unavailable")}}
	hash, err := sourceorder.PlantPayloadHash(decoded)
	if err != nil {
		t.Fatal(err)
	}
	order := authority.SourceOrderSnapshot{
		WorkID: orderID, WorkType: plantFamily(), PlacementKind: "SOURCE_PERSISTENCE_BUCKET",
		BucketVersion: 1, BucketID: bucket.VirtualShard(), SourceAuthorityKind: "PLAYER_ACTOR",
		CanonicalSourceAuthorityID: fmt.Sprint(testFarmID), AuthorityLayoutVersion: bucket.LayoutVersion(),
		ActorRouteDomain: "PLAYER", SubjectPlayerID: decoded.GetOwnerPlayerId(), OperationID: "plant-operation-1",
		WorkVersion: 1, TypedPayload: payload, PayloadHash: fmt.Sprintf("%x", hash[:]), State: "PENDING",
	}
	direct, err := NewDirectDispatcher(DirectConfig{
		WorkLease: lease, SourceOrders: fakeSourceOrders{order: order, bucket: bucket},
		Route: testRoute, Player: &fakeParticipant{}, Farm: farm, FarmRoute: testFarmRoute,
		Owner: "direct-failure", Token: "direct-token",
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		BucketForPlayer: func(playerID uint64) (rediskey.AuthorityBucketRef, error) {
			return testBucketForPlayer(calculator, playerID)
		},
		ItemDeadline: time.Second, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	address, err := sourceorder.WorkAddress(bucket, plantFamily(), orderID, workidentity.RouteDomainPlayer, testFarmID)
	if err != nil {
		t.Fatal(err)
	}
	if err := direct.DispatchOnce(context.Background(), address); err == nil {
		t.Fatal("RPC failure must be surfaced")
	}
	if len(farm.requests) != 1 || lease.claimCalls != 0 || lease.completeCalls != 0 {
		t.Fatalf("requests=%d claims=%d completes=%d", len(farm.requests), lease.claimCalls, lease.completeCalls)
	}
}

func TestPlantDirectDispatcherAttemptsBeforeClaim(t *testing.T) {
	calculator, bucket := testPlayerBucket(t)
	orderID := plantOrderID(t, "direct")
	payload, decoded := plantPayload(t, orderID)
	lease := newFakeLease()
	lease.payload[orderID] = payload
	lease.subjectPlayerID = testFarmID
	farm := &fakeParticipant{responses: []*contractspb.ParticipantResponse{plantResponse(t, orderID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}}
	hash, err := sourceorder.PlantPayloadHash(decoded)
	if err != nil {
		t.Fatal(err)
	}
	order := authority.SourceOrderSnapshot{
		WorkID: orderID, WorkType: plantFamily(), PlacementKind: "SOURCE_PERSISTENCE_BUCKET",
		BucketVersion: 1, BucketID: bucket.VirtualShard(), SourceAuthorityKind: "PLAYER_ACTOR",
		CanonicalSourceAuthorityID: fmt.Sprint(testFarmID), AuthorityLayoutVersion: bucket.LayoutVersion(),
		ActorRouteDomain: "PLAYER", SubjectPlayerID: decoded.GetOwnerPlayerId(), OperationID: "plant-operation-1",
		WorkVersion: 1, TypedPayload: payload, PayloadHash: fmt.Sprintf("%x", hash[:]), State: "PENDING",
	}
	direct, err := NewDirectDispatcher(DirectConfig{
		WorkLease: lease, SourceOrders: fakeSourceOrders{order: order, bucket: bucket},
		Route: testRoute, Player: &fakeParticipant{}, Farm: farm, FarmRoute: testFarmRoute,
		Owner: "direct-1", Token: "direct-token",
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		BucketForPlayer: func(playerID uint64) (rediskey.AuthorityBucketRef, error) {
			return testBucketForPlayer(calculator, playerID)
		},
		LeaseTTL: 2 * time.Second, ItemDeadline: time.Second, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	address, err := sourceorder.WorkAddress(bucket, plantFamily(), orderID, workidentity.RouteDomainPlayer, testFarmID)
	if err != nil {
		t.Fatal(err)
	}
	if err := direct.DispatchOnce(context.Background(), address); err != nil {
		t.Fatal(err)
	}
	if len(farm.requests) != 1 || lease.claimCalls != 1 || lease.completeCalls != 1 {
		t.Fatalf("requests=%d claims=%d completes=%d", len(farm.requests), lease.claimCalls, lease.completeCalls)
	}
}

func mustPlantInvocation(t *testing.T, orderID string) string {
	t.Helper()
	value, err := sourceorder.PlantInvocation(orderID)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
