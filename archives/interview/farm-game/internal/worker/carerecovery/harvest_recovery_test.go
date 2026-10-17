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
	"farmgame/internal/identity"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/sourceorder"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

func harvestFamily() string { return string(identity.WorkTypeHarvest) }

func harvestDueRef(t *testing.T, workID string) authority.DueWorkRef {
	t.Helper()
	member, err := workidentity.EncodeWorkRefV1(harvestFamily(), workID)
	if err != nil {
		t.Fatal(err)
	}
	return authority.DueWorkRef{Ref: workidentity.WorkRef{WorkType: harvestFamily(), WorkID: workID}, DueAtMs: time.Now().UnixMilli(), Member: member}
}

func harvestOrderID(t *testing.T, name string) string {
	t.Helper()
	value, err := identity.SourceOrderID(identity.WorkTypeHarvest, identity.SourceKindFarmActor, fmt.Sprint(testFarmID), "harvest-"+name, 1)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func harvestPayload(t *testing.T, orderID string) ([]byte, *contractspb.GrantHarvestRewardPayload) {
	t.Helper()
	cycleID, err := identity.CropCycleID(orderID)
	if err != nil {
		t.Fatal(err)
	}
	decision := &contractspb.HarvestDecision{
		WorkflowId: orderID, OwnerPlayerId: testFarmID, FarmId: testFarmID,
		PlotId: 3, CropCycleId: cycleID, CropDefinitionId: 1, RewardItemId: 2001,
		RewardQuantity: 10, RewardExp: 5, CropRuleVersion: 1,
	}
	hash, err := contracts.CanonicalHash(decision)
	if err != nil {
		t.Fatal(err)
	}
	decision.HarvestDecisionHash = append([]byte(nil), hash[:]...)
	payload := &contractspb.GrantHarvestRewardPayload{
		PlayerId: testFarmID, CommitHarvestResultHash: bytes.Repeat([]byte{7}, 32), HarvestDecision: decision,
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sourceorder.DecodeGrantHarvestPayload(orderID, raw); err != nil {
		t.Fatal(err)
	}
	return raw, payload
}

func harvestResponse(t *testing.T, orderID string, outcome contractspb.ParticipantOutcome) *contractspb.ParticipantResponse {
	t.Helper()
	invocation, err := sourceorder.HarvestInvocation(orderID)
	if err != nil {
		t.Fatal(err)
	}
	result := &contractspb.GrantHarvestRewardResult{
		CropCycleId: "cycle", RewardItemId: 2001, GrantedQuantity: 10, GrantedExp: 5,
		PlayerRecordVersion: 9,
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD)
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

func harvestInvocation(t *testing.T, orderID string) string {
	t.Helper()
	value, err := sourceorder.HarvestInvocation(orderID)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func makeHarvestWorker(t *testing.T, lease *fakeLease, due fakeDue, participant *fakeParticipant, payloads map[string][]byte, bucket rediskey.AuthorityBucketRef, calculator *actorrouting.Calculator) *Worker {
	t.Helper()
	lease.payload = payloads
	lease.subjectPlayerID = testFarmID
	worker, err := NewWorker(Config{
		WorkLease: lease, DueIndex: due, Buckets: []rediskey.AuthorityBucketRef{bucket},
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) {
			return testBucketForFarm(calculator, farmID)
		},
		Route: testRoute, Player: participant, Owner: "worker-1", Token: "token-1", BootID: "boot-1",
		ClaimLimit: 32, MaxConcurrent: 4, LeaseTTL: 2 * time.Second, SafetyMargin: 100 * time.Millisecond,
		OperationTimeout: time.Second, Backoff: workflow.BackoffConfig{Base: time.Millisecond, Max: 5 * time.Millisecond},
		Now: time.Now, Family: harvestFamily(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func TestHarvestWorkerSingleSuccessCompletesAfterProof(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	orderID := harvestOrderID(t, "success")
	payload, _ := harvestPayload(t, orderID)
	lease := newFakeLease()
	participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{harvestResponse(t, orderID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}}
	worker := makeHarvestWorker(t, lease, fakeDue{refs: []authority.DueWorkRef{harvestDueRef(t, orderID)}}, participant, map[string][]byte{orderID: payload}, bucket, calculator)
	stats, err := worker.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Claimed != 1 || stats.Completed != 1 || stats.Rescheduled != 0 || lease.completeCalls != 1 {
		t.Fatalf("stats=%+v complete=%d", stats, lease.completeCalls)
	}
	if len(participant.requests) != 1 {
		t.Fatalf("requests=%d", len(participant.requests))
	}
	request := participant.requests[0]
	if request.GetParticipantCommand() != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD ||
		request.GetRouteHeader().GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER {
		t.Fatalf("request=%+v", request)
	}
	if _, ok := request.GetCallerContext().(*contractspb.ParticipantRequest_Recovery); !ok {
		t.Fatalf("caller=%T", request.GetCallerContext())
	}
}

func TestHarvestWorkerTimeoutUnknownRetriesSameInvocation(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	orderID := harvestOrderID(t, "timeout")
	payload, _ := harvestPayload(t, orderID)
	lease := newFakeLease()
	lease.releaseOnRenew = true
	participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{
		{ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD, ParticipantInvocationId: harvestInvocation(t, orderID), Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN},
		harvestResponse(t, orderID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED),
	}}
	worker := makeHarvestWorker(t, lease, fakeDue{refs: []authority.DueWorkRef{harvestDueRef(t, orderID)}}, participant, map[string][]byte{orderID: payload}, bucket, calculator)
	first, firstErr := worker.RunOnce(context.Background())
	if first.Completed != 0 || first.Rescheduled != 1 || firstErr == nil {
		t.Fatalf("first stats=%+v err=%v", first, firstErr)
	}
	worker = makeHarvestWorker(t, lease, fakeDue{refs: []authority.DueWorkRef{harvestDueRef(t, orderID)}}, participant, map[string][]byte{orderID: payload}, bucket, calculator)
	second, secondErr := worker.RunOnce(context.Background())
	if secondErr != nil || second.Completed != 1 || lease.completeCalls != 1 {
		t.Fatalf("second stats=%+v err=%v complete=%d", second, secondErr, lease.completeCalls)
	}
	firstInvocation := participant.requests[0].GetCallerContext().(*contractspb.ParticipantRequest_Recovery).Recovery.GetParticipantInvocationId()
	secondInvocation := participant.requests[1].GetCallerContext().(*contractspb.ParticipantRequest_Recovery).Recovery.GetParticipantInvocationId()
	if firstInvocation != secondInvocation || firstInvocation != harvestInvocation(t, orderID) {
		t.Fatalf("invocations %q %q", firstInvocation, secondInvocation)
	}
}

func TestHarvestWorkerResponseLossReplaysSameInvocation(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	orderID := harvestOrderID(t, "response-loss")
	payload, _ := harvestPayload(t, orderID)
	lease := newFakeLease()
	lease.releaseOnRenew = true
	participant := &fakeParticipant{
		errs:      []error{errors.New("response lost after Player commit")},
		responses: []*contractspb.ParticipantResponse{harvestResponse(t, orderID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)},
	}
	worker := makeHarvestWorker(t, lease, fakeDue{refs: []authority.DueWorkRef{harvestDueRef(t, orderID)}}, participant, map[string][]byte{orderID: payload}, bucket, calculator)
	first, firstErr := worker.RunOnce(context.Background())
	if firstErr == nil || first.Unknown != 1 || first.Completed != 0 || lease.completeCalls != 0 {
		t.Fatalf("first stats=%+v err=%v complete=%d", first, firstErr, lease.completeCalls)
	}
	worker = makeHarvestWorker(t, lease, fakeDue{refs: []authority.DueWorkRef{harvestDueRef(t, orderID)}}, participant, map[string][]byte{orderID: payload}, bucket, calculator)
	second, secondErr := worker.RunOnce(context.Background())
	if secondErr != nil || second.Completed != 1 || lease.completeCalls != 1 || len(participant.requests) != 2 {
		t.Fatalf("second stats=%+v err=%v complete=%d requests=%d", second, secondErr, lease.completeCalls, len(participant.requests))
	}
}

func TestHarvestWorkerBadPayloadIsolatedFromOtherOrder(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	goodID, badID := harvestOrderID(t, "good"), harvestOrderID(t, "bad")
	goodPayload, _ := harvestPayload(t, goodID)
	lease := newFakeLease()
	participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{harvestResponse(t, goodID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}}
	worker := makeHarvestWorker(t, lease, fakeDue{refs: []authority.DueWorkRef{harvestDueRef(t, badID), harvestDueRef(t, goodID)}}, participant, map[string][]byte{badID: []byte("not-protobuf"), goodID: goodPayload}, bucket, calculator)
	stats, err := worker.RunOnce(context.Background())
	if err == nil || stats.BadPayload != 1 || stats.Completed != 1 || len(participant.requests) != 1 {
		t.Fatalf("stats=%+v err=%v requests=%d", stats, err, len(participant.requests))
	}
}

func TestHarvestWorkerConcurrentClaimsFenceOneParticipant(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	orderID := harvestOrderID(t, "race")
	payload, _ := harvestPayload(t, orderID)
	lease := newFakeLease()
	lease.subjectPlayerID = testFarmID
	lease.payload = map[string][]byte{orderID: payload}
	participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{harvestResponse(t, orderID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}}
	config := Config{
		WorkLease: lease, DueIndex: fakeDue{refs: []authority.DueWorkRef{harvestDueRef(t, orderID)}}, Buckets: []rediskey.AuthorityBucketRef{bucket},
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		Route:         testRoute, Player: participant, Owner: "worker-1", Token: "token-1", BootID: "boot-1",
		MaxConcurrent: 2, LeaseTTL: 2 * time.Second, SafetyMargin: 100 * time.Millisecond, OperationTimeout: time.Second,
		Now: time.Now, Family: harvestFamily(),
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
	if statsA.Completed+statsB.Completed != 1 || len(participant.requests) != 1 || lease.completeCalls != 1 {
		t.Fatalf("A=%+v B=%+v requests=%d completes=%d", statsA, statsB, len(participant.requests), lease.completeCalls)
	}
}

func TestHarvestDirectDispatcherAttemptsBeforeClaim(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	orderID := harvestOrderID(t, "direct")
	payload, decoded := harvestPayload(t, orderID)
	lease := newFakeLease()
	lease.payload[orderID] = payload
	lease.subjectPlayerID = testFarmID
	participant := &fakeParticipant{responses: []*contractspb.ParticipantResponse{harvestResponse(t, orderID, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED)}}
	hash, err := sourceorder.HarvestPayloadHash(decoded)
	if err != nil {
		t.Fatal(err)
	}
	order := authority.SourceOrderSnapshot{
		WorkID: orderID, WorkType: harvestFamily(), PlacementKind: "SOURCE_PERSISTENCE_BUCKET",
		BucketVersion: 1, BucketID: bucket.VirtualShard(), SourceAuthorityKind: "FARM_ACTOR",
		CanonicalSourceAuthorityID: fmt.Sprint(testFarmID), AuthorityLayoutVersion: bucket.LayoutVersion(),
		ActorRouteDomain: "FARM", SubjectPlayerID: decoded.GetPlayerId(), OperationID: "harvest-operation-1",
		WorkVersion: 1, TypedPayload: payload, PayloadHash: fmt.Sprintf("%x", hash[:]), State: "PENDING",
	}
	direct, err := NewDirectDispatcher(DirectConfig{
		WorkLease: lease, SourceOrders: fakeSourceOrders{order: order, bucket: bucket}, Route: testRoute, Player: participant,
		Owner: "direct-1", Token: "direct-token",
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		LeaseTTL:      2 * time.Second, ItemDeadline: time.Second, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	address, err := sourceorder.WorkAddress(bucket, harvestFamily(), orderID, workidentity.RouteDomainFarm, testFarmID)
	if err != nil {
		t.Fatal(err)
	}
	if err := direct.DispatchOnce(context.Background(), address); err != nil {
		t.Fatal(err)
	}
	if len(participant.requests) != 1 || lease.claimCalls != 1 || lease.completeCalls != 1 {
		t.Fatalf("requests=%d claims=%d completes=%d", len(participant.requests), lease.claimCalls, lease.completeCalls)
	}
}

func TestHarvestDirectDispatcherRPCFailureDoesNotClaimOrComplete(t *testing.T) {
	_, calculator, bucket := testBucket(t)
	orderID := harvestOrderID(t, "direct-failure")
	payload, decoded := harvestPayload(t, orderID)
	lease := newFakeLease()
	lease.payload[orderID] = payload
	lease.subjectPlayerID = testFarmID
	participant := &fakeParticipant{errs: []error{errors.New("Player RPC unavailable")}}
	hash, err := sourceorder.HarvestPayloadHash(decoded)
	if err != nil {
		t.Fatal(err)
	}
	order := authority.SourceOrderSnapshot{
		WorkID: orderID, WorkType: harvestFamily(), PlacementKind: "SOURCE_PERSISTENCE_BUCKET",
		BucketVersion: 1, BucketID: bucket.VirtualShard(), SourceAuthorityKind: "FARM_ACTOR",
		CanonicalSourceAuthorityID: fmt.Sprint(testFarmID), AuthorityLayoutVersion: bucket.LayoutVersion(),
		ActorRouteDomain: "FARM", SubjectPlayerID: decoded.GetPlayerId(), OperationID: "harvest-operation-1",
		WorkVersion: 1, TypedPayload: payload, PayloadHash: fmt.Sprintf("%x", hash[:]), State: "PENDING",
	}
	direct, err := NewDirectDispatcher(DirectConfig{
		WorkLease: lease, SourceOrders: fakeSourceOrders{order: order, bucket: bucket}, Route: testRoute, Player: participant,
		Owner: "direct-failure", Token: "direct-token",
		BucketForFarm: func(farmID uint64) (rediskey.AuthorityBucketRef, error) { return testBucketForFarm(calculator, farmID) },
		ItemDeadline:  time.Second, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	address, err := sourceorder.WorkAddress(bucket, harvestFamily(), orderID, workidentity.RouteDomainFarm, testFarmID)
	if err != nil {
		t.Fatal(err)
	}
	if err := direct.DispatchOnce(context.Background(), address); err == nil {
		t.Fatal("RPC failure must be surfaced")
	}
	if len(participant.requests) != 1 || lease.claimCalls != 0 || lease.completeCalls != 0 {
		t.Fatalf("requests=%d claims=%d completes=%d", len(participant.requests), lease.claimCalls, lease.completeCalls)
	}
}
