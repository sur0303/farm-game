package guardrecovery

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	playerpet "farmgame/internal/player/pet"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const bestEffortDispatcherTestPlayerID uint64 = 10001

type bestEffortDispatcherTestCoordinator struct {
	mu       sync.Mutex
	requests []*contractspb.ParticipantRequest
	response *contractspb.ParticipantResponse
	err      error
}

func (f *bestEffortDispatcherTestCoordinator) ExecuteStep(_ context.Context, request *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	f.mu.Lock()
	f.requests = append(f.requests, request)
	response, err := f.response, f.err
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (f *bestEffortDispatcherTestCoordinator) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func (f *bestEffortDispatcherTestCoordinator) lastRequest() *contractspb.ParticipantRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) == 0 {
		return nil
	}
	return f.requests[len(f.requests)-1]
}

func bestEffortDispatcherTestBucket(t *testing.T) rediskey.AuthorityBucketRef {
	t.Helper()
	ns, err := rediskey.NewNamespace("test", "guard-best-effort-dispatcher")
	if err != nil {
		t.Fatal(err)
	}
	profile := rediskey.PhysicalProfile{
		ProfileVersion:         "redis-physical-v1",
		Namespace:              ns,
		AuthorityLayoutVersion: "v1",
		WorkflowLayoutVersion:  "v1",
		WorkflowHashVersion:    1,
		WorkflowBucketCount:    256,
	}
	calculator, err := actorrouting.NewCalculator(
		"classic-farm",
		actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040001},
		1,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := profile.AuthorityBucketForActor(calculator, actorrouting.ActorAddress{
		Domain:  actorrouting.DomainPlayer,
		ActorID: bestEffortDispatcherTestPlayerID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return bucket
}

func bestEffortDispatcherTestOrder(t *testing.T) (authoritystore.AttachedOrder, *contractspb.ApplyGuardSnapshotPayload) {
	t.Helper()
	snapshot := playerpet.GuardSnapshot{
		SchemaVersion:    playerpet.GuardSnapshotSchemaVersion,
		GuardVersion:     7,
		ActiveGuardPetID: 2,
		GuardUntilMs:     123456,
		Rule: playerpet.GuardRuleSnapshot{
			PetConfigVersion:       3,
			GuardRuleVersion:       4,
			TriggerProbabilityBp:   5000,
			InterceptPolicy:        playerpet.InterceptPolicyFullGross,
			RandomAlgorithmVersion: 1,
			RandomKeyVersion:       2,
		},
	}
	snapshotHash := snapshot.Hash()
	orderID, err := identity.SourceOrderID(
		identity.WorkTypeGuardSyncOrder,
		identity.SourceKindPlayerActor,
		"10001",
		"PET_GUARD_SLOT",
		snapshot.GuardVersion,
	)
	if err != nil {
		t.Fatal(err)
	}
	activeGuardPetID := uint32(snapshot.ActiveGuardPetID)
	payload := &contractspb.ApplyGuardSnapshotPayload{
		OrderId:        orderID,
		SourcePlayerId: bestEffortDispatcherTestPlayerID,
		TargetFarmId:   bestEffortDispatcherTestPlayerID,
		SnapshotHash:   append([]byte(nil), snapshotHash[:]...),
		GuardSnapshot: &contractspb.GuardSnapshot{
			SchemaVersion:    uint32(snapshot.SchemaVersion),
			GuardVersion:     snapshot.GuardVersion,
			ActiveGuardPetId: &activeGuardPetID,
			GuardUntilMs:     snapshot.GuardUntilMs,
			GuardRule: &contractspb.GuardRuleSnapshot{
				PetConfigVersion:       snapshot.Rule.PetConfigVersion,
				GuardRuleVersion:       snapshot.Rule.GuardRuleVersion,
				TriggerProbabilityBp:   snapshot.Rule.TriggerProbabilityBp,
				InterceptPolicy:        contractspb.GuardInterceptPolicy(snapshot.Rule.InterceptPolicy),
				RandomAlgorithmVersion: snapshot.Rule.RandomAlgorithmVersion,
				RandomKeyVersion:       snapshot.Rule.RandomKeyVersion,
			},
			SnapshotHash: append([]byte(nil), snapshotHash[:]...),
		},
	}
	rawPayload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	payloadHash, err := hex.DecodeString(ApplyGuardSnapshotPayloadHash(rawPayload))
	if err != nil || len(payloadHash) != len(authoritystore.PayloadHash{}) {
		t.Fatalf("payload hash: %v", err)
	}
	var orderPayloadHash authoritystore.PayloadHash
	copy(orderPayloadHash[:], payloadHash)
	return authoritystore.AttachedOrder{
		OrderID:         orderID,
		OrderType:       FamilyGuardSync,
		Payload:         rawPayload,
		PayloadHash:     orderPayloadHash,
		DueAtMs:         1700000000000,
		OperationID:     "guard-best-effort-operation",
		SubjectPlayerID: bestEffortDispatcherTestPlayerID,
	}, payload
}

func bestEffortDispatcherTestResponse(t *testing.T, payload *contractspb.ApplyGuardSnapshotPayload) *contractspb.ParticipantResponse {
	t.Helper()
	invocation, err := identity.ParticipantInvocationID(
		identity.WorkTypeGuardSyncOrder,
		payload.GetOrderId(),
		identity.StepApplyGuardSnapshot,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.LookupParticipantCommand(
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT,
	)
	if err != nil {
		t.Fatal(err)
	}
	result := &contractspb.ApplyGuardSnapshotResult{
		GuardVersion:           payload.GetGuardSnapshot().GetGuardVersion(),
		SnapshotHash:           append([]byte(nil), payload.GetSnapshotHash()...),
		ApplyStatus:            contractspb.GuardApplyStatus_GUARD_APPLY_STATUS_APPLIED,
		CommittedRecordVersion: 11,
	}
	typedResult, resultHash, err := contracts.ValidateTypedResult(spec, result)
	if err != nil {
		t.Fatal(err)
	}
	return &contractspb.ParticipantResponse{
		ParticipantCommand:      spec.Command,
		ParticipantInvocationId: invocation,
		Outcome:                 contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
		TypedResult:             typedResult,
		ParticipantResultHash:   resultHash,
		CommittedRecordVersion:  result.GetCommittedRecordVersion(),
	}
}

func bestEffortDispatcherTestRoute(_ context.Context, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
	return &logicrpcpb.ActorRouteHeader{
		RouteDomain:          logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM,
		ActorId:              actorID,
		VirtualShard:         0,
		ShardRevision:        1,
		ExpectedHolderId:     "farm-1",
		ExpectedHolderBootId: "farm-boot-1",
		ExpectedWriterEpoch:  1,
	}, nil
}

func newBestEffortDispatcherTest(t *testing.T, route RouteProvider, farm CoordinatorParticipantClient) *BestEffortDispatcher {
	t.Helper()
	bucket := bestEffortDispatcherTestBucket(t)
	dispatcher, err := NewBestEffortDispatcher(BestEffortConfig{
		Route:               route,
		Farm:                farm,
		BucketForPlayer:     func(uint64) (rediskey.AuthorityBucketRef, error) { return bucket, nil },
		OperationTimeout:    time.Second,
		ParticipantBudgetMs: 321,
	})
	if err != nil {
		t.Fatal(err)
	}
	return dispatcher
}

func TestBestEffortDispatcherCarriesTrustedEnvelopeWithoutRedisSourceOrder(t *testing.T) {
	order, payload := bestEffortDispatcherTestOrder(t)
	route := func(ctx context.Context, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
		return bestEffortDispatcherTestRoute(ctx, actorID)
	}
	farm := &bestEffortDispatcherTestCoordinator{response: bestEffortDispatcherTestResponse(t, payload)}
	dispatcher := newBestEffortDispatcherTest(t, route, farm)

	// BestEffortConfig has no SourceOrderReader or AuthorityStore: this dispatch
	// succeeds solely from the immutable AttachedOrder and in-memory fakes.
	if err := dispatcher.Dispatch(context.Background(), order); err != nil {
		t.Fatal(err)
	}
	if got := farm.requestCount(); got != 1 {
		t.Fatalf("Coordinator requests=%d, want 1", got)
	}
	request := farm.lastRequest()
	if request == nil {
		t.Fatal("Coordinator request is nil")
	}
	if request.GetParticipantCommand() != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT ||
		!bytes.Equal(request.GetTypedPayload(), order.Payload) {
		t.Fatalf("request command/payload does not match AttachedOrder: %v", request)
	}
	wantRoute, err := bestEffortDispatcherTestRoute(context.Background(), payload.GetTargetFarmId())
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(request.GetRouteHeader(), wantRoute) {
		t.Fatalf("route=%v, want %v", request.GetRouteHeader(), wantRoute)
	}
	coordinator := request.GetCoordinator()
	if coordinator == nil {
		t.Fatalf("caller context is not CoordinatorWorkContext: %v", request.GetCallerContext())
	}
	bucket := bestEffortDispatcherTestBucket(t)
	address, err := guardWorkAddress(bucket, order.OrderID, order.SubjectPlayerID)
	if err != nil {
		t.Fatal(err)
	}
	wantWork, err := wireGuardWorkAddress(address, bucket.LayoutVersion())
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(coordinator.GetWork(), wantWork) {
		t.Fatalf("Coordinator work=%v, want %v", coordinator.GetWork(), wantWork)
	}
	wantInvocation, err := identity.ParticipantInvocationID(
		identity.WorkTypeGuardSyncOrder,
		order.OrderID,
		identity.StepApplyGuardSnapshot,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if coordinator.GetExpectedWorkVersion() != 1 ||
		coordinator.GetParticipantStep() != contractspb.ParticipantStep_PARTICIPANT_STEP_APPLY_GUARD_SNAPSHOT ||
		coordinator.GetParticipantInvocationId() != wantInvocation ||
		coordinator.GetOperationId() != order.OperationID ||
		coordinator.GetSubjectPlayerId() != order.SubjectPlayerID ||
		coordinator.GetRemainingBudgetMs() != 321 {
		t.Fatalf("Coordinator context=%v", coordinator)
	}

	envelope := request.GetBestEffortSourceOrder()
	if envelope == nil {
		t.Fatal("trusted CreatedSourceOrderEnvelope is missing")
	}
	call, err := contracts.ValidateAndDecodeWithOptions(
		context.Background(),
		request,
		contracts.ParticipantCallerCoordinator,
		contracts.ParticipantIngressOptions{AllowBestEffortSourceOrder: true},
	)
	if err != nil {
		t.Fatalf("envelope is not accepted by the ingress contract: %v", err)
	}
	if call.BestEffortSourceOrder == nil || !proto.Equal(call.BestEffortSourceOrder, envelope) {
		t.Fatalf("validated source-order envelope=%v, request envelope=%v", call.BestEffortSourceOrder, envelope)
	}
	if !proto.Equal(envelope.GetWork(), coordinator.GetWork()) ||
		envelope.GetWorkVersion() != 1 ||
		envelope.GetOperationId() != order.OperationID ||
		envelope.GetSubjectPlayerId() != order.SubjectPlayerID ||
		!bytes.Equal(envelope.GetTypedPayload(), order.Payload) ||
		!bytes.Equal(envelope.GetPayloadHash(), order.PayloadHash[:]) ||
		envelope.GetDueAtMs() != order.DueAtMs {
		t.Fatalf("untrusted or mismatched envelope=%v", envelope)
	}
	if envelope.GetWork().GetSourceAuthority().GetAuthorityKind() != contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR ||
		envelope.GetWork().GetSourceAuthority().GetCanonicalAuthorityId() != "10001" ||
		envelope.GetWork().GetSourceAuthority().GetActorRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER {
		t.Fatalf("envelope source authority=%v", envelope.GetWork().GetSourceAuthority())
	}
}

func TestBestEffortDispatcherFailsForRoutePayloadAndTerminalProofErrors(t *testing.T) {
	routeErr := errors.New("route unavailable")
	tests := []struct {
		name           string
		routeErr       error
		mutateOrder    func(*authoritystore.AttachedOrder)
		mutateResponse func(*contractspb.ParticipantResponse)
		wantErr        error
		wantRouteCalls int
		wantRPCCalls   int
	}{
		{
			name:           "route",
			routeErr:       routeErr,
			wantErr:        routeErr,
			wantRouteCalls: 1,
			wantRPCCalls:   0,
		},
		{
			name: "payload binding",
			mutateOrder: func(order *authoritystore.AttachedOrder) {
				order.PayloadHash[0] ^= 0xff
			},
			wantErr:        ErrPayloadInvalid,
			wantRouteCalls: 0,
			wantRPCCalls:   0,
		},
		{
			name: "terminal proof",
			mutateResponse: func(response *contractspb.ParticipantResponse) {
				response.ParticipantResultHash[0] ^= 0xff
			},
			wantErr:        ErrTerminalProof,
			wantRouteCalls: 1,
			wantRPCCalls:   1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			order, payload := bestEffortDispatcherTestOrder(t)
			if test.mutateOrder != nil {
				test.mutateOrder(&order)
			}
			response := bestEffortDispatcherTestResponse(t, payload)
			if test.mutateResponse != nil {
				test.mutateResponse(response)
			}
			farm := &bestEffortDispatcherTestCoordinator{response: response}
			routeCalls := 0
			route := func(ctx context.Context, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
				routeCalls++
				if test.routeErr != nil {
					return nil, test.routeErr
				}
				return bestEffortDispatcherTestRoute(ctx, actorID)
			}
			dispatcher := newBestEffortDispatcherTest(t, route, farm)

			err := dispatcher.Dispatch(context.Background(), order)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error=%v, want errors.Is(..., %v)", err, test.wantErr)
			}
			if routeCalls != test.wantRouteCalls {
				t.Fatalf("route calls=%d, want %d", routeCalls, test.wantRouteCalls)
			}
			if got := farm.requestCount(); got != test.wantRPCCalls {
				t.Fatalf("Coordinator requests=%d, want %d", got, test.wantRPCCalls)
			}
		})
	}
}
