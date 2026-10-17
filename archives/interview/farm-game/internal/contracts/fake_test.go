package contracts

import (
	"context"
	"testing"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/identity"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func participantTestUint64(value uint64) *uint64 { return &value }

func mustInvocation(t *testing.T, workType identity.WorkType, workID string, step identity.ParticipantStep) string {
	t.Helper()
	id, err := identity.ParticipantInvocationID(workType, workID, step, 1)
	if err != nil {
		t.Fatalf("invocation: %v", err)
	}
	return id
}

func newReserveSeedRequest(t *testing.T, workID string, payload *contractspb.ReserveSeedPayload) *contractspb.ParticipantRequest {
	t.Helper()
	raw, err := proto.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &contractspb.ParticipantRequest{
		RouteHeader: &logicrpcpb.ActorRouteHeader{
			RouteDomain:          logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER,
			ActorId:              payload.GetPlayerId(),
			VirtualShard:         7,
			ShardRevision:        1,
			ExpectedHolderId:     "contracts-holder",
			ExpectedHolderBootId: "contracts-boot",
			ExpectedWriterEpoch:  1,
			ExpectedLeaseEpoch:   1,
			ExpectedLeaseUntilMs: participantTestUint64(1_900_000_000_000),
		},
		CallerContext: &contractspb.ParticipantRequest_Coordinator{
			Coordinator: &contractspb.CoordinatorWorkContext{
				Work: &contractspb.WorkAddress{
					WorkId:        workID,
					WorkType:      contractspb.WorkType_WORK_TYPE_PLANT,
					PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET,
					BucketVersion: 1,
				},
				ExpectedWorkVersion:     1,
				ParticipantStep:         contractspb.ParticipantStep_PARTICIPANT_STEP_RESERVE_SEED,
				ParticipantInvocationId: mustInvocation(t, identity.WorkTypePlant, workID, identity.StepReserveSeed),
				OperationId:             "op-0001",
				SubjectPlayerId:         payload.GetPlayerId(),
				RemainingBudgetMs:       1000,
			},
		},
		ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED,
		TypedPayload:       raw,
	}
}

func appliedSeedHandler(ctx context.Context, req *contractspb.ParticipantRequest, payload proto.Message) (*StepOutcome, error) {
	p := payload.(*contractspb.ReserveSeedPayload)
	return &StepOutcome{
		Outcome:                contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
		CommittedRecordVersion: 1,
		Result: &contractspb.ReserveSeedResult{
			ReservationId:              p.GetReservationId(),
			SeedItemId:                 p.GetSeedItemId(),
			ReservedQuantity:           p.GetQuantity(),
			AvailableSeedQuantityAfter: 9,
			PlayerRecordVersion:        7,
		},
	}, nil
}

func TestFakeReceiptReplaySamePayload(t *testing.T) {
	server := NewFakeParticipantServer()
	server.RegisterHandler(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED, appliedSeedHandler)
	client := NewFakeParticipantClient(server)

	payload := &contractspb.ReserveSeedPayload{
		PlayerId: 10001, FarmId: 20002, PlotId: 3,
		ReservationId: "sid1_2xmYbjcijkoeMWC2ArJClmGQ8-8pzLJbaCQhD3JwYvo",
		SeedItemId:    701, Quantity: 1, CropRuleVersion: 42,
	}
	workID := "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q"
	req := newReserveSeedRequest(t, workID, payload)

	first, err := client.ExecuteStep(context.Background(), req)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if first.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("first outcome = %v", first.GetOutcome())
	}
	if first.GetReplayedFromReceipt() {
		t.Fatal("first call must not be a replay")
	}
	if first.GetCommittedRecordVersion() != 1 {
		t.Fatalf("first version = %d", first.GetCommittedRecordVersion())
	}

	second, err := client.ExecuteStep(context.Background(), req)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if !second.GetReplayedFromReceipt() {
		t.Fatal("same invocation + same payload must replay from receipt")
	}
	if second.GetOutcome() != first.GetOutcome() ||
		second.GetCommittedRecordVersion() != first.GetCommittedRecordVersion() ||
		string(second.GetTypedResult()) != string(first.GetTypedResult()) ||
		string(second.GetParticipantResultHash()) != string(first.GetParticipantResultHash()) {
		t.Fatal("replay must return the original outcome/result/hash/version")
	}
}

func TestFakeReceiptConflictDifferentPayload(t *testing.T) {
	server := NewFakeParticipantServer()
	server.RegisterHandler(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED, appliedSeedHandler)
	client := NewFakeParticipantClient(server)

	workID := "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q"
	payload := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1}
	if _, err := client.ExecuteStep(context.Background(), newReserveSeedRequest(t, workID, payload)); err != nil {
		t.Fatalf("first: %v", err)
	}

	changed := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 4, SeedItemId: 701, Quantity: 1}
	_, err := client.ExecuteStep(context.Background(), newReserveSeedRequest(t, workID, changed))
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition || st.Message() != "PARTICIPANT_IDEMPOTENCY_CONFLICT" {
		t.Fatalf("same invocation + different payload must conflict, got %v", err)
	}
}

func TestFakeRejectsCallerContextMismatch(t *testing.T) {
	server := NewFakeParticipantServer()
	client := NewFakeParticipantClient(server)
	workID := "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q"
	payload := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1}
	req := newReserveSeedRequest(t, workID, payload)

	if _, err := client.ResumeStep(context.Background(), req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ResumeStep with coordinator context must fail, got %v", err)
	}

	recovery := &contractspb.ParticipantRequest{
		RouteHeader:        req.GetRouteHeader(),
		ParticipantCommand: req.GetParticipantCommand(),
		TypedPayload:       req.GetTypedPayload(),
		CallerContext: &contractspb.ParticipantRequest_Recovery{
			Recovery: &contractspb.InternalCallerContext{
				CallerType:              contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY,
				CallerInstanceId:        "worker-1",
				CallerBootId:            "boot-1",
				Work:                    req.GetCoordinator().GetWork(),
				ExpectedWorkVersion:     1,
				ParticipantStep:         contractspb.ParticipantStep_PARTICIPANT_STEP_RESERVE_SEED,
				ParticipantInvocationId: req.GetCoordinator().GetParticipantInvocationId(),
				LeaseOwnerToken:         "tok",
				LeaseEpoch:              9,
				RemainingBudgetMs:       1000,
			},
		},
	}
	if _, err := client.ExecuteStep(context.Background(), recovery); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ExecuteStep with recovery context must fail, got %v", err)
	}
}

func TestFakeRejectsInvocationBindingMismatch(t *testing.T) {
	server := NewFakeParticipantServer()
	server.RegisterHandler(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED, appliedSeedHandler)
	client := NewFakeParticipantClient(server)
	workID := "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q"
	payload := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1}
	req := newReserveSeedRequest(t, workID, payload)
	req.GetCoordinator().ParticipantInvocationId = "sid1_forged0000000000000000000000000000000000"

	if _, err := client.ExecuteStep(context.Background(), req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("forged invocation id must fail closed, got %v", err)
	}
}

func TestFakeRejectsWorkAddressShapeMismatch(t *testing.T) {
	server := NewFakeParticipantServer()
	server.RegisterHandler(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED, appliedSeedHandler)
	client := NewFakeParticipantClient(server)
	workID := "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q"
	payload := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1}
	req := newReserveSeedRequest(t, workID, payload)
	req.GetCoordinator().GetWork().PlacementKind = contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET

	if _, err := client.ExecuteStep(context.Background(), req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("placement mismatch must fail closed, got %v", err)
	}
}

func TestFakeUnknownCommandFailsClosed(t *testing.T) {
	server := NewFakeParticipantServer()
	client := NewFakeParticipantClient(server)
	req := &contractspb.ParticipantRequest{
		ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_UNSPECIFIED,
		CallerContext:      &contractspb.ParticipantRequest_Coordinator{Coordinator: &contractspb.CoordinatorWorkContext{}},
	}
	if _, err := client.ExecuteStep(context.Background(), req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown command must fail closed, got %v", err)
	}
}

func TestFakeTimeoutUnknownNotPersisted(t *testing.T) {
	server := NewFakeParticipantServer()
	calls := 0
	server.RegisterHandler(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED,
		func(ctx context.Context, req *contractspb.ParticipantRequest, payload proto.Message) (*StepOutcome, error) {
			calls++
			if calls == 1 {
				return &StepOutcome{Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN}, nil
			}
			return appliedSeedHandler(ctx, req, payload)
		})
	client := NewFakeParticipantClient(server)
	workID := "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q"
	payload := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1}
	req := newReserveSeedRequest(t, workID, payload)

	first, err := client.ExecuteStep(context.Background(), req)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if first.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN {
		t.Fatalf("first outcome = %v", first.GetOutcome())
	}
	second, err := client.ExecuteStep(context.Background(), req)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second.GetReplayedFromReceipt() {
		t.Fatal("TIMEOUT_UNKNOWN must not persist a receipt")
	}
	if second.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
		t.Fatalf("second outcome = %v", second.GetOutcome())
	}
	if calls != 2 {
		t.Fatalf("handler calls = %d, want 2", calls)
	}
}

func TestFakeRejectsMalformedPayload(t *testing.T) {
	server := NewFakeParticipantServer()
	server.RegisterHandler(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED, appliedSeedHandler)
	client := NewFakeParticipantClient(server)
	workID := "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q"
	payload := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1}
	req := newReserveSeedRequest(t, workID, payload)
	req.TypedPayload = []byte{0xff, 0xff, 0xff}

	if _, err := client.ExecuteStep(context.Background(), req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed payload must fail, got %v", err)
	}
}

func TestFakeWorkerRecoveryPathReplays(t *testing.T) {
	server := NewFakeParticipantServer()
	server.RegisterHandler(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED, appliedSeedHandler)
	client := NewFakeParticipantClient(server)
	workID := "sid1_flbe_o4l2GM6HW-8ovsvzKsdaAzvZS6Colc0JdVe77Q"
	payload := &contractspb.ReserveSeedPayload{PlayerId: 10001, FarmId: 20002, PlotId: 3, SeedItemId: 701, Quantity: 1}
	coordinatorReq := newReserveSeedRequest(t, workID, payload)

	if _, err := client.ExecuteStep(context.Background(), coordinatorReq); err != nil {
		t.Fatalf("coordinator: %v", err)
	}

	// A worker taking over with the same invocation must hit the receipt
	// even through the recovery entrypoint (contract 29 §7.5).
	recoveryReq := &contractspb.ParticipantRequest{
		RouteHeader:        coordinatorReq.GetRouteHeader(),
		ParticipantCommand: coordinatorReq.GetParticipantCommand(),
		TypedPayload:       coordinatorReq.GetTypedPayload(),
		CallerContext: &contractspb.ParticipantRequest_Recovery{
			Recovery: &contractspb.InternalCallerContext{
				CallerType:              contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY,
				CallerInstanceId:        "worker-2",
				CallerBootId:            "boot-2",
				Work:                    coordinatorReq.GetCoordinator().GetWork(),
				ExpectedWorkVersion:     1,
				ParticipantStep:         contractspb.ParticipantStep_PARTICIPANT_STEP_RESERVE_SEED,
				ParticipantInvocationId: coordinatorReq.GetCoordinator().GetParticipantInvocationId(),
				LeaseOwnerToken:         "tok",
				LeaseEpoch:              10,
				RemainingBudgetMs:       1000,
			},
		},
	}
	resp, err := client.ResumeStep(context.Background(), recoveryReq)
	if err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if !resp.GetReplayedFromReceipt() {
		t.Fatal("worker recovery with same invocation must replay receipt")
	}
}
