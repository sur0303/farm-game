package theft

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/identity"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestTheftRPCStaleWorkProofWrapsBeforeGenericClassifier locks the
// WF-WORKVERSION-PROOF-RACE ordering in both theft invokers: a participant
// WorkVersion proof mismatch (FailedPrecondition "participant proof:
// WorkVersion ...") must surface as workflow.ErrStaleWorkProof — the engine
// reload primitive — and must NOT be swallowed by the conservative generic
// classifier (which would otherwise turn it into a response-lost
// StepUnknown pause or an idempotency-conflict business REJECTED).
func TestTheftRPCStaleWorkProofWrapsBeforeGenericClassifier(t *testing.T) {
	call := theftRPCStepCall(t, 0)
	stale := status.Error(codes.FailedPrecondition, "participant proof: WorkVersion mismatch")

	// Coordinator fast path.
	coordinator := &rpcClientFunc{err: stale}
	coordinatorInvoker, err := NewCoordinatorInvoker(coordinator, theftRPCRoute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinatorInvoker.Invoke(context.Background(), call); !errors.Is(err, workflow.ErrStaleWorkProof) {
		t.Fatalf("coordinator invoker did not wrap stale proof: %v", err)
	}

	// Worker recovery path.
	recovery := &resumeStepClient{err: stale}
	recoveryInvoker, err := NewWorkerRecoveryInvoker(recovery, theftRPCRoute, WorkerLeaseContext{
		CallerInstanceID: "instance-1", CallerBootID: "boot-1",
		LeaseOwnerToken: "token-1", LeaseEpoch: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recoveryInvoker.Invoke(context.Background(), call); !errors.Is(err, workflow.ErrStaleWorkProof) {
		t.Fatalf("worker recovery invoker did not wrap stale proof: %v", err)
	}

	// Idempotency conflict stays a business REJECTED (not stale).
	conflict := &rpcClientFunc{err: status.Error(codes.FailedPrecondition, ErrorClassParticipantIdempotencyConflict)}
	conflictInvoker, err := NewCoordinatorInvoker(conflict, theftRPCRoute)
	if err != nil {
		t.Fatal(err)
	}
	result, err := conflictInvoker.Invoke(context.Background(), call)
	if err != nil || result.Outcome != workflow.StepOutcomeRejected || result.ErrorClass != ErrorClassParticipantIdempotencyConflict {
		t.Fatalf("idempotency conflict result=%+v err=%v", result, err)
	}
}

func theftRPCStepCall(t *testing.T, stepIndex int) workflow.StepCall {
	t.Helper()
	cycle, err := workidentity.StableOpaqueID("theft-rpc-test-cycle", "one")
	if err != nil {
		t.Fatal(err)
	}
	permituID, err := workidentity.StableOpaqueID("theft-rpc-test-permit", "one")
	if err != nil {
		t.Fatal(err)
	}
	root, err := BuildRoot(TheftInput{
		VisitorPlayerID: 10001, OwnerPlayerID: 20002, TargetFarmID: 20002,
		PlotID: 3, CropCycleID: cycle, OperationID: "theft-rpc-test",
		ActionPermitSnapshot: &contractspb.ActionPermitSnapshot{
			PermitId: permituID, VisitorPlayerId: 10001, OwnerPlayerId: 20002,
			TargetFarmId: 20002, OperationId: "theft-rpc-test",
			ActionScope:                    contractspb.FriendActionScope_FRIEND_ACTION_SCOPE_THEFT,
			FriendRelationEpoch:            9,
			CanonicalPayloadHash:           bytes.Repeat([]byte{0x11}, sha256.Size),
			TargetBindingHash:              bytes.Repeat([]byte{0x22}, sha256.Size),
			IssuedAtAuthorityMs:            1_000,
			ExpireAtAuthorityMs:            2_000,
			ExecutionAcceptedAtAuthorityMs: 1_100,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	address, err := root.Address(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	steps := []string{
		string(identity.StepDecideTheft),
		string(identity.StepSettleTheftVisitor),
		string(identity.StepCreditGuardIncome),
	}
	step := steps[stepIndex]
	invocation, err := workidentity.ParticipantInvocationID(string(identity.WorkTypeTheft), root.WorkflowID, step, uint64(stepIndex+1))
	if err != nil {
		t.Fatal(err)
	}
	return workflow.StepCall{
		Work: address, WorkVersion: 1, StepIndex: stepIndex, StepName: step,
		InvocationID: invocation, Payload: root.PayloadBytes, PayloadHash: root.PayloadHash,
		OperationID: "theft-rpc-test", SubjectPlayerID: 10001,
	}
}

func theftRPCRoute(_ context.Context, _ workflow.StepCall, command contractspb.ParticipantCommandType, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
	domain := logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM
	switch command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME:
		domain = logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER
	}
	return &logicrpcpb.ActorRouteHeader{RouteDomain: domain, ActorId: actorID, VirtualShard: 1, ShardRevision: 1, ExpectedHolderId: "holder", ExpectedHolderBootId: "boot", ExpectedWriterEpoch: 1}, nil
}

type rpcClientFunc struct {
	err error
}

func (c *rpcClientFunc) ExecuteStep(_ context.Context, _ *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	return nil, c.err
}

type resumeStepClient struct {
	err error
}

func (c *resumeStepClient) ResumeStep(_ context.Context, _ *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	return nil, c.err
}
