package harvest

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestHarvestRPCMapsParticipantIdempotencyConflictToRejectedResult(t *testing.T) {
	call := rpcTestStepCall(t)
	client := &rpcSequenceClient{err: status.Error(codes.FailedPrecondition, ErrorClassParticipantIdempotencyConflict)}
	invoker, err := NewRPCInvoker(client, rpcTestRoute)
	if err != nil {
		t.Fatal(err)
	}
	result, err := invoker.Invoke(context.Background(), call)
	if err != nil || result.Outcome != workflow.StepOutcomeRejected || result.ErrorClass != ErrorClassParticipantIdempotencyConflict {
		t.Fatalf("conflict result=%+v err=%v", result, err)
	}
	if client.calls != 1 {
		t.Fatalf("idempotency conflict was retried: calls=%d", client.calls)
	}
}

func TestHarvestRPCRetriesOnlyProvenNotSentAndLeavesResponseLossForUnknown(t *testing.T) {
	call := rpcTestStepCall(t)
	notSent := &rpcSequenceClient{
		errs:     []error{status.Error(codes.Unavailable, "UNAVAILABLE_NOT_SENT")},
		response: rpcFarmAppliedResponse,
	}
	invoker, err := NewRPCInvoker(notSent, rpcTestRoute)
	if err != nil {
		t.Fatal(err)
	}
	result, err := invoker.Invoke(context.Background(), call)
	if err != nil || result.Outcome != workflow.StepOutcomeApplied || notSent.calls != 2 {
		t.Fatalf("not-sent retry result=%+v calls=%d err=%v", result, notSent.calls, err)
	}
	if got := ClassifyParticipantRPCError(status.Error(codes.Unavailable, "UNAVAILABLE_NOT_SENT")); got != ParticipantRPCErrorNotSent {
		t.Fatalf("not-sent class=%s", got)
	}

	responseLost := &rpcSequenceClient{err: errors.New("connection closed after dispatch")}
	invoker, err = NewRPCInvoker(responseLost, rpcTestRoute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := invoker.Invoke(context.Background(), call); err == nil ||
		ClassifyParticipantRPCError(err) != ParticipantRPCErrorResponseLost || responseLost.calls != 1 {
		t.Fatalf("response-loss classification err=%v calls=%d", err, responseLost.calls)
	}
}

func TestHarvestRPCReconcileRejectedIsNormalizedToUnknownForPlayer(t *testing.T) {
	response := &contractspb.ParticipantResponse{
		ParticipantCommand:      contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD,
		ParticipantInvocationId: "grant-invocation",
		Outcome:                 contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED,
		Failure: &contractspb.ParticipantFailure{Error: &logicrpcpb.ErrorDetail{
			ErrorCode: commonpb.ErrorCode_ERR_INTERNAL, ErrorMessage: ErrorClassReconcileRequired,
		}},
	}
	result, err := decodeParticipantResponse(response, response.GetParticipantCommand(), response.GetParticipantInvocationId())
	if err != nil || result.Outcome != workflow.StepOutcomeRejected || result.ErrorClass != ErrorClassReconcileRequired {
		t.Fatalf("reconcile decode result=%+v err=%v", result, err)
	}
	result = normalizeParticipantStepResult(1, result)
	if result.Outcome != workflow.StepOutcomeTimeoutUnknown || result.Result != nil || result.ResultHash != ([32]byte{}) {
		t.Fatalf("reconcile was not kept recoverable: %+v", result)
	}
}

type rpcSequenceClient struct {
	errs     []error
	err      error
	response func(*contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error)
	calls    int
}

func (c *rpcSequenceClient) ExecuteStep(_ context.Context, request *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	c.calls++
	if len(c.errs) >= c.calls {
		return nil, c.errs[c.calls-1]
	}
	if c.err != nil {
		return nil, c.err
	}
	if c.response == nil {
		return nil, errors.New("rpc test response is not configured")
	}
	return c.response(request)
}

// TestStaleWorkProofErrorClassification locks the exact classification of a
// participant WorkVersion proof mismatch (WF-WORKVERSION-PROOF-RACE) through
// the shared workflow.ClassifyStaleWorkProof classifier:
//   - only gRPC FailedPrecondition + the exact "participant proof: WorkVersion"
//     prefix wraps as workflow.ErrStaleWorkProof (the participant verifier's
//     ErrWorkVersionMismatch text);
//   - the same text under a different gRPC code stays unwrapped;
//   - every other FailedPrecondition message (idempotency conflict, identity
//     mismatch, unregistered step, verifier-not-configured, unrelated text)
//     stays unwrapped so the generic error/INVOCATION_UNKNOWN path owns it and
//     stale proof is never misclassified from an unrelated precondition.
func TestStaleWorkProofErrorClassification(t *testing.T) {
	if workflow.ClassifyStaleWorkProof(nil) != nil {
		t.Fatal("nil must stay nil")
	}
	wrapped := workflow.ClassifyStaleWorkProof(status.Error(codes.FailedPrecondition, "participant proof: WorkVersion mismatch"))
	if !errors.Is(wrapped, workflow.ErrStaleWorkProof) {
		t.Fatalf("WorkVersion mismatch must wrap ErrStaleWorkProof, got %v", wrapped)
	}
	if got := workflow.ClassifyStaleWorkProof(status.Error(codes.Aborted, "participant proof: WorkVersion mismatch")); errors.Is(got, workflow.ErrStaleWorkProof) {
		t.Fatalf("non-FailedPrecondition must not classify as stale proof: %v", got)
	}
	for _, message := range []string{
		ErrorClassParticipantIdempotencyConflict,
		"participant proof: identity mismatch",
		`participant proof: step "COMMIT_HARVEST" is not registered`,
		"farm participant WorkVersion verifier is not configured",
		"player participant work version verifier is not configured",
		"unrelated precondition failure",
	} {
		if got := workflow.ClassifyStaleWorkProof(status.Error(codes.FailedPrecondition, message)); errors.Is(got, workflow.ErrStaleWorkProof) {
			t.Fatalf("FailedPrecondition %q must not classify as stale proof: %v", message, got)
		}
	}
}

func rpcTestStepCall(t *testing.T) workflow.StepCall {
	t.Helper()
	cycle, err := workidentity.StableOpaqueID("harvest-rpc-test-cycle", "one")
	if err != nil {
		t.Fatal(err)
	}
	root, err := BuildRoot(HarvestInput{
		SubjectPlayerID: 10001, OperationID: "harvest-rpc-test", TargetFarmID: 10001,
		PlotID: 3, CropCycleID: cycle, ExpectedPlotVersion: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	address, err := root.Address(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	invocation, err := workidentity.ParticipantInvocationID(string(identity.WorkTypeHarvest), root.WorkflowID, string(identity.StepCommitHarvest), 1)
	if err != nil {
		t.Fatal(err)
	}
	return workflow.StepCall{
		Work: address, WorkVersion: 1, StepIndex: 0, StepName: string(identity.StepCommitHarvest),
		InvocationID: invocation, Payload: root.PayloadBytes, PayloadHash: sha256Bytes(root.PayloadBytes),
		OperationID: "harvest-rpc-test", SubjectPlayerID: 10001,
	}
}

func rpcTestRoute(_ context.Context, _ workflow.StepCall, command contractspb.ParticipantCommandType, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
	domain := logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM
	if command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD {
		domain = logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER
	}
	return &logicrpcpb.ActorRouteHeader{RouteDomain: domain, ActorId: actorID, VirtualShard: 1, ShardRevision: 1, ExpectedHolderId: "holder", ExpectedHolderBootId: "boot", ExpectedWriterEpoch: 1}, nil
}

func rpcFarmAppliedResponse(request *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error) {
	payload := &contractspb.CommitHarvestPayload{}
	if err := proto.Unmarshal(request.GetTypedPayload(), payload); err != nil {
		return nil, err
	}
	decision := &contractspb.HarvestDecision{
		WorkflowId: request.GetCoordinator().GetWork().GetWorkId(), OwnerPlayerId: payload.GetOwnerPlayerId(),
		FarmId: payload.GetFarmId(), PlotId: payload.GetPlotId(), CropCycleId: payload.GetCropCycleId(),
		CropDefinitionId: 7, RewardItemId: 702, RewardQuantity: 50, RewardExp: 10, CropRuleVersion: 42,
		CompendiumUnlockSnapshot: &contractspb.CompendiumUnlockSnapshot{
			CropDefinitionId: 7, UnlockEligible: true, PlantedByPlayerId: payload.GetOwnerPlayerId(),
			CatalogVersionAtPlant: 9, DefinitionVersionAtPlant: 11,
		},
	}
	decision.HarvestDecisionHash = decisionHash(decision)
	message := &contractspb.CommitHarvestResult{HarvestDecision: decision, CommittedPlotVersion: 8, CommittedFarmVersion: 9, FarmRecordVersion: 10}
	typed, err := deterministicProto(message)
	if err != nil {
		return nil, err
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST)
	if err != nil {
		return nil, err
	}
	hash, err := contracts.CanonicalParticipantHash(spec, message)
	if err != nil {
		return nil, err
	}
	return &contractspb.ParticipantResponse{
		ParticipantCommand:      contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST,
		ParticipantInvocationId: request.GetCoordinator().GetParticipantInvocationId(),
		Outcome:                 contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
		TypedResult:             typed, ParticipantResultHash: hash[:], CommittedRecordVersion: 10,
	}, nil
}

func sha256Bytes(value []byte) [32]byte {
	return sha256.Sum256(value)
}
