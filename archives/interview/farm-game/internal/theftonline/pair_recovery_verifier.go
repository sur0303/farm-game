package theftonline

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	farmingress "farmgame/internal/farm/ingress"
	"farmgame/internal/friend"
	"farmgame/internal/observability/redisworkload"
	"farmgame/internal/scenario/theft"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/theftrecovery"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type pairRecoveryLeaseReader interface {
	VerifyRecoveryLease(context.Context, theftrecovery.VerifyRecoveryLeaseRequest) (theftrecovery.RecoveryLeaseVerification, error)
}

// PairDecideRecoveryVerifier is the source-placed DECIDE_THEFT proof. It
// reloads the admitted Pair dispatch anchor and atomically verifies the exact
// Pair Work lease; it deliberately never asks the generic WorkRepository for a
// destination WorkRecord.
type PairDecideRecoveryVerifier struct {
	pairs     *friend.RedisFriendshipRepositories
	leases    pairRecoveryLeaseReader
	bucketFor func(uint64) (rediskey.AuthorityBucketRef, error)
}

func NewPairDecideRecoveryVerifier(pairs *friend.RedisFriendshipRepositories, leases pairRecoveryLeaseReader,
	bucketFor func(uint64) (rediskey.AuthorityBucketRef, error)) (*PairDecideRecoveryVerifier, error) {
	if pairs == nil || leases == nil || bucketFor == nil {
		return nil, fmt.Errorf("%w: Pair repository, lease reader and Farm bucket resolver are required", ErrInvalidComposition)
	}
	return &PairDecideRecoveryVerifier{pairs: pairs, leases: leases, bucketFor: bucketFor}, nil
}

var _ farmingress.DecideTheftRecoveryVerifier = (*PairDecideRecoveryVerifier)(nil)

func (v *PairDecideRecoveryVerifier) VerifyDecideTheftRecovery(ctx context.Context, call *contracts.ParticipantCall) error {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentSourceVerify)
	if v == nil || v.pairs == nil || v.leases == nil || v.bucketFor == nil || call == nil || call.Work == nil || call.RouteHeader == nil {
		return status.Error(codes.FailedPrecondition, "Pair DECIDE_THEFT recovery proof is unavailable")
	}
	if call.CallerKind != contracts.ParticipantCallerWorkerRecovery ||
		call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT ||
		call.ParticipantStep != "DECIDE_THEFT" || call.ExpectedWorkVersion == 0 || call.LeaseEpoch == 0 ||
		call.CallerInstanceID == "" || call.CallerBootID == "" || call.LeaseOwnerToken == "" {
		return status.Error(codes.FailedPrecondition, "Pair DECIDE_THEFT recovery caller fence is invalid")
	}
	payload, ok := call.Payload.(*contractspb.DecideTheftPayload)
	if !ok || payload == nil || payload.GetActionPermitSnapshot() == nil {
		return status.Error(codes.FailedPrecondition, "Pair DECIDE_THEFT recovery payload is invalid")
	}
	permit := payload.GetActionPermitSnapshot()
	order, err := theftrecovery.LoadPairTheftDispatchByOperation(ctx, v.pairs,
		payload.GetVisitorPlayerId(), payload.GetOwnerPlayerId(), permit.GetOperationId())
	if err != nil {
		return pairRecoveryProofError(err)
	}
	storedPermit, err := actionPermitProto(order.Permit)
	if err != nil || !proto.Equal(storedPermit, permit) || payload.GetFarmId() != order.TargetFarmID ||
		payload.GetVisitorPlayerId() != order.VisitorPlayerID || payload.GetOwnerPlayerId() != order.OwnerPlayerID ||
		payload.GetPlotId() == 0 || payload.GetCropCycleId() == "" ||
		!bytes.Equal(order.TargetBindingHash, farmingress.TheftTargetBindingHash(payload.GetVisitorPlayerId(), payload.GetOwnerPlayerId(), payload.GetPlotId(), payload.GetCropCycleId())) {
		return status.Error(codes.FailedPrecondition, "Pair DECIDE_THEFT recovery source binding is invalid")
	}

	input := theft.Input{
		VisitorPlayerID: order.VisitorPlayerID, OwnerPlayerID: order.OwnerPlayerID,
		TargetFarmID: order.TargetFarmID, PlotID: payload.GetPlotId(), CropCycleID: payload.GetCropCycleId(),
		OperationID: order.OperationID, ActionPermitSnapshot: storedPermit,
		AdmissionHash: append([]byte(nil), order.TargetBindingHash...), WorkflowDeadlineAtMs: order.DeadlineAtMs,
	}
	expected, err := farmingress.BuildTheftSourceCall(input, v.bucketFor)
	if err != nil || expected == nil || !proto.Equal(expected.Work, call.Work) || !proto.Equal(expected.Payload, call.Payload) ||
		expected.InvocationID != call.InvocationID || expected.ParticipantStep != call.ParticipantStep ||
		expected.CanonicalStepPayloadHash != call.CanonicalStepPayloadHash || call.RouteHeader.GetActorId() != order.TargetFarmID {
		return status.Error(codes.FailedPrecondition, "Pair DECIDE_THEFT recovery Participant binding is invalid")
	}

	verification, err := v.leases.VerifyRecoveryLease(ctx, theftrecovery.VerifyRecoveryLeaseRequest{
		Order: order,
		Fence: theftrecovery.LeaseFence{HolderID: call.CallerInstanceID, BootID: call.CallerBootID,
			LeaseToken: call.LeaseOwnerToken, LeaseEpoch: call.LeaseEpoch, WorkVersion: call.ExpectedWorkVersion},
	})
	if err != nil {
		return pairRecoveryProofError(err)
	}
	if verification.WorkVersion != call.ExpectedWorkVersion || verification.LeaseEpoch != call.LeaseEpoch ||
		verification.LeaseExpireAtMs <= verification.TrustedNowMs || verification.DueIndexScoreMs != verification.LeaseExpireAtMs {
		return status.Error(codes.FailedPrecondition, "Pair DECIDE_THEFT recovery lease evidence is invalid")
	}
	return nil
}

func pairRecoveryProofError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, theftrecovery.ErrLeaseLost) || errors.Is(err, theftrecovery.ErrInvalidDispatch) || errors.Is(err, friend.ErrFriendReconcile) {
		return status.Error(codes.FailedPrecondition, "Pair DECIDE_THEFT recovery proof is invalid")
	}
	return status.Error(codes.Unavailable, "Pair DECIDE_THEFT recovery proof is unavailable")
}
