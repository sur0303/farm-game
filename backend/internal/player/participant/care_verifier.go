package participant

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strconv"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/observability/redisworkload"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// CareSourceBucketResolver is the same physical FARM bucket calculation used
// by Farm Authority commit and the CR3 worker. It is injected by the process
// composition root; the PlayerActor never derives a bucket from a caller's
// WorkAddress alone.
type CareSourceBucketResolver func(uint64) (rediskey.AuthorityBucketRef, error)

// RedisCareDependencyVerifier is the production admission proof for Player
// APPLY_CARE_REWARD. All reads happen before PlayerActor admission and only
// read the auth-v1 SourceOrder, DueIndex and WorkLease fields.
type RedisCareDependencyVerifier struct {
	orders    *authority.WorkLeaseStore
	bucketFor CareSourceBucketResolver
}

func NewRedisCareDependencyVerifier(store *authority.Store, bucketFor CareSourceBucketResolver) (*RedisCareDependencyVerifier, error) {
	if store == nil || bucketFor == nil {
		return nil, fmt.Errorf("care dependency verifier: authority store and FARM bucket resolver are required")
	}
	return &RedisCareDependencyVerifier{orders: store.WorkLease(), bucketFor: bucketFor}, nil
}

func (v *RedisCareDependencyVerifier) VerifyCareDependency(ctx context.Context, call *contracts.ParticipantCall) (CareDependencyProof, error) {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentSourceVerify)
	if v == nil || v.orders == nil || v.bucketFor == nil {
		return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "Care Authority verifier is unavailable")
	}
	if call == nil || call.Work == nil || call.Payload == nil {
		return CareDependencyProof{}, status.Error(codes.InvalidArgument, "Care dependency call is incomplete")
	}
	payload, ok := call.Payload.(*contractspb.ApplyCareRewardPayload)
	if !ok || payload == nil || payload.GetCareResolutionSnapshot() == nil {
		return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "Care payload proof is incomplete")
	}
	snapshot := payload.GetCareResolutionSnapshot()
	if call.Work.GetWorkType() != contractspb.WorkType_WORK_TYPE_CARE_REWARD ||
		call.Work.GetPlacementKind() != contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET ||
		call.Work.GetSourceAuthority() == nil ||
		call.Work.GetSourceAuthority().GetAuthorityKind() != contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_FARM_ACTOR ||
		call.Work.GetSourceAuthority().GetActorRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM {
		return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "Care WorkAddress source binding is invalid")
	}
	if call.Work.GetWorkId() != payload.GetCareRewardOrderId() ||
		call.Work.GetSourceAuthority().GetCanonicalAuthorityId() != strconv.FormatUint(snapshot.GetSourceFarmId(), 10) ||
		call.Work.GetBucketVersion() != 1 {
		return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "Care WorkAddress/order binding is invalid")
	}
	wantInvocation, err := identity.ParticipantInvocationID(identity.WorkTypeCareReward, call.Work.GetWorkId(), identity.StepApplyCareReward, 1)
	if err != nil || call.InvocationID != wantInvocation || call.ParticipantStep != string(identity.StepApplyCareReward) {
		return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "Care invocation/step binding is invalid")
	}
	canonicalPayload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(payload)
	if err != nil || len(canonicalPayload) == 0 {
		return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "Care payload canonicalization failed")
	}
	if !bytes.Equal(canonicalPayload, callPayloadBytes(call)) {
		return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "Care payload bytes are not canonical")
	}
	canonicalHash, err := contracts.CanonicalParticipantHash(call.Spec, payload)
	if err != nil || canonicalHash != call.CanonicalStepPayloadHash {
		return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "Care canonical payload hash is invalid")
	}

	farmID, err := strconv.ParseUint(call.Work.GetSourceAuthority().GetCanonicalAuthorityId(), 10, 64)
	if err != nil || farmID == 0 {
		return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "Care source farm id is invalid")
	}
	bucket, err := v.bucketFor(farmID)
	if err != nil || bucket.Domain() != actorrouting.DomainFarm || bucket.VirtualShard() != call.Work.GetBucketId() || bucket.LayoutVersion() != call.Work.GetSourceAuthority().GetAuthorityLayoutVersion() {
		return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "Care source bucket proof is invalid")
	}
	order, err := v.orders.GetSourceOrder(ctx, bucket, string(identity.WorkTypeCareReward), call.Work.GetWorkId())
	if err != nil {
		return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "CareRewardOrder is unavailable")
	}
	if err := validateRedisCareOrder(order, call, payload, canonicalHash); err != nil {
		return CareDependencyProof{}, status.Error(codes.FailedPrecondition, err.Error())
	}
	nowMs, err := v.orders.TrustedNowMs(ctx)
	if err != nil {
		return CareDependencyProof{}, status.Error(codes.Unavailable, "Care Authority time is unavailable")
	}
	dueScore, dueBound, err := v.orders.DueIndexScore(ctx, bucket, string(identity.WorkTypeCareReward), order.WorkID)
	if err != nil || !dueBound {
		return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "Care SourceOrder is not bound to DueIndex")
	}
	if dueScore != order.NextDueAtMs {
		return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "Care SourceOrder next_due and DueIndex disagree")
	}
	call.TrustedSourceDueAtMs = order.NextDueAtMs
	proof := CareDependencyProof{
		SourceOrderFound: true, SourceOrderID: order.WorkID,
		SourceOrderType: contractspb.WorkType_WORK_TYPE_CARE_REWARD,
		SourceAuthority: proto.Clone(call.Work.GetSourceAuthority()).(*contractspb.SourceAuthorityRef),
		Snapshot:        proto.Clone(snapshot).(*contractspb.CareResolutionSnapshot),
		SnapshotHash:    append([]byte(nil), snapshot.GetCareResolutionHash()...),
		WorkVersion:     order.WorkVersion, InvocationID: call.InvocationID,
		ParticipantStep: call.ParticipantStep, DueIndexBound: true, FenceValid: true,
	}
	switch call.CallerKind {
	case contracts.ParticipantCallerCoordinator:
		if order.State != "PENDING" || dueScore > nowMs || call.OperationID != order.OperationID {
			return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "Care Direct admission state/operation is invalid")
		}
		if order.LeaseHolderID != "" || order.LeaseHolderBootID != "" || order.LeaseEpoch != 0 || order.LeaseTokenHash != "" || order.LeaseExpireAtMs != 0 {
			return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "Care Direct admission carries a Worker lease")
		}
		proof.OrderState = CareRewardOrderStatePending
		proof.ExecutionMode = CareDependencyExecutionModeCoordinator
	case contracts.ParticipantCallerWorkerRecovery:
		if order.State != "REWARD_DISPATCHING" || order.LeaseHolderID != call.CallerInstanceID || order.LeaseHolderBootID != call.CallerBootID || order.LeaseEpoch == 0 || order.LeaseEpoch != call.LeaseEpoch || order.LeaseExpireAtMs <= nowMs || dueScore != order.LeaseExpireAtMs || order.LeaseTokenHash != authority.HashLeaseToken(call.LeaseOwnerToken) {
			return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "Care Worker lease proof is invalid or expired")
		}
		proof.OrderState = CareRewardOrderStateRewardDispatching
		proof.ExecutionMode = CareDependencyExecutionModeRecoveryLease
		proof.LeaseValid = true
		proof.LeaseOwnerToken = call.LeaseOwnerToken
		proof.LeaseEpoch = order.LeaseEpoch
		proof.LeaseExpireAtMs = order.LeaseExpireAtMs
	default:
		return CareDependencyProof{}, status.Error(codes.InvalidArgument, "Care caller kind is invalid")
	}
	return proof, nil
}

func callPayloadBytes(call *contracts.ParticipantCall) []byte {
	if call == nil || call.Payload == nil {
		return nil
	}
	encoded, _ := (proto.MarshalOptions{Deterministic: true}).Marshal(call.Payload)
	return encoded
}

func validateRedisCareOrder(order authority.SourceOrderSnapshot, call *contracts.ParticipantCall, payload *contractspb.ApplyCareRewardPayload, canonicalHash [32]byte) error {
	snapshot := payload.GetCareResolutionSnapshot()
	if order.WorkID != call.Work.GetWorkId() || order.WorkType != string(identity.WorkTypeCareReward) || order.PlacementKind != "SOURCE_PERSISTENCE_BUCKET" || order.BucketVersion != 1 || order.BucketID != call.Work.GetBucketId() || order.SourceAuthorityKind != "FARM_ACTOR" || order.CanonicalSourceAuthorityID != call.Work.GetSourceAuthority().GetCanonicalAuthorityId() || order.AuthorityLayoutVersion != call.Work.GetSourceAuthority().GetAuthorityLayoutVersion() || order.ActorRouteDomain != "FARM" || order.SubjectPlayerID != payload.GetCarerPlayerId() || order.OperationID == "" || order.WorkVersion == 0 || order.WorkVersion != call.ExpectedWorkVersion || !bytes.Equal(order.TypedPayload, callPayloadBytes(call)) {
		return fmt.Errorf("Care SourceOrder identity is invalid")
	}
	if order.PayloadHash != hex.EncodeToString(canonicalHash[:]) {
		return fmt.Errorf("Care SourceOrder payload_hash does not match canonical payload")
	}
	resolutionHash, err := CareResolutionHash(snapshot)
	if err != nil || !bytes.Equal(resolutionHash[:], snapshot.GetCareResolutionHash()) {
		return fmt.Errorf("Care frozen resolution hash is invalid")
	}
	return nil
}
