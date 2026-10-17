package participant

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/observability/redisworkload"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/worker/sourceorder"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RedisHarvestSourceVerifier struct {
	orders    *authority.WorkLeaseStore
	bucketFor CareSourceBucketResolver
}

func NewRedisHarvestSourceVerifier(store *authority.Store, bucketFor CareSourceBucketResolver) (*RedisHarvestSourceVerifier, error) {
	if store == nil || bucketFor == nil {
		return nil, fmt.Errorf("harvest source verifier: authority store and FARM bucket resolver are required")
	}
	return &RedisHarvestSourceVerifier{orders: store.WorkLease(), bucketFor: bucketFor}, nil
}

func (v *RedisHarvestSourceVerifier) VerifyHarvestSource(ctx context.Context, call *contracts.ParticipantCall) error {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentSourceVerify)
	if v == nil || v.orders == nil || v.bucketFor == nil {
		return status.Error(codes.FailedPrecondition, "Harvest SourceOrder verifier is unavailable")
	}
	if call == nil || call.Work == nil {
		return status.Error(codes.InvalidArgument, "Harvest source call is incomplete")
	}
	payload, ok := call.Payload.(*contractspb.GrantHarvestRewardPayload)
	if !ok || payload == nil || payload.GetHarvestDecision() == nil {
		return status.Error(codes.FailedPrecondition, "Harvest source payload is incomplete")
	}
	if call.Work.GetWorkType() != contractspb.WorkType_WORK_TYPE_HARVEST ||
		call.Work.GetPlacementKind() != contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET ||
		call.Work.GetSourceAuthority() == nil ||
		call.Work.GetSourceAuthority().GetAuthorityKind() != contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_FARM_ACTOR ||
		call.Work.GetSourceAuthority().GetActorRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM {
		return status.Error(codes.FailedPrecondition, "Harvest WorkAddress source binding is invalid")
	}
	decoded, err := sourceorder.DecodeGrantHarvestPayload(call.Work.GetWorkId(), callPayloadBytes(call))
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	if decoded.GetPlayerId() != payload.GetPlayerId() {
		return status.Error(codes.FailedPrecondition, "Harvest payload identity mismatch")
	}
	canonical, err := sourceorder.HarvestPayloadHash(payload)
	if err != nil || canonical != call.CanonicalStepPayloadHash {
		return status.Error(codes.FailedPrecondition, "Harvest canonical payload hash is invalid")
	}
	farmID, err := strconv.ParseUint(call.Work.GetSourceAuthority().GetCanonicalAuthorityId(), 10, 64)
	if err != nil || farmID == 0 || farmID != payload.GetHarvestDecision().GetFarmId() {
		return status.Error(codes.FailedPrecondition, "Harvest source farm id is invalid")
	}
	bucket, err := v.bucketFor(farmID)
	if err != nil || bucket.Domain() != actorrouting.DomainFarm || bucket.VirtualShard() != call.Work.GetBucketId() ||
		call.Work.GetBucketVersion() != 1 || bucket.LayoutVersion() != call.Work.GetSourceAuthority().GetAuthorityLayoutVersion() {
		return status.Error(codes.FailedPrecondition, "Harvest source bucket proof is invalid")
	}
	workType := string(identity.WorkTypeHarvest)
	order, err := v.orders.GetSourceOrder(ctx, bucket, workType, call.Work.GetWorkId())
	if err != nil {
		return status.Error(codes.FailedPrecondition, "HARVEST SourceOrder is unavailable")
	}
	wantInvocation, invocationErr := sourceorder.HarvestInvocation(call.Work.GetWorkId())
	if invocationErr != nil || call.InvocationID != wantInvocation || call.ParticipantStep != string(identity.StepGrantHarvestReward) ||
		order.WorkID != call.Work.GetWorkId() || order.WorkType != workType || order.PlacementKind != "SOURCE_PERSISTENCE_BUCKET" ||
		order.BucketVersion != 1 || order.BucketID != bucket.VirtualShard() || order.SourceAuthorityKind != "FARM_ACTOR" ||
		order.CanonicalSourceAuthorityID != strconv.FormatUint(farmID, 10) || order.AuthorityLayoutVersion != bucket.LayoutVersion() ||
		order.ActorRouteDomain != "FARM" || order.SubjectPlayerID != payload.GetPlayerId() || order.OperationID == "" ||
		order.PayloadHash != hex.EncodeToString(canonical[:]) || !bytes.Equal(order.TypedPayload, callPayloadBytes(call)) {
		return status.Error(codes.FailedPrecondition, "HARVEST SourceOrder binding mismatch")
	}
	if err := sourceorder.VerifyRedisAdmissionFence(ctx, v.orders, bucket, workType, order, call); err != nil {
		return sourceAdmissionError("HARVEST", err)
	}
	call.TrustedSourceDueAtMs = order.NextDueAtMs
	return nil
}

func (v *RedisHarvestSourceVerifier) VerifyTheftSource(ctx context.Context, call *contracts.ParticipantCall) error {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentSourceVerify)
	if v == nil || v.orders == nil || v.bucketFor == nil {
		return status.Error(codes.FailedPrecondition, "Theft SourceOrder verifier is unavailable")
	}
	if call == nil || call.Work == nil || call.Payload == nil {
		return status.Error(codes.InvalidArgument, "Theft source call is incomplete")
	}
	if call.Work.GetWorkType() != contractspb.WorkType_WORK_TYPE_THEFT ||
		call.Work.GetPlacementKind() != contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET ||
		call.Work.GetSourceAuthority() == nil ||
		call.Work.GetSourceAuthority().GetAuthorityKind() != contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_FARM_ACTOR ||
		call.Work.GetSourceAuthority().GetActorRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM {
		return status.Error(codes.FailedPrecondition, "Theft WorkAddress source binding is invalid")
	}

	var (
		workType        string
		participantStep string
		subjectPlayerID uint64
		payloadFarmID   uint64
		payloadHash     [32]byte
		wantInvocation  string
		err             error
	)
	switch call.Spec.Command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR:
		workType = string(identity.WorkTypeTheftVisitor)
		participantStep = string(identity.StepSettleTheftVisitor)
		payload, ok := call.Payload.(*contractspb.SettleTheftVisitorPayload)
		if !ok || payload == nil {
			return status.Error(codes.FailedPrecondition, "Theft visitor payload is invalid")
		}
		if _, err = sourceorder.DecodeSettleTheftVisitorPayload(call.Work.GetWorkId(), callPayloadBytes(call)); err == nil {
			payloadHash, err = sourceorder.TheftVisitorPayloadHash(payload)
		}
		if payload.GetTheftTargetDecision() != nil {
			payloadFarmID = payload.GetTheftTargetDecision().GetFarmId()
		}
		subjectPlayerID = payload.GetVisitorPlayerId()
		wantInvocation, _ = sourceorder.TheftVisitorInvocation(call.Work.GetWorkId())
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME:
		workType = string(identity.WorkTypeTheftGuard)
		participantStep = string(identity.StepCreditGuardIncome)
		payload, ok := call.Payload.(*contractspb.CreditGuardIncomePayload)
		if !ok || payload == nil {
			return status.Error(codes.FailedPrecondition, "Theft guard payload is invalid")
		}
		if _, err = sourceorder.DecodeCreditGuardIncomePayload(call.Work.GetWorkId(), callPayloadBytes(call)); err == nil {
			payloadHash, err = sourceorder.TheftGuardPayloadHash(payload)
		}
		if payload.GetTheftTargetDecision() != nil {
			payloadFarmID = payload.GetTheftTargetDecision().GetFarmId()
		}
		subjectPlayerID = payload.GetOwnerPlayerId()
		wantInvocation, _ = sourceorder.TheftGuardInvocation(call.Work.GetWorkId())
	default:
		return status.Error(codes.FailedPrecondition, "Theft source command is invalid")
	}
	if err != nil || payloadHash != call.CanonicalStepPayloadHash || wantInvocation == "" || call.InvocationID != wantInvocation || call.ParticipantStep != participantStep {
		return status.Error(codes.FailedPrecondition, "Theft payload/invocation binding is invalid")
	}

	farmID, err := strconv.ParseUint(call.Work.GetSourceAuthority().GetCanonicalAuthorityId(), 10, 64)
	if err != nil || farmID == 0 || farmID != payloadFarmID {
		return status.Error(codes.FailedPrecondition, "Theft source farm id is invalid")
	}
	bucket, err := v.bucketFor(farmID)
	if err != nil || bucket.Domain() != actorrouting.DomainFarm || bucket.VirtualShard() != call.Work.GetBucketId() ||
		call.Work.GetBucketVersion() != 1 || bucket.LayoutVersion() != call.Work.GetSourceAuthority().GetAuthorityLayoutVersion() {
		return status.Error(codes.FailedPrecondition, "Theft source bucket proof is invalid")
	}
	order, err := v.orders.GetSourceOrder(ctx, bucket, workType, call.Work.GetWorkId())
	if err != nil {
		return status.Error(codes.FailedPrecondition, "THEFT SourceOrder is unavailable")
	}
	if order.WorkID != call.Work.GetWorkId() || order.WorkType != workType || order.PlacementKind != "SOURCE_PERSISTENCE_BUCKET" ||
		order.BucketVersion != 1 || order.BucketID != bucket.VirtualShard() || order.SourceAuthorityKind != "FARM_ACTOR" ||
		order.CanonicalSourceAuthorityID != strconv.FormatUint(farmID, 10) || order.AuthorityLayoutVersion != bucket.LayoutVersion() ||
		order.ActorRouteDomain != "FARM" || order.SubjectPlayerID != subjectPlayerID || order.OperationID == "" ||
		order.PayloadHash != hex.EncodeToString(payloadHash[:]) || !bytes.Equal(order.TypedPayload, callPayloadBytes(call)) {
		return status.Error(codes.FailedPrecondition, "THEFT SourceOrder binding mismatch")
	}
	if err := sourceorder.VerifyRedisAdmissionFence(ctx, v.orders, bucket, workType, order, call); err != nil {
		return sourceAdmissionError("THEFT", err)
	}
	resolvedAt := order.UpdatedAtMs
	if resolvedAt <= 0 {
		resolvedAt = order.CreatedAtMs
	}
	if resolvedAt <= 0 {
		return status.Error(codes.FailedPrecondition, "THEFT SourceOrder resolved time is missing")
	}
	call.TrustedResolvedAtMs = resolvedAt
	return nil
}

func sourceAdmissionError(family string, err error) error {
	if errors.Is(err, sourceorder.ErrSourceOrderAdmissionUnavailable) {
		return status.Error(codes.Unavailable, family+" SourceOrder admission evidence is unavailable")
	}
	return status.Error(codes.FailedPrecondition, family+" SourceOrder admission fence is invalid")
}

func sourcePlacement(call *contracts.ParticipantCall) bool {
	return call != nil && call.Work != nil &&
		call.Work.GetPlacementKind() == contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET
}

func skipsGenericWorkRecordFence(call *contracts.ParticipantCall) bool {
	if isCarePlayerCommand(call) {
		return true
	}
	if isHarvestPlayerCommand(call) && sourcePlacement(call) {
		return true
	}
	if isTheftPlayerCommand(call) && sourcePlacement(call) {
		return true
	}
	if plantSourcePlacement(call) || unlockSourcePlacement(call) {
		return true
	}
	return false
}
