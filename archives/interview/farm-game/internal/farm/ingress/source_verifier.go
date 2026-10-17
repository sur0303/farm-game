package ingress

import (
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
	"farmgame/internal/worker/sourceorder"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type PlayerSourceBucketResolver func(uint64) (rediskey.AuthorityBucketRef, error)

type PlayerSourceOrderVerifier interface {
	VerifyPlayerSource(context.Context, *contracts.ParticipantCall) error
}

type RedisPlayerSourceVerifier struct {
	orders    *authority.WorkLeaseStore
	bucketFor PlayerSourceBucketResolver
}

func NewRedisPlayerSourceVerifier(store *authority.Store, bucketFor PlayerSourceBucketResolver) (*RedisPlayerSourceVerifier, error) {
	if store == nil || bucketFor == nil {
		return nil, fmt.Errorf("farm player-source verifier: authority store and PLAYER bucket resolver are required")
	}
	return &RedisPlayerSourceVerifier{orders: store.WorkLease(), bucketFor: bucketFor}, nil
}

func farmSourcePlacement(call *contracts.ParticipantCall) bool {
	return call != nil && call.Work != nil &&
		call.Work.GetPlacementKind() == contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET
}

func skipsFarmWorkRecordFence(call *contracts.ParticipantCall) bool {
	if !farmSourcePlacement(call) {
		return false
	}
	switch call.Spec.Command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK:
		return true
	default:
		return false
	}
}

func skipsDecideTheftWorkRecordFence(call *contracts.ParticipantCall) bool {
	return farmSourcePlacement(call) &&
		call.Spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT
}

func (v *RedisPlayerSourceVerifier) VerifyPlayerSource(ctx context.Context, call *contracts.ParticipantCall) error {
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentSourceVerify)
	if v == nil || v.orders == nil || v.bucketFor == nil {
		return status.Error(codes.FailedPrecondition, "Farm SourceOrder verifier is unavailable")
	}
	if call == nil || call.Work == nil || call.Payload == nil {
		return status.Error(codes.InvalidArgument, "Farm source call is incomplete")
	}
	if call.Work.GetPlacementKind() != contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET ||
		call.Work.GetSourceAuthority() == nil ||
		call.Work.GetSourceAuthority().GetAuthorityKind() != contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR ||
		call.Work.GetSourceAuthority().GetActorRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER {
		return status.Error(codes.FailedPrecondition, "Farm dest WorkAddress source binding is invalid")
	}

	var (
		workType        string
		wireWorkType    contractspb.WorkType
		participantStep string
		sourcePlayerID  uint64
		payloadHash     [32]byte
		wantInvocation  string
		err             error
	)
	payloadBytes := farmCallPayloadBytes(call)
	switch call.Spec.Command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT:
		workType = string(identity.WorkTypePlant)
		wireWorkType = contractspb.WorkType_WORK_TYPE_PLANT
		participantStep = string(identity.StepCommitPlant)
		payload, ok := call.Payload.(*contractspb.CommitPlantPayload)
		if !ok || payload == nil {
			return status.Error(codes.FailedPrecondition, "Plant source payload is invalid")
		}
		if _, err = sourceorder.DecodeCommitPlantPayload(call.Work.GetWorkId(), payloadBytes); err == nil {
			payloadHash, err = sourceorder.PlantPayloadHash(payload)
		}
		sourcePlayerID = payload.GetOwnerPlayerId()
		wantInvocation, _ = sourceorder.PlantInvocation(call.Work.GetWorkId())
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK:
		workType = string(identity.WorkTypeUnlockPlot)
		wireWorkType = contractspb.WorkType_WORK_TYPE_UNLOCK_PLOT
		participantStep = string(identity.StepCommitPlotUnlock)
		payload, ok := call.Payload.(*contractspb.CommitPlotUnlockPayload)
		if !ok || payload == nil {
			return status.Error(codes.FailedPrecondition, "Unlock source payload is invalid")
		}
		if _, err = sourceorder.DecodeCommitPlotUnlockPayload(call.Work.GetWorkId(), payloadBytes); err == nil {
			payloadHash, err = sourceorder.UnlockPayloadHash(payload)
		}
		sourcePlayerID = payload.GetOwnerPlayerId()
		wantInvocation, _ = sourceorder.UnlockInvocation(call.Work.GetWorkId())
	default:
		return status.Error(codes.FailedPrecondition, "Farm source command is invalid")
	}
	if err != nil || payloadHash != call.CanonicalStepPayloadHash || wantInvocation == "" ||
		call.InvocationID != wantInvocation || call.ParticipantStep != participantStep || call.Work.GetWorkType() != wireWorkType {
		return status.Error(codes.FailedPrecondition, "Farm source payload/invocation binding is invalid")
	}

	playerID, err := strconv.ParseUint(call.Work.GetSourceAuthority().GetCanonicalAuthorityId(), 10, 64)
	if err != nil || playerID == 0 || playerID != sourcePlayerID {
		return status.Error(codes.FailedPrecondition, "Farm dest source player id is invalid")
	}
	bucket, err := v.bucketFor(playerID)
	if err != nil || bucket.Domain() != actorrouting.DomainPlayer || bucket.VirtualShard() != call.Work.GetBucketId() ||
		call.Work.GetBucketVersion() != 1 || bucket.LayoutVersion() != call.Work.GetSourceAuthority().GetAuthorityLayoutVersion() {
		return status.Error(codes.FailedPrecondition, "Farm dest source bucket proof is invalid")
	}
	order, err := v.orders.VerifySourceOrderAdmission(ctx, bucket, workType, call.Work.GetWorkId())
	if err != nil {
		return status.Error(codes.Unavailable, "Player SourceOrder admission evidence is unavailable")
	}
	if !order.Found {
		return status.Error(codes.FailedPrecondition, "Player SourceOrder is unavailable")
	}
	if order.WorkID != call.Work.GetWorkId() || order.WorkType != workType || order.PlacementKind != "SOURCE_PERSISTENCE_BUCKET" ||
		order.BucketVersion != 1 || order.BucketID != bucket.VirtualShard() || order.SourceAuthorityKind != "PLAYER_ACTOR" ||
		order.CanonicalSourceAuthority != strconv.FormatUint(playerID, 10) || order.AuthorityLayoutVersion != bucket.LayoutVersion() ||
		order.ActorRouteDomain != "PLAYER" || order.SubjectPlayerID != sourcePlayerID || order.OperationID == "" ||
		order.PayloadHash != hex.EncodeToString(payloadHash[:]) {
		return status.Error(codes.FailedPrecondition, "Player SourceOrder binding mismatch")
	}
	if err := sourceorder.VerifyAdmissionEvidence(&order, call); err != nil {
		return status.Error(codes.FailedPrecondition, "Player SourceOrder admission fence is invalid")
	}
	return nil
}

func farmCallPayloadBytes(call *contracts.ParticipantCall) []byte {
	if call == nil || call.Payload == nil {
		return nil
	}
	encoded, _ := (proto.MarshalOptions{Deterministic: true}).Marshal(call.Payload)
	return encoded
}
