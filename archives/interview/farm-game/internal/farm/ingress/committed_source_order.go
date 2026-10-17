package ingress

import (
	"bytes"
	"fmt"
	"strconv"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/identity"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/sourceorder"
)

func attachedPlayerSourceOrder(envelope *contractspb.CreatedSourceOrderEnvelope, bucket rediskey.AuthorityBucketRef, playerID uint64, workID, workType string) (authoritystore.AttachedOrder, error) {
	if envelope == nil || envelope.GetWork() == nil || playerID == 0 || workID == "" || workType == "" {
		return authoritystore.AttachedOrder{}, fmt.Errorf("committed Player SourceOrder envelope is unavailable")
	}
	work := envelope.GetWork()
	source := work.GetSourceAuthority()
	wantProtoType := contractspb.WorkType_WORK_TYPE_UNSPECIFIED
	switch workType {
	case string(identity.WorkTypePlant):
		wantProtoType = contractspb.WorkType_WORK_TYPE_PLANT
	case string(identity.WorkTypeUnlockPlot):
		wantProtoType = contractspb.WorkType_WORK_TYPE_UNLOCK_PLOT
	default:
		return authoritystore.AttachedOrder{}, fmt.Errorf("unsupported committed Player SourceOrder type %q", workType)
	}
	if work.GetWorkId() != workID || work.GetWorkType() != wantProtoType ||
		work.GetPlacementKind() != contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET ||
		work.GetBucketVersion() != 1 || work.GetBucketId() != bucket.VirtualShard() ||
		source == nil || source.GetAuthorityKind() != contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR ||
		source.GetCanonicalAuthorityId() != strconv.FormatUint(playerID, 10) ||
		source.GetAuthorityLayoutVersion() != bucket.LayoutVersion() || source.ActorRouteDomain == nil ||
		source.GetActorRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER ||
		envelope.GetWorkVersion() != 1 || envelope.GetOperationId() == "" ||
		envelope.GetSubjectPlayerId() != playerID || envelope.GetDueAtMs() <= 0 ||
		len(envelope.GetTypedPayload()) == 0 || len(envelope.GetPayloadHash()) != 32 {
		return authoritystore.AttachedOrder{}, fmt.Errorf("committed Player SourceOrder envelope identity mismatch")
	}
	var semanticHash [32]byte
	var err error
	switch workType {
	case string(identity.WorkTypePlant):
		payload, decodeErr := sourceorder.DecodeCommitPlantPayload(workID, envelope.GetTypedPayload())
		if decodeErr != nil {
			return authoritystore.AttachedOrder{}, decodeErr
		}
		semanticHash, err = sourceorder.PlantPayloadHash(payload)
	case string(identity.WorkTypeUnlockPlot):
		payload, decodeErr := sourceorder.DecodeCommitPlotUnlockPayload(workID, envelope.GetTypedPayload())
		if decodeErr != nil {
			return authoritystore.AttachedOrder{}, decodeErr
		}
		semanticHash, err = sourceorder.UnlockPayloadHash(payload)
	}
	if err != nil || !bytes.Equal(semanticHash[:], envelope.GetPayloadHash()) {
		return authoritystore.AttachedOrder{}, fmt.Errorf("committed Player SourceOrder envelope payload hash mismatch")
	}
	return authoritystore.AttachedOrder{
		OrderID: workID, OrderType: workType,
		Payload: append([]byte(nil), envelope.GetTypedPayload()...), PayloadHash: authoritystore.PayloadHash(semanticHash),
		DueAtMs: envelope.GetDueAtMs(), OperationID: envelope.GetOperationId(), SubjectPlayerID: playerID,
	}, nil
}
