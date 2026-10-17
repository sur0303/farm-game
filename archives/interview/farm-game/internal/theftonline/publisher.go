package theftonline

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strconv"
	"strings"

	commonpb "farmgame/api/gen/common"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/identity"
	"farmgame/internal/scenario/theft"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"
)

// RedisResultPublisher writes Theft terminals into the existing generic
// ResultDirectory. It deliberately does not create a Theft-specific result
// keyspace. WorkRecord remains the terminal authority and Pair fallback owns
// the pre-WorkRecord PROCESSING gap.
type RedisResultPublisher struct {
	directory *redisauthority.ResultDirectoryStore
}

func NewRedisResultPublisher(client redis.UniversalClient, namespace rediskey.Namespace) (*RedisResultPublisher, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: Redis client is required", theft.ErrInvalidInput)
	}
	directory, err := redisauthority.NewResultDirectoryStore(client, nil, namespace, 0)
	if err != nil {
		return nil, err
	}
	return &RedisResultPublisher{directory: directory}, nil
}

func (p *RedisResultPublisher) Publish(ctx context.Context, publication theft.ResultPublication) error {
	if p == nil || p.directory == nil || publication.Outcome == nil ||
		publication.OperationID == "" || publication.SubjectPlayerID == 0 || publication.PairID == "" {
		return errors.New("theft online: result publisher is unavailable")
	}
	if workidentity.ValidateStableOpaqueID(publication.PairID) != nil || len(publication.PayloadHash) != sha256.Size ||
		publication.Outcome.GetOriginalCommandType() != commonpb.CommandType_THEFT ||
		publication.Outcome.GetOriginalOperationId() != publication.OperationID ||
		publication.Outcome.GetFarmContext().GetTargetFarmId() == 0 {
		return errors.New("theft online: invalid result binding")
	}
	switch publication.Outcome.GetStatus() {
	case commonpb.CommandStatus_SUCCEEDED, commonpb.CommandStatus_NO_CHANGE, commonpb.CommandStatus_REJECTED:
	default:
		return errors.New("theft online: result is not a legal terminal")
	}
	wantPairID, err := identity.CanonicalFriendRelationID(publication.SubjectPlayerID,
		publication.Outcome.GetFarmContext().GetTargetFarmId())
	if err != nil || wantPairID != publication.PairID {
		return errors.New("theft online: result Pair binding mismatch")
	}
	key := identity.EffectiveIdempotencyKey{SubjectPlayerID: publication.SubjectPlayerID,
		StableCommandTypeName: string(identity.WorkTypeTheft), OperationID: publication.OperationID,
		Root: identity.OperationRootRef{Kind: identity.RootKindFriendPair, CanonicalID: publication.PairID}}
	wantDigest, err := key.OperationDigest()
	if err != nil || wantDigest != publication.OperationDigest {
		return errors.New("theft online: result operation digest mismatch")
	}
	sourceRef := strings.TrimSpace(publication.SourceRef)
	if sourceRef == "" {
		wantWorkflowID, workflowErr := identity.WorkflowID(key)
		if workflowErr != nil || publication.WorkflowID != wantWorkflowID ||
			workidentity.ValidateStableOpaqueID(publication.WorkflowID) != nil ||
			publication.WorkAddress.WorkID != publication.WorkflowID || publication.WorkAddress.ValidateShape() != nil {
			return errors.New("theft online: invalid published workflow id")
		}
		sourceRef = resultWorkAddress(publication.WorkAddress)
	} else if sourceRef != "pair-result-v1:"+publication.OperationDigest || publication.WorkflowID != "" {
		return errors.New("theft online: invalid result source ref")
	}
	outcomeBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(publication.Outcome)
	if err != nil {
		return err
	}
	data, err := redisauthority.EncodeResultBinding(redisauthority.ResultBinding{
		SubjectPlayerID: publication.SubjectPlayerID, CommandType: "THEFT", OperationID: publication.OperationID,
		PayloadHash: publication.PayloadHash, Outcome: outcomeBytes,
	})
	if err != nil {
		return err
	}
	entry := redisauthority.ResultEntry{
		CommandType:      "THEFT",
		OperationDigest:  publication.OperationDigest,
		RootKind:         "FRIEND_PAIR",
		RootID:           publication.PairID,
		SourceRef:        sourceRef,
		FinalStatusClass: publication.Outcome.GetStatus().String(),
		MinimalOutcome:   data,
	}
	status, err := p.directory.PublishResult(ctx, publication.SubjectPlayerID, entry)
	if err != nil {
		return fmt.Errorf("theft online: ResultDirectory publish: %v", err)
	}
	switch status {
	case redisauthority.ResultPublishPublished, redisauthority.ResultPublishAlreadyPublished:
		return nil
	case redisauthority.ResultPublishConflict:
		return errors.New("theft online: terminal ResultDirectory conflict")
	default:
		return fmt.Errorf("theft online: ResultDirectory publish status %s", status)
	}
}

func resultWorkAddress(address workidentity.WorkAddress) string {
	return strings.Join([]string{"work-address-v1", address.WorkType, address.WorkID,
		strconv.FormatUint(uint64(address.PlacementKind), 10), strconv.FormatUint(uint64(address.BucketVersion), 10),
		strconv.FormatUint(uint64(address.BucketID), 10), strconv.FormatUint(uint64(address.SourceRouteDomain), 10),
		strconv.FormatUint(address.SourceActorID, 10)}, ":")
}

// ResultPublicationHash is a compact stable proof for worker completion and
// metrics. It intentionally hashes the immutable published protobuf, not a
// mutable Redis timestamp or WorkVersion.
func ResultPublicationHash(result *publicpb.TheftResult) ([]byte, error) {
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(result)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(data)
	return append([]byte(nil), hash[:]...), nil
}
