package friend

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"strings"
	"time"

	friendpb "farmgame/api/gen/friend"

	"google.golang.org/protobuf/proto"
)

const DefaultActionPermitTTL = 30 * time.Second

var ErrPermitInvalid = errors.New("friend: action permit invalid or expired")

type PairAuthorityReader interface {
	GetPair(context.Context, uint64, uint64) (PairSnapshot, error)
}

// PermitService is the FriendshipDomain authorization boundary used by
// FarmOperationCoordinator. The FarmActor side verifies both the signature and
// the current relation epoch, so a permit issued before revoke cannot survive a
// later re-friend.
type PermitService struct {
	pairs  PairAuthorityReader
	secret []byte
	ttl    time.Duration
	now    func() time.Time
}

func NewPermitService(pairs PairAuthorityReader, secret []byte, ttl time.Duration) (*PermitService, error) {
	if pairs == nil || len(secret) < 16 {
		return nil, errors.New("friend: Pair reader and at least 16-byte permit secret required")
	}
	if ttl <= 0 {
		ttl = DefaultActionPermitTTL
	}
	return &PermitService{
		pairs: pairs, secret: append([]byte(nil), secret...),
		ttl: ttl, now: time.Now,
	}, nil
}

func (s *PermitService) Issue(ctx context.Context, viewer, target uint64, operationID string, action friendpb.FriendAction) (*friendpb.ActionPermit, error) {
	if !ValidPair(viewer, target) || strings.TrimSpace(operationID) == "" ||
		action == friendpb.FriendAction_FRIEND_ACTION_UNSPECIFIED {
		return nil, ErrPermitInvalid
	}
	pair, err := s.pairs.GetPair(ctx, viewer, target)
	if err != nil {
		return nil, err
	}
	if !pair.CanIssuePermit(viewer, target) || pair.Relation.RelationEpoch == 0 {
		return nil, ErrPermitInvalid
	}
	now := s.now()
	permit := &friendpb.ActionPermit{
		ViewerPlayerId: viewer, TargetOwnerId: target,
		RelationEpoch: pair.Relation.RelationEpoch,
		OperationId:   strings.TrimSpace(operationID), Action: action,
		IssuedAtMs: now.UnixMilli(), ExpiresAtMs: now.Add(s.ttl).UnixMilli(),
	}
	signature, err := s.sign(permit)
	if err != nil {
		return nil, err
	}
	permit.Signature = signature
	return permit, nil
}

// IssueBestEffortSnapshot performs only the canonical Pair read and returns a
// short-lived in-memory ActionPermitSnapshot. It deliberately does not create
// FriendActionDispatchOrder, DueIndex, or any recovery record. Production
// durable paths must keep using CommitFriendActionAdmission; this seam exists
// solely for the explicit dirty_best_effort actor experiment.
func (s *PermitService) IssueBestEffortSnapshot(
	ctx context.Context,
	viewer, target uint64,
	operationID string,
	scope ActionScope,
	payloadHash, targetBindingHash []byte,
) (ActionPermitSnapshot, error) {
	if s == nil || !ValidPair(viewer, target) || strings.TrimSpace(operationID) == "" ||
		(scope != ActionScopeCare && scope != ActionScopeTheft) || len(payloadHash) == 0 || len(targetBindingHash) == 0 {
		return ActionPermitSnapshot{}, ErrPermitInvalid
	}
	pair, err := s.pairs.GetPair(ctx, viewer, target)
	if err != nil {
		return ActionPermitSnapshot{}, err
	}
	if !pair.CanIssuePermit(viewer, target) || pair.Relation.RelationEpoch == 0 || pair.PairID == "" {
		return ActionPermitSnapshot{}, ErrPermitInvalid
	}
	now := s.now()
	permitID, err := stableID("farm-best-effort-action-permit-v1", pair.PairID, itoa(viewer), operationID)
	if err != nil {
		return ActionPermitSnapshot{}, err
	}
	return ActionPermitSnapshot{
		PermitID: permitID, RelationID: pair.PairID, RelationEpoch: pair.Relation.RelationEpoch,
		VisitorPlayerID: viewer, OwnerPlayerID: target, TargetFarmID: target,
		OperationID: strings.TrimSpace(operationID), Scope: scope,
		PayloadHash: append([]byte(nil), payloadHash...), TargetBindingHash: append([]byte(nil), targetBindingHash...),
		IssuedAtAuthorityMs: now.UnixMilli(), ExpireAtAuthorityMs: now.Add(s.ttl).UnixMilli(),
		ExecutionAcceptedAtAuthorityMs: now.UnixMilli(),
	}, nil
}

func (s *PermitService) Verify(ctx context.Context, permit *friendpb.ActionPermit, viewer, target uint64, operationID string, action friendpb.FriendAction) error {
	if err := s.VerifySignature(permit, viewer, target, operationID, action); err != nil {
		return err
	}
	pair, err := s.pairs.GetPair(ctx, viewer, target)
	if err != nil {
		return err
	}
	if !pair.CanIssuePermit(viewer, target) || pair.Relation.RelationEpoch != permit.RelationEpoch {
		return ErrPermitInvalid
	}
	return nil
}

// VerifySignature performs the FarmActor-side check without remote storage I/O.
// Issue already read the canonical relation; the short TTL bounds the lifetime
// of a capability if the relation is revoked immediately afterwards.
func (s *PermitService) VerifySignature(permit *friendpb.ActionPermit, viewer, target uint64, operationID string, action friendpb.FriendAction) error {
	if permit == nil || permit.ViewerPlayerId != viewer || permit.TargetOwnerId != target ||
		permit.OperationId != strings.TrimSpace(operationID) || permit.Action != action ||
		permit.RelationEpoch == 0 || len(permit.Signature) == 0 {
		return ErrPermitInvalid
	}
	now := s.now().UnixMilli()
	if permit.IssuedAtMs > now+5_000 || now >= permit.ExpiresAtMs {
		return ErrPermitInvalid
	}
	expected, err := s.sign(permit)
	if err != nil || !hmac.Equal(expected, permit.Signature) {
		return ErrPermitInvalid
	}
	return nil
}

func (s *PermitService) sign(permit *friendpb.ActionPermit) ([]byte, error) {
	unsigned := proto.Clone(permit).(*friendpb.ActionPermit)
	unsigned.Signature = nil
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(unsigned)
	if err != nil {
		return nil, err
	}
	mac := hmac.New(sha256.New, s.secret)
	_, _ = mac.Write(payload)
	return mac.Sum(nil), nil
}
