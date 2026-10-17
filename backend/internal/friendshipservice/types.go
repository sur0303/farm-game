package friendshipservice

import (
	"context"
	"errors"

	publicpb "farmgame/api/gen/public"
	"farmgame/internal/friend"
)

var (
	ErrInvalidArgument   = errors.New("friendship service: invalid argument")
	ErrShareLinkInvalid  = errors.New("friendship service: share link invalid")
	ErrShareSelfInvite   = errors.New("friendship service: share self invite")
	ErrNotFriend         = errors.New("friendship service: friend relationship not found")
	ErrMalformedResponse = errors.New("friendship service: malformed RPC response")
	ErrUnavailable       = errors.New("friendship service: unavailable")
	// ErrCapacity is the resource-exhausted classification: the backend
	// rejected the call at a rate/concurrency gate. HTTP adapters should
	// translate it to 429 + Retry-After, never to the generic 503.
	ErrCapacity = errors.New("friendship service: capacity exhausted")
)

const ShareAcceptModeMulti = "MULTI_ACCEPT"

// ShareProjection is the HTTPS token-to-issuer lookup record. FriendPair is
// relationship authority; this projection only preserves the frozen public
// share-link SCB.
type ShareProjection struct {
	InviterPlayerID uint64
	CreatedAtMs     int64
	ExpiresAtMs     int64
	AcceptMode      string
}

type ShareProjectionStore interface {
	Create(context.Context, uint64) (string, error)
	Resolve(context.Context, string) (ShareProjection, error)
	ResolveForAdmission(context.Context, string) (ShareProjection, error)
	AlignAuthorityToken(context.Context, string, int64, int64) error
}

type Outcome struct {
	Status         friend.StatusClass
	Reason         friend.ReasonCode
	Identity       friend.CommitIdentity
	CommittedEpoch uint64
	CommittedAtMs  int64
	Detail         string
	PayloadHash    []byte
}

type PairState struct {
	RelationID    string
	RelationEpoch uint64
	Status        friend.RelationStatus
	Changed       bool
}

type CreateShareInput struct {
	SubjectPlayerID uint64
	OperationID     string
	ObservationOnly bool
}

type CreateShareResult struct {
	Outcome         Outcome
	ShareToken      string
	AcceptMode      string
	TokenGeneration uint64
	IssuedAtMs      int64
	ExpiresAtMs     int64
}

type ResolveShareInput struct{ ShareToken string }

type ResolveShareResult struct {
	InviterPlayerID uint64
	InviterProfile  *publicpb.PublicProfileView
	AcceptMode      string
}

type RedeemShareInput struct {
	RedeemerPlayerID uint64
	ShareToken       string
	OperationID      string
	ObservationOnly  bool
}

type RedeemShareResult struct {
	Outcome         Outcome
	Pair            PairState
	InviterPlayerID uint64
	InviterProfile  *publicpb.PublicProfileView
}

type GetFriendListInput struct {
	ViewerPlayerID uint64
	Cursor         string
	PageSize       int
}

type GetFriendListResult struct {
	Page friend.FriendPage
}

type PairMutationInput struct {
	CallerPlayerID  uint64
	PeerPlayerID    uint64
	OperationID     string
	ObservationOnly bool
}

type PairMutationResult struct {
	Outcome Outcome
	Pair    PairState
}

type NextOpportunityStatus uint8

const (
	NextOpportunityUnspecified NextOpportunityStatus = iota
	NextOpportunityFound
	NextOpportunityNone
	NextOpportunityUnknown
)

const MaxFriendOpportunityHintedTargets = 16

type FriendOpportunityHintedTarget struct {
	OwnerID             uint64
	OpportunityRevision uint64
}

type GetNextFriendOpportunityInput struct {
	ViewerPlayerID uint64
	HintedTargets  []FriendOpportunityHintedTarget
}

type GetNextFriendOpportunityResult struct {
	Status NextOpportunityStatus
	Friend *friend.FriendCard
}

type AuthorizeVisitInput struct {
	ViewerPlayerID  uint64
	OwnerPlayerID   uint64
	TargetFarmID    uint64
	JoinOperationID string
	JoinAttemptID   string
	NowMs           int64
	TTLMs           int64
}

type AuthorizeVisitResult struct {
	Outcome Outcome
	Permit  friend.VisitPermitSnapshot
}

type Client interface {
	CreateShare(context.Context, CreateShareInput) (CreateShareResult, error)
	ResolveShare(context.Context, ResolveShareInput) (ResolveShareResult, error)
	RedeemShare(context.Context, RedeemShareInput) (RedeemShareResult, error)
	GetFriendList(context.Context, GetFriendListInput) (GetFriendListResult, error)
	DeleteFriend(context.Context, PairMutationInput) (PairMutationResult, error)
	BlockFriend(context.Context, PairMutationInput) (PairMutationResult, error)
	UnblockFriend(context.Context, PairMutationInput) (PairMutationResult, error)
	GetNextFriendOpportunity(context.Context, GetNextFriendOpportunityInput) (GetNextFriendOpportunityResult, error)
	AuthorizeVisit(context.Context, AuthorizeVisitInput) (AuthorizeVisitResult, error)
	CommitFriendActionAdmission(context.Context, friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error)
	GetFriendActionAdmission(context.Context, friend.ActionAdmissionCommand) (friend.ActionAdmissionOutcome, error)
}
