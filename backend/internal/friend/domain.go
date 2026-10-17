package friend

// This file freezes the shared domain vocabulary of the Friendship authority
// (contract 33/33A). It is pure domain code: no Redis, no Actor, no Proto
// dependency. The canonical FriendRelation is the single source of truth;
// FriendIndex, PublicProfile and FarmOpportunitySummary are rebuildable query
// copies and never authorize anything.

import "errors"

// RelationStatus is the lifecycle of the canonical FriendRelation.
// Relations never expire by time (33 §2.3); only explicit delete or a block
// in either direction terminates them.
type RelationStatus uint8

const (
	RelationStatusNone    RelationStatus = 0
	RelationStatusActive  RelationStatus = 1
	RelationStatusDeleted RelationStatus = 2
)

func (s RelationStatus) String() string {
	switch s {
	case RelationStatusActive:
		return "ACTIVE"
	case RelationStatusDeleted:
		return "DELETED"
	default:
		return "NONE"
	}
}

// BlockStatus is the lifecycle of one directed BlockRelation (33 §2.4).
type BlockStatus uint8

const (
	BlockStatusNone    BlockStatus = 0
	BlockStatusActive  BlockStatus = 1
	BlockStatusRemoved BlockStatus = 2
)

// TerminationReason records why a relation left ACTIVE (33 §2.3).
type TerminationReason uint8

const (
	TerminationNone       TerminationReason = 0
	TerminationUserDelete TerminationReason = 1
	TerminationBlocked    TerminationReason = 2
)

// BarrierState is the reactivation barrier lifecycle (33 §3.3, Q33-06).
type BarrierState uint8

const (
	BarrierNone     BarrierState = 0
	BarrierPending  BarrierState = 1
	BarrierReady    BarrierState = 2
	BarrierConsumed BarrierState = 3
)

func (s BarrierState) String() string {
	switch s {
	case BarrierPending:
		return "PENDING"
	case BarrierReady:
		return "READY"
	case BarrierConsumed:
		return "CONSUMED"
	default:
		return "NONE"
	}
}

// TokenStatus is the ShareToken lifecycle (33 §4.5). EXPIRED is derived from
// trusted authority time and never persisted as a mutation.
type TokenStatus uint8

const (
	TokenStatusNone    TokenStatus = 0
	TokenStatusActive  TokenStatus = 1
	TokenStatusRevoked TokenStatus = 2
)

// SharePurpose is the only P0 token purpose (33 §4.1).
type SharePurpose uint8

const (
	SharePurposeUnspecified SharePurpose = 0
	SharePurposeAddFriend   SharePurpose = 1
)

// ActionScope distinguishes the two permit kinds (33 §7.4, §8.1).
type ActionScope uint8

const (
	ActionScopeUnspecified ActionScope = 0
	ActionScopeVisitFarm   ActionScope = 1
	ActionScopeCare        ActionScope = 2
	ActionScopeTheft       ActionScope = 3
)

// StatusClass is the deterministic terminal class of every authority
// mutation that crossed Execution-Accept (33 §3.2, 33A §3.2).
type StatusClass uint8

const (
	StatusUnspecified StatusClass = 0
	StatusSucceeded   StatusClass = 1
	StatusRejected    StatusClass = 2
	StatusNoChange    StatusClass = 3
)

// ReasonCode is the deterministic business reason attached to a terminal
// outcome. Technical conflicts (version/schema/capacity) are a separate
// axis and never masquerade as business results (33A §9.4).
type ReasonCode string

const (
	ReasonNone                          ReasonCode = ""
	ReasonNotFriends                    ReasonCode = "NOT_FRIENDS"
	ReasonAlreadyFriends                ReasonCode = "ALREADY_FRIENDS"
	ReasonAlreadyBlocked                ReasonCode = "ALREADY_BLOCKED"
	ReasonAlreadyUnblocked              ReasonCode = "ALREADY_UNBLOCKED"
	ReasonBlocked                       ReasonCode = "BLOCKED"
	ReasonSelfAdd                       ReasonCode = "SELF_ADD"
	ReasonFriendLimitReached            ReasonCode = "FRIEND_LIMIT_REACHED"
	ReasonShareTokenExpired             ReasonCode = "SHARE_TOKEN_EXPIRED"
	ReasonShareTokenRevoked             ReasonCode = "SHARE_TOKEN_REVOKED"
	ReasonShareTokenStaleGeneration     ReasonCode = "SHARE_TOKEN_STALE_GENERATION"
	ReasonShareTokenPurposeMismatch     ReasonCode = "SHARE_TOKEN_PURPOSE_MISMATCH"
	ReasonShareTokenPredatesRelationDel ReasonCode = "SHARE_TOKEN_PREDATES_RELATION_DELETE"
	ReasonReactivationRotateRequired    ReasonCode = "REACTIVATION_ROTATE_REQUIRED"
	ReasonRelationReactivationPending   ReasonCode = "RELATION_REACTIVATION_PENDING"
	ReasonRelationCleanupPending        ReasonCode = "RELATION_CLEANUP_PENDING"
	ReasonDomainEpochConflict           ReasonCode = "DOMAIN_EPOCH_CONFLICT"
	ReasonBarrierRevisionMismatch       ReasonCode = "BARRIER_REVISION_MISMATCH"
	ReasonVisitNotPermitted             ReasonCode = "VISIT_NOT_PERMITTED"
	ReasonRelationAlreadyTerminated     ReasonCode = "RELATION_ALREADY_TERMINATED"
)

// Technical (non-business) errors. Business rejections are values inside
// Outcome, never Go errors; Go errors are reserved for malformed input and
// idempotency/technical conflicts.
var (
	ErrInvalidPair          = errors.New("friend: invalid player pair")
	ErrNotFound             = errors.New("friend: relationship not found")
	ErrInvalidCommand       = errors.New("friend: invalid command")
	ErrIdempotentConflict   = errors.New("friend: idempotency conflict")
	ErrOutcomeNotFound      = errors.New("friend: stored outcome not found")
	ErrReservationNotFound  = errors.New("friend: reservation not found")
	ErrBarrierProofMismatch = errors.New("friend: reactivation barrier proof mismatch")
	// ErrPairActionScopeNotOwned is a normal shared-DueIndex routing result:
	// the WorkType is generic, but its typed payload belongs to another Pair
	// mutation family and must not be handled or mutated as CARE/THEFT.
	ErrPairActionScopeNotOwned = errors.New("friend: Pair work is not an ActionScope order")
)

// CommitIdentity is the business idempotency identity of one authority
// mutation (33A §3.2). Public operations carry the Root-scoped effective
// idempotency key; workflow participants carry the participant invocation.
// Exactly one of the two kinds is set.
type CommitIdentity struct {
	// Public operation identity (root = this authority).
	SubjectPlayerID uint64
	CommandType     string
	OperationID     string

	// Workflow participant identity.
	WorkAddressID           string
	ParticipantInvocationID string
	ParticipantStep         string
}

// IsPublic reports whether the identity is a public operation.
func (c CommitIdentity) IsPublic() bool { return c.OperationID != "" }

// Key returns the canonical lookup key used by receipt stores. The kind
// prefix prevents public digests and participant digests from colliding
// (33A §2.5).
func (c CommitIdentity) Key() string {
	if c.IsPublic() {
		return "op:" + c.CommandType + ":" + itoa(c.SubjectPlayerID) + ":" + c.OperationID
	}
	return "participant:" + c.ParticipantInvocationID + ":" + c.ParticipantStep
}

// Valid performs shape validation. Empty identities are rejected; an
// identity carrying both kinds is rejected.
func (c CommitIdentity) Valid() bool {
	if c.IsPublic() {
		return c.CommandType != "" && c.SubjectPlayerID != 0 &&
			c.ParticipantInvocationID == "" && c.ParticipantStep == ""
	}
	return c.ParticipantInvocationID != "" && c.ParticipantStep != "" &&
		c.CommandType == "" && c.OperationID == ""
}

// Outcome is the deterministic, persisted terminal result of one authority
// mutation. Replays of the same CommitIdentity with the same payload hash
// return the stored Outcome; the same identity with a different payload hash
// is an idempotency conflict.
type Outcome struct {
	Status         StatusClass
	Reason         ReasonCode
	Identity       CommitIdentity
	PayloadHash    []byte
	CommittedAtMs  int64
	CommittedEpoch uint64 // relation_epoch / block_epoch / token_generation after commit, when meaningful
	Detail         string // optional deterministic detail, e.g. which side hit FRIEND_LIMIT_REACHED
}

func (o Outcome) succeeded() bool { return o.Status == StatusSucceeded }

// payloadEqual compares a stored payload hash with a candidate.
func payloadEqual(stored, candidate []byte) bool {
	if len(stored) != len(candidate) {
		return false
	}
	var diff byte
	for i := range stored {
		diff |= stored[i] ^ candidate[i]
	}
	return diff == 0
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
