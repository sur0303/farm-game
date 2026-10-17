// Package authoritystore defines the storage-independent authority contract
// used by PlayerActor and FarmActor. It deliberately contains no Redis key,
// Lua script, connection or Tcaplus type.
package authoritystore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"farmgame/internal/actormodel/fieldv2"
	"farmgame/internal/actorrouting"
	"farmgame/internal/identity"
	"farmgame/internal/workidentity"
)

type ActorAddress = actorrouting.ActorAddress

var (
	ErrInvalidRequest           = errors.New("authoritystore: invalid request")
	ErrAuthorityAddressMismatch = errors.New("authoritystore: authority address mismatch")
	ErrCapacityExceeded         = errors.New("authoritystore: capacity exceeded")
	ErrActorNotFound            = errors.New("authoritystore: actor not found")
	ErrSourceWorkNotFound       = errors.New("authoritystore: source work not found")
	ErrWorkConflict             = errors.New("authoritystore: attached work conflict")
	ErrScheduleConflict         = errors.New("authoritystore: attached schedule conflict")
	ErrRecoveryOverdue          = errors.New("authoritystore: recovery evidence overdue")
	ErrInjectedFault            = errors.New("authoritystore: injected read fault")
	ErrStaleLeaseGrant          = errors.New("authoritystore: stale lease grant")
	ErrInconsistentLeaseGrant   = errors.New("authoritystore: inconsistent lease grant")
	ErrLeaseDeadlineExpired     = errors.New("authoritystore: local lease deadline expired")
)

type ShardResolver interface {
	ShardFor(domain actorrouting.RouteDomain, actorID uint64) (uint32, error)
	Profile() actorrouting.HashProfile
}

type ShardAuthorityState uint8

const (
	ShardAuthorityUnknown ShardAuthorityState = iota
	ShardAuthorityUnassigned
	ShardAuthorityActive
)

func (s ShardAuthorityState) String() string {
	switch s {
	case ShardAuthorityUnassigned:
		return "UNASSIGNED"
	case ShardAuthorityActive:
		return "ACTIVE"
	default:
		return "UNKNOWN"
	}
}

type ShardWriteAuthority struct {
	AuthorityLayoutVersion string
	HashVersion            uint32
	HashSeed               uint64
	Domain                 actorrouting.RouteDomain
	VirtualShard           uint32
	HolderID               string
	HolderBootID           string
	AssignmentRevision     uint64
	WriterEpoch            uint64
	LeaseEpoch             uint64
	State                  ShardAuthorityState
	LeaseUntilMs           int64
	LastTransitionID       string
	LastTransitionAtMs     int64
}

// ShardWriteAuthorityObservation is the read-only recovery view of one
// authority record. LastTransitionDigest is deliberately kept outside the
// routable authority tuple: it proves the exact mutation intent that produced
// LastTransitionID, but it is not itself part of a holder generation.
type ShardWriteAuthorityObservation struct {
	Authority            ShardWriteAuthority
	LastTransitionDigest string
}

// ShardAuthorityTransitionIntentDigest is the storage-independent digest
// contract shared by the Redis transition scripts and Manager recovery. Every
// field is encoded as a 4-byte big-endian length followed by its bytes before
// SHA-256; changing field order or omitting a field changes the digest.
func ShardAuthorityTransitionIntentDigest(kind, layout, domain string, shard uint32, fence ShardWriteFence, newHolderID, newHolderBootID string, requestedTTLms int64) string {
	parts := [...]string{
		kind, layout, domain, strconv.FormatUint(uint64(shard), 10),
		fence.ExpectedHolderID, fence.ExpectedHolderBootID,
		strconv.FormatUint(fence.ExpectedAssignmentRevision, 10),
		strconv.FormatUint(fence.ExpectedWriterEpoch, 10),
		strconv.FormatUint(fence.ExpectedLeaseEpoch, 10),
		newHolderID, newHolderBootID, strconv.FormatInt(requestedTTLms, 10),
	}
	h := sha256.New()
	var size [4]byte
	for _, part := range parts {
		binary.BigEndian.PutUint32(size[:], uint32(len(part)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

type ShardWriteFence struct {
	ExpectedHolderID           string
	ExpectedHolderBootID       string
	ExpectedAssignmentRevision uint64
	ExpectedWriterEpoch        uint64
	ExpectedLeaseEpoch         uint64
}

type ShardAuthorityCallerKind uint8

const (
	ShardAuthorityCallerUnknown ShardAuthorityCallerKind = iota
	ShardAuthorityCallerRouteManager
	ShardAuthorityCallerHolder
)

// TrustedShardAuthorityCaller is constructed only after the transport adapter
// authenticates the workload identity. It is not a client-provided role.
type TrustedShardAuthorityCaller struct {
	Kind       ShardAuthorityCallerKind
	InstanceID string
	BootID     string
}

type ShardAuthorityTransitionStatus uint8

const (
	ShardTransitionUnknown ShardAuthorityTransitionStatus = iota
	ShardTransitionApplied
	ShardTransitionAlreadyApplied
	ShardTransitionActiveLeaseConflict
	ShardTransitionFenceMismatch
	ShardTransitionLeaseExpired
	ShardTransitionNotFound
	ShardTransitionUnauthorized
	ShardTransitionReconcileRequired
	ShardTransitionUnavailableNotSent
	ShardTransitionTimeoutUnknown
)

func (s ShardAuthorityTransitionStatus) String() string {
	switch s {
	case ShardTransitionApplied:
		return "APPLIED"
	case ShardTransitionAlreadyApplied:
		return "ALREADY_APPLIED"
	case ShardTransitionActiveLeaseConflict:
		return "ACTIVE_LEASE_CONFLICT"
	case ShardTransitionFenceMismatch:
		return "FENCE_MISMATCH"
	case ShardTransitionLeaseExpired:
		return "LEASE_EXPIRED"
	case ShardTransitionNotFound:
		return "NOT_FOUND"
	case ShardTransitionUnauthorized:
		return "UNAUTHORIZED"
	case ShardTransitionReconcileRequired:
		return "RECONCILE_REQUIRED"
	case ShardTransitionUnavailableNotSent:
		return "UNAVAILABLE_NOT_SENT"
	case ShardTransitionTimeoutUnknown:
		return "TIMEOUT_UNKNOWN"
	default:
		return "UNKNOWN"
	}
}

type ClaimShardAuthorityRequest struct {
	Caller                 TrustedShardAuthorityCaller
	TransitionID           string
	AuthorityLayoutVersion string
	Domain                 actorrouting.RouteDomain
	VirtualShard           uint32
	NewHolderID            string
	NewHolderBootID        string
	RequestedTTLms         int64
}

type RenewShardAuthorityRequest struct {
	Caller                 TrustedShardAuthorityCaller
	AuthorityLayoutVersion string
	Domain                 actorrouting.RouteDomain
	VirtualShard           uint32
	ExpectedFence          ShardWriteFence
	RequestedTTLms         int64
}

type TransferShardAuthorityRequest struct {
	Caller                 TrustedShardAuthorityCaller
	TransitionID           string
	AuthorityLayoutVersion string
	Domain                 actorrouting.RouteDomain
	VirtualShard           uint32
	ExpectedFence          ShardWriteFence
	NewHolderID            string
	NewHolderBootID        string
	RequestedTTLms         int64
}

// ExpiredTakeoverShardAuthorityRequest is intentionally wire-compatible with
// TransferShardAuthorityRequest, but has the opposite lease precondition:
// the exact fenced source generation must still be present and its lease must
// already be expired. Keeping this as a distinct operation prevents an
// ordinary planned transfer from silently changing semantics at expiry.
type ExpiredTakeoverShardAuthorityRequest = TransferShardAuthorityRequest

type UnassignShardAuthorityRequest struct {
	Caller                 TrustedShardAuthorityCaller
	TransitionID           string
	AuthorityLayoutVersion string
	Domain                 actorrouting.RouteDomain
	VirtualShard           uint32
	ExpectedFence          ShardWriteFence
}

type ShardAuthorityTransitionResult struct {
	Status        ShardAuthorityTransitionStatus
	Authority     ShardWriteAuthority
	AcceptedTTLms int64
}

type ShardWriteAuthorityStore interface {
	ClaimShardAuthority(context.Context, ClaimShardAuthorityRequest) (ShardAuthorityTransitionResult, error)
	RenewShardAuthority(context.Context, RenewShardAuthorityRequest) (ShardAuthorityTransitionResult, error)
	TransferShardAuthority(context.Context, TransferShardAuthorityRequest) (ShardAuthorityTransitionResult, error)
	UnassignShardAuthority(context.Context, UnassignShardAuthorityRequest) (ShardAuthorityTransitionResult, error)
}

// ExpiredTakeoverShardAuthorityStore is an optional production extension used
// only by crash recovery. The expected source fence is checked atomically with
// the expired-lease test and the new generation write.
type ExpiredTakeoverShardAuthorityStore interface {
	TakeoverExpiredShardAuthority(context.Context, ExpiredTakeoverShardAuthorityRequest) (ShardAuthorityTransitionResult, error)
}

type AuthorityGroupRef struct {
	AuthorityLayoutVersion string
	Domain                 actorrouting.RouteDomain
	ActorID                uint64
	VirtualShard           uint32
}

type ModelState map[string][]byte

// ModelLayoutVersion identifies the logical Actor model layout inside the
// existing authority record. It is deliberately independent of
// AuthorityLayoutVersion: the latter remains the static shard/key routing
// profile (currently v1), while this discriminator selects exactly one model
// truth set in that same record.
type ModelLayoutVersion uint8

const (
	ModelLayoutUnknown ModelLayoutVersion = iota
	ModelLayoutV1
	ModelLayoutV2
)

func (v ModelLayoutVersion) String() string {
	switch v {
	case ModelLayoutV1:
		return "1"
	case ModelLayoutV2:
		return "2"
	default:
		return "0"
	}
}

// ModelMutationIntent distinguishes an ordinary sparse mutation from the
// one-way v1-to-v2 installation. Migration is not a side script: it uses the
// same receipt, fence, WriterClaim, RecordVersion and PersistenceLane as every
// other Actor commit.
type ModelMutationIntent uint8

const (
	ModelMutationIntentUnknown ModelMutationIntent = iota
	ModelMutationIntentUpdate
	ModelMutationIntentMigrateV1ToV2
)

// ActorModelMutation carries sparse physical writes alongside CandidateState,
// which remains the bounded full post-commit snapshot. The resulting claims
// are caller-supplied and checked exactly by every Store implementation.
// ModelDeletes and ResultingManifest must be strictly sorted.
type ActorModelMutation struct {
	Intent                       ModelMutationIntent
	ModelUpserts                 ModelState
	ModelDeletes                 []string
	ResultingManifest            []string
	ResultingTotalBytes          int
	ResultingBusinessStateDigest PayloadHash
	ResultingModelLayout         ModelLayoutVersion
}

// ModelFieldReadResult is one bounded read-only authority slice. Found=false
// means the Actor record does not exist. A missing requested v1 projection is
// left out of ModelFields and may use its explicitly defined v1 compatibility
// path. V2 readers must fail closed and never fall back to model:record.
type ModelFieldReadResult struct {
	ActorID       uint64
	RecordVersion uint64
	ModelLayout   ModelLayoutVersion
	ModelFields   ModelState
	Found         bool
	Err           error
}

// ModelFieldBatchReader is the read-only query path for a bounded list of
// Actor IDs and named model fields. It never claims writer ownership and is
// deliberately separate from ActorAuthorityStore.
type ModelFieldBatchReader interface {
	BatchReadModelFields(ctx context.Context, domain actorrouting.RouteDomain, actorIDs []uint64, fieldNames []string) ([]ModelFieldReadResult, error)
}

type PayloadHash [sha256.Size]byte

// IdempotencyKey is the frozen root-scoped effective idempotency key
// (contract 28 §3.2 / 31 §8.1): subject_player_id +
// stable_command_type_name + operation_id + root.kind + root.canonical_id.
// Its OperationDigest only locates a receipt; stores must also compare the
// full key and the payload hash before declaring replay vs conflict.
// The root itself is derived by CommandCatalog/FirstHop/Coordinator (e.g. a
// FRIEND_PAIR operation may commit into the target FarmActor); stores never
// re-derive it and never equate it with the commit target actor.
type IdempotencyKey = identity.EffectiveIdempotencyKey

// OperationRootRef re-exports the frozen root reference for callers that
// only import this package.
type OperationRootRef = identity.OperationRootRef

// RootAuthorityKind re-exports the frozen root kinds.
type RootAuthorityKind = identity.RootAuthorityKind

const (
	RootKindPlayerActor     = identity.RootKindPlayerActor
	RootKindFarmActor       = identity.RootKindFarmActor
	RootKindFriendPair      = identity.RootKindFriendPair
	RootKindIndependentWork = identity.RootKindIndependentWork
)

type TerminalStatus uint8

const (
	TerminalUnknown TerminalStatus = iota
	TerminalSucceeded
	TerminalRejected
	TerminalNoChange
)

func (s TerminalStatus) String() string {
	switch s {
	case TerminalSucceeded:
		return "SUCCEEDED"
	case TerminalRejected:
		return "REJECTED"
	case TerminalNoChange:
		return "NO_CHANGE"
	default:
		return "UNKNOWN"
	}
}

type StoredOperationOutcome struct {
	Status    TerminalStatus
	ErrorCode string
	Payload   []byte
}

type AuthorityReceipt struct {
	ReceiptID            string
	IdempotencyKey       IdempotencyKey
	CanonicalPayloadHash PayloadHash
	Outcome              StoredOperationOutcome
}

type AttachedOrder struct {
	OrderID     string
	OrderType   string
	Payload     []byte
	PayloadHash PayloadHash
	DueAtMs     int64
	// OperationID is the business operation bound to this SourceOrder. It is
	// distinct from the synthetic actor-CAS idempotency operation used to
	// commit the enclosing actor record. Empty keeps the historical fallback
	// to that commit operation for generic callers.
	OperationID string
	// SubjectPlayerID is persisted onto SourceOrder.subject_player_id when
	// non-zero. CareRewardOrder must use the carer (not the Farm Actor id from
	// the synthetic CASEpoch idempotency key). Zero keeps the commit-key subject.
	SubjectPlayerID uint64
}

// AttachedSchedule is the mutable, source-local wakeup primitive. Unlike an
// AttachedOrder, ScheduleID/Type identify one current schedule slot whose
// content is ordered by Generation. A zero DueAtMs is a cancellation/tombstone
// for an existing schedule generation; positive DueAtMs is an executable
// boundary in the source authority's UTC millisecond clock.
type AttachedSchedule struct {
	ScheduleID      string
	ScheduleType    string
	Generation      uint64
	DueAtMs         int64
	Payload         []byte
	PayloadHash     PayloadHash
	SubjectPlayerID uint64
}

type AttachedScheduleSnapshot struct {
	AttachedSchedule
	State             string
	WorkVersion       uint64
	LeaseHolderID     string
	LeaseHolderBootID string
	LeaseEpoch        uint64
	LeaseExpireAtMs   int64
	CreatedAtMs       int64
	UpdatedAtMs       int64
	CompletedAtMs     int64
}

type ScheduleWorkStatus uint8

const (
	ScheduleWorkUnknown ScheduleWorkStatus = iota
	ScheduleWorkClaimed
	ScheduleWorkLeaseHeld
	ScheduleWorkNotDue
	ScheduleWorkNotFound
	ScheduleWorkNotClaimable
	ScheduleWorkAdvanced
	ScheduleWorkCompleted
	ScheduleWorkAlreadyCompleted
	ScheduleWorkFenceMismatch
	ScheduleWorkLeaseExpired
	ScheduleWorkStaleVersion
	ScheduleWorkStaleGeneration
)

func (s ScheduleWorkStatus) String() string {
	switch s {
	case ScheduleWorkClaimed:
		return "CLAIMED"
	case ScheduleWorkLeaseHeld:
		return "LEASE_HELD"
	case ScheduleWorkNotDue:
		return "NOT_DUE"
	case ScheduleWorkNotFound:
		return "NOT_FOUND"
	case ScheduleWorkNotClaimable:
		return "NOT_CLAIMABLE"
	case ScheduleWorkAdvanced:
		return "ADVANCED"
	case ScheduleWorkCompleted:
		return "COMPLETED"
	case ScheduleWorkAlreadyCompleted:
		return "ALREADY_COMPLETED"
	case ScheduleWorkFenceMismatch:
		return "FENCE_MISMATCH"
	case ScheduleWorkLeaseExpired:
		return "LEASE_EXPIRED"
	case ScheduleWorkStaleVersion:
		return "STALE_VERSION"
	case ScheduleWorkStaleGeneration:
		return "STALE_GENERATION"
	default:
		return "UNKNOWN"
	}
}

type ScheduleWorkClaim struct {
	Status            ScheduleWorkStatus
	Snapshot          AttachedScheduleSnapshot
	LeaseConflictAtMs int64
	AcceptedTTLms     int64
}

type ScheduleWorkAdvanceResult struct {
	Status      ScheduleWorkStatus
	WorkVersion uint64
	Generation  uint64
	NextDueAtMs int64
}

type ScheduleWorkCompleteResult struct {
	Status ScheduleWorkStatus
}

// SourceWorkRecord is the read-only admission view of an order committed in
// an Actor authority bucket. It mirrors the existing SourceOrder fields; it
// does not create another workflow record or grant mutation capability.
type SourceWorkRecord struct {
	SchemaVersion            string
	WorkID                   string
	WorkType                 string
	PlacementKind            string
	BucketVersion            uint32
	BucketID                 uint32
	SourceAuthorityKind      string
	CanonicalSourceAuthority string
	AuthorityLayoutVersion   string
	ActorRouteDomain         actorrouting.RouteDomain
	SubjectPlayerID          uint64
	OperationID              string
	PayloadHash              string
	State                    string
	WorkVersion              uint64
	CurrentStep              string
	TypedPayload             []byte
	NextDueAtMs              int64
	RetryCount               uint64
	LastErrorClass           string
	LeaseHolderID            string
	LeaseHolderBootID        string
	LeaseEpoch               uint64
	LeaseTokenHash           string
	LeaseExpireAtMs          int64
	CreatedAtMs              int64
	UpdatedAtMs              int64
	CompletedAtMs            int64
	ReadAtMs                 int64
}

// SourceWorkReader proves an existing source-slotted order for Participant
// admission. Implementations are read-only and derive physical placement
// from the source Actor address rather than trusting caller-supplied keys.
type SourceWorkReader interface {
	ReadSourceWork(context.Context, ActorAddress, string, string) (SourceWorkRecord, error)
}

type PendingOutbox struct {
	OutboxID  string
	EventType string
	Payload   []byte
	DueAtMs   int64
}

type AuthoritySnapshot struct {
	ActorAddress        ActorAddress
	WriterEpoch         uint64
	WriterClaimID       string
	RecordVersion       uint64
	ModelLayout         ModelLayoutVersion
	BusinessStateDigest PayloadHash
	ModelFields         ModelState
	PhysicalUsage       PhysicalUsage
}

// PhysicalUsage is the version-bound aggregate physical state of one actor
// record. Trustworthy is explicit because zero receipts/bytes are valid. A
// consumer must also require RecordVersion to match the enclosing authority
// snapshot before using the values for pre-admission.
type PhysicalUsage struct {
	Trustworthy   bool
	RecordVersion uint64
	ModelBytes    int
	ReceiptItems  int
	ReceiptBytes  int
}

func (u PhysicalUsage) Validate(recordVersion uint64) error {
	if !u.Trustworthy || recordVersion == 0 || u.RecordVersion != recordVersion ||
		u.ModelBytes < 0 || u.ReceiptItems < 0 || u.ReceiptBytes < 0 {
		return fmt.Errorf("%w: unavailable or stale physical usage", ErrInvalidRequest)
	}
	return nil
}

type LoadAndClaimStatus uint8

const (
	LoadAndClaimUnknown LoadAndClaimStatus = iota
	LoadAndClaimClaimed
	LoadAndClaimAlreadyClaimed
	LoadAndClaimWriterEpochMismatch
	LoadAndClaimUnavailableNotSent
	LoadAndClaimTimeoutUnknown
	LoadAndClaimReconcileRequired
)

func (s LoadAndClaimStatus) String() string {
	switch s {
	case LoadAndClaimClaimed:
		return "CLAIMED"
	case LoadAndClaimAlreadyClaimed:
		return "ALREADY_CLAIMED"
	case LoadAndClaimWriterEpochMismatch:
		return "WRITER_EPOCH_MISMATCH"
	case LoadAndClaimUnavailableNotSent:
		return "UNAVAILABLE_NOT_SENT"
	case LoadAndClaimTimeoutUnknown:
		return "TIMEOUT_UNKNOWN"
	case LoadAndClaimReconcileRequired:
		return "RECONCILE_REQUIRED"
	default:
		return "UNKNOWN"
	}
}

type LoadAndClaimRequest struct {
	ActorAddress           ActorAddress
	AuthorityLayoutVersion string
	ShardWriteFence        ShardWriteFence
	ClaimOperationID       string
	NewWriterClaimID       string
	InitialModelFields     ModelState
	// InitialModelLayout defaults to v1. Normal runtime remains write=v1;
	// callers may request v2 only through an explicitly qualified policy.
	InitialModelLayout ModelLayoutVersion
}

type LoadAndClaimResult struct {
	Status   LoadAndClaimStatus
	Snapshot AuthoritySnapshot
}

type AuthorityCommitStatus uint8

const (
	CommitUnknown AuthorityCommitStatus = iota
	CommitCommitted
	CommitAlreadyCommitted
	CommitIdempotencyConflict
	CommitRecordVersionConflict
	CommitWriterEpochMismatch
	CommitWriterClaimMismatch
	CommitUnavailableNotSent
	CommitTimeoutUnknown
)

func (s AuthorityCommitStatus) String() string {
	switch s {
	case CommitCommitted:
		return "COMMITTED"
	case CommitAlreadyCommitted:
		return "ALREADY_COMMITTED"
	case CommitIdempotencyConflict:
		return "IDEMPOTENCY_CONFLICT"
	case CommitRecordVersionConflict:
		return "RECORD_VERSION_CONFLICT"
	case CommitWriterEpochMismatch:
		return "WRITER_EPOCH_MISMATCH"
	case CommitWriterClaimMismatch:
		return "WRITER_CLAIM_MISMATCH"
	case CommitUnavailableNotSent:
		return "UNAVAILABLE_NOT_SENT"
	case CommitTimeoutUnknown:
		return "TIMEOUT_UNKNOWN"
	default:
		return "UNKNOWN"
	}
}

type AuthorityCommitRequest struct {
	CommitRequestID        string
	ActorAddress           ActorAddress
	AuthorityLayoutVersion string
	AssertedAuthorityGroup *AuthorityGroupRef
	ShardWriteFence        ShardWriteFence
	ExpectedWriterClaimID  string
	ExpectedRecordVersion  uint64
	IdempotencyKey         IdempotencyKey
	CanonicalPayloadHash   PayloadHash
	// CandidateState is the complete immutable snapshot to install only after
	// an explicit COMMITTED result. ModelMutation optionally narrows the
	// physical HSET/HDEL set while binding the exact resulting manifest, bytes,
	// digest and model layout. A nil ModelMutation preserves the v1 full-state
	// API and is adapted to this same primitive by Store implementations.
	CandidateState    ModelState
	ModelMutation     *ActorModelMutation
	Receipt           AuthorityReceipt
	OperationResult   StoredOperationOutcome
	AttachedOrders    []AttachedOrder
	AttachedSchedules []AttachedSchedule
	PendingOutbox     []PendingOutbox
}

type CommitResult struct {
	Status   AuthorityCommitStatus
	Snapshot AuthoritySnapshot
	Outcome  StoredOperationOutcome
	// CommitStamp is present only on a definite first COMMITTED output. Its
	// sealed interface is exact-request-bound; ALREADY_COMMITTED and unknown
	// outcomes deliberately leave it nil.
	CommitStamp CommitStamp
}

type FenceResolutionStatus uint8

const (
	FenceResolutionUnknown FenceResolutionStatus = iota
	FenceResolutionCommitted
	FenceResolutionNotCommitted
	FenceResolutionReconcileRequired
	FenceResolutionWriterEpochMismatch
	FenceResolutionWriterClaimMismatch
	FenceResolutionUnavailableNotSent
	FenceResolutionTimeoutUnknown
)

func (s FenceResolutionStatus) String() string {
	switch s {
	case FenceResolutionCommitted:
		return "COMMITTED"
	case FenceResolutionNotCommitted:
		return "NOT_COMMITTED"
	case FenceResolutionReconcileRequired:
		return "RECONCILE_REQUIRED"
	case FenceResolutionWriterEpochMismatch:
		return "WRITER_EPOCH_MISMATCH"
	case FenceResolutionWriterClaimMismatch:
		return "WRITER_CLAIM_MISMATCH"
	case FenceResolutionUnavailableNotSent:
		return "UNAVAILABLE_NOT_SENT"
	case FenceResolutionTimeoutUnknown:
		return "TIMEOUT_UNKNOWN"
	default:
		return "UNKNOWN"
	}
}

type FenceAndClaimRequest struct {
	ResolveOperationID        string
	ActorAddress              ActorAddress
	AuthorityLayoutVersion    string
	AssertedAuthorityGroup    *AuthorityGroupRef
	ShardWriteFence           ShardWriteFence
	OldWriterClaimID          string
	NewWriterClaimID          string
	OriginalIdempotencyKey    IdempotencyKey
	CanonicalPayloadHash      PayloadHash
	BeforeRecordVersion       uint64
	BeforeBusinessStateDigest PayloadHash
}

type ResolutionReceipt struct {
	ResolveOperationID        string
	ActorAddress              ActorAddress
	OldWriterClaimID          string
	NewWriterClaimID          string
	OriginalIdempotencyKey    IdempotencyKey
	CanonicalPayloadHash      PayloadHash
	BeforeRecordVersion       uint64
	BeforeBusinessStateDigest PayloadHash
	CutRecordVersion          uint64
	CutBusinessStateDigest    PayloadHash
	Status                    FenceResolutionStatus
	OriginalReceiptID         string
}

type FenceAndClaimResult struct {
	Status     FenceResolutionStatus
	Snapshot   AuthoritySnapshot
	Outcome    StoredOperationOutcome
	Resolution ResolutionReceipt
}

type IdempotencyLookupStatus uint8

const (
	IdempotencyUnknown IdempotencyLookupStatus = iota
	IdempotencyFoundMatch
	IdempotencyNotFound
	IdempotencyHashConflict
	IdempotencyRecoveryOverdue
)

type IdempotencyLookupRequest struct {
	ActorAddress         ActorAddress
	IdempotencyKey       IdempotencyKey
	CanonicalPayloadHash PayloadHash
}

type IdempotencyLookupResult struct {
	Status     IdempotencyLookupStatus
	Outcome    StoredOperationOutcome
	ReceiptRef string
}

type ActorAuthorityStore interface {
	LoadAndClaim(context.Context, LoadAndClaimRequest) (LoadAndClaimResult, error)
	Commit(context.Context, AuthorityCommitRequest) (CommitResult, error)
	FenceAndClaim(context.Context, FenceAndClaimRequest) (FenceAndClaimResult, error)
	LookupIdempotencyOutcome(context.Context, IdempotencyLookupRequest) (IdempotencyLookupResult, error)
}

// AuditedClaimCommitter is an optional hot-path extension for a Binding that
// has already completed LoadAndClaim's full model/receipt audit. The method
// must retain the ordinary Commit boundary: ShardWriteFence, WriterClaim,
// RecordVersion, targeted idempotency receipt, model integrity, capacity and
// attached work all remain atomically checked. It may rely on the invariant
// that historical receipt counters/manifests were audited when this exact
// WriterClaim was acquired and have since changed only through Store commits.
// Callers without that proof must use ActorAuthorityStore.Commit.
type AuditedClaimCommitter interface {
	CommitAuditedClaim(context.Context, AuthorityCommitRequest) (CommitResult, error)
}

// SoftFlushCommitter is an optional specialization for delayed dirty-state
// writeback. It retains every AuditedClaimCommitter fence and atomicity rule,
// but a newly COMMITTED result may omit Snapshot.ModelFields because the
// caller already owns the exact immutable candidate installed by that commit.
// Snapshot metadata (record version, digest and physical usage) remains
// mandatory. Replay/conflict results keep the ordinary complete-snapshot
// contract so recovery can reconcile against the authoritative state.
//
// Implementations must not retain request maps or byte slices after the call.
// Callers without an immutable frozen candidate must use Commit or
// CommitAuditedClaim instead.
type SoftFlushCommitter interface {
	CommitSoftFlush(context.Context, AuthorityCommitRequest) (CommitResult, error)
}

// ExactCommitter is the explicit opt-in Store capability for source actor
// paths that need a sealed first-COMMITTED output. Ordinary Commit and
// CommitAuditedClaim must never fingerprint a request, issue a CommitStamp, or
// select a storage revision that returns an exact commit timestamp. Callers
// must request this capability only when they will propagate exact evidence;
// stores retain every ordinary Commit fence and audit on this path.
type ExactCommitter interface {
	CommitExact(context.Context, AuthorityCommitRequest) (CommitResult, error)
}

// AdmissionSnapshotReuseCapable is an optional Store capability used between
// a Participant's outside-mailbox admission read and its serialized mailbox
// turn. Returning true promises that model state and PhysicalUsage cannot
// change without RecordVersion changing, and that the eventual Commit still
// atomically checks Fence, WriterClaim and RecordVersion. Stores without this
// explicit contract retain the conservative second LoadAndClaim.
type AdmissionSnapshotReuseCapable interface {
	AdmissionSnapshotReuseSafe() bool
}

// PhysicalCapabilities is the immutable read-only limit profile enforced by
// one concrete ActorAuthorityStore. Actor runtimes may use it for conservative
// pre-admission, but Store.Commit remains the final atomic capacity boundary.
type PhysicalCapabilities struct {
	MaxModelFields        int
	MaxModelBytes         int
	MaxReceiptItems       int
	MaxReceiptBytes       int
	MaxSingleReceiptBytes int
	MaxCommitBytes        int
}

func (c PhysicalCapabilities) Validate() error {
	if c.MaxModelFields <= 0 || c.MaxModelBytes <= 0 || c.MaxReceiptItems <= 0 ||
		c.MaxReceiptBytes <= 0 || c.MaxSingleReceiptBytes <= 0 || c.MaxCommitBytes <= 0 {
		return fmt.Errorf("%w: incomplete physical capabilities", ErrInvalidRequest)
	}
	return nil
}

// PhysicalCapabilitiesProvider is optional on the frozen authority interface:
// ordinary actor users remain source compatible, while runtimes that perform
// physical pre-admission must obtain limits from their actual Store instance.
type PhysicalCapabilitiesProvider interface {
	PhysicalCapabilities() (PhysicalCapabilities, error)
}

// PhysicalCommitUsage is the concrete Store's physical charge for one new
// Authority commit. ReceiptItems/ReceiptBytes are deltas against the current
// aggregate usage; ModelBytes is the complete replacement model size.
type PhysicalCommitUsage struct {
	ModelBytes         int
	ReceiptItems       int
	ReceiptBytes       int
	SingleReceiptBytes int
	CommitBytes        int
}

func (u PhysicalCommitUsage) Validate() error {
	if u.ModelBytes <= 0 || u.ReceiptItems <= 0 || u.ReceiptBytes <= 0 ||
		u.SingleReceiptBytes <= 0 || u.CommitBytes <= 0 || u.SingleReceiptBytes != u.ReceiptBytes {
		return fmt.Errorf("%w: incomplete physical commit usage", ErrInvalidRequest)
	}
	return nil
}

// PhysicalCommitUsageProjector is optional on the frozen authority interface.
// Participant runtimes require it from the same concrete Store used by Commit
// so nested model bytes are never mistaken for outer Authority receipt usage.
// Implementations must be pure and must use the same accounting as Commit.
type PhysicalCommitUsageProjector interface {
	ProjectPhysicalCommit(AuthorityCommitRequest) (PhysicalCommitUsage, error)
}

type Limits struct {
	MaxModelFields          int
	MaxModelBytes           int
	MaxReceipts             int
	MaxReceiptBytes         int
	MaxSingleReceiptBytes   int
	MaxResolutionReceipts   int
	MaxOrders               int
	MaxSchedules            int
	MaxSchedulePayloadBytes int
	MaxPendingWorkBytes     int
	MaxOutbox               int
	MaxCommitBytes          int
}

const (
	maxAttachedSchedulesPerCommit = 16
	maxSchedulePayloadHard        = 16 << 10
)

func DefaultLimits() Limits {
	return Limits{
		MaxModelFields:          64,
		MaxModelBytes:           512 << 10,
		MaxReceipts:             4096,
		MaxReceiptBytes:         4096 * 1024,
		MaxSingleReceiptBytes:   1024,
		MaxResolutionReceipts:   512,
		MaxOrders:               1024,
		MaxSchedules:            1024,
		MaxSchedulePayloadBytes: 16 << 10,
		MaxPendingWorkBytes:     64 << 20,
		MaxOutbox:               1024,
		MaxCommitBytes:          1 << 20,
	}
}

// NormalizeLimits fills physical limit fields added after the original Limits
// shape. Existing callers that specify earlier fields by name keep their
// values; the new limits can be tightened explicitly.
func NormalizeLimits(limits Limits) Limits {
	if limits == (Limits{}) {
		return DefaultLimits()
	}
	defaults := DefaultLimits()
	if limits.MaxSingleReceiptBytes <= 0 {
		limits.MaxSingleReceiptBytes = defaults.MaxSingleReceiptBytes
	}
	if limits.MaxReceiptBytes <= 0 && limits.MaxReceipts > 0 {
		limits.MaxReceiptBytes = limits.MaxReceipts * limits.MaxSingleReceiptBytes
	}
	if limits.MaxSchedules <= 0 {
		limits.MaxSchedules = defaults.MaxSchedules
	}
	if limits.MaxSchedulePayloadBytes <= 0 {
		limits.MaxSchedulePayloadBytes = defaults.MaxSchedulePayloadBytes
	}
	if limits.MaxSchedulePayloadBytes > maxSchedulePayloadHard {
		limits.MaxSchedulePayloadBytes = maxSchedulePayloadHard
	}
	if limits.MaxPendingWorkBytes <= 0 {
		limits.MaxPendingWorkBytes = defaults.MaxPendingWorkBytes
	}
	return limits
}

// AuthorityCommitSerializedBytes is the bounded logical serialization size
// enforced by both in-memory and Redis Authority stores before mutation.
func AuthorityCommitSerializedBytes(req AuthorityCommitRequest) int {
	// Receipt and operation_result are distinct fields of the logical commit
	// request even though they must contain the same terminal outcome.
	total := AuthorityReceiptSerializedBytes(req.Receipt)
	for name, value := range req.CandidateState {
		total += len(name) + len(value)
	}
	if mutation := req.ModelMutation; mutation != nil {
		for name, value := range mutation.ModelUpserts {
			total += len(name) + len(value)
		}
		for _, name := range mutation.ModelDeletes {
			total += len(name)
		}
		for _, name := range mutation.ResultingManifest {
			total += len(name)
		}
		total += 2 + 8 + len(mutation.ResultingBusinessStateDigest)
	}
	total += len(req.OperationResult.Payload)
	for _, order := range req.AttachedOrders {
		total += len(order.OrderID) + len(order.OrderType) + len(order.Payload) + len(order.PayloadHash) + len(order.OperationID)
	}
	for _, schedule := range req.AttachedSchedules {
		total += AttachedScheduleSerializedBytes(schedule)
	}
	for _, item := range req.PendingOutbox {
		total += len(item.OutboxID) + len(item.EventType) + len(item.Payload)
	}
	return total
}

// AuthorityReceiptSerializedBytes is the deterministic logical envelope size
// used by MemoryStore's receipt byte limits. Redis uses its exact LP encoding
// while exposing the same limit dimensions through PhysicalCapabilities.
func AuthorityReceiptSerializedBytes(receipt AuthorityReceipt) int {
	key := receipt.IdempotencyKey
	return len(receipt.ReceiptID) + 32 + 7*4 + 20 + len(key.StableCommandTypeName) +
		len(key.OperationID) + len(key.Root.Kind) + len(key.Root.CanonicalID) +
		len(receipt.Outcome.ErrorCode) + len(receipt.Outcome.Payload)
}

// ModelStateSerializedBytes is the logical byte accounting used by the
// authority capacity contract: every field name and already-serialized value
// is counted exactly once. It does not claim to include Redis hash metadata or
// protocol framing.
func ModelStateSerializedBytes(state ModelState) int {
	total := 0
	for name, value := range state {
		total += len(name) + len(value)
	}
	return total
}

// BuildActorModelMutation constructs deterministic resulting claims for an
// explicit sparse mutation. Store.Commit still revalidates every claim and
// checks the sparse transition against the currently committed snapshot.
func BuildActorModelMutation(intent ModelMutationIntent, layout ModelLayoutVersion, candidate, upserts ModelState, deletes []string) (*ActorModelMutation, error) {
	return buildActorModelMutation(intent, layout, candidate, upserts, deletes, true)
}

// BuildActorModelMutationBorrowed constructs the same exact sparse mutation
// claims without copying upsert byte slices. It is reserved for an immutable
// candidate frozen for one synchronous Store call; neither the map nor its
// byte slices may be changed until that call returns, and the Store must not
// retain them. General business and migration paths should use the defensive
// BuildActorModelMutation API.
func BuildActorModelMutationBorrowed(intent ModelMutationIntent, layout ModelLayoutVersion, candidate, upserts ModelState, deletes []string) (*ActorModelMutation, error) {
	return buildActorModelMutation(intent, layout, candidate, upserts, deletes, false)
}

func buildActorModelMutation(intent ModelMutationIntent, layout ModelLayoutVersion, candidate, upserts ModelState, deletes []string, defensiveCopy bool) (*ActorModelMutation, error) {
	if candidate == nil || upserts == nil ||
		(intent != ModelMutationIntentUpdate && intent != ModelMutationIntentMigrateV1ToV2) ||
		(layout != ModelLayoutV1 && layout != ModelLayoutV2) {
		return nil, fmt.Errorf("%w: actor model mutation", ErrInvalidRequest)
	}
	manifest := make([]string, 0, len(candidate))
	for name := range candidate {
		manifest = append(manifest, name)
	}
	sort.Strings(manifest)
	deleteCopy := append([]string(nil), deletes...)
	sort.Strings(deleteCopy)
	mutationUpserts := upserts
	if defensiveCopy {
		mutationUpserts = cloneModelState(upserts)
	}
	return &ActorModelMutation{
		Intent:                       intent,
		ModelUpserts:                 mutationUpserts,
		ModelDeletes:                 deleteCopy,
		ResultingManifest:            manifest,
		ResultingTotalBytes:          ModelStateSerializedBytes(candidate),
		ResultingBusinessStateDigest: businessStateDigest(candidate),
		ResultingModelLayout:         layout,
	}, nil
}

// NormalizeActorModelMutation validates all caller-supplied resulting claims.
// legacyReplace is true only for the compatible nil-ModelMutation API; stores
// adapt that shape to the same transition engine while restricting it to v1.
func NormalizeActorModelMutation(address ActorAddress, candidate ModelState, mutation *ActorModelMutation) (normalized *ActorModelMutation, legacyReplace bool, err error) {
	return normalizeActorModelMutation(address, candidate, mutation, true)
}

// NormalizeActorModelMutationBorrowed performs the same exact validation as
// NormalizeActorModelMutation without cloning caller-owned mutation fields.
// It is reserved for synchronous Store implementations: candidate and
// mutation must remain immutable until the Store call returns, and the Store
// must not retain the returned maps or slices. General callers should keep
// using NormalizeActorModelMutation's defensive-copy contract.
func NormalizeActorModelMutationBorrowed(address ActorAddress, candidate ModelState, mutation *ActorModelMutation) (normalized *ActorModelMutation, legacyReplace bool, err error) {
	return normalizeActorModelMutation(address, candidate, mutation, false)
}

func normalizeActorModelMutation(address ActorAddress, candidate ModelState, mutation *ActorModelMutation, defensiveCopy bool) (normalized *ActorModelMutation, legacyReplace bool, err error) {
	if candidate == nil {
		return nil, false, fmt.Errorf("%w: candidate state required", ErrInvalidRequest)
	}
	if mutation == nil {
		built, buildErr := BuildActorModelMutation(ModelMutationIntentUpdate, ModelLayoutV1, candidate, candidate, nil)
		return built, true, buildErr
	}
	if mutation.Intent != ModelMutationIntentUpdate && mutation.Intent != ModelMutationIntentMigrateV1ToV2 {
		return nil, false, fmt.Errorf("%w: model mutation intent", ErrInvalidRequest)
	}
	if mutation.ResultingModelLayout != ModelLayoutV1 && mutation.ResultingModelLayout != ModelLayoutV2 {
		return nil, false, fmt.Errorf("%w: resulting model layout", ErrInvalidRequest)
	}
	if mutation.ModelUpserts == nil {
		return nil, false, fmt.Errorf("%w: model upserts required", ErrInvalidRequest)
	}
	if mutation.ResultingTotalBytes != ModelStateSerializedBytes(candidate) ||
		mutation.ResultingBusinessStateDigest != businessStateDigest(candidate) {
		return nil, false, fmt.Errorf("%w: resulting model aggregate", ErrInvalidRequest)
	}
	wantManifest := make([]string, 0, len(candidate))
	for name := range candidate {
		wantManifest = append(wantManifest, name)
	}
	sort.Strings(wantManifest)
	if !equalStrings(mutation.ResultingManifest, wantManifest) || !strictlySorted(mutation.ResultingManifest) {
		return nil, false, fmt.Errorf("%w: resulting manifest", ErrInvalidRequest)
	}
	if !strictlySorted(mutation.ModelDeletes) {
		return nil, false, fmt.Errorf("%w: model deletes", ErrInvalidRequest)
	}
	deleted := make(map[string]struct{}, len(mutation.ModelDeletes))
	for _, name := range mutation.ModelDeletes {
		if name == "" || len(name) > 128 {
			return nil, false, fmt.Errorf("%w: model delete name", ErrInvalidRequest)
		}
		if _, remains := candidate[name]; remains {
			return nil, false, fmt.Errorf("%w: deleted model remains in candidate", ErrInvalidRequest)
		}
		deleted[name] = struct{}{}
	}
	for name, value := range mutation.ModelUpserts {
		candidateValue, ok := candidate[name]
		if !ok || !bytes.Equal(candidateValue, value) {
			return nil, false, fmt.Errorf("%w: model upsert is not exact candidate", ErrInvalidRequest)
		}
		if _, overlap := deleted[name]; overlap {
			return nil, false, fmt.Errorf("%w: model upsert/delete overlap", ErrInvalidRequest)
		}
	}
	if mutation.ResultingModelLayout == ModelLayoutV2 {
		if err := fieldv2.ValidateExactNames(address.Domain, mutation.ResultingManifest); err != nil {
			return nil, false, fmt.Errorf("%w: v2 schema: %v", ErrInvalidRequest, err)
		}
		if err := fieldv2.ValidateExactFields(address.Domain, candidate); err != nil {
			return nil, false, fmt.Errorf("%w: v2 fields: %v", ErrInvalidRequest, err)
		}
	}
	if mutation.Intent == ModelMutationIntentMigrateV1ToV2 {
		_, retainsRecord := candidate["record"]
		if mutation.ResultingModelLayout != ModelLayoutV2 || retainsRecord || !containsSorted(mutation.ModelDeletes, "record") {
			return nil, false, fmt.Errorf("%w: v1-to-v2 migration must delete record", ErrInvalidRequest)
		}
	}
	copyMutation := *mutation
	if defensiveCopy {
		copyMutation.ModelUpserts = cloneModelState(mutation.ModelUpserts)
		copyMutation.ModelDeletes = append([]string(nil), mutation.ModelDeletes...)
		copyMutation.ResultingManifest = append([]string(nil), mutation.ResultingManifest...)
	}
	return &copyMutation, false, nil
}

// ValidateActorModelTransition proves that the sparse mutation produces the
// exact full candidate from the current snapshot and obeys the one-way layout
// state machine. Redundant writes/deletes are rejected.
func ValidateActorModelTransition(current ModelState, currentLayout ModelLayoutVersion, candidate ModelState, mutation *ActorModelMutation, legacyReplace bool) error {
	if current == nil || candidate == nil || mutation == nil {
		return fmt.Errorf("%w: model transition state", ErrInvalidRequest)
	}
	if currentLayout == ModelLayoutUnknown {
		currentLayout = ModelLayoutV1
	}
	if legacyReplace {
		if currentLayout != ModelLayoutV1 || mutation.ResultingModelLayout != ModelLayoutV1 {
			return fmt.Errorf("%w: legacy commit cannot write v2", ErrInvalidRequest)
		}
		return nil
	}
	switch mutation.Intent {
	case ModelMutationIntentUpdate:
		if currentLayout != mutation.ResultingModelLayout {
			return fmt.Errorf("%w: model layout update/revert", ErrInvalidRequest)
		}
	case ModelMutationIntentMigrateV1ToV2:
		if currentLayout != ModelLayoutV1 || mutation.ResultingModelLayout != ModelLayoutV2 {
			return fmt.Errorf("%w: migration layout transition", ErrInvalidRequest)
		}
	default:
		return fmt.Errorf("%w: model mutation intent", ErrInvalidRequest)
	}
	upserts := mutation.ModelUpserts
	deletes := make(map[string]struct{}, len(mutation.ModelDeletes))
	for _, name := range mutation.ModelDeletes {
		if _, exists := current[name]; !exists {
			return fmt.Errorf("%w: delete of missing model %q", ErrInvalidRequest, name)
		}
		deletes[name] = struct{}{}
	}
	for name, currentValue := range current {
		candidateValue, remains := candidate[name]
		_, deleted := deletes[name]
		upsertValue, upserted := upserts[name]
		if deleted {
			if remains || upserted {
				return fmt.Errorf("%w: inconsistent model delete %q", ErrInvalidRequest, name)
			}
			continue
		}
		if !remains {
			return fmt.Errorf("%w: undeclared model delete %q", ErrInvalidRequest, name)
		}
		if upserted {
			if bytes.Equal(currentValue, upsertValue) {
				return fmt.Errorf("%w: redundant model upsert %q", ErrInvalidRequest, name)
			}
		} else if !bytes.Equal(currentValue, candidateValue) {
			return fmt.Errorf("%w: undeclared model upsert %q", ErrInvalidRequest, name)
		}
	}
	for name := range candidate {
		if _, existed := current[name]; !existed {
			if _, upserted := upserts[name]; !upserted {
				return fmt.Errorf("%w: undeclared new model %q", ErrInvalidRequest, name)
			}
		}
	}
	return nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func strictlySorted(values []string) bool {
	for i, value := range values {
		if value == "" || (i > 0 && values[i-1] >= value) {
			return false
		}
	}
	return true
}

func containsSorted(values []string, want string) bool {
	i := sort.SearchStrings(values, want)
	return i < len(values) && values[i] == want
}

// AttachedOrderSerializedBytes is the bounded logical envelope submitted to
// Store.Commit for one order. Redis adds SourceOrder metadata separately; the
// read-only layout inspector measures those persisted hashes directly.
func AttachedOrderSerializedBytes(order AttachedOrder) int {
	return len(order.OrderID) + len(order.OrderType) + len(order.Payload) + len(order.PayloadHash) + len(order.OperationID) + 8 + 8
}

// AttachedScheduleSerializedBytes is the bounded logical envelope charged for
// one mutable source schedule in AuthorityCommitSerializedBytes.
func AttachedScheduleSerializedBytes(schedule AttachedSchedule) int {
	return len(schedule.ScheduleID) + len(schedule.ScheduleType) + len(schedule.Payload) + len(schedule.PayloadHash) + 8 + 8 + 8
}

// NormalizeAttachedSchedules validates and defensively copies schedules before
// they cross either Store implementation. A zero payload hash is filled from
// the payload for compatibility with callers that do not precompute it; a
// non-zero hash must prove the exact payload. A zero subject inherits the
// commit-key subject, matching the existing AttachedOrder fallback.
func NormalizeAttachedSchedules(schedules []AttachedSchedule, fallbackSubject uint64, limits Limits) ([]AttachedSchedule, error) {
	if len(schedules) == 0 {
		return nil, nil
	}
	limits = NormalizeLimits(limits)
	if len(schedules) > limits.MaxSchedules || len(schedules) > maxAttachedSchedulesPerCommit {
		return nil, ErrCapacityExceeded
	}
	seen := make(map[string]struct{}, len(schedules))
	out := make([]AttachedSchedule, len(schedules))
	for i, input := range schedules {
		if input.ScheduleID == "" || workidentity.ValidateStableOpaqueID(input.ScheduleID) != nil || !validScheduleType(input.ScheduleType) {
			return nil, fmt.Errorf("%w: attached schedule identity", ErrInvalidRequest)
		}
		if input.Generation == 0 || input.DueAtMs < 0 {
			return nil, fmt.Errorf("%w: attached schedule generation/due", ErrInvalidRequest)
		}
		if len(input.Payload) > limits.MaxSchedulePayloadBytes || len(input.Payload) > limits.MaxCommitBytes {
			return nil, ErrCapacityExceeded
		}
		key := input.ScheduleType + "\x00" + input.ScheduleID
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("%w: duplicate attached schedule", ErrInvalidRequest)
		}
		seen[key] = struct{}{}
		if input.SubjectPlayerID == 0 {
			input.SubjectPlayerID = fallbackSubject
		}
		if input.SubjectPlayerID == 0 {
			return nil, fmt.Errorf("%w: attached schedule subject", ErrInvalidRequest)
		}
		wantHash := PayloadHash(sha256.Sum256(input.Payload))
		if input.PayloadHash == (PayloadHash{}) {
			input.PayloadHash = wantHash
		} else if input.PayloadHash != wantHash {
			return nil, fmt.Errorf("%w: attached schedule payload hash", ErrInvalidRequest)
		}
		input.Payload = append([]byte(nil), input.Payload...)
		out[i] = input
	}
	return out, nil
}

// AuthorityCommitResponseSerializedBytes returns the deterministic logical
// response envelope size at the storage-independent seam. It deliberately
// excludes Go allocation overhead and Redis protocol framing.
func AuthorityCommitResponseSerializedBytes(result CommitResult) int {
	total := 4 // status
	total += len(result.Snapshot.ActorAddress.Domain) + 8
	total += 8 + len(result.Snapshot.WriterClaimID) + 8 + 1 + len(result.Snapshot.BusinessStateDigest)
	total += ModelStateSerializedBytes(result.Snapshot.ModelFields)
	total += 1 + 4*8 // PhysicalUsage.Trustworthy plus four integer fields
	total += 4 + len(result.Outcome.ErrorCode) + len(result.Outcome.Payload)
	if result.CommitStamp != nil {
		total += 16 // committed record version + authority commit time
	}
	return total
}

func cloneModelState(in ModelState) ModelState {
	if in == nil {
		return nil
	}
	out := make(ModelState, len(in))
	for key, value := range in {
		out[key] = append([]byte(nil), value...)
	}
	return out
}

func cloneOutcome(in StoredOperationOutcome) StoredOperationOutcome {
	in.Payload = append([]byte(nil), in.Payload...)
	return in
}

func cloneSnapshot(in AuthoritySnapshot) AuthoritySnapshot {
	in.ModelFields = cloneModelState(in.ModelFields)
	return in
}

func cloneReceipt(in AuthorityReceipt) AuthorityReceipt {
	in.Outcome = cloneOutcome(in.Outcome)
	return in
}

func cloneOrder(in AttachedOrder) AttachedOrder {
	in.Payload = append([]byte(nil), in.Payload...)
	return in
}

func cloneSchedule(in AttachedSchedule) AttachedSchedule {
	in.Payload = append([]byte(nil), in.Payload...)
	return in
}

func cloneOutbox(in PendingOutbox) PendingOutbox {
	in.Payload = append([]byte(nil), in.Payload...)
	return in
}

func businessStateDigest(state ModelState) PayloadHash {
	keys := make([]string, 0, len(state))
	for key := range state {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	h := sha256.New()
	var size [8]byte
	for _, key := range keys {
		binary.BigEndian.PutUint32(size[:4], uint32(len(key)))
		_, _ = h.Write(size[:4])
		_, _ = h.Write([]byte(key))
		value := state[key]
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = h.Write(size[:])
		_, _ = h.Write(value)
	}
	var out PayloadHash
	copy(out[:], h.Sum(nil))
	return out
}

func validateAddress(address ActorAddress) error {
	if address.ActorID == 0 || (address.Domain != actorrouting.DomainPlayer && address.Domain != actorrouting.DomainFarm) {
		return fmt.Errorf("%w: actor address", ErrInvalidRequest)
	}
	return nil
}

func validateStableInternalID(name, value string) error {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 256 {
		return fmt.Errorf("%w: %s", ErrInvalidRequest, name)
	}
	return nil
}

func validScheduleType(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}

func validateOutcome(outcome StoredOperationOutcome) error {
	switch outcome.Status {
	case TerminalSucceeded, TerminalRejected, TerminalNoChange:
		return nil
	default:
		return fmt.Errorf("%w: operation result is not terminal", ErrInvalidRequest)
	}
}

func validateModelState(state ModelState, limits Limits) error {
	if state == nil {
		return fmt.Errorf("%w: candidate state required", ErrInvalidRequest)
	}
	if len(state) > limits.MaxModelFields {
		return ErrCapacityExceeded
	}
	total := 0
	for key, value := range state {
		if key == "" || len(key) > 128 {
			return fmt.Errorf("%w: model field name", ErrInvalidRequest)
		}
		total += len(key) + len(value)
		if total > limits.MaxModelBytes {
			return ErrCapacityExceeded
		}
	}
	return nil
}

func trustedNow(now func() time.Time) time.Time {
	if now == nil {
		return time.Now()
	}
	return now()
}
