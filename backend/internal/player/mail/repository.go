package mail

import (
	"context"
	"errors"
)

// Contract-violation errors fail closed as Go errors and are never mapped
// to client-visible business statuses (31 §6.7).
var (
	ErrContractViolation = errors.New("mail: store contract violation")
	ErrInvalidRequest    = errors.New("mail: invalid repository request")
)

// InsertStatus is the typed outcome of InsertMailIfAbsentV1 (35A §4.1).
type InsertStatus string

const (
	InsertInserted             InsertStatus = "INSERTED"
	InsertAlreadyDelivered     InsertStatus = "ALREADY_DELIVERED"
	InsertDeliveryConflict     InsertStatus = "DELIVERY_ID_CONFLICT"
	InsertMailboxFullProtected InsertStatus = "MAILBOX_FULL_PROTECTED"
	InsertCapacityExceeded     InsertStatus = "CAPACITY_EXCEEDED"
	InsertNotVisibleYet        InsertStatus = "NOT_VISIBLE_YET"
	InsertUnavailableNotSent   InsertStatus = "UNAVAILABLE_NOT_SENT"
	InsertTimeoutUnknown       InsertStatus = "TIMEOUT_UNKNOWN"
)

// InsertResult carries the stable delivery outcome. On InsertAlreadyDelivered
// the original mail id is returned; on InsertTimeoutUnknown the caller must
// re-enter with the identical delivery identity (35A PM5/§5).
type InsertResult struct {
	Status  InsertStatus
	MailID  string
	Summary MailboxSummary
}

// MutationStatus is the typed outcome of MarkMailReadV1/DeleteMailV1.
type MutationStatus string

const (
	MutationReadMarked         MutationStatus = "READ_MARKED"
	MutationDeleted            MutationStatus = "DELETED"
	MutationNoChange           MutationStatus = "NO_CHANGE"
	MutationMailNotFound       MutationStatus = "MAIL_NOT_FOUND"
	MutationRejectUnclaimed    MutationStatus = "REJECT_UNCLAIMED"
	MutationUnavailableNotSent MutationStatus = "UNAVAILABLE_NOT_SENT"
	MutationTimeoutUnknown     MutationStatus = "TIMEOUT_UNKNOWN"
)

// MutationResult is the stored stable result of one mutation identity.
type MutationResult struct {
	Status      MutationStatus
	Summary     MailboxSummary
	AppliedAtMs int64
}

// DeleteEligibility is decided by the repository caller before invoking the
// delete script (35A §4.3). The script only accepts these server-constructed
// values; clients can never submit them.
type DeleteEligibility string

const (
	// EligibilityAllow covers: no attachment, claim_end passed, or the
	// claimed projection already shows claimed.
	EligibilityAllow DeleteEligibility = "ALLOW"
	// EligibilityAllowWithClaimProof covers: the PlayerAuthority
	// MailClaimState was precisely read and a claim receipt exists; the
	// proof binds the entry's claim_material_hash.
	EligibilityAllowWithClaimProof DeleteEligibility = "ALLOW_WITH_CLAIM_PROOF"
)

// ProjectionStatus is the typed outcome of ApplyMailClaimProjectionV1.
type ProjectionStatus string

const (
	ProjectionApplied             ProjectionStatus = "APPLIED"
	ProjectionNoChange            ProjectionStatus = "NO_CHANGE"
	ProjectionTargetAlreadyPurged ProjectionStatus = "TARGET_ALREADY_PURGED"
	ProjectionHashMismatch        ProjectionStatus = "CLAIM_MATERIAL_HASH_MISMATCH"
	ProjectionUnavailableNotSent  ProjectionStatus = "UNAVAILABLE_NOT_SENT"
	ProjectionTimeoutUnknown      ProjectionStatus = "TIMEOUT_UNKNOWN"
)

// ClaimProjectionOrder mirrors the contract-35 MailClaimProjectionOrder
// fields consumed by ApplyMailClaimProjectionV1 (35A §4.4). The projection
// is REBUILDABLE_PROJECTION: it can never grant assets twice and never
// resurrects a deleted mail.
type ClaimProjectionOrder struct {
	RecipientPlayerID uint64
	WorkID            string
	WorkVersion       uint64
	MailID            string
	ClaimReceiptRef   string
	ClaimMaterialHash string
	ClaimedAtMs       int64
}

// ProjectionResult is the stored work receipt outcome.
type ProjectionResult struct {
	Status  ProjectionStatus
	Summary MailboxSummary
}

// ExpireStatus is the typed outcome of ExpireMailVisibilityV1 (35A §4.5).
type ExpireStatus string

const (
	ExpireApplied             ExpireStatus = "EXPIRED"
	ExpireNoChange            ExpireStatus = "NO_CHANGE"
	ExpireNotDue              ExpireStatus = "NOT_DUE"
	ExpireStaleRef            ExpireStatus = "STALE_REF"
	ExpireTargetAlreadyPurged ExpireStatus = "TARGET_ALREADY_PURGED"
	ExpireUnavailableNotSent  ExpireStatus = "UNAVAILABLE_NOT_SENT"
	ExpireTimeoutUnknown      ExpireStatus = "TIMEOUT_UNKNOWN"
)

// ExpireResult reports the visibility-expiry outcome. CurrentStateVersion
// lets the worker re-register a fresh DueRef on STALE_REF.
type ExpireResult struct {
	Status              ExpireStatus
	Summary             MailboxSummary
	CurrentStateVersion uint64
	ExpireAtMs          int64
}

// GcStatus is the typed outcome of GcMailV1 (35A §4.6).
type GcStatus string

const (
	GcPurged              GcStatus = "PURGED"
	GcNotDue              GcStatus = "NOT_DUE"
	GcStaleRef            GcStatus = "STALE_REF"
	GcTargetAlreadyPurged GcStatus = "TARGET_ALREADY_PURGED"
	GcReconcileRequired   GcStatus = "RECONCILE_REQUIRED"
	GcUnavailableNotSent  GcStatus = "UNAVAILABLE_NOT_SENT"
	GcTimeoutUnknown      GcStatus = "TIMEOUT_UNKNOWN"
)

// GcResult reports a physical purge. On STALE_REF the current versions let
// the worker re-register a fresh ref; PurgeAtMs carries the store-side
// purge score for NOT_DUE re-registration.
type GcResult struct {
	Status              GcStatus
	FreedBytes          int64
	PurgeAtMs           int64
	CurrentMailVersion  uint64
	CurrentStateVersion uint64
}

// MailEntryRepository is the write port owned by PlayerMailDomain
// (35A §10). Implementations must preserve: Entry+State+Dedup+Index+
// Summary+DueInfo in one atomic step, stable identity re-entry, and typed
// Unknown outcomes.
type MailEntryRepository interface {
	InsertMailIfAbsent(ctx context.Context, entry MailEntry) (InsertResult, error)
	MarkMailRead(ctx context.Context, recipientPlayerID uint64, mailID, mutationID string) (MutationResult, error)
	// DeleteMail only accepts a server-constructed DeleteAuthorization
	// (35A §4.3); the proof is structurally validated before the script
	// runs and defensively re-verified inside the script.
	DeleteMail(ctx context.Context, recipientPlayerID uint64, mailID, mutationID string, auth DeleteAuthorization) (MutationResult, error)
	ApplyMailClaimProjection(ctx context.Context, order ClaimProjectionOrder) (ProjectionResult, error)
}

// MailReadRepository is the bounded read port. All reads address explicit
// fields; HGETALL of a mailbox, KEYS and full scans are forbidden (35A PM7).
type MailReadRepository interface {
	// GetEntry strongly reads one mail for its recipient. Recipient scoping
	// is physical: the mailbox hash key derives from the recipient, so a
	// mail id can never cross recipients.
	GetEntry(ctx context.Context, recipientPlayerID uint64, mailID string) (MailEntry, MailUserState, error)
	// GetSummary reads the atomic counter block only.
	GetSummary(ctx context.Context, recipientPlayerID uint64) (MailboxSummary, error)
	// ListPage returns at most PageSize items after the exclusive cursor,
	// bounded by PageScanItems/PageScanBytes (35A §3).
	ListPage(ctx context.Context, recipientPlayerID uint64, cursor string, nowMs int64) (Page, error)
}

// ErrMailNotFound is returned by GetEntry when the pair (recipient,
// mail_id) has no live entry.
var ErrMailNotFound = errors.New("mail: entry not found")

// DueCandidate is one decoded DueIndex member plus its score.
type DueCandidate struct {
	Ref     MailDueRefV1
	DueAtMs int64
	Member  string
}

// ScanLeaseStatus is the outcome of a bucket scan lease claim.
type ScanLeaseStatus string

const (
	ScanLeaseAcquired           ScanLeaseStatus = "LEASED"
	ScanLeaseHeldByOther        ScanLeaseStatus = "HELD_BY_OTHER"
	ScanLeaseUnavailableNotSent ScanLeaseStatus = "UNAVAILABLE_NOT_SENT"
	ScanLeaseTimeoutUnknown     ScanLeaseStatus = "TIMEOUT_UNKNOWN"
)

// ScanLeaseResult mirrors the Work Lease contract (30-14): the store
// decides with Redis TIME and returns the accepted TTL; the worker builds
// its conservative local deadline from it.
type ScanLeaseResult struct {
	Status        ScanLeaseStatus
	AcceptedTTLMs int64
	LeaseEpoch    uint64
}

// MailMaintenanceRepository is the bounded worker port (35A §7). Workers
// only scan known buckets with LIMIT; global scans are impossible through
// this port. Reconcile may only repair indexes/summaries/projections.
type MailMaintenanceRepository interface {
	ClaimScanLease(ctx context.Context, bucketID uint32, ownerToken string, requestedTTLMs int64) (ScanLeaseResult, error)
	ListExpireDue(ctx context.Context, bucketID uint32, nowMs int64, limit int) ([]DueCandidate, error)
	ListGcDue(ctx context.Context, bucketID uint32, nowMs int64, limit int) ([]DueCandidate, error)
	ExpireVisibility(ctx context.Context, ref MailDueRefV1) (ExpireResult, error)
	GcMail(ctx context.Context, ref MailDueRefV1) (GcResult, error)
	// RegisterDueRef re-registers a fresh due member after a STALE_REF or
	// NOT_DUE outcome (35A §7.1: stale refs only repair the index, they
	// never delete newer versions). It is a single-key idempotent add.
	RegisterDueRef(ctx context.Context, ref MailDueRefV1, scoreMs int64) error
}
