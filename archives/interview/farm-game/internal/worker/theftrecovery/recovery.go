// Package theftrecovery contains the recovery-only orchestration for the
// FRIEND_PAIR Theft dispatch order.  It deliberately depends on ports for the
// Pair Authority, WorkLease authority and Theft workflow.  No Participant or
// public command is implemented here.
package theftrecovery

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"farmgame/internal/friend"
	"farmgame/internal/identity"
	"farmgame/internal/workidentity"
)

const (
	FamilyTheft                      = "THEFT"
	FriendActionDispatchOrder        = "FRIEND_ACTION_DISPATCH_ORDER"
	ResultPublishOrder               = "RESULT_PUBLISH_ORDER"
	SourcePersistenceBucket          = "SOURCE_PERSISTENCE_BUCKET"
	FriendPairAuthority              = "FRIEND_PAIR"
	ReconcileRequiredState           = "RECONCILE_REQUIRED"
	DefaultClaimLimit                = 32
	DefaultDiscoveryLimit            = 128
	DefaultLeaseTTL                  = 5 * time.Second
	DefaultOperationTimeout          = 3 * time.Second
	DefaultMaxAttempts        uint64 = 8
)

var (
	ErrInvalidConfig      = errors.New("theft recovery: invalid configuration")
	ErrInvalidDispatch    = errors.New("theft recovery: invalid dispatch order")
	ErrMalformedMember    = errors.New("theft recovery: malformed due member")
	ErrNotTheft           = errors.New("theft recovery: dispatch is not Theft")
	ErrLeaseLost          = errors.New("theft recovery: work lease lost")
	ErrReconcileRequired  = errors.New("theft recovery: reconcile required")
	ErrReceiptUnavailable = errors.New("theft recovery: receipt unavailable")
	ErrResultProofMissing = errors.New("theft recovery: result proof missing")
)

// WorkAddress is the complete physical address carried by a Pair source
// order.  It is intentionally independent of actor routing: FRIEND_PAIR is a
// non-Actor source and must never be turned into an actor RouteDomain.
type WorkAddress struct {
	WorkID                   string
	WorkType                 string
	PlacementKind            string
	BucketVersion            string
	BucketID                 uint32
	SourceAuthorityKind      string
	CanonicalSourceAuthority string
	SourceLayoutVersion      string
}

func (a WorkAddress) String() string {
	return a.WorkType + ":" + a.WorkID + "@" + a.SourceAuthorityKind + ":" + a.CanonicalSourceAuthority
}

// DispatchCandidate is a member returned by one bounded DueIndex page.  The
// worker never deletes a candidate.  A malformed or foreign member is simply
// consumed by the bounded page/cursor and cannot cause deletion of another
// work item.
type DispatchCandidate struct {
	BucketID uint32
	Member   string
	ScoreMs  int64
}

type DiscoveryCursor struct {
	Token           string
	HeadFingerprint string
}

type DiscoveryRequest struct {
	Limit  int
	Cursor DiscoveryCursor
}

type DiscoveryPage struct {
	Candidates []DispatchCandidate
	Next       DiscoveryCursor
	Exhausted  bool
}

// PairDispatchRepository is the read/discovery seam for the Pair Authority.
// Implementations must use the Pair bucket DueIndex and a bounded page; the
// worker never falls back to Redis SCAN or an in-memory queue.
type PairDispatchRepository interface {
	Discover(ctx context.Context, request DiscoveryRequest) (DiscoveryPage, error)
	ActionScope(ctx context.Context, candidate DispatchCandidate) (friend.ActionScope, error)
	LoadDispatch(ctx context.Context, candidate DispatchCandidate) (DispatchOrder, error)
}

// PairRepository is retained as a short alias for test seams and composition
// roots that name the dependency after its authority rather than its view.
type PairRepository = PairDispatchRepository

type PairDueObserver interface {
	ObserveDue(context.Context, int) (PairDueObservation, error)
}

// PairResultOrderProcessor owns RESULT_PUBLISH_ORDER. It returns completed
// only after the terminal projection and source-order terminalization are both
// durable. A processing Work leaves the member due for a later round.
type PairResultOrderProcessor interface {
	ProcessResultOrder(context.Context, DispatchCandidate) (completed bool, err error)
}

// PairCareOrderProcessor is the correctly scoped owner for Pair CARE dispatch
// and result orders. The shared DueIndex router invokes it only after the
// immutable ActionScope has been validated as CARE.
type PairCareOrderProcessor interface {
	ProcessCareOrder(context.Context, DispatchCandidate) (completed bool, err error)
}

type AdmissionProof struct {
	ReceiptID    string
	PayloadHash  []byte
	PermitHash   []byte
	AcceptedAtMs int64
}

// DispatchOrder is the immutable, typed projection of a persisted
// FriendActionDispatchOrder.  Retry/lease fields are loaded from the same
// physical WorkRecord and are never trusted from a discovery member alone.
type DispatchOrder struct {
	Work                 WorkAddress
	WorkRef              string
	WorkVersion          uint64
	Payload              []byte
	PayloadHash          []byte // hash of the immutable typed order payload
	CanonicalPayloadHash []byte // original command/canonical payload hash
	TargetBindingHash    []byte
	PairID               string
	OperationID          string
	VisitorPlayerID      uint64
	OwnerPlayerID        uint64
	TargetFarmID         uint64
	PlotID               uint32
	CropCycleID          string
	Permit               friend.ActionPermitSnapshot
	Admission            AdmissionProof

	WorkflowID   string
	SettlementID string
	AttemptID    string

	NextDueAtMs     int64
	RetryCount      uint64
	DeadlineAtMs    int64
	LastErrorClass  string
	SourceProofHash []byte
	ResultProofHash []byte
}

type TheftIdentity struct {
	WorkflowID   string
	SettlementID string
	AttemptID    string
	OperationID  string
	PairID       string
}

// DeriveTheftIdentity is the only identity derivation used by the worker.
// Changing operation, lease, retry or worker boot never changes these IDs.
func DeriveTheftIdentity(order DispatchOrder) (TheftIdentity, error) {
	if strings.TrimSpace(order.PairID) == "" || order.VisitorPlayerID == 0 ||
		order.TargetFarmID == 0 || strings.TrimSpace(order.OperationID) == "" ||
		strings.TrimSpace(order.CropCycleID) == "" {
		return TheftIdentity{}, fmt.Errorf("%w: identity inputs", ErrInvalidDispatch)
	}
	wantPair, err := identity.CanonicalFriendRelationID(order.VisitorPlayerID, order.OwnerPlayerID)
	if err != nil || wantPair != order.PairID {
		return TheftIdentity{}, fmt.Errorf("%w: canonical Pair identity", ErrInvalidDispatch)
	}
	workflowID, err := workidentity.StableOpaqueID("farm-workflow-id-v2",
		strconv.FormatUint(order.VisitorPlayerID, 10), FamilyTheft, order.OperationID,
		FriendPairAuthority, order.PairID)
	if err != nil {
		return TheftIdentity{}, err
	}
	settlementID, err := workidentity.TheftSettlementID(workflowID)
	if err != nil {
		return TheftIdentity{}, err
	}
	attemptID, err := workidentity.TheftAttemptID(order.TargetFarmID, order.CropCycleID, order.VisitorPlayerID)
	if err != nil {
		return TheftIdentity{}, err
	}
	return TheftIdentity{WorkflowID: workflowID, SettlementID: settlementID, AttemptID: attemptID,
		OperationID: order.OperationID, PairID: order.PairID}, nil
}

// NormalizeDispatchOrder validates a Theft order and fills its deterministic
// workflow identities. Scope-independent Pair validation is shared with the
// CARE owner through NormalizePairActionDispatchOrder.
func NormalizeDispatchOrder(order DispatchOrder) (DispatchOrder, error) {
	order, err := NormalizePairActionDispatchOrder(order, friend.ActionScopeTheft)
	if err != nil {
		return DispatchOrder{}, err
	}
	if order.PlotID == 0 || workidentity.ValidateStableOpaqueID(order.CropCycleID) != nil {
		return DispatchOrder{}, fmt.Errorf("%w: theft target binding", ErrInvalidDispatch)
	}
	ids, err := DeriveTheftIdentity(order)
	if err != nil {
		return DispatchOrder{}, err
	}
	if order.WorkflowID != "" && order.WorkflowID != ids.WorkflowID ||
		order.SettlementID != "" && order.SettlementID != ids.SettlementID ||
		order.AttemptID != "" && order.AttemptID != ids.AttemptID {
		return DispatchOrder{}, fmt.Errorf("%w: deterministic identity drift", ErrInvalidDispatch)
	}
	order.WorkflowID, order.SettlementID, order.AttemptID = ids.WorkflowID, ids.SettlementID, ids.AttemptID
	return order, nil
}

// NormalizePairActionDispatchOrder validates the immutable Pair source tuple
// without applying family-specific target semantics. It is the common fence
// used before either the CARE or THEFT handler may claim the source order.
func NormalizePairActionDispatchOrder(order DispatchOrder, expectedScope friend.ActionScope) (DispatchOrder, error) {
	if expectedScope != friend.ActionScopeCare && expectedScope != friend.ActionScopeTheft {
		return DispatchOrder{}, fmt.Errorf("%w: unsupported action scope", ErrInvalidDispatch)
	}
	if err := validateDispatchOrderShape(order); err != nil {
		return DispatchOrder{}, err
	}
	if order.WorkRef == "" {
		ref, err := workidentity.EncodeWorkRefV1(order.Work.WorkType, order.Work.WorkID)
		if err != nil {
			return DispatchOrder{}, fmt.Errorf("%w: work ref: %v", ErrInvalidDispatch, err)
		}
		order.WorkRef = ref
	}
	ref, err := workidentity.DecodeWorkRefV1(order.WorkRef)
	if err != nil || ref.WorkType != order.Work.WorkType || ref.WorkID != order.Work.WorkID {
		return DispatchOrder{}, fmt.Errorf("%w: work ref binding", ErrInvalidDispatch)
	}
	if err := validatePayloadHash(order.Payload, order.PayloadHash); err != nil {
		return DispatchOrder{}, fmt.Errorf("%w: typed payload hash: %v", ErrInvalidDispatch, err)
	}
	wantPair, err := identity.CanonicalFriendRelationID(order.VisitorPlayerID, order.OwnerPlayerID)
	if err != nil || order.PairID != wantPair || order.PairID != order.Work.CanonicalSourceAuthority {
		return DispatchOrder{}, fmt.Errorf("%w: source pair binding", ErrInvalidDispatch)
	}
	if order.Permit.RelationID != order.PairID || order.Permit.OperationID != order.OperationID ||
		order.Permit.VisitorPlayerID != order.VisitorPlayerID || order.Permit.OwnerPlayerID != order.OwnerPlayerID ||
		order.Permit.TargetFarmID != order.TargetFarmID || order.Permit.Scope != expectedScope {
		return DispatchOrder{}, fmt.Errorf("%w: permit/order binding", ErrInvalidDispatch)
	}
	if !bytesEqual(order.Permit.PayloadHash, order.CanonicalPayloadHash) ||
		!bytesEqual(order.Permit.TargetBindingHash, order.TargetBindingHash) {
		return DispatchOrder{}, fmt.Errorf("%w: permit payload/target hash", ErrInvalidDispatch)
	}
	if err := friend.VerifyAdmittedPermit(order.Permit); err != nil {
		return DispatchOrder{}, fmt.Errorf("%w: permit admission: %v", ErrInvalidDispatch, err)
	}
	if order.Admission.ReceiptID == "" || !bytesEqual(order.Admission.PayloadHash, order.CanonicalPayloadHash) {
		return DispatchOrder{}, fmt.Errorf("%w: admission proof", ErrInvalidDispatch)
	}
	if !bytesEqual(order.Admission.PermitHash, PermitHash(order.Permit)) {
		return DispatchOrder{}, fmt.Errorf("%w: admission permit hash", ErrInvalidDispatch)
	}
	if order.Admission.AcceptedAtMs != 0 && order.Admission.AcceptedAtMs != order.Permit.ExecutionAcceptedAtAuthorityMs {
		return DispatchOrder{}, fmt.Errorf("%w: admission accepted time", ErrInvalidDispatch)
	}
	if order.Permit.TargetFarmID != order.OwnerPlayerID || order.OwnerPlayerID == order.VisitorPlayerID {
		return DispatchOrder{}, fmt.Errorf("%w: action target binding", ErrInvalidDispatch)
	}
	if order.DeadlineAtMs <= 0 || order.NextDueAtMs < 0 || order.NextDueAtMs > order.DeadlineAtMs {
		return DispatchOrder{}, fmt.Errorf("%w: deadline/due", ErrInvalidDispatch)
	}
	if order.DeadlineAtMs <= order.Permit.ExpireAtAuthorityMs {
		return DispatchOrder{}, fmt.Errorf("%w: workflow deadline is not independent from permit expiry", ErrInvalidDispatch)
	}
	if len(order.SourceProofHash) != 0 && !bytesEqual(order.SourceProofHash, SourceProofHash(order)) {
		return DispatchOrder{}, fmt.Errorf("%w: source proof hash", ErrInvalidDispatch)
	}
	return order, nil
}

func validateDispatchOrderShape(order DispatchOrder) error {
	if order.Work.WorkType != FriendActionDispatchOrder || order.Work.PlacementKind != SourcePersistenceBucket ||
		order.Work.BucketVersion == "" || order.Work.SourceAuthorityKind != FriendPairAuthority ||
		workidentity.ValidateStableOpaqueID(order.Work.CanonicalSourceAuthority) != nil || order.Work.SourceLayoutVersion == "" ||
		workidentity.ValidateStableOpaqueID(order.Work.WorkID) != nil ||
		workidentity.ValidateStableOpaqueID(order.PairID) != nil || order.PairID != order.Work.CanonicalSourceAuthority {
		return fmt.Errorf("%w: work address", ErrInvalidDispatch)
	}
	if order.WorkRef != "" {
		if _, err := workidentity.DecodeWorkRefV1(order.WorkRef); err != nil {
			return fmt.Errorf("%w: malformed work ref", ErrInvalidDispatch)
		}
	}
	if len(order.Payload) == 0 || len(order.PayloadHash) != sha256.Size ||
		len(order.CanonicalPayloadHash) != sha256.Size || len(order.TargetBindingHash) != sha256.Size {
		return fmt.Errorf("%w: payload hashes", ErrInvalidDispatch)
	}
	return nil
}

func validatePayloadHash(payload, want []byte) error {
	got := sha256.Sum256(payload)
	if len(want) != sha256.Size || !bytesEqual(got[:], want) {
		return ErrInvalidDispatch
	}
	return nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}

// PermitHash is the immutable hash binding used by an admission receipt and
// its dispatch order.  It is deliberately independent of HMAC secrets.
func PermitHash(permit friend.ActionPermitSnapshot) []byte {
	h := sha256.New()
	writeLP := func(value string) {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(value))
	}
	writeLP(permit.PermitID)
	writeLP(permit.RelationID)
	for _, value := range []uint64{permit.RelationEpoch, permit.VisitorPlayerID, permit.OwnerPlayerID, permit.TargetFarmID} {
		var number [8]byte
		binary.BigEndian.PutUint64(number[:], value)
		_, _ = h.Write(number[:])
	}
	writeLP(permit.OperationID)
	writeLP(strconv.FormatUint(uint64(permit.Scope), 10))
	for _, value := range [][]byte{permit.PayloadHash, permit.TargetBindingHash} {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		_, _ = h.Write(length[:])
		_, _ = h.Write(value)
	}
	for _, value := range []int64{permit.IssuedAtAuthorityMs, permit.ExpireAtAuthorityMs, permit.ExecutionAcceptedAtAuthorityMs} {
		var number [8]byte
		binary.BigEndian.PutUint64(number[:], uint64(value))
		_, _ = h.Write(number[:])
	}
	return h.Sum(nil)
}

func SourceProofHash(order DispatchOrder) []byte {
	h := sha256.New()
	for _, value := range []string{order.PairID, order.WorkRef, order.Work.WorkType, order.Work.BucketVersion,
		strconv.FormatUint(uint64(order.Work.BucketID), 10), order.OperationID} {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(value))
	}
	for _, value := range [][]byte{order.PayloadHash, order.CanonicalPayloadHash, order.TargetBindingHash, PermitHash(order.Permit)} {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		_, _ = h.Write(length[:])
		_, _ = h.Write(value)
	}
	var deadline [8]byte
	binary.BigEndian.PutUint64(deadline[:], uint64(order.DeadlineAtMs))
	_, _ = h.Write(deadline[:])
	return h.Sum(nil)
}

type LeaseClaimStatus uint8

const (
	LeaseClaimUnknown LeaseClaimStatus = iota
	LeaseClaimed
	LeaseHeld
	LeaseNotDue
	LeaseNotFound
	LeaseNotClaimable
	LeaseUnavailableNotSent
	LeaseTimeoutUnknown
)

type LeaseClaim struct {
	Status          LeaseClaimStatus
	WorkVersion     uint64
	RetryCount      uint64
	DeadlineAtMs    int64
	NextDueAtMs     int64
	LastErrorClass  string
	HolderID        string
	BootID          string
	LeaseEpoch      uint64
	LeaseExpireAtMs int64
	AcceptedTTL     time.Duration
	PayloadHash     []byte
	SourceProofHash []byte
	ResultProofHash []byte
}

type LeaseRenewStatus uint8

const (
	LeaseRenewUnknown LeaseRenewStatus = iota
	LeaseRenewed
	LeaseRenewFenceMismatch
	LeaseRenewExpired
	LeaseRenewNotFound
	LeaseRenewUnavailableNotSent
	LeaseRenewTimeoutUnknown
)

type LeaseRenewResult struct {
	Status          LeaseRenewStatus
	LeaseExpireAtMs int64
	AcceptedTTL     time.Duration
	WorkVersion     uint64
}

type LeaseCompleteStatus uint8

const (
	LeaseCompleteUnknown LeaseCompleteStatus = iota
	LeaseCompleted
	LeaseAlreadyCompleted
	LeaseCompleteFenceMismatch
	LeaseCompleteStaleVersion
	LeaseCompleteNotFound
	LeaseCompleteUnavailableNotSent
	LeaseCompleteTimeoutUnknown
)

type LeaseCompleteResult struct{ Status LeaseCompleteStatus }

type LeaseRescheduleStatus uint8

const (
	LeaseRescheduleUnknown LeaseRescheduleStatus = iota
	LeaseRescheduled
	LeaseReconcileRequired
	LeaseRescheduleFenceMismatch
	LeaseRescheduleStaleVersion
	LeaseRescheduleExpired
	LeaseRescheduleNotFound
	LeaseRescheduleUnavailableNotSent
	LeaseRescheduleTimeoutUnknown
)

type LeaseRescheduleResult struct{ Status LeaseRescheduleStatus }

type ClaimRequest struct {
	Order        DispatchOrder
	HolderID     string
	BootID       string
	LeaseToken   string
	RequestedTTL time.Duration
}

type LeaseFence struct {
	HolderID    string
	BootID      string
	LeaseToken  string
	LeaseEpoch  uint64
	WorkVersion uint64
}

// VerifyRecoveryLeaseRequest is the dedicated FRIEND_PAIR DECIDE_THEFT
// admission proof. Order identifies the Pair source record; Fence is the exact
// worker claim carried over WorkerRecoveryIngress.
type VerifyRecoveryLeaseRequest struct {
	Order DispatchOrder
	Fence LeaseFence
}

// RecoveryLeaseVerification contains only Redis-adjudicated evidence. It is
// returned for tests/diagnostics; callers must treat a nil error as the proof.
type RecoveryLeaseVerification struct {
	WorkVersion     uint64
	LeaseEpoch      uint64
	LeaseExpireAtMs int64
	DueIndexScoreMs int64
	TrustedNowMs    int64
}

type RenewRequest struct {
	Order           DispatchOrder
	Fence           LeaseFence
	RequestedTTL    time.Duration
	PayloadHash     []byte
	SourceProofHash []byte
}

type CompletionProof struct {
	PayloadHash     []byte
	SourceProofHash []byte
	ResultProofHash []byte
}

type CompleteRequest struct {
	Order DispatchOrder
	Fence LeaseFence
	Proof CompletionProof
}

type RescheduleRequest struct {
	Order         DispatchOrder
	Fence         LeaseFence
	RetryCount    uint64
	DeadlineAtMs  int64
	NextDueAtMs   int64
	ErrorClass    string
	TerminalState string
	Proof         CompletionProof
}

// WorkLeaseRepository is the storage seam for the Redis Authority WorkLease
// primitive.  Production implementations must adjudicate time in Redis TIME;
// the worker only supplies requested TTLs and immutable proofs.
type WorkLeaseRepository interface {
	Claim(ctx context.Context, request ClaimRequest) (LeaseClaim, error)
	Renew(ctx context.Context, request RenewRequest) (LeaseRenewResult, error)
	Complete(ctx context.Context, request CompleteRequest) (LeaseCompleteResult, error)
	Reschedule(ctx context.Context, request RescheduleRequest) (LeaseRescheduleResult, error)
}

// TheftWorkflowAdvancer is intentionally narrow.  It owns CreateIfAbsent and
// continuation of the one deterministic Theft workflow; Direct and Worker
// callers therefore converge on the same Work/Receipt rather than inventing
// another operation.
type TheftWorkflowAdvancer interface {
	CreateIfAbsentOrContinue(ctx context.Context, request AdvanceRequest) (AdvanceResult, error)
	ReceiptFirst(ctx context.Context, request ReceiptRequest) (AdvanceResult, error)
}

type AdvanceRequest struct {
	Identity        TheftIdentity
	Order           DispatchOrder
	Lease           LeaseFence
	CreateIfAbsent  bool
	ReceiptFirst    bool
	PayloadHash     []byte
	SourceProofHash []byte
}

type ReceiptRequest struct {
	Identity        TheftIdentity
	Order           DispatchOrder
	Lease           LeaseFence
	PayloadHash     []byte
	SourceProofHash []byte
}

type AdvanceStatus uint8

const (
	AdvanceUnknown AdvanceStatus = iota
	AdvanceApplied
	AdvanceAlreadyApplied
	AdvanceRejected
	AdvanceNoChange
	AdvanceNotCommitted
	AdvanceReconcileRequired
)

type AdvanceResult struct {
	Status          AdvanceStatus
	ResultProofHash []byte
	ErrorClass      string
	ReceiptID       string
}

type Backoff struct {
	Base time.Duration
	Max  time.Duration
}

func (b Backoff) delay(retry uint64) time.Duration {
	if b.Base <= 0 {
		b.Base = 250 * time.Millisecond
	}
	if b.Max <= 0 {
		b.Max = 30 * time.Second
	}
	delay := b.Base
	for i := uint64(1); i < retry && delay < b.Max; i++ {
		if delay > b.Max/2 {
			delay = b.Max
			break
		}
		delay *= 2
	}
	if delay > b.Max {
		return b.Max
	}
	return delay
}

type Config struct {
	PairRepository     PairDispatchRepository
	Leases             WorkLeaseRepository
	Advancer           TheftWorkflowAdvancer
	ResultProcessor    PairResultOrderProcessor
	CareProcessor      PairCareOrderProcessor
	HolderID           string
	BootID             string
	LeaseToken         string
	ClaimLimit         int
	DiscoveryLimit     int
	LeaseTTL           time.Duration
	OperationTimeout   time.Duration
	MaxAttempts        uint64
	Backoff            Backoff
	Now                func() time.Time
	RenewBeforeAdvance bool
	// DueObservationCacheTTL only controls the expensive readiness/backlog
	// proof. Durable work discovery still runs on every recovery round.
	DueObservationCacheTTL time.Duration
}

type Worker struct {
	config           Config
	mu               sync.Mutex
	cursor           DiscoveryCursor
	dueObservationMu sync.Mutex
	dueObservation   PairDueObservation
	dueObservedAt    time.Time
	dueObservedLimit int
}

type Stats struct {
	Discovered       int
	MalformedMembers int
	NonTheft         int
	NotOwned         int
	InvalidOrders    int
	Claimed          int
	Completed        int
	ResultsCompleted int
	CareCompleted    int
	Retried          int
	Reconciled       int
	LeaseHeld        int
	LeaseLost        int
	StepUnknown      int
	Errors           int
	// SweepExhausted marks the round that reached the end of the finite Pair
	// bucket space. Backoff is safe only after an empty complete sweep.
	SweepExhausted bool
	// LastError is bounded internal worker telemetry. It is deliberately not
	// persisted into Pair/Work or returned by the public operation API.
	LastError         string
	TerminalLatencies []time.Duration
}

func New(config Config) (*Worker, error) {
	if config.PairRepository == nil || config.Leases == nil || config.Advancer == nil ||
		strings.TrimSpace(config.HolderID) == "" || strings.TrimSpace(config.BootID) == "" ||
		strings.TrimSpace(config.LeaseToken) == "" {
		return nil, fmt.Errorf("%w: Pair repository, WorkLease and advancer with holder/boot/token are required", ErrInvalidConfig)
	}
	if config.ClaimLimit <= 0 {
		config.ClaimLimit = DefaultClaimLimit
	}
	if config.DiscoveryLimit <= 0 {
		config.DiscoveryLimit = DefaultDiscoveryLimit
	}
	if config.DiscoveryLimit < config.ClaimLimit {
		config.DiscoveryLimit = config.ClaimLimit * 4
	}
	if config.LeaseTTL <= 0 {
		config.LeaseTTL = DefaultLeaseTTL
	}
	if config.OperationTimeout <= 0 {
		config.OperationTimeout = DefaultOperationTimeout
	}
	if config.MaxAttempts == 0 {
		config.MaxAttempts = DefaultMaxAttempts
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.DueObservationCacheTTL <= 0 {
		config.DueObservationCacheTTL = 10 * time.Second
	}
	// Renewal is part of the recovery fence, not an optional optimization.
	// Keep the field for source compatibility with early test seams, but force
	// the safe behavior for every production composition.
	config.RenewBeforeAdvance = true
	return &Worker{config: config}, nil
}

func (w *Worker) PendingCursor() DiscoveryCursor {
	if w == nil {
		return DiscoveryCursor{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cursor
}

func (w *Worker) ObserveDue(ctx context.Context, limit int) (PairDueObservation, error) {
	if w == nil {
		return PairDueObservation{}, fmt.Errorf("%w: nil worker", ErrInvalidConfig)
	}
	observer, ok := w.config.PairRepository.(PairDueObserver)
	if !ok {
		return PairDueObservation{}, nil
	}
	now := w.config.Now()
	w.dueObservationMu.Lock()
	defer w.dueObservationMu.Unlock()
	if !w.dueObservedAt.IsZero() && w.dueObservedLimit == limit && now.Sub(w.dueObservedAt) < w.config.DueObservationCacheTTL {
		return w.dueObservation, nil
	}
	observation, err := observer.ObserveDue(ctx, limit)
	if err != nil {
		return PairDueObservation{}, err
	}
	w.dueObservation = observation
	w.dueObservedAt = now
	w.dueObservedLimit = limit
	return observation, nil
}

func (w *Worker) RunOnce(ctx context.Context) (Stats, error) {
	if w == nil {
		return Stats{}, fmt.Errorf("%w: nil worker", ErrInvalidConfig)
	}
	request := DiscoveryRequest{Limit: w.config.DiscoveryLimit, Cursor: w.PendingCursor()}
	page, err := w.config.PairRepository.Discover(ctx, request)
	if err != nil {
		return Stats{}, err
	}
	stats := Stats{SweepExhausted: page.Exhausted}
	for _, candidate := range page.Candidates {
		stats.Discovered++
		if err := validateCandidate(candidate); err != nil {
			stats.MalformedMembers++
			continue
		}
		ref, _ := workidentity.DecodeWorkRefV1(candidate.Member)
		if ref.WorkType != FriendActionDispatchOrder && ref.WorkType != ResultPublishOrder {
			stats.NonTheft++
			continue
		}
		scope, scopeErr := w.config.PairRepository.ActionScope(ctx, candidate)
		if errors.Is(scopeErr, ErrNotTheft) {
			stats.NotOwned++
			continue
		}
		if scopeErr != nil {
			stats.InvalidOrders++
			stats.Errors++
			stats.LastError = boundedError(scopeErr)
			continue
		}
		switch scope {
		case friend.ActionScopeCare:
			if w.config.CareProcessor == nil {
				stats.NotOwned++
				continue
			}
			completed, processErr := w.config.CareProcessor.ProcessCareOrder(ctx, candidate)
			if processErr != nil {
				// This is a correctly routed CARE handler failure, not an invalid
				// Theft order. Preserve that distinction in incident telemetry.
				stats.Errors++
				stats.LastError = boundedError(processErr)
				continue
			}
			if completed {
				stats.CareCompleted++
			}
			continue
		case friend.ActionScopeTheft:
			// Continue below to the Theft family handler.
		default:
			stats.InvalidOrders++
			continue
		}
		if ref.WorkType == ResultPublishOrder {
			if w.config.ResultProcessor == nil {
				stats.Errors++
				stats.LastError = "Theft result processor unavailable"
				continue
			}
			completed, processErr := w.config.ResultProcessor.ProcessResultOrder(ctx, candidate)
			if errors.Is(processErr, ErrNotTheft) {
				stats.NotOwned++
				continue
			}
			if processErr != nil {
				stats.InvalidOrders++
				stats.Errors++
				stats.LastError = boundedError(processErr)
				continue
			}
			if completed {
				stats.ResultsCompleted++
			}
			continue
		}
		order, err := w.config.PairRepository.LoadDispatch(ctx, candidate)
		if errors.Is(err, ErrNotTheft) {
			stats.NotOwned++
			continue
		}
		if err != nil {
			stats.InvalidOrders++
			stats.Errors++
			stats.LastError = boundedError(err)
			continue
		}
		if order.Work.WorkID != ref.WorkID || order.Work.WorkType != ref.WorkType ||
			order.Work.BucketID != candidate.BucketID {
			stats.InvalidOrders++
			continue
		}
		if normalized, normalizeErr := NormalizeDispatchOrder(order); normalizeErr != nil {
			stats.InvalidOrders++
			continue
		} else {
			order = normalized
		}
		claim, err := w.config.Leases.Claim(ctx, ClaimRequest{Order: order, HolderID: w.config.HolderID,
			BootID: w.config.BootID, LeaseToken: w.config.LeaseToken, RequestedTTL: w.config.LeaseTTL})
		if err != nil {
			stats.Errors++
			stats.LastError = boundedError(err)
			continue
		}
		switch claim.Status {
		case LeaseHeld:
			stats.LeaseHeld++
			continue
		case LeaseNotDue, LeaseNotFound, LeaseNotClaimable:
			continue
		case LeaseUnavailableNotSent, LeaseTimeoutUnknown:
			stats.Errors++
			stats.LastError = fmt.Sprintf("lease status %d", claim.Status)
			continue
		case LeaseClaimed:
			stats.Claimed++
		default:
			stats.Errors++
			stats.LastError = fmt.Sprintf("lease status %d", claim.Status)
			continue
		}
		result := w.processClaim(ctx, order, claim, &stats)
		if result != nil {
			stats.Errors++
			stats.LastError = boundedError(result)
		}
	}
	if page.Exhausted {
		w.mu.Lock()
		w.cursor = DiscoveryCursor{}
		w.mu.Unlock()
	} else {
		w.mu.Lock()
		w.cursor = page.Next
		w.mu.Unlock()
	}
	return stats, nil
}

// Run polls the durable Pair DueIndex. It intentionally has no channel or
// in-memory queue: a restart resumes from Redis through the cursor and the
// source WorkRecord lease.
func (w *Worker) Run(ctx context.Context, interval time.Duration) error {
	if w == nil {
		return fmt.Errorf("%w: nil worker", ErrInvalidConfig)
	}
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if _, err := w.RunOnce(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func validateCandidate(candidate DispatchCandidate) error {
	if candidate.Member == "" || len(candidate.Member) > 1024 {
		return ErrMalformedMember
	}
	ref, err := workidentity.DecodeWorkRefV1(candidate.Member)
	if err != nil || ref.WorkID == "" {
		return ErrMalformedMember
	}
	return nil
}

func (w *Worker) processClaim(ctx context.Context, order DispatchOrder, claim LeaseClaim, stats *Stats) error {
	if claim.WorkVersion == 0 || (order.WorkVersion != 0 && claim.WorkVersion != order.WorkVersion) ||
		claim.LeaseEpoch == 0 || claim.HolderID != w.config.HolderID || claim.BootID != w.config.BootID ||
		claim.AcceptedTTL <= 0 || len(claim.PayloadHash) != sha256.Size || !bytesEqual(claim.PayloadHash, order.PayloadHash) ||
		len(claim.SourceProofHash) != sha256.Size || !bytesEqual(claim.SourceProofHash, SourceProofHash(order)) {
		stats.LeaseLost++
		return fmt.Errorf("%w: pair claim identity version=%d/%d epoch=%d holder=%q/%q boot=%q/%q ttl=%s payload=%t source=%t",
			ErrLeaseLost, claim.WorkVersion, order.WorkVersion, claim.LeaseEpoch, claim.HolderID, w.config.HolderID,
			claim.BootID, w.config.BootID, claim.AcceptedTTL, len(claim.PayloadHash) == sha256.Size && bytesEqual(claim.PayloadHash, order.PayloadHash),
			len(claim.SourceProofHash) == sha256.Size && bytesEqual(claim.SourceProofHash, SourceProofHash(order)))
	}
	if claim.DeadlineAtMs == 0 {
		claim.DeadlineAtMs = order.DeadlineAtMs
	}
	if claim.NextDueAtMs == 0 {
		claim.NextDueAtMs = order.NextDueAtMs
	}
	if claim.RetryCount < order.RetryCount {
		stats.LeaseLost++
		return ErrLeaseLost
	}
	if order.WorkVersion == 0 {
		order.WorkVersion = claim.WorkVersion
	}
	if len(order.ResultProofHash) == 0 && len(claim.ResultProofHash) == sha256.Size {
		order.ResultProofHash = append([]byte(nil), claim.ResultProofHash...)
	}
	fence := LeaseFence{HolderID: w.config.HolderID, BootID: w.config.BootID,
		LeaseToken: w.config.LeaseToken, LeaseEpoch: claim.LeaseEpoch, WorkVersion: claim.WorkVersion}
	if w.config.RenewBeforeAdvance {
		renewed, err := w.config.Leases.Renew(ctx, RenewRequest{Order: order, Fence: fence,
			RequestedTTL: w.config.LeaseTTL, PayloadHash: order.PayloadHash, SourceProofHash: SourceProofHash(order)})
		if err != nil || renewed.Status != LeaseRenewed {
			stats.LeaseLost++
			return errors.Join(ErrLeaseLost, err)
		}
	}
	ids, err := DeriveTheftIdentity(order)
	if err != nil {
		return w.reschedule(ctx, order, claim, fence, "INVALID_IDENTITY", err, stats)
	}
	advanceRequest := AdvanceRequest{Identity: ids, Order: order, Lease: fence, CreateIfAbsent: true,
		PayloadHash: order.CanonicalPayloadHash, SourceProofHash: SourceProofHash(order)}
	operationCtx, cancel := context.WithTimeout(ctx, w.config.OperationTimeout)
	result, advanceErr := w.config.Advancer.CreateIfAbsentOrContinue(operationCtx, advanceRequest)
	cancel()
	if advanceErr != nil {
		stats.StepUnknown++
		// An RPC error is never treated as NOT_COMMITTED. Resolve the durable
		// receipt first, with the same IDs and hashes, before scheduling retry.
		receipt, receiptErr := w.config.Advancer.ReceiptFirst(ctx, ReceiptRequest{Identity: ids, Order: order,
			Lease: fence, PayloadHash: order.CanonicalPayloadHash, SourceProofHash: SourceProofHash(order)})
		if receiptErr == nil && isTerminalAdvance(receipt.Status) {
			return w.complete(ctx, order, claim, fence, receipt, stats)
		}
		if receiptErr != nil && !errors.Is(receiptErr, ErrReceiptUnavailable) {
			advanceErr = errors.Join(advanceErr, receiptErr)
		}
		return w.reschedule(ctx, order, claim, fence, errorClass(advanceErr, "ADVANCE_UNKNOWN"), advanceErr, stats)
	}
	if result.Status == AdvanceNotCommitted {
		return w.reschedule(ctx, order, claim, fence, errorClassString(result.ErrorClass, "NOT_COMMITTED"), ErrReceiptUnavailable, stats)
	}
	if result.Status == AdvanceReconcileRequired {
		return w.rescheduleTerminal(ctx, order, claim, fence, ReconcileRequiredState, errorClassString(result.ErrorClass, ReconcileRequiredState), ErrReconcileRequired, stats)
	}
	if !isTerminalAdvance(result.Status) {
		return w.reschedule(ctx, order, claim, fence, errorClassString(result.ErrorClass, "ADVANCE_INVALID"), fmt.Errorf("%w: advance status %d", ErrResultProofMissing, result.Status), stats)
	}
	if len(result.ResultProofHash) != sha256.Size {
		return w.reschedule(ctx, order, claim, fence, "RESULT_PROOF_MISSING", ErrResultProofMissing, stats)
	}
	order.ResultProofHash = append([]byte(nil), result.ResultProofHash...)
	return w.complete(ctx, order, claim, fence, result, stats)
}

func isTerminalAdvance(status AdvanceStatus) bool {
	return status == AdvanceApplied || status == AdvanceAlreadyApplied || status == AdvanceRejected || status == AdvanceNoChange
}

func (w *Worker) complete(ctx context.Context, order DispatchOrder, claim LeaseClaim, fence LeaseFence, result AdvanceResult, stats *Stats) error {
	proof := CompletionProof{PayloadHash: order.PayloadHash, SourceProofHash: SourceProofHash(order), ResultProofHash: result.ResultProofHash}
	completed, err := w.config.Leases.Complete(ctx, CompleteRequest{Order: order, Fence: fence, Proof: proof})
	if err == nil && (completed.Status == LeaseCompleted || completed.Status == LeaseAlreadyCompleted) {
		stats.Completed++
		w.observeTerminalLatency(order, stats)
		return nil
	}
	if completed.Status == LeaseCompleteFenceMismatch || completed.Status == LeaseCompleteStaleVersion {
		stats.LeaseLost++
		return ErrLeaseLost
	}
	// Complete response loss is safe to retry with the same result proof. A
	// receipt-first lookup prevents a second business invocation before the
	// idempotent Work completion retry.
	if err != nil || completed.Status == LeaseCompleteTimeoutUnknown || completed.Status == LeaseCompleteUnavailableNotSent {
		resolved, receiptErr := w.config.Advancer.ReceiptFirst(ctx, ReceiptRequest{Identity: TheftIdentity{WorkflowID: order.WorkflowID,
			SettlementID: order.SettlementID, AttemptID: order.AttemptID, OperationID: order.OperationID, PairID: order.PairID},
			Order: order, Lease: fence, PayloadHash: order.CanonicalPayloadHash, SourceProofHash: SourceProofHash(order)})
		if receiptErr == nil && isTerminalAdvance(resolved.Status) {
			completed, err = w.config.Leases.Complete(ctx, CompleteRequest{Order: order, Fence: fence, Proof: proof})
			if err == nil && (completed.Status == LeaseCompleted || completed.Status == LeaseAlreadyCompleted) {
				stats.Completed++
				w.observeTerminalLatency(order, stats)
				return nil
			}
		}
	}
	stats.Errors++
	return errors.Join(err, fmt.Errorf("theft recovery: complete status %d", completed.Status))
}

func (w *Worker) observeTerminalLatency(order DispatchOrder, stats *Stats) {
	if stats == nil || order.Permit.ExecutionAcceptedAtAuthorityMs <= 0 {
		return
	}
	latency := w.config.Now().Sub(time.UnixMilli(order.Permit.ExecutionAcceptedAtAuthorityMs))
	if latency >= 0 {
		stats.TerminalLatencies = append(stats.TerminalLatencies, latency)
	}
}

func (w *Worker) reschedule(ctx context.Context, order DispatchOrder, claim LeaseClaim, fence LeaseFence, class string, cause error, stats *Stats) error {
	nextRetry := claim.RetryCount + 1
	nowMs := w.config.Now().UnixMilli()
	deadline := claim.DeadlineAtMs
	if deadline <= 0 {
		deadline = order.DeadlineAtMs
	}
	nextDue := nowMs + w.config.Backoff.delay(nextRetry).Milliseconds()
	if nextDue > deadline {
		nextDue = deadline
	}
	if nextRetry >= w.config.MaxAttempts || nowMs >= deadline || nextDue >= deadline {
		return w.rescheduleTerminal(ctx, order, claim, fence, ReconcileRequiredState, class, cause, stats)
	}
	response, err := w.config.Leases.Reschedule(ctx, RescheduleRequest{Order: order, Fence: fence,
		RetryCount: nextRetry, DeadlineAtMs: deadline, NextDueAtMs: nextDue, ErrorClass: class,
		Proof: CompletionProof{PayloadHash: order.PayloadHash, SourceProofHash: SourceProofHash(order), ResultProofHash: order.ResultProofHash}})
	if err == nil && response.Status == LeaseRescheduled {
		stats.Retried++
		return cause
	}
	if response.Status == LeaseRescheduleFenceMismatch || response.Status == LeaseRescheduleStaleVersion || response.Status == LeaseRescheduleExpired {
		stats.LeaseLost++
		return errors.Join(ErrLeaseLost, err)
	}
	return errors.Join(cause, err, fmt.Errorf("theft recovery: reschedule status %d", response.Status))
}

func (w *Worker) rescheduleTerminal(ctx context.Context, order DispatchOrder, claim LeaseClaim, fence LeaseFence, state, class string, cause error, stats *Stats) error {
	deadline := claim.DeadlineAtMs
	if deadline <= 0 {
		deadline = order.DeadlineAtMs
	}
	response, err := w.config.Leases.Reschedule(ctx, RescheduleRequest{Order: order, Fence: fence,
		RetryCount: claim.RetryCount + 1, DeadlineAtMs: deadline, NextDueAtMs: 0,
		ErrorClass: class, TerminalState: state,
		Proof: CompletionProof{PayloadHash: order.PayloadHash, SourceProofHash: SourceProofHash(order), ResultProofHash: order.ResultProofHash}})
	if err == nil && response.Status == LeaseReconcileRequired {
		stats.Reconciled++
		return errors.Join(ErrReconcileRequired, cause)
	}
	if response.Status == LeaseRescheduleFenceMismatch || response.Status == LeaseRescheduleStaleVersion || response.Status == LeaseRescheduleExpired {
		stats.LeaseLost++
		return errors.Join(ErrLeaseLost, err)
	}
	return errors.Join(ErrReconcileRequired, cause, err, fmt.Errorf("theft recovery: terminal reschedule status %d", response.Status))
}

func errorClass(err error, fallback string) string {
	if err == nil {
		return fallback
	}
	return errorClassString(err.Error(), fallback)
}

func errorClassString(value, fallback string) string {
	if strings.TrimSpace(fallback) == "" {
		fallback = "WORKER_ERROR"
	}
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	message := strings.ToUpper(strings.TrimSpace(value))
	if len(message) > 96 {
		message = message[:96]
	}
	for i := 0; i < len(message); i++ {
		if (message[i] < 'A' || message[i] > 'Z') && (message[i] < '0' || message[i] > '9') && message[i] != '_' {
			message = message[:i] + "_" + message[i+1:]
		}
	}
	if message == "" {
		return fallback
	}
	return message
}

func boundedError(err error) string {
	if err == nil {
		return ""
	}
	message := strings.TrimSpace(err.Error())
	if len(message) > 256 {
		message = message[:256]
	}
	return message
}

// HexHash is useful to Redis adapters that store proof hashes as lowercase
// hexadecimal fields.
func HexHash(value []byte) string { return hex.EncodeToString(value) }
