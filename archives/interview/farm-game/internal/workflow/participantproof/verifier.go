package participantproof

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrInvalidCall means that an adapter was called without the typed,
	// kernel-validated ParticipantCall it requires.
	ErrInvalidCall = errors.New("participant proof: invalid call")
	// ErrRepositoryUnavailable is returned when the durable WorkRepository
	// could not establish a proof. An unavailable repository is never treated
	// as a successful admission.
	ErrRepositoryUnavailable = errors.New("participant proof: repository unavailable")
	// ErrWorkNotFound means that no durable WorkRecord exists for the supplied
	// WorkAddress. Missing work is not an implicit success or empty record.
	ErrWorkNotFound = errors.New("participant proof: work record not found")
	// ErrInvalidRecord means that the repository returned a malformed record.
	ErrInvalidRecord = errors.New("participant proof: invalid work record")
	// ErrWorkAddressMismatch means that the request does not name the exact
	// persisted WorkAddress or its versioned physical bucket.
	ErrWorkAddressMismatch = errors.New("participant proof: WorkAddress mismatch")
	// ErrWorkVersionMismatch means that the request's expected version is not
	// the version currently persisted by the WorkRepository.
	ErrWorkVersionMismatch = errors.New("participant proof: WorkVersion mismatch")
	// ErrIdentityMismatch means that subject/operation identity is not bound to
	// the persisted WorkRecord.
	ErrIdentityMismatch = errors.New("participant proof: identity mismatch")
	// ErrStepMismatch means that the invocation is not persisted for this work,
	// is not the current first-execution step, or its frozen payload is missing
	// or forged.
	ErrStepMismatch = errors.New("participant proof: step mismatch")
	// ErrLeaseMismatch means that a recovery request does not hold the current
	// persisted lease fence.
	ErrLeaseMismatch = errors.New("participant proof: lease mismatch")
	// ErrLeaseExpired means that the persisted lease is no longer live.
	ErrLeaseExpired = errors.New("participant proof: lease expired")
	// ErrDependencyMismatch means that a Plant dependency is not proven by a
	// durable terminal StepRecord and its result hash.
	ErrDependencyMismatch = errors.New("participant proof: Plant dependency mismatch")
	// ErrRouteFenceUnavailable means that no physical route verifier was
	// injected. The adapter never derives or queries route state itself.
	ErrRouteFenceUnavailable = errors.New("participant proof: route fence verifier unavailable")
	// ErrBucketResolverUnavailable means that exact physical WorkAddress
	// validation was attempted without the W31-A bucket resolver.
	ErrBucketResolverUnavailable = errors.New("participant proof: bucket resolver unavailable")
	// ErrClockUnavailable means that lease expiry cannot be adjudicated from an
	// injected repository-authoritative/shared clock.
	ErrClockUnavailable = errors.New("participant proof: authoritative clock unavailable")
	// ErrSourceLayoutUnproven means that a source WorkAddress carries layout
	// metadata which the frozen WorkRecord representation cannot authoritatively
	// compare.
	ErrSourceLayoutUnproven = errors.New("participant proof: source authority layout is unproven")
	// ErrTerminalWork means that a first execution was attempted against an
	// immutable WorkRecord. Receipt replay must happen before this adapter.
	ErrTerminalWork = errors.New("participant proof: work record is terminal")
)

// Error is a proof failure that remains compatible with errors.Is while also
// producing a stable FailedPrecondition or Unavailable gRPC status when it
// crosses an existing Participant server boundary.
type Error struct {
	kind  error
	cause error
	code  codes.Code
	text  string
}

func (e *Error) Error() string {
	if e == nil {
		return "participant proof: nil error"
	}
	if e.text != "" {
		return e.text
	}
	if e.kind != nil {
		return e.kind.Error()
	}
	return "participant proof: failure"
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *Error) Is(target error) bool {
	if e == nil {
		return false
	}
	return target == e.kind || errors.Is(e.cause, target)
}

func (e *Error) GRPCStatus() *status.Status {
	if e == nil {
		return status.New(codes.Internal, "participant proof: nil error")
	}
	code := e.code
	if code == codes.OK {
		code = codes.FailedPrecondition
	}
	return status.New(code, e.Error())
}

func proofError(kind error, code codes.Code, format string, args ...any) error {
	return &Error{kind: kind, code: code, text: fmt.Sprintf(format, args...)}
}

func proofErrorWithCause(kind, cause error, code codes.Code, format string, args ...any) error {
	return &Error{kind: kind, cause: cause, code: code, text: fmt.Sprintf(format, args...)}
}

// RouteFenceVerifier is intentionally an injected physical dependency. The
// participant-proof package does not know a routing calculator, Redis key or
// holder lease layout.
type RouteFenceVerifier interface {
	VerifyRoute(context.Context, *contracts.ParticipantCall) error
}

// RouteFenceVerifierFunc adapts the existing route-owner callback without
// giving this package a route calculator or persistence dependency.
type RouteFenceVerifierFunc func(context.Context, *contracts.ParticipantCall) error

func (f RouteFenceVerifierFunc) VerifyRoute(ctx context.Context, call *contracts.ParticipantCall) error {
	return f(ctx, call)
}

// Options supplies the only physical dependencies owned by composition. The
// WorkRepository is the source of truth for every WorkRecord proof. The
// bucket resolver is the versioned W31-A physical placement calculator. The
// route verifier is supplied by the existing routing/authority composition.
type Options struct {
	Repository         workflow.WorkRepository
	SourceWorkReader   authoritystore.SourceWorkReader
	BucketResolver     workidentity.BucketResolver
	RouteFenceVerifier RouteFenceVerifier
	Now                func() time.Time
}

// Option configures a Verifier constructed with New.
type Option func(*Options)

// WithBucketResolver injects the versioned physical WorkAddress resolver.
func WithBucketResolver(resolver workidentity.BucketResolver) Option {
	return func(options *Options) { options.BucketResolver = resolver }
}

// WithSourceWorkReader admits existing source-slotted orders directly from
// their Actor authority bucket. It is used only for the statically registered
// GuardSyncOrder; independent workflows continue to use WorkRepository.
func WithSourceWorkReader(reader authoritystore.SourceWorkReader) Option {
	return func(options *Options) { options.SourceWorkReader = reader }
}

// WithRouteFenceVerifier injects the current routing/holder fence proof.
func WithRouteFenceVerifier(verifier RouteFenceVerifier) Option {
	return func(options *Options) { options.RouteFenceVerifier = verifier }
}

// WithClock injects the repository-authoritative/shared clock used for lease
// expiry adjudication. It is required for construction; tests use it to make
// takeover and expiry cases deterministic.
func WithClock(now func() time.Time) Option {
	return func(options *Options) { options.Now = now }
}

// Verifier is a reusable production adapter. It is deliberately stateless
// between calls: every proof reloads the current WorkRecord, so a stale read
// cannot be cached across a Participant admission.
type Verifier struct {
	repository     workflow.WorkRepository
	sourceWork     authoritystore.SourceWorkReader
	bucketResolver workidentity.BucketResolver
	routeFence     RouteFenceVerifier
	now            func() time.Time
}

// New creates a repository-backed Participant proof adapter. A repository,
// bucket resolver, route-fence verifier, and authoritative/shared clock are
// all required for a production adapter; omitting any of them is a
// construction error rather than an implicit allow path.
func New(repository workflow.WorkRepository, options ...Option) (*Verifier, error) {
	configured := Options{Repository: repository}
	for _, option := range options {
		if option != nil {
			option(&configured)
		}
	}
	return NewWithOptions(configured)
}

// NewWithOptions is the explicit constructor for composition roots that keep
// all proof dependencies in one struct.
func NewWithOptions(options Options) (*Verifier, error) {
	if options.Repository == nil {
		return nil, proofError(ErrRepositoryUnavailable, codes.FailedPrecondition, "participant proof: WorkRepository is required")
	}
	if options.BucketResolver == nil {
		return nil, proofError(ErrBucketResolverUnavailable, codes.FailedPrecondition, "participant proof: bucket resolver is required")
	}
	if options.RouteFenceVerifier == nil {
		return nil, proofError(ErrRouteFenceUnavailable, codes.FailedPrecondition, "participant proof: route fence verifier is required")
	}
	if options.Now == nil {
		return nil, proofError(ErrClockUnavailable, codes.FailedPrecondition, "participant proof: authoritative clock is required")
	}
	return &Verifier{
		repository:     options.Repository,
		sourceWork:     options.SourceWorkReader,
		bucketResolver: options.BucketResolver,
		routeFence:     options.RouteFenceVerifier,
		now:            options.Now,
	}, nil
}

// NewVerifier is a concise constructor for the production seams. Supply
// WithClock as one of options; retaining the variadic form keeps callers that
// used the original constructor source-compatible while failing closed when
// no authoritative clock is supplied.
func NewVerifier(repository workflow.WorkRepository, resolver workidentity.BucketResolver, route RouteFenceVerifier, options ...Option) (*Verifier, error) {
	configured := Options{
		Repository:         repository,
		BucketResolver:     resolver,
		RouteFenceVerifier: route,
	}
	for _, option := range options {
		if option != nil {
			option(&configured)
		}
	}
	return NewWithOptions(configured)
}

// VerifyRoute delegates route-fence checking to the injected owner. It never
// loads Redis or derives a route inside an Actor turn.
func (v *Verifier) VerifyRoute(ctx context.Context, call *contracts.ParticipantCall) error {
	if v == nil || v.routeFence == nil {
		return proofError(ErrRouteFenceUnavailable, codes.FailedPrecondition, "participant proof: route fence verifier is not configured")
	}
	if call == nil || call.RouteHeader == nil {
		return proofError(ErrInvalidCall, codes.InvalidArgument, "participant proof: route call is incomplete")
	}
	return v.routeFence.VerifyRoute(ctx, call)
}

// VerifyWorkAddress proves the request WorkAddress against the persisted
// record and the versioned physical bucket resolver.
func (v *Verifier) VerifyWorkAddress(ctx context.Context, call *contracts.ParticipantCall) error {
	// SourceOrder FirstHop creates the durable order in the same Lua as the
	// actor mutation. There is no generic WorkRecord to prove yet; CARE already
	// skipped this lookup, and PLANT/HARVEST/UNLOCK/THEFT share that layout.
	if isAuthoritySourceOrderCall(call) {
		return nil
	}
	_, err := v.loadRecord(ctx, call)
	return err
}

// VerifyIdentityBinding proves coordinator subject/operation identity and the
// subject carried by the typed payload. Worker requests have no operation or
// subject fields in the frozen protocol; their subject is therefore derived
// from the typed payload and bound to the persisted record.
func (v *Verifier) VerifyIdentityBinding(ctx context.Context, call *contracts.ParticipantCall) error {
	if isAuthoritySourceOrderCall(call) {
		return nil
	}
	record, err := v.loadRecord(ctx, call)
	if err != nil {
		return err
	}
	if err := v.verifyIdentityStepBinding(record, call); err != nil {
		return err
	}
	subject, ok := participantSubject(call.Payload)
	if !ok || subject == 0 || subject != record.SubjectPlayerID {
		if call.Spec.WorkType == identity.WorkTypeTheft {
			root, rootErr := decodeTheftRoot(record)
			if rootErr != nil {
				return rootErr
			}
			wantSubject := root.GetVisitorPlayerId()
			if call.Spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME {
				wantSubject = root.GetOwnerPlayerId()
			}
			if !ok || subject == 0 || subject != wantSubject ||
				(record.SubjectPlayerID != root.GetVisitorPlayerId() && record.SubjectPlayerID != root.GetOwnerPlayerId()) {
				return proofError(ErrIdentityMismatch, codes.FailedPrecondition,
					"participant proof: Theft typed payload subject is not bound to the frozen Work identities")
			}
			if call.CallerKind == contracts.ParticipantCallerCoordinator && call.SubjectPlayerID != wantSubject {
				return proofError(ErrIdentityMismatch, codes.FailedPrecondition,
					"participant proof: Theft coordinator subject is not bound to the participant target")
			}
		} else {
			return proofError(ErrIdentityMismatch, codes.FailedPrecondition,
				"participant proof: typed payload subject does not match WorkRecord subject")
		}
	}
	switch call.CallerKind {
	case contracts.ParticipantCallerCoordinator:
		if call.OperationID != record.OperationID || (call.Spec.WorkType != identity.WorkTypeTheft && call.SubjectPlayerID != record.SubjectPlayerID) {
			return proofError(ErrIdentityMismatch, codes.FailedPrecondition,
				"participant proof: coordinator subject/operation does not match WorkRecord")
		}
	case contracts.ParticipantCallerWorkerRecovery:
		// InternalCallerContext intentionally has no operation_id or subject
		// field. The durable invocation, WorkAddress and payload subject above
		// are the protocol's worker identity binding.
	default:
		return proofError(ErrInvalidCall, codes.InvalidArgument, "participant proof: unknown caller kind")
	}
	return nil
}

// VerifyWorkVersion proves that the request's expected version is the exact
// current persisted WorkVersion. The request value is never authoritative by
// itself.
func (v *Verifier) VerifyWorkVersion(ctx context.Context, call *contracts.ParticipantCall) error {
	record, err := v.loadRecord(ctx, call)
	if err != nil {
		return err
	}
	if record.Terminal() {
		return proofError(ErrTerminalWork, codes.FailedPrecondition, "participant proof: WorkRecord is terminal")
	}
	if call.ExpectedWorkVersion == 0 || record.WorkVersion != call.ExpectedWorkVersion {
		return proofError(ErrWorkVersionMismatch, codes.FailedPrecondition,
			"participant proof: WorkVersion got=%d want=%d", call.ExpectedWorkVersion, record.WorkVersion)
	}
	return v.verifyStepBinding(record, call)
}

// VerifyWorkLease proves a worker recovery request against the persisted
// lease owner, epoch, token hash and trusted expiry. Caller fields are only
// accepted after all four persisted checks succeed.
func (v *Verifier) VerifyWorkLease(ctx context.Context, call *contracts.ParticipantCall) error {
	var sourceReadAtMs int64
	var record *workflow.WorkRecord
	var err error
	if call != nil && call.Spec.WorkType == identity.WorkTypeGuardSyncOrder && call.Work != nil &&
		call.Work.GetPlacementKind() == contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET &&
		v != nil && v.sourceWork != nil {
		workType, typeErr := contracts.StableWorkTypeName(call.Work.GetWorkType())
		if typeErr != nil {
			return proofError(ErrInvalidCall, codes.InvalidArgument, "participant proof: WorkType is invalid")
		}
		record, err = v.loadGuardSyncSourceRecord(ctx, call, workType, &sourceReadAtMs)
	} else {
		record, err = v.loadRecord(ctx, call)
	}
	if err != nil {
		return err
	}
	if call.CallerKind != contracts.ParticipantCallerWorkerRecovery {
		return proofError(ErrLeaseMismatch, codes.FailedPrecondition, "participant proof: lease proof requires worker recovery")
	}
	if record.Terminal() || record.LeaseOwner == "" || record.LeaseEpoch == 0 || record.LeaseTokenHash == "" {
		return proofError(ErrLeaseMismatch, codes.FailedPrecondition, "participant proof: WorkRecord has no live worker lease")
	}
	if record.LeaseOwner != call.CallerInstanceID || record.LeaseEpoch != call.LeaseEpoch {
		return proofError(ErrLeaseMismatch, codes.FailedPrecondition, "participant proof: lease owner or epoch mismatch")
	}
	leaseMatches := secureLeaseTokenEqual(record.LeaseTokenHash, call.LeaseOwnerToken)
	if record.Address.PlacementKind == workidentity.SourcePersistenceBucket {
		leaseMatches = secureSourceLeaseTokenEqual(record.LeaseTokenHash, call.LeaseOwnerToken)
	}
	if !leaseMatches {
		return proofError(ErrLeaseMismatch, codes.FailedPrecondition, "participant proof: lease token mismatch")
	}
	if sourceReadAtMs > 0 {
		if record.LeaseExpireAtMs <= sourceReadAtMs {
			return proofError(ErrLeaseExpired, codes.FailedPrecondition, "participant proof: worker lease has expired")
		}
		return v.verifyStepBinding(record, call)
	}
	if v == nil || v.now == nil {
		return proofError(ErrClockUnavailable, codes.FailedPrecondition,
			"participant proof: authoritative clock is not configured")
	}
	now := v.now()
	if now.IsZero() {
		return proofError(ErrClockUnavailable, codes.FailedPrecondition,
			"participant proof: authoritative clock returned an invalid time")
	}
	if record.LeaseExpireAtMs <= now.UnixMilli() {
		return proofError(ErrLeaseExpired, codes.FailedPrecondition, "participant proof: worker lease has expired")
	}
	return v.verifyStepBinding(record, call)
}

func isCareRewardCall(call *contracts.ParticipantCall) bool {
	return call != nil && call.Spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD
}

func isAuthoritySourceOrderCall(call *contracts.ParticipantCall) bool {
	if call == nil || call.Work == nil ||
		call.Work.GetPlacementKind() != contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET {
		return isCareRewardCall(call)
	}
	switch call.Spec.WorkType {
	case identity.WorkTypeCareReward, identity.WorkTypePlant, identity.WorkTypeUnlockPlot,
		identity.WorkTypeHarvest, identity.WorkTypeTheftVisitor, identity.WorkTypeTheftGuard,
		identity.WorkTypeTheft:
		// SETTLE_THEFT_VISITOR / CREDIT_GUARD_INCOME keep Spec.WorkType=THEFT
		// while the wire address is SOURCE_PERSISTENCE_BUCKET (THEFT_VISITOR /
		// THEFT_GUARD SourceOrder). Generic dest WorkRecord stays on
		// INDEPENDENT_WORK_BUCKET and still goes through loadRecord.
		return true
	default:
		return isCareRewardCall(call)
	}
}

func secureLeaseTokenEqual(expectedHash, token string) bool {
	if token == "" || expectedHash == "" {
		return false
	}
	digest := sha256.Sum256([]byte("wf-lease-token\x00" + token))
	actual := hex.EncodeToString(digest[:])
	return len(expectedHash) == len(actual) && subtle.ConstantTimeCompare([]byte(expectedHash), []byte(actual)) == 1
}

func secureSourceLeaseTokenEqual(expectedHash, token string) bool {
	if token == "" || expectedHash == "" {
		return false
	}
	h := sha256.New()
	for _, part := range []string{"lease-token", token} {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(part)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(part))
	}
	actual := hex.EncodeToString(h.Sum(nil))
	return len(expectedHash) == len(actual) && subtle.ConstantTimeCompare([]byte(expectedHash), []byte(actual)) == 1
}

func (v *Verifier) loadRecord(ctx context.Context, call *contracts.ParticipantCall) (*workflow.WorkRecord, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if v == nil || v.repository == nil {
		return nil, proofError(ErrRepositoryUnavailable, codes.FailedPrecondition, "participant proof: WorkRepository is not configured")
	}
	if call == nil || call.Work == nil {
		return nil, proofError(ErrInvalidCall, codes.InvalidArgument, "participant proof: WorkAddress is required")
	}
	workType, err := contracts.StableWorkTypeName(call.Work.GetWorkType())
	if err != nil || strings.TrimSpace(call.Work.GetWorkId()) == "" {
		return nil, proofError(ErrInvalidCall, codes.InvalidArgument, "participant proof: WorkAddress shape is invalid")
	}
	if call.Spec.WorkType == identity.WorkTypeGuardSyncOrder &&
		call.Work.GetPlacementKind() == contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET &&
		v.sourceWork != nil {
		return v.loadGuardSyncSourceRecord(ctx, call, workType)
	}
	record, err := v.repository.Get(ctx, workType, call.Work.GetWorkId())
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		if errors.Is(err, workflow.ErrNotFound) {
			return nil, proofErrorWithCause(ErrWorkNotFound, err, codes.FailedPrecondition,
				"participant proof: WorkRecord %s/%s is missing", workType, call.Work.GetWorkId())
		}
		return nil, proofErrorWithCause(ErrRepositoryUnavailable, err, codes.Unavailable,
			"participant proof: WorkRepository.Get failed for %s/%s: %v", workType, call.Work.GetWorkId(), err)
	}
	if record == nil {
		return nil, proofError(ErrWorkNotFound, codes.FailedPrecondition,
			"participant proof: WorkRepository returned a nil WorkRecord")
	}
	if err := record.Validate(); err != nil {
		return nil, proofErrorWithCause(ErrInvalidRecord, err, codes.FailedPrecondition,
			"participant proof: persisted WorkRecord is invalid: %v", err)
	}
	want, err := workAddressFromWire(call.Work)
	if err != nil {
		return nil, err
	}
	if record.Address != want {
		return nil, proofError(ErrWorkAddressMismatch, codes.FailedPrecondition,
			"participant proof: request WorkAddress does not equal persisted WorkAddress")
	}
	if v.bucketResolver == nil {
		return nil, proofError(ErrBucketResolverUnavailable, codes.FailedPrecondition,
			"participant proof: bucket resolver is not configured")
	}
	if err := record.Address.Validate(v.bucketResolver); err != nil {
		return nil, proofErrorWithCause(ErrWorkAddressMismatch, err, codes.FailedPrecondition,
			"participant proof: persisted WorkAddress failed physical bucket validation: %v", err)
	}
	return record, nil
}

func (v *Verifier) loadGuardSyncSourceRecord(ctx context.Context, call *contracts.ParticipantCall, workType string, readAtOut ...*int64) (*workflow.WorkRecord, error) {
	address := call.Work
	source := address.GetSourceAuthority()
	if source == nil || source.GetAuthorityKind() != contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR ||
		source.GetActorRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER || source.GetAuthorityLayoutVersion() == "" {
		return nil, proofError(ErrInvalidCall, codes.InvalidArgument,
			"participant proof: GuardSyncOrder source authority is invalid")
	}
	sourceID, err := strconv.ParseUint(source.GetCanonicalAuthorityId(), 10, 64)
	if err != nil || sourceID == 0 || strconv.FormatUint(sourceID, 10) != source.GetCanonicalAuthorityId() {
		return nil, proofError(ErrInvalidCall, codes.InvalidArgument,
			"participant proof: GuardSyncOrder source actor id is not canonical")
	}
	wantAddress := workidentity.WorkAddress{
		WorkID: address.GetWorkId(), WorkType: workType,
		PlacementKind: workidentity.SourcePersistenceBucket,
		BucketVersion: address.GetBucketVersion(), BucketID: address.GetBucketId(),
		SourceRouteDomain: workidentity.RouteDomainPlayer, SourceActorID: sourceID,
	}
	if v.bucketResolver == nil {
		return nil, proofError(ErrBucketResolverUnavailable, codes.FailedPrecondition,
			"participant proof: bucket resolver is not configured")
	}
	if err := wantAddress.Validate(v.bucketResolver); err != nil {
		return nil, proofErrorWithCause(ErrWorkAddressMismatch, err, codes.FailedPrecondition,
			"participant proof: GuardSyncOrder physical WorkAddress is invalid: %v", err)
	}
	stored, err := v.sourceWork.ReadSourceWork(ctx,
		authoritystore.ActorAddress{Domain: "PLAYER", ActorID: sourceID}, workType, address.GetWorkId())
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		if errors.Is(err, authoritystore.ErrSourceWorkNotFound) {
			return nil, proofErrorWithCause(ErrWorkNotFound, err, codes.FailedPrecondition,
				"participant proof: source GuardSyncOrder %s is missing", address.GetWorkId())
		}
		return nil, proofErrorWithCause(ErrRepositoryUnavailable, err, codes.Unavailable,
			"participant proof: source GuardSyncOrder read failed: %v", err)
	}
	if stored.SchemaVersion != "1" || stored.WorkID != address.GetWorkId() || stored.WorkType != workType ||
		stored.PlacementKind != "SOURCE_PERSISTENCE_BUCKET" || stored.BucketVersion != address.GetBucketVersion() ||
		stored.BucketID != address.GetBucketId() || stored.SourceAuthorityKind != "PLAYER_ACTOR" ||
		stored.CanonicalSourceAuthority != source.GetCanonicalAuthorityId() ||
		stored.AuthorityLayoutVersion != source.GetAuthorityLayoutVersion() || stored.ActorRouteDomain != "PLAYER" {
		return nil, proofError(ErrWorkAddressMismatch, codes.FailedPrecondition,
			"participant proof: source GuardSyncOrder address does not match persisted authority metadata")
	}
	if stored.SubjectPlayerID != sourceID || stored.OperationID == "" || stored.WorkVersion == 0 ||
		stored.CurrentStep != "" || len(stored.TypedPayload) == 0 {
		return nil, proofError(ErrInvalidRecord, codes.FailedPrecondition,
			"participant proof: source GuardSyncOrder identity/control fields are invalid")
	}
	payloadHash, err := hex.DecodeString(stored.PayloadHash)
	if err != nil || len(payloadHash) != sha256.Size {
		return nil, proofError(ErrInvalidRecord, codes.FailedPrecondition,
			"participant proof: source GuardSyncOrder payload hash is invalid")
	}
	var rootHash [sha256.Size]byte
	copy(rootHash[:], payloadHash)
	deterministic, err := proto.MarshalOptions{Deterministic: true}.Marshal(call.Payload)
	if err != nil || !bytes.Equal(deterministic, stored.TypedPayload) {
		return nil, proofError(ErrStepMismatch, codes.FailedPrecondition,
			"participant proof: source GuardSyncOrder typed payload does not match Participant payload")
	}
	stepHash := sha256.Sum256(stored.TypedPayload)
	stepStatus := workflow.StepPending
	workState := workflow.WorkStateCreated
	switch stored.State {
	case "PENDING":
	case "REWARD_DISPATCHING":
		if stored.LeaseHolderID == "" || stored.LeaseHolderBootID == "" || stored.LeaseEpoch == 0 ||
			stored.LeaseTokenHash == "" || stored.LeaseExpireAtMs <= 0 {
			return nil, proofError(ErrInvalidRecord, codes.FailedPrecondition,
				"participant proof: claimed source GuardSyncOrder lease fence is incomplete")
		}
		if call.CallerKind == contracts.ParticipantCallerWorkerRecovery && stored.LeaseHolderBootID != call.CallerBootID {
			return nil, proofError(ErrLeaseMismatch, codes.FailedPrecondition,
				"participant proof: source GuardSyncOrder lease boot id mismatch")
		}
	case "COMPLETED":
		stepStatus, workState = workflow.StepApplied, workflow.WorkStateCompleted
	case "SUPERSEDED":
		stepStatus, workState = workflow.StepNoChange, workflow.WorkStateSuperseded
	case "REJECTED":
		stepStatus, workState = workflow.StepRejected, workflow.WorkStateRejected
	case "FAILED":
		stepStatus, workState = workflow.StepRejected, workflow.WorkStateFailed
	default:
		return nil, proofError(ErrInvalidRecord, codes.FailedPrecondition,
			"participant proof: source GuardSyncOrder state is invalid")
	}
	invocationID, err := identity.ParticipantInvocationID(identity.WorkTypeGuardSyncOrder,
		address.GetWorkId(), identity.StepApplyGuardSnapshot, 1)
	if err != nil {
		return nil, proofErrorWithCause(ErrInvalidRecord, err, codes.FailedPrecondition,
			"participant proof: source GuardSyncOrder invocation is invalid")
	}
	record := &workflow.WorkRecord{
		Address: wantAddress, SubjectPlayerID: stored.SubjectPlayerID, OperationID: stored.OperationID,
		Payload: append([]byte(nil), stored.TypedPayload...), PayloadHash: rootHash,
		State: workState, WorkVersion: stored.WorkVersion, CurrentStep: 0,
		Steps: []workflow.StepRecord{{Name: string(identity.StepApplyGuardSnapshot), InvocationID: invocationID,
			Payload: append([]byte(nil), stored.TypedPayload...), PayloadHash: stepHash, Status: stepStatus}},
		NextDueAtMs: stored.NextDueAtMs, RetryCount: stored.RetryCount, LastErrorClass: stored.LastErrorClass,
		LeaseOwner: stored.LeaseHolderID, LeaseEpoch: stored.LeaseEpoch,
		LeaseTokenHash: stored.LeaseTokenHash, LeaseExpireAtMs: stored.LeaseExpireAtMs,
		CreatedAtMs: stored.CreatedAtMs, UpdatedAtMs: stored.UpdatedAtMs, CompletedAtMs: stored.CompletedAtMs,
	}
	if err := record.Validate(); err != nil {
		return nil, proofErrorWithCause(ErrInvalidRecord, err, codes.FailedPrecondition,
			"participant proof: source GuardSyncOrder is invalid: %v", err)
	}
	if stored.ReadAtMs <= 0 {
		return nil, proofError(ErrInvalidRecord, codes.FailedPrecondition,
			"participant proof: source GuardSyncOrder read time is invalid")
	}
	if len(readAtOut) != 0 && readAtOut[0] != nil {
		*readAtOut[0] = stored.ReadAtMs
	}
	return record, nil
}

func workAddressFromWire(address *contractspb.WorkAddress) (workidentity.WorkAddress, error) {
	if address == nil {
		return workidentity.WorkAddress{}, proofError(ErrInvalidCall, codes.InvalidArgument, "participant proof: nil WorkAddress")
	}
	workType, err := contracts.StableWorkTypeName(address.GetWorkType())
	if err != nil {
		return workidentity.WorkAddress{}, proofErrorWithCause(ErrInvalidCall, err, codes.InvalidArgument,
			"participant proof: WorkType is invalid: %v", err)
	}
	var placement workidentity.WorkPlacementKind
	switch address.GetPlacementKind() {
	case contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET:
		placement = workidentity.IndependentWorkBucket
	case contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET:
		placement = workidentity.SourcePersistenceBucket
	default:
		return workidentity.WorkAddress{}, proofError(ErrInvalidCall, codes.InvalidArgument,
			"participant proof: WorkPlacementKind is invalid")
	}
	out := workidentity.WorkAddress{
		WorkID:        address.GetWorkId(),
		WorkType:      workType,
		PlacementKind: placement,
		BucketVersion: address.GetBucketVersion(),
		BucketID:      address.GetBucketId(),
	}
	source := address.GetSourceAuthority()
	if placement == workidentity.IndependentWorkBucket {
		if source != nil {
			return workidentity.WorkAddress{}, proofError(ErrInvalidCall, codes.InvalidArgument,
				"participant proof: independent WorkAddress carries source authority")
		}
		if err := out.ValidateShape(); err != nil {
			return workidentity.WorkAddress{}, proofErrorWithCause(ErrInvalidCall, err, codes.InvalidArgument,
				"participant proof: WorkAddress shape is invalid: %v", err)
		}
		return out, nil
	}
	if source == nil {
		return workidentity.WorkAddress{}, proofError(ErrInvalidCall, codes.InvalidArgument,
			"participant proof: source WorkAddress has no source authority")
	}
	// WorkRecord's frozen internal WorkAddress has no AuthorityLayoutVersion.
	// The shared ingress kernel rejects an empty layout version; a non-empty
	// value therefore cannot be silently discarded or compared here.
	if source.GetAuthorityLayoutVersion() != "" {
		return workidentity.WorkAddress{}, proofErrorWithCause(ErrWorkAddressMismatch, ErrSourceLayoutUnproven,
			codes.FailedPrecondition,
			"participant proof: source AuthorityLayoutVersion cannot be proven by the persisted WorkAddress")
	}
	var domain workidentity.RouteDomain
	switch source.GetAuthorityKind() {
	case contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR:
		if source.GetActorRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER {
			return workidentity.WorkAddress{}, proofError(ErrInvalidCall, codes.InvalidArgument,
				"participant proof: player source route domain is invalid")
		}
		domain = workidentity.RouteDomainPlayer
	case contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_FARM_ACTOR:
		if source.GetActorRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM {
			return workidentity.WorkAddress{}, proofError(ErrInvalidCall, codes.InvalidArgument,
				"participant proof: farm source route domain is invalid")
		}
		domain = workidentity.RouteDomainFarm
	default:
		return workidentity.WorkAddress{}, proofError(ErrInvalidCall, codes.InvalidArgument,
			"participant proof: non-actor source cannot be represented by WorkRecord WorkAddress")
	}
	actorID, err := strconv.ParseUint(source.GetCanonicalAuthorityId(), 10, 64)
	if err != nil || actorID == 0 || strconv.FormatUint(actorID, 10) != source.GetCanonicalAuthorityId() {
		return workidentity.WorkAddress{}, proofError(ErrInvalidCall, codes.InvalidArgument,
			"participant proof: source actor id is not canonical")
	}
	out.SourceRouteDomain = domain
	out.SourceActorID = actorID
	if err := out.ValidateShape(); err != nil {
		return workidentity.WorkAddress{}, proofErrorWithCause(ErrInvalidCall, err, codes.InvalidArgument,
			"participant proof: source WorkAddress shape is invalid: %v", err)
	}
	return out, nil
}

func (v *Verifier) verifyStepBinding(record *workflow.WorkRecord, call *contracts.ParticipantCall) error {
	step, index, err := persistedStepForCall(record, call)
	if err != nil {
		return err
	}
	if index != record.CurrentStep {
		return proofError(ErrStepMismatch, codes.FailedPrecondition,
			"participant proof: request step/invocation is not the persisted current step")
	}
	if step.Status != workflow.StepPending && step.Status != workflow.StepUnknown {
		return proofError(ErrStepMismatch, codes.FailedPrecondition,
			"participant proof: current StepRecord is not executable (%s)", step.Status)
	}
	return verifyPersistedStepPayload(step, call)
}

// verifyIdentityStepBinding is the ingress-safe identity proof. Shared
// Participant ingress runs before the durable receipt lookup, so an exact
// retry may name a terminal historical StepRecord after WorkRecord.CurrentStep
// has advanced. First execution remains fenced by verifyStepBinding below.
func (v *Verifier) verifyIdentityStepBinding(record *workflow.WorkRecord, call *contracts.ParticipantCall) error {
	step, index, err := persistedStepForCall(record, call)
	if err != nil {
		return err
	}
	if index != record.CurrentStep && !workflow.StepTerminal(step.Status) {
		return proofError(ErrStepMismatch, codes.FailedPrecondition,
			"participant proof: non-current retry step has no terminal receipt state")
	}
	if index == record.CurrentStep && step.Status != workflow.StepPending &&
		step.Status != workflow.StepUnknown && !workflow.StepTerminal(step.Status) {
		return proofError(ErrStepMismatch, codes.FailedPrecondition,
			"participant proof: current StepRecord is not a valid retry or execution state (%s)", step.Status)
	}
	return verifyPersistedStepPayload(step, call)
}

func persistedStepForCall(record *workflow.WorkRecord, call *contracts.ParticipantCall) (*workflow.StepRecord, int, error) {
	if record == nil || call == nil {
		return nil, -1, proofError(ErrInvalidCall, codes.InvalidArgument, "participant proof: step call is incomplete")
	}
	if call.ParticipantStep == "" || call.InvocationID == "" || call.CanonicalStepPayloadHash == ([32]byte{}) {
		return nil, -1, proofError(ErrStepMismatch, codes.FailedPrecondition, "participant proof: step identity is incomplete")
	}
	if record.CurrentStep < 0 || record.CurrentStep >= len(record.Steps) {
		return nil, -1, proofError(ErrInvalidRecord, codes.FailedPrecondition, "participant proof: current step is out of range")
	}
	var (
		matched      *workflow.StepRecord
		matchedIndex = -1
	)
	for index := range record.Steps {
		step := &record.Steps[index]
		if step.Name != call.ParticipantStep || step.InvocationID != call.InvocationID {
			continue
		}
		if matched != nil {
			return nil, -1, proofError(ErrStepMismatch, codes.FailedPrecondition,
				"participant proof: invocation matches multiple persisted steps")
		}
		matched = step
		matchedIndex = index
	}
	if matched == nil {
		return nil, -1, proofError(ErrStepMismatch, codes.FailedPrecondition,
			"participant proof: request step/invocation is not persisted")
	}
	return matched, matchedIndex, nil
}

func verifyPersistedStepPayload(step *workflow.StepRecord, call *contracts.ParticipantCall) error {
	spec, err := participantSpecForStep(step.Name)
	if err != nil {
		return err
	}
	if call.Spec.Command != spec.Command || call.Spec.Step != spec.Step || call.Spec.WorkType != spec.WorkType {
		return proofError(ErrStepMismatch, codes.FailedPrecondition,
			"participant proof: request command registration does not match persisted step")
	}
	if call.Payload == nil {
		return proofError(ErrStepMismatch, codes.FailedPrecondition,
			"participant proof: request payload is missing")
	}
	requestHash, err := contracts.CanonicalParticipantHash(spec, call.Payload)
	if err != nil {
		return proofErrorWithCause(ErrStepMismatch, err, codes.FailedPrecondition,
			"participant proof: request payload cannot be canonically hashed: %v", err)
	}
	if requestHash != call.CanonicalStepPayloadHash {
		return proofError(ErrStepMismatch, codes.FailedPrecondition,
			"participant proof: request payload hash is not self-consistent")
	}
	if len(step.Payload) == 0 || step.PayloadHash == ([32]byte{}) {
		return proofError(ErrStepMismatch, codes.FailedPrecondition,
			"participant proof: persisted StepRecord payload is missing")
	}
	if sha256.Sum256(step.Payload) != step.PayloadHash {
		return proofError(ErrStepMismatch, codes.FailedPrecondition,
			"participant proof: persisted StepRecord payload hash is corrupt")
	}
	payload := spec.NewPayload()
	if err := proto.Unmarshal(step.Payload, payload); err != nil {
		return proofErrorWithCause(ErrStepMismatch, err, codes.FailedPrecondition,
			"participant proof: persisted step payload is not valid protobuf: %v", err)
	}
	deterministic, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil || string(deterministic) != string(step.Payload) {
		return proofError(ErrStepMismatch, codes.FailedPrecondition,
			"participant proof: persisted step payload is not deterministic")
	}
	canonical, err := contracts.CanonicalParticipantHash(spec, payload)
	if err != nil {
		return proofErrorWithCause(ErrStepMismatch, err, codes.FailedPrecondition,
			"participant proof: persisted step payload cannot be canonically hashed: %v", err)
	}
	if canonical != call.CanonicalStepPayloadHash {
		return proofError(ErrStepMismatch, codes.FailedPrecondition,
			"participant proof: request payload hash does not match persisted StepRecord payload")
	}
	return nil
}

func participantSpecForStep(step string) (contracts.ParticipantCommandSpec, error) {
	for _, spec := range contracts.ParticipantCatalog() {
		if string(spec.Step) == step {
			return spec, nil
		}
	}
	return contracts.ParticipantCommandSpec{}, proofError(ErrStepMismatch, codes.FailedPrecondition,
		"participant proof: step %q is not registered", step)
}

func participantSubject(payload proto.Message) (uint64, bool) {
	switch payload := payload.(type) {
	case *contractspb.ReserveUnlockCoinPayload:
		return payload.GetPlayerId(), true
	case *contractspb.CommitPlotUnlockPayload:
		return payload.GetOwnerPlayerId(), true
	case *contractspb.CommitUnlockCoinReservationPayload:
		return payload.GetPlayerId(), true
	case *contractspb.ReleaseUnlockCoinReservationPayload:
		return payload.GetPlayerId(), true
	case *contractspb.ReserveSeedPayload:
		return payload.GetPlayerId(), true
	case *contractspb.CommitPlantPayload:
		return payload.GetOwnerPlayerId(), true
	case *contractspb.CommitSeedReservationPayload:
		return payload.GetPlayerId(), true
	case *contractspb.ReleaseSeedReservationPayload:
		return payload.GetPlayerId(), true
	case *contractspb.CommitHarvestPayload:
		return payload.GetOwnerPlayerId(), true
	case *contractspb.GrantHarvestRewardPayload:
		return payload.GetPlayerId(), true
	case *contractspb.DecideTheftPayload:
		return payload.GetVisitorPlayerId(), true
	case *contractspb.SettleTheftVisitorPayload:
		return payload.GetVisitorPlayerId(), true
	case *contractspb.CreditGuardIncomePayload:
		return payload.GetOwnerPlayerId(), true
	case *contractspb.ApplyCareRewardPayload:
		return payload.GetCarerPlayerId(), true
	case *contractspb.ApplyGuardSnapshotPayload:
		return payload.GetSourcePlayerId(), true
	default:
		return 0, false
	}
}
