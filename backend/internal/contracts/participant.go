package contracts

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/identity"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ParticipantCallerKind identifies which frozen unary ingress is being
// validated. Both entries intentionally return the same ParticipantCall so a
// production adapter cannot grow separate player/farm or coordinator/worker
// decoding paths.
type ParticipantCallerKind uint8

const (
	ParticipantCallerCoordinator ParticipantCallerKind = iota + 1
	ParticipantCallerWorkerRecovery
)

// ParticipantCall is the fail-closed, typed view of a ParticipantRequest.
// It contains no business decision and no persistence handle; callers may
// pass it across their verifier seam and then into one shared Handler.
type ParticipantCall struct {
	Spec                   ParticipantCommandSpec
	RouteHeader            *logicrpcpb.ActorRouteHeader
	Work                   *contractspb.WorkAddress
	SourceOrderCommitProof *contractspb.SourceOrderCommitProof
	BestEffortSourceOrder  *contractspb.CreatedSourceOrderEnvelope
	CallerKind             ParticipantCallerKind

	ParticipantStep string
	InvocationID    string
	OperationID     string
	SubjectPlayerID uint64

	ExpectedWorkVersion uint64
	RemainingBudgetMs   uint32
	CallerInstanceID    string
	CallerBootID        string
	LeaseOwnerToken     string
	LeaseEpoch          uint64

	Payload                  proto.Message
	CanonicalStepPayloadHash [32]byte
	ActorKey                 string
	// TrustedResolvedAtMs is populated only by the source Work proof for
	// participant steps whose task fact is classified by source resolution
	// time. It is never decoded from a public request field.
	TrustedResolvedAtMs int64
	// TrustedSourceDueAtMs is populated only by the durable SourceOrder Redis
	// verifier. Shadow commit-proof admission compares this value with the
	// signed exact-commit due time; callers cannot supply it on the wire.
	TrustedSourceDueAtMs int64

	// AdmissionCapacity* is a read-only snapshot produced by the durable
	// receipt lookup. The nested PlayerRecord candidate remains distinct from
	// the version-bound outer Authority usage held by PlayerActor; admission
	// joins them only through the concrete Store-backed capacity guard.
	AdmissionCapacityKnown               bool
	AdmissionRecordVersion               uint64
	AdmissionCandidateReceiptItems       int
	AdmissionCandidateModelBytes         int
	AdmissionCandidateReceiptBytes       int
	AdmissionCandidateSingleReceiptBytes int

	// budgetStart/budgetLimitMs/now are ingress-owned bookkeeping. They keep
	// the effective budget monotonic across decode and every injected proof;
	// callers cannot extend it by replacing RemainingBudgetMs after admission.
	budgetStart   time.Time
	budgetLimitMs uint32
	now           func() time.Time
}

// ParticipantRouteFenceVerifier proves that the complete route snapshot still
// names the current holder/shard fence. Structural route validation remains in
// this package; the physical assignment/lease lookup is injected by the
// service composition and is deliberately not guessed from actor_id.
type ParticipantRouteFenceVerifier interface {
	VerifyRoute(context.Context, *ParticipantCall) error
}

// ParticipantWorkAddressVerifier proves the versioned physical WorkAddress
// bucket identity (and any repository-specific source-authority binding).
// The shared kernel cannot import a Redis layout or a routing calculator.
type ParticipantWorkAddressVerifier interface {
	VerifyWorkAddress(context.Context, *ParticipantCall) error
}

// ParticipantIdentityBindingVerifier proves the WorkRecord binding between
// subject_player_id/operation_id and the supplied WorkAddress. The frozen
// ParticipantRequest carries the fields but not the Work Repository record.
type ParticipantIdentityBindingVerifier interface {
	VerifyIdentityBinding(context.Context, *ParticipantCall) error
}

type ParticipantRouteFenceVerifierFunc func(context.Context, *ParticipantCall) error

func (f ParticipantRouteFenceVerifierFunc) VerifyRoute(ctx context.Context, call *ParticipantCall) error {
	return f(ctx, call)
}

type ParticipantWorkAddressVerifierFunc func(context.Context, *ParticipantCall) error

func (f ParticipantWorkAddressVerifierFunc) VerifyWorkAddress(ctx context.Context, call *ParticipantCall) error {
	return f(ctx, call)
}

type ParticipantIdentityBindingVerifierFunc func(context.Context, *ParticipantCall) error

func (f ParticipantIdentityBindingVerifierFunc) VerifyIdentityBinding(ctx context.Context, call *ParticipantCall) error {
	return f(ctx, call)
}

// ParticipantIngressOptions are physical/repository proofs that cannot be
// derived from the frozen request alone. A production Participant adapter
// must supply all three; the compatibility ValidateAndDecode wrapper leaves
// them unset for contract-only callers such as the legacy fake.
type ParticipantIngressOptions struct {
	RouteFenceVerifier      ParticipantRouteFenceVerifier
	WorkAddressVerifier     ParticipantWorkAddressVerifier
	IdentityBindingVerifier ParticipantIdentityBindingVerifier
	// AllowBestEffortSourceOrder admits an in-memory source-order envelope in
	// Coordinator calls. The envelope replaces only the physical source-order
	// and WorkRecord lookups; route fencing and all typed/binding checks remain
	// mandatory. Worker recovery can never use this mode.
	AllowBestEffortSourceOrder bool
	// RequirePhysicalProof makes the shared kernel reject a route header that
	// omits the holder/boot/shard/writer fields. Production servers always enable
	// it. During the lease-proof rollout, a fully absent lease epoch/deadline is
	// still accepted and observed; a partial lease proof is always rejected.
	RequirePhysicalProof bool
	// RequireRouteLeaseProof is the opt-in strict rollout gate. When enabled, a
	// complete lease epoch/deadline is mandatory in addition to the physical
	// holder tuple. It defaults false so new servers can accept legacy producers
	// while all in-tree producers are upgraded first.
	RequireRouteLeaseProof bool
	// Now is injectable only for deterministic elapsed-budget tests. Production
	// composition leaves it nil and uses the wall/monotonic clock from time.Now.
	Now func() time.Time
}

// ParticipantMaxBudgetMs is the conservative numeric upper bound for a
// Participant step. The frozen catalog carries the TimeoutBudget config key,
// not a wire number; this shared adapter cap is the local command-spec upper
// bound and cannot be raised by a request caller.
const ParticipantMaxBudgetMs uint32 = 5000

// ParticipantCommandMaxBudgetMs returns the frozen adapter maximum for a
// registered Participant command. Keeping the function keyed by the
// catalog spec makes the min(request, context, command-spec) rule explicit
// without changing the frozen catalog/IDL.
func ParticipantCommandMaxBudgetMs(spec ParticipantCommandSpec) uint32 {
	if spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_UNSPECIFIED {
		return 0
	}
	return ParticipantMaxBudgetMs
}

// ValidateAndDecode is the single Participant ingress kernel. It validates
// the caller oneof, route fence shape, budget/deadline, WorkAddress shape,
// command/step and WorkType/placement/source binding, deterministic
// invocation id, typed payload size/schema and the semantic
// CanonicalStepPayloadHash. It must run before a mailbox is selected or a
// handler is invoked.
func ValidateAndDecode(req *contractspb.ParticipantRequest, callerKind ParticipantCallerKind) (*ParticipantCall, error) {
	return ValidateAndDecodeWithOptions(context.Background(), req, callerKind, ParticipantIngressOptions{})
}

// ValidateAndDecodeWithOptions runs the same kernel as ValidateAndDecode and
// then executes the injected physical route, bucket and WorkRecord identity
// proofs. Verifiers run after decoding (so they receive the typed call) but
// before any receipt lookup or actor mailbox access.
func ValidateAndDecodeWithOptions(ctx context.Context, req *contractspb.ParticipantRequest, callerKind ParticipantCallerKind, options ParticipantIngressOptions) (*ParticipantCall, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "contracts: participant request is required")
	}
	if proof := req.GetSourceOrderCommitProof(); proof != nil {
		if callerKind != ParticipantCallerCoordinator {
			return nil, status.Error(codes.InvalidArgument, "contracts: source-order commit proof is Coordinator-only")
		}
		switch req.GetParticipantCommand() {
		case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD,
			contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD:
		default:
			return nil, status.Error(codes.InvalidArgument, "contracts: source-order commit proof is CARE/HARVEST-only")
		}
	}
	if envelope := req.GetBestEffortSourceOrder(); envelope != nil {
		if !options.AllowBestEffortSourceOrder {
			return nil, status.Error(codes.FailedPrecondition, "contracts: best-effort source order is disabled")
		}
		if callerKind != ParticipantCallerCoordinator {
			return nil, status.Error(codes.InvalidArgument, "contracts: best-effort source order is Coordinator-only")
		}
		if req.GetSourceOrderCommitProof() != nil {
			return nil, status.Error(codes.InvalidArgument, "contracts: best-effort source order and durable commit proof are mutually exclusive")
		}
		if !bestEffortSourceCommand(req.GetParticipantCommand()) {
			return nil, status.Error(codes.InvalidArgument, "contracts: participant command does not admit a best-effort source order")
		}
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	// Start before route/caller validation so every decode/admission operation,
	// not just protobuf unmarshal, consumes the effective request budget.
	started := now()
	spec, err := LookupParticipantCommand(req.GetParticipantCommand())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	strictPhysical := options.RequirePhysicalProof || options.RequireRouteLeaseProof || options.RouteFenceVerifier != nil ||
		options.WorkAddressVerifier != nil || options.IdentityBindingVerifier != nil
	legacyRouteLeaseProof, err := validateRouteHeader(spec, req, strictPhysical, options.RequireRouteLeaseProof)
	if err != nil {
		return nil, err
	}
	caller, err := extractCaller(req, callerKind)
	if err != nil {
		return nil, err
	}
	if err := validateCallerShape(caller); err != nil {
		return nil, err
	}
	effectiveBudget, err := effectiveParticipantBudget(ctx, caller.remainingBudgetMs, ParticipantCommandMaxBudgetMs(spec), started)
	if err != nil {
		return nil, err
	}
	if err := validateStepAndInvocation(spec, caller); err != nil {
		return nil, err
	}
	if len(req.GetTypedPayload()) > spec.MaxPayloadBytes {
		return nil, status.Error(codes.InvalidArgument, "contracts: typed payload exceeds MaxPayloadBytes")
	}
	payload := spec.NewPayload()
	if err := proto.Unmarshal(req.GetTypedPayload(), payload); err != nil {
		return nil, status.Error(codes.InvalidArgument, "contracts: typed payload decode failed")
	}
	stepHash, err := CanonicalParticipantHash(spec, payload)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("contracts: canonical step hash: %v", err))
	}
	if payloadActorID, ok := participantPayloadActorID(payload); !ok || payloadActorID == 0 || payloadActorID != req.GetRouteHeader().GetActorId() {
		return nil, status.Error(codes.InvalidArgument, "contracts: typed payload actor id does not match route header")
	}
	if caller.kind == ParticipantCallerCoordinator {
		if payloadSubjectID, ok := participantPayloadSubjectPlayerID(payload); ok &&
			(payloadSubjectID == 0 || payloadSubjectID != caller.subjectPlayerID) {
			return nil, status.Error(codes.FailedPrecondition, "contracts: subject player does not match typed payload")
		}
	}

	call := &ParticipantCall{
		Spec:                     spec,
		RouteHeader:              req.GetRouteHeader(),
		Work:                     caller.work,
		SourceOrderCommitProof:   cloneSourceOrderCommitProof(req.GetSourceOrderCommitProof()),
		BestEffortSourceOrder:    cloneBestEffortSourceOrder(req.GetBestEffortSourceOrder()),
		CallerKind:               caller.kind,
		ParticipantStep:          string(spec.Step),
		InvocationID:             caller.invocationID,
		OperationID:              caller.operationID,
		SubjectPlayerID:          caller.subjectPlayerID,
		ExpectedWorkVersion:      caller.expectedWorkVersion,
		RemainingBudgetMs:        effectiveBudget,
		CallerInstanceID:         caller.callerInstanceID,
		CallerBootID:             caller.callerBootID,
		LeaseOwnerToken:          caller.leaseOwnerToken,
		LeaseEpoch:               caller.leaseEpoch,
		Payload:                  payload,
		CanonicalStepPayloadHash: stepHash,
		ActorKey:                 participantActorKey(req),
		budgetStart:              started,
		budgetLimitMs:            effectiveBudget,
		now:                      now,
	}
	if call.BestEffortSourceOrder != nil {
		if err := validateBestEffortSourceOrder(req, call); err != nil {
			return nil, err
		}
		// Theft settlement classifies the immutable decision by the source
		// order's resolution time. In best-effort mode the signed/durable proof
		// does not exist, so the validated envelope is the trusted internal
		// source for that timestamp.
		call.TrustedSourceDueAtMs = call.BestEffortSourceOrder.GetDueAtMs()
		call.TrustedResolvedAtMs = call.BestEffortSourceOrder.GetDueAtMs()
	}
	if err := CheckParticipantBudget(ctx, call); err != nil {
		return nil, err
	}
	if options.RouteFenceVerifier != nil {
		proofCtx, cancel, err := ParticipantBudgetContext(ctx, call)
		if err != nil {
			return nil, err
		}
		err = options.RouteFenceVerifier.VerifyRoute(proofCtx, call)
		cancel()
		if err != nil {
			return nil, normalizeParticipantVerifierError(err)
		}
		if err := CheckParticipantBudget(ctx, call); err != nil {
			return nil, err
		}
	}
	if options.WorkAddressVerifier != nil && call.BestEffortSourceOrder == nil {
		proofCtx, cancel, err := ParticipantBudgetContext(ctx, call)
		if err != nil {
			return nil, err
		}
		err = options.WorkAddressVerifier.VerifyWorkAddress(proofCtx, call)
		cancel()
		if err != nil {
			return nil, normalizeParticipantVerifierError(err)
		}
		if err := CheckParticipantBudget(ctx, call); err != nil {
			return nil, err
		}
	}
	if options.IdentityBindingVerifier != nil && call.BestEffortSourceOrder == nil {
		proofCtx, cancel, err := ParticipantBudgetContext(ctx, call)
		if err != nil {
			return nil, err
		}
		err = options.IdentityBindingVerifier.VerifyIdentityBinding(proofCtx, call)
		cancel()
		if err != nil {
			return nil, normalizeParticipantVerifierError(err)
		}
		if err := CheckParticipantBudget(ctx, call); err != nil {
			return nil, err
		}
	}
	if strictPhysical && legacyRouteLeaseProof {
		observeLegacyParticipantRouteLeaseProof(callerKind)
	}
	return call, nil
}

// ParticipantBudgetContext gives an injected verifier a context bounded by
// the trusted remaining request budget. Verifiers must honor cancellation;
// CheckParticipantBudget remains the monotonic fake-clock/backstop check after
// the verifier returns.
func ParticipantBudgetContext(ctx context.Context, call *ParticipantCall) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := CheckParticipantBudget(ctx, call); err != nil {
		return nil, func() {}, err
	}
	bounded, cancel := context.WithTimeout(ctx, time.Duration(call.RemainingBudgetMs)*time.Millisecond)
	return bounded, cancel, nil
}

func cloneSourceOrderCommitProof(proof *contractspb.SourceOrderCommitProof) *contractspb.SourceOrderCommitProof {
	if proof == nil {
		return nil
	}
	return proto.Clone(proof).(*contractspb.SourceOrderCommitProof)
}

func cloneBestEffortSourceOrder(envelope *contractspb.CreatedSourceOrderEnvelope) *contractspb.CreatedSourceOrderEnvelope {
	if envelope == nil {
		return nil
	}
	return proto.Clone(envelope).(*contractspb.CreatedSourceOrderEnvelope)
}

func bestEffortSourceCommand(command contractspb.ParticipantCommandType) bool {
	switch command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT:
		return true
	default:
		return false
	}
}

func validateBestEffortSourceOrder(req *contractspb.ParticipantRequest, call *ParticipantCall) error {
	if req == nil || call == nil || call.BestEffortSourceOrder == nil || call.Work == nil {
		return status.Error(codes.InvalidArgument, "contracts: incomplete best-effort source order")
	}
	envelope := call.BestEffortSourceOrder
	if envelope.GetWork() == nil || !proto.Equal(envelope.GetWork(), call.Work) ||
		envelope.GetWorkVersion() == 0 || envelope.GetWorkVersion() != call.ExpectedWorkVersion ||
		envelope.GetOperationId() == "" || envelope.GetOperationId() != call.OperationID ||
		envelope.GetSubjectPlayerId() == 0 || envelope.GetSubjectPlayerId() != call.SubjectPlayerID ||
		envelope.GetDueAtMs() <= 0 || !bytes.Equal(envelope.GetTypedPayload(), req.GetTypedPayload()) ||
		len(envelope.GetPayloadHash()) != len(call.CanonicalStepPayloadHash) ||
		!bytes.Equal(envelope.GetPayloadHash(), call.CanonicalStepPayloadHash[:]) {
		return status.Error(codes.FailedPrecondition, "contracts: best-effort source order binding mismatch")
	}
	return nil
}

func normalizeParticipantVerifierError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "contracts: participant verifier budget exhausted")
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "contracts: participant verifier canceled")
	}
	return err
}

// ValidateTypedResult checks the registered result descriptor and produces
// the deterministic typed bytes/hash used by ParticipantResponse and the
// Actor-local receipt. A nil result remains legal for REJECTED/NO_CHANGE for
// compatibility with the frozen Fake contract; business handlers may choose
// to provide a typed result for every terminal outcome.
func ValidateTypedResult(spec ParticipantCommandSpec, result proto.Message) ([]byte, []byte, error) {
	if result == nil {
		return nil, nil, nil
	}
	wantSchema := spec.NewResult().ProtoReflect().Descriptor().FullName()
	gotSchema := result.ProtoReflect().Descriptor().FullName()
	if gotSchema != wantSchema {
		return nil, nil, status.Error(codes.Internal,
			fmt.Sprintf("contracts: typed result schema %s does not match registered %s", gotSchema, wantSchema))
	}
	typedResult, err := proto.MarshalOptions{Deterministic: true}.Marshal(result)
	if err != nil {
		return nil, nil, status.Error(codes.Internal, "contracts: typed result marshal failed")
	}
	hash, err := CanonicalParticipantHash(spec, result)
	if err != nil {
		return nil, nil, status.Error(codes.Internal, fmt.Sprintf("contracts: result hash: %v", err))
	}
	return typedResult, hash[:], nil
}

type callerShape struct {
	kind                ParticipantCallerKind
	work                *contractspb.WorkAddress
	step                contractspb.ParticipantStep
	stepName            identity.ParticipantStep
	invocationID        string
	operationID         string
	subjectPlayerID     uint64
	expectedWorkVersion uint64
	callerInstanceID    string
	callerBootID        string
	leaseOwnerToken     string
	leaseEpoch          uint64
	remainingBudgetMs   uint32
}

func extractCaller(req *contractspb.ParticipantRequest, want ParticipantCallerKind) (callerShape, error) {
	if want != ParticipantCallerCoordinator && want != ParticipantCallerWorkerRecovery {
		return callerShape{}, status.Error(codes.Internal, "contracts: invalid participant caller kind")
	}
	switch c := req.GetCallerContext().(type) {
	case *contractspb.ParticipantRequest_Coordinator:
		if want != ParticipantCallerCoordinator {
			return callerShape{}, status.Error(codes.InvalidArgument, "contracts: ResumeStep requires InternalCallerContext")
		}
		if c.Coordinator == nil {
			return callerShape{}, status.Error(codes.InvalidArgument, "contracts: missing CoordinatorWorkContext")
		}
		return callerShape{
			kind:                ParticipantCallerCoordinator,
			work:                c.Coordinator.GetWork(),
			step:                c.Coordinator.GetParticipantStep(),
			invocationID:        c.Coordinator.GetParticipantInvocationId(),
			operationID:         c.Coordinator.GetOperationId(),
			subjectPlayerID:     c.Coordinator.GetSubjectPlayerId(),
			expectedWorkVersion: c.Coordinator.GetExpectedWorkVersion(),
			remainingBudgetMs:   c.Coordinator.GetRemainingBudgetMs(),
		}, nil
	case *contractspb.ParticipantRequest_Recovery:
		if want != ParticipantCallerWorkerRecovery {
			return callerShape{}, status.Error(codes.InvalidArgument, "contracts: ExecuteStep requires CoordinatorWorkContext")
		}
		if c.Recovery == nil {
			return callerShape{}, status.Error(codes.InvalidArgument, "contracts: missing InternalCallerContext")
		}
		if c.Recovery.GetCallerType() != contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY {
			return callerShape{}, status.Error(codes.InvalidArgument, "contracts: unknown internal caller type")
		}
		return callerShape{
			kind:                ParticipantCallerWorkerRecovery,
			work:                c.Recovery.GetWork(),
			step:                c.Recovery.GetParticipantStep(),
			invocationID:        c.Recovery.GetParticipantInvocationId(),
			operationID:         "",
			subjectPlayerID:     0,
			expectedWorkVersion: c.Recovery.GetExpectedWorkVersion(),
			callerInstanceID:    c.Recovery.GetCallerInstanceId(),
			callerBootID:        c.Recovery.GetCallerBootId(),
			leaseOwnerToken:     c.Recovery.GetLeaseOwnerToken(),
			leaseEpoch:          c.Recovery.GetLeaseEpoch(),
			remainingBudgetMs:   c.Recovery.GetRemainingBudgetMs(),
		}, nil
	default:
		return callerShape{}, status.Error(codes.InvalidArgument, "contracts: missing caller context")
	}
}

func validateCallerShape(c callerShape) error {
	if c.work == nil {
		return status.Error(codes.InvalidArgument, "contracts: missing work address")
	}
	if c.expectedWorkVersion == 0 {
		return status.Error(codes.InvalidArgument, "contracts: expected work version must be positive")
	}
	if !validOpaqueASCII(c.invocationID, 128) {
		return status.Error(codes.InvalidArgument, "contracts: participant invocation id required")
	}
	if c.kind == ParticipantCallerCoordinator {
		if !validOpaqueASCII(c.operationID, 64) || c.subjectPlayerID == 0 {
			return status.Error(codes.InvalidArgument, "contracts: coordinator operation and subject are required")
		}
		return nil
	}
	if !validBoundText(c.callerInstanceID, 128) || !validBoundText(c.callerBootID, 128) ||
		!validBoundText(c.leaseOwnerToken, 256) || c.leaseEpoch == 0 {
		return status.Error(codes.InvalidArgument, "contracts: worker caller and lease identity are required")
	}
	return nil
}

func validBoundText(value string, maxBytes int) bool {
	return value != "" && len(value) <= maxBytes && utf8.ValidString(value) && strings.TrimSpace(value) == value
}

func validOpaqueASCII(value string, maxBytes int) bool {
	if !validBoundText(value, maxBytes) {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 0x21 || value[i] > 0x7e {
			return false
		}
	}
	return true
}

func effectiveParticipantBudget(ctx context.Context, requested, commandMax uint32, now time.Time) (uint32, error) {
	if requested == 0 {
		return 0, status.Error(codes.InvalidArgument, "contracts: remaining participant budget must be positive")
	}
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return 0, status.Error(codes.DeadlineExceeded, "contracts: participant deadline/cancellation exhausted")
		}
		return 0, status.Error(codes.Canceled, "contracts: participant deadline/cancellation exhausted")
	}
	effective := requested
	if commandMax == 0 {
		return 0, status.Error(codes.Internal, "contracts: participant command has no budget maximum")
	}
	if effective > commandMax {
		effective = commandMax
	}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := deadline.Sub(now)
		if remaining <= 0 {
			return 0, status.Error(codes.DeadlineExceeded, "contracts: participant remaining budget exceeds deadline")
		}
		deadlineMs := uint64(remaining / time.Millisecond)
		if deadlineMs == 0 {
			return 0, status.Error(codes.DeadlineExceeded, "contracts: participant deadline/cancellation exhausted")
		}
		if uint64(effective) > deadlineMs {
			effective = uint32(deadlineMs)
		}
	}
	if effective == 0 {
		return 0, status.Error(codes.DeadlineExceeded, "contracts: participant budget exhausted")
	}
	return effective, nil
}

// CheckParticipantBudget subtracts time spent by the ingress kernel and by
// the Player Participant's WorkVersion/WorkLease verifier. It must be called
// immediately before mailbox admission and is safe to call after every proof
// seam. A verifier may only consume budget; it can never replenish it.
func CheckParticipantBudget(ctx context.Context, call *ParticipantCall) error {
	if call == nil || call.budgetLimitMs == 0 || call.now == nil {
		return status.Error(codes.Internal, "contracts: participant budget bookkeeping is missing")
	}
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return status.Error(codes.DeadlineExceeded, "contracts: participant deadline/cancellation exhausted")
		}
		return status.Error(codes.Canceled, "contracts: participant deadline/cancellation exhausted")
	}
	now := call.now()
	elapsed := now.Sub(call.budgetStart)
	if elapsed < 0 {
		elapsed = 0
	}
	elapsedMs := uint64(elapsed / time.Millisecond)
	if elapsedMs >= uint64(call.budgetLimitMs) {
		call.RemainingBudgetMs = 0
		return status.Error(codes.DeadlineExceeded, "contracts: participant budget exhausted before mailbox")
	}
	remaining := uint32(uint64(call.budgetLimitMs) - elapsedMs)
	if deadline, ok := ctx.Deadline(); ok {
		deadlineRemaining := deadline.Sub(now)
		if deadlineRemaining <= 0 {
			call.RemainingBudgetMs = 0
			return status.Error(codes.DeadlineExceeded, "contracts: participant deadline/cancellation exhausted")
		}
		deadlineMs := uint64(deadlineRemaining / time.Millisecond)
		if deadlineMs == 0 {
			call.RemainingBudgetMs = 0
			return status.Error(codes.DeadlineExceeded, "contracts: participant deadline/cancellation exhausted")
		}
		if deadlineMs < uint64(remaining) {
			remaining = uint32(deadlineMs)
		}
	}
	if remaining == 0 {
		call.RemainingBudgetMs = 0
		return status.Error(codes.DeadlineExceeded, "contracts: participant budget exhausted before mailbox")
	}
	call.RemainingBudgetMs = remaining
	return nil
}

// validateRouteHeader enforces the registered participant authority and rejects
// partial route fences. Production adapters require holder/boot/shard/writer.
// During compatibility rollout they validate a complete lease tuple when it is
// present but may accept it fully absent; requireLease enables strict mode.
func validateRouteHeader(spec ParticipantCommandSpec, req *contractspb.ParticipantRequest, requirePhysical, requireLease bool) (bool, error) {
	route := req.GetRouteHeader()
	if route == nil {
		return false, status.Error(codes.InvalidArgument, "contracts: missing route header")
	}
	if route.GetActorId() == 0 {
		return false, status.Error(codes.InvalidArgument, "contracts: route header actor id must be positive")
	}
	if route.GetRouteDomain() != spec.TargetRouteDomain {
		return false, status.Error(codes.InvalidArgument, "contracts: route domain does not match participant command target")
	}
	holderID := route.GetExpectedHolderId()
	holderBootID := route.GetExpectedHolderBootId()
	hasHolder := holderID != "" || holderBootID != ""
	hasLeaseFence := route.GetExpectedLeaseEpoch() != 0 || route.ExpectedLeaseUntilMs != nil
	if hasHolder {
		if !validBoundText(holderID, 128) || !validBoundText(holderBootID, 128) || route.GetShardRevision() == 0 || route.GetExpectedWriterEpoch() == 0 {
			return false, status.Error(codes.InvalidArgument, "contracts: route holder fence is incomplete")
		}
		if hasLeaseFence && (route.GetExpectedLeaseEpoch() == 0 || route.ExpectedLeaseUntilMs == nil || route.GetExpectedLeaseUntilMs() == 0) {
			return false, status.Error(codes.InvalidArgument, "contracts: route lease fence is incomplete")
		}
		if requireLease && !hasLeaseFence {
			return false, status.Error(codes.InvalidArgument, "contracts: route lease fence is required")
		}
	} else if requirePhysical {
		return false, status.Error(codes.InvalidArgument, "contracts: route holder fence is required")
	} else if route.GetShardRevision() != 0 || route.GetExpectedWriterEpoch() != 0 || hasLeaseFence {
		return false, status.Error(codes.InvalidArgument, "contracts: route generation fence has no holder")
	}
	hasSource := route.SourceSnapshotRevision != nil || route.GetSourceInstanceId() != "" || route.GetSourceBootId() != ""
	if hasSource {
		if route.SourceSnapshotRevision == nil || route.GetSourceSnapshotRevision() == 0 ||
			!validBoundText(route.GetSourceInstanceId(), 128) || !validBoundText(route.GetSourceBootId(), 128) {
			return false, status.Error(codes.InvalidArgument, "contracts: route source fence is incomplete")
		}
	}
	return hasHolder && !hasLeaseFence, nil
}

// validateStepAndInvocation enforces the frozen WorkAddress registration.
// Physical bucket hashing is intentionally left to the W31-A repository
// adapter; this kernel validates the semantic WorkType/placement/source shape.
func validateStepAndInvocation(spec ParticipantCommandSpec, caller callerShape) error {
	stepName, err := StableStepName(caller.step)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	caller.stepName = identity.ParticipantStep(stepName)
	if caller.stepName != spec.Step {
		return status.Error(codes.InvalidArgument, "contracts: participant step does not match command")
	}
	work := caller.work
	workTypeName, err := StableWorkTypeName(work.GetWorkType())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if identity.WorkType(workTypeName) != spec.WorkType {
		return status.Error(codes.InvalidArgument, "contracts: work type does not match participant command")
	}
	workSpec, err := LookupWorkType(spec.WorkType)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if work.GetPlacementKind() != workSpec.Placement {
		migrated, migrateErr := sourceOrderMigrationSpec(spec.WorkType, work.GetPlacementKind(), workSpec)
		if migrateErr != nil {
			return status.Error(codes.InvalidArgument, "contracts: work placement mismatch")
		}
		workSpec = migrated
	}
	if work.GetBucketVersion() == 0 {
		return status.Error(codes.InvalidArgument, "contracts: work bucket version must be positive")
	}
	if err := workidentity.ValidateStableOpaqueID(work.GetWorkId()); err != nil {
		return status.Error(codes.InvalidArgument, "contracts: work id is not a valid stable opaque id")
	}
	if err := validateSourceAuthority(workSpec, work.GetSourceAuthority()); err != nil {
		return err
	}
	stepAllowed := false
	for _, allowed := range workSpec.Steps {
		if allowed == spec.Step {
			stepAllowed = true
			break
		}
	}
	if !stepAllowed {
		return status.Error(codes.InvalidArgument, "contracts: step not allowed for work type")
	}
	want, err := identity.ParticipantInvocationID(spec.WorkType, work.GetWorkId(), spec.Step, 1)
	if err != nil {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("contracts: invocation derivation: %v", err))
	}
	if caller.invocationID != want {
		return status.Error(codes.InvalidArgument, "contracts: participant invocation binding mismatch")
	}
	return nil
}

func sourceOrderMigrationSpec(workType identity.WorkType, placement contractspb.WorkPlacementKind, base WorkTypeSpec) (WorkTypeSpec, error) {
	if placement != contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET {
		return WorkTypeSpec{}, fmt.Errorf("contracts: work placement mismatch")
	}
	var kinds []identity.SourceAuthorityKind
	switch workType {
	case identity.WorkTypeHarvest, identity.WorkTypeTheft:
		kinds = []identity.SourceAuthorityKind{identity.SourceKindFarmActor}
	case identity.WorkTypePlant, identity.WorkTypeUnlockPlot:
		kinds = []identity.SourceAuthorityKind{identity.SourceKindPlayerActor}
	default:
		return WorkTypeSpec{}, fmt.Errorf("contracts: work placement mismatch")
	}
	migrated := base
	migrated.Placement = placement
	migrated.AllowedSourceKinds = kinds
	return migrated, nil
}

func validateSourceAuthority(workSpec WorkTypeSpec, source *contractspb.SourceAuthorityRef) error {
	if workSpec.Placement == contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_INDEPENDENT_WORK_BUCKET {
		if source != nil {
			return status.Error(codes.InvalidArgument, "contracts: independent work must not carry source authority")
		}
		return nil
	}
	if source == nil {
		return status.Error(codes.InvalidArgument, "contracts: source persistence bucket requires source authority")
	}
	kindName, ok := contractspb.SourceAuthorityKind_name[int32(source.GetAuthorityKind())]
	if !ok || source.GetAuthorityKind() == contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_UNSPECIFIED {
		return status.Error(codes.InvalidArgument, "contracts: unknown source authority kind")
	}
	kind := identity.SourceAuthorityKind(strings.TrimPrefix(kindName, "SOURCE_AUTHORITY_KIND_"))
	kindAllowed := false
	for _, allowed := range workSpec.AllowedSourceKinds {
		if allowed == kind {
			kindAllowed = true
			break
		}
	}
	if !kindAllowed {
		return status.Error(codes.InvalidArgument, "contracts: source authority kind not allowed for work type")
	}
	if !validBoundText(source.GetCanonicalAuthorityId(), 128) {
		return status.Error(codes.InvalidArgument, "contracts: source authority canonical id required")
	}
	if !validBoundText(source.GetAuthorityLayoutVersion(), 64) {
		return status.Error(codes.InvalidArgument, "contracts: source authority layout version required")
	}
	switch kind {
	case identity.SourceKindPlayerActor:
		if source.GetActorRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER {
			return status.Error(codes.InvalidArgument, "contracts: player actor source requires PLAYER route domain")
		}
		if err := validateCanonicalActorID(source.GetCanonicalAuthorityId()); err != nil {
			return err
		}
	case identity.SourceKindFarmActor:
		if source.GetActorRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM {
			return status.Error(codes.InvalidArgument, "contracts: farm actor source requires FARM route domain")
		}
		if err := validateCanonicalActorID(source.GetCanonicalAuthorityId()); err != nil {
			return err
		}
	case identity.SourceKindFriendPair:
		if source.ActorRouteDomain != nil {
			return status.Error(codes.InvalidArgument, "contracts: friend pair source must not carry actor route domain")
		}
		if err := workidentity.ValidateStableOpaqueID(source.GetCanonicalAuthorityId()); err != nil {
			return status.Error(codes.InvalidArgument, "contracts: friend pair source id is not canonical")
		}
	}
	return nil
}

func validateCanonicalActorID(value string) error {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed == 0 || strconv.FormatUint(parsed, 10) != value {
		return status.Error(codes.InvalidArgument, "contracts: actor source id is not canonical decimal")
	}
	return nil
}

func participantActorKey(req *contractspb.ParticipantRequest) string {
	route := req.GetRouteHeader()
	return fmt.Sprintf("%d/%d", int32(route.GetRouteDomain()), route.GetActorId())
}

// participantPayloadActorID is part of the shared ingress kernel rather than
// a Player/Farm adapter: every registered Participant schema has one trusted
// actor target field. The target domain in the Catalog selects whether that
// field is a player or farm id; no caller can route a typed payload to a
// different actor mailbox.
func participantPayloadActorID(payload proto.Message) (uint64, bool) {
	switch payload := payload.(type) {
	case *contractspb.ReserveUnlockCoinPayload:
		return payload.GetPlayerId(), true
	case *contractspb.CommitPlotUnlockPayload:
		return payload.GetFarmId(), true
	case *contractspb.CommitUnlockCoinReservationPayload:
		return payload.GetPlayerId(), true
	case *contractspb.ReleaseUnlockCoinReservationPayload:
		return payload.GetPlayerId(), true
	case *contractspb.ReserveSeedPayload:
		return payload.GetPlayerId(), true
	case *contractspb.CommitPlantPayload:
		return payload.GetFarmId(), true
	case *contractspb.CommitSeedReservationPayload:
		return payload.GetPlayerId(), true
	case *contractspb.ReleaseSeedReservationPayload:
		return payload.GetPlayerId(), true
	case *contractspb.CommitHarvestPayload:
		return payload.GetFarmId(), true
	case *contractspb.GrantHarvestRewardPayload:
		return payload.GetPlayerId(), true
	case *contractspb.DecideTheftPayload:
		return payload.GetFarmId(), true
	case *contractspb.SettleTheftVisitorPayload:
		return payload.GetVisitorPlayerId(), true
	case *contractspb.CreditGuardIncomePayload:
		return payload.GetOwnerPlayerId(), true
	case *contractspb.ApplyCareRewardPayload:
		return payload.GetCarerPlayerId(), true
	case *contractspb.ApplyGuardSnapshotPayload:
		return payload.GetTargetFarmId(), true
	default:
		return 0, false
	}
}

// participantPayloadSubjectPlayerID returns the immutable player subject
// carried by the registered typed schema. It is deliberately separate from
// participantPayloadActorID: Farm-routed commands target a farm actor but
// still carry the player whose WorkRecord authorized the step.
func participantPayloadSubjectPlayerID(payload proto.Message) (uint64, bool) {
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
