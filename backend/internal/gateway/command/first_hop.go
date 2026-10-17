package command

import (
	"context"
	"errors"
	"fmt"

	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"google.golang.org/protobuf/proto"
)

var (
	ErrFirstHopInvalid       = errors.New("gate: invalid FirstHop route header")
	ErrFirstHopIncomplete    = errors.New("gate: incomplete FirstHop route header")
	ErrFirstHopStale         = errors.New("gate: stale FirstHop route")
	ErrFirstHopNotHolder     = errors.New("gate: FirstHop holder mismatch")
	ErrFirstHopLease         = errors.New("gate: FirstHop local lease denied")
	ErrFirstHopShardMismatch = errors.New("gate: FirstHop virtual shard mismatch")
)

// LeaseAdmission is the local, process-only lease gate used before
// Execution-Accept/Actor Mailbox. A false result must never be repaired by a
// remote lookup from this hot path.
type LeaseAdmission interface {
	CanStartActorTurn(actorrouting.RouteDomain, uint32) bool
}

// RouteGenerationAdmission is the required holder-side guard seam. A boolean
// lease check alone cannot prove that the resolver assignment and local guard
// are the same generation.
type RouteGenerationAdmission interface {
	ValidateRouteGeneration(domain actorrouting.RouteDomain, shard uint32, holderID, holderBootID string, assignmentRevision, writerEpoch, leaseEpoch uint64) bool
}

// LeaseAdmissionFunc adapts a local guard registry or runtime to LeaseAdmission.
type LeaseAdmissionFunc func(actorrouting.RouteDomain, uint32) bool

func (f LeaseAdmissionFunc) CanStartActorTurn(domain actorrouting.RouteDomain, shard uint32) bool {
	return f != nil && f(domain, shard)
}

type FirstHopRouteValidatorOptions struct {
	Realm          string
	Resolver       actorrouting.Resolver
	HolderID       string
	HolderBootID   string
	LeaseAdmission LeaseAdmission
}

// FirstHopRouteValidator is the holder-side pre-mailbox route fence. It
// validates the complete server-generated header against the latest local
// assignment and local LeaseGuard before an actor turn can be accepted.
type FirstHopRouteValidator struct {
	realm          string
	resolver       actorrouting.Resolver
	holderID       string
	holderBootID   string
	leaseAdmission LeaseAdmission
}

func NewFirstHopRouteValidator(opts FirstHopRouteValidatorOptions) (*FirstHopRouteValidator, error) {
	if opts.Realm == "" || opts.Resolver == nil || opts.HolderID == "" || opts.HolderBootID == "" || opts.LeaseAdmission == nil {
		return nil, fmt.Errorf("%w: realm/resolver/holder identity/local lease required", ErrFirstHopInvalid)
	}
	if _, ok := opts.LeaseAdmission.(RouteGenerationAdmission); !ok {
		return nil, fmt.Errorf("%w: complete local lease generation admission required", ErrFirstHopInvalid)
	}
	return &FirstHopRouteValidator{
		realm: opts.Realm, resolver: opts.Resolver, holderID: opts.HolderID,
		holderBootID: opts.HolderBootID, leaseAdmission: opts.LeaseAdmission,
	}, nil
}

// FirstHopError keeps a fail-closed cause and, when a newer local assignment
// is known, a Redirect that the caller may apply with the existing bounded
// RetryingResolver. It never performs forwarding itself.
type FirstHopError struct {
	Cause    error
	Redirect *actorrouting.RouteRedirect
}

func (e *FirstHopError) Error() string {
	if e == nil || e.Cause == nil {
		return ErrFirstHopInvalid.Error()
	}
	return e.Cause.Error()
}

func (e *FirstHopError) Unwrap() error {
	if e == nil {
		return ErrFirstHopInvalid
	}
	return e.Cause
}

func AsFirstHopError(err error) (*FirstHopError, bool) {
	var firstHop *FirstHopError
	if errors.As(err, &firstHop) {
		return firstHop, true
	}
	return nil, false
}

// Validate performs all route checks before the caller enters Execution-Accept
// or an Actor mailbox.
func (v *FirstHopRouteValidator) Validate(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest) (actorrouting.Assignment, error) {
	if v == nil || v.resolver == nil || v.leaseAdmission == nil {
		return actorrouting.Assignment{}, &FirstHopError{Cause: ErrFirstHopInvalid}
	}
	if ctx == nil {
		return actorrouting.Assignment{}, &FirstHopError{Cause: ErrFirstHopInvalid}
	}
	// Resolver implementations are allowed to be local/static and therefore
	// may not inspect ctx themselves.  The admission boundary must reject an
	// already-cancelled request before doing even local route work.
	if err := ctx.Err(); err != nil {
		return actorrouting.Assignment{}, &FirstHopError{Cause: err}
	}
	if req == nil || req.Dispatch == nil || req.Dispatch.GetRouteMode() != logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY || req.Dispatch.GetActorRoute() == nil {
		return actorrouting.Assignment{}, &FirstHopError{Cause: ErrFirstHopIncomplete}
	}
	header := req.Dispatch.GetActorRoute()
	domain, err := routeDomain(header.GetRouteDomain())
	if err != nil || header.GetActorId() == 0 {
		return actorrouting.Assignment{}, &FirstHopError{Cause: ErrFirstHopInvalid}
	}
	address := actorrouting.Address{Realm: v.realm, Kind: actorrouting.Kind(domain), ID: header.GetActorId()}
	assignment, err := v.resolver.Resolve(ctx, address)
	if err != nil {
		return actorrouting.Assignment{}, &FirstHopError{Cause: fmt.Errorf("%w: resolve: %v", ErrFirstHopStale, err), Redirect: nil}
	}
	if assignment.HolderPod == "" || assignment.HolderBootID == "" || assignment.ShardRevision == 0 || assignment.WriterEpoch == 0 || assignment.LeaseEpoch == 0 || assignment.AssignmentRevision == 0 {
		return actorrouting.Assignment{}, &FirstHopError{Cause: ErrFirstHopIncomplete}
	}
	if header.GetVirtualShard() != assignment.ShardID {
		return actorrouting.Assignment{}, v.staleError(ErrFirstHopShardMismatch, assignment, domain, header.GetActorId())
	}
	if header.GetShardRevision() == 0 || header.GetExpectedHolderId() == "" || header.GetExpectedHolderBootId() == "" || header.GetExpectedWriterEpoch() == 0 {
		return actorrouting.Assignment{}, &FirstHopError{Cause: ErrFirstHopIncomplete, Redirect: v.redirect(assignment, domain, header.GetActorId())}
	}
	if header.GetShardRevision() != assignment.ShardRevision || header.GetExpectedHolderId() != assignment.HolderPod || header.GetExpectedHolderBootId() != assignment.HolderBootID || header.GetExpectedWriterEpoch() != assignment.WriterEpoch {
		return actorrouting.Assignment{}, v.staleError(ErrFirstHopStale, assignment, domain, header.GetActorId())
	}
	if header.GetExpectedLeaseEpoch() != 0 && header.GetExpectedLeaseEpoch() != assignment.LeaseEpoch {
		return actorrouting.Assignment{}, v.staleError(ErrFirstHopStale, assignment, domain, header.GetActorId())
	}
	if expectedLeaseUntil := header.GetExpectedLeaseUntilMs(); expectedLeaseUntil != 0 && assignment.LeaseUntilMs <= 0 {
		return actorrouting.Assignment{}, v.staleError(ErrFirstHopStale, assignment, domain, header.GetActorId())
	}
	if sourceRevision := header.GetSourceSnapshotRevision(); sourceRevision != 0 && sourceRevision != assignment.SnapshotRevision && !routeFenceHeaderMatchesAssignment(header, assignment) {
		return actorrouting.Assignment{}, v.staleError(ErrFirstHopStale, assignment, domain, header.GetActorId())
	}
	if (header.GetSourceInstanceId() == "") != (header.GetSourceBootId() == "") {
		return actorrouting.Assignment{}, &FirstHopError{Cause: ErrFirstHopIncomplete, Redirect: v.redirect(assignment, domain, header.GetActorId())}
	}
	if assignment.HolderPod != v.holderID || assignment.HolderBootID != v.holderBootID {
		return actorrouting.Assignment{}, v.staleError(ErrFirstHopNotHolder, assignment, domain, header.GetActorId())
	}
	generationAdmission, ok := v.leaseAdmission.(RouteGenerationAdmission)
	// Check immediately before the local generation gate: a resolver that
	// ignores context must not turn a cancellation race into mailbox entry.
	if err := ctx.Err(); err != nil {
		return actorrouting.Assignment{}, &FirstHopError{Cause: err}
	}
	if !ok || !generationAdmission.ValidateRouteGeneration(domain, assignment.ShardID, assignment.HolderPod, assignment.HolderBootID, assignment.AssignmentRevision, assignment.WriterEpoch, assignment.LeaseEpoch) {
		return actorrouting.Assignment{}, &FirstHopError{Cause: ErrFirstHopLease}
	}
	// Admission may itself race with cancellation.  The final check is the
	// last operation before the caller is allowed to execute/enter a mailbox.
	if err := ctx.Err(); err != nil {
		return actorrouting.Assignment{}, &FirstHopError{Cause: err}
	}
	return assignment, nil
}

// ValidateBeforeAccept is an explicit name for transport adapters that call
// the validator at the Execution-Accept boundary.
func (v *FirstHopRouteValidator) ValidateBeforeAccept(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest) (actorrouting.Assignment, error) {
	return v.Validate(ctx, req)
}

func (v *FirstHopRouteValidator) staleError(cause error, assignment actorrouting.Assignment, domain actorrouting.RouteDomain, actorID uint64) error {
	return &FirstHopError{Cause: cause, Redirect: v.redirect(assignment, domain, actorID)}
}

func (v *FirstHopRouteValidator) redirect(assignment actorrouting.Assignment, domain actorrouting.RouteDomain, actorID uint64) *actorrouting.RouteRedirect {
	redirect := &actorrouting.RouteRedirect{
		Domain:               domain,
		ActorID:              actorID,
		ShardID:              assignment.ShardID,
		GlobalRevision:       assignment.SnapshotRevision,
		ShardRevision:        assignment.ShardRevision,
		ExpectedHolderID:     assignment.HolderPod,
		ExpectedHolderBootID: assignment.HolderBootID,
		ExpectedWriterEpoch:  assignment.WriterEpoch,
		ExpectedLeaseEpoch:   assignment.LeaseEpoch,
		LeaseUntilMs:         assignment.LeaseUntilMs,
		Reason:               actorrouting.RedirectReasonStaleRoute,
	}
	if source, ok := v.resolver.(interface {
		Snapshot() actorrouting.RouteSnapshot
	}); ok {
		snapshot := source.Snapshot()
		redirect.Snapshot = &snapshot
		redirect.GlobalRevision = snapshot.GlobalRevision
		for index, holder := range snapshot.Holders {
			if holder.InstanceID == assignment.HolderPod && holder.BootID == assignment.HolderBootID {
				redirect.NewHolderIndex = index
				break
			}
		}
	}
	return redirect
}

func routeFenceHeaderMatchesAssignment(header *logicrpcpb.ActorRouteHeader, assignment actorrouting.Assignment) bool {
	return header != nil && header.GetVirtualShard() == assignment.ShardID &&
		header.GetShardRevision() != 0 && header.GetShardRevision() == assignment.ShardRevision &&
		header.GetExpectedHolderId() != "" && header.GetExpectedHolderId() == assignment.HolderPod &&
		header.GetExpectedHolderBootId() != "" && header.GetExpectedHolderBootId() == assignment.HolderBootID &&
		header.GetExpectedWriterEpoch() != 0 && header.GetExpectedWriterEpoch() == assignment.WriterEpoch &&
		header.GetExpectedLeaseEpoch() != 0 && header.GetExpectedLeaseEpoch() == assignment.LeaseEpoch &&
		(header.GetExpectedLeaseUntilMs() == 0 || assignment.LeaseUntilMs > 0)
}

func routeDomain(domain logicrpcpb.RouteDomain) (actorrouting.RouteDomain, error) {
	switch domain {
	case logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER:
		return actorrouting.DomainPlayer, nil
	case logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM:
		return actorrouting.DomainFarm, nil
	default:
		return "", ErrFirstHopInvalid
	}
}

// WithResolvedActorRoute is the Gate-side completion seam used by
// ShardedLogicClient. It copies the request, verifies any route observation
// supplied by an upstream trusted hop, and fills the complete server-owned
// header from the current Assignment.
func WithResolvedActorRoute(req *logicrpcpb.ExecuteCommandRequest, assignment actorrouting.Assignment) (*logicrpcpb.ExecuteCommandRequest, error) {
	if req == nil || req.Dispatch == nil || req.Dispatch.ActorRoute == nil {
		return nil, ErrFirstHopIncomplete
	}
	header := req.Dispatch.ActorRoute
	domain, err := routeDomain(header.GetRouteDomain())
	if err != nil || header.GetActorId() == 0 || actorrouting.Kind(domain) != assignment.Kind {
		return nil, fmt.Errorf("%w: actor address", ErrFirstHopInvalid)
	}
	if header.GetVirtualShard() != 0 && header.GetVirtualShard() != assignment.ShardID {
		return nil, fmt.Errorf("%w: virtual shard", ErrFirstHopStale)
	}
	if header.GetShardRevision() != 0 && header.GetShardRevision() != assignment.ShardRevision {
		return nil, fmt.Errorf("%w: shard revision", ErrFirstHopStale)
	}
	if header.GetExpectedHolderId() != "" && header.GetExpectedHolderId() != assignment.HolderPod {
		return nil, fmt.Errorf("%w: holder", ErrFirstHopStale)
	}
	if header.GetExpectedHolderBootId() != "" && header.GetExpectedHolderBootId() != assignment.HolderBootID {
		return nil, fmt.Errorf("%w: holder boot", ErrFirstHopStale)
	}
	if header.GetExpectedWriterEpoch() != 0 && header.GetExpectedWriterEpoch() != assignment.WriterEpoch {
		return nil, fmt.Errorf("%w: writer epoch", ErrFirstHopStale)
	}
	if header.GetExpectedLeaseEpoch() != 0 && header.GetExpectedLeaseEpoch() != assignment.LeaseEpoch {
		return nil, fmt.Errorf("%w: lease epoch", ErrFirstHopStale)
	}
	if expectedLeaseUntil := header.GetExpectedLeaseUntilMs(); expectedLeaseUntil != 0 && assignment.LeaseUntilMs <= 0 {
		return nil, fmt.Errorf("%w: lease deadline unavailable", ErrFirstHopStale)
	}
	if sourceRevision := header.GetSourceSnapshotRevision(); sourceRevision != 0 && sourceRevision != assignment.SnapshotRevision && !routeFenceHeaderMatchesAssignment(header, assignment) {
		return nil, fmt.Errorf("%w: source snapshot", ErrFirstHopStale)
	}
	forward, ok := proto.Clone(req).(*logicrpcpb.ExecuteCommandRequest)
	if !ok || forward.Dispatch == nil || forward.Dispatch.ActorRoute == nil {
		return nil, fmt.Errorf("%w: clone dispatch", ErrFirstHopInvalid)
	}
	completed := forward.Dispatch.ActorRoute
	completed.VirtualShard = assignment.ShardID
	completed.ShardRevision = assignment.ShardRevision
	completed.ExpectedHolderId = assignment.HolderPod
	completed.ExpectedHolderBootId = assignment.HolderBootID
	completed.ExpectedWriterEpoch = assignment.WriterEpoch
	completed.ExpectedLeaseEpoch = assignment.LeaseEpoch
	if assignment.LeaseUntilMs > 0 {
		leaseUntil := uint64(assignment.LeaseUntilMs)
		completed.ExpectedLeaseUntilMs = &leaseUntil
	}
	completed.SourceInstanceId = ""
	completed.SourceBootId = ""
	completed.SourceSnapshotRevision = nil
	if assignment.SnapshotRevision > 0 {
		revision := assignment.SnapshotRevision
		completed.SourceSnapshotRevision = &revision
	}
	return forward, nil
}
