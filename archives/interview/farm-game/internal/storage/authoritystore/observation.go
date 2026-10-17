package authoritystore

import (
	"context"
	"time"
)

// CommitObservation contains byte accounting derived only from the exact
// request/result crossing ActorAuthorityStore.Commit. It is informational:
// neither the observer nor any value it returns can participate in a storage
// decision.
type CommitObservation struct {
	Domain                  string
	StableCommand           string
	Status                  string
	ActorRecordBytes        int
	CommitRequestBytes      int
	CommitResponseBytes     int
	ReceiptBytes            int
	OrderBytes              []int
	OrderPayloadBytes       int
	ScheduleBytes           []int
	ModelStateBytes         int
	PhysicalCommitBytes     int
	SerializationDuration   time.Duration
	PhysicalProjectionKnown bool
}

// CommitObserver starts one observation scope. Implementations may attach
// read-only tracing state to ctx (for example an actual Redis round-trip
// counter). The completion callback has no return value by design.
type CommitObserver interface {
	BeginAuthorityCommit(context.Context, string, string) (context.Context, func(CommitObservation))
}

type observedActorStore struct {
	inner    ActorAuthorityStore
	observer CommitObserver
}

type observedStoreWithCapabilities struct {
	*observedActorStore
	capabilities PhysicalCapabilitiesProvider
}

type observedStoreWithProjector struct {
	*observedActorStore
	projector PhysicalCommitUsageProjector
}

type observedStoreWithPhysicalContracts struct {
	*observedActorStore
	capabilities PhysicalCapabilitiesProvider
	projector    PhysicalCommitUsageProjector
}

type observedStoreWithExact struct{ *observedActorStore }
type observedStoreWithCapabilitiesAndExact struct{ *observedStoreWithCapabilities }
type observedStoreWithProjectorAndExact struct{ *observedStoreWithProjector }
type observedStoreWithPhysicalContractsAndExact struct {
	*observedStoreWithPhysicalContracts
}

// NewObservedActorStore decorates only the ActorAuthorityStore calls. A nil
// observer returns inner unchanged. The decorator delegates all decisions and
// preserves the optional physical capability/projector contracts used by
// Participant admission and the explicit exact-commit capability.
func NewObservedActorStore(inner ActorAuthorityStore, observer CommitObserver) ActorAuthorityStore {
	if inner == nil || observer == nil {
		return inner
	}
	observed := &observedActorStore{inner: inner, observer: observer}
	capabilities, hasCapabilities := inner.(PhysicalCapabilitiesProvider)
	projector, hasProjector := inner.(PhysicalCommitUsageProjector)
	_, hasExact := inner.(ExactCommitter)
	switch {
	case hasCapabilities && hasProjector && hasExact:
		base := &observedStoreWithPhysicalContracts{observedActorStore: observed, capabilities: capabilities, projector: projector}
		return &observedStoreWithPhysicalContractsAndExact{observedStoreWithPhysicalContracts: base}
	case hasCapabilities && hasProjector:
		return &observedStoreWithPhysicalContracts{observedActorStore: observed, capabilities: capabilities, projector: projector}
	case hasCapabilities && hasExact:
		base := &observedStoreWithCapabilities{observedActorStore: observed, capabilities: capabilities}
		return &observedStoreWithCapabilitiesAndExact{observedStoreWithCapabilities: base}
	case hasCapabilities:
		return &observedStoreWithCapabilities{observedActorStore: observed, capabilities: capabilities}
	case hasProjector && hasExact:
		base := &observedStoreWithProjector{observedActorStore: observed, projector: projector}
		return &observedStoreWithProjectorAndExact{observedStoreWithProjector: base}
	case hasProjector:
		return &observedStoreWithProjector{observedActorStore: observed, projector: projector}
	case hasExact:
		return &observedStoreWithExact{observedActorStore: observed}
	default:
		return observed
	}
}

func (s *observedActorStore) LoadAndClaim(ctx context.Context, req LoadAndClaimRequest) (LoadAndClaimResult, error) {
	return s.inner.LoadAndClaim(ctx, req)
}

func (s *observedActorStore) Commit(ctx context.Context, req AuthorityCommitRequest) (CommitResult, error) {
	return s.commit(ctx, req, false, false)
}

// CommitAuditedClaim preserves the optional hot-claim contract through the
// non-adjudicating observation decorator. An inner Store without the optional
// capability falls back to ordinary Commit, so exposing this method can never
// weaken validation.
func (s *observedActorStore) CommitAuditedClaim(ctx context.Context, req AuthorityCommitRequest) (CommitResult, error) {
	return s.commit(ctx, req, true, false)
}

func (s *observedActorStore) AdmissionSnapshotReuseSafe() bool {
	capability, ok := s.inner.(AdmissionSnapshotReuseCapable)
	return ok && capability.AdmissionSnapshotReuseSafe()
}

func (s *observedActorStore) commit(ctx context.Context, req AuthorityCommitRequest, auditedClaim, exact bool) (result CommitResult, err error) {
	domain := string(req.ActorAddress.Domain)
	command := req.IdempotencyKey.StableCommandTypeName
	observedCtx, complete := s.observer.BeginAuthorityCommit(ctx, domain, command)
	if observedCtx == nil {
		observedCtx = ctx
	}

	started := time.Now()
	observation := CommitObservation{
		Domain:             domain,
		StableCommand:      command,
		ActorRecordBytes:   len(req.CandidateState["record"]),
		CommitRequestBytes: AuthorityCommitSerializedBytes(req),
		ReceiptBytes:       AuthorityReceiptSerializedBytes(req.Receipt),
		ModelStateBytes:    ModelStateSerializedBytes(req.CandidateState),
		OrderBytes:         make([]int, 0, len(req.AttachedOrders)),
		ScheduleBytes:      make([]int, 0, len(req.AttachedSchedules)),
	}
	for _, order := range req.AttachedOrders {
		observation.OrderBytes = append(observation.OrderBytes, AttachedOrderSerializedBytes(order))
		observation.OrderPayloadBytes += len(order.Payload)
	}
	for _, schedule := range req.AttachedSchedules {
		observation.ScheduleBytes = append(observation.ScheduleBytes, AttachedScheduleSerializedBytes(schedule))
	}
	if projector, ok := s.inner.(PhysicalCommitUsageProjector); ok {
		if usage, projectErr := projector.ProjectPhysicalCommit(req); projectErr == nil {
			observation.ReceiptBytes = usage.ReceiptBytes
			observation.PhysicalCommitBytes = usage.CommitBytes
			observation.PhysicalProjectionKnown = true
		}
	}
	observation.SerializationDuration = time.Since(started)

	switch {
	case exact:
		if committer, ok := s.inner.(ExactCommitter); ok {
			result, err = committer.CommitExact(observedCtx, req)
		} else {
			result, err = s.inner.Commit(observedCtx, req)
		}
	case auditedClaim:
		if committer, ok := s.inner.(AuditedClaimCommitter); ok {
			result, err = committer.CommitAuditedClaim(observedCtx, req)
		} else {
			result, err = s.inner.Commit(observedCtx, req)
		}
	default:
		result, err = s.inner.Commit(observedCtx, req)
	}
	responseStarted := time.Now()
	observation.Status = result.Status.String()
	observation.CommitResponseBytes = AuthorityCommitResponseSerializedBytes(result)
	observation.SerializationDuration += time.Since(responseStarted)
	if complete != nil {
		complete(observation)
	}
	return result, err
}

func (s *observedActorStore) FenceAndClaim(ctx context.Context, req FenceAndClaimRequest) (FenceAndClaimResult, error) {
	return s.inner.FenceAndClaim(ctx, req)
}

func (s *observedActorStore) LookupIdempotencyOutcome(ctx context.Context, req IdempotencyLookupRequest) (IdempotencyLookupResult, error) {
	return s.inner.LookupIdempotencyOutcome(ctx, req)
}

func (s *observedStoreWithCapabilities) PhysicalCapabilities() (PhysicalCapabilities, error) {
	return s.capabilities.PhysicalCapabilities()
}

func (s *observedStoreWithProjector) ProjectPhysicalCommit(req AuthorityCommitRequest) (PhysicalCommitUsage, error) {
	return s.projector.ProjectPhysicalCommit(req)
}

func (s *observedStoreWithPhysicalContracts) PhysicalCapabilities() (PhysicalCapabilities, error) {
	return s.capabilities.PhysicalCapabilities()
}

func (s *observedStoreWithPhysicalContracts) ProjectPhysicalCommit(req AuthorityCommitRequest) (PhysicalCommitUsage, error) {
	return s.projector.ProjectPhysicalCommit(req)
}

func (s *observedStoreWithExact) CommitExact(ctx context.Context, req AuthorityCommitRequest) (CommitResult, error) {
	return s.commit(ctx, req, false, true)
}

func (s *observedStoreWithCapabilitiesAndExact) CommitExact(ctx context.Context, req AuthorityCommitRequest) (CommitResult, error) {
	return s.commit(ctx, req, false, true)
}

func (s *observedStoreWithProjectorAndExact) CommitExact(ctx context.Context, req AuthorityCommitRequest) (CommitResult, error) {
	return s.commit(ctx, req, false, true)
}

func (s *observedStoreWithPhysicalContractsAndExact) CommitExact(ctx context.Context, req AuthorityCommitRequest) (CommitResult, error) {
	return s.commit(ctx, req, false, true)
}
