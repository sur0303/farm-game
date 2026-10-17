package scaleplancontrol

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/routecontrol"
	"farmgame/internal/routeplan"
	"farmgame/internal/scaleplan"
	"farmgame/internal/storage/authoritystore"
)

// ManualClock is a deterministic clock for coordinator, placement and
// quiescence tests.
type ManualClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewManualClock(now time.Time) *ManualClock { return &ManualClock{now: now} }

func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *ManualClock) Advance(delta time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(delta)
	c.mu.Unlock()
}

// MemoryWorkloads is a read-only WorkloadStateReader with explicit mutation
// helpers for deterministic tests.
type MemoryWorkloads struct {
	mu    sync.Mutex
	state scaleplan.WorkloadState
}

func NewMemoryWorkloads(state scaleplan.WorkloadState) *MemoryWorkloads {
	return &MemoryWorkloads{state: cloneWorkload(state)}
}

func (m *MemoryWorkloads) Get(ctx context.Context, ref scaleplan.WorkloadRef) (scaleplan.WorkloadState, error) {
	if err := kitContext(ctx); err != nil {
		return scaleplan.WorkloadState{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state.Ref != ref {
		return scaleplan.WorkloadState{}, fmt.Errorf("%w: workload ref", scaleplan.ErrConflict)
	}
	return cloneWorkload(m.state), nil
}

func (m *MemoryWorkloads) Set(state scaleplan.WorkloadState) {
	m.mu.Lock()
	m.state = cloneWorkload(state)
	m.mu.Unlock()
}

func (m *MemoryWorkloads) State() scaleplan.WorkloadState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneWorkload(m.state)
}

// MemoryHolders implements both exact HolderRegistry read surfaces used by
// the production HolderStateReader adapter.
type MemoryHolders struct {
	mu                   sync.Mutex
	records              map[holderKey]routecontrol.HolderRecord
	exclusions           map[holderKey]routecontrol.PlacementExclusion
	listHook             func()
	releaseCalls         int
	lastReleaseToken     routecontrol.NestedControlOperationToken
	forcedReleaseOutcome routecontrol.PlacementReleaseOutcome
}

type holderKey struct {
	domain actorrouting.RouteDomain
	id     string
	boot   string
}

func NewMemoryHolders() *MemoryHolders {
	return &MemoryHolders{
		records:    make(map[holderKey]routecontrol.HolderRecord),
		exclusions: make(map[holderKey]routecontrol.PlacementExclusion),
	}
}

func (m *MemoryHolders) Get(ctx context.Context, domain actorrouting.RouteDomain, holderID string) (routecontrol.HolderRecord, bool, error) {
	if err := kitContext(ctx); err != nil {
		return routecontrol.HolderRecord{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, record := range m.records {
		if key.domain == domain && key.id == holderID {
			return record, true, nil
		}
	}
	return routecontrol.HolderRecord{}, false, nil
}

func (m *MemoryHolders) List(ctx context.Context, domain actorrouting.RouteDomain) ([]routecontrol.HolderRecord, error) {
	if err := kitContext(ctx); err != nil {
		return nil, err
	}
	m.mu.Lock()
	records := make([]routecontrol.HolderRecord, 0, len(m.records))
	for _, record := range m.records {
		if record.Domain == domain {
			records = append(records, record)
		}
	}
	hook := m.listHook
	m.listHook = nil
	m.mu.Unlock()
	sort.Slice(records, func(i, j int) bool {
		if records[i].DeploymentIdentity.Ordinal == records[j].DeploymentIdentity.Ordinal {
			return records[i].HolderID < records[j].HolderID
		}
		return records[i].DeploymentIdentity.Ordinal < records[j].DeploymentIdentity.Ordinal
	})
	if hook != nil {
		hook()
	}
	return records, nil
}

func (m *MemoryHolders) GetPlacementExclusion(ctx context.Context, domain actorrouting.RouteDomain, holderID, bootID string) (routecontrol.PlacementExclusion, bool, error) {
	if err := kitContext(ctx); err != nil {
		return routecontrol.PlacementExclusion{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	value, found := m.exclusions[holderKey{domain: domain, id: holderID, boot: bootID}]
	return value, found, nil
}

func (m *MemoryHolders) ExcludePlacement(ctx context.Context, domain actorrouting.RouteDomain, holderID, bootID string,
	precondition routecontrol.PlacementExclusionPrecondition) (routecontrol.PlacementExclusionResult, error) {
	if err := kitContext(ctx); err != nil {
		return routecontrol.PlacementExclusionResult{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := holderKey{domain: domain, id: holderID, boot: bootID}
	state, found := m.exclusions[key]
	if !found {
		return routecontrol.PlacementExclusionResult{}, routecontrol.ErrHolderNotFound
	}
	if state.Excluded {
		if state.OwnerPlanID == precondition.OwnerPlanID &&
			precondition.ExpectedRevision != ^uint64(0) &&
			state.Revision == precondition.ExpectedRevision+1 {
			return routecontrol.PlacementExclusionResult{State: state}, nil
		}
		return routecontrol.PlacementExclusionResult{}, routecontrol.ErrPlacementExclusionConflict
	}
	if precondition.ExpectedRevision == 0 || precondition.ExpectedExcluded ||
		state.Revision != precondition.ExpectedRevision || precondition.OwnerPlanID == "" {
		return routecontrol.PlacementExclusionResult{}, routecontrol.ErrPlacementExclusionConflict
	}
	state.Excluded = true
	state.OwnerPlanID = precondition.OwnerPlanID
	state.Revision++
	m.exclusions[key] = state
	return routecontrol.PlacementExclusionResult{Committed: true, State: state}, nil
}

func (m *MemoryHolders) ReleasePlacementExclusionForOwner(ctx context.Context, token routecontrol.NestedControlOperationToken,
	domain actorrouting.RouteDomain, holderID, bootID string,
	release routecontrol.PlacementExclusionRelease) (routecontrol.PlacementExclusionResult, error) {
	if err := kitContext(ctx); err != nil {
		return routecontrol.PlacementExclusionResult{}, err
	}
	if !token.Issued() {
		return routecontrol.PlacementExclusionResult{}, routecontrol.ErrPlacementExclusionInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseCalls++
	m.lastReleaseToken = token
	key := holderKey{domain: domain, id: holderID, boot: bootID}
	_, found := m.records[key]
	if !found {
		for candidateKey := range m.records {
			if candidateKey.domain == domain && candidateKey.id == holderID {
				return routecontrol.PlacementExclusionResult{Outcome: routecontrol.PlacementReleaseByBootRollover}, nil
			}
		}
		return routecontrol.PlacementExclusionResult{Outcome: routecontrol.PlacementReleaseNotFound}, nil
	}
	state, found := m.exclusions[key]
	if !found {
		return routecontrol.PlacementExclusionResult{Outcome: routecontrol.PlacementReleaseNotFound}, nil
	}
	if state.Domain != domain || state.HolderID != holderID || state.BootID != bootID ||
		routecontrol.ValidatePlacementExclusion(state) != nil {
		return routecontrol.PlacementExclusionResult{Outcome: routecontrol.PlacementReleaseMalformed, State: state}, nil
	}
	if release.OwnerPlanID == "" || release.ExpectedRevision == 0 {
		return routecontrol.PlacementExclusionResult{Outcome: routecontrol.PlacementReleaseMalformed}, nil
	}
	if forced := m.forcedReleaseOutcome; forced != "" {
		m.forcedReleaseOutcome = ""
		return routecontrol.PlacementExclusionResult{Outcome: forced, State: state}, nil
	}
	if !state.Excluded {
		return routecontrol.PlacementExclusionResult{Outcome: routecontrol.PlacementReleaseAlreadyReleased, State: state}, nil
	}
	if state.OwnerPlanID != release.OwnerPlanID {
		return routecontrol.PlacementExclusionResult{Outcome: routecontrol.PlacementReleaseOwnerMismatch, State: state}, nil
	}
	if state.Revision != release.ExpectedRevision {
		return routecontrol.PlacementExclusionResult{Outcome: routecontrol.PlacementReleaseRevisionConflict, State: state}, nil
	}
	state.Excluded = false
	state.OwnerPlanID = ""
	state.Revision++
	m.exclusions[key] = state
	return routecontrol.PlacementExclusionResult{Committed: true, Outcome: routecontrol.PlacementReleaseReleased, State: state}, nil
}

func (m *MemoryHolders) SetReleaseOutcome(outcome routecontrol.PlacementReleaseOutcome) {
	m.mu.Lock()
	m.forcedReleaseOutcome = outcome
	m.mu.Unlock()
}

func (m *MemoryHolders) ReleaseCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.releaseCalls
}

func (m *MemoryHolders) LastReleaseToken() routecontrol.NestedControlOperationToken {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastReleaseToken
}

func (m *MemoryHolders) Set(record routecontrol.HolderRecord, exclusion routecontrol.PlacementExclusion) {
	m.mu.Lock()
	key := holderKey{domain: record.Domain, id: record.HolderID, boot: record.BootID}
	m.records[key] = record
	m.exclusions[key] = exclusion
	m.mu.Unlock()
}

func (m *MemoryHolders) SetRecord(record routecontrol.HolderRecord) {
	m.mu.Lock()
	key := holderKey{domain: record.Domain, id: record.HolderID, boot: record.BootID}
	m.records[key] = record
	m.mu.Unlock()
}

func (m *MemoryHolders) SetExclusion(value routecontrol.PlacementExclusion) {
	m.mu.Lock()
	m.exclusions[holderKey{domain: value.Domain, id: value.HolderID, boot: value.BootID}] = value
	m.mu.Unlock()
}

func (m *MemoryHolders) SetListHook(hook func()) {
	m.mu.Lock()
	m.listHook = hook
	m.mu.Unlock()
}

// MemoryAuthorities implements the exact AuthorityReader surface. RenewLease
// deliberately changes only the deadline, allowing liveness-exemption tests
// to assert that PlacementCaptureToken remains stable.
type MemoryAuthorities struct {
	mu     sync.Mutex
	values map[authorityKey]authoritystore.ShardWriteAuthority
}

type authorityKey struct {
	domain actorrouting.RouteDomain
	shard  uint32
}

func NewMemoryAuthorities() *MemoryAuthorities {
	return &MemoryAuthorities{values: make(map[authorityKey]authoritystore.ShardWriteAuthority)}
}

func (m *MemoryAuthorities) ReadShardWriteAuthority(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool, error) {
	if err := kitContext(ctx); err != nil {
		return authoritystore.ShardWriteAuthority{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	value, found := m.values[authorityKey{domain: domain, shard: shard}]
	return value, found, nil
}

func (m *MemoryAuthorities) Set(value authoritystore.ShardWriteAuthority) {
	m.mu.Lock()
	m.values[authorityKey{domain: value.Domain, shard: value.VirtualShard}] = value
	m.mu.Unlock()
}

func (m *MemoryAuthorities) RenewLease(domain actorrouting.RouteDomain, shard uint32, untilMs int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := authorityKey{domain: domain, shard: shard}
	value, found := m.values[key]
	if !found || untilMs <= value.LeaseUntilMs {
		return false
	}
	value.LeaseUntilMs = untilMs
	m.values[key] = value
	return true
}

// MemoryTransitions implements the universal Manager enumeration surface.
type MemoryTransitions struct {
	mu         sync.Mutex
	recoveries []authority.TransitionRecovery
	tombstones []authority.TransitionTombstone
}

func NewMemoryTransitions() *MemoryTransitions { return &MemoryTransitions{} }

func (m *MemoryTransitions) LoadTransitionRecoveries(ctx context.Context) ([]authority.TransitionRecovery, error) {
	if err := kitContext(ctx); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]authority.TransitionRecovery(nil), m.recoveries...), nil
}

func (m *MemoryTransitions) LoadTransitionTombstones(ctx context.Context) ([]authority.TransitionTombstone, error) {
	if err := kitContext(ctx); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]authority.TransitionTombstone(nil), m.tombstones...), nil
}

func (m *MemoryTransitions) SetRecoveries(values ...authority.TransitionRecovery) {
	m.mu.Lock()
	m.recoveries = append([]authority.TransitionRecovery(nil), values...)
	m.mu.Unlock()
}

func (m *MemoryTransitions) SetTombstones(values ...authority.TransitionTombstone) {
	m.mu.Lock()
	m.tombstones = append([]authority.TransitionTombstone(nil), values...)
	m.mu.Unlock()
}

// MemoryRoutePlans is a deterministic core RoutePlanPort fake for quiescence
// tests. It compares the complete request struct, including the source fence.
type MemoryRoutePlans struct {
	mu            sync.Mutex
	requests      map[string]scaleplan.RoutePlanMoveRequest
	views         map[string]scaleplan.RoutePlanView
	verifications map[string]scaleplan.RoutePlanVerification
}

func NewMemoryRoutePlans() *MemoryRoutePlans {
	return &MemoryRoutePlans{
		requests:      make(map[string]scaleplan.RoutePlanMoveRequest),
		views:         make(map[string]scaleplan.RoutePlanView),
		verifications: make(map[string]scaleplan.RoutePlanVerification),
	}
}

func (m *MemoryRoutePlans) EnsureMove(ctx context.Context, request scaleplan.RoutePlanMoveRequest) (scaleplan.RoutePlanView, error) {
	if err := kitContext(ctx); err != nil {
		return scaleplan.RoutePlanView{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if current, found := m.requests[request.PlanID]; found && current != request {
		return m.views[request.PlanID], fmt.Errorf("%w: routeplan immutable request", scaleplan.ErrConflict)
	}
	if _, found := m.requests[request.PlanID]; !found {
		m.requests[request.PlanID] = request
		m.views[request.PlanID] = scaleplan.RoutePlanView{PlanID: request.PlanID, PayloadDigest: request.PayloadDigest, Phase: scaleplan.RoutePlanPhaseSucceeded}
		m.verifications[request.PlanID] = scaleplan.RoutePlanVerification{PlanID: request.PlanID, Consistent: true}
	}
	return m.views[request.PlanID], nil
}

func (m *MemoryRoutePlans) GetMove(ctx context.Context, request scaleplan.RoutePlanMoveRequest) (scaleplan.RoutePlanView, bool, error) {
	if err := kitContext(ctx); err != nil {
		return scaleplan.RoutePlanView{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	current, found := m.requests[request.PlanID]
	if !found {
		return scaleplan.RoutePlanView{}, false, nil
	}
	view := m.views[request.PlanID]
	if current != request {
		return view, true, fmt.Errorf("%w: routeplan immutable request", scaleplan.ErrConflict)
	}
	return view, true, nil
}

func (m *MemoryRoutePlans) Verify(ctx context.Context, planID string) (scaleplan.RoutePlanVerification, error) {
	if err := kitContext(ctx); err != nil {
		return scaleplan.RoutePlanVerification{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	value, found := m.verifications[planID]
	if !found {
		return scaleplan.RoutePlanVerification{}, routeplan.ErrPlanNotFound
	}
	return value, nil
}

func (m *MemoryRoutePlans) SetMove(request scaleplan.RoutePlanMoveRequest, view scaleplan.RoutePlanView, verification scaleplan.RoutePlanVerification) {
	m.mu.Lock()
	m.requests[request.PlanID] = request
	m.views[request.PlanID] = view
	m.verifications[request.PlanID] = verification
	m.mu.Unlock()
}

func (m *MemoryRoutePlans) SetPhase(planID, phase string) {
	m.mu.Lock()
	value := m.views[planID]
	value.Phase = phase
	m.views[planID] = value
	m.mu.Unlock()
}

// NewMemoryPlanStore exposes the reference durable child-stop store without
// adding any adapter behavior to the core package.
func NewMemoryPlanStore(now func() time.Time) (*routeplan.MemoryPlanStore, error) {
	options := routeplan.DefaultMemoryPlanStoreOptions()
	if now != nil {
		options.Now = now
	}
	return routeplan.NewMemoryPlanStore(options)
}

func NewMemorySnapshotStore() *routecontrol.MemoryRouteSnapshotStore {
	return routecontrol.NewMemoryRouteSnapshotStore()
}

func cloneWorkload(value scaleplan.WorkloadState) scaleplan.WorkloadState {
	value.Pods = append([]scaleplan.WorkloadPodState(nil), value.Pods...)
	return value
}

func kitContext(ctx context.Context) error {
	if ctx == nil {
		return scaleplan.ErrInvalidPlan
	}
	return ctx.Err()
}

var _ interface {
	Get(context.Context, scaleplan.WorkloadRef) (scaleplan.WorkloadState, error)
} = (*MemoryWorkloads)(nil)
var _ interface {
	Get(context.Context, actorrouting.RouteDomain, string) (routecontrol.HolderRecord, bool, error)
	List(context.Context, actorrouting.RouteDomain) ([]routecontrol.HolderRecord, error)
	GetPlacementExclusion(context.Context, actorrouting.RouteDomain, string, string) (routecontrol.PlacementExclusion, bool, error)
} = (*MemoryHolders)(nil)
var _ routecontrol.PlacementExclusionStore = (*MemoryHolders)(nil)
var _ interface {
	ReadShardWriteAuthority(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error)
} = (*MemoryAuthorities)(nil)
var _ interface {
	LoadTransitionRecoveries(context.Context) ([]authority.TransitionRecovery, error)
	LoadTransitionTombstones(context.Context) ([]authority.TransitionTombstone, error)
} = (*MemoryTransitions)(nil)
var _ scaleplan.RoutePlanPort = (*MemoryRoutePlans)(nil)
