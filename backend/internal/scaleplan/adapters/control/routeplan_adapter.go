package control

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderidentity"
	routecore "farmgame/internal/routecontrol"
	"farmgame/internal/routeplan"
	core "farmgame/internal/scaleplan"
)

var (
	ErrRoutePlanAdapterInvalid       = errors.New("scaleplan control adapter: invalid routeplan dependency")
	ErrRoutePlanAdapterMismatch      = errors.New("scaleplan control adapter: immutable routeplan mismatch")
	ErrRoutePlanAdapterNotProduction = errors.New("scaleplan control adapter: production verifier required")
	ErrRoutePlanAdapterDrift         = errors.New("scaleplan control adapter: routeplan record drift")
)

type RoutePlanAdapterOptions struct {
	Store         routeplan.PlanStore
	Authorities   AuthorityReader
	Snapshots     routecore.RouteSnapshotStore
	Verification  routeplan.VerificationPort
	OnPlanEnsured func()
	Now           func() time.Time
	PlanTTL       time.Duration
}

type routePlanAdapter struct {
	store         routeplan.PlanStore
	authorities   AuthorityReader
	snapshots     routecore.RouteSnapshotStore
	verification  routeplan.VerificationPort
	onPlanEnsured func()
	now           func() time.Time
	planTTL       time.Duration
}

func NewRoutePlanPort(options RoutePlanAdapterOptions) (core.RoutePlanPort, error) {
	if options.Store == nil || options.Authorities == nil || options.Snapshots == nil || options.Verification == nil {
		return nil, ErrRoutePlanAdapterInvalid
	}
	if !routeplan.IsProductionVerificationPort(options.Verification) {
		return nil, ErrRoutePlanAdapterNotProduction
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.PlanTTL <= 0 {
		options.PlanTTL = 24 * time.Hour
	}
	if options.PlanTTL > 7*24*time.Hour {
		return nil, ErrRoutePlanAdapterInvalid
	}
	return &routePlanAdapter{
		store: options.Store, authorities: options.Authorities, snapshots: options.Snapshots,
		verification: options.Verification, onPlanEnsured: options.OnPlanEnsured, now: options.Now, planTTL: options.PlanTTL,
	}, nil
}

func (a *routePlanAdapter) signalPlanEnsured() {
	if a != nil && a.onPlanEnsured != nil {
		a.onPlanEnsured()
	}
}

func (a *routePlanAdapter) EnsureMove(ctx context.Context, request core.RoutePlanMoveRequest) (core.RoutePlanView, error) {
	if err := routePlanContext(ctx); err != nil {
		return core.RoutePlanView{}, err
	}
	if a == nil || a.store == nil {
		return core.RoutePlanView{}, ErrRoutePlanAdapterInvalid
	}
	if err := validateRoutePlanRequest(request); err != nil {
		return core.RoutePlanView{}, err
	}
	current, found, err := a.store.Load(ctx, request.PlanID)
	if err != nil {
		return core.RoutePlanView{}, err
	}
	if found {
		view, viewErr := routePlanView(current.Plan, request.PayloadDigest)
		if viewErr != nil {
			return core.RoutePlanView{}, viewErr
		}
		if !routePlanImmutableMatches(current.Plan, request) {
			return view, fmt.Errorf("%w: %s", ErrRoutePlanAdapterMismatch, request.PlanID)
		}
		a.signalPlanEnsured()
		return view, nil
	}
	now := a.now()
	if now.IsZero() || now.UnixMilli() <= 0 {
		return core.RoutePlanView{}, ErrRoutePlanAdapterInvalid
	}
	requestedTTL := time.Duration(request.RequestedTTLms) * time.Millisecond
	expiresAt := now.Add(a.planTTL)
	if !expiresAt.After(now) {
		return core.RoutePlanView{}, ErrRoutePlanAdapterInvalid
	}
	candidate := routeplan.Plan{
		PlanID: request.PlanID, TransitionID: request.TransitionID,
		Domain: request.Domain, VirtualShard: request.VirtualShard,
		SourceHolderID: request.Source.HolderID, SourceBootID: request.Source.BootID,
		SourceDeploymentIdentity: deploymentIdentity(request.Source),
		SourceEndpoint:           request.SourceEndpoint,
		TargetHolderID:           request.Target.HolderID, TargetBootID: request.Target.BootID,
		TargetDeploymentIdentity: deploymentIdentity(request.Target),
		TargetEndpoint:           request.TargetEndpoint,
		ExpectedSourceFence:      request.ExpectedSourceFence,
		RequestedTTLms:           requestedTTL.Milliseconds(),
		Operation:                routeplan.OperationPlannedDrain,
		Phase:                    routeplan.PhasePending, Revision: 1,
		CreatedAtMs: now.UnixMilli(), UpdatedAtMs: now.UnixMilli(),
		ExpiresAtMs:        expiresAt.UnixMilli(),
		OwnerScalePlanID:   request.OwnerScalePlanID,
		OwnerRetainUntilMs: request.OwnerRetainUntilMs,
		OwnerWaveID:        request.OwnerWaveID,
		OwnerWaveSize:      request.OwnerWaveSize,
		OwnerWavePosition:  request.OwnerWavePosition,
	}
	if err := candidate.Validate(); err != nil {
		return core.RoutePlanView{}, fmt.Errorf("%w: %v", ErrRoutePlanAdapterInvalid, err)
	}
	result, err := a.store.CompareAndSwap(ctx, 0, candidate)
	switch result.Status {
	case routeplan.CASCommitted, routeplan.CASAlreadyCommitted:
		view, viewErr := routePlanView(result.Record.Plan, request.PayloadDigest)
		if viewErr == nil {
			a.signalPlanEnsured()
		}
		return view, viewErr
	case routeplan.CASRevisionConflict:
		observed, observedFound, loadErr := a.store.Load(ctx, request.PlanID)
		if loadErr != nil {
			return core.RoutePlanView{}, loadErr
		}
		if !observedFound {
			return core.RoutePlanView{}, fmt.Errorf("%w: create conflict without record", ErrRoutePlanAdapterDrift)
		}
		view, viewErr := routePlanView(observed.Plan, request.PayloadDigest)
		if viewErr != nil {
			return core.RoutePlanView{}, viewErr
		}
		if routePlanImmutableMatches(observed.Plan, request) {
			a.signalPlanEnsured()
			return view, nil
		}
		return view, fmt.Errorf("%w: %s", ErrRoutePlanAdapterMismatch, request.PlanID)
	default:
		if err != nil {
			return core.RoutePlanView{}, err
		}
		return core.RoutePlanView{}, fmt.Errorf("%w: unknown CAS result", ErrRoutePlanAdapterDrift)
	}
}

func (a *routePlanAdapter) GetMove(ctx context.Context, request core.RoutePlanMoveRequest) (core.RoutePlanView, bool, error) {
	if err := routePlanContext(ctx); err != nil {
		return core.RoutePlanView{}, false, err
	}
	if a == nil || a.store == nil {
		return core.RoutePlanView{}, false, ErrRoutePlanAdapterInvalid
	}
	if err := validateRoutePlanRequest(request); err != nil {
		return core.RoutePlanView{}, false, err
	}
	record, found, err := a.store.Load(ctx, request.PlanID)
	if err != nil || !found {
		return core.RoutePlanView{}, found, err
	}
	view, viewErr := routePlanView(record.Plan, request.PayloadDigest)
	if viewErr != nil {
		return core.RoutePlanView{}, true, viewErr
	}
	if !routePlanImmutableMatches(record.Plan, request) {
		return view, true, fmt.Errorf("%w: %s", ErrRoutePlanAdapterMismatch, request.PlanID)
	}
	return view, true, nil
}

func (a *routePlanAdapter) Verify(ctx context.Context, planID string) (core.RoutePlanVerification, error) {
	if err := routePlanContext(ctx); err != nil {
		return core.RoutePlanVerification{}, err
	}
	if a == nil || a.store == nil || a.authorities == nil || a.snapshots == nil ||
		!routeplan.IsProductionVerificationPort(a.verification) {
		return core.RoutePlanVerification{}, ErrRoutePlanAdapterInvalid
	}
	if err := routeplan.ValidatePlanID(planID); err != nil {
		return core.RoutePlanVerification{}, err
	}
	record, found, err := a.store.Load(ctx, planID)
	if err != nil {
		return core.RoutePlanVerification{}, err
	}
	if !found {
		return core.RoutePlanVerification{}, routeplan.ErrPlanNotFound
	}
	report := core.RoutePlanVerification{PlanID: planID, Consistent: true}
	value, authorityFound, authorityErr := a.authorities.ReadShardWriteAuthority(ctx, record.Plan.Domain, record.Plan.VirtualShard)
	if authorityErr != nil {
		report.Consistent = false
		report.ErrorCode = "AUTHORITY_READ"
		return report, nil
	}
	if !authorityFound || value.Domain != record.Plan.Domain || value.VirtualShard != record.Plan.VirtualShard {
		report.Consistent = false
		report.ErrorCode = "AUTHORITY_MISSING_OR_MISMATCH"
		return report, nil
	}
	stored, snapshotFound, snapshotErr := a.snapshots.LoadCurrent(ctx)
	if snapshotErr != nil {
		report.Consistent = false
		report.ErrorCode = "SNAPSHOT_READ"
		return report, nil
	}
	if !snapshotFound || stored.Snapshot.GlobalRevision != stored.Revision {
		report.Consistent = false
		report.ErrorCode = "SNAPSHOT_MISSING_OR_MISMATCH"
		return report, nil
	}
	checks := []struct {
		name string
		err  error
	}{
		{"transition", a.verification.VerifyTransition(ctx, record.Plan, value, stored.Snapshot)},
		{"recovery", a.verification.VerifyRecovery(ctx, record.Plan, value, stored.Snapshot)},
		{"tombstone", a.verification.VerifyTombstone(ctx, record.Plan, value, stored.Snapshot)},
		{"gate_resolution", a.verification.VerifyGateResolution(ctx, record.Plan, value, stored.Snapshot)},
	}
	for _, check := range checks {
		if check.err != nil {
			report.Consistent = false
			if report.ErrorCode == "" {
				report.ErrorCode = "VERIFICATION_" + check.name
			}
		}
	}
	return report, nil
}

func (a *routePlanAdapter) LoadPlan(ctx context.Context, planID string) (routeplan.PlanRecord, bool, error) {
	if a == nil || a.store == nil {
		return routeplan.PlanRecord{}, false, ErrRoutePlanAdapterInvalid
	}
	return a.store.Load(ctx, planID)
}

func (a *routePlanAdapter) StopOwnedPlanForScaleTermination(ctx context.Context, command routeplan.DurableStopCommand) (routeplan.DurableStopResult, error) {
	durable, ok := a.store.(routeplan.DurableStopStore)
	if !ok || durable == nil {
		return routeplan.DurableStopResult{Status: routeplan.DurableStopUnsafe}, ErrRoutePlanAdapterInvalid
	}
	return durable.StopOwnedPlanForScaleTermination(ctx, command)
}

func (a *routePlanAdapter) PlanStore() routeplan.PlanStore {
	if a == nil {
		return nil
	}
	return a.store
}

func validateRoutePlanRequest(request core.RoutePlanMoveRequest) error {
	if err := routeplan.ValidatePlanID(request.PlanID); err != nil {
		return err
	}
	if request.TransitionID == "" || request.RequestedTTLms <= 0 || request.OwnerScalePlanID == "" ||
		request.OwnerRetainUntilMs <= 0 || request.PayloadDigest == "" {
		return fmt.Errorf("%w: incomplete routeplan request", ErrRoutePlanAdapterInvalid)
	}
	if request.Domain != actorrouting.DomainPlayer && request.Domain != actorrouting.DomainFarm {
		return fmt.Errorf("%w: routeplan domain", ErrRoutePlanAdapterInvalid)
	}
	if request.OwnerWaveID == "" {
		if request.OwnerWaveSize != 0 || request.OwnerWavePosition != 0 {
			return fmt.Errorf("%w: partial owner wave", ErrRoutePlanAdapterInvalid)
		}
	} else if request.OwnerWaveSize == 0 || request.OwnerWaveSize > 32 || request.OwnerWavePosition >= request.OwnerWaveSize {
		return fmt.Errorf("%w: owner wave bounds", ErrRoutePlanAdapterInvalid)
	}
	if !request.Source.Exact() || !request.Target.Exact() ||
		request.Source.Domain != request.Domain || request.Target.Domain != request.Domain ||
		request.SourceEndpoint == "" || request.TargetEndpoint == "" {
		return fmt.Errorf("%w: holder identity/endpoints", ErrRoutePlanAdapterInvalid)
	}
	if request.Source == request.Target {
		return fmt.Errorf("%w: identical routeplan holders", ErrRoutePlanAdapterInvalid)
	}
	if request.ExpectedSourceFence.ExpectedHolderID != request.Source.HolderID ||
		request.ExpectedSourceFence.ExpectedHolderBootID != request.Source.BootID ||
		request.ExpectedSourceFence.ExpectedAssignmentRevision == 0 ||
		request.ExpectedSourceFence.ExpectedWriterEpoch == 0 ||
		request.ExpectedSourceFence.ExpectedLeaseEpoch == 0 {
		return fmt.Errorf("%w: source fence", ErrRoutePlanAdapterInvalid)
	}
	if request.RequestedTTLms > routeplan.MaxRequestedTTLms {
		return fmt.Errorf("%w: requested ttl", ErrRoutePlanAdapterInvalid)
	}
	return nil
}

func routePlanImmutableMatches(plan routeplan.Plan, request core.RoutePlanMoveRequest) bool {
	return plan.PlanID == request.PlanID &&
		plan.TransitionID == request.TransitionID &&
		plan.Domain == request.Domain &&
		plan.VirtualShard == request.VirtualShard &&
		plan.SourceHolderID == request.Source.HolderID &&
		plan.SourceBootID == request.Source.BootID &&
		reflect.DeepEqual(plan.SourceDeploymentIdentity, deploymentIdentity(request.Source)) &&
		plan.SourceEndpoint == request.SourceEndpoint &&
		plan.TargetHolderID == request.Target.HolderID &&
		plan.TargetBootID == request.Target.BootID &&
		reflect.DeepEqual(plan.TargetDeploymentIdentity, deploymentIdentity(request.Target)) &&
		plan.TargetEndpoint == request.TargetEndpoint &&
		reflect.DeepEqual(plan.ExpectedSourceFence, request.ExpectedSourceFence) &&
		plan.RequestedTTLms == request.RequestedTTLms &&
		plan.Operation == routeplan.OperationPlannedDrain &&
		plan.OwnerScalePlanID == request.OwnerScalePlanID &&
		plan.OwnerRetainUntilMs == request.OwnerRetainUntilMs &&
		plan.OwnerWaveID == request.OwnerWaveID &&
		plan.OwnerWaveSize == request.OwnerWaveSize &&
		plan.OwnerWavePosition == request.OwnerWavePosition &&
		plan.Validate() == nil
}

func routePlanView(plan routeplan.Plan, payloadDigest string) (core.RoutePlanView, error) {
	if err := plan.Validate(); err != nil {
		return core.RoutePlanView{}, fmt.Errorf("%w: stored plan: %v", ErrRoutePlanAdapterDrift, err)
	}
	phase := plan.Phase.String()
	if !validObservedRoutePlanPhase(phase) {
		return core.RoutePlanView{}, fmt.Errorf("%w: invalid observed phase %q", ErrRoutePlanAdapterDrift, phase)
	}
	return core.RoutePlanView{PlanID: plan.PlanID, PayloadDigest: payloadDigest, Phase: phase, ErrorCode: plan.ErrorCode}, nil
}

func validObservedRoutePlanPhase(value string) bool {
	switch value {
	case core.RoutePlanPhasePending, core.RoutePlanPhaseDraining, core.RoutePlanPhaseAssigning,
		core.RoutePlanPhasePreparing, core.RoutePlanPhaseActive, core.RoutePlanPhaseSucceeded,
		core.RoutePlanPhaseFailed, core.RoutePlanPhasePaused:
		return true
	default:
		return false
	}
}

func deploymentIdentity(ref core.HolderRef) holderidentity.DeploymentIdentity {
	return holderidentity.DeploymentIdentity{PodUID: ref.PodUID, WorkloadUID: ref.WorkloadUID, Ordinal: ref.Ordinal}
}

func routePlanContext(ctx context.Context) error {
	if ctx == nil {
		return ErrRoutePlanAdapterInvalid
	}
	return ctx.Err()
}

var _ core.RoutePlanPort = (*routePlanAdapter)(nil)
var _ routeplan.DurableStopStore = (*routeplan.MemoryPlanStore)(nil)
