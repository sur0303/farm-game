package scaleplan

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

type ExecutorOptions struct {
	Repository         ScalePlanRepository
	Workloads          WorkloadScaler
	RoutePlans         RoutePlanPort
	Holders            HolderStateReader
	Placements         ShardPlacementReader
	Exclusions         PlacementExclusionPort
	BlockedTermination *BlockedTerminationOwner
	// Scope must equal the owner's. It is carried here as well so the value
	// reaching the lease comes from this Executor's configuration rather than
	// being read back out of the owner, which would make the parameter chain
	// decorative.
	Scope MutationScope
	Now   func() time.Time
}

type Executor struct {
	repository         ScalePlanRepository
	workloads          WorkloadScaler
	routePlans         RoutePlanPort
	holders            HolderStateReader
	placements         ShardPlacementReader
	exclusions         PlacementExclusionPort
	blockedTermination *BlockedTerminationOwner
	scope              MutationScope
	now                func() time.Time
}

func NewExecutor(options ExecutorOptions) (*Executor, error) {
	if options.Repository == nil || options.Workloads == nil || options.RoutePlans == nil ||
		options.Holders == nil || options.Placements == nil || options.Exclusions == nil || options.BlockedTermination == nil {
		return nil, fmt.Errorf("%w: every code-only port is required", ErrInvalidPlan)
	}
	repository, ok := options.Repository.(blockedTerminationRepository)
	if !ok || repository.repositoryIdentity() == nil || options.BlockedTermination.repository.repositoryIdentity() != repository.repositoryIdentity() {
		return nil, fmt.Errorf("%w: blocked termination owner/repository mismatch", ErrInvalidPlan)
	}
	if err := options.Scope.Validate(); err != nil {
		return nil, err
	}
	if options.Scope != options.BlockedTermination.scope {
		return nil, fmt.Errorf("%w: executor/owner mutation scope mismatch", ErrInvalidPlan)
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &Executor{
		repository: options.Repository, workloads: options.Workloads, routePlans: options.RoutePlans,
		holders: options.Holders, placements: options.Placements, exclusions: options.Exclusions,
		blockedTermination: options.BlockedTermination, scope: options.Scope, now: options.Now,
	}, nil
}

// enterPlanMutation takes the realm-global barrier for this Executor's scope.
// The returned guard is always held; callers may use it without a nil check.
func (e *Executor) enterPlanMutation(ctx context.Context) (PlanMutationGuard, error) {
	return e.blockedTermination.enterPlanMutation(ctx, e.scope)
}

func (e *Executor) Submit(ctx context.Context, submission Submission) (PlanRecord, error) {
	if e == nil || ctx == nil {
		return PlanRecord{}, ErrInvalidPlan
	}
	guard, err := e.enterPlanMutation(ctx)
	if err != nil {
		return PlanRecord{}, err
	}
	defer releaseGuard(guard, ctx)
	ctx = guard.Context()
	if err := ctx.Err(); err != nil {
		return PlanRecord{}, err
	}
	plan, err := BuildInitialPlan(submission, e.now())
	if err != nil {
		return PlanRecord{}, err
	}
	// The lease we hold covers one realm. A submission for any other realm is
	// unserialized with respect to whoever holds that realm's lease, so it is
	// rejected here rather than committed under the wrong barrier.
	if plan.Submission.Realm != e.scope.Realm || plan.Submission.Workload.Namespace != e.scope.Namespace {
		return PlanRecord{}, fmt.Errorf("%w: submission outside leased scope", ErrInvalidPlan)
	}
	result, err := e.repository.CreateOrGet(ctx, IdempotencySubmission{
		Key: plan.IdempotencyKey, PayloadDigest: plan.PayloadDigest, WorkloadKey: plan.Submission.Workload.Key(),
	}, plan)
	if err != nil {
		return result.Record, err
	}
	switch result.Status {
	case CreateCommitted, CreateExistingSamePayload:
		return result.Record, nil
	case CreateIdempotencyConflict:
		return result.Record, ErrIdempotencyConflict
	case CreateWorkloadBusy:
		return result.Record, ErrWorkloadBusy
	case CreateCapacityExceeded:
		return result.Record, ErrCapacityExceeded
	default:
		return result.Record, ErrCorruptRecord
	}
}

// Execute advances deterministic steps until the plan is terminal or an
// observed dependency is pending. The bound is derived from the immutable
// manifest size and is a correctness guard, not a throughput setting.
func (e *Executor) Execute(ctx context.Context, planID string) (PlanRecord, error) {
	if e == nil || ctx == nil {
		return PlanRecord{}, ErrInvalidPlan
	}
	guard, err := e.enterPlanMutation(ctx)
	if err != nil {
		return PlanRecord{}, err
	}
	defer releaseGuard(guard, ctx)
	ctx = guard.Context()
	return e.executeUnlocked(ctx, planID)
}

func (e *Executor) executeUnlocked(ctx context.Context, planID string) (PlanRecord, error) {
	record, found, err := e.repository.Load(ctx, planID)
	if err != nil || !found {
		if err == nil {
			err = ErrPlanNotFound
		}
		return record, err
	}
	if record.Plan.Status.Terminal() {
		return record, nil
	}
	if record.Plan.Status == StatusPaused {
		return record, ErrPaused
	}
	if record.Plan.Status == StatusBlocked {
		return record, ErrBlocked
	}
	if blocked, deadlineErr := e.enforceDeadlines(ctx, record); deadlineErr != nil {
		return blocked, deadlineErr
	}
	record, err = e.mutate(ctx, record, func(plan *Plan) error {
		if plan.Attempt == ^uint64(0) {
			return fmt.Errorf("%w: attempt overflow", ErrInvalidPlan)
		}
		plan.Attempt++
		return nil
	})
	if err != nil {
		return record, err
	}
	limit := 32 + len(record.Plan.Submission.Moves)*8
	for i := 0; i < limit; i++ {
		if err := ctx.Err(); err != nil {
			return record, err
		}
		record, err = e.stepUnlocked(ctx, record.Plan.PlanID)
		if err != nil {
			if errors.Is(err, ErrPending) || errors.Is(err, ErrPaused) || errors.Is(err, ErrBlocked) || record.Plan.Status.Terminal() {
				return record, err
			}
			return record, err
		}
		if record.Plan.Status.Terminal() {
			return record, nil
		}
	}
	return record, fmt.Errorf("%w: deterministic step bound exhausted", ErrCorruptRecord)
}

// Step performs at most one durable state-machine step. It is exported so
// restart tests and a future controller loop can reconstruct Executor between
// every checkpoint without relying on process memory.
func (e *Executor) Step(ctx context.Context, planID string) (PlanRecord, error) {
	if e == nil || ctx == nil {
		return PlanRecord{}, ErrInvalidPlan
	}
	guard, err := e.enterPlanMutation(ctx)
	if err != nil {
		return PlanRecord{}, err
	}
	defer releaseGuard(guard, ctx)
	ctx = guard.Context()
	return e.stepUnlocked(ctx, planID)
}

func (e *Executor) stepUnlocked(ctx context.Context, planID string) (PlanRecord, error) {
	if err := ctx.Err(); err != nil {
		return PlanRecord{}, err
	}
	record, found, err := e.repository.Load(ctx, planID)
	if err != nil || !found {
		if err == nil {
			err = ErrPlanNotFound
		}
		return record, err
	}
	if record.Plan.Status.Terminal() {
		return record, nil
	}
	if record.Plan.Status == StatusPaused {
		return record, ErrPaused
	}
	if record.Plan.Status == StatusBlocked {
		return record, ErrBlocked
	}
	if blocked, deadlineErr := e.enforceDeadlines(ctx, record); deadlineErr != nil {
		return blocked, deadlineErr
	}
	switch record.Plan.Phase {
	case PhasePending:
		if record.Plan.Submission.Operation == OperationScaleOut {
			return e.beginScaleOut(ctx, record)
		}
		return e.mutate(ctx, record, func(plan *Plan) error { plan.Phase = PhaseSelectingVictim; return nil })
	case PhaseScalingUp:
		return e.reconcileScale(ctx, record, PhaseWaitingHolderReady)
	case PhaseWaitingHolderReady:
		return e.waitScaleOutTarget(ctx, record)
	case PhaseSelectingVictim:
		return e.selectScaleInVictim(ctx, record)
	case PhaseExcludingVictim:
		return e.excludeVictim(ctx, record)
	case PhaseCapturingVictimShards:
		return e.captureVictimShards(ctx, record)
	case PhaseMovingWave:
		return e.moveWave(ctx, record)
	case PhaseVerifyingWave:
		return e.verifyWave(ctx, record)
	case PhaseWaitingZeroShard:
		return e.waitZeroShard(ctx, record)
	case PhaseScalingDown:
		return e.reconcileScale(ctx, record, PhaseVerifyingFinal)
	case PhaseVerifyingFinal:
		return e.verifyFinal(ctx, record)
	case PhaseReleasingExclusion:
		return e.reconcileExclusionRelease(ctx, record, ErrOutcomeUnknown)
	default:
		return e.fail(ctx, record, "INVALID_PHASE", ErrInvalidTransition)
	}
}

func (e *Executor) beginScaleOut(ctx context.Context, record PlanRecord) (PlanRecord, error) {
	state, err := e.workloads.Get(ctx, record.Plan.Submission.Workload)
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	if err := validateWorkloadState(state, record.Plan.Submission.Workload); err != nil ||
		(state.Replicas != record.Plan.Submission.ExpectedReplicas &&
			state.Replicas != record.Plan.Submission.DesiredReplicas) {
		if err == nil {
			err = ErrConflict
		}
		return e.fail(ctx, record, "WORKLOAD_PRECONDITION", err)
	}
	if e.now().UnixMilli() >= record.Plan.StartDeadlineMs {
		return e.fail(ctx, record, "START_DEADLINE_EXPIRED", ErrConflict)
	}
	// Kubernetes may already have created the +1 StatefulSet ordinal before
	// the informer-driven auto reconciler can persist this plan. Freezing an
	// intent against the exact current Workload UID/resourceVersion is safe:
	// reconcileScale will observe DesiredReplicas and durably classify the
	// external mutation as ADOPTED_COMMITTED. Any other replica count remains a
	// hard conflict.
	return e.mutate(ctx, record, func(plan *Plan) error {
		plan.WorkloadIdentity = WorkloadIdentity{UID: state.UID, ResourceVersion: state.ResourceVersion}
		plan.ScaleIntent = &ScaleIntent{WorkloadUID: state.UID, ExpectedResourceVersion: state.ResourceVersion,
			ExpectedReplicas: plan.Submission.ExpectedReplicas, DesiredReplicas: plan.Submission.DesiredReplicas, Effect: EffectPending}
		plan.Phase = PhaseScalingUp
		return nil
	})
}

func (e *Executor) reconcileScale(ctx context.Context, record PlanRecord, next Phase) (PlanRecord, error) {
	intent := record.Plan.ScaleIntent
	if intent == nil {
		return e.fail(ctx, record, "SCALE_INTENT_MISSING", ErrCorruptRecord)
	}
	state, err := e.workloads.Get(ctx, record.Plan.Submission.Workload)
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	if state.UID != intent.WorkloadUID {
		return e.fail(ctx, record, "WORKLOAD_IDENTITY_CHANGED", ErrConflict)
	}
	if state.Replicas == intent.DesiredReplicas {
		return e.commitScaleObservation(ctx, record, next, EffectAdopted)
	}
	if state.Replicas != intent.ExpectedReplicas || state.ResourceVersion != intent.ExpectedResourceVersion {
		return e.fail(ctx, record, "WORKLOAD_SCALE_CONFLICT", ErrConflict)
	}
	if record.Plan.Submission.Operation == OperationScaleIn && record.Plan.Phase == PhaseScalingDown {
		if err := e.validateBeforeScaleDownEffect(ctx, record, state); err != nil {
			switch {
			case errors.Is(err, ErrPending):
				return record, ErrPending
			case errors.Is(err, ErrConflict):
				return e.block(ctx, record, "SCALE_DOWN_EFFECT_BARRIER_CHANGED")
			default:
				return e.pauseDependency(ctx, record, err)
			}
		}
	}
	if e.now().UnixMilli() >= record.Plan.RecoveryRetainUntilMs {
		return e.block(ctx, record, "RECOVERY_RETENTION_EXCEEDED")
	}
	result, scaleErr := e.workloads.Scale(ctx, record.Plan.Submission.Workload, ScalePrecondition{
		WorkloadUID: intent.WorkloadUID, ResourceVersion: intent.ExpectedResourceVersion, ExpectedReplicas: intent.ExpectedReplicas,
	}, intent.DesiredReplicas)
	observed, getErr := e.workloads.Get(ctx, record.Plan.Submission.Workload)
	if getErr != nil {
		return e.pauseDependency(ctx, record, getErr)
	}
	if observed.UID != intent.WorkloadUID {
		return e.fail(ctx, record, "WORKLOAD_IDENTITY_CHANGED", ErrConflict)
	}
	if observed.Replicas == intent.DesiredReplicas {
		effect := EffectAdopted
		if scaleErr == nil && result.Committed {
			effect = EffectCommitted
		}
		return e.commitScaleObservation(ctx, record, next, effect)
	}
	if observed.Replicas == intent.ExpectedReplicas && observed.ResourceVersion == intent.ExpectedResourceVersion {
		switch {
		case errors.Is(scaleErr, ErrOutcomeUnknown):
			return record, ErrPending
		case errors.Is(scaleErr, ErrConflict):
			return e.fail(ctx, record, "WORKLOAD_SCALE_CONFLICT", ErrConflict)
		case scaleErr != nil:
			return e.pauseDependency(ctx, record, scaleErr)
		default:
			return e.fail(ctx, record, "WORKLOAD_SCALE_REJECTED", ErrConflict)
		}
	}
	return e.fail(ctx, record, "WORKLOAD_SCALE_CONFLICT", ErrConflict)
}

func (e *Executor) commitScaleObservation(ctx context.Context, record PlanRecord, next Phase, effect EffectState) (PlanRecord, error) {
	return e.mutate(ctx, record, func(plan *Plan) error {
		plan.ScaleIntent.Effect = effect
		plan.Phase = next
		return nil
	})
}

func (e *Executor) waitScaleOutTarget(ctx context.Context, record PlanRecord) (PlanRecord, error) {
	desiredOrdinal := record.Plan.Submission.DesiredReplicas - 1
	targetID := record.Plan.Submission.Moves[0].TargetHolderID
	workload, err := e.workloads.Get(ctx, record.Plan.Submission.Workload)
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	if err := validateWorkloadState(workload, record.Plan.Submission.Workload); err != nil {
		return e.fail(ctx, record, "WORKLOAD_IDENTITY_JOIN", err)
	}
	if workload.UID != record.Plan.WorkloadIdentity.UID || workload.Replicas != record.Plan.Submission.DesiredReplicas {
		return record, ErrPending
	}
	for _, move := range record.Plan.Submission.Moves {
		if move.TargetHolderID != targetID || move.TargetOrdinal != desiredOrdinal {
			return e.fail(ctx, record, "SCALE_OUT_MANIFEST_TARGET", ErrInvalidPlan)
		}
	}
	holder, found, err := e.holders.GetHolder(ctx, HolderRef{Domain: record.Plan.Submission.Domain, HolderID: targetID, Ordinal: desiredOrdinal})
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	if !found || !eligibleZeroShardTarget(holder, record.Plan.WorkloadIdentity.UID, desiredOrdinal, e.now()) ||
		!podPresent(workload, holder.Ref) {
		return record, ErrPending
	}
	return e.mutate(ctx, record, func(plan *Plan) error {
		plan.Target = holder.Ref
		plan.Phase = PhaseMovingWave
		plan.WaveIndex = 0
		return nil
	})
}

func (e *Executor) selectScaleInVictim(ctx context.Context, record PlanRecord) (PlanRecord, error) {
	state, err := e.workloads.Get(ctx, record.Plan.Submission.Workload)
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	if err := validateWorkloadState(state, record.Plan.Submission.Workload); err != nil ||
		state.Replicas != record.Plan.Submission.ExpectedReplicas {
		if err == nil {
			err = ErrConflict
		}
		return e.fail(ctx, record, "WORKLOAD_PRECONDITION", err)
	}
	holders, err := e.holders.ListDomainHolders(ctx, record.Plan.Submission.Domain)
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	var victim HolderState
	found := false
	if record.Plan.Submission.Operation == OperationDrainHolder {
		sourceID := record.Plan.Submission.VictimHolderID
		if sourceID == "" && len(record.Plan.Submission.Moves) > 0 {
			// Compatibility for drain plans written before VictimHolderID was
			// introduced. New plans always carry the explicit binding.
			sourceID = record.Plan.Submission.Moves[0].SourceHolderID
		}
		for _, holder := range holders {
			if holder.Ref.HolderID != sourceID || holder.Ref.WorkloadUID != state.UID || !holder.Ref.Exact() ||
				!holder.ProcessReady || holder.Draining || !holder.PlacementEligible || !holder.AssignmentKnownFresh ||
				!holder.LeaseUntil.After(e.now()) ||
				!((holder.State == HolderServing && holder.ServingShardCount > 0) ||
					(holder.State == HolderReadyZeroShard && holder.ServingShardCount == 0)) {
				continue
			}
			if found {
				return e.fail(ctx, record, "VICTIM_SELECTION", ErrConflict)
			}
			victim, found = holder, true
		}
	} else {
		victim, found = highestOrdinalHolder(holders, state.UID, e.now())
	}
	if !found || (record.Plan.Submission.Operation == OperationScaleIn &&
		(victim.Ref.Ordinal != state.Replicas-1 || !victimStillHighest(state, victim.Ref))) {
		return e.fail(ctx, record, "VICTIM_SELECTION", ErrConflict)
	}
	var pinnedTarget HolderRef
	if record.Plan.Submission.PinnedTarget != (HolderRef{}) {
		pinnedTarget = record.Plan.Submission.PinnedTarget
		targetFound := false
		for _, holder := range holders {
			if holder.Ref != pinnedTarget {
				continue
			}
			targetFound = eligibleZeroShardTarget(holder, state.UID, pinnedTarget.Ordinal, e.now()) &&
				podPresent(state, pinnedTarget)
			break
		}
		if !targetFound {
			return e.block(ctx, record, "DRAIN_TARGET_GENERATION_CHANGED")
		}
	}
	for _, move := range record.Plan.Submission.Moves {
		if move.SourceHolderID != victim.Ref.HolderID || move.TargetHolderID == victim.Ref.HolderID {
			return e.fail(ctx, record, "SCALE_IN_MANIFEST_SOURCE", ErrConflict)
		}
	}
	exclusion, found, err := e.exclusions.Get(ctx, victim.Ref)
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	if !found || exclusion.Holder != victim.Ref || exclusion.Revision == 0 || exclusion.Excluded ||
		!victim.PlacementEligible || victim.PlacementRevision != exclusion.Revision {
		return e.fail(ctx, record, "VICTIM_EXCLUSION_PRECONDITION", ErrConflict)
	}
	if e.now().UnixMilli() >= record.Plan.StartDeadlineMs {
		return e.fail(ctx, record, "START_DEADLINE_EXPIRED", ErrConflict)
	}
	return e.mutate(ctx, record, func(plan *Plan) error {
		plan.WorkloadIdentity = WorkloadIdentity{UID: state.UID, ResourceVersion: state.ResourceVersion}
		plan.Victim = victim.Ref
		plan.Target = pinnedTarget
		plan.ExclusionIntent = &ExclusionIntent{Victim: victim.Ref, ExpectedRevision: exclusion.Revision,
			OwnerPlanID: plan.PlanID, Effect: EffectPending}
		plan.Phase = PhaseExcludingVictim
		return nil
	})
}

func (e *Executor) excludeVictim(ctx context.Context, record PlanRecord) (PlanRecord, error) {
	intent := record.Plan.ExclusionIntent
	if intent == nil {
		return e.fail(ctx, record, "EXCLUSION_INTENT_MISSING", ErrCorruptRecord)
	}
	holder, holderFound, err := e.holders.GetHolder(ctx, intent.Victim)
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	if !holderFound || holder.Ref != intent.Victim {
		return e.fail(ctx, record, "VICTIM_BOOT_CHANGED", ErrConflict)
	}
	current, found, err := e.exclusions.Get(ctx, intent.Victim)
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	if !found || current.Holder != intent.Victim {
		return e.fail(ctx, record, "VICTIM_BOOT_CHANGED", ErrConflict)
	}
	if current.Excluded {
		if current.OwnerPlanID != record.Plan.PlanID {
			return e.block(ctx, record, "PLACEMENT_EXCLUDED_BY_OTHER_PLAN")
		}
		return e.commitExclusionObservation(ctx, record, EffectAdopted, current.Revision)
	}
	if current.Revision != intent.ExpectedRevision {
		return e.block(ctx, record, "PLACEMENT_EXCLUSION_REVISION_CHANGED")
	}
	if e.now().UnixMilli() >= record.Plan.RecoveryRetainUntilMs {
		return e.block(ctx, record, "RECOVERY_RETENTION_EXCEEDED")
	}
	result, excludeErr := e.exclusions.Exclude(ctx, intent.Victim, PlacementExclusionPrecondition{
		ExpectedRevision: intent.ExpectedRevision, ExpectedExcluded: false, OwnerPlanID: record.Plan.PlanID,
	})
	observed, found, getErr := e.exclusions.Get(ctx, intent.Victim)
	if getErr != nil {
		return e.pauseDependency(ctx, record, getErr)
	}
	if !found || observed.Holder != intent.Victim {
		return e.fail(ctx, record, "VICTIM_BOOT_CHANGED", ErrConflict)
	}
	if observed.Excluded && observed.OwnerPlanID == record.Plan.PlanID {
		effect := EffectAdopted
		if excludeErr == nil && result.Committed {
			effect = EffectCommitted
		}
		return e.commitExclusionObservation(ctx, record, effect, observed.Revision)
	}
	if errors.Is(excludeErr, ErrOutcomeUnknown) && observed.Revision == intent.ExpectedRevision && !observed.Excluded {
		return record, ErrPending
	}
	if excludeErr != nil {
		return e.pauseDependency(ctx, record, excludeErr)
	}
	return e.block(ctx, record, "PLACEMENT_EXCLUSION_NOT_CONFIRMED")
}

func (e *Executor) commitExclusionObservation(ctx context.Context, record PlanRecord, effect EffectState, revision uint64) (PlanRecord, error) {
	return e.mutate(ctx, record, func(plan *Plan) error {
		plan.ExclusionIntent.Effect = effect
		plan.ExclusionIntent.ObservedRevision = revision
		plan.Phase = PhaseCapturingVictimShards
		return nil
	})
}

func (e *Executor) captureVictimShards(ctx context.Context, record PlanRecord) (PlanRecord, error) {
	exclusion, found, err := e.exclusions.Get(ctx, record.Plan.Victim)
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	if !found || exclusion.Holder != record.Plan.Victim || !exclusion.Excluded || exclusion.OwnerPlanID != record.Plan.PlanID ||
		exclusion.Revision != record.Plan.ExclusionIntent.ObservedRevision {
		return e.block(ctx, record, "PLACEMENT_EXCLUSION_LOST")
	}
	holder, found, err := e.holders.GetHolder(ctx, record.Plan.Victim)
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	if !found || holder.Ref != record.Plan.Victim || holder.Draining || !holder.ProcessReady || holder.PlacementEligible ||
		holder.PlacementRevision != exclusion.Revision || !holder.AssignmentKnownFresh || !holder.LeaseUntil.After(e.now()) {
		return e.block(ctx, record, "PLACEMENT_EXCLUSION_NOT_VISIBLE")
	}
	moveCount := len(record.Plan.Submission.Moves)
	if moveCount == 0 {
		if holder.State != HolderReadyZeroShard || holder.ServingShardCount != 0 {
			return e.block(ctx, record, "SCALE_ONLY_VICTIM_NOT_ZERO_SHARD")
		}
	} else if holder.State != HolderServing || holder.ServingShardCount == 0 {
		return e.block(ctx, record, "PLACEMENT_EXCLUSION_NOT_VISIBLE")
	}
	capture, err := e.placements.CaptureHolderShards(ctx, record.Plan.Victim)
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	for _, placement := range capture.Placements {
		if placement.Active && (!placement.KnownFresh || !placement.LeaseUntil.After(e.now())) {
			return e.block(ctx, record, "SCALE_IN_PLACEMENT_STALE")
		}
	}
	if capture.Holder != record.Plan.Victim || !manifestExactlyCovers(record.Plan.Submission.Moves, capture.Placements, record.Plan.Victim) {
		return e.block(ctx, record, "SCALE_IN_MANIFEST_DRIFT")
	}
	if err := e.placements.Revalidate(ctx, capture.Token); err != nil {
		return e.block(ctx, record, "PLACEMENT_CAPTURE_CHANGED")
	}
	return e.mutate(ctx, record, func(plan *Plan) error {
		if len(plan.Submission.Moves) == 0 {
			plan.Phase = PhaseWaitingZeroShard
		} else {
			plan.Phase = PhaseMovingWave
		}
		plan.WaveIndex = 0
		return nil
	})
}

func (e *Executor) moveWave(ctx context.Context, record PlanRecord) (PlanRecord, error) {
	start := int(record.Plan.WaveIndex)
	end := waveEnd(record.Plan)
	if start < 0 || start >= len(record.Plan.Submission.Moves) || end <= start || len(record.Plan.ResolvedMoves) < start || len(record.Plan.ResolvedMoves) > end {
		return e.fail(ctx, record, "WAVE_INDEX", ErrCorruptRecord)
	}
	// Freeze the complete bounded wave before creating any child. This keeps
	// RoutePlan effects from changing the source placement used to resolve a
	// later move in the same wave.
	if len(record.Plan.ResolvedMoves) < end {
		return e.resolveWave(ctx, record, end)
	}
	return e.reconcileWaveChildren(ctx, record, start, end)
}

func (e *Executor) resolveWave(ctx context.Context, record PlanRecord, end int) (PlanRecord, error) {
	start := len(record.Plan.ResolvedMoves)
	keys := make([]ShardKey, 0, end-start)
	for index := start; index < end; index++ {
		intent := record.Plan.Submission.Moves[index]
		keys = append(keys, ShardKey{Domain: intent.Domain, VirtualShard: intent.VirtualShard})
	}
	capture, err := e.placements.CaptureShards(ctx, keys)
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	if len(capture.Placements) != len(keys) {
		return e.fail(ctx, record, "SOURCE_PLACEMENT", ErrConflict)
	}
	placements := make(map[ShardKey]ShardPlacement, len(capture.Placements))
	for _, placement := range capture.Placements {
		key := ShardKey{Domain: placement.Domain, VirtualShard: placement.VirtualShard}
		if _, duplicate := placements[key]; duplicate {
			return e.fail(ctx, record, "SOURCE_PLACEMENT", ErrConflict)
		}
		placements[key] = placement
	}

	var exclusionBefore PlacementExclusion
	var capturedWorkload WorkloadState
	if usesVictimExclusion(record.Plan.Submission.Operation) {
		var found bool
		exclusionBefore, found, err = e.exclusions.Get(ctx, record.Plan.Victim)
		if err != nil {
			return e.pauseDependency(ctx, record, err)
		}
		if !found || !exclusionBefore.Excluded || exclusionBefore.Holder != record.Plan.Victim ||
			exclusionBefore.OwnerPlanID != record.Plan.PlanID || exclusionBefore.Revision != record.Plan.ExclusionIntent.ObservedRevision {
			return e.block(ctx, record, "PLACEMENT_EXCLUSION_LOST")
		}
		capturedWorkload, err = e.workloads.Get(ctx, record.Plan.Submission.Workload)
		if err != nil {
			return e.pauseDependency(ctx, record, err)
		}
		if err := validateWorkloadState(capturedWorkload, record.Plan.Submission.Workload); err != nil ||
			capturedWorkload.UID != record.Plan.WorkloadIdentity.UID ||
			capturedWorkload.Replicas != record.Plan.Submission.ExpectedReplicas {
			return e.block(ctx, record, "SCALE_IN_TARGET_IDENTITY_CHANGED")
		}
	}

	resolved := make([]ResolvedMove, 0, end-start)
	targets := make(map[HolderRef]HolderState)
	for index := start; index < end; index++ {
		intent := record.Plan.Submission.Moves[index]
		placement, found := placements[ShardKey{Domain: intent.Domain, VirtualShard: intent.VirtualShard}]
		if !found || !placement.KnownFresh || !placement.LeaseUntil.After(e.now()) {
			return e.fail(ctx, record, "SOURCE_PLACEMENT", ErrConflict)
		}
		if usesVictimExclusion(record.Plan.Submission.Operation) && placement.Holder != record.Plan.Victim {
			return e.block(ctx, record, "VICTIM_SOURCE_GENERATION_CHANGED")
		}
		targetKey := HolderRef{Domain: intent.Domain, HolderID: intent.TargetHolderID, Ordinal: intent.TargetOrdinal}
		target, cached := targets[targetKey]
		if !cached {
			var found bool
			target, found, err = e.holders.GetHolder(ctx, targetKey)
			if err != nil {
				return e.pauseDependency(ctx, record, err)
			}
			if !found {
				return record, ErrPending
			}
			targets[targetKey] = target
		}
		if !eligibleRouteTarget(target, record.Plan.WorkloadIdentity.UID, e.now()) {
			return record, ErrPending
		}
		if record.Plan.Submission.Operation == OperationScaleOut && target.Ref != record.Plan.Target {
			return e.block(ctx, record, "SCALE_OUT_TARGET_GENERATION_CHANGED")
		}
		if record.Plan.Submission.PinnedTarget != (HolderRef{}) && target.Ref != record.Plan.Target {
			return e.block(ctx, record, "DRAIN_TARGET_GENERATION_CHANGED")
		}
		if usesVictimExclusion(record.Plan.Submission.Operation) {
			if !podPresent(capturedWorkload, target.Ref) {
				return e.block(ctx, record, "SCALE_IN_TARGET_IDENTITY_CHANGED")
			}
		}
		resolvedMove, err := ResolveChildMove(record.Plan, index, placement, target)
		if err != nil {
			return e.fail(ctx, record, "CHILD_RESOLUTION", err)
		}
		resolved = append(resolved, resolvedMove)
	}
	if usesVictimExclusion(record.Plan.Submission.Operation) {
		exclusionAfter, found, err := e.exclusions.Get(ctx, record.Plan.Victim)
		if err != nil {
			return e.pauseDependency(ctx, record, err)
		}
		if !found || exclusionAfter != exclusionBefore {
			return e.block(ctx, record, "PLACEMENT_EXCLUSION_CHANGED_DURING_CAPTURE")
		}
	}
	if err := e.placements.Revalidate(ctx, capture.Token); err != nil {
		return record, ErrPending
	}
	return e.appendResolvedMoves(ctx, record, resolved)
}

func (e *Executor) appendResolvedMoves(ctx context.Context, record PlanRecord, moves []ResolvedMove) (PlanRecord, error) {
	if len(moves) == 0 {
		return record, nil
	}
	return e.mutate(ctx, record, func(plan *Plan) error {
		plan.ResolvedMoves = append(plan.ResolvedMoves, moves...)
		return nil
	})
}

func (e *Executor) persistResolvedThen(ctx context.Context, record PlanRecord, moves []ResolvedMove, next func(PlanRecord) (PlanRecord, error)) (PlanRecord, error) {
	if len(moves) == 0 {
		return next(record)
	}
	updated, err := e.appendResolvedMoves(ctx, record, moves)
	if err != nil {
		return updated, err
	}
	return next(updated)
}

func (e *Executor) reconcileWaveChildren(ctx context.Context, record PlanRecord, start, end int) (PlanRecord, error) {
	if record.Plan.Submission.Limits.Parallel() {
		return e.reconcileSlidingChildren(ctx, record, start, end)
	}
	if updated, handled, err := e.observeCompletedParallelWave(ctx, record, start, end); handled || err != nil {
		return updated, err
	}
	current := record
	nonterminal := 0
	for index := start; index < end; index++ {
		move := current.Plan.ResolvedMoves[index]
		if move.Verified {
			continue
		}
		expectedChild := move.RoutePlanRequest(current.Plan.Submission.RoutePlanTTLms, current.Plan.PlanID, current.Plan.RecoveryRetainUntilMs, current.Plan.Submission.Limits, len(current.Plan.Submission.Moves))
		view, found, err := e.routePlans.GetMove(ctx, expectedChild)
		if err != nil {
			if errors.Is(err, ErrConflict) {
				if move.EnsureObserved {
					return e.block(ctx, current, "CHILD_PLAN_EVIDENCE_NON_EXACT")
				}
				return e.fail(ctx, current, "CHILD_PLAN_CONFLICT", ErrConflict)
			}
			return e.pauseDependency(ctx, current, err)
		}
		if !found {
			if move.EnsureObserved {
				return e.block(ctx, current, "CHILD_PLAN_EVIDENCE_UNAVAILABLE")
			}
			if nonterminal >= int(current.Plan.Submission.Limits.MaxRoutePlansInflight) {
				return current, ErrPending
			}
			if current.Plan.Submission.Operation == OperationScaleIn {
				if barrierErr := e.validateBeforeScaleInChildEffect(ctx, current, move); barrierErr != nil {
					switch {
					case errors.Is(barrierErr, ErrPending):
						return current, ErrPending
					case errors.Is(barrierErr, ErrConflict):
						return e.block(ctx, current, "SCALE_IN_CHILD_EFFECT_BARRIER_CHANGED")
					default:
						return e.pauseDependency(ctx, current, barrierErr)
					}
				}
			}
			if e.now().UnixMilli() >= current.Plan.RecoveryRetainUntilMs {
				return e.block(ctx, current, "RECOVERY_RETENTION_EXCEEDED")
			}
			view, err = e.routePlans.EnsureMove(ctx, expectedChild)
			found = err == nil
			if errors.Is(err, ErrOutcomeUnknown) {
				view, found, err = e.routePlans.GetMove(ctx, expectedChild)
			}
			if err != nil {
				if errors.Is(err, ErrConflict) {
					if move.EnsureObserved {
						return e.block(ctx, current, "CHILD_PLAN_EVIDENCE_NON_EXACT")
					}
					return e.fail(ctx, current, "CHILD_PLAN_CONFLICT", ErrConflict)
				}
				return e.pauseDependency(ctx, current, err)
			}
			if !found {
				return current, ErrPending
			}
		}
		if view.PlanID != move.ChildPlanID || view.PayloadDigest != move.ChildPayloadDigest {
			if move.EnsureObserved {
				return e.block(ctx, current, "CHILD_PLAN_EVIDENCE_NON_EXACT")
			}
			return e.fail(ctx, current, "CHILD_PLAN_CONFLICT", ErrConflict)
		}
		if !validRoutePlanObservedPhase(view.Phase) {
			if move.EnsureObserved {
				return e.block(ctx, current, "CHILD_PLAN_EVIDENCE_MALFORMED")
			}
			return e.fail(ctx, current, "CHILD_PLAN_PHASE_INVALID", ErrDependency)
		}
		if move.EnsureObserved && view.Phase != RoutePlanPhasePaused && view.Phase != RoutePlanPhaseSucceeded &&
			!routePlanNonterminalCanAdvance(move.ChildPhase, view.Phase) {
			return e.block(ctx, current, "CHILD_PLAN_PHASE_REGRESSION")
		}
		switch view.Phase {
		case RoutePlanPhaseFailed:
			if move.EnsureObserved {
				return e.block(ctx, current, "CHILD_ROUTEPLAN_TERMINAL_FAILED")
			}
			return e.fail(ctx, current, "CHILD_ROUTEPLAN_FAILED", ErrDependency)
		case RoutePlanPhasePaused:
			if retryableChildPause(view.ErrorCode) {
				return current, ErrPending
			}
			return e.block(ctx, current, "CHILD_ROUTEPLAN_PAUSED")
		case RoutePlanPhaseSucceeded:
			if move.EnsureObserved && move.ChildPhase == RoutePlanPhaseSucceeded {
				continue
			}
			updated, updateErr := e.mutate(ctx, current, func(plan *Plan) error {
				plan.ResolvedMoves[index].EnsureObserved = true
				plan.ResolvedMoves[index].ChildPhase = RoutePlanPhaseSucceeded
				// Preserve the old 1/1 checkpoint shape: the sole child
				// observation and MOVING_WAVE -> VERIFYING_WAVE edge are one
				// durable CAS. Parallel waves retain terminal observations in
				// MOVING_WAVE until every child is ready.
				if plan.Submission.Limits.LegacySerial() {
					plan.Phase = PhaseVerifyingWave
				}
				return nil
			})
			if updateErr != nil {
				return updated, updateErr
			}
			current = updated
		default:
			nonterminal++
			if move.EnsureObserved && move.ChildPhase == view.Phase {
				continue
			}
			updated, updateErr := e.mutate(ctx, current, func(plan *Plan) error {
				plan.ResolvedMoves[index].EnsureObserved = true
				plan.ResolvedMoves[index].ChildPhase = view.Phase
				return nil
			})
			if updateErr != nil {
				return updated, updateErr
			}
			current = updated
		}
	}
	if !waveReadyForVerification(current.Plan, start, end) {
		return current, ErrPending
	}
	if current.Plan.Phase == PhaseVerifyingWave {
		return current, nil
	}
	return e.mutate(ctx, current, func(plan *Plan) error {
		plan.Phase = PhaseVerifyingWave
		return nil
	})
}

// reconcileSlidingChildren maintains a bounded sliding window of independently
// durable child RoutePlans. MaxRoutePlansInflight is a slot count, not a fixed
// wave: as soon as one child succeeds its slot may be filled by the next
// unresolved move. Each child remains independently fenced and independently
// publishes its Snapshot. Parent observations are persisted in one CAS so the
// checkpoint cost does not grow by one full-plan rewrite per shard.
func (e *Executor) reconcileSlidingChildren(ctx context.Context, record PlanRecord, start, end int) (PlanRecord, error) {
	if updated, handled, err := e.observeCompletedParallelWave(ctx, record, start, end); handled || err != nil {
		return updated, err
	}
	type childObservation struct {
		index    int
		phase    string
		verified bool
	}
	type childState struct {
		request RoutePlanMoveRequest
		view    RoutePlanView
		found   bool
	}
	states := make([]childState, end-start)
	inflight := 0
	for index := start; index < end; index++ {
		move := record.Plan.ResolvedMoves[index]
		if move.Verified {
			continue
		}
		expectedChild := move.RoutePlanRequest(record.Plan.Submission.RoutePlanTTLms, record.Plan.PlanID,
			record.Plan.RecoveryRetainUntilMs, record.Plan.Submission.Limits, len(record.Plan.Submission.Moves))
		view, found, err := e.routePlans.GetMove(ctx, expectedChild)
		if err != nil {
			if errors.Is(err, ErrConflict) {
				if move.EnsureObserved {
					return e.block(ctx, record, "CHILD_PLAN_EVIDENCE_NON_EXACT")
				}
				return e.fail(ctx, record, "CHILD_PLAN_CONFLICT", ErrConflict)
			}
			return e.pauseDependency(ctx, record, err)
		}
		states[index-start] = childState{request: expectedChild, view: view, found: found}
		if found && slidingChildConsumesSlot(view) {
			inflight++
		}
	}
	limit := int(record.Plan.Submission.Limits.MaxRoutePlansInflight)
	if limit <= 0 {
		return e.fail(ctx, record, "CHILD_WINDOW_INVALID", ErrCorruptRecord)
	}
	observations := make([]childObservation, 0, end-start)
	for index := start; index < end; index++ {
		move := record.Plan.ResolvedMoves[index]
		if move.Verified {
			continue
		}
		state := states[index-start]
		expectedChild, view, found := state.request, state.view, state.found
		var err error
		if !found {
			if move.EnsureObserved {
				return e.block(ctx, record, "CHILD_PLAN_EVIDENCE_UNAVAILABLE")
			}
			if inflight >= limit {
				continue
			}
			if record.Plan.Submission.Operation == OperationScaleIn {
				if barrierErr := e.validateBeforeScaleInChildEffect(ctx, record, move); barrierErr != nil {
					switch {
					case errors.Is(barrierErr, ErrPending):
						return record, ErrPending
					case errors.Is(barrierErr, ErrConflict):
						return e.block(ctx, record, "SCALE_IN_CHILD_EFFECT_BARRIER_CHANGED")
					default:
						return e.pauseDependency(ctx, record, barrierErr)
					}
				}
			}
			if e.now().UnixMilli() >= record.Plan.RecoveryRetainUntilMs {
				return e.block(ctx, record, "RECOVERY_RETENTION_EXCEEDED")
			}
			view, err = e.routePlans.EnsureMove(ctx, expectedChild)
			found = err == nil
			if errors.Is(err, ErrOutcomeUnknown) {
				view, found, err = e.routePlans.GetMove(ctx, expectedChild)
			}
			if err != nil {
				if errors.Is(err, ErrConflict) {
					return e.fail(ctx, record, "CHILD_PLAN_CONFLICT", ErrConflict)
				}
				return e.pauseDependency(ctx, record, err)
			}
			if !found {
				return record, ErrPending
			}
			if slidingChildConsumesSlot(view) {
				inflight++
			}
		}
		if view.PlanID != move.ChildPlanID || view.PayloadDigest != move.ChildPayloadDigest {
			if move.EnsureObserved {
				return e.block(ctx, record, "CHILD_PLAN_EVIDENCE_NON_EXACT")
			}
			return e.fail(ctx, record, "CHILD_PLAN_CONFLICT", ErrConflict)
		}
		if !validRoutePlanObservedPhase(view.Phase) {
			if move.EnsureObserved {
				return e.block(ctx, record, "CHILD_PLAN_EVIDENCE_MALFORMED")
			}
			return e.fail(ctx, record, "CHILD_PLAN_PHASE_INVALID", ErrDependency)
		}
		if move.EnsureObserved && view.Phase != RoutePlanPhasePaused && view.Phase != RoutePlanPhaseSucceeded &&
			!routePlanNonterminalCanAdvance(move.ChildPhase, view.Phase) {
			return e.block(ctx, record, "CHILD_PLAN_PHASE_REGRESSION")
		}
		switch view.Phase {
		case RoutePlanPhaseFailed:
			if move.EnsureObserved {
				return e.block(ctx, record, "CHILD_ROUTEPLAN_TERMINAL_FAILED")
			}
			return e.fail(ctx, record, "CHILD_ROUTEPLAN_FAILED", ErrDependency)
		case RoutePlanPhasePaused:
			if retryableChildPause(view.ErrorCode) {
				// A transiently paused shard retains its own slot, but does not
				// prevent unrelated child intents from being created or observed.
				continue
			}
			return e.block(ctx, record, "CHILD_ROUTEPLAN_PAUSED")
		case RoutePlanPhaseSucceeded:
			verification, verifyErr := e.routePlans.Verify(ctx, move.ChildPlanID)
			if verifyErr != nil {
				return e.pauseDependency(ctx, record, verifyErr)
			}
			if verification.PlanID != move.ChildPlanID {
				return e.block(ctx, record, "CHILD_VERIFICATION_NON_EXACT")
			}
			if !verification.Consistent {
				if retryableChildVerification(verification.ErrorCode) {
					// This shard stays unverified, but unrelated shards keep
					// their independent sliding-window progress.
					observations = append(observations, childObservation{index: index, phase: view.Phase})
					continue
				}
				return e.block(ctx, record, "CHILD_VERIFICATION_NON_EXACT")
			}
			observations = append(observations, childObservation{index: index, phase: view.Phase, verified: true})
		default:
			if !move.EnsureObserved || move.ChildPhase != view.Phase {
				observations = append(observations, childObservation{index: index, phase: view.Phase})
			}
		}
	}
	if len(observations) == 0 {
		return record, ErrPending
	}
	return e.mutate(ctx, record, func(plan *Plan) error {
		for _, observation := range observations {
			plan.ResolvedMoves[observation.index].EnsureObserved = true
			plan.ResolvedMoves[observation.index].ChildPhase = observation.phase
			if observation.verified {
				plan.ResolvedMoves[observation.index].Verified = true
			}
		}
		if waveVerified(*plan, start, end) {
			advanceAfterVerifiedWave(plan, end)
		} else if waveReadyForVerification(*plan, start, end) {
			plan.Phase = PhaseVerifyingWave
		}
		return nil
	})
}

func slidingChildConsumesSlot(view RoutePlanView) bool {
	switch view.Phase {
	case RoutePlanPhasePending, RoutePlanPhaseDraining, RoutePlanPhaseAssigning,
		RoutePlanPhasePreparing, RoutePlanPhaseActive:
		return true
	case RoutePlanPhasePaused:
		return retryableChildPause(view.ErrorCode)
	default:
		return false
	}
}

// observeCompletedParallelWave collapses the common case where every child in
// a parallel wave has already succeeded into one parent CAS. Child RoutePlans
// remain independently durable; this only avoids rewriting an ever-growing
// ScalePlan once per child while observing a completed wave. Any exceptional or
// nonterminal child falls back to the detailed fail-closed path above.
func (e *Executor) observeCompletedParallelWave(ctx context.Context, record PlanRecord, start, end int) (PlanRecord, bool, error) {
	if !record.Plan.Submission.Limits.Parallel() || start < 0 || end <= start || len(record.Plan.ResolvedMoves) < end {
		return record, false, nil
	}
	for index := start; index < end; index++ {
		move := record.Plan.ResolvedMoves[index]
		if move.Verified {
			if !childReadyForWaveVerification(move) {
				return record, false, nil
			}
			continue
		}
		view, found, err := e.routePlans.GetMove(ctx, move.RoutePlanRequest(
			record.Plan.Submission.RoutePlanTTLms, record.Plan.PlanID,
			record.Plan.RecoveryRetainUntilMs, record.Plan.Submission.Limits,
			len(record.Plan.Submission.Moves)))
		if err != nil || !found || view.PlanID != move.ChildPlanID ||
			view.PayloadDigest != move.ChildPayloadDigest || view.Phase != RoutePlanPhaseSucceeded {
			return record, false, nil
		}
	}
	updated, err := e.mutate(ctx, record, func(plan *Plan) error {
		for index := start; index < end; index++ {
			if plan.ResolvedMoves[index].Verified {
				continue
			}
			plan.ResolvedMoves[index].EnsureObserved = true
			plan.ResolvedMoves[index].ChildPhase = RoutePlanPhaseSucceeded
		}
		plan.Phase = PhaseVerifyingWave
		return nil
	})
	return updated, true, err
}

func waveReadyForVerification(plan Plan, start, end int) bool {
	if len(plan.ResolvedMoves) < end {
		return false
	}
	for index := start; index < end; index++ {
		if !childReadyForWaveVerification(plan.ResolvedMoves[index]) {
			return false
		}
	}
	return true
}

func waveVerified(plan Plan, start, end int) bool {
	if len(plan.ResolvedMoves) < end {
		return false
	}
	for index := start; index < end; index++ {
		if !plan.ResolvedMoves[index].Verified {
			return false
		}
	}
	return true
}

func (e *Executor) verifyWave(ctx context.Context, record PlanRecord) (PlanRecord, error) {
	start := int(record.Plan.WaveIndex)
	end := waveEnd(record.Plan)
	if start < 0 || start >= len(record.Plan.Submission.Moves) || len(record.Plan.ResolvedMoves) != end {
		return e.fail(ctx, record, "CHILD_PLAN_MISSING", ErrCorruptRecord)
	}
	if updated, handled, err := e.verifyCompletedParallelWave(ctx, record, start, end); handled || err != nil {
		return updated, err
	}
	current := record
	for index := start; index < end; index++ {
		move := current.Plan.ResolvedMoves[index]
		if move.Verified {
			continue
		}
		view, found, err := e.routePlans.GetMove(ctx, move.RoutePlanRequest(current.Plan.Submission.RoutePlanTTLms, current.Plan.PlanID, current.Plan.RecoveryRetainUntilMs, current.Plan.Submission.Limits, len(current.Plan.Submission.Moves)))
		if err != nil {
			if errors.Is(err, ErrConflict) {
				return e.block(ctx, current, "CHILD_PLAN_EVIDENCE_NON_EXACT")
			}
			return e.pauseDependency(ctx, current, err)
		}
		if !found || view.PlanID != move.ChildPlanID || view.PayloadDigest != move.ChildPayloadDigest {
			return e.block(ctx, current, "CHILD_PLAN_EVIDENCE_UNAVAILABLE")
		}
		if !validRoutePlanObservedPhase(view.Phase) {
			return e.block(ctx, current, "CHILD_PLAN_EVIDENCE_MALFORMED")
		}
		if view.Phase != RoutePlanPhaseSucceeded {
			if view.Phase == RoutePlanPhasePaused {
				if retryableChildPause(view.ErrorCode) {
					return current, ErrPending
				}
				return e.block(ctx, current, "CHILD_ROUTEPLAN_PAUSED")
			}
			if view.Phase == RoutePlanPhaseFailed {
				return e.block(ctx, current, "CHILD_ROUTEPLAN_TERMINAL_FAILED")
			}
			return e.block(ctx, current, "CHILD_PLAN_PHASE_REGRESSION")
		}
		verification, err := e.routePlans.Verify(ctx, move.ChildPlanID)
		if err != nil {
			return e.pauseDependency(ctx, current, err)
		}
		if verification.PlanID != move.ChildPlanID {
			return e.block(ctx, current, "CHILD_VERIFICATION_NON_EXACT")
		}
		if !verification.Consistent {
			if retryableChildVerification(verification.ErrorCode) {
				return e.pauseDependency(ctx, current, ErrDependency)
			}
			return e.block(ctx, current, "CHILD_VERIFICATION_NON_EXACT")
		}
		updated, updateErr := e.mutate(ctx, current, func(plan *Plan) error {
			plan.ResolvedMoves[index].Verified = true
			plan.ResolvedMoves[index].ChildPhase = RoutePlanPhaseSucceeded
			if waveVerified(*plan, start, end) {
				advanceAfterVerifiedWave(plan, end)
			}
			return nil
		})
		if updateErr != nil {
			return updated, updateErr
		}
		return updated, nil
	}
	if waveVerified(current.Plan, start, end) {
		return e.mutate(ctx, current, func(plan *Plan) error {
			advanceAfterVerifiedWave(plan, end)
			return nil
		})
	}
	return current, ErrPending
}

// verifyCompletedParallelWave verifies at most one sliding-window worth of
// successful children and persists that progress in one CAS. Normal execution
// verifies children as they finish; this bounded path is for restart recovery
// and older records that reached VERIFYING_WAVE without per-child evidence.
// It must not monopolize the shared controller lock by re-verifying an
// unbounded manifest in one reconcile.
func (e *Executor) verifyCompletedParallelWave(ctx context.Context, record PlanRecord, start, end int) (PlanRecord, bool, error) {
	if !record.Plan.Submission.Limits.Parallel() || start < 0 || end <= start || len(record.Plan.ResolvedMoves) != end {
		return record, false, nil
	}
	limit := int(record.Plan.Submission.Limits.MaxRoutePlansInflight)
	if limit <= 0 {
		return record, false, nil
	}
	verified := make([]int, 0, limit)
	for index := start; index < end && len(verified) < limit; index++ {
		move := record.Plan.ResolvedMoves[index]
		if move.Verified {
			continue
		}
		view, found, err := e.routePlans.GetMove(ctx, move.RoutePlanRequest(
			record.Plan.Submission.RoutePlanTTLms, record.Plan.PlanID,
			record.Plan.RecoveryRetainUntilMs, record.Plan.Submission.Limits,
			len(record.Plan.Submission.Moves)))
		if err != nil || !found || view.PlanID != move.ChildPlanID ||
			view.PayloadDigest != move.ChildPayloadDigest || view.Phase != RoutePlanPhaseSucceeded {
			return record, false, nil
		}
		verification, err := e.routePlans.Verify(ctx, move.ChildPlanID)
		if err != nil || verification.PlanID != move.ChildPlanID || !verification.Consistent {
			return record, false, nil
		}
		verified = append(verified, index)
	}
	if len(verified) == 0 {
		return record, false, nil
	}
	updated, err := e.mutate(ctx, record, func(plan *Plan) error {
		for _, index := range verified {
			if plan.ResolvedMoves[index].Verified {
				continue
			}
			plan.ResolvedMoves[index].Verified = true
			plan.ResolvedMoves[index].ChildPhase = RoutePlanPhaseSucceeded
		}
		if waveVerified(*plan, start, end) {
			advanceAfterVerifiedWave(plan, end)
		}
		return nil
	})
	if err == nil && !waveVerified(updated.Plan, start, end) {
		err = ErrPending
	}
	return updated, true, err
}

func (e *Executor) waitZeroShard(ctx context.Context, record PlanRecord) (PlanRecord, error) {
	holder, found, err := e.holders.GetHolder(ctx, record.Plan.Victim)
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	if !found || holder.Ref != record.Plan.Victim {
		return e.fail(ctx, record, "VICTIM_BOOT_CHANGED", ErrConflict)
	}
	exclusion, excluded, err := e.exclusions.Get(ctx, record.Plan.Victim)
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	capture, err := e.placements.CaptureHolderShards(ctx, record.Plan.Victim)
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	if capture.Holder != record.Plan.Victim || !excluded || !exclusion.Excluded || exclusion.OwnerPlanID != record.Plan.PlanID ||
		exclusion.Revision != record.Plan.ExclusionIntent.ObservedRevision || holder.PlacementEligible ||
		holder.PlacementRevision != exclusion.Revision ||
		!holder.AssignmentKnownFresh || holder.ServingShardCount != 0 ||
		holder.State != HolderReadyZeroShard || activePlacementsFor(capture.Placements, record.Plan.Victim) != 0 {
		return record, ErrPending
	}
	if err := e.placements.Revalidate(ctx, capture.Token); err != nil {
		return record, ErrPending
	}
	state, err := e.workloads.Get(ctx, record.Plan.Submission.Workload)
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	if err := validateWorkloadState(state, record.Plan.Submission.Workload); err != nil {
		return e.fail(ctx, record, "WORKLOAD_IDENTITY_JOIN", err)
	}
	if state.UID != record.Plan.WorkloadIdentity.UID || state.Replicas != record.Plan.Submission.ExpectedReplicas ||
		(record.Plan.Submission.Operation == OperationScaleIn && !victimStillHighest(state, record.Plan.Victim)) ||
		(record.Plan.Submission.Operation == OperationDrainHolder && !podPresent(state, record.Plan.Victim)) {
		return e.fail(ctx, record, "SCALE_DOWN_PRECONDITION", ErrConflict)
	}
	if state.ResourceVersion != record.Plan.WorkloadIdentity.ResourceVersion {
		return e.block(ctx, record, "WORKLOAD_RESOURCE_VERSION_CHANGED")
	}
	return e.mutate(ctx, record, func(plan *Plan) error {
		if plan.Submission.Operation == OperationDrainHolder {
			plan.Phase = PhaseVerifyingFinal
			return nil
		}
		plan.ScaleIntent = &ScaleIntent{WorkloadUID: state.UID, ExpectedResourceVersion: state.ResourceVersion,
			ExpectedReplicas: plan.Submission.ExpectedReplicas, DesiredReplicas: plan.Submission.DesiredReplicas, Effect: EffectPending}
		plan.Phase = PhaseScalingDown
		return nil
	})
}

func (e *Executor) verifyFinal(ctx context.Context, record PlanRecord) (PlanRecord, error) {
	state, err := e.workloads.Get(ctx, record.Plan.Submission.Workload)
	if err != nil {
		return e.pauseDependency(ctx, record, err)
	}
	if err := validateWorkloadState(state, record.Plan.Submission.Workload); err != nil {
		return e.fail(ctx, record, "WORKLOAD_IDENTITY_JOIN", err)
	}
	if state.UID != record.Plan.WorkloadIdentity.UID {
		return e.fail(ctx, record, "WORKLOAD_IDENTITY_CHANGED", ErrConflict)
	}
	if state.Replicas != record.Plan.Submission.DesiredReplicas {
		return record, ErrPending
	}
	if record.Plan.Submission.Operation == OperationScaleOut {
		keys := make([]ShardKey, 0, len(record.Plan.ResolvedMoves))
		for _, move := range record.Plan.ResolvedMoves {
			keys = append(keys, ShardKey{Domain: move.Intent.Domain, VirtualShard: move.Intent.VirtualShard})
		}
		capture, err := e.placements.CaptureShards(ctx, keys)
		if err != nil {
			return e.pauseDependency(ctx, record, err)
		}
		if len(capture.Placements) != len(record.Plan.ResolvedMoves) {
			return record, ErrPending
		}
		observed := make(map[ShardKey]ShardPlacement, len(capture.Placements))
		for _, placement := range capture.Placements {
			key := ShardKey{Domain: placement.Domain, VirtualShard: placement.VirtualShard}
			if _, duplicate := observed[key]; duplicate {
				return e.fail(ctx, record, "FINAL_PLACEMENT_DUPLICATE", ErrConflict)
			}
			observed[key] = placement
		}
		for _, move := range record.Plan.ResolvedMoves {
			placement, found := observed[ShardKey{Domain: move.Intent.Domain, VirtualShard: move.Intent.VirtualShard}]
			if !found || !placement.Active || !placement.KnownFresh || !placement.LeaseUntil.After(e.now()) ||
				placement.Holder != move.Target {
				return record, ErrPending
			}
		}
		if err := e.placements.Revalidate(ctx, capture.Token); err != nil {
			return record, ErrPending
		}
	} else if record.Plan.Submission.Operation == OperationScaleIn {
		for _, pod := range state.Pods {
			if pod.Present && pod.Ordinal == record.Plan.Victim.Ordinal {
				return record, ErrPending
			}
		}
	} else {
		if !podPresent(state, record.Plan.Victim) {
			return e.fail(ctx, record, "DRAIN_VICTIM_CHANGED", ErrConflict)
		}
		holder, found, err := e.holders.GetHolder(ctx, record.Plan.Victim)
		if err != nil {
			return e.pauseDependency(ctx, record, err)
		}
		exclusion, excluded, err := e.exclusions.Get(ctx, record.Plan.Victim)
		if err != nil {
			return e.pauseDependency(ctx, record, err)
		}
		capture, err := e.placements.CaptureHolderShards(ctx, record.Plan.Victim)
		if err != nil {
			return e.pauseDependency(ctx, record, err)
		}
		if !found || holder.Ref != record.Plan.Victim || holder.ServingShardCount != 0 ||
			holder.State != HolderReadyZeroShard || holder.PlacementEligible || !holder.AssignmentKnownFresh ||
			!excluded || !exclusion.Excluded || exclusion.OwnerPlanID != record.Plan.PlanID ||
			exclusion.Revision != record.Plan.ExclusionIntent.ObservedRevision ||
			capture.Holder != record.Plan.Victim || activePlacementsFor(capture.Placements, record.Plan.Victim) != 0 {
			return record, ErrPending
		}
		if err := e.placements.Revalidate(ctx, capture.Token); err != nil {
			return record, ErrPending
		}
	}
	return e.mutate(ctx, record, func(plan *Plan) error {
		plan.Status = StatusSucceeded
		plan.CompletedAtMs = e.auditNow(plan.UpdatedAtMs)
		plan.UpdatedAtMs = plan.CompletedAtMs
		return nil
	})
}

func (e *Executor) validateBeforeScaleInChildEffect(ctx context.Context, record PlanRecord, move ResolvedMove) error {
	exclusionBefore, found, err := e.exclusions.Get(ctx, record.Plan.Victim)
	if err != nil {
		return err
	}
	if !found || !exclusionBefore.Excluded || exclusionBefore.Holder != record.Plan.Victim ||
		exclusionBefore.OwnerPlanID != record.Plan.PlanID || exclusionBefore.Revision != record.Plan.ExclusionIntent.ObservedRevision {
		return ErrConflict
	}
	victim, found, err := e.getPinnedHolder(ctx, record.Plan.Victim)
	if err != nil {
		return err
	}
	if !found {
		return ErrPending
	}
	if victim.Ref != record.Plan.Victim || !pinnedScaleInVictimAdmissible(victim, exclusionBefore, e.now()) {
		return ErrConflict
	}
	// The complete victim manifest, exact source generation and per-shard
	// Authority fence were captured and durably bracketed before MOVING_WAVE.
	// Do not recapture the domain-global placement view for every child: a
	// sibling RoutePlan legitimately advances the global Snapshot while this
	// child is being admitted, and that must not become a cross-shard barrier.
	// The child RoutePlan binds move.ExpectedSourceFence and revalidates the
	// exact shard's current Snapshot tuple immediately before its Authority CAS;
	// stale same-shard intent therefore remains fail-closed at the actual
	// writer-effect boundary.
	target, found, err := e.getPinnedHolder(ctx, move.Target)
	if err != nil {
		return err
	}
	if !found {
		return ErrPending
	}
	if target.Ref != move.Target || !pinnedScaleInTargetAdmissible(target, record.Plan.WorkloadIdentity.UID, e.now()) {
		return ErrConflict
	}
	workload, err := e.workloads.Get(ctx, record.Plan.Submission.Workload)
	if err != nil {
		return err
	}
	if err := validateWorkloadState(workload, record.Plan.Submission.Workload); err != nil ||
		workload.UID != record.Plan.WorkloadIdentity.UID || workload.Replicas != record.Plan.Submission.ExpectedReplicas ||
		!podPresent(workload, move.Target) {
		return ErrConflict
	}
	exclusionAfter, found, err := e.exclusions.Get(ctx, record.Plan.Victim)
	if err != nil {
		return err
	}
	if !found || exclusionAfter != exclusionBefore {
		return ErrConflict
	}
	victimAfter, found, err := e.getPinnedHolder(ctx, record.Plan.Victim)
	if err != nil {
		return err
	}
	if !found {
		return ErrPending
	}
	if !stableScaleInVictimAcrossChildBarrier(victim, victimAfter, exclusionBefore, e.now()) {
		return ErrConflict
	}
	return nil
}

// getPinnedHolder distinguishes controller-cache warmup from an observed
// replacement generation. An absent exact generation is retryable only while
// no holder with the same stable identity is visible. If the holder ID and
// ordinal are present with a different PodUID/boot ID, the immutable ScalePlan
// identity has genuinely changed and the caller must fail closed.
func (e *Executor) getPinnedHolder(ctx context.Context, expected HolderRef) (HolderState, bool, error) {
	holder, found, err := e.holders.GetHolder(ctx, expected)
	if err != nil || found {
		return holder, found, err
	}
	observed, replacementFound, err := e.holders.GetHolder(ctx, HolderRef{
		Domain: expected.Domain, HolderID: expected.HolderID, Ordinal: expected.Ordinal,
	})
	if err != nil {
		return HolderState{}, false, err
	}
	if replacementFound {
		return observed, false, ErrConflict
	}
	return HolderState{}, false, nil
}

// stableScaleInVictimAcrossChildBarrier compares the placement generation and
// lifecycle facts that make a child effect safe. Holder heartbeats are allowed
// to renew LeaseUntil and to observe a newer, unrelated global Snapshot while
// the barrier is open; treating either monotonic refresh as identity drift can
// permanently block a large scale-in while its child records are being
// persisted. ProcessReady may also toggle false while an unrelated shard's
// ASSIGNING Snapshot is being installed. That global composition signal is not
// a per-shard writer fence: the exact Pod/workload/boot identity, live lease,
// endpoints, exclusion generation and the child RoutePlan's exact per-shard
// Authority fence remain authoritative.
// A sibling child owned by this ScalePlan can complete between the two Holder
// reads, so the victim's serving set may shrink but never grow. The child
// RoutePlan revalidates its own shard tuple immediately before the Authority
// CAS; unrelated Snapshot publications are deliberately not a parent barrier.
func stableScaleInVictimAcrossChildBarrier(before, after HolderState, exclusion PlacementExclusion, now time.Time) bool {
	return after.Ref == before.Ref &&
		after.PodName == before.PodName &&
		after.LogicEndpoint == before.LogicEndpoint &&
		after.HolderControlEndpoint == before.HolderControlEndpoint &&
		after.TLSDNSIdentity == before.TLSDNSIdentity &&
		after.State == HolderServing && !after.Draining &&
		after.ServingShardCount <= before.ServingShardCount && after.ServingShardCount != 0 &&
		after.AssignmentKnownFresh && after.SnapshotRevision >= before.SnapshotRevision &&
		!after.PlacementEligible && after.PlacementRevision == exclusion.Revision &&
		!after.LeaseUntil.Before(before.LeaseUntil) && after.LeaseUntil.After(now)
}

func pinnedScaleInVictimAdmissible(holder HolderState, exclusion PlacementExclusion, now time.Time) bool {
	return holder.Ref.Exact() && holder.State == HolderServing && !holder.Draining && holder.ServingShardCount > 0 &&
		!holder.PlacementEligible && holder.PlacementRevision == exclusion.Revision && holder.AssignmentKnownFresh &&
		holder.SnapshotRevision > 0 && holder.LogicEndpoint != "" && holder.HolderControlEndpoint != "" &&
		holder.TLSDNSIdentity != "" && holder.LeaseUntil.After(now)
}

func pinnedScaleInTargetAdmissible(holder HolderState, workloadUID string, now time.Time) bool {
	if !holder.Ref.Exact() || holder.Ref.WorkloadUID != workloadUID || !holder.PlacementEligible || holder.Draining ||
		!holder.AssignmentKnownFresh || holder.SnapshotRevision == 0 || holder.LogicEndpoint == "" ||
		holder.HolderControlEndpoint == "" || holder.TLSDNSIdentity == "" || !holder.LeaseUntil.After(now) {
		return false
	}
	if holder.State == HolderReadyZeroShard {
		return holder.ProcessReady && holder.ServingShardCount == 0
	}
	// Once the first child has activated, the target is no longer a
	// zero-shard holder. It may be temporarily assignment-unready while a
	// sibling shard Snapshot converges, but the child RoutePlan still requires
	// the exact target generation to apply its own published revision before
	// that shard becomes ACTIVE.
	return holder.State == HolderServing && holder.ServingShardCount > 0
}

func (e *Executor) validateBeforeScaleDownEffect(ctx context.Context, record PlanRecord, workload WorkloadState) error {
	if workload.UID != record.Plan.WorkloadIdentity.UID || workload.Replicas != record.Plan.Submission.ExpectedReplicas ||
		!victimStillHighest(workload, record.Plan.Victim) {
		return ErrConflict
	}
	exclusionBefore, found, err := e.exclusions.Get(ctx, record.Plan.Victim)
	if err != nil {
		return err
	}
	if !found || !exclusionBefore.Excluded || exclusionBefore.Holder != record.Plan.Victim ||
		exclusionBefore.OwnerPlanID != record.Plan.PlanID || exclusionBefore.Revision != record.Plan.ExclusionIntent.ObservedRevision {
		return ErrConflict
	}
	holder, found, err := e.holders.GetHolder(ctx, record.Plan.Victim)
	if err != nil {
		return err
	}
	if !found || holder.Ref != record.Plan.Victim || holder.PlacementEligible ||
		holder.PlacementRevision != exclusionBefore.Revision || !holder.AssignmentKnownFresh ||
		holder.ServingShardCount != 0 || holder.State != HolderReadyZeroShard {
		return ErrConflict
	}
	capture, err := e.placements.CaptureHolderShards(ctx, record.Plan.Victim)
	if err != nil {
		return err
	}
	if capture.Holder != record.Plan.Victim || activePlacementsFor(capture.Placements, record.Plan.Victim) != 0 {
		return ErrConflict
	}
	exclusionAfter, found, err := e.exclusions.Get(ctx, record.Plan.Victim)
	if err != nil {
		return err
	}
	if !found || exclusionAfter != exclusionBefore {
		return ErrConflict
	}
	if err := e.placements.Revalidate(ctx, capture.Token); err != nil {
		return ErrPending
	}
	return nil
}

func advanceAfterVerifiedWave(plan *Plan, end int) {
	if end < len(plan.Submission.Moves) {
		plan.WaveIndex = uint32(end)
		plan.Phase = PhaseMovingWave
		return
	}
	if plan.Submission.Limits.LegacySerial() {
		// Preserve the old terminal serial checkpoint for already persisted
		// 1/1 plans. Parallel 8/8 and 20/20 plans use the exclusive completed-wave index.
		plan.WaveIndex = uint32(end - 1)
	} else {
		plan.WaveIndex = uint32(end)
	}
	if plan.Submission.Operation == OperationScaleOut {
		plan.Phase = PhaseVerifyingFinal
	} else {
		plan.Phase = PhaseWaitingZeroShard
	}
}

func (e *Executor) Pause(ctx context.Context, planID, reason string) (PlanRecord, error) {
	if e == nil || ctx == nil {
		return PlanRecord{}, ErrInvalidPlan
	}
	guard, err := e.enterPlanMutation(ctx)
	if err != nil {
		return PlanRecord{}, err
	}
	defer releaseGuard(guard, ctx)
	ctx = guard.Context()
	return e.pauseUnlocked(ctx, planID, reason)
}

func (e *Executor) pauseUnlocked(ctx context.Context, planID, reason string) (PlanRecord, error) {
	record, found, err := e.repository.Load(ctx, planID)
	if err != nil || !found {
		if err == nil {
			err = ErrPlanNotFound
		}
		return record, err
	}
	if reason != "OPERATOR_PAUSED" && reason != "DEPENDENCY_TEMPORARILY_UNAVAILABLE" {
		return record, fmt.Errorf("%w: pause reason", ErrInvalidTransition)
	}
	return e.pause(ctx, record, reason)
}

func (e *Executor) Resume(ctx context.Context, planID string) (PlanRecord, error) {
	if e == nil || ctx == nil {
		return PlanRecord{}, ErrInvalidPlan
	}
	guard, err := e.enterPlanMutation(ctx)
	if err != nil {
		return PlanRecord{}, err
	}
	defer releaseGuard(guard, ctx)
	ctx = guard.Context()
	return e.resumeUnlocked(ctx, planID)
}

func (e *Executor) resumeUnlocked(ctx context.Context, planID string) (PlanRecord, error) {
	record, found, err := e.repository.Load(ctx, planID)
	if err != nil || !found {
		if err == nil {
			err = ErrPlanNotFound
		}
		return record, err
	}
	if record.Plan.Status != StatusPaused ||
		(record.Plan.PausedReason != "OPERATOR_PAUSED" && record.Plan.PausedReason != "DEPENDENCY_TEMPORARILY_UNAVAILABLE" && record.Plan.PausedReason != "DEPENDENCY_UNAVAILABLE") {
		return record, fmt.Errorf("%w: pause is not resumable", ErrInvalidTransition)
	}
	return e.mutate(ctx, record, func(plan *Plan) error {
		plan.Status = StatusRunning
		plan.PausedReason = ""
		return nil
	})
}

// ReconcileBlocked performs read-only external observation. It can adopt a
// factually complete PAUSED child, or a SUCCEEDED child whose earlier
// verification raced a later exact production Verify, only when that Verify
// reports the exact deterministic child as consistent.
func (e *Executor) ReconcileBlocked(ctx context.Context, planID string) (PlanRecord, error) {
	if e == nil || ctx == nil {
		return PlanRecord{}, ErrInvalidPlan
	}
	guard, err := e.enterPlanMutation(ctx)
	if err != nil {
		return PlanRecord{}, err
	}
	defer releaseGuard(guard, ctx)
	ctx = guard.Context()
	record, found, err := e.repository.Load(ctx, planID)
	if err != nil || !found {
		if err == nil {
			err = ErrPlanNotFound
		}
		return record, err
	}
	if record.Plan.Status != StatusBlocked {
		return record, fmt.Errorf("%w: plan is not blocked", ErrInvalidTransition)
	}
	if blockedExclusionReleaseReason(record.Plan.BlockedReason) {
		if record.Plan.Phase != PhaseReleasingExclusion || record.Plan.CleanupIntent == nil {
			return record, ErrBlocked
		}
		updated, updateErr := e.mutate(ctx, record, func(plan *Plan) error {
			plan.Status = StatusRunning
			plan.BlockedReason = ""
			return nil
		})
		if updateErr != nil {
			return updated, updateErr
		}
		return e.reconcileExclusionRelease(ctx, updated, ErrOutcomeUnknown)
	}
	if record.Plan.BlockedReason == "CHILD_VERIFICATION_NON_EXACT" {
		return e.reconcileBlockedChildVerification(ctx, record)
	}
	if record.Plan.BlockedReason != "CHILD_ROUTEPLAN_PAUSED" {
		switch record.Plan.BlockedReason {
		case "PLACEMENT_EXCLUSION_NOT_VISIBLE", "SCALE_IN_PLACEMENT_STALE", "SCALE_IN_MANIFEST_DRIFT", "PLACEMENT_CAPTURE_CHANGED":
			return e.reconcileBlockedCapture(ctx, record)
		default:
			return record, ErrBlocked
		}
	}
	start := int(record.Plan.WaveIndex)
	end := waveEnd(record.Plan)
	if start < 0 || start >= len(record.Plan.Submission.Moves) || len(record.Plan.ResolvedMoves) < start || len(record.Plan.ResolvedMoves) > end {
		return record, ErrCorruptRecord
	}
	index := -1
	for candidateIndex := start; candidateIndex < len(record.Plan.ResolvedMoves) && candidateIndex < end; candidateIndex++ {
		move := record.Plan.ResolvedMoves[candidateIndex]
		if move.Verified {
			continue
		}
		view, found, getErr := e.routePlans.GetMove(ctx, move.RoutePlanRequest(record.Plan.Submission.RoutePlanTTLms, record.Plan.PlanID, record.Plan.RecoveryRetainUntilMs, record.Plan.Submission.Limits, len(record.Plan.Submission.Moves)))
		if getErr != nil {
			return record, ErrBlocked
		}
		if !found {
			continue
		}
		if view.PlanID != move.ChildPlanID || view.PayloadDigest != move.ChildPayloadDigest {
			return record, ErrBlocked
		}
		if view.Phase != RoutePlanPhasePaused {
			continue
		}
		verification, verifyErr := e.routePlans.Verify(ctx, move.ChildPlanID)
		if verifyErr != nil || verification.PlanID != move.ChildPlanID || !verification.Consistent {
			return record, ErrBlocked
		}
		index = candidateIndex
		break
	}
	if index < 0 {
		return record, ErrBlocked
	}
	if e.now().UnixMilli() >= record.Plan.RecoveryRetainUntilMs {
		updated, updateErr := e.mutate(ctx, record, func(plan *Plan) error {
			plan.BlockedReason = "RECOVERY_RETENTION_EXCEEDED"
			return nil
		})
		if updateErr != nil {
			return updated, updateErr
		}
		return updated, ErrBlocked
	}
	return e.mutate(ctx, record, func(plan *Plan) error {
		plan.ResolvedMoves[index].EnsureObserved = true
		plan.ResolvedMoves[index].ChildPhase = RoutePlanPhasePaused
		plan.ResolvedMoves[index].Verified = true
		plan.ResolvedMoves[index].Reconciled = true
		plan.Status = StatusRunning
		plan.BlockedReason = ""
		if waveReadyForVerification(*plan, start, end) {
			plan.Phase = PhaseVerifyingWave
		} else {
			plan.Phase = PhaseMovingWave
		}
		return nil
	})
}

func retryableChildVerification(code string) bool {
	switch code {
	case "AUTHORITY_READ", "SNAPSHOT_READ", "SNAPSHOT_MISSING_OR_MISMATCH",
		"VERIFICATION_transition", "VERIFICATION_recovery", "VERIFICATION_tombstone",
		"VERIFICATION_gate_resolution":
		return true
	default:
		return false
	}
}

func retryableChildPause(code string) bool {
	switch code {
	case "CANCELED", "TIMEOUT", "authority_committed_snapshot_pending":
		return true
	default:
		return false
	}
}

func (e *Executor) reconcileBlockedChildVerification(ctx context.Context, record PlanRecord) (PlanRecord, error) {
	if record.Plan.Phase != PhaseVerifyingWave {
		return record, ErrBlocked
	}
	start, end := int(record.Plan.WaveIndex), waveEnd(record.Plan)
	if start < 0 || start >= len(record.Plan.ResolvedMoves) || len(record.Plan.ResolvedMoves) != end {
		return record, ErrCorruptRecord
	}
	for index := start; index < end; index++ {
		move := record.Plan.ResolvedMoves[index]
		if move.Verified || move.ChildPhase != RoutePlanPhaseSucceeded {
			continue
		}
		view, found, err := e.routePlans.GetMove(ctx, move.RoutePlanRequest(record.Plan.Submission.RoutePlanTTLms, record.Plan.PlanID, record.Plan.RecoveryRetainUntilMs, record.Plan.Submission.Limits, len(record.Plan.Submission.Moves)))
		if err != nil || !found || view.PlanID != move.ChildPlanID || view.PayloadDigest != move.ChildPayloadDigest ||
			view.Phase != RoutePlanPhaseSucceeded {
			return record, ErrBlocked
		}
		verification, err := e.routePlans.Verify(ctx, move.ChildPlanID)
		if err != nil || verification.PlanID != move.ChildPlanID || !verification.Consistent {
			return record, ErrBlocked
		}
		if e.now().UnixMilli() >= record.Plan.RecoveryRetainUntilMs {
			return record, ErrBlocked
		}
		return e.mutate(ctx, record, func(plan *Plan) error {
			plan.Status = StatusRunning
			plan.BlockedReason = ""
			return nil
		})
	}
	return record, ErrBlocked
}

func (e *Executor) reconcileBlockedCapture(ctx context.Context, record PlanRecord) (PlanRecord, error) {
	if record.Plan.Phase != PhaseCapturingVictimShards || record.Plan.ExclusionIntent == nil {
		return record, ErrBlocked
	}
	exclusion, found, err := e.exclusions.Get(ctx, record.Plan.Victim)
	if err != nil || !found || !exclusion.Excluded || exclusion.OwnerPlanID != record.Plan.PlanID ||
		exclusion.Holder != record.Plan.Victim || exclusion.Revision != record.Plan.ExclusionIntent.ObservedRevision {
		return record, ErrBlocked
	}
	holder, found, err := e.holders.GetHolder(ctx, record.Plan.Victim)
	if err != nil || !found || holder.Ref != record.Plan.Victim || holder.PlacementEligible || holder.Draining ||
		!holder.ProcessReady || !holder.AssignmentKnownFresh || holder.PlacementRevision != exclusion.Revision ||
		!holder.LeaseUntil.After(e.now()) {
		return record, ErrBlocked
	}
	zeroShardPlan := len(record.Plan.Submission.Moves) == 0
	if (zeroShardPlan && (holder.State != HolderReadyZeroShard || holder.ServingShardCount != 0)) ||
		(!zeroShardPlan && (holder.State != HolderServing || holder.ServingShardCount == 0)) {
		return record, ErrBlocked
	}
	capture, err := e.placements.CaptureHolderShards(ctx, record.Plan.Victim)
	if err != nil || capture.Holder != record.Plan.Victim ||
		!manifestExactlyCovers(record.Plan.Submission.Moves, capture.Placements, record.Plan.Victim) {
		return record, ErrBlocked
	}
	for _, placement := range capture.Placements {
		if placement.Active && (!placement.KnownFresh || !placement.LeaseUntil.After(e.now())) {
			return record, ErrBlocked
		}
	}
	if err := e.placements.Revalidate(ctx, capture.Token); err != nil {
		return record, ErrBlocked
	}
	if e.now().UnixMilli() >= record.Plan.RecoveryRetainUntilMs {
		return record, ErrBlocked
	}
	return e.mutate(ctx, record, func(plan *Plan) error {
		plan.Status = StatusRunning
		plan.BlockedReason = ""
		if zeroShardPlan {
			plan.Phase = PhaseWaitingZeroShard
		} else {
			plan.Phase = PhaseMovingWave
		}
		plan.WaveIndex = 0
		return nil
	})
}

// OperatorFailBlocked delegates whole-plan proof and the proof-bound terminal
// CAS to one owner. Executor never receives a reusable safe boolean and never
// performs the workload-release CAS itself.
func (e *Executor) OperatorFailBlocked(ctx context.Context, request BlockedTerminationRequest) (PlanRecord, error) {
	if e == nil || ctx == nil || request.PlanID == "" || request.ExpectedRevision == 0 ||
		request.ExpectedBlockedReason == "" || len(request.ExpectedPayloadDigest) != 64 ||
		len(request.ExpectedCheckpointDigest) != 64 || request.TerminationID == "" {
		return PlanRecord{}, ErrInvalidPlan
	}
	result, err := e.blockedTermination.TerminalizeBlocked(ctx, request)
	if err != nil {
		return result.Record, err
	}
	switch result.Status {
	case BlockedTerminationCommitted, BlockedTerminationAlreadyCommitted:
		return result.Record, nil
	case BlockedTerminationUnsafe, BlockedTerminationProofExpired, BlockedTerminationExpectationMismatch:
		return result.Record, ErrVerification
	case BlockedTerminationRevisionConflict:
		return result.Record, ErrRevisionConflict
	case BlockedTerminationNotBlocked:
		if result.Record.Plan.Status == StatusFailed && result.Record.Plan.BlockedTermination != nil &&
			result.Record.Plan.BlockedTermination.TerminationID == request.TerminationID &&
			result.Record.Plan.BlockedTermination.ExpectedCheckpointDigest == request.ExpectedCheckpointDigest {
			return result.Record, nil
		}
		return result.Record, ErrInvalidTransition
	default:
		return result.Record, ErrCorruptRecord
	}
}

func (e *Executor) pauseDependency(ctx context.Context, record PlanRecord, cause error) (PlanRecord, error) {
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return record, cause
	}
	return e.pause(ctx, record, "DEPENDENCY_UNAVAILABLE")
}

func (e *Executor) pause(ctx context.Context, record PlanRecord, reason string) (PlanRecord, error) {
	if record.Plan.Status == StatusPaused {
		return record, ErrPaused
	}
	updated, err := e.mutate(ctx, record, func(plan *Plan) error {
		plan.Status = StatusPaused
		plan.PausedReason = reason
		return nil
	})
	if err != nil {
		return updated, err
	}
	return updated, ErrPaused
}

func (e *Executor) block(ctx context.Context, record PlanRecord, reason string) (PlanRecord, error) {
	if record.Plan.Status == StatusBlocked {
		return record, ErrBlocked
	}
	updated, err := e.mutate(ctx, record, func(plan *Plan) error {
		plan.Status = StatusBlocked
		plan.PausedReason = ""
		plan.BlockedReason = reason
		return nil
	})
	if err != nil {
		return updated, err
	}
	return updated, ErrBlocked
}

func (e *Executor) enforceDeadlines(ctx context.Context, record PlanRecord) (PlanRecord, error) {
	now := e.now().UnixMilli()
	if now < record.Plan.StartDeadlineMs {
		return record, nil
	}
	if !record.Plan.HasExternalIntent() {
		return e.fail(ctx, record, "START_DEADLINE_EXPIRED", ErrConflict)
	}
	if now >= record.Plan.RecoveryRetainUntilMs {
		return e.block(ctx, record, "RECOVERY_RETENTION_EXCEEDED")
	}
	// Once a write-ahead intent exists, continue reconciliation and the
	// remaining safety steps even after the ordinary start deadline.
	return record, nil
}

func shouldStartExclusionCleanup(plan Plan) bool {
	return usesVictimExclusion(plan.Submission.Operation) && plan.Phase != PhaseReleasingExclusion &&
		plan.CleanupIntent == nil && plan.ExclusionIntent != nil &&
		plan.ExclusionIntent.Effect != EffectPending &&
		(plan.ScaleIntent == nil || plan.ScaleIntent.Effect == EffectPending)
}

func (e *Executor) reconcileExclusionRelease(ctx context.Context, record PlanRecord, cause error) (PlanRecord, error) {
	if record.Plan.Phase != PhaseReleasingExclusion || record.Plan.CleanupIntent == nil ||
		record.Plan.ExclusionIntent == nil {
		return record, ErrCorruptRecord
	}
	cleanup := record.Plan.CleanupIntent
	if cleanup.ScaleResolution == ScaleResolutionPending {
		state, err := e.workloads.Get(ctx, record.Plan.Submission.Workload)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return record, err
			}
			return e.block(ctx, record, "EXCLUSION_RELEASE_SCALE_UNKNOWN")
		}
		resolution, resolveErr := resolveScaleForCleanup(record.Plan, state)
		if resolveErr != nil {
			return e.block(ctx, record, "EXCLUSION_RELEASE_SCALE_UNKNOWN")
		}
		updated, updateErr := e.mutate(ctx, record, func(plan *Plan) error {
			plan.CleanupIntent.ScaleResolution = resolution
			if resolution == ScaleResolutionCommitted && plan.ScaleIntent != nil &&
				plan.ScaleIntent.Effect == EffectPending {
				plan.ScaleIntent.Effect = EffectAdopted
			}
			return nil
		})
		if updateErr != nil {
			return updated, updateErr
		}
		record = updated
		cleanup = record.Plan.CleanupIntent
	}
	if cleanup.ScaleResolution == ScaleResolutionCommitted ||
		cleanup.ReleaseOutcome.TerminalForScalePlan() {
		return e.failTerminal(ctx, record, cleanup.FailureCode, cause)
	}
	if cleanup.ScaleResolution != ScaleResolutionNotCommitted {
		return e.block(ctx, record, "EXCLUSION_RELEASE_SCALE_UNKNOWN")
	}
	result, releaseErr := e.exclusions.ReleaseForOwner(ctx, record.Plan.Victim, PlacementExclusionRelease{
		OwnerPlanID: record.Plan.PlanID, ExpectedRevision: record.Plan.ExclusionIntent.ObservedRevision,
	})
	if releaseErr != nil && (errors.Is(releaseErr, context.Canceled) || errors.Is(releaseErr, context.DeadlineExceeded)) {
		return record, releaseErr
	}
	outcome := normalizePlacementReleaseOutcome(result, releaseErr)
	if !outcome.TerminalForScalePlan() {
		return e.block(ctx, record, fmt.Sprintf("EXCLUSION_RELEASE_%s", outcome))
	}
	updated, updateErr := e.mutate(ctx, record, func(plan *Plan) error {
		plan.CleanupIntent.ReleaseOutcome = outcome
		return nil
	})
	if updateErr != nil {
		return updated, updateErr
	}
	return e.failTerminal(ctx, updated, updated.Plan.CleanupIntent.FailureCode, cause)
}

func resolveScaleForCleanup(plan Plan, state WorkloadState) (ScaleResolution, error) {
	if err := validateWorkloadState(state, plan.Submission.Workload); err != nil ||
		state.UID != plan.WorkloadIdentity.UID {
		return "", ErrConflict
	}
	if plan.Submission.Operation == OperationDrainHolder && state.Replicas == plan.Submission.ExpectedReplicas {
		return ScaleResolutionNotCommitted, nil
	}
	if state.Replicas == plan.Submission.DesiredReplicas {
		return ScaleResolutionCommitted, nil
	}
	if blockedScaleNotCommittedEvidence(plan, state) {
		return ScaleResolutionNotCommitted, nil
	}
	return "", ErrConflict
}

func normalizePlacementReleaseOutcome(result PlacementExclusionResult, err error) PlacementReleaseOutcome {
	if err != nil {
		return PlacementReleaseUnknown
	}
	switch result.Outcome {
	case PlacementReleaseReleased, PlacementReleaseAlreadyReleased, PlacementReleaseNotFound,
		PlacementReleaseOwnerMismatch, PlacementReleaseRevisionConflict, PlacementReleaseMalformed,
		PlacementReleaseUnknown:
		return result.Outcome
	case PlacementReleaseBootRollover:
		return PlacementReleaseBootRollover
	case PlacementReleaseOutcome("RELEASED_BY_BOOT_ROLLOVER"):
		return PlacementReleaseBootRollover
	default:
		return PlacementReleaseUnknown
	}
}

func (e *Executor) fail(ctx context.Context, record PlanRecord, code string, cause error) (PlanRecord, error) {
	if record.Plan.Status.Terminal() {
		return record, cause
	}
	if shouldStartExclusionCleanup(record.Plan) {
		updated, err := e.mutate(ctx, record, func(plan *Plan) error {
			plan.Phase = PhaseReleasingExclusion
			plan.CleanupIntent = &CleanupIntent{FailureCode: code, ScaleResolution: ScaleResolutionPending}
			return nil
		})
		if err != nil {
			return updated, err
		}
		return e.reconcileExclusionRelease(ctx, updated, cause)
	}
	return e.failTerminal(ctx, record, code, cause)
}

func (e *Executor) failTerminal(ctx context.Context, record PlanRecord, code string, cause error) (PlanRecord, error) {
	if record.Plan.Status.Terminal() {
		return record, cause
	}
	updated, err := e.mutate(ctx, record, func(plan *Plan) error {
		plan.Status = StatusFailed
		plan.PausedReason = ""
		plan.BlockedReason = ""
		plan.ErrorCode = code
		plan.CompletedAtMs = e.auditNow(plan.UpdatedAtMs)
		plan.UpdatedAtMs = plan.CompletedAtMs
		return nil
	})
	if err != nil {
		return updated, err
	}
	return updated, cause
}

func (e *Executor) mutate(ctx context.Context, record PlanRecord, mutation func(*Plan) error) (PlanRecord, error) {
	candidate := record.Plan.Clone()
	if err := mutation(&candidate); err != nil {
		return record, err
	}
	candidate.Revision = record.Plan.Revision + 1
	if candidate.UpdatedAtMs == record.Plan.UpdatedAtMs {
		candidate.UpdatedAtMs = e.auditNow(record.Plan.UpdatedAtMs)
	}
	if candidate.Status.Terminal() && candidate.CompletedAtMs == 0 {
		candidate.CompletedAtMs = candidate.UpdatedAtMs
	}
	if err := ValidatePlanUpdate(record.Plan, candidate); err != nil {
		return record, err
	}
	result, err := e.repository.CompareAndSwap(ctx, record.Plan.Revision, candidate)
	if err != nil {
		return result.Record, err
	}
	switch result.Status {
	case CASCommitted, CASAlreadyCommitted:
		return result.Record, nil
	case CASRevisionConflict:
		return result.Record, ErrRevisionConflict
	case CASCapacityExceeded:
		return result.Record, ErrCapacityExceeded
	default:
		return result.Record, ErrCorruptRecord
	}
}

func (e *Executor) auditNow(previous int64) int64 {
	now := e.now().UnixMilli()
	if now < previous {
		return previous
	}
	return now
}

func validateWorkloadState(state WorkloadState, expected WorkloadRef) error {
	if state.Ref != expected || state.UID == "" || state.ResourceVersion == "" || state.Replicas < 0 ||
		state.ReadyReplicas < 0 || state.ReadyReplicas > state.Replicas || state.ObservedGeneration < 0 {
		return fmt.Errorf("%w: workload state", ErrConflict)
	}
	ordinals, podUIDs := make(map[int32]struct{}), make(map[string]struct{})
	present := int32(0)
	for _, pod := range state.Pods {
		if !pod.Present {
			continue
		}
		present++
		if pod.PodName == "" || pod.PodUID == "" || pod.WorkloadUID != state.UID || pod.Ordinal < 0 || pod.HolderID == "" {
			return fmt.Errorf("%w: pod/workload identity join", ErrConflict)
		}
		if _, duplicate := ordinals[pod.Ordinal]; duplicate {
			return fmt.Errorf("%w: duplicate pod ordinal", ErrConflict)
		}
		if _, duplicate := podUIDs[pod.PodUID]; duplicate {
			return fmt.Errorf("%w: duplicate pod uid", ErrConflict)
		}
		ordinals[pod.Ordinal], podUIDs[pod.PodUID] = struct{}{}, struct{}{}
	}
	if present > state.Replicas {
		return fmt.Errorf("%w: present pods exceed replicas", ErrConflict)
	}
	return nil
}

func eligibleZeroShardTarget(holder HolderState, workloadUID string, ordinal int32, now time.Time) bool {
	return holder.Ref.Exact() && holder.Ref.Ordinal == ordinal && holder.Ref.WorkloadUID == workloadUID && holder.PlacementEligible &&
		holder.State == HolderReadyZeroShard && holder.ProcessReady && !holder.Draining && holder.ServingShardCount == 0 &&
		holder.SnapshotRevision > 0 && holder.AssignmentKnownFresh && holder.LogicEndpoint != "" &&
		holder.HolderControlEndpoint != "" && holder.TLSDNSIdentity != "" && holder.LeaseUntil.After(now)
}

func eligibleRouteTarget(holder HolderState, workloadUID string, now time.Time) bool {
	return holder.Ref.Exact() && holder.Ref.WorkloadUID == workloadUID && holder.PlacementEligible && holder.ProcessReady && !holder.Draining &&
		(holder.State == HolderReadyZeroShard || holder.State == HolderServing) && holder.SnapshotRevision > 0 &&
		holder.AssignmentKnownFresh && holder.LogicEndpoint != "" && holder.HolderControlEndpoint != "" &&
		holder.TLSDNSIdentity != "" && holder.LeaseUntil.After(now)
}

func highestOrdinalHolder(holders []HolderState, workloadUID string, now time.Time) (HolderState, bool) {
	var selected HolderState
	found := false
	for _, holder := range holders {
		if !holder.Ref.Exact() || holder.Ref.WorkloadUID != workloadUID || !holder.PlacementEligible || !holder.ProcessReady ||
			!holder.AssignmentKnownFresh || holder.Draining || !holder.LeaseUntil.After(now) ||
			(holder.State != HolderServing && holder.State != HolderReadyZeroShard) {
			continue
		}
		if !found || holder.Ref.Ordinal > selected.Ref.Ordinal {
			selected, found = holder, true
		}
	}
	return selected, found
}

func manifestExactlyCovers(moves []ShardMoveIntent, placements []ShardPlacement, victim HolderRef) bool {
	active := make([]ShardPlacement, 0, len(placements))
	for _, placement := range placements {
		if placement.Active && placement.Holder == victim {
			active = append(active, placement)
		}
	}
	if len(active) != len(moves) {
		return false
	}
	sort.Slice(active, func(i, j int) bool {
		if active[i].Domain != active[j].Domain {
			return active[i].Domain < active[j].Domain
		}
		return active[i].VirtualShard < active[j].VirtualShard
	})
	for i := range moves {
		if moves[i].Domain != active[i].Domain || moves[i].VirtualShard != active[i].VirtualShard ||
			moves[i].SourceHolderID != victim.HolderID || moves[i].TargetHolderID == victim.HolderID {
			return false
		}
	}
	return true
}

func activePlacementsFor(placements []ShardPlacement, holder HolderRef) int {
	count := 0
	for _, placement := range placements {
		if placement.Active && placement.Holder == holder {
			count++
		}
	}
	return count
}

func podPresent(state WorkloadState, holder HolderRef) bool {
	for _, pod := range state.Pods {
		if pod.Present && pod.Ready && pod.Ordinal == holder.Ordinal && pod.HolderID == holder.HolderID &&
			pod.PodUID == holder.PodUID && pod.WorkloadUID == holder.WorkloadUID {
			return true
		}
	}
	return false
}

func victimStillHighest(state WorkloadState, victim HolderRef) bool {
	highest := int32(-1)
	victimPresent := false
	for _, pod := range state.Pods {
		if !pod.Present {
			continue
		}
		if pod.Ordinal > highest {
			highest = pod.Ordinal
		}
		if pod.Ordinal == victim.Ordinal && pod.HolderID == victim.HolderID && pod.PodUID == victim.PodUID &&
			pod.WorkloadUID == victim.WorkloadUID {
			victimPresent = true
		}
	}
	return victimPresent && highest == victim.Ordinal
}
