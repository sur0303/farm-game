package autorebalance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	core "farmgame/internal/scaleplan"
)

// DesiredReplicaSource is deliberately separate from StatefulSet
// spec.replicas. A safe scale-in request must be visible before Kubernetes
// deletes the highest ordinal, so production uses a controller-owned
// annotation rather than treating a direct spec.replicas decrement as safe.
type DesiredReplicaSource interface {
	DesiredReplicas(context.Context, core.WorkloadRef) (desired int32, configured bool, err error)
}

// DesiredReplicaAcknowledger turns the safe desired annotation into a
// one-shot control intent. Implementations must remove it only when the exact
// observed Workload identity/resourceVersion still matches, otherwise a
// concurrent operator update could be lost.
type DesiredReplicaAcknowledger interface {
	AcknowledgeDesiredReplicas(context.Context, core.WorkloadRef, core.WorkloadState, int32) error
}

type PlanExecutor interface {
	Submit(context.Context, core.Submission) (core.PlanRecord, error)
	Execute(context.Context, string) (core.PlanRecord, error)
}

type resumablePlanExecutor interface {
	Resume(context.Context, string) (core.PlanRecord, error)
}

type CaptureFunc func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error)
type RevalidateFunc func(context.Context, core.PlacementCaptureToken) error

type ControllerOptions struct {
	Workload    core.WorkloadRef
	Workloads   core.WorkloadScaler
	Holders     core.HolderStateReader
	Exclusions  core.PlacementExclusionPort
	Capture     CaptureFunc
	Revalidate  RevalidateFunc
	Desired     DesiredReplicaSource
	Rollout     RolloutControl
	Executor    PlanExecutor
	Policy      Policy
	Interval    time.Duration
	StepTimeout time.Duration
	Wake        <-chan struct{}
	Ready       func() bool
	OnResult    func(Result)
	// ReconcileMu may be shared by the PLAYER and FARM controllers. The
	// production ScalePlan repository owns one realm-scoped mutation lease, so
	// two route domains must not race to acquire it from the same process.
	ReconcileMu *sync.Mutex
}

type Controller struct {
	workload     core.WorkloadRef
	workloads    core.WorkloadScaler
	holders      core.HolderStateReader
	exclusions   core.PlacementExclusionPort
	capture      CaptureFunc
	revalidate   RevalidateFunc
	desired      DesiredReplicaSource
	rollout      RolloutControl
	executor     PlanExecutor
	policy       Policy
	interval     time.Duration
	stepTimeout  time.Duration
	wake         <-chan struct{}
	ready        func() bool
	onResult     func(Result)
	reconcileMu  *sync.Mutex
	activePlanID string
}

type Action string

const (
	ActionNone            Action = "NONE"
	ActionAdoptScaleOut   Action = "ADOPT_SCALE_OUT"
	ActionPlannedScaleOut Action = "PLANNED_SCALE_OUT"
	ActionSafeScaleIn     Action = "SAFE_SCALE_IN"
	ActionResumePlan      Action = "RESUME_ACTIVE_PLAN"
	ActionAcknowledge     Action = "ACKNOWLEDGE_DESIRED_REPLICAS"
	ActionRolloutStart    Action = "ROLLOUT_START"
	ActionRolloutExpand   Action = "ROLLOUT_EXPAND"
	ActionRolloutDrain    Action = "ROLLOUT_DRAIN_HOLDER"
	ActionRolloutDelete   Action = "ROLLOUT_DELETE_OLD_POD"
	ActionRolloutWait     Action = "ROLLOUT_WAIT_REPLACEMENT"
	ActionRolloutContract Action = "ROLLOUT_CONTRACT_SPARE"
	ActionRolloutComplete Action = "ROLLOUT_COMPLETE"
	ActionRolloutBlocked  Action = "ROLLOUT_BLOCKED"
	ActionRolloutResume   Action = "ROLLOUT_RESUME"
	ActionRolloutAbort    Action = "ROLLOUT_ABORT"
	ActionRolloutAborted  Action = "ROLLOUT_ABORTED"
)

type Result struct {
	Workload             core.WorkloadRef
	Action               Action
	PlanID               string
	Status               core.ExecutionStatus
	Err                  error
	RolloutID            string
	RolloutPhase         RolloutPhase
	RolloutStartedAtMs   int64
	RolloutVictimOrdinal int32
	RolloutBlockedReason string
	OldRevisionPods      int
	TargetReadyPods      int
	PodDeleted           bool
	DrainPlanSubmitted   bool
	RolloutJustBlocked   bool
}

func NewController(options ControllerOptions) (*Controller, error) {
	if err := options.Workload.Validate(); err != nil || options.Workloads == nil || options.Holders == nil ||
		options.Capture == nil || options.Revalidate == nil || options.Executor == nil {
		return nil, fmt.Errorf("%w: controller dependencies", ErrInvalidTopology)
	}
	if options.Policy == (Policy{}) {
		options.Policy = DefaultPolicy()
	}
	if err := options.Policy.validate(); err != nil {
		return nil, err
	}
	if options.Interval <= 0 {
		options.Interval = 5 * time.Second
	}
	if options.StepTimeout <= 0 {
		options.StepTimeout = 30 * time.Second
	}
	if options.Ready == nil {
		options.Ready = func() bool { return true }
	}
	if options.ReconcileMu == nil {
		options.ReconcileMu = new(sync.Mutex)
	}
	return &Controller{
		workload: options.Workload, workloads: options.Workloads, holders: options.Holders, exclusions: options.Exclusions,
		capture: options.Capture, revalidate: options.Revalidate, desired: options.Desired,
		executor: options.Executor, policy: options.Policy, interval: options.Interval, rollout: options.Rollout,
		stepTimeout: options.StepTimeout, wake: options.Wake, ready: options.Ready, onResult: options.OnResult,
		reconcileMu: options.ReconcileMu,
	}, nil
}

// SeedActivePlan reconnects a controller to a durable nonterminal plan found
// during route-controller startup recovery. The Plan remains the source of
// truth; this field is only a local resume cursor.
func (c *Controller) SeedActivePlan(planID string) error {
	if c == nil || planID == "" {
		return ErrInvalidTopology
	}
	c.reconcileMu.Lock()
	defer c.reconcileMu.Unlock()
	if c.activePlanID != "" && c.activePlanID != planID {
		return fmt.Errorf("%w: multiple active plans for workload", ErrInvalidTopology)
	}
	c.activePlanID = planID
	return nil
}

func (c *Controller) Run(ctx context.Context) error {
	if c == nil || ctx == nil {
		return ErrInvalidTopology
	}
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	c.runOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			c.runOnce(ctx)
		case <-c.wake:
			c.runOnce(ctx)
		}
	}
}

func (c *Controller) runOnce(parent context.Context) {
	if !c.ready() {
		return
	}
	ctx, cancel := context.WithTimeout(parent, c.stepTimeout)
	result := c.Reconcile(ctx)
	cancel()
	if c.onResult != nil {
		c.onResult(result)
	}
}

// Reconcile performs one deterministic observation/plan/advance pass. It is
// serialized per workload; production additionally shares the same lock
// between PLAYER and FARM because both use one realm-scoped mutation lease.
func (c *Controller) Reconcile(ctx context.Context) Result {
	if c == nil || ctx == nil {
		return Result{Action: ActionNone, Err: ErrInvalidTopology}
	}
	result := Result{Workload: c.workload, Action: ActionNone}
	c.reconcileMu.Lock()
	defer c.reconcileMu.Unlock()

	if c.activePlanID != "" {
		result.Action = ActionResumePlan
		result.PlanID = c.activePlanID
		record, executeErr := c.executor.Execute(ctx, c.activePlanID)
		if record.Plan.Status == core.StatusPaused && dependencyPause(record.Plan.PausedReason) {
			resumable, ok := c.executor.(resumablePlanExecutor)
			if !ok {
				result.Status = record.Plan.Status
				result.Err = executeErr
				return result
			}
			resumed, resumeErr := resumable.Resume(ctx, c.activePlanID)
			if resumeErr != nil {
				result.Status = resumed.Plan.Status
				result.Err = resumeErr
				return result
			}
			record, executeErr = c.executor.Execute(ctx, c.activePlanID)
		}
		result.Status = record.Plan.Status
		if record.Plan.Status.Terminal() {
			if record.Plan.Submission.Operation == core.OperationDrainHolder && c.rollout != nil {
				executeErr = c.finishDrainPlan(ctx, record, executeErr)
			}
			if executeErr == nil || record.Plan.Status != core.StatusSucceeded {
				c.activePlanID = ""
			}
		}
		if executeErr == nil && record.Plan.Status == core.StatusSucceeded {
			executeErr = c.acknowledgeReachedDesired(ctx)
		}
		result.Err = executeErr
		return result
	}

	state, err := c.workloads.Get(ctx, c.workload)
	if err != nil {
		result.Err = err
		return result
	}
	configuredDesired, configured := int32(0), false
	if c.desired != nil {
		configuredDesired, configured, err = c.desired.DesiredReplicas(ctx, c.workload)
		if err != nil {
			result.Err = err
			return result
		}
	}
	if configured && configuredDesired < 1 {
		result.Err = fmt.Errorf("%w: configured desired replicas must be positive", ErrInvalidTopology)
		return result
	}
	token, placements, err := c.capture(ctx)
	if err != nil {
		result.Err = err
		return result
	}
	holders, err := c.holders.ListDomainHolders(ctx, c.workload.Domain)
	if err != nil {
		result.Err = err
		return result
	}
	topology := Topology{Workload: state, Token: token, Placements: placements, Holders: holders}
	for _, placement := range placements {
		if placement.Holder.WorkloadUID == state.UID && placement.Holder.Ordinal >= state.Replicas {
			result.Err = fmt.Errorf("%w: holder=%s ordinal=%d replicas=%d", ErrUnsafeExternalScaleIn,
				placement.Holder.HolderID, placement.Holder.Ordinal, state.Replicas)
			return result
		}
	}
	if c.rollout != nil {
		rolloutResult, handled := c.reconcileRollout(ctx, topology, configuredDesired, configured)
		if handled {
			return rolloutResult
		}
	}
	// A direct StatefulSet decrement can make spec.replicas equal the safe
	// desired annotation before RouteControl drained the removed ordinal. Never
	// acknowledge and delete that intent until the placement capture proves no
	// active Shard remains beyond the observed replica boundary.
	if configured && configuredDesired == state.Replicas {
		if acknowledger, ok := c.desired.(DesiredReplicaAcknowledger); ok {
			result.Action = ActionAcknowledge
			result.Err = acknowledger.AcknowledgeDesiredReplicas(ctx, c.workload, state, configuredDesired)
		}
		return result
	}

	var submission core.Submission
	switch {
	case configured && configuredDesired < state.Replicas:
		submission, err = ScaleIn(topology, state.Replicas-1, c.policy)
		result.Action = ActionSafeScaleIn
	case configured && configuredDesired > state.Replicas:
		// An operator or HPA may already have created the newest ordinal while a
		// multi-step target still asks for more replicas. Adopt and rebalance that
		// verified zero-Shard Pod first; only then create the next ordinal.
		submission, err = AdoptedScaleOut(topology, c.policy)
		result.Action = ActionAdoptScaleOut
		if errors.Is(err, ErrNoPlan) {
			targetID := fmt.Sprintf("%s-%d", c.workload.Name, state.Replicas)
			submission, err = PlannedScaleOut(topology, targetID, c.policy)
			result.Action = ActionPlannedScaleOut
		}
	default:
		submission, err = AdoptedScaleOut(topology, c.policy)
		result.Action = ActionAdoptScaleOut
	}
	if errors.Is(err, ErrNoPlan) {
		result.Action = ActionNone
		return result
	}
	if err != nil {
		result.Err = err
		return result
	}
	if err := c.revalidate(ctx, token); err != nil {
		result.Err = err
		return result
	}

	record, err := c.executor.Submit(ctx, submission)
	if errors.Is(err, core.ErrWorkloadBusy) && record.Plan.PlanID != "" {
		result.Action = ActionResumePlan
	} else if err != nil {
		result.Err = err
		return result
	}
	result.PlanID = record.Plan.PlanID
	c.activePlanID = record.Plan.PlanID
	record, err = c.executor.Execute(ctx, record.Plan.PlanID)
	result.Status = record.Plan.Status
	if record.Plan.Status.Terminal() {
		c.activePlanID = ""
	}
	if err == nil && record.Plan.Status == core.StatusSucceeded {
		err = c.acknowledgeReachedDesired(ctx)
	}
	if errors.Is(err, core.ErrPending) || errors.Is(err, core.ErrPaused) || errors.Is(err, core.ErrBlocked) {
		// The durable plan owns recovery. A later informer event or periodic pass
		// resumes the exact same PlanID; no second migration is created.
		result.Err = err
		return result
	}
	result.Err = err
	return result
}

func dependencyPause(reason string) bool {
	return reason == "DEPENDENCY_UNAVAILABLE" || reason == "DEPENDENCY_TEMPORARILY_UNAVAILABLE"
}

func (c *Controller) reconcileRollout(ctx context.Context, topology Topology, configuredDesired int32, configured bool) (Result, bool) {
	result := Result{Workload: c.workload, Action: ActionNone}
	state := topology.Workload
	rollout, found, err := c.rollout.Observe(ctx, c.workload)
	if err != nil {
		result.Err = err
		return result, true
	}
	if !found {
		if state.UpdateStrategy != "OnDelete" || state.UpdateRevision == "" ||
			(state.CurrentRevision == state.UpdateRevision && !hasOldRevisionPod(state, state.UpdateRevision, state.Replicas)) {
			return result, false
		}
		if configured {
			return result, false
		}
		begun, beginErr := c.rollout.Begin(ctx, c.workload, state, state.UpdateRevision)
		err = beginErr
		if errors.Is(err, ErrNoPlan) {
			// Revision drift is observable state, not permission to release.
			// A production RolloutControl returns ErrNoPlan until an explicit
			// immutable rollout intent is present.
			return result, false
		}
		result.Action = ActionRolloutStart
		result.Err = err
		if err == nil {
			decorateRolloutResult(&result, begun, state)
		}
		return result, true
	}
	decorateRolloutResult(&result, rollout, state)
	if err := rollout.Validate(); err != nil {
		result.Action, result.Err = ActionRolloutBlocked, err
		return result, true
	}
	if rollout.Phase == RolloutPhaseBlocked {
		return c.reconcileBlockedRollout(ctx, topology, rollout, configuredDesired, configured)
	}
	if state.UpdateStrategy != "OnDelete" {
		return c.blockRollout(ctx, rollout, "UPDATE_STRATEGY_NOT_ONDELETE")
	}
	if state.UpdateRevision == "" || state.UpdateRevision != rollout.TargetRevision {
		return c.blockRollout(ctx, rollout, "TARGET_REVISION_CHANGED")
	}
	if state.UID != rollout.WorkloadUID {
		return c.blockRollout(ctx, rollout, "WORKLOAD_UID_CHANGED")
	}
	if time.Now().UnixMilli() >= rollout.DeadlineAtMs {
		return c.blockRollout(ctx, rollout, "ROLLOUT_DEADLINE_EXCEEDED")
	}
	switch rollout.Phase {
	case RolloutPhaseExpanding:
		desired := rollout.OriginalReplicas + 1
		if state.Replicas > desired {
			return c.blockRollout(ctx, rollout, "EXPANSION_REPLICA_DRIFT")
		}
		if configured {
			// A managed surge is created directly by RolloutControl and must stay
			// outside the ordinary ScalePlan rebalance path. Any safe-desired
			// annotation here belongs to another (or legacy) control intent.
			return c.blockRollout(ctx, rollout, "EXPANSION_DESIRED_CONFLICT")
		}
		if state.Replicas < desired {
			result.Action = ActionRolloutExpand
			result.Err = c.rollout.EnsureDesiredReplicas(ctx, c.workload, rollout, desired)
			return result, true
		}
		if !replacementReady(state, topology.Holders, rollout.OriginalReplicas, "", rollout.TargetRevision, rollout.TargetImageDigest) {
			result.Action = ActionRolloutWait
			return result, true
		}
		next := clearVictim(rollout, RolloutPhaseDraining)
		_, err := c.rollout.Update(ctx, c.workload, rollout, next)
		result.Action, result.Err = ActionRolloutDrain, err
		return result, true

	case RolloutPhaseDraining:
		if rollout.DrainPlanID != "" {
			record, executeErr := c.executor.Execute(ctx, rollout.DrainPlanID)
			result.Action, result.PlanID, result.Status = ActionRolloutDrain, rollout.DrainPlanID, record.Plan.Status
			if record.Plan.Status.Terminal() {
				executeErr = c.finishDrainPlan(ctx, record, executeErr)
			} else {
				c.activePlanID = rollout.DrainPlanID
			}
			result.Err = executeErr
			return result, true
		}
		if !hasOldRevisionPod(state, rollout.TargetRevision, rollout.OriginalReplicas) {
			next := clearVictim(rollout, RolloutPhaseContracting)
			_, err := c.rollout.Update(ctx, c.workload, rollout, next)
			result.Action, result.Err = ActionRolloutContract, err
			return result, true
		}
		victimPod, victimHolder, found := nextOldRevisionVictim(state, topology.Holders, rollout)
		if !found {
			// An old Pod still exists, but Kubernetes readiness and the verified
			// HolderControl identity have not converged yet. Waiting is safer than
			// mistaking temporary discovery lag for rollout completion.
			result.Action = ActionRolloutWait
			return result, true
		}
		_, replacementHolder, found := nextRolloutDrainTarget(state, topology.Holders, rollout, victimHolder.Ref.HolderID)
		if !found {
			// A managed replacement is one-to-one. Do not fall back to ordinary
			// least-loaded placement while the designated zero-Shard target has
			// not converged yet.
			result.Action = ActionRolloutWait
			return result, true
		}
		submission, planErr := DrainHolderTo(topology, victimHolder.Ref.HolderID, replacementHolder.Ref, c.policy)
		if planErr != nil {
			result.Action, result.Err = ActionRolloutDrain, planErr
			return result, true
		}
		planID, planErr := core.DeterministicPlanID(submission)
		if planErr != nil {
			result.Action, result.Err = ActionRolloutDrain, planErr
			return result, true
		}
		// Bind the durable drain to the same complete placement capture used by
		// the planner. The Executor remains fail-closed, but rejecting drift here
		// avoids persisting a rollout checkpoint for an already-stale manifest.
		if err := c.revalidate(ctx, topology.Token); err != nil {
			result.Action, result.Err = ActionRolloutDrain, err
			return result, true
		}
		next := rollout
		next.VictimHolderID, next.VictimPodUID, next.VictimBootID = victimHolder.Ref.HolderID, victimPod.PodUID, victimHolder.Ref.BootID
		next.VictimOrdinal, next.DrainPlanID = victimPod.Ordinal, planID
		if _, err := c.rollout.Update(ctx, c.workload, rollout, next); err != nil {
			result.Action, result.Err = ActionRolloutDrain, err
			return result, true
		}
		record, submitErr := c.executor.Submit(ctx, submission)
		if errors.Is(submitErr, core.ErrWorkloadBusy) && record.Plan.PlanID != planID {
			blocked, _ := c.blockRollout(ctx, next, "WORKLOAD_BUSY_OTHER_PLAN")
			blocked.PlanID, blocked.Status = record.Plan.PlanID, record.Plan.Status
			return blocked, true
		}
		if submitErr != nil && !errors.Is(submitErr, core.ErrWorkloadBusy) {
			result.Action, result.Err = ActionRolloutDrain, submitErr
			return result, true
		}
		if record.Plan.PlanID != planID {
			blocked, _ := c.blockRollout(ctx, next, "DRAIN_PLAN_ID_MISMATCH")
			blocked.PlanID, blocked.Status = record.Plan.PlanID, record.Plan.Status
			return blocked, true
		}
		result.DrainPlanSubmitted = submitErr == nil
		c.activePlanID = planID
		record, executeErr := c.executor.Execute(ctx, planID)
		result.Action, result.PlanID, result.Status = ActionRolloutDrain, planID, record.Plan.Status
		if record.Plan.Status.Terminal() {
			executeErr = c.finishDrainPlan(ctx, record, executeErr)
			if executeErr == nil || record.Plan.Status != core.StatusSucceeded {
				c.activePlanID = ""
			}
		}
		result.Err = executeErr
		return result, true

	case RolloutPhaseDeleting:
		if barrierErr := c.revalidateDeleteBarrier(ctx, rollout); barrierErr != nil {
			result.Action, result.Err = ActionRolloutDelete, barrierErr
			return result, true
		}
		done, deleteErr := c.rollout.DeleteVictimPod(ctx, c.workload, rollout)
		result.Action, result.Err = ActionRolloutDelete, deleteErr
		result.PodDeleted = done && deleteErr == nil
		if deleteErr != nil || !done {
			return result, true
		}
		next := rollout
		next.Phase = RolloutPhaseWaitingReplacement
		_, result.Err = c.rollout.Update(ctx, c.workload, rollout, next)
		return result, true

	case RolloutPhaseWaitingReplacement:
		result.Action = ActionRolloutWait
		if !replacementReady(state, topology.Holders, rollout.VictimOrdinal, rollout.VictimPodUID, rollout.TargetRevision, rollout.TargetImageDigest) {
			return result, true
		}
		next := clearVictim(rollout, RolloutPhaseDraining)
		_, result.Err = c.rollout.Update(ctx, c.workload, rollout, next)
		return result, true

	case RolloutPhaseContracting:
		if state.Replicas < rollout.OriginalReplicas || state.Replicas > rollout.OriginalReplicas+1 {
			return c.blockRollout(ctx, rollout, "CONTRACTION_REPLICA_DRIFT")
		}
		if state.Replicas > rollout.OriginalReplicas {
			if configured && configuredDesired != rollout.OriginalReplicas {
				return c.blockRollout(ctx, rollout, "CONTRACTION_DESIRED_CONFLICT")
			}
			if !configured {
				result.Action = ActionRolloutContract
				result.Err = c.rollout.EnsureDesiredReplicas(ctx, c.workload, rollout, rollout.OriginalReplicas)
				return result, true
			}
			return result, false
		}
		if configured {
			result.Action = ActionRolloutWait
			return result, true
		}
		if hasOldRevisionPod(state, rollout.TargetRevision, rollout.OriginalReplicas) {
			return c.blockRollout(ctx, rollout, "OLD_REVISION_REMAINS")
		}
		result.Action = ActionRolloutComplete
		result.Err = c.rollout.Complete(ctx, c.workload, rollout)
		return result, true

	case RolloutPhaseAborting:
		if !canAbortWithoutReplacingOldPod(state, rollout) {
			return c.blockRollout(ctx, rollout, "ABORT_TOPOLOGY_CHANGED")
		}
		if state.Replicas > rollout.OriginalReplicas {
			if configured && configuredDesired != rollout.OriginalReplicas {
				return c.blockRollout(ctx, rollout, "ABORT_DESIRED_CONFLICT")
			}
			if !configured {
				result.Action = ActionRolloutAbort
				result.Err = c.rollout.EnsureDesiredReplicas(ctx, c.workload, rollout, rollout.OriginalReplicas)
				return result, true
			}
			// Let the ordinary safe ScaleIn path drain the surge Holder and
			// acknowledge the same desired-replicas intent.
			return result, false
		}
		if configured {
			return result, false
		}
		result.Action = ActionRolloutAborted
		result.Err = c.rollout.Complete(ctx, c.workload, rollout)
		return result, true
	}
	return c.blockRollout(ctx, rollout, "UNKNOWN_ROLLOUT_PHASE")
}

func (c *Controller) reconcileBlockedRollout(ctx context.Context, topology Topology, rollout RolloutState,
	configuredDesired int32, configured bool) (Result, bool) {
	result := Result{Workload: c.workload, Action: ActionRolloutBlocked}
	decorateRolloutResult(&result, rollout, topology.Workload)
	recovery, found, err := c.rollout.ObserveRecovery(ctx, c.workload)
	if err != nil {
		result.Err = err
		return result, true
	}
	if !found {
		result.Err = fmt.Errorf("%w: %s", core.ErrBlocked, rollout.BlockedReason)
		return result, true
	}
	if err := recovery.Validate(); err != nil || recovery.RolloutID != rollout.RolloutID ||
		recovery.IntentHash != rollout.IntentHash || recovery.DeadlineAtMs <= time.Now().UnixMilli() {
		result.Err = fmt.Errorf("%w: invalid or expired rollout recovery command", core.ErrConflict)
		return result, true
	}

	switch recovery.Action {
	case RolloutRecoveryResume:
		state := topology.Workload
		if state.UpdateStrategy != "OnDelete" || state.UID != rollout.WorkloadUID ||
			state.UpdateRevision != rollout.TargetRevision || recovery.DeadlineAtMs <= time.Now().UnixMilli() {
			result.Err = fmt.Errorf("%w: rollout resume preconditions are not restored", core.ErrConflict)
			return result, true
		}
		next, nextErr := ResumeBlockedRolloutWithDeadline(rollout, recovery.DeadlineAtMs)
		if nextErr == nil {
			_, nextErr = c.rollout.Recover(ctx, c.workload, rollout, recovery, next)
		}
		result.Action, result.Err = ActionRolloutResume, nextErr
		return result, true

	case RolloutRecoveryAbort:
		if configured && configuredDesired != rollout.OriginalReplicas {
			result.Err = fmt.Errorf("%w: another desired-replicas intent owns the workload", core.ErrConflict)
			return result, true
		}
		if !canAbortWithoutReplacingOldPod(topology.Workload, rollout) {
			result.Err = fmt.Errorf("%w: partially replaced rollout requires a forward release", core.ErrConflict)
			return result, true
		}
		next, nextErr := abortBlockedRollout(rollout)
		if nextErr == nil {
			_, nextErr = c.rollout.Recover(ctx, c.workload, rollout, recovery, next)
		}
		result.Action, result.Err = ActionRolloutAbort, nextErr
		return result, true
	}
	result.Err = fmt.Errorf("%w: unsupported rollout recovery action", core.ErrConflict)
	return result, true
}

func canAbortWithoutReplacingOldPod(state core.WorkloadState, rollout RolloutState) bool {
	if state.UID != rollout.WorkloadUID || state.UpdateStrategy != "OnDelete" ||
		state.Replicas < rollout.OriginalReplicas || state.Replicas > rollout.OriginalReplicas+1 ||
		rollout.DrainPlanID != "" || rollout.VictimHolderID != "" || rollout.VictimPodUID != "" ||
		rollout.VictimBootID != "" || rollout.ExclusionRevision != 0 {
		return false
	}
	seen := make([]bool, rollout.OriginalReplicas)
	for _, pod := range state.Pods {
		if !pod.Present || pod.Ordinal < 0 || pod.Ordinal >= rollout.OriginalReplicas {
			continue
		}
		if pod.ControllerRevision != rollout.SourceRevision || seen[pod.Ordinal] {
			return false
		}
		seen[pod.Ordinal] = true
	}
	for _, present := range seen {
		if !present {
			return false
		}
	}
	return true
}

func (c *Controller) finishDrainPlan(ctx context.Context, record core.PlanRecord, cause error) error {
	rollout, found, err := c.rollout.Observe(ctx, c.workload)
	if err != nil || !found {
		if err == nil {
			err = fmt.Errorf("%w: rollout checkpoint missing", core.ErrConflict)
		}
		return err
	}
	if record.Plan.Status == core.StatusSucceeded && rollout.DrainPlanID == record.Plan.PlanID &&
		(rollout.Phase == RolloutPhaseDeleting || rollout.Phase == RolloutPhaseWaitingReplacement) {
		// A prior reconcile already persisted the orchestration edge. This is a
		// normal restart/race outcome; never strand activePlanID on a successful
		// durable drain merely because its checkpoint is one step ahead.
		return nil
	}
	if rollout.DrainPlanID != record.Plan.PlanID || rollout.Phase != RolloutPhaseDraining {
		return fmt.Errorf("%w: drain plan is not owned by current rollout", core.ErrConflict)
	}
	if record.Plan.Status != core.StatusSucceeded {
		next := blockRollout(rollout, "DRAIN_PLAN_"+string(record.Plan.Status))
		_, updateErr := c.rollout.Update(ctx, c.workload, rollout, next)
		if updateErr != nil {
			return updateErr
		}
		if cause != nil {
			return cause
		}
		return fmt.Errorf("%w: drain plan did not succeed", core.ErrBlocked)
	}
	if record.Plan.ExclusionIntent == nil || record.Plan.ExclusionIntent.OwnerPlanID != record.Plan.PlanID ||
		record.Plan.ExclusionIntent.ObservedRevision == 0 || record.Plan.ExclusionIntent.Victim.HolderID != rollout.VictimHolderID ||
		record.Plan.ExclusionIntent.Victim.BootID != rollout.VictimBootID {
		return fmt.Errorf("%w: completed drain lacks exact exclusion proof", core.ErrConflict)
	}
	next := rollout
	next.Phase = RolloutPhaseDeleting
	next.ExclusionRevision = record.Plan.ExclusionIntent.ObservedRevision
	_, err = c.rollout.Update(ctx, c.workload, rollout, next)
	return err
}

func (c *Controller) blockRollout(ctx context.Context, current RolloutState, reason string) (Result, bool) {
	result := Result{Workload: c.workload, Action: ActionRolloutBlocked, RolloutBlockedReason: reason}
	next := blockRollout(current, reason)
	result.RolloutID, result.RolloutPhase = next.RolloutID, RolloutPhaseBlocked
	result.RolloutStartedAtMs, result.RolloutVictimOrdinal = next.StartedAtMs, next.VictimOrdinal
	_, result.Err = c.rollout.Update(ctx, c.workload, current, next)
	if result.Err == nil {
		result.RolloutJustBlocked = true
		result.Err = fmt.Errorf("%w: %s", core.ErrBlocked, reason)
	}
	return result, true
}

func decorateRolloutResult(result *Result, rollout RolloutState, state core.WorkloadState) {
	if result == nil {
		return
	}
	result.RolloutID, result.RolloutPhase = rollout.RolloutID, rollout.Phase
	result.RolloutStartedAtMs, result.RolloutVictimOrdinal = rollout.StartedAtMs, rollout.VictimOrdinal
	result.RolloutBlockedReason = rollout.BlockedReason
	for _, pod := range state.Pods {
		if !pod.Present || pod.Ordinal < 0 || pod.Ordinal >= rollout.OriginalReplicas {
			continue
		}
		if pod.ControllerRevision != rollout.TargetRevision {
			result.OldRevisionPods++
		} else if pod.Ready {
			result.TargetReadyPods++
		}
	}
}

func hasOldRevisionPod(state core.WorkloadState, target string, below int32) bool {
	for _, pod := range state.Pods {
		if pod.Present && pod.Ordinal < below && pod.ControllerRevision != target {
			return true
		}
	}
	return false
}

func nextOldRevisionVictim(state core.WorkloadState, holders []core.HolderState, rollout RolloutState) (core.WorkloadPodState, core.HolderState, bool) {
	var pod core.WorkloadPodState
	found := false
	for _, candidate := range state.Pods {
		if !candidate.Present || !candidate.Ready || candidate.Ordinal >= rollout.OriginalReplicas ||
			candidate.ControllerRevision == rollout.TargetRevision {
			continue
		}
		if !found || candidate.Ordinal > pod.Ordinal {
			pod, found = candidate, true
		}
	}
	if !found {
		return core.WorkloadPodState{}, core.HolderState{}, false
	}
	for _, holder := range holders {
		if holder.Ref.HolderID == pod.HolderID && holder.Ref.PodUID == pod.PodUID && holder.Ref.WorkloadUID == state.UID &&
			holder.Ref.Ordinal == pod.Ordinal && holder.ProcessReady && !holder.Draining && holder.PlacementEligible &&
			holder.AssignmentKnownFresh && (holder.State == core.HolderServing || holder.State == core.HolderReadyZeroShard) {
			return pod, holder, true
		}
	}
	return core.WorkloadPodState{}, core.HolderState{}, false
}

func replacementReady(state core.WorkloadState, holders []core.HolderState, ordinal int32, previousPodUID, targetRevision, targetImageDigest string) bool {
	_, _, found := rolloutReplacementAtOrdinal(state, holders, ordinal, previousPodUID, targetRevision, targetImageDigest)
	return found
}

func rolloutReplacementAtOrdinal(state core.WorkloadState, holders []core.HolderState, ordinal int32, previousPodUID, targetRevision, targetImageDigest string) (core.WorkloadPodState, core.HolderState, bool) {
	for _, pod := range state.Pods {
		if !pod.Present || !pod.Ready || pod.Ordinal != ordinal || pod.PodUID == previousPodUID ||
			pod.ControllerRevision != targetRevision || !podRunsDigest(pod, targetImageDigest) {
			continue
		}
		for _, holder := range holders {
			if holder.Ref.HolderID == pod.HolderID && holder.Ref.PodUID == pod.PodUID && holder.Ref.WorkloadUID == state.UID &&
				holder.Ref.Ordinal == ordinal && holder.ProcessReady && !holder.Draining && holder.PlacementEligible &&
				holder.AssignmentKnownFresh && holder.State == core.HolderReadyZeroShard && holder.ServingShardCount == 0 {
				return pod, holder, true
			}
		}
	}
	return core.WorkloadPodState{}, core.HolderState{}, false
}

// nextRolloutDrainTarget chooses only a verified target-revision Pod that is
// still READY_ZERO_SHARD. Stable ordinal ordering is used solely to pick which
// empty replacement participates in this rollout step; DrainHolderTo then
// freezes its complete PodUID/BootID identity in the durable plan.
func nextRolloutDrainTarget(state core.WorkloadState, holders []core.HolderState, rollout RolloutState, victimHolderID string) (core.WorkloadPodState, core.HolderState, bool) {
	var selectedPod core.WorkloadPodState
	var selectedHolder core.HolderState
	found := false
	for _, pod := range state.Pods {
		if !pod.Present || !pod.Ready || pod.HolderID == victimHolderID || pod.ControllerRevision != rollout.TargetRevision ||
			!podRunsDigest(pod, rollout.TargetImageDigest) || pod.Ordinal < 0 || pod.Ordinal > rollout.OriginalReplicas {
			continue
		}
		for _, holder := range holders {
			if holder.Ref.HolderID != pod.HolderID || holder.Ref.PodUID != pod.PodUID || holder.Ref.WorkloadUID != state.UID ||
				holder.Ref.Ordinal != pod.Ordinal || !holder.ProcessReady || holder.Draining || !holder.PlacementEligible ||
				!holder.AssignmentKnownFresh || holder.State != core.HolderReadyZeroShard || holder.ServingShardCount != 0 {
				continue
			}
			if !found || pod.Ordinal < selectedPod.Ordinal || (pod.Ordinal == selectedPod.Ordinal && pod.PodUID < selectedPod.PodUID) {
				selectedPod, selectedHolder, found = pod, holder, true
			}
		}
	}
	return selectedPod, selectedHolder, found
}

func podRunsDigest(pod core.WorkloadPodState, digest string) bool {
	if digest == "" {
		return false
	}
	return strings.HasSuffix(pod.ContainerImage, "@"+digest) &&
		(strings.HasSuffix(pod.ContainerImageID, "@"+digest) || strings.HasSuffix(pod.ContainerImageID, digest))
}

// revalidateDeleteBarrier deliberately re-reads every proof used to authorize
// a destructive Pod delete. A completed DRAIN_HOLDER plan is historical
// evidence; deletion requires a fresh placement capture, exact exclusion
// ownership and exact workload/Pod/Boot identity in the same reconcile.
func (c *Controller) revalidateDeleteBarrier(ctx context.Context, rollout RolloutState) error {
	if c.exclusions == nil {
		return fmt.Errorf("%w: rollout deletion requires placement exclusions", ErrInvalidTopology)
	}
	state, err := c.workloads.Get(ctx, c.workload)
	if err != nil {
		return err
	}
	if state.UID != rollout.WorkloadUID || state.Replicas != rollout.OriginalReplicas+1 ||
		state.UpdateStrategy != "OnDelete" || state.UpdateRevision != rollout.TargetRevision {
		return fmt.Errorf("%w: delete workload drift", core.ErrConflict)
	}
	var pod *core.WorkloadPodState
	for i := range state.Pods {
		candidate := &state.Pods[i]
		if candidate.Ordinal == rollout.VictimOrdinal && candidate.PodUID == rollout.VictimPodUID &&
			candidate.WorkloadUID == rollout.WorkloadUID && candidate.HolderID == rollout.VictimHolderID {
			if pod != nil {
				return fmt.Errorf("%w: duplicate victim pod", core.ErrConflict)
			}
			pod = candidate
		}
	}
	if pod == nil || !pod.Present {
		// Kubernetes deletion may already have committed before the caller saw
		// its result. The UID-preconditioned adapter resolves that idempotently.
		return nil
	}
	token, placements, err := c.capture(ctx)
	if err != nil {
		return err
	}
	for _, placement := range placements {
		if placement.Holder.HolderID == rollout.VictimHolderID && placement.Holder.BootID == rollout.VictimBootID {
			return fmt.Errorf("%w: victim still owns shard %d", core.ErrConflict, placement.VirtualShard)
		}
	}
	holders, err := c.holders.ListDomainHolders(ctx, c.workload.Domain)
	if err != nil {
		return err
	}
	var victim *core.HolderState
	for i := range holders {
		holder := &holders[i]
		if holder.Ref.HolderID == rollout.VictimHolderID && holder.Ref.BootID == rollout.VictimBootID &&
			holder.Ref.PodUID == rollout.VictimPodUID && holder.Ref.WorkloadUID == rollout.WorkloadUID &&
			holder.Ref.Ordinal == rollout.VictimOrdinal {
			if victim != nil {
				return fmt.Errorf("%w: duplicate victim holder", core.ErrConflict)
			}
			victim = holder
		}
	}
	if victim == nil || victim.ServingShardCount != 0 || victim.PlacementEligible ||
		!victim.AssignmentKnownFresh || (victim.State != core.HolderReadyZeroShard && victim.State != core.HolderDrained) ||
		(victim.State == core.HolderReadyZeroShard && !victim.ProcessReady) {
		return fmt.Errorf("%w: victim is not exact excluded zero-shard holder", core.ErrConflict)
	}
	exclusion, found, err := c.exclusions.Get(ctx, victim.Ref)
	if err != nil {
		return err
	}
	if !found || !exclusion.Excluded || exclusion.OwnerPlanID != rollout.DrainPlanID ||
		exclusion.Revision != rollout.ExclusionRevision || exclusion.Holder != victim.Ref {
		return fmt.Errorf("%w: victim exclusion proof changed", core.ErrConflict)
	}
	return c.revalidate(ctx, token)
}

func (c *Controller) acknowledgeReachedDesired(ctx context.Context) error {
	acknowledger, ok := c.desired.(DesiredReplicaAcknowledger)
	if !ok {
		return nil
	}
	desired, configured, err := c.desired.DesiredReplicas(ctx, c.workload)
	if err != nil || !configured {
		return err
	}
	state, err := c.workloads.Get(ctx, c.workload)
	if err != nil {
		return err
	}
	if state.Replicas != desired {
		return nil
	}
	return acknowledger.AcknowledgeDesiredReplicas(ctx, c.workload, state, desired)
}
