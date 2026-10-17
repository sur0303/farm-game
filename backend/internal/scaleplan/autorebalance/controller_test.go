package autorebalance

import (
	"context"
	"errors"
	"testing"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/scaleplan"
)

type fakeWorkloads struct{ state core.WorkloadState }

func (f *fakeWorkloads) Get(context.Context, core.WorkloadRef) (core.WorkloadState, error) {
	return f.state, nil
}
func (*fakeWorkloads) Scale(context.Context, core.WorkloadRef, core.ScalePrecondition, int32) (core.ScaleMutationResult, error) {
	return core.ScaleMutationResult{}, errors.New("unexpected scale call from planner controller")
}

type fakeHolderReader struct{ holders []core.HolderState }

func (f *fakeHolderReader) GetHolder(context.Context, core.HolderRef) (core.HolderState, bool, error) {
	return core.HolderState{}, false, nil
}

type fakeExclusions struct {
	state core.PlacementExclusion
	found bool
}

func (f *fakeExclusions) Get(context.Context, core.HolderRef) (core.PlacementExclusion, bool, error) {
	return f.state, f.found, nil
}
func (*fakeExclusions) Exclude(context.Context, core.HolderRef, core.PlacementExclusionPrecondition) (core.PlacementExclusionResult, error) {
	return core.PlacementExclusionResult{}, errors.New("unexpected exclusion mutation")
}
func (*fakeExclusions) ReleaseForOwner(context.Context, core.HolderRef, core.PlacementExclusionRelease) (core.PlacementExclusionResult, error) {
	return core.PlacementExclusionResult{}, errors.New("unexpected exclusion release")
}
func (f *fakeHolderReader) ListDomainHolders(context.Context, actorrouting.RouteDomain) ([]core.HolderState, error) {
	return append([]core.HolderState(nil), f.holders...), nil
}

type desiredValue struct {
	value int32
	set   bool
}

func (d desiredValue) DesiredReplicas(context.Context, core.WorkloadRef) (int32, bool, error) {
	return d.value, d.set, nil
}

type acknowledgingDesired struct {
	desiredValue
	calls int
}

func (d *acknowledgingDesired) AcknowledgeDesiredReplicas(_ context.Context, _ core.WorkloadRef, state core.WorkloadState, desired int32) error {
	if state.Replicas != desired {
		return errors.New("acknowledged before target was reached")
	}
	d.calls++
	return nil
}

type fakeExecutor struct {
	submission    core.Submission
	submitErr     error
	executed      string
	executeCalls  int
	executeRecord core.PlanRecord
	executeErr    error
}

type fakeRolloutControl struct {
	state        RolloutState
	found        bool
	recovery     RolloutRecoveryRequest
	recoverySet  bool
	ensured      int32
	deleted      bool
	deleteResult bool
}

func (f *fakeRolloutControl) Observe(context.Context, core.WorkloadRef) (RolloutState, bool, error) {
	return f.state, f.found, nil
}
func (f *fakeRolloutControl) ObserveRecovery(context.Context, core.WorkloadRef) (RolloutRecoveryRequest, bool, error) {
	return f.recovery, f.recoverySet, nil
}
func (f *fakeRolloutControl) Begin(_ context.Context, _ core.WorkloadRef, state core.WorkloadState, target string) (RolloutState, error) {
	f.state = validTestRollout(RolloutPhaseExpanding, state.Replicas)
	f.state.WorkloadUID, f.state.TargetRevision = state.UID, target
	f.found = true
	return f.state, nil
}
func (f *fakeRolloutControl) Update(_ context.Context, _ core.WorkloadRef, expected, next RolloutState) (RolloutState, error) {
	if f.state != expected {
		return RolloutState{}, core.ErrRevisionConflict
	}
	if err := ValidateRolloutUpdate(expected, next); err != nil {
		return RolloutState{}, err
	}
	f.state = next
	return next, nil
}
func (f *fakeRolloutControl) Recover(_ context.Context, _ core.WorkloadRef, expected RolloutState,
	recovery RolloutRecoveryRequest, next RolloutState) (RolloutState, error) {
	if f.state != expected || f.recovery != recovery || !f.recoverySet {
		return RolloutState{}, core.ErrRevisionConflict
	}
	if err := ValidateRolloutRecoveryUpdate(expected, next, recovery.Action); err != nil {
		return RolloutState{}, err
	}
	f.state, f.recoverySet = next, false
	return next, nil
}
func (f *fakeRolloutControl) EnsureDesiredReplicas(_ context.Context, _ core.WorkloadRef, _ RolloutState, desired int32) error {
	f.ensured = desired
	return nil
}
func (f *fakeRolloutControl) DeleteVictimPod(context.Context, core.WorkloadRef, RolloutState) (bool, error) {
	f.deleted = true
	return f.deleteResult, nil
}
func (f *fakeRolloutControl) Complete(context.Context, core.WorkloadRef, RolloutState) error {
	f.found = false
	return nil
}

type deterministicExecutor struct {
	submission core.Submission
	record     core.PlanRecord
}

type dependencyRecoveringExecutor struct {
	planID      string
	pauseReason string
	resumed     bool
	resumeCalls int
}

func (f *dependencyRecoveringExecutor) Submit(context.Context, core.Submission) (core.PlanRecord, error) {
	return core.PlanRecord{}, errors.New("unexpected submit")
}

func (f *dependencyRecoveringExecutor) Execute(_ context.Context, planID string) (core.PlanRecord, error) {
	if planID != f.planID {
		return core.PlanRecord{}, core.ErrConflict
	}
	if !f.resumed {
		reason := f.pauseReason
		if reason == "" {
			reason = "DEPENDENCY_UNAVAILABLE"
		}
		return core.PlanRecord{Plan: core.Plan{
			PlanID: planID, Status: core.StatusPaused, PausedReason: reason,
		}}, core.ErrPaused
	}
	return core.PlanRecord{Plan: core.Plan{PlanID: planID, Status: core.StatusSucceeded}}, nil
}

func (f *dependencyRecoveringExecutor) Resume(_ context.Context, planID string) (core.PlanRecord, error) {
	if planID != f.planID {
		return core.PlanRecord{}, core.ErrConflict
	}
	f.resumeCalls++
	f.resumed = true
	return core.PlanRecord{Plan: core.Plan{PlanID: planID, Status: core.StatusRunning}}, nil
}

func (f *deterministicExecutor) Submit(_ context.Context, submission core.Submission) (core.PlanRecord, error) {
	f.submission = submission
	id, err := core.DeterministicPlanID(submission)
	if err != nil {
		return core.PlanRecord{}, err
	}
	f.record = core.PlanRecord{Plan: core.Plan{PlanID: id, Submission: submission, Status: core.StatusRunning}}
	return f.record, nil
}
func (f *deterministicExecutor) Execute(_ context.Context, planID string) (core.PlanRecord, error) {
	if f.record.Plan.PlanID != planID {
		return core.PlanRecord{}, core.ErrConflict
	}
	return f.record, core.ErrPending
}

func (f *fakeExecutor) Submit(_ context.Context, submission core.Submission) (core.PlanRecord, error) {
	f.submission = submission
	return core.PlanRecord{Plan: core.Plan{PlanID: "plan-a", Status: core.StatusRunning}}, f.submitErr
}
func (f *fakeExecutor) Execute(_ context.Context, planID string) (core.PlanRecord, error) {
	f.executed = planID
	f.executeCalls++
	if f.executeRecord.Plan.PlanID != "" {
		return f.executeRecord, f.executeErr
	}
	return core.PlanRecord{Plan: core.Plan{PlanID: planID, Status: core.StatusSucceeded}}, f.executeErr
}

func TestControllerAdoptsExternallyCreatedPod(t *testing.T) {
	topology := testTopology(t, 3, []int{6, 6}, true)
	executor := &fakeExecutor{}
	revalidated := false
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Executor: executor,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(_ context.Context, token core.PlacementCaptureToken) error {
			revalidated = token == topology.Token
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if result.Err != nil || result.Action != ActionAdoptScaleOut || !revalidated || executor.executed != "plan-a" {
		t.Fatalf("result=%+v revalidated=%t executed=%q", result, revalidated, executor.executed)
	}
	if executor.submission.ExpectedReplicas != 2 || executor.submission.DesiredReplicas != 3 {
		t.Fatalf("submission=%+v", executor.submission)
	}
}

func TestControllerAutomaticallyResumesDependencyPausedActivePlan(t *testing.T) {
	executor := &dependencyRecoveringExecutor{planID: "plan-dependency"}
	controller, err := NewController(ControllerOptions{
		Workload: core.WorkloadRef{
			Realm: "classic-farm", Domain: actorrouting.DomainFarm,
			Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr",
		},
		Workloads: &fakeWorkloads{},
		Holders:   &fakeHolderReader{},
		Executor:  executor,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return core.PlacementCaptureToken{}, nil, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.SeedActivePlan(executor.planID); err != nil {
		t.Fatal(err)
	}

	result := controller.Reconcile(context.Background())
	if result.Err != nil || result.Status != core.StatusSucceeded || result.Action != ActionResumePlan {
		t.Fatalf("result=%+v", result)
	}
	if executor.resumeCalls != 1 {
		t.Fatalf("resume calls=%d, want 1", executor.resumeCalls)
	}
}

func TestControllerDoesNotAutomaticallyResumeOperatorPausedActivePlan(t *testing.T) {
	executor := &dependencyRecoveringExecutor{planID: "plan-operator", pauseReason: "OPERATOR_PAUSED"}
	controller, err := NewController(ControllerOptions{
		Workload: core.WorkloadRef{
			Realm: "classic-farm", Domain: actorrouting.DomainFarm,
			Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr",
		},
		Workloads: &fakeWorkloads{},
		Holders:   &fakeHolderReader{},
		Executor:  executor,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return core.PlacementCaptureToken{}, nil, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.SeedActivePlan(executor.planID); err != nil {
		t.Fatal(err)
	}

	result := controller.Reconcile(context.Background())
	if !errors.Is(result.Err, core.ErrPaused) || result.Status != core.StatusPaused {
		t.Fatalf("result=%+v", result)
	}
	if executor.resumeCalls != 0 {
		t.Fatalf("resume calls=%d, want 0", executor.resumeCalls)
	}
}

func TestControllerStartsManagedOnDeleteRolloutOnRevisionMismatch(t *testing.T) {
	topology := testTopology(t, 2, []int{6, 6}, false)
	topology.Workload.CurrentRevision = "revision-1"
	topology.Workload.UpdateRevision = "revision-2"
	topology.Workload.UpdateStrategy = "OnDelete"
	topology.Workload.Pods = rolloutPods(topology, "revision-1", "")
	rollout := &fakeRolloutControl{}
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Executor: &fakeExecutor{}, Rollout: rollout,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if result.Err != nil || result.Action != ActionRolloutStart || !rollout.found ||
		rollout.state.Phase != RolloutPhaseExpanding || rollout.state.OriginalReplicas != 2 {
		t.Fatalf("result=%+v rollout=%+v", result, rollout.state)
	}
}

func TestControllerManagedRolloutCreatesSurgeWithoutOrdinaryScalePlan(t *testing.T) {
	topology := testTopology(t, 2, []int{6, 6}, false)
	topology.Workload.CurrentRevision = "revision-1"
	topology.Workload.UpdateRevision = "revision-2"
	topology.Workload.UpdateStrategy = "OnDelete"
	topology.Workload.Pods = rolloutPods(topology, "revision-1", "")
	rollout := &fakeRolloutControl{found: true, state: validTestRollout(RolloutPhaseExpanding, 2)}
	executor := &fakeExecutor{}
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Executor: executor, Rollout: rollout,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if result.Err != nil || result.Action != ActionRolloutExpand || rollout.ensured != 3 {
		t.Fatalf("result=%+v ensured=%d", result, rollout.ensured)
	}
	if executor.submission.Operation != "" || executor.executed != "" {
		t.Fatalf("managed surge entered ordinary ScalePlan: submission=%+v executed=%q", executor.submission, executor.executed)
	}
}

func TestControllerWaitsWhenOldRevisionPodHasNoVerifiedHolder(t *testing.T) {
	topology := testTopology(t, 3, []int{4, 4, 4}, false)
	topology.Workload.CurrentRevision = "revision-1"
	topology.Workload.UpdateRevision = "revision-2"
	topology.Workload.UpdateStrategy = "OnDelete"
	topology.Workload.Pods = rolloutPods(topology, "revision-1", "revision-2")
	rollout := &fakeRolloutControl{found: true, state: validTestRollout(RolloutPhaseDraining, 2)}
	// Holder discovery for ordinal 1 is intentionally missing even though the
	// old Pod is still Ready. This must wait, not contract the spare.
	holders := []core.HolderState{topology.Holders[0], topology.Holders[2]}
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: holders}, Executor: &fakeExecutor{}, Rollout: rollout,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if result.Err != nil || result.Action != ActionRolloutWait || rollout.state.Phase != RolloutPhaseDraining {
		t.Fatalf("result=%+v rollout=%+v", result, rollout.state)
	}
}

func TestControllerDoesNotFanOutWhenReplacementAlreadyServesShards(t *testing.T) {
	topology := testTopology(t, 3, []int{4, 4, 4}, false)
	topology.Workload.CurrentRevision = "revision-1"
	topology.Workload.UpdateRevision = "revision-2"
	topology.Workload.UpdateStrategy = "OnDelete"
	topology.Workload.Pods = rolloutPods(topology, "revision-1", "revision-2")
	rollout := &fakeRolloutControl{found: true, state: validTestRollout(RolloutPhaseDraining, 2)}
	executor := &deterministicExecutor{}
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Executor: executor, Rollout: rollout,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if result.Err != nil || result.Action != ActionRolloutWait || executor.record.Plan.PlanID != "" ||
		rollout.state.DrainPlanID != "" {
		t.Fatalf("managed rollout fell back to distributed drain: result=%+v plan=%+v checkpoint=%+v",
			result, executor.record.Plan, rollout.state)
	}
}

func TestControllerCreatesReplicaPreservingDrainForHighestOldOrdinal(t *testing.T) {
	topology := testTopology(t, 3, []int{4, 4, 0}, false)
	topology.Holders[2].State = core.HolderReadyZeroShard
	topology.Workload.CurrentRevision = "revision-1"
	topology.Workload.UpdateRevision = "revision-2"
	topology.Workload.UpdateStrategy = "OnDelete"
	topology.Workload.Pods = rolloutPods(topology, "revision-1", "revision-2")
	rollout := &fakeRolloutControl{found: true, state: validTestRollout(RolloutPhaseDraining, 2)}
	executor := &deterministicExecutor{}
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Executor: executor, Rollout: rollout,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if !errors.Is(result.Err, core.ErrPending) || result.Action != ActionRolloutDrain {
		t.Fatalf("result=%+v", result)
	}
	if executor.submission.Operation != core.OperationDrainHolder || executor.submission.ExpectedReplicas != 3 ||
		executor.submission.DesiredReplicas != 3 || len(executor.submission.Moves) != 4 {
		t.Fatalf("submission=%+v", executor.submission)
	}
	for _, move := range executor.submission.Moves {
		if move.SourceHolderID != "farm-svr-1" || move.TargetHolderID != "farm-svr-2" || move.TargetOrdinal != 2 {
			t.Fatalf("rollout did not pin old Pod to the surge replacement: %+v", move)
		}
	}
	if executor.submission.PinnedTarget != topology.Holders[2].Ref {
		t.Fatalf("exact replacement generation was not persisted: %+v", executor.submission.PinnedTarget)
	}
	if rollout.state.VictimOrdinal != 1 || rollout.state.DrainPlanID == "" {
		t.Fatalf("checkpoint=%+v", rollout.state)
	}
}

func TestControllerRejectsRolloutTopologyDriftBeforePersistingVictim(t *testing.T) {
	topology := testTopology(t, 3, []int{4, 4, 0}, false)
	topology.Holders[2].State = core.HolderReadyZeroShard
	topology.Workload.CurrentRevision = "revision-1"
	topology.Workload.UpdateRevision = "revision-2"
	topology.Workload.UpdateStrategy = "OnDelete"
	topology.Workload.Pods = rolloutPods(topology, "revision-1", "revision-2")
	rollout := &fakeRolloutControl{found: true, state: validTestRollout(RolloutPhaseDraining, 2)}
	drift := errors.New("route snapshot changed")
	executor := &deterministicExecutor{}
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Executor: executor, Rollout: rollout,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return drift },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if !errors.Is(result.Err, drift) || rollout.state.DrainPlanID != "" || executor.record.Plan.PlanID != "" {
		t.Fatalf("result=%+v checkpoint=%+v submitted=%+v", result, rollout.state, executor.record.Plan)
	}
}

func TestFinishDrainPlanIsIdempotentAfterCheckpointAdvanced(t *testing.T) {
	topology := testTopology(t, 3, []int{4, 4, 4}, false)
	advanced := validTestRollout(RolloutPhaseDeleting, 2)
	advanced.VictimOrdinal = 1
	advanced.VictimHolderID, advanced.VictimPodUID, advanced.VictimBootID, advanced.DrainPlanID = "farm-svr-1", "pod-old", "boot-old", "plan-a"
	advanced.ExclusionRevision = 2
	rollout := &fakeRolloutControl{found: true, state: advanced}
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Executor: &fakeExecutor{}, Rollout: rollout,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	record := core.PlanRecord{Plan: core.Plan{PlanID: "plan-a", Status: core.StatusSucceeded,
		Submission: core.Submission{Operation: core.OperationDrainHolder}}}
	if err := controller.finishDrainPlan(context.Background(), record, nil); err != nil {
		t.Fatalf("idempotent finish: %v", err)
	}
}

func TestControllerRevalidatesCurrentZeroShardProofBeforeDeletingPod(t *testing.T) {
	topology := testTopology(t, 3, []int{6, 0, 6}, false)
	topology.Workload.CurrentRevision = "revision-1"
	topology.Workload.UpdateRevision = "revision-2"
	topology.Workload.UpdateStrategy = "OnDelete"
	topology.Workload.Pods = rolloutPods(topology, "revision-1", "revision-2")
	victim := topology.Holders[1]
	victim.State, victim.ProcessReady, victim.ServingShardCount, victim.PlacementEligible = core.HolderDrained, false, 0, false
	topology.Holders[1] = victim
	placements := make([]core.ShardPlacement, 0, len(topology.Placements))
	for _, placement := range topology.Placements {
		if placement.Holder.HolderID != victim.Ref.HolderID {
			placements = append(placements, placement)
		}
	}
	topology.Placements = placements
	topology.Token.ActiveCardinality = uint64(len(placements))
	state := validTestRollout(RolloutPhaseDeleting, 2)
	state.VictimHolderID, state.VictimPodUID, state.VictimBootID = victim.Ref.HolderID, victim.Ref.PodUID, victim.Ref.BootID
	state.VictimOrdinal, state.DrainPlanID, state.ExclusionRevision = victim.Ref.Ordinal, "plan-drain", 7
	rollout := &fakeRolloutControl{found: true, state: state, deleteResult: true}
	revalidations := 0
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Exclusions: &fakeExclusions{found: true,
			state: core.PlacementExclusion{Holder: victim.Ref, Excluded: true, Revision: 7, OwnerPlanID: "plan-drain"}},
		Executor: &fakeExecutor{}, Rollout: rollout,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, append([]core.ShardPlacement(nil), topology.Placements...), nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { revalidations++; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if result.Err != nil || result.Action != ActionRolloutDelete || !rollout.deleted ||
		rollout.state.Phase != RolloutPhaseWaitingReplacement || revalidations != 1 {
		t.Fatalf("result=%+v rollout=%+v revalidations=%d", result, rollout.state, revalidations)
	}
}

func TestControllerRefusesDeleteWhenFreshCaptureStillContainsVictimShard(t *testing.T) {
	topology := testTopology(t, 3, []int{6, 0, 6}, false)
	topology.Workload.CurrentRevision = "revision-1"
	topology.Workload.UpdateRevision = "revision-2"
	topology.Workload.UpdateStrategy = "OnDelete"
	topology.Workload.Pods = rolloutPods(topology, "revision-1", "revision-2")
	victim := topology.Holders[1]
	victim.State, victim.ServingShardCount, victim.PlacementEligible = core.HolderReadyZeroShard, 0, false
	topology.Holders[1] = victim
	stalePlacement := topology.Placements[0]
	stalePlacement.Holder = victim.Ref
	stalePlacement.VirtualShard = 999
	topology.Placements = append(topology.Placements, stalePlacement)
	topology.Token.ActiveCardinality = uint64(len(topology.Placements))
	state := validTestRollout(RolloutPhaseDeleting, 2)
	state.VictimHolderID, state.VictimPodUID, state.VictimBootID = victim.Ref.HolderID, victim.Ref.PodUID, victim.Ref.BootID
	state.VictimOrdinal, state.DrainPlanID, state.ExclusionRevision = victim.Ref.Ordinal, "plan-drain", 7
	rollout := &fakeRolloutControl{found: true, state: state, deleteResult: true}
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Exclusions: &fakeExclusions{found: true,
			state: core.PlacementExclusion{Holder: victim.Ref, Excluded: true, Revision: 7, OwnerPlanID: "plan-drain"}},
		Executor: &fakeExecutor{}, Rollout: rollout,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, append([]core.ShardPlacement(nil), topology.Placements...), nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if !errors.Is(result.Err, core.ErrConflict) || rollout.deleted {
		t.Fatalf("delete was not fenced: result=%+v rollout=%+v", result, rollout.state)
	}
}

func TestControllerResumesBlockedRolloutOnlyWithExactRecoveryCommand(t *testing.T) {
	topology := testTopology(t, 3, []int{4, 4, 4}, false)
	topology.Workload.CurrentRevision = "revision-1"
	topology.Workload.UpdateRevision = "revision-2"
	topology.Workload.UpdateStrategy = "OnDelete"
	topology.Workload.Pods = rolloutPods(topology, "revision-1", "revision-2")
	blocked := blockRollout(validTestRollout(RolloutPhaseDraining, 2), "TEMPORARY_DISCOVERY_DRIFT")
	recovery := RolloutRecoveryRequest{CommandID: "resume-1", RolloutID: blocked.RolloutID,
		IntentHash: blocked.IntentHash, Action: RolloutRecoveryResume, DeadlineAtMs: 4_102_444_800_000}
	rollout := &fakeRolloutControl{found: true, state: blocked, recovery: recovery, recoverySet: true}
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Executor: &fakeExecutor{}, Rollout: rollout,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if result.Err != nil || result.Action != ActionRolloutResume || rollout.state.Phase != RolloutPhaseDraining || rollout.recoverySet {
		t.Fatalf("result=%+v state=%+v recovery_set=%t", result, rollout.state, rollout.recoverySet)
	}
}

func TestControllerAbortsBlockedRolloutOnlyBeforeOldPodReplacement(t *testing.T) {
	topology := testTopology(t, 3, []int{4, 4, 4}, false)
	topology.Workload.CurrentRevision = "revision-1"
	topology.Workload.UpdateRevision = "revision-2"
	topology.Workload.UpdateStrategy = "OnDelete"
	topology.Workload.Pods = rolloutPods(topology, "revision-1", "revision-2")
	blocked := blockRollout(validTestRollout(RolloutPhaseExpanding, 2), "EXPANSION_REPLICA_DRIFT")
	recovery := RolloutRecoveryRequest{CommandID: "abort-1", RolloutID: blocked.RolloutID,
		IntentHash: blocked.IntentHash, Action: RolloutRecoveryAbort, DeadlineAtMs: 4_102_444_800_000}
	rollout := &fakeRolloutControl{found: true, state: blocked, recovery: recovery, recoverySet: true}
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Executor: &fakeExecutor{}, Rollout: rollout,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if result.Err != nil || result.Action != ActionRolloutAbort || rollout.state.Phase != RolloutPhaseAborting {
		t.Fatalf("result=%+v state=%+v", result, rollout.state)
	}
	result = controller.Reconcile(context.Background())
	if result.Err != nil || result.Action != ActionRolloutAbort || rollout.ensured != 2 {
		t.Fatalf("cleanup result=%+v ensured=%d", result, rollout.ensured)
	}

	partial := topology
	partial.Workload.Pods = append([]core.WorkloadPodState(nil), topology.Workload.Pods...)
	partial.Workload.Pods[1].ControllerRevision = "revision-2"
	blocked = blockRollout(validTestRollout(RolloutPhaseExpanding, 2), "EXPANSION_REPLICA_DRIFT")
	recovery.RolloutID, recovery.IntentHash = blocked.RolloutID, blocked.IntentHash
	rollout = &fakeRolloutControl{found: true, state: blocked, recovery: recovery, recoverySet: true}
	controller, err = NewController(ControllerOptions{
		Workload: partial.Workload.Ref, Workloads: &fakeWorkloads{state: partial.Workload},
		Holders: &fakeHolderReader{holders: partial.Holders}, Executor: &fakeExecutor{}, Rollout: rollout,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return partial.Token, partial.Placements, nil
		}, Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result = controller.Reconcile(context.Background())
	if !errors.Is(result.Err, core.ErrConflict) || rollout.state.Phase != RolloutPhaseBlocked {
		t.Fatalf("partially replaced rollout aborted: result=%+v state=%+v", result, rollout.state)
	}
}

func rolloutPods(topology Topology, oldRevision, spareRevision string) []core.WorkloadPodState {
	pods := make([]core.WorkloadPodState, 0, len(topology.Holders))
	for _, holder := range topology.Holders {
		revision := oldRevision
		if holder.Ref.Ordinal >= 2 && spareRevision != "" {
			revision = spareRevision
		}
		pods = append(pods, core.WorkloadPodState{PodName: holder.PodName, PodUID: holder.Ref.PodUID,
			WorkloadUID: topology.Workload.UID, ControllerRevision: revision,
			ContainerImage:   "registry.test/farm-svr@" + testRolloutDigest,
			ContainerImageID: "registry.test/farm-svr@" + testRolloutDigest, Ordinal: holder.Ref.Ordinal,
			HolderID: holder.Ref.HolderID, Present: true, Ready: true})
	}
	return pods
}

func TestControllerUsesSafeDesiredReplicaSourceForScaleIn(t *testing.T) {
	topology := testTopology(t, 3, []int{4, 4, 4}, false)
	executor := &fakeExecutor{}
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Executor: executor,
		Desired: desiredValue{value: 2, set: true},
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if result.Err != nil || result.Action != ActionSafeScaleIn {
		t.Fatalf("result=%+v", result)
	}
	if executor.submission.Operation != core.OperationScaleIn || executor.submission.DesiredReplicas != 2 {
		t.Fatalf("submission=%+v", executor.submission)
	}
}

func TestControllerUsesSafeDesiredReplicaSourceForPlannedScaleOut(t *testing.T) {
	topology := testTopology(t, 2, []int{6, 6}, false)
	executor := &fakeExecutor{}
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Executor: executor,
		Desired: desiredValue{value: 3, set: true},
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if result.Err != nil || result.Action != ActionPlannedScaleOut {
		t.Fatalf("result=%+v", result)
	}
	if executor.submission.ExpectedReplicas != 2 || executor.submission.DesiredReplicas != 3 ||
		executor.submission.Moves[0].TargetHolderID != "farm-svr-2" {
		t.Fatalf("submission=%+v", executor.submission)
	}
}

func TestControllerAdoptsExistingZeroShardPodBeforeNextPlannedScaleOut(t *testing.T) {
	topology := testTopology(t, 3, []int{6, 6}, true)
	executor := &fakeExecutor{}
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Executor: executor,
		Desired: desiredValue{value: 4, set: true},
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if result.Err != nil || result.Action != ActionAdoptScaleOut {
		t.Fatalf("result=%+v", result)
	}
	if executor.submission.ExpectedReplicas != 2 || executor.submission.DesiredReplicas != 3 ||
		executor.submission.Moves[0].TargetHolderID != "farm-svr-2" {
		t.Fatalf("submission=%+v", executor.submission)
	}
}

func TestControllerAcknowledgesReachedDesiredAfterTopologySafetyScan(t *testing.T) {
	topology := testTopology(t, 2, []int{6, 6}, false)
	desired := &acknowledgingDesired{desiredValue: desiredValue{value: 2, set: true}}
	captureCalled := false
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Executor: &fakeExecutor{}, Desired: desired,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			captureCalled = true
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if result.Err != nil || result.Action != ActionAcknowledge || desired.calls != 1 || !captureCalled {
		t.Fatalf("result=%+v calls=%d capture=%t", result, desired.calls, captureCalled)
	}
}

func TestControllerDoesNotAcknowledgeUnsafeDirectScaleIn(t *testing.T) {
	topology := testTopology(t, 3, []int{4, 4, 4}, false)
	topology.Workload.Replicas = 2
	desired := &acknowledgingDesired{desiredValue: desiredValue{value: 2, set: true}}
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Executor: &fakeExecutor{}, Desired: desired,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if !errors.Is(result.Err, ErrUnsafeExternalScaleIn) || desired.calls != 0 {
		t.Fatalf("result=%+v acknowledge_calls=%d", result, desired.calls)
	}
}

func TestControllerRejectsTopologyDriftBeforeSubmit(t *testing.T) {
	topology := testTopology(t, 3, []int{6, 6}, true)
	executor := &fakeExecutor{}
	drift := errors.New("route snapshot changed")
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Executor: executor,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return drift },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if !errors.Is(result.Err, drift) || executor.submission.IdempotencyKey != "" {
		t.Fatalf("result=%+v submission=%+v", result, executor.submission)
	}
}

func TestControllerDetectsDirectUnsafeStatefulSetScaleIn(t *testing.T) {
	topology := testTopology(t, 3, []int{4, 4, 4}, false)
	topology.Workload.Replicas = 2 // Kubernetes deleted ordinal 2 before RouteControl drained it.
	executor := &fakeExecutor{}
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Executor: executor,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if !errors.Is(result.Err, ErrUnsafeExternalScaleIn) || executor.submission.IdempotencyKey != "" {
		t.Fatalf("result=%+v submission=%+v", result, executor.submission)
	}
}

func TestControllerResumesSeededDurablePlanBeforeReplanningTopology(t *testing.T) {
	topology := testTopology(t, 3, []int{4, 4, 4}, false)
	executor := &fakeExecutor{
		executeRecord: core.PlanRecord{Plan: core.Plan{PlanID: "persisted-plan", Status: core.StatusRunning}},
		executeErr:    core.ErrPending,
	}
	captureCalled := false
	controller, err := NewController(ControllerOptions{
		Workload: topology.Workload.Ref, Workloads: &fakeWorkloads{state: topology.Workload},
		Holders: &fakeHolderReader{holders: topology.Holders}, Executor: executor,
		Capture: func(context.Context) (core.PlacementCaptureToken, []core.ShardPlacement, error) {
			captureCalled = true
			return topology.Token, topology.Placements, nil
		},
		Revalidate: func(context.Context, core.PlacementCaptureToken) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.SeedActivePlan("persisted-plan"); err != nil {
		t.Fatal(err)
	}
	result := controller.Reconcile(context.Background())
	if result.Action != ActionResumePlan || result.PlanID != "persisted-plan" ||
		!errors.Is(result.Err, core.ErrPending) || executor.executeCalls != 1 || captureCalled {
		t.Fatalf("result=%+v calls=%d capture=%t", result, executor.executeCalls, captureCalled)
	}
}
