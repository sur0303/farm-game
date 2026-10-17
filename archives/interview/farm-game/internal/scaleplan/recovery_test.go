package scaleplan_test

import (
	"context"
	"errors"
	"testing"
	"time"

	core "farmgame/internal/scaleplan"
	testkit "farmgame/internal/testkit/scaleplan"
)

func TestScalePlanScaleUnknownAppliedAndNotApplied(t *testing.T) {
	t.Run("applied response lost", func(t *testing.T) {
		f := newScaleOutFixture(t)
		f.workloads.SetUnknown(testkit.ScaleApplyThenUnknown)
		record, _ := f.executor.Submit(context.Background(), f.submission)
		result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
		if err != nil || result.Plan.Status != core.StatusSucceeded || f.workloads.Calls() != 1 {
			t.Fatalf("result=%s/%s err=%v calls=%d", result.Plan.Status, result.Plan.Phase, err, f.workloads.Calls())
		}
		if result.Plan.ScaleIntent.Effect != core.EffectAdopted {
			t.Fatalf("effect=%s", result.Plan.ScaleIntent.Effect)
		}
	})

	t.Run("not applied is safely retried", func(t *testing.T) {
		f := newScaleOutFixture(t)
		f.workloads.SetUnknown(testkit.ScaleNoApplyThenUnknown)
		record, _ := f.executor.Submit(context.Background(), f.submission)
		pending, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
		if !errors.Is(err, core.ErrPending) || pending.Plan.Phase != core.PhaseScalingUp || f.workloads.Calls() != 1 {
			t.Fatalf("pending=%s err=%v calls=%d", pending.Plan.Phase, err, f.workloads.Calls())
		}
		result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
		if err != nil || result.Plan.Status != core.StatusSucceeded || f.workloads.Calls() != 2 {
			t.Fatalf("retry=%s/%s err=%v calls=%d", result.Plan.Status, result.Plan.Phase, err, f.workloads.Calls())
		}
	})
}

func TestScalePlanScaleUnknownThirdValueConflicts(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	record, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if err != nil || record.Plan.Phase != core.PhaseScalingUp {
		t.Fatalf("begin=%s err=%v", record.Plan.Phase, err)
	}
	state := f.workloads.State()
	state.Replicas = 3
	state.ReadyReplicas = 3
	f.workloads.SetState(state)
	result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrConflict) || result.Plan.Status != core.StatusFailed || result.Plan.ErrorCode != "WORKLOAD_SCALE_CONFLICT" {
		t.Fatalf("result=%s code=%s err=%v", result.Plan.Status, result.Plan.ErrorCode, err)
	}
}

func TestScalePlanExclusionUnknownReconcilesExactBoot(t *testing.T) {
	f := newScaleInFixture(t)
	f.exclusions.SetUnknown(testkit.ExclusionApplyThenUnknown)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || result.Plan.Status != core.StatusSucceeded || f.exclusions.Calls() != 1 {
		t.Fatalf("result=%s/%s err=%v exclusion_calls=%d", result.Plan.Status, result.Plan.Phase, err, f.exclusions.Calls())
	}
	if result.Plan.ExclusionIntent.Effect != core.EffectAdopted {
		t.Fatalf("exclusion effect=%s", result.Plan.ExclusionIntent.Effect)
	}
}

func TestScalePlanExclusionUnknownNotAppliedSafelyRetries(t *testing.T) {
	f := newScaleInFixture(t)
	f.exclusions.SetUnknown(testkit.ExclusionNoApplyThenUnknown)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	pending, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) || pending.Plan.Phase != core.PhaseExcludingVictim || f.exclusions.Calls() != 1 {
		t.Fatalf("pending=%s err=%v calls=%d", pending.Plan.Phase, err, f.exclusions.Calls())
	}
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || result.Plan.Status != core.StatusSucceeded || f.exclusions.Calls() != 2 {
		t.Fatalf("result=%s err=%v calls=%d", result.Plan.Status, err, f.exclusions.Calls())
	}
}

func TestScalePlanRoutePlanUnknownUsesSameChildID(t *testing.T) {
	f := newScaleOutFixture(t)
	f.routePlans.SetUnknown(testkit.RoutePlanApplyThenUnknown)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || result.Plan.Status != core.StatusSucceeded {
		t.Fatalf("result=%s/%s err=%v", result.Plan.Status, result.Plan.Phase, err)
	}
	child := result.Plan.ResolvedMoves[0]
	if f.routePlans.EnsureCalls(child.ChildPlanID) != 1 {
		t.Fatalf("ensure calls=%d", f.routePlans.EnsureCalls(child.ChildPlanID))
	}
}

func TestScalePlanRoutePlanUnknownNotAppliedSafelyRetriesSameID(t *testing.T) {
	f := newScaleOutFixture(t)
	f.routePlans.SetUnknown(testkit.RoutePlanNoApplyThenUnknown)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	pending, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) || pending.Plan.Phase != core.PhaseMovingWave || len(pending.Plan.ResolvedMoves) != 1 {
		t.Fatalf("pending=%s children=%d err=%v", pending.Plan.Phase, len(pending.Plan.ResolvedMoves), err)
	}
	childID := pending.Plan.ResolvedMoves[0].ChildPlanID
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || result.Plan.Status != core.StatusSucceeded || result.Plan.ResolvedMoves[0].ChildPlanID != childID {
		t.Fatalf("result=%s err=%v id=%s/%s", result.Plan.Status, err, result.Plan.ResolvedMoves[0].ChildPlanID, childID)
	}
	if f.routePlans.EnsureCalls(childID) != 2 {
		t.Fatalf("ensure calls=%d", f.routePlans.EnsureCalls(childID))
	}
}

func TestScalePlanControllerRestartAtEveryScaleOutCheckpoint(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	seen := map[core.Phase]bool{record.Plan.Phase: true}
	for i := 0; i < 32 && !record.Plan.Status.Terminal(); i++ {
		f.executor = newExecutor(t, f)
		var err error
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil && !errors.Is(err, core.ErrPending) {
			t.Fatalf("step %d: phase=%s err=%v", i, record.Plan.Phase, err)
		}
		seen[record.Plan.Phase] = true
	}
	for _, phase := range []core.Phase{core.PhaseScalingUp, core.PhaseWaitingHolderReady, core.PhaseMovingWave, core.PhaseVerifyingWave, core.PhaseVerifyingFinal} {
		if !seen[phase] {
			t.Fatalf("checkpoint %s not observed: %v", phase, seen)
		}
	}
	if record.Plan.Status != core.StatusSucceeded || f.workloads.Calls() != 1 || f.routePlans.EnsureCalls(record.Plan.ResolvedMoves[0].ChildPlanID) != 1 {
		t.Fatalf("result=%s calls scale=%d route=%d", record.Plan.Status, f.workloads.Calls(), f.routePlans.EnsureCalls(record.Plan.ResolvedMoves[0].ChildPlanID))
	}
}

func TestScalePlanControllerRestartAtEveryScaleInCheckpoint(t *testing.T) {
	f := newScaleInFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	seen := map[core.Phase]bool{record.Plan.Phase: true}
	for i := 0; i < 48 && !record.Plan.Status.Terminal(); i++ {
		f.executor = newExecutor(t, f)
		var err error
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil && !errors.Is(err, core.ErrPending) {
			t.Fatalf("step %d: phase=%s err=%v", i, record.Plan.Phase, err)
		}
		seen[record.Plan.Phase] = true
	}
	for _, phase := range []core.Phase{core.PhaseSelectingVictim, core.PhaseExcludingVictim, core.PhaseCapturingVictimShards,
		core.PhaseMovingWave, core.PhaseVerifyingWave, core.PhaseWaitingZeroShard, core.PhaseScalingDown, core.PhaseVerifyingFinal} {
		if !seen[phase] {
			t.Fatalf("checkpoint %s not observed: %v", phase, seen)
		}
	}
	if record.Plan.Status != core.StatusSucceeded || f.exclusions.Calls() != 1 || f.workloads.Calls() != 1 ||
		f.routePlans.EnsureCalls(record.Plan.ResolvedMoves[0].ChildPlanID) != 1 {
		t.Fatalf("result=%s calls exclusion=%d scale=%d", record.Plan.Status, f.exclusions.Calls(), f.workloads.Calls())
	}
}

func TestScalePlanCrashAfterScaleEffectBeforeParentCASAdoptsFact(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	record, _ = f.executor.Step(context.Background(), record.Plan.PlanID) // persist scale intent
	injected := errors.New("injected parent cas loss")
	f.repository.FailNextCAS(injected)
	if _, err := f.executor.Step(context.Background(), record.Plan.PlanID); !errors.Is(err, injected) {
		t.Fatalf("injected err=%v", err)
	}
	if f.workloads.Calls() != 1 || f.workloads.State().Replicas != 2 {
		t.Fatalf("effect not applied calls=%d replicas=%d", f.workloads.Calls(), f.workloads.State().Replicas)
	}
	f.executor = newExecutor(t, f)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || result.Plan.Status != core.StatusSucceeded || f.workloads.Calls() != 1 {
		t.Fatalf("recovery=%s err=%v calls=%d", result.Plan.Status, err, f.workloads.Calls())
	}
}

func TestScalePlanCrashAfterExclusionEffectBeforeParentCASAdoptsFact(t *testing.T) {
	f := newScaleInFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for i := 0; i < 2; i++ { // select phase, freeze victim and persist exclusion intent
		var err error
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}
	injected := errors.New("injected parent cas loss")
	f.repository.FailNextCAS(injected)
	if _, err := f.executor.Step(context.Background(), record.Plan.PlanID); !errors.Is(err, injected) {
		t.Fatalf("injected err=%v", err)
	}
	if f.exclusions.Calls() != 1 {
		t.Fatalf("exclusion calls=%d", f.exclusions.Calls())
	}
	f.executor = newExecutor(t, f)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || result.Plan.Status != core.StatusSucceeded || f.exclusions.Calls() != 1 {
		t.Fatalf("recovery=%s err=%v exclusion_calls=%d", result.Plan.Status, err, f.exclusions.Calls())
	}
}

func TestScalePlanCrashAfterRoutePlanEffectBeforeParentCASReconcilesID(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for i := 0; i < 4; i++ { // scale, holder-ready, then persist child write-ahead intent
		var err error
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(record.Plan.ResolvedMoves) != 1 || record.Plan.ResolvedMoves[0].EnsureObserved {
		t.Fatalf("child intent not at write-ahead checkpoint: %+v", record.Plan.ResolvedMoves)
	}
	childID := record.Plan.ResolvedMoves[0].ChildPlanID
	injected := errors.New("injected parent cas loss")
	f.repository.FailNextCAS(injected)
	if _, err := f.executor.Step(context.Background(), record.Plan.PlanID); !errors.Is(err, injected) {
		t.Fatalf("injected err=%v", err)
	}
	if f.routePlans.EnsureCalls(childID) != 1 {
		t.Fatalf("ensure calls=%d", f.routePlans.EnsureCalls(childID))
	}
	f.executor = newExecutor(t, f)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || result.Plan.Status != core.StatusSucceeded || f.routePlans.EnsureCalls(childID) != 1 {
		t.Fatalf("recovery=%s err=%v ensure_calls=%d", result.Plan.Status, err, f.routePlans.EnsureCalls(childID))
	}
}

func TestScalePlanDeadlineRecheckedImmediatelyBeforeFirstIntentCAS(t *testing.T) {
	for _, operation := range []core.Operation{core.OperationScaleOut, core.OperationScaleIn} {
		t.Run(string(operation), func(t *testing.T) {
			var f *scaleFixture
			if operation == core.OperationScaleOut {
				f = newScaleOutFixture(t)
			} else {
				f = newScaleInFixture(t)
			}
			f.submission.PlanTTLms = time.Minute.Milliseconds()
			f.workloads.SetGetHook(func() { f.clock.Advance(2 * time.Minute) })
			record, _ := f.executor.Submit(context.Background(), f.submission)
			result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
			if !errors.Is(err, core.ErrConflict) || result.Plan.Status != core.StatusFailed ||
				result.Plan.ErrorCode != "START_DEADLINE_EXPIRED" || result.Plan.HasExternalIntent() {
				t.Fatalf("result=%s code=%s intent=%v err=%v", result.Plan.Status, result.Plan.ErrorCode, result.Plan.HasExternalIntent(), err)
			}
		})
	}
}

func TestScalePlanRecoveryRetentionRecheckedImmediatelyBeforeEffects(t *testing.T) {
	t.Run("scale", func(t *testing.T) {
		f := newScaleOutFixture(t)
		f.submission.PlanTTLms = time.Minute.Milliseconds()
		f.submission.RecoveryRetentionMs = time.Minute.Milliseconds()
		record, _ := f.executor.Submit(context.Background(), f.submission)
		record, _ = f.executor.Step(context.Background(), record.Plan.PlanID)
		f.workloads.SetGetHook(func() { f.clock.Advance(3 * time.Minute) })
		result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
		if !errors.Is(err, core.ErrBlocked) || result.Plan.BlockedReason != "RECOVERY_RETENTION_EXCEEDED" || f.workloads.Calls() != 0 {
			t.Fatalf("result=%s reason=%s calls=%d err=%v", result.Plan.Status, result.Plan.BlockedReason, f.workloads.Calls(), err)
		}
	})

	t.Run("exclusion", func(t *testing.T) {
		f := newScaleInFixture(t)
		f.submission.PlanTTLms = time.Minute.Milliseconds()
		f.submission.RecoveryRetentionMs = time.Minute.Milliseconds()
		record, _ := f.executor.Submit(context.Background(), f.submission)
		for i := 0; i < 2; i++ {
			record, _ = f.executor.Step(context.Background(), record.Plan.PlanID)
		}
		f.exclusions.SetGetHook(func() { f.clock.Advance(3 * time.Minute) })
		result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
		if !errors.Is(err, core.ErrBlocked) || result.Plan.BlockedReason != "RECOVERY_RETENTION_EXCEEDED" || f.exclusions.Calls() != 0 {
			t.Fatalf("result=%s reason=%s calls=%d err=%v", result.Plan.Status, result.Plan.BlockedReason, f.exclusions.Calls(), err)
		}
	})

	t.Run("child", func(t *testing.T) {
		f := newScaleOutFixture(t)
		f.submission.PlanTTLms = time.Minute.Milliseconds()
		f.submission.RecoveryRetentionMs = time.Minute.Milliseconds()
		record, _ := f.executor.Submit(context.Background(), f.submission)
		for len(record.Plan.ResolvedMoves) == 0 {
			record, _ = f.executor.Step(context.Background(), record.Plan.PlanID)
		}
		childID := record.Plan.ResolvedMoves[0].ChildPlanID
		f.routePlans.SetGetHook(func() { f.clock.Advance(3 * time.Minute) })
		result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
		if !errors.Is(err, core.ErrBlocked) || result.Plan.BlockedReason != "RECOVERY_RETENTION_EXCEEDED" ||
			f.routePlans.EnsureCalls(childID) != 0 {
			t.Fatalf("result=%s reason=%s calls=%d err=%v", result.Plan.Status, result.Plan.BlockedReason,
				f.routePlans.EnsureCalls(childID), err)
		}
	})
}

func TestScalePlanPostIntentStartDeadlineStillReconciles(t *testing.T) {
	f := newScaleOutFixture(t)
	f.submission.PlanTTLms = time.Minute.Milliseconds()
	record, _ := f.executor.Submit(context.Background(), f.submission)
	record, err := f.executor.Step(context.Background(), record.Plan.PlanID) // durable ScaleIntent
	if err != nil || record.Plan.ScaleIntent == nil {
		t.Fatalf("intent=%+v err=%v", record.Plan.ScaleIntent, err)
	}
	f.clock.Advance(2 * time.Minute) // beyond start deadline, inside recovery retention and fake leases
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || result.Plan.Status != core.StatusSucceeded || f.workloads.Calls() != 1 {
		t.Fatalf("result=%s/%s err=%v calls=%d", result.Plan.Status, result.Plan.Phase, err, f.workloads.Calls())
	}
}

func TestScalePlanPostIntentDeadlineContinuesExclusionAndChildSafetySteps(t *testing.T) {
	t.Run("exclusion intent", func(t *testing.T) {
		f := newScaleInFixture(t)
		f.submission.PlanTTLms = time.Minute.Milliseconds()
		record, _ := f.executor.Submit(context.Background(), f.submission)
		for i := 0; i < 2; i++ {
			record, _ = f.executor.Step(context.Background(), record.Plan.PlanID)
		}
		if record.Plan.ExclusionIntent == nil {
			t.Fatal("exclusion intent missing")
		}
		f.clock.Advance(2 * time.Minute)
		result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
		if err != nil || result.Plan.Status != core.StatusSucceeded || f.exclusions.Calls() != 1 {
			t.Fatalf("result=%s err=%v exclusions=%d", result.Plan.Status, err, f.exclusions.Calls())
		}
	})

	t.Run("child intent", func(t *testing.T) {
		f := newScaleOutFixture(t)
		f.submission.PlanTTLms = time.Minute.Milliseconds()
		record, _ := f.executor.Submit(context.Background(), f.submission)
		for i := 0; i < 4; i++ {
			record, _ = f.executor.Step(context.Background(), record.Plan.PlanID)
		}
		if len(record.Plan.ResolvedMoves) != 1 || record.Plan.ResolvedMoves[0].EnsureObserved {
			t.Fatal("child write-ahead intent missing")
		}
		f.clock.Advance(2 * time.Minute)
		result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
		if err != nil || result.Plan.Status != core.StatusSucceeded || len(f.routePlans.Requests()) != 1 {
			t.Fatalf("result=%s err=%v children=%d", result.Plan.Status, err, len(f.routePlans.Requests()))
		}
	})
}

func TestScalePlanMovingWaveObservedChildEvidenceLossBlocksAndKeepsWorkloadLock(t *testing.T) {
	for name, corrupt := range map[string]func(*testkit.FakeRoutePlans, string){
		"missing":         func(plans *testkit.FakeRoutePlans, id string) { plans.Delete(id) },
		"non exact":       func(plans *testkit.FakeRoutePlans, id string) { plans.CorruptRequest(id) },
		"malformed phase": func(plans *testkit.FakeRoutePlans, id string) { plans.SetView(id, "UNKNOWN_PHASE") },
	} {
		t.Run(name, func(t *testing.T) {
			f := newScaleOutFixture(t)
			f.routePlans.SetNextPhase(core.RoutePlanPhasePending)
			record, _ := f.executor.Submit(context.Background(), f.submission)
			record, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
			if !errors.Is(err, core.ErrPending) || record.Plan.Phase != core.PhaseMovingWave || !record.Plan.ResolvedMoves[0].EnsureObserved {
				t.Fatalf("phase=%s moves=%+v err=%v", record.Plan.Phase, record.Plan.ResolvedMoves, err)
			}
			childID := record.Plan.ResolvedMoves[0].ChildPlanID
			corrupt(f.routePlans, childID)
			blocked, err := f.executor.Step(context.Background(), record.Plan.PlanID)
			if !errors.Is(err, core.ErrBlocked) || blocked.Plan.Status != core.StatusBlocked {
				t.Fatalf("status=%s reason=%s err=%v", blocked.Plan.Status, blocked.Plan.BlockedReason, err)
			}
			second := f.submission.Clone()
			second.IdempotencyKey = "moving-observed-" + name
			if _, err := f.executor.Submit(context.Background(), second); !errors.Is(err, core.ErrWorkloadBusy) {
				t.Fatalf("workload lock released: %v", err)
			}
		})
	}
}

func TestScalePlanMovingWaveObservedFailedOrRegressedChildBlocksOwnership(t *testing.T) {
	for name, phase := range map[string]string{"failed": core.RoutePlanPhaseFailed, "regressed": core.RoutePlanPhasePending} {
		t.Run(name, func(t *testing.T) {
			f := newScaleOutFixture(t)
			f.routePlans.SetNextPhase(core.RoutePlanPhaseActive)
			record, _ := f.executor.Submit(context.Background(), f.submission)
			record, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
			if !errors.Is(err, core.ErrPending) || record.Plan.Phase != core.PhaseMovingWave ||
				!record.Plan.ResolvedMoves[0].EnsureObserved || record.Plan.ResolvedMoves[0].ChildPhase != core.RoutePlanPhaseActive {
				t.Fatalf("phase=%s moves=%+v err=%v", record.Plan.Phase, record.Plan.ResolvedMoves, err)
			}
			f.routePlans.SetView(record.Plan.ResolvedMoves[0].ChildPlanID, phase)
			blocked, err := f.executor.Step(context.Background(), record.Plan.PlanID)
			if !errors.Is(err, core.ErrBlocked) || blocked.Plan.Status != core.StatusBlocked {
				t.Fatalf("status=%s reason=%s err=%v", blocked.Plan.Status, blocked.Plan.BlockedReason, err)
			}
			second := f.submission.Clone()
			second.IdempotencyKey = "moving-" + name
			if _, err := f.executor.Submit(context.Background(), second); !errors.Is(err, core.ErrWorkloadBusy) {
				t.Fatalf("workload lock released: %v", err)
			}
		})
	}
}

func TestScalePlanMissingExactChildEvidenceBlocksAndKeepsWorkloadLock(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for i := 0; i < 5; i++ {
		var err error
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil && !errors.Is(err, core.ErrPending) {
			t.Fatalf("step=%d phase=%s err=%v", i, record.Plan.Phase, err)
		}
	}
	if len(record.Plan.ResolvedMoves) != 1 || !record.Plan.ResolvedMoves[0].EnsureObserved || record.Plan.Phase != core.PhaseVerifyingWave {
		t.Fatalf("phase=%s moves=%+v", record.Plan.Phase, record.Plan.ResolvedMoves)
	}
	childID := record.Plan.ResolvedMoves[0].ChildPlanID
	f.routePlans.Delete(childID)
	blocked, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || blocked.Plan.Status != core.StatusBlocked || blocked.Plan.BlockedReason != "CHILD_PLAN_EVIDENCE_UNAVAILABLE" {
		t.Fatalf("blocked=%s reason=%s err=%v", blocked.Plan.Status, blocked.Plan.BlockedReason, err)
	}
	second := f.submission.Clone()
	second.IdempotencyKey = "missing-child-evidence"
	if _, err := f.executor.Submit(context.Background(), second); !errors.Is(err, core.ErrWorkloadBusy) {
		t.Fatalf("workload lock released: %v", err)
	}
}

func TestScalePlanRecoveryRetentionBlocksAndKeepsWorkloadLock(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	record, err := f.executor.Step(context.Background(), record.Plan.PlanID) // durable ScaleIntent
	if err != nil || record.Plan.ScaleIntent == nil {
		t.Fatal("scale intent missing")
	}
	f.clock.Advance(26 * time.Hour)
	blocked, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || blocked.Plan.Status != core.StatusBlocked || blocked.Plan.BlockedReason != "RECOVERY_RETENTION_EXCEEDED" {
		t.Fatalf("blocked=%s reason=%s err=%v", blocked.Plan.Status, blocked.Plan.BlockedReason, err)
	}
	second := f.submission.Clone()
	second.IdempotencyKey = "after-retention"
	if _, err := f.executor.Submit(context.Background(), second); !errors.Is(err, core.ErrWorkloadBusy) {
		t.Fatalf("workload lock released: %v", err)
	}
	digest, err := core.BlockedCheckpointDigest(blocked.Plan)
	if err != nil {
		t.Fatal(err)
	}
	request := core.BlockedTerminationRequest{PlanID: blocked.Plan.PlanID, ExpectedRevision: blocked.Plan.Revision,
		ExpectedBlockedReason: blocked.Plan.BlockedReason, ExpectedPayloadDigest: blocked.Plan.PayloadDigest,
		ExpectedCheckpointDigest: digest, TerminationID: "operator-safe-termination-1"}
	if _, err := f.executor.OperatorFailBlocked(context.Background(), request); !errors.Is(err, core.ErrVerification) {
		t.Fatalf("unsafe operator termination=%v", err)
	}
	if _, err := f.executor.OperatorFailBlocked(context.Background(), request); !errors.Is(err, core.ErrVerification) {
		t.Fatalf("expired evidence released workload: %v", err)
	}
	if _, err := f.executor.Submit(context.Background(), second); !errors.Is(err, core.ErrWorkloadBusy) {
		t.Fatalf("expired proof released workload: %v", err)
	}
}

func TestScalePlanProofOwnedBlockedTerminationCommitsReceiptAndReplays(t *testing.T) {
	f := newScaleOutFixture(t)
	f.routePlans.SetNextPhase(core.RoutePlanPhasePaused)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	blocked, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) {
		t.Fatal(err)
	}
	request, err := core.BuildBlockedTerminationRequest(blocked.Plan, "operator-safe-termination-1")
	if err != nil {
		t.Fatal(err)
	}
	terminated, err := f.executor.OperatorFailBlocked(context.Background(), request)
	if err != nil || terminated.Plan.Status != core.StatusFailed || terminated.Plan.ErrorCode != "OPERATOR_SAFE_TERMINATION" ||
		terminated.Plan.BlockedTermination == nil || terminated.Plan.BlockedTermination.TerminationID != request.TerminationID {
		t.Fatalf("termination=%s code=%s receipt=%+v err=%v", terminated.Plan.Status, terminated.Plan.ErrorCode, terminated.Plan.BlockedTermination, err)
	}
	replayed, err := f.executor.OperatorFailBlocked(context.Background(), request)
	if err != nil || replayed.Plan.Revision != terminated.Plan.Revision {
		t.Fatalf("termination replay revision=%d/%d err=%v", replayed.Plan.Revision, terminated.Plan.Revision, err)
	}
	second := f.submission.Clone()
	second.IdempotencyKey = "proof-owned-release"
	if _, err := f.executor.Submit(context.Background(), second); err != nil {
		t.Fatalf("safe terminal did not release workload: %v", err)
	}
}

func TestScalePlanGenericCASCannotForgeBlockedTermination(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	record, _ = f.executor.Step(context.Background(), record.Plan.PlanID)
	f.clock.Advance(26 * time.Hour)
	blocked, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) {
		t.Fatal(err)
	}
	forged := blocked.Plan.Clone()
	forged.Status = core.StatusFailed
	forged.BlockedReason = ""
	forged.ErrorCode = "OPERATOR_SAFE_TERMINATION"
	forged.Revision++
	forged.UpdatedAtMs++
	forged.CompletedAtMs = forged.UpdatedAtMs
	if _, err := f.repository.CompareAndSwap(context.Background(), blocked.Plan.Revision, forged); err == nil {
		t.Fatal("generic CAS forged termination")
	}
	second := f.submission.Clone()
	second.IdempotencyKey = "generic-cas-forgery"
	if _, err := f.executor.Submit(context.Background(), second); !errors.Is(err, core.ErrWorkloadBusy) {
		t.Fatalf("forgery released workload: %v", err)
	}
}

func TestScalePlanExpiredBlockedChildDoesNotPersistReconciledOutcome(t *testing.T) {
	f := newScaleOutFixture(t)
	f.routePlans.SetNextPhase(core.RoutePlanPhasePaused)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	blocked, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || blocked.Plan.Status != core.StatusBlocked {
		t.Fatal("expected blocked child")
	}
	f.clock.Advance(25 * time.Hour)
	result, err := f.executor.ReconcileBlocked(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || result.Plan.Status != core.StatusBlocked ||
		result.Plan.BlockedReason != "RECOVERY_RETENTION_EXCEEDED" || result.Plan.ResolvedMoves[0].Reconciled {
		t.Fatalf("result=%s reason=%s child=%+v err=%v", result.Plan.Status, result.Plan.BlockedReason,
			result.Plan.ResolvedMoves[0], err)
	}
}

func TestScalePlanRecoverPerformsReadOnlyBlockedChildReconciliation(t *testing.T) {
	f := newScaleOutFixture(t)
	f.routePlans.SetNextPhase(core.RoutePlanPhasePaused)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	blocked, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) {
		t.Fatalf("block=%v", err)
	}
	child := blocked.Plan.ResolvedMoves[0]
	f.placements.Move(child.Intent.Domain, child.Intent.VirtualShard, child.Target, child.TargetEndpoint)
	f.executor = newExecutor(t, f)
	if err := f.executor.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	reconciled, _, _ := f.repository.Load(context.Background(), record.Plan.PlanID)
	if reconciled.Plan.Status != core.StatusRunning || !reconciled.Plan.ResolvedMoves[0].Reconciled {
		t.Fatalf("reconciled=%s child=%+v", reconciled.Plan.Status, reconciled.Plan.ResolvedMoves[0])
	}
	if err := f.executor.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	completed, _, _ := f.repository.Load(context.Background(), record.Plan.PlanID)
	if completed.Plan.Status != core.StatusSucceeded || f.routePlans.EnsureCalls(child.ChildPlanID) != 1 {
		t.Fatalf("completed=%s ensure=%d", completed.Plan.Status, f.routePlans.EnsureCalls(child.ChildPlanID))
	}
}

func TestScalePlanRecoverDoesNotRegenerateExternalEffects(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	// Persist the scale intent, then reconstruct before the first external call.
	if _, err := f.executor.Step(context.Background(), record.Plan.PlanID); err != nil {
		t.Fatal(err)
	}
	f.executor = newExecutor(t, f)
	if err := f.executor.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, found, err := f.repository.Load(context.Background(), record.Plan.PlanID)
	if err != nil || !found || result.Plan.Status != core.StatusSucceeded {
		t.Fatalf("recovered=%v status=%s err=%v", found, result.Plan.Status, err)
	}
	if f.workloads.Calls() != 1 || f.routePlans.EnsureCalls(result.Plan.ResolvedMoves[0].ChildPlanID) != 1 {
		t.Fatalf("effects duplicated scale=%d route=%d", f.workloads.Calls(), f.routePlans.EnsureCalls(result.Plan.ResolvedMoves[0].ChildPlanID))
	}
}
