package scaleplan_test

import (
	"context"
	"errors"
	"testing"

	core "farmgame/internal/scaleplan"
)

func TestScalePlanVerificationExactChildConflictBlocksAndRetainsOwnership(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for record.Plan.Phase != core.PhaseVerifyingWave {
		var err error
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}
	childID := record.Plan.ResolvedMoves[0].ChildPlanID
	if !f.routePlans.CorruptRequest(childID) {
		t.Fatal("child request missing")
	}
	result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || result.Plan.Status != core.StatusBlocked ||
		result.Plan.BlockedReason != "CHILD_PLAN_EVIDENCE_NON_EXACT" {
		t.Fatalf("result=%s reason=%s err=%v", result.Plan.Status, result.Plan.BlockedReason, err)
	}
	second := f.submission.Clone()
	second.IdempotencyKey = "non-exact-child"
	if _, err := f.executor.Submit(context.Background(), second); !errors.Is(err, core.ErrWorkloadBusy) {
		t.Fatalf("workload lock released: %v", err)
	}
}

func TestScalePlanVerificationTerminalRegressionBlocksDurably(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for record.Plan.Phase != core.PhaseVerifyingWave {
		var err error
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}
	childID := record.Plan.ResolvedMoves[0].ChildPlanID
	f.routePlans.SetView(childID, core.RoutePlanPhaseActive)
	result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || result.Plan.Status != core.StatusBlocked ||
		result.Plan.BlockedReason != "CHILD_PLAN_PHASE_REGRESSION" {
		t.Fatalf("result=%s reason=%s err=%v", result.Plan.Status, result.Plan.BlockedReason, err)
	}
}

func TestScalePlanInvalidRoutePlanPhaseFailsDurably(t *testing.T) {
	f := newScaleOutFixture(t)
	f.routePlans.SetNextPhase("UNKNOWN_PHASE")
	record, _ := f.executor.Submit(context.Background(), f.submission)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrDependency) || result.Plan.Status != core.StatusFailed ||
		result.Plan.ErrorCode != "CHILD_PLAN_PHASE_INVALID" {
		t.Fatalf("result=%s code=%s err=%v", result.Plan.Status, result.Plan.ErrorCode, err)
	}
}

func TestScalePlanChildVerificationFailureBlocksPlan(t *testing.T) {
	f := newScaleOutFixture(t)
	f.routePlans.SetEnsureHook(func(request core.RoutePlanMoveRequest) {
		f.routePlans.SetVerification(request.PlanID, false, "INJECTED_VERIFICATION_FAILURE")
	})
	record, _ := f.executor.Submit(context.Background(), f.submission)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || result.Plan.Status != core.StatusBlocked || result.Plan.BlockedReason != "CHILD_VERIFICATION_NON_EXACT" {
		t.Fatalf("result=%s reason=%s err=%v", result.Plan.Status, result.Plan.BlockedReason, err)
	}
	if f.workloads.Calls() != 1 {
		t.Fatalf("unexpected scale calls=%d", f.workloads.Calls())
	}
}

func TestScalePlanRetryableChildVerificationPausesThenCompletes(t *testing.T) {
	f := newScaleOutFixture(t)
	record, err := f.executor.Submit(context.Background(), f.submission)
	if err != nil {
		t.Fatal(err)
	}
	for record.Plan.Phase != core.PhaseVerifyingWave {
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}
	childID := record.Plan.ResolvedMoves[0].ChildPlanID
	f.routePlans.SetVerification(childID, false, "VERIFICATION_gate_resolution")
	paused, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPaused) || paused.Plan.Status != core.StatusPaused ||
		paused.Plan.PausedReason != "DEPENDENCY_UNAVAILABLE" || paused.Plan.BlockedReason != "" {
		t.Fatalf("paused=%s reason=%s blocked=%s err=%v", paused.Plan.Status, paused.Plan.PausedReason, paused.Plan.BlockedReason, err)
	}
	if f.workloads.Calls() != 1 {
		t.Fatalf("scale-out already committed before verification: calls=%d", f.workloads.Calls())
	}
	f.routePlans.SetVerification(childID, true, "")
	if _, err := f.executor.Resume(context.Background(), record.Plan.PlanID); err != nil {
		t.Fatal(err)
	}
	completed, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || completed.Plan.Status != core.StatusSucceeded {
		t.Fatalf("completed=%s err=%v", completed.Plan.Status, err)
	}
}

func TestScalePlanBlockedVerificationCanResumeAfterReadOnlyReconcile(t *testing.T) {
	f := newScaleOutFixture(t)
	record, err := f.executor.Submit(context.Background(), f.submission)
	if err != nil {
		t.Fatal(err)
	}
	for record.Plan.Phase != core.PhaseVerifyingWave {
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}
	childID := record.Plan.ResolvedMoves[0].ChildPlanID
	f.routePlans.SetVerification(childID, false, "INJECTED_VERIFICATION_FAILURE")
	blocked, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || blocked.Plan.Status != core.StatusBlocked ||
		blocked.Plan.BlockedReason != "CHILD_VERIFICATION_NON_EXACT" || blocked.Plan.Phase != core.PhaseVerifyingWave {
		t.Fatalf("blocked=%s/%s reason=%s err=%v", blocked.Plan.Status, blocked.Plan.Phase, blocked.Plan.BlockedReason, err)
	}
	if _, err := f.executor.ReconcileBlocked(context.Background(), record.Plan.PlanID); !errors.Is(err, core.ErrBlocked) {
		t.Fatalf("reconcile while verification remains inconsistent: %v", err)
	}
	f.routePlans.SetVerification(childID, true, "")
	reconciled, err := f.executor.ReconcileBlocked(context.Background(), record.Plan.PlanID)
	if err != nil || reconciled.Plan.Status != core.StatusRunning || reconciled.Plan.Phase != core.PhaseVerifyingWave ||
		reconciled.Plan.BlockedReason != "" {
		t.Fatalf("reconciled=%s/%s reason=%s err=%v", reconciled.Plan.Status, reconciled.Plan.Phase, reconciled.Plan.BlockedReason, err)
	}
	completed, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || completed.Plan.Status != core.StatusSucceeded {
		t.Fatalf("completed=%s err=%v", completed.Plan.Status, err)
	}
}

func TestScalePlanScaleInRequiresRegistryAndPlacementZeroShard(t *testing.T) {
	f := newScaleInFixture(t)
	// RoutePlan reports success and Holder reports zero, but the placement is
	// deliberately left on the victim. Scale-down must not run.
	f.routePlans.SetEnsureHook(func(request core.RoutePlanMoveRequest) {
		_ = f.holders.Update(request.Source, func(value *core.HolderState) {
			value.ServingShardCount = 0
			value.State = core.HolderReadyZeroShard
		})
	})
	record, _ := f.executor.Submit(context.Background(), f.submission)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) || result.Plan.Phase != core.PhaseWaitingZeroShard || result.Plan.Status != core.StatusRunning {
		t.Fatalf("result=%s/%s err=%v", result.Plan.Status, result.Plan.Phase, err)
	}
	if f.workloads.Calls() != 0 {
		t.Fatalf("unsafe scale-down calls=%d", f.workloads.Calls())
	}
}

func TestScalePlanScaleInRequiresCompleteManifest(t *testing.T) {
	f := newScaleInFixture(t)
	capture, _ := f.placements.CaptureShard(context.Background(), f.submission.Domain, 8)
	extra := capture.Placement
	extra.VirtualShard = 9
	extra.ExpectedSourceFence.ExpectedAssignmentRevision = 2
	f.placements.Set(extra)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || result.Plan.Status != core.StatusBlocked || result.Plan.BlockedReason != "SCALE_IN_MANIFEST_DRIFT" {
		t.Fatalf("result=%s reason=%s err=%v", result.Plan.Status, result.Plan.BlockedReason, err)
	}
	if f.exclusions.Calls() != 1 || f.workloads.Calls() != 0 {
		t.Fatalf("effects exclusion=%d scale=%d", f.exclusions.Calls(), f.workloads.Calls())
	}
}

func TestScalePlanScaleInRejectsZeroShardVictimForNonemptyManifest(t *testing.T) {
	f := newScaleInFixture(t)
	victim, found, _ := f.holders.GetHolder(context.Background(), core.HolderRef{Domain: f.submission.Domain, HolderID: "player-svr-1", Ordinal: 1})
	if !found {
		t.Fatal("victim missing")
	}
	victim.State, victim.ServingShardCount = core.HolderReadyZeroShard, 0
	f.holders.Set(victim)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || result.Plan.BlockedReason != "PLACEMENT_EXCLUSION_NOT_VISIBLE" || len(f.routePlans.Requests()) != 0 {
		t.Fatalf("status=%s reason=%s child=%d err=%v", result.Plan.Status, result.Plan.BlockedReason, len(f.routePlans.Requests()), err)
	}
}

func TestScalePlanExcludesPlacementBeforeCapturingManifest(t *testing.T) {
	f := newScaleInFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for i := 0; i < 2; i++ { // PENDING->SELECTING->EXCLUDING with durable intent
		var err error
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}
	if f.exclusions.Calls() != 0 || len(f.routePlans.Requests()) != 0 || record.Plan.ExclusionIntent == nil {
		t.Fatalf("effect before write-ahead exclusion=%d routes=%d", f.exclusions.Calls(), len(f.routePlans.Requests()))
	}
	record, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if err != nil || record.Plan.Phase != core.PhaseCapturingVictimShards || f.exclusions.Calls() != 1 {
		t.Fatalf("exclude=%s err=%v calls=%d", record.Plan.Phase, err, f.exclusions.Calls())
	}
	victim, found, err := f.holders.GetHolder(context.Background(), record.Plan.Victim)
	if err != nil || !found || victim.PlacementEligible || victim.Draining || victim.State != core.HolderServing || !victim.ProcessReady {
		t.Fatalf("excluded source cannot keep serving: %+v found=%v err=%v", victim, found, err)
	}
	if len(f.routePlans.Requests()) != 0 {
		t.Fatal("child routeplan created before post-exclusion capture")
	}
}

func TestScalePlanNewShardDuringExclusionWindowBlocksBeforeChild(t *testing.T) {
	f := newScaleInFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for i := 0; i < 2; i++ {
		record, _ = f.executor.Step(context.Background(), record.Plan.PlanID)
	}
	capture, _ := f.placements.CaptureShard(context.Background(), f.submission.Domain, 8)
	extra := capture.Placement
	extra.VirtualShard = 9
	extra.ExpectedSourceFence.ExpectedAssignmentRevision++
	f.placements.Set(extra)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || result.Plan.Status != core.StatusBlocked || result.Plan.BlockedReason != "SCALE_IN_MANIFEST_DRIFT" {
		t.Fatalf("result=%s reason=%s err=%v", result.Plan.Status, result.Plan.BlockedReason, err)
	}
	if f.exclusions.Calls() != 1 || len(f.routePlans.Requests()) != 0 || f.workloads.Calls() != 0 {
		t.Fatalf("unsafe effects exclusion=%d routes=%d scale=%d", f.exclusions.Calls(), len(f.routePlans.Requests()), f.workloads.Calls())
	}
}

func TestScalePlanBlockedManifestCanOnlyResumeAfterReadOnlyReconcile(t *testing.T) {
	f := newScaleInFixture(t)
	capture, _ := f.placements.CaptureShard(context.Background(), f.submission.Domain, 8)
	extra := capture.Placement
	extra.VirtualShard = 9
	extra.ExpectedSourceFence.ExpectedAssignmentRevision++
	f.placements.Set(extra)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	blocked, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || blocked.Plan.Status != core.StatusBlocked {
		t.Fatalf("blocked=%s err=%v", blocked.Plan.Status, err)
	}
	if _, err := f.executor.ReconcileBlocked(context.Background(), record.Plan.PlanID); !errors.Is(err, core.ErrBlocked) {
		t.Fatalf("reconcile while drift remains=%v", err)
	}
	target, found, _ := f.holders.GetHolder(context.Background(), core.HolderRef{Domain: f.submission.Domain, HolderID: "player-svr-0", Ordinal: 0})
	if !found || !f.placements.Move(extra.Domain, extra.VirtualShard, target.Ref, target.LogicEndpoint) {
		t.Fatal("failed to remove extra victim placement")
	}
	reconciled, err := f.executor.ReconcileBlocked(context.Background(), record.Plan.PlanID)
	if err != nil || reconciled.Plan.Status != core.StatusRunning || reconciled.Plan.Phase != core.PhaseMovingWave {
		t.Fatalf("reconciled=%s/%s err=%v", reconciled.Plan.Status, reconciled.Plan.Phase, err)
	}
}

func TestScalePlanZeroShardCaptureDriftCanResumeAfterReadOnlyReconcile(t *testing.T) {
	f := newScaleInFixture(t)
	if !f.placements.Unassign(f.submission.Domain, 8) {
		t.Fatal("failed to make scale-in victim zero-shard")
	}
	victim, found, err := f.holders.GetHolder(context.Background(), core.HolderRef{
		Domain: f.submission.Domain, HolderID: "player-svr-1", Ordinal: 1,
	})
	if err != nil || !found {
		t.Fatalf("load victim: found=%t err=%v", found, err)
	}
	if err := f.holders.Update(victim.Ref, func(value *core.HolderState) {
		value.ServingShardCount = 0
		value.State = core.HolderReadyZeroShard
	}); err != nil {
		t.Fatal(err)
	}
	f.submission.Moves = nil
	f.placements.InvalidateNextRevalidate()

	record, err := f.executor.Submit(context.Background(), f.submission)
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || blocked.Plan.Status != core.StatusBlocked ||
		blocked.Plan.Phase != core.PhaseCapturingVictimShards || blocked.Plan.BlockedReason != "PLACEMENT_CAPTURE_CHANGED" {
		t.Fatalf("blocked=%s/%s reason=%s err=%v", blocked.Plan.Status, blocked.Plan.Phase, blocked.Plan.BlockedReason, err)
	}

	reconciled, err := f.executor.ReconcileBlocked(context.Background(), record.Plan.PlanID)
	if err != nil || reconciled.Plan.Status != core.StatusRunning || reconciled.Plan.Phase != core.PhaseWaitingZeroShard {
		t.Fatalf("reconciled=%s/%s err=%v", reconciled.Plan.Status, reconciled.Plan.Phase, err)
	}
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || result.Plan.Status != core.StatusSucceeded {
		t.Fatalf("result=%s/%s err=%v", result.Plan.Status, result.Plan.Phase, err)
	}
}

func TestScalePlanBlockedManifestReconcilePreservesServingLifecycleGate(t *testing.T) {
	f := newScaleInFixture(t)
	capture, _ := f.placements.CaptureShard(context.Background(), f.submission.Domain, 8)
	extra := capture.Placement
	extra.VirtualShard = 9
	extra.ExpectedSourceFence.ExpectedAssignmentRevision++
	f.placements.Set(extra)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	blocked, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || blocked.Plan.BlockedReason != "SCALE_IN_MANIFEST_DRIFT" {
		t.Fatalf("blocked=%s reason=%s err=%v", blocked.Plan.Status, blocked.Plan.BlockedReason, err)
	}
	target, found, _ := f.holders.GetHolder(context.Background(), core.HolderRef{Domain: f.submission.Domain, HolderID: "player-svr-0", Ordinal: 0})
	if !found || !f.placements.Move(extra.Domain, extra.VirtualShard, target.Ref, target.LogicEndpoint) {
		t.Fatal("failed to restore exact manifest")
	}
	if err := f.holders.Update(blocked.Plan.Victim, func(value *core.HolderState) { value.State = core.HolderWarming }); err != nil {
		t.Fatal(err)
	}
	result, err := f.executor.ReconcileBlocked(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || result.Plan.Status != core.StatusBlocked || result.Plan.Phase != core.PhaseCapturingVictimShards {
		t.Fatalf("result=%s/%s err=%v", result.Plan.Status, result.Plan.Phase, err)
	}
}

func TestScalePlanScaleInResourceVersionDriftBlocksBeforeScaleIntent(t *testing.T) {
	f := newScaleInFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for record.Plan.Phase != core.PhaseWaitingZeroShard {
		var err error
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}
	state := f.workloads.State()
	state.ResourceVersion = "unrelated-update-rv"
	f.workloads.SetState(state)
	result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || result.Plan.Status != core.StatusBlocked ||
		result.Plan.BlockedReason != "WORKLOAD_RESOURCE_VERSION_CHANGED" || result.Plan.ScaleIntent != nil {
		t.Fatalf("result=%s reason=%s intent=%+v err=%v", result.Plan.Status, result.Plan.BlockedReason, result.Plan.ScaleIntent, err)
	}
	if f.workloads.Calls() != 0 {
		t.Fatalf("scale-down executed after resource-version drift: %d", f.workloads.Calls())
	}
}

func TestScalePlanPlacementCaptureChangeDoesNotPersistChild(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for i := 0; i < 3; i++ { // reach MOVING_WAVE before child intent
		record, _ = f.executor.Step(context.Background(), record.Plan.PlanID)
	}
	f.placements.InvalidateNextRevalidate()
	result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) || result.Plan.Phase != core.PhaseMovingWave || len(result.Plan.ResolvedMoves) != 0 {
		t.Fatalf("result=%s children=%d err=%v", result.Plan.Phase, len(result.Plan.ResolvedMoves), err)
	}
	if len(f.routePlans.Requests()) != 0 {
		t.Fatal("child created from invalidated placement capture")
	}
}

func TestScaleInPinnedChildDoesNotWaitForUnrelatedGlobalPlacementBracket(t *testing.T) {
	f := newScaleInFixture(t)
	record, err := f.executor.Submit(context.Background(), f.submission)
	if err != nil {
		t.Fatal(err)
	}
	for record.Plan.Phase != core.PhaseMovingWave || len(record.Plan.ResolvedMoves) == 0 {
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}

	// A sibling shard may publish a new global Snapshot immediately after the
	// victim manifest was captured. Scale-in child admission must use the
	// already-pinned per-shard fence; the RoutePlan owns the exact same-shard
	// revalidation at its writer-effect boundary.
	f.placements.InvalidateNextRevalidate()
	result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if err != nil {
		t.Fatalf("step=%s/%s err=%v", result.Plan.Status, result.Plan.Phase, err)
	}
	if calls := f.routePlans.EnsureCalls(record.Plan.ResolvedMoves[0].ChildPlanID); calls != 1 {
		t.Fatalf("child ensure calls=%d, want 1", calls)
	}
}

func TestScaleInPinnedHolderCacheWarmupWaitsButReplacementBlocks(t *testing.T) {
	t.Run("temporarily absent", func(t *testing.T) {
		f := newScaleInFixture(t)
		record, err := f.executor.Submit(context.Background(), f.submission)
		if err != nil {
			t.Fatal(err)
		}
		for record.Plan.Phase != core.PhaseMovingWave || len(record.Plan.ResolvedMoves) == 0 {
			record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
			if err != nil {
				t.Fatal(err)
			}
		}
		f.holders.Delete(record.Plan.Victim)
		result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
		if !errors.Is(err, core.ErrPending) || result.Plan.Status != core.StatusRunning || result.Plan.BlockedReason != "" {
			t.Fatalf("status=%s blocked=%s err=%v", result.Plan.Status, result.Plan.BlockedReason, err)
		}
		if calls := f.routePlans.EnsureCalls(record.Plan.ResolvedMoves[0].ChildPlanID); calls != 0 {
			t.Fatalf("child ensure calls=%d during holder cache warmup", calls)
		}
	})

	t.Run("replacement generation", func(t *testing.T) {
		f := newScaleInFixture(t)
		record, err := f.executor.Submit(context.Background(), f.submission)
		if err != nil {
			t.Fatal(err)
		}
		for record.Plan.Phase != core.PhaseMovingWave || len(record.Plan.ResolvedMoves) == 0 {
			record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
			if err != nil {
				t.Fatal(err)
			}
		}
		replacement := record.Plan.Victim
		replacement.BootID = "replacement-boot"
		replacement.PodUID = "replacement-pod"
		victim, _, _ := f.holders.GetHolder(context.Background(), record.Plan.Victim)
		victim.Ref = replacement
		f.holders.Set(victim)
		result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
		if !errors.Is(err, core.ErrBlocked) || result.Plan.BlockedReason != "SCALE_IN_CHILD_EFFECT_BARRIER_CHANGED" {
			t.Fatalf("status=%s blocked=%s err=%v", result.Plan.Status, result.Plan.BlockedReason, err)
		}
	})
}

func TestScalePlanFinalVerificationUsesOneManifestCaptureAndEndBracket(t *testing.T) {
	f := newScaleOutFixture(t)
	source, found, _ := f.holders.GetHolder(context.Background(), core.HolderRef{Domain: f.submission.Domain, HolderID: "player-svr-0", Ordinal: 0})
	if !found {
		t.Fatal("source missing")
	}
	_ = f.holders.Update(source.Ref, func(value *core.HolderState) { value.ServingShardCount = 2 })
	f.placements.Set(shard(f.now, 6, source.Ref, source.LogicEndpoint))
	f.submission.Moves = append(f.submission.Moves, core.ShardMoveIntent{Domain: f.submission.Domain, VirtualShard: 6,
		SourceHolderID: source.Ref.HolderID, TargetHolderID: "player-svr-1", TargetOrdinal: 1})
	f.routePlans.SetEnsureHook(func(request core.RoutePlanMoveRequest) {
		f.placements.Move(request.Domain, request.VirtualShard, request.Target, request.TargetEndpoint)
		_ = f.holders.Update(source.Ref, func(value *core.HolderState) {
			value.ServingShardCount--
			if value.ServingShardCount == 0 {
				value.State = core.HolderReadyZeroShard
			}
		})
		_ = f.holders.Update(request.Target, func(value *core.HolderState) {
			value.ServingShardCount++
			value.State = core.HolderServing
		})
	})
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for record.Plan.Phase != core.PhaseVerifyingFinal {
		var err error
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}
	f.placements.InvalidateNextRevalidate()
	pending, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) || pending.Plan.Status != core.StatusRunning || f.placements.ShardSetCaptureCalls() != 3 {
		t.Fatalf("pending=%s err=%v captures=%d", pending.Plan.Status, err, f.placements.ShardSetCaptureCalls())
	}
	completed, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if err != nil || completed.Plan.Status != core.StatusSucceeded || f.placements.ShardSetCaptureCalls() != 4 {
		t.Fatalf("completed=%s err=%v captures=%d", completed.Plan.Status, err, f.placements.ShardSetCaptureCalls())
	}
}

func TestScalePlanFinalVerificationFailsOnWorkloadUIDReplacement(t *testing.T) {
	for _, operation := range []core.Operation{core.OperationScaleOut, core.OperationScaleIn} {
		t.Run(string(operation), func(t *testing.T) {
			var f *scaleFixture
			if operation == core.OperationScaleOut {
				f = newScaleOutFixture(t)
			} else {
				f = newScaleInFixture(t)
			}
			record, _ := f.executor.Submit(context.Background(), f.submission)
			for record.Plan.Phase != core.PhaseVerifyingFinal {
				var err error
				record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
				if err != nil {
					t.Fatal(err)
				}
			}
			state := f.workloads.State()
			state.UID = "replacement-workload-uid"
			for i := range state.Pods {
				state.Pods[i].WorkloadUID = state.UID
			}
			f.workloads.SetState(state)
			result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
			if !errors.Is(err, core.ErrConflict) || result.Plan.Status != core.StatusFailed ||
				result.Plan.ErrorCode != "WORKLOAD_IDENTITY_CHANGED" {
				t.Fatalf("result=%s code=%s err=%v", result.Plan.Status, result.Plan.ErrorCode, err)
			}
		})
	}
}

func TestScalePlanWorkloadIdentityJoinFailsClosed(t *testing.T) {
	f := newScaleOutFixture(t)
	f.workloads.SetScaleHook(func(state *core.WorkloadState, desired int32) {
		state.ReadyReplicas = desired
		state.Pods = append(state.Pods, core.WorkloadPodState{PodName: "player-svr-1", PodUID: "wrong-pod",
			WorkloadUID: "wrong-workload", Ordinal: 1, HolderID: "player-svr-1", Present: true, Ready: true})
	})
	record, _ := f.executor.Submit(context.Background(), f.submission)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrConflict) || result.Plan.Status != core.StatusFailed || result.Plan.ErrorCode != "WORKLOAD_IDENTITY_JOIN" {
		t.Fatalf("result=%s code=%s err=%v", result.Plan.Status, result.Plan.ErrorCode, err)
	}
	if len(f.routePlans.Requests()) != 0 {
		t.Fatal("routeplan created after invalid Pod/Workload join")
	}
}

func TestScalePlanExclusionLossAfterChildIntentBlocksActualRoutePlanEffect(t *testing.T) {
	f := newScaleInFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for len(record.Plan.ResolvedMoves) == 0 {
		var err error
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}
	lost := core.PlacementExclusion{Holder: record.Plan.Victim,
		Revision: record.Plan.ExclusionIntent.ObservedRevision + 1, Excluded: false}
	f.exclusions.Set(lost)
	result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || result.Plan.BlockedReason != "SCALE_IN_CHILD_EFFECT_BARRIER_CHANGED" {
		t.Fatalf("result=%s reason=%s err=%v", result.Plan.Status, result.Plan.BlockedReason, err)
	}
	childID := record.Plan.ResolvedMoves[0].ChildPlanID
	if f.routePlans.EnsureCalls(childID) != 0 {
		t.Fatalf("child effect executed after exclusion loss: %d", f.routePlans.EnsureCalls(childID))
	}
}

func TestScalePlanVictimLifecycleDriftAfterChildIntentBlocksActualRoutePlanEffect(t *testing.T) {
	for name, mutate := range map[string]func(*core.HolderState){
		"ready zero shard": func(value *core.HolderState) { value.State, value.ServingShardCount = core.HolderReadyZeroShard, 0 },
		"draining":         func(value *core.HolderState) { value.Draining = true },
	} {
		t.Run(name, func(t *testing.T) {
			f := newScaleInFixture(t)
			record, _ := f.executor.Submit(context.Background(), f.submission)
			for len(record.Plan.ResolvedMoves) == 0 {
				record, _ = f.executor.Step(context.Background(), record.Plan.PlanID)
			}
			_ = f.holders.Update(record.Plan.Victim, mutate)
			result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
			if !errors.Is(err, core.ErrBlocked) || result.Plan.BlockedReason != "SCALE_IN_CHILD_EFFECT_BARRIER_CHANGED" {
				t.Fatalf("status=%s reason=%s err=%v", result.Plan.Status, result.Plan.BlockedReason, err)
			}
			if calls := f.routePlans.EnsureCalls(record.Plan.ResolvedMoves[0].ChildPlanID); calls != 0 {
				t.Fatalf("child calls=%d", calls)
			}
		})
	}
}

func TestScalePlanExclusionLossAfterScaleDownIntentBlocksActualScaleEffect(t *testing.T) {
	f := newScaleInFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for record.Plan.Phase != core.PhaseScalingDown {
		var err error
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}
	lost := core.PlacementExclusion{Holder: record.Plan.Victim,
		Revision: record.Plan.ExclusionIntent.ObservedRevision + 1, Excluded: false}
	f.exclusions.Set(lost)
	result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || result.Plan.BlockedReason != "SCALE_DOWN_EFFECT_BARRIER_CHANGED" {
		t.Fatalf("result=%s reason=%s err=%v", result.Plan.Status, result.Plan.BlockedReason, err)
	}
	if f.workloads.Calls() != 0 {
		t.Fatalf("scale-down executed after exclusion loss: %d", f.workloads.Calls())
	}
}

func TestScalePlanScaleInTargetMustJoinRemainingPod(t *testing.T) {
	f := newScaleInFixture(t)
	target, found, _ := f.holders.GetHolder(context.Background(), core.HolderRef{Domain: f.submission.Domain, HolderID: "player-svr-0", Ordinal: 0})
	if !found {
		t.Fatal("target missing")
	}
	target.Ref.BootID = "replacement-target-boot"
	target.Ref.PodUID = "replacement-target-pod"
	f.holders.Set(target)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || result.Plan.BlockedReason != "SCALE_IN_TARGET_IDENTITY_CHANGED" {
		t.Fatalf("result=%s reason=%s err=%v", result.Plan.Status, result.Plan.BlockedReason, err)
	}
	if len(f.routePlans.Requests()) != 0 {
		t.Fatal("child created for target not joined to current Pod")
	}
}

func TestScalePlanScaleInRejectsTargetOrdinalRemovedByScaleDown(t *testing.T) {
	f := newScaleInFixture(t)
	bad := f.submission.Clone()
	bad.Moves[0].TargetOrdinal = bad.DesiredReplicas
	if _, err := bad.Normalize(); !errors.Is(err, core.ErrInvalidPlan) {
		t.Fatalf("out-of-range target ordinal=%v", err)
	}
}

func TestScalePlanReplacementVictimGenerationAfterCaptureBlocksChild(t *testing.T) {
	f := newScaleInFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for i := 0; i < 4; i++ { // through post-exclusion capture to MOVING_WAVE
		var err error
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}
	replacement := record.Plan.Victim
	replacement.BootID = "boot-replacement"
	replacement.PodUID = "pod-uid-replacement"
	capture, _ := f.placements.CaptureShard(context.Background(), f.submission.Domain, 8)
	replacedPlacement := capture.Placement
	replacedPlacement.Holder = replacement
	replacedPlacement.ExpectedSourceFence.ExpectedHolderBootID = replacement.BootID
	f.placements.Set(replacedPlacement)
	result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || result.Plan.BlockedReason != "VICTIM_SOURCE_GENERATION_CHANGED" || len(result.Plan.ResolvedMoves) != 0 {
		t.Fatalf("result=%s reason=%s children=%d err=%v", result.Plan.Status, result.Plan.BlockedReason, len(result.Plan.ResolvedMoves), err)
	}
	if len(f.routePlans.Requests()) != 0 {
		t.Fatal("child created for replacement source outside exact exclusion")
	}
}

func TestScalePlanReplacementScaleOutTargetGenerationBlocksChild(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for i := 0; i < 3; i++ { // freeze original target and reach MOVING_WAVE
		record, _ = f.executor.Step(context.Background(), record.Plan.PlanID)
	}
	target, found, _ := f.holders.GetHolder(context.Background(), record.Plan.Target)
	if !found {
		t.Fatal("target missing")
	}
	target.Ref.BootID = "boot-target-replacement"
	target.Ref.PodUID = "pod-target-replacement"
	f.holders.Set(target)
	result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || result.Plan.BlockedReason != "SCALE_OUT_TARGET_GENERATION_CHANGED" || len(result.Plan.ResolvedMoves) != 0 {
		t.Fatalf("result=%s reason=%s children=%d err=%v", result.Plan.Status, result.Plan.BlockedReason, len(result.Plan.ResolvedMoves), err)
	}
}

func TestScalePlanExactRoutePlanLookupRejectsDifferentImmutableRequest(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for i := 0; i < 5; i++ { // create/observe child
		var err error
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}
	move := record.Plan.ResolvedMoves[0]
	expected := move.RoutePlanRequest(record.Plan.Submission.RoutePlanTTLms, record.Plan.PlanID, record.Plan.RecoveryRetainUntilMs, record.Plan.Submission.Limits, len(record.Plan.Submission.Moves))
	expected.Target.Ordinal++
	if _, _, err := f.routePlans.GetMove(context.Background(), expected); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("different immutable lookup=%v", err)
	}
}

func TestScalePlanCASRejectsArbitraryChildIdentityAndIntentBinding(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	record, _ = f.executor.Step(context.Background(), record.Plan.PlanID) // valid scale intent
	badIntent := record.Plan.Clone()
	badIntent.ScaleIntent.WorkloadUID = "another-workload"
	badIntent.Revision++
	badIntent.UpdatedAtMs++
	if err := core.ValidatePlanUpdate(record.Plan, badIntent); !errors.Is(err, core.ErrInvalidPlan) {
		t.Fatalf("invalid intent accepted: %v", err)
	}
	for record.Plan.Phase != core.PhaseMovingWave {
		record, _ = f.executor.Step(context.Background(), record.Plan.PlanID)
	}
	record, _ = f.executor.Step(context.Background(), record.Plan.PlanID) // persist child intent
	badChild := record.Plan.Clone()
	badChild.ResolvedMoves[0].ChildPlanID = "sp-rp-v2-arbitrary"
	badChild.Revision++
	badChild.UpdatedAtMs++
	if err := core.ValidatePlanUpdate(record.Plan, badChild); !errors.Is(err, core.ErrInvalidPlan) {
		t.Fatalf("arbitrary child identity accepted: %v", err)
	}
}

func TestScalePlanVictimBootChangeFailsClosed(t *testing.T) {
	f := newScaleInFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	// PENDING -> SELECTING -> EXCLUDING with victim identity frozen.
	for i := 0; i < 2; i++ {
		var err error
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}
	victim, found, _ := f.holders.GetHolder(context.Background(), record.Plan.Victim)
	if !found {
		t.Fatal("victim missing")
	}
	victim.Ref.BootID = "boot-replaced"
	f.holders.Set(victim)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrConflict) || result.Plan.Status != core.StatusFailed || result.Plan.ErrorCode != "VICTIM_BOOT_CHANGED" {
		t.Fatalf("result=%s code=%s err=%v", result.Plan.Status, result.Plan.ErrorCode, err)
	}
	if f.exclusions.Calls() != 0 {
		t.Fatalf("exclusion called after boot change=%d", f.exclusions.Calls())
	}
}
