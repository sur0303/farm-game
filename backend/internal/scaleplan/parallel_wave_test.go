package scaleplan_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	core "farmgame/internal/scaleplan"
)

func newParallelScaleOutFixture(t *testing.T, moveCount int) *scaleFixture {
	t.Helper()
	if moveCount < 1 {
		t.Fatal("parallel fixture requires moves")
	}
	f := newScaleOutFixture(t)
	source, found, err := f.holders.GetHolder(context.Background(), core.HolderRef{
		Domain: f.submission.Domain, HolderID: "player-svr-0", Ordinal: 0,
	})
	if err != nil || !found {
		t.Fatalf("source holder: found=%t err=%v", found, err)
	}
	if err := f.holders.Update(source.Ref, func(value *core.HolderState) {
		value.ServingShardCount = uint32(moveCount)
	}); err != nil {
		t.Fatal(err)
	}
	f.submission.Moves = nil
	for shardID := 0; shardID < moveCount; shardID++ {
		value := shard(f.now, uint32(shardID), source.Ref, source.LogicEndpoint)
		f.placements.Set(value)
		f.submission.Moves = append(f.submission.Moves, core.ShardMoveIntent{
			Domain: f.submission.Domain, VirtualShard: uint32(shardID),
			SourceHolderID: source.Ref.HolderID, TargetHolderID: "player-svr-1", TargetOrdinal: 1,
		})
	}
	// Preserve the first deployed 8/8 format in the compatibility cases.
	f.submission.Limits = core.LegacyParallelFunctionalLimits()
	f.routePlans.SetDefaultPhase(core.RoutePlanPhasePending)
	return f
}

func startParallelWave(t *testing.T, f *scaleFixture) core.PlanRecord {
	t.Helper()
	record, err := f.executor.Submit(context.Background(), f.submission)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) {
		t.Fatalf("parallel wave did not wait: phase=%s status=%s err=%v", pending.Plan.Phase, pending.Plan.Status, err)
	}
	return pending
}

func TestFunctionalLimitsAcceptOnlyLegacyOrParallelPair(t *testing.T) {
	f := newScaleOutFixture(t)
	for _, limits := range []core.FunctionalLimits{
		{MaxRoutePlansInflight: core.FunctionalMaxInflight, MaxShardsPerWave: core.FunctionalMaxShardsPerWave},
		core.LegacyParallelFunctionalLimits(),
		{MaxRoutePlansInflight: core.ParallelSixteenMaxRoutePlansInflight, MaxShardsPerWave: core.ParallelSixteenMaxShardsPerWave},
		{MaxRoutePlansInflight: core.LegacyTwentyMaxRoutePlansInflight, MaxShardsPerWave: core.LegacyTwentyMaxShardsPerWave},
		core.ProductionFunctionalLimits(),
	} {
		value := f.submission.Clone()
		value.Limits = limits
		if _, err := value.Normalize(); err != nil {
			t.Fatalf("allowed limits %+v rejected: %v", limits, err)
		}
	}
	for _, limits := range []core.FunctionalLimits{
		{MaxRoutePlansInflight: 1, MaxShardsPerWave: 8},
		{MaxRoutePlansInflight: 8, MaxShardsPerWave: 1},
		{MaxRoutePlansInflight: 2, MaxShardsPerWave: 2},
		{MaxRoutePlansInflight: 0, MaxShardsPerWave: 0},
	} {
		value := f.submission.Clone()
		value.Limits = limits
		if _, err := value.Normalize(); !errors.Is(err, core.ErrInvalidPlan) {
			t.Errorf("mixed/other limits %+v err=%v", limits, err)
		}
	}
}

func TestFunctionalLimitsForParallelismExposesOnlyControlledProfiles(t *testing.T) {
	for _, parallelism := range []uint32{1, 8, 16, 32} {
		limits, err := core.FunctionalLimitsForParallelism(parallelism)
		if err != nil {
			t.Fatalf("parallelism=%d: %v", parallelism, err)
		}
		if limits.MaxRoutePlansInflight != parallelism || limits.MaxShardsPerWave != parallelism {
			t.Fatalf("parallelism=%d limits=%+v", parallelism, limits)
		}
	}
	if _, err := core.FunctionalLimitsForParallelism(20); !errors.Is(err, core.ErrInvalidPlan) {
		t.Fatalf("new 20/20 profile err=%v, want invalid while legacy decoding remains supported", err)
	}
}

func TestParallelSlidingDispatchFillsInitialWindow(t *testing.T) {
	f := newParallelScaleOutFixture(t, 9)
	record := startParallelWave(t, f)
	if record.Plan.WaveIndex != 0 || len(record.Plan.ResolvedMoves) != 9 || len(f.routePlans.Requests()) != 8 {
		t.Fatalf("wave checkpoint wave=%d resolved=%d children=%d", record.Plan.WaveIndex,
			len(record.Plan.ResolvedMoves), len(f.routePlans.Requests()))
	}
}

func TestProductionParallelSlidingDispatchBackfillsThirtyThird(t *testing.T) {
	f := newParallelScaleOutFixture(t, 33)
	f.submission.Limits = core.ProductionFunctionalLimits()
	record := startParallelWave(t, f)
	if record.Plan.WaveIndex != 0 || len(record.Plan.ResolvedMoves) != 33 || len(f.routePlans.Requests()) != 32 {
		t.Fatalf("production wave checkpoint wave=%d resolved=%d children=%d", record.Plan.WaveIndex,
			len(record.Plan.ResolvedMoves), len(f.routePlans.Requests()))
	}
	f.routePlans.SetView(record.Plan.ResolvedMoves[0].ChildPlanID, core.RoutePlanPhaseSucceeded)
	record, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) || len(f.routePlans.Requests()) != 33 {
		t.Fatalf("sliding backfill status=%s children=%d err=%v", record.Plan.Status, len(f.routePlans.Requests()), err)
	}
	requests := f.routePlans.Requests()
	for _, request := range requests {
		if request.OwnerWaveID != "" || request.OwnerWaveSize != 0 || request.OwnerWavePosition != 0 {
			t.Fatalf("shard %d retained cross-shard projection barrier=%q/%d/%d", request.VirtualShard,
				request.OwnerWaveID, request.OwnerWaveSize, request.OwnerWavePosition)
		}
	}
}

func TestParallelSlidingWindowVerifiesEachShardBeforeWindowCompletes(t *testing.T) {
	f := newParallelScaleOutFixture(t, 9)
	record := startParallelWave(t, f)
	first := record.Plan.ResolvedMoves[0].ChildPlanID
	f.routePlans.SetView(first, core.RoutePlanPhaseSucceeded)

	updated, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) {
		t.Fatalf("sliding verification status=%s phase=%s err=%v", updated.Plan.Status, updated.Plan.Phase, err)
	}
	if !updated.Plan.ResolvedMoves[0].Verified || updated.Plan.ResolvedMoves[0].ChildPhase != core.RoutePlanPhaseSucceeded {
		t.Fatalf("completed shard was not immediately verified: %+v", updated.Plan.ResolvedMoves[0])
	}
	if len(f.routePlans.Requests()) != 9 {
		t.Fatalf("verified shard did not free and refill its slot: children=%d", len(f.routePlans.Requests()))
	}
	if updated.Plan.Phase != core.PhaseMovingWave {
		t.Fatalf("unrelated children were forced through a final barrier: phase=%s", updated.Plan.Phase)
	}
}

func TestParallelVerificationRecoveryIsBoundedBySlidingWindow(t *testing.T) {
	f := newParallelScaleOutFixture(t, 64)
	f.submission.Limits = core.ProductionFunctionalLimits()
	record := startParallelWave(t, f)
	for _, move := range record.Plan.ResolvedMoves {
		request := move.RoutePlanRequest(record.Plan.Submission.RoutePlanTTLms, record.Plan.PlanID,
			record.Plan.RecoveryRetainUntilMs, record.Plan.Submission.Limits, len(record.Plan.Submission.Moves))
		if _, err := f.routePlans.EnsureMove(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		f.routePlans.SetView(move.ChildPlanID, core.RoutePlanPhaseSucceeded)
	}
	legacy := record.Plan.Clone()
	legacy.Phase = core.PhaseVerifyingWave
	for index := range legacy.ResolvedMoves {
		legacy.ResolvedMoves[index].EnsureObserved = true
		legacy.ResolvedMoves[index].ChildPhase = core.RoutePlanPhaseSucceeded
		legacy.ResolvedMoves[index].Verified = false
	}
	f.repository.ForceReplaceForTest(legacy)

	first, err := f.executor.Step(context.Background(), legacy.PlanID)
	if !errors.Is(err, core.ErrPending) || first.Plan.Phase != core.PhaseVerifyingWave {
		t.Fatalf("first bounded verification phase=%s err=%v", first.Plan.Phase, err)
	}
	verified := 0
	for _, move := range first.Plan.ResolvedMoves {
		if move.Verified {
			verified++
		}
	}
	if verified != int(core.ProductionFunctionalLimits().MaxRoutePlansInflight) {
		t.Fatalf("verified=%d, want one 32-slot recovery window", verified)
	}
	second, err := f.executor.Step(context.Background(), legacy.PlanID)
	if err != nil || second.Plan.Phase != core.PhaseVerifyingFinal {
		t.Fatalf("second bounded verification phase=%s err=%v", second.Plan.Phase, err)
	}
}

func TestParallelWaveResolutionUsesOneCompleteShardSetCapture(t *testing.T) {
	f := newParallelScaleOutFixture(t, 32)
	f.submission.Limits = core.ProductionFunctionalLimits()
	record := startParallelWave(t, f)
	if len(record.Plan.ResolvedMoves) != 32 {
		t.Fatalf("resolved moves=%d, want 32", len(record.Plan.ResolvedMoves))
	}
	if calls := f.placements.ShardSetCaptureCalls(); calls != 1 {
		t.Fatalf("placement shard-set captures=%d, want one complete wave capture", calls)
	}
}

func TestParallelWaveOutOfOrderSuccessWaitsForAllAndAdvancesByEight(t *testing.T) {
	f := newParallelScaleOutFixture(t, 8)
	record := startParallelWave(t, f)
	f.routePlans.SetView(record.Plan.ResolvedMoves[7].ChildPlanID, core.RoutePlanPhaseSucceeded)
	waiting, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) || waiting.Plan.Phase != core.PhaseMovingWave || waiting.Plan.WaveIndex != 0 {
		t.Fatalf("out-of-order child advanced wave: phase=%s wave=%d err=%v", waiting.Plan.Phase, waiting.Plan.WaveIndex, err)
	}
	if waiting.Plan.ResolvedMoves[7].ChildPhase != core.RoutePlanPhaseSucceeded {
		t.Fatal("successful out-of-order child was not durably observed")
	}
	for _, move := range waiting.Plan.ResolvedMoves {
		f.routePlans.SetView(move.ChildPlanID, core.RoutePlanPhaseSucceeded)
	}
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || result.Plan.Status != core.StatusSucceeded || result.Plan.WaveIndex != 8 {
		t.Fatalf("full wave did not complete at exclusive index 8: phase=%s status=%s wave=%d err=%v",
			result.Plan.Phase, result.Plan.Status, result.Plan.WaveIndex, err)
	}
}

func TestIndependentShardWindowWaitsWhileTransientChildPauseRecovers(t *testing.T) {
	f := newParallelScaleOutFixture(t, 8)
	record := startParallelWave(t, f)
	child := record.Plan.ResolvedMoves[0].ChildPlanID
	f.routePlans.SetViewWithError(child, core.RoutePlanPhasePaused, "TIMEOUT")
	waiting, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) || waiting.Plan.Status == core.StatusBlocked {
		t.Fatalf("transient per-shard pause blocked parent: status=%s phase=%s err=%v", waiting.Plan.Status, waiting.Plan.Phase, err)
	}
	f.routePlans.SetView(child, core.RoutePlanPhasePending)
	if resumed, err := f.executor.Execute(context.Background(), waiting.Plan.PlanID); !errors.Is(err, core.ErrPending) || resumed.Plan.Status == core.StatusBlocked {
		t.Fatalf("recovered child did not remain resumable: status=%s phase=%s err=%v", resumed.Plan.Status, resumed.Plan.Phase, err)
	}
}

func TestCompletedParallelWaveUsesBoundedParentCheckpoints(t *testing.T) {
	for _, parallelism := range []uint32{8, 16, 32} {
		t.Run(fmt.Sprintf("parallelism_%d", parallelism), func(t *testing.T) {
			f := newParallelScaleOutFixture(t, int(parallelism))
			limits, err := core.FunctionalLimitsForParallelism(parallelism)
			if err != nil {
				t.Fatal(err)
			}
			f.submission.Limits = limits
			record := startParallelWave(t, f)
			beforeRevision := record.Plan.Revision
			for _, move := range record.Plan.ResolvedMoves {
				f.routePlans.SetView(move.ChildPlanID, core.RoutePlanPhaseSucceeded)
			}

			result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
			if err != nil || result.Plan.Status != core.StatusSucceeded {
				t.Fatalf("completed wave status=%s phase=%s err=%v", result.Plan.Status, result.Plan.Phase, err)
			}
			// A completed parallel wave needs one Execute attempt checkpoint, one
			// batched observation, one batched verification, and final verification.
			// The count must not grow with the number of child RoutePlans.
			if delta := result.Plan.Revision - beforeRevision; delta != 4 {
				t.Fatalf("parallelism=%d parent revision delta=%d, want 4", parallelism, delta)
			}
		})
	}
}

func TestParallelSlidingWindowCompletesAfterBackfill(t *testing.T) {
	f := newParallelScaleOutFixture(t, 10)
	first := startParallelWave(t, f)
	for index := 0; index < 8; index++ {
		f.routePlans.SetView(first.Plan.ResolvedMoves[index].ChildPlanID, core.RoutePlanPhaseSucceeded)
	}
	second, err := f.executor.Execute(context.Background(), first.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) || second.Plan.Phase != core.PhaseMovingWave || second.Plan.WaveIndex != 0 || len(second.Plan.ResolvedMoves) != 10 {
		t.Fatalf("short wave checkpoint phase=%s wave=%d resolved=%d err=%v", second.Plan.Phase,
			second.Plan.WaveIndex, len(second.Plan.ResolvedMoves), err)
	}
	if len(f.routePlans.Requests()) != 10 {
		t.Fatalf("short wave children=%d", len(f.routePlans.Requests()))
	}
	for _, move := range second.Plan.ResolvedMoves {
		f.routePlans.SetView(move.ChildPlanID, core.RoutePlanPhaseSucceeded)
	}
	result, err := f.executor.Execute(context.Background(), second.Plan.PlanID)
	if err != nil || result.Plan.Status != core.StatusSucceeded || result.Plan.WaveIndex != 10 {
		t.Fatalf("final short wave result phase=%s status=%s wave=%d err=%v", result.Plan.Phase,
			result.Plan.Status, result.Plan.WaveIndex, err)
	}
}

func TestParallelWavePartialCreateRestartRetainsChildIDs(t *testing.T) {
	f := newParallelScaleOutFixture(t, 8)
	f.routePlans.SetEnsureFailureAfter(3, errors.New("injected routeplan dependency"))
	record, err := f.executor.Submit(context.Background(), f.submission)
	if err != nil {
		t.Fatal(err)
	}
	paused, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPaused) || paused.Plan.Status != core.StatusPaused || len(f.routePlans.Requests()) != 3 {
		t.Fatalf("partial create status=%s children=%d err=%v", paused.Plan.Status, len(f.routePlans.Requests()), err)
	}
	ids := make([]string, len(paused.Plan.ResolvedMoves))
	for index, move := range paused.Plan.ResolvedMoves {
		ids[index] = move.ChildPlanID
	}
	f.routePlans.SetEnsureFailureAfter(-1, nil)
	if _, err := f.executor.Resume(context.Background(), paused.Plan.PlanID); err != nil {
		t.Fatal(err)
	}
	f.executor = newExecutor(t, f)
	recovered, err := f.executor.Execute(context.Background(), paused.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) || len(f.routePlans.Requests()) != 8 {
		t.Fatalf("recovered status=%s phase=%s children=%d err=%v", recovered.Plan.Status, recovered.Plan.Phase,
			len(f.routePlans.Requests()), err)
	}
	for index, move := range recovered.Plan.ResolvedMoves {
		if move.ChildPlanID != ids[index] {
			t.Fatalf("child %d id changed %s/%s", index, move.ChildPlanID, ids[index])
		}
		if index < 3 && f.routePlans.EnsureCalls(move.ChildPlanID) != 1 {
			t.Fatalf("child %d was recreated calls=%d", index, f.routePlans.EnsureCalls(move.ChildPlanID))
		}
	}
}

func TestParallelWaveParentCASAmbiguityReconcilesPartialCreation(t *testing.T) {
	f := newParallelScaleOutFixture(t, 8)
	record, err := f.executor.Submit(context.Background(), f.submission)
	if err != nil {
		t.Fatal(err)
	}
	for len(record.Plan.ResolvedMoves) != 8 {
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}
	childID := record.Plan.ResolvedMoves[0].ChildPlanID
	injected := errors.New("injected parent CAS ambiguity")
	f.repository.FailNextCAS(injected)
	if _, err := f.executor.Step(context.Background(), record.Plan.PlanID); !errors.Is(err, injected) {
		t.Fatalf("parent CAS error=%v", err)
	}
	if len(f.routePlans.Requests()) != 8 || f.routePlans.EnsureCalls(childID) != 1 {
		t.Fatalf("bulk child creation children=%d calls=%d", len(f.routePlans.Requests()), f.routePlans.EnsureCalls(childID))
	}
	f.executor = newExecutor(t, f)
	recovered, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) || len(f.routePlans.Requests()) != 8 || f.routePlans.EnsureCalls(childID) != 1 {
		t.Fatalf("recovery phase=%s children=%d calls=%d err=%v", recovered.Plan.Phase, len(f.routePlans.Requests()),
			f.routePlans.EnsureCalls(childID), err)
	}
}

func TestParallelWavePauseOrFailureStopsBeyondFrozenChild(t *testing.T) {
	for _, test := range []struct {
		name  string
		phase string
		want  core.ExecutionStatus
		err   error
	}{
		{name: "paused", phase: core.RoutePlanPhasePaused, want: core.StatusBlocked, err: core.ErrBlocked},
		{name: "failed", phase: core.RoutePlanPhaseFailed, want: core.StatusFailed, err: core.ErrDependency},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newParallelScaleOutFixture(t, 8)
			f.routePlans.SetNextPhase(test.phase)
			record, err := f.executor.Submit(context.Background(), f.submission)
			if err != nil {
				t.Fatal(err)
			}
			result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
			if !errors.Is(err, test.err) || result.Plan.Status != test.want || len(f.routePlans.Requests()) != 1 {
				t.Fatalf("status=%s phase=%s children=%d err=%v", result.Plan.Status, result.Plan.Phase,
					len(f.routePlans.Requests()), err)
			}
		})
	}
}

func newParallelScaleInFixture(t *testing.T, moveCount int) *scaleFixture {
	t.Helper()
	f := newScaleInFixture(t)
	victim, found, err := f.holders.GetHolder(context.Background(), core.HolderRef{
		Domain: f.submission.Domain, HolderID: "player-svr-1", Ordinal: 1,
	})
	if err != nil || !found {
		t.Fatalf("victim holder: found=%t err=%v", found, err)
	}
	target, found, err := f.holders.GetHolder(context.Background(), core.HolderRef{
		Domain: f.submission.Domain, HolderID: "player-svr-0", Ordinal: 0,
	})
	if err != nil || !found {
		t.Fatalf("target holder: found=%t err=%v", found, err)
	}
	if !f.placements.Unassign(f.submission.Domain, 8) {
		t.Fatal("failed to remove original victim shard")
	}
	if err := f.holders.Update(victim.Ref, func(value *core.HolderState) {
		value.ServingShardCount = uint32(moveCount)
		value.State = core.HolderServing
	}); err != nil {
		t.Fatal(err)
	}
	f.submission.Moves = nil
	for shardID := 0; shardID < moveCount; shardID++ {
		value := shard(f.now, uint32(shardID), victim.Ref, victim.LogicEndpoint)
		f.placements.Set(value)
		f.submission.Moves = append(f.submission.Moves, core.ShardMoveIntent{
			Domain: f.submission.Domain, VirtualShard: uint32(shardID),
			SourceHolderID: victim.Ref.HolderID, TargetHolderID: target.Ref.HolderID, TargetOrdinal: target.Ref.Ordinal,
		})
	}
	f.submission.Limits = core.LegacyParallelFunctionalLimits()
	f.routePlans.SetDefaultPhase(core.RoutePlanPhasePending)
	f.routePlans.SetEnsureHook(func(request core.RoutePlanMoveRequest) {
		f.placements.Move(request.Domain, request.VirtualShard, request.Target, request.TargetEndpoint)
		_ = f.holders.Update(victim.Ref, func(value *core.HolderState) {
			if value.ServingShardCount > 0 {
				value.ServingShardCount--
			}
			if value.ServingShardCount == 0 {
				value.State = core.HolderReadyZeroShard
			}
		})
		_ = f.holders.Update(target.Ref, func(value *core.HolderState) {
			value.ServingShardCount++
			value.State = core.HolderServing
		})
	})
	return f
}

func TestParallelScaleInDoesNotScaleDownBeforeEveryWaveChildIsVerified(t *testing.T) {
	f := newParallelScaleInFixture(t, 8)
	record := startParallelWave(t, f)
	if record.Plan.Phase != core.PhaseMovingWave || f.workloads.Calls() != 0 {
		t.Fatalf("scale-in advanced early phase=%s scale_calls=%d", record.Plan.Phase, f.workloads.Calls())
	}
	for index := 0; index < 7; index++ {
		f.routePlans.SetView(record.Plan.ResolvedMoves[index].ChildPlanID, core.RoutePlanPhaseSucceeded)
	}
	waiting, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) || waiting.Plan.Phase == core.PhaseScalingDown || f.workloads.Calls() != 0 {
		t.Fatalf("partial success scaled down phase=%s calls=%d err=%v", waiting.Plan.Phase, f.workloads.Calls(), err)
	}
	f.routePlans.SetView(record.Plan.ResolvedMoves[7].ChildPlanID, core.RoutePlanPhaseSucceeded)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || result.Plan.Status != core.StatusSucceeded || f.workloads.Calls() != 1 {
		t.Fatalf("scale-in result phase=%s status=%s calls=%d err=%v", result.Plan.Phase, result.Plan.Status,
			f.workloads.Calls(), err)
	}
}
