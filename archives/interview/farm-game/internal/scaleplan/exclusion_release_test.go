package scaleplan_test

import (
	"context"
	"errors"
	"testing"

	core "farmgame/internal/scaleplan"
	testkit "farmgame/internal/testkit/scaleplan"
)

func TestPlacementReleaseOutcomeMappingIsClosed(t *testing.T) {
	outcomes := []core.PlacementReleaseOutcome{
		core.PlacementReleaseReleased,
		core.PlacementReleaseAlreadyReleased,
		core.PlacementReleaseBootRollover,
		core.PlacementReleaseNotFound,
		core.PlacementReleaseOwnerMismatch,
		core.PlacementReleaseRevisionConflict,
		core.PlacementReleaseMalformed,
		core.PlacementReleaseUnknown,
	}
	if len(outcomes) != 8 {
		t.Fatalf("outcome table has %d rows", len(outcomes))
	}
	for index, outcome := range outcomes {
		if !outcome.Valid() {
			t.Fatalf("outcome %d is not valid: %q", index, outcome)
		}
		wantTerminal := index < 3
		if outcome.TerminalForScalePlan() != wantTerminal {
			t.Fatalf("outcome %q terminal=%t want=%t", outcome, outcome.TerminalForScalePlan(), wantTerminal)
		}
	}
	for _, outcome := range []core.PlacementReleaseOutcome{"", "NEW_OUTCOME"} {
		if outcome.Valid() || outcome.TerminalForScalePlan() {
			t.Fatalf("unclosed outcome accepted: %q", outcome)
		}
	}
}

func scaleInAtPhase(t *testing.T, f *scaleFixture, phase core.Phase) core.PlanRecord {
	t.Helper()
	record, err := f.executor.Submit(context.Background(), f.submission)
	if err != nil {
		t.Fatal(err)
	}
	for record.Plan.Phase != phase {
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatalf("advance to %s: phase=%s status=%s err=%v", phase, record.Plan.Phase, record.Plan.Status, err)
		}
	}
	return record
}

func markVictimPlacementInactive(t *testing.T, f *scaleFixture) {
	t.Helper()
	capture, err := f.placements.CaptureShard(context.Background(), f.submission.Domain, 8)
	if err != nil || !capture.Found {
		t.Fatalf("victim placement capture found=%t err=%v", capture.Found, err)
	}
	capture.Placement.Active = false
	f.placements.Set(capture.Placement)
}

func TestScalePlanScaleInFailureReleasesExclusionBeforeTerminal(t *testing.T) {
	f := newScaleInFixture(t)
	record := scaleInAtPhase(t, f, core.PhaseMovingWave)
	markVictimPlacementInactive(t, f)
	result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrConflict) || result.Plan.Status != core.StatusFailed || result.Plan.ErrorCode != "CHILD_RESOLUTION" {
		t.Fatalf("result=%s/%s code=%s err=%v", result.Plan.Status, result.Plan.Phase, result.Plan.ErrorCode, err)
	}
	if f.exclusions.ReleaseCalls() != 1 {
		t.Fatalf("release calls=%d", f.exclusions.ReleaseCalls())
	}
	if result.Plan.CleanupIntent == nil || result.Plan.CleanupIntent.ScaleResolution != core.ScaleResolutionNotCommitted ||
		result.Plan.CleanupIntent.ReleaseOutcome != core.PlacementReleaseReleased {
		t.Fatalf("cleanup=%+v", result.Plan.CleanupIntent)
	}
	state, found, err := f.exclusions.Get(context.Background(), result.Plan.Victim)
	if err != nil || !found || state.Excluded || state.OwnerPlanID != "" {
		t.Fatalf("exclusion remained after terminal failure: found=%t state=%+v err=%v", found, state, err)
	}
}

func TestScalePlanScaleInFailureAcceptsBootRolloverRelease(t *testing.T) {
	f := newScaleInFixture(t)
	f.exclusions.SetReleaseOutcome(core.PlacementReleaseBootRollover)
	record := scaleInAtPhase(t, f, core.PhaseMovingWave)
	markVictimPlacementInactive(t, f)
	result, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrConflict) || result.Plan.Status != core.StatusFailed || result.Plan.ErrorCode != "CHILD_RESOLUTION" {
		t.Fatalf("result=%s/%s code=%s err=%v", result.Plan.Status, result.Plan.Phase, result.Plan.ErrorCode, err)
	}
	if result.Plan.CleanupIntent == nil || result.Plan.CleanupIntent.ReleaseOutcome != core.PlacementReleaseBootRollover {
		t.Fatalf("cleanup=%+v", result.Plan.CleanupIntent)
	}
}

func TestScalePlanScaleInFailureAfterUnknownScaleBlocksBeforeRelease(t *testing.T) {
	f := newScaleInFixture(t)
	record := scaleInAtPhase(t, f, core.PhaseScalingDown)
	f.workloads.SetUnknown(testkit.ScaleNoApplyThenUnknown)
	pending, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) || pending.Plan.Phase != core.PhaseScalingDown || f.workloads.Calls() != 1 {
		t.Fatalf("pending=%s/%s err=%v calls=%d", pending.Plan.Status, pending.Plan.Phase, err, f.workloads.Calls())
	}
	state := f.workloads.State()
	state.ResourceVersion = "scale-unknown-drift"
	f.workloads.SetState(state)
	blocked, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || blocked.Plan.Status != core.StatusBlocked ||
		blocked.Plan.Phase != core.PhaseReleasingExclusion || blocked.Plan.BlockedReason != "EXCLUSION_RELEASE_SCALE_UNKNOWN" {
		t.Fatalf("blocked=%s/%s reason=%s err=%v", blocked.Plan.Status, blocked.Plan.Phase, blocked.Plan.BlockedReason, err)
	}
	if f.exclusions.ReleaseCalls() != 0 || blocked.Plan.CleanupIntent == nil ||
		blocked.Plan.CleanupIntent.ScaleResolution != core.ScaleResolutionPending {
		t.Fatalf("unknown scale was released: calls=%d cleanup=%+v", f.exclusions.ReleaseCalls(), blocked.Plan.CleanupIntent)
	}
	active, found, err := f.exclusions.Get(context.Background(), blocked.Plan.Victim)
	if err != nil || !found || !active.Excluded {
		t.Fatalf("exclusion not retained while scale is unknown: found=%t state=%+v err=%v", found, active, err)
	}

	state.ResourceVersion = blocked.Plan.WorkloadIdentity.ResourceVersion
	f.workloads.SetState(state)
	f.executor = newExecutor(t, f)
	terminal, err := f.executor.ReconcileBlocked(context.Background(), blocked.Plan.PlanID)
	if !errors.Is(err, core.ErrOutcomeUnknown) || terminal.Plan.Status != core.StatusFailed || f.exclusions.ReleaseCalls() != 1 {
		t.Fatalf("reconciled=%s/%s err=%v release_calls=%d", terminal.Plan.Status, terminal.Plan.Phase, err, f.exclusions.ReleaseCalls())
	}
}

func blockedScaleInBeforeScale(t *testing.T, outcome core.PlacementReleaseOutcome) (*scaleFixture, core.PlanRecord) {
	t.Helper()
	f := newScaleInFixture(t)
	f.exclusions.SetReleaseOutcome(outcome)
	record := scaleInAtPhase(t, f, core.PhaseCapturingVictimShards)
	markVictimPlacementInactive(t, f)
	blocked, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || blocked.Plan.Status != core.StatusBlocked || blocked.Plan.BlockedReason != "SCALE_IN_MANIFEST_DRIFT" {
		t.Fatalf("blocked=%s/%s reason=%s err=%v", blocked.Plan.Status, blocked.Plan.Phase, blocked.Plan.BlockedReason, err)
	}
	return f, blocked
}

func TestBlockedScaleInSafeTerminationReleasesExactExclusion(t *testing.T) {
	f, blocked := blockedScaleInBeforeScale(t, core.PlacementReleaseReleased)
	request, err := core.BuildBlockedTerminationRequest(blocked.Plan, "scale-in-safe-release")
	if err != nil {
		t.Fatal(err)
	}
	terminated, err := f.executor.OperatorFailBlocked(context.Background(), request)
	if err != nil || terminated.Plan.Status != core.StatusFailed || terminated.Plan.BlockedTerminationProof == nil ||
		terminated.Plan.BlockedTerminationProof.ScaleResolution != core.ScaleResolutionNotCommitted ||
		terminated.Plan.BlockedTerminationProof.ExclusionReleaseOutcome != core.PlacementReleaseReleased {
		t.Fatalf("terminated=%s proof=%+v err=%v", terminated.Plan.Status, terminated.Plan.BlockedTerminationProof, err)
	}
	if f.exclusions.ReleaseCalls() != 1 {
		t.Fatalf("release calls=%d", f.exclusions.ReleaseCalls())
	}
	replayed, err := f.executor.OperatorFailBlocked(context.Background(), request)
	if err != nil || replayed.Plan.Revision != terminated.Plan.Revision || f.exclusions.ReleaseCalls() != 1 {
		t.Fatalf("replay revision=%d/%d release_calls=%d err=%v", replayed.Plan.Revision, terminated.Plan.Revision, f.exclusions.ReleaseCalls(), err)
	}
}

func TestBlockedScaleInSafeTerminationFailsClosedForNotFoundAndUnknown(t *testing.T) {
	for _, test := range []struct {
		name    string
		outcome core.PlacementReleaseOutcome
		wantErr error
	}{
		{name: "not found", outcome: core.PlacementReleaseNotFound, wantErr: core.ErrVerification},
		{name: "unknown", outcome: core.PlacementReleaseUnknown, wantErr: core.ErrOutcomeUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, blocked := blockedScaleInBeforeScale(t, test.outcome)
			request, err := core.BuildBlockedTerminationRequest(blocked.Plan, "scale-in-unsafe-"+test.name)
			if err != nil {
				t.Fatal(err)
			}
			result, err := f.executor.OperatorFailBlocked(context.Background(), request)
			if !errors.Is(err, test.wantErr) || result.Plan.Status != core.StatusBlocked {
				t.Fatalf("result=%s reason=%s err=%v want=%v", result.Plan.Status, result.Plan.BlockedReason, err, test.wantErr)
			}
			if f.exclusions.ReleaseCalls() != 1 {
				t.Fatalf("release calls=%d", f.exclusions.ReleaseCalls())
			}
			state, found, getErr := f.exclusions.Get(context.Background(), blocked.Plan.Victim)
			if getErr != nil || !found || !state.Excluded {
				t.Fatalf("unsafe release changed exclusion: found=%t state=%+v err=%v", found, state, getErr)
			}
		})
	}
}
