package scaleplan_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/scaleplan"
)

func TestScalePlanRejectsIllegalPhaseJump(t *testing.T) {
	f := newScaleOutFixture(t)
	plan, err := core.BuildInitialPlan(f.submission, f.now)
	if err != nil {
		t.Fatal(err)
	}
	candidate := plan.Clone()
	candidate.Phase = core.PhaseWaitingHolderReady
	candidate.Revision++
	candidate.UpdatedAtMs++
	if err := core.ValidatePlanUpdate(plan, candidate); !errors.Is(err, core.ErrInvalidPlan) {
		t.Fatalf("illegal jump=%v", err)
	}
}

func TestScalePlanPauseResumePreservesCheckpoint(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	paused, err := f.executor.Pause(context.Background(), record.Plan.PlanID, "OPERATOR_PAUSED")
	if !errors.Is(err, core.ErrPaused) || paused.Plan.Status != core.StatusPaused || paused.Plan.Phase != core.PhasePending {
		t.Fatalf("pause=%s/%s err=%v", paused.Plan.Status, paused.Plan.Phase, err)
	}
	if _, err := f.executor.Execute(context.Background(), record.Plan.PlanID); !errors.Is(err, core.ErrPaused) || f.workloads.Calls() != 0 {
		t.Fatalf("paused execute err=%v calls=%d", err, f.workloads.Calls())
	}
	resumed, err := f.executor.Resume(context.Background(), record.Plan.PlanID)
	if err != nil || resumed.Plan.Status != core.StatusRunning || resumed.Plan.Phase != core.PhasePending {
		t.Fatalf("resume=%s/%s err=%v", resumed.Plan.Status, resumed.Plan.Phase, err)
	}
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || result.Plan.Status != core.StatusSucceeded {
		t.Fatalf("execute=%s err=%v", result.Plan.Status, err)
	}
}

func TestScalePlanChildPausedRequiresControlledReconciliation(t *testing.T) {
	f := newScaleOutFixture(t)
	f.routePlans.SetNextPhase(core.RoutePlanPhasePaused)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || result.Plan.Status != core.StatusBlocked || result.Plan.BlockedReason != "CHILD_ROUTEPLAN_PAUSED" {
		t.Fatalf("result=%s reason=%s err=%v", result.Plan.Status, result.Plan.BlockedReason, err)
	}
	if _, err := f.executor.Resume(context.Background(), record.Plan.PlanID); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("resume child pause=%v", err)
	}
	child := result.Plan.ResolvedMoves[0]
	f.placements.Move(child.Intent.Domain, child.Intent.VirtualShard, child.Target, child.TargetEndpoint)
	reconciled, err := f.executor.ReconcileBlocked(context.Background(), record.Plan.PlanID)
	if err != nil || reconciled.Plan.Status != core.StatusRunning || !reconciled.Plan.ResolvedMoves[0].Reconciled ||
		reconciled.Plan.Phase != core.PhaseVerifyingWave {
		t.Fatalf("reconciled=%s/%s child=%+v err=%v", reconciled.Plan.Status, reconciled.Plan.Phase, reconciled.Plan.ResolvedMoves[0], err)
	}
	completed, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || completed.Plan.Status != core.StatusSucceeded {
		t.Fatalf("completed=%s err=%v", completed.Plan.Status, err)
	}
}

func TestScalePlanObservedChildCanLaterUseControlledPausedReconciliation(t *testing.T) {
	f := newScaleOutFixture(t)
	f.routePlans.SetNextPhase(core.RoutePlanPhaseDraining)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	pending, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) || pending.Plan.Phase != core.PhaseMovingWave ||
		len(pending.Plan.ResolvedMoves) != 1 || pending.Plan.ResolvedMoves[0].ChildPhase != core.RoutePlanPhaseDraining {
		t.Fatalf("pending=%s/%s child=%+v err=%v", pending.Plan.Status, pending.Plan.Phase, pending.Plan.ResolvedMoves, err)
	}
	child := pending.Plan.ResolvedMoves[0]
	f.routePlans.SetView(child.ChildPlanID, core.RoutePlanPhasePaused)
	blocked, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || blocked.Plan.Status != core.StatusBlocked {
		t.Fatalf("blocked=%s err=%v", blocked.Plan.Status, err)
	}
	reconciled, err := f.executor.ReconcileBlocked(context.Background(), record.Plan.PlanID)
	if err != nil || reconciled.Plan.Status != core.StatusRunning || reconciled.Plan.Phase != core.PhaseVerifyingWave ||
		!reconciled.Plan.ResolvedMoves[0].Reconciled {
		t.Fatalf("reconciled=%s/%s child=%+v err=%v", reconciled.Plan.Status, reconciled.Plan.Phase,
			reconciled.Plan.ResolvedMoves[0], err)
	}
}

func TestScalePlanPausedChildObservedDuringVerificationCanReconcile(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for record.Plan.Phase != core.PhaseVerifyingWave {
		var err error
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}
	child := record.Plan.ResolvedMoves[0]
	f.routePlans.SetView(child.ChildPlanID, core.RoutePlanPhasePaused)
	blocked, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || blocked.Plan.Status != core.StatusBlocked ||
		blocked.Plan.Phase != core.PhaseVerifyingWave || blocked.Plan.BlockedReason != "CHILD_ROUTEPLAN_PAUSED" {
		t.Fatalf("blocked=%s/%s reason=%s err=%v", blocked.Plan.Status, blocked.Plan.Phase, blocked.Plan.BlockedReason, err)
	}
	reconciled, err := f.executor.ReconcileBlocked(context.Background(), record.Plan.PlanID)
	if err != nil || reconciled.Plan.Status != core.StatusRunning || reconciled.Plan.Phase != core.PhaseVerifyingWave ||
		!reconciled.Plan.ResolvedMoves[0].Reconciled {
		t.Fatalf("reconciled=%s/%s child=%+v err=%v", reconciled.Plan.Status, reconciled.Plan.Phase,
			reconciled.Plan.ResolvedMoves[0], err)
	}
	completed, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || completed.Plan.Status != core.StatusSucceeded {
		t.Fatalf("completed=%s err=%v", completed.Plan.Status, err)
	}
}

func TestScalePlanBlockedChildKeepsWorkloadLockUntilVerified(t *testing.T) {
	f := newScaleOutFixture(t)
	f.routePlans.SetNextPhase(core.RoutePlanPhasePaused)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	blocked, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || blocked.Plan.Status != core.StatusBlocked {
		t.Fatalf("blocked=%s err=%v", blocked.Plan.Status, err)
	}
	child := blocked.Plan.ResolvedMoves[0]
	f.routePlans.SetVerification(child.ChildPlanID, false, "NOT_CONSISTENT")
	if _, err := f.executor.ReconcileBlocked(context.Background(), record.Plan.PlanID); !errors.Is(err, core.ErrBlocked) {
		t.Fatalf("unsafe reconcile=%v", err)
	}
	second := f.submission.Clone()
	second.IdempotencyKey = "blocked-child-second-plan"
	if _, err := f.executor.Submit(context.Background(), second); !errors.Is(err, core.ErrWorkloadBusy) {
		t.Fatalf("blocked plan released workload lock: %v", err)
	}
}

func TestScalePlanCASRejectsProgressWhileStatusRemainsPaused(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	record, _ = f.executor.Step(context.Background(), record.Plan.PlanID)
	paused, err := f.executor.Pause(context.Background(), record.Plan.PlanID, "OPERATOR_PAUSED")
	if !errors.Is(err, core.ErrPaused) {
		t.Fatal(err)
	}
	candidate := paused.Plan.Clone()
	candidate.ScaleIntent.Effect = core.EffectCommitted
	candidate.Phase = core.PhaseWaitingHolderReady
	candidate.Revision++
	candidate.UpdatedAtMs++
	if err := core.ValidatePlanUpdate(paused.Plan, candidate); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("paused phase progress accepted: %v", err)
	}
}

// CF-2 chose to keep the DEGRADED escalation counters in controller memory
// rather than in the Plan record, which is only defensible while DEGRADED stays
// out of every safety decision. This test pins the property that decision rests
// on: BLOCKED models a reason that is fixed once determined, with exactly one
// whitelisted exception. If a later change adds a second one -- a DEGRADED
// transition being the obvious candidate -- this fails and CF-2 has to be
// reopened rather than quietly outgrown.
func TestBlockedReasonHasExactlyOneWhitelistedTransition(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for record.Plan.Phase != core.PhaseMovingWave {
		record, _ = f.executor.Step(context.Background(), record.Plan.PlanID)
	}
	base := record.Plan.Clone()
	base.Status = core.StatusBlocked

	advance := func(from, to string) error {
		current := base.Clone()
		current.BlockedReason = from
		candidate := current.Clone()
		candidate.BlockedReason = to
		candidate.Revision++
		candidate.UpdatedAtMs++
		return core.ValidatePlanUpdate(current, candidate)
	}

	if err := advance("CHILD_ROUTEPLAN_PAUSED", "RECOVERY_RETENTION_EXCEEDED"); err != nil {
		t.Fatalf("the one whitelisted blocked-reason transition was rejected: %v", err)
	}
	for _, reason := range []string{
		"DEGRADED_EXCLUSION_UNRELEASED",
		"DEGRADED_BARRIER_UNRESOLVED",
		"CHILD_ROUTEPLAN_PAUSED",
	} {
		if err := advance("RECOVERY_RETENTION_EXCEEDED", reason); !errors.Is(err, core.ErrInvalidTransition) {
			t.Fatalf("blocked reason advanced to %s without reconciliation: %v", reason, err)
		}
	}
}

func TestScalePlanCASRejectsSamePhaseFactsWhileStatusRemainsBlocked(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for record.Plan.Phase != core.PhaseMovingWave {
		record, _ = f.executor.Step(context.Background(), record.Plan.PlanID)
	}
	current := record.Plan.Clone()
	current.Status = core.StatusBlocked
	current.BlockedReason = "RECOVERY_RETENTION_EXCEEDED"
	capture, _ := f.placements.CaptureShard(context.Background(), f.submission.Domain, 7)
	target, found, _ := f.holders.GetHolder(context.Background(), current.Target)
	if !found {
		t.Fatal("target missing")
	}
	move, err := core.ResolveChildMove(current, 0, capture.Placement, target)
	if err != nil {
		t.Fatal(err)
	}
	candidate := current.Clone()
	candidate.ResolvedMoves = append(candidate.ResolvedMoves, move)
	candidate.Revision++
	candidate.UpdatedAtMs++
	if err := core.ValidatePlanUpdate(current, candidate); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("blocked same-phase facts accepted: %v", err)
	}
}

func TestScalePlanBlockedReconciliationRequiresExactBlockedReason(t *testing.T) {
	f := newScaleOutFixture(t)
	f.routePlans.SetNextPhase(core.RoutePlanPhasePaused)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	blocked, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) {
		t.Fatal(err)
	}
	rewritten := blocked.Plan.Clone()
	rewritten.BlockedReason = "SCALE_IN_MANIFEST_DRIFT"
	rewritten.Revision++
	rewritten.UpdatedAtMs++
	if err := core.ValidatePlanUpdate(blocked.Plan, rewritten); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("blocked reason rewrite accepted: %v", err)
	}
	wrongCurrent := blocked.Plan.Clone()
	wrongCurrent.BlockedReason = "SCALE_IN_MANIFEST_DRIFT"
	candidate := wrongCurrent.Clone()
	candidate.Status = core.StatusRunning
	candidate.BlockedReason = ""
	candidate.Phase = core.PhaseVerifyingWave
	candidate.ResolvedMoves[0].EnsureObserved = true
	candidate.ResolvedMoves[0].Verified = true
	candidate.ResolvedMoves[0].Reconciled = true
	candidate.ResolvedMoves[0].ChildPhase = core.RoutePlanPhasePaused
	candidate.Revision++
	candidate.UpdatedAtMs++
	if err := core.ValidatePlanUpdate(wrongCurrent, candidate); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("wrong blocked reason reconciliation accepted: %v", err)
	}
}

func TestScalePlanCASAcceptsSkippedForwardNonterminalObservations(t *testing.T) {
	f := newScaleOutFixture(t)
	f.routePlans.SetNextPhase(core.RoutePlanPhasePending)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	pending, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) || pending.Plan.Phase != core.PhaseMovingWave ||
		pending.Plan.ResolvedMoves[0].ChildPhase != core.RoutePlanPhasePending {
		t.Fatalf("pending=%s/%s child=%+v err=%v", pending.Plan.Status, pending.Plan.Phase, pending.Plan.ResolvedMoves, err)
	}
	for _, phase := range []string{core.RoutePlanPhasePreparing, core.RoutePlanPhaseActive} {
		candidate := pending.Plan.Clone()
		candidate.ResolvedMoves[0].ChildPhase = phase
		candidate.Revision++
		candidate.UpdatedAtMs++
		if err := core.ValidatePlanUpdate(pending.Plan, candidate); err != nil {
			t.Fatalf("PENDING -> %s rejected: %v", phase, err)
		}
	}
	draining := pending.Plan.Clone()
	draining.ResolvedMoves[0].ChildPhase = core.RoutePlanPhaseDraining
	candidate := draining.Clone()
	candidate.ResolvedMoves[0].ChildPhase = core.RoutePlanPhaseActive
	candidate.Revision++
	candidate.UpdatedAtMs++
	if err := core.ValidatePlanUpdate(draining, candidate); err != nil {
		t.Fatalf("DRAINING -> ACTIVE rejected: %v", err)
	}
	regressed := draining.Clone()
	regressed.ResolvedMoves[0].ChildPhase = core.RoutePlanPhasePending
	regressed.Revision++
	regressed.UpdatedAtMs++
	if err := core.ValidatePlanUpdate(draining, regressed); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("DRAINING -> PENDING accepted: %v", err)
	}
}

func TestScalePlanCASRejectsTerminalChildOutcomeInSameMovingPhase(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for record.Plan.Phase != core.PhaseMovingWave || len(record.Plan.ResolvedMoves) == 0 {
		var err error
		record, err = f.executor.Step(context.Background(), record.Plan.PlanID)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, phase := range []string{core.RoutePlanPhaseSucceeded, core.RoutePlanPhasePaused, core.RoutePlanPhaseFailed} {
		candidate := record.Plan.Clone()
		candidate.ResolvedMoves[0].EnsureObserved = true
		candidate.ResolvedMoves[0].ChildPhase = phase
		candidate.Revision++
		candidate.UpdatedAtMs++
		if err := core.ValidatePlanUpdate(record.Plan, candidate); !errors.Is(err, core.ErrInvalidTransition) {
			t.Fatalf("same-phase terminal %s accepted: %v", phase, err)
		}
	}
	pending := record.Plan.Clone()
	pending.ResolvedMoves[0].EnsureObserved = true
	pending.ResolvedMoves[0].ChildPhase = core.RoutePlanPhaseDraining
	pending.Revision++
	pending.UpdatedAtMs++
	if err := core.ValidatePlanUpdate(record.Plan, pending); err != nil {
		t.Fatalf("legal nonterminal observation rejected: %v", err)
	}
}

func TestScalePlanCASRejectsForgedChildObservationTransitions(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	for record.Plan.Phase != core.PhaseMovingWave {
		record, _ = f.executor.Step(context.Background(), record.Plan.PlanID)
	}
	capture, _ := f.placements.CaptureShard(context.Background(), f.submission.Domain, 7)
	target, found, _ := f.holders.GetHolder(context.Background(), record.Plan.Target)
	if !found {
		t.Fatal("target missing")
	}
	move, err := core.ResolveChildMove(record.Plan, 0, capture.Placement, target)
	if err != nil {
		t.Fatal(err)
	}
	forgedAppend := record.Plan.Clone()
	move.EnsureObserved, move.Verified, move.Reconciled, move.ChildPhase = true, true, true, core.RoutePlanPhasePaused
	forgedAppend.ResolvedMoves = append(forgedAppend.ResolvedMoves, move)
	forgedAppend.Revision++
	forgedAppend.UpdatedAtMs++
	if err := core.ValidatePlanUpdate(record.Plan, forgedAppend); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("forged appended outcome accepted: %v", err)
	}

	valid, err := f.executor.Step(context.Background(), record.Plan.PlanID)
	if err != nil || len(valid.Plan.ResolvedMoves) != 1 {
		t.Fatal("valid child append failed")
	}
	forgedVerify := valid.Plan.Clone()
	forgedVerify.Phase = core.PhaseVerifyingWave
	forgedVerify.ResolvedMoves[0].EnsureObserved = true
	forgedVerify.ResolvedMoves[0].Verified = true
	forgedVerify.ResolvedMoves[0].Reconciled = true
	forgedVerify.ResolvedMoves[0].ChildPhase = core.RoutePlanPhasePaused
	forgedVerify.Revision++
	forgedVerify.UpdatedAtMs++
	if err := core.ValidatePlanUpdate(valid.Plan, forgedVerify); !errors.Is(err, core.ErrInvalidTransition) {
		t.Fatalf("forged verification edge accepted: %v", err)
	}
}

func TestScalePlanModelRejectsUnknownEffectAndResourceVersionDrift(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	record, _ = f.executor.Step(context.Background(), record.Plan.PlanID)
	unknown := record.Plan.Clone()
	unknown.ScaleIntent.Effect = core.EffectState("UNKNOWN")
	unknown.Revision++
	unknown.UpdatedAtMs++
	if err := core.ValidatePlanUpdate(record.Plan, unknown); !errors.Is(err, core.ErrInvalidPlan) {
		t.Fatalf("unknown effect accepted: %v", err)
	}
	drift := record.Plan.Clone()
	drift.ScaleIntent.ExpectedResourceVersion = "different-rv"
	drift.Revision++
	drift.UpdatedAtMs++
	if err := core.ValidatePlanUpdate(record.Plan, drift); !errors.Is(err, core.ErrInvalidPlan) {
		t.Fatalf("resource-version drift accepted: %v", err)
	}
}

func TestScalePlanIdentityIsDeterministicAndPayloadBound(t *testing.T) {
	f := newScaleOutFixture(t)
	digest1, err := core.SubmissionPayloadDigest(f.submission)
	if err != nil {
		t.Fatal(err)
	}
	id1, err := core.DeterministicPlanID(f.submission)
	if err != nil {
		t.Fatal(err)
	}
	reordered := f.submission.Clone()
	reordered.Moves = append([]core.ShardMoveIntent(nil), reordered.Moves...)
	digest2, _ := core.SubmissionPayloadDigest(reordered)
	id2, _ := core.DeterministicPlanID(reordered)
	if digest1 != digest2 || id1 != id2 || len(digest1) != 64 || len(id1) > core.MaxOpaqueIDBytes {
		t.Fatalf("identity unstable digest=%q/%q id=%q/%q", digest1, digest2, id1, id2)
	}
	changed := f.submission.Clone()
	changed.RoutePlanTTLms += time.Second.Milliseconds()
	changedDigest, _ := core.SubmissionPayloadDigest(changed)
	if changedDigest == digest1 {
		t.Fatal("payload change did not change digest")
	}
	const wantDigest = "7d08f65c8ba340f0bf2fb0ee6ceb17f52b97e57b0bbb1301bf1f9bf65e4723d0"
	const wantID = "sp-v2-6f49f6904651d871a83037bd451164098b731d4a1fe2ae9a13c89b121c9996d3"
	if digest1 != wantDigest || id1 != wantID {
		t.Fatalf("freeze golden vectors: digest=%s id=%s", digest1, id1)
	}
}

func TestScalePlanChildIdentityGoldenVector(t *testing.T) {
	f := newScaleOutFixture(t)
	plan, err := core.BuildInitialPlan(f.submission, f.now)
	if err != nil {
		t.Fatal(err)
	}
	capture, err := f.placements.CaptureShard(context.Background(), f.submission.Domain, 7)
	placement, found := capture.Placement, capture.Found
	if err != nil || !found {
		t.Fatal("placement missing")
	}
	plan.WorkloadIdentity = core.WorkloadIdentity{UID: "workload-player-v1", ResourceVersion: "100"}
	target := holder(f.now, "workload-player-v1", "player-svr-1", "boot-1", 1, core.HolderReadyZeroShard, 0)
	resolved, err := core.ResolveChildMove(plan, 0, placement, target)
	if err != nil {
		t.Fatal(err)
	}
	const wantChildID = "sp-rp-v2-4d6f615efb27867cd2086fd7a84bdc6bda1a5a9505484866e82b5252f5f801ba"
	const wantTransitionID = "sp-tr-v2-3e634eb463bcb72b26190284c1d8ad6aa9c00612b0300cd9d06d76c5e13fe6f7"
	const wantPayload = "57de79dc135022d5b1195251ef7533a34c52da82b4b5faa8af9ea69d3ed0a259"
	if resolved.ChildPlanID != wantChildID || resolved.ChildTransitionID != wantTransitionID || resolved.ChildPayloadDigest != wantPayload {
		t.Fatalf("freeze child vectors: id=%s transition=%s payload=%s", resolved.ChildPlanID, resolved.ChildTransitionID, resolved.ChildPayloadDigest)
	}
}

func TestScalePlanRejectsRecoveryHorizonBeyondThirtySevenDays(t *testing.T) {
	f := newScaleOutFixture(t)
	f.submission.PlanTTLms = core.MaxPlanTTL.Milliseconds()
	f.submission.RecoveryRetentionMs = core.MaxRecoveryRetention.Milliseconds()
	if _, err := f.executor.Submit(context.Background(), f.submission); err != nil {
		t.Fatalf("maximum 37-day horizon rejected: %v", err)
	}
	f.submission.RecoveryRetentionMs++
	if _, err := f.executor.Submit(context.Background(), f.submission); !errors.Is(err, core.ErrInvalidPlan) {
		t.Fatalf("overlong recovery horizon err=%v", err)
	}
}

func TestBlockedCheckpointDigestBindsEveryMutableCheckpointField(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	record, _ = f.executor.Step(context.Background(), record.Plan.PlanID)
	f.clock.Advance(26 * time.Hour)
	blocked, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) {
		t.Fatal(err)
	}
	baseline, err := core.BlockedCheckpointDigest(blocked.Plan)
	if err != nil {
		t.Fatal(err)
	}
	mutations := []func(*core.Plan){
		func(plan *core.Plan) { plan.BlockedReason += "-other" },
		func(plan *core.Plan) { plan.Revision++ },
		func(plan *core.Plan) { plan.Attempt++ },
		func(plan *core.Plan) { plan.UpdatedAtMs++ },
		func(plan *core.Plan) { plan.ScaleIntent.ExpectedResourceVersion += "-other" },
	}
	for index, mutate := range mutations {
		candidate := blocked.Plan.Clone()
		mutate(&candidate)
		got, digestErr := core.BlockedCheckpointDigest(candidate)
		if digestErr == nil && got == baseline {
			t.Fatalf("mutation %d retained checkpoint digest", index)
		}
	}
}

func TestScalePlanHolderRefExactUsesFrozenVisibleASCIIGrammar(t *testing.T) {
	valid := core.HolderRef{Domain: actorrouting.DomainPlayer, HolderID: "holder-a", BootID: "boot-a", PodUID: "pod-a", WorkloadUID: "workload-a", Ordinal: 0}
	if err := valid.ValidateExact(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*core.HolderRef){
		"pod whitespace":   func(ref *core.HolderRef) { ref.PodUID = "pod uid" },
		"workload unicode": func(ref *core.HolderRef) { ref.WorkloadUID = "workload-雪" },
		"pod oversized":    func(ref *core.HolderRef) { ref.PodUID = strings.Repeat("p", core.MaxOpaqueIDBytes+1) },
		"boot control":     func(ref *core.HolderRef) { ref.BootID = "boot\n" },
	} {
		t.Run(name, func(t *testing.T) {
			ref := valid
			mutate(&ref)
			if ref.Exact() || !errors.Is(ref.ValidateExact(), core.ErrInvalidPlan) {
				t.Fatalf("invalid ref accepted: %+v", ref)
			}
		})
	}
}

func TestScalePlanExpiredPlanFailsBeforeExternalEffect(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	f.clock.Advance(2 * time.Hour)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrConflict) || result.Plan.Status != core.StatusFailed || result.Plan.ErrorCode != "START_DEADLINE_EXPIRED" {
		t.Fatalf("result=%s code=%s err=%v", result.Plan.Status, result.Plan.ErrorCode, err)
	}
	if f.workloads.Calls() != 0 {
		t.Fatalf("external effect after expiry=%d", f.workloads.Calls())
	}
}

func TestScalePlanManifestNormalizationAndFunctionalLimits(t *testing.T) {
	f := newScaleOutFixture(t)
	bad := f.submission.Clone()
	bad.Moves = append(bad.Moves, bad.Moves[0])
	if _, err := bad.Normalize(); !errors.Is(err, core.ErrInvalidPlan) {
		t.Fatalf("duplicate manifest=%v", err)
	}
	bad = f.submission.Clone()
	bad.Limits.MaxShardsPerWave = 2
	if _, err := bad.Normalize(); !errors.Is(err, core.ErrInvalidPlan) {
		t.Fatalf("nonfunctional wave=%v", err)
	}
}
