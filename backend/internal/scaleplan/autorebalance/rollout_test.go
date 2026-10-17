package autorebalance

import (
	"errors"
	"testing"
	"time"
)

const testRolloutDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func validTestRollout(phase RolloutPhase, replicas int32) RolloutState {
	return RolloutState{
		SchemaVersion: RolloutSchemaVersion, RolloutID: "rollout-a", ReleaseID: "release-a",
		WorkloadUID: "workload-a", SourceRevision: "revision-1", TargetRevision: "revision-2",
		TargetImageDigest: testRolloutDigest, ExpectedReplicas: replicas, OriginalReplicas: replicas,
		StartedAtMs: 1, DeadlineAtMs: 4_102_444_800_000,
		IntentHash: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Phase:      phase, VictimOrdinal: -1,
	}
}

func TestRolloutStateAcceptsOnlySafeReplacementSequence(t *testing.T) {
	state := validTestRollout(RolloutPhaseExpanding, 2)
	draining := clearVictim(state, RolloutPhaseDraining)
	if err := ValidateRolloutUpdate(state, draining); err != nil {
		t.Fatal(err)
	}
	bound := draining
	bound.VictimHolderID, bound.VictimPodUID, bound.VictimBootID = "farm-svr-1", "pod-old", "boot-old"
	bound.VictimOrdinal, bound.DrainPlanID = 1, "drain-plan-a"
	if err := ValidateRolloutUpdate(draining, bound); err != nil {
		t.Fatal(err)
	}
	deleting := bound
	deleting.Phase = RolloutPhaseDeleting
	deleting.ExclusionRevision = 2
	if err := ValidateRolloutUpdate(bound, deleting); err != nil {
		t.Fatal(err)
	}
	waiting := deleting
	waiting.Phase = RolloutPhaseWaitingReplacement
	if err := ValidateRolloutUpdate(deleting, waiting); err != nil {
		t.Fatal(err)
	}
	nextDrain := clearVictim(waiting, RolloutPhaseDraining)
	if err := ValidateRolloutUpdate(waiting, nextDrain); err != nil {
		t.Fatal(err)
	}
	contracting := clearVictim(nextDrain, RolloutPhaseContracting)
	if err := ValidateRolloutUpdate(nextDrain, contracting); err != nil {
		t.Fatal(err)
	}
}

func TestRolloutStateRejectsSkippingDrainAndMutatingVictim(t *testing.T) {
	state := validTestRollout(RolloutPhaseExpanding, 2)
	illegal := state
	illegal.Phase = RolloutPhaseDeleting
	illegal.VictimHolderID, illegal.VictimPodUID, illegal.VictimBootID = "farm-svr-1", "pod-old", "boot-old"
	illegal.VictimOrdinal, illegal.DrainPlanID = 1, "drain-plan-a"
	if err := ValidateRolloutUpdate(state, illegal); !errors.Is(err, ErrInvalidTopology) {
		t.Fatalf("unsafe phase jump err=%v", err)
	}

	draining := clearVictim(state, RolloutPhaseDraining)
	bound := draining
	bound.VictimHolderID, bound.VictimPodUID, bound.VictimBootID = "farm-svr-1", "pod-old", "boot-old"
	bound.VictimOrdinal, bound.DrainPlanID = 1, "drain-plan-a"
	deleting := bound
	deleting.Phase = RolloutPhaseDeleting
	deleting.ExclusionRevision = 2
	deleting.VictimPodUID = "pod-forged"
	if err := ValidateRolloutUpdate(bound, deleting); !errors.Is(err, ErrInvalidTopology) {
		t.Fatalf("victim rewrite err=%v", err)
	}
}

func TestRolloutStateMayBlockWithoutChangingCheckpoint(t *testing.T) {
	state := validTestRollout(RolloutPhaseDraining, 2)
	state.VictimOrdinal = 1
	state.VictimHolderID, state.VictimPodUID, state.VictimBootID, state.DrainPlanID = "farm-svr-1", "pod-old", "boot-old", "plan-a"
	blocked := blockRollout(state, "TARGET_REVISION_CHANGED")
	if err := ValidateRolloutUpdate(state, blocked); err != nil {
		t.Fatal(err)
	}
	blocked.VictimPodUID = "pod-forged"
	if err := ValidateRolloutUpdate(state, blocked); !errors.Is(err, ErrInvalidTopology) {
		t.Fatalf("blocked checkpoint rewrite err=%v", err)
	}
}

func TestBlockedRolloutRequiresExplicitRecoveryTransition(t *testing.T) {
	state := validTestRollout(RolloutPhaseDraining, 2)
	blocked := blockRollout(state, "TARGET_REVISION_CHANGED")
	resumed, err := resumeBlockedRollout(blocked)
	if err != nil {
		t.Fatal(err)
	}
	if resumed != state {
		t.Fatalf("resumed=%+v want=%+v", resumed, state)
	}
	if err := ValidateRolloutUpdate(blocked, resumed); !errors.Is(err, ErrInvalidTopology) {
		t.Fatalf("ordinary update escaped BLOCKED: %v", err)
	}
	if err := ValidateRolloutRecoveryUpdate(blocked, resumed, RolloutRecoveryResume); err != nil {
		t.Fatal(err)
	}
}

func TestBlockedRolloutRecoveryMayExtendExpiredDeadlineOnlyExplicitly(t *testing.T) {
	blocked := validTestRollout(RolloutPhaseDraining, 2)
	blocked.DeadlineAtMs = time.Now().Add(-time.Minute).UnixMilli()
	blocked.VictimOrdinal = 1
	blocked.VictimHolderID, blocked.VictimPodUID, blocked.VictimBootID = "farm-svr-1", "pod-old", "boot-old"
	blocked.DrainPlanID = "drain-plan-a"
	blocked = blockRollout(blocked, "ROLLOUT_DEADLINE_EXCEEDED")
	deadline := time.Now().Add(time.Hour).UnixMilli()
	resumed, err := ResumeBlockedRolloutWithDeadline(blocked, deadline)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Phase != RolloutPhaseDraining || resumed.DeadlineAtMs != deadline || resumed.VictimPodUID != blocked.VictimPodUID {
		t.Fatalf("resumed=%+v", resumed)
	}
	if _, err := ResumeBlockedRolloutWithDeadline(blocked, time.Now().Add(-time.Second).UnixMilli()); !errors.Is(err, ErrInvalidTopology) {
		t.Fatalf("expired recovery deadline err=%v", err)
	}
	if _, err := ResumeBlockedRolloutWithDeadline(blocked, blocked.DeadlineAtMs-1); !errors.Is(err, ErrInvalidTopology) {
		t.Fatalf("shortened recovery deadline err=%v", err)
	}
}

func TestBlockedRolloutMayAbortOnlyBeforeVictimDrain(t *testing.T) {
	blocked := blockRollout(validTestRollout(RolloutPhaseExpanding, 2), "EXPANSION_REPLICA_DRIFT")
	aborting, err := abortBlockedRollout(blocked)
	if err != nil {
		t.Fatal(err)
	}
	if aborting.Phase != RolloutPhaseAborting {
		t.Fatalf("phase=%s", aborting.Phase)
	}
	if err := ValidateRolloutRecoveryUpdate(blocked, aborting, RolloutRecoveryAbort); err != nil {
		t.Fatal(err)
	}

	draining := validTestRollout(RolloutPhaseDraining, 2)
	draining.VictimHolderID, draining.VictimPodUID, draining.VictimBootID = "farm-svr-1", "pod-old", "boot-old"
	draining.VictimOrdinal, draining.DrainPlanID = 1, "plan-a"
	blockedAfterDrainStarted := blockRollout(draining, "DRAIN_PLAN_FAILED")
	if _, err := abortBlockedRollout(blockedAfterDrainStarted); !errors.Is(err, ErrInvalidTopology) {
		t.Fatalf("partially progressed rollout aborted: %v", err)
	}
}
