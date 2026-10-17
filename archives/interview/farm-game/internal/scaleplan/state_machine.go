package scaleplan

import (
	"fmt"
	"reflect"
	"strings"
)

func ValidateInitialPlan(plan Plan) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	if plan.Status != StatusRunning || plan.Phase != PhasePending || plan.WaveIndex != 0 ||
		plan.Attempt != 0 || plan.Revision != 1 || plan.CompletedAtMs != 0 ||
		plan.WorkloadIdentity != (WorkloadIdentity{}) || plan.Target != (HolderRef{}) ||
		plan.Victim != (HolderRef{}) || plan.ScaleIntent != nil || plan.ExclusionIntent != nil || plan.CleanupIntent != nil || plan.BlockedTermination != nil || plan.BlockedTerminationProof != nil ||
		len(plan.ResolvedMoves) != 0 || plan.PausedReason != "" || plan.BlockedReason != "" || plan.ErrorCode != "" {
		return fmt.Errorf("%w: initial audit state", ErrInvalidTransition)
	}
	return nil
}

// ValidatePlanUpdate freezes the submission and every already-observed
// external identity while permitting one monotonic state-machine/CAS step.
func ValidateBlockedTerminationUpdate(current, candidate Plan, expectation BlockedTerminationRequest, proof BlockedQuiescenceProof, receipt BlockedTerminationReceipt) error {
	if err := current.Validate(); err != nil {
		return fmt.Errorf("%w: current: %v", ErrCorruptRecord, err)
	}
	if current.Status != StatusBlocked || current.BlockedTermination != nil || current.PlanID != expectation.PlanID ||
		current.Revision != expectation.ExpectedRevision || current.BlockedReason != expectation.ExpectedBlockedReason ||
		current.PayloadDigest != expectation.ExpectedPayloadDigest || receipt.TerminationID != expectation.TerminationID ||
		receipt.ExpectedCheckpointDigest != expectation.ExpectedCheckpointDigest || receipt.ExpectedBlockedRevision != current.Revision ||
		receipt.ExpectedBlockedReason != current.BlockedReason || receipt.ExpectedBlockedUpdatedAtMs != current.UpdatedAtMs || receipt.ExpectedPayloadDigest != current.PayloadDigest ||
		proof.PlanID != current.PlanID || proof.ExpectedRevision != current.Revision || proof.ExpectedBlockedReason != current.BlockedReason ||
		proof.ExpectedPayloadDigest != current.PayloadDigest || proof.ExpectedCheckpointDigest != expectation.ExpectedCheckpointDigest ||
		proof.IntentCount != blockedTerminationIntentCount(current) || proof.ChildCount != uint32(len(current.ResolvedMoves)) {
		return fmt.Errorf("%w: blocked termination expectation", ErrRevisionConflict)
	}
	digest, err := BlockedCheckpointDigest(current)
	proofDigest, proofErr := BlockedQuiescenceProofDigest(proof)
	if err != nil || proofErr != nil || digest != expectation.ExpectedCheckpointDigest || proofDigest != receipt.ProofDigest {
		return fmt.Errorf("%w: blocked checkpoint digest", ErrRevisionConflict)
	}
	if candidate.Status != StatusFailed || candidate.ErrorCode != "OPERATOR_SAFE_TERMINATION" || candidate.BlockedReason != "" ||
		candidate.PausedReason != "" || candidate.BlockedTermination == nil || *candidate.BlockedTermination != receipt ||
		candidate.BlockedTerminationProof == nil || *candidate.BlockedTerminationProof != proof ||
		candidate.Revision != current.Revision+1 || candidate.CompletedAtMs != candidate.UpdatedAtMs ||
		candidate.CompletedAtMs != receipt.CommittedAtMs || receipt.EvidenceRetainUntilMs != proof.EvidenceRetainUntilMs ||
		receipt.ProvedAtMs != proof.CapturedAtMs || receipt.ProofExpiresAtMs != proof.ExpiresAtMs || !factsEqual(current, candidate) ||
		current.Phase != candidate.Phase || current.WaveIndex != candidate.WaveIndex || current.Attempt != candidate.Attempt {
		return fmt.Errorf("%w: blocked termination delta", ErrInvalidTransition)
	}
	return candidate.Validate()
}

func ValidatePlanUpdate(current, candidate Plan) error {
	if err := current.Validate(); err != nil {
		return fmt.Errorf("%w: current: %v", ErrCorruptRecord, err)
	}
	if err := candidate.Validate(); err != nil {
		return err
	}
	if candidate.BlockedTermination != nil || candidate.BlockedTerminationProof != nil || candidate.ErrorCode == "OPERATOR_SAFE_TERMINATION" {
		return fmt.Errorf("%w: blocked termination requires proof-owned repository commit", ErrInvalidTransition)
	}
	if current.PlanID != candidate.PlanID || current.IdempotencyKey != candidate.IdempotencyKey ||
		current.PayloadDigest != candidate.PayloadDigest || !reflect.DeepEqual(current.Submission, candidate.Submission) ||
		current.SchemaVersion != candidate.SchemaVersion || current.CreatedAtMs != candidate.CreatedAtMs ||
		current.StartDeadlineMs != candidate.StartDeadlineMs || current.RecoveryRetainUntilMs != candidate.RecoveryRetainUntilMs {
		return fmt.Errorf("%w: immutable plan fields changed", ErrRevisionConflict)
	}
	if current.Status.Terminal() {
		return fmt.Errorf("%w: terminal plan is immutable", ErrRevisionConflict)
	}
	if candidate.Revision != current.Revision+1 || candidate.UpdatedAtMs < current.UpdatedAtMs {
		return fmt.Errorf("%w: revision/audit progression", ErrRevisionConflict)
	}
	if candidate.Attempt != current.Attempt && candidate.Attempt != current.Attempt+1 {
		return fmt.Errorf("%w: attempt progression", ErrRevisionConflict)
	}
	if !validStatusAdvance(current.Status, candidate.Status) {
		return fmt.Errorf("%w: status %s -> %s", ErrInvalidTransition, current.Status, candidate.Status)
	}
	if current.Status == candidate.Status {
		if current.Status == StatusPaused && current.PausedReason != candidate.PausedReason {
			return fmt.Errorf("%w: paused reason changed without resume", ErrInvalidTransition)
		}
		blockedExpired := current.Status == StatusBlocked && current.BlockedReason == "CHILD_ROUTEPLAN_PAUSED" &&
			candidate.BlockedReason == "RECOVERY_RETENTION_EXCEEDED"
		if current.Status == StatusBlocked && current.BlockedReason != candidate.BlockedReason && !blockedExpired {
			return fmt.Errorf("%w: blocked reason changed without reconciliation", ErrInvalidTransition)
		}
	}
	if err := validateExactPlanDelta(current, candidate); err != nil {
		return err
	}
	// Ordinary status changes preserve the checkpoint. A controlled blocked
	// reconciliation may atomically adopt verified facts and advance one legal
	// progress edge while returning to RUNNING.
	if current.Status != candidate.Status {
		blockedRecovery := current.Status == StatusBlocked && candidate.Status == StatusRunning && canAdvanceProgress(current, candidate)
		if !blockedRecovery && (current.Phase != candidate.Phase || current.WaveIndex != candidate.WaveIndex) {
			return fmt.Errorf("%w: status update changed progress", ErrInvalidTransition)
		}
	} else if (current.Phase != candidate.Phase || current.WaveIndex != candidate.WaveIndex) &&
		(candidate.Status != StatusRunning || !canAdvanceProgress(current, candidate)) {
		return fmt.Errorf("%w: status %s cannot progress %s/%d -> %s/%d", ErrInvalidTransition,
			candidate.Status, current.Phase, current.WaveIndex, candidate.Phase, candidate.WaveIndex)
	}
	if current.WorkloadIdentity != (WorkloadIdentity{}) && candidate.WorkloadIdentity != current.WorkloadIdentity {
		return fmt.Errorf("%w: workload identity changed", ErrConflict)
	}
	if current.Target != (HolderRef{}) && candidate.Target != current.Target {
		return fmt.Errorf("%w: target identity changed", ErrConflict)
	}
	if current.Victim != (HolderRef{}) && candidate.Victim != current.Victim {
		return fmt.Errorf("%w: victim identity changed", ErrConflict)
	}
	if err := validateScaleIntentAdvance(current.ScaleIntent, candidate.ScaleIntent); err != nil {
		return err
	}
	if err := validateExclusionIntentAdvance(current.ExclusionIntent, candidate.ExclusionIntent); err != nil {
		return err
	}
	if err := validateCleanupIntentAdvance(current.CleanupIntent, candidate.CleanupIntent); err != nil {
		return err
	}
	if err := validateMovesAdvance(current.ResolvedMoves, candidate.ResolvedMoves); err != nil {
		return err
	}
	return nil
}

type mutablePlanFacts struct {
	WorkloadIdentity WorkloadIdentity
	Target           HolderRef
	Victim           HolderRef
	ScaleIntent      *ScaleIntent
	ExclusionIntent  *ExclusionIntent
	CleanupIntent    *CleanupIntent
	ResolvedMoves    []ResolvedMove
}

func factsOf(plan Plan) mutablePlanFacts {
	facts := mutablePlanFacts{WorkloadIdentity: plan.WorkloadIdentity, Target: plan.Target, Victim: plan.Victim,
		ScaleIntent: plan.ScaleIntent, ExclusionIntent: plan.ExclusionIntent, CleanupIntent: plan.CleanupIntent,
		ResolvedMoves: plan.ResolvedMoves}
	return facts
}

func factsEqual(left, right Plan) bool { return reflect.DeepEqual(factsOf(left), factsOf(right)) }

// validateExactPlanDelta binds every first durable fact to one state-machine
// edge. Same-phase CAS is not a general mutation escape hatch.
func validateExactPlanDelta(current, candidate Plan) error {
	if current.Status != candidate.Status {
		if current.Status == StatusBlocked && candidate.Status == StatusRunning {
			if current.BlockedReason == "CHILD_ROUTEPLAN_PAUSED" &&
				((current.Phase == PhaseMovingWave &&
					(candidate.Phase == PhaseMovingWave || candidate.Phase == PhaseVerifyingWave)) ||
					(current.Phase == PhaseVerifyingWave && candidate.Phase == PhaseVerifyingWave)) {
				return validateBlockedChildReconcile(current, candidate)
			}
			if blockedCaptureReason(current.BlockedReason) && current.Phase == PhaseCapturingVictimShards && factsEqual(current, candidate) {
				expectedPhase := PhaseMovingWave
				if len(current.Submission.Moves) == 0 {
					expectedPhase = PhaseWaitingZeroShard
				}
				if candidate.Phase == expectedPhase {
					return nil
				}
			}
			if blockedExclusionReleaseReason(current.BlockedReason) && current.Phase == PhaseReleasingExclusion &&
				candidate.Phase == PhaseReleasingExclusion && factsEqual(current, candidate) {
				return nil
			}
			if current.BlockedReason == "CHILD_VERIFICATION_NON_EXACT" &&
				current.Phase == PhaseVerifyingWave && candidate.Phase == PhaseVerifyingWave &&
				factsEqual(current, candidate) {
				return nil
			}
			return fmt.Errorf("%w: invalid blocked reconciliation delta", ErrInvalidTransition)
		}
		if !factsEqual(current, candidate) {
			return fmt.Errorf("%w: status-only update changed durable facts", ErrInvalidTransition)
		}
		return nil
	}
	if current.Status != StatusRunning && !factsEqual(current, candidate) {
		return fmt.Errorf("%w: non-running status changed durable facts", ErrInvalidTransition)
	}
	if current.Phase == candidate.Phase {
		if factsEqual(current, candidate) {
			return nil
		}
		if current.Phase == PhaseReleasingExclusion {
			return validateCleanupPhaseUpdate(current, candidate)
		}
		if current.Phase == PhaseMovingWave {
			return validateMovingWaveCheckpoint(current, candidate)
		}
		if current.Phase == PhaseVerifyingWave {
			return validateVerifyingWaveCheckpoint(current, candidate)
		}
		if !factsEqual(current, candidate) {
			return fmt.Errorf("%w: same-phase update changed durable facts", ErrInvalidTransition)
		}
		return nil
	}

	switch current.Submission.Operation {
	case OperationScaleOut:
		switch {
		case current.Phase == PhasePending && candidate.Phase == PhaseScalingUp:
			if current.HasExternalIntent() || current.WorkloadIdentity != (WorkloadIdentity{}) ||
				candidate.ScaleIntent == nil || candidate.ScaleIntent.Effect != EffectPending ||
				candidate.WorkloadIdentity == (WorkloadIdentity{}) || candidate.Target != (HolderRef{}) ||
				candidate.Victim != (HolderRef{}) || candidate.ExclusionIntent != nil || len(candidate.ResolvedMoves) != 0 {
				return fmt.Errorf("%w: invalid scale-out write-ahead delta", ErrInvalidTransition)
			}
			return nil
		case current.Phase == PhaseScalingUp && candidate.Phase == PhaseWaitingHolderReady:
			return validateOnlyScaleEffectChanged(current, candidate)
		case current.Phase == PhaseWaitingHolderReady && candidate.Phase == PhaseMovingWave:
			left, right := factsOf(current), factsOf(candidate)
			left.Target, right.Target = HolderRef{}, HolderRef{}
			if !reflect.DeepEqual(left, right) || current.Target != (HolderRef{}) || !candidate.Target.Exact() {
				return fmt.Errorf("%w: invalid target freeze delta", ErrInvalidTransition)
			}
			return nil
		case current.Phase == PhaseMovingWave && candidate.Phase == PhaseVerifyingWave:
			return validateMoveToVerify(current, candidate)
		case current.Phase == PhaseVerifyingWave && (candidate.Phase == PhaseMovingWave || candidate.Phase == PhaseVerifyingFinal):
			return validateVerifiedChildAdvance(current, candidate)
		case current.Phase == PhaseVerifyingFinal && candidate.Phase == PhaseVerifyingFinal:
			return nil
		}
	case OperationScaleIn, OperationDrainHolder:
		switch {
		case candidate.Phase == PhaseReleasingExclusion && current.CleanupIntent == nil:
			return validateEnteringCleanup(current, candidate)
		case current.Phase == PhasePending && candidate.Phase == PhaseSelectingVictim:
			if !factsEqual(current, candidate) {
				return fmt.Errorf("%w: victim selection phase introduced facts early", ErrInvalidTransition)
			}
			return nil
		case current.Phase == PhaseSelectingVictim && candidate.Phase == PhaseExcludingVictim:
			expectedTarget := HolderRef{}
			if current.Submission.Operation == OperationDrainHolder {
				expectedTarget = current.Submission.PinnedTarget
			}
			if current.HasExternalIntent() || current.WorkloadIdentity != (WorkloadIdentity{}) || current.Victim != (HolderRef{}) ||
				candidate.WorkloadIdentity == (WorkloadIdentity{}) || !candidate.Victim.Exact() || candidate.ExclusionIntent == nil ||
				candidate.ExclusionIntent.Effect != EffectPending || candidate.ScaleIntent != nil ||
				candidate.Target != expectedTarget || len(candidate.ResolvedMoves) != 0 {
				return fmt.Errorf("%w: invalid exclusion write-ahead delta", ErrInvalidTransition)
			}
			return nil
		case current.Phase == PhaseExcludingVictim && candidate.Phase == PhaseCapturingVictimShards:
			return validateOnlyExclusionEffectChanged(current, candidate)
		case current.Phase == PhaseCapturingVictimShards &&
			(candidate.Phase == PhaseMovingWave || candidate.Phase == PhaseWaitingZeroShard):
			if !factsEqual(current, candidate) {
				return fmt.Errorf("%w: capture completion changed durable facts", ErrInvalidTransition)
			}
			return nil
		case current.Phase == PhaseMovingWave && candidate.Phase == PhaseVerifyingWave:
			return validateMoveToVerify(current, candidate)
		case current.Phase == PhaseVerifyingWave && (candidate.Phase == PhaseMovingWave || candidate.Phase == PhaseWaitingZeroShard):
			return validateVerifiedChildAdvance(current, candidate)
		case current.Phase == PhaseWaitingZeroShard && candidate.Phase == PhaseScalingDown:
			if current.Submission.Operation != OperationScaleIn {
				break
			}
			left, right := factsOf(current), factsOf(candidate)
			left.ScaleIntent, right.ScaleIntent = nil, nil
			if !reflect.DeepEqual(left, right) || current.ScaleIntent != nil || candidate.ScaleIntent == nil || candidate.ScaleIntent.Effect != EffectPending {
				return fmt.Errorf("%w: invalid scale-down write-ahead delta", ErrInvalidTransition)
			}
			return nil
		case current.Phase == PhaseScalingDown && candidate.Phase == PhaseVerifyingFinal:
			return validateOnlyScaleEffectChanged(current, candidate)
		case current.Submission.Operation == OperationDrainHolder &&
			current.Phase == PhaseWaitingZeroShard && candidate.Phase == PhaseVerifyingFinal:
			if !factsEqual(current, candidate) {
				return fmt.Errorf("%w: holder drain finalization changed durable facts", ErrInvalidTransition)
			}
			return nil
		}
	}
	if !factsEqual(current, candidate) {
		return fmt.Errorf("%w: unsupported phase edge changed durable facts", ErrInvalidTransition)
	}
	return nil
}

func blockedCaptureReason(reason string) bool {
	switch reason {
	case "PLACEMENT_EXCLUSION_NOT_VISIBLE", "SCALE_IN_PLACEMENT_STALE", "SCALE_IN_MANIFEST_DRIFT", "PLACEMENT_CAPTURE_CHANGED":
		return true
	default:
		return false
	}
}

func blockedExclusionReleaseReason(reason string) bool {
	return strings.HasPrefix(reason, "EXCLUSION_RELEASE_")
}

func validateChildOnlyFacts(current, candidate Plan) error {
	left, right := factsOf(current), factsOf(candidate)
	left.ResolvedMoves, right.ResolvedMoves = nil, nil
	if !reflect.DeepEqual(left, right) {
		return fmt.Errorf("%w: child edge changed non-child facts", ErrInvalidTransition)
	}
	return nil
}

func clearChildObservation(move ResolvedMove) ResolvedMove {
	move.ChildPhase = ""
	move.EnsureObserved = false
	move.Verified = false
	move.Reconciled = false
	return move
}

func childObservationEmpty(move ResolvedMove) bool {
	return move.ChildPhase == "" && !move.EnsureObserved && !move.Verified && !move.Reconciled
}

func validateMovingWaveCheckpoint(current, candidate Plan) error {
	if err := validateChildOnlyFacts(current, candidate); err != nil {
		return err
	}
	start, end := int(current.WaveIndex), waveEnd(current)
	if candidate.WaveIndex != current.WaveIndex || len(current.ResolvedMoves) < start || len(candidate.ResolvedMoves) < start ||
		len(candidate.ResolvedMoves) < len(current.ResolvedMoves) || len(candidate.ResolvedMoves) > end {
		return fmt.Errorf("%w: invalid moving-wave child count", ErrInvalidTransition)
	}
	for i := range current.ResolvedMoves {
		if current.ResolvedMoves[i].Intent != candidate.ResolvedMoves[i].Intent ||
			current.ResolvedMoves[i].Source != candidate.ResolvedMoves[i].Source ||
			current.ResolvedMoves[i].SourceEndpoint != candidate.ResolvedMoves[i].SourceEndpoint ||
			current.ResolvedMoves[i].Target != candidate.ResolvedMoves[i].Target ||
			current.ResolvedMoves[i].TargetEndpoint != candidate.ResolvedMoves[i].TargetEndpoint ||
			current.ResolvedMoves[i].ExpectedSourceFence != candidate.ResolvedMoves[i].ExpectedSourceFence ||
			current.ResolvedMoves[i].ChildPlanID != candidate.ResolvedMoves[i].ChildPlanID ||
			current.ResolvedMoves[i].ChildTransitionID != candidate.ResolvedMoves[i].ChildTransitionID ||
			current.ResolvedMoves[i].ChildPayloadDigest != candidate.ResolvedMoves[i].ChildPayloadDigest ||
			current.ResolvedMoves[i].WaveIndex != candidate.ResolvedMoves[i].WaveIndex {
			return fmt.Errorf("%w: child intent changed while moving wave", ErrConflict)
		}
		if i < start {
			if current.ResolvedMoves[i] != candidate.ResolvedMoves[i] {
				return fmt.Errorf("%w: completed child changed while moving wave", ErrInvalidTransition)
			}
			continue
		}
		if err := validateMovingChildObservation(current.ResolvedMoves[i], candidate.ResolvedMoves[i], current.Submission.Limits.Parallel()); err != nil {
			return err
		}
	}
	for i := len(current.ResolvedMoves); i < len(candidate.ResolvedMoves); i++ {
		if !candidate.ResolvedMoves[i].Frozen() || !childObservationEmpty(candidate.ResolvedMoves[i]) {
			return fmt.Errorf("%w: appended child contains observation facts", ErrInvalidTransition)
		}
	}
	return nil
}

func validateMovingChildObservation(before, after ResolvedMove, allowSucceeded bool) error {
	if clearChildObservation(before) != clearChildObservation(after) {
		return fmt.Errorf("%w: child intent changed while observing", ErrConflict)
	}
	if before == after {
		return nil
	}
	if allowSucceeded && !before.Reconciled && !before.Verified &&
		after.EnsureObserved && after.ChildPhase == RoutePlanPhaseSucceeded && after.Verified && !after.Reconciled {
		if before.ChildPhase != "" && before.ChildPhase != RoutePlanPhaseSucceeded {
			if _, valid := routePlanNonterminalRank(before.ChildPhase); !valid {
				return fmt.Errorf("%w: child observation regressed", ErrInvalidTransition)
			}
		}
		return nil
	}
	if before.Reconciled || before.Verified || after.Reconciled || after.Verified {
		return fmt.Errorf("%w: verified child changed while moving wave", ErrInvalidTransition)
	}
	if !after.EnsureObserved || !validRoutePlanObservedPhase(after.ChildPhase) || after.ChildPhase == RoutePlanPhasePaused ||
		after.ChildPhase == RoutePlanPhaseFailed {
		return fmt.Errorf("%w: invalid moving-wave observation", ErrInvalidTransition)
	}
	if after.ChildPhase == RoutePlanPhaseSucceeded {
		if !allowSucceeded {
			return fmt.Errorf("%w: serial moving wave cannot retain terminal observation", ErrInvalidTransition)
		}
		if before.ChildPhase != "" && before.ChildPhase != RoutePlanPhaseSucceeded {
			if _, valid := routePlanNonterminalRank(before.ChildPhase); !valid {
				return fmt.Errorf("%w: child observation regressed", ErrInvalidTransition)
			}
		}
		return nil
	}
	if !routePlanNonterminalCanAdvance(before.ChildPhase, after.ChildPhase) {
		return fmt.Errorf("%w: child observation regressed", ErrInvalidTransition)
	}
	return nil
}

func childReadyForWaveVerification(move ResolvedMove) bool {
	return move.EnsureObserved &&
		(move.ChildPhase == RoutePlanPhaseSucceeded ||
			(move.ChildPhase == RoutePlanPhasePaused && move.Verified && move.Reconciled))
}

func validateMoveToVerify(current, candidate Plan) error {
	if err := validateChildOnlyFacts(current, candidate); err != nil {
		return err
	}
	start, end := int(current.WaveIndex), waveEnd(current)
	if candidate.WaveIndex != current.WaveIndex || len(candidate.ResolvedMoves) != end || len(current.ResolvedMoves) > end || len(current.ResolvedMoves) < start {
		return fmt.Errorf("%w: invalid child count entering verification", ErrInvalidTransition)
	}
	for i := range current.ResolvedMoves {
		if current.ResolvedMoves[i].Intent != candidate.ResolvedMoves[i].Intent ||
			current.ResolvedMoves[i].Source != candidate.ResolvedMoves[i].Source ||
			current.ResolvedMoves[i].SourceEndpoint != candidate.ResolvedMoves[i].SourceEndpoint ||
			current.ResolvedMoves[i].Target != candidate.ResolvedMoves[i].Target ||
			current.ResolvedMoves[i].TargetEndpoint != candidate.ResolvedMoves[i].TargetEndpoint ||
			current.ResolvedMoves[i].ExpectedSourceFence != candidate.ResolvedMoves[i].ExpectedSourceFence ||
			current.ResolvedMoves[i].ChildPlanID != candidate.ResolvedMoves[i].ChildPlanID ||
			current.ResolvedMoves[i].ChildTransitionID != candidate.ResolvedMoves[i].ChildTransitionID ||
			current.ResolvedMoves[i].ChildPayloadDigest != candidate.ResolvedMoves[i].ChildPayloadDigest ||
			current.ResolvedMoves[i].WaveIndex != candidate.ResolvedMoves[i].WaveIndex {
			return fmt.Errorf("%w: child intent changed entering verification", ErrConflict)
		}
		if i < start {
			if current.ResolvedMoves[i] != candidate.ResolvedMoves[i] {
				return fmt.Errorf("%w: completed child changed entering verification", ErrInvalidTransition)
			}
			continue
		}
		if i < len(current.ResolvedMoves) {
			if err := validateMovingChildObservation(current.ResolvedMoves[i], candidate.ResolvedMoves[i], true); err != nil {
				// A paused child accepted by blocked reconciliation is already
				// verified and is the one terminal exception to the moving edge.
				if !(current.ResolvedMoves[i] == candidate.ResolvedMoves[i] && childReadyForWaveVerification(candidate.ResolvedMoves[i])) {
					return err
				}
			}
		}
	}
	for i := len(current.ResolvedMoves); i < len(candidate.ResolvedMoves); i++ {
		if !candidate.ResolvedMoves[i].Frozen() || !childObservationEmpty(candidate.ResolvedMoves[i]) {
			return fmt.Errorf("%w: appended child contains observation facts", ErrInvalidTransition)
		}
	}
	for i := start; i < end; i++ {
		if !childReadyForWaveVerification(candidate.ResolvedMoves[i]) {
			return fmt.Errorf("%w: wave child %d is not ready for verification", ErrInvalidTransition, i)
		}
	}
	return nil
}

func validateVerifyingWaveCheckpoint(current, candidate Plan) error {
	if err := validateChildOnlyFacts(current, candidate); err != nil {
		return err
	}
	start, end := int(current.WaveIndex), waveEnd(current)
	if candidate.WaveIndex != current.WaveIndex || len(current.ResolvedMoves) != end || len(candidate.ResolvedMoves) != end {
		return fmt.Errorf("%w: invalid verifying-wave child count", ErrInvalidTransition)
	}
	for i := range current.ResolvedMoves {
		if i < start {
			if current.ResolvedMoves[i] != candidate.ResolvedMoves[i] {
				return fmt.Errorf("%w: completed child changed while verifying wave", ErrInvalidTransition)
			}
			continue
		}
		before, after := current.ResolvedMoves[i], candidate.ResolvedMoves[i]
		if clearChildObservation(before) != clearChildObservation(after) {
			return fmt.Errorf("%w: child intent changed while verifying wave", ErrConflict)
		}
		if before == after {
			continue
		}
		if before.EnsureObserved && before.ChildPhase == RoutePlanPhaseSucceeded && !before.Verified &&
			after.EnsureObserved && after.ChildPhase == RoutePlanPhaseSucceeded && after.Verified && !after.Reconciled {
			continue
		}
		return fmt.Errorf("%w: invalid verification checkpoint", ErrInvalidTransition)
	}
	return nil
}

func validateVerifiedChildAdvance(current, candidate Plan) error {
	if err := validateChildOnlyFacts(current, candidate); err != nil {
		return err
	}
	start, end := int(current.WaveIndex), waveEnd(current)
	if len(current.ResolvedMoves) != end || len(candidate.ResolvedMoves) != end || start >= end {
		return fmt.Errorf("%w: invalid child count after verification", ErrInvalidTransition)
	}
	for i := range current.ResolvedMoves {
		before, after := current.ResolvedMoves[i], candidate.ResolvedMoves[i]
		if i < start {
			if before != after {
				return fmt.Errorf("%w: completed child changed after verification", ErrInvalidTransition)
			}
			continue
		}
		if clearChildObservation(before) != clearChildObservation(after) {
			return fmt.Errorf("%w: child intent changed after verification", ErrConflict)
		}
		if before == after {
			if !before.Verified || !childReadyForWaveVerification(before) {
				return fmt.Errorf("%w: unverified child retained after verification", ErrInvalidTransition)
			}
			continue
		}
		if before.EnsureObserved && before.ChildPhase == RoutePlanPhaseSucceeded && !before.Verified &&
			after.EnsureObserved && after.ChildPhase == RoutePlanPhaseSucceeded && after.Verified && !after.Reconciled {
			continue
		}
		return fmt.Errorf("%w: invalid successful verification delta", ErrInvalidTransition)
	}
	for i := start; i < end; i++ {
		if !candidate.ResolvedMoves[i].Verified {
			return fmt.Errorf("%w: wave child %d was not verified", ErrInvalidTransition, i)
		}
	}
	if candidate.Phase == PhaseMovingWave {
		if candidate.WaveIndex != uint32(end) {
			return fmt.Errorf("%w: wave index did not advance by completed wave length", ErrInvalidTransition)
		}
		return nil
	}
	if candidate.Phase != PhaseVerifyingFinal && candidate.Phase != PhaseWaitingZeroShard {
		return fmt.Errorf("%w: invalid post-wave phase", ErrInvalidTransition)
	}
	if candidate.Submission.Limits.LegacySerial() {
		if candidate.WaveIndex != uint32(end-1) && candidate.WaveIndex != uint32(end) {
			return fmt.Errorf("%w: legacy final wave index", ErrInvalidTransition)
		}
	} else if candidate.WaveIndex != uint32(end) {
		return fmt.Errorf("%w: final wave index did not advance by completed wave length", ErrInvalidTransition)
	}
	return nil
}

func validateBlockedChildReconcile(current, candidate Plan) error {
	if err := validateChildOnlyFacts(current, candidate); err != nil {
		return err
	}
	start, end := int(current.WaveIndex), waveEnd(current)
	if len(current.ResolvedMoves) != len(candidate.ResolvedMoves) || len(current.ResolvedMoves) < start || len(current.ResolvedMoves) > end {
		return fmt.Errorf("%w: invalid blocked child count", ErrInvalidTransition)
	}
	changed := 0
	for i := range current.ResolvedMoves {
		before, after := current.ResolvedMoves[i], candidate.ResolvedMoves[i]
		if before == after {
			continue
		}
		if i < start || clearChildObservation(before) != clearChildObservation(after) ||
			after.ChildPhase != RoutePlanPhasePaused || !after.EnsureObserved || !after.Verified || !after.Reconciled {
			return fmt.Errorf("%w: invalid blocked child reconciliation", ErrInvalidTransition)
		}
		changed++
	}
	if changed != 1 {
		return fmt.Errorf("%w: blocked reconciliation must advance one child", ErrInvalidTransition)
	}
	if candidate.Phase != PhaseMovingWave && candidate.Phase != PhaseVerifyingWave {
		return fmt.Errorf("%w: invalid blocked reconciliation phase", ErrInvalidTransition)
	}
	if candidate.Phase == PhaseVerifyingWave {
		if len(candidate.ResolvedMoves) != end {
			return fmt.Errorf("%w: blocked reconciliation entered verification early", ErrInvalidTransition)
		}
		for i := start; i < end; i++ {
			if !childReadyForWaveVerification(candidate.ResolvedMoves[i]) {
				return fmt.Errorf("%w: blocked reconciliation left wave child unresolved", ErrInvalidTransition)
			}
		}
	}
	return nil
}

func validateOnlyScaleEffectChanged(current, candidate Plan) error {
	left, right := factsOf(current), factsOf(candidate)
	left.ScaleIntent, right.ScaleIntent = nil, nil
	if !reflect.DeepEqual(left, right) || current.ScaleIntent == nil || candidate.ScaleIntent == nil ||
		current.ScaleIntent.Effect != EffectPending || candidate.ScaleIntent.Effect == EffectPending {
		return fmt.Errorf("%w: invalid scale effect delta", ErrInvalidTransition)
	}
	return nil
}

func validateOnlyExclusionEffectChanged(current, candidate Plan) error {
	left, right := factsOf(current), factsOf(candidate)
	left.ExclusionIntent, right.ExclusionIntent = nil, nil
	if !reflect.DeepEqual(left, right) || current.ExclusionIntent == nil || candidate.ExclusionIntent == nil ||
		current.ExclusionIntent.Effect != EffectPending || candidate.ExclusionIntent.Effect == EffectPending {
		return fmt.Errorf("%w: invalid exclusion effect delta", ErrInvalidTransition)
	}
	return nil
}

func validStatusAdvance(from, to ExecutionStatus) bool {
	if from == to {
		return true
	}
	switch from {
	case StatusRunning:
		return to == StatusPaused || to == StatusBlocked || to == StatusSucceeded || to == StatusFailed || to == StatusCancelled
	case StatusPaused:
		return to == StatusRunning || to == StatusBlocked || to == StatusFailed || to == StatusCancelled
	case StatusBlocked:
		return to == StatusRunning
	default:
		return false
	}
}

func canAdvanceProgress(current, candidate Plan) bool {
	if current.Phase == candidate.Phase && current.WaveIndex == candidate.WaveIndex {
		return true
	}
	if current.Submission.Operation != candidate.Submission.Operation {
		return false
	}
	switch current.Submission.Operation {
	case OperationScaleOut:
		switch current.Phase {
		case PhasePending:
			return candidate.Phase == PhaseScalingUp && candidate.WaveIndex == 0
		case PhaseScalingUp:
			return candidate.Phase == PhaseWaitingHolderReady && candidate.WaveIndex == 0
		case PhaseWaitingHolderReady:
			return candidate.Phase == PhaseMovingWave && candidate.WaveIndex == 0
		case PhaseMovingWave:
			return candidate.Phase == PhaseVerifyingWave && candidate.WaveIndex == current.WaveIndex
		case PhaseVerifyingWave:
			end := waveEnd(current)
			if candidate.Phase == PhaseMovingWave {
				return candidate.WaveIndex == uint32(end)
			}
			if candidate.Phase == PhaseVerifyingFinal {
				return candidate.WaveIndex == uint32(end) ||
					(current.Submission.Limits.LegacySerial() && candidate.WaveIndex == uint32(end-1))
			}
		}
	case OperationScaleIn, OperationDrainHolder:
		switch current.Phase {
		case PhasePending:
			return candidate.Phase == PhaseSelectingVictim && candidate.WaveIndex == 0
		case PhaseSelectingVictim:
			return candidate.Phase == PhaseExcludingVictim && candidate.WaveIndex == 0
		case PhaseExcludingVictim:
			return candidate.Phase == PhaseCapturingVictimShards && candidate.WaveIndex == 0
		case PhaseCapturingVictimShards:
			if candidate.WaveIndex != 0 {
				return false
			}
			if len(current.Submission.Moves) == 0 {
				return candidate.Phase == PhaseWaitingZeroShard
			}
			return candidate.Phase == PhaseMovingWave
		case PhaseMovingWave:
			if candidate.Phase == PhaseReleasingExclusion {
				return candidate.WaveIndex == current.WaveIndex
			}
			return candidate.Phase == PhaseVerifyingWave && candidate.WaveIndex == current.WaveIndex
		case PhaseVerifyingWave:
			if candidate.Phase == PhaseReleasingExclusion {
				return candidate.WaveIndex == current.WaveIndex
			}
			end := waveEnd(current)
			if candidate.Phase == PhaseMovingWave {
				return candidate.WaveIndex == uint32(end)
			}
			if candidate.Phase == PhaseWaitingZeroShard {
				return candidate.WaveIndex == uint32(end) ||
					(current.Submission.Limits.LegacySerial() && candidate.WaveIndex == uint32(end-1))
			}
		case PhaseWaitingZeroShard:
			if candidate.Phase == PhaseReleasingExclusion {
				return candidate.WaveIndex == current.WaveIndex
			}
			if current.Submission.Operation == OperationDrainHolder {
				return candidate.Phase == PhaseVerifyingFinal && candidate.WaveIndex == current.WaveIndex
			}
			return candidate.Phase == PhaseScalingDown && candidate.WaveIndex == current.WaveIndex
		case PhaseScalingDown:
			if candidate.Phase == PhaseReleasingExclusion {
				return candidate.WaveIndex == current.WaveIndex
			}
			return candidate.Phase == PhaseVerifyingFinal && candidate.WaveIndex == current.WaveIndex
		}
	}
	return false
}

func validateScaleIntentAdvance(current, candidate *ScaleIntent) error {
	if current == nil {
		if candidate == nil {
			return nil
		}
		if candidate.WorkloadUID == "" || candidate.ExpectedResourceVersion == "" ||
			candidate.ExpectedReplicas < 1 || candidate.DesiredReplicas < 1 || candidate.Effect != EffectPending {
			return fmt.Errorf("%w: invalid initial scale intent", ErrInvalidTransition)
		}
		return nil
	}
	if candidate == nil || current.WorkloadUID != candidate.WorkloadUID ||
		current.ExpectedResourceVersion != candidate.ExpectedResourceVersion ||
		current.ExpectedReplicas != candidate.ExpectedReplicas || current.DesiredReplicas != candidate.DesiredReplicas {
		return fmt.Errorf("%w: scale intent changed", ErrConflict)
	}
	if !effectCanAdvance(current.Effect, candidate.Effect) {
		return fmt.Errorf("%w: scale effect regressed", ErrInvalidTransition)
	}
	return nil
}

func validateExclusionIntentAdvance(current, candidate *ExclusionIntent) error {
	if current == nil {
		if candidate == nil {
			return nil
		}
		if !candidate.Victim.Exact() || candidate.ExpectedRevision == 0 || candidate.OwnerPlanID == "" || candidate.Effect != EffectPending {
			return fmt.Errorf("%w: invalid initial exclusion intent", ErrInvalidTransition)
		}
		return nil
	}
	if candidate == nil || current.Victim != candidate.Victim || current.ExpectedRevision != candidate.ExpectedRevision ||
		current.OwnerPlanID != candidate.OwnerPlanID {
		return fmt.Errorf("%w: exclusion intent changed", ErrConflict)
	}
	if current.ObservedRevision != 0 && candidate.ObservedRevision != current.ObservedRevision {
		return fmt.Errorf("%w: observed exclusion revision changed", ErrConflict)
	}
	if current.ObservedRevision == 0 && candidate.ObservedRevision != 0 && candidate.ObservedRevision <= current.ExpectedRevision {
		return fmt.Errorf("%w: observed exclusion revision did not advance", ErrInvalidTransition)
	}
	if !effectCanAdvance(current.Effect, candidate.Effect) {
		return fmt.Errorf("%w: exclusion effect regressed", ErrInvalidTransition)
	}
	return nil
}

func effectCanAdvance(from, to EffectState) bool {
	if from == to {
		return true
	}
	return from == EffectPending && (to == EffectCommitted || to == EffectAdopted)
}

func validateMovesAdvance(current, candidate []ResolvedMove) error {
	if len(candidate) < len(current) || len(candidate) > len(current)+MaxMoves {
		return fmt.Errorf("%w: resolved move list changed", ErrInvalidTransition)
	}
	for i := range current {
		left, right := current[i], candidate[i]
		// Frozen child intent cannot change. Only observed phase/ensure/verify may advance.
		left.ChildPhase, right.ChildPhase = "", ""
		left.EnsureObserved, right.EnsureObserved = false, false
		left.Verified, right.Verified = false, false
		left.Reconciled, right.Reconciled = false, false
		if left != right {
			return fmt.Errorf("%w: child intent %d changed", ErrConflict, i)
		}
		if current[i].EnsureObserved && !candidate[i].EnsureObserved {
			return fmt.Errorf("%w: child ensure regressed", ErrInvalidTransition)
		}
		if current[i].Verified && !candidate[i].Verified {
			return fmt.Errorf("%w: child verification regressed", ErrInvalidTransition)
		}
		if current[i].Reconciled && !candidate[i].Reconciled {
			return fmt.Errorf("%w: child reconciliation regressed", ErrInvalidTransition)
		}
		if current[i].Reconciled && current[i] != candidate[i] {
			return fmt.Errorf("%w: reconciled child changed", ErrInvalidTransition)
		}
		if current[i].Verified && current[i].ChildPhase != candidate[i].ChildPhase {
			// A paused child can be observed as SUCCEEDED before a later
			// controlled reconciliation records the accepted PAUSED outcome.
			if !(current[i].ChildPhase == RoutePlanPhaseSucceeded && candidate[i].ChildPhase == RoutePlanPhasePaused &&
				candidate[i].EnsureObserved && candidate[i].Verified && candidate[i].Reconciled) {
				return fmt.Errorf("%w: verified child phase changed", ErrConflict)
			}
		}
		if current[i].ChildPhase != "" && current[i].ChildPhase != RoutePlanPhaseSucceeded &&
			current[i].ChildPhase != RoutePlanPhasePaused && candidate[i].ChildPhase != RoutePlanPhasePaused &&
			candidate[i].ChildPhase != RoutePlanPhaseSucceeded &&
			!routePlanNonterminalCanAdvance(current[i].ChildPhase, candidate[i].ChildPhase) {
			return fmt.Errorf("%w: child observation regressed", ErrInvalidTransition)
		}
		if current[i].ChildPhase == RoutePlanPhaseSucceeded && candidate[i].ChildPhase != RoutePlanPhaseSucceeded &&
			!(candidate[i].ChildPhase == RoutePlanPhasePaused && candidate[i].EnsureObserved &&
				candidate[i].Verified && candidate[i].Reconciled) {
			return fmt.Errorf("%w: succeeded child phase regressed", ErrInvalidTransition)
		}
	}
	for i := len(current); i < len(candidate); i++ {
		if !candidate[i].Frozen() || !childObservationEmpty(candidate[i]) {
			return fmt.Errorf("%w: appended child intent is incomplete", ErrInvalidTransition)
		}
	}
	return nil
}
func validateEnteringCleanup(current, candidate Plan) error {
	if candidate.Phase != PhaseReleasingExclusion || candidate.CleanupIntent == nil ||
		candidate.CleanupIntent.ScaleResolution != ScaleResolutionPending || candidate.CleanupIntent.ReleaseOutcome != "" ||
		candidate.CleanupIntent.FailureCode == "" || current.ExclusionIntent == nil ||
		current.ExclusionIntent.Effect == EffectPending {
		return fmt.Errorf("%w: invalid cleanup phase entry", ErrInvalidTransition)
	}
	switch current.Phase {
	case PhaseCapturingVictimShards, PhaseMovingWave, PhaseVerifyingWave, PhaseWaitingZeroShard, PhaseScalingDown:
	default:
		return fmt.Errorf("%w: cleanup entered from unsafe phase", ErrInvalidTransition)
	}
	left, right := factsOf(current), factsOf(candidate)
	left.CleanupIntent, right.CleanupIntent = nil, nil
	if !reflect.DeepEqual(left, right) {
		return fmt.Errorf("%w: cleanup phase entry changed durable facts", ErrInvalidTransition)
	}
	return nil
}
func validateCleanupPhaseUpdate(current, candidate Plan) error {
	if current.CleanupIntent == nil || candidate.CleanupIntent == nil ||
		current.CleanupIntent.FailureCode != candidate.CleanupIntent.FailureCode {
		return fmt.Errorf("%w: cleanup intent identity changed", ErrConflict)
	}
	left, right := factsOf(current), factsOf(candidate)
	left.CleanupIntent, right.CleanupIntent = nil, nil
	before, after := current.CleanupIntent, candidate.CleanupIntent
	if before.ScaleResolution == ScaleResolutionPending && after.ScaleResolution == ScaleResolutionCommitted &&
		current.ScaleIntent != nil && candidate.ScaleIntent != nil && current.ScaleIntent.Effect == EffectPending {
		left.ScaleIntent, right.ScaleIntent = nil, nil
	}
	if !reflect.DeepEqual(left, right) {
		return fmt.Errorf("%w: cleanup update changed durable facts", ErrInvalidTransition)
	}
	return validateCleanupIntentAdvance(before, after)
}

func validateCleanupIntentAdvance(current, candidate *CleanupIntent) error {
	if current == nil {
		if candidate == nil {
			return nil
		}
		if candidate.FailureCode == "" || candidate.ScaleResolution != ScaleResolutionPending || candidate.ReleaseOutcome != "" {
			return fmt.Errorf("%w: invalid initial cleanup intent", ErrInvalidTransition)
		}
		return nil
	}
	if candidate == nil || current.FailureCode != candidate.FailureCode {
		return fmt.Errorf("%w: cleanup intent changed", ErrConflict)
	}
	if current.ScaleResolution == candidate.ScaleResolution {
		if current.ScaleResolution == ScaleResolutionNotCommitted {
			if current.ReleaseOutcome == "" && candidate.ReleaseOutcome.TerminalForScalePlan() {
				return nil
			}
			if current.ReleaseOutcome == candidate.ReleaseOutcome {
				return nil
			}
			return fmt.Errorf("%w: invalid cleanup release replay", ErrInvalidTransition)
		}
		if current.ReleaseOutcome == candidate.ReleaseOutcome && candidate.ReleaseOutcome == "" {
			return nil
		}
		return fmt.Errorf("%w: cleanup resolution changed without progress", ErrInvalidTransition)
	}
	if current.ScaleResolution == ScaleResolutionPending &&
		(candidate.ScaleResolution == ScaleResolutionNotCommitted || candidate.ScaleResolution == ScaleResolutionCommitted) &&
		candidate.ReleaseOutcome == "" {
		return nil
	}
	return fmt.Errorf("%w: cleanup resolution regressed", ErrInvalidTransition)
}
