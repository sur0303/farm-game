package domain

import "math"

// TheftCommand is the farm-side adjudication input of DecideTheft (design
// 32 §6.3). Friendship and permit checks happen at admission; the domain
// adjudicates quota, maturity, per-visitor attempt and guard interception.
type TheftCommand struct {
	VisitorPlayerID uint64
	PlotID          uint32
	CycleID         string
}

// DecideGuardInterception settles the guard part of a theft with P0 binary
// semantics (design 32 Q32-06B / design 36 §9):
//
//   - an ineffective guard (no pet selected, or trustedNowMs >=
//     guardUntilMs) never intercepts;
//   - an effective guard intercepts when randomDrawBP <
//     trigger_probability_bp;
//   - P0 interception is all-or-nothing: guard_lost is either 0 or the full
//     gross quantity;
//   - intercepted crops are converted to owner coins at the cycle's frozen
//     unit value; the visitor's wallet is never touched.
//
// The random draw is an input captured once per decision turn, so retries,
// reloads and config hot updates can never re-roll a committed attempt.
func DecideGuardInterception(guard GuardSnapshot, grossQuantity uint32, frozenUnitCoinValue uint64, randomDrawBP uint32, trustedNowMs int64) (GuardDecision, *Error) {
	if randomDrawBP >= basisPointsDenominator {
		return GuardDecision{}, reject(CodeInvalidArgument,
			"random draw %d out of [0, %d)", randomDrawBP, basisPointsDenominator)
	}
	decision := GuardDecision{
		GuardVersion:     guard.GuardVersion,
		GuardRuleVersion: guard.Rule.GuardRuleVersion,
		RandomDrawBP:     randomDrawBP,
	}
	if !guard.Effective(trustedNowMs) {
		return decision, nil
	}
	decision.GuardEffective = true
	if randomDrawBP >= guard.Rule.TriggerProbabilityBP {
		return decision, nil
	}
	decision.GuardTriggered = true
	switch guard.Rule.InterceptPolicy {
	case InterceptPolicyFullGross:
		decision.GuardLostQuantity = grossQuantity
	default:
		// Publish-time validation restricts P0 to FULL_GROSS; reaching this
		// point means an unvalidated rule slipped through. Fail closed.
		return GuardDecision{}, reject(CodeInvalidArgument,
			"unsupported intercept policy %d", guard.Rule.InterceptPolicy)
	}
	if decision.GuardLostQuantity != 0 && frozenUnitCoinValue > math.MaxUint64/uint64(decision.GuardLostQuantity) {
		return GuardDecision{}, reject(CodeInvalidArgument,
			"guard coin value overflows: lost=%d unit=%d", decision.GuardLostQuantity, frozenUnitCoinValue)
	}
	decision.GuardCoinValue = uint64(decision.GuardLostQuantity) * frozenUnitCoinValue
	return decision, nil
}

// DecideTheft adjudicates one theft attempt (design 32 §6.3):
//
//   - an immature or ended cycle rejects without consuming the attempt;
//   - each visitor gets exactly one legal attempt per cycle;
//   - the attempt consumes the next slot of the frozen steal plan; the
//     cumulative stolen amount never exceeds the 20% cap;
//   - an exhausted quota is NO_CHANGE / NO_STEALABLE_QUOTA;
//   - a full guard interception is still a successful, consumed attempt
//     (TheftOutcomeBlockedByGuard, net = 0).
//
// stolen_amount always increases by the gross quantity, so owner + all
// gross stolen == base_yield at harvest (no overproduction).
func DecideTheft(state FarmState, cmd TheftCommand, randomDrawBP uint32, trustedNowMs int64) (CandidateState, TheftDecision, *Error) {
	if cmd.VisitorPlayerID == 0 {
		return CandidateState{}, TheftDecision{}, reject(CodeInvalidArgument, "visitor required")
	}
	if cmd.VisitorPlayerID == state.OwnerPlayerID {
		return CandidateState{}, TheftDecision{}, reject(CodeVisitorIsOwner,
			"visitor %d is the owner", cmd.VisitorPlayerID)
	}
	plot := state.FindPlot(cmd.PlotID)
	if plot == nil {
		return CandidateState{}, TheftDecision{}, reject(CodePlotNotFound, "plot %d", cmd.PlotID)
	}
	cycle := state.FindCycle(cmd.CycleID)
	if cycle == nil || cycle.PlotID != cmd.PlotID {
		return CandidateState{}, TheftDecision{}, reject(CodeCycleMismatch,
			"plot %d does not hold cycle %q", cmd.PlotID, cmd.CycleID)
	}
	if cycle.Ended {
		return CandidateState{}, TheftDecision{}, reject(CodeCycleEnded,
			"cycle %q already ended", cmd.CycleID)
	}
	if plot.ActiveCropCycleID != cmd.CycleID {
		return CandidateState{}, TheftDecision{}, reject(CodeCycleMismatch,
			"plot %d no longer holds cycle %q", cmd.PlotID, cmd.CycleID)
	}
	if !cycle.Mature(trustedNowMs) {
		return CandidateState{}, TheftDecision{}, reject(CodeCropNotMature,
			"cycle %q matures at %d, now %d", cmd.CycleID, cycle.MatureAtMs, trustedNowMs)
	}
	if cycle.StolenAmount > cycle.BaseYield || cycle.StealableLimit > cycle.BaseYield ||
		cycle.StolenAmount > cycle.StealableLimit {
		return CandidateState{}, TheftDecision{}, reject(CodeInvalidArgument,
			"cycle %q has invalid stolen/limit invariant", cmd.CycleID)
	}
	if cycle.HasAttempt(cmd.VisitorPlayerID) {
		return CandidateState{}, TheftDecision{}, reject(CodeStealAlreadyAttempted,
			"visitor %d already attempted cycle %q", cmd.VisitorPlayerID, cmd.CycleID)
	}
	slotIndex := cycle.NextStealSlotIndex
	if int(slotIndex) >= len(cycle.StealPlan.SlotQuantity) {
		return CandidateState{}, TheftDecision{}, noChange(CodeNoStealableQuota,
			"cycle %q consumed all %d steal slots", cmd.CycleID, len(cycle.StealPlan.SlotQuantity))
	}
	gross := cycle.StealPlan.SlotQuantity[slotIndex]
	if gross == 0 || gross > cycle.StealableLimit-cycle.StolenAmount {
		return CandidateState{}, TheftDecision{}, noChange(CodeNoStealableQuota,
			"cycle %q has no stealable quota left", cmd.CycleID)
	}
	if slotIndex >= uint32(len(cycle.StealPlan.SlotRatioBP)) {
		return CandidateState{}, TheftDecision{}, reject(CodeInvalidArgument,
			"cycle %q steal slot ratio is missing", cmd.CycleID)
	}

	guardDecision, err := DecideGuardInterception(state.Guard, gross, cycle.CropUnitCoinValue, randomDrawBP, trustedNowMs)
	if err != nil {
		return CandidateState{}, TheftDecision{}, err
	}

	candidate := state.Clone()
	candidateCycle := candidate.FindCycle(cmd.CycleID)
	candidateCycle.StolenAmount += gross
	candidateCycle.NextStealSlotIndex++
	candidateCycle.Attempts = append(candidateCycle.Attempts, TheftAttemptRecord{
		VisitorPlayerID: cmd.VisitorPlayerID,
		StealSlotIndex:  slotIndex,
		DecidedAtMs:     trustedNowMs,
	})
	candidatePlot := candidate.FindPlot(cmd.PlotID)
	commitMutation(&candidate, candidatePlot, trustedNowMs)

	outcome := TheftOutcomeStolen
	if guardDecision.GuardTriggered && guardDecision.GuardLostQuantity == gross {
		outcome = TheftOutcomeBlockedByGuard
	}
	decision := TheftDecision{
		PlotID:               cmd.PlotID,
		CycleID:              cmd.CycleID,
		VisitorPlayerID:      cmd.VisitorPlayerID,
		Outcome:              outcome,
		StealSlotIndex:       slotIndex,
		AppliedSlotRatioBP:   cycle.StealPlan.SlotRatioBP[slotIndex],
		GrossStolenQuantity:  gross,
		Guard:                guardDecision,
		NetReceivedQuantity:  gross - guardDecision.GuardLostQuantity,
		CommittedPlotVersion: candidatePlot.PlotVersion,
		CommittedFarmVersion: candidate.FarmVersion,
	}
	return CandidateState{State: candidate}, decision, nil
}
