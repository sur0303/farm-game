package domain

// HarvestCommand is the farm-side adjudication input of CommitHarvest
// (design 32 §4.3). Only the owner may harvest; friends interact through
// Care/Theft.
type HarvestCommand struct {
	PlayerID uint64
	PlotID   uint32
	CycleID  string
}

// DecideHarvest ends a mature crop cycle and freezes the final settlement
// (design 32 §4.3):
//
//	reward_quantity = base_yield - stolen_amount
//	0 <= stolen_amount <= stealable_limit <= base_yield
//
// Unresolved care incidents never block harvest and end together with the
// cycle (design 32 Q32-04). The frozen compendium snapshot is passed
// through unchanged so the PlayerActor can bind first-harvest unlock to the
// reward commit without re-reading the catalog. An immature cycle rejects;
// harvesting an ended cycle rejects so the same cycle can never be settled
// twice.
func DecideHarvest(state FarmState, cmd HarvestCommand, trustedNowMs int64) (CandidateState, HarvestDecision, *Error) {
	if cmd.PlayerID == 0 || cmd.PlayerID != state.OwnerPlayerID {
		return CandidateState{}, HarvestDecision{}, reject(CodeInvalidArgument,
			"only the owner may harvest plot %d", cmd.PlotID)
	}
	plot := state.FindPlot(cmd.PlotID)
	if plot == nil {
		return CandidateState{}, HarvestDecision{}, reject(CodePlotNotFound, "plot %d", cmd.PlotID)
	}
	cycle := state.FindCycle(cmd.CycleID)
	if cycle == nil || cycle.PlotID != cmd.PlotID {
		return CandidateState{}, HarvestDecision{}, reject(CodeCycleMismatch,
			"plot %d does not hold cycle %q", cmd.PlotID, cmd.CycleID)
	}
	if cycle.Ended {
		return CandidateState{}, HarvestDecision{}, reject(CodeCycleEnded,
			"cycle %q already ended", cmd.CycleID)
	}
	if plot.ActiveCropCycleID != cmd.CycleID {
		return CandidateState{}, HarvestDecision{}, reject(CodeCycleMismatch,
			"plot %d no longer holds cycle %q", cmd.PlotID, cmd.CycleID)
	}
	if !cycle.Mature(trustedNowMs) {
		return CandidateState{}, HarvestDecision{}, reject(CodeCropNotMature,
			"cycle %q matures at %d, now %d", cmd.CycleID, cycle.MatureAtMs, trustedNowMs)
	}

	reward := cycle.OwnerClaimable()
	var endedIncidentIDs []string

	candidate := state.Clone()
	var candidateCycle *CropCycle
	for i := range candidate.Cycles {
		if candidate.Cycles[i].CycleID == cmd.CycleID {
			candidateCycle = &candidate.Cycles[i]
			break
		}
	}
	candidateCycle.Ended = true
	candidateCycle.EndReason = CycleEndReasonHarvested
	for i := range candidateCycle.Incidents {
		if !candidateCycle.Incidents[i].Resolved() {
			endedIncidentIDs = append(endedIncidentIDs, candidateCycle.Incidents[i].IncidentID)
		}
	}
	candidatePlot := candidate.FindPlot(cmd.PlotID)
	candidatePlot.ActiveCropCycleID = ""
	commitMutation(&candidate, candidatePlot, trustedNowMs)

	decision := HarvestDecision{
		PlotID:               cmd.PlotID,
		CycleID:              cmd.CycleID,
		CropDefinitionID:     cycle.CropDefinitionID,
		RewardQuantity:       reward,
		StolenAmount:         cycle.StolenAmount,
		FrozenHarvestExp:     cycle.HarvestExp,
		Compendium:           cycle.Compendium,
		EndedIncidentIDs:     endedIncidentIDs,
		CommittedPlotVersion: candidatePlot.PlotVersion,
		CommittedFarmVersion: candidate.FarmVersion,
	}
	return CandidateState{State: candidate}, decision, nil
}
