package domain

// PlantCommand is the farm-side adjudication input of CommitPlant (design
// 32 §3.3). The seed reservation is PlayerActor authority; the farm domain
// only creates the frozen crop cycle on an empty, unlocked plot.
type PlantCommand struct {
	PlotID                  uint32
	CycleID                 string
	ExpectedPlotVersion     uint64
	ExpectedCropRuleVersion uint64
	// Compendium eligibility is frozen by the caller from the versioned
	// catalog at Workflow creation and passed through unchanged (design
	// 32 §3.3 / design 37).
	Compendium CompendiumUnlockSnapshot
}

// DecidePlant creates a CropCycle with every rule parameter frozen at plant
// time (design 32 §3.3):
//
//   - mature_at = trustedNowMs (the FarmActor commit turn's time) + frozen
//     grow duration, with checked arithmetic;
//   - steal plan slot quantities derived deterministically from the frozen
//     base yield;
//   - P0 FIXED_OFFSET care plan: at most one WATER and one REMOVE_PEST
//     incident, due at plant_at + configured offset, independent of each
//     other;
//   - the compendium unlock snapshot is persisted with the cycle.
//
// The same command replayed after a reload produces an identical cycle
// given the same cycle ID and trustedNowMs.
func DecidePlant(state FarmState, cmd PlantCommand, rule CropRule, trustedNowMs int64) (CandidateState, PlantDecision, *Error) {
	if cmd.CycleID == "" {
		return CandidateState{}, PlantDecision{}, reject(CodeInvalidArgument, "cycle id required")
	}
	if cmd.Compendium.CropDefinitionID != rule.CropDefinitionID {
		// The compendium snapshot frozen at Workflow creation must describe
		// the same crop definition as the frozen rule.
		return CandidateState{}, PlantDecision{}, reject(CodeInvalidArgument,
			"compendium crop %d does not match rule crop %d", cmd.Compendium.CropDefinitionID, rule.CropDefinitionID)
	}
	if cmd.ExpectedCropRuleVersion != 0 && cmd.ExpectedCropRuleVersion != rule.RuleVersion {
		return CandidateState{}, PlantDecision{}, reject(CodeRuleVersionChanged,
			"crop rule version %d, client expected %d", rule.RuleVersion, cmd.ExpectedCropRuleVersion)
	}
	if err := ValidateCropRule(rule); err != nil {
		return CandidateState{}, PlantDecision{}, reject(CodeCropRuleUnknown, "%v", err)
	}
	if state.FindCycle(cmd.CycleID) != nil {
		return CandidateState{}, PlantDecision{}, reject(CodeInvalidArgument,
			"cycle id %q already exists", cmd.CycleID)
	}
	plot := state.FindPlot(cmd.PlotID)
	if plot == nil {
		return CandidateState{}, PlantDecision{}, reject(CodePlotNotFound, "plot %d", cmd.PlotID)
	}
	if !plot.Unlocked() {
		return CandidateState{}, PlantDecision{}, reject(CodePlotLocked, "plot %d is locked", cmd.PlotID)
	}
	// ExpectedPlotVersion is ignored on CommitPlant. Steal/care bump PlotVersion
	// and must not reject an otherwise valid plant (the compensation saga's
	// main false-reject). Empty/locked/cycle-exists remain the only gates.
	if _, active := state.ActiveCycleOf(cmd.PlotID); active != nil {
		return CandidateState{}, PlantDecision{}, reject(CodePlotNotEmpty, "plot %d is not empty", cmd.PlotID)
	}

	matureAt, err := checkedAddTime(trustedNowMs, rule.GrowDurationMs)
	if err != nil {
		return CandidateState{}, PlantDecision{}, err
	}
	slotQuantity, stealableLimit, cfgErr := stealQuantities(rule.BaseYield, rule.StealPlan)
	if cfgErr != nil {
		return CandidateState{}, PlantDecision{}, reject(CodeCropRuleUnknown, "%v", cfgErr)
	}
	incidents, err := buildCarePlanIncidents(cmd.CycleID, rule.CarePlan, trustedNowMs)
	if err != nil {
		return CandidateState{}, PlantDecision{}, err
	}

	cycle := CropCycle{
		CycleID:           cmd.CycleID,
		PlotID:            cmd.PlotID,
		CropDefinitionID:  rule.CropDefinitionID,
		RuleVersion:       rule.RuleVersion,
		PlantAtMs:         trustedNowMs,
		MatureAtMs:        matureAt,
		BaseYield:         rule.BaseYield,
		StealableLimit:    stealableLimit,
		HarvestExp:        rule.HarvestExp,
		CropUnitCoinValue: rule.CropUnitCoinValue,
		StealPlan: StealPlanSnapshot{
			PolicyVersion:   rule.StealPlan.PolicyVersion,
			TotalCapRatioBP: rule.StealPlan.TotalCapRatioBP,
			SlotRatioBP:     append([]uint32(nil), rule.StealPlan.SlotRatioBP...),
			SlotQuantity:    slotQuantity,
		},
		CarePlan: CarePlanSnapshot{
			CareRuleVersion: rule.CarePlan.CareRuleVersion,
			ScheduleMode:    rule.CarePlan.ScheduleMode,
		},
		Compendium: cmd.Compendium,
		Incidents:  incidents,
	}

	candidate := state.Clone()
	candidate.Cycles = append(candidate.Cycles, cycle)
	candidatePlot := candidate.FindPlot(cmd.PlotID)
	candidatePlot.ActiveCropCycleID = cycle.CycleID
	commitMutation(&candidate, candidatePlot, trustedNowMs)

	decision := PlantDecision{
		PlotID:               cmd.PlotID,
		CycleID:              cycle.CycleID,
		CropDefinitionID:     cycle.CropDefinitionID,
		AppliedRuleVersion:   cycle.RuleVersion,
		PlantAtMs:            cycle.PlantAtMs,
		MatureAtMs:           cycle.MatureAtMs,
		CommittedPlotVersion: candidatePlot.PlotVersion,
		CommittedFarmVersion: candidate.FarmVersion,
	}
	return CandidateState{State: candidate}, decision, nil
}

// buildCarePlanIncidents freezes the P0 FIXED_OFFSET care plan: at most one
// WATER and one REMOVE_PEST incident per cycle, due at plantAt + offset.
// Unhandled incidents never change mature_at, base_yield or steal quota
// (design 32 Q32-04).
func buildCarePlanIncidents(cycleID string, plan CarePlanConfig, plantAtMs int64) ([]CareIncident, *Error) {
	var incidents []CareIncident
	add := func(careType CareType, offsetMs int64) *Error {
		dueAt, err := checkedAddTime(plantAtMs, offsetMs)
		if err != nil {
			return err
		}
		incidents = append(incidents, CareIncident{
			IncidentID: incidentID(cycleID, careType),
			CareType:   careType,
			DueAtMs:    dueAt,
		})
		return nil
	}
	if plan.WaterEnabled {
		if err := add(CareTypeWater, plan.WaterOffsetMs); err != nil {
			return nil, err
		}
	}
	if plan.PestEnabled {
		if err := add(CareTypeRemovePest, plan.PestOffsetMs); err != nil {
			return nil, err
		}
	}
	return incidents, nil
}
