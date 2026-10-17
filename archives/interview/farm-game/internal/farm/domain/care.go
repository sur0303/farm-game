package domain

// CareCommand is the farm-side adjudication input of one care intent
// (design 32 §5.1/§5.3). Friendship and permit checks happen at admission;
// the domain adjudicates the incident itself.
type CareCommand struct {
	ResolverPlayerID uint64
	PlotID           uint32
	CycleID          string
	IncidentID       string
	CareType         CareType
}

// DecideCare resolves a farm-global care incident (design 32 §5). The first
// legal resolver wins: the incident records resolved_by/resolved_at and
// exactly one reward obligation is created. Every later request — from the
// same or any other player — returns NO_CHANGE / ALREADY_RESOLVED without
// creating a second reward obligation.
//
// An incident is only resolvable once trustedNowMs >= dueAtMs; a forged
// early incident ID is rejected (design 32 §5.5). Care on an ended cycle is
// rejected: a late care after harvest earns nothing. Unresolved incidents
// never affect maturity, base yield or steal quota.
func DecideCare(state FarmState, cmd CareCommand, trustedNowMs int64) (CandidateState, CareDecision, *Error) {
	if cmd.ResolverPlayerID == 0 {
		return CandidateState{}, CareDecision{}, reject(CodeInvalidArgument, "resolver required")
	}
	plot := state.FindPlot(cmd.PlotID)
	if plot == nil {
		return CandidateState{}, CareDecision{}, reject(CodePlotNotFound, "plot %d", cmd.PlotID)
	}
	cycle := state.FindCycle(cmd.CycleID)
	if cycle == nil || cycle.PlotID != cmd.PlotID {
		return CandidateState{}, CareDecision{}, reject(CodeCycleMismatch,
			"plot %d does not hold cycle %q", cmd.PlotID, cmd.CycleID)
	}
	if cycle.Ended {
		return CandidateState{}, CareDecision{}, reject(CodeCycleEnded,
			"cycle %q already ended", cmd.CycleID)
	}
	if plot.ActiveCropCycleID != cmd.CycleID {
		return CandidateState{}, CareDecision{}, reject(CodeCycleMismatch,
			"plot %d no longer holds cycle %q", cmd.PlotID, cmd.CycleID)
	}
	incident := cycle.FindIncident(cmd.IncidentID)
	if incident == nil {
		return CandidateState{}, CareDecision{}, reject(CodeIncidentUnknown,
			"incident %q not in cycle %q", cmd.IncidentID, cmd.CycleID)
	}
	if incident.CareType != cmd.CareType {
		return CandidateState{}, CareDecision{}, reject(CodeCareTypeMismatch,
			"incident %q is %s not %s", cmd.IncidentID, incident.CareType, cmd.CareType)
	}
	if incident.Resolved() {
		// Global first-success semantics: no second reward, no error.
		return CandidateState{}, CareDecision{
				PlotID:             cmd.PlotID,
				CycleID:            cmd.CycleID,
				IncidentID:         cmd.IncidentID,
				CareType:           incident.CareType,
				Outcome:            CareOutcomeAlreadyResolved,
				ResolvedByPlayerID: incident.ResolvedByPlayerID,
				ResolvedAtMs:       incident.ResolvedAtMs,
			}, noChange(CodeAlreadyResolved, "incident %q already resolved by %d",
				cmd.IncidentID, incident.ResolvedByPlayerID)
	}
	if trustedNowMs < incident.DueAtMs {
		return CandidateState{}, CareDecision{}, reject(CodeIncidentNotDue,
			"incident %q due at %d, now %d", cmd.IncidentID, incident.DueAtMs, trustedNowMs)
	}

	candidate := state.Clone()
	candidateCycle := candidate.FindCycle(cmd.CycleID)
	candidateIncident := candidateCycle.FindIncident(cmd.IncidentID)
	candidateIncident.ResolvedAtMs = trustedNowMs
	candidateIncident.ResolvedByPlayerID = cmd.ResolverPlayerID
	candidatePlot := candidate.FindPlot(cmd.PlotID)
	commitMutation(&candidate, candidatePlot, trustedNowMs)

	decision := CareDecision{
		PlotID:                  cmd.PlotID,
		CycleID:                 cmd.CycleID,
		IncidentID:              cmd.IncidentID,
		CareType:                incident.CareType,
		Outcome:                 CareOutcomeResolved,
		ResolvedByPlayerID:      cmd.ResolverPlayerID,
		ResolvedAtMs:            trustedNowMs,
		RewardObligationCreated: true,
		CommittedPlotVersion:    candidatePlot.PlotVersion,
		CommittedFarmVersion:    candidate.FarmVersion,
	}
	return CandidateState{State: candidate}, decision, nil
}
