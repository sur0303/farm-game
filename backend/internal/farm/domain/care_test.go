package domain

import "testing"

func TestDecideCareFirstSuccessThenNoChange(t *testing.T) {
	rule := validCropRule(50)
	state := newTestFarm(t)
	state, plant := plantOn(t, state, 1, "cc-1", testNow, rule)
	waterID := incidentID("cc-1", CareTypeWater)
	dueAt := testNow + 10_000
	_ = plant

	// First legal care wins and creates exactly one reward obligation.
	candidate, decision, derr := DecideCare(state, CareCommand{
		ResolverPlayerID: 2002, PlotID: 1, CycleID: "cc-1",
		IncidentID: waterID, CareType: CareTypeWater,
	}, dueAt)
	mustOK(t, derr)
	if decision.Outcome != CareOutcomeResolved || !decision.RewardObligationCreated ||
		decision.ResolvedByPlayerID != 2002 || decision.ResolvedAtMs != dueAt {
		t.Fatalf("decision=%+v", decision)
	}
	state = candidate.State

	// Every later request — same or different resolver — is NO_CHANGE
	// without a second reward obligation (design 32 §5.1).
	for _, resolver := range []uint64{2002, 2003, 1001} {
		again, repeated, derr := DecideCare(state, CareCommand{
			ResolverPlayerID: resolver, PlotID: 1, CycleID: "cc-1",
			IncidentID: waterID, CareType: CareTypeWater,
		}, dueAt+1)
		mustErr(t, derr, CodeAlreadyResolved, StatusNoChange)
		if repeated.Outcome != CareOutcomeAlreadyResolved || repeated.RewardObligationCreated ||
			repeated.ResolvedByPlayerID != 2002 {
			t.Fatalf("repeat by %d decision=%+v", resolver, repeated)
		}
		_ = again
	}
}

func TestDecideCareWaterAndPestAreIndependent(t *testing.T) {
	rule := validCropRule(50)
	state := newTestFarm(t)
	state, _ = plantOn(t, state, 1, "cc-1", testNow, rule)

	// Resolving WATER must not resolve REMOVE_PEST: both incidents may be
	// pending at the same time and each has its own global first success
	// (design 32 Q32-04).
	candidate, _, derr := DecideCare(state, CareCommand{
		ResolverPlayerID: 2002, PlotID: 1, CycleID: "cc-1",
		IncidentID: incidentID("cc-1", CareTypeWater), CareType: CareTypeWater,
	}, testNow+10_000)
	mustOK(t, derr)
	state = candidate.State

	candidate, pest, derr := DecideCare(state, CareCommand{
		ResolverPlayerID: 2003, PlotID: 1, CycleID: "cc-1",
		IncidentID: incidentID("cc-1", CareTypeRemovePest), CareType: CareTypeRemovePest,
	}, testNow+20_000)
	mustOK(t, derr)
	if pest.Outcome != CareOutcomeResolved || pest.ResolvedByPlayerID != 2003 {
		t.Fatalf("pest decision=%+v", pest)
	}
	cycle := candidate.State.FindCycle("cc-1")
	if !cycle.FindIncident(incidentID("cc-1", CareTypeWater)).Resolved() ||
		!cycle.FindIncident(incidentID("cc-1", CareTypeRemovePest)).Resolved() {
		t.Fatalf("incidents=%+v", cycle.Incidents)
	}
}

func TestDecideCareRejections(t *testing.T) {
	rule := validCropRule(50)
	waterID := incidentID("cc-1", CareTypeWater)
	tests := []struct {
		name       string
		prepare    func(state FarmState) FarmState
		cmd        CareCommand
		now        int64
		wantCode   Code
		wantStatus Status
	}{
		{
			name: "incident not due, forged early id rejected",
			cmd: CareCommand{
				ResolverPlayerID: 2002, PlotID: 1, CycleID: "cc-1",
				IncidentID: waterID, CareType: CareTypeWater,
			},
			now:        testNow + 9_999,
			wantCode:   CodeIncidentNotDue,
			wantStatus: StatusRejected,
		},
		{
			name: "unknown incident id",
			cmd: CareCommand{
				ResolverPlayerID: 2002, PlotID: 1, CycleID: "cc-1",
				IncidentID: "cc-1#WATER#9", CareType: CareTypeWater,
			},
			now:        testNow + 10_000,
			wantCode:   CodeIncidentUnknown,
			wantStatus: StatusRejected,
		},
		{
			name: "care type mismatch",
			cmd: CareCommand{
				ResolverPlayerID: 2002, PlotID: 1, CycleID: "cc-1",
				IncidentID: waterID, CareType: CareTypeRemovePest,
			},
			now:        testNow + 10_000,
			wantCode:   CodeCareTypeMismatch,
			wantStatus: StatusRejected,
		},
		{
			name: "old cycle id after replant cannot receive care",
			prepare: func(state FarmState) FarmState {
				candidate, _, derr := DecideHarvest(state, HarvestCommand{
					PlayerID: 1001, PlotID: 1, CycleID: "cc-1",
				}, testNow+60_000)
				mustOK(t, derr)
				next, _ := plantOn(t, candidate.State, 1, "cc-2", testNow+60_001, rule)
				return next
			},
			cmd: CareCommand{
				ResolverPlayerID: 2002, PlotID: 1, CycleID: "cc-1",
				IncidentID: waterID, CareType: CareTypeWater,
			},
			now:        testNow + 61_000,
			wantCode:   CodeCycleEnded,
			wantStatus: StatusRejected,
		},
		{
			name: "care after harvest rejected without reward",
			prepare: func(state FarmState) FarmState {
				mature := testNow + 60_000
				candidate, _, derr := DecideHarvest(state, HarvestCommand{
					PlayerID: 1001, PlotID: 1, CycleID: "cc-1",
				}, mature)
				mustOK(t, derr)
				return candidate.State
			},
			cmd: CareCommand{
				ResolverPlayerID: 2002, PlotID: 1, CycleID: "cc-1",
				IncidentID: waterID, CareType: CareTypeWater,
			},
			now:        testNow + 61_000,
			wantCode:   CodeCycleEnded,
			wantStatus: StatusRejected,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestFarm(t)
			state, _ = plantOn(t, state, 1, "cc-1", testNow, rule)
			if tc.prepare != nil {
				state = tc.prepare(state)
			}
			_, _, derr := DecideCare(state, tc.cmd, tc.now)
			mustErr(t, derr, tc.wantCode, tc.wantStatus)
		})
	}
}

// TestUnresolvedCareDoesNotAffectYield proves unhandled incidents never
// change mature_at, base_yield or the harvest reward (design 32 Q32-04).
func TestUnresolvedCareDoesNotAffectYield(t *testing.T) {
	rule := validCropRule(250)
	state := newTestFarm(t)
	state, _ = plantOn(t, state, 1, "cc-1", testNow, rule)
	candidate, harvest, derr := DecideHarvest(state, HarvestCommand{
		PlayerID: 1001, PlotID: 1, CycleID: "cc-1",
	}, testNow+60_000)
	mustOK(t, derr)
	if harvest.RewardQuantity != 250 || harvest.StolenAmount != 0 {
		t.Fatalf("harvest=%+v", harvest)
	}
	// Both unresolved incidents end together with the cycle.
	if len(harvest.EndedIncidentIDs) != 2 {
		t.Fatalf("ended incidents=%v", harvest.EndedIncidentIDs)
	}
	cycle := candidate.State.FindCycle("cc-1")
	if !cycle.Ended || cycle.EndReason != CycleEndReasonHarvested {
		t.Fatalf("cycle=%+v", cycle)
	}
}
