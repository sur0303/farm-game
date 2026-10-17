package domain

import "testing"

// TestOpportunityHeaderLifecycle walks the header through plant, care due,
// care resolved, maturity, theft quota exhaustion and harvest, proving the
// candidate summary is always derivable from the CandidateState and tracks
// the committed farm version (design 27 §4.2 / design 32 §5.3).
func TestOpportunityHeaderLifecycle(t *testing.T) {
	rule := validCropRule(250)
	state := newTestFarm(t)
	mature := testNow + 60_000
	waterDue := testNow + 10_000

	// After plant: no active opportunity yet; the header exposes the next
	// possible change (the water incident due time).
	state, _ = plantOn(t, state, 1, "cc-1", testNow, rule)
	header := state.Header
	if header.CandidateMask != OpportunityNone {
		t.Fatalf("mask=%d after plant", header.CandidateMask)
	}
	if header.NextOpportunityAtMs != waterDue {
		t.Fatalf("next=%d want %d", header.NextOpportunityAtMs, waterDue)
	}
	if header.SourceFarmVersion != state.FarmVersion || header.DirtyGeneration != 1 {
		t.Fatalf("header=%+v", header)
	}

	// Read-path recompute at a later instant derives the same summary from
	// the same state without mutating it: after water is due the care
	// opportunity appears.
	derived := ComputeOpportunity(&state, waterDue, state.FarmVersion, state.Header.DirtyGeneration)
	if derived.CandidateMask&OpportunityCareActive == 0 {
		t.Fatalf("mask=%d at water due", derived.CandidateMask)
	}
	if derived.NextOpportunityAtMs != testNow+20_000 {
		t.Fatalf("next=%d want pest due", derived.NextOpportunityAtMs)
	}

	// Care resolved: the mask clears the care bit on the committed header.
	candidate, _, derr := DecideCare(state, CareCommand{
		ResolverPlayerID: 2002, PlotID: 1, CycleID: "cc-1",
		IncidentID: incidentID("cc-1", CareTypeWater), CareType: CareTypeWater,
	}, waterDue)
	mustOK(t, derr)
	state = candidate.State
	if state.Header.CandidateMask&OpportunityCareActive != 0 {
		t.Fatalf("care still active after resolve: %+v", state.Header)
	}
	if state.Header.DirtyGeneration != 2 {
		t.Fatalf("dirty=%d", state.Header.DirtyGeneration)
	}

	// After maturity: harvest + theft candidates appear; next points to the
	// pest due time.
	derived = ComputeOpportunity(&state, mature, state.FarmVersion, state.Header.DirtyGeneration)
	if derived.CandidateMask&OpportunityHarvestReady == 0 ||
		derived.CandidateMask&OpportunityTheftCandidate == 0 {
		t.Fatalf("mask=%d at maturity", derived.CandidateMask)
	}

	// Theft quota exhausted by ten friends: the theft candidate bit clears.
	for i := uint64(0); i < 10; i++ {
		state, _ = stealFrom(t, state, uint64(2000)+i, 1, "cc-1", 9999, mature)
	}
	if state.Header.CandidateMask&OpportunityTheftCandidate != 0 {
		t.Fatalf("theft candidate after quota exhausted: %+v", state.Header)
	}
	if state.Header.CandidateMask&OpportunityHarvestReady == 0 {
		t.Fatalf("harvest candidate lost: %+v", state.Header)
	}

	// Harvest ends the cycle: no candidate remains, no future opportunity.
	candidate, _, derr = DecideHarvest(state, HarvestCommand{
		PlayerID: 1001, PlotID: 1, CycleID: "cc-1",
	}, mature+1)
	mustOK(t, derr)
	state = candidate.State
	if state.Header.CandidateMask != OpportunityNone || state.Header.NextOpportunityAtMs != 0 {
		t.Fatalf("header=%+v after harvest", state.Header)
	}
}

// TestOpportunityHeaderTracksUnlock proves plot unlock commits also refresh
// the derived head from the same CandidateState.
func TestOpportunityHeaderTracksUnlock(t *testing.T) {
	rule := validP0UnlockRule()
	state := newTestFarm(t)
	candidate, _, derr := DecidePlotUnlock(state, UnlockPlotCommand{PlotID: 6}, rule, testNow)
	mustOK(t, derr)
	if candidate.State.Header.SourceFarmVersion != candidate.State.FarmVersion ||
		candidate.State.Header.DirtyGeneration != 1 {
		t.Fatalf("header=%+v", candidate.State.Header)
	}
}
