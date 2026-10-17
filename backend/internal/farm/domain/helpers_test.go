package domain

import "testing"

// testNow is the fixed trusted timestamp used across the suite. Every test
// derives its timelines from it so results are fully deterministic.
const testNow int64 = 1_700_000_000_000

// validP0UnlockRule builds a valid versioned P0 unlock rule: 24 plots, 5
// initially unlocked, plots 6..24 purchased in fixed order.
func validP0UnlockRule() PlotUnlockRule {
	rule := PlotUnlockRule{
		RuleVersion:          7,
		MaxPlots:             P0MaxPlots,
		InitialUnlockedPlots: P0InitialUnlockedPlots,
	}
	for plotID := uint32(P0InitialUnlockedPlots + 1); plotID <= P0MaxPlots; plotID++ {
		rule.Entries = append(rule.Entries, PlotUnlockEntry{
			PlotID:          plotID,
			UnlockOrder:     plotID,
			RequiredLevel:   plotID - P0InitialUnlockedPlots,
			NormalCoinPrice: uint64(plotID-P0InitialUnlockedPlots) * 100,
		})
	}
	return rule
}

// validCropRule builds a valid versioned crop rule. baseYield must satisfy
// the Q32-12 exact-integer constraint for the P0 2% ten-slot curve.
func validCropRule(baseYield uint32) CropRule {
	slots := make([]uint32, 10)
	for i := range slots {
		slots[i] = 200
	}
	return CropRule{
		CropDefinitionID:  42,
		RuleVersion:       3,
		GrowDurationMs:    60_000,
		BaseYield:         baseYield,
		HarvestExp:        5,
		CropUnitCoinValue: 3,
		StealPlan: StealPlanConfig{
			PolicyVersion:   1,
			TotalCapRatioBP: 2000,
			SlotRatioBP:     slots,
		},
		CarePlan: CarePlanConfig{
			CareRuleVersion: 2,
			ScheduleMode:    ScheduleModeFixedOffset,
			WaterEnabled:    true,
			WaterOffsetMs:   10_000,
			PestEnabled:     true,
			PestOffsetMs:    20_000,
		},
	}
}

func testCompendium(cropDefinitionID uint32) CompendiumUnlockSnapshot {
	return CompendiumUnlockSnapshot{
		CropDefinitionID:         cropDefinitionID,
		UnlockEligible:           true,
		PlantedByPlayerID:        1001,
		CatalogVersionAtPlant:    11,
		DefinitionVersionAtPlant: 12,
	}
}

// newTestFarm creates the genesis farm: owner 1001 == farm 1001.
func newTestFarm(t *testing.T) FarmState {
	t.Helper()
	state, err := NewFarmState(1001, 1001, validP0UnlockRule())
	if err != nil {
		t.Fatalf("NewFarmState: %v", err)
	}
	return state
}

// plantOn plants a cycle on an unlocked plot and returns the installed
// candidate state plus the decision.
func plantOn(t *testing.T, state FarmState, plotID uint32, cycleID string, plantAtMs int64, rule CropRule) (FarmState, PlantDecision) {
	t.Helper()
	candidate, decision, derr := DecidePlant(state, PlantCommand{
		PlotID:     plotID,
		CycleID:    cycleID,
		Compendium: testCompendium(rule.CropDefinitionID),
	}, rule, plantAtMs)
	if derr != nil {
		t.Fatalf("DecidePlant(plot=%d cycle=%s): %v", plotID, cycleID, derr)
	}
	return candidate.State, decision
}

// stealFrom performs one theft and returns the installed state plus decision.
func stealFrom(t *testing.T, state FarmState, visitor uint64, plotID uint32, cycleID string, drawBP uint32, now int64) (FarmState, TheftDecision) {
	t.Helper()
	candidate, decision, derr := DecideTheft(state, TheftCommand{
		VisitorPlayerID: visitor,
		PlotID:          plotID,
		CycleID:         cycleID,
	}, drawBP, now)
	if derr != nil {
		t.Fatalf("DecideTheft(visitor=%d cycle=%s): %v", visitor, cycleID, derr)
	}
	return candidate.State, decision
}

func mustErr(t *testing.T, derr *Error, wantCode Code, wantStatus Status) {
	t.Helper()
	if derr == nil {
		t.Fatalf("expected %s/%v, got success", wantCode, wantStatus)
	}
	if derr.Code != wantCode || derr.Status != wantStatus {
		t.Fatalf("error = %s/%v, want %s/%v", derr.Code, derr.Status, wantCode, wantStatus)
	}
}

func mustOK(t *testing.T, derr *Error) {
	t.Helper()
	if derr != nil {
		t.Fatalf("expected success, got %v", derr)
	}
}
