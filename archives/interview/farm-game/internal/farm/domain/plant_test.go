package domain

import (
	"math"
	"testing"
)

func TestDecidePlantFreezesRuleSnapshot(t *testing.T) {
	rule := validCropRule(250)
	state := newTestFarm(t)
	next, decision := plantOn(t, state, 1, "cc-1", testNow, rule)

	if decision.PlantAtMs != testNow || decision.MatureAtMs != testNow+rule.GrowDurationMs {
		t.Fatalf("decision times=%d/%d", decision.PlantAtMs, decision.MatureAtMs)
	}
	cycle := next.FindCycle("cc-1")
	if cycle == nil {
		t.Fatalf("cycle not installed")
	}
	// Frozen fields (design 32 §3.3): definition, rule version, plant/mature
	// times, base yield, stealable limit, unit coin value, harvest exp.
	if cycle.CropDefinitionID != rule.CropDefinitionID || cycle.RuleVersion != rule.RuleVersion ||
		cycle.BaseYield != 250 || cycle.StealableLimit != 50 ||
		cycle.CropUnitCoinValue != rule.CropUnitCoinValue || cycle.HarvestExp != rule.HarvestExp {
		t.Fatalf("cycle=%+v", cycle)
	}
	// Steal plan: ten exact 2% slots of 5, summing exactly to 20%.
	if len(cycle.StealPlan.SlotQuantity) != 10 {
		t.Fatalf("slots=%d", len(cycle.StealPlan.SlotQuantity))
	}
	var sum uint32
	for i, q := range cycle.StealPlan.SlotQuantity {
		if q != 5 || cycle.StealPlan.SlotRatioBP[i] != 200 {
			t.Fatalf("slot %d quantity=%d ratio=%d", i, q, cycle.StealPlan.SlotRatioBP[i])
		}
		sum += q
	}
	if sum != cycle.StealableLimit || sum != cycle.BaseYield/5 {
		t.Fatalf("sum=%d limit=%d", sum, cycle.StealableLimit)
	}
	// P0 FIXED_OFFSET care plan: one WATER at +10s, one REMOVE_PEST at
	// +20s, deterministic IDs bound to the cycle.
	if len(cycle.Incidents) != 2 {
		t.Fatalf("incidents=%d", len(cycle.Incidents))
	}
	water := cycle.FindIncident(incidentID("cc-1", CareTypeWater))
	pest := cycle.FindIncident(incidentID("cc-1", CareTypeRemovePest))
	if water == nil || pest == nil {
		t.Fatalf("incident ids=%v", cycle.Incidents)
	}
	if water.DueAtMs != testNow+10_000 || pest.DueAtMs != testNow+20_000 {
		t.Fatalf("due_at water=%d pest=%d", water.DueAtMs, pest.DueAtMs)
	}
	if water.CareType != CareTypeWater || pest.CareType != CareTypeRemovePest {
		t.Fatalf("care types=%s/%s", water.CareType, pest.CareType)
	}
	// Compendium snapshot passes through frozen.
	if cycle.Compendium != testCompendium(rule.CropDefinitionID) {
		t.Fatalf("compendium=%+v", cycle.Compendium)
	}
	// Plot occupies the cycle and versions advance.
	plot := next.FindPlot(1)
	if plot.ActiveCropCycleID != "cc-1" || plot.PlotVersion != 2 {
		t.Fatalf("plot=%+v", plot)
	}
	if next.FarmVersion != state.FarmVersion+1 {
		t.Fatalf("farm version %d -> %d", state.FarmVersion, next.FarmVersion)
	}
}

func TestMaturityIsTimeDerived(t *testing.T) {
	rule := validCropRule(50)
	state := newTestFarm(t)
	state, decision := plantOn(t, state, 1, "cc-1", testNow, rule)
	cycle := state.FindCycle("cc-1")

	tests := []struct {
		name      string
		now       int64
		wantPhase CropPhase
	}{
		{name: "before maturity", now: decision.MatureAtMs - 1, wantPhase: CropPhaseGrowing},
		{name: "at maturity boundary", now: decision.MatureAtMs, wantPhase: CropPhaseMature},
		{name: "long after maturity, no wither", now: decision.MatureAtMs + 30*24*3600*1000, wantPhase: CropPhaseMature},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := cycle.Phase(tc.now); got != tc.wantPhase {
				t.Fatalf("phase=%v want %v", got, tc.wantPhase)
			}
		})
	}
	// The persisted record never carries a MATURE flag: reaching mature_at
	// writes nothing (design 32 §4.1).
	if state.FarmVersion != 2 {
		t.Fatalf("maturity derivation mutated state: farm_version=%d", state.FarmVersion)
	}
}

func TestDecidePlantRejections(t *testing.T) {
	rule := validCropRule(50)
	tests := []struct {
		name       string
		prepare    func(state FarmState) FarmState
		cmd        PlantCommand
		mutateRule func(*CropRule)
		wantCode   Code
	}{
		{
			name:     "locked plot",
			cmd:      PlantCommand{PlotID: 6, CycleID: "cc-x", Compendium: testCompendium(42)},
			wantCode: CodePlotLocked,
		},
		{
			name:     "plot not found",
			cmd:      PlantCommand{PlotID: 99, CycleID: "cc-x", Compendium: testCompendium(42)},
			wantCode: CodePlotNotFound,
		},
		{
			name: "plot not empty",
			prepare: func(state FarmState) FarmState {
				next, _ := plantOn(t, state, 1, "cc-old", testNow, rule)
				return next
			},
			cmd:      PlantCommand{PlotID: 1, CycleID: "cc-new", Compendium: testCompendium(42)},
			wantCode: CodePlotNotEmpty,
		},
		{
			name: "stale crop rule version",
			cmd: PlantCommand{
				PlotID: 1, CycleID: "cc-x", Compendium: testCompendium(42),
				ExpectedCropRuleVersion: rule.RuleVersion - 1,
			},
			wantCode: CodeRuleVersionChanged,
		},
		{
			name:       "invalid crop rule rejected fail-closed",
			cmd:        PlantCommand{PlotID: 1, CycleID: "cc-x", Compendium: testCompendium(42)},
			mutateRule: func(r *CropRule) { r.BaseYield = 51 },
			wantCode:   CodeCropRuleUnknown,
		},
		{
			name:     "compendium crop mismatch",
			cmd:      PlantCommand{PlotID: 1, CycleID: "cc-x", Compendium: testCompendium(43)},
			wantCode: CodeInvalidArgument,
		},
		{
			name:     "empty cycle id",
			cmd:      PlantCommand{PlotID: 1, CycleID: "", Compendium: testCompendium(42)},
			wantCode: CodeInvalidArgument,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestFarm(t)
			if tc.prepare != nil {
				state = tc.prepare(state)
			}
			r := rule
			if tc.mutateRule != nil {
				tc.mutateRule(&r)
			}
			_, _, derr := DecidePlant(state, tc.cmd, r, testNow)
			mustErr(t, derr, tc.wantCode, StatusRejected)
		})
	}
}

func TestDecidePlantIgnoresStalePlotVersionAfterCareBump(t *testing.T) {
	rule := validCropRule(50)
	state := newTestFarm(t)
	plot := state.FindPlot(1)
	if plot == nil {
		t.Fatal("plot 1 missing")
	}
	plot.PlotVersion = 99
	_, _, derr := DecidePlant(state, PlantCommand{
		PlotID: 1, CycleID: "cc-after-care", Compendium: testCompendium(42),
		ExpectedPlotVersion: 1,
	}, rule, testNow)
	if derr != nil {
		t.Fatalf("care/steal plot-version bump must not reject plant: %v", derr)
	}
}

func TestDecidePlantTimeOverflow(t *testing.T) {
	rule := validCropRule(50)
	state := newTestFarm(t)
	_, _, derr := DecidePlant(state, PlantCommand{
		PlotID: 1, CycleID: "cc-x", Compendium: testCompendium(42),
	}, rule, math.MaxInt64-1)
	mustErr(t, derr, CodeInvalidArgument, StatusRejected)
}

// TestCarePlanDeterministicAcrossReplay proves the P0 FIXED_OFFSET plan
// regenerates identical incident IDs and due times after reload/retry
// (design 32 §3.5).
func TestCarePlanDeterministicAcrossReplay(t *testing.T) {
	rule := validCropRule(50)
	state := newTestFarm(t)
	cmd := PlantCommand{PlotID: 1, CycleID: "cc-1", Compendium: testCompendium(42)}
	first, _, _ := DecidePlant(state, cmd, rule, testNow)
	second, _, _ := DecidePlant(state, cmd, rule, testNow)
	a := first.State.FindCycle("cc-1")
	b := second.State.FindCycle("cc-1")
	if !cropCycleEqual(a, b) {
		t.Fatalf("replayed plant diverged")
	}
}
