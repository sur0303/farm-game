package domain

import "testing"

func TestDecideHarvestHappyPath(t *testing.T) {
	rule := validCropRule(250)
	state := newTestFarm(t)
	state, _ = plantOn(t, state, 1, "cc-1", testNow, rule)
	mature := testNow + 60_000

	candidate, decision, derr := DecideHarvest(state, HarvestCommand{
		PlayerID: 1001, PlotID: 1, CycleID: "cc-1",
	}, mature)
	mustOK(t, derr)
	if decision.RewardQuantity != 250 || decision.StolenAmount != 0 ||
		decision.FrozenHarvestExp != rule.HarvestExp ||
		decision.CropDefinitionID != rule.CropDefinitionID {
		t.Fatalf("decision=%+v", decision)
	}
	// The frozen compendium snapshot passes through unchanged for the
	// PlayerActor reward commit (design 32 §4.4 / design 37).
	if decision.Compendium != testCompendium(rule.CropDefinitionID) {
		t.Fatalf("compendium=%+v", decision.Compendium)
	}
	// Cycle ends, plot returns to EMPTY, versions advance.
	cycle := candidate.State.FindCycle("cc-1")
	if !cycle.Ended || cycle.EndReason != CycleEndReasonHarvested {
		t.Fatalf("cycle=%+v", cycle)
	}
	plot := candidate.State.FindPlot(1)
	if plot.ActiveCropCycleID != "" || plot.PlotVersion != 3 {
		t.Fatalf("plot=%+v", plot)
	}
	if decision.CommittedFarmVersion != candidate.State.FarmVersion {
		t.Fatalf("farm version mismatch decision=%+v", decision)
	}
}

func TestDecideHarvestRejections(t *testing.T) {
	rule := validCropRule(50)
	mature := testNow + 60_000
	tests := []struct {
		name     string
		prepare  func(state FarmState) FarmState
		cmd      HarvestCommand
		now      int64
		wantCode Code
	}{
		{
			name: "friend cannot harvest",
			cmd: HarvestCommand{
				PlayerID: 2002, PlotID: 1, CycleID: "cc-1",
			},
			now:      mature,
			wantCode: CodeInvalidArgument,
		},
		{
			name: "immature harvest rejected",
			cmd: HarvestCommand{
				PlayerID: 1001, PlotID: 1, CycleID: "cc-1",
			},
			now:      mature - 1,
			wantCode: CodeCropNotMature,
		},
		{
			name: "double harvest rejected",
			prepare: func(state FarmState) FarmState {
				candidate, _, derr := DecideHarvest(state, HarvestCommand{
					PlayerID: 1001, PlotID: 1, CycleID: "cc-1",
				}, mature)
				mustOK(t, derr)
				return candidate.State
			},
			cmd: HarvestCommand{
				PlayerID: 1001, PlotID: 1, CycleID: "cc-1",
			},
			now:      mature + 1,
			wantCode: CodeCycleEnded,
		},
		{
			name: "old cycle id after replant",
			prepare: func(state FarmState) FarmState {
				candidate, _, derr := DecideHarvest(state, HarvestCommand{
					PlayerID: 1001, PlotID: 1, CycleID: "cc-1",
				}, mature)
				mustOK(t, derr)
				next, _ := plantOn(t, candidate.State, 1, "cc-2", mature+1, rule)
				return next
			},
			cmd: HarvestCommand{
				PlayerID: 1001, PlotID: 1, CycleID: "cc-1",
			},
			now:      mature + 2,
			wantCode: CodeCycleEnded,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestFarm(t)
			state, _ = plantOn(t, state, 1, "cc-1", testNow, rule)
			if tc.prepare != nil {
				state = tc.prepare(state)
			}
			_, _, derr := DecideHarvest(state, tc.cmd, tc.now)
			mustErr(t, derr, tc.wantCode, StatusRejected)
		})
	}
}

// TestMatureCycleNeverWithers proves a mature cycle stays harvestable
// across actor unload, restart or long offline gaps (design 32 §4.5).
func TestMatureCycleNeverWithers(t *testing.T) {
	rule := validCropRule(50)
	state := newTestFarm(t)
	state, _ = plantOn(t, state, 1, "cc-1", testNow, rule)
	longAfter := testNow + 365*24*3600*1000
	_, decision, derr := DecideHarvest(state, HarvestCommand{
		PlayerID: 1001, PlotID: 1, CycleID: "cc-1",
	}, longAfter)
	mustOK(t, derr)
	if decision.RewardQuantity != 50 {
		t.Fatalf("decision=%+v", decision)
	}
}
