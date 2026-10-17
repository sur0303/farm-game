package domain

import "testing"

func TestNewFarmStateGenesis(t *testing.T) {
	state := newTestFarm(t)
	if len(state.Plots) != P0MaxPlots {
		t.Fatalf("plots=%d want %d", len(state.Plots), P0MaxPlots)
	}
	for i, plot := range state.Plots {
		plotID := uint32(i) + 1
		if plot.PlotID != plotID {
			t.Fatalf("slot %d has plot_id=%d", i, plot.PlotID)
		}
		wantUnlocked := plotID <= P0InitialUnlockedPlots
		if plot.Unlocked() != wantUnlocked {
			t.Fatalf("plot %d unlocked=%v want %v", plotID, plot.Unlocked(), wantUnlocked)
		}
		// Initial plots are free and never produce a purchase record
		// (design 32 §2A.1).
		if plot.UnlockWorkflowID != "" || plot.UnlockRuleVersion != 0 || plot.UnlockedAtMs != 0 {
			t.Fatalf("plot %d has unexpected purchase provenance", plotID)
		}
	}
}

func TestUnlockPlotFullSweep5To24(t *testing.T) {
	rule := validP0UnlockRule()
	state := newTestFarm(t)
	// Sequentially purchase plots 6..24; every purchase must succeed in
	// fixed order and advance plot/farm versions.
	for plotID := uint32(P0InitialUnlockedPlots + 1); plotID <= P0MaxPlots; plotID++ {
		plot := state.FindPlot(plotID)
		candidate, decision, derr := DecidePlotUnlock(state, UnlockPlotCommand{
			PlotID:                    plotID,
			ExpectedPlotVersion:       plot.PlotVersion,
			ExpectedUnlockRuleVersion: rule.RuleVersion,
			UnlockWorkflowID:          "wf-unlock",
		}, rule, testNow+int64(plotID))
		mustOK(t, derr)
		entry, _ := rule.EntryOf(plotID)
		if decision.UnlockOrder != plotID || decision.RequiredLevel != entry.RequiredLevel ||
			decision.NormalCoinPrice != entry.NormalCoinPrice || decision.AppliedRuleVersion != rule.RuleVersion {
			t.Fatalf("plot %d decision=%+v", plotID, decision)
		}
		if decision.UnlockedAtMs != testNow+int64(plotID) {
			t.Fatalf("plot %d unlocked_at=%d", plotID, decision.UnlockedAtMs)
		}
		next := candidate.State.FindPlot(plotID)
		if !next.Unlocked() || next.UnlockRuleVersion != rule.RuleVersion ||
			next.UnlockWorkflowID != "wf-unlock" || next.PlotVersion != plot.PlotVersion+1 {
			t.Fatalf("plot %d candidate=%+v", plotID, next)
		}
		if decision.CommittedPlotVersion != next.PlotVersion || decision.CommittedFarmVersion != candidate.State.FarmVersion {
			t.Fatalf("plot %d version mismatch decision=%+v", plotID, decision)
		}
		state = candidate.State
	}
	for _, plot := range state.Plots {
		if !plot.Unlocked() {
			t.Fatalf("plot %d still locked after full sweep", plot.PlotID)
		}
	}
}

func TestUnlockPlotRejections(t *testing.T) {
	rule := validP0UnlockRule()
	tests := []struct {
		name       string
		cmd        UnlockPlotCommand
		prepare    func(state FarmState) FarmState
		wantCode   Code
		wantStatus Status
	}{
		{
			name: "skip order plot 7 before 6",
			cmd: UnlockPlotCommand{
				PlotID:                    7,
				ExpectedUnlockRuleVersion: rule.RuleVersion,
			},
			wantCode:   CodePreviousPlotLocked,
			wantStatus: StatusRejected,
		},
		{
			name: "initial plot 5 already unlocked",
			cmd: UnlockPlotCommand{
				PlotID:                    5,
				ExpectedUnlockRuleVersion: rule.RuleVersion,
			},
			wantCode:   CodeAlreadyUnlocked,
			wantStatus: StatusNoChange,
		},
		{
			name: "stale rule version",
			cmd: UnlockPlotCommand{
				PlotID:                    6,
				ExpectedUnlockRuleVersion: rule.RuleVersion - 1,
			},
			wantCode:   CodeRuleVersionChanged,
			wantStatus: StatusRejected,
		},
		{
			name: "stale plot version",
			cmd: UnlockPlotCommand{
				PlotID:                    6,
				ExpectedPlotVersion:       99,
				ExpectedUnlockRuleVersion: rule.RuleVersion,
			},
			wantCode:   CodeStalePlotVersion,
			wantStatus: StatusRejected,
		},
		{
			name: "unknown plot",
			cmd: UnlockPlotCommand{
				PlotID:                    25,
				ExpectedUnlockRuleVersion: rule.RuleVersion,
			},
			wantCode:   CodePlotNotFound,
			wantStatus: StatusRejected,
		},
		{
			name: "already unlocked by another operation",
			prepare: func(state FarmState) FarmState {
				candidate, _, derr := DecidePlotUnlock(state, UnlockPlotCommand{PlotID: 6}, rule, testNow)
				mustOK(t, derr)
				return candidate.State
			},
			cmd:        UnlockPlotCommand{PlotID: 6, ExpectedUnlockRuleVersion: rule.RuleVersion},
			wantCode:   CodeAlreadyUnlocked,
			wantStatus: StatusNoChange,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestFarm(t)
			if tc.prepare != nil {
				state = tc.prepare(state)
			}
			_, _, derr := DecidePlotUnlock(state, tc.cmd, rule, testNow)
			mustErr(t, derr, tc.wantCode, tc.wantStatus)
		})
	}
}

func TestCheckUnlockEligibility(t *testing.T) {
	decision := PlotUnlockDecision{RequiredLevel: 3, NormalCoinPrice: 300}
	tests := []struct {
		name      string
		level     uint32
		coin      uint64
		wantCode  Code
		wantError bool
	}{
		{name: "eligible", level: 3, coin: 300},
		{name: "above requirement", level: 9, coin: 301},
		{name: "level too low", level: 2, coin: 300, wantCode: CodeLevelRequirementNotMet, wantError: true},
		{name: "coin too low", level: 3, coin: 299, wantCode: CodeInsufficientCoin, wantError: true},
		{name: "level checked first", level: 1, coin: 0, wantCode: CodeLevelRequirementNotMet, wantError: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			derr := CheckUnlockEligibility(decision, tc.level, tc.coin)
			if !tc.wantError {
				mustOK(t, derr)
				return
			}
			mustErr(t, derr, tc.wantCode, StatusRejected)
		})
	}
}

// TestDecidePlotUnlockDeterministic proves the transition is pure: identical
// inputs produce identical outputs and the input state is never mutated.
func TestDecidePlotUnlockDeterministic(t *testing.T) {
	rule := validP0UnlockRule()
	state := newTestFarm(t)
	before := state.Clone()
	cmd := UnlockPlotCommand{PlotID: 6, UnlockWorkflowID: "wf-1"}
	first, firstDecision, firstErr := DecidePlotUnlock(state, cmd, rule, testNow)
	second, secondDecision, secondErr := DecidePlotUnlock(state, cmd, rule, testNow)
	mustOK(t, firstErr)
	mustOK(t, secondErr)
	if firstDecision != secondDecision {
		t.Fatalf("decisions diverge: %+v vs %+v", firstDecision, secondDecision)
	}
	if !farmStateEqual(first.State, second.State) {
		t.Fatalf("candidates diverge")
	}
	if !farmStateEqual(state, before) {
		t.Fatalf("input state mutated")
	}
}

// farmStateEqual is a test helper comparing two states field by field.
func farmStateEqual(a, b FarmState) bool {
	if a.FarmID != b.FarmID || a.OwnerPlayerID != b.OwnerPlayerID || a.FarmVersion != b.FarmVersion ||
		len(a.Plots) != len(b.Plots) || len(a.Cycles) != len(b.Cycles) || a.Header != b.Header ||
		a.Guard != b.Guard {
		return false
	}
	for i := range a.Plots {
		if a.Plots[i] != b.Plots[i] {
			return false
		}
	}
	for i := range a.Cycles {
		if !cropCycleEqual(&a.Cycles[i], &b.Cycles[i]) {
			return false
		}
	}
	return true
}

func cropCycleEqual(a, b *CropCycle) bool {
	if a.CycleID != b.CycleID || a.PlotID != b.PlotID || a.CropDefinitionID != b.CropDefinitionID ||
		a.RuleVersion != b.RuleVersion || a.PlantAtMs != b.PlantAtMs || a.MatureAtMs != b.MatureAtMs ||
		a.BaseYield != b.BaseYield || a.StealableLimit != b.StealableLimit || a.HarvestExp != b.HarvestExp ||
		a.CropUnitCoinValue != b.CropUnitCoinValue || a.StolenAmount != b.StolenAmount ||
		a.NextStealSlotIndex != b.NextStealSlotIndex || a.Ended != b.Ended || a.EndReason != b.EndReason ||
		a.StealPlan.PolicyVersion != b.StealPlan.PolicyVersion ||
		a.StealPlan.TotalCapRatioBP != b.StealPlan.TotalCapRatioBP ||
		a.CarePlan != b.CarePlan || a.Compendium != b.Compendium ||
		len(a.Incidents) != len(b.Incidents) || len(a.Attempts) != len(b.Attempts) ||
		len(a.StealPlan.SlotRatioBP) != len(b.StealPlan.SlotRatioBP) ||
		len(a.StealPlan.SlotQuantity) != len(b.StealPlan.SlotQuantity) {
		return false
	}
	for i := range a.Incidents {
		if a.Incidents[i] != b.Incidents[i] {
			return false
		}
	}
	for i := range a.Attempts {
		if a.Attempts[i] != b.Attempts[i] {
			return false
		}
	}
	for i := range a.StealPlan.SlotRatioBP {
		if a.StealPlan.SlotRatioBP[i] != b.StealPlan.SlotRatioBP[i] ||
			a.StealPlan.SlotQuantity[i] != b.StealPlan.SlotQuantity[i] {
			return false
		}
	}
	return true
}
