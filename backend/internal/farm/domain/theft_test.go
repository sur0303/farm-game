package domain

import "testing"

func TestDecideTheftRejectsImmatureAndKeepsAttempt(t *testing.T) {
	rule := validCropRule(50)
	state := newTestFarm(t)
	state, plant := plantOn(t, state, 1, "cc-1", testNow, rule)

	// Immature theft rejects without consuming the visitor's attempt
	// (design 32 §6.6).
	_, _, derr := DecideTheft(state, TheftCommand{
		VisitorPlayerID: 2002, PlotID: 1, CycleID: "cc-1",
	}, 9999, plant.MatureAtMs-1)
	mustErr(t, derr, CodeCropNotMature, StatusRejected)
	if state.FindCycle("cc-1").HasAttempt(2002) {
		t.Fatalf("immature rejection consumed the attempt")
	}

	// After maturity the same visitor can still use their one attempt.
	next, decision, derr := DecideTheft(state, TheftCommand{
		VisitorPlayerID: 2002, PlotID: 1, CycleID: "cc-1",
	}, 9999, plant.MatureAtMs)
	mustOK(t, derr)
	if decision.GrossStolenQuantity != 1 || decision.NetReceivedQuantity != 1 ||
		decision.Outcome != TheftOutcomeStolen || decision.StealSlotIndex != 0 ||
		decision.AppliedSlotRatioBP != 200 {
		t.Fatalf("decision=%+v", decision)
	}
	if !next.State.FindCycle("cc-1").HasAttempt(2002) {
		t.Fatalf("attempt not recorded")
	}
}

func TestDecideTheftOneAttemptPerVisitorPerCycle(t *testing.T) {
	rule := validCropRule(50)
	state := newTestFarm(t)
	state, plant := plantOn(t, state, 1, "cc-1", testNow, rule)
	state, _ = stealFrom(t, state, 2002, 1, "cc-1", 9999, plant.MatureAtMs)

	_, _, derr := DecideTheft(state, TheftCommand{
		VisitorPlayerID: 2002, PlotID: 1, CycleID: "cc-1",
	}, 9999, plant.MatureAtMs+1)
	mustErr(t, derr, CodeStealAlreadyAttempted, StatusRejected)
}

// TestDecideTheftTenSlotsThenNoQuota drives ten different friends through
// the ten 2% slots: cumulative theft is exactly 20%, the owner keeps 80%,
// and the eleventh attempt is NO_CHANGE (design 32 Q32-06A).
func TestDecideTheftTenSlotsThenNoQuota(t *testing.T) {
	rule := validCropRule(250)
	state := newTestFarm(t)
	state, plant := plantOn(t, state, 1, "cc-1", testNow, rule)

	for i := uint64(0); i < 10; i++ {
		visitor := uint64(2000) + i
		next, decision, derr := DecideTheft(state, TheftCommand{
			VisitorPlayerID: visitor, PlotID: 1, CycleID: "cc-1",
		}, 9999, plant.MatureAtMs+int64(i))
		mustOK(t, derr)
		if decision.StealSlotIndex != uint32(i) || decision.GrossStolenQuantity != 5 {
			t.Fatalf("visitor %d decision=%+v", visitor, decision)
		}
		state = next.State
	}
	cycle := state.FindCycle("cc-1")
	if cycle.StolenAmount != 50 || cycle.StolenAmount != cycle.StealableLimit ||
		cycle.StolenAmount*5 != cycle.BaseYield {
		t.Fatalf("stolen=%d limit=%d base=%d", cycle.StolenAmount, cycle.StealableLimit, cycle.BaseYield)
	}
	if cycle.OwnerClaimable() != 200 {
		t.Fatalf("owner claimable=%d want 200 (80%%)", cycle.OwnerClaimable())
	}
	if len(cycle.Attempts) != 10 {
		t.Fatalf("attempts=%d", len(cycle.Attempts))
	}

	// The eleventh distinct friend gets NO_CHANGE / NO_STEALABLE_QUOTA.
	_, _, derr := DecideTheft(state, TheftCommand{
		VisitorPlayerID: 3000, PlotID: 1, CycleID: "cc-1",
	}, 9999, plant.MatureAtMs+100)
	mustErr(t, derr, CodeNoStealableQuota, StatusNoChange)
}

// TestHarvestTheftOrdering proves the FarmActor turn order fully explains
// the outcome either way and production never exceeds base_yield
// (design 32 §10.2).
func TestHarvestTheftOrdering(t *testing.T) {
	rule := validCropRule(250)
	mature := testNow + 60_000

	t.Run("theft first then owner harvests remainder", func(t *testing.T) {
		state := newTestFarm(t)
		state, _ = plantOn(t, state, 1, "cc-1", testNow, rule)
		state, theft := stealFrom(t, state, 2002, 1, "cc-1", 9999, mature)
		if theft.GrossStolenQuantity != 5 {
			t.Fatalf("theft=%+v", theft)
		}
		candidate, harvest, derr := DecideHarvest(state, HarvestCommand{
			PlayerID: 1001, PlotID: 1, CycleID: "cc-1",
		}, mature+1)
		mustOK(t, derr)
		if harvest.RewardQuantity != 245 || harvest.StolenAmount != 5 {
			t.Fatalf("harvest=%+v", harvest)
		}
		// Invariant: owner + all gross stolen == base_yield.
		if harvest.RewardQuantity+harvest.StolenAmount != 250 {
			t.Fatalf("overproduction: %+v", harvest)
		}
		_ = candidate
	})

	t.Run("harvest first then theft rejected", func(t *testing.T) {
		state := newTestFarm(t)
		state, _ = plantOn(t, state, 1, "cc-1", testNow, rule)
		candidate, _, derr := DecideHarvest(state, HarvestCommand{
			PlayerID: 1001, PlotID: 1, CycleID: "cc-1",
		}, mature)
		mustOK(t, derr)
		_, _, derr = DecideTheft(candidate.State, TheftCommand{
			VisitorPlayerID: 2002, PlotID: 1, CycleID: "cc-1",
		}, 9999, mature+1)
		mustErr(t, derr, CodeCycleEnded, StatusRejected)
	})
}

// TestTheftAttemptDoesNotLeakAcrossCycles proves B3: an attempt on the old
// cycle never blocks the same visitor on the next cycle of the same plot.
func TestTheftAttemptDoesNotLeakAcrossCycles(t *testing.T) {
	rule := validCropRule(250)
	mature := testNow + 60_000
	state := newTestFarm(t)
	state, _ = plantOn(t, state, 1, "cc-1", testNow, rule)
	state, _ = stealFrom(t, state, 2002, 1, "cc-1", 9999, mature)
	candidate, _, derr := DecideHarvest(state, HarvestCommand{
		PlayerID: 1001, PlotID: 1, CycleID: "cc-1",
	}, mature+1)
	mustOK(t, derr)
	state = candidate.State

	// Replant a new cycle on the same plot; the same friend may steal from
	// the new cycle exactly once.
	state, _ = plantOn(t, state, 1, "cc-2", mature+2, rule)
	next, decision, derr := DecideTheft(state, TheftCommand{
		VisitorPlayerID: 2002, PlotID: 1, CycleID: "cc-2",
	}, 9999, mature+2+60_000)
	mustOK(t, derr)
	if decision.CycleID != "cc-2" || decision.StealSlotIndex != 0 {
		t.Fatalf("decision=%+v", decision)
	}
	_, _, derr = DecideTheft(next.State, TheftCommand{
		VisitorPlayerID: 2002, PlotID: 1, CycleID: "cc-2",
	}, 9999, mature+2+60_001)
	mustErr(t, derr, CodeStealAlreadyAttempted, StatusRejected)
}

func TestDecideTheftRejections(t *testing.T) {
	rule := validCropRule(50)
	mature := testNow + 60_000
	tests := []struct {
		name     string
		cmd      TheftCommand
		wantCode Code
	}{
		{
			name:     "owner cannot steal own farm",
			cmd:      TheftCommand{VisitorPlayerID: 1001, PlotID: 1, CycleID: "cc-1"},
			wantCode: CodeVisitorIsOwner,
		},
		{
			name:     "plot not found",
			cmd:      TheftCommand{VisitorPlayerID: 2002, PlotID: 99, CycleID: "cc-1"},
			wantCode: CodePlotNotFound,
		},
		{
			name:     "old cycle id after replant",
			cmd:      TheftCommand{VisitorPlayerID: 2002, PlotID: 1, CycleID: "cc-old"},
			wantCode: CodeCycleMismatch,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newTestFarm(t)
			state, _ = plantOn(t, state, 1, "cc-1", testNow, rule)
			_, _, derr := DecideTheft(state, tc.cmd, 9999, mature)
			mustErr(t, derr, tc.wantCode, StatusRejected)
		})
	}
}

// TestDecideTheftDeterministic proves identical inputs (including the
// injected random draw) produce identical immutable decisions and never
// mutate the input state.
func TestDecideTheftDeterministic(t *testing.T) {
	rule := validCropRule(250)
	state := newTestFarm(t)
	state, plant := plantOn(t, state, 1, "cc-1", testNow, rule)
	state.Guard = GuardSnapshot{
		SchemaVersion:    1,
		GuardVersion:     9,
		ActiveGuardPetID: 7,
		GuardUntilMs:     plant.MatureAtMs + 3600_000,
		Rule: GuardRule{
			PetConfigVersion: 1, GuardRuleVersion: 2,
			TriggerProbabilityBP: 5000, InterceptPolicy: InterceptPolicyFullGross,
			RandomAlgorithmVersion: 1, RandomKeyVersion: 1,
		},
	}
	before := state.Clone()
	cmd := TheftCommand{VisitorPlayerID: 2002, PlotID: 1, CycleID: "cc-1"}
	first, firstDecision, firstErr := DecideTheft(state, cmd, 4999, plant.MatureAtMs)
	second, secondDecision, secondErr := DecideTheft(state, cmd, 4999, plant.MatureAtMs)
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
