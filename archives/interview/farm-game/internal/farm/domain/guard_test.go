package domain

import "testing"

func activeGuard(probabilityBP uint32, untilMs int64) GuardSnapshot {
	return GuardSnapshot{
		SchemaVersion:    1,
		GuardVersion:     5,
		ActiveGuardPetID: 7,
		GuardUntilMs:     untilMs,
		Rule: GuardRule{
			PetConfigVersion: 3, GuardRuleVersion: 2,
			TriggerProbabilityBP: probabilityBP, InterceptPolicy: InterceptPolicyFullGross,
			RandomAlgorithmVersion: 1, RandomKeyVersion: 1,
		},
	}
}

func TestGuardEffectivenessBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		guard GuardSnapshot
		now   int64
		want  bool
	}{
		{name: "no pet selected", guard: GuardSnapshot{GuardUntilMs: testNow + 1000}, now: testNow, want: false},
		{name: "active before expiry", guard: activeGuard(5000, testNow+1000), now: testNow, want: true},
		{name: "at expiry boundary ineffective", guard: activeGuard(5000, testNow), now: testNow, want: false},
		{name: "after expiry ineffective", guard: activeGuard(5000, testNow-1), now: testNow, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.guard.Effective(tc.now); got != tc.want {
				t.Fatalf("effective=%v want %v", got, tc.want)
			}
		})
	}
}

func TestDecideGuardInterception(t *testing.T) {
	const (
		gross     = 5
		unitValue = 3
		until     = testNow + 3600_000
	)
	tests := []struct {
		name        string
		guard       GuardSnapshot
		drawBP      uint32
		wantTrigger bool
		wantLost    uint32
		wantCoin    uint64
		wantErr     Code
	}{
		{name: "guard inactive no interception", guard: GuardSnapshot{}, drawBP: 0},
		{name: "guard expired no interception", guard: activeGuard(10_000, testNow-1), drawBP: 0},
		{name: "draw below probability triggers full interception", guard: activeGuard(5000, until), drawBP: 4999, wantTrigger: true, wantLost: gross, wantCoin: gross * unitValue},
		{name: "draw at probability does not trigger", guard: activeGuard(5000, until), drawBP: 5000},
		{name: "draw above probability does not trigger", guard: activeGuard(5000, until), drawBP: 9999},
		{name: "zero probability never triggers", guard: activeGuard(0, until), drawBP: 0},
		{name: "full probability always triggers", guard: activeGuard(10_000, until), drawBP: 9999, wantTrigger: true, wantLost: gross, wantCoin: gross * unitValue},
		{name: "draw out of range is invalid", guard: activeGuard(5000, until), drawBP: 10_000, wantErr: CodeInvalidArgument},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decision, derr := DecideGuardInterception(tc.guard, gross, unitValue, tc.drawBP, testNow)
			if tc.wantErr != "" {
				mustErr(t, derr, tc.wantErr, StatusRejected)
				return
			}
			mustOK(t, derr)
			if decision.GuardTriggered != tc.wantTrigger ||
				decision.GuardLostQuantity != tc.wantLost ||
				decision.GuardCoinValue != tc.wantCoin ||
				decision.RandomDrawBP != tc.drawBP {
				t.Fatalf("decision=%+v", decision)
			}
			// P0 binary rule: lost is either 0 or the full gross.
			if decision.GuardLostQuantity != 0 && decision.GuardLostQuantity != gross {
				t.Fatalf("partial interception leaked into P0: %+v", decision)
			}
		})
	}
}

// TestTheftGuardSettlement proves the settlement semantics of design 32
// §6.3-6.4: interception never touches the visitor wallet, only reduces the
// visitor's net crop intake; the owner is compensated with coins at the
// frozen unit value; a fully blocked attempt is still consumed.
func TestTheftGuardSettlement(t *testing.T) {
	rule := validCropRule(250) // 2% slot = 5, unit coin value = 3
	mature := testNow + 60_000

	t.Run("full interception", func(t *testing.T) {
		state := newTestFarm(t)
		state, _ = plantOn(t, state, 1, "cc-1", testNow, rule)
		state.Guard = activeGuard(5000, mature+3600_000)
		next, decision, derr := DecideTheft(state, TheftCommand{
			VisitorPlayerID: 2002, PlotID: 1, CycleID: "cc-1",
		}, 4999, mature)
		mustOK(t, derr)
		if decision.Outcome != TheftOutcomeBlockedByGuard ||
			decision.GrossStolenQuantity != 5 ||
			decision.Guard.GuardLostQuantity != 5 ||
			decision.NetReceivedQuantity != 0 ||
			decision.Guard.GuardCoinValue != 15 {
			t.Fatalf("decision=%+v", decision)
		}
		// stolen_amount advances by gross: intercepted crops do not stay on
		// the farm, they convert to owner coins.
		if next.State.FindCycle("cc-1").StolenAmount != 5 {
			t.Fatalf("stolen=%d", next.State.FindCycle("cc-1").StolenAmount)
		}
		// A fully blocked attempt is still the visitor's one legal attempt.
		_, _, derr = DecideTheft(next.State, TheftCommand{
			VisitorPlayerID: 2002, PlotID: 1, CycleID: "cc-1",
		}, 4999, mature+1)
		mustErr(t, derr, CodeStealAlreadyAttempted, StatusRejected)
	})

	t.Run("no interception", func(t *testing.T) {
		state := newTestFarm(t)
		state, _ = plantOn(t, state, 1, "cc-1", testNow, rule)
		state.Guard = activeGuard(5000, mature+3600_000)
		_, decision, derr := DecideTheft(state, TheftCommand{
			VisitorPlayerID: 2002, PlotID: 1, CycleID: "cc-1",
		}, 5000, mature)
		mustOK(t, derr)
		if decision.Outcome != TheftOutcomeStolen ||
			decision.Guard.GuardLostQuantity != 0 ||
			decision.NetReceivedQuantity != 5 ||
			decision.Guard.GuardCoinValue != 0 {
			t.Fatalf("decision=%+v", decision)
		}
	})

	t.Run("expired guard does not intercept", func(t *testing.T) {
		state := newTestFarm(t)
		state, _ = plantOn(t, state, 1, "cc-1", testNow, rule)
		state.Guard = activeGuard(10_000, mature-1)
		_, decision, derr := DecideTheft(state, TheftCommand{
			VisitorPlayerID: 2002, PlotID: 1, CycleID: "cc-1",
		}, 0, mature)
		mustOK(t, derr)
		if decision.Guard.GuardEffective || decision.Guard.GuardLostQuantity != 0 {
			t.Fatalf("decision=%+v", decision)
		}
	})
}

func TestApplyGuardSnapshot(t *testing.T) {
	newSnapshot := func(version uint64, petID uint32) GuardSnapshot {
		snapshot := activeGuard(5000, testNow+3600_000)
		snapshot.GuardVersion = version
		snapshot.ActiveGuardPetID = petID
		snapshot.SnapshotHash = CanonicalGuardSnapshotHash(snapshot)
		return snapshot
	}

	t.Run("higher version applies", func(t *testing.T) {
		state := newTestFarm(t)
		incoming := newSnapshot(6, 7)
		candidate, decision, derr := ApplyGuardSnapshot(state, 1001, 1001, incoming)
		mustOK(t, derr)
		if decision.Outcome != GuardApplyApplied || decision.AppliedGuardVersion != 6 ||
			decision.AppliedSnapshotHash != incoming.SnapshotHash {
			t.Fatalf("decision=%+v", decision)
		}
		// First install of pet 7 changes the public appearance (Switch).
		if !decision.AppearanceChanged {
			t.Fatalf("appearance change not detected")
		}
		if candidate.State.Guard != incoming {
			t.Fatalf("guard=%+v", candidate.State.Guard)
		}
	})

	t.Run("same version same hash replays", func(t *testing.T) {
		state := newTestFarm(t)
		state.Guard = newSnapshot(6, 7)
		_, decision, derr := ApplyGuardSnapshot(state, 1001, 1001, newSnapshot(6, 7))
		mustOK(t, derr)
		if decision.Outcome != GuardApplyReplayed {
			t.Fatalf("decision=%+v", decision)
		}
	})

	t.Run("lower version superseded", func(t *testing.T) {
		state := newTestFarm(t)
		state.Guard = newSnapshot(6, 7)
		candidate, decision, derr := ApplyGuardSnapshot(state, 1001, 1001, newSnapshot(5, 8))
		mustOK(t, derr)
		if decision.Outcome != GuardApplySuperseded || candidate.State.FarmID != 0 {
			t.Fatalf("decision=%+v candidate=%v", decision, candidate.State.FarmID)
		}
		if state.Guard.ActiveGuardPetID != 7 {
			t.Fatalf("superseded snapshot overwrote current")
		}
	})

	t.Run("same version different hash invariant violation", func(t *testing.T) {
		state := newTestFarm(t)
		state.Guard = newSnapshot(6, 7)
		conflicting := newSnapshot(6, 8)
		_, decision, derr := ApplyGuardSnapshot(state, 1001, 1001, conflicting)
		if derr == nil {
			t.Fatalf("expected invariant violation error")
		}
		if decision.Outcome != GuardApplyInvariantViolation {
			t.Fatalf("decision=%+v", decision)
		}
		if state.Guard.ActiveGuardPetID != 7 {
			t.Fatalf("conflicting snapshot overwrote current")
		}
	})

	t.Run("feed same pet keeps appearance, switch changes it", func(t *testing.T) {
		state := newTestFarm(t)
		state.Guard = newSnapshot(6, 7)
		feed := newSnapshot(7, 7)
		_, feedDecision, derr := ApplyGuardSnapshot(state, 1001, 1001, feed)
		mustOK(t, derr)
		if feedDecision.AppearanceChanged {
			t.Fatalf("feed must not change public appearance")
		}
		switchTo := newSnapshot(8, 9)
		_, switchDecision, derr := ApplyGuardSnapshot(state, 1001, 1001, switchTo)
		mustOK(t, derr)
		if !switchDecision.AppearanceChanged {
			t.Fatalf("real switch must change public appearance")
		}
	})

	t.Run("owner or farm mismatch rejected", func(t *testing.T) {
		state := newTestFarm(t)
		_, _, derr := ApplyGuardSnapshot(state, 2002, 1001, newSnapshot(6, 7))
		mustErr(t, derr, CodeGuardOwnerMismatch, StatusRejected)
		_, _, derr = ApplyGuardSnapshot(state, 1001, 2002, newSnapshot(6, 7))
		mustErr(t, derr, CodeGuardOwnerMismatch, StatusRejected)
	})
}

func TestCanonicalGuardSnapshotHashDeterministic(t *testing.T) {
	snapshot := activeGuard(5000, testNow+3600_000)
	first := CanonicalGuardSnapshotHash(snapshot)
	second := CanonicalGuardSnapshotHash(snapshot)
	if first != second || first == "" {
		t.Fatalf("hash not deterministic: %q vs %q", first, second)
	}
	// The hash never covers itself.
	snapshot.SnapshotHash = "anything"
	if CanonicalGuardSnapshotHash(snapshot) != first {
		t.Fatalf("hash must not cover itself")
	}
	// Any rule field participates.
	changed := activeGuard(5001, testNow+3600_000)
	if CanonicalGuardSnapshotHash(changed) == first {
		t.Fatalf("hash ignores rule changes")
	}
}
