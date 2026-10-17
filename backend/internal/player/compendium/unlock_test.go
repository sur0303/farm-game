package compendium

import (
	"sync"
	"testing"
)

func TestDecideUnlock_FirstUnlockProducesDelta(t *testing.T) {
	state := NewState()
	out := DecideUnlock(state, ownHarvestCmd(42, 1001, snap(1001, true, 42, "")), testLimits())
	if out.Status != StatusSucceeded {
		t.Fatalf("status = %s, want SUCCEEDED (err=%v)", out.Status, out.Err)
	}
	if !out.Delta.NewlyUnlocked || out.Delta.CropDefinitionID != 1001 || out.Delta.RevisionAfter != 1 {
		t.Fatalf("delta = %+v, want {true 1001 1}", out.Delta)
	}
	if out.Candidate.Revision != 1 || out.Candidate.UnlockedCount() != 1 {
		t.Fatalf("candidate = rev %d count %d, want rev 1 count 1",
			out.Candidate.Revision, out.Candidate.UnlockedCount())
	}
	fact, ok := out.Candidate.FactOf(1001)
	if !ok {
		t.Fatal("fact 1001 missing")
	}
	if fact.UnlockRevision != 1 || fact.CatalogVersionAtUnlock != 7 || fact.DefinitionVersionAtUnlock != 3 {
		t.Fatalf("fact provenance = %+v", fact)
	}
	if !out.Candidate.HasNewUnlock() {
		t.Fatal("red dot must derive from revision > seen_revision")
	}
	// Input state must be untouched.
	if state.UnlockedCount() != 0 || state.Revision != 0 {
		t.Fatal("input state mutated")
	}
}

func TestDecideUnlock_RepeatHarvestNoChange(t *testing.T) {
	state := unlockOne(t, NewState(), 42, 1001, "")
	out := DecideUnlock(state, ownHarvestCmd(42, 1001, snap(1001, true, 42, "")), testLimits())
	if out.Status != StatusNoChange {
		t.Fatalf("status = %s, want NO_CHANGE", out.Status)
	}
	if out.Delta.NewlyUnlocked || out.Delta.RevisionAfter != 1 {
		t.Fatalf("delta = %+v, want {false 0 1}", out.Delta)
	}
	// Revision must not grow on repeat harvest (docs/design/37 §5.2).
	if out.Candidate.Revision != 0 {
		t.Fatal("NO_CHANGE must not produce a candidate")
	}
}

// TestDecideUnlock_SourceMatrix covers the unique-unlock-condition table of
// docs/design/37 §3.1: only self-planted + self-harvested unlocks.
func TestDecideUnlock_SourceMatrix(t *testing.T) {
	cases := []struct {
		name string
		cmd  UnlockCommand
	}{
		{"stolen crop: planted_by != owner", UnlockCommand{
			PlayerID: 42, OwnerPlayerID: 42, CropDefinitionID: 1001,
			Snapshot: snap(1001, true, 7, ""), // planted by friend 7
		}},
		{"harvest decision of another player", UnlockCommand{
			PlayerID: 42, OwnerPlayerID: 7, CropDefinitionID: 1001,
			Snapshot: snap(1001, true, 7, ""),
		}},
		{"not collectible (mail/care/shop grants carry eligible=false)", UnlockCommand{
			PlayerID: 42, OwnerPlayerID: 42, CropDefinitionID: 1001,
			Snapshot: snap(1001, false, 42, ""),
		}},
		{"snapshot/decision crop mismatch", UnlockCommand{
			PlayerID: 42, OwnerPlayerID: 42, CropDefinitionID: 1001,
			Snapshot: snap(2002, true, 42, ""),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := DecideUnlock(NewState(), tc.cmd, testLimits())
			if out.Status != StatusNoChange {
				t.Fatalf("status = %s, want NO_CHANGE", out.Status)
			}
			if out.Delta.NewlyUnlocked {
				t.Fatal("non-self-harvest source must never unlock")
			}
			if out.Delta.RevisionAfter != 0 {
				t.Fatalf("revision moved to %d", out.Delta.RevisionAfter)
			}
		})
	}
}

// TestDecideUnlock_LegacySeedAfterArchive: an activity that archived with
// legacy_seed_plantable=true still freezes unlock_eligible=true at Plant;
// the later self-harvest unlocks by the frozen snapshot alone
// (docs/design/37 §4.2).
func TestDecideUnlock_LegacySeedAfterArchive(t *testing.T) {
	legacySnap := snap(3003, true, 42, "spring_fest")
	out := DecideUnlock(NewState(), ownHarvestCmd(42, 3003, legacySnap), testLimits())
	if out.Status != StatusSucceeded || !out.Delta.NewlyUnlocked {
		t.Fatalf("legacy-seed self harvest must unlock: %s %+v", out.Status, out.Delta)
	}
	fact, _ := out.Candidate.FactOf(3003)
	if fact.SourceActivityID != "spring_fest" {
		t.Fatalf("provenance activity = %q", fact.SourceActivityID)
	}
}

func TestDecideUnlock_RejectsInvalidInput(t *testing.T) {
	base := ownHarvestCmd(42, 1001, snap(1001, true, 42, ""))
	for _, tc := range []struct {
		name string
		mut  func(*UnlockCommand)
	}{
		{"zero player", func(c *UnlockCommand) { c.PlayerID = 0 }},
		{"zero owner", func(c *UnlockCommand) { c.OwnerPlayerID = 0 }},
		{"zero crop", func(c *UnlockCommand) { c.CropDefinitionID = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := base
			tc.mut(&cmd)
			out := DecideUnlock(NewState(), cmd, testLimits())
			if out.Status != StatusRejected || out.Err.Code != ErrInvalidArgument {
				t.Fatalf("got %s %v", out.Status, out.Err)
			}
		})
	}
}

func TestDecideUnlock_CapacityFailClosed(t *testing.T) {
	t.Run("max unlocked entries", func(t *testing.T) {
		limits := testLimits()
		limits.MaxUnlockedEntries = 1
		state := unlockOne(t, NewState(), 42, 1001, "")
		out := DecideUnlock(state, ownHarvestCmd(42, 1002, snap(1002, true, 42, "")), limits)
		if out.Status != StatusRejected || out.Err.Code != ErrWorkCapacityExceeded {
			t.Fatalf("got %s %v, want REJECTED/WORK_CAPACITY_EXCEEDED", out.Status, out.Err)
		}
	})
	t.Run("max state bytes", func(t *testing.T) {
		limits := testLimits()
		limits.MaxCompendiumStateBytes = 10 // below the fixed framing cost
		out := DecideUnlock(NewState(), ownHarvestCmd(42, 1001, snap(1001, true, 42, "")), limits)
		if out.Status != StatusRejected || out.Err.Code != ErrWorkCapacityExceeded {
			t.Fatalf("got %s %v", out.Status, out.Err)
		}
	})
	t.Run("invalid profile", func(t *testing.T) {
		out := DecideUnlock(NewState(), ownHarvestCmd(42, 1001, snap(1001, true, 42, "")), LimitProfile{})
		if out.Status != StatusRejected || out.Err.Code != ErrInvalidArgument {
			t.Fatalf("got %s %v", out.Status, out.Err)
		}
	})
}

// TestDecideUnlock_ConcurrentSamePlant simulates the PlayerActor mailbox
// serializing two harvests of the same yet-unlocked plant: exactly one delta
// reports newly_unlocked=true (docs/design/37 §6, acceptance 10).
func TestDecideUnlock_ConcurrentSamePlant(t *testing.T) {
	state := NewState()
	first := DecideUnlock(state, ownHarvestCmd(42, 1001, snap(1001, true, 42, "")), testLimits())
	second := DecideUnlock(first.Candidate, ownHarvestCmd(42, 1001, snap(1001, true, 42, "")), testLimits())
	if first.Status != StatusSucceeded || !first.Delta.NewlyUnlocked {
		t.Fatalf("first = %s %+v", first.Status, first.Delta)
	}
	if second.Status != StatusNoChange || second.Delta.NewlyUnlocked {
		t.Fatalf("second = %s %+v", second.Status, second.Delta)
	}
	if first.Delta.RevisionAfter != second.Delta.RevisionAfter {
		t.Fatal("both operations must report the same committed revision")
	}
}

// TestDecideUnlock_ReplayDeterminism: identical inputs must produce
// byte-identical outcomes (acceptance 2/11 domain premise).
func TestDecideUnlock_ReplayDeterminism(t *testing.T) {
	state := unlockOne(t, NewState(), 42, 1001, "")
	cmd := ownHarvestCmd(42, 2002, snap(2002, true, 42, "fest"))
	want := DecideUnlock(state, cmd, testLimits())
	for i := 0; i < 100; i++ {
		got := DecideUnlock(state, cmd, testLimits())
		if got.Status != want.Status || got.Delta != want.Delta {
			t.Fatalf("run %d diverged: %+v vs %+v", i, got, want)
		}
		if len(got.Candidate.Facts) != len(want.Candidate.Facts) {
			t.Fatal("candidate diverged")
		}
		for j := range got.Candidate.Facts {
			if got.Candidate.Facts[j] != want.Candidate.Facts[j] {
				t.Fatal("candidate facts diverged")
			}
		}
	}
}

// TestDecideUnlock_FactsStaySorted: membership stays a sorted deduped set
// regardless of unlock order (24 plots of different crops dedupe stably,
// acceptance 4).
func TestDecideUnlock_FactsStaySorted(t *testing.T) {
	state := NewState()
	for _, crop := range []uint32{50, 3, 17, 900, 1, 42} {
		state = unlockOne(t, state, 42, crop, "")
	}
	if state.Revision != 6 || state.UnlockedCount() != 6 {
		t.Fatalf("rev %d count %d", state.Revision, state.UnlockedCount())
	}
	revs := make([]uint64, 0, len(state.Facts))
	for i := 1; i < len(state.Facts); i++ {
		if state.Facts[i-1].CropDefinitionID >= state.Facts[i].CropDefinitionID {
			t.Fatalf("facts not sorted at %d: %+v", i, state.Facts)
		}
	}
	for _, f := range state.Facts {
		revs = append(revs, f.UnlockRevision)
	}
	for i := 1; i < len(revs); i++ {
		for j := i; j > 0 && revs[j] < revs[j-1]; j-- {
			revs[j], revs[j-1] = revs[j-1], revs[j]
		}
	}
	for i, r := range revs {
		if r != uint64(i+1) {
			t.Fatalf("unlock revisions = %v, want a permutation of 1..6", revs)
		}
	}
	// Re-harvest of any of them is a pure NO_CHANGE.
	out := DecideUnlock(state, ownHarvestCmd(42, 17, snap(17, true, 42, "")), testLimits())
	if out.Status != StatusNoChange || out.Delta.RevisionAfter != 6 {
		t.Fatalf("re-harvest = %s %+v", out.Status, out.Delta)
	}
}

// TestConcurrentDomainCallsRaceFree proves the domain holds no shared
// mutable state: many goroutines may evaluate transitions over the same
// immutable inputs; the mailbox remains the only serialization point.
func TestConcurrentDomainCallsRaceFree(t *testing.T) {
	state := unlockOne(t, NewState(), 42, 1001, "")
	catalog := CatalogSnapshot{
		CatalogVersion: 1,
		Entries:        []PlantCatalogEntry{normalEntry(1001)},
	}
	cmd := ownHarvestCmd(42, 2002, snap(2002, true, 42, ""))
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = DecideUnlock(state, cmd, testLimits())
			_ = AckSeen(state, uint64(i))
			if _, err := BuildPageView(state, catalog, testLimits(), ""); err != nil {
				t.Errorf("view build: %v", err)
			}
		}(i)
	}
	wg.Wait()
}
