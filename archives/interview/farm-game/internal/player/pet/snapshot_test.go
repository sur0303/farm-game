package pet

import (
	"sync"
	"testing"
)

func TestGuardSnapshotHash(t *testing.T) {
	cat := testCatalog()
	s := stateWithPets(t, cat, testPetDogID)
	s = mustSwitch(t, s, cat, testPetDogID)
	fed := Feed(s, cat, feedReq(cat, testPetDogID, 1), 1000, 10)
	syncIn := fed.GuardSync

	t.Run("stable across calls", func(t *testing.T) {
		if syncIn.Snapshot.Hash() != syncIn.SnapshotHash {
			t.Fatalf("hash not stable")
		}
	})

	t.Run("version participates in hash", func(t *testing.T) {
		other := syncIn.Snapshot
		other.GuardVersion++
		if other.Hash() == syncIn.SnapshotHash {
			t.Fatalf("guard_version must participate in hash")
		}
	})

	t.Run("same version different payload has different hash", func(t *testing.T) {
		// FarmActor INVARIANT_VIOLATION detection (docs/design/36 §6.4)
		// relies on this: identical guard_version, divergent payload.
		other := syncIn.Snapshot
		other.Rule.TriggerProbabilityBp++
		if other.Hash() == syncIn.SnapshotHash {
			t.Fatalf("payload divergence must change hash at same version")
		}
		other = syncIn.Snapshot
		other.GuardUntilMs++
		if other.Hash() == syncIn.SnapshotHash {
			t.Fatalf("guard_until_ms must participate in hash")
		}
		other = syncIn.Snapshot
		other.ActiveGuardPetID = testPetCatID
		if other.Hash() == syncIn.SnapshotHash {
			t.Fatalf("active_guard_pet_id must participate in hash")
		}
	})

	t.Run("schema version participates in hash", func(t *testing.T) {
		other := syncIn.Snapshot
		other.SchemaVersion++
		if other.Hash() == syncIn.SnapshotHash {
			t.Fatalf("schema_version must participate in hash")
		}
	})

	t.Run("snapshot carries no player-private fields", func(t *testing.T) {
		// Compile-time documentation of docs/design/36 §6.2: the snapshot
		// holds only what theft adjudication needs — no owner, wallet,
		// inventory, or owned set.
		snap := syncIn.Snapshot
		if snap.SchemaVersion != GuardSnapshotSchemaVersion ||
			snap.GuardVersion != fed.Candidate.GuardVersion ||
			snap.ActiveGuardPetID != testPetDogID ||
			snap.Rule != *fed.Candidate.GuardRuleSnapshot {
			t.Fatalf("snapshot=%+v", snap)
		}
	})
}

func TestBuildGuardSyncInput(t *testing.T) {
	t.Run("unlock-only state does not sync", func(t *testing.T) {
		cat := testCatalog()
		res := UnlockByCoin(NewState(), cat, UnlockByCoinRequest{
			PetID: testPetDogID, ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 1,
		}, 100)
		if res.Status != StatusSucceeded {
			t.Fatalf("unlock failed")
		}
		if _, ok := BuildGuardSyncInput(res.Candidate); ok {
			t.Fatalf("guard_version==0 must not produce a sync input")
		}
	})

	t.Run("sync hash equals candidate state snapshot hash", func(t *testing.T) {
		cat := testCatalog()
		s := stateWithPets(t, cat, testPetDogID, testPetCatID)
		s = mustSwitch(t, s, cat, testPetDogID)
		fed := Feed(s, cat, feedReq(cat, testPetDogID, 1), 7, 10)

		fromState, ok := BuildGuardSyncInput(fed.Candidate)
		if !ok {
			t.Fatalf("expected sync input")
		}
		// Acceptance item 24: the hash embedded in the mutation result is
		// byte-identical to one rebuilt from the committed PetState — no
		// post-commit re-read of hot config can diverge.
		if fromState.SnapshotHash != fed.GuardSync.SnapshotHash {
			t.Fatalf("hash rebuilt from state differs from emitted hash")
		}
		if fromState.Snapshot.Rule != *fed.Candidate.GuardRuleSnapshot {
			t.Fatalf("rule snapshot differs between state and sync input")
		}
	})
}

// TestCandidateStateImmutability verifies mutation functions never alias or
// mutate their input: attackers of determinism like a shared owned-set slice
// would corrupt replay semantics.
func TestCandidateStateImmutability(t *testing.T) {
	cat := testCatalog()
	base := stateWithPets(t, cat, testPetDogID)
	base = mustSwitch(t, base, cat, testPetDogID)
	snapshotOfBase := base.Clone()

	fed := Feed(base, cat, feedReq(cat, testPetDogID, 1), 1000, 10)
	if fed.Status != StatusSucceeded {
		t.Fatalf("feed failed")
	}
	// Mutate every mutable reachable part of the candidate.
	fed.Candidate.OwnedPetIDs[0] = 999
	fed.Candidate.GuardRuleSnapshot.TriggerProbabilityBp = 1

	assertStateEqual(t, snapshotOfBase, base, "base state mutated via candidate")

	unlock := UnlockByCoin(base, cat, UnlockByCoinRequest{
		PetID: testPetCatID, ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 1,
	}, 1000)
	unlock.Candidate.OwnedPetIDs[len(unlock.Candidate.OwnedPetIDs)-1] = 12345
	if base.Owns(12345) {
		t.Fatalf("base owned set aliased by candidate")
	}
}

// TestConcurrentDomainCallsRaceFree runs unlock/feed/switch evaluations from
// many goroutines against independent clones of a shared base state. The
// domain layer holds no package-level mutable state, so under -race this
// proves the only serialization point is the PlayerActor mailbox.
func TestConcurrentDomainCallsRaceFree(t *testing.T) {
	cat := testCatalog()
	base := stateWithPets(t, cat, testPetDogID, testPetCatID)
	base = mustSwitch(t, base, cat, testPetDogID)
	baseHash := func() [32]byte {
		in, ok := BuildGuardSyncInput(base)
		if !ok {
			t.Fatalf("base has no sync input")
		}
		return in.SnapshotHash
	}()

	const workers = 32
	const iterations = 50

	var wg sync.WaitGroup
	errs := make(chan string, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				s := base.Clone()
				switch w % 4 {
				case 0:
					res := Feed(s, cat, feedReq(cat, testPetDogID, 1), int64(i), 10)
					if res.Status != StatusSucceeded {
						errs <- "feed failed"
						return
					}
				case 1:
					res := Switch(s, cat, SwitchRequest{TargetPetID: testPetCatID, ExpectedPetConfigVersion: 1, ExpectedGuardRuleVersion: 2})
					if res.Status != StatusSucceeded {
						errs <- "switch failed"
						return
					}
				case 2:
					res := UnlockByCoin(s, cat, UnlockByCoinRequest{PetID: testPetCatID, ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 1}, 1000)
					if res.Status != StatusNoChange && res.Status != StatusSucceeded {
						errs <- "unlock failed"
						return
					}
				case 3:
					_ = s.GuardEffective(int64(i))
					in, ok := BuildGuardSyncInput(s)
					if !ok || in.SnapshotHash != baseHash {
						errs <- "base sync hash changed"
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}

	// The shared base must be byte-identical after the storm.
	in, _ := BuildGuardSyncInput(base)
	if in.SnapshotHash != baseHash {
		t.Fatalf("shared base state mutated by concurrent evaluations")
	}
	if !base.Owns(testPetDogID) || base.ActiveGuardPetID != testPetDogID {
		t.Fatalf("shared base content changed")
	}
}
