package pet

import (
	"math"
	"testing"
)

// stateWithPets builds a state owning the given pets (versions start at the
// given values) without going through the unlock path.
func stateWithPets(t *testing.T, cat CatalogSnapshot, petIDs ...int32) State {
	t.Helper()
	s := NewState()
	for _, id := range petIDs {
		def, ok := cat.Find(id)
		if !ok {
			t.Fatalf("pet %d not in catalog", id)
		}
		res := ApplyTaskUnlockGrant(s, cat, TaskUnlockGrant{PetID: id, PetConfigVersion: def.PetConfigVersion, GrantDefinitionVersion: 1})
		if res.Status != StatusSucceeded {
			t.Fatalf("setup unlock pet %d: %v/%v", id, res.Status, res.ErrorCode)
		}
		s = res.Candidate
	}
	return s
}

func mustSwitch(t *testing.T, s State, cat CatalogSnapshot, petID int32) State {
	t.Helper()
	def, _ := cat.Find(petID)
	res := Switch(s, cat, SwitchRequest{
		OperationID:              "setup-switch",
		TargetPetID:              petID,
		ExpectedPetConfigVersion: def.PetConfigVersion,
		ExpectedGuardRuleVersion: def.GuardRule.GuardRuleVersion,
	})
	if res.Status != StatusSucceeded {
		t.Fatalf("setup switch pet %d: %v/%v", petID, res.Status, res.ErrorCode)
	}
	return res.Candidate
}

func feedReq(cat CatalogSnapshot, petID int32, quantity int64) FeedRequest {
	def, _ := cat.Find(petID)
	return FeedRequest{
		OperationID:                    "op-feed",
		Quantity:                       quantity,
		ExpectedFeedRuleVersion:        cat.FeedRule.FeedRuleVersion,
		ExpectedActivePetConfigVersion: def.PetConfigVersion,
		ExpectedGuardRuleVersion:       def.GuardRule.GuardRuleVersion,
	}
}

// ---------------------------------------------------------------- unlock

func TestUnlockByCoin_Success(t *testing.T) {
	cat := testCatalog()
	s := NewState()
	res := UnlockByCoin(s, cat, UnlockByCoinRequest{
		OperationID: "op-1", PetID: testPetDogID,
		ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 1,
	}, 1000)
	if res.Status != StatusSucceeded {
		t.Fatalf("status=%v err=%v", res.Status, res.ErrorCode)
	}
	if res.Fee.Amount != 100 || res.Fee.PriceVersion != 1 {
		t.Fatalf("fee=%+v", res.Fee)
	}
	if !res.Candidate.Owns(testPetDogID) {
		t.Fatalf("pet not unlocked in candidate")
	}
	if res.Candidate.PetStateVersion != s.PetStateVersion+1 {
		t.Fatalf("pet_state_version not bumped: %d -> %d", s.PetStateVersion, res.Candidate.PetStateVersion)
	}
	if res.Candidate.GuardVersion != s.GuardVersion {
		t.Fatalf("unlock must not bump guard_version")
	}
	if res.Candidate.HasActiveGuardPet() || res.Candidate.GuardEffective(0) {
		t.Fatalf("unlock must not select or arm the guard")
	}
	// Input state must be untouched.
	if s.Owns(testPetDogID) || len(s.OwnedPetIDs) != 0 {
		t.Fatalf("input state mutated")
	}
	if res.Receipt.PetID != testPetDogID || res.Receipt.CoinPrice != 100 || res.Receipt.OperationID != "op-1" {
		t.Fatalf("receipt=%+v", res.Receipt)
	}
}

// TestUnlockByCoin_ReplayedOperationChargesOnce simulates acceptance item 1:
// the same purchase operation replayed 100 times charges at most once. The
// domain is deterministic, and once the candidate is committed the replay is
// a NO_CHANGE with zero fee.
func TestUnlockByCoin_ReplayedOperationChargesOnce(t *testing.T) {
	cat := testCatalog()
	req := UnlockByCoinRequest{OperationID: "op-replay", PetID: testPetDogID, ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 1}

	s := NewState()
	totalCharged := int64(0)
	for i := 0; i < 100; i++ {
		res := UnlockByCoin(s, cat, req, 1000-totalCharged)
		switch res.Status {
		case StatusSucceeded:
			totalCharged += res.Fee.Amount
			s = res.Candidate
		case StatusNoChange:
			if res.ErrorCode != ErrPetAlreadyUnlocked || res.Fee.Amount != 0 {
				t.Fatalf("replay %d: NO_CHANGE must carry zero fee, got %+v", i, res)
			}
		default:
			t.Fatalf("replay %d: unexpected %v/%v", i, res.Status, res.ErrorCode)
		}
	}
	if totalCharged != 100 {
		t.Fatalf("charged %d, want exactly one price 100", totalCharged)
	}
	if s.PetStateVersion != 1 {
		t.Fatalf("pet_state_version=%d, want 1 (single real change)", s.PetStateVersion)
	}
}

func TestUnlockByCoin_RejectMatrix(t *testing.T) {
	cat := testCatalog()
	owned := stateWithPets(t, cat, testPetDogID)

	cases := []struct {
		name    string
		state   State
		req     UnlockByCoinRequest
		balance int64
		want    ErrorCode
		status  ActionStatus
	}{
		{"pet not in catalog", NewState(), UnlockByCoinRequest{PetID: 999, ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 1}, 1000, ErrPetNotFound, StatusRejected},
		{"config version stale", NewState(), UnlockByCoinRequest{PetID: testPetDogID, ExpectedPetConfigVersion: 99, ExpectedPriceVersion: 1}, 1000, ErrPetConfigVersionChanged, StatusRejected},
		{"delisted coin unlock", NewState(), UnlockByCoinRequest{PetID: testPetFoxID, ExpectedPetConfigVersion: 3, ExpectedPriceVersion: 0}, 1000, ErrPetNotAvailable, StatusRejected},
		{"price version stale", NewState(), UnlockByCoinRequest{PetID: testPetDogID, ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 99}, 1000, ErrPetPriceChanged, StatusRejected},
		{"already unlocked", owned, UnlockByCoinRequest{PetID: testPetDogID, ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 1}, 1000, ErrPetAlreadyUnlocked, StatusNoChange},
		{"insufficient coin", NewState(), UnlockByCoinRequest{PetID: testPetDogID, ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 1}, 99, ErrInsufficientCoin, StatusRejected},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.state.Clone()
			res := UnlockByCoin(tc.state, cat, tc.req, tc.balance)
			if res.Status != tc.status || res.ErrorCode != tc.want {
				t.Fatalf("got %v/%v, want %v/%v", res.Status, res.ErrorCode, tc.status, tc.want)
			}
			if res.Fee.Amount != 0 {
				t.Fatalf("rejected/no-change unlock must not emit a fee")
			}
			assertStateEqual(t, before, tc.state, "input state mutated")
		})
	}
}

// TestUnlockByCoin_OwnedSetCap is defense in depth behind the publish-time
// 32-pet limit: even a (contract-illegal) 33-pet catalog cannot grow a full
// owned set.
func TestUnlockByCoin_OwnedSetCap(t *testing.T) {
	cat := CatalogSnapshot{CatalogVersion: 1, FeedRule: validFeedRule()}
	s := NewState()
	for i := int32(1); i <= MaxOwnedPets; i++ {
		cat.Pets = append(cat.Pets, validDefinition(i))
		s.OwnedPetIDs = append(s.OwnedPetIDs, i)
	}
	cat.Pets = append(cat.Pets, validDefinition(MaxOwnedPets+1))
	res := UnlockByCoin(s, cat, UnlockByCoinRequest{
		PetID: MaxOwnedPets + 1, ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 1,
	}, math.MaxInt64)
	if res.Status != StatusRejected || res.ErrorCode != ErrPetNotAvailable {
		t.Fatalf("got %v/%v, want REJECTED/PET_NOT_AVAILABLE", res.Status, res.ErrorCode)
	}
	if len(res.Candidate.OwnedPetIDs) > MaxOwnedPets {
		t.Fatalf("owned set exceeded %d", MaxOwnedPets)
	}
}

// ---------------------------------------------------------------- task grant

func TestApplyTaskUnlockGrant(t *testing.T) {
	cat := testCatalog()
	s := NewState()

	grant := TaskUnlockGrant{PetID: testPetFoxID, PetConfigVersion: 3, GrantDefinitionVersion: 1}
	res := ApplyTaskUnlockGrant(s, cat, grant)
	if res.Status != StatusSucceeded || !res.Changed {
		t.Fatalf("first grant: %v/%v changed=%v", res.Status, res.ErrorCode, res.Changed)
	}
	if !res.Candidate.Owns(testPetFoxID) || res.Candidate.PetStateVersion != 1 {
		t.Fatalf("candidate=%+v", res.Candidate)
	}

	// Delisted pets remain grantable by task (下架只停金币新增).
	// Same pet again: set-idempotent NoChange, no version bump, task still claims.
	again := ApplyTaskUnlockGrant(res.Candidate, cat, grant)
	if again.Status != StatusNoChange || again.Changed || again.ErrorCode != ErrPetAlreadyUnlocked {
		t.Fatalf("duplicate grant: %v/%v changed=%v", again.Status, again.ErrorCode, again.Changed)
	}
	if again.Candidate.PetStateVersion != res.Candidate.PetStateVersion {
		t.Fatalf("duplicate grant bumped pet_state_version")
	}

	// Coin unlock then task grant for the same pet: grant is NoChange.
	coin := UnlockByCoin(s, cat, UnlockByCoinRequest{PetID: testPetDogID, ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 1}, 1000)
	if coin.Status != StatusSucceeded {
		t.Fatalf("coin unlock failed")
	}
	dogGrant := ApplyTaskUnlockGrant(coin.Candidate, cat, TaskUnlockGrant{PetID: testPetDogID, PetConfigVersion: 1, GrantDefinitionVersion: 1})
	if dogGrant.Status != StatusNoChange || dogGrant.Changed {
		t.Fatalf("grant after coin unlock must be set-idempotent NoChange")
	}

	// Version mismatch / unknown pet rejected.
	if r := ApplyTaskUnlockGrant(s, cat, TaskUnlockGrant{PetID: testPetFoxID, PetConfigVersion: 99, GrantDefinitionVersion: 1}); r.ErrorCode != ErrPetConfigVersionChanged {
		t.Fatalf("stale grant version: %v", r.ErrorCode)
	}
	if r := ApplyTaskUnlockGrant(s, cat, TaskUnlockGrant{PetID: 777, PetConfigVersion: 1, GrantDefinitionVersion: 1}); r.ErrorCode != ErrPetNotFound {
		t.Fatalf("unknown grant pet: %v", r.ErrorCode)
	}
}

// ---------------------------------------------------------------- switch

func TestSwitch_SuccessPreservesGuardUntil(t *testing.T) {
	cat := testCatalog()
	s := stateWithPets(t, cat, testPetDogID, testPetCatID)
	s = mustSwitch(t, s, cat, testPetDogID)

	now := int64(1_000_000)
	fed := Feed(s, cat, feedReq(cat, testPetDogID, 1), now, 10)
	if fed.Status != StatusSucceeded {
		t.Fatalf("feed: %v/%v", fed.Status, fed.ErrorCode)
	}
	before := fed.Candidate

	res := Switch(before, cat, SwitchRequest{
		OperationID: "op-sw", TargetPetID: testPetCatID,
		ExpectedPetConfigVersion: 1, ExpectedGuardRuleVersion: 2,
	})
	if res.Status != StatusSucceeded {
		t.Fatalf("switch: %v/%v", res.Status, res.ErrorCode)
	}
	c := res.Candidate
	if c.ActiveGuardPetID != testPetCatID {
		t.Fatalf("active=%d", c.ActiveGuardPetID)
	}
	if c.GuardUntilMs != before.GuardUntilMs {
		t.Fatalf("guard_until changed on switch: %d -> %d", before.GuardUntilMs, c.GuardUntilMs)
	}
	if c.PetStateVersion != before.PetStateVersion+1 || c.GuardVersion != before.GuardVersion+1 {
		t.Fatalf("versions (%d,%d) -> (%d,%d)", before.PetStateVersion, before.GuardVersion, c.PetStateVersion, c.GuardVersion)
	}
	if c.GuardRuleSnapshot == nil || c.GuardRuleSnapshot.GuardRuleVersion != 2 || c.GuardRuleSnapshot.TriggerProbabilityBp != 10000 {
		t.Fatalf("rule snapshot not frozen from cat definition: %+v", c.GuardRuleSnapshot)
	}
	if res.GuardSync.GuardVersion != c.GuardVersion {
		t.Fatalf("sync input version=%d, want %d", res.GuardSync.GuardVersion, c.GuardVersion)
	}
	if res.GuardSync.SnapshotHash != res.GuardSync.Snapshot.Hash() {
		t.Fatalf("sync hash mismatch")
	}
	if res.Receipt.PreviousActiveGuardPetID != testPetDogID || res.Receipt.ResultingActiveGuardPetID != testPetCatID {
		t.Fatalf("receipt=%+v", res.Receipt)
	}
	// Guard remains effective with the remaining time after the switch.
	if !c.GuardEffective(now + testHourMs) {
		t.Fatalf("guard should remain effective after switch")
	}
}

func TestSwitch_Matrix(t *testing.T) {
	cat := testCatalog()
	s := stateWithPets(t, cat, testPetDogID, testPetFoxID)
	s = mustSwitch(t, s, cat, testPetDogID)
	pv, gv := s.PetStateVersion, s.GuardVersion

	cases := []struct {
		name   string
		req    SwitchRequest
		status ActionStatus
		want   ErrorCode
	}{
		{"unknown pet", SwitchRequest{TargetPetID: 999, ExpectedPetConfigVersion: 1, ExpectedGuardRuleVersion: 1}, StatusRejected, ErrPetNotFound},
		{"config version stale", SwitchRequest{TargetPetID: testPetFoxID, ExpectedPetConfigVersion: 99, ExpectedGuardRuleVersion: 1}, StatusRejected, ErrPetConfigVersionChanged},
		{"guard rule version stale", SwitchRequest{TargetPetID: testPetFoxID, ExpectedPetConfigVersion: 3, ExpectedGuardRuleVersion: 99}, StatusRejected, ErrPetGuardRuleChanged},
		{"unowned pet", SwitchRequest{TargetPetID: testPetCatID, ExpectedPetConfigVersion: 1, ExpectedGuardRuleVersion: 2}, StatusRejected, ErrPetNotUnlocked},
		{"already active", SwitchRequest{TargetPetID: testPetDogID, ExpectedPetConfigVersion: 1, ExpectedGuardRuleVersion: 1}, StatusNoChange, ErrPetAlreadyActive},
		{"delisted but owned", SwitchRequest{TargetPetID: testPetFoxID, ExpectedPetConfigVersion: 3, ExpectedGuardRuleVersion: 1}, StatusSucceeded, ErrNone},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := Switch(s, cat, tc.req)
			if res.Status != tc.status || res.ErrorCode != tc.want {
				t.Fatalf("got %v/%v, want %v/%v", res.Status, res.ErrorCode, tc.status, tc.want)
			}
			if tc.status != StatusSucceeded {
				if res.GuardSync.GuardVersion != 0 {
					t.Fatalf("non-success switch must not emit GuardSync input")
				}
				assertStateEqual(t, State{OwnedPetIDs: s.OwnedPetIDs, ActiveGuardPetID: s.ActiveGuardPetID, GuardUntilMs: s.GuardUntilMs, PetStateVersion: pv, GuardVersion: gv, GuardRuleSnapshot: s.GuardRuleSnapshot}, s, "state changed on non-success switch")
			}
		})
	}
}

// ---------------------------------------------------------------- feed

// TestFeed_DurationMatrix covers the 8h/24h boundary matrix
// (docs/design/36 §4.1): accept iff old_until_or_now + q*8h <= now+24h.
func TestFeed_DurationMatrix(t *testing.T) {
	cat := testCatalog()
	now := int64(10_000_000)

	cases := []struct {
		name        string
		guardUntil  int64 // relative to now: now+delta; 0 means never fed
		quantity    int64
		wantStatus  ActionStatus
		wantErr     ErrorCode
		wantUntilMs int64
	}{
		{"never fed, 1 food", 0, 1, StatusSucceeded, ErrNone, now + 8*testHourMs},
		{"never fed, 3 food = exact 24h horizon", 0, 3, StatusSucceeded, ErrNone, now + 24*testHourMs},
		{"never fed, 4 food exceeds per-command max", 0, 4, StatusRejected, ErrPetFeedLimitExceeded, 0},
		{"16h remaining + 1 food = exact horizon", now + 16*testHourMs, 1, StatusSucceeded, ErrNone, now + 24*testHourMs},
		{"16h remaining + 2 food exceeds horizon", now + 16*testHourMs, 2, StatusRejected, ErrPetFeedLimitExceeded, 0},
		{"17h remaining + 1 food exceeds horizon", now + 17*testHourMs, 1, StatusRejected, ErrPetFeedLimitExceeded, 0},
		{"8h remaining + 2 food = exact horizon", now + 8*testHourMs, 2, StatusSucceeded, ErrNone, now + 24*testHourMs},
		{"1ms remaining + 2 food fits", now + 1, 2, StatusSucceeded, ErrNone, now + 16*testHourMs + 1},
		{"8h+1ms remaining + 2 food exceeds horizon by 1ms", now + 8*testHourMs + 1, 2, StatusRejected, ErrPetFeedLimitExceeded, 0},
		{"expired guard restarts from now (no backfill)", now - testHourMs, 1, StatusSucceeded, ErrNone, now + 8*testHourMs},
		{"exactly at deadline counts as expired", now, 2, StatusSucceeded, ErrNone, now + 16*testHourMs},
		{"zero quantity", 0, 0, StatusRejected, ErrPetFeedLimitExceeded, 0},
		{"negative quantity", 0, -1, StatusRejected, ErrPetFeedLimitExceeded, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := stateWithPets(t, cat, testPetDogID)
			s = mustSwitch(t, s, cat, testPetDogID)
			s.GuardUntilMs = tc.guardUntil

			res := Feed(s, cat, feedReq(cat, testPetDogID, tc.quantity), now, 100)
			if res.Status != tc.wantStatus || res.ErrorCode != tc.wantErr {
				t.Fatalf("got %v/%v, want %v/%v", res.Status, res.ErrorCode, tc.wantStatus, tc.wantErr)
			}
			if tc.wantStatus == StatusSucceeded {
				if res.Candidate.GuardUntilMs != tc.wantUntilMs {
					t.Fatalf("guard_until=%d, want %d", res.Candidate.GuardUntilMs, tc.wantUntilMs)
				}
				if res.Consumption.ItemID != testDogFoodItemID || res.Consumption.Quantity != tc.quantity {
					t.Fatalf("consumption=%+v", res.Consumption)
				}
				if res.Receipt.PreviousGuardUntilMs != tc.guardUntil || res.Receipt.ResultingGuardUntilMs != tc.wantUntilMs {
					t.Fatalf("receipt=%+v", res.Receipt)
				}
				if res.Candidate.PetStateVersion != s.PetStateVersion+1 || res.Candidate.GuardVersion != s.GuardVersion+1 {
					t.Fatalf("versions not bumped")
				}
			} else {
				// Fail closed: not a single dog food is consumed on rejection.
				if res.Consumption.Quantity != 0 {
					t.Fatalf("rejected feed emitted consumption %+v", res.Consumption)
				}
			}
		})
	}
}

func TestFeed_ValidationMatrix(t *testing.T) {
	cat := testCatalog()
	now := int64(10_000_000)

	t.Run("no active pet", func(t *testing.T) {
		s := stateWithPets(t, cat, testPetDogID) // unlocked but never selected
		res := Feed(s, cat, FeedRequest{Quantity: 1, ExpectedFeedRuleVersion: 1}, now, 10)
		if res.Status != StatusRejected || res.ErrorCode != ErrPetNotAvailable {
			t.Fatalf("got %v/%v", res.Status, res.ErrorCode)
		}
	})

	s := stateWithPets(t, cat, testPetDogID)
	s = mustSwitch(t, s, cat, testPetDogID)

	cases := []struct {
		name    string
		req     FeedRequest
		balance int64
		want    ErrorCode
	}{
		{"feed rule version stale", FeedRequest{Quantity: 1, ExpectedFeedRuleVersion: 99, ExpectedActivePetConfigVersion: 1, ExpectedGuardRuleVersion: 1}, 10, ErrPetFeedRuleChanged},
		{"pet config version stale", FeedRequest{Quantity: 1, ExpectedFeedRuleVersion: 1, ExpectedActivePetConfigVersion: 99, ExpectedGuardRuleVersion: 1}, 10, ErrPetConfigVersionChanged},
		{"guard rule version stale", FeedRequest{Quantity: 1, ExpectedFeedRuleVersion: 1, ExpectedActivePetConfigVersion: 1, ExpectedGuardRuleVersion: 99}, 10, ErrPetGuardRuleChanged},
		{"insufficient dog food", feedReq(cat, testPetDogID, 2), 1, ErrInsufficientDogFood},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := Feed(s, cat, tc.req, now, tc.balance)
			if res.Status != StatusRejected || res.ErrorCode != tc.want {
				t.Fatalf("got %v/%v, want REJECTED/%v", res.Status, res.ErrorCode, tc.want)
			}
			if res.Consumption.Quantity != 0 || res.GuardSync.GuardVersion != 0 {
				t.Fatalf("rejected feed must not emit consumption or sync")
			}
		})
	}
}

// TestFeed_CheckedArithmeticOverflow: with a (contract-illegal but
// structurally possible) huge per-command quantity cap, the multiplication
// must reject on overflow instead of wrapping to a small deadline.
func TestFeed_CheckedArithmeticOverflow(t *testing.T) {
	cat := testCatalog()
	cat.FeedRule.MaxQuantityPerCommand = math.MaxInt64
	s := stateWithPets(t, cat, testPetDogID)
	s = mustSwitch(t, s, cat, testPetDogID)

	res := Feed(s, cat, feedReq(cat, testPetDogID, math.MaxInt64), 0, math.MaxInt64)
	if res.Status != StatusRejected || res.ErrorCode != ErrPetFeedLimitExceeded {
		t.Fatalf("overflow must reject, got %v/%v until=%d", res.Status, res.ErrorCode, res.Candidate.GuardUntilMs)
	}
}

// TestFeed_SequentialCommandsSerialize models acceptance item 4: two feeds
// racing at the 24h boundary are serialized by the PlayerActor mailbox; the
// second recomputes from the committed deadline and is cleanly rejected with
// zero partial consumption.
func TestFeed_SequentialCommandsSerialize(t *testing.T) {
	cat := testCatalog()
	now := int64(0)
	s := stateWithPets(t, cat, testPetDogID)
	s = mustSwitch(t, s, cat, testPetDogID)

	first := Feed(s, cat, feedReq(cat, testPetDogID, 2), now, 10)
	if first.Status != StatusSucceeded || first.Candidate.GuardUntilMs != 16*testHourMs {
		t.Fatalf("first feed: %v until=%d", first.Status, first.Candidate.GuardUntilMs)
	}
	second := Feed(first.Candidate, cat, feedReq(cat, testPetDogID, 1), now, 10)
	if second.Status != StatusSucceeded || second.Candidate.GuardUntilMs != 24*testHourMs {
		t.Fatalf("second feed: %v until=%d", second.Status, second.Candidate.GuardUntilMs)
	}
	third := Feed(second.Candidate, cat, feedReq(cat, testPetDogID, 1), now, 10)
	if third.Status != StatusRejected || third.ErrorCode != ErrPetFeedLimitExceeded || third.Consumption.Quantity != 0 {
		t.Fatalf("third feed must reject without partial consumption: %v/%v", third.Status, third.ErrorCode)
	}
	if second.Candidate.GuardVersion != first.Candidate.GuardVersion+1 {
		t.Fatalf("guard_version must increment per accepted feed")
	}
}

// TestFeed_ReplayDeterminism: the same (state, request, trusted_now) always
// yields the byte-identical decision, including the snapshot hash — the
// property that lets the Actor layer safely dedupe by operation_id.
func TestFeed_ReplayDeterminism(t *testing.T) {
	cat := testCatalog()
	s := stateWithPets(t, cat, testPetDogID)
	s = mustSwitch(t, s, cat, testPetDogID)
	req := feedReq(cat, testPetDogID, 2)

	first := Feed(s, cat, req, 42, 10)
	for i := 0; i < 100; i++ {
		again := Feed(s, cat, req, 42, 10)
		if again.Status != first.Status ||
			again.Candidate.GuardUntilMs != first.Candidate.GuardUntilMs ||
			again.Candidate.GuardVersion != first.Candidate.GuardVersion ||
			again.GuardSync.SnapshotHash != first.GuardSync.SnapshotHash ||
			again.Receipt != first.Receipt {
			t.Fatalf("replay %d diverged", i)
		}
	}
	// Input state remains pristine after 101 evaluations.
	if s.GuardUntilMs != 0 || s.GuardVersion != 1 {
		t.Fatalf("input state mutated by Feed")
	}
}

// TestGuardEffective_Boundaries is acceptance item 5: never fed, exactly
// expired and expired guards all produce no guard effect.
func TestGuardEffective_Boundaries(t *testing.T) {
	cat := testCatalog()
	s := stateWithPets(t, cat, testPetDogID)

	if s.GuardEffective(0) {
		t.Fatalf("unselected pet must not guard")
	}
	s = mustSwitch(t, s, cat, testPetDogID)
	if s.GuardEffective(0) {
		t.Fatalf("selected but never fed (guard_until=0) must not guard")
	}

	now := int64(5_000_000)
	fed := Feed(s, cat, feedReq(cat, testPetDogID, 1), now, 10)
	g := fed.Candidate
	if !g.GuardEffective(now) || !g.GuardEffective(g.GuardUntilMs-1) {
		t.Fatalf("guard must be effective inside [now, until)")
	}
	if g.GuardEffective(g.GuardUntilMs) {
		t.Fatalf("now == until is already expired")
	}
	if g.GuardEffective(g.GuardUntilMs + 1) {
		t.Fatalf("expired guard must not be effective")
	}
}

// assertStateEqual fails if two states differ (used for immutability checks).
func assertStateEqual(t *testing.T, want, got State, msg string) {
	t.Helper()
	if want.ActiveGuardPetID != got.ActiveGuardPetID ||
		want.GuardUntilMs != got.GuardUntilMs ||
		want.PetStateVersion != got.PetStateVersion ||
		want.GuardVersion != got.GuardVersion ||
		len(want.OwnedPetIDs) != len(got.OwnedPetIDs) {
		t.Fatalf("%s: %+v vs %+v", msg, want, got)
	}
	for i := range want.OwnedPetIDs {
		if want.OwnedPetIDs[i] != got.OwnedPetIDs[i] {
			t.Fatalf("%s: owned set differs at %d", msg, i)
		}
	}
	if (want.GuardRuleSnapshot == nil) != (got.GuardRuleSnapshot == nil) {
		t.Fatalf("%s: rule snapshot presence differs", msg)
	}
	if want.GuardRuleSnapshot != nil && *want.GuardRuleSnapshot != *got.GuardRuleSnapshot {
		t.Fatalf("%s: rule snapshot differs", msg)
	}
}
