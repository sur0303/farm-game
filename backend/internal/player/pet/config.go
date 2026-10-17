// Package pet implements the pure, versioned pet-guard domain frozen by
// docs/design/36 (简化宠物守护系统合同 v1.1). It contains no Actor, Redis,
// RPC, Workflow or cmd dependencies: every function maps an immutable input
// (current PetState + versioned catalog + request + trusted_now) to a
// deterministic decision plus a CandidateState. Wallet deduction, inventory
// consumption, Receipt/Outcome persistence and GuardSyncOrder creation are
// performed by the PlayerActor Authority layer using the outputs of this
// package; this package never performs them itself.
package pet

import "fmt"

// MaxOwnedPets is the frozen upper bound of the P0 pet catalog and therefore
// of any single player's owned set (docs/design/36 §6.1 rule 1, §8.1).
// Config publish must reject catalogs larger than this; the runtime check is
// defense in depth.
const MaxOwnedPets = 32

// InterceptPolicy is the theft-interception policy of a guard rule.
// P0 freezes FULL_GROSS as the only legal value (docs/design/32 FROZEN_32,
// docs/design/36 §8.1).
type InterceptPolicy uint8

const (
	InterceptPolicyFullGross InterceptPolicy = 1
)

func (p InterceptPolicy) String() string {
	if p == InterceptPolicyFullGross {
		return "FULL_GROSS"
	}
	return fmt.Sprintf("UNKNOWN(%d)", uint8(p))
}

// UnlockOffer is the versioned coin-purchase offer of one pet.
type UnlockOffer struct {
	CoinUnlockEnabled bool
	CoinPrice         int64
	PriceVersion      uint64
}

// GuardRule is the versioned guard rule of one pet definition.
type GuardRule struct {
	GuardRuleVersion       uint64
	TriggerProbabilityBp   uint32 // [0, 10000]
	InterceptPolicy        InterceptPolicy
	RandomAlgorithmVersion uint64
	RandomKeyVersion       uint64
}

// Definition is the versioned server-side definition of one pet
// (docs/design/36 §3.4 PetDefinition).
type Definition struct {
	PetID               int32
	PetConfigVersion    uint64
	DisplayDefinitionID int32
	UnlockOffer         UnlockOffer
	GuardRule           GuardRule
}

// FeedRule is the versioned dog-food rule (docs/design/36 §3.4 PetFeedRule).
// P0 defaults: DurationPerFoodMs = 8h, MaxGuardHorizonMs = 24h.
type FeedRule struct {
	FeedRuleVersion       uint64
	DogFoodItemID         int32
	DurationPerFoodMs     int64
	MaxGuardHorizonMs     int64
	MaxQuantityPerCommand int64
}

// CatalogSnapshot is the versioned, immutable pet catalog
// (docs/design/36 §3.4 PetCatalogSnapshot). Pets are looked up by PetID;
// the slice itself is never mutated after validation.
type CatalogSnapshot struct {
	CatalogVersion uint64
	FeedRule       FeedRule
	Pets           []Definition
}

// Clone returns a deep copy that shares no mutable memory with c.
func (c CatalogSnapshot) Clone() CatalogSnapshot {
	out := c
	out.Pets = append([]Definition(nil), c.Pets...)
	return out
}

// Current lets a standalone snapshot satisfy Provider in pure-domain tests.
func (c CatalogSnapshot) Current() CatalogSnapshot { return c.Clone() }

// DefinitionAt returns the exact version of a pet in this snapshot.
func (c CatalogSnapshot) DefinitionAt(petID int32, petConfigVersion uint64) (Definition, bool) {
	if petConfigVersion == 0 {
		return Definition{}, false
	}
	for _, d := range c.Pets {
		if d.PetID == petID && d.PetConfigVersion == petConfigVersion {
			return d, true
		}
	}
	return Definition{}, false
}

// Find returns the definition for petID.
func (c CatalogSnapshot) Find(petID int32) (Definition, bool) {
	for _, d := range c.Pets {
		if d.PetID == petID {
			return d, true
		}
	}
	return Definition{}, false
}

// ValidateCatalog enforces the publish-time invariants frozen in
// docs/design/36 §8.1. A catalog that fails validation must never reach the
// runtime (acceptance item 15: the 33rd pet is rejected at publish).
func ValidateCatalog(c CatalogSnapshot) error {
	if c.CatalogVersion == 0 {
		return fmt.Errorf("pet catalog: zero catalog_version")
	}
	if len(c.Pets) == 0 {
		return fmt.Errorf("pet catalog: empty catalog")
	}
	if len(c.Pets) > MaxOwnedPets {
		return fmt.Errorf("pet catalog: %d pets exceeds frozen limit %d", len(c.Pets), MaxOwnedPets)
	}
	seen := make(map[int32]struct{}, len(c.Pets))
	for _, d := range c.Pets {
		if d.PetID <= 0 {
			return fmt.Errorf("pet catalog: pet_id %d must be positive", d.PetID)
		}
		if d.DisplayDefinitionID <= 0 {
			return fmt.Errorf("pet catalog: pet %d has invalid display_definition_id %d", d.PetID, d.DisplayDefinitionID)
		}
		if _, dup := seen[d.PetID]; dup {
			return fmt.Errorf("pet catalog: duplicate pet_id %d", d.PetID)
		}
		seen[d.PetID] = struct{}{}
		if d.PetConfigVersion == 0 {
			return fmt.Errorf("pet catalog: pet %d has zero pet_config_version", d.PetID)
		}
		if d.UnlockOffer.CoinPrice < 0 {
			return fmt.Errorf("pet catalog: pet %d has negative coin price %d", d.PetID, d.UnlockOffer.CoinPrice)
		}
		if d.UnlockOffer.CoinUnlockEnabled && d.UnlockOffer.PriceVersion == 0 {
			return fmt.Errorf("pet catalog: pet %d coin unlock enabled with zero price_version", d.PetID)
		}
		if d.GuardRule.TriggerProbabilityBp > 10000 {
			return fmt.Errorf("pet catalog: pet %d trigger_probability_bp %d out of [0,10000]", d.PetID, d.GuardRule.TriggerProbabilityBp)
		}
		if d.GuardRule.InterceptPolicy != InterceptPolicyFullGross {
			return fmt.Errorf("pet catalog: pet %d intercept_policy %s not allowed in P0", d.PetID, d.GuardRule.InterceptPolicy)
		}
		if d.GuardRule.GuardRuleVersion == 0 || d.GuardRule.RandomAlgorithmVersion == 0 || d.GuardRule.RandomKeyVersion == 0 {
			return fmt.Errorf("pet catalog: pet %d has zero guard/random version", d.PetID)
		}
	}
	fr := c.FeedRule
	if fr.FeedRuleVersion == 0 {
		return fmt.Errorf("pet catalog: zero feed_rule_version")
	}
	if fr.DogFoodItemID <= 0 {
		return fmt.Errorf("pet catalog: invalid dog_food_item_id %d", fr.DogFoodItemID)
	}
	if fr.DurationPerFoodMs <= 0 {
		return fmt.Errorf("pet catalog: duration_per_food_ms %d must be > 0", fr.DurationPerFoodMs)
	}
	if fr.MaxGuardHorizonMs < fr.DurationPerFoodMs {
		return fmt.Errorf("pet catalog: max_guard_horizon_ms %d < duration_per_food_ms %d", fr.MaxGuardHorizonMs, fr.DurationPerFoodMs)
	}
	if fr.MaxQuantityPerCommand <= 0 {
		return fmt.Errorf("pet catalog: max_quantity_per_command %d must be > 0", fr.MaxQuantityPerCommand)
	}
	if _, ok := checkedMul(fr.MaxQuantityPerCommand, fr.DurationPerFoodMs); !ok {
		return fmt.Errorf("pet catalog: max_quantity_per_command * duration_per_food_ms overflows")
	}
	return nil
}

// checkedAdd returns a+b and whether the result fits int64.
// All pet money/quantity/time math must go through checked arithmetic:
// overflow is rejected, never wrapped (docs/design/36 §3.4).
func checkedAdd(a, b int64) (int64, bool) {
	s := a + b
	if (b > 0 && s < a) || (b < 0 && s > a) {
		return 0, false
	}
	return s, true
}

// checkedMul returns a*b and whether the result fits int64. Inputs are
// expected non-negative in this domain; negative or overflowing products are
// rejected.
func checkedMul(a, b int64) (int64, bool) {
	if a < 0 || b < 0 {
		return 0, false
	}
	if a != 0 && b > (int64(^uint64(0)>>1))/a {
		return 0, false
	}
	return a * b, true
}
