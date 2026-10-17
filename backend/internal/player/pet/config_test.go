package pet

import (
	"math"
	"testing"
)

const (
	testHourMs = int64(3_600_000)

	testDogFoodItemID = int32(9001)

	testPetDogID = int32(1)
	testPetCatID = int32(2)
	testPetFoxID = int32(3)
)

// testCatalog returns a valid P0 catalog: 8h per food, 24h horizon, at most
// 3 food per command, three pets with distinct rules/prices.
func testCatalog() CatalogSnapshot {
	return CatalogSnapshot{
		CatalogVersion: 1,
		FeedRule: FeedRule{
			FeedRuleVersion:       1,
			DogFoodItemID:         testDogFoodItemID,
			DurationPerFoodMs:     8 * testHourMs,
			MaxGuardHorizonMs:     24 * testHourMs,
			MaxQuantityPerCommand: 3,
		},
		Pets: []Definition{
			{
				PetID:               testPetDogID,
				PetConfigVersion:    1,
				DisplayDefinitionID: 101,
				UnlockOffer:         UnlockOffer{CoinUnlockEnabled: true, CoinPrice: 100, PriceVersion: 1},
				GuardRule: GuardRule{
					GuardRuleVersion:       1,
					TriggerProbabilityBp:   5000,
					InterceptPolicy:        InterceptPolicyFullGross,
					RandomAlgorithmVersion: 1,
					RandomKeyVersion:       1,
				},
			},
			{
				PetID:               testPetCatID,
				PetConfigVersion:    1,
				DisplayDefinitionID: 102,
				UnlockOffer:         UnlockOffer{CoinUnlockEnabled: true, CoinPrice: 200, PriceVersion: 1},
				GuardRule: GuardRule{
					GuardRuleVersion:       2,
					TriggerProbabilityBp:   10000,
					InterceptPolicy:        InterceptPolicyFullGross,
					RandomAlgorithmVersion: 1,
					RandomKeyVersion:       2,
				},
			},
			{
				// 下架样例：coin unlock disabled, definition retained.
				PetID:               testPetFoxID,
				PetConfigVersion:    3,
				DisplayDefinitionID: 103,
				UnlockOffer:         UnlockOffer{CoinUnlockEnabled: false, CoinPrice: 0, PriceVersion: 0},
				GuardRule: GuardRule{
					GuardRuleVersion:       1,
					TriggerProbabilityBp:   0,
					InterceptPolicy:        InterceptPolicyFullGross,
					RandomAlgorithmVersion: 2,
					RandomKeyVersion:       1,
				},
			},
		},
	}
}

func validDefinition(id int32) Definition {
	return Definition{
		PetID:               id,
		PetConfigVersion:    1,
		DisplayDefinitionID: 100 + id,
		UnlockOffer:         UnlockOffer{CoinUnlockEnabled: true, CoinPrice: 10, PriceVersion: 1},
		GuardRule: GuardRule{
			GuardRuleVersion:       1,
			TriggerProbabilityBp:   1,
			InterceptPolicy:        InterceptPolicyFullGross,
			RandomAlgorithmVersion: 1,
			RandomKeyVersion:       1,
		},
	}
}

func validFeedRule() FeedRule {
	return FeedRule{
		FeedRuleVersion:       1,
		DogFoodItemID:         testDogFoodItemID,
		DurationPerFoodMs:     8 * testHourMs,
		MaxGuardHorizonMs:     24 * testHourMs,
		MaxQuantityPerCommand: 3,
	}
}

func TestValidateCatalog_TableDriven(t *testing.T) {
	base := func() CatalogSnapshot {
		return CatalogSnapshot{
			CatalogVersion: 1,
			FeedRule:       validFeedRule(),
			Pets:           []Definition{validDefinition(1), validDefinition(2)},
		}
	}

	cases := []struct {
		name    string
		mutate  func(*CatalogSnapshot)
		wantErr bool
	}{
		{"valid", func(c *CatalogSnapshot) {}, false},
		{"empty catalog", func(c *CatalogSnapshot) { c.Pets = nil }, true},
		{"duplicate pet id", func(c *CatalogSnapshot) { c.Pets[1].PetID = c.Pets[0].PetID }, true},
		{"zero pet id", func(c *CatalogSnapshot) { c.Pets[0].PetID = 0 }, true},
		{"negative pet id", func(c *CatalogSnapshot) { c.Pets[0].PetID = -1 }, true},
		{"zero pet config version", func(c *CatalogSnapshot) { c.Pets[0].PetConfigVersion = 0 }, true},
		{"negative price", func(c *CatalogSnapshot) { c.Pets[0].UnlockOffer.CoinPrice = -1 }, true},
		{"enabled offer without price version", func(c *CatalogSnapshot) { c.Pets[0].UnlockOffer.PriceVersion = 0 }, true},
		{"probability above 10000bp", func(c *CatalogSnapshot) { c.Pets[0].GuardRule.TriggerProbabilityBp = 10001 }, true},
		{"probability at 10000bp", func(c *CatalogSnapshot) { c.Pets[0].GuardRule.TriggerProbabilityBp = 10000 }, false},
		{"non FULL_GROSS policy", func(c *CatalogSnapshot) { c.Pets[0].GuardRule.InterceptPolicy = InterceptPolicy(9) }, true},
		{"zero guard rule version", func(c *CatalogSnapshot) { c.Pets[0].GuardRule.GuardRuleVersion = 0 }, true},
		{"zero random algorithm version", func(c *CatalogSnapshot) { c.Pets[0].GuardRule.RandomAlgorithmVersion = 0 }, true},
		{"zero random key version", func(c *CatalogSnapshot) { c.Pets[0].GuardRule.RandomKeyVersion = 0 }, true},
		{"zero feed rule version", func(c *CatalogSnapshot) { c.FeedRule.FeedRuleVersion = 0 }, true},
		{"invalid dog food item", func(c *CatalogSnapshot) { c.FeedRule.DogFoodItemID = 0 }, true},
		{"zero duration", func(c *CatalogSnapshot) { c.FeedRule.DurationPerFoodMs = 0 }, true},
		{"horizon below duration", func(c *CatalogSnapshot) { c.FeedRule.MaxGuardHorizonMs = c.FeedRule.DurationPerFoodMs - 1 }, true},
		{"zero max quantity", func(c *CatalogSnapshot) { c.FeedRule.MaxQuantityPerCommand = 0 }, true},
		{"quantity*duration overflow", func(c *CatalogSnapshot) {
			c.FeedRule.MaxQuantityPerCommand = math.MaxInt64
			c.FeedRule.DurationPerFoodMs = 2
			c.FeedRule.MaxGuardHorizonMs = math.MaxInt64
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := base()
			tc.mutate(&c)
			err := ValidateCatalog(c)
			if tc.wantErr && err == nil {
				t.Fatalf("expected validation error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected valid catalog, got %v", err)
			}
		})
	}
}

// TestValidateCatalog_RejectsThirtyThirdPet is acceptance item 15: the 33rd
// pet is rejected at config publish, never reaching the runtime.
func TestValidateCatalog_RejectsThirtyThirdPet(t *testing.T) {
	c := CatalogSnapshot{CatalogVersion: 1, FeedRule: validFeedRule()}
	for i := int32(1); i <= MaxOwnedPets; i++ {
		c.Pets = append(c.Pets, validDefinition(i))
	}
	if err := ValidateCatalog(c); err != nil {
		t.Fatalf("32 pets must be legal, got %v", err)
	}
	c.Pets = append(c.Pets, validDefinition(MaxOwnedPets+1))
	if err := ValidateCatalog(c); err == nil {
		t.Fatalf("33rd pet must be rejected at publish")
	}
}
