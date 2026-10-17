package pet

import (
	"errors"
	"sync"
	"testing"
)

func TestDefaultP0Catalog(t *testing.T) {
	cat := DefaultP0Catalog()
	if err := ValidateCatalog(cat); err != nil {
		t.Fatalf("default catalog invalid: %v", err)
	}
	if len(cat.Pets) < 2 {
		t.Fatalf("default catalog has %d pets, want at least 2", len(cat.Pets))
	}
	if cat.FeedRule.DurationPerFoodMs != 8*p0HourMs ||
		cat.FeedRule.MaxGuardHorizonMs != 24*p0HourMs ||
		cat.FeedRule.MaxQuantityPerCommand != 3 {
		t.Fatalf("unexpected default feed rule: %+v", cat.FeedRule)
	}
	for _, def := range cat.Pets {
		if def.DisplayDefinitionID <= 0 ||
			!def.UnlockOffer.CoinUnlockEnabled ||
			def.UnlockOffer.CoinPrice <= 0 ||
			def.UnlockOffer.PriceVersion == 0 ||
			def.GuardRule.InterceptPolicy != InterceptPolicyFullGross ||
			def.GuardRule.RandomAlgorithmVersion == 0 ||
			def.GuardRule.RandomKeyVersion == 0 {
			t.Fatalf("incomplete default definition: %+v", def)
		}
		if _, ok := cat.DefinitionAt(def.PetID, def.PetConfigVersion); !ok {
			t.Fatalf("default definition %d/%d not addressable", def.PetID, def.PetConfigVersion)
		}
	}

	cat.Pets[0].DisplayDefinitionID = 9999
	fresh := DefaultP0Catalog()
	if fresh.Pets[0].DisplayDefinitionID == 9999 {
		t.Fatal("default catalog shares mutable pet storage")
	}
}

func TestMemoryRepositoryPublishesHistoryAndCopiesReads(t *testing.T) {
	repo := NewMemoryRepository()
	v1 := DefaultP0Catalog()
	if err := repo.Publish(v1); err != nil {
		t.Fatalf("publish v1: %v", err)
	}

	current := repo.Current()
	current.Pets[0].DisplayDefinitionID = 7777
	currentAgain := repo.Current()
	if currentAgain.Pets[0].DisplayDefinitionID != v1.Pets[0].DisplayDefinitionID {
		t.Fatal("Current returned an aliased snapshot")
	}
	oldDog, ok := repo.DefinitionAt(P0DogPetID, 1)
	if !ok || oldDog != v1.Pets[0] {
		t.Fatalf("v1 dog definition=%+v ok=%v, want %+v", oldDog, ok, v1.Pets[0])
	}

	v2 := v1.Clone()
	v2.CatalogVersion = 2
	v2.Pets = []Definition{v2.Pets[1]}
	v2.Pets[0].PetConfigVersion = 2
	v2.Pets[0].DisplayDefinitionID = 2002
	v2.Pets[0].UnlockOffer.CoinPrice = 250
	v2.Pets[0].UnlockOffer.PriceVersion = 2
	v2.Pets[0].GuardRule.GuardRuleVersion = 2
	if err := repo.Publish(v2); err != nil {
		t.Fatalf("publish v2: %v", err)
	}

	gotCurrent := repo.Current()
	if gotCurrent.CatalogVersion != 2 || len(gotCurrent.Pets) != 1 ||
		gotCurrent.Pets[0].PetID != P0CatPetID ||
		gotCurrent.Pets[0].PetConfigVersion != 2 {
		t.Fatalf("current=%+v, want v2 cat-only", gotCurrent)
	}
	if got, ok := repo.DefinitionAt(P0DogPetID, 1); !ok || got != v1.Pets[0] {
		t.Fatalf("historical dog was overwritten: %+v ok=%v", got, ok)
	}
	if got, ok := repo.DefinitionAt(P0CatPetID, 1); !ok || got != v1.Pets[1] {
		t.Fatalf("historical cat v1 missing: %+v ok=%v", got, ok)
	}
	if got, ok := repo.DefinitionAt(P0CatPetID, 2); !ok || got != v2.Pets[0] {
		t.Fatalf("current cat v2 missing: %+v ok=%v", got, ok)
	}

	def, _ := repo.DefinitionAt(P0CatPetID, 1)
	def.DisplayDefinitionID = 8888
	again, _ := repo.DefinitionAt(P0CatPetID, 1)
	if again.DisplayDefinitionID == 8888 {
		t.Fatal("DefinitionAt returned mutable repository state")
	}
}

func TestMemoryRepositoryRejectsBadPublishAndPreservesOldCurrent(t *testing.T) {
	repo := NewMemoryRepository()
	base := DefaultP0Catalog()
	if err := repo.Publish(base); err != nil {
		t.Fatalf("publish base: %v", err)
	}

	bad := base.Clone()
	bad.CatalogVersion = 2
	bad.FeedRule.DurationPerFoodMs = 0
	if err := repo.Publish(bad); err == nil {
		t.Fatal("bad publish unexpectedly succeeded")
	}
	if got := repo.Current(); !catalogSnapshotsEqual(got, base) {
		t.Fatalf("bad publish replaced current: got=%+v want=%+v", got, base)
	}
	if got, ok := repo.DefinitionAt(P0DogPetID, 1); !ok || got != base.Pets[0] {
		t.Fatalf("bad publish changed history: %+v ok=%v", got, ok)
	}

	conflict := base.Clone()
	conflict.CatalogVersion = 3
	conflict.Pets[0].DisplayDefinitionID = 9999
	if err := repo.Publish(conflict); !errors.Is(err, ErrHistoricalDefinitionConflict) {
		t.Fatalf("historical overwrite error=%v, want %v", err, ErrHistoricalDefinitionConflict)
	}
	if got := repo.Current(); !catalogSnapshotsEqual(got, base) {
		t.Fatalf("historical conflict replaced current: got=%+v want=%+v", got, base)
	}

	older := base.Clone()
	older.CatalogVersion = 0
	if err := repo.Publish(older); err == nil {
		t.Fatal("zero-version publish unexpectedly succeeded")
	}
	if got := repo.Current(); !catalogSnapshotsEqual(got, base) {
		t.Fatalf("zero-version publish replaced current: got=%+v want=%+v", got, base)
	}
}

func TestMemoryRepositoryConcurrentReadAndPublish(t *testing.T) {
	repo := NewMemoryRepository()
	base := DefaultP0Catalog()
	if err := repo.Publish(base); err != nil {
		t.Fatalf("publish base: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for version := uint64(2); version <= 64; version++ {
			next := base.Clone()
			next.CatalogVersion = version
			next.Pets[0].PetConfigVersion = version
			next.Pets[0].DisplayDefinitionID = 2000 + int32(version)
			next.Pets[0].UnlockOffer.PriceVersion = version
			next.Pets[0].GuardRule.GuardRuleVersion = version
			if err := repo.Publish(next); err != nil {
				t.Errorf("publish version %d: %v", version, err)
				return
			}
		}
	}()

	const readers = 12
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				current := repo.Current()
				if current.CatalogVersion != 0 {
					if err := ValidateCatalog(current); err != nil {
						t.Errorf("reader saw invalid current: %v", err)
						return
					}
					if len(current.Pets) != 2 {
						t.Errorf("reader saw partial current: %+v", current)
						return
					}
					current.Pets[0].DisplayDefinitionID = -1
				}
				if got, ok := repo.DefinitionAt(P0DogPetID, 1); !ok || got.PetID != P0DogPetID {
					t.Errorf("reader lost history: %+v ok=%v", got, ok)
					return
				}
			}
		}()
	}
	wg.Wait()
	if got := repo.Current(); got.CatalogVersion != 64 {
		t.Fatalf("final current version=%d, want 64", got.CatalogVersion)
	}
	if _, ok := repo.DefinitionAt(P0DogPetID, 1); !ok {
		t.Fatal("final history lost v1")
	}
}

func TestHistoricalTaskGrantUsesDefinitionAt(t *testing.T) {
	repo, err := NewP0Repository()
	if err != nil {
		t.Fatal(err)
	}
	v1 := repo.Current()
	v2 := v1.Clone()
	v2.CatalogVersion = 2
	v2.Pets = []Definition{v1.Pets[1]}
	if err := repo.Publish(v2); err != nil {
		t.Fatalf("publish delisted catalog: %v", err)
	}

	grant := TaskUnlockGrant{
		PetID:                  v1.Pets[0].PetID,
		PetConfigVersion:       v1.Pets[0].PetConfigVersion,
		GrantDefinitionVersion: 1,
	}
	res := ApplyTaskUnlockGrant(NewState(), repo, grant)
	if res.Status != StatusSucceeded || !res.Changed || !res.Candidate.Owns(grant.PetID) {
		t.Fatalf("historical grant result=%+v", res)
	}

	owned := ApplyTaskUnlockGrant(res.Candidate, repo, grant)
	if owned.Status != StatusNoChange || owned.ErrorCode != ErrPetAlreadyUnlocked || owned.Changed {
		t.Fatalf("historical grant replay=%+v", owned)
	}
}

func TestIdempotentOperationsSurviveCatalogRemoval(t *testing.T) {
	base := DefaultP0Catalog()
	delisted := base.Clone()
	delisted.Pets = []Definition{base.Pets[1]}

	owned := stateWithPets(t, base, P0DogPetID)
	beforeUnlock := owned.Clone()
	unlock := UnlockByCoin(owned, delisted, UnlockByCoinRequest{
		PetID:                    P0DogPetID,
		ExpectedPetConfigVersion: 999,
		ExpectedPriceVersion:     999,
	}, 0)
	if unlock.Status != StatusNoChange || unlock.ErrorCode != ErrPetAlreadyUnlocked || unlock.Fee.Amount != 0 {
		t.Fatalf("owned unlock after removal=%+v", unlock)
	}
	assertStateEqual(t, beforeUnlock, owned, "owned unlock mutated input")

	owned = stateWithPets(t, base, P0DogPetID, P0CatPetID)
	owned = mustSwitch(t, owned, base, P0DogPetID)
	before := owned.Clone()
	switched := Switch(owned, delisted, SwitchRequest{
		TargetPetID:              P0DogPetID,
		ExpectedPetConfigVersion: 999,
		ExpectedGuardRuleVersion: 999,
	})
	if switched.Status != StatusNoChange || switched.ErrorCode != ErrPetAlreadyActive ||
		switched.GuardSync.GuardVersion != 0 {
		t.Fatalf("active switch after removal=%+v", switched)
	}
	assertStateEqual(t, before, owned, "idempotent switch mutated input")
}

func TestValidateCatalogRequiresVersionAndDisplayIDs(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*CatalogSnapshot)
	}{
		{"catalog version", func(c *CatalogSnapshot) { c.CatalogVersion = 0 }},
		{"display id", func(c *CatalogSnapshot) { c.Pets[0].DisplayDefinitionID = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cat := DefaultP0Catalog()
			tc.mutate(&cat)
			if err := ValidateCatalog(cat); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
