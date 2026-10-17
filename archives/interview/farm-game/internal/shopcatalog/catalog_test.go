package shopcatalog

import (
	"testing"

	"farmgame/internal/cropcatalog"
	playerpet "farmgame/internal/player/pet"
)

func TestCropOfferIDsNeverCollideWithSupplies(t *testing.T) {
	for _, crop := range cropcatalog.All() {
		if crop.OfferID >= supplyOfferIDBase {
			t.Fatalf("crop %q offer id %d reaches the supply range starting at %d",
				crop.Name, crop.OfferID, supplyOfferIDBase)
		}
	}
	seen := map[int32]string{}
	for _, offer := range All() {
		if previous, exists := seen[offer.OfferID]; exists {
			t.Fatalf("offer id %d is used by both %q and %q", offer.OfferID, previous, offer.Name)
		}
		seen[offer.OfferID] = offer.Name
	}
}

func TestCropSeedOffersProjectCropCatalogPricing(t *testing.T) {
	for _, crop := range cropcatalog.All() {
		offer, ok := ByOfferID(crop.OfferID)
		if !ok {
			t.Fatalf("crop offer %d is not purchasable", crop.OfferID)
		}
		if offer.Kind != KindCropSeed || offer.ItemID != crop.SeedItemID || offer.BuyPrice != crop.BuyPrice {
			t.Fatalf("offer %+v drifted from crop definition %+v", offer, crop)
		}
	}
}

func TestDogFoodIsPurchasableSupply(t *testing.T) {
	offer, ok := ByOfferID(DogFoodOfferID)
	if !ok {
		t.Fatal("dog food is not purchasable")
	}
	if offer.Kind != KindSupply || offer.ItemID != playerpet.P0DogFoodItemID || offer.BuyPrice != DogFoodBuyPrice {
		t.Fatalf("dog food offer=%+v", offer)
	}
	if _, ok := cropcatalog.ByOfferID(DogFoodOfferID); ok {
		t.Fatal("dog food offer id collides with a crop offer")
	}
}

func TestUnknownOfferIsRejected(t *testing.T) {
	for _, id := range []int32{0, -1, supplyOfferIDBase + 999} {
		if offer, ok := ByOfferID(id); ok {
			t.Fatalf("offer id %d unexpectedly resolved to %+v", id, offer)
		}
		if version := OfferVersionOf(id); version != 0 {
			t.Fatalf("unknown offer id %d reported version %d", id, version)
		}
	}
}

// A published offer priced at version 0 could never be quoted by a client,
// because 0 is what an unknown offer and an unset request field both look like.
func TestEveryPublishedOfferCarriesANonZeroVersion(t *testing.T) {
	for _, offer := range All() {
		if offer.OfferVersion == 0 {
			t.Fatalf("offer %+v has no published version", offer)
		}
		if got := OfferVersionOf(offer.OfferID); got != offer.OfferVersion {
			t.Fatalf("offer %d version lookup=%d, want %d", offer.OfferID, got, offer.OfferVersion)
		}
	}
}

// Prices and their versions are edited by hand in two packages, so pin them
// together: changing a price without republishing the version would let a stale
// client page settle at the new price, which is exactly what 32 §2.3 forbids.
func TestRepublishingAPriceRequiresBumpingItsOfferVersion(t *testing.T) {
	published := map[int32]struct {
		price   int64
		version uint64
	}{
		cropcatalog.CarrotOfferID: {price: 10, version: 1},
		cropcatalog.TomatoOfferID: {price: 15, version: 1},
		cropcatalog.CornOfferID:   {price: 20, version: 1},
		DogFoodOfferID:            {price: 30, version: 1},
	}
	offers := All()
	if len(offers) != len(published) {
		t.Fatalf("shop publishes %d offers but this test pins %d", len(offers), len(published))
	}
	for _, offer := range offers {
		want, ok := published[offer.OfferID]
		if !ok {
			t.Fatalf("offer %+v is published but not pinned here", offer)
		}
		if offer.BuyPrice != want.price || offer.OfferVersion != want.version {
			t.Fatalf("offer %d is now %d coins at version %d, pinned as %d at version %d; "+
				"republish the version alongside the price and update this pin",
				offer.OfferID, offer.BuyPrice, offer.OfferVersion, want.price, want.version)
		}
	}
}
