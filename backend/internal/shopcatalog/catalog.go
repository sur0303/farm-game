// Package shopcatalog is the versioned, server-authoritative set of coin
// offers the shop can sell.
//
// Crop seed offers are projected from cropcatalog so seed pricing keeps a
// single source of truth: the shop cannot drift from what planting and
// settlement expect. Supply offers such as pet food belong to no crop and are
// defined here instead.
//
// Offer IDs are persisted inside client BUY requests and must never be reused.
// Crop offers occupy the low range; supplies start at supplyOfferIDBase.
package shopcatalog

import (
	"farmgame/internal/cropcatalog"
	playerpet "farmgame/internal/player/pet"
)

// Kind lets a caller that only means "seed" avoid silently widening to every
// purchasable item when a new supply is published.
type Kind uint8

const (
	KindCropSeed Kind = iota + 1
	KindSupply
)

const supplyOfferIDBase int32 = 101

// DogFoodOfferID is the supply that feeds a guard pet.
const DogFoodOfferID int32 = supplyOfferIDBase

// DogFoodBuyPrice is a placeholder awaiting the economy pass. It is set
// against the 100 coin dog unlock (one unit is 8h of guard, so the 24h horizon
// costs 90), but that anchor is meaningless while a single crop cycle sells for
// thousands of coins, so the number is not yet a design decision.
const DogFoodBuyPrice int64 = 30

// DogFoodOfferVersion must be bumped whenever DogFoodBuyPrice changes.
const DogFoodOfferVersion uint64 = 1

// Offer is one purchasable shop entry. Buying it debits BuyPrice per unit and
// credits ItemID per unit. OfferVersion identifies the published price: a BUY
// that expects a different version is refused rather than settled (32 §2.3).
type Offer struct {
	OfferID      int32
	Name         string
	Kind         Kind
	ItemID       int32
	BuyPrice     int64
	OfferVersion uint64
}

var supplies = [...]Offer{
	{
		OfferID:      DogFoodOfferID,
		Name:         "狗粮",
		Kind:         KindSupply,
		ItemID:       playerpet.P0DogFoodItemID,
		BuyPrice:     DogFoodBuyPrice,
		OfferVersion: DogFoodOfferVersion,
	},
}

// ByOfferID resolves any purchasable offer, crop seed or supply.
func ByOfferID(id int32) (Offer, bool) {
	if crop, ok := cropcatalog.ByOfferID(id); ok {
		return cropSeedOffer(crop), true
	}
	for _, offer := range supplies {
		if offer.OfferID == id {
			return offer, true
		}
	}
	return Offer{}, false
}

// OfferVersionOf returns the published version of an offer. An unknown offer
// yields 0, which no published offer uses, so a BUY built from it is refused
// instead of quietly settling at whatever the server currently charges.
func OfferVersionOf(id int32) uint64 {
	offer, ok := ByOfferID(id)
	if !ok {
		return 0
	}
	return offer.OfferVersion
}

// All returns every purchasable offer, crop seeds first.
func All() []Offer {
	crops := cropcatalog.All()
	offers := make([]Offer, 0, len(crops)+len(supplies))
	for _, crop := range crops {
		offers = append(offers, cropSeedOffer(crop))
	}
	offers = append(offers, supplies[:]...)
	return offers
}

func cropSeedOffer(crop cropcatalog.Definition) Offer {
	return Offer{
		OfferID:      crop.OfferID,
		Name:         crop.Name,
		Kind:         KindCropSeed,
		ItemID:       crop.SeedItemID,
		BuyPrice:     crop.BuyPrice,
		OfferVersion: crop.OfferVersion,
	}
}
