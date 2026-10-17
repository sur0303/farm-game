// Package cropcatalog contains the versioned, server-authoritative crop and
// shop definitions shared by PlayerActor and FarmActor.
//
// Keeping these definitions outside either actor prevents a subtle class of
// bugs where the shop sells one item but planting or settlement grants another.
// The IDs are deliberately stable: they are persisted in PlayerRecord,
// CropCycle and CodexEntry and therefore must not be reused.
package cropcatalog

import "time"

type Definition struct {
	ID             int32
	Name           string
	Icon           string
	SeedItemID     int32
	ProduceItemID  int32
	OfferID        int32
	BuyPrice       int64
	SellPrice      int64
	OfferVersion   uint64
	MatureAfter    time.Duration
	BaseYield      int32
	StealableLimit int32

	// The fields below are the small, versioned rule input used by Plant.
	// BaseYield and StealableLimit above remain the legacy demo projection;
	// SnapshotBaseYield is the explicit integer P0 quantity used by
	// CropRuleSnapshot and is required for a complete versioned definition.
	CatalogVersion      uint64
	DefinitionVersion   uint64
	CropRuleVersion     uint64
	SnapshotBaseYield   uint64
	HarvestExp          uint64
	CropUnitCoinValue   uint64
	LegacySeedPlantable bool
	UnlockEligible      bool
	ActivityID          string
	StealPolicyVersion  uint64
	CareRuleVersion     uint64
	WaterOffset         time.Duration
	RemovePestOffset    time.Duration
}

const (
	CarrotDefinitionID int32 = 1
	TomatoDefinitionID int32 = 2
	CornDefinitionID   int32 = 3

	CarrotSeedItemID int32 = 1001
	TomatoSeedItemID int32 = 1002
	CornSeedItemID   int32 = 1003

	CarrotItemID int32 = 2001
	TomatoItemID int32 = 2002
	CornItemID   int32 = 2003

	CarrotOfferID int32 = 1
	TomatoOfferID int32 = 2
	CornOfferID   int32 = 3
)

var definitions = [...]Definition{
	{
		ID: CarrotDefinitionID, Name: "胡萝卜", Icon: "🥕",
		SeedItemID: CarrotSeedItemID, ProduceItemID: CarrotItemID, OfferID: CarrotOfferID,
		BuyPrice: 10, SellPrice: 15, OfferVersion: 1, MatureAfter: 10 * time.Second, BaseYield: 10, StealableLimit: 3,
		CatalogVersion: 1, DefinitionVersion: 1, CropRuleVersion: 1,
		SnapshotBaseYield: 500, HarvestExp: 10, CropUnitCoinValue: 15,
		LegacySeedPlantable: true, UnlockEligible: true, StealPolicyVersion: 1, CareRuleVersion: 1,
		WaterOffset: 3 * time.Second, RemovePestOffset: 6 * time.Second,
	},
	{
		ID: TomatoDefinitionID, Name: "番茄", Icon: "🍅",
		SeedItemID: TomatoSeedItemID, ProduceItemID: TomatoItemID, OfferID: TomatoOfferID,
		BuyPrice: 15, SellPrice: 22, OfferVersion: 1, MatureAfter: 15 * time.Second, BaseYield: 8, StealableLimit: 2,
		CatalogVersion: 1, DefinitionVersion: 1, CropRuleVersion: 1,
		SnapshotBaseYield: 400, HarvestExp: 8, CropUnitCoinValue: 22,
		LegacySeedPlantable: true, UnlockEligible: true, StealPolicyVersion: 1, CareRuleVersion: 1,
		WaterOffset: 5 * time.Second, RemovePestOffset: 10 * time.Second,
	},
	{
		ID: CornDefinitionID, Name: "玉米", Icon: "🌽",
		SeedItemID: CornSeedItemID, ProduceItemID: CornItemID, OfferID: CornOfferID,
		BuyPrice: 20, SellPrice: 30, OfferVersion: 1, MatureAfter: 20 * time.Second, BaseYield: 6, StealableLimit: 2,
		CatalogVersion: 1, DefinitionVersion: 1, CropRuleVersion: 1,
		SnapshotBaseYield: 300, HarvestExp: 6, CropUnitCoinValue: 30,
		LegacySeedPlantable: true, UnlockEligible: true, StealPolicyVersion: 1, CareRuleVersion: 1,
		WaterOffset: 7 * time.Second, RemovePestOffset: 14 * time.Second,
	},
}

func All() []Definition {
	result := make([]Definition, len(definitions))
	copy(result, definitions[:])
	return result
}

func ByDefinitionID(id int32) (Definition, bool) {
	for _, definition := range definitions {
		if definition.ID == id {
			return definition, true
		}
	}
	return Definition{}, false
}

func ByOfferID(id int32) (Definition, bool) {
	for _, definition := range definitions {
		if definition.OfferID == id {
			return definition, true
		}
	}
	return Definition{}, false
}

func BySeedItemID(id int32) (Definition, bool) {
	for _, definition := range definitions {
		if definition.SeedItemID == id {
			return definition, true
		}
	}
	return Definition{}, false
}

func ByProduceItemID(id int32) (Definition, bool) {
	for _, definition := range definitions {
		if definition.ProduceItemID == id {
			return definition, true
		}
	}
	return Definition{}, false
}
