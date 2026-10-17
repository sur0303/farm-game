// Package taskcatalog contains the small versioned task definition set used by
// the P0 task vertical slice. Definitions are configuration; TaskInstance is
// the durable player-owned state in PlayerRecord.
package taskcatalog

type Event int32

const (
	EventBuyOffer Event = 1
	EventHarvest  Event = 2
)

type Definition struct {
	ID          int32
	Name        string
	Description string
	Event       Event
	Target      int64
	RewardCoin  int64
	RewardExp   int64
}

var definitions = [...]Definition{
	{ID: 1, Name: "新手收获", Description: "收获任意 3 个成熟作物", Event: EventHarvest, Target: 3, RewardCoin: 30, RewardExp: 10},
	{ID: 2, Name: "备足种子", Description: "在商店购买 3 份种子", Event: EventBuyOffer, Target: 3, RewardCoin: 20, RewardExp: 5},
}

func All() []Definition {
	result := make([]Definition, len(definitions))
	copy(result, definitions[:])
	return result
}

func ByID(id int32) (Definition, bool) {
	for _, definition := range definitions {
		if definition.ID == id {
			return definition, true
		}
	}
	return Definition{}, false
}
