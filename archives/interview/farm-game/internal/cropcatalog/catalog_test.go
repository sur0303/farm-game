package cropcatalog

import "testing"

func TestCatalogIDsAndEconomyAreStableAndDistinct(t *testing.T) {
	all := All()
	if len(all) != 3 {
		t.Fatalf("catalog size=%d", len(all))
	}
	seenDefinitions := map[int32]bool{}
	seenSeeds := map[int32]bool{}
	seenProduce := map[int32]bool{}
	seenOffers := map[int32]bool{}
	for _, crop := range all {
		if crop.Name == "" || crop.Icon == "" || crop.MatureAfter <= 0 || crop.BaseYield <= 0 || crop.StealableLimit <= 0 {
			t.Fatalf("invalid crop definition=%+v", crop)
		}
		for name, seen := range map[string]map[int32]bool{
			"definition": seenDefinitions, "seed": seenSeeds, "produce": seenProduce, "offer": seenOffers,
		} {
			id := crop.ID
			switch name {
			case "seed":
				id = crop.SeedItemID
			case "produce":
				id = crop.ProduceItemID
			case "offer":
				id = crop.OfferID
			}
			if seen[id] {
				t.Fatalf("duplicate %s id=%d", name, id)
			}
			seen[id] = true
		}
	}
}
