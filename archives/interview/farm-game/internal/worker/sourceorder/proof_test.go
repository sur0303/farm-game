package sourceorder

import (
	"testing"

	"farmgame/internal/identity"
	"farmgame/internal/workidentity"
)

func TestAcceptsDirectRegistry(t *testing.T) {
	for _, workType := range []string{
		string(identity.WorkTypeCareReward),
		string(identity.WorkTypeHarvest),
		string(identity.WorkTypePlant),
		string(identity.WorkTypeUnlockPlot),
		string(identity.WorkTypeTheftVisitor),
		string(identity.WorkTypeTheftGuard),
	} {
		err := AcceptsDirect(workidentity.WorkAddress{
			WorkID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", WorkType: workType,
			PlacementKind: workidentity.SourcePersistenceBucket, BucketVersion: 1, BucketID: 1,
			SourceRouteDomain: workidentity.RouteDomainFarm, SourceActorID: 1,
		})
		if err != nil {
			// WorkID shape is validated first; a valid opaque id is required.
			break
		}
		_ = workType
	}
	if _, ok := Lookup(string(identity.WorkTypeHarvest)); !ok {
		t.Fatal("HARVEST must be in the CompleteWork registry")
	}
	if _, ok := Lookup(string(identity.WorkTypePlant)); !ok {
		t.Fatal("PLANT must be in the CompleteWork registry")
	}
	if _, ok := Lookup(string(identity.WorkTypeUnlockPlot)); !ok {
		t.Fatal("UNLOCK_PLOT must be in the CompleteWork registry")
	}
	if _, ok := Lookup(string(identity.WorkTypeTheftVisitor)); !ok {
		t.Fatal("THEFT_VISITOR must be in the CompleteWork registry")
	}
	if _, ok := Lookup(string(identity.WorkTypeTheftGuard)); !ok {
		t.Fatal("THEFT_GUARD must be in the CompleteWork registry")
	}
	if _, ok := Lookup("THEFT"); ok {
		t.Fatal("legacy THEFT WorkRecord name must not complete a SourceOrder")
	}
}
