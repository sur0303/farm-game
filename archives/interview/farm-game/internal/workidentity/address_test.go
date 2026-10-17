package workidentity

import (
	"errors"
	"testing"
)

type fixedResolver struct {
	independent uint32
	authority   uint32
	err         error
}

func (r fixedResolver) IndependentWorkBucket(string, uint32) (uint32, error) {
	return r.independent, r.err
}

func (r fixedResolver) AuthorityPersistenceBucket(RouteDomain, uint64, uint32) (uint32, error) {
	return r.authority, r.err
}

func TestWorkAddressValidatesPlacementAndBucket(t *testing.T) {
	workID, _ := WorkflowID(10001, "PLANT", "op-1")
	independent := WorkAddress{
		WorkID: workID, WorkType: "PLANT", PlacementKind: IndependentWorkBucket,
		BucketVersion: 1, BucketID: 17,
	}
	if err := independent.Validate(fixedResolver{independent: 17}); err != nil {
		t.Fatal(err)
	}
	source := WorkAddress{
		WorkID: workID, WorkType: "CARE_REWARD", PlacementKind: SourcePersistenceBucket,
		BucketVersion: 1, BucketID: 23, SourceRouteDomain: RouteDomainFarm, SourceActorID: 20002,
	}
	if err := source.Validate(fixedResolver{authority: 23}); err != nil {
		t.Fatal(err)
	}
	if err := source.Validate(fixedResolver{authority: 24}); !errors.Is(err, ErrWorkAddressMismatch) {
		t.Fatalf("bucket mismatch err=%v", err)
	}
}

func TestWorkAddressRejectsSourceShapeConfusion(t *testing.T) {
	workID, _ := WorkflowID(10001, "PLANT", "op-1")
	cases := []WorkAddress{
		{WorkID: workID, WorkType: "PLANT", PlacementKind: IndependentWorkBucket, BucketVersion: 1, SourceActorID: 1, SourceRouteDomain: RouteDomainFarm},
		{WorkID: workID, WorkType: "CARE_REWARD", PlacementKind: SourcePersistenceBucket, BucketVersion: 1},
		{WorkID: "not-stable", WorkType: "PLANT", PlacementKind: IndependentWorkBucket, BucketVersion: 1},
	}
	for _, address := range cases {
		if err := address.ValidateShape(); !errors.Is(err, ErrWorkAddressMismatch) {
			t.Fatalf("address=%+v err=%v", address, err)
		}
	}
}
