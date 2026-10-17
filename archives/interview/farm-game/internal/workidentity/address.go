package workidentity

import (
	"errors"
	"fmt"
)

type WorkPlacementKind uint8

const (
	WorkPlacementUnspecified WorkPlacementKind = iota
	IndependentWorkBucket
	SourcePersistenceBucket
)

type RouteDomain uint8

const (
	RouteDomainUnspecified RouteDomain = iota
	RouteDomainPlayer
	RouteDomainFarm
)

type WorkAddress struct {
	WorkID            string
	WorkType          string
	PlacementKind     WorkPlacementKind
	BucketVersion     uint32
	BucketID          uint32
	SourceRouteDomain RouteDomain
	SourceActorID     uint64
}

var ErrWorkAddressMismatch = errors.New("workidentity: work address mismatch")

// BucketResolver owns versioned physical placement. W7D validates the address;
// W31-A supplies the concrete stable hash and Authority bucket implementation.
type BucketResolver interface {
	IndependentWorkBucket(workID string, bucketVersion uint32) (uint32, error)
	AuthorityPersistenceBucket(domain RouteDomain, actorID uint64, bucketVersion uint32) (uint32, error)
}

func (a WorkAddress) ValidateShape() error {
	if ValidateStableOpaqueID(a.WorkID) != nil || !validStableName(a.WorkType) || a.BucketVersion == 0 {
		return fmt.Errorf("%w: base fields", ErrWorkAddressMismatch)
	}
	switch a.PlacementKind {
	case IndependentWorkBucket:
		if a.SourceRouteDomain != RouteDomainUnspecified || a.SourceActorID != 0 {
			return fmt.Errorf("%w: independent work has source", ErrWorkAddressMismatch)
		}
	case SourcePersistenceBucket:
		if (a.SourceRouteDomain != RouteDomainPlayer && a.SourceRouteDomain != RouteDomainFarm) || a.SourceActorID == 0 {
			return fmt.Errorf("%w: source work missing authority", ErrWorkAddressMismatch)
		}
	default:
		return fmt.Errorf("%w: placement kind", ErrWorkAddressMismatch)
	}
	return nil
}

func (a WorkAddress) Validate(resolver BucketResolver) error {
	if err := a.ValidateShape(); err != nil {
		return err
	}
	if resolver == nil {
		return fmt.Errorf("%w: bucket resolver required", ErrWorkAddressMismatch)
	}
	var (
		expected uint32
		err      error
	)
	if a.PlacementKind == IndependentWorkBucket {
		expected, err = resolver.IndependentWorkBucket(a.WorkID, a.BucketVersion)
	} else {
		expected, err = resolver.AuthorityPersistenceBucket(a.SourceRouteDomain, a.SourceActorID, a.BucketVersion)
	}
	if err != nil {
		return err
	}
	if expected != a.BucketID {
		return fmt.Errorf("%w: bucket id got=%d want=%d", ErrWorkAddressMismatch, a.BucketID, expected)
	}
	return nil
}
