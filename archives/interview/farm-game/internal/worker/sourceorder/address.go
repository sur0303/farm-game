package sourceorder

import (
	"fmt"
	"strconv"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/identity"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

// WorkAddress builds a source-persistence WorkAddress for Direct enqueue.
func WorkAddress(bucket rediskey.AuthorityBucketRef, workType string, orderID string, sourceDomain workidentity.RouteDomain, sourceActorID uint64) (workidentity.WorkAddress, error) {
	address := workidentity.WorkAddress{
		WorkID: orderID, WorkType: workType, PlacementKind: workidentity.SourcePersistenceBucket,
		BucketVersion: 1, BucketID: bucket.VirtualShard(), SourceRouteDomain: sourceDomain, SourceActorID: sourceActorID,
	}
	if err := AcceptsDirect(address); err != nil {
		return workidentity.WorkAddress{}, err
	}
	return address, nil
}

// WireWorkType maps a SourceOrder type onto the Participant WorkType enum.
// THEFT_VISITOR/THEFT_GUARD share WORK_TYPE_THEFT because the step, not the
// Redis order type, selects SETTLE vs CREDIT.
func WireWorkType(workType string) (contractspb.WorkType, error) {
	switch workType {
	case string(identity.WorkTypeTheftVisitor), string(identity.WorkTypeTheftGuard):
		return contractspb.WorkType_WORK_TYPE_THEFT, nil
	}
	value, ok := contractspb.WorkType_value["WORK_TYPE_"+workType]
	if !ok {
		return contractspb.WorkType_WORK_TYPE_UNSPECIFIED, fmt.Errorf("sourceorder: no wire work type for %q", workType)
	}
	return contractspb.WorkType(value), nil
}

func WireSourceDomain(domain workidentity.RouteDomain) logicrpcpb.RouteDomain {
	if domain == workidentity.RouteDomainPlayer {
		return logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER
	}
	return logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM
}

func CanonicalActorID(actorID uint64) string {
	return strconv.FormatUint(actorID, 10)
}
