package routeplan

import (
	"context"

	"farmgame/internal/actorrouting"
	"farmgame/internal/routecontrol"
)

type routePlanTestPlacementExclusions struct{}

func newRoutePlanTestPlacementExclusions() *routePlanTestPlacementExclusions {
	return &routePlanTestPlacementExclusions{}
}
func (*routePlanTestPlacementExclusions) GetPlacementExclusion(_ context.Context, domain actorrouting.RouteDomain, holderID, bootID string) (routecontrol.PlacementExclusion, bool, error) {
	return routecontrol.PlacementExclusion{Domain: domain, HolderID: holderID, BootID: bootID, Revision: 1}, true, nil
}
func (*routePlanTestPlacementExclusions) ExcludePlacement(_ context.Context, domain actorrouting.RouteDomain, holderID, bootID string, pre routecontrol.PlacementExclusionPrecondition) (routecontrol.PlacementExclusionResult, error) {
	state := routecontrol.PlacementExclusion{Domain: domain, HolderID: holderID, BootID: bootID, Excluded: true, Revision: pre.ExpectedRevision + 1, OwnerPlanID: pre.OwnerPlanID}
	return routecontrol.PlacementExclusionResult{Committed: true, State: state}, nil
}
func (*routePlanTestPlacementExclusions) ValidatePlacementTarget(context.Context, actorrouting.RouteDomain, string, string) (uint64, error) {
	return 1, nil
}

// ReleasePlacementExclusionForOwner reverses an exclusion owned by the named
// plan. A0.5 only widens the method set; A1R supplies the behavior.
func (*routePlanTestPlacementExclusions) ReleasePlacementExclusionForOwner(_ context.Context, token routecontrol.NestedControlOperationToken, domain actorrouting.RouteDomain, holderID, bootID string, release routecontrol.PlacementExclusionRelease) (routecontrol.PlacementExclusionResult, error) {
	if !token.Issued() {
		return routecontrol.PlacementExclusionResult{}, routecontrol.ErrPlacementExclusionInvalid
	}
	if release.OwnerPlanID == "" || release.ExpectedRevision == 0 {
		return routecontrol.PlacementExclusionResult{}, routecontrol.ErrPlacementExclusionInvalid
	}
	// Nothing was ever excluded here, so the release is vacuously already done.
	return routecontrol.PlacementExclusionResult{
		Committed: false,
		Outcome:   routecontrol.PlacementReleaseAlreadyReleased,
		State: routecontrol.PlacementExclusion{Domain: domain, HolderID: holderID, BootID: bootID,
			Excluded: false, Revision: release.ExpectedRevision},
	}, nil
}
