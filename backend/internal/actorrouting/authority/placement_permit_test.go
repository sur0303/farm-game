package authority

import (
	"context"
	"sync"

	"farmgame/internal/actorrouting"
	"farmgame/internal/routecontrol"
)

// allowAllPlacementExclusions is test-only infrastructure for legacy Manager
// unit tests. Production NewRouteAssignmentManager still requires an explicit
// controller and route-controller supplies the durable HolderRegistry.
type allowAllPlacementExclusions struct {
	mu       sync.Mutex
	revision uint64
}

func newAllowAllPlacementExclusions() *allowAllPlacementExclusions {
	return &allowAllPlacementExclusions{revision: 1}
}

func (a *allowAllPlacementExclusions) GetPlacementExclusion(_ context.Context, domain actorrouting.RouteDomain, holderID, bootID string) (routecontrol.PlacementExclusion, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return routecontrol.PlacementExclusion{Domain: domain, HolderID: holderID, BootID: bootID, Revision: a.revision}, true, nil
}

func (a *allowAllPlacementExclusions) ExcludePlacement(_ context.Context, domain actorrouting.RouteDomain, holderID, bootID string, precondition routecontrol.PlacementExclusionPrecondition) (routecontrol.PlacementExclusionResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if precondition.ExpectedRevision != a.revision {
		return routecontrol.PlacementExclusionResult{}, routecontrol.ErrPlacementExclusionConflict
	}
	a.revision++
	state := routecontrol.PlacementExclusion{Domain: domain, HolderID: holderID, BootID: bootID,
		Excluded: true, Revision: a.revision, OwnerPlanID: precondition.OwnerPlanID}
	return routecontrol.PlacementExclusionResult{Committed: true, State: state}, nil
}

func (a *allowAllPlacementExclusions) ValidatePlacementTarget(context.Context, actorrouting.RouteDomain, string, string) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.revision, nil
}

// ReleasePlacementExclusionForOwner reverses an exclusion owned by the named
// plan. A0.5 only widens the method set; A1R supplies the behavior.
func (a *allowAllPlacementExclusions) ReleasePlacementExclusionForOwner(_ context.Context, token routecontrol.NestedControlOperationToken, domain actorrouting.RouteDomain, holderID, bootID string, release routecontrol.PlacementExclusionRelease) (routecontrol.PlacementExclusionResult, error) {
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
