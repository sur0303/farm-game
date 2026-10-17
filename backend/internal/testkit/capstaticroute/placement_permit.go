package capstaticroute

import (
	"context"
	"sync"

	"farmgame/internal/actorrouting"
	"farmgame/internal/routecontrol"
)

// static initializer fixtures predate dynamic placement exclusion. They still
// provide an explicit controller so Manager construction cannot bypass the
// production contract accidentally.
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
func (a *allowAllPlacementExclusions) ExcludePlacement(_ context.Context, domain actorrouting.RouteDomain, holderID, bootID string, pre routecontrol.PlacementExclusionPrecondition) (routecontrol.PlacementExclusionResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if pre.ExpectedRevision != a.revision {
		return routecontrol.PlacementExclusionResult{}, routecontrol.ErrPlacementExclusionConflict
	}
	a.revision++
	state := routecontrol.PlacementExclusion{Domain: domain, HolderID: holderID, BootID: bootID, Excluded: true, Revision: a.revision, OwnerPlanID: pre.OwnerPlanID}
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
