package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	routecore "farmgame/internal/routecontrol"
	core "farmgame/internal/scaleplan"
	testkit "farmgame/internal/testkit/scaleplancontrol"
)

type placementAdapterFixture struct {
	ctx         context.Context
	clock       *testkit.ManualClock
	holders     *testkit.MemoryHolders
	coordinator ExclusiveControlCoordinator
	token       core.ControlOperationToken
	holder      core.HolderRef
	port        core.PlacementExclusionPort
}

func newPlacementAdapterFixture(t *testing.T, excluded bool, revision uint64, owner string) placementAdapterFixture {
	t.Helper()
	ctx := context.Background()
	clock := testkit.NewManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	scope := testCoordinatorScope()
	coordinator := newTestCoordinator(t, clock, time.Minute)
	token, err := coordinator.Acquire(ctx, testCoordinatorRequest("placement-adapter", scope, testCoordinatorOwner("controller-a")))
	if err != nil {
		t.Fatal(err)
	}
	holder := core.HolderRef{
		Domain: actorrouting.DomainPlayer, HolderID: "holder-a", BootID: "boot-a",
		PodUID: "pod-a", WorkloadUID: "workload-a", Ordinal: 0,
	}
	holders := testkit.NewMemoryHolders()
	holders.Set(routecore.HolderRecord{Domain: holder.Domain, HolderID: holder.HolderID, BootID: holder.BootID},
		routecore.PlacementExclusion{
			Domain: holder.Domain, HolderID: holder.HolderID, BootID: holder.BootID,
			Excluded: excluded, Revision: revision, OwnerPlanID: owner,
		})
	port, err := NewPlacementExclusionPort(PlacementExclusionPortOptions{
		Store: holders, Coordinator: coordinator, Scope: scope,
	})
	if err != nil {
		t.Fatal(err)
	}
	return placementAdapterFixture{
		ctx: ctx, clock: clock, holders: holders, coordinator: coordinator,
		token: token, holder: holder, port: port,
	}
}

func TestPlacementExclusionAdapterUsesHolderProductionSeam(t *testing.T) {
	const owner = "plan-a"
	f := newPlacementAdapterFixture(t, false, 7, "")

	state, found, err := f.port.Get(f.ctx, f.holder)
	if err != nil || !found || state.Excluded || state.Revision != 7 || state.Holder != f.holder {
		t.Fatalf("initial state=%+v found=%t err=%v", state, found, err)
	}
	excluded, err := f.port.Exclude(f.ctx, f.holder, core.PlacementExclusionPrecondition{
		ExpectedRevision: 7, OwnerPlanID: owner,
	})
	if err != nil || !excluded.Committed || !excluded.State.Excluded ||
		excluded.State.OwnerPlanID != owner || excluded.State.Revision != 8 {
		t.Fatalf("exclude result=%+v err=%v", excluded, err)
	}

	ctx := WithControlOperationToken(f.ctx, f.token)
	released, err := f.port.ReleaseForOwner(ctx, f.holder, core.PlacementExclusionRelease{
		OwnerPlanID: owner, ExpectedRevision: 8,
	})
	if err != nil || !released.Committed || released.Outcome != core.PlacementReleaseReleased ||
		released.State.Excluded || released.State.OwnerPlanID != "" || released.State.Revision != 9 {
		t.Fatalf("release result=%+v err=%v", released, err)
	}
	if f.holders.ReleaseCalls() != 1 || f.holders.LastReleaseToken() != f.token {
		t.Fatalf("release calls=%d token forwarded=%t", f.holders.ReleaseCalls(), f.holders.LastReleaseToken() == f.token)
	}
	final, found, err := f.port.Get(f.ctx, f.holder)
	if err != nil || !found || final.Excluded || final.OwnerPlanID != "" || final.Revision != 9 {
		t.Fatalf("final state=%+v found=%t err=%v", final, found, err)
	}
}

func TestPlacementExclusionAdapterMapsClosedReleaseOutcomes(t *testing.T) {
	f := newPlacementAdapterFixture(t, true, 8, "plan-a")
	tokenAware, ok := f.port.(TokenAwarePlacementExclusionPort)
	if !ok {
		t.Fatal("production adapter does not expose its adapter-local token seam")
	}
	tests := []struct {
		route routecore.PlacementReleaseOutcome
		core  core.PlacementReleaseOutcome
	}{
		{routecore.PlacementReleaseReleased, core.PlacementReleaseReleased},
		{routecore.PlacementReleaseAlreadyReleased, core.PlacementReleaseAlreadyReleased},
		{routecore.PlacementReleaseByBootRollover, core.PlacementReleaseBootRollover},
		{routecore.PlacementReleaseNotFound, core.PlacementReleaseNotFound},
		{routecore.PlacementReleaseOwnerMismatch, core.PlacementReleaseOwnerMismatch},
		{routecore.PlacementReleaseRevisionConflict, core.PlacementReleaseRevisionConflict},
		{routecore.PlacementReleaseMalformed, core.PlacementReleaseMalformed},
	}
	for _, test := range tests {
		f.holders.SetReleaseOutcome(test.route)
		result, err := tokenAware.ReleaseForOwnerWithToken(f.ctx, f.token, f.holder,
			core.PlacementExclusionRelease{OwnerPlanID: "plan-a", ExpectedRevision: 8})
		if err != nil || result.Outcome != test.core {
			t.Errorf("route=%q result=%+v err=%v want=%q", test.route, result, err, test.core)
		}
	}
	f.holders.SetReleaseOutcome(routecore.PlacementReleaseOutcome("FUTURE_OUTCOME"))
	result, err := tokenAware.ReleaseForOwnerWithToken(f.ctx, f.token, f.holder,
		core.PlacementExclusionRelease{OwnerPlanID: "plan-a", ExpectedRevision: 8})
	if result.Outcome != core.PlacementReleaseUnknown || !errors.Is(err, ErrPlacementExclusionAdapterDrift) {
		t.Fatalf("unknown result=%+v err=%v", result, err)
	}
}

func TestPlacementExclusionAdapterRejectsForeignAndMissingTokens(t *testing.T) {
	f := newPlacementAdapterFixture(t, true, 8, "plan-a")
	foreignCoordinator := newTestCoordinator(t, f.clock, time.Minute)
	foreign, err := foreignCoordinator.Acquire(f.ctx, testCoordinatorRequest(
		"foreign-placement-adapter", testCoordinatorScope(), testCoordinatorOwner("controller-b")))
	if err != nil {
		t.Fatal(err)
	}
	tokenAware, ok := f.port.(TokenAwarePlacementExclusionPort)
	if !ok {
		t.Fatal("production adapter does not expose its adapter-local token seam")
	}
	result, err := tokenAware.ReleaseForOwnerWithToken(f.ctx, foreign, f.holder,
		core.PlacementExclusionRelease{OwnerPlanID: "plan-a", ExpectedRevision: 8})
	if result.Outcome != core.PlacementReleaseUnknown || !errors.Is(err, ErrPlacementExclusionAdapterToken) {
		t.Fatalf("foreign token result=%+v err=%v", result, err)
	}
	if f.holders.ReleaseCalls() != 0 {
		t.Fatalf("foreign token reached store: calls=%d", f.holders.ReleaseCalls())
	}

	noToken, err := NewPlacementExclusionPort(PlacementExclusionPortOptions{
		Store: f.holders, Coordinator: f.coordinator, Scope: testCoordinatorScope(),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err = noToken.ReleaseForOwner(f.ctx, f.holder,
		core.PlacementExclusionRelease{OwnerPlanID: "plan-a", ExpectedRevision: 8})
	if result.Outcome != core.PlacementReleaseUnknown || !errors.Is(err, ErrPlacementExclusionAdapterToken) {
		t.Fatalf("missing token result=%+v err=%v", result, err)
	}
	if f.holders.ReleaseCalls() != 0 {
		t.Fatalf("missing token reached store: calls=%d", f.holders.ReleaseCalls())
	}
}

func TestPlacementExclusionAdapterRejectsInvalidStateProjection(t *testing.T) {
	f := newPlacementAdapterFixture(t, false, 7, "")
	f.holders.Set(routecore.HolderRecord{Domain: f.holder.Domain, HolderID: f.holder.HolderID, BootID: f.holder.BootID},
		routecore.PlacementExclusion{
			Domain: f.holder.Domain, HolderID: "other-holder", BootID: f.holder.BootID,
			Revision: 7,
		})
	if _, _, err := f.port.Get(f.ctx, f.holder); !errors.Is(err, ErrPlacementExclusionAdapterDrift) {
		t.Fatalf("drifted get err=%v", err)
	}
}
