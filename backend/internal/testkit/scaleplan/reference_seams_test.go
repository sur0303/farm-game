package scaleplan_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/controlfence"
	"farmgame/internal/routecontrol"
	routeplan "farmgame/internal/routeplan"
)

// Seam 4: the durable-stop primitive. The point of the test is that
// DurablyStopped is a conclusion drawn from the stored record rather than a
// flag the caller sets, so a caller cannot assert its way past the evidence.
func TestDurableStopIsDerivedFromTheStoredRecord(t *testing.T) {
	store, err := routeplan.NewMemoryPlanStore(routeplan.DefaultMemoryPlanStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	nowMs := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC).UnixMilli()
	token := controlfence.NewIssuer().Issue("stop-operation", 1)

	command := routeplan.DurableStopCommand{
		OwnerScalePlanID: "scaleplan-1", PlanID: "child-1", ExpectedRevision: 1,
		NowMs: nowMs, Fence: token,
	}

	// A child that is not there cannot be concluded stopped.
	result, err := store.StopOwnedPlanForScaleTermination(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != routeplan.DurableStopNotFound || result.DurablyStopped("scaleplan-1") {
		t.Fatalf("absent child reported stopped: %s", result.Status)
	}

	// A stop with no issued token is refused before anything is read.
	ungated := command
	ungated.Fence = controlfence.NestedControlOperationToken{}
	if _, err := store.StopOwnedPlanForScaleTermination(context.Background(), ungated); err == nil {
		t.Fatal("durable stop accepted an unissued token")
	}
}

// Seam 5: exclusion release. Only three of the seven outcomes may terminate the
// owning plan; the other four keep it BLOCKED. Encoding that here rather than
// in prose is what stops a later reader from reasoning that a missing record
// probably means the victim is gone.
func TestOnlyThreeReleaseOutcomesPermitTermination(t *testing.T) {
	terminal := map[routecontrol.PlacementReleaseOutcome]bool{
		routecontrol.PlacementReleaseReleased:        true,
		routecontrol.PlacementReleaseAlreadyReleased: true,
		routecontrol.PlacementReleaseByBootRollover:  true,

		routecontrol.PlacementReleaseNotFound:         false,
		routecontrol.PlacementReleaseOwnerMismatch:    false,
		routecontrol.PlacementReleaseRevisionConflict: false,
		routecontrol.PlacementReleaseMalformed:        false,
	}
	if len(terminal) != 7 {
		t.Fatalf("the outcome table has %d rows, not seven", len(terminal))
	}
	for outcome, want := range terminal {
		if outcome.TerminalForScalePlan() != want {
			t.Fatalf("%s: terminal=%t want=%t", outcome, outcome.TerminalForScalePlan(), want)
		}
	}
	// An outcome nobody enumerated is not silently safe.
	if routecontrol.PlacementReleaseOutcome("SOMETHING_NEW").TerminalForScalePlan() {
		t.Fatal("an unenumerated outcome permitted termination")
	}
}

// Seam 6: the Bootstrap fence. A gated publisher refuses an unissued token; an
// ungated one takes the G21 path, which is what makes mode=off a true no-op
// rather than a differently-configured writer.
func TestBootstrapFenceIsEnforcedOnlyWhenGated(t *testing.T) {
	store := routecontrol.NewMemoryRouteSnapshotStore()
	publisher, err := routecontrol.NewDurableSnapshotPublisher(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := actorrouting.RouteSnapshot{}
	var zero routecontrol.NestedControlOperationToken

	// Ungated: identical to today's behavior, token or not.
	if _, err := publisher.Bootstrap(context.Background(), zero, snapshot); err != nil {
		if errors.Is(err, routecontrol.ErrSnapshotCASConflict) {
			t.Fatalf("ungated bootstrap was gated: %v", err)
		}
	}

	publisher.RequireControlToken()
	if _, err := publisher.Bootstrap(context.Background(), zero, snapshot); !errors.Is(err, routecontrol.ErrSnapshotCASConflict) {
		t.Fatalf("gated bootstrap accepted an unissued token: %v", err)
	}
	if err := publisher.Publish(context.Background(), zero, snapshot); !errors.Is(err, routecontrol.ErrSnapshotCASConflict) {
		t.Fatalf("gated publish accepted an unissued token: %v", err)
	}
}
