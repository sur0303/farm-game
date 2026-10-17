package authoritycomposition

import (
	"context"
	"errors"
	"testing"

	"farmgame/internal/actorrouting"
)

// TestStaticMembershipRuntimeRegisteredHolderZeroRoutesReady freezes the first
// static boot-mismatch contract: a registered holder with no local route is a
// valid zero-shard topology and must NOT trip the gate.
func TestStaticMembershipRuntimeRegisteredHolderZeroRoutesReady(t *testing.T) {
	if err := checkLocalRouteBootMismatch(membershipTestSnapshot(1, nil), "holder-2", "boot-2", nil); err != nil {
		t.Fatalf("registered zero-route holder must pass: %v", err)
	}
}

// TestStaticMembershipRuntimeForeignHolderRouteIgnored freezes the second
// contract: a route owned by another holder is never local and must not fail a
// process that happens to be registered in the same snapshot.
func TestStaticMembershipRuntimeForeignHolderRouteIgnored(t *testing.T) {
	snapshot := membershipTestSnapshot(1, []actorrouting.ShardRoute{membershipTestActiveRoute(0, 1, 1, 1)})
	if err := checkLocalRouteBootMismatch(snapshot, "holder-2", "boot-2", nil); err != nil {
		t.Fatalf("foreign holder route must be ignored: %v", err)
	}
}

// TestStaticMembershipRuntimeLocalHolderBootMismatchFailsStartup freezes the
// hardening: an ACTIVE route owned by the local holder ID but expecting a
// different boot must fail the composed startup with ErrLocalRouteBootMismatch
// instead of silently starting as a 0-shard ready process (which would leave
// the shard permanently unserved in the static mode).
func TestStaticMembershipRuntimeLocalHolderBootMismatchFailsStartup(t *testing.T) {
	snapshot := membershipTestSnapshot(1, []actorrouting.ShardRoute{membershipTestActiveRoute(1, 1, 1, 1)})
	err := checkLocalRouteBootMismatch(snapshot, "holder-2", "wrong-boot", nil)
	if !errors.Is(err, ErrLocalRouteBootMismatch) {
		t.Fatalf("local holder boot mismatch must fail: %v", err)
	}

	// End-to-end: the composed runtime construction fails at startup with the
	// same cause, never a silent zero-shard ready.
	fixture := newCompositionFixture(t)
	snap := fixture.snap
	snap.GlobalRevision = 1
	snap.Holders = append([]actorrouting.HolderRef(nil), fixture.snap.Holders...)
	for i := range snap.Holders {
		if snap.Holders[i].InstanceID == "player-pod-1" {
			snap.Holders[i].BootID = "wrong-boot-9"
		}
	}
	path := writeSnapshotPath(t, snap)
	opts := membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1")
	opts.RouteSnapshotPath = path

	runtime, err := NewRuntimeForMode(context.Background(), opts, MembershipModeComposed)
	if err == nil {
		if runtime != nil {
			_ = runtime.Close()
		}
		t.Fatal("assigned-shard boot mismatch must fail composed startup")
	}
	if !errors.Is(err, ErrLocalRouteBootMismatch) {
		t.Fatalf("startup cause=%v, want ErrLocalRouteBootMismatch", err)
	}
}

// TestStaticMembershipRuntimeLocalHolderMatchingBootContinues freezes the
// positive case: an ACTIVE route owned by the local holder ID with the exact
// matching boot proceeds through construction (no false positive).
func TestStaticMembershipRuntimeLocalHolderMatchingBootContinues(t *testing.T) {
	snapshot := membershipTestSnapshot(1, []actorrouting.ShardRoute{membershipTestActiveRoute(1, 1, 1, 1)})
	if err := checkLocalRouteBootMismatch(snapshot, "holder-2", "boot-2", nil); err != nil {
		t.Fatalf("matching local boot route must continue: %v", err)
	}
}

// TestStaticMembershipRuntimeMixedForeignAndMismatchingRouteFails freezes the
// all-or-nothing property: as soon as a single ACTIVE route claims the local
// holder ID with a mismatching boot, the whole static topology is untrusted --
// a foreign-holder route on another shard must NOT mask it.
func TestStaticMembershipRuntimeMixedForeignAndMismatchingRouteFails(t *testing.T) {
	snapshot := actorrouting.RouteSnapshot{
		Realm: "mixed-test", GlobalRevision: 1,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608060002,
		Holders: []actorrouting.HolderRef{
			{InstanceID: "holder-1", BootID: "boot-1", State: actorrouting.HolderStateActive},
			{InstanceID: "holder-2", BootID: "boot-old", State: actorrouting.HolderStateActive}, // stale boot for the local holder
		},
		Player: actorrouting.DomainSnapshot{ShardCount: 2, Routes: []actorrouting.ShardRoute{
			membershipTestActiveRoute(0, 1, 1, 1), // foreign holder route (ignored alone)
			membershipTestActiveRoute(1, 1, 1, 1), // local holder, but holder table says boot-old != process boot-2
		}},
	}
	err := checkLocalRouteBootMismatch(snapshot, "holder-2", "boot-2", nil)
	if !errors.Is(err, ErrLocalRouteBootMismatch) {
		t.Fatalf("mixed foreign+mismatching topology must fail with ErrLocalRouteBootMismatch: %v", err)
	}
}

// TestDynamicMembershipRuntimeLocalHolderOldBootMayStartDegraded registers the
// FUTURE dynamic-watch contract (Remote Watch ON): a new boot may start as a
// 0-shard Ready while RouteManager re-assigns, exposing
// membership_topology_degraded instead of failing startup.  It is
// intentionally NOT implemented in the static runtime; this test stays skipped
// until the dynamic watch lands (contract registered in the RT1B-3C-C
// handoff).
func TestDynamicMembershipRuntimeLocalHolderOldBootMayStartDegraded(t *testing.T) {
	t.Skip("dynamic watch not wired; contract registered in RT1B-3C-C handoff")
}
