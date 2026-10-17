package authoritycomposition

import (
	"context"
	"errors"
	"testing"

	"farmgame/internal/actorrouting"
)

// TestMembershipModeFromEnvironment freezes the AUTHORITY_MEMBERSHIP_MODE
// parsing contract: true-family selects composed, false-family and the empty
// value select legacy, and anything unknown is a startup error (fail closed).
func TestMembershipModeFromEnvironment(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    MembershipMode
		wantErr bool
	}{
		{name: "empty defaults to legacy", raw: "", want: MembershipModeLegacy},
		{name: "true", raw: "true", want: MembershipModeComposed},
		{name: "TRUE", raw: "TRUE", want: MembershipModeComposed},
		{name: "1", raw: "1", want: MembershipModeComposed},
		{name: "on", raw: "on", want: MembershipModeComposed},
		{name: "yes", raw: "yes", want: MembershipModeComposed},
		{name: "composed", raw: "composed", want: MembershipModeComposed},
		{name: "membership", raw: "membership", want: MembershipModeComposed},
		{name: "false", raw: "false", want: MembershipModeLegacy},
		{name: "0", raw: "0", want: MembershipModeLegacy},
		{name: "off", raw: "off", want: MembershipModeLegacy},
		{name: "no", raw: "no", want: MembershipModeLegacy},
		{name: "legacy", raw: "legacy", want: MembershipModeLegacy},
		{name: "unknown fails", raw: "sometimes", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("AUTHORITY_MEMBERSHIP_MODE", tc.raw)
			got, err := MembershipModeFromEnvironment()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("raw=%q: expected an error, got mode=%s", tc.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("raw=%q: unexpected error %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("raw=%q: mode=%s, want %s", tc.raw, got, tc.want)
			}
		})
	}
}

// TestNewRuntimeForModeComposedNeverStartsLegacyRenewal proves the frozen mode
// contract on the composed side: NewMembershipRuntime is the only path, the
// legacy lease table stays empty (so the legacy runRenewal loop, even if it
// were started, would renew zero leases), and every serving shard has exactly
// one scheduler entry (sole renew owner).
func TestNewRuntimeForModeComposedNeverStartsLegacyRenewal(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime, err := NewRuntimeForMode(context.Background(),
		membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"), MembershipModeComposed)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runtime.Close() }()

	if runtime.scheduler == nil {
		t.Fatal("composed mode must install the per-shard lease scheduler")
	}
	if !runtime.ProcessReady() {
		t.Fatal("composed runtime must be process-ready after startup convergence")
	}
	if runtime.ServingShardCount() != 1 {
		t.Fatalf("serving=%d, want 1 (startup snapshot assigns player shard 0)", runtime.ServingShardCount())
	}
	if runtime.scheduler.EntryCount() != 1 {
		t.Fatalf("entries=%d, want 1 (one renew owner per serving shard)", runtime.scheduler.EntryCount())
	}
	// The legacy global runRenewal renews only the legacy lease table.  A
	// composed runtime never fills it: legacy renewal calls stay at zero.
	if len(runtime.leases) != 0 {
		t.Fatalf("composed mode must not populate the legacy lease table, got %d leases", len(runtime.leases))
	}
}

// TestNewRuntimeForModeLegacyNeverStartsPerShardScheduler proves the frozen
// mode contract on the legacy side: New (global runRenewal) is the only path,
// the per-shard scheduler is never installed, and guards/leases come from the
// legacy installLocalGrants path.
func TestNewRuntimeForModeLegacyNeverStartsPerShardScheduler(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime, err := NewRuntimeForMode(context.Background(),
		membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"), MembershipModeLegacy)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runtime.Close() }()

	if runtime.scheduler != nil {
		t.Fatal("legacy mode must never install the per-shard lease scheduler")
	}
	if !runtime.ProcessReady() {
		t.Fatal("legacy runtime must be process-ready")
	}
	if runtime.ServingShardCount() != 1 {
		t.Fatalf("serving=%d, want 1 (legacy installLocalGrants admits player shard 0)", runtime.ServingShardCount())
	}
	if len(runtime.leases) != 1 {
		t.Fatalf("legacy mode must own one legacy lease, got %d", len(runtime.leases))
	}
}

// TestNewRuntimeForModeUnknownModeFails proves there is no silent fallback: an
// unrecognized mode is an error, never a downgrade to either real mode.
func TestNewRuntimeForModeUnknownModeFails(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime, err := NewRuntimeForMode(context.Background(),
		membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"), MembershipMode("sometimes"))
	if err == nil {
		if runtime != nil {
			_ = runtime.Close()
		}
		t.Fatal("unknown membership mode must fail")
	}
}

// TestMembershipRuntimeFarmDomainZeroShardProcessReady verifies the cmd-wiring
// contract for farm-svr: a registered farm holder with zero assigned shards is
// process-ready (composed mode), exactly like the player holder.
func TestMembershipRuntimeFarmDomainZeroShardProcessReady(t *testing.T) {
	fixture := newCompositionFixture(t)
	snapshot := fixture.snap
	snapshot.GlobalRevision = 1
	snapshot.Holders = append(append([]actorrouting.HolderRef(nil), fixture.snap.Holders...), actorrouting.HolderRef{
		InstanceID: "farm-pod-2", BootID: "farm-boot-2", Endpoint: "farm-2.local:9090",
		State: actorrouting.HolderStateActive, Weight: 1,
	})
	snapshot.Farm.Routes = append([]actorrouting.ShardRoute(nil), fixture.snap.Farm.Routes...)
	snapshot.Farm.Routes[0] = actorrouting.ShardRoute{
		// Held by farm-pod-2 with the same generation numbers the fixture uses
		// for farm-pod-1, so no snapshot generation-rollback check trips.
		HolderIndex: 2, State: actorrouting.ShardStateActive,
		ShardRevision:       fixture.snap.Farm.Routes[0].ShardRevision,
		ExpectedWriterEpoch: fixture.snap.Farm.Routes[0].ExpectedWriterEpoch,
		ExpectedLeaseEpoch:  fixture.snap.Farm.Routes[0].ExpectedLeaseEpoch,
		LeaseUntilMs:        fixture.snap.Farm.Routes[0].LeaseUntilMs,
	}
	path := writeSnapshotPath(t, snapshot)
	opts := membershipRuntimeOptions(fixture, "farm-pod-1", "farm-boot-1")
	opts.Domain = actorrouting.DomainFarm
	opts.RouteSnapshotPath = path

	runtime, err := NewRuntimeForMode(context.Background(), opts, MembershipModeComposed)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runtime.Close() }()

	if !runtime.ProcessReady() {
		t.Fatal("registered zero-shard farm holder must be process-ready")
	}
	if runtime.ServingShardCount() != 0 {
		t.Fatalf("serving=%d, want 0", runtime.ServingShardCount())
	}
	if runtime.scheduler == nil || runtime.scheduler.EntryCount() != 0 {
		t.Fatalf("zero-shard farm holder must hold no scheduler entries (scheduler=%v entries=%d)", runtime.scheduler != nil, runtime.scheduler.EntryCount())
	}
	if runtime.CanServeShard(actorrouting.DomainFarm, 0) {
		t.Fatal("zero-shard farm holder must not serve farm shard 0")
	}
}

// TestMembershipRuntimeFarmDomainAdmitsAssignedShard verifies the cmd-wiring
// contract for farm-svr with a real assignment: the startup snapshot assigns
// farm shard 0, ServingShardCount and the scheduler entry both become 1, and
// ProcessReady stays true.
func TestMembershipRuntimeFarmDomainAdmitsAssignedShard(t *testing.T) {
	fixture := newCompositionFixture(t)
	opts := membershipRuntimeOptions(fixture, "farm-pod-1", "farm-boot-1")
	opts.Domain = actorrouting.DomainFarm

	runtime, err := NewRuntimeForMode(context.Background(), opts, MembershipModeComposed)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runtime.Close() }()

	if !runtime.ProcessReady() {
		t.Fatal("assigned farm holder must be process-ready")
	}
	if runtime.ServingShardCount() != 1 {
		t.Fatalf("serving=%d, want 1", runtime.ServingShardCount())
	}
	if runtime.scheduler == nil || runtime.scheduler.EntryCount() != 1 {
		t.Fatalf("entries=%d, want 1 (one renew owner per serving shard)", runtime.scheduler.EntryCount())
	}
	if !runtime.CanServeShard(actorrouting.DomainFarm, 0) {
		t.Fatal("assigned farm holder must serve farm shard 0")
	}
}

// TestNewRuntimeForModeComposedFailureNeverFallsBackToLegacy proves the
// fail-closed wiring contract: when the composed construction fails (an
// unregistered local holder), NewRuntimeForMode returns (nil, err) so the
// command root must fail the process startup -- it must not silently degrade
// to the legacy path.
func TestNewRuntimeForModeComposedFailureNeverFallsBackToLegacy(t *testing.T) {
	fixture := newCompositionFixture(t)
	// Ghost holder: absent from Snapshot.Holders, so buildCompositionCommon
	// fails before any runtime is constructed.
	opts := membershipRuntimeOptions(fixture, "ghost-pod", "ghost-boot")

	runtime, err := NewRuntimeForMode(context.Background(), opts, MembershipModeComposed)
	if err == nil {
		if runtime != nil {
			_ = runtime.Close()
		}
		t.Fatal("composed construction with an unregistered holder must fail")
	}
	if runtime != nil {
		t.Fatalf("composed failure must return (nil, err), got a runtime %v", runtime)
	}
	if !errors.Is(err, ErrLocalHolderNotRegistered) {
		t.Fatalf("failure cause=%v, want ErrLocalHolderNotRegistered", err)
	}
}
