package authoritycomposition

import (
	"context"
	"encoding/json"
	"errors"
	"farmgame/internal/controlfence"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	routeauthority "farmgame/internal/logic/actor/authority"
	"farmgame/internal/storage/authoritystore"
)

// membershipRuntimeOptions builds Options for the player holder of the shared
// composition fixture with explicit per-shard scheduler tuning.
func membershipRuntimeOptions(fixture *compositionFixture, holder, boot string) Options {
	opts := compositionOptions(fixture, actorrouting.DomainPlayer, holder, boot)
	opts.LeaseBaseDelay = 20 * time.Millisecond
	opts.LeaseMaxDelay = 200 * time.Millisecond
	opts.LeaseJitter = 5 * time.Millisecond
	opts.RenewAttemptTimeout = 500 * time.Millisecond
	// The recovery cadence must satisfy the RT1B-3D fail-closed budget
	// 2*timeout + max + jitter + slack < SafetyMargin (100ms here).
	opts.RenewRecoveryTimeout = 20 * time.Millisecond
	opts.LeaseRecoveryBaseDelay = 20 * time.Millisecond
	opts.LeaseRecoveryMaxDelay = 20 * time.Millisecond
	opts.LeaseRecoveryJitterMax = 5 * time.Millisecond
	opts.LeaseRecoverySchedulerSlack = 0
	opts.MembershipMaxRetries = 3
	opts.MembershipRetryDelay = 10 * time.Millisecond
	return opts
}

func newMembershipRuntimeFromOptions(t *testing.T, opts Options) *Runtime {
	t.Helper()
	runtime, err := NewMembershipRuntime(context.Background(), opts)
	if err != nil {
		if runtime != nil {
			_ = runtime.Close()
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	return runtime
}

type trackingMembershipInstaller struct {
	inner    MembershipInstaller
	prepares atomic.Int32
	commits  atomic.Int32
}

func (i *trackingMembershipInstaller) PrepareMembership(
	ctx context.Context,
	member ShardMembership,
) (*PreparedMembership, error) {
	i.prepares.Add(1)
	return i.inner.PrepareMembership(ctx, member)
}

func (i *trackingMembershipInstaller) CommitPreparedMembership(
	ctx context.Context,
	prepared PreparedMembership,
) error {
	i.commits.Add(1)
	return i.inner.CommitPreparedMembership(ctx, prepared)
}

func TestMembershipInstallerDecoratorIsSharedWithReconciler(t *testing.T) {
	fixture := newCompositionFixture(t)
	opts := membershipRuntimeOptions(
		fixture,
		"player-pod-1",
		"player-boot-1",
	)
	var decorated *trackingMembershipInstaller
	opts.MembershipInstallerDecorator = func(
		inner MembershipInstaller,
	) (MembershipInstaller, error) {
		decorated = &trackingMembershipInstaller{inner: inner}
		return decorated, nil
	}
	runtime := newMembershipRuntimeFromOptions(t, opts)
	if decorated == nil || runtime.MembershipInstaller() != decorated {
		t.Fatal("runtime did not expose the reconciler's decorated installer")
	}
	if decorated.prepares.Load() == 0 || decorated.commits.Load() == 0 {
		t.Fatalf(
			"reconciler bypassed decorator prepares=%d commits=%d",
			decorated.prepares.Load(),
			decorated.commits.Load(),
		)
	}
}

func writeSnapshotPath(t *testing.T, snapshot actorrouting.RouteSnapshot) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "route.json")
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// zeroShardSnapshot registers player-pod-1 without assigning it any player
// shard.  The player route is held by a second PLAYER-domain holder so the
// local holder has zero desired shards while the topology fingerprint (domain
// shard counts + hash profile) stays stable across revisions and no holder is
// shared across domains.
func zeroShardSnapshot(t *testing.T, fixture *compositionFixture) actorrouting.RouteSnapshot {
	t.Helper()
	snapshot := fixture.snap
	snapshot.GlobalRevision = 1
	// Deep-copy holders and routes so mutating the derived snapshot never
	// corrupts the shared fixture (slice headers share the backing array).
	snapshot.Holders = append([]actorrouting.HolderRef(nil), fixture.snap.Holders...)
	snapshot.Holders = append(snapshot.Holders, actorrouting.HolderRef{
		InstanceID: "player-pod-2", BootID: "player-boot-2", Endpoint: "player-2.local:9100",
		State: actorrouting.HolderStateActive, Weight: 1,
	})
	snapshot.Player.Routes = append([]actorrouting.ShardRoute(nil), fixture.snap.Player.Routes...)
	snapshot.Player.Routes[0] = actorrouting.ShardRoute{
		// Held by player-pod-2 with the same generation numbers the fixture
		// uses for player-pod-1, so later revisions that reassign the shard do
		// not trip the snapshot generation-rollback check.
		HolderIndex: 2, State: actorrouting.ShardStateActive,
		ShardRevision:       fixture.snap.Player.Routes[0].ShardRevision,
		ExpectedWriterEpoch: fixture.snap.Player.Routes[0].ExpectedWriterEpoch,
		ExpectedLeaseEpoch:  fixture.snap.Player.Routes[0].ExpectedLeaseEpoch,
		LeaseUntilMs:        fixture.snap.Player.Routes[0].LeaseUntilMs,
	}
	return snapshot
}

func TestMembershipRuntimeDeadlineProjectionDoesNotReplaceGeneration(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	before := runtime.MembershipReconciler().CurrentInstalledMembership()
	if len(before) != 1 {
		t.Fatalf("startup memberships=%+v", before)
	}
	installed := before[0]
	beforeStats := runtime.MembershipEvidenceStats()
	next := actorrouting.CloneRouteSnapshot(runtime.AssignmentProvider.Snapshot())
	next.GlobalRevision++
	next.Player.Routes[0].LeaseUntilMs += time.Minute.Milliseconds()
	result := runtime.observeMembership(next)
	if result.Rejected || result.Stale || !result.FullyReconciled || len(result.Actions) != 0 {
		t.Fatalf("deadline projection result=%+v", result)
	}
	after := runtime.MembershipReconciler().CurrentInstalledMembership()
	if len(after) != 1 || !after[0].Generation.equal(installed.Generation) || after[0].Generation.LeaseUntilMs != next.Player.Routes[0].LeaseUntilMs {
		t.Fatalf("deadline projection changed immutable generation or failed to advance deadline before=%+v after=%+v", installed, after)
	}
	afterStats := runtime.MembershipEvidenceStats()
	if afterStats.ActionTokenSequence != beforeStats.ActionTokenSequence || afterStats.SuccessfulAdds != beforeStats.SuccessfulAdds ||
		afterStats.SuccessfulReplaces != beforeStats.SuccessfulReplaces || afterStats.SuccessfulRemoves != beforeStats.SuccessfulRemoves {
		t.Fatalf("deadline projection created membership churn before=%+v after=%+v", beforeStats, afterStats)
	}
	if !runtime.ProcessReady() || runtime.FatalErr() != nil || runtime.scheduler.EntryCount() != 1 || !runtime.CanServeShard(actorrouting.DomainPlayer, 0) {
		t.Fatalf("deadline projection degraded runtime ready=%v fatal=%v entries=%d serve=%v",
			runtime.ProcessReady(), runtime.FatalErr(), runtime.scheduler.EntryCount(), runtime.CanServeShard(actorrouting.DomainPlayer, 0))
	}
}

// TestMembershipRuntimeStartupZeroShardProcessReady verifies hard invariant 3
// (registered holder + zero shard → ProcessReady=true after initial
// reconciliation) and 10 (the composed runtime does not bulk-load actors: it
// constructs only route/authority seams, no actor registry).
func TestMembershipRuntimeStartupZeroShardProcessReady(t *testing.T) {
	fixture := newCompositionFixture(t)
	opts := membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1")
	opts.RouteSnapshotPath = writeSnapshotPath(t, zeroShardSnapshot(t, fixture))
	runtime := newMembershipRuntimeFromOptions(t, opts)

	if !runtime.ProcessReady() {
		t.Fatal("registered zero-shard holder must be process-ready after initial reconciliation")
	}
	if runtime.ServingShardCount() != 0 {
		t.Fatalf("ServingShardCount=%d, want 0", runtime.ServingShardCount())
	}
	if runtime.CanServeShard(actorrouting.DomainPlayer, 0) {
		t.Fatal("zero-shard holder must fail closed for every shard")
	}
	assertMembershipRuntimeConsistent(t, runtime.MembershipReconciler(), runtime.Guards, runtime.scheduler)
	if len(runtime.leases) != 0 {
		t.Fatal("legacy renewal table must stay empty on the membership path")
	}
}

// TestMembershipRuntimeStartupAdmitsAssignedShard verifies hard invariants 1
// (no legacy runRenewal), 2 (one renew owner per generation) and 4 (an assigned
// startup shard is safely admitted before ready).
func TestMembershipRuntimeStartupAdmitsAssignedShard(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))

	if !runtime.ProcessReady() {
		t.Fatal("assigned holder must be process-ready after initial reconciliation")
	}
	if !runtime.CanServeShard(actorrouting.DomainPlayer, 0) {
		t.Fatal("installed player shard 0 must be servable")
	}
	if runtime.ServingShardCount() != 1 {
		t.Fatalf("ServingShardCount=%d, want 1", runtime.ServingShardCount())
	}
	if runtime.scheduler.EntryCount() != 1 {
		t.Fatalf("exactly one scheduler entry per installed generation, got %d", runtime.scheduler.EntryCount())
	}
	if len(runtime.leases) != 0 {
		t.Fatal("legacy renewal table must stay empty: runRenewal must never start")
	}
	assertMembershipRuntimeConsistent(t, runtime.MembershipReconciler(), runtime.Guards, runtime.scheduler)
}

// TestMembershipRuntimeMigrationRemovesOldGenerationKeepsReady drives a planned
// cross-holder migration (G1 transfers away) through the composed runtime.  It
// verifies hard invariant 8 (the runtime follows 0→1→G1→0 snapshot flows) and
// invariant 2 (removing a generation leaves exactly zero renew owners).  The
// same-holder generation REPLACE mechanism itself is exercised by the RT1B-3B
// scheduler tests; the runtime composition layer proves the observer drives the
// reconciler through ADD and REMOVE with ProcessReady stable.
func TestMembershipRuntimeMigrationRemovesOldGenerationKeepsReady(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	ctx := context.Background()
	if runtime.scheduler.EntryCount() != 1 {
		t.Fatalf("startup must register one renew owner, got %d", runtime.scheduler.EntryCount())
	}

	old := runtime.GuardsSnapshotForTest(actorrouting.DomainPlayer, 0)
	transfer, err := runtime.Store.TransferShardAuthority(ctx, authoritystore.TransferShardAuthorityRequest{
		Caller:       authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-2", BootID: "route-manager-boot-2"},
		TransitionID: "transfer-rt3c-2", AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: old, NewHolderID: "player-pod-2", NewHolderBootID: "player-boot-2", RequestedTTLms: 5000,
	})
	if err != nil || transfer.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("transfer=%+v err=%v", transfer, err)
	}
	next := runtime.AssignmentProvider.Snapshot()
	next.GlobalRevision = 2
	next.Holders = append(next.Holders, actorrouting.HolderRef{InstanceID: "player-pod-2", BootID: "player-boot-2", Endpoint: "player-2.local:9100", State: actorrouting.HolderStateActive, Weight: 1})
	next.Player.Routes[0] = actorrouting.ShardRoute{
		HolderIndex: 2, State: actorrouting.ShardStateActive,
		ShardRevision: transfer.Authority.AssignmentRevision, ExpectedWriterEpoch: transfer.Authority.WriterEpoch,
		ExpectedLeaseEpoch: transfer.Authority.LeaseEpoch, LeaseUntilMs: transfer.Authority.LeaseUntilMs,
	}
	if err := runtime.publisher.Publish(ctx, controlfence.NestedControlOperationToken{}, next); err != nil {
		t.Fatal(err)
	}

	// The membership observer drives the reconciler to REMOVE the old
	// generation: the guard stops admitting and the entry disappears.
	waitLeaseCondition(t, func() bool {
		snapshot, ok := runtime.Guards.Snapshot(actorrouting.DomainPlayer, 0)
		return (!ok || !snapshot.Admitted) && runtime.scheduler.EntryCount() == 0
	})
	if runtime.scheduler.EntryCount() != 0 {
		t.Fatalf("migration must leave zero renew owners, got %d", runtime.scheduler.EntryCount())
	}
	if !runtime.ProcessReady() {
		t.Fatal("a planned migration must keep the process ready")
	}
	assertMembershipRuntimeConsistent(t, runtime.MembershipReconciler(), runtime.Guards, runtime.scheduler)
}

// TestMembershipRuntimeLostGenerationKeepsProcessReady verifies hard invariant
// 5: a normal lost generation (repository proves the generation moved away)
// terminates only the target shard and never lowers ProcessReady.
func TestMembershipRuntimeLostGenerationKeepsProcessReady(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))

	old := runtime.GuardsSnapshotForTest(actorrouting.DomainPlayer, 0)
	transfer, err := runtime.Store.TransferShardAuthority(context.Background(), authoritystore.TransferShardAuthorityRequest{
		Caller:       authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-2", BootID: "route-manager-boot-2"},
		TransitionID: "transfer-rt3c-lg", AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: old, NewHolderID: "player-pod-2", NewHolderBootID: "player-boot-2", RequestedTTLms: 5000,
	})
	if err != nil || transfer.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("transfer=%+v err=%v", transfer, err)
	}
	// The snapshot is NOT updated: only the repository proves the loss.  The
	// scheduler entry sees FenceMismatch, fences the target shard and stops.
	waitLeaseCondition(t, func() bool { return runtime.scheduler.EntryCount() == 0 })
	snapshot, ok := runtime.Guards.Snapshot(actorrouting.DomainPlayer, 0)
	if !ok || snapshot.Admitted {
		t.Fatal("lost generation must fence the target shard locally")
	}
	if !runtime.ProcessReady() {
		t.Fatal("a normal lost generation must not lower ProcessReady")
	}
	assertMembershipRuntimeConsistent(t, runtime.MembershipReconciler(), runtime.Guards, runtime.scheduler)
}

// TestMembershipRuntimeFenceFailureLowersProcessReady verifies hard invariant 6:
// a lost generation that cannot be fenced locally (guard still admitted) raises
// Scheduler.Fatal and pulls ProcessReady down.
func TestMembershipRuntimeFenceFailureLowersProcessReady(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))

	old := runtime.GuardsSnapshotForTest(actorrouting.DomainPlayer, 0)
	transfer, err := runtime.Store.TransferShardAuthority(context.Background(), authoritystore.TransferShardAuthorityRequest{
		Caller:       authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-2", BootID: "route-manager-boot-2"},
		TransitionID: "transfer-rt3c-ff", AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: old, NewHolderID: "player-pod-2", NewHolderBootID: "player-boot-2", RequestedTTLms: 5000,
	})
	if err != nil || transfer.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("transfer=%+v err=%v", transfer, err)
	}
	// Simulate an un-fencable lost generation: the renewal runtime has no
	// handle, so PrepareRenew reports LostGeneration without fencing the still
	// admitted local guard; OnFatal must pull ProcessReady down.
	runtime.scheduler.mu.Lock()
	if e, ok := runtime.scheduler.entries[adapterShardKey(0)]; ok {
		e.runtime = &routeauthority.HolderLeaseRuntime{}
	}
	runtime.scheduler.mu.Unlock()

	waitLeaseCondition(t, func() bool { return !runtime.ProcessReady() })
	if !runtime.scheduler.Fatal() {
		t.Fatal("an un-fencable lost generation must raise Scheduler.Fatal")
	}
}

// TestMembershipRuntimeTopologyMismatchFailsClosed verifies hard invariant 7:
// a topology change after the fingerprint is sealed rejects the snapshot and
// fails the process closed.
func TestMembershipRuntimeTopologyMismatchFailsClosed(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	if !runtime.ProcessReady() {
		t.Fatal("runtime must be ready before the topology mutation")
	}
	next := runtime.AssignmentProvider.Snapshot()
	next.GlobalRevision = 2
	next.HashSeed = compositionHashSeed + 1 // changes the sealed fingerprint
	result := runtime.observeMembership(next)
	if !result.Rejected {
		t.Fatal("a topology change must be rejected by the reconciler")
	}
	if runtime.ProcessReady() {
		t.Fatal("a topology mismatch must fail the process closed")
	}
	// enterFatal must fail the LOCAL DATA PLANE closed immediately, not just
	// flip readiness: no serving shard, no admitted guard, no scheduler entry.
	if runtime.ServingShardCount() != 0 {
		t.Fatalf("topology fatal left serving shards: %d", runtime.ServingShardCount())
	}
	if runtime.scheduler.EntryCount() != 0 {
		t.Fatalf("topology fatal left scheduler entries: %d", runtime.scheduler.EntryCount())
	}
	for _, snap := range runtime.Guards.SnapshotAll() {
		if snap.Admitted {
			t.Fatalf("topology fatal left an admitted guard: %+v", snap.Authority)
		}
	}
}

// TestMembershipRuntimeFatalIsStickyAfterStartup verifies P1-1: once the
// one-way state machine enters Fatal (un-fencable lost generation), no later
// successful observe can ever revive ProcessReady.
func TestMembershipRuntimeFatalIsStickyAfterStartup(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))

	old := runtime.GuardsSnapshotForTest(actorrouting.DomainPlayer, 0)
	transfer, err := runtime.Store.TransferShardAuthority(context.Background(), authoritystore.TransferShardAuthorityRequest{
		Caller:       authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-2", BootID: "route-manager-boot-2"},
		TransitionID: "transfer-rt3c-fatal-sticky", AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: old, NewHolderID: "player-pod-2", NewHolderBootID: "player-boot-2", RequestedTTLms: 5000,
	})
	if err != nil || transfer.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("transfer=%+v err=%v", transfer, err)
	}
	// Un-fencable lost generation → OnFatal → ready=false.
	runtime.scheduler.mu.Lock()
	if e, ok := runtime.scheduler.entries[adapterShardKey(0)]; ok {
		e.runtime = &routeauthority.HolderLeaseRuntime{}
	}
	runtime.scheduler.mu.Unlock()
	waitLeaseCondition(t, func() bool { return !runtime.ProcessReady() })

	// After Fatal the reconciler is closed by enterFatal: any late snapshot is
	// rejected as Closed and ready stays false (Fatal never revives).
	result := runtime.observeMembership(runtime.AssignmentProvider.Snapshot())
	if !result.Closed {
		t.Fatalf("a snapshot after fatal must be rejected as Closed, got %+v", result)
	}
	if runtime.ProcessReady() {
		t.Fatal("Fatal must never revive to Ready")
	}
}

// TestMembershipRuntimeStartupPartialFailureFailsClosed verifies P1-2: a
// startup snapshot with an un-installable shard fails the whole construction
// instead of returning a half-admitted runtime.
func TestMembershipRuntimeStartupPartialFailureFailsClosed(t *testing.T) {
	fixture := newCompositionFixture(t)
	snapshot := fixture.snap
	snapshot.Player.ShardCount = 2
	snapshot.Player.Routes = append([]actorrouting.ShardRoute(nil), snapshot.Player.Routes...)
	// Shard 1 uses a generation the repository has never seen, so the adapter
	// cannot prove it and the startup must fail closed.
	snapshot.Player.Routes = append(snapshot.Player.Routes, actorrouting.ShardRoute{
		HolderIndex: 0, State: actorrouting.ShardStateActive,
		ShardRevision: 99, ExpectedWriterEpoch: 99, ExpectedLeaseEpoch: 99,
		LeaseUntilMs: time.Now().Add(30 * time.Second).UnixMilli(),
	})
	opts := membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1")
	opts.RouteSnapshotPath = writeSnapshotPath(t, snapshot)
	runtime, err := newMembershipRuntimeForTest(context.Background(), opts)
	if err == nil {
		_ = runtime.Close()
		t.Fatal("startup with an un-installable shard must fail closed")
	}
	if runtime == nil {
		t.Fatal("failed construction must return the fail-closed runtime for external probing")
	}
	// The ordered plan installs shard 0 before shard 1, so shard 0 really
	// reached Admitted/Entry before shard 1 failed; the returned runtime must
	// prove the partial success was rolled back: no admitted guard, no active
	// entry, no serving shard.
	if runtime.ServingShardCount() != 0 {
		t.Fatalf("rollback left serving shards: %d", runtime.ServingShardCount())
	}
	if runtime.scheduler.EntryCount() != 0 {
		t.Fatalf("rollback left scheduler entries: %d", runtime.scheduler.EntryCount())
	}
	for _, snap := range runtime.Guards.SnapshotAll() {
		if snap.Admitted {
			t.Fatalf("rollback left an admitted guard: %+v", snap.Authority)
		}
	}
	_ = runtime.Close()
	// The production contract stays conventional: NewMembershipRuntime returns
	// (nil, err) on failure so a failed runtime can never be installed.
	if prod, prodErr := NewMembershipRuntime(context.Background(), opts); prod != nil || prodErr == nil {
		if prod != nil {
			_ = prod.Close()
		}
		t.Fatal("production construction must return (nil, err) on failure")
	}
}

func TestMembershipRuntimeStartupDeadlineAtLocalPhaseFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name  string
		phase membershipActionPhase
	}{
		{name: "after-reserve", phase: membershipActionAfterReserve},
		{name: "after-guard-install", phase: membershipActionAfterGuardCommit},
		{name: "after-activate", phase: membershipActionAfterActivate},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCompositionFixture(t)
			opts := membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			reached := false
			opts.membershipActionHook = func(phase membershipActionPhase) {
				if phase == test.phase {
					reached = true
					cancel()
				}
			}

			runtime, err := newMembershipRuntimeForTest(ctx, opts)
			if err == nil {
				if runtime != nil {
					_ = runtime.Close()
				}
				t.Fatal("startup action canceled at local phase unexpectedly succeeded")
			}
			if !reached || runtime == nil {
				t.Fatalf("phase reached=%v runtime=%v err=%v", reached, runtime, err)
			}
			if runtime.ProcessReady() || runtime.ServingShardCount() != 0 || runtime.scheduler.EntryCount() != 0 {
				t.Fatalf("failed startup ready=%v serving=%d entries=%d", runtime.ProcessReady(), runtime.ServingShardCount(), runtime.scheduler.EntryCount())
			}
			runtime.scheduler.mu.Lock()
			reservations := len(runtime.scheduler.reservations)
			runtime.scheduler.mu.Unlock()
			if reservations != 0 {
				t.Fatalf("failed startup reservations=%d want 0", reservations)
			}
			reconciler := runtime.MembershipReconciler()
			if len(reconciler.CurrentInstalledMembership()) != 0 || reconciler.LastFullyReconciledRevision() != 0 {
				t.Fatalf("failed startup installed=%d revision=%d", len(reconciler.CurrentInstalledMembership()), reconciler.LastFullyReconciledRevision())
			}
			reconciler.mu.Lock()
			pending, degraded := len(reconciler.pending), len(reconciler.degraded)
			reconciler.mu.Unlock()
			if pending == 0 || degraded == 0 {
				t.Fatalf("failed action state pending=%d degraded=%d", pending, degraded)
			}
			for _, guard := range runtime.Guards.SnapshotAll() {
				if guard.Admitted {
					t.Fatalf("failed startup left admitted guard: %+v", guard.Authority)
				}
			}
			_ = runtime.Close()
		})
	}
}

// TestRejectedRouteSnapshotNeverReachesMembershipReconciler verifies P1-3:
// a snapshot rejected at the provider's validation point never reaches the
// reconciler, so the route table and the membership runtime never diverge.
func TestRejectedRouteSnapshotNeverReachesMembershipReconciler(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	ctx := context.Background()
	installedBefore := len(runtime.MembershipReconciler().CurrentInstalledMembership())

	next := runtime.AssignmentProvider.Snapshot()
	next.GlobalRevision = 2
	next.Player.Routes[0].ShardRevision = next.Player.Routes[0].ShardRevision - 1 // generation rollback
	if err := runtime.publisher.Publish(ctx, controlfence.NestedControlOperationToken{}, next); err == nil {
		t.Fatal("a generation-rollback snapshot must be rejected by the provider")
	}
	if got := len(runtime.MembershipReconciler().CurrentInstalledMembership()); got != installedBefore {
		t.Fatalf("rejected snapshot reached the reconciler: installed %d → %d", installedBefore, got)
	}
	if !runtime.ProcessReady() {
		t.Fatal("a rejected publish must not lower ready")
	}
}

// TestMembershipRuntimeRemoveFenceFailureImmediatelyLowersReady verifies P1-4:
// a REMOVE fence failure whose exact old generation is still admitted lowers
// ProcessReady immediately instead of waiting for the next scheduler renewal.
// (With the current purely-local FenceIfMatches this state cannot occur in
// production, but the defensive check must hold if a future adapter adds a
// store-backed fence.)
func TestMembershipRuntimeRemoveFenceFailureImmediatelyLowersReady(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	installed := runtime.MembershipReconciler().CurrentInstalledMembership()
	if len(installed) != 1 {
		t.Fatalf("want one installed membership, got %d", len(installed))
	}
	runtime.checkMembershipResult(ObserveResult{
		Degraded: []ShardDegraded{{
			Shard: adapterShardKey(0), Action: MembershipActionRemove, OldGeneration: installed[0].Generation,
		}},
	})
	if runtime.ProcessReady() {
		t.Fatal("a remove fence failure with the old generation still admitted must lower ready immediately")
	}
}

// TestMembershipRuntimeRemoveCleanupFailureAfterSuccessfulFenceKeepsReady
// verifies P1-4: once the exact old generation is already fenced, a failed
// REMOVE cleanup is safe to retry in the background and does not lower ready.
func TestMembershipRuntimeRemoveCleanupFailureAfterSuccessfulFenceKeepsReady(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	installed := runtime.MembershipReconciler().CurrentInstalledMembership()
	if len(installed) != 1 {
		t.Fatalf("want one installed membership, got %d", len(installed))
	}
	// Simulate the successful fence: the guard for the old generation stops
	// admitting (it stays in the registry, fenced).
	snapshot, ok := runtime.Guards.Snapshot(actorrouting.DomainPlayer, 0)
	if !ok {
		t.Fatal("guard missing")
	}
	if err := runtime.Guards.FenceIfMatches(actorrouting.DomainPlayer, 0, snapshot.Authority); err != nil {
		t.Fatal(err)
	}

	runtime.checkMembershipResult(ObserveResult{
		Degraded: []ShardDegraded{{
			Shard: adapterShardKey(0), Action: MembershipActionRemove, OldGeneration: installed[0].Generation,
		}},
	})
	if !runtime.ProcessReady() {
		t.Fatal("a remove cleanup failure after a successful fence must keep ready (background retry is safe)")
	}
}

// TestMembershipRuntimeCloseFailClosedAndIdempotent verifies P1-5: after
// Close there are no admitted guards, no scheduler entries, zero serving
// shards, and Close is idempotent.
func TestMembershipRuntimeCloseFailClosedAndIdempotent(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	if runtime.ServingShardCount() != 1 {
		t.Fatalf("pre-close serving=%d, want 1", runtime.ServingShardCount())
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal("Close must be idempotent")
	}
	if runtime.ServingShardCount() != 0 {
		t.Fatalf("post-close serving=%d, want 0", runtime.ServingShardCount())
	}
	if runtime.scheduler.EntryCount() != 0 {
		t.Fatalf("post-close entries=%d, want 0", runtime.scheduler.EntryCount())
	}
	for _, snap := range runtime.Guards.SnapshotAll() {
		if snap.Admitted {
			t.Fatalf("Close left an admitted guard: %+v", snap.Authority)
		}
	}
}

func TestMembershipRuntimeCloseJoinsContextBoundObserverAndRetry(t *testing.T) {
	for _, test := range []struct {
		name       string
		blockRetry bool
		wantCalls  int32
	}{
		{name: "observer", wantCalls: 1},
		{name: "retry", blockRetry: true, wantCalls: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCompositionFixture(t)
			opts := membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1")
			opts.RenewInterval = 20 * time.Millisecond
			opts.MembershipRetryDelay = 2 * time.Millisecond
			runtime, err := NewMembershipRuntime(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close()

			entered := make(chan struct{})
			var enteredOnce sync.Once
			var calls atomic.Int32
			runtime.adapter.prepareMembershipHook = func(ctx context.Context) error {
				call := calls.Add(1)
				if test.blockRetry && call == 1 {
					return errors.New("injected first observe failure")
				}
				enteredOnce.Do(func() { close(entered) })
				<-ctx.Done()
				return ctx.Err()
			}

			next := actorrouting.CloneRouteSnapshot(runtime.AssignmentProvider.Snapshot())
			next.GlobalRevision++
			next.Player.Routes[0].ShardRevision++
			next.Player.Routes[0].ExpectedWriterEpoch++
			next.Player.Routes[0].ExpectedLeaseEpoch++
			if err := runtime.publisher.Publish(context.Background(), controlfence.NestedControlOperationToken{}, next); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatalf("%s Port call did not block", test.name)
			}

			closeDone := make(chan error, 1)
			go func() { closeDone <- runtime.Close() }()
			select {
			case err := <-closeDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("Close deadlocked joining blocked %s Port", test.name)
			}
			if got := calls.Load(); got != test.wantCalls {
				t.Fatalf("%s Port calls=%d want %d", test.name, got, test.wantCalls)
			}
			if result := runtime.MembershipReconciler().Reconcile(); !result.Closed {
				t.Fatalf("post-Close retry was not joined: %+v", result)
			}
			if runtime.ServingShardCount() != 0 || runtime.scheduler.EntryCount() != 0 {
				t.Fatalf("post-Close serving=%d entries=%d", runtime.ServingShardCount(), runtime.scheduler.EntryCount())
			}
			for _, guard := range runtime.Guards.SnapshotAll() {
				if runtime.ownsGuard(guard) && guard.Admitted {
					t.Fatalf("post-Close late Observe re-admitted guard: %+v", guard.Authority)
				}
			}
		})
	}
}

// TestMembershipRuntimeCloseRejectsLateSnapshot verifies P1-5: a snapshot
// observed after Close is rejected and can never re-admit a shard.
func TestMembershipRuntimeCloseRejectsLateSnapshot(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	result := runtime.observeMembership(runtime.AssignmentProvider.Snapshot())
	if !result.Closed {
		t.Fatal("a late snapshot must be rejected after Close")
	}
	if runtime.ServingShardCount() != 0 {
		t.Fatalf("late snapshot re-admitted a shard: serving=%d", runtime.ServingShardCount())
	}
}

// TestRuntimeOwnershipFilterDoesNotTouchForeignGuards verifies that
// failClosedLocalState / Close / ServingShardCount all route through
// ownsGuard (domain + holder + boot), so a shared registry can never let one
// runtime fence/remove or count another runtime's guards.
func TestRuntimeOwnershipFilterDoesNotTouchForeignGuards(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))

	own := runtime.ownsGuard(routeauthority.LeaseGenerationSnapshot{Authority: authoritystore.ShardWriteAuthority{
		Domain: actorrouting.DomainPlayer, HolderID: "player-pod-1", HolderBootID: "player-boot-1",
	}})
	if !own {
		t.Fatal("the runtime's own domain/holder/boot guard must be owned")
	}
	foreigns := map[string]authoritystore.ShardWriteAuthority{
		"foreign-domain": {Domain: actorrouting.DomainFarm, HolderID: "farm-pod-1", HolderBootID: "farm-boot-1"},
		"foreign-holder": {Domain: actorrouting.DomainPlayer, HolderID: "player-pod-2", HolderBootID: "player-boot-2"},
		"foreign-boot":   {Domain: actorrouting.DomainPlayer, HolderID: "player-pod-1", HolderBootID: "player-boot-2"},
	}
	for name, authority := range foreigns {
		if runtime.ownsGuard(routeauthority.LeaseGenerationSnapshot{Authority: authority}) {
			t.Fatalf("%s guard must not be owned by this runtime", name)
		}
	}

	// ServingShardCount and single-shard admission both require the runtime's
	// own domain/holder/boot. A foreign ACTIVE guard in a shared registry must
	// never be surfaced as locally servable.
	if runtime.ServingShardCount() != 1 {
		t.Fatalf("ServingShardCount=%d, want 1 (own player shard only)", runtime.ServingShardCount())
	}
	now := time.Now()
	foreignGuard, err := authoritystore.NewHolderLeaseGuard("farm-pod-1", "farm-boot-1", time.Millisecond, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	foreignAuthority := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: fixture.profile.AuthorityLayoutVersion,
		HashVersion:            fixture.snap.HashVersion, HashSeed: fixture.snap.HashSeed,
		Domain: actorrouting.DomainFarm, VirtualShard: 0, HolderID: "farm-pod-1", HolderBootID: "farm-boot-1",
		AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1,
		State: authoritystore.ShardAuthorityActive, LeaseUntilMs: now.Add(time.Minute).UnixMilli(),
	}
	if err := foreignGuard.InstallGrant(now, authoritystore.ShardAuthorityTransitionResult{
		Status: authoritystore.ShardTransitionApplied, Authority: foreignAuthority, AcceptedTTLms: time.Minute.Milliseconds(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Guards.Install(actorrouting.DomainFarm, 0, foreignGuard); err != nil {
		t.Fatal(err)
	}
	if runtime.CanServeShard(actorrouting.DomainFarm, 0) {
		t.Fatal("foreign ACTIVE guard reported as locally servable")
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if runtime.ServingShardCount() != 0 {
		t.Fatalf("ServingShardCount=%d after Close, want 0", runtime.ServingShardCount())
	}
}

// TestRuntimeDynamicAddFailureKeepsProcessReadyAndTargetShardClosed verifies
// single-shard failure isolation for a runtime ADD: assigning a new local
// shard that cannot be installed fails that shard closed while existing
// shards keep serving and ProcessReady stays true.
func TestRuntimeDynamicAddFailureKeepsProcessReadyAndTargetShardClosed(t *testing.T) {
	fixture := newCompositionFixture(t)
	ctx := context.Background()
	// Startup: shard 0 local (claim), shard 1 held by player-pod-2.
	startup := fixture.snap
	startup.GlobalRevision = 1
	startup.Player.ShardCount = 2
	startup.Holders = append([]actorrouting.HolderRef(nil), fixture.snap.Holders...)
	startup.Holders = append(startup.Holders, actorrouting.HolderRef{InstanceID: "player-pod-2", BootID: "player-boot-2", Endpoint: "player-2.local:9100", State: actorrouting.HolderStateActive, Weight: 1})
	startup.Player.Routes = append([]actorrouting.ShardRoute(nil), fixture.snap.Player.Routes...)
	startup.Player.Routes = append(startup.Player.Routes, actorrouting.ShardRoute{
		HolderIndex: 2, State: actorrouting.ShardStateActive,
		ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
		LeaseUntilMs: time.Now().Add(30 * time.Second).UnixMilli(),
	})
	opts := membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1")
	opts.RouteSnapshotPath = writeSnapshotPath(t, startup)
	runtime := newMembershipRuntimeFromOptions(t, opts)
	if runtime.ServingShardCount() != 1 {
		t.Fatalf("startup serving=%d, want 1", runtime.ServingShardCount())
	}

	// rev2 assigns shard 1 to this holder with a generation the repository has
	// never seen → the ADD fails, that shard stays fail-closed, and the
	// runtime keeps serving shard 0 with ProcessReady up.
	rev2 := runtime.AssignmentProvider.Snapshot()
	rev2.GlobalRevision = 2
	rev2.Player.Routes[1] = actorrouting.ShardRoute{
		HolderIndex: 0, State: actorrouting.ShardStateActive,
		ShardRevision: 2, ExpectedWriterEpoch: 2, ExpectedLeaseEpoch: 2,
		LeaseUntilMs: time.Now().Add(30 * time.Second).UnixMilli(),
	}
	if err := runtime.publisher.Publish(ctx, controlfence.NestedControlOperationToken{}, rev2); err != nil {
		t.Fatal(err)
	}
	result := runtime.observeMembership(rev2)
	if result.Closed || result.Rejected {
		t.Fatalf("rev2 must be accepted by the reconciler: %+v", result)
	}
	if snap, ok := runtime.Guards.Snapshot(actorrouting.DomainPlayer, 1); ok && snap.Admitted {
		t.Fatal("the un-installable shard 1 must stay fail-closed")
	}
	if snap, ok := runtime.Guards.Snapshot(actorrouting.DomainPlayer, 0); !ok || !snap.Admitted {
		t.Fatal("existing shard 0 must keep serving")
	}
	if !runtime.ProcessReady() {
		t.Fatal("a dynamic ADD failure must not lower ProcessReady (single-shard isolation)")
	}
	if runtime.scheduler.EntryCount() != 1 {
		t.Fatalf("entry count=%d, want 1 (only shard 0)", runtime.scheduler.EntryCount())
	}
}

// TestRuntimeFatalImmediatelyFencesAllOwnedGuards verifies that enterFatal
// fails the LOCAL DATA PLANE closed immediately (not just readiness): owned
// guards stop admitting, owned scheduler entries are cancelled, serving count
// drops to zero, and the fatal cause is recorded.
func TestRuntimeFatalImmediatelyFencesAllOwnedGuards(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	if runtime.ServingShardCount() != 1 {
		t.Fatalf("pre-fatal serving=%d, want 1", runtime.ServingShardCount())
	}
	runtime.enterFatal(errors.New("injected runtime fatal"))
	if runtime.ProcessReady() {
		t.Fatal("fatal must clear ProcessReady")
	}
	if runtime.ServingShardCount() != 0 {
		t.Fatalf("fatal must drop serving to 0, got %d", runtime.ServingShardCount())
	}
	if runtime.scheduler.EntryCount() != 0 {
		t.Fatalf("fatal must cancel owned entries, got %d", runtime.scheduler.EntryCount())
	}
	for _, snap := range runtime.Guards.SnapshotAll() {
		if snap.Admitted {
			t.Fatalf("fatal left an admitted guard: %+v", snap.Authority)
		}
	}
	if runtime.FatalErr() == nil {
		t.Fatal("fatal cause must be recorded")
	}
}

// TestRuntimeCloseIsNotFatal verifies that a graceful Close moves the lifecycle
// to Closed (NOT Fatal): no FatalErr is recorded, no scheduler fatal, no
// fatal-metric pollution -- a graceful shutdown is not a runtime fault.
func TestRuntimeCloseIsNotFatal(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if runtime.membershipState.Load() != membershipStateClosed {
		t.Fatalf("Close must move the lifecycle to Closed, got state=%d", runtime.membershipState.Load())
	}
	if runtime.scheduler.Fatal() {
		t.Fatal("a graceful Close must not mark the scheduler fatal")
	}
	if runtime.FatalErr() != nil {
		t.Fatalf("a graceful Close must not record a fatal error: %v", runtime.FatalErr())
	}
}

func TestRuntimeConcurrentCloseSharesSchedulerBarrier(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	if runtime.scheduler.EntryCount() != 1 {
		t.Fatalf("startup entries=%d want 1", runtime.scheduler.EntryCount())
	}
	closeEntered := make(chan struct{}, 2)
	entryExiting := make(chan struct{})
	releaseEntry := make(chan struct{})
	var exitOnce sync.Once
	runtime.scheduler.mu.Lock()
	runtime.scheduler.closeEnterHook = func() { closeEntered <- struct{}{} }
	runtime.scheduler.entryExitHook = func() {
		exitOnce.Do(func() { close(entryExiting) })
		<-releaseEntry
	}
	runtime.scheduler.mu.Unlock()

	returned := make(chan error, 2)
	for range 2 {
		go func() { returned <- runtime.Close() }()
	}
	for range 2 {
		select {
		case <-closeEntered:
		case <-time.After(time.Second):
			t.Fatal("concurrent Runtime.Close did not reach scheduler barrier")
		}
	}
	select {
	case <-entryExiting:
	case <-time.After(time.Second):
		t.Fatal("runtime scheduler entry did not reach exit barrier")
	}
	select {
	case err := <-returned:
		t.Fatalf("Runtime.Close returned before scheduler join: %v", err)
	default:
	}
	close(releaseEntry)
	for range 2 {
		select {
		case err := <-returned:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent Runtime.Close did not return after scheduler barrier")
		}
	}
	if runtime.scheduler.EntryCount() != 0 || runtime.ServingShardCount() != 0 {
		t.Fatalf("post-Close entries=%d serving=%d", runtime.scheduler.EntryCount(), runtime.ServingShardCount())
	}
	select {
	case <-runtime.scheduler.closeDone:
	default:
		t.Fatal("Runtime.Close returned before scheduler closeDone")
	}
}

// TestCloseWinsConcurrentFatalWithoutRecordingFatal verifies the terminal-race
// rule: once Closed wins, a late enterFatal must not overwrite Closed, must not
// record a FatalErr and must not turn a graceful shutdown into a fault.
func TestCloseWinsConcurrentFatalWithoutRecordingFatal(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	runtime.enterFatal(errors.New("late fatal"))
	if runtime.membershipState.Load() != membershipStateClosed {
		t.Fatalf("state=%d, want Closed (a late fatal must not overwrite a graceful close)", runtime.membershipState.Load())
	}
	if runtime.FatalErr() != nil {
		t.Fatalf("a late fatal after Close recorded FatalErr: %v", runtime.FatalErr())
	}
}

// TestFatalWinsConcurrentCloseAndPreservesCause verifies the other direction:
// once Fatal wins, Close must not overwrite Fatal or the primary cause.
func TestFatalWinsConcurrentCloseAndPreservesCause(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	runtime.enterFatal(errors.New("primary fatal cause"))
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if runtime.membershipState.Load() != membershipStateFatal {
		t.Fatalf("state=%d, want Fatal (Close must not overwrite a fatal)", runtime.membershipState.Load())
	}
	if runtime.FatalErr() == nil || !strings.Contains(runtime.FatalErr().Error(), "primary fatal cause") {
		t.Fatalf("primary cause lost after Close: %v", runtime.FatalErr())
	}
	if runtime.ProcessReady() {
		t.Fatal("a fatal runtime must not be ready")
	}
}

// TestCloseAndFatalConcurrentNoDeadlock hammers Close and enterFatal from two
// goroutines and requires termination plus a fail-closed data plane.
func TestCloseAndFatalConcurrentNoDeadlock(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = runtime.Close() }()
	go func() { defer wg.Done(); runtime.enterFatal(errors.New("concurrent fatal")) }()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close/Fatal concurrency deadlocked")
	}
	if runtime.ProcessReady() {
		t.Fatal("a terminal runtime must not be ready")
	}
	if runtime.ServingShardCount() != 0 {
		t.Fatalf("serving=%d after the terminal race, want 0", runtime.ServingShardCount())
	}
}

// TestRuntimeFatalRejectsLateCommitPrepared verifies the Runtime Fatal Gate:
// once the runtime is terminal, every local membership side effect through the
// gated adapter is refused, so an in-flight or retried reconcile action can
// never re-install a guard.
func TestRuntimeFatalRejectsLateCommitPrepared(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	gate := &runtimeGateMembershipAdapter{inner: runtime.adapter, runtime: runtime}
	// Active runtime passes the gate (inner may fail on an empty prepared, but
	// not with ErrRuntimeTerminal).
	if err := gate.CommitPreparedMembership(context.Background(), PreparedMembership{}); errors.Is(err, ErrRuntimeTerminal) {
		t.Fatal("an active runtime must not refuse membership side effects")
	}
	runtime.enterFatal(errors.New("fatal before late commit"))
	if err := gate.CommitPreparedMembership(context.Background(), PreparedMembership{}); !errors.Is(err, ErrRuntimeTerminal) {
		t.Fatalf("a terminal runtime must refuse late commits, got %v", err)
	}
	if _, err := gate.PrepareMembership(context.Background(), ShardMembership{}); !errors.Is(err, ErrRuntimeTerminal) {
		t.Fatalf("a terminal runtime must refuse prepares, got %v", err)
	}
	if err := gate.FenceMembership(context.Background(), ShardMembership{}); !errors.Is(err, ErrRuntimeTerminal) {
		t.Fatalf("a terminal runtime must refuse fences, got %v", err)
	}
	if err := gate.RemoveMembership(context.Background(), ShardMembership{}); !errors.Is(err, ErrRuntimeTerminal) {
		t.Fatalf("a terminal runtime must refuse removes, got %v", err)
	}
}

// TestRuntimeFatalCancelsDegradedAddRetry verifies that enterFatal closes the
// reconciler: a late snapshot cannot start a degraded retry that installs
// anything, and the data plane stays at zero.
func TestRuntimeFatalCancelsDegradedAddRetry(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	runtime.enterFatal(errors.New("fatal"))
	result := runtime.observeMembership(runtime.AssignmentProvider.Snapshot())
	if !result.Closed {
		t.Fatalf("a late snapshot after fatal must be rejected as Closed, got %+v", result)
	}
	if runtime.ServingShardCount() != 0 {
		t.Fatalf("serving=%d after fatal, want 0", runtime.ServingShardCount())
	}
	if runtime.scheduler.EntryCount() != 0 {
		t.Fatalf("entries=%d after fatal, want 0", runtime.scheduler.EntryCount())
	}
}

// TestMembershipObserverMaySkipIntermediateFullSnapshotsAndConverge freezes the
// snapshot contract: the observer polls the latest full snapshot and MAY skip
// intermediate revisions (R2), converging directly from R1 to R3 without
// leaving any R2 residue.
func TestMembershipObserverMaySkipIntermediateFullSnapshotsAndConverge(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	ctx := context.Background()

	// R2: transfer away (pod-2, generation 2).
	t2, err := runtime.Store.TransferShardAuthority(ctx, authoritystore.TransferShardAuthorityRequest{
		Caller:       authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-4", BootID: "route-manager-boot-4"},
		TransitionID: "transfer-rt3c-skip-away", AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: runtime.GuardsSnapshotForTest(actorrouting.DomainPlayer, 0),
		NewHolderID:   "player-pod-2", NewHolderBootID: "player-boot-2", RequestedTTLms: 5000,
	})
	if err != nil || t2.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("transfer-away=%+v err=%v", t2, err)
	}
	rev2 := runtime.AssignmentProvider.Snapshot()
	rev2.GlobalRevision = 2
	rev2.Holders = append(rev2.Holders, actorrouting.HolderRef{InstanceID: "player-pod-2", BootID: "player-boot-2", Endpoint: "player-2.local:9100", State: actorrouting.HolderStateActive, Weight: 1})
	rev2.Player.Routes[0] = actorrouting.ShardRoute{
		HolderIndex: 2, State: actorrouting.ShardStateActive,
		ShardRevision: t2.Authority.AssignmentRevision, ExpectedWriterEpoch: t2.Authority.WriterEpoch,
		ExpectedLeaseEpoch: t2.Authority.LeaseEpoch, LeaseUntilMs: t2.Authority.LeaseUntilMs,
	}

	// R3: transfer back (pod-1, generation 3).
	t3, err := runtime.Store.TransferShardAuthority(ctx, authoritystore.TransferShardAuthorityRequest{
		Caller:       authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-4", BootID: "route-manager-boot-4"},
		TransitionID: "transfer-rt3c-skip-back", AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: authoritystore.ShardWriteFence{
			ExpectedHolderID: t2.Authority.HolderID, ExpectedHolderBootID: t2.Authority.HolderBootID,
			ExpectedAssignmentRevision: t2.Authority.AssignmentRevision,
			ExpectedWriterEpoch:        t2.Authority.WriterEpoch, ExpectedLeaseEpoch: t2.Authority.LeaseEpoch,
		},
		NewHolderID: "player-pod-1", NewHolderBootID: "player-boot-1", RequestedTTLms: 5000,
	})
	if err != nil || t3.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("transfer-back=%+v err=%v", t3, err)
	}
	rev3 := runtime.AssignmentProvider.Snapshot()
	rev3.GlobalRevision = 3
	rev3.Player.Routes[0] = actorrouting.ShardRoute{
		HolderIndex: 0, State: actorrouting.ShardStateActive,
		ShardRevision: t3.Authority.AssignmentRevision, ExpectedWriterEpoch: t3.Authority.WriterEpoch,
		ExpectedLeaseEpoch: t3.Authority.LeaseEpoch, LeaseUntilMs: t3.Authority.LeaseUntilMs,
	}

	// Publish R2 then immediately R3; the observer polls the latest value and
	// may skip R2 entirely, converging directly to R3.
	if err := runtime.publisher.Publish(ctx, controlfence.NestedControlOperationToken{}, rev2); err != nil {
		t.Fatal(err)
	}
	if err := runtime.publisher.Publish(ctx, controlfence.NestedControlOperationToken{}, rev3); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool {
		snap, ok := runtime.Guards.Snapshot(actorrouting.DomainPlayer, 0)
		return ok && snap.Admitted && snap.Authority.WriterEpoch == t3.Authority.WriterEpoch && runtime.scheduler.EntryCount() == 1
	})
	if !runtime.ProcessReady() {
		t.Fatal("converged runtime must stay ready")
	}
	all := runtime.Guards.SnapshotAll()
	if len(all) != 1 {
		t.Fatalf("skipped intermediate revisions left residue guards: %d", len(all))
	}
	if all[0].Authority.WriterEpoch != t3.Authority.WriterEpoch {
		t.Fatalf("observer did not converge to the latest generation, got epoch=%d want=%d", all[0].Authority.WriterEpoch, t3.Authority.WriterEpoch)
	}
}

// TestMembershipRuntimeObserverDrivesRemoveAndReadd drives a full migration
// round-trip through the composed runtime with a legal control-plane sequence
// (generation strictly progressing on every transfer): G1 installed →
// REMOVE (migrate away) → RE-ADD (migrate back, new generation).  This
// verifies hard invariant 8 (the runtime follows assignment flows pushed by
// the provider) and invariant 2 (exactly one renew owner at every point).
func TestMembershipRuntimeObserverDrivesRemoveAndReadd(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	ctx := context.Background()
	if runtime.scheduler.EntryCount() != 1 {
		t.Fatalf("startup must register one renew owner, got %d", runtime.scheduler.EntryCount())
	}

	// Migrate away: transfer to player-pod-2 (G2) and publish rev2.
	transfer2, err := runtime.Store.TransferShardAuthority(ctx, authoritystore.TransferShardAuthorityRequest{
		Caller:       authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-3", BootID: "route-manager-boot-3"},
		TransitionID: "transfer-rt3c-away", AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: runtime.GuardsSnapshotForTest(actorrouting.DomainPlayer, 0),
		NewHolderID:   "player-pod-2", NewHolderBootID: "player-boot-2", RequestedTTLms: 5000,
	})
	if err != nil || transfer2.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("transfer-away=%+v err=%v", transfer2, err)
	}
	rev2 := runtime.AssignmentProvider.Snapshot()
	rev2.GlobalRevision = 2
	rev2.Holders = append(rev2.Holders, actorrouting.HolderRef{InstanceID: "player-pod-2", BootID: "player-boot-2", Endpoint: "player-2.local:9100", State: actorrouting.HolderStateActive, Weight: 1})
	rev2.Player.Routes[0] = actorrouting.ShardRoute{
		HolderIndex: 2, State: actorrouting.ShardStateActive,
		ShardRevision: transfer2.Authority.AssignmentRevision, ExpectedWriterEpoch: transfer2.Authority.WriterEpoch,
		ExpectedLeaseEpoch: transfer2.Authority.LeaseEpoch, LeaseUntilMs: transfer2.Authority.LeaseUntilMs,
	}
	if err := runtime.publisher.Publish(ctx, controlfence.NestedControlOperationToken{}, rev2); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool {
		snapshot, ok := runtime.Guards.Snapshot(actorrouting.DomainPlayer, 0)
		return (!ok || !snapshot.Admitted) && runtime.scheduler.EntryCount() == 0
	})
	if runtime.ServingShardCount() != 0 {
		t.Fatalf("post-migrate serving=%d, want 0", runtime.ServingShardCount())
	}

	// Migrate back: transfer to player-pod-1 (G3) and publish rev3 → RE-ADD.
	transfer3, err := runtime.Store.TransferShardAuthority(ctx, authoritystore.TransferShardAuthorityRequest{
		Caller:       authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-3", BootID: "route-manager-boot-3"},
		TransitionID: "transfer-rt3c-back", AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: authoritystore.ShardWriteFence{
			ExpectedHolderID: transfer2.Authority.HolderID, ExpectedHolderBootID: transfer2.Authority.HolderBootID,
			ExpectedAssignmentRevision: transfer2.Authority.AssignmentRevision,
			ExpectedWriterEpoch:        transfer2.Authority.WriterEpoch, ExpectedLeaseEpoch: transfer2.Authority.LeaseEpoch,
		},
		NewHolderID: "player-pod-1", NewHolderBootID: "player-boot-1", RequestedTTLms: 5000,
	})
	if err != nil || transfer3.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("transfer-back=%+v err=%v", transfer3, err)
	}
	rev3 := runtime.AssignmentProvider.Snapshot()
	rev3.GlobalRevision = 3
	rev3.Player.Routes[0] = actorrouting.ShardRoute{
		HolderIndex: 0, State: actorrouting.ShardStateActive,
		ShardRevision: transfer3.Authority.AssignmentRevision, ExpectedWriterEpoch: transfer3.Authority.WriterEpoch,
		ExpectedLeaseEpoch: transfer3.Authority.LeaseEpoch, LeaseUntilMs: transfer3.Authority.LeaseUntilMs,
	}
	if err := runtime.publisher.Publish(ctx, controlfence.NestedControlOperationToken{}, rev3); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool {
		snapshot, ok := runtime.Guards.Snapshot(actorrouting.DomainPlayer, 0)
		return ok && snapshot.Admitted && snapshot.Authority.WriterEpoch == transfer3.Authority.WriterEpoch && runtime.scheduler.EntryCount() == 1
	})
	if runtime.scheduler.EntryCount() != 1 {
		t.Fatalf("re-add must register exactly one renew owner, got %d", runtime.scheduler.EntryCount())
	}
	if !runtime.ProcessReady() {
		t.Fatal("a migration round-trip must keep the process ready")
	}
	assertMembershipRuntimeConsistent(t, runtime.MembershipReconciler(), runtime.Guards, runtime.scheduler)
}

// TestFatalAfterCommitGateCheckCannotLeaveGuardOrEntry proves the fatal
// cleanup linearizes AFTER an in-flight commit that already passed the runtime
// gate: the commit parks on the adapter hook just before the local guard
// install, enterFatal runs while it is parked, then the released commit
// installs its guard -- and the runtime's fail-closed sweep (reconciler.Close
// action-barrier followed by failClosedLocalState) must still remove every
// trace.  After the race the runtime is Fatal with zero serving shards, zero
// active lease entries and zero owned admitted guards.
func TestFatalAfterCommitGateCheckCannotLeaveGuardOrEntry(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	if runtime.scheduler.EntryCount() != 1 {
		t.Fatalf("startup must register one renew owner, got %d", runtime.scheduler.EntryCount())
	}
	ctx := context.Background()

	// Park the in-flight commit at the adapter just before InstallIfMatches:
	// by the time the hook runs, the runtime gate has already passed.
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	runtime.adapter.commitPreparedHook = func() {
		once.Do(func() { close(entered) })
		<-release
	}

	// Build a strictly newer generation for shard 0 (transfer away then back,
	// generation advancing on every transfer) so observing rev3 replans a
	// REPLACE whose commit parks on the hook.
	old := runtime.GuardsSnapshotForTest(actorrouting.DomainPlayer, 0)
	transfer2, err := runtime.Store.TransferShardAuthority(ctx, authoritystore.TransferShardAuthorityRequest{
		Caller:       authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-5", BootID: "route-manager-boot-5"},
		TransitionID: "fatal-inflight-away", AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: old, NewHolderID: "player-pod-2", NewHolderBootID: "player-boot-2", RequestedTTLms: 5000,
	})
	if err != nil || transfer2.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("transfer-away=%+v err=%v", transfer2, err)
	}
	transfer3, err := runtime.Store.TransferShardAuthority(ctx, authoritystore.TransferShardAuthorityRequest{
		Caller:       authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "route-manager-5", BootID: "route-manager-boot-5"},
		TransitionID: "fatal-inflight-back", AuthorityLayoutVersion: "v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: authoritystore.ShardWriteFence{
			ExpectedHolderID: transfer2.Authority.HolderID, ExpectedHolderBootID: transfer2.Authority.HolderBootID,
			ExpectedAssignmentRevision: transfer2.Authority.AssignmentRevision,
			ExpectedWriterEpoch:        transfer2.Authority.WriterEpoch, ExpectedLeaseEpoch: transfer2.Authority.LeaseEpoch,
		},
		NewHolderID: "player-pod-1", NewHolderBootID: "player-boot-1", RequestedTTLms: 5000,
	})
	if err != nil || transfer3.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("transfer-back=%+v err=%v", transfer3, err)
	}
	rev3 := runtime.AssignmentProvider.Snapshot()
	rev3.GlobalRevision = 3
	rev3.Player.Routes[0] = actorrouting.ShardRoute{
		HolderIndex: 0, State: actorrouting.ShardStateActive,
		ShardRevision: transfer3.Authority.AssignmentRevision, ExpectedWriterEpoch: transfer3.Authority.WriterEpoch,
		ExpectedLeaseEpoch: transfer3.Authority.LeaseEpoch, LeaseUntilMs: transfer3.Authority.LeaseUntilMs,
	}

	// Observe rev3 in a goroutine: the REPLACE's commit passes the runtime gate
	// (state still Ready) and parks on the hook before installing the guard.
	observeDone := make(chan struct{})
	go func() {
		defer close(observeDone)
		runtime.observeMembership(rev3)
	}()
	<-entered

	// Trigger fatal in a goroutine: it CASes to Fatal and then parks inside
	// reconciler.Close's action barrier waiting for the in-flight commit.
	fatalDone := make(chan struct{})
	go func() {
		defer close(fatalDone)
		runtime.enterFatal(errors.New("fatal after gate-checked commit"))
	}()
	// Deterministically wait for the CAS (not a sleep): once Fatal is visible,
	// enterFatal has begun its cleanup and the parked commit still holds the
	// action worker, so releasing the commit exercises the real race window.
	for runtime.membershipState.Load() != membershipStateFatal {
		time.Sleep(time.Millisecond)
	}
	close(release)
	<-observeDone
	<-fatalDone

	if runtime.membershipState.Load() != membershipStateFatal {
		t.Fatalf("state=%d after the race, want Fatal", runtime.membershipState.Load())
	}
	if runtime.ProcessReady() {
		t.Fatal("a fatal runtime must not be ready")
	}
	if runtime.ServingShardCount() != 0 {
		t.Fatalf("serving=%d after the race, want 0", runtime.ServingShardCount())
	}
	if runtime.scheduler.EntryCount() != 0 {
		t.Fatalf("entries=%d after the race, want 0", runtime.scheduler.EntryCount())
	}
	if len(runtime.scheduler.ActiveEntries()) != 0 {
		t.Fatalf("active entries=%d after the race, want 0", len(runtime.scheduler.ActiveEntries()))
	}
	for _, snap := range runtime.Guards.SnapshotAll() {
		if !runtime.ownsGuard(snap) {
			continue
		}
		if snap.Admitted && snap.Authority.State == authoritystore.ShardAuthorityActive {
			t.Fatalf("owned admitted guard left after fatal: %+v", snap.Authority)
		}
	}
}

// TestCloseAfterFatalPreservesCauseAndReapsAllResources proves a Close issued
// AFTER Fatal still reaps every resource (observer/applier goroutines,
// reconciler worker, scheduler entries, guards) without overwriting the Fatal
// state or its primary cause: state stays Fatal, FatalErr keeps the original
// cause, and a second Close is safe.
func TestCloseAfterFatalPreservesCauseAndReapsAllResources(t *testing.T) {
	fixture := newCompositionFixture(t)
	runtime := newMembershipRuntimeFromOptions(t, membershipRuntimeOptions(fixture, "player-pod-1", "player-boot-1"))
	if runtime.ServingShardCount() != 1 || runtime.scheduler.EntryCount() != 1 {
		t.Fatalf("startup serving=%d entries=%d, want 1/1", runtime.ServingShardCount(), runtime.scheduler.EntryCount())
	}

	cause := errors.New("original fatal cause")
	runtime.enterFatal(cause)
	if runtime.membershipState.Load() != membershipStateFatal {
		t.Fatalf("state=%d after enterFatal, want Fatal", runtime.membershipState.Load())
	}
	if !errors.Is(runtime.FatalErr(), cause) {
		t.Fatalf("primary cause=%v, want %v", runtime.FatalErr(), cause)
	}

	if err := runtime.Close(); err != nil {
		t.Fatalf("Close after fatal: %v", err)
	}
	// Fatal stays Fatal: a Close must never overwrite it or its cause.
	if runtime.membershipState.Load() != membershipStateFatal {
		t.Fatalf("Close overwrote Fatal with state=%d", runtime.membershipState.Load())
	}
	if !errors.Is(runtime.FatalErr(), cause) {
		t.Fatalf("Close lost the primary cause: %v", runtime.FatalErr())
	}
	if runtime.ProcessReady() {
		t.Fatal("a fatal runtime must not be ready")
	}
	if runtime.ServingShardCount() != 0 {
		t.Fatalf("serving=%d after Close, want 0", runtime.ServingShardCount())
	}
	if runtime.scheduler.EntryCount() != 0 {
		t.Fatalf("entries=%d after Close, want 0", runtime.scheduler.EntryCount())
	}
	for _, snap := range runtime.Guards.SnapshotAll() {
		if !runtime.ownsGuard(snap) {
			continue
		}
		if snap.Admitted && snap.Authority.State == authoritystore.ShardAuthorityActive {
			t.Fatalf("owned admitted guard left after Close: %+v", snap.Authority)
		}
	}

	// Second Close is idempotent and reaps anything still running (observer /
	// applier / scheduler goroutines): if wg had unfinished goroutines this
	// would block.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runtime.Close()
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("second Close after fatal deadlocked on goroutine reaping")
	}
}
