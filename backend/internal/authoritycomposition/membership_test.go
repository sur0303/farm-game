package authoritycomposition_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/authoritycomposition"
	"farmgame/internal/authoritycomposition/testkit"
)

const (
	membershipLocalHolder = "holder-2"
	membershipLocalBoot   = "boot-2"
)

type membershipRouteSpec struct {
	holderIndex int
	state       actorrouting.ShardRouteState
	revision    uint64
	writer      uint64
	lease       uint64
	leaseUntil  int64
}

func membershipHolders() []actorrouting.HolderRef {
	return []actorrouting.HolderRef{
		{InstanceID: "holder-1", BootID: "boot-1", Endpoint: "h1:9100", State: actorrouting.HolderStateActive, Weight: 1},
		{InstanceID: "holder-2", BootID: "boot-2", Endpoint: "h2:9100", State: actorrouting.HolderStateActive, Weight: 1},
	}
}

func membershipActiveSpec(holderIndex int, rev, writer, lease uint64) membershipRouteSpec {
	return membershipRouteSpec{
		holderIndex: holderIndex, state: actorrouting.ShardStateActive,
		revision: rev, writer: writer, lease: lease,
		leaseUntil: time.Now().Add(time.Hour).UnixMilli(),
	}
}

func membershipSnapshot(rev uint64, player []membershipRouteSpec, farm []membershipRouteSpec) actorrouting.RouteSnapshot {
	toRoutes := func(specs []membershipRouteSpec) []actorrouting.ShardRoute {
		routes := make([]actorrouting.ShardRoute, len(specs))
		for i, spec := range specs {
			routes[i] = actorrouting.ShardRoute{
				HolderIndex: spec.holderIndex, State: spec.state,
				ShardRevision: spec.revision, ExpectedWriterEpoch: spec.writer,
				ExpectedLeaseEpoch: spec.lease, LeaseUntilMs: spec.leaseUntil,
			}
		}
		return routes
	}
	playerRoutes := toRoutes(player)
	farmRoutes := toRoutes(farm)
	return actorrouting.RouteSnapshot{
		Realm: "membership-test", GlobalRevision: rev,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608060001,
		Player:  actorrouting.DomainSnapshot{ShardCount: uint32(len(playerRoutes)), Routes: playerRoutes},
		Farm:    actorrouting.DomainSnapshot{ShardCount: uint32(len(farmRoutes)), Routes: farmRoutes},
		Holders: membershipHolders(),
	}
}

func membershipReconciler(t *testing.T, ports *testkit.FakeMembershipPorts) *authoritycomposition.ShardMembershipReconciler {
	t.Helper()
	reconciler, err := authoritycomposition.NewShardMembershipReconciler(authoritycomposition.MembershipReconcilerOptions{
		HolderID:     membershipLocalHolder,
		HolderBootID: membershipLocalBoot,
		Installer:    ports,
		Fencer:       ports,
		Remover:      ports,
		MaxRetries:   3,
		RetryDelay:   time.Millisecond,
		Now:          time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reconciler.Close)
	return reconciler
}

func TestMembershipZeroOneZero(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipReconciler(t, ports)

	// Revision 1: zero local shards (shard 0 belongs to the other holder).
	rev1 := membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(0, 1, 1, 1)}, nil)
	result := reconciler.Observe(rev1)
	if result.Stale || result.Duplicate || !result.FullyReconciled {
		t.Fatalf("zero-shard observe result=%+v", result)
	}
	if reconciler.LastFullyReconciledRevision() != 1 {
		t.Fatalf("zero-shard fully reconciled revision=%d, want 1", reconciler.LastFullyReconciledRevision())
	}
	if len(reconciler.CurrentInstalledMembership()) != 0 {
		t.Fatalf("zero-shard installed=%d, want 0", len(reconciler.CurrentInstalledMembership()))
	}
	if len(ports.Installs()) != 0 || len(ports.Fences()) != 0 || len(ports.Removes()) != 0 {
		t.Fatalf("zero-shard produced port calls: installs=%d fences=%d removes=%d",
			len(ports.Installs()), len(ports.Fences()), len(ports.Removes()))
	}

	// Revision 2: shard 0 moves to the local holder -> ADD.
	rev2 := membershipSnapshot(2, []membershipRouteSpec{membershipActiveSpec(1, 2, 2, 2)}, nil)
	result2 := reconciler.Observe(rev2)
	if result2.Stale || !result2.FullyReconciled || len(result2.Actions) != 1 || result2.Actions[0].Type != authoritycomposition.MembershipActionAdd {
		t.Fatalf("add observe result=%+v", result2)
	}
	if len(ports.Installs()) != 1 || ports.Installs()[0].Key.Shard != 0 {
		t.Fatalf("installs=%+v, want exactly shard 0", ports.Installs())
	}
	if reconciler.LastFullyReconciledRevision() != 2 {
		t.Fatalf("reconciled revision=%d, want 2", reconciler.LastFullyReconciledRevision())
	}

	// Revision 3: shard 0 leaves the local holder -> REMOVE (fence then remove).
	rev3 := membershipSnapshot(3, []membershipRouteSpec{membershipActiveSpec(0, 3, 3, 3)}, nil)
	result3 := reconciler.Observe(rev3)
	if result3.Stale || !result3.FullyReconciled || len(result3.Actions) != 1 || result3.Actions[0].Type != authoritycomposition.MembershipActionRemove {
		t.Fatalf("remove observe result=%+v", result3)
	}
	if len(ports.Fences()) != 1 || len(ports.Removes()) != 1 {
		t.Fatalf("fences=%d removes=%d, want 1/1", len(ports.Fences()), len(ports.Removes()))
	}
	if len(reconciler.CurrentInstalledMembership()) != 0 {
		t.Fatalf("membership not emptied after remove")
	}
	if reconciler.LastFullyReconciledRevision() != 3 {
		t.Fatalf("reconciled revision=%d, want 3", reconciler.LastFullyReconciledRevision())
	}
}

func TestMembershipDuplicateSnapshotIsIdempotent(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipReconciler(t, ports)

	rev := membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil)
	first := reconciler.Observe(rev)
	if !first.FullyReconciled {
		t.Fatalf("first observe not reconciled: %+v", first)
	}
	installCount := len(ports.Installs())

	second := reconciler.Observe(rev)
	if !second.Duplicate || !second.FullyReconciled || len(second.Actions) != 0 {
		t.Fatalf("duplicate observe result=%+v", second)
	}
	if len(ports.Installs()) != installCount {
		t.Fatalf("duplicate snapshot re-installed shards: %d -> %d", installCount, len(ports.Installs()))
	}
}

func TestMembershipStaleSnapshotIsRejected(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipReconciler(t, ports)

	reconciler.Observe(membershipSnapshot(10, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil))
	before := len(ports.Installs())

	stale := reconciler.Observe(membershipSnapshot(9, nil, nil))
	if !stale.Stale || stale.Duplicate {
		t.Fatalf("stale observe result=%+v", stale)
	}
	if len(ports.Installs()) != before {
		t.Fatalf("stale snapshot mutated installs: %d -> %d", before, len(ports.Installs()))
	}
	if len(reconciler.CurrentInstalledMembership()) != 1 {
		t.Fatalf("stale snapshot dropped installed membership")
	}
}

func TestMembershipGenerationReplacementFencesBeforeInstall(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipReconciler(t, ports)

	reconciler.Observe(membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil))
	ports.Reset() // clear the initial install record

	result := reconciler.Observe(membershipSnapshot(2, []membershipRouteSpec{membershipActiveSpec(1, 2, 2, 2)}, nil))
	if len(result.Actions) != 1 || result.Actions[0].Type != authoritycomposition.MembershipActionReplace {
		t.Fatalf("expected REPLACE, got %+v", result.Actions)
	}
	if !result.FullyReconciled || reconciler.LastFullyReconciledRevision() != 2 {
		t.Fatalf("replacement did not reconcile: %+v revision=%d", result, reconciler.LastFullyReconciledRevision())
	}
	order := ports.Order()
	if len(order) != 3 || order[0] != "fence" || order[1] != "prepare" || order[2] != "install" {
		t.Fatalf("replacement must fence before prepare/install, order=%v", order)
	}
	fences := ports.Fences()
	if len(fences) != 1 || fences[0].Generation.AssignmentRevision != 1 || fences[0].Generation.WriterEpoch != 1 {
		t.Fatalf("fence must target the old generation, got %+v", fences)
	}
	installs := ports.Installs()
	if len(installs) != 1 || installs[0].Generation.AssignmentRevision != 2 || installs[0].Generation.WriterEpoch != 2 {
		t.Fatalf("install must carry the new generation, got %+v", installs)
	}
}

func TestMembershipRemoveBeforeAddOrdering(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipReconciler(t, ports)

	// Revision 1: player shard 0 owned locally; farm shard 0 belongs to the
	// other holder.  Both domains keep a stable ShardCount=1 across revisions
	// so the topology fingerprint is constant.
	reconciler.Observe(membershipSnapshot(1,
		[]membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)},
		[]membershipRouteSpec{membershipActiveSpec(0, 1, 1, 1)}))
	ports.Reset()

	// Revision 2: player shard 0 leaves; farm shard 0 arrives on the same
	// snapshot.  Fence/remove must execute before install.
	result := reconciler.Observe(membershipSnapshot(2,
		[]membershipRouteSpec{membershipActiveSpec(0, 2, 2, 2)},
		[]membershipRouteSpec{membershipActiveSpec(1, 2, 2, 2)}))
	if !result.FullyReconciled || len(result.Actions) != 2 {
		t.Fatalf("remove+add observe result=%+v", result)
	}
	order := ports.Order()
	if len(order) != 4 || order[0] != "fence" || order[1] != "remove" || order[2] != "prepare" || order[3] != "install" {
		t.Fatalf("fence/remove must precede prepare/install, order=%v", order)
	}
}

func TestMembershipPartialFailureAndRetry(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipReconciler(t, ports)

	// farm shard 0 install fails; player shard 0 succeeds.  The failure is
	// scoped to the farm shard and the player membership stays installed.
	ports.SetInstallErrFunc(func(m authoritycomposition.ShardMembership) error {
		if m.Key.Domain == actorrouting.DomainFarm {
			return errors.New("injected farm install failure")
		}
		return nil
	})
	rev := membershipSnapshot(1,
		[]membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)},
		[]membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)})
	result := reconciler.Observe(rev)
	if result.FullyReconciled {
		t.Fatalf("failed install reported fully reconciled: %+v", result)
	}
	if len(result.Degraded) == 0 {
		t.Fatalf("no degraded status after failure")
	}
	if reconciler.LastFullyReconciledRevision() != 0 {
		t.Fatalf("failed snapshot advanced reconciled revision to %d, want 0", reconciler.LastFullyReconciledRevision())
	}
	members := reconciler.CurrentInstalledMembership()
	if len(members) != 1 || members[0].Key.Domain != actorrouting.DomainPlayer {
		t.Fatalf("player shard must remain installed despite farm failure: %+v", members)
	}
	if len(ports.Installs()) != 2 {
		t.Fatalf("installs=%d, want 2 attempts (player ok + farm failed)", len(ports.Installs()))
	}

	// Clear the failure; explicit Reconcile retries the pending farm action.
	ports.SetInstallErrFunc(nil)
	retry := reconciler.Reconcile()
	if !retry.FullyReconciled {
		t.Fatalf("retry not fully reconciled: %+v", retry)
	}
	if reconciler.LastFullyReconciledRevision() != 1 {
		t.Fatalf("reconciled revision=%d, want 1", reconciler.LastFullyReconciledRevision())
	}
	if len(reconciler.CurrentInstalledMembership()) != 2 {
		t.Fatalf("installed=%d, want 2 after retry", len(reconciler.CurrentInstalledMembership()))
	}
}

func TestMembershipFailureIsScopedToTargetShard(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipReconciler(t, ports)

	// farm shard 0 install fails persistently; player shard 0 must still
	// install and the reconciler must keep the player membership.  A second
	// Reconcile pass does not re-attempt the player shard.
	ports.SetInstallErrFunc(func(m authoritycomposition.ShardMembership) error {
		if m.Key.Domain == actorrouting.DomainFarm {
			return errors.New("farm-only failure")
		}
		return nil
	})
	_ = reconciler.Observe(membershipSnapshot(1,
		[]membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)},
		[]membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}))
	members := reconciler.CurrentInstalledMembership()
	if len(members) != 1 || members[0].Key.Domain != actorrouting.DomainPlayer {
		t.Fatalf("player shard must remain installed despite farm failure: %+v", members)
	}
	playerInstalls := countDomainInstalls(ports.Installs(), actorrouting.DomainPlayer)
	_ = reconciler.Reconcile()
	if countDomainInstalls(ports.Installs(), actorrouting.DomainPlayer) != playerInstalls {
		t.Fatalf("reconcile re-installed the healthy player shard")
	}
	degraded := reconciler.Reconcile().Degraded
	if len(degraded) == 0 || degraded[0].Shard.Domain != actorrouting.DomainFarm {
		t.Fatalf("failure must be scoped to the farm shard, got %+v", degraded)
	}
}

func countDomainInstalls(members []authoritycomposition.ShardMembership, domain actorrouting.RouteDomain) int {
	count := 0
	for _, member := range members {
		if member.Key.Domain == domain {
			count++
		}
	}
	return count
}

func TestMembershipBoundedRetrySettlesDegraded(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler, err := authoritycomposition.NewShardMembershipReconciler(authoritycomposition.MembershipReconcilerOptions{
		HolderID: membershipLocalHolder, HolderBootID: membershipLocalBoot,
		Installer: ports, Fencer: ports, Remover: ports,
		MaxRetries: 2, RetryDelay: time.Millisecond, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reconciler.Close()

	ports.SetInstallErr(errors.New("persistent failure"))
	for range 4 {
		reconciler.Observe(membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil))
	}
	// After the bound, the shard settles degraded and is no longer retried.
	if len(reconciler.CurrentInstalledMembership()) != 0 {
		t.Fatalf("failing shard must not be installed")
	}
	if reconciler.LastFullyReconciledRevision() != 0 {
		t.Fatalf("failing snapshot must not advance fully-reconciled revision")
	}
}

func TestMembershipCloseDuringReconcile(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler, err := authoritycomposition.NewShardMembershipReconciler(authoritycomposition.MembershipReconcilerOptions{
		HolderID: membershipLocalHolder, HolderBootID: membershipLocalBoot,
		Installer: ports, Fencer: ports, Remover: ports,
		MaxRetries: 3, RetryDelay: time.Millisecond, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}

	block := make(chan struct{})
	ports.SetPrepareBlock(block)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = reconciler.Observe(membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil))
	}()

	// Give the goroutine a chance to enter the blocked install.
	deadline := time.Now().Add(2 * time.Second)
	for len(ports.Prepares()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	reconciler.Close() // cancels ctx; blocked install returns ctx.Err()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reconcile did not return after Close")
	}
	// Close is idempotent and post-close operations fail closed.
	reconciler.Close()
	if result := reconciler.Observe(membershipSnapshot(2, nil, nil)); !result.Closed {
		t.Fatalf("post-close Observe must report closed, got %+v", result)
	}
	close(block)
}

func TestMembershipObserveContextBoundsStartupAction(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler, err := authoritycomposition.NewShardMembershipReconciler(authoritycomposition.MembershipReconcilerOptions{
		HolderID: membershipLocalHolder, HolderBootID: membershipLocalBoot,
		Installer: ports, Fencer: ports, Remover: ports,
		MaxRetries: 1, RetryDelay: time.Hour, Now: time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reconciler.Close()

	block := make(chan struct{})
	ports.SetPrepareBlock(block)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	result := reconciler.ObserveContext(ctx, membershipSnapshot(1,
		[]membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil))
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("startup ObserveContext exceeded its deadline by too much: %s", elapsed)
	}
	if result.FullyReconciled || len(result.Degraded) != 1 {
		t.Fatalf("deadline-bounded startup result=%+v", result)
	}
	if len(ports.Prepares()) != 1 || len(ports.Installs()) != 0 {
		t.Fatalf("deadline-bounded action prepares=%d installs=%d", len(ports.Prepares()), len(ports.Installs()))
	}
}

func TestMembershipNoActorBulkLoad(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipReconciler(t, ports)

	// A 4-shard desired set installs exactly 4 shard memberships; the core has
	// no actor enumeration API and never loads actor records.
	rev := membershipSnapshot(1, []membershipRouteSpec{
		membershipActiveSpec(1, 1, 1, 1),
		membershipActiveSpec(1, 1, 1, 1),
		membershipActiveSpec(1, 1, 1, 1),
		membershipActiveSpec(1, 1, 1, 1),
	}, nil)
	result := reconciler.Observe(rev)
	if !result.FullyReconciled {
		t.Fatalf("4-shard observe not reconciled: %+v", result)
	}
	if len(ports.Installs()) != 4 {
		t.Fatalf("installs=%d, want exactly 4 (one per desired shard)", len(ports.Installs()))
	}
	if len(reconciler.CurrentInstalledMembership()) != 4 {
		t.Fatalf("installed=%d, want 4", len(reconciler.CurrentInstalledMembership()))
	}
}

func TestMembershipConcurrentObserveReconcileClose(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipReconciler(t, ports)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				rev := uint64(time.Now().UnixNano() % 100)
				reconciler.Observe(membershipSnapshot(rev,
					[]membershipRouteSpec{membershipActiveSpec(1, rev%5+1, rev%5+1, rev%5+1)},
					nil))
				reconciler.Reconcile()
				_ = reconciler.LastObservedRevision()
				_ = reconciler.LastFullyReconciledRevision()
				_ = reconciler.CurrentInstalledMembership()
			}
		})
	}
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// topologyChangedSnapshot mutates one fingerprint component after sealing.
func topologyChangedSnapshot(rev uint64, mutate func(*actorrouting.RouteSnapshot)) actorrouting.RouteSnapshot {
	snapshot := membershipSnapshot(rev, []membershipRouteSpec{membershipActiveSpec(1, rev, rev, rev)}, nil)
	mutate(&snapshot)
	return snapshot
}

func TestMembershipRejectsHashVersionChange(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipReconciler(t, ports)
	if result := reconciler.Observe(membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil)); !result.FullyReconciled {
		t.Fatalf("baseline not reconciled: %+v", result)
	}
	result := reconciler.Observe(topologyChangedSnapshot(2, func(s *actorrouting.RouteSnapshot) {
		s.HashVersion = 999
	}))
	if !result.Rejected {
		t.Fatalf("hash-version change must fail closed, got %+v", result)
	}
	if len(reconciler.CurrentInstalledMembership()) != 1 {
		t.Fatalf("rejected snapshot must not mutate installed membership")
	}
}

func TestMembershipRejectsHashSeedChange(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipReconciler(t, ports)
	reconciler.Observe(membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil))
	result := reconciler.Observe(topologyChangedSnapshot(2, func(s *actorrouting.RouteSnapshot) {
		s.HashSeed = 424242
	}))
	if !result.Rejected {
		t.Fatalf("hash-seed change must fail closed, got %+v", result)
	}
	if len(reconciler.CurrentInstalledMembership()) != 1 {
		t.Fatalf("rejected snapshot must not mutate installed membership")
	}
}

func TestMembershipRejectsPlayerShardCountChange(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipReconciler(t, ports)
	reconciler.Observe(membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil))
	changed := membershipSnapshot(2,
		[]membershipRouteSpec{
			membershipActiveSpec(1, 2, 2, 2),
			membershipActiveSpec(1, 2, 2, 2),
		}, nil)
	result := reconciler.Observe(changed)
	if !result.Rejected {
		t.Fatalf("player shard-count change must fail closed, got %+v", result)
	}
}

func TestMembershipRejectsFarmShardCountChange(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipReconciler(t, ports)
	reconciler.Observe(membershipSnapshot(1,
		[]membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)},
		[]membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}))
	changed := membershipSnapshot(2,
		[]membershipRouteSpec{membershipActiveSpec(1, 2, 2, 2)},
		[]membershipRouteSpec{
			membershipActiveSpec(1, 2, 2, 2),
			membershipActiveSpec(1, 2, 2, 2),
		})
	result := reconciler.Observe(changed)
	if !result.Rejected {
		t.Fatalf("farm shard-count change must fail closed, got %+v", result)
	}
}

func TestMembershipRestrictsAllowedDomains(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler, err := authoritycomposition.NewShardMembershipReconciler(authoritycomposition.MembershipReconcilerOptions{
		HolderID:               membershipLocalHolder,
		HolderBootID:           membershipLocalBoot,
		Installer:              ports,
		Fencer:                 ports,
		Remover:                ports,
		AllowedDomains:         []actorrouting.RouteDomain{actorrouting.DomainPlayer},
		AuthorityLayoutVersion: "v1",
		MaxRetries:             3,
		RetryDelay:             time.Millisecond,
		Now:                    time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reconciler.Close)

	// Both domains have a local route; only PLAYER may be reconciled.
	result := reconciler.Observe(membershipSnapshot(1,
		[]membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)},
		[]membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}))
	if !result.FullyReconciled {
		t.Fatalf("player-only reconciler not reconciled: %+v", result)
	}
	members := reconciler.CurrentInstalledMembership()
	if len(members) != 1 || members[0].Key.Domain != actorrouting.DomainPlayer {
		t.Fatalf("allowed-domains must restrict to PLAYER, got %+v", members)
	}
	installs := ports.Installs()
	if len(installs) != 1 || installs[0].Key.Domain != actorrouting.DomainPlayer {
		t.Fatalf("install must be player-only, got %+v", installs)
	}
}

func TestDuplicateSnapshotDoesNotResetRetryBudget(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipReconciler(t, ports)
	ports.SetInstallErr(errors.New("persistent install failure"))
	rev := membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil)
	reconciler.Observe(rev) // first failure, retries=1
	reconciler.Observe(rev) // duplicate: must NOT reset the retry budget
	reconciler.Observe(rev) // duplicate again
	degraded := reconciler.Reconcile().Degraded
	if len(degraded) != 1 || degraded[0].Retries < 3 {
		t.Fatalf("duplicate snapshots must not reset the retry budget, degraded=%+v", degraded)
	}
	if reconciler.LastFullyReconciledRevision() != 0 {
		t.Fatalf("failed snapshot must not advance the reconciled revision")
	}
}

func TestShardDegradedIncludesDesiredAndOldGeneration(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipReconciler(t, ports)
	reconciler.Observe(membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil))
	ports.Reset()

	// REPLACE fails at the install step: fence succeeds, install of the new
	// generation (writer epoch 2) fails.
	ports.SetInstallErrFunc(func(m authoritycomposition.ShardMembership) error {
		if m.Generation.WriterEpoch == 2 {
			return errors.New("replace install failure")
		}
		return nil
	})
	result := reconciler.Observe(membershipSnapshot(2, []membershipRouteSpec{membershipActiveSpec(1, 2, 2, 2)}, nil))
	if len(result.Degraded) != 1 {
		t.Fatalf("expected one degraded shard, got %+v", result.Degraded)
	}
	degraded := result.Degraded[0]
	if degraded.Action != authoritycomposition.MembershipActionReplace {
		t.Fatalf("expected REPLACE degraded, got %+v", degraded)
	}
	if degraded.DesiredGeneration.WriterEpoch != 2 || degraded.DesiredGeneration.AssignmentRevision != 2 {
		t.Fatalf("degraded must carry desired generation, got %+v", degraded.DesiredGeneration)
	}
	if degraded.OldGeneration.WriterEpoch != 1 || degraded.OldGeneration.AssignmentRevision != 1 {
		t.Fatalf("degraded must carry old generation, got %+v", degraded.OldGeneration)
	}
	if degraded.SnapshotRevision != 2 {
		t.Fatalf("degraded snapshot revision=%d, want 2", degraded.SnapshotRevision)
	}
	if degraded.ActionToken == 0 {
		t.Fatal("degraded must carry the failed action token")
	}
}

// membershipBlockedReconciler builds a reconciler without an automatic
// cleanup so blocking tests control the Close lifecycle explicitly.
func membershipBlockedReconciler(t *testing.T, ports *testkit.FakeMembershipPorts) *authoritycomposition.ShardMembershipReconciler {
	t.Helper()
	reconciler, err := authoritycomposition.NewShardMembershipReconciler(authoritycomposition.MembershipReconcilerOptions{
		HolderID:     membershipLocalHolder,
		HolderBootID: membershipLocalBoot,
		Installer:    ports,
		Fencer:       ports,
		Remover:      ports,
		MaxRetries:   3,
		RetryDelay:   time.Millisecond,
		Now:          time.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return reconciler
}

func waitMembershipCondition(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition not met within deadline")
	}
}

func TestNewRevisionIsObservedWhileOldPortIsBlocked(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipBlockedReconciler(t, ports)
	block := make(chan struct{})
	ports.SetPrepareBlock(block)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = reconciler.Observe(membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil))
	}()
	waitMembershipCondition(t, func() bool { return len(ports.Prepares()) > 0 })

	// A newer revision must be observable while the old install is blocked.
	start := time.Now()
	result := reconciler.Observe(membershipSnapshot(2, []membershipRouteSpec{membershipActiveSpec(1, 2, 2, 2)}, nil))
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("Observe blocked behind an in-flight Port")
	}
	if result.Closed || result.Stale {
		t.Fatalf("new revision must be observed, got %+v", result)
	}
	if reconciler.LastObservedRevision() != 2 {
		t.Fatalf("observed revision=%d, want 2", reconciler.LastObservedRevision())
	}

	ports.SetPrepareBlock(nil)
	close(block)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked observe did not finish")
	}
	reconciler.Close()
}

func TestMembershipGettersRemainResponsiveWhilePortBlocked(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipBlockedReconciler(t, ports)
	block := make(chan struct{})
	ports.SetPrepareBlock(block)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = reconciler.Observe(membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil))
	}()
	waitMembershipCondition(t, func() bool { return len(ports.Prepares()) > 0 })

	start := time.Now()
	_ = reconciler.LastObservedRevision()
	_ = reconciler.LastFullyReconciledRevision()
	_ = reconciler.CurrentInstalledMembership()
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("getters stalled behind a blocked Port")
	}

	ports.SetPrepareBlock(nil)
	close(block)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked observe did not finish")
	}
	reconciler.Close()
}

func TestCloseCancelsBlockedMembershipPort(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipBlockedReconciler(t, ports)
	block := make(chan struct{})
	ports.SetPrepareBlock(block)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = reconciler.Observe(membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil))
	}()
	waitMembershipCondition(t, func() bool { return len(ports.Prepares()) > 0 })

	reconciler.Close() // cancels the reconciler context; the blocked Port returns
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not cancel the blocked Port")
	}
	if result := reconciler.Observe(membershipSnapshot(2, nil, nil)); !result.Closed {
		t.Fatalf("post-close Observe must report closed, got %+v", result)
	}
	close(block)
}

func TestBlockedOldInstallCannotCommitAfterNewRevision(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipBlockedReconciler(t, ports)
	block := make(chan struct{})
	ports.SetPrepareBlock(block)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = reconciler.Observe(membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil))
	}()
	waitMembershipCondition(t, func() bool { return len(ports.Prepares()) > 0 })

	// A newer revision for the same shard supersedes the blocked install.
	_ = reconciler.Observe(membershipSnapshot(2, []membershipRouteSpec{membershipActiveSpec(1, 2, 2, 2)}, nil))
	ports.SetPrepareBlock(nil)
	close(block)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked observe did not finish")
	}

	// The stale install result must be dropped; only the new generation may
	// be installed and the reconciler must still converge.
	waitMembershipCondition(t, func() bool { return reconciler.Reconcile().FullyReconciled })
	members := reconciler.CurrentInstalledMembership()
	if len(members) != 1 || members[0].Generation.WriterEpoch != 2 {
		t.Fatalf("stale generation committed after supersede: %+v", members)
	}
	reconciler.Close()
}

func TestBlockedOldRemoveCannotRemoveNewGeneration(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipBlockedReconciler(t, ports)
	reconciler.Observe(membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil))
	ports.Reset()

	block := make(chan struct{})
	ports.SetRemoveBlock(block)
	done := make(chan struct{})
	go func() {
		defer close(done)
		// shard 0 moves away: REMOVE (fence ok, remove blocked).
		_ = reconciler.Observe(membershipSnapshot(2, []membershipRouteSpec{membershipActiveSpec(0, 2, 2, 2)}, nil))
	}()
	waitMembershipCondition(t, func() bool { return len(ports.Removes()) > 0 })

	// The shard returns to this holder under a newer generation while the old
	// remove is still blocked.
	_ = reconciler.Observe(membershipSnapshot(3, []membershipRouteSpec{membershipActiveSpec(1, 3, 3, 3)}, nil))
	ports.SetRemoveBlock(nil)
	close(block)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked remove did not finish")
	}

	waitMembershipCondition(t, func() bool { return reconciler.Reconcile().FullyReconciled })
	members := reconciler.CurrentInstalledMembership()
	if len(members) != 1 || members[0].Generation.WriterEpoch != 3 {
		t.Fatalf("stale remove must not remove the new generation: %+v", members)
	}
	reconciler.Close()
}

func TestBlockedOldFenceCannotFenceNewGeneration(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipBlockedReconciler(t, ports)
	reconciler.Observe(membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil))
	ports.Reset()

	block := make(chan struct{})
	ports.SetFenceBlock(block)
	done := make(chan struct{})
	go func() {
		defer close(done)
		// REPLACE: fence of generation 1 blocks.
		_ = reconciler.Observe(membershipSnapshot(2, []membershipRouteSpec{membershipActiveSpec(1, 2, 2, 2)}, nil))
	}()
	waitMembershipCondition(t, func() bool { return len(ports.Fences()) > 0 })

	// Generation 3 supersedes while the old fence is blocked.
	_ = reconciler.Observe(membershipSnapshot(3, []membershipRouteSpec{membershipActiveSpec(1, 3, 3, 3)}, nil))
	ports.SetFenceBlock(nil)
	close(block)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked fence did not finish")
	}

	waitMembershipCondition(t, func() bool { return reconciler.Reconcile().FullyReconciled })
	members := reconciler.CurrentInstalledMembership()
	if len(members) != 1 || members[0].Generation.WriterEpoch != 3 {
		t.Fatalf("stale fence must not fence the new generation: %+v", members)
	}
	reconciler.Close()
}

// TestPreparedOldInstallIsDiscardedAfterNewRevision is the deterministic
// PhaseBarrier test for F1: a revision-N prepare is blocked in the network
// phase; revision N+1 supersedes it; when the block is released the stale
// prepared generation is discarded and never committed into the registry.
func TestPreparedOldInstallIsDiscardedAfterNewRevision(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipBlockedReconciler(t, ports)
	block := make(chan struct{})
	ports.SetPrepareBlock(block)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = reconciler.Observe(membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil))
	}()
	waitMembershipCondition(t, func() bool { return len(ports.Prepares()) > 0 })

	// Revision 2 supersedes while revision 1's prepare is blocked.
	_ = reconciler.Observe(membershipSnapshot(2, []membershipRouteSpec{membershipActiveSpec(1, 2, 2, 2)}, nil))

	ports.SetPrepareBlock(nil)
	close(block)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked prepare did not finish")
	}

	// The stale prepared generation (revision 1) must never have been
	// committed.  Once the block is released the superseding revision-2
	// install may already have landed (the reconciler drives it immediately),
	// so only the stale generation is forbidden here; convergence to exactly
	// one revision-2 install is asserted below.
	for _, inst := range ports.Installs() {
		if inst.Generation.WriterEpoch != 2 {
			t.Fatalf("stale prepared generation was committed: %+v", ports.Installs())
		}
	}
	// The reconciler converges to revision 2 only.
	waitMembershipCondition(t, func() bool { return reconciler.Reconcile().FullyReconciled })
	installs := ports.Installs()
	if len(installs) != 1 || installs[0].Generation.WriterEpoch != 2 {
		t.Fatalf("expected exactly generation-2 install, got %+v", installs)
	}
	reconciler.Close()
}

// TestPreparedOldInstallCannotBecomeAdmittedAfterRemove proves that a prepare
// superseded by a removal (the shard is no longer desired) is discarded and
// never becomes admitted.
func TestPreparedOldInstallCannotBecomeAdmittedAfterRemove(t *testing.T) {
	ports := testkit.NewFakeMembershipPorts()
	reconciler := membershipBlockedReconciler(t, ports)
	block := make(chan struct{})
	ports.SetPrepareBlock(block)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = reconciler.Observe(membershipSnapshot(1, []membershipRouteSpec{membershipActiveSpec(1, 1, 1, 1)}, nil))
	}()
	waitMembershipCondition(t, func() bool { return len(ports.Prepares()) > 0 })

	// Revision 2 removes the shard from this holder while the prepare is
	// blocked; the new plan has no action for it at all.
	_ = reconciler.Observe(membershipSnapshot(2, []membershipRouteSpec{membershipActiveSpec(0, 2, 2, 2)}, nil))

	ports.SetPrepareBlock(nil)
	close(block)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked prepare did not finish")
	}

	if len(ports.Installs()) != 0 {
		t.Fatalf("superseded prepare became admitted: %+v", ports.Installs())
	}
	if len(reconciler.CurrentInstalledMembership()) != 0 {
		t.Fatalf("installed=%d, want 0", len(reconciler.CurrentInstalledMembership()))
	}
	waitMembershipCondition(t, func() bool { return reconciler.Reconcile().FullyReconciled })
	if len(ports.Installs()) != 0 || len(reconciler.CurrentInstalledMembership()) != 0 {
		t.Fatalf("removed shard must stay uninstalled: installs=%d installed=%d",
			len(ports.Installs()), len(reconciler.CurrentInstalledMembership()))
	}
	reconciler.Close()
}
