package authoritycomposition

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	routeauthority "farmgame/internal/logic/actor/authority"
	"farmgame/internal/storage/authoritystore"
)

const (
	adapterLayout = "v1"
	adapterHolder = "holder-2"
	adapterBoot   = "boot-2"
	adapterSeed   = uint64(202608060002)
)

type adapterClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *adapterClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *adapterClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type membershipAdapterFixture struct {
	t          *testing.T
	clock      *adapterClock
	store      *authoritystore.MemoryStore
	registry   *routeauthority.LeaseGuardRegistry
	adapter    *RealMembershipAdapter
	reconciler *ShardMembershipReconciler
}

func newMembershipAdapterFixture(t *testing.T) *membershipAdapterFixture {
	t.Helper()
	clock := &adapterClock{t: time.Now().Truncate(time.Second)}
	calc, err := actorrouting.NewCalculator("test-realm", actorrouting.HashProfile{
		Version: actorrouting.HashVersionXXH64V2,
		Seed:    adapterSeed,
	}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver:               calc,
		AuthorityLayoutVersion: adapterLayout,
		Now:                    clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := routeauthority.NewLeaseGuardRegistry()
	adapter, err := NewRealMembershipAdapter(RealMembershipAdapterOptions{
		Store:                  store,
		Registry:               registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2,
		HashSeed:               adapterSeed,
		HolderID:               adapterHolder,
		HolderBootID:           adapterBoot,
		LeaseTTL:               30 * time.Second,
		SafetyMargin:           5 * time.Second,
		Now:                    clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	reconciler, err := NewShardMembershipReconciler(MembershipReconcilerOptions{
		HolderID:               adapterHolder,
		HolderBootID:           adapterBoot,
		Installer:              adapter,
		Fencer:                 adapter,
		Remover:                adapter,
		AllowedDomains:         []actorrouting.RouteDomain{actorrouting.DomainPlayer},
		AuthorityLayoutVersion: adapterLayout,
		MaxRetries:             3,
		RetryDelay:             time.Millisecond,
		Now:                    clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reconciler.Close)
	return &membershipAdapterFixture{t: t, clock: clock, store: store, registry: registry, adapter: adapter, reconciler: reconciler}
}

func (f *membershipAdapterFixture) claimPlayerShard(shard uint32, holder, boot, transition string) {
	f.t.Helper()
	result, err := f.store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller: authoritystore.TrustedShardAuthorityCaller{
			Kind:       authoritystore.ShardAuthorityCallerRouteManager,
			InstanceID: "route-manager",
			BootID:     "rm-1",
		},
		TransitionID:           transition,
		AuthorityLayoutVersion: adapterLayout,
		Domain:                 actorrouting.DomainPlayer,
		VirtualShard:           shard,
		NewHolderID:            holder,
		NewHolderBootID:        boot,
		RequestedTTLms:         int64((30 * time.Second).Milliseconds()),
	})
	if err != nil {
		f.t.Fatal(err)
	}
	if result.Status != authoritystore.ShardTransitionApplied && result.Status != authoritystore.ShardTransitionAlreadyApplied {
		f.t.Fatalf("claim shard %d failed: %s", shard, result.Status)
	}
}

func (f *membershipAdapterFixture) playerSnapshot(rev uint64, holderIndexes []int, writerEpochs []uint64) actorrouting.RouteSnapshot {
	f.t.Helper()
	routes := make([]actorrouting.ShardRoute, len(holderIndexes))
	for i, holderIndex := range holderIndexes {
		writer := uint64(1)
		if i < len(writerEpochs) {
			writer = writerEpochs[i]
		}
		routes[i] = actorrouting.ShardRoute{
			HolderIndex: holderIndex, State: actorrouting.ShardStateActive,
			ShardRevision: 1, ExpectedWriterEpoch: writer, ExpectedLeaseEpoch: 1,
			LeaseUntilMs: f.clock.Now().Add(30 * time.Second).UnixMilli(),
		}
	}
	return actorrouting.RouteSnapshot{
		Realm: "test-realm", GlobalRevision: rev,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		Holders: []actorrouting.HolderRef{
			{InstanceID: "holder-1", BootID: "boot-1", Endpoint: "h1:9100", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "holder-2", BootID: "boot-2", Endpoint: "h2:9100", State: actorrouting.HolderStateActive, Weight: 1},
		},
		Player: actorrouting.DomainSnapshot{ShardCount: uint32(len(routes)), Routes: routes},
		Farm:   actorrouting.DomainSnapshot{ShardCount: 0},
	}
}

func adapterShardKey(shard uint32) ShardKey {
	return ShardKey{Domain: actorrouting.DomainPlayer, Shard: shard}
}

func adapterGeneration(rev, writer, lease uint64) MembershipGeneration {
	return MembershipGeneration{
		AssignmentRevision: rev, WriterEpoch: writer, LeaseEpoch: lease,
		HolderID: adapterHolder, HolderBootID: adapterBoot, LeaseUntilMs: 1,
	}
}

func (f *membershipAdapterFixture) guardAdmitted(shard uint32) bool {
	snapshot, ok := f.registry.Snapshot(actorrouting.DomainPlayer, shard)
	return ok && snapshot.Admitted
}

// installMember drives the real two-phase install directly (prepare then
// commit), as the reconciler would for an ADD/REPLACE action.
func (f *membershipAdapterFixture) installMember(member ShardMembership) error {
	f.t.Helper()
	prepared, err := f.adapter.PrepareMembership(context.Background(), member)
	if err != nil {
		return err
	}
	return f.adapter.CommitPreparedMembership(context.Background(), *prepared)
}

func TestRealMembershipPrepareReturnsExactRenewedDeadline(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-renewed-deadline")
	requestedDeadline := fixture.clock.Now().Add(30 * time.Second).UnixMilli()
	fixture.clock.Advance(5 * time.Second)
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	member.Generation.LeaseUntilMs = requestedDeadline
	prepared, err := fixture.adapter.PrepareMembership(context.Background(), member)
	if err != nil {
		t.Fatal(err)
	}
	wantDeadline := fixture.clock.Now().Add(30 * time.Second).UnixMilli()
	if prepared.Generation.LeaseUntilMs != wantDeadline || prepared.Authority.LeaseUntilMs != wantDeadline {
		t.Fatalf("prepared deadline generation=%d authority=%d want=%d", prepared.Generation.LeaseUntilMs, prepared.Authority.LeaseUntilMs, wantDeadline)
	}
	if prepared.Generation.AssignmentRevision != member.Generation.AssignmentRevision ||
		prepared.Generation.WriterEpoch != member.Generation.WriterEpoch ||
		prepared.Generation.LeaseEpoch != member.Generation.LeaseEpoch {
		t.Fatalf("renew changed generation fence: got=%+v want=%+v", prepared.Generation, member.Generation)
	}
}

func TestRealMembershipCommitChecksContextImmediatelyBeforeGuardInstall(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-context-preinstall")
	prepared, err := fixture.adapter.PrepareMembership(context.Background(), member)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	fixture.adapter.commitPreparedHook = cancel
	if err := fixture.adapter.CommitPreparedMembership(ctx, *prepared); !errors.Is(err, context.Canceled) {
		t.Fatalf("commit error=%v want context.Canceled", err)
	}
	if _, ok := fixture.registry.Snapshot(member.Key.Domain, member.Key.Shard); ok {
		t.Fatal("adapter installed a guard after its final pre-install context check")
	}
}

func TestRealMembershipInstallZeroToOne(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-a1")

	result := fixture.reconciler.Observe(fixture.playerSnapshot(1, []int{1}, nil))
	if result.Stale || result.Duplicate || !result.FullyReconciled {
		t.Fatalf("install observe result=%+v", result)
	}
	if !fixture.guardAdmitted(0) {
		t.Fatal("shard 0 must be admitted after real install")
	}
	if len(fixture.reconciler.CurrentInstalledMembership()) != 1 {
		t.Fatalf("installed=%d, want 1", len(fixture.reconciler.CurrentInstalledMembership()))
	}
	if fixture.reconciler.LastFullyReconciledRevision() != 1 {
		t.Fatalf("reconciled revision=%d, want 1", fixture.reconciler.LastFullyReconciledRevision())
	}
}

func TestRealMembershipRemoveOneToZero(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-b1")
	if result := fixture.reconciler.Observe(fixture.playerSnapshot(1, []int{1}, nil)); !result.FullyReconciled {
		t.Fatalf("install failed: %+v", result)
	}

	// Shard moves to the other holder: REMOVE must fence and uninstall.
	result := fixture.reconciler.Observe(fixture.playerSnapshot(2, []int{0}, nil))
	if result.Stale || !result.FullyReconciled {
		t.Fatalf("remove observe result=%+v", result)
	}
	if fixture.guardAdmitted(0) {
		t.Fatal("shard 0 must not be admitted after real remove")
	}
	if _, ok := fixture.registry.Snapshot(actorrouting.DomainPlayer, 0); ok {
		t.Fatal("guard must be removed from the registry")
	}
	if len(fixture.reconciler.CurrentInstalledMembership()) != 0 {
		t.Fatalf("installed=%d, want 0", len(fixture.reconciler.CurrentInstalledMembership()))
	}
}

func TestDuplicateGenerationInstallIsIdempotent(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-c1")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}

	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	if !fixture.guardAdmitted(0) {
		t.Fatal("first install must admit")
	}
	// Same generation re-install is idempotent.
	if err := fixture.installMember(member); err != nil {
		t.Fatalf("duplicate generation install must be idempotent: %v", err)
	}
	if !fixture.guardAdmitted(0) {
		t.Fatal("duplicate install must keep the shard admitted")
	}
}

func TestOldInstallCannotReplaceNewGeneration(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-d1")
	gen1 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(gen1); err != nil {
		t.Fatal(err)
	}

	// The repository advances to generation 2 (e.g. a control-plane epoch
	// bump), and the new generation is installed.
	authority := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2,
		HashSeed:               adapterSeed,
		Domain:                 actorrouting.DomainPlayer,
		VirtualShard:           0,
		HolderID:               adapterHolder,
		HolderBootID:           adapterBoot,
		AssignmentRevision:     2, WriterEpoch: 2, LeaseEpoch: 2,
		State:        authoritystore.ShardAuthorityActive,
		LeaseUntilMs: fixture.clock.Now().Add(30 * time.Second).UnixMilli(),
	}
	if err := fixture.store.SetShardWriteAuthority(authority); err != nil {
		t.Fatal(err)
	}
	gen2 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(2, 2, 2)}
	if err := fixture.installMember(gen2); err != nil {
		t.Fatalf("new generation install must succeed: %v", err)
	}

	// A stale install of generation 1 must be rejected and leave generation 2
	// untouched.
	if err := fixture.installMember(gen1); err == nil {
		t.Fatal("stale generation install must be rejected")
	}
	snapshot, ok := fixture.registry.Snapshot(actorrouting.DomainPlayer, 0)
	if !ok || !snapshot.Admitted {
		t.Fatal("generation 2 guard must remain admitted")
	}
	if snapshot.Authority.WriterEpoch != 2 || snapshot.Authority.AssignmentRevision != 2 {
		t.Fatalf("installed generation was replaced by the stale install: %+v", snapshot.Authority)
	}
}

func TestOldFenceCannotFenceNewGeneration(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-e1")
	gen1 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(gen1); err != nil {
		t.Fatal(err)
	}
	authority := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2,
		HashSeed:               adapterSeed,
		Domain:                 actorrouting.DomainPlayer,
		VirtualShard:           0,
		HolderID:               adapterHolder,
		HolderBootID:           adapterBoot,
		AssignmentRevision:     2, WriterEpoch: 2, LeaseEpoch: 2,
		State:        authoritystore.ShardAuthorityActive,
		LeaseUntilMs: fixture.clock.Now().Add(30 * time.Second).UnixMilli(),
	}
	if err := fixture.store.SetShardWriteAuthority(authority); err != nil {
		t.Fatal(err)
	}
	gen2 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(2, 2, 2)}
	if err := fixture.installMember(gen2); err != nil {
		t.Fatal(err)
	}

	// A stale fence of generation 1 must not fence generation 2.
	if err := fixture.adapter.FenceMembership(context.Background(), gen1); err == nil {
		t.Fatal("stale generation fence must be rejected")
	}
	if !fixture.guardAdmitted(0) {
		t.Fatal("new generation must remain admitted after a stale fence")
	}
}

func TestOldRemoveCannotRemoveNewGeneration(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-f1")
	gen1 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(gen1); err != nil {
		t.Fatal(err)
	}
	authority := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2,
		HashSeed:               adapterSeed,
		Domain:                 actorrouting.DomainPlayer,
		VirtualShard:           0,
		HolderID:               adapterHolder,
		HolderBootID:           adapterBoot,
		AssignmentRevision:     2, WriterEpoch: 2, LeaseEpoch: 2,
		State:        authoritystore.ShardAuthorityActive,
		LeaseUntilMs: fixture.clock.Now().Add(30 * time.Second).UnixMilli(),
	}
	if err := fixture.store.SetShardWriteAuthority(authority); err != nil {
		t.Fatal(err)
	}
	gen2 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(2, 2, 2)}
	if err := fixture.installMember(gen2); err != nil {
		t.Fatal(err)
	}

	if err := fixture.adapter.RemoveMembership(context.Background(), gen1); err == nil {
		t.Fatal("stale generation remove must be rejected")
	}
	if !fixture.guardAdmitted(0) {
		t.Fatal("new generation guard must survive a stale remove")
	}
}

func TestAuthorityMismatchKeepsShardFailClosed(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	// Repository holds generation 1 for the local holder, but the desired
	// snapshot asks for writer epoch 2 -> the install must fail closed.
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-g1")

	result := fixture.reconciler.Observe(fixture.playerSnapshot(1, []int{1}, []uint64{2}))
	if result.FullyReconciled {
		t.Fatalf("mismatched authority must not reconcile: %+v", result)
	}
	if fixture.guardAdmitted(0) {
		t.Fatal("shard must stay fail-closed when authority does not match")
	}
	if len(fixture.reconciler.CurrentInstalledMembership()) != 0 {
		t.Fatalf("mismatched shard must not be installed")
	}
}

func TestLocalDeadlineExpiryStopsAdmission(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-h1")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	if !fixture.guardAdmitted(0) {
		t.Fatal("install must admit before deadline expiry")
	}

	// The accepted TTL (30s) minus safety margin (5s) means the local
	// monotonic deadline passes well inside 35s; expiry must stop admission
	// without consulting a remote clock.
	fixture.clock.Advance(35 * time.Second)
	if fixture.guardAdmitted(0) {
		t.Fatal("shard must stop being admitted after local deadline expiry")
	}
}

func TestSingleShardFailureDoesNotAffectOtherShard(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	// shard 0 is healthy for the local holder.
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-i0")
	// shard 1 belongs to the other holder in the repository; the snapshot
	// wrongly assigns it to the local holder -> that install must fail closed
	// without affecting shard 0.
	fixture.claimPlayerShard(1, "holder-1", "boot-1", "claim-i1")

	result := fixture.reconciler.Observe(fixture.playerSnapshot(1, []int{1, 1}, nil))
	if result.FullyReconciled {
		t.Fatalf("one failing shard must keep the pass non-fully-reconciled: %+v", result)
	}
	if !fixture.guardAdmitted(0) {
		t.Fatal("healthy shard 0 must stay admitted")
	}
	if fixture.guardAdmitted(1) {
		t.Fatal("mismatched shard 1 must stay fail-closed")
	}
	degraded := result.Degraded
	if len(degraded) != 1 || degraded[0].Shard.Shard != 1 {
		t.Fatalf("only shard 1 must be degraded, got %+v", degraded)
	}
}

func TestMembershipInstallDoesNotLoadActors(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-j1")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	if !fixture.guardAdmitted(0) {
		t.Fatal("install must admit the guard")
	}
	// No actor record may have been created or loaded by membership install.
	if _, ok := fixture.store.Inspect(authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 1}); ok {
		t.Fatal("membership install must not load actors")
	}
}

func adapterAuthorityFor(rev, writer, lease uint64, clock *adapterClock) authoritystore.ShardWriteAuthority {
	return authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2,
		HashSeed:               adapterSeed,
		Domain:                 actorrouting.DomainPlayer,
		VirtualShard:           0,
		HolderID:               adapterHolder,
		HolderBootID:           adapterBoot,
		AssignmentRevision:     rev, WriterEpoch: writer, LeaseEpoch: lease,
		State:        authoritystore.ShardAuthorityActive,
		LeaseUntilMs: clock.Now().Add(30 * time.Second).UnixMilli(),
	}
}

// TestPreparedOldInstallCannotOverwriteNewGeneration is the adapter-level
// PhaseBarrier test for F1: a gen1 prepare succeeds (Renew done), a gen2
// install commits first, then the stale gen1 commit must be rejected and leave
// the registry at gen2.
func TestPreparedOldInstallCannotOverwriteNewGeneration(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-m1")
	gen1 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	gen2 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(2, 2, 2)}

	preparedCh := make(chan *PreparedMembership, 1)
	gate := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		prepared, err := fixture.adapter.PrepareMembership(context.Background(), gen1)
		if err != nil {
			preparedCh <- nil
			return
		}
		preparedCh <- prepared
		<-gate
		_ = fixture.adapter.CommitPreparedMembership(context.Background(), *prepared)
	}()
	prepared := <-preparedCh
	if prepared == nil {
		t.Fatal("gen1 prepare must succeed")
	}

	// The repository advances to gen2 and gen2 commits before the stale gen1
	// commit is released.
	if err := fixture.store.SetShardWriteAuthority(adapterAuthorityFor(2, 2, 2, fixture.clock)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.installMember(gen2); err != nil {
		t.Fatalf("gen2 install must succeed: %v", err)
	}

	close(gate)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stale commit did not finish")
	}

	snapshot, ok := fixture.registry.Snapshot(actorrouting.DomainPlayer, 0)
	if !ok || !snapshot.Admitted {
		t.Fatal("gen2 guard must remain admitted")
	}
	if snapshot.Authority.WriterEpoch != 2 || snapshot.Authority.AssignmentRevision != 2 {
		t.Fatalf("stale gen1 commit overwrote the new generation: %+v", snapshot.Authority)
	}
}

// TestPreparedMembershipExpiredBeforeCommitStaysFenced proves the frozen
// invariant that a prepared membership whose local deadline has already passed
// must never be admitted at commit time.
func TestPreparedMembershipExpiredBeforeCommitStaysFenced(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-n1")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}

	preparedCh := make(chan *PreparedMembership, 1)
	gate := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		prepared, err := fixture.adapter.PrepareMembership(context.Background(), member)
		if err != nil {
			preparedCh <- nil
			return
		}
		preparedCh <- prepared
		<-gate
		_ = fixture.adapter.CommitPreparedMembership(context.Background(), *prepared)
	}()
	prepared := <-preparedCh
	if prepared == nil {
		t.Fatal("prepare must succeed")
	}

	// The local monotonic deadline (accepted TTL 30s - safety margin 5s) has
	// passed before the commit is released.
	fixture.clock.Advance(35 * time.Second)
	close(gate)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("expired commit did not finish")
	}

	if fixture.guardAdmitted(0) {
		t.Fatal("expired prepared membership must not be admitted")
	}
	if _, ok := fixture.registry.Snapshot(actorrouting.DomainPlayer, 0); ok {
		t.Fatal("no guard may be installed for an expired prepared membership")
	}
}

// TestSameGenerationInstallRefreshesLocalDeadline proves F3: a successful
// same-generation install must refresh the existing runtime's local monotonic
// deadline from the new accepted TTL, so the local deadline never lags the
// remote lease.
func TestSameGenerationInstallRefreshesLocalDeadline(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-p1")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}

	// First deadline is approximately t0 + acceptedTTL(30s) - margin(5s).
	fixture.clock.Advance(15 * time.Second) // t1 = t0 + 15s
	// Same generation re-install: Renew extends the lease and the commit must
	// refresh the local deadline to approximately t1 + 25s.
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(12 * time.Second) // t2 = t0 + 27s (> old t0+25s, < refreshed t1+25s)
	if !fixture.guardAdmitted(0) {
		t.Fatal("same-generation refresh must extend the local deadline")
	}
}

// TestSameGenerationRefreshDoesNotCreateSecondGuard proves that an identical
// generation re-install reuses the existing HolderLeaseRuntime and never
// creates a second guard.
func TestSameGenerationRefreshDoesNotCreateSecondGuard(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-p2")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	if len(fixture.registry.SnapshotAll()) != 1 {
		t.Fatalf("guards=%d, want 1 (no second guard)", len(fixture.registry.SnapshotAll()))
	}
}

// assertRegistryMatchesInstalled verifies the terminating consistency
// invariant after a concurrent race: every registry-admitted guard has an
// exactly matching reconciler installed generation and vice versa.
func assertRegistryMatchesInstalled(t *testing.T, fixture *membershipAdapterFixture) {
	t.Helper()
	installed := fixture.reconciler.CurrentInstalledMembership()
	byKey := make(map[ShardKey]MembershipGeneration, len(installed))
	for _, member := range installed {
		byKey[member.Key] = member.Generation
	}
	guards := fixture.registry.SnapshotAll()
	admitted := make(map[ShardKey]MembershipGeneration)
	for _, guard := range guards {
		if !guard.Admitted {
			continue
		}
		key := ShardKey{Domain: guard.Authority.Domain, Shard: guard.Authority.VirtualShard}
		admitted[key] = MembershipGeneration{
			AssignmentRevision: guard.Authority.AssignmentRevision,
			WriterEpoch:        guard.Authority.WriterEpoch,
			LeaseEpoch:         guard.Authority.LeaseEpoch,
			HolderID:           guard.Authority.HolderID,
			HolderBootID:       guard.Authority.HolderBootID,
			LeaseUntilMs:       guard.Authority.LeaseUntilMs,
		}
	}
	if len(admitted) != len(byKey) {
		t.Fatalf("registry admitted guards (%d) != reconciler installed (%d)", len(admitted), len(byKey))
	}
	for key, gen := range byKey {
		guardGen, ok := admitted[key]
		if !ok {
			t.Fatalf("installed %v is missing from the registry", key)
		}
		if guardGen.WriterEpoch != gen.WriterEpoch || guardGen.AssignmentRevision != gen.AssignmentRevision || guardGen.LeaseEpoch != gen.LeaseEpoch {
			t.Fatalf("generation mismatch for %v: installed=%+v guard=%+v", key, gen, guardGen)
		}
	}
}

// TestMembershipAdapterConcurrentConsistency races Observe/Reconcile against
// the real adapter and verifies that at the end the registry's admitted guards
// exactly match the reconciler's installed generations.
func TestMembershipAdapterConcurrentConsistency(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-q0")

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
				rev := uint64(time.Now().UnixNano()%50 + 1)
				fixture.reconciler.Observe(fixture.playerSnapshot(rev, []int{1}, nil))
				fixture.reconciler.Reconcile()
			}
		})
	}
	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()

	assertRegistryMatchesInstalled(t, fixture)
}
