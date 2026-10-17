package authoritycomposition

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	routeauthority "farmgame/internal/logic/actor/authority"
	"farmgame/internal/storage/authoritystore"
)

// faultRenewStore wraps a ShardWriteAuthorityStore and injects renew
// failures/statuses/blocking without touching the other authority methods.
type faultRenewStore struct {
	authoritystore.ShardWriteAuthorityStore
	mu           sync.Mutex
	renewErr     error
	statusFn     func(domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardAuthorityTransitionStatus, bool)
	renewBlock   <-chan struct{}
	renewBlockFn func(domain actorrouting.RouteDomain, shard uint32) <-chan struct{}
	renewEntered func()
	renewPanic   any
}

func (f *faultRenewStore) RenewShardAuthority(ctx context.Context, req authoritystore.RenewShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	f.mu.Lock()
	block := f.renewBlock
	if f.renewBlockFn != nil {
		block = f.renewBlockFn(req.Domain, req.VirtualShard)
	}
	err := f.renewErr
	statusFn := f.statusFn
	panicVal := f.renewPanic
	entered := f.renewEntered
	f.mu.Unlock()
	if panicVal != nil {
		panic(panicVal)
	}
	if block != nil {
		if entered != nil {
			entered()
		}
		select {
		case <-ctx.Done():
			return authoritystore.ShardAuthorityTransitionResult{}, ctx.Err()
		case <-block:
		}
	}
	if err != nil {
		return authoritystore.ShardAuthorityTransitionResult{}, err
	}
	if statusFn != nil {
		if status, ok := statusFn(req.Domain, req.VirtualShard); ok {
			return authoritystore.ShardAuthorityTransitionResult{Status: status}, nil
		}
	}
	return f.ShardWriteAuthorityStore.RenewShardAuthority(ctx, req)
}

func (f *faultRenewStore) SetRenewErr(err error) { f.mu.Lock(); defer f.mu.Unlock(); f.renewErr = err }
func (f *faultRenewStore) SetStatusFn(fn func(actorrouting.RouteDomain, uint32) (authoritystore.ShardAuthorityTransitionStatus, bool)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statusFn = fn
}
func (f *faultRenewStore) SetRenewBlock(ch <-chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewBlock = ch
}
func (f *faultRenewStore) SetRenewBlockFn(fn func(actorrouting.RouteDomain, uint32) <-chan struct{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewBlockFn = fn
}
func (f *faultRenewStore) SetRenewEntered(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewEntered = fn
}
func (f *faultRenewStore) SetRenewPanic(p any) { f.mu.Lock(); defer f.mu.Unlock(); f.renewPanic = p }

func waitLeaseCondition(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition not met within deadline")
	}
}

func newSchedulerFixture(t *testing.T) (*membershipAdapterFixture, *faultRenewStore, *PerShardLeaseScheduler) {
	t.Helper()
	fixture := newMembershipAdapterFixture(t)
	fault := &faultRenewStore{ShardWriteAuthorityStore: fixture.store}
	scheduler, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store:                  fault,
		Registry:               fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2,
		HashSeed:               adapterSeed,
		HolderID:               adapterHolder,
		HolderBootID:           adapterBoot,
		LeaseTTL:               30 * time.Second,
		BaseDelay:              5 * time.Millisecond,
		MaxDelay:               200 * time.Millisecond,
		RenewAttemptTimeout:    time.Second,
		RenewRecoveryTimeout:   time.Second,
		Jitter:                 3 * time.Millisecond,
		Now:                    fixture.clock.Now,
		JitterSeed:             42,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scheduler.Close)
	return fixture, fault, scheduler
}

// registerEntry drives the real reservation lifecycle (Reserve then Activate),
// as the reconciler does for an ADD/REPLACE commit.
func registerEntry(t *testing.T, s *PerShardLeaseScheduler, key ShardKey, gen MembershipGeneration) error {
	t.Helper()
	reservation, err := s.Reserve(key, gen)
	if err != nil {
		return err
	}
	return s.Activate(reservation)
}

func TestLeaseSchedulerZeroShardIdleAndClose(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	scheduler, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store:                  fixture.store,
		Registry:               fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2,
		HashSeed:               adapterSeed,
		HolderID:               adapterHolder,
		HolderBootID:           adapterBoot,
		LeaseTTL:               30 * time.Second,
		BaseDelay:              5 * time.Millisecond,
		MaxDelay:               200 * time.Millisecond,
		RenewAttemptTimeout:    time.Second,
		RenewRecoveryTimeout:   time.Second,
		Jitter:                 1 * time.Millisecond,
		Now:                    fixture.clock.Now,
		JitterSeed:             7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if scheduler.EntryCount() != 0 {
		t.Fatalf("zero-shard scheduler must hold no entries, got %d", scheduler.EntryCount())
	}
	before := runtime.NumGoroutine()
	scheduler.Close() // safe with zero entries
	time.Sleep(20 * time.Millisecond)
	if runtime.NumGoroutine() > before+2 {
		t.Fatal("zero-shard scheduler leaked goroutines")
	}
	if _, err := scheduler.Reserve(adapterShardKey(0), adapterGeneration(1, 1, 1)); err == nil {
		t.Fatal("post-close reserve must fail closed")
	}
}

func TestLeaseSchedulerConcurrentCloseSharesCompleteBarrier(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-close-barrier")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, member.Key, member.Generation); err != nil {
		t.Fatal(err)
	}

	closeEntered := make(chan struct{}, 2)
	entryExiting := make(chan struct{})
	releaseEntry := make(chan struct{})
	var exitOnce sync.Once
	scheduler.mu.Lock()
	scheduler.closeEnterHook = func() { closeEntered <- struct{}{} }
	scheduler.entryExitHook = func() {
		exitOnce.Do(func() { close(entryExiting) })
		<-releaseEntry
	}
	scheduler.mu.Unlock()

	returned := make(chan struct{}, 2)
	for range 2 {
		go func() {
			scheduler.Close()
			returned <- struct{}{}
		}()
	}
	for range 2 {
		select {
		case <-closeEntered:
		case <-time.After(time.Second):
			t.Fatal("concurrent Close caller did not enter scheduler barrier")
		}
	}
	select {
	case <-entryExiting:
	case <-time.After(time.Second):
		t.Fatal("scheduler entry did not reach the join barrier")
	}
	select {
	case <-returned:
		t.Fatal("Close returned before the shared entry barrier completed")
	default:
	}
	close(releaseEntry)
	for range 2 {
		select {
		case <-returned:
		case <-time.After(time.Second):
			t.Fatal("concurrent Close did not return after shared barrier release")
		}
	}
	if scheduler.EntryCount() != 0 {
		t.Fatalf("post-Close entries=%d want 0", scheduler.EntryCount())
	}
	scheduler.mu.Lock()
	reservations := len(scheduler.reservations)
	scheduler.mu.Unlock()
	if reservations != 0 {
		t.Fatalf("post-Close reservations=%d want 0", reservations)
	}
	select {
	case <-scheduler.closeDone:
	default:
		t.Fatal("all Close callers returned before closeDone")
	}
}

func TestLeaseSchedulerInstallRegistersExactlyOneEntry(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-s1")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	if scheduler.EntryCount() != 1 {
		t.Fatalf("entries=%d, want 1", scheduler.EntryCount())
	}
}

func TestLeaseSchedulerDuplicateInstallNoDuplicateEntry(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-s2")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	if scheduler.EntryCount() != 1 {
		t.Fatalf("duplicate register must not create a second entry, got %d", scheduler.EntryCount())
	}
}

func TestLeaseSchedulerRemoveCancelsEntry(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-s3")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Cancel(adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	if scheduler.EntryCount() != 0 {
		t.Fatalf("cancel must remove the entry, got %d", scheduler.EntryCount())
	}
	// Cancel is idempotent.
	if err := scheduler.Cancel(adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseSchedulerInflightRenewAfterRemoveCannotReadmit(t *testing.T) {
	fixture, fault, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-s4")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}

	block := make(chan struct{})
	fault.SetRenewBlock(block)
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 1 })

	// Simulate a removal while a renewal is in flight (blocked in the store):
	// the guard is uninstalled first, then the entry is cancelled.
	if err := fixture.adapter.RemoveMembership(context.Background(), member); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Cancel(adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	fault.SetRenewBlock(nil)
	close(block)
	time.Sleep(50 * time.Millisecond)

	// The late renewal response must have been discarded: the removed shard is
	// never re-admitted and the entry stays gone.
	if scheduler.EntryCount() != 0 {
		t.Fatalf("entry must stay cancelled, got %d", scheduler.EntryCount())
	}
	if fixture.guardAdmitted(0) {
		t.Fatal("removed shard must not be re-admitted by a late renewal")
	}
}

func TestLeaseSchedulerInflightG1RenewCannotRefreshG2(t *testing.T) {
	fixture, fault, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-s5")
	gen1 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	gen2 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(2, 2, 2)}
	if err := fixture.installMember(gen1); err != nil {
		t.Fatal(err)
	}

	block := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	fault.SetRenewBlock(block)
	fault.SetRenewEntered(func() { once.Do(func() { close(entered) }) })
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen1.Generation); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("G1 renewal did not enter the store")
	}
	fault.SetRenewEntered(nil)

	// Generation 2 replaces generation 1 while G1's renewal is in flight.
	if err := fixture.store.SetShardWriteAuthority(adapterAuthorityFor(2, 2, 2, fixture.clock)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.installMember(gen2); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen2.Generation); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 1 })

	fault.SetRenewBlock(nil)
	close(block)
	// Deterministic barrier (no fixed sleep): wait until the stale G1
	// LostGeneration response has been fully processed before asserting G2's
	// state, so a cancel-by-key bug cannot slip through the timing gap.
	lostGen := make(chan struct{})
	var lostGenOnce sync.Once
	scheduler.mu.Lock()
	scheduler.lostGenHook = func(key ShardKey, gen MembershipGeneration) { lostGenOnce.Do(func() { close(lostGen) }) }
	scheduler.mu.Unlock()
	select {
	case <-lostGen:
	case <-time.After(2 * time.Second):
		t.Fatal("stale G1 lost-generation response was not processed")
	}

	// G2's guard must remain admitted at G2 and its entry must survive the
	// stale G1 response (only G1 may be cancelled).
	snapshot, ok := fixture.registry.Snapshot(actorrouting.DomainPlayer, 0)
	if !ok || !snapshot.Admitted {
		t.Fatal("G2 guard must remain admitted")
	}
	if snapshot.Authority.WriterEpoch != 2 || snapshot.Authority.AssignmentRevision != 2 {
		t.Fatalf("G1 late renewal corrupted the G2 guard: %+v", snapshot.Authority)
	}
	if scheduler.EntryCount() != 1 {
		t.Fatalf("stale G1 response must not cancel G2's entry, got %d", scheduler.EntryCount())
	}
}

func TestLeaseSchedulerTransientFailureBeforeDeadlineKeepsAdmission(t *testing.T) {
	fixture, fault, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-s6")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}

	fault.SetRenewErr(errors.New("network partition"))
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // let several transient attempts run
	if !fixture.guardAdmitted(0) {
		t.Fatal("transient renewal before deadline must keep the shard admitted")
	}
}

func TestLeaseSchedulerTransientFailurePastDeadlineFences(t *testing.T) {
	fixture, fault, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-s7")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	fault.SetRenewErr(errors.New("network partition"))
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if !fixture.guardAdmitted(0) {
		t.Fatal("must still be admitted before the local deadline")
	}
	// The local monotonic deadline (30s TTL - 5s margin) passes.
	fixture.clock.Advance(35 * time.Second)
	waitLeaseCondition(t, func() bool { return !fixture.guardAdmitted(0) })
}

func TestLeaseSchedulerLostGenerationFencesOnlyTargetShard(t *testing.T) {
	fixture, fault, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-s8")
	fixture.claimPlayerShard(1, adapterHolder, adapterBoot, "claim-s9")
	g0 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	g1 := ShardMembership{Key: adapterShardKey(1), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(g0); err != nil {
		t.Fatal(err)
	}
	if err := fixture.installMember(g1); err != nil {
		t.Fatal(err)
	}

	// Shard 0 loses its generation; shard 1 keeps renewing normally.
	fault.SetStatusFn(func(domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardAuthorityTransitionStatus, bool) {
		if shard == 0 {
			return authoritystore.ShardTransitionFenceMismatch, true
		}
		return 0, false
	})
	if err := registerEntry(t, scheduler, adapterShardKey(0), g0.Generation); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(1), g1.Generation); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return !fixture.guardAdmitted(0) })
	// The lost-shard entry is removed after its guard is fenced; shard 1's
	// entry and guard remain untouched.
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 1 })
	if !fixture.guardAdmitted(1) {
		t.Fatal("shard 1 must stay admitted when shard 0 loses its generation")
	}
}

func TestLeaseSchedulerDeterministicJitterSpreadsShards(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	newSched := func(seed int64) *PerShardLeaseScheduler {
		s, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
			Store: fixture.store, Registry: fixture.registry,
			AuthorityLayoutVersion: adapterLayout,
			HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
			HolderID: adapterHolder, HolderBootID: adapterBoot,
			LeaseTTL: 30 * time.Second, BaseDelay: 5 * time.Millisecond,
			MaxDelay: 200 * time.Millisecond, Jitter: 8 * time.Millisecond,
			RenewAttemptTimeout:  time.Second,
			RenewRecoveryTimeout: time.Second,
			Now:                  fixture.clock.Now, JitterSeed: seed,
		})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s1 := newSched(99)
	delays1 := make([]time.Duration, 0, 8)
	for range 8 {
		delays1 = append(delays1, s1.initialDelay())
	}
	s1.Close()

	s2 := newSched(99) // same seed => identical sequence
	delays2 := make([]time.Duration, 0, 8)
	for range 8 {
		delays2 = append(delays2, s2.initialDelay())
	}
	s2.Close()

	for i := range delays1 {
		if delays1[i] != delays2[i] {
			t.Fatalf("jitter must be deterministic, attempt %d: %v != %v", i, delays1[i], delays2[i])
		}
	}
	distinct := make(map[time.Duration]bool)
	for _, d := range delays1 {
		distinct[d] = true
	}
	if len(distinct) < 2 {
		t.Fatalf("jitter must spread shards, got only %d distinct delays", len(distinct))
	}
}

func TestLeaseSchedulerSingleShardFailureIsolation(t *testing.T) {
	fixture, fault, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-sa")
	fixture.claimPlayerShard(1, adapterHolder, adapterBoot, "claim-sb")
	g0 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	g1 := ShardMembership{Key: adapterShardKey(1), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(g0); err != nil {
		t.Fatal(err)
	}
	if err := fixture.installMember(g1); err != nil {
		t.Fatal(err)
	}

	fault.SetRenewErr(errors.New("store down for shard renewals"))
	if err := registerEntry(t, scheduler, adapterShardKey(0), g0.Generation); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(1), g1.Generation); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	// Both stay admitted before deadline (transient), and no fatal was raised.
	if !fixture.guardAdmitted(0) || !fixture.guardAdmitted(1) {
		t.Fatal("transient failures must not fence healthy shards before deadline")
	}
	if scheduler.Fatal() {
		t.Fatal("transient failure must not raise the fatal flag")
	}
}

func TestLeaseSchedulerRaceAndCloseDuringRenew(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-sc")
	g0 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(g0); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 3 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = registerEntry(t, scheduler, adapterShardKey(0), g0.Generation)
				_ = scheduler.Cancel(adapterShardKey(0), g0.Generation)
				_ = scheduler.EntryCount()
				_ = scheduler.Fatal()
			}
		})
	}
	time.Sleep(50 * time.Millisecond)
	scheduler.Close() // cancels in-flight renewals and stops goroutines
	close(stop)
	wg.Wait()
	if scheduler.EntryCount() != 0 {
		t.Fatalf("post-close scheduler must have no entries, got %d", scheduler.EntryCount())
	}
}

// Reconciler integration: ADD registers an entry and REMOVE cancels it.
func TestLeaseSchedulerReconcilerRegistersOnInstallAndCancelsOnRemove(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fault := &faultRenewStore{ShardWriteAuthorityStore: fixture.store}
	scheduler, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store: fault, Registry: fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		LeaseTTL: 30 * time.Second, BaseDelay: 5 * time.Millisecond,
		MaxDelay: 200 * time.Millisecond, Jitter: 1 * time.Millisecond,
		RenewAttemptTimeout:  time.Second,
		RenewRecoveryTimeout: time.Second,
		Now:                  fixture.clock.Now, JitterSeed: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scheduler.Close)

	// Build a reconciler wired to the scheduler through the narrow seam.
	reconciler, err := NewShardMembershipReconciler(MembershipReconcilerOptions{
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		Installer: fixture.adapter, Fencer: fixture.adapter, Remover: fixture.adapter,
		AllowedDomains:         []actorrouting.RouteDomain{actorrouting.DomainPlayer},
		AuthorityLayoutVersion: adapterLayout,
		Scheduler:              scheduler,
		MaxRetries:             3, RetryDelay: time.Millisecond, Now: fixture.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reconciler.Close()

	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-sd")
	if result := reconciler.Observe(fixture.playerSnapshot(1, []int{1}, nil)); !result.FullyReconciled {
		t.Fatalf("install observe result=%+v", result)
	}
	if scheduler.EntryCount() != 1 {
		t.Fatalf("install must register an entry, got %d", scheduler.EntryCount())
	}
	assertMembershipRuntimeConsistent(t, reconciler, fixture.registry, scheduler)

	// Remove: the entry is cancelled when the shard leaves the holder.
	if result := reconciler.Observe(fixture.playerSnapshot(2, []int{0}, nil)); !result.FullyReconciled {
		t.Fatalf("remove observe result=%+v", result)
	}
	if scheduler.EntryCount() != 0 {
		t.Fatalf("remove must cancel the entry, got %d", scheduler.EntryCount())
	}
	assertMembershipRuntimeConsistent(t, reconciler, fixture.registry, scheduler)
}

// activateFailingScheduler wraps a real scheduler and can fail Activate to
// exercise the commit rollback path.
type activateFailingScheduler struct {
	inner        *PerShardLeaseScheduler
	failActivate bool
	beforeFail   func()
}

func (s *activateFailingScheduler) Reserve(key ShardKey, generation MembershipGeneration) (*LeaseRegistrationReservation, error) {
	return s.inner.Reserve(key, generation)
}
func (s *activateFailingScheduler) Activate(reservation *LeaseRegistrationReservation) error {
	if s.failActivate {
		if s.beforeFail != nil {
			s.beforeFail()
		}
		return errors.New("injected activate failure")
	}
	return s.inner.Activate(reservation)
}

type contextEnforcingMembershipPorts struct {
	inner *RealMembershipAdapter
}

func (p *contextEnforcingMembershipPorts) PrepareMembership(ctx context.Context, member ShardMembership) (*PreparedMembership, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return p.inner.PrepareMembership(ctx, member)
}

func (p *contextEnforcingMembershipPorts) CommitPreparedMembership(ctx context.Context, prepared PreparedMembership) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.inner.CommitPreparedMembership(ctx, prepared)
}

func (p *contextEnforcingMembershipPorts) FenceMembership(ctx context.Context, member ShardMembership) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.inner.FenceMembership(ctx, member)
}

func (p *contextEnforcingMembershipPorts) RemoveMembership(ctx context.Context, member ShardMembership) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return p.inner.RemoveMembership(ctx, member)
}
func (s *activateFailingScheduler) CancelReservation(reservation *LeaseRegistrationReservation) {
	s.inner.CancelReservation(reservation)
}
func (s *activateFailingScheduler) Cancel(key ShardKey, generation MembershipGeneration) error {
	return s.inner.Cancel(key, generation)
}

func TestSuccessfulRenewAfterLocalSafetyDeadlineReadmitsSameGeneration(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-f1a")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	// The local safety deadline (t0+25s) passes while the authority lease
	// (t0+30s) is still valid: admission is paused but recoverable.
	fixture.clock.Advance(26 * time.Second)
	if fixture.guardAdmitted(0) {
		t.Fatal("must be fail-closed after the local safety deadline")
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return fixture.guardAdmitted(0) })
	if scheduler.EntryState(adapterShardKey(0)) != LeaseEntryActive {
		t.Fatalf("entry must return to active after rearm, got %v", scheduler.EntryState(adapterShardKey(0)))
	}
}

func TestRenewAfterAuthoritativeLeaseExpiredCannotReadmit(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-f1b")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	// Both the local deadline and the authority lease have passed.
	fixture.clock.Advance(35 * time.Second)
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 0 })
	if fixture.guardAdmitted(0) {
		t.Fatal("an expired authority lease must never re-admit")
	}
}

func TestRemovedMembershipCannotBeReadmittedBySuccessfulLateRenew(t *testing.T) {
	fixture, fault, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-f1c")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	fault.SetRenewBlock(block)
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 1 })

	// Remove the guard and cancel the entry while a renewal is in flight.
	if err := fixture.adapter.RemoveMembership(context.Background(), member); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Cancel(adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	fault.SetRenewBlock(nil)
	close(block)
	time.Sleep(50 * time.Millisecond)

	// The late successful renewal must be discarded: the removed shard stays
	// uninstalled and the entry stays gone.
	if fixture.guardAdmitted(0) {
		t.Fatal("removed membership must not be re-admitted by a late renewal")
	}
	if scheduler.EntryCount() != 0 {
		t.Fatalf("entry must stay cancelled, got %d", scheduler.EntryCount())
	}
}

func TestLostGenerationCannotBeReadmitted(t *testing.T) {
	fixture, fault, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-f1d")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	fault.SetStatusFn(func(domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardAuthorityTransitionStatus, bool) {
		if shard == 0 {
			return authoritystore.ShardTransitionFenceMismatch, true
		}
		return 0, false
	})
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 0 })
	if fixture.guardAdmitted(0) {
		t.Fatal("lost generation must not be admitted")
	}
	// The entry is terminal: clearing the fault cannot revive it.
	fault.SetStatusFn(nil)
	time.Sleep(30 * time.Millisecond)
	if fixture.guardAdmitted(0) {
		t.Fatal("a terminal lost generation must stay fenced")
	}
	if scheduler.EntryCount() != 0 {
		t.Fatalf("lost generation entry must stay cancelled, got %d", scheduler.EntryCount())
	}
}

func TestSchedulerClosedBeforeReserveLeavesNoGuard(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	scheduler, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store: fixture.store, Registry: fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		LeaseTTL: 30 * time.Second, BaseDelay: 5 * time.Millisecond,
		MaxDelay: 200 * time.Millisecond, Jitter: 1 * time.Millisecond,
		RenewAttemptTimeout:  time.Second,
		RenewRecoveryTimeout: time.Second,
		Now:                  fixture.clock.Now, JitterSeed: 11,
	})
	if err != nil {
		t.Fatal(err)
	}
	scheduler.Close() // closed before any commit
	reconciler, err := NewShardMembershipReconciler(MembershipReconcilerOptions{
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		Installer: fixture.adapter, Fencer: fixture.adapter, Remover: fixture.adapter,
		AllowedDomains:         []actorrouting.RouteDomain{actorrouting.DomainPlayer},
		AuthorityLayoutVersion: adapterLayout,
		Scheduler:              scheduler,
		MaxRetries:             3, RetryDelay: time.Millisecond, Now: fixture.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reconciler.Close()
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-f2a")

	result := reconciler.Observe(fixture.playerSnapshot(1, []int{1}, nil))
	if result.FullyReconciled {
		t.Fatalf("closed scheduler must abort the commit: %+v", result)
	}
	if fixture.guardAdmitted(0) {
		t.Fatal("closed scheduler must abort before a guard is installed")
	}
	if len(reconciler.CurrentInstalledMembership()) != 0 {
		t.Fatal("nothing may be marked installed when the scheduler is closed")
	}
}

func TestSchedulerActivateFailureRollsBackGuard(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	inner, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store: fixture.store, Registry: fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		LeaseTTL: 30 * time.Second, BaseDelay: 5 * time.Millisecond,
		MaxDelay: 200 * time.Millisecond, Jitter: 1 * time.Millisecond,
		RenewAttemptTimeout:  time.Second,
		RenewRecoveryTimeout: time.Second,
		Now:                  fixture.clock.Now, JitterSeed: 13,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(inner.Close)
	wrapper := &activateFailingScheduler{inner: inner, failActivate: true}
	reconciler, err := NewShardMembershipReconciler(MembershipReconcilerOptions{
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		Installer: fixture.adapter, Fencer: fixture.adapter, Remover: fixture.adapter,
		AllowedDomains:         []actorrouting.RouteDomain{actorrouting.DomainPlayer},
		AuthorityLayoutVersion: adapterLayout,
		Scheduler:              wrapper,
		MaxRetries:             3, RetryDelay: time.Millisecond, Now: fixture.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reconciler.Close()
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-f2b")

	result := reconciler.Observe(fixture.playerSnapshot(1, []int{1}, nil))
	if result.FullyReconciled {
		t.Fatalf("activate failure must not reconcile: %+v", result)
	}
	if fixture.guardAdmitted(0) {
		t.Fatal("activate failure must roll back the installed guard")
	}
	if _, ok := fixture.registry.Snapshot(actorrouting.DomainPlayer, 0); ok {
		t.Fatal("activate failure must remove the guard from the registry")
	}
	if len(reconciler.CurrentInstalledMembership()) != 0 {
		t.Fatal("activate failure must not mark the shard installed")
	}
	if reconciler.LastFullyReconciledRevision() != 0 {
		t.Fatalf("activate failure must not advance the reconciled revision, got %d", reconciler.LastFullyReconciledRevision())
	}
	assertMembershipRuntimeConsistent(t, reconciler, fixture.registry, inner)
}

func TestSchedulerActivateFailureRollbackDoesNotUseExpiredActionContext(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	inner, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store: fixture.store, Registry: fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		LeaseTTL: 30 * time.Second, BaseDelay: 5 * time.Millisecond,
		MaxDelay: 200 * time.Millisecond, Jitter: time.Millisecond,
		RenewAttemptTimeout: time.Second, RenewRecoveryTimeout: time.Second,
		Now: fixture.clock.Now, JitterSeed: 29,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(inner.Close)
	actionCtx, cancelAction := context.WithCancel(context.Background())
	wrapper := &activateFailingScheduler{inner: inner, failActivate: true, beforeFail: cancelAction}
	ports := &contextEnforcingMembershipPorts{inner: fixture.adapter}
	reconciler, err := NewShardMembershipReconciler(MembershipReconcilerOptions{
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		Installer: ports, Fencer: ports, Remover: ports,
		AllowedDomains:         []actorrouting.RouteDomain{actorrouting.DomainPlayer},
		AuthorityLayoutVersion: adapterLayout, Scheduler: wrapper,
		MaxRetries: 1, RetryDelay: time.Hour, Now: fixture.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reconciler.Close()
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-expired-rollback")

	result := reconciler.ObserveContext(actionCtx, fixture.playerSnapshot(1, []int{1}, nil))
	if result.FullyReconciled || actionCtx.Err() == nil {
		t.Fatalf("activate failure result=%+v actionErr=%v", result, actionCtx.Err())
	}
	if _, ok := fixture.registry.Snapshot(actorrouting.DomainPlayer, 0); ok {
		t.Fatal("expired action context stranded the locally admitted guard during rollback")
	}
	if len(reconciler.CurrentInstalledMembership()) != 0 || inner.EntryCount() != 0 {
		t.Fatalf("expired rollback residue installed=%d entries=%d", len(reconciler.CurrentInstalledMembership()), inner.EntryCount())
	}
}

func TestSchedulerRegisterFailureDoesNotAdvanceReconciledRevision(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	scheduler, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store: fixture.store, Registry: fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		LeaseTTL: 30 * time.Second, BaseDelay: 5 * time.Millisecond,
		MaxDelay: 200 * time.Millisecond, Jitter: 1 * time.Millisecond,
		RenewAttemptTimeout:  time.Second,
		RenewRecoveryTimeout: time.Second,
		Now:                  fixture.clock.Now, JitterSeed: 17,
	})
	if err != nil {
		t.Fatal(err)
	}
	scheduler.Close() // register (reserve) will fail
	reconciler, err := NewShardMembershipReconciler(MembershipReconcilerOptions{
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		Installer: fixture.adapter, Fencer: fixture.adapter, Remover: fixture.adapter,
		AllowedDomains:         []actorrouting.RouteDomain{actorrouting.DomainPlayer},
		AuthorityLayoutVersion: adapterLayout,
		Scheduler:              scheduler,
		MaxRetries:             3, RetryDelay: time.Millisecond, Now: fixture.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reconciler.Close()
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-f2c")

	reconciler.Observe(fixture.playerSnapshot(1, []int{1}, nil))
	if reconciler.LastFullyReconciledRevision() != 0 {
		t.Fatalf("register failure must not advance the reconciled revision, got %d", reconciler.LastFullyReconciledRevision())
	}
}

func TestCancelledReservationCannotActivate(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-f5a")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	reservation, err := scheduler.Reserve(adapterShardKey(0), member.Generation)
	if err != nil {
		t.Fatal(err)
	}
	scheduler.CancelReservation(reservation)
	if err := scheduler.Activate(reservation); err == nil {
		t.Fatal("a cancelled reservation must not activate")
	}
	if scheduler.EntryCount() != 0 {
		t.Fatalf("no entry may be created from a cancelled reservation, got %d", scheduler.EntryCount())
	}
}

func TestOldRegisterCannotRecreateEntryAfterRemove(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-f5b")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	reservation, err := scheduler.Reserve(adapterShardKey(0), member.Generation)
	if err != nil {
		t.Fatal(err)
	}
	// The shard is removed while the reservation is un-activated.
	if err := fixture.adapter.RemoveMembership(context.Background(), member); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Cancel(adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	// The stale registration must not leave a durable entry: it starts and
	// immediately dies because no guard exists for the generation.
	if err := scheduler.Activate(reservation); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 0 })
	if fixture.guardAdmitted(0) {
		t.Fatal("a removed membership must never be re-admitted")
	}
}

func TestOldRegisterCannotRecreateEntryAfterReplace(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-f5c")
	gen1 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	gen2 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(2, 2, 2)}
	if err := fixture.installMember(gen1); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen1.Generation); err != nil {
		t.Fatal(err)
	}
	// Replace: cancel the old generation entry, then activate the new one.
	if err := scheduler.Cancel(adapterShardKey(0), gen1.Generation); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.SetShardWriteAuthority(adapterAuthorityFor(2, 2, 2, fixture.clock)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.installMember(gen2); err != nil {
		t.Fatal(err)
	}
	reservation2, err := scheduler.Reserve(adapterShardKey(0), gen2.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Activate(reservation2); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 1 })
	if scheduler.EntryState(adapterShardKey(0)) == LeaseEntryCancelled {
		t.Fatal("replacement entry must be active")
	}
}

func timeoutSchedulerFixture(t *testing.T, fixture *membershipAdapterFixture, fault *faultRenewStore, onFatal func(error)) *PerShardLeaseScheduler {
	t.Helper()
	scheduler, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store: fault, Registry: fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		LeaseTTL: 30 * time.Second, BaseDelay: 5 * time.Millisecond,
		MaxDelay: 200 * time.Millisecond, Jitter: 1 * time.Millisecond,
		RenewAttemptTimeout:  30 * time.Millisecond,
		RenewRecoveryTimeout: 30 * time.Millisecond,
		Now:                  fixture.clock.Now, JitterSeed: 23,
		OnFatal: onFatal,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scheduler.Close)
	return scheduler
}

func TestLeaseSchedulerRenewAttemptTimeout(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fault := &faultRenewStore{ShardWriteAuthorityStore: fixture.store}
	scheduler := timeoutSchedulerFixture(t, fixture, fault, nil)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-t1")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	fault.SetRenewBlock(block) // never released
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond) // several bounded timeouts
	// The entry keeps running (bounded retries) and the shard stays admitted
	// before its local deadline.
	if scheduler.EntryCount() != 1 {
		t.Fatalf("entry must survive bounded timeouts, got %d", scheduler.EntryCount())
	}
	if !fixture.guardAdmitted(0) {
		t.Fatal("blocked renewals must not fence the shard before its deadline")
	}
}

func TestRenewAttemptTimeoutDoesNotBlockOtherShards(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fault := &faultRenewStore{ShardWriteAuthorityStore: fixture.store}
	scheduler := timeoutSchedulerFixture(t, fixture, fault, nil)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-t2a")
	fixture.claimPlayerShard(1, adapterHolder, adapterBoot, "claim-t2b")
	g0 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	g1 := ShardMembership{Key: adapterShardKey(1), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(g0); err != nil {
		t.Fatal(err)
	}
	if err := fixture.installMember(g1); err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	fault.SetRenewBlockFn(func(domain actorrouting.RouteDomain, shard uint32) <-chan struct{} {
		if shard == 0 {
			return block
		}
		return nil
	})
	if err := registerEntry(t, scheduler, adapterShardKey(0), g0.Generation); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(1), g1.Generation); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	// Shard 1 renews normally (transparent store) and stays admitted; shard 0
	// is not hung and its entry keeps running.
	if !fixture.guardAdmitted(1) {
		t.Fatal("a blocked shard must not stall another shard's renewals")
	}
	if scheduler.EntryCount() != 2 {
		t.Fatalf("both entries must survive, got %d", scheduler.EntryCount())
	}
}

func TestRenewAttemptTimeoutAllowsClose(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fault := &faultRenewStore{ShardWriteAuthorityStore: fixture.store}
	scheduler := timeoutSchedulerFixture(t, fixture, fault, nil)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-t3")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	fault.SetRenewBlock(block)
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	done := make(chan struct{})
	go func() {
		scheduler.Close() // must not hang even with a blocked store
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung behind a blocked renewal")
	}
	if scheduler.EntryCount() != 0 {
		t.Fatalf("post-close entries=%d, want 0", scheduler.EntryCount())
	}
}

func TestLeaseSchedulerEntryPanicFencesShard(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fault := &faultRenewStore{ShardWriteAuthorityStore: fixture.store}
	scheduler := timeoutSchedulerFixture(t, fixture, fault, nil)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-p1")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	fault.SetRenewPanic(errors.New("boom"))
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 0 })
	if fixture.guardAdmitted(0) {
		t.Fatal("an entry panic must fence the target shard")
	}
	if !scheduler.Fatal() {
		t.Fatal("an entry panic must raise the fatal flag")
	}
}

func TestLeaseSchedulerEntryPanicCallsOnFatal(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fault := &faultRenewStore{ShardWriteAuthorityStore: fixture.store}
	fatalCh := make(chan error, 1)
	scheduler := timeoutSchedulerFixture(t, fixture, fault, func(err error) { fatalCh <- err })
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-p2")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	fault.SetRenewPanic(errors.New("boom"))
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return scheduler.Fatal() })
	select {
	case err := <-fatalCh:
		if err == nil {
			t.Fatal("OnFatal must receive the panic error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnFatal was not invoked after an entry panic")
	}
}

func TestLeaseSchedulerEntryPanicLeavesNoActiveEntry(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fault := &faultRenewStore{ShardWriteAuthorityStore: fixture.store}
	fatalCh := make(chan error, 1)
	scheduler := timeoutSchedulerFixture(t, fixture, fault, func(err error) { fatalCh <- err })
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-p3")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	fault.SetRenewPanic(errors.New("boom"))
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	select {
	case <-fatalCh:
		// notifyFatal stores fatal only after the entry was removed and its
		// guard fenced, so this callback event is the happens-before barrier.
	case <-time.After(2 * time.Second):
		t.Fatal("OnFatal was not invoked after an entry panic")
	}
	if scheduler.EntryCount() != 0 {
		t.Fatalf("entry must be cancelled after the contained panic, got %d", scheduler.EntryCount())
	}
	if scheduler.Fatal() == false {
		t.Fatal("panic must be observed as fatal")
	}
	// No entry goroutine survives the panic; the scheduler closes cleanly.
	scheduler.Close()
}

// onFatalPanicScheduler builds a scheduler whose OnFatal callback panics,
// installs one shard, injects a renewal panic and waits for the entry to be
// cleaned up, exercising the contained-callback-panic path.
func onFatalPanicScheduler(t *testing.T) (*membershipAdapterFixture, *faultRenewStore, *PerShardLeaseScheduler) {
	t.Helper()
	fixture := newMembershipAdapterFixture(t)
	fault := &faultRenewStore{ShardWriteAuthorityStore: fixture.store}
	fatalCh := make(chan error, 1)
	scheduler := timeoutSchedulerFixture(t, fixture, fault, func(err error) {
		fatalCh <- err
		panic("injected onFatal panic")
	})
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-oc")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	fault.SetRenewPanic(errors.New("boom"))
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	select {
	case <-fatalCh:
		// handleEntryPanic removes the entry and fences the guard before
		// notifyFatal stores fatal and invokes this callback.  Receiving this
		// event therefore makes all three state changes visible to the test.
	case <-time.After(2 * time.Second):
		t.Fatal("OnFatal was not invoked after an entry panic")
	}
	if scheduler.EntryCount() != 0 {
		t.Fatalf("entry must be cancelled after the contained panic, got %d", scheduler.EntryCount())
	}
	waitLeaseCondition(t, func() bool {
		err := scheduler.FatalErr()
		return err != nil && strings.Contains(err.Error(), "OnFatal callback panicked")
	})
	return fixture, fault, scheduler
}

func TestOnFatalPanicIsContained(t *testing.T) {
	fixture, fault, scheduler := onFatalPanicScheduler(t)
	// The entry panic and the callback panic must both be contained: the
	// process keeps running, the shard is fenced, the entry is cancelled and
	// the fatal state (including the callback-panic diagnostic) is recorded.
	if !scheduler.Fatal() {
		t.Fatal("fatal flag must be set after an entry panic")
	}
	if err := scheduler.FatalErr(); err == nil || !strings.Contains(err.Error(), "OnFatal callback panicked") {
		t.Fatalf("contained callback panic diagnostic must be recorded: %v", err)
	}
	if fixture.guardAdmitted(0) {
		t.Fatal("target shard must be fenced after an entry panic")
	}
	// A separate shard keeps renewing normally after the contained panic.
	fault.SetRenewPanic(nil)
	fixture.claimPlayerShard(1, adapterHolder, adapterBoot, "claim-oc-other")
	other := ShardMembership{Key: adapterShardKey(1), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(other); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(1), other.Generation); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 1 })
	if !fixture.guardAdmitted(1) {
		t.Fatal("other shard must stay admitted")
	}
}

func TestOnFatalPanicDoesNotLeaveShardAdmitted(t *testing.T) {
	fixture, _, _ := onFatalPanicScheduler(t)
	if fixture.guardAdmitted(0) {
		t.Fatal("an onFatal panic must not leave the target shard admitted")
	}
}

func TestOnFatalPanicDoesNotLeaveActiveEntry(t *testing.T) {
	_, _, scheduler := onFatalPanicScheduler(t)
	if scheduler.EntryCount() != 0 {
		t.Fatalf("an onFatal panic must not leave an active entry, got %d", scheduler.EntryCount())
	}
}

func TestPanicDuringPostRenewValidationDoesNotDeadlock(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	// The renewal RPC succeeds; the panic is injected exactly at the
	// post-renew revalidation point, which runs outside s.mu, so the entry's
	// recover handler can re-lock s.mu without deadlocking.
	scheduler.mu.Lock()
	scheduler.postRenewHook = func() { panic("post-renew validation boom") }
	scheduler.mu.Unlock()
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-pr")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	// Barrier: the panic is contained, the entry is cleaned up and the shard
	// is fenced; Close then completes within the deadline.
	waitLeaseCondition(t, func() bool { return scheduler.Fatal() && scheduler.EntryCount() == 0 })
	if !scheduler.Fatal() {
		t.Fatal("a post-renew panic must be observed as fatal")
	}
	if fixture.guardAdmitted(0) {
		t.Fatal("a post-renew panic must fence the target shard")
	}
	done := make(chan struct{})
	go func() {
		scheduler.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung after a post-renew panic")
	}
}

func TestEntryPanicCleanupDoesNotHoldSchedulerLock(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fault := &faultRenewStore{ShardWriteAuthorityStore: fixture.store}
	fatalDone := make(chan struct{})
	var scheduler *PerShardLeaseScheduler
	scheduler = timeoutSchedulerFixture(t, fixture, fault, func(error) {
		// Re-enter the scheduler from the callback: if handleEntryPanic still
		// held scheduler.mu, EntryCount would block forever and the select
		// below would time out, proving the cleanup path released the lock.
		_ = scheduler.EntryCount()
		close(fatalDone)
	})
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-ol")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	fault.SetRenewPanic(errors.New("boom"))
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	select {
	case <-fatalDone:
	case <-time.After(2 * time.Second):
		t.Fatal("OnFatal ran while scheduler.mu was held (deadlock risk)")
	}
	if scheduler.EntryCount() != 0 {
		t.Fatalf("entry must be cancelled after the contained panic, got %d", scheduler.EntryCount())
	}
	if fixture.guardAdmitted(0) {
		t.Fatal("target shard must be fenced")
	}
}

// assertMembershipRuntimeConsistent asserts that the reconciler's installed
// memberships, the registry's ADMITTED guards and the scheduler's active
// entries agree on the exact (shard key, generation) set:
//
//   - every admitted guard has an active entry and vice versa (no
//     admitted-without-entry or entry-without-admitted-guard);
//   - every active entry is covered by a reconciler-installed membership.
//
// A terminal-fenced guard (lost generation / entry panic / removed) is not
// admitted and has no entry, so it may remain in the reconciler's installed
// set only as a stale record without any live runtime state; this mirrors the
// production invariant that a fenced shard can never serve.
func assertMembershipRuntimeConsistent(t *testing.T, reconciler *ShardMembershipReconciler, registry *routeauthority.LeaseGuardRegistry, scheduler *PerShardLeaseScheduler) {
	t.Helper()
	installed := map[ShardKey]MembershipGeneration{}
	for _, m := range reconciler.CurrentInstalledMembership() {
		installed[m.Key] = m.Generation
	}
	guards := map[ShardKey]MembershipGeneration{}
	for _, snap := range registry.SnapshotAll() {
		if snap.Authority.AssignmentRevision == 0 || !snap.Admitted {
			continue
		}
		guards[ShardKey{Domain: snap.Authority.Domain, Shard: snap.Authority.VirtualShard}] = MembershipGeneration{
			HolderID: snap.Authority.HolderID, HolderBootID: snap.Authority.HolderBootID,
			AssignmentRevision: snap.Authority.AssignmentRevision,
			WriterEpoch:        snap.Authority.WriterEpoch, LeaseEpoch: snap.Authority.LeaseEpoch,
			LeaseUntilMs: snap.Authority.LeaseUntilMs,
		}
	}
	entries := scheduler.ActiveEntries()
	if len(guards) != len(entries) {
		t.Fatalf("admitted guards %v disagree with active entries %v", guards, entries)
	}
	for key, gen := range guards {
		if entryGen, ok := entries[key]; !ok || entryGen != gen {
			t.Fatalf("admitted guard %v=%+v but scheduler entry=%+v", key, gen, entryGen)
		}
	}
	for key, gen := range entries {
		if instGen, ok := installed[key]; !ok || instGen != gen {
			t.Fatalf("active entry %v=%+v but reconciler installed=%+v", key, gen, instGen)
		}
	}
}

func TestMembershipRuntimeConsistencyAcrossLifecycle(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fault := &faultRenewStore{ShardWriteAuthorityStore: fixture.store}
	scheduler, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store: fault, Registry: fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		LeaseTTL: 30 * time.Second, BaseDelay: 5 * time.Millisecond,
		MaxDelay: 200 * time.Millisecond, Jitter: 1 * time.Millisecond,
		RenewAttemptTimeout: time.Second, RenewRecoveryTimeout: time.Second,
		Now: fixture.clock.Now, JitterSeed: 31,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scheduler.Close)
	reconciler, err := NewShardMembershipReconciler(MembershipReconcilerOptions{
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		Installer: fixture.adapter, Fencer: fixture.adapter, Remover: fixture.adapter,
		AllowedDomains:         []actorrouting.RouteDomain{actorrouting.DomainPlayer},
		AuthorityLayoutVersion: adapterLayout,
		Scheduler:              scheduler,
		MaxRetries:             3, RetryDelay: time.Millisecond, Now: fixture.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reconciler.Close()

	// Install: reconciler, registry and scheduler must all agree on one shard.
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-cons-1")
	if result := reconciler.Observe(fixture.playerSnapshot(1, []int{1}, nil)); !result.FullyReconciled {
		t.Fatalf("install observe result=%+v", result)
	}
	assertMembershipRuntimeConsistent(t, reconciler, fixture.registry, scheduler)

	// Remove: all three must drain to empty together.
	if result := reconciler.Observe(fixture.playerSnapshot(2, []int{0}, nil)); !result.FullyReconciled {
		t.Fatalf("remove observe result=%+v", result)
	}
	assertMembershipRuntimeConsistent(t, reconciler, fixture.registry, scheduler)
}

// lostGenerationScheduler builds a reconciler-driven scheduler for shard 0 and
// drives one renewal to a lost generation (FenceMismatch).
func lostGenerationScheduler(t *testing.T, onFatal func(error)) (*membershipAdapterFixture, *faultRenewStore, *ShardMembershipReconciler, *PerShardLeaseScheduler) {
	t.Helper()
	fixture := newMembershipAdapterFixture(t)
	fault := &faultRenewStore{ShardWriteAuthorityStore: fixture.store}
	scheduler, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store: fault, Registry: fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		LeaseTTL: 30 * time.Second, BaseDelay: 5 * time.Millisecond,
		MaxDelay: 200 * time.Millisecond, Jitter: 1 * time.Millisecond,
		RenewAttemptTimeout: time.Second, RenewRecoveryTimeout: time.Second,
		Now: fixture.clock.Now, JitterSeed: 41,
		OnFatal: onFatal,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scheduler.Close)
	reconciler, err := NewShardMembershipReconciler(MembershipReconcilerOptions{
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		Installer: fixture.adapter, Fencer: fixture.adapter, Remover: fixture.adapter,
		AllowedDomains:         []actorrouting.RouteDomain{actorrouting.DomainPlayer},
		AuthorityLayoutVersion: adapterLayout,
		Scheduler:              scheduler,
		MaxRetries:             3, RetryDelay: time.Millisecond, Now: fixture.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reconciler.Close)
	return fixture, fault, reconciler, scheduler
}

func TestLostGenerationWithSuccessfulFenceDoesNotSetSchedulerFatal(t *testing.T) {
	fixture, fault, reconciler, scheduler := lostGenerationScheduler(t, nil)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-lg1")
	if result := reconciler.Observe(fixture.playerSnapshot(1, []int{1}, nil)); !result.FullyReconciled {
		t.Fatalf("install observe result=%+v", result)
	}
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 1 })

	// The repository proves the generation moved away: the local fence
	// succeeds, the entry stops, and this is a normal control-plane event that
	// must not mark the scheduler fatal.
	fault.SetStatusFn(func(domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardAuthorityTransitionStatus, bool) {
		if shard == 0 {
			return authoritystore.ShardTransitionFenceMismatch, true
		}
		return 0, false
	})
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 0 })
	waitLeaseCondition(t, func() bool { return !fixture.guardAdmitted(0) })

	if scheduler.Fatal() {
		t.Fatal("a normal lost generation with a successful local fence must not raise Scheduler.Fatal")
	}
	if scheduler.FatalErr() != nil {
		t.Fatalf("a normal lost generation must not record a fatal error: %v", scheduler.FatalErr())
	}
	assertMembershipRuntimeConsistent(t, reconciler, fixture.registry, scheduler)
}

func TestLostGenerationWithSuccessfulFenceDoesNotCallOnFatal(t *testing.T) {
	onFatalCh := make(chan error, 1)
	fixture, fault, reconciler, scheduler := lostGenerationScheduler(t, func(err error) { onFatalCh <- err })
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-lg2")
	if result := reconciler.Observe(fixture.playerSnapshot(1, []int{1}, nil)); !result.FullyReconciled {
		t.Fatalf("install observe result=%+v", result)
	}
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 1 })
	fault.SetStatusFn(func(domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardAuthorityTransitionStatus, bool) {
		if shard == 0 {
			return authoritystore.ShardTransitionFenceMismatch, true
		}
		return 0, false
	})
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 0 })
	select {
	case err := <-onFatalCh:
		t.Fatalf("OnFatal must not fire for a normal lost generation: %v", err)
	default:
	}
}

func TestLostGenerationFenceFailureCallsOnFatal(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	var callbackCount atomic.Int32
	callbackErrCh := make(chan error, 1)
	scheduler.onFatal = func(err error) {
		callbackCount.Add(1)
		select {
		case callbackErrCh <- err:
		default:
			// A second invocation (should never happen) must not block the
			// scheduler goroutine; the count assertion below reports it.
		}
	}
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-lg3")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 1 })
	// Simulate an un-fencable lost generation: the renewal runtime has no
	// handle, so PrepareRenew reports LostGeneration without fencing the
	// still-admitted local guard.  Only this invariant violation (the exact
	// generation's guard is still admitted) raises OnFatal.
	scheduler.mu.Lock()
	if e, ok := scheduler.entries[adapterShardKey(0)]; ok {
		e.runtime = &routeauthority.HolderLeaseRuntime{}
	}
	scheduler.mu.Unlock()
	// Wait for the fatal signal AND the callback delivery (the callback runs
	// after the fatal flag is set, so both must be observed before reading).
	waitLeaseCondition(t, func() bool { return scheduler.Fatal() && callbackCount.Load() == 1 })
	if got := callbackCount.Load(); got != 1 {
		t.Fatalf("OnFatal count = %d, want exactly 1", got)
	}
	select {
	case callbackErr := <-callbackErrCh:
		// Both the OnFatal callback error and Scheduler.FatalErr() must carry
		// the full shard key and the full membership generation built from
		// THIS test's real generation (not a placeholder or fixed string).
		key := member.Key
		gen := member.Generation
		wantKey := fmt.Sprintf("key=%v", key)
		wantGeneration := fmt.Sprintf("generation=%+v", gen)
		for name, err := range map[string]error{
			"callback": callbackErr,
			"fatalErr": scheduler.FatalErr(),
		} {
			if err == nil {
				t.Fatalf("%s error is nil", name)
			}
			if !strings.Contains(err.Error(), wantKey) {
				t.Fatalf("%s missing shard key %q: %v", name, wantKey, err)
			}
			if !strings.Contains(err.Error(), wantGeneration) {
				t.Fatalf("%s missing full generation %q: %v", name, wantGeneration, err)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("OnFatal was not invoked when the lost generation could not be fenced")
	}
	if scheduler.EntryCount() != 0 {
		t.Fatalf("entry must be cancelled after the fence failure, got %d", scheduler.EntryCount())
	}
	if !fixture.guardAdmitted(0) {
		t.Fatal("the injected handle-less runtime must leave the guard admitted to prove the fence failure path")
	}
}

func TestOnFatalPanicPreservesPrimaryFatalCause(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	fault := &faultRenewStore{ShardWriteAuthorityStore: fixture.store}
	scheduler := timeoutSchedulerFixture(t, fixture, fault, func(error) { panic("injected onFatal panic") })
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-pp")
	member := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(member); err != nil {
		t.Fatal(err)
	}
	fault.SetRenewPanic(errors.New("boom"))
	if err := registerEntry(t, scheduler, adapterShardKey(0), member.Generation); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool {
		err := scheduler.FatalErr()
		return err != nil && strings.Contains(err.Error(), "OnFatal callback panicked")
	})
	err := scheduler.FatalErr()
	if err == nil {
		t.Fatal("fatal error must be recorded")
	}
	if !strings.Contains(err.Error(), "lease entry panic") {
		t.Fatalf("primary fatal cause was overwritten by the callback panic: %v", err)
	}
	if !strings.Contains(err.Error(), "OnFatal callback panicked") {
		t.Fatalf("secondary callback panic diagnostic missing: %v", err)
	}
}

func TestPlannedGenerationReplacementDoesNotLowerProcessReady(t *testing.T) {
	// Planned migration: shard 0 moves to another Pod.  This holder's next
	// renewal sees LostGeneration, fences shard 0 precisely, and must not
	// emit the scheduler fatal signal (the signal RT1B-3C-A will map to
	// Runtime.ProcessReady=false), while shard 1 keeps serving.  The real
	// Runtime.ProcessReady()/readiness probe is verified in RT1B-3C-A after
	// composition wiring; this test only proves the scheduler does not emit
	// the fatal signal that ProcessReady would be driven by.
	onFatalCh := make(chan error, 1)
	fixture, fault, reconciler, scheduler := lostGenerationScheduler(t, func(err error) { onFatalCh <- err })
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-mig0")
	fixture.claimPlayerShard(1, adapterHolder, adapterBoot, "claim-mig1")
	if result := reconciler.Observe(fixture.playerSnapshot(1, []int{1, 1}, nil)); !result.FullyReconciled {
		t.Fatalf("install observe result=%+v", result)
	}
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 2 })

	fault.SetStatusFn(func(domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardAuthorityTransitionStatus, bool) {
		if shard == 0 {
			return authoritystore.ShardTransitionFenceMismatch, true
		}
		return 0, false
	})
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 1 })
	waitLeaseCondition(t, func() bool { return !fixture.guardAdmitted(0) })
	if !fixture.guardAdmitted(1) {
		t.Fatal("shard 1 must keep serving through a planned migration of shard 0")
	}
	if scheduler.Fatal() {
		t.Fatal("a planned generation replacement must not emit the fatal signal (Scheduler.Fatal must stay false)")
	}
	select {
	case err := <-onFatalCh:
		t.Fatalf("OnFatal must not fire for a planned migration: %v", err)
	default:
	}
	assertMembershipRuntimeConsistent(t, reconciler, fixture.registry, scheduler)
}

// TestStaleLostGenerationResponseAfterReplaceDoesNotCancelNewEntry is the
// deterministic race test for the stale late-response hazard: G1's renewal is
// in flight when the snapshot replaces G1 with G2; when G1's renewal finally
// returns LostGeneration it must only terminate G1 -- never cancel G2's entry,
// fence G2's guard, mutate G2's token, or raise Scheduler.Fatal/OnFatal.
func TestStaleLostGenerationResponseAfterReplaceDoesNotCancelNewEntry(t *testing.T) {
	fixture, fault, scheduler := newSchedulerFixture(t)
	onFatalCh := make(chan error, 1)
	scheduler.onFatal = func(err error) { onFatalCh <- err }
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-stale")
	gen1 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(gen1); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen1.Generation); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 1 })

	// Block renewals and wait until G1's renewal is in flight in the store.
	block := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	fault.SetRenewBlock(block)
	fault.SetRenewEntered(func() { once.Do(func() { close(entered) }) })
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("G1 renewal did not enter the store")
	}
	fault.SetRenewEntered(nil)

	// G2 replaces G1 while G1's renewal is in flight: G2's guard and scheduler
	// entry are both installed and its own renewals begin.
	if err := fixture.store.SetShardWriteAuthority(adapterAuthorityFor(2, 2, 2, fixture.clock)); err != nil {
		t.Fatal(err)
	}
	gen2 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(2, 2, 2)}
	if err := fixture.installMember(gen2); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen2.Generation); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 1 })

	// Capture G2's entry identity (token) before releasing the stale G1.
	scheduler.mu.Lock()
	g2, ok := scheduler.entries[adapterShardKey(0)]
	var g2Token uint64
	if ok {
		g2Token = g2.token
	}
	scheduler.mu.Unlock()
	if !ok || g2Token == 0 {
		t.Fatal("G2 entry missing before release")
	}

	// Release: G1's late renewal returns LostGeneration (fence mismatch
	// against the installed G2 authority) while G2's own renewal succeeds.
	// Deterministic barrier (not a fixed sleep): wait until the stale G1
	// response has been fully processed before asserting the invariants, so a
	// buggy cancel-by-key path cannot slip through because the assertion ran
	// before the stale response was handled.
	lostGen := make(chan struct{})
	var lostGenOnce sync.Once
	scheduler.mu.Lock()
	scheduler.lostGenHook = func(key ShardKey, gen MembershipGeneration) { lostGenOnce.Do(func() { close(lostGen) }) }
	scheduler.mu.Unlock()
	fault.SetRenewBlock(nil)
	close(block)
	select {
	case <-lostGen:
	case <-time.After(2 * time.Second):
		t.Fatal("stale G1 LostGeneration response was not processed")
	}
	// After the stale response is processed, G2's guard and entry must be
	// intact and no fatal signal may have been raised.
	waitLeaseCondition(t, func() bool {
		return scheduler.EntryCount() == 1 && fixture.guardAdmitted(0) && !scheduler.Fatal()
	})

	if scheduler.EntryCount() != 1 {
		t.Fatalf("stale G1 response cancelled G2's entry, entries=%d", scheduler.EntryCount())
	}
	scheduler.mu.Lock()
	cur, ok := scheduler.entries[adapterShardKey(0)]
	scheduler.mu.Unlock()
	if !ok || cur.token != g2Token || !cur.generation.equal(gen2.Generation) {
		t.Fatalf("G2 entry was replaced or cancelled by the stale G1 response: %+v", cur)
	}
	snapshot, ok := fixture.registry.Snapshot(actorrouting.DomainPlayer, 0)
	if !ok || !snapshot.Admitted || snapshot.Authority.WriterEpoch != 2 {
		t.Fatalf("G2 guard must remain admitted at generation 2: %+v", snapshot)
	}
	if scheduler.Fatal() {
		t.Fatal("a stale lost-generation response must not raise Scheduler.Fatal")
	}
	select {
	case err := <-onFatalCh:
		t.Fatalf("OnFatal must not fire for a stale lost-generation response: %v", err)
	default:
	}
}

// TestStaleG1AndG2ResponsesCannotCancelG3 proves the generation fence does not
// rely on a two-generation assumption: stale LostGeneration responses from
// both G1 and G2 landing after a G1->G2->G3 replace chain must only terminate
// their own generations and leave G3's entry, guard, token and fatal state
// untouched.
func TestStaleG1AndG2ResponsesCannotCancelG3(t *testing.T) {
	fixture, fault, scheduler := newSchedulerFixture(t)
	onFatalCh := make(chan error, 1)
	scheduler.onFatal = func(err error) { onFatalCh <- err }
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-g3")
	g1 := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(1, 1, 1)}
	if err := fixture.installMember(g1); err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	fault.SetRenewBlock(block)
	fault.SetRenewEntered(func() { once.Do(func() { close(entered) }) })
	if err := registerEntry(t, scheduler, adapterShardKey(0), g1.Generation); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("G1 renewal did not enter the store")
	}
	fault.SetRenewEntered(nil)

	// Two consecutive replaces while the older renewals are in flight.  Each
	// generation's renewal is confirmed to enter the store before the next
	// replace, so both stale responses are genuinely late.
	for rev := uint64(2); rev <= 3; rev++ {
		if err := fixture.store.SetShardWriteAuthority(adapterAuthorityFor(rev, rev, rev, fixture.clock)); err != nil {
			t.Fatal(err)
		}
		gen := ShardMembership{Key: adapterShardKey(0), Generation: adapterGeneration(rev, rev, rev)}
		if err := fixture.installMember(gen); err != nil {
			t.Fatal(err)
		}
		enteredN := make(chan struct{})
		var onceN sync.Once
		fault.SetRenewEntered(func() { onceN.Do(func() { close(enteredN) }) })
		if err := registerEntry(t, scheduler, adapterShardKey(0), gen.Generation); err != nil {
			t.Fatal(err)
		}
		select {
		case <-enteredN:
		case <-time.After(2 * time.Second):
			t.Fatalf("generation %d renewal did not enter the store", rev)
		}
		fault.SetRenewEntered(nil)
		waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 1 })
	}

	scheduler.mu.Lock()
	g3, ok := scheduler.entries[adapterShardKey(0)]
	var g3Token uint64
	if ok {
		g3Token = g3.token
	}
	scheduler.mu.Unlock()
	if !ok || g3Token == 0 || !g3.generation.equal(adapterGeneration(3, 3, 3)) {
		t.Fatalf("G3 entry missing before release: %+v", g3)
	}

	lostGen := make(chan struct{}, 2)
	scheduler.mu.Lock()
	scheduler.lostGenHook = func(key ShardKey, gen MembershipGeneration) { lostGen <- struct{}{} }
	scheduler.mu.Unlock()
	fault.SetRenewBlock(nil)
	close(block)
	for range 2 {
		select {
		case <-lostGen:
		case <-time.After(2 * time.Second):
			t.Fatal("stale LostGeneration response was not processed")
		}
	}
	waitLeaseCondition(t, func() bool {
		return scheduler.EntryCount() == 1 && fixture.guardAdmitted(0) && !scheduler.Fatal()
	})
	if scheduler.EntryCount() != 1 {
		t.Fatalf("stale G1/G2 responses cancelled G3's entry, entries=%d", scheduler.EntryCount())
	}
	scheduler.mu.Lock()
	cur, ok := scheduler.entries[adapterShardKey(0)]
	scheduler.mu.Unlock()
	if !ok || cur.token != g3Token || !cur.generation.equal(adapterGeneration(3, 3, 3)) {
		t.Fatalf("G3 entry was replaced or cancelled by stale G1/G2 responses: %+v", cur)
	}
	snapshot, ok := fixture.registry.Snapshot(actorrouting.DomainPlayer, 0)
	if !ok || !snapshot.Admitted || snapshot.Authority.WriterEpoch != 3 {
		t.Fatalf("G3 guard must remain admitted at generation 3: %+v", snapshot)
	}
	if scheduler.Fatal() {
		t.Fatal("stale G1/G2 responses must not raise Scheduler.Fatal")
	}
	select {
	case err := <-onFatalCh:
		t.Fatalf("OnFatal must not fire for stale G1/G2 responses: %v", err)
	default:
	}
}
