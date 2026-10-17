package authoritycomposition

// RT1B-3D lease recovery budget hardening tests: the recovery path uses its
// own cadence (never the ordinary transient backoff), entering the local fence
// resets the backoff, and the frozen startup invariant
// 2*RecoveryAttemptTimeout+RecoveryMaxDelay+RecoveryJitterMax+Slack <
// SafetyMargin is enforced fail-closed against the final effective cadence.

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	routeauthority "farmgame/internal/logic/actor/authority"
	"farmgame/internal/storage/authoritystore"
)

// TestUnsafeRecoveryBudgetRejectedAtStartup freezes the fail-closed startup
// invariant: an explicit recovery budget that cannot complete inside the
// safety margin must fail scheduler construction.
func TestUnsafeRecoveryBudgetRejectedAtStartup(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	scheduler, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store: fixture.store, Registry: fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		LeaseTTL: 30 * time.Second, SafetyMargin: 5 * time.Second,
		BaseDelay: 5 * time.Millisecond, MaxDelay: 200 * time.Millisecond,
		RenewAttemptTimeout: time.Second, RenewRecoveryTimeout: 2 * time.Second,
		RecoveryMaxDelay: 4 * time.Second, // budget 4s+2s >= 5s margin
		Now:              fixture.clock.Now, JitterSeed: 42,
	})
	if err == nil {
		if scheduler != nil {
			scheduler.Close()
		}
		t.Fatal("unsafe recovery budget must fail startup")
	}
}

// TestSafeRecoveryBudgetAcceptedAtStartup: a budget inside the margin starts.
func TestSafeRecoveryBudgetAcceptedAtStartup(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	scheduler, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store: fixture.store, Registry: fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		LeaseTTL: 30 * time.Second, SafetyMargin: 5 * time.Second,
		BaseDelay: 5 * time.Millisecond, MaxDelay: 200 * time.Millisecond,
		RenewAttemptTimeout: time.Second, RenewRecoveryTimeout: time.Second,
		RecoveryMaxDelay: time.Second, // budget 1s+1s+0.25s jitter < 5s
		Now:              fixture.clock.Now, JitterSeed: 42,
	})
	if err != nil {
		t.Fatalf("safe recovery budget must start: %v", err)
	}
	scheduler.Close()
}

// TestLocalFenceTransitionResetsOrdinaryBackoff: after ordinary transient
// backoff has grown, the first observation of the local fence resets it so the
// recovery cadence starts fresh.
func TestLocalFenceTransitionResetsOrdinaryBackoff(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-rb1")
	gen := adapterGeneration(1, 1, 1)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen}); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen); err != nil {
		t.Fatal(err)
	}

	scheduler.mu.Lock()
	e := scheduler.entries[adapterShardKey(0)]
	if e == nil {
		scheduler.mu.Unlock()
		t.Fatal("entry missing")
	}
	e.backoff = 10 // ordinary transient backoff has grown (10 -> 5s<<10 capped)
	e.fenced = false
	scheduler.mu.Unlock()

	// The first observation of the local fence must reset the ordinary backoff
	// (RT1B-3D transition); leaving the fence must clear the flag.
	e.markFenceTransition(true)
	if !e.fenced {
		t.Fatal("entry must be marked fenced")
	}
	if e.backoff != 0 {
		t.Fatalf("local fence transition must reset ordinary backoff, got %d", e.backoff)
	}
	e.backoff = 3
	e.markFenceTransition(true)
	if e.backoff != 3 {
		t.Fatalf("already-fenced entry must not be reset again, got %d", e.backoff)
	}
	e.markFenceTransition(false)
	if e.fenced {
		t.Fatal("leaving the fence must clear the flag")
	}
}

// TestRecoveryBackoffUsesIndependentCadence: while fenced, backoffDelay uses
// the recovery cadence (bounded by RecoveryMaxDelay), not the ordinary max.
func TestRecoveryBackoffUsesIndependentCadence(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-rb2")
	gen := adapterGeneration(1, 1, 1)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen}); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen); err != nil {
		t.Fatal(err)
	}
	handle, ok := fixture.registry.Guard(actorrouting.DomainPlayer, 0)
	if !ok || handle == nil {
		t.Fatal("guard missing")
	}
	handle.FenceForReason(routeauthority.LeaseFenceLocalDeadlineExpired)
	// Give the entry a large ordinary backoff; the recovery cadence must
	// ignore it.
	scheduler.mu.Lock()
	e := scheduler.entries[adapterShardKey(0)]
	e.backoff = 100
	e.fenced = true
	scheduler.mu.Unlock()

	delay := scheduler.backoffDelay(e)
	if delay > scheduler.recoveryMax+scheduler.recoveryJitter {
		t.Fatalf("fenced delay %v must use the recovery cadence (max %v)", delay, scheduler.recoveryMax)
	}
	if delay < scheduler.recoveryBase {
		t.Fatalf("fenced delay %v must not busy-loop below recovery base %v", delay, scheduler.recoveryBase)
	}
}

// TestRecoveryDelayNeverFallsBelowBase freezes the no-busy-loop lower bound:
// symmetric jitter may shorten a recovery delay, but never below recoveryBase.
func TestRecoveryDelayNeverFallsBelowBase(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	scheduler, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store: fixture.store, Registry: fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		LeaseTTL: 30 * time.Second, SafetyMargin: 5 * time.Second,
		BaseDelay: 5 * time.Millisecond, MaxDelay: 200 * time.Millisecond,
		RenewAttemptTimeout: 100 * time.Millisecond, RenewRecoveryTimeout: 100 * time.Millisecond,
		RecoveryBaseDelay: 50 * time.Millisecond, RecoveryMaxDelay: 200 * time.Millisecond,
		RecoveryJitterMax: 200 * time.Millisecond,
		Now:               fixture.clock.Now, JitterSeed: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-rb-delay-floor")
	gen := adapterGeneration(1, 1, 1)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen}); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen); err != nil {
		t.Fatal(err)
	}
	handle, ok := fixture.registry.Guard(actorrouting.DomainPlayer, 0)
	if !ok || handle == nil {
		t.Fatal("guard missing")
	}
	handle.FenceForReason(routeauthority.LeaseFenceLocalDeadlineExpired)
	scheduler.mu.Lock()
	e := scheduler.entries[adapterShardKey(0)]
	e.backoff = 0
	scheduler.mu.Unlock()
	// Stop the entry before directly sampling its immutable scheduler cadence.
	scheduler.Close()
	for i := 0; i < 1000; i++ {
		if delay := scheduler.backoffDelay(e); delay < scheduler.recoveryBase {
			t.Fatalf("recovery delay %v fell below base %v at sample %d", delay, scheduler.recoveryBase, i)
		}
	}
	// Extreme legacy durations saturate at max instead of wrapping negative.
	scheduler.recoveryBase = time.Duration(math.MaxInt64) / 2
	scheduler.recoveryMax = time.Duration(math.MaxInt64)
	scheduler.recoveryJitter = 0
	e.backoff = 20
	if delay := scheduler.backoffDelay(e); delay != scheduler.recoveryMax {
		t.Fatalf("overflowing recovery shift must saturate at max: got %v want %v", delay, scheduler.recoveryMax)
	}
}

// TestRecoveryJitterIsBoundedAndStable: recovery jitter is bounded and the
// same seed produces the same sequence across scheduler instances.
func TestRecoveryJitterIsBoundedAndStable(t *testing.T) {
	fixture := newMembershipAdapterFixture(t)
	opts := PerShardLeaseSchedulerOptions{
		Store: fixture.store, Registry: fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		LeaseTTL: 30 * time.Second, SafetyMargin: 5 * time.Second,
		BaseDelay: 5 * time.Millisecond, MaxDelay: 200 * time.Millisecond,
		RenewAttemptTimeout: time.Second, RenewRecoveryTimeout: time.Second,
		RecoveryMaxDelay: time.Second, RecoveryJitterMax: 100 * time.Millisecond,
		Now: fixture.clock.Now, JitterSeed: 7,
	}
	scheduler, err := NewPerShardLeaseScheduler(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	sameSeed, err := NewPerShardLeaseScheduler(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer sameSeed.Close()
	for i := 0; i < 100; i++ {
		d := scheduler.recoveryJitterDelay()
		if d < -100*time.Millisecond || d > 100*time.Millisecond {
			t.Fatalf("recovery jitter out of bounds: %v", d)
		}
		if d2 := sameSeed.recoveryJitterDelay(); d2 != d {
			t.Fatalf("same jitter seed diverged at sample %d: %v != %v", i, d, d2)
		}
	}
}

// TestConcurrentRecoveryJitterIsRaceFree exercises the shared scheduler PRNG
// from many shard-like goroutines; this test is a regression guard under
// go test -race because math/rand.Rand is not concurrency-safe by itself.
func TestConcurrentRecoveryJitterIsRaceFree(t *testing.T) {
	opts, _ := recoverySchedulerOptions(t)
	opts.RecoveryJitterMax = 100 * time.Millisecond
	scheduler, err := NewPerShardLeaseScheduler(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				_ = scheduler.recoveryJitterDelay()
			}
		}()
	}
	wg.Wait()
}

// TestRestoreBeforeLatestSafePointRearmsSameGeneration: when the store still
// accepts the same generation (authority lease valid), a successful recovery
// renew re-admits the guard and the generation identity is unchanged.
func TestRestoreBeforeLatestSafePointRearmsSameGeneration(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-rb3")
	gen := adapterGeneration(1, 1, 1)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen}); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen); err != nil {
		t.Fatal(err)
	}
	// Let the local safety deadline expire by advancing the clock past the
	// margin window: the entry's next renewal observes the recoverable local
	// fence, and because the store still accepts the SAME generation
	// (authority lease valid, network restored), a successful recovery renew
	// must re-arm the guard with an unchanged generation identity.
	// The adapter fixture uses SafetyMargin=5s and LeaseTTL=30s, so advancing
	// past deadline-margin triggers the recoverable local fence.
	fixture.clock.Advance(26 * time.Second)

	waitLeaseCondition(t, func() bool { return fixture.guardAdmitted(0) })
	snapshot, _ := fixture.registry.Snapshot(actorrouting.DomainPlayer, 0)
	if snapshot.Authority.WriterEpoch != 1 || snapshot.Authority.LeaseEpoch != 1 || snapshot.Authority.AssignmentRevision != 1 {
		t.Fatalf("same-generation identity must be preserved: %+v", snapshot.Authority)
	}
}

// TestRestoreAfterAuthorityExpiryCannotRearm: a LostGeneration (authority
// expired) renew cancels the entry and fences the guard; it must NOT re-admit
// the old generation.
func TestRestoreAfterAuthorityExpiryCannotRearm(t *testing.T) {
	fixture, fault, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-rb4")
	gen := adapterGeneration(1, 1, 1)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen}); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen); err != nil {
		t.Fatal(err)
	}
	fault.SetStatusFn(func(actorrouting.RouteDomain, uint32) (authoritystore.ShardAuthorityTransitionStatus, bool) {
		return authoritystore.ShardTransitionLeaseExpired, true
	})
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 0 })
	if fixture.guardAdmitted(0) {
		t.Fatal("expired authority must not leave the guard admitted")
	}
	if scheduler.Fatal() {
		t.Fatal("a normally fenced lost generation must not raise Fatal")
	}
}

// TestOneShardRecoveryDoesNotDelayOtherShards: recovery of one shard leaves
// another shard's renew cadence unaffected.
func TestOneShardRecoveryDoesNotDelayOtherShards(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	for _, shard := range []uint32{0, 1} {
		fixture.claimPlayerShard(shard, adapterHolder, adapterBoot, "claim-rb5-"+itoa(shard))
		gen := adapterGeneration(1, 1, 1)
		if err := fixture.installMember(ShardMembership{Key: adapterShardKey(shard), Generation: gen}); err != nil {
			t.Fatal(err)
		}
		if err := registerEntry(t, scheduler, adapterShardKey(shard), gen); err != nil {
			t.Fatal(err)
		}
	}
	// Fence shard 0 only; shard 1 stays active and keeps renewing.
	handle, ok := fixture.registry.Guard(actorrouting.DomainPlayer, 0)
	if !ok || handle == nil {
		t.Fatal("guard missing")
	}
	handle.FenceForReason(routeauthority.LeaseFenceLocalDeadlineExpired)
	waitLeaseCondition(t, func() bool { return fixture.guardAdmitted(1) })
	if fixture.guardAdmitted(0) {
		t.Fatal("fenced shard must not be admitted")
	}
	if scheduler.EntryCount() != 2 {
		t.Fatalf("both entries must survive, got %d", scheduler.EntryCount())
	}
}

// TestCloseCancelsRecoveryAttempt: closing the scheduler while an entry is
// recovering cancels it without Fatal.
func TestCloseCancelsRecoveryAttempt(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-rb6")
	gen := adapterGeneration(1, 1, 1)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen}); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen); err != nil {
		t.Fatal(err)
	}
	handle, ok := fixture.registry.Guard(actorrouting.DomainPlayer, 0)
	if !ok || handle == nil {
		t.Fatal("guard missing")
	}
	handle.FenceForReason(routeauthority.LeaseFenceLocalDeadlineExpired)
	scheduler.Close()
	if scheduler.EntryCount() != 0 {
		t.Fatalf("close must cancel the recovering entry, got %d", scheduler.EntryCount())
	}
	if scheduler.Fatal() {
		t.Fatal("close must not raise Fatal")
	}
}

// TestStaleGenerationRecoveryCannotAffectNewGeneration: a stale G1 entry
// recovering must not disturb a G2 entry installed for the same shard.
func TestStaleGenerationRecoveryCannotAffectNewGeneration(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-rb7")
	gen1 := adapterGeneration(1, 1, 1)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen1}); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen1); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.SetShardWriteAuthority(adapterAuthorityFor(2, 2, 2, fixture.clock)); err != nil {
		t.Fatal(err)
	}
	gen2 := adapterGeneration(2, 2, 2)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen2}); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen2); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 1 })
	// The surviving entry must be G2.
	scheduler.mu.Lock()
	entry := scheduler.entries[adapterShardKey(0)]
	scheduler.mu.Unlock()
	if entry == nil || !entry.generation.equal(gen2) {
		t.Fatal("surviving entry must be generation 2")
	}
}

func itoa(v uint32) string { return fmt.Sprintf("%d", v) }

// recoverySchedulerOptions builds a minimal scheduler fixture with explicit
// recovery tuning for the budget tests below.
func recoverySchedulerOptions(t *testing.T) (PerShardLeaseSchedulerOptions, *membershipAdapterFixture) {
	t.Helper()
	fixture := newMembershipAdapterFixture(t)
	return PerShardLeaseSchedulerOptions{
		Store: fixture.store, Registry: fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		LeaseTTL: 30 * time.Second, SafetyMargin: 5 * time.Second,
		BaseDelay: 5 * time.Millisecond, MaxDelay: 200 * time.Millisecond,
		RenewAttemptTimeout: time.Second,
		Now:                 fixture.clock.Now, JitterSeed: 42,
	}, fixture
}

// TestPartialExplicitRecoveryOptionsStillValidateBudget (P1 #1): a budget check
// must run even when RecoveryMaxDelay is the zero-valued (derived) field.  Here
// the caller explicitly configures timeout/jitter/slack but leaves max at its
// default derivation; the effective budget still exceeds the margin and the
// construction MUST fail closed.
func TestPartialExplicitRecoveryOptionsStillValidateBudget(t *testing.T) {
	opts, _ := recoverySchedulerOptions(t)
	opts.RenewRecoveryTimeout = 2 * time.Second // explicit
	opts.RecoveryJitterMax = 3 * time.Second    // explicit
	opts.RecoverySchedulerSlack = time.Second   // explicit
	opts.RecoveryMaxDelay = 0                   // derived: min(MaxDelay, margin/4) = 200ms
	scheduler, err := NewPerShardLeaseScheduler(opts)
	if err == nil {
		if scheduler != nil {
			scheduler.Close()
		}
		t.Fatal("partially explicit unsafe budget must fail startup")
	}
}

// TestJitterAndSlackCanMakeDefaultMaxDelayUnsafe (P1 #1): even with the max
// delay derived from safe defaults, an explicit jitter/slack pushes the
// effective budget past the margin.  The old gate (check only when
// RecoveryMaxDelay was explicitly non-zero) would have silently accepted this.
func TestJitterAndSlackCanMakeDefaultMaxDelayUnsafe(t *testing.T) {
	opts, _ := recoverySchedulerOptions(t)
	opts.RenewRecoveryTimeout = 0             // derived: min(1s, margin/4) = 1s
	opts.RecoveryJitterMax = 3 * time.Second  // explicit
	opts.RecoverySchedulerSlack = time.Second // explicit
	opts.RecoveryMaxDelay = 0                 // derived
	scheduler, err := NewPerShardLeaseScheduler(opts)
	if err == nil {
		if scheduler != nil {
			scheduler.Close()
		}
		t.Fatal("explicit jitter/slack with derived max must fail startup when the effective budget exceeds the margin")
	}
}

// TestTimeoutAloneLargerThanMarginIsRejected (P1 #1): 2*timeout alone can
// exceed the margin even with zero max/jitter/slack.
func TestTimeoutAloneLargerThanMarginIsRejected(t *testing.T) {
	opts, _ := recoverySchedulerOptions(t)
	opts.RenewRecoveryTimeout = 3 * time.Second // 2*3s = 6s >= 5s margin
	opts.RecoveryJitterMax = 0
	opts.RecoverySchedulerSlack = 0
	scheduler, err := NewPerShardLeaseScheduler(opts)
	if err == nil {
		if scheduler != nil {
			scheduler.Close()
		}
		t.Fatal("an attempt timeout larger than half the margin must fail startup")
	}
}

// TestDefaultOptionsProduceSafeEffectiveBudget (P1 #1): with every recovery
// field at zero the all-default derivation must produce a budget strictly
// inside the margin and the scheduler must start.
func TestDefaultOptionsProduceSafeEffectiveBudget(t *testing.T) {
	opts, _ := recoverySchedulerOptions(t)
	// RecoveryMaxDelay / RecoveryJitterMax / RecoverySchedulerSlack /
	// RenewRecoveryTimeout are all zero: fully derived.
	opts.RenewRecoveryTimeout = 0
	opts.RecoveryMaxDelay = 0
	opts.RecoveryJitterMax = 0
	opts.RecoverySchedulerSlack = 0
	scheduler, err := NewPerShardLeaseScheduler(opts)
	if err != nil {
		t.Fatalf("all-default recovery cadence must start: %v", err)
	}
	defer scheduler.Close()
	info := scheduler.RecoveryBudgetInfo()
	if !info.Satisfied {
		t.Fatalf("all-default effective budget must satisfy the margin: %+v", info)
	}
	// Derived: max = min(200ms, 5s/4) = 200ms; timeout = min(1s, 5s/4) = 1s;
	// jitter = 200ms/4 = 50ms; budget = 2*1s + 200ms + 50ms = 2.25s.
	want := 2*time.Second + 200*time.Millisecond + 50*time.Millisecond
	if info.CompletionBudget != want {
		t.Fatalf("effective completion budget = %v, want %v", info.CompletionBudget, want)
	}
}

// TestDefaultRecoveryTimeoutIsCappedBySafetyMargin closes the partial-default
// path where zero must remain distinguishable from an explicit timeout until
// the final effective values are derived.
func TestDefaultRecoveryTimeoutIsCappedBySafetyMargin(t *testing.T) {
	opts, _ := recoverySchedulerOptions(t)
	opts.RenewAttemptTimeout = 4 * time.Second
	opts.RenewRecoveryTimeout = 0 // derived and capped to margin/4 = 1.25s
	opts.RecoveryMaxDelay = 100 * time.Millisecond
	opts.RecoveryJitterMax = 25 * time.Millisecond
	scheduler, err := NewPerShardLeaseScheduler(opts)
	if err != nil {
		t.Fatalf("derived recovery timeout must be capped into a safe budget: %v", err)
	}
	defer scheduler.Close()
	info := scheduler.RecoveryBudgetInfo()
	if info.RecoveryAttemptTimeout != 1250*time.Millisecond {
		t.Fatalf("derived recovery timeout = %v, want 1.25s: %+v", info.RecoveryAttemptTimeout, info)
	}
	if !info.Satisfied {
		t.Fatalf("capped default recovery budget must satisfy margin: %+v", info)
	}
}

// TestExplicitRecoveryBaseAboveMaxRejected: an EXPLICIT recovery base above
// the effective recovery max is a configuration contradiction and must fail
// closed (a derived base is capped instead).
func TestExplicitRecoveryBaseAboveMaxRejected(t *testing.T) {
	opts, _ := recoverySchedulerOptions(t)
	opts.RecoveryBaseDelay = 2 * time.Second
	opts.RecoveryMaxDelay = time.Second
	scheduler, err := NewPerShardLeaseScheduler(opts)
	if err == nil {
		if scheduler != nil {
			scheduler.Close()
		}
		t.Fatal("explicit recovery base above recovery max must fail startup")
	}
}

// TestRecoveryDurationOverflowRejected (P1 #1): a budget whose durations
// overflow int64 nanoseconds must fail closed instead of wrapping.
func TestRecoveryDurationOverflowRejected(t *testing.T) {
	opts, _ := recoverySchedulerOptions(t)
	opts.RenewRecoveryTimeout = time.Duration(math.MaxInt64)/2 + time.Second
	opts.RecoveryMaxDelay = time.Duration(math.MaxInt64) / 2
	opts.RecoveryJitterMax = time.Duration(math.MaxInt64) / 4
	opts.RecoverySchedulerSlack = time.Second
	scheduler, err := NewPerShardLeaseScheduler(opts)
	if err == nil {
		if scheduler != nil {
			scheduler.Close()
		}
		t.Fatal("an overflowing recovery budget must fail startup")
	}
}

// TestRecoveryBudgetInfoExposesEffectiveValues: the scheduler exposes the
// FINAL EFFECTIVE recovery cadence (not the raw options) so startup logging
// and operator diagnostics see exactly what the runtime will enforce.
func TestRecoveryBudgetInfoExposesEffectiveValues(t *testing.T) {
	opts, _ := recoverySchedulerOptions(t)
	opts.RenewRecoveryTimeout = time.Second
	opts.RecoveryMaxDelay = time.Second
	opts.RecoveryJitterMax = 100 * time.Millisecond
	opts.RecoverySchedulerSlack = 50 * time.Millisecond
	scheduler, err := NewPerShardLeaseScheduler(opts)
	if err != nil {
		t.Fatalf("scheduler must start: %v", err)
	}
	defer scheduler.Close()
	info := scheduler.RecoveryBudgetInfo()
	if info.SafetyMargin != 5*time.Second || info.RecoveryMaxDelay != time.Second ||
		info.RecoveryJitterMax != 100*time.Millisecond || info.RecoverySchedulerSlack != 50*time.Millisecond ||
		info.RecoveryAttemptTimeout != time.Second || info.RecoveryBaseDelay != 5*time.Millisecond {
		t.Fatalf("effective recovery info mismatch: %+v", info)
	}
	want := 2*time.Second + time.Second + 100*time.Millisecond + 50*time.Millisecond
	if info.CompletionBudget != want || !info.Satisfied {
		t.Fatalf("completion budget = %v (want %v), satisfied = %v", info.CompletionBudget, want, info.Satisfied)
	}
}

// TestRestoreWhileRecoveryAttemptAlreadyBlockedMeetsBudget (P1 #2): the
// frozen worst-case restore timeline --
//
//  1. the entry is already in a blocked Recovery Attempt A when the network
//     comes back (A cannot be reused and burns a full attempt timeout);
//  2. the recovery cadence waits max+jitter;
//  3. Attempt B must still complete inside the authority lease.
//
// The conservative completion budget is
//
//	2*RecoveryAttemptTimeout + RecoveryMaxDelay + RecoveryJitterMax +
//	RecoverySchedulerSlack
//
// and the deterministic test asserts that a full blocked-attempt restore
// finishes within that budget from the moment A starts.
func TestRestoreWhileRecoveryAttemptAlreadyBlockedMeetsBudget(t *testing.T) {
	fixture, fault, _ := newSchedulerFixture(t)
	// Explicit, deterministic recovery cadence with a small completion budget.
	scheduler2, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store: fault, Registry: fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		LeaseTTL: 30 * time.Second, SafetyMargin: 5 * time.Second,
		BaseDelay: 5 * time.Millisecond, MaxDelay: 200 * time.Millisecond, Jitter: 3 * time.Millisecond,
		RenewAttemptTimeout:    300 * time.Millisecond,
		RenewRecoveryTimeout:   300 * time.Millisecond,
		RecoveryMaxDelay:       300 * time.Millisecond,
		RecoveryJitterMax:      50 * time.Millisecond,
		RecoverySchedulerSlack: 50 * time.Millisecond,
		Now:                    fixture.clock.Now, JitterSeed: 42,
	})
	if err != nil {
		t.Fatalf("safe recovery budget must start: %v", err)
	}
	defer scheduler2.Close()

	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-rb-budget")
	gen := adapterGeneration(1, 1, 1)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen}); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler2, adapterShardKey(0), gen); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return fixture.guardAdmitted(0) })

	// Block every renew: the entry's next renew becomes Recovery Attempt A.
	block := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	fault.SetRenewEntered(func() { once.Do(func() { close(entered) }) })
	fault.SetRenewBlock(block)
	// Cross the local safety deadline so A (and B) run under the recovery
	// attempt timeout (300ms), never the pre-deadline renew timeout.
	fixture.clock.Advance(26 * time.Second)

	// A is now in flight and blocked.
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("recovery attempt A did not enter the blocked renew")
	}
	started := time.Now()

	// A cannot be reused: wait for its timeout to be observed by the entry
	// state machine, then restore the network. Closing the block also releases
	// B if it raced into the old fault window; no wall-clock sleep is needed.
	waitLeaseCondition(t, func() bool {
		scheduler2.mu.Lock()
		defer scheduler2.mu.Unlock()
		e := scheduler2.entries[adapterShardKey(0)]
		return e != nil && e.backoff > 0
	})
	close(block)
	fault.SetRenewBlock(nil)

	waitLeaseCondition(t, func() bool { return fixture.guardAdmitted(0) })
	elapsed := time.Since(started)
	info := scheduler2.RecoveryBudgetInfo()
	if !info.Satisfied {
		t.Fatalf("budget must be satisfied for the blocked-attempt test: %+v", info)
	}
	// A full timeout (A) + cadence (max+jitter) + one more timeout (B) must
	// fit inside the completion budget; allow a small scheduling buffer.
	budget := info.CompletionBudget
	if elapsed > budget+150*time.Millisecond {
		t.Fatalf("restore after a blocked attempt took %v > completion budget %v (+buffer)", elapsed, budget)
	}
}

// longOrdinaryBackoffScheduler builds a scheduler whose ORDINARY backoff can
// grow to 30s (far past any local safety deadline), so deadline-aware wakeup
// tests can prove the entry never sleeps past the deadline.
func longOrdinaryBackoffScheduler(t *testing.T, fixture *membershipAdapterFixture, fault *faultRenewStore) *PerShardLeaseScheduler {
	t.Helper()
	scheduler, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store: fault, Registry: fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		LeaseTTL:  30 * time.Second,
		BaseDelay: 5 * time.Millisecond, MaxDelay: 30 * time.Second, Jitter: 3 * time.Millisecond,
		RenewAttemptTimeout:  time.Second,
		RenewRecoveryTimeout: time.Second,
		Now:                  fixture.clock.Now, JitterSeed: 42,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(scheduler.Close)
	return scheduler
}

// TestOrdinaryBackoffCannotSleepPastLocalDeadline (deadline-aware wakeup #1):
// an ordinary backoff that grew to 30s must never delay the recoverable local
// fence: the next wakeup is capped at the local deadline.
func TestOrdinaryBackoffCannotSleepPastLocalDeadline(t *testing.T) {
	fixture, fault, _ := newSchedulerFixture(t)
	scheduler := longOrdinaryBackoffScheduler(t, fixture, fault)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-dw1")
	gen := adapterGeneration(1, 1, 1)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen}); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return fixture.guardAdmitted(0) })

	fault.SetRenewErr(fmt.Errorf("ordinary fault"))
	scheduler.mu.Lock()
	if e, ok := scheduler.entries[adapterShardKey(0)]; ok {
		e.backoff = 13 // min(5ms<<13, MaxDelay=30s) = 30s ordinary backoff
	} else {
		scheduler.mu.Unlock()
		t.Fatal("entry missing")
	}
	scheduler.mu.Unlock()
	// ~30ms remain before the local safety deadline (install deadline = t0+25s).
	fixture.clock.Advance(24*time.Second + 970*time.Millisecond)

	entered := make(chan struct{})
	var once sync.Once
	block := make(chan struct{})
	fault.SetRenewEntered(func() { once.Do(func() { close(entered) }) })
	fault.SetRenewBlock(block)
	defer fault.SetRenewBlock(nil)

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("entry slept past the local deadline instead of the deadline-aware wakeup")
	}
}

// TestInitialWakeCannotSleepPastLocalDeadline applies the same deadline cap to
// a newly activated entry's initial scatter timer, not only later retries.
func TestInitialWakeCannotSleepPastLocalDeadline(t *testing.T) {
	fixture, fault, _ := newSchedulerFixture(t)
	scheduler, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store: fault, Registry: fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		LeaseTTL:  30 * time.Second,
		BaseDelay: 30 * time.Second, MaxDelay: 30 * time.Second,
		RenewAttemptTimeout: time.Second, RenewRecoveryTimeout: time.Second,
		Now: fixture.clock.Now, JitterSeed: 42,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-dw-initial")
	gen := adapterGeneration(1, 1, 1)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen}); err != nil {
		t.Fatal(err)
	}
	// The installed guard has a 25s local window. Leave only ~30ms before
	// activation; the uncapped initial delay would be 15s.
	fixture.clock.Advance(24*time.Second + 970*time.Millisecond)
	entered := make(chan struct{})
	var once sync.Once
	fault.SetRenewEntered(func() { once.Do(func() { close(entered) }) })
	probe := make(chan struct{})
	close(probe)
	fault.SetRenewBlock(probe)
	defer fault.SetRenewBlock(nil)
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("initial scatter timer slept past the local safety deadline")
	}
}

// TestDeadlineWakeResetsOrdinaryBackoff (deadline-aware wakeup #2): once the
// deadline fires the wakeup, the entry observes the recoverable fence, resets
// the ordinary backoff exactly once, and retries on the recovery cadence.
func TestDeadlineWakeResetsOrdinaryBackoff(t *testing.T) {
	fixture, fault, _ := newSchedulerFixture(t)
	scheduler := longOrdinaryBackoffScheduler(t, fixture, fault)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-dw2")
	gen := adapterGeneration(1, 1, 1)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen}); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return fixture.guardAdmitted(0) })

	fault.SetRenewErr(fmt.Errorf("ordinary fault"))
	scheduler.mu.Lock()
	scheduler.entries[adapterShardKey(0)].backoff = 13
	scheduler.mu.Unlock()
	// Cross the local deadline.
	fixture.clock.Advance(25*time.Second + 100*time.Millisecond)

	// The deadline-aware wakeup fires immediately; the entry must observe the
	// recoverable local fence and reset the ordinary backoff.
	waitLeaseCondition(t, func() bool {
		scheduler.mu.Lock()
		defer scheduler.mu.Unlock()
		e, ok := scheduler.entries[adapterShardKey(0)]
		return ok && e.fenced
	})
	scheduler.mu.Lock()
	e, ok := scheduler.entries[adapterShardKey(0)]
	scheduler.mu.Unlock()
	if !ok {
		t.Fatal("entry must survive the deadline transition")
	}
	if delay := scheduler.backoffDelay(e); delay > scheduler.recoveryMax+scheduler.recoveryJitter {
		t.Fatalf("after the deadline wake the retry must use the recovery cadence, got %v", delay)
	}
}

// TestRestoreBeforeLatestSafePointDuringLongOrdinaryBackoffRearms
// (deadline-aware wakeup #3, core regression): while the entry is still parked
// on a 30s ordinary backoff, the network recovers before latest_safe_restore_at.
// The deadline-aware wakeup must let the same-generation renew rearm the guard
// before the authority lease expires.
func TestRestoreBeforeLatestSafePointDuringLongOrdinaryBackoffRearms(t *testing.T) {
	fixture, fault, _ := newSchedulerFixture(t)
	scheduler := longOrdinaryBackoffScheduler(t, fixture, fault)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-dw3")
	gen := adapterGeneration(1, 1, 1)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen}); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return fixture.guardAdmitted(0) })

	// Network fault while the ordinary backoff grows to 30s.
	fault.SetRenewErr(fmt.Errorf("network fault"))
	scheduler.mu.Lock()
	scheduler.entries[adapterShardKey(0)].backoff = 13
	scheduler.mu.Unlock()
	// ~100ms remain until the local safety deadline.
	fixture.clock.Advance(24*time.Second + 900*time.Millisecond)

	// Network restored BEFORE latest_safe_restore_at (the authority lease is
	// still valid for multiple recovery budgets).
	fault.SetRenewErr(nil)

	// The deadline-aware wakeup lets a successful same-generation renew refresh
	// the deadline; wait for the refreshed (future) deadline.
	waitLeaseCondition(t, func() bool {
		snap, ok := fixture.registry.Snapshot(actorrouting.DomainPlayer, 0)
		return ok && !snap.Deadline.IsZero() && snap.Deadline.After(fixture.clock.Now().Add(23*time.Second))
	})
	// Cross the ORIGINAL deadline: the refreshed deadline must keep the shard up.
	fixture.clock.Advance(300 * time.Millisecond)
	if !fixture.guardAdmitted(0) {
		t.Fatal("same-generation renew must rearm the guard before the safe point despite the long ordinary backoff")
	}
}

// TestDeadlineWakeDoesNotBusyLoop (deadline-aware wakeup #4): while fenced and
// every recovery renew fails, the renew rate must follow the independent
// recovery cadence -- never a zero-delay busy loop.
func TestDeadlineWakeDoesNotBusyLoop(t *testing.T) {
	fixture, fault, _ := newSchedulerFixture(t)
	scheduler, err := NewPerShardLeaseScheduler(PerShardLeaseSchedulerOptions{
		Store: fault, Registry: fixture.registry,
		AuthorityLayoutVersion: adapterLayout,
		HashVersion:            actorrouting.HashVersionXXH64V2, HashSeed: adapterSeed,
		HolderID: adapterHolder, HolderBootID: adapterBoot,
		LeaseTTL: 30 * time.Second, SafetyMargin: 5 * time.Second,
		BaseDelay: 5 * time.Millisecond, MaxDelay: 200 * time.Millisecond, Jitter: 3 * time.Millisecond,
		RenewAttemptTimeout:    time.Second,
		RenewRecoveryTimeout:   100 * time.Millisecond,
		RecoveryBaseDelay:      50 * time.Millisecond,
		RecoveryMaxDelay:       200 * time.Millisecond,
		RecoveryJitterMax:      0,
		RecoverySchedulerSlack: 0,
		Now:                    fixture.clock.Now, JitterSeed: 42,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Close()
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-dw4")
	gen := adapterGeneration(1, 1, 1)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen}); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return fixture.guardAdmitted(0) })

	handle, ok := fixture.registry.Guard(actorrouting.DomainPlayer, 0)
	if !ok || handle == nil {
		t.Fatal("guard missing")
	}
	handle.FenceForReason(routeauthority.LeaseFenceLocalDeadlineExpired)
	fault.SetRenewErr(fmt.Errorf("recovery keeps failing"))
	var count atomic.Int64
	fault.SetRenewEntered(func() { count.Add(1) })
	// A pre-closed block channel makes every attempt enter the probe without
	// ever blocking (the store error fails it immediately).
	done := make(chan struct{})
	close(done)
	fault.SetRenewBlockFn(func(actorrouting.RouteDomain, uint32) <-chan struct{} { return done })
	defer fault.SetRenewBlockFn(nil)

	time.Sleep(300 * time.Millisecond)
	// Cadence 50/100/200ms: at most ~4 attempts in 300ms; a busy loop would
	// produce far more.  The budget check here is intentionally loose to stay
	// deterministic while still bounding the rate away from a busy loop.
	if n := count.Load(); n > 10 {
		t.Fatalf("recovery renew busied: %d attempts in 300ms", n)
	}
}

// TestTerminalFenceDoesNotEnterRecoveryCadence (deadline-aware wakeup #5): a
// terminal fence (MembershipRemoved / LostGeneration / RuntimeFatal) must never
// be woken into a recovery cadence -- the entry stops instead.
func TestTerminalFenceDoesNotEnterRecoveryCadence(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-dw5")
	gen := adapterGeneration(1, 1, 1)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen}); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return fixture.guardAdmitted(0) })
	handle, ok := fixture.registry.Guard(actorrouting.DomainPlayer, 0)
	if !ok || handle == nil {
		t.Fatal("guard missing")
	}
	// A terminal LostGeneration fence: the deadline-aware wakeup must NOT start
	// a recovery cadence; the entry cancels itself.
	handle.FenceForReason(routeauthority.LeaseFenceLostGeneration)
	fixture.clock.Advance(30 * time.Second)
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 0 })
	if scheduler.Fatal() {
		t.Fatal("a terminal fence must not raise Fatal")
	}
}

// TestStaleDeadlineWakeCannotAffectReplacementGeneration (deadline-aware
// wakeup #6): a stale G1 entry whose deadline timer fires must never disturb
// the G2 entry installed for the same shard.
func TestStaleDeadlineWakeCannotAffectReplacementGeneration(t *testing.T) {
	fixture, _, scheduler := newSchedulerFixture(t)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-dw6")
	gen1 := adapterGeneration(1, 1, 1)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen1}); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen1); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.SetShardWriteAuthority(adapterAuthorityFor(2, 2, 2, fixture.clock)); err != nil {
		t.Fatal(err)
	}
	gen2 := adapterGeneration(2, 2, 2)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen2}); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen2); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return scheduler.EntryCount() == 1 })
	scheduler.mu.Lock()
	entry := scheduler.entries[adapterShardKey(0)]
	scheduler.mu.Unlock()
	if entry == nil || !entry.generation.equal(gen2) {
		t.Fatal("surviving entry must be generation 2")
	}
	// Cross the deadline: any stale G1 wakeup must be discarded by the
	// generation/token revalidation; G2 keeps serving.
	fixture.clock.Advance(26 * time.Second)
	if entry2, ok := scheduler.entries[adapterShardKey(0)]; !ok || !entry2.generation.equal(gen2) {
		t.Fatal("G2 entry must survive the stale deadline wake")
	}
}

// TestCloseCancelsDeadlineWakeAndRecovery (deadline-aware wakeup #7): closing
// the scheduler while the entry is parked on a deadline-aware wake cancels the
// timer and any blocked recovery without Fatal.
func TestCloseCancelsDeadlineWakeAndRecovery(t *testing.T) {
	fixture, fault, _ := newSchedulerFixture(t)
	scheduler := longOrdinaryBackoffScheduler(t, fixture, fault)
	fixture.claimPlayerShard(0, adapterHolder, adapterBoot, "claim-dw7")
	gen := adapterGeneration(1, 1, 1)
	if err := fixture.installMember(ShardMembership{Key: adapterShardKey(0), Generation: gen}); err != nil {
		t.Fatal(err)
	}
	if err := registerEntry(t, scheduler, adapterShardKey(0), gen); err != nil {
		t.Fatal(err)
	}
	waitLeaseCondition(t, func() bool { return fixture.guardAdmitted(0) })
	fault.SetRenewErr(fmt.Errorf("ordinary fault"))
	scheduler.mu.Lock()
	scheduler.entries[adapterShardKey(0)].backoff = 13
	scheduler.mu.Unlock()
	// Cross the deadline so the deadline-aware wakeup is armed.
	fixture.clock.Advance(25*time.Second + 100*time.Millisecond)
	block := make(chan struct{})
	fault.SetRenewBlock(block)
	defer fault.SetRenewBlock(nil)

	scheduler.Close()
	if scheduler.EntryCount() != 0 {
		t.Fatalf("close must cancel the waking entry, got %d", scheduler.EntryCount())
	}
	if scheduler.Fatal() {
		t.Fatal("close must not raise Fatal")
	}
}

// TestDefaultBaseWithExplicitSmallerMaxIsCapped (deadline-aware wakeup #8):
// when RecoveryBaseDelay is left at its default derivation but the effective
// RecoveryMaxDelay is EXPLICITLY smaller, the derived base is capped to the
// effective max (never a silent base>max configuration).
func TestDefaultBaseWithExplicitSmallerMaxIsCapped(t *testing.T) {
	opts, _ := recoverySchedulerOptions(t)
	opts.RecoveryBaseDelay = 0                   // derived: BaseDelay = 5ms
	opts.RecoveryMaxDelay = 2 * time.Millisecond // explicit, smaller than derived base
	opts.RenewRecoveryTimeout = time.Second
	opts.RecoveryJitterMax = 0
	opts.RecoverySchedulerSlack = 0
	scheduler, err := NewPerShardLeaseScheduler(opts)
	if err != nil {
		t.Fatalf("a derived base must be capped to the effective max, not rejected: %v", err)
	}
	defer scheduler.Close()
	info := scheduler.RecoveryBudgetInfo()
	if info.RecoveryBaseDelay != 2*time.Millisecond || info.RecoveryMaxDelay != 2*time.Millisecond {
		t.Fatalf("effective recovery base/max must both be 2ms (derived base capped), got %+v", info)
	}
}
