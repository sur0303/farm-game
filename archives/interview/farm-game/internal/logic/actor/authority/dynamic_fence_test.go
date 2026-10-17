package authority

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/storage/authoritystore"
)

func TestDynamicFenceSourceUsesContinuousAssignmentAndLocalLease(t *testing.T) {
	start := time.Now()
	now := start
	calc, err := actorrouting.NewCalculator("classic-farm", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040010}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	store, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver: calc, AuthorityLayoutVersion: "auth-v1", Now: func() time.Time { return now },
		LeaseLimits: authoritystore.LeaseLimits{MinTTL: 100 * time.Millisecond, MaxTTL: 5 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimShardAuthority(context.Background(), authoritystore.ClaimShardAuthorityRequest{
		Caller:       authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerRouteManager, InstanceID: "rm-a"},
		TransitionID: "claim-player-a", AuthorityLayoutVersion: "auth-v1", Domain: actorrouting.DomainPlayer,
		VirtualShard: 0, NewHolderID: "player-a", NewHolderBootID: "player-a-boot", RequestedTTLms: 1000,
	})
	if err != nil || claim.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	snapshot := actorrouting.RouteSnapshot{
		Realm: "classic-farm", GlobalRevision: 1, HashVersion: calc.Profile().Version, HashSeed: calc.Profile().Seed,
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: claim.Authority.AssignmentRevision,
			ExpectedWriterEpoch: claim.Authority.WriterEpoch, ExpectedLeaseEpoch: claim.Authority.LeaseEpoch,
			LeaseUntilMs: claim.Authority.LeaseUntilMs,
		}}},
		Farm:    actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 1, State: actorrouting.ShardStateAssigning, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1}}},
		Holders: []actorrouting.HolderRef{{InstanceID: "player-a", BootID: "player-a-boot", Endpoint: "player-a:9000", State: actorrouting.HolderStateActive, Weight: 1}, {InstanceID: "farm-a", BootID: "farm-a-boot", Endpoint: "farm-a:9001", State: actorrouting.HolderStateAssigning, Weight: 1}},
	}
	provider, _, err := actorrouting.NewDynamicAssignmentProvider(snapshot, 4)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := authoritystore.NewHolderLeaseGuard("player-a", "player-a-boot", 100*time.Millisecond, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := guard.InstallGrant(start, claim); err != nil {
		t.Fatal(err)
	}
	guards := NewLeaseGuardRegistry()
	if err := guards.Install(actorrouting.DomainPlayer, 0, guard); err != nil {
		t.Fatal(err)
	}
	source, err := NewDynamicFenceSource(DynamicFenceSourceOptions{Calculator: calc, Snapshots: provider, Guards: guards, AuthorityLayoutVersion: "auth-v1", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	fence, err := source.WriteFence(context.Background(), authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 42})
	if err != nil {
		t.Fatal(err)
	}
	if fence.ExpectedHolderID != "player-a" || fence.ExpectedHolderBootID != "player-a-boot" || fence.ExpectedLeaseEpoch != claim.Authority.LeaseEpoch {
		t.Fatalf("fence=%+v", fence)
	}

	// Renew extends the guard's local deadline without changing the tuple.
	now = start.Add(800 * time.Millisecond)
	runtime, err := NewHolderLeaseRuntime(guard, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	renewed, err := runtime.Renew(context.Background(), store, authoritystore.RenewShardAuthorityRequest{
		Caller:                 authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: "player-a", BootID: "player-a-boot"},
		AuthorityLayoutVersion: "auth-v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: fence, RequestedTTLms: 1000,
	})
	if err != nil || renewed.Status != authoritystore.ShardTransitionApplied {
		t.Fatalf("renewed=%+v err=%v", renewed, err)
	}
	now = start.Add(1500 * time.Millisecond)
	if _, err := source.WriteFence(context.Background(), authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 42}); err != nil {
		t.Fatalf("renewed local deadline should still admit: %v", err)
	}
	now = start.Add(1800 * time.Millisecond)
	if _, err := source.WriteFence(context.Background(), authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 42}); !errors.Is(err, ErrLeaseAdmission) && !errors.Is(err, authoritystore.ErrLeaseDeadlineExpired) {
		t.Fatalf("expired local lease err=%v", err)
	}
}

func TestDynamicFenceSourceFencesOnNewerAssignmentAndSeparatesDomains(t *testing.T) {
	start := time.Now()
	calc, err := actorrouting.NewCalculator("classic-farm", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040011}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	playerAuthority := authoritystore.ShardWriteAuthority{AuthorityLayoutVersion: "auth-v1", HashVersion: 2, HashSeed: calc.Profile().Seed, Domain: actorrouting.DomainPlayer, VirtualShard: 0, HolderID: "player-a", HolderBootID: "player-boot", AssignmentRevision: 7, WriterEpoch: 9, LeaseEpoch: 11, State: authoritystore.ShardAuthorityActive, LeaseUntilMs: start.Add(time.Minute).UnixMilli()}
	farmAuthority := playerAuthority
	farmAuthority.Domain = actorrouting.DomainFarm
	farmAuthority.HolderID = "farm-a"
	farmAuthority.HolderBootID = "farm-boot"
	snapshot := actorrouting.RouteSnapshot{
		Realm: "classic-farm", GlobalRevision: 7, HashVersion: 2, HashSeed: calc.Profile().Seed,
		Player:  actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 7, ExpectedWriterEpoch: 9, ExpectedLeaseEpoch: 11, LeaseUntilMs: start.Add(time.Minute).UnixMilli()}}},
		Farm:    actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: 7, ExpectedWriterEpoch: 9, ExpectedLeaseEpoch: 11, LeaseUntilMs: start.Add(time.Minute).UnixMilli()}}},
		Holders: []actorrouting.HolderRef{{InstanceID: "player-a", BootID: "player-boot", Endpoint: "player:9000", State: actorrouting.HolderStateActive, Weight: 1}, {InstanceID: "farm-a", BootID: "farm-boot", Endpoint: "farm:9001", State: actorrouting.HolderStateActive, Weight: 1}},
	}
	provider, _, err := actorrouting.NewDynamicAssignmentProvider(snapshot, 4)
	if err != nil {
		t.Fatal(err)
	}
	guards := NewLeaseGuardRegistry()
	for _, item := range []struct {
		domain actorrouting.RouteDomain
		holder string
		boot   string
		auth   authoritystore.ShardWriteAuthority
	}{
		{actorrouting.DomainPlayer, "player-a", "player-boot", playerAuthority},
		{actorrouting.DomainFarm, "farm-a", "farm-boot", farmAuthority},
	} {
		guard, guardErr := authoritystore.NewHolderLeaseGuard(item.holder, item.boot, 100*time.Millisecond, func() time.Time { return start })
		if guardErr != nil {
			t.Fatal(guardErr)
		}
		if guardErr = guard.InstallGrant(start, authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionApplied, Authority: item.auth, AcceptedTTLms: 60_000}); guardErr != nil {
			t.Fatal(guardErr)
		}
		if guardErr = guards.Install(item.domain, 0, guard); guardErr != nil {
			t.Fatal(guardErr)
		}
	}
	source, err := NewDynamicFenceSource(DynamicFenceSourceOptions{Calculator: calc, Snapshots: provider, Guards: guards, AuthorityLayoutVersion: "auth-v1", Now: func() time.Time { return start }})
	if err != nil {
		t.Fatal(err)
	}
	playerFence, err := source.WriteFence(context.Background(), authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 100})
	if err != nil {
		t.Fatal(err)
	}
	farmFence, err := source.WriteFence(context.Background(), authoritystore.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 100})
	if err != nil {
		t.Fatal(err)
	}
	if playerFence.ExpectedHolderID == farmFence.ExpectedHolderID || playerFence.ExpectedWriterEpoch != farmFence.ExpectedWriterEpoch {
		// The holder identity must be independent even when the actor ID is equal;
		// epoch spaces are checked separately below with a domain-local update.
		t.Fatalf("domain fences shared: player=%+v farm=%+v", playerFence, farmFence)
	}
	newPlayer := playerAuthority
	newPlayer.AssignmentRevision++
	newPlayer.WriterEpoch++
	newPlayer.LeaseEpoch++
	playerGuard, ok := guards.Guard(actorrouting.DomainPlayer, 0)
	if !ok || playerGuard == nil {
		t.Fatal("player guard missing")
	}
	if err := playerGuard.ObserveAuthority(newPlayer); err != nil {
		t.Fatal(err)
	}
	if _, err := source.WriteFence(context.Background(), authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 100}); !errors.Is(err, ErrLeaseAdmission) {
		t.Fatalf("newer assignment must fence old local guard: %v", err)
	}
	if _, err := source.WriteFence(context.Background(), authoritystore.ActorAddress{Domain: actorrouting.DomainFarm, ActorID: 100}); err != nil {
		t.Fatalf("farm guard must remain independent: %v", err)
	}
}

func TestDynamicFenceSourceBindsAuthorityProfileAtGuardInstall(t *testing.T) {
	start := time.UnixMilli(1_900_000_000_000)
	calc, err := actorrouting.NewCalculator("classic-farm", actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040099}, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	authority := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "auth-v1", HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: calc.Profile().Seed + 1,
		Domain: actorrouting.DomainPlayer, VirtualShard: 0, HolderID: "player-a", HolderBootID: "boot-a",
		AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1, State: authoritystore.ShardAuthorityActive, LeaseUntilMs: start.Add(time.Minute).UnixMilli(),
	}
	guard, err := authoritystore.NewHolderLeaseGuard("player-a", "boot-a", 0, func() time.Time { return start })
	if err != nil {
		t.Fatal(err)
	}
	result := authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionApplied, Authority: authority, AcceptedTTLms: 60_000}
	if err := guard.InstallGrant(start, result); err != nil {
		t.Fatal(err)
	}
	registry := NewLeaseGuardRegistry()
	if err := registry.Install(actorrouting.DomainPlayer, 0, guard); err != nil {
		t.Fatal(err)
	}
	providerSnapshot := actorrouting.RouteSnapshot{
		Realm: "classic-farm", GlobalRevision: 1, HashVersion: calc.Profile().Version, HashSeed: calc.Profile().Seed,
		Player:  actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: start.Add(time.Minute).UnixMilli()}}},
		Farm:    actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{HolderIndex: 1, State: actorrouting.ShardStateAssigning, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1}}},
		Holders: []actorrouting.HolderRef{{InstanceID: "player-a", BootID: "boot-a", Endpoint: "player:9000", State: actorrouting.HolderStateActive, Weight: 1}, {InstanceID: "farm-a", BootID: "farm-a-boot", Endpoint: "farm:9001", State: actorrouting.HolderStateAssigning, Weight: 1}},
	}
	provider, _, err := actorrouting.NewDynamicAssignmentProvider(providerSnapshot, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewDynamicFenceSource(DynamicFenceSourceOptions{Calculator: calc, Snapshots: provider, Guards: registry, AuthorityLayoutVersion: "auth-v1", Now: func() time.Time { return start }}); !errors.Is(err, ErrLeaseAdmission) {
		t.Fatalf("profile-mismatched guard was installed: %v", err)
	}
}

func TestLeaseGuardHandleExpiryCannotBeRevivedBySameGeneration(t *testing.T) {
	start := time.UnixMilli(1_900_000_000_000)
	now := start
	guard, err := authoritystore.NewHolderLeaseGuard("player-a", "boot-a", 0, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	authority := authoritystore.ShardWriteAuthority{AuthorityLayoutVersion: "auth-v1", HashVersion: 2, HashSeed: 1, Domain: actorrouting.DomainPlayer, VirtualShard: 0, HolderID: "player-a", HolderBootID: "boot-a", AssignmentRevision: 4, WriterEpoch: 4, LeaseEpoch: 4, State: authoritystore.ShardAuthorityActive, LeaseUntilMs: start.Add(time.Second).UnixMilli()}
	result := authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionAlreadyApplied, Authority: authority, AcceptedTTLms: 1000}
	handle := newLeaseGuardHandle(guard, func() time.Time { return now })
	if err := handle.InstallGrant(start, result); err != nil {
		t.Fatal(err)
	}
	now = start.Add(2 * time.Second)
	if snapshot := handle.Snapshot(); snapshot.Admitted {
		t.Fatalf("expired handle remained admitted: %+v", snapshot)
	}
	// A replay carrying the stale requestStarted cannot revive the expired
	// guard: the rebuilt local deadline would already be in the past.
	if err := handle.InstallGrant(start, result); err == nil {
		t.Fatalf("stale-requestStarted replay revived the expired guard")
	}
	// RT1B-3B F1: while the authority lease is still valid, a same-generation
	// renewal with a fresh requestStarted re-arms the local deadline and
	// restores admission.
	if err := handle.InstallGrant(now, result); err != nil {
		t.Fatalf("valid same-generation renewal must rearm the guard: %v", err)
	}
	if snapshot := handle.Snapshot(); !snapshot.Admitted {
		t.Fatalf("rearmed guard must be admitted: %+v", snapshot)
	}
}

func TestLeaseGuardUsesConvertedLocalDeadlineAcrossWallClockJump(t *testing.T) {
	start := time.UnixMilli(1_900_000_000_000)
	authorityWall := start
	localMono := start
	guard, err := authoritystore.NewHolderLeaseGuard("player-a", "boot-a", 0, func() time.Time { return start })
	if err != nil {
		t.Fatal(err)
	}
	authority := authoritystore.ShardWriteAuthority{AuthorityLayoutVersion: "auth-v1", HashVersion: 2, HashSeed: 1, Domain: actorrouting.DomainPlayer, VirtualShard: 0, HolderID: "player-a", HolderBootID: "boot-a", AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1, State: authoritystore.ShardAuthorityActive, LeaseUntilMs: start.Add(time.Second).UnixMilli()}
	handle := newLeaseGuardHandleWithClocks(guard, func() time.Time { return authorityWall }, func() time.Time { return localMono })
	if err := handle.InstallGrant(start, authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionApplied, Authority: authority, AcceptedTTLms: 1000}); err != nil {
		t.Fatal(err)
	}
	// The trusted authority wall clock jumps forward after conversion. The
	// local monotonic deadline is unchanged, so the lease is not resurrected
	// or prematurely fenced by re-reading LeaseUntilMs.
	authorityWall = start.Add(10 * time.Minute)
	localMono = start.Add(500 * time.Millisecond)
	if snapshot := handle.Snapshot(); !snapshot.Admitted {
		t.Fatalf("wall-clock jump prematurely fenced local lease: %+v", snapshot)
	}
	localMono = start.Add(2 * time.Second)
	if snapshot := handle.Snapshot(); snapshot.Admitted {
		t.Fatalf("local monotonic deadline failed to expire lease: %+v", snapshot)
	}
}

func TestHolderLeaseRuntimeWithClocksUsesLocalDeadlineAfterWallJump(t *testing.T) {
	start := time.UnixMilli(1_900_000_000_000)
	authorityWall := start
	localMono := start
	runtime, err := NewHolderLeaseRuntimeWithClocks("player-a", "boot-a", 0, func() time.Time { return authorityWall }, func() time.Time { return localMono })
	if err != nil {
		t.Fatal(err)
	}
	authority := authoritystore.ShardWriteAuthority{AuthorityLayoutVersion: "auth-v1", HashVersion: 2, HashSeed: 1, Domain: actorrouting.DomainPlayer, VirtualShard: 0, HolderID: "player-a", HolderBootID: "boot-a", AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1, State: authoritystore.ShardAuthorityActive, LeaseUntilMs: start.Add(time.Second).UnixMilli()}
	if err := runtime.InstallGrant(localMono, authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionApplied, Authority: authority, AcceptedTTLms: 1000}); err != nil {
		t.Fatal(err)
	}
	authorityWall = start.Add(20 * time.Minute)
	localMono = start.Add(500 * time.Millisecond)
	if !runtime.CanStartActorTurn() {
		t.Fatalf("authority wall jump fenced locally valid runtime: %+v", runtime.Guard().Snapshot())
	}
	localMono = start.Add(2 * time.Second)
	if runtime.CanStartActorTurn() {
		t.Fatalf("local monotonic deadline did not expire runtime: %+v", runtime.Guard().Snapshot())
	}
}

func TestLeaseGuardRejectsSameGenerationLeaseDeadlineRegression(t *testing.T) {
	start := time.UnixMilli(1_900_000_000_000)
	now := start
	guard, err := authoritystore.NewHolderLeaseGuard("player-a", "boot-a", 0, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	handle := newLeaseGuardHandle(guard, func() time.Time { return now })
	base := authoritystore.ShardWriteAuthority{AuthorityLayoutVersion: "auth-v1", HashVersion: 2, HashSeed: 1, Domain: actorrouting.DomainPlayer, VirtualShard: 0, HolderID: "player-a", HolderBootID: "boot-a", AssignmentRevision: 4, WriterEpoch: 4, LeaseEpoch: 4, State: authoritystore.ShardAuthorityActive, LeaseUntilMs: start.Add(time.Minute).UnixMilli()}
	if err := handle.InstallGrant(start, authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionApplied, Authority: base, AcceptedTTLms: 60_000}); err != nil {
		t.Fatal(err)
	}
	regressed := base
	regressed.LeaseUntilMs = start.Add(30 * time.Second).UnixMilli()
	if err := handle.ObserveAuthority(regressed); !errors.Is(err, authoritystore.ErrStaleLeaseGrant) {
		t.Fatalf("same-generation observation deadline regression err=%v", err)
	}
	if handle.Snapshot().Admitted {
		t.Fatal("deadline regression did not fence the old runtime")
	}
}

func TestLeaseGuardHandleRejectsSameRevisionBootEpochMismatch(t *testing.T) {
	start := time.UnixMilli(1_900_000_000_000)
	guard, err := authoritystore.NewHolderLeaseGuard("player-a", "boot-a", 0, func() time.Time { return start })
	if err != nil {
		t.Fatal(err)
	}
	base := authoritystore.ShardWriteAuthority{AuthorityLayoutVersion: "auth-v1", HashVersion: 2, HashSeed: 1, Domain: actorrouting.DomainPlayer, VirtualShard: 0, HolderID: "player-a", HolderBootID: "boot-a", AssignmentRevision: 4, WriterEpoch: 4, LeaseEpoch: 4, State: authoritystore.ShardAuthorityActive, LeaseUntilMs: start.Add(time.Second).UnixMilli()}
	handle := newLeaseGuardHandle(guard, func() time.Time { return start })
	if err := handle.InstallGrant(start, authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionApplied, Authority: base, AcceptedTTLms: 1000}); err != nil {
		t.Fatal(err)
	}
	mismatch := base
	mismatch.HolderBootID = "boot-other"
	if err := handle.InstallGrant(start, authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionAlreadyApplied, Authority: mismatch, AcceptedTTLms: 1000}); err == nil {
		t.Fatal("same revision boot mismatch was accepted")
	}
	if snapshot := handle.Snapshot(); !snapshot.Admitted || snapshot.Authority.HolderBootID != "boot-a" {
		t.Fatalf("mismatched grant changed the active generation: %+v", snapshot)
	}
}

func TestDynamicFenceGuardFenceCannotRaceReturnedFence(t *testing.T) {
	start := time.UnixMilli(1_900_000_000_000)
	guard, err := authoritystore.NewHolderLeaseGuard("player-a", "boot-a", 0, func() time.Time { return start })
	if err != nil {
		t.Fatal(err)
	}
	authority := authoritystore.ShardWriteAuthority{AuthorityLayoutVersion: "auth-v1", HashVersion: 2, HashSeed: 1, Domain: actorrouting.DomainPlayer, VirtualShard: 0, HolderID: "player-a", HolderBootID: "boot-a", AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1, State: authoritystore.ShardAuthorityActive, LeaseUntilMs: start.Add(time.Minute).UnixMilli()}
	if err := guard.InstallGrant(start, authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionApplied, Authority: authority, AcceptedTTLms: 60_000}); err != nil {
		t.Fatal(err)
	}
	handle := newLeaseGuardHandle(guard, func() time.Time { return start })
	entered := make(chan struct{})
	release := make(chan struct{})
	fenceDone := make(chan struct{})
	returned := make(chan authoritystore.ShardWriteFence, 1)
	go func() {
		fence, fenceErr := handle.withFence(func(snapshot LeaseGenerationSnapshot) (authoritystore.ShardWriteFence, error) {
			close(entered)
			<-release
			return authoritystore.ShardWriteFence{ExpectedHolderID: snapshot.Authority.HolderID, ExpectedHolderBootID: snapshot.Authority.HolderBootID, ExpectedAssignmentRevision: snapshot.Authority.AssignmentRevision, ExpectedWriterEpoch: snapshot.Authority.WriterEpoch, ExpectedLeaseEpoch: snapshot.Authority.LeaseEpoch}, nil
		})
		if fenceErr != nil {
			t.Errorf("withFence: %v", fenceErr)
		}
		returned <- fence
	}()
	<-entered
	go func() {
		handle.Fence()
		close(fenceDone)
	}()
	select {
	case <-fenceDone:
		t.Fatal("Fence ran between guard admission and fence construction")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-returned
	select {
	case <-fenceDone:
	case <-time.After(time.Second):
		t.Fatal("Fence did not complete after coherent fence construction")
	}
	if handle.Snapshot().Admitted {
		t.Fatal("guard remained admitted after the serialized Fence")
	}
}

func TestLeaseGuardAdmissionValidatesCompleteGenerationTuple(t *testing.T) {
	start := time.UnixMilli(1_900_000_000_000)
	guard, err := authoritystore.NewHolderLeaseGuard("player-a", "boot-a", 0, func() time.Time { return start })
	if err != nil {
		t.Fatal(err)
	}
	authority := authoritystore.ShardWriteAuthority{AuthorityLayoutVersion: "auth-v1", HashVersion: 2, HashSeed: 1, Domain: actorrouting.DomainPlayer, VirtualShard: 2, HolderID: "player-a", HolderBootID: "boot-a", AssignmentRevision: 7, WriterEpoch: 9, LeaseEpoch: 11, State: authoritystore.ShardAuthorityActive, LeaseUntilMs: start.Add(time.Minute).UnixMilli()}
	if err := guard.InstallGrant(start, authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionApplied, Authority: authority, AcceptedTTLms: 60_000}); err != nil {
		t.Fatal(err)
	}
	guards := NewLeaseGuardRegistry()
	if err := guards.Install(actorrouting.DomainPlayer, 2, guard); err != nil {
		t.Fatal(err)
	}
	admission, err := NewLeaseGuardAdmission(guards)
	if err != nil {
		t.Fatal(err)
	}
	if !admission.ValidateRouteGeneration(actorrouting.DomainPlayer, 2, "player-a", "boot-a", 7, 9, 11) {
		t.Fatal("complete local generation was rejected")
	}
	if admission.ValidateRouteGeneration(actorrouting.DomainPlayer, 2, "player-a", "boot-a", 7, 9, 10) || admission.ValidateRouteGeneration(actorrouting.DomainFarm, 2, "player-a", "boot-a", 7, 9, 11) {
		t.Fatal("lease epoch/domain mismatch was accepted")
	}
}

func TestLeaseGuardRegistryRemoveAndReplaceRevokeOldHandleAndFloor(t *testing.T) {
	start := time.UnixMilli(1_900_000_000_000)
	authority := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "auth-v1", HashVersion: 2, HashSeed: 1,
		Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		HolderID: "player-a", HolderBootID: "boot-a", AssignmentRevision: 4,
		WriterEpoch: 4, LeaseEpoch: 4, State: authoritystore.ShardAuthorityActive,
		LeaseUntilMs: start.Add(time.Minute).UnixMilli(),
	}
	result := authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionApplied, Authority: authority, AcceptedTTLms: 60_000}
	registry := NewLeaseGuardRegistry()
	newGuard := func() *authoritystore.HolderLeaseGuard {
		guard, err := authoritystore.NewHolderLeaseGuard("player-a", "boot-a", 0, func() time.Time { return start })
		if err != nil {
			t.Fatal(err)
		}
		return guard
	}
	first, err := InstallHolderLeaseGrant(registry, actorrouting.DomainPlayer, 0, newGuard(), start, result, func() time.Time { return start })
	if err != nil || first == nil || !first.CanStartActorTurn() {
		t.Fatalf("first install runtime=%v err=%v", first, err)
	}
	old := first.Guard()
	registry.Remove(actorrouting.DomainPlayer, 0)
	if old.Snapshot().Admitted {
		t.Fatal("Remove left an already-held old runtime admitted")
	}
	if _, ok := registry.Guard(actorrouting.DomainPlayer, 0); ok {
		t.Fatal("Remove left a registry handle")
	}
	if _, err := InstallHolderLeaseGrant(registry, actorrouting.DomainPlayer, 0, newGuard(), start, result, func() time.Time { return start }); !errors.Is(err, authoritystore.ErrStaleLeaseGrant) {
		t.Fatalf("same generation reinstalled after Remove: %v", err)
	}

	newAuthority := authority
	newAuthority.AssignmentRevision = 5
	newAuthority.WriterEpoch = 5
	newAuthority.LeaseEpoch = 5
	newResult := result
	newResult.Authority = newAuthority
	replacement, err := InstallHolderLeaseGrant(registry, actorrouting.DomainPlayer, 0, newGuard(), start, newResult, func() time.Time { return start })
	if err != nil || replacement == nil || !replacement.CanStartActorTurn() {
		t.Fatalf("new generation install runtime=%v err=%v", replacement, err)
	}
	oldReplacement := replacement.Guard()
	newerAuthority := newAuthority
	newerAuthority.AssignmentRevision = 6
	newerAuthority.WriterEpoch = 6
	newerAuthority.LeaseEpoch = 6
	newerResult := newResult
	newerResult.Authority = newerAuthority
	latest, err := InstallHolderLeaseGrant(registry, actorrouting.DomainPlayer, 0, newGuard(), start, newerResult, func() time.Time { return start })
	if err != nil || latest == nil || !latest.CanStartActorTurn() {
		t.Fatalf("replacement install runtime=%v err=%v", latest, err)
	}
	if oldReplacement.Snapshot().Admitted {
		t.Fatal("Replace left the old runtime admitted")
	}
}

func TestLeaseGuardObserveRejectsNonNewerGenerationDifferences(t *testing.T) {
	start := time.UnixMilli(1_900_000_000_000)
	base := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "auth-v1", HashVersion: 2, HashSeed: 1,
		Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		HolderID: "player-a", HolderBootID: "boot-a", AssignmentRevision: 4,
		WriterEpoch: 4, LeaseEpoch: 4, State: authoritystore.ShardAuthorityActive,
		LeaseUntilMs: start.Add(time.Minute).UnixMilli(),
	}
	for name, mutate := range map[string]func(*authoritystore.ShardWriteAuthority){
		"lower revision":  func(a *authoritystore.ShardWriteAuthority) { a.AssignmentRevision = 3 },
		"boot mismatch":   func(a *authoritystore.ShardWriteAuthority) { a.HolderBootID = "boot-other" },
		"writer mismatch": func(a *authoritystore.ShardWriteAuthority) { a.WriterEpoch = 5 },
		"lease mismatch":  func(a *authoritystore.ShardWriteAuthority) { a.LeaseEpoch = 5 },
		"hash mismatch":   func(a *authoritystore.ShardWriteAuthority) { a.HashSeed++ },
		"state mismatch": func(a *authoritystore.ShardWriteAuthority) {
			a.State = authoritystore.ShardAuthorityUnassigned
			a.HolderID = ""
			a.HolderBootID = ""
			a.LeaseUntilMs = 0
		},
	} {
		t.Run(name, func(t *testing.T) {
			guard, err := authoritystore.NewHolderLeaseGuard("player-a", "boot-a", 0, func() time.Time { return start })
			if err != nil {
				t.Fatal(err)
			}
			handle := newLeaseGuardHandle(guard, func() time.Time { return start })
			if err := handle.InstallGrant(start, authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionApplied, Authority: base, AcceptedTTLms: 60_000}); err != nil {
				t.Fatal(err)
			}
			observed := base
			mutate(&observed)
			if err := handle.ObserveAuthority(observed); err == nil {
				t.Fatal("non-newer/mismatched observation was accepted")
			}
			if handle.Snapshot().Admitted {
				t.Fatalf("mismatched observation did not fence guard: %+v", handle.Snapshot())
			}
		})
	}
}

type mismatchedRenewStore struct {
	result authoritystore.ShardAuthorityTransitionResult
}

func (s mismatchedRenewStore) ClaimShardAuthority(context.Context, authoritystore.ClaimShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return authoritystore.ShardAuthorityTransitionResult{}, nil
}
func (s mismatchedRenewStore) RenewShardAuthority(context.Context, authoritystore.RenewShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return s.result, nil
}
func (s mismatchedRenewStore) TransferShardAuthority(context.Context, authoritystore.TransferShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return authoritystore.ShardAuthorityTransitionResult{}, nil
}
func (s mismatchedRenewStore) UnassignShardAuthority(context.Context, authoritystore.UnassignShardAuthorityRequest) (authoritystore.ShardAuthorityTransitionResult, error) {
	return authoritystore.ShardAuthorityTransitionResult{}, nil
}

func TestHolderLeaseRuntimeRejectsRenewGenerationChangeAndFences(t *testing.T) {
	start := time.UnixMilli(1_900_000_000_000)
	base := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "auth-v1", HashVersion: 2, HashSeed: 1,
		Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		HolderID: "player-a", HolderBootID: "boot-a", AssignmentRevision: 4,
		WriterEpoch: 4, LeaseEpoch: 4, State: authoritystore.ShardAuthorityActive,
		LeaseUntilMs: start.Add(time.Minute).UnixMilli(),
	}
	guard, err := authoritystore.NewHolderLeaseGuard("player-a", "boot-a", 0, func() time.Time { return start })
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewHolderLeaseRuntime(guard, func() time.Time { return start })
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.InstallGrant(start, authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionApplied, Authority: base, AcceptedTTLms: 60_000}); err != nil {
		t.Fatal(err)
	}
	newer := base
	newer.AssignmentRevision++
	newer.WriterEpoch++
	newer.LeaseEpoch++
	result := authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionApplied, Authority: newer, AcceptedTTLms: 60_000}
	_, err = runtime.Renew(context.Background(), mismatchedRenewStore{result: result}, authoritystore.RenewShardAuthorityRequest{
		Caller:                 authoritystore.TrustedShardAuthorityCaller{Kind: authoritystore.ShardAuthorityCallerHolder, InstanceID: "player-a", BootID: "boot-a"},
		AuthorityLayoutVersion: "auth-v1", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		ExpectedFence: fenceForTest(base), RequestedTTLms: 60_000,
	})
	if !errors.Is(err, ErrRenewGenerationChanged) || runtime.CanStartActorTurn() {
		t.Fatalf("renew generation change was adopted: err=%v admitted=%v", err, runtime.CanStartActorTurn())
	}
}

func fenceForTest(authority authoritystore.ShardWriteAuthority) authoritystore.ShardWriteFence {
	return authoritystore.ShardWriteFence{
		ExpectedHolderID: authority.HolderID, ExpectedHolderBootID: authority.HolderBootID,
		ExpectedAssignmentRevision: authority.AssignmentRevision,
		ExpectedWriterEpoch:        authority.WriterEpoch, ExpectedLeaseEpoch: authority.LeaseEpoch,
	}
}

// fenceClock is a goroutine-safe controllable clock shared by the guard and
// handle layers of the monotonic-fence tests.
type fenceClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fenceClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fenceClock) set(t time.Time) {
	c.mu.Lock()
	c.t = t
	c.mu.Unlock()
}

// monotonicFenceFixture installs one handle whose local safety deadline expires
// at start+25s (TTL 30s, safety margin 5s) while the authority lease stays
// valid until start+30s, and registers it in a fresh registry.
type monotonicFenceFixture struct {
	start     time.Time
	now       *fenceClock
	registry  *LeaseGuardRegistry
	handle    *LeaseGuardHandle
	authority authoritystore.ShardWriteAuthority
}

func newMonotonicFenceFixture(t *testing.T) *monotonicFenceFixture {
	t.Helper()
	start := time.UnixMilli(1_900_000_000_000)
	clock := &fenceClock{t: start}
	guard, err := authoritystore.NewHolderLeaseGuard("player-a", "boot-a", 5*time.Second, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	authority := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "auth-v1", HashVersion: 2, HashSeed: 1,
		Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		HolderID: "player-a", HolderBootID: "boot-a",
		AssignmentRevision: 4, WriterEpoch: 4, LeaseEpoch: 4,
		State: authoritystore.ShardAuthorityActive, LeaseUntilMs: start.Add(30 * time.Second).UnixMilli(),
	}
	handle := newLeaseGuardHandle(guard, clock.Now)
	if err := handle.InstallGrant(start, authoritystore.ShardAuthorityTransitionResult{Status: authoritystore.ShardTransitionApplied, Authority: authority, AcceptedTTLms: 30_000}); err != nil {
		t.Fatal(err)
	}
	registry := NewLeaseGuardRegistry()
	if err := registry.installHandle(actorrouting.DomainPlayer, 0, handle); err != nil {
		t.Fatal(err)
	}
	return &monotonicFenceFixture{start: start, now: clock, registry: registry, handle: handle, authority: authority}
}

func (f *monotonicFenceFixture) preparedRenew() PreparedLeaseRenew {
	return PreparedLeaseRenew{RequestStarted: f.now.Now(), AcceptedTTLMs: 30_000, Authority: f.authority}
}

func TestMembershipRemovedReasonCannotBeDowngradedByDeadlineExpiry(t *testing.T) {
	f := newMonotonicFenceFixture(t)
	// Pass the local safety deadline (start+25s) while the authority lease
	// (start+30s) is still valid: admission is paused but recoverable.
	f.now.set(f.start.Add(26 * time.Second))
	if f.handle.Snapshot().Admitted {
		t.Fatal("must be fail-closed after the local safety deadline")
	}
	// A REMOVE fence must not be downgraded to a recoverable deadline expiry.
	f.handle.FenceForReason(LeaseFenceMembershipRemoved)
	if reason := f.handle.FenceReason(); reason != LeaseFenceMembershipRemoved {
		t.Fatalf("MembershipRemoved downgraded to %v", reason)
	}
	// Any later hot-path snapshot must keep the terminal reason.
	f.handle.Snapshot()
	if reason := f.handle.FenceReason(); reason != LeaseFenceMembershipRemoved {
		t.Fatalf("deadline snapshot downgraded terminal reason to %v", reason)
	}
	// The terminal guard can never be rearmed by a same-generation renewal.
	if err := f.registry.RefreshOrRearmIfMatches(actorrouting.DomainPlayer, 0, f.authority, f.preparedRenew()); err == nil {
		t.Fatal("MembershipRemoved guard was rearmed by a same-generation renewal")
	}
	if f.handle.Snapshot().Admitted {
		t.Fatal("removed membership must stay fenced")
	}
}

func TestLostGenerationReasonCannotBeDowngraded(t *testing.T) {
	f := newMonotonicFenceFixture(t)
	f.now.set(f.start.Add(26 * time.Second))
	f.handle.FenceForReason(LeaseFenceLostGeneration)
	if reason := f.handle.FenceReason(); reason != LeaseFenceLostGeneration {
		t.Fatalf("LostGeneration downgraded to %v", reason)
	}
	f.handle.Snapshot()
	if reason := f.handle.FenceReason(); reason != LeaseFenceLostGeneration {
		t.Fatalf("deadline snapshot downgraded terminal reason to %v", reason)
	}
	if err := f.registry.RefreshOrRearmIfMatches(actorrouting.DomainPlayer, 0, f.authority, f.preparedRenew()); err == nil {
		t.Fatal("LostGeneration guard was rearmed by a same-generation renewal")
	}
	if f.handle.Snapshot().Admitted {
		t.Fatal("lost generation must stay fenced")
	}
}

func TestRuntimeFatalCannotBeRearmed(t *testing.T) {
	f := newMonotonicFenceFixture(t)
	f.now.set(f.start.Add(26 * time.Second))
	f.handle.FenceForReason(LeaseFenceRuntimeFatal)
	if reason := f.handle.FenceReason(); reason != LeaseFenceRuntimeFatal {
		t.Fatalf("RuntimeFatal downgraded to %v", reason)
	}
	if err := f.registry.RefreshOrRearmIfMatches(actorrouting.DomainPlayer, 0, f.authority, f.preparedRenew()); err == nil {
		t.Fatal("RuntimeFatal guard was rearmed by a same-generation renewal")
	}
	if f.handle.Snapshot().Admitted {
		t.Fatal("fatal guard must stay fenced")
	}
}

func TestRefreshOrRearmRejectsEveryTerminalFenceReason(t *testing.T) {
	for name, reason := range map[string]LeaseFenceReason{
		"membership removed": LeaseFenceMembershipRemoved,
		"lost generation":    LeaseFenceLostGeneration,
		"runtime fatal":      LeaseFenceRuntimeFatal,
	} {
		t.Run(name, func(t *testing.T) {
			f := newMonotonicFenceFixture(t)
			f.now.set(f.start.Add(26 * time.Second))
			f.handle.FenceForReason(reason)
			if err := f.registry.RefreshOrRearmIfMatches(actorrouting.DomainPlayer, 0, f.authority, f.preparedRenew()); err == nil {
				t.Fatalf("%v guard was rearmed by a same-generation renewal", reason)
			}
			if f.handle.Snapshot().Admitted {
				t.Fatalf("%v guard must stay fenced", reason)
			}
		})
	}
}

func TestTerminalFenceReasonWinsConcurrentDeadlineExpiry(t *testing.T) {
	f := newMonotonicFenceFixture(t)
	// Establish the terminal reason first so the concurrent deadline snapshots
	// race a sticky terminal fence rather than a plain expiry.
	f.handle.FenceForReason(LeaseFenceMembershipRemoved)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 200 {
				f.handle.Snapshot()
			}
		})
	}
	// While snapshots race, the local deadline passes: snapshotLocked attempts
	// to promote LocalDeadlineExpired, which the sticky terminal reason must
	// reject on every interleaving.
	f.now.set(f.start.Add(30 * time.Second))
	wg.Wait()
	if reason := f.handle.FenceReason(); reason != LeaseFenceMembershipRemoved {
		t.Fatalf("concurrent deadline expiry downgraded terminal reason to %v", reason)
	}
	if f.handle.Snapshot().Admitted {
		t.Fatal("removed membership must stay fenced")
	}
}
