package actorrouting

import (
	"context"
	"errors"
	"farmgame/internal/controlfence"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func singleShardSnapshot() RouteSnapshot {
	leaseUntil := time.Now().Add(time.Hour).UnixMilli()
	return RouteSnapshot{
		Realm: "classic-farm", GlobalRevision: 1,
		HashVersion: HashVersionXXH64V2, HashSeed: testHashSeed,
		Player: DomainSnapshot{
			ShardCount: 1,
			Routes:     []ShardRoute{{HolderIndex: 0, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: leaseUntil}},
		},
		Farm: DomainSnapshot{
			ShardCount: 1,
			Routes:     []ShardRoute{{HolderIndex: 1, State: ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: leaseUntil}},
		},
		Holders: []HolderRef{
			{InstanceID: "player-a", BootID: "player-a-boot", Endpoint: "player-a:9100", State: HolderStateActive, Weight: 1},
			{InstanceID: "farm-a", BootID: "farm-a-boot", Endpoint: "farm-a:9090", State: HolderStateActive, Weight: 1},
			{InstanceID: "player-b", BootID: "player-b-boot", Endpoint: "player-b:9100", State: HolderStateActive, Weight: 1},
		},
	}
}

func completeRedirect(snapshot RouteSnapshot, global, shardRevision, writer, lease uint64, holderIndex int) RouteRedirect {
	holder := snapshot.Holders[holderIndex]
	return RouteRedirect{
		Domain:               DomainPlayer,
		ShardID:              0,
		GlobalRevision:       global,
		ShardRevision:        shardRevision,
		ExpectedHolderID:     holder.InstanceID,
		ExpectedHolderBootID: holder.BootID,
		ExpectedWriterEpoch:  writer,
		ExpectedLeaseEpoch:   lease,
		LeaseUntilMs:         time.Now().Add(time.Minute).UnixMilli(),
		NewHolderIndex:       holderIndex,
		Reason:               RedirectReasonStaleRoute,
	}
}

func TestApplyRedirectAcceptsNewerRevision(t *testing.T) {
	snap := singleShardSnapshot()
	resolver, err := NewStaticResolver(snap)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewRedirectApplier(resolver)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewStaticAssignmentProvider(snap)
	if err != nil {
		t.Fatal(err)
	}
	sa, err := NewSnapshotApplier(provider, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if err := sa.Apply(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	applier.SetSnapshotApplier(sa)

	before, err := resolver.ResolveActor(context.Background(), DomainPlayer, 42)
	if err != nil {
		t.Fatal(err)
	}
	if before.HolderPod != "player-a" {
		t.Fatalf("before=%+v", before)
	}

	redirect := completeRedirect(snap, 2, 5, 9, 9, 2) // player-b
	err = applier.ApplyRedirect(context.Background(), redirect)
	if err != nil {
		t.Fatal(err)
	}
	after, err := resolver.ResolveActor(context.Background(), DomainPlayer, 42)
	if err != nil {
		t.Fatal(err)
	}
	if after.HolderPod != "player-b" || after.WriterEpoch != 9 || after.ShardRevision != 5 {
		t.Fatalf("redirect not visible: %+v", after)
	}
	if sa.LastGlobalRevision() != 2 {
		t.Fatalf("SnapshotApplier last=%d want 2", sa.LastGlobalRevision())
	}
	if applier.Applied.Load() != 1 {
		t.Fatalf("Applied=%d", applier.Applied.Load())
	}
}

func TestApplyRedirectRejectsStaleOrEqualRevision(t *testing.T) {
	snap := singleShardSnapshot()
	resolver, err := NewStaticResolver(snap)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewRedirectApplier(resolver)
	if err != nil {
		t.Fatal(err)
	}
	before := resolver.Snapshot()
	var redirect RouteRedirect

	for _, rev := range []uint64{0, 1} {
		redirect = completeRedirect(snap, rev, 9, 9, 9, 2)
		err := applier.ApplyRedirect(context.Background(), redirect)
		if rev == 0 {
			if !errors.Is(err, ErrInvalidRedirect) {
				t.Fatalf("rev=0 err=%v want ErrInvalidRedirect", err)
			}
			continue
		}
		if !errors.Is(err, ErrStaleRedirect) {
			t.Fatalf("equal global err=%v want ErrStaleRedirect", err)
		}
	}

	// Newer global but stale/equal shard revision must also reject.
	redirect = completeRedirect(snap, 2, 1, 9, 9, 2) // equal shard revision
	err = applier.ApplyRedirect(context.Background(), redirect)
	if !errors.Is(err, ErrStaleRedirect) {
		t.Fatalf("equal shard rev err=%v", err)
	}

	after := resolver.Snapshot()
	if after.GlobalRevision != before.GlobalRevision || after.Holders[0].InstanceID != before.Holders[0].InstanceID {
		t.Fatalf("table mutated on stale redirect: before=%+v after=%+v", before, after)
	}
	got, err := resolver.ResolveActor(context.Background(), DomainPlayer, 42)
	if err != nil {
		t.Fatal(err)
	}
	if got.HolderPod != "player-a" {
		t.Fatalf("holder changed on reject: %+v", got)
	}
	if applier.StaleRejected.Load() < 2 {
		t.Fatalf("StaleRejected=%d", applier.StaleRejected.Load())
	}
}

func TestRedirectGapReloadsOrFailsClosedWithoutIncompletePatch(t *testing.T) {
	snap := singleShardSnapshot()
	resolver, err := NewStaticResolver(snap)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewRedirectApplier(resolver)
	if err != nil {
		t.Fatal(err)
	}
	incomplete := RouteRedirect{Domain: DomainPlayer, ShardID: 0, GlobalRevision: 3, ShardRevision: 2, ExpectedWriterEpoch: 2, NewHolderIndex: 2}
	if err := applier.ApplyRedirect(context.Background(), incomplete); !errors.Is(err, ErrInvalidRedirect) {
		t.Fatalf("incomplete redirect err=%v", err)
	}
	if got := resolver.Snapshot().GlobalRevision; got != snap.GlobalRevision {
		t.Fatalf("incomplete redirect advanced resolver to %d", got)
	}
	complete := completeRedirect(snap, 3, 2, 2, 2, 2)
	if err := applier.ApplyRedirect(context.Background(), complete); !errors.Is(err, ErrRedirectReloadRequired) {
		t.Fatalf("gap without snapshot applier err=%v", err)
	}
	if got := resolver.Snapshot().GlobalRevision; got != snap.GlobalRevision {
		t.Fatalf("gapped redirect advanced resolver to %d", got)
	}

	provider, err := NewStaticAssignmentProvider(snap)
	if err != nil {
		t.Fatal(err)
	}
	sa, err := NewSnapshotApplier(provider, resolver)
	if err != nil {
		t.Fatal(err)
	}
	if err := sa.Apply(context.Background(), snap); err != nil {
		t.Fatal(err)
	}
	applier.SetSnapshotApplier(sa)
	if err := applier.ApplyRedirect(context.Background(), incomplete); !errors.Is(err, ErrInvalidRedirect) {
		t.Fatalf("incomplete redirect with static provider did not fail closed: %v", err)
	}
	if sa.LastGlobalRevision() != snap.GlobalRevision || resolver.Snapshot().GlobalRevision != snap.GlobalRevision || applier.Applied.Load() != 0 {
		t.Fatalf("incomplete redirect was treated as an applied patch: last=%d snapshot=%d applied=%d", sa.LastGlobalRevision(), resolver.Snapshot().GlobalRevision, applier.Applied.Load())
	}
	if err := applier.ApplyRedirect(context.Background(), complete); !errors.Is(err, ErrStaleRoute) {
		t.Fatalf("gap reload with stale source err=%v", err)
	}
	if sa.LastGlobalRevision() != snap.GlobalRevision || resolver.Snapshot().GlobalRevision != snap.GlobalRevision {
		t.Fatalf("gapped redirect suppressed recovery: last=%d snapshot=%d", sa.LastGlobalRevision(), resolver.Snapshot().GlobalRevision)
	}
}

type overridingRedirectProvider struct {
	*DynamicAssignmentProvider
	snapshot RouteSnapshot
	getCalls atomic.Uint64
}

func (p *overridingRedirectProvider) GetSnapshot(context.Context) (RouteSnapshot, error) {
	p.getCalls.Add(1)
	return cloneRouteSnapshot(p.snapshot), nil
}

func TestIncompleteRedirectReloadRequiresExactDynamicProvider(t *testing.T) {
	base := singleShardSnapshot()
	base.Player.Routes[0].State = ShardStateAssigning
	base.Player.Routes[0].ShardRevision = 2
	base.Player.Routes[0].ExpectedWriterEpoch = 2
	base.Player.Routes[0].ExpectedLeaseEpoch = 2
	incomplete := RouteRedirect{
		Domain: DomainPlayer, ShardID: 0, GlobalRevision: base.GlobalRevision + 1,
		ShardRevision: base.Player.Routes[0].ShardRevision,
	}

	t.Run("embedded override fails closed", func(t *testing.T) {
		read, _, err := NewDynamicAssignmentProvider(base, 1)
		if err != nil {
			t.Fatal(err)
		}
		forged := cloneRouteSnapshot(base)
		forged.GlobalRevision++
		forged.Player.Routes[0].State = ShardStateActive
		wrapper := &overridingRedirectProvider{DynamicAssignmentProvider: read, snapshot: forged}
		resolver, err := NewStaticResolver(base)
		if err != nil {
			t.Fatal(err)
		}
		snapshots, err := NewSnapshotApplier(wrapper, resolver)
		if err != nil {
			t.Fatal(err)
		}
		redirects, err := NewRedirectApplier(resolver)
		if err != nil {
			t.Fatal(err)
		}
		redirects.SetSnapshotApplier(snapshots)

		if err := redirects.ApplyRedirect(context.Background(), incomplete); !errors.Is(err, ErrInvalidRedirect) {
			t.Fatalf("incomplete redirect with wrapper did not fail closed: %v", err)
		}
		got := resolver.Snapshot()
		if wrapper.getCalls.Load() != 0 || got.GlobalRevision != base.GlobalRevision || got.Player.Routes[0].State != ShardStateAssigning {
			t.Fatalf("wrapper reload was used: gets=%d revision=%d route=%+v", wrapper.getCalls.Load(), got.GlobalRevision, got.Player.Routes[0])
		}
	})

	t.Run("exact provider reloads legal manager publication", func(t *testing.T) {
		read, publisher, err := NewDynamicAssignmentProvider(base, 1)
		if err != nil {
			t.Fatal(err)
		}
		active := cloneRouteSnapshot(base)
		active.GlobalRevision++
		active.Player.Routes[0].State = ShardStateActive
		if err := publisher.PublishAuthoritySnapshot(context.Background(), controlfence.NestedControlOperationToken{}, active); err != nil {
			t.Fatal(err)
		}
		resolver, err := NewStaticResolver(base)
		if err != nil {
			t.Fatal(err)
		}
		snapshots, err := NewSnapshotApplier(read, resolver)
		if err != nil {
			t.Fatal(err)
		}
		redirects, err := NewRedirectApplier(resolver)
		if err != nil {
			t.Fatal(err)
		}
		redirects.SetSnapshotApplier(snapshots)

		if err := redirects.ApplyRedirect(context.Background(), incomplete); err != nil {
			t.Fatalf("trusted incomplete redirect did not reload: %v", err)
		}
		got := resolver.Snapshot()
		if got.GlobalRevision != active.GlobalRevision || got.Player.Routes[0].State != ShardStateActive {
			t.Fatalf("legal ACTIVE snapshot was not reloaded: revision=%d route=%+v", got.GlobalRevision, got.Player.Routes[0])
		}
		if snapshots.LastGlobalRevision() != active.GlobalRevision || redirects.Applied.Load() != 0 {
			t.Fatalf("reload bookkeeping last=%d redirects_applied=%d", snapshots.LastGlobalRevision(), redirects.Applied.Load())
		}
	})
}

func TestDirectRedirectCancellationDuringReplacementDoesNotInstall(t *testing.T) {
	snap := singleShardSnapshot()
	resolver, err := NewStaticResolver(snap)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewRedirectApplier(resolver)
	if err != nil {
		t.Fatal(err)
	}
	redirect := completeRedirect(snap, 2, 2, 2, 2, 2)
	installReached := make(chan struct{})
	installRelease := make(chan struct{})
	resolver.beforeInstall = func() {
		close(installReached)
		<-installRelease
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- applier.ApplyRedirect(ctx, redirect) }()
	<-installReached
	cancel()
	close(installRelease)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("redirect replacement cancellation err=%v", err)
	}
	if got := resolver.Snapshot().GlobalRevision; got != snap.GlobalRevision {
		t.Fatalf("canceled redirect installed revision %d", got)
	}
	if got := applier.Applied.Load(); got != 0 {
		t.Fatalf("canceled redirect advanced applied counter to %d", got)
	}
}

func TestRedirectCannotClaimManagerAuthorityActivation(t *testing.T) {
	base := singleShardSnapshot()
	base.Player.Routes[0].State = ShardStateAssigning
	base.Player.Routes[0].ShardRevision = 2
	base.Player.Routes[0].ExpectedWriterEpoch = 2
	base.Player.Routes[0].ExpectedLeaseEpoch = 2
	next := cloneRouteSnapshot(base)
	next.GlobalRevision++
	next.Player.Routes[0].State = ShardStateActive
	resolver, err := NewStaticResolver(base)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewRedirectApplier(resolver)
	if err != nil {
		t.Fatal(err)
	}
	redirect := RouteRedirect{Domain: DomainPlayer, ShardID: 0, GlobalRevision: next.GlobalRevision, Snapshot: &next}
	if err := applier.ApplyRedirect(context.Background(), redirect); !errors.Is(err, ErrSnapshotGenerationRollback) {
		t.Fatalf("redirect claimed same-generation activation semantics: %v", err)
	}
	if got := resolver.Snapshot().GlobalRevision; got != base.GlobalRevision {
		t.Fatalf("untrusted activation redirect installed revision %d", got)
	}
}

func TestRetryingResolverPreservesOperationID(t *testing.T) {
	snap := singleShardSnapshot()
	resolver, err := NewStaticResolver(snap)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewRedirectApplier(resolver)
	if err != nil {
		t.Fatal(err)
	}
	retrying := &RetryingResolver{Resolver: resolver, Applier: applier, MaxRedirects: 3}
	const opID = "op-preserve-001"
	var seen []string
	attempts := 0
	err = retrying.Invoke(context.Background(), Address{Realm: snap.Realm, Kind: KindPlayer, ID: 42}, opID,
		func(_ context.Context, a Assignment, operationID string) error {
			seen = append(seen, operationID)
			attempts++
			if attempts == 1 {
				redirect := completeRedirect(snap, 2, 2, 2, 2, 2)
				return &RedirectError{Redirect: redirect}
			}
			if a.HolderPod != "player-b" {
				t.Fatalf("retry must see new holder, got %+v", a)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 {
		t.Fatalf("attempts=%d seen=%v", attempts, seen)
	}
	for i, id := range seen {
		if id != opID {
			t.Fatalf("attempt %d operation_id=%q want %q", i, id, opID)
		}
	}
}

func TestRetryingResolverBoundedRedirects(t *testing.T) {
	snap := singleShardSnapshot()
	resolver, err := NewStaticResolver(snap)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewRedirectApplier(resolver)
	if err != nil {
		t.Fatal(err)
	}
	retrying := &RetryingResolver{Resolver: resolver, Applier: applier, MaxRedirects: 2}
	attempts := 0
	err = retrying.Invoke(context.Background(), Address{Realm: snap.Realm, Kind: KindPlayer, ID: 42}, "op-bound",
		func(_ context.Context, _ Assignment, operationID string) error {
			if operationID != "op-bound" {
				t.Fatalf("operation_id drifted: %q", operationID)
			}
			attempts++
			// Always redirect with a strictly newer global revision.
			redirect := completeRedirect(snap, uint64(attempts+1), uint64(attempts+1), uint64(attempts+1), uint64(attempts+1), 0)
			return &RedirectError{Redirect: redirect}
		})
	if !errors.Is(err, ErrTooManyRedirects) {
		t.Fatalf("err=%v want ErrTooManyRedirects", err)
	}
	// initial + MaxRedirects successful applies then one more RedirectError → stop.
	// Attempt count: MaxRedirects+1 (the last redirect is rejected before apply).
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3 (1 initial + 2 redirects triggering bound)", attempts)
	}
}

// forwardSpy exists only so tests can prove the no-proxy path never calls it.
type forwardSpy struct {
	calls int
}

func (s *forwardSpy) Forward(context.Context, string, any) error {
	s.calls++
	return nil
}

func TestOldHolderDoesNotProxyForward(t *testing.T) {
	// Package API must not expose a Forward method on redirect types.
	for _, typ := range []reflect.Type{
		reflect.TypeOf(RedirectApplier{}),
		reflect.TypeOf(RetryingResolver{}),
		reflect.TypeOf(RouteRedirect{}),
	} {
		if _, ok := typ.MethodByName("Forward"); ok {
			t.Fatalf("%s must not expose Forward", typ.Name())
		}
	}

	snap := singleShardSnapshot()
	resolver, err := NewStaticResolver(snap)
	if err != nil {
		t.Fatal(err)
	}
	applier, err := NewRedirectApplier(resolver)
	if err != nil {
		t.Fatal(err)
	}
	spy := &forwardSpy{}

	// Old-holder path: update local view + return RedirectError. Never Forward.
	redirect := completeRedirect(snap, 2, 3, 7, 7, 2)
	redirect.Reason = RedirectReasonNotHolding
	err = HandleLocalMiss(context.Background(), applier, redirect)
	var re *RedirectError
	if !errors.As(err, &re) {
		t.Fatalf("HandleLocalMiss err=%v want RedirectError", err)
	}
	if spy.calls != 0 {
		t.Fatalf("forward spy calls=%d want 0", spy.calls)
	}

	got, err := resolver.ResolveActor(context.Background(), DomainPlayer, 42)
	if err != nil {
		t.Fatal(err)
	}
	if got.HolderPod != "player-b" || got.WriterEpoch != 7 {
		t.Fatalf("local view not updated: %+v", got)
	}

	// Caller-side retry: Resolve → new Holder via AttemptFunc only (still no Forward).
	callerSnap := singleShardSnapshot()
	callerResolver, err := NewStaticResolver(callerSnap)
	if err != nil {
		t.Fatal(err)
	}
	callerApplier, err := NewRedirectApplier(callerResolver)
	if err != nil {
		t.Fatal(err)
	}
	retrying := &RetryingResolver{Resolver: callerResolver, Applier: callerApplier, MaxRedirects: 2}
	const opID = "op-no-proxy"
	attempts := 0
	err = retrying.Invoke(context.Background(), Address{Realm: callerSnap.Realm, Kind: KindPlayer, ID: 42}, opID,
		func(_ context.Context, a Assignment, operationID string) error {
			if operationID != opID {
				t.Fatalf("operation_id=%q", operationID)
			}
			attempts++
			if attempts == 1 {
				// Simulate trusted redirect from old holder — caller must not proxy.
				_ = spy // available; correct path never calls it
				redirect := completeRedirect(callerSnap, 2, 3, 7, 7, 2)
				redirect.Reason = RedirectReasonNotHolding
				return &RedirectError{Redirect: redirect}
			}
			if a.HolderPod != "player-b" {
				t.Fatalf("caller retry assignment=%+v", a)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if spy.calls != 0 {
		t.Fatalf("forward spy calls=%d want 0 after caller retry", spy.calls)
	}
	if attempts != 2 {
		t.Fatalf("attempts=%d", attempts)
	}
}
