package control

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	core "farmgame/internal/scaleplan"
	testkit "farmgame/internal/testkit/scaleplancontrol"
)

func testCoordinatorScope() core.MutationScope {
	return core.MutationScope{Namespace: "farm", Realm: "realm-a"}
}

func testCoordinatorOwner(id string) core.MutationLeaseOwner {
	return core.MutationLeaseOwner{
		ControllerID:     id,
		ControllerBootID: "boot-" + id,
		OperationNonce:   "nonce-" + id,
	}
}

func testCoordinatorRequest(id string, scope core.MutationScope, owner core.MutationLeaseOwner) core.ControlOperationRequest {
	return core.ControlOperationRequest{
		OperationID: id,
		Kind:        core.ControlOperationScalePlan,
		Scope:       scope,
		Owner:       owner,
	}
}

func newTestCoordinator(t *testing.T, clock *testkit.ManualClock, ttl time.Duration) ExclusiveControlCoordinator {
	t.Helper()
	coordinator, err := NewControlMutationCoordinator(ControlMutationCoordinatorOptions{
		Scope:    scopeForTest(),
		LeaseTTL: ttl,
		Now:      clock.Now,
		Epoch:    7,
	})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator
}

func TestControlCoordinatorAcceptsLongBoundedScalePlanLease(t *testing.T) {
	clock := testkit.NewManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	if _, err := NewControlMutationCoordinator(ControlMutationCoordinatorOptions{
		Scope: scopeForTest(), LeaseTTL: 30*time.Minute + 30*time.Second, Now: clock.Now, Epoch: 7,
	}); err != nil {
		t.Fatalf("long bounded coordinator lease rejected: %v", err)
	}
	if _, err := NewControlMutationCoordinator(ControlMutationCoordinatorOptions{
		Scope: scopeForTest(), LeaseTTL: maxCoordinatorLeaseTTL + time.Millisecond, Now: clock.Now, Epoch: 7,
	}); !errors.Is(err, ErrCoordinatorInvalid) {
		t.Fatalf("oversized coordinator lease error=%v, want invalid", err)
	}
}

func scopeForTest() core.MutationScope {
	return testCoordinatorScope()
}

func TestControlCoordinatorRejectsForgedForeignAndResolvedTokens(t *testing.T) {
	ctx := context.Background()
	scope := testCoordinatorScope()
	clock := testkit.NewManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	coordinator := newTestCoordinator(t, clock, time.Second)
	request := testCoordinatorRequest("operation-a", scope, testCoordinatorOwner("controller-a"))

	token, err := coordinator.Acquire(ctx, request)
	if err != nil || !token.Issued() {
		t.Fatalf("acquire token=%v err=%v", token, err)
	}
	if err := coordinator.Verify(ctx, token, scope); err != nil {
		t.Fatalf("verify issued token: %v", err)
	}
	if err := coordinator.Verify(ctx, core.ControlOperationToken{}, scope); !errors.Is(err, ErrCoordinatorForeignToken) {
		t.Fatalf("zero token error=%v, want foreign-token", err)
	}

	other, err := NewControlMutationCoordinator(ControlMutationCoordinatorOptions{
		Scope: scope, LeaseTTL: time.Second, Now: clock.Now, Epoch: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := other.Acquire(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Verify(ctx, foreign, scope); !errors.Is(err, ErrCoordinatorForeignToken) {
		t.Fatalf("foreign issuer error=%v, want foreign-token", err)
	}

	if err := coordinator.Resolve(ctx, token); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if err := coordinator.Resolve(ctx, token); err != nil {
		t.Fatalf("idempotent resolve: %v", err)
	}
	if mode, err := coordinator.Mode(ctx, scope); err != nil || mode != core.ControlGateOpen {
		t.Fatalf("mode=%s err=%v, want OPEN", mode, err)
	}
	if err := coordinator.Verify(ctx, token, scope); !errors.Is(err, ErrCoordinatorResolved) {
		t.Fatalf("resolved token error=%v, want resolved", err)
	}
}

func TestControlCoordinatorClosingAllowsOnlyOwnerResolution(t *testing.T) {
	ctx := context.Background()
	scope := testCoordinatorScope()
	owner := testCoordinatorOwner("controller-a")
	clock := testkit.NewManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	coordinator := newTestCoordinator(t, clock, time.Second)

	active, err := coordinator.Acquire(ctx, testCoordinatorRequest("active", scope, owner))
	if err != nil {
		t.Fatal(err)
	}
	exclusiveRequest := testCoordinatorRequest("exclusive", scope, owner)
	exclusiveContext, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		fence      ExclusiveControlFence
		acquireErr error
	)
	done := make(chan struct{})
	go func() {
		fence, acquireErr = coordinator.AcquireExclusive(exclusiveContext, exclusiveRequest)
		close(done)
	}()

	waitForCoordinatorMode(t, coordinator, ctx, scope, core.ControlGateClosing)
	if _, err := coordinator.Acquire(ctx, testCoordinatorRequest("new-writer", scope, owner)); !errors.Is(err, ErrCoordinatorClosing) {
		t.Fatalf("new writer error=%v, want closing", err)
	}
	foreignResolution := testCoordinatorRequest("foreign-resolution", scope, testCoordinatorOwner("controller-b"))
	foreignResolution.ResolutionOnly = true
	if _, err := coordinator.Acquire(ctx, foreignResolution); !errors.Is(err, ErrCoordinatorClosing) {
		t.Fatalf("foreign resolution error=%v, want closing", err)
	}

	resolution := testCoordinatorRequest("resolution", scope, owner)
	resolution.Kind = core.ControlOperationResolveOnly
	resolution.ResolutionOnly = true
	resolutionToken, err := coordinator.Acquire(ctx, resolution)
	if err != nil {
		t.Fatalf("owner resolution acquire: %v", err)
	}
	if err := coordinator.Resolve(ctx, resolutionToken); err != nil {
		t.Fatalf("owner resolution resolve: %v", err)
	}
	if err := coordinator.Resolve(ctx, active); err != nil {
		t.Fatalf("active resolve: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("exclusive acquire did not complete after active operations resolved")
	}
	if acquireErr != nil || fence == nil {
		t.Fatalf("exclusive fence=%v err=%v", fence, acquireErr)
	}
	if mode, err := coordinator.Mode(ctx, scope); err != nil || mode != core.ControlGateExclusive {
		t.Fatalf("mode=%s err=%v, want EXCLUSIVE", mode, err)
	}
	tokenSource, ok := fence.(ExclusiveControlTokenSource)
	if !ok || !tokenSource.ControlToken().Issued() {
		t.Fatalf("exclusive fence did not expose an issued inherited token")
	}
	if err := coordinator.Verify(ctx, tokenSource.ControlToken(), scope); err != nil {
		t.Fatalf("verify exclusive inherited token: %v", err)
	}
	if err := fence.Revalidate(ctx); err != nil {
		t.Fatalf("exclusive revalidate: %v", err)
	}
	if err := fence.Release(ctx); err != nil {
		t.Fatalf("exclusive release: %v", err)
	}
	if err := fence.Release(ctx); err != nil {
		t.Fatalf("idempotent exclusive release: %v", err)
	}
}

func TestControlCoordinatorExpiryFailsClosed(t *testing.T) {
	ctx := context.Background()
	scope := testCoordinatorScope()
	clock := testkit.NewManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	coordinator := newTestCoordinator(t, clock, time.Second)
	token, err := coordinator.Acquire(ctx, testCoordinatorRequest("expiring", scope, testCoordinatorOwner("controller-a")))
	if err != nil {
		t.Fatal(err)
	}

	clock.Advance(2 * time.Second)
	if err := coordinator.Verify(ctx, token, scope); !errors.Is(err, ErrCoordinatorExpired) {
		t.Fatalf("expired verify error=%v, want expired", err)
	}
	if _, err := coordinator.Mode(ctx, scope); !errors.Is(err, ErrCoordinatorExpired) {
		t.Fatalf("expired mode error=%v, want expired", err)
	}
	if _, err := coordinator.Acquire(ctx, testCoordinatorRequest("new", scope, testCoordinatorOwner("controller-a"))); !errors.Is(err, ErrCoordinatorExpired) {
		t.Fatalf("acquire with expired active set error=%v, want expired", err)
	}
}

func TestControlCoordinatorExclusiveExpiryFailsClosed(t *testing.T) {
	ctx := context.Background()
	scope := testCoordinatorScope()
	clock := testkit.NewManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	coordinator := newTestCoordinator(t, clock, time.Second)
	fence, err := coordinator.AcquireExclusive(ctx, testCoordinatorRequest("exclusive-expiring", scope, testCoordinatorOwner("controller-a")))
	if err != nil {
		t.Fatal(err)
	}

	clock.Advance(2 * time.Second)
	if err := fence.Revalidate(ctx); !errors.Is(err, ErrCoordinatorExpired) {
		t.Fatalf("expired exclusive revalidate error=%v, want expired", err)
	}
	if err := fence.Release(ctx); !errors.Is(err, ErrCoordinatorExpired) {
		t.Fatalf("expired exclusive release error=%v, want expired", err)
	}
	if _, err := coordinator.Mode(ctx, scope); !errors.Is(err, ErrCoordinatorExpired) {
		t.Fatalf("expired exclusive mode error=%v, want expired", err)
	}
}

func waitForCoordinatorMode(t *testing.T, coordinator ExclusiveControlCoordinator, ctx context.Context, scope core.MutationScope, want core.ControlGateMode) {
	t.Helper()
	for attempt := 0; attempt < 10000; attempt++ {
		mode, err := coordinator.Mode(ctx, scope)
		if err == nil && mode == want {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("coordinator did not reach mode %s", want)
}
