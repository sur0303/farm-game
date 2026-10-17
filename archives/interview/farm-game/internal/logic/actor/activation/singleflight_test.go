package activation

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestLoadFutureSingleflightSharesOneIdentityWithHundredRequests(t *testing.T) {
	group := NewLoadFutureGroup(LoadFutureOptions{MaxWaitersPerActor: 100, MaxTotalWaiters: 100})
	var identityCalls atomic.Int64
	newIdentity := func() (LoadIdentity, error) {
		identityCalls.Add(1)
		return LoadIdentity{ActivationGeneration: 7, ClaimOperationID: "claim-op-7", NewWriterClaimID: "claim-7"}, nil
	}
	owner, isOwner, err := group.BeginOrJoin(42, newIdentity)
	if err != nil || !isOwner || !owner.IsOwner() {
		t.Fatalf("owner=%v isOwner=%v err=%v", owner, isOwner, err)
	}

	const joined = 99
	waiters := make([]*LoadHandle, joined)
	var unexpectedFactoryCalls atomic.Int64
	var wg sync.WaitGroup
	wg.Add(joined)
	for i := 0; i < joined; i++ {
		go func(index int) {
			defer wg.Done()
			handle, ownerFlag, joinErr := group.BeginOrJoin(42, func() (LoadIdentity, error) {
				unexpectedFactoryCalls.Add(1)
				return LoadIdentity{}, errors.New("identity factory called for a joined request")
			})
			if joinErr != nil || ownerFlag {
				t.Errorf("join[%d] owner=%v err=%v", index, ownerFlag, joinErr)
				return
			}
			waiters[index] = handle
		}(i)
	}
	wg.Wait()
	if got := identityCalls.Load(); got != 1 {
		t.Fatalf("identity factory calls=%d, want 1", got)
	}
	if got := unexpectedFactoryCalls.Load(); got != 0 {
		t.Fatalf("joined identity factory calls=%d, want 0", got)
	}
	for index, waiter := range waiters {
		if waiter == nil || waiter.Identity() != owner.Identity() {
			t.Fatalf("waiter[%d] identity=%+v owner=%+v", index, waiter.Identity(), owner.Identity())
		}
	}
	if got := group.Stats(); got.ActiveFlights != 1 || got.CurrentWaiters != joined || got.JoinedTotal != joined || got.StartedTotal != 1 {
		t.Fatalf("stats before complete=%+v", got)
	}
	if err := owner.Complete(nil); err != nil {
		t.Fatal(err)
	}
	for index, waiter := range waiters {
		if err := waiter.Wait(context.Background()); err != nil {
			t.Fatalf("waiter[%d]: %v", index, err)
		}
	}
	if got := group.Stats(); got.ActiveFlights != 0 || got.CurrentWaiters != 0 || got.CompletedTotal != 1 {
		t.Fatalf("stats after complete=%+v", got)
	}
}

func TestLoadFutureCanceledWaiterDoesNotCancelSharedRecovery(t *testing.T) {
	group := NewLoadFutureGroup(LoadFutureOptions{MaxWaitersPerActor: 2, MaxTotalWaiters: 2})
	owner, _, err := group.BeginOrJoin(9, func() (LoadIdentity, error) {
		return LoadIdentity{ActivationGeneration: 1, ClaimOperationID: "claim-op", NewWriterClaimID: "claim"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter, _, err := group.BeginOrJoin(9, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waiter.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait err=%v", err)
	}
	if got := group.Stats(); got.ActiveFlights != 1 || got.CurrentWaiters != 0 {
		t.Fatalf("cancellation canceled shared flight: %+v", got)
	}
	next, _, err := group.BeginOrJoin(9, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("claim recovery failed closed")
	if err := owner.Complete(want); err != nil {
		t.Fatal(err)
	}
	if err := next.Wait(context.Background()); !errors.Is(err, want) {
		t.Fatalf("shared result=%v", err)
	}
}

func TestLoadFutureWaitersAreBounded(t *testing.T) {
	group := NewLoadFutureGroup(LoadFutureOptions{MaxWaitersPerActor: 1, MaxTotalWaiters: 1})
	owner, _, err := group.BeginOrJoin(77, func() (LoadIdentity, error) {
		return LoadIdentity{ActivationGeneration: 1, ClaimOperationID: "claim-op", NewWriterClaimID: "claim"}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	waiter, _, err := group.BeginOrJoin(77, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := group.BeginOrJoin(77, nil); !errors.Is(err, ErrTooManyLoadWaiters) {
		t.Fatalf("overflow err=%v", err)
	}
	if err := owner.Complete(nil); err != nil {
		t.Fatal(err)
	}
	if err := waiter.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}
