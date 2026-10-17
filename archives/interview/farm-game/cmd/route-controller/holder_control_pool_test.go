package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holdercontrol"
	core "farmgame/internal/routecontrol"
)

func TestHolderControlPoolReusesExactGenerationAndBoundsConnections(t *testing.T) {
	var dials atomic.Int32
	pool := newHolderControlClientPool(2, func(context.Context, core.HolderRecord) (*holdercontrol.HolderControlClient, error) {
		dials.Add(1)
		return &holdercontrol.HolderControlClient{}, nil
	})
	t.Cleanup(func() { _ = pool.Close() })
	record := testPoolHolder("holder-a", "boot-a")
	first, releaseFirst, err := pool.Acquire(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	second, releaseSecond, err := pool.Acquire(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || dials.Load() != 1 {
		t.Fatalf("exact generation was not reused: same=%t dials=%d", first == second, dials.Load())
	}
	releaseFirst()
	releaseSecond()

	newBoot := testPoolHolder("holder-a", "boot-b")
	_, releaseNewBoot, err := pool.Acquire(context.Background(), newBoot)
	if err != nil {
		t.Fatal(err)
	}
	other := testPoolHolder("holder-b", "boot-a")
	_, releaseOther, err := pool.Acquire(context.Background(), other)
	if err != nil {
		t.Fatal(err)
	}
	if dials.Load() != 3 {
		t.Fatalf("dials=%d want 3 exact generations", dials.Load())
	}
	releaseNewBoot()
	releaseOther()
}

func TestHolderControlPoolCoalescesConcurrentDial(t *testing.T) {
	started := make(chan struct{})
	finish := make(chan struct{})
	var dials atomic.Int32
	pool := newHolderControlClientPool(8, func(context.Context, core.HolderRecord) (*holdercontrol.HolderControlClient, error) {
		if dials.Add(1) == 1 {
			close(started)
		}
		<-finish
		return &holdercontrol.HolderControlClient{}, nil
	})
	t.Cleanup(func() { _ = pool.Close() })
	record := testPoolHolder("holder-a", "boot-a")
	const callers = 16
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, release, err := pool.Acquire(context.Background(), record)
			if err == nil {
				release()
			}
			errs <- err
		}()
	}
	<-started
	close(finish)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if dials.Load() != 1 {
		t.Fatalf("concurrent dials=%d want 1", dials.Load())
	}
}

func TestHolderControlPoolFailsClosedWhenAllSlotsAreActive(t *testing.T) {
	pool := newHolderControlClientPool(2, func(context.Context, core.HolderRecord) (*holdercontrol.HolderControlClient, error) {
		return &holdercontrol.HolderControlClient{}, nil
	})
	t.Cleanup(func() { _ = pool.Close() })
	_, releaseA, err := pool.Acquire(context.Background(), testPoolHolder("holder-a", "boot-a"))
	if err != nil {
		t.Fatal(err)
	}
	defer releaseA()
	_, releaseB, err := pool.Acquire(context.Background(), testPoolHolder("holder-b", "boot-a"))
	if err != nil {
		t.Fatal(err)
	}
	defer releaseB()
	if _, _, err := pool.Acquire(context.Background(), testPoolHolder("holder-c", "boot-a")); !errors.Is(err, errHolderControlPoolExhausted) {
		t.Fatalf("third active generation err=%v", err)
	}
}

func testPoolHolder(holderID, bootID string) core.HolderRecord {
	return core.HolderRecord{
		Domain: actorrouting.DomainFarm, HolderID: holderID, BootID: bootID,
		HolderControlEndpoint: holderID + ":19101", TLSDNSIdentity: "farm-svr.internal",
	}
}
