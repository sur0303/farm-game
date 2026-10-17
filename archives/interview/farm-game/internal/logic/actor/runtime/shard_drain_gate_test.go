package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testShardGate(t *testing.T, shards uint32) *ShardDrainGate {
	t.Helper()
	gate, err := NewShardDrainGate(ShardDrainGateOptions{
		ShardCount: shards,
		ShardFor: func(actorID uint64) (uint32, error) {
			return uint32(actorID % uint64(shards)), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return gate
}

func waitShardStatus(t *testing.T, gate *ShardDrainGate, shard uint32, want func(ShardDrainStatus) bool) ShardDrainStatus {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		status, ok := gate.Status(shard)
		if ok && want(status) {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("status did not converge: %+v", status)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestShardDrainGateIsScopedAndWaitIsExplicit(t *testing.T) {
	gate := testShardGate(t, 2)
	release0, err := gate.Admit(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	release1, err := gate.Admit(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.BeginDrain(0); err != nil {
		t.Fatal(err)
	}
	if err := gate.BeginDrain(0); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Admit(context.Background(), 2); !errors.Is(err, ErrShardDraining) {
		t.Fatalf("admit on drained shard err=%v", err)
	}
	status, _ := gate.Status(1)
	if status.Phase != ShardDrainPhaseOpen || status.AcceptedJobs != 1 {
		t.Fatalf("other shard changed: %+v", status)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := gate.Wait(ctx, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait deadline err=%v", err)
	}
	status, _ = gate.Status(0)
	if status.WaitComplete {
		t.Fatal("deadline must not complete the wait barrier")
	}
	release0()
	if err := gate.Wait(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	status = waitShardStatus(t, gate, 0, func(status ShardDrainStatus) bool {
		return status.AcceptedJobs == 0 && status.WaitComplete
	})
	if status.Phase != ShardDrainPhaseDraining {
		t.Fatalf("phase=%v, want DRAINING", status.Phase)
	}
	release1()
}

func TestShardDrainGateRearmsOnlyAfterCompletedDrain(t *testing.T) {
	gate := testShardGate(t, 1)
	release, err := gate.Admit(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := gate.BeginDrain(0); err != nil {
		t.Fatal(err)
	}
	if err := gate.Rearm(0); !errors.Is(err, ErrShardDrainNotComplete) {
		t.Fatalf("rearm with accepted job err=%v", err)
	}
	release()
	if err := gate.Wait(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if err := gate.Rearm(0); err != nil {
		t.Fatal(err)
	}
	status, ok := gate.Status(0)
	if !ok || status.Phase != ShardDrainPhaseOpen ||
		status.AcceptedJobs != 0 || status.WaitComplete {
		t.Fatalf("rearmed status=%+v found=%t", status, ok)
	}
	release, err = gate.Admit(context.Background(), 1)
	if err != nil {
		t.Fatalf("rearmed admission err=%v", err)
	}
	release()
	if err := gate.BeginDrain(0); err != nil {
		t.Fatal(err)
	}
	if err := gate.Wait(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
}

func TestExecutorGateRejectsBeforeMailboxAndSystemQueueIsIndependent(t *testing.T) {
	gate := testShardGate(t, 2)
	e := NewOptions(Options{
		UserMailboxSize: 1,
		SystemQueueSize: 2,
		AdmissionGate:   gate,
		MaxUserBytes:    1 << 20,
	})
	defer e.Stop()

	if err := gate.BeginDrain(0); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Submit(context.Background(), 2, func(context.Context) (any, error) {
		return nil, nil
	}); !errors.Is(err, ErrShardDraining) {
		t.Fatalf("user admission err=%v", err)
	}
	if e.ActiveKeys() != 0 {
		t.Fatalf("drained user submit created mailbox: %d", e.ActiveKeys())
	}
	if _, err := e.SubmitSystem(context.Background(), 2, SystemKindDrain, func(context.Context) (any, error) {
		return "system-ok", nil
	}); err != nil {
		t.Fatalf("system queue was blocked by user gate: %v", err)
	}
	if _, err := e.Submit(context.Background(), 1, func(context.Context) (any, error) {
		return "other-shard", nil
	}); err != nil {
		t.Fatalf("other shard user submit failed: %v", err)
	}
}

func TestExecutorAdmissionReleaseCoversQueuedCancellationFullAndPanic(t *testing.T) {
	gate := testShardGate(t, 1)
	e := NewOptions(Options{
		UserMailboxSize: 1,
		AdmissionGate:   gate,
		MaxUserBytes:    1 << 20,
	})
	defer e.Stop()

	started := make(chan struct{})
	releaseBlocker := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := e.Submit(context.Background(), 7, func(context.Context) (any, error) {
			close(started)
			<-releaseBlocker
			return nil, nil
		})
		firstDone <- err
	}()
	<-started

	queuedCtx, cancelQueued := context.WithCancel(context.Background())
	queuedDone := make(chan error, 1)
	var queuedRan atomic.Bool
	go func() {
		_, err := e.Submit(queuedCtx, 7, func(context.Context) (any, error) {
			queuedRan.Store(true)
			return nil, nil
		})
		queuedDone <- err
	}()
	waitShardStatus(t, gate, 0, func(status ShardDrainStatus) bool {
		return status.AcceptedJobs == 2
	})
	cancelQueued()
	select {
	case err := <-queuedDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("queued caller err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued caller did not observe cancellation")
	}
	status, _ := gate.Status(0)
	if status.AcceptedJobs != 2 {
		t.Fatalf("caller cancellation released admission early: %+v", status)
	}

	if _, err := e.Submit(context.Background(), 7, func(context.Context) (any, error) {
		return nil, nil
	}); !errors.Is(err, ErrMailboxFull) && !errors.Is(err, ErrActorBusy) {
		t.Fatalf("full submit err=%v", err)
	}
	status, _ = gate.Status(0)
	if status.AcceptedJobs != 2 {
		t.Fatalf("rejected full submit leaked or released wrong count: %+v", status)
	}

	if err := gate.BeginDrain(0); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancelWait := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelWait()
	if err := gate.Wait(waitCtx, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait before accepted tasks finish err=%v", err)
	}
	close(releaseBlocker)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := gate.Wait(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if queuedRan.Load() {
		t.Fatal("canceled queued task ran")
	}
	status = waitShardStatus(t, gate, 0, func(status ShardDrainStatus) bool {
		return status.AcceptedJobs == 0 && status.WaitComplete
	})
	if status.AcceptedJobs != 0 {
		t.Fatalf("accepted count leaked after queued cancellation: %+v", status)
	}

	panicGate := testShardGate(t, 1)
	panicExecutor := NewOptions(Options{AdmissionGate: panicGate})
	_, err := panicExecutor.Submit(context.Background(), 1, func(context.Context) (any, error) {
		panic("phase1")
	})
	if !errors.Is(err, ErrActorPanic) {
		t.Fatalf("panic err=%v", err)
	}
	if status, _ := panicGate.Status(0); status.AcceptedJobs != 0 {
		t.Fatalf("panic leaked admission: %+v", status)
	}
	panicExecutor.Stop()
}

func TestExecutorStopReleasesQueuedAdmissionExactlyOnce(t *testing.T) {
	gate := testShardGate(t, 1)
	e := NewOptions(Options{
		UserMailboxSize: 1,
		AdmissionGate:   gate,
		MaxUserBytes:    1 << 20,
	})

	started := make(chan struct{})
	releaseBlocker := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := e.Submit(context.Background(), 9, func(context.Context) (any, error) {
			close(started)
			<-releaseBlocker
			return nil, nil
		})
		firstDone <- err
	}()
	<-started
	queuedDone := make(chan error, 1)
	var stoppedRan atomic.Bool
	go func() {
		_, err := e.Submit(context.Background(), 9, func(context.Context) (any, error) {
			stoppedRan.Store(true)
			return nil, nil
		})
		queuedDone <- err
	}()
	waitShardStatus(t, gate, 0, func(status ShardDrainStatus) bool {
		return status.AcceptedJobs == 2
	})

	stopDone := make(chan struct{})
	go func() {
		e.Stop()
		close(stopDone)
	}()
	deadline := time.Now().Add(time.Second)
	for !e.stoppedFast.Load() {
		if time.Now().After(deadline) {
			t.Fatal("executor stop did not linearize")
		}
		time.Sleep(time.Millisecond)
	}
	close(releaseBlocker)
	<-stopDone
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-queuedDone; !errors.Is(err, ErrStopped) {
		t.Fatalf("queued stop err=%v", err)
	}
	if stoppedRan.Load() {
		t.Fatal("stopped queued task ran")
	}
	status, _ := gate.Status(0)
	if status.AcceptedJobs != 0 {
		t.Fatalf("stop leaked admission: %+v", status)
	}
}

func TestExecutorAdmissionConcurrentDrainRace(t *testing.T) {
	gate := testShardGate(t, 4)
	e := NewOptions(Options{
		UserMailboxSize: 4,
		AdmissionGate:   gate,
		MaxUserBytes:    1 << 20,
	})
	defer e.Stop()

	var wg sync.WaitGroup
	var accepted atomic.Int64
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			_, err := e.Submit(context.Background(), id, func(context.Context) (any, error) {
				accepted.Add(1)
				return nil, nil
			})
			if err != nil && !errors.Is(err, ErrShardDraining) && !errors.Is(err, ErrMailboxFull) &&
				!errors.Is(err, ErrActorBusy) {
				t.Errorf("submit %d: %v", id, err)
			}
		}(uint64(i + 1))
		if i == 16 {
			_ = gate.BeginDrain(0)
			_ = gate.BeginDrain(1)
		}
	}
	wg.Wait()
	for shard := uint32(0); shard < 2; shard++ {
		status, _ := gate.Status(shard)
		if status.AcceptedJobs != 0 {
			t.Fatalf("shard %d leaked count: %+v", shard, status)
		}
	}
	if accepted.Load() == 0 {
		t.Fatal("race test accepted no work")
	}
}
