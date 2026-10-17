package theftonline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	farmingress "farmgame/internal/farm/ingress"
)

type blockingTheftDispatch struct {
	started chan string
	release chan struct{}

	mu       sync.Mutex
	calls    map[string]int
	recorded int
}

func newBlockingTheftDispatch() *blockingTheftDispatch {
	return &blockingTheftDispatch{started: make(chan string, 8), release: make(chan struct{}), calls: make(map[string]int)}
}

func (d *blockingTheftDispatch) DispatchTheft(ctx context.Context, orderID string) error {
	d.mu.Lock()
	d.calls[orderID]++
	d.mu.Unlock()
	d.started <- orderID
	select {
	case <-d.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *blockingTheftDispatch) RecordTheftTargetBinding(context.Context, farmingress.TheftTargetBinding) error {
	d.mu.Lock()
	d.recorded++
	d.mu.Unlock()
	return nil
}

func TestAsyncDispatchReturnsBeforeDelegateAndCoalesces(t *testing.T) {
	delegate := newBlockingTheftDispatch()
	dispatch, err := NewAsyncDispatch(AsyncDispatchConfig{Delegate: delegate, MaxItems: 2, MaxBytes: 1024, Workers: 1, ItemTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(delegate.release)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = dispatch.Close(ctx)
	}()

	startedAt := time.Now()
	if err := dispatch.DispatchTheft(context.Background(), "order-1"); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(startedAt); elapsed > 100*time.Millisecond {
		t.Fatalf("enqueue blocked on delegate: %v", elapsed)
	}
	select {
	case got := <-delegate.started:
		if got != "order-1" {
			t.Fatalf("started %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("delegate did not start")
	}
	if err := dispatch.DispatchTheft(context.Background(), "order-1"); err != nil {
		t.Fatalf("duplicate should coalesce: %v", err)
	}
	if got := dispatch.Stats().Coalesced; got != 1 {
		t.Fatalf("coalesced=%d", got)
	}
}

func TestAsyncDispatchQueueFullLeavesWorkForRecovery(t *testing.T) {
	delegate := newBlockingTheftDispatch()
	dispatch, err := NewAsyncDispatch(AsyncDispatchConfig{Delegate: delegate, MaxItems: 1, MaxBytes: 1024, Workers: 1, ItemTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatch.DispatchTheft(context.Background(), "order-running"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-delegate.started:
	case <-time.After(time.Second):
		t.Fatal("delegate did not start")
	}
	if err := dispatch.DispatchTheft(context.Background(), "order-queued"); err != nil {
		t.Fatal(err)
	}
	if err := dispatch.DispatchTheft(context.Background(), "order-recovery"); !errors.Is(err, ErrAsyncDispatchQueueFull) {
		t.Fatalf("want queue full, got %v", err)
	}
	if got := dispatch.Stats().QueueFull; got != 1 {
		t.Fatalf("queue_full=%d", got)
	}
	close(delegate.release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := dispatch.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncDispatchDelegatesTargetBindingRecorder(t *testing.T) {
	delegate := newBlockingTheftDispatch()
	dispatch, err := NewAsyncDispatch(AsyncDispatchConfig{Delegate: delegate, MaxItems: 1, MaxBytes: 1024, Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatch.RecordTheftTargetBinding(context.Background(), farmingress.TheftTargetBinding{}); err != nil {
		t.Fatal(err)
	}
	delegate.mu.Lock()
	recorded := delegate.recorded
	delegate.mu.Unlock()
	if recorded != 1 {
		t.Fatalf("recorded=%d", recorded)
	}
	close(delegate.release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := dispatch.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
