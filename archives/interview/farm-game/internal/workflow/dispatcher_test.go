package workflow

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDispatcherEnqueueBoundsItemsAndBytes(t *testing.T) {
	f := newCrashFixture(t)
	dispatcher, err := NewDirectDispatcher(f.repo, f.engine, DispatcherConfig{MaxItems: 1, MaxBytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	first := f.createOrder(t, "op-dq-1")
	second := f.createOrder(t, "op-dq-2")
	if err := dispatcher.Enqueue(first.Address, 4); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Enqueue(second.Address, 4); !errors.Is(err, ErrDispatcherFullItems) {
		t.Fatalf("item bound not enforced: %v", err)
	}
	stats := dispatcher.Stats()
	if stats.Depth != 1 || stats.Dropped != 1 || stats.Bytes != 4 {
		t.Fatalf("stats=%+v", stats)
	}

	// Byte bound: room for items but not for bytes.
	roomy, err := NewDirectDispatcher(f.repo, f.engine, DispatcherConfig{MaxItems: 8, MaxBytes: 5})
	if err != nil {
		t.Fatal(err)
	}
	if err := roomy.Enqueue(first.Address, 4); err != nil {
		t.Fatal(err)
	}
	if err := roomy.Enqueue(second.Address, 4); !errors.Is(err, ErrDispatcherFullBytes) {
		t.Fatalf("byte bound not enforced: %v", err)
	}
	// Invalid addresses never enter the queue.
	bad := first.Address
	bad.WorkID = "not-a-stable-id"
	if err := dispatcher.Enqueue(bad, 0); err == nil {
		t.Fatal("invalid address enqueued")
	}
}

func TestDispatcherFastPathCompletesAndBacksOff(t *testing.T) {
	f := newCrashFixture(t)
	dispatcher, err := NewDirectDispatcher(f.repo, f.engine, DispatcherConfig{ItemDeadline: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	order := f.createOrder(t, "op-dq-fast")
	if err := dispatcher.DispatchOnce(context.Background(), order.Address); err != nil {
		t.Fatal(err)
	}
	if got := f.get(t, order); got.State != WorkStateCompleted {
		t.Fatalf("state=%s", got.State)
	}
	if stats := dispatcher.Stats(); stats.Completed != 1 || stats.Dispatched != 1 {
		t.Fatalf("stats=%+v", stats)
	}

	// Participant fault → work is rescheduled, not lost.
	faulty := f.createOrder(t, "op-dq-fault")
	applyInvocation := testInvocationID(t, testOrderType, faulty.Address.WorkID, stepApply)
	f.p1.script(applyInvocation, faultCrashBeforeApply)
	if err := dispatcher.DispatchOnce(context.Background(), faulty.Address); !errors.Is(err, ErrRetryScheduled) {
		t.Fatalf("dispatch=%v", err)
	}
	stored := f.get(t, faulty)
	if stored.Terminal() || stored.NextDueAtMs <= f.now.UnixMilli() {
		t.Fatalf("retry not scheduled: %+v", stored)
	}
	if stats := dispatcher.Stats(); stats.Reschedule != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	// Recovery finishes it without re-applying beyond the first failed try.
	f.now = f.now.Add(2 * time.Second)
	if stats := f.recoverOnce(t, "worker-1"); stats.Completed != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	if got := f.p1.applyCount(applyInvocation); got != 1 {
		t.Fatalf("applied %d times", got)
	}
}

func TestDispatcherStartDrainsQueueAndClose(t *testing.T) {
	f := newCrashFixture(t)
	dispatcher, err := NewDirectDispatcher(f.repo, f.engine, DispatcherConfig{Workers: 2, ItemDeadline: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	dispatcher.Start(ctx)
	var orders []*WorkRecord
	for i := 0; i < 4; i++ {
		order := f.createOrder(t, "op-dq-drain-"+string(rune('a'+i)))
		orders = append(orders, order)
		if err := dispatcher.Enqueue(order.Address, 1); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		done := 0
		for _, order := range orders {
			if f.get(t, order).Terminal() {
				done++
			}
		}
		if done == len(orders) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue did not drain: %d/%d", done, len(orders))
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	dispatcher.Close()
	if stats := dispatcher.Stats(); stats.Completed != 4 {
		t.Fatalf("stats=%+v", stats)
	}
	if f.p1.totalApplies() != 4 {
		t.Fatalf("applies=%d", f.p1.totalApplies())
	}
}

func TestDispatcherMissingRecordFailsWithoutRetryLoop(t *testing.T) {
	f := newCrashFixture(t)
	dispatcher, err := NewDirectDispatcher(f.repo, f.engine, DispatcherConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ghost := f.createOrder(t, "op-dq-ghost").Address
	ghost.WorkID = testAddress(testOrderType, "op-dq-missing").WorkID
	if err := dispatcher.DispatchOnce(context.Background(), ghost); !errors.Is(err, ErrNotFound) {
		t.Fatalf("dispatch=%v", err)
	}
}
