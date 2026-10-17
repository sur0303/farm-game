package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestTurnObserverReceivesExecutionDuration 验证 TurnObserver 在每个 turn 执行
// 后回调且耗时 >0(用于 farm_actor_turn_latency_seconds 直方图)。
func TestTurnObserverReceivesExecutionDuration(t *testing.T) {
	var calls atomic.Int64
	e := NewOptions(Options{
		UserMailboxSize: 16,
		TurnObserver: func(_ uint64, duration time.Duration) {
			calls.Add(1)
			if duration <= 0 {
				t.Errorf("turn duration=%v, want >0", duration)
			}
		},
	})
	defer e.Stop()

	if _, err := e.SubmitUser(context.Background(), 42, func(context.Context) (any, error) {
		time.Sleep(2 * time.Millisecond)
		return "ok", nil
	}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("turn observer calls=%d, want 1", calls.Load())
	}
}

// TestSetTurnObserverAfterCreate 验证 SetTurnObserver 可在 New 之后挂接。
func TestSetTurnObserverAfterCreate(t *testing.T) {
	var calls atomic.Int64
	e := New(16)
	defer e.Stop()
	e.SetTurnObserver(func(uint64, time.Duration) { calls.Add(1) })
	if _, err := e.SubmitUser(context.Background(), 7, func(context.Context) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("observer calls=%d, want 1", calls.Load())
	}
}
