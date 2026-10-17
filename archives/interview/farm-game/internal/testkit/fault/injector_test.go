package fault

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestInjectorFIFOAndAllowByDefault(t *testing.T) {
	inj := NewInjector()
	calls := 0
	fn := func(context.Context, int) (int, error) { calls++; return 42, nil }

	got, err := Do(context.Background(), inj, "rpc", 1, fn)
	if err != nil || got != 42 || calls != 1 {
		t.Fatalf("default allow: got=%d err=%v calls=%d", got, err, calls)
	}

	boom := errors.New("boom")
	inj.ScriptErr("rpc", Fail, boom)
	inj.Script("rpc", Allow)
	if _, err := Do(context.Background(), inj, "rpc", 1, fn); !errors.Is(err, boom) {
		t.Fatalf("fail injection err=%v", err)
	}
	if calls != 1 {
		t.Fatal("Fail must not reach the real implementation")
	}
	if got, err := Do(context.Background(), inj, "rpc", 1, fn); err != nil || got != 42 {
		t.Fatalf("scripted allow: got=%d err=%v", got, err)
	}
	if inj.Calls("rpc") != 3 {
		t.Fatalf("Calls=%d want 3", inj.Calls("rpc"))
	}
}

func TestInjectorHangReleaseAndTimeout(t *testing.T) {
	inj := NewInjector()
	inj.Script("rpc", Hang)
	fn := func(context.Context, int) (int, error) { return 7, nil }

	done := make(chan error, 1)
	go func() {
		got, err := Do(context.Background(), inj, "rpc", 1, fn)
		if got != 7 {
			done <- errors.New("unexpected result")
			return
		}
		done <- err
	}()

	// 确定性握手:对端已登记 pending 并阻塞在 gate 上,禁止 sleep 猜时序。
	entered := inj.HangEntered("rpc")
	if entered == nil {
		t.Fatal("Hang script must expose an entered handshake")
	}
	<-entered

	// 已握手但未放行时,调用必须仍挂起(非阻塞检查,非时序假设)。
	select {
	case err := <-done:
		t.Fatalf("Hang returned before release: %v", err)
	default:
	}
	inj.ReleaseHang("rpc")
	if err := <-done; err != nil {
		t.Fatalf("after release err=%v", err)
	}

	// ctx 取消模型调用方超时。
	inj.Script("rpc", Hang)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, err := Do(ctx, inj, "rpc", 1, fn)
		done <- err
	}()
	<-inj.HangEntered("rpc")
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("hang cancel err=%v", err)
	}
}

func TestInjectorReleaseAllIsIdempotent(t *testing.T) {
	inj := NewInjector()
	inj.ReleaseAll()
	inj.ReleaseAll()
	inj.Script("rpc", Hang)
	fn := func(context.Context, int) (int, error) { return 1, nil }
	got, err := Do(context.Background(), inj, "rpc", 1, fn)
	if err != nil || got != 1 {
		t.Fatalf("post-ReleaseAll Hang must pass through: got=%d err=%v", got, err)
	}
}

func TestInjectorCrashPanics(t *testing.T) {
	inj := NewInjector()
	inj.ScriptCrash("rpc", "actor exploded")
	fn := func(context.Context, int) (int, error) { return 1, nil }
	defer func() {
		if r := recover(); r != "actor exploded" {
			t.Fatalf("recover=%v", r)
		}
	}()
	_, _ = Do(context.Background(), inj, "rpc", 1, fn)
}

func TestInjectorConcurrent(t *testing.T) {
	inj := NewInjector()
	fn := func(context.Context, int) (int, error) { return 1, nil }
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if _, err := Do(context.Background(), inj, "rpc", 1, fn); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	if inj.Calls("rpc") != 800 {
		t.Fatalf("Calls=%d want 800", inj.Calls("rpc"))
	}
}
