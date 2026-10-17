package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRetireIdleMailboxReleasesGoroutine 验证:空闲 mailbox 按 generation 退休后,
// ActiveKeys 回落、处理 goroutine 退出(exited 关闭),后续 Submit 以新 generation 正常工作。
func TestRetireIdleMailboxReleasesGoroutine(t *testing.T) {
	e := New(8)
	defer e.Stop()

	const key = uint64(11)
	value, err := e.Submit(context.Background(), key, func(ctx context.Context) (any, error) {
		return "first", nil
	})
	if err != nil || value != "first" {
		t.Fatalf("first submit: value=%v err=%v", value, err)
	}

	snap, ok := e.MailboxSnapshot(key)
	if !ok {
		t.Fatal("mailbox snapshot missing after submit")
	}
	if snap.InFlight || snap.UserQueued != 0 || snap.SystemQueued != 0 {
		t.Fatalf("mailbox not idle after completed submit: %+v", snap)
	}

	e.mu.RLock()
	mb := e.boxes[key]
	e.mu.RUnlock()
	if mb == nil {
		t.Fatal("mailbox missing from map")
	}

	if outcome := e.RetireIfIdle(key, snap.Generation); outcome != RetireRetired {
		t.Fatalf("RetireIfIdle=%v, want retired", outcome)
	}
	if got := e.ActiveKeys(); got != 0 {
		t.Fatalf("ActiveKeys=%d, want 0 after retire", got)
	}
	select {
	case <-mb.exited:
	case <-time.After(2 * time.Second):
		t.Fatal("mailbox goroutine did not exit after retire")
	}

	// 退休后再提交:应创建新 generation 的 mailbox 并正常执行。
	value, err = e.Submit(context.Background(), key, func(ctx context.Context) (any, error) {
		return "second", nil
	})
	if err != nil || value != "second" {
		t.Fatalf("submit after retire: value=%v err=%v", value, err)
	}
	snap2, ok := e.MailboxSnapshot(key)
	if !ok {
		t.Fatal("mailbox snapshot missing after reactivation")
	}
	if snap2.Generation == snap.Generation {
		t.Fatalf("generation did not change after retire: %d", snap2.Generation)
	}
	if got := e.Stats().MailboxRetiredTotal; got != 1 {
		t.Fatalf("MailboxRetiredTotal=%d, want 1", got)
	}
}

// TestRetireBusyMailbox 验证:有排队 user 任务时退休被拒绝且 mailbox 保留。
func TestRetireBusyMailbox(t *testing.T) {
	e := New(8)
	defer e.Stop()

	const key = uint64(21)
	block := make(chan struct{})
	started := make(chan struct{})
	done := make(chan any, 1)
	go func() {
		value, err := e.Submit(context.Background(), key, func(ctx context.Context) (any, error) {
			close(started)
			<-block
			return "blocked", nil
		})
		if err != nil {
			done <- err
			return
		}
		done <- value
	}()
	<-started

	// 在途任务存在:Busy。
	snap, ok := e.MailboxSnapshot(key)
	if !ok {
		t.Fatal("snapshot missing")
	}
	if outcome := e.RetireIfIdle(key, snap.Generation); outcome != RetireBusy {
		t.Fatalf("RetireIfIdle with in-flight=%v, want busy", outcome)
	}

	// 排队任务存在:Busy。
	queued := make(chan any, 1)
	go func() {
		value, err := e.Submit(context.Background(), key, func(ctx context.Context) (any, error) {
			return "queued", nil
		})
		if err != nil {
			queued <- err
			return
		}
		queued <- value
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		snap, _ = e.MailboxSnapshot(key)
		if snap.UserQueued > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("queued submit never observed in mailbox")
		}
		time.Sleep(time.Millisecond)
	}
	if outcome := e.RetireIfIdle(key, snap.Generation); outcome != RetireBusy {
		t.Fatalf("RetireIfIdle with queued user job=%v, want busy", outcome)
	}
	if got := e.ActiveKeys(); got != 1 {
		t.Fatalf("ActiveKeys=%d, want 1 while busy", got)
	}

	close(block)
	if got := <-done; got != "blocked" {
		t.Fatalf("blocked submit result=%v", got)
	}
	if got := <-queued; got != "queued" {
		t.Fatalf("queued submit result=%v", got)
	}
	if got := e.Stats().MailboxRetireBusyTotal; got < 2 {
		t.Fatalf("MailboxRetireBusyTotal=%d, want >=2", got)
	}
}

// TestRetireStaleGeneration 验证:旧 generation 不能退休新 mailbox。
func TestRetireStaleGeneration(t *testing.T) {
	e := New(8)
	defer e.Stop()

	const key = uint64(31)
	if _, err := e.Submit(context.Background(), key, func(ctx context.Context) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	snap, _ := e.MailboxSnapshot(key)
	if outcome := e.RetireIfIdle(key, snap.Generation+100); outcome != RetireStaleGeneration {
		t.Fatalf("RetireIfIdle with wrong generation=%v, want stale_generation", outcome)
	}
	if got := e.ActiveKeys(); got != 1 {
		t.Fatalf("ActiveKeys=%d, want 1 (stale generation must not retire)", got)
	}
	if outcome := e.RetireIfIdle(key, snap.Generation); outcome != RetireRetired {
		t.Fatalf("RetireIfIdle=%v, want retired", outcome)
	}
	if outcome := e.RetireIfIdle(key, snap.Generation); outcome != RetireNotFound {
		t.Fatalf("second RetireIfIdle=%v, want not_found", outcome)
	}
	if got := e.Stats().MailboxRetireStaleTotal; got != 1 {
		t.Fatalf("MailboxRetireStaleTotal=%d, want 1", got)
	}
}

// TestSubmitRaceWithRetire 验证:Submit 与退休并发时任务不丢、不重、不 panic、
// 不投递到已关闭 channel;每个任务恰好执行一次。
func TestSubmitRaceWithRetire(t *testing.T) {
	e := New(4)
	defer e.Stop()

	const key = uint64(41)
	const rounds = 200
	var executed atomic.Int64

	for i := 0; i < rounds; i++ {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			value, err := e.Submit(context.Background(), key, func(ctx context.Context) (any, error) {
				executed.Add(1)
				return "ok", nil
			})
			if err != nil {
				t.Errorf("submit: %v", err)
				return
			}
			if value != "ok" {
				t.Errorf("submit value=%v", value)
			}
		}()
		go func() {
			defer wg.Done()
			if snap, ok := e.MailboxSnapshot(key); ok {
				e.RetireIfIdle(key, snap.Generation)
			}
		}()
		wg.Wait()
	}
	if got := executed.Load(); got != rounds {
		t.Fatalf("executed=%d, want %d (每轮恰好执行一次)", got, rounds)
	}
}

func TestClaimIsVisibleToRetireBeforeTaskStarts(t *testing.T) {
	const key = uint64(43)
	claimEntered := make(chan struct{})
	releaseClaim := make(chan struct{})
	taskRelease := make(chan struct{})
	var hookOnce sync.Once
	var active atomic.Int32
	var maxActive atomic.Int32
	updateMax := func() {
		for {
			current := active.Load()
			maximum := maxActive.Load()
			if current <= maximum || maxActive.CompareAndSwap(maximum, current) {
				return
			}
		}
	}

	e := NewOptions(Options{
		UserMailboxSize: 4,
		BeforeJobExecution: func(uint64, uint64) {
			hookOnce.Do(func() {
				close(claimEntered)
				<-releaseClaim
			})
		},
	})
	defer e.Stop()

	firstStarted := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := e.Submit(context.Background(), key, func(context.Context) (any, error) {
			active.Add(1)
			updateMax()
			close(firstStarted)
			<-taskRelease
			active.Add(-1)
			return "first", nil
		})
		firstDone <- err
	}()
	<-claimEntered

	snap, ok := e.MailboxSnapshot(key)
	if !ok || !snap.InFlight {
		t.Fatalf("claimed job was not visible as in-flight: %+v ok=%v", snap, ok)
	}
	if outcome := e.RetireIfIdle(key, snap.Generation); outcome != RetireBusy {
		t.Fatalf("RetireIfIdle during claimed-before-task hook=%v, want busy", outcome)
	}

	secondStarted := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		_, err := e.Submit(context.Background(), key, func(context.Context) (any, error) {
			active.Add(1)
			updateMax()
			close(secondStarted)
			<-taskRelease
			active.Add(-1)
			return "second", nil
		})
		secondDone <- err
	}()

	close(releaseClaim)
	<-firstStarted
	select {
	case <-secondStarted:
		t.Fatal("same-key second generation ran before first task released")
	default:
	}
	close(taskRelease)
	if err := <-firstDone; err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second submit: %v", err)
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("same-key max concurrent tasks=%d, want 1", got)
	}
}

// TestRetireWhileSystemTaskQueued 验证:SystemQueue 非空同样阻止退休。
func TestRetireWhileSystemTaskQueued(t *testing.T) {
	e := New(4)
	defer e.Stop()

	const key = uint64(51)
	block := make(chan struct{})
	started := make(chan struct{})
	finish := make(chan struct{})
	go func() {
		_, _ = e.Submit(context.Background(), key, func(ctx context.Context) (any, error) {
			close(started)
			<-block
			return nil, nil
		})
		close(finish)
	}()
	<-started

	systemDone := make(chan struct{})
	go func() {
		_, _ = e.SubmitSystem(context.Background(), key, SystemKindPassivate, func(ctx context.Context) (any, error) {
			return nil, nil
		})
		close(systemDone)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		snap, _ := e.MailboxSnapshot(key)
		if snap.SystemQueued > 0 {
			if outcome := e.RetireIfIdle(key, snap.Generation); outcome != RetireBusy {
				t.Fatalf("RetireIfIdle with queued system job=%v, want busy", outcome)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("system job never observed queued")
		}
		time.Sleep(time.Millisecond)
	}
	close(block)
	<-finish
	<-systemDone
}

// TestStopAfterRetire 验证:退休后 Stop 仍快速返回(不重复关闭 channel)。
func TestStopAfterRetire(t *testing.T) {
	e := New(4)
	const key = uint64(61)
	if _, err := e.Submit(context.Background(), key, func(ctx context.Context) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	snap, _ := e.MailboxSnapshot(key)
	if outcome := e.RetireIfIdle(key, snap.Generation); outcome != RetireRetired {
		t.Fatalf("RetireIfIdle=%v", outcome)
	}
	done := make(chan struct{})
	go func() {
		e.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop blocked after retire")
	}
	if _, err := e.Submit(context.Background(), key, func(ctx context.Context) (any, error) {
		return nil, nil
	}); !errors.Is(err, ErrStopped) {
		t.Fatalf("submit after stop err=%v, want ErrStopped", err)
	}
}
