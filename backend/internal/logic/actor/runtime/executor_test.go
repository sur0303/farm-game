package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSameKeySerial 验证同一 key 的任务严格串行:并发提交对同一 key 的自增,
// 若串行则无竞态、结果精确;用一个非原子计数器检测串行性。
func TestSameKeySerial(t *testing.T) {
	e := New(1024)
	defer e.Stop()

	const key = uint64(1)
	const n = 500
	counter := 0 // 故意用非并发安全的普通 int,串行才不会丢更新

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = e.Submit(context.Background(), key, func(ctx context.Context) (any, error) {
				counter++ // 同 key 串行 → 无数据竞争
				return nil, nil
			})
		}()
	}
	wg.Wait()
	if counter != n {
		t.Fatalf("counter=%d, want %d (同 key 未严格串行)", counter, n)
	}
}

// TestSameKeySerialMixedUserSystem 混投 user+system,共享非原子计数器无丢失。
func TestSameKeySerialMixedUserSystem(t *testing.T) {
	const users, systems = 200, 200
	e := NewOptions(Options{
		UserMailboxSize: users,
		SystemQueueSize: systems,
		MaxUserBytes:    1 << 20,
	})
	defer e.Stop()

	const key = uint64(2)
	counter := 0

	var wg sync.WaitGroup
	for i := 0; i < users; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.SubmitUser(context.Background(), key, func(ctx context.Context) (any, error) {
				counter++
				return nil, nil
			}); err != nil {
				t.Errorf("SubmitUser: %v", err)
			}
		}()
	}
	for i := 0; i < systems; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.SubmitSystem(context.Background(), key, SystemKindLeaseLost, func(ctx context.Context) (any, error) {
				counter++
				return nil, nil
			}); err != nil {
				t.Errorf("SubmitSystem: %v", err)
			}
		}()
	}
	wg.Wait()
	if counter != users+systems {
		t.Fatalf("counter=%d, want %d (混投未严格串行)", counter, users+systems)
	}
}

// TestDifferentKeysParallel 验证不同 key 并行:两个 key 各自阻塞一会,
// 总耗时应接近单个耗时而非两倍。
func TestDifferentKeysParallel(t *testing.T) {
	e := New(16)
	defer e.Stop()

	block := 80 * time.Millisecond
	start := time.Now()
	var wg sync.WaitGroup
	for _, k := range []uint64{1, 2, 3, 4} {
		wg.Add(1)
		go func(key uint64) {
			defer wg.Done()
			_, _ = e.Submit(context.Background(), key, func(ctx context.Context) (any, error) {
				time.Sleep(block)
				return nil, nil
			})
		}(k)
	}
	wg.Wait()
	elapsed := time.Since(start)
	if elapsed > 3*block {
		t.Fatalf("elapsed=%v, 不同 key 未并行(应 ~%v)", elapsed, block)
	}
	if e.ActiveKeys() != 4 {
		t.Fatalf("active keys=%d, want 4", e.ActiveKeys())
	}
}

// TestMailboxFull 验证 UserMailbox 有界过载返回 ErrMailboxFull / ErrActorBusy。
func TestMailboxFull(t *testing.T) {
	e := New(1)
	defer e.Stop()

	const key = uint64(7)
	release := make(chan struct{})
	// 占住处理 goroutine
	go e.Submit(context.Background(), key, func(ctx context.Context) (any, error) {
		<-release
		return nil, nil
	})
	time.Sleep(20 * time.Millisecond) // 让首个任务进入执行

	// 填满容量为 1 的 UserMailbox 后,再投递应满。
	var full atomic.Bool
	for i := 0; i < 50; i++ {
		go func() {
			_, err := e.Submit(context.Background(), key, func(ctx context.Context) (any, error) { return nil, nil })
			if errors.Is(err, ErrMailboxFull) || errors.Is(err, ErrActorBusy) {
				full.Store(true)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	if !full.Load() {
		t.Fatalf("expected at least one ErrMailboxFull/ErrActorBusy under user overload")
	}
}

// TestStop 验证停止后拒绝新提交。
func TestMailboxInitialCapacityUsesLazyOverflowUpToMax(t *testing.T) {
	e := NewOptions(Options{
		UserMailboxSize:        4,
		UserMailboxInitialSize: 1,
		MaxUserBytes:           1 << 20,
	})
	defer e.Stop()

	const key = uint64(8)
	release := make(chan struct{})
	started := make(chan struct{})
	go func() {
		_, _ = e.Submit(context.Background(), key, func(context.Context) (any, error) {
			close(started)
			<-release
			return nil, nil
		})
	}()
	<-started

	e.mu.RLock()
	mb := e.boxes[key]
	e.mu.RUnlock()
	if mb == nil || cap(mb.userCh) != 1 {
		t.Fatalf("initial user mailbox capacity=%d, want 1", cap(mb.userCh))
	}

	results := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() {
			_, err := e.Submit(context.Background(), key, func(context.Context) (any, error) { return nil, nil })
			results <- err
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		snap, ok := e.MailboxSnapshot(key)
		if ok && snap.UserQueued == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("queued=%+v ok=%v, want all 4 jobs admitted", snap, ok)
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := e.Submit(context.Background(), key, func(context.Context) (any, error) { return nil, nil }); !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("overflow beyond logical max err=%v, want ErrMailboxFull", err)
	}
	close(release)
	for i := 0; i < 4; i++ {
		if err := <-results; err != nil {
			t.Fatalf("queued task %d: %v", i, err)
		}
	}
}

func TestStop(t *testing.T) {
	e := New(4)
	e.Stop()
	if _, err := e.Submit(context.Background(), 1, func(ctx context.Context) (any, error) { return nil, nil }); err != ErrStopped {
		t.Fatalf("want ErrStopped, got %v", err)
	}
}

func TestConcurrentStopWaitsForSharedCompletion(t *testing.T) {
	e := New(4)
	started := make(chan struct{})
	release := make(chan struct{})
	taskDone := make(chan error, 1)
	go func() {
		_, err := e.Submit(context.Background(), 77, func(context.Context) (any, error) {
			close(started)
			<-release
			return nil, nil
		})
		taskDone <- err
	}()
	<-started

	firstDone := make(chan struct{})
	go func() {
		e.Stop()
		close(firstDone)
	}()
	deadline := time.Now().Add(time.Second)
	for !e.stoppedFast.Load() {
		if time.Now().After(deadline) {
			t.Fatal("first Stop did not linearize")
		}
	}

	secondDone := make(chan struct{})
	go func() {
		e.Stop()
		close(secondDone)
	}()
	select {
	case <-secondDone:
		t.Fatal("concurrent Stop returned before the blocking task completed")
	case <-time.After(10 * time.Millisecond):
	}

	close(release)
	if err := <-taskDone; err != nil {
		t.Fatalf("blocking task: %v", err)
	}
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first Stop did not complete")
	}
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second Stop did not complete")
	}
	if got := e.ActiveKeys(); got != 0 {
		t.Fatalf("ActiveKeys after Stop=%d, want 0", got)
	}
}

// TestResultPropagation 验证返回值与错误正确传回。
func TestResultPropagation(t *testing.T) {
	e := New(4)
	defer e.Stop()
	v, err := e.Submit(context.Background(), 9, func(ctx context.Context) (any, error) {
		return "ok", nil
	})
	if err != nil || v.(string) != "ok" {
		t.Fatalf("v=%v err=%v", v, err)
	}
}

// TestSubmitRejectsCancellationAtEnqueueBoundary pins the admission race:
// cancellation observed after mailbox lookup but before the channel send
// must not enqueue or invoke the actor task. The dequeue guard remains a
// separate defense for cancellation after a task was admitted.
func TestSubmitRejectsCancellationAtEnqueueBoundary(t *testing.T) {
	e := New(4)
	defer e.Stop()

	ctx := &cancelAtSecondErrContext{done: make(chan struct{})}
	var handlerCalls atomic.Int32
	if _, err := e.Submit(ctx, 123, func(context.Context) (any, error) {
		handlerCalls.Add(1)
		return nil, nil
	}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("enqueue cancellation err=%v", err)
	}
	if handlerCalls.Load() != 0 {
		t.Fatal("canceled task entered actor handler")
	}
	if stats := e.Stats(); stats.Submitted != 0 || stats.UserQueued != 0 {
		t.Fatalf("canceled task occupied mailbox: %+v", stats)
	}
}

func TestSubmitSystemCancellationDoesNotCreateMailbox(t *testing.T) {
	e := New(4)
	defer e.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := e.SubmitSystem(ctx, 124, SystemKindDrain, func(context.Context) (any, error) {
		t.Fatal("canceled system task ran")
		return nil, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled system submit err=%v", err)
	}
	if got := e.ActiveKeys(); got != 0 {
		t.Fatalf("canceled system submit created mailbox: %d", got)
	}
}

func TestCancellationAfterMailboxCreationRetiresExactEmptyGeneration(t *testing.T) {
	e := New(4)
	defer e.Stop()

	ctx := &cancelAfterChecksContext{cancelAt: 5}
	_, err := e.SubmitSystem(ctx, 125, SystemKindDrain, func(context.Context) (any, error) {
		t.Fatal("canceled system task ran")
		return nil, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("post-create cancellation err=%v", err)
	}
	if got := e.ActiveKeys(); got != 0 {
		t.Fatalf("post-create cancellation leaked empty mailbox: %d", got)
	}
}

type cancelAtSecondErrContext struct {
	done   chan struct{}
	checks atomic.Int32
}

type cancelAfterChecksContext struct {
	checks   atomic.Int32
	cancelAt int32
}

func (c *cancelAfterChecksContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *cancelAfterChecksContext) Done() <-chan struct{}       { return nil }
func (c *cancelAfterChecksContext) Value(any) any               { return nil }
func (c *cancelAfterChecksContext) Err() error {
	if c.checks.Add(1) >= c.cancelAt {
		return context.Canceled
	}
	return nil
}

func (c *cancelAtSecondErrContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *cancelAtSecondErrContext) Done() <-chan struct{}       { return c.done }
func (c *cancelAtSecondErrContext) Value(any) any               { return nil }
func (c *cancelAtSecondErrContext) Err() error {
	if c.checks.Add(1) >= 2 {
		return context.DeadlineExceeded
	}
	return nil
}

// TestSystemQueueProgressesWhenUserMailboxFull 验收 27 §6.9 第 10 条:
// UserMailbox 满时 SystemQueue 仍可入队并执行;对照 user 路径仍 busy。
func TestSystemQueueProgressesWhenUserMailboxFull(t *testing.T) {
	e := NewOptions(Options{
		UserMailboxSize: 1,
		SystemQueueSize: 8,
		MaxUserBytes:    1 << 20,
	})
	defer e.Stop()

	const key = uint64(42)
	release := make(chan struct{})
	started := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = e.SubmitUser(context.Background(), key, func(ctx context.Context) (any, error) {
			close(started)
			<-release
			return nil, nil
		})
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking user task did not start")
	}

	// 填满 UserMailbox(item=1)。
	queued := make(chan error, 1)
	go func() {
		_, err := e.SubmitUser(context.Background(), key, func(ctx context.Context) (any, error) {
			return nil, nil
		})
		queued <- err
	}()
	time.Sleep(30 * time.Millisecond)

	_, err := e.SubmitUser(context.Background(), key, func(ctx context.Context) (any, error) {
		return "should-not-run", nil
	})
	if !errors.Is(err, ErrMailboxFull) && !errors.Is(err, ErrActorBusy) {
		t.Fatalf("want user busy/full after fill, got %v", err)
	}
	if errors.Is(err, ErrSystemQueueFull) {
		t.Fatalf("user full must not surface as ErrSystemQueueFull")
	}

	var systemRan atomic.Bool
	sysDone := make(chan error, 1)
	go func() {
		_, err := e.SubmitSystem(context.Background(), key, SystemKindLeaseLost, func(ctx context.Context) (any, error) {
			systemRan.Store(true)
			return "lease-lost", nil
		})
		sysDone <- err
	}()

	// System 必须能在 user 满时入队(SubmitSystem 返回前会等执行,故先确认未立刻返回 user-full)。
	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-sysDone:
		t.Fatalf("SubmitSystem returned before release: %v (system should wait behind blocking user)", err)
	default:
	}

	// 对照:满员状态下再 Submit user 仍应 busy/full。
	_, err = e.Submit(context.Background(), key, func(ctx context.Context) (any, error) {
		return nil, nil
	})
	if !errors.Is(err, ErrMailboxFull) && !errors.Is(err, ErrActorBusy) {
		t.Fatalf("want user still busy/full, got %v", err)
	}

	close(release)
	wg.Wait()

	select {
	case err := <-sysDone:
		if err != nil {
			t.Fatalf("SubmitSystem failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("system task did not complete")
	}
	if !systemRan.Load() {
		t.Fatal("system task did not execute")
	}
	if err := <-queued; err != nil {
		t.Fatalf("queued user after blocker failed: %v", err)
	}
}

// TestSystemPriorityOverUser 队列中同时有 system 与后续 user 时,system 先执行。
func TestSystemPriorityOverUser(t *testing.T) {
	e := NewOptions(Options{
		UserMailboxSize: 8,
		SystemQueueSize: 8,
		MaxUserBytes:    1 << 20,
	})
	defer e.Stop()

	const key = uint64(11)
	release := make(chan struct{})
	started := make(chan struct{})

	go e.SubmitUser(context.Background(), key, func(ctx context.Context) (any, error) {
		close(started)
		<-release
		return nil, nil
	})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("blocker did not start")
	}

	var mu sync.Mutex
	order := []string{}
	record := func(label string) {
		mu.Lock()
		order = append(order, label)
		mu.Unlock()
	}

	var wg sync.WaitGroup
	enqueue := func(label string, system bool) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			task := func(ctx context.Context) (any, error) {
				record(label)
				return nil, nil
			}
			if system {
				_, _ = e.SubmitSystem(context.Background(), key, SystemKindDrain, task)
			} else {
				_, _ = e.SubmitUser(context.Background(), key, task)
			}
		}()
		time.Sleep(10 * time.Millisecond) // 保证入队顺序:user 先入队,再 system
	}

	enqueue("user-a", false)
	enqueue("user-b", false)
	enqueue("system", true)
	enqueue("user-c", false)
	time.Sleep(20 * time.Millisecond)

	close(release)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 4 {
		t.Fatalf("order=%v, want 4 entries", order)
	}
	// 释放后应优先清空 system,再处理已排队的 user。
	if order[0] != "system" {
		t.Fatalf("order=%v, want system first among queued work", order)
	}
}

// TestSystemQueueFullIsolatedFromUser SystemQueue 满返回独立错误,且 user 仍可提交。
func TestSystemQueueFullIsolatedFromUser(t *testing.T) {
	e := NewOptions(Options{
		UserMailboxSize: 8,
		SystemQueueSize: 1,
		MaxUserBytes:    1 << 20,
	})
	defer e.Stop()

	const key = uint64(13)
	release := make(chan struct{})
	started := make(chan struct{})

	go e.SubmitUser(context.Background(), key, func(ctx context.Context) (any, error) {
		close(started)
		<-release
		return nil, nil
	})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("blocker did not start")
	}

	// 占满 SystemQueue(capacity=1)。
	sysBlocked := make(chan error, 1)
	go func() {
		_, err := e.SubmitSystem(context.Background(), key, SystemKindPersistenceCompleted, func(ctx context.Context) (any, error) {
			return nil, nil
		})
		sysBlocked <- err
	}()
	time.Sleep(30 * time.Millisecond)

	_, err := e.SubmitSystem(context.Background(), key, SystemKindLeaseLost, func(ctx context.Context) (any, error) {
		return nil, nil
	})
	if !errors.Is(err, ErrSystemQueueFull) {
		t.Fatalf("want ErrSystemQueueFull, got %v", err)
	}
	if errors.Is(err, ErrMailboxFull) || errors.Is(err, ErrActorBusy) {
		t.Fatalf("system full must not be reported as user busy/full")
	}

	// user 路径在有空位时仍可提交。
	userDone := make(chan error, 1)
	go func() {
		_, err := e.SubmitUser(context.Background(), key, func(ctx context.Context) (any, error) {
			return "user-ok", nil
		})
		userDone <- err
	}()
	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-userDone:
		t.Fatalf("user submit returned too early: %v", err)
	default:
	}

	close(release)
	if err := <-sysBlocked; err != nil {
		t.Fatalf("queued system failed: %v", err)
	}
	if err := <-userDone; err != nil {
		t.Fatalf("user submit failed while system was full: %v", err)
	}
}

// TestSubmitUserSizedByteBudget SubmitUserSized 超 byte 预算 → busy;不超则成功。
func TestSubmitUserSizedByteBudget(t *testing.T) {
	e := NewOptions(Options{
		UserMailboxSize: 8,
		SystemQueueSize: 4,
		MaxUserBytes:    1000,
	})
	defer e.Stop()

	const key = uint64(21)
	release := make(chan struct{})
	started := make(chan struct{})

	go e.SubmitUserSized(context.Background(), key, 100, func(ctx context.Context) (any, error) {
		close(started)
		<-release
		return nil, nil
	})
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("blocker did not start")
	}

	// 占用 900 bytes 排队 → 预算用尽。
	queued := make(chan error, 1)
	go func() {
		_, err := e.SubmitUserSized(context.Background(), key, 900, func(ctx context.Context) (any, error) {
			return nil, nil
		})
		queued <- err
	}()
	time.Sleep(30 * time.Millisecond)

	_, err := e.SubmitUserSized(context.Background(), key, 200, func(ctx context.Context) (any, error) {
		return nil, nil
	})
	if !errors.Is(err, ErrActorBusy) && !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("want busy on byte budget, got %v", err)
	}

	// 小 payload 仍可能因 byte 拒绝;用 0 字节验证 item 路径在预算边界上可区分。
	// 预算已满(900),任何正数都应拒绝;释放后应可再提交。
	close(release)
	if err := <-queued; err != nil {
		t.Fatalf("queued sized user failed: %v", err)
	}

	v, err := e.SubmitUserSized(context.Background(), key, 500, func(ctx context.Context) (any, error) {
		return "ok", nil
	})
	if err != nil || v.(string) != "ok" {
		t.Fatalf("within budget submit failed: v=%v err=%v", v, err)
	}
}

func TestActorBusyIsMailboxFull(t *testing.T) {
	if !errors.Is(ErrActorBusy, ErrMailboxFull) {
		t.Fatal("errors.Is(ErrActorBusy, ErrMailboxFull) should be true")
	}
	if !errors.Is(ErrMailboxFull, ErrActorBusy) {
		t.Fatal("errors.Is(ErrMailboxFull, ErrActorBusy) should be true")
	}
	if errors.Is(ErrSystemQueueFull, ErrMailboxFull) || errors.Is(ErrSystemQueueFull, ErrActorBusy) {
		t.Fatal("ErrSystemQueueFull must not match user-full errors")
	}
}

func TestStatsDoesNotBlockMailboxCreation(t *testing.T) {
	e := New(8)
	defer e.Stop()

	started := make(chan struct{})
	release := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		_, err := e.Submit(context.Background(), 1, func(ctx context.Context) (any, error) {
			close(started)
			<-release
			return nil, nil
		})
		errCh <- err
	}()
	<-started

	e.mu.RLock()
	mb := e.boxes[1]
	e.mu.RUnlock()
	if mb == nil {
		t.Fatal("mailbox missing")
	}
	mb.mu.Lock()

	statsDone := make(chan struct{})
	go func() {
		_ = e.Stats()
		close(statsDone)
	}()

	created := make(chan error, 1)
	go func() {
		_, err := e.mailboxFor(context.Background(), 2)
		created <- err
	}()
	select {
	case err := <-created:
		if err != nil {
			mb.mu.Unlock()
			close(release)
			t.Fatalf("mailboxFor: %v", err)
		}
	case <-time.After(2 * time.Second):
		mb.mu.Unlock()
		close(release)
		t.Fatal("mailboxFor blocked behind Stats holding executor lock")
	}

	select {
	case <-statsDone:
		mb.mu.Unlock()
		close(release)
		t.Fatal("Stats returned without waiting for mailbox lock")
	default:
	}
	mb.mu.Unlock()
	<-statsDone
	close(release)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestUserMailboxOldestWaitAndBytesInStats(t *testing.T) {
	e := NewOptions(Options{UserMailboxSize: 8, MaxUserBytes: 64 * 1024})
	defer e.Stop()

	started := make(chan struct{})
	release := make(chan struct{})
	errCh := make(chan error, 2)
	go func() {
		_, err := e.SubmitUserSized(context.Background(), 7, 100, func(ctx context.Context) (any, error) {
			close(started)
			<-release
			return nil, nil
		})
		errCh <- err
	}()
	<-started
	go func() {
		_, err := e.SubmitUserSized(context.Background(), 7, 200, func(ctx context.Context) (any, error) {
			return "queued", nil
		})
		errCh <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		stats := e.Stats()
		if stats.UserQueued >= 1 && stats.UserBytes >= 200 && stats.OldestUserWait > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stats not ready: %+v", e.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}
	stats := e.Stats()
	if stats.UserBytes < 200 {
		t.Fatalf("UserBytes=%d, want >=200", stats.UserBytes)
	}
	if stats.OldestUserWait <= 0 {
		t.Fatalf("OldestUserWait=%v, want >0", stats.OldestUserWait)
	}
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
	if got := e.Stats(); got.UserQueued != 0 || got.UserBytes != 0 || got.OldestUserWait != 0 {
		t.Fatalf("after drain stats=%+v", got)
	}
}

// TestPanicDoesNotKillMailboxAndSurfacesError 验收:panic → ErrActorPanic + OnPanic,
// mailbox goroutine 继续服务后续任务。
func TestPanicDoesNotKillMailboxAndSurfacesError(t *testing.T) {
	var panickedKey atomic.Uint64
	e := NewOptions(Options{
		UserMailboxSize: 8,
		OnPanic: func(key uint64) {
			panickedKey.Store(key)
		},
	})
	defer e.Stop()

	const key = uint64(99)
	_, err := e.Submit(context.Background(), key, func(ctx context.Context) (any, error) {
		panic("boom")
	})
	if !errors.Is(err, ErrActorPanic) {
		t.Fatalf("err=%v, want ErrActorPanic", err)
	}
	if panickedKey.Load() != key {
		t.Fatalf("OnPanic key=%d, want %d", panickedKey.Load(), key)
	}
	if e.Stats().PanicTotal != 1 {
		t.Fatalf("PanicTotal=%d", e.Stats().PanicTotal)
	}

	v, err := e.Submit(context.Background(), key, func(ctx context.Context) (any, error) {
		return "alive", nil
	})
	if err != nil || v.(string) != "alive" {
		t.Fatalf("mailbox died after panic: v=%v err=%v", v, err)
	}
}
