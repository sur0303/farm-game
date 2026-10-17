package idle

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestSweeper(clock *fakeClock, opts Options, retire RetireFunc) *Sweeper {
	opts.Now = clock.Now
	if opts.IdleTimeout == 0 {
		opts.IdleTimeout = time.Minute
	}
	if opts.BucketWidth == 0 {
		opts.BucketWidth = 10 * time.Second
	}
	return New(opts, retire)
}

// TestIdleKeyBecomesCandidateAfterTimeout 验证:key 在 idle timeout 后的下一个桶边界成为候选。
func TestIdleKeyBecomesCandidateAfterTimeout(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	var retired []uint64
	s := newTestSweeper(clock, Options{}, func(ctx context.Context, key uint64) Result {
		retired = append(retired, key)
		return Result{Decision: DecisionRetired}
	})

	s.Touch(1)
	s.SweepOnce(context.Background())
	if len(retired) != 0 {
		t.Fatalf("retired=%v, want none before timeout", retired)
	}

	clock.Advance(time.Minute + 10*time.Second)
	if got := s.SweepOnce(context.Background()); got != 1 {
		t.Fatalf("candidates=%d, want 1", got)
	}
	if len(retired) != 1 || retired[0] != 1 {
		t.Fatalf("retired=%v, want [1]", retired)
	}
	if got := s.Stats().TrackedKeys; got != 0 {
		t.Fatalf("TrackedKeys=%d, want 0 after retire", got)
	}
}

// TestTouchKeepsActiveKeyOutOfExpiredBucket 验证:活跃 key 只移动桶归属,不被扫描。
func TestTouchKeepsActiveKeyOutOfExpiredBucket(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	calls := 0
	s := newTestSweeper(clock, Options{}, func(ctx context.Context, key uint64) Result {
		calls++
		return Result{Decision: DecisionRetired}
	})

	s.Touch(1)
	for i := 0; i < 12; i++ {
		clock.Advance(20 * time.Second)
		s.Touch(1)
		s.SweepOnce(context.Background())
	}
	if calls != 0 {
		t.Fatalf("retire calls=%d, want 0 for continuously active key", calls)
	}
	if got := s.Stats().Buckets; got > 1 {
		t.Fatalf("Buckets=%d, want <=1 (无桶堆积)", got)
	}
}

// TestBusyCandidateRetriesWithoutBusyLoop 验证:blocked 候选按 BlockedRetryDelay
// 重新到期并统计 reason,不会在同一 tick 内反复处理。
func TestBusyCandidateRetriesWithoutBusyLoop(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	calls := 0
	s := newTestSweeper(clock, Options{BlockedRetryDelay: 30 * time.Second}, func(ctx context.Context, key uint64) Result {
		calls++
		if calls < 3 {
			return Result{Decision: DecisionBusy, Reasons: []string{"turn_running"}}
		}
		return Result{Decision: DecisionRetired}
	})

	s.Touch(7)
	clock.Advance(70 * time.Second)
	s.SweepOnce(context.Background())
	if calls != 1 {
		t.Fatalf("calls=%d after first sweep, want 1", calls)
	}
	// 同一 tick 不再处理;延迟未到期也不处理。
	s.SweepOnce(context.Background())
	if calls != 1 {
		t.Fatalf("calls=%d (busy loop?), want 1", calls)
	}
	clock.Advance(31 * time.Second)
	s.SweepOnce(context.Background())
	if calls != 2 {
		t.Fatalf("calls=%d after retry delay, want 2", calls)
	}
	clock.Advance(31 * time.Second)
	s.SweepOnce(context.Background())
	if calls != 3 {
		t.Fatalf("calls=%d, want 3", calls)
	}
	stats := s.Stats()
	if stats.BlockedTotal["turn_running"] != 2 {
		t.Fatalf("blocked[turn_running]=%d, want 2", stats.BlockedTotal["turn_running"])
	}
	if stats.RetiredTotal != 1 {
		t.Fatalf("RetiredTotal=%d, want 1", stats.RetiredTotal)
	}
}

// TestNotIdleCandidateReschedules 验证:NotIdle 候选重新计 idle。
func TestNotIdleCandidateReschedules(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	calls := 0
	s := newTestSweeper(clock, Options{}, func(ctx context.Context, key uint64) Result {
		calls++
		if calls == 1 {
			return Result{Decision: DecisionNotIdle}
		}
		return Result{Decision: DecisionRetired}
	})

	s.Touch(3)
	clock.Advance(70 * time.Second)
	s.SweepOnce(context.Background())
	if calls != 1 {
		t.Fatalf("calls=%d, want 1", calls)
	}
	// NotIdle 重新计时:一个 bucket 内不会立即重试。
	clock.Advance(20 * time.Second)
	s.SweepOnce(context.Background())
	if calls != 1 {
		t.Fatalf("calls=%d, want 1 (NotIdle 重新计时)", calls)
	}
	clock.Advance(50 * time.Second)
	s.SweepOnce(context.Background())
	if calls != 2 {
		t.Fatalf("calls=%d, want 2", calls)
	}
}

// TestMaxPerTickCapsWorkAndReportsLag 验证:单 tick 上限与 lag 统计。
func TestMaxPerTickCapsWorkAndReportsLag(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	processed := 0
	s := newTestSweeper(clock, Options{MaxPerTick: 3}, func(ctx context.Context, key uint64) Result {
		processed++
		return Result{Decision: DecisionRetired}
	})

	for key := uint64(1); key <= 10; key++ {
		s.Touch(key)
	}
	clock.Advance(70 * time.Second)
	if got := s.SweepOnce(context.Background()); got != 3 {
		t.Fatalf("candidates=%d, want 3 (MaxPerTick)", got)
	}
	if got := s.Stats().LastLagSeconds; got <= 0 {
		t.Fatalf("lag=%v, want >0 (有未处理积压)", got)
	}
	// 后续 tick 继续消化。
	total := processed
	for total < 10 {
		total += s.SweepOnce(context.Background())
	}
	if processed != 10 {
		t.Fatalf("processed=%d, want 10", processed)
	}
	if got := s.Stats().LastLagSeconds; got != 0 {
		t.Fatalf("lag=%v, want 0 after drain", got)
	}
}

// TestForgetStopsTracking 验证:Forget 后不再扫描。
func TestForgetStopsTracking(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	calls := 0
	s := newTestSweeper(clock, Options{}, func(ctx context.Context, key uint64) Result {
		calls++
		return Result{Decision: DecisionRetired}
	})
	s.Touch(5)
	s.Forget(5)
	clock.Advance(2 * time.Minute)
	s.SweepOnce(context.Background())
	if calls != 0 {
		t.Fatalf("calls=%d, want 0 after Forget", calls)
	}
}

// TestManyKeysBucketsBounded 验证:大量 key 分散在少量桶中,扫描成本与桶相关。
func TestManyKeysBucketsBounded(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	s := newTestSweeper(clock, Options{}, func(ctx context.Context, key uint64) Result {
		return Result{Decision: DecisionRetired}
	})
	const n = 10000
	for key := uint64(1); key <= n; key++ {
		s.Touch(key)
	}
	if got := s.Stats().TrackedKeys; got != n {
		t.Fatalf("TrackedKeys=%d, want %d", got, n)
	}
	if got := s.Stats().Buckets; got > 2 {
		t.Fatalf("Buckets=%d, want <=2 for synchronized touches", got)
	}
	clock.Advance(70 * time.Second)
	s.opts.MaxPerTick = n
	s.SweepOnce(context.Background())
	if got := s.Stats().TrackedKeys; got != 0 {
		t.Fatalf("TrackedKeys=%d after sweep, want 0", got)
	}
}
