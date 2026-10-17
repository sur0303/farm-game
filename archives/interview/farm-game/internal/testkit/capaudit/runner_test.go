package capaudit

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type stubCheck struct {
	gate  string
	value int64
	err   error

	mu    sync.Mutex
	calls int
}

func (c *stubCheck) Gate() string { return c.gate }

func (c *stubCheck) Observe(ctx context.Context) (int64, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	if c.err != nil {
		return 0, c.err
	}
	if deadline, ok := ctx.Deadline(); !ok || deadline.IsZero() {
		return 0, errors.New("check ran without a deadline")
	}
	return c.value, nil
}

func (c *stubCheck) called() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func newRunner(t *testing.T, now func() time.Time, checks ...Check) *Runner {
	t.Helper()
	runner, err := NewRunner(Config{
		Interval: time.Minute, FreshFor: 3 * time.Minute, Timeout: 10 * time.Second, Now: now,
	}, checks...)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestAFreshAnswerIsServedAndAStaleOneIsNot(t *testing.T) {
	clock := time.Unix(1_800_000_000, 0)
	runner := newRunner(t, func() time.Time { return clock }, &stubCheck{gate: "duplicate_grant_total"})
	runner.RunOnce(context.Background())

	value, err := runner.Value("duplicate_grant_total")
	if err != nil || value != 0 {
		t.Fatalf("fresh answer value=%v err=%v, want 0 and no error", value, err)
	}

	clock = clock.Add(4 * time.Minute)
	if _, err := runner.Value("duplicate_grant_total"); err == nil {
		t.Fatal("a stale answer must not be served; the gate has to read as unobserved")
	} else if !strings.Contains(err.Error(), "beyond the") {
		t.Fatalf("staleness error should say how old the answer is, got %v", err)
	}
}

// A scan that breaks must not leave its last clean number in place, or the run
// would keep reading a result nobody is producing any more.
func TestAFailedCheckReplacesItsPreviousAnswer(t *testing.T) {
	clock := time.Unix(1_800_000_000, 0)
	check := &stubCheck{gate: "duplicate_debit_total", value: 0}
	runner := newRunner(t, func() time.Time { return clock }, check)
	runner.RunOnce(context.Background())
	if _, err := runner.Value("duplicate_debit_total"); err != nil {
		t.Fatalf("first answer should be usable: %v", err)
	}

	check.err = errors.New("redis unreachable")
	runner.RunOnce(context.Background())
	if _, err := runner.Value("duplicate_debit_total"); err == nil {
		t.Fatal("a failed check must invalidate the gate rather than keep the old value")
	}
}

func TestAGateIsUnobservedUntilItHasBeenEvaluated(t *testing.T) {
	runner := newRunner(t, time.Now, &stubCheck{gate: "duplicate_grant_total"})
	if _, err := runner.Value("duplicate_grant_total"); err == nil {
		t.Fatal("a gate that has never run must not report a value")
	}
	if _, err := runner.Value("never_registered"); err == nil {
		t.Fatal("an unknown gate must not report a value")
	}
}

func TestANegativeCountIsTreatedAsABrokenCheck(t *testing.T) {
	runner := newRunner(t, time.Now, &stubCheck{gate: "duplicate_grant_total", value: -1})
	runner.RunOnce(context.Background())
	if _, err := runner.Value("duplicate_grant_total"); err == nil {
		t.Fatal("a negative count is not a count and must not be served")
	}
}

func TestRunEvaluatesOnTheIntervalUntilTheContextEnds(t *testing.T) {
	check := &stubCheck{gate: "duplicate_grant_total"}
	runner, err := NewRunner(Config{
		Interval: 10 * time.Millisecond, FreshFor: time.Minute, Timeout: time.Second,
	}, check)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { runner.Run(ctx); close(done) }()

	deadline := time.Now().Add(2 * time.Second)
	for check.called() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop when the context ended")
	}
	if check.called() < 3 {
		t.Fatalf("check ran %d times, want repeated evaluation", check.called())
	}
}

func TestConfigurationThatCouldServeStaleOrDuplicateAnswersIsRefused(t *testing.T) {
	tests := []struct {
		name   string
		config Config
		checks []Check
	}{
		{
			name:   "freshness not longer than the interval",
			config: Config{Interval: time.Minute, FreshFor: time.Minute, Timeout: time.Second},
			checks: []Check{&stubCheck{gate: "duplicate_grant_total"}},
		},
		{
			name:   "no checks",
			config: Config{Interval: time.Minute, FreshFor: 2 * time.Minute, Timeout: time.Second},
		},
		{
			name:   "two checks on one gate",
			config: Config{Interval: time.Minute, FreshFor: 2 * time.Minute, Timeout: time.Second},
			checks: []Check{&stubCheck{gate: "duplicate_grant_total"}, &stubCheck{gate: "duplicate_grant_total"}},
		},
		{
			name:   "unnamed gate",
			config: Config{Interval: time.Minute, FreshFor: 2 * time.Minute, Timeout: time.Second},
			checks: []Check{&stubCheck{}},
		},
		{
			name:   "no timeout",
			config: Config{Interval: time.Minute, FreshFor: 2 * time.Minute},
			checks: []Check{&stubCheck{gate: "duplicate_grant_total"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewRunner(tt.config, tt.checks...); err == nil {
				t.Fatal("configuration should have been refused")
			}
		})
	}
}
