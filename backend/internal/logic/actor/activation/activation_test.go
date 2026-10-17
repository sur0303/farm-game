package activation

import (
	"errors"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	actorruntime "farmgame/internal/logic/actor/runtime"

	"github.com/prometheus/client_golang/prometheus"
)

func TestFSM_AbsentLoadingActive(t *testing.T) {
	c := New(Options{ActivationRatePerSec: 0})
	const key = uint64(1)
	if c.StateOf(key) != Absent {
		t.Fatalf("state=%s", c.StateOf(key))
	}
	if err := c.BeginLoad(key); err != nil {
		t.Fatal(err)
	}
	if c.StateOf(key) != Loading {
		t.Fatalf("state=%s", c.StateOf(key))
	}
	if err := c.CompleteLoad(key); err != nil {
		t.Fatal(err)
	}
	if c.StateOf(key) != Active {
		t.Fatalf("state=%s", c.StateOf(key))
	}
	action, err := c.AdmitUserCommand(key, true)
	if err != nil || action != AdmitProceed {
		t.Fatalf("admit action=%v err=%v", action, err)
	}
}

func TestLoadStorm_RejectsWhenConcurrentOrPendingExceeded(t *testing.T) {
	opts := DefaultOptions()
	opts.MaxConcurrentLoads = 1
	opts.MaxPendingLoads = 2
	opts.ActivationRatePerSec = 0
	c := New(opts)

	if err := c.BeginLoad(1); err != nil {
		t.Fatal(err)
	}
	if err := c.BeginLoad(2); !errors.Is(err, ErrActivationThrottled) {
		t.Fatalf("concurrent full: err=%v, want ErrActivationThrottled", err)
	}

	if err := c.EnqueuePending(); err != nil {
		t.Fatal(err)
	}
	if err := c.EnqueuePending(); err != nil {
		t.Fatal(err)
	}
	if err := c.EnqueuePending(); !errors.Is(err, ErrTooManyPendingLoads) {
		t.Fatalf("pending full enqueue: err=%v", err)
	}
	if err := c.BeginLoad(3); !errors.Is(err, ErrTooManyPendingLoads) {
		t.Fatalf("pending+concurrent full: err=%v, want ErrTooManyPendingLoads", err)
	}

	if err := c.CompleteLoad(1); err != nil {
		t.Fatal(err)
	}
	c.DequeuePending()
	c.DequeuePending()
	if err := c.BeginLoad(2); err != nil {
		t.Fatalf("after release: %v", err)
	}
}

func TestLoadFailureBackoff(t *testing.T) {
	now := time.Unix(100, 0)
	c := New(Options{
		ActivationRatePerSec: 0,
		LoadFailureBackoff:   time.Second,
	})
	c.SetClock(func() time.Time { return now })

	if err := c.BeginLoad(7); err != nil {
		t.Fatal(err)
	}
	if err := c.FailLoad(7); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AdmitUserCommand(7, true); !errors.Is(err, ErrLoadBackoff) {
		t.Fatalf("admit during backoff: %v", err)
	}
	if err := c.BeginLoad(7); !errors.Is(err, ErrLoadBackoff) {
		t.Fatalf("begin during backoff: %v", err)
	}

	now = now.Add(time.Second)
	action, err := c.AdmitUserCommand(7, true)
	if err != nil || action != AdmitNeedsLoad {
		t.Fatalf("after backoff admit=%v err=%v", action, err)
	}
	if err := c.BeginLoad(7); err != nil {
		t.Fatal(err)
	}
}

func TestPassivating_WritableCancelsToActive(t *testing.T) {
	c := New(Options{ActivationRatePerSec: 0})
	mustActivate(t, c, 9)

	if err := c.BeginPassivate(9); err != nil {
		t.Fatal(err)
	}
	if c.StateOf(9) != Quiescing {
		t.Fatalf("state=%s", c.StateOf(9))
	}
	action, err := c.AdmitUserCommand(9, true)
	if err != nil || action != AdmitProceed {
		t.Fatalf("admit=%v err=%v", action, err)
	}
	if c.StateOf(9) != Active {
		t.Fatalf("state=%s, want ACTIVE", c.StateOf(9))
	}
}

func TestPassivating_NotWritableReturnsStaleRouteAndDoesNotResurrect(t *testing.T) {
	c := New(Options{ActivationRatePerSec: 0})
	mustActivate(t, c, 11)

	if err := c.BeginPassivate(11); err != nil {
		t.Fatal(err)
	}
	action, err := c.AdmitUserCommand(11, false)
	if action != 0 || !errors.Is(err, ErrStaleRoute) {
		t.Fatalf("admit=%v err=%v", action, err)
	}
	if !errors.Is(err, actorrouting.ErrStaleRoute) {
		t.Fatalf("errors.Is actorrouting.ErrStaleRoute failed: %v", err)
	}
	if c.StateOf(11) != Quiescing {
		t.Fatalf("state=%s, must stay QUIESCING (not resurrect to ACTIVE)", c.StateOf(11))
	}
}

func TestIllegalTransitions(t *testing.T) {
	c := New(Options{ActivationRatePerSec: 0})
	cases := []struct {
		name string
		run  func() error
	}{
		{"CompleteLoad from ABSENT", func() error { return c.CompleteLoad(1) }},
		{"FailLoad from ABSENT", func() error { return c.FailLoad(1) }},
		{"BeginPassivate from ABSENT", func() error { return c.BeginPassivate(1) }},
		{"BeginLoad from ACTIVE", func() error {
			mustActivate(t, c, 2)
			return c.BeginLoad(2)
		}},
		{"Fence from LOADING", func() error {
			if err := c.BeginLoad(3); err != nil {
				return err
			}
			return c.Fence(3)
		}},
		{"FinishPassivate from ACTIVE", func() error {
			mustActivate(t, c, 4)
			return c.FinishPassivate(4)
		}},
		{"CancelPassivate from ACTIVE", func() error {
			mustActivate(t, c, 5)
			return c.CancelPassivate(5)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("err=%v, want ErrInvalidTransition", err)
			}
		})
	}
}

func TestAdmitFencedAndDraining(t *testing.T) {
	c := New(Options{ActivationRatePerSec: 0})
	mustActivate(t, c, 20)
	if err := c.Fence(20); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AdmitUserCommand(20, true); !errors.Is(err, ErrStaleRoute) {
		t.Fatalf("fenced admit: %v", err)
	}
	c.MarkAbsent(20)

	mustActivate(t, c, 21)
	if err := c.BeginDrain(21); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AdmitUserCommand(21, true); !errors.Is(err, ErrStaleRoute) {
		t.Fatalf("draining admit: %v", err)
	}
}

func TestAdmitLoadingBusy(t *testing.T) {
	c := New(Options{ActivationRatePerSec: 0})
	if err := c.BeginLoad(30); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AdmitUserCommand(30, true); !errors.Is(err, ErrActorBusy) {
		t.Fatalf("err=%v", err)
	}
	if !errors.Is(ErrActorBusy, actorruntime.ErrActorBusy) {
		t.Fatal("ErrActorBusy should align with runtime")
	}
}

func TestOnPanicReturnsAbsent(t *testing.T) {
	c := New(Options{ActivationRatePerSec: 0})
	mustActivate(t, c, 40)
	c.OnPanic(40)
	if c.StateOf(40) != Absent {
		t.Fatalf("state=%s", c.StateOf(40))
	}
	if c.Stats().PanicTotal != 1 {
		t.Fatalf("panicTotal=%d", c.Stats().PanicTotal)
	}
	action, err := c.AdmitUserCommand(40, true)
	if err != nil || action != AdmitNeedsLoad {
		t.Fatalf("after panic admit=%v err=%v", action, err)
	}
}

func TestResolveUnknownTransitions(t *testing.T) {
	c := New(Options{ActivationRatePerSec: 0})
	mustActivate(t, c, 50)
	if err := c.BeginResolveUnknown(50); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AdmitUserCommand(50, true); !errors.Is(err, ErrActorBusy) {
		t.Fatalf("err=%v", err)
	}
	if err := c.ResolveUnknownToActive(50); err != nil {
		t.Fatal(err)
	}
	if c.StateOf(50) != Active {
		t.Fatalf("state=%s", c.StateOf(50))
	}

	mustActivate(t, c, 51)
	if err := c.BeginResolveUnknown(51); err != nil {
		t.Fatal(err)
	}
	if err := c.ResolveUnknownToAbsent(51); err != nil {
		t.Fatal(err)
	}
	if c.StateOf(51) != Absent {
		t.Fatalf("state=%s", c.StateOf(51))
	}
}

func mustActivate(t *testing.T, c *Controller, key uint64) {
	t.Helper()
	if c.StateOf(key) == Active {
		return
	}
	if err := c.BeginLoad(key); err != nil {
		t.Fatal(err)
	}
	if err := c.CompleteLoad(key); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterActivationMetricsReflectsStateCounts(t *testing.T) {
	c := New(Options{ActivationRatePerSec: 0})
	reg := prometheus.NewRegistry()
	RegisterActivationMetrics(reg, "farm", c.Stats)
	mustActivate(t, c, 1)
	mustActivate(t, c, 2)
	if err := c.BeginPassivate(2); err != nil {
		t.Fatal(err)
	}
	stats := c.Stats()
	if stats.StateActive != 1 || stats.StateQuiescing != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var activeCount, quiescingCount float64
	for _, family := range families {
		if family.GetName() != "farm_actor_activation_actors" {
			continue
		}
		for _, metric := range family.Metric {
			var state string
			for _, label := range metric.Label {
				if label.GetName() == "state" {
					state = label.GetValue()
				}
			}
			switch state {
			case "ACTIVE":
				activeCount = metric.GetGauge().GetValue()
			case "QUIESCING":
				quiescingCount = metric.GetGauge().GetValue()
			}
		}
	}
	if activeCount != 1 || quiescingCount != 1 {
		t.Fatalf("activation_actors ACTIVE=%v QUIESCING=%v", activeCount, quiescingCount)
	}
}

// TestForgetIfAbsentReclaimsEntry 验证:完整钝化后 ABSENT entry 可从 map 删除,
// 非 ABSENT 状态的 entry 不会被误删。
func TestForgetIfAbsentReclaimsEntry(t *testing.T) {
	c := New(DefaultOptions())
	const key = uint64(7)
	if err := c.BeginLoad(key); err != nil {
		t.Fatalf("BeginLoad: %v", err)
	}
	if err := c.CompleteLoad(key); err != nil {
		t.Fatalf("CompleteLoad: %v", err)
	}
	if err := c.BeginPassivate(key); err != nil {
		t.Fatalf("BeginPassivate: %v", err)
	}
	if c.ForgetIfAbsent(key) {
		t.Fatal("ForgetIfAbsent must not remove QUIESCING entry")
	}
	if err := c.CommitPassivate(key); err != nil {
		t.Fatalf("CommitPassivate: %v", err)
	}
	if err := c.FinishPassivate(key); err != nil {
		t.Fatalf("FinishPassivate: %v", err)
	}
	if got := c.Stats().PassivateCompletedTotal; got != 1 {
		t.Fatalf("PassivateCompletedTotal=%d, want 1", got)
	}
	if !c.ForgetIfAbsent(key) {
		t.Fatal("ForgetIfAbsent must remove ABSENT entry")
	}
	if got := c.Stats().TrackedKeys; got != 0 {
		t.Fatalf("TrackedKeys=%d, want 0 after forget", got)
	}
	if got := c.Stats().EntryForgottenTotal; got != 1 {
		t.Fatalf("EntryForgottenTotal=%d, want 1", got)
	}
}

// TestReactivationAfterPassivation 验证:钝化后再次加载计为 reactivation。
func TestReactivationAfterPassivation(t *testing.T) {
	c := New(DefaultOptions())
	const key = uint64(9)
	if err := c.BeginLoad(key); err != nil {
		t.Fatalf("BeginLoad: %v", err)
	}
	if err := c.CompleteLoad(key); err != nil {
		t.Fatalf("CompleteLoad: %v", err)
	}
	if err := c.BeginPassivate(key); err != nil {
		t.Fatalf("BeginPassivate: %v", err)
	}
	if err := c.CommitPassivate(key); err != nil {
		t.Fatalf("CommitPassivate: %v", err)
	}
	if err := c.FinishPassivate(key); err != nil {
		t.Fatalf("FinishPassivate: %v", err)
	}
	// 未 Forget 时重新加载:tombstone 仍在,计 reactivation。
	if err := c.BeginLoad(key); err != nil {
		t.Fatalf("BeginLoad after passivation: %v", err)
	}
	if got := c.Stats().ReactivatedTotal; got != 1 {
		t.Fatalf("ReactivatedTotal=%d, want 1", got)
	}
	if err := c.CompleteLoad(key); err != nil {
		t.Fatalf("CompleteLoad: %v", err)
	}
}
