package leasebatch

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{
		Limits:      Limits{MaxItems: 3, MaxBytes: 100, MaxDispatchDuration: 20 * time.Millisecond},
		Schedule:    Schedule{Interval: time.Second, BucketCount: 10, MaxJitter: 10 * time.Millisecond},
		MaxPlanScan: 32,
	}
}

func testItem(key, holder string, due time.Time, bytes int) Item[string] {
	return Item[string]{Key: key, Identity: key, Holder: holder, Due: due,
		Deadline: due.Add(time.Minute), EstimatedBytes: bytes, Payload: key}
}

func TestStablePhaseIsDeterministicAndDistributed(t *testing.T) {
	schedule := testConfig().Schedule
	counts := make([]int, schedule.BucketCount)
	for i := 0; i < 10_000; i++ {
		identity := fmt.Sprintf("gate-a:boot-a:route:%d", i)
		bucketA, jitterA, err := StablePhase(identity, schedule)
		if err != nil {
			t.Fatal(err)
		}
		bucketB, jitterB, _ := StablePhase(identity, schedule)
		if bucketA != bucketB || jitterA != jitterB {
			t.Fatalf("identity %q phase changed", identity)
		}
		if jitterA < 0 || jitterA > schedule.MaxJitter {
			t.Fatalf("jitter=%s exceeds cap", jitterA)
		}
		counts[bucketA]++
	}
	for bucket, count := range counts {
		if count < 800 || count > 1200 {
			t.Fatalf("bucket %d count=%d outside deterministic distribution bound", bucket, count)
		}
	}
}

func TestValidateSafetyRejectsDurationOverflow(t *testing.T) {
	cfg := testConfig()
	cfg.Interval = time.Duration(1<<63 - 1)
	cfg.BucketCount = 1
	cfg.MaxJitter = 0
	if err := cfg.ValidateSafety(time.Duration(1<<63-1), 0); err == nil {
		t.Fatal("overflowing interval plus dispatch was accepted")
	}
}

func TestStablePhaseSupportsMaximumDurationWithoutSignedOverflow(t *testing.T) {
	maximum := time.Duration(1<<63 - 1)
	_, jitter, err := StablePhase("max-duration", Schedule{
		Interval: maximum, BucketCount: 1, MaxJitter: maximum,
	})
	if err != nil {
		t.Fatal(err)
	}
	if jitter < 0 || jitter > maximum {
		t.Fatalf("jitter=%s outside [0,%s]", jitter, maximum)
	}
}

func TestPlannerEnforcesItemAndByteEdges(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	cfg := testConfig()
	cfg.MaxItems = 1
	planner, _ := NewPlanner[string](cfg)
	_ = planner.Upsert(testItem("a", "holder", now, 40))
	_ = planner.Upsert(testItem("b", "holder", now, 40))
	if batch := planner.Plan(now, false); len(batch.Items) != 1 {
		t.Fatalf("max_items=1 planned %d", len(batch.Items))
	}

	cfg = testConfig()
	planner, _ = NewPlanner[string](cfg)
	_ = planner.Upsert(testItem("a", "holder", now, 60))
	_ = planner.Upsert(testItem("b", "holder", now, 40))
	_ = planner.Upsert(testItem("c", "holder", now, 1))
	batch := planner.Plan(now, false)
	if len(batch.Items) != 2 || batch.EstimatedBytes != 100 {
		t.Fatalf("byte boundary batch=%+v", batch)
	}

	cfg = testConfig()
	cfg.MaxBytes = int(^uint(0) >> 1)
	planner, _ = NewPlanner[string](cfg)
	_ = planner.Upsert(testItem("large-a", "holder", now, cfg.MaxBytes-5))
	_ = planner.Upsert(testItem("large-b", "holder", now, 10))
	batch = planner.Plan(now, false)
	if len(batch.Items) != 1 || batch.EstimatedBytes != cfg.MaxBytes-5 {
		t.Fatalf("byte addition overflow escaped cap: %+v", batch)
	}
}

func TestPlannerEmitsOversizeAloneWithoutStarvation(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	planner, _ := NewPlanner[string](testConfig())
	_ = planner.Upsert(testItem("a-oversize", "holder", now, 101))
	_ = planner.Upsert(testItem("b-normal", "holder", now, 10))
	batch := planner.Plan(now, false)
	if !batch.Oversize || len(batch.Items) != 1 || batch.Items[0].Key != "a-oversize" {
		t.Fatalf("oversize batch=%+v", batch)
	}
	next := planner.Plan(now, false)
	if next.Oversize || len(next.Items) != 1 || next.Items[0].Key != "b-normal" {
		t.Fatalf("normal item starved: %+v", next)
	}
}

func TestPlannerHolderIsolationRollbackLargeJumpAndDrain(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	planner, _ := NewPlanner[string](testConfig())
	for i, holder := range []string{"holder-a", "holder-b", "holder-a", "holder-b"} {
		_ = planner.Upsert(testItem(fmt.Sprintf("%d", i), holder, now, 10))
	}
	batch := planner.Plan(now, false)
	for _, item := range batch.Items {
		if item.Holder != batch.Holder {
			t.Fatalf("mixed holder batch=%+v", batch)
		}
	}
	_ = planner.Plan(now.Add(-time.Hour), false)
	if planner.Stats().ClockRollback != 1 {
		t.Fatalf("rollback stats=%+v", planner.Stats())
	}
	jump := planner.Plan(now.Add(24*time.Hour), false)
	if len(jump.Items) > testConfig().MaxItems {
		t.Fatalf("large jump escaped bound: %d", len(jump.Items))
	}
	_ = planner.Upsert(testItem("drain-pending", "holder", now.Add(48*time.Hour), 10))
	cleared := planner.Stop()
	if cleared == 0 || planner.Stats().Backlog != 0 || !planner.Stats().Stopped {
		t.Fatalf("drain cleared=%d stats=%+v", cleared, planner.Stats())
	}
	if got := planner.Plan(now.Add(48*time.Hour), true); len(got.Items) != 0 {
		t.Fatalf("stopped planner produced work: %+v", got)
	}
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

func TestDispatchAppliesDurationDeadlineAndReportsLateStore(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1_800_000_000, 0)}
	maxDuration := 20 * time.Millisecond
	report, err := Dispatch(context.Background(), clock, maxDuration, func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > maxDuration || time.Until(deadline) <= 0 {
			t.Fatalf("dispatch context deadline=%v ok=%v", deadline, ok)
		}
		clock.now = clock.now.Add(maxDuration + time.Millisecond)
		return nil
	})
	if err != nil || !report.Exceeded || report.Duration != maxDuration+time.Millisecond {
		t.Fatalf("report=%+v err=%v", report, err)
	}
}

func TestHolderHandoffFencesInflightGeneration(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	planner, _ := NewPlanner[string](testConfig())
	old := testItem("lease", "holder-a/boot-a/epoch-1", now, 10)
	old.Payload = "generation-1"
	_ = planner.Upsert(old)
	inflight := planner.Plan(now, false).Items[0]

	canonicalHolder := "holder-b/boot-b/epoch-2"
	canonicalGeneration := "generation-2"
	lateApplies := inflight.Holder == canonicalHolder && inflight.Payload == canonicalGeneration
	if lateApplies {
		t.Fatal("old holder result crossed handoff generation fence")
	}
	newItem := testItem("lease-v2", canonicalHolder, now, 10)
	newItem.Payload = canonicalGeneration
	if err := planner.Upsert(newItem); err != nil {
		t.Fatal(err)
	}
	batch := planner.Plan(now, false)
	if len(batch.Items) != 1 || batch.Holder != canonicalHolder || batch.Items[0].Payload != canonicalGeneration {
		t.Fatalf("new holder generation not independently scheduled: %+v", batch)
	}
}

func TestPlannerMovesSameKeyBetweenHolderIndexes(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	planner, _ := NewPlanner[string](testConfig())
	item := testItem("same-fence", "holder-a", now, 10)
	if err := planner.Upsert(item); err != nil {
		t.Fatal(err)
	}
	item.Holder = "holder-b"
	item.Payload = "generation-b"
	if err := planner.Upsert(item); err != nil {
		t.Fatal(err)
	}
	batch := planner.Plan(now, false)
	if len(batch.Items) != 1 || batch.Holder != "holder-b" || batch.Items[0].Payload != "generation-b" {
		t.Fatalf("same-key handoff remained in old Holder index: %+v", batch)
	}
	if stats := planner.Stats(); stats.Backlog != 0 {
		t.Fatalf("same-key handoff leaked index entries: %+v", stats)
	}
}
