package capaudit

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"farmgame/internal/workflow"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

const testPrefix = "farm:test:workflow"

type storedRecord struct {
	ref       string
	state     workflow.WorkState
	steps     []workflow.StepStatus
	updatedAt time.Time
	terminal  bool
	dueAt     time.Time
}

func writeRecords(t *testing.T, client *redis.Client, records ...storedRecord) {
	t.Helper()
	keys, err := workflow.NewWorkKeys(testPrefix)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, record := range records {
		steps := make([]map[string]any, 0, len(record.steps))
		for _, status := range record.steps {
			steps = append(steps, map[string]any{"status": status})
		}
		data, err := json.Marshal(map[string]any{"state": record.state, "steps": steps})
		if err != nil {
			t.Fatal(err)
		}
		terminal := "0"
		if record.terminal {
			terminal = "1"
		}
		if err := client.HSet(ctx, keys.RecordByRef(record.ref), map[string]any{
			"data":          string(data),
			"updated_at_ms": strconv.FormatInt(record.updatedAt.UnixMilli(), 10),
			"terminal":      terminal,
		}).Err(); err != nil {
			t.Fatal(err)
		}
		if err := client.ZAdd(ctx, keys.Due(), redis.Z{
			Score: float64(record.dueAt.UnixMilli()), Member: record.ref,
		}).Err(); err != nil {
			t.Fatal(err)
		}
	}
}

func newCheck(t *testing.T, now time.Time) (*StepUnknownCheck, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	check, err := NewStepUnknownCheck(StepUnknownConfig{
		Client: client, Prefix: testPrefix, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return check, client
}

// STEP_UNKNOWN is the healthy marker for a step whose answer has not come back
// yet, so a live system always has some. Only one that outlived the retry
// machinery is a residual.
func TestOnlyAnUnresolvedStepOlderThanTheRetryWindowCounts(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	check, client := newCheck(t, now)
	writeRecords(t, client,
		storedRecord{ref: "wr1_fresh", state: workflow.WorkStateStepUnknown, updatedAt: now.Add(-5 * time.Second)},
		storedRecord{ref: "wr1_stuck", state: workflow.WorkStateStepUnknown, updatedAt: now.Add(-2 * time.Minute)},
	)

	got, err := check.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("residual=%d, want only the record that outlived the retry window", got)
	}
}

func TestAnUnknownStepCountsEvenWhenTheWorkMovedOn(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	check, client := newCheck(t, now)
	writeRecords(t, client, storedRecord{
		ref: "wr1_running", state: workflow.WorkStateRunning,
		steps:     []workflow.StepStatus{workflow.StepApplied, workflow.StepUnknown},
		updatedAt: now.Add(-2 * time.Minute),
	})

	got, err := check.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("residual=%d, want the unresolved step counted at step level", got)
	}
}

func TestHealthyWorkIsNotAResidual(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	check, client := newCheck(t, now)
	writeRecords(t, client,
		storedRecord{ref: "wr1_created", state: workflow.WorkStateCreated, updatedAt: now.Add(-time.Hour)},
		storedRecord{ref: "wr1_running", state: workflow.WorkStateRunning,
			steps: []workflow.StepStatus{workflow.StepApplied}, updatedAt: now.Add(-time.Hour)},
		storedRecord{ref: "wr1_done", state: workflow.WorkStateCompleted,
			terminal: true, updatedAt: now.Add(-time.Hour)},
	)

	got, err := check.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Fatalf("residual=%d, want 0; only an unresolved step is this gate's business", got)
	}
}

// A due entry whose record is gone belongs to the due residual gate. Counting
// it here as well would report one fault as two.
func TestAMissingRecordIsLeftToTheDueResidualGate(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	check, client := newCheck(t, now)
	keys, err := workflow.NewWorkKeys(testPrefix)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(context.Background(), keys.Due(), redis.Z{Score: 1, Member: "wr1_orphan"}).Err(); err != nil {
		t.Fatal(err)
	}

	got, err := check.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Fatalf("residual=%d, want 0 for an orphaned index entry", got)
	}
}

// Reading part of an oversized index would produce a count that looks clean
// because the rest was never read.
func TestAnIndexTooLargeToReadIsAFailureNotACount(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	check, err := NewStepUnknownCheck(StepUnknownConfig{
		Client: client, Prefix: testPrefix, Limit: 2, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	writeRecords(t, client,
		storedRecord{ref: "wr1_a", state: workflow.WorkStateRunning, updatedAt: now},
		storedRecord{ref: "wr1_b", state: workflow.WorkStateRunning, updatedAt: now},
		storedRecord{ref: "wr1_c", state: workflow.WorkStateRunning, updatedAt: now},
	)

	if _, err := check.Observe(context.Background()); err == nil {
		t.Fatal("an index beyond the readable limit must fail rather than report a partial count")
	}
}

func TestARecordThatCannotBeAgedIsAFailure(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	check, client := newCheck(t, now)
	keys, err := workflow.NewWorkKeys(testPrefix)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := client.HSet(ctx, keys.RecordByRef("wr1_noage"), map[string]any{
		"data": `{"state":3}`, "terminal": "0",
	}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(ctx, keys.Due(), redis.Z{Score: 1, Member: "wr1_noage"}).Err(); err != nil {
		t.Fatal(err)
	}

	if _, err := check.Observe(ctx); err == nil {
		t.Fatal("a record with no update time cannot be aged and must not be silently skipped")
	}
}

func TestTheGateIsNamedByTheContract(t *testing.T) {
	check, _ := newCheck(t, time.Now())
	if check.Gate() != "workflow_step_unknown_residual" {
		t.Fatalf("Gate()=%s", check.Gate())
	}
}
