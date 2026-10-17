package capaudit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"farmgame/internal/workflow"

	"github.com/redis/go-redis/v9"
)

// StepUnknownGate is the contract name this check answers for.
const StepUnknownGate = "workflow_step_unknown_residual"

// DefaultStepUnknownAge is how long a record may sit in STEP_UNKNOWN before it
// counts as abandoned rather than in flight.
//
// STEP_UNKNOWN is the healthy marker for "the step was sent and the answer has
// not come back", so a live system always has some. What it may not have is a
// record still saying that long after the machinery that resolves it has had
// its turn: a claim lease of at most 30s plus a retry backoff capped at 30s.
// Below that, counting would flag ordinary retries; above it, a genuinely
// stuck record would stay invisible.
const DefaultStepUnknownAge = 40 * time.Second

// StepUnknownCheck counts workflow records left with an unresolved step.
//
// It reads the due index rather than scanning the keyspace: the index names
// every record that is not terminal, and terminal records are removed from it,
// so the walk is bounded by work in flight instead of by history.
type StepUnknownCheck struct {
	client *redis.Client
	keys   workflow.WorkKeys
	age    time.Duration
	// limit bounds one pass. A due index larger than this means the audit
	// cannot see all of it, which is reported as a failure rather than as a
	// count over the part it happened to read.
	limit int64
	now   func() time.Time
}

type StepUnknownConfig struct {
	Client *redis.Client
	// Prefix is WORKFLOW_STORE_PREFIX, the same value the services are given.
	Prefix string
	Age    time.Duration
	Limit  int64
	Now    func() time.Time
}

func NewStepUnknownCheck(config StepUnknownConfig) (*StepUnknownCheck, error) {
	if config.Client == nil {
		return nil, errors.New("capaudit: redis client is required")
	}
	keys, err := workflow.NewWorkKeys(config.Prefix)
	if err != nil {
		return nil, err
	}
	age := config.Age
	if age <= 0 {
		age = DefaultStepUnknownAge
	}
	limit := config.Limit
	if limit <= 0 {
		limit = 65536
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &StepUnknownCheck{client: config.Client, keys: keys, age: age, limit: limit, now: now}, nil
}

func (c *StepUnknownCheck) Gate() string { return StepUnknownGate }

func (c *StepUnknownCheck) Observe(ctx context.Context) (int64, error) {
	size, err := c.client.ZCard(ctx, c.keys.Due()).Result()
	if err != nil {
		return 0, fmt.Errorf("read due index: %w", err)
	}
	if size > c.limit {
		return 0, fmt.Errorf("due index holds %d entries, beyond the %d this audit can read in one pass", size, c.limit)
	}
	refs, err := c.client.ZRange(ctx, c.keys.Due(), 0, -1).Result()
	if err != nil {
		return 0, fmt.Errorf("read due index: %w", err)
	}
	cutoff := c.now().Add(-c.age).UnixMilli()
	var residual int64
	for _, ref := range refs {
		stale, err := c.unresolvedSince(ctx, ref, cutoff)
		if err != nil {
			return 0, err
		}
		if stale {
			residual++
		}
	}
	return residual, nil
}

// unresolvedSince reads one record and reports whether its step is unresolved
// and has been so since before the cutoff.
func (c *StepUnknownCheck) unresolvedSince(ctx context.Context, ref string, cutoffMS int64) (bool, error) {
	fields, err := c.client.HMGet(ctx, c.keys.RecordByRef(ref), "data", "updated_at_ms", "terminal").Result()
	if err != nil {
		return false, fmt.Errorf("read record %s: %w", ref, err)
	}
	if len(fields) != 3 || fields[0] == nil {
		// The record is gone while the index still names it. That is a due
		// residual, which a different gate owns; this one stays silent rather
		// than attributing it here.
		return false, nil
	}
	if terminal, _ := fields[2].(string); terminal == "1" {
		return false, nil
	}
	// The stored update time is authoritative. The copy inside the JSON is not
	// stamped on every persist, so trusting it would age records wrongly.
	updatedAtMS, err := parseMillis(fields[1])
	if err != nil {
		return false, fmt.Errorf("record %s: %w", ref, err)
	}
	if updatedAtMS >= cutoffMS {
		return false, nil
	}
	raw, ok := fields[0].(string)
	if !ok {
		return false, fmt.Errorf("record %s: data field is not a string", ref)
	}
	return stepUnresolved([]byte(raw))
}

func parseMillis(field any) (int64, error) {
	text, ok := field.(string)
	if !ok || text == "" {
		return 0, errors.New("updated_at_ms is missing, so the record cannot be aged")
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("updated_at_ms %q is not a timestamp", text)
	}
	return value, nil
}

// stepUnresolved reports an unresolved step at either level: the work itself
// may be marked STEP_UNKNOWN, or an individual step may carry the unknown
// status while the work moved on.
func stepUnresolved(data []byte) (bool, error) {
	var record struct {
		State workflow.WorkState `json:"state"`
		Steps []struct {
			Status workflow.StepStatus `json:"status"`
		} `json:"steps"`
	}
	if err := json.Unmarshal(data, &record); err != nil {
		return false, fmt.Errorf("decode work record: %w", err)
	}
	if record.State == workflow.WorkStateStepUnknown {
		return true, nil
	}
	for _, step := range record.Steps {
		if step.Status == workflow.StepUnknown {
			return true, nil
		}
	}
	return false, nil
}
