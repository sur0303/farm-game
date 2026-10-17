// Package capaudit evaluates correctness invariants by reading stored state
// directly, without asking the services under test whether they behaved.
//
// A service can only count the violations it noticed. Most guards here refuse
// a bad write rather than record one that slipped through, so the interesting
// question — did anything get through — is only answerable from the data. That
// is what an audit check does.
//
// Checks are too expensive to run on every scrape, so a Runner evaluates them
// on an interval and serves the last answer. The age of that answer is part of
// the answer: once it is older than the freshness limit, the gate reports as
// unobserved rather than as its last known value, because a stale zero makes
// exactly the claim this whole mechanism exists to prevent.
package capaudit

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Check evaluates one invariant and returns how many times it was violated.
type Check interface {
	// Gate is the contract name the value is served under.
	Gate() string
	Observe(context.Context) (int64, error)
}

type observation struct {
	value   int64
	at      time.Time
	problem error
}

type Runner struct {
	groups   []GroupCheck
	before   []Precondition
	interval time.Duration
	freshFor time.Duration
	timeout  time.Duration
	now      func() time.Time

	mu      sync.RWMutex
	results map[string]observation
}

// GroupCheck answers several gates from a single read. Some invariants share
// one expensive sweep, and evaluating them separately would both multiply the
// cost and let the answers come from different moments in time.
type GroupCheck interface {
	Gates() []string
	ObserveAll(context.Context) (map[string]int64, error)
}

// asGroup lets the runner hold one kind of thing.
type singleGroup struct{ check Check }

func (g singleGroup) Gates() []string { return []string{g.check.Gate()} }

func (g singleGroup) ObserveAll(ctx context.Context) (map[string]int64, error) {
	value, err := g.check.Observe(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]int64{g.check.Gate(): value}, nil
}

type Config struct {
	// Groups are checks that answer several gates from one read.
	Groups []GroupCheck
	// Before are the claims that must hold before any gate may report a
	// number. Without them an audit pointed at an empty keyspace reports the
	// same clean zeros as an audit of a clean run.
	Before []Precondition
	// Interval is how often every check is re-evaluated.
	Interval time.Duration
	// FreshFor is how long an answer stays usable. It must exceed Interval,
	// or every answer would expire before the next one replaced it.
	FreshFor time.Duration
	// Timeout bounds a single check, so one slow scan cannot stall the rest.
	Timeout time.Duration
	// Now is injectable for tests.
	Now func() time.Time
}

func NewRunner(config Config, checks ...Check) (*Runner, error) {
	if config.Interval <= 0 {
		return nil, errors.New("capaudit: interval is required")
	}
	if config.FreshFor <= config.Interval {
		return nil, fmt.Errorf("capaudit: results must stay fresh longer than the %s interval", config.Interval)
	}
	if config.Timeout <= 0 {
		return nil, errors.New("capaudit: per-check timeout is required")
	}
	groups := make([]GroupCheck, 0, len(checks)+len(config.Groups))
	for _, check := range checks {
		if check == nil || check.Gate() == "" {
			return nil, errors.New("capaudit: every check must name the gate it serves")
		}
		groups = append(groups, singleGroup{check: check})
	}
	groups = append(groups, config.Groups...)
	if len(groups) == 0 {
		return nil, errors.New("capaudit: a runner without checks would serve nothing")
	}
	seen := map[string]bool{}
	for _, group := range groups {
		if group == nil || len(group.Gates()) == 0 {
			return nil, errors.New("capaudit: every check must name the gates it serves")
		}
		for _, gate := range group.Gates() {
			if gate == "" {
				return nil, errors.New("capaudit: every check must name the gates it serves")
			}
			if seen[gate] {
				return nil, fmt.Errorf("capaudit: two checks claim gate %s", gate)
			}
			seen[gate] = true
		}
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	for _, before := range config.Before {
		if before == nil || before.Describe() == "" {
			return nil, errors.New("capaudit: every precondition must describe what it asserts")
		}
	}
	return &Runner{
		groups: groups, before: config.Before, interval: config.Interval, freshFor: config.FreshFor,
		timeout: config.Timeout, now: now, results: map[string]observation{},
	}, nil
}

// Gates lists what this runner can answer for, so wiring can be checked
// against the contract instead of discovered at scrape time.
func (r *Runner) Gates() []string {
	out := make([]string, 0, len(r.groups))
	for _, group := range r.groups {
		out = append(out, group.Gates()...)
	}
	sort.Strings(out)
	return out
}

// Run evaluates every check immediately and then on the interval, until the
// context ends.
func (r *Runner) Run(ctx context.Context) {
	r.RunOnce(ctx)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.RunOnce(ctx)
		}
	}
}

// RunOnce evaluates every check once. A failing check replaces its previous
// answer with the failure: keeping the old value would let a scan that has
// stopped working report the last clean number indefinitely.
func (r *Runner) RunOnce(ctx context.Context) {
	if err := r.verify(ctx); err != nil {
		at := r.now()
		r.mu.Lock()
		for _, group := range r.groups {
			for _, gate := range group.Gates() {
				r.results[gate] = observation{at: at, problem: err}
			}
		}
		r.mu.Unlock()
		return
	}
	for _, group := range r.groups {
		values, err := r.observe(ctx, group)
		at := r.now()
		r.mu.Lock()
		for _, gate := range group.Gates() {
			r.results[gate] = observation{value: values[gate], at: at, problem: err}
		}
		r.mu.Unlock()
	}
}

// verify runs the preconditions. A failure withholds every gate: the checks
// would all read the same wrong place, so none of their answers would mean
// what the name says.
func (r *Runner) verify(ctx context.Context) error {
	for _, before := range r.before {
		ctx, cancel := context.WithTimeout(ctx, r.timeout)
		err := before.Verify(ctx)
		cancel()
		if err != nil {
			return fmt.Errorf("%s: %w", before.Describe(), err)
		}
	}
	return nil
}

// observe evaluates one group. A group must answer for every gate it claims:
// a missing gate would otherwise be served as a zero nobody computed.
func (r *Runner) observe(ctx context.Context, group GroupCheck) (map[string]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	values, err := group.ObserveAll(ctx)
	if err != nil {
		return nil, err
	}
	for _, gate := range group.Gates() {
		value, answered := values[gate]
		if !answered {
			return nil, fmt.Errorf("check did not answer for %s", gate)
		}
		if value < 0 {
			return nil, fmt.Errorf("%s returned %d, which is not a count", gate, value)
		}
	}
	return values, nil
}

// Value serves the last answer for a gate while it is still fresh.
func (r *Runner) Value(gate string) (float64, error) {
	r.mu.RLock()
	result, found := r.results[gate]
	r.mu.RUnlock()
	if !found {
		return 0, fmt.Errorf("capaudit: %s has not been evaluated yet", gate)
	}
	if result.problem != nil {
		return 0, fmt.Errorf("capaudit: %s could not be evaluated: %w", gate, result.problem)
	}
	if age := r.now().Sub(result.at); age > r.freshFor {
		return 0, fmt.Errorf("capaudit: %s was last evaluated %s ago, beyond the %s limit", gate, age.Round(time.Second), r.freshFor)
	}
	return float64(result.value), nil
}
