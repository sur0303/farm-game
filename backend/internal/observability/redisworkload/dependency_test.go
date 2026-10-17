package redisworkload

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// The exported name is spelled out here on purpose: the capacity harness maps
// dependency_error_percent onto this exact series, so renaming the metric must
// break a test rather than a load run.
const dependencyMetricName = "farm_redis_dependency_error_percent"

var (
	errWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
	errConnReset = errors.New("read tcp 127.0.0.1:6379: connection reset by peer")
)

// dispatch describes one go-redis dispatch as the hook sees it: what go-redis
// recorded on each command, and what the dispatch itself returned.
type dispatch struct {
	pipeline    bool
	commandErrs []error
	dispatchErr error
}

func TestDependencyErrorPercentIsExportedBeforeAnythingFails(t *testing.T) {
	_, registry := newTestDependencyHealth(t, nil)
	// No client was ever attached and no command was ever issued. The harness
	// voids an entire capacity run when one per-second sample lacks this
	// metric, so all three domains must already be on /metrics as 0.
	requireDependencyPercent(t, registry, map[string]float64{
		DomainAuthority:  0,
		DomainRuntime:    0,
		DomainProjection: 0,
	})
}

func TestDependencyErrorPercentIgnoresWorkloadSwitch(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	registry := prometheus.NewRegistry()

	workload, err := AttachFromEnvironment(client, registry, "false")
	if err != nil || workload != nil {
		t.Fatalf("AttachFromEnvironment metrics=%v err=%v", workload, err)
	}
	if _, err := AttachDependencyHealth(registry, DependencyTargets{Authority: client}); err != nil {
		t.Fatalf("AttachDependencyHealth: %v", err)
	}
	// The high-cardinality per-command counters stay off, the dependency
	// percentage does not: it is a hard requirement of the capacity harness.
	requireDependencyPercent(t, registry, map[string]float64{
		DomainAuthority:  0,
		DomainRuntime:    0,
		DomainProjection: 0,
	})
}

func TestDependencyHealthExportsRedisPoolQueueStats(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), PoolSize: 2})
	defer client.Close()
	registry := prometheus.NewRegistry()
	if _, err := AttachDependencyHealth(registry, DependencyTargets{Authority: client}); err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	foundConnections, foundWaits := false, false
	for _, family := range families {
		switch family.GetName() {
		case "farm_redis_pool_connections":
			for _, metric := range family.GetMetric() {
				labels := map[string]string{}
				for _, label := range metric.GetLabel() {
					labels[label.GetName()] = label.GetValue()
				}
				if labels["domain"] == DomainAuthority && labels["state"] == "total" && metric.GetGauge().GetValue() >= 1 {
					foundConnections = true
				}
			}
		case "farm_redis_pool_waits_total":
			for _, metric := range family.GetMetric() {
				for _, label := range metric.GetLabel() {
					if label.GetName() == "domain" && label.GetValue() == DomainAuthority {
						foundWaits = metric.Counter != nil
					}
				}
			}
		}
	}
	if !foundConnections || !foundWaits {
		t.Fatalf("pool metrics connections=%t waits=%t", foundConnections, foundWaits)
	}
}

func TestDependencyErrorPercentCountsFailures(t *testing.T) {
	missingKey := fmt.Errorf("get profile: %w", redis.Nil)
	cases := []struct {
		name       string
		dispatches []dispatch
		want       float64
	}{
		{
			name: "no traffic reports zero",
			want: 0,
		},
		{
			name:       "every command succeeds",
			dispatches: []dispatch{{}, {}, {}, {}},
			want:       0,
		},
		{
			name:       "one failure in four commands",
			dispatches: []dispatch{{}, {}, {}, {dispatchErr: errConnReset}},
			want:       25,
		},
		{
			name:       "missing key is an answer not a failure",
			dispatches: []dispatch{{dispatchErr: redis.Nil}, {dispatchErr: redis.Nil}, {}, {}},
			want:       0,
		},
		{
			name:       "wrapped missing key is still an answer",
			dispatches: []dispatch{{dispatchErr: missingKey}, {}},
			want:       0,
		},
		{
			name:       "deadline is a dependency failure",
			dispatches: []dispatch{{dispatchErr: context.DeadlineExceeded}},
			want:       100,
		},
		{
			// go-redis leaves a server-side rejection on the one command that
			// received it and merely returns it as the dispatch error. Reading
			// the dispatch error per command would report 100% here.
			name: "pipeline rejection counts one command not the whole batch",
			dispatches: []dispatch{{
				pipeline:    true,
				commandErrs: []error{nil, nil, nil, errWrongType, nil, nil, nil, nil, nil, nil},
				dispatchErr: errWrongType,
			}},
			want: 10,
		},
		{
			// A transport failure is different: go-redis fans it out onto every
			// command, so every command really did fail.
			name: "pipeline transport failure marks every command",
			dispatches: []dispatch{{
				pipeline: true,
				commandErrs: []error{
					errConnReset, errConnReset, errConnReset, errConnReset, errConnReset,
					errConnReset, errConnReset, errConnReset, errConnReset, errConnReset,
				},
				dispatchErr: errConnReset,
			}},
			want: 100,
		},
		{
			// The dispatch error is redis.Nil because that command came first;
			// only the rejected command may count.
			name: "pipeline mixes answers rejections and successes",
			dispatches: []dispatch{{
				pipeline:    true,
				commandErrs: []error{redis.Nil, redis.Nil, errWrongType, nil, nil, nil, nil, nil, nil, nil},
				dispatchErr: redis.Nil,
			}},
			want: 10,
		},
		{
			name: "single and pipeline dispatches share one ratio",
			dispatches: []dispatch{
				{},
				{pipeline: true, commandErrs: []error{nil, nil, nil, errWrongType}, dispatchErr: errWrongType},
			},
			want: 20,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			clock := newFakeClock()
			health, registry := newTestDependencyHealth(t, clock)
			hook := health.hook(DomainAuthority)
			for _, plan := range testCase.dispatches {
				runDispatch(hook, plan)
			}
			// Only the attributed domain moves; the other two stay exported at 0.
			requireDependencyPercent(t, registry, map[string]float64{
				DomainAuthority:  testCase.want,
				DomainRuntime:    0,
				DomainProjection: 0,
			})
		})
	}
}

func TestDependencyErrorPercentIsScopedPerDomain(t *testing.T) {
	clock := newFakeClock()
	health, registry := newTestDependencyHealth(t, clock)
	runDispatch(health.hook(DomainAuthority), dispatch{})
	runDispatch(health.hook(DomainRuntime), dispatch{dispatchErr: errConnReset})
	runDispatch(health.hook(DomainProjection), dispatch{dispatchErr: redis.Nil})
	requireDependencyPercent(t, registry, map[string]float64{
		DomainAuthority:  0,
		DomainRuntime:    100,
		DomainProjection: 0,
	})
}

func TestDependencyFailureWindowSlides(t *testing.T) {
	type step struct {
		atSecond int64
		observed uint64
		failed   uint64
	}
	cases := []struct {
		name      string
		steps     []step
		readAt    int64
		want      float64
		rationale string
	}{
		{
			name:      "empty window is zero not undefined",
			readAt:    0,
			want:      0,
			rationale: "0/0 must read as zero so the series is never NaN or absent",
		},
		{
			name:      "failures inside the window count",
			steps:     []step{{atSecond: 0, observed: 100, failed: 5}},
			readAt:    9,
			want:      5,
			rationale: "the bucket is still the oldest live slot",
		},
		{
			name:      "failures decay out of the window",
			steps:     []step{{atSecond: 0, observed: 100, failed: 100}},
			readAt:    10,
			want:      0,
			rationale: "a cumulative ratio would still report 100 here and latch the stop condition forever",
		},
		{
			name: "recent failures are not diluted by old successes",
			steps: []step{
				{atSecond: 0, observed: 100_000, failed: 0},
				{atSecond: 12, observed: 100, failed: 5},
			},
			readAt:    12,
			want:      5,
			rationale: "a cumulative ratio would report 0.005 and never trip on a real outage",
		},
		{
			name: "partial window mixes only the live buckets",
			steps: []step{
				{atSecond: 3, observed: 50, failed: 0},
				{atSecond: 7, observed: 50, failed: 2},
			},
			readAt:    7,
			want:      2,
			rationale: "both buckets are live so the ratio spans them",
		},
		{
			name: "a clock step backwards does not poison the ratio",
			steps: []step{
				{atSecond: 5_000, observed: 10, failed: 10},
				{atSecond: 100, observed: 10, failed: 0},
			},
			readAt:    100,
			want:      0,
			rationale: "the future slot is skipped instead of counted",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			window := &failureWindow{}
			for _, item := range testCase.steps {
				window.record(time.Unix(item.atSecond, 0), item.observed, item.failed)
			}
			if got := window.percent(time.Unix(testCase.readAt, 0)); !nearly(got, testCase.want) {
				t.Fatalf("percent=%v want=%v (%s)", got, testCase.want, testCase.rationale)
			}
		})
	}
}

func TestDependencyErrorPercentDecaysAfterFailuresStop(t *testing.T) {
	clock := newFakeClock()
	health, registry := newTestDependencyHealth(t, clock)
	hook := health.hook(DomainAuthority)
	runDispatch(hook, dispatch{dispatchErr: errConnReset})
	requireDependencyPercent(t, registry, map[string]float64{
		DomainAuthority: 100, DomainRuntime: 0, DomainProjection: 0,
	})
	// The stop condition is "at least 1% for 30 consecutive samples". A burst
	// that stopped must fall back to 0 on its own, otherwise every run that
	// ever saw one Redis error would be voided as INCONCLUSIVE_DEPENDENCY.
	clock.advance(dependencyWindowSeconds * time.Second)
	requireDependencyPercent(t, registry, map[string]float64{
		DomainAuthority: 0, DomainRuntime: 0, DomainProjection: 0,
	})
}

func TestDependencyErrorPercentThroughGoRedis(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), PoolSize: 1})
	defer client.Close()
	ctx := context.Background()
	// Warm the pool and seed the fixture before attaching so connection setup
	// commands cannot be mistaken for workload.
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.LPush(ctx, "list-key", "member").Err(); err != nil {
		t.Fatal(err)
	}

	registry := prometheus.NewRegistry()
	if _, err := AttachDependencyHealth(registry, DependencyTargets{Authority: client}); err != nil {
		t.Fatalf("AttachDependencyHealth: %v", err)
	}

	if err := client.Get(ctx, "absent-key").Err(); !errors.Is(err, redis.Nil) {
		t.Fatalf("Get absent key err=%v want redis.Nil", err)
	}
	if err := client.Set(ctx, "string-key", "value", 0).Err(); err != nil {
		t.Fatal(err)
	}
	requireDependencyPercent(t, registry, map[string]float64{
		DomainAuthority: 0, DomainRuntime: 0, DomainProjection: 0,
	})

	// One real rejection inside a three command pipeline, driven through
	// go-redis itself rather than a hand-built dispatch.
	_, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, "string-key", "value", 0)
		pipe.Get(ctx, "list-key")
		pipe.Set(ctx, "other-key", "value", 0)
		return nil
	})
	if err == nil {
		t.Fatal("pipeline over a wrong-typed key returned no error")
	}
	// Five commands observed since attach, exactly one of them failed.
	requireDependencyPercent(t, registry, map[string]float64{
		DomainAuthority: 20, DomainRuntime: 0, DomainProjection: 0,
	})
}

func TestDependencyErrorPercentIsConcurrencySafe(t *testing.T) {
	clock := newFakeClock()
	health, registry := newTestDependencyHealth(t, clock)
	const (
		writers            = 8
		dispatchesPerWrite = 125
	)
	var writing sync.WaitGroup
	for writer := 0; writer < writers; writer++ {
		writer := writer
		writing.Add(1)
		go func() {
			defer writing.Done()
			hook := health.hook(dependencyDomains[writer%len(dependencyDomains)])
			for index := 0; index < dispatchesPerWrite; index++ {
				if index%2 == 0 {
					runDispatch(hook, dispatch{dispatchErr: errConnReset})
					continue
				}
				runDispatch(hook, dispatch{
					pipeline:    true,
					commandErrs: []error{nil, errWrongType, nil, nil},
					dispatchErr: errWrongType,
				})
			}
		}()
	}
	// Scrape while the writers run so -race also covers Collect.
	stopScraping := make(chan struct{})
	scraping := make(chan struct{})
	go func() {
		defer close(scraping)
		for {
			select {
			case <-stopScraping:
				return
			default:
			}
			if _, err := registry.Gather(); err != nil {
				t.Errorf("gather: %v", err)
				return
			}
		}
	}()
	writing.Wait()
	close(stopScraping)
	<-scraping

	// Every writer runs 63 failing single commands and 62 pipelines of width 4
	// carrying one failure each, so each domain lands on the same ratio no
	// matter how many writers were assigned to it.
	const singles = (dispatchesPerWrite + 1) / 2
	const pipelines = dispatchesPerWrite / 2
	want := 100 * float64(singles+pipelines) / float64(singles+pipelines*4)
	requireDependencyPercent(t, registry, map[string]float64{
		DomainAuthority: want, DomainRuntime: want, DomainProjection: want,
	})
}

func runDispatch(hook redis.Hook, plan dispatch) {
	ctx := context.Background()
	if !plan.pipeline {
		// go-redis calls cmd.SetErr only after the hook chain returns, so a
		// single command dispatch shows the hook nothing but the returned error.
		cmd := redis.NewStatusCmd(ctx, "set", "key", "value")
		_ = hook.ProcessHook(func(context.Context, redis.Cmder) error {
			return plan.dispatchErr
		})(ctx, cmd)
		return
	}
	cmds := make([]redis.Cmder, len(plan.commandErrs))
	for index := range cmds {
		cmds[index] = redis.NewStatusCmd(ctx, "set", "key", "value")
	}
	_ = hook.ProcessPipelineHook(func(_ context.Context, dispatched []redis.Cmder) error {
		for index, err := range plan.commandErrs {
			if err != nil {
				dispatched[index].SetErr(err)
			}
		}
		return plan.dispatchErr
	})(ctx, cmds)
}

func newTestDependencyHealth(t *testing.T, clock *fakeClock) (*DependencyHealth, *prometheus.Registry) {
	t.Helper()
	registry := prometheus.NewRegistry()
	health, err := newDependencyHealth(registry)
	if err != nil {
		t.Fatalf("newDependencyHealth: %v", err)
	}
	if clock != nil {
		health.now = clock.now
	}
	return health, registry
}

func requireDependencyPercent(t *testing.T, gatherer prometheus.Gatherer, want map[string]float64) {
	t.Helper()
	values := gatherDependencyPercent(t, gatherer)
	if len(values) != len(want) {
		t.Fatalf("%s exported %d series %v, want %d", dependencyMetricName, len(values), values, len(want))
	}
	for domain, expected := range want {
		got, found := values[domain]
		if !found {
			t.Fatalf("%s{domain=%q} absent; exported=%v", dependencyMetricName, domain, values)
		}
		if !nearly(got, expected) {
			t.Fatalf("%s{domain=%q}=%v want=%v", dependencyMetricName, domain, got, expected)
		}
	}
}

func gatherDependencyPercent(t *testing.T, gatherer prometheus.Gatherer) map[string]float64 {
	t.Helper()
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != dependencyMetricName {
			continue
		}
		values := make(map[string]float64, len(family.Metric))
		for _, metric := range family.GetMetric() {
			domain := ""
			for _, label := range metric.GetLabel() {
				if label.GetName() == "domain" {
					domain = label.GetValue()
				}
			}
			if metric.Gauge == nil {
				t.Fatalf("%s{domain=%q} is not a gauge", dependencyMetricName, domain)
			}
			values[domain] = metric.Gauge.GetValue()
		}
		return values
	}
	t.Fatalf("%s is absent from the registry; the capacity harness would void the run", dependencyMetricName)
	return nil
}

func nearly(got, want float64) bool {
	return math.Abs(got-want) < 1e-9
}

type fakeClock struct {
	mu sync.Mutex
	at time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{at: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *fakeClock) advance(step time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(step)
}
