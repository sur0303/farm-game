package redisworkload

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// Redis storage domain label values. The set is closed so the dependency
// health family stays at exactly three series for the life of the process.
const (
	DomainAuthority  = "authority"
	DomainRuntime    = "runtime"
	DomainProjection = "projection"
)

// dependencyDomains is emitted in full on every scrape, including domains this
// process never opened a client for. cmd/cap-single-pod treats a missing
// dependency_error_percent sample as INCONCLUSIVE_OBSERVABILITY and voids the
// whole run, so the family must never be empty and no label combination may
// appear only after the first failure.
var dependencyDomains = []string{DomainAuthority, DomainRuntime, DomainProjection}

// dependencyWindowSeconds is the width of the sliding failure window.
//
// This is a window ratio, not a cumulative ratio. cmd/cap-single-pod stops a
// run when dependency_error_percent stays >= 1 for 30 consecutive one-second
// samples. A cumulative 100*failed/total since process start cannot express
// "still failing": once a burst pushes it over 1% it stays over 1% for the
// rest of the run even after Redis fully recovers, so one early blip would
// pin a bogus INCONCLUSIVE_DEPENDENCY verdict; and late in a long run the same
// average is diluted by millions of past successes and barely moves when Redis
// really does start failing.
//
// A short window has the opposite weakness: with little traffic one failure
// reads as a large percentage. The 30-sample sustain requirement absorbs that.
// A burst confined to a single second holds the window above the threshold for
// only 10 samples and then decays to 0 as buckets age out, so tripping the stop
// condition requires failures spread across at least 21 of the 30 seconds,
// which is exactly the state the verdict is meant to describe. Under capacity
// load each domain sees thousands of commands per window, so sampling noise is
// negligible where the number is actually consumed.
const dependencyWindowSeconds = 10

// DependencyTargets names the Redis clients one process owns. A nil field means
// the process does not use that storage domain; that domain is still exported
// as 0 rather than omitted.
type DependencyTargets struct {
	Authority  redis.UniversalClient
	Runtime    redis.UniversalClient
	Projection redis.UniversalClient
}

// DependencyHealth exports farm_redis_dependency_error_percent, the share of
// Redis commands that failed per storage domain.
//
// It is registered unconditionally and is deliberately not gated by
// REDIS_WORKLOAD_METRICS. That switch guards the per-command, per-outcome
// counters whose cardinality only a controlled experiment should pay for.
// This collector is three gauge series and is a hard requirement of the
// capacity harness, so it has to be on in every deployment.
type DependencyHealth struct {
	description *prometheus.Desc
	windows     map[string]*failureWindow
	now         func() time.Time
}

var _ prometheus.Collector = (*DependencyHealth)(nil)

// AttachDependencyHealth registers the dependency failure percentage on
// registerer and attaches one hook per Redis client the process owns.
func AttachDependencyHealth(registerer prometheus.Registerer, targets DependencyTargets) (*DependencyHealth, error) {
	health, err := newDependencyHealth(registerer)
	if err != nil {
		return nil, err
	}
	if _, err := newPoolMetrics(registerer, targets); err != nil {
		return nil, err
	}
	for _, target := range []struct {
		domain string
		client redis.UniversalClient
	}{
		{DomainAuthority, targets.Authority},
		{DomainRuntime, targets.Runtime},
		{DomainProjection, targets.Projection},
	} {
		if target.client == nil {
			continue
		}
		target.client.AddHook(health.hook(target.domain))
	}
	return health, nil
}

func newDependencyHealth(registerer prometheus.Registerer) (*DependencyHealth, error) {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	health := &DependencyHealth{
		description: prometheus.NewDesc(
			prometheus.BuildFQName("farm", "redis_dependency", "error_percent"),
			"Percentage of Redis commands that failed in the trailing 10 second window, per storage domain "+
				"(100 * failed / observed). Missing-key replies are answers, not failures. Every domain is "+
				"exported on every scrape, as 0 when nothing failed, so the series is never absent.",
			[]string{"domain"},
			nil,
		),
		windows: make(map[string]*failureWindow, len(dependencyDomains)),
		now:     time.Now,
	}
	for _, domain := range dependencyDomains {
		health.windows[domain] = &failureWindow{}
	}
	if err := registerer.Register(health); err != nil {
		return nil, err
	}
	return health, nil
}

func (h *DependencyHealth) Describe(descriptions chan<- *prometheus.Desc) {
	descriptions <- h.description
}

// Collect evaluates the window against scrape time, not against the time of
// the last command. A domain that stops receiving traffic therefore decays to
// 0 instead of freezing at whatever it read when the traffic stopped.
func (h *DependencyHealth) Collect(metrics chan<- prometheus.Metric) {
	now := h.now()
	for _, domain := range dependencyDomains {
		metrics <- prometheus.MustNewConstMetric(
			h.description,
			prometheus.GaugeValue,
			h.windows[domain].percent(now),
			domain,
		)
	}
}

func (h *DependencyHealth) hook(domain string) redis.Hook {
	return dependencyHook{window: h.windows[domain], now: h.now}
}

type dependencyHook struct {
	window *failureWindow
	now    func() time.Time
}

var _ redis.Hook = dependencyHook{}

// DialHook stays a pass-through: a pool that cannot dial fails the command it
// was dialing for, and that command is already counted below. Counting the
// dial as well would report one outage twice.
func (h dependencyHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h dependencyHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		var failed uint64
		if isDependencyFailure(commandError(cmd, err, true)) {
			failed = 1
		}
		h.window.record(h.now(), 1, failed)
		return err
	}
}

func (h dependencyHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		var failed uint64
		for _, cmd := range cmds {
			if isDependencyFailure(commandError(cmd, err, false)) {
				failed++
			}
		}
		h.window.record(h.now(), uint64(len(cmds)), failed)
		return err
	}
}

// isDependencyFailure decides what counts against Redis. redis.Nil is a normal
// business answer ("key absent") and never a failure. Everything else go-redis
// reports for a command is: server errors, pool and connection errors, and
// deadline or cancellation errors all mean the command did not get an answer
// out of the dependency.
func isDependencyFailure(err error) bool {
	return err != nil && !errors.Is(err, redis.Nil)
}

// failureWindow is a ring of one-second (observed, failed) command counts.
// Each slot remembers which second it holds so a stale slot is dropped on read
// instead of needing a background sweeper.
type failureWindow struct {
	mu       sync.Mutex
	second   [dependencyWindowSeconds]int64
	observed [dependencyWindowSeconds]uint64
	failed   [dependencyWindowSeconds]uint64
}

func (w *failureWindow) record(now time.Time, observed, failed uint64) {
	if observed == 0 {
		return
	}
	second := now.Unix()
	index := windowIndex(second)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.second[index] != second {
		w.second[index] = second
		w.observed[index] = 0
		w.failed[index] = 0
	}
	w.observed[index] += observed
	w.failed[index] += failed
}

func (w *failureWindow) percent(now time.Time) float64 {
	second := now.Unix()
	oldest := second - dependencyWindowSeconds + 1
	var observed, failed uint64
	w.mu.Lock()
	for index, slot := range w.second {
		// A slot from the future can only come from a clock step backwards; it
		// is skipped here and overwritten the next time that slot is written.
		if slot < oldest || slot > second {
			continue
		}
		observed += w.observed[index]
		failed += w.failed[index]
	}
	w.mu.Unlock()
	if observed == 0 {
		return 0
	}
	return 100 * float64(failed) / float64(observed)
}

func windowIndex(second int64) int {
	return int(((second % dependencyWindowSeconds) + dependencyWindowSeconds) % dependencyWindowSeconds)
}
