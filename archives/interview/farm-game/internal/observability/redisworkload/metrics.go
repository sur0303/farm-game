// Package redisworkload exposes opt-in, aggregate Redis client measurements
// for isolated performance experiments. It never records command arguments,
// keys, credentials, payloads, or replies.
package redisworkload

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

const unknownCommand = "other"

const (
	// ComponentUnspecified is used for Redis work that was not explicitly
	// attributed by the caller. The label set is deliberately bounded so the
	// experiment cannot create unbounded Prometheus cardinality.
	ComponentUnspecified       = "unspecified"
	ComponentActorLoad         = "actor_load"
	ComponentActorCommit       = "actor_commit"
	ComponentSourceVerify      = "source_verify"
	ComponentWorkComplete      = "work_complete"
	ComponentDueScan           = "due_scan"
	ComponentOpportunityWakeup = "opportunity_wakeup"
	ComponentRouteControl      = "route_control"
	ComponentGateAccount       = "gate_account"
	ComponentGateSession       = "gate_session"
	ComponentGateTicket        = "gate_ticket"
	ComponentGateBootLease     = "gate_boot_lease"

	// ComponentOpportunity is retained as a source-compatible alias for newer
	// callers. The label value remains opportunity_wakeup so existing queries
	// and historical comparisons do not split one workload into two series.
	ComponentOpportunity = ComponentOpportunityWakeup
)

type componentContextKey struct{}

var allowedComponents = map[string]struct{}{
	ComponentUnspecified:       {},
	ComponentActorLoad:         {},
	ComponentActorCommit:       {},
	ComponentSourceVerify:      {},
	ComponentWorkComplete:      {},
	ComponentDueScan:           {},
	ComponentOpportunityWakeup: {},
	ComponentRouteControl:      {},
	ComponentGateAccount:       {},
	ComponentGateSession:       {},
	ComponentGateTicket:        {},
	ComponentGateBootLease:     {},
}

// attributionActive is monotonic in production: it changes to true only after
// at least one workload hook has been registered and attached. Keeping this
// package-local bit avoids a context.WithValue allocation at every attribution
// call when the opt-in collector is disabled. Unlike go-redis's OTel recorder,
// it owns no process-global callback and cannot conflict with another library.
var attributionActive atomic.Bool

var allowedCommands = map[string]struct{}{
	"auth": {}, "client": {}, "del": {}, "eval": {}, "evalsha": {}, "exists": {},
	"expire": {}, "get": {}, "hdel": {}, "hexists": {}, "hget": {}, "hgetall": {},
	"hincrby": {}, "hlen": {}, "hmget": {}, "hmset": {}, "hscan": {}, "hset": {},
	"incr": {}, "mget": {}, "multi": {}, "pexpire": {}, "ping": {}, "psetex": {},
	"pttl": {}, "publish": {}, "sadd": {}, "scan": {}, "set": {}, "setnx": {},
	"subscribe": {}, "time": {}, "ttl": {}, "unlink": {}, "unwatch": {}, "watch": {},
	"zadd": {}, "zcard": {}, "zcount": {}, "zrange": {}, "zrangebyscore": {}, "zrem": {},
	"zremrangebyscore": {}, "zscore": {},
}

// Metrics is both a go-redis Hook and a Prometheus collector owner. Metrics are
// deliberately process-local so an A/B runner can scrape before/after deltas
// without relying on noisy shared Redis INFO counters.
type Metrics struct {
	commands         *prometheus.CounterVec
	requestBytes     *prometheus.CounterVec
	responseBytes    *prometheus.CounterVec
	roundTrips       *prometheus.CounterVec
	roundTripTime    *prometheus.HistogramVec
	pipelineWidth    *prometheus.HistogramVec
	batchSize        *prometheus.HistogramVec
	defaultComponent string

	attachMu       sync.Mutex
	attachedClient []hookClient
}

var (
	_ redis.Hook           = (*Metrics)(nil)
	_ prometheus.Collector = (*Metrics)(nil)
)

type hookClient interface {
	AddHook(redis.Hook)
}

// AttachFromEnvironment attaches the hook only when REDIS_WORKLOAD_METRICS is
// true/1. When disabled, no hook is installed and WithComponent returns its
// input context without allocating.
func AttachFromEnvironment(client interface{ AddHook(redis.Hook) }, registerer prometheus.Registerer, raw string) (*Metrics, error) {
	if !enabled(raw) {
		return nil, nil
	}
	return attach([]hookClient{client}, registerer, ComponentUnspecified)
}

// AttachTargetsFromEnvironment attaches one shared collector to every Redis
// storage domain used by a process. Metrics remain aggregated by bounded
// component; storage endpoints and domains are deliberately not labels.
func AttachTargetsFromEnvironment(targets DependencyTargets, registerer prometheus.Registerer, raw string) (*Metrics, error) {
	if !enabled(raw) {
		return nil, nil
	}
	return attach([]hookClient{
		targets.Authority,
		targets.Runtime,
		targets.Projection,
	}, registerer, ComponentUnspecified)
}

// AttachFromEnvironmentWithComponent supplies a bounded fallback component
// for a composition whose entire Redis client has one role. An explicit
// WithComponent value on a request still takes precedence.
func AttachFromEnvironmentWithComponent(client interface{ AddHook(redis.Hook) }, registerer prometheus.Registerer, raw, component string) (*Metrics, error) {
	if !enabled(raw) {
		return nil, nil
	}
	return attach([]hookClient{client}, registerer, component)
}

func attach(clients []hookClient, registerer prometheus.Registerer, defaultComponent string) (*Metrics, error) {
	if !hasHookClient(clients) {
		return nil, errors.New("redis workload metrics: at least one client is required")
	}
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	defaultComponent = normalizeComponent(defaultComponent)
	candidate := newMetrics(defaultComponent)
	m := candidate
	if err := registerer.Register(candidate); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if !errors.As(err, &alreadyRegistered) {
			return nil, fmt.Errorf("redis workload metrics: register collector: %w", err)
		}
		existing, ok := alreadyRegistered.ExistingCollector.(*Metrics)
		if !ok {
			return nil, fmt.Errorf("redis workload metrics: conflicting collector %T: %w", alreadyRegistered.ExistingCollector, err)
		}
		m = existing
	}
	if m.defaultComponent != defaultComponent {
		return nil, fmt.Errorf("redis workload metrics: collector default component is %q, cannot attach as %q", m.defaultComponent, defaultComponent)
	}
	m.attach(clients)
	attributionActive.Store(true)
	return m, nil
}

func newMetrics(defaultComponent string) *Metrics {
	return &Metrics{
		commands: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "redis_workload", Name: "commands_total",
			Help: "Observed Redis commands in the opt-in workload window; arguments are never recorded.",
		}, []string{"component", "command", "outcome"}),
		requestBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "redis_workload", Name: "request_bytes_total",
			Help: "RESP request bytes calculated from in-memory command arguments; values are not exported.",
		}, []string{"component", "command"}),
		responseBytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "redis_workload", Name: "response_bytes_total",
			Help: "ESTIMATED RESP response bytes reconstructed from in-memory command arguments and command-specific reply shapes; never exact wire truth (use a Harness proxy for exact bytes).",
		}, []string{"component", "command", "estimated"}),
		roundTrips: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "farm", Subsystem: "redis_workload", Name: "round_trips_total",
			Help: "go-redis process and pipeline dispatches.",
		}, []string{"component", "kind", "outcome"}),
		roundTripTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm", Subsystem: "redis_workload", Name: "round_trip_duration_seconds",
			Help:    "go-redis process and pipeline dispatch latency.",
			Buckets: []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}, []string{"component", "kind"}),
		pipelineWidth: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm", Subsystem: "redis_workload", Name: "pipeline_width",
			Help:    "Number of commands in one go-redis pipeline dispatch.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512},
		}, []string{"component"}),
		batchSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "farm", Subsystem: "redis_workload", Name: "batch_size",
			Help:    "Number of commands in one dispatch (single command or pipeline); recorded once per dispatch, never per command.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512},
		}, []string{"component"}),
		defaultComponent: defaultComponent,
	}
}

func hasHookClient(clients []hookClient) bool {
	for _, client := range clients {
		if !nilHookClient(client) {
			return true
		}
	}
	return false
}

func nilHookClient(client hookClient) bool {
	if client == nil {
		return true
	}
	value := reflect.ValueOf(client)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func sameHookClient(left, right hookClient) bool {
	if nilHookClient(left) || nilHookClient(right) {
		return false
	}
	leftValue := reflect.ValueOf(left)
	rightValue := reflect.ValueOf(right)
	return leftValue.Type() == rightValue.Type() && leftValue.Comparable() && leftValue.Equal(rightValue)
}

// attach serializes AddHook because go-redis mutates its hook slice outside its
// own chain mutex. It also makes repeated or concurrent composition-root calls
// idempotent for an identical collector/client pair.
func (m *Metrics) attach(clients []hookClient) {
	m.attachMu.Lock()
	defer m.attachMu.Unlock()
	for _, client := range clients {
		if nilHookClient(client) {
			continue
		}
		duplicate := false
		for _, existing := range m.attachedClient {
			if sameHookClient(existing, client) {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		client.AddHook(m)
		m.attachedClient = append(m.attachedClient, client)
	}
}

func (m *Metrics) Describe(descriptions chan<- *prometheus.Desc) {
	m.commands.Describe(descriptions)
	m.requestBytes.Describe(descriptions)
	m.responseBytes.Describe(descriptions)
	m.roundTrips.Describe(descriptions)
	m.roundTripTime.Describe(descriptions)
	m.pipelineWidth.Describe(descriptions)
	m.batchSize.Describe(descriptions)
}

func (m *Metrics) Collect(metrics chan<- prometheus.Metric) {
	m.commands.Collect(metrics)
	m.requestBytes.Collect(metrics)
	m.responseBytes.Collect(metrics)
	m.roundTrips.Collect(metrics)
	m.roundTripTime.Collect(metrics)
	m.pipelineWidth.Collect(metrics)
	m.batchSize.Collect(metrics)
}

// WithComponent attributes all Redis calls made with the returned context to
// one bounded subsystem. It changes metrics only; it never changes Redis keys,
// commands, deadlines, or cancellation.
func WithComponent(ctx context.Context, component string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if !attributionActive.Load() {
		return ctx
	}
	return context.WithValue(ctx, componentContextKey{}, normalizeComponent(component))
}

func enabled(raw string) bool {
	raw = strings.TrimSpace(strings.ToLower(raw))
	return raw == "1" || raw == "true" || raw == "yes" || raw == "on"
}

func (m *Metrics) DialHook(next redis.DialHook) redis.DialHook { return next }

func (m *Metrics) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		started := time.Now()
		err := next(ctx, cmd)
		m.observe(ctx, []redis.Cmder{cmd}, "single", err, time.Since(started))
		return err
	}
}

func (m *Metrics) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		started := time.Now()
		err := next(ctx, cmds)
		m.observe(ctx, cmds, "pipeline", err, time.Since(started))
		return err
	}
}

func (m *Metrics) observe(ctx context.Context, cmds []redis.Cmder, kind string, err error, elapsed time.Duration) {
	if m == nil {
		return
	}
	single := kind == "single"
	component := m.componentFromContext(ctx)
	for _, cmd := range cmds {
		name := normalizeCommand(cmd)
		// Per-command outcome, never the dispatch outcome: one rejected command
		// in a pipeline of N must not be reported as N failed commands.
		cmdErr := commandError(cmd, err, single)
		outcome := outcomeFor(cmdErr)
		m.commands.WithLabelValues(component, name, outcome).Inc()
		requestBytes := respRequestBytes(cmd.Args())
		m.requestBytes.WithLabelValues(component, name).Add(float64(requestBytes))
		// Response bytes are estimated from the command shape and the observed
		// error outcome; go-redis does not expose reply sizes. The estimate is
		// always labeled estimated=true so dashboards cannot mistake it for wire
		// truth. A Harness proxy is required for exact response bytes.
		estimatedResponse := estimateResponseBytes(cmd, cmdErr, outcome)
		m.responseBytes.WithLabelValues(component, name, "true").Add(float64(estimatedResponse))
	}
	// The batch histogram records one observation per dispatch (single command
	// or pipeline), never one observation per command in that dispatch.
	m.batchSize.WithLabelValues(component).Observe(float64(len(cmds)))
	// round_trips_total stays dispatch-scoped: it counts dispatches, so the
	// dispatch-level error is the right outcome for it.
	m.roundTrips.WithLabelValues(component, kind, outcomeFor(err)).Inc()
	m.roundTripTime.WithLabelValues(component, kind).Observe(elapsed.Seconds())
	if kind == "pipeline" {
		m.pipelineWidth.WithLabelValues(component).Observe(float64(len(cmds)))
	}
}

func outcomeFor(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, redis.Nil):
		return "nil"
	default:
		return "error"
	}
}

// commandError resolves the error belonging to one command in a dispatch,
// because go-redis attributes errors differently per dispatch shape.
//
// Single command: Client.Process calls cmd.SetErr only after the hook chain
// returns, so cmd.Err() is still empty in here and the dispatch error is the
// command's own error.
//
// Pipeline: transport-level failures are fanned out onto every command by
// setCmdsErr, but a server-side rejection stays on the one command that
// received it and is merely returned as the dispatch error (generalProcessPipeline
// skips the fan-out for Redis errors). Reading the dispatch error per command
// would therefore turn a single WRONGTYPE into N failed commands, so a pipeline
// command is only failed when go-redis marked it as failed.
func commandError(cmd redis.Cmder, dispatchErr error, singleCommandDispatch bool) error {
	if cmd != nil {
		if err := cmd.Err(); err != nil {
			return err
		}
	}
	if singleCommandDispatch {
		return dispatchErr
	}
	return nil
}

func normalizeComponent(component string) string {
	component = strings.TrimSpace(strings.ToLower(component))
	if _, ok := allowedComponents[component]; ok {
		return component
	}
	return ComponentUnspecified
}

func componentFromContext(ctx context.Context) string {
	return componentFromContextWithDefault(ctx, ComponentUnspecified)
}

func componentFromContextWithDefault(ctx context.Context, fallback string) string {
	if ctx != nil {
		if component, present := ctx.Value(componentContextKey{}).(string); present {
			return normalizeComponent(component)
		}
	}
	return normalizeComponent(fallback)
}

func (m *Metrics) componentFromContext(ctx context.Context) string {
	if m == nil {
		return ComponentUnspecified
	}
	return componentFromContextWithDefault(ctx, m.defaultComponent)
}

func normalizeCommand(cmd redis.Cmder) string {
	if cmd == nil {
		return unknownCommand
	}
	name := strings.ToLower(strings.TrimSpace(cmd.Name()))
	if _, ok := allowedCommands[name]; ok {
		return name
	}
	return unknownCommand
}

func respRequestBytes(args []interface{}) int {
	total := 1 + decimalDigits(len(args)) + 2 // *<argc>\r\n
	for _, arg := range args {
		length := redisArgumentLength(arg)
		total += 1 + decimalDigits(length) + 2 + length + 2 // $<len>\r\n<arg>\r\n
	}
	return total
}

func redisArgumentLength(value interface{}) int {
	switch typed := value.(type) {
	case nil:
		return 0
	case string:
		return len(typed)
	case []byte:
		return len(typed)
	case int:
		return len(strconv.Itoa(typed))
	case int8:
		return len(strconv.FormatInt(int64(typed), 10))
	case int16:
		return len(strconv.FormatInt(int64(typed), 10))
	case int32:
		return len(strconv.FormatInt(int64(typed), 10))
	case int64:
		return len(strconv.FormatInt(typed, 10))
	case uint:
		return len(strconv.FormatUint(uint64(typed), 10))
	case uint8:
		return len(strconv.FormatUint(uint64(typed), 10))
	case uint16:
		return len(strconv.FormatUint(uint64(typed), 10))
	case uint32:
		return len(strconv.FormatUint(uint64(typed), 10))
	case uint64:
		return len(strconv.FormatUint(typed, 10))
	case float32:
		return len(strconv.FormatFloat(float64(typed), 'f', -1, 32))
	case float64:
		return len(strconv.FormatFloat(typed, 'f', -1, 64))
	case bool:
		return 1
	default:
		// The value is used only for its encoded length and is never retained or
		// exported. This mirrors go-redis's textual fallback closely enough for
		// uncommon argument types while keeping metrics free of content.
		return len(fmt.Sprint(value))
	}
}

// estimateResponseBytes reconstructs a plausible RESP reply size from the
// command name, argument shape and outcome. It is an upper-bound-style
// estimate, never exact: exact reply bytes require a real proxy/Harness.
func estimateResponseBytes(cmd redis.Cmder, err error, outcome string) int {
	if cmd == nil {
		return 0
	}
	if outcome == "nil" {
		// RESP null bulk string: $-1\r\n
		return 5
	}
	if outcome == "error" {
		if err == nil {
			return 5
		}
		// -<error message>\r\n; error text length is bounded above by the
		// command argument bytes to avoid unbounded estimates.
		text := len(err.Error())
		if text > 4096 {
			text = 4096
		}
		return 1 + text + 2
	}
	switch strings.ToLower(cmd.Name()) {
	case "auth", "client", "del", "exists", "expire", "hdel", "hexists", "hset",
		"hmset", "incr", "multi", "pexpire", "psetex", "publish", "sadd", "set",
		"setnx", "time", "ttl", "unlink", "unwatch", "watch", "zadd", "zrem",
		"zremrangebyscore", "zscore":
		// Integer or simple status replies: +OK\r\n / :<int>\r\n
		return 1 + len("OK") + 2
	case "hgetall", "hscan", "scan", "zrange", "zrangebyscore":
		// Bulk-string array reply; estimate one small member per argument,
		// capped so the estimate cannot balloon with key sizes.
		args := cmd.Args()
		argc := len(args)
		if argc > 32 {
			argc = 32
		}
		return 4 + argc*24
	case "get", "hget", "hmget", "mget", "pttl":
		// Bulk string / array of bulk strings; estimate bounded by argument
		// bytes to keep the estimate useful without exporting content.
		args := cmd.Args()
		bytes := 0
		for index, arg := range args {
			if index == 0 {
				continue
			}
			length := redisArgumentLength(arg)
			if length > 1024 {
				length = 1024
			}
			bytes += 4 + length
		}
		if bytes == 0 {
			bytes = 8
		}
		return bytes
	default:
		// Unobserved commands use a small bounded fallback; the estimate flag
		// remains set so the value is never read as exact evidence.
		return 16
	}
}

func decimalDigits(value int) int {
	if value == 0 {
		return 1
	}
	digits := 0
	for value > 0 {
		digits++
		value /= 10
	}
	return digits
}
