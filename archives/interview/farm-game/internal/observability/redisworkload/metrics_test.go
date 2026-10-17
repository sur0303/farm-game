package redisworkload

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
)

func TestAttachMeasuresCommandsWithoutLabelsFromArguments(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	registry := prometheus.NewRegistry()
	metrics, err := AttachFromEnvironment(client, registry, "true")
	if err != nil || metrics == nil {
		t.Fatalf("AttachFromEnvironment metrics=%v err=%v", metrics, err)
	}
	ctx := WithComponent(context.Background(), ComponentOpportunityWakeup)
	if err := client.Set(ctx, "secret-key", []byte("secret-payload"), 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Get(ctx, "secret-key")
		pipe.Exists(ctx, "secret-key")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(metrics.commands.WithLabelValues(ComponentOpportunityWakeup, "set", "ok")); got != 1 {
		t.Fatalf("set commands=%v", got)
	}
	if got := testutil.ToFloat64(metrics.commands.WithLabelValues(ComponentOpportunityWakeup, "get", "ok")); got != 1 {
		t.Fatalf("get commands=%v", got)
	}
	if got := testutil.ToFloat64(metrics.commands.WithLabelValues(ComponentOpportunityWakeup, "exists", "ok")); got != 1 {
		t.Fatalf("exists commands=%v", got)
	}
	if got := testutil.ToFloat64(metrics.roundTrips.WithLabelValues(ComponentOpportunityWakeup, "single", "ok")); got < 1 {
		t.Fatalf("single round trips=%v", got)
	}
	if got := testutil.ToFloat64(metrics.roundTrips.WithLabelValues(ComponentOpportunityWakeup, "pipeline", "ok")); got != 1 {
		t.Fatalf("pipeline round trips=%v", got)
	}
	if got := testutil.ToFloat64(metrics.requestBytes.WithLabelValues(ComponentOpportunityWakeup, "set")); got != float64(respRequestBytes([]interface{}{"set", "secret-key", []byte("secret-payload")})) {
		t.Fatalf("set request bytes=%v", got)
	}
}

func TestUnknownComponentFallsBackToBoundedLabel(t *testing.T) {
	if got := componentFromContext(WithComponent(context.Background(), "user-controlled-value")); got != ComponentUnspecified {
		t.Fatalf("component=%q want=%q", got, ComponentUnspecified)
	}
}

func TestAttachDisabledHasNoCollectorOrAttributionAllocation(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	wasActive := attributionActive.Swap(false)
	defer attributionActive.Store(wasActive)
	metrics, err := AttachFromEnvironment(client, prometheus.NewRegistry(), "false")
	if err != nil || metrics != nil {
		t.Fatalf("AttachFromEnvironment metrics=%v err=%v", metrics, err)
	}
	ctx := context.Background()
	if got := WithComponent(ctx, ComponentActorLoad); got != ctx {
		t.Fatal("disabled attribution changed the context")
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if got := WithComponent(ctx, ComponentActorLoad); got != ctx {
			panic("disabled attribution changed the context")
		}
	}); allocations != 0 {
		t.Fatalf("disabled WithComponent allocations=%v want=0", allocations)
	}
}

func TestRESPRequestBytes(t *testing.T) {
	args := []interface{}{"PUBLISH", "channel", []byte{1, 2, 3}}
	const expected = len("*3\r\n$7\r\nPUBLISH\r\n$7\r\nchannel\r\n$3\r\n") + 3 + len("\r\n")
	if got := respRequestBytes(args); got != expected {
		t.Fatalf("respRequestBytes=%d want=%d", got, expected)
	}
}

func TestEstimatedResponseBytesLabeledAndBounded(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	registry := prometheus.NewRegistry()
	metrics, err := AttachFromEnvironment(client, registry, "true")
	if err != nil || metrics == nil {
		t.Fatalf("AttachFromEnvironment metrics=%v err=%v", metrics, err)
	}
	ctx := WithComponent(context.Background(), ComponentOpportunityWakeup)
	if err := client.Set(ctx, "secret-key", "secret-payload", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(metrics.responseBytes.WithLabelValues(ComponentOpportunityWakeup, "set", "true")); got <= 0 {
		t.Fatalf("estimated response bytes=%v want > 0", got)
	}
	// The estimated label is deliberately constant so evidence consumers cannot
	// confuse estimates with exact wire bytes.
	if got := batchSizeSampleCount(metrics.batchSize.WithLabelValues(ComponentOpportunityWakeup)); got < 1 {
		t.Fatalf("batch size observations=%v", got)
	}
}

func TestEstimateResponseBytesBoundaries(t *testing.T) {
	if got := estimateResponseBytes(nil, nil, "ok"); got != 0 {
		t.Fatalf("nil command estimate=%d", got)
	}
	set := redis.NewStringCmd(context.Background(), "set", "key", "value")
	if got := estimateResponseBytes(set, nil, "ok"); got <= 0 {
		t.Fatalf("set estimate=%d", got)
	}
	// A nil reply maps to the RESP null bulk string (5 bytes).
	if got := estimateResponseBytes(set, redis.Nil, "nil"); got != 5 {
		t.Fatalf("nil reply estimate=%d", got)
	}
	// Errors stay bounded even for a pathological error string.
	if got := estimateResponseBytes(set, errors.New(strings.Repeat("x", 10000)), "error"); got > 5000 {
		t.Fatalf("error estimate unbounded: %d", got)
	}
}

func batchSizeSampleCount(observer prometheus.Observer) float64 {
	collected := make(chan prometheus.Metric, 4)
	observer.(prometheus.Collector).Collect(collected)
	close(collected)
	var total float64
	for metric := range collected {
		var dtoMetric dto.Metric
		if err := metric.Write(&dtoMetric); err == nil {
			if histogram := dtoMetric.GetHistogram(); histogram != nil {
				total += float64(histogram.GetSampleCount())
			}
		}
	}
	return total
}

func TestEstimateResponseBytesNeverPanicsOnWeirdArgs(t *testing.T) {
	cmd := redis.NewStringCmd(context.Background(), "hgetall", "key", struct{}{})
	if got := estimateResponseBytes(cmd, nil, "ok"); got <= 0 {
		t.Fatalf("weird args estimate=%d", got)
	}
}

func TestBatchHistogramOneSamplePerDispatch(t *testing.T) {
	wasActive := attributionActive.Swap(true)
	defer attributionActive.Store(wasActive)
	registry := prometheus.NewRegistry()
	metrics := &Metrics{
		commands:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_commands"}, []string{"component", "command", "outcome"}),
		requestBytes:  prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_req"}, []string{"component", "command"}),
		responseBytes: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_resp"}, []string{"component", "command", "estimated"}),
		roundTrips:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "t_rt"}, []string{"component", "kind", "outcome"}),
		roundTripTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "t_rtt"}, []string{"component", "kind"}),
		pipelineWidth: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "t_width", Buckets: []float64{1, 2, 4, 8}}, []string{"component"}),
		batchSize:     prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "t_batch", Buckets: []float64{1, 2, 4, 8}}, []string{"component"}),
	}
	registry.MustRegister(metrics.commands, metrics.requestBytes, metrics.responseBytes, metrics.roundTrips, metrics.roundTripTime, metrics.pipelineWidth, metrics.batchSize)

	ctx := WithComponent(context.Background(), ComponentOpportunityWakeup)
	// Assert the owned contract directly: one ProcessPipelineHook dispatch of
	// width N records exactly one batch sample with value N. Do not depend on
	// go-redis Pipelined invocation fan-out.
	pipeline := metrics.ProcessPipelineHook(func(context.Context, []redis.Cmder) error { return nil })
	cmds := make([]redis.Cmder, 5)
	for i := range cmds {
		cmds[i] = redis.NewStatusCmd(ctx, "set", "k", "v")
	}
	if err := pipeline(ctx, cmds); err != nil {
		t.Fatal(err)
	}
	if got := batchSizeSampleCount(metrics.batchSize.WithLabelValues(ComponentOpportunityWakeup)); got != 1 {
		t.Fatalf("batch samples for width-5 pipeline=%v want 1", got)
	}
	if sum, ok := histogramSum(metrics.batchSize.WithLabelValues(ComponentOpportunityWakeup)); !ok || sum != 5 {
		t.Fatalf("batch size sum=%v want 5", sum)
	}
	single := metrics.ProcessHook(func(context.Context, redis.Cmder) error { return nil })
	if err := single(ctx, redis.NewStringCmd(ctx, "get", "k")); err != nil {
		t.Fatal(err)
	}
	if got := batchSizeSampleCount(metrics.batchSize.WithLabelValues(ComponentOpportunityWakeup)); got != 2 {
		t.Fatalf("after one single + one pipeline batch samples=%v want 2", got)
	}
	if sum, ok := histogramSum(metrics.pipelineWidth.WithLabelValues(ComponentOpportunityWakeup)); !ok || sum != 5 {
		t.Fatalf("pipeline width sum=%v want 5", sum)
	}
}

func histogramSum(observer prometheus.Observer) (float64, bool) {
	collected := make(chan prometheus.Metric, 4)
	observer.(prometheus.Collector).Collect(collected)
	close(collected)
	for metric := range collected {
		var dtoMetric dto.Metric
		if err := metric.Write(&dtoMetric); err != nil {
			continue
		}
		if histogram := dtoMetric.GetHistogram(); histogram != nil {
			return histogram.GetSampleSum(), true
		}
	}
	return 0, false
}

func TestComponentAllowlistIsClosedAndBounded(t *testing.T) {
	if ComponentOpportunityWakeup != "opportunity_wakeup" || ComponentOpportunity != ComponentOpportunityWakeup {
		t.Fatalf("opportunity component changed: wakeup=%q alias=%q", ComponentOpportunityWakeup, ComponentOpportunity)
	}
	expected := map[string]struct{}{
		ComponentUnspecified: {}, ComponentActorLoad: {}, ComponentActorCommit: {},
		ComponentSourceVerify: {}, ComponentWorkComplete: {}, ComponentDueScan: {},
		ComponentOpportunityWakeup: {}, ComponentRouteControl: {},
		ComponentGateAccount: {}, ComponentGateSession: {}, ComponentGateTicket: {},
		ComponentGateBootLease: {},
	}
	if len(allowedComponents) != len(expected) {
		t.Fatalf("allowed components=%v want exactly %v", allowedComponents, expected)
	}
	for component := range expected {
		if got := normalizeComponent(component); got != component {
			t.Fatalf("normalizeComponent(%q)=%q", component, got)
		}
	}
	for _, untrusted := range []string{"", "gateway", "user-123", "redis://user:password@example"} {
		if got := normalizeComponent(untrusted); got != ComponentUnspecified {
			t.Fatalf("normalizeComponent(%q)=%q want=%q", untrusted, got, ComponentUnspecified)
		}
	}
}

func TestAttachTargetsMeasuresEveryStorageClientWithoutDomainOrSecretLabels(t *testing.T) {
	server := miniredis.RunT(t)
	clients := []*redis.Client{
		redis.NewClient(&redis.Options{Addr: server.Addr()}),
		redis.NewClient(&redis.Options{Addr: server.Addr()}),
		redis.NewClient(&redis.Options{Addr: server.Addr()}),
	}
	for _, client := range clients {
		defer client.Close()
	}
	registry := prometheus.NewRegistry()
	metrics, err := AttachTargetsFromEnvironment(DependencyTargets{
		Authority: clients[0], Runtime: clients[1], Projection: clients[2],
	}, registry, "on")
	if err != nil || metrics == nil {
		t.Fatalf("AttachTargetsFromEnvironment metrics=%v err=%v", metrics, err)
	}
	components := []string{ComponentActorLoad, ComponentActorCommit, ComponentSourceVerify}
	for index, client := range clients {
		ctx := WithComponent(context.Background(), components[index])
		if err := client.Set(ctx, "credential-key", "credential-value", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if got := testutil.ToFloat64(metrics.commands.WithLabelValues(components[index], "set", "ok")); got != 1 {
			t.Fatalf("component %q set commands=%v want=1", components[index], got)
		}
	}

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "domain" || label.GetName() == "endpoint" || label.GetName() == "address" || label.GetName() == "key" {
					t.Fatalf("workload metric %q exported forbidden label %q", family.GetName(), label.GetName())
				}
				if strings.Contains(label.GetValue(), "credential") || strings.Contains(label.GetValue(), "password") {
					t.Fatalf("workload metric %q leaked sensitive label value", family.GetName())
				}
			}
		}
	}
}

func TestDefaultComponentAttributionStaysBounded(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	registry := prometheus.NewRegistry()
	metrics, err := AttachFromEnvironmentWithComponent(client, registry, "true", ComponentRouteControl)
	if err != nil || metrics == nil {
		t.Fatalf("AttachFromEnvironmentWithComponent metrics=%v err=%v", metrics, err)
	}
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(WithComponent(context.Background(), ComponentActorLoad)).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(WithComponent(context.Background(), "request-or-secret-derived")).Err(); err != nil {
		t.Fatal(err)
	}
	for _, component := range []string{ComponentRouteControl, ComponentActorLoad, ComponentUnspecified} {
		if got := testutil.ToFloat64(metrics.commands.WithLabelValues(component, "ping", "ok")); got != 1 {
			t.Fatalf("component %q ping commands=%v want=1", component, got)
		}
	}
}

func TestAttachTargetsRequiresAtLeastOneEnabledClient(t *testing.T) {
	if metrics, err := AttachTargetsFromEnvironment(DependencyTargets{}, prometheus.NewRegistry(), "true"); err == nil || metrics != nil {
		t.Fatalf("enabled empty targets metrics=%v err=%v", metrics, err)
	}
	if metrics, err := AttachTargetsFromEnvironment(DependencyTargets{}, prometheus.NewRegistry(), "false"); err != nil || metrics != nil {
		t.Fatalf("disabled empty targets metrics=%v err=%v", metrics, err)
	}
}

type hookCountingClient struct {
	mu    sync.Mutex
	hooks []redis.Hook
}

func (c *hookCountingClient) AddHook(hook redis.Hook) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hooks = append(c.hooks, hook)
}

func (c *hookCountingClient) hookCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.hooks)
}

func TestAttachTwiceReusesCollectorAndHook(t *testing.T) {
	client := &hookCountingClient{}
	registry := prometheus.NewRegistry()
	first, err := AttachFromEnvironment(client, registry, "true")
	if err != nil {
		t.Fatal(err)
	}
	second, err := AttachFromEnvironment(client, registry, "true")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("double attach returned different collectors: %p and %p", first, second)
	}
	if got := client.hookCount(); got != 1 {
		t.Fatalf("double attach added %d hooks want=1", got)
	}
}

func TestAttachConcurrentReusesCollectorAndHook(t *testing.T) {
	client := &hookCountingClient{}
	registry := prometheus.NewRegistry()
	const callers = 32
	start := make(chan struct{})
	results := make(chan *Metrics, callers)
	errors := make(chan error, callers)
	var callersDone sync.WaitGroup
	callersDone.Add(callers)
	for range callers {
		go func() {
			defer callersDone.Done()
			<-start
			metrics, err := AttachFromEnvironment(client, registry, "true")
			if err != nil {
				errors <- err
				return
			}
			results <- metrics
		}()
	}
	close(start)
	callersDone.Wait()
	close(results)
	close(errors)
	for err := range errors {
		t.Errorf("concurrent attach: %v", err)
	}
	var first *Metrics
	for metrics := range results {
		if first == nil {
			first = metrics
			continue
		}
		if metrics != first {
			t.Errorf("concurrent attach returned collectors %p and %p", first, metrics)
		}
	}
	if first == nil {
		t.Fatal("concurrent attach returned no collector")
	}
	if got := client.hookCount(); got != 1 {
		t.Fatalf("concurrent attach added %d hooks want=1", got)
	}
}

func TestAttachTargetsDeduplicatesIdenticalClients(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()
	metrics, err := AttachTargetsFromEnvironment(DependencyTargets{
		Authority: client, Runtime: client, Projection: client,
	}, prometheus.NewRegistry(), "true")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(metrics.commands.WithLabelValues(ComponentUnspecified, "ping", "ok")); got != 1 {
		t.Fatalf("identical target clients recorded ping %v times want=1", got)
	}
}

func TestAttachReturnsRegistrationAndOwnerConflicts(t *testing.T) {
	t.Run("registration conflict", func(t *testing.T) {
		client := &hookCountingClient{}
		registry := prometheus.NewRegistry()
		registry.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{
			Name: "farm_redis_workload_commands_total", Help: "intentional test conflict",
		}))
		if metrics, err := AttachFromEnvironment(client, registry, "true"); err == nil || metrics != nil {
			t.Fatalf("conflicting registration metrics=%v err=%v", metrics, err)
		}
		if got := client.hookCount(); got != 0 {
			t.Fatalf("registration failure attached %d hooks want=0", got)
		}
	})
	t.Run("default component conflict", func(t *testing.T) {
		registry := prometheus.NewRegistry()
		firstClient := &hookCountingClient{}
		if _, err := AttachFromEnvironment(firstClient, registry, "true"); err != nil {
			t.Fatal(err)
		}
		secondClient := &hookCountingClient{}
		if metrics, err := AttachFromEnvironmentWithComponent(secondClient, registry, "true", ComponentRouteControl); err == nil || metrics != nil {
			t.Fatalf("conflicting owner metrics=%v err=%v", metrics, err)
		}
		if got := secondClient.hookCount(); got != 0 {
			t.Fatalf("owner conflict attached %d hooks want=0", got)
		}
	})
}

func TestWorkloadMetricCardinalityIsClosedAndSecretFree(t *testing.T) {
	client := &hookCountingClient{}
	registry := prometheus.NewRegistry()
	metrics, err := AttachFromEnvironment(client, registry, "true")
	if err != nil {
		t.Fatal(err)
	}
	components := make([]string, 0, len(allowedComponents)+512)
	for component := range allowedComponents {
		components = append(components, component)
	}
	for index := 0; index < 512; index++ {
		components = append(components, fmt.Sprintf("secret-user-component-%d", index))
	}
	for index, component := range components {
		ctx := WithComponent(context.Background(), component)
		command := "get"
		if index%2 != 0 {
			command = fmt.Sprintf("secret-command-%d", index)
		}
		cmd := redis.NewStringCmd(ctx, command, "secret-key", "secret-value")
		kind := "single"
		if index%3 == 0 {
			kind = "pipeline"
		}
		metrics.observe(ctx, []redis.Cmder{cmd}, kind, nil, time.Millisecond)
	}

	limits := map[string]int{
		"farm_redis_workload_commands_total":              len(allowedComponents) * (len(allowedCommands) + 1) * 3,
		"farm_redis_workload_request_bytes_total":         len(allowedComponents) * (len(allowedCommands) + 1),
		"farm_redis_workload_response_bytes_total":        len(allowedComponents) * (len(allowedCommands) + 1),
		"farm_redis_workload_round_trips_total":           len(allowedComponents) * 2 * 3,
		"farm_redis_workload_round_trip_duration_seconds": len(allowedComponents) * 2,
		"farm_redis_workload_pipeline_width":              len(allowedComponents),
		"farm_redis_workload_batch_size":                  len(allowedComponents),
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		limit, expected := limits[family.GetName()]
		if !expected {
			t.Fatalf("unexpected workload metric family %q", family.GetName())
		}
		if got := len(family.GetMetric()); got > limit {
			t.Fatalf("metric family %q cardinality=%d exceeds bound=%d", family.GetName(), got, limit)
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				name, value := label.GetName(), label.GetValue()
				if strings.Contains(value, "secret") || strings.Contains(value, "user-") {
					t.Fatalf("metric family %q leaked label %s=%q", family.GetName(), name, value)
				}
				switch name {
				case "component":
					if _, ok := allowedComponents[value]; !ok {
						t.Fatalf("metric family %q exported unbounded component %q", family.GetName(), value)
					}
				case "command":
					if _, ok := allowedCommands[value]; !ok && value != unknownCommand {
						t.Fatalf("metric family %q exported unbounded command %q", family.GetName(), value)
					}
				case "outcome":
					if value != "ok" && value != "nil" && value != "error" {
						t.Fatalf("metric family %q exported unbounded outcome %q", family.GetName(), value)
					}
				case "kind":
					if value != "single" && value != "pipeline" {
						t.Fatalf("metric family %q exported unbounded kind %q", family.GetName(), value)
					}
				case "estimated":
					if value != "true" {
						t.Fatalf("metric family %q exported estimated=%q", family.GetName(), value)
					}
				default:
					t.Fatalf("metric family %q exported unexpected label %q", family.GetName(), name)
				}
			}
		}
	}
}
