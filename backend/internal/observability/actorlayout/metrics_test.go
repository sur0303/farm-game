package actorlayout

import (
	"context"
	"testing"

	"farmgame/internal/sourceordermetrics"
	"farmgame/internal/storage/authoritystore"

	"github.com/prometheus/client_golang/prometheus"
	clientmodel "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
)

func TestMetricsBoundLabelsAndCountOnlyObservedRedisRoundTrips(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	process := metrics.ProcessHook(func(context.Context, redis.Cmder) error { return nil })
	pipeline := metrics.ProcessPipelineHook(func(context.Context, []redis.Cmder) error { return nil })

	// A shared-client dispatch outside an Authority Commit must not leak into
	// redis_round_trips. The hooks are invoked directly so this unit test does
	// not require a listening socket.
	if err := process(context.Background(), redis.NewCmd(context.Background(), "PING")); err != nil {
		t.Fatal(err)
	}

	ctx, complete := metrics.BeginAuthorityCommit(context.Background(), "PLAYER", "user-controlled-value")
	if err := pipeline(ctx, []redis.Cmder{redis.NewCmd(ctx, "GET", "missing-a"), redis.NewCmd(ctx, "GET", "missing-b")}); err != nil {
		t.Fatal(err)
	}
	complete(authoritystore.CommitObservation{
		ActorRecordBytes: 10, CommitRequestBytes: 20, CommitResponseBytes: 30,
		ReceiptBytes: 40, OrderBytes: []int{45}, ModelStateBytes: 50,
	})

	assertHistogramCountAndSum(t, registry, "redis_round_trips", map[string]string{"domain": "player"}, 1, 1)
	assertHistogramCountAndSum(t, registry, "authority_commit_request_bytes", map[string]string{"domain": "player", "command": "OTHER"}, 1, 20)
	assertHistogramCountAndSum(t, registry, "actor_record_serialized_bytes", map[string]string{"domain": "player"}, 1, 10)
	assertHistogramCountAndSum(t, registry, "authority_commit_response_bytes", map[string]string{"domain": "player"}, 1, 30)
	assertHistogramCountAndSum(t, registry, "receipt_bytes", map[string]string{"domain": "player"}, 1, 40)
	assertHistogramCountAndSum(t, registry, "order_bytes", map[string]string{"domain": "player"}, 1, 45)
	assertHistogramCountAndSum(t, registry, "model_state_bytes", map[string]string{"domain": "player"}, 1, 50)
}

func TestCommittedObservationPublishesSourceMetricsThroughPrivateRegistry(t *testing.T) {
	registry := prometheus.NewRegistry()
	sourceordermetrics.Register(registry)
	metrics := NewMetrics(registry)
	_, complete := metrics.BeginAuthorityCommit(context.Background(), "FARM", "AR1_RECORD_COMMIT")
	complete(authoritystore.CommitObservation{
		Status: authoritystore.CommitCommitted.String(), PhysicalCommitBytes: 123, OrderPayloadBytes: 45,
	})

	assertCounterValue(t, registry, "farm_actor_commit_bytes_total", 123)
	assertCounterValue(t, registry, "farm_source_order_payload_bytes_total", 45)
}

func assertCounterValue(t *testing.T, gatherer prometheus.Gatherer, name string, want float64) {
	t.Helper()
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name && len(family.GetMetric()) == 1 {
			if got := family.GetMetric()[0].GetCounter().GetValue(); got != want {
				t.Fatalf("%s=%v want=%v", name, got, want)
			}
			return
		}
	}
	t.Fatalf("metric %s not found", name)
}

func assertHistogramCountAndSum(t *testing.T, gatherer prometheus.Gatherer, name string, labels map[string]string, wantCount uint64, wantSum float64) {
	t.Helper()
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			if metricLabels(metric).matches(labels) {
				if metric.GetHistogram().GetSampleCount() != wantCount || metric.GetHistogram().GetSampleSum() != wantSum {
					t.Fatalf("%s count/sum=(%d,%v) want=(%d,%v)", name, metric.GetHistogram().GetSampleCount(), metric.GetHistogram().GetSampleSum(), wantCount, wantSum)
				}
				return
			}
		}
	}
	t.Fatalf("metric %s labels=%v not found", name, labels)
}

type labelSet map[string]string

func metricLabels(metric *clientmodel.Metric) labelSet {
	labels := make(labelSet, len(metric.Label))
	for _, pair := range metric.Label {
		labels[pair.GetName()] = pair.GetValue()
	}
	return labels
}

func (set labelSet) matches(want map[string]string) bool {
	if len(set) != len(want) {
		return false
	}
	for name, value := range want {
		if set[name] != value {
			return false
		}
	}
	return true
}
