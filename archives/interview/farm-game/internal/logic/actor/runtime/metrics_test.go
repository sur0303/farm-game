package runtime

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	clientmodel "github.com/prometheus/client_model/go"
)

func TestRegisterMetricsSnapshotsStatsOncePerGather(t *testing.T) {
	var calls atomic.Int32
	source := func() Stats {
		calls.Add(1)
		return Stats{
			ActiveKeys:              3,
			Queued:                  5,
			UserQueued:              4,
			SystemQueued:            1,
			UserBytes:               40,
			OldestUserWait:          2 * time.Second,
			MaxDepth:                4,
			Submitted:               9,
			Rejected:                7,
			UserRejected:            6,
			SystemRejected:          1,
			MailboxRetiredTotal:     2,
			MailboxRetireBusyTotal:  8,
			MailboxRetireStaleTotal: 1,
		}
	}
	registry := prometheus.NewPedanticRegistry()
	RegisterMetrics(registry, "player", source)

	want := map[string]float64{
		"farm_actor_active_keys":                      3,
		"farm_actor_mailbox_queued":                   5,
		"farm_actor_user_mailbox_queued":              4,
		"farm_actor_user_mailbox_bytes":               40,
		"farm_actor_user_mailbox_oldest_wait_seconds": 2,
		"farm_actor_system_queue_queued":              1,
		"farm_actor_mailbox_max_depth":                4,
		"farm_actor_submit_total":                     9,
		"farm_actor_mailbox_rejected_total":           7,
		"farm_actor_user_mailbox_rejected_total":      6,
		"farm_actor_system_queue_rejected_total":      1,
		"farm_actor_mailbox_retired_total":            2,
		"farm_actor_mailbox_retire_busy_total":        8,
		"farm_actor_mailbox_retire_stale_total":       1,
	}
	got := gatherActorMetrics(t, registry)
	if int(calls.Load()) != 1 {
		t.Fatalf("source calls=%d, want 1 per Gather", calls.Load())
	}
	if len(got) != len(want) {
		t.Fatalf("series=%v, want %d names", got, len(want))
	}
	for name, value := range want {
		sample, ok := got[name]
		if !ok {
			t.Fatalf("missing %s in %v", name, got)
		}
		if sample.value != value {
			t.Fatalf("%s=%v, want %v", name, sample.value, value)
		}
		if sample.kind != "player" {
			t.Fatalf("%s actor_kind=%q, want player", name, sample.kind)
		}
	}

	if _, err := registry.Gather(); err != nil {
		t.Fatal(err)
	}
	if int(calls.Load()) != 2 {
		t.Fatalf("source calls=%d, want 1 per Gather", calls.Load())
	}
}

type actorMetricSample struct {
	value float64
	kind  string
}

func gatherActorMetrics(t *testing.T, gatherer prometheus.Gatherer) map[string]actorMetricSample {
	t.Helper()
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]actorMetricSample{}
	for _, family := range families {
		if len(family.Metric) != 1 {
			t.Fatalf("%s series=%d, want 1", family.GetName(), len(family.Metric))
		}
		metric := family.Metric[0]
		sample := actorMetricSample{kind: metricLabel(metric, "actor_kind")}
		switch family.GetType() {
		case clientmodel.MetricType_COUNTER:
			sample.value = metric.GetCounter().GetValue()
		case clientmodel.MetricType_GAUGE:
			sample.value = metric.GetGauge().GetValue()
		default:
			t.Fatalf("%s type=%v", family.GetName(), family.GetType())
		}
		got[family.GetName()] = sample
	}
	return got
}

func metricLabel(metric *clientmodel.Metric, name string) string {
	for _, pair := range metric.Label {
		if pair.GetName() == name {
			return pair.GetValue()
		}
	}
	return ""
}
