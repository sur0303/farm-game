package dirtywriteback

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRegisterMetricsExportsSnapshot(t *testing.T) {
	registry := prometheus.NewRegistry()
	RegisterMetrics(registry, "farm", func() MetricsSnapshot {
		return MetricsSnapshot{
			Marks: 7, CoalescedMarks: 4, FlushAttempts: 3,
			FlushSucceeded: 2, FlushFailed: 1, QueueDepth: 5,
			Inflight: 1, OldestDirtyAge: 1500 * time.Millisecond, MaxQueueDepth: 9,
		}
	})
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{
		"farm_actor_dirty_writeback_marks_total":              7,
		"farm_actor_dirty_writeback_coalesced_marks_total":    4,
		"farm_actor_dirty_writeback_flush_attempts_total":     3,
		"farm_actor_dirty_writeback_flush_succeeded_total":    2,
		"farm_actor_dirty_writeback_flush_failed_total":       1,
		"farm_actor_dirty_writeback_queue_depth":              5,
		"farm_actor_dirty_writeback_inflight":                 1,
		"farm_actor_dirty_writeback_oldest_dirty_age_seconds": 1.5,
		"farm_actor_dirty_writeback_max_queue_depth":          9,
	}
	for _, family := range families {
		value, ok := want[family.GetName()]
		if !ok {
			continue
		}
		if len(family.Metric) != 1 {
			t.Fatalf("%s metric count=%d", family.GetName(), len(family.Metric))
		}
		metric := family.Metric[0]
		got := 0.0
		if metric.Counter != nil {
			got = metric.Counter.GetValue()
		} else if metric.Gauge != nil {
			got = metric.Gauge.GetValue()
		}
		if got != value {
			t.Errorf("%s=%v want=%v", family.GetName(), got, value)
		}
		delete(want, family.GetName())
	}
	if len(want) != 0 {
		t.Fatalf("missing metric families: %v", want)
	}
}
