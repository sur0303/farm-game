package loadbot

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestMetricsUseOnlyLowCardinalityLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	metrics.ObserveRun("player-123/run-456", "unexpected-operation-789")
	metrics.ObserveAction(Action("player-123"), "unexpected-farm-456")
	metrics.ObserveLease("worker-999", "shard-12")
	metrics.ObserveDuration("run-999", "operation-888", time.Second)
	metrics.SetAssignments("worker-999", 3)
	metrics.SetAssignments(MetricAssignmentActive, 2)

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	if len(families) != 5 {
		t.Fatalf("metric family count = %d, want 5", len(families))
	}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				name := label.GetName()
				switch name {
				case "phase", "outcome", "action", "event", "state":
				default:
					t.Fatalf("metric %q has unapproved label %q", family.GetName(), name)
				}
				value := label.GetValue()
				for _, forbidden := range []string{"player-123", "farm-456", "operation-789", "run-999", "shard-12", "worker-999"} {
					if strings.Contains(value, forbidden) {
						t.Fatalf("metric %q leaked high-cardinality identity %q in label value %q", family.GetName(), forbidden, value)
					}
				}
			}
		}
	}
	if value := metricLabelValue(families, "farm_mixed_farm_bot_run_events_total", "phase"); value != MetricOutcomeOther {
		t.Fatalf("unknown run phase normalized to %q, want %q", value, MetricOutcomeOther)
	}
	if value := metricLabelValue(families, "farm_mixed_farm_bot_bot_actions_total", "action"); value != MetricOutcomeOther {
		t.Fatalf("unknown action normalized to %q, want %q", value, MetricOutcomeOther)
	}
}

func metricLabelValue(families []*dto.MetricFamily, familyName, labelName string) string {
	for _, family := range families {
		if family.GetName() != familyName {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == labelName {
					return label.GetValue()
				}
			}
		}
	}
	return ""
}

func TestMetricNormalizationKeepsKnownValues(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	metrics.ObserveRun(MetricPhasePrepare, MetricOutcomeOK)
	metrics.ObserveAction(ActionTheft, MetricOutcomeRejected)
	metrics.ObserveLease(MetricLeaseExpired, MetricOutcomeExpired)
	metrics.SetAssignments(MetricAssignmentReleased, 1)
	metrics.ObserveDuration(MetricPhaseCleanup, MetricOutcomeOK, 250*time.Millisecond)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 5 {
		t.Fatalf("metric family count = %d, want 5", len(families))
	}
}
