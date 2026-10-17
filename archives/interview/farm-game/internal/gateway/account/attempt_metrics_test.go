package account

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestAttemptMetricsUseBoundedOutcomeAndDimensionLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewAttemptMetrics(registry)
	metrics.Observe("login", AttemptDecision{Allowed: true}, nil)
	metrics.Observe("login", AttemptDecision{
		Allowed: false, RetryAfter: time.Minute, LimitedBy: []string{"account", "ip"},
	}, nil)
	metrics.Observe("register", AttemptDecision{}, errors.New("redis unavailable"))

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]bool{
		"farm_gate_account_auth_attempts_total":      false,
		"farm_gate_account_auth_limited_total":       false,
		"farm_gate_account_auth_retry_after_seconds": false,
	}
	for _, family := range families {
		if _, ok := expected[family.GetName()]; ok && len(family.Metric) > 0 {
			expected[family.GetName()] = true
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetValue() == "farmer_one" || label.GetValue() == "192.0.2.1" ||
					label.GetValue() == "browser-a" {
					t.Fatalf("raw risk identifier leaked into metric label: %s=%s",
						label.GetName(), label.GetValue())
				}
			}
		}
	}
	for name, found := range expected {
		if !found {
			t.Fatalf("metric family %s was not observed", name)
		}
	}
}
