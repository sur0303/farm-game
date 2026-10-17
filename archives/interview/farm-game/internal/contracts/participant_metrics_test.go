package contracts

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestParticipantRouteLeaseProofMetricsHaveFixedCallerCardinality(t *testing.T) {
	registry := prometheus.NewRegistry()
	if err := RegisterParticipantRouteLeaseProofMetrics(registry); err != nil {
		t.Fatal(err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "farm_participant_legacy_route_lease_proof_accepted_total" {
			continue
		}
		if len(family.GetMetric()) != 2 {
			t.Fatalf("legacy route lease metric series=%d, want fixed 2", len(family.GetMetric()))
		}
		for _, metric := range family.GetMetric() {
			if len(metric.GetLabel()) != 1 || metric.GetLabel()[0].GetName() != "caller" {
				t.Fatalf("unexpected compatibility labels: %+v", metric.GetLabel())
			}
		}
		return
	}
	t.Fatal("legacy route lease compatibility metric is missing")
}
