package sourceordermetrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestPrivateRegistryGatherIncludesSourceOrderMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	Register(registry)

	ObserveWorkflowAdvance(17)
	ObserveActorCommit(23, 29)
	ObserveReleaseSeed("registry_test")
	ObservePlantInvariantReject()
	ObserveMailOverflow()

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := make(map[string]bool, len(families))
	for _, family := range families {
		found[family.GetName()] = true
	}
	for _, name := range []string{
		"farm_workflow_advance_writes_total",
		"farm_workflow_advance_bytes_total",
		"farm_actor_commit_bytes_total",
		"farm_source_order_payload_bytes_total",
		"farm_plant_release_seed_total",
		"farm_plant_farm_invariant_reject_total",
		"farm_source_order_mail_overflow_total",
	} {
		if !found[name] {
			t.Fatalf("private registry did not gather %s (all=%v)", name, found)
		}
	}
}
