package command

import (
	"fmt"
	"testing"

	commonpb "farmgame/api/gen/common"
	"farmgame/internal/actorrouting"

	"github.com/prometheus/client_golang/prometheus"
)

func TestBackendRejectReasonSeparatesRouteTransitions(t *testing.T) {
	if got := backendRejectReason(fmt.Errorf("wrapped: %w", actorrouting.ErrTransportGenerationTransition)); got != ReasonBackendRouteGeneration {
		t.Fatalf("route transition reason=%q want=%q", got, ReasonBackendRouteGeneration)
	}
	if got := backendRejectReason(fmt.Errorf("wrapped: %w", actorrouting.ErrStaleRoute)); got != ReasonBackendStaleRoute {
		t.Fatalf("stale route reason=%q want=%q", got, ReasonBackendStaleRoute)
	}
}

func TestObserveCommandResultRecordsStatusAndErrorCode(t *testing.T) {
	registry := prometheus.NewRegistry()
	gate := &Gate{metrics: NewMetrics(registry)}
	gate.observeCommandResult(&commonpb.CommandResultEnvelope{
		CommandType: commonpb.CommandType_HARVEST,
		Status:      commonpb.CommandStatus_REJECTED,
		Error: &commonpb.ErrorDetail{
			ErrorCode: commonpb.ErrorCode_ERR_CROP_NOT_MATURE,
		},
	})
	gate.observeCommandResult(&commonpb.CommandResultEnvelope{
		CommandType: commonpb.CommandType_SELL,
		Status:      commonpb.CommandStatus_SUCCEEDED,
	})

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var rejected, succeeded float64
	for _, family := range families {
		if family.GetName() != "farm_gate_command_results_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string, len(metric.GetLabel()))
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			switch {
			case labels["command_type"] == "HARVEST" && labels["status"] == "REJECTED" && labels["error_code"] == "ERR_CROP_NOT_MATURE":
				rejected = metric.GetCounter().GetValue()
			case labels["command_type"] == "SELL" && labels["status"] == "SUCCEEDED" && labels["error_code"] == "NONE":
				succeeded = metric.GetCounter().GetValue()
			}
		}
	}
	if rejected != 1 || succeeded != 1 {
		t.Fatalf("command result counters rejected=%v succeeded=%v, want 1/1", rejected, succeeded)
	}
}
