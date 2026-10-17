package command

import (
	"context"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/gateway/auth"

	"github.com/prometheus/client_golang/prometheus"
)

// conflictLogic answers every command the way a broken idempotency guard would:
// with the conflict itself instead of the original receipt.
type conflictLogic struct {
	testLogic
	code commonpb.ErrorCode
}

func (l *conflictLogic) ExecuteCommand(_ context.Context, _ *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	return &logicrpcpb.ExecuteCommandResponse{Outcome: &logicrpcpb.ExecuteCommandResponse_Business{
		Business: &logicrpcpb.BusinessCommandOutcome{
			Status: commonpb.CommandStatus_REJECTED,
			Error:  &logicrpcpb.ErrorDetail{ErrorCode: l.code, ErrorMessage: "operation_id reused with different payload"},
		},
	}}, nil
}

// The gate is where an outcome becomes what the client sees, so that is where
// the leak is counted. A rejection for any other reason is ordinary business.
func TestReceiptConflictIsCountedOnlyWhenItReachesTheClient(t *testing.T) {
	tests := []struct {
		name string
		code commonpb.ErrorCode
		want float64
	}{
		{name: "idempotency conflict", code: commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT, want: 1},
		{name: "ordinary rejection", code: commonpb.ErrorCode_ERR_NOT_IN_FARM, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
			gate := NewGate(authSvc, &conflictLogic{code: tt.code}, 64<<10)
			registry := prometheus.NewRegistry()
			gate.SetMetrics(NewMetrics(registry))
			activateHello(t, gate, authSvc, "leak-conn")

			if _, err := gate.HandleBinary(context.Background(), "leak-conn", 1,
				encodeCommandFrame(t, "leak-op", 2)); err != nil {
				t.Fatalf("command should return a business rejection, not a transport error: %v", err)
			}

			if got := counterTotal(t, registry, "farm_gate_command_receipt_conflict_leak_total"); got != tt.want {
				t.Fatalf("receipt_conflict_leak_total=%v after %s, want %v", got, tt.name, tt.want)
			}
		})
	}
}

// A healthy run must publish an explicit zero. An absent series would let the
// capacity oracle mistake "never measured" for "never happened".
func TestReceiptConflictGateIsPublishedBeforeItEverFires(t *testing.T) {
	registry := prometheus.NewRegistry()
	NewMetrics(registry)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == "farm_gate_command_receipt_conflict_leak_total" {
			return
		}
	}
	t.Fatal("farm_gate_command_receipt_conflict_leak_total is not registered")
}

// The capacity oracle reads local subscription lease expiry as the unexpected
// expiry gate. That reading only holds while Disconnect takes the connection
// and its lease schedules out before they can reach a deadline, so a
// disconnected session must leave the counter at an explicit zero.
func TestDisconnectDoesNotLookLikeAnUnexpectedLeaseExpiry(t *testing.T) {
	authSvc := newTestAuth(t, auth.Config{GatewayID: "gw", WssEndpoint: "wss://x"})
	gate := NewGate(authSvc, &testLogic{}, 64<<10)
	registry := prometheus.NewRegistry()
	gate.SetMetrics(NewMetrics(registry))
	activateHello(t, gate, authSvc, "gone-conn")

	gate.Disconnect("gone-conn", 1)
	gate.ExpireSubscriptionLeases(time.Now().Add(time.Hour))

	const name = "farm_gate_subscription_lease_local_expired_total"
	if got := counterTotal(t, registry, name); got != 0 {
		t.Fatalf("%s=%v after a plain disconnect, want 0", name, got)
	}
	if !published(t, registry, name) {
		t.Fatalf("%s must be published as zero, or the oracle cannot tell it from unmeasured", name)
	}
}

func published(t *testing.T, registry *prometheus.Registry, name string) bool {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return true
		}
	}
	return false
}

func counterTotal(t *testing.T, registry *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	total := 0.0
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			total += metric.GetCounter().GetValue()
		}
	}
	return total
}
