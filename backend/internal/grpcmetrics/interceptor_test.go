package grpcmetrics

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUnaryInterceptorRecordsCallsAndLatency(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	interceptor := m.UnaryServerInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/logicrpc.LogicService/ExecuteCommand"}
	if _, err := interceptor(context.Background(), "req", info, func(context.Context, any) (any, error) {
		return "resp", nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := interceptor(context.Background(), "bad", info, func(context.Context, any) (any, error) {
		return nil, status.Error(codes.NotFound, "missing")
	}); err == nil {
		t.Fatal("expected handler error")
	}

	metricFamilies, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	findCounter := func(name string) (float64, bool) {
		for _, mf := range metricFamilies {
			if mf.GetName() != name {
				continue
			}
			var total float64
			for _, metric := range mf.GetMetric() {
				total += metric.GetCounter().GetValue()
			}
			return total, true
		}
		return 0, false
	}
	total, ok := findCounter("farm_rpc_calls_total")
	if !ok || total != 2 {
		t.Fatalf("farm_rpc_calls_total=%v ok=%v, want 2", total, ok)
	}
	for _, want := range []string{"OK", "NotFound"} {
		found := false
		for _, mf := range metricFamilies {
			if mf.GetName() != "farm_rpc_calls_total" {
				continue
			}
			for _, metric := range mf.GetMetric() {
				for _, label := range metric.GetLabel() {
					if label.GetName() == "code" && label.GetValue() == want {
						found = true
					}
				}
			}
		}
		if !found {
			t.Fatalf("missing code label %q", want)
		}
	}
	latencySeen := false
	for _, mf := range metricFamilies {
		if mf.GetName() == "farm_rpc_server_seconds" && mf.GetMetric()[0].GetHistogram().GetSampleCount() == 2 {
			latencySeen = true
		}
	}
	if !latencySeen {
		t.Fatal("farm_rpc_server_seconds histogram missing 2 samples")
	}
}

func TestSplitFullMethod(t *testing.T) {
	service, method := splitFullMethod("/farm.logicrpc.LogicService/ExecuteCommand")
	if service != "farm.logicrpc.LogicService" || method != "ExecuteCommand" {
		t.Fatalf("split=(%q,%q)", service, method)
	}
}
