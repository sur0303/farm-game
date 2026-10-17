package downlink

import (
	"testing"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRegisterMetricsReflectsLaneLifecycle(t *testing.T) {
	config := DefaultDispatcherConfig()
	config.LaneIdleTimeout = 10 * time.Millisecond
	dispatcher := NewDispatcherWithConfig(&captureGateway{}, config)
	defer dispatcher.Close()
	registry := prometheus.NewRegistry()
	RegisterMetrics(registry, dispatcher)
	if err := dispatcher.Enqueue(&logicrpcpb.DownlinkItem{ItemId: "metric"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && dispatcher.Stats().LanesRetired == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]float64{}
	for _, family := range families {
		if len(family.Metric) == 0 {
			continue
		}
		metric := family.Metric[0]
		switch {
		case metric.Counter != nil:
			values[family.GetName()] = metric.Counter.GetValue()
		case metric.Gauge != nil:
			values[family.GetName()] = metric.Gauge.GetValue()
		}
	}
	if values["farm_downlink_lanes_created_total"] != 1 || values["farm_downlink_lanes_retired_total"] != 1 ||
		values["farm_downlink_lanes_current"] != 0 {
		t.Fatalf("lane metrics=%v", values)
	}
}
