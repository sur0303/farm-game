package main

import (
	"fmt"
	"testing"

	"farmgame/internal/actorrouting/authority"
	core "farmgame/internal/routecontrol"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRouteDeadlineMetricsExposeBoundedPassOutcomes(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newRouteDeadlineMetrics(registry)
	metrics.observe(authority.DeadlineProjectionResult{Changed: true, Published: true, SnapshotBytes: 321, ChangedShards: 3, SnapshotRevision: 9, MinRemainingMs: 1234, ProjectionLagMs: 4000}, nil)
	metrics.observe(authority.DeadlineProjectionResult{MinRemainingMs: 1200}, nil)
	metrics.observe(authority.DeadlineProjectionResult{FailureStage: "read", MinRemainingMs: 900}, fmt.Errorf("wrapped: %w", authority.ErrDeadlineProjection))
	metrics.observe(authority.DeadlineProjectionResult{FailureStage: "publish", MinRemainingMs: 800}, fmt.Errorf("wrapped: %w", core.ErrSnapshotCASConflict))

	if got := testutil.ToFloat64(metrics.publishes); got != 1 {
		t.Fatalf("publishes=%v", got)
	}
	if got := testutil.ToFloat64(metrics.snapshotBytes); got != 321 {
		t.Fatalf("snapshot bytes=%v", got)
	}
	if got := testutil.ToFloat64(metrics.noops); got != 1 {
		t.Fatalf("noops=%v", got)
	}
	if got := testutil.ToFloat64(metrics.readErrors); got != 1 {
		t.Fatalf("read errors=%v", got)
	}
	if got := testutil.ToFloat64(metrics.casConflicts); got != 1 {
		t.Fatalf("CAS conflicts=%v", got)
	}
	if got := testutil.ToFloat64(metrics.changedShards); got != 0 {
		t.Fatalf("last changed shards=%v", got)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"route_deadline_refresh_pass_total": false, "route_deadline_refresh_publish_total": false,
		"route_snapshot_publish_total": false, "route_snapshot_bytes_total": false,
		"route_deadline_refresh_noop_total": false, "route_deadline_refresh_changed_shards": false,
		"route_deadline_refresh_cas_conflict_total": false, "route_deadline_refresh_read_error_total": false,
		"route_snapshot_min_remaining_ms": false, "route_deadline_projection_lag_ms": false,
	}
	for _, family := range families {
		if _, ok := want[family.GetName()]; ok {
			want[family.GetName()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("metric %s missing", name)
		}
	}
}
