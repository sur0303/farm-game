package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestCollectionRuntimeStopsFailClosed(t *testing.T) {
	t.Run("missing metric", func(t *testing.T) {
		s := newCollectionState()
		got := s.addSample(resourceSnapshot{MissingMetrics: []string{"mailbox_wait"}, MetricValues: map[string]float64{}}, 60, 5)
		if got != VerdictInconclusiveObservability {
			t.Fatalf("got=%s", got)
		}
	})
	t.Run("environment", func(t *testing.T) {
		s := newCollectionState()
		got := s.addSample(resourceSnapshot{MetricValues: map[string]float64{"identity_drift": 1}}, 60, 5)
		if got != VerdictInconclusiveEnvironment {
			t.Fatalf("got=%s", got)
		}
	})
	t.Run("dependency sustained", func(t *testing.T) {
		s := newCollectionState()
		sample := resourceSnapshot{DependencyCPUPercent: 90, MetricValues: map[string]float64{}}
		var got string
		for i := 0; i < 60; i++ {
			got = s.addSample(sample, 60, 5)
		}
		if got != VerdictInconclusiveDependency {
			t.Fatalf("got=%s", got)
		}
	})
	t.Run("target sustained", func(t *testing.T) {
		s := newCollectionState()
		sample := resourceSnapshot{ServiceCPUPercent: 95, MetricValues: map[string]float64{}}
		var got string
		for i := 0; i < 60; i++ {
			got = s.addSample(sample, 60, 5)
		}
		if got != VerdictFailTargetSaturation {
			t.Fatalf("got=%s", got)
		}
	})
}

func TestSnapshotScrapeGapUnderFiveSecondsRecovers(t *testing.T) {
	s := newCollectionState()
	failed := resourceSnapshot{ScrapeErrorCode: "SNAPSHOT_SCRAPE_FAILED", MetricValues: map[string]float64{}}
	for i := 0; i < 5; i++ {
		if got := s.addSample(failed, 60, 6); got != "" {
			t.Fatalf("early stop=%s", got)
		}
	}
	if got := s.addSample(resourceSnapshot{MetricValues: map[string]float64{}}, 60, 6); got != "" {
		t.Fatalf("recovery stop=%s", got)
	}
	_, _, _, complete, _ := s.snapshot()
	if !complete {
		t.Fatal("transient gap marked incomplete")
	}
}

func TestSnapshotScrapeGapOverFiveSecondsStops(t *testing.T) {
	s := newCollectionState()
	failed := resourceSnapshot{ScrapeErrorCode: "SNAPSHOT_SCRAPE_FAILED", MetricValues: map[string]float64{}}
	var got string
	for i := 0; i < 6; i++ {
		got = s.addSample(failed, 60, 6)
	}
	if got != VerdictInconclusiveObservability {
		t.Fatalf("got=%s", got)
	}
}

func TestRealFetchFailureUsesGapStateMachine(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	collector := &collector{plan: runPlan{SnapshotURL: server.URL}, client: adapterHTTPClient(), state: newCollectionState(), accumulator: newRunAccumulator()}
	failed := collector.fetchSnapshot(context.Background())
	if failed.ScrapeErrorCode != "SNAPSHOT_SCRAPE_FAILED" || len(failed.MissingMetrics) != 0 {
		t.Fatalf("failed=%+v", failed)
	}
	for i := 0; i < 5; i++ {
		if got := collector.state.addSample(failed, 60, 6); got != "" {
			t.Fatalf("early=%s", got)
		}
	}
	if got := collector.state.addSample(failed, 60, 6); got != VerdictInconclusiveObservability {
		t.Fatalf("late=%s", got)
	}
}

func TestCounterResetFailsObservabilityAndIsRetained(t *testing.T) {
	s := newCollectionState(map[string]string{"completed_total": "counter"})
	if got := s.addSample(resourceSnapshot{MetricValues: map[string]float64{"completed_total": 10}}, 60, 5); got != "" {
		t.Fatalf("first=%s", got)
	}
	if got := s.addSample(resourceSnapshot{MetricValues: map[string]float64{"completed_total": 9}}, 60, 5); got != VerdictInconclusiveObservability {
		t.Fatalf("reset=%s", got)
	}
	last := s.lastSample()
	if len(last.MissingMetrics) != 1 || last.MissingMetrics[0] != "COUNTER_RESET:completed_total" {
		t.Fatalf("missing=%v", last.MissingMetrics)
	}
}

func TestNormalizeSnapshotIncludesHarnessCounters(t *testing.T) {
	acc := newRunAccumulator()
	acc.planned = 10
	acc.launched = 9
	acc.completed = 8
	acc.unsent = 1
	sample := resourceSnapshot{ServiceCPUPercent: 50, MetricValues: map[string]float64{}}
	normalizeSnapshotMetrics(&sample, acc, nil)
	if sample.MetricValues["planned_arrivals_total"] != 10 || sample.MetricValues["completed_total"] != 8 || sample.MetricValues["service_cpu_percent"] != 50 {
		t.Fatalf("values=%v", sample.MetricValues)
	}
}

func TestCollectorEnforcesRequiredMetricsAndOracleGateCompleteness(t *testing.T) {
	snapshotServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(resourceSnapshot{MetricValues: map[string]float64{}})
	}))
	defer snapshotServer.Close()
	oracleServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(oracleSnapshot{ObservationComplete: true, Gates: map[string]int64{"duplicate_grant_total": 0}})
	}))
	defer oracleServer.Close()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	plan := runPlan{Service: "worker", BindingPath: filepath.Join(root, "deploy", "load", "cap-single-pod-r1-binding.json"), RequiredMetricsPath: filepath.Join(root, "docs", "verification", "perf", "cap-single-pod", "CAP-SINGLE-POD-L2-R1", "required-metrics.json"), SnapshotURL: snapshotServer.URL, OracleURL: oracleServer.URL}
	collector, err := newCollector(plan, filepath.Join(t.TempDir(), "samples.ndjson"), newRunAccumulator(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer collector.close()
	sample := collector.fetchSnapshot(context.Background())
	if len(sample.MissingMetrics) == 0 {
		t.Fatal("missing required metrics accepted")
	}
	oracle := collector.fetchOracle(context.Background())
	if oracle.ObservationComplete || oracle.ErrorCode != "ORACLE_GATE_SET_INCOMPLETE" {
		t.Fatalf("oracle=%+v", oracle)
	}
}

func TestOracleIncompleteStopsNewArrivalsImmediately(t *testing.T) {
	s := newCollectionState()
	stop := s.addOracle(oracleSnapshot{ObservationComplete: false, Gates: map[string]int64{}, ErrorCode: "ORACLE_SCRAPE_FAILED"})
	if !stop {
		t.Fatal("oracle failure did not request stop")
	}
	_, _, complete, observable, runtimeStop := s.snapshot()
	if complete || observable || runtimeStop != VerdictInconclusiveObservability {
		t.Fatalf("complete=%v observable=%v stop=%s", complete, observable, runtimeStop)
	}
}

func TestArrivalLagPositiveSlopeFailsQualification(t *testing.T) {
	assertDriverSlopeStops(t, "arrival_lag_p99_ms")
}
func TestDriverEventBacklogPositiveSlopeFailsQualification(t *testing.T) {
	assertDriverSlopeStops(t, "driver_event_backlog")
}
func TestDriverSocketErrorPositiveSlopeFailsQualification(t *testing.T) {
	assertDriverSlopeStops(t, "driver_socket_error_total")
}
func assertDriverSlopeStops(t *testing.T, name string) {
	t.Helper()
	s := newCollectionState()
	var got string
	for i := 0; i < 60; i++ {
		sample := resourceSnapshot{MetricValues: map[string]float64{name: float64(i), "actual_to_planned_ratio": 1}}
		got = s.addSample(sample, 60, 6)
		if got != "" {
			break
		}
	}
	if got != VerdictInconclusiveDriver {
		t.Fatalf("metric=%s got=%s", name, got)
	}
}

func TestOracleSnapshotsAreLevelsNotSummed(t *testing.T) {
	s := newCollectionState()
	for i := 0; i < 5; i++ {
		if !s.addOracle(oracleSnapshot{ObservationComplete: true, Gates: map[string]int64{"duplicate_grant_total": 1}}) {
			t.Fatal("signal missing")
		}
	}
	_, gates, complete, _, _ := s.snapshot()
	if !complete || gates["duplicate_grant_total"] != 1 {
		t.Fatalf("gates=%v", gates)
	}
}
