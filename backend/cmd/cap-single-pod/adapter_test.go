package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPrometheusMappingReadsLabelsAndHistogram(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "# TYPE test_queue gauge\ntest_queue{role=\"a\"} 2\ntest_queue{role=\"b\"} 3\n# TYPE test_latency histogram\ntest_latency_bucket{le=\"0.1\"} 1\ntest_latency_bucket{le=\"1\"} 10\ntest_latency_bucket{le=\"+Inf\"} 10\ntest_latency_sum 5\ntest_latency_count 10\n")
	}))
	defer server.Close()
	snapshot, err := scrapePrometheusSources(context.Background(), adapterHTTPClient(), []prometheusSource{{Name: "s", URL: server.URL}})
	if err != nil {
		t.Fatal(err)
	}
	queue, err := mappedMetricValue(snapshot, metricMapping{LogicalName: "q", Source: "s", Metric: "test_queue", Value: "value", Aggregate: "SUM"})
	if err != nil || queue != 5 {
		t.Fatalf("queue=%f err=%v", queue, err)
	}
	p99, err := mappedMetricValue(snapshot, metricMapping{LogicalName: "p", Source: "s", Metric: "test_latency", Value: "p99", Aggregate: "SINGLE"})
	if err != nil || p99 != 1 {
		t.Fatalf("p99=%f err=%v", p99, err)
	}
}

func TestOracleAdapterCounterDeltaGaugeAndReset(t *testing.T) {
	var mu sync.Mutex
	counter, residual := 10, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		_, _ = fmt.Fprintf(w, "# TYPE errors_total counter\nerrors_total %d\n# TYPE residual gauge\nresidual %d\n", counter, residual)
	}))
	defer server.Close()
	config := oracleAdapterConfig{Sources: []prometheusSource{{Name: "s", URL: server.URL}}, Gates: []oracleGateMapping{
		{Name: "errors", Source: "s", Metric: "errors_total", Value: "value", Aggregate: "SINGLE", Mode: "COUNTER_DELTA"},
		{Name: "residual", Source: "s", Metric: "residual", Value: "value", Aggregate: "SINGLE", Mode: "GAUGE_CURRENT"},
	}}
	adapter := oracleAdapter{config: config, client: adapterHTTPClient(), baseline: map[string]float64{}}
	if err := adapter.arm(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	counter = 12
	residual = 1
	mu.Unlock()
	got := adapter.observe(context.Background())
	if !got.ObservationComplete || got.Gates["errors"] != 2 || got.Gates["residual"] != 1 {
		t.Fatalf("got=%+v", got)
	}
	mu.Lock()
	counter = 9
	mu.Unlock()
	got = adapter.observe(context.Background())
	if got.ObservationComplete || got.ErrorCode != "ORACLE_COUNTER_RESET" {
		t.Fatalf("reset=%+v", got)
	}
}

func TestAdapterHealthBindsExactBinaryAndConfig(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binarySHA, err := exactFileSHA256(executable)
	if err != nil {
		t.Fatal(err)
	}
	serve := func(kind, configSHA string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(adapterHealth{SchemaVersion: "cap-single-pod-l2.adapter-health.v0.1", Kind: kind, BinarySHA256: binarySHA, ConfigSHA256: configSHA})
		}))
	}
	collectorSHA := strings.Repeat("a", 64)
	oracleSHA := strings.Repeat("b", 64)
	collector := serve("collector", collectorSHA)
	defer collector.Close()
	oracle := serve("oracle", oracleSHA)
	defer oracle.Close()
	plan := runPlan{CollectorHealthURL: collector.URL, OracleHealthURL: oracle.URL, CollectorConfigSHA256: collectorSHA, OracleConfigSHA256: oracleSHA}
	if err := verifyAdapterHealth(plan); err != nil {
		t.Fatal(err)
	}
	plan.OracleConfigSHA256 = strings.Repeat("c", 64)
	if err := verifyAdapterHealth(plan); err == nil {
		t.Fatal("oracle config drift accepted")
	}
}

func TestCollectorAdapterReadsCgroupAndIdentity(t *testing.T) {
	root := t.TempDir()
	service := writeFakeCgroup(t, root, "service", 4, 4096, 100)
	driver := writeFakeCgroup(t, root, "driver", 8, 8192, 200)
	observer := writeFakeCgroup(t, root, "observer", 1, 1024, 50)
	identity := filepath.Join(root, "identity")
	if err := os.WriteFile(identity, []byte("exact"), 0o600); err != nil {
		t.Fatal(err)
	}
	digest, _ := exactFileSHA256(identity)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "# TYPE service_heap_bytes gauge\nservice_heap_bytes 123\n# TYPE primary_queue gauge\nprimary_queue 7\n")
	}))
	defer server.Close()
	config := collectorAdapterConfig{AllowedCgroupRoot: root, Sources: []prometheusSource{{Name: "service", URL: server.URL}}, Mappings: []metricMapping{
		{LogicalName: "service_heap_bytes", Source: "service", Metric: "service_heap_bytes", Value: "value", Aggregate: "SINGLE"},
		{LogicalName: "primary_queue", Source: "service", Metric: "primary_queue", Value: "value", Aggregate: "SINGLE"},
	}, MeasuredService: cgroupProbeConfig{Name: "service", Path: service, ExpectedCPUQuota: 4, ExpectedMemoryBytes: 4096}, Driver: cgroupProbeConfig{Name: "driver", Path: driver, ExpectedCPUQuota: 8, ExpectedMemoryBytes: 8192}, Observer: cgroupProbeConfig{Name: "observer", Path: observer, ExpectedCPUQuota: 1, ExpectedMemoryBytes: 1024}, IdentityFiles: []identityFileConfig{{Path: identity, SHA256: digest}}, PrimaryQueueMetric: "primary_queue"}
	stat := filepath.Join(root, "stat")
	if err := os.WriteFile(stat, []byte("cpu  100 0 100 800 0 0 0 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	adapter := collectorAdapter{config: config, client: adapterHTTPClient(), host: newHostSampler(stat), previous: map[string]cpuPrevious{}}
	first := adapter.collect(context.Background())
	if first.ScrapeErrorCode != "" || first.PrimaryQueueItems != 7 || first.MetricValues["service_heap_bytes"] != 123 || first.MetricValues["identity_drift"] != 0 {
		t.Fatalf("first=%+v", first)
	}
	time.Sleep(10 * time.Millisecond)
	if err := os.WriteFile(filepath.Join(service, "cpu.stat"), []byte("usage_usec 2100\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	second := adapter.collect(context.Background())
	if second.ServiceCPUPercent <= 0 || second.MetricValues["cgroup_limit_drift"] != 0 {
		t.Fatalf("second=%+v", second)
	}
	if err := os.WriteFile(identity, []byte("drift"), 0o600); err != nil {
		t.Fatal(err)
	}
	third := adapter.collect(context.Background())
	if third.MetricValues["identity_drift"] != 1 {
		t.Fatalf("third=%+v", third)
	}
}

// A container is not owned by the observer watching it, so /proc/<pid>/fd is
// closed to it and the pid inside the cgroup buys nothing. Both signals that used
// to depend on it are published by the service itself, and a run that has neither
// source has to say so rather than report a comfortable zero.
func TestDescriptorsAndRestartsAreTakenFromTheServiceWhenProcfsIsClosed(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		serviceExport bool
		wantFDPercent float64
		wantMissing   []string
	}{
		{
			name:          "the service publishes its own descriptors and start time",
			serviceExport: true,
			wantFDPercent: 20,
		},
		{
			name:        "neither the service nor procfs can answer",
			wantMissing: []string{"measured_service_fd_percent_of_limit", "service_fd_open", "unexpected_container_restart_total"},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			root := t.TempDir()
			service := writeFakeCgroup(t, root, "service", 4, 4096, 100)
			// An empty roster is how a cgroup the observer may read looks when the
			// process inside it belongs to somebody else.
			if err := os.WriteFile(filepath.Join(service, "cgroup.procs"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			driver := writeFakeCgroup(t, root, "driver", 8, 8192, 200)
			observer := writeFakeCgroup(t, root, "observer", 1, 1024, 50)

			startedAt := 1000.0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprintf(w, "# TYPE process_open_fds gauge\nprocess_open_fds 40\n"+
					"# TYPE process_max_fds gauge\nprocess_max_fds 200\n"+
					"# TYPE process_start_time_seconds gauge\nprocess_start_time_seconds %f\n", startedAt)
			}))
			defer server.Close()

			config := collectorAdapterConfig{
				AllowedCgroupRoot: root,
				Sources:           []prometheusSource{{Name: "service", URL: server.URL}},
				MeasuredService:   cgroupProbeConfig{Name: "service", Path: service, ExpectedCPUQuota: 4, ExpectedMemoryBytes: 4096},
				Driver:            cgroupProbeConfig{Name: "driver", Path: driver, ExpectedCPUQuota: 8, ExpectedMemoryBytes: 8192},
				Observer:          cgroupProbeConfig{Name: "observer", Path: observer, ExpectedCPUQuota: 1, ExpectedMemoryBytes: 1024},
			}
			if testCase.serviceExport {
				config.Mappings = []metricMapping{
					{LogicalName: serviceOpenFDsMetric, Source: "service", Metric: "process_open_fds", Value: "value", Aggregate: "SINGLE"},
					{LogicalName: serviceMaxFDsMetric, Source: "service", Metric: "process_max_fds", Value: "value", Aggregate: "SINGLE"},
					{LogicalName: serviceStartTimeMetric, Source: "service", Metric: "process_start_time_seconds", Value: "value", Aggregate: "SINGLE"},
				}
			}
			stat := filepath.Join(root, "stat")
			if err := os.WriteFile(stat, []byte("cpu  100 0 100 800 0 0 0 0\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			adapter := collectorAdapter{config: config, client: adapterHTTPClient(), host: newHostSampler(stat), previous: map[string]cpuPrevious{}}

			snapshot := adapter.collect(context.Background())
			for _, name := range testCase.wantMissing {
				if _, present := snapshot.MetricValues[name]; present {
					t.Fatalf("%s was reported as %v when nothing could measure it", name, snapshot.MetricValues[name])
				}
				if !slices.Contains(snapshot.MissingMetrics, name) {
					t.Fatalf("%s is absent but not declared missing: %v", name, snapshot.MissingMetrics)
				}
			}
			if testCase.wantMissing != nil {
				return
			}

			if got := snapshot.MetricValues["measured_service_fd_percent_of_limit"]; got != testCase.wantFDPercent {
				t.Fatalf("fd percent = %v, want %v", got, testCase.wantFDPercent)
			}
			if got := snapshot.ServiceFDOpen; got != 40 {
				t.Fatalf("open descriptors = %d, want 40", got)
			}
			if got := snapshot.MetricValues["unexpected_container_restart_total"]; got != 0 {
				t.Fatalf("first sighting counted as a restart: %v", got)
			}
			// A clock nudging the derived start time is not a restart.
			startedAt = 1000.4
			if got := adapter.collect(context.Background()).MetricValues["unexpected_container_restart_total"]; got != 0 {
				t.Fatalf("a nudged start time counted as a restart: %v", got)
			}
			startedAt = 2000
			if got := adapter.collect(context.Background()).MetricValues["unexpected_container_restart_total"]; got != 1 {
				t.Fatalf("restart total = %v, want 1", got)
			}
		})
	}
}

func writeFakeCgroup(t *testing.T, root, name string, cpu float64, memory, usage int64) string {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	// The probe reads the process inside the cgroup to count file descriptors and to
	// notice a restart, so the fixture names a process that really exists.
	files := map[string]string{
		"cpu.stat": fmt.Sprintf("usage_usec %d\n", usage), "cpu.max": fmt.Sprintf("%.0f 100000\n", cpu*100000),
		"memory.current": fmt.Sprintf("%d\n", memory/2), "memory.max": fmt.Sprintf("%d\n", memory),
		"memory.events": "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n",
		"cgroup.procs":  fmt.Sprintf("%d\n", os.Getpid()),
	}
	for file, value := range files {
		if err := os.WriteFile(filepath.Join(path, file), []byte(value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return path
}
