package capcorrectness

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func serviceExposing(t *testing.T, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

func producerFor(t *testing.T, endpoint string, metric string, labels map[string]string) *ScrapeProducer {
	t.Helper()
	producer, err := NewScrapeProducer(&http.Client{Timeout: time.Second}, endpoint+"/metrics", metric, labels)
	if err != nil {
		t.Fatal(err)
	}
	return producer
}

func TestScrapeReadsAnExplicitZero(t *testing.T) {
	server := serviceExposing(t, `# TYPE farm_receipt_conflict_leak_total counter
farm_receipt_conflict_leak_total 0
`)
	value, err := producerFor(t, server.URL, "farm_receipt_conflict_leak_total", nil).Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if value != 0 {
		t.Fatalf("value=%v", value)
	}
}

// A counter that only appears after its first increment is indistinguishable
// from a service that never registered it, so the run must not read either as
// a clean zero.
func TestUnexportedMetricIsAnError(t *testing.T) {
	server := serviceExposing(t, "# TYPE something_else_total counter\nsomething_else_total 3\n")
	_, err := producerFor(t, server.URL, "farm_silent_drop_total", nil).Observe(context.Background())
	if err == nil {
		t.Fatal("an unexported gate counter must not observe as zero")
	}
	if !strings.Contains(err.Error(), "farm_silent_drop_total") {
		t.Fatalf("error must name the gate: %v", err)
	}
}

func TestScrapeSumsEverySeriesOfOneInvariant(t *testing.T) {
	server := serviceExposing(t, `# TYPE farm_wrong_scope_dispatch_total counter
farm_wrong_scope_dispatch_total{shard="1"} 2
farm_wrong_scope_dispatch_total{shard="2"} 5
`)
	value, err := producerFor(t, server.URL, "farm_wrong_scope_dispatch_total", nil).Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if value != 7 {
		t.Fatalf("value=%v want the total across shards", value)
	}
}

func TestScrapeSelectsRequestedLabels(t *testing.T) {
	server := serviceExposing(t, `# TYPE farm_lease_expired_total counter
farm_lease_expired_total{reason="planned"} 9
farm_lease_expired_total{reason="unexpected"} 1
`)
	value, err := producerFor(t, server.URL, "farm_lease_expired_total", map[string]string{"reason": "unexpected"}).Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if value != 1 {
		t.Fatalf("value=%v want only the unexpected series", value)
	}
}

func TestMissingLabelSetIsAnError(t *testing.T) {
	server := serviceExposing(t, `# TYPE farm_lease_expired_total counter
farm_lease_expired_total{reason="planned"} 9
`)
	_, err := producerFor(t, server.URL, "farm_lease_expired_total", map[string]string{"reason": "unexpected"}).Observe(context.Background())
	if err == nil {
		t.Fatal("a label set that never appeared must not observe as zero")
	}
}

func TestUnreachableServiceIsAnError(t *testing.T) {
	producer, err := NewScrapeProducer(&http.Client{Timeout: 100 * time.Millisecond}, "http://127.0.0.1:1/metrics", "farm_silent_drop_total", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := producer.Observe(context.Background()); err == nil {
		t.Fatal("an unreachable service must not observe as zero")
	}
}

func TestNonLoopbackEndpointIsRefused(t *testing.T) {
	for _, endpoint := range []string{"http://198.18.0.4:9090/metrics", "https://127.0.0.1:9090/metrics", "not a url"} {
		if _, err := NewScrapeProducer(&http.Client{}, endpoint, "farm_silent_drop_total", nil); err == nil {
			t.Fatalf("endpoint %q must be refused", endpoint)
		}
	}
}

// The scraped gate has to reach the exposition under its contract name, not
// under whatever the service happens to call it.
func TestScrapedGateIsServedUnderTheContractName(t *testing.T) {
	server := serviceExposing(t, `# TYPE farm_change_version_gap_total counter
farm_change_version_gap_total 4
`)
	producers := allZero()
	producers["farm_version_gap_total"] = producerFor(t, server.URL, "farm_change_version_gap_total", nil)
	exporter, err := New(producers, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	exposed := exposition(t, exporter)
	if !strings.Contains(exposed, "farm_version_gap_total 4") {
		t.Fatalf("contract name missing from exposition:\n%s", exposed)
	}
	if strings.Contains(exposed, "farm_change_version_gap_total") {
		t.Fatalf("service-internal name leaked into the contract exposition:\n%s", exposed)
	}
}
