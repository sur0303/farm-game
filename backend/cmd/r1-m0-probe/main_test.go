package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestClassifyEndpointDoesNotCallLoopbackStaging(t *testing.T) {
	endpoint, class, official, err := classifyEndpoint("http://127.0.0.1:18500/metrics?secret=removed")
	if err != nil {
		t.Fatal(err)
	}
	if endpoint != "http://127.0.0.1:18500/metrics" || class != "localhost" || official {
		t.Fatalf("loopback classification = %q %q %v", endpoint, class, official)
	}
}

func TestProbeIsGETOnlyAndReportsBytesAndMetrics(t *testing.T) {
	var method string
	body := "# TYPE farm_gate_subscription_lease_renew_entries_total counter\n" +
		"farm_gate_subscription_lease_renew_entries_total 7\n" +
		"farm_gate_subscription_lease_renew_duration_seconds_count 2\n"
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		method = request.Method
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header), Request: request}, nil
	})
	result := probeWithClient(context.Background(), "http://127.0.0.1:18500/metrics", "test-run", time.Second, transport)
	if result.Status != "OBSERVED_LOCAL_ONLY" || method != http.MethodGet || !result.ReadOnly {
		t.Fatalf("result=%+v method=%q", result, method)
	}
	if result.ResponseBytes != len(body) {
		t.Fatalf("response bytes=%d", result.ResponseBytes)
	}
	entries, ok := result.Metrics["farm_gate_subscription_lease_renew_entries_total"]
	if !ok || entries.ValuesSum != 7 || entries.SampleCount != 1 {
		t.Fatalf("entries metric=%+v", entries)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestProbeRejectsUserInfoAndNonHTTP(t *testing.T) {
	for _, endpoint := range []string{"redis://127.0.0.1:6379/0", "http://user:pass@example.test/metrics"} {
		result := probe(context.Background(), endpoint, "test-run", time.Second)
		if result.Status != "FAIL" || !strings.Contains(result.Error, "metrics URL") {
			t.Fatalf("endpoint=%q result=%+v", endpoint, result)
		}
	}
}
