package gatedriver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"farmgame/internal/testkit/capdriver"
)

// The load generator has to report its own protocol work and backlog, or a run
// cannot tell a saturated service from a load generator that stopped keeping
// up. The stub answers each session with exactly one ServerHello, so the frame
// counts are exact rather than approximate.
func TestDriverPublishesLoadGeneratorWork(t *testing.T) {
	stub := newGateStub(t)
	adapter, err := New(stub.config())
	if err != nil {
		t.Fatal(err)
	}
	driver, err := capdriver.NewServer(capdriver.Config{
		BinarySHA256: strings.Repeat("a", 64), ConfigSHA256: strings.Repeat("b", 64),
		Service: capdriver.ServiceGate, Profile: "gate-qualification",
		MaximumInflight: 4, Ready: true,
	}, adapter)
	if err != nil {
		t.Fatal(err)
	}
	handler := driver.Handler()

	const sessions = 3
	for sequence := uint64(1); sequence <= sessions; sequence++ {
		body, err := json.Marshal(capdriver.OperationRequest{
			SchemaVersion: capdriver.OperationSchema, Sequence: sequence,
			ScheduledAt: time.Now().Format(time.RFC3339Nano),
			Service:     capdriver.ServiceGate, Profile: "gate-qualification",
			Phase: "LOAD_GENERATOR_QUALIFICATION",
		})
		if err != nil {
			t.Fatal(err)
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/operation", strings.NewReader(string(body))))
		if recorder.Code != http.StatusOK {
			t.Fatalf("sequence %d: code=%d body=%s", sequence, recorder.Code, recorder.Body.String())
		}
	}

	scraped := httptest.NewRecorder()
	handler.ServeHTTP(scraped, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if scraped.Code != http.StatusOK {
		t.Fatalf("metrics code=%d", scraped.Code)
	}
	exposed := scraped.Body.String()
	for name, want := range map[string]string{
		"driver_frame_encode_total": "3",
		"driver_frame_decode_total": "3",
		"driver_event_backlog":      "0",
		"driver_socket_error_total": "0",
	} {
		if line := metricLine(exposed, name); line != name+" "+want {
			t.Fatalf("%s: got %q want %q", name, line, name+" "+want)
		}
	}
}

func metricLine(exposed string, name string) string {
	for _, line := range strings.Split(exposed, "\n") {
		if strings.HasPrefix(line, name+" ") {
			return line
		}
	}
	return ""
}
