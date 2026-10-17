package capdriver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testSHA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

type completeAdapter struct{}

func (completeAdapter) Execute(context.Context, OperationRequest) Result {
	return Result{Status: StatusCompleted, QueueDepth: 0, Correctness: map[string]int{"duplicate_effect": 0}}
}

func TestBlockedServerPublishesIdentityAndRejectsOperations(t *testing.T) {
	server, err := NewServer(Config{
		BinarySHA256: testSHA, ConfigSHA256: testSHA, Service: ServicePlayer,
		Profile: "l2-player-r2", MaximumInflight: 17, Ready: false, Blocker: "DESIGN_BLOCKER",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	healthRequest := httptest.NewRequest(http.MethodGet, "/health", nil)
	healthResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(healthResponse, healthRequest)
	if healthResponse.Code != http.StatusOK {
		t.Fatalf("health status=%d", healthResponse.Code)
	}
	var health Health
	if err := json.Unmarshal(healthResponse.Body.Bytes(), &health); err != nil {
		t.Fatal(err)
	}
	if health.Ready || health.Blocker != "DESIGN_BLOCKER" || health.Service != ServicePlayer || health.MaximumInflight != 17 {
		t.Fatalf("unexpected health: %+v", health)
	}

	operation := `{"schema_version":"cap-l2-driver-operation.v1","sequence":1,"scheduled_at":"` + time.Now().UTC().Format(time.RFC3339Nano) + `","service":"player","profile":"l2-player-r2","phase":"SMOKE"}`
	request := httptest.NewRequest(http.MethodPost, "/operation", strings.NewReader(operation))
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("operation status=%d body=%s", response.Code, response.Body.String())
	}
	var result OperationResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Sequence != 1 || result.ErrorClass != "DESIGN_BLOCKER" || result.Status != StatusFailed {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestReadyAdapterCompletesWithoutRawIdentity(t *testing.T) {
	server, err := NewServer(Config{
		BinarySHA256: testSHA, ConfigSHA256: testSHA, Service: ServiceGate,
		Profile: "gate-hold", MaximumInflight: 1, Ready: true,
	}, completeAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	operation := `{"schema_version":"cap-l2-driver-operation.v1","sequence":9,"scheduled_at":"` + time.Now().UTC().Format(time.RFC3339Nano) + `","service":"gate","profile":"gate-hold","phase":"LOAD_GENERATOR_QUALIFICATION"}`
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/operation", strings.NewReader(operation)))
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "operation_id") || strings.Contains(response.Body.String(), "session_id") {
		t.Fatalf("raw identity leaked: %s", response.Body.String())
	}
	var result OperationResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Status != StatusCompleted || result.Correctness["duplicate_effect"] != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestOperationRejectsUnknownFieldsAndIdentityDrift(t *testing.T) {
	server, err := NewServer(Config{
		BinarySHA256: testSHA, ConfigSHA256: testSHA, Service: ServiceFarm,
		Profile: "farm", MaximumInflight: 1, Ready: true,
	}, completeAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{
		`{"schema_version":"cap-l2-driver-operation.v1","sequence":1,"scheduled_at":"2026-08-10T00:00:00Z","service":"farm","profile":"farm","phase":"SMOKE","raw_operation_id":"forbidden"}`,
		`{"schema_version":"cap-l2-driver-operation.v1","sequence":1,"scheduled_at":"2026-08-10T00:00:00Z","service":"player","profile":"farm","phase":"SMOKE"}`,
	} {
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/operation", strings.NewReader(body)))
		if response.Code != http.StatusBadRequest {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
	}
}

func TestConfigValidationFailsClosed(t *testing.T) {
	if _, err := NewServer(Config{BinarySHA256: testSHA, ConfigSHA256: testSHA, Service: ServiceWorker, Profile: "worker", MaximumInflight: 1, Ready: true}, nil); err == nil {
		t.Fatal("ready driver accepted without adapter")
	}
	if _, err := NewServer(Config{BinarySHA256: testSHA, ConfigSHA256: testSHA, Service: ServiceWorker, Profile: "worker", MaximumInflight: 1, Blocker: "raw internal error"}, nil); err == nil {
		t.Fatal("unallowlisted blocker accepted")
	}
}
