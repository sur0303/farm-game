package demoorchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"farmgame/internal/demokit"
)

func TestHTTPJSONHealthRunsAndEventsAreReadOnly(t *testing.T) {
	store := NewEventStore(8)
	if _, err := store.Append(demokit.Event{RunID: "run-http", Kind: demokit.KindScenarioStarted, Phase: "START"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(demokit.Event{RunID: "run-http", Kind: demokit.KindClientReady, Player: 7}); err != nil {
		t.Fatal(err)
	}
	handler := NewHTTPHandler(NewLiveEvidence(store))

	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK || !strings.Contains(health.Body.String(), `"mode":"LIVE"`) {
		t.Fatalf("health = %d %s", health.Code, health.Body.String())
	}

	runs := httptest.NewRecorder()
	handler.ServeHTTP(runs, httptest.NewRequest(http.MethodGet, "/api/v1/runs", nil))
	var views []demokit.RunView
	if err := json.Unmarshal(runs.Body.Bytes(), &views); err != nil || len(views) != 1 {
		t.Fatalf("runs = %d %s, err=%v", runs.Code, runs.Body.String(), err)
	}

	events := httptest.NewRecorder()
	handler.ServeHTTP(events, httptest.NewRequest(http.MethodGet, "/api/v1/events?run_id=run-http&after_seq=1", nil))
	var page EventPage
	if err := json.Unmarshal(events.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if events.Code != http.StatusOK || len(page.Events) != 1 || page.Events[0].Kind != demokit.KindClientReady {
		t.Fatalf("events = %d %#v", events.Code, page)
	}

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(method, "/api/v1/events", nil))
		if response.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s /api/v1/events = %d, want 405", method, response.Code)
		}
	}
	unknown := httptest.NewRecorder()
	handler.ServeHTTP(unknown, httptest.NewRequest(http.MethodGet, "/api/v1/execute", nil))
	if unknown.Code != http.StatusNotFound {
		t.Fatalf("unknown command route = %d, want 404", unknown.Code)
	}
}

func TestHTTPReplaySSECompletesAndUsesResumeCursor(t *testing.T) {
	bundle := filepath.Join(t.TempDir(), "replay")
	if err := os.MkdirAll(bundle, 0o755); err != nil {
		t.Fatal(err)
	}
	body := "{\"ts_ms\":10,\"run_id\":\"run-sse\",\"kind\":\"scenario.started\"}\n" +
		"{\"ts_ms\":11,\"run_id\":\"run-sse\",\"kind\":\"client.ready\"}\n"
	if err := os.WriteFile(filepath.Join(bundle, "events.ndjson"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	replay, err := ReadReplay(bundle)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHTTPHandler(replay)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/runs/run-sse/events?after_seq=1", nil)
	request.Header.Set("Accept", "text/event-stream")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("SSE status = %d", response.Code)
	}
	stream := response.Body.String()
	if strings.Contains(stream, `"kind":"scenario.started"`) || !strings.Contains(stream, `"kind":"client.ready"`) {
		t.Fatalf("SSE resume body = %s", stream)
	}
	if strings.Contains(stream, "event:") || !strings.Contains(stream, "id: 2\n") {
		t.Fatalf("SSE framing = %s", stream)
	}
}

func TestHTTPAcceptsLastEventIDForSSEResume(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("Last-Event-ID", "42")
	cursor, err := parseCursor(request)
	if err != nil || cursor != 42 {
		t.Fatalf("parseCursor() = %d, %v; want 42", cursor, err)
	}
	request = httptest.NewRequest(http.MethodGet, "/api/v1/events?after_seq=9", nil)
	cursor, err = parseCursor(request)
	if err != nil || cursor != 9 {
		t.Fatalf("parseCursor(after_seq) = %d, %v; want 9", cursor, err)
	}
}

func TestHTTPReplaySSESendsCompleteBoundedInitialWindow(t *testing.T) {
	bundle := filepath.Join(t.TempDir(), "replay-many")
	if err := os.MkdirAll(bundle, 0o755); err != nil {
		t.Fatal(err)
	}
	const eventCount = DefaultPageSize + 50
	events := make([]demokit.Event, 0, eventCount)
	for index := 0; index < eventCount; index++ {
		events = append(events, demokit.Event{
			TsMs:  int64(index + 1),
			RunID: "run-sse-many",
			Kind:  demokit.KindStepCompleted,
		})
	}
	writeReplayEvents(t, filepath.Join(bundle, "events.ndjson"), events)
	replay, err := NewReplayEvidence(bundle, eventCount)
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/runs/run-sse-many/events",
		nil,
	)
	request.Header.Set("Accept", "text/event-stream")
	response := httptest.NewRecorder()
	NewHTTPHandler(replay).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("SSE status = %d, body=%s", response.Code, response.Body.String())
	}
	stream := response.Body.String()
	if got := strings.Count(stream, "data: "); got != eventCount {
		t.Fatalf("SSE event count = %d, want %d", got, eventCount)
	}
	if !strings.Contains(stream, "id: 250") {
		t.Fatalf("SSE omitted final bounded event: %s", stream)
	}
}
