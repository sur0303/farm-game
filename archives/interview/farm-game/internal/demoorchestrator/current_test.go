package demoorchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"farmgame/internal/demokit"
)

func TestCurrentRunAndEvidenceEndpointsSelectNewestRun(t *testing.T) {
	store := NewEventStore(8)
	if _, err := store.Append(demokit.Event{RunID: "run-old", TsMs: 10, Kind: demokit.KindScenarioStarted}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(demokit.Event{RunID: "run-current", TsMs: 20, Kind: demokit.KindScenarioStarted, Phase: "START"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(demokit.Event{RunID: "run-current", TsMs: 21, Kind: demokit.KindClientReady, Player: 7}); err != nil {
		t.Fatal(err)
	}
	handler := NewHTTPHandler(NewLiveEvidence(store))

	runResponse := httptest.NewRecorder()
	handler.ServeHTTP(runResponse, httptest.NewRequest(http.MethodGet, "/api/v1/runs/current", nil))
	if runResponse.Code != http.StatusOK {
		t.Fatalf("current run status = %d, body=%s", runResponse.Code, runResponse.Body.String())
	}
	var view demokit.RunView
	if err := json.Unmarshal(runResponse.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if view.RunID != "run-current" || view.Mode != demokit.EvidenceModeLive {
		t.Fatalf("current run = %#v", view)
	}

	evidenceResponse := httptest.NewRecorder()
	handler.ServeHTTP(evidenceResponse, httptest.NewRequest(http.MethodGet, "/api/v1/evidence/current", nil))
	if evidenceResponse.Code != http.StatusOK {
		t.Fatalf("current evidence status = %d, body=%s", evidenceResponse.Code, evidenceResponse.Body.String())
	}
	var snapshot EvidenceSnapshot
	if err := json.Unmarshal(evidenceResponse.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.RunID != "run-current" || snapshot.Run.RunID != "run-current" || len(snapshot.Events) != 2 {
		t.Fatalf("current evidence = %#v", snapshot)
	}
	if snapshot.Events[0].RunID != "run-current" || snapshot.Events[1].Kind != demokit.KindClientReady {
		t.Fatalf("current evidence events = %#v", snapshot.Events)
	}
}

func TestCurrentEndpointsRemainReadOnlyAndReturnNotFoundWhenEmpty(t *testing.T) {
	handler := NewHTTPHandler(NewLiveEvidence(NewEventStore(4)))
	for _, path := range []string{"/api/v1/runs/current", "/api/v1/evidence/current"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("empty %s status = %d, want 404", path, response.Code)
		}
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
			response = httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(method, path, nil))
			if response.Code != http.StatusMethodNotAllowed {
				t.Fatalf("%s %s status = %d, want 405", method, path, response.Code)
			}
		}
	}
}

func TestCurrentEvidenceIncludesGapMetadataAfterEviction(t *testing.T) {
	store := NewEventStore(2)
	for index := 0; index < 3; index++ {
		if _, err := store.Append(demokit.Event{RunID: "run-gap", Kind: demokit.KindStepCompleted, TsMs: int64(index + 1)}); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, ok := NewLiveEvidence(store).CurrentEvidence()
	if !ok || !snapshot.Gap || snapshot.OldestSeq != 2 || snapshot.LatestSeq != 3 {
		t.Fatalf("current snapshot gap = %#v, found=%v", snapshot, ok)
	}
}

func TestCurrentEvidenceMarksBoundedPageHasMore(t *testing.T) {
	store := NewEventStore(MaxPageSize + 5)
	for index := 0; index < MaxPageSize+5; index++ {
		if _, err := store.Append(demokit.Event{
			RunID: "run-page",
			Kind:  demokit.KindStepCompleted,
			TsMs:  int64(index + 1),
		}); err != nil {
			t.Fatal(err)
		}
	}

	direct := EvidenceReader(NewLiveEvidence(store))
	fallback := EvidenceReader(struct{ EvidenceReader }{EvidenceReader: direct})
	for name, reader := range map[string]EvidenceReader{
		"direct":   direct,
		"fallback": fallback,
	} {
		t.Run(name, func(t *testing.T) {
			snapshot, ok := currentEvidence(reader)
			if !ok {
				t.Fatal("currentEvidence() did not find run")
			}
			if !snapshot.HasMore || snapshot.Gap {
				t.Fatalf("snapshot paging flags = %#v", snapshot)
			}
			if len(snapshot.Events) != MaxPageSize {
				t.Fatalf("snapshot events = %d, want %d", len(snapshot.Events), MaxPageSize)
			}
			if snapshot.RetainedEventCount != MaxPageSize+5 {
				t.Fatalf("retained count = %d, want %d", snapshot.RetainedEventCount, MaxPageSize+5)
			}
		})
	}
}
