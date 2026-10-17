package demoorchestrator

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"farmgame/internal/demokit"
)

// HTTPServer exposes only read endpoints for the evidence projection.
type HTTPServer struct {
	source EvidenceReader
}

// NewHTTPServer constructs a read-only controller. A nil source produces an
// empty bounded LIVE source, which is useful for local health checks and tests.
func NewHTTPServer(source EvidenceReader) *HTTPServer {
	if source == nil {
		source = NewLiveEvidence(nil)
	}
	return &HTTPServer{source: source}
}

// NewHTTPHandler is a convenience constructor for callers that only need an
// http.Handler.
func NewHTTPHandler(source EvidenceReader) http.Handler {
	return NewHTTPServer(source).Handler()
}

// Handler returns the complete read-only route set. Method-specific patterns
// make POST/PUT/DELETE requests fail with 405; no command or mutation route is
// registered.
func (server *HTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.health)
	mux.HandleFunc("GET /readyz", server.health)
	mux.HandleFunc("GET /api/v1/runs", server.runs)
	mux.HandleFunc("GET /api/v1/events", server.events)
	mux.HandleFunc("GET /api/v1/runs/current", server.currentRun)
	mux.HandleFunc("GET /api/v1/evidence/current", server.currentEvidence)
	mux.HandleFunc("GET /api/v1/runs/{runID}", server.run)
	mux.HandleFunc("GET /api/v1/runs/{runID}/events", server.runEvents)
	return mux
}

func (server *HTTPServer) health(writer http.ResponseWriter, _ *http.Request) {
	stats := server.source.Stats()
	writeJSON(writer, http.StatusOK, map[string]any{
		"status": "ok",
		"mode":   server.source.Mode(),
		"store":  stats,
	})
}

func (server *HTTPServer) runs(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, server.source.RunViews())
}

func (server *HTTPServer) currentRun(writer http.ResponseWriter, _ *http.Request) {
	view, ok := currentRun(server.source)
	if !ok {
		writeError(writer, http.StatusNotFound, ErrRunNotFound.Error())
		return
	}
	writeJSON(writer, http.StatusOK, view)
}

func (server *HTTPServer) currentEvidence(writer http.ResponseWriter, _ *http.Request) {
	snapshot, ok := currentEvidence(server.source)
	if !ok {
		writeError(writer, http.StatusNotFound, ErrRunNotFound.Error())
		return
	}
	writeJSON(writer, http.StatusOK, snapshot)
}

func currentRun(source EvidenceReader) (demokit.RunView, bool) {
	if reader, ok := source.(CurrentRunReader); ok {
		return reader.CurrentRunView()
	}
	views := source.RunViews()
	if len(views) == 0 {
		return demokit.RunView{}, false
	}
	selected := views[0]
	for _, view := range views[1:] {
		if newerRun(view, selected) {
			selected = view
		}
	}
	return selected, true
}

func currentEvidence(source EvidenceReader) (EvidenceSnapshot, bool) {
	if reader, ok := source.(CurrentEvidenceReader); ok {
		return reader.CurrentEvidence()
	}
	view, ok := currentRun(source)
	if !ok {
		return EvidenceSnapshot{}, false
	}
	page := source.Page(view.RunID, 0, MaxPageSize)
	return EvidenceSnapshot{
		Mode:               source.Mode(),
		RunID:              view.RunID,
		Run:                view,
		Events:             page.Events,
		EventCount:         view.EventCount,
		RetainedEventCount: view.RetainedEventCount,
		OldestSeq:          page.OldestSeq,
		LatestSeq:          page.LatestSeq,
		HasMore:            page.HasMore,
		Gap:                page.Gap,
	}, true
}
func (server *HTTPServer) run(writer http.ResponseWriter, request *http.Request) {
	runID := request.PathValue("runID")
	view, ok := server.source.RunView(runID)
	if !ok {
		writeError(writer, http.StatusNotFound, ErrRunNotFound.Error())
		return
	}
	writeJSON(writer, http.StatusOK, view)
}

func (server *HTTPServer) runEvents(writer http.ResponseWriter, request *http.Request) {
	runID := request.PathValue("runID")
	if _, ok := server.source.RunView(runID); !ok {
		writeError(writer, http.StatusNotFound, ErrRunNotFound.Error())
		return
	}
	server.serveEvents(writer, request, runID)
}

func (server *HTTPServer) events(writer http.ResponseWriter, request *http.Request) {
	runID := strings.TrimSpace(request.URL.Query().Get("run_id"))
	if runID != "" {
		if _, ok := server.source.RunView(runID); !ok {
			writeError(writer, http.StatusNotFound, ErrRunNotFound.Error())
			return
		}
	}
	server.serveEvents(writer, request, runID)
}

func (server *HTTPServer) serveEvents(writer http.ResponseWriter, request *http.Request, runID string) {
	after, err := parseCursor(request)
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	if wantsSSE(request) {
		server.serveSSE(writer, request, runID, after)
		return
	}
	limit, err := parseLimit(request.URL.Query().Get("limit"))
	if err != nil {
		writeError(writer, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, server.source.Page(runID, after, limit))
}

func parseCursor(request *http.Request) (uint64, error) {
	value := strings.TrimSpace(request.URL.Query().Get("after"))
	if value == "" {
		value = strings.TrimSpace(request.URL.Query().Get("since"))
	}
	if value == "" {
		value = strings.TrimSpace(request.URL.Query().Get("after_seq"))
	}
	if value == "" && wantsSSE(request) {
		value = strings.TrimSpace(request.Header.Get("Last-Event-ID"))
	}
	if value == "" {
		return 0, nil
	}
	cursor, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("after must be an unsigned event sequence: %q", value)
	}
	return cursor, nil
}

func parseLimit(value string) (int, error) {
	if strings.TrimSpace(value) == "" {
		return DefaultPageSize, nil
	}
	limit, err := strconv.Atoi(value)
	if err != nil || limit <= 0 {
		return 0, errors.New("limit must be a positive integer")
	}
	if limit > MaxPageSize {
		return MaxPageSize, nil
	}
	return limit, nil
}

func wantsSSE(request *http.Request) bool {
	if request.URL.Query().Get("stream") == "1" || strings.EqualFold(request.URL.Query().Get("format"), "sse") {
		return true
	}
	return strings.Contains(strings.ToLower(request.Header.Get("Accept")), "text/event-stream")
}

func (server *HTTPServer) serveSSE(writer http.ResponseWriter, request *http.Request, runID string, after uint64) {
	flusher, ok := writer.(http.Flusher)
	if !ok {
		writeError(writer, http.StatusInternalServerError, "streaming is not supported by this response writer")
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "keep-alive")
	writer.Header().Set("X-Accel-Buffering", "no")
	writer.WriteHeader(http.StatusOK)
	flusher.Flush()

	subscription := server.source.Subscribe(runID, after)
	defer subscription.Close()
	// Initial is already bounded by the store capacity. Send it in full so the
	// stream never skips the suffix between the cursor and live delivery.
	for _, event := range subscription.Initial {
		if err := writeSSE(writer, event); err != nil {
			return
		}
		flusher.Flush()
	}
	if server.source.Mode() == demokit.EvidenceModeReplay {
		return
	}

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case event, open := <-subscription.Events:
			if !open {
				return
			}
			if err := writeSSE(writer, event); err != nil {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			if _, err := io.WriteString(writer, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case <-request.Context().Done():
			return
		}
	}
}

func writeSSE(writer io.Writer, event demokit.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	if event.Seq > 0 {
		if _, err := fmt.Fprintf(writer, "id: %d\n", event.Seq); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(writer, "data: %s\n\n", payload)
	return err
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, map[string]string{"error": message})
}
