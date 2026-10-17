package demoorchestrator

import "farmgame/internal/demokit"

// EvidenceSnapshot is the bounded, read-only aggregate consumed by the
// cockpit's initial load. Events are one bounded current-run page. Gap means
// older events were evicted; HasMore means additional retained events require
// cursor pagination. Neither state may be presented as a complete history.
type EvidenceSnapshot struct {
	Mode               demokit.EvidenceMode `json:"mode"`
	RunID              string               `json:"run_id"`
	Run                demokit.RunView      `json:"run"`
	Events             []demokit.Event      `json:"events"`
	EventCount         uint64               `json:"event_count"`
	RetainedEventCount uint64               `json:"retained_event_count"`
	OldestSeq          uint64               `json:"oldest_seq,omitempty"`
	LatestSeq          uint64               `json:"latest_seq,omitempty"`
	HasMore            bool                 `json:"has_more,omitempty"`
	Gap                bool                 `json:"gap,omitempty"`
}

// CurrentRunReader provides deterministic current-run selection without
// exposing any mutation method to the HTTP layer.
type CurrentRunReader interface {
	CurrentRunView() (demokit.RunView, bool)
}

// CurrentEvidenceReader provides the initial aggregate used by the cockpit.
type CurrentEvidenceReader interface {
	CurrentEvidence() (EvidenceSnapshot, bool)
}

// CurrentRunView selects the run with the newest retained sequence. For runs
// that have no events yet, registration time metadata is used as a fallback.
func (store *EventStore) CurrentRunView() (demokit.RunView, bool) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.currentRunViewLocked()
}

func (store *EventStore) currentRunViewLocked() (demokit.RunView, bool) {
	var selected demokit.RunView
	selectedID := ""
	for runID, state := range store.runs {
		view := store.viewLocked(runID, state)
		if selectedID == "" || newerRun(view, selected) {
			selectedID = runID
			selected = view
		}
	}
	return selected, selectedID != ""
}

func newerRun(candidate, selected demokit.RunView) bool {
	if candidate.LatestSeq != selected.LatestSeq {
		return candidate.LatestSeq > selected.LatestSeq
	}
	if candidate.LastEventAtMs != selected.LastEventAtMs {
		return candidate.LastEventAtMs > selected.LastEventAtMs
	}
	if candidate.StartedAtMs != selected.StartedAtMs {
		return candidate.StartedAtMs > selected.StartedAtMs
	}
	return candidate.RunID > selected.RunID
}

func snapshotForStore(store *EventStore) (EvidenceSnapshot, bool) {
	view, ok := store.CurrentRunView()
	if !ok {
		return EvidenceSnapshot{}, false
	}
	page := store.Page(view.RunID, 0, store.Capacity())
	return EvidenceSnapshot{
		Mode:               store.Mode(),
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

// CurrentRunView exposes current selection for LIVE callers.
func (evidence *LiveEvidence) CurrentRunView() (demokit.RunView, bool) {
	return evidence.store.CurrentRunView()
}

// CurrentEvidence returns the retained LIVE aggregate.
func (evidence *LiveEvidence) CurrentEvidence() (EvidenceSnapshot, bool) {
	return snapshotForStore(evidence.store)
}

// CurrentRunView exposes current selection for REPLAY callers.
func (evidence *ReplayEvidence) CurrentRunView() (demokit.RunView, bool) {
	return evidence.store.CurrentRunView()
}

// CurrentEvidence returns the retained REPLAY aggregate.
func (evidence *ReplayEvidence) CurrentEvidence() (EvidenceSnapshot, bool) {
	return snapshotForStore(evidence.store)
}
