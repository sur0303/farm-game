// Package demoorchestrator contains the read-only evidence projection used by
// the demo release cockpit. It stores and serves observations only; it has no
// fault injector, process runner, shell, or arbitrary command endpoint.
package demoorchestrator

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"farmgame/internal/demokit"
)

const (
	// DefaultEventCapacity bounds the in-memory LIVE window when callers do not
	// provide an explicit capacity.
	DefaultEventCapacity = 1024
	// MaxEventCapacity is the hard in-memory retention ceiling. Checked
	// constructors reject larger values; compatibility constructors clamp them.
	MaxEventCapacity     = 100_000
	DefaultPageSize      = 200
	MaxPageSize          = 1000
	subscriberBufferSize = 64
)

var (
	ErrRunNotFound           = errors.New("run not found")
	ErrInvalidMode           = errors.New("invalid evidence mode")
	ErrEventCapacityExceeded = errors.New("event capacity exceeds maximum")
)

// EventPage is the JSON response for a bounded event read. A gap means that
// the requested cursor predates the retained window; clients should surface
// that fact and restart from OldestSeq rather than silently showing a partial
// history.
type EventPage struct {
	RunID     string          `json:"run_id,omitempty"`
	Events    []demokit.Event `json:"events"`
	NextSeq   uint64          `json:"next_seq,omitempty"`
	OldestSeq uint64          `json:"oldest_seq,omitempty"`
	LatestSeq uint64          `json:"latest_seq,omitempty"`
	HasMore   bool            `json:"has_more,omitempty"`
	Gap       bool            `json:"gap,omitempty"`
}

// Subscription contains the race-free replay prefix and the live suffix. The
// caller must call Close when it stops consuming Events.
type Subscription struct {
	Initial []demokit.Event
	Events  <-chan demokit.Event
	close   func()
}

// Close unregisters the subscription. It is safe to call more than once.
func (subscription Subscription) Close() {
	if subscription.close != nil {
		subscription.close()
	}
}

type eventSubscriber struct {
	id    uint64
	runID string
	ch    chan demokit.Event
}

type runState struct {
	view    demokit.RunView
	count   uint64
	dropped uint64
}

// StoreStats is a small read-only health summary for the HTTP health checks.
type StoreStats struct {
	Capacity       int    `json:"capacity"`
	RetainedEvents int    `json:"retained_events"`
	TotalEvents    uint64 `json:"total_events"`
	DroppedEvents  uint64 `json:"dropped_events"`
	Runs           int    `json:"runs"`
	Subscribers    int    `json:"subscribers"`
}

// EventStore is a concurrency-safe append-only ring buffer. Events are never
// updated or removed individually; once capacity is reached, only the oldest
// retained event is evicted. HTTP handlers expose no method that appends.
type EventStore struct {
	mu          sync.RWMutex
	capacity    int
	mode        demokit.EvidenceMode
	nextSeq     uint64
	events      []demokit.Event
	runs        map[string]*runState
	subscribers map[uint64]*eventSubscriber
	nextSubID   uint64
}

// NewEventStore creates a LIVE bounded store. A non-positive capacity selects
// DefaultEventCapacity; a capacity above MaxEventCapacity is safely clamped.
func NewEventStore(capacity int) *EventStore {
	return newEventStore(capacity, demokit.EvidenceModeLive)
}

// NewBoundedEventStore is an explicit-name alias for NewEventStore.
func NewBoundedEventStore(capacity int) *EventStore {
	return NewEventStore(capacity)
}

// NewEventStoreChecked creates a bounded store and rejects unknown modes or a
// capacity above MaxEventCapacity before allocating the retention buffer.
func NewEventStoreChecked(capacity int, mode demokit.EvidenceMode) (*EventStore, error) {
	if mode != demokit.EvidenceModeLive && mode != demokit.EvidenceModeReplay {
		return nil, fmt.Errorf("%w: %q", ErrInvalidMode, mode)
	}
	if capacity > MaxEventCapacity {
		return nil, fmt.Errorf("%w: got %d, maximum %d", ErrEventCapacityExceeded, capacity, MaxEventCapacity)
	}
	return newEventStore(capacity, mode), nil
}

// NewEventStoreWithMode is retained for source compatibility. New controller
// code must use NewEventStoreChecked so invalid external configuration cannot
// silently select LIVE.
func NewEventStoreWithMode(capacity int, mode demokit.EvidenceMode) *EventStore {
	normalizedCapacity := normalizeEventCapacity(capacity)
	store, err := NewEventStoreChecked(normalizedCapacity, mode)
	if err != nil {
		return newEventStore(normalizedCapacity, demokit.EvidenceModeLive)
	}
	return store
}

func newEventStore(capacity int, mode demokit.EvidenceMode) *EventStore {
	capacity = normalizeEventCapacity(capacity)
	return &EventStore{
		capacity:    capacity,
		mode:        mode,
		events:      make([]demokit.Event, 0, capacity),
		runs:        make(map[string]*runState),
		subscribers: make(map[uint64]*eventSubscriber),
	}
}

func normalizeEventCapacity(capacity int) int {
	if capacity <= 0 {
		return DefaultEventCapacity
	}
	if capacity > MaxEventCapacity {
		return MaxEventCapacity
	}
	return capacity
}

// Mode returns the source mode represented by this store.
func (store *EventStore) Mode() demokit.EvidenceMode {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.mode
}

// Capacity returns the fixed event retention bound.
func (store *EventStore) Capacity() int {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.capacity
}

// RegisterRun seeds metadata for a run before events arrive. Replay readers use
// it for manifest-only plan bundles; LIVE producers may use it to associate
// scenario metadata without adding a synthetic event.
func (store *EventStore) RegisterRun(view demokit.RunView) error {
	view.RunID = strings.TrimSpace(view.RunID)
	if view.RunID == "" {
		return errors.New("run_id is required")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if view.Mode != demokit.EvidenceModeLive && view.Mode != demokit.EvidenceModeReplay {
		view.Mode = store.mode
	}
	if view.Status == "" {
		view.Status = "read_only"
	}
	state, ok := store.runs[view.RunID]
	if !ok {
		store.runs[view.RunID] = &runState{view: view}
		return nil
	}
	mergeRunMetadata(&state.view, view)
	return nil
}

func mergeRunMetadata(destination *demokit.RunView, source demokit.RunView) {
	if source.ScenarioID != "" {
		destination.ScenarioID = source.ScenarioID
	}
	if source.ScenarioTitle != "" {
		destination.ScenarioTitle = source.ScenarioTitle
	}
	if source.SourceRef != "" {
		destination.SourceRef = source.SourceRef
	}
	if source.Execution != "" {
		destination.Execution = source.Execution
	}
	if source.Status != "" {
		destination.Status = source.Status
	}
	if source.Phase != "" {
		destination.Phase = source.Phase
	}
	if source.StartedAtMs != 0 {
		destination.StartedAtMs = source.StartedAtMs
	}
	if source.LastEventAtMs != 0 {
		destination.LastEventAtMs = source.LastEventAtMs
	}
	if source.Mode == demokit.EvidenceModeLive || source.Mode == demokit.EvidenceModeReplay {
		destination.Mode = source.Mode
	}
}

// Append validates and appends one event, assigning a monotonic store cursor.
// The supplied Seq is ignored so callers cannot rewrite cursor history.
func (store *EventStore) Append(event demokit.Event) (demokit.Event, error) {
	event.RunID = strings.TrimSpace(event.RunID)
	event.Kind = demokit.EventKind(strings.TrimSpace(string(event.Kind)))
	if event.TsMs == 0 {
		event.TsMs = time.Now().UnixMilli()
	}
	if err := event.Validate(); err != nil {
		return demokit.Event{}, err
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.nextSeq == ^uint64(0) {
		return demokit.Event{}, errors.New("event sequence exhausted")
	}
	store.nextSeq++
	event.Seq = store.nextSeq
	event = demokit.CloneEvent(event)

	state, ok := store.runs[event.RunID]
	if !ok {
		state = &runState{view: demokit.RunView{
			RunID:  event.RunID,
			Mode:   store.mode,
			Status: "read_only",
		}}
		store.runs[event.RunID] = state
	}
	state.count++
	if state.view.StartedAtMs == 0 || event.TsMs < state.view.StartedAtMs {
		state.view.StartedAtMs = event.TsMs
	}
	if event.TsMs > state.view.LastEventAtMs {
		state.view.LastEventAtMs = event.TsMs
	}
	if event.Phase != "" {
		state.view.Phase = event.Phase
	}
	if event.Version > state.view.HeadVersion {
		state.view.HeadVersion = event.Version
	}
	store.events = append(store.events, event)
	if len(store.events) > store.capacity {
		evicted := store.events[0]
		store.events = store.events[1:]
		if evictedState, exists := store.runs[evicted.RunID]; exists {
			evictedState.dropped++
		}
	}
	store.broadcastLocked(event)
	return demokit.CloneEvent(event), nil
}

func (store *EventStore) broadcastLocked(event demokit.Event) {
	for id, subscriber := range store.subscribers {
		if subscriber.runID != "" && subscriber.runID != event.RunID {
			continue
		}
		select {
		case subscriber.ch <- demokit.CloneEvent(event):
		default:
			// Do not block an append on a slow browser. Closing the stream makes
			// the loss explicit; Last-Event-ID can recover from the retained ring.
			delete(store.subscribers, id)
			close(subscriber.ch)
		}
	}
}

// Page returns retained events strictly after afterSeq. An empty runID reads
// the global stream; a non-empty runID reads only that run while retaining the
// global sequence cursor for reconnects.
func (store *EventStore) Page(runID string, afterSeq uint64, limit int) EventPage {
	runID = strings.TrimSpace(runID)
	limit = normalizeLimit(limit)
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.pageLocked(runID, afterSeq, limit)
}

// Events is a descriptive alias for Page.
func (store *EventStore) Events(runID string, afterSeq uint64, limit int) EventPage {
	return store.Page(runID, afterSeq, limit)
}

func normalizeLimit(limit int) int {
	if limit <= 0 {
		return DefaultPageSize
	}
	if limit > MaxPageSize {
		return MaxPageSize
	}
	return limit
}

func (store *EventStore) pageLocked(runID string, afterSeq uint64, limit int) EventPage {
	page := EventPage{RunID: runID, Events: make([]demokit.Event, 0)}
	var firstSeq, latestSeq uint64
	if runID == "" {
		if len(store.events) > 0 {
			firstSeq = store.events[0].Seq
			latestSeq = store.events[len(store.events)-1].Seq
		}
	} else if _, ok := store.runs[runID]; ok {
		for _, event := range store.events {
			if event.RunID != runID {
				continue
			}
			if firstSeq == 0 {
				firstSeq = event.Seq
			}
			latestSeq = event.Seq
		}
	}
	page.OldestSeq = firstSeq
	page.LatestSeq = latestSeq
	if firstSeq != 0 {
		if afterSeq == 0 {
			page.Gap = store.hasDroppedBeforeLocked(runID)
		} else {
			page.Gap = afterSeq+1 < firstSeq
		}
	}
	for _, event := range store.events {
		if runID != "" && event.RunID != runID {
			continue
		}
		if event.Seq <= afterSeq {
			continue
		}
		if len(page.Events) == limit {
			page.HasMore = true
			break
		}
		page.Events = append(page.Events, demokit.CloneEvent(event))
	}
	if len(page.Events) > 0 {
		page.NextSeq = page.Events[len(page.Events)-1].Seq
	} else {
		page.NextSeq = afterSeq
	}
	return page
}

func (store *EventStore) hasDroppedBeforeLocked(runID string) bool {
	if runID == "" {
		return store.nextSeq > uint64(len(store.events))
	}
	state, ok := store.runs[runID]
	return ok && state.dropped > 0
}

// All returns a defensive copy of every currently retained event.
func (store *EventStore) All() []demokit.Event {
	store.mu.RLock()
	defer store.mu.RUnlock()
	events := make([]demokit.Event, len(store.events))
	for index, event := range store.events {
		events[index] = demokit.CloneEvent(event)
	}
	return events
}

// RunView returns a current read model for one registered or observed run.
func (store *EventStore) RunView(runID string) (demokit.RunView, bool) {
	runID = strings.TrimSpace(runID)
	store.mu.RLock()
	defer store.mu.RUnlock()
	state, ok := store.runs[runID]
	if !ok {
		return demokit.RunView{}, false
	}
	return store.viewLocked(runID, state), true
}

func (store *EventStore) viewLocked(runID string, state *runState) demokit.RunView {
	view := state.view
	view.RunID = runID
	view.Mode = store.mode
	view.EventCount = state.count
	view.DroppedEventCount = state.dropped
	view.RetainedEventCount = 0
	view.OldestSeq = 0
	view.LatestSeq = 0
	for _, event := range store.events {
		if event.RunID != runID {
			continue
		}
		view.RetainedEventCount++
		if view.OldestSeq == 0 {
			view.OldestSeq = event.Seq
		}
		view.LatestSeq = event.Seq
	}
	if view.Status == "" {
		view.Status = "read_only"
	}
	return view
}

// RunViews returns deterministic read models for all registered or observed
// runs. No event or metadata is mutated while producing the result.
func (store *EventStore) RunViews() []demokit.RunView {
	store.mu.RLock()
	defer store.mu.RUnlock()
	ids := make([]string, 0, len(store.runs))
	for runID := range store.runs {
		ids = append(ids, runID)
	}
	sort.Strings(ids)
	views := make([]demokit.RunView, 0, len(ids))
	for _, runID := range ids {
		views = append(views, store.viewLocked(runID, store.runs[runID]))
	}
	return views
}

// Stats returns a read-only health snapshot.
func (store *EventStore) Stats() StoreStats {
	store.mu.RLock()
	defer store.mu.RUnlock()
	var total, dropped uint64
	for _, state := range store.runs {
		total += state.count
		dropped += state.dropped
	}
	return StoreStats{
		Capacity:       store.capacity,
		RetainedEvents: len(store.events),
		TotalEvents:    total,
		DroppedEvents:  dropped,
		Runs:           len(store.runs),
		Subscribers:    len(store.subscribers),
	}
}

// Subscribe snapshots retained events and registers the live suffix while
// holding one lock, eliminating the usual snapshot/subscribe race.
func (store *EventStore) Subscribe(runID string, afterSeq uint64) Subscription {
	runID = strings.TrimSpace(runID)
	store.mu.Lock()
	defer store.mu.Unlock()
	initial := store.pageLocked(runID, afterSeq, store.capacity).Events
	store.nextSubID++
	id := store.nextSubID
	subscriber := &eventSubscriber{id: id, runID: runID, ch: make(chan demokit.Event, subscriberBufferSize)}
	store.subscribers[id] = subscriber
	return Subscription{
		Initial: initial,
		Events:  subscriber.ch,
		close:   func() { store.unsubscribe(id) },
	}
}

func (store *EventStore) unsubscribe(id uint64) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if subscriber, ok := store.subscribers[id]; ok {
		delete(store.subscribers, id)
		close(subscriber.ch)
	}
}
