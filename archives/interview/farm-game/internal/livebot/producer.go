package livebot

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"farmgame/internal/demokit"
	"farmgame/internal/demoorchestrator"
)

// Producer is an in-process evidence writer. It has no HTTP-facing mutation
// method; callers inject the existing demoorchestrator.EventStore directly.
type Producer struct {
	store *demoorchestrator.EventStore
	now   func() time.Time

	mu        sync.Mutex
	appendErr error
}

// NewProducer wraps an injected LIVE EventStore. A nil store gets the normal
// bounded in-memory default so tests can use the producer without networking.
func NewProducer(store *demoorchestrator.EventStore) *Producer {
	if store == nil {
		store = demoorchestrator.NewEventStore(demoorchestrator.DefaultEventCapacity)
	}
	return &Producer{store: store, now: time.Now}
}

// NewProducerChecked rejects nil or REPLAY stores for a real LIVE run.
func NewProducerChecked(store *demoorchestrator.EventStore) (*Producer, error) {
	if store == nil {
		return nil, errors.New("livebot event store is required")
	}
	if store.Mode() != demokit.EvidenceModeLive {
		return nil, ErrLiveStoreRequired
	}
	return NewProducer(store), nil
}

// Store exposes the injected store to in-process callers; it does not expose
// a network or HTTP write path.
func (producer *Producer) Store() *demoorchestrator.EventStore {
	if producer == nil {
		return nil
	}
	return producer.store
}

// Err returns the first append error observed by the producer.
func (producer *Producer) Err() error {
	if producer == nil {
		return errors.New("nil livebot producer")
	}
	producer.mu.Lock()
	defer producer.mu.Unlock()
	return producer.appendErr
}

// Append is the only low-level producer seam. Fields are sanitized before
// they reach EventStore, including fields supplied by a test or future
// adapter.
func (producer *Producer) Append(event demokit.Event) (demokit.Event, error) {
	if producer == nil || producer.store == nil {
		return demokit.Event{}, errors.New("livebot producer has no event store")
	}
	event.Fields = sanitizeFields(event.Fields)
	producer.mu.Lock()
	defer producer.mu.Unlock()
	appended, err := producer.store.Append(event)
	if err != nil && producer.appendErr == nil {
		producer.appendErr = err
	}
	return appended, err
}

// RegisterRun seeds the LIVE RunView before the first lifecycle event.
func (producer *Producer) RegisterRun(spec LiveRunSpec, startedAt time.Time) error {
	if producer == nil || producer.store == nil {
		return errors.New("livebot producer has no event store")
	}
	normalized, err := spec.normalized()
	if err != nil {
		return err
	}
	if producer.store.Mode() != demokit.EvidenceModeLive {
		return ErrLiveStoreRequired
	}
	if startedAt.IsZero() {
		startedAt = producer.currentTime()
	}
	return producer.store.RegisterRun(demokit.RunView{
		RunID:       normalized.RunID,
		ScenarioID:  normalized.ScenarioID,
		Mode:        demokit.EvidenceModeLive,
		Execution:   ExecutionRealSandbox,
		Status:      string(RunRunning),
		Phase:       "START",
		StartedAtMs: startedAt.UnixMilli(),
	})
}

// RunStarted appends the run-start lifecycle event and registers its view.
func (producer *Producer) RunStarted(spec LiveRunSpec) error {
	return producer.RunStartedAt(spec, time.Time{})
}

// RunStartedAt is RunStarted with an injectable timestamp for deterministic
// tests.
func (producer *Producer) RunStartedAt(spec LiveRunSpec, startedAt time.Time) error {
	normalized, err := spec.normalized()
	if err != nil {
		return err
	}
	if startedAt.IsZero() {
		startedAt = producer.currentTime()
	}
	if err := producer.RegisterRun(normalized, startedAt); err != nil {
		return err
	}
	_, err = producer.Append(demokit.Event{
		RunID:  normalized.RunID,
		TsMs:   startedAt.UnixMilli(),
		Kind:   demokit.KindRunStarted,
		Phase:  "START",
		Fields: runStartFields(normalized),
	})
	if err != nil {
		return err
	}
	_, err = producer.Append(demokit.Event{
		RunID:   normalized.RunID,
		TsMs:    startedAt.UnixMilli(),
		Kind:    demokit.KindScenarioStarted,
		Phase:   "START",
		Message: "live scenario started",
		Fields: map[string]any{
			"scenario": normalized.ScenarioID,
			"step":     "run_started",
			"state":    "started",
		},
	})
	return err
}

// RunFinished appends a terminal run lifecycle event and updates its RunView.
func (producer *Producer) RunFinished(result RunResult) error {
	return producer.RunFinishedAt(result, time.Time{})
}

// RunFinishedAt is RunFinished with an injectable timestamp.
func (producer *Producer) RunFinishedAt(result RunResult, finishedAt time.Time) error {
	if producer == nil || producer.store == nil {
		return errors.New("livebot producer has no event store")
	}
	if finishedAt.IsZero() {
		finishedAt = producer.currentTime()
	}
	kind := demokit.KindRunFailed
	phase := "FAILED"
	switch result.Status {
	case RunSucceeded:
		kind = demokit.KindRunCompleted
		phase = "SUCCEEDED"
	case RunCancelled:
		kind = demokit.KindRunCancelled
		phase = "CANCELLED"
	case RunFailed:
	default:
		result.Status = RunFailed
	}
	if _, err := producer.Append(demokit.Event{
		RunID:  result.RunID,
		TsMs:   finishedAt.UnixMilli(),
		Kind:   kind,
		Phase:  phase,
		Fields: runResultFields(result),
	}); err != nil {
		return err
	}
	if _, err := producer.Append(demokit.Event{
		RunID:   result.RunID,
		TsMs:    finishedAt.UnixMilli(),
		Kind:    demokit.KindStepCompleted,
		Phase:   phase,
		Message: "live run terminal",
		Fields: map[string]any{
			"step":   "run_terminal",
			"status": string(result.Status),
		},
	}); err != nil {
		return err
	}
	return producer.store.RegisterRun(demokit.RunView{
		RunID:         result.RunID,
		Mode:          demokit.EvidenceModeLive,
		Execution:     ExecutionRealSandbox,
		Status:        string(result.Status),
		Phase:         phase,
		LastEventAtMs: finishedAt.UnixMilli(),
	})
}

// Observer returns a per-bot observer bound to spec.
func (producer *Producer) Observer(spec BotSpec) *BotObserver {
	return NewBotObserver(producer, spec)
}

// NewBotObserver binds lifecycle callbacks to one bot. It is exported so an
// adapter can unit-test its callbacks without constructing a Runner.
func NewBotObserver(producer *Producer, spec BotSpec) *BotObserver {
	return &BotObserver{producer: producer, spec: spec}
}

// BotObserver maps one bot's lifecycle into safe demokit events.
type BotObserver struct {
	producer *Producer
	spec     BotSpec

	mu             sync.Mutex
	terminal       bool
	terminalResult BotResult
	ready          bool
	reconnects     int
	lastLatency    time.Duration
	actionLatency  LatencyHistogram
	actionOrdinal  int
	phases         map[string]LatencyHistogram
}

var _ Observer = (*BotObserver)(nil)

// Observe accepts the safe lifecycle vocabulary from an Executor.
func (observer *BotObserver) Observe(observation Observation) {
	if observer == nil {
		return
	}
	switch observation.Kind {
	case demokit.KindBotLogin:
		observer.Login()
	case demokit.KindBotHello:
		observer.Hello()
	case demokit.KindBotJoin:
		observer.Join()
	case demokit.KindBotReady:
		observer.Ready()
	case demokit.KindBotAction:
		latency := observation.Latency
		if latency == 0 {
			if value, ok := observation.Fields["latency_ms"].(int64); ok {
				latency = time.Duration(value) * time.Millisecond
			}
		}
		observer.Action(observation.Action, latency)
	case demokit.KindBotReconnect:
		observer.Reconnect(observation.ReconnectAttempt)
	case demokit.KindBotTerminal:
		observer.Terminal(BotResult{Status: observation.Status})
	}
}

func (observer *BotObserver) emit(kind demokit.EventKind, phase string, fields map[string]any) {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.terminal {
		return
	}
	_, _ = observer.producer.Append(demokit.Event{
		RunID:   observer.spec.RunID,
		Player:  observer.spec.PlayerID,
		Kind:    kind,
		Phase:   phase,
		Message: "bot lifecycle",
		Fields:  fields,
	})
}

// appendScene records a run-scoped scene marker without copying bot identity
// into the scene stream. The existing bot event remains the client-specific
// observation; scene markers stay bounded and aggregate-friendly.
func (observer *BotObserver) appendScene(kind demokit.EventKind, phase, message string, fields map[string]any) {
	if observer == nil {
		return
	}
	_, _ = observer.producer.Append(demokit.Event{
		RunID:   observer.spec.RunID,
		Kind:    kind,
		Phase:   phase,
		Message: message,
		Fields:  fields,
	})
}

func (observer *BotObserver) Login() {
	observer.emit(demokit.KindBotLogin, "JOINING", map[string]any{"state": "joining"})
}

func (observer *BotObserver) OnLogin() { observer.Login() }

func (observer *BotObserver) Hello() {
	observer.emit(demokit.KindBotHello, "JOINING", map[string]any{"state": "joining"})
}

func (observer *BotObserver) OnHello() { observer.Hello() }

func (observer *BotObserver) Join() {
	observer.emit(demokit.KindBotJoin, "JOINING", map[string]any{"state": "joining"})
}

func (observer *BotObserver) OnJoin() { observer.Join() }

func (observer *BotObserver) Ready() {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.terminal {
		return
	}
	observer.ready = true
	_, _ = observer.producer.Append(demokit.Event{
		RunID:   observer.spec.RunID,
		Player:  observer.spec.PlayerID,
		Kind:    demokit.KindBotReady,
		Phase:   "READY",
		Message: "bot lifecycle",
		Fields:  map[string]any{"state": "ready"},
	})
	observer.appendScene(
		demokit.KindScenarioBarrier,
		"READY",
		"bot ready barrier",
		map[string]any{"barrier": "bot_ready", "state": "ready"},
	)
}

func (observer *BotObserver) OnReady() { observer.Ready() }

func (observer *BotObserver) Action(action string, latency ...time.Duration) {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.terminal {
		return
	}
	if strings.TrimSpace(action) == "" {
		action = observer.spec.PickAction(observer.actionOrdinal)
	}
	observer.actionOrdinal++
	action = safeActionName(action)
	var observedLatency time.Duration
	if len(latency) > 0 {
		observedLatency = latency[0]
	}
	if observedLatency > 0 {
		observer.lastLatency = observedLatency
		// Every action is a sample. Keeping only the last one would make a
		// bot that acted a hundred times count once in the run distribution.
		observer.actionLatency.Observe(observedLatency)
	}
	// A sustained run writes no event per action. Both appends below take a
	// process-wide lock, and a run acting tens of thousands of times a second
	// spends more time queueing for it than talking to the server, which
	// makes the driver the thing being measured. The events would not even
	// survive: at that rate they are evicted from the bounded store within
	// seconds. The counters and histograms above are the evidence instead.
	if observer.spec.SessionDuration > 0 {
		return
	}
	fields := map[string]any{"action": action, "ordinal": observer.actionOrdinal, "state": "ready"}
	if observedLatency > 0 {
		fields["latency_ms"] = observedLatency.Milliseconds()
	}
	_, _ = observer.producer.Append(demokit.Event{
		RunID:   observer.spec.RunID,
		Player:  observer.spec.PlayerID,
		Kind:    demokit.KindBotAction,
		Phase:   "READY",
		Message: "bot lifecycle",
		Fields:  fields,
	})
	observer.appendScene(
		demokit.KindStepCompleted,
		"ACTION",
		"bot action completed",
		map[string]any{"action": action, "step": "bot_action"},
	)
}

func (observer *BotObserver) OnAction(action string, latency ...time.Duration) {
	observer.Action(action, latency...)
}

// Phase accumulates a latency sample for one bot without emitting an event.
// A sustained online bot produces many samples per phase, and the bounded
// event store is evidence of lifecycle, not a metrics pipeline.
func (observer *BotObserver) Phase(phase string, elapsed time.Duration) {
	if observer == nil || elapsed <= 0 {
		return
	}
	name := SafePhaseName(phase)
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.phases == nil {
		observer.phases = make(map[string]LatencyHistogram, MaxPhaseCardinality)
	}
	histogram, tracked := observer.phases[name]
	if !tracked {
		if len(observer.phases) >= MaxPhaseCardinality {
			return
		}
		histogram = NewLatencyHistogram()
	}
	histogram.Observe(elapsed)
	observer.phases[name] = histogram
}

func (observer *BotObserver) OnPhase(phase string, elapsed time.Duration) {
	observer.Phase(phase, elapsed)
}

// PhaseHistograms returns a copy of this bot's phase distributions.
func (observer *BotObserver) PhaseHistograms() map[string]LatencyHistogram {
	if observer == nil {
		return nil
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if len(observer.phases) == 0 {
		return nil
	}
	copied := make(map[string]LatencyHistogram, len(observer.phases))
	for phase, histogram := range observer.phases {
		copied[phase] = histogram
	}
	return copied
}

// ActionCount reports how many actions this bot completed.
func (observer *BotObserver) ActionCount() int {
	if observer == nil {
		return 0
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.actionOrdinal
}

// ActionLatency returns every action latency this bot reported.
func (observer *BotObserver) ActionLatency() LatencyHistogram {
	if observer == nil {
		return LatencyHistogram{}
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.actionLatency
}

func (observer *BotObserver) Reconnect(attempt ...int) {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.terminal {
		return
	}
	value := observer.reconnects + 1
	if len(attempt) > 0 && attempt[0] > value {
		value = attempt[0]
	}
	observer.reconnects = value
	_, _ = observer.producer.Append(demokit.Event{
		RunID:   observer.spec.RunID,
		Player:  observer.spec.PlayerID,
		Kind:    demokit.KindBotReconnect,
		Phase:   "RECONNECT",
		Message: "bot lifecycle",
		Fields:  map[string]any{"attempt": value, "state": "connecting"},
	})
	observer.appendScene(
		demokit.KindScenarioBarrier,
		"RECONNECT",
		"bot reconnect barrier",
		map[string]any{"barrier": "bot_reconnect", "state": "connecting"},
	)
}

func (observer *BotObserver) OnReconnect(attempt ...int) { observer.Reconnect(attempt...) }

func (observer *BotObserver) Terminal(result BotResult) {
	if observer == nil {
		return
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.terminal {
		return
	}
	if result.BotID == "" {
		result.BotID = observer.spec.BotID
	}
	if result.PlayerID == 0 {
		result.PlayerID = observer.spec.PlayerID
	}
	if result.Index == 0 && observer.spec.Index != 0 {
		result.Index = observer.spec.Index
	}
	if result.Status != BotSucceeded && result.Status != BotFailed && result.Status != BotCancelled {
		result.Status = BotFailed
	}
	if observer.ready {
		result.Ready = true
	}
	if result.Reconnects < observer.reconnects {
		result.Reconnects = observer.reconnects
	}
	if result.Latency <= 0 {
		result.Latency = observer.lastLatency
	}
	observer.terminal = true
	observer.terminalResult = result
	_, _ = observer.producer.Append(demokit.Event{
		RunID:   observer.spec.RunID,
		Player:  observer.spec.PlayerID,
		Kind:    demokit.KindBotTerminal,
		Phase:   "CLOSED",
		Message: "bot terminal",
		Fields: map[string]any{
			"state":  "closed",
			"status": string(result.Status),
		},
	})
}

func (observer *BotObserver) OnTerminal(result BotResult) { observer.Terminal(result) }

// TerminalEmitted reports whether a terminal event has already been accepted
// by this observer. Duplicate terminal callbacks are ignored.
func (observer *BotObserver) TerminalEmitted() bool {
	if observer == nil {
		return false
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.terminal
}

// TerminalResult returns the first terminal result, if any.
func (observer *BotObserver) TerminalResult() (BotResult, bool) {
	if observer == nil {
		return BotResult{}, false
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.terminalResult, observer.terminal
}

func (observer *BotObserver) ReadySeen() bool {
	if observer == nil {
		return false
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.ready
}

func (observer *BotObserver) ReconnectCount() int {
	if observer == nil {
		return 0
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.reconnects
}

func runStartFields(spec LiveRunSpec) map[string]any {
	return map[string]any{
		"environment":    string(spec.Environment),
		"players":        spec.Players,
		"player_start":   spec.PlayerStart,
		"duration_ms":    spec.Duration.Milliseconds(),
		"arrival_rate":   spec.ArrivalRate,
		"max_online":     spec.MaxOnline,
		"seed":           spec.Seed,
		"scenario":       spec.ScenarioID,
		"capacity_claim": false,
		"execution":      ExecutionRealSandbox,
	}
}

func runResultFields(result RunResult) map[string]any {
	return map[string]any{
		"status":         string(result.Status),
		"attempted":      result.Summary.Attempted,
		"ready":          result.Summary.Ready,
		"succeeded":      result.Summary.Succeeded,
		"failed":         result.Summary.Failed,
		"cancelled":      result.Summary.Cancelled,
		"reconnect":      result.Summary.Reconnect,
		"capacity_claim": false,
		"execution":      ExecutionRealSandbox,
	}
}

func (producer *Producer) currentTime() time.Time {
	if producer != nil && producer.now != nil {
		return producer.now()
	}
	return time.Now()
}

func safeActionName(action string) string {
	action = strings.TrimSpace(action)
	if !validIdentifier(action, MaxActionLength) || containsSensitiveText(action) {
		return "redacted"
	}
	return action
}

func sanitizeFields(fields map[string]any) map[string]any {
	if len(fields) == 0 {
		return nil
	}
	clean := make(map[string]any)
	for key, value := range fields {
		if containsSensitiveText(key) {
			continue
		}
		if sanitized, ok := sanitizeFieldValue(value); ok {
			clean[key] = sanitized
		}
	}
	if len(clean) == 0 {
		return nil
	}
	return clean
}

func sanitizeFieldValue(value any) (any, bool) {
	switch typed := value.(type) {
	case string:
		if containsSensitiveText(typed) {
			return "redacted", true
		}
		return typed, true
	case map[string]any:
		return sanitizeFields(typed), true
	case []any:
		clean := make([]any, 0, len(typed))
		for _, item := range typed {
			if sanitized, ok := sanitizeFieldValue(item); ok {
				clean = append(clean, sanitized)
			}
		}
		return clean, true
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return typed, true
	default:
		return nil, false
	}
}

func containsSensitiveText(value string) bool {
	value = strings.ToLower(value)
	for _, word := range []string{"password", "ticket", "cookie", "secret", "token", "credential", "authorization"} {
		if strings.Contains(value, word) {
			return true
		}
	}
	return false
}

// FormatStatusError is a deliberately low-cardinality helper for adapters
// that need a safe status text. It never accepts or formats secret material.
func FormatStatusError(status BotStatus) string {
	switch status {
	case BotSucceeded, BotFailed, BotCancelled:
		return string(status)
	default:
		return fmt.Sprintf("status-%s", safeActionName(string(status)))
	}
}
