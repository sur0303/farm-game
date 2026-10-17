package demokit

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// EvidenceMode identifies where a read-only cockpit obtains its evidence.
// LIVE is backed by an in-process event store; REPLAY is loaded from an
// evidence directory. Neither mode exposes a control or execution surface.
type EvidenceMode string

const (
	EvidenceModeLive   EvidenceMode = "LIVE"
	EvidenceModeReplay EvidenceMode = "REPLAY"
)

// EventKind is the stable vocabulary used by the demo console event stream.
type EventKind string

const (
	KindScenarioStarted EventKind = "scenario.started"
	KindScenarioBarrier EventKind = "scenario.barrier"
	KindFaultArmed      EventKind = "fault.armed"
	KindFaultTriggered  EventKind = "fault.triggered"
	KindFaultReleased   EventKind = "fault.released"
	KindClientJoined    EventKind = "client.joined"
	KindClientReady     EventKind = "client.ready"
	KindClientLagging   EventKind = "client.lagging"
	KindClientResync    EventKind = "client.resync"
	KindClientClosed    EventKind = "client.closed"
	KindClientReconnect EventKind = "client.reconnect"
	KindVersionApplied  EventKind = "version.applied"
	KindProbeFarm       EventKind = "probe.farm"
	KindVerdict         EventKind = "verdict"
	KindStepCompleted   EventKind = "step.completed"

	// KindPlanStep is emitted only when an old plan-only bundle is replayed.
	// It explicitly describes an inert plan step and must not be interpreted as
	// proof that the step was executed.
	KindPlanStep EventKind = "plan.step"

	// KindStreamGap is reserved for a future stream-gap notification. The
	// current HTTP API reports gaps in JSON metadata and reconnect guidance,
	// rather than fabricating a domain event.
	KindStreamGap EventKind = "stream.gap"

	// LIVE bot execution lifecycle. These observations are produced by the
	// in-process sandbox runner and are distinct from inert plan.step events.
	KindRunStarted   EventKind = "run.started"
	KindRunCompleted EventKind = "run.completed"
	KindRunFailed    EventKind = "run.failed"
	KindRunCancelled EventKind = "run.cancelled"
	KindBotLogin     EventKind = "bot.login"
	KindBotHello     EventKind = "bot.hello"
	KindBotJoin      EventKind = "bot.join"
	KindBotReady     EventKind = "bot.ready"
	KindBotAction    EventKind = "bot.action"
	KindBotReconnect EventKind = "bot.reconnect"
	KindBotTerminal  EventKind = "bot.terminal"
	KindBotJoined    EventKind = KindBotJoin
	KindRunFinished  EventKind = KindRunCompleted

	// Short aliases keep event-kind assertions concise for LIVE callers.
	KindLogin     EventKind = KindBotLogin
	KindHello     EventKind = KindBotHello
	KindJoin      EventKind = KindBotJoin
	KindReady     EventKind = KindBotReady
	KindAction    EventKind = KindBotAction
	KindReconnect EventKind = KindBotReconnect
	KindTerminal  EventKind = KindBotTerminal
)

// Event is the read-only, JSON-compatible event contract shared by LIVE and
// REPLAY readers. Seq is assigned by the bounded store and is intentionally
// separate from the millisecond timestamp: clients can use it as a resume
// cursor even when several events have the same timestamp.
type Event struct {
	Seq     uint64         `json:"seq,omitempty"`
	TsMs    int64          `json:"ts_ms"`
	RunID   string         `json:"run_id"`
	Kind    EventKind      `json:"kind"`
	Phase   string         `json:"phase,omitempty"`
	Player  uint64         `json:"player_id,omitempty"`
	GateID  string         `json:"gate_id,omitempty"`
	Version uint64         `json:"version,omitempty"`
	Message string         `json:"message,omitempty"`
	Fields  map[string]any `json:"fields,omitempty"`
}

// Validate checks the fields required for an event to be stored. Event data
// remains deliberately open-ended, but it must still be JSON encodable.
func (event Event) Validate() error {
	if strings.TrimSpace(event.RunID) == "" {
		return errors.New("event run_id is required")
	}
	if strings.TrimSpace(string(event.Kind)) == "" {
		return errors.New("event kind is required")
	}
	if _, err := json.Marshal(event.Fields); err != nil {
		return fmt.Errorf("event fields are not JSON encodable: %w", err)
	}
	return nil
}

// CloneEvent returns a defensive copy suitable for crossing a store or HTTP
// boundary. The event schema intentionally uses JSON-shaped field values, so
// the recursive copy handles the map/slice forms produced by encoding/json.
func CloneEvent(event Event) Event {
	clone := event
	clone.Fields = cloneFields(event.Fields)
	return clone
}

func cloneFields(fields map[string]any) map[string]any {
	if fields == nil {
		return nil
	}
	clone := make(map[string]any, len(fields))
	for key, value := range fields {
		clone[key] = cloneFieldValue(value)
	}
	return clone
}

func cloneFieldValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneFields(typed)
	case []any:
		clone := make([]any, len(typed))
		for index, item := range typed {
			clone[index] = cloneFieldValue(item)
		}
		return clone
	default:
		return value
	}
}

// RunView is the read model exposed by the cockpit. Counts include all events
// appended for the run, while RetainedEventCount describes the current
// bounded in-memory window. A replay of plan-only evidence has Execution set
// to PLAN_ONLY and therefore cannot be mistaken for an executed run.
type RunView struct {
	RunID              string       `json:"run_id"`
	ScenarioID         string       `json:"scenario_id,omitempty"`
	ScenarioTitle      string       `json:"scenario_title,omitempty"`
	SourceRef          string       `json:"source_ref,omitempty"`
	Mode               EvidenceMode `json:"mode"`
	Execution          string       `json:"execution,omitempty"`
	Status             string       `json:"status,omitempty"`
	Phase              string       `json:"phase,omitempty"`
	HeadVersion        uint64       `json:"head_version,omitempty"`
	EventCount         uint64       `json:"event_count"`
	RetainedEventCount uint64       `json:"retained_event_count"`
	DroppedEventCount  uint64       `json:"dropped_event_count,omitempty"`
	OldestSeq          uint64       `json:"oldest_seq,omitempty"`
	LatestSeq          uint64       `json:"latest_seq,omitempty"`
	StartedAtMs        int64        `json:"started_at_ms,omitempty"`
	LastEventAtMs      int64        `json:"last_event_at_ms,omitempty"`
}
