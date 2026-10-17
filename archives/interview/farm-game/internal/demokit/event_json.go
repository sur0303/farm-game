package demokit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

const maxSafeJavaScriptInteger = uint64(1<<53 - 1)

// MarshalJSON keeps the existing numeric player_id shape for safe small ids,
// but emits a decimal string for values that JavaScript Number cannot represent
// exactly. This prevents a read-only UI from silently changing a client id.
func (event Event) MarshalJSON() ([]byte, error) {
	type wireEvent struct {
		Seq     uint64         `json:"seq,omitempty"`
		TsMs    int64          `json:"ts_ms"`
		RunID   string         `json:"run_id"`
		Kind    EventKind      `json:"kind"`
		Phase   string         `json:"phase,omitempty"`
		Player  any            `json:"player_id,omitempty"`
		GateID  string         `json:"gate_id,omitempty"`
		Version uint64         `json:"version,omitempty"`
		Message string         `json:"message,omitempty"`
		Fields  map[string]any `json:"fields,omitempty"`
	}
	var player any
	if event.Player != 0 {
		player = event.Player
		if event.Player > maxSafeJavaScriptInteger {
			player = strconv.FormatUint(event.Player, 10)
		}
	}
	return json.Marshal(wireEvent{
		Seq:     event.Seq,
		TsMs:    event.TsMs,
		RunID:   event.RunID,
		Kind:    event.Kind,
		Phase:   event.Phase,
		Player:  player,
		GateID:  event.GateID,
		Version: event.Version,
		Message: event.Message,
		Fields:  event.Fields,
	})
}

// UnmarshalJSON accepts both the historical numeric player_id and the safe
// decimal-string form emitted for large uint64 ids.
func (event *Event) UnmarshalJSON(input []byte) error {
	type wireEvent struct {
		Seq     uint64          `json:"seq,omitempty"`
		TsMs    int64           `json:"ts_ms"`
		RunID   string          `json:"run_id"`
		Kind    EventKind       `json:"kind"`
		Phase   string          `json:"phase,omitempty"`
		Player  json.RawMessage `json:"player_id"`
		GateID  string          `json:"gate_id,omitempty"`
		Version uint64          `json:"version,omitempty"`
		Message string          `json:"message,omitempty"`
		Fields  map[string]any  `json:"fields,omitempty"`
	}
	var wire wireEvent
	if err := json.Unmarshal(input, &wire); err != nil {
		return err
	}
	player, err := decodePlayerID(wire.Player)
	if err != nil {
		return err
	}
	*event = Event{
		Seq:     wire.Seq,
		TsMs:    wire.TsMs,
		RunID:   wire.RunID,
		Kind:    wire.Kind,
		Phase:   wire.Phase,
		Player:  player,
		GateID:  wire.GateID,
		Version: wire.Version,
		Message: wire.Message,
		Fields:  wire.Fields,
	}
	return nil
}

func decodePlayerID(raw json.RawMessage) (uint64, error) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, nil
	}
	trimmed := bytes.TrimSpace(raw)
	if trimmed[0] == '"' {
		var text string
		if err := json.Unmarshal(trimmed, &text); err != nil {
			return 0, fmt.Errorf("decode player_id string: %w", err)
		}
		value, err := strconv.ParseUint(text, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("decode player_id %q: %w", text, err)
		}
		return value, nil
	}
	var value uint64
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return 0, fmt.Errorf("decode numeric player_id: %w", err)
	}
	return value, nil
}
