package demokit

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEventJSONProtectsLargePlayerIDsFromJavaScriptRounding(t *testing.T) {
	large := uint64(1<<53 + 1)
	raw, err := json.Marshal(Event{RunID: "run", Kind: KindClientReady, Player: large})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"player_id":"9007199254740993"`) {
		t.Fatalf("large player JSON = %s, want decimal string", raw)
	}
	var decoded Event
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Player != large {
		t.Fatalf("decoded player = %d, want %d", decoded.Player, large)
	}

	raw, err = json.Marshal(Event{RunID: "run", Kind: KindClientReady, Player: 7})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"player_id":7`) {
		t.Fatalf("small player JSON = %s, want numeric compatibility", raw)
	}
}
