package datamode

import "testing"

func TestParseDefaultsToDurableCAS(t *testing.T) {
	for _, raw := range []string{"", "  ", "DURABLE_CAS"} {
		got, err := Parse(raw)
		if err != nil || got != DurableCAS {
			t.Fatalf("Parse(%q)=(%q,%v)", raw, got, err)
		}
	}
}

func TestParseDirtyBestEffort(t *testing.T) {
	got, err := Parse(" dirty_best_effort ")
	if err != nil || got != DirtyBestEffort || !got.DirtyWritebackEnabled() {
		t.Fatalf("got=(%q,%v) enabled=%v", got, err, got.DirtyWritebackEnabled())
	}
}

func TestParseRejectsUnknownMode(t *testing.T) {
	if _, err := Parse("eventual"); err == nil {
		t.Fatal("expected strict parse error")
	}
}

func TestFromLookup(t *testing.T) {
	got, err := FromLookup(func(key string) (string, bool) {
		if key != EnvKey {
			t.Fatalf("key=%q", key)
		}
		return string(DirtyBestEffort), true
	})
	if err != nil || got != DirtyBestEffort {
		t.Fatalf("got=(%q,%v)", got, err)
	}
}
