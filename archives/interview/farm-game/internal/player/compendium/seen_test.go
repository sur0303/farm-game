package compendium

import "testing"

// stateAtRevision builds a state whose compendium_revision == rev by
// unlocking that many crops.
func stateAtRevision(t *testing.T, rev uint64) State {
	t.Helper()
	state := NewState()
	for i := uint64(1); i <= rev; i++ {
		state = unlockOne(t, state, 42, uint32(1000+i), "")
	}
	return state
}

func TestAckSeen_FirstAdvance(t *testing.T) {
	state := stateAtRevision(t, 5)
	out := AckSeen(state, 5)
	if out.Status != StatusSucceeded {
		t.Fatalf("status = %s (err=%v)", out.Status, out.Err)
	}
	if out.SeenAfter != 5 || out.CurrentRevision != 5 {
		t.Fatalf("seen_after=%d current=%d", out.SeenAfter, out.CurrentRevision)
	}
	if out.Candidate.HasNewUnlock() {
		t.Fatal("red dot must clear once seen catches up")
	}
	if state.SeenRevision != 0 {
		t.Fatal("input state mutated")
	}
}

func TestAckSeen_RepeatAndOlderAckNoChange(t *testing.T) {
	state := stateAtRevision(t, 5)
	state = AckSeen(state, 4).Candidate

	for _, observed := range []uint64{4, 2, 0, 4} {
		out := AckSeen(state, observed)
		if out.Status != StatusNoChange {
			t.Fatalf("ack(%d) = %s, want NO_CHANGE", observed, out.Status)
		}
		if out.Err == nil || out.Err.Code != ErrSeenAlreadyAdvanced {
			t.Fatalf("ack(%d) err = %v, want COMPENDIUM_SEEN_ALREADY_ADVANCED", observed, out.Err)
		}
		if out.SeenAfter != 4 {
			t.Fatalf("ack(%d) regressed seen to %d", observed, out.SeenAfter)
		}
	}
}

// TestAckSeen_LateAckKeepsNewerRedDot is the fence scenario of
// docs/design/37 §5.2: GET returned revision=5, a concurrent unlock created
// revision=6, then the late Ack(5) arrives. seen advances to 5 only; the
// revision-6 red dot survives.
func TestAckSeen_LateAckKeepsNewerRedDot(t *testing.T) {
	state := stateAtRevision(t, 6)
	out := AckSeen(state, 5)
	if out.Status != StatusSucceeded || out.SeenAfter != 5 {
		t.Fatalf("ack(5) = %s seen=%d", out.Status, out.SeenAfter)
	}
	if !out.Candidate.HasNewUnlock() {
		t.Fatal("red dot of revision 6 must survive the late ack(5)")
	}
}

// TestAckSeen_ClampedAtRevision: a client can never ack beyond the
// authoritative revision.
func TestAckSeen_ClampedAtRevision(t *testing.T) {
	state := stateAtRevision(t, 3)
	out := AckSeen(state, 999)
	if out.Status != StatusSucceeded || out.SeenAfter != 3 {
		t.Fatalf("ack(999) = %s seen=%d, want clamped to 3", out.Status, out.SeenAfter)
	}
}

// TestAckSeen_OutOfOrderMonotonic: ack(3) then ack(2) then ack(3): the
// watermark only advances; duplicates are NO_CHANGE.
func TestAckSeen_OutOfOrderMonotonic(t *testing.T) {
	state := stateAtRevision(t, 5)
	if out := AckSeen(state, 3); out.Status != StatusSucceeded {
		t.Fatalf("ack(3) = %s", out.Status)
	} else {
		state = out.Candidate
	}
	if out := AckSeen(state, 2); out.Status != StatusNoChange {
		t.Fatalf("ack(2) = %s, want NO_CHANGE", out.Status)
	}
	if out := AckSeen(state, 3); out.Status != StatusNoChange {
		t.Fatalf("ack(3) replay = %s, want NO_CHANGE", out.Status)
	}
	if out := AckSeen(state, 4); out.Status != StatusSucceeded || out.SeenAfter != 4 {
		t.Fatalf("ack(4) = %s seen=%d", out.Status, out.SeenAfter)
	}
}

// TestAckSeen_NeverExceedsRevision: after more unlocks and more acks the
// invariant seen <= revision always holds.
func TestAckSeen_NeverExceedsRevision(t *testing.T) {
	state := NewState()
	for i := uint64(1); i <= 10; i++ {
		out := AckSeen(state, i*2) // always ahead of revision
		if out.Candidate.SeenRevision > out.Candidate.Revision {
			t.Fatalf("seen %d > revision %d", out.Candidate.SeenRevision, out.Candidate.Revision)
		}
		if out.Status == StatusSucceeded {
			state = out.Candidate
		}
		state = unlockOne(t, state, 42, uint32(2000+i), "")
	}
	if state.SeenRevision > state.Revision {
		t.Fatalf("final seen %d > revision %d", state.SeenRevision, state.Revision)
	}
}
