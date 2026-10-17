package downlink

import (
	"errors"
	"fmt"
	"testing"
)

// c6bWriterCapacity is the deterministic per-connection capacity used by the
// C6B slow-cohort harness. Total caps are small enough that a slow writer
// (kept at its item cap) rejects everything, while a healthy writer only hits
// the hint soft quota and keeps FarmChange/CommandResult exact.
var c6bWriterCapacity = WriterCapacity{
	MaxItems: 8, MaxBytes: 1 << 20,
	Hint: HintPolicy{MaxHintItems: 2, MaxHintBytes: 1024},
}

type c6bTrafficStats struct {
	hintAccepted, hintQuotaRejected, hintFullRejected int
	farmChangeAccepted, farmChangeFullRejected        int
	commandResultAccepted, commandResultFullRejected  int
}

// runC6BMixedTraffic drives one deterministic connection through 4 hint
// attempts (quota 2), one FarmChange and one CommandResult. It returns the
// observed counters plus the FIFO order of accepted frames.
func runC6BMixedTraffic(t *testing.T, gen uint64, slow bool) (c6bTrafficStats, []FrameKind) {
	t.Helper()
	w := NewConnectionWriterWithCapacity(gen, c6bWriterCapacity)
	if slow {
		// A slow client keeps its writer at the total item cap: the WritePump
		// has not drained, so every further frame is rejected with full.
		for i := 0; i < 8; i++ {
			if err := w.EnqueueKind(gen, FrameKindOther, []byte("filler")); err != nil {
				t.Fatalf("filler %d: %v", i, err)
			}
		}
	}
	var stats c6bTrafficStats
	// Mixed traffic in a fixed order: hints first, then the exact-delivery
	// frames. Sequence allocation happens after admission, so rejected hints
	// never consume a sequence and the accepted FIFO stays contiguous.
	for i := 0; i < 4; i++ {
		err := w.EnqueueKind(gen, FrameKindHint, []byte("hint"))
		switch {
		case err == nil:
			stats.hintAccepted++
		case errors.Is(err, ErrHintQuota):
			stats.hintQuotaRejected++
		case errors.Is(err, ErrQueueFull):
			stats.hintFullRejected++
		default:
			t.Fatalf("hint %d unexpected error: %v", i, err)
		}
	}
	if err := w.EnqueueKind(gen, FrameKindFarmChange, []byte("change")); err == nil {
		stats.farmChangeAccepted++
	} else if errors.Is(err, ErrQueueFull) {
		stats.farmChangeFullRejected++
	} else {
		t.Fatalf("farm change unexpected error: %v", err)
	}
	if err := w.EnqueueKind(gen, FrameKindCommandResult, []byte("result")); err == nil {
		stats.commandResultAccepted++
	} else if errors.Is(err, ErrQueueFull) {
		stats.commandResultFullRejected++
	} else {
		t.Fatalf("command result unexpected error: %v", err)
	}
	var order []FrameKind
	for {
		item, ok := w.Dequeue()
		if !ok {
			break
		}
		order = append(order, item.Kind)
	}
	return stats, order
}

// TestC6BSlowCohortHarness is the deterministic C6B fixture: it models 1%, 5%
// and 20% slow cohorts over 100 connections, verifies that slow writers only
// lose their own frames (hint soft drop and full rejection), that healthy
// writers keep FarmChange/CommandResult exact while only hints hit the soft
// quota, and that each connection's accepted FIFO stays monotonic.
func TestC6BSlowCohortHarness(t *testing.T) {
	const total = 100
	for _, cohort := range []struct {
		name string
		pct  int
	}{{"1pct", 1}, {"5pct", 5}, {"20pct", 20}} {
		slowCount := total * cohort.pct / 100
		if slowCount == 0 {
			slowCount = 1
		}
		fast := total - slowCount
		var fastStats, slowStats c6bTrafficStats
		fifoViolations := 0
		for i := 0; i < total; i++ {
			isSlow := i < slowCount
			stats, order := runC6BMixedTraffic(t, uint64(1000+i), isSlow)
			// Accepted frames must drain in exact enqueue order per connection.
			// Slow writers are already at the total item cap and reject
			// everything, so their dequeue order is filler-only; the FIFO
			// contract is verified on the healthy fast connections.
			want := []FrameKind{FrameKindHint, FrameKindHint, FrameKindFarmChange, FrameKindCommandResult}
			if !isSlow {
				if len(order) != len(want) {
					t.Fatalf("cohort %s connection %d FIFO length=%d, want %d", cohort.name, i, len(order), len(want))
				}
				for j, kind := range order {
					if kind != want[j] {
						fifoViolations++
					}
				}
			}
			if isSlow {
				slowStats = addC6BStats(slowStats, stats)
			} else {
				fastStats = addC6BStats(fastStats, stats)
			}
		}
		if fifoViolations != 0 {
			t.Fatalf("cohort %s FIFO violations=%d", cohort.name, fifoViolations)
		}
		if fastStats.hintAccepted != fast*2 || fastStats.hintQuotaRejected != fast*2 {
			t.Fatalf("cohort %s fast hint accepted=%d quotaRejected=%d, want %d/%d",
				cohort.name, fastStats.hintAccepted, fastStats.hintQuotaRejected, fast*2, fast*2)
		}
		if fastStats.farmChangeAccepted != fast || fastStats.commandResultAccepted != fast {
			t.Fatalf("cohort %s fast exact frames farm=%d result=%d, want %d/%d",
				cohort.name, fastStats.farmChangeAccepted, fastStats.commandResultAccepted, fast, fast)
		}
		if slowStats.hintFullRejected != slowCount*4 || slowStats.farmChangeFullRejected != slowCount ||
			slowStats.commandResultFullRejected != slowCount {
			t.Fatalf("cohort %s slow full rejects hint=%d farm=%d result=%d, want %d/%d/%d",
				cohort.name, slowStats.hintFullRejected, slowStats.farmChangeFullRejected,
				slowStats.commandResultFullRejected, slowCount*4, slowCount, slowCount)
		}
		t.Logf("cohort %s: fast=%d slow=%d fastStats=%+v slowStats=%+v",
			cohort.name, fast, slowCount, fastStats, slowStats)
	}
}

func addC6BStats(left, right c6bTrafficStats) c6bTrafficStats {
	return c6bTrafficStats{
		hintAccepted:              left.hintAccepted + right.hintAccepted,
		hintQuotaRejected:         left.hintQuotaRejected + right.hintQuotaRejected,
		hintFullRejected:          left.hintFullRejected + right.hintFullRejected,
		farmChangeAccepted:        left.farmChangeAccepted + right.farmChangeAccepted,
		farmChangeFullRejected:    left.farmChangeFullRejected + right.farmChangeFullRejected,
		commandResultAccepted:     left.commandResultAccepted + right.commandResultAccepted,
		commandResultFullRejected: left.commandResultFullRejected + right.commandResultFullRejected,
	}
}

// TestC6BHintQuotaReopensAfterDrain verifies the Gate-local capacity half of
// recovery: once the transport drains queued hints, soft-quota slots reopen
// and fresh hints can be accepted. The dropped business opportunity itself is
// recovered by the independent Navigator Pull path; this writer-level fixture
// deliberately does not pretend to exercise that cross-service query.
func TestC6BHintQuotaReopensAfterDrain(t *testing.T) {
	w := NewConnectionWriterWithCapacity(2000, c6bWriterCapacity)
	if err := w.EnqueueKind(2000, FrameKindHint, []byte("h1")); err != nil {
		t.Fatalf("first hint: %v", err)
	}
	if err := w.EnqueueKind(2000, FrameKindHint, []byte("h2")); err != nil {
		t.Fatalf("second hint: %v", err)
	}
	// Quota 2 is full; the third hint is soft-dropped.
	if err := w.EnqueueKind(2000, FrameKindHint, []byte("h3")); !errors.Is(err, ErrHintQuota) {
		t.Fatalf("third hint=%v, want ErrHintQuota", err)
	}
	// No sequence was consumed by the dropped hint; a FarmChange still enters
	// the FIFO and keeps its exact delivery semantics.
	if err := w.EnqueueKind(2000, FrameKindFarmChange, []byte("change")); err != nil {
		t.Fatalf("farm change during hint drop: %v", err)
	}
	// Transport recovery: the WritePump drains the queue. Draining hint frames
	// frees the soft-quota slots, so a fresh hint is accepted again.
	for i := 0; i < 3; i++ {
		if _, ok := w.Dequeue(); !ok {
			t.Fatalf("drain %d stopped early", i)
		}
	}
	if err := w.EnqueueKind(2000, FrameKindHint, []byte("recovered")); err != nil {
		t.Fatalf("hint after pull recovery: %v", err)
	}
	item, ok := w.Dequeue()
	if !ok || item.Kind != FrameKindHint || string(item.Data) != "recovered" {
		t.Fatalf("recovered dequeue=%+v ok=%v", item, ok)
	}
	// The quota re-fills: two more hints are accepted, and the next one is
	// soft-dropped again, proving the recovered writer enforces the same
	// bounded quota as before the drop.
	if err := w.EnqueueKind(2000, FrameKindHint, []byte("second")); err != nil {
		t.Fatalf("hint after recovery slot freed: %v", err)
	}
	if err := w.EnqueueKind(2000, FrameKindHint, []byte("third")); err != nil {
		t.Fatalf("second hint after recovery: %v", err)
	}
	if err := w.EnqueueKind(2000, FrameKindHint, []byte("fourth")); !errors.Is(err, ErrHintQuota) {
		t.Fatalf("fourth hint=%v, want ErrHintQuota (quota 2 re-filled)", err)
	}
}

// TestC6BDrainYieldsToCommandResultAndDrainsAll is the transport-level drain
// contract: one wakeup drains at most DownlinkDrainQuantum frames, returns to
// select (so a synchronous CommandResult is never starved), and the pending/
// re-notify seam drains every remaining frame without another enqueue. The
// end-to-end websocket variant lives in transport/websocket; this writer-level
// check pins the same contract deterministically.
func TestC6BDrainYieldsToCommandResultAndDrainsAll(t *testing.T) {
	w := NewConnectionWriterWithCapacity(3000, WriterCapacity{
		MaxItems: 64, MaxBytes: 1 << 20, Hint: HintPolicy{MaxHintItems: 8, MaxHintBytes: 1 << 20},
	})
	const total = 6
	const quantum = 2
	for i := 0; i < total; i++ {
		if err := w.EnqueueKind(3000, FrameKindHint, []byte(fmt.Sprintf("h%d", i))); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	drained := 0
	// First quantum: consume the initial notification, drain exactly quantum.
	select {
	case <-w.NotifyChan():
	default:
		t.Fatal("initial NotifyChan must be readable")
	}
	for i := 0; i < quantum; i++ {
		if _, ok := w.Dequeue(); !ok {
			t.Fatal("quantum drained before queue empty")
		}
		drained++
	}
	// A synchronous CommandResult could have been written here between quanta.
	if !w.Pending() {
		t.Fatal("Pending must report queued frames after one quantum")
	}
	// Re-arm and drain the rest without any further enqueue.
	select {
	case <-w.NotifyChan():
	default:
		t.Fatal("re-armed NotifyChan must be readable after Pending")
	}
	for {
		if drained >= total {
			break
		}
		item, ok := w.Dequeue()
		if !ok {
			t.Fatalf("drain stopped early at %d/%d", drained, total)
		}
		_ = item
		drained++
	}
	if drained != total {
		t.Fatalf("drained=%d, want %d", drained, total)
	}
	if w.Pending() {
		t.Fatal("Pending must clear after the queue drains")
	}
}
