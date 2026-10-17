package downlink

import (
	"errors"
	"testing"

	commonpb "farmgame/api/gen/common"
	gateprotocol "farmgame/internal/gateway/protocol"
	"google.golang.org/protobuf/proto"
)

func marshalPushFrame(t *testing.T, pushType commonpb.PushType) []byte {
	t.Helper()
	raw, err := proto.Marshal(gateprotocol.NewServerPushFrame(1, &commonpb.ServerPushEnvelope{
		PushType:    pushType,
		PushContext: &commonpb.ServerPushEnvelope_Session{Session: &commonpb.SessionPushContext{TargetPlayerId: 1}},
		Payload:     []byte("p"),
	}))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestConnectionWriterGenerationAndBounds(t *testing.T) {
	w := NewConnectionWriter(7, 2, 3)
	if err := w.Enqueue(6, []byte("x")); err != ErrStaleGeneration {
		t.Fatalf("stale generation error=%v", err)
	}
	if err := w.Enqueue(7, []byte("ab")); err != nil {
		t.Fatal(err)
	}
	if err := w.Enqueue(7, []byte("cd")); err != ErrQueueFull {
		t.Fatalf("want byte queue full, got %v", err)
	}
	it, ok := w.Dequeue()
	if !ok || string(it.Data) != "ab" {
		t.Fatalf("dequeue=%+v ok=%v", it, ok)
	}
}

func TestConnectionWriterHintSoftQuotaRejectsBeforeSequence(t *testing.T) {
	w := NewConnectionWriterWithCapacity(9, WriterCapacity{
		MaxItems: 64, MaxBytes: 1 << 20,
		Hint: HintPolicy{MaxHintItems: 2, MaxHintBytes: 64},
	})
	if err := w.EnqueueKind(9, FrameKindHint, []byte("h1")); err != nil {
		t.Fatalf("first hint: %v", err)
	}
	if err := w.EnqueueKind(9, FrameKindHint, []byte("h2")); err != nil {
		t.Fatalf("second hint: %v", err)
	}
	// The third hint exceeds only the soft item quota; it must be rejected
	// before any sequence is allocated and must not disturb queued frames.
	if err := w.EnqueueKind(9, FrameKindHint, []byte("h3")); !errors.Is(err, ErrHintQuota) {
		t.Fatalf("third hint error=%v, want ErrHintQuota", err)
	}
	stats := w.Stats()
	if stats.HintItems != 2 {
		t.Fatalf("hint items=%d, want 2", stats.HintItems)
	}
	if got := stats.RejectedKind(FrameKindHint, "reject", "quota"); got != 1 {
		t.Fatalf("hint quota rejects=%d, want 1", got)
	}
	// Non-hint frames still enqueue under the same total caps.
	if err := w.EnqueueKind(9, FrameKindFarmChange, []byte("change")); err != nil {
		t.Fatalf("farm change after hint quota: %v", err)
	}
	if items, _ := w.Len(); items != 3 {
		t.Fatalf("writer items=%d, want 3", items)
	}
	if err := w.EnqueueKind(9, FrameKindHint, []byte("h4")); !errors.Is(err, ErrHintQuota) {
		t.Fatalf("fourth hint error=%v, want ErrHintQuota", err)
	}
}

func TestConnectionWriterHintQuotaFreesOnDequeue(t *testing.T) {
	w := NewConnectionWriterWithCapacity(10, WriterCapacity{
		MaxItems: 64, MaxBytes: 1 << 20,
		Hint: HintPolicy{MaxHintItems: 1, MaxHintBytes: 1024},
	})
	if err := w.EnqueueKind(10, FrameKindHint, []byte("hint-a")); err != nil {
		t.Fatal(err)
	}
	if err := w.EnqueueKind(10, FrameKindHint, []byte("hint-b")); !errors.Is(err, ErrHintQuota) {
		t.Fatalf("second hint error=%v, want ErrHintQuota", err)
	}
	if item, ok := w.Dequeue(); !ok || item.Kind != FrameKindHint {
		t.Fatalf("dequeue=%+v ok=%v", item, ok)
	}
	if err := w.EnqueueKind(10, FrameKindHint, []byte("hint-c")); err != nil {
		t.Fatalf("hint after dequeue: %v", err)
	}
}

func TestConnectionWriterTotalCapsAndStats(t *testing.T) {
	w := NewConnectionWriterWithCapacity(11, WriterCapacity{MaxItems: 2, MaxBytes: 32})
	if err := w.EnqueueKind(11, FrameKindFarmChange, []byte("ab")); err != nil {
		t.Fatal(err)
	}
	if err := w.EnqueueKind(11, FrameKindHint, []byte("cd")); err != nil {
		t.Fatal(err)
	}
	if err := w.EnqueueKind(11, FrameKindControl, []byte("ef")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("overflow error=%v, want ErrQueueFull", err)
	}
	stats := w.Stats()
	if stats.Items != 2 || stats.Bytes != 4 {
		t.Fatalf("items=%d bytes=%d, want 2/4", stats.Items, stats.Bytes)
	}
	if stats.EnqueuedItems[FrameKindFarmChange] != 1 || stats.EnqueuedItems[FrameKindHint] != 1 {
		t.Fatalf("enqueued items=%v", stats.EnqueuedItems)
	}
	if got := stats.RejectedKind(FrameKindControl, "reject", "full"); got != 1 {
		t.Fatalf("full rejects=%d, want 1", got)
	}
	// FIFO order is preserved: farm change first, hint second.
	first, ok := w.Dequeue()
	if !ok || first.Kind != FrameKindFarmChange {
		t.Fatalf("first item kind=%v", first.Kind)
	}
	second, ok := w.Dequeue()
	if !ok || second.Kind != FrameKindHint {
		t.Fatalf("second item kind=%v", second.Kind)
	}
}

func TestClassifyFrameKindTable(t *testing.T) {
	must := func(raw []byte) []byte { return raw }
	_ = must
	cases := []struct {
		name string
		raw  []byte
		want FrameKind
	}{
		{"hint", marshalPushFrame(t, commonpb.PushType_FRIEND_OPPORTUNITY_CHANGED), FrameKindHint},
		{"farm_change", marshalPushFrame(t, commonpb.PushType_FARM_CHANGE), FrameKindFarmChange},
		{"control", marshalPushFrame(t, commonpb.PushType_REJOIN_REQUIRED), FrameKindControl},
		{"session", marshalPushFrame(t, commonpb.PushType_FRIEND_ROSTER_CHANGED), FrameKindSession},
		{"system", marshalPushFrame(t, commonpb.PushType_SUBSCRIPTION_READY), FrameKindSystem},
	}
	for _, tc := range cases {
		if got := ClassifyFrameKind(tc.raw); got != tc.want {
			t.Fatalf("%s classified=%s, want %s", tc.name, got, tc.want)
		}
	}
}

// TestLevelTriggeredNotifierRemainsReadableUntilReset proves that a ready
// select branch cannot consume and lose the continuation signal.
func TestLevelTriggeredNotifierDrainsWithoutReEnqueue(t *testing.T) {
	var notifier LevelTriggeredNotifier
	notifier.Signal()
	if !notifier.Pending() {
		t.Fatal("signal must leave a pending notification")
	}
	// Repeated observations remain readable while work is pending. This models
	// select choosing a command response before choosing the downlink case.
	select {
	case <-notifier.WaitCh():
	default:
		t.Fatal("WaitCh after Signal must be readable")
	}
	select {
	case <-notifier.WaitCh():
	default:
		t.Fatal("pending notifier must remain readable until reset")
	}
	if !notifier.Pending() {
		t.Fatal("observation must not consume pending state")
	}
	notifier.Reset()
	select {
	case <-notifier.WaitCh():
		t.Fatal("reset notifier must return to an unreadable idle channel")
	default:
	}
	if notifier.Pending() {
		t.Fatal("reset notifier must not remain pending")
	}
	// A later signal wakes the fresh idle channel.
	notifier.Signal()
	select {
	case <-notifier.WaitCh():
	default:
		t.Fatal("second signal must wake a fresh wait channel")
	}
}

// TestConnectionWriterPendingReArmsAfterQuantumDrain models the WritePump
// quantum loop: enqueue many frames once, drain a quantum (2), then re-arm and
// verify Pending keeps the remaining frames draining with no further enqueue.
func TestConnectionWriterPendingReArmsAfterQuantumDrain(t *testing.T) {
	w := NewConnectionWriter(12, 64, 1<<20)
	for i := 0; i < 6; i++ {
		if err := w.EnqueueKind(12, FrameKindHint, []byte("h")); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}
	// Observe the first notification exactly as the pump does after subscribing.
	select {
	case <-w.NotifyChan():
	default:
		t.Fatal("first NotifyChan must be readable after enqueues")
	}
	var drained []FrameKind
	drain := func(quantum int) {
		for i := 0; i < quantum; i++ {
			item, ok := w.Dequeue()
			if !ok {
				return
			}
			drained = append(drained, item.Kind)
		}
	}
	drain(2)
	if !w.Pending() {
		t.Fatal("Pending must report queued frames after one quantum")
	}
	// The level remains readable while Pending reports queued frames, so the
	// next quantum drains without another enqueue.
	select {
	case <-w.NotifyChan():
	default:
		t.Fatal("re-armed NotifyChan must be readable after Pending")
	}
	drain(2)
	if !w.Pending() {
		t.Fatal("Pending must still report queued frames after two quanta")
	}
	select {
	case <-w.NotifyChan():
	default:
		t.Fatal("second re-arm must be readable")
	}
	drain(2)
	if w.Pending() {
		t.Fatal("Pending must be false once the queue is empty")
	}
	if len(drained) != 6 {
		t.Fatalf("drained=%d, want 6 (one enqueue burst, three quanta)", len(drained))
	}
}

// TestHintPolicyItemOnlyQuota verifies that a policy with only MaxHintItems set
// never rejects on the zero byte dimension.
func TestHintPolicyItemOnlyQuota(t *testing.T) {
	w := NewConnectionWriterWithCapacity(20, WriterCapacity{
		MaxItems: 64, MaxBytes: 1 << 20,
		Hint: HintPolicy{MaxHintItems: 2, MaxHintBytes: 0},
	})
	if err := w.EnqueueKind(20, FrameKindHint, []byte("long-bytes-ignored-by-item-only-policy")); err != nil {
		t.Fatalf("first hint: %v", err)
	}
	if err := w.EnqueueKind(20, FrameKindHint, []byte("another-long-bytes")); err != nil {
		t.Fatalf("second hint: %v", err)
	}
	if err := w.EnqueueKind(20, FrameKindHint, []byte("third")); !errors.Is(err, ErrHintQuota) {
		t.Fatalf("third hint error=%v, want ErrHintQuota (item quota)", err)
	}
	stats := w.Stats()
	if stats.HintItems != 2 {
		t.Fatalf("hint items=%d, want 2", stats.HintItems)
	}
	if got := stats.RejectedKind(FrameKindHint, "reject", "quota"); got != 1 {
		t.Fatalf("hint quota rejects=%d, want 1", got)
	}
}

// TestHintPolicyByteOnlyQuota verifies that a policy with only MaxHintBytes set
// never rejects on the zero item dimension.
func TestHintPolicyByteOnlyQuota(t *testing.T) {
	w := NewConnectionWriterWithCapacity(21, WriterCapacity{
		MaxItems: 64, MaxBytes: 1 << 20,
		Hint: HintPolicy{MaxHintItems: 0, MaxHintBytes: 5},
	})
	if err := w.EnqueueKind(21, FrameKindHint, []byte("ab")); err != nil {
		t.Fatalf("first hint: %v", err)
	}
	if err := w.EnqueueKind(21, FrameKindHint, []byte("cd")); err != nil {
		t.Fatalf("second hint: %v", err)
	}
	// Two items already hold 4 bytes; a third 2-byte hint would push the byte
	// total over the 5-byte cap even though the item dimension is disabled.
	if err := w.EnqueueKind(21, FrameKindHint, []byte("ef")); !errors.Is(err, ErrHintQuota) {
		t.Fatalf("third hint error=%v, want ErrHintQuota (byte quota)", err)
	}
	stats := w.Stats()
	if stats.HintItems != 2 {
		t.Fatalf("hint items=%d, want 2", stats.HintItems)
	}
	if got := stats.RejectedKind(FrameKindHint, "reject", "quota"); got != 1 {
		t.Fatalf("hint quota rejects=%d, want 1", got)
	}
}

// TestHintPolicyDisabled verifies that a zero HintPolicy rejects nothing: hints
// are bounded only by the total writer caps.
func TestHintPolicyDisabled(t *testing.T) {
	w := NewConnectionWriterWithCapacity(22, WriterCapacity{
		MaxItems: 4, MaxBytes: 1 << 20,
		Hint: HintPolicy{},
	})
	for i := 0; i < 4; i++ {
		if err := w.EnqueueKind(22, FrameKindHint, []byte("h")); err != nil {
			t.Fatalf("hint %d: %v", i, err)
		}
	}
	if err := w.EnqueueKind(22, FrameKindHint, []byte("h")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("fifth hint error=%v, want ErrQueueFull (total caps only)", err)
	}
	stats := w.Stats()
	if stats.HintItems != 4 {
		t.Fatalf("hint items=%d, want 4", stats.HintItems)
	}
	if got := stats.RejectedKind(FrameKindHint, "reject", "quota"); got != 0 {
		t.Fatalf("disabled policy quota rejects=%d, want 0", got)
	}
	if got := stats.RejectedKind(FrameKindHint, "reject", "full"); got != 1 {
		t.Fatalf("full rejects=%d, want 1", got)
	}
}

// TestWriterCapacityAPIReadyForProductionWiring pins the C6B writer seam that
// production command.Gate will adopt after review. command.Gate currently
// calls downlink.NewConnectionWriter(generation, 256, g.maxFrame); switching
// to NewConnectionWriterWithCapacity(generation, WriterCapacity{
// MaxItems: 256, MaxBytes: g.maxFrame, Hint: HintPolicy{...}})
// activates the hint soft quota without changing any other writer semantics.
// This test locks the constructor contract so the wiring change is a one-line
// diff with no behavioral surprises.
func TestWriterCapacityAPIReadyForProductionWiring(t *testing.T) {
	legacy := NewConnectionWriter(30, 256, 4096)
	capacity := NewConnectionWriterWithCapacity(30, WriterCapacity{
		MaxItems: 256, MaxBytes: 4096, Hint: HintPolicy{MaxHintItems: 1, MaxHintBytes: 1024},
	})
	if !legacy.HintCapacity().IsZero() {
		t.Fatal("legacy constructor must leave hint soft quota disabled")
	}
	if capacity.HintCapacity() != (HintPolicy{MaxHintItems: 1, MaxHintBytes: 1024}) {
		t.Fatalf("capacity constructor hint=%+v", capacity.HintCapacity())
	}
	// Identical total caps: both writers admit the same non-hint frames.
	for _, w := range []*ConnectionWriter{legacy, capacity} {
		if err := w.EnqueueKind(30, FrameKindFarmChange, []byte("change")); err != nil {
			t.Fatalf("farm change: %v", err)
		}
		if err := w.EnqueueKind(30, FrameKindHint, []byte("hint-a")); err != nil {
			t.Fatalf("first hint: %v", err)
		}
	}
	if err := legacy.EnqueueKind(30, FrameKindHint, []byte("hint-b")); err != nil {
		t.Fatalf("legacy second hint must be admitted (no soft quota): %v", err)
	}
	if err := capacity.EnqueueKind(30, FrameKindHint, []byte("hint-b")); !errors.Is(err, ErrHintQuota) {
		t.Fatalf("capacity second hint=%v, want ErrHintQuota", err)
	}
	// Total caps still apply identically on the byte dimension (4096).
	overflow := make([]byte, 4096)
	if err := legacy.EnqueueKind(30, FrameKindOther, overflow); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("legacy overflow=%v, want ErrQueueFull", err)
	}
	if err := capacity.EnqueueKind(30, FrameKindOther, overflow); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("capacity overflow=%v, want ErrQueueFull", err)
	}
}
