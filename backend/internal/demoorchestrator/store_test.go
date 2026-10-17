package demoorchestrator

import (
	"errors"
	"sync"
	"testing"

	"farmgame/internal/demokit"
)

func TestEventStoreIsBoundedAndReportsGap(t *testing.T) {
	store := NewEventStore(3)
	for index := 0; index < 5; index++ {
		if _, err := store.Append(demokit.Event{
			Seq:     900,
			TsMs:    int64(index + 1),
			RunID:   "run-1",
			Kind:    demokit.KindStepCompleted,
			Version: uint64(index + 1),
		}); err != nil {
			t.Fatalf("Append(%d) error = %v", index, err)
		}
	}

	page := store.Page("run-1", 0, 10)
	if len(page.Events) != 3 || page.Events[0].Seq != 3 || page.Events[2].Seq != 5 {
		t.Fatalf("retained page = %#v, want seq 3..5", page)
	}
	if !page.Gap || page.OldestSeq != 3 || page.LatestSeq != 5 {
		t.Fatalf("page gap metadata = %#v", page)
	}
	view, ok := store.RunView("run-1")
	if !ok {
		t.Fatal("RunView() did not find appended run")
	}
	if view.EventCount != 5 || view.RetainedEventCount != 3 || view.DroppedEventCount != 2 || view.HeadVersion != 5 {
		t.Fatalf("view = %#v", view)
	}

	resumed := store.Page("run-1", 2, 10)
	if resumed.Gap || len(resumed.Events) != 3 || resumed.Events[0].Seq != 3 {
		t.Fatalf("resumed page = %#v", resumed)
	}
}

func TestEventStoreDefensivelyCopiesFieldsAndRejectsInvalidEvents(t *testing.T) {
	store := NewEventStore(4)
	fields := map[string]any{"nested": map[string]any{"value": "before"}}
	appended, err := store.Append(demokit.Event{
		RunID:  "run-copy",
		Kind:   demokit.KindProbeFarm,
		Fields: fields,
	})
	if err != nil {
		t.Fatal(err)
	}
	fields["nested"].(map[string]any)["value"] = "input-mutated"
	appended.Fields["nested"].(map[string]any)["value"] = "return-mutated"
	page := store.Page("run-copy", 0, 10)
	if got := page.Events[0].Fields["nested"].(map[string]any)["value"]; got != "before" {
		t.Fatalf("stored field = %v, want before", got)
	}
	if _, err := store.Append(demokit.Event{RunID: "", Kind: demokit.KindProbeFarm}); err == nil {
		t.Fatal("Append() accepted an event without run_id")
	}
	if _, err := store.Append(demokit.Event{RunID: "run-copy", Kind: demokit.EventKind(" ")}); err == nil {
		t.Fatal("Append() accepted an event without kind")
	}
}

func TestEventStoreSubscriptionReplaysThenBroadcasts(t *testing.T) {
	store := NewEventStore(8)
	first, err := store.Append(demokit.Event{RunID: "run-1", Kind: demokit.KindScenarioStarted})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(demokit.Event{RunID: "run-2", Kind: demokit.KindScenarioStarted}); err != nil {
		t.Fatal(err)
	}
	subscription := store.Subscribe("run-1", first.Seq)
	defer subscription.Close()
	if len(subscription.Initial) != 0 {
		t.Fatalf("initial replay = %#v, want empty after first", subscription.Initial)
	}
	want, err := store.Append(demokit.Event{RunID: "run-1", Kind: demokit.KindClientReady})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-subscription.Events:
		if got.Seq != want.Seq || got.RunID != "run-1" {
			t.Fatalf("broadcast = %#v, want %#v", got, want)
		}
	default:
		t.Fatal("subscription did not receive appended event")
	}
	if got := store.Stats().Subscribers; got != 1 {
		t.Fatalf("subscribers before close = %d, want 1", got)
	}
	subscription.Close()
	if got := store.Stats().Subscribers; got != 0 {
		t.Fatalf("subscribers after close = %d, want 0", got)
	}
}

func TestSlowSubscriberIsExplicitlyDisconnected(t *testing.T) {
	store := NewEventStore(100)
	subscription := store.Subscribe("", 0)
	for index := 0; index < subscriberBufferSize+1; index++ {
		if _, err := store.Append(demokit.Event{RunID: "run-slow", Kind: demokit.KindStepCompleted}); err != nil {
			t.Fatal(err)
		}
	}
	if got := store.Stats().Subscribers; got != 0 {
		t.Fatalf("slow subscriber count = %d, want explicit disconnect", got)
	}
	select {
	case _, open := <-subscription.Events:
		if !open {
			return
		}
	default:
		// The channel may still contain its bounded buffer, but the registry
		// has already removed the subscriber and no further events are sent.
	}
}

func TestEventStoreConcurrentAppend(t *testing.T) {
	store := NewEventStore(1000)
	var wait sync.WaitGroup
	for worker := 0; worker < 20; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := 0; index < 50; index++ {
				if _, err := store.Append(demokit.Event{RunID: "run-concurrent", Kind: demokit.KindStepCompleted}); err != nil {
					t.Errorf("Append() error = %v", err)
					return
				}
			}
		}()
	}
	wait.Wait()
	view, ok := store.RunView("run-concurrent")
	if !ok || view.EventCount != 1000 || view.RetainedEventCount != 1000 {
		t.Fatalf("concurrent view = %#v, found=%v", view, ok)
	}
}

func TestNewEventStoreCheckedRejectsInvalidMode(t *testing.T) {
	store, err := NewEventStoreChecked(4, demokit.EvidenceMode("INVALID"))
	if store != nil || !errors.Is(err, ErrInvalidMode) {
		t.Fatalf("NewEventStoreChecked() = %#v, %v; want ErrInvalidMode", store, err)
	}
	replay, err := NewEventStoreChecked(4, demokit.EvidenceModeReplay)
	if err != nil || replay.Mode() != demokit.EvidenceModeReplay {
		t.Fatalf("checked REPLAY store = %#v, %v", replay, err)
	}
}

func TestEventStoreCapacityBoundary(t *testing.T) {
	checked, err := NewEventStoreChecked(MaxEventCapacity, demokit.EvidenceModeLive)
	if err != nil {
		t.Fatalf("NewEventStoreChecked(max) error = %v", err)
	}
	if checked.Capacity() != MaxEventCapacity {
		t.Fatalf("checked capacity = %d, want %d", checked.Capacity(), MaxEventCapacity)
	}

	oversized, err := NewEventStoreChecked(MaxEventCapacity+1, demokit.EvidenceModeLive)
	if oversized != nil || !errors.Is(err, ErrEventCapacityExceeded) {
		t.Fatalf("NewEventStoreChecked(over max) = %#v, %v; want ErrEventCapacityExceeded", oversized, err)
	}

	compatibility := NewEventStore(MaxEventCapacity + 1)
	if compatibility.Capacity() != MaxEventCapacity {
		t.Fatalf("compatibility capacity = %d, want clamp %d", compatibility.Capacity(), MaxEventCapacity)
	}

	replayCompatibility := NewEventStoreWithMode(MaxEventCapacity+1, demokit.EvidenceModeReplay)
	if replayCompatibility.Capacity() != MaxEventCapacity || replayCompatibility.Mode() != demokit.EvidenceModeReplay {
		t.Fatalf("REPLAY compatibility store mode=%s capacity=%d", replayCompatibility.Mode(), replayCompatibility.Capacity())
	}
}
