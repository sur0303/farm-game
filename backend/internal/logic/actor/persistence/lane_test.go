package persistence

import (
	"errors"
	"testing"
)

func TestLaneDurableUnknownBlocksUntilResolution(t *testing.T) {
	lane := NewLane(Options{})
	if err := lane.MarkMemoryVersion(12); err != nil {
		t.Fatal(err)
	}
	token, err := lane.BeginDurable(12)
	if err != nil {
		t.Fatal(err)
	}
	if err := lane.Complete(token, CompletionTimeoutUnknown); err != nil {
		t.Fatal(err)
	}
	if got := lane.Snapshot(); got.State != ResolvingUnknown || got.SaveInflightVersion != 12 {
		t.Fatalf("snapshot=%+v", got)
	}
	if _, err := lane.BeginDurable(12); !errors.Is(err, ErrLaneBusy) {
		t.Fatalf("err=%v", err)
	}
	if err := lane.ResolveUnknown(token, true); err != nil {
		t.Fatal(err)
	}
	if got := lane.Snapshot(); got.State != Idle || got.PersistedThroughVersion != 12 || got.DurableThroughVersion != 12 || got.Dirty {
		t.Fatalf("snapshot=%+v", got)
	}
}

func TestLaneNotSentDoesNotAdvanceDurability(t *testing.T) {
	lane := NewLane(Options{})
	if err := lane.MarkMemoryVersion(4); err != nil {
		t.Fatal(err)
	}
	token, err := lane.BeginDurable(4)
	if err != nil {
		t.Fatal(err)
	}
	if err := lane.Complete(token, CompletionNotSent); err != nil {
		t.Fatal(err)
	}
	got := lane.Snapshot()
	if got.State != Idle || got.DurableThroughVersion != 0 || !got.Dirty {
		t.Fatalf("snapshot=%+v", got)
	}
}

func TestLaneRejectsStaleCallbackAndLowVersionRegression(t *testing.T) {
	lane := NewLane(Options{})
	if err := lane.MarkMemoryVersion(10); err != nil {
		t.Fatal(err)
	}
	token, err := lane.BeginDurable(10)
	if err != nil {
		t.Fatal(err)
	}
	if err := lane.Complete(token, CompletionCommitted); err != nil {
		t.Fatal(err)
	}
	if err := lane.MarkMemoryVersion(11); err != nil {
		t.Fatal(err)
	}
	if err := lane.Complete(token, CompletionCommitted); !errors.Is(err, ErrStaleCompletion) {
		t.Fatalf("stale completion err=%v", err)
	}
	if _, err := lane.BeginDurable(9); !errors.Is(err, ErrVersionRegression) {
		t.Fatalf("regression err=%v", err)
	}
}

func TestLaneSoftSaveIsFeatureGatedAndCannotOverlapBarrier(t *testing.T) {
	disabled := NewLane(Options{})
	if err := disabled.MarkMemoryVersion(1); err != nil {
		t.Fatal(err)
	}
	if _, err := disabled.BeginSoft(1); !errors.Is(err, ErrSoftSaveDisabled) {
		t.Fatalf("err=%v", err)
	}

	lane := NewLane(Options{EnableSoftSave: true})
	if err := lane.MarkMemoryVersion(10); err != nil {
		t.Fatal(err)
	}
	soft, err := lane.BeginSoft(10)
	if err != nil {
		t.Fatal(err)
	}
	if err := lane.MarkMemoryVersion(11); err != nil {
		t.Fatal(err)
	}
	if _, err := lane.BeginDurable(11); !errors.Is(err, ErrLaneBusy) {
		t.Fatalf("barrier must wait for soft save: %v", err)
	}
	if err := lane.Complete(soft, CompletionCommitted); err != nil {
		t.Fatal(err)
	}
	got := lane.Snapshot()
	if got.PersistedThroughVersion != 10 || got.MemoryStateVersion != 11 || !got.Dirty {
		t.Fatalf("snapshot=%+v", got)
	}
	durable, err := lane.BeginDurable(11)
	if err != nil {
		t.Fatal(err)
	}
	if err := lane.Complete(durable, CompletionCommitted); err != nil {
		t.Fatal(err)
	}
	if got := lane.Snapshot(); got.DurableThroughVersion != 11 || got.Dirty {
		t.Fatalf("snapshot=%+v", got)
	}
}

func TestLaneDrainOnlyStartsWhenIdle(t *testing.T) {
	lane := NewLane(Options{})
	if err := lane.MarkMemoryVersion(2); err != nil {
		t.Fatal(err)
	}
	token, err := lane.BeginDurable(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := lane.BeginDrain(); !errors.Is(err, ErrLaneBusy) {
		t.Fatalf("err=%v", err)
	}
	if err := lane.Complete(token, CompletionCommitted); err != nil {
		t.Fatal(err)
	}
	if err := lane.BeginDrain(); err != nil {
		t.Fatal(err)
	}
	if _, err := lane.BeginDurable(2); !errors.Is(err, ErrLaneDraining) {
		t.Fatalf("err=%v", err)
	}
}

func TestLaneDrainRejectsDirtyIdleState(t *testing.T) {
	lane := NewLane(Options{})
	if err := lane.MarkMemoryVersion(1); err != nil {
		t.Fatal(err)
	}
	if err := lane.BeginDrain(); !errors.Is(err, ErrLaneDirty) {
		t.Fatalf("err=%v", err)
	}
}

func TestRestorePersistedLogicalVersion(t *testing.T) {
	lane := NewLane(Options{EnableSoftSave: true})
	if err := lane.RestorePersistedVersion(9); err != nil {
		t.Fatal(err)
	}
	snapshot := lane.Snapshot()
	if snapshot.MemoryStateVersion != 9 || snapshot.PersistedThroughVersion != 9 || snapshot.Dirty {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	if err := lane.MarkMemoryVersion(10); err != nil {
		t.Fatal(err)
	}
	if _, err := lane.BeginSoft(10); err != nil {
		t.Fatal(err)
	}
}
