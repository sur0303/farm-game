package main

import (
	"path/filepath"
	"testing"
	"time"
)

func TestCoordinatorFilesLinearizeObservationWindow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "round.ready")
	if err := signalFile(path); err != nil {
		t.Fatal(err)
	}
	if err := waitForFile(path, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

func TestCountClosedUsesPerConnectionAtomicState(t *testing.T) {
	bots := []*bot{{}, {}, {}}
	bots[0].closed.Store(true)
	bots[2].closed.Store(true)
	if got := countClosed(bots); got != 2 {
		t.Fatalf("closed=%d want=2", got)
	}
}
