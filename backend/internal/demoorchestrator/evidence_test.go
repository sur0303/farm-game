package demoorchestrator

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"farmgame/internal/demokit"
)

func TestReadReplayUnderstandsPlanOnlyEvidence(t *testing.T) {
	output := filepath.Join(t.TempDir(), "plan-only")
	if _, err := demokit.Plan(demokit.PlanRequest{
		ScenarioID: "F2",
		OutputDir:  output,
		SourceRef:  "0c9cae3",
		CreatedAt:  time.Date(2026, 8, 12, 8, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatal(err)
	}
	replay, err := ReadReplay(output)
	if err != nil {
		t.Fatalf("ReadReplay() error = %v", err)
	}
	views := replay.RunViews()
	if len(views) != 1 {
		t.Fatalf("replay views = %#v, want one run", views)
	}
	view := views[0]
	if view.Mode != demokit.EvidenceModeReplay || view.Execution != "PLAN_ONLY" || view.ScenarioID != "F2" {
		t.Fatalf("replay view = %#v", view)
	}
	page := replay.Page(view.RunID, 0, 20)
	if len(page.Events) != 3 || page.Events[0].Kind != demokit.KindPlanStep || page.Events[0].Phase != "PLAN_ONLY" {
		t.Fatalf("plan replay events = %#v", page.Events)
	}
	if page.Events[0].Fields["executable"] != false {
		t.Fatalf("plan replay executable field = %#v", page.Events[0].Fields["executable"])
	}
}

func TestReadReplayLoadsEventsNDJSONAndFiniteSubscription(t *testing.T) {
	root := t.TempDir()
	bundle := filepath.Join(root, "run-1")
	if err := os.MkdirAll(bundle, 0o755); err != nil {
		t.Fatal(err)
	}
	lines := []demokit.Event{
		{TsMs: 10, RunID: "run-1", Kind: demokit.KindScenarioStarted, Phase: "START"},
		{TsMs: 11, RunID: "run-1", Kind: demokit.KindClientReady, Player: 7},
	}
	file, err := os.Create(filepath.Join(bundle, "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	for _, event := range lines {
		if err := encoder.Encode(event); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	replay, err := NewReplayEvidence(root, 10)
	if err != nil {
		t.Fatalf("NewReplayEvidence() error = %v", err)
	}
	view, ok := replay.RunView("run-1")
	if !ok || view.Mode != demokit.EvidenceModeReplay || view.EventCount != 2 {
		t.Fatalf("replay view = %#v, found=%v", view, ok)
	}
	subscription := replay.Subscribe("run-1", 0)
	if len(subscription.Initial) != 2 {
		t.Fatalf("replay initial = %#v", subscription.Initial)
	}
	select {
	case _, open := <-subscription.Events:
		if open {
			t.Fatal("replay subscription unexpectedly remained live")
		}
	default:
		t.Fatal("replay subscription was not closed")
	}
	subscription.Close()
	if got := replay.Stats().Subscribers; got != 0 {
		t.Fatalf("replay subscribers after finite Subscribe = %d, want 0", got)
	}
}

func TestReadReplayRejectsMalformedEvent(t *testing.T) {
	bundle := filepath.Join(t.TempDir(), "bad")
	if err := os.MkdirAll(bundle, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bundle, "events.ndjson"), []byte("{not-json}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadReplay(bundle); err == nil {
		t.Fatal("ReadReplay() error = nil, want malformed event error")
	}
}

func TestReadReplayRejectsManifestRunIDMismatch(t *testing.T) {
	bundle := filepath.Join(t.TempDir(), "mismatch")
	if err := os.MkdirAll(bundle, 0o755); err != nil {
		t.Fatal(err)
	}
	writeReplayJSON(t, filepath.Join(bundle, "run-manifest.json"), replayManifest{RunID: "run-manifest"})
	writeReplayEvents(t, filepath.Join(bundle, "events.ndjson"), []demokit.Event{{
		RunID: "run-other",
		Kind:  demokit.KindScenarioStarted,
	}})
	if _, err := ReadReplay(bundle); err == nil || !strings.Contains(err.Error(), "does not match bundle run_id") {
		t.Fatalf("ReadReplay() mismatch error = %v", err)
	}
}

func TestReadReplayEnforcesPerFileAndTotalByteLimits(t *testing.T) {
	t.Run("per file", func(t *testing.T) {
		bundle := filepath.Join(t.TempDir(), "per-file")
		if err := os.MkdirAll(bundle, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(bundle, "events.ndjson"), make([]byte, 65), 0o644); err != nil {
			t.Fatal(err)
		}
		limits := ReplayLimits{MaxFileBytes: 64, MaxTotalBytes: 1024, MaxEvents: 10, MaxBundles: 1}
		if _, err := NewReplayEvidenceWithLimits(bundle, 8, limits); err == nil || !strings.Contains(err.Error(), "per-file limit") {
			t.Fatalf("per-file limit error = %v", err)
		}
	})

	t.Run("total bytes", func(t *testing.T) {
		bundle := filepath.Join(t.TempDir(), "total")
		if err := os.MkdirAll(bundle, 0o755); err != nil {
			t.Fatal(err)
		}
		manifestPayload, err := json.Marshal(replayManifest{RunID: "run-total"})
		if err != nil {
			t.Fatal(err)
		}
		eventPayload, err := json.Marshal(demokit.Event{
			RunID:   "run-total",
			Kind:    demokit.KindStepCompleted,
			Message: "bounded total replay input",
		})
		if err != nil {
			t.Fatal(err)
		}
		eventPayload = append(eventPayload, byte(10))
		if err := os.WriteFile(filepath.Join(bundle, "run-manifest.json"), manifestPayload, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(bundle, "events.ndjson"), eventPayload, 0o644); err != nil {
			t.Fatal(err)
		}
		maxFileBytes := len(manifestPayload)
		if len(eventPayload) > maxFileBytes {
			maxFileBytes = len(eventPayload)
		}
		limits := ReplayLimits{
			MaxFileBytes:  int64(maxFileBytes),
			MaxTotalBytes: int64(len(manifestPayload) + len(eventPayload) - 1),
			MaxEvents:     10,
			MaxBundles:    1,
		}
		if _, err := NewReplayEvidenceWithLimits(bundle, 8, limits); err == nil || !strings.Contains(err.Error(), "input total exceeds") {
			t.Fatalf("total byte limit error = %v", err)
		}
	})
}

func TestReadReplayEnforcesEventAndBundleLimits(t *testing.T) {
	t.Run("events", func(t *testing.T) {
		bundle := filepath.Join(t.TempDir(), "events")
		if err := os.MkdirAll(bundle, 0o755); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(bundle, "events.ndjson")
		writeReplayEvents(t, path, []demokit.Event{
			{RunID: "run-limit", Kind: demokit.KindScenarioStarted},
			{RunID: "run-limit", Kind: demokit.KindStepCompleted},
		})
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		limits := ReplayLimits{
			MaxFileBytes:  info.Size() + 1,
			MaxTotalBytes: info.Size() + 1,
			MaxEvents:     1,
			MaxBundles:    1,
		}
		if _, err := NewReplayEvidenceWithLimits(bundle, 8, limits); err == nil || !strings.Contains(err.Error(), "event limit") {
			t.Fatalf("event limit error = %v", err)
		}
	})

	t.Run("bundles", func(t *testing.T) {
		root := t.TempDir()
		for _, name := range []string{"bundle-a", "bundle-b"} {
			bundle := filepath.Join(root, name)
			if err := os.MkdirAll(bundle, 0o755); err != nil {
				t.Fatal(err)
			}
			writeReplayEvents(t, filepath.Join(bundle, "events.ndjson"), []demokit.Event{{
				RunID: name,
				Kind:  demokit.KindScenarioStarted,
			}})
		}
		limits := ReplayLimits{MaxFileBytes: 1024, MaxTotalBytes: 2048, MaxEvents: 10, MaxBundles: 1}
		if _, err := NewReplayEvidenceWithLimits(root, 8, limits); err == nil || !strings.Contains(err.Error(), "bundle limit") {
			t.Fatalf("bundle limit error = %v", err)
		}
	})
}

func writeReplayJSON(t *testing.T, path string, value any) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(file).Encode(value); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeReplayEvents(t *testing.T, path string, events []demokit.Event) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	for _, event := range events {
		if err := encoder.Encode(event); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}
