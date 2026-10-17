//go:build linux

package routecontrol

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestTimelinePartialWritePoisonsAndNeverInstallsManifest(t *testing.T) {
	oldWrite := timelineWrite
	defer func() { timelineWrite = oldWrite }()
	injected := errors.New("injected partial write")
	timelineWrite = func(_ *os.File, data []byte) (int, error) {
		return len(data) / 2, injected
	}

	timelineDir := filepath.Join(t.TempDir(), "run")
	timeline, err := NewTimeline(timelineDir, testProvenance(EvidenceE1, SourceSelfCheck))
	if err != nil {
		t.Fatal(err)
	}
	if err := timeline.RecordRevision(RevisionObservation{
		Domain: "player",
		Shard:  "7",
		Source: "fault",
	}); !errors.Is(err, injected) {
		t.Fatalf("partial write error=%v, want injected error", err)
	}
	if err := timeline.RecordRevision(RevisionObservation{
		Domain: "player",
		Shard:  "7",
		Source: "after-poison",
	}); !errors.Is(err, ErrTimelinePoisoned) {
		t.Fatalf("record after partial write error=%v, want ErrTimelinePoisoned", err)
	}
	if err := timeline.FinalizeManifest(timelineManifest(timeline)); !errors.Is(err, ErrTimelinePoisoned) {
		t.Fatalf("finalize after partial write error=%v, want ErrTimelinePoisoned", err)
	}
	assertTimelineFilesClosed(t, timeline)
	if _, err := os.Stat(filepath.Join(timelineDir, "run-manifest.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("poisoned timeline installed a manifest: %v", err)
	}
}

func TestTimelineSyncFaultPoisonsAndClosesAllFiles(t *testing.T) {
	oldSync := timelineSync
	defer func() { timelineSync = oldSync }()
	injected := errors.New("injected sync failure")
	timelineSync = func(*os.File) error {
		return injected
	}

	timelineDir := filepath.Join(t.TempDir(), "run")
	timeline, err := NewTimeline(timelineDir, testProvenance(EvidenceE1, SourceSelfCheck))
	if err != nil {
		t.Fatal(err)
	}
	if err := timeline.Flush(); !errors.Is(err, injected) {
		t.Fatalf("flush error=%v, want injected error", err)
	}
	if err := timeline.RecordFault(FaultObservation{
		Name: "after-poison", Action: "record", Phase: FaultShutdown,
	}); !errors.Is(err, ErrTimelinePoisoned) {
		t.Fatalf("record after sync fault error=%v, want ErrTimelinePoisoned", err)
	}
	if err := timeline.FinalizeManifest(timelineManifest(timeline)); !errors.Is(err, ErrTimelinePoisoned) {
		t.Fatalf("finalize after sync fault error=%v, want ErrTimelinePoisoned", err)
	}
	assertTimelineFilesClosed(t, timeline)
	if _, err := os.Stat(filepath.Join(timelineDir, "run-manifest.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sync-poisoned timeline installed a manifest: %v", err)
	}
}

func TestTimelineHashAndSeekFaultsPoisonFinalization(t *testing.T) {
	tests := []struct {
		name string
		set  func(error)
	}{
		{
			name: "hash",
			set: func(injected error) {
				timelineCopy = func(io.Writer, io.Reader) (int64, error) {
					return 0, injected
				}
			},
		},
		{
			name: "seek",
			set: func(injected error) {
				timelineSeek = func(*os.File, int64, int) (int64, error) {
					return 0, injected
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldCopy := timelineCopy
			oldSeek := timelineSeek
			defer func() {
				timelineCopy = oldCopy
				timelineSeek = oldSeek
			}()
			injected := errors.New("injected " + test.name + " failure")
			test.set(injected)

			timelineDir := filepath.Join(t.TempDir(), "run")
			timeline, err := NewTimeline(timelineDir, testProvenance(EvidenceE1, SourceSelfCheck))
			if err != nil {
				t.Fatal(err)
			}
			if err := timeline.RecordRevision(RevisionObservation{
				Domain: "player", Shard: "7", Source: "hash-input",
			}); err != nil {
				t.Fatal(err)
			}
			if err := timeline.FinalizeManifest(timelineManifest(timeline)); !errors.Is(err, ErrTimelinePoisoned) {
				t.Fatalf("finalize error=%v, want ErrTimelinePoisoned", err)
			}
			assertTimelineFilesClosed(t, timeline)
			if _, err := os.Stat(filepath.Join(timelineDir, "run-manifest.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s-poisoned timeline installed a manifest: %v", test.name, err)
			}
		})
	}
}

func timelineManifest(timeline *Timeline) Manifest {
	provenance := timeline.provenance
	return Manifest{
		Schema:        SchemaVersion,
		TaskID:        provenance.TaskID,
		RunID:         provenance.RunID,
		BaseCommit:    provenance.BaseCommit,
		HeadCommit:    provenance.HeadCommit,
		Branch:        provenance.Branch,
		Worktree:      provenance.Worktree,
		EvidenceLevel: EvidenceE1,
		Status:        StatusPass,
		Reason:        "timeline fault seam",
		Provenance:    provenance,
	}
}

func assertTimelineFilesClosed(t *testing.T, timeline *Timeline) {
	t.Helper()
	for _, stream := range streamOrder {
		if timeline.files[stream] != nil {
			t.Fatalf("%s file remained open after finalization", stream)
		}
	}
}
