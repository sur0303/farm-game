//go:build linux

package routecontrol

import (
	"path/filepath"
	"testing"
)

func TestTimelineFinalizationClosesLinuxDirectoryCapabilities(t *testing.T) {
	timeline, err := NewTimeline(filepath.Join(t.TempDir(), "run"), testProvenance(EvidenceE1, SourceSelfCheck))
	if err != nil {
		t.Fatal(err)
	}
	provenance := timeline.provenance
	if err := timeline.FinalizeManifest(Manifest{
		Schema:        SchemaVersion,
		TaskID:        provenance.TaskID,
		RunID:         provenance.RunID,
		BaseCommit:    provenance.BaseCommit,
		HeadCommit:    provenance.HeadCommit,
		Branch:        provenance.Branch,
		Worktree:      provenance.Worktree,
		EvidenceLevel: EvidenceE1,
		Status:        StatusPass,
		Reason:        "fd closure",
		Provenance:    provenance,
	}); err != nil {
		t.Fatal(err)
	}
	runDir, ok := timeline.runDir.(*linuxSecureRunDir)
	if !ok {
		t.Fatalf("run directory type=%T, want *linuxSecureRunDir", timeline.runDir)
	}
	if runDir.dir != nil || runDir.parent != nil {
		t.Fatalf("directory capabilities remained open: dir=%v parent=%v", runDir.dir, runDir.parent)
	}
}
