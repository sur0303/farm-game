//go:build linux

package routecontrol

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTimelineHasIndependentStreamsAndRecordsOnlyFaultPhase(t *testing.T) {
	root := t.TempDir()
	provenance := testProvenance(EvidenceE1, SourceSelfCheck)
	runDir := filepath.Join(root, "run")
	timeline, err := NewTimelineWithOptions(runDir, TimelineOptions{
		Provenance: provenance,
		Now:        func() time.Time { return time.Unix(1_900_000_001, 0) },
		Redactor:   NewRedactor("secret-note"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := timeline.RecordRevision(RevisionObservation{
		Domain:   "player",
		Shard:    "7",
		Revision: 1,
		Source:   "self-check",
	}); err != nil {
		t.Fatal(err)
	}
	if err := timeline.RecordAuthority(AuthorityObservation{
		Operation:    "claim",
		TransitionID: "transition-1",
		State:        "observed",
		Revision:     1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := timeline.RecordHolder(HolderObservation{
		HolderID:    "holder-a",
		BootID:      "boot-a",
		State:       "active",
		WriterEpoch: 2,
		LeaseEpoch:  3,
	}); err != nil {
		t.Fatal(err)
	}
	if err := timeline.RecordFault(FaultObservation{
		Name:   "renew",
		Action: "drop-response",
		Phase:  FaultResponseLost,
		Note:   "secret-note",
	}); err != nil {
		t.Fatal(err)
	}
	if err := timeline.Close(); err != nil {
		t.Fatal(err)
	}

	for _, stream := range streamOrder {
		path := filepath.Join(runDir, string(stream)+".ndjson")
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) == 0 {
			t.Fatalf("%s stream is empty", stream)
		}
		if stream == StreamFault {
			text := string(raw)
			if strings.Contains(text, "secret-note") || strings.Contains(text, `"verdict"`) {
				t.Fatalf("fault stream leaked secret or verdict: %s", text)
			}
		}
	}
	if err := timeline.RecordRevision(RevisionObservation{
		Domain: "player",
		Shard:  "7",
		Source: "late",
	}); err == nil {
		t.Fatal("record after close unexpectedly succeeded")
	}
}

func TestTimelineConcurrentRecordsKeepPerStreamSequences(t *testing.T) {
	root := t.TempDir()
	runDir := filepath.Join(root, "run")
	timeline, err := NewTimeline(runDir, testProvenance(EvidenceE1, SourceSelfCheck))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(4)
		go func(i int) {
			defer wg.Done()
			_ = timeline.RecordRevision(RevisionObservation{
				Domain: "player", Shard: "revision", Revision: uint64(i + 1), Source: "unit",
			})
		}(i)
		go func(i int) {
			defer wg.Done()
			_ = timeline.RecordAuthority(AuthorityObservation{
				Operation: "observe", TransitionID: "authority", State: "seen", Revision: uint64(i + 1),
			})
		}(i)
		go func(i int) {
			defer wg.Done()
			_ = timeline.RecordHolder(HolderObservation{
				HolderID: "holder", BootID: "boot", State: "active", WriterEpoch: uint64(i + 1),
			})
		}(i)
		go func(i int) {
			defer wg.Done()
			_ = timeline.RecordFault(FaultObservation{
				Name: "fault", Action: "record", Phase: FaultInFlight, Note: "unit",
			})
		}(i)
	}
	wg.Wait()
	if err := timeline.Close(); err != nil {
		t.Fatal(err)
	}

	for _, stream := range streamOrder {
		file, err := os.Open(filepath.Join(runDir, string(stream)+".ndjson"))
		if err != nil {
			t.Fatal(err)
		}
		scanner := bufio.NewScanner(file)
		var sequences []uint64
		for scanner.Scan() {
			var envelope timelineEnvelope
			if err := json.Unmarshal(scanner.Bytes(), &envelope); err != nil {
				t.Fatal(err)
			}
			if envelope.Stream != stream {
				t.Fatalf("%s envelope stream=%s", stream, envelope.Stream)
			}
			sequences = append(sequences, envelope.Sequence)
		}
		_ = file.Close()
		if err := scanner.Err(); err != nil {
			t.Fatal(err)
		}
		if len(sequences) != 100 {
			t.Fatalf("%s records=%d want 100", stream, len(sequences))
		}
		seen := make(map[uint64]bool, len(sequences))
		for _, sequence := range sequences {
			seen[sequence] = true
		}
		for sequence := uint64(1); sequence <= 100; sequence++ {
			if !seen[sequence] {
				t.Fatalf("%s missing sequence %d", stream, sequence)
			}
		}
	}
}

func TestTimelineRejectsUnknownFaultPhase(t *testing.T) {
	root := t.TempDir()
	timeline, err := NewTimeline(filepath.Join(root, "run"), testProvenance(EvidenceE1, SourceSelfCheck))
	if err != nil {
		t.Fatal(err)
	}
	defer timeline.Close()
	err = timeline.RecordFault(FaultObservation{Name: "x", Action: "y", Phase: FaultPhase("decide")})
	if err == nil {
		t.Fatal("unknown fault phase accepted")
	}
	if errors.Is(err, ErrInvalidEvidence) {
		t.Fatal("fault phase error should remain observation-local")
	}
}

func TestTimelineRejectsExistingSymlinkAndConcurrentRunDirectory(t *testing.T) {
	root := t.TempDir()
	provenance := testProvenance(EvidenceE1, SourceSelfCheck)
	runDir := filepath.Join(root, "run")
	timeline, err := NewTimeline(runDir, provenance)
	if err != nil {
		t.Fatal(err)
	}
	if err := timeline.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := NewTimeline(runDir, provenance); !errors.Is(err, ErrRunAlreadyExists) {
		t.Fatalf("existing run error=%v, want ErrRunAlreadyExists", err)
	}

	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := NewTimeline(link, provenance); !errors.Is(err, ErrRunAlreadyExists) {
		t.Fatalf("symlink run error=%v, want ErrRunAlreadyExists", err)
	}

	concurrentDir := filepath.Join(root, "concurrent")
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			created, err := NewTimeline(concurrentDir, provenance)
			if err == nil {
				err = created.Close()
			}
			results <- err
		}()
	}
	var successes, alreadyExists int
	for i := 0; i < 2; i++ {
		err := <-results
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrRunAlreadyExists):
			alreadyExists++
		default:
			t.Fatalf("concurrent run error=%v", err)
		}
	}
	if successes != 1 || alreadyExists != 1 {
		t.Fatalf("concurrent runs successes=%d alreadyExists=%d", successes, alreadyExists)
	}
}

func TestTimelineRedactsProvenanceAndObservationFreeText(t *testing.T) {
	root := t.TempDir()
	provenance := testProvenance(EvidenceE1, SourceSelfCheck)
	provenance.Worktree = "https://user:developer_004@example.invalid:443/run?token=top-secret"
	timeline, err := NewTimelineWithOptions(filepath.Join(root, "run"), TimelineOptions{
		Provenance: provenance,
		Redactor:   NewRedactor("top-secret"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := timeline.RecordRevision(RevisionObservation{
		Domain: "Authorization: Bearer top-secret",
		Shard:  "198.18.0.205:6379",
		Source: "https://user:developer_004@example.invalid:443/path?api-key=top-secret",
	}); err != nil {
		t.Fatal(err)
	}
	paths := timeline.Paths()
	if err := timeline.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		for _, forbidden := range []string{"top-secret", "password", "real.example", "198.18.0.205"} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("timeline %s leaked %q: %s", path, forbidden, text)
			}
		}
	}
}

func TestTimelineDoesNotCreateRunBelowSwappedSymlinkParent(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "runs")
	outside := t.TempDir()
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	swap := filepath.Join(root, "runs-swap")
	if err := os.Symlink(outside, swap); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = os.Remove(parent)
			_ = os.Rename(swap, parent)
			_ = os.Remove(parent)
			_ = os.Mkdir(parent, 0o700)
			_ = os.Symlink(outside, swap)
		}
	}()
	defer func() {
		close(stop)
		wg.Wait()
	}()

	for i := 0; i < 100; i++ {
		runDir := filepath.Join(parent, fmt.Sprintf("run-%03d", i))
		timeline, err := NewTimeline(runDir, testProvenance(EvidenceE1, SourceSelfCheck))
		if err == nil {
			if err := timeline.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	entries, err := os.ReadDir(outside)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("timeline created output outside root: %v", entries)
	}
}

func TestTimelineFinalizeUsesHeldFilesAfterRenameAndRecreate(t *testing.T) {
	root := t.TempDir()
	runDir := filepath.Join(root, "run")
	provenance := testProvenance(EvidenceE1, SourceSelfCheck)
	timeline, err := NewTimeline(runDir, provenance)
	if err != nil {
		t.Fatal(err)
	}
	if err := timeline.RecordRevision(RevisionObservation{
		Domain: "player", Shard: "7", Revision: 1, Source: "original",
	}); err != nil {
		t.Fatal(err)
	}

	originalDir := filepath.Join(root, "run-original")
	if err := os.Rename(runDir, originalDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(runDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, stream := range streamOrder {
		if err := os.WriteFile(filepath.Join(runDir, string(stream)+".ndjson"), []byte("replacement\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	manifest := Manifest{
		Schema:        SchemaVersion,
		TaskID:        provenance.TaskID,
		RunID:         provenance.RunID,
		BaseCommit:    provenance.BaseCommit,
		HeadCommit:    provenance.HeadCommit,
		Branch:        provenance.Branch,
		Worktree:      provenance.Worktree,
		EvidenceLevel: EvidenceE1,
		Status:        StatusPass,
		Reason:        "held capability finalization",
		Provenance:    provenance,
	}
	if err := timeline.FinalizeManifest(manifest); err != nil {
		t.Fatal(err)
	}

	manifestPath := filepath.Join(originalDir, "run-manifest.json")
	loaded, err := ReadManifest(originalDir, manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := loaded.ValidateArtifacts(originalDir); err != nil {
		t.Fatal(err)
	}
	originalArtifact, err := ComputeArtifact(originalDir, "revision.ndjson", "revision.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Artifacts[0].SHA256 != originalArtifact.SHA256 {
		t.Fatalf("finalizer hashed replacement file: got=%s want=%s", loaded.Artifacts[0].SHA256, originalArtifact.SHA256)
	}
	if _, err := os.Stat(filepath.Join(runDir, "run-manifest.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement run directory received manifest: %v", err)
	}
	if err := timeline.RecordRevision(RevisionObservation{
		Domain: "player", Shard: "7", Revision: 2, Source: "late",
	}); err == nil {
		t.Fatal("record after finalization unexpectedly succeeded")
	}
}

func TestTimelineFinalizeRejectsCallerArtifactsAndClosesOnError(t *testing.T) {
	root := t.TempDir()
	timeline, err := NewTimeline(filepath.Join(root, "run"), testProvenance(EvidenceE1, SourceSelfCheck))
	if err != nil {
		t.Fatal(err)
	}
	err = timeline.FinalizeManifest(Manifest{
		Artifacts: []Artifact{{
			Name: "caller", Path: "caller.ndjson", Bytes: 0, SHA256: strings.Repeat("a", 64),
		}},
	})
	if !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("finalizer artifact injection error=%v, want ErrInvalidManifest", err)
	}
	if err := timeline.Close(); err != nil {
		t.Fatalf("Close after failed finalization: %v", err)
	}
	for _, stream := range streamOrder {
		if timeline.files[stream] != nil {
			t.Fatalf("%s file capability remained open after finalization error", stream)
		}
	}
	if err := timeline.RecordFault(FaultObservation{
		Name: "late", Action: "record", Phase: FaultShutdown,
	}); err == nil {
		t.Fatal("timeline remained open after failed finalization")
	}
}
