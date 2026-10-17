//go:build linux

package routecontrol

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestManifestRequiredFieldsAndArtifactDigest(t *testing.T) {
	root := t.TempDir()
	timelinePath := filepath.Join(root, "timeline.ndjson")
	if err := os.WriteFile(timelinePath, []byte(`{"stream":"revision"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := ComputeArtifact(root, "timeline.ndjson", "timeline")
	if err != nil {
		t.Fatal(err)
	}
	manifest := validManifest(artifact)
	if err := manifest.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := manifest.ValidateArtifacts(root); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(root, "run-manifest.json")
	if err := WriteManifest(root, path, manifest); err != nil {
		t.Fatal(err)
	}
	loaded, err := ReadManifest(root, path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Artifacts[0].SHA256 != artifact.SHA256 {
		t.Fatalf("loaded sha=%s want %s", loaded.Artifacts[0].SHA256, artifact.SHA256)
	}

	if err := os.WriteFile(timelinePath, []byte(`{"stream":"revision","changed":true}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(manifest.ValidateArtifacts(root), ErrArtifactMismatch) {
		t.Fatal("artifact drift was not rejected")
	}
}

func TestManifestRejectsMissingAndUnsafeFields(t *testing.T) {
	root := t.TempDir()
	artifact, err := ComputeArtifact(root, "missing-placeholder", "placeholder")
	if err == nil {
		t.Fatal("missing artifact unexpectedly hashed")
	}
	_ = artifact

	invalid := Manifest{}
	if !errors.Is(invalid.Validate(), ErrInvalidManifest) {
		t.Fatal("empty manifest was accepted")
	}

	artifact = Artifact{
		Name:   "a",
		Path:   "../secret",
		Bytes:  0,
		SHA256: strings.Repeat("d", 64),
	}
	manifest := validManifest(artifact)
	if !errors.Is(manifest.Validate(), ErrInvalidManifest) {
		t.Fatal("path traversal artifact was accepted")
	}
}

func TestComputeArtifactRejectsSymlinkOutsideRootAndNonRegularFile(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "outside.txt")
	if err := os.WriteFile(outsideFile, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link.txt")
	if err := os.Symlink(outsideFile, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := ComputeArtifact(root, "link.txt", "link"); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("file symlink error=%v, want ErrUnsafePath", err)
	}

	outsideDir := filepath.Join(outside, "dir")
	if err := os.Mkdir(outsideDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outsideDir, "nested.txt"), []byte("nested"), 0o600); err != nil {
		t.Fatal(err)
	}
	dirLink := filepath.Join(root, "dir-link")
	if err := os.Symlink(outsideDir, dirLink); err != nil {
		t.Skipf("directory symlink unavailable: %v", err)
	}
	if _, err := ComputeArtifact(root, "dir-link/nested.txt", "nested"); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("directory symlink error=%v, want ErrUnsafePath", err)
	}

	if err := os.Mkdir(filepath.Join(root, "directory"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := ComputeArtifact(root, "directory", "directory"); !errors.Is(err, ErrArtifactMismatch) {
		t.Fatalf("directory artifact error=%v, want ErrArtifactMismatch", err)
	}
}

func TestManifestRejectsUppercaseAndDuplicateArtifactDigests(t *testing.T) {
	artifact := Artifact{
		Name:   "a",
		Path:   "a.ndjson",
		Bytes:  1,
		SHA256: strings.Repeat("A", 64),
	}
	if err := validManifest(artifact).Validate(); !errors.Is(err, ErrInvalidManifest) {
		t.Fatal("uppercase digest was accepted")
	}

	artifact = Artifact{Name: "a", Path: "a.ndjson", Bytes: 1, SHA256: strings.Repeat("e", 64)}
	manifest := validManifest(artifact)
	manifest.Artifacts = append(manifest.Artifacts, artifact)
	if err := manifest.Validate(); !errors.Is(err, ErrInvalidManifest) {
		t.Fatal("duplicate artifact was accepted")
	}
}

func TestManifestBindsTopLevelAndNestedProvenanceAndRequiresReason(t *testing.T) {
	artifact := Artifact{Name: "a", Path: "a.ndjson", Bytes: 1, SHA256: strings.Repeat("e", 64)}
	manifest := validManifest(artifact)
	manifest.Reason = ""
	if !errors.Is(manifest.Validate(), ErrInvalidManifest) {
		t.Fatal("empty reason was accepted")
	}

	manifest = validManifest(artifact)
	manifest.HeadCommit = strings.Repeat("c", 40)
	if !errors.Is(manifest.Validate(), ErrInvalidManifest) {
		t.Fatal("top-level head commit drift was accepted")
	}

	manifest = validManifest(artifact)
	manifest.Provenance.TaskID = "other-task"
	if !errors.Is(manifest.Validate(), ErrInvalidManifest) {
		t.Fatal("nested task binding drift was accepted")
	}
}

func TestSyntheticProvenanceCannotEscalate(t *testing.T) {
	provenance := testProvenance(EvidenceE3, SourceRealProcess)
	provenance.BaseCommit = SyntheticE1Commit
	provenance.HeadCommit = SyntheticE1Commit
	provenance.Synthetic = true
	claim := Claim{
		Level:      EvidenceE3,
		Status:     StatusInconclusive,
		Reason:     "synthetic input",
		Provenance: provenance,
	}
	if !errors.Is(claim.Validate(), ErrEvidenceEscalation) {
		t.Fatal("synthetic E1 provenance escalated to E3")
	}
}

func TestWriteAndReadManifestForceArtifactHashAndRejectExistingOutput(t *testing.T) {
	root := t.TempDir()
	artifactPath := filepath.Join(root, "artifact.ndjson")
	if err := os.WriteFile(artifactPath, []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := ComputeArtifact(root, "artifact.ndjson", "artifact")
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "run-manifest.json")
	manifest := validManifest(artifact)
	if err := WriteManifest(root, manifestPath, manifest); err != nil {
		t.Fatal(err)
	}
	badDigest := manifest
	badDigest.Artifacts = append([]Artifact(nil), manifest.Artifacts...)
	badDigest.Artifacts[0].SHA256 = strings.Repeat("0", 64)
	if err := WriteManifest(root, filepath.Join(root, "bad-manifest.json"), badDigest); !errors.Is(err, ErrArtifactMismatch) {
		t.Fatalf("write accepted wrong artifact SHA: %v", err)
	}
	first, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteManifest(root, manifestPath, manifest); !errors.Is(err, ErrRunAlreadyExists) {
		t.Fatalf("second write error=%v, want ErrRunAlreadyExists", err)
	}
	second, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("existing manifest was overwritten")
	}

	if err := os.WriteFile(artifactPath, []byte("two\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadManifest(root, manifestPath); !errors.Is(err, ErrArtifactMismatch) {
		t.Fatalf("read accepted artifact drift: %v", err)
	}
	if err := os.Remove(artifactPath); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadManifest(root, manifestPath); !errors.Is(err, ErrArtifactMismatch) {
		t.Fatalf("read accepted missing artifact: %v", err)
	}
}

func TestManifestOutputRejectsSymlinkTarget(t *testing.T) {
	root := t.TempDir()
	artifactPath := filepath.Join(root, "artifact.ndjson")
	if err := os.WriteFile(artifactPath, []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := ComputeArtifact(root, "artifact.ndjson", "artifact")
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "run-manifest.json")
	if err := os.Symlink(outside, manifestPath); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if err := WriteManifest(root, manifestPath, validManifest(artifact)); !errors.Is(err, ErrRunAlreadyExists) {
		t.Fatalf("symlink target error=%v, want ErrRunAlreadyExists", err)
	}
	raw, err := os.ReadFile(outside)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "outside" {
		t.Fatal("symlink target was overwritten")
	}
	if _, err := ReadManifest(root, manifestPath); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("read symlink error=%v, want ErrUnsafePath", err)
	}
}

func TestComputeArtifactNeverHashesOutsideRootDuringSymlinkSwap(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	artifactPath := filepath.Join(root, "artifact.ndjson")
	outsidePath := filepath.Join(outside, "outside.ndjson")
	if err := os.WriteFile(artifactPath, []byte("inside\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outsidePath, []byte("outside\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	inside, err := ComputeArtifact(root, "artifact.ndjson", "artifact")
	if err != nil {
		t.Fatal(err)
	}
	outsideArtifact, err := ComputeArtifact(outside, "outside.ndjson", "outside")
	if err != nil {
		t.Fatal(err)
	}
	if inside.SHA256 == outsideArtifact.SHA256 {
		t.Fatal("test fixture hashes unexpectedly match")
	}
	swapPath := filepath.Join(root, "swap")
	if err := os.Symlink(outsidePath, swapPath); err != nil {
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
			_ = os.Remove(artifactPath)
			_ = os.Rename(swapPath, artifactPath)
			_ = os.Remove(artifactPath)
			_ = os.WriteFile(artifactPath, []byte("inside\n"), 0o600)
			_ = os.Symlink(outsidePath, swapPath)
		}
	}()
	defer func() {
		close(stop)
		wg.Wait()
	}()

	for i := 0; i < 500; i++ {
		artifact, err := ComputeArtifact(root, "artifact.ndjson", "artifact")
		if err != nil {
			continue
		}
		if artifact.SHA256 == outsideArtifact.SHA256 {
			t.Fatalf("iteration %d hashed outside file: %+v", i, artifact)
		}
	}
}

func TestWriteManifestNeverOverwritesOutsideSymlinkDuringSwap(t *testing.T) {
	root := t.TempDir()
	artifactPath := filepath.Join(root, "artifact.ndjson")
	if err := os.WriteFile(artifactPath, []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := ComputeArtifact(root, "artifact.ndjson", "artifact")
	if err != nil {
		t.Fatal(err)
	}
	manifest := validManifest(artifact)
	outside := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outside, []byte("outside\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "run-manifest.json")
	swapPath := filepath.Join(root, "manifest-swap")
	if err := os.Symlink(outside, swapPath); err != nil {
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
			_ = os.Remove(manifestPath)
			_ = os.Rename(swapPath, manifestPath)
			_ = os.Remove(manifestPath)
			_ = os.Symlink(outside, swapPath)
		}
	}()
	defer func() {
		close(stop)
		wg.Wait()
	}()

	for i := 0; i < 200; i++ {
		_ = WriteManifest(root, manifestPath, manifest)
	}
	raw, err := os.ReadFile(outside)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "outside\n" {
		t.Fatalf("outside symlink target changed: %q", raw)
	}
}

func TestWriteManifestRedactsFreeTextAndProvenance(t *testing.T) {
	root := t.TempDir()
	artifactPath := filepath.Join(root, "artifact.ndjson")
	if err := os.WriteFile(artifactPath, []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := ComputeArtifact(root, "artifact.ndjson", "artifact")
	if err != nil {
		t.Fatal(err)
	}
	manifest := validManifest(artifact)
	manifest.Reason = "Authorization: Bearer top-secret endpoint=https://user:developer_004@example.invalid:443/run?token=top-secret"
	manifest.Provenance.Worktree = "https://user:developer_004@example.invalid:443/run?token=top-secret"
	manifest.Worktree = manifest.Provenance.Worktree
	path := filepath.Join(root, "run-manifest.json")
	if err := WriteManifestWithRedactor(root, path, manifest, NewRedactor("top-secret")); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, forbidden := range []string{"top-secret", "password", "real.example"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("manifest leaked %q: %s", forbidden, text)
		}
	}
	loaded, err := ReadManifestWithRedactor(root, path, NewRedactor("top-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(loaded.Reason, "<redacted-host>") ||
		!strings.Contains(loaded.Reason, "<redacted>") {
		t.Fatalf("loaded reason was not redacted: %q", loaded.Reason)
	}
}

func TestWriteManifestRejectsSensitiveArtifactNameBeforeHashOrWrite(t *testing.T) {
	root := t.TempDir()
	artifactPath := filepath.Join(root, "artifact.ndjson")
	if err := os.WriteFile(artifactPath, []byte("one\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := ComputeArtifact(root, "artifact.ndjson", "artifact")
	if err != nil {
		t.Fatal(err)
	}
	artifact.Name = "secret"
	manifest := validManifest(artifact)
	manifestPath := filepath.Join(root, "run-manifest.json")
	if err := WriteManifestWithRedactor(root, manifestPath, manifest, NewRedactor("secret")); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("sensitive artifact name error=%v, want ErrInvalidManifest", err)
	}
	if _, err := os.Lstat(manifestPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manifest was created despite sensitive artifact metadata: %v", err)
	}
	if manifest.Artifacts[0].Name != "secret" {
		t.Fatalf("caller artifact was mutated: %+v", manifest.Artifacts[0])
	}
	artifact.Path = "secret.ndjson"
	manifest = validManifest(artifact)
	if err := WriteManifestWithRedactor(root, filepath.Join(root, "path-manifest.json"), manifest, NewRedactor("secret")); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("sensitive artifact path error=%v, want ErrInvalidManifest", err)
	}
}

func TestManifestRedactedDeepCopiesArtifactMetadata(t *testing.T) {
	manifest := validManifest(Artifact{
		Name:   "secret",
		Path:   "secret.ndjson",
		Bytes:  1,
		SHA256: strings.Repeat("e", 64),
	})
	redacted := manifest.Redacted(NewRedactor("secret"))
	if manifest.Artifacts[0].Name != "secret" || manifest.Artifacts[0].Path != "secret.ndjson" {
		t.Fatalf("Redacted mutated caller artifact: %+v", manifest.Artifacts[0])
	}
	raw, err := json.Marshal(redacted)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if strings.Contains(text, "secret") {
		t.Fatalf("artifact metadata leaked: %s", text)
	}
	if !strings.Contains(text, "redacted") {
		t.Fatalf("artifact metadata was not redacted: %s", text)
	}
}

func TestReadManifestRejectsTrailingJSON(t *testing.T) {
	root := t.TempDir()
	artifactPath := filepath.Join(root, "a.ndjson")
	if err := os.WriteFile(artifactPath, []byte("a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	artifact, err := ComputeArtifact(root, "a.ndjson", "a")
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "manifest.json")
	if err := WriteManifest(root, manifestPath, validManifest(artifact)); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(manifestPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("{}\n"); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadManifest(root, manifestPath); err == nil {
		t.Fatal("trailing JSON value was accepted")
	}
}

func validManifest(artifact Artifact) Manifest {
	provenance := testProvenance(EvidenceE1, SourceSelfCheck)
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
		Reason:        "unit self-check",
		Provenance:    provenance,
		Artifacts:     []Artifact{artifact},
	}
}
