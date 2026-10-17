package routecontrol

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func testProvenance(level EvidenceLevel, source SourceKind) Provenance {
	worktree, _ := os.Getwd()
	head, _ := CurrentGitHead(worktree)
	if head == "" {
		head = strings.Repeat("a", 40)
	}
	return Provenance{
		TaskID:        "RT2Q0-ROUTE-CONTROL-HARNESS",
		RunID:         "test-run",
		BaseCommit:    head,
		HeadCommit:    head,
		Branch:        "codex/test",
		Worktree:      worktree,
		Collector:     "routecontrol-test",
		SourceKind:    source,
		DeclaredLevel: level,
		RecordedAt:    time.Unix(1_900_000_000, 0).UTC().Format(time.RFC3339Nano),
	}
}

func TestClaimE1SelfCheckCanPass(t *testing.T) {
	claim := Claim{
		Level:      EvidenceE1,
		Status:     StatusPass,
		Reason:     "local self-check",
		Provenance: testProvenance(EvidenceE1, SourceSelfCheck),
	}
	if err := claim.Validate(); err != nil {
		t.Fatal(err)
	}
	if !claim.CanPass() {
		t.Fatal("valid E1 self-check claim cannot pass")
	}
}

func TestClaimE3WithoutRealAttestationIsOnlyInconclusive(t *testing.T) {
	provenance := testProvenance(EvidenceE3, SourceRealProcess)
	for _, status := range []EvidenceStatus{StatusPass, StatusFail} {
		claim := Claim{Level: EvidenceE3, Status: status, Reason: "not run", Provenance: provenance}
		want := ErrAttestationRequired
		if status == StatusPass {
			want = ErrTrustedVerifierRequired
		}
		if err := claim.Validate(); !errors.Is(err, want) {
			t.Fatalf("status=%s error=%v, want %v", status, err, want)
		}
		if claim.CanPass() {
			t.Fatalf("status=%s unexpectedly passed without attestation", status)
		}
	}
	inconclusive := Claim{
		Level:       EvidenceE3,
		Status:      StatusInconclusive,
		Reason:      "real attestation not supplied",
		Provenance:  provenance,
		Attestation: nil,
	}
	if err := inconclusive.Validate(); err != nil {
		t.Fatalf("inconclusive E3 should be valid: %v", err)
	}
}

func TestClaimRejectsSourceRelabelingAndWeakAttestation(t *testing.T) {
	fixture := Claim{
		Level:      EvidenceE3,
		Status:     StatusInconclusive,
		Reason:     "fixture cannot support E3",
		Provenance: testProvenance(EvidenceE3, SourceFixture),
	}
	if !errors.Is(fixture.Validate(), ErrEvidenceEscalation) {
		t.Fatal("fixture source was relabeled as E3")
	}

	claim := Claim{
		Level:      EvidenceE3,
		Status:     StatusFail,
		Reason:     "should be rejected",
		Provenance: testProvenance(EvidenceE3, SourceRealProcess),
		Attestation: &Attestation{
			ID:             "att-1",
			Issuer:         "test",
			Level:          EvidenceE2,
			ArtifactSHA256: strings.Repeat("b", 64),
			Real:           true,
			Verified:       true,
		},
	}
	if !errors.Is(claim.Validate(), ErrEvidenceEscalation) {
		t.Fatal("weak attestation was accepted for E3")
	}
}

func TestAttestationRequiresRealAndVerifiedFlags(t *testing.T) {
	attestation := Attestation{
		ID:             "att-1",
		Issuer:         "test",
		Level:          EvidenceE4,
		ArtifactSHA256: strings.Repeat("c", 64),
		Real:           false,
		Verified:       true,
	}
	if !errors.Is(attestation.ValidateFor(EvidenceE4), ErrAttestationRequired) {
		t.Fatal("unreal attestation accepted")
	}
}

func TestE3AndE4CanNeverPassFromCallerAttestationBools(t *testing.T) {
	for _, level := range []EvidenceLevel{EvidenceE3, EvidenceE4} {
		provenance := testProvenance(level, SourceExternalAttestation)
		claim := Claim{
			Level:      level,
			Status:     StatusPass,
			Reason:     "caller-supplied booleans are not trusted",
			Provenance: provenance,
			Attestation: &Attestation{
				ID:             "claimed-attestation",
				Issuer:         "caller",
				Level:          level,
				ArtifactSHA256: strings.Repeat("f", 64),
				Real:           true,
				Verified:       true,
			},
		}
		if claim.CanPass() {
			t.Fatalf("level %s passed from caller attestation booleans", level)
		}
		if !errors.Is(claim.Validate(), ErrTrustedVerifierRequired) {
			t.Fatalf("level %s did not require trusted verifier: %v", level, claim.Validate())
		}
	}
}

func TestCommitValidationAcceptsOddSevenHexAndRejectsZeroOrSynthetic(t *testing.T) {
	provenance := testProvenance(EvidenceE1, SourceSelfCheck)
	provenance.BaseCommit = "abcdef1"
	provenance.HeadCommit = "1234567"
	if err := provenance.ValidateFor(EvidenceE1); err != nil {
		t.Fatalf("odd-length seven-hex commits were rejected: %v", err)
	}

	for _, value := range []string{"0000000", strings.Repeat("0", 40), SyntheticE1Commit} {
		provenance.BaseCommit = value
		if err := provenance.ValidateFor(EvidenceE1); !errors.Is(err, ErrInvalidEvidence) {
			t.Fatalf("commit %q error=%v, want ErrInvalidEvidence", value, err)
		}
	}
}

func TestRealProvenanceBindsGitHeadWithoutShellInterpolation(t *testing.T) {
	worktree, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	head, err := CurrentGitHead(worktree)
	if err != nil {
		t.Fatal(err)
	}
	provenance, err := RealProvenance(
		"task",
		"run",
		worktree,
		"test",
		EvidenceE3,
		SourceRealProcess,
		time.Unix(1_900_000_000, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	if provenance.Synthetic || provenance.HeadCommit != head || provenance.BaseCommit != head {
		t.Fatalf("real provenance=%+v, want head=%s and synthetic=false", provenance, head)
	}
	if err := provenance.ValidateFor(EvidenceE3); err != nil {
		t.Fatalf("real provenance did not validate: %v", err)
	}

	marker := t.TempDir() + "/injected"
	_, err = CurrentGitHead(t.TempDir() + "/worktree;touch " + marker)
	if err == nil {
		t.Fatal("malicious worktree unexpectedly resolved")
	}
	if _, statErr := os.Stat(marker); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("shell injection created marker: %v", statErr)
	}
}

func TestRealProvenanceRejectsGitHeadDrift(t *testing.T) {
	worktree, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	provenance := testProvenance(EvidenceE3, SourceRealProcess)
	provenance.Worktree = worktree
	provenance.HeadCommit = strings.Repeat("c", 40)
	if err := provenance.ValidateFor(EvidenceE3); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("drifted Git HEAD error=%v, want ErrInvalidEvidence", err)
	}
}

func TestRealProvenanceRejectsUnknownBaseCommit(t *testing.T) {
	worktree, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	provenance, err := RealProvenance(
		"task",
		"run",
		worktree,
		"test",
		EvidenceE3,
		SourceRealProcess,
		time.Unix(1_900_000_000, 0),
	)
	if err != nil {
		t.Fatal(err)
	}
	provenance.BaseCommit = strings.Repeat("f", 40)
	if err := provenance.ValidateFor(EvidenceE3); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("unknown base commit error=%v, want ErrInvalidEvidence", err)
	}
}

func TestGitCommandsIgnoreAmbientRepositoryOverrides(t *testing.T) {
	oldSource := gitEnvironmentSource
	defer func() { gitEnvironmentSource = oldSource }()
	gitEnvironmentSource = func() []string {
		return []string{
			"GIT_DIR=" + t.TempDir(),
			"GIT_WORK_TREE=" + t.TempDir(),
			"GIT_COMMON_DIR=" + t.TempDir(),
			"GIT_OBJECT_DIRECTORY=" + t.TempDir(),
			"GIT_CONFIG_GLOBAL=" + t.TempDir(),
		}
	}
	worktree, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CurrentGitHead(worktree); err != nil {
		t.Fatalf("ambient Git overrides changed bound HEAD lookup: %v", err)
	}
	env := safeGitEnvironment("/usr/bin/git", gitEnvironmentSource())
	for _, value := range env {
		if strings.HasPrefix(value, "GIT_DIR=") ||
			strings.HasPrefix(value, "GIT_WORK_TREE=") ||
			strings.HasPrefix(value, "GIT_COMMON_DIR=") ||
			strings.HasPrefix(value, "GIT_OBJECT_DIRECTORY=") {
			t.Fatalf("unsafe ambient Git variable survived: %q", value)
		}
	}
}

func TestCurrentGitBranchDistinguishesDetachedAndOtherExit(t *testing.T) {
	oldExec := gitCommandExec
	defer func() { gitCommandExec = oldExec }()

	gitCommandExec = func(context.Context, string, []string, []string) ([]byte, error) {
		return nil, exec.Command("sh", "-c", "exit 1").Run()
	}
	branch, err := CurrentGitBranch("ignored-by-seam")
	if err != nil || branch != "detached" {
		t.Fatalf("exit 1 branch=%q err=%v, want detached", branch, err)
	}

	gitCommandExec = func(context.Context, string, []string, []string) ([]byte, error) {
		return nil, exec.Command("sh", "-c", "exit 2").Run()
	}
	if _, err := CurrentGitBranch("ignored-by-seam"); !errors.Is(err, ErrGitCommandFailed) {
		t.Fatalf("exit 2 error=%v, want ErrGitCommandFailed", err)
	}
}

func TestGitCommandContextTimeoutIsBounded(t *testing.T) {
	oldExec := gitCommandExec
	oldTimeout := gitCommandTimeout
	defer func() {
		gitCommandExec = oldExec
		gitCommandTimeout = oldTimeout
	}()
	gitCommandTimeout = 10 * time.Millisecond
	gitCommandExec = func(ctx context.Context, _ string, _ []string, _ []string) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	started := time.Now()
	_, err := CurrentGitHead("ignored-by-seam")
	if !errors.Is(err, ErrGitCommandTimeout) {
		t.Fatalf("timeout error=%v, want ErrGitCommandTimeout", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Git timeout seam exceeded bound: %s", elapsed)
	}
}
