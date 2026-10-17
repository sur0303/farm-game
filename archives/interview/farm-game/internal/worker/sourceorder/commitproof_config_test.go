package sourceorder

import (
	"errors"
	"testing"
)

func TestCommitProofEnvironmentDefaultsOffAndBlocksEnvironmentOnlyPrefer(t *testing.T) {
	mode, err := CommitProofModeFromEnvironment(func(string) string { return "" })
	if err != nil || mode != CommitProofModeOff || CommitProofMode(0) != CommitProofModeOff {
		t.Fatalf("empty/zero mode=%v err=%v", mode, err)
	}
	if err := ValidateEnvironmentCommitProofRollout(mode); err != nil {
		t.Fatalf("off rollout rejected: %v", err)
	}
	if err := ValidateEnvironmentCommitProofRollout(CommitProofModePrefer); !errors.Is(err, ErrCommitProofBypassUnavailable) {
		t.Fatalf("environment-only prefer was not startup-blocked: %v", err)
	}
}

func TestCommitProofEphemeralSignerBindingIsExactAndFailClosed(t *testing.T) {
	signer, publication, err := NewEphemeralHolderBootSigner("realm-a", "farm-holder-0", "farm-holder-0-boot-a")
	if err != nil || !signer.MatchesPublication(publication) {
		t.Fatalf("signer/publication err=%v", err)
	}
	statement := testCommitProofStatement(t)
	statement.SourceRealm = "realm-a"
	statement.SourceHolderID = "farm-holder-0"
	statement.SourceHolderBootID = "farm-holder-0-boot-a"
	if _, err := signer.Sign(statement); err != nil {
		t.Fatalf("exact holder boot rejected: %v", err)
	}
	statement.SourceHolderBootID = "farm-holder-0-boot-b"
	if _, err := signer.Sign(statement); !errors.Is(err, ErrCommitProofInvalid) {
		t.Fatalf("rotated boot reused old signer: %v", err)
	}
}
