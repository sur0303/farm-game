package sourceorder

import (
	"fmt"
	"time"
)

const (
	CommitProofModeEnv             = "SOURCE_ORDER_COMMIT_PROOF_MODE"
	CommitProofTTLEnv              = "SOURCE_ORDER_COMMIT_PROOF_TTL"
	CommitProofLeaseSafetyEnv      = "SOURCE_ORDER_COMMIT_PROOF_LEASE_SAFETY_MARGIN"
	DefaultCommitProofTTL          = 10 * time.Second
	DefaultCommitProofSafetyMargin = 2 * time.Second
)

type EnvironmentGetter func(string) string

// CommitProofModeFromEnvironment keeps an absent value fully off. Only an
// explicit non-off value enables any proof work.
func CommitProofModeFromEnvironment(getenv EnvironmentGetter) (CommitProofMode, error) {
	if getenv == nil {
		return CommitProofModeOff, fmt.Errorf("%w: environment getter", ErrCommitProofInvalid)
	}
	return ParseCommitProofMode(getenv(CommitProofModeEnv))
}

// ValidateEnvironmentCommitProofRollout rejects prefer for legacy callers
// that select policy from environment without composing the production
// ephemeral provisioner and watched route keyring. Production command roots
// use ValidateProductionPreferComposition after those invariants exist.
func ValidateEnvironmentCommitProofRollout(mode CommitProofMode) error {
	if err := mode.ValidatePreferRollout(); err != nil {
		return err
	}
	if mode == CommitProofModePrefer {
		return fmt.Errorf("%w: environment-only prefer lacks holder/boot-isolated provisioning and watched public-key publication", ErrCommitProofBypassUnavailable)
	}
	return nil
}
