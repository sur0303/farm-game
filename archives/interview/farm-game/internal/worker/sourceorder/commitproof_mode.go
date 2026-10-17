package sourceorder

import (
	"fmt"
	"strings"
)

// CommitProofMode names the mixed-version rollout ladder. Off is both the
// zero value and the safe default: no evidence is copied, minted, carried, or
// verified. Shadow is explicit and compares proofs with authoritative Redis.
// Prefer is explicit opt-in and may bypass source Redis only after complete
// cryptographic and current-route validation. Require remains reserved.
type CommitProofMode uint8

const (
	CommitProofModeOff CommitProofMode = iota
	CommitProofModeShadow
	CommitProofModePrefer
	CommitProofModeRequire
)

func (m CommitProofMode) String() string {
	switch m {
	case CommitProofModeShadow:
		return "shadow"
	case CommitProofModeOff:
		return "off"
	case CommitProofModePrefer:
		return "prefer"
	case CommitProofModeRequire:
		return "require"
	default:
		return "invalid"
	}
}

// ValidatePreferRollout permits only the production off/shadow/prefer ladder.
// Require cannot be selected until a later no-fallback rollout is reviewed.
func (m CommitProofMode) ValidatePreferRollout() error {
	switch m {
	case CommitProofModeShadow, CommitProofModeOff, CommitProofModePrefer:
		return nil
	case CommitProofModeRequire:
		return fmt.Errorf("%w: mode %s", ErrCommitProofBypassUnavailable, m)
	default:
		return fmt.Errorf("%w: mode %d", ErrCommitProofInvalid, m)
	}
}

// ValidateShadowOnly remains for callers that intentionally forbid bypass.
func (m CommitProofMode) ValidateShadowOnly() error {
	if m == CommitProofModePrefer {
		return fmt.Errorf("%w: mode %s", ErrCommitProofBypassUnavailable, m)
	}
	return m.ValidatePreferRollout()
}

func ParseCommitProofMode(raw string) (CommitProofMode, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "off":
		return CommitProofModeOff, nil
	case "shadow":
		return CommitProofModeShadow, nil
	case "prefer":
		return CommitProofModePrefer, nil
	case "require":
		return CommitProofModeOff, fmt.Errorf("%w: mode require", ErrCommitProofBypassUnavailable)
	default:
		return CommitProofModeOff, fmt.Errorf("%w: unknown mode %q", ErrCommitProofInvalid, raw)
	}
}
