// Package authoritycomposition hosts the RT1B-3C-B command-root mode seam.
//
// The two membership modes are mutually exclusive and frozen:
//
//	MembershipModeComposed (AUTHORITY_MEMBERSHIP_MODE=true)  →
//	  NewMembershipRuntime (RealMembershipAdapter +
//	  ShardMembershipReconciler + PerShardLeaseScheduler).  The legacy global
//	  runRenewal loop is NEVER started.
//	MembershipModeLegacy   (AUTHORITY_MEMBERSHIP_MODE=false) →
//	  the legacy New path (global runRenewal).  The per-shard scheduler is
//	  NEVER started.
//
// There is NO silent fallback: a construction failure in the selected mode is
// returned and the command root must fail the process startup.
package authoritycomposition

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// MembershipMode selects how a command root composes the authority runtime.
type MembershipMode string

const (
	// MembershipModeComposed selects the per-shard membership pipeline as the
	// sole lease-renewal owner (RT1B-3C-A).  The legacy runRenewal loop never
	// starts.
	MembershipModeComposed MembershipMode = "composed"
	// MembershipModeLegacy selects the legacy New path with the global
	// runRenewal loop; the per-shard scheduler never starts.
	MembershipModeLegacy MembershipMode = "legacy"
)

// MembershipModeFromEnvironment parses AUTHORITY_MEMBERSHIP_MODE.  An unset or
// empty value defaults to the legacy mode (the pre-wire behavior), so a
// deployment that has not opted in keeps running the old path.  An unknown
// value is an error: the command root must fail startup instead of guessing.
func MembershipModeFromEnvironment() (MembershipMode, error) {
	return membershipModeFromValue(os.Getenv("AUTHORITY_MEMBERSHIP_MODE"))
}

func membershipModeFromValue(value string) (MembershipMode, error) {
	raw := strings.ToLower(strings.TrimSpace(value))
	switch raw {
	case "", "false", "0", "off", "no", "legacy":
		return MembershipModeLegacy, nil
	case "true", "1", "on", "yes", "composed", "membership":
		return MembershipModeComposed, nil
	default:
		return "", fmt.Errorf("authority composition: AUTHORITY_MEMBERSHIP_MODE must be true or false, got %q", raw)
	}
}

// NewRuntimeForMode composes the authority runtime for a command root according
// to the frozen membership mode.  There is no fallback: a construction failure
// in the selected mode is returned and the caller must fail the process
// startup -- never silently downgrade to the other mode.
func NewRuntimeForMode(ctx context.Context, opts Options, mode MembershipMode) (*Runtime, error) {
	if err := ValidateModeCombination(mode, opts.AssignmentMode); err != nil {
		return nil, err
	}
	return newRuntimeForMode(ctx, opts, mode, nil)
}

// NewRuntimeForModeWithAssignment is the construction-only injection seam for
// a process-local complete route source.  It never creates a second provider,
// resolver, watch, or client.
func NewRuntimeForModeWithAssignment(ctx context.Context, opts Options, mode MembershipMode, assignment *AssignmentComposition) (*Runtime, error) {
	if err := ValidateModeCombination(mode, opts.AssignmentMode); err != nil {
		return nil, err
	}
	if opts.AssignmentMode != AssignmentModeRemoteWatch || assignment == nil {
		return nil, fmt.Errorf("%w: mode=%q assignment=%v", ErrRemoteAssignmentComposition, opts.AssignmentMode, assignment != nil)
	}
	return newRuntimeForMode(ctx, opts, mode, assignment)
}

func newRuntimeForMode(ctx context.Context, opts Options, mode MembershipMode, assignment *AssignmentComposition) (*Runtime, error) {
	switch mode {
	case MembershipModeComposed:
		return newMembershipRuntimeWithAssignment(ctx, opts, false, assignment)
	case MembershipModeLegacy:
		return newLegacyRuntime(ctx, opts, assignment)
	default:
		return nil, fmt.Errorf("authority composition: unknown membership mode %q", mode)
	}
}
