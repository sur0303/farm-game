package routecontrol

import (
	"context"
	"errors"
	"fmt"

	"farmgame/internal/actorrouting"
)

var (
	ErrPlacementExclusionInvalid     = errors.New("routecontrol: invalid placement exclusion")
	ErrPlacementExclusionConflict    = errors.New("routecontrol: placement exclusion conflict")
	ErrPlacementExclusionUnavailable = errors.New("routecontrol: placement exclusion unavailable")
	ErrPlacementExcluded             = errors.New("routecontrol: holder generation is placement-excluded")
)

// PlacementExclusion is a durable target-admission fact for one exact holder
// boot. It is deliberately independent of Holder lifecycle: an excluded boot
// remains process-ready, renewable and legal as a planned-migration source.
type PlacementExclusion struct {
	Domain      actorrouting.RouteDomain `json:"domain"`
	HolderID    string                   `json:"holder_id"`
	BootID      string                   `json:"boot_id"`
	Excluded    bool                     `json:"excluded"`
	Revision    uint64                   `json:"revision"`
	OwnerPlanID string                   `json:"owner_plan_id"`
}

// PlacementExclusionPrecondition is the exact false-to-true CAS contract.
// OwnerPlanID is immutable after commit and also serves as replay identity.
type PlacementExclusionPrecondition struct {
	ExpectedRevision uint64
	ExpectedExcluded bool
	OwnerPlanID      string
}

type PlacementExclusionResult struct {
	Committed bool
	State     PlacementExclusion
	// Outcome classifies a release against the seven-row table in section 8.3.
	// It is empty for the exclude path, which predates the table.
	Outcome PlacementReleaseOutcome
}

// PlacementReleaseOutcome enumerates every value the release path can produce.
// The list is closed on purpose: section 17 makes any value outside it a stop
// condition, because an unenumerated outcome is one nobody decided whether to
// treat as safe.
type PlacementReleaseOutcome string

const (
	// PlacementReleaseReleased is the ordinary success: the owner matched and
	// the record flipped true -> false.
	PlacementReleaseReleased PlacementReleaseOutcome = "RELEASED"
	// PlacementReleaseAlreadyReleased is the idempotent replay.
	PlacementReleaseAlreadyReleased PlacementReleaseOutcome = "ALREADY_RELEASED"
	// PlacementReleaseByBootRollover is the one inert terminal state. The boot
	// this exclusion named is gone, so load_placement will synthesize an
	// unexcluded baseline for the new boot and no write is needed.
	PlacementReleaseByBootRollover PlacementReleaseOutcome = "RELEASED_BY_BOOT_ROLLOVER"
	// PlacementReleaseNotFound blocks. A missing record does not prove the
	// victim process is gone; it may simply have outlived its TTL.
	PlacementReleaseNotFound PlacementReleaseOutcome = "NOT_FOUND"
	// PlacementReleaseOwnerMismatch blocks. Releasing another plan's exclusion
	// would readmit placement the other plan is still relying on.
	PlacementReleaseOwnerMismatch PlacementReleaseOutcome = "OWNER_MISMATCH"
	// PlacementReleaseRevisionConflict blocks and is retried.
	PlacementReleaseRevisionConflict PlacementReleaseOutcome = "REVISION_CONFLICT"
	// PlacementReleaseMalformed blocks and latches. A record of the wrong
	// shape is corruption, not a state.
	PlacementReleaseMalformed PlacementReleaseOutcome = "MALFORMED"
)

// TerminalForScalePlan reports whether this outcome lets the owning plan reach
// a terminal state. Only two of the seven do; everything else stays BLOCKED.
func (o PlacementReleaseOutcome) TerminalForScalePlan() bool {
	return o == PlacementReleaseReleased || o == PlacementReleaseAlreadyReleased ||
		o == PlacementReleaseByBootRollover
}

// PlacementExclusionRelease is the exact true-to-false contract. Both fields
// are expectations: a release names the revision it believes it is reversing,
// so a concurrent change is a conflict rather than a silent overwrite.
type PlacementExclusionRelease struct {
	OwnerPlanID      string
	ExpectedRevision uint64
}

func validatePlacementExclusionRelease(value PlacementExclusionRelease, maxIDBytes int) error {
	if value.ExpectedRevision == 0 {
		return fmt.Errorf("%w: release requires an expected revision", ErrPlacementExclusionInvalid)
	}
	return validateComponent("owner_plan_id", value.OwnerPlanID, maxIDBytes)
}

// PlacementExclusionStore must atomically bind every state to the current
// exact Holder boot. Absence is not an eligibility answer: implementations
// return a synthetic revision-one unexcluded baseline only after proving the
// exact current Holder record in the same storage operation.
type PlacementExclusionStore interface {
	GetPlacementExclusion(context.Context, actorrouting.RouteDomain, string, string) (PlacementExclusion, bool, error)
	ExcludePlacement(context.Context, actorrouting.RouteDomain, string, string, PlacementExclusionPrecondition) (PlacementExclusionResult, error)
	// ReleasePlacementExclusionForOwner reverses an exclusion the named plan
	// owns. It is owner-scoped so a plan can never release an exclusion another
	// plan installed. Outcomes are classified by the seven-row table in
	// section 8.3; anything outside it blocks.
	ReleasePlacementExclusionForOwner(context.Context, NestedControlOperationToken, actorrouting.RouteDomain, string, string, PlacementExclusionRelease) (PlacementExclusionResult, error)
}

// PlacementExclusionController is the production capability consumed by the
// RouteAssignmentManager. Holder processes receive registry liveness methods,
// not this mutation capability, from their composition root.
type PlacementExclusionController interface {
	GetPlacementExclusion(context.Context, actorrouting.RouteDomain, string, string) (PlacementExclusion, bool, error)
	ExcludePlacement(context.Context, actorrouting.RouteDomain, string, string, PlacementExclusionPrecondition) (PlacementExclusionResult, error)
	ReleasePlacementExclusionForOwner(context.Context, NestedControlOperationToken, actorrouting.RouteDomain, string, string, PlacementExclusionRelease) (PlacementExclusionResult, error)
	ValidatePlacementTarget(context.Context, actorrouting.RouteDomain, string, string) (uint64, error)
}

func ValidatePlacementExclusion(value PlacementExclusion) error {
	return validatePlacementExclusion(value, DefaultHolderMaxIDBytes)
}

func validatePlacementExclusion(value PlacementExclusion, maxIDBytes int) error {
	if err := validateDomain(value.Domain); err != nil {
		return fmt.Errorf("%w: %v", ErrPlacementExclusionInvalid, err)
	}
	if err := validateComponent("holder_id", value.HolderID, maxIDBytes); err != nil {
		return fmt.Errorf("%w: %v", ErrPlacementExclusionInvalid, err)
	}
	if err := validateComponent("boot_id", value.BootID, maxIDBytes); err != nil {
		return fmt.Errorf("%w: %v", ErrPlacementExclusionInvalid, err)
	}
	if value.Revision == 0 {
		return fmt.Errorf("%w: revision must be positive", ErrPlacementExclusionInvalid)
	}
	if value.Excluded {
		if err := validateComponent("owner_plan_id", value.OwnerPlanID, maxIDBytes); err != nil {
			return fmt.Errorf("%w: %v", ErrPlacementExclusionInvalid, err)
		}
	} else if value.OwnerPlanID != "" {
		return fmt.Errorf("%w: unexcluded state has owner", ErrPlacementExclusionInvalid)
	}
	return nil
}

func validatePlacementExclusionPrecondition(value PlacementExclusionPrecondition, maxIDBytes int) error {
	if value.ExpectedRevision == 0 || value.ExpectedExcluded {
		return fmt.Errorf("%w: only exact unexcluded CAS is supported", ErrPlacementExclusionInvalid)
	}
	if err := validateComponent("owner_plan_id", value.OwnerPlanID, maxIDBytes); err != nil {
		return fmt.Errorf("%w: %v", ErrPlacementExclusionInvalid, err)
	}
	return nil
}
