package domain

import "fmt"

// Status is the deterministic outcome class of a domain decision, mirroring
// the command statuses of design 28/32 at the domain boundary.
type Status int32

const (
	StatusSucceeded Status = iota
	StatusRejected
	StatusNoChange
)

// Code is a stable, machine-readable domain error/decision code. Free-form
// strings never participate in control flow (design 32 §10.6).
type Code string

const (
	CodeOK Code = "OK"

	// UnlockPlot (design 32 §2A.7).
	CodeLevelRequirementNotMet Code = "LEVEL_REQUIREMENT_NOT_MET"
	CodeInsufficientCoin       Code = "INSUFFICIENT_COIN"
	CodeRuleVersionChanged     Code = "RULE_VERSION_CHANGED"
	CodeStalePlotVersion       Code = "STALE_PLOT_VERSION"
	CodePreviousPlotLocked     Code = "PREVIOUS_PLOT_LOCKED"
	CodeAlreadyUnlocked        Code = "ALREADY_UNLOCKED"

	// Shared plot/cycle preconditions.
	CodePlotNotFound  Code = "PLOT_NOT_FOUND"
	CodePlotLocked    Code = "PLOT_LOCKED"
	CodePlotNotEmpty  Code = "PLOT_NOT_EMPTY"
	CodePlotEmpty     Code = "PLOT_EMPTY"
	CodeCycleMismatch Code = "CYCLE_MISMATCH"
	CodeCycleEnded    Code = "CYCLE_ENDED"

	// Plant (design 32 §3.4).
	CodeCropRuleUnknown Code = "CROP_RULE_UNKNOWN"

	// Maturity (design 32 §4.1).
	CodeCropNotMature Code = "CROP_NOT_MATURE"

	// Care (design 32 §5).
	CodeIncidentUnknown  Code = "INCIDENT_UNKNOWN"
	CodeIncidentNotDue   Code = "INCIDENT_NOT_DUE"
	CodeAlreadyResolved  Code = "ALREADY_RESOLVED"
	CodeCareTypeMismatch Code = "CARE_TYPE_MISMATCH"

	// Theft (design 32 §6.3).
	CodeVisitorIsOwner        Code = "VISITOR_IS_OWNER"
	CodeStealAlreadyAttempted Code = "STEAL_ALREADY_ATTEMPTED"
	CodeNoStealableQuota      Code = "NO_STEALABLE_QUOTA"

	// Guard snapshot application (design 36 §6.4).
	CodeGuardOwnerMismatch Code = "GUARD_OWNER_MISMATCH"

	// Invalid caller input.
	CodeInvalidArgument Code = "INVALID_ARGUMENT"
)

// Error is a deterministic domain rejection or no-change verdict. It
// carries no wall clock, no randomness and no environment dependency.
// Status separates a definitive business rejection from "the target no
// longer needs changing" (design 32 §10.6: both are terminal for the
// original operation but the client treats them differently).
type Error struct {
	Status  Status
	Code    Code
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("farm domain: %s: %s", e.Code, e.Message) }

func reject(code Code, format string, args ...any) *Error {
	return &Error{Status: StatusRejected, Code: code, Message: fmt.Sprintf(format, args...)}
}

func noChange(code Code, format string, args ...any) *Error {
	return &Error{Status: StatusNoChange, Code: code, Message: fmt.Sprintf(format, args...)}
}

// CareOutcome is the result class of a care decision (design 32 §5.4).
type CareOutcome int32

const (
	CareOutcomeUnspecified CareOutcome = iota
	CareOutcomeResolved
	CareOutcomeAlreadyResolved
)

// TheftOutcome is the result class of a theft decision (design 32 §6.5).
type TheftOutcome int32

const (
	TheftOutcomeUnspecified TheftOutcome = iota
	TheftOutcomeStolen
	TheftOutcomeBlockedByGuard
	// TheftOutcomePartiallyBlocked is reserved for a future versioned guard
	// rule; P0 binary interception never produces it.
	TheftOutcomePartiallyBlocked
)

// PlotUnlockDecision is the immutable farm-side result of CommitPlotUnlock
// (design 32 §2A.5). Coin reservation/release and level checks are
// PlayerActor concerns driven by the frozen numbers carried here.
type PlotUnlockDecision struct {
	PlotID               uint32
	UnlockOrder          uint32
	RequiredLevel        uint32
	NormalCoinPrice      uint64
	AppliedRuleVersion   uint64
	UnlockedAtMs         int64
	CommittedPlotVersion uint64
	CommittedFarmVersion uint64
}

// PlantDecision is the immutable result of CommitPlant (design 32 §3.4).
type PlantDecision struct {
	PlotID               uint32
	CycleID              string
	CropDefinitionID     uint32
	AppliedRuleVersion   uint64
	PlantAtMs            int64
	MatureAtMs           int64
	CommittedPlotVersion uint64
	CommittedFarmVersion uint64
}

// CareDecision is the immutable result of resolving a care incident (design
// 32 §5.4). RewardObligationCreated tells the caller whether exactly one
// CareRewardOrder must be created in the same authority commit.
type CareDecision struct {
	PlotID     uint32
	CycleID    string
	IncidentID string
	CareType   CareType
	Outcome    CareOutcome
	// ResolvedByPlayerID is the first resolver for RESOLVED, or the
	// original resolver echoed for ALREADY_RESOLVED.
	ResolvedByPlayerID      uint64
	ResolvedAtMs            int64
	RewardObligationCreated bool
	CommittedPlotVersion    uint64
	CommittedFarmVersion    uint64
}

// GuardDecision is the immutable interception settlement inside a theft
// decision (design 32 §6.3 / design 36 §9). The visitor's wallet is never
// touched: interception only reduces the visitor's net crop intake and
// compensates the owner with coins at the frozen unit value.
type GuardDecision struct {
	GuardEffective    bool
	GuardTriggered    bool
	GuardVersion      uint64
	GuardRuleVersion  uint64
	RandomDrawBP      uint32
	GuardLostQuantity uint32
	GuardCoinValue    uint64
}

// TheftDecision is the immutable result of DecideTheft (design 32 §6.3-6.5).
// Visitor inventory and owner coin are settled by the respective
// PlayerActors from these frozen numbers; this package never mutates them.
type TheftDecision struct {
	PlotID               uint32
	CycleID              string
	VisitorPlayerID      uint64
	Outcome              TheftOutcome
	StealSlotIndex       uint32
	AppliedSlotRatioBP   uint32
	GrossStolenQuantity  uint32
	Guard                GuardDecision
	NetReceivedQuantity  uint32
	CommittedPlotVersion uint64
	CommittedFarmVersion uint64
}

// HarvestDecision is the immutable result of CommitHarvest (design 32 §4.3).
// RewardItemID is the produce item the caller maps from the frozen crop
// definition; the domain carries the definition ID and lets the caller map.
type HarvestDecision struct {
	PlotID               uint32
	CycleID              string
	CropDefinitionID     uint32
	RewardQuantity       uint32
	StolenAmount         uint32
	FrozenHarvestExp     uint64
	Compendium           CompendiumUnlockSnapshot
	EndedIncidentIDs     []string
	CommittedPlotVersion uint64
	CommittedFarmVersion uint64
}

// GuardApplyOutcome is the result of applying a GuardSnapshot (design 36
// §6.4).
type GuardApplyOutcome int32

const (
	GuardApplyUnspecified GuardApplyOutcome = iota
	GuardApplyApplied
	// GuardApplyReplayed: same version and same hash, idempotent retry.
	GuardApplyReplayed
	// GuardApplySuperseded: a higher version is already installed.
	GuardApplySuperseded
	// GuardApplyInvariantViolation: same version but different hash; the
	// current snapshot is kept and the caller must alert.
	GuardApplyInvariantViolation
)

// GuardApplyDecision is the immutable result of ApplyGuardSnapshot.
type GuardApplyDecision struct {
	Outcome             GuardApplyOutcome
	AppliedGuardVersion uint64
	AppliedSnapshotHash string
	// AppearanceChanged reports whether the public guard pet appearance
	// changed (a real Switch). Feed installs a hidden snapshot and only
	// advances record_version; a real Switch advances farm_version and
	// produces one FarmMetaDelta (design 36 §7.5).
	AppearanceChanged bool
}
