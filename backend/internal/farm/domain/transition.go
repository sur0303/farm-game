package domain

import "fmt"

// CandidateState is the output of a successful decision: a cloned, mutated
// state that is not yet authoritative. The caller installs it only after
// its authority commit succeeds (design 27: BuildCandidateState must not
// overwrite authoritative memory before persist).
type CandidateState struct {
	State FarmState
}

// commitMutation advances farm/plot versions of the candidate and recomputes
// the opportunity header from the same candidate state (design 27 §4.2:
// header, versions and business facts commit together).
func commitMutation(candidate *FarmState, plot *Plot, trustedNowMs int64) {
	plot.PlotVersion++
	candidate.FarmVersion++
	candidate.Header = ComputeOpportunity(candidate, trustedNowMs,
		candidate.FarmVersion, candidate.Header.DirtyGeneration+1)
}

// NewFarmState creates the genesis state of a farm (design 32 §0.4): exactly
// MaxPlots stable slots, the first InitialUnlockedPlots unlocked for free
// and never producing a purchase record. The unlock rule must already be
// validated; genesis fails closed on invalid config.
func NewFarmState(farmID, ownerPlayerID uint64, unlockRule PlotUnlockRule) (FarmState, error) {
	if err := ValidatePlotUnlockRule(unlockRule); err != nil {
		return FarmState{}, err
	}
	plots := make([]Plot, unlockRule.MaxPlots)
	for i := range plots {
		plotID := uint32(i) + 1
		plots[i] = Plot{PlotID: plotID, LockState: PlotLocked, PlotVersion: 1}
		if plotID <= unlockRule.InitialUnlockedPlots {
			plots[i].LockState = PlotUnlocked
		}
	}
	state := FarmState{
		FarmID:        farmID,
		OwnerPlayerID: ownerPlayerID,
		FarmVersion:   1,
		Plots:         plots,
	}
	return state, nil
}

// UnlockPlotCommand is the farm-side adjudication input of one purchase
// intent. Level/coin eligibility is evaluated separately by
// CheckUnlockEligibility on the PlayerActor side; the Workflow freezes the
// same rule numbers this decision freezes.
type UnlockPlotCommand struct {
	PlotID                    uint32
	ExpectedPlotVersion       uint64
	ExpectedUnlockRuleVersion uint64
	UnlockWorkflowID          string
}

// DecidePlotUnlock adjudicates CommitPlotUnlock (design 32 §2A.5). The
// target must still be locked, be the current next plot of the fixed order,
// and match the expected versions. On success the candidate carries the
// unlocked plot with frozen rule provenance; no player asset is touched.
//
// A plot already unlocked by another operation returns NO_CHANGE /
// ALREADY_UNLOCKED so the coordinator releases its coin reservation instead
// of charging twice.
func DecidePlotUnlock(state FarmState, cmd UnlockPlotCommand, rule PlotUnlockRule, trustedNowMs int64) (CandidateState, PlotUnlockDecision, *Error) {
	if cmd.ExpectedUnlockRuleVersion != 0 && cmd.ExpectedUnlockRuleVersion != rule.RuleVersion {
		return CandidateState{}, PlotUnlockDecision{}, reject(CodeRuleVersionChanged,
			"unlock rule version %d, client expected %d", rule.RuleVersion, cmd.ExpectedUnlockRuleVersion)
	}
	plot := state.FindPlot(cmd.PlotID)
	if plot == nil {
		return CandidateState{}, PlotUnlockDecision{}, reject(CodePlotNotFound, "plot %d", cmd.PlotID)
	}
	entry, ok := rule.EntryOf(cmd.PlotID)
	if !ok {
		// Initial plots have no purchase entry; a purchase intent against
		// them is already satisfied.
		if plot.Unlocked() {
			return CandidateState{}, PlotUnlockDecision{}, noChange(CodeAlreadyUnlocked,
				"plot %d is a free initial plot", cmd.PlotID)
		}
		return CandidateState{}, PlotUnlockDecision{}, reject(CodePlotNotFound,
			"plot %d has no unlock rule entry", cmd.PlotID)
	}
	if plot.Unlocked() {
		return CandidateState{}, PlotUnlockDecision{
			PlotID:             cmd.PlotID,
			UnlockOrder:        entry.UnlockOrder,
			RequiredLevel:      entry.RequiredLevel,
			NormalCoinPrice:    entry.NormalCoinPrice,
			AppliedRuleVersion: rule.RuleVersion,
		}, noChange(CodeAlreadyUnlocked, "plot %d already unlocked", cmd.PlotID)
	}
	if cmd.ExpectedPlotVersion != 0 && cmd.ExpectedPlotVersion != plot.PlotVersion {
		return CandidateState{}, PlotUnlockDecision{}, reject(CodeStalePlotVersion,
			"plot %d version %d, client expected %d", cmd.PlotID, plot.PlotVersion, cmd.ExpectedPlotVersion)
	}
	if entry.UnlockOrder > 1 {
		previous := state.FindPlot(entry.UnlockOrder - 1)
		if previous == nil || !previous.Unlocked() {
			return CandidateState{}, PlotUnlockDecision{}, reject(CodePreviousPlotLocked,
				"plot %d requires plot %d unlocked first", cmd.PlotID, entry.UnlockOrder-1)
		}
	}

	candidate := state.Clone()
	candidatePlot := candidate.FindPlot(cmd.PlotID)
	candidatePlot.LockState = PlotUnlocked
	candidatePlot.UnlockWorkflowID = cmd.UnlockWorkflowID
	candidatePlot.UnlockRuleVersion = rule.RuleVersion
	candidatePlot.UnlockedAtMs = trustedNowMs
	commitMutation(&candidate, candidatePlot, trustedNowMs)

	decision := PlotUnlockDecision{
		PlotID:               cmd.PlotID,
		UnlockOrder:          entry.UnlockOrder,
		RequiredLevel:        entry.RequiredLevel,
		NormalCoinPrice:      entry.NormalCoinPrice,
		AppliedRuleVersion:   rule.RuleVersion,
		UnlockedAtMs:         trustedNowMs,
		CommittedPlotVersion: candidatePlot.PlotVersion,
		CommittedFarmVersion: candidate.FarmVersion,
	}
	return CandidateState{State: candidate}, decision, nil
}

// CheckUnlockEligibility is the pure PlayerActor-side eligibility check for
// ReserveUnlockCoin (design 32 §2A.4): the player must meet the frozen level
// requirement and hold enough available normal coin. It never mutates
// anything; the reservation itself is player authority.
func CheckUnlockEligibility(decision PlotUnlockDecision, ownerLevel uint32, availableNormalCoin uint64) *Error {
	if ownerLevel < decision.RequiredLevel {
		return reject(CodeLevelRequirementNotMet,
			"level %d < required %d", ownerLevel, decision.RequiredLevel)
	}
	if availableNormalCoin < decision.NormalCoinPrice {
		return reject(CodeInsufficientCoin,
			"coin %d < price %d", availableNormalCoin, decision.NormalCoinPrice)
	}
	return nil
}

// checkedAddTime adds a non-negative duration to a timestamp with overflow
// protection (design 32 §3.3: mature_at must use checked arithmetic).
func checkedAddTime(baseMs, deltaMs int64) (int64, *Error) {
	if deltaMs < 0 {
		return 0, reject(CodeInvalidArgument, "negative time delta %d", deltaMs)
	}
	if baseMs > 0 && deltaMs > (int64(^uint64(0)>>1))-baseMs {
		return 0, reject(CodeInvalidArgument, "time addition overflows: %d + %d", baseMs, deltaMs)
	}
	return baseMs + deltaMs, nil
}

// incidentID derives the deterministic incident identity of a cycle (design
// 32 §3.3: reload or retry of the same cycle must yield identical incident
// IDs and times). The cycle ID is part of the identity, so an old cycle's
// incident can never match the next cycle (B3).
func incidentID(cycleID string, careType CareType) string {
	return fmt.Sprintf("%s#%s#1", cycleID, careType.String())
}
