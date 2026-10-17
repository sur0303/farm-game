package domain

// CareType identifies the kind of a care incident. WATER and REMOVE_PEST are
// independent incidents of one crop cycle (design 32, Q32-04).
type CareType int32

const (
	CareTypeUnspecified CareType = iota
	CareTypeWater
	CareTypeRemovePest
)

func (t CareType) String() string {
	switch t {
	case CareTypeWater:
		return "WATER"
	case CareTypeRemovePest:
		return "REMOVE_PEST"
	default:
		return "UNSPECIFIED"
	}
}

// PlotLockState is the persistent lock state of a plot.
type PlotLockState int32

const (
	PlotLocked PlotLockState = iota
	PlotUnlocked
)

// CycleEndReason records why a crop cycle ended.
type CycleEndReason int32

const (
	CycleEndReasonUnspecified CycleEndReason = iota
	CycleEndReasonHarvested
)

// CropPhase is the display phase derived from trusted time; it is never
// persisted (design 32 §4.1).
type CropPhase int32

const (
	CropPhaseGrowing CropPhase = iota
	CropPhaseMature
)

// IncidentPhase is the turn-local derived phase of a care incident; it is
// never persisted (design 32 §4.1/§5.3).
type IncidentPhase int32

const (
	// IncidentPending: not resolved and trustedNowMs < dueAtMs.
	IncidentPending IncidentPhase = iota
	// IncidentActive: not resolved and trustedNowMs >= dueAtMs.
	IncidentActive
	// IncidentResolved: resolvedAtMs present.
	IncidentResolved
)

// Plot is one of at most 24 stable slots of a farm.
type Plot struct {
	PlotID      uint32
	LockState   PlotLockState
	PlotVersion uint64
	// Unlock provenance; empty for the initial free plots which never
	// produce a purchase record (design 32 §2A.1).
	UnlockWorkflowID  string
	UnlockRuleVersion uint64
	UnlockedAtMs      int64
	ActiveCropCycleID string
}

// Unlocked reports whether the plot can be planted/cared/stolen from.
func (p Plot) Unlocked() bool { return p.LockState == PlotUnlocked }

// CareIncident is a farm-global pending care event of one crop cycle. The
// first legal care resolves it for everyone; later requests are NO_CHANGE
// (design 32 §5.1).
type CareIncident struct {
	IncidentID         string
	CareType           CareType
	DueAtMs            int64
	ResolvedAtMs       int64 // 0 = unresolved
	ResolvedByPlayerID uint64
}

// Resolved reports whether the incident has been resolved.
func (i CareIncident) Resolved() bool { return i.ResolvedAtMs != 0 }

// Phase derives the incident phase from a single trusted timestamp.
func (i CareIncident) Phase(trustedNowMs int64) IncidentPhase {
	if i.Resolved() {
		return IncidentResolved
	}
	if trustedNowMs < i.DueAtMs {
		return IncidentPending
	}
	return IncidentActive
}

// StealPlanSnapshot is the versioned steal curve frozen into the crop cycle
// at plant time (design 32 §6.1, Q32-06A/Q32-12).
type StealPlanSnapshot struct {
	PolicyVersion   uint64
	TotalCapRatioBP uint32
	SlotRatioBP     []uint32
	SlotQuantity    []uint32
}

// CarePlanSnapshot is the versioned care plan frozen into the crop cycle at
// plant time (design 32 §3.3, Q32-04). P0 uses FIXED_OFFSET: incident due
// times are computed once as plantAt + configured offset.
type CarePlanSnapshot struct {
	CareRuleVersion uint64
	ScheduleMode    ScheduleMode
	// RandomSeed is reserved for the future DETERMINISTIC_RANDOM_WINDOW
	// schedule mode; P0 FIXED_OFFSET never populates it.
	RandomSeed uint64
}

// CompendiumUnlockSnapshot freezes the compendium eligibility of this cycle
// at plant time (design 32 §3.3 / design 37). Harvest passes it through
// unchanged; nothing may re-evaluate it for an in-flight cycle.
type CompendiumUnlockSnapshot struct {
	CropDefinitionID         uint32
	UnlockEligible           bool
	PlantedByPlayerID        uint64
	ActivityID               string
	CatalogVersionAtPlant    uint64
	DefinitionVersionAtPlant uint64
}

// TheftAttemptRecord is the per-visitor-per-cycle theft receipt. Records live
// inside their crop cycle, so an ended cycle can never leak attempts into the
// next cycle (design 32 B3).
type TheftAttemptRecord struct {
	VisitorPlayerID uint64
	StealSlotIndex  uint32
	DecidedAtMs     int64
}

// CropCycle is one single-season crop lifecycle (design 32 §3.3/§4.1). All
// rule parameters are frozen at plant time.
type CropCycle struct {
	CycleID          string
	PlotID           uint32
	CropDefinitionID uint32
	RuleVersion      uint64
	PlantAtMs        int64
	MatureAtMs       int64
	BaseYield        uint32
	StealableLimit   uint32
	// HarvestExp is the frozen harvest experience (settled by PlayerActor).
	HarvestExp uint64
	// CropUnitCoinValue is the frozen per-crop coin value used to compensate
	// the owner for guard-intercepted crops (design 32 §6.3).
	CropUnitCoinValue  uint64
	StealPlan          StealPlanSnapshot
	CarePlan           CarePlanSnapshot
	Compendium         CompendiumUnlockSnapshot
	Incidents          []CareIncident
	StolenAmount       uint32
	NextStealSlotIndex uint32
	Attempts           []TheftAttemptRecord
	Ended              bool
	EndReason          CycleEndReason
}

// Phase derives the display phase from a single trusted timestamp. P0 has no
// WITHER state: a mature cycle stays harvestable indefinitely (design 32
// §4.1, S05).
func (c *CropCycle) Phase(trustedNowMs int64) CropPhase {
	if trustedNowMs < c.MatureAtMs {
		return CropPhaseGrowing
	}
	return CropPhaseMature
}

// MatureAt reports the frozen maturity instant.
func (c *CropCycle) Mature(trustedNowMs int64) bool {
	return c.Phase(trustedNowMs) == CropPhaseMature
}

// OwnerClaimable is the quantity the owner can still harvest.
func (c *CropCycle) OwnerClaimable() uint32 { return c.BaseYield - c.StolenAmount }

// RemainingStealable is the quota still available to thieves.
func (c *CropCycle) RemainingStealable() uint32 { return c.StealableLimit - c.StolenAmount }

// HasAttempt reports whether the visitor already consumed their one legal
// attempt on this cycle.
func (c *CropCycle) HasAttempt(visitorPlayerID uint64) bool {
	for _, attempt := range c.Attempts {
		if attempt.VisitorPlayerID == visitorPlayerID {
			return true
		}
	}
	return false
}

// FindIncident locates an incident by ID.
func (c *CropCycle) FindIncident(incidentID string) *CareIncident {
	for i := range c.Incidents {
		if c.Incidents[i].IncidentID == incidentID {
			return &c.Incidents[i]
		}
	}
	return nil
}

// GuardSnapshot is the versioned copy of the owner's pet guard state used by
// theft decisions (design 36 §6.2). It is applied monotonically by
// GuardVersion and never re-read from hot config during a decision.
type GuardSnapshot struct {
	SchemaVersion    uint64
	GuardVersion     uint64
	ActiveGuardPetID uint32 // 0 = no pet selected
	GuardUntilMs     int64
	Rule             GuardRule
	SnapshotHash     string
}

// Effective reports whether the guard currently intercepts theft:
// a pet is selected and trustedNowMs < guardUntilMs. Expiry is lazily
// derived; no EXPIRED state is written (design 36 §4.5).
func (g GuardSnapshot) Effective(trustedNowMs int64) bool {
	return g.ActiveGuardPetID != 0 && trustedNowMs < g.GuardUntilMs
}

// FarmState is the authoritative farm-side domain state a FarmActor turn
// adjudicates on. Versions are advanced only by successful decisions.
type FarmState struct {
	FarmID        uint64
	OwnerPlayerID uint64
	FarmVersion   uint64
	Plots         []Plot
	Cycles        []CropCycle
	Guard         GuardSnapshot
	// Header is the fixed-size derived opportunity head persisted with the
	// authority record (design 27 §4.2). It is recomputed from the same
	// CandidateState on every opportunity-changing commit.
	Header OpportunityHeader
}

// Clone deep-copies the state so decision functions can build a
// CandidateState without touching authoritative memory.
func (s FarmState) Clone() FarmState {
	out := s
	out.Plots = append([]Plot(nil), s.Plots...)
	out.Cycles = make([]CropCycle, len(s.Cycles))
	for i, cycle := range s.Cycles {
		out.Cycles[i] = *cloneCycle(&cycle)
	}
	out.Header = s.Header
	return out
}

func cloneCycle(c *CropCycle) *CropCycle {
	out := *c
	out.StealPlan.SlotRatioBP = append([]uint32(nil), c.StealPlan.SlotRatioBP...)
	out.StealPlan.SlotQuantity = append([]uint32(nil), c.StealPlan.SlotQuantity...)
	out.Incidents = append([]CareIncident(nil), c.Incidents...)
	out.Attempts = append([]TheftAttemptRecord(nil), c.Attempts...)
	return &out
}

// FindPlot locates a plot by ID.
func (s *FarmState) FindPlot(plotID uint32) *Plot {
	for i := range s.Plots {
		if s.Plots[i].PlotID == plotID {
			return &s.Plots[i]
		}
	}
	return nil
}

// FindCycle locates a cycle by ID.
func (s *FarmState) FindCycle(cycleID string) *CropCycle {
	if cycleID == "" {
		return nil
	}
	for i := range s.Cycles {
		if s.Cycles[i].CycleID == cycleID {
			return &s.Cycles[i]
		}
	}
	return nil
}

// ActiveCycleOf returns the cycle currently occupying the plot, or nil.
func (s *FarmState) ActiveCycleOf(plotID uint32) (*Plot, *CropCycle) {
	plot := s.FindPlot(plotID)
	if plot == nil {
		return nil, nil
	}
	cycle := s.FindCycle(plot.ActiveCropCycleID)
	if cycle == nil || cycle.Ended {
		return plot, nil
	}
	return plot, cycle
}
