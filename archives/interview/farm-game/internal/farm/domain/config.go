package domain

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// ScheduleMode selects how care incident due times are produced at plant
// time (design 32 §3.3).
type ScheduleMode int32

const (
	ScheduleModeUnspecified ScheduleMode = iota
	// ScheduleModeFixedOffset: due_at = plant_at + configured offset, frozen
	// once at plant time. P0 default.
	ScheduleModeFixedOffset
	// ScheduleModeDeterministicRandomWindow: reserved future mode. The
	// stable seed is fixed at plant time and the final due_at is still
	// persisted into the same CarePlan; in-flight cycles never re-roll.
	ScheduleModeDeterministicRandomWindow
)

// DefaultPlotUnlockRule is the frozen P0 rule used by FarmActor and the
// UnlockPlot coordinator. It is returned by value so callers cannot mutate a
// shared process-global rule while a Workflow is in flight.
func DefaultPlotUnlockRule() PlotUnlockRule {
	rule := PlotUnlockRule{
		RuleVersion:          1,
		MaxPlots:             P0MaxPlots,
		InitialUnlockedPlots: P0InitialUnlockedPlots,
		Entries:              make([]PlotUnlockEntry, 0, P0MaxPlots-P0InitialUnlockedPlots),
	}
	for plotID := uint32(P0InitialUnlockedPlots + 1); plotID <= P0MaxPlots; plotID++ {
		rule.Entries = append(rule.Entries, PlotUnlockEntry{
			PlotID:          plotID,
			UnlockOrder:     plotID,
			RequiredLevel:   plotID - P0InitialUnlockedPlots,
			NormalCoinPrice: uint64(plotID-P0InitialUnlockedPlots) * 100,
		})
	}
	return rule
}

// PlotUnlockRuleSnapshotHash is the canonical frozen-rule digest carried by
// the UnlockPlot workflow and rechecked by both Player and Farm participants.
// It contains the complete P0 shape, so a participant cannot accept a hash
// that merely has the right length while silently changing price/order data.
func PlotUnlockRuleSnapshotHash(rule PlotUnlockRule) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("UNLOCK_PLOT_RULE_V1"))
	_, _ = h.Write([]byte(strconv.FormatUint(rule.RuleVersion, 10)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strconv.FormatUint(uint64(rule.MaxPlots), 10)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strconv.FormatUint(uint64(rule.InitialUnlockedPlots), 10)))
	for _, entry := range rule.Entries {
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strconv.FormatUint(uint64(entry.PlotID), 10)))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strconv.FormatUint(uint64(entry.UnlockOrder), 10)))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strconv.FormatUint(uint64(entry.RequiredLevel), 10)))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strconv.FormatUint(entry.NormalCoinPrice, 10)))
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

// InterceptPolicy selects how much of a gross steal the guard intercepts
// (design 32 Q32-06B).
type InterceptPolicy int32

const (
	InterceptPolicyUnspecified InterceptPolicy = iota
	// InterceptPolicyFullGross: P0 binary interception, guard_lost is either
	// 0 or the full gross quantity of the current slot.
	InterceptPolicyFullGross
)

// PlotUnlockEntry is the versioned purchase rule of one plot (design 32
// §2A.1). Reaching RequiredLevel only makes the plot purchasable; the player
// must still click and pay NormalCoinPrice.
type PlotUnlockEntry struct {
	PlotID          uint32
	UnlockOrder     uint32
	RequiredLevel   uint32
	NormalCoinPrice uint64
}

// PlotUnlockRule is the versioned unlock configuration of the whole farm.
type PlotUnlockRule struct {
	RuleVersion          uint64
	MaxPlots             uint32
	InitialUnlockedPlots uint32
	Entries              []PlotUnlockEntry
}

// EntryOf returns the purchase entry of a plot.
func (r PlotUnlockRule) EntryOf(plotID uint32) (PlotUnlockEntry, bool) {
	for _, entry := range r.Entries {
		if entry.PlotID == plotID {
			return entry, true
		}
	}
	return PlotUnlockEntry{}, false
}

// CarePlanConfig is the versioned care schedule of a crop rule.
type CarePlanConfig struct {
	CareRuleVersion uint64
	ScheduleMode    ScheduleMode
	// WaterEnabled / PestEnabled decide whether the cycle gets the
	// corresponding incident at all (design 32 §3.3).
	WaterEnabled  bool
	WaterOffsetMs int64
	PestEnabled   bool
	PestOffsetMs  int64
}

// StealPlanConfig is the versioned steal curve of a crop rule (design 32
// Q32-06A). P0: total cap 20%, ten 2% slots.
type StealPlanConfig struct {
	PolicyVersion   uint64
	TotalCapRatioBP uint32
	SlotRatioBP     []uint32
}

// CropRule is the versioned, server-authoritative definition used to freeze
// a crop cycle at plant time.
type CropRule struct {
	CropDefinitionID uint32
	RuleVersion      uint64
	GrowDurationMs   int64
	BaseYield        uint32
	// HarvestExp is the frozen experience granted on harvest (settled by
	// PlayerActor; the farm domain only carries the frozen number).
	HarvestExp        uint64
	CropUnitCoinValue uint64
	StealPlan         StealPlanConfig
	CarePlan          CarePlanConfig
}

// GuardRule is the versioned guard rule of a pet (design 36 §3.4).
type GuardRule struct {
	PetConfigVersion       uint64
	GuardRuleVersion       uint64
	TriggerProbabilityBP   uint32
	InterceptPolicy        InterceptPolicy
	RandomAlgorithmVersion uint64
	RandomKeyVersion       uint64
}

// maxTotalCapRatioBP is the hard product cap: cumulative theft of one cycle
// may never exceed 20% of base yield (design 32 Q32-06A/Q32-12).
const maxTotalCapRatioBP = 2000

// basisPointsDenominator converts ratio basis points to quantities.
const basisPointsDenominator = 10000

// Config validation errors. They are returned when a versioned config is
// published or loaded, never during a player command.
var (
	ErrConfigMaxPlots          = errors.New("farm domain config: max_plots must be 24")
	ErrConfigInitialPlots      = errors.New("farm domain config: initial unlocked plots must be 5 and <= max_plots")
	ErrConfigUnlockSequence    = errors.New("farm domain config: unlock entries must cover plots initial+1..max with plot_id == unlock_order, contiguous and unique")
	ErrConfigGrowDuration      = errors.New("farm domain config: grow_duration_ms must be > 0")
	ErrConfigBaseYield         = errors.New("farm domain config: base_yield must be > 0")
	ErrConfigStealCapExceeded  = errors.New("farm domain config: steal slot ratios must sum to total_cap_ratio_bp and never exceed 2000bp (20%)")
	ErrConfigStealSlotRounding = errors.New("farm domain config: every steal slot must convert to an exact positive integer quantity")
	ErrConfigStealLimit        = errors.New("farm domain config: stealable_limit must not exceed base_yield")
	ErrConfigCareOffset        = errors.New("farm domain config: enabled care incidents require schedule mode and non-negative offset")
	ErrConfigGuardProbability  = errors.New("farm domain config: trigger_probability_bp must be within [0, 10000]")
	ErrConfigInterceptPolicy   = errors.New("farm domain config: P0 intercept_policy must be FULL_GROSS")
)

// P0 plot scale frozen by design 32 §0.4.
const (
	P0MaxPlots             = 24
	P0InitialUnlockedPlots = 5
)

// ValidatePlotUnlockRule enforces the publish-time invariants of the
// versioned unlock rule: exactly 24 plots, exactly 5 initially unlocked, and
// one contiguous fixed purchase order for plots 6..24.
func ValidatePlotUnlockRule(rule PlotUnlockRule) error {
	if rule.MaxPlots != P0MaxPlots {
		return fmt.Errorf("%w, got %d", ErrConfigMaxPlots, rule.MaxPlots)
	}
	if rule.InitialUnlockedPlots != P0InitialUnlockedPlots ||
		rule.InitialUnlockedPlots > rule.MaxPlots {
		return fmt.Errorf("%w, got %d", ErrConfigInitialPlots, rule.InitialUnlockedPlots)
	}
	want := int(rule.MaxPlots - rule.InitialUnlockedPlots)
	if len(rule.Entries) != want {
		return fmt.Errorf("%w, got %d entries want %d", ErrConfigUnlockSequence, len(rule.Entries), want)
	}
	seen := make(map[uint32]bool, len(rule.Entries))
	for i, entry := range rule.Entries {
		wantID := rule.InitialUnlockedPlots + uint32(i) + 1
		if entry.PlotID != wantID || entry.UnlockOrder != wantID || seen[entry.PlotID] {
			return fmt.Errorf("%w, entry %d has plot_id=%d unlock_order=%d want %d",
				ErrConfigUnlockSequence, i, entry.PlotID, entry.UnlockOrder, wantID)
		}
		seen[entry.PlotID] = true
	}
	return nil
}

// ValidateCropRule enforces the publish-time invariants of a crop rule
// (design 32 Q32-12): slot ratios sum exactly to the total cap, the cap
// never exceeds 20%, every slot converts to an exact positive integer
// quantity (which forces base_yield to be a multiple of 50 for the P0 2%
// curve), and all arithmetic is overflow-checked.
func ValidateCropRule(rule CropRule) error {
	if rule.GrowDurationMs <= 0 {
		return ErrConfigGrowDuration
	}
	if rule.BaseYield == 0 {
		return ErrConfigBaseYield
	}
	if _, _, err := stealQuantities(rule.BaseYield, rule.StealPlan); err != nil {
		return err
	}
	if rule.CarePlan.WaterEnabled || rule.CarePlan.PestEnabled {
		if rule.CarePlan.ScheduleMode != ScheduleModeFixedOffset {
			return fmt.Errorf("%w: schedule_mode=%d", ErrConfigCareOffset, rule.CarePlan.ScheduleMode)
		}
	}
	if rule.CarePlan.WaterEnabled && rule.CarePlan.WaterOffsetMs < 0 {
		return fmt.Errorf("%w: water offset %d", ErrConfigCareOffset, rule.CarePlan.WaterOffsetMs)
	}
	if rule.CarePlan.PestEnabled && rule.CarePlan.PestOffsetMs < 0 {
		return fmt.Errorf("%w: pest offset %d", ErrConfigCareOffset, rule.CarePlan.PestOffsetMs)
	}
	return nil
}

// ValidateGuardRule enforces the publish-time invariants of a guard rule
// (design 36 §8.1).
func ValidateGuardRule(rule GuardRule) error {
	if rule.TriggerProbabilityBP > basisPointsDenominator {
		return fmt.Errorf("%w, got %d", ErrConfigGuardProbability, rule.TriggerProbabilityBP)
	}
	if rule.InterceptPolicy != InterceptPolicyFullGross {
		return fmt.Errorf("%w, got %d", ErrConfigInterceptPolicy, rule.InterceptPolicy)
	}
	return nil
}

// stealQuantities deterministically converts a steal plan into per-slot
// quantities and the total stealable limit. It is used both at config
// validation time and at plant time (with the already validated rule).
func stealQuantities(baseYield uint32, plan StealPlanConfig) (slotQuantity []uint32, limit uint32, err error) {
	var sumBP uint32
	for _, bp := range plan.SlotRatioBP {
		sumBP += bp
	}
	if sumBP != plan.TotalCapRatioBP || sumBP > maxTotalCapRatioBP {
		return nil, 0, fmt.Errorf("%w, slots sum %dbp cap %dbp",
			ErrConfigStealCapExceeded, sumBP, plan.TotalCapRatioBP)
	}
	quantities := make([]uint32, len(plan.SlotRatioBP))
	var total uint32
	for i, bp := range plan.SlotRatioBP {
		product := uint64(baseYield) * uint64(bp)
		if product > math.MaxUint32*basisPointsDenominator {
			return nil, 0, fmt.Errorf("%w: slot %d overflows", ErrConfigStealSlotRounding, i)
		}
		if product%basisPointsDenominator != 0 {
			return nil, 0, fmt.Errorf("%w: base_yield=%d slot %d at %dbp is not an exact integer",
				ErrConfigStealSlotRounding, baseYield, i, bp)
		}
		quantity := uint32(product / basisPointsDenominator)
		if quantity == 0 {
			return nil, 0, fmt.Errorf("%w: base_yield=%d slot %d at %dbp rounds to zero",
				ErrConfigStealSlotRounding, baseYield, i, bp)
		}
		quantities[i] = quantity
		total += quantity
	}
	if total > baseYield {
		return nil, 0, fmt.Errorf("%w: limit=%d base_yield=%d", ErrConfigStealLimit, total, baseYield)
	}
	return quantities, total, nil
}
