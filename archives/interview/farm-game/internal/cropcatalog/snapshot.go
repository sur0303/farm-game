package cropcatalog

import (
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	contractspb "farmgame/api/gen/contracts"
	contractshash "farmgame/internal/contracts"

	"google.golang.org/protobuf/proto"
)

// VersionedDefinition is the catalog entry consumed by the Plant snapshot
// builder. It is an alias, rather than a second mutable catalog type, so the
// catalog remains the single source of truth for both the legacy demo view
// and the server-authoritative Plant rules.
type VersionedDefinition = Definition

const (
	// CropRuleSnapshotSchemaVersion is the generated contract schema accepted
	// by SC1 Plant and FarmActor CommitPlant.
	CropRuleSnapshotSchemaVersion uint32 = 1

	// P0 theft is ten independent 2-percent slots with a hard 20-percent cap.
	P0StealPolicyVersion   uint64 = 1
	P0StealTotalCapRatioBP uint32 = 2000
	P0StealSlotRatioBP     uint32 = 200
	P0StealSlotCount              = 10

	// Farm converts a full intercepted P0 slot to coins with a uint64
	// multiplication. BaseYield is bounded by the SC1/Farm int32 admission
	// range and each slot is exactly 2%, so this is the largest legal slot
	// quantity for which CropUnitCoinValue must remain safe.
	maxP0StealSlotQuantity uint64 = uint64(math.MaxInt32) * uint64(P0StealSlotRatioBP) / 10000
	maxP0CropUnitCoinValue uint64 = math.MaxUint64 / maxP0StealSlotQuantity

	// P0 care uses one fixed opportunity of each supported type. The offsets
	// themselves remain per-definition inputs and are frozen into the output.
	P0CareRuleVersion uint64 = 1
)

// BuildCropRuleSnapshot maps one complete, versioned catalog definition to a
// fresh CropRuleSnapshot. Every mutable protobuf child and repeated field is
// allocated by this call; no part of the result aliases catalog storage or a
// previous result. Callers must treat the returned protobuf as immutable once
// it is handed to a workflow.
//
// The builder is deliberately narrow: it only emits the frozen Plant
// contract and does not provide a general configuration or remote reload
// mechanism.
func BuildCropRuleSnapshot(definition VersionedDefinition) (*contractspb.CropRuleSnapshot, error) {
	baseYield, growDurationMs, waterOffsetMs, pestOffsetMs, coinValue, err := validateDefinition(definition)
	if err != nil {
		return nil, err
	}

	activityID := definition.ActivityID
	waterOffset := waterOffsetMs
	pestOffset := pestOffsetMs

	snapshot := &contractspb.CropRuleSnapshot{
		SchemaVersion:       CropRuleSnapshotSchemaVersion,
		CropDefinitionId:    uint32(definition.ID),
		CropRuleVersion:     definition.CropRuleVersion,
		SeedItemId:          uint32(definition.SeedItemID),
		HarvestItemId:       uint32(definition.ProduceItemID),
		GrowDurationMs:      growDurationMs,
		BaseYield:           baseYield,
		HarvestExp:          definition.HarvestExp,
		CropUnitCoinValue:   coinValue,
		LegacySeedPlantable: definition.LegacySeedPlantable,
		CompendiumRule: &contractspb.CompendiumUnlockRuleSnapshot{
			CropDefinitionId:         uint32(definition.ID),
			UnlockEligible:           definition.UnlockEligible,
			CatalogVersionAtPlant:    definition.CatalogVersion,
			DefinitionVersionAtPlant: definition.DefinitionVersion,
		},
		StealPlanTemplate: &contractspb.StealPlanTemplate{
			PolicyVersion:   definition.StealPolicyVersion,
			TotalCapRatioBp: P0StealTotalCapRatioBP,
			SlotRatioBp:     make([]uint32, P0StealSlotCount),
		},
		CarePlanTemplate: &contractspb.CarePlanTemplate{
			CareRuleVersion: definition.CareRuleVersion,
			Incidents: []*contractspb.CarePlanIncidentTemplate{
				{
					CareType:      contractspb.CareType_CARE_TYPE_WATER,
					Ordinal:       1,
					ScheduleMode:  contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_FIXED_OFFSET,
					FixedOffsetMs: &waterOffset,
				},
				{
					CareType:      contractspb.CareType_CARE_TYPE_REMOVE_PEST,
					Ordinal:       1,
					ScheduleMode:  contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_FIXED_OFFSET,
					FixedOffsetMs: &pestOffset,
				},
			},
		},
	}
	if activityID != "" {
		snapshot.CompendiumRule.ActivityId = &activityID
	}
	for i := range snapshot.StealPlanTemplate.SlotRatioBp {
		snapshot.StealPlanTemplate.SlotRatioBp[i] = P0StealSlotRatioBP
	}

	if err := ValidateCropRuleSnapshot(snapshot); err != nil {
		return nil, err
	}
	if _, err := CropRuleSnapshotHash(snapshot); err != nil {
		return nil, fmt.Errorf("cropcatalog: snapshot is not canonically hashable: %w", err)
	}
	return snapshot, nil
}

// BuildSnapshot is the short catalog-facing spelling retained for callers
// that do not need to mention the generated contract type.
func BuildSnapshot(definition VersionedDefinition) (*contractspb.CropRuleSnapshot, error) {
	return BuildCropRuleSnapshot(definition)
}

// CropRuleSnapshotHash validates and hashes the complete snapshot using the
// same canonical encoding consumed by Plant and Participant idempotency.
func CropRuleSnapshotHash(snapshot *contractspb.CropRuleSnapshot) ([32]byte, error) {
	if err := ValidateCropRuleSnapshot(snapshot); err != nil {
		return [32]byte{}, err
	}
	return contractshash.CanonicalHash(snapshot)
}

// SnapshotHash is an ergonomic alias for CropRuleSnapshotHash.
func SnapshotHash(snapshot *contractspb.CropRuleSnapshot) ([32]byte, error) {
	return CropRuleSnapshotHash(snapshot)
}

// CloneCropRuleSnapshot returns a detached protobuf graph. It is useful at
// an authority boundary when a caller needs to retain the frozen value while
// preventing a mutable input protobuf from being reused by another request.
func CloneCropRuleSnapshot(snapshot *contractspb.CropRuleSnapshot) (*contractspb.CropRuleSnapshot, error) {
	if err := ValidateCropRuleSnapshot(snapshot); err != nil {
		return nil, err
	}
	return proto.Clone(snapshot).(*contractspb.CropRuleSnapshot), nil
}

func validateDefinition(definition VersionedDefinition) (baseYield uint64, growDurationMs, waterOffsetMs, pestOffsetMs int64, coinValue uint64, err error) {
	if definition.ID <= 0 || definition.SeedItemID <= 0 || definition.ProduceItemID <= 0 {
		return 0, 0, 0, 0, 0, fmt.Errorf("cropcatalog: definition and item IDs must be positive")
	}
	if definition.CatalogVersion == 0 || definition.DefinitionVersion == 0 || definition.CropRuleVersion == 0 {
		return 0, 0, 0, 0, 0, fmt.Errorf("cropcatalog: catalog, definition, and crop rule versions are required")
	}
	if definition.CropRuleVersion > math.MaxInt32 {
		return 0, 0, 0, 0, 0, fmt.Errorf("cropcatalog: crop rule version %d exceeds int32 participant range", definition.CropRuleVersion)
	}
	if definition.StealPolicyVersion == 0 || definition.CareRuleVersion == 0 {
		return 0, 0, 0, 0, 0, fmt.Errorf("cropcatalog: steal and care rule versions are required")
	}
	if !utf8.ValidString(definition.ActivityID) {
		return 0, 0, 0, 0, 0, fmt.Errorf("cropcatalog: activity ID must be valid UTF-8")
	}
	if definition.ActivityID != "" && strings.TrimSpace(definition.ActivityID) == "" {
		return 0, 0, 0, 0, 0, fmt.Errorf("cropcatalog: activity ID cannot be whitespace")
	}
	if definition.ActivityID != "" && !definition.LegacySeedPlantable {
		return 0, 0, 0, 0, 0, fmt.Errorf("cropcatalog: activity definitions must remain legacy-seed plantable")
	}

	if definition.MatureAfter <= 0 || definition.MatureAfter%time.Millisecond != 0 {
		return 0, 0, 0, 0, 0, fmt.Errorf("cropcatalog: mature duration must be a positive whole millisecond")
	}
	growDurationMs = int64(definition.MatureAfter / time.Millisecond)
	if growDurationMs <= 0 {
		return 0, 0, 0, 0, 0, fmt.Errorf("cropcatalog: mature duration overflows millisecond representation")
	}

	baseYield = definition.SnapshotBaseYield
	if baseYield == 0 || baseYield > math.MaxInt32 || baseYield%50 != 0 {
		return 0, 0, 0, 0, 0, fmt.Errorf("cropcatalog: snapshot base yield %d must be a positive multiple of 50 within int32 range", baseYield)
	}

	coinValue = definition.CropUnitCoinValue
	if err := validateCropUnitCoinValue(coinValue); err != nil {
		return 0, 0, 0, 0, 0, err
	}

	waterOffsetMs, err = careOffsetMs("WATER", definition.WaterOffset, growDurationMs)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	pestOffsetMs, err = careOffsetMs("REMOVE_PEST", definition.RemovePestOffset, growDurationMs)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}

	product := baseYield * uint64(P0StealSlotRatioBP)
	if product%10000 != 0 || product/10000 == 0 {
		return 0, 0, 0, 0, 0, fmt.Errorf("cropcatalog: P0 steal slot quantity is not a positive exact integer")
	}
	total := uint64(P0StealSlotCount) * (product / 10000)
	if total != baseYield*uint64(P0StealTotalCapRatioBP)/10000 || total > baseYield {
		return 0, 0, 0, 0, 0, fmt.Errorf("cropcatalog: P0 steal quantities do not meet the 20%% cap")
	}
	return baseYield, growDurationMs, waterOffsetMs, pestOffsetMs, coinValue, nil
}

func validateCropUnitCoinValue(value uint64) error {
	if value == 0 {
		return fmt.Errorf("cropcatalog: crop unit coin value is required")
	}
	if value > maxP0CropUnitCoinValue {
		return fmt.Errorf("cropcatalog: crop unit coin value %d exceeds P0 guard multiplication bound %d for max slot quantity %d", value, maxP0CropUnitCoinValue, maxP0StealSlotQuantity)
	}
	return nil
}

func careOffsetMs(name string, offset time.Duration, growDurationMs int64) (int64, error) {
	if offset <= 0 || offset%time.Millisecond != 0 {
		return 0, fmt.Errorf("cropcatalog: %s offset must be a positive whole millisecond", name)
	}
	offsetMs := int64(offset / time.Millisecond)
	if offsetMs <= 0 || offsetMs >= growDurationMs {
		return 0, fmt.Errorf("cropcatalog: %s offset %d must be before grow duration %d", name, offsetMs, growDurationMs)
	}
	return offsetMs, nil
}

// ValidateCropRuleSnapshot mirrors the complete semantic admission shared by
// Plant and Farm CommitPlant. It is intentionally independent of either
// workflow or actor package so catalog validation happens before effects.
func ValidateCropRuleSnapshot(snapshot *contractspb.CropRuleSnapshot) error {
	if snapshot == nil || snapshot.GetSchemaVersion() != CropRuleSnapshotSchemaVersion ||
		snapshot.GetCropDefinitionId() == 0 || snapshot.GetCropRuleVersion() == 0 ||
		snapshot.GetCropRuleVersion() > math.MaxInt32 || snapshot.GetSeedItemId() == 0 ||
		snapshot.GetHarvestItemId() == 0 || snapshot.GetGrowDurationMs() <= 0 ||
		snapshot.GetBaseYield() == 0 || snapshot.GetBaseYield() > math.MaxInt32 ||
		snapshot.GetBaseYield()%50 != 0 || snapshot.GetCropDefinitionId() > math.MaxInt32 {
		return fmt.Errorf("cropcatalog: snapshot top-level fields are invalid")
	}
	if err := validateCropUnitCoinValue(snapshot.GetCropUnitCoinValue()); err != nil {
		return err
	}

	compendium := snapshot.GetCompendiumRule()
	if compendium == nil || compendium.GetCropDefinitionId() != snapshot.GetCropDefinitionId() ||
		compendium.GetCatalogVersionAtPlant() == 0 || compendium.GetDefinitionVersionAtPlant() == 0 {
		return fmt.Errorf("cropcatalog: snapshot compendium rule is incomplete")
	}
	if !utf8.ValidString(compendium.GetActivityId()) {
		return fmt.Errorf("cropcatalog: snapshot activity ID must be valid UTF-8")
	}

	steal := snapshot.GetStealPlanTemplate()
	if steal == nil || steal.GetPolicyVersion() == 0 || steal.GetTotalCapRatioBp() != P0StealTotalCapRatioBP ||
		len(steal.GetSlotRatioBp()) != P0StealSlotCount {
		return fmt.Errorf("cropcatalog: snapshot steal plan is not P0")
	}
	var ratioSum uint32
	for _, ratio := range steal.GetSlotRatioBp() {
		if ratio != P0StealSlotRatioBP {
			return fmt.Errorf("cropcatalog: snapshot steal plan must contain ten 2-percent slots")
		}
		ratioSum += ratio
	}
	if ratioSum != steal.GetTotalCapRatioBp() {
		return fmt.Errorf("cropcatalog: snapshot steal slot ratios do not equal the cap")
	}

	care := snapshot.GetCarePlanTemplate()
	if care == nil || care.GetCareRuleVersion() == 0 || len(care.GetIncidents()) != 2 {
		return fmt.Errorf("cropcatalog: snapshot care plan must contain WATER and REMOVE_PEST")
	}
	seen := make(map[contractspb.CareType]struct{}, len(care.GetIncidents()))
	for _, incident := range care.GetIncidents() {
		if incident == nil || incident.GetOrdinal() != 1 ||
			incident.GetScheduleMode() != contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_FIXED_OFFSET ||
			incident.FixedOffsetMs == nil || incident.GetFixedOffsetMs() <= 0 ||
			incident.GetFixedOffsetMs() >= snapshot.GetGrowDurationMs() ||
			incident.RandomWindowStartMs != nil || incident.RandomWindowEndMs != nil || incident.RandomAlgorithmVersion != nil {
			return fmt.Errorf("cropcatalog: snapshot care incident is not a valid fixed P0 schedule")
		}
		switch incident.GetCareType() {
		case contractspb.CareType_CARE_TYPE_WATER, contractspb.CareType_CARE_TYPE_REMOVE_PEST:
		default:
			return fmt.Errorf("cropcatalog: snapshot care incident type is unknown")
		}
		if _, exists := seen[incident.GetCareType()]; exists {
			return fmt.Errorf("cropcatalog: snapshot care incident type is duplicated")
		}
		seen[incident.GetCareType()] = struct{}{}
	}
	if len(seen) != 2 {
		return fmt.Errorf("cropcatalog: snapshot care plan is missing an opportunity")
	}
	return nil
}
