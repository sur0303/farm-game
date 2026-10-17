package cropcatalog

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/scenario/plant"

	"google.golang.org/protobuf/proto"
)

func TestBuildCropRuleSnapshotForEveryCatalogEntry(t *testing.T) {
	for _, definition := range All() {
		definition := definition
		t.Run(fmt.Sprintf("definition-%d", definition.ID), func(t *testing.T) {
			snapshot, err := BuildCropRuleSnapshot(definition)
			if err != nil {
				t.Fatal(err)
			}

			if snapshot.GetSchemaVersion() != CropRuleSnapshotSchemaVersion ||
				snapshot.GetCropDefinitionId() != uint32(definition.ID) ||
				snapshot.GetCropRuleVersion() != definition.CropRuleVersion ||
				snapshot.GetSeedItemId() != uint32(definition.SeedItemID) ||
				snapshot.GetHarvestItemId() != uint32(definition.ProduceItemID) ||
				snapshot.GetGrowDurationMs() != int64(definition.MatureAfter/time.Millisecond) ||
				snapshot.GetBaseYield() != definition.SnapshotBaseYield ||
				snapshot.GetHarvestExp() != definition.HarvestExp ||
				snapshot.GetCropUnitCoinValue() != definition.CropUnitCoinValue ||
				snapshot.GetLegacySeedPlantable() != definition.LegacySeedPlantable {
				t.Fatalf("snapshot top-level mapping=%v definition=%+v", snapshot, definition)
			}

			compendium := snapshot.GetCompendiumRule()
			if compendium == nil || compendium.GetCropDefinitionId() != uint32(definition.ID) ||
				compendium.GetUnlockEligible() != definition.UnlockEligible ||
				compendium.GetCatalogVersionAtPlant() != definition.CatalogVersion ||
				compendium.GetDefinitionVersionAtPlant() != definition.DefinitionVersion {
				t.Fatalf("compendium mapping=%v definition=%+v", compendium, definition)
			}
			if compendium.GetActivityId() != definition.ActivityID {
				t.Fatalf("activity_id=%q want %q", compendium.GetActivityId(), definition.ActivityID)
			}

			steal := snapshot.GetStealPlanTemplate()
			if steal == nil || steal.GetPolicyVersion() != definition.StealPolicyVersion ||
				steal.GetTotalCapRatioBp() != P0StealTotalCapRatioBP || len(steal.GetSlotRatioBp()) != P0StealSlotCount {
				t.Fatalf("steal template=%v", steal)
			}
			for _, ratio := range steal.GetSlotRatioBp() {
				if ratio != P0StealSlotRatioBP {
					t.Fatalf("slot ratio=%d", ratio)
				}
			}

			care := snapshot.GetCarePlanTemplate()
			if care == nil || care.GetCareRuleVersion() != definition.CareRuleVersion || len(care.GetIncidents()) != 2 {
				t.Fatalf("care template=%v", care)
			}
			wantOffsets := map[contractspb.CareType]int64{
				contractspb.CareType_CARE_TYPE_WATER:       int64(definition.WaterOffset / time.Millisecond),
				contractspb.CareType_CARE_TYPE_REMOVE_PEST: int64(definition.RemovePestOffset / time.Millisecond),
			}
			for _, incident := range care.GetIncidents() {
				if incident.GetScheduleMode() != contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_FIXED_OFFSET ||
					incident.GetOrdinal() != 1 || incident.GetFixedOffsetMs() != wantOffsets[incident.GetCareType()] {
					t.Fatalf("care incident=%v", incident)
				}
			}

			hash, err := CropRuleSnapshotHash(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			input := plant.PlantInput{
				SubjectPlayerID:         10001,
				OperationID:             fmt.Sprintf("op-catalog-%04d", definition.ID),
				TargetFarmID:            10001,
				PlotID:                  1,
				ExpectedPlotVersion:     1,
				CropDefinitionID:        snapshot.GetCropDefinitionId(),
				ExpectedCropRuleVersion: snapshot.GetCropRuleVersion(),
				SeedItemID:              snapshot.GetSeedItemId(),
				CropRuleSnapshot:        snapshot,
			}
			root, err := plant.BuildRoot(input)
			if err != nil {
				t.Fatalf("SC1 rejected catalog snapshot: %v", err)
			}
			if root.Payload.GetCropRuleSnapshot().GetSeedItemId() != snapshot.GetSeedItemId() ||
				!bytes.Equal(root.Payload.GetCropRuleSnapshotHash(), hash[:]) {
				t.Fatalf("root did not carry mapped seed/hash: payload=%v", root.Payload)
			}
		})
	}
}

func TestCropRuleSnapshotBuildsAreDeterministicAndDetached(t *testing.T) {
	definition := All()[0]
	first, err := BuildCropRuleSnapshot(definition)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildCropRuleSnapshot(definition)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(first, second) {
		t.Fatalf("same definition produced different snapshots\nfirst=%v\nsecond=%v", first, second)
	}
	firstHash, err := CropRuleSnapshotHash(first)
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := CropRuleSnapshotHash(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf("same definition produced different hashes: %x vs %x", firstHash, secondHash)
	}

	first.StealPlanTemplate.SlotRatioBp[0] = 999
	first.CarePlanTemplate.Incidents[0].FixedOffsetMs = new(int64)
	first.CompendiumRule.ActivityId = new(string)
	if second.GetStealPlanTemplate().GetSlotRatioBp()[0] != P0StealSlotRatioBP ||
		second.GetCarePlanTemplate().GetIncidents()[0].GetFixedOffsetMs() == 0 ||
		second.GetCompendiumRule().GetActivityId() != "" {
		t.Fatal("snapshot builds share mutable protobuf state")
	}

	clone, err := CloneCropRuleSnapshot(second)
	if err != nil {
		t.Fatal(err)
	}
	clone.StealPlanTemplate.SlotRatioBp[1] = 998
	if second.GetStealPlanTemplate().GetSlotRatioBp()[1] != P0StealSlotRatioBP {
		t.Fatal("explicit snapshot clone aliases the source")
	}
}

func TestActivitySnapshotKeepsLegacySeedPlantableAndCompendiumProvenance(t *testing.T) {
	definition := All()[0]
	definition.ActivityID = "activity-summer-2026"
	definition.LegacySeedPlantable = true
	definition.UnlockEligible = true

	snapshot, err := BuildCropRuleSnapshot(definition)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.GetLegacySeedPlantable() || !snapshot.GetCompendiumRule().GetUnlockEligible() ||
		snapshot.GetCompendiumRule().GetActivityId() != definition.ActivityID {
		t.Fatalf("activity/legacy provenance was not frozen: %v", snapshot)
	}
}

func TestCropUnitCoinValueGuardMultiplicationBoundary(t *testing.T) {
	definition := All()[0]
	definition.SnapshotBaseYield = uint64(math.MaxInt32 / 50 * 50)
	definition.CropUnitCoinValue = maxP0CropUnitCoinValue

	snapshot, err := BuildCropRuleSnapshot(definition)
	if err != nil {
		t.Fatalf("maximum safe coin value rejected: %v", err)
	}
	if snapshot.GetCropUnitCoinValue() != maxP0CropUnitCoinValue {
		t.Fatalf("coin value=%d want %d", snapshot.GetCropUnitCoinValue(), maxP0CropUnitCoinValue)
	}

	definition.CropUnitCoinValue++
	if snapshot, err := BuildCropRuleSnapshot(definition); err == nil || snapshot != nil {
		t.Fatalf("coin value beyond guard multiplication bound accepted: snapshot=%v err=%v", snapshot, err)
	}

	snapshot.CropUnitCoinValue = maxP0CropUnitCoinValue + 1
	if err := ValidateCropRuleSnapshot(snapshot); err == nil {
		t.Fatal("snapshot validator accepted a coin value that overflows the maximum legal P0 slot")
	}
}

func TestInvalidUTF8ActivityIDRejectedBeforeSnapshotEncoding(t *testing.T) {
	definition := All()[0]
	definition.ActivityID = string([]byte{0xff, 0xfe})

	snapshot, err := BuildCropRuleSnapshot(definition)
	if err == nil || snapshot != nil {
		t.Fatalf("invalid UTF-8 activity ID accepted: snapshot=%v err=%v", snapshot, err)
	}
}

func TestInvalidCropDefinitionsFailBeforeSnapshotConstruction(t *testing.T) {
	base := All()[0]
	tests := []struct {
		name   string
		mutate func(*Definition)
	}{
		{name: "missing_catalog_version", mutate: func(d *Definition) { d.CatalogVersion = 0 }},
		{name: "negative_definition_id", mutate: func(d *Definition) { d.ID = -1 }},
		{name: "rule_version_out_of_participant_range", mutate: func(d *Definition) { d.CropRuleVersion = uint64(math.MaxInt32) + 1 }},
		{name: "base_yield_not_exact", mutate: func(d *Definition) { d.SnapshotBaseYield = 51 }},
		{name: "base_yield_overflow", mutate: func(d *Definition) { d.SnapshotBaseYield = uint64(math.MaxInt32) + 1 }},
		{name: "submillisecond_grow_duration", mutate: func(d *Definition) { d.MatureAfter = time.Second + time.Nanosecond }},
		{name: "water_offset_missing", mutate: func(d *Definition) { d.WaterOffset = 0 }},
		{name: "pest_offset_at_maturity", mutate: func(d *Definition) { d.RemovePestOffset = d.MatureAfter }},
		{name: "coin_value_missing", mutate: func(d *Definition) { d.CropUnitCoinValue = 0 }},
		{name: "activity_id_whitespace", mutate: func(d *Definition) { d.ActivityID = "   " }},
		{name: "activity_not_legacy_seed_plantable", mutate: func(d *Definition) { d.ActivityID = "activity-summer"; d.LegacySeedPlantable = false }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := base
			test.mutate(&definition)
			if snapshot, err := BuildCropRuleSnapshot(definition); err == nil || snapshot != nil {
				t.Fatalf("invalid definition accepted: snapshot=%v err=%v", snapshot, err)
			}
		})
	}
}
