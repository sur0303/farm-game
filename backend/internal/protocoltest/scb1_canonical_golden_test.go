package protocoltest

import (
	"encoding/hex"
	"testing"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
)

// Golden vectors for the canonical step payload/result hash rules of
// contract 29 §7.11. The contract freezes the encoding rules; these values
// pin the Go implementation against any encoder drift (field order, width,
// presence bit, LP framing). Future Lua fixtures and cross-language tools
// must reproduce the same values.

// mustHex decodes frozen hex literals; it panics only on programmer error
// in the literals themselves.
func mustGoldenHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func goldenReserveSeedPayload() *contractspb.ReserveSeedPayload {
	return &contractspb.ReserveSeedPayload{
		PlayerId:             10001,
		FarmId:               20002,
		PlotId:               3,
		ReservationId:        "sid1_2xmYbjcijkoeMWC2ArJClmGQ8-8pzLJbaCQhD3JwYvo",
		CropDefinitionId:     7,
		SeedItemId:           701,
		Quantity:             1,
		CropRuleVersion:      42,
		CropRuleSnapshotHash: mustGoldenHex("00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff"),
	}
}

func TestCanonicalStepPayloadHashGolden(t *testing.T) {
	h, err := contracts.CanonicalHash(goldenReserveSeedPayload())
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	const want = "089fab093258d0da7965ff672d613a7779e13d165b3830901c2177793fbb9117"
	if got := hex.EncodeToString(h[:]); got != want {
		t.Fatalf("reserve seed step hash = %s, want %s", got, want)
	}
}

func TestCanonicalStepPayloadPreimageGolden(t *testing.T) {
	// The preimage pins the exact framing: fixed-width big-endian integers
	// in declared field order, LP strings/bytes, no presence bits for
	// proto3 non-optional scalars.
	pre, err := contracts.CanonicalBytes(goldenReserveSeedPayload())
	if err != nil {
		t.Fatalf("preimage: %v", err)
	}
	const want = "0000000000002711" + // player_id=10001
		"0000000000004e22" + // farm_id=20002
		"00000003" + // plot_id=3
		"00000030" + "736964315f32786d59626a63696a6b6f654d57433241724a436c6d4751382d38707a4c4a626143516844334a7759766f" + // LP reservation_id
		"00000007" + // crop_definition_id=7
		"000002bd" + // seed_item_id=701
		"00000001" + // quantity=1
		"000000000000002a" + // crop_rule_version=42
		"00000020" + "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff" // LP snapshot hash
	if got := hex.EncodeToString(pre); got != want {
		t.Fatalf("preimage = %s, want %s", got, want)
	}
}

func TestCanonicalStepResultHashGolden(t *testing.T) {
	result := &contractspb.ReserveSeedResult{
		ReservationId:              "sid1_2xmYbjcijkoeMWC2ArJClmGQ8-8pzLJbaCQhD3JwYvo",
		SeedItemId:                 701,
		ReservedQuantity:           1,
		AvailableSeedQuantityAfter: 9,
		PlayerRecordVersion:        7,
	}
	h, err := contracts.CanonicalHash(result)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	const want = "b2469a1e82fca51b3cf4e4f6e5fdeee7e1d9dd601ffe51a1dd8251c3aa20ed18"
	if got := hex.EncodeToString(h[:]); got != want {
		t.Fatalf("reserve seed result hash = %s, want %s", got, want)
	}
}

func TestCanonicalNestedSnapshotHashGolden(t *testing.T) {
	fixedOffset := int64(30000)
	payload := &contractspb.CommitPlantPayload{
		OwnerPlayerId:         10001,
		FarmId:                20002,
		PlotId:                3,
		ExpectedPlotVersion:   5,
		ReservationId:         "sid1_2xmYbjcijkoeMWC2ArJClmGQ8-8pzLJbaCQhD3JwYvo",
		ReserveSeedResultHash: mustGoldenHex("b2469a1e82fca51b3cf4e4f6e5fdeee7e1d9dd601ffe51a1dd8251c3aa20ed18"),
		CropCycleId:           "sid1_KDm_gSYlKXdcw1DATIGdnP1IJHPjHyK1VQUU11max00",
		CropRuleSnapshot: &contractspb.CropRuleSnapshot{
			SchemaVersion:     1,
			CropDefinitionId:  7,
			CropRuleVersion:   42,
			SeedItemId:        701,
			HarvestItemId:     702,
			GrowDurationMs:    60000,
			BaseYield:         100,
			HarvestExp:        10,
			CropUnitCoinValue: 2,
			CompendiumRule: &contractspb.CompendiumUnlockRuleSnapshot{
				CropDefinitionId:         7,
				UnlockEligible:           true,
				CatalogVersionAtPlant:    3,
				DefinitionVersionAtPlant: 4,
			},
			StealPlanTemplate: &contractspb.StealPlanTemplate{
				PolicyVersion:   1,
				TotalCapRatioBp: 2000,
				SlotRatioBp:     []uint32{200, 200, 200, 200, 200, 200, 200, 200, 200, 200},
			},
			CarePlanTemplate: &contractspb.CarePlanTemplate{
				CareRuleVersion: 1,
				Incidents: []*contractspb.CarePlanIncidentTemplate{
					{
						CareType:      contractspb.CareType_CARE_TYPE_WATER,
						Ordinal:       1,
						ScheduleMode:  contractspb.CareScheduleMode_CARE_SCHEDULE_MODE_FIXED_OFFSET,
						FixedOffsetMs: &fixedOffset,
					},
				},
			},
		},
	}
	h, err := contracts.CanonicalHash(payload)
	if err != nil {
		t.Fatalf("hash: %v", err)
	}
	const want = "fef77e5ff5fc4e324b92f8538394241eec7d1fba0773302dadab3b0b51191eb9"
	if got := hex.EncodeToString(h[:]); got != want {
		t.Fatalf("commit plant step hash = %s, want %s", got, want)
	}
}
