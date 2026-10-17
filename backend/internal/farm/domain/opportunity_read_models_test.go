package domain

import (
	"bytes"
	"reflect"
	"testing"
)

func TestViewerOpportunityMaskUsesStableIndependentBits(t *testing.T) {
	if ViewerOpportunityNone != 0 || ViewerCanSteal != 1 || ViewerCanWater != 2 || ViewerCanRemovePest != 4 {
		t.Fatalf("unexpected mask allocation: none=%d steal=%d water=%d pest=%d",
			ViewerOpportunityNone, ViewerCanSteal, ViewerCanWater, ViewerCanRemovePest)
	}
}

func TestFarmOpportunityReadModelsLifecycleAndDeterminism(t *testing.T) {
	state := FarmState{FarmID: 7, OwnerPlayerID: 7, FarmVersion: 11, Cycles: []CropCycle{
		{
			CycleID: "cycle-b", PlotID: 2, MatureAtMs: 3000, BaseYield: 100, StealableLimit: 20,
			StealPlan: StealPlanSnapshot{SlotQuantity: []uint32{2, 2, 2, 2, 2, 2, 2, 2, 2, 2}},
			Attempts:  []TheftAttemptRecord{{VisitorPlayerID: 20}, {VisitorPlayerID: 10}},
			Incidents: []CareIncident{
				{IncidentID: "pest-b", CareType: CareTypeRemovePest, DueAtMs: 2500},
				{IncidentID: "water-b", CareType: CareTypeWater, DueAtMs: 2000},
			},
		},
		{
			CycleID: "cycle-a", PlotID: 1, MatureAtMs: 1500, BaseYield: 100, StealableLimit: 20,
			StealPlan: StealPlanSnapshot{SlotQuantity: []uint32{2, 2, 2, 2, 2, 2, 2, 2, 2, 2}},
			Incidents: []CareIncident{{IncidentID: "water-a", CareType: CareTypeWater, DueAtMs: 1200}},
		},
	}}

	header, facts, err := DeriveFarmOpportunityReadModels(&state)
	if err != nil {
		t.Fatal(err)
	}
	if header.NextStealableAtMs != 1500 || header.NextWaterAtMs != 1200 || header.NextPestAtMs != 2500 || header.NextOpportunityAtMs != 1200 {
		t.Fatalf("unexpected header: %+v", header)
	}
	if len(facts.Cycles) != 2 || facts.Cycles[0].PlotID != 1 || facts.Cycles[1].PlotID != 2 {
		t.Fatalf("facts are not deterministically sorted: %+v", facts.Cycles)
	}
	if got := facts.Cycles[1].TheftVisitorIDs; !reflect.DeepEqual(got, []uint64{10, 20}) {
		t.Fatalf("visitor ids are not sorted/deduplicated: %v", got)
	}
	if got := facts.Cycles[1].RemainingTheftSlots; got != 8 {
		t.Fatalf("remaining theft slots=%d want=8", got)
	}

	// At the exact trusted maturity boundary WATER is already forbidden and the
	// same cycle becomes an exact Theft target.
	atBoundary, err := EvaluateFarmOpportunityFacts(FarmOpportunityFactsV1{
		SchemaVersion: FarmOpportunityFactsSchemaV1,
		Cycles: []FarmOpportunityCycleFactV1{{CycleID: "boundary", PlotID: 4, MatureAtMs: 2500,
			RemainingTheftSlots: 1, Water: OpportunityIncidentFactV1{IncidentID: "water-boundary", DueAtMs: 2000}}},
	}, 30, 2500)
	if err != nil || atBoundary.Mask&ViewerCanWater != 0 || atBoundary.Mask&ViewerCanSteal == 0 ||
		len(atBoundary.EligibleTheftTargets) != 1 || atBoundary.EligibleTheftTargets[0].PlotID != 4 {
		t.Fatalf("exact maturity boundary is wrong: result=%+v err=%v", atBoundary, err)
	}

	// Before maturity, a due WATER incident is actionable; the already-mature
	// cycle never advertises WATER even though its historical incident remains.
	growing, err := EvaluateFarmOpportunityFacts(facts, 30, 2500)
	if err != nil || growing.Mask&ViewerCanWater == 0 || len(growing.EligibleTheftTargets) != 1 || growing.EligibleTheftTargets[0].PlotID != 1 {
		t.Fatalf("growing/mature opportunity boundary is wrong: result=%+v err=%v", growing, err)
	}

	// Resolving the globally unique water incident removes only that care fact.
	state.Cycles[0].Incidents[1].ResolvedAtMs = 2100
	state.Cycles[0].Incidents[1].ResolvedByPlayerID = 99
	header2, facts2, err := DeriveFarmOpportunityReadModels(&state)
	if err != nil {
		t.Fatal(err)
	}
	if header2.NextWaterAtMs != 1200 || facts2.Cycles[1].Water.IncidentID != "" {
		t.Fatalf("resolved water incident was retained: header=%+v fact=%+v", header2, facts2.Cycles[1].Water)
	}

	headerBytes, err := MarshalFarmOpportunityHeaderV2(header2)
	if err != nil {
		t.Fatal(err)
	}
	headerRoundTrip, err := UnmarshalFarmOpportunityHeaderV2(headerBytes)
	if err != nil || !reflect.DeepEqual(header2, headerRoundTrip) {
		t.Fatalf("header round trip failed: got=%+v err=%v", headerRoundTrip, err)
	}
	factsBytes, err := MarshalFarmOpportunityFactsV1(facts2)
	if err != nil {
		t.Fatal(err)
	}
	factsRoundTrip, err := UnmarshalFarmOpportunityFactsV1(factsBytes)
	if err != nil || !reflect.DeepEqual(facts2, factsRoundTrip) {
		t.Fatalf("facts round trip failed: got=%+v err=%v", factsRoundTrip, err)
	}
	factsBytesAgain, err := MarshalFarmOpportunityFactsV1(facts2)
	if err != nil || !bytes.Equal(factsBytes, factsBytesAgain) {
		t.Fatalf("facts encoding is not deterministic: err=%v", err)
	}
	evaluated, err := EvaluateFarmOpportunityFacts(facts2, 10, 4000)
	if err != nil {
		t.Fatal(err)
	}
	if evaluated.Mask&ViewerCanSteal == 0 || evaluated.Mask&ViewerCanWater != 0 || evaluated.Mask&ViewerCanRemovePest == 0 ||
		len(evaluated.EligibleTheftTargets) != 1 || evaluated.EligibleTheftTargets[0].PlotID != 1 {
		t.Fatalf("viewer exact mask/targets are wrong after maturity: %+v", evaluated)
	}
	evaluatedAttempted, err := EvaluateFarmOpportunityFacts(FarmOpportunityFactsV1{
		SchemaVersion: FarmOpportunityFactsSchemaV1,
		Cycles:        []FarmOpportunityCycleFactV1{{CycleID: "only", PlotID: 1, MatureAtMs: 1, RemainingTheftSlots: 9, TheftVisitorIDs: []uint64{10}}},
	}, 10, 4000)
	if err != nil || evaluatedAttempted.Mask&ViewerCanSteal != 0 {
		t.Fatalf("existing theft receipt did not fence viewer: mask=%v err=%v", evaluatedAttempted.Mask, err)
	}
}

func TestFarmOpportunityReadModelsRejectMoreThanTenVisitors(t *testing.T) {
	cycle := CropCycle{CycleID: "cycle", PlotID: 1, MatureAtMs: 1, BaseYield: 100, StealableLimit: 20}
	for id := uint64(1); id <= 11; id++ {
		cycle.Attempts = append(cycle.Attempts, TheftAttemptRecord{VisitorPlayerID: id})
	}
	_, _, err := DeriveFarmOpportunityReadModels(&FarmState{FarmID: 1, OwnerPlayerID: 1, Cycles: []CropCycle{cycle}})
	if err == nil {
		t.Fatal("expected theft visitor bound rejection")
	}
}
