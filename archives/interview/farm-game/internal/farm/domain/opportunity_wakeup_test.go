package domain

import (
	"reflect"
	"testing"
)

func TestOpportunityWakeupWalksDistinctBoundaries(t *testing.T) {
	facts := FarmOpportunityFactsV1{SchemaVersion: FarmOpportunityFactsSchemaV1, SourceFarmVersion: 9, Cycles: []FarmOpportunityCycleFactV1{
		{PlotID: 2, CycleID: "later", MatureAtMs: 3000, RemainingTheftSlots: 1,
			Water: OpportunityIncidentFactV1{IncidentID: "water-later", DueAtMs: 1500},
			Pest:  OpportunityIncidentFactV1{IncidentID: "pest-later", DueAtMs: 4000}},
		{PlotID: 1, CycleID: "first", MatureAtMs: 2000, RemainingTheftSlots: 2,
			Water: OpportunityIncidentFactV1{IncidentID: "water-first", DueAtMs: 1500}},
	}}

	first, ok, err := NextFarmOpportunityBoundary(facts, 1000)
	if err != nil || !ok || first.AtMs != 1500 || first.Mask != OpportunityBoundaryWater {
		t.Fatalf("first boundary=%+v ok=%v err=%v", first, ok, err)
	}
	// The same facts always produce the same identity, independent of cycle order.
	reversed := facts
	reversed.Cycles = append([]FarmOpportunityCycleFactV1(nil), facts.Cycles...)
	reversed.Cycles[0], reversed.Cycles[1] = reversed.Cycles[1], reversed.Cycles[0]
	again, ok, err := NextFarmOpportunityBoundary(reversed, 1000)
	if err != nil || !ok || !reflect.DeepEqual(first, again) {
		t.Fatalf("boundary is not deterministic: first=%+v again=%+v err=%v", first, again, err)
	}

	second, ok, err := NextFarmOpportunityBoundary(facts, first.AtMs)
	if err != nil || !ok || second.AtMs != 2000 || second.Mask != OpportunityBoundaryStealable {
		t.Fatalf("second boundary=%+v ok=%v err=%v", second, ok, err)
	}
	third, ok, err := NextFarmOpportunityBoundary(facts, second.AtMs)
	if err != nil || !ok || third.AtMs != 3000 || third.Mask != OpportunityBoundaryStealable {
		t.Fatalf("third boundary=%+v ok=%v err=%v", third, ok, err)
	}
	fourth, ok, err := NextFarmOpportunityBoundary(facts, third.AtMs)
	if err != nil || !ok || fourth.AtMs != 4000 || fourth.Mask != OpportunityBoundaryPest {
		t.Fatalf("fourth boundary=%+v ok=%v err=%v", fourth, ok, err)
	}
	if _, ok, err := NextFarmOpportunityBoundary(facts, fourth.AtMs); err != nil || ok {
		t.Fatalf("expected exhausted boundary chain: ok=%v err=%v", ok, err)
	}
}

func TestOpportunityWakeupUsesCurrentGlobalFactsWithoutAuthorizingViewer(t *testing.T) {
	facts := FarmOpportunityFactsV1{SchemaVersion: FarmOpportunityFactsSchemaV1, SourceFarmVersion: 4, Cycles: []FarmOpportunityCycleFactV1{
		{PlotID: 1, CycleID: "mature", MatureAtMs: 2000, RemainingTheftSlots: 1, TheftVisitorIDs: []uint64{7}},
		{PlotID: 2, CycleID: "growing", MatureAtMs: 5000,
			Water: OpportunityIncidentFactV1{IncidentID: "water", DueAtMs: 2500},
			Pest:  OpportunityIncidentFactV1{IncidentID: "pest", DueAtMs: 3000}},
	}}
	global, err := EvaluateGlobalFarmOpportunityFacts(facts, 3500)
	if err != nil || global.Mask != OpportunityBoundaryStealable|OpportunityBoundaryWater|OpportunityBoundaryPest || global.ActionableFromMs != 2000 {
		t.Fatalf("global opportunity=%+v err=%v", global, err)
	}
	viewer, err := EvaluateFarmOpportunityFacts(facts, 7, 3500)
	if err != nil || viewer.Mask&ViewerCanSteal != 0 || viewer.Mask&ViewerCanWater == 0 || viewer.Mask&ViewerCanRemovePest == 0 {
		t.Fatalf("viewer exact result=%+v err=%v", viewer, err)
	}
}

func TestOpportunityWakeupDoesNotScheduleWaterAtMaturity(t *testing.T) {
	facts := FarmOpportunityFactsV1{SchemaVersion: FarmOpportunityFactsSchemaV1, SourceFarmVersion: 1, Cycles: []FarmOpportunityCycleFactV1{{
		PlotID: 1, CycleID: "cycle", MatureAtMs: 2000, RemainingTheftSlots: 1,
		Water: OpportunityIncidentFactV1{IncidentID: "water", DueAtMs: 2000},
	}}}
	boundary, ok, err := NextFarmOpportunityBoundary(facts, 1000)
	if err != nil || !ok || boundary.AtMs != 2000 || boundary.Mask != OpportunityBoundaryStealable {
		t.Fatalf("maturity boundary=%+v ok=%v err=%v", boundary, ok, err)
	}
}
