package opportunitywakeup

import (
	"bytes"
	"reflect"
	"testing"

	"farmgame/internal/farm/domain"
)

func TestPayloadFromFactsRoundTrip(t *testing.T) {
	facts := domain.FarmOpportunityFactsV1{SchemaVersion: domain.FarmOpportunityFactsSchemaV1, SourceFarmVersion: 12,
		Cycles: []domain.FarmOpportunityCycleFactV1{{PlotID: 1, CycleID: "cycle", MatureAtMs: 5000, RemainingTheftSlots: 2,
			Water: domain.OpportunityIncidentFactV1{IncidentID: "water", DueAtMs: 3000}}}}
	payload, found, err := FromFacts(77, facts, 2000)
	if err != nil || !found || payload.DueAtMs != 3000 || payload.BoundaryMask != domain.OpportunityBoundaryWater {
		t.Fatalf("payload=%+v found=%v err=%v", payload, found, err)
	}
	encoded, err := Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Unmarshal(encoded)
	if err != nil || !reflect.DeepEqual(payload, decoded) {
		t.Fatalf("round trip decoded=%+v err=%v", decoded, err)
	}
	again, _, err := FromFacts(77, facts, 2000)
	if err != nil {
		t.Fatal(err)
	}
	encodedAgain, _ := Marshal(again)
	if !bytes.Equal(encoded, encodedAgain) {
		t.Fatal("same facts produced a different schedule payload")
	}
}

func TestPayloadFromFactsExhausted(t *testing.T) {
	facts := domain.FarmOpportunityFactsV1{SchemaVersion: domain.FarmOpportunityFactsSchemaV1, SourceFarmVersion: 2,
		Cycles: []domain.FarmOpportunityCycleFactV1{{PlotID: 1, CycleID: "done", MatureAtMs: 1000}}}
	if _, found, err := FromFacts(7, facts, 2000); err != nil || found {
		t.Fatalf("expected no future payload: found=%v err=%v", found, err)
	}
}
