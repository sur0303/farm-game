package domain

import "testing"

func TestFarmOpportunityRevisionAdvancesAcrossTimeBoundariesAndFarmVersions(t *testing.T) {
	facts := FarmOpportunityFactsV1{
		SchemaVersion: FarmOpportunityFactsSchemaV1, SourceFarmVersion: 7,
		Cycles: []FarmOpportunityCycleFactV1{{
			PlotID: 1, CycleID: "cycle-1", MatureAtMs: 300,
			RemainingTheftSlots: 2,
			Water:               OpportunityIncidentFactV1{IncidentID: "water", DueAtMs: 100},
			Pest:                OpportunityIncidentFactV1{IncidentID: "pest", DueAtMs: 200},
		}},
	}
	base, err := FarmOpportunityRevisionForSource(7)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		now  int64
		want uint64
	}{{99, base}, {100, base + 1}, {200, base + 2}, {300, base + 3}} {
		got, err := FarmOpportunityRevisionAt(facts, test.now)
		if err != nil || got != test.want {
			t.Fatalf("now=%d revision=%d want=%d err=%v", test.now, got, test.want, err)
		}
	}
	nextBase, err := FarmOpportunityRevisionForSource(8)
	if err != nil || nextBase <= base+3 {
		t.Fatalf("next FarmVersion revision=%d old=%d err=%v", nextBase, base+3, err)
	}
}

func TestFarmOpportunityRevisionCoalescesCoincidentBoundaries(t *testing.T) {
	facts := FarmOpportunityFactsV1{
		SchemaVersion: FarmOpportunityFactsSchemaV1, SourceFarmVersion: 2,
		Cycles: []FarmOpportunityCycleFactV1{{
			PlotID: 1, CycleID: "cycle-1", MatureAtMs: 200,
			RemainingTheftSlots: 1,
			Pest:                OpportunityIncidentFactV1{IncidentID: "pest", DueAtMs: 200},
		}},
	}
	base, _ := FarmOpportunityRevisionForSource(2)
	got, err := FarmOpportunityRevisionAt(facts, 200)
	if err != nil || got != base+1 {
		t.Fatalf("coincident revision=%d want=%d err=%v", got, base+1, err)
	}
}
