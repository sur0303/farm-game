package actor

import (
	"testing"

	"farmgame/internal/farm/domain"
	"farmgame/internal/farm/opportunitywakeup"
)

func TestCurrentFarmOpportunityScheduleUsesFarmVersionGeneration(t *testing.T) {
	facts := domain.FarmOpportunityFactsV1{
		SchemaVersion:     domain.FarmOpportunityFactsSchemaV1,
		SourceFarmVersion: 17,
		Cycles: []domain.FarmOpportunityCycleFactV1{{
			PlotID: 2, CycleID: "cycle-2", MatureAtMs: 9_000,
			RemainingTheftSlots: 3,
		}},
	}
	schedule, err := currentFarmOpportunitySchedule(41, facts)
	if err != nil {
		t.Fatal(err)
	}
	wantScheduleID, err := opportunitywakeup.ScheduleIDForFarm(41)
	if err != nil {
		t.Fatal(err)
	}
	if schedule.ScheduleType != opportunitywakeup.WorkType || schedule.ScheduleID != wantScheduleID ||
		schedule.Generation != 17 || schedule.DueAtMs != 9_000 || schedule.SubjectPlayerID != 41 {
		t.Fatalf("unexpected schedule: %+v", schedule)
	}
	payload, err := opportunitywakeup.Unmarshal(schedule.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if payload.FarmID != 41 || payload.SourceFarmVersion != 17 {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestCurrentFarmOpportunityScheduleCancelsWhenNoBoundaryRemains(t *testing.T) {
	schedule, err := currentFarmOpportunitySchedule(41, domain.FarmOpportunityFactsV1{
		SchemaVersion: domain.FarmOpportunityFactsSchemaV1, SourceFarmVersion: 18,
	})
	if err != nil {
		t.Fatal(err)
	}
	if schedule.Generation != 18 || schedule.DueAtMs != 0 || len(schedule.Payload) != 0 {
		t.Fatalf("unexpected cancellation: %+v", schedule)
	}
}
