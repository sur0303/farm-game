package task

import (
	"reflect"
	"testing"

	playerpb "farmgame/api/gen/player"
	taskpb "farmgame/api/gen/task"
)

func TestDomainStateMapperRoundTripUsesBoundedModelTaskState(t *testing.T) {
	state := DomainState{
		DailyPeriodID:      "20260805",
		DailyConfigVersion: 91,
		GrowthCursor:       2,
		GrowthChainVersion: 7,
		GrowthActive:       true,
		TotalHarvestCount:  12,
		HighestLevel:       4,
		TaskStateVersion:   9,
		ClaimableCount:     2,
		DailyTasks: [DailySlotCount]Instance{
			{InstanceID: "daily/20260805/login.v1", TaskID: "login.v1", Category: CategoryDaily, Kind: DailyLogin, DefinitionVersion: 1, PeriodID: "20260805", ConfigVersion: 91, Target: 1, Progress: 1, State: StateClaimable, Reward: RewardBundle{Coin: 1}, RewardHash: "r1", FirstWitnessID: "turn-1"},
			{InstanceID: "daily/20260805/visit.v1", TaskID: "visit.v1", Category: CategoryDaily, Kind: DailyFriendVisit, DefinitionVersion: 1, PeriodID: "20260805", ConfigVersion: 91, Target: 1, State: StateInProgress, Reward: RewardBundle{Items: []ItemGrant{{ItemID: 10, Count: 2}}}, RewardHash: "r2"},
			{InstanceID: "daily/20260805/interact.v1", TaskID: "interact.v1", Category: CategoryDaily, Kind: DailyFriendInteract, DefinitionVersion: 1, PeriodID: "20260805", ConfigVersion: 91, Target: 1, Progress: 1, State: StateClaimable, Reward: RewardBundle{Exp: 3, PetUnlock: &PetUnlockGrant{PetID: 8, PetConfigVersion: 5, GrantDefinitionVersion: 6}}, RewardHash: "r3", FirstWitnessID: "order-1"},
		},
		CurrentGrowth: &Instance{
			InstanceID: "growth/harvest.v1/3", TaskID: "harvest.v1", Category: CategoryGrowth, Metric: GrowthMetricTotalHarvest,
			DefinitionVersion: 3, ConfigVersion: 7, Target: 20, Progress: 12, State: StateInProgress,
			Reward: RewardBundle{Coin: 4, Exp: 2}, RewardHash: "growth-r",
		},
	}
	record := &playerpb.PlayerRecord{
		Tasks: []*taskpb.TaskInstance{{TaskDefinitionId: 999, Progress: 1, Target: 1, State: taskpb.TaskState_CLAIMABLE}},
	}
	if err := ApplyDomainStateToRecord(record, &state); err != nil {
		t.Fatal(err)
	}
	if len(record.GetTasks()) != 1 || record.GetTasks()[0].GetTaskDefinitionId() != 999 {
		t.Fatalf("bounded mapper touched legacy tasks: %+v", record.GetTasks())
	}
	if got := record.GetTaskDomainState(); got == nil || len(got.GetDailyTasks()) != DailySlotCount || got.GetGrowthChainVersion() != 7 {
		t.Fatalf("model:task state = %+v", got)
	}
	decoded, err := DomainStateFromRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, state) {
		t.Fatalf("round trip mismatch:\n got  %#v\n want %#v", decoded, state)
	}
}

func TestDomainStateMapperDoesNotFallbackToLegacyTasks(t *testing.T) {
	record := &playerpb.PlayerRecord{Tasks: []*taskpb.TaskInstance{{TaskDefinitionId: 3, Progress: 1, Target: 1, State: taskpb.TaskState_CLAIMABLE}}}
	state, err := DomainStateFromRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if state.DailyPeriodID != "" || state.TaskStateVersion != 0 || state.DailyTasks[0].InstanceID != "" || state.DailyTasks[1].InstanceID != "" || state.DailyTasks[2].InstanceID != "" {
		t.Fatalf("legacy TaskInstance leaked into TaskDomain state: %+v", state)
	}
}

func TestDomainStateMapperRejectsUnboundedDailyShape(t *testing.T) {
	_, err := DomainStateFromModel(&taskpb.TaskDomainState{DailyTasks: []*taskpb.TaskDomainInstance{{}}})
	if err == nil {
		t.Fatal("expected malformed model:task state to fail closed")
	}
}

func TestPublicSummaryProjectsOnlyTaskDomainListView(t *testing.T) {
	view := ListView{
		Summary: Summary{DailyPeriodID: "20260808", TaskStateVersion: 12, ClaimableCount: 2},
		DailyTasks: [DailySlotCount]InstanceView{
			{InstanceID: "daily/20260808/login", TaskID: "daily.login", Progress: 1, Target: 1, State: StateClaimable},
			{InstanceID: "daily/20260808/visit", TaskID: "daily.visit", Progress: 0, Target: 1, State: StateInProgress},
			{InstanceID: "daily/20260808/interact", TaskID: "daily.interact", Progress: 1, Target: 1, State: StateClaimed},
		},
		CurrentGrowth: &InstanceView{InstanceID: "growth/harvest/1", TaskID: "growth.harvest", Progress: 2, Target: 3, State: StateInProgress},
	}
	got := PublicSummaryFromListView(view)
	if got.GetTaskStateVersion() != 12 || got.GetClaimableCount() != 2 || got.GetDailyPeriodId() != "20260808" || len(got.GetDailyTasks()) != DailySlotCount {
		t.Fatalf("summary=%v", got)
	}
	if got.GetDailyTasks()[0].GetState().String() != "PLAYER_TASK_PROGRESS_CLAIMABLE" || got.GetDailyTasks()[1].GetState().String() != "PLAYER_TASK_PROGRESS_ACTIVE" || got.GetDailyTasks()[2].GetState().String() != "PLAYER_TASK_PROGRESS_CLAIMED" {
		t.Fatalf("daily projection=%v", got.GetDailyTasks())
	}
	if got.GetCurrentGrowth().GetTaskId() != "growth.harvest" || got.GetCurrentGrowth().GetState().String() != "PLAYER_TASK_PROGRESS_ACTIVE" {
		t.Fatalf("growth projection=%v", got.GetCurrentGrowth())
	}
}
