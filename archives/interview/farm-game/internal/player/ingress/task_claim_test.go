package ingress

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	commonpb "farmgame/api/gen/common"
	playerpb "farmgame/api/gen/player"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/player/pet"
	"farmgame/internal/player/task"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/protobuf/proto"
)

type taskPetClaimRepository struct {
	daily  task.Config
	growth task.Repository
}

func newTaskPetClaimRepository(grant task.PetUnlockGrant) task.Repository {
	return &taskPetClaimRepository{
		daily: task.Config{
			Version:     77,
			Timezone:    task.DefaultGameTimezone,
			PayloadHash: "task-pet-claim-config-v1",
			Daily: [task.DailySlotCount]task.DailyDefinition{
				{
					Kind:   task.DailyLogin,
					TaskID: "daily.login.pet_claim.v1",
					Reward: task.RewardBundle{Exp: 1, PetUnlock: &grant},
				},
				{
					Kind:   task.DailyFriendVisit,
					TaskID: "daily.friend_visit.pet_claim.v1",
					Reward: task.RewardBundle{Exp: 1},
				},
				{
					Kind:   task.DailyFriendInteract,
					TaskID: "daily.friend_interact.pet_claim.v1",
					Reward: task.RewardBundle{Exp: 1},
				},
			},
		},
		growth: task.MustNewP0Repository(),
	}
}

func (r *taskPetClaimRepository) AssignmentForPeriod(periodID string) (task.Assignment, bool) {
	if r == nil || periodID == "" {
		return task.Assignment{}, false
	}
	return task.Assignment{
		PeriodID:          periodID,
		ConfigVersion:     r.daily.Version,
		ConfigPayloadHash: r.daily.PayloadHash,
	}, true
}

func (r *taskPetClaimRepository) DailySnapshot(version uint64) (*task.Config, bool) {
	if r == nil || version != r.daily.Version {
		return nil, false
	}
	config := r.daily
	for index := range config.Daily {
		config.Daily[index].Reward.Items = append([]task.ItemGrant(nil), r.daily.Daily[index].Reward.Items...)
		if grant := r.daily.Daily[index].Reward.PetUnlock; grant != nil {
			copyGrant := *grant
			config.Daily[index].Reward.PetUnlock = &copyGrant
		}
	}
	return &config, true
}

func (r *taskPetClaimRepository) LatestGrowthChain() (*task.GrowthChain, bool) {
	if r == nil || r.growth == nil {
		return nil, false
	}
	return r.growth.LatestGrowthChain()
}

func (r *taskPetClaimRepository) Validate() error {
	if r == nil {
		return task.ErrInvalidArgument
	}
	if err := r.daily.Validate(task.DefaultGameTimezone); err != nil {
		return err
	}
	if r.growth == nil {
		return task.ErrGrowthChainUnavailable
	}
	chain, ok := r.growth.LatestGrowthChain()
	if !ok || chain == nil {
		return task.ErrGrowthChainUnavailable
	}
	return chain.Validate()
}

func taskClaimDailyLoginInstance(t *testing.T, service *Service) string {
	t.Helper()
	outcome := service.executeForTest(t, context.Background(),
		playerCommandEnvelope(t, "task-pet-list", commonpb.CommandType_GET_TASK_LIST, &publicpb.GetTaskListRequest{}))
	if outcome.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("task list=%v", outcome)
	}
	var result publicpb.GetTaskListResult
	if err := proto.Unmarshal(outcome.GetResultPayload(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.GetDailyTasks()) != task.DailySlotCount {
		t.Fatalf("daily task count=%d, want %d", len(result.GetDailyTasks()), task.DailySlotCount)
	}
	progress := result.GetDailyTasks()[0].GetProgress()
	if progress.GetState() != publicpb.PlayerTaskProgressState_PLAYER_TASK_PROGRESS_CLAIMABLE ||
		progress.GetInstanceId() == "" {
		t.Fatalf("login task progress=%v", progress)
	}
	return progress.GetInstanceId()
}

func wireTaskPetClaimDomain(t *testing.T, rig *petAuthorityRig, grant task.PetUnlockGrant) {
	t.Helper()
	if err := rig.service.SetBootstrapTaskDomain(
		newTaskPetClaimRepository(grant),
		task.DefaultGameTimezone,
		func() int64 { return petAuthorityNowMs },
	); err != nil {
		t.Fatal(err)
	}
}

func taskClaimCommand(t *testing.T, operationID, instanceID string) *commonpb.ClientCommandEnvelope {
	t.Helper()
	return playerCommandEnvelope(t, operationID, commonpb.CommandType_CLAIM_TASK,
		&publicpb.ClaimTaskRequest{TaskInstanceId: instanceID})
}

func TestTaskClaimPetUnlockDurableAtomicReplay(t *testing.T) {
	rig := newPetAuthorityRig(t)
	wireTaskPetClaimDomain(t, rig, task.PetUnlockGrant{
		PetID:                  pet.P0DogPetID,
		PetConfigVersion:       1,
		GrantDefinitionVersion: 1,
	})
	instanceID := taskClaimDailyLoginInstance(t, rig.service)
	_, beforeVersion := petReadRecord(t, rig)

	command := taskClaimCommand(t, "task-pet-claim-1", instanceID)
	claimed := rig.service.executeForTest(t, context.Background(), command)
	if claimed.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("claim=%v", claimed)
	}
	var result publicpb.ClaimTaskResult
	if err := proto.Unmarshal(claimed.GetResultPayload(), &result); err != nil {
		t.Fatal(err)
	}
	if result.GetGrantedReward().GetPetUnlock().GetPetId() != pet.P0DogPetID ||
		result.GetGrantedReward().GetPetUnlock().GetPetConfigVersion() != 1 ||
		result.GetGrantedReward().GetPetUnlock().GetGrantDefinitionVersion() != 1 {
		t.Fatalf("granted pet reward=%v", result.GetGrantedReward().GetPetUnlock())
	}
	if result.GetPrivateState().GetExp() != 1 {
		t.Fatalf("private state=%v", result.GetPrivateState())
	}
	petRequireDurableCommit(t, rig, beforeVersion, claimed, authoritystore.TerminalSucceeded)

	record, version := petReadRecord(t, rig)
	if version != beforeVersion+1 {
		t.Fatalf("record version=%d, want %d", version, beforeVersion+1)
	}
	if record.GetPetState().GetPetStateVersion() != 1 ||
		!reflect.DeepEqual(record.GetPetState().GetOwnedPetIds(), []int32{pet.P0DogPetID}) {
		t.Fatalf("pet state=%v", record.GetPetState())
	}
	taskState, err := task.DomainStateFromRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if taskState.DailyTasks[0].State != task.StateClaimed {
		t.Fatalf("task state=%v, want CLAIMED", taskState.DailyTasks[0].State)
	}

	commitCalls := rig.fault.Calls(authoritystore.MethodCommit)
	replay := rig.service.executeForTest(t, context.Background(), command)
	if replay.GetStatus() != commonpb.CommandStatus_SUCCEEDED ||
		!bytes.Equal(replay.GetResultPayload(), claimed.GetResultPayload()) {
		t.Fatalf("replay=%v", replay)
	}
	if rig.fault.Calls(authoritystore.MethodCommit) != commitCalls {
		t.Fatal("same-operation replay issued another authority commit")
	}
	_, replayVersion := petReadRecord(t, rig)
	if replayVersion != version {
		t.Fatalf("replay record version=%d, want %d", replayVersion, version)
	}
}

func TestTaskClaimPetUnlockAlreadyOwnedStillClaims(t *testing.T) {
	rig := newPetAuthorityRig(t)
	wireTaskPetClaimDomain(t, rig, task.PetUnlockGrant{
		PetID:                  pet.P0DogPetID,
		PetConfigVersion:       1,
		GrantDefinitionVersion: 1,
	})
	_, err := rig.registry.Submit(context.Background(), petAuthorityPlayerID,
		func(record *playerpb.PlayerRecord) (any, bool, error) {
			if err := pet.ApplyStateToPlayerRecord(record, pet.State{
				OwnedPetIDs:     []int32{pet.P0DogPetID},
				PetStateVersion: 7,
			}); err != nil {
				return nil, false, err
			}
			return nil, true, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	instanceID := taskClaimDailyLoginInstance(t, rig.service)
	_, beforeVersion := petReadRecord(t, rig)

	claimed := rig.service.executeForTest(t, context.Background(),
		taskClaimCommand(t, "task-pet-owned-1", instanceID))
	if claimed.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("claim with owned pet=%v", claimed)
	}
	petRequireDurableCommit(t, rig, beforeVersion, claimed, authoritystore.TerminalSucceeded)

	record, _ := petReadRecord(t, rig)
	if record.GetPetState().GetPetStateVersion() != 7 ||
		!reflect.DeepEqual(record.GetPetState().GetOwnedPetIds(), []int32{pet.P0DogPetID}) {
		t.Fatalf("already-owned pet state changed=%v", record.GetPetState())
	}
	taskState, err := task.DomainStateFromRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if taskState.DailyTasks[0].State != task.StateClaimed {
		t.Fatalf("task state=%v, want CLAIMED", taskState.DailyTasks[0].State)
	}
}

func TestTaskClaimPetUnlockInvalidConfigRejectedAtDomainConstruction(t *testing.T) {
	repo := newTaskPetClaimRepository(task.PetUnlockGrant{PetID: pet.P0DogPetID, PetConfigVersion: 99, GrantDefinitionVersion: 1})
	if _, err := task.NewDomain(repo, task.DefaultGameTimezone); err == nil {
		t.Fatal("invalid pet config must be rejected during TaskDomain construction")
	}
}
