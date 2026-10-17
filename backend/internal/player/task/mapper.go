package task

import (
	"errors"
	"fmt"

	playerpb "farmgame/api/gen/player"
	publicpb "farmgame/api/gen/public"
	taskpb "farmgame/api/gen/task"
)

// ErrInvalidModelState is returned when the bounded model:task carrier is
// malformed. A PlayerActor must discard the candidate and fail closed rather
// than reconstructing task truth from the legacy PlayerRecord.Tasks list.
var ErrInvalidModelState = errors.New("task: invalid model:task state")

// PublicSummaryFromListView projects only the bounded TaskDomain/ListView
// read model. It intentionally has no path to PlayerRecord.Tasks, rewards,
// receipts or any other private state.
func PublicSummaryFromListView(view ListView) *publicpb.PlayerTaskSummaryView {
	result := &publicpb.PlayerTaskSummaryView{
		TaskStateVersion: view.TaskStateVersion,
		ClaimableCount:   view.ClaimableCount,
		DailyPeriodId:    view.DailyPeriodID,
		DailyTasks:       make([]*publicpb.PlayerTaskProgressView, 0, DailySlotCount),
	}
	for i := range view.DailyTasks {
		result.DailyTasks = append(result.DailyTasks, PublicProgress(&view.DailyTasks[i]))
	}
	if view.CurrentGrowth != nil {
		result.CurrentGrowth = PublicProgress(view.CurrentGrowth)
	}
	return result
}

// PublicProgress projects one authority task instance without exposing its
// frozen reward. It is used by summaries and write-result next-task hints.
func PublicProgress(view *InstanceView) *publicpb.PlayerTaskProgressView {
	if view == nil {
		return nil
	}
	state := publicpb.PlayerTaskProgressState_PLAYER_TASK_PROGRESS_STATE_UNSPECIFIED
	switch view.State {
	case StateInProgress:
		state = publicpb.PlayerTaskProgressState_PLAYER_TASK_PROGRESS_ACTIVE
	case StateClaimable:
		state = publicpb.PlayerTaskProgressState_PLAYER_TASK_PROGRESS_CLAIMABLE
	case StateClaimed:
		state = publicpb.PlayerTaskProgressState_PLAYER_TASK_PROGRESS_CLAIMED
	}
	return &publicpb.PlayerTaskProgressView{
		InstanceId: view.InstanceID,
		TaskId:     view.TaskID,
		Progress:   view.Progress,
		Target:     view.Target,
		State:      state,
	}
}

// PublicReward projects the server-frozen display reward. Clients never echo
// this value into ClaimTask.
func PublicReward(reward RewardBundle) *publicpb.PlayerTaskRewardView {
	result := &publicpb.PlayerTaskRewardView{Coin: reward.Coin, Exp: reward.Exp}
	for _, item := range reward.Items {
		result.Items = append(result.Items, &publicpb.PlayerTaskItemRewardView{ItemId: item.ItemID, Count: item.Count})
	}
	if reward.PetUnlock != nil {
		result.PetUnlock = &publicpb.PlayerTaskPetUnlockRewardView{
			PetId: reward.PetUnlock.PetID, PetConfigVersion: reward.PetUnlock.PetConfigVersion,
			GrantDefinitionVersion: reward.PetUnlock.GrantDefinitionVersion,
		}
	}
	return result
}

func publicDetail(view *InstanceView) *publicpb.PlayerTaskDetailView {
	if view == nil {
		return nil
	}
	return &publicpb.PlayerTaskDetailView{
		Progress: PublicProgress(view), Category: int32(view.Category), Kind: int32(view.Kind),
		DefinitionVersion: view.DefinitionVersion, PeriodId: view.PeriodID,
		ConfigVersion: view.ConfigVersion, Reward: PublicReward(view.Reward),
	}
}

// PublicListResult is the complete fixed-size TaskReadAPI payload.
func PublicListResult(view ListView) *publicpb.GetTaskListResult {
	result := &publicpb.GetTaskListResult{
		Summary: PublicSummaryFromListView(view), NextResetAtMs: view.NextResetAtMs,
		DailyConfigVersion: view.DailyConfigVer,
		DailyTasks:         make([]*publicpb.PlayerTaskDetailView, 0, DailySlotCount),
	}
	for i := range view.DailyTasks {
		result.DailyTasks = append(result.DailyTasks, publicDetail(&view.DailyTasks[i]))
	}
	result.CurrentGrowth = publicDetail(view.CurrentGrowth)
	return result
}

// DomainStateFromRecord reads only PlayerRecord.TaskDomainState. The old
// repeated TaskInstance field is a bounded compatibility carrier for older
// Plant/Harvest code; it is intentionally not a fallback for TaskDomain.
//
// W31 seam: PlayerRecord is still the Registry's compatibility serialization
// envelope. This adapter is the only Care/TaskDomain path that touches the
// bounded model:task field, so the later multi-Model physical split can move
// it to its own ModelMutation without changing TaskDomain semantics.
func DomainStateFromRecord(record *playerpb.PlayerRecord) (DomainState, error) {
	if record == nil {
		return DomainState{}, fmt.Errorf("%w: nil PlayerRecord", ErrInvalidModelState)
	}
	return DomainStateFromModel(record.GetTaskDomainState())
}

// ApplyDomainStateToRecord writes the complete bounded model:task state. It
// never appends or updates PlayerRecord.Tasks, which prevents a second task
// state machine from being maintained by Care.
func ApplyDomainStateToRecord(record *playerpb.PlayerRecord, state *DomainState) error {
	if record == nil || state == nil {
		return fmt.Errorf("%w: record and state are required", ErrInvalidModelState)
	}
	model, err := DomainStateToModel(state)
	if err != nil {
		return err
	}
	record.TaskDomainState = model
	return nil
}

// DomainStateFromModel decodes the complete bounded physical model. A
// non-nil model must carry exactly the frozen three daily slots; silently
// padding or truncating would turn a storage corruption into a different task
// truth.
func DomainStateFromModel(model *taskpb.TaskDomainState) (DomainState, error) {
	if model == nil {
		return DomainState{}, nil
	}
	if len(model.GetDailyTasks()) != DailySlotCount {
		return DomainState{}, fmt.Errorf("%w: daily_tasks length=%d want=%d", ErrInvalidModelState, len(model.GetDailyTasks()), DailySlotCount)
	}
	state := DomainState{
		DailyPeriodID:      model.GetDailyPeriodId(),
		DailyConfigVersion: model.GetDailyConfigVersion(),
		GrowthCursor:       int(model.GetGrowthCursor()),
		GrowthChainVersion: model.GetGrowthChainVersion(),
		TotalHarvestCount:  model.GetTotalHarvestCount(),
		HighestLevel:       model.GetHighestLevel(),
		TaskStateVersion:   model.GetTaskStateVersion(),
		ClaimableCount:     model.GetClaimableCount(),
	}
	for i, instance := range model.GetDailyTasks() {
		decoded, err := instanceFromModel(instance)
		if err != nil {
			return DomainState{}, fmt.Errorf("%w: daily task %d: %v", ErrInvalidModelState, i, err)
		}
		state.DailyTasks[i] = decoded
	}
	if model.GetCurrentGrowth() != nil {
		decoded, err := instanceFromModel(model.GetCurrentGrowth())
		if err != nil {
			return DomainState{}, fmt.Errorf("%w: current growth: %v", ErrInvalidModelState, err)
		}
		state.CurrentGrowth = &decoded
		state.GrowthActive = true
	}
	return state, nil
}

// DomainStateToModel encodes the full task-domain truth into the bounded
// model:task protobuf. No operation id, payload hash, source receipt or claim
// history is encoded here; those remain in the PlayerActor receipt fields.
func DomainStateToModel(state *DomainState) (*taskpb.TaskDomainState, error) {
	if state == nil {
		return nil, fmt.Errorf("%w: nil domain state", ErrInvalidModelState)
	}
	if state.GrowthCursor < 0 || uint64(state.GrowthCursor) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("%w: growth_cursor out of uint32 range", ErrInvalidModelState)
	}
	model := &taskpb.TaskDomainState{
		DailyPeriodId:      state.DailyPeriodID,
		DailyConfigVersion: state.DailyConfigVersion,
		GrowthCursor:       uint32(state.GrowthCursor),
		GrowthChainVersion: state.GrowthChainVersion,
		TotalHarvestCount:  state.TotalHarvestCount,
		HighestLevel:       state.HighestLevel,
		TaskStateVersion:   state.TaskStateVersion,
		ClaimableCount:     state.ClaimableCount,
		DailyTasks:         make([]*taskpb.TaskDomainInstance, DailySlotCount),
	}
	for i := range state.DailyTasks {
		encoded, err := instanceToModel(&state.DailyTasks[i])
		if err != nil {
			return nil, fmt.Errorf("%w: daily task %d: %v", ErrInvalidModelState, i, err)
		}
		model.DailyTasks[i] = encoded
	}
	if state.CurrentGrowth != nil {
		encoded, err := instanceToModel(state.CurrentGrowth)
		if err != nil {
			return nil, fmt.Errorf("%w: current growth: %v", ErrInvalidModelState, err)
		}
		model.CurrentGrowth = encoded
	}
	return model, nil
}

func instanceFromModel(model *taskpb.TaskDomainInstance) (Instance, error) {
	if model == nil {
		return Instance{}, errors.New("nil task instance")
	}
	return Instance{
		InstanceID:        model.GetInstanceId(),
		TaskID:            model.GetTaskId(),
		Category:          Category(model.GetCategory()),
		Kind:              DailyKind(model.GetKind()),
		Metric:            GrowthMetric(model.GetMetric()),
		DefinitionVersion: model.GetDefinitionVersion(),
		PeriodID:          model.GetPeriodId(),
		ConfigVersion:     model.GetConfigVersion(),
		Target:            model.GetTarget(),
		Progress:          model.GetProgress(),
		State:             State(model.GetState()),
		Reward:            rewardFromModel(model.GetReward()),
		RewardHash:        model.GetRewardHash(),
		FirstWitnessID:    model.GetFirstWitnessId(),
		ClaimedAtMs:       model.GetClaimedAtMs(),
	}, nil
}

func instanceToModel(instance *Instance) (*taskpb.TaskDomainInstance, error) {
	if instance == nil {
		return nil, errors.New("nil task instance")
	}
	return &taskpb.TaskDomainInstance{
		InstanceId:        instance.InstanceID,
		TaskId:            instance.TaskID,
		Category:          int32(instance.Category),
		Kind:              int32(instance.Kind),
		Metric:            int32(instance.Metric),
		DefinitionVersion: instance.DefinitionVersion,
		PeriodId:          instance.PeriodID,
		ConfigVersion:     instance.ConfigVersion,
		Target:            instance.Target,
		Progress:          instance.Progress,
		State:             int32(instance.State),
		Reward:            rewardToModel(instance.Reward),
		RewardHash:        instance.RewardHash,
		FirstWitnessId:    instance.FirstWitnessID,
		ClaimedAtMs:       instance.ClaimedAtMs,
	}, nil
}

func rewardFromModel(model *taskpb.TaskRewardBundle) RewardBundle {
	if model == nil {
		return RewardBundle{}
	}
	reward := RewardBundle{Coin: model.GetCoin(), Exp: model.GetExp()}
	for _, item := range model.GetItems() {
		if item == nil {
			continue
		}
		reward.Items = append(reward.Items, ItemGrant{ItemID: item.GetItemId(), Count: item.GetCount()})
	}
	if grant := model.GetPetUnlock(); grant != nil {
		reward.PetUnlock = &PetUnlockGrant{
			PetID:                  grant.GetPetId(),
			PetConfigVersion:       grant.GetPetConfigVersion(),
			GrantDefinitionVersion: grant.GetGrantDefinitionVersion(),
		}
	}
	return reward
}

func rewardToModel(reward RewardBundle) *taskpb.TaskRewardBundle {
	if reward.isZero() {
		return nil
	}
	model := &taskpb.TaskRewardBundle{Coin: reward.Coin, Exp: reward.Exp}
	for _, item := range reward.Items {
		model.Items = append(model.Items, &taskpb.TaskItemGrant{ItemId: item.ItemID, Count: item.Count})
	}
	if reward.PetUnlock != nil {
		model.PetUnlock = &taskpb.TaskPetUnlockGrant{
			PetId:                  reward.PetUnlock.PetID,
			PetConfigVersion:       reward.PetUnlock.PetConfigVersion,
			GrantDefinitionVersion: reward.PetUnlock.GrantDefinitionVersion,
		}
	}
	return model
}
