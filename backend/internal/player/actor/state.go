package actor

import (
	"sort"

	economypb "farmgame/api/gen/economy"
	playerpb "farmgame/api/gen/player"
	taskpb "farmgame/api/gen/task"
	"farmgame/internal/cropcatalog"
	"farmgame/internal/taskcatalog"
)

const (
	ItemCarrotSeed  = cropcatalog.CarrotSeedItemID
	ItemCarrot      = cropcatalog.CarrotItemID
	OfferCarrotSeed = cropcatalog.CarrotOfferID
)

func ItemCount(rec *playerpb.PlayerRecord, itemID int32) int64 {
	if rec == nil || rec.Inventory == nil {
		return 0
	}
	for _, stack := range rec.Inventory.Stacks {
		if stack != nil && stack.ItemId == itemID {
			return stack.Count
		}
	}
	return 0
}

func AddItem(rec *playerpb.PlayerRecord, itemID int32, delta int64) bool {
	normalize(rec)
	for i, stack := range rec.Inventory.Stacks {
		if stack == nil || stack.ItemId != itemID {
			continue
		}
		next := stack.Count + delta
		if next < 0 {
			return false
		}
		if next == 0 {
			rec.Inventory.Stacks = append(rec.Inventory.Stacks[:i], rec.Inventory.Stacks[i+1:]...)
		} else {
			stack.Count = next
		}
		sortStacks(rec)
		return true
	}
	if delta < 0 {
		return false
	}
	if delta > 0 {
		rec.Inventory.Stacks = append(rec.Inventory.Stacks, &economypb.ItemStack{ItemId: itemID, Count: delta})
		sortStacks(rec)
	}
	return true
}

func PrivateState(rec *playerpb.PlayerRecord) *playerpb.PrivateState {
	normalize(rec)
	state := &playerpb.PrivateState{
		EconomyVersion: rec.EconomyVersion,
		Coin:           rec.Wallet.Coin,
		Level:          rec.Progression.Level,
		Exp:            rec.Progression.Exp,
	}
	for _, stack := range rec.Inventory.Stacks {
		if stack != nil {
			state.Inventory = append(state.Inventory, &economypb.ItemStack{ItemId: stack.ItemId, Count: stack.Count})
		}
	}
	for _, entry := range rec.CodexEntries {
		if entry != nil {
			state.CodexEntries = append(state.CodexEntries, &playerpb.CodexEntry{
				CropDefinitionId:   entry.CropDefinitionId,
				FirstHarvestedAtMs: entry.FirstHarvestedAtMs,
				SourceOperationId:  entry.SourceOperationId,
			})
		}
	}
	for _, instance := range rec.Tasks {
		if instance != nil {
			state.Tasks = append(state.Tasks, &taskpb.TaskInstance{
				TaskDefinitionId: instance.TaskDefinitionId,
				Progress:         instance.Progress,
				Target:           instance.Target,
				State:            instance.State,
				UpdatedAtMs:      instance.UpdatedAtMs,
			})
		}
	}
	sort.Slice(state.Inventory, func(i, j int) bool { return state.Inventory[i].ItemId < state.Inventory[j].ItemId })
	sort.Slice(state.CodexEntries, func(i, j int) bool {
		return state.CodexEntries[i].CropDefinitionId < state.CodexEntries[j].CropDefinitionId
	})
	sort.Slice(state.Tasks, func(i, j int) bool {
		return state.Tasks[i].TaskDefinitionId < state.Tasks[j].TaskDefinitionId
	})
	return state
}

// EnsureTasks makes configuration additions visible to old player records
// without resetting any existing progress. New definitions therefore behave
// like a forward-compatible schema migration inside the PlayerActor turn.
func EnsureTasks(rec *playerpb.PlayerRecord, nowMs int64) {
	for _, definition := range taskcatalog.All() {
		found := false
		for _, instance := range rec.Tasks {
			if instance != nil && instance.TaskDefinitionId == definition.ID {
				found = true
				break
			}
		}
		if !found {
			rec.Tasks = append(rec.Tasks, &taskpb.TaskInstance{
				TaskDefinitionId: definition.ID,
				Target:           definition.Target,
				State:            taskpb.TaskState_ACTIVE,
				UpdatedAtMs:      nowMs,
			})
		}
	}
	sort.Slice(rec.Tasks, func(i, j int) bool {
		return rec.Tasks[i] != nil && (rec.Tasks[j] == nil || rec.Tasks[i].TaskDefinitionId < rec.Tasks[j].TaskDefinitionId)
	})
}

// AdvanceTask records an established fact in the same PlayerActor turn as the fact's
// asset mutation. It is intentionally event-specific instead of accepting a
// client-supplied progress number.
func AdvanceTask(rec *playerpb.PlayerRecord, event taskcatalog.Event, amount int64, nowMs int64) {
	if amount <= 0 {
		return
	}
	EnsureTasks(rec, nowMs)
	for _, instance := range rec.Tasks {
		if instance == nil || instance.State != taskpb.TaskState_ACTIVE {
			continue
		}
		definition, ok := taskcatalog.ByID(instance.TaskDefinitionId)
		if !ok || definition.Event != event {
			continue
		}
		instance.Progress += amount
		if instance.Progress >= instance.Target {
			instance.Progress = instance.Target
			instance.State = taskpb.TaskState_CLAIMABLE
		}
		instance.UpdatedAtMs = nowMs
	}
}

// UnlockCodex records the first successful harvest of a crop. PlayerActor is
// the authority for this fact; callers may retry safely because the crop id is
// unique inside the record.
func UnlockCodex(rec *playerpb.PlayerRecord, cropDefinitionID int32, operationID string, nowMs int64) bool {
	if cropDefinitionID <= 0 {
		return false
	}
	for _, entry := range rec.CodexEntries {
		if entry != nil && entry.CropDefinitionId == cropDefinitionID {
			return false
		}
	}
	rec.CodexEntries = append(rec.CodexEntries, &playerpb.CodexEntry{
		CropDefinitionId:   cropDefinitionID,
		FirstHarvestedAtMs: nowMs,
		SourceOperationId:  operationID,
	})
	sort.Slice(rec.CodexEntries, func(i, j int) bool {
		return rec.CodexEntries[i].CropDefinitionId < rec.CodexEntries[j].CropDefinitionId
	})
	return true
}

func sortStacks(rec *playerpb.PlayerRecord) {
	sort.Slice(rec.Inventory.Stacks, func(i, j int) bool {
		if rec.Inventory.Stacks[i] == nil {
			return false
		}
		if rec.Inventory.Stacks[j] == nil {
			return true
		}
		return rec.Inventory.Stacks[i].ItemId < rec.Inventory.Stacks[j].ItemId
	})
}
