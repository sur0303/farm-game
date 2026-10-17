package capacity

import (
	"fmt"
	"testing"

	commonpb "farmgame/api/gen/common"
	economypb "farmgame/api/gen/economy"
	farmpb "farmgame/api/gen/farm"
	playerpb "farmgame/api/gen/player"

	"google.golang.org/protobuf/proto"
)

const modelNowMs = int64(1_800_000_000_000)

func playerRecord(receipts, reservations int) *playerpb.PlayerRecord {
	inventory := &economypb.Inventory{}
	privateState := &playerpb.PrivateState{
		EconomyVersion: 1000, Coin: 999999, Level: 30, Exp: 123456,
	}
	for i := 0; i < 20; i++ {
		stack := &economypb.ItemStack{ItemId: int32(2000 + i), Count: int64(100 + i)}
		inventory.Stacks = append(inventory.Stacks, stack)
		privateState.Inventory = append(privateState.Inventory, proto.Clone(stack).(*economypb.ItemStack))
	}
	result, _ := proto.MarshalOptions{Deterministic: true}.Marshal(&playerpb.EconomyCommandResult{
		PrivateState: privateState, CoinDelta: 15, ItemId: 2001, ItemDelta: -1,
	})
	rec := &playerpb.PlayerRecord{
		PlayerId: 100000001, EconomyVersion: 1000, BootstrapId: "capacity-model",
		Wallet: &economypb.Wallet{Coin: 999999}, Inventory: inventory,
		Progression: &economypb.Progression{Level: 30, Exp: 123456},
	}
	for i := 0; i < receipts; i++ {
		rec.RecentOperationReceipts = append(rec.RecentOperationReceipts, &playerpb.PlayerOperationReceipt{
			OperationId: fmt.Sprintf("player-operation-%08d", i),
			Status:      commonpb.CommandStatus_SUCCEEDED, ErrorCode: commonpb.ErrorCode_OK,
			ResultPayload: result, CommittedAtMs: modelNowMs + int64(i),
			PayloadHash: make([]byte, 32),
		})
	}
	for i := 0; i < reservations; i++ {
		rec.ItemReservations = append(rec.ItemReservations, &playerpb.ItemReservation{
			ReservationId: fmt.Sprintf("plant-reservation-%08d", i),
			ItemId:        1001, Count: 1, State: playerpb.ReservationState_COMMITTED,
			UpdatedAtMs: modelNowMs + int64(i), PayloadHash: make([]byte, 32),
		})
	}
	return rec
}

func farmRecord(activeCrops, receipts, settlements int) *farmpb.FarmRecord {
	core := &farmpb.FarmCore{FarmId: 100000001, FarmVersion: 1000}
	snapshot := &farmpb.FarmSnapshot{
		OwnerId: 100000001, FarmVersion: 1000, SnapshotTimeMs: modelNowMs,
	}
	for i := 0; i < 12; i++ {
		plot := &farmpb.Plot{Index: int32(i), PlotVersion: 100}
		view := &farmpb.PlotView{PlotIndex: uint32(i), PlotVersion: 100}
		if i < activeCrops {
			cycleID := fmt.Sprintf("crop-cycle-%08d", i)
			plot.ActiveCropCycleId = cycleID
			core.CropCycles = append(core.CropCycles, &farmpb.CropCycle{
				CropCycleId: cycleID, PlotIndex: int32(i), CropDefinitionId: 1,
				RuleVersion: 1, PlantAt: modelNowMs, MatureAt: modelNowMs + 10_000,
				BaseYield: 10, StealableLimit: 3, StolenTotal: 1, Watered: true,
			})
			view.CropCycleId = cycleID
			view.CropDefinitionId = 1
			view.CropPhase = farmpb.CropPhase_GROWING
			view.PlantAtMs = modelNowMs
			view.MatureAtMs = modelNowMs + 10_000
			view.HarvestableCount = 9
			view.StolenTotal = 1
			view.Watered = true
		}
		core.Plots = append(core.Plots, plot)
		snapshot.Plots = append(snapshot.Plots, view)
	}
	// Durable operation receipts intentionally exclude the recoverable
	// FarmSnapshot; the live result/push path carries it separately.
	result, _ := proto.MarshalOptions{Deterministic: true}.Marshal(&farmpb.FarmCommandResult{
		SettlementId: "settlement-result",
	})
	rec := &farmpb.FarmRecord{FarmId: 100000001, Core: core}
	for i := 0; i < receipts; i++ {
		rec.RecentOperationReceipts = append(rec.RecentOperationReceipts, &farmpb.FarmOperationReceipt{
			OperationId: fmt.Sprintf("farm-operation-%08d", i),
			Status:      commonpb.CommandStatus_SUCCEEDED, ErrorCode: commonpb.ErrorCode_OK,
			ResultPayload: result, CommittedAtMs: modelNowMs + int64(i),
			PayloadHash: make([]byte, 32),
		})
	}
	for i := 0; i < settlements; i++ {
		rec.ActiveSettlements = append(rec.ActiveSettlements, &farmpb.FarmSettlement{
			SettlementId: fmt.Sprintf("farm-settlement-%08d", i),
			Kind:         farmpb.FarmSettlementKind_THEFT, State: farmpb.FarmSettlementState_COMPLETED,
			BeneficiaryPlayerId: uint64(200000000 + i), ItemId: 2001, ItemCount: 1,
			CropCycleId: fmt.Sprintf("crop-cycle-%08d", i%12),
			UpdatedAtMs: modelNowMs + int64(i), RelationEpoch: 10,
			Participants: []*farmpb.FarmSettlementParticipant{
				{PlayerId: uint64(200000000 + i), Role: farmpb.FarmParticipantRole_VISITOR, ItemId: 2001, ItemCount: 1},
				{PlayerId: 100000001, Role: farmpb.FarmParticipantRole_OWNER, CoinDelta: 1},
			},
		})
	}
	return rec
}

func TestCurrentRecordSizeModel(t *testing.T) {
	scenarios := []struct {
		name   string
		record proto.Message
	}{
		{name: "player/new", record: playerRecord(0, 0)},
		{name: "player/7d-model", record: playerRecord(140, 10)},
		{name: "player/hard-window", record: playerRecord(1024, 1024)},
		{name: "farm/new", record: farmRecord(0, 0, 0)},
		{name: "farm/7d-model", record: farmRecord(6, 140, 10)},
		{name: "farm/hard-window", record: farmRecord(12, 1024, 1024)},
	}
	t.Log("Current split PlayerRecord/FarmRecord model (protobuf payload only):")
	for _, scenario := range scenarios {
		data, err := proto.MarshalOptions{Deterministic: true}.Marshal(scenario.record)
		if err != nil {
			t.Fatalf("marshal %s: %v", scenario.name, err)
		}
		t.Logf("%-24s %8d bytes %8.1f KiB", scenario.name, len(data), float64(len(data))/1024)
		if len(data) > 2*1024*1024 {
			t.Fatalf("%s exceeds 2 MiB guardrail: %d", scenario.name, len(data))
		}
	}
}
