package ownercoresize

import (
	"fmt"
	"testing"

	economypb "farmgame/api/gen/economy"
	farmpb "farmgame/api/gen/farm"
	ownercorepb "farmgame/api/gen/ownercore"
	settlementpb "farmgame/api/gen/settlement"

	commonpb "farmgame/api/gen/common"

	"google.golang.org/protobuf/proto"
)

// buildRecord 构造一个可参数化规模的 OwnerCoreRecord,用于大小预算实测。
//
//	plots:      地块数(空地或有作物)
//	activeCrops: 活跃作物茬数
//	invStacks:  背包道具种类数
//	opResults:  近期幂等结果条数(热窗口)
//	outbox:     待处理 Outbox 条数
//	settlements:活跃偷菜结算条数
//	receipts:   近期 LootReceipt 条数
func buildRecord(plots, activeCrops, invStacks, opResults, outbox, settlements, receipts int) *ownercorepb.OwnerCoreRecord {
	rec := &ownercorepb.OwnerCoreRecord{
		OwnerId: 100000000000001,
		OwnerMeta: &ownercorepb.OwnerMeta{
			OwnerId: 100000000000001, OwnerEpoch: 3, StateVersion: 128374, SchemaVersion: 1,
		},
		PlayerProfile: &economypb.PlayerProfile{
			Nickname: "农场主小明", AvatarId: 12, CreatedAt: 1700000000000, LastLoginAt: 1701234567890,
		},
		Progression: &economypb.Progression{Level: 27, Exp: 183940, Unlocked: []int32{1, 2, 3, 4, 5, 6, 7, 8}},
		Wallet:      &economypb.Wallet{Coin: 9876543},
		Farm:        &farmpb.FarmCore{FarmId: 100000000000001, FarmVersion: 88213},
	}

	inv := &economypb.Inventory{}
	for i := 0; i < invStacks; i++ {
		inv.Stacks = append(inv.Stacks, &economypb.ItemStack{ItemId: int32(1000 + i), Count: int64(50 + i*7)})
	}
	rec.Inventory = inv

	for i := 0; i < plots; i++ {
		p := &farmpb.Plot{Index: int32(i), PlotVersion: uint64(1000 + i)}
		if i < activeCrops {
			ccid := fmt.Sprintf("cc-%013d-%03d", rec.OwnerId, i)
			p.ActiveCropCycleId = ccid
			rec.Farm.CropCycles = append(rec.Farm.CropCycles, &farmpb.CropCycle{
				CropCycleId: ccid, PlotIndex: int32(i), CropDefinitionId: int32(2000 + i%20),
				RuleVersion: 5, PlantAt: 1701200000000, MatureAt: 1701203600000,
				BaseYield: 10, StealableLimit: 3, StolenTotal: int32(i % 4), Watered: i%2 == 0,
			})
		}
		rec.Farm.Plots = append(rec.Farm.Plots, p)
	}

	for i := 0; i < opResults; i++ {
		rec.RecentOperationResults = append(rec.RecentOperationResults, &settlementpb.OperationResult{
			OperationId:   fmt.Sprintf("op-%013d-%06d", rec.OwnerId, i),
			Status:        commonpb.CommandStatus_SUCCEEDED,
			ErrorCode:     commonpb.ErrorCode_OK,
			CommittedAt:   1701234000000 + int64(i),
			ResultPayload: []byte("harvest-result-crop-2003-yield-10-exp-25"),
		})
	}
	for i := 0; i < outbox; i++ {
		rec.PendingOutbox = append(rec.PendingOutbox, &settlementpb.PendingOutbox{
			OutboxId: fmt.Sprintf("ob-%013d-%06d", rec.OwnerId, i), Kind: settlementpb.OutboxKind_GRANT_LOOT,
			SettlementId: fmt.Sprintf("st-%013d-%06d", rec.OwnerId, i), TargetOwnerId: uint64(200000000000002 + int64(i)),
			Attempts: 0, NextRetryAt: 1701234500000,
		})
	}
	for i := 0; i < settlements; i++ {
		rec.ActiveTheftSettlements = append(rec.ActiveTheftSettlements, &settlementpb.TheftSettlement{
			SettlementId: fmt.Sprintf("st-%013d-%06d", rec.OwnerId, i), VisitorId: uint64(300000000000003 + int64(i)),
			TargetOwnerId: rec.OwnerId, CropCycleId: fmt.Sprintf("cc-%013d-%03d", rec.OwnerId, i%plotsOr1(plots)),
			Amount: 1, State: settlementpb.TheftSettlementState_TARGET_COMMITTED, CreatedAt: 1701234400000,
		})
	}
	for i := 0; i < receipts; i++ {
		rec.RecentLootReceipts = append(rec.RecentLootReceipts, &settlementpb.LootReceipt{
			SettlementId: fmt.Sprintf("st-%013d-%06d", rec.OwnerId, i), Amount: 1, ItemId: 2003, GrantedAt: 1701234450000,
		})
	}
	return rec
}

func plotsOr1(p int) int {
	if p <= 0 {
		return 1
	}
	return p
}

// TestOwnerCoreSizeBudget 实测不同规模 OwnerCoreRecord 的序列化字节数,产出大小预算表。
// 运行:go test ./internal/storage/ownercoresize/ -run SizeBudget -v
func TestOwnerCoreSizeBudget(t *testing.T) {
	type scenario struct {
		name                                                  string
		plots, crops, inv, ops, outbox, settlements, receipts int
	}
	scenarios := []scenario{
		{"新号(空农场)", 12, 0, 2, 0, 0, 0, 0},
		{"典型活跃玩家", 12, 10, 20, 8, 0, 0, 2},
		{"重度(满地块+社交)", 24, 24, 60, 16, 4, 4, 8},
		{"满载(热窗口拉满)", 36, 36, 120, 32, 16, 16, 32},
		{"病态(超大热窗口)", 48, 48, 200, 64, 64, 64, 64},
	}

	t.Log("OwnerCoreRecord 大小预算(proto.Size,单位字节):")
	t.Logf("%-22s %10s %10s", "场景", "字节", "≈KB")
	for _, s := range scenarios {
		rec := buildRecord(s.plots, s.crops, s.inv, s.ops, s.outbox, s.settlements, s.receipts)
		size := proto.Size(rec)
		// 同时验证可真正 marshal(排除非法组合)。
		if _, err := proto.Marshal(rec); err != nil {
			t.Fatalf("marshal %s: %v", s.name, err)
		}
		t.Logf("%-22s %10d %9.1f", s.name, size, float64(size)/1024.0)
	}
}
