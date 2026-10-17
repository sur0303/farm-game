package ownerstore

import (
	"context"
	"errors"

	economypb "farmgame/api/gen/economy"
	farmpb "farmgame/api/gen/farm"
	ownercorepb "farmgame/api/gen/ownercore"
)

// DefaultSchemaVersion 是建档时记录的 schema 版本。
const DefaultSchemaVersion = 1

// DefaultInitialCoin 是新号初始金币(预处理,真实发放以 Join Snapshot 为准)。
const DefaultInitialCoin = int64(100)

// DefaultPlotCount 是新农场地块数。
const DefaultPlotCount = 12

// ErrBootstrapConflict 表示已存在 Owner 的 bootstrap_id 与本次请求不一致。
var ErrBootstrapConflict = errors.New("ownerstore: bootstrap_id conflict")

// EnsureOwnerInitialized 幂等地确保某 player 的 OwnerCore 已建档。
//
// 语义见 docs/design/41-sequences.md §1 与 00-overview.md 冻结结论:
//   - 记录不存在:条件创建一次空 OwnerCore 与初始资产;
//   - 已存在且 bootstrap_id 一致:返回 created=false,不重复发初始资产;
//   - bootstrap_id 冲突:返回 ErrBootstrapConflict;
//   - 并发重复调用只允许一次 Insert 真正成功,其余走"已存在"分支。
//
// 调用方(Logic/Actor)在结果返回前不得向用户宣称建档成功。
func EnsureOwnerInitialized(
	ctx context.Context,
	store OwnerStore,
	playerID uint64,
	bootstrapID string,
	initialRuleVersion int32,
) (created bool, err error) {
	rec, _, getErr := store.Get(ctx, playerID)
	if getErr == nil {
		// 已存在:校验初始化事实一致性。
		if rec.OwnerMeta != nil && rec.OwnerMeta.BootstrapId != "" &&
			rec.OwnerMeta.BootstrapId != bootstrapID {
			return false, ErrBootstrapConflict
		}
		return false, nil
	}
	if !errors.Is(getErr, ErrOwnerNotFound) {
		return false, getErr
	}
	// 不存在:尝试条件创建。
	init := BuildInitialRecord(playerID, bootstrapID, initialRuleVersion)
	if insErr := store.Insert(ctx, init); insErr != nil {
		if errors.Is(insErr, ErrOwnerExists) {
			// 竞争失败:另一条并发建档已成功,复用其记录。
			existing, _, e2 := store.Get(ctx, playerID)
			if e2 != nil {
				// Insert 的结果已知是“已有记录”，但后续读取失败时不能把
				// 建档当作成功；否则会把超时/未知结果伪装成已初始化。
				return false, e2
			}
			if existing == nil {
				return false, ErrUnavailable
			}
			if existing.OwnerMeta != nil &&
				existing.OwnerMeta.BootstrapId != "" && existing.OwnerMeta.BootstrapId != bootstrapID {
				return false, ErrBootstrapConflict
			}
			return false, nil
		}
		return false, insErr
	}
	return true, nil
}

// BuildInitialRecord 构造一个空农场的新 OwnerCore(初始资产/12 块空地)。
func BuildInitialRecord(playerID uint64, bootstrapID string, initialRuleVersion int32) *ownercorepb.OwnerCoreRecord {
	plots := make([]*farmpb.Plot, 0, DefaultPlotCount)
	for i := 0; i < DefaultPlotCount; i++ {
		plots = append(plots, &farmpb.Plot{Index: int32(i), PlotVersion: 1})
	}
	return &ownercorepb.OwnerCoreRecord{
		OwnerId: playerID,
		OwnerMeta: &ownercorepb.OwnerMeta{
			OwnerId:                     playerID,
			OwnerEpoch:                  1,
			StateVersion:                1,
			SchemaVersion:               DefaultSchemaVersion,
			BootstrapId:                 bootstrapID,
			AppliedBootstrapRuleVersion: initialRuleVersion,
		},
		PlayerProfile: &economypb.PlayerProfile{
			Nickname:    "农场主",
			CreatedAt:   nowMs(),
			LastLoginAt: nowMs(),
		},
		Progression: &economypb.Progression{Level: 1},
		Wallet:      &economypb.Wallet{Coin: DefaultInitialCoin},
		Inventory:   &economypb.Inventory{},
		Farm: &farmpb.FarmCore{
			FarmId:      playerID,
			FarmVersion: 0,
			Plots:       plots,
		},
	}
}
