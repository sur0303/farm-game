package actor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	"farmgame/internal/logic/actor/activation"
	"farmgame/internal/logic/actor/idle"
	"farmgame/internal/storage/blobstore"
)

func plantForGuard(ctx context.Context, actor *FarmActor, op string) (any, error) {
	return actor.Plant(ctx, op, testPlantCropCycleID(actor.farmID, op), []byte("plant-hash-"+op), &farmpb.PlantCropRequest{
		PlotIndex: 0, CropDefinitionId: 1,
	}, 0, 0, time.UnixMilli(1_800_000_000_000), time.Minute)
}

// TestGuardClearedOnExplicitCommitFailure 验证:明确未提交时 Guard
// 被清除且不得发布变化(27 §5.17)。
func TestGuardClearedOnExplicitCommitFailure(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	ctx := context.Background()

	var saveErr error
	var guardAfterFailure bool
	_, err := registry.Submit(ctx, 88, func(ctx context.Context, actor *FarmActor) (any, error) {
		// 破坏 recordVersion 使 CAS 明确冲突。
		actor.recordVersion = 99
		before := actor.record
		actor.record.Core.FarmVersion = 5
		saveErr = actor.save(ctx, before, "op-conflict", commonpb.CommandType_PLANT)
		_, _, guardAfterFailure = actor.guardSnapshot()
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(saveErr, blobstore.ErrCASConflict) {
		t.Fatalf("save err=%v, want CAS conflict", saveErr)
	}
	if guardAfterFailure {
		t.Fatal("guard must be cleared on explicit commit failure")
	}
}

// TestGuardUnknownRetainedUntilResolved 验证:TIMEOUT_UNKNOWN 保留 Guard,
// 权威解析完成后清除(27 §5.17)。
func TestGuardUnknownRetainedUntilResolved(t *testing.T) {
	store := newAmbiguousCASStore(false)
	registry := NewRegistry(store, 8)
	registry.SetAutoResolveUnknown(false)
	defer registry.Stop()
	ctx := context.Background()

	_, err := registry.Submit(ctx, 41, func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.Plant(ctx, "guard-unknown-op", testPlantCropCycleID(actor.farmID, "guard-unknown-op"), []byte("plant-hash"), &farmpb.PlantCropRequest{
			PlotIndex: 0, CropDefinitionId: 1,
		}, 0, 0, time.UnixMilli(1_800_000_000_000), time.Minute)
	})
	if !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("plant err=%v, want TIMEOUT_UNKNOWN", err)
	}
	var retainedVersion uint64
	var retained bool
	_, err = registry.SubmitSystem(ctx, 41, 0, func(ctx context.Context, actor *FarmActor) (any, error) {
		retainedVersion, _, retained = actor.guardSnapshot()
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !retained || retainedVersion == 0 {
		t.Fatalf("guard must be retained as UNKNOWN, retained=%v version=%d", retained, retainedVersion)
	}
	store.releaseReads()
	if _, err := registry.ResolveUnknown(ctx, 41); err != nil {
		t.Fatal(err)
	}
	var stillGuarded bool
	_, err = registry.SubmitSystem(ctx, 41, 0, func(ctx context.Context, actor *FarmActor) (any, error) {
		_, _, stillGuarded = actor.guardSnapshot()
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if stillGuarded {
		t.Fatal("guard must be cleared after unknown resolved")
	}
}

// TestPanicWithPendingGuardInstallsInvalid 验证:Supervisor 对 panic 时仍持有的
// Guard 幂等安装 invalid_from_version<=V(27 §5.17)。
func TestPanicWithPendingGuardInstallsInvalid(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	ctx := context.Background()

	_, err := registry.Submit(ctx, 55, func(ctx context.Context, actor *FarmActor) (any, error) {
		actor.guardInstall(7)
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	registry.handlePanic(55)

	// 27 §5.17:Holder 丢失时 Hub 随订阅一起回收,失效状态不跨 Lane 存活
	// (客户端按重建合同 Fresh Join);可观察产物是置位计数与生成记录。
	if got := registry.SubscriptionHub().InvalidTotal(); got != 1 {
		t.Fatalf("invalid_total=%d, want 1(panic guard 置位已计数)", got)
	}
	// min_non_zero 语义由 subscription 包的 deliveryguard_test 覆盖。
}

// TestGuardCommittedThenClearedAfterPublish 验证:提交成功 Guard 转 COMMITTED,
// 变化进入实时流后清除 Guard 并推进 last_enqueued_version。
func TestGuardCommittedThenClearedAfterPublish(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	ctx := context.Background()
	now := time.Now()

	// 直接驱动 save→publish 窗口:save 后 Guard 应为 COMMITTED,publish 后清除。
	var guardVersion uint64
	var guardState commitGuardState
	var guardOK, clearedAfterPublish bool
	_, err := registry.Submit(ctx, 88, func(ctx context.Context, actor *FarmActor) (any, error) {
		before := actor.record
		actor.record.Core.FarmVersion = 1
		if err := actor.save(ctx, before, "op-ok", commonpb.CommandType_PLANT); err != nil {
			return nil, err
		}
		guardVersion, guardState, guardOK = actor.guardSnapshot()
		// 提交成功但 publish 前 Guard 仍在(两结果不变量的窗口期)。
		if _, _, _, err := actor.publish(actor.record.Core.Plots[0], now); err != nil {
			return nil, err
		}
		_, _, stillGuarded := actor.guardSnapshot()
		clearedAfterPublish = !stillGuarded
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !guardOK || guardVersion != 1 || guardState != guardCommitted {
		t.Fatalf("guard=(%d,%d,%v), want (1,COMMITTED,true)", guardVersion, guardState, guardOK)
	}
	if !clearedAfterPublish {
		t.Fatal("guard must be cleared after change entered the realtime stream")
	}
	realtime, stateOK, err := registry.SubscriptionHub().RealtimeState(ctx, 88)
	if err != nil || !stateOK {
		t.Fatalf("realtime ok=%v err=%v", stateOK, err)
	}
	if realtime.LastEnqueuedVersion != 1 {
		t.Fatalf("last_enqueued=%d, want 1", realtime.LastEnqueuedVersion)
	}
}

func TestPendingGuardBlocksPassivationAndLeaseEvictionSettlesIt(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	ctx := context.Background()
	if _, err := registry.Submit(ctx, 89, func(context.Context, *FarmActor) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.SubmitSystem(ctx, 89, 0, func(context.Context, *FarmActor) (any, error) {
		registry.actors[89].guardInstall(1)
		registry.actors[89].guardMarkCommitted()
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	registry.passivation.IdleTimeout = time.Nanosecond
	registry.passivation.Now = func() time.Time { return time.Now().Add(time.Hour) }
	result := registry.tryPassivateIdle(ctx, 89)
	if result.Decision != idle.DecisionBusy || !containsString(result.Reasons, BlockerParticipantUnanchor) {
		t.Fatalf("pending guard passivation result=%+v", result)
	}
	if err := registry.BeginPassivate(89); err == nil || !strings.Contains(err.Error(), BlockerParticipantUnanchor) {
		t.Fatalf("BeginPassivate err=%v, want guard blocker", err)
	}
	if err := registry.NotifyLeaseLost(ctx, 89); err != nil {
		t.Fatal(err)
	}
	if got := registry.Activation().StateOf(89); got != activation.Absent {
		t.Fatalf("lease eviction state=%s, want ABSENT", got)
	}
	if got := registry.SubscriptionHub().InvalidTotal(); got != 1 {
		t.Fatalf("guard invalidation count=%d, want 1", got)
	}
}

func TestLegacyUnknownLeaseLossSettlesGuardBeforeEviction(t *testing.T) {
	const farmID = uint64(1248)
	store := newAmbiguousCASStore(true)
	registry := NewRegistry(store, 8)
	registry.SetAutoResolveUnknown(false)
	defer registry.Stop()
	ctx := context.Background()

	if _, err := registry.Submit(ctx, farmID, func(ctx context.Context, actor *FarmActor) (any, error) {
		return plantForGuard(ctx, actor, "legacy-unknown-lease-loss")
	}); !errors.Is(err, blobstore.ErrTimeoutUnknown) {
		t.Fatalf("plant err=%v, want TIMEOUT_UNKNOWN", err)
	}
	if got := registry.Activation().StateOf(farmID); got != activation.ResolvingUnknown {
		t.Fatalf("activation=%s, want RESOLVING_UNKNOWN", got)
	}

	registry.SetWritableCheck(func(uint64) bool { return false })
	invalidBefore := registry.SubscriptionHub().InvalidTotal()
	resolution, err := registry.ResolveUnknown(ctx, farmID)
	if !errors.Is(err, activation.ErrStaleRoute) || resolution != UnknownStillUnknown {
		t.Fatalf("resolution=%v err=%v, want stale route with unresolved result", resolution, err)
	}
	if got := registry.SubscriptionHub().InvalidTotal(); got <= invalidBefore {
		t.Fatalf("legacy lease-loss eviction bypassed Guard fence: invalid_total=%d before=%d", got, invalidBefore)
	}
	if got := registry.Activation().StateOf(farmID); got != activation.Absent {
		t.Fatalf("settled fenced eviction state=%s, want ABSENT", got)
	}
}
