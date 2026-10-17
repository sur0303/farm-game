package ingress

import (
	"context"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	farmactor "farmgame/internal/farm/actor"

	"google.golang.org/protobuf/proto"
)

// TestJoinContinuationItemsCarryPresenceEpochFence 钉死 26 §3.5 与 26.4 审计缺陷修复:
// Join 主链每个下行条目(SNAPSHOT → Catch-up DELTA_BATCH → post-ready FARM_CHANGE)
// 必须携带同一 PresenceEpoch 与既有 Fence(subscription_id/connection_id/
// transport_generation/JoinFence/TargetFence),不被真实 Gate 判为 stale connection。
func TestJoinContinuationItemsCarryPresenceEpochFence(t *testing.T) {
	stack := newTestStack(t)
	capture := &resultCaptureDownlink{}
	stack.farm.SetDownlinkEnqueuer(capture)
	stack.buySeed(t, 9, "buy-fence-1")
	stack.buySeed(t, 9, "buy-fence-2")

	trusted := joinTrusted(9, "conn-fence")
	response, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command: typedCommand(t, "join-fence-op", "join-fence", commonpb.CommandType_JOIN_FARM,
			&publicpb.JoinFarmRequest{TargetFarmId: 9}),
		Trusted:  trusted,
		Dispatch: farmActorDispatch(9),
	})
	if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_ACCEPTED {
		t.Fatalf("join resp=%v err=%v", response, err)
	}
	subscriptionID := joinSubscription(t, response)
	armJoin(t, stack, trusted, response)

	// Joining 阶段种下第一颗 -> 变化进入 Catch-up 缓冲。
	if planted := stack.executeFarm(t, 9, typedCommand(t, "plant-joining", "r-plant-joining",
		commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: 0, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1})); planted.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("plant during joining=%v", planted)
	}

	snapshotItem := capture.item(0)
	if snapshotItem == nil {
		t.Fatal("missing snapshot downlink item")
	}
	if snapshotItem.GetPresenceEpoch() == 0 {
		t.Fatalf("snapshot item must carry the authoritative presence epoch: %v", snapshotItem)
	}
	epoch := snapshotItem.GetPresenceEpoch()
	assertFence := func(name string, item *logicrpcpb.DownlinkItem) {
		t.Helper()
		if item == nil {
			t.Fatalf("missing %s downlink item", name)
		}
		if item.GetPresenceEpoch() != epoch {
			t.Errorf("%s presence_epoch=%d, want %d(与 SNAPSHOT 同源)", name, item.GetPresenceEpoch(), epoch)
		}
		if item.GetSubscriptionId() != subscriptionID {
			t.Errorf("%s subscription_id=%q, want %q", name, item.GetSubscriptionId(), subscriptionID)
		}
		if item.GetConnectionId() != "conn-fence" {
			t.Errorf("%s connection_id=%q, want conn-fence", name, item.GetConnectionId())
		}
		if item.GetJoinFence().GetSubscriptionId() != subscriptionID ||
			item.GetJoinFence().GetJoinAttemptId() == "" {
			t.Errorf("%s join fence incomplete: %v", name, item.GetJoinFence())
		}
		if item.GetTargetFence().GetConnectionId() != "conn-fence" ||
			item.GetTransportGeneration() != item.GetTargetFence().GetConnectionGeneration() {
			t.Errorf("%s target fence/generation mismatch: item=(%s,%d) fence=(%s,%d)",
				name, item.GetConnectionId(), item.GetTransportGeneration(),
				item.GetTargetFence().GetConnectionId(), item.GetTargetFence().GetConnectionGeneration())
		}
	}
	pushTypeOf := func(t *testing.T, item *logicrpcpb.DownlinkItem) commonpb.PushType {
		t.Helper()
		var frame commonpb.FarmFrame
		if err := proto.Unmarshal(item.GetFrame(), &frame); err != nil {
			t.Fatalf("decode downlink frame: %v", err)
		}
		return frame.GetServerPush().GetPushType()
	}
	assertFence("snapshot", snapshotItem)
	if got := pushTypeOf(t, snapshotItem); got != commonpb.PushType_FARM_SNAPSHOT {
		t.Fatalf("item[0] push_type=%v, want FARM_SNAPSHOT", got)
	}

	// Snapshot ack -> Flushing,Catch-up 条目入队,必须携带同一 epoch。
	snapshotCB := capture.takeCallback(0)
	if snapshotCB == nil {
		t.Fatal("missing snapshot callback")
	}
	snapshotCB(&logicrpcpb.DeliverBatchItemResult{
		ItemId: "join-fence:snapshot", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && capture.lenItems() < 2 {
		time.Sleep(time.Millisecond)
	}
	catchupItem := capture.item(1)
	if catchupItem == nil {
		t.Fatalf("missing catch-up downlink item; items=%d", capture.lenItems())
	}
	if got := pushTypeOf(t, catchupItem); got != commonpb.PushType_FARM_DELTA_BATCH {
		t.Fatalf("catch-up push_type=%v, want FARM_DELTA_BATCH", got)
	}
	assertFence("catch-up", catchupItem)

	// Flushing 阶段种下第二颗 -> version>ReadyCut,进入 PostReadyBuffer(HC-ACCESS-39)。
	if planted := stack.executeFarm(t, 9, typedCommand(t, "plant-flushing", "r-plant-flushing",
		commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: 1, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1})); planted.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("plant during flushing=%v", planted)
	}

	// Catch-up ack -> Activating -> activateJoinAsync -> Ready 回调 ->
	// post-ready FARM_CHANGE 入队,必须携带同一 PresenceEpoch。
	changeCB := capture.takeCallback(1)
	if changeCB == nil {
		t.Fatal("missing catch-up callback")
	}
	changeCB(&logicrpcpb.DeliverBatchItemResult{
		ItemId: "join-fence:change", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	})
	waitPresencePhase(t, stack, 9, subscriptionID, farmactor.PresenceInFarm)
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && capture.lenItems() < 3 {
		time.Sleep(time.Millisecond)
	}
	postReadyItem := capture.item(2)
	if postReadyItem == nil {
		t.Fatalf("missing post-ready downlink item; items=%d", capture.lenItems())
	}
	if got := pushTypeOf(t, postReadyItem); got != commonpb.PushType_FARM_CHANGE {
		t.Fatalf("post-ready push_type=%v, want FARM_CHANGE", got)
	}
	assertFence("post-ready", postReadyItem)
}
