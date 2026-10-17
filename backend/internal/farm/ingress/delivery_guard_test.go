package ingress

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	farmactor "farmgame/internal/farm/actor"

	"google.golang.org/protobuf/proto"
)

// deliveryGuardDownlink 是 A3 投递守卫测试的窄 Fake。它精确模拟"本地
// Enqueue 成功、远端结果失败"与独立 Control Lane,支持:
//   - 对普通 FARM_CHANGE / post-ready 条目分别配置远端 status(如 UNAVAILABLE、
//     QUEUE_FULL、STALE_CONNECTION、STALE_SUBSCRIPTION)或本地拒绝 error;
//   - missing result(callback 收到 nil);
//   - manual 模式:仅捕获 callback,由测试在任意时刻手动触发(模拟迟到回调)。
//
// Control Lane(EnqueueControlWithResult)记录在独立 controls 队列,可配置
// 本地拒绝,用于验证 terminal REJOIN_REQUIRED 不被普通变化队列挤掉、以及
// control lane 满时的 Fail Closed。
type deliveryGuardDownlink struct {
	mu               sync.Mutex
	items            []*logicrpcpb.DownlinkItem
	callbacks        []func(*logicrpcpb.DeliverBatchItemResult)
	controls         []*logicrpcpb.DownlinkItem
	changeStatus     logicrpcpb.DownlinkItemStatus // 普通 :change: 条目远端结果
	postReadyStatus  logicrpcpb.DownlinkItemStatus // :post-ready: 条目远端结果
	changeEnqueueErr error                         // 普通 :change: 条目本地拒绝
	postReadyErr     error                         // :post-ready: 条目本地拒绝
	controlErr       error                         // Control lane 本地拒绝
	controlStatus    logicrpcpb.DownlinkItemStatus // Control lane 远端结果
	missingResult    bool                          // 普通 lane callback(nil)
	manual           bool                          // 普通 lane 只捕获,不自动触发 callback
	manualControl    bool                          // Control lane 只捕获,不自动触发 callback
}

func (d *deliveryGuardDownlink) Enqueue(item *logicrpcpb.DownlinkItem) error {
	return d.EnqueueWithResult(item, nil)
}

func (d *deliveryGuardDownlink) EnqueueWithResult(item *logicrpcpb.DownlinkItem,
	callback func(*logicrpcpb.DeliverBatchItemResult)) error {
	d.mu.Lock()
	localErr := d.enqueueErrFor(item)
	if localErr != nil {
		d.mu.Unlock()
		return localErr
	}
	status := d.statusFor(item)
	missing := d.missingResult && isChangeItem(item)
	manual := d.manual
	d.items = append(d.items, proto.Clone(item).(*logicrpcpb.DownlinkItem))
	d.callbacks = append(d.callbacks, callback)
	d.mu.Unlock()
	if callback != nil && !manual {
		if missing {
			callback(nil)
		} else {
			callback(&logicrpcpb.DeliverBatchItemResult{ItemId: item.ItemId, Status: status})
		}
	}
	return nil
}

func (d *deliveryGuardDownlink) EnqueueControlWithResult(item *logicrpcpb.DownlinkItem,
	callback func(*logicrpcpb.DeliverBatchItemResult)) error {
	d.mu.Lock()
	err := d.controlErr
	if err != nil {
		d.mu.Unlock()
		return err
	}
	status := d.controlStatus
	manual := d.manualControl
	d.controls = append(d.controls, proto.Clone(item).(*logicrpcpb.DownlinkItem))
	d.mu.Unlock()
	if callback != nil && !manual {
		if status == logicrpcpb.DownlinkItemStatus_DOWNLINK_ITEM_STATUS_UNSPECIFIED {
			status = logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED
		}
		callback(&logicrpcpb.DeliverBatchItemResult{ItemId: item.ItemId, Status: status})
	}
	return nil
}

func isChangeItem(item *logicrpcpb.DownlinkItem) bool {
	if item == nil {
		return false
	}
	return strings.Contains(item.ItemId, ":change:") || strings.Contains(item.ItemId, ":post-ready:")
}

func (d *deliveryGuardDownlink) ActivateSubscription(_ context.Context,
	req *logicrpcpb.ActivateSubscriptionRequest) (*logicrpcpb.ActivateSubscriptionResponse, error) {
	return &logicrpcpb.ActivateSubscriptionResponse{
		Status:              logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED,
		ReadyThroughVersion: req.ReadyThroughVersion,
	}, nil
}

func (d *deliveryGuardDownlink) statusFor(item *logicrpcpb.DownlinkItem) logicrpcpb.DownlinkItemStatus {
	if item == nil {
		return logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED
	}
	if strings.Contains(item.ItemId, ":post-ready:") {
		if d.postReadyStatus != logicrpcpb.DownlinkItemStatus_DOWNLINK_ITEM_STATUS_UNSPECIFIED {
			return d.postReadyStatus
		}
	}
	if strings.Contains(item.ItemId, ":change:") ||
		strings.Contains(item.ItemId, ":post-ready:") {
		if d.changeStatus != logicrpcpb.DownlinkItemStatus_DOWNLINK_ITEM_STATUS_UNSPECIFIED {
			return d.changeStatus
		}
	}
	return logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED
}

func (d *deliveryGuardDownlink) enqueueErrFor(item *logicrpcpb.DownlinkItem) error {
	if item == nil {
		return nil
	}
	if strings.Contains(item.ItemId, ":post-ready:") && d.postReadyErr != nil {
		return d.postReadyErr
	}
	if strings.Contains(item.ItemId, ":change:") && d.changeEnqueueErr != nil {
		return d.changeEnqueueErr
	}
	return nil
}

// triggerCallback 手动触发最后一个匹配 item_id 片段的 callback(迟到/手动模式)。
// callback 在锁外执行,避免失效收尾反向进入 fake 时死锁。
func (d *deliveryGuardDownlink) triggerCallback(contains string, result *logicrpcpb.DeliverBatchItemResult) bool {
	d.mu.Lock()
	index := -1
	for i := len(d.callbacks) - 1; i >= 0; i-- {
		if d.callbacks[i] != nil && strings.Contains(d.items[i].ItemId, contains) {
			index = i
			break
		}
	}
	if index < 0 {
		d.mu.Unlock()
		return false
	}
	callback := d.callbacks[index]
	d.callbacks[index] = nil
	d.mu.Unlock()
	if callback != nil {
		callback(result)
	}
	return true
}

func (d *deliveryGuardDownlink) lenItems() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.items)
}

func (d *deliveryGuardDownlink) lenControls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.controls)
}

func (d *deliveryGuardDownlink) findRejoin(subscriptionID string, reason farmpb.RejoinReason) *farmpb.RejoinRequiredPush {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, item := range append(append([]*logicrpcpb.DownlinkItem{}, d.items...), d.controls...) {
		if item.SubscriptionId != subscriptionID {
			continue
		}
		push := decodeRejoinPush(item)
		if push != nil && (reason == farmpb.RejoinReason_REJOIN_REASON_UNSPECIFIED || push.Reason == reason) {
			return push
		}
	}
	return nil
}

func (d *deliveryGuardDownlink) findChangeItem(subscriptionID string, contains string) *logicrpcpb.DownlinkItem {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, item := range d.items {
		if item.SubscriptionId == subscriptionID && strings.Contains(item.ItemId, contains) {
			return proto.Clone(item).(*logicrpcpb.DownlinkItem)
		}
	}
	return nil
}

func decodeRejoinPush(item *logicrpcpb.DownlinkItem) *farmpb.RejoinRequiredPush {
	if item == nil {
		return nil
	}
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(item.Frame, &frame); err != nil {
		return nil
	}
	push := frame.GetServerPush()
	if push == nil || push.PushType != commonpb.PushType_REJOIN_REQUIRED {
		return nil
	}
	var required farmpb.RejoinRequiredPush
	if err := proto.Unmarshal(push.Payload, &required); err != nil {
		return nil
	}
	return &required
}

func waitDeliveryRejoin(t *testing.T, capture *deliveryGuardDownlink, subscriptionID string,
	reason farmpb.RejoinReason) *farmpb.RejoinRequiredPush {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if push := capture.findRejoin(subscriptionID, reason); push != nil {
			return push
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("missing REJOIN_REQUIRED reason=%s subscription=%s", reason, subscriptionID)
	return nil
}

func waitChangeItem(t *testing.T, capture *deliveryGuardDownlink, subscriptionID string, contains string) *logicrpcpb.DownlinkItem {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if item := capture.findChangeItem(subscriptionID, contains); item != nil {
			return item
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("missing downlink item containing %q subscription=%s", contains, subscriptionID)
	return nil
}

type deliveryJoin struct {
	trusted        *logicrpcpb.TrustedCommandContext
	subscriptionID string
}

func joinAndArm(t *testing.T, stack testStack, operationPrefix, connection string) deliveryJoin {
	t.Helper()
	operationID := strings.ReplaceAll(operationPrefix, " ", "-")
	trusted := joinTrusted(9, connection)
	response, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command: typedCommand(t, operationID+"-join", operationID+"-join", commonpb.CommandType_JOIN_FARM,
			&publicpb.JoinFarmRequest{TargetFarmId: 9}),
		Trusted:  trusted,
		Dispatch: farmActorDispatch(9),
	})
	if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_ACCEPTED {
		t.Fatalf("join resp=%v err=%v", response, err)
	}
	subscriptionID := joinSubscription(t, response)
	armJoin(t, stack, trusted, response)
	return deliveryJoin{trusted: trusted, subscriptionID: subscriptionID}
}

func targetFromTrusted(trusted *logicrpcpb.TrustedCommandContext) *logicrpcpb.DownlinkTargetFence {
	return &logicrpcpb.DownlinkTargetFence{
		TargetPlayerId: trusted.Session.PlayerId, GatewayId: trusted.Session.GatewayId,
		GatewayBootId: trusted.Session.GatewayBootId, ConnectionId: trusted.Session.ConnectionId,
		ConnectionGeneration: trusted.Session.ConnectionGeneration,
		SessionEpoch:         trusted.Session.ActiveSessionEpoch,
		SessionRouteRevision: trusted.Session.SessionRouteRevision,
	}
}

func plantVersion(t *testing.T, stack testStack, operationPrefix string, plot uint32) *farmpb.FarmCommandResult {
	t.Helper()
	operationID := strings.ReplaceAll(operationPrefix, " ", "-")
	outcome := stack.executeFarm(t, 9, typedCommand(t, operationID, "r-"+operationID, commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: plot, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1}))
	if outcome.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("plant %s=%v", operationPrefix, outcome)
	}
	var result farmpb.FarmCommandResult
	if err := proto.Unmarshal(outcome.ResultPayload, &result); err != nil {
		t.Fatal(err)
	}
	return &result
}

// TestDeliveryGuardChangeRemoteRejectionEmitsRejoin 覆盖远端拒绝且无后续
// Change 的场景:普通 FARM_CHANGE 本地 Enqueue 成功但远端返回
// UNAVAILABLE/QUEUE_FULL/STALE_CONNECTION/STALE_SUBSCRIPTION。Ingress 必须
// 通过精确 Fence 触发 MarkSubscriptionResyncRequired 并沿独立 Control Lane
// 下发 REJOIN_REQUIRED(invalid_from_version = 已提交版本),Presence 在
// control ack 后精确清理,不依赖后续 Change 探测。
func TestDeliveryGuardChangeRemoteRejectionEmitsRejoin(t *testing.T) {
	tests := []struct {
		name   string
		status logicrpcpb.DownlinkItemStatus
	}{
		{name: "network unavailable", status: logicrpcpb.DownlinkItemStatus_UNAVAILABLE},
		{name: "queue full", status: logicrpcpb.DownlinkItemStatus_QUEUE_FULL},
		{name: "stale connection", status: logicrpcpb.DownlinkItemStatus_STALE_CONNECTION},
		{name: "stale subscription", status: logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stack := newTestStack(t)
			capture := &deliveryGuardDownlink{changeStatus: tt.status}
			stack.farm.SetDownlinkEnqueuer(capture)
			stack.buySeed(t, 9, "buy-"+strings.ReplaceAll(tt.name, " ", "-"))
			join := joinAndArm(t, stack, tt.name, "conn-"+strings.ReplaceAll(tt.name, " ", "-"))
			waitPresencePhase(t, stack, 9, join.subscriptionID, farmactor.PresenceInFarm)

			planted := plantVersion(t, stack, "plant-"+tt.name, 0)
			required := waitDeliveryRejoin(t, capture, join.subscriptionID,
				farmpb.RejoinReason_REJOIN_REASON_DELIVERY_FAILED)
			if required.InvalidFromVersion == nil || *required.InvalidFromVersion != planted.Snapshot.FarmVersion {
				t.Fatalf("invalid_from=%v, want %d", required.InvalidFromVersion, planted.Snapshot.FarmVersion)
			}
			waitPresenceGone(t, stack, 9, join.subscriptionID)
			if capture.lenControls() == 0 {
				t.Fatal("terminal REJOIN_REQUIRED must use the reserved control lane")
			}
		})
	}
}

// TestDeliveryGuardMissingResultEmitsRejoin 覆盖 Dispatcher 未返回任何结果
// (missing result)的防御路径:callback 收到 nil 同样按失效处理,绝不静默丢弃。
func TestDeliveryGuardMissingResultEmitsRejoin(t *testing.T) {
	stack := newTestStack(t)
	capture := &deliveryGuardDownlink{missingResult: true}
	stack.farm.SetDownlinkEnqueuer(capture)
	stack.buySeed(t, 9, "buy-missing-result")
	join := joinAndArm(t, stack, "missing-result", "conn-missing-result")
	waitPresencePhase(t, stack, 9, join.subscriptionID, farmactor.PresenceInFarm)

	planted := plantVersion(t, stack, "plant-missing-result", 0)
	required := waitDeliveryRejoin(t, capture, join.subscriptionID,
		farmpb.RejoinReason_REJOIN_REASON_DELIVERY_FAILED)
	if required.InvalidFromVersion == nil || *required.InvalidFromVersion != planted.Snapshot.FarmVersion {
		t.Fatalf("invalid_from=%v, want %d", required.InvalidFromVersion, planted.Snapshot.FarmVersion)
	}
	waitPresenceGone(t, stack, 9, join.subscriptionID)
}

// TestDeliveryGuardPostReadyRejectionEmitsRejoin 覆盖 Ready 之后的 post-ready
// FARM_CHANGE 远端拒绝:即使 Presence 正在 Join 收尾,已提交版本也必须落入
// level-triggered 失效并要求 Fresh Join,而不是只清理 Presence。
func TestDeliveryGuardPostReadyRejectionEmitsRejoin(t *testing.T) {
	stack := newTestStack(t)
	capture := &deliveryGuardDownlink{postReadyStatus: logicrpcpb.DownlinkItemStatus_QUEUE_FULL, manual: true}
	stack.farm.SetDownlinkEnqueuer(capture)
	stack.buySeed(t, 9, "buy-post-ready")
	stack.buySeed(t, 9, "buy-post-ready-2")
	join := joinAndArm(t, stack, "post-ready", "conn-post-ready")

	// Joining 阶段种下第一颗 -> 进入 Catch-up 缓冲。
	first := plantVersion(t, stack, "plant-post-ready-first", 0)
	// Snapshot QueueAccepted -> Flushing,Catch-up DELTA_BATCH(v1) 入队。
	if !capture.triggerCallback(":snapshot:", &logicrpcpb.DeliverBatchItemResult{
		ItemId: "post-ready:snapshot", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	}) {
		t.Fatal("missing snapshot callback")
	}
	waitChangeItem(t, capture, join.subscriptionID, ":catchup:")
	// Flushing 阶段种下第二颗 -> 版本>ReadyCut,进入 PostReadyBuffer。
	second := plantVersion(t, stack, "plant-post-ready-second", 1)
	// Catch-up QueueAccepted -> Activating -> Ready -> post-ready FARM_CHANGE 入队。
	if !capture.triggerCallback(":catchup:", &logicrpcpb.DeliverBatchItemResult{
		ItemId: "post-ready:catchup", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	}) {
		t.Fatal("missing catch-up callback")
	}
	postReadyItem := waitChangeItem(t, capture, join.subscriptionID, ":post-ready:")
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(postReadyItem.Frame, &frame); err != nil {
		t.Fatal(err)
	}
	if got := frame.GetServerPush().GetPushType(); got != commonpb.PushType_FARM_CHANGE {
		t.Fatalf("post-ready push_type=%v", got)
	}
	// 远端拒绝 post-ready 条目。
	if !capture.triggerCallback(":post-ready:", &logicrpcpb.DeliverBatchItemResult{
		ItemId: postReadyItem.ItemId, Status: logicrpcpb.DownlinkItemStatus_QUEUE_FULL,
	}) {
		t.Fatal("missing post-ready callback")
	}
	required := waitDeliveryRejoin(t, capture, join.subscriptionID,
		farmpb.RejoinReason_REJOIN_REASON_DELIVERY_FAILED)
	if required.InvalidFromVersion == nil || *required.InvalidFromVersion != second.Snapshot.FarmVersion {
		t.Fatalf("invalid_from=%v, want %d", required.InvalidFromVersion, second.Snapshot.FarmVersion)
	}
	if first.Snapshot.FarmVersion >= second.Snapshot.FarmVersion {
		t.Fatalf("second plant must advance the farm version: first=%d second=%d",
			first.Snapshot.FarmVersion, second.Snapshot.FarmVersion)
	}
	waitPresenceGone(t, stack, 9, join.subscriptionID)
}

// TestDeliveryGuardPostReadyLocalRejectionEmitsRejoin 覆盖 post-ready 条目的
// 本地 Enqueue 拒绝:item 从未进入任何投递队列,同样必须 level-triggered 失效。
func TestDeliveryGuardPostReadyLocalRejectionEmitsRejoin(t *testing.T) {
	stack := newTestStack(t)
	capture := &deliveryGuardDownlink{postReadyErr: errors.New("post-ready lane full"), manual: true}
	stack.farm.SetDownlinkEnqueuer(capture)
	stack.buySeed(t, 9, "buy-post-ready-local")
	stack.buySeed(t, 9, "buy-post-ready-local-2")
	join := joinAndArm(t, stack, "post-ready-local", "conn-post-ready-local")

	plantVersion(t, stack, "plant-post-ready-local-first", 0)
	if !capture.triggerCallback(":snapshot:", &logicrpcpb.DeliverBatchItemResult{
		ItemId: "post-ready-local:snapshot", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	}) {
		t.Fatal("missing snapshot callback")
	}
	waitChangeItem(t, capture, join.subscriptionID, ":catchup:")
	second := plantVersion(t, stack, "plant-post-ready-local-second", 1)
	if !capture.triggerCallback(":catchup:", &logicrpcpb.DeliverBatchItemResult{
		ItemId: "post-ready-local:catchup", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	}) {
		t.Fatal("missing catch-up callback")
	}
	// post-ready EnqueueWithResult 本地拒绝 -> runDeliveryFailure fail closed。
	// 本地拒绝的 item 从未进入投递队列,直接等待 level-triggered 失效出现。
	required := waitDeliveryRejoin(t, capture, join.subscriptionID,
		farmpb.RejoinReason_REJOIN_REASON_DELIVERY_FAILED)
	if required.InvalidFromVersion == nil || *required.InvalidFromVersion != second.Snapshot.FarmVersion {
		t.Fatalf("invalid_from=%v, want %d", required.InvalidFromVersion, second.Snapshot.FarmVersion)
	}
	waitPresenceGone(t, stack, 9, join.subscriptionID)
}

// TestDeliveryGuardStaleCallbackDoesNotCloseRejoinedSubscription 覆盖旧 callback
// 迟到:change 的 Enqueue 早已"本地成功",但 callback 在旧订阅被精确清理、新
// Fresh Join 已建立之后才带旧 Fence 返回失败。失效收尾按完整 Fence(含
// PresenceEpoch/connection)匹配,必须 no-op,不得关闭新 Subscription。
func TestDeliveryGuardStaleCallbackDoesNotCloseRejoinedSubscription(t *testing.T) {
	stack := newTestStack(t)
	capture := &deliveryGuardDownlink{manual: true}
	stack.farm.SetDownlinkEnqueuer(capture)
	stack.buySeed(t, 9, "buy-stale-callback")
	stack.buySeed(t, 9, "buy-stale-callback-2")

	oldJoin := joinAndArm(t, stack, "stale-old", "conn-stale-old")
	if !capture.triggerCallback(":snapshot:", &logicrpcpb.DeliverBatchItemResult{
		ItemId: "stale-old:snapshot", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	}) {
		t.Fatal("missing old snapshot callback")
	}
	waitPresencePhase(t, stack, 9, oldJoin.subscriptionID, farmactor.PresenceInFarm)

	// 第一个已提交 change:本地入队成功,但 callback 被捕获且稍后才触发。
	plantVersion(t, stack, "plant-stale-old", 0)
	waitChangeItem(t, capture, oldJoin.subscriptionID, ":change:")

	// 旧订阅精确清理,随后同 player 以新 Fence Fresh Join。
	cleanupResponse, err := stack.farm.CleanupSubscription(context.Background(), &logicrpcpb.CleanupSubscriptionRequest{
		Subscription: &logicrpcpb.SubscriptionFence{
			FarmId: 9, SubscriptionId: oldJoin.subscriptionID,
			JoinAttemptId: oldJoin.trusted.JoinAttempt.JoinAttemptId,
		},
		Target: targetFromTrusted(oldJoin.trusted),
	})
	if err != nil || cleanupResponse == nil || !cleanupResponse.Removed {
		t.Fatalf("old subscription cleanup failed response=%v err=%v", cleanupResponse, err)
	}
	waitPresenceGone(t, stack, 9, oldJoin.subscriptionID)

	newJoin := joinAndArm(t, stack, "stale-new", "conn-stale-new")
	if !capture.triggerCallback(":snapshot:", &logicrpcpb.DeliverBatchItemResult{
		ItemId: "stale-new:snapshot", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	}) {
		t.Fatal("missing new snapshot callback")
	}
	waitPresencePhase(t, stack, 9, newJoin.subscriptionID, farmactor.PresenceInFarm)

	// 旧 callback 迟到返回失败:带旧 Fence,必须被拒绝为 no-op。
	if !capture.triggerCallback(":change:", &logicrpcpb.DeliverBatchItemResult{
		ItemId: oldJoin.subscriptionID + ":change:", Status: logicrpcpb.DownlinkItemStatus_UNAVAILABLE,
	}) {
		t.Fatal("missing stale change callback")
	}
	time.Sleep(150 * time.Millisecond)
	if push := capture.findRejoin(newJoin.subscriptionID, farmpb.RejoinReason_REJOIN_REASON_UNSPECIFIED); push != nil {
		t.Fatalf("stale callback closed rejoined subscription: %v", push)
	}
	if push := capture.findRejoin(oldJoin.subscriptionID, farmpb.RejoinReason_REJOIN_REASON_UNSPECIFIED); push != nil {
		t.Fatalf("stale callback emitted terminal rejoin for removed subscription: %v", push)
	}
	if phase, ok := presencePhase(t, stack, 9, newJoin.subscriptionID); !ok || phase != farmactor.PresenceInFarm {
		t.Fatalf("stale callback disturbed new subscription: phase=%v ok=%v", phase, ok)
	}

	// 新订阅随后仍能正常收到并确认后续 change。
	plantVersion(t, stack, "plant-stale-new", 1)
	waitChangeItem(t, capture, newJoin.subscriptionID, ":change:")
	if !capture.triggerCallback(":change:", &logicrpcpb.DeliverBatchItemResult{
		ItemId: newJoin.subscriptionID + ":change:", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	}) {
		t.Fatal("missing new change callback")
	}
	time.Sleep(150 * time.Millisecond)
	if phase, ok := presencePhase(t, stack, 9, newJoin.subscriptionID); !ok || phase != farmactor.PresenceInFarm {
		t.Fatalf("new subscription disturbed after accepted change: phase=%v ok=%v", phase, ok)
	}
}

// TestDeliveryGuardControlLaneFullFailsClosed 覆盖 Control Lane 也满的场景:
// REJOIN_REQUIRED 无法入队时,Ingress 对精确 Presence Fail Closed——主动清理
// 该 Presence 并解除其占用,使后续 Fresh Join 可进,而不是把客户端永久挡在
// fallback TTL 之外。
func TestDeliveryGuardControlLaneFullFailsClosed(t *testing.T) {
	stack := newTestStack(t)
	capture := &deliveryGuardDownlink{
		changeStatus: logicrpcpb.DownlinkItemStatus_UNAVAILABLE,
		controlErr:   errors.New("control lane full"),
	}
	stack.farm.SetDownlinkEnqueuer(capture)
	stack.buySeed(t, 9, "buy-control-full")
	join := joinAndArm(t, stack, "control-full", "conn-control-full")
	waitPresencePhase(t, stack, 9, join.subscriptionID, farmactor.PresenceInFarm)

	plantVersion(t, stack, "plant-control-full", 0)
	// Control lane 满 -> 精确 Presence 被清理,连接/Presence Fail Closed。
	waitPresenceGone(t, stack, 9, join.subscriptionID)
	if capture.lenControls() != 0 {
		t.Fatalf("control enqueue should have failed locally, got %d controls", capture.lenControls())
	}

	// Fresh Join 必须可进。
	stack.buySeed(t, 9, "buy-control-full-fresh")
	fresh := joinAndArm(t, stack, "control-full-fresh", "conn-control-full-fresh")
	if fresh.subscriptionID == join.subscriptionID {
		t.Fatal("Fresh Join reused the failed subscription")
	}
	if !capture.triggerCallback(":snapshot:", &logicrpcpb.DeliverBatchItemResult{
		ItemId: "control-full-fresh:snapshot", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	}) {
		t.Fatal("missing fresh snapshot callback")
	}
	waitPresencePhase(t, stack, 9, fresh.subscriptionID, farmactor.PresenceInFarm)
}
