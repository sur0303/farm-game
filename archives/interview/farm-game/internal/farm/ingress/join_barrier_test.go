package ingress

import (
	"context"
	"sync"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	farmactor "farmgame/internal/farm/actor"
	"farmgame/internal/gateway/session"

	"google.golang.org/protobuf/proto"
)

type resultCaptureDownlink struct {
	mu        sync.Mutex
	items     []*logicrpcpb.DownlinkItem
	callbacks []func(*logicrpcpb.DeliverBatchItemResult)
}

func (c *resultCaptureDownlink) Enqueue(item *logicrpcpb.DownlinkItem) error {
	return c.EnqueueWithResult(item, nil)
}

func (c *resultCaptureDownlink) EnqueueWithResult(item *logicrpcpb.DownlinkItem, callback func(*logicrpcpb.DeliverBatchItemResult)) error {
	c.mu.Lock()
	c.items = append(c.items, proto.Clone(item).(*logicrpcpb.DownlinkItem))
	c.callbacks = append(c.callbacks, callback)
	c.mu.Unlock()
	return nil
}

func (c *resultCaptureDownlink) ActivateSubscription(_ context.Context,
	req *logicrpcpb.ActivateSubscriptionRequest) (*logicrpcpb.ActivateSubscriptionResponse, error) {
	return &logicrpcpb.ActivateSubscriptionResponse{
		Status:              logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED,
		ReadyThroughVersion: req.ReadyThroughVersion,
	}, nil
}

func (c *resultCaptureDownlink) takeCallback(index int) func(*logicrpcpb.DeliverBatchItemResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if index < 0 || index >= len(c.callbacks) {
		return nil
	}
	return c.callbacks[index]
}

func (c *resultCaptureDownlink) item(index int) *logicrpcpb.DownlinkItem {
	c.mu.Lock()
	defer c.mu.Unlock()
	if index < 0 || index >= len(c.items) {
		return nil
	}
	return proto.Clone(c.items[index]).(*logicrpcpb.DownlinkItem)
}

func (c *resultCaptureDownlink) lenItems() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func joinTrusted(playerID uint64, connectionID string) *logicrpcpb.TrustedCommandContext {
	return &logicrpcpb.TrustedCommandContext{
		Session: &logicrpcpb.SessionPermitSnapshot{
			PlayerId: playerID, ConnectionId: connectionID, ConnectionGeneration: 1,
			GatewayId: "gate-a", GatewayBootId: "boot-a", ActiveSessionEpoch: 1, SessionRouteRevision: 1,
		},
		JoinAttempt: &logicrpcpb.JoinAdmissionContext{JoinAttemptId: "attempt-" + connectionID, TargetFarmId: playerID},
	}
}

func presencePhase(t *testing.T, stack testStack, farmID uint64, subscriptionID string) (farmactor.PresencePhase, bool) {
	t.Helper()
	value, err := stack.farm.registry.Submit(context.Background(), farmID, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		phase, ok := actor.PresencePhase(subscriptionID)
		return []any{phase, ok}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	tuple := value.([]any)
	return tuple[0].(farmactor.PresencePhase), tuple[1].(bool)
}

func waitPresenceGone(t *testing.T, stack testStack, farmID uint64, subscriptionID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := presencePhase(t, stack, farmID, subscriptionID); !ok {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("presence %s still present after QUEUE_FULL leave", subscriptionID)
}

func waitPresencePhase(t *testing.T, stack testStack, farmID uint64, subscriptionID string, want farmactor.PresencePhase) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, ok := presencePhase(t, stack, farmID, subscriptionID)
		if ok && got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	got, ok := presencePhase(t, stack, farmID, subscriptionID)
	t.Fatalf("presence phase=%v ok=%v, want %v", got, ok, want)
}

func joinSubscription(t *testing.T, response *logicrpcpb.ExecuteCommandResponse) string {
	t.Helper()
	metadata := response.GetBusiness().GetJoinAccepted()
	if metadata == nil || metadata.SubscriptionId == "" || metadata.JoinAttemptId == "" {
		t.Fatalf("JoinFarm ACCEPTED metadata=%v", metadata)
	}
	return metadata.SubscriptionId
}

func armJoin(t *testing.T, stack testStack, trusted *logicrpcpb.TrustedCommandContext,
	response *logicrpcpb.ExecuteCommandResponse) *logicrpcpb.ArmSnapshotResponse {
	t.Helper()
	metadata := response.GetBusiness().GetJoinAccepted()
	if metadata == nil || trusted == nil || trusted.Session == nil {
		t.Fatalf("cannot arm incomplete Join metadata=%v trusted=%v", metadata, trusted)
	}
	target := &logicrpcpb.DownlinkTargetFence{
		TargetPlayerId: trusted.Session.PlayerId, GatewayId: trusted.Session.GatewayId,
		GatewayBootId: trusted.Session.GatewayBootId, ConnectionId: trusted.Session.ConnectionId,
		ConnectionGeneration: trusted.Session.ConnectionGeneration,
		SessionEpoch:         trusted.Session.ActiveSessionEpoch,
		SessionRouteRevision: trusted.Session.SessionRouteRevision,
	}
	armed, err := stack.farm.ArmSnapshot(context.Background(), &logicrpcpb.ArmSnapshotRequest{
		Subscription: &logicrpcpb.SubscriptionFence{
			FarmId: metadata.TargetFarmId, SubscriptionId: metadata.SubscriptionId,
			JoinAttemptId: metadata.JoinAttemptId,
		},
		Target: target,
	})
	if err != nil || armed == nil ||
		(armed.Status != logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_ARMED &&
			armed.Status != logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_ALREADY_ARMED) {
		t.Fatalf("ArmSnapshot response=%v err=%v", armed, err)
	}
	return armed
}

func waitRejoinReason(t *testing.T, capture *resultCaptureDownlink, subscriptionID string,
	reason farmpb.RejoinReason) *farmpb.RejoinRequiredPush {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		capture.mu.Lock()
		items := append([]*logicrpcpb.DownlinkItem(nil), capture.items...)
		capture.mu.Unlock()
		for _, item := range items {
			if item.SubscriptionId != subscriptionID {
				continue
			}
			var frame commonpb.FarmFrame
			if err := proto.Unmarshal(item.Frame, &frame); err != nil {
				continue
			}
			push := frame.GetServerPush()
			if push == nil {
				continue
			}
			if push.PushType != commonpb.PushType_REJOIN_REQUIRED {
				continue
			}
			var required farmpb.RejoinRequiredPush
			if err := proto.Unmarshal(push.Payload, &required); err != nil {
				continue
			}
			if required.Reason == reason {
				return &required
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("missing REJOIN_REQUIRED reason=%s subscription=%s", reason, subscriptionID)
	return nil
}

func TestJoinBarrierEntersInFarmOnlyAfterSnapshotQueueAccepted(t *testing.T) {
	stack := newTestStack(t)
	capture := &resultCaptureDownlink{}
	stack.farm.SetDownlinkEnqueuer(capture)
	trusted := joinTrusted(9, "conn-barrier")
	response, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command:  typedCommand(t, "join-barrier-op", "join-barrier", commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}),
		Trusted:  trusted,
		Dispatch: farmActorDispatch(9),
	})
	if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_ACCEPTED {
		t.Fatalf("join resp=%v err=%v", response, err)
	}
	subscriptionID := joinSubscription(t, response)
	armJoin(t, stack, trusted, response)
	if phase, ok := presencePhase(t, stack, 9, subscriptionID); !ok || phase != farmactor.PresenceJoining {
		t.Fatalf("pre-accept phase=%v ok=%v", phase, ok)
	}
	callback := capture.takeCallback(0)
	if callback == nil {
		t.Fatal("missing snapshot callback")
	}
	callback(&logicrpcpb.DeliverBatchItemResult{
		ItemId: "join-barrier:snapshot", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	})
	waitPresencePhase(t, stack, 9, subscriptionID, farmactor.PresenceInFarm)
}

func TestJoinReplayReusesActiveSubscriptionWithoutRearmingSnapshot(t *testing.T) {
	stack := newTestStack(t)
	capture := &resultCaptureDownlink{}
	stack.farm.SetDownlinkEnqueuer(capture)
	trusted := joinTrusted(9, "conn-replay")
	execute := func(requestID string) *logicrpcpb.ExecuteCommandResponse {
		response, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
			Command:  typedCommand(t, "join-replay-op", requestID, commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}),
			Trusted:  trusted,
			Dispatch: farmActorDispatch(9),
		})
		if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_ACCEPTED {
			t.Fatalf("join resp=%v err=%v", response, err)
		}
		return response
	}
	first := execute("join-replay-first")
	subscriptionID := joinSubscription(t, first)
	armJoin(t, stack, trusted, first)
	callback := capture.takeCallback(0)
	if callback == nil {
		t.Fatal("missing first snapshot callback")
	}
	callback(&logicrpcpb.DeliverBatchItemResult{ItemId: "first", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED})
	waitPresencePhase(t, stack, 9, subscriptionID, farmactor.PresenceInFarm)

	second := execute("join-replay-second")
	if got := joinSubscription(t, second); got != subscriptionID {
		t.Fatalf("replayed subscription=%q, want %q", got, subscriptionID)
	}
	if capture.lenItems() != 1 {
		t.Fatalf("snapshot items=%d, active Join replay must not rearm a second baseline", capture.lenItems())
	}
}

func TestEstablishedPresenceReusesSubscriptionForNewJoinOperationOnSameFence(t *testing.T) {
	stack := newTestStack(t)
	capture := &resultCaptureDownlink{}
	stack.farm.SetDownlinkEnqueuer(capture)
	trusted := joinTrusted(9, "conn-same-farm")
	execute := func(operationID string) *logicrpcpb.ExecuteCommandResponse {
		response, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
			Command:  typedCommand(t, operationID, "request-"+operationID, commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}),
			Trusted:  trusted,
			Dispatch: farmActorDispatch(9),
		})
		if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_ACCEPTED {
			t.Fatalf("join %s response=%v err=%v", operationID, response, err)
		}
		return response
	}

	first := execute("join-same-farm-first")
	subscriptionID := joinSubscription(t, first)
	armJoin(t, stack, trusted, first)
	firstCallback := capture.takeCallback(0)
	if firstCallback == nil {
		t.Fatal("missing first snapshot callback")
	}
	capture.mu.Lock()
	firstItemID := capture.items[0].ItemId
	capture.mu.Unlock()
	firstCallback(&logicrpcpb.DeliverBatchItemResult{
		ItemId: firstItemID, Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	})
	waitPresencePhase(t, stack, 9, subscriptionID, farmactor.PresenceInFarm)

	second := execute("join-same-farm-second")
	if got := joinSubscription(t, second); got != subscriptionID {
		t.Fatalf("same-farm subscription=%q, want existing %q", got, subscriptionID)
	}
	if capture.lenItems() != 1 {
		t.Fatalf("snapshot items=%d, active same-farm Join must reuse the current baseline", capture.lenItems())
	}
	value, err := stack.farm.registry.Submit(context.Background(), 9, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		return actor.PresenceCount(), nil
	})
	if err != nil || value.(int) != 1 {
		t.Fatalf("presence count=%v err=%v, want one reused Presence", value, err)
	}
	if phase, ok := presencePhase(t, stack, 9, subscriptionID); !ok || phase != farmactor.PresenceInFarm {
		t.Fatalf("reused presence phase=%v ok=%v, want InFarm", phase, ok)
	}
}

func TestJoinSnapshotQueueFullRemovesOnlySlowPresence(t *testing.T) {
	stack := newTestStack(t)
	capture := &resultCaptureDownlink{}
	stack.farm.SetDownlinkEnqueuer(capture)

	// Commit business before the slow Join fails so QUEUE_FULL cannot look like a rollback.
	stack.buySeed(t, 9, "buy-before-full")
	planted := stack.executeFarm(t, 9, typedCommand(t, "plant-before-full", "r-plant-before-full", commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: 0, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1}))
	if planted.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("plant=%v", planted)
	}
	var plantResult farmpb.FarmCommandResult
	if err := proto.Unmarshal(planted.ResultPayload, &plantResult); err != nil {
		t.Fatal(err)
	}
	committedVersion := plantResult.Snapshot.FarmVersion

	trusted := joinTrusted(9, "conn-full")
	response, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command:  typedCommand(t, "join-full-op", "join-full", commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}),
		Trusted:  trusted,
		Dispatch: farmActorDispatch(9),
	})
	if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_ACCEPTED {
		t.Fatalf("join resp=%v err=%v", response, err)
	}
	subscriptionID := joinSubscription(t, response)
	armJoin(t, stack, trusted, response)
	callback := capture.takeCallback(0)
	if callback == nil {
		t.Fatal("missing snapshot callback")
	}
	callback(&logicrpcpb.DeliverBatchItemResult{
		ItemId: "join-full:snapshot", Status: logicrpcpb.DownlinkItemStatus_QUEUE_FULL,
	})
	waitPresenceGone(t, stack, 9, subscriptionID)
	// QUEUE_FULL means the exact connection FIFO cannot accept even the
	// Snapshot. Farm cleans only this Presence; Gate/H5 owns connection close
	// and Fresh Join recovery because another frame cannot reliably be queued.

	snapshot, err := stack.farm.registry.Submit(context.Background(), 9, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		return actor.Snapshot(stack.farm.now()), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshot.(*farmpb.FarmSnapshot).FarmVersion; got != committedVersion {
		t.Fatalf("QUEUE_FULL rolled back farm version=%d, want %d", got, committedVersion)
	}
	if snapshot.(*farmpb.FarmSnapshot).Plots[0].CropPhase != farmpb.CropPhase_GROWING {
		t.Fatalf("QUEUE_FULL rolled back planted crop: %v", snapshot)
	}
}

func TestJoinCatchUpDeltaQueueFullLeavesPresenceWithoutBusinessRollback(t *testing.T) {
	stack := newTestStack(t)
	capture := &resultCaptureDownlink{}
	stack.farm.SetDownlinkEnqueuer(capture)
	stack.buySeed(t, 9, "buy-catchup-full")

	trusted := joinTrusted(9, "conn-catchup")
	response, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command:  typedCommand(t, "join-catchup-op", "join-catchup", commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}),
		Trusted:  trusted,
		Dispatch: farmActorDispatch(9),
	})
	if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_ACCEPTED {
		t.Fatalf("join resp=%v err=%v", response, err)
	}
	subscriptionID := joinSubscription(t, response)
	armJoin(t, stack, trusted, response)
	if phase, ok := presencePhase(t, stack, 9, subscriptionID); !ok || phase != farmactor.PresenceJoining {
		t.Fatalf("joining phase=%v ok=%v", phase, ok)
	}

	planted := stack.executeFarm(t, 9, typedCommand(t, "plant-during-join", "r-plant-during-join", commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: 0, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1}))
	if planted.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("plant during join=%v", planted)
	}
	var plantResult farmpb.FarmCommandResult
	if err := proto.Unmarshal(planted.ResultPayload, &plantResult); err != nil {
		t.Fatal(err)
	}

	snapshotCB := capture.takeCallback(0)
	if snapshotCB == nil {
		t.Fatal("missing snapshot callback")
	}
	snapshotCB(&logicrpcpb.DeliverBatchItemResult{
		ItemId: "join-catchup:snapshot", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && capture.lenItems() < 2 {
		time.Sleep(time.Millisecond)
	}
	changeCB := capture.takeCallback(1)
	if changeCB == nil {
		t.Fatalf("missing catch-up delta callback; items=%d", capture.lenItems())
	}
	changeCB(&logicrpcpb.DeliverBatchItemResult{
		ItemId: "join-catchup:change", Status: logicrpcpb.DownlinkItemStatus_QUEUE_FULL,
	})
	waitPresenceGone(t, stack, 9, subscriptionID)
	// The catch-up frame also failed to enter the exact connection FIFO. The
	// Presence is removed without rolling back the committed Plant; Gate/H5
	// recovers by closing/rejoining rather than relying on another queued frame.

	if plantResult.Snapshot.FarmVersion == 0 || plantResult.Snapshot.Plots[0].CropPhase != farmpb.CropPhase_GROWING {
		t.Fatalf("catch-up QUEUE_FULL lost committed plant: %v", plantResult.Snapshot)
	}
}

func TestJoinFlushPreservesContinuousCatchUpVersions(t *testing.T) {
	stack := newTestStack(t)
	capture := &resultCaptureDownlink{}
	stack.farm.SetDownlinkEnqueuer(capture)
	stack.buySeed(t, 9, "buy-hole")
	stack.buySeed(t, 9, "buy-hole-2")

	trusted := joinTrusted(9, "conn-hole")
	response, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command:  typedCommand(t, "join-hole-op", "join-hole", commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}),
		Trusted:  trusted,
		Dispatch: farmActorDispatch(9),
	})
	if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_ACCEPTED {
		t.Fatalf("join resp=%v err=%v", response, err)
	}
	subscriptionID := joinSubscription(t, response)
	armJoin(t, stack, trusted, response)

	// Two publishes while JOINING must be buffered and later flushed without gaps.
	for index, op := range []string{"plant-hole-1", "plant-hole-2"} {
		planted := stack.executeFarm(t, 9, typedCommand(t, op, "r-"+op, commonpb.CommandType_PLANT,
			&publicpb.PlantRequest{PlotId: uint32(index), CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1}))
		if planted.Status != commonpb.CommandStatus_SUCCEEDED {
			t.Fatalf("%s=%v", op, planted)
		}
	}

	snapshotCB := capture.takeCallback(0)
	if snapshotCB == nil {
		t.Fatal("missing snapshot callback")
	}
	snapshotCB(&logicrpcpb.DeliverBatchItemResult{
		ItemId: "join-hole:snapshot", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && capture.lenItems() < 2 {
		time.Sleep(time.Millisecond)
	}
	if capture.lenItems() < 2 {
		t.Fatalf("catch-up items=%d, want snapshot plus one finite batch", capture.lenItems())
	}
	capture.mu.Lock()
	item := capture.items[1]
	callback := capture.callbacks[1]
	capture.mu.Unlock()
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(item.Frame, &frame); err != nil {
		t.Fatal(err)
	}
	push := frame.GetServerPush()
	if push == nil || push.PushType != commonpb.PushType_FARM_DELTA_BATCH {
		t.Fatalf("catch-up push=%v", push)
	}
	var batch farmpb.FarmDeltaBatchPush
	if err := proto.Unmarshal(push.Payload, &batch); err != nil {
		t.Fatal(err)
	}
	if len(batch.Deltas) != 2 || batch.Deltas[0].FarmVersion+1 != batch.Deltas[1].FarmVersion ||
		batch.FromVersion != batch.Deltas[0].FarmVersion || batch.ThroughVersion != batch.Deltas[1].FarmVersion {
		t.Fatalf("catch-up batch=%v, want continuous two-version batch", &batch)
	}
	if callback == nil {
		t.Fatal("missing catch-up callback")
	}
	callback(&logicrpcpb.DeliverBatchItemResult{
		ItemId: item.ItemId, Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	})
	waitPresencePhase(t, stack, 9, subscriptionID, farmactor.PresenceInFarm)
}

func TestStaleGateBootFenceCannotRemoveRejoinedPresence(t *testing.T) {
	stack := newTestStack(t)
	capture := &resultCaptureDownlink{}
	stack.farm.SetDownlinkEnqueuer(capture)

	oldTrusted := &logicrpcpb.TrustedCommandContext{
		Session: &logicrpcpb.SessionPermitSnapshot{
			PlayerId: 9, ConnectionId: "conn-restart", ConnectionGeneration: 1,
			GatewayId: "gate-a", GatewayBootId: "boot-old", ActiveSessionEpoch: 1, SessionRouteRevision: 1,
		},
		JoinAttempt: &logicrpcpb.JoinAdmissionContext{JoinAttemptId: "attempt-old-boot", TargetFarmId: 9},
	}
	first, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command:  typedCommand(t, "join-old-boot-op", "join-old-boot", commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}),
		Trusted:  oldTrusted,
		Dispatch: farmActorDispatch(9),
	})
	if err != nil || first.GetBusiness().Status != commonpb.CommandStatus_ACCEPTED {
		t.Fatalf("first join=%v err=%v", first, err)
	}
	oldSubscriptionID := joinSubscription(t, first)
	armJoin(t, stack, oldTrusted, first)
	capture.takeCallback(0)(&logicrpcpb.DeliverBatchItemResult{
		ItemId: "join-old-boot:snapshot", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	})
	waitPresencePhase(t, stack, 9, oldSubscriptionID, farmactor.PresenceInFarm)

	// Gate restart: new boot_id / generation replaces Presence for the same player.
	newTrusted := &logicrpcpb.TrustedCommandContext{
		Session: &logicrpcpb.SessionPermitSnapshot{
			PlayerId: 9, ConnectionId: "conn-restart", ConnectionGeneration: 2,
			GatewayId: "gate-a", GatewayBootId: "boot-new", ActiveSessionEpoch: 2, SessionRouteRevision: 1,
		},
		JoinAttempt: &logicrpcpb.JoinAdmissionContext{JoinAttemptId: "attempt-new-boot", TargetFarmId: 9},
	}
	second, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command:  typedCommand(t, "join-new-boot-op", "join-new-boot", commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}),
		Trusted:  newTrusted,
		Dispatch: farmActorDispatch(9),
	})
	if err != nil || second.GetBusiness().Status != commonpb.CommandStatus_ACCEPTED {
		t.Fatalf("second join=%v err=%v", second, err)
	}
	newSubscriptionID := joinSubscription(t, second)
	armJoin(t, stack, newTrusted, second)
	// Accept the new snapshot so the replacement Presence reaches IN_FARM.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && capture.lenItems() < 2 {
		time.Sleep(time.Millisecond)
	}
	capture.takeCallback(1)(&logicrpcpb.DeliverBatchItemResult{
		ItemId: "join-new-boot:snapshot", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	})
	waitPresencePhase(t, stack, 9, newSubscriptionID, farmactor.PresenceInFarm)

	_, err = stack.farm.registry.Submit(context.Background(), 9, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		actor.LeaveConnectionFence(session.ActiveRoute{
			PlayerID: 9, SessionEpoch: 1, RouteRevision: 1,
			RouteFence: session.RouteFence{
				GatewayID: "gate-a", GatewayBootID: "boot-old",
				ConnectionID: "conn-restart", ConnectionGeneration: 1,
			},
		})
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if phase, ok := presencePhase(t, stack, 9, newSubscriptionID); !ok || phase != farmactor.PresenceInFarm {
		t.Fatalf("stale Gate boot leave removed rejoined presence: phase=%v ok=%v", phase, ok)
	}
}
