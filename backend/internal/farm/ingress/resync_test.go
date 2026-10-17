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
	"farmgame/internal/friend"
	playeractor "farmgame/internal/player/actor"
	playeringress "farmgame/internal/player/ingress"
	"farmgame/internal/storage/blobstore"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

type failChangeDownlink struct {
	mu            sync.Mutex
	items         []*logicrpcpb.DownlinkItem
	failChanges   bool
	controls      int
	controlErr    error
	controlStatus logicrpcpb.DownlinkItemStatus
}

func (d *failChangeDownlink) Enqueue(item *logicrpcpb.DownlinkItem) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failChanges && strings.Contains(item.ItemId, ":change:") {
		return errors.New("downlink queue full")
	}
	d.items = append(d.items, proto.Clone(item).(*logicrpcpb.DownlinkItem))
	return nil
}

func (d *failChangeDownlink) EnqueueWithResult(item *logicrpcpb.DownlinkItem,
	callback func(*logicrpcpb.DeliverBatchItemResult)) error {
	if err := d.Enqueue(item); err != nil {
		return err
	}
	if callback != nil {
		callback(&logicrpcpb.DeliverBatchItemResult{
			ItemId: item.ItemId, Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
		})
	}
	return nil
}

func (d *failChangeDownlink) ActivateSubscription(_ context.Context,
	req *logicrpcpb.ActivateSubscriptionRequest) (*logicrpcpb.ActivateSubscriptionResponse, error) {
	return &logicrpcpb.ActivateSubscriptionResponse{
		Status:              logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED,
		ReadyThroughVersion: req.ReadyThroughVersion,
	}, nil
}

func (d *failChangeDownlink) findRejoin(subscriptionID string,
	reason farmpb.RejoinReason) *farmpb.RejoinRequiredPush {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, item := range d.items {
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
		if reason == farmpb.RejoinReason_REJOIN_REASON_UNSPECIFIED || required.Reason == reason {
			return &required
		}
	}
	return nil
}

func waitDownlinkRejoin(t *testing.T, find func() *farmpb.RejoinRequiredPush) *farmpb.RejoinRequiredPush {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if got := find(); got != nil {
			return got
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for REJOIN_REQUIRED downlink item")
	return nil
}

func TestJoiningBufferDurationOverflowEmitsRejoinRequired(t *testing.T) {
	stack := newTestStack(t)
	capture := &resultCaptureDownlink{}
	stack.farm.SetDownlinkEnqueuer(capture)
	stack.buySeed(t, 9, "buy-join-duration")

	trusted := joinTrusted(9, "conn-duration")
	response, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command:  typedCommand(t, "join-duration-op", "join-duration", commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}),
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

	*stack.now = stack.now.Add(15 * time.Second)
	planted := stack.executeFarm(t, 9, typedCommand(t, "plant-during-expired-join", "r-plant-expired-join", commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: 0, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1}))
	if planted.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("plant during expired join=%v", planted)
	}
	var plantResult farmpb.FarmCommandResult
	if err := proto.Unmarshal(planted.ResultPayload, &plantResult); err != nil {
		t.Fatal(err)
	}

	waitPresencePhase(t, stack, 9, subscriptionID, farmactor.PresenceResyncRequired)
	required := waitRejoinReason(t, capture, subscriptionID, farmpb.RejoinReason_REJOIN_REASON_STREAM_GAP)
	if required.InvalidFromVersion == nil || *required.InvalidFromVersion != plantResult.Snapshot.FarmVersion {
		t.Fatalf("rejoin invalid_from=%v, want %d", required.InvalidFromVersion, plantResult.Snapshot.FarmVersion)
	}
	callback := capture.takeCallback(1)
	if callback == nil {
		t.Fatal("missing terminal rejoin queue-result callback")
	}
	callback(&logicrpcpb.DeliverBatchItemResult{
		ItemId: subscriptionID + ":rejoin:joining_buffer_duration_exceeded",
		Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
	})
	waitPresenceGone(t, stack, 9, subscriptionID)
}

func (d *failChangeDownlink) EnqueueControlWithResult(item *logicrpcpb.DownlinkItem,
	callback func(*logicrpcpb.DeliverBatchItemResult)) error {
	d.mu.Lock()
	d.controls++
	err := d.controlErr
	status := d.controlStatus
	d.mu.Unlock()
	if err != nil {
		return err
	}
	if err := d.Enqueue(item); err != nil {
		return err
	}
	if callback != nil {
		if status == logicrpcpb.DownlinkItemStatus_DOWNLINK_ITEM_STATUS_UNSPECIFIED {
			status = logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED
		}
		callback(&logicrpcpb.DeliverBatchItemResult{ItemId: item.ItemId, Status: status})
	}
	return nil
}

func TestFarmChangeVersionGapEmitsRejoinRequired(t *testing.T) {
	stack := newTestStack(t)
	capture := &captureDownlink{}
	stack.farm.SetDownlinkEnqueuer(capture)
	stack.buySeed(t, 9, "buy-version-gap")

	trusted := joinTrusted(9, "conn-gap")
	response, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command:  typedCommand(t, "join-gap-op", "join-gap", commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}),
		Trusted:  trusted,
		Dispatch: farmActorDispatch(9),
	})
	if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_ACCEPTED {
		t.Fatalf("join resp=%v err=%v", response, err)
	}
	subscriptionID := joinSubscription(t, response)
	armJoin(t, stack, trusted, response)
	waitPresencePhase(t, stack, 9, subscriptionID, farmactor.PresenceInFarm)

	// Empty farm joins at version 0 (gap check skipped). Rewind to a non-zero
	// stale cursor so the next Plant (version 1) is observed as a hole.
	if _, err := stack.farm.registry.Submit(context.Background(), 9, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		if !actor.ForcePresenceCursorForTest(subscriptionID, 1) {
			t.Fatal("presence cursor rewind failed")
		}
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}

	planted := stack.executeFarm(t, 9, typedCommand(t, "plant-after-gap", "r-plant-after-gap", commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: 0, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1}))
	if planted.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("plant after gap=%v", planted)
	}
	var plantResult farmpb.FarmCommandResult
	if err := proto.Unmarshal(planted.ResultPayload, &plantResult); err != nil {
		t.Fatal(err)
	}

	waitPresenceGone(t, stack, 9, subscriptionID)
	required := waitDownlinkRejoin(t, func() *farmpb.RejoinRequiredPush {
		for _, item := range capture.items {
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
			var got farmpb.RejoinRequiredPush
			if err := proto.Unmarshal(push.Payload, &got); err != nil {
				continue
			}
			if got.Reason == farmpb.RejoinReason_REJOIN_REASON_STREAM_GAP {
				return &got
			}
		}
		return nil
	})
	if required.InvalidFromVersion == nil || *required.InvalidFromVersion != plantResult.Snapshot.FarmVersion {
		t.Fatalf("rejoin invalid_from=%v, want %d", required.InvalidFromVersion, plantResult.Snapshot.FarmVersion)
	}
}

func TestHolderRebuiltEnqueuesRejoinRequiredItem(t *testing.T) {
	store := blobstore.NewMemoryStore()
	playerRegistry := playeractor.NewRegistry(blobstore.NewMemoryStore(), 16)
	farmRegistry := farmactor.NewRegistry(store, 16)
	t.Cleanup(playerRegistry.Stop)
	t.Cleanup(farmRegistry.Stop)
	farmRegistry.SetWriterEpochResolver(func(uint64) (uint64, error) { return 1, nil })

	playerService := playeringress.NewService(playerRegistry)
	for _, playerID := range []uint64{9, 10} {
		if _, err := playerService.EnsureOwnerInitialized(context.Background(), &logicrpcpb.EnsureOwnerInitializedRequest{
			PlayerId: playerID, BootstrapId: "bootstrap",
		}); err != nil {
			t.Fatal(err)
		}
	}
	farmService := NewService(farmRegistry, playerService)
	workflows := workflow.NewMemoryStore()
	farmService.SetWorkflowStore(workflows)
	pairs := activeIngressPairReader(t, 9, 10)
	friendPermits, err := friend.NewPermitService(pairs, []byte("0123456789abcdef0123456789abcdef"), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	farmService.SetFriendPermitAuthority(friendPermits)
	current := time.UnixMilli(1_800_000_000_000)
	farmService.now = func() time.Time { return current }
	farmService.SetMatureAfter(10 * time.Second)
	capture := &captureDownlink{}
	farmService.SetDownlinkEnqueuer(capture)
	stack := testStack{farm: farmService, player: playerService, workflows: workflows, now: &current}

	stack.buySeed(t, 9, "buy-holder-rebuilt")
	trusted := joinTrusted(9, "conn-holder")
	response, err := farmService.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command:  typedCommand(t, "join-holder-op", "join-holder", commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}),
		Trusted:  trusted,
		Dispatch: farmActorDispatch(9),
	})
	if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_ACCEPTED {
		t.Fatalf("join resp=%v err=%v", response, err)
	}
	subscriptionID := joinSubscription(t, response)
	armJoin(t, stack, trusted, response)
	waitPresencePhase(t, stack, 9, subscriptionID, farmactor.PresenceInFarm)

	if err := store.Fence(context.Background(), 9, 2); err != nil {
		t.Fatal(err)
	}
	workflowID, err := workidentity.WorkflowID(9, "PLANT", "plant-after-fence")
	if err != nil {
		t.Fatal(err)
	}
	cropCycleID, err := workidentity.CropCycleID(workflowID)
	if err != nil {
		t.Fatal(err)
	}
	_, plantErr := farmRegistry.Submit(context.Background(), 9, func(ctx context.Context, actor *farmactor.FarmActor) (any, error) {
		return actor.Plant(ctx, "plant-after-fence", cropCycleID, []byte("plant-after-fence"),
			&farmpb.PlantCropRequest{PlotIndex: 0, CropDefinitionId: 1}, 0, 1, current, 10*time.Second)
	})
	if plantErr == nil {
		t.Fatal("fenced plant unexpectedly succeeded")
	}

	required := waitDownlinkRejoin(t, func() *farmpb.RejoinRequiredPush {
		for _, item := range capture.items {
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
			var got farmpb.RejoinRequiredPush
			if err := proto.Unmarshal(push.Payload, &got); err != nil {
				continue
			}
			if got.Reason == farmpb.RejoinReason_REJOIN_REASON_HOLDER_REBUILT {
				return &got
			}
		}
		return nil
	})
	if required.Reason != farmpb.RejoinReason_REJOIN_REASON_HOLDER_REBUILT {
		t.Fatalf("rejoin=%v", required)
	}
}

func TestEmitChangeQueueFullEmitsRejoinAndClearsPresence(t *testing.T) {
	stack := newTestStack(t)
	capture := &failChangeDownlink{}
	stack.farm.SetDownlinkEnqueuer(capture)
	stack.buySeed(t, 9, "buy-change-full")

	trusted := joinTrusted(9, "conn-change-full")
	response, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command:  typedCommand(t, "join-change-full-op", "join-change-full", commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}),
		Trusted:  trusted,
		Dispatch: farmActorDispatch(9),
	})
	if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_ACCEPTED {
		t.Fatalf("join resp=%v err=%v", response, err)
	}
	subscriptionID := joinSubscription(t, response)
	armJoin(t, stack, trusted, response)
	waitPresencePhase(t, stack, 9, subscriptionID, farmactor.PresenceInFarm)

	capture.mu.Lock()
	capture.failChanges = true
	capture.mu.Unlock()

	planted := stack.executeFarm(t, 9, typedCommand(t, "plant-change-full", "r-plant-change-full", commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: 0, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1}))
	if planted.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("plant on full downlink=%v", planted)
	}
	var plantResult farmpb.FarmCommandResult
	if err := proto.Unmarshal(planted.ResultPayload, &plantResult); err != nil {
		t.Fatal(err)
	}

	waitPresenceGone(t, stack, 9, subscriptionID)
	required := waitDownlinkRejoin(t, func() *farmpb.RejoinRequiredPush {
		return capture.findRejoin(subscriptionID, farmpb.RejoinReason_REJOIN_REASON_DELIVERY_FAILED)
	})
	if required.InvalidFromVersion == nil || *required.InvalidFromVersion != plantResult.Snapshot.FarmVersion {
		t.Fatalf("rejoin invalid_from=%v, want %d", required.InvalidFromVersion, plantResult.Snapshot.FarmVersion)
	}
	if plantResult.Snapshot.Plots[0].CropPhase != farmpb.CropPhase_GROWING {
		t.Fatalf("QUEUE_FULL rolled back planted crop: %v", plantResult.Snapshot)
	}
	capture.mu.Lock()
	controls := capture.controls
	capture.mu.Unlock()
	if controls != 1 {
		t.Fatalf("terminal control enqueues=%d, want 1 reserved control enqueue", controls)
	}
}

func TestTerminalDeliveryFailureStillAllowsFreshJoin(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status logicrpcpb.DownlinkItemStatus
	}{
		{name: "reserved control queue full", err: errors.New("control queue full")},
		{name: "gate rpc unavailable", status: logicrpcpb.DownlinkItemStatus_UNAVAILABLE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stack := newTestStack(t)
			capture := &failChangeDownlink{controlErr: tt.err, controlStatus: tt.status}
			stack.farm.SetDownlinkEnqueuer(capture)
			stack.buySeed(t, 9, "buy-terminal-failure")

			trusted := joinTrusted(9, "conn-terminal-failure")
			response, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
				Command:  typedCommand(t, "join-terminal-failure-op", "join-terminal-failure", commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}),
				Trusted:  trusted,
				Dispatch: farmActorDispatch(9),
			})
			if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_ACCEPTED {
				t.Fatalf("join resp=%v err=%v", response, err)
			}
			subscriptionID := joinSubscription(t, response)
			armJoin(t, stack, trusted, response)
			waitPresencePhase(t, stack, 9, subscriptionID, farmactor.PresenceInFarm)

			capture.mu.Lock()
			capture.failChanges = true
			capture.mu.Unlock()
			planted := stack.executeFarm(t, 9, typedCommand(t, "plant-terminal-failure", "r-plant-terminal-failure", commonpb.CommandType_PLANT,
				&publicpb.PlantRequest{PlotId: 0, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1}))
			if planted.Status != commonpb.CommandStatus_SUCCEEDED {
				t.Fatalf("plant status=%v", planted)
			}
			// The undeliverable terminal must not strand this session behind the
			// fallback TTL. Exact cleanup makes a later Fresh Join admissible.
			waitPresenceGone(t, stack, 9, subscriptionID)
		})
	}
}
