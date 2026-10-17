package actor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	"farmgame/internal/actorrouting"
	"farmgame/internal/gateway/session"
	"farmgame/internal/logic/actor/activation"
	actorruntime "farmgame/internal/logic/actor/runtime"
	"farmgame/internal/storage/blobstore"
	"farmgame/internal/workidentity"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type countingStore struct {
	blobstore.Store
	casCalls    atomic.Int32
	insertCalls atomic.Int32
}

func testPlantCropCycleID(ownerID uint64, operationID string) string {
	workflowID, err := workidentity.WorkflowID(ownerID, "PLANT", operationID)
	if err != nil {
		panic(err)
	}
	cropCycleID, err := workidentity.CropCycleID(workflowID)
	if err != nil {
		panic(err)
	}
	return cropCycleID
}

func (s *countingStore) CAS(ctx context.Context, id, version uint64, data []byte) error {
	s.casCalls.Add(1)
	return s.Store.CAS(ctx, id, version, data)
}

func (s *countingStore) Insert(ctx context.Context, id uint64, data []byte) error {
	s.insertCalls.Add(1)
	return s.Store.Insert(ctx, id, data)
}

func TestFarmReceiptBindsOperationToPayload(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	ctx := context.Background()
	now := time.UnixMilli(1_800_000_000_000)
	firstHash := []byte("first-payload")

	value, err := registry.Submit(ctx, 9, func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.Plant(ctx, "plant-1", testPlantCropCycleID(9, "plant-1"), firstHash, &farmpb.PlantCropRequest{
			PlotIndex: 0, CropDefinitionId: 1,
		}, 0, 1, now, time.Second)
	})
	if err != nil || value.(*ActionResult).Status != commonpb.CommandStatus_PROCESSING {
		t.Fatalf("first plant=%v err=%v", value, err)
	}

	_, err = registry.Submit(ctx, 9, func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.Plant(ctx, "plant-1", testPlantCropCycleID(9, "plant-1"), []byte("different-payload"), &farmpb.PlantCropRequest{
			PlotIndex: 1, CropDefinitionId: 1,
		}, 0, 1, now, time.Second)
	})
	var domain *Error
	if !errors.As(err, &domain) || domain.Code != commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT {
		t.Fatalf("different payload was not rejected: %v", err)
	}

	value, err = registry.Submit(ctx, 9, func(ctx context.Context, actor *FarmActor) (any, error) {
		replayed, replayErr := actor.Plant(ctx, "plant-1", testPlantCropCycleID(9, "plant-1"), firstHash, &farmpb.PlantCropRequest{
			PlotIndex: 0, CropDefinitionId: 1,
		}, 0, 1, now, time.Second)
		if replayErr != nil {
			return nil, replayErr
		}
		snapshot := actor.Snapshot(now)
		if snapshot.FarmVersion != 1 || snapshot.Plots[1].CropCycleId != "" {
			t.Fatalf("conflicting replay mutated farm: %v", snapshot)
		}
		return replayed, nil
	})
	if err != nil || !value.(*ActionResult).Replayed {
		t.Fatalf("same payload did not replay: %v err=%v", value, err)
	}
}

func TestPlantUsesCropDefinitionRules(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	now := time.UnixMilli(1_800_000_000_000)
	value, err := registry.Submit(context.Background(), 9, func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.Plant(ctx, "plant-tomato", testPlantCropCycleID(9, "plant-tomato"), []byte("tomato"), &farmpb.PlantCropRequest{
			PlotIndex: 0, CropDefinitionId: 2,
		}, 0, 1, now, 0)
	})
	if err != nil {
		t.Fatal(err)
	}
	action := value.(*ActionResult)
	cycle := action.Result.Snapshot.Plots[0]
	if cycle.CropDefinitionId != 2 || cycle.MatureAtMs-now.UnixMilli() != 15_000 || cycle.HarvestableCount != 8 {
		t.Fatalf("tomato rules not applied: %+v", cycle)
	}
}

func TestPresenceReplacementUsesCompleteSessionFence(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	ctx := context.Background()
	now := time.UnixMilli(1_800_000_000_000)
	firstReq := JoinRequest{
		SubscriptionID: "sub-old", ViewerPlayerID: 10001, SessionEpoch: 7,
		GatewayID: "gate-a", GatewayBootID: "boot-a", ConnectionID: "conn-a",
		TransportGeneration: 1, RouteRevision: 3,
	}
	var oldJoin, newJoin *JoinResult
	_, err := registry.Submit(ctx, 9, func(_ context.Context, actor *FarmActor) (any, error) {
		var joinErr error
		oldJoin, joinErr = actor.Join(firstReq, now)
		if joinErr != nil {
			return nil, joinErr
		}
		newJoin, joinErr = actor.Join(JoinRequest{
			SubscriptionID: "sub-new", ViewerPlayerID: 10001, SessionEpoch: 8,
			GatewayID: "gate-b", GatewayBootID: "boot-b", ConnectionID: "conn-b",
			TransportGeneration: 2, RouteRevision: 4,
		}, now.Add(time.Millisecond))
		if joinErr != nil {
			return nil, joinErr
		}
		if len(actor.presence) != 1 || actor.presence["sub-new"] == nil {
			t.Fatalf("replacement presence=%v", actor.presence)
		}
		if actor.LeaveJoin(oldJoin) {
			t.Fatal("delayed old callback removed the replacement")
		}
		if actor.presence["sub-new"] == nil {
			t.Fatal("replacement disappeared after stale callback")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if oldJoin.PresenceEpoch == newJoin.PresenceEpoch {
		t.Fatal("replacement reused presence_epoch")
	}

	_, err = registry.Submit(ctx, 9, func(_ context.Context, actor *FarmActor) (any, error) {
		_, conflictErr := actor.Join(JoinRequest{
			SubscriptionID: "sub-conflict", ViewerPlayerID: 10001, SessionEpoch: 8,
			GatewayID: "gate-c", GatewayBootID: "boot-c", ConnectionID: "conn-c",
			TransportGeneration: 99, RouteRevision: 99,
		}, now.Add(2*time.Millisecond))
		return nil, conflictErr
	})
	if !errors.Is(err, ErrJoinStale) {
		t.Fatalf("same-session different fence err=%v", err)
	}

	_, err = registry.Submit(ctx, 9, func(_ context.Context, actor *FarmActor) (any, error) {
		_, staleErr := actor.Join(firstReq, now.Add(3*time.Millisecond))
		return nil, staleErr
	})
	if !errors.Is(err, ErrJoinStale) {
		t.Fatalf("old session rejoin err=%v", err)
	}
}

func TestForceFreshJoinDoesNotReuseStillCleaningUpPresence(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	ctx := context.Background()
	now := time.UnixMilli(1_800_000_000_000)
	firstReq := JoinRequest{
		OperationID: "join-old", JoinAttemptID: "attempt-old", ViewerPlayerID: 10001,
		SessionEpoch: 7, GatewayID: "gate-a", GatewayBootID: "boot-a", ConnectionID: "conn-a",
		TransportGeneration: 1, RouteRevision: 3,
	}
	secondReq := firstReq
	secondReq.OperationID = "join-new"
	secondReq.JoinAttemptID = "attempt-new"
	secondReq.ForceFresh = true
	secondReq.SubscriptionID = ""

	var first, second *JoinResult
	_, err := registry.Submit(ctx, 9, func(_ context.Context, actor *FarmActor) (any, error) {
		var joinErr error
		first, joinErr = actor.Join(firstReq, now)
		if joinErr != nil {
			return nil, joinErr
		}
		// Model Gate's break-before-make local revoke while the remote cleanup
		// callback is still in flight: the old FarmActor Presence is still
		// visible and already IN_FARM.
		actor.presence[first.SubscriptionID].Phase = PresenceInFarm
		second, joinErr = actor.CreateOrReuseJoining(secondReq, now.Add(time.Millisecond))
		return second, joinErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if first == nil || second == nil || first.SubscriptionID == second.SubscriptionID || second.JoinAttemptID != secondReq.JoinAttemptID {
		t.Fatalf("force-fresh join reused old metadata: first=%+v second=%+v", first, second)
	}
	if value, err := registry.Submit(ctx, 9, func(_ context.Context, actor *FarmActor) (any, error) {
		_, present := actor.presence[first.SubscriptionID]
		return present, nil
	}); err != nil || value.(bool) {
		t.Fatalf("old presence still present=%v err=%v", value, err)
	}
}

func TestCancelJoinUsesOperationAndFullFence(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	ctx := context.Background()
	now := time.UnixMilli(1_800_000_000_000)
	firstFence := LeaveRequest{
		ViewerPlayerID: 10001, SessionEpoch: 7, GatewayID: "gate-a", GatewayBootID: "boot-a",
		ConnectionID: "conn-a", TransportGeneration: 1, RouteRevision: 3,
	}
	var firstSubscription string
	_, err := registry.Submit(ctx, 9, func(_ context.Context, actor *FarmActor) (any, error) {
		joined, joinErr := actor.Join(JoinRequest{
			OperationID: "join-cancel", ViewerPlayerID: firstFence.ViewerPlayerID,
			SessionEpoch: firstFence.SessionEpoch, GatewayID: firstFence.GatewayID,
			GatewayBootID: firstFence.GatewayBootID, ConnectionID: firstFence.ConnectionID,
			TransportGeneration: firstFence.TransportGeneration, RouteRevision: firstFence.RouteRevision,
		}, now)
		if joined != nil {
			firstSubscription = joined.SubscriptionID
		}
		return joined, joinErr
	})
	if err != nil || firstSubscription == "" {
		t.Fatalf("join subscription=%q err=%v", firstSubscription, err)
	}
	canceled, err := registry.CancelJoin(ctx, 9, CancelJoinRequest{OperationID: "join-cancel", Fence: firstFence})
	if err != nil || !canceled {
		t.Fatalf("cancel by operation canceled=%v err=%v", canceled, err)
	}

	// Current reconnect policy may reuse the same Join operation_id. It can
	// recreate the idempotent subscription under a newer complete fence.
	newFence := firstFence
	newFence.SessionEpoch = 8
	newFence.ConnectionID = "conn-b"
	newFence.TransportGeneration = 2
	newFence.RouteRevision = 4
	_, err = registry.Submit(ctx, 9, func(_ context.Context, actor *FarmActor) (any, error) {
		return actor.Join(JoinRequest{
			OperationID: "join-cancel", ViewerPlayerID: newFence.ViewerPlayerID,
			SessionEpoch: newFence.SessionEpoch, GatewayID: newFence.GatewayID,
			GatewayBootID: newFence.GatewayBootID, ConnectionID: newFence.ConnectionID,
			TransportGeneration: newFence.TransportGeneration, RouteRevision: newFence.RouteRevision,
		}, now.Add(time.Second))
	})
	if err != nil {
		t.Fatal(err)
	}
	// A delayed CancelJoin carrying the old fence must not remove the new one.
	canceled, err = registry.CancelJoin(ctx, 9, CancelJoinRequest{
		OperationID: "join-cancel", SubscriptionID: firstSubscription, Fence: firstFence,
	})
	if err != nil || canceled {
		t.Fatalf("stale cancel canceled=%v err=%v", canceled, err)
	}
	value, err := registry.Submit(ctx, 9, func(_ context.Context, actor *FarmActor) (any, error) {
		_, ok := actor.PresencePhase(firstSubscription)
		return ok, nil
	})
	if err != nil || !value.(bool) {
		t.Fatalf("new presence survived=%v err=%v", value, err)
	}
}

func TestCleanupFarmSubscriptionIsExactAndDoesNotScan(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	ctx := context.Background()
	now := time.UnixMilli(1_800_000_000_000)

	joins := make(map[uint64]*JoinResult)
	for _, farmID := range []uint64{71, 72} {
		value, err := registry.Submit(ctx, farmID, func(_ context.Context, actor *FarmActor) (any, error) {
			return actor.Join(JoinRequest{
				OperationID: "join-exact", ViewerPlayerID: 10001, SessionEpoch: 7,
				GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "conn",
				TransportGeneration: 3, RouteRevision: 9,
			}, now)
		})
		if err != nil {
			t.Fatal(err)
		}
		joins[farmID] = value.(*JoinResult)
	}
	first := joins[71]
	removed, err := registry.CleanupFarmSubscription(ctx, 71, LeaveRequest{
		SubscriptionID: first.SubscriptionID, PresenceEpoch: first.PresenceEpoch,
		ViewerPlayerID: first.ViewerPlayerID, SessionEpoch: first.SessionEpoch,
		GatewayID: first.GatewayID, GatewayBootID: first.GatewayBootID,
		ConnectionID: first.ConnectionID, TransportGeneration: first.Generation,
		RouteRevision: first.RouteRevision,
	})
	if err != nil || !removed {
		t.Fatalf("exact cleanup removed=%v err=%v", removed, err)
	}
	if scans := registry.CleanupActorScanTotal(); scans != 0 {
		t.Fatalf("exact cleanup used legacy actor scan: %d", scans)
	}
	for farmID, want := range map[uint64]int{71: 0, 72: 1} {
		value, submitErr := registry.Submit(ctx, farmID, func(_ context.Context, actor *FarmActor) (any, error) {
			return actor.PresenceCount(), nil
		})
		if submitErr != nil || value.(int) != want {
			t.Fatalf("farm=%d presence=%v want=%d err=%v", farmID, value, want, submitErr)
		}
	}

	// Replaying the old cleanup after a newer session reused the subscription
	// ID must not remove the new Presence.
	second := joins[72]
	_, err = registry.Submit(ctx, 72, func(_ context.Context, actor *FarmActor) (any, error) {
		actor.LeaveJoin(second)
		return actor.Join(JoinRequest{
			OperationID: "join-new", SubscriptionID: second.SubscriptionID,
			ViewerPlayerID: second.ViewerPlayerID, SessionEpoch: second.SessionEpoch + 1,
			GatewayID: second.GatewayID, GatewayBootID: second.GatewayBootID,
			ConnectionID: second.ConnectionID, TransportGeneration: second.Generation + 1,
			RouteRevision: second.RouteRevision + 1,
		}, now.Add(time.Second))
	})
	if err != nil {
		t.Fatal(err)
	}
	removed, err = registry.CleanupFarmSubscription(ctx, 72, LeaveRequest{
		SubscriptionID: second.SubscriptionID, PresenceEpoch: second.PresenceEpoch,
		ViewerPlayerID: second.ViewerPlayerID, SessionEpoch: second.SessionEpoch,
		GatewayID: second.GatewayID, GatewayBootID: second.GatewayBootID,
		ConnectionID: second.ConnectionID, TransportGeneration: second.Generation,
		RouteRevision: second.RouteRevision,
	})
	if err != nil || removed {
		t.Fatalf("stale cleanup removed newer Presence=%v err=%v", removed, err)
	}
	value, err := registry.Submit(ctx, 72, func(_ context.Context, actor *FarmActor) (any, error) {
		return actor.PresenceCount(), nil
	})
	if err != nil || value.(int) != 1 {
		t.Fatalf("new Presence count=%v err=%v", value, err)
	}
}

func TestCleanupFarmSubscriptionDoesNotActivateAbsentActor(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	removed, err := registry.CleanupFarmSubscription(context.Background(), 999, LeaveRequest{
		SubscriptionID: "missing", PresenceEpoch: 1,
	})
	if err != nil || removed {
		t.Fatalf("absent cleanup removed=%v err=%v", removed, err)
	}
	if state := registry.Activation().StateOf(999); state != activation.Absent {
		t.Fatalf("absent cleanup activated actor: %s", state)
	}
}

func TestJoinOperationRetryReusesServerSubscriptionID(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	now := time.UnixMilli(1_800_000_000_000)
	var first, second *JoinResult
	_, err := registry.Submit(context.Background(), 9, func(_ context.Context, actor *FarmActor) (any, error) {
		var err error
		first, err = actor.Join(JoinRequest{
			OperationID: "join-operation", ViewerPlayerID: 10001,
			SessionEpoch: 7, GatewayID: "gate", GatewayBootID: "boot",
			ConnectionID: "conn", TransportGeneration: 1, RouteRevision: 1,
		}, now)
		if err != nil {
			return nil, err
		}
		second, err = actor.Join(JoinRequest{
			OperationID: "join-operation", ViewerPlayerID: 10001,
			SessionEpoch: 7, GatewayID: "gate", GatewayBootID: "boot",
			ConnectionID: "conn", TransportGeneration: 1, RouteRevision: 1,
		}, now.Add(time.Millisecond))
		if err != nil {
			return nil, err
		}
		if len(actor.presence) != 1 {
			return nil, fmt.Errorf("presence count=%d, want 1", len(actor.presence))
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if first == nil || second == nil || first.SubscriptionID == "" || first.SubscriptionID != second.SubscriptionID {
		var firstID, secondID string
		if first != nil {
			firstID = first.SubscriptionID
		}
		if second != nil {
			secondID = second.SubscriptionID
		}
		t.Fatalf("join subscriptions first=%q second=%q", firstID, secondID)
	}
	if !second.Replayed {
		t.Fatal("same operation retry was not replayed")
	}
}

func TestJoinOperationRejectsDifferentViewerIntent(t *testing.T) {
	actor := &FarmActor{
		farmID:           9,
		record:           InitialRecord(9),
		presence:         make(map[string]*Presence),
		presenceByPlayer: make(map[uint64]string),
		joinIdempotency:  make(map[string]joinIdempotencyEntry),
		maxSubscriptions: 2,
	}
	now := time.UnixMilli(1_800_000_000_000)
	if _, err := actor.Join(JoinRequest{
		OperationID: "join-intent", ViewerPlayerID: 1, SessionEpoch: 1,
		GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "conn-1",
		TransportGeneration: 1, RouteRevision: 1,
	}, now); err != nil {
		t.Fatal(err)
	}
	_, err := actor.Join(JoinRequest{
		OperationID: "join-intent", ViewerPlayerID: 2, SessionEpoch: 1,
		GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "conn-2",
		TransportGeneration: 1, RouteRevision: 1,
	}, now)
	var domain *Error
	if !errors.As(err, &domain) || domain.Code != commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT {
		t.Fatalf("different Join intent error=%v", err)
	}
}

func TestJoinIdempotencyIsRuntimeBoundedAndExpires(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	store := &countingStore{Store: blobstore.NewMemoryStore()}
	registry := NewRegistry(store, 8)
	defer registry.Stop()
	var firstSubscription string
	_, err := registry.Submit(context.Background(), 9, func(_ context.Context, actor *FarmActor) (any, error) {
		actor.maxSubscriptions = maxJoinIdempotencyEntries + 10
		for i := 0; i <= maxJoinIdempotencyEntries; i++ {
			join, joinErr := actor.Join(JoinRequest{
				OperationID: fmt.Sprintf("join-%d", i), ViewerPlayerID: uint64(i + 1),
				SessionEpoch: 1, GatewayID: "gate", GatewayBootID: "boot",
				ConnectionID: fmt.Sprintf("conn-%d", i), TransportGeneration: 1, RouteRevision: 1,
			}, now)
			if joinErr != nil {
				return nil, joinErr
			}
			if i == 0 {
				firstSubscription = join.SubscriptionID
			}
		}
		if len(actor.joinIdempotency) != maxJoinIdempotencyEntries {
			return nil, fmt.Errorf("idempotency entries=%d, want %d", len(actor.joinIdempotency), maxJoinIdempotencyEntries)
		}
		retry, retryErr := actor.Join(JoinRequest{
			OperationID: "join-0", ViewerPlayerID: 1,
			SessionEpoch: 2, GatewayID: "gate", GatewayBootID: "boot",
			ConnectionID: "conn-retry", TransportGeneration: 2, RouteRevision: 2,
		}, now)
		if retryErr != nil {
			return nil, retryErr
		}
		if retry.SubscriptionID == firstSubscription {
			return nil, errors.New("evicted Join mapping was not replaced")
		}
		expired, expiredErr := actor.Join(JoinRequest{
			OperationID: "join-expiring", ViewerPlayerID: 5000,
			SessionEpoch: 1, GatewayID: "gate", GatewayBootID: "boot",
			ConnectionID: "conn-expiring", TransportGeneration: 1, RouteRevision: 1,
		}, now)
		if expiredErr != nil {
			return nil, expiredErr
		}
		retryExpired, retryExpiredErr := actor.Join(JoinRequest{
			OperationID: "join-expiring", ViewerPlayerID: 5000,
			SessionEpoch: 2, GatewayID: "gate", GatewayBootID: "boot",
			ConnectionID: "conn-expiring-2", TransportGeneration: 2, RouteRevision: 2,
		}, now.Add(joinIdempotencyTTL))
		if retryExpiredErr != nil {
			return nil, retryExpiredErr
		}
		if retryExpired.SubscriptionID == expired.SubscriptionID {
			return nil, errors.New("expired Join mapping was reused")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestJoinIdempotencyDoesNotWriteFarmRecordAndResetsOnReactivation(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	store := &countingStore{Store: blobstore.NewMemoryStore()}
	registry := NewRegistry(store, 8)
	defer registry.Stop()
	var actor *FarmActor
	var firstSubscription string
	_, err := registry.Submit(context.Background(), 9, func(_ context.Context, current *FarmActor) (any, error) {
		actor = current
		join, joinErr := current.Join(JoinRequest{
			OperationID: "join-restart", ViewerPlayerID: 1,
			SessionEpoch: 1, GatewayID: "gate", GatewayBootID: "boot",
			ConnectionID: "conn", TransportGeneration: 1, RouteRevision: 1,
		}, now)
		if joinErr != nil {
			return nil, joinErr
		}
		firstSubscription = join.SubscriptionID
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	store.casCalls.Store(0)
	store.insertCalls.Store(0)
	registry.removeActor(9, actor)
	_, err = registry.Submit(context.Background(), 9, func(_ context.Context, current *FarmActor) (any, error) {
		join, joinErr := current.Join(JoinRequest{
			OperationID: "join-restart", ViewerPlayerID: 1,
			SessionEpoch: 2, GatewayID: "gate", GatewayBootID: "boot",
			ConnectionID: "conn-new", TransportGeneration: 2, RouteRevision: 2,
		}, now.Add(time.Second))
		if joinErr != nil {
			return nil, joinErr
		}
		if join.SubscriptionID == firstSubscription {
			return nil, errors.New("reactivated actor reused runtime-only Join mapping")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if store.casCalls.Load() != 0 || store.insertCalls.Load() != 0 {
		t.Fatalf("Join caused storage writes: CAS=%d Insert=%d", store.casCalls.Load(), store.insertCalls.Load())
	}
}

func TestPresenceCapacityIsPerDistinctViewer(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	_, err := registry.Submit(context.Background(), 10, func(_ context.Context, actor *FarmActor) (any, error) {
		actor.maxSubscriptions = 2
		for player := uint64(1); player <= 2; player++ {
			if _, joinErr := actor.Join(JoinRequest{
				SubscriptionID: fmt.Sprintf("sub-%d", player), ViewerPlayerID: player,
				SessionEpoch: 1, GatewayID: "gate", GatewayBootID: "boot",
				ConnectionID: fmt.Sprintf("conn-%d", player), TransportGeneration: 1,
				RouteRevision: 1,
			}, time.Now()); joinErr != nil {
				return nil, joinErr
			}
		}
		_, joinErr := actor.Join(JoinRequest{
			SubscriptionID: "sub-3", ViewerPlayerID: 3, SessionEpoch: 1,
			GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "conn-3",
			TransportGeneration: 1, RouteRevision: 1,
		}, time.Now())
		return nil, joinErr
	})
	var domain *Error
	if !errors.As(err, &domain) || domain.Code != commonpb.ErrorCode_ERR_FARM_FULL {
		t.Fatalf("capacity err=%v", err)
	}
}

func TestJoiningOverflowRetainsOnlyTerminalFence(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	now := time.UnixMilli(1_800_000_000_000)
	_, err := registry.Submit(context.Background(), 11, func(_ context.Context, actor *FarmActor) (any, error) {
		actor.maxBufferItems = 1
		actor.maxBufferBytes = 1 << 20
		if _, joinErr := actor.Join(JoinRequest{
			SubscriptionID: "slow", ViewerPlayerID: 1, SessionEpoch: 1,
			GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "conn",
			TransportGeneration: 1, RouteRevision: 1,
		}, now); joinErr != nil {
			return nil, joinErr
		}
		plot := actor.record.Core.Plots[0]
		if _, _, _, publishErr := actor.publish(plot, now); publishErr != nil {
			return nil, publishErr
		}
		if _, _, _, publishErr := actor.publish(plot, now.Add(time.Millisecond)); publishErr != nil {
			return nil, publishErr
		}
		if presence := actor.presence["slow"]; presence == nil || presence.Phase != PresenceResyncRequired {
			t.Fatalf("overflowing join presence=%+v, want terminal fence", presence)
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPublishResyncNoticeCoversJoiningLimitsAndVersionGaps(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*FarmActor, time.Time)
		want  string
	}{
		{
			name: "items",
			setup: func(actor *FarmActor, now time.Time) {
				actor.maxBufferItems = 0
				actor.record.Core.FarmVersion = 1
				_, _ = actor.Join(JoinRequest{
					SubscriptionID: "sub-items", ViewerPlayerID: 1, SessionEpoch: 1,
					GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "conn",
					TransportGeneration: 1, RouteRevision: 1,
				}, now)
			},
			want: "joining_buffer_items_exceeded",
		},
		{
			name: "bytes",
			setup: func(actor *FarmActor, now time.Time) {
				actor.maxBufferBytes = 1
				actor.record.Core.FarmVersion = 1
				_, _ = actor.Join(JoinRequest{
					SubscriptionID: "sub-bytes", ViewerPlayerID: 1, SessionEpoch: 1,
					GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "conn",
					TransportGeneration: 1, RouteRevision: 1,
				}, now)
			},
			want: "joining_buffer_bytes_exceeded",
		},
		{
			name: "duration",
			setup: func(actor *FarmActor, now time.Time) {
				actor.maxJoinDuration = time.Second
				actor.record.Core.FarmVersion = 1
				_, _ = actor.Join(JoinRequest{
					SubscriptionID: "sub-duration", ViewerPlayerID: 1, SessionEpoch: 1,
					GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "conn",
					TransportGeneration: 1, RouteRevision: 1,
				}, now.Add(-2*time.Second))
			},
			want: "joining_buffer_duration_exceeded",
		},
		{
			name: "version gap",
			setup: func(actor *FarmActor, now time.Time) {
				actor.record.Core.FarmVersion = 3
				_, _ = actor.Join(JoinRequest{
					SubscriptionID: "sub-gap", ViewerPlayerID: 1, SessionEpoch: 1,
					GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "conn",
					TransportGeneration: 1, RouteRevision: 1,
				}, now)
				actor.presence["sub-gap"].LastPushedVersion = 1
			},
			want: "farm_change_version_gap",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := time.UnixMilli(1_800_000_000_000)
			actor := &FarmActor{
				farmID: 11, record: InitialRecord(11),
				presence:         make(map[string]*Presence),
				presenceByPlayer: make(map[uint64]string),
				maxBufferItems:   64, maxBufferBytes: defaultMaxJoinBufferBytes,
			}
			tt.setup(actor, now)
			_, _, notices, err := actor.publish(actor.record.Core.Plots[0], now)
			if err != nil || len(notices) != 1 || notices[0].Reason != tt.want ||
				notices[0].Presence.SubscriptionID == "" {
				t.Fatalf("publish err=%v notices=%+v, want %s", err, notices, tt.want)
			}
		})
	}
}

func TestReceiptCapacityRejectsBeforeFarmMutation(t *testing.T) {
	store := blobstore.NewMemoryStore()
	registry := NewRegistry(store, 8)
	defer registry.Stop()
	ctx := context.Background()
	now := time.UnixMilli(1_800_000_000_000)

	_, err := registry.Submit(ctx, 10, func(ctx context.Context, actor *FarmActor) (any, error) {
		for i := 0; i < maxRecentReceipts; i++ {
			actor.record.RecentOperationReceipts = append(actor.record.RecentOperationReceipts, &farmpb.FarmOperationReceipt{
				OperationId: "live", CommittedAtMs: now.UnixMilli(),
			})
		}
		before := actor.Snapshot(now)
		_, plantErr := actor.Plant(ctx, "plant-over-capacity", testPlantCropCycleID(10, "plant-over-capacity"), []byte("payload"), &farmpb.PlantCropRequest{
			PlotIndex: 0, CropDefinitionId: 1,
		}, 0, 1, now, time.Second)
		after := actor.Snapshot(now)
		if after.FarmVersion != before.FarmVersion || after.Plots[0].CropCycleId != "" {
			t.Fatal("capacity rejection mutated farm")
		}
		return nil, plantErr
	})
	var domain *Error
	if !errors.As(err, &domain) || domain.Code != commonpb.ErrorCode_ERR_RATE_LIMITED {
		t.Fatalf("full live receipt window err=%v", err)
	}
}

func TestFarmReceiptStoresOutcomeWithoutRepeatingSnapshot(t *testing.T) {
	result := &farmpb.FarmCommandResult{
		Snapshot:         &farmpb.FarmSnapshot{OwnerId: 9, FarmVersion: 8},
		GrantedItemId:    2001,
		GrantedItemCount: 3,
		PrivateState:     []byte("private"),
		SettlementId:     "settlement-1",
	}
	compact := compactOperationResult(result)
	if compact.Snapshot != nil {
		t.Fatal("receipt must not duplicate recoverable farm snapshot")
	}
	if compact.GrantedItemId != result.GrantedItemId ||
		compact.GrantedItemCount != result.GrantedItemCount ||
		compact.SettlementId != result.SettlementId ||
		string(compact.PrivateState) != string(result.PrivateState) {
		t.Fatalf("compact outcome lost durable fields: %v", compact)
	}
	compact.PrivateState[0] = 'X'
	if string(compact.PrivateState) == string(result.PrivateState) {
		t.Fatal("compact outcome aliases caller-owned private state")
	}
}

func TestMatureCropRejectsOwnerWaterAndFriendCare(t *testing.T) {
	store := blobstore.NewMemoryStore()
	registry := NewRegistry(store, 8)
	defer registry.Stop()
	ctx := context.Background()
	plantedAt := time.UnixMilli(1_800_000_000_000)

	_, err := registry.Submit(ctx, 11, func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.Plant(ctx, "plant-before-mature-water", testPlantCropCycleID(11, "plant-before-mature-water"), []byte("plant"), &farmpb.PlantCropRequest{
			PlotIndex: 0, CropDefinitionId: 1,
		}, 0, 0, plantedAt, time.Second)
	})
	if err != nil {
		t.Fatal(err)
	}

	maturedAt := plantedAt.Add(time.Second)
	_, err = registry.Submit(ctx, 11, func(ctx context.Context, actor *FarmActor) (any, error) {
		before := actor.Snapshot(maturedAt)
		_, waterErr := actor.Water(ctx, "water-after-mature", []byte("water"), &farmpb.WaterCropRequest{
			PlotIndex: 0,
		}, before.FarmVersion, before.Plots[0].PlotVersion, before.Plots[0].CropCycleId, maturedAt)
		after := actor.Snapshot(maturedAt)
		if after.FarmVersion != before.FarmVersion || after.Plots[0].Watered {
			t.Fatalf("rejected owner water mutated farm: before=%v after=%v", before, after)
		}
		return nil, waterErr
	})
	var domain *Error
	if !errors.As(err, &domain) || domain.Code != commonpb.ErrorCode_ERR_CROP_ALREADY_MATURE {
		t.Fatalf("owner water error=%v", err)
	}

	_, err = registry.Submit(ctx, 11, func(ctx context.Context, actor *FarmActor) (any, error) {
		before := actor.Snapshot(maturedAt)
		_, careErr := actor.PrepareCare(ctx, "care-after-mature", []byte("care"), 12, 1, &farmpb.WaterCropRequest{
			PlotIndex: 0,
		}, before.FarmVersion, before.Plots[0].PlotVersion, before.Plots[0].CropCycleId, maturedAt)
		after := actor.Snapshot(maturedAt)
		if after.FarmVersion != before.FarmVersion || after.Plots[0].Watered {
			t.Fatalf("rejected friend care mutated farm: before=%v after=%v", before, after)
		}
		return nil, careErr
	})
	if !errors.As(err, &domain) || domain.Code != commonpb.ErrorCode_ERR_CROP_ALREADY_MATURE {
		t.Fatalf("friend care error=%v", err)
	}
}

func TestRegistryEvictsFencedActorAndRejectsLowerEpochReactivation(t *testing.T) {
	store := blobstore.NewMemoryStore()
	registry := NewRegistry(store, 8)
	defer registry.Stop()
	ctx := context.Background()
	routeMetrics := actorrouting.RegisterRouteMetrics(prometheus.NewRegistry())
	registry.SetRouteMetrics(routeMetrics)
	registry.SetWriterEpochResolver(func(uint64) (uint64, error) {
		return 1, nil
	})
	var resyncReason, resyncSubscription string
	registry.SetResyncNotifier(func(_ uint64, targets []Presence, reason string, _ uint64, _ uint64) {
		resyncReason = reason
		if len(targets) > 0 {
			resyncSubscription = targets[0].SubscriptionID
		}
	})

	if _, err := registry.Submit(ctx, 21, func(_ context.Context, actor *FarmActor) (any, error) {
		if _, err := actor.Join(JoinRequest{
			SubscriptionID: "fenced-sub", ViewerPlayerID: 21, SessionEpoch: 1,
			GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "conn",
			TransportGeneration: 1, RouteRevision: 1,
		}, time.UnixMilli(1)); err != nil {
			return nil, err
		}
		return actor.Snapshot(time.UnixMilli(1)), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Fence(ctx, 21, 2); err != nil {
		t.Fatal(err)
	}

	_, err := registry.Submit(ctx, 21, func(ctx context.Context, actor *FarmActor) (any, error) {
		return actor.Plant(ctx, "fenced-write", testPlantCropCycleID(21, "fenced-write"), []byte("fenced-write"), &farmpb.PlantCropRequest{
			PlotIndex: 0, CropDefinitionId: 1,
		}, 0, 1, time.UnixMilli(2), time.Second)
	})
	if !errors.Is(err, blobstore.ErrWriterEpochMismatch) {
		t.Fatalf("fenced write err=%v", err)
	}
	if got := testutil.ToFloat64(routeMetrics.WriterEpochMismatch); got != 1 {
		t.Fatalf("writer_epoch_mismatch_total=%v, want 1", got)
	}
	if resyncReason != "holder_rebuilt" || resyncSubscription != "fenced-sub" {
		t.Fatalf("fenced eviction notification reason=%q subscription=%q", resyncReason, resyncSubscription)
	}
	registry.mu.Lock()
	_, cached := registry.actors[21]
	registry.mu.Unlock()
	if cached {
		t.Fatal("fenced actor remained cached")
	}

	var readCalled atomic.Bool
	_, err = registry.Submit(ctx, 21, func(_ context.Context, actor *FarmActor) (any, error) {
		readCalled.Store(true)
		return actor.Snapshot(time.UnixMilli(3)), nil
	})
	if !errors.Is(err, blobstore.ErrWriterEpochMismatch) {
		t.Fatalf("lower-epoch reactivation err=%v", err)
	}
	if readCalled.Load() {
		t.Fatal("read served from a cached fenced actor")
	}
}

func TestRegistryFencedRemovalRacesWithConcurrentSubmit(t *testing.T) {
	store := blobstore.NewMemoryStore()
	registry := NewRegistry(store, 32)
	defer registry.Stop()
	ctx := context.Background()
	registry.SetWriterEpochResolver(func(uint64) (uint64, error) {
		return 1, nil
	})
	if _, err := registry.Submit(ctx, 22, func(_ context.Context, actor *FarmActor) (any, error) {
		return actor.Snapshot(time.UnixMilli(1)), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Fence(ctx, 22, 2); err != nil {
		t.Fatal(err)
	}

	var wait sync.WaitGroup
	for i := 0; i < 16; i++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			operationID := fmt.Sprintf("race-%d", index)
			_, _ = registry.Submit(ctx, 22, func(ctx context.Context, actor *FarmActor) (any, error) {
				return actor.Plant(ctx, operationID, testPlantCropCycleID(22, operationID), []byte("race"), &farmpb.PlantCropRequest{
					PlotIndex: 0, CropDefinitionId: 1,
				}, 0, 1, time.UnixMilli(int64(index+2)), time.Second)
			})
		}(i)
	}
	wait.Wait()

	registry.mu.Lock()
	_, cached := registry.actors[22]
	registry.mu.Unlock()
	if cached {
		t.Fatal("fenced actor remained cached after concurrent submits")
	}
}

func TestRegistryConnectionFenceNotifiesOnlyAffectedSubscription(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	var got []Presence
	registry.SetResyncNotifier(func(_ uint64, targets []Presence, reason string, _ uint64, _ uint64) {
		if reason != "connection_fence_changed" {
			t.Errorf("reason=%q", reason)
		}
		got = append(got, targets...)
	})
	_, err := registry.Submit(context.Background(), 31, func(_ context.Context, actor *FarmActor) (any, error) {
		for _, join := range []JoinRequest{
			{SubscriptionID: "affected", ViewerPlayerID: 1, SessionEpoch: 1, GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "conn-1", TransportGeneration: 1, RouteRevision: 1},
			{SubscriptionID: "unaffected", ViewerPlayerID: 2, SessionEpoch: 1, GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "conn-2", TransportGeneration: 1, RouteRevision: 1},
		} {
			if _, joinErr := actor.Join(join, time.UnixMilli(1)); joinErr != nil {
				return nil, joinErr
			}
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.CleanupConnectionFence(context.Background(), session.ActiveRoute{
		PlayerID: 1, SessionEpoch: 1, RouteRevision: 1,
		RouteFence: session.RouteFence{
			GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "conn-1",
			ConnectionGeneration: 1,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].SubscriptionID != "affected" {
		t.Fatalf("notified subscriptions=%v", got)
	}
	if scans := registry.CleanupActorScanTotal(); scans != 1 {
		t.Fatalf("legacy cleanup scan metric=%d, want 1", scans)
	}
}

func TestRegistryPassivatingWritableCancelsToActive(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	ctx := context.Background()

	var first *FarmActor
	if _, err := registry.Submit(ctx, 41, func(_ context.Context, actor *FarmActor) (any, error) {
		first = actor
		return actor.Snapshot(time.UnixMilli(1)), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.BeginPassivate(41); err != nil {
		t.Fatal(err)
	}
	if registry.Activation().StateOf(41) != activation.Quiescing {
		t.Fatalf("state=%s", registry.Activation().StateOf(41))
	}

	var second *FarmActor
	if _, err := registry.Submit(ctx, 41, func(_ context.Context, actor *FarmActor) (any, error) {
		second = actor
		return actor.Snapshot(time.UnixMilli(2)), nil
	}); err != nil {
		t.Fatal(err)
	}
	if registry.Activation().StateOf(41) != activation.Active {
		t.Fatalf("state=%s, want ACTIVE", registry.Activation().StateOf(41))
	}
	if first == nil || second != first {
		t.Fatal("writable passivating cancel should reuse same actor pointer")
	}
	if err := registry.FinishPassivate(41); err == nil {
		t.Fatal("a stale finish must not delete an actor after QUIESCING was canceled")
	}
	registry.mu.Lock()
	cached := registry.actors[41]
	registry.mu.Unlock()
	if cached != first || registry.Activation().StateOf(41) != activation.Active {
		t.Fatalf("stale finish deleted or changed live actor: cached=%p first=%p state=%s", cached, first, registry.Activation().StateOf(41))
	}
}

func TestRegistryPassivatingNotWritableStaleRouteDoesNotResurrect(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	ctx := context.Background()

	var stale *FarmActor
	if _, err := registry.Submit(ctx, 42, func(_ context.Context, actor *FarmActor) (any, error) {
		stale = actor
		return actor.Snapshot(time.UnixMilli(1)), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.BeginPassivate(42); err != nil {
		t.Fatal(err)
	}
	registry.SetWritableCheck(func(uint64) bool { return false })

	_, err := registry.Submit(ctx, 42, func(_ context.Context, actor *FarmActor) (any, error) {
		t.Fatal("task must not run when passivating without write rights")
		return actor.Snapshot(time.UnixMilli(2)), nil
	})
	if !errors.Is(err, activation.ErrStaleRoute) {
		t.Fatalf("err=%v, want ErrStaleRoute", err)
	}
	if registry.Activation().StateOf(42) != activation.Quiescing {
		t.Fatalf("state=%s, must not resurrect to ACTIVE", registry.Activation().StateOf(42))
	}
	if err := registry.FinishPassivate(42); err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	cached := registry.actors[42]
	registry.mu.Unlock()
	if cached != nil {
		t.Fatal("finish passivate left actor cached")
	}
	if registry.Activation().StateOf(42) != activation.Absent {
		t.Fatalf("state=%s", registry.Activation().StateOf(42))
	}

	// 重新激活必须走 ABSENT→LOADING→ACTIVE,不得把旧指针塞回 ACTIVE。
	registry.SetWritableCheck(nil)
	var fresh *FarmActor
	if _, err := registry.Submit(ctx, 42, func(_ context.Context, actor *FarmActor) (any, error) {
		fresh = actor
		return actor.Snapshot(time.UnixMilli(3)), nil
	}); err != nil {
		t.Fatal(err)
	}
	if fresh == nil || fresh == stale {
		t.Fatal("reactivated actor reused stale pointer")
	}
}

func TestRegistryLoadStormThrottlesSubmit(t *testing.T) {
	base := blobstore.NewMemoryStore()
	started := make(chan struct{})
	release := make(chan struct{})
	store := &blockingGetStore{Store: base, blockID: 51, started: started, release: release}

	opts := activation.DefaultOptions()
	opts.MaxConcurrentLoads = 1
	opts.ActivationRatePerSec = 0
	registry := NewRegistryWithOptions(store, RegistryOptions{
		MailboxSize: 8,
		Activation:  opts,
	})
	defer registry.Stop()
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = registry.Submit(ctx, 51, func(_ context.Context, actor *FarmActor) (any, error) {
			return actor.Snapshot(time.UnixMilli(1)), nil
		})
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first load did not start")
	}

	_, err := registry.Submit(ctx, 52, func(_ context.Context, actor *FarmActor) (any, error) {
		return actor.Snapshot(time.UnixMilli(1)), nil
	})
	if !errors.Is(err, activation.ErrActivationThrottled) {
		t.Fatalf("err=%v, want ErrActivationThrottled", err)
	}
	close(release)
	wg.Wait()
}

func TestRegistryPanicRemovesActorAndAllowsReload(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	ctx := context.Background()

	var first *FarmActor
	if _, err := registry.Submit(ctx, 61, func(_ context.Context, actor *FarmActor) (any, error) {
		first = actor
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}

	_, err := registry.Submit(ctx, 61, func(_ context.Context, _ *FarmActor) (any, error) {
		panic("farm actor corrupted")
	})
	if !errors.Is(err, actorruntime.ErrActorPanic) {
		t.Fatalf("err=%v, want ErrActorPanic", err)
	}
	if registry.Activation().StateOf(61) != activation.Absent {
		t.Fatalf("state=%s after panic", registry.Activation().StateOf(61))
	}
	registry.mu.Lock()
	_, cached := registry.actors[61]
	registry.mu.Unlock()
	if cached {
		t.Fatal("panicked actor remained cached")
	}

	var second *FarmActor
	if _, err := registry.Submit(ctx, 61, func(_ context.Context, actor *FarmActor) (any, error) {
		second = actor
		return actor.Snapshot(time.UnixMilli(1)), nil
	}); err != nil {
		t.Fatal(err)
	}
	if second == nil || second == first {
		t.Fatal("reload after panic reused damaged actor")
	}
}

// TestRegistrySystemPathProgressesWhenUserMailboxFull 验收生产路径已接入 SystemQueue:
// UserMailbox 满时 NotifyLeaseLost / CleanupFarmSubscription 仍可入队并执行。
func TestRegistrySystemPathProgressesWhenUserMailboxFull(t *testing.T) {
	registry := NewRegistryWithOptions(blobstore.NewMemoryStore(), RegistryOptions{MailboxSize: 1})
	defer registry.Stop()
	ctx := context.Background()
	const farmID = uint64(81)

	var resyncReasons []string
	registry.SetResyncNotifier(func(_ uint64, _ []Presence, reason string, _ uint64, _ uint64) {
		resyncReasons = append(resyncReasons, reason)
	})

	if _, err := registry.Submit(ctx, farmID, func(_ context.Context, actor *FarmActor) (any, error) {
		_, joinErr := actor.Join(JoinRequest{
			SubscriptionID: "sub-sys", ViewerPlayerID: 9, SessionEpoch: 1,
			GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "conn-sys",
			TransportGeneration: 1, RouteRevision: 1,
		}, time.UnixMilli(1))
		return nil, joinErr
	}); err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	started := make(chan struct{})
	var blockerWG sync.WaitGroup
	blockerWG.Add(1)
	go func() {
		defer blockerWG.Done()
		_, _ = registry.Submit(ctx, farmID, func(context.Context, *FarmActor) (any, error) {
			close(started)
			<-release
			return nil, nil
		})
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("blocking user task did not start")
	}

	// Fill UserMailbox (size=1).
	queuedDone := make(chan error, 1)
	go func() {
		_, err := registry.Submit(ctx, farmID, func(context.Context, *FarmActor) (any, error) {
			return nil, nil
		})
		queuedDone <- err
	}()
	time.Sleep(30 * time.Millisecond)

	_, err := registry.Submit(ctx, farmID, func(context.Context, *FarmActor) (any, error) {
		return "should-not-run", nil
	})
	if !errors.Is(err, actorruntime.ErrMailboxFull) && !errors.Is(err, actorruntime.ErrActorBusy) {
		t.Fatalf("want user mailbox full, got %v", err)
	}

	leaseDone := make(chan error, 1)
	go func() {
		leaseDone <- registry.NotifyLeaseLost(ctx, farmID)
	}()
	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-leaseDone:
		t.Fatalf("NotifyLeaseLost returned before release: %v", err)
	default:
	}

	// Contrasting user path still busy.
	_, err = registry.Submit(ctx, farmID, func(context.Context, *FarmActor) (any, error) {
		return nil, nil
	})
	if !errors.Is(err, actorruntime.ErrMailboxFull) && !errors.Is(err, actorruntime.ErrActorBusy) {
		t.Fatalf("want user still full, got %v", err)
	}

	close(release)
	blockerWG.Wait()

	select {
	case err := <-leaseDone:
		if err != nil {
			t.Fatalf("NotifyLeaseLost: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("NotifyLeaseLost did not complete — SystemQueue not wired?")
	}
	// LeaseLost 必须已执行（resync）；排队的 user 随后可能重新激活，这是预期串行语义。
	if len(resyncReasons) == 0 || resyncReasons[0] != "holder_rebuilt" {
		t.Fatalf("resync reasons=%v — LeaseLost did not evict", resyncReasons)
	}
	if err := <-queuedDone; err != nil {
		t.Fatalf("queued user: %v", err)
	}

	// Second farm: exact subscription cleanup under user-full.
	const farmID2 = uint64(82)
	registry2 := NewRegistryWithOptions(blobstore.NewMemoryStore(), RegistryOptions{MailboxSize: 1})
	defer registry2.Stop()
	var exactJoin *JoinResult
	if _, err := registry2.Submit(ctx, farmID2, func(_ context.Context, actor *FarmActor) (any, error) {
		var e error
		exactJoin, e = actor.Join(JoinRequest{
			SubscriptionID: "sub-fence", ViewerPlayerID: 3, SessionEpoch: 1,
			GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "conn-f",
			TransportGeneration: 1, RouteRevision: 1,
		}, time.UnixMilli(1))
		return nil, e
	}); err != nil {
		t.Fatal(err)
	}
	release2 := make(chan struct{})
	started2 := make(chan struct{})
	go registry2.Submit(ctx, farmID2, func(context.Context, *FarmActor) (any, error) {
		close(started2)
		<-release2
		return nil, nil
	})
	select {
	case <-started2:
	case <-time.After(2 * time.Second):
		t.Fatal("blocker2 did not start")
	}
	go registry2.Submit(ctx, farmID2, func(context.Context, *FarmActor) (any, error) { return nil, nil })
	time.Sleep(30 * time.Millisecond)
	if _, err := registry2.Submit(ctx, farmID2, func(context.Context, *FarmActor) (any, error) { return nil, nil }); !errors.Is(err, actorruntime.ErrMailboxFull) && !errors.Is(err, actorruntime.ErrActorBusy) {
		t.Fatalf("farm2 user full: %v", err)
	}

	cleanupDone := make(chan error, 1)
	go func() {
		_, cleanupErr := registry2.CleanupFarmSubscription(ctx, farmID2, LeaveRequest{
			SubscriptionID: exactJoin.SubscriptionID, PresenceEpoch: exactJoin.PresenceEpoch,
			ViewerPlayerID: 3, SessionEpoch: 1, RouteRevision: 1,
			GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "conn-f",
			TransportGeneration: 1,
		})
		cleanupDone <- cleanupErr
	}()
	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-cleanupDone:
		t.Fatalf("CleanupFarmSubscription returned before release: %v", err)
	default:
	}
	close(release2)
	select {
	case err := <-cleanupDone:
		if err != nil {
			t.Fatalf("CleanupFarmSubscription: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CleanupFarmSubscription stuck — still on UserMailbox?")
	}
	if scans := registry2.CleanupActorScanTotal(); scans != 0 {
		t.Fatalf("exact cleanup used legacy scan: %d", scans)
	}
	value, err := registry2.Submit(ctx, farmID2, func(_ context.Context, actor *FarmActor) (any, error) {
		return actor.PresenceCount(), nil
	})
	if err != nil || value.(int) != 0 {
		t.Fatalf("Presence remained after exact SystemQueue cleanup: count=%v err=%v", value, err)
	}
}

type blockingGetStore struct {
	blobstore.Store
	blockID uint64
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingGetStore) Get(ctx context.Context, id uint64) ([]byte, uint64, error) {
	if id == s.blockID {
		s.once.Do(func() { close(s.started) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		}
	}
	return s.Store.Get(ctx, id)
}
