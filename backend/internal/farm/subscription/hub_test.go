package subscription

import (
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestFarmEventLaneOrdersEventsAndBoundsQueuedWork(t *testing.T) {
	lane := newFarmEventLane(1)
	started := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	var order []int
	firstDone := make(chan error, 1)
	go func() {
		_, err := lane.execute(context.Background(), false, func(*subscriptionState) (any, error) {
			close(started)
			<-release
			mu.Lock()
			order = append(order, 1)
			mu.Unlock()
			return nil, nil
		})
		firstDone <- err
	}()
	<-started
	secondDone := make(chan error, 1)
	go func() {
		_, err := lane.execute(context.Background(), false, func(*subscriptionState) (any, error) {
			mu.Lock()
			order = append(order, 2)
			mu.Unlock()
			return nil, nil
		})
		secondDone <- err
	}()
	deadline := time.Now().Add(time.Second)
	for len(lane.events) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if _, err := lane.execute(context.Background(), false, func(*subscriptionState) (any, error) {
		return nil, nil
	}); !errors.Is(err, ErrFarmEventLaneFull) {
		t.Fatalf("third event err=%v, want ErrFarmEventLaneFull", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Fatalf("event order=%v, want [1 2]", order)
	}
}

func TestCancelBeforeCreateSurvivesConcurrentLaneRetirement(t *testing.T) {
	for iteration := 0; iteration < 200; iteration++ {
		hub := NewSubscriptionHub(4)
		farmID := uint64(iteration + 1)
		lane := hub.liveLane(farmID)
		lane.admissionMu.Lock()

		now := time.Now()
		fence := LeaveRequest{
			ViewerPlayerID: 9001, SessionEpoch: 3,
			GatewayID: "gate-a", GatewayBootID: "boot-a", ConnectionID: "conn-a",
			TransportGeneration: 7, RouteRevision: 11,
		}
		cancelDone := make(chan error, 1)
		go func() {
			result, err := hub.CancelJoinAttempt(context.Background(), farmID, CancelJoinAttemptRequest{
				JoinAttemptID: "attempt", OperationID: "operation", Fence: fence, Now: now,
			})
			if err == nil && result.Status != CancelAttemptCancelled {
				err = errors.New("cancel did not install tombstone")
			}
			cancelDone <- err
		}()
		retireDone := make(chan error, 1)
		go func() {
			_, err := hub.RetireFarmIfEmpty(context.Background(), farmID)
			retireDone <- err
		}()
		runtime.Gosched()
		lane.admissionMu.Unlock()

		if err := <-cancelDone; err != nil {
			hub.Stop()
			t.Fatalf("iteration %d cancel: %v", iteration, err)
		}
		if err := <-retireDone; err != nil {
			hub.Stop()
			t.Fatalf("iteration %d retire: %v", iteration, err)
		}
		_, err := hub.ExecuteCreateOrReuseJoining(context.Background(), farmID, nil,
			"attempt", now, func() (any, error) { return "created", nil })
		if !errors.Is(err, ErrJoinAttemptCancelled) {
			hub.Stop()
			t.Fatalf("iteration %d late Join err=%v, want ErrJoinAttemptCancelled", iteration, err)
		}
		hub.Stop()
	}
}

func TestReindexFarmOnlyTouchesOneFarm(t *testing.T) {
	hub := NewSubscriptionHub(8)
	t.Cleanup(hub.Stop)
	const farms = 64
	for farmID := uint64(1); farmID <= farms; farmID++ {
		installBootPresence(t, hub, farmID, "gate-a", "boot-a", 1)
	}
	if got := len(hub.SubscriptionRefs(GateBootFence{GatewayID: "gate-a", GatewayBootID: "boot-a"})); got != farms {
		t.Fatalf("boot refs=%d want=%d", got, farms)
	}

	lane := hub.liveLane(7)
	if _, err := lane.execute(context.Background(), true, func(state *subscriptionState) (any, error) {
		presence := state.presence["sub-7"]
		presence.GatewayID = "gate-b"
		presence.GatewayBootID = "boot-b"
		presence.PresenceEpoch++
		hub.reindexFarm(7, state)
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}

	oldRefs := hub.SubscriptionRefs(GateBootFence{GatewayID: "gate-a", GatewayBootID: "boot-a"})
	if len(oldRefs) != farms-1 {
		t.Fatalf("old boot refs=%d want=%d", len(oldRefs), farms-1)
	}
	for _, ref := range oldRefs {
		if ref.FarmID == 7 {
			t.Fatalf("farm 7 still indexed on old boot: %+v", ref)
		}
	}
	newRefs := hub.SubscriptionRefs(GateBootFence{GatewayID: "gate-b", GatewayBootID: "boot-b"})
	if len(newRefs) != 1 || newRefs[0].FarmID != 7 || newRefs[0].PresenceEpoch != 2 {
		t.Fatalf("new boot refs=%+v", newRefs)
	}

	removed, err := hub.DrainFarm(context.Background(), 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 {
		t.Fatalf("drained=%d want=1", len(removed))
	}
	if got := hub.SubscriptionRefs(GateBootFence{GatewayID: "gate-b", GatewayBootID: "boot-b"}); len(got) != 0 {
		t.Fatalf("drained farm still indexed: %+v", got)
	}
	if got := len(hub.SubscriptionRefs(GateBootFence{GatewayID: "gate-a", GatewayBootID: "boot-a"})); got != farms-1 {
		t.Fatalf("other farms dropped, boot refs=%d", got)
	}
}

func TestPruneExpiredRetiresExpiredTombstoneLane(t *testing.T) {
	hub := NewSubscriptionHub(8)
	t.Cleanup(hub.Stop)
	hub.SetCancelTombstoneLimitsForTest(1, 4096, time.Second)
	now := time.Unix(500, 0)
	result, err := hub.CancelJoinAttempt(context.Background(), 75, CancelJoinAttemptRequest{
		JoinAttemptID: "attempt-expire", OperationID: "join-op-expire",
		Fence: LeaveRequest{
			ViewerPlayerID: 700, SessionEpoch: 9, GatewayID: "gate-1", GatewayBootID: "boot-1",
			ConnectionID: "conn-1", TransportGeneration: 3, RouteRevision: 4,
		},
		Now: now,
	})
	if err != nil || result.Status != CancelAttemptCancelled {
		t.Fatalf("cancel result=%+v err=%v", result, err)
	}
	hub.mu.Lock()
	expiry := hub.farmExpiry[75]
	hub.mu.Unlock()
	if expiry.IsZero() {
		t.Fatalf("tombstone farm missing expiry index")
	}
	if _, err := hub.PruneExpired(context.Background(), now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if stats := hub.Stats(); stats.ActiveLanes != 0 {
		t.Fatalf("expired tombstone retained lane: %+v expiry=%v", stats, expiry)
	}
}

func TestPruneExpiredSkipsFarmsThatAreNotDue(t *testing.T) {
	hub := NewSubscriptionHub(8)
	t.Cleanup(hub.Stop)
	now := time.Unix(1_700_000_000, 0)
	for farmID := uint64(1); farmID <= 32; farmID++ {
		installBootPresence(t, hub, farmID, "gate-a", "boot-a", 1)
		lane := hub.liveLane(farmID)
		if _, err := lane.execute(context.Background(), true, func(state *subscriptionState) (any, error) {
			state.presence["sub-"+itoa(farmID)].LeaseExpiresAt = now.Add(time.Hour)
			hub.reindexFarm(farmID, state)
			return nil, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := hub.liveLane(1).execute(context.Background(), true, func(state *subscriptionState) (any, error) {
		state.presence["sub-1"].LeaseExpiresAt = now.Add(-time.Second)
		hub.reindexFarm(1, state)
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	removed, err := hub.PruneExpired(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0].SubscriptionID != "sub-1" {
		t.Fatalf("removed=%+v", removed)
	}
	if got, err := hub.PresenceCount(context.Background(), 2); err != nil || got != 1 {
		t.Fatalf("live farm pruned: count=%d err=%v", got, err)
	}
}

func installBootPresence(t *testing.T, hub *SubscriptionHub, farmID uint64, gatewayID, bootID string, epoch uint64) {
	t.Helper()
	presence := &Presence{
		SubscriptionID: "sub-" + itoa(farmID),
		JoinAttemptID:  "attempt-" + itoa(farmID),
		ViewerPlayerID: 1000 + farmID,
		GatewayID:      gatewayID,
		GatewayBootID:  bootID,
		ConnectionID:   "conn-" + itoa(farmID),
		PresenceEpoch:  epoch,
		Phase:          PresenceInFarm,
	}
	lane := hub.liveLane(farmID)
	if _, err := lane.execute(context.Background(), true, func(state *subscriptionState) (any, error) {
		state.presence[presence.SubscriptionID] = presence
		state.presenceByPlayer[presence.ViewerPlayerID] = presence.SubscriptionID
		state.presenceByAttempt[presence.JoinAttemptID] = presence.SubscriptionID
		hub.reindexFarm(farmID, state)
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
}

func itoa(value uint64) string {
	return strconv.FormatUint(value, 10)
}
