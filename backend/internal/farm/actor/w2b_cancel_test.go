package actor

import (
	"context"
	"errors"
	"testing"
	"time"

	publicpb "farmgame/api/gen/public"
)

func newW2BCancelActor(farmID uint64, hub *SubscriptionHub) *FarmActor {
	actor := &FarmActor{
		farmID: farmID, record: InitialRecord(farmID), subscriptionHub: hub,
		presence: make(map[string]*Presence), presenceByPlayer: make(map[uint64]string),
		presenceByAttempt: make(map[string]string), joinIdempotency: make(map[string]joinIdempotencyEntry),
		maxSubscriptions: defaultMaxSubscriptions, maxBufferItems: 64,
		maxBufferBytes: defaultMaxJoinBufferBytes, maxJoinDuration: defaultMaxJoinDuration,
	}
	hub.Attach(farmID, actor)
	return actor
}

func w2BJoinRequest(operationID, attemptID, subscriptionID string) JoinRequest {
	return JoinRequest{
		OperationID: operationID, JoinAttemptID: attemptID, SubscriptionID: subscriptionID,
		ViewerPlayerID: 700, SessionEpoch: 9, GatewayID: "gate-1", GatewayBootID: "boot-1",
		ConnectionID: "conn-1", TransportGeneration: 3, RouteRevision: 4,
	}
}

func w2BCancelRequest(join JoinRequest, now time.Time) CancelJoinAttemptRequest {
	return CancelJoinAttemptRequest{
		JoinAttemptID: join.JoinAttemptID, OperationID: join.OperationID,
		Fence: LeaveRequest{
			JoinAttemptID: join.JoinAttemptID, ViewerPlayerID: join.ViewerPlayerID,
			SessionEpoch: join.SessionEpoch, GatewayID: join.GatewayID,
			GatewayBootID: join.GatewayBootID, ConnectionID: join.ConnectionID,
			TransportGeneration: join.TransportGeneration, RouteRevision: join.RouteRevision,
		},
		Now: now,
	}
}

func TestW2BCancelBeforeCreateTombstoneRejectsLateJoin(t *testing.T) {
	hub := NewSubscriptionHub(8)
	actor := newW2BCancelActor(71, hub)
	now := time.Unix(100, 0)
	join := w2BJoinRequest("join-op", "attempt-1", "sub-1")

	result, err := hub.CancelJoinAttempt(context.Background(), 71, w2BCancelRequest(join, now))
	if err != nil || result.Status != CancelAttemptCancelled {
		t.Fatalf("cancel result=%+v err=%v", result, err)
	}
	if retired, err := hub.RetireFarmIfEmpty(context.Background(), 71); err != nil || retired {
		t.Fatalf("lane with live cancel tombstone retired=%v err=%v", retired, err)
	}
	if _, err := actor.CreateOrReuseJoining(join, now.Add(time.Millisecond)); !errors.Is(err, ErrJoinAttemptCancelled) {
		t.Fatalf("late Join err=%v, want ErrJoinAttemptCancelled", err)
	}
	if count, err := hub.PresenceCount(context.Background(), 71); err != nil || count != 0 {
		t.Fatalf("presence count=%d err=%v", count, err)
	}
}

func TestW2BCreateBeforeCancelRemovesOnlyUnarmedJoin(t *testing.T) {
	hub := NewSubscriptionHub(8)
	actor := newW2BCancelActor(72, hub)
	now := time.Unix(200, 0)
	join := w2BJoinRequest("join-op", "attempt-2", "sub-2")

	created, err := actor.CreateOrReuseJoining(join, now)
	if err != nil {
		t.Fatal(err)
	}
	request := w2BCancelRequest(join, now.Add(time.Millisecond))
	result, err := hub.CancelJoinAttempt(context.Background(), 72, request)
	if err != nil || result.Status != CancelAttemptCancelled {
		t.Fatalf("cancel result=%+v err=%v", result, err)
	}
	if count, _ := hub.PresenceCount(context.Background(), 72); count != 0 {
		t.Fatalf("presence count=%d, want 0", count)
	}
	// Repeated cancellation is resolved from the retained tombstone.
	result, err = hub.CancelJoinAttempt(context.Background(), 72, request)
	if err != nil || result.Status != CancelAttemptCancelled {
		t.Fatalf("repeat cancel result=%+v err=%v", result, err)
	}
	request.Fence.TransportGeneration++
	result, err = hub.CancelJoinAttempt(context.Background(), 72, request)
	if err != nil || result.Status != CancelAttemptStaleFence {
		t.Fatalf("stale cancel result=%+v err=%v", result, err)
	}
	if created.SubscriptionID != "sub-2" {
		t.Fatalf("created subscription=%q", created.SubscriptionID)
	}
}

func TestW2BCancelArmedJoinRequiresExactCleanup(t *testing.T) {
	hub := NewSubscriptionHub(8)
	actor := newW2BCancelActor(73, hub)
	now := time.Unix(300, 0)
	join := w2BJoinRequest("join-op", "attempt-3", "sub-3")
	created, err := actor.CreateOrReuseJoining(join, now)
	if err != nil {
		t.Fatal(err)
	}
	fence := w2BCancelRequest(join, now).Fence
	fence.SubscriptionID = created.SubscriptionID
	if _, _, err := actor.ArmSnapshot(ArmSnapshotRequest{
		SubscriptionID: created.SubscriptionID, JoinAttemptID: join.JoinAttemptID, Fence: fence,
		OwnerPublicProfile: &publicpb.PublicProfileView{PlayerId: 73, DisplayName: "农场主", AvatarId: 1, Level: 1},
	}, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	result, err := hub.CancelJoinAttempt(context.Background(), 73,
		w2BCancelRequest(join, now.Add(2*time.Millisecond)))
	if err != nil || result.Status != CancelAttemptAlreadyProgressed || result.SubscriptionID != "sub-3" {
		t.Fatalf("cancel armed result=%+v err=%v", result, err)
	}
	if count, _ := hub.PresenceCount(context.Background(), 73); count != 1 {
		t.Fatalf("armed Presence was broadly deleted, count=%d", count)
	}
}

func TestW2BTombstoneCapacityFailsJoinClosedUntilTTLPrune(t *testing.T) {
	hub := NewSubscriptionHub(8)
	hub.SetCancelTombstoneLimitsForTest(1, 4096, time.Second)
	actor := newW2BCancelActor(74, hub)
	now := time.Unix(400, 0)
	first := w2BJoinRequest("join-op-1", "attempt-4", "sub-4")
	if result, err := hub.CancelJoinAttempt(context.Background(), 74, w2BCancelRequest(first, now)); err != nil || result.Status != CancelAttemptCancelled {
		t.Fatalf("first cancel result=%+v err=%v", result, err)
	}
	second := w2BJoinRequest("join-op-2", "attempt-5", "sub-5")
	if _, err := actor.CreateOrReuseJoining(second, now.Add(time.Millisecond)); !errors.Is(err, ErrCancelTombstoneCapacity) {
		t.Fatalf("Join under saturated tombstones err=%v", err)
	}
	if _, err := actor.CreateOrReuseJoining(second, now.Add(2*time.Second)); err != nil {
		t.Fatalf("Join after tombstone TTL prune err=%v", err)
	}
}

func TestW2BExpiredTombstoneAllowsLaneRetirement(t *testing.T) {
	hub := NewSubscriptionHub(8)
	hub.SetCancelTombstoneLimitsForTest(1, 4096, time.Second)
	now := time.Unix(500, 0)
	join := w2BJoinRequest("join-op-expire", "attempt-expire", "sub-expire")
	if result, err := hub.CancelJoinAttempt(context.Background(), 75, w2BCancelRequest(join, now)); err != nil || result.Status != CancelAttemptCancelled {
		t.Fatalf("cancel result=%+v err=%v", result, err)
	}
	if _, err := hub.PruneExpired(context.Background(), now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if stats := hub.Stats(); stats.ActiveLanes != 0 {
		t.Fatalf("expired tombstone retained lane: %+v", stats)
	}
}
