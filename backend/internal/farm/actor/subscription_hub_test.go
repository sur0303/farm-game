package actor

import (
	"context"
	"testing"
	"time"

	"farmgame/internal/logic/actor/activation"
	"farmgame/internal/storage/blobstore"
)

func TestPresenceSurvivesActorPassivationAndExactCleanupDoesNotReactivate(t *testing.T) {
	registry := NewRegistry(blobstore.NewMemoryStore(), 8)
	defer registry.Stop()
	ctx := context.Background()
	var joined *JoinResult
	if _, err := registry.Submit(ctx, 77, func(_ context.Context, actor *FarmActor) (any, error) {
		var err error
		joined, err = actor.Join(JoinRequest{
			OperationID: "join-passivate", SubscriptionID: "sub-passivate", ViewerPlayerID: 700,
			SessionEpoch: 9, GatewayID: "gw", GatewayBootID: "boot", ConnectionID: "conn",
			TransportGeneration: 3, RouteRevision: 4,
		}, time.Now())
		return joined, err
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.BeginPassivate(77); err != nil {
		t.Fatal(err)
	}
	if err := registry.FinishPassivate(77); err != nil {
		t.Fatal(err)
	}
	if registry.Activation().StateOf(77) != activation.Absent {
		t.Fatalf("actor state=%s, want ABSENT", registry.Activation().StateOf(77))
	}
	if count, err := registry.SubscriptionHub().PresenceCount(ctx, 77); err != nil || count != 1 {
		t.Fatalf("Hub Presence after actor passivation count=%d err=%v", count, err)
	}
	removed, err := registry.CleanupFarmSubscription(ctx, 77, LeaveRequest{
		SubscriptionID: joined.SubscriptionID, ViewerPlayerID: joined.ViewerPlayerID,
		SessionEpoch: joined.SessionEpoch, GatewayID: joined.GatewayID, GatewayBootID: joined.GatewayBootID,
		ConnectionID: joined.ConnectionID, TransportGeneration: joined.Generation,
		RouteRevision: joined.RouteRevision, PresenceEpoch: joined.PresenceEpoch,
	})
	if err != nil || !removed {
		t.Fatalf("exact Hub cleanup removed=%v err=%v", removed, err)
	}
	if registry.Activation().StateOf(77) != activation.Absent {
		t.Fatalf("Hub cleanup reactivated actor: %s", registry.Activation().StateOf(77))
	}
	stats := registry.SubscriptionHub().Stats()
	if stats.ActiveLanes != 0 || stats.LanesRetired != 1 {
		t.Fatalf("Hub did not passivate independently: %+v", stats)
	}
}

func TestCommittedPublishLaneFullKeepsBusinessTruthAndForcesResync(t *testing.T) {
	hub := NewSubscriptionHub(1)
	actor := &FarmActor{
		farmID: 88, record: InitialRecord(88), activationEpoch: 3, subscriptionHub: hub,
		maxSubscriptions: defaultMaxSubscriptions, maxBufferItems: 64,
		maxBufferBytes: defaultMaxJoinBufferBytes, maxJoinDuration: defaultMaxJoinDuration,
	}
	hub.Attach(88, actor)
	if _, err := actor.Join(JoinRequest{
		OperationID: "join-overload", SubscriptionID: "sub-overload", ViewerPlayerID: 800,
		SessionEpoch: 1, GatewayID: "gw", GatewayBootID: "boot", ConnectionID: "conn",
		TransportGeneration: 1, RouteRevision: 1,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- hub.BlockLaneForTest(88, started, release)
	}()
	<-started
	secondDone := make(chan error, 1)
	go func() {
		_, err := hub.Execute(context.Background(), 88, nil, func() (any, error) { return nil, nil })
		secondDone <- err
	}()
	deadline := time.Now().Add(time.Second)
	for hub.LaneEventQueueLenForTest(88) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// Simulate a CAS which has already committed FarmVersion=1. The publish
	// is the third event and must not turn this business success into failure.
	actor.record.Core.FarmVersion = 1
	published := make(chan struct {
		changeVersion uint64
		resyncs       []ResyncNotice
		err           error
	}, 1)
	go func() {
		change, _, resyncs, err := actor.publish(actor.record.Core.Plots[0], time.Now())
		version := uint64(0)
		if change != nil {
			version = change.FarmVersion
		}
		published <- struct {
			changeVersion uint64
			resyncs       []ResyncNotice
			err           error
		}{changeVersion: version, resyncs: resyncs, err: err}
	}()
	time.Sleep(time.Millisecond)
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	result := <-published
	if result.err != nil || result.changeVersion != 1 {
		t.Fatalf("publish version=%d err=%v", result.changeVersion, result.err)
	}
	if len(result.resyncs) != 1 || result.resyncs[0].Reason != "farm_event_lane_full" {
		t.Fatalf("resyncs=%v", result.resyncs)
	}
	if actor.record.Core.FarmVersion != 1 {
		t.Fatalf("business truth rolled back to version %d", actor.record.Core.FarmVersion)
	}
	if count, _ := hub.PresenceCount(context.Background(), 88); count != 1 {
		t.Fatalf("terminal Presence count=%d, want 1", count)
	}
	if got := actor.presence[result.resyncs[0].Presence.SubscriptionID]; got == nil || got.Phase != PresenceResyncRequired {
		t.Fatalf("terminal Presence=%+v", got)
	}
}

func TestSubscriptionFallbackTTLIsLongLivedAndNotRenewedPerEvent(t *testing.T) {
	hub := NewSubscriptionHub(8)
	hub.SetFallbackTTL(time.Hour)
	now := time.Unix(100, 0)
	actor := &FarmActor{farmID: 99, record: InitialRecord(99), subscriptionHub: hub,
		maxSubscriptions: defaultMaxSubscriptions, maxJoinDuration: defaultMaxJoinDuration,
		maxBufferItems: 64, maxBufferBytes: defaultMaxJoinBufferBytes}
	hub.Attach(99, actor)
	if _, err := actor.Join(JoinRequest{OperationID: "join-ttl", SubscriptionID: "sub-ttl", ViewerPlayerID: 900,
		SessionEpoch: 1, GatewayID: "gw", GatewayBootID: "boot", ConnectionID: "conn",
		TransportGeneration: 1, RouteRevision: 1}, now); err != nil {
		t.Fatal(err)
	}
	deadline := actor.presence["sub-ttl"].FallbackExpiresAt
	if !deadline.Equal(now.Add(time.Hour)) {
		t.Fatalf("fallback deadline=%v", deadline)
	}
	// Reading and pushing do not renew the per-subscription fallback.
	actor.record.Core.FarmVersion = 1
	if _, _, _, err := actor.publish(actor.record.Core.Plots[0], now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := actor.presence["sub-ttl"].FallbackExpiresAt; !got.Equal(deadline) {
		t.Fatalf("event renewed fallback deadline: got=%v want=%v", got, deadline)
	}
	removed, err := hub.PruneExpired(context.Background(), deadline)
	if err != nil || len(removed) != 1 {
		t.Fatalf("fallback prune removed=%d err=%v", len(removed), err)
	}
}
