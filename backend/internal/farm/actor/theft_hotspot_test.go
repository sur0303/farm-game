package actor

import (
	"fmt"
	"testing"
	"time"
)

func TestFarmHotspotTwoHundredPresentConnectionsObserveOneFarmChange(t *testing.T) {
	const farmID = uint64(1800)
	now := time.UnixMilli(2_026_080_500_000)
	hub := NewSubscriptionHub(256)
	actor := &FarmActor{
		farmID: farmID, record: InitialRecord(farmID), activationEpoch: 1,
		subscriptionHub: hub, maxSubscriptions: defaultMaxSubscriptions,
		maxBufferItems: 64, maxBufferBytes: defaultMaxJoinBufferBytes,
		maxJoinDuration: defaultMaxJoinDuration,
	}
	hub.Attach(farmID, actor)
	for i := uint64(0); i < 200; i++ {
		joined, err := actor.Join(JoinRequest{
			OperationID:    fmt.Sprintf("hotspot-join-%03d", i),
			SubscriptionID: fmt.Sprintf("hotspot-sub-%03d", i), ViewerPlayerID: 800000 + i,
			SessionEpoch: 1, GatewayID: fmt.Sprintf("gateway-%03d", i), GatewayBootID: "hotspot-boot",
			ConnectionID: fmt.Sprintf("connection-%03d", i), TransportGeneration: 1, RouteRevision: 1,
		}, now)
		if err != nil {
			t.Fatalf("join %d: %v", i, err)
		}
		if _, err := actor.ConfirmSnapshotQueued(joined); err != nil {
			t.Fatalf("confirm snapshot %d: %v", i, err)
		}
		if changes, err := actor.DrainChangesOrConfirm(joined); err != nil || len(changes) != 0 {
			t.Fatalf("drain %d changes=%d err=%v", i, len(changes), err)
		}
	}
	if got := actor.PresenceCount(); got != 200 {
		t.Fatalf("presence count=%d, want 200", got)
	}
	actor.record.Core.FarmVersion = 1
	change, recipients, resyncs, err := actor.publish(actor.record.Core.Plots[0], now)
	if err != nil {
		t.Fatal(err)
	}
	if len(resyncs) != 0 || change == nil || change.GetFarmVersion() != 1 {
		t.Fatalf("change=%v recipients=%d resyncs=%v", change, len(recipients), resyncs)
	}
	if len(recipients) != 200 {
		t.Fatalf("recipients=%d, want every present connection", len(recipients))
	}
	seen := make(map[uint64]bool, len(recipients))
	for _, recipient := range recipients {
		if seen[recipient.ViewerPlayerID] {
			t.Fatalf("duplicate recipient player=%d", recipient.ViewerPlayerID)
		}
		seen[recipient.ViewerPlayerID] = true
		if recipient.LastPushedVersion != change.GetFarmVersion() {
			t.Fatalf("player=%d observed version=%d want=%d", recipient.ViewerPlayerID, recipient.LastPushedVersion, change.GetFarmVersion())
		}
	}
}
