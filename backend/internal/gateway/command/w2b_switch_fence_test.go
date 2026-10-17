package command

import (
	"errors"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/gateway/auth"
	"farmgame/internal/gateway/commandcatalog"
	"farmgame/internal/gateway/downlink"
	"farmgame/internal/gateway/session"
)

func TestW2BFinalAdmissionRejectsSubscriptionChangedBySwitch(t *testing.T) {
	// 旧 WATER_CROP 已更名为 CARE(编号沿用);目标只来自 Typed Payload。
	spec, ok := commandcatalog.Lookup(commonpb.CommandType_CARE)
	if !ok {
		t.Fatal("missing CARE spec")
	}
	decoded, err := spec.Decoder(nil)
	if err != nil {
		t.Fatal(err)
	}
	decoded.(*publicpb.CareRequest).TargetFarmId = 202
	decoded.(*publicpb.CareRequest).CareType = publicpb.CareType_CARE_TYPE_WATER
	binding := w2bTestBinding(101, "conn-switch", 7)
	expected := connection{
		generation: 7, binding: binding, currentFarmID: 202, recoveryComplete: true,
		currentSubscriptionID: "sub-a", currentJoinAttemptID: "attempt-a",
		currentPresenceEpoch: 19, subscriptionContextGeneration: 3,
		routeLeaseDeadline:        time.Now().Add(time.Minute),
		subscriptionLeaseDeadline: time.Now().Add(time.Minute),
	}
	current := expected
	current.currentFarmID = 303
	current.currentSubscriptionID = "sub-b"
	current.currentJoinAttemptID = "attempt-b"
	current.currentPresenceEpoch = 29
	current.subscriptionContextGeneration = 4
	gate := &Gate{connections: map[string]connection{"conn-switch": current}}

	_, err = gate.refreshAdmissionContext("conn-switch", 7, expected, spec, decoded)
	if err == nil {
		t.Fatal("old farm command admitted after switch fence advanced")
	}
	if !errors.Is(err, ErrBadMessage) {
		t.Fatalf("old farm command rejected by wrong boundary: %v", err)
	}
}

func TestW2BDelayedTerminalCannotClearNewerSubscription(t *testing.T) {
	binding := w2bTestBinding(101, "conn-terminal", 7)
	gate := &Gate{connections: map[string]connection{
		"conn-terminal": {
			generation: 7, binding: binding, currentFarmID: 303,
			currentSubscriptionID: "sub-b", currentJoinAttemptID: "attempt-b",
			currentPresenceEpoch: 29, subscriptionContextGeneration: 4,
		},
	}}
	gate.terminateSubscription(downlink.SubscriptionTermination{
		ConnectionID: "conn-terminal", Generation: 7, FarmID: 202,
		SubscriptionID: "sub-a", JoinAttemptID: "attempt-a", PresenceEpoch: 19,
	})
	gate.mu.Lock()
	current := gate.connections["conn-terminal"]
	gate.mu.Unlock()
	if current.currentFarmID != 303 || current.currentSubscriptionID != "sub-b" ||
		current.subscriptionContextGeneration != 4 {
		t.Fatalf("delayed old terminal cleared new subscription: %+v", current)
	}

	gate.terminateSubscription(downlink.SubscriptionTermination{
		ConnectionID: "conn-terminal", Generation: 7, FarmID: 303,
		SubscriptionID: "sub-b", JoinAttemptID: "attempt-b", PresenceEpoch: 29,
	})
	gate.mu.Lock()
	current = gate.connections["conn-terminal"]
	gate.mu.Unlock()
	if current.currentFarmID != 0 || current.currentSubscriptionID != "" ||
		current.subscriptionContextGeneration != 5 {
		t.Fatalf("exact terminal did not clear subscription: %+v", current)
	}
}

func w2bTestBinding(playerID uint64, connectionID string, generation uint64) *auth.SessionBinding {
	return &auth.SessionBinding{
		PlayerID: playerID, ActiveSessionEpoch: 11,
		Route: session.ActiveRoute{
			RouteFence: session.RouteFence{
				GatewayID: "gate-a", GatewayBootID: "boot-a",
				ConnectionID: connectionID, ConnectionGeneration: generation,
			},
			PlayerID: playerID, SessionEpoch: 11, RouteRevision: 13,
			LeaseExpiresAt: time.Now().Add(time.Minute),
		},
	}
}
