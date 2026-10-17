package subscription

import (
	"context"
	"testing"
	"time"
)

func installLeasePresence(t *testing.T, hub *SubscriptionHub, farmID uint64, phase PresencePhase,
	now time.Time) *Presence {
	t.Helper()
	presence := &Presence{
		SubscriptionID:      "sub-1",
		JoinAttemptID:       "attempt-1",
		ViewerPlayerID:      1001,
		SessionEpoch:        7,
		GatewayID:           "gate-a",
		GatewayBootID:       "boot-a",
		ConnectionID:        "conn-a",
		TransportGeneration: 3,
		RouteRevision:       11,
		PresenceEpoch:       19,
		Phase:               phase,
		LeaseExpiresAt:      now.Add(30 * time.Second),
		FallbackExpiresAt:   now.Add(time.Hour),
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
	return presence
}

func exactLeaseRequest(presence *Presence, requested time.Duration) SubscriptionLeaseRequest {
	return SubscriptionLeaseRequest{
		SubscriptionID: presence.SubscriptionID,
		JoinAttemptID:  presence.JoinAttemptID,
		RequestedTTL:   requested,
		Fence: LeaveRequest{
			SubscriptionID:      presence.SubscriptionID,
			JoinAttemptID:       presence.JoinAttemptID,
			ViewerPlayerID:      presence.ViewerPlayerID,
			SessionEpoch:        presence.SessionEpoch,
			GatewayID:           presence.GatewayID,
			GatewayBootID:       presence.GatewayBootID,
			ConnectionID:        presence.ConnectionID,
			TransportGeneration: presence.TransportGeneration,
			RouteRevision:       presence.RouteRevision,
		},
	}
}

func TestRenewSubscriptionExactFenceAndTTLClamp(t *testing.T) {
	for _, phase := range []PresencePhase{
		PresenceJoining, PresenceFlushing, PresenceActivating, PresenceInFarm,
	} {
		t.Run(time.Duration(phase).String(), func(t *testing.T) {
			hub := NewSubscriptionHub(8)
			hub.SetSubscriptionLeaseConfig(20*time.Second, 25*time.Second)
			now := time.UnixMilli(1_900_000_000_000)
			presence := installLeasePresence(t, hub, 91, phase, now)
			result, err := hub.RenewSubscription(context.Background(), 91,
				exactLeaseRequest(presence, time.Minute), now.Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if result.Status != SubscriptionLeaseRenewed || result.AcceptedTTL != 25*time.Second {
				t.Fatalf("result=%+v", result)
			}
			wantExpiry := now.Add(26 * time.Second)
			if !presence.LeaseExpiresAt.Equal(wantExpiry) {
				t.Fatalf("lease expiry=%v want=%v", presence.LeaseExpiresAt, wantExpiry)
			}
		})
	}
}

func TestRenewSubscriptionRejectsStaleClosedAndExpired(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)

	t.Run("stale full fence", func(t *testing.T) {
		hub := NewSubscriptionHub(8)
		presence := installLeasePresence(t, hub, 92, PresenceInFarm, now)
		req := exactLeaseRequest(presence, 10*time.Second)
		req.Fence.TransportGeneration++
		result, err := hub.RenewSubscription(context.Background(), 92, req, now.Add(time.Second))
		if err != nil || result.Status != SubscriptionLeaseStaleFence {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		if !presence.LeaseExpiresAt.Equal(now.Add(30 * time.Second)) {
			t.Fatal("stale renew changed deadline")
		}
	})

	t.Run("terminal phase", func(t *testing.T) {
		hub := NewSubscriptionHub(8)
		presence := installLeasePresence(t, hub, 93, PresenceResyncRequired, now)
		result, err := hub.RenewSubscription(context.Background(), 93,
			exactLeaseRequest(presence, 10*time.Second), now.Add(time.Second))
		if err != nil || result.Status != SubscriptionLeaseClosed {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	})

	t.Run("expired cannot revive", func(t *testing.T) {
		hub := NewSubscriptionHub(8)
		presence := installLeasePresence(t, hub, 94, PresenceInFarm, now)
		presence.LeaseExpiresAt = now
		result, err := hub.RenewSubscription(context.Background(), 94,
			exactLeaseRequest(presence, 10*time.Second), now)
		if err != nil || result.Status != SubscriptionLeaseClosed {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		if count, countErr := hub.PresenceCount(context.Background(), 94); countErr != nil || count != 0 {
			t.Fatalf("presence count=%d err=%v", count, countErr)
		}
		late, lateErr := hub.RenewSubscription(context.Background(), 94,
			exactLeaseRequest(presence, 10*time.Second), now.Add(time.Second))
		if lateErr != nil || late.Status != SubscriptionLeaseNotFound {
			t.Fatalf("late result=%+v err=%v", late, lateErr)
		}
	})
}

func TestRenewSubscriptionDoesNotCreateFarmLane(t *testing.T) {
	hub := NewSubscriptionHub(8)
	request := SubscriptionLeaseRequest{
		SubscriptionID: "missing", JoinAttemptID: "missing-attempt", RequestedTTL: time.Second,
	}
	result, err := hub.RenewSubscription(context.Background(), 999, request, time.Now())
	if err != nil || result.Status != SubscriptionLeaseNotFound {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if got := hub.Stats().ActiveLanes; got != 0 {
		t.Fatalf("renew activated lane count=%d", got)
	}
}
