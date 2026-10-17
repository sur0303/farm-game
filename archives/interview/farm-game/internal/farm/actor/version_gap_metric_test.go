package actor

import (
	"testing"
	"time"
)

// The capacity oracle reads farm_change_version_gap_total as a hard gate, so a
// resync caused by buffer capacity must not raise it. Only a break in the
// delivered version sequence counts.
func TestVersionGapCountsTheGapAndNotTheOverflow(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	tests := []struct {
		name    string
		setup   func(actor *FarmActor, now time.Time)
		reason  string
		wantGap uint64
	}{
		{
			name: "delivery gap",
			setup: func(actor *FarmActor, now time.Time) {
				actor.record.Core.FarmVersion = 3
				join(t, actor, "sub-gap", now)
				actor.presence["sub-gap"].LastPushedVersion = 1
			},
			reason:  "farm_change_version_gap",
			wantGap: 1,
		},
		{
			name: "buffer overflow",
			setup: func(actor *FarmActor, now time.Time) {
				actor.maxBufferItems = 0
				actor.record.Core.FarmVersion = 1
				join(t, actor, "sub-full", now)
			},
			reason:  "joining_buffer_items_exceeded",
			wantGap: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hub := NewSubscriptionHub(8)
			actor := &FarmActor{
				farmID: 21, record: InitialRecord(21), subscriptionHub: hub,
				presence: make(map[string]*Presence), presenceByPlayer: make(map[uint64]string),
				maxSubscriptions: defaultMaxSubscriptions, maxBufferItems: 64,
				maxBufferBytes: defaultMaxJoinBufferBytes, maxJoinDuration: defaultMaxJoinDuration,
			}
			hub.Attach(21, actor)
			tt.setup(actor, now)

			_, _, notices, err := actor.publish(actor.record.Core.Plots[0], now)
			if err != nil || len(notices) != 1 || notices[0].Reason != tt.reason {
				t.Fatalf("publish err=%v notices=%+v, want reason %s", err, notices, tt.reason)
			}
			if got := hub.VersionGaps(); got != tt.wantGap {
				t.Fatalf("VersionGaps()=%d after %s, want %d", got, tt.reason, tt.wantGap)
			}
		})
	}
}

func join(t *testing.T, actor *FarmActor, subscriptionID string, now time.Time) {
	t.Helper()
	if _, err := actor.Join(JoinRequest{
		SubscriptionID: subscriptionID, ViewerPlayerID: 1, SessionEpoch: 1,
		GatewayID: "gate", GatewayBootID: "boot", ConnectionID: "conn",
		TransportGeneration: 1, RouteRevision: 1,
	}, now); err != nil {
		t.Fatalf("Join(%s): %v", subscriptionID, err)
	}
}
