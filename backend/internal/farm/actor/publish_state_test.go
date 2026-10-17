package actor

import (
	"testing"
	"time"

	farmpb "farmgame/api/gen/farm"
)

// TestPublishSkipsUnarmedSubscription 是 26 HC-ACCESS-37 的负向断言:
// 未 Arm 的订阅不得捕获任何 Change,也不得建立捕获游标;
// armed 订阅不受影响,正常捕获。
func TestPublishSkipsUnarmedSubscription(t *testing.T) {
	hub := NewSubscriptionHub(8)
	defer hub.Stop()
	actor := &FarmActor{
		farmID: 88, record: InitialRecord(88), activationEpoch: 3, subscriptionHub: hub,
		maxSubscriptions: defaultMaxSubscriptions, maxBufferItems: 64,
		maxBufferBytes: defaultMaxJoinBufferBytes, maxJoinDuration: defaultMaxJoinDuration,
	}
	hub.Attach(88, actor)
	now := time.Now()
	actor.record.Core.FarmVersion = 10

	// 直接注入两个订阅:unarmed 与 armed(快照版本同为 10)。
	unarmed := &Presence{
		JoinOperationID: "op-u", SubscriptionID: "sub-u", ViewerPlayerID: 9001,
		SessionEpoch: 1, GatewayID: "gw", GatewayBootID: "boot", ConnectionID: "c1",
		TransportGeneration: 1, RouteRevision: 1, PresenceEpoch: 1,
		SnapshotVersion: 10, LastPushedVersion: 10, Phase: PresenceJoining, Armed: false,
		JoiningStartedAt: now, FallbackExpiresAt: now.Add(time.Hour),
	}
	armed := &Presence{
		JoinOperationID: "op-a", SubscriptionID: "sub-a", ViewerPlayerID: 9002,
		SessionEpoch: 1, GatewayID: "gw", GatewayBootID: "boot", ConnectionID: "c2",
		TransportGeneration: 1, RouteRevision: 1, PresenceEpoch: 2,
		SnapshotVersion: 10, LastPushedVersion: 10, Phase: PresenceJoining, Armed: true,
		JoiningStartedAt: now, FallbackExpiresAt: now.Add(time.Hour),
	}
	actor.presence["sub-u"] = unarmed
	actor.presence["sub-a"] = armed

	actor.record.Core.FarmVersion = 11
	change, recipients, resyncs, err := actor.publish(actor.record.Core.Plots[0], now)
	if err != nil {
		t.Fatal(err)
	}
	if change == nil || change.FarmVersion != 11 {
		t.Fatalf("change=%v", change)
	}
	if len(recipients) != 0 || len(resyncs) != 0 {
		t.Fatalf("recipients=%v resyncs=%v, want none", recipients, resyncs)
	}
	if len(unarmed.Buffer) != 0 || unarmed.LastPushedVersion != 10 {
		t.Fatalf("unarmed captured change: buffer=%d last=%d, want empty/10",
			len(unarmed.Buffer), unarmed.LastPushedVersion)
	}
	if len(armed.Buffer) != 1 || armed.LastPushedVersion != 11 {
		t.Fatalf("armed buffer=%d last=%d, want 1/11", len(armed.Buffer), armed.LastPushedVersion)
	}
}

// TestPublishFrozenCutRoutesToPostReady 验证:ready_cut_version=N 冻结后,
// version<=N 的变化继续 catch-up 缓冲,version>N 的变化进入 post-ready 缓冲
// 且保持连续(26 HC-ACCESS-25/39)。
func TestPublishFrozenCutRoutesToPostReady(t *testing.T) {
	hub := NewSubscriptionHub(8)
	defer hub.Stop()
	actor := &FarmActor{
		farmID: 88, record: InitialRecord(88), activationEpoch: 3, subscriptionHub: hub,
		maxSubscriptions: defaultMaxSubscriptions, maxBufferItems: 64,
		maxBufferBytes: defaultMaxJoinBufferBytes, maxJoinDuration: defaultMaxJoinDuration,
	}
	hub.Attach(88, actor)
	now := time.Now()
	actor.record.Core.FarmVersion = 11

	presence := &Presence{
		JoinOperationID: "op-f", SubscriptionID: "sub-f", ViewerPlayerID: 9003,
		SessionEpoch: 1, GatewayID: "gw", GatewayBootID: "boot", ConnectionID: "c3",
		TransportGeneration: 1, RouteRevision: 1, PresenceEpoch: 3,
		SnapshotVersion: 10, LastPushedVersion: 11, Phase: PresenceFlushing, Armed: true,
		ReadyCutVersion: 12, ReadyCutFrozen: true,
		Buffer:           []*farmpb.FarmChange{{OwnerId: 88, FarmVersion: 11}},
		JoiningStartedAt: now, FallbackExpiresAt: now.Add(time.Hour),
	}
	actor.presence["sub-f"] = presence

	// v12 <= N:继续进 catch-up 缓冲。
	actor.record.Core.FarmVersion = 12
	if _, _, _, err := actor.publish(actor.record.Core.Plots[0], now); err != nil {
		t.Fatal(err)
	}
	if len(presence.Buffer) != 2 || presence.LastPushedVersion != 12 {
		t.Fatalf("buffer=%d last=%d, want 2/12", len(presence.Buffer), presence.LastPushedVersion)
	}
	// v13 > N:进入 post-ready,不得混入 catch-up。
	actor.record.Core.FarmVersion = 13
	if _, _, _, err := actor.publish(actor.record.Core.Plots[0], now); err != nil {
		t.Fatal(err)
	}
	if len(presence.Buffer) != 2 {
		t.Fatalf("catch-up buffer changed: %d", len(presence.Buffer))
	}
	if len(presence.PostReadyBuffer) != 1 || presence.PostReadyLastVersion != 13 {
		t.Fatalf("post_ready=%d last=%d, want 1/13",
			len(presence.PostReadyBuffer), presence.PostReadyLastVersion)
	}
}

// TestPostReadyBufferOverflowInvalidates 验证:post-ready 缓冲满时订阅进入
// 失效(Resync)路径而不是无限增长(W1A 故障样板)。
func TestPostReadyBufferOverflowInvalidates(t *testing.T) {
	hub := NewSubscriptionHub(8)
	defer hub.Stop()
	actor := &FarmActor{
		farmID: 88, record: InitialRecord(88), activationEpoch: 3, subscriptionHub: hub,
		maxSubscriptions: defaultMaxSubscriptions, maxBufferItems: 1,
		maxBufferBytes: defaultMaxJoinBufferBytes, maxJoinDuration: defaultMaxJoinDuration,
	}
	hub.Attach(88, actor)
	now := time.Now()
	actor.record.Core.FarmVersion = 12

	presence := &Presence{
		JoinOperationID: "op-o", SubscriptionID: "sub-o", ViewerPlayerID: 9004,
		SessionEpoch: 1, GatewayID: "gw", GatewayBootID: "boot", ConnectionID: "c4",
		TransportGeneration: 1, RouteRevision: 1, PresenceEpoch: 4,
		SnapshotVersion: 10, Phase: PresenceActivating, Armed: true,
		ReadyCutVersion: 11, ReadyCutFrozen: true,
		JoiningStartedAt: now, FallbackExpiresAt: now.Add(time.Hour),
	}
	actor.presence["sub-o"] = presence

	// v12 进 post-ready(容量 1);v13 溢出 → 订阅失效并产生 Resync。
	if _, _, _, err := actor.publish(actor.record.Core.Plots[0], now); err != nil {
		t.Fatal(err)
	}
	if len(presence.PostReadyBuffer) != 1 {
		t.Fatalf("post_ready=%d, want 1", len(presence.PostReadyBuffer))
	}
	actor.record.Core.FarmVersion = 13
	_, _, resyncs, err := actor.publish(actor.record.Core.Plots[0], now)
	if err != nil {
		t.Fatal(err)
	}
	if len(resyncs) != 1 || resyncs[0].Reason != "post_ready_buffer_items_exceeded" {
		t.Fatalf("resyncs=%v, want 1 post_ready overflow", resyncs)
	}
	if got := actor.presence["sub-o"]; got == nil || got.Phase != PresenceResyncRequired {
		t.Fatalf("overflowed subscription=%+v, want terminal fence", got)
	}
	if hub.JoiningBufferDropped() != 1 {
		t.Fatalf("dropped=%d, want 1", hub.JoiningBufferDropped())
	}
}
