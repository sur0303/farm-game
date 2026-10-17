package subscription

import (
	"context"
	"testing"

	farmpb "farmgame/api/gen/farm"
)

func TestInvalidateIsLevelTriggeredAndMinNonZero(t *testing.T) {
	hub := NewSubscriptionHub(4)
	defer hub.Stop()
	ctx := context.Background()

	// 首次置位 from=100。
	gen, first, err := hub.InvalidateFarm(ctx, 77, 100, "lane_full")
	if err != nil || !first || gen != 1 {
		t.Fatalf("invalidate gen=%d first=%v err=%v", gen, first, err)
	}
	// 较晚版本重复置位:invalid_from 保持 100(min_non_zero),generation 仍递增。
	gen, first, err = hub.InvalidateFarm(ctx, 77, 150, "lane_full")
	if err != nil || first || gen != 2 {
		t.Fatalf("re-invalidate gen=%d first=%v err=%v", gen, first, err)
	}
	state, ok, err := hub.RealtimeState(ctx, 77)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if state.InvalidFromVersion != 100 || state.DeliveryGeneration != 2 {
		t.Fatalf("state=%+v, want invalid_from=100 gen=2", state)
	}
	// "Lane 恢复"(后续事件正常执行)不得自动清除失效(level-triggered)。
	seedPresence(hub, 77, testPresence("sub-1", 9001, PresenceInFarm, true))
	_, _ = hub.Execute(ctx, 77, nil, func() (any, error) { return nil, nil })
	state, _, _ = hub.RealtimeState(ctx, 77)
	if state.InvalidFromVersion != 100 {
		t.Fatalf("invalid cleared by lane recovery: %+v", state)
	}
	if hub.InvalidTotal() != 1 {
		t.Fatalf("invalid_total=%d, want 1(仅首次计数)", hub.InvalidTotal())
	}
}

func TestInstallBaselineClearsOnlyWithNewGenerationAndNoOldPresence(t *testing.T) {
	hub := NewSubscriptionHub(4)
	defer hub.Stop()
	ctx := context.Background()

	if _, _, err := hub.InvalidateFarm(ctx, 77, 100, "lane_full"); err != nil {
		t.Fatal(err)
	}
	// 旧代(gen=0)订阅仍在:不得清除。
	oldPresence := testPresence("sub-old", 9001, PresenceInFarm, true)
	oldPresence.DeliveryGeneration = 0
	seedPresence(hub, 77, oldPresence)
	cleared, err := hub.InstallBaseline(ctx, 77, 1)
	if err != nil || cleared {
		t.Fatalf("cleared=%v err=%v, want not cleared with old presence", cleared, err)
	}
	// 旧代终止后,新代基线安装 → 清除。
	if _, err := hub.CleanupPresence(ctx, 77, LeaveRequest{
		SubscriptionID: "sub-old", ViewerPlayerID: 9001, SessionEpoch: 1,
		GatewayID: "gate-a", GatewayBootID: "boot-a", ConnectionID: "conn-sub-old",
		TransportGeneration: 1, RouteRevision: 1,
	}); err != nil {
		t.Fatal(err)
	}
	newPresence := testPresence("sub-new", 9002, PresenceInFarm, true)
	newPresence.DeliveryGeneration = 1
	seedPresence(hub, 77, newPresence)
	cleared, err = hub.InstallBaseline(ctx, 77, 1)
	if err != nil || !cleared {
		t.Fatalf("cleared=%v err=%v, want cleared", cleared, err)
	}
	state, _, _ := hub.RealtimeState(ctx, 77)
	if state.InvalidFromVersion != 0 {
		t.Fatalf("state=%+v, want invalid cleared", state)
	}
	// 已清除后再次安装无副作用。
	if cleared, _ := hub.InstallBaseline(ctx, 77, 1); cleared {
		t.Fatal("double clear must be no-op")
	}
}

func TestMarkEnqueuedAdvancesLastVersion(t *testing.T) {
	hub := NewSubscriptionHub(4)
	defer hub.Stop()
	ctx := context.Background()
	if err := hub.MarkEnqueued(ctx, 77, 42); err != nil {
		t.Fatal(err)
	}
	if err := hub.MarkEnqueued(ctx, 77, 40); err != nil {
		t.Fatal(err)
	}
	state, ok, err := hub.RealtimeState(ctx, 77)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if state.LastEnqueuedVersion != 42 {
		t.Fatalf("last_enqueued=%d, want 42", state.LastEnqueuedVersion)
	}
}

// TestCommittedLaneFullMarksInvalidAndResyncs 是 §4.3 故障样板的 Hub 侧:
// 变化已提交但无法进入 Lane → 置失效 + 旧代订阅被移除要求重进。
// (Lane 满容量的机制覆盖在 actor 侧 TestCommittedPublishLaneFull...;
// 这里验证置失效与 Drain 的组合效果。)
func TestCommittedLaneFullMarksInvalidAndResyncs(t *testing.T) {
	hub := NewSubscriptionHub(1)
	defer hub.Stop()
	ctx := context.Background()
	seedPresence(hub, 77, testPresence("sub-1", 9001, PresenceInFarm, true))

	if _, _, err := hub.InvalidateFarm(ctx, 77, 43, "lane_full"); err != nil {
		t.Fatal(err)
	}
	removed, err := hub.DrainFarm(ctx, 77)
	if err != nil {
		t.Fatal(err)
	}

	if len(removed) != 1 || removed[0].SubscriptionID != "sub-1" {
		t.Fatalf("removed=%v, want sub-1", removed)
	}
	state, _, _ := hub.RealtimeState(ctx, 77)
	if state.InvalidFromVersion != 43 || state.DeliveryGeneration != 1 {
		t.Fatalf("state=%+v, want invalid_from=43 gen=1", state)
	}
}

func TestMarkSubscriptionResyncRequiredUsesExactFenceAndIsIdempotent(t *testing.T) {
	hub := NewSubscriptionHub(4)
	defer hub.Stop()
	ctx := context.Background()
	presence := testPresence("sub-exact", 9001, PresenceInFarm, true)
	presence.JoinAttemptID = "attempt-exact"
	presence.Buffer = []*farmpb.FarmChange{changeAt(11)}
	presence.BufferBytes = 10
	presence.PostReadyBuffer = []*farmpb.FarmChange{changeAt(12)}
	presence.PostReadyBufferBytes = 10
	seedPresence(hub, 77, presence)

	fence := LeaveRequest{
		SubscriptionID: presence.SubscriptionID, JoinAttemptID: presence.JoinAttemptID,
		ViewerPlayerID: presence.ViewerPlayerID, SessionEpoch: presence.SessionEpoch,
		GatewayID: presence.GatewayID, GatewayBootID: presence.GatewayBootID,
		ConnectionID: presence.ConnectionID, TransportGeneration: presence.TransportGeneration,
		RouteRevision: presence.RouteRevision, PresenceEpoch: presence.PresenceEpoch,
	}
	stale := fence
	stale.SessionEpoch++
	if notice, matched, err := hub.MarkSubscriptionResyncRequired(ctx, 77, stale, "queue_full", 12, 3); err != nil || matched || notice != nil {
		t.Fatalf("stale mark notice=%v matched=%v err=%v", notice, matched, err)
	}

	notice, matched, err := hub.MarkSubscriptionResyncRequired(ctx, 77, fence, "queue_full", 12, 3)
	if err != nil || !matched || notice == nil {
		t.Fatalf("exact mark notice=%v matched=%v err=%v", notice, matched, err)
	}
	if notice.Presence.Phase != PresenceResyncRequired || len(notice.Presence.Buffer) != 0 || len(notice.Presence.PostReadyBuffer) != 0 {
		t.Fatalf("terminal presence=%+v", notice.Presence)
	}
	repeated, matched, err := hub.MarkSubscriptionResyncRequired(ctx, 77, fence, "queue_full", 12, 3)
	if err != nil || !matched || repeated == nil || repeated.Presence.Phase != PresenceResyncRequired {
		t.Fatalf("repeated mark notice=%v matched=%v err=%v", repeated, matched, err)
	}
}
