package subscription

import (
	"context"
	"errors"
	"testing"
	"time"

	farmpb "farmgame/api/gen/farm"
)

func testPresence(subscriptionID string, viewer uint64, phase PresencePhase, armed bool) *Presence {
	return &Presence{
		JoinOperationID: "op-" + subscriptionID, SubscriptionID: subscriptionID,
		ViewerPlayerID: viewer, SessionEpoch: 1,
		GatewayID: "gate-a", GatewayBootID: "boot-a", ConnectionID: "conn-" + subscriptionID,
		TransportGeneration: 1, RouteRevision: 1, PresenceEpoch: 1,
		SnapshotVersion: 10, LastPushedVersion: 10,
		Phase: phase, Armed: armed,
		JoiningStartedAt: time.Now(), FallbackExpiresAt: time.Now().Add(time.Hour),
	}
}

func TestW2QueueAcceptedCallbacksAreIdempotent(t *testing.T) {
	hub := NewSubscriptionHub(4)
	defer hub.Stop()
	ctx := context.Background()
	presence := testPresence("sub-w2", 9001, PresenceJoining, true)
	presence.JoinAttemptID = "attempt-w2"
	presence.Buffer = []*farmpb.FarmChange{changeAt(11), changeAt(12)}
	presence.LastPushedVersion = 12
	seedPresence(hub, 77, presence)
	fence := JoinControlFence{
		SubscriptionID: presence.SubscriptionID, JoinAttemptID: presence.JoinAttemptID,
		ViewerPlayerID: presence.ViewerPlayerID, SessionEpoch: presence.SessionEpoch,
		GatewayID: presence.GatewayID, GatewayBootID: presence.GatewayBootID,
		ConnectionID: presence.ConnectionID, TransportGeneration: presence.TransportGeneration,
		RouteRevision: presence.RouteRevision,
	}

	changes, cut, activating, epoch, err := hub.SnapshotQueueAccepted(ctx, 77, fence, 10)
	if err != nil || cut != 12 || activating || len(changes) != 2 {
		t.Fatalf("snapshot ack changes=%d cut=%d activating=%v err=%v", len(changes), cut, activating, err)
	}
	if epoch != presence.PresenceEpoch {
		t.Fatalf("snapshot ack epoch=%d, want presence epoch %d", epoch, presence.PresenceEpoch)
	}
	changes, replayCut, _, _, err := hub.SnapshotQueueAccepted(ctx, 77, fence, 10)
	if !errors.Is(err, ErrJoinCallbackAlreadyApplied) || replayCut != 12 || len(changes) != 0 {
		t.Fatalf("snapshot replay changes=%d cut=%d err=%v", len(changes), replayCut, err)
	}

	activating, epoch, err = hub.CatchupBatchQueueAccepted(ctx, 77, fence, 11, 12)
	if err != nil || !activating {
		t.Fatalf("catchup ack activating=%v err=%v", activating, err)
	}
	if epoch != presence.PresenceEpoch {
		t.Fatalf("catchup ack epoch=%d, want presence epoch %d", epoch, presence.PresenceEpoch)
	}
	activating, _, err = hub.CatchupBatchQueueAccepted(ctx, 77, fence, 11, 12)
	if !errors.Is(err, ErrJoinCallbackAlreadyApplied) || !activating {
		t.Fatalf("catchup replay activating=%v err=%v", activating, err)
	}

	postReady, _, epoch, err := hub.ReadyQueueAccepted(ctx, 77, fence, 12)
	if err != nil || len(postReady) != 0 {
		t.Fatalf("ready ack post_ready=%d err=%v", len(postReady), err)
	}
	if epoch != presence.PresenceEpoch {
		t.Fatalf("ready ack epoch=%d, want presence epoch %d", epoch, presence.PresenceEpoch)
	}
	postReady, _, _, err = hub.ReadyQueueAccepted(ctx, 77, fence, 12)
	if !errors.Is(err, ErrJoinCallbackAlreadyApplied) || len(postReady) != 0 {
		t.Fatalf("ready replay post_ready=%d err=%v", len(postReady), err)
	}
}

func seedPresence(h *SubscriptionHub, farmID uint64, presence *Presence) {
	_, _ = h.ExecuteSystem(context.Background(), farmID, nil, func() (any, error) { return nil, nil })
	lane := h.liveLane(farmID)
	_, _ = lane.execute(context.Background(), true, func(state *subscriptionState) (any, error) {
		state.presence[presence.SubscriptionID] = presence
		state.presenceByPlayer[presence.ViewerPlayerID] = presence.SubscriptionID
		return nil, nil
	})
}

func changeAt(version uint64) *farmpb.FarmChange {
	return &farmpb.FarmChange{OwnerId: 77, FarmVersion: version}
}

func TestArmSubscriptionTransitions(t *testing.T) {
	hub := NewSubscriptionHub(4)
	defer hub.Stop()
	ctx := context.Background()
	seedPresence(hub, 77, testPresence("sub-1", 9001, PresenceJoining, false))

	armed, err := hub.ArmSubscription(ctx, 77, "sub-1")
	if err != nil || !armed {
		t.Fatalf("arm=%v err=%v, want armed", armed, err)
	}
	// 重复 Arm 必须报非法迁移,不得静默成功。
	if _, err := hub.ArmSubscription(ctx, 77, "sub-1"); err == nil {
		t.Fatal("re-arm must fail as invalid transition")
	}
	// 不存在的订阅幂等返回 false。
	if found, err := hub.ArmSubscription(ctx, 77, "sub-missing"); err != nil || found {
		t.Fatalf("missing arm found=%v err=%v", found, err)
	}
}

func TestFreezeReadyCutSplitsBuffer(t *testing.T) {
	hub := NewSubscriptionHub(4)
	defer hub.Stop()
	ctx := context.Background()
	presence := testPresence("sub-1", 9001, PresenceJoining, true)
	// 已捕获 v11..v15;在 N=13 冻结。
	presence.Buffer = []*farmpb.FarmChange{changeAt(11), changeAt(12), changeAt(13), changeAt(14), changeAt(15)}
	presence.LastPushedVersion = 15
	seedPresence(hub, 77, presence)

	catchUp, err := hub.FreezeReadyCut(ctx, 77, "sub-1", 13)
	if err != nil {
		t.Fatal(err)
	}
	if len(catchUp) != 3 || catchUp[0].FarmVersion != 11 || catchUp[2].FarmVersion != 13 {
		t.Fatalf("catch_up=%v, want v11..v13", catchUp)
	}
	lane := hub.liveLane(77)
	value, _ := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		p := state.presence["sub-1"]
		return []any{p.Phase, p.ReadyCutVersion, len(p.PostReadyBuffer), p.PostReadyLastVersion}, nil
	})
	got := value.([]any)
	if got[0].(PresencePhase) != PresenceFlushing || got[1].(uint64) != 13 ||
		got[2].(int) != 2 || got[3].(uint64) != 15 {
		t.Fatalf("state=%v, want FLUSHING cut=13 post_ready=2 last=15", got)
	}
	// 二次冻结必须失败。
	if _, err := hub.FreezeReadyCut(ctx, 77, "sub-1", 13); err == nil {
		t.Fatal("re-freeze must fail")
	}
}

func TestActivatingAndInFarmDrainPostReady(t *testing.T) {
	hub := NewSubscriptionHub(4)
	defer hub.Stop()
	ctx := context.Background()
	presence := testPresence("sub-1", 9001, PresenceJoining, true)
	presence.Buffer = []*farmpb.FarmChange{changeAt(11), changeAt(12)}
	presence.LastPushedVersion = 12
	seedPresence(hub, 77, presence)

	catchUp, err := hub.FreezeReadyCut(ctx, 77, "sub-1", 12)
	if err != nil || len(catchUp) != 2 {
		t.Fatalf("catchUp=%v err=%v", catchUp, err)
	}
	// 模拟 catch-up 下发:清空 catch-up 缓冲。
	lane := hub.liveLane(77)
	_, _ = lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		p := state.presence["sub-1"]
		p.Buffer = nil
		p.BufferBytes = 0
		// 冻结后 v13 到达,应进 post-ready(模拟 publish 路径的等价效果)。
		p.PostReadyBuffer = append(p.PostReadyBuffer, changeAt(13))
		p.PostReadyLastVersion = 13
		return nil, nil
	})
	// catch-up 未 drain 完时不得进入 ACTIVATING。
	if _, err := hub.MarkActivating(ctx, 77, "sub-1", 11); err == nil {
		t.Fatal("mark activating with wrong ready_through must fail")
	}
	ok, err := hub.MarkActivating(ctx, 77, "sub-1", 12)
	if err != nil || !ok {
		t.Fatalf("activating ok=%v err=%v", ok, err)
	}
	postReady, err := hub.MarkInFarm(ctx, 77, "sub-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(postReady) != 1 || postReady[0].FarmVersion != 13 {
		t.Fatalf("post_ready=%v, want [v13]", postReady)
	}
	value, _ := lane.execute(ctx, true, func(state *subscriptionState) (any, error) {
		p := state.presence["sub-1"]
		return []any{p.Phase, p.LastPushedVersion}, nil
	})
	got := value.([]any)
	if got[0].(PresencePhase) != PresenceInFarm || got[1].(uint64) != 13 {
		t.Fatalf("state=%v, want IN_FARM last=13", got)
	}
}

func TestStateCountsByPhase(t *testing.T) {
	hub := NewSubscriptionHub(4)
	defer hub.Stop()
	ctx := context.Background()
	seedPresence(hub, 77, testPresence("sub-u", 9001, PresenceJoining, false))
	seedPresence(hub, 77, testPresence("sub-a", 9002, PresenceJoining, true))
	seedPresence(hub, 78, testPresence("sub-i", 9003, PresenceInFarm, true))

	counts := hub.StateCounts(ctx)
	if counts.JoiningUnarmed != 1 || counts.JoiningArmed != 1 || counts.InFarm != 1 {
		t.Fatalf("counts=%+v", counts)
	}
}
