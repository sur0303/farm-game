package ingress

import (
	"context"
	"errors"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
)

type farmProfileReaderFunc func(context.Context, uint64) (*publicpb.PublicProfileView, error)

func (f farmProfileReaderFunc) Read(ctx context.Context, playerID uint64) (*publicpb.PublicProfileView, error) {
	return f(ctx, playerID)
}

func TestArmSnapshotProfileFailureCleansExactJoiningPresenceWithoutDownlink(t *testing.T) {
	stack := newTestStack(t)
	capture := &w2aCaptureDownlink{}
	stack.farm.SetDownlinkEnqueuer(capture)
	stack.farm.SetProfileReader(farmProfileReaderFunc(func(context.Context, uint64) (*publicpb.PublicProfileView, error) {
		return nil, errors.New("profile timeout")
	}))
	trusted := joinTrusted(9, "profile-failure-conn")
	response, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command: typedCommand(t, "profile-failure-join", "profile-failure-request", commonpb.CommandType_JOIN_FARM,
			&publicpb.JoinFarmRequest{TargetFarmId: 9}),
		Trusted: trusted, Dispatch: farmActorDispatch(9),
	})
	if err != nil || response.GetBusiness().GetStatus() != commonpb.CommandStatus_ACCEPTED {
		t.Fatalf("join response=%v err=%v", response, err)
	}
	metadata := response.GetBusiness().GetJoinAccepted()
	if metadata == nil {
		t.Fatalf("join metadata=%v", metadata)
	}
	target := &logicrpcpb.DownlinkTargetFence{
		TargetPlayerId: trusted.Session.PlayerId, GatewayId: trusted.Session.GatewayId,
		GatewayBootId: trusted.Session.GatewayBootId, ConnectionId: trusted.Session.ConnectionId,
		ConnectionGeneration: trusted.Session.ConnectionGeneration, SessionEpoch: trusted.Session.ActiveSessionEpoch,
		SessionRouteRevision: trusted.Session.SessionRouteRevision,
	}
	arm, err := stack.farm.ArmSnapshot(context.Background(), &logicrpcpb.ArmSnapshotRequest{
		Subscription: &logicrpcpb.SubscriptionFence{FarmId: 9, SubscriptionId: metadata.SubscriptionId, JoinAttemptId: metadata.JoinAttemptId},
		Target:       target,
	})
	if err != nil || arm.GetStatus() != logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_UNAVAILABLE {
		t.Fatalf("arm=%v err=%v", arm, err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, ok := presencePhase(t, stack, 9, metadata.SubscriptionId); !ok {
			capture.mu.Lock()
			items := len(capture.items)
			capture.mu.Unlock()
			if items != 0 {
				t.Fatalf("profile failure downlinked items=%d", items)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("joining presence was not cleaned")
}
