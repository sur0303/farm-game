package auth

import (
	"context"
	"testing"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// recordingBootstrapClient returns a fixed home projection.
type recordingBootstrapClient struct {
	view *publicpb.GetPlayerBootstrapResult
}

func (c *recordingBootstrapClient) EnsureOwnerInitialized(ctx context.Context, req *logicrpcpb.EnsureOwnerInitializedRequest, _ ...grpc.CallOption) (*logicrpcpb.EnsureOwnerInitializedResponse, error) {
	return &logicrpcpb.EnsureOwnerInitializedResponse{
		Owner:         &logicrpcpb.OwnerReady{OwnerId: req.PlayerId, RecordVersion: 11},
		HomeBootstrap: c.view,
	}, nil
}

func shardedBootstrapFixture(t *testing.T, client logicrpcpb.BootstrapServiceClient) *ShardedBootstrapper {
	t.Helper()
	snapshot := actorrouting.RouteSnapshot{
		Realm: "classic-farm", GlobalRevision: 1,
		HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 0xA9F00D20260817,
		Player: actorrouting.DomainSnapshot{
			ShardCount: 1,
			Routes: []actorrouting.ShardRoute{
				{HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli()},
			},
		},
		Farm: actorrouting.DomainSnapshot{
			ShardCount: 1,
			Routes: []actorrouting.ShardRoute{
				{HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: time.Now().Add(time.Hour).UnixMilli()},
			},
		},
		Holders: []actorrouting.HolderRef{
			{InstanceID: "player-a", BootID: "player-a-boot", Endpoint: "player-a:9100", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-a", BootID: "farm-a-boot", Endpoint: "farm-a:9090", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
	resolver, err := actorrouting.NewStaticResolver(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := actorrouting.NewActorRouteClient(resolver, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := NewShardedBootstrapper("classic-farm", routes, map[string]logicrpcpb.BootstrapServiceClient{"player-a:9100": client})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

// TestShardedBootstrapperAttachesHomeBootstrap pins that the sharded login
// path forwards the backend-attached projection into LoginResult.
func TestShardedBootstrapperAttachesHomeBootstrap(t *testing.T) {
	view := &publicpb.GetPlayerBootstrapResult{
		PlayerStateRevision: 11,
		Profile:             &publicpb.PlayerPrivateProfileView{PlayerId: 7, DisplayName: "sharded"},
	}
	svc := shardedBootstrapFixture(t, &recordingBootstrapClient{view: view})
	got, err := svc.EnsureOwnerInitialized(context.Background(), 7, "boot-7", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, view) {
		t.Fatalf("projection=%+v want %+v", got, view)
	}
}

// TestShardedBootstrapperOmitsProjectionWithoutBackendAttach covers the legacy
// server path: no home_bootstrap field means the client falls back to the
// GET_PLAYER_BOOTSTRAP RecoveryRequery.
func TestShardedBootstrapperOmitsProjectionWithoutBackendAttach(t *testing.T) {
	svc := shardedBootstrapFixture(t, &recordingBootstrapClient{view: nil})
	got, err := svc.EnsureOwnerInitialized(context.Background(), 7, "boot-7", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("projection=%+v want nil", got)
	}
}
