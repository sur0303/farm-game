package auth

import (
	"context"
	"errors"
	"fmt"

	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"
)

type ShardedBootstrapper struct {
	realm   string
	routes  *actorrouting.ActorRouteClient
	clients map[string]logicrpcpb.BootstrapServiceClient
}

func NewShardedBootstrapper(realm string, routes *actorrouting.ActorRouteClient, clients map[string]logicrpcpb.BootstrapServiceClient) (*ShardedBootstrapper, error) {
	if realm == "" || routes == nil || len(clients) == 0 {
		return nil, errors.New("auth: sharded bootstrap configuration required")
	}
	return &ShardedBootstrapper{realm: realm, routes: routes, clients: clients}, nil
}

func (b *ShardedBootstrapper) EnsureOwnerInitialized(ctx context.Context, playerID uint64, bootstrapID string, initialRuleVersion int32) (*publicpb.GetPlayerBootstrapResult, error) {
	return b.ensureOwnerInitialized(ctx, playerID, bootstrapID, "", 0, initialRuleVersion)
}

func (b *ShardedBootstrapper) EnsureOwnerInitializedWithProfile(ctx context.Context, playerID uint64, bootstrapID, displayName string, avatarID uint32, initialRuleVersion int32) (*publicpb.GetPlayerBootstrapResult, error) {
	return b.ensureOwnerInitialized(ctx, playerID, bootstrapID, displayName, avatarID, initialRuleVersion)
}

func (b *ShardedBootstrapper) ensureOwnerInitialized(ctx context.Context, playerID uint64, bootstrapID, displayName string, avatarID uint32, _ int32) (*publicpb.GetPlayerBootstrapResult, error) {
	address := actorrouting.Address{Realm: b.realm, Kind: actorrouting.KindPlayer, ID: playerID}
	assignment, err := b.routes.Resolve(ctx, address)
	if err != nil {
		return nil, err
	}
	client := b.clients[assignment.Endpoint]
	if client == nil {
		return nil, fmt.Errorf("%w: bootstrap endpoint %s", actorrouting.ErrNoAssignment, assignment.Endpoint)
	}
	resp, err := client.EnsureOwnerInitialized(actorrouting.WithRoute(ctx, address, assignment), &logicrpcpb.EnsureOwnerInitializedRequest{
		PlayerId: playerID, BootstrapId: bootstrapID, DisplayName: displayName, AvatarId: avatarID,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetHomeBootstrap(), nil
}
