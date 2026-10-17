package auth

import (
	"context"

	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
)

type grpcBootstrapper struct {
	client logicrpcpb.BootstrapServiceClient
}

func NewGRPCBootstrapper(client logicrpcpb.BootstrapServiceClient, _ int32) Bootstrapper {
	if client == nil {
		return nil
	}
	return &grpcBootstrapper{client: client}
}

func (b *grpcBootstrapper) EnsureOwnerInitialized(ctx context.Context, playerID uint64, bootstrapID string, _ int32) (*publicpb.GetPlayerBootstrapResult, error) {
	resp, err := b.client.EnsureOwnerInitialized(ctx, &logicrpcpb.EnsureOwnerInitializedRequest{PlayerId: playerID, BootstrapId: bootstrapID})
	if err != nil {
		return nil, err
	}
	return resp.GetHomeBootstrap(), nil
}

func (b *grpcBootstrapper) EnsureOwnerInitializedWithProfile(ctx context.Context, playerID uint64, bootstrapID, displayName string, avatarID uint32, _ int32) (*publicpb.GetPlayerBootstrapResult, error) {
	resp, err := b.client.EnsureOwnerInitialized(ctx, &logicrpcpb.EnsureOwnerInitializedRequest{
		PlayerId: playerID, BootstrapId: bootstrapID, DisplayName: displayName, AvatarId: avatarID,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetHomeBootstrap(), nil
}
