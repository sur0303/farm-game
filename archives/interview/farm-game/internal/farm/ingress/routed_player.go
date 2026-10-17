package ingress

import (
	"context"
	"errors"
	"fmt"

	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
)

type RoutedPlayerParticipant struct {
	realm   string
	routes  *actorrouting.ActorRouteClient
	clients map[string]logicrpcpb.PlayerParticipantServiceClient
}

func NewRoutedPlayerParticipant(realm string, routes *actorrouting.ActorRouteClient, clients map[string]logicrpcpb.PlayerParticipantServiceClient) (*RoutedPlayerParticipant, error) {
	if realm == "" || routes == nil || len(clients) == 0 {
		return nil, errors.New("farm ingress: routed player participant configuration required")
	}
	return &RoutedPlayerParticipant{realm: realm, routes: routes, clients: clients}, nil
}

func (p *RoutedPlayerParticipant) client(ctx context.Context, playerID uint64) (context.Context, logicrpcpb.PlayerParticipantServiceClient, error) {
	address := actorrouting.Address{Realm: p.realm, Kind: actorrouting.KindPlayer, ID: playerID}
	assignment, err := p.routes.Resolve(ctx, address)
	if err != nil {
		return ctx, nil, err
	}
	client := p.clients[assignment.Endpoint]
	if client == nil {
		return ctx, nil, fmt.Errorf("%w: player endpoint %s", actorrouting.ErrNoAssignment, assignment.Endpoint)
	}
	return actorrouting.WithRoute(ctx, address, assignment), client, nil
}

func (p *RoutedPlayerParticipant) ReserveItem(ctx context.Context, req *logicrpcpb.ReserveItemRequest) (*logicrpcpb.ReservationResponse, error) {
	ctx, client, err := p.client(ctx, req.GetPlayerId())
	if err != nil {
		return nil, err
	}
	return client.ReserveItem(ctx, req)
}

func (p *RoutedPlayerParticipant) CommitReservation(ctx context.Context, req *logicrpcpb.CommitReservationRequest) (*logicrpcpb.ReservationResponse, error) {
	ctx, client, err := p.client(ctx, req.GetPlayerId())
	if err != nil {
		return nil, err
	}
	return client.CommitReservation(ctx, req)
}

func (p *RoutedPlayerParticipant) ReleaseReservation(ctx context.Context, req *logicrpcpb.ReleaseReservationRequest) (*logicrpcpb.ReservationResponse, error) {
	ctx, client, err := p.client(ctx, req.GetPlayerId())
	if err != nil {
		return nil, err
	}
	return client.ReleaseReservation(ctx, req)
}

func (p *RoutedPlayerParticipant) GrantItem(ctx context.Context, req *logicrpcpb.GrantItemRequest) (*logicrpcpb.GrantItemResponse, error) {
	ctx, client, err := p.client(ctx, req.GetPlayerId())
	if err != nil {
		return nil, err
	}
	return client.GrantItem(ctx, req)
}

func (p *RoutedPlayerParticipant) ApplyReward(ctx context.Context, req *logicrpcpb.ApplyRewardRequest) (*logicrpcpb.ApplyRewardResponse, error) {
	ctx, client, err := p.client(ctx, req.GetPlayerId())
	if err != nil {
		return nil, err
	}
	return client.ApplyReward(ctx, req)
}

func (p *RoutedPlayerParticipant) GetPrivateState(ctx context.Context, req *logicrpcpb.GetPrivateStateRequest) (*logicrpcpb.GetPrivateStateResponse, error) {
	ctx, client, err := p.client(ctx, req.GetPlayerId())
	if err != nil {
		return nil, err
	}
	return client.GetPrivateState(ctx, req)
}

func (p *RoutedPlayerParticipant) GetPublicProfile(ctx context.Context, req *logicrpcpb.GetPublicProfileRequest) (*logicrpcpb.GetPublicProfileResponse, error) {
	ctx, client, err := p.client(ctx, req.GetPlayerId())
	if err != nil {
		return nil, err
	}
	return client.GetPublicProfile(ctx, req)
}
