package workflow

import (
	"context"
	"errors"
	"fmt"

	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
)

type RoutedFarmParticipant struct {
	realm   string
	routes  *actorrouting.ActorRouteClient
	clients map[string]logicrpcpb.FarmWorkflowServiceClient
}

func NewRoutedFarmParticipant(realm string, routes *actorrouting.ActorRouteClient, clients map[string]logicrpcpb.FarmWorkflowServiceClient) (*RoutedFarmParticipant, error) {
	if realm == "" || routes == nil || len(clients) == 0 {
		return nil, errors.New("workflow: routed farm participant configuration required")
	}
	return &RoutedFarmParticipant{realm: realm, routes: routes, clients: clients}, nil
}

func (p *RoutedFarmParticipant) client(ctx context.Context, farmID uint64) (context.Context, logicrpcpb.FarmWorkflowServiceClient, error) {
	address := actorrouting.Address{Realm: p.realm, Kind: actorrouting.KindFarm, ID: farmID}
	assignment, err := p.routes.Resolve(ctx, address)
	if err != nil {
		return ctx, nil, err
	}
	client := p.clients[assignment.Endpoint]
	if client == nil {
		return ctx, nil, fmt.Errorf("%w: farm endpoint %s", actorrouting.ErrNoAssignment, assignment.Endpoint)
	}
	return actorrouting.WithRoute(ctx, address, assignment), client, nil
}

func (p *RoutedFarmParticipant) InspectFarmOperation(ctx context.Context, req *logicrpcpb.InspectFarmOperationRequest) (*logicrpcpb.InspectFarmOperationResponse, error) {
	ctx, client, err := p.client(ctx, req.GetFarmId())
	if err != nil {
		return nil, err
	}
	return client.InspectFarmOperation(ctx, req)
}

func (p *RoutedFarmParticipant) CompleteFarmOperation(ctx context.Context, req *logicrpcpb.CompleteFarmOperationRequest) (*logicrpcpb.CompleteFarmOperationResponse, error) {
	ctx, client, err := p.client(ctx, req.GetFarmId())
	if err != nil {
		return nil, err
	}
	return client.CompleteFarmOperation(ctx, req)
}
