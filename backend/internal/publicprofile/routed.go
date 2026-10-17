package publicprofile

import (
	"context"
	"errors"
	"fmt"
	"sync"

	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"
)

// RoutedReader uses the same static ActorRouteClient + endpoint client map as
// other player RPC composition. The profile read carries the resolved route
// context, so direct and routed readers have identical validation semantics.
type RoutedReader struct {
	realm   string
	routes  *actorrouting.ActorRouteClient
	clients map[string]logicrpcpb.PlayerParticipantServiceClient
}

func NewRoutedReader(realm string, routes *actorrouting.ActorRouteClient, clients map[string]logicrpcpb.PlayerParticipantServiceClient) (*RoutedReader, error) {
	if realm == "" || routes == nil || len(clients) == 0 {
		return nil, errors.New("public profile routed reader configuration required")
	}
	return &RoutedReader{realm: realm, routes: routes, clients: clients}, nil
}

func (r *RoutedReader) BatchGetPublicProfiles(ctx context.Context, playerIDs []uint64) (map[uint64]*publicpb.PublicProfileView, error) {
	if r == nil || r.routes == nil || len(playerIDs) == 0 || len(playerIDs) > MaxBatchProfiles {
		return nil, errors.New("public profile routed batch reader unavailable")
	}
	type endpointGroup struct {
		client  logicrpcpb.PlayerParticipantServiceClient
		ids     []uint64
		indexes []int
	}
	groups := make(map[string]*endpointGroup)
	for index, playerID := range playerIDs {
		if playerID == 0 {
			return nil, errors.New("public profile routed batch player id is required")
		}
		address := actorrouting.Address{Realm: r.realm, Kind: actorrouting.KindPlayer, ID: playerID}
		assignment, err := r.routes.Resolve(ctx, address)
		if err != nil {
			return nil, err
		}
		client := r.clients[assignment.Endpoint]
		if client == nil {
			return nil, fmt.Errorf("%w: player endpoint %s", actorrouting.ErrNoAssignment, assignment.Endpoint)
		}
		group := groups[assignment.Endpoint]
		if group == nil {
			group = &endpointGroup{client: client}
			groups[assignment.Endpoint] = group
		}
		group.ids = append(group.ids, playerID)
		group.indexes = append(group.indexes, index)
	}
	profiles := make([]*publicpb.PublicProfileView, len(playerIDs))
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	for _, group := range groups {
		group := group
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := group.client.BatchGetPublicProfiles(ctx, &logicrpcpb.BatchGetPublicProfilesRequest{PlayerIds: group.ids})
			if err != nil || response == nil || len(response.PublicProfiles) != len(group.ids) {
				if err == nil {
					err = errors.New("public profile routed batch cardinality mismatch")
				}
				once.Do(func() { firstErr = err })
				return
			}
			for i, playerID := range group.ids {
				profile, validateErr := cloneValidated(response.PublicProfiles[i], playerID)
				if validateErr != nil {
					once.Do(func() { firstErr = validateErr })
					return
				}
				profiles[group.indexes[i]] = profile
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	result := make(map[uint64]*publicpb.PublicProfileView, len(playerIDs))
	for i, profile := range profiles {
		if profile == nil {
			return nil, errors.New("public profile routed batch incomplete")
		}
		result[playerIDs[i]] = profile
	}
	return result, nil
}

func (r *RoutedReader) Read(ctx context.Context, playerID uint64) (*publicpb.PublicProfileView, error) {
	if r == nil || r.routes == nil || playerID == 0 {
		return nil, errors.New("public profile routed reader unavailable")
	}
	address := actorrouting.Address{Realm: r.realm, Kind: actorrouting.KindPlayer, ID: playerID}
	assignment, err := r.routes.Resolve(ctx, address)
	if err != nil {
		return nil, err
	}
	client := r.clients[assignment.Endpoint]
	if client == nil {
		return nil, fmt.Errorf("%w: player endpoint %s", actorrouting.ErrNoAssignment, assignment.Endpoint)
	}
	response, err := client.GetPublicProfile(actorrouting.WithRoute(ctx, address, assignment), &logicrpcpb.GetPublicProfileRequest{PlayerId: playerID})
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, errors.New("public profile routed reader returned nil response")
	}
	return cloneValidated(response.GetPublicProfile(), playerID)
}
