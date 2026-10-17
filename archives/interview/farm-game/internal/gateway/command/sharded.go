package command

import (
	"context"
	"errors"
	"fmt"
	"sync"

	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/gateway/commandcatalog"
)

var ErrServiceLBUnavailable = errors.New("gate: ServiceLB destination unavailable")

// ShardedLogicClient resolves each Actor address before selecting a concrete
// Logic endpoint. The endpoint map is immutable for one process generation;
// assignment changes update only the Resolver snapshot/cache.
type ShardedLogicClient struct {
	realm     string
	routes    *actorrouting.ActorRouteClient
	clients   map[string]LogicClient
	serviceLB map[commandcatalog.FirstHopRole]LogicClient
}

func NewShardedLogicClient(realm string, routes *actorrouting.ActorRouteClient, clients map[string]LogicClient) (*ShardedLogicClient, error) {
	return NewShardedLogicClientWithServiceLB(realm, routes, clients, nil)
}

// NewShardedLogicClientWithServiceLB composes the static Actor holder routes
// with separately configured service-discovery clients. ServiceLB never uses
// the Actor holder map as an implicit fallback.
func NewShardedLogicClientWithServiceLB(realm string, routes *actorrouting.ActorRouteClient, clients map[string]LogicClient, serviceLB map[commandcatalog.FirstHopRole]LogicClient) (*ShardedLogicClient, error) {
	if realm == "" || routes == nil || len(clients) == 0 {
		return nil, errors.New("gate: sharded logic route configuration required")
	}
	copyClients := make(map[string]LogicClient, len(clients))
	for endpoint, client := range clients {
		if endpoint == "" || client == nil {
			return nil, errors.New("gate: invalid sharded logic endpoint")
		}
		copyClients[endpoint] = client
	}
	copyServiceLB := make(map[commandcatalog.FirstHopRole]LogicClient, len(serviceLB))
	for firstHop, client := range serviceLB {
		if (firstHop != commandcatalog.FirstHopPlayerSvr && firstHop != commandcatalog.FirstHopFarmSvr) || client == nil {
			return nil, errors.New("gate: invalid ServiceLB destination")
		}
		copyServiceLB[firstHop] = client
	}
	return &ShardedLogicClient{realm: realm, routes: routes, clients: copyClients, serviceLB: copyServiceLB}, nil
}

// ExecuteCommand selects the route mode from the frozen CommandCatalog.
// ACTOR_AFFINITY retains the existing ActorRoute path. SERVICE_LB uses only
// the catalog-declared FirstHop pool and never fabricates ActorRoute metadata.
func (c *ShardedLogicClient) ExecuteCommand(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	if req == nil || req.Command == nil {
		return nil, errors.New("gate: command required")
	}
	spec, ok := commandcatalog.Lookup(req.Command.CommandType)
	if !ok {
		return nil, fmt.Errorf("gate: unregistered command type %s", req.Command.CommandType)
	}
	switch spec.RouteMode {
	case commandcatalog.RouteModeActorAffinity:
		return c.executeActorAffinity(ctx, req)
	case commandcatalog.RouteModeServiceLB:
		return c.executeServiceLB(ctx, spec, req)
	default:
		return nil, fmt.Errorf("gate: unsupported route mode %s for %s", spec.RouteMode, req.Command.CommandType)
	}
}

func (c *ShardedLogicClient) executeActorAffinity(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	route := req.Dispatch.GetActorRoute()
	if req.Dispatch.GetRouteMode() != logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY || route == nil {
		return nil, errors.New("gate: actor route header required")
	}
	switch route.GetRouteDomain() {
	case logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER:
		return c.executeAt(ctx, actorrouting.KindPlayer, route.GetActorId(), req)
	case logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM:
		return c.executeAt(ctx, actorrouting.KindFarm, route.GetActorId(), req)
	default:
		return nil, errors.New("gate: actor route domain required")
	}
}

func (c *ShardedLogicClient) executeServiceLB(ctx context.Context, spec commandcatalog.CommandSpec, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	if req.Dispatch.GetRouteMode() != logicrpcpb.RouteMode_ROUTE_MODE_SERVICE_LB || req.Dispatch.GetActorRoute() != nil {
		return nil, fmt.Errorf("gate: invalid ServiceLB dispatch for %s", req.Command.CommandType)
	}
	client := c.serviceLB[spec.FirstHop]
	if client == nil {
		return nil, fmt.Errorf("%w: first hop %s is not configured", ErrServiceLBUnavailable, spec.FirstHop)
	}
	response, err := client.ExecuteCommand(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("%w: first hop %s: %w", ErrServiceLBUnavailable, spec.FirstHop, err)
	}
	return response, nil
}

func (c *ShardedLogicClient) CancelJoinAttempt(ctx context.Context, req *logicrpcpb.CancelJoinAttemptRequest) (*logicrpcpb.CancelJoinAttemptResponse, error) {
	if req == nil || req.TargetFarmId == 0 {
		return nil, errors.New("gate: CancelJoinAttempt target farm required")
	}
	address := actorrouting.Address{Realm: c.realm, Kind: actorrouting.KindFarm, ID: req.TargetFarmId}
	assignment, err := c.routes.Resolve(ctx, address)
	if err != nil {
		return nil, err
	}
	client := c.clients[assignment.Endpoint]
	canceler, ok := client.(JoinAttemptCanceler)
	if !ok {
		return nil, fmt.Errorf("gate: endpoint %s does not support CancelJoinAttempt", assignment.Endpoint)
	}
	return canceler.CancelJoinAttempt(actorrouting.WithRoute(ctx, address, assignment), req)
}

func (c *ShardedLogicClient) ArmSnapshot(ctx context.Context, req *logicrpcpb.ArmSnapshotRequest) (*logicrpcpb.ArmSnapshotResponse, error) {
	if req == nil || req.Subscription == nil || req.Subscription.FarmId == 0 {
		return nil, errors.New("gate: ArmSnapshot target farm required")
	}
	address := actorrouting.Address{Realm: c.realm, Kind: actorrouting.KindFarm, ID: req.Subscription.FarmId}
	assignment, err := c.routes.Resolve(ctx, address)
	if err != nil {
		return nil, err
	}
	client := c.clients[assignment.Endpoint]
	armer, ok := client.(JoinSnapshotArmer)
	if !ok {
		return nil, fmt.Errorf("gate: endpoint %s does not support ArmSnapshot", assignment.Endpoint)
	}
	return armer.ArmSnapshot(actorrouting.WithRoute(ctx, address, assignment), req)
}

func (c *ShardedLogicClient) CleanupSubscription(ctx context.Context, req *logicrpcpb.CleanupSubscriptionRequest) (*logicrpcpb.CleanupSubscriptionResponse, error) {
	if req == nil || req.Subscription == nil || req.Subscription.FarmId == 0 {
		return nil, errors.New("gate: CleanupSubscription target farm required")
	}
	address := actorrouting.Address{Realm: c.realm, Kind: actorrouting.KindFarm, ID: req.Subscription.FarmId}
	assignment, err := c.routes.Resolve(ctx, address)
	if err != nil {
		return nil, err
	}
	client := c.clients[assignment.Endpoint]
	cleaner, ok := client.(SubscriptionCleaner)
	if !ok {
		return nil, fmt.Errorf("gate: endpoint %s does not support CleanupSubscription", assignment.Endpoint)
	}
	return cleaner.CleanupSubscription(actorrouting.WithRoute(ctx, address, assignment), req)
}

// BatchRenewSubscriptions resolves every subscription in the FARM route
// domain, groups entries by the current Holder endpoint and keeps the result
// vector aligned with the caller's input vector. A route or endpoint failure
// is local to one entry/group; it must not fail unrelated subscriptions in the
// same Gate renewal round.
func (c *ShardedLogicClient) BatchRenewSubscriptions(ctx context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
	if req == nil {
		return nil, errors.New("gate: subscription lease renew request required")
	}
	response := &logicrpcpb.BatchRenewSubscriptionsResponse{
		Results: make([]*logicrpcpb.SubscriptionLeaseResult, len(req.Entries)),
	}
	type renewGroup struct {
		renewer SubscriptionLeaseRenewer
		// A pooled endpoint client selects its connection from the route attached
		// to the outgoing context, so the dispatch has to carry one. Every entry in
		// a group resolves to the same shard generation, which is all the route
		// carries, so the first entry's address stands for the group.
		address    actorrouting.Address
		assignment actorrouting.Assignment
		entries    []*logicrpcpb.SubscriptionLeaseEntry
		indexes    []int
	}
	groups := make(map[string]*renewGroup)
	for i, entry := range req.Entries {
		if entry == nil || entry.TargetFarmId == 0 {
			response.Results[i] = subscriptionLeaseRoutingResult(entry, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_ROUTE_STALE)
			continue
		}
		address := actorrouting.Address{Realm: c.realm, Kind: actorrouting.KindFarm, ID: entry.TargetFarmId}
		assignment, err := c.routes.Resolve(ctx, address)
		if err != nil {
			response.Results[i] = subscriptionLeaseRoutingResult(entry, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_ROUTE_STALE)
			continue
		}
		client := c.clients[assignment.Endpoint]
		renewer, ok := client.(SubscriptionLeaseRenewer)
		if !ok || renewer == nil {
			response.Results[i] = subscriptionLeaseRoutingResult(entry, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_UNAVAILABLE)
			continue
		}
		partition := subscriptionLeaseAssignmentPartition(assignment)
		group := groups[partition]
		if group == nil {
			group = &renewGroup{renewer: renewer, address: address, assignment: assignment}
			groups[partition] = group
		}
		group.entries = append(group.entries, entry)
		group.indexes = append(group.indexes, i)
	}

	var wg sync.WaitGroup
	for _, group := range groups {
		group := group
		wg.Add(1)
		go func() {
			defer wg.Done()
			groupResponse, err := group.renewer.BatchRenewSubscriptions(
				actorrouting.WithRoute(ctx, group.address, group.assignment),
				&logicrpcpb.BatchRenewSubscriptionsRequest{
					SourceGatewayId:     req.SourceGatewayId,
					SourceGatewayBootId: req.SourceGatewayBootId,
					Entries:             group.entries,
				})
			if err != nil || groupResponse == nil || len(groupResponse.Results) != len(group.entries) {
				for position, originalIndex := range group.indexes {
					response.Results[originalIndex] = subscriptionLeaseRoutingResult(group.entries[position], logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_UNAVAILABLE)
				}
				return
			}
			for position, originalIndex := range group.indexes {
				entry := group.entries[position]
				result := groupResponse.Results[position]
				if !sameSubscriptionLeaseIdentity(entry, result) {
					response.Results[originalIndex] = subscriptionLeaseRoutingResult(entry, logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_UNAVAILABLE)
					continue
				}
				response.Results[originalIndex] = result
			}
		}()
	}
	wg.Wait()
	return response, nil
}

func subscriptionLeaseAssignmentPartition(assignment actorrouting.Assignment) string {
	return stableLeaseIdentity("farm-holder", fmt.Sprint(assignment.ShardID), assignment.HolderPod,
		assignment.HolderBootID, fmt.Sprint(assignment.WriterEpoch), fmt.Sprint(assignment.LeaseEpoch),
		fmt.Sprint(assignment.ShardRevision))
}

func (c *ShardedLogicClient) SubscriptionLeasePartition(ctx context.Context, farmID uint64) (string, error) {
	if farmID == 0 {
		return "", errors.New("gate: subscription lease farm required")
	}
	assignment, err := c.routes.Resolve(ctx, actorrouting.Address{Realm: c.realm, Kind: actorrouting.KindFarm, ID: farmID})
	if err != nil {
		return "", err
	}
	return subscriptionLeaseAssignmentPartition(assignment), nil
}

// BatchRenewSubscriptionsPartition re-resolves every item and dispatches only
// entries still owned by the exact planned Holder boot/writer/lease/shard
// generation. Handoffs are returned as per-item ROUTE_STALE and never share an
// RPC with the old generation.
func (c *ShardedLogicClient) BatchRenewSubscriptionsPartition(ctx context.Context, expectedPartition string,
	req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
	if req == nil || expectedPartition == "" {
		return nil, errors.New("gate: partitioned subscription lease request required")
	}
	response := &logicrpcpb.BatchRenewSubscriptionsResponse{
		Results: make([]*logicrpcpb.SubscriptionLeaseResult, len(req.Entries)),
	}
	entries := make([]*logicrpcpb.SubscriptionLeaseEntry, 0, len(req.Entries))
	indexes := make([]int, 0, len(req.Entries))
	var renewer SubscriptionLeaseRenewer
	expectedEndpoint := ""
	resolvedPartition := ""
	// A pooled endpoint client picks its connection from the route on the outgoing
	// context. Every dispatched entry shares one shard generation, enforced below,
	// so the first accepted entry's route is the route for the whole batch.
	var dispatchAddress actorrouting.Address
	var dispatchAssignment actorrouting.Assignment
	for index, entry := range req.Entries {
		if entry == nil || entry.TargetFarmId == 0 {
			response.Results[index] = subscriptionLeaseRoutingResult(entry,
				logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_ROUTE_STALE)
			continue
		}
		address := actorrouting.Address{Realm: c.realm, Kind: actorrouting.KindFarm, ID: entry.TargetFarmId}
		assignment, err := c.routes.Resolve(ctx, address)
		if err != nil {
			response.Results[index] = subscriptionLeaseRoutingResult(entry,
				logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_ROUTE_STALE)
			continue
		}
		partition := subscriptionLeaseAssignmentPartition(assignment)
		exactHolder := subscriptionLeaseDispatchHolder(req.SourceGatewayId,
			req.SourceGatewayBootId, partition)
		farmHolder := subscriptionLeaseDispatchHolder(req.SourceGatewayId,
			req.SourceGatewayBootId, subscriptionLeaseFarmPartition(entry.TargetFarmId))
		if expectedPartition != exactHolder && expectedPartition != farmHolder {
			response.Results[index] = subscriptionLeaseRoutingResult(entry,
				logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_ROUTE_STALE)
			continue
		}
		clientRenewer, ok := c.clients[assignment.Endpoint].(SubscriptionLeaseRenewer)
		if !ok || clientRenewer == nil ||
			(expectedEndpoint != "" && expectedEndpoint != assignment.Endpoint) ||
			(resolvedPartition != "" && resolvedPartition != partition) {
			response.Results[index] = subscriptionLeaseRoutingResult(entry,
				logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_UNAVAILABLE)
			continue
		}
		renewer = clientRenewer
		expectedEndpoint = assignment.Endpoint
		resolvedPartition = partition
		if len(entries) == 0 {
			dispatchAddress, dispatchAssignment = address, assignment
		}
		entries = append(entries, entry)
		indexes = append(indexes, index)
	}
	if len(entries) == 0 {
		return response, nil
	}
	groupResponse, err := renewer.BatchRenewSubscriptions(
		actorrouting.WithRoute(ctx, dispatchAddress, dispatchAssignment),
		&logicrpcpb.BatchRenewSubscriptionsRequest{
			SourceGatewayId: req.SourceGatewayId, SourceGatewayBootId: req.SourceGatewayBootId, Entries: entries,
		})
	if err != nil || groupResponse == nil || len(groupResponse.Results) != len(entries) {
		for position, originalIndex := range indexes {
			response.Results[originalIndex] = subscriptionLeaseRoutingResult(entries[position],
				logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_UNAVAILABLE)
		}
		return response, nil
	}
	for position, originalIndex := range indexes {
		result := groupResponse.Results[position]
		if !sameSubscriptionLeaseIdentity(entries[position], result) {
			result = subscriptionLeaseRoutingResult(entries[position],
				logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_UNAVAILABLE)
		}
		response.Results[originalIndex] = result
	}
	return response, nil
}

func subscriptionLeaseRoutingResult(entry *logicrpcpb.SubscriptionLeaseEntry, status logicrpcpb.SubscriptionLeaseStatus) *logicrpcpb.SubscriptionLeaseResult {
	result := &logicrpcpb.SubscriptionLeaseResult{Status: status}
	if entry != nil {
		result.TargetFarmId = entry.TargetFarmId
		result.SubscriptionId = entry.SubscriptionId
		result.JoinAttemptId = entry.JoinAttemptId
	}
	return result
}

func sameSubscriptionLeaseIdentity(entry *logicrpcpb.SubscriptionLeaseEntry, result *logicrpcpb.SubscriptionLeaseResult) bool {
	return entry != nil && result != nil &&
		entry.TargetFarmId == result.TargetFarmId &&
		entry.SubscriptionId == result.SubscriptionId &&
		entry.JoinAttemptId == result.JoinAttemptId
}

func (c *ShardedLogicClient) executeAt(ctx context.Context, kind actorrouting.Kind, id uint64, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	address := actorrouting.Address{Realm: c.realm, Kind: kind, ID: id}
	assignment, err := c.routes.Resolve(ctx, address)
	if err != nil {
		return nil, err
	}
	client := c.clients[assignment.Endpoint]
	if client == nil {
		return nil, fmt.Errorf("%w: endpoint %s", actorrouting.ErrNoAssignment, assignment.Endpoint)
	}
	forward, err := WithResolvedActorRoute(req, assignment)
	if err != nil {
		return nil, err
	}
	return client.ExecuteCommand(actorrouting.WithRoute(ctx, address, assignment), forward)
}
