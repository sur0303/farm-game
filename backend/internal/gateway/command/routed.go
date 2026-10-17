package command

import (
	"context"
	"errors"
	"fmt"

	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/gateway/commandcatalog"
)

// RoutedLogicClient keeps the command catalog at farm-gate while PlayerActor
// and FarmActor scale as independent services.
type RoutedLogicClient struct {
	player LogicClient
	farm   LogicClient
}

func NewRoutedLogicClient(player, farm LogicClient) LogicClient {
	if player == nil || farm == nil {
		return nil
	}
	return &RoutedLogicClient{player: player, farm: farm}
}

// ExecuteCommand 按 CommandCatalog 的唯一 FirstHop 声明路由(28 §6.1);
// 未注册命令 fail closed,不存在第二份命令总表。
func (c *RoutedLogicClient) ExecuteCommand(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	if req == nil || req.Command == nil {
		return nil, errors.New("gate: command required")
	}
	spec, ok := commandcatalog.Lookup(req.Command.CommandType)
	if !ok {
		return nil, fmt.Errorf("gate: unregistered command type %s", req.Command.CommandType)
	}
	switch spec.FirstHop {
	case commandcatalog.FirstHopPlayerSvr:
		return c.player.ExecuteCommand(ctx, req)
	case commandcatalog.FirstHopFarmSvr:
		return c.farm.ExecuteCommand(ctx, req)
	default:
		return nil, fmt.Errorf("gate: no first hop for %s", req.Command.CommandType)
	}
}

func (c *RoutedLogicClient) CancelJoinAttempt(ctx context.Context, req *logicrpcpb.CancelJoinAttemptRequest) (*logicrpcpb.CancelJoinAttemptResponse, error) {
	canceler, ok := c.farm.(JoinAttemptCanceler)
	if !ok {
		return nil, errors.New("gate: farm client does not support CancelJoinAttempt")
	}
	return canceler.CancelJoinAttempt(ctx, req)
}

func (c *RoutedLogicClient) ArmSnapshot(ctx context.Context, req *logicrpcpb.ArmSnapshotRequest) (*logicrpcpb.ArmSnapshotResponse, error) {
	armer, ok := c.farm.(JoinSnapshotArmer)
	if !ok {
		return nil, errors.New("gate: farm client does not support ArmSnapshot")
	}
	return armer.ArmSnapshot(ctx, req)
}

func (c *RoutedLogicClient) CleanupSubscription(ctx context.Context, req *logicrpcpb.CleanupSubscriptionRequest) (*logicrpcpb.CleanupSubscriptionResponse, error) {
	cleaner, ok := c.farm.(SubscriptionCleaner)
	if !ok {
		return nil, errors.New("gate: farm client does not support CleanupSubscription")
	}
	return cleaner.CleanupSubscription(ctx, req)
}

func (c *RoutedLogicClient) BatchRenewSubscriptions(ctx context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
	renewer, ok := c.farm.(SubscriptionLeaseRenewer)
	if !ok {
		return nil, errors.New("gate: farm client does not support BatchRenewSubscriptions")
	}
	return renewer.BatchRenewSubscriptions(ctx, req)
}

func (c *RoutedLogicClient) SubscriptionLeasePartition(_ context.Context, farmID uint64) (string, error) {
	if farmID == 0 {
		return "", errors.New("gate: subscription lease farm required")
	}
	// The direct/static API exposes no Holder boot or ownership epoch. One farm
	// is therefore the strongest honest isolation partition.
	return subscriptionLeaseFarmPartition(farmID), nil
}

func (c *RoutedLogicClient) BatchRenewSubscriptionsPartition(ctx context.Context, partition string, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
	if partition == "" || req == nil {
		return nil, errors.New("gate: static farm lease partition required")
	}
	response := &logicrpcpb.BatchRenewSubscriptionsResponse{
		Results: make([]*logicrpcpb.SubscriptionLeaseResult, len(req.Entries)),
	}
	entries := make([]*logicrpcpb.SubscriptionLeaseEntry, 0, len(req.Entries))
	indexes := make([]int, 0, len(req.Entries))
	for index, entry := range req.Entries {
		expected, err := c.SubscriptionLeasePartition(ctx, entry.GetTargetFarmId())
		if err != nil || partition != subscriptionLeaseDispatchHolder(req.SourceGatewayId,
			req.SourceGatewayBootId, expected) {
			response.Results[index] = subscriptionLeaseRoutingResult(entry,
				logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_ROUTE_STALE)
			continue
		}
		entries = append(entries, entry)
		indexes = append(indexes, index)
	}
	if len(entries) == 0 {
		return response, nil
	}
	backendResponse, err := c.BatchRenewSubscriptions(ctx, &logicrpcpb.BatchRenewSubscriptionsRequest{
		SourceGatewayId: req.SourceGatewayId, SourceGatewayBootId: req.SourceGatewayBootId, Entries: entries,
	})
	if err != nil {
		return response, err
	}
	if backendResponse == nil || len(backendResponse.Results) != len(entries) {
		for position, originalIndex := range indexes {
			response.Results[originalIndex] = resultOrUnavailable(entries[position])
		}
		return response, nil
	}
	for position, originalIndex := range indexes {
		response.Results[originalIndex] = backendResponse.Results[position]
	}
	return response, nil
}
