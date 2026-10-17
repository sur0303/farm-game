package command

import (
	"context"

	logicrpcpb "farmgame/api/gen/logicrpc"
)

type grpcLogicClient struct{ client logicrpcpb.LogicServiceClient }

func NewGRPCLogicClient(client logicrpcpb.LogicServiceClient) LogicClient {
	if client == nil {
		return nil
	}
	return &grpcLogicClient{client: client}
}

func (c *grpcLogicClient) ExecuteCommand(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	return c.client.ExecuteCommand(ctx, req)
}

func (c *grpcLogicClient) CancelJoinAttempt(ctx context.Context, req *logicrpcpb.CancelJoinAttemptRequest) (*logicrpcpb.CancelJoinAttemptResponse, error) {
	return c.client.CancelJoinAttempt(ctx, req)
}

func (c *grpcLogicClient) ArmSnapshot(ctx context.Context, req *logicrpcpb.ArmSnapshotRequest) (*logicrpcpb.ArmSnapshotResponse, error) {
	return c.client.ArmSnapshot(ctx, req)
}

func (c *grpcLogicClient) CleanupSubscription(ctx context.Context, req *logicrpcpb.CleanupSubscriptionRequest) (*logicrpcpb.CleanupSubscriptionResponse, error) {
	return c.client.CleanupSubscription(ctx, req)
}

func (c *grpcLogicClient) BatchRenewSubscriptions(ctx context.Context, req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
	return c.client.BatchRenewSubscriptions(ctx, req)
}
