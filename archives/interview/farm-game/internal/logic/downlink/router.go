package downlink

import (
	"context"
	"errors"
	"strings"

	logicrpcpb "farmgame/api/gen/logicrpc"

	"google.golang.org/grpc"
)

// RoutedGatewayClient is the multi-Gate boundary used behind Dispatcher.
// Actor turns persist gateway_id with each presence; this client partitions a
// batch and sends every item only to the Gate that owns its connection.
type RoutedGatewayClient struct {
	clients  map[string]GatewayClient
	fallback GatewayClient
}

func NewRoutedGatewayClient(clients map[string]GatewayClient, fallback GatewayClient) (*RoutedGatewayClient, error) {
	copied := make(map[string]GatewayClient, len(clients))
	for gatewayID, client := range clients {
		gatewayID = strings.TrimSpace(gatewayID)
		if gatewayID == "" || client == nil {
			return nil, errors.New("logic downlink: invalid gateway route")
		}
		copied[gatewayID] = client
	}
	if len(copied) == 0 && fallback == nil {
		return nil, errors.New("logic downlink: no gateway routes")
	}
	return &RoutedGatewayClient{clients: copied, fallback: fallback}, nil
}

// ResolveGateway is consumed by Dispatcher before queueing, which keeps every
// Gate on its own bounded lane instead of partitioning only after a mixed
// process-wide batch has already formed.
func (r *RoutedGatewayClient) ResolveGateway(gatewayID string) GatewayClient {
	if r == nil {
		return nil
	}
	client := r.clients[strings.TrimSpace(gatewayID)]
	if client == nil {
		client = r.fallback
	}
	return client
}

func (r *RoutedGatewayClient) DeliverBatch(ctx context.Context, req *logicrpcpb.DeliverBatchRequest, opts ...grpc.CallOption) (*logicrpcpb.DeliverBatchResponse, error) {
	if req == nil {
		return &logicrpcpb.DeliverBatchResponse{}, nil
	}
	type routeBatch struct {
		client GatewayClient
		items  []*logicrpcpb.DownlinkItem
	}
	groups := make(map[string]*routeBatch)
	results := make(map[string]*logicrpcpb.DeliverBatchItemResult, len(req.Items))
	for _, item := range req.Items {
		if item == nil {
			continue
		}
		gatewayID := strings.TrimSpace(item.GatewayId)
		client := r.clients[gatewayID]
		if client == nil {
			client = r.fallback
		}
		if client == nil {
			results[resultKey(gatewayID, item.ItemId)] = unavailable(item.ItemId)
			continue
		}
		group := groups[gatewayID]
		if group == nil {
			group = &routeBatch{client: client}
			groups[gatewayID] = group
		}
		group.items = append(group.items, item)
	}
	for gatewayID, group := range groups {
		response, err := group.client.DeliverBatch(ctx, &logicrpcpb.DeliverBatchRequest{Items: group.items}, opts...)
		if err != nil || response == nil {
			for _, item := range group.items {
				results[resultKey(gatewayID, item.ItemId)] = unavailable(item.ItemId)
			}
			continue
		}
		for _, result := range response.Results {
			if result != nil {
				results[resultKey(gatewayID, result.ItemId)] = result
			}
		}
	}
	response := &logicrpcpb.DeliverBatchResponse{}
	for _, item := range req.Items {
		if item == nil {
			response.Results = append(response.Results, unavailable(""))
			continue
		}
		result := results[resultKey(strings.TrimSpace(item.GatewayId), item.ItemId)]
		if result == nil {
			result = unavailable(item.ItemId)
		}
		response.Results = append(response.Results, result)
	}
	return response, nil
}

func resultKey(gatewayID, itemID string) string {
	return gatewayID + "\x00" + itemID
}

func unavailable(itemID string) *logicrpcpb.DeliverBatchItemResult {
	return &logicrpcpb.DeliverBatchItemResult{
		ItemId: itemID,
		Status: logicrpcpb.DownlinkItemStatus_UNAVAILABLE,
	}
}
