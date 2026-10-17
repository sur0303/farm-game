package downlink

import (
	"context"
	"errors"
	"testing"

	logicrpcpb "farmgame/api/gen/logicrpc"

	"google.golang.org/grpc"
)

type routedCapture struct {
	requests []*logicrpcpb.DeliverBatchRequest
	err      error
}

func (c *routedCapture) DeliverBatch(_ context.Context, req *logicrpcpb.DeliverBatchRequest, _ ...grpc.CallOption) (*logicrpcpb.DeliverBatchResponse, error) {
	c.requests = append(c.requests, req)
	if c.err != nil {
		return nil, c.err
	}
	response := &logicrpcpb.DeliverBatchResponse{}
	for _, item := range req.Items {
		response.Results = append(response.Results, &logicrpcpb.DeliverBatchItemResult{
			ItemId: item.ItemId,
			Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
		})
	}
	return response, nil
}

func TestRoutedGatewayClientPartitionsByOwningGate(t *testing.T) {
	gateA := &routedCapture{}
	gateB := &routedCapture{}
	router, err := NewRoutedGatewayClient(map[string]GatewayClient{"gate-a": gateA, "gate-b": gateB}, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := router.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{
		{ItemId: "a1", GatewayId: "gate-a"},
		{ItemId: "b1", GatewayId: "gate-b"},
		{ItemId: "a2", GatewayId: "gate-a"},
	}})
	if err != nil || len(response.Results) != 3 ||
		len(gateA.requests) != 1 || len(gateA.requests[0].Items) != 2 ||
		len(gateB.requests) != 1 || len(gateB.requests[0].Items) != 1 {
		t.Fatalf("response=%v gateA=%v gateB=%v err=%v", response, gateA.requests, gateB.requests, err)
	}
	for _, result := range response.Results {
		if result.Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
			t.Fatalf("unexpected route result: %v", result)
		}
	}
}

func TestRoutedGatewayClientFailsOneMissingOrUnavailableRouteClosed(t *testing.T) {
	gateA := &routedCapture{err: errors.New("draining")}
	router, err := NewRoutedGatewayClient(map[string]GatewayClient{"gate-a": gateA}, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := router.DeliverBatch(context.Background(), &logicrpcpb.DeliverBatchRequest{Items: []*logicrpcpb.DownlinkItem{
		{ItemId: "a1", GatewayId: "gate-a"},
		{ItemId: "b1", GatewayId: "gate-b"},
	}})
	if err != nil || len(response.Results) != 2 {
		t.Fatalf("response=%v err=%v", response, err)
	}
	for _, result := range response.Results {
		if result.Status != logicrpcpb.DownlinkItemStatus_UNAVAILABLE {
			t.Fatalf("route must fail closed: %v", result)
		}
	}
}
