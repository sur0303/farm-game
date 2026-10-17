package main

import (
	"context"
	"sync"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type gateCapture struct {
	mu             sync.Mutex
	requests       []*logicrpcpb.DeliverBatchRequest
	batchSizes     []int
	deliveredItems int
	notify         chan struct{}
}

func newGateCapture() *gateCapture {
	return &gateCapture{notify: make(chan struct{}, 64)}
}

func (c *gateCapture) DeliverBatch(_ context.Context, request *logicrpcpb.DeliverBatchRequest,
	_ ...grpc.CallOption) (*logicrpcpb.DeliverBatchResponse, error) {
	cloned := proto.Clone(request).(*logicrpcpb.DeliverBatchRequest)
	c.mu.Lock()
	c.requests = append(c.requests, cloned)
	c.batchSizes = append(c.batchSizes, len(cloned.Items))
	c.deliveredItems += len(cloned.Items)
	c.mu.Unlock()
	select {
	case c.notify <- struct{}{}:
	default:
	}
	response := &logicrpcpb.DeliverBatchResponse{}
	for _, item := range request.Items {
		response.Results = append(response.Results, &logicrpcpb.DeliverBatchItemResult{
			ItemId: item.ItemId, Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
		})
	}
	return response, nil
}

func (c *gateCapture) snapshot() (batchSizes []int, delivered int, requestCount int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	batchSizes = append([]int(nil), c.batchSizes...)
	return batchSizes, c.deliveredItems, len(c.requests)
}

func (c *gateCapture) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = nil
	c.batchSizes = nil
	c.deliveredItems = 0
}

func (c *gateCapture) waitDelivered(ctx context.Context, want int) error {
	if want <= 0 {
		return nil
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(5 * time.Second)
	}
	for {
		_, delivered, _ := c.snapshot()
		if delivered >= want {
			return nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.DeadlineExceeded
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-c.notify:
			timer.Stop()
		case <-timer.C:
			_, delivered, _ = c.snapshot()
			if delivered >= want {
				return nil
			}
			return context.DeadlineExceeded
		}
	}
}
