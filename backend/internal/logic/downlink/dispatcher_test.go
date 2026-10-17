package downlink

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"
	"google.golang.org/grpc"
)

func TestDispatcherReservedControlLaneSurvivesOrdinaryQueueSaturation(t *testing.T) {
	client := &captureGateway{called: make(chan struct{}), release: make(chan struct{})}
	config := DefaultDispatcherConfig()
	config.LaneQueueSize = 1
	config.ControlQueueSize = 1
	config.BatchSize = 1
	dispatcher := NewDispatcherWithConfig(client, config)
	defer dispatcher.Close()
	defer close(client.release)

	if err := dispatcher.Enqueue(&logicrpcpb.DownlinkItem{ItemId: "in-flight"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.called:
	case <-time.After(time.Second):
		t.Fatal("ordinary delivery did not enter the RPC")
	}
	if err := dispatcher.Enqueue(&logicrpcpb.DownlinkItem{ItemId: "ordinary-queued"}); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Enqueue(&logicrpcpb.DownlinkItem{ItemId: "ordinary-overflow"}); !errors.Is(err, ErrDispatcherFull) {
		t.Fatalf("ordinary overflow error=%v, want ErrDispatcherFull", err)
	}
	if err := dispatcher.EnqueueControlWithResult(&logicrpcpb.DownlinkItem{ItemId: "terminal"}, nil); err != nil {
		t.Fatalf("reserved terminal enqueue failed while ordinary lane was full: %v", err)
	}
}

type captureGateway struct {
	mu       sync.Mutex
	request  *logicrpcpb.DeliverBatchRequest
	options  []grpc.CallOption
	response *logicrpcpb.DeliverBatchResponse
	err      error
	called   chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (c *captureGateway) DeliverBatch(_ context.Context, req *logicrpcpb.DeliverBatchRequest, options ...grpc.CallOption) (*logicrpcpb.DeliverBatchResponse, error) {
	c.mu.Lock()
	c.request = req
	c.options = append([]grpc.CallOption(nil), options...)
	c.mu.Unlock()
	if c.called != nil {
		c.once.Do(func() { close(c.called) })
	}
	if c.release != nil {
		<-c.release
	}
	if c.response != nil {
		return c.response, c.err
	}
	return &logicrpcpb.DeliverBatchResponse{}, c.err
}

func TestDispatcherReportsGatewayItemResult(t *testing.T) {
	client := &captureGateway{response: &logicrpcpb.DeliverBatchResponse{Results: []*logicrpcpb.DeliverBatchItemResult{{ItemId: "join", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED}}}}
	d := NewDispatcher(client, 4)
	defer d.Close()
	result := make(chan *logicrpcpb.DeliverBatchItemResult, 1)
	if err := d.EnqueueWithResult(&logicrpcpb.DownlinkItem{ItemId: "join"}, func(value *logicrpcpb.DeliverBatchItemResult) { result <- value }); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got == nil || got.ItemId != "join" || got.Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
			t.Fatalf("result=%v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not report gateway result")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.options) != 0 {
		t.Fatalf("DeliverBatch options=%d, WaitForReady must not be used", len(client.options))
	}
}

type contextBlockedGateway struct {
	started chan struct{}
	once    sync.Once
}

func (c *contextBlockedGateway) DeliverBatch(ctx context.Context, _ *logicrpcpb.DeliverBatchRequest, _ ...grpc.CallOption) (*logicrpcpb.DeliverBatchResponse, error) {
	c.once.Do(func() { close(c.started) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestDispatcherBadGateDoesNotBlockHealthyGate(t *testing.T) {
	bad := &contextBlockedGateway{started: make(chan struct{})}
	good := &captureGateway{response: &logicrpcpb.DeliverBatchResponse{Results: []*logicrpcpb.DeliverBatchItemResult{{ItemId: "good", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED}}}}
	router, err := NewRoutedGatewayClient(map[string]GatewayClient{"gate-bad": bad, "gate-good": good}, nil)
	if err != nil {
		t.Fatal(err)
	}
	config := DefaultDispatcherConfig()
	config.RPCTimeout = 150 * time.Millisecond
	config.MaxConcurrent = 2
	dispatcher := NewDispatcherWithConfig(router, config)
	defer dispatcher.Close()
	badResult := make(chan *logicrpcpb.DeliverBatchItemResult, 1)
	goodResult := make(chan *logicrpcpb.DeliverBatchItemResult, 1)
	if err := dispatcher.EnqueueWithResult(&logicrpcpb.DownlinkItem{ItemId: "bad", GatewayId: "gate-bad"}, func(result *logicrpcpb.DeliverBatchItemResult) {
		badResult <- result
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-bad.started:
	case <-time.After(time.Second):
		t.Fatal("bad Gate RPC did not start")
	}
	started := time.Now()
	if err := dispatcher.EnqueueWithResult(&logicrpcpb.DownlinkItem{ItemId: "good", GatewayId: "gate-good"}, func(result *logicrpcpb.DeliverBatchItemResult) {
		goodResult <- result
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-goodResult:
		if result.Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
			t.Fatalf("healthy Gate result=%v", result)
		}
		if elapsed := time.Since(started); elapsed >= config.RPCTimeout {
			t.Fatalf("healthy Gate waited for bad Gate deadline: %v", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("healthy Gate lane was blocked")
	}
	select {
	case result := <-badResult:
		if result.Status != logicrpcpb.DownlinkItemStatus_UNAVAILABLE {
			t.Fatalf("bad Gate result=%v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("bad Gate did not fail at its independent deadline")
	}
}

func TestDispatcherCallbacksRemainOrderedWithinGateLane(t *testing.T) {
	client := &captureGateway{response: &logicrpcpb.DeliverBatchResponse{Results: []*logicrpcpb.DeliverBatchItemResult{
		{ItemId: "first", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED},
		{ItemId: "second", Status: logicrpcpb.DownlinkItemStatus_UNAVAILABLE},
	}}}
	dispatcher := NewDispatcher(client, 8)
	defer dispatcher.Close()
	var mu sync.Mutex
	var order []string
	done := make(chan struct{})
	callback := func(id string) func(*logicrpcpb.DeliverBatchItemResult) {
		return func(*logicrpcpb.DeliverBatchItemResult) {
			mu.Lock()
			order = append(order, id)
			if len(order) == 2 {
				close(done)
			}
			mu.Unlock()
		}
	}
	if err := dispatcher.EnqueueWithResult(&logicrpcpb.DownlinkItem{ItemId: "first"}, callback("first")); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.EnqueueWithResult(&logicrpcpb.DownlinkItem{ItemId: "second"}, callback("second")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("callbacks did not complete")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "first" || order[1] != "second" {
		t.Fatalf("callback order=%v", order)
	}
}

func TestDispatcherRetiresIdleGatewayLane(t *testing.T) {
	client := &captureGateway{}
	config := DefaultDispatcherConfig()
	config.LaneIdleTimeout = 15 * time.Millisecond
	dispatcher := NewDispatcherWithConfig(client, config)
	defer dispatcher.Close()
	if err := dispatcher.Enqueue(&logicrpcpb.DownlinkItem{ItemId: "idle"}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats := dispatcher.Stats()
		if stats.LanesRetired == 1 && stats.LanesCurrent == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("idle lane was not retired: %+v", dispatcher.Stats())
}

type concurrencyGateway struct {
	active  atomic.Int32
	maximum atomic.Int32
	started chan struct{}
	release chan struct{}
}

func (c *concurrencyGateway) DeliverBatch(_ context.Context, req *logicrpcpb.DeliverBatchRequest, _ ...grpc.CallOption) (*logicrpcpb.DeliverBatchResponse, error) {
	active := c.active.Add(1)
	for {
		maximum := c.maximum.Load()
		if active <= maximum || c.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	c.started <- struct{}{}
	<-c.release
	c.active.Add(-1)
	response := &logicrpcpb.DeliverBatchResponse{}
	for _, item := range req.Items {
		response.Results = append(response.Results, &logicrpcpb.DeliverBatchItemResult{ItemId: item.ItemId, Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED})
	}
	return response, nil
}

func TestDispatcherBoundsGlobalRPCConcurrency(t *testing.T) {
	client := &concurrencyGateway{started: make(chan struct{}, 2), release: make(chan struct{}, 2)}
	router, err := NewRoutedGatewayClient(map[string]GatewayClient{"gate-a": client, "gate-b": client}, nil)
	if err != nil {
		t.Fatal(err)
	}
	config := DefaultDispatcherConfig()
	config.MaxConcurrent = 1
	dispatcher := NewDispatcherWithConfig(router, config)
	defer dispatcher.Close()
	done := make(chan struct{}, 2)
	for _, gatewayID := range []string{"gate-a", "gate-b"} {
		if err := dispatcher.EnqueueWithResult(&logicrpcpb.DownlinkItem{ItemId: gatewayID, GatewayId: gatewayID}, func(*logicrpcpb.DeliverBatchItemResult) {
			done <- struct{}{}
		}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("first RPC did not start")
	}
	select {
	case <-client.started:
		t.Fatal("second Gate exceeded global RPC concurrency limit")
	case <-time.After(25 * time.Millisecond):
	}
	client.release <- struct{}{}
	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("second RPC did not start after slot release")
	}
	client.release <- struct{}{}
	for range 2 {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("bounded RPC callback did not finish")
		}
	}
	if maximum := client.maximum.Load(); maximum != 1 {
		t.Fatalf("maximum RPC concurrency=%d, want 1", maximum)
	}
}

func TestDispatcherReportsUnavailableOnRPCFailure(t *testing.T) {
	client := &captureGateway{err: context.DeadlineExceeded}
	d := NewDispatcher(client, 4)
	defer d.Close()
	result := make(chan *logicrpcpb.DeliverBatchItemResult, 1)
	if err := d.EnqueueWithResult(&logicrpcpb.DownlinkItem{ItemId: "join"}, func(value *logicrpcpb.DeliverBatchItemResult) { result <- value }); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got == nil || got.Status != logicrpcpb.DownlinkItemStatus_UNAVAILABLE {
			t.Fatalf("result=%v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not report RPC failure")
	}
}

func TestDispatcherBatchesItemsOutsideActorTurn(t *testing.T) {
	client := &captureGateway{called: make(chan struct{}), release: make(chan struct{})}
	d := NewDispatcher(client, 8)
	first := &logicrpcpb.DownlinkItem{ItemId: "a"}
	second := &logicrpcpb.DownlinkItem{ItemId: "b"}
	if err := d.Enqueue(first); err != nil {
		t.Fatal(err)
	}
	if err := d.Enqueue(second); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.called:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not invoke GatewayDownlinkService")
	}
	close(client.release)
	d.Close()
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.request == nil || len(client.request.Items) != 2 || client.request.Items[0].ItemId != "a" || client.request.Items[1].ItemId != "b" {
		t.Fatalf("batched request=%v", client.request)
	}
}

func TestDispatcherCollectLaneBatchSupportsSixtyFourItemCandidate(t *testing.T) {
	config := DefaultDispatcherConfig()
	config.BatchSize = 64
	dispatcher := &Dispatcher{config: config}
	lane := &gatewayLane{queue: make(chan queuedItem, 63), control: make(chan queuedItem, 1)}
	for ordinal := 1; ordinal < 64; ordinal++ {
		lane.queue <- queuedItem{item: &logicrpcpb.DownlinkItem{ItemId: fmt.Sprintf("item-%d", ordinal)}}
	}
	batch := dispatcher.collectLaneBatch(lane, queuedItem{item: &logicrpcpb.DownlinkItem{ItemId: "item-0"}})
	if len(batch) != 64 {
		t.Fatalf("batch size=%d, want 64", len(batch))
	}
}

func TestDispatcherQueueIsBounded(t *testing.T) {
	client := &captureGateway{called: make(chan struct{}), release: make(chan struct{})}
	d := NewDispatcher(client, 1)
	defer func() {
		close(client.release)
		d.Close()
	}()
	item := func(id string) *logicrpcpb.DownlinkItem { return &logicrpcpb.DownlinkItem{ItemId: id} }
	if err := d.Enqueue(item("a")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.called:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not start first batch")
	}
	if err := d.Enqueue(item("b")); err != nil {
		t.Fatal(err)
	}
	if err := d.Enqueue(item("c")); err != ErrDispatcherFull {
		t.Fatalf("third item should be rejected by bounded queue, got %v", err)
	}
}

func TestShardLaneKeyKeepsOneConnectionOnOneWorker(t *testing.T) {
	first := shardLaneKey("farm-gate", "conn-a", "sub-a", 4)
	second := shardLaneKey("farm-gate", "conn-a", "sub-b", 4)
	if first != second || first == "farm-gate" {
		t.Fatalf("same connection hashed to %q and %q", first, second)
	}
	if shardLaneKey("farm-gate", "conn-a", "", 1) != "farm-gate" {
		t.Fatal("LaneWorkers=1 must keep the historical gateway key")
	}
}

func TestDispatcherLaneWorkersDeliverConcurrentlyOnOneGate(t *testing.T) {
	client := &concurrencyGateway{started: make(chan struct{}, 2), release: make(chan struct{}, 2)}
	config := DefaultDispatcherConfig()
	config.LaneWorkers = 8
	config.MaxConcurrent = 8
	config.BatchSize = 1
	dispatcher := NewDispatcherWithConfig(client, config)
	defer dispatcher.Close()

	left, right := "conn-0", "conn-1"
	for i := 0; i < 64; i++ {
		candidate := fmt.Sprintf("conn-%d", i)
		if shardLaneKey("farm-gate", candidate, "", 8) != shardLaneKey("farm-gate", left, "", 8) {
			right = candidate
			break
		}
	}
	if shardLaneKey("farm-gate", left, "", 8) == shardLaneKey("farm-gate", right, "", 8) {
		t.Fatal("could not find two connections that hash to different lane workers")
	}

	done := make(chan struct{}, 2)
	for _, connectionID := range []string{left, right} {
		item := &logicrpcpb.DownlinkItem{
			ItemId: connectionID, GatewayId: "farm-gate", ConnectionId: connectionID,
		}
		if err := dispatcher.EnqueueWithResult(item, func(*logicrpcpb.DeliverBatchItemResult) {
			done <- struct{}{}
		}); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		select {
		case <-client.started:
		case <-time.After(time.Second):
			t.Fatal("expected two in-flight DeliverBatch RPCs on one gateway")
		}
	}
	if maximum := client.maximum.Load(); maximum < 2 {
		t.Fatalf("maximum RPC concurrency=%d, want at least 2", maximum)
	}
	client.release <- struct{}{}
	client.release <- struct{}{}
	for range 2 {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("sharded lane callback did not finish")
		}
	}
}
