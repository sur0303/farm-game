package downlink

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// flakyGateway 前 failTimes 次以 failErr 使 RPC 失败,之后按 response 成功响应。
type flakyGateway struct {
	failTimes int32
	failErr   error
	response  *logicrpcpb.DeliverBatchResponse
	attempts  atomic.Int32
}

func (f *flakyGateway) DeliverBatch(_ context.Context, _ *logicrpcpb.DeliverBatchRequest, _ ...grpc.CallOption) (*logicrpcpb.DeliverBatchResponse, error) {
	if f.attempts.Add(1) <= f.failTimes {
		return nil, f.failErr
	}
	return f.response, nil
}

func transientTestConfig() DispatcherConfig {
	config := DefaultDispatcherConfig()
	config.TransientMaxAttempts = 4
	config.TransientBackoff = 10 * time.Millisecond
	config.TransientBudget = 2 * time.Second
	return config
}

func awaitItemResult(t *testing.T, result <-chan *logicrpcpb.DeliverBatchItemResult) *logicrpcpb.DeliverBatchItemResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(3 * time.Second):
		t.Fatal("dispatcher did not report an item result")
		return nil
	}
}

// TestDispatcherRetriesTransientUnavailableUntilGateReady 钉死启动竞态修复:
// Gate 尚未监听/连接未 Ready(codes.Unavailable)时,Dispatcher 做有界重试,
// Gate Ready 后同一批条目按原 FIFO 成功送达,Join 主链不再被一次性失败打断。
func TestDispatcherRetriesTransientUnavailableUntilGateReady(t *testing.T) {
	client := &flakyGateway{
		failTimes: 2,
		failErr:   status.Error(codes.Unavailable, "connection refused"),
		response: &logicrpcpb.DeliverBatchResponse{Results: []*logicrpcpb.DeliverBatchItemResult{
			{ItemId: "snapshot", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED},
			{ItemId: "catchup", Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED},
		}},
	}
	d := NewDispatcherWithConfig(client, transientTestConfig())
	defer d.Close()

	snapshotResult := make(chan *logicrpcpb.DeliverBatchItemResult, 1)
	catchupResult := make(chan *logicrpcpb.DeliverBatchItemResult, 1)
	if err := d.EnqueueWithResult(&logicrpcpb.DownlinkItem{ItemId: "snapshot"},
		func(value *logicrpcpb.DeliverBatchItemResult) { snapshotResult <- value }); err != nil {
		t.Fatal(err)
	}
	if err := d.EnqueueWithResult(&logicrpcpb.DownlinkItem{ItemId: "catchup"},
		func(value *logicrpcpb.DeliverBatchItemResult) { catchupResult <- value }); err != nil {
		t.Fatal(err)
	}
	if got := awaitItemResult(t, snapshotResult); got == nil || got.Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("snapshot result=%v", got)
	}
	if got := awaitItemResult(t, catchupResult); got == nil || got.Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("catchup result=%v", got)
	}
	if attempts := client.attempts.Load(); attempts != 3 {
		t.Fatalf("attempts=%d, want 3(2 次瞬时失败后成功)", attempts)
	}
}

// TestDispatcherDoesNotRetryBusinessRejection 钉死边界:业务拒绝(STALE_*)
// 随正常响应返回,绝不是瞬时错误,不得重试、不得改写。
func TestDispatcherDoesNotRetryBusinessRejection(t *testing.T) {
	client := &flakyGateway{
		response: &logicrpcpb.DeliverBatchResponse{Results: []*logicrpcpb.DeliverBatchItemResult{
			{ItemId: "join", Status: logicrpcpb.DownlinkItemStatus_STALE_CONNECTION},
		}},
	}
	d := NewDispatcherWithConfig(client, transientTestConfig())
	defer d.Close()

	result := make(chan *logicrpcpb.DeliverBatchItemResult, 1)
	if err := d.EnqueueWithResult(&logicrpcpb.DownlinkItem{ItemId: "join"},
		func(value *logicrpcpb.DeliverBatchItemResult) { result <- value }); err != nil {
		t.Fatal(err)
	}
	if got := awaitItemResult(t, result); got == nil || got.Status != logicrpcpb.DownlinkItemStatus_STALE_CONNECTION {
		t.Fatalf("business rejection must be reported as-is, got %v", got)
	}
	if attempts := client.attempts.Load(); attempts != 1 {
		t.Fatalf("business rejection must not be retried, attempts=%d", attempts)
	}
}

// TestDispatcherDoesNotRetryPermanentRPCError 钉死边界:InvalidArgument 等
// 非瞬时 RPC 错误立即失败,不重试放大病态 Gate。
func TestDispatcherDoesNotRetryPermanentRPCError(t *testing.T) {
	client := &flakyGateway{failTimes: 8, failErr: status.Error(codes.InvalidArgument, "malformed batch")}
	d := NewDispatcherWithConfig(client, transientTestConfig())
	defer d.Close()

	result := make(chan *logicrpcpb.DeliverBatchItemResult, 1)
	if err := d.EnqueueWithResult(&logicrpcpb.DownlinkItem{ItemId: "join"},
		func(value *logicrpcpb.DeliverBatchItemResult) { result <- value }); err != nil {
		t.Fatal(err)
	}
	if got := awaitItemResult(t, result); got == nil || got.Status != logicrpcpb.DownlinkItemStatus_UNAVAILABLE {
		t.Fatalf("result=%v", got)
	}
	if attempts := client.attempts.Load(); attempts != 1 {
		t.Fatalf("permanent RPC error must not be retried, attempts=%d", attempts)
	}
}

// TestDispatcherDeadlineExceededIsNotTransient 钉死边界:自身 RPC deadline
// 超时不算"明确的瞬时不可用"——Gate 过载挂起时重试只会放大故障。
func TestDispatcherDeadlineExceededIsNotTransient(t *testing.T) {
	client := &flakyGateway{failTimes: 8, failErr: context.DeadlineExceeded}
	d := NewDispatcherWithConfig(client, transientTestConfig())
	defer d.Close()

	result := make(chan *logicrpcpb.DeliverBatchItemResult, 1)
	if err := d.EnqueueWithResult(&logicrpcpb.DownlinkItem{ItemId: "join"},
		func(value *logicrpcpb.DeliverBatchItemResult) { result <- value }); err != nil {
		t.Fatal(err)
	}
	if got := awaitItemResult(t, result); got == nil || got.Status != logicrpcpb.DownlinkItemStatus_UNAVAILABLE {
		t.Fatalf("result=%v", got)
	}
	if attempts := client.attempts.Load(); attempts != 1 {
		t.Fatalf("deadline exceeded must not be retried, attempts=%d", attempts)
	}
}

// TestDispatcherTransientRetryIsBounded 钉死"禁止无限重试":Gate 持续不可用
// 时按尝试次数上限放弃并向回调报告 UNAVAILABLE,Join 由上层有序收尾。
func TestDispatcherTransientRetryIsBounded(t *testing.T) {
	client := &flakyGateway{failTimes: 100, failErr: status.Error(codes.Unavailable, "gate is down")}
	config := transientTestConfig()
	config.TransientMaxAttempts = 3
	d := NewDispatcherWithConfig(client, config)
	defer d.Close()

	started := time.Now()
	result := make(chan *logicrpcpb.DeliverBatchItemResult, 1)
	if err := d.EnqueueWithResult(&logicrpcpb.DownlinkItem{ItemId: "join"},
		func(value *logicrpcpb.DeliverBatchItemResult) { result <- value }); err != nil {
		t.Fatal(err)
	}
	if got := awaitItemResult(t, result); got == nil || got.Status != logicrpcpb.DownlinkItemStatus_UNAVAILABLE {
		t.Fatalf("result=%v", got)
	}
	if attempts := client.attempts.Load(); attempts != 3 {
		t.Fatalf("attempts=%d, want bounded 3", attempts)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("bounded retry took too long: %v", elapsed)
	}
}

// flakyActivator 同时实现 DeliverBatch/ActivateSubscription,前 failTimes 次
// ActivateSubscription 以瞬时 Unavailable 失败。
type flakyActivator struct {
	failTimes int32
	attempts  atomic.Int32
}

func (f *flakyActivator) DeliverBatch(_ context.Context, _ *logicrpcpb.DeliverBatchRequest, _ ...grpc.CallOption) (*logicrpcpb.DeliverBatchResponse, error) {
	return &logicrpcpb.DeliverBatchResponse{}, nil
}

func (f *flakyActivator) ActivateSubscription(_ context.Context, req *logicrpcpb.ActivateSubscriptionRequest, _ ...grpc.CallOption) (*logicrpcpb.ActivateSubscriptionResponse, error) {
	if f.attempts.Add(1) <= f.failTimes {
		return nil, status.Error(codes.Unavailable, "gate listener not ready")
	}
	return &logicrpcpb.ActivateSubscriptionResponse{
		Status:              logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED,
		ReadyThroughVersion: req.GetReadyThroughVersion(),
	}, nil
}

// TestDispatcherActivateSubscriptionRetriesTransientUnavailable 钉死 Ready
// 排序竞态修复:ActivateSubscription 遇到启动期瞬时不可用同样有界重试。
func TestDispatcherActivateSubscriptionRetriesTransientUnavailable(t *testing.T) {
	client := &flakyActivator{failTimes: 2}
	d := NewDispatcherWithConfig(client, transientTestConfig())
	defer d.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := d.ActivateSubscription(ctx, &logicrpcpb.ActivateSubscriptionRequest{
		Subscription:        &logicrpcpb.SubscriptionFence{SubscriptionId: "sub-1"},
		ReadyThroughVersion: 7,
		Target: &logicrpcpb.DownlinkTargetFence{
			GatewayId: "gate-a", ConnectionId: "conn-1", ConnectionGeneration: 1,
		},
	})
	if err != nil || response.GetStatus() != logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED {
		t.Fatalf("activate resp=%v err=%v", response, err)
	}
	if attempts := client.attempts.Load(); attempts != 3 {
		t.Fatalf("attempts=%d, want 3(2 次瞬时失败后成功)", attempts)
	}
}

// TestDispatcherActivateSubscriptionRetryIsBounded 钉死 Ready 重试同样有界。
func TestDispatcherActivateSubscriptionRetryIsBounded(t *testing.T) {
	client := &flakyActivator{failTimes: 100}
	config := transientTestConfig()
	config.TransientMaxAttempts = 3
	d := NewDispatcherWithConfig(client, config)
	defer d.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := d.ActivateSubscription(ctx, &logicrpcpb.ActivateSubscriptionRequest{
		Subscription:        &logicrpcpb.SubscriptionFence{SubscriptionId: "sub-1"},
		ReadyThroughVersion: 7,
		Target: &logicrpcpb.DownlinkTargetFence{
			GatewayId: "gate-a", ConnectionId: "conn-1", ConnectionGeneration: 1,
		},
	})
	if err == nil {
		t.Fatal("persistent unavailability must fail after bounded retries")
	}
	if attempts := client.attempts.Load(); attempts != 3 {
		t.Fatalf("attempts=%d, want bounded 3", attempts)
	}
}
