package downlink

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// metricSnapshot 把注册表 gather 结果归一成 "name{label=value,...}" -> 值。
func metricSnapshot(t *testing.T, registry *prometheus.Registry) map[string]float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]float64{}
	for _, family := range families {
		for _, metric := range family.Metric {
			labels := make([]string, 0, len(metric.Label))
			for _, label := range metric.Label {
				labels = append(labels, label.GetName()+"="+label.GetValue())
			}
			sort.Strings(labels)
			key := family.GetName()
			if len(labels) > 0 {
				key += "{" + strings.Join(labels, ",") + "}"
			}
			if metric.Counter != nil {
				values[key] = metric.Counter.GetValue()
			}
		}
	}
	return values
}

func activateRequest(subscriptionID string, readyThrough uint64) *logicrpcpb.ActivateSubscriptionRequest {
	return &logicrpcpb.ActivateSubscriptionRequest{
		Subscription:        &logicrpcpb.SubscriptionFence{SubscriptionId: subscriptionID},
		ReadyThroughVersion: readyThrough,
		Target: &logicrpcpb.DownlinkTargetFence{
			GatewayId: "gate-a", ConnectionId: "conn-1", ConnectionGeneration: 1,
		},
	}
}

// TestDispatcherActivateSubscriptionClosedDuringBackoff 钉死生命周期缺口:关闭
// 期间的在途 ActivateSubscription 不得返回 (nil, nil),关闭后新调用必须返回
// ErrDispatcherClosed。
func TestDispatcherActivateSubscriptionClosedDuringBackoff(t *testing.T) {
	client := &flakyActivator{failTimes: 1 << 30} // 持续瞬时不可用
	config := transientTestConfig()
	config.TransientBackoff = 50 * time.Millisecond
	d := NewDispatcherWithConfig(client, config)
	registry := prometheus.NewRegistry()
	RegisterMetrics(registry, d)

	done := make(chan error, 1)
	go func() {
		_, err := d.ActivateSubscription(context.Background(), activateRequest("sub-1", 1))
		done <- err
	}()
	// 至少一次 RPC 尝试后关闭,让 Close 落在退避等待窗口内。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && client.attempts.Load() < 1 {
		time.Sleep(2 * time.Millisecond)
	}
	d.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("ActivateSubscription returned nil,nil on a closed dispatcher")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ActivateSubscription did not unblock after Close")
	}

	// 关闭后新调用必须返回 ErrDispatcherClosed。
	_, err := d.ActivateSubscription(context.Background(), activateRequest("sub-2", 2))
	if !errors.Is(err, ErrDispatcherClosed) {
		t.Fatalf("ActivateSubscription after Close err=%v, want ErrDispatcherClosed", err)
	}
	// P3:关闭期间 backoff 被打断必须记为终态 result=error,attempt 与 result 对称。
	values := metricSnapshot(t, registry)
	if got := values["farm_downlink_rpc_results_total{method=activate_subscription,result=error}"]; got != 1 {
		t.Fatalf("close-interrupted backoff result=error metric=%v, want 1", got)
	}
	if got := values["farm_downlink_rpc_attempts_total{attempt=1,method=activate_subscription}"]; got != 1 {
		t.Fatalf("close-interrupted backoff attempt metric=%v, want 1", got)
	}
}

// blockingActivator 记录并发上限,用于验证 ActivateSubscription 与
// DeliverBatch 共享 MaxConcurrent 槽池。
type blockingActivator struct {
	active  atomic.Int32
	maximum atomic.Int32
	release chan struct{}
}

func (b *blockingActivator) DeliverBatch(_ context.Context, _ *logicrpcpb.DeliverBatchRequest, _ ...grpc.CallOption) (*logicrpcpb.DeliverBatchResponse, error) {
	return &logicrpcpb.DeliverBatchResponse{}, nil
}

func (b *blockingActivator) ActivateSubscription(ctx context.Context, _ *logicrpcpb.ActivateSubscriptionRequest, _ ...grpc.CallOption) (*logicrpcpb.ActivateSubscriptionResponse, error) {
	active := b.active.Add(1)
	for {
		maximum := b.maximum.Load()
		if active <= maximum || b.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	select {
	case <-ctx.Done():
	case <-b.release:
	}
	b.active.Add(-1)
	return &logicrpcpb.ActivateSubscriptionResponse{Status: logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED}, nil
}

// TestDispatcherActivateSubscriptionSharesConcurrencyBudget 钉死容量缺口:100
// 并发 ActivateSubscription 不得超过 MaxConcurrent。
func TestDispatcherActivateSubscriptionSharesConcurrencyBudget(t *testing.T) {
	client := &blockingActivator{release: make(chan struct{}, 1)}
	config := DefaultDispatcherConfig()
	config.MaxConcurrent = 3
	config.RPCTimeout = 5 * time.Second // 测试窗口内不被单次 RPC 超时打断
	d := NewDispatcherWithConfig(client, config)
	defer d.Close()

	const calls = 100
	done := make(chan struct{}, calls)
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = d.ActivateSubscription(ctx, activateRequest("sub", 1))
			done <- struct{}{}
		}()
	}

	// 等待并发顶到 MaxConcurrent。
	limit := int32(config.MaxConcurrent)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && client.maximum.Load() < limit {
		time.Sleep(2 * time.Millisecond)
	}
	if maximum := client.maximum.Load(); maximum != limit {
		t.Fatalf("maximum ActivateSubscription concurrency=%d, want %d", maximum, limit)
	}
	close(client.release)
	for i := 0; i < calls; i++ {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("ActivateSubscription calls did not complete after release")
		}
	}
	wg.Wait()
	if maximum := client.maximum.Load(); maximum > limit {
		t.Fatalf("ActivateSubscription exceeded MaxConcurrent: %d", maximum)
	}
}

// deadlineCapturingGateway 记录每个 RPC ctx 的截止时间,并阻塞到截止。
type deadlineCapturingGateway struct {
	mu       sync.Mutex
	deadline time.Time
}

func (g *deadlineCapturingGateway) DeliverBatch(ctx context.Context, _ *logicrpcpb.DeliverBatchRequest, _ ...grpc.CallOption) (*logicrpcpb.DeliverBatchResponse, error) {
	if deadline, ok := ctx.Deadline(); ok {
		g.mu.Lock()
		g.deadline = deadline
		g.mu.Unlock()
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (g *deadlineCapturingGateway) ActivateSubscription(ctx context.Context, _ *logicrpcpb.ActivateSubscriptionRequest, _ ...grpc.CallOption) (*logicrpcpb.ActivateSubscriptionResponse, error) {
	if deadline, ok := ctx.Deadline(); ok {
		g.mu.Lock()
		g.deadline = deadline
		g.mu.Unlock()
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestDispatcherActivateDeadlineIsBudgetWhenBudgetBelowRPCTimeout 钉死单次 RPC
// 超时 = min(RPCTimeout, 剩余预算, 调用方 deadline):预算小于 RPCTimeout 时
// RPC 必须在预算处截止,而不是吃掉整个 RPCTimeout。
func TestDispatcherActivateDeadlineIsBudgetWhenBudgetBelowRPCTimeout(t *testing.T) {
	client := &deadlineCapturingGateway{}
	config := DefaultDispatcherConfig()
	config.RPCTimeout = 2 * time.Second
	config.TransientBudget = 100 * time.Millisecond
	d := NewDispatcherWithConfig(client, config)
	defer d.Close()

	started := time.Now()
	_, err := d.ActivateSubscription(context.Background(), activateRequest("sub", 1))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v want DeadlineExceeded from budget expiry", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("RPC used RPCTimeout(2s) instead of TransientBudget(100ms): elapsed=%v", elapsed)
	}
	client.mu.Lock()
	deadline := client.deadline
	client.mu.Unlock()
	if deadline.IsZero() {
		t.Fatal("gateway did not observe an RPC deadline")
	}
	if observed := deadline.Sub(started); observed > 400*time.Millisecond || observed < 40*time.Millisecond {
		t.Fatalf("RPC deadline window=%v not bounded by TransientBudget=100ms", observed)
	}
}

// TestDispatcherDeliverBatchDeadlineIsBudgetWhenBudgetBelowRPCTimeout 同上,覆盖
// DeliverBatch 路径。
func TestDispatcherDeliverBatchDeadlineIsBudgetWhenBudgetBelowRPCTimeout(t *testing.T) {
	client := &deadlineCapturingGateway{}
	config := DefaultDispatcherConfig()
	config.RPCTimeout = 2 * time.Second
	config.TransientBudget = 100 * time.Millisecond
	d := NewDispatcherWithConfig(client, config)
	defer d.Close()

	started := time.Now()
	result := make(chan *logicrpcpb.DeliverBatchItemResult, 1)
	if err := d.EnqueueWithResult(&logicrpcpb.DownlinkItem{ItemId: "x"},
		func(value *logicrpcpb.DeliverBatchItemResult) { result <- value }); err != nil {
		t.Fatal(err)
	}
	if got := awaitItemResult(t, result); got.Status != logicrpcpb.DownlinkItemStatus_UNAVAILABLE {
		t.Fatalf("result=%v want UNAVAILABLE", got)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("DeliverBatch used RPCTimeout(2s) instead of TransientBudget(100ms): elapsed=%v", elapsed)
	}
	client.mu.Lock()
	deadline := client.deadline
	client.mu.Unlock()
	if deadline.IsZero() {
		t.Fatal("gateway did not observe a deadline")
	}
	if observed := deadline.Sub(started); observed > 400*time.Millisecond || observed < 30*time.Millisecond {
		t.Fatalf("DeliverBatch deadline window=%v not bounded by 100ms budget", observed)
	}
}

// TestStableJitterIsDeterministicAndBounded 钉死 jitter 确定性:同一 attempt 的
// 倍率完全一致,所有倍率落在 [0.8, 1.2),相邻 attempt 又足够分散。
func TestStableJitterIsDeterministicAndBounded(t *testing.T) {
	if got := stableJitterFraction(1); got != stableJitterFraction(1) {
		t.Fatalf("jitter not deterministic for the same attempt: %v vs %v", got, stableJitterFraction(1))
	}
	seen := map[float64]bool{}
	for attempt := 1; attempt <= 16; attempt++ {
		frac := stableJitterFraction(attempt)
		if frac < 0.8 || frac >= 1.2 {
			t.Fatalf("attempt=%d jitter=%v out of [0.8,1.2)", attempt, frac)
		}
		seen[frac] = true
	}
	if len(seen) < 8 {
		t.Fatalf("jitter too uniform: %d distinct values over 16 attempts", len(seen))
	}
}

// TestJitteredBackoffAppliesStableJitter 验证实际退避时长叠加了稳定 jitter。
func TestJitteredBackoffAppliesStableJitter(t *testing.T) {
	config := transientTestConfig()
	d := NewDispatcherWithConfig(&captureGateway{}, config)
	defer d.Close()
	want := time.Duration(float64(config.TransientBackoff<<(3-1)) * stableJitterFraction(3))
	if got := d.jitteredBackoff(3); got != want {
		t.Fatalf("backoff=%v, want stable %v", got, want)
	}
	// jitter 乘数 [0.8,1.2) 作用在 4 倍封顶后的指数值上,因此上限为 4*base*1.2。
	maxBackoff := time.Duration(float64(4*config.TransientBackoff) * 1.2)
	seen := map[time.Duration]bool{}
	for attempt := 1; attempt <= 8; attempt++ {
		backoff := d.jitteredBackoff(attempt)
		if backoff <= 0 || backoff > maxBackoff {
			t.Fatalf("attempt=%d backoff=%v out of bounds", attempt, backoff)
		}
		seen[backoff] = true
	}
	if len(seen) < 4 {
		t.Fatalf("backoff too uniform: %d distinct over 8 attempts", len(seen))
	}
}

// TestDispatcherPersistentUnavailableMetrics 钉死持续不可用时的指标:method 为
// activate_subscription,attempt 逐次计数,result=unavailable,exhausted=1。
func TestDispatcherPersistentUnavailableMetrics(t *testing.T) {
	config := transientTestConfig()
	config.TransientMaxAttempts = 3
	config.TransientBackoff = 5 * time.Millisecond
	client := &flakyActivator{failTimes: 1 << 30} // 持续瞬时不可用
	d := NewDispatcherWithConfig(client, config)
	defer d.Close()
	registry := prometheus.NewRegistry()
	RegisterMetrics(registry, d)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := d.ActivateSubscription(ctx, activateRequest("sub-1", 1)); err == nil {
		t.Fatal("persistent unavailable must fail after bounded retries")
	}
	if attempts := client.attempts.Load(); attempts != 3 {
		t.Fatalf("attempts=%d, want bounded 3", attempts)
	}

	values := metricSnapshot(t, registry)
	for attempt, want := range map[string]float64{
		"farm_downlink_rpc_attempts_total{attempt=1,method=activate_subscription}": 1,
		"farm_downlink_rpc_attempts_total{attempt=2,method=activate_subscription}": 1,
		"farm_downlink_rpc_attempts_total{attempt=3,method=activate_subscription}": 1,
	} {
		if got := values[attempt]; got != want {
			t.Fatalf("metric %s=%v, want %v", attempt, got, want)
		}
	}
	if got := values["farm_downlink_rpc_results_total{method=activate_subscription,result=unavailable}"]; got != 1 {
		t.Fatalf("result=unavailable metric=%v, want 1", got)
	}
	if got := values["farm_downlink_rpc_exhausted_total{method=activate_subscription}"]; got != 1 {
		t.Fatalf("exhausted metric=%v, want 1", got)
	}
}

// TestDispatcherBusinessRejectionMetricsNoRetry 钉死业务拒绝不重试且按成功 RPC
// 计数:STALE 状态作为条目结果返回,不进入 exhausted。
func TestDispatcherBusinessRejectionMetricsNoRetry(t *testing.T) {
	client := &flakyGateway{
		response: &logicrpcpb.DeliverBatchResponse{Results: []*logicrpcpb.DeliverBatchItemResult{
			{ItemId: "join", Status: logicrpcpb.DownlinkItemStatus_STALE_CONNECTION},
		}},
	}
	d := NewDispatcherWithConfig(client, transientTestConfig())
	defer d.Close()
	registry := prometheus.NewRegistry()
	RegisterMetrics(registry, d)

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

	values := metricSnapshot(t, registry)
	if got := values["farm_downlink_rpc_attempts_total{attempt=1,method=deliver_batch}"]; got != 1 {
		t.Fatalf("attempt 1 metric=%v, want 1", got)
	}
	if got := values["farm_downlink_rpc_results_total{method=deliver_batch,result=ok}"]; got != 1 {
		t.Fatalf("result=ok metric=%v, want 1", got)
	}
	if got := values["farm_downlink_rpc_exhausted_total{method=deliver_batch}"]; got != 0 {
		t.Fatalf("business rejection must not exhaust retries, metric=%v", got)
	}
}

// ambiguousBatchGateway 模拟 Gate 已接收帧但响应在网络上丢失:首次调用返回
// Unavailable,之后按成功响应;记录每次调用携带的 item_id。
type ambiguousBatchGateway struct {
	mu       sync.Mutex
	calls    [][]string
	attempts atomic.Int32
}

func (g *ambiguousBatchGateway) DeliverBatch(_ context.Context, req *logicrpcpb.DeliverBatchRequest, _ ...grpc.CallOption) (*logicrpcpb.DeliverBatchResponse, error) {
	ids := make([]string, 0, len(req.Items))
	for _, item := range req.Items {
		ids = append(ids, item.GetItemId())
	}
	g.mu.Lock()
	g.calls = append(g.calls, ids)
	g.mu.Unlock()
	if g.attempts.Add(1) == 1 {
		// Gate 已把帧写入连接,但响应在网络上丢失。
		return nil, status.Error(codes.Unavailable, "response lost after accept")
	}
	return &logicrpcpb.DeliverBatchResponse{Results: []*logicrpcpb.DeliverBatchItemResult{
		{ItemId: ids[0], Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED},
	}}, nil
}

// TestDispatcherAmbiguousResponseRetryKeepsIdempotencyKey 钉死幂等:响应丢失后
// 重试携带完全相同的 item_id(幂等键),Gate 不会产生第二个可见帧,回调只触发一次。
func TestDispatcherAmbiguousResponseRetryKeepsIdempotencyKey(t *testing.T) {
	client := &ambiguousBatchGateway{}
	d := NewDispatcherWithConfig(client, transientTestConfig())
	defer d.Close()

	result := make(chan *logicrpcpb.DeliverBatchItemResult, 1)
	if err := d.EnqueueWithResult(&logicrpcpb.DownlinkItem{ItemId: "frame-42"},
		func(value *logicrpcpb.DeliverBatchItemResult) { result <- value }); err != nil {
		t.Fatal(err)
	}
	if got := awaitItemResult(t, result); got == nil || got.Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
		t.Fatalf("result=%v", got)
	}
	if attempts := client.attempts.Load(); attempts != 2 {
		t.Fatalf("attempts=%d, want 2(响应丢失后同键重试)", attempts)
	}
	// 回调只触发一次,不会产生第二个可见帧。
	select {
	case extra := <-result:
		t.Fatalf("duplicate visible frame after retry: %v", extra)
	case <-time.After(50 * time.Millisecond):
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.calls) != 2 {
		t.Fatalf("rpc calls=%v", client.calls)
	}
	for _, ids := range client.calls {
		if len(ids) != 1 || ids[0] != "frame-42" {
			t.Fatalf("retry changed idempotency key: %v", client.calls)
		}
	}
}

// ambiguousActivateGateway 模拟 Ready 请求响应丢失:首次 Unavailable,之后成功。
type ambiguousActivateGateway struct {
	mu       sync.Mutex
	requests []*logicrpcpb.ActivateSubscriptionRequest
	attempts atomic.Int32
}

func (g *ambiguousActivateGateway) DeliverBatch(_ context.Context, _ *logicrpcpb.DeliverBatchRequest, _ ...grpc.CallOption) (*logicrpcpb.DeliverBatchResponse, error) {
	return &logicrpcpb.DeliverBatchResponse{}, nil
}

func (g *ambiguousActivateGateway) ActivateSubscription(_ context.Context, req *logicrpcpb.ActivateSubscriptionRequest, _ ...grpc.CallOption) (*logicrpcpb.ActivateSubscriptionResponse, error) {
	g.mu.Lock()
	g.requests = append(g.requests, proto.Clone(req).(*logicrpcpb.ActivateSubscriptionRequest))
	g.mu.Unlock()
	if g.attempts.Add(1) == 1 {
		return nil, status.Error(codes.Unavailable, "response lost after accept")
	}
	return &logicrpcpb.ActivateSubscriptionResponse{
		Status:              logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED,
		ReadyThroughVersion: req.GetReadyThroughVersion(),
	}, nil
}

// TestDispatcherActivateAmbiguousResponseRetryKeepsReadyIdentity 钉死 Ready 请求
// 重试幂等:重试携带完全相同的 subscription/ReadyThroughVersion。
func TestDispatcherActivateAmbiguousResponseRetryKeepsReadyIdentity(t *testing.T) {
	client := &ambiguousActivateGateway{}
	d := NewDispatcherWithConfig(client, transientTestConfig())
	defer d.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := d.ActivateSubscription(ctx, activateRequest("sub-ready", 7))
	if err != nil || response.GetStatus() != logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED {
		t.Fatalf("resp=%v err=%v", response, err)
	}
	if attempts := client.attempts.Load(); attempts != 2 {
		t.Fatalf("attempts=%d, want 2", attempts)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.requests) != 2 {
		t.Fatalf("requests=%d", len(client.requests))
	}
	for _, got := range client.requests {
		if got.GetSubscription().GetSubscriptionId() != "sub-ready" || got.GetReadyThroughVersion() != 7 {
			t.Fatalf("retry changed Ready identity: %+v", got)
		}
	}
}

// TestDispatcherActivateDeadlineRespectsCallerDeadline 钉死 rpcDeadline 第三维:
// 调用方 deadline 小于 RPCTimeout 与剩余预算时,RPC 必须在调用方 deadline 截止,
// 而不是吃满 RPCTimeout 或预算。
func TestDispatcherActivateDeadlineRespectsCallerDeadline(t *testing.T) {
	client := &deadlineCapturingGateway{}
	config := DefaultDispatcherConfig()
	config.RPCTimeout = 2 * time.Second
	config.TransientBudget = 5 * time.Second // 预算宽裕,唯一收紧维度是调用方 deadline
	d := NewDispatcherWithConfig(client, config)
	defer d.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := d.ActivateSubscription(ctx, activateRequest("sub", 1))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v want DeadlineExceeded from caller deadline", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("RPC ignored caller deadline: elapsed=%v", elapsed)
	}
	client.mu.Lock()
	deadline := client.deadline
	client.mu.Unlock()
	if deadline.IsZero() {
		t.Fatal("gateway did not observe an RPC deadline")
	}
	if observed := deadline.Sub(started); observed > 300*time.Millisecond || observed < 30*time.Millisecond {
		t.Fatalf("RPC deadline window=%v not bounded by caller 80ms deadline", observed)
	}
}

// TestDispatcherActivateSubscriptionClosedInterruptsInflightRPC 钉死 P2-1:RPC 父
// 上下文为 d.ctx,Close 立即打断在途 ActivateSubscription 并返回 ErrDispatcherClosed,
// 不等待 RPC 跑满整个超时。
func TestDispatcherActivateSubscriptionClosedInterruptsInflightRPC(t *testing.T) {
	client := &blockingActivator{release: make(chan struct{}, 1)}
	config := DefaultDispatcherConfig()
	config.RPCTimeout = 5 * time.Second // 若未被打断,将占用整个超时
	d := NewDispatcherWithConfig(client, config)

	done := make(chan error, 1)
	go func() {
		_, err := d.ActivateSubscription(context.Background(), activateRequest("sub", 1))
		done <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && client.active.Load() < 1 {
		time.Sleep(2 * time.Millisecond)
	}
	if client.active.Load() < 1 {
		t.Fatal("ActivateSubscription RPC did not enter the gateway")
	}
	started := time.Now()
	d.Close()
	select {
	case err := <-done:
		if !errors.Is(err, ErrDispatcherClosed) {
			t.Fatalf("err=%v want ErrDispatcherClosed for in-flight RPC on close", err)
		}
		if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
			t.Fatalf("Close did not promptly interrupt in-flight RPC: %v", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ActivateSubscription did not return after Close")
	}
}

// TestJitteredBackoffClippedAtLargeAttempt 钉死 P2-2:attempt 极大导致指数移位
// 溢出为负时,退避被 4 倍基准封顶,而非退化为负值 busy 重试。
func TestJitteredBackoffClippedAtLargeAttempt(t *testing.T) {
	config := transientTestConfig()
	d := NewDispatcherWithConfig(&captureGateway{}, config)
	defer d.Close()
	maxBackoff := time.Duration(float64(4*config.TransientBackoff) * 1.2)
	for _, attempt := range []int{8, 16, 64, 1000} {
		backoff := d.jitteredBackoff(attempt)
		if backoff <= 0 || backoff > maxBackoff {
			t.Fatalf("attempt=%d backoff=%v out of bounds (must clip to 4x base)", attempt, backoff)
		}
	}
}
