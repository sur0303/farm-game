// Package downlink keeps network delivery outside Actor turns.
package downlink

import (
	"context"
	"errors"
	"hash/fnv"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	logicrpcpb "farmgame/api/gen/logicrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrDispatcherFull          = errors.New("logic downlink: gateway lane queue full")
	ErrDispatcherClosed        = errors.New("logic downlink: dispatcher closed")
	ErrGatewayRouteUnavailable = errors.New("logic downlink: gateway route unavailable")
	ErrGatewayLaneLimit        = errors.New("logic downlink: gateway lane limit reached")
)

type GatewayClient interface {
	DeliverBatch(context.Context, *logicrpcpb.DeliverBatchRequest, ...grpc.CallOption) (*logicrpcpb.DeliverBatchResponse, error)
}

type GatewaySubscriptionClient interface {
	GatewayClient
	ActivateSubscription(context.Context, *logicrpcpb.ActivateSubscriptionRequest, ...grpc.CallOption) (*logicrpcpb.ActivateSubscriptionResponse, error)
}

// GatewayClientResolver lets Dispatcher bypass a batch-level router and put
// each gateway_id into its own queue/worker/RPC deadline. RoutedGatewayClient
// implements this interface; a plain client uses one shared "single" lane.
type GatewayClientResolver interface {
	ResolveGateway(gatewayID string) GatewayClient
}

type DispatcherConfig struct {
	LaneQueueSize    int
	ControlQueueSize int
	BatchSize        int
	RPCTimeout       time.Duration
	LaneIdleTimeout  time.Duration
	MaxConcurrent    int
	MaxLanes         int
	// LaneWorkers splits one gateway_id into this many connection-hashed
	// sub-lanes so a single Gate can run concurrent DeliverBatch RPCs.
	// Items for the same connection_id stay on one sub-lane and keep FIFO.
	// Zero or one preserves the historical single-worker lane.
	LaneWorkers int
	// TransientMaxAttempts/TransientBackoff/TransientBudget 约束对"明确的瞬时
	// 连接不可用"(gRPC Unavailable:Gate 未监听/连接被拒/传输未就绪,即启动
	// 竞态)的有界重试:尝试次数与总预算双上限。零值取默认。
	TransientMaxAttempts int
	TransientBackoff     time.Duration
	TransientBudget      time.Duration
}

func DefaultDispatcherConfig() DispatcherConfig {
	return DispatcherConfig{
		LaneQueueSize: 256, ControlQueueSize: 32, BatchSize: 32, RPCTimeout: 2 * time.Second,
		LaneIdleTimeout: time.Minute, MaxConcurrent: 16, MaxLanes: 1024, LaneWorkers: 1,
		TransientMaxAttempts: 4, TransientBackoff: 100 * time.Millisecond, TransientBudget: 2 * time.Second,
	}
}

type DispatcherStats struct {
	LanesCurrent         uint64
	LanesCreated         uint64
	LanesRetired         uint64
	QueueItems           uint64
	QueueCapacity        uint64
	ControlQueueItems    uint64
	ControlQueueCapacity uint64
	QueueFull            uint64
	RPCFailures          uint64
}

type Dispatcher struct {
	client   GatewayClient
	resolver GatewayClientResolver
	config   DispatcherConfig
	ctx      context.Context
	cancel   context.CancelFunc
	slots    chan struct{}
	metrics  *DispatcherMetrics

	mu     sync.Mutex
	lanes  map[string]*gatewayLane
	closed bool
	once   sync.Once
	wg     sync.WaitGroup

	lanesCreated atomic.Uint64
	lanesRetired atomic.Uint64
	queueFull    atomic.Uint64
	rpcFailures  atomic.Uint64
}

type gatewayLane struct {
	key     string
	client  GatewayClient
	queue   chan queuedItem
	control chan queuedItem
}

type queuedItem struct {
	item     *logicrpcpb.DownlinkItem
	callback func(*logicrpcpb.DeliverBatchItemResult)
}

// NewDispatcher retains the old constructor while changing the queue from one
// process-wide FIFO to bounded per-Gate lanes. queueSize is now the per-lane
// capacity; callers needing the other bounds use NewDispatcherWithConfig.
func NewDispatcher(client GatewayClient, queueSize int) *Dispatcher {
	config := DefaultDispatcherConfig()
	if queueSize > 0 {
		config.LaneQueueSize = queueSize
	}
	return NewDispatcherWithConfig(client, config)
}

func NewDispatcherWithConfig(client GatewayClient, config DispatcherConfig) *Dispatcher {
	defaults := DefaultDispatcherConfig()
	if config.LaneQueueSize <= 0 {
		config.LaneQueueSize = defaults.LaneQueueSize
	}
	if config.ControlQueueSize <= 0 {
		config.ControlQueueSize = defaults.ControlQueueSize
	}
	if config.BatchSize <= 0 {
		config.BatchSize = defaults.BatchSize
	}
	if config.RPCTimeout <= 0 {
		config.RPCTimeout = defaults.RPCTimeout
	}
	if config.LaneIdleTimeout <= 0 {
		config.LaneIdleTimeout = defaults.LaneIdleTimeout
	}
	if config.MaxConcurrent <= 0 {
		config.MaxConcurrent = defaults.MaxConcurrent
	}
	if config.MaxLanes <= 0 {
		config.MaxLanes = defaults.MaxLanes
	}
	if config.LaneWorkers <= 0 {
		config.LaneWorkers = defaults.LaneWorkers
	}
	if config.LaneWorkers > 16 {
		config.LaneWorkers = 16
	}
	if config.TransientMaxAttempts <= 0 {
		config.TransientMaxAttempts = defaults.TransientMaxAttempts
	}
	if config.TransientBackoff <= 0 {
		config.TransientBackoff = defaults.TransientBackoff
	}
	if config.TransientBudget <= 0 {
		config.TransientBudget = defaults.TransientBudget
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &Dispatcher{
		client: client, config: config, ctx: ctx, cancel: cancel,
		slots:   make(chan struct{}, config.MaxConcurrent),
		lanes:   make(map[string]*gatewayLane),
		metrics: newDispatcherMetrics(),
	}
	if resolver, ok := client.(GatewayClientResolver); ok {
		d.resolver = resolver
	}
	return d
}

func (d *Dispatcher) Enqueue(item *logicrpcpb.DownlinkItem) error {
	return d.EnqueueWithResult(item, nil)
}

// ActivateSubscription keeps the network RPC outside FarmActor turns. The
// Gate response means SUBSCRIPTION_READY was accepted into the exact bounded
// ConnectionWriter, not that the browser has already consumed it.
// 与 DeliverBatch 一致:只对明确的瞬时 Unavailable(启动竞态)做有界重试,
// Ready 排序的最终保障仍是 Join 回调链本身;业务态响应(含 STALE 拒绝)立即返回。
// 生命周期/容量语义:
//   - 关闭期间调用返回 ErrDispatcherClosed,绝不返回 (nil, nil);
//   - 与 DeliverBatch 共享 MaxConcurrent 并发槽;
//   - 单次 RPC 截止 = min(RPCTimeout, 剩余瞬时预算, 调用方 deadline)。
func (d *Dispatcher) ActivateSubscription(ctx context.Context,
	req *logicrpcpb.ActivateSubscriptionRequest) (*logicrpcpb.ActivateSubscriptionResponse, error) {
	if d == nil || req == nil || req.Target == nil {
		return nil, ErrGatewayRouteUnavailable
	}
	if !d.isOpen() {
		return nil, ErrDispatcherClosed
	}
	_, client := d.resolve(req.Target.GatewayId)
	activator, ok := client.(GatewaySubscriptionClient)
	if !ok {
		return nil, ErrGatewayRouteUnavailable
	}
	deadline := time.Now().Add(d.config.TransientBudget)
	for attempt := 1; ; attempt++ {
		d.metrics.attempt(metricMethodActivate, attempt)
		var response *logicrpcpb.ActivateSubscriptionResponse
		var rpcErr error
		if err := d.acquireConcurrencySlot(); err != nil {
			d.metrics.result(metricMethodActivate, metricResultError)
			return nil, ErrDispatcherClosed
		}
		// RPC 父上下文为 d.ctx:Close 立即打断在途 RPC;调用方 deadline 已由
		// rpcDeadline 折叠进截止时间。
		rpcCtx, cancel := context.WithDeadline(d.ctx, d.rpcDeadline(ctx, deadline))
		// 单次 RPC 仍 fail-fast(不 WaitForReady),重试只发生在明确的瞬时
		// Unavailable 上;WaitForReady 会让缺席 Gate 无限期占有 worker。
		response, rpcErr = activator.ActivateSubscription(rpcCtx, req)
		cancel()
		<-d.slots
		if rpcErr == nil && response == nil {
			// Gate 空响应视为路由失败,禁止 (nil, nil) 逸出。
			rpcErr = ErrGatewayRouteUnavailable
		}
		if d.ctx.Err() != nil {
			// 关闭优先:关闭期间的在途 RPC 一律收口为 ErrDispatcherClosed,
			// 不对外暴露真实 RPC 结果。
			d.metrics.result(metricMethodActivate, metricResultError)
			return nil, ErrDispatcherClosed
		}
		if rpcErr == nil {
			d.metrics.result(metricMethodActivate, metricResultOK)
			return response, nil
		}
		if !isTransientDeliveryError(rpcErr) {
			log.Printf("logic downlink: ActivateSubscription failed gateway=%s subscription=%s attempt=%d/%d: %v",
				req.Target.GatewayId, req.GetSubscription().GetSubscriptionId(),
				attempt, d.config.TransientMaxAttempts, rpcErr)
			d.metrics.result(metricMethodActivate, metricResultError)
			return response, rpcErr
		}
		if attempt >= d.config.TransientMaxAttempts || !time.Now().Before(deadline) {
			log.Printf("logic downlink: ActivateSubscription gave up gateway=%s subscription=%s attempt=%d/%d: %v",
				req.Target.GatewayId, req.GetSubscription().GetSubscriptionId(),
				attempt, d.config.TransientMaxAttempts, rpcErr)
			d.metrics.result(metricMethodActivate, metricResultUnavailable)
			d.metrics.exhausted(metricMethodActivate)
			return response, rpcErr
		}
		log.Printf("logic downlink: ActivateSubscription transient unavailable, retrying gateway=%s subscription=%s attempt=%d/%d: %v",
			req.Target.GatewayId, req.GetSubscription().GetSubscriptionId(),
			attempt, d.config.TransientMaxAttempts, rpcErr)
		if !d.sleepTransientBackoffWithContext(ctx, attempt, deadline) {
			d.metrics.result(metricMethodActivate, metricResultError)
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, ErrDispatcherClosed
		}
	}
}

func (d *Dispatcher) sleepTransientBackoffWithContext(ctx context.Context, attempt int, deadline time.Time) bool {
	backoff := d.jitteredBackoff(attempt)
	if remaining := time.Until(deadline); backoff > remaining {
		backoff = remaining
	}
	if backoff <= 0 {
		return true
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-d.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// EnqueueWithResult performs no network work in the Actor turn. Items for one
// gateway_id retain FIFO order; callbacks run in that same lane and in batch
// order, so an asynchronous Join continuation cannot overtake an older result.
func (d *Dispatcher) EnqueueWithResult(item *logicrpcpb.DownlinkItem, callback func(*logicrpcpb.DeliverBatchItemResult)) error {
	return d.enqueue(item, callback, false)
}

// EnqueueControlWithResult uses a small reserved per-Gate lane for terminal
// subscription controls such as REJOIN_REQUIRED. A saturated ordinary
// FarmChange queue therefore cannot suppress the control that closes it.
func (d *Dispatcher) EnqueueControlWithResult(item *logicrpcpb.DownlinkItem,
	callback func(*logicrpcpb.DeliverBatchItemResult)) error {
	return d.enqueue(item, callback, true)
}

func (d *Dispatcher) enqueue(item *logicrpcpb.DownlinkItem,
	callback func(*logicrpcpb.DeliverBatchItemResult), control bool) error {
	if item == nil {
		return errors.New("logic downlink: nil item")
	}
	baseKey, client := d.resolve(item.GetGatewayId())
	if client == nil {
		return ErrGatewayRouteUnavailable
	}
	key := shardLaneKey(baseKey, item.GetConnectionId(), item.GetSubscriptionId(), d.config.LaneWorkers)

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrDispatcherClosed
	}
	lane := d.lanes[key]
	if lane == nil {
		if len(d.lanes) >= d.config.MaxLanes {
			return ErrGatewayLaneLimit
		}
		lane = &gatewayLane{
			key:     key,
			client:  client,
			queue:   make(chan queuedItem, d.config.LaneQueueSize),
			control: make(chan queuedItem, d.config.ControlQueueSize),
		}
		d.lanes[key] = lane
		d.lanesCreated.Add(1)
		d.wg.Add(1)
		go d.runLane(lane)
	}
	destination := lane.queue
	if control {
		destination = lane.control
	}
	select {
	case destination <- queuedItem{item: item, callback: callback}:
		return nil
	default:
		d.queueFull.Add(1)
		return ErrDispatcherFull
	}
}

func shardLaneKey(baseKey, connectionID, subscriptionID string, workers int) string {
	if workers <= 1 || baseKey == "" {
		return baseKey
	}
	seed := strings.TrimSpace(connectionID)
	if seed == "" {
		seed = strings.TrimSpace(subscriptionID)
	}
	if seed == "" {
		return baseKey + "/0"
	}
	sum := fnv.New32a()
	_, _ = sum.Write([]byte(seed))
	return baseKey + "/" + strconv.FormatUint(uint64(sum.Sum32()%uint32(workers)), 10)
}

func (d *Dispatcher) resolve(gatewayID string) (string, GatewayClient) {
	if d.resolver == nil {
		return "single", d.client
	}
	key := strings.TrimSpace(gatewayID)
	if key == "" {
		return "", nil
	}
	return key, d.resolver.ResolveGateway(key)
}

func (d *Dispatcher) runLane(lane *gatewayLane) {
	defer d.wg.Done()
	timer := time.NewTimer(d.config.LaneIdleTimeout)
	defer timer.Stop()
	for {
		// Terminal controls have a reserved lane and may overtake queued ordinary
		// changes after continuity has already been lost. Gate fences make any
		// later stale change harmless, while prompt terminal delivery prevents a
		// saturated change lane from trapping the client on an invalid stream.
		select {
		case first := <-lane.control:
			stopAndDrainTimer(timer)
			d.deliverLaneBatch(lane, d.collectLaneBatch(lane, first))
			timer.Reset(d.config.LaneIdleTimeout)
			continue
		default:
		}
		select {
		case <-d.ctx.Done():
			d.drainLaneUnavailable(lane)
			return
		case first := <-lane.control:
			stopAndDrainTimer(timer)
			d.deliverLaneBatch(lane, d.collectLaneBatch(lane, first))
			timer.Reset(d.config.LaneIdleTimeout)
		case first := <-lane.queue:
			stopAndDrainTimer(timer)
			d.deliverLaneBatch(lane, d.collectLaneBatch(lane, first))
			timer.Reset(d.config.LaneIdleTimeout)
		case <-timer.C:
			if d.retireLane(lane) {
				return
			}
			timer.Reset(d.config.LaneIdleTimeout)
		}
	}
}

func (d *Dispatcher) collectLaneBatch(lane *gatewayLane, first queuedItem) []queuedItem {
	batch := []queuedItem{first}
	for len(batch) < d.config.BatchSize {
		// Prefer reserved controls whenever one is already available. Ordinary
		// items still make progress once the bounded control lane is drained.
		select {
		case item := <-lane.control:
			batch = append(batch, item)
			continue
		default:
		}
		select {
		case item := <-lane.control:
			batch = append(batch, item)
		case item := <-lane.queue:
			batch = append(batch, item)
		default:
			return batch
		}
	}
	return batch
}

func (d *Dispatcher) deliverLaneBatch(lane *gatewayLane, batch []queuedItem) {
	items := make([]*logicrpcpb.DownlinkItem, 0, len(batch))
	for _, queued := range batch {
		items = append(items, queued.item)
	}

	response, deliveryErr := d.deliverBatchWithRetry(lane, items)
	if deliveryErr != nil {
		d.rpcFailures.Add(1)
		log.Printf("logic downlink: DeliverBatch failed gateway=%s items=%d: %v", lane.key, len(items), deliveryErr)
	}

	results := make(map[string]*logicrpcpb.DeliverBatchItemResult, len(batch))
	statusCounts := make(map[logicrpcpb.DownlinkItemStatus]int)
	if response != nil {
		for _, result := range response.Results {
			if result != nil && result.ItemId != "" {
				results[result.ItemId] = result
				statusCounts[result.Status]++
			}
		}
	}
	if deliveryErr == nil {
		if missing := len(batch) - len(results); missing > 0 {
			log.Printf("logic downlink: DeliverBatch missing results gateway=%s items=%d results=%d", lane.key, len(batch), len(results))
		}
		for status, count := range statusCounts {
			if status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
				log.Printf("logic downlink: DeliverBatch status=%s gateway=%s count=%d items=%d", status.String(), lane.key, count, len(batch))
			}
		}
	}
	for _, queued := range batch {
		if queued.callback == nil {
			continue
		}
		result := results[queued.item.GetItemId()]
		if result == nil {
			result = unavailable(queued.item.GetItemId())
		}
		queued.callback(result)
	}
}

// deliverBatchWithRetry 只对明确的瞬时不可用(gRPC Unavailable:Gate 尚未监听、
// 连接被拒、传输层未 Ready——即启动竞态)做有界、带 deadline 的重试。
// 约束:
//   - 有界:尝试次数与总预算双上限,退避递增;
//   - 业务拒绝(STALE_*/REJOIN 等)作为正常响应的条目结果返回,从不进入重试;
//   - DeadlineExceeded/InvalidArgument 等非瞬时错误立即失败,不放大病态 Gate;
//   - 不在 Actor turn 内等待网络:本函数只在 lane goroutine 执行,
//     且可被 Dispatcher 关闭打断。
func (d *Dispatcher) deliverBatchWithRetry(lane *gatewayLane, items []*logicrpcpb.DownlinkItem) (*logicrpcpb.DeliverBatchResponse, error) {
	deadline := time.Now().Add(d.config.TransientBudget)
	for attempt := 1; ; attempt++ {
		d.metrics.attempt(metricMethodDeliver, attempt)
		var response *logicrpcpb.DeliverBatchResponse
		var deliveryErr error
		if err := d.acquireConcurrencySlot(); err == nil {
			rpcCtx, cancel := context.WithDeadline(d.ctx, d.rpcDeadline(d.ctx, deadline))
			// 单次 RPC 仍 fail-fast(不 WaitForReady),重试只发生在明确的瞬时
			// Unavailable 上;WaitForReady 会让缺席 Gate 无限期占有 worker。
			response, deliveryErr = lane.client.DeliverBatch(rpcCtx, &logicrpcpb.DeliverBatchRequest{Items: items})
			cancel()
			<-d.slots
		} else {
			deliveryErr = err
		}
		if deliveryErr == nil {
			d.metrics.result(metricMethodDeliver, metricResultOK)
			return response, nil
		}
		if !isTransientDeliveryError(deliveryErr) {
			d.metrics.result(metricMethodDeliver, metricResultError)
			return response, deliveryErr
		}
		if attempt >= d.config.TransientMaxAttempts || !time.Now().Before(deadline) {
			d.metrics.result(metricMethodDeliver, metricResultUnavailable)
			d.metrics.exhausted(metricMethodDeliver)
			return response, deliveryErr
		}
		log.Printf("logic downlink: DeliverBatch transient unavailable, retrying gateway=%s items=%d attempt=%d/%d: %v",
			lane.key, len(items), attempt, d.config.TransientMaxAttempts, deliveryErr)
		if !d.sleepTransientBackoff(attempt, deadline) {
			d.metrics.result(metricMethodDeliver, metricResultError)
			return nil, d.ctx.Err()
		}
	}
}

// isTransientDeliveryError 只认 gRPC Unavailable。业务拒绝随正常响应返回,
// 永远不会成为 RPC error;其余错误码一律终态。
func isTransientDeliveryError(err error) bool {
	return status.Code(err) == codes.Unavailable
}

// isOpen 报告调度器是否仍接受新工作;关闭后 ActivateSubscription 立即返回
// ErrDispatcherClosed,避免对已关闭实例产生 (nil, nil) 或悬空 RPC。
func (d *Dispatcher) isOpen() bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return !d.closed
}

// acquireConcurrencySlot 领取一个 MaxConcurrent 共享并发槽。ActivateSubscription
// 与 DeliverBatch 使用同一个槽池,因此两条链路总并发恒被上限约束;关闭后
// 所有等待立即失败,不再占用 worker。
func (d *Dispatcher) acquireConcurrencySlot() error {
	select {
	case <-d.ctx.Done():
		return d.ctx.Err()
	default:
	}
	select {
	case d.slots <- struct{}{}:
		return nil
	case <-d.ctx.Done():
		return d.ctx.Err()
	}
}

// rpcDeadline 返回单次 RPC 的截止时间:RPCTimeout、剩余瞬时预算、调用方
// deadline 三者取最小。调用方无 deadline 时该维度不参与。
func (d *Dispatcher) rpcDeadline(ctx context.Context, budgetDeadline time.Time) time.Time {
	deadline := time.Now().Add(d.config.RPCTimeout)
	if remaining := time.Until(budgetDeadline); remaining < d.config.RPCTimeout {
		deadline = budgetDeadline
	}
	if callerDeadline, ok := ctx.Deadline(); ok && callerDeadline.Before(deadline) {
		deadline = callerDeadline
	}
	return deadline
}

// jitteredBackoff 计算第 attempt 次重试的退避时长:指数增长、4 倍基准封顶,
// 并叠加稳定 jitter(同一 attempt 的倍率完全确定,便于复现与压测)。
func (d *Dispatcher) jitteredBackoff(attempt int) time.Duration {
	backoff := d.config.TransientBackoff << (attempt - 1)
	if maxBackoff := 4 * d.config.TransientBackoff; backoff <= 0 || backoff > maxBackoff {
		backoff = maxBackoff
	}
	return time.Duration(float64(backoff) * stableJitterFraction(attempt))
}

// stableJitterFraction 返回第 attempt 次退避的确定性倍率 [0.8, 1.2):
// 仅依赖 attempt,并发实例与重复运行得到完全相同的序列(稳定),
// 相邻 attempt 又有差异(抖动),避免恢复中的 Gate 被惊群压垮。
func stableJitterFraction(attempt int) float64 {
	x := uint64(attempt) + 0x9E3779B97F4A7C15
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return 0.8 + float64(x%1000)/1000.0*0.4
}

func (d *Dispatcher) sleepTransientBackoff(attempt int, deadline time.Time) bool {
	backoff := d.jitteredBackoff(attempt)
	if remaining := time.Until(deadline); backoff > remaining {
		backoff = remaining
	}
	if backoff <= 0 {
		return true
	}
	timer := time.NewTimer(backoff)
	defer timer.Stop()
	select {
	case <-d.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (d *Dispatcher) retireLane(lane *gatewayLane) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return false
	}
	if current := d.lanes[lane.key]; current != lane || len(lane.queue) != 0 || len(lane.control) != 0 {
		return false
	}
	delete(d.lanes, lane.key)
	d.lanesRetired.Add(1)
	return true
}

func (d *Dispatcher) drainLaneUnavailable(lane *gatewayLane) {
	for {
		select {
		case queued := <-lane.control:
			reportUnavailable(queued)
			continue
		default:
		}
		select {
		case queued := <-lane.control:
			reportUnavailable(queued)
		case queued := <-lane.queue:
			reportUnavailable(queued)
		default:
			return
		}
	}
}

func reportUnavailable(queued queuedItem) {
	if queued.callback != nil {
		queued.callback(unavailable(queued.item.GetItemId()))
	}
}

func (d *Dispatcher) Stats() DispatcherStats {
	if d == nil {
		return DispatcherStats{}
	}
	d.mu.Lock()
	current := len(d.lanes)
	var queueItems, queueCapacity, controlItems, controlCapacity uint64
	for _, lane := range d.lanes {
		queueItems += uint64(len(lane.queue))
		queueCapacity += uint64(cap(lane.queue))
		controlItems += uint64(len(lane.control))
		controlCapacity += uint64(cap(lane.control))
	}
	d.mu.Unlock()
	return DispatcherStats{
		LanesCurrent: uint64(current), LanesCreated: d.lanesCreated.Load(),
		LanesRetired: d.lanesRetired.Load(), QueueItems: queueItems,
		QueueCapacity: queueCapacity, ControlQueueItems: controlItems,
		ControlQueueCapacity: controlCapacity, QueueFull: d.queueFull.Load(),
		RPCFailures: d.rpcFailures.Load(),
	}
}

func (d *Dispatcher) Close() {
	if d == nil {
		return
	}
	d.once.Do(func() {
		d.mu.Lock()
		d.closed = true
		d.mu.Unlock()
		d.cancel()
		d.wg.Wait()
	})
}

func stopAndDrainTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
