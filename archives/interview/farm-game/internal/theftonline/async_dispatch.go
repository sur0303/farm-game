package theftonline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	farmingress "farmgame/internal/farm/ingress"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	defaultAsyncDispatchItems   = 4096
	defaultAsyncDispatchBytes   = 8 << 20
	defaultAsyncDispatchWorkers = 16
	defaultAsyncDispatchTimeout = 5 * time.Second
	asyncDispatchItemOverhead   = 64
)

var (
	ErrAsyncDispatchClosed    = errors.New("theft online: async dispatch closed")
	ErrAsyncDispatchQueueFull = errors.New("theft online: async dispatch queue full")
)

// AsyncDispatchConfig bounds the non-authoritative online acceleration path.
// A queue miss never loses accepted Theft work: Pair SourceOrder + DueIndex is
// committed before this component is called and remains the recovery root.
type AsyncDispatchConfig struct {
	Delegate    farmingress.TheftDispatchPort
	MaxItems    int
	MaxBytes    int64
	Workers     int
	ItemTimeout time.Duration
}

// AsyncDispatch decouples public admission latency from Workflow execution.
// It deliberately uses a process-local bounded queue, not MQ and not another
// Redis correctness structure. Duplicate order ids are coalesced while queued
// or running; different orders execute concurrently up to Workers.
type AsyncDispatch struct {
	delegate farmingress.TheftDispatchPort
	queue    chan asyncDispatchItem
	maxBytes int64
	timeout  time.Duration

	mu          sync.Mutex
	pending     map[string]int64
	queuedBytes int64
	closed      bool

	baseCtx context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	enqueued       atomic.Uint64
	coalesced      atomic.Uint64
	queueFull      atomic.Uint64
	completed      atomic.Uint64
	delegateErrors atomic.Uint64
	inFlight       atomic.Int64
}

type asyncDispatchItem struct {
	orderID string
	bytes   int64
}

type AsyncDispatchStats struct {
	QueueItems     int
	QueueBytes     int64
	InFlight       int64
	Enqueued       uint64
	Coalesced      uint64
	QueueFull      uint64
	Completed      uint64
	DelegateErrors uint64
}

func NewAsyncDispatch(config AsyncDispatchConfig) (*AsyncDispatch, error) {
	if config.Delegate == nil {
		return nil, fmt.Errorf("%w: delegate is required", ErrDispatchUnavailable)
	}
	if config.MaxItems <= 0 {
		config.MaxItems = defaultAsyncDispatchItems
	}
	if config.MaxBytes <= 0 {
		config.MaxBytes = defaultAsyncDispatchBytes
	}
	if config.Workers <= 0 {
		config.Workers = defaultAsyncDispatchWorkers
	}
	if config.ItemTimeout <= 0 {
		config.ItemTimeout = defaultAsyncDispatchTimeout
	}
	baseCtx, cancel := context.WithCancel(context.Background())
	dispatch := &AsyncDispatch{
		delegate: config.Delegate,
		queue:    make(chan asyncDispatchItem, config.MaxItems),
		maxBytes: config.MaxBytes,
		timeout:  config.ItemTimeout,
		pending:  make(map[string]int64, config.MaxItems),
		baseCtx:  baseCtx,
		cancel:   cancel,
	}
	dispatch.wg.Add(config.Workers)
	for i := 0; i < config.Workers; i++ {
		go dispatch.runWorker()
	}
	return dispatch, nil
}

var _ farmingress.TheftDispatchPort = (*AsyncDispatch)(nil)
var _ farmingress.TheftTargetBindingRecorder = (*AsyncDispatch)(nil)

// DispatchTheft only admits the item into the local accelerator. It never
// waits for Redis Work creation or Participant RPCs and never launches a
// request-scoped goroutine.
func (d *AsyncDispatch) DispatchTheft(_ context.Context, orderID string) error {
	if d == nil {
		return ErrAsyncDispatchClosed
	}
	orderID = strings.TrimSpace(orderID)
	if orderID == "" {
		return fmt.Errorf("%w: empty order id", ErrDispatchUnavailable)
	}
	itemBytes := int64(len(orderID) + asyncDispatchItemOverhead)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrAsyncDispatchClosed
	}
	if _, exists := d.pending[orderID]; exists {
		d.coalesced.Add(1)
		return nil
	}
	if itemBytes > d.maxBytes || d.queuedBytes > d.maxBytes-itemBytes {
		d.queueFull.Add(1)
		return ErrAsyncDispatchQueueFull
	}
	item := asyncDispatchItem{orderID: orderID, bytes: itemBytes}
	select {
	case d.queue <- item:
		d.pending[orderID] = itemBytes
		d.queuedBytes += itemBytes
		d.enqueued.Add(1)
		return nil
	default:
		d.queueFull.Add(1)
		return ErrAsyncDispatchQueueFull
	}
}

func (d *AsyncDispatch) RecordTheftTargetBinding(ctx context.Context, binding farmingress.TheftTargetBinding) error {
	recorder, ok := d.delegate.(farmingress.TheftTargetBindingRecorder)
	if !ok {
		return fmt.Errorf("%w: target binding recorder is required", ErrDispatchUnavailable)
	}
	return recorder.RecordTheftTargetBinding(ctx, binding)
}

func (d *AsyncDispatch) runWorker() {
	defer d.wg.Done()
	for item := range d.queue {
		d.inFlight.Add(1)
		ctx, cancel := context.WithTimeout(d.baseCtx, d.timeout)
		err := d.delegate.DispatchTheft(ctx, item.orderID)
		cancel()
		if err != nil {
			d.delegateErrors.Add(1)
		} else {
			d.completed.Add(1)
		}
		d.inFlight.Add(-1)
		d.mu.Lock()
		delete(d.pending, item.orderID)
		d.queuedBytes -= item.bytes
		d.mu.Unlock()
	}
}

// Close stops admission and drains already-queued acceleration work. If the
// caller's shutdown budget expires, in-flight delegate calls are cancelled;
// the durable recovery worker will continue from Pair DueIndex.
func (d *AsyncDispatch) Close(ctx context.Context) error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	if !d.closed {
		d.closed = true
		close(d.queue)
	}
	d.mu.Unlock()
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		d.cancel()
		return nil
	case <-ctx.Done():
		d.cancel()
		return ctx.Err()
	}
}

func (d *AsyncDispatch) Stats() AsyncDispatchStats {
	if d == nil {
		return AsyncDispatchStats{}
	}
	d.mu.Lock()
	queueItems, queueBytes := len(d.pending), d.queuedBytes
	d.mu.Unlock()
	return AsyncDispatchStats{
		QueueItems: queueItems, QueueBytes: queueBytes, InFlight: d.inFlight.Load(),
		Enqueued: d.enqueued.Load(), Coalesced: d.coalesced.Load(), QueueFull: d.queueFull.Load(),
		Completed: d.completed.Load(), DelegateErrors: d.delegateErrors.Load(),
	}
}

func RegisterAsyncDispatchMetrics(registry *prometheus.Registry, dispatch *AsyncDispatch) {
	if registry == nil || dispatch == nil {
		return
	}
	registry.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "farm", Subsystem: "theft_dispatch", Name: "queue_items", Help: "Queued or in-flight Theft fast-path orders."}, func() float64 { return float64(dispatch.Stats().QueueItems) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "farm", Subsystem: "theft_dispatch", Name: "queue_bytes", Help: "Logical bytes retained by the Theft fast-path queue."}, func() float64 { return float64(dispatch.Stats().QueueBytes) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "farm", Subsystem: "theft_dispatch", Name: "in_flight", Help: "Theft fast-path delegate calls currently running."}, func() float64 { return float64(dispatch.Stats().InFlight) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "farm", Subsystem: "theft_dispatch", Name: "enqueued_total", Help: "Theft orders admitted to the local fast path."}, func() float64 { return float64(dispatch.Stats().Enqueued) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "farm", Subsystem: "theft_dispatch", Name: "coalesced_total", Help: "Duplicate Theft fast-path submissions coalesced by order id."}, func() float64 { return float64(dispatch.Stats().Coalesced) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "farm", Subsystem: "theft_dispatch", Name: "queue_full_total", Help: "Theft orders left to durable recovery because the local fast path was full."}, func() float64 { return float64(dispatch.Stats().QueueFull) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "farm", Subsystem: "theft_dispatch", Name: "completed_total", Help: "Theft orders completed successfully by the local fast path."}, func() float64 { return float64(dispatch.Stats().Completed) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "farm", Subsystem: "theft_dispatch", Name: "delegate_error_total", Help: "Theft fast-path delegate failures left to durable recovery."}, func() float64 { return float64(dispatch.Stats().DelegateErrors) }),
	)
}
