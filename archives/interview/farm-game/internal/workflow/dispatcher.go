package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"farmgame/internal/workidentity"
)

var (
	// ErrDispatcherFullItems / ErrDispatcherFullBytes are the backpressure
	// signals of the bounded in-memory queue. They never lose work: the
	// durable record and its due index entry were committed before enqueue,
	// and the recovery loop will pick the work up after next_due_at.
	ErrDispatcherFullItems = errors.New("workflow: dispatcher item capacity reached")
	ErrDispatcherFullBytes = errors.New("workflow: dispatcher byte capacity reached")
)

// DispatcherConfig bounds the DirectDispatcher fast path. All values are
// configuration candidates (PHYSICAL_PENDING) and must come from versioned
// configuration with metrics, never hardcoded business rules.
type DispatcherConfig struct {
	MaxItems     int
	MaxBytes     int
	ItemDeadline time.Duration
	Workers      int
	Backoff      BackoffConfig
}

func (c DispatcherConfig) withDefaults() DispatcherConfig {
	if c.MaxItems <= 0 {
		c.MaxItems = 1024
	}
	if c.MaxBytes <= 0 {
		c.MaxBytes = 4 << 20
	}
	if c.ItemDeadline <= 0 {
		c.ItemDeadline = 5 * time.Second
	}
	if c.Workers <= 0 {
		c.Workers = 4
	}
	c.Backoff = c.Backoff.withDefaults()
	return c
}

// DispatcherStats is the observable state of the fast path.
type DispatcherStats struct {
	Depth      int64
	Bytes      int64
	Enqueued   uint64
	Dropped    uint64
	Dispatched uint64
	Completed  uint64
	Reschedule uint64
	Failed     uint64
}

type dispatchItem struct {
	address workidentity.WorkAddress
	size    int
}

// DirectDispatcher is the in-process fast path that runs right after a
// durable source commit. The in-memory queue only lowers latency; the
// durable record + due index remain the authority. Queue full, process
// crash or RPC failure never lose work — the recovery loop resumes from the
// due index. The dispatcher holds no cross-process work lease; its advances
// are fenced by work_version and participant receipts.
type DirectDispatcher struct {
	repo   WorkRepository
	engine *Engine
	config DispatcherConfig

	queue   chan dispatchItem
	bytes   atomic.Int64
	closing atomic.Bool
	wg      sync.WaitGroup

	enqueued   atomic.Uint64
	dropped    atomic.Uint64
	dispatched atomic.Uint64
	completed  atomic.Uint64
	reschedule atomic.Uint64
	failed     atomic.Uint64
}

func NewDirectDispatcher(repo WorkRepository, engine *Engine, config DispatcherConfig) (*DirectDispatcher, error) {
	if repo == nil || engine == nil {
		return nil, fmt.Errorf("%w: repository and engine required", ErrInvalid)
	}
	config = config.withDefaults()
	d := &DirectDispatcher{
		repo:   repo,
		engine: engine,
		config: config,
		queue:  make(chan dispatchItem, config.MaxItems),
	}
	return d, nil
}

// Enqueue offers a fully addressed work to the fast path. The caller must
// have already durably committed the record and its due index entry; a full
// queue is a backpressure signal, not a loss.
func (d *DirectDispatcher) Enqueue(address workidentity.WorkAddress, payloadBytes int) error {
	if err := address.ValidateShape(); err != nil {
		return err
	}
	if d.closing.Load() {
		return ErrDispatcherFullItems
	}
	if payloadBytes < 0 {
		payloadBytes = 0
	}
	// Reserve byte budget first so the item/byte bounds both hold under
	// concurrent enqueues.
	if d.bytes.Add(int64(payloadBytes)) > int64(d.config.MaxBytes) {
		d.bytes.Add(-int64(payloadBytes))
		d.dropped.Add(1)
		return ErrDispatcherFullBytes
	}
	item := dispatchItem{address: address, size: payloadBytes}
	select {
	case d.queue <- item:
		d.enqueued.Add(1)
		return nil
	default:
		d.bytes.Add(-int64(payloadBytes))
		d.dropped.Add(1)
		return ErrDispatcherFullItems
	}
}

// Start launches the bounded worker set. Workers never block an Actor Turn:
// the dispatcher runs outside any actor mailbox.
func (d *DirectDispatcher) Start(ctx context.Context) {
	for i := 0; i < d.config.Workers; i++ {
		d.wg.Add(1)
		go d.loop(ctx)
	}
}

// Close stops accepting new items and waits for in-flight dispatches.
func (d *DirectDispatcher) Close() {
	d.closing.Store(true)
	d.wg.Wait()
}

func (d *DirectDispatcher) Stats() DispatcherStats {
	return DispatcherStats{
		Depth:      int64(len(d.queue)),
		Bytes:      d.bytes.Load(),
		Enqueued:   d.enqueued.Load(),
		Dropped:    d.dropped.Load(),
		Dispatched: d.dispatched.Load(),
		Completed:  d.completed.Load(),
		Reschedule: d.reschedule.Load(),
		Failed:     d.failed.Load(),
	}
}

func (d *DirectDispatcher) loop(ctx context.Context) {
	defer d.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-d.queue:
			d.bytes.Add(-int64(item.size))
			d.dispatch(ctx, item.address)
		}
	}
}

// DispatchOnce processes one addressed work synchronously; it is exported
// for deterministic tests and for callers that already hold the item.
func (d *DirectDispatcher) DispatchOnce(ctx context.Context, address workidentity.WorkAddress) error {
	return d.dispatch(ctx, address)
}

func (d *DirectDispatcher) dispatch(ctx context.Context, address workidentity.WorkAddress) error {
	d.dispatched.Add(1)
	record, err := d.repo.Get(ctx, address.WorkType, address.WorkID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			d.failed.Add(1)
			return err
		}
		d.reschedule.Add(1)
		return err
	}
	if record.Terminal() {
		d.completed.Add(1)
		return nil
	}
	stepCtx, cancel := context.WithTimeout(ctx, d.config.ItemDeadline)
	err = d.engine.Resume(stepCtx, record, LeaseFence{}, nil)
	cancel()
	switch {
	case err == nil:
		d.completed.Add(1)
		return nil
	case errors.Is(err, ErrRetryScheduled):
		d.reschedule.Add(1)
		return err
	default:
		// Lease loss, revision conflict or storage faults: the durable record
		// still carries next_due_at; the recovery loop takes over.
		d.reschedule.Add(1)
		return err
	}
}
