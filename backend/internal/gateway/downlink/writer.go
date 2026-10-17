package downlink

import (
	"errors"
	"sync"
	"time"

	commonpb "farmgame/api/gen/common"
	"google.golang.org/protobuf/proto"
)

var (
	ErrQueueFull       = errors.New("downlink: queue full")
	ErrStaleGeneration = errors.New("downlink: stale connection generation")
	ErrClosed          = errors.New("downlink: closed")
	ErrHintQuota       = errors.New("downlink: hint soft quota exceeded")
)

// FrameKind is a bounded classification of a queued downlink frame. It is
// deliberately a small fixed whitelist; connection_id/player_id never become
// label values anywhere in the writer's aggregated observability.
type FrameKind uint8

const (
	FrameKindOther FrameKind = iota
	FrameKindHint
	FrameKindFarmChange
	FrameKindCommandResult
	FrameKindControl
	FrameKindSession
	FrameKindSystem
)

func (k FrameKind) String() string {
	switch k {
	case FrameKindHint:
		return "hint"
	case FrameKindFarmChange:
		return "farm_change"
	case FrameKindCommandResult:
		return "command_result"
	case FrameKindControl:
		return "control"
	case FrameKindSession:
		return "session"
	case FrameKindSystem:
		return "system"
	default:
		return "other"
	}
}

// HintPolicy is a bounded hint soft-quota policy evaluated before a frame is
// admitted to the FIFO. Hint soft quota is best-effort: exceeding it rejects
// the hint before sequence allocation so already-sequenced frames in the FIFO
// are never selectively dropped or reordered (28 L2-PROTO-26).
type HintPolicy struct {
	MaxHintItems int
	MaxHintBytes int
}

func (p HintPolicy) IsZero() bool { return p.MaxHintItems <= 0 && p.MaxHintBytes <= 0 }

type enqueueResult struct {
	kind   FrameKind
	result string
	reason string
}

type Item struct {
	Generation uint64
	Data       []byte
	Kind       FrameKind
	enqueuedAt time.Time
}

// ConnectionWriter 不在 Actor turn 内执行网络写；Enqueue 只做有界入队。
// notifier 是 level-triggered 的 pending/re-notify 信号:Enqueue 成功后
// Signal 一次,消费者每次唤醒排空到量子上限后重新 WaitCh 并再查 Pending,
// 因此没有第二次入队也能把剩余帧全部排空,且量子之间仍回到 select。
type ConnectionWriter struct {
	mu         sync.Mutex
	queue      []Item
	bytes      int
	maxItems   int
	maxBytes   int
	generation uint64
	closed     bool
	notifier   LevelTriggeredNotifier

	hintItems int
	hintBytes int
	hintCap   HintPolicy
	metrics   *Metrics

	enqueuedItems map[FrameKind]uint64
	enqueuedBytes map[FrameKind]uint64
	rejected      map[rejectKey]uint64
	oldestAge     map[FrameKind]time.Duration
}

type rejectKey struct {
	kind   FrameKind
	result string
	reason string
}

// WriterStats is the low-cardinality aggregate snapshot of one writer. It is
// used by fixtures/tests and by the registry-wide metrics; labels are the
// bounded FrameKind/result/reason whitelist and never carry identity values.
type WriterStats struct {
	Items         int
	Bytes         int
	HintItems     int
	HintBytes     int
	EnqueuedItems map[FrameKind]uint64
	EnqueuedBytes map[FrameKind]uint64
	Rejected      map[string]uint64
	OldestAge     map[FrameKind]time.Duration
}

// QueueSnapshot is the allocation-free live subset used by Prometheus Gauge
// functions. Full Stats intentionally retains maps for JSON evidence/tests,
// but metrics must not allocate one map per connection on every scrape.
type QueueSnapshot struct {
	Items     int
	Bytes     int
	HintItems int
	HintBytes int
	OldestAge time.Duration
}

// RejectedKey formats a bounded reject observation as kind/result/reason.
func RejectedKey(kind FrameKind, result, reason string) string {
	return kind.String() + "/" + result + "/" + reason
}

func NewConnectionWriter(generation uint64, maxItems, maxBytes int) *ConnectionWriter {
	return NewConnectionWriterWithHint(generation, maxItems, maxBytes, HintPolicy{})
}

func NewConnectionWriterWithHint(generation uint64, maxItems, maxBytes int, hint HintPolicy) *ConnectionWriter {
	return NewConnectionWriterWithCapacity(generation, WriterCapacity{MaxItems: maxItems, MaxBytes: maxBytes, Hint: hint})
}

// NewConnectionWriterWithCapacity constructs a writer with the full bounded
// capacity parameter set (total item/byte caps plus hint soft quota). Fixtures
// pass explicit values; production wiring can adopt the same constructor when
// the C6A config seam lands.
func NewConnectionWriterWithCapacity(generation uint64, capacity WriterCapacity) *ConnectionWriter {
	maxItems := capacity.MaxItems
	if maxItems <= 0 {
		maxItems = 256
	}
	maxBytes := capacity.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	return &ConnectionWriter{
		generation: generation, maxItems: maxItems, maxBytes: maxBytes,
		hintCap:       capacity.Hint,
		enqueuedItems: make(map[FrameKind]uint64), enqueuedBytes: make(map[FrameKind]uint64),
		rejected: make(map[rejectKey]uint64), oldestAge: make(map[FrameKind]time.Duration),
	}
}

func (w *ConnectionWriter) Generation() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.generation
}

// SetMetrics attaches the shared registry metrics collector. It is safe before
// serving traffic; nil detaches. The writer records enqueue/reject counters on
// every admission path so the registry-wide totals can never miss a path.
func (w *ConnectionWriter) SetMetrics(m *Metrics) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.metrics = m
}

func (w *ConnectionWriter) Enqueue(generation uint64, data []byte) error {
	return w.EnqueueKind(generation, FrameKindOther, data)
}

func (w *ConnectionWriter) EnqueueKind(generation uint64, kind FrameKind, data []byte) error {
	_, err := w.enqueue(generation, kind, data)
	return err
}

func (w *ConnectionWriter) enqueue(generation uint64, kind FrameKind, data []byte) (enqueueResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		w.writeRejectedLocked(kind, "reject", "closed")
		return enqueueResult{kind: kind, result: "reject", reason: "closed"}, ErrClosed
	}
	if generation != w.generation {
		w.writeRejectedLocked(kind, "reject", "stale")
		return enqueueResult{kind: kind, result: "reject", reason: "stale"}, ErrStaleGeneration
	}
	if len(w.queue) >= w.maxItems || w.bytes+len(data) > w.maxBytes {
		w.writeRejectedLocked(kind, "reject", "full")
		return enqueueResult{kind: kind, result: "reject", reason: "full"}, ErrQueueFull
	}
	if kind == FrameKindHint {
		// Each quota dimension is enforced only when configured: MaxHintItems
		// and MaxHintBytes are independent soft limits, so an item-only policy
		// must never reject on the zero byte dimension and vice versa.
		if w.hintCap.MaxHintItems > 0 && w.hintItems >= w.hintCap.MaxHintItems {
			w.writeRejectedLocked(kind, "reject", "quota")
			return enqueueResult{kind: kind, result: "reject", reason: "quota"}, ErrHintQuota
		}
		if w.hintCap.MaxHintBytes > 0 && w.hintBytes+len(data) > w.hintCap.MaxHintBytes {
			w.writeRejectedLocked(kind, "reject", "quota")
			return enqueueResult{kind: kind, result: "reject", reason: "quota"}, ErrHintQuota
		}
	}
	now := time.Now()
	w.queue = append(w.queue, Item{Generation: generation, Data: append([]byte(nil), data...), Kind: kind, enqueuedAt: now})
	w.bytes += len(data)
	if kind == FrameKindHint {
		w.hintItems++
		w.hintBytes += len(data)
	}
	w.notifier.Signal()
	w.writeAcceptedLocked(kind, len(data))
	return enqueueResult{kind: kind, result: "accepted", reason: ""}, nil
}

func (w *ConnectionWriter) writeAcceptedLocked(kind FrameKind, bytes int) {
	w.enqueuedItems[kind]++
	w.enqueuedBytes[kind] += uint64(bytes)
	if w.metrics != nil {
		w.metrics.WriterEnqueuedItems.WithLabelValues(kind.String()).Inc()
		w.metrics.WriterEnqueuedBytes.WithLabelValues(kind.String()).Add(float64(bytes))
	}
}

func (w *ConnectionWriter) writeRejectedLocked(kind FrameKind, result, reason string) {
	w.rejected[rejectKey{kind: kind, result: result, reason: reason}]++
	if w.metrics != nil {
		w.metrics.WriterRejected.WithLabelValues(kind.String(), result, reason).Inc()
	}
}

// NotifyChan is the writer's level-triggered wait channel: it is readable only
// while a notification is pending and must be re-armed (called again) after a
// wakeup before the pump returns to select.
func (w *ConnectionWriter) NotifyChan() <-chan struct{} {
	return w.notifier.WaitCh()
}

// Pending is the non-destructive probe used after a drain quantum. The
// notifier stays readable while the queue is non-empty and is reset only after
// emptiness is observed under w.mu, so a concurrently ready select branch
// cannot consume and lose the continuation.
func (w *ConnectionWriter) Pending() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false
	}
	if len(w.queue) > 0 {
		w.notifier.Signal()
		return true
	}
	w.notifier.Reset()
	return false
}

// Closed distinguishes a terminal writer from a normal level-triggered
// notification. Both states make NotifyChan readable by closing its current
// channel, so consumers must not use the channel receive ok bit as a terminal
// signal.
func (w *ConnectionWriter) Closed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

func (w *ConnectionWriter) Dequeue() (Item, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.queue) == 0 {
		return Item{}, false
	}
	it := w.queue[0]
	w.queue[0] = Item{}
	w.queue = w.queue[1:]
	w.bytes -= len(it.Data)
	if it.Kind == FrameKindHint {
		w.hintItems--
		w.hintBytes -= len(it.Data)
	}
	return it, true
}

func (w *ConnectionWriter) Len() (items, bytes int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.queue), w.bytes
}

// QueueSnapshot returns only live queue gauges without allocating maps. It is
// deliberately separate from Stats because the latter is an evidence API.
func (w *ConnectionWriter) QueueSnapshot(now time.Time) QueueSnapshot {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := QueueSnapshot{Items: len(w.queue), Bytes: w.bytes, HintItems: w.hintItems, HintBytes: w.hintBytes}
	for _, item := range w.queue {
		if age := now.Sub(item.enqueuedAt); age > out.OldestAge {
			out.OldestAge = age
		}
	}
	return out
}

// Stats is the aggregate writer observation used by tests and fixtures.
// All label values are the bounded FrameKind/result/reason whitelist; no
// connection_id/player_id ever appears.
func (w *ConnectionWriter) Stats() WriterStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.statsLocked()
}

func (w *ConnectionWriter) statsLocked() WriterStats {
	out := WriterStats{
		Items: len(w.queue), Bytes: w.bytes,
		HintItems: w.hintItems, HintBytes: w.hintBytes,
		EnqueuedItems: make(map[FrameKind]uint64), EnqueuedBytes: make(map[FrameKind]uint64),
		Rejected: make(map[string]uint64), OldestAge: make(map[FrameKind]time.Duration),
	}
	for k, v := range w.enqueuedItems {
		out.EnqueuedItems[k] = v
	}
	for k, v := range w.enqueuedBytes {
		out.EnqueuedBytes[k] = v
	}
	for k, v := range w.rejected {
		key := k.kind.String() + "/" + k.result + "/" + k.reason
		out.Rejected[key] += v
	}
	now := time.Now()
	for _, it := range w.queue {
		age := now.Sub(it.enqueuedAt)
		if age > out.OldestAge[it.Kind] {
			out.OldestAge[it.Kind] = age
		}
	}
	return out
}

func (w *ConnectionWriter) Close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.queue = nil
	w.bytes = 0
	w.hintItems = 0
	w.hintBytes = 0
	w.notifier.Close()
	w.mu.Unlock()
}

// ClassifyFrameKind maps a normalized FarmFrame to its bounded FrameKind.
// Classification is a best-effort aggregate label; it never changes admission
// or ordering semantics.
func ClassifyFrameKind(raw []byte) FrameKind {
	var frame commonpb.FarmFrame
	if err := proto.Unmarshal(raw, &frame); err != nil {
		return FrameKindOther
	}
	push := frame.GetServerPush()
	if push == nil {
		if frame.GetCommandResult() != nil {
			return FrameKindCommandResult
		}
		return FrameKindOther
	}
	return ClassifyPushKind(push.PushType)
}

// ClassifyPushKind maps a public PushType to its bounded FrameKind whitelist.
func ClassifyPushKind(pushType commonpb.PushType) FrameKind {
	switch pushType {
	case commonpb.PushType_FRIEND_OPPORTUNITY_CHANGED:
		return FrameKindHint
	case commonpb.PushType_FARM_CHANGE:
		return FrameKindFarmChange
	case commonpb.PushType_KICK, commonpb.PushType_DRAIN_NOTICE, commonpb.PushType_REJOIN_REQUIRED:
		return FrameKindControl
	case commonpb.PushType_FRIEND_ROSTER_CHANGED:
		return FrameKindSession
	default:
		return FrameKindSystem
	}
}

// HintCapacity returns the configured hint soft quota. It is exposed so the
// fixture can assert quota semantics without reaching into the writer.
func (w *ConnectionWriter) HintCapacity() HintPolicy {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.hintCap
}

// RejectedKind returns the aggregate reject count for one bounded kind/result/
// reason tuple (e.g. hint/reject/quota). It is a test/fixture convenience and
// never emits identity labels.
func (s WriterStats) RejectedKind(kind FrameKind, result, reason string) uint64 {
	return s.Rejected[RejectedKey(kind, result, reason)]
}
