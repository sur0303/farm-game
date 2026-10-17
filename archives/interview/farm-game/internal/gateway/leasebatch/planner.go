// Package leasebatch provides the deterministic, bounded scheduling primitive
// used by FarmGate's independent SessionRoute and Subscription lease lanes.
// It deliberately knows nothing about Redis, gRPC, or Gate connection state:
// planning is pure local bookkeeping and executors apply their own fences.
package leasebatch

import (
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Limits are simultaneously enforced for every planned physical dispatch.
// MaxDispatchDuration is consumed by the executor as a context deadline; it is
// kept beside the item and byte limits so one validated object describes the
// complete batch budget.
type Limits struct {
	MaxItems            int
	MaxBytes            int
	MaxDispatchDuration time.Duration
}

// Schedule describes one deterministic renewal cycle. BucketCount divides the
// interval into stable phases and MaxJitter adds a second, bounded hash offset
// inside each bucket. Neither value is seeded from process-global state/time.
type Schedule struct {
	Interval    time.Duration
	BucketCount uint32
	MaxJitter   time.Duration
}

// Config is immutable after a Planner is built.
type Config struct {
	Limits
	Schedule
	MaxPlanScan int
}

func (c Config) Validate() error {
	if c.MaxItems <= 0 {
		return errors.New("leasebatch: max_items must be positive")
	}
	if c.MaxBytes <= 0 {
		return errors.New("leasebatch: max_bytes must be positive")
	}
	if c.MaxDispatchDuration <= 0 {
		return errors.New("leasebatch: max_dispatch_duration must be positive")
	}
	if c.Interval <= 0 {
		return errors.New("leasebatch: interval must be positive")
	}
	if c.BucketCount == 0 {
		return errors.New("leasebatch: bucket_count must be positive")
	}
	width := c.Interval / time.Duration(c.BucketCount)
	if width <= 0 {
		return errors.New("leasebatch: bucket_count exceeds interval resolution")
	}
	if c.MaxJitter < 0 || c.MaxJitter > width {
		return fmt.Errorf("leasebatch: max_jitter %s must be in [0,%s]", c.MaxJitter, width)
	}
	if c.MaxPlanScan < 0 {
		return errors.New("leasebatch: max_plan_scan must not be negative")
	}
	return nil
}

// ValidateSafety proves that one full schedule interval plus a worst-case
// physical dispatch completes before the local conservative deadline.
func (c Config) ValidateSafety(ttl, safetyMargin time.Duration) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if ttl <= 0 || safetyMargin < 0 || safetyMargin >= ttl {
		return fmt.Errorf("leasebatch: invalid ttl=%s safety_margin=%s", ttl, safetyMargin)
	}
	if c.Interval > time.Duration(1<<63-1)-c.MaxDispatchDuration {
		return errors.New("leasebatch: interval + dispatch duration overflows")
	}
	trustedWindow := ttl - safetyMargin
	if c.Interval+c.MaxDispatchDuration >= trustedWindow {
		return fmt.Errorf("leasebatch: interval %s + dispatch %s must be less than trusted window %s",
			c.Interval, c.MaxDispatchDuration, trustedWindow)
	}
	return nil
}

// StablePhase returns the bucket and bounded in-bucket jitter for identity.
// SHA-256 is used only as a stable distribution function, not for secrecy.
func StablePhase(identity string, schedule Schedule) (uint32, time.Duration, error) {
	if schedule.Interval <= 0 || schedule.BucketCount == 0 {
		return 0, 0, errors.New("leasebatch: invalid stable phase schedule")
	}
	width := schedule.Interval / time.Duration(schedule.BucketCount)
	if width <= 0 || schedule.MaxJitter < 0 || schedule.MaxJitter > width {
		return 0, 0, errors.New("leasebatch: invalid stable phase jitter")
	}
	sum := sha256.Sum256([]byte(identity))
	bucket := uint32(binary.BigEndian.Uint64(sum[:8]) % uint64(schedule.BucketCount))
	if schedule.MaxJitter == 0 {
		return bucket, 0, nil
	}
	// Convert before adding so MaxJitter==MaxInt64 cannot overflow signed
	// time.Duration. The inclusive modulus then fits exactly in uint64.
	jitterRange := uint64(schedule.MaxJitter) + 1
	jitter := time.Duration(binary.BigEndian.Uint64(sum[8:16]) % jitterRange)
	return bucket, jitter, nil
}

// NextDue maps identity onto the next absolute interval phase while returning
// a time derived with now.Add, preserving Go's monotonic component. Wall time
// selects the phase only; it is never part of the identity hash.
func NextDue(now time.Time, identity string, schedule Schedule) (time.Time, error) {
	bucket, jitter, err := StablePhase(identity, schedule)
	if err != nil {
		return time.Time{}, err
	}
	width := schedule.Interval / time.Duration(schedule.BucketCount)
	phase := time.Duration(bucket)*width + jitter
	if phase >= schedule.Interval {
		phase = schedule.Interval - time.Nanosecond
	}
	intervalNS := schedule.Interval.Nanoseconds()
	wallMod := now.UnixNano() % intervalNS
	if wallMod < 0 {
		wallMod += intervalNS
	}
	delta := phase - time.Duration(wallMod)
	if delta <= 0 {
		delta += schedule.Interval
	}
	return now.Add(delta), nil
}

// Item is a fully estimated, holder-isolated scheduling unit. Key is its local
// generation fence; Identity is the stable hash input and Holder is the exact
// dispatch partition. Payload is an immutable snapshot validated again by the
// executor against canonical Gate state.
type Item[T any] struct {
	Key            string
	Identity       string
	Holder         string
	Due            time.Time
	Deadline       time.Time
	EstimatedBytes int
	Payload        T
}

type node[T any] struct {
	item        Item[T]
	holderIndex int
	seq         uint64
}

type itemHeap[T any] []*node[T]

func (h itemHeap[T]) Len() int { return len(h) }
func (h itemHeap[T]) Less(i, j int) bool {
	if h[i].item.Due.Equal(h[j].item.Due) {
		if h[i].item.Key == h[j].item.Key {
			return h[i].seq < h[j].seq
		}
		return h[i].item.Key < h[j].item.Key
	}
	return h[i].item.Due.Before(h[j].item.Due)
}
func (h itemHeap[T]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].holderIndex = i
	h[j].holderIndex = j
}
func (h *itemHeap[T]) Push(value any) {
	n := value.(*node[T])
	n.holderIndex = len(*h)
	*h = append(*h, n)
}
func (h *itemHeap[T]) Pop() any {
	old := *h
	n := old[len(old)-1]
	n.holderIndex = -1
	*h = old[:len(old)-1]
	return n
}

type holderQueue[T any] struct {
	holder string
	items  itemHeap[T]
	index  int
}

type holderHeap[T any] []*holderQueue[T]

func (h holderHeap[T]) Len() int { return len(h) }
func (h holderHeap[T]) Less(i, j int) bool {
	left, right := h[i].items[0].item, h[j].items[0].item
	if left.Due.Equal(right.Due) {
		return h[i].holder < h[j].holder
	}
	return left.Due.Before(right.Due)
}
func (h holderHeap[T]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *holderHeap[T]) Push(value any) {
	queue := value.(*holderQueue[T])
	queue.index = len(*h)
	*h = append(*h, queue)
}
func (h *holderHeap[T]) Pop() any {
	old := *h
	queue := old[len(old)-1]
	queue.index = -1
	*h = old[:len(old)-1]
	return queue
}

// Batch is the result of a single bounded planning operation. Oversize means
// exactly one item exceeded MaxBytes and was deliberately emitted alone.
type Batch[T any] struct {
	Holder         string
	Items          []Item[T]
	EstimatedBytes int
	Oversize       bool
	MaxDueLag      time.Duration
}

// Stats contains bounded scheduler state useful to tests and low-cardinality
// observations. It never exposes item identities.
type Stats struct {
	Backlog       int
	ClockRollback uint64
	Stopped       bool
}

// Planner owns a global Holder heap plus one item heap per Holder, so hot
// renewal ticks never scan Gate's connection map or unrelated Holder items.
// Upsert/Remove are O(log N); each Plan examines at most MaxPlanScan items.
type Planner[T any] struct {
	mu       sync.Mutex
	cfg      Config
	items    map[string]*node[T]
	holders  map[string]*holderQueue[T]
	heap     holderHeap[T]
	seq      uint64
	lastNow  time.Time
	rollback uint64
	stopped  bool
}

func NewPlanner[T any](cfg Config) (*Planner[T], error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.MaxPlanScan == 0 {
		cfg.MaxPlanScan = cfg.MaxItems*4 + 64
	}
	p := &Planner[T]{cfg: cfg, items: make(map[string]*node[T]), holders: make(map[string]*holderQueue[T])}
	heap.Init(&p.heap)
	return p, nil
}

func (p *Planner[T]) Config() Config { return p.cfg }

func (p *Planner[T]) Upsert(item Item[T]) error {
	if item.Key == "" || item.Identity == "" || item.Holder == "" {
		return errors.New("leasebatch: key, identity, and holder are required")
	}
	if item.Due.IsZero() || item.Deadline.IsZero() {
		return errors.New("leasebatch: due and deadline are required")
	}
	if item.EstimatedBytes <= 0 {
		return errors.New("leasebatch: estimated bytes must be positive")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return errors.New("leasebatch: planner stopped")
	}
	if existing := p.items[item.Key]; existing != nil {
		oldHolder := existing.item.Holder
		oldQueue := p.holders[oldHolder]
		if oldHolder != item.Holder {
			p.removeNodeLocked(existing)
			delete(p.items, item.Key)
			p.addItemLocked(item)
			return nil
		}
		existing.item = item
		p.seq++
		existing.seq = p.seq
		heap.Fix(&oldQueue.items, existing.holderIndex)
		heap.Fix(&p.heap, oldQueue.index)
		return nil
	}
	p.addItemLocked(item)
	return nil
}

func (p *Planner[T]) addItemLocked(item Item[T]) {
	p.seq++
	n := &node[T]{item: item, seq: p.seq}
	p.items[item.Key] = n
	queue := p.holders[item.Holder]
	if queue == nil {
		queue = &holderQueue[T]{holder: item.Holder}
		p.holders[item.Holder] = queue
		heap.Push(&queue.items, n)
		heap.Push(&p.heap, queue)
		return
	}
	heap.Push(&queue.items, n)
	heap.Fix(&p.heap, queue.index)
}

func (p *Planner[T]) Remove(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := p.items[key]
	if n == nil {
		return false
	}
	delete(p.items, key)
	p.removeNodeLocked(n)
	return true
}

func (p *Planner[T]) removeNodeLocked(n *node[T]) {
	queue := p.holders[n.item.Holder]
	heap.Remove(&queue.items, n.holderIndex)
	if len(queue.items) == 0 {
		delete(p.holders, queue.holder)
		heap.Remove(&p.heap, queue.index)
		return
	}
	heap.Fix(&p.heap, queue.index)
}

// Plan returns at most one holder-isolated batch. force is reserved for
// explicit/manual maintenance calls; lifecycle loops always use force=false.
func (p *Planner[T]) Plan(now time.Time, force bool) Batch[T] {
	p.mu.Lock()
	defer p.mu.Unlock()
	var batch Batch[T]
	if p.stopped || len(p.heap) == 0 {
		return batch
	}
	if !p.lastNow.IsZero() && now.Before(p.lastNow) {
		now = p.lastNow
		p.rollback++
	} else {
		p.lastNow = now
	}
	if !force && p.heap[0].items[0].item.Due.After(now) {
		return batch
	}
	queue := heap.Pop(&p.heap).(*holderQueue[T])
	firstNode := heap.Pop(&queue.items).(*node[T])
	first := firstNode.item
	delete(p.items, first.Key)
	batch.Holder = first.Holder
	batch.Items = append(batch.Items, first)
	batch.EstimatedBytes = first.EstimatedBytes
	batch.Oversize = first.EstimatedBytes > p.cfg.MaxBytes
	batch.MaxDueLag = dueLag(now, first.Due)
	if batch.Oversize || p.cfg.MaxItems == 1 {
		p.finishQueueLocked(queue)
		return batch
	}
	skipped := make([]*node[T], 0)
	scanned := 1
	for len(queue.items) > 0 && len(batch.Items) < p.cfg.MaxItems && scanned < p.cfg.MaxPlanScan {
		if !force && queue.items[0].item.Due.After(now) {
			break
		}
		candidateNode := heap.Pop(&queue.items).(*node[T])
		candidate := candidateNode.item
		scanned++
		if candidate.EstimatedBytes > p.cfg.MaxBytes || candidate.EstimatedBytes > p.cfg.MaxBytes-batch.EstimatedBytes {
			skipped = append(skipped, candidateNode)
			continue
		}
		delete(p.items, candidate.Key)
		batch.Items = append(batch.Items, candidate)
		batch.EstimatedBytes += candidate.EstimatedBytes
		if lag := dueLag(now, candidate.Due); lag > batch.MaxDueLag {
			batch.MaxDueLag = lag
		}
	}
	for _, item := range skipped {
		heap.Push(&queue.items, item)
	}
	p.finishQueueLocked(queue)
	return batch
}

func (p *Planner[T]) finishQueueLocked(queue *holderQueue[T]) {
	if len(queue.items) == 0 {
		delete(p.holders, queue.holder)
		return
	}
	heap.Push(&p.heap, queue)
}

func dueLag(now, due time.Time) time.Duration {
	if due.IsZero() || now.Before(due) {
		return 0
	}
	return now.Sub(due)
}

func (p *Planner[T]) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{Backlog: len(p.items), ClockRollback: p.rollback, Stopped: p.stopped}
}

func (p *Planner[T]) NextDue() (time.Time, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped || len(p.heap) == 0 {
		return time.Time{}, false
	}
	return p.heap[0].items[0].item.Due, true
}

// Stop atomically rejects future registration and clears all pending work.
func (p *Planner[T]) Stop() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return 0
	}
	cleared := len(p.items)
	p.stopped = true
	p.items = make(map[string]*node[T])
	p.holders = make(map[string]*holderQueue[T])
	p.heap = nil
	return cleared
}
