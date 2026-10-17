// Package dirtywriteback provides a process-local, unique delayed flush queue.
//
// A Registry supplies a callback that freezes actor state in a mailbox turn,
// performs Redis I/O outside the mailbox, and posts a bounded completion turn.
// Repeated mutations coalesce to one actor entry. An entry remains tracked
// while its flush is in flight so a concurrent mutation cannot be lost.
package dirtywriteback

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrClosed         = errors.New("dirty writeback: scheduler closed")
	ErrInvalidActorID = errors.New("dirty writeback: actor id required")
	ErrQueueFull      = errors.New("dirty writeback: unique actor capacity reached")
	ErrFlushInFlight  = errors.New("dirty writeback: actor flush already in flight")
)

type FlushFunc func(context.Context, uint64) error

type Options struct {
	FlushInterval time.Duration
	Jitter        time.Duration
	MaxDirtyAge   time.Duration
	RetryDelay    time.Duration
	FlushTimeout  time.Duration
	Workers       int
	// QueueSize bounds unique tracked actor IDs, including in-flight entries.
	QueueSize int
	Now       func() time.Time
}

func DefaultOptions() Options {
	return Options{
		FlushInterval: 3 * time.Second,
		Jitter:        600 * time.Millisecond,
		MaxDirtyAge:   10 * time.Second,
		RetryDelay:    time.Second,
		FlushTimeout:  2 * time.Second,
		Workers:       4,
		QueueSize:     1024,
		Now:           time.Now,
	}
}

func (o Options) withDefaults() Options {
	defaults := DefaultOptions()
	if o.FlushInterval <= 0 {
		o.FlushInterval = defaults.FlushInterval
	}
	if o.Jitter < 0 {
		o.Jitter = 0
	}
	if o.MaxDirtyAge <= 0 {
		o.MaxDirtyAge = defaults.MaxDirtyAge
	}
	if o.MaxDirtyAge < o.FlushInterval {
		o.MaxDirtyAge = o.FlushInterval
	}
	if o.RetryDelay <= 0 {
		o.RetryDelay = defaults.RetryDelay
	}
	if o.FlushTimeout <= 0 {
		o.FlushTimeout = defaults.FlushTimeout
	}
	if o.Workers <= 0 {
		o.Workers = defaults.Workers
	}
	if o.QueueSize <= 0 {
		o.QueueSize = defaults.QueueSize
	}
	if o.Now == nil {
		o.Now = defaults.Now
	}
	return o
}

func (o Options) Validate() error {
	if o.FlushInterval <= 0 || o.MaxDirtyAge < o.FlushInterval || o.RetryDelay <= 0 || o.FlushTimeout <= 0 {
		return errors.New("dirty writeback: invalid timing options")
	}
	if o.Workers <= 0 || o.QueueSize <= 0 || o.Now == nil {
		return errors.New("dirty writeback: invalid worker options")
	}
	return nil
}

type MetricsSnapshot struct {
	Marks          uint64
	CoalescedMarks uint64
	FlushAttempts  uint64
	FlushSucceeded uint64
	FlushFailed    uint64
	QueueDepth     int
	Inflight       int
	OldestDirtyAge time.Duration
	MaxQueueDepth  uint64
}

type metrics struct {
	marks          atomic.Uint64
	coalescedMarks atomic.Uint64
	flushAttempts  atomic.Uint64
	flushSucceeded atomic.Uint64
	flushFailed    atomic.Uint64
	maxQueueDepth  atomic.Uint64
}

type entry struct {
	actorID    uint64
	firstDirty time.Time
	due        time.Time
	index      int
	inflight   bool
	remarked   bool
	forgotten  bool
}

type entryHeap []*entry

func (h entryHeap) Len() int { return len(h) }
func (h entryHeap) Less(i, j int) bool {
	if h[i].due.Equal(h[j].due) {
		return h[i].actorID < h[j].actorID
	}
	return h[i].due.Before(h[j].due)
}
func (h entryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *entryHeap) Push(value any) {
	e := value.(*entry)
	e.index = len(*h)
	*h = append(*h, e)
}
func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	e.index = -1
	*h = old[:n-1]
	return e
}

type Scheduler struct {
	opts  Options
	flush FlushFunc

	mu      sync.Mutex
	entries map[uint64]*entry
	queue   entryHeap
	closed  bool

	wake chan struct{}
	stop chan struct{}
	done chan struct{}
	jobs chan uint64

	workers   sync.WaitGroup
	close     sync.Once
	closeJobs sync.Once
	metrics   metrics
}

func New(options Options, flush FlushFunc) (*Scheduler, error) {
	if flush == nil {
		return nil, errors.New("dirty writeback: flush callback required")
	}
	options = options.withDefaults()
	if err := options.Validate(); err != nil {
		return nil, err
	}
	s := &Scheduler{
		opts: options, flush: flush,
		entries: make(map[uint64]*entry),
		wake:    make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
		jobs: make(chan uint64, options.QueueSize),
	}
	heap.Init(&s.queue)
	for i := 0; i < options.Workers; i++ {
		s.workers.Add(1)
		go s.worker()
	}
	go s.dispatch()
	return s, nil
}

func (s *Scheduler) Mark(actorID uint64) error {
	if actorID == 0 {
		return ErrInvalidActorID
	}
	now := s.opts.Now()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.metrics.marks.Add(1)
	if current := s.entries[actorID]; current != nil {
		current.forgotten = false
		if current.inflight {
			current.remarked = true
		}
		s.metrics.coalescedMarks.Add(1)
		s.mu.Unlock()
		return nil
	}
	if len(s.entries) >= s.opts.QueueSize {
		s.mu.Unlock()
		return ErrQueueFull
	}
	due := s.normalDue(actorID, now)
	e := &entry{actorID: actorID, firstDirty: now, due: due, index: -1}
	s.entries[actorID] = e
	heap.Push(&s.queue, e)
	s.observeQueueDepthLocked()
	s.mu.Unlock()
	s.signal()
	return nil
}

func (s *Scheduler) Prioritize(actorID uint64) error {
	if actorID == 0 {
		return ErrInvalidActorID
	}
	now := s.opts.Now()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if current := s.entries[actorID]; current != nil {
		current.forgotten = false
		if current.inflight {
			current.remarked = true
			s.mu.Unlock()
			return nil
		}
		current.due = now
		heap.Fix(&s.queue, current.index)
		s.mu.Unlock()
		s.signal()
		return nil
	}
	if len(s.entries) >= s.opts.QueueSize {
		s.mu.Unlock()
		return ErrQueueFull
	}
	e := &entry{actorID: actorID, firstDirty: now, due: now, index: -1}
	s.entries[actorID] = e
	heap.Push(&s.queue, e)
	s.observeQueueDepthLocked()
	s.mu.Unlock()
	s.signal()
	return nil
}

func (s *Scheduler) FlushNow(ctx context.Context, actorID uint64) error {
	if actorID == 0 {
		return ErrInvalidActorID
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Passivation and shard drain may race an already executing background
	// flush. They must wait for that frozen attempt to install its completion,
	// then run once more if the Actor was remarked in the meantime. This is a
	// lifecycle-only wait; ordinary Actor turns never call FlushNow.
	waitedForInflight := false
	for {
		if waitedForInflight && !s.tracked(actorID) {
			// The in-flight attempt (or its remarked follow-up) already made the
			// entry clean. Avoid manufacturing a third no-op callback.
			return nil
		}
		err := s.beginImmediate(actorID)
		if !errors.Is(err, ErrFlushInFlight) {
			if err != nil {
				return err
			}
			break
		}
		waitedForInflight = true
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		}
	}
	err := s.runFlush(ctx, actorID)
	s.finishInflight(actorID, err, true)
	return err
}

func (s *Scheduler) tracked(actorID uint64) bool {
	s.mu.Lock()
	_, ok := s.entries[actorID]
	s.mu.Unlock()
	return ok
}

// Forget removes a pending flush after the holder has lost its writer
// generation. An already in-flight writer-fenced Redis call cannot be
// cancelled safely; it is marked forgotten and discarded on completion.
func (s *Scheduler) Forget(actorID uint64) {
	if s == nil || actorID == 0 {
		return
	}
	s.mu.Lock()
	if e := s.entries[actorID]; e != nil {
		if e.inflight {
			e.forgotten = true
			e.remarked = false
		} else {
			if e.index >= 0 {
				heap.Remove(&s.queue, e.index)
			}
			delete(s.entries, actorID)
		}
	}
	s.mu.Unlock()
}

func (s *Scheduler) Snapshot() MetricsSnapshot {
	now := s.opts.Now()
	s.mu.Lock()
	depth := len(s.entries)
	inflight := 0
	var oldest time.Time
	for _, e := range s.entries {
		if e.inflight {
			inflight++
		}
		if oldest.IsZero() || e.firstDirty.Before(oldest) {
			oldest = e.firstDirty
		}
	}
	s.mu.Unlock()
	oldestAge := time.Duration(0)
	if !oldest.IsZero() && now.After(oldest) {
		oldestAge = now.Sub(oldest)
	}
	return MetricsSnapshot{
		Marks: s.metrics.marks.Load(), CoalescedMarks: s.metrics.coalescedMarks.Load(),
		FlushAttempts: s.metrics.flushAttempts.Load(), FlushSucceeded: s.metrics.flushSucceeded.Load(),
		FlushFailed: s.metrics.flushFailed.Load(), QueueDepth: depth, Inflight: inflight,
		OldestDirtyAge: oldestAge, MaxQueueDepth: s.metrics.maxQueueDepth.Load(),
	}
}

// Close stops background dispatch, waits for already accepted jobs, and makes
// one bounded final attempt for every entry that remains dirty. Failed entries
// stay observable in Snapshot instead of being silently discarded.
func (s *Scheduler) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.close.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		close(s.stop)
	})
	select {
	case <-s.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.closeJobs.Do(func() { close(s.jobs) })
	workersDone := make(chan struct{})
	go func() {
		s.workers.Wait()
		close(workersDone)
	}()
	select {
	case <-workersDone:
	case <-ctx.Done():
		return ctx.Err()
	}

	var joined error
	for _, actorID := range s.pendingIDs() {
		if err := s.beginFinal(actorID); err != nil {
			joined = errors.Join(joined, fmt.Errorf("actor %d: %w", actorID, err))
			continue
		}
		err := s.runFlush(ctx, actorID)
		s.finishInflight(actorID, err, false)
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("actor %d: %w", actorID, err))
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(joined, err)
		}
	}
	return joined
}

func (s *Scheduler) dispatch() {
	defer close(s.done)
	for {
		due, ok := s.nextDue()
		if !ok {
			select {
			case <-s.wake:
				continue
			case <-s.stop:
				return
			}
		}
		delay := due.Sub(s.opts.Now())
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-s.wake:
				stopAndDrainTimer(timer)
				continue
			case <-s.stop:
				stopAndDrainTimer(timer)
				return
			}
		}
		entries := s.popDue(s.opts.Now())
		for index, e := range entries {
			select {
			case s.jobs <- e.actorID:
			case <-s.stop:
				s.restoreUnsent(entries[index:])
				return
			}
		}
	}
}

func (s *Scheduler) worker() {
	defer s.workers.Done()
	for actorID := range s.jobs {
		err := s.runFlush(context.Background(), actorID)
		s.finishInflight(actorID, err, true)
	}
}

func (s *Scheduler) runFlush(parent context.Context, actorID uint64) error {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, s.opts.FlushTimeout)
	defer cancel()
	s.metrics.flushAttempts.Add(1)
	err := s.flush(ctx, actorID)
	if err == nil {
		s.metrics.flushSucceeded.Add(1)
		return nil
	}
	s.metrics.flushFailed.Add(1)
	return err
}

func (s *Scheduler) nextDue() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) == 0 {
		return time.Time{}, false
	}
	return s.queue[0].due, true
}

func (s *Scheduler) popDue(now time.Time) []*entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	var entries []*entry
	for len(s.queue) > 0 && !s.queue[0].due.After(now) {
		e := heap.Pop(&s.queue).(*entry)
		if s.entries[e.actorID] != e || e.inflight {
			continue
		}
		e.inflight = true
		e.remarked = false
		entries = append(entries, e)
	}
	return entries
}

func (s *Scheduler) restoreUnsent(entries []*entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		if s.entries[e.actorID] != e || !e.inflight {
			continue
		}
		e.inflight = false
		e.remarked = true
		e.index = -1
	}
}

func (s *Scheduler) finishInflight(actorID uint64, flushErr error, allowRetry bool) {
	s.mu.Lock()
	e := s.entries[actorID]
	if e == nil || !e.inflight {
		s.mu.Unlock()
		return
	}
	e.inflight = false
	if e.forgotten {
		delete(s.entries, actorID)
		s.mu.Unlock()
		return
	}
	needsAnother := flushErr != nil || e.remarked
	if !needsAnother {
		delete(s.entries, actorID)
		s.mu.Unlock()
		return
	}
	e.remarked = false
	e.index = -1
	if allowRetry && !s.closed {
		e.due = s.opts.Now().Add(s.opts.RetryDelay)
		heap.Push(&s.queue, e)
		s.mu.Unlock()
		s.signal()
		return
	}
	s.mu.Unlock()
}

func (s *Scheduler) beginImmediate(actorID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.entries[actorID]
	if e == nil {
		now := s.opts.Now()
		e = &entry{actorID: actorID, firstDirty: now, due: now, index: -1}
		s.entries[actorID] = e
		s.observeQueueDepthLocked()
	}
	if e.inflight {
		return ErrFlushInFlight
	}
	if e.index >= 0 {
		heap.Remove(&s.queue, e.index)
	}
	e.inflight = true
	e.remarked = false
	e.forgotten = false
	return nil
}

func (s *Scheduler) beginFinal(actorID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.entries[actorID]
	if e == nil {
		return nil
	}
	if e.inflight {
		return ErrFlushInFlight
	}
	if e.index >= 0 {
		heap.Remove(&s.queue, e.index)
	}
	e.inflight = true
	e.remarked = false
	return nil
}

func (s *Scheduler) pendingIDs() []uint64 {
	s.mu.Lock()
	ids := make([]uint64, 0, len(s.entries))
	for actorID := range s.entries {
		ids = append(ids, actorID)
	}
	s.mu.Unlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func (s *Scheduler) normalDue(actorID uint64, now time.Time) time.Time {
	due := now.Add(s.opts.FlushInterval + deterministicJitter(actorID, s.opts.Jitter))
	latest := now.Add(s.opts.MaxDirtyAge)
	if due.After(latest) {
		return latest
	}
	return due
}

func (s *Scheduler) observeQueueDepthLocked() {
	depth := uint64(len(s.entries))
	for {
		old := s.metrics.maxQueueDepth.Load()
		if depth <= old || s.metrics.maxQueueDepth.CompareAndSwap(old, depth) {
			return
		}
	}
}

func (s *Scheduler) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func stopAndDrainTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func deterministicJitter(actorID uint64, max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	x := actorID
	x ^= x >> 12
	x ^= x << 25
	x ^= x >> 27
	return time.Duration((x * 2685821657736338717) % uint64(max+1))
}
