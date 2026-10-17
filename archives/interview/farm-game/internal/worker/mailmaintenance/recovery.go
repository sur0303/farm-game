// Package mailmaintenance owns the bounded PlayerMail expiry and receipt
// maintenance loop. Each process has a static modulo bucket assignment and
// advances a bounded rotating cursor; it never performs a global Redis scan.
package mailmaintenance

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"farmgame/internal/player/mail"
	redisplayermail "farmgame/internal/storage/redis/playermail"
)

const (
	DefaultBatchLimit       = 64
	MaxBatchLimit           = 512
	DefaultMaxConcurrent    = 4
	MaxMaxConcurrent        = 64
	DefaultBucketsPerRound  = 64
	MaxBucketsPerRound      = 256
	DefaultLeaseTTL         = 10 * time.Second
	DefaultSafetyMargin     = 2 * time.Second
	DefaultOperationTimeout = 3 * time.Second
	DefaultRoundTimeout     = 2 * time.Second
	MaxRoundTimeout         = 30 * time.Second
	MaxBucketCount          = 65536
)

var ErrInvalidConfig = errors.New("mail maintenance: invalid configuration")

// Repository is the current PlayerMail maintenance surface. The due-index
// methods are the domain interface; receipt discovery/GC currently live on
// redisplayermail.Store with these concrete value types, so the worker adapts
// that existing signature without widening the domain package.
type Repository interface {
	mail.MailMaintenanceRepository
	ListReceiptDue(context.Context, uint32, int64, int) ([]redisplayermail.ReceiptDueCandidate, error)
	GcReceipt(context.Context, redisplayermail.ReceiptDueRef) (redisplayermail.ReceiptGcResult, error)
}

type Config struct {
	Repository    Repository
	OwnerToken    string
	BatchLimit    int
	MaxConcurrent int
	// BucketCount is the physical PlayerMail bucket count. WorkerIndex and
	// WorkerCount define static modulo ownership: bucket%WorkerCount ==
	// WorkerIndex. They must be identical across all maintenance instances.
	BucketCount     uint32
	WorkerIndex     uint32
	WorkerCount     uint32
	BucketsPerRound int

	LeaseTTL         time.Duration
	SafetyMargin     time.Duration
	OperationTimeout time.Duration
	RoundTimeout     time.Duration
	Now              func() time.Time
}

// Ownership is the static bucket assignment for one process.
type Ownership struct {
	TotalBuckets uint32
	WorkerIndex  uint32
	WorkerCount  uint32
	OwnedBuckets uint32
}

// Owns reports whether this assignment owns bucket.
func (o Ownership) Owns(bucket uint32) bool {
	return o.TotalBuckets > 0 && o.WorkerCount > 0 && o.WorkerIndex < o.WorkerCount &&
		bucket < o.TotalBuckets && bucket%o.WorkerCount == o.WorkerIndex
}

func (o Ownership) bucketAt(ordinal uint32) uint32 {
	return o.WorkerIndex + ordinal*o.WorkerCount
}

type normalizedConfig struct {
	repository       Repository
	ownership        Ownership
	ownerToken       string
	batchLimit       int
	maxConcurrent    int
	bucketsPerRound  int
	leaseTTL         time.Duration
	safetyMargin     time.Duration
	operationTimeout time.Duration
	roundTimeout     time.Duration
	now              func() time.Time
}

// Worker is a restart-safe, statically sharded maintenance worker. The scan
// lease only coordinates duplicate discovery; it is advisory and is not a
// mutation epoch fence. Expiry/GC safety comes from each repository mutation's
// version/hash checks, idempotency and atomic compare-and-set behavior.
type Worker struct {
	config normalizedConfig

	mu               sync.Mutex
	cursor           uint32
	observations     []DueObservation
	availableBuckets uint32
	dueDepth         int64
	dueHeap          dueObservationHeap
	dueEntries       []*dueHeapEntry
}

type dueHeapEntry struct {
	ordinal uint32
	score   int64
	index   int
}

type dueObservationHeap []*dueHeapEntry

func (h dueObservationHeap) Len() int           { return len(h) }
func (h dueObservationHeap) Less(i, j int) bool { return h[i].score < h[j].score }
func (h dueObservationHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *dueObservationHeap) Push(value any) {
	entry := value.(*dueHeapEntry)
	entry.index = len(*h)
	*h = append(*h, entry)
}
func (h *dueObservationHeap) Pop() any {
	old := *h
	entry := old[len(old)-1]
	entry.index = -1
	*h = old[:len(old)-1]
	return entry
}

// DueObservation is the cached proof for all buckets owned by this instance.
// Available remains false until every owned bucket has a successful bounded
// observation, and becomes false again when a selected bucket cannot be
// observed completely.
type DueObservation struct {
	Depth         int64
	OldestDueAtMs int64
	Available     bool
}

type RoundStats struct {
	OwnedBuckets          uint64
	Buckets               uint64
	AdvisoryLeasedBuckets uint64
	HeldByOther           uint64
	LeaseUnavailable      uint64
	LeaseTimeoutUnknown   uint64

	ExpireCandidates  uint64
	GcCandidates      uint64
	ReceiptCandidates uint64
	Expired           uint64
	ExpireNoChange    uint64
	GcPurged          uint64
	GcNoChange        uint64
	ReceiptPurged     uint64
	ReceiptNoChange   uint64
	NotDue            uint64
	Stale             uint64
	Retried           uint64
	Unknown           uint64
	Failed            uint64
	SweepCompleted    bool
	DeadlineExceeded  bool

	Due DueObservation
}

func (s RoundStats) Candidates() uint64 {
	return s.ExpireCandidates + s.GcCandidates + s.ReceiptCandidates
}

func (s RoundStats) Completed() uint64 {
	return s.Expired + s.ExpireNoChange + s.GcPurged + s.GcNoChange + s.ReceiptPurged + s.ReceiptNoChange
}

type bucketRound struct {
	ordinal uint32
	stats   RoundStats
	err     error
}

type bucketJob struct {
	ordinal uint32
	bucket  uint32
}

type itemResult struct {
	expired, expireNoChange        uint64
	gcPurged, gcNoChange           uint64
	receiptPurged, receiptNoChange uint64
	notDue, stale, retried         uint64
	unknown, failed                uint64
	err                            error
}

func NewWorker(config Config) (*Worker, error) {
	if config.Repository == nil || strings.TrimSpace(config.OwnerToken) == "" || len(config.OwnerToken) > 128 {
		return nil, fmt.Errorf("%w: repository and bounded owner token are required", ErrInvalidConfig)
	}
	if config.BucketCount == 0 || config.BucketCount > MaxBucketCount {
		return nil, fmt.Errorf("%w: bucket count must be in [1,%d]", ErrInvalidConfig, MaxBucketCount)
	}
	if config.WorkerCount == 0 || config.WorkerIndex >= config.WorkerCount {
		return nil, fmt.Errorf("%w: worker index/count must identify one static shard", ErrInvalidConfig)
	}
	ownedBuckets := uint32(0)
	if config.WorkerIndex < config.BucketCount {
		ownedBuckets = (config.BucketCount-1-config.WorkerIndex)/config.WorkerCount + 1
	}
	if ownedBuckets == 0 {
		return nil, fmt.Errorf("%w: worker shard owns no buckets", ErrInvalidConfig)
	}
	if config.BatchLimit <= 0 {
		config.BatchLimit = DefaultBatchLimit
	}
	if config.BatchLimit > MaxBatchLimit {
		return nil, fmt.Errorf("%w: batch limit exceeds %d", ErrInvalidConfig, MaxBatchLimit)
	}
	if config.MaxConcurrent <= 0 {
		config.MaxConcurrent = DefaultMaxConcurrent
	}
	if config.MaxConcurrent > MaxMaxConcurrent {
		return nil, fmt.Errorf("%w: max concurrency exceeds %d", ErrInvalidConfig, MaxMaxConcurrent)
	}
	if config.BucketsPerRound <= 0 {
		config.BucketsPerRound = DefaultBucketsPerRound
	}
	if config.BucketsPerRound > MaxBucketsPerRound {
		return nil, fmt.Errorf("%w: buckets per round exceeds %d", ErrInvalidConfig, MaxBucketsPerRound)
	}
	if config.LeaseTTL <= 0 {
		config.LeaseTTL = DefaultLeaseTTL
	}
	if config.SafetyMargin <= 0 || config.SafetyMargin >= config.LeaseTTL {
		config.SafetyMargin = DefaultSafetyMargin
		if config.SafetyMargin >= config.LeaseTTL {
			config.SafetyMargin = config.LeaseTTL / 5
		}
		if config.SafetyMargin <= 0 {
			config.SafetyMargin = time.Millisecond
		}
	}
	if config.OperationTimeout <= 0 {
		config.OperationTimeout = DefaultOperationTimeout
	}
	if config.RoundTimeout <= 0 {
		config.RoundTimeout = DefaultRoundTimeout
	}
	if config.RoundTimeout > MaxRoundTimeout {
		return nil, fmt.Errorf("%w: round timeout exceeds %s", ErrInvalidConfig, MaxRoundTimeout)
	}
	if config.Now == nil {
		config.Now = time.Now
	}

	ownership := Ownership{
		TotalBuckets: config.BucketCount, WorkerIndex: config.WorkerIndex,
		WorkerCount: config.WorkerCount, OwnedBuckets: ownedBuckets,
	}
	return &Worker{config: normalizedConfig{
		repository: config.Repository, ownership: ownership, ownerToken: config.OwnerToken,
		batchLimit: config.BatchLimit, maxConcurrent: config.MaxConcurrent,
		bucketsPerRound: config.BucketsPerRound,
		leaseTTL:        config.LeaseTTL, safetyMargin: config.SafetyMargin,
		operationTimeout: config.OperationTimeout, roundTimeout: config.RoundTimeout, now: config.Now,
	}, observations: make([]DueObservation, ownedBuckets), dueEntries: make([]*dueHeapEntry, ownedBuckets)}, nil
}

// Ownership returns the immutable static assignment for this worker.
func (w *Worker) Ownership() Ownership {
	if w == nil {
		return Ownership{}
	}
	return w.config.ownership
}

// RunOnce advances the rotating cursor by at most BucketsPerRound. A fixed
// worker pool bounds bucket goroutines, all item calls share a global
// concurrency bound, and RoundTimeout caps the entire pass.
func (w *Worker) RunOnce(ctx context.Context) (RoundStats, error) {
	if w == nil || w.config.repository == nil || w.config.ownership.OwnedBuckets == 0 {
		return RoundStats{}, fmt.Errorf("%w: incomplete worker", ErrInvalidConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return RoundStats{OwnedBuckets: uint64(w.config.ownership.OwnedBuckets), Due: w.ownedDue()}, err
	}
	roundCtx, roundCancel := context.WithTimeout(ctx, w.config.roundTimeout)
	defer roundCancel()
	jobs, sweepCompleted := w.nextJobs()
	stats := RoundStats{OwnedBuckets: uint64(w.config.ownership.OwnedBuckets), SweepCompleted: sweepCompleted}
	results := make(chan bucketRound, len(jobs))
	jobQueue := make(chan bucketJob, len(jobs))
	for _, job := range jobs {
		jobQueue <- job
	}
	close(jobQueue)

	workerCount := w.config.maxConcurrent
	if workerCount > len(jobs) {
		workerCount = len(jobs)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < workerCount; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobQueue {
				if err := roundCtx.Err(); err != nil {
					results <- bucketRound{ordinal: job.ordinal, stats: RoundStats{Buckets: 1}, err: err}
					continue
				}
				result := w.runBucket(roundCtx, job.bucket)
				result.ordinal = job.ordinal
				results <- result
			}
		}()
	}
	wg.Wait()
	close(results)

	var errs []error
	for result := range results {
		stats.add(result.stats)
		w.recordObservation(result.ordinal, result.stats.Due)
		if result.err != nil {
			errs = append(errs, result.err)
		}
	}
	stats.Due = w.ownedDue()
	if roundCtx.Err() != nil {
		stats.DeadlineExceeded = errors.Is(roundCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
		errs = append(errs, roundCtx.Err())
	}
	return stats, errors.Join(errs...)
}

func (w *Worker) nextJobs() ([]bucketJob, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	count := int(w.config.ownership.OwnedBuckets)
	selected := w.config.bucketsPerRound
	if selected > count {
		selected = count
	}
	jobs := make([]bucketJob, 0, selected)
	start := w.cursor
	for offset := 0; offset < selected; offset++ {
		ordinal := (start + uint32(offset)) % w.config.ownership.OwnedBuckets
		w.invalidateObservationLocked(ordinal)
		jobs = append(jobs, bucketJob{ordinal: ordinal, bucket: w.config.ownership.bucketAt(ordinal)})
	}
	w.cursor = (start + uint32(selected)) % w.config.ownership.OwnedBuckets
	return jobs, start+uint32(selected) >= w.config.ownership.OwnedBuckets
}

func (w *Worker) recordObservation(ordinal uint32, observation DueObservation) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if ordinal >= uint32(len(w.observations)) {
		return
	}
	w.invalidateObservationLocked(ordinal)
	w.observations[ordinal] = observation
	if observation.Available {
		w.availableBuckets++
		w.dueDepth += observation.Depth
		if observation.OldestDueAtMs > 0 {
			entry := &dueHeapEntry{ordinal: ordinal, score: observation.OldestDueAtMs}
			w.dueEntries[ordinal] = entry
			heap.Push(&w.dueHeap, entry)
		}
	}
}

func (w *Worker) invalidateObservationLocked(ordinal uint32) {
	if ordinal >= uint32(len(w.observations)) {
		return
	}
	previous := w.observations[ordinal]
	if previous.Available {
		w.availableBuckets--
		w.dueDepth -= previous.Depth
	}
	if entry := w.dueEntries[ordinal]; entry != nil {
		heap.Remove(&w.dueHeap, entry.index)
		w.dueEntries[ordinal] = nil
	}
	w.observations[ordinal] = DueObservation{}
}

func (w *Worker) ownedDue() DueObservation {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := DueObservation{
		Available: w.availableBuckets == w.config.ownership.OwnedBuckets,
		Depth:     w.dueDepth,
	}
	if len(w.dueHeap) != 0 {
		result.OldestDueAtMs = w.dueHeap[0].score
	}
	return result
}

func (s *RoundStats) add(other RoundStats) {
	s.Buckets += other.Buckets
	s.AdvisoryLeasedBuckets += other.AdvisoryLeasedBuckets
	s.HeldByOther += other.HeldByOther
	s.LeaseUnavailable += other.LeaseUnavailable
	s.LeaseTimeoutUnknown += other.LeaseTimeoutUnknown
	s.ExpireCandidates += other.ExpireCandidates
	s.GcCandidates += other.GcCandidates
	s.ReceiptCandidates += other.ReceiptCandidates
	s.Expired += other.Expired
	s.ExpireNoChange += other.ExpireNoChange
	s.GcPurged += other.GcPurged
	s.GcNoChange += other.GcNoChange
	s.ReceiptPurged += other.ReceiptPurged
	s.ReceiptNoChange += other.ReceiptNoChange
	s.NotDue += other.NotDue
	s.Stale += other.Stale
	s.Retried += other.Retried
	s.Unknown += other.Unknown
	s.Failed += other.Failed
}

func mergeDue(left, right DueObservation) DueObservation {
	merged := DueObservation{Available: left.Available && right.Available, Depth: left.Depth + right.Depth}
	merged.OldestDueAtMs = left.OldestDueAtMs
	if merged.OldestDueAtMs == 0 || (right.OldestDueAtMs > 0 && right.OldestDueAtMs < merged.OldestDueAtMs) {
		merged.OldestDueAtMs = right.OldestDueAtMs
	}
	return merged
}

func (w *Worker) runBucket(ctx context.Context, bucket uint32) bucketRound {
	result := bucketRound{stats: RoundStats{Buckets: 1, Due: DueObservation{Available: true}}}
	if ctx.Err() != nil {
		result.stats.Due.Available = false
		result.err = ctx.Err()
		return result
	}

	leaseStarted := time.Now()
	leaseCtx, leaseCancel := operationContext(ctx, w.config.operationTimeout, time.Time{})
	lease, err := w.config.repository.ClaimScanLease(leaseCtx, bucket, w.config.ownerToken, w.config.leaseTTL.Milliseconds())
	leaseContextErr := leaseCtx.Err()
	leaseCancel()
	if leaseContextErr != nil {
		result.stats.LeaseTimeoutUnknown++
		result.stats.Unknown++
		result.stats.Due.Available = false
		result.err = fmt.Errorf("bucket %d claim scan lease context: %w", bucket, leaseContextErr)
		return result
	}
	if err != nil {
		result.stats.LeaseUnavailable++
		result.stats.Due.Available = false
		result.err = fmt.Errorf("bucket %d claim scan lease: %w", bucket, err)
		return result
	}
	switch lease.Status {
	case mail.ScanLeaseHeldByOther:
		result.stats.HeldByOther++
		result.stats.Due.Available = false
		return result
	case mail.ScanLeaseUnavailableNotSent:
		result.stats.LeaseUnavailable++
		result.stats.Due.Available = false
		result.err = fmt.Errorf("bucket %d scan lease unavailable", bucket)
		return result
	case mail.ScanLeaseTimeoutUnknown:
		result.stats.LeaseTimeoutUnknown++
		result.stats.Unknown++
		result.stats.Due.Available = false
		result.err = fmt.Errorf("bucket %d scan lease timeout unknown", bucket)
		return result
	case mail.ScanLeaseAcquired:
		result.stats.AdvisoryLeasedBuckets++
	default:
		result.stats.Failed++
		result.stats.Due.Available = false
		result.err = fmt.Errorf("bucket %d unsupported scan lease status %q", bucket, lease.Status)
		return result
	}
	if lease.AcceptedTTLMs <= 0 {
		result.stats.Failed++
		result.stats.Due.Available = false
		result.err = fmt.Errorf("bucket %d scan lease returned invalid ttl %d", bucket, lease.AcceptedTTLMs)
		return result
	}
	safeDeadline := leaseStarted.Add(time.Duration(lease.AcceptedTTLMs)*time.Millisecond - w.config.safetyMargin)
	if !safeDeadline.After(leaseStarted) {
		result.stats.Failed++
		result.stats.Due.Available = false
		result.err = fmt.Errorf("bucket %d scan lease leaves no safety window", bucket)
		return result
	}

	// The lease is advisory duplicate-scan coordination only. LeaseEpoch is
	// deliberately not presented as write authority: every mutation below is
	// independently version/hash checked and idempotent in its Redis script.
	var errs []error
	for _, run := range []func(context.Context, uint32, time.Time, *RoundStats) error{
		w.runExpire, w.runGC, w.runReceiptGC,
	} {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if !time.Now().Before(safeDeadline) {
			errs = append(errs, context.DeadlineExceeded)
			break
		}
		if err := run(ctx, bucket, safeDeadline, &result.stats); err != nil {
			errs = append(errs, err)
		}
	}
	result.err = errors.Join(errs...)
	if result.err != nil {
		result.stats.Due.Available = false
	}
	return result
}

func (w *Worker) runExpire(ctx context.Context, bucket uint32, safeDeadline time.Time, stats *RoundStats) error {
	listCtx, cancel := operationContext(ctx, w.config.operationTimeout, safeDeadline)
	candidates, err := w.config.repository.ListExpireDue(listCtx, bucket, w.config.now().UnixMilli(), w.config.batchLimit)
	listContextErr := listCtx.Err()
	cancel()
	if listContextErr != nil {
		stats.Failed++
		stats.Due.Available = false
		return fmt.Errorf("bucket %d list expire due context: %w", bucket, listContextErr)
	}
	if err != nil {
		stats.Failed++
		stats.Due.Available = false
		return fmt.Errorf("bucket %d list expire due: %w", bucket, err)
	}
	stats.ExpireCandidates += uint64(len(candidates))
	observeDue(&stats.Due, candidates, w.config.batchLimit)
	return w.processExpire(ctx, candidates, safeDeadline, stats)
}

func (w *Worker) runGC(ctx context.Context, bucket uint32, safeDeadline time.Time, stats *RoundStats) error {
	listCtx, cancel := operationContext(ctx, w.config.operationTimeout, safeDeadline)
	candidates, err := w.config.repository.ListGcDue(listCtx, bucket, w.config.now().UnixMilli(), w.config.batchLimit)
	listContextErr := listCtx.Err()
	cancel()
	if listContextErr != nil {
		stats.Failed++
		stats.Due.Available = false
		return fmt.Errorf("bucket %d list gc due context: %w", bucket, listContextErr)
	}
	if err != nil {
		stats.Failed++
		stats.Due.Available = false
		return fmt.Errorf("bucket %d list gc due: %w", bucket, err)
	}
	stats.GcCandidates += uint64(len(candidates))
	observeDue(&stats.Due, candidates, w.config.batchLimit)
	return w.processGC(ctx, candidates, safeDeadline, stats)
}

func (w *Worker) runReceiptGC(ctx context.Context, bucket uint32, safeDeadline time.Time, stats *RoundStats) error {
	listCtx, cancel := operationContext(ctx, w.config.operationTimeout, safeDeadline)
	candidates, err := w.config.repository.ListReceiptDue(listCtx, bucket, w.config.now().UnixMilli(), w.config.batchLimit)
	listContextErr := listCtx.Err()
	cancel()
	if listContextErr != nil {
		stats.Failed++
		stats.Due.Available = false
		return fmt.Errorf("bucket %d list receipt due context: %w", bucket, listContextErr)
	}
	if err != nil {
		stats.Failed++
		stats.Due.Available = false
		return fmt.Errorf("bucket %d list receipt due: %w", bucket, err)
	}
	stats.ReceiptCandidates += uint64(len(candidates))
	observeReceiptDue(&stats.Due, candidates, w.config.batchLimit)
	return w.processReceiptGC(ctx, candidates, safeDeadline, stats)
}

func observeDue(observation *DueObservation, candidates []mail.DueCandidate, limit int) {
	if len(candidates) >= limit {
		observation.Available = false
	}
	for _, candidate := range candidates {
		observation.Depth++
		if observation.OldestDueAtMs == 0 || candidate.DueAtMs < observation.OldestDueAtMs {
			observation.OldestDueAtMs = candidate.DueAtMs
		}
	}
}

func observeReceiptDue(observation *DueObservation, candidates []redisplayermail.ReceiptDueCandidate, limit int) {
	if len(candidates) >= limit {
		observation.Available = false
	}
	for _, candidate := range candidates {
		observation.Depth++
		if observation.OldestDueAtMs == 0 || candidate.DueAtMs < observation.OldestDueAtMs {
			observation.OldestDueAtMs = candidate.DueAtMs
		}
	}
}

func (w *Worker) processExpire(ctx context.Context, candidates []mail.DueCandidate, safeDeadline time.Time, stats *RoundStats) error {
	return w.processItems(ctx, len(candidates), safeDeadline, func(index int) itemResult {
		return w.expireOne(ctx, candidates[index], safeDeadline)
	}, func(result itemResult) {
		stats.Expired += result.expired
		stats.ExpireNoChange += result.expireNoChange
		stats.NotDue += result.notDue
		stats.Stale += result.stale
		stats.Retried += result.retried
		stats.Unknown += result.unknown
		stats.Failed += result.failed
	})
}

func (w *Worker) processGC(ctx context.Context, candidates []mail.DueCandidate, safeDeadline time.Time, stats *RoundStats) error {
	return w.processItems(ctx, len(candidates), safeDeadline, func(index int) itemResult {
		return w.gcOne(ctx, candidates[index], safeDeadline)
	}, func(result itemResult) {
		stats.GcPurged += result.gcPurged
		stats.GcNoChange += result.gcNoChange
		stats.NotDue += result.notDue
		stats.Stale += result.stale
		stats.Retried += result.retried
		stats.Unknown += result.unknown
		stats.Failed += result.failed
	})
}

func (w *Worker) processReceiptGC(ctx context.Context, candidates []redisplayermail.ReceiptDueCandidate, safeDeadline time.Time, stats *RoundStats) error {
	return w.processItems(ctx, len(candidates), safeDeadline, func(index int) itemResult {
		return w.receiptGCOne(ctx, candidates[index], safeDeadline)
	}, func(result itemResult) {
		stats.ReceiptPurged += result.receiptPurged
		stats.ReceiptNoChange += result.receiptNoChange
		stats.NotDue += result.notDue
		stats.Retried += result.retried
		stats.Unknown += result.unknown
		stats.Failed += result.failed
	})
}

func (w *Worker) processItems(ctx context.Context, count int, deadline time.Time, run func(int) itemResult, add func(itemResult)) error {
	var errs []error
	for index := 0; index < count; index++ {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			errs = append(errs, context.DeadlineExceeded)
			break
		}
		result := run(index)
		add(result)
		if result.err != nil {
			errs = append(errs, result.err)
		}
	}
	return errors.Join(errs...)
}

func (w *Worker) expireOne(ctx context.Context, candidate mail.DueCandidate, safeDeadline time.Time) itemResult {
	opCtx, cancel := operationContext(ctx, w.config.operationTimeout, safeDeadline)
	result, err := w.config.repository.ExpireVisibility(opCtx, candidate.Ref)
	contextErr := opCtx.Err()
	cancel()
	if contextErr != nil {
		return itemResult{unknown: 1, retried: 1, err: fmt.Errorf("expire %s context: %w", candidate.Ref.MailID, contextErr)}
	}
	if err != nil {
		return itemResult{failed: 1, err: fmt.Errorf("expire %s: %w", candidate.Ref.MailID, err)}
	}
	switch result.Status {
	case mail.ExpireApplied:
		return itemResult{expired: 1}
	case mail.ExpireNoChange, mail.ExpireTargetAlreadyPurged:
		return itemResult{expireNoChange: 1}
	case mail.ExpireNotDue:
		// The mutation script atomically advances the existing member to the
		// Redis-time due score, so clock skew cannot create a hot loop.
		return itemResult{expireNoChange: 1, notDue: 1, retried: 1}
	case mail.ExpireStaleRef:
		// ExpireVisibility atomically replaces the stale member when its
		// pre-read versions still match, or retains the old member on a race.
		return itemResult{stale: 1, retried: 1}
	case mail.ExpireUnavailableNotSent:
		return itemResult{failed: 1, retried: 1, err: fmt.Errorf("expire %s unavailable not sent", candidate.Ref.MailID)}
	case mail.ExpireTimeoutUnknown:
		return itemResult{unknown: 1, retried: 1, err: fmt.Errorf("expire %s timeout unknown", candidate.Ref.MailID)}
	default:
		return itemResult{failed: 1, err: fmt.Errorf("expire %s unsupported status %q", candidate.Ref.MailID, result.Status)}
	}
}

func (w *Worker) gcOne(ctx context.Context, candidate mail.DueCandidate, safeDeadline time.Time) itemResult {
	opCtx, cancel := operationContext(ctx, w.config.operationTimeout, safeDeadline)
	result, err := w.config.repository.GcMail(opCtx, candidate.Ref)
	contextErr := opCtx.Err()
	cancel()
	if contextErr != nil {
		return itemResult{unknown: 1, retried: 1, err: fmt.Errorf("gc %s context: %w", candidate.Ref.MailID, contextErr)}
	}
	if err != nil {
		return itemResult{failed: 1, err: fmt.Errorf("gc %s: %w", candidate.Ref.MailID, err)}
	}
	switch result.Status {
	case mail.GcPurged, mail.GcTargetAlreadyPurged:
		return itemResult{gcPurged: 1}
	case mail.GcNotDue:
		return itemResult{gcNoChange: 1, notDue: 1, retried: 1}
	case mail.GcStaleRef:
		return itemResult{stale: 1, retried: 1}
	case mail.GcReconcileRequired:
		return itemResult{failed: 1, err: fmt.Errorf("gc %s reconcile required", candidate.Ref.MailID)}
	case mail.GcUnavailableNotSent:
		return itemResult{failed: 1, retried: 1, err: fmt.Errorf("gc %s unavailable not sent", candidate.Ref.MailID)}
	case mail.GcTimeoutUnknown:
		return itemResult{unknown: 1, retried: 1, err: fmt.Errorf("gc %s timeout unknown", candidate.Ref.MailID)}
	default:
		return itemResult{failed: 1, err: fmt.Errorf("gc %s unsupported status %q", candidate.Ref.MailID, result.Status)}
	}
}

func (w *Worker) receiptGCOne(ctx context.Context, candidate redisplayermail.ReceiptDueCandidate, safeDeadline time.Time) itemResult {
	opCtx, cancel := operationContext(ctx, w.config.operationTimeout, safeDeadline)
	result, err := w.config.repository.GcReceipt(opCtx, candidate.Ref)
	contextErr := opCtx.Err()
	cancel()
	if contextErr != nil {
		return itemResult{unknown: 1, retried: 1, err: fmt.Errorf("receipt gc %s context: %w", candidate.Ref.MailID, contextErr)}
	}
	if err != nil {
		return itemResult{failed: 1, err: fmt.Errorf("receipt gc %s: %w", candidate.Ref.MailID, err)}
	}
	switch result.Status {
	case redisplayermail.ReceiptGcPurged:
		return itemResult{receiptPurged: 1}
	case redisplayermail.ReceiptGcTargetAlreadyPurged:
		return itemResult{receiptNoChange: 1}
	case redisplayermail.ReceiptGcNotDue:
		// GcReceipt atomically advances the member to RetainUntil using Redis
		// TIME, preventing repeated discovery at the stale score.
		return itemResult{receiptNoChange: 1, notDue: 1, retried: 1}
	case redisplayermail.ReceiptGcUnavailableNotSent:
		return itemResult{failed: 1, retried: 1, err: fmt.Errorf("receipt gc %s unavailable not sent", candidate.Ref.MailID)}
	case redisplayermail.ReceiptGcTimeoutUnknown:
		return itemResult{unknown: 1, retried: 1, err: fmt.Errorf("receipt gc %s timeout unknown", candidate.Ref.MailID)}
	default:
		return itemResult{failed: 1, err: fmt.Errorf("receipt gc %s unsupported status %q", candidate.Ref.MailID, result.Status)}
	}
}

func operationContext(parent context.Context, timeout time.Duration, deadline time.Time) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	ctx := parent
	cancels := make([]context.CancelFunc, 0, 2)
	if !deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		cancels = append(cancels, cancel)
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		cancels = append(cancels, cancel)
	}
	return ctx, func() {
		for index := len(cancels) - 1; index >= 0; index-- {
			cancels[index]()
		}
	}
}
