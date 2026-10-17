// Package maildeliveryrecovery consumes PLAYER Authority MAIL_DELIVERY_ORDER
// records and rebuilds the independent PlayerMail entry. It never activates
// PlayerActor and never reads a mutable template or reward configuration.
package maildeliveryrecovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/identity"
	"farmgame/internal/player/mail"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"
)

const (
	FamilyMailDeliveryOrder = string(identity.WorkTypeMailDeliveryOrder)

	defaultClaimLimit       = 32
	defaultMaxConcurrent    = 4
	defaultOperationTimeout = 3 * time.Second
	defaultLeaseTTL         = 10 * time.Second
	defaultBucketsPerRound  = 16
	defaultRoundTimeBudget  = 3 * time.Second
	maxBucketsPerRound      = 4096
	defaultRetryDelay       = 250 * time.Millisecond

	mailSourceKind    = "PLAYER_ACTOR"
	mailRouteDomain   = "PLAYER"
	mailPlacementKind = "SOURCE_PERSISTENCE_BUCKET"
	mailDeliveryStep  = "RESUME_MAIL_DELIVERY_ORDER"
)

var (
	ErrInvalidConfig  = errors.New("mail delivery recovery: invalid configuration")
	ErrLeaseLost      = errors.New("mail delivery recovery: work lease lost")
	ErrPayloadInvalid = errors.New("mail delivery recovery: MailEntry payload is invalid")
	ErrRetryDeferred  = errors.New("mail delivery recovery: durable retry deferred")
)

// WorkLease is the existing PLAYER Authority work-lease surface.
type WorkLease interface {
	ClaimWorkWithBoot(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, string, int64) (authority.WorkClaim, error)
	RenewWorkLeaseWithBoot(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, uint64, string, int64) (authority.WorkLeaseRenewResult, error)
	CompleteWorkWithProof(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, uint64, uint64, string, authority.WorkCompletionProof) (authority.WorkCompleteResult, error)
	RescheduleWork(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, uint64, uint64, string, uint64, int64, string) (authority.WorkRescheduleResult, error)
}

// DueIndexScanner reads the existing bounded PLAYER Authority Source DueIndex.
type DueIndexScanner interface {
	ListDueWork(context.Context, rediskey.AuthorityBucketRef, int) ([]authority.DueWorkRef, error)
}

// SourceOrderReader loads the existing SourceOrder HASH after a claim. It is
// required so a worker can bind the claimed bytes to persisted source metadata.
type SourceOrderReader interface {
	GetSourceOrder(context.Context, rediskey.AuthorityBucketRef, string, string) (authority.SourceOrderSnapshot, error)
}

type Config struct {
	AuthorityStore *authority.Store
	WorkLease      WorkLease
	DueIndex       DueIndexScanner
	SourceOrders   SourceOrderReader
	MailEntries    mail.MailEntryRepository
	Limits         mail.Limits

	// Buckets must be a bounded list of PLAYER Authority buckets from the
	// same route/hash snapshot as AuthorityStore.
	Buckets []rediskey.AuthorityBucketRef
	// OwnedBuckets is the explicit subset this worker instance may scan and
	// claim. It must be non-empty and every bucket must belong to Buckets.
	// Ownership is intentionally supplied by composition; this worker never
	// assumes that every configured PLAYER bucket is owned.
	OwnedBuckets []rediskey.AuthorityBucketRef

	Owner  string
	Token  string
	BootID string

	ClaimLimit       int
	BucketsPerRound  int
	RoundTimeBudget  time.Duration
	MaxConcurrent    int
	LeaseTTL         time.Duration
	SafetyMargin     time.Duration
	OperationTimeout time.Duration
	RetryDelay       time.Duration
	Now              func() time.Time
}

type RoundStats struct {
	Buckets         uint64
	Scanned         uint64
	Candidates      uint64
	OwnedBuckets    uint64
	Claimed         uint64
	Inserted        uint64
	Duplicates      uint64
	Completed       uint64
	Conflicts       uint64
	Rescheduled     uint64
	BadPayload      uint64
	Unknown         uint64
	Failed          uint64
	LeaseRenewed    uint64
	LeaseLost       uint64
	BudgetExhausted uint64
}

type DueIndexObservation struct {
	Depth         int64
	OldestDueAtMs int64
	Available     bool
}

type normalizedConfig struct {
	WorkLease        WorkLease
	DueIndex         DueIndexScanner
	SourceOrders     SourceOrderReader
	MailEntries      mail.MailEntryRepository
	Limits           mail.Limits
	Buckets          []rediskey.AuthorityBucketRef
	OwnedBuckets     []rediskey.AuthorityBucketRef
	Owner            string
	Token            string
	BootID           string
	ClaimLimit       int
	MaxConcurrent    int
	BucketsPerRound  int
	RoundTimeBudget  time.Duration
	LeaseTTL         time.Duration
	SafetyMargin     time.Duration
	OperationTimeout time.Duration
	RetryDelay       time.Duration
	Now              func() time.Time
}

// Worker is a bounded, restart-safe MAIL_DELIVERY_ORDER consumer. The only
// mutation it performs outside Authority lease state is InsertMailIfAbsent.
type Worker struct {
	nextBucket      int
	observationNext int
	config          normalizedConfig
	mu              sync.Mutex
	observations    map[string]DueIndexObservation
}

type bucketRound struct {
	stats RoundStats
	err   error
}

type itemResult struct {
	Claimed, Inserted, Duplicates, Completed, Conflicts, Rescheduled uint64
	BadPayload, Unknown, Failed, LeaseRenewed, LeaseLost             uint64
	err                                                              error
}

type leaseWindow struct {
	safeDeadline time.Time
	renewed      bool
}

func NewWorker(config Config) (*Worker, error) {
	if config.AuthorityStore != nil {
		if config.WorkLease == nil {
			config.WorkLease = config.AuthorityStore.WorkLease()
		}
		if config.DueIndex == nil {
			config.DueIndex = config.AuthorityStore.WorkLease()
		}
		if config.SourceOrders == nil {
			config.SourceOrders = config.AuthorityStore.WorkLease()
		}
	}
	if config.Limits.ProfileVersion == "" {
		config.Limits = mail.LimitsV1()
	}
	frozenLimits := mail.LimitsV1()
	if !reflect.DeepEqual(config.Limits, frozenLimits) {
		return nil, fmt.Errorf("%w: MAIL_DELIVERY_ORDER requires mail.LimitsV1", ErrInvalidConfig)
	}
	if err := config.Limits.Validate(); err != nil {
		return nil, fmt.Errorf("%w: mail limits: %v", ErrInvalidConfig, err)
	}
	if config.WorkLease == nil || config.DueIndex == nil || config.SourceOrders == nil ||
		config.MailEntries == nil || strings.TrimSpace(config.Owner) == "" ||
		strings.TrimSpace(config.Token) == "" || strings.TrimSpace(config.BootID) == "" {
		return nil, fmt.Errorf("%w: lease, source order, due index, mail repository and worker identity are required", ErrInvalidConfig)
	}
	if len(config.Buckets) == 0 {
		return nil, fmt.Errorf("%w: at least one PLAYER Authority bucket is required", ErrInvalidConfig)
	}
	if len(config.OwnedBuckets) == 0 {
		return nil, fmt.Errorf("%w: explicit OwnedBuckets subset is required", ErrInvalidConfig)
	}
	if config.ClaimLimit <= 0 {
		config.ClaimLimit = defaultClaimLimit
	}
	if config.ClaimLimit > 4096 {
		return nil, fmt.Errorf("%w: claim limit exceeds bounded discovery limit", ErrInvalidConfig)
	}
	if config.MaxConcurrent <= 0 {
		config.MaxConcurrent = defaultMaxConcurrent
	}
	if config.BucketsPerRound <= 0 {
		config.BucketsPerRound = defaultBucketsPerRound
	}
	if config.BucketsPerRound > maxBucketsPerRound {
		return nil, fmt.Errorf("%w: buckets per round exceeds bounded limit", ErrInvalidConfig)
	}
	if config.RoundTimeBudget <= 0 {
		config.RoundTimeBudget = defaultRoundTimeBudget
	}
	if config.RoundTimeBudget > time.Minute {
		return nil, fmt.Errorf("%w: round time budget exceeds one minute", ErrInvalidConfig)
	}
	if config.OperationTimeout <= 0 {
		config.OperationTimeout = defaultOperationTimeout
	}
	if config.LeaseTTL <= 0 {
		config.LeaseTTL = defaultLeaseTTL
	}
	if config.SafetyMargin <= 0 || config.SafetyMargin >= config.LeaseTTL {
		config.SafetyMargin = config.LeaseTTL / 5
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = defaultRetryDelay
	}
	if config.Now == nil {
		config.Now = time.Now
	}

	buckets := make([]rediskey.AuthorityBucketRef, 0, len(config.Buckets))
	seen := make(map[string]struct{}, len(config.Buckets))
	for _, bucket := range config.Buckets {
		if err := bucket.Validate(); err != nil {
			return nil, fmt.Errorf("%w: PLAYER Authority bucket: %v", ErrInvalidConfig, err)
		}
		if bucket.Domain() != actorrouting.DomainPlayer {
			return nil, fmt.Errorf("%w: non-PLAYER Authority bucket", ErrInvalidConfig)
		}
		key := fmt.Sprintf("%s/%d", bucket.LayoutVersion(), bucket.VirtualShard())
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		buckets = append(buckets, bucket)
	}
	if len(buckets) == 0 {
		return nil, fmt.Errorf("%w: no valid PLAYER Authority buckets", ErrInvalidConfig)
	}
	// Normalize the configured universe once, then validate the explicit
	// ownership subset against it. The worker never promotes the universe to
	// ownership implicitly.
	universe := make(map[string]rediskey.AuthorityBucketRef, len(buckets))
	for _, bucket := range buckets {
		universe[fmt.Sprintf("%s/%d", bucket.LayoutVersion(), bucket.VirtualShard())] = bucket
	}
	ownedBuckets := make([]rediskey.AuthorityBucketRef, 0, len(config.OwnedBuckets))
	ownedSeen := make(map[string]struct{}, len(config.OwnedBuckets))
	for _, bucket := range config.OwnedBuckets {
		if err := bucket.Validate(); err != nil {
			return nil, fmt.Errorf("%w: owned PLAYER Authority bucket: %v", ErrInvalidConfig, err)
		}
		if bucket.Domain() != actorrouting.DomainPlayer {
			return nil, fmt.Errorf("%w: owned bucket is not PLAYER Authority", ErrInvalidConfig)
		}
		key := fmt.Sprintf("%s/%d", bucket.LayoutVersion(), bucket.VirtualShard())
		canonical, ok := universe[key]
		if !ok {
			return nil, fmt.Errorf("%w: owned bucket %s is outside configured universe", ErrInvalidConfig, key)
		}
		if _, exists := ownedSeen[key]; exists {
			return nil, fmt.Errorf("%w: duplicate owned bucket %s", ErrInvalidConfig, key)
		}
		ownedSeen[key] = struct{}{}
		ownedBuckets = append(ownedBuckets, canonical)
	}
	if len(ownedBuckets) == 0 {
		return nil, fmt.Errorf("%w: no valid owned PLAYER Authority buckets", ErrInvalidConfig)
	}
	sort.Slice(buckets, func(i, j int) bool {
		if buckets[i].LayoutVersion() == buckets[j].LayoutVersion() {
			return buckets[i].VirtualShard() < buckets[j].VirtualShard()
		}
		return buckets[i].LayoutVersion() < buckets[j].LayoutVersion()
	})
	sort.Slice(ownedBuckets, func(i, j int) bool {
		if ownedBuckets[i].LayoutVersion() == ownedBuckets[j].LayoutVersion() {
			return ownedBuckets[i].VirtualShard() < ownedBuckets[j].VirtualShard()
		}
		return ownedBuckets[i].LayoutVersion() < ownedBuckets[j].LayoutVersion()
	})
	if config.BucketsPerRound > len(ownedBuckets) {
		config.BucketsPerRound = len(ownedBuckets)
	}

	return &Worker{config: normalizedConfig{
		WorkLease: config.WorkLease, DueIndex: config.DueIndex, SourceOrders: config.SourceOrders,
		MailEntries: config.MailEntries, Limits: config.Limits, Buckets: buckets, OwnedBuckets: ownedBuckets,
		Owner: config.Owner, Token: config.Token, BootID: config.BootID,
		ClaimLimit: config.ClaimLimit, MaxConcurrent: config.MaxConcurrent,
		BucketsPerRound: config.BucketsPerRound, RoundTimeBudget: config.RoundTimeBudget,
		LeaseTTL: config.LeaseTTL, SafetyMargin: config.SafetyMargin,
		OperationTimeout: config.OperationTimeout, RetryDelay: config.RetryDelay, Now: config.Now,
	}, observations: make(map[string]DueIndexObservation, len(ownedBuckets))}, nil
}

// DecodeMailDeliveryPayload strictly accepts the canonical MailEntryV1 blob
// used as AttachedOrder.Payload by the source authority.
func DecodeMailDeliveryPayload(raw []byte) (mail.MailEntry, error) {
	return decodeMailDeliveryPayload(raw, mail.LimitsV1())
}

func decodeMailDeliveryPayload(raw []byte, limits mail.Limits) (mail.MailEntry, error) {
	if len(raw) == 0 || len(raw) > limits.MaxMailEntryBytes {
		return mail.MailEntry{}, fmt.Errorf("%w: payload size", ErrPayloadInvalid)
	}
	entry, err := mail.DecodeMailEntry(raw)
	if err != nil {
		return mail.MailEntry{}, fmt.Errorf("%w: decode MailEntryV1: %v", ErrPayloadInvalid, err)
	}
	canonical, err := mail.EncodeMailEntry(entry, limits)
	if err != nil {
		return mail.MailEntry{}, fmt.Errorf("%w: encode MailEntryV1: %v", ErrPayloadInvalid, err)
	}
	if !bytes.Equal(canonical, raw) {
		return mail.MailEntry{}, fmt.Errorf("%w: non-deterministic MailEntryV1 encoding", ErrPayloadInvalid)
	}
	if err := validateFrozenEntry(entry, limits); err != nil {
		return mail.MailEntry{}, err
	}
	return entry, nil
}

func validateFrozenEntry(entry mail.MailEntry, limits mail.Limits) error {
	if entry.ExpireAtMs <= entry.VisibleAtMs {
		return fmt.Errorf("%w: invalid retention boundary", ErrPayloadInvalid)
	}
	retention := entry.ExpireAtMs - entry.VisibleAtMs
	rebuilt, err := mail.BuildMailEntry(mail.DeliveryParams{
		RecipientPlayerID:   entry.RecipientPlayerID,
		DeliveryID:          entry.DeliveryID,
		SourceType:          entry.SourceType,
		SourceID:            entry.SourceID,
		TitleSnapshot:       entry.TitleSnapshot,
		BodySnapshot:        entry.BodySnapshot,
		Reward:              entry.Reward,
		TemplateVersion:     entry.TemplateVersion,
		RewardConfigVersion: entry.RewardConfigVersion,
		CreatedAtMs:         entry.CreatedAtMs,
		VisibleAtMs:         entry.VisibleAtMs,
		RetentionMs:         retention,
	}, limits)
	if err != nil {
		return fmt.Errorf("%w: frozen entry validation: %v", ErrPayloadInvalid, err)
	}
	if !reflect.DeepEqual(rebuilt, entry) {
		return fmt.Errorf("%w: frozen entry/hash or boundary mismatch", ErrPayloadInvalid)
	}
	mailID, err := mail.MailID(entry.RecipientPlayerID, entry.DeliveryID)
	if err != nil || entry.MailID != mailID {
		return fmt.Errorf("%w: mail id does not bind recipient/delivery", ErrPayloadInvalid)
	}
	deliveryHash, err := mail.DeliveryPayloadHash(entry)
	if err != nil || entry.DeliveryPayloadHash != deliveryHash {
		return fmt.Errorf("%w: delivery payload hash mismatch", ErrPayloadInvalid)
	}
	if entry.HasAttachment() {
		claimHash, err := mail.ClaimMaterialHash(entry)
		if err != nil || entry.ClaimMaterialHash != claimHash {
			return fmt.Errorf("%w: claim material hash mismatch", ErrPayloadInvalid)
		}
	}
	return nil
}

// RunOnce performs one bounded scan of every configured PLAYER Authority
// bucket. It never scans another domain and filters before claiming.
func (w *Worker) RunOnce(ctx context.Context) (RoundStats, error) {
	if w == nil || w.config.WorkLease == nil || w.config.DueIndex == nil ||
		w.config.SourceOrders == nil || w.config.MailEntries == nil {
		return RoundStats{}, fmt.Errorf("%w: incomplete worker", ErrInvalidConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	roundCtx, roundCancel := context.WithTimeout(ctx, w.config.RoundTimeBudget)
	defer roundCancel()
	buckets := w.nextRoundBuckets()
	sem := make(chan struct{}, w.config.MaxConcurrent)
	results := make(chan bucketRound, len(buckets))
	var wg sync.WaitGroup
	for _, bucket := range buckets {
		bucket := bucket
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- w.runBucket(roundCtx, bucket, sem)
		}()
	}
	wg.Wait()
	close(results)

	stats := RoundStats{OwnedBuckets: uint64(len(w.config.OwnedBuckets))}
	var errs []error
	for result := range results {
		stats.Buckets += result.stats.Buckets
		stats.Scanned += result.stats.Scanned
		stats.Candidates += result.stats.Candidates
		stats.Claimed += result.stats.Claimed
		stats.Inserted += result.stats.Inserted
		stats.Duplicates += result.stats.Duplicates
		stats.Completed += result.stats.Completed
		stats.Conflicts += result.stats.Conflicts
		stats.Rescheduled += result.stats.Rescheduled
		stats.BadPayload += result.stats.BadPayload
		stats.Unknown += result.stats.Unknown
		stats.Failed += result.stats.Failed
		stats.LeaseRenewed += result.stats.LeaseRenewed
		stats.LeaseLost += result.stats.LeaseLost
		stats.BudgetExhausted += result.stats.BudgetExhausted
		if result.err != nil {
			budgetExceeded := errors.Is(roundCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil
			if !budgetExceeded || !errors.Is(result.err, context.DeadlineExceeded) {
				errs = append(errs, result.err)
			}
		}
	}
	if errors.Is(roundCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
		stats.BudgetExhausted++
	}
	return stats, errors.Join(errs...)
}

func (w *Worker) PendingDue(ctx context.Context) (int64, error) {
	// nextRoundBuckets advances a process-local rotating cursor over the
	// explicitly owned subset. It never expands to the configured bucket universe.
	if w == nil || w.config.DueIndex == nil {
		return 0, fmt.Errorf("%w: incomplete worker", ErrInvalidConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var total int64
	var errs []error
	for _, bucket := range w.config.OwnedBuckets {
		refs, err := w.config.DueIndex.ListDueWork(ctx, bucket, w.config.ClaimLimit)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, ref := range refs {
			if ref.Ref.WorkType == FamilyMailDeliveryOrder {
				total++
			}
		}
	}
	return total, errors.Join(errs...)
}

func bucketObservationKey(bucket rediskey.AuthorityBucketRef) string {
	return fmt.Sprintf("%s/%d", bucket.LayoutVersion(), bucket.VirtualShard())
}

// nextRoundBuckets advances a process-local rotating cursor over the
// explicitly owned subset. It never expands to the configured bucket universe.
func (w *Worker) nextRoundBuckets() []rediskey.AuthorityBucketRef {
	w.mu.Lock()
	defer w.mu.Unlock()
	owned := w.config.OwnedBuckets
	if len(owned) == 0 {
		return nil
	}
	start := w.nextBucket % len(owned)
	count := w.config.BucketsPerRound
	if count <= 0 || count > len(owned) {
		count = len(owned)
	}
	buckets := make([]rediskey.AuthorityBucketRef, 0, count)
	for offset := 0; offset < count; offset++ {
		buckets = append(buckets, owned[(start+offset)%len(owned)])
	}
	w.nextBucket = (start + count) % len(owned)
	return buckets
}

func (w *Worker) nextObservationBuckets() []rediskey.AuthorityBucketRef {
	w.mu.Lock()
	defer w.mu.Unlock()
	owned := w.config.OwnedBuckets
	if len(owned) == 0 {
		return nil
	}
	start := w.observationNext % len(owned)
	count := w.config.BucketsPerRound
	if count <= 0 || count > len(owned) {
		count = len(owned)
	}
	buckets := make([]rediskey.AuthorityBucketRef, 0, count)
	for offset := 0; offset < count; offset++ {
		buckets = append(buckets, owned[(start+offset)%len(owned)])
	}
	w.observationNext = (start + count) % len(owned)
	return buckets
}

func (w *Worker) recordDueObservation(bucket rediskey.AuthorityBucketRef, observation DueIndexObservation) DueIndexObservation {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.observations[bucketObservationKey(bucket)] = observation
	combined := DueIndexObservation{Available: len(w.observations) == len(w.config.OwnedBuckets)}
	for _, owned := range w.config.OwnedBuckets {
		observed, ok := w.observations[bucketObservationKey(owned)]
		if !ok || !observed.Available {
			combined.Available = false
			continue
		}
		combined.Depth += observed.Depth
		if combined.OldestDueAtMs == 0 || (observed.OldestDueAtMs > 0 && observed.OldestDueAtMs < combined.OldestDueAtMs) {
			combined.OldestDueAtMs = observed.OldestDueAtMs
		}
	}
	return combined
}

// ObserveDue returns a bounded readiness proof. If the per-bucket discovery
// bound is exhausted, Available is false instead of claiming a partial view.
func (w *Worker) ObserveDue(ctx context.Context, limit int) (DueIndexObservation, error) {
	if w == nil || w.config.DueIndex == nil || limit <= 0 || limit > 4096 {
		return DueIndexObservation{}, fmt.Errorf("%w: due observation limit", ErrInvalidConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	roundCtx, cancel := context.WithTimeout(ctx, w.config.RoundTimeBudget)
	defer cancel()
	buckets := w.nextObservationBuckets()
	combined := DueIndexObservation{}
	for _, bucket := range buckets {
		observation := DueIndexObservation{Available: true}
		probeLimit := limit + 1
		if probeLimit > 4096 {
			probeLimit = 4096
		}
		refs, err := w.config.DueIndex.ListDueWork(roundCtx, bucket, probeLimit)
		if err != nil {
			return DueIndexObservation{}, fmt.Errorf("bucket %d due index: %w", bucket.VirtualShard(), err)
		}
		if len(refs) >= probeLimit {
			observation.Available = false
		} else {
			for _, ref := range refs {
				if ref.Ref.WorkType != FamilyMailDeliveryOrder {
					continue
				}
				observation.Depth++
				if observation.OldestDueAtMs == 0 || ref.DueAtMs < observation.OldestDueAtMs {
					observation.OldestDueAtMs = ref.DueAtMs
				}
			}
		}
		combined = w.recordDueObservation(bucket, observation)
	}
	return combined, nil
}

func (w *Worker) runBucket(ctx context.Context, bucket rediskey.AuthorityBucketRef, sem chan struct{}) bucketRound {
	result := bucketRound{stats: RoundStats{Buckets: 1}}
	refs, err := w.config.DueIndex.ListDueWork(ctx, bucket, w.config.ClaimLimit)
	if err != nil {
		result.err = fmt.Errorf("bucket %d due index: %w", bucket.VirtualShard(), err)
		return result
	}
	result.stats.Scanned = uint64(len(refs))
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, due := range refs {
		if due.Ref.WorkType != FamilyMailDeliveryOrder {
			continue
		}
		result.stats.Candidates++
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func(due authority.DueWorkRef) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			item := w.processDue(ctx, bucket, due)
			mu.Lock()
			result.stats.Claimed += item.Claimed
			result.stats.Inserted += item.Inserted
			result.stats.Duplicates += item.Duplicates
			result.stats.Completed += item.Completed
			result.stats.Conflicts += item.Conflicts
			result.stats.Rescheduled += item.Rescheduled
			result.stats.BadPayload += item.BadPayload
			result.stats.Unknown += item.Unknown
			result.stats.Failed += item.Failed
			result.stats.LeaseRenewed += item.LeaseRenewed
			result.stats.LeaseLost += item.LeaseLost
			if item.err != nil {
				result.err = errors.Join(result.err, item.err)
			}
			mu.Unlock()
		}(due)
	}
	wg.Wait()
	return result
}

func (w *Worker) processDue(ctx context.Context, bucket rediskey.AuthorityBucketRef, due authority.DueWorkRef) itemResult {
	var out itemResult
	claimStarted := w.config.Now()
	claimCtx, claimCancel := operationContext(ctx, w.config.OperationTimeout, time.Time{})
	claim, err := w.config.WorkLease.ClaimWorkWithBoot(claimCtx, bucket, FamilyMailDeliveryOrder,
		due.Ref.WorkID, w.config.Owner, w.config.BootID, w.config.Token, w.config.LeaseTTL.Milliseconds())
	claimCancel()
	if err != nil {
		out.err = fmt.Errorf("claim %s: %w", due.Ref.WorkID, err)
		return out
	}
	if claim.Status != authority.WorkClaimClaimed {
		return out
	}
	out.Claimed = 1

	entry, err := validateClaim(due, claim, w.config.Limits)
	if err != nil {
		out.BadPayload = 1
		out.Rescheduled = 1
		out.err = w.retry(ctx, bucket, claim, err)
		return out
	}
	sourceCtx, sourceCancel := operationContext(ctx, w.config.OperationTimeout, time.Time{})
	source, sourceErr := w.config.SourceOrders.GetSourceOrder(sourceCtx, bucket, FamilyMailDeliveryOrder, claim.WorkID)
	sourceCancel()
	if sourceErr != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.err = w.retry(ctx, bucket, claim, sourceErr)
		return out
	}
	if err := validateSourceOrder(bucket, claim, entry, source); err != nil {
		if errors.Is(err, ErrLeaseLost) {
			out.LeaseLost = 1
		} else {
			out.BadPayload = 1
		}
		out.Rescheduled = 1
		out.err = w.retry(ctx, bucket, claim, err)
		return out
	}

	window, err := w.ensureLease(ctx, bucket, claim, claimStarted)
	if err != nil {
		if errors.Is(err, ErrLeaseLost) {
			out.LeaseLost = 1
		}
		out.Rescheduled = 1
		out.err = w.retry(ctx, bucket, claim, err)
		return out
	}
	if window.renewed {
		out.LeaseRenewed = 1
	}
	if !w.config.Now().Before(window.safeDeadline) {
		out.LeaseLost = 1
		out.Rescheduled = 1
		out.err = w.retry(ctx, bucket, claim, ErrLeaseLost)
		return out
	}

	insertCtx, insertCancel := operationContext(ctx, w.config.OperationTimeout, window.safeDeadline)
	result, insertErr := w.config.MailEntries.InsertMailIfAbsent(insertCtx, entry)
	insertContextErr := insertCtx.Err()
	insertCancel()
	if insertErr != nil || insertContextErr != nil {
		if insertContextErr != nil && insertErr == nil {
			insertErr = insertContextErr
		}
		if insertErr == nil {
			insertErr = errors.New("mail insert failed")
		}
		out.Unknown = 1
		out.Rescheduled = 1
		out.err = w.retry(ctx, bucket, claim, insertErr)
		return out
	}

	var outcome string
	switch result.Status {
	case mail.InsertInserted:
		out.Inserted = 1
		outcome = string(mail.InsertInserted)
	case mail.InsertAlreadyDelivered:
		out.Duplicates = 1
		outcome = string(mail.InsertAlreadyDelivered)
	case mail.InsertDeliveryConflict:
		out.Conflicts = 1
		out.Rescheduled = 1
		out.err = w.retry(ctx, bucket, claim, fmt.Errorf("%w: delivery id conflict", ErrPayloadInvalid))
		return out
	case mail.InsertTimeoutUnknown:
		out.Unknown = 1
		out.Rescheduled = 1
		out.err = w.retry(ctx, bucket, claim, errors.New("mail insert TIMEOUT_UNKNOWN"))
		return out
	case mail.InsertUnavailableNotSent, mail.InsertMailboxFullProtected,
		mail.InsertCapacityExceeded, mail.InsertNotVisibleYet:
		out.Failed = 1
		out.Rescheduled = 1
		out.err = w.retry(ctx, bucket, claim, fmt.Errorf("mail insert status %s", result.Status))
		return out
	default:
		out.Failed = 1
		out.Rescheduled = 1
		out.err = w.retry(ctx, bucket, claim, fmt.Errorf("mail insert unsupported status %q", result.Status))
		return out
	}

	if !w.config.Now().Before(window.safeDeadline) {
		out.LeaseLost = 1
		out.err = fmt.Errorf("%w: lease expired after mail insert", ErrLeaseLost)
		return out
	}
	proof, err := buildCompletionProof(claim, entry, source, outcome)
	if err != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.err = w.retry(ctx, bucket, claim, err)
		return out
	}
	completeCtx, completeCancel := operationContext(ctx, w.config.OperationTimeout, window.safeDeadline)
	complete, completeErr := w.config.WorkLease.CompleteWorkWithProof(completeCtx, bucket, FamilyMailDeliveryOrder,
		claim.WorkID, w.config.Owner, w.config.BootID, claim.LeaseEpoch, claim.WorkVersion, w.config.Token, proof)
	completeCancel()
	if completeErr != nil {
		if errors.Is(completeErr, context.DeadlineExceeded) || errors.Is(completeErr, context.Canceled) {
			out.LeaseLost = 1
			out.err = fmt.Errorf("%w: complete %s: %v", ErrLeaseLost, claim.WorkID, completeErr)
		} else {
			out.Failed = 1
			out.err = fmt.Errorf("complete %s: %w", claim.WorkID, completeErr)
		}
		return out
	}
	switch complete.Status {
	case authority.WorkLeaseCompleted, authority.WorkLeaseAlreadyCompleted:
		out.Completed = 1
	case authority.WorkLeaseFenceMismatch, authority.WorkLeaseExpired,
		authority.WorkLeaseStaleVersion, authority.WorkLeaseStaleGeneration,
		authority.WorkLeaseNotFound:
		out.LeaseLost = 1
		out.err = fmt.Errorf("%w: complete %s status=%s", ErrLeaseLost, claim.WorkID, complete.Status)
	default:
		out.Failed = 1
		out.err = fmt.Errorf("complete %s status=%s", claim.WorkID, complete.Status)
	}
	return out
}

func validateClaim(due authority.DueWorkRef, claim authority.WorkClaim, limits mail.Limits) (mail.MailEntry, error) {
	if due.Ref.WorkType != FamilyMailDeliveryOrder ||
		workidentity.ValidateStableOpaqueID(due.Ref.WorkID) != nil ||
		claim.WorkID != due.Ref.WorkID || claim.WorkType != FamilyMailDeliveryOrder ||
		claim.SubjectPlayerID == 0 || strings.TrimSpace(claim.OperationID) == "" ||
		claim.WorkVersion == 0 || claim.LeaseEpoch == 0 || claim.AcceptedTTLms <= 0 ||
		len(claim.Payload) == 0 {
		return mail.MailEntry{}, fmt.Errorf("%w: claimed SourceOrder identity", ErrPayloadInvalid)
	}
	if err := mail.ValidateHash(claim.PayloadHash, false); err != nil {
		return mail.MailEntry{}, fmt.Errorf("%w: payload hash shape: %v", ErrPayloadInvalid, err)
	}
	digest := sha256.Sum256(claim.Payload)
	if claim.PayloadHash != hex.EncodeToString(digest[:]) {
		return mail.MailEntry{}, fmt.Errorf("%w: SourceOrder payload_hash mismatch", ErrPayloadInvalid)
	}
	entry, err := decodeMailDeliveryPayload(claim.Payload, limits)
	if err != nil {
		return mail.MailEntry{}, err
	}
	if entry.RecipientPlayerID != claim.SubjectPlayerID {
		return mail.MailEntry{}, fmt.Errorf("%w: recipient/subject mismatch", ErrPayloadInvalid)
	}
	return entry, nil
}

func validateSourceOrder(bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, entry mail.MailEntry, source authority.SourceOrderSnapshot) error {
	if source.WorkID != claim.WorkID || source.WorkType != FamilyMailDeliveryOrder ||
		source.PlacementKind != mailPlacementKind || source.BucketVersion != 1 ||
		source.BucketID != bucket.VirtualShard() || source.SourceAuthorityKind != mailSourceKind ||
		source.CanonicalSourceAuthorityID != strconv.FormatUint(entry.RecipientPlayerID, 10) ||
		source.AuthorityLayoutVersion != bucket.LayoutVersion() || source.ActorRouteDomain != mailRouteDomain ||
		source.SubjectPlayerID != entry.RecipientPlayerID || source.WorkVersion != claim.WorkVersion ||
		source.OperationID != claim.OperationID || source.PayloadHash != claim.PayloadHash ||
		!bytes.Equal(source.TypedPayload, claim.Payload) ||
		(source.State != "PENDING" && source.State != "REWARD_DISPATCHING") {
		return fmt.Errorf("%w: SourceOrder binding", ErrPayloadInvalid)
	}
	if source.CurrentStep != "" && source.CurrentStep != mailDeliveryStep {
		return fmt.Errorf("%w: SourceOrder step %q", ErrPayloadInvalid, source.CurrentStep)
	}
	if source.LeaseEpoch != 0 && source.LeaseEpoch != claim.LeaseEpoch {
		return fmt.Errorf("%w: SourceOrder lease epoch", ErrLeaseLost)
	}
	return nil
}

func (w *Worker) ensureLease(ctx context.Context, bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, claimStarted time.Time) (leaseWindow, error) {
	accepted := time.Duration(claim.AcceptedTTLms) * time.Millisecond
	if accepted <= 0 {
		return leaseWindow{}, ErrLeaseLost
	}
	safeDeadline := claimStarted.Add(accepted - w.config.SafetyMargin)
	need := w.config.OperationTimeout + w.config.SafetyMargin
	now := w.config.Now()
	if now.Before(safeDeadline) && safeDeadline.Sub(now) >= need {
		return leaseWindow{safeDeadline: safeDeadline}, nil
	}
	renewStarted := w.config.Now()
	renewCtx, renewCancel := operationContext(ctx, w.config.OperationTimeout, time.Time{})
	renew, err := w.config.WorkLease.RenewWorkLeaseWithBoot(renewCtx, bucket, FamilyMailDeliveryOrder,
		claim.WorkID, w.config.Owner, w.config.BootID, claim.LeaseEpoch, w.config.Token, w.config.LeaseTTL.Milliseconds())
	renewCancel()
	if err != nil {
		return leaseWindow{}, fmt.Errorf("%w: renew %s: %v", ErrLeaseLost, claim.WorkID, err)
	}
	if renew.Status != authority.WorkLeaseRenewed || renew.AcceptedTTLms <= 0 {
		return leaseWindow{}, fmt.Errorf("%w: renew %s status=%s", ErrLeaseLost, claim.WorkID, renew.Status)
	}
	accepted = time.Duration(renew.AcceptedTTLms) * time.Millisecond
	safeDeadline = renewStarted.Add(accepted - w.config.SafetyMargin)
	if !w.config.Now().Before(safeDeadline) || safeDeadline.Sub(w.config.Now()) < need {
		return leaseWindow{}, fmt.Errorf("%w: renew %s leaves no operation budget", ErrLeaseLost, claim.WorkID)
	}
	return leaseWindow{safeDeadline: safeDeadline, renewed: true}, nil
}

func (w *Worker) retry(ctx context.Context, bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, cause error) error {
	if cause == nil {
		cause = ErrRetryDeferred
	}
	if ctx != nil && ctx.Err() != nil {
		return fmt.Errorf("%w: %v", ErrRetryDeferred, cause)
	}
	if claim.RetryCount == ^uint64(0) {
		return fmt.Errorf("%w: retry count overflow: %v", ErrRetryDeferred, cause)
	}
	attempt := claim.RetryCount + 1
	delayMs := w.config.RetryDelay.Milliseconds()
	if delayMs < 1 {
		delayMs = 1
	}
	rescheduleCtx, cancel := context.WithTimeout(context.Background(), w.config.OperationTimeout)
	rescheduled, err := w.config.WorkLease.RescheduleWork(rescheduleCtx, bucket, FamilyMailDeliveryOrder,
		claim.WorkID, w.config.Owner, w.config.BootID, claim.LeaseEpoch, claim.WorkVersion,
		w.config.Token, attempt, delayMs, errorClass(cause))
	cancel()
	if err != nil {
		return fmt.Errorf("%w: %w; reschedule: %v", ErrRetryDeferred, cause, err)
	}
	switch rescheduled.Status {
	case authority.WorkLeaseRenewed, authority.WorkLeaseReconcileRequired:
		return fmt.Errorf("%w: %w", ErrRetryDeferred, cause)
	case authority.WorkLeaseFenceMismatch, authority.WorkLeaseExpired,
		authority.WorkLeaseStaleVersion, authority.WorkLeaseStaleGeneration:
		return fmt.Errorf("%w: reschedule %s status=%s", ErrLeaseLost, claim.WorkID, rescheduled.Status)
	default:
		return fmt.Errorf("%w: %w; reschedule status=%s", ErrRetryDeferred, cause, rescheduled.Status)
	}
}

func errorClass(err error) string {
	switch {
	case errors.Is(err, ErrPayloadInvalid):
		return "PAYLOAD_INVALID"
	case errors.Is(err, ErrLeaseLost):
		return "LEASE_LOST"
	default:
		return "RETRYABLE_ERROR"
	}
}

func buildCompletionProof(claim authority.WorkClaim, entry mail.MailEntry, source authority.SourceOrderSnapshot, outcome string) (authority.WorkCompletionProof, error) {
	if err := mail.ValidateHash(claim.PayloadHash, false); err != nil {
		return authority.WorkCompletionProof{}, fmt.Errorf("%w: completion payload hash", ErrPayloadInvalid)
	}
	resultHash := deliveryResultHash(claim, entry, outcome)
	proof := authority.WorkCompletionProof{
		SourceAuthorityKind:        source.SourceAuthorityKind,
		CanonicalSourceAuthorityID: source.CanonicalSourceAuthorityID,
		AuthorityLayoutVersion:     source.AuthorityLayoutVersion,
		ActorRouteDomain:           source.ActorRouteDomain,
		PayloadHash:                claim.PayloadHash,
		InvocationID:               claim.WorkID,
		ParticipantStep:            mailDeliveryStep,
		ParticipantOutcome:         outcome,
		ParticipantResultHash:      resultHash,
		ParticipantResultVersion:   claim.WorkVersion,
	}
	proof.ResultProofHash = authority.WorkCompletionProofHash(proof)
	return proof, nil
}

func deliveryResultHash(claim authority.WorkClaim, entry mail.MailEntry, outcome string) string {
	h := sha256.New()
	for _, value := range []string{
		"MAIL_DELIVERY_RESULT_V1",
		claim.WorkID,
		strconv.FormatUint(claim.WorkVersion, 10),
		entry.MailID,
		entry.DeliveryID,
		entry.DeliveryPayloadHash,
		outcome,
	} {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(value))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func operationContext(parent context.Context, timeout time.Duration, safeDeadline time.Time) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if timeout <= 0 {
		timeout = defaultOperationTimeout
	}
	deadline := time.Now().Add(timeout)
	if !safeDeadline.IsZero() && safeDeadline.Before(deadline) {
		deadline = safeDeadline
	}
	return context.WithDeadline(parent, deadline)
}
