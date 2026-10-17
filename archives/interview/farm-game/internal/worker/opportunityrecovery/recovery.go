package opportunityrecovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"farmgame/internal/farm/domain"
	"farmgame/internal/farm/opportunitywakeup"
	"farmgame/internal/observability/redisworkload"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
)

const (
	// WorkType is kept as a package constant for callers that register metrics
	// or readiness under the exact durable schedule type.
	WorkType = opportunitywakeup.WorkType

	DefaultClaimLimit      = 32
	DefaultDiscoveryLimit  = 64
	DefaultLeaseTTL        = 10 * time.Second
	DefaultMaxConcurrent   = 4
	DefaultRoundTimeBudget = 800 * time.Millisecond
	maxClaimLimit          = 4096
	maxBucketCount         = 4096
	maxMaxConcurrent       = 256
)

var (
	ErrInvalidConfig   = errors.New("opportunity recovery: invalid configuration")
	ErrLeaseLost       = errors.New("opportunity recovery: schedule lease lost")
	ErrInvalidSchedule = errors.New("opportunity recovery: invalid schedule")
	ErrInvalidPayload  = errors.New("opportunity recovery: invalid schedule payload")
	ErrStaleGeneration = errors.New("opportunity recovery: stale schedule generation")
	ErrNotDue          = errors.New("opportunity recovery: schedule is not due at trusted time")
)

// ScheduleStore is the narrow mutable-schedule port used by the worker. The
// production implementation is authority.WorkLeaseStore; keeping the port
// here lets unit tests exercise fencing without a Redis server.
type ScheduleStore interface {
	ListDueScheduledWork(context.Context, rediskey.AuthorityBucketRef, int64, int) ([]authority.ScheduledDueRef, error)
	ClaimScheduledWorkWithBoot(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, string, int64) (authority.WorkClaim, error)
	GetScheduledWork(context.Context, rediskey.AuthorityBucketRef, string, string) (authority.ScheduledWorkSnapshot, error)
	AdvanceScheduledWork(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, uint64, uint64, uint64, string, int64, []byte, string) (authority.ScheduledWorkAdvanceResult, error)
	CompleteScheduledWork(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, uint64, uint64, uint64, string) (authority.WorkCompleteResult, error)
}

// FarmOpportunity is the small read-only seam needed by the worker. OwnerID
// is optional for readers whose Farm ID is the owner ID; when it is zero the
// worker uses FarmID. Header and Facts must come from the same authority read.
type FarmOpportunity struct {
	FarmID  uint64
	OwnerID uint64
	Header  domain.FarmOpportunityHeaderV2
	Facts   domain.FarmOpportunityFactsV1
}

// FarmOpportunityReader reads one committed Farm Header/Facts pair. It must
// not activate FarmActor or mutate Farm state.
type FarmOpportunityReader interface {
	ReadFarmOpportunity(context.Context, uint64) (FarmOpportunity, error)
}

// TargetedHintPublisher receives only invalidation identity. It is responsible
// for looking up friends and fanning the hint out to currently online clients.
// In particular, no global mask, viewer ID, or final action mask crosses this
// boundary.
type TargetedHintPublisher interface {
	PublishTargetedHint(context.Context, uint64, uint64, uint64, uint64) error
}

// TargetedHintPublisherFunc adapts a function to TargetedHintPublisher.
type TargetedHintPublisherFunc func(context.Context, uint64, uint64, uint64, uint64) error

func (f TargetedHintPublisherFunc) PublishTargetedHint(ctx context.Context, ownerID, farmID, sourceFarmVersion, opportunityRevision uint64) error {
	if f == nil {
		return ErrInvalidConfig
	}
	return f(ctx, ownerID, farmID, sourceFarmVersion, opportunityRevision)
}

// Config contains only bounded discovery, lease identity, and read/publish
// ports. Buckets are the configured FARM Authority universe; the scheduler
// scans only OwnedShards from ShardAssignment. Schedule IDs are always taken
// from each returned DueRef.
type Config struct {
	ScheduleStore ScheduleStore
	FarmReader    FarmOpportunityReader
	Publisher     TargetedHintPublisher
	// TargetedHintPublisher is an explicit alias for Publisher for composition
	// roots that prefer naming the dependency after its port.
	TargetedHintPublisher TargetedHintPublisher

	Buckets    []rediskey.AuthorityBucketRef
	MaxBuckets int

	// ShardAssignment selects OwnedShards within Buckets. Nil defaults to a
	// Static provider that owns every configured FARM shard.
	ShardAssignment WorkerShardAssignmentProvider
	// ShardClaimAdmission rechecks the frozen assignment revision immediately
	// before every new WorkLease claim. When nil, New uses ShardAssignment if it
	// implements WorkerShardClaimAdmission.
	ShardClaimAdmission WorkerShardClaimAdmission

	// Owner/Token/BootID are the worker's lease identity. HolderID and
	// LeaseToken are compatibility aliases for callers using the authority
	// terminology; when present they must agree with the primary fields.
	Owner      string
	HolderID   string
	Token      string
	LeaseToken string
	BootID     string

	ClaimLimit      int
	DiscoveryLimit  int
	LeaseTTL        time.Duration
	MaxConcurrent   int
	RoundTimeBudget time.Duration

	// Shadow preserves the complete claim/read/advance/complete lifecycle but
	// suppresses TargetedHintPublisher calls.
	Shadow     bool
	ShadowMode bool

	// Now is the trusted time source captured once per round. It is never used
	// to mutate Farm; it only supplies ListDue and pure domain evaluation. In
	// production TrustedNowMs must be backed by Redis TIME. Now remains the
	// deterministic unit-test fallback.
	TrustedNowMs func(context.Context) (int64, error)
	Now          func() time.Time
}

// RoundStats describes one bounded round. Advanced and Completed are mutually
// exclusive for one claimed schedule. StaleGeneration is a safe no-op path:
// the worker never publishes an old generation. Observability fields carry only
// worker-aggregate counters (no farm/player labels).
type RoundStats struct {
	AssignmentRevision uint64
	OwnedShards        uint64
	Buckets            uint64
	EmptyShards        uint64
	Scanned            uint64
	Candidates         uint64
	Claimed            uint64
	LeaseHeld          uint64
	Skipped            uint64
	Published          uint64
	Advanced           uint64
	Completed          uint64
	StaleGeneration    uint64
	LeaseLost          uint64
	Failed             uint64
	DueBacklog         uint64
	MaxDueLagMs        int64
	BudgetExhausted    uint64
	ClaimRejected      uint64
	Shadow             bool
}

type normalizedConfig struct {
	schedule        ScheduleStore
	farmReader      FarmOpportunityReader
	publisher       TargetedHintPublisher
	buckets         []rediskey.AuthorityBucketRef
	bucketByShard   map[uint32]rediskey.AuthorityBucketRef
	shardAssignment WorkerShardAssignmentProvider
	claimAdmission  WorkerShardClaimAdmission
	holderID        string
	leaseToken      string
	bootID          string
	claimLimit      int
	discoveryLimit  int
	leaseTTL        time.Duration
	maxConcurrent   int
	roundTimeBudget time.Duration
	shadow          bool
	trustedNowMs    func(context.Context) (int64, error)
	now             func() time.Time
}

// Worker is a single bounded FARM_OPPORTUNITY_WAKEUP worker. It has no local
// queue: the mutable Authority schedule remains the sole durable cursor.
type Worker struct {
	config normalizedConfig

	// runMu serializes RunOnce so cursor advancement and discovery rounds are
	// safe when a caller invokes the worker concurrently.
	runMu sync.Mutex

	// nextStartShard is the shard ID at which the next round should begin. The
	// bool keeps zero a valid shard ID while allowing the first round to start
	// at the beginning of its assignment.
	nextStartShard uint32
	cursorSet      bool
}

func New(config Config) (*Worker, error) {
	schedule := config.ScheduleStore
	if schedule == nil {
		return nil, fmt.Errorf("%w: schedule store is required", ErrInvalidConfig)
	}
	reader := config.FarmReader
	if reader == nil {
		return nil, fmt.Errorf("%w: Farm Header/Facts reader is required", ErrInvalidConfig)
	}
	publisher := config.Publisher
	if publisher == nil {
		publisher = config.TargetedHintPublisher
	}

	holderID, err := aliasedIdentity(config.Owner, config.HolderID, "owner/holder id")
	if err != nil {
		return nil, err
	}
	leaseToken, err := aliasedIdentity(config.Token, config.LeaseToken, "token/lease token")
	if err != nil {
		return nil, err
	}
	bootID := strings.TrimSpace(config.BootID)
	if holderID == "" || leaseToken == "" || bootID == "" {
		return nil, fmt.Errorf("%w: owner, token, and boot id are required", ErrInvalidConfig)
	}
	if !config.Shadow && !config.ShadowMode && publisher == nil {
		return nil, fmt.Errorf("%w: publisher is required outside shadow mode", ErrInvalidConfig)
	}
	maxBuckets := config.MaxBuckets
	if maxBuckets <= 0 {
		maxBuckets = maxBucketCount
	}
	if maxBuckets > maxBucketCount {
		return nil, fmt.Errorf("%w: max buckets must be <= %d", ErrInvalidConfig, maxBucketCount)
	}
	if len(config.Buckets) == 0 || len(config.Buckets) > maxBuckets {
		return nil, fmt.Errorf("%w: buckets must contain 1..%d entries", ErrInvalidConfig, maxBuckets)
	}
	buckets := make([]rediskey.AuthorityBucketRef, len(config.Buckets))
	seenBuckets := make(map[string]struct{}, len(config.Buckets))
	for i, bucket := range config.Buckets {
		if err := bucket.Validate(); err != nil {
			return nil, fmt.Errorf("%w: bucket %d: %v", ErrInvalidConfig, i, err)
		}
		key := fmt.Sprintf("%s/%s/%d", bucket.Domain(), bucket.LayoutVersion(), bucket.VirtualShard())
		if _, exists := seenBuckets[key]; exists {
			return nil, fmt.Errorf("%w: duplicate bucket %s", ErrInvalidConfig, key)
		}
		seenBuckets[key] = struct{}{}
		buckets[i] = bucket
	}

	bucketByShard := make(map[uint32]rediskey.AuthorityBucketRef, len(buckets))
	for _, bucket := range buckets {
		bucketByShard[bucket.VirtualShard()] = bucket
	}
	shardAssignment := config.ShardAssignment
	if shardAssignment == nil {
		staticAssignment, assignErr := NewAllConfiguredShardsProvider(configuredShardsFromBuckets(buckets))
		if assignErr != nil {
			return nil, assignErr
		}
		shardAssignment = staticAssignment
	}
	assignment, err := shardAssignment.Assignment(context.Background())
	if err != nil {
		return nil, err
	}
	if err := validateOwnedShards(assignment.OwnedShards, bucketByShard, true); err != nil {
		return nil, err
	}
	claimAdmission := config.ShardClaimAdmission
	if claimAdmission == nil {
		claimAdmission, _ = shardAssignment.(WorkerShardClaimAdmission)
	}
	if claimAdmission == nil {
		claimAdmission = providerBackedClaimAdmission{provider: shardAssignment}
	}

	claimLimit := config.ClaimLimit
	if claimLimit <= 0 {
		claimLimit = DefaultClaimLimit
	}
	if claimLimit > maxClaimLimit {
		return nil, fmt.Errorf("%w: claim limit must be <= %d", ErrInvalidConfig, maxClaimLimit)
	}
	discoveryLimit := config.DiscoveryLimit
	if discoveryLimit <= 0 {
		discoveryLimit = DefaultDiscoveryLimit
	}
	if discoveryLimit > maxClaimLimit {
		return nil, fmt.Errorf("%w: discovery limit must be <= %d", ErrInvalidConfig, maxClaimLimit)
	}
	leaseTTL := config.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = DefaultLeaseTTL
	}
	if leaseTTL.Milliseconds() <= 0 {
		return nil, fmt.Errorf("%w: lease ttl must contain at least one millisecond", ErrInvalidConfig)
	}
	maxConcurrent := config.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = DefaultMaxConcurrent
	}
	if maxConcurrent > maxMaxConcurrent {
		return nil, fmt.Errorf("%w: max concurrent must be <= %d", ErrInvalidConfig, maxMaxConcurrent)
	}
	roundTimeBudget := config.RoundTimeBudget
	if roundTimeBudget <= 0 {
		roundTimeBudget = DefaultRoundTimeBudget
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}

	return &Worker{config: normalizedConfig{
		schedule: schedule, farmReader: reader, publisher: publisher, buckets: buckets,
		bucketByShard: bucketByShard, shardAssignment: shardAssignment, claimAdmission: claimAdmission,
		holderID: holderID, leaseToken: leaseToken, bootID: bootID,
		claimLimit: claimLimit, discoveryLimit: discoveryLimit, leaseTTL: leaseTTL,
		maxConcurrent: maxConcurrent, roundTimeBudget: roundTimeBudget,
		shadow: config.Shadow || config.ShadowMode, trustedNowMs: config.TrustedNowMs, now: now,
	}}, nil
}

func validateOwnedShards(owned []uint32, bucketByShard map[uint32]rediskey.AuthorityBucketRef, allowEmpty bool) error {
	_, err := normalizeAndValidateOwnedShards(owned, bucketByShard, allowEmpty)
	return err
}

func normalizeAndValidateOwnedShards(owned []uint32, bucketByShard map[uint32]rediskey.AuthorityBucketRef, allowEmpty bool) ([]uint32, error) {
	normalized, err := normalizeOwnedShards(owned, allowEmpty)
	if err != nil {
		return nil, err
	}
	for _, shard := range normalized {
		if _, ok := bucketByShard[shard]; !ok {
			return nil, fmt.Errorf("%w: owned shard %d is outside configured FARM buckets", ErrInvalidConfig, shard)
		}
	}
	return normalized, nil
}

func (w *Worker) nextStartIndex(shards []uint32) int {
	if !w.cursorSet {
		return 0
	}
	for index, shard := range shards {
		if shard >= w.nextStartShard {
			return index
		}
	}
	return 0
}

func aliasedIdentity(primary, alias, label string) (string, error) {
	primary = strings.TrimSpace(primary)
	alias = strings.TrimSpace(alias)
	if primary != "" && alias != "" && primary != alias {
		return "", fmt.Errorf("%w: conflicting %s", ErrInvalidConfig, label)
	}
	if primary != "" {
		return primary, nil
	}
	return alias, nil
}

// RunOnce discovers and processes at most ClaimLimit DueRefs across OwnedShards
// only. Discovery is serial (one Redis ListDue pipeline at a time); item work
// is bounded by MaxConcurrent. RoundTimeBudget caps the round so one tick
// cannot create unbounded goroutines or Redis fan-out. A failed item remains
// claimed until the authority lease expires, so a later round can retry it
// with a fresh fence.
func (w *Worker) RunOnce(ctx context.Context) (RoundStats, error) {
	if w == nil {
		return RoundStats{}, fmt.Errorf("%w: incomplete worker", ErrInvalidConfig)
	}
	w.runMu.Lock()
	defer w.runMu.Unlock()
	if w.config.schedule == nil || w.config.farmReader == nil || w.config.shardAssignment == nil || w.config.claimAdmission == nil {
		return RoundStats{}, fmt.Errorf("%w: incomplete worker", ErrInvalidConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = redisworkload.WithComponent(ctx, redisworkload.ComponentOpportunity)
	if err := ctx.Err(); err != nil {
		return RoundStats{}, err
	}
	nowMs := int64(0)
	if w.config.trustedNowMs != nil {
		var err error
		nowMs, err = w.config.trustedNowMs(ctx)
		if err != nil {
			return RoundStats{}, fmt.Errorf("read trusted Redis time: %w", err)
		}
	} else {
		now := w.config.now()
		if !now.IsZero() {
			nowMs = now.UnixMilli()
		}
	}
	if nowMs <= 0 {
		return RoundStats{}, fmt.Errorf("%w: trusted now is invalid", ErrInvalidConfig)
	}

	assignment, err := w.config.shardAssignment.Assignment(ctx)
	if err != nil {
		return RoundStats{}, err
	}
	ownedShards, err := normalizeAndValidateOwnedShards(assignment.OwnedShards, w.config.bucketByShard, true)
	if err != nil {
		return RoundStats{}, err
	}
	if assignment.Revision == 0 {
		assignment.Revision = 1
	}

	roundCtx, cancel := context.WithTimeout(ctx, w.config.roundTimeBudget)
	defer cancel()

	stats := RoundStats{Shadow: w.config.shadow, AssignmentRevision: assignment.Revision, OwnedShards: uint64(len(ownedShards))}
	if len(ownedShards) == 0 {
		return stats, nil
	}
	remaining := w.config.claimLimit
	startIndex := w.nextStartIndex(ownedShards)
	lastScannedIndex := -1
	var (
		errs []error
		mu   sync.Mutex
		wg   sync.WaitGroup
	)
	sem := make(chan struct{}, w.config.maxConcurrent)
	recordErr := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}

	for offset := 0; offset < len(ownedShards); offset++ {
		if remaining == 0 {
			break
		}
		if err := roundCtx.Err(); err != nil {
			mu.Lock()
			stats.BudgetExhausted++
			mu.Unlock()
			break
		}
		shardIndex := (startIndex + offset) % len(ownedShards)
		lastScannedIndex = shardIndex
		shard := ownedShards[shardIndex]
		bucket, ok := w.config.bucketByShard[shard]
		if !ok {
			return RoundStats{}, fmt.Errorf("%w: owned shard %d missing configured bucket", ErrInvalidConfig, shard)
		}
		mu.Lock()
		stats.Buckets++
		mu.Unlock()
		limit := w.config.discoveryLimit
		if limit > remaining {
			limit = remaining
		}
		dueRefs, listErr := w.config.schedule.ListDueScheduledWork(roundCtx, bucket, nowMs, limit)
		if listErr != nil {
			mu.Lock()
			stats.Failed++
			if errors.Is(listErr, context.DeadlineExceeded) || errors.Is(listErr, context.Canceled) || roundCtx.Err() != nil {
				stats.BudgetExhausted++
			}
			mu.Unlock()
			recordErr(fmt.Errorf("list opportunity schedules in bucket %d: %w", bucket.VirtualShard(), listErr))
			if errors.Is(listErr, context.DeadlineExceeded) || errors.Is(listErr, context.Canceled) || roundCtx.Err() != nil {
				break
			}
			continue
		}
		if len(dueRefs) == 0 {
			mu.Lock()
			stats.EmptyShards++
			mu.Unlock()
			continue
		}
		mu.Lock()
		stats.Scanned += uint64(len(dueRefs))
		stats.DueBacklog += uint64(len(dueRefs))
		for _, due := range dueRefs {
			if due.DueAtMs > 0 && due.DueAtMs <= nowMs {
				lag := nowMs - due.DueAtMs
				if lag > stats.MaxDueLagMs {
					stats.MaxDueLagMs = lag
				}
			}
		}
		mu.Unlock()
		for _, due := range dueRefs {
			if remaining == 0 {
				break
			}
			if err := roundCtx.Err(); err != nil {
				mu.Lock()
				stats.BudgetExhausted++
				mu.Unlock()
				remaining = 0
				break
			}
			remaining--
			mu.Lock()
			stats.Candidates++
			mu.Unlock()
			if due.ScheduleType != WorkType || due.ScheduleID == "" || due.DueAtMs <= 0 || due.DueAtMs > nowMs {
				mu.Lock()
				stats.Skipped++
				mu.Unlock()
				continue
			}
			wg.Add(1)
			go func(due authority.ScheduledDueRef, bucket rediskey.AuthorityBucketRef, shard uint32, assignmentRevision uint64) {
				defer wg.Done()
				select {
				case sem <- struct{}{}:
				case <-roundCtx.Done():
					mu.Lock()
					stats.BudgetExhausted++
					stats.Skipped++
					mu.Unlock()
					return
				}
				defer func() { <-sem }()
				itemStats, itemErr := w.processDue(roundCtx, bucket, due, nowMs, shard, assignmentRevision)
				mu.Lock()
				mergeRoundStats(&stats, itemStats)
				if itemErr != nil {
					errs = append(errs, itemErr)
				}
				mu.Unlock()
			}(due, bucket, shard, assignment.Revision)
		}
	}
	wg.Wait()
	if lastScannedIndex >= 0 {
		w.nextStartShard = ownedShards[(lastScannedIndex+1)%len(ownedShards)]
		w.cursorSet = true
	}
	mu.Lock()
	if roundCtx.Err() != nil && stats.BudgetExhausted == 0 {
		stats.BudgetExhausted++
	}
	outStats, outErrs := stats, errs
	mu.Unlock()
	return outStats, errors.Join(outErrs...)
}

func (w *Worker) processDue(ctx context.Context, bucket rediskey.AuthorityBucketRef, due authority.ScheduledDueRef, nowMs int64, shard uint32, assignmentRevision uint64) (RoundStats, error) {
	stats := RoundStats{Shadow: w.config.shadow}
	fail := func(err error) (RoundStats, error) {
		stats.Failed++
		return stats, err
	}

	if err := w.config.claimAdmission.CanClaimShard(ctx, shard, assignmentRevision); err != nil {
		stats.ClaimRejected++
		stats.Skipped++
		return stats, nil
	}
	claim, err := w.config.schedule.ClaimScheduledWorkWithBoot(ctx, bucket, due.ScheduleType, due.ScheduleID,
		w.config.holderID, w.config.bootID, w.config.leaseToken, w.config.leaseTTL.Milliseconds())
	if err != nil {
		return fail(fmt.Errorf("claim opportunity schedule %s: %w", due.ScheduleID, err))
	}
	switch claim.Status {
	case authority.WorkClaimClaimed:
		stats.Claimed++
	case authority.WorkClaimLeaseHeld:
		stats.LeaseHeld++
		return stats, nil
	case authority.WorkClaimNotDue, authority.WorkClaimNotFound, authority.WorkClaimNotClaimable:
		stats.Skipped++
		return stats, nil
	default:
		return fail(fmt.Errorf("claim opportunity schedule %s returned status %s", due.ScheduleID, claim.Status.String()))
	}

	snapshot, err := w.config.schedule.GetScheduledWork(ctx, bucket, due.ScheduleType, due.ScheduleID)
	if err != nil {
		if errors.Is(err, authority.ErrSourceOrderNotFound) {
			stats.Skipped++
			return stats, nil
		}
		return fail(fmt.Errorf("read claimed opportunity schedule %s: %w", due.ScheduleID, err))
	}
	if snapshot.Generation > claim.Generation {
		stats.StaleGeneration++
		return stats, nil
	}
	if err := validateClaimSnapshot(snapshot, claim, due, w.config.holderID, w.config.bootID); err != nil {
		if errors.Is(err, ErrStaleGeneration) {
			stats.StaleGeneration++
			return stats, nil
		}
		if errors.Is(err, ErrLeaseLost) {
			stats.LeaseLost++
			return fail(err)
		}
		return fail(err)
	}
	payload, err := opportunitywakeup.Unmarshal(snapshot.Payload)
	if err != nil {
		return fail(fmt.Errorf("decode opportunity schedule %s: %w", due.ScheduleID, errors.Join(ErrInvalidPayload, err)))
	}
	if err := validatePayload(payload, snapshot, claim); err != nil {
		return fail(err)
	}
	if payload.DueAtMs > nowMs {
		return fail(fmt.Errorf("schedule %s: %w", due.ScheduleID, ErrNotDue))
	}

	farm, err := w.config.farmReader.ReadFarmOpportunity(ctx, payload.FarmID)
	if err != nil {
		return fail(fmt.Errorf("read Farm Header/Facts for farm %d: %w", payload.FarmID, err))
	}
	if err := validateFarmOpportunity(farm, payload.FarmID); err != nil {
		return fail(err)
	}
	currentGeneration := farm.Facts.SourceFarmVersion
	if currentGeneration > payload.ScheduleGeneration {
		return w.completeStale(ctx, bucket, snapshot, claim, stats)
	}
	if currentGeneration < payload.ScheduleGeneration {
		return fail(fmt.Errorf("farm %d: %w (current=%d payload=%d)", payload.FarmID, ErrInvalidSchedule, currentGeneration, payload.ScheduleGeneration))
	}
	if err := validatePayloadBoundary(payload, farm.Facts); err != nil {
		return fail(err)
	}

	global, err := domain.EvaluateGlobalFarmOpportunityFacts(farm.Facts, nowMs)
	if err != nil {
		return fail(fmt.Errorf("evaluate global opportunity for farm %d: %w", payload.FarmID, err))
	}
	// P0 H5 exposes only WATER and THEFT opportunity navigation. PLANT has no
	// immediate Hint and a PEST-only boundary must not wake every online friend
	// into an exact pull for an action the client cannot perform.
	const publicHintMask = domain.OpportunityBoundaryStealable | domain.OpportunityBoundaryWater
	if payload.BoundaryMask&publicHintMask != 0 && global.Mask&publicHintMask != 0 && !w.config.shadow {
		ownerID := farm.OwnerID
		if ownerID == 0 {
			ownerID = payload.FarmID
		}
		opportunityRevision, revisionErr := domain.FarmOpportunityRevisionAt(farm.Facts, nowMs)
		if revisionErr != nil {
			return fail(fmt.Errorf("compute opportunity revision for farm %d: %w", payload.FarmID, revisionErr))
		}
		if err := w.config.publisher.PublishTargetedHint(ctx, ownerID, payload.FarmID, farm.Facts.SourceFarmVersion, opportunityRevision); err != nil {
			return fail(fmt.Errorf("publish opportunity hint for farm %d: %w", payload.FarmID, err))
		}
		stats.Published++
	}

	next, found, err := domain.NextFarmOpportunityBoundary(farm.Facts, nowMs)
	if err != nil {
		return fail(fmt.Errorf("compute next opportunity boundary for farm %d: %w", payload.FarmID, err))
	}
	if !found {
		return w.complete(ctx, bucket, snapshot, claim, stats)
	}
	nextPayload := opportunitywakeup.Payload{
		FarmID: payload.FarmID, ScheduleGeneration: farm.Facts.SourceFarmVersion,
		SourceFarmVersion: farm.Facts.SourceFarmVersion, DueAtMs: next.AtMs,
		BoundaryMask: next.Mask, BoundaryDigest: next.Digest,
	}
	encoded, err := opportunitywakeup.Marshal(nextPayload)
	if err != nil {
		return fail(fmt.Errorf("encode next opportunity schedule for farm %d: %w", payload.FarmID, err))
	}
	hash := sha256.Sum256(encoded)
	advanced, err := w.config.schedule.AdvanceScheduledWork(ctx, bucket, snapshot.ScheduleType, snapshot.ScheduleID,
		w.config.holderID, w.config.bootID, claim.LeaseEpoch, claim.WorkVersion, claim.Generation,
		w.config.leaseToken, next.AtMs, encoded, hex.EncodeToString(hash[:]))
	if err != nil {
		return fail(fmt.Errorf("advance opportunity schedule %s: %w", snapshot.ScheduleID, err))
	}
	switch advanced.Status {
	case authority.WorkLeaseRenewed:
		stats.Advanced++
		return stats, nil
	case authority.WorkLeaseStaleGeneration:
		stats.StaleGeneration++
		return stats, nil
	case authority.WorkLeaseFenceMismatch, authority.WorkLeaseExpired, authority.WorkLeaseStaleVersion:
		stats.LeaseLost++
		return fail(fmt.Errorf("advance opportunity schedule %s: %w (status=%s)", snapshot.ScheduleID, ErrLeaseLost, advanced.Status.String()))
	case authority.WorkLeaseNotFound:
		stats.Skipped++
		return stats, nil
	default:
		return fail(fmt.Errorf("advance opportunity schedule %s returned status %s", snapshot.ScheduleID, advanced.Status.String()))
	}
}

func validateClaimSnapshot(snapshot authority.ScheduledWorkSnapshot, claim authority.WorkClaim, due authority.ScheduledDueRef, holderID, bootID string) error {
	if snapshot.ScheduleType != due.ScheduleType || snapshot.ScheduleID != due.ScheduleID {
		return fmt.Errorf("%w: schedule identity changed", ErrInvalidSchedule)
	}
	if snapshot.Generation > claim.Generation {
		return ErrStaleGeneration
	}
	if snapshot.Generation != claim.Generation || snapshot.WorkVersion != claim.WorkVersion ||
		snapshot.LeaseEpoch != claim.LeaseEpoch || (snapshot.State != "SCHEDULE_DISPATCHING" && snapshot.State != "CLAIMED") ||
		snapshot.LeaseHolderID != holderID || snapshot.LeaseHolderBootID != bootID {
		return fmt.Errorf("%w: schedule %s claim fence changed", ErrLeaseLost, due.ScheduleID)
	}
	if !bytes.Equal(snapshot.Payload, claim.Payload) ||
		(snapshot.PayloadHash != "" && claim.PayloadHash != "" && snapshot.PayloadHash != claim.PayloadHash) {
		return fmt.Errorf("%w: schedule %s payload changed after claim", ErrLeaseLost, due.ScheduleID)
	}
	return nil
}

func validatePayload(payload opportunitywakeup.Payload, snapshot authority.ScheduledWorkSnapshot, claim authority.WorkClaim) error {
	if err := payload.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
	if payload.ScheduleGeneration != snapshot.Generation || payload.SourceFarmVersion != snapshot.Generation ||
		payload.DueAtMs != snapshot.NextDueAtMs {
		return fmt.Errorf("%w: payload generation/due does not match schedule %s", ErrInvalidPayload, snapshot.ScheduleID)
	}
	if snapshot.SubjectPlayerID != 0 && payload.FarmID != snapshot.SubjectPlayerID {
		return fmt.Errorf("%w: payload FarmID %d does not match schedule subject %d", ErrInvalidPayload, payload.FarmID, snapshot.SubjectPlayerID)
	}
	wantHash := sha256.Sum256(snapshot.Payload)
	wantHashText := hex.EncodeToString(wantHash[:])
	if snapshot.PayloadHash != "" && snapshot.PayloadHash != wantHashText {
		return fmt.Errorf("%w: schedule %s stored payload hash mismatch", ErrInvalidPayload, snapshot.ScheduleID)
	}
	if claim.PayloadHash != "" && claim.PayloadHash != wantHashText {
		return fmt.Errorf("%w: schedule %s claimed payload hash mismatch", ErrInvalidPayload, snapshot.ScheduleID)
	}
	return nil
}

func validateFarmOpportunity(farm FarmOpportunity, farmID uint64) error {
	if farmID == 0 || (farm.FarmID != 0 && farm.FarmID != farmID) {
		return fmt.Errorf("%w: FarmID mismatch (payload=%d read=%d)", ErrInvalidSchedule, farmID, farm.FarmID)
	}
	if farm.Header.SchemaVersion != domain.FarmOpportunityHeaderSchemaV2 || farm.Facts.SchemaVersion != domain.FarmOpportunityFactsSchemaV1 ||
		farm.Header.SourceFarmVersion == 0 || farm.Facts.SourceFarmVersion == 0 ||
		farm.Header.SourceFarmVersion != farm.Facts.SourceFarmVersion {
		return fmt.Errorf("%w: Farm Header/Facts version pair is invalid for farm %d", ErrInvalidSchedule, farmID)
	}
	return nil
}

func validatePayloadBoundary(payload opportunitywakeup.Payload, facts domain.FarmOpportunityFactsV1) error {
	cursor := payload.DueAtMs - 1
	if cursor < 0 {
		cursor = 0
	}
	boundary, found, err := domain.NextFarmOpportunityBoundary(facts, cursor)
	if err != nil {
		return fmt.Errorf("%w: validate payload boundary: %v", ErrInvalidPayload, err)
	}
	if !found || boundary.AtMs != payload.DueAtMs || boundary.Mask != payload.BoundaryMask || boundary.Digest != payload.BoundaryDigest {
		return fmt.Errorf("%w: payload boundary does not belong to current Farm Facts", ErrInvalidPayload)
	}
	return nil
}

func (w *Worker) completeStale(ctx context.Context, bucket rediskey.AuthorityBucketRef, snapshot authority.ScheduledWorkSnapshot, claim authority.WorkClaim, stats RoundStats) (RoundStats, error) {
	before := stats.StaleGeneration
	completed, err := w.complete(ctx, bucket, snapshot, claim, stats)
	if completed.StaleGeneration == before {
		completed.StaleGeneration++
	}
	return completed, err
}

func (w *Worker) complete(ctx context.Context, bucket rediskey.AuthorityBucketRef, snapshot authority.ScheduledWorkSnapshot, claim authority.WorkClaim, stats RoundStats) (RoundStats, error) {
	completed, err := w.config.schedule.CompleteScheduledWork(ctx, bucket, snapshot.ScheduleType, snapshot.ScheduleID,
		w.config.holderID, w.config.bootID, claim.LeaseEpoch, claim.WorkVersion, claim.Generation, w.config.leaseToken)
	if err != nil {
		stats.Failed++
		return stats, fmt.Errorf("complete opportunity schedule %s: %w", snapshot.ScheduleID, err)
	}
	switch completed.Status {
	case authority.WorkLeaseCompleted, authority.WorkLeaseAlreadyCompleted:
		stats.Completed++
		return stats, nil
	case authority.WorkLeaseStaleGeneration:
		stats.StaleGeneration++
		return stats, nil
	case authority.WorkLeaseFenceMismatch, authority.WorkLeaseExpired, authority.WorkLeaseStaleVersion:
		stats.LeaseLost++
		stats.Failed++
		return stats, fmt.Errorf("complete opportunity schedule %s: %w (status=%s)", snapshot.ScheduleID, ErrLeaseLost, completed.Status.String())
	case authority.WorkLeaseNotFound:
		stats.Skipped++
		return stats, nil
	default:
		stats.Failed++
		return stats, fmt.Errorf("complete opportunity schedule %s returned status %s", snapshot.ScheduleID, completed.Status.String())
	}
}

func mergeRoundStats(dst *RoundStats, src RoundStats) {
	if dst == nil {
		return
	}
	dst.Scanned += src.Scanned
	dst.Candidates += src.Candidates
	dst.Claimed += src.Claimed
	dst.LeaseHeld += src.LeaseHeld
	dst.Skipped += src.Skipped
	dst.Published += src.Published
	dst.Advanced += src.Advanced
	dst.Completed += src.Completed
	dst.StaleGeneration += src.StaleGeneration
	dst.LeaseLost += src.LeaseLost
	dst.Failed += src.Failed
	dst.DueBacklog += src.DueBacklog
	dst.EmptyShards += src.EmptyShards
	dst.BudgetExhausted += src.BudgetExhausted
	dst.ClaimRejected += src.ClaimRejected
	if src.MaxDueLagMs > dst.MaxDueLagMs {
		dst.MaxDueLagMs = src.MaxDueLagMs
	}
}
