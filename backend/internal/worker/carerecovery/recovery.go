// Package carerecovery owns the source-attached CareRewardOrder fast path and
// recovery path.  It is deliberately a composition package: Farm/Player
// adjudication remains in their existing actors and the package only builds
// the frozen APPLY_CARE_REWARD Participant call and advances the existing
// authority WorkLease.
package carerecovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	"farmgame/internal/storage/authoritystore"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/worker/sourceorder"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	// FamilyCareReward is the registry-frozen SourceOrder work type.
	FamilyCareReward = string(identity.WorkTypeCareReward)

	defaultClaimLimit       = 32
	defaultMaxConcurrent    = 4
	defaultOperationTimeout = 3 * time.Second
	defaultLeaseTTL         = 10 * time.Second
	defaultParticipantMs    = contracts.ParticipantMaxBudgetMs
)

var (
	ErrInvalidConfig             = errors.New("care recovery: invalid configuration")
	ErrLeaseLost                 = errors.New("care recovery: work lease lost")
	ErrTerminalProof             = errors.New("care recovery: Participant terminal proof is invalid")
	ErrRetryDeferred             = errors.New("care recovery: durable retry deferred")
	ErrPayloadInvalid            = errors.New("care recovery: CareRewardOrder payload is invalid")
	ErrDirectQueueFull           = errors.New("care recovery: direct dispatcher queue is full")
	ErrDirectQueueBytes          = errors.New("care recovery: direct dispatcher byte capacity reached")
	ErrDirectClosed              = errors.New("care recovery: direct dispatcher is closed")
	ErrDirectNotClaimed          = errors.New("care recovery: direct result delivered; completion left for worker")
	ErrBestEffortEnvelopeMissing = errors.New("care recovery: dirty best-effort dispatch requires a committed in-memory envelope")
	ErrUnsupportedWorkType       = errors.New("care recovery: unsupported SourceOrder work type")
	ErrDueIndexFamilyUnavailable = errors.New("care recovery: family DueIndex view is unavailable")
)

// WorkLease is the existing authority work-lease surface.  In particular,
// implementations must be backed by ClaimWork/RenewWorkLease/CompleteWork;
// this package has no alternate lease or Redis schema.
type WorkLease interface {
	ClaimWorkWithBoot(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, string, int64) (authority.WorkClaim, error)
	RenewWorkLeaseWithBoot(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, uint64, string, int64) (authority.WorkLeaseRenewResult, error)
	CompleteWorkWithProof(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, uint64, uint64, string, authority.WorkCompletionProof) (authority.WorkCompleteResult, error)
	ClaimAndCompleteWithProof(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, string, int64, authority.WorkCompletionProof) (authority.WorkCompleteResult, error)
	RescheduleWork(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, uint64, uint64, string, uint64, int64, string) (authority.WorkRescheduleResult, error)
}

type batchWorkLease interface {
	ClaimAndCompleteWithProofBatch(context.Context, []authority.WorkClaimAndCompleteRequest) []authority.WorkClaimAndCompleteBatchResult
}

// DueIndexScanner reads the existing FARM authority bucket Source DueIndex.
type DueIndexScanner interface {
	ListDueWork(context.Context, rediskey.AuthorityBucketRef, int) ([]authority.DueWorkRef, error)
}

// DueIndexPageScanner is the proof-carrying SourceOrder discovery surface used
// by the shared family cache. Complete and NextCursor must retain the exact
// WorkLease read semantics; implementations without this metadata are never
// eligible for shared-cache composition.
type DueIndexPageScanner interface {
	ListDueWorkPage(context.Context, rediskey.AuthorityBucketRef, string, authority.DueWorkCursor, int) (authority.DueWorkPage, error)
}

type careDueIndexScanner interface {
	ListDueCareWork(context.Context, rediskey.AuthorityBucketRef, int) ([]authority.DueWorkRef, error)
}

type trustedNowSource interface {
	TrustedNowMs(context.Context) (int64, error)
}

// DueIndexObservation is available only when the complete shared authority
// DueIndex view fits inside a bounded read. Available=false is never a zero
// backlog claim.
type DueIndexObservation struct {
	Depth         int64
	OldestDueAtMs int64
	Available     bool
}

type DueIndexObserver interface {
	ObserveDue(context.Context, int) (DueIndexObservation, error)
}

// BucketDueIndexObserver allows readiness observation to follow the same
// dynamic shard ownership as recovery. Implementations must inspect only the
// supplied buckets; this prevents every farm-worker replica from observing the
// complete DueIndex after a ScalePlan rebalance.
type BucketDueIndexObserver interface {
	ObserveBuckets(context.Context, []rediskey.AuthorityBucketRef, int) (DueIndexObservation, error)
}

// ScanLeaseStore is optional.  When present, a worker uses the existing
// bucket scan lease to avoid duplicate discovery; WorkLease still fences every
// individual order, so scan-lease availability is never a correctness gate.
type ScanLeaseStore interface {
	ClaimBucketScanLease(context.Context, rediskey.AuthorityBucketRef, string, int64) (authority.ScanLeaseResult, error)
}

// ShardAssignment is one immutable process-local recovery ownership view.
// Revision fences a scan round against a concurrent ScalePlan rebalance.
// An empty OwnedShards slice is valid while this worker owns no recovery
// partitions.
type ShardAssignment struct {
	Revision    uint64
	OwnedShards []uint32
}

// ShardAssignmentProvider must be a local-memory read on the polling path.
// RouteControl watches may update its immutable snapshot in the background,
// but RunOnce must never query Redis merely to discover ownership.
type ShardAssignmentProvider interface {
	Assignment(context.Context) (ShardAssignment, error)
}

// ShardClaimAdmission rechecks ownership immediately before a new WorkLease
// claim. Work already claimed under the previous assignment may finish; new
// claims from a stale revision fail closed and remain discoverable in DueIndex.
type ShardClaimAdmission interface {
	CanClaimShard(context.Context, uint32, uint64) error
}

// SourceOrderReader is used only by DirectDispatcher.  It is a read-only view
// of SourceOrder and must not claim the order before the fast-path RPC.
type SourceOrderReader interface {
	GetSourceOrder(context.Context, rediskey.AuthorityBucketRef, string, string) (authority.SourceOrderSnapshot, error)
}

type WorkerRecoveryParticipantClient interface {
	ResumeStep(context.Context, *contractspb.ParticipantRequest, ...grpc.CallOption) (*contractspb.ParticipantResponse, error)
}

type CoordinatorParticipantClient interface {
	ExecuteStep(context.Context, *contractspb.ParticipantRequest, ...grpc.CallOption) (*contractspb.ParticipantResponse, error)
}

// RouteProvider returns a current, physically fenced PLAYER route header.
// Route resolution stays outside the order runner and is re-run for every
// attempt so a holder move is not hidden by a stale cached endpoint.
type RouteProvider func(context.Context, uint64) (*logicrpcpb.ActorRouteHeader, error)

// Config composes the durable source-order recovery loop.  AuthorityStore is
// a convenience for production composition; the explicit interfaces keep the
// unit tests independent of Redis and of CR1/CR2 business implementations.
type Config struct {
	AuthorityStore *authority.Store
	WorkLease      WorkLease
	DueIndex       DueIndexScanner
	SourceOrders   SourceOrderReader
	ScanLeases     ScanLeaseStore
	DueObserver    DueIndexObserver

	// Buckets is the known FARM authority bucket set.  It must be a bounded
	// list derived from the same route/hash snapshot as the authority store.
	Buckets         []rediskey.AuthorityBucketRef
	BucketForFarm   func(uint64) (rediskey.AuthorityBucketRef, error)
	BucketForPlayer func(uint64) (rediskey.AuthorityBucketRef, error)
	ShardAssignment ShardAssignmentProvider
	ClaimAdmission  ShardClaimAdmission
	// BucketsPerRound bounds discovery fan-out. Successive rounds rotate over
	// the currently owned shards instead of spawning one goroutine per shard.
	BucketsPerRound        int
	DueObservationInterval time.Duration
	// RecoveryGrace reserves a bounded window for the source holder's Direct
	// path before crash recovery may claim a newly persisted, first-attempt
	// order. Redis TIME and immutable created_at_ms bound the window; retries
	// and expired-lease takeovers are never grace-delayed.
	RecoveryGrace time.Duration

	RouteResolver actorrouting.Resolver
	Realm         string
	Route         RouteProvider
	Player        WorkerRecoveryParticipantClient
	PlayerClients map[string]WorkerRecoveryParticipantClient
	Farm          WorkerRecoveryParticipantClient
	FarmClients   map[string]WorkerRecoveryParticipantClient
	FarmRoute     RouteProvider

	Owner  string
	Token  string
	BootID string

	ClaimLimit          int
	MaxConcurrent       int
	LeaseTTL            time.Duration
	SafetyMargin        time.Duration
	OperationTimeout    time.Duration
	ParticipantBudgetMs uint32
	Backoff             workflow.BackoffConfig
	ScanLeaseTTL        time.Duration
	UseScanLease        bool
	Now                 func() time.Time
	// Family selects which SourceOrder work type this worker claims.
	// Empty keeps the historical CARE_REWARD-only Care worker.
	Family string
}

// Composition is the farm-worker Care slice.  Direct is intentionally
// optional: farm-svr normally composes it, while farm-worker only needs the
// durable recovery Worker.
type Composition struct {
	Worker *Worker
	Direct *DirectDispatcher
}

// RoundStats describes one bounded FARM-bucket recovery round.
type RoundStats struct {
	OwnedBuckets uint64
	Revision     uint64
	Buckets      uint64
	Scanned      uint64
	Candidates   uint64
	Claimed      uint64
	Completed    uint64
	Rescheduled  uint64
	BadPayload   uint64
	Unknown      uint64
	Failed       uint64
	LeaseRenewed uint64
}

// New builds the Care worker composition.  A missing Player Recovery route is
// fail-closed; a missing optional scan lease merely falls back to per-order
// ClaimWork fencing.
func New(config Config) (*Composition, error) {
	worker, _, err := newWorker(config)
	if err != nil {
		return nil, err
	}
	return &Composition{Worker: worker}, nil
}

// NewWorker is the explicit constructor used by farm-worker composition roots.
func NewWorker(config Config) (*Worker, error) {
	worker, _, err := newWorker(config)
	return worker, err
}

// NewPlayerRouteProvider exposes the same current-assignment route proof used
// by the recovery worker to the optional in-process Direct dispatcher.
func NewPlayerRouteProvider(resolver actorrouting.Resolver, realm string) (RouteProvider, error) {
	if resolver == nil || strings.TrimSpace(realm) == "" {
		return nil, fmt.Errorf("%w: PLAYER route resolver and realm are required", ErrInvalidConfig)
	}
	return recoveryRouteProvider(resolver, realm), nil
}

func (c *Composition) RunOnce(ctx context.Context) (RoundStats, error) {
	if c == nil || c.Worker == nil {
		return RoundStats{}, fmt.Errorf("%w: nil composition", ErrInvalidConfig)
	}
	return c.Worker.RunOnce(ctx)
}

func (c *Composition) PendingDue(ctx context.Context) (int64, error) {
	if c == nil || c.Worker == nil {
		return 0, fmt.Errorf("%w: nil composition", ErrInvalidConfig)
	}
	return c.Worker.PendingDue(ctx)
}

type normalizedConfig struct {
	WorkLease              WorkLease
	DueIndex               DueIndexScanner
	SourceOrders           SourceOrderReader
	ScanLeases             ScanLeaseStore
	DueObserver            DueIndexObserver
	Buckets                []rediskey.AuthorityBucketRef
	BucketByShard          map[uint32]rediskey.AuthorityBucketRef
	BucketForFarm          func(uint64) (rediskey.AuthorityBucketRef, error)
	BucketForPlayer        func(uint64) (rediskey.AuthorityBucketRef, error)
	ShardAssignment        ShardAssignmentProvider
	ClaimAdmission         ShardClaimAdmission
	BucketsPerRound        int
	DueObservationInterval time.Duration
	RecoveryGrace          time.Duration
	RecoveryNowMs          func(context.Context) (int64, error)
	Route                  RouteProvider
	Player                 WorkerRecoveryParticipantClient
	Farm                   WorkerRecoveryParticipantClient
	FarmRoute              RouteProvider
	Owner                  string
	Token                  string
	BootID                 string
	ClaimLimit             int
	MaxConcurrent          int
	LeaseTTL               time.Duration
	SafetyMargin           time.Duration
	OperationTimeout       time.Duration
	ParticipantBudgetMs    uint32
	Backoff                workflow.BackoffConfig
	ScanLeaseTTL           time.Duration
	UseScanLease           bool
	Now                    func() time.Time
	Family                 string
}

func newWorker(config Config) (*Worker, normalizedConfig, error) {
	dueFamily := strings.TrimSpace(config.Family)
	if dueFamily == "" {
		dueFamily = FamilyCareReward
	}
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
		if config.ScanLeases == nil {
			config.ScanLeases = config.AuthorityStore.WorkLease()
		}
		if config.DueObserver == nil {
			config.DueObserver = &redisCareDueObserver{client: config.AuthorityStore.Client(), keys: config.AuthorityStore.Keys(), buckets: config.Buckets, family: dueFamily}
		}
	}
	if config.WorkLease == nil || config.DueIndex == nil || strings.TrimSpace(config.Owner) == "" ||
		strings.TrimSpace(config.Token) == "" || strings.TrimSpace(config.BootID) == "" {
		return nil, normalizedConfig{}, fmt.Errorf("%w: lease, due index, owner, token and boot id are required", ErrInvalidConfig)
	}
	if len(config.Buckets) == 0 {
		return nil, normalizedConfig{}, fmt.Errorf("%w: at least one FARM authority bucket is required", ErrInvalidConfig)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.ClaimLimit <= 0 {
		config.ClaimLimit = defaultClaimLimit
	}
	if config.MaxConcurrent <= 0 {
		config.MaxConcurrent = defaultMaxConcurrent
	}
	if config.BucketsPerRound <= 0 {
		config.BucketsPerRound = 256
	}
	if config.DueObservationInterval <= 0 {
		config.DueObservationInterval = time.Second
	}
	if config.RecoveryGrace < 0 {
		return nil, normalizedConfig{}, fmt.Errorf("%w: negative recovery grace", ErrInvalidConfig)
	}
	var recoveryNowMs func(context.Context) (int64, error)
	if trusted, ok := config.WorkLease.(trustedNowSource); ok {
		recoveryNowMs = trusted.TrustedNowMs
	}
	if config.RecoveryGrace > 0 && (config.SourceOrders == nil || recoveryNowMs == nil) {
		return nil, normalizedConfig{}, fmt.Errorf("%w: recovery grace requires SourceOrder evidence and Redis trusted time", ErrInvalidConfig)
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
	if config.ParticipantBudgetMs == 0 {
		config.ParticipantBudgetMs = defaultParticipantMs
	}
	if config.ScanLeaseTTL <= 0 {
		config.ScanLeaseTTL = config.LeaseTTL
	}
	config.Backoff = config.BackoffWithDefaults()

	wantDomain := actorrouting.DomainFarm
	switch strings.TrimSpace(config.Family) {
	case string(identity.WorkTypePlant), string(identity.WorkTypeUnlockPlot):
		wantDomain = actorrouting.DomainPlayer
	}
	buckets := make([]rediskey.AuthorityBucketRef, 0, len(config.Buckets))
	seenBuckets := make(map[uint32]struct{}, len(config.Buckets))
	bucketByShard := make(map[uint32]rediskey.AuthorityBucketRef, len(config.Buckets))
	for _, bucket := range config.Buckets {
		if err := bucket.Validate(); err != nil || bucket.Domain() != wantDomain {
			return nil, normalizedConfig{}, fmt.Errorf("%w: authority bucket domain: %v", ErrInvalidConfig, err)
		}
		if _, exists := seenBuckets[bucket.VirtualShard()]; exists {
			continue
		}
		seenBuckets[bucket.VirtualShard()] = struct{}{}
		buckets = append(buckets, bucket)
		bucketByShard[bucket.VirtualShard()] = bucket
	}
	if len(buckets) == 0 {
		return nil, normalizedConfig{}, fmt.Errorf("%w: no valid authority buckets", ErrInvalidConfig)
	}

	route := config.Route
	if route == nil {
		if config.RouteResolver == nil || strings.TrimSpace(config.Realm) == "" {
			return nil, normalizedConfig{}, fmt.Errorf("%w: PLAYER route resolver is required", ErrInvalidConfig)
		}
		route = recoveryRouteProvider(config.RouteResolver, config.Realm)
	}
	farmRoute := config.FarmRoute
	if farmRoute == nil && config.RouteResolver != nil && strings.TrimSpace(config.Realm) != "" {
		farmRoute = recoveryFarmRouteProvider(config.RouteResolver, config.Realm)
	}
	player := config.Player
	if player == nil && len(config.PlayerClients) > 0 {
		var err error
		player, err = newRoutedRecoveryClient(config.RouteResolver, config.Realm, config.PlayerClients)
		if err != nil {
			return nil, normalizedConfig{}, err
		}
	}
	farm := config.Farm
	if farm == nil && len(config.FarmClients) > 0 {
		var err error
		farm, err = newRoutedFarmRecoveryClient(config.RouteResolver, config.Realm, config.FarmClients)
		if err != nil {
			return nil, normalizedConfig{}, err
		}
	}
	if player == nil && farm == nil {
		return nil, normalizedConfig{}, fmt.Errorf("%w: PLAYER or FARM Recovery client is required", ErrInvalidConfig)
	}
	normalized := normalizedConfig{
		WorkLease: config.WorkLease, DueIndex: config.DueIndex, SourceOrders: config.SourceOrders,
		ScanLeases: config.ScanLeases, DueObserver: config.DueObserver, Buckets: buckets, BucketByShard: bucketByShard,
		BucketForFarm: config.BucketForFarm, BucketForPlayer: config.BucketForPlayer,
		ShardAssignment: config.ShardAssignment, ClaimAdmission: config.ClaimAdmission,
		BucketsPerRound: config.BucketsPerRound, DueObservationInterval: config.DueObservationInterval,
		RecoveryGrace: config.RecoveryGrace, RecoveryNowMs: recoveryNowMs,
		Route: route, Player: player, Farm: farm, FarmRoute: farmRoute,
		Owner: config.Owner, Token: config.Token, BootID: config.BootID,
		ClaimLimit: config.ClaimLimit, MaxConcurrent: config.MaxConcurrent, LeaseTTL: config.LeaseTTL,
		SafetyMargin: config.SafetyMargin, OperationTimeout: config.OperationTimeout,
		ParticipantBudgetMs: config.ParticipantBudgetMs, Backoff: config.Backoff,
		ScanLeaseTTL: config.ScanLeaseTTL, UseScanLease: config.UseScanLease, Now: config.Now,
		Family: config.Family,
	}
	worker := &Worker{config: normalized, random: rand.New(rand.NewSource(config.Now().UnixNano()))}
	return worker, normalized, nil
}

// BackoffWithDefaults keeps the defaulting operation available to composition
// roots without exposing workflow's private normalization helper.
func (c Config) BackoffWithDefaults() workflow.BackoffConfig {
	backoff := c.Backoff
	if backoff.Base <= 0 {
		backoff.Base = 250 * time.Millisecond
	}
	if backoff.Max <= 0 {
		backoff.Max = 30 * time.Second
	}
	if backoff.Max < backoff.Base {
		backoff.Max = backoff.Base
	}
	if backoff.JitterPPM > 1_000_000 {
		backoff.JitterPPM = 1_000_000
	}
	return backoff
}

// Worker consumes only due CareRewardOrder refs and uses the existing
// authority lease API for every durable transition.
type Worker struct {
	config              normalizedConfig
	random              *rand.Rand
	randomMu            sync.Mutex
	dueObservationMu    sync.Mutex
	dueObservation      DueIndexObservation
	dueObservedAt       time.Time
	dueObservedLimit    int
	dueObservedRevision uint64
	roundCursor         atomic.Uint64
	dueCursorMu         sync.Mutex
	dueCursors          map[uint32]authority.DueWorkCursor
}

type bucketRound struct {
	stats RoundStats
	err   error
}

// RunOnce performs a bounded, bucket-parallel round.  Each order has its own
// deadline and semaphore slot, so one slow Player RPC cannot serialize all
// FARM buckets or create an unbounded goroutine/claim storm.
func (w *Worker) RunOnce(ctx context.Context) (RoundStats, error) {
	if w == nil || w.config.WorkLease == nil || w.config.DueIndex == nil || (w.config.Player == nil && w.config.Farm == nil) {
		return RoundStats{}, fmt.Errorf("%w: incomplete worker", ErrInvalidConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	assignment, buckets, err := w.ownedBuckets(ctx, true)
	if err != nil {
		return RoundStats{}, err
	}
	stats := RoundStats{OwnedBuckets: uint64(len(assignment.OwnedShards)), Revision: assignment.Revision}
	if len(buckets) == 0 {
		return stats, nil
	}
	sem := make(chan struct{}, w.config.MaxConcurrent)
	results := make(chan bucketRound, len(buckets))
	var wg sync.WaitGroup
	for _, bucket := range buckets {
		bucket := bucket
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- w.runBucket(ctx, bucket, assignment.Revision, sem)
		}()
	}
	wg.Wait()
	close(results)
	var errs []error
	for result := range results {
		stats.Buckets += result.stats.Buckets
		stats.Scanned += result.stats.Scanned
		stats.Candidates += result.stats.Candidates
		stats.Claimed += result.stats.Claimed
		stats.Completed += result.stats.Completed
		stats.Rescheduled += result.stats.Rescheduled
		stats.BadPayload += result.stats.BadPayload
		stats.Unknown += result.stats.Unknown
		stats.Failed += result.stats.Failed
		stats.LeaseRenewed += result.stats.LeaseRenewed
		if result.err != nil {
			errs = append(errs, result.err)
		}
	}
	return stats, errors.Join(errs...)
}

func (w *Worker) ownedBuckets(ctx context.Context, rotate bool) (ShardAssignment, []rediskey.AuthorityBucketRef, error) {
	assignment := ShardAssignment{Revision: 1, OwnedShards: make([]uint32, 0, len(w.config.Buckets))}
	if w.config.ShardAssignment != nil {
		var err error
		assignment, err = w.config.ShardAssignment.Assignment(ctx)
		if err != nil {
			return ShardAssignment{}, nil, err
		}
		if assignment.Revision == 0 {
			return ShardAssignment{}, nil, fmt.Errorf("%w: zero shard assignment revision", ErrInvalidConfig)
		}
	} else {
		for _, bucket := range w.config.Buckets {
			assignment.OwnedShards = append(assignment.OwnedShards, bucket.VirtualShard())
		}
	}
	seen := make(map[uint32]struct{}, len(assignment.OwnedShards))
	owned := make([]rediskey.AuthorityBucketRef, 0, len(assignment.OwnedShards))
	for _, shard := range assignment.OwnedShards {
		if _, duplicate := seen[shard]; duplicate {
			return ShardAssignment{}, nil, fmt.Errorf("%w: duplicate owned shard %d", ErrInvalidConfig, shard)
		}
		seen[shard] = struct{}{}
		bucket, exists := w.config.BucketByShard[shard]
		if !exists {
			return ShardAssignment{}, nil, fmt.Errorf("%w: owned shard %d is not configured", ErrInvalidConfig, shard)
		}
		owned = append(owned, bucket)
	}
	if !rotate || len(owned) <= w.config.BucketsPerRound {
		return assignment, owned, nil
	}
	limit := w.config.BucketsPerRound
	start := int((w.roundCursor.Add(uint64(limit)) - uint64(limit)) % uint64(len(owned)))
	selected := make([]rediskey.AuthorityBucketRef, 0, limit)
	for offset := 0; offset < limit; offset++ {
		selected = append(selected, owned[(start+offset)%len(owned)])
	}
	return assignment, selected, nil
}

// PendingDue returns the bounded discovery count across the shards currently
// assigned to this worker. Production monitoring should prefer ObserveDue,
// which adds a bounded cache and an explicit availability result.
func (w *Worker) PendingDue(ctx context.Context) (int64, error) {
	if w == nil || w.config.DueIndex == nil {
		return 0, fmt.Errorf("%w: incomplete worker", ErrInvalidConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_, buckets, err := w.ownedBuckets(ctx, false)
	if err != nil {
		return 0, err
	}
	var total int64
	var errs []error
	for _, bucket := range buckets {
		refs, err := w.listDueCare(ctx, bucket)
		if err != nil {
			errs = append(errs, fmt.Errorf("bucket %d: %w", bucket.VirtualShard(), err))
			continue
		}
		for _, ref := range refs {
			if ref.Ref.WorkType == w.family() {
				total++
			}
		}
	}
	return total, errors.Join(errs...)
}

func (w *Worker) ObserveDue(ctx context.Context, limit int) (DueIndexObservation, error) {
	if w == nil || w.config.DueObserver == nil {
		return DueIndexObservation{}, nil
	}
	now := w.config.Now()
	assignment, buckets, err := w.ownedBuckets(ctx, false)
	if err != nil {
		return DueIndexObservation{}, err
	}
	w.dueObservationMu.Lock()
	defer w.dueObservationMu.Unlock()
	if !w.dueObservedAt.IsZero() && w.dueObservedLimit == limit && w.dueObservedRevision == assignment.Revision &&
		now.Sub(w.dueObservedAt) < w.config.DueObservationInterval {
		return w.dueObservation, nil
	}
	var observation DueIndexObservation
	if len(buckets) == 0 && len(w.config.Buckets) != 0 {
		observation = DueIndexObservation{Available: true}
	} else if bucketObserver, ok := w.config.DueObserver.(BucketDueIndexObserver); ok {
		observation, err = bucketObserver.ObserveBuckets(ctx, buckets, limit)
	} else {
		// Compatibility path for tests and non-Redis observers. Production's
		// Redis observer implements BucketDueIndexObserver.
		observation, err = w.config.DueObserver.ObserveDue(ctx, limit)
	}
	if err != nil {
		return DueIndexObservation{}, err
	}
	w.dueObservation = observation
	w.dueObservedAt = now
	w.dueObservedLimit = limit
	w.dueObservedRevision = assignment.Revision
	return observation, nil
}

type redisCareDueObserver struct {
	client  redis.UniversalClient
	keys    rediskey.AuthorityKeys
	buckets []rediskey.AuthorityBucketRef
	family  string
}

func (o *redisCareDueObserver) ObserveDue(ctx context.Context, limit int) (DueIndexObservation, error) {
	if o == nil {
		return DueIndexObservation{}, fmt.Errorf("%w: Care DueIndex observer", ErrInvalidConfig)
	}
	return o.ObserveBuckets(ctx, o.buckets, limit)
}

func (o *redisCareDueObserver) ObserveBuckets(ctx context.Context, buckets []rediskey.AuthorityBucketRef, limit int) (DueIndexObservation, error) {
	if o == nil || o.client == nil || limit <= 0 || limit > 4096 || len(buckets) == 0 {
		return DueIndexObservation{}, fmt.Errorf("%w: Care DueIndex observer", ErrInvalidConfig)
	}
	now, err := o.client.Time(ctx).Result()
	if err != nil {
		return DueIndexObservation{}, err
	}
	observation := DueIndexObservation{Available: true}
	family := strings.TrimSpace(o.family)
	if family == "" {
		family = FamilyCareReward
	}
	scanned := 0
	for _, bucket := range buckets {
		dueKey, keyErr := o.keys.SourceDueIndex(bucket)
		if keyErr != nil {
			return DueIndexObservation{}, keyErr
		}
		remaining := limit - scanned
		entries, readErr := o.client.ZRangeByScoreWithScores(ctx, dueKey.String(), &redis.ZRangeBy{
			Min: "-inf", Max: strconv.FormatInt(now.UnixMilli(), 10), Count: int64(remaining + 1),
		}).Result()
		if readErr != nil {
			return DueIndexObservation{}, readErr
		}
		if len(entries) > remaining {
			return DueIndexObservation{}, nil
		}
		scanned += len(entries)
		for _, entry := range entries {
			member, ok := entry.Member.(string)
			if !ok {
				return DueIndexObservation{}, nil
			}
			ref, decodeErr := workidentity.DecodeWorkRefV1(member)
			if decodeErr != nil {
				return DueIndexObservation{}, nil
			}
			if ref.WorkType != family {
				continue
			}
			observation.Depth++
			dueAt := int64(entry.Score)
			if observation.OldestDueAtMs == 0 || dueAt < observation.OldestDueAtMs {
				observation.OldestDueAtMs = dueAt
			}
		}
	}
	return observation, nil
}

func (w *Worker) runBucket(ctx context.Context, bucket rediskey.AuthorityBucketRef, revision uint64, sem chan struct{}) bucketRound {
	result := bucketRound{stats: RoundStats{Buckets: 1}}
	if w.config.ClaimAdmission != nil {
		if err := w.config.ClaimAdmission.CanClaimShard(ctx, bucket.VirtualShard(), revision); err != nil {
			result.err = err
			return result
		}
	}
	if w.config.UseScanLease && w.config.ScanLeases != nil {
		scan, err := w.config.ScanLeases.ClaimBucketScanLease(ctx, bucket, w.config.Owner, w.config.ScanLeaseTTL.Milliseconds())
		if err == nil && scan.Status != authority.ScanLeaseClaimed && scan.Status != authority.ScanLeaseRenewed {
			// Another scanner is doing discovery.  Individual WorkLease claims
			// remain the correctness fence, so this is a normal skip.
			return result
		}
		// Scan leases reduce duplicate discovery but are not the correctness
		// boundary.  If this optional optimization is unavailable, continue
		// with bounded DueIndex discovery and let ClaimWork fence each order.
	}
	refs, err := w.listDueCare(ctx, bucket)
	if err != nil {
		result.err = fmt.Errorf("bucket %d due index: %w", bucket.VirtualShard(), err)
		return result
	}
	result.stats.Scanned = uint64(len(refs))
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, due := range refs {
		if due.Ref.WorkType != w.family() {
			continue
		}
		if w.config.RecoveryGrace > 0 {
			deferred, graceErr := w.deferFreshFirstAttempt(ctx, bucket, due)
			if graceErr != nil {
				if result.err == nil {
					result.err = graceErr
				} else {
					result.err = errors.Join(result.err, graceErr)
				}
				continue
			}
			if deferred {
				continue
			}
		}
		mu.Lock()
		result.stats.Candidates++
		mu.Unlock()
		if ctx.Err() != nil {
			break
		}
		// The goroutine is bounded by sem; the candidate list itself is bounded
		// by ClaimLimit and never grows with backlog in one round.
		wg.Add(1)
		go func(due authority.DueWorkRef) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			item := w.processDue(ctx, bucket, due, revision)
			mu.Lock()
			result.stats.Claimed += item.Claimed
			result.stats.Completed += item.Completed
			result.stats.Rescheduled += item.Rescheduled
			result.stats.BadPayload += item.BadPayload
			result.stats.Unknown += item.Unknown
			result.stats.Failed += item.Failed
			result.stats.LeaseRenewed += item.LeaseRenewed
			if item.Err != nil {
				if result.err == nil {
					result.err = item.Err
				} else {
					result.err = errors.Join(result.err, item.Err)
				}
			}
			mu.Unlock()
		}(due)
	}
	wg.Wait()
	return result
}

// RecoveryEvidenceReader is implemented by the Redis WorkLeaseStore. The
// grace check prefers this single-EVALSHA evidence and falls back to the
// legacy GetSourceOrder + separate Redis TIME for stores without it.
type RecoveryEvidenceReader interface {
	VerifySourceOrderRecoveryEvidence(context.Context, rediskey.AuthorityBucketRef, string, string) (authority.SourceOrderRecoveryEvidence, error)
}

func (w *Worker) deferFreshFirstAttempt(ctx context.Context, bucket rediskey.AuthorityBucketRef, due authority.DueWorkRef) (bool, error) {
	if reader, ok := w.config.SourceOrders.(RecoveryEvidenceReader); ok {
		readCtx, cancel := context.WithTimeout(ctx, minDuration(w.config.OperationTimeout, time.Second))
		order, err := reader.VerifySourceOrderRecoveryEvidence(readCtx, bucket, w.family(), due.Ref.WorkID)
		cancel()
		if err != nil {
			return false, fmt.Errorf("recovery grace read %s: %w", due.Ref.WorkID, err)
		}
		if !order.Found {
			return false, fmt.Errorf("%w: recovery grace SourceOrder missing", ErrPayloadInvalid)
		}
		if order.WorkID != due.Ref.WorkID || order.WorkType != w.family() {
			return false, fmt.Errorf("%w: recovery grace SourceOrder identity", ErrPayloadInvalid)
		}
		// Any durable retry or lease evidence means Direct already had its
		// only grace window. This includes an expired lease whose holder
		// fields remain present and a reschedule whose active holder fields
		// were cleared.
		if order.RetryCount != 0 || order.LeaseEpoch != 0 || order.LeaseHolderID != "" ||
			order.LeaseHolderBootID != "" || order.LeaseTokenHash != "" || order.LeaseExpireAtMs != 0 {
			return false, nil
		}
		if order.CreatedAtMs <= 0 {
			return false, fmt.Errorf("%w: recovery grace created_at evidence", ErrPayloadInvalid)
		}
		// The trusted now is read inside the same EVALSHA after the order
		// fields, preserving the original evidence-before-clock ordering.
		trustedNowMs := order.TrustedNowMs
		if trustedNowMs < order.CreatedAtMs {
			return false, fmt.Errorf("%w: recovery grace Redis time/created_at evidence", ErrPayloadInvalid)
		}
		graceMs := w.config.RecoveryGrace.Milliseconds()
		if graceMs == 0 {
			graceMs = 1
		}
		return trustedNowMs-order.CreatedAtMs < graceMs, nil
	}
	return w.deferFreshFirstAttemptLegacy(ctx, bucket, due)
}

func (w *Worker) deferFreshFirstAttemptLegacy(ctx context.Context, bucket rediskey.AuthorityBucketRef, due authority.DueWorkRef) (bool, error) {
	readCtx, cancel := context.WithTimeout(ctx, minDuration(w.config.OperationTimeout, time.Second))
	order, err := w.config.SourceOrders.GetSourceOrder(readCtx, bucket, w.family(), due.Ref.WorkID)
	cancel()
	if err != nil {
		return false, fmt.Errorf("recovery grace read %s: %w", due.Ref.WorkID, err)
	}
	if order.WorkID != due.Ref.WorkID || order.WorkType != w.family() {
		return false, fmt.Errorf("%w: recovery grace SourceOrder identity", ErrPayloadInvalid)
	}
	// Any durable retry or lease evidence means Direct already had its only
	// grace window. This includes an expired lease whose holder fields remain
	// present and a reschedule whose active holder fields were cleared.
	if order.RetryCount != 0 || order.LeaseEpoch != 0 || order.LeaseHolderID != "" ||
		order.LeaseHolderBootID != "" || order.LeaseTokenHash != "" || order.LeaseExpireAtMs != 0 {
		return false, nil
	}
	if order.CreatedAtMs <= 0 {
		return false, fmt.Errorf("%w: recovery grace created_at evidence", ErrPayloadInvalid)
	}
	// Observe Redis TIME only after the SourceOrder evidence read. Taking the
	// clock first would let a slow evidence read extend Direct's grace beyond
	// the configured window by the read latency.
	timeCtx, timeCancel := context.WithTimeout(ctx, minDuration(w.config.OperationTimeout, time.Second))
	trustedNowMs, timeErr := w.config.RecoveryNowMs(timeCtx)
	timeCancel()
	if timeErr != nil {
		return false, fmt.Errorf("recovery grace trusted time %s: %w", due.Ref.WorkID, timeErr)
	}
	if trustedNowMs < order.CreatedAtMs {
		return false, fmt.Errorf("%w: recovery grace Redis time/created_at evidence", ErrPayloadInvalid)
	}
	graceMs := w.config.RecoveryGrace.Milliseconds()
	if graceMs == 0 {
		graceMs = 1
	}
	return trustedNowMs-order.CreatedAtMs < graceMs, nil
}

func (w *Worker) family() string {
	if w != nil && strings.TrimSpace(w.config.Family) != "" {
		return w.config.Family
	}
	return FamilyCareReward
}

func (w *Worker) listDueCare(ctx context.Context, bucket rediskey.AuthorityBucketRef) ([]authority.DueWorkRef, error) {
	if pager, ok := w.config.DueIndex.(DueIndexPageScanner); ok {
		// Preserve a bounded per-bucket cursor when the worker is composed
		// directly against WorkLease. SharedDueIndexCache owns this cursor in
		// production, but the direct path must also page fairly past other
		// families, schedules, and malformed members.
		w.dueCursorMu.Lock()
		if w.dueCursors == nil {
			w.dueCursors = make(map[uint32]authority.DueWorkCursor, len(w.config.Buckets))
		}
		cursor := w.dueCursors[bucket.VirtualShard()]
		w.dueCursorMu.Unlock()
		page, err := pager.ListDueWorkPage(ctx, bucket, w.family(), cursor, w.config.ClaimLimit)
		if err != nil {
			return nil, err
		}
		w.dueCursorMu.Lock()
		if w.dueCursors[bucket.VirtualShard()] == cursor {
			w.dueCursors[bucket.VirtualShard()] = page.NextCursor
		}
		w.dueCursorMu.Unlock()
		if len(page.Refs) != 0 || page.Complete {
			return page.Refs, nil
		}
		return nil, fmt.Errorf("%w: bucket %d family %s cursor score=%g member=%q", ErrDueIndexFamilyUnavailable, bucket.VirtualShard(), w.family(), page.NextCursor.Score, page.NextCursor.Member)
	}
	if w.family() == FamilyCareReward {
		if scanner, ok := w.config.DueIndex.(careDueIndexScanner); ok {
			return scanner.ListDueCareWork(ctx, bucket, w.config.ClaimLimit)
		}
	}
	return w.config.DueIndex.ListDueWork(ctx, bucket, w.config.ClaimLimit)
}

type itemResult struct {
	Claimed, Completed, Rescheduled, BadPayload, Unknown, Failed, LeaseRenewed uint64
	Err                                                                        error
}

func (w *Worker) processDue(ctx context.Context, bucket rediskey.AuthorityBucketRef, due authority.DueWorkRef, revision uint64) itemResult {
	var out itemResult
	if w.config.ClaimAdmission != nil {
		if err := w.config.ClaimAdmission.CanClaimShard(ctx, bucket.VirtualShard(), revision); err != nil {
			out.Err = err
			return out
		}
	}
	claimStarted := w.config.Now()
	claimCtx, claimCancel := context.WithTimeout(ctx, minDuration(w.config.OperationTimeout, time.Second))
	claim, err := w.config.WorkLease.ClaimWorkWithBoot(claimCtx, bucket, w.family(), due.Ref.WorkID, w.config.Owner, w.config.BootID, w.config.Token, w.config.LeaseTTL.Milliseconds())
	claimCancel()
	if err != nil {
		out.Err = fmt.Errorf("claim %s: %w", due.Ref.WorkID, err)
		return out
	}
	if claim.Status != authority.WorkClaimClaimed {
		return out
	}
	out.Claimed = 1
	if claim.WorkID != due.Ref.WorkID || claim.WorkType != w.family() || claim.WorkVersion == 0 || claim.LeaseEpoch == 0 || claim.SubjectPlayerID == 0 || strings.TrimSpace(claim.OperationID) == "" {
		out.BadPayload = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, fmt.Errorf("%w: claimed work identity", ErrPayloadInvalid))
		return out
	}
	if w.family() == string(identity.WorkTypeHarvest) {
		return w.processHarvestClaim(ctx, bucket, claim, claimStarted, out)
	}
	if w.family() == string(identity.WorkTypePlant) {
		return w.processPlantClaim(ctx, bucket, claim, claimStarted, out)
	}
	if w.family() == string(identity.WorkTypeUnlockPlot) {
		return w.processUnlockClaim(ctx, bucket, claim, claimStarted, out)
	}
	if w.family() == string(identity.WorkTypeTheftVisitor) {
		return w.processTheftVisitorClaim(ctx, bucket, claim, claimStarted, out)
	}
	if w.family() == string(identity.WorkTypeTheftGuard) {
		return w.processTheftGuardClaim(ctx, bucket, claim, claimStarted, out)
	}
	payload, err := DecodeCareRewardPayload(claim.WorkID, claim.Payload)
	if err == nil {
		canonical, hashErr := CareRewardPayloadHash(payload)
		if hashErr != nil || claim.PayloadHash != hex.EncodeToString(canonical[:]) {
			err = fmt.Errorf("%w: SourceOrder payload_hash mismatch", ErrPayloadInvalid)
		}
	}
	if err == nil && claim.WorkType != FamilyCareReward {
		err = fmt.Errorf("%w: claimed work type %q", ErrUnsupportedWorkType, claim.WorkType)
	}
	if err == nil && claim.SubjectPlayerID != payload.GetCarerPlayerId() {
		err = fmt.Errorf("%w: subject player mismatch", ErrPayloadInvalid)
	}
	if err == nil {
		err = w.validateSourceBucket(bucket, payload.GetCareResolutionSnapshot().GetSourceFarmId())
	}
	if err != nil {
		out.BadPayload = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}

	window, err := w.ensureLease(ctx, bucket, claim, claimStarted)
	if err != nil {
		out.Rescheduled = 1
		out.Err = err
		return out
	}
	if window.renewed {
		out.LeaseRenewed = 1
	}
	address, err := careWorkAddress(bucket, claim.WorkID, payload.GetCareResolutionSnapshot().GetSourceFarmId())
	if err != nil {
		out.BadPayload = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	invocation, err := identity.ParticipantInvocationID(identity.WorkTypeCareReward, claim.WorkID, identity.StepApplyCareReward, 1)
	if err != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	opCtx, cancel := boundedOperationContext(ctx, w.config.OperationTimeout, window.safeDeadline, w.config.Now)
	defer cancel()
	route, err := w.config.Route(opCtx, payload.GetCarerPlayerId())
	if err != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	request, err := recoveryRequest(route, address, claim, invocation, w.config.Owner, w.config.BootID, w.config.Token, w.config.ParticipantBudgetMs, claim.Payload, bucket.LayoutVersion())
	if err != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	if !w.config.Now().Before(window.safeDeadline) {
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, ErrLeaseLost)
		return out
	}
	response, rpcErr := w.config.Player.ResumeStep(opCtx, request)
	if rpcErr != nil {
		out.Unknown = 1
		out.Rescheduled = 1
		// A transport error is conservatively response-lost.  The next call
		// uses the exact same invocation and lets Player ingress resolve the
		// ParticipantReceipt before any possible effect.
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, rpcErr)
		return out
	}
	if response != nil && response.GetOutcome() == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN {
		out.Unknown = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, errors.New("Participant TIMEOUT_UNKNOWN; receipt-first retry required"))
		return out
	}
	if err := ValidateTerminalProof(response, claim.WorkID, payload); err != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	// The participant is already terminally proven.  CompleteWork is allowed
	// only after that proof and remains fenced by the exact claim epoch/version.
	completionProof, proofErr := BuildWorkCompletionProof(claim, payload, response, bucket.LayoutVersion())
	if proofErr != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, proofErr)
		return out
	}
	completeCtx, completeCancel := context.WithTimeout(ctx, minDuration(w.config.OperationTimeout, time.Second))
	complete, completeErr := w.config.WorkLease.CompleteWorkWithProof(completeCtx, bucket, FamilyCareReward, claim.WorkID, w.config.Owner, w.config.BootID, claim.LeaseEpoch, claim.WorkVersion, w.config.Token, completionProof)
	completeCancel()
	if completeErr != nil {
		out.Failed = 1
		out.Err = fmt.Errorf("complete %s: %w", claim.WorkID, completeErr)
		return out
	}
	switch complete.Status {
	case authority.WorkLeaseCompleted, authority.WorkLeaseAlreadyCompleted:
		out.Completed = 1
	default:
		out.Failed = 1
		out.Err = fmt.Errorf("complete %s: %s", claim.WorkID, complete.Status)
	}
	return out
}

func (w *Worker) processHarvestClaim(ctx context.Context, bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, claimStarted time.Time, out itemResult) itemResult {
	payload, err := sourceorder.DecodeGrantHarvestPayload(claim.WorkID, claim.Payload)
	if err == nil {
		canonical, hashErr := sourceorder.HarvestPayloadHash(payload)
		if hashErr != nil || claim.PayloadHash != hex.EncodeToString(canonical[:]) {
			err = fmt.Errorf("%w: harvest payload_hash mismatch", ErrPayloadInvalid)
		}
	}
	if err == nil && claim.SubjectPlayerID != payload.GetPlayerId() {
		err = fmt.Errorf("%w: harvest subject player mismatch", ErrPayloadInvalid)
	}
	if err == nil {
		err = w.validateSourceBucket(bucket, payload.GetHarvestDecision().GetFarmId())
	}
	if err != nil {
		out.BadPayload = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	window, err := w.ensureLease(ctx, bucket, claim, claimStarted)
	if err != nil {
		out.Rescheduled = 1
		out.Err = err
		return out
	}
	if window.renewed {
		out.LeaseRenewed = 1
	}
	address, err := sourceorder.WorkAddress(bucket, string(identity.WorkTypeHarvest), claim.WorkID, workidentity.RouteDomainFarm, payload.GetHarvestDecision().GetFarmId())
	if err != nil {
		out.BadPayload = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	invocation, err := sourceorder.HarvestInvocation(claim.WorkID)
	if err != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	opCtx, cancel := boundedOperationContext(ctx, w.config.OperationTimeout, window.safeDeadline, w.config.Now)
	defer cancel()
	route, err := w.config.Route(opCtx, payload.GetPlayerId())
	if err != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	request, err := harvestRecoveryRequest(route, address, claim, invocation, w.config.Owner, w.config.BootID, w.config.Token, w.config.ParticipantBudgetMs, bucket.LayoutVersion())
	if err != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	response, rpcErr := w.config.Player.ResumeStep(opCtx, request)
	if rpcErr != nil {
		out.Unknown = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, rpcErr)
		return out
	}
	if response != nil && response.GetOutcome() == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN {
		out.Unknown = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, errors.New("Participant TIMEOUT_UNKNOWN; receipt-first retry required"))
		return out
	}
	if err := validateHarvestTerminalProof(response, claim.WorkID, invocation); err != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, err)
		return out
	}
	completionProof, proofErr := buildHarvestCompletionProof(claim, payload, response, bucket.LayoutVersion())
	if proofErr != nil {
		out.Failed = 1
		out.Rescheduled = 1
		out.Err = w.deferClaim(ctx, bucket, claim, claim.RetryCount+1, proofErr)
		return out
	}
	completeCtx, completeCancel := context.WithTimeout(ctx, minDuration(w.config.OperationTimeout, time.Second))
	complete, completeErr := w.config.WorkLease.CompleteWorkWithProof(completeCtx, bucket, string(identity.WorkTypeHarvest), claim.WorkID, w.config.Owner, w.config.BootID, claim.LeaseEpoch, claim.WorkVersion, w.config.Token, completionProof)
	completeCancel()
	if completeErr != nil {
		out.Failed = 1
		out.Err = fmt.Errorf("complete %s: %w", claim.WorkID, completeErr)
		return out
	}
	switch complete.Status {
	case authority.WorkLeaseCompleted, authority.WorkLeaseAlreadyCompleted:
		out.Completed = 1
	default:
		out.Failed = 1
		out.Err = fmt.Errorf("complete %s: %s", claim.WorkID, complete.Status)
	}
	return out
}

func harvestRecoveryRequest(route *logicrpcpb.ActorRouteHeader, address workidentity.WorkAddress, claim authority.WorkClaim, invocation, callerID, bootID, leaseToken string, budget uint32, authorityLayoutVersion string) (*contractspb.ParticipantRequest, error) {
	if route == nil || route.GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER || route.GetActorId() != claim.SubjectPlayerID {
		return nil, fmt.Errorf("%w: PLAYER route does not match harvest owner", ErrInvalidConfig)
	}
	wire, err := wireCareWorkAddress(address, authorityLayoutVersion)
	if err != nil {
		return nil, err
	}
	return &contractspb.ParticipantRequest{
		RouteHeader: route, ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD,
		TypedPayload: append([]byte(nil), claim.Payload...), CallerContext: &contractspb.ParticipantRequest_Recovery{Recovery: &contractspb.InternalCallerContext{
			CallerType:       contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY,
			CallerInstanceId: callerID, CallerBootId: bootID, Work: wire, ExpectedWorkVersion: claim.WorkVersion,
			ParticipantStep:         contractspb.ParticipantStep_PARTICIPANT_STEP_GRANT_HARVEST_REWARD,
			ParticipantInvocationId: invocation, LeaseOwnerToken: leaseToken, LeaseEpoch: claim.LeaseEpoch, RemainingBudgetMs: budget,
		}},
	}, nil
}

type leaseWindow struct {
	safeDeadline  time.Time
	claimExpireAt time.Time
	renewed       bool
}

func (w *Worker) ensureLease(ctx context.Context, bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, claimStarted time.Time) (leaseWindow, error) {
	accepted := time.Duration(claim.AcceptedTTLms) * time.Millisecond
	if accepted <= 0 {
		return leaseWindow{}, fmt.Errorf("%w: claim returned no accepted TTL", ErrLeaseLost)
	}
	window := leaseWindow{
		safeDeadline:  claimStarted.Add(accepted - w.config.SafetyMargin),
		claimExpireAt: claimStarted.Add(accepted),
	}
	need := w.config.OperationTimeout + w.config.SafetyMargin
	if w.config.Now().Before(window.safeDeadline) && window.safeDeadline.Sub(w.config.Now()) >= need {
		return window, nil
	}
	renewStarted := w.config.Now()
	renewCtx, renewCancel := context.WithTimeout(ctx, minDuration(w.config.OperationTimeout, time.Second))
	renew, err := w.config.WorkLease.RenewWorkLeaseWithBoot(renewCtx, bucket, claim.WorkType, claim.WorkID, w.config.Owner, w.config.BootID, claim.LeaseEpoch, w.config.Token, w.config.LeaseTTL.Milliseconds())
	renewCancel()
	if err != nil {
		return leaseWindow{}, fmt.Errorf("renew %s: %w", claim.WorkID, err)
	}
	if renew.Status != authority.WorkLeaseRenewed {
		return leaseWindow{}, fmt.Errorf("%w: renew status %s", ErrLeaseLost, renew.Status)
	}
	accepted = time.Duration(renew.AcceptedTTLms) * time.Millisecond
	if accepted <= 0 {
		return leaseWindow{}, fmt.Errorf("%w: renew returned no accepted TTL", ErrLeaseLost)
	}
	window = leaseWindow{safeDeadline: renewStarted.Add(accepted - w.config.SafetyMargin), claimExpireAt: renewStarted.Add(accepted), renewed: true}
	if !w.config.Now().Before(window.safeDeadline) || window.safeDeadline.Sub(w.config.Now()) < need {
		return leaseWindow{}, fmt.Errorf("%w: lease safety margin leaves no operation budget", ErrLeaseLost)
	}
	return window, nil
}

func (w *Worker) deferClaim(ctx context.Context, bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, attempt uint64, cause error) error {
	if cause == nil {
		cause = ErrRetryDeferred
	}
	if ctx != nil && ctx.Err() != nil {
		return fmt.Errorf("%w: %v", ErrRetryDeferred, cause)
	}
	w.randomMu.Lock()
	attempt = maxUint64(attempt, claim.RetryCount+1)
	delay := w.config.Backoff.NextBackoff(attempt, w.random)
	w.randomMu.Unlock()
	if delay <= 0 {
		delay = w.config.Backoff.Base
	}
	delayMs := delay.Milliseconds()
	if delayMs < 1 {
		delayMs = 1
	}
	// RescheduleWork persists the retry count, error class and Redis-time due
	// point on the SourceOrder WorkRecord. It also moves permanent failures to
	// RECONCILE_REQUIRED at the durable retry/deadline boundary.
	renewCtx, cancel := context.WithTimeout(context.Background(), minDuration(w.config.OperationTimeout, time.Second))
	rescheduled, err := w.config.WorkLease.RescheduleWork(renewCtx, bucket, claim.WorkType, claim.WorkID, w.config.Owner, w.config.BootID, claim.LeaseEpoch, claim.WorkVersion, w.config.Token, attempt, delayMs, errorClass(cause))
	cancel()
	if err != nil {
		return fmt.Errorf("%w: %v; renew retry: %v", ErrRetryDeferred, cause, err)
	}
	if rescheduled.Status != authority.WorkLeaseRenewed && rescheduled.Status != authority.WorkLeaseReconcileRequired {
		return fmt.Errorf("%w: %v; durable retry status=%s", ErrRetryDeferred, cause, rescheduled.Status)
	}
	return fmt.Errorf("%w: %v", ErrRetryDeferred, cause)
}

func maxUint64(left, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}

func errorClass(err error) string {
	switch {
	case errors.Is(err, ErrPayloadInvalid), errors.Is(err, ErrUnsupportedWorkType):
		return "PAYLOAD_INVALID"
	case errors.Is(err, ErrLeaseLost):
		return "LEASE_LOST"
	case errors.Is(err, ErrTerminalProof):
		return "RESULT_PROOF_INVALID"
	default:
		return "RETRYABLE_ERROR"
	}
}

func (w *Worker) validateSourceBucket(bucket rediskey.AuthorityBucketRef, farmID uint64) error {
	if farmID == 0 {
		return fmt.Errorf("%w: source farm id is zero", ErrPayloadInvalid)
	}
	if w.config.BucketForFarm == nil {
		return nil
	}
	want, err := w.config.BucketForFarm(farmID)
	if err != nil {
		return fmt.Errorf("%w: source bucket: %v", ErrPayloadInvalid, err)
	}
	if want.Domain() != actorrouting.DomainFarm || want.VirtualShard() != bucket.VirtualShard() || want.LayoutVersion() != bucket.LayoutVersion() {
		return fmt.Errorf("%w: source farm is not in scanned authority bucket", ErrPayloadInvalid)
	}
	return nil
}

func (w *Worker) validateSourcePlayerBucket(bucket rediskey.AuthorityBucketRef, playerID uint64) error {
	if playerID == 0 {
		return fmt.Errorf("%w: source player id is zero", ErrPayloadInvalid)
	}
	if w.config.BucketForPlayer == nil {
		return nil
	}
	want, err := w.config.BucketForPlayer(playerID)
	if err != nil {
		return fmt.Errorf("%w: source player bucket: %v", ErrPayloadInvalid, err)
	}
	if want.Domain() != actorrouting.DomainPlayer || want.VirtualShard() != bucket.VirtualShard() || want.LayoutVersion() != bucket.LayoutVersion() {
		return fmt.Errorf("%w: source player is not in scanned authority bucket", ErrPayloadInvalid)
	}
	return nil
}

// DecodeCareRewardPayload decodes and verifies the frozen typed payload.  It
// does not make a Farm or Player business decision; it only binds identity,
// snapshot/hash and deterministic protobuf bytes to the SourceOrder.
func DecodeCareRewardPayload(workID string, payloadBytes []byte) (*contractspb.ApplyCareRewardPayload, error) {
	if workidentity.ValidateStableOpaqueID(workID) != nil || len(payloadBytes) == 0 {
		return nil, fmt.Errorf("%w: work id or payload bytes", ErrPayloadInvalid)
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD)
	if err != nil {
		return nil, err
	}
	payload := spec.NewPayload().(*contractspb.ApplyCareRewardPayload)
	if err := proto.Unmarshal(payloadBytes, payload); err != nil {
		return nil, fmt.Errorf("%w: protobuf decode: %v", ErrPayloadInvalid, err)
	}
	deterministic, err := (proto.MarshalOptions{Deterministic: true}).Marshal(payload)
	if err != nil || !bytes.Equal(deterministic, payloadBytes) {
		return nil, fmt.Errorf("%w: payload is not deterministic protobuf", ErrPayloadInvalid)
	}
	if _, err := contracts.CanonicalParticipantHash(spec, payload); err != nil {
		return nil, fmt.Errorf("%w: canonical payload: %v", ErrPayloadInvalid, err)
	}
	snapshot := payload.GetCareResolutionSnapshot()
	if payload.GetCareRewardOrderId() != workID || payload.GetCarerPlayerId() == 0 || snapshot == nil ||
		snapshot.GetSourceFarmId() == 0 || snapshot.GetSourceOwnerPlayerId() == 0 ||
		snapshot.GetCarerPlayerId() != payload.GetCarerPlayerId() || snapshot.GetPlotId() == 0 ||
		workidentity.ValidateStableOpaqueID(snapshot.GetCropCycleId()) != nil ||
		workidentity.ValidateStableOpaqueID(snapshot.GetIncidentId()) != nil ||
		len(payload.GetCareResolutionHash()) != 32 || len(snapshot.GetCareResolutionHash()) != 32 ||
		!bytes.Equal(payload.GetCareResolutionHash(), snapshot.GetCareResolutionHash()) {
		return nil, fmt.Errorf("%w: order, carer, snapshot or resolution hash binding", ErrPayloadInvalid)
	}
	if snapshot.GetResolvedAtMs() <= 0 || snapshot.GetCommittedPlotVersion() == 0 || snapshot.GetCommittedFarmVersion() == 0 || snapshot.GetCareRewardRuleVersion() == 0 {
		return nil, fmt.Errorf("%w: incomplete frozen resolution snapshot", ErrPayloadInvalid)
	}
	switch snapshot.GetCareType() {
	case contractspb.CareType_CARE_TYPE_WATER, contractspb.CareType_CARE_TYPE_REMOVE_PEST:
	default:
		return nil, fmt.Errorf("%w: unsupported care type", ErrPayloadInvalid)
	}
	switch snapshot.GetCareContext() {
	case contractspb.CareContext_CARE_CONTEXT_OWNER:
		if snapshot.GetCarerPlayerId() != snapshot.GetSourceOwnerPlayerId() || snapshot.GetFriendInteractionEligible() {
			return nil, fmt.Errorf("%w: owner care binding", ErrPayloadInvalid)
		}
	case contractspb.CareContext_CARE_CONTEXT_FRIEND:
		if snapshot.GetCarerPlayerId() == snapshot.GetSourceOwnerPlayerId() || !snapshot.GetFriendInteractionEligible() {
			return nil, fmt.Errorf("%w: friend care binding", ErrPayloadInvalid)
		}
	default:
		return nil, fmt.Errorf("%w: unsupported care context", ErrPayloadInvalid)
	}
	resolutionHash, hashErr := CareResolutionHash(snapshot)
	if hashErr != nil || !bytes.Equal(resolutionHash[:], snapshot.GetCareResolutionHash()) {
		return nil, fmt.Errorf("%w: frozen resolution hash", ErrPayloadInvalid)
	}
	wantOrderID, orderErr := identity.SourceOrderID(identity.WorkTypeCareReward, identity.SourceKindFarmActor, strconv.FormatUint(snapshot.GetSourceFarmId(), 10), snapshot.GetIncidentId(), snapshot.GetCommittedFarmVersion())
	if orderErr != nil || wantOrderID != workID {
		return nil, fmt.Errorf("%w: deterministic source order identity", ErrPayloadInvalid)
	}
	return payload, nil
}

// CareRewardPayloadHash is the semantic canonical hash persisted in the
// Farm-authority SourceOrder payload_hash field. It is intentionally distinct
// from the actor commit's operation hash and from raw protobuf bytes.
func CareRewardPayloadHash(payload *contractspb.ApplyCareRewardPayload) ([32]byte, error) {
	if payload == nil {
		return [32]byte{}, fmt.Errorf("%w: nil payload", ErrPayloadInvalid)
	}
	spec, err := contracts.LookupParticipantCommand(contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD)
	if err != nil {
		return [32]byte{}, err
	}
	hash, err := contracts.CanonicalParticipantHash(spec, payload)
	if err != nil {
		return [32]byte{}, fmt.Errorf("%w: canonical payload hash: %v", ErrPayloadInvalid, err)
	}
	return hash, nil
}

// CareResolutionHash is the frozen semantic hash over the complete
// CareResolutionSnapshot except its self-referential hash field.  The payload
// and Player terminal result both bind to this value; raw protobuf bytes are
// not used as the business hash.
func CareResolutionHash(snapshot *contractspb.CareResolutionSnapshot) ([32]byte, error) {
	if snapshot == nil {
		return [32]byte{}, fmt.Errorf("%w: nil resolution snapshot", ErrPayloadInvalid)
	}
	copyOf, ok := proto.Clone(snapshot).(*contractspb.CareResolutionSnapshot)
	if !ok {
		return [32]byte{}, fmt.Errorf("%w: resolution snapshot clone", ErrPayloadInvalid)
	}
	copyOf.CareResolutionHash = nil
	return contracts.CanonicalHash(copyOf)
}

// ValidateTerminalProof accepts only a typed, hash-verified Player receipt
// proof.  REJECTED and TIMEOUT_UNKNOWN deliberately do not complete a
// business-required CareRewardOrder.
func ValidateTerminalProof(response *contractspb.ParticipantResponse, orderID string, payload *contractspb.ApplyCareRewardPayload) error {
	if response == nil || response.GetParticipantCommand() != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD {
		return fmt.Errorf("%w: response command", ErrTerminalProof)
	}
	wantInvocation, err := identity.ParticipantInvocationID(identity.WorkTypeCareReward, orderID, identity.StepApplyCareReward, 1)
	if err != nil || response.GetParticipantInvocationId() != wantInvocation {
		return fmt.Errorf("%w: response invocation", ErrTerminalProof)
	}
	if response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED &&
		response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE {
		return fmt.Errorf("%w: non-terminal or rejected outcome %s", ErrTerminalProof, response.GetOutcome())
	}
	if response.GetFailure() != nil {
		return fmt.Errorf("%w: terminal outcome carries failure", ErrTerminalProof)
	}
	if response.GetCommittedRecordVersion() == 0 || len(response.GetTypedResult()) == 0 || len(response.GetParticipantResultHash()) == 0 {
		return fmt.Errorf("%w: missing committed version/result/hash", ErrTerminalProof)
	}
	spec, err := contracts.LookupParticipantCommand(response.GetParticipantCommand())
	if err != nil {
		return fmt.Errorf("%w: command spec: %v", ErrTerminalProof, err)
	}
	result := spec.NewResult().(*contractspb.ApplyCareRewardResult)
	if err := proto.Unmarshal(response.GetTypedResult(), result); err != nil {
		return fmt.Errorf("%w: typed result decode: %v", ErrTerminalProof, err)
	}
	deterministic, err := (proto.MarshalOptions{Deterministic: true}).Marshal(result)
	if err != nil || !bytes.Equal(deterministic, response.GetTypedResult()) {
		return fmt.Errorf("%w: typed result is not deterministic protobuf", ErrTerminalProof)
	}
	hash, err := contracts.CanonicalParticipantHash(spec, result)
	if err != nil || !bytes.Equal(hash[:], response.GetParticipantResultHash()) {
		return fmt.Errorf("%w: result hash", ErrTerminalProof)
	}
	snapshot := (*contractspb.CareResolutionSnapshot)(nil)
	if payload != nil {
		snapshot = payload.GetCareResolutionSnapshot()
	}
	if snapshot == nil || result.GetCareRewardOrderId() != orderID || result.GetIncidentId() != snapshot.GetIncidentId() ||
		result.GetGrantedExp() != snapshot.GetRewardExp() ||
		result.GetFriendInteractionFactCommitted() != (snapshot.GetCareContext() == contractspb.CareContext_CARE_CONTEXT_FRIEND && snapshot.GetFriendInteractionEligible()) ||
		result.GetTaskPeriodKey() == "" || result.GetPlayerRecordVersion() == 0 || result.GetPlayerRecordVersion() != response.GetCommittedRecordVersion() {
		return fmt.Errorf("%w: result identity/version", ErrTerminalProof)
	}
	return nil
}

// BuildWorkCompletionProof turns the already validated Participant terminal
// response into the bounded proof consumed by CompleteWorkV2.
func BuildWorkCompletionProof(claim authority.WorkClaim, payload *contractspb.ApplyCareRewardPayload, response *contractspb.ParticipantResponse, authorityLayoutVersion string) (authority.WorkCompletionProof, error) {
	if err := ValidateTerminalProof(response, claim.WorkID, payload); err != nil {
		return authority.WorkCompletionProof{}, err
	}
	payloadHash, err := CareRewardPayloadHash(payload)
	if err != nil {
		return authority.WorkCompletionProof{}, err
	}
	payloadHashHex := hex.EncodeToString(payloadHash[:])
	if claim.PayloadHash != payloadHashHex {
		return authority.WorkCompletionProof{}, fmt.Errorf("%w: SourceOrder payload_hash mismatch", ErrPayloadInvalid)
	}
	outcome := ""
	switch response.GetOutcome() {
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED:
		outcome = "APPLIED"
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE:
		outcome = "NO_CHANGE"
	default:
		return authority.WorkCompletionProof{}, fmt.Errorf("%w: terminal outcome", ErrTerminalProof)
	}
	resultHash := hex.EncodeToString(response.GetParticipantResultHash())
	snapshot := payload.GetCareResolutionSnapshot()
	proof := authority.WorkCompletionProof{
		SourceAuthorityKind:        "FARM_ACTOR",
		CanonicalSourceAuthorityID: strconv.FormatUint(snapshot.GetSourceFarmId(), 10),
		AuthorityLayoutVersion:     authorityLayoutVersion,
		ActorRouteDomain:           "FARM",
		PayloadHash:                payloadHashHex,
		InvocationID:               response.GetParticipantInvocationId(),
		ParticipantStep:            "APPLY_CARE_REWARD",
		ParticipantOutcome:         outcome,
		ParticipantResultHash:      resultHash,
		ParticipantResultVersion:   response.GetCommittedRecordVersion(),
	}
	proof.ResultProofHash = authority.WorkCompletionProofHash(proof)
	return proof, nil
}

func careWorkAddress(bucket rediskey.AuthorityBucketRef, orderID string, sourceFarmID uint64) (workidentity.WorkAddress, error) {
	address := workidentity.WorkAddress{
		WorkID: orderID, WorkType: FamilyCareReward, PlacementKind: workidentity.SourcePersistenceBucket,
		BucketVersion: 1, BucketID: bucket.VirtualShard(), SourceRouteDomain: workidentity.RouteDomainFarm, SourceActorID: sourceFarmID,
	}
	if err := address.ValidateShape(); err != nil {
		return workidentity.WorkAddress{}, err
	}
	return address, nil
}

// CareWorkAddress builds the durable WorkAddress for a committed Care order;
// it does not inspect or mutate the order.
func CareWorkAddress(bucket rediskey.AuthorityBucketRef, orderID string, sourceFarmID uint64) (workidentity.WorkAddress, error) {
	return careWorkAddress(bucket, orderID, sourceFarmID)
}

func wireCareWorkAddress(address workidentity.WorkAddress, authorityLayoutVersion string) (*contractspb.WorkAddress, error) {
	if err := address.ValidateShape(); err != nil {
		return nil, err
	}
	if address.BucketVersion != 1 {
		return nil, fmt.Errorf("%w: Care source bucket version", ErrPayloadInvalid)
	}
	if strings.TrimSpace(authorityLayoutVersion) == "" {
		return nil, fmt.Errorf("%w: authority layout version", ErrInvalidConfig)
	}
	workType, err := sourceorder.WireWorkType(address.WorkType)
	if err != nil {
		return nil, ErrUnsupportedWorkType
	}
	domain := sourceorder.WireSourceDomain(address.SourceRouteDomain)
	kind := contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_FARM_ACTOR
	if address.SourceRouteDomain == workidentity.RouteDomainPlayer {
		kind = contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR
	}
	return &contractspb.WorkAddress{
		WorkId: address.WorkID, WorkType: workType,
		PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
		BucketVersion: address.BucketVersion, BucketId: address.BucketID,
		SourceAuthority: &contractspb.SourceAuthorityRef{
			AuthorityKind:          kind,
			CanonicalAuthorityId:   strconv.FormatUint(address.SourceActorID, 10),
			AuthorityLayoutVersion: authorityLayoutVersion,
			ActorRouteDomain:       &domain,
		},
	}, nil
}

func recoveryRequest(route *logicrpcpb.ActorRouteHeader, address workidentity.WorkAddress, claim authority.WorkClaim, invocation, callerID, bootID, leaseToken string, budget uint32, payload []byte, authorityLayoutVersion string) (*contractspb.ParticipantRequest, error) {
	if route == nil || route.GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER || route.GetActorId() != claim.SubjectPlayerID {
		return nil, fmt.Errorf("%w: PLAYER route does not match carer", ErrInvalidConfig)
	}
	wire, err := wireCareWorkAddress(address, authorityLayoutVersion)
	if err != nil {
		return nil, err
	}
	return &contractspb.ParticipantRequest{
		RouteHeader: route, ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD,
		TypedPayload: append([]byte(nil), payload...), CallerContext: &contractspb.ParticipantRequest_Recovery{Recovery: &contractspb.InternalCallerContext{
			CallerType:       contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY,
			CallerInstanceId: callerID, CallerBootId: bootID, Work: wire, ExpectedWorkVersion: claim.WorkVersion,
			ParticipantStep:         contractspb.ParticipantStep_PARTICIPANT_STEP_APPLY_CARE_REWARD,
			ParticipantInvocationId: invocation, LeaseOwnerToken: leaseToken, LeaseEpoch: claim.LeaseEpoch, RemainingBudgetMs: budget,
		}},
	}, nil
}

// DirectConfig composes the in-process fast path. Direct delivery does not
// claim before ExecuteStep. After a verified terminal Participant response it
// invokes the WorkLease direct-completion primitive, fencing and completing an
// unowned order in one Redis hop while a live worker lease stays recoverable.
type DirectConfig struct {
	WorkLease           WorkLease
	SourceOrders        SourceOrderReader
	Route               RouteProvider
	Player              CoordinatorParticipantClient
	Farm                CoordinatorParticipantClient
	FarmRoute           RouteProvider
	Owner               string
	Token               string
	BootID              string
	BucketForFarm       func(uint64) (rediskey.AuthorityBucketRef, error)
	BucketForPlayer     func(uint64) (rediskey.AuthorityBucketRef, error)
	LeaseTTL            time.Duration
	SafetyMargin        time.Duration
	ItemDeadline        time.Duration
	ParticipantBudgetMs uint32
	MaxItems            int
	MaxBytes            int
	Workers             int
	Backoff             workflow.BackoffConfig
	CompletionBatchSize int
	CompletionBatchWait time.Duration
	Now                 func() time.Time
	// CommitProofMode is off/shadow/prefer. Prefer is the only mode that may
	// rely on a valid route-fenced proof; it remains opt-in. CommitProofIssuer
	// is required for prefer and optional for mixed-version shadow rollout.
	CommitProofMode   sourceorder.CommitProofMode
	CommitProofIssuer *sourceorder.CommitProofIssuer
	// BestEffortNoSourcePersistence is an explicit experiment-only mode for
	// resident dirty actors. Only immutable envelopes returned by the source
	// actor are accepted; the dispatcher neither reads nor completes a durable
	// SourceOrder. Participant success is therefore process-local and is not
	// recoverable by farm-worker after a crash.
	BestEffortNoSourcePersistence bool
}

type DirectDispatcher struct {
	config          directConfig
	queue           chan directItem
	stop            chan struct{}
	completionStop  chan struct{}
	runCtx          context.Context
	runCancel       context.CancelFunc
	shutdownDone    chan struct{}
	bytes           atomic.Int64
	closing         atomic.Bool
	startOnce       sync.Once
	shutdownOnce    sync.Once
	abortOnce       sync.Once
	admissionMu     sync.RWMutex
	activeDispatch  sync.WaitGroup
	dispatchWG      sync.WaitGroup
	completionMu    sync.RWMutex
	completionWG    sync.WaitGroup
	completionQueue chan directCompletionItem
	// completionClosing is protected by completionMu. completionStarted is an
	// atomic fast-path check because Start may race a synchronous DispatchOnce.
	completionClosing bool
	completionStarted atomic.Bool
	enqueued          atomic.Uint64
	dropped           atomic.Uint64
	dispatched        atomic.Uint64
	completed         atomic.Uint64
	rescheduled       atomic.Uint64
	failed            atomic.Uint64
	proofAttached     atomic.Uint64
	proofUnavailable  atomic.Uint64
}

type directConfig struct {
	WorkLease                            WorkLease
	SourceOrders                         SourceOrderReader
	Route                                RouteProvider
	Player                               CoordinatorParticipantClient
	Farm                                 CoordinatorParticipantClient
	FarmRoute                            RouteProvider
	Owner, Token                         string
	BucketForFarm                        func(uint64) (rediskey.AuthorityBucketRef, error)
	BucketForPlayer                      func(uint64) (rediskey.AuthorityBucketRef, error)
	LeaseTTL, SafetyMargin, ItemDeadline time.Duration
	BootID                               string
	ParticipantBudgetMs                  uint32
	MaxItems, MaxBytes, Workers          int
	Backoff                              workflow.BackoffConfig
	CompletionBatchSize                  int
	CompletionBatchWait                  time.Duration
	Now                                  func() time.Time
	CommitProofMode                      sourceorder.CommitProofMode
	CommitProofIssuer                    *sourceorder.CommitProofIssuer
	BestEffortNoSourcePersistence        bool
}

type directItem struct {
	address        workidentity.WorkAddress
	size           int
	committed      *authority.SourceOrderSnapshot
	commitEvidence *sourceorder.DurableCommitEvidence
}

type directCompletionItem struct {
	request authority.WorkClaimAndCompleteRequest
	result  chan authority.WorkClaimAndCompleteBatchResult
}

type DirectStats struct {
	Depth, Bytes                                                  int64
	Enqueued, Dropped, Dispatched, Completed, Rescheduled, Failed uint64
	ProofAttached, ProofUnavailable                               uint64
}

func NewDirectDispatcher(config DirectConfig) (*DirectDispatcher, error) {
	if config.Player == nil || config.Route == nil ||
		config.BucketForFarm == nil || strings.TrimSpace(config.Owner) == "" || strings.TrimSpace(config.Token) == "" {
		return nil, fmt.Errorf("%w: direct route, Player Coordinator and identity are required", ErrInvalidConfig)
	}
	if !config.BestEffortNoSourcePersistence && (config.WorkLease == nil || config.SourceOrders == nil) {
		return nil, fmt.Errorf("%w: durable direct mode requires WorkLease and SourceOrder reader", ErrInvalidConfig)
	}
	if config.BestEffortNoSourcePersistence && config.CommitProofMode != sourceorder.CommitProofModeOff {
		return nil, fmt.Errorf("%w: dirty best-effort mode cannot mint durable SourceOrder commit proofs", ErrInvalidConfig)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if strings.TrimSpace(config.BootID) == "" {
		config.BootID = config.Owner
	}
	if config.LeaseTTL <= 0 {
		config.LeaseTTL = defaultLeaseTTL
	}
	if config.SafetyMargin <= 0 || config.SafetyMargin >= config.LeaseTTL {
		config.SafetyMargin = config.LeaseTTL / 5
	}
	if config.ItemDeadline <= 0 {
		config.ItemDeadline = defaultOperationTimeout
	}
	if config.ParticipantBudgetMs == 0 {
		config.ParticipantBudgetMs = defaultParticipantMs
	}
	if config.MaxItems <= 0 {
		config.MaxItems = 1024
	}
	if config.MaxBytes <= 0 {
		config.MaxBytes = 4 << 20
	}
	if config.Workers <= 0 {
		config.Workers = 4
	}
	if config.CompletionBatchSize <= 0 {
		config.CompletionBatchSize = 64
	}
	if config.CompletionBatchSize > 256 {
		return nil, fmt.Errorf("%w: completion batch exceeds 256", ErrInvalidConfig)
	}
	if config.CompletionBatchWait <= 0 {
		config.CompletionBatchWait = time.Millisecond
	}
	if config.Backoff.Base <= 0 {
		config.Backoff.Base = 250 * time.Millisecond
	}
	if config.Backoff.Max <= 0 {
		config.Backoff.Max = 30 * time.Second
	}
	if config.Backoff.Max < config.Backoff.Base {
		config.Backoff.Max = config.Backoff.Base
	}
	if err := config.CommitProofMode.ValidatePreferRollout(); err != nil {
		return nil, fmt.Errorf("%w: commit proof mode: %v", ErrInvalidConfig, err)
	}
	if config.CommitProofMode == sourceorder.CommitProofModePrefer && config.CommitProofIssuer == nil {
		return nil, fmt.Errorf("%w: prefer commit proof issuer is required", ErrInvalidConfig)
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	return &DirectDispatcher{config: directConfig{
		WorkLease: config.WorkLease, SourceOrders: config.SourceOrders, Route: config.Route, Player: config.Player,
		Farm: config.Farm, FarmRoute: config.FarmRoute,
		Owner: config.Owner, Token: config.Token, BootID: config.BootID,
		BucketForFarm: config.BucketForFarm, BucketForPlayer: config.BucketForPlayer,
		LeaseTTL: config.LeaseTTL, SafetyMargin: config.SafetyMargin, ItemDeadline: config.ItemDeadline,
		ParticipantBudgetMs: config.ParticipantBudgetMs, MaxItems: config.MaxItems, MaxBytes: config.MaxBytes,
		Workers: config.Workers, Backoff: config.Backoff,
		CompletionBatchSize: config.CompletionBatchSize, CompletionBatchWait: config.CompletionBatchWait, Now: config.Now,
		CommitProofMode: config.CommitProofMode, CommitProofIssuer: config.CommitProofIssuer,
		BestEffortNoSourcePersistence: config.BestEffortNoSourcePersistence,
	}, queue: make(chan directItem, config.MaxItems), completionQueue: make(chan directCompletionItem, config.MaxItems),
		stop: make(chan struct{}), completionStop: make(chan struct{}), runCtx: runCtx, runCancel: runCancel,
		shutdownDone: make(chan struct{})}, nil
}

// SetFarmDest attaches the Farm Coordinator dest used by PLANT/UNLOCK_PLOT.
// CARE/HARVEST Direct construction happens before the local Farm participant
// server exists; farm-svr calls this once that server is ready.
func (d *DirectDispatcher) SetFarmDest(farm CoordinatorParticipantClient, farmRoute RouteProvider, bucketForPlayer func(uint64) (rediskey.AuthorityBucketRef, error)) {
	if d == nil {
		return
	}
	d.config.Farm = farm
	d.config.FarmRoute = farmRoute
	if bucketForPlayer != nil {
		d.config.BucketForPlayer = bucketForPlayer
	}
}

// Enqueue accepts only a fully addressed source order. The payload size is a
// memory-budget hint; the durable SourceOrder remains the source of truth.
func (d *DirectDispatcher) Enqueue(address workidentity.WorkAddress, payloadBytes int) error {
	return d.enqueue(directItem{address: address, size: payloadBytes})
}

// EnqueueCommitted carries the immutable AttachedOrder returned only after a
// successful source-actor Authority Commit. CARE/HARVEST can therefore skip
// reading the just-written SourceOrder back from Redis. The envelope is only a
// latency hint: a lost/full queue still leaves DueIndex recovery unchanged.
func (d *DirectDispatcher) EnqueueCommitted(address workidentity.WorkAddress, attached authoritystore.AttachedOrder) error {
	return d.enqueueCommitted(address, attached, nil)
}

// EnqueueCommittedWithEvidence is the production CARE/HARVEST handoff. It
// carries the exact source actor CommitStamp beside the immutable AttachedOrder
// without turning either value into persisted state.
func (d *DirectDispatcher) EnqueueCommittedWithEvidence(address workidentity.WorkAddress, attached authoritystore.AttachedOrder, exact *authoritystore.ExactCommitEvidence) error {
	return d.enqueueCommitted(address, attached, exact)
}

func (d *DirectDispatcher) enqueueCommitted(address workidentity.WorkAddress, attached authoritystore.AttachedOrder, exact *authoritystore.ExactCommitEvidence) error {
	snapshot, evidence, err := d.committedSnapshot(address, attached, exact)
	if err != nil {
		return err
	}
	return d.enqueue(directItem{address: address, size: len(snapshot.TypedPayload), committed: snapshot, commitEvidence: evidence})
}

// DispatchCommittedOnce is the synchronous form used by tests and source
// paths that already own their execution budget. Production CARE/HARVEST use
// EnqueueCommitted so command admission never waits on the destination RPC.
func (d *DirectDispatcher) DispatchCommittedOnce(ctx context.Context, address workidentity.WorkAddress, attached authoritystore.AttachedOrder) error {
	return d.dispatchCommittedOnce(ctx, address, attached, nil)
}

func (d *DirectDispatcher) DispatchCommittedOnceWithEvidence(ctx context.Context, address workidentity.WorkAddress, attached authoritystore.AttachedOrder, exact *authoritystore.ExactCommitEvidence) error {
	return d.dispatchCommittedOnce(ctx, address, attached, exact)
}

func (d *DirectDispatcher) dispatchCommittedOnce(ctx context.Context, address workidentity.WorkAddress, attached authoritystore.AttachedOrder, exact *authoritystore.ExactCommitEvidence) error {
	if !d.beginDirectDispatch() {
		return ErrDirectClosed
	}
	defer d.activeDispatch.Done()
	snapshot, evidence, err := d.committedSnapshot(address, attached, exact)
	if err != nil {
		return err
	}
	return d.dispatchOnce(ctx, address, snapshot, evidence)
}

func (d *DirectDispatcher) committedSnapshot(address workidentity.WorkAddress, attached authoritystore.AttachedOrder, exact *authoritystore.ExactCommitEvidence) (*authority.SourceOrderSnapshot, *sourceorder.DurableCommitEvidence, error) {
	if d == nil {
		return nil, nil, ErrDirectClosed
	}
	switch address.WorkType {
	case FamilyCareReward, string(identity.WorkTypeHarvest), string(identity.WorkTypeTheftVisitor), string(identity.WorkTypeTheftGuard):
		if address.SourceRouteDomain != workidentity.RouteDomainFarm {
			return nil, nil, fmt.Errorf("%w: committed source must be FARM", ErrPayloadInvalid)
		}
	case string(identity.WorkTypePlant), string(identity.WorkTypeUnlockPlot):
		if address.SourceRouteDomain != workidentity.RouteDomainPlayer {
			return nil, nil, fmt.Errorf("%w: committed source must be PLAYER", ErrPayloadInvalid)
		}
	default:
		return nil, nil, fmt.Errorf("%w: committed envelope work type", ErrUnsupportedWorkType)
	}
	if attached.OrderID != address.WorkID || attached.OrderType != address.WorkType ||
		attached.SubjectPlayerID == 0 || strings.TrimSpace(attached.OperationID) == "" ||
		attached.DueAtMs <= 0 || attached.PayloadHash == (authoritystore.PayloadHash{}) {
		return nil, nil, fmt.Errorf("%w: committed source order identity", ErrPayloadInvalid)
	}
	sourceKind := "FARM_ACTOR"
	actorDomain := "FARM"
	if address.SourceRouteDomain == workidentity.RouteDomainPlayer {
		sourceKind = "PLAYER_ACTOR"
		actorDomain = "PLAYER"
	}
	bucket, err := d.resolveBucket(address)
	if err != nil {
		return nil, nil, err
	}
	var evidence *sourceorder.DurableCommitEvidence
	if d.config.CommitProofMode != sourceorder.CommitProofModeOff && exact != nil {
		durable, evidenceErr := sourceorder.NewDurableCommitEvidenceFromExact(exact, attached.OrderID)
		if evidenceErr != nil {
			return nil, nil, evidenceErr
		}
		evidence = &durable
	}
	return &authority.SourceOrderSnapshot{
		WorkID: attached.OrderID, WorkType: attached.OrderType,
		PlacementKind: "SOURCE_PERSISTENCE_BUCKET", BucketVersion: address.BucketVersion, BucketID: address.BucketID,
		SourceAuthorityKind: sourceKind, CanonicalSourceAuthorityID: strconv.FormatUint(address.SourceActorID, 10),
		AuthorityLayoutVersion: bucket.LayoutVersion(), ActorRouteDomain: actorDomain,
		SubjectPlayerID: attached.SubjectPlayerID, OperationID: attached.OperationID,
		PayloadHash: hex.EncodeToString(attached.PayloadHash[:]), State: "PENDING", WorkVersion: 1,
		TypedPayload: append([]byte(nil), attached.Payload...), NextDueAtMs: attached.DueAtMs,
	}, evidence, nil
}

func (d *DirectDispatcher) enqueue(item directItem) error {
	if d == nil {
		return ErrDirectClosed
	}
	// Hold read admission through the non-blocking queue send. Close takes the
	// write side before stopping dispatch, so every accepted item is visible to
	// the drain and no producer can enqueue behind it.
	d.admissionMu.RLock()
	defer d.admissionMu.RUnlock()
	if d.closing.Load() {
		return ErrDirectClosed
	}
	if err := item.address.ValidateShape(); err != nil {
		return err
	}
	if err := sourceorder.AcceptsDirect(item.address); err != nil {
		return err
	}
	if item.size < 0 {
		item.size = 0
	}
	if d.bytes.Add(int64(item.size)) > int64(d.config.MaxBytes) {
		d.bytes.Add(-int64(item.size))
		d.dropped.Add(1)
		return ErrDirectQueueBytes
	}
	select {
	case d.queue <- item:
		d.enqueued.Add(1)
		return nil
	default:
		d.bytes.Add(-int64(item.size))
		d.dropped.Add(1)
		return ErrDirectQueueFull
	}
}

func (d *DirectDispatcher) Start(ctx context.Context) {
	if d == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	d.start(ctx)
}

func (d *DirectDispatcher) start(ctx context.Context) {
	d.startOnce.Do(func() {
		if !d.config.BestEffortNoSourcePersistence {
			if _, ok := d.config.WorkLease.(batchWorkLease); ok && d.config.CompletionBatchSize > 1 {
				d.completionStarted.Store(true)
				d.completionWG.Add(1)
				go d.completionLoop()
			}
		}
		for i := 0; i < d.config.Workers; i++ {
			d.dispatchWG.Add(1)
			go d.loop()
		}
		if done := ctx.Done(); done != nil {
			go func() {
				<-done
				shutdownCtx, cancel := context.WithTimeout(context.Background(), d.config.ItemDeadline)
				defer cancel()
				_ = d.Shutdown(shutdownCtx)
			}()
		}
	})
}

// Close preserves the original blocking lifecycle contract. Production
// composition should use Shutdown with an explicit bounded context.
func (d *DirectDispatcher) Close() {
	_ = d.Shutdown(context.Background())
}

// Shutdown first closes admission, then drains every accepted dispatch and
// completion while its caller keeps route/participant/Redis dependencies
// alive. If ctx expires, in-flight calls are cancelled and queued items remain
// untouched; their already-durable SourceOrder and DueIndex entries are the
// recovery handoff.
func (d *DirectDispatcher) Shutdown(ctx context.Context) error {
	if d == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Even a dispatcher used only for pre-Start enqueue admission must start so
	// a successful bounded shutdown can drain those accepted items.
	d.start(context.Background())
	d.shutdownOnce.Do(func() {
		// Holding the write lock linearizes the close after every queue send and
		// synchronous-dispatch WaitGroup Add.
		d.admissionMu.Lock()
		d.closing.Store(true)
		close(d.stop)
		d.admissionMu.Unlock()
		go d.finishShutdown()
	})
	select {
	case <-d.shutdownDone:
		return nil
	case <-ctx.Done():
		d.abortOnce.Do(d.runCancel)
		return ctx.Err()
	}
}

func (d *DirectDispatcher) finishShutdown() {
	// Completion batching remains live while queue and synchronous dispatch
	// producers can still enqueue completion requests.
	d.dispatchWG.Wait()
	d.activeDispatch.Wait()

	d.completionMu.Lock()
	d.completionClosing = true
	close(d.completionStop)
	d.completionMu.Unlock()
	d.completionWG.Wait()
	d.completionStarted.Store(false)
	d.runCancel()
	close(d.shutdownDone)
}

func (d *DirectDispatcher) Stats() DirectStats {
	if d == nil {
		return DirectStats{}
	}
	return DirectStats{Depth: int64(len(d.queue)), Bytes: d.bytes.Load(), Enqueued: d.enqueued.Load(), Dropped: d.dropped.Load(), Dispatched: d.dispatched.Load(), Completed: d.completed.Load(), Rescheduled: d.rescheduled.Load(), Failed: d.failed.Load(), ProofAttached: d.proofAttached.Load(), ProofUnavailable: d.proofUnavailable.Load()}
}

func (d *DirectDispatcher) loop() {
	defer d.dispatchWG.Done()
	for {
		if d.runCtx.Err() != nil {
			return
		}
		select {
		case <-d.runCtx.Done():
			return
		case item := <-d.queue:
			d.dispatchItem(d.runCtx, item)
		case <-d.stop:
			// Admission is already stopped, so an empty queue is final. Every
			// worker may help drain without closing the producer-facing channel.
			// A bounded-shutdown timeout cancels runCtx and leaves the remaining
			// queue backed by its durable DueIndex entries.
			for {
				if d.runCtx.Err() != nil {
					return
				}
				select {
				case <-d.runCtx.Done():
					return
				case item := <-d.queue:
					d.dispatchItem(d.runCtx, item)
				default:
					return
				}
			}
		}
	}
}

func (d *DirectDispatcher) dispatchItem(ctx context.Context, item directItem) {
	d.bytes.Add(-int64(item.size))
	if err := d.dispatchOnce(ctx, item.address, item.committed, item.commitEvidence); err != nil {
		d.failed.Add(1)
	}
}

func (d *DirectDispatcher) completionLoop() {
	defer d.completionWG.Done()
	batcher := d.config.WorkLease.(batchWorkLease)
	stopping := false
	for {
		var first directCompletionItem
		if stopping {
			select {
			case first = <-d.completionQueue:
			default:
				return
			}
		} else {
			select {
			case first = <-d.completionQueue:
			case <-d.completionStop:
				stopping = true
				continue
			}
		}
		items := make([]directCompletionItem, 0, d.config.CompletionBatchSize)
		items = append(items, first)
		timer := time.NewTimer(d.config.CompletionBatchWait)
	collect:
		for len(items) < d.config.CompletionBatchSize {
			select {
			case item := <-d.completionQueue:
				items = append(items, item)
			case <-timer.C:
				break collect
			case <-d.completionStop:
				stopping = true
				break collect
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		d.flushCompletionBatch(batcher, items)
	}
}

func (d *DirectDispatcher) flushCompletionBatch(batcher batchWorkLease, items []directCompletionItem) {
	requests := make([]authority.WorkClaimAndCompleteRequest, len(items))
	for i := range items {
		requests[i] = items[i].request
	}
	batchCtx, cancel := context.WithTimeout(d.runCtx, d.config.ItemDeadline)
	results := batcher.ClaimAndCompleteWithProofBatch(batchCtx, requests)
	cancel()
	if len(results) != len(items) {
		err := fmt.Errorf("%w: completion batch result count", ErrInvalidConfig)
		results = make([]authority.WorkClaimAndCompleteBatchResult, len(items))
		for i := range results {
			results[i].Err = err
		}
	}
	for i := range items {
		items[i].result <- results[i]
	}
}

func (d *DirectDispatcher) completeDirectProof(ctx context.Context, request authority.WorkClaimAndCompleteRequest) (authority.WorkCompleteResult, error) {
	_, ok := d.config.WorkLease.(batchWorkLease)
	if !ok || !d.completionStarted.Load() || d.config.CompletionBatchSize <= 1 {
		return d.completeDirectProofSingle(ctx, request)
	}
	item := directCompletionItem{request: request, result: make(chan authority.WorkClaimAndCompleteBatchResult, 1)}
	// Pair the admission check and send under the read side. Close cannot stop
	// the completion loop between them; it takes the write side only after all
	// dispatch producers have drained.
	d.completionMu.RLock()
	if d.completionClosing {
		d.completionMu.RUnlock()
		return d.completeDirectProofSingle(ctx, request)
	}
	select {
	case d.completionQueue <- item:
		d.completionMu.RUnlock()
	default:
		d.completionMu.RUnlock()
		return d.completeDirectProofSingle(ctx, request)
	}
	select {
	case result := <-item.result:
		return result.Result, result.Err
	case <-ctx.Done():
		return authority.WorkCompleteResult{}, ctx.Err()
	}
}

func (d *DirectDispatcher) completeDirectProofSingle(ctx context.Context, request authority.WorkClaimAndCompleteRequest) (authority.WorkCompleteResult, error) {
	return d.config.WorkLease.ClaimAndCompleteWithProof(ctx, request.Bucket, request.WorkType, request.WorkID,
		request.HolderID, request.BootID, request.LeaseToken, request.RequestedTTLms, request.Proof)
}

// DispatchOnce is exported for deterministic tests and for a source actor
// that already has a durable WorkAddress at hand.
func (d *DirectDispatcher) DispatchOnce(ctx context.Context, address workidentity.WorkAddress) error {
	if !d.beginDirectDispatch() {
		return ErrDirectClosed
	}
	defer d.activeDispatch.Done()
	return d.dispatchOnce(ctx, address, nil, nil)
}

func (d *DirectDispatcher) beginDirectDispatch() bool {
	if d == nil {
		return false
	}
	d.admissionMu.RLock()
	defer d.admissionMu.RUnlock()
	if d.closing.Load() {
		return false
	}
	d.activeDispatch.Add(1)
	return true
}

func (d *DirectDispatcher) dispatchOnce(ctx context.Context, address workidentity.WorkAddress, committed *authority.SourceOrderSnapshot, commitEvidence *sourceorder.DurableCommitEvidence) error {
	if d == nil || (!d.config.BestEffortNoSourcePersistence && d.config.SourceOrders == nil) {
		return fmt.Errorf("%w: incomplete direct dispatcher", ErrInvalidConfig)
	}
	if d.config.BestEffortNoSourcePersistence && committed == nil {
		d.rescheduled.Add(1)
		return ErrBestEffortEnvelopeMissing
	}
	if ctx == nil {
		ctx = context.Background()
	}
	itemCtx, itemCancel := context.WithTimeout(ctx, d.config.ItemDeadline)
	stopOnShutdown := context.AfterFunc(d.runCtx, itemCancel)
	defer func() {
		stopOnShutdown()
		itemCancel()
	}()
	ctx = itemCtx
	if err := address.ValidateShape(); err != nil {
		d.rescheduled.Add(1)
		return err
	}
	if err := sourceorder.AcceptsDirect(address); err != nil {
		d.rescheduled.Add(1)
		return fmt.Errorf("%w: %v", ErrUnsupportedWorkType, err)
	}
	switch address.WorkType {
	case string(identity.WorkTypeHarvest):
		if d.config.Player == nil {
			return fmt.Errorf("%w: incomplete direct dispatcher", ErrInvalidConfig)
		}
		return d.dispatchHarvest(ctx, itemCtx, address, committed, commitEvidence)
	case string(identity.WorkTypePlant):
		return d.dispatchPlant(ctx, itemCtx, address, committed)
	case string(identity.WorkTypeUnlockPlot):
		return d.dispatchUnlock(ctx, itemCtx, address, committed)
	case string(identity.WorkTypeTheftVisitor):
		if d.config.Player == nil {
			return fmt.Errorf("%w: incomplete direct dispatcher", ErrInvalidConfig)
		}
		return d.dispatchTheftVisitor(ctx, itemCtx, address, committed)
	case string(identity.WorkTypeTheftGuard):
		if d.config.Player == nil {
			return fmt.Errorf("%w: incomplete direct dispatcher", ErrInvalidConfig)
		}
		return d.dispatchTheftGuard(ctx, itemCtx, address, committed)
	}
	if d.config.Player == nil {
		return fmt.Errorf("%w: incomplete direct dispatcher", ErrInvalidConfig)
	}
	if address.WorkType != FamilyCareReward || address.SourceRouteDomain != workidentity.RouteDomainFarm {
		d.rescheduled.Add(1)
		return fmt.Errorf("%w: DirectDispatcher CARE path requires FARM source", ErrUnsupportedWorkType)
	}
	d.dispatched.Add(1)
	bucket, err := d.resolveBucket(address)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	var order authority.SourceOrderSnapshot
	if committed != nil {
		order = *committed
	} else {
		order, err = d.config.SourceOrders.GetSourceOrder(itemCtx, bucket, FamilyCareReward, address.WorkID)
		if err != nil {
			d.rescheduled.Add(1)
			return err
		}
	}
	if order.State == "COMPLETED" {
		d.completed.Add(1)
		return nil
	}
	if order.State != "PENDING" || order.WorkType != FamilyCareReward || order.WorkID != address.WorkID ||
		order.PlacementKind != "SOURCE_PERSISTENCE_BUCKET" || order.BucketVersion != address.BucketVersion ||
		order.BucketID != address.BucketID || order.SourceAuthorityKind != "FARM_ACTOR" ||
		order.ActorRouteDomain != "FARM" || order.AuthorityLayoutVersion != bucket.LayoutVersion() ||
		order.CanonicalSourceAuthorityID != strconv.FormatUint(address.SourceActorID, 10) || order.SubjectPlayerID == 0 ||
		order.OperationID == "" || order.WorkVersion == 0 {
		d.rescheduled.Add(1)
		return fmt.Errorf("%w: source order identity", ErrPayloadInvalid)
	}
	payload, err := DecodeCareRewardPayload(order.WorkID, order.TypedPayload)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	canonicalPayloadHash, hashErr := CareRewardPayloadHash(payload)
	if hashErr != nil || order.PayloadHash != hex.EncodeToString(canonicalPayloadHash[:]) {
		d.rescheduled.Add(1)
		return fmt.Errorf("%w: SourceOrder payload_hash mismatch", ErrPayloadInvalid)
	}
	if payload == nil || payload.GetCareResolutionSnapshot() == nil || payload.GetCarerPlayerId() != order.SubjectPlayerID {
		d.rescheduled.Add(1)
		return fmt.Errorf("%w: source order subject binding", ErrPayloadInvalid)
	}
	if d.config.BucketForFarm != nil {
		want, bucketErr := d.config.BucketForFarm(payload.GetCareResolutionSnapshot().GetSourceFarmId())
		if bucketErr != nil || want.VirtualShard() != address.BucketID || want.Domain() != actorrouting.DomainFarm {
			d.rescheduled.Add(1)
			if bucketErr != nil {
				return bucketErr
			}
			return fmt.Errorf("%w: direct source bucket", ErrPayloadInvalid)
		}
	}
	address, err = careWorkAddress(bucket, order.WorkID, payload.GetCareResolutionSnapshot().GetSourceFarmId())
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	invocation, err := identity.ParticipantInvocationID(identity.WorkTypeCareReward, order.WorkID, identity.StepApplyCareReward, 1)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	route, err := d.config.Route(itemCtx, payload.GetCarerPlayerId())
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	request, err := coordinatorRequest(route, address, order, invocation, d.config.ParticipantBudgetMs)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	if d.config.BestEffortNoSourcePersistence {
		if err := attachBestEffortSourceOrder(request, address, order); err != nil {
			d.rescheduled.Add(1)
			return err
		}
	}
	if d.config.CommitProofMode != sourceorder.CommitProofModeOff {
		if err := d.attachCommitProof(itemCtx, request, address, order, canonicalPayloadHash, invocation, string(identity.StepApplyCareReward), commitEvidence); err != nil {
			d.rescheduled.Add(1)
			return err
		}
	}
	response, rpcErr := d.config.Player.ExecuteStep(itemCtx, request)
	if rpcErr != nil {
		d.rescheduled.Add(1)
		return rpcErr
	}
	if err := ValidateTerminalProof(response, order.WorkID, payload); err != nil {
		d.rescheduled.Add(1)
		return err
	}
	if d.config.BestEffortNoSourcePersistence {
		d.completed.Add(1)
		return nil
	}
	synthetic := authority.WorkClaim{WorkID: order.WorkID, PayloadHash: order.PayloadHash}
	completionProof, proofErr := BuildWorkCompletionProof(synthetic, payload, response, order.AuthorityLayoutVersion)
	if proofErr != nil {
		d.rescheduled.Add(1)
		return proofErr
	}
	return d.finishDirect(ctx, bucket, FamilyCareReward, order.WorkID, completionProof)
}

func (d *DirectDispatcher) dispatchHarvest(ctx, itemCtx context.Context, address workidentity.WorkAddress, committed *authority.SourceOrderSnapshot, commitEvidence *sourceorder.DurableCommitEvidence) error {
	d.dispatched.Add(1)
	bucket, err := d.resolveBucket(address)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	family := string(identity.WorkTypeHarvest)
	var order authority.SourceOrderSnapshot
	if committed != nil {
		order = *committed
	} else {
		order, err = d.config.SourceOrders.GetSourceOrder(itemCtx, bucket, family, address.WorkID)
		if err != nil {
			d.rescheduled.Add(1)
			return err
		}
	}
	if order.State == "COMPLETED" {
		d.completed.Add(1)
		return nil
	}
	if order.State != "PENDING" || order.WorkType != family || order.WorkID != address.WorkID ||
		order.PlacementKind != "SOURCE_PERSISTENCE_BUCKET" || order.SubjectPlayerID == 0 ||
		order.OperationID == "" || order.WorkVersion == 0 {
		d.rescheduled.Add(1)
		return fmt.Errorf("%w: harvest source order identity", ErrPayloadInvalid)
	}
	payload, err := sourceorder.DecodeGrantHarvestPayload(order.WorkID, order.TypedPayload)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	canonicalPayloadHash, hashErr := sourceorder.HarvestPayloadHash(payload)
	if hashErr != nil || order.PayloadHash != hex.EncodeToString(canonicalPayloadHash[:]) {
		d.rescheduled.Add(1)
		return fmt.Errorf("%w: harvest payload_hash mismatch", ErrPayloadInvalid)
	}
	if payload.GetPlayerId() != order.SubjectPlayerID {
		d.rescheduled.Add(1)
		return fmt.Errorf("%w: harvest subject binding", ErrPayloadInvalid)
	}
	address, err = sourceorder.WorkAddress(bucket, family, order.WorkID, workidentity.RouteDomainFarm, payload.GetHarvestDecision().GetFarmId())
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	invocation, err := sourceorder.HarvestInvocation(order.WorkID)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	route, err := d.config.Route(itemCtx, payload.GetPlayerId())
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	request, err := harvestCoordinatorRequest(route, address, order, invocation, d.config.ParticipantBudgetMs)
	if err != nil {
		d.rescheduled.Add(1)
		return err
	}
	if d.config.BestEffortNoSourcePersistence {
		if err := attachBestEffortSourceOrder(request, address, order); err != nil {
			d.rescheduled.Add(1)
			return err
		}
	}
	if d.config.CommitProofMode != sourceorder.CommitProofModeOff {
		if err := d.attachCommitProof(itemCtx, request, address, order, canonicalPayloadHash, invocation, string(identity.StepGrantHarvestReward), commitEvidence); err != nil {
			d.rescheduled.Add(1)
			return err
		}
	}
	response, rpcErr := d.config.Player.ExecuteStep(itemCtx, request)
	if rpcErr != nil {
		d.rescheduled.Add(1)
		return rpcErr
	}
	if err := validateHarvestTerminalProof(response, order.WorkID, invocation); err != nil {
		d.rescheduled.Add(1)
		return err
	}
	if d.config.BestEffortNoSourcePersistence {
		d.completed.Add(1)
		return nil
	}
	completionProof, proofErr := buildHarvestCompletionProof(authority.WorkClaim{}, payload, response, order.AuthorityLayoutVersion)
	if proofErr != nil {
		d.rescheduled.Add(1)
		return proofErr
	}
	return d.finishDirect(ctx, bucket, family, order.WorkID, completionProof)
}

func (d *DirectDispatcher) attachCommitProof(ctx context.Context, request *contractspb.ParticipantRequest, address workidentity.WorkAddress, order authority.SourceOrderSnapshot, payloadHash [sha256.Size]byte, invocation, step string, evidence *sourceorder.DurableCommitEvidence) error {
	if d == nil || request == nil {
		return sourceorder.ErrCommitProofInvalid
	}
	switch d.config.CommitProofMode {
	case sourceorder.CommitProofModeOff:
		return nil
	case sourceorder.CommitProofModeShadow, sourceorder.CommitProofModePrefer:
	default:
		return sourceorder.ErrCommitProofBypassUnavailable
	}
	if d.config.CommitProofIssuer == nil || evidence == nil {
		d.proofUnavailable.Add(1)
		if d.config.CommitProofMode == sourceorder.CommitProofModePrefer {
			return sourceorder.ErrCommitProofMissing
		}
		return nil
	}
	proof, err := d.config.CommitProofIssuer.Mint(ctx, sourceorder.CommitProofMintRequest{
		Work: address, WorkVersion: order.WorkVersion, OperationID: order.OperationID,
		SubjectPlayerID: order.SubjectPlayerID, PayloadHash: payloadHash, DueAtMs: order.NextDueAtMs,
		AuthorityLayoutVersion: order.AuthorityLayoutVersion,
		DestinationRouteDomain: workidentity.RouteDomainPlayer, DestinationActorID: order.SubjectPlayerID,
		DestinationStep: step, DestinationInvocationID: invocation, Commit: *evidence,
	})
	if err != nil {
		d.proofUnavailable.Add(1)
		if d.config.CommitProofMode == sourceorder.CommitProofModeShadow {
			return nil
		}
		return err
	}
	wire, err := sourceorder.CommitProofToProto(*proof)
	if err != nil {
		d.proofUnavailable.Add(1)
		if d.config.CommitProofMode == sourceorder.CommitProofModeShadow {
			return nil
		}
		return err
	}
	request.SourceOrderCommitProof = wire
	d.proofAttached.Add(1)
	return nil
}

func harvestCoordinatorRequest(route *logicrpcpb.ActorRouteHeader, address workidentity.WorkAddress, order authority.SourceOrderSnapshot, invocation string, budget uint32) (*contractspb.ParticipantRequest, error) {
	if route == nil || route.GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER || route.GetActorId() != order.SubjectPlayerID {
		return nil, fmt.Errorf("%w: Coordinator PLAYER route", ErrInvalidConfig)
	}
	wire, err := wireCareWorkAddress(address, order.AuthorityLayoutVersion)
	if err != nil {
		return nil, err
	}
	return &contractspb.ParticipantRequest{RouteHeader: route,
		ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD,
		TypedPayload:       append([]byte(nil), order.TypedPayload...),
		CallerContext: &contractspb.ParticipantRequest_Coordinator{Coordinator: &contractspb.CoordinatorWorkContext{
			Work: wire, ExpectedWorkVersion: order.WorkVersion, ParticipantStep: contractspb.ParticipantStep_PARTICIPANT_STEP_GRANT_HARVEST_REWARD,
			ParticipantInvocationId: invocation, OperationId: order.OperationID, SubjectPlayerId: order.SubjectPlayerID, RemainingBudgetMs: budget,
		}},
	}, nil
}

func validateHarvestTerminalProof(response *contractspb.ParticipantResponse, orderID, invocation string) error {
	if response == nil || response.GetParticipantCommand() != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD {
		return fmt.Errorf("%w: harvest command", ErrTerminalProof)
	}
	switch response.GetOutcome() {
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED, contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE:
	default:
		return fmt.Errorf("%w: harvest outcome", ErrTerminalProof)
	}
	if response.GetParticipantInvocationId() != invocation || len(response.GetParticipantResultHash()) != 32 || response.GetCommittedRecordVersion() == 0 {
		return fmt.Errorf("%w: harvest proof fields", ErrTerminalProof)
	}
	return nil
}

func buildHarvestCompletionProof(claim authority.WorkClaim, payload *contractspb.GrantHarvestRewardPayload, response *contractspb.ParticipantResponse, authorityLayoutVersion string) (authority.WorkCompletionProof, error) {
	payloadHash, err := sourceorder.HarvestPayloadHash(payload)
	if err != nil {
		return authority.WorkCompletionProof{}, err
	}
	payloadHashHex := hex.EncodeToString(payloadHash[:])
	outcome := "APPLIED"
	if response.GetOutcome() == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE {
		outcome = "NO_CHANGE"
	}
	proof := authority.WorkCompletionProof{
		SourceAuthorityKind:        "FARM_ACTOR",
		CanonicalSourceAuthorityID: strconv.FormatUint(payload.GetHarvestDecision().GetFarmId(), 10),
		AuthorityLayoutVersion:     authorityLayoutVersion,
		ActorRouteDomain:           "FARM",
		PayloadHash:                payloadHashHex,
		InvocationID:               response.GetParticipantInvocationId(),
		ParticipantStep:            string(identity.StepGrantHarvestReward),
		ParticipantOutcome:         outcome,
		ParticipantResultHash:      hex.EncodeToString(response.GetParticipantResultHash()),
		ParticipantResultVersion:   response.GetCommittedRecordVersion(),
	}
	proof.ResultProofHash = authority.WorkCompletionProofHash(proof)
	return proof, nil
}

func coordinatorRequest(route *logicrpcpb.ActorRouteHeader, address workidentity.WorkAddress, order authority.SourceOrderSnapshot, invocation string, budget uint32) (*contractspb.ParticipantRequest, error) {
	if route == nil || route.GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER || route.GetActorId() != order.SubjectPlayerID {
		return nil, fmt.Errorf("%w: Coordinator PLAYER route", ErrInvalidConfig)
	}
	wire, err := wireCareWorkAddress(address, order.AuthorityLayoutVersion)
	if err != nil {
		return nil, err
	}
	return &contractspb.ParticipantRequest{RouteHeader: route,
		ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_CARE_REWARD,
		TypedPayload:       append([]byte(nil), order.TypedPayload...),
		CallerContext: &contractspb.ParticipantRequest_Coordinator{Coordinator: &contractspb.CoordinatorWorkContext{
			Work: wire, ExpectedWorkVersion: order.WorkVersion, ParticipantStep: contractspb.ParticipantStep_PARTICIPANT_STEP_APPLY_CARE_REWARD,
			ParticipantInvocationId: invocation, OperationId: order.OperationID, SubjectPlayerId: order.SubjectPlayerID, RemainingBudgetMs: budget,
		}},
	}, nil
}

func attachBestEffortSourceOrder(request *contractspb.ParticipantRequest, address workidentity.WorkAddress, order authority.SourceOrderSnapshot) error {
	if request == nil || request.GetCoordinator() == nil || request.GetCoordinator().GetWork() == nil {
		return fmt.Errorf("%w: best-effort Coordinator request", ErrInvalidConfig)
	}
	if order.WorkID == "" || order.WorkID != address.WorkID || order.WorkVersion == 0 ||
		order.OperationID == "" || order.SubjectPlayerID == 0 || order.NextDueAtMs <= 0 ||
		len(order.TypedPayload) == 0 || len(order.PayloadHash) != sha256.Size*2 {
		return fmt.Errorf("%w: incomplete best-effort SourceOrder", ErrPayloadInvalid)
	}
	payloadHash, err := hex.DecodeString(order.PayloadHash)
	if err != nil || len(payloadHash) != sha256.Size {
		return fmt.Errorf("%w: best-effort payload_hash", ErrPayloadInvalid)
	}
	request.BestEffortSourceOrder = &contractspb.CreatedSourceOrderEnvelope{
		Work:            proto.Clone(request.GetCoordinator().GetWork()).(*contractspb.WorkAddress),
		WorkVersion:     order.WorkVersion,
		OperationId:     order.OperationID,
		SubjectPlayerId: order.SubjectPlayerID,
		TypedPayload:    append([]byte(nil), order.TypedPayload...),
		PayloadHash:     payloadHash,
		DueAtMs:         order.NextDueAtMs,
	}
	return nil
}

func (d *DirectDispatcher) resolveBucket(address workidentity.WorkAddress) (rediskey.AuthorityBucketRef, error) {
	if d == nil {
		return rediskey.AuthorityBucketRef{}, fmt.Errorf("%w: direct source bucket resolver is required", ErrInvalidConfig)
	}
	if address.SourceRouteDomain == workidentity.RouteDomainPlayer {
		if d.config.BucketForPlayer == nil {
			return rediskey.AuthorityBucketRef{}, fmt.Errorf("%w: PLAYER source bucket resolver is required", ErrInvalidConfig)
		}
		bucket, err := d.config.BucketForPlayer(address.SourceActorID)
		if err != nil {
			return rediskey.AuthorityBucketRef{}, err
		}
		if bucket.Domain() != actorrouting.DomainPlayer || bucket.VirtualShard() != address.BucketID {
			return rediskey.AuthorityBucketRef{}, fmt.Errorf("%w: direct PLAYER source bucket mismatch", ErrPayloadInvalid)
		}
		return bucket, nil
	}
	if d.config.BucketForFarm == nil {
		return rediskey.AuthorityBucketRef{}, fmt.Errorf("%w: direct source bucket resolver is required", ErrInvalidConfig)
	}
	bucket, err := d.config.BucketForFarm(address.SourceActorID)
	if err != nil {
		return rediskey.AuthorityBucketRef{}, err
	}
	if bucket.Domain() != actorrouting.DomainFarm || bucket.VirtualShard() != address.BucketID {
		return rediskey.AuthorityBucketRef{}, fmt.Errorf("%w: direct source bucket mismatch", ErrPayloadInvalid)
	}
	return bucket, nil
}

func boundedOperationContext(parent context.Context, timeout time.Duration, safeDeadline time.Time, now func() time.Time) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if timeout <= 0 {
		timeout = defaultOperationTimeout
	}
	if now == nil {
		now = time.Now
	}
	deadline := now().Add(timeout)
	if !safeDeadline.IsZero() && safeDeadline.Before(deadline) {
		deadline = safeDeadline
	}
	return context.WithDeadline(parent, deadline)
}

func minDuration(left, right time.Duration) time.Duration {
	if left <= 0 {
		return right
	}
	if right <= 0 || left < right {
		return left
	}
	return right
}

func recoveryRouteProvider(resolver actorrouting.Resolver, realm string) RouteProvider {
	return recoveryRouteProviderFor(resolver, realm, actorrouting.DomainPlayer, logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER)
}

func recoveryFarmRouteProvider(resolver actorrouting.Resolver, realm string) RouteProvider {
	return recoveryRouteProviderFor(resolver, realm, actorrouting.DomainFarm, logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM)
}

func recoveryRouteProviderFor(resolver actorrouting.Resolver, realm string, kind actorrouting.RouteDomain, wire logicrpcpb.RouteDomain) RouteProvider {
	return func(ctx context.Context, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
		if resolver == nil || strings.TrimSpace(realm) == "" || actorID == 0 {
			return nil, fmt.Errorf("%w: route resolver", ErrInvalidConfig)
		}
		assignment, err := resolver.Resolve(ctx, actorrouting.Address{Realm: realm, Kind: kind, ID: actorID})
		if err != nil {
			return nil, err
		}
		if assignment.Endpoint == "" || assignment.HolderPod == "" || assignment.HolderBootID == "" || assignment.ShardRevision == 0 ||
			assignment.WriterEpoch == 0 || assignment.LeaseEpoch == 0 || assignment.LeaseUntilMs <= 0 {
			return nil, fmt.Errorf("%w: incomplete route", actorrouting.ErrStaleRoute)
		}
		leaseUntilMs := uint64(assignment.LeaseUntilMs)
		return &logicrpcpb.ActorRouteHeader{RouteDomain: wire, ActorId: actorID,
			VirtualShard: assignment.ShardID, ShardRevision: assignment.ShardRevision, ExpectedHolderId: assignment.HolderPod,
			ExpectedHolderBootId: assignment.HolderBootID, ExpectedWriterEpoch: assignment.WriterEpoch,
			ExpectedLeaseEpoch: assignment.LeaseEpoch, ExpectedLeaseUntilMs: &leaseUntilMs}, nil
	}
}

// NewFarmRouteProvider exposes the current FARM assignment used by PLANT/UNLOCK dest RPCs.
func NewFarmRouteProvider(resolver actorrouting.Resolver, realm string) (RouteProvider, error) {
	if resolver == nil || strings.TrimSpace(realm) == "" {
		return nil, fmt.Errorf("%w: FARM route resolver and realm are required", ErrInvalidConfig)
	}
	return recoveryFarmRouteProvider(resolver, realm), nil
}

type routedRecoveryClient struct {
	resolver actorrouting.Resolver
	realm    string
	kind     actorrouting.RouteDomain
	wire     logicrpcpb.RouteDomain
	clients  map[string]WorkerRecoveryParticipantClient
}

func newRoutedRecoveryClient(resolver actorrouting.Resolver, realm string, clients map[string]WorkerRecoveryParticipantClient) (*routedRecoveryClient, error) {
	return newRoutedRecoveryClientFor(resolver, realm, actorrouting.DomainPlayer, logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, clients)
}

func newRoutedFarmRecoveryClient(resolver actorrouting.Resolver, realm string, clients map[string]WorkerRecoveryParticipantClient) (*routedRecoveryClient, error) {
	return newRoutedRecoveryClientFor(resolver, realm, actorrouting.DomainFarm, logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, clients)
}

func newRoutedRecoveryClientFor(resolver actorrouting.Resolver, realm string, kind actorrouting.RouteDomain, wire logicrpcpb.RouteDomain, clients map[string]WorkerRecoveryParticipantClient) (*routedRecoveryClient, error) {
	if resolver == nil || strings.TrimSpace(realm) == "" || len(clients) == 0 {
		return nil, fmt.Errorf("%w: routed Recovery client", ErrInvalidConfig)
	}
	copyClients := make(map[string]WorkerRecoveryParticipantClient, len(clients))
	for endpoint, client := range clients {
		if endpoint != "" && client != nil {
			copyClients[endpoint] = client
		}
	}
	if len(copyClients) == 0 {
		return nil, fmt.Errorf("%w: routed Recovery clients are empty", ErrInvalidConfig)
	}
	return &routedRecoveryClient{resolver: resolver, realm: realm, kind: kind, wire: wire, clients: copyClients}, nil
}

func (c *routedRecoveryClient) ResumeStep(ctx context.Context, request *contractspb.ParticipantRequest, options ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	if c == nil || c.resolver == nil || request == nil || request.GetRouteHeader() == nil {
		return nil, fmt.Errorf("%w: routed Recovery request", ErrInvalidConfig)
	}
	header := request.GetRouteHeader()
	if header.GetRouteDomain() != c.wire || header.GetActorId() == 0 {
		return nil, fmt.Errorf("%w: Recovery route domain", ErrInvalidConfig)
	}
	address := actorrouting.Address{Realm: c.realm, Kind: c.kind, ID: header.GetActorId()}
	assignment, err := c.resolver.Resolve(ctx, address)
	if err != nil {
		return nil, err
	}
	if !sameAssignment(header, assignment) {
		return nil, fmt.Errorf("%w: stale PLAYER route", actorrouting.ErrStaleRoute)
	}
	client := c.clients[assignment.Endpoint]
	if client == nil {
		return nil, fmt.Errorf("%w: PLAYER endpoint %s", actorrouting.ErrNoAssignment, assignment.Endpoint)
	}
	return client.ResumeStep(actorrouting.WithRoute(ctx, address, assignment), request, options...)
}

func sameAssignment(header *logicrpcpb.ActorRouteHeader, assignment actorrouting.Assignment) bool {
	return header != nil && header.GetVirtualShard() == assignment.ShardID && header.GetShardRevision() == assignment.ShardRevision &&
		header.GetExpectedHolderId() == assignment.HolderPod && header.GetExpectedHolderBootId() == assignment.HolderBootID &&
		header.GetExpectedWriterEpoch() == assignment.WriterEpoch && actorrouting.CompatibleLeaseProof(
		header.GetExpectedLeaseEpoch(), header.ExpectedLeaseUntilMs, assignment, false,
	)
}
