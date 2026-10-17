package guardrecovery

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/contracts"
	"farmgame/internal/identity"
	authority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	FamilyGuardSync         = string(identity.WorkTypeGuardSyncOrder)
	FamilyGuardSyncOrder    = FamilyGuardSync
	defaultClaimLimit       = 32
	defaultMaxConcurrent    = 4
	defaultOperationTimeout = 3 * time.Second
	defaultLeaseTTL         = 10 * time.Second
	defaultRetryDelay       = 250 * time.Millisecond
	sourceAuthorityKind     = "PLAYER_ACTOR"
	sourceRouteDomain       = "PLAYER"
	placementKind           = "SOURCE_PERSISTENCE_BUCKET"
	participantStep         = "APPLY_GUARD_SNAPSHOT"
	sha256HexLength         = 64
)

var (
	ErrInvalidConfig       = errors.New("guard recovery: invalid configuration")
	ErrLeaseLost           = errors.New("guard recovery: work lease lost")
	ErrTerminalProof       = errors.New("guard recovery: Participant terminal proof is invalid")
	ErrRetryDeferred       = errors.New("guard recovery: durable retry deferred")
	ErrPayloadInvalid      = errors.New("guard recovery: ApplyGuardSnapshotPayload is invalid")
	ErrUnsupportedWorkType = errors.New("guard recovery: unsupported SourceOrder work type")
)

type WorkLease interface {
	ClaimWorkWithBoot(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, string, int64) (authority.WorkClaim, error)
	RenewWorkLeaseWithBoot(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, uint64, string, int64) (authority.WorkLeaseRenewResult, error)
	CompleteWorkWithProof(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, uint64, uint64, string, authority.WorkCompletionProof) (authority.WorkCompleteResult, error)
	RescheduleWork(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, uint64, uint64, string, uint64, int64, string) (authority.WorkRescheduleResult, error)
}

type DueIndexScanner interface {
	ListDueWork(context.Context, rediskey.AuthorityBucketRef, int) ([]authority.DueWorkRef, error)
}

type SourceOrderReader interface {
	GetSourceOrder(context.Context, rediskey.AuthorityBucketRef, string, string) (authority.SourceOrderSnapshot, error)
}

type WorkerRecoveryParticipantClient = contractspb.WorkerRecoveryIngressClient
type RouteProvider func(context.Context, uint64) (*logicrpcpb.ActorRouteHeader, error)

type Config struct {
	AuthorityStore      *authority.Store
	WorkLease           WorkLease
	DueIndex            DueIndexScanner
	SourceOrders        SourceOrderReader
	Buckets             []rediskey.AuthorityBucketRef
	RouteResolver       actorrouting.Resolver
	Realm               string
	Route               RouteProvider
	Farm                WorkerRecoveryParticipantClient
	FarmClients         map[string]WorkerRecoveryParticipantClient
	Owner               string
	Token               string
	BootID              string
	ClaimLimit          int
	MaxConcurrent       int
	LeaseTTL            time.Duration
	SafetyMargin        time.Duration
	OperationTimeout    time.Duration
	ParticipantBudgetMs uint32
	RetryDelay          time.Duration
	Now                 func() time.Time
}

type Composition struct{ Worker *Worker }

type RoundStats struct {
	Buckets, Scanned, Candidates, Claimed, Completed, Rescheduled uint64
	BadPayload, Unknown, Failed, LeaseRenewed, LeaseLost          uint64
}

// DueIndexObservation is the bounded readiness proof for GUARD_SYNC_ORDER.
// Available is false when the configured bound cannot prove that the entire
// GUARD_SYNC subset of the shared PLAYER Source DueIndex was observed.
type DueIndexObservation struct {
	Depth         int64
	OldestDueAtMs int64
	Available     bool
}

type normalizedConfig struct {
	WorkLease                                            WorkLease
	DueIndex                                             DueIndexScanner
	SourceOrders                                         SourceOrderReader
	Buckets                                              []rediskey.AuthorityBucketRef
	Route                                                RouteProvider
	Farm                                                 WorkerRecoveryParticipantClient
	Owner, Token, BootID                                 string
	ClaimLimit, MaxConcurrent                            int
	LeaseTTL, SafetyMargin, OperationTimeout, RetryDelay time.Duration
	ParticipantBudgetMs                                  uint32
	Now                                                  func() time.Time
}

type Worker struct{ config normalizedConfig }
type bucketRound struct {
	stats RoundStats
	err   error
}
type itemResult struct {
	Claimed, Completed, Rescheduled, BadPayload, Unknown, Failed, LeaseRenewed, LeaseLost uint64
	err                                                                                   error
}
type leaseWindow struct {
	safeDeadline time.Time
	renewed      bool
}

func New(config Config) (*Composition, error) {
	worker, err := NewWorker(config)
	if err != nil {
		return nil, err
	}
	return &Composition{Worker: worker}, nil
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
	if config.WorkLease == nil || config.DueIndex == nil ||
		strings.TrimSpace(config.Owner) == "" || strings.TrimSpace(config.Token) == "" ||
		strings.TrimSpace(config.BootID) == "" {
		return nil, fmt.Errorf("%w: lease, due index, owner, token and boot id are required", ErrInvalidConfig)
	}
	if len(config.Buckets) == 0 {
		return nil, fmt.Errorf("%w: at least one PLAYER authority bucket is required", ErrInvalidConfig)
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
		config.ParticipantBudgetMs = contracts.ParticipantMaxBudgetMs
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = defaultRetryDelay
	}

	buckets := make([]rediskey.AuthorityBucketRef, 0, len(config.Buckets))
	seen := make(map[string]struct{}, len(config.Buckets))
	for _, bucket := range config.Buckets {
		if err := bucket.Validate(); err != nil || bucket.Domain() != actorrouting.DomainPlayer {
			return nil, fmt.Errorf("%w: invalid PLAYER bucket: %v", ErrInvalidConfig, err)
		}
		key := bucket.LayoutVersion() + ":" + strconv.FormatUint(uint64(bucket.VirtualShard()), 10)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		buckets = append(buckets, bucket)
	}
	route := config.Route
	if route == nil {
		if config.RouteResolver == nil || strings.TrimSpace(config.Realm) == "" {
			return nil, fmt.Errorf("%w: FARM route resolver is required", ErrInvalidConfig)
		}
		route = farmRouteProvider(config.RouteResolver, config.Realm)
	}
	farm := config.Farm
	if farm == nil {
		if config.RouteResolver == nil || strings.TrimSpace(config.Realm) == "" || len(config.FarmClients) == 0 {
			return nil, fmt.Errorf("%w: FARM WorkerRecovery client is required", ErrInvalidConfig)
		}
		var err error
		farm, err = newRoutedRecoveryClient(config.RouteResolver, config.Realm, config.FarmClients)
		if err != nil {
			return nil, err
		}
	}
	return &Worker{config: normalizedConfig{
		WorkLease: config.WorkLease, DueIndex: config.DueIndex, SourceOrders: config.SourceOrders,
		Buckets: buckets, Route: route, Farm: farm, Owner: config.Owner, Token: config.Token,
		BootID: config.BootID, ClaimLimit: config.ClaimLimit, MaxConcurrent: config.MaxConcurrent,
		LeaseTTL: config.LeaseTTL, SafetyMargin: config.SafetyMargin,
		OperationTimeout: config.OperationTimeout, ParticipantBudgetMs: config.ParticipantBudgetMs,
		RetryDelay: config.RetryDelay, Now: config.Now,
	}}, nil
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

func (w *Worker) PendingDue(ctx context.Context) (int64, error) {
	if w == nil || w.config.DueIndex == nil {
		return 0, fmt.Errorf("%w: incomplete worker", ErrInvalidConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var total int64
	var errs []error
	for _, bucket := range w.config.Buckets {
		refs, err := w.config.DueIndex.ListDueWork(ctx, bucket, w.config.ClaimLimit)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, ref := range refs {
			if ref.Ref.WorkType == FamilyGuardSync {
				total++
			}
		}
	}
	return total, errors.Join(errs...)
}

func (w *Worker) ObserveDue(ctx context.Context, limit int) (DueIndexObservation, error) {
	if w == nil || w.config.DueIndex == nil || limit <= 0 || limit > 4096 {
		return DueIndexObservation{}, fmt.Errorf("%w: due observation limit", ErrInvalidConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	observation := DueIndexObservation{Available: true}
	scanned := 0
	for _, bucket := range w.config.Buckets {
		remaining := limit - scanned
		if remaining <= 0 {
			return DueIndexObservation{}, nil
		}
		// ListDueWork has a hard physical bound of 4096. The previous
		// remaining+1 probe turned the valid readiness default (4096) into
		// 4097 and made GUARD_SYNC permanently unready. At the hard bound we
		// cannot request a sentinel row; treat an exactly-full page as a
		// conservative truncated observation instead.
		queryLimit := remaining + 1
		if queryLimit > 4096 {
			queryLimit = 4096
		}
		refs, err := w.config.DueIndex.ListDueWork(ctx, bucket, queryLimit)
		if err != nil {
			return DueIndexObservation{}, fmt.Errorf("bucket %d: %w", bucket.VirtualShard(), err)
		}
		if len(refs) > remaining || (remaining == 4096 && len(refs) == 4096) {
			return DueIndexObservation{}, nil
		}
		scanned += len(refs)
		for _, ref := range refs {
			if ref.Ref.WorkType != FamilyGuardSync {
				continue
			}
			observation.Depth++
			if observation.OldestDueAtMs == 0 || ref.DueAtMs < observation.OldestDueAtMs {
				observation.OldestDueAtMs = ref.DueAtMs
			}
		}
	}
	return observation, nil
}

func (w *Worker) RunOnce(ctx context.Context) (RoundStats, error) {
	if w == nil || w.config.WorkLease == nil || w.config.DueIndex == nil || w.config.Farm == nil {
		return RoundStats{}, fmt.Errorf("%w: incomplete worker", ErrInvalidConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	sem := make(chan struct{}, w.config.MaxConcurrent)
	results := make(chan bucketRound, len(w.config.Buckets))
	var wg sync.WaitGroup
	for _, bucket := range w.config.Buckets {
		bucket := bucket
		wg.Add(1)
		go func() { defer wg.Done(); results <- w.runBucket(ctx, bucket, sem) }()
	}
	wg.Wait()
	close(results)
	var stats RoundStats
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
		stats.LeaseLost += result.stats.LeaseLost
		if result.err != nil {
			errs = append(errs, result.err)
		}
	}
	return stats, errors.Join(errs...)
}

func (w *Worker) runBucket(ctx context.Context, bucket rediskey.AuthorityBucketRef, sem chan struct{}) bucketRound {
	out := bucketRound{stats: RoundStats{Buckets: 1}}
	refs, err := w.config.DueIndex.ListDueWork(ctx, bucket, w.config.ClaimLimit)
	if err != nil {
		out.err = fmt.Errorf("bucket %d due index: %w", bucket.VirtualShard(), err)
		return out
	}
	out.stats.Scanned = uint64(len(refs))
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, due := range refs {
		if due.Ref.WorkType != FamilyGuardSync {
			continue
		}
		out.stats.Candidates++
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
			out.stats.Claimed += item.Claimed
			out.stats.Completed += item.Completed
			out.stats.Rescheduled += item.Rescheduled
			out.stats.BadPayload += item.BadPayload
			out.stats.Unknown += item.Unknown
			out.stats.Failed += item.Failed
			out.stats.LeaseRenewed += item.LeaseRenewed
			out.stats.LeaseLost += item.LeaseLost
			if item.err != nil {
				out.err = errors.Join(out.err, item.err)
			}
			mu.Unlock()
		}(due)
	}
	wg.Wait()
	return out
}

func (w *Worker) processDue(ctx context.Context, bucket rediskey.AuthorityBucketRef, due authority.DueWorkRef) itemResult {
	var out itemResult
	claimStarted := w.config.Now()
	claimCtx, cancel := context.WithTimeout(ctx, minDuration(w.config.OperationTimeout, time.Second))
	claim, err := w.config.WorkLease.ClaimWorkWithBoot(claimCtx, bucket, FamilyGuardSync, due.Ref.WorkID,
		w.config.Owner, w.config.BootID, w.config.Token, w.config.LeaseTTL.Milliseconds())
	cancel()
	if err != nil {
		out.err = fmt.Errorf("claim %s: %w", due.Ref.WorkID, err)
		return out
	}
	if claim.Status != authority.WorkClaimClaimed {
		return out
	}
	out.Claimed = 1
	payload, err := validateClaimPayload(due.Ref.WorkID, claim)
	if err == nil && w.config.SourceOrders != nil {
		source, readErr := w.config.SourceOrders.GetSourceOrder(ctx, bucket, FamilyGuardSync, claim.WorkID)
		if readErr != nil {
			err = readErr
		} else {
			err = validateSourceOrder(bucket, claim, source)
		}
	}
	if err != nil {
		out.BadPayload, out.Rescheduled = 1, 1
		out.err = w.deferClaim(bucket, claim, claim.RetryCount+1, err)
		return out
	}
	window, err := w.ensureLease(ctx, bucket, claim, claimStarted)
	if err != nil {
		out.Rescheduled = 1
		if errors.Is(err, ErrLeaseLost) {
			out.LeaseLost = 1
		}
		out.err = w.deferClaim(bucket, claim, claim.RetryCount+1, err)
		return out
	}
	if window.renewed {
		out.LeaseRenewed = 1
	}
	address, err := guardWorkAddress(bucket, claim.WorkID, payload.GetSourcePlayerId())
	if err != nil {
		out.BadPayload, out.Rescheduled = 1, 1
		out.err = w.deferClaim(bucket, claim, claim.RetryCount+1, err)
		return out
	}
	invocation, err := identity.ParticipantInvocationID(identity.WorkTypeGuardSyncOrder, claim.WorkID, identity.StepApplyGuardSnapshot, 1)
	if err != nil {
		out.Failed, out.Rescheduled = 1, 1
		out.err = w.deferClaim(bucket, claim, claim.RetryCount+1, err)
		return out
	}
	opCtx, cancelOp := boundedOperationContext(ctx, w.config.OperationTimeout, window.safeDeadline, w.config.Now)
	defer cancelOp()
	route, err := w.config.Route(opCtx, payload.GetTargetFarmId())
	if err != nil {
		out.Failed, out.Rescheduled = 1, 1
		out.err = w.deferClaim(bucket, claim, claim.RetryCount+1, err)
		return out
	}
	request, err := recoveryRequest(route, address, claim, invocation, w.config.Owner, w.config.BootID,
		w.config.Token, w.config.ParticipantBudgetMs, claim.Payload, bucket.LayoutVersion())
	if err != nil {
		out.Failed, out.Rescheduled = 1, 1
		out.err = w.deferClaim(bucket, claim, claim.RetryCount+1, err)
		return out
	}
	if !w.config.Now().Before(window.safeDeadline) {
		out.Rescheduled, out.LeaseLost = 1, 1
		out.err = w.deferClaim(bucket, claim, claim.RetryCount+1, ErrLeaseLost)
		return out
	}
	response, rpcErr := w.config.Farm.ResumeStep(opCtx, request)
	if rpcErr != nil {
		out.Unknown, out.Rescheduled = 1, 1
		out.err = w.deferClaim(bucket, claim, claim.RetryCount+1, rpcErr)
		return out
	}
	if response != nil && response.GetOutcome() == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN {
		out.Unknown, out.Rescheduled = 1, 1
		out.err = w.deferClaim(bucket, claim, claim.RetryCount+1, errors.New("Participant TIMEOUT_UNKNOWN; receipt-first retry required"))
		return out
	}
	if err := ValidateTerminalProof(response, claim.WorkID, payload); err != nil {
		out.Failed, out.Rescheduled = 1, 1
		out.err = w.deferClaim(bucket, claim, claim.RetryCount+1, err)
		return out
	}
	proof, err := BuildWorkCompletionProof(claim, payload, response, bucket.LayoutVersion())
	if err != nil {
		out.Failed, out.Rescheduled = 1, 1
		out.err = w.deferClaim(bucket, claim, claim.RetryCount+1, err)
		return out
	}
	completeCtx, cancelComplete := context.WithTimeout(ctx, minDuration(w.config.OperationTimeout, time.Second))
	complete, completeErr := w.config.WorkLease.CompleteWorkWithProof(completeCtx, bucket, FamilyGuardSync,
		claim.WorkID, w.config.Owner, w.config.BootID, claim.LeaseEpoch, claim.WorkVersion, w.config.Token, proof)
	cancelComplete()
	if completeErr != nil {
		out.Failed = 1
		out.err = fmt.Errorf("complete %s: %w", claim.WorkID, completeErr)
		return out
	}
	switch complete.Status {
	case authority.WorkLeaseCompleted, authority.WorkLeaseAlreadyCompleted:
		out.Completed = 1
	case authority.WorkLeaseFenceMismatch, authority.WorkLeaseExpired, authority.WorkLeaseStaleVersion,
		authority.WorkLeaseStaleGeneration, authority.WorkLeaseNotFound:
		out.LeaseLost = 1
		out.err = fmt.Errorf("%w: complete %s status=%s", ErrLeaseLost, claim.WorkID, complete.Status)
	default:
		out.Failed = 1
		out.err = fmt.Errorf("complete %s status=%s", claim.WorkID, complete.Status)
	}
	return out
}

func validateClaimPayload(workID string, claim authority.WorkClaim) (*contractspb.ApplyGuardSnapshotPayload, error) {
	if claim.WorkID != workID || claim.WorkType != FamilyGuardSync || claim.WorkVersion == 0 ||
		claim.LeaseEpoch == 0 || claim.SubjectPlayerID == 0 || strings.TrimSpace(claim.OperationID) == "" ||
		claim.AcceptedTTLms <= 0 {
		return nil, fmt.Errorf("%w: claimed work identity or lease", ErrPayloadInvalid)
	}
	payload, err := DecodeApplyGuardSnapshotPayload(claim.WorkID, claim.Payload)
	if err != nil {
		return nil, err
	}
	if len(claim.PayloadHash) != sha256HexLength || claim.PayloadHash != ApplyGuardSnapshotPayloadHash(claim.Payload) {
		return nil, fmt.Errorf("%w: SourceOrder payload_hash must match the canonical Participant payload", ErrPayloadInvalid)
	}
	if payload.GetSourcePlayerId() != claim.SubjectPlayerID {
		return nil, fmt.Errorf("%w: subject player mismatch", ErrPayloadInvalid)
	}
	if claim.Generation != 0 && claim.Generation != payload.GetGuardSnapshot().GetGuardVersion() {
		return nil, fmt.Errorf("%w: generation mismatch", ErrPayloadInvalid)
	}
	return payload, nil
}

func validateSourceOrder(bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, source authority.SourceOrderSnapshot) error {
	if source.WorkID != claim.WorkID || source.WorkType != FamilyGuardSync ||
		source.PlacementKind != placementKind || source.BucketVersion != 1 ||
		source.BucketID != bucket.VirtualShard() || source.SourceAuthorityKind != sourceAuthorityKind ||
		source.CanonicalSourceAuthorityID != strconv.FormatUint(claim.SubjectPlayerID, 10) ||
		source.AuthorityLayoutVersion != bucket.LayoutVersion() || source.ActorRouteDomain != sourceRouteDomain ||
		source.SubjectPlayerID != claim.SubjectPlayerID || source.OperationID != claim.OperationID ||
		source.WorkVersion != claim.WorkVersion || source.PayloadHash != claim.PayloadHash ||
		!bytes.Equal(source.TypedPayload, claim.Payload) {
		return fmt.Errorf("%w: SourceOrder projection is not claim-bound", ErrPayloadInvalid)
	}
	if source.State != "" && source.State != "PENDING" && source.State != "REWARD_DISPATCHING" {
		return fmt.Errorf("%w: SourceOrder state %q", ErrPayloadInvalid, source.State)
	}
	if source.CurrentStep != "" && source.CurrentStep != participantStep {
		return fmt.Errorf("%w: SourceOrder step %q", ErrPayloadInvalid, source.CurrentStep)
	}
	if source.LeaseEpoch != 0 && source.LeaseEpoch != claim.LeaseEpoch {
		return fmt.Errorf("%w: SourceOrder lease epoch mismatch", ErrLeaseLost)
	}
	return nil
}

func (w *Worker) ensureLease(ctx context.Context, bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, claimStarted time.Time) (leaseWindow, error) {
	accepted := time.Duration(claim.AcceptedTTLms) * time.Millisecond
	if accepted <= 0 {
		return leaseWindow{}, fmt.Errorf("%w: claim returned no accepted TTL", ErrLeaseLost)
	}
	window := leaseWindow{safeDeadline: claimStarted.Add(accepted - w.config.SafetyMargin)}
	need := w.config.OperationTimeout + w.config.SafetyMargin
	now := w.config.Now()
	if now.Before(window.safeDeadline) && window.safeDeadline.Sub(now) >= need {
		return window, nil
	}
	renewStarted := now
	renewCtx, cancel := context.WithTimeout(ctx, minDuration(w.config.OperationTimeout, time.Second))
	renew, err := w.config.WorkLease.RenewWorkLeaseWithBoot(renewCtx, bucket, FamilyGuardSync, claim.WorkID,
		w.config.Owner, w.config.BootID, claim.LeaseEpoch, w.config.Token, w.config.LeaseTTL.Milliseconds())
	cancel()
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
	window = leaseWindow{safeDeadline: renewStarted.Add(accepted - w.config.SafetyMargin), renewed: true}
	now = w.config.Now()
	if !now.Before(window.safeDeadline) || window.safeDeadline.Sub(now) < need {
		return leaseWindow{}, fmt.Errorf("%w: lease safety margin leaves no operation budget", ErrLeaseLost)
	}
	return window, nil
}

func (w *Worker) deferClaim(bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, attempt uint64, cause error) error {
	if cause == nil {
		cause = ErrRetryDeferred
	}
	delay := w.config.RetryDelay
	if delay <= 0 {
		delay = defaultRetryDelay
	}
	delayMs := delay.Milliseconds()
	if delayMs < 1 {
		delayMs = 1
	}
	retryCtx, cancel := context.WithTimeout(context.Background(), minDuration(w.config.OperationTimeout, time.Second))
	result, err := w.config.WorkLease.RescheduleWork(retryCtx, bucket, FamilyGuardSync, claim.WorkID,
		w.config.Owner, w.config.BootID, claim.LeaseEpoch, claim.WorkVersion, w.config.Token,
		attempt, delayMs, errorClass(cause))
	cancel()
	if err != nil {
		return fmt.Errorf("%w: %v; reschedule: %v", ErrRetryDeferred, cause, err)
	}
	if result.Status != authority.WorkLeaseRenewed && result.Status != authority.WorkLeaseReconcileRequired {
		return fmt.Errorf("%w: %v; reschedule status=%s", ErrRetryDeferred, cause, result.Status)
	}
	return fmt.Errorf("%w: %v", ErrRetryDeferred, cause)
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

func ValidateTerminalProof(response *contractspb.ParticipantResponse, orderID string, payload *contractspb.ApplyGuardSnapshotPayload) error {
	if response == nil || response.GetParticipantCommand() != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT {
		return fmt.Errorf("%w: response command", ErrTerminalProof)
	}
	wantInvocation, err := identity.ParticipantInvocationID(identity.WorkTypeGuardSyncOrder, orderID, identity.StepApplyGuardSnapshot, 1)
	if err != nil || response.GetParticipantInvocationId() != wantInvocation {
		return fmt.Errorf("%w: response invocation", ErrTerminalProof)
	}
	if response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED &&
		response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE {
		return fmt.Errorf("%w: non-terminal or rejected outcome %s", ErrTerminalProof, response.GetOutcome())
	}
	if response.GetFailure() != nil || response.GetCommittedRecordVersion() == 0 ||
		len(response.GetTypedResult()) == 0 || len(response.GetParticipantResultHash()) != 32 {
		return fmt.Errorf("%w: missing committed version/result/hash", ErrTerminalProof)
	}
	spec, err := contracts.LookupParticipantCommand(response.GetParticipantCommand())
	if err != nil {
		return fmt.Errorf("%w: command spec: %v", ErrTerminalProof, err)
	}
	result := spec.NewResult().(*contractspb.ApplyGuardSnapshotResult)
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(response.GetTypedResult(), result); err != nil {
		return fmt.Errorf("%w: typed result decode: %v", ErrTerminalProof, err)
	}
	if len(result.ProtoReflect().GetUnknown()) != 0 {
		return fmt.Errorf("%w: typed result contains unknown fields", ErrTerminalProof)
	}
	deterministic, err := (proto.MarshalOptions{Deterministic: true}).Marshal(result)
	if err != nil || !bytes.Equal(deterministic, response.GetTypedResult()) {
		return fmt.Errorf("%w: typed result is not deterministic protobuf", ErrTerminalProof)
	}
	resultHash, err := contracts.CanonicalParticipantHash(spec, result)
	if err != nil || !bytes.Equal(resultHash[:], response.GetParticipantResultHash()) {
		return fmt.Errorf("%w: result hash", ErrTerminalProof)
	}
	if payload == nil || payload.GetGuardSnapshot() == nil ||
		result.GetGuardVersion() != payload.GetGuardSnapshot().GetGuardVersion() ||
		result.GetCommittedRecordVersion() != response.GetCommittedRecordVersion() ||
		len(result.GetSnapshotHash()) != 32 || !bytes.Equal(result.GetSnapshotHash(), payload.GetSnapshotHash()) {
		return fmt.Errorf("%w: result identity/version", ErrTerminalProof)
	}
	switch result.GetApplyStatus() {
	case contractspb.GuardApplyStatus_GUARD_APPLY_STATUS_APPLIED:
		if response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED {
			return fmt.Errorf("%w: APPLIED status/outcome mismatch", ErrTerminalProof)
		}
	case contractspb.GuardApplyStatus_GUARD_APPLY_STATUS_SUPERSEDED:
		if response.GetOutcome() != contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE {
			return fmt.Errorf("%w: SUPERSEDED status/outcome mismatch", ErrTerminalProof)
		}
	default:
		return fmt.Errorf("%w: result apply status", ErrTerminalProof)
	}
	return nil
}

func BuildWorkCompletionProof(claim authority.WorkClaim, payload *contractspb.ApplyGuardSnapshotPayload, response *contractspb.ParticipantResponse, layout string) (authority.WorkCompletionProof, error) {
	if err := ValidateTerminalProof(response, claim.WorkID, payload); err != nil {
		return authority.WorkCompletionProof{}, err
	}
	if len(claim.PayloadHash) != sha256HexLength || !isLowerHexDigest(claim.PayloadHash) {
		return authority.WorkCompletionProof{}, fmt.Errorf("%w: raw payload hash", ErrPayloadInvalid)
	}
	outcome := "APPLIED"
	if response.GetOutcome() == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE {
		outcome = "NO_CHANGE"
	}
	proof := authority.WorkCompletionProof{
		SourceAuthorityKind:        sourceAuthorityKind,
		CanonicalSourceAuthorityID: strconv.FormatUint(payload.GetSourcePlayerId(), 10),
		AuthorityLayoutVersion:     layout, ActorRouteDomain: sourceRouteDomain,
		PayloadHash: claim.PayloadHash, InvocationID: response.GetParticipantInvocationId(),
		ParticipantStep: participantStep, ParticipantOutcome: outcome,
		ParticipantResultHash:    hex.EncodeToString(response.GetParticipantResultHash()),
		ParticipantResultVersion: response.GetCommittedRecordVersion(),
	}
	proof.ResultProofHash = authority.WorkCompletionProofHash(proof)
	return proof, nil
}

func isLowerHexDigest(value string) bool {
	if len(value) != sha256HexLength || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func guardWorkAddress(bucket rediskey.AuthorityBucketRef, orderID string, sourcePlayerID uint64) (workidentity.WorkAddress, error) {
	address := workidentity.WorkAddress{WorkID: orderID, WorkType: FamilyGuardSync,
		PlacementKind: workidentity.SourcePersistenceBucket, BucketVersion: 1,
		BucketID: bucket.VirtualShard(), SourceRouteDomain: workidentity.RouteDomainPlayer, SourceActorID: sourcePlayerID}
	if err := address.ValidateShape(); err != nil {
		return workidentity.WorkAddress{}, err
	}
	return address, nil
}

func GuardWorkAddress(bucket rediskey.AuthorityBucketRef, orderID string, sourcePlayerID uint64) (workidentity.WorkAddress, error) {
	return guardWorkAddress(bucket, orderID, sourcePlayerID)
}

func wireGuardWorkAddress(address workidentity.WorkAddress, layout string) (*contractspb.WorkAddress, error) {
	if err := address.ValidateShape(); err != nil {
		return nil, err
	}
	if address.WorkType != FamilyGuardSync || address.BucketVersion != 1 {
		return nil, fmt.Errorf("%w: Guard source work address", ErrPayloadInvalid)
	}
	if strings.TrimSpace(layout) == "" {
		return nil, fmt.Errorf("%w: authority layout version", ErrInvalidConfig)
	}
	workType, ok := contractspb.WorkType_value["WORK_TYPE_"+address.WorkType]
	if !ok {
		return nil, ErrUnsupportedWorkType
	}
	domain := logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER
	return &contractspb.WorkAddress{
		WorkId: address.WorkID, WorkType: contractspb.WorkType(workType),
		PlacementKind: contractspb.WorkPlacementKind_WORK_PLACEMENT_KIND_SOURCE_PERSISTENCE_BUCKET,
		BucketVersion: address.BucketVersion, BucketId: address.BucketID,
		SourceAuthority: &contractspb.SourceAuthorityRef{
			AuthorityKind:          contractspb.SourceAuthorityKind_SOURCE_AUTHORITY_KIND_PLAYER_ACTOR,
			CanonicalAuthorityId:   strconv.FormatUint(address.SourceActorID, 10),
			AuthorityLayoutVersion: layout, ActorRouteDomain: &domain,
		},
	}, nil
}

func recoveryRequest(route *logicrpcpb.ActorRouteHeader, address workidentity.WorkAddress, claim authority.WorkClaim, invocation, callerID, bootID, token string, budget uint32, payload []byte, layout string) (*contractspb.ParticipantRequest, error) {
	if route == nil || route.GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM ||
		route.GetActorId() == 0 || route.GetActorId() != claim.SubjectPlayerID {
		return nil, fmt.Errorf("%w: FARM route does not match target", ErrInvalidConfig)
	}
	wire, err := wireGuardWorkAddress(address, layout)
	if err != nil {
		return nil, err
	}
	return &contractspb.ParticipantRequest{
		RouteHeader:        route,
		ParticipantCommand: contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT,
		TypedPayload:       append([]byte(nil), payload...),
		CallerContext: &contractspb.ParticipantRequest_Recovery{Recovery: &contractspb.InternalCallerContext{
			CallerType:       contractspb.InternalCallerType_INTERNAL_CALLER_TYPE_WORKER_RECOVERY,
			CallerInstanceId: callerID, CallerBootId: bootID, Work: wire,
			ExpectedWorkVersion:     claim.WorkVersion,
			ParticipantStep:         contractspb.ParticipantStep_PARTICIPANT_STEP_APPLY_GUARD_SNAPSHOT,
			ParticipantInvocationId: invocation, LeaseOwnerToken: token,
			LeaseEpoch: claim.LeaseEpoch, RemainingBudgetMs: budget,
		}},
	}, nil
}

func NewFarmRouteProvider(resolver actorrouting.Resolver, realm string) (RouteProvider, error) {
	if resolver == nil || strings.TrimSpace(realm) == "" {
		return nil, fmt.Errorf("%w: FARM route resolver and realm are required", ErrInvalidConfig)
	}
	return farmRouteProvider(resolver, realm), nil
}

func farmRouteProvider(resolver actorrouting.Resolver, realm string) RouteProvider {
	return func(ctx context.Context, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
		if resolver == nil || strings.TrimSpace(realm) == "" || actorID == 0 {
			return nil, fmt.Errorf("%w: FARM route", ErrInvalidConfig)
		}
		assignment, err := resolver.Resolve(ctx, actorrouting.Address{Realm: realm, Kind: actorrouting.DomainFarm, ID: actorID})
		if err != nil {
			return nil, err
		}
		if assignment.Kind != "" && assignment.Kind != actorrouting.DomainFarm {
			return nil, fmt.Errorf("%w: route domain %s", actorrouting.ErrStaleRoute, assignment.Kind)
		}
		if assignment.Endpoint == "" || assignment.HolderPod == "" || assignment.HolderBootID == "" ||
			assignment.ShardRevision == 0 || assignment.WriterEpoch == 0 || assignment.LeaseEpoch == 0 || assignment.LeaseUntilMs <= 0 {
			return nil, fmt.Errorf("%w: incomplete FARM route", actorrouting.ErrStaleRoute)
		}
		leaseUntilMs := uint64(assignment.LeaseUntilMs)
		return &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM,
			ActorId: actorID, VirtualShard: assignment.ShardID, ShardRevision: assignment.ShardRevision,
			ExpectedHolderId: assignment.HolderPod, ExpectedHolderBootId: assignment.HolderBootID,
			ExpectedWriterEpoch: assignment.WriterEpoch, ExpectedLeaseEpoch: assignment.LeaseEpoch,
			ExpectedLeaseUntilMs: &leaseUntilMs}, nil
	}
}

type routedRecoveryClient struct {
	resolver actorrouting.Resolver
	realm    string
	clients  map[string]WorkerRecoveryParticipantClient
}

func newRoutedRecoveryClient(resolver actorrouting.Resolver, realm string, clients map[string]WorkerRecoveryParticipantClient) (*routedRecoveryClient, error) {
	if resolver == nil || strings.TrimSpace(realm) == "" || len(clients) == 0 {
		return nil, fmt.Errorf("%w: routed FARM Recovery client", ErrInvalidConfig)
	}
	copyClients := make(map[string]WorkerRecoveryParticipantClient, len(clients))
	for endpoint, client := range clients {
		if strings.TrimSpace(endpoint) != "" && client != nil {
			copyClients[endpoint] = client
		}
	}
	if len(copyClients) == 0 {
		return nil, fmt.Errorf("%w: routed FARM Recovery clients are empty", ErrInvalidConfig)
	}
	return &routedRecoveryClient{resolver: resolver, realm: realm, clients: copyClients}, nil
}

func (c *routedRecoveryClient) ResumeStep(ctx context.Context, request *contractspb.ParticipantRequest, options ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	if c == nil || c.resolver == nil || request == nil || request.GetRouteHeader() == nil {
		return nil, fmt.Errorf("%w: routed Recovery request", ErrInvalidConfig)
	}
	header := request.GetRouteHeader()
	if header.GetRouteDomain() != logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM || header.GetActorId() == 0 {
		return nil, fmt.Errorf("%w: Recovery route domain", ErrInvalidConfig)
	}
	address := actorrouting.Address{Realm: c.realm, Kind: actorrouting.DomainFarm, ID: header.GetActorId()}
	assignment, err := c.resolver.Resolve(ctx, address)
	if err != nil {
		return nil, err
	}
	if !sameAssignment(header, assignment) {
		return nil, fmt.Errorf("%w: stale FARM route", actorrouting.ErrStaleRoute)
	}
	client := c.clients[assignment.Endpoint]
	if client == nil {
		return nil, fmt.Errorf("%w: FARM endpoint %s", actorrouting.ErrNoAssignment, assignment.Endpoint)
	}
	return client.ResumeStep(actorrouting.WithRoute(ctx, address, assignment), request, options...)
}

func sameAssignment(header *logicrpcpb.ActorRouteHeader, assignment actorrouting.Assignment) bool {
	return header != nil && header.GetVirtualShard() == assignment.ShardID &&
		header.GetShardRevision() == assignment.ShardRevision &&
		header.GetExpectedHolderId() == assignment.HolderPod &&
		header.GetExpectedHolderBootId() == assignment.HolderBootID &&
		header.GetExpectedWriterEpoch() == assignment.WriterEpoch &&
		compatibleLeaseProof(header, assignment)
}

func compatibleLeaseProof(header *logicrpcpb.ActorRouteHeader, assignment actorrouting.Assignment) bool {
	hasEpoch := header.GetExpectedLeaseEpoch() != 0
	hasDeadline := header.ExpectedLeaseUntilMs != nil
	if !hasEpoch && !hasDeadline {
		return true
	}
	return hasEpoch && hasDeadline && header.GetExpectedLeaseUntilMs() != 0 &&
		assignment.LeaseEpoch != 0 && assignment.LeaseUntilMs > 0 &&
		header.GetExpectedLeaseEpoch() == assignment.LeaseEpoch &&
		uint64(assignment.LeaseUntilMs) >= header.GetExpectedLeaseUntilMs()
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
