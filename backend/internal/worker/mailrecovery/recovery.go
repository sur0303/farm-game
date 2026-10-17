// Package mailrecovery consumes the PLAYER Authority source-local
// MAIL_CLAIM_PROJECTION_ORDER. It only advances the rebuildable mailbox
// projection; it never grants assets or activates PlayerActor.
package mailrecovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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
	// FamilyMailClaimProjection is the registry-frozen SourceOrder work type.
	FamilyMailClaimProjection = string(identity.WorkTypeMailClaimProjectionOrder)
	// FamilyMailClaimProjectionOrder is a descriptive compatibility alias.
	FamilyMailClaimProjectionOrder = FamilyMailClaimProjection

	defaultClaimLimit       = 32
	defaultMaxConcurrent    = 4
	defaultOperationTimeout = 3 * time.Second
	defaultLeaseTTL         = 10 * time.Second
	defaultRetryDelay       = 250 * time.Millisecond

	mailSourceKind     = "PLAYER_ACTOR"
	mailRouteDomain    = "PLAYER"
	mailPlacementKind  = "SOURCE_PERSISTENCE_BUCKET"
	mailProjectionStep = "APPLY_MAIL_CLAIM_PROJECTION"
)

var (
	ErrInvalidConfig  = errors.New("mail recovery: invalid configuration")
	ErrLeaseLost      = errors.New("mail recovery: work lease lost")
	ErrPayloadInvalid = errors.New("mail recovery: ApplyMailClaimProjection payload is invalid")
	ErrRetryDeferred  = errors.New("mail recovery: durable retry deferred")
)

// WorkLease is the existing Authority Work Lease surface. Every terminal
// projection result is completed with the exact claim epoch/version fence.
type WorkLease interface {
	ClaimWorkWithBoot(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, string, int64) (authority.WorkClaim, error)
	RenewWorkLeaseWithBoot(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, uint64, string, int64) (authority.WorkLeaseRenewResult, error)
	CompleteWorkWithProof(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, uint64, uint64, string, authority.WorkCompletionProof) (authority.WorkCompleteResult, error)
	RescheduleWork(context.Context, rediskey.AuthorityBucketRef, string, string, string, string, uint64, uint64, string, uint64, int64, string) (authority.WorkRescheduleResult, error)
}

// DueIndexScanner reads the existing PLAYER Authority Source DueIndex.
type DueIndexScanner interface {
	ListDueWork(context.Context, rediskey.AuthorityBucketRef, int) ([]authority.DueWorkRef, error)
}

// SourceOrderReader is optional for tests and composition roots that already
// have a claimed SourceOrder snapshot. Production AuthorityStore wiring uses
// the existing WorkLeaseStore reader so completion proof fields are bound to
// the durable source record rather than to caller input.
type SourceOrderReader interface {
	GetSourceOrder(context.Context, rediskey.AuthorityBucketRef, string, string) (authority.SourceOrderSnapshot, error)
}

type Config struct {
	AuthorityStore *authority.Store
	WorkLease      WorkLease
	DueIndex       DueIndexScanner
	SourceOrders   SourceOrderReader
	MailEntries    mail.MailEntryRepository

	// Buckets must be a bounded list of PLAYER Authority buckets from the same
	// physical route/hash snapshot as the AuthorityStore.
	Buckets []rediskey.AuthorityBucketRef

	Owner  string
	Token  string
	BootID string

	ClaimLimit       int
	MaxConcurrent    int
	LeaseTTL         time.Duration
	SafetyMargin     time.Duration
	OperationTimeout time.Duration
	RetryDelay       time.Duration
	Now              func() time.Time
}

type RoundStats struct {
	Buckets      uint64
	Scanned      uint64
	Candidates   uint64
	Claimed      uint64
	Applied      uint64
	NoChange     uint64
	Purged       uint64
	Completed    uint64
	Rescheduled  uint64
	BadPayload   uint64
	Unknown      uint64
	Failed       uint64
	LeaseRenewed uint64
	LeaseLost    uint64
}

// DueIndexObservation is the bounded readiness proof for this worker family.
// Available is false when the configured bound is too small to prove the
// complete MAIL subset of the shared PLAYER Source DueIndex.
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
	Buckets          []rediskey.AuthorityBucketRef
	Owner            string
	Token            string
	BootID           string
	ClaimLimit       int
	MaxConcurrent    int
	LeaseTTL         time.Duration
	SafetyMargin     time.Duration
	OperationTimeout time.Duration
	RetryDelay       time.Duration
	Now              func() time.Time
}

// Worker is a bounded, restart-safe SourceOrder consumer. It does not own an
// actor route or invoke any PlayerActor RPC.
type Worker struct {
	config normalizedConfig
}

type bucketRound struct {
	stats RoundStats
	err   error
}

type itemResult struct {
	Claimed, Applied, NoChange, Purged, Completed, Rescheduled, BadPayload, Unknown, Failed, LeaseRenewed, LeaseLost uint64
	err                                                                                                              error
}

type leaseWindow struct {
	safeDeadline time.Time
}

type proofBinding struct {
	sourceKind string
	sourceID   string
	layout     string
	route      string
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
	if config.WorkLease == nil || config.DueIndex == nil || config.MailEntries == nil ||
		strings.TrimSpace(config.Owner) == "" || strings.TrimSpace(config.Token) == "" || strings.TrimSpace(config.BootID) == "" {
		return nil, fmt.Errorf("%w: lease, due index, mail repository and worker identity are required", ErrInvalidConfig)
	}
	if len(config.Buckets) == 0 {
		return nil, fmt.Errorf("%w: at least one PLAYER Authority bucket is required", ErrInvalidConfig)
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
		if err := bucket.Validate(); err != nil || bucket.Domain() != actorrouting.DomainPlayer {
			return nil, fmt.Errorf("%w: PLAYER Authority bucket: %v", ErrInvalidConfig, err)
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
	return &Worker{config: normalizedConfig{
		WorkLease: config.WorkLease, DueIndex: config.DueIndex, SourceOrders: config.SourceOrders, MailEntries: config.MailEntries,
		Buckets: buckets, Owner: config.Owner, Token: config.Token, BootID: config.BootID,
		ClaimLimit: config.ClaimLimit, MaxConcurrent: config.MaxConcurrent, LeaseTTL: config.LeaseTTL,
		SafetyMargin: config.SafetyMargin, OperationTimeout: config.OperationTimeout, RetryDelay: config.RetryDelay, Now: config.Now,
	}}, nil
}

// RunOnce performs a bounded scan of each configured bucket. WorkLease
// fencing, rather than discovery ownership, decides which worker may mutate a
// given SourceOrder.
func (w *Worker) RunOnce(ctx context.Context) (RoundStats, error) {
	if w == nil || w.config.WorkLease == nil || w.config.DueIndex == nil || w.config.MailEntries == nil {
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
		go func() {
			defer wg.Done()
			results <- w.runBucket(ctx, bucket, sem)
		}()
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
		stats.Applied += result.stats.Applied
		stats.NoChange += result.stats.NoChange
		stats.Purged += result.stats.Purged
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
			if ref.Ref.WorkType == FamilyMailClaimProjection {
				total++
			}
		}
	}
	return total, errors.Join(errs...)
}

// ObserveDue returns a bounded depth and oldest-due proof across all PLAYER
// buckets. Reading limit+1 makes truncation explicit instead of reporting a
// deceptively healthy partial count.
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
		// ListDueWork has a hard physical bound of 4096. A raw remaining+1
		// probe turns the valid readiness default (4096) into 4097 and makes
		// MAIL claim projection permanently unready. At the hard bound we
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
			if ref.Ref.WorkType != FamilyMailClaimProjection {
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
		if due.Ref.WorkType != FamilyMailClaimProjection {
			continue
		}
		mu.Lock()
		result.stats.Candidates++
		mu.Unlock()
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
			result.stats.Applied += item.Applied
			result.stats.NoChange += item.NoChange
			result.stats.Purged += item.Purged
			result.stats.Completed += item.Completed
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
	claim, err := w.config.WorkLease.ClaimWorkWithBoot(claimCtx, bucket, FamilyMailClaimProjection, due.Ref.WorkID,
		w.config.Owner, w.config.BootID, w.config.Token, w.config.LeaseTTL.Milliseconds())
	claimCancel()
	if err != nil {
		out.err = fmt.Errorf("claim %s: %w", due.Ref.WorkID, err)
		return out
	}
	if claim.Status != authority.WorkClaimClaimed {
		return out
	}
	out.Claimed = 1

	payload, err := validateClaimPayload(due, claim)
	if err != nil {
		out.BadPayload = 1
		out.err = w.retry(ctx, bucket, claim, err)
		out.Rescheduled = 1
		return out
	}

	binding := proofBinding{sourceKind: mailSourceKind, sourceID: strconv.FormatUint(payload.RecipientPlayerID, 10), layout: bucket.LayoutVersion(), route: mailRouteDomain}
	if w.config.SourceOrders != nil {
		readCtx, readCancel := operationContext(ctx, w.config.OperationTimeout, time.Time{})
		source, readErr := w.config.SourceOrders.GetSourceOrder(readCtx, bucket, FamilyMailClaimProjection, claim.WorkID)
		readCancel()
		if readErr != nil {
			out.Failed = 1
			out.err = w.retry(ctx, bucket, claim, readErr)
			out.Rescheduled = 1
			return out
		}
		binding, err = validateSourceOrder(bucket, claim, payload, source)
		if err != nil {
			out.BadPayload = 1
			out.err = w.retry(ctx, bucket, claim, err)
			out.Rescheduled = 1
			return out
		}
	}

	window, err := w.ensureLease(ctx, bucket, claim, claimStarted)
	if err != nil {
		if errors.Is(err, ErrLeaseLost) {
			out.LeaseLost = 1
		}
		out.err = w.retry(ctx, bucket, claim, err)
		out.Rescheduled = 1
		return out
	}
	if window.safeDeadline.IsZero() {
		out.LeaseLost = 1
		out.err = w.retry(ctx, bucket, claim, ErrLeaseLost)
		out.Rescheduled = 1
		return out
	}

	projectionOrder := mail.ClaimProjectionOrder{
		RecipientPlayerID: payload.RecipientPlayerID,
		WorkID:            claim.WorkID,
		WorkVersion:       claim.WorkVersion,
		MailID:            payload.MailID,
		ClaimReceiptRef:   payload.ClaimReceiptRef,
		ClaimMaterialHash: payload.ClaimMaterialHash,
		ClaimedAtMs:       payload.ClaimedAtMs,
	}
	projectionCtx, projectionCancel := operationContext(ctx, w.config.OperationTimeout, window.safeDeadline)
	projection, projectionErr := w.config.MailEntries.ApplyMailClaimProjection(projectionCtx, projectionOrder)
	projectionContextErr := projectionCtx.Err()
	projectionCancel()
	if projectionErr != nil || projectionContextErr != nil {
		out.Failed = 1
		if projectionContextErr != nil && projectionErr == nil {
			projectionErr = projectionContextErr
		}
		out.err = w.retry(ctx, bucket, claim, projectionErr)
		out.Rescheduled = 1
		return out
	}

	var terminalOutcome string
	switch projection.Status {
	case mail.ProjectionApplied:
		out.Applied = 1
		terminalOutcome = string(mail.ProjectionApplied)
	case mail.ProjectionNoChange:
		out.NoChange = 1
		terminalOutcome = string(mail.ProjectionNoChange)
	case mail.ProjectionTargetAlreadyPurged:
		out.Purged = 1
		terminalOutcome = string(mail.ProjectionTargetAlreadyPurged)
	case mail.ProjectionTimeoutUnknown:
		out.Unknown = 1
		out.err = w.retry(ctx, bucket, claim, errors.New("mail projection TIMEOUT_UNKNOWN"))
		out.Rescheduled = 1
		return out
	case mail.ProjectionUnavailableNotSent:
		out.Failed = 1
		out.err = w.retry(ctx, bucket, claim, errors.New("mail projection UNAVAILABLE_NOT_SENT"))
		out.Rescheduled = 1
		return out
	case mail.ProjectionHashMismatch:
		out.BadPayload = 1
		out.err = w.retry(ctx, bucket, claim, fmt.Errorf("%w: repository claim material hash mismatch", ErrPayloadInvalid))
		out.Rescheduled = 1
		return out
	default:
		out.Failed = 1
		out.err = w.retry(ctx, bucket, claim, fmt.Errorf("mail projection: unsupported result status %q", projection.Status))
		out.Rescheduled = 1
		return out
	}

	if !w.config.Now().Before(window.safeDeadline) {
		out.LeaseLost = 1
		out.err = w.retry(ctx, bucket, claim, ErrLeaseLost)
		out.Rescheduled = 1
		return out
	}
	proof, proofErr := buildCompletionProof(claim, payload, binding, terminalOutcome)
	if proofErr != nil {
		out.Failed = 1
		out.err = w.retry(ctx, bucket, claim, proofErr)
		out.Rescheduled = 1
		return out
	}
	completeCtx, completeCancel := operationContext(ctx, w.config.OperationTimeout, window.safeDeadline)
	complete, completeErr := w.config.WorkLease.CompleteWorkWithProof(completeCtx, bucket, FamilyMailClaimProjection,
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
	case authority.WorkLeaseFenceMismatch, authority.WorkLeaseExpired, authority.WorkLeaseStaleVersion, authority.WorkLeaseStaleGeneration:
		out.LeaseLost = 1
		out.err = fmt.Errorf("%w: complete %s status=%s", ErrLeaseLost, claim.WorkID, complete.Status)
	default:
		out.Failed = 1
		out.err = fmt.Errorf("complete %s: status=%s", claim.WorkID, complete.Status)
	}
	return out
}

func validateClaimPayload(due authority.DueWorkRef, claim authority.WorkClaim) (MailClaimProjectionPayload, error) {
	if due.Ref.WorkType != FamilyMailClaimProjection || workidentity.ValidateStableOpaqueID(due.Ref.WorkID) != nil ||
		claim.WorkID != due.Ref.WorkID || claim.WorkType != FamilyMailClaimProjection || claim.SubjectPlayerID == 0 ||
		claim.OperationID == "" || claim.WorkVersion == 0 || claim.LeaseEpoch == 0 || claim.AcceptedTTLms <= 0 || len(claim.Payload) == 0 {
		return MailClaimProjectionPayload{}, fmt.Errorf("%w: claimed SourceOrder identity", ErrPayloadInvalid)
	}
	payload, err := DecodeMailClaimProjectionPayload(claim.Payload)
	if err != nil {
		return MailClaimProjectionPayload{}, err
	}
	if payload.RecipientPlayerID != claim.SubjectPlayerID {
		return MailClaimProjectionPayload{}, fmt.Errorf("%w: recipient/subject mismatch", ErrPayloadInvalid)
	}
	wantHash := MailClaimProjectionPayloadHash(payload)
	if err := mail.ValidateHash(claim.PayloadHash, false); err != nil || claim.PayloadHash != wantHash {
		return MailClaimProjectionPayload{}, fmt.Errorf("%w: SourceOrder payload_hash mismatch", ErrPayloadInvalid)
	}
	return payload, nil
}

func validateSourceOrder(bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, payload MailClaimProjectionPayload, source authority.SourceOrderSnapshot) (proofBinding, error) {
	wantID := strconv.FormatUint(payload.RecipientPlayerID, 10)
	if source.WorkID != claim.WorkID || source.WorkType != FamilyMailClaimProjection || source.PlacementKind != mailPlacementKind ||
		source.BucketVersion != 1 || source.BucketID != bucket.VirtualShard() || source.SourceAuthorityKind != mailSourceKind ||
		source.CanonicalSourceAuthorityID != wantID || source.AuthorityLayoutVersion != bucket.LayoutVersion() || source.ActorRouteDomain != mailRouteDomain ||
		source.SubjectPlayerID != payload.RecipientPlayerID || source.WorkVersion != claim.WorkVersion || source.OperationID == "" ||
		source.PayloadHash != claim.PayloadHash || !bytes.Equal(source.TypedPayload, claim.Payload) ||
		(source.State != "PENDING" && source.State != "REWARD_DISPATCHING") {
		return proofBinding{}, fmt.Errorf("%w: SourceOrder binding", ErrPayloadInvalid)
	}
	if source.LeaseEpoch != 0 && source.LeaseEpoch != claim.LeaseEpoch {
		return proofBinding{}, fmt.Errorf("%w: SourceOrder lease epoch", ErrLeaseLost)
	}
	return proofBinding{sourceKind: source.SourceAuthorityKind, sourceID: source.CanonicalSourceAuthorityID,
		layout: source.AuthorityLayoutVersion, route: source.ActorRouteDomain}, nil
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
	renew, err := w.config.WorkLease.RenewWorkLeaseWithBoot(renewCtx, bucket, FamilyMailClaimProjection, claim.WorkID,
		w.config.Owner, w.config.BootID, claim.LeaseEpoch, w.config.Token, w.config.LeaseTTL.Milliseconds())
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
	return leaseWindow{safeDeadline: safeDeadline}, nil
}

func (w *Worker) retry(ctx context.Context, bucket rediskey.AuthorityBucketRef, claim authority.WorkClaim, cause error) error {
	if cause == nil {
		cause = ErrRetryDeferred
	}
	if ctx != nil && ctx.Err() != nil {
		return fmt.Errorf("%w: %w", ErrRetryDeferred, cause)
	}
	attempt := claim.RetryCount + 1
	if attempt == 0 {
		attempt = 1
	}
	delayMs := w.config.RetryDelay.Milliseconds()
	if delayMs < 1 {
		delayMs = 1
	}
	rescheduleCtx, cancel := context.WithTimeout(context.Background(), w.config.OperationTimeout)
	rescheduled, err := w.config.WorkLease.RescheduleWork(rescheduleCtx, bucket, FamilyMailClaimProjection, claim.WorkID,
		w.config.Owner, w.config.BootID, claim.LeaseEpoch, claim.WorkVersion, w.config.Token, attempt, delayMs, errorClass(cause))
	cancel()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return fmt.Errorf("%w: %w; reschedule: %v", ErrRetryDeferred, cause, err)
		}
		return fmt.Errorf("%w: %w; reschedule: %v", ErrRetryDeferred, cause, err)
	}
	switch rescheduled.Status {
	case authority.WorkLeaseRenewed, authority.WorkLeaseReconcileRequired:
		return fmt.Errorf("%w: %w", ErrRetryDeferred, cause)
	case authority.WorkLeaseFenceMismatch, authority.WorkLeaseExpired, authority.WorkLeaseStaleVersion, authority.WorkLeaseStaleGeneration:
		return fmt.Errorf("%w: reschedule %s status=%s", ErrLeaseLost, claim.WorkID, rescheduled.Status)
	default:
		return fmt.Errorf("%w: %w; reschedule status=%s", ErrRetryDeferred, cause, rescheduled.Status)
	}
}

func errorClass(err error) string {
	if errors.Is(err, ErrPayloadInvalid) {
		return "PAYLOAD_INVALID"
	}
	if errors.Is(err, ErrLeaseLost) {
		return "LEASE_LOST"
	}
	return "RETRYABLE_ERROR"
}

func buildCompletionProof(claim authority.WorkClaim, payload MailClaimProjectionPayload, binding proofBinding, outcome string) (authority.WorkCompletionProof, error) {
	if err := mail.ValidateHash(claim.PayloadHash, false); err != nil || claim.PayloadHash != MailClaimProjectionPayloadHash(payload) {
		return authority.WorkCompletionProof{}, fmt.Errorf("%w: completion payload hash", ErrPayloadInvalid)
	}
	resultHash := projectionResultHash(claim, payload, outcome)
	proof := authority.WorkCompletionProof{
		SourceAuthorityKind:        binding.sourceKind,
		CanonicalSourceAuthorityID: binding.sourceID,
		AuthorityLayoutVersion:     binding.layout,
		ActorRouteDomain:           binding.route,
		PayloadHash:                claim.PayloadHash,
		InvocationID:               claim.WorkID,
		ParticipantStep:            mailProjectionStep,
		ParticipantOutcome:         outcome,
		ParticipantResultHash:      resultHash,
		ParticipantResultVersion:   claim.WorkVersion,
	}
	proof.ResultProofHash = authority.WorkCompletionProofHash(proof)
	return proof, nil
}

func projectionResultHash(claim authority.WorkClaim, payload MailClaimProjectionPayload, outcome string) string {
	h := sha256.New()
	for _, value := range []string{"MAIL_CLAIM_PROJECTION_RESULT_V1", claim.WorkID, strconv.FormatUint(claim.WorkVersion, 10),
		payload.MailID, payload.ClaimReceiptRef, payload.ClaimMaterialHash, strconv.FormatInt(payload.ClaimedAtMs, 10), outcome} {
		var length [4]byte
		binaryAppendUint32(length[:], uint32(len(value)))
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(value))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func binaryAppendUint32(dst []byte, value uint32) {
	dst[0] = byte(value >> 24)
	dst[1] = byte(value >> 16)
	dst[2] = byte(value >> 8)
	dst[3] = byte(value)
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
