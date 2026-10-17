package workflow

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

// LeaseController is the worker's view of its current work lease. Expired is
// the conservative local deadline (claim-start monotonic + accepted TTL -
// safety margin); once it fires the worker must not start new participant
// steps, though it may still persist what it already resolved.
type LeaseController interface {
	Fence() LeaseFence
	Expired() bool
	Renew(ctx context.Context) error
}

// WorkHandler resumes one leased record. The generic engine and the legacy
// runner both implement it, so a single recovery loop drives every work
// family — there is exactly one worker mainline.
type WorkHandler interface {
	Resume(ctx context.Context, record *WorkRecord, lease LeaseController) error
}

// EngineHandler adapts the generic Engine to WorkHandler.
type EngineHandler struct{ Engine *Engine }

func (h EngineHandler) Resume(ctx context.Context, record *WorkRecord, lease LeaseController) error {
	if h.Engine == nil {
		return fmt.Errorf("%w: engine", ErrInvalid)
	}
	return h.Engine.Resume(ctx, record, lease.Fence(), lease.Expired)
}

// RecoveryConfig bounds one recovery worker. Owner must be a stable
// instance identity, Token a per-boot secret; both feed the lease fence.
type RecoveryConfig struct {
	Owner        string
	Token        string
	ClaimLimit   int
	LeaseTTL     time.Duration
	SafetyMargin time.Duration
	ClaimTiming  *ClaimTimingRegistry
	ClaimNow     func() time.Time
}

func (c RecoveryConfig) withDefaults() RecoveryConfig {
	if c.ClaimLimit <= 0 {
		c.ClaimLimit = 32
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 10 * time.Second
	}
	if c.SafetyMargin <= 0 {
		c.SafetyMargin = c.LeaseTTL / 5
	}
	if c.SafetyMargin >= c.LeaseTTL {
		c.SafetyMargin = c.LeaseTTL / 5
	}
	return c
}

// RecoveryStats reports one scan round for metrics (queue depth, due lag,
// retry and dead-letter-like counters live in repository/handler metrics).
type RecoveryStats struct {
	Claimed   int
	Resumed   int
	Completed int
	Retried   int
	LeaseLost int
	Skipped   int
	Failed    int
	// TerminalLatencies is bounded by ClaimLimit. It is populated only when a
	// completed record carried a durable creation timestamp.
	TerminalLatencies []time.Duration
}

// RecoveryLoop is the farm-worker recovery model: scan the due index of the
// buckets it owns, claim with a trusted-time lease, resume through the
// registered handler, and rely on work_version CAS + participant receipts
// for exactly-once effects. It never creates new business intent.
type RecoveryLoop struct {
	repo      WorkRepository
	handlers  map[string]WorkHandler
	workTypes []string
	config    RecoveryConfig
	mono      func() time.Time
	claimNow  func() time.Time
}

func NewRecoveryLoop(repo WorkRepository, handlers map[string]WorkHandler, config RecoveryConfig) (*RecoveryLoop, error) {
	if repo == nil || len(handlers) == 0 {
		return nil, fmt.Errorf("%w: repository and handlers required", ErrInvalid)
	}
	if config.Owner == "" || config.Token == "" {
		return nil, fmt.Errorf("%w: owner/token", ErrInvalid)
	}
	config = config.withDefaults()
	if config.ClaimTiming == nil {
		config.ClaimTiming = NewClaimTimingRegistry()
	}
	claimNow := config.ClaimNow
	if claimNow == nil {
		claimNow = time.Now
	}
	workTypes := make([]string, 0, len(handlers))
	for workType := range handlers {
		workTypes = append(workTypes, workType)
	}
	sort.Strings(workTypes)
	return &RecoveryLoop{
		repo:      repo,
		handlers:  handlers,
		workTypes: workTypes,
		config:    config,
		mono:      time.Now,
		claimNow:  claimNow,
	}, nil
}

// PendingDue exposes the current backlog for due-lag metrics.
func (l *RecoveryLoop) PendingDue(ctx context.Context) (int64, error) {
	return l.repo.PendingDue(ctx)
}

// ObserveDue returns a bounded typed proof for this loop's registered work
// types. A repository without the optional proof contract returns
// Available=false rather than an invented aggregate from a shared index.
func (l *RecoveryLoop) ObserveDue(ctx context.Context, limit int) (DueIndexObservation, error) {
	observer, ok := l.repo.(TypedDueIndexObserver)
	if !ok {
		return DueIndexObservation{}, nil
	}
	return observer.ObserveDueByTypes(ctx, append([]string(nil), l.workTypes...), limit)
}

// RunOnce executes one bounded scan-claim-resume round. It is exported (not
// only a blocking Run) so composition and tests control scheduling and so a
// crashed worker is simulated by simply never calling it again.
func (l *RecoveryLoop) RunOnce(ctx context.Context) (RecoveryStats, error) {
	var stats RecoveryStats
	claimStart := l.claimNow()
	leaseStart := l.mono()
	var claimed []*ClaimedWork
	var err error
	if typed, ok := l.repo.(TypedWorkLeaseRepository); ok {
		claimed, err = typed.ClaimDueByTypes(ctx, l.config.Owner, l.config.Token, l.workTypes, l.config.ClaimLimit, l.config.LeaseTTL)
	} else {
		claimed, err = l.repo.ClaimDue(ctx, l.config.Owner, l.config.Token, l.config.ClaimLimit, l.config.LeaseTTL)
	}
	if err != nil {
		return stats, err
	}
	// This is intentionally immediately adjacent to the single ClaimDue. The
	// registry records every returned family before any handler can consume
	// time on an earlier item in the batch.
	l.config.ClaimTiming.RecordClaimed(claimStart, claimed, l.config.SafetyMargin)
	stats.Claimed = len(claimed)
	for _, item := range claimed {
		record := item.Record
		handler, ok := l.handlers[record.Address.WorkType]
		if !ok {
			stats.Skipped++
			continue
		}
		stats.Resumed++
		lease := &workLeaseController{
			repo:     l.repo,
			fence:    LeaseFence{Owner: l.config.Owner, Epoch: record.LeaseEpoch},
			deadline: leaseStart.Add(item.AcceptedTTL - l.config.SafetyMargin),
			token:    l.config.Token,
			ttl:      l.config.LeaseTTL,
			margin:   l.config.SafetyMargin,
			workType: record.Address.WorkType,
			workID:   record.Address.WorkID,
			mono:     l.mono,
		}
		err := handler.Resume(ctx, record, lease)
		switch {
		case err == nil:
			stats.Completed++
			if record.CreatedAtMs > 0 {
				latency := l.claimNow().Sub(time.UnixMilli(record.CreatedAtMs))
				if latency >= 0 {
					stats.TerminalLatencies = append(stats.TerminalLatencies, latency)
				}
			}
		case errors.Is(err, ErrRetryScheduled):
			stats.Retried++
		case errors.Is(err, ErrLeaseLost):
			stats.LeaseLost++
		default:
			stats.Failed++
		}
	}
	return stats, nil
}

type workLeaseController struct {
	repo     WorkRepository
	fence    LeaseFence
	deadline time.Time
	token    string
	ttl      time.Duration
	margin   time.Duration
	workType string
	workID   string
	mono     func() time.Time
}

func (c *workLeaseController) Fence() LeaseFence { return c.fence }

func (c *workLeaseController) Expired() bool { return !c.mono().Before(c.deadline) }

func (c *workLeaseController) Renew(ctx context.Context) error {
	updated, err := c.repo.RenewLease(ctx, c.workType, c.workID, c.fence.Owner, c.token, c.fence.Epoch, c.ttl)
	if err != nil {
		return err
	}
	c.fence.Epoch = updated.LeaseEpoch
	c.deadline = c.mono().Add(c.ttl - c.margin)
	return nil
}
