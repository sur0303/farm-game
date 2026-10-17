package workflow

import (
	"context"
	"strings"
	"sync"
	"time"
)

// AdvanceOptions fences a durable work mutation. Online coordinators and the
// DirectDispatcher leave LeaseOwner empty and may only advance while no live
// worker lease exists. Workers must present the exact owner/epoch returned by
// ClaimDue; ReleaseLease hands the record back to the due index.
type AdvanceOptions struct {
	ExpectedWorkVersion uint64
	LeaseOwner          string
	LeaseEpoch          uint64
	ReleaseLease        bool
}

// ClaimedWork is one leased record plus the authority-adjudicated TTL. The
// worker builds its conservative local deadline from
// claim-start-monotonic + AcceptedTTL - safety margin (contract 29 §7.3.1).
type ClaimedWork struct {
	Record      *WorkRecord
	AcceptedTTL time.Duration
}

// WorkRepository is the storage-independent port for generic Work records
// and their due index. Implementations must guarantee:
//   - CreateIfAbsent is atomic; same intent reuses, different intent conflicts.
//   - Advance is a CAS on WorkVersion plus the optional lease fence; the
//     record and the due index entry change in the same atomic boundary.
//   - ClaimDue adjudicates expiry with repository-trusted time, increments
//     the lease epoch, and moves the due score to the lease expiry so a
//     crashed worker's work reappears automatically.
//   - RenewLease never advances WorkVersion.
type WorkRepository interface {
	CreateIfAbsent(ctx context.Context, candidate *WorkRecord) (*WorkRecord, bool, error)
	Get(ctx context.Context, workType, workID string) (*WorkRecord, error)
	Advance(ctx context.Context, next *WorkRecord, opts AdvanceOptions) (*WorkRecord, error)
	ClaimDue(ctx context.Context, owner, token string, limit int, ttl time.Duration) ([]*ClaimedWork, error)
	RenewLease(ctx context.Context, workType, workID, owner, token string, epoch uint64, ttl time.Duration) (*WorkRecord, error)
	PendingDue(ctx context.Context) (int64, error)
}

// SpecificWorkLeaseRepository is an optional physical extension used by
// source-order recovery when one exact independent WorkRecord must be leased.
// It avoids claiming an unrelated Plant/Harvest record from the shared due
// index. Production Theft recovery requires this extension; the base
// WorkRepository contract remains compatible with existing handlers.
type SpecificWorkLeaseRepository interface {
	ClaimWork(context.Context, string, string, string, string, time.Duration) (*ClaimedWork, error)
}

// TypedWorkLeaseRepository is an optional extension for a recovery loop that
// shares a physical due index with work families owned by another recovery
// root. The type filter must be applied in the same atomic operation that
// acquires the leases; claiming an unsupported family and skipping it would
// temporarily steal that family's lease.
type TypedWorkLeaseRepository interface {
	ClaimDueByTypes(context.Context, string, string, []string, int, time.Duration) ([]*ClaimedWork, error)
}

// DueIndexObservation is a bounded, read-only proof about the durable due
// index. Available is false when the implementation cannot prove the entire
// requested typed view inside its configured bound; callers must not turn a
// partial count or age into authoritative telemetry.
type DueIndexObservation struct {
	Depth         int64
	OldestDueAtMs int64
	Available     bool
}

// TypedDueIndexObserver is an optional repository observability extension.
// It never replaces ClaimDue/ClaimDueByTypes and therefore cannot become an
// alternate notification queue or recovery truth.
type TypedDueIndexObserver interface {
	ObserveDueByTypes(context.Context, []string, int) (DueIndexObservation, error)
}

// MemoryWorkRepository is the hermetic WorkRepository. Its injected clock is
// the repository-trusted time used for lease and due adjudication.
type MemoryWorkRepository struct {
	mu      sync.Mutex
	records map[string]*WorkRecord
	now     func() time.Time
}

func NewMemoryWorkRepository(now func() time.Time) *MemoryWorkRepository {
	if now == nil {
		now = time.Now
	}
	return &MemoryWorkRepository{records: make(map[string]*WorkRecord), now: now}
}

func workKey(workType, workID string) string { return workType + "\x00" + workID }

func (s *MemoryWorkRepository) trustedNow() time.Time { return s.now() }

func (s *MemoryWorkRepository) CreateIfAbsent(ctx context.Context, candidate *WorkRecord) (*WorkRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := candidate.Validate(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := workKey(candidate.Address.WorkType, candidate.Address.WorkID)
	if existing := s.records[key]; existing != nil {
		if !sameWorkIntent(existing, candidate) {
			return nil, false, ErrIdempotencyConflict
		}
		return cloneWorkRecord(existing), false, nil
	}
	now := s.trustedNow()
	created := cloneWorkRecord(candidate)
	created.WorkVersion = 1
	created.LeaseOwner = ""
	created.LeaseEpoch = 0
	created.LeaseTokenHash = ""
	created.LeaseExpireAtMs = 0
	if created.CreatedAtMs == 0 {
		created.CreatedAtMs = now.UnixMilli()
	}
	created.UpdatedAtMs = now.UnixMilli()
	s.records[key] = created
	return cloneWorkRecord(created), true, nil
}

func (s *MemoryWorkRepository) Get(ctx context.Context, workType, workID string) (*WorkRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.records[workKey(workType, workID)]
	if record == nil {
		return nil, ErrNotFound
	}
	return cloneWorkRecord(record), nil
}

func (s *MemoryWorkRepository) Advance(ctx context.Context, next *WorkRecord, opts AdvanceOptions) (*WorkRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := next.Validate(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.trustedNow()
	key := workKey(next.Address.WorkType, next.Address.WorkID)
	current := s.records[key]
	if current == nil {
		return nil, ErrNotFound
	}
	if !sameWorkIntent(current, next) {
		return nil, ErrIdempotencyConflict
	}
	if current.Terminal() {
		return nil, ErrTerminal
	}
	if current.WorkVersion != opts.ExpectedWorkVersion {
		return nil, ErrRevisionConflict
	}
	if err := fenceWorkLease(current, opts, now); err != nil {
		return nil, err
	}
	updated := cloneWorkRecord(next)
	updated.WorkVersion = current.WorkVersion + 1
	updated.UpdatedAtMs = now.UnixMilli()
	switch {
	case updated.Terminal():
		updated.LeaseOwner = ""
		updated.LeaseTokenHash = ""
		updated.LeaseExpireAtMs = 0
		updated.NextDueAtMs = 0
		updated.CompletedAtMs = now.UnixMilli()
	case opts.ReleaseLease:
		updated.LeaseOwner = ""
		updated.LeaseTokenHash = ""
		updated.LeaseExpireAtMs = 0
	default:
		updated.LeaseOwner = current.LeaseOwner
		updated.LeaseEpoch = current.LeaseEpoch
		updated.LeaseTokenHash = current.LeaseTokenHash
		updated.LeaseExpireAtMs = current.LeaseExpireAtMs
	}
	if !updated.Terminal() {
		updated.CompletedAtMs = 0
	}
	s.records[key] = updated
	return cloneWorkRecord(updated), nil
}

// fenceWorkLease mirrors contract 29 §7.4: a leased record may only be
// advanced by the current owner/epoch inside the trusted-time lease window;
// an unleased advance (coordinator/dispatcher) is rejected while a live
// worker lease exists.
func fenceWorkLease(current *WorkRecord, opts AdvanceOptions, now time.Time) error {
	if opts.LeaseOwner != "" {
		if current.LeaseOwner != opts.LeaseOwner ||
			current.LeaseEpoch != opts.LeaseEpoch ||
			current.LeaseExpireAtMs <= now.UnixMilli() {
			return ErrLeaseLost
		}
		return nil
	}
	if current.LeaseOwner != "" && current.LeaseExpireAtMs > now.UnixMilli() {
		return ErrLeaseLost
	}
	return nil
}

func (s *MemoryWorkRepository) ClaimDue(ctx context.Context, owner, token string, limit int, ttl time.Duration) ([]*ClaimedWork, error) {
	return s.claimDueByTypes(ctx, owner, token, nil, limit, ttl)
}

func (s *MemoryWorkRepository) ClaimDueByTypes(ctx context.Context, owner, token string, workTypes []string, limit int, ttl time.Duration) ([]*ClaimedWork, error) {
	if len(workTypes) == 0 {
		return nil, ErrInvalid
	}
	allowed := make(map[string]struct{}, len(workTypes))
	for _, workType := range workTypes {
		workType = strings.TrimSpace(workType)
		if workType == "" {
			return nil, ErrInvalid
		}
		allowed[workType] = struct{}{}
	}
	return s.claimDueByTypes(ctx, owner, token, allowed, limit, ttl)
}

func (s *MemoryWorkRepository) claimDueByTypes(ctx context.Context, owner, token string, allowed map[string]struct{}, limit int, ttl time.Duration) ([]*ClaimedWork, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	owner = strings.TrimSpace(owner)
	if owner == "" || strings.TrimSpace(token) == "" || ttl <= 0 {
		return nil, ErrInvalid
	}
	if limit <= 0 {
		limit = 32
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.trustedNow()
	nowMs := now.UnixMilli()
	var due []*WorkRecord
	for _, record := range s.records {
		if allowed != nil {
			if _, ok := allowed[record.Address.WorkType]; !ok {
				continue
			}
		}
		if !record.Terminal() && record.NextDueAtMs <= nowMs && record.LeaseExpireAtMs <= nowMs {
			due = append(due, record)
		}
	}
	sortDue(due)
	if len(due) > limit {
		due = due[:limit]
	}
	expireAtMs := now.Add(ttl).UnixMilli()
	claimed := make([]*ClaimedWork, 0, len(due))
	for _, record := range due {
		record.WorkVersion++
		record.LeaseOwner = owner
		record.LeaseEpoch++
		record.LeaseTokenHash = leaseTokenHash(token)
		record.LeaseExpireAtMs = expireAtMs
		record.NextDueAtMs = expireAtMs
		record.UpdatedAtMs = nowMs
		claimed = append(claimed, &ClaimedWork{Record: cloneWorkRecord(record), AcceptedTTL: ttl})
	}
	return claimed, nil
}

func (s *MemoryWorkRepository) ClaimWork(ctx context.Context, workType, workID, owner, token string, ttl time.Duration) (*ClaimedWork, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(owner) == "" || strings.TrimSpace(token) == "" || ttl <= 0 {
		return nil, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.records[workKey(workType, workID)]
	if record == nil {
		return nil, ErrNotFound
	}
	if record.Terminal() {
		return nil, ErrTerminal
	}
	now := s.trustedNow()
	if record.LeaseOwner != "" && record.LeaseExpireAtMs > now.UnixMilli() {
		return nil, ErrLeaseLost
	}
	if record.NextDueAtMs > now.UnixMilli() {
		return nil, ErrNotDue
	}
	record.WorkVersion++
	record.LeaseOwner = owner
	record.LeaseEpoch++
	record.LeaseTokenHash = leaseTokenHash(token)
	record.LeaseExpireAtMs = now.Add(ttl).UnixMilli()
	record.NextDueAtMs = record.LeaseExpireAtMs
	record.UpdatedAtMs = now.UnixMilli()
	return &ClaimedWork{Record: cloneWorkRecord(record), AcceptedTTL: ttl}, nil
}

func (s *MemoryWorkRepository) RenewLease(ctx context.Context, workType, workID, owner, token string, epoch uint64, ttl time.Duration) (*WorkRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(owner) == "" || strings.TrimSpace(token) == "" || epoch == 0 || ttl <= 0 {
		return nil, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.trustedNow()
	record := s.records[workKey(workType, workID)]
	if record == nil {
		return nil, ErrNotFound
	}
	if record.Terminal() {
		return nil, ErrTerminal
	}
	if record.LeaseOwner != owner || record.LeaseEpoch != epoch ||
		record.LeaseTokenHash != leaseTokenHash(token) || record.LeaseExpireAtMs <= now.UnixMilli() {
		return nil, ErrLeaseLost
	}
	expireAtMs := now.Add(ttl).UnixMilli()
	record.LeaseExpireAtMs = expireAtMs
	record.NextDueAtMs = expireAtMs
	record.UpdatedAtMs = now.UnixMilli()
	return cloneWorkRecord(record), nil
}

func (s *MemoryWorkRepository) PendingDue(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	nowMs := s.trustedNow().UnixMilli()
	var pending int64
	for _, record := range s.records {
		if !record.Terminal() && record.NextDueAtMs <= nowMs {
			pending++
		}
	}
	return pending, nil
}

func (s *MemoryWorkRepository) ObserveDueByTypes(ctx context.Context, workTypes []string, limit int) (DueIndexObservation, error) {
	if err := ctx.Err(); err != nil {
		return DueIndexObservation{}, err
	}
	if limit <= 0 || len(workTypes) == 0 {
		return DueIndexObservation{}, ErrInvalid
	}
	allowed := make(map[string]struct{}, len(workTypes))
	for _, workType := range workTypes {
		workType = strings.TrimSpace(workType)
		if workType == "" {
			return DueIndexObservation{}, ErrInvalid
		}
		allowed[workType] = struct{}{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	nowMs := s.trustedNow().UnixMilli()
	observation := DueIndexObservation{Available: true}
	for _, record := range s.records {
		if _, ok := allowed[record.Address.WorkType]; !ok || record.Terminal() || record.NextDueAtMs > nowMs {
			continue
		}
		observation.Depth++
		if observation.OldestDueAtMs == 0 || record.NextDueAtMs < observation.OldestDueAtMs {
			observation.OldestDueAtMs = record.NextDueAtMs
		}
		if observation.Depth > int64(limit) {
			return DueIndexObservation{}, nil
		}
	}
	return observation, nil
}
