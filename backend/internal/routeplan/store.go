package routeplan

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type CASStatus uint8

const (
	CASUnknown CASStatus = iota
	CASCommitted
	CASAlreadyCommitted
	CASRevisionConflict
	CASCapacityExceeded
)

func (s CASStatus) String() string {
	switch s {
	case CASCommitted:
		return "COMMITTED"
	case CASAlreadyCommitted:
		return "ALREADY_COMMITTED"
	case CASRevisionConflict:
		return "REVISION_CONFLICT"
	case CASCapacityExceeded:
		return "CAPACITY_EXCEEDED"
	default:
		return "UNKNOWN"
	}
}

type PlanRecord struct {
	Plan     Plan
	Envelope PlanEnvelope
}

func (r PlanRecord) Clone() PlanRecord {
	r.Plan = r.Plan.Clone()
	r.Envelope.CanonicalJSON = append([]byte(nil), r.Envelope.CanonicalJSON...)
	return r
}

type CASResult struct {
	Status CASStatus
	Record PlanRecord
}

// PlanStore is the only persistence capability accepted by PlanExecutor.
// Implementations must provide exact-ID reads and bounded incomplete listing.
type PlanStore interface {
	Load(context.Context, string) (PlanRecord, bool, error)
	CompareAndSwap(context.Context, uint64, Plan) (CASResult, error)
	ListIncomplete(context.Context) ([]PlanRecord, error)
}

type MemoryPlanStoreOptions struct {
	MaxItems       int
	MaxBytes       int
	PlanTTL        time.Duration
	AuditRetention time.Duration
	Now            func() time.Time
}

func DefaultMemoryPlanStoreOptions() MemoryPlanStoreOptions {
	return MemoryPlanStoreOptions{MaxItems: 1024, MaxBytes: 8 << 20, PlanTTL: 24 * time.Hour, AuditRetention: 24 * time.Hour, Now: time.Now}
}

type MemoryPlanStore struct {
	mu      sync.Mutex
	options MemoryPlanStoreOptions
	records map[string]PlanRecord
	history []CASResult
}

func NewMemoryPlanStore(options MemoryPlanStoreOptions) (*MemoryPlanStore, error) {
	defaults := DefaultMemoryPlanStoreOptions()
	if options.MaxItems <= 0 {
		options.MaxItems = defaults.MaxItems
	}
	if options.MaxBytes <= 0 {
		options.MaxBytes = defaults.MaxBytes
	}
	if options.PlanTTL <= 0 {
		options.PlanTTL = defaults.PlanTTL
	}
	if options.AuditRetention <= 0 {
		options.AuditRetention = defaults.AuditRetention
	}
	if options.Now == nil {
		options.Now = defaults.Now
	}
	if options.AuditRetention > 38*24*time.Hour {
		return nil, ErrInvalidPlan
	}
	return &MemoryPlanStore{options: options, records: make(map[string]PlanRecord)}, nil
}

func (s *MemoryPlanStore) Load(ctx context.Context, id string) (PlanRecord, bool, error) {
	if err := ValidatePlanID(id); err != nil {
		return PlanRecord{}, false, err
	}
	if err := contextErr(ctx); err != nil {
		return PlanRecord{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[id]
	if ok && physicalExpiryMs(record.Plan, s.options.AuditRetention) <= s.options.Now().UnixMilli() {
		delete(s.records, id)
		ok = false
	}
	if !ok {
		return PlanRecord{}, false, nil
	}
	return record.Clone(), true, nil
}

func (s *MemoryPlanStore) CompareAndSwap(ctx context.Context, expectedRevision uint64, plan Plan) (CASResult, error) {
	if err := contextErr(ctx); err != nil {
		return CASResult{}, err
	}
	envelope, err := EncodePlan(plan)
	if err != nil {
		return CASResult{}, err
	}
	if plan.OwnerRetainUntilMs > physicalExpiryMs(Plan{ExpiresAtMs: plan.ExpiresAtMs}, s.options.AuditRetention) {
		return CASResult{}, fmt.Errorf("%w: owner retention exceeds configured audit horizon", ErrInvalidPlan)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpiredLocked()
	current, found := s.records[plan.PlanID]
	if !found {
		if expectedRevision != 0 || plan.Revision != 1 {
			return s.recordResultLocked(CASResult{Status: CASRevisionConflict}), nil
		}
		if err := ValidateInitialPlan(plan); err != nil {
			return s.recordResultLocked(CASResult{Status: CASRevisionConflict}), err
		}
		if len(s.records) >= s.options.MaxItems || envelopeSize(envelope) > s.options.MaxBytes || totalRecordBytesLocked(s.records)+envelopeSize(envelope) > s.options.MaxBytes {
			return s.recordResultLocked(CASResult{Status: CASCapacityExceeded}), ErrPlanCapacity
		}
		record := PlanRecord{Plan: plan.Clone(), Envelope: envelope}
		s.records[plan.PlanID] = record
		return s.recordResultLocked(CASResult{Status: CASCommitted, Record: record.Clone()}), nil
	}
	if current.Plan.Revision == plan.Revision && string(current.Envelope.CanonicalJSON) == string(envelope.CanonicalJSON) && current.Envelope.Checksum == envelope.Checksum {
		return s.recordResultLocked(CASResult{Status: CASAlreadyCommitted, Record: current.Clone()}), nil
	}
	if current.Plan.Revision != expectedRevision || plan.Revision != expectedRevision+1 {
		return s.recordResultLocked(CASResult{Status: CASRevisionConflict, Record: current.Clone()}), ErrPlanConflict
	}
	if err := ValidatePlanUpdate(current.Plan, plan); err != nil {
		return s.recordResultLocked(CASResult{Status: CASRevisionConflict, Record: current.Clone()}), err
	}
	oldBytes := envelopeSize(current.Envelope)
	newBytes := envelopeSize(envelope)
	totalBytes := 0
	for _, record := range s.records {
		totalBytes += envelopeSize(record.Envelope)
	}
	if totalBytes-oldBytes+newBytes > s.options.MaxBytes {
		return s.recordResultLocked(CASResult{Status: CASCapacityExceeded, Record: current.Clone()}), ErrPlanCapacity
	}
	record := PlanRecord{Plan: plan.Clone(), Envelope: envelope}
	s.records[plan.PlanID] = record
	return s.recordResultLocked(CASResult{Status: CASCommitted, Record: record.Clone()}), nil
}

func (s *MemoryPlanStore) ListIncomplete(ctx context.Context) ([]PlanRecord, error) {
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpiredLocked()
	result := make([]PlanRecord, 0, len(s.records))
	for _, record := range s.records {
		plan := record.Plan
		// A ScalePlan-owned PAUSED child remains recoverable: independent
		// per-shard children retry their exact transition, while older wave
		// records re-aggregate through the compatibility path.
		if !plan.Phase.Terminal() || (plan.Phase == PhasePaused && plan.OwnerScalePlanID != "" &&
			(retryablePauseCode(plan.ErrorCode) || plan.ErrorCode == ErrorAuthorityCommittedSnapshotPending)) ||
			(plan.Phase == PhasePaused && retryableCrashTakeoverPause(plan)) {
			result = append(result, record.Clone())
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Plan.UpdatedAtMs == result[j].Plan.UpdatedAtMs {
			return result[i].Plan.PlanID < result[j].Plan.PlanID
		}
		return result[i].Plan.UpdatedAtMs < result[j].Plan.UpdatedAtMs
	})
	return result, nil
}

func (s *MemoryPlanStore) History() []CASResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]CASResult, len(s.history))
	for i := range s.history {
		result[i] = s.history[i]
		result[i].Record = result[i].Record.Clone()
	}
	return result
}

func (s *MemoryPlanStore) pruneExpiredLocked() {
	now := s.options.Now().UnixMilli()
	for id, record := range s.records {
		if physicalExpiryMs(record.Plan, s.options.AuditRetention) <= now {
			delete(s.records, id)
		}
	}
}

func physicalExpiryMs(plan Plan, retention time.Duration) int64 {
	retentionMs := retention.Milliseconds()
	if plan.OwnerRetainUntilMs > plan.ExpiresAtMs && plan.OwnerRetainUntilMs-plan.ExpiresAtMs > retentionMs {
		retentionMs = plan.OwnerRetainUntilMs - plan.ExpiresAtMs
	}
	if retentionMs <= 0 || plan.ExpiresAtMs > int64(^uint64(0)>>1)-retentionMs {
		return int64(^uint64(0) >> 1)
	}
	return plan.ExpiresAtMs + retentionMs
}

func (s *MemoryPlanStore) recordResultLocked(result CASResult) CASResult {
	s.history = append(s.history, result)
	if len(s.history) > 4096 {
		s.history = s.history[len(s.history)-4096:]
	}
	return result
}

func envelopeSize(envelope PlanEnvelope) int {
	return len(envelope.CanonicalJSON) + len(envelope.Checksum) + 64
}

func totalRecordBytesLocked(records map[string]PlanRecord) int {
	total := 0
	for _, record := range records {
		total += envelopeSize(record.Envelope)
	}
	return total
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return ErrExecutorInvalid
	}
	return ctx.Err()
}
