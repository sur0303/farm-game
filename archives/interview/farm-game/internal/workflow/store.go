// Package workflow owns the durable scheduler record for cross-authority farm
// operations. Business facts remain in PlayerRecord/FarmRecord receipts.
package workflow

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	workflowpb "farmgame/api/gen/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

var (
	ErrNotFound            = errors.New("workflow: record not found")
	ErrInvalid             = errors.New("workflow: invalid record")
	ErrIdempotencyConflict = errors.New("workflow: idempotency conflict")
	ErrRevisionConflict    = errors.New("workflow: revision conflict")
	ErrLeaseLost           = errors.New("workflow: lease lost")
	ErrNotDue              = errors.New("workflow: record is not due")
	ErrTerminal            = errors.New("workflow: terminal record is immutable")
)

// UpdateOptions fences a workflow mutation. Online coordinators leave
// LeaseOwner empty and may update only while no live worker lease exists.
// Workers must present the exact owner/epoch returned by ClaimDue.
type UpdateOptions struct {
	ExpectedRevision uint64
	LeaseOwner       string
	LeaseEpoch       uint64
	Now              time.Time
}

type Store interface {
	// CreateOrLoad atomically inserts candidate or loads the existing workflow.
	// An existing workflow_id is reusable only for the same immutable intent.
	CreateOrLoad(context.Context, *workflowpb.WorkflowRecord) (*workflowpb.WorkflowRecord, bool, error)
	Get(context.Context, string) (*workflowpb.WorkflowRecord, error)
	// Update performs a revision/lease-fenced mutation and releases any claim.
	Update(context.Context, *workflowpb.WorkflowRecord, UpdateOptions) (*workflowpb.WorkflowRecord, error)
	// ClaimDue leases due workflows to one stable worker identity.
	ClaimDue(context.Context, string, time.Time, int, time.Duration) ([]*workflowpb.WorkflowRecord, error)
	// PendingDue 返回当前到期待领取的 workflow 数量(backlog 观测,不含已领取的 lease)。
	PendingDue(context.Context, time.Time) (int64, error)
}

func Terminal(state workflowpb.WorkflowState) bool {
	return state == workflowpb.WorkflowState_COMPLETED || state == workflowpb.WorkflowState_COMPENSATED
}

func Validate(record *workflowpb.WorkflowRecord) error {
	if record == nil || strings.TrimSpace(record.WorkflowId) == "" ||
		workidentity.ValidateStableOpaqueID(record.WorkflowId) != nil ||
		strings.TrimSpace(record.OperationId) == "" ||
		record.Kind == workflowpb.WorkflowKind_WORKFLOW_KIND_UNSPECIFIED ||
		record.FarmId == 0 || record.InitiatorPlayerId == 0 ||
		len(record.PayloadHash) == 0 {
		return ErrInvalid
	}
	return nil
}

func sameIntent(left, right *workflowpb.WorkflowRecord) bool {
	return left != nil && right != nil &&
		left.WorkflowId == right.WorkflowId &&
		left.OperationId == right.OperationId &&
		left.Kind == right.Kind &&
		left.FarmId == right.FarmId &&
		left.InitiatorPlayerId == right.InitiatorPlayerId &&
		bytes.Equal(left.PayloadHash, right.PayloadHash)
}

func clone(record *workflowpb.WorkflowRecord) *workflowpb.WorkflowRecord {
	if record == nil {
		return nil
	}
	return proto.Clone(record).(*workflowpb.WorkflowRecord)
}

type MemoryStore struct {
	mu      sync.Mutex
	records map[string]*workflowpb.WorkflowRecord
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{records: make(map[string]*workflowpb.WorkflowRecord)}
}

func (s *MemoryStore) CreateOrLoad(ctx context.Context, candidate *workflowpb.WorkflowRecord) (*workflowpb.WorkflowRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if err := Validate(candidate); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.records[candidate.WorkflowId]; existing != nil {
		if !sameIntent(existing, candidate) {
			return nil, false, ErrIdempotencyConflict
		}
		return clone(existing), false, nil
	}
	created := clone(candidate)
	created.WorkflowRevision = 1
	created.LeaseOwner = ""
	created.LeaseEpoch = 0
	created.LeaseUntilMs = 0
	s.records[created.WorkflowId] = created
	return clone(created), true, nil
}

func (s *MemoryStore) Get(ctx context.Context, id string) (*workflowpb.WorkflowRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.records[id]
	if record == nil {
		return nil, ErrNotFound
	}
	return clone(record), nil
}

func (s *MemoryStore) PendingDue(ctx context.Context, now time.Time) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	limit := now.UnixMilli()
	s.mu.Lock()
	defer s.mu.Unlock()
	var pending int64
	for _, record := range s.records {
		if record == nil || Terminal(record.State) {
			continue
		}
		if record.NextRetryAtMs <= limit {
			pending++
		}
	}
	return pending, nil
}

func (s *MemoryStore) Update(ctx context.Context, next *workflowpb.WorkflowRecord, options UpdateOptions) (*workflowpb.WorkflowRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := Validate(next); err != nil || options.ExpectedRevision == 0 {
		return nil, ErrInvalid
	}
	now := options.Now
	if now.IsZero() {
		now = time.Now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.records[next.WorkflowId]
	if current == nil {
		return nil, ErrNotFound
	}
	if !sameIntent(current, next) {
		return nil, ErrIdempotencyConflict
	}
	if current.WorkflowRevision != options.ExpectedRevision {
		return nil, ErrRevisionConflict
	}
	if Terminal(current.State) {
		return nil, ErrTerminal
	}
	if err := validateLease(current, options, now); err != nil {
		return nil, err
	}
	updated := clone(next)
	updated.WorkflowRevision = current.WorkflowRevision + 1
	updated.LeaseOwner = ""
	updated.LeaseEpoch = current.LeaseEpoch
	updated.LeaseUntilMs = 0
	s.records[updated.WorkflowId] = updated
	return clone(updated), nil
}

func validateLease(current *workflowpb.WorkflowRecord, options UpdateOptions, now time.Time) error {
	if options.LeaseOwner != "" {
		if current.LeaseOwner != options.LeaseOwner ||
			current.LeaseEpoch != options.LeaseEpoch ||
			current.LeaseUntilMs <= now.UnixMilli() {
			return ErrLeaseLost
		}
		return nil
	}
	if current.LeaseOwner != "" && current.LeaseUntilMs > now.UnixMilli() {
		return ErrLeaseLost
	}
	return nil
}

func (s *MemoryStore) ClaimDue(ctx context.Context, owner string, now time.Time, limit int, lease time.Duration) ([]*workflowpb.WorkflowRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return nil, ErrInvalid
	}
	if limit <= 0 {
		limit = 32
	}
	if lease <= 0 {
		lease = 5 * time.Second
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var due []*workflowpb.WorkflowRecord
	for _, record := range s.records {
		if !Terminal(record.State) &&
			record.NextRetryAtMs <= now.UnixMilli() &&
			record.LeaseUntilMs <= now.UnixMilli() {
			due = append(due, record)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].NextRetryAtMs == due[j].NextRetryAtMs {
			return due[i].WorkflowId < due[j].WorkflowId
		}
		return due[i].NextRetryAtMs < due[j].NextRetryAtMs
	})
	if len(due) > limit {
		due = due[:limit]
	}
	result := make([]*workflowpb.WorkflowRecord, 0, len(due))
	for _, record := range due {
		record.WorkflowRevision++
		record.LeaseOwner = owner
		record.LeaseEpoch++
		record.LeaseUntilMs = now.Add(lease).UnixMilli()
		result = append(result, clone(record))
	}
	return result, nil
}
