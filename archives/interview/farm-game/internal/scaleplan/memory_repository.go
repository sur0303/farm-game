package scaleplan

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// ScalePlanTestRepository exposes fault injection without exporting the
// concrete repository that owns the sealed blocked-terminal capability.
type ScalePlanTestRepository interface {
	ScalePlanRepository
	FailNextCAS(error)
	FailNextBlockedTermination(error)
	ApplyThenFailNextBlockedTermination(error)
	SetBeforeBlockedTerminationCommit(func())
	ForceReplaceForTest(Plan)
}

type repositoryIdentity struct{ marker byte }

type memoryRepository struct {
	identity                 *repositoryIdentity
	mu                       sync.Mutex
	mutation                 chan struct{}
	fence                    atomic.Uint64
	maxItems                 int
	plans                    map[string]PlanRecord
	idempotency              map[string]memoryIdempotencyRecord
	active                   map[string]string
	now                      func() time.Time
	nextCASErr               error
	nextTerminationErr       error
	nextTerminationApplyThen error
	beforeTerminationCommit  func()
}

type memoryIdempotencyRecord struct{ planID, digest string }

func NewMemoryRepository(maxItems int, clocks ...func() time.Time) ScalePlanTestRepository {
	if maxItems <= 0 {
		maxItems = 1024
	}
	now := time.Now
	if len(clocks) > 0 && clocks[0] != nil {
		now = clocks[0]
	}
	return &memoryRepository{identity: &repositoryIdentity{}, maxItems: maxItems, plans: make(map[string]PlanRecord), idempotency: make(map[string]memoryIdempotencyRecord), active: make(map[string]string), now: now, mutation: make(chan struct{}, 1)}
}

func (r *memoryRepository) repositoryIdentity() *repositoryIdentity { return r.identity }

// memoryLeaseTTL keeps the synthesized lease plausible for callers that
// inspect the expiry; nothing in memory enforces it, because there is no
// second process to fence out.
const memoryLeaseTTL = 20 * time.Second

func (r *memoryRepository) enterPlanMutation(ctx context.Context, scope MutationScope) (PlanMutationGuard, error) {
	if ctx == nil {
		return nil, ErrInvalidPlan
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	select {
	case r.mutation <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	fence := r.fence.Add(1)
	if fence > MaxLuaSafeInteger {
		<-r.mutation
		return nil, fmt.Errorf("%w: fence exhausted", ErrInvalidPlan)
	}
	lease, err := adoptBackendLease(BackendMutationLease{
		RepositoryID: "memory-scaleplan-repository",
		Scope:        scope,
		Owner:        MutationLeaseOwner{ControllerID: "memory", ControllerBootID: "memory", OperationNonce: strconv.FormatUint(fence, 10)},
		Fence:        strconv.FormatUint(fence, 10),
		ExpiresAtMs:  r.now().Add(memoryLeaseTTL).UnixMilli(),
	})
	if err != nil {
		<-r.mutation
		return nil, err
	}
	guardCtx, cancel := context.WithCancel(ctx)
	return &memoryMutationGuard{ctx: guardCtx, cancel: cancel, lease: lease,
		release: func() { <-r.mutation }}, nil
}

func (r *memoryRepository) CreateOrGet(ctx context.Context, submission IdempotencySubmission, plan Plan) (CreateResult, error) {
	if ctx == nil {
		return CreateResult{}, ErrInvalidPlan
	}
	if err := ctx.Err(); err != nil {
		return CreateResult{}, err
	}
	if err := ValidateInitialPlan(plan); err != nil {
		return CreateResult{}, err
	}
	if submission.Key != plan.IdempotencyKey || submission.PayloadDigest != plan.PayloadDigest || submission.WorkloadKey != plan.Submission.Workload.Key() {
		return CreateResult{}, ErrInvalidPlan
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.idempotency[submission.Key]; ok {
		record := r.plans[existing.planID].Clone()
		if existing.digest == submission.PayloadDigest {
			return CreateResult{Status: CreateExistingSamePayload, Record: record}, nil
		}
		return CreateResult{Status: CreateIdempotencyConflict, Record: record}, ErrIdempotencyConflict
	}
	if id, ok := r.active[submission.WorkloadKey]; ok {
		return CreateResult{Status: CreateWorkloadBusy, Record: r.plans[id].Clone()}, ErrWorkloadBusy
	}
	if len(r.plans) >= r.maxItems {
		return CreateResult{Status: CreateCapacityExceeded}, ErrCapacityExceeded
	}
	record := PlanRecord{Plan: plan.Clone()}
	r.plans[plan.PlanID] = record
	r.idempotency[submission.Key] = memoryIdempotencyRecord{plan.PlanID, plan.PayloadDigest}
	r.active[submission.WorkloadKey] = plan.PlanID
	return CreateResult{Status: CreateCommitted, Record: record.Clone()}, nil
}

func (r *memoryRepository) Load(ctx context.Context, id string) (PlanRecord, bool, error) {
	if ctx == nil {
		return PlanRecord{}, false, ErrInvalidPlan
	}
	if err := ctx.Err(); err != nil {
		return PlanRecord{}, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	record, ok := r.plans[id]
	return record.Clone(), ok, nil
}

func (r *memoryRepository) CompareAndSwap(ctx context.Context, expected uint64, plan Plan) (CASResult, error) {
	if ctx == nil {
		return CASResult{}, ErrInvalidPlan
	}
	if err := ctx.Err(); err != nil {
		return CASResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nextCASErr != nil {
		err := r.nextCASErr
		r.nextCASErr = nil
		return CASResult{Status: CASRevisionConflict, Record: r.plans[plan.PlanID].Clone()}, err
	}
	current, ok := r.plans[plan.PlanID]
	if !ok {
		return CASResult{Status: CASRevisionConflict}, ErrRevisionConflict
	}
	if current.Plan.Revision == plan.Revision && reflect.DeepEqual(current.Plan, plan) {
		return CASResult{Status: CASAlreadyCommitted, Record: current.Clone()}, nil
	}
	if current.Plan.Revision != expected || plan.Revision != expected+1 {
		return CASResult{Status: CASRevisionConflict, Record: current.Clone()}, ErrRevisionConflict
	}
	if err := ValidatePlanUpdate(current.Plan, plan); err != nil {
		return CASResult{Status: CASRevisionConflict, Record: current.Clone()}, err
	}
	record := PlanRecord{Plan: plan.Clone()}
	r.plans[plan.PlanID] = record
	if plan.Status.Terminal() {
		delete(r.active, plan.Submission.Workload.Key())
	}
	return CASResult{Status: CASCommitted, Record: record.Clone()}, nil
}

func (r *memoryRepository) commitBlockedTermination(ctx context.Context, expectation BlockedTerminationRequest, proof BlockedQuiescenceProof) (BlockedTerminationResult, error) {
	if ctx == nil {
		return BlockedTerminationResult{}, ErrInvalidPlan
	}
	if err := ctx.Err(); err != nil {
		return BlockedTerminationResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.beforeTerminationCommit != nil {
		r.beforeTerminationCommit()
	}
	current, ok := r.plans[expectation.PlanID]
	if !ok {
		return BlockedTerminationResult{Status: BlockedTerminationRevisionConflict}, ErrPlanNotFound
	}
	if current.Plan.Status == StatusFailed && current.Plan.BlockedTermination != nil {
		if validateTerminalReplay(current.Plan, expectation) == nil {
			return BlockedTerminationResult{Status: BlockedTerminationAlreadyCommitted, Record: current.Clone()}, nil
		}
		return BlockedTerminationResult{Status: BlockedTerminationRevisionConflict, Record: current.Clone()}, ErrRevisionConflict
	}
	commitMs := r.now().UnixMilli()
	if proof.CapturedAtMs < current.Plan.UpdatedAtMs || commitMs < current.Plan.UpdatedAtMs || commitMs < proof.CapturedAtMs || commitMs >= proof.ExpiresAtMs || proof.EvidenceRetainUntilMs < current.Plan.RecoveryRetainUntilMs || proof.EvidenceRetainUntilMs < proof.ExpiresAtMs {
		return BlockedTerminationResult{Status: BlockedTerminationProofExpired, Record: current.Clone()}, ErrVerification
	}
	proofDigest, err := BlockedQuiescenceProofDigest(proof)
	if err != nil {
		return BlockedTerminationResult{Status: BlockedTerminationUnsafe, Record: current.Clone()}, err
	}
	receipt := BlockedTerminationReceipt{ProtocolVersion: BlockedTerminationReceiptVersion, TerminationID: expectation.TerminationID, ExpectedBlockedRevision: expectation.ExpectedRevision, ExpectedBlockedReason: expectation.ExpectedBlockedReason, ExpectedBlockedUpdatedAtMs: current.Plan.UpdatedAtMs, ExpectedPayloadDigest: expectation.ExpectedPayloadDigest, ExpectedCheckpointDigest: expectation.ExpectedCheckpointDigest, ProofDigest: proofDigest, ProvedAtMs: proof.CapturedAtMs, ProofExpiresAtMs: proof.ExpiresAtMs, EvidenceRetainUntilMs: proof.EvidenceRetainUntilMs, CommittedAtMs: commitMs, IntentCount: proof.IntentCount, ChildCount: proof.ChildCount, ResultCode: "OPERATOR_SAFE_TERMINATION"}
	candidate := current.Plan.Clone()
	candidate.Status = StatusFailed
	candidate.BlockedReason = ""
	candidate.PausedReason = ""
	candidate.ErrorCode = "OPERATOR_SAFE_TERMINATION"
	candidate.Revision++
	candidate.UpdatedAtMs = commitMs
	candidate.CompletedAtMs = commitMs
	candidate.BlockedTermination = &receipt
	proofCopy := proof
	candidate.BlockedTerminationProof = &proofCopy
	if r.nextTerminationErr != nil {
		err := r.nextTerminationErr
		r.nextTerminationErr = nil
		return BlockedTerminationResult{Status: BlockedTerminationRevisionConflict, Record: current.Clone()}, err
	}
	if err := ValidateBlockedTerminationUpdate(current.Plan, candidate, expectation, proof, receipt); err != nil {
		return BlockedTerminationResult{Status: BlockedTerminationRevisionConflict, Record: current.Clone()}, err
	}
	record := PlanRecord{Plan: candidate}
	r.plans[candidate.PlanID] = record
	delete(r.active, candidate.Submission.Workload.Key())
	if r.nextTerminationApplyThen != nil {
		err := r.nextTerminationApplyThen
		r.nextTerminationApplyThen = nil
		return BlockedTerminationResult{Status: BlockedTerminationCommitted, Record: record.Clone()}, err
	}
	return BlockedTerminationResult{Status: BlockedTerminationCommitted, Record: record.Clone()}, nil
}

func (r *memoryRepository) ListRecoverable(ctx context.Context) ([]PlanRecord, error) {
	if ctx == nil {
		return nil, ErrInvalidPlan
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]PlanRecord, 0, len(r.plans))
	for _, record := range r.plans {
		if !record.Plan.Status.Terminal() {
			out = append(out, record.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Plan.UpdatedAtMs == out[j].Plan.UpdatedAtMs {
			return out[i].Plan.PlanID < out[j].Plan.PlanID
		}
		return out[i].Plan.UpdatedAtMs < out[j].Plan.UpdatedAtMs
	})
	return out, nil
}
func (r *memoryRepository) FailNextCAS(err error) { r.mu.Lock(); r.nextCASErr = err; r.mu.Unlock() }
func (r *memoryRepository) FailNextBlockedTermination(err error) {
	r.mu.Lock()
	r.nextTerminationErr = err
	r.mu.Unlock()
}
func (r *memoryRepository) ApplyThenFailNextBlockedTermination(err error) {
	r.mu.Lock()
	r.nextTerminationApplyThen = err
	r.mu.Unlock()
}
func (r *memoryRepository) SetBeforeBlockedTerminationCommit(hook func()) {
	r.mu.Lock()
	r.beforeTerminationCommit = hook
	r.mu.Unlock()
}
func (r *memoryRepository) ForceReplaceForTest(plan Plan) {
	r.mu.Lock()
	r.plans[plan.PlanID] = PlanRecord{Plan: plan.Clone()}
	r.mu.Unlock()
}
