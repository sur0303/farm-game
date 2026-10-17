package scaleplan

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	core "farmgame/internal/scaleplan"
)

var referenceBackendSequence atomic.Uint64

// ReferenceBackend is the one-off in-tree adapter A0.5 owes the seam.
//
// A0.5 rewrites the core of the routing control plane and freezes it before a
// single adapter exists to consume it, which is to say it finalizes an
// interface with no callers. This backend is the caller: it drives every seam
// the way A1, A1R, A2 and A3 eventually will, so that "the seam looks right"
// becomes "the seam compiles and the tests pass". It also discharges a proof
// obligation that cannot be discharged anywhere else -- a test inside package
// scaleplan can reach unexported fields, so only a second package can show
// that an external backend is unable to mint a terminal.
//
// It is deliberately not a production store: everything lives in a map.
type ReferenceBackend struct {
	mu    sync.Mutex
	plans map[string]core.PlanRecord
	// idempotency and active mirror what a durable store must key on.
	idempotency  map[string]string
	active       map[string]string
	now          func() time.Time
	repositoryID string

	fence atomic.Uint64
	lease *core.BackendMutationLease

	// failRenew makes renewal stop succeeding, which is the only way to
	// observe that the guard context is cancelled rather than merely stale.
	failRenew atomic.Bool
	// sealsSeen records every seal the backend was handed, so a test can
	// assert on what the core minted rather than on what it expected.
	sealsSeen []core.SealedBlockedTerminationMutation
	// unauthorizedRejected counts seals refused before any store access.
	unauthorizedRejected int
	renewals             atomic.Int64
	releases             atomic.Int64
}

func NewReferenceBackend(now func() time.Time) *ReferenceBackend {
	if now == nil {
		now = time.Now
	}
	return &ReferenceBackend{
		plans: make(map[string]core.PlanRecord), idempotency: make(map[string]string),
		active: make(map[string]string), now: now,
		repositoryID: fmt.Sprintf("reference-backend-%d", referenceBackendSequence.Add(1)),
	}
}

var (
	_ core.DurableRepositoryBackend = (*ReferenceBackend)(nil)
)

func (b *ReferenceBackend) RepositoryBindingID() string { return b.repositoryID }

// ErrReferenceLeaseLost is what a store returns once it stops recognizing the
// owner. A1's Lua will reach the same conclusion from owner, fence and expiry.
var ErrReferenceLeaseLost = errors.New("testkit: reference backend no longer holds this lease")

func (b *ReferenceBackend) AcquireMutationLease(ctx context.Context, scope core.MutationScope, owner core.MutationLeaseOwner) (core.BackendMutationLease, error) {
	if err := ctx.Err(); err != nil {
		return core.BackendMutationLease{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.lease != nil && !b.leaseExpiredLocked() {
		return core.BackendMutationLease{}, fmt.Errorf("%w: scope already leased", core.ErrInvalidPlan)
	}
	// Expiry authorizes takeover. The replacement advances the fence, so the
	// former owner cannot resume writes or release its successor's lease.
	lease := core.BackendMutationLease{
		RepositoryID: b.repositoryID,
		Scope:        scope, Owner: owner,
		Fence:       decimal(b.fence.Add(1)),
		ExpiresAtMs: b.now().Add(20 * time.Second).UnixMilli(),
	}
	b.lease = &lease
	return lease, nil
}

func (b *ReferenceBackend) RenewMutationLease(ctx context.Context, lease core.BackendMutationLease) (core.BackendMutationLease, error) {
	if err := ctx.Err(); err != nil {
		return core.BackendMutationLease{}, err
	}
	b.renewals.Add(1)
	if b.failRenew.Load() {
		return core.BackendMutationLease{}, ErrReferenceLeaseLost
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkLiveLeaseLocked(lease); err != nil {
		return core.BackendMutationLease{}, err
	}
	renewed := *b.lease
	renewed.ExpiresAtMs = b.now().Add(20 * time.Second).UnixMilli()
	if renewed.ExpiresAtMs <= b.lease.ExpiresAtMs {
		// Manual clocks may stay still while a heartbeat runs. Even then, a
		// successful renewal must make strict logical progress. Production Redis
		// obtains the new deadline from authoritative server time.
		renewed.ExpiresAtMs = b.lease.ExpiresAtMs + 1
	}
	b.lease = &renewed
	return renewed, nil
}

func (b *ReferenceBackend) ReleaseMutationLease(_ context.Context, lease core.BackendMutationLease) error {
	b.releases.Add(1)
	b.mu.Lock()
	defer b.mu.Unlock()
	// Release compares owner and fence. A stale owner that wakes up and
	// releases must not free the lease its successor now holds.
	if err := b.checkLeaseIdentityLocked(lease); err != nil {
		return err
	}
	b.lease = nil
	return nil
}

// checkLeaseIdentityLocked is deliberately expiry-blind. Release may clear its
// own expired lease, while owner/fence comparison prevents a stale owner from
// clearing a successor's lease.
func (b *ReferenceBackend) checkLeaseIdentityLocked(lease core.BackendMutationLease) error {
	if b.lease == nil {
		return ErrReferenceLeaseLost
	}
	if lease.RepositoryID != b.repositoryID || b.lease.RepositoryID != lease.RepositoryID ||
		b.lease.Owner != lease.Owner || b.lease.Fence != lease.Fence || b.lease.Scope != lease.Scope {
		return ErrReferenceLeaseLost
	}
	return nil
}

// checkLiveLeaseLocked is the authorization rule for renew and durable writes.
// The persisted backend deadline is authoritative; a caller cannot extend its
// authority by changing the deadline in its copy. Equality is expired.
func (b *ReferenceBackend) checkLiveLeaseLocked(lease core.BackendMutationLease) error {
	if err := b.checkLeaseIdentityLocked(lease); err != nil {
		return err
	}
	if b.leaseExpiredLocked() {
		return ErrReferenceLeaseLost
	}
	return nil
}

func (b *ReferenceBackend) leaseExpiredLocked() bool {
	return b.lease != nil && b.now().UnixMilli() >= b.lease.ExpiresAtMs
}

func (b *ReferenceBackend) CreateOrGet(ctx context.Context, lease core.BackendMutationLease, command core.DurableCreateCommand) (core.CreateResult, error) {
	if err := ctx.Err(); err != nil {
		return core.CreateResult{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkLiveLeaseLocked(lease); err != nil {
		return core.CreateResult{}, err
	}
	submission, plan := command.Submission, command.Plan
	if existing, ok := b.idempotency[submission.Key]; ok {
		record := b.plans[existing].Clone()
		if record.Plan.PayloadDigest == submission.PayloadDigest {
			return core.CreateResult{Status: core.CreateExistingSamePayload, Record: record}, nil
		}
		return core.CreateResult{Status: core.CreateIdempotencyConflict, Record: record}, core.ErrIdempotencyConflict
	}
	if id, ok := b.active[submission.WorkloadKey]; ok {
		return core.CreateResult{Status: core.CreateWorkloadBusy, Record: b.plans[id].Clone()}, core.ErrWorkloadBusy
	}
	record := core.PlanRecord{Plan: plan.Clone()}
	b.plans[plan.PlanID] = record
	b.idempotency[submission.Key] = plan.PlanID
	b.active[submission.WorkloadKey] = plan.PlanID
	return core.CreateResult{Status: core.CreateCommitted, Record: record.Clone()}, nil
}

func (b *ReferenceBackend) Load(ctx context.Context, id string) (core.PlanRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return core.PlanRecord{}, false, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	record, ok := b.plans[id]
	return record.Clone(), ok, nil
}

func (b *ReferenceBackend) CompareAndSwap(ctx context.Context, lease core.BackendMutationLease, command core.DurableCASCommand) (core.CASResult, error) {
	if err := ctx.Err(); err != nil {
		return core.CASResult{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkLiveLeaseLocked(lease); err != nil {
		return core.CASResult{}, err
	}
	current, ok := b.plans[command.Plan.PlanID]
	if !ok {
		return core.CASResult{Status: core.CASRevisionConflict}, core.ErrRevisionConflict
	}
	if current.Plan.Revision != command.Expected || command.Plan.Revision != command.Expected+1 {
		return core.CASResult{Status: core.CASRevisionConflict, Record: current.Clone()}, core.ErrRevisionConflict
	}
	if err := core.ValidatePlanUpdate(current.Plan, command.Plan); err != nil {
		return core.CASResult{Status: core.CASRevisionConflict, Record: current.Clone()}, err
	}
	record := core.PlanRecord{Plan: command.Plan.Clone()}
	b.plans[command.Plan.PlanID] = record
	if command.Plan.Status.Terminal() {
		delete(b.active, command.Plan.Submission.Workload.Key())
	}
	return core.CASResult{Status: core.CASCommitted, Record: record.Clone()}, nil
}

func (b *ReferenceBackend) ListRecoverable(ctx context.Context, cursor core.RecoverableCursor) (core.RecoverablePage, error) {
	if err := ctx.Err(); err != nil {
		return core.RecoverablePage{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]core.PlanRecord, 0, len(b.plans))
	for _, record := range b.plans {
		if !record.Plan.Status.Terminal() {
			out = append(out, record.Clone())
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Plan.PlanID < out[j].Plan.PlanID })
	return core.RecoverablePage{Records: out, Done: true}, nil
}

// ApplySealedBlockedTermination is the proof surface. The Authorized check
// happens before any map access on purpose: A1's equivalent must reject before
// it opens a Redis connection, and a backend that checks afterwards would pass
// a test that only inspects the return value.
func (b *ReferenceBackend) ApplySealedBlockedTermination(ctx context.Context, lease core.BackendMutationLease, sealed core.SealedBlockedTerminationMutation) (core.BlockedTerminationResult, error) {
	if err := ctx.Err(); err != nil {
		return core.BlockedTerminationResult{}, err
	}
	// Both checks, in this order, before any store access. Authorized() catches
	// a forged or zero-value seal; BoundTo catches a genuine seal that belongs
	// to a different runtime, or to this one before a fence change.
	if !sealed.Authorized() || lease.RepositoryID != b.repositoryID || !sealed.BoundTo(lease) {
		b.mu.Lock()
		b.unauthorizedRejected++
		b.mu.Unlock()
		return core.BlockedTerminationResult{Status: core.BlockedTerminationUnsafe}, core.ErrVerification
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.checkLiveLeaseLocked(lease); err != nil {
		return core.BlockedTerminationResult{Status: core.BlockedTerminationUnsafe}, err
	}
	b.sealsSeen = append(b.sealsSeen, sealed)

	request := sealed.Request()
	current, ok := b.plans[request.PlanID]
	if !ok {
		return core.BlockedTerminationResult{Status: core.BlockedTerminationRevisionConflict}, core.ErrPlanNotFound
	}
	if current.Plan.Revision != request.ExpectedRevision {
		return core.BlockedTerminationResult{Status: core.BlockedTerminationExpectationMismatch, Record: current.Clone()}, nil
	}
	candidate := current.Plan.Clone()
	candidate.Status = core.StatusFailed
	candidate.BlockedReason = ""
	candidate.ErrorCode = "OPERATOR_SAFE_TERMINATION"
	candidate.Revision++
	commitMs := b.now().UnixMilli()
	candidate.UpdatedAtMs = commitMs
	candidate.CompletedAtMs = commitMs
	record := core.PlanRecord{Plan: candidate}
	b.plans[candidate.PlanID] = record
	delete(b.active, candidate.Submission.Workload.Key())
	return core.BlockedTerminationResult{Status: core.BlockedTerminationCommitted, Record: record.Clone()}, nil
}

// FailRenewals makes the next renewal fail. The guard must cancel its context
// rather than keep running on a lease nobody holds.
func (b *ReferenceBackend) FailRenewals() { b.failRenew.Store(true) }

func (b *ReferenceBackend) SealsSeen() []core.SealedBlockedTerminationMutation {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]core.SealedBlockedTerminationMutation(nil), b.sealsSeen...)
}

func (b *ReferenceBackend) UnauthorizedRejected() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.unauthorizedRejected
}

func (b *ReferenceBackend) LeaseHeld() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lease != nil
}

func (b *ReferenceBackend) Releases() int64 { return b.releases.Load() }

func decimal(value uint64) string {
	if value == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	return string(buf[i:])
}
