package routeplan

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/holderidentity"
	"farmgame/internal/netendpoint"
	core "farmgame/internal/routecontrol"
	scaleplan "farmgame/internal/scaleplan"
	"farmgame/internal/storage/authoritystore"
)

var errPlanTTLInsufficient = errors.New("routeplan: plan ttl insufficient for manager operation")

// AssignmentManager is deliberately narrower than the concrete manager. It
// contains no generation calculation or public transfer implementation of its
// own; the concrete Manager remains the sole authority-generation writer.
type AssignmentManager interface {
	CurrentSnapshot() actorrouting.RouteSnapshot
	DrainValidated(context.Context, authority.TransferRequest, authority.DrainController, authority.PendingProjectionTargetValidator) (authoritystore.ShardAuthorityTransitionResult, error)
	HandoffWorkerValidated(context.Context, authority.TransferRequest, authority.WorkerHandoffSourceValidator, authority.PendingProjectionTargetValidator) (authoritystore.ShardAuthorityTransitionResult, error)
	TransferValidated(context.Context, authority.TransferRequest, authority.PendingProjectionTargetValidator) (authoritystore.ShardAuthorityTransitionResult, error)
}

// AuthorityReader is the exact-key read-only capability shared with the
// manager. It cannot mutate Authority.
type AuthorityWaveBatchManager interface {
	BeginAuthorityWaveBatch(context.Context, string, []core.AuthorityWaveMember) error
	AuthorityWaveBatchBaseRevision(string) (uint64, bool)
	AuthorityWaveBatchStage(string) (core.AuthorityWaveBatchStage, bool)
	AuthorityWaveBatchContains(string, actorrouting.RouteDomain, uint32) bool
	CommitAuthorityWaveBatch(context.Context, string) (int, core.SnapshotCASResult, error)
	AbortEmptyAuthorityWaveBatch(string) error
}

type AuthorityReader interface {
	ReadShardWriteAuthority(context.Context, actorrouting.RouteDomain, uint32) (authoritystore.ShardWriteAuthority, bool, error)
}

type HolderReader interface {
	Get(context.Context, actorrouting.RouteDomain, string) (core.HolderRecord, bool, error)
	List(context.Context, actorrouting.RouteDomain) ([]core.HolderRecord, error)
}

type CachedHolderReader interface {
	GetCached(context.Context, actorrouting.RouteDomain, string) (core.HolderRecord, bool, error)
}

// DrainControllerFactory binds the fixed source/target Holder peers for one
// plan. The factory is intentionally not allowed to choose a different shard
// or generation after the plan is durable.
type DrainControllerFactory func(context.Context, Plan) (authority.DrainController, func() error, error)

// ExecutionGuard is an injected single-controller composition seam. Enter
// must first recover every Manager-owned pending projection and retain its
// single-owner token until the returned release function runs. It does not
// expose Drain, Transfer, generation, or transition mutation to Executor.
type ExecutionGuard interface {
	Enter(context.Context) (release func(), err error)
}

// AuthorityWaveExecutionGuard is the stronger recovery seam used only while
// reconstructing a complete durable Authority wave. It excludes ordinary
// RoutePlan execution and generic pending-projection recovery without trying
// to recover the wave's individual members one by one.
type AuthorityWaveExecutionGuard interface {
	EnterAuthorityWave(context.Context) (release func(), err error)
}

type PlanExecutorOptions struct {
	Store                  PlanStore
	Manager                AssignmentManager
	AuthorityReader        AuthorityReader
	SnapshotStore          core.RouteSnapshotStore
	HolderReader           HolderReader
	DrainControllerFactory DrainControllerFactory
	Now                    func() time.Time
	OperationTimeout       time.Duration
	PlanTTL                time.Duration
	SingleController       SingleControllerAttestation
	Verification           VerificationPort
	ExecutionGuard         ExecutionGuard
	ControlMutation        ControlMutationOptions
	LeaseCheckMode         actorrouting.LeaseCheckMode
}

const (
	defaultExecutorTimeout = 30 * time.Second
	maxExecutorTimeout     = 30 * time.Minute
	defaultExecutorPlanTTL = 24 * time.Hour
	executorTTLSafety      = time.Second

	SingleControllerContract               = "single-replica-recreate-no-overlap-v1"
	ErrorAuthorityCommittedSnapshotPending = "authority_committed_snapshot_pending"
)

// SingleControllerAttestation is immutable construction-time evidence for the
// frozen non-HA deployment contract. It is not a lease or leader election.
type SingleControllerAttestation struct {
	Contract     string
	ControllerID string
	BootID       string
}

// VerificationPort is a narrow, read-only composition seam. A library caller
// may omit it, in which case Verify emits explicit failed dependency checks;
// the production command root requires ProductionVerification.
type VerificationPort interface {
	VerifyGateResolution(context.Context, Plan, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot) error
	VerifyTransition(context.Context, Plan, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot) error
	VerifyRecovery(context.Context, Plan, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot) error
	VerifyTombstone(context.Context, Plan, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot) error
}

func (o PlanExecutorOptions) normalized() (PlanExecutorOptions, error) {
	if o.Store == nil || o.Manager == nil || o.AuthorityReader == nil || o.SnapshotStore == nil || o.HolderReader == nil {
		return PlanExecutorOptions{}, fmt.Errorf("%w: store, manager, authority, snapshot and holder readers are required", ErrExecutorInvalid)
	}
	if o.SingleController.Contract != SingleControllerContract || o.SingleController.ControllerID == "" || o.SingleController.BootID == "" {
		return PlanExecutorOptions{}, fmt.Errorf("%w: explicit single-controller attestation required", ErrExecutorInvalid)
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.OperationTimeout <= 0 {
		o.OperationTimeout = defaultExecutorTimeout
	}
	if o.OperationTimeout > maxExecutorTimeout {
		return PlanExecutorOptions{}, fmt.Errorf("%w: operation timeout", ErrExecutorInvalid)
	}
	if o.PlanTTL <= 0 {
		o.PlanTTL = defaultExecutorPlanTTL
	}
	if o.PlanTTL > 7*24*time.Hour {
		return PlanExecutorOptions{}, fmt.Errorf("%w: plan ttl", ErrExecutorInvalid)
	}
	if o.OperationTimeout > time.Duration(1<<63-1)-executorTTLSafety || o.PlanTTL <= o.OperationTimeout+executorTTLSafety {
		return PlanExecutorOptions{}, fmt.Errorf("%w: plan ttl must cover operation timeout and safety", ErrExecutorInvalid)
	}
	if err := o.ControlMutation.Validate(); err != nil {
		return PlanExecutorOptions{}, err
	}
	if o.LeaseCheckMode == "" {
		o.LeaseCheckMode = actorrouting.LeaseCheckSnapshot
	}
	if o.LeaseCheckMode != actorrouting.LeaseCheckSnapshot && o.LeaseCheckMode != actorrouting.LeaseCheckAuthorityDelegated {
		return PlanExecutorOptions{}, fmt.Errorf("%w: lease check mode", ErrExecutorInvalid)
	}
	return o, nil
}

type PlanExecutor struct {
	store           PlanStore
	manager         AssignmentManager
	authorityReader AuthorityReader
	snapshotStore   core.RouteSnapshotStore
	holderReader    HolderReader
	drainFactory    DrainControllerFactory
	now             func() time.Time
	timeout         time.Duration
	planTTL         time.Duration
	controller      SingleControllerAttestation
	verification    VerificationPort
	executionGuard  ExecutionGuard
	control         ControlMutationOptions
	leaseCheckMode  actorrouting.LeaseCheckMode

	mu     sync.Mutex
	shards map[shardKey]string
	// recoveryGate keeps periodic durable recovery from stealing a freshly
	// persisted foreground batch between Submit and Execute. It does not
	// serialize Shards inside the batch; their per-shard locks remain
	// independent and execute concurrently.
	recoveryGateOnce sync.Once
	recoveryGate     chan struct{}
	foregroundMu     sync.Mutex
	foreground       *foregroundBatch

	// Test seams. Zero selects the production sliding-window gather defaults.
	slidingRecoveryWindow            int
	legacyRecoveryGatherTimeout      time.Duration
	legacyRecoveryGatherPollInterval time.Duration
}

type foregroundBatch struct {
	createdAtMs int64
	incomplete  map[shardKey]PlanRecord
}

// BeginForegroundBatch excludes the generic recovery owner while a caller
// persists a bounded set of intents and starts their executions. The returned
// release is idempotent so callers can defer it safely.
func (e *PlanExecutor) BeginForegroundBatch(ctx context.Context) (func(), error) {
	if e == nil || ctx == nil {
		return nil, ErrExecutorInvalid
	}
	releaseGate, err := e.acquireRecoveryGate(ctx)
	if err != nil {
		return nil, err
	}
	records, err := e.store.ListIncomplete(ctx)
	if err != nil {
		releaseGate()
		return nil, err
	}
	batch := &foregroundBatch{createdAtMs: e.now().UnixMilli(), incomplete: make(map[shardKey]PlanRecord, len(records))}
	for _, record := range records {
		key := shardKey{domain: record.Plan.Domain, shard: record.Plan.VirtualShard}
		if _, exists := batch.incomplete[key]; exists {
			releaseGate()
			return nil, fmt.Errorf("%w: multiple incomplete plans for %s shard %d", ErrPlanCorrupt, key.domain, key.shard)
		}
		batch.incomplete[key] = record
	}
	e.foregroundMu.Lock()
	if e.foreground != nil {
		e.foregroundMu.Unlock()
		releaseGate()
		return nil, ErrExecutorInvalid
	}
	e.foreground = batch
	e.foregroundMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			e.foregroundMu.Lock()
			if e.foreground == batch {
				e.foreground = nil
			}
			e.foregroundMu.Unlock()
			releaseGate()
		})
	}, nil
}

func (e *PlanExecutor) acquireRecoveryGate(ctx context.Context) (func(), error) {
	if e == nil || ctx == nil {
		return nil, ErrExecutorInvalid
	}
	e.recoveryGateOnce.Do(func() {
		e.recoveryGate = make(chan struct{}, 1)
		e.recoveryGate <- struct{}{}
	})
	select {
	case <-e.recoveryGate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	var once sync.Once
	return func() { once.Do(func() { e.recoveryGate <- struct{}{} }) }, nil
}

func (e *PlanExecutor) foregroundSnapshot(key shardKey) (int64, PlanRecord, bool, bool) {
	e.foregroundMu.Lock()
	defer e.foregroundMu.Unlock()
	if e.foreground == nil {
		return 0, PlanRecord{}, false, false
	}
	record, found := e.foreground.incomplete[key]
	return e.foreground.createdAtMs, record, found, true
}

func (e *PlanExecutor) rememberForeground(record PlanRecord) {
	e.foregroundMu.Lock()
	defer e.foregroundMu.Unlock()
	if e.foreground == nil {
		return
	}
	e.foreground.incomplete[shardKey{domain: record.Plan.Domain, shard: record.Plan.VirtualShard}] = record
}

type shardKey struct {
	domain actorrouting.RouteDomain
	shard  uint32
}

func (e *PlanExecutor) authorityWaveManager() (AuthorityWaveBatchManager, bool) {
	if e == nil || e.manager == nil {
		return nil, false
	}
	manager, ok := e.manager.(AuthorityWaveBatchManager)
	return manager, ok
}

func (e *PlanExecutor) authorityWaveBatchContains(waveID string, domain actorrouting.RouteDomain, shard uint32) bool {
	manager, ok := e.authorityWaveManager()
	return ok && manager.AuthorityWaveBatchContains(waveID, domain, shard)
}

func (e *PlanExecutor) authorityWaveBatchBaseRevision(waveID string) (uint64, bool) {
	manager, ok := e.authorityWaveManager()
	if !ok {
		return 0, false
	}
	return manager.AuthorityWaveBatchBaseRevision(waveID)
}

func (e *PlanExecutor) authorityWaveBatchStage(waveID string) (core.AuthorityWaveBatchStage, bool) {
	manager, ok := e.authorityWaveManager()
	if !ok {
		return core.AuthorityWaveBatchStageInvalid, false
	}
	return manager.AuthorityWaveBatchStage(waveID)
}

func (e *PlanExecutor) authorityWaveActive(plan Plan) bool {
	if plan.OwnerWaveID == "" {
		return false
	}
	_, active := e.authorityWaveBatchBaseRevision(plan.OwnerWaveID)
	return active
}

func (e *PlanExecutor) canReplayPendingProjection(plan Plan) bool {
	if plan.OwnerWaveID == "" {
		// Newly created ScalePlan children publish independently per shard.
		// Unowned/manual plans retain the older operator-review boundary.
		return plan.OwnerScalePlanID != ""
	}
	return e.authorityWaveActive(plan)
}

func (e *PlanExecutor) holderForPlan(ctx context.Context, plan Plan, holderID string) (core.HolderRecord, bool, error) {
	// ScalePlan resolves and authenticates every exact PodUID/workload UID/boot
	// generation before it persists the child RoutePlan. Keep the formerly
	// wave-owned execution path on that verified TTL-bound view: synchronously
	// refreshing the whole route domain for every independent child couples
	// unrelated shards again and makes one Snapshot publication invalidate the
	// other sliding-window members while Holders reconcile it.
	useCached := plan.OwnerScalePlanID != "" ||
		(plan.Operation == OperationCrashTakeover && plan.TargetDeploymentIdentity.Exact())
	if !useCached && plan.OwnerWaveID != "" {
		_, useCached = e.authorityWaveBatchBaseRevision(plan.OwnerWaveID)
	}
	return e.readHolder(ctx, plan.Domain, holderID, useCached)
}

func (e *PlanExecutor) readHolder(ctx context.Context, domain actorrouting.RouteDomain, holderID string, useCached bool) (core.HolderRecord, bool, error) {
	if useCached {
		if cached, ok := e.holderReader.(CachedHolderReader); ok {
			return cached.GetCached(ctx, domain, holderID)
		}
	}
	return e.holderReader.Get(ctx, domain, holderID)
}

func NewPlanExecutor(options PlanExecutorOptions) (*PlanExecutor, error) {
	normalized, err := options.normalized()
	if err != nil {
		return nil, err
	}
	executor := &PlanExecutor{
		store: normalized.Store, manager: normalized.Manager, authorityReader: normalized.AuthorityReader,
		snapshotStore: normalized.SnapshotStore, holderReader: normalized.HolderReader,
		drainFactory: normalized.DrainControllerFactory, now: normalized.Now,
		timeout: normalized.OperationTimeout, planTTL: normalized.PlanTTL,
		controller: normalized.SingleController, verification: normalized.Verification,
		executionGuard: normalized.ExecutionGuard,
		control:        normalized.ControlMutation,
		leaseCheckMode: normalized.LeaseCheckMode,
		shards:         make(map[shardKey]string),
	}
	executor.recoveryGateOnce.Do(func() {
		executor.recoveryGate = make(chan struct{}, 1)
		executor.recoveryGate <- struct{}{}
	})
	return executor, nil
}

// MoveRequest is the complete explicit planned-drain submission. IDs and the
// source fence are never generated from a plan or a snapshot by the executor.
type MoveRequest struct {
	PlanID                   string
	TransitionID             string
	Domain                   actorrouting.RouteDomain
	VirtualShard             uint32
	SourceHolderID           string
	SourceBootID             string
	SourceDeploymentIdentity holderidentity.DeploymentIdentity
	SourceEndpoint           string
	TargetHolderID           string
	TargetBootID             string
	TargetDeploymentIdentity holderidentity.DeploymentIdentity
	TargetEndpoint           string
	OwnerScalePlanID         string
	OwnerRetainUntilMs       int64
	ExpectedSourceFence      authoritystore.ShardWriteFence
	RequestedTTL             time.Duration
}

type TakeoverRequest struct {
	PlanID                   string
	TransitionID             string
	Domain                   actorrouting.RouteDomain
	VirtualShard             uint32
	SourceHolderID           string
	SourceBootID             string
	SourceEndpoint           string
	TargetHolderID           string
	TargetBootID             string
	TargetDeploymentIdentity holderidentity.DeploymentIdentity
	TargetEndpoint           string
	ExpectedSourceFence      authoritystore.ShardWriteFence
	RequestedTTL             time.Duration
	Proof                    TakeoverProof
}

func (e *PlanExecutor) SubmitMove(ctx context.Context, request MoveRequest) (PlanRecord, error) {
	// ScalePlan children are persisted by the in-process routePlanAdapter,
	// which owns the parent/child construction boundary. Do not let a direct
	// caller mint that trusted provenance by supplying an arbitrary owner ID.
	if request.OwnerScalePlanID != "" || request.OwnerRetainUntilMs != 0 ||
		!request.SourceDeploymentIdentity.IsZero() || !request.TargetDeploymentIdentity.IsZero() {
		return PlanRecord{}, fmt.Errorf("%w: owned child must be created by ScalePlan", ErrInvalidPlan)
	}
	return e.submit(ctx, request.PlanID, request.TransitionID, request.Domain, request.VirtualShard,
		request.SourceHolderID, request.SourceBootID, request.SourceDeploymentIdentity, request.SourceEndpoint,
		request.TargetHolderID, request.TargetBootID, request.TargetDeploymentIdentity, request.TargetEndpoint,
		request.OwnerScalePlanID, request.OwnerRetainUntilMs, request.ExpectedSourceFence, request.RequestedTTL,
		OperationPlannedDrain, TakeoverProof{})
}

func (e *PlanExecutor) SubmitPlannedDrain(ctx context.Context, request MoveRequest) (PlanRecord, error) {
	return e.SubmitMove(ctx, request)
}

func (e *PlanExecutor) SubmitCrashTakeover(ctx context.Context, request TakeoverRequest) (PlanRecord, error) {
	return e.submit(ctx, request.PlanID, request.TransitionID, request.Domain, request.VirtualShard,
		request.SourceHolderID, request.SourceBootID, holderidentity.DeploymentIdentity{}, request.SourceEndpoint,
		request.TargetHolderID, request.TargetBootID, request.TargetDeploymentIdentity, request.TargetEndpoint,
		"", 0, request.ExpectedSourceFence, request.RequestedTTL, OperationCrashTakeover, request.Proof)
}

func (e *PlanExecutor) SubmitTakeover(ctx context.Context, request TakeoverRequest) (PlanRecord, error) {
	return e.SubmitCrashTakeover(ctx, request)
}

func (e *PlanExecutor) submit(ctx context.Context, planID, transitionID string, domain actorrouting.RouteDomain, shard uint32,
	sourceHolderID, sourceBootID string, sourceIdentity holderidentity.DeploymentIdentity, sourceEndpoint,
	targetHolderID, targetBootID string, targetIdentity holderidentity.DeploymentIdentity, targetEndpoint,
	ownerScalePlanID string, ownerRetainUntilMs int64, fence authoritystore.ShardWriteFence,
	requestedTTL time.Duration, operation OperationKind, proof TakeoverProof) (record PlanRecord, returnErr error) {
	if e == nil {
		return PlanRecord{}, ErrExecutorInvalid
	}
	bounded, cancel, err := e.operationContext(ctx)
	if err != nil {
		return PlanRecord{}, err
	}
	defer cancel()
	controlCtx, resolveControl, err := e.control.Begin(
		bounded,
		e.submitControlOperationID(bounded, planID, domain, targetHolderID, targetBootID, targetIdentity),
		scaleplan.ControlOperationRoutePlan,
	)
	if err != nil {
		return PlanRecord{}, err
	}
	bounded = controlCtx
	controlOutcomeKnown := true
	defer func() {
		if resolveControl != nil && (returnErr == nil || controlOutcomeKnown) {
			if resolveErr := resolveControl(); resolveErr != nil {
				if returnErr == nil {
					returnErr = resolveErr
				} else {
					returnErr = errors.Join(returnErr, resolveErr)
				}
			}
		}
	}()
	if e.executionGuard != nil {
		release, err := e.executionGuard.Enter(bounded)
		if err != nil {
			return PlanRecord{}, err
		}
		if release == nil {
			return PlanRecord{}, ErrExecutorInvalid
		}
		defer release()
	}
	if requestedTTL <= 0 || requestedTTL < time.Millisecond {
		return PlanRecord{}, fmt.Errorf("%w: requested ttl", ErrInvalidPlan)
	}
	if err := e.validateSubmission(bounded, domain, shard, sourceHolderID, sourceBootID, sourceIdentity, sourceEndpoint,
		targetHolderID, targetBootID, targetIdentity, targetEndpoint, fence, operation, proof); err != nil {
		return PlanRecord{}, err
	}
	if err := validateSubmissionIDs(planID, transitionID); err != nil {
		return PlanRecord{}, err
	}
	key := shardKey{domain: domain, shard: shard}
	now := e.now().UnixMilli()
	foregroundNow, _, _, foreground := e.foregroundSnapshot(key)
	if operation == OperationCrashTakeover && foreground {
		now = foregroundNow
	}
	expiresAt, ok := safeAddMilliseconds(now, e.planTTL.Milliseconds())
	if !ok {
		return PlanRecord{}, fmt.Errorf("%w: plan expiry overflow", ErrInvalidPlan)
	}
	plan := Plan{
		PlanID: planID, TransitionID: transitionID, Domain: domain, VirtualShard: shard,
		SourceHolderID: sourceHolderID, SourceBootID: sourceBootID,
		SourceDeploymentIdentity: sourceIdentity, SourceEndpoint: sourceEndpoint,
		TargetHolderID: targetHolderID, TargetBootID: targetBootID,
		TargetDeploymentIdentity: targetIdentity, TargetEndpoint: targetEndpoint,
		ExpectedSourceFence: fence, RequestedTTLms: requestedTTL.Milliseconds(), Operation: operation,
		Phase: PhasePending, Attempt: 0, Revision: 1, CreatedAtMs: now, UpdatedAtMs: now,
		ExpiresAtMs: expiresAt, OwnerScalePlanID: ownerScalePlanID, OwnerRetainUntilMs: ownerRetainUntilMs,
		TakeoverProof: proof,
	}
	if err := plan.Validate(); err != nil {
		return PlanRecord{}, err
	}
	if err := e.acquireShard(key, "submit:"+planID); err != nil {
		return PlanRecord{}, err
	}
	defer e.releaseShard(key)

	if operation == OperationCrashTakeover && foreground {
		_, current, found, active := e.foregroundSnapshot(key)
		if !active {
			return PlanRecord{}, ErrExecutorInvalid
		}
		if found {
			if current.Plan.PlanID == planID && sameSubmission(current.Plan, plan) {
				return current, nil
			}
			return PlanRecord{}, ErrShardBusy
		}
	} else {
		incomplete, err := e.store.ListIncomplete(bounded)
		if err != nil {
			return PlanRecord{}, err
		}
		for _, record := range incomplete {
			if record.Plan.Domain == domain && record.Plan.VirtualShard == shard {
				if record.Plan.PlanID == planID && sameSubmission(record.Plan, plan) {
					return record, nil
				}
				return PlanRecord{}, ErrShardBusy
			}
		}
	}
	existing, found, err := e.store.Load(bounded, planID)
	if err != nil {
		return PlanRecord{}, err
	}
	if found {
		if sameSubmission(existing.Plan, plan) {
			e.rememberForeground(existing)
			return existing, nil
		}
		return PlanRecord{}, ErrPlanConflict
	}
	// CompareAndSwap is the only submit-side mutation with a potentially unknown
	// transport outcome. Every validation/read failure before this point is safe
	// to resolve immediately and must not poison the shared control gate.
	controlOutcomeKnown = false
	result, err := e.store.CompareAndSwap(bounded, 0, plan)
	if err == nil || errors.Is(err, ErrPlanConflict) || errors.Is(err, ErrPlanCapacity) || errors.Is(err, ErrPlanCorrupt) {
		// Conflict/capacity/corruption replies are explicit fail-before-write
		// outcomes from the PlanStore contract. In particular, Redis creation may
		// detect that an existing RoutePlan changed while its bounded index was
		// being reconciled; that is retryable concurrency, not an unknown commit.
		// Only transport/cancellation failures can leave the CAS outcome unknown.
		controlOutcomeKnown = true
	}
	if err != nil && !errors.Is(err, ErrPlanConflict) {
		return PlanRecord{}, err
	}
	if result.Status == CASCommitted || result.Status == CASAlreadyCommitted {
		e.rememberForeground(result.Record)
		return result.Record, nil
	}
	return PlanRecord{}, ErrPlanConflict
}

// submitControlOperationID versions the control-plane attempt independently
// from the durable Plan ID. A Plan ID remains stable for idempotency, while a
// pre-write validation failure (for example a WARMING target whose heartbeat
// has not caught the current RouteSnapshot yet) must be retryable after the
// target generation advances. Binding the attempt to the observed Route and
// Holder revisions preserves deterministic replay within one observation and
// yields a fresh gate operation only after the relevant control facts change.
func (e *PlanExecutor) submitControlOperationID(ctx context.Context, planID string, domain actorrouting.RouteDomain,
	targetHolderID, targetBootID string, targetIdentity holderidentity.DeploymentIdentity) string {
	routeRevision := e.manager.CurrentSnapshot().GlobalRevision
	var holderSequence, holderSnapshotRevision uint64
	if record, found, err := e.readHolder(ctx, domain, targetHolderID, targetIdentity.Exact()); err == nil && found &&
		record.HolderID == targetHolderID && record.BootID == targetBootID {
		holderSequence = record.Sequence
		holderSnapshotRevision = record.SnapshotRevision
	}
	logicalID := fmt.Sprintf("%s/route-%d/target-sequence-%d/target-snapshot-%d",
		planID, routeRevision, holderSequence, holderSnapshotRevision)
	return planControlOperationID("submit", logicalID)
}

func validateSubmissionIDs(planID, transitionID string) error {
	if err := ValidatePlanID(planID); err != nil {
		return err
	}
	if err := validateOpaque("transition_id", transitionID, MaxTransitionIDBytes); err != nil {
		return err
	}
	return nil
}

func sameSubmission(left, right Plan) bool {
	return left.PlanID == right.PlanID && left.TransitionID == right.TransitionID && left.Domain == right.Domain &&
		left.VirtualShard == right.VirtualShard && left.SourceHolderID == right.SourceHolderID && left.SourceBootID == right.SourceBootID &&
		left.SourceDeploymentIdentity == right.SourceDeploymentIdentity && left.SourceEndpoint == right.SourceEndpoint &&
		left.TargetHolderID == right.TargetHolderID && left.TargetBootID == right.TargetBootID &&
		left.TargetDeploymentIdentity == right.TargetDeploymentIdentity && left.TargetEndpoint == right.TargetEndpoint &&
		left.OwnerScalePlanID == right.OwnerScalePlanID && left.OwnerRetainUntilMs == right.OwnerRetainUntilMs &&
		left.OwnerWaveID == right.OwnerWaveID && left.OwnerWaveSize == right.OwnerWaveSize && left.OwnerWavePosition == right.OwnerWavePosition &&
		left.ExpectedSourceFence == right.ExpectedSourceFence && left.RequestedTTLms == right.RequestedTTLms &&
		left.Operation == right.Operation && left.TakeoverProof == right.TakeoverProof
}

func (e *PlanExecutor) validateSubmission(ctx context.Context, domain actorrouting.RouteDomain, shard uint32,
	sourceHolderID, sourceBootID string, sourceIdentity holderidentity.DeploymentIdentity, sourceEndpoint,
	targetHolderID, targetBootID string, targetIdentity holderidentity.DeploymentIdentity, targetEndpoint string,
	fence authoritystore.ShardWriteFence, operation OperationKind, proof TakeoverProof) error {
	if domain != actorrouting.DomainPlayer &&
		domain != actorrouting.DomainFarm &&
		domain != actorrouting.DomainFarmWorker {
		return fmt.Errorf("%w: domain", ErrInvalidPlan)
	}
	if err := validateOpaque("source_holder_id", sourceHolderID, MaxHolderIDBytes); err != nil {
		return err
	}
	if err := validateOpaque("source_boot_id", sourceBootID, MaxHolderIDBytes); err != nil {
		return err
	}
	if err := netendpoint.Validate(sourceEndpoint, MaxEndpointBytes); err != nil {
		return fmt.Errorf("%w: source endpoint must be canonical host:port", ErrInvalidPlan)
	}
	if err := validateOpaque("target_holder_id", targetHolderID, MaxHolderIDBytes); err != nil {
		return err
	}
	if err := validateOpaque("target_boot_id", targetBootID, MaxHolderIDBytes); err != nil {
		return err
	}
	if err := netendpoint.Validate(targetEndpoint, MaxEndpointBytes); err != nil {
		return fmt.Errorf("%w: target endpoint must be canonical host:port", ErrInvalidPlan)
	}
	if sourceHolderID == targetHolderID && sourceBootID == targetBootID {
		return fmt.Errorf("%w: source and target are identical", ErrInvalidPlan)
	}
	if fence.ExpectedHolderID != sourceHolderID || fence.ExpectedHolderBootID != sourceBootID ||
		fence.ExpectedAssignmentRevision == 0 || fence.ExpectedWriterEpoch == 0 || fence.ExpectedLeaseEpoch == 0 {
		return fmt.Errorf("%w: complete expected source fence", ErrInvalidPlan)
	}
	if operation == OperationCrashTakeover {
		if err := proof.Validate(sourceBootID); err != nil {
			return err
		}
	} else if operation != OperationPlannedDrain || proof != (TakeoverProof{}) {
		return fmt.Errorf("%w: operation", ErrInvalidPlan)
	}
	if e.holderReader == nil {
		return fmt.Errorf("%w: holder reader required", ErrExecutorInvalid)
	}
	authorityValue, found, err := e.authorityReader.ReadShardWriteAuthority(ctx, domain, shard)
	if err != nil {
		return err
	}
	if !found || authorityValue.State != authoritystore.ShardAuthorityActive || authorityValue.Domain != domain || authorityValue.VirtualShard != shard {
		return fmt.Errorf("%w: source authority unavailable", ErrTakeoverProof)
	}
	if !sameFence(authorityValue, fence) {
		return fmt.Errorf("%w: source authority fence", ErrPlanConflict)
	}
	if authorityValue.HolderID != sourceHolderID || authorityValue.HolderBootID != sourceBootID {
		return fmt.Errorf("%w: source authority holder", ErrPlanConflict)
	}
	if err := e.validateSnapshotSource(domain, shard, sourceHolderID, sourceBootID, sourceEndpoint); err != nil {
		return err
	}
	if operation == OperationPlannedDrain {
		sourceRecord, sourceFound, sourceErr := e.holderReader.Get(ctx, domain, sourceHolderID)
		if sourceErr != nil {
			return sourceErr
		}
		if !sourceFound || !canonicalHolderEndpoints(sourceRecord) || sourceRecord.Domain != domain || sourceRecord.HolderID != sourceHolderID ||
			sourceRecord.BootID != sourceBootID || sourceRecord.LogicEndpoint != sourceEndpoint || sourceRecord.HolderControlEndpoint == "" ||
			sourceRecord.TLSDNSIdentity == "" || sourceRecord.CapacityWeight == 0 || !plannedRuntimeAdmissible(sourceRecord, false) ||
			!sourceRecord.LeaseUntil.After(e.now()) {
			return fmt.Errorf("%w: source holder is not serving", ErrPlanConflict)
		}
		if !sourceIdentity.IsZero() && sourceIdentity != sourceRecord.DeploymentIdentity {
			return fmt.Errorf("%w: source deployment identity", ErrPlanConflict)
		}
	}
	if operation == OperationPlannedDrain && authorityValue.LeaseUntilMs <= e.now().UnixMilli() {
		return fmt.Errorf("%w: source lease expired", ErrPlanConflict)
	}
	if err := e.validateTarget(ctx, domain, targetHolderID, targetBootID, targetIdentity, targetEndpoint,
		operation == OperationPlannedDrain || targetIdentity.Exact(), operation == OperationCrashTakeover); err != nil {
		return err
	}
	if operation == OperationCrashTakeover {
		if err := e.validateTakeoverProof(ctx, domain, authorityValue, sourceHolderID, sourceBootID, proof, targetIdentity.Exact()); err != nil {
			return err
		}
	}
	return nil
}

func sameFence(value authoritystore.ShardWriteAuthority, fence authoritystore.ShardWriteFence) bool {
	return value.HolderID == fence.ExpectedHolderID && value.HolderBootID == fence.ExpectedHolderBootID &&
		value.AssignmentRevision == fence.ExpectedAssignmentRevision && value.WriterEpoch == fence.ExpectedWriterEpoch &&
		value.LeaseEpoch == fence.ExpectedLeaseEpoch
}

func (e *PlanExecutor) validateSnapshotSource(domain actorrouting.RouteDomain, shard uint32, holderID, bootID, endpoint string) error {
	snapshot := e.manager.CurrentSnapshot()
	route, holder, err := snapshotRoute(snapshot, domain, shard)
	if err != nil {
		return err
	}
	if route.State != actorrouting.ShardStateActive || holder.InstanceID != holderID || holder.BootID != bootID || holder.Endpoint != endpoint {
		return fmt.Errorf("%w: source snapshot tuple", ErrPlanConflict)
	}
	return nil
}

func (e *PlanExecutor) validateTarget(ctx context.Context, domain actorrouting.RouteDomain, holderID, bootID string,
	identity holderidentity.DeploymentIdentity, endpoint string, requireDeploymentIdentity, allowColdWarming bool) error {
	if ctx == nil {
		return ErrExecutorInvalid
	}
	// Freeze the minimum revision before the first Holder read. Deadline
	// publications may advance the global revision while this submission is
	// waiting for the same immutable target generation to catch up.
	snapshotRevision := e.manager.CurrentSnapshot().GlobalRevision
	const retryBackoff = 50 * time.Millisecond
	timer := time.NewTimer(retryBackoff)
	defer timer.Stop()
	for {
		record, found, err := e.readHolder(ctx, domain, holderID, identity.Exact())
		if err != nil && !errors.Is(err, core.ErrHolderRegistryUnavailable) {
			return err
		}
		if err == nil {
			if !found || !canonicalHolderEndpoints(record) || record.Domain != domain || record.HolderID != holderID || record.BootID != bootID ||
				(requireDeploymentIdentity && !identity.IsZero() && record.DeploymentIdentity != identity) || record.LogicEndpoint != endpoint ||
				record.HolderControlEndpoint == "" ||
				record.TLSDNSIdentity == "" || record.CapacityWeight == 0 ||
				!targetRuntimeAdmissible(record, allowColdWarming, false) ||
				!record.LeaseUntil.After(e.now()) {
				return fmt.Errorf("%w: target holder not ready", ErrPlanConflict)
			}
			if record.SnapshotRevision >= snapshotRevision {
				return nil
			}
		}
		timer.Reset(retryBackoff)
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("%w: target holder not ready", ErrPlanConflict)
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (e *PlanExecutor) validateTakeoverProof(ctx context.Context, domain actorrouting.RouteDomain, value authoritystore.ShardWriteAuthority,
	sourceHolderID, sourceBootID string, proof TakeoverProof, useCached bool) error {
	if proof.ExpectedHolderBootID != sourceBootID || (!proof.HolderUnavailable && !proof.LeaseExpired) || proof.Evidence == "" {
		return ErrTakeoverProof
	}
	if proof.LeaseExpired && value.LeaseUntilMs > e.now().UnixMilli() {
		return fmt.Errorf("%w: authority lease is still active", ErrTakeoverProof)
	}
	record, found, err := e.readHolder(ctx, domain, sourceHolderID, useCached)
	if err != nil {
		return err
	}
	if proof.HolderUnavailable && found && record.BootID == sourceBootID && record.LeaseUntil.After(e.now()) && !proof.LeaseExpired {
		return fmt.Errorf("%w: old holder is live", ErrTakeoverProof)
	}
	return nil
}

// retryablePauseCode reports whether a paused plan failed with a transient
// error that is safe to re-execute. Authority-committed-snapshot-pending is
// handled separately by the wave path (its Authority is already written and
// re-execution only re-buffers the projection).
func retryablePauseCode(code string) bool {
	switch code {
	case "CANCELED", "TIMEOUT", "PROJECTION_RECOVERY":
		return true
	default:
		return false
	}
}

// retryableCrashTakeoverPause is narrower than the ScalePlan child policy.
// Execute reconciles the immutable per-shard transition before reaching this
// branch: a committed Authority resumes only its pending projection, while an
// unchanged source fence safely replays the same crash proof and target.
// EXECUTION_FAILED is accepted only for records written before projection
// recovery had its own stable error code.
func retryableCrashTakeoverPause(plan Plan) bool {
	return plan.Operation == OperationCrashTakeover &&
		(retryablePauseCode(plan.ErrorCode) || plan.ErrorCode == "EXECUTION_FAILED")
}

func pausedPlanShouldRetry(plan Plan) bool {
	if retryableCrashTakeoverPause(plan) {
		return true
	}
	if plan.OwnerScalePlanID == "" {
		return false
	}
	return retryablePauseCode(plan.ErrorCode) || plan.ErrorCode == ErrorAuthorityCommittedSnapshotPending
}

func (e *PlanExecutor) Execute(ctx context.Context, planID string) (PlanRecord, error) {
	return e.execute(ctx, planID, false)
}

func (e *PlanExecutor) execute(ctx context.Context, planID string, authorityWaveGuardHeld bool) (record PlanRecord, returnErr error) {
	if e == nil {
		return PlanRecord{}, ErrExecutorInvalid
	}
	bounded, cancel, err := e.operationContext(ctx)
	if err != nil {
		return PlanRecord{}, err
	}
	defer cancel()
	record, found, err := e.store.Load(bounded, planID)
	if err != nil {
		return PlanRecord{}, err
	}
	if !found {
		return PlanRecord{}, ErrPlanNotFound
	}
	// Serialize the shard before acquiring a control token. Background recovery
	// can observe the same durable plan while its foreground AutoFailover or
	// ScalePlan execution is still running. Treat that overlap as a local busy
	// result without allocating an operation lease: otherwise every harmless
	// overlap leaves an unresolved token which eventually expires and poisons
	// the process-wide mutation coordinator.
	key := shardKey{domain: record.Plan.Domain, shard: record.Plan.VirtualShard}
	if err := e.acquireShard(key, "execute:"+planID); err != nil {
		return record, err
	}
	defer e.releaseShard(key)
	// Terminal no-op outcomes must not consume execute/<plan>/revision-N. A
	// PAUSED record is Terminal(), so Begin-then-return would resolve that ID
	// and poison every later Recover of the same revision with
	// "operation already resolved".
	switch record.Plan.Phase {
	case PhaseSucceeded:
		return record, nil
	case PhaseFailed:
		return record, ErrPlanFailed
	case PhasePaused:
		if !pausedPlanShouldRetry(record.Plan) {
			state, _, _, reconcileErr := e.reconcile(bounded, record.Plan)
			if reconcileErr == nil && state == reconcileComplete {
				return record, nil
			}
			return record, ErrPlanPaused
		}
		record, err = e.unpauseRetryablePlan(bounded, record)
		if err != nil {
			return record, err
		}
	}
	executionID := fmt.Sprintf("%s/revision-%d", planID, record.Plan.Revision)
	controlCtx, resolveControl, err := e.control.Begin(bounded, planControlOperationID("execute", executionID), scaleplan.ControlOperationRoutePlan)
	if err != nil {
		return record, err
	}
	bounded = controlCtx
	defer func() {
		// A successfully persisted terminal record is the durable resolution of
		// this RoutePlan attempt even when the returned error explains FAILED or
		// PAUSED. A wave member that has buffered its projection also returns
		// nil while still non-terminal; that execute attempt is complete. Only a
		// non-terminal error keeps the token unresolved because its external
		// mutation outcome may still be unknown.
		if resolveControl != nil && (returnErr == nil || record.Plan.Phase.Terminal()) {
			if resolveErr := resolveControl(); resolveErr != nil {
				if returnErr == nil {
					returnErr = resolveErr
				} else {
					returnErr = errors.Join(returnErr, resolveErr)
				}
			}
		}
	}()
	state, _, _, reconcileErr := e.reconcile(bounded, record.Plan)
	if reconcileErr == nil && state == reconcileComplete {
		return e.finishReconciledPlan(bounded, record.Plan.PlanID)
	}
	if e.executionGuard != nil && !authorityWaveGuardHeld {
		release, guardErr := e.executionGuard.Enter(bounded)
		if guardErr != nil {
			if !record.Plan.Phase.Terminal() {
				// Manager-owned projection recovery may fail after Authority has
				// committed but before the matching Snapshot is visible. Reconcile
				// with an independent bounded context before freezing the terminal
				// audit record so this outcome keeps its mandatory exact reason.
				code := stableErrorCode(guardErr)
				reconcileCtx, cancel := e.persistenceContext()
				pendingState, _, _, pendingErr := e.reconcile(reconcileCtx, record.Plan)
				cancel()
				if pendingErr == nil && pendingState == reconcileAuthorityCommittedSnapshotPending {
					code = ErrorAuthorityCommittedSnapshotPending
				}
				return e.pausePlan(record, code, guardErr)
			}
			return record, guardErr
		}
		if release == nil {
			return record, ErrExecutorInvalid
		}
		defer release()
	}
	return e.executeBounded(bounded, record)
}

func (e *PlanExecutor) unpauseRetryablePlan(ctx context.Context, record PlanRecord) (PlanRecord, error) {
	if err := e.unpauseForRetry(ctx, record.Plan.PlanID); err != nil {
		return record, err
	}
	reloaded, found, err := e.store.Load(ctx, record.Plan.PlanID)
	if err != nil {
		return record, err
	}
	if !found {
		return record, ErrPlanNotFound
	}
	return reloaded, nil
}

func (e *PlanExecutor) executeBounded(ctx context.Context, record PlanRecord) (PlanRecord, error) {
	// Logical expiry cannot decide the outcome before durable facts do. Manager
	// may already have committed while the snapshot publication was interrupted;
	// physical audit retention exists specifically so that state can reconcile.
	reconcileState, authorityValue, _, err := e.reconcile(ctx, record.Plan)
	if err != nil {
		return e.pausePlan(record, stableErrorCode(err), err)
	}
	if reconcileState == reconcileAuthorityCommittedSnapshotPending {
		if !e.canReplayPendingProjection(record.Plan) {
			return e.pausePlan(record, ErrorAuthorityCommittedSnapshotPending, ErrVerification)
		}
		// The member already wrote its Authority during an earlier wave attempt
		// whose commit never landed. Re-entering the Manager call replays the
		// exact transition as ALREADY_APPLIED and re-buffers its projection into
		// the current wave, so the sealed batch covers every member. Do not
		// short-circuit here: the projection step is required.
	}
	if reconcileState == reconcileComplete {
		if err := e.requireContextOrPause(ctx, record.Plan.PlanID); err != nil {
			return e.pausePlan(record, "CANCELED", err)
		}
		return e.finishReconciledPlan(ctx, record.Plan.PlanID)
	}
	if record.Plan.ExpiresAtMs <= e.now().UnixMilli() {
		return e.failPlan(record, "PLAN_EXPIRED", ErrPlanFailed)
	}
	plan, err := e.bumpAttempt(ctx, record.Plan.PlanID)
	if err != nil {
		return record, err
	}
	record.Plan = plan
	if err := e.requireContextOrPause(ctx, record.Plan.PlanID); err != nil {
		return e.pausePlan(record, "CANCELED", err)
	}
	planRecord, found, err := e.store.Load(ctx, record.Plan.PlanID)
	if err != nil || !found {
		if err == nil {
			err = ErrPlanNotFound
		}
		return record, err
	}
	plan = planRecord.Plan
	if plan.Operation == OperationPlannedDrain {
		if err := e.advancePhase(ctx, plan.PlanID, PhaseDraining); err != nil {
			return e.failPlan(planRecord, stableErrorCode(err), err)
		}
		var tracking *trackingDrainController
		if plan.Domain != actorrouting.DomainFarmWorker {
			if e.drainFactory == nil {
				return e.failPlan(planRecord, "DRAIN_CONTROLLER_REQUIRED", authority.ErrDrainControllerRequired)
			}
			controller, closeController, controllerErr := e.drainFactory(ctx, plan)
			if controllerErr != nil {
				return e.failPlan(planRecord, stableErrorCode(controllerErr), controllerErr)
			}
			if controller == nil {
				return e.failPlan(planRecord, "DRAIN_CONTROLLER_REQUIRED", authority.ErrDrainControllerRequired)
			}
			if closeController != nil {
				defer func() { _ = closeController() }()
			}
			tracking = &trackingDrainController{
				inner: controller, executor: e, planID: plan.PlanID,
			}
		}
		var finalState reconcileStatus
		planRecord, finalState, err = e.validateImmediatelyBeforeManager(ctx, plan.PlanID)
		if err != nil {
			return e.pausePlan(planRecord, stableErrorCode(err), err)
		}
		if finalState == reconcileComplete {
			return e.finishReconciledPlan(ctx, plan.PlanID)
		}
		if finalState == reconcileAuthorityCommittedSnapshotPending {
			if !e.canReplayPendingProjection(plan) {
				return e.pausePlan(planRecord, ErrorAuthorityCommittedSnapshotPending, ErrVerification)
			}
		}
		plan = planRecord.Plan
		managerRequest, requestErr := ManagerTransitionRequestForPlan(plan)
		if requestErr != nil {
			return e.pausePlan(planRecord, stableErrorCode(requestErr), requestErr)
		}
		managerCtx, managerCancel := context.WithTimeout(ctx, e.timeout)
		defer managerCancel()
		var result authoritystore.ShardAuthorityTransitionResult
		if plan.Domain == actorrouting.DomainFarmWorker {
			result, err = e.manager.HandoffWorkerValidated(
				managerCtx,
				managerRequest.Transfer,
				e.workerHandoffSourceValidator(plan.PlanID),
				e.pendingProjectionTargetValidator(plan.PlanID),
			)
		} else {
			result, err = e.manager.DrainValidated(
				managerCtx,
				managerRequest.Transfer,
				tracking,
				e.pendingProjectionTargetValidator(plan.PlanID),
			)
		}
		if err != nil {
			return e.handleExecutionError(planRecord, err)
		}
		authorityValue = result.Authority
	} else {
		if err := e.advancePhase(ctx, plan.PlanID, PhaseAssigning); err != nil {
			return e.failPlan(planRecord, stableErrorCode(err), err)
		}
		if err := e.validateTakeoverAtExecution(ctx, plan); err != nil {
			return e.pausePlan(planRecord, stableErrorCode(err), err)
		}
		var finalState reconcileStatus
		planRecord, finalState, err = e.validateImmediatelyBeforeManager(ctx, plan.PlanID)
		if err != nil {
			return e.pausePlan(planRecord, stableErrorCode(err), err)
		}
		if finalState == reconcileComplete {
			return e.finishReconciledPlan(ctx, plan.PlanID)
		}
		if finalState == reconcileAuthorityCommittedSnapshotPending {
			if !e.canReplayPendingProjection(plan) {
				return e.pausePlan(planRecord, ErrorAuthorityCommittedSnapshotPending, ErrVerification)
			}
		}
		plan = planRecord.Plan
		managerRequest, requestErr := ManagerTransitionRequestForPlan(plan)
		if requestErr != nil {
			return e.pausePlan(planRecord, stableErrorCode(requestErr), requestErr)
		}
		managerCtx, managerCancel := context.WithTimeout(ctx, e.timeout)
		defer managerCancel()
		result, err := e.manager.TransferValidated(managerCtx, managerRequest.Transfer, e.pendingProjectionTargetValidator(plan.PlanID))
		if err != nil {
			return e.handleExecutionError(planRecord, err)
		}
		authorityValue = result.Authority
	}
	if err := e.requireContextOrPause(ctx, plan.PlanID); err != nil {
		return e.pausePlan(planRecord, "CANCELED", err)
	}
	if err := validateManagerResult(plan, authorityValue); err != nil {
		return e.handleExecutionError(planRecord, err)
	}
	if plan.OwnerWaveID != "" && e.authorityWaveBatchContains(plan.OwnerWaveID, plan.Domain, plan.VirtualShard) {
		buffered, found, loadErr := e.store.Load(ctx, plan.PlanID)
		if loadErr != nil || !found {
			if loadErr == nil {
				loadErr = ErrPlanNotFound
			}
			return planRecord, loadErr
		}
		return buffered, nil
	}
	if err := e.validateActiveSnapshot(ctx, plan, authorityValue); err != nil {
		return e.handleExecutionError(planRecord, err)
	}
	return e.finishPlan(ctx, plan.PlanID)
}

func (e *PlanExecutor) validateTakeoverAtExecution(ctx context.Context, plan Plan) error {
	value, found, err := e.authorityReader.ReadShardWriteAuthority(ctx, plan.Domain, plan.VirtualShard)
	if err != nil {
		return err
	}
	if !found || value.LastTransitionID == plan.TransitionID {
		return nil
	}
	if !sameFence(value, plan.ExpectedSourceFence) {
		return fmt.Errorf("%w: source authority changed", ErrTakeoverProof)
	}
	return e.validateTakeoverProof(ctx, plan.Domain, value, plan.SourceHolderID, plan.SourceBootID, plan.TakeoverProof, plan.TargetDeploymentIdentity.Exact())
}

func validateManagerResult(plan Plan, value authoritystore.ShardWriteAuthority) error {
	if value.Domain != plan.Domain || value.VirtualShard != plan.VirtualShard || value.State != authoritystore.ShardAuthorityActive ||
		value.HolderID != plan.TargetHolderID || value.HolderBootID != plan.TargetBootID || value.LastTransitionID != plan.TransitionID ||
		value.AssignmentRevision == 0 || value.WriterEpoch == 0 || value.LeaseEpoch == 0 || value.LeaseUntilMs <= 0 {
		return fmt.Errorf("%w: manager result tuple", ErrPlanCorrupt)
	}
	return nil
}

type reconcileStatus uint8

const (
	reconcileIncomplete reconcileStatus = iota
	reconcileComplete
	reconcileAuthorityCommittedSnapshotPending
)

func (e *PlanExecutor) reconcile(ctx context.Context, plan Plan) (reconcileStatus, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot, error) {
	value, found, err := e.authorityReader.ReadShardWriteAuthority(ctx, plan.Domain, plan.VirtualShard)
	if err != nil {
		return reconcileIncomplete, authoritystore.ShardWriteAuthority{}, actorrouting.RouteSnapshot{}, err
	}
	snapshot, err := e.loadSnapshot(ctx)
	if err != nil {
		return reconcileIncomplete, value, snapshot, err
	}
	if found && value.State == authoritystore.ShardAuthorityActive && value.HolderID == plan.TargetHolderID &&
		value.HolderBootID == plan.TargetBootID && value.LastTransitionID == plan.TransitionID {
		if routeMatchesAuthority(snapshot, plan, value) {
			return reconcileComplete, value, snapshot, nil
		}
		return reconcileAuthorityCommittedSnapshotPending, value, snapshot, nil
	}
	if found && (!sameFence(value, plan.ExpectedSourceFence) && value.LastTransitionID != plan.TransitionID) {
		return reconcileIncomplete, value, snapshot, fmt.Errorf("%w: durable source fence changed", ErrPlanConflict)
	}
	return reconcileIncomplete, value, snapshot, nil
}

// validateImmediatelyBeforeManager re-reads every mutable dependency after
// phase persistence/controller construction and immediately before the only
// Manager side effect. Any drift leaves the Manager call count at zero.
func (e *PlanExecutor) validateImmediatelyBeforeManager(ctx context.Context, planID string) (PlanRecord, reconcileStatus, error) {
	record, found, err := e.store.Load(ctx, planID)
	if err != nil {
		return PlanRecord{}, reconcileIncomplete, err
	}
	if !found {
		return PlanRecord{}, reconcileIncomplete, ErrPlanNotFound
	}
	plan := record.Plan
	if plan.Phase.Terminal() {
		return record, reconcileIncomplete, ErrPlanConflict
	}
	value, authorityFound, err := e.authorityReader.ReadShardWriteAuthority(ctx, plan.Domain, plan.VirtualShard)
	if err != nil {
		return record, reconcileIncomplete, err
	}
	snapshot, err := e.loadSnapshot(ctx)
	if err != nil {
		return record, reconcileIncomplete, err
	}
	if authorityFound && value.State == authoritystore.ShardAuthorityActive && value.HolderID == plan.TargetHolderID &&
		value.HolderBootID == plan.TargetBootID && value.LastTransitionID == plan.TransitionID {
		if routeMatchesAuthority(snapshot, plan, value) {
			return record, reconcileComplete, nil
		}
		return record, reconcileAuthorityCommittedSnapshotPending, nil
	}
	// Expiry and remaining-TTL checks apply only after current facts prove the
	// transition has not already committed. They still precede every Manager
	// call, so an insufficient budget can never start a new side effect.
	if err := e.requireSideEffectBudget(plan); err != nil {
		return record, reconcileIncomplete, err
	}
	if !authorityFound || value.State != authoritystore.ShardAuthorityActive || value.Domain != plan.Domain ||
		value.VirtualShard != plan.VirtualShard || !sameFence(value, plan.ExpectedSourceFence) ||
		value.HolderID != plan.SourceHolderID || value.HolderBootID != plan.SourceBootID {
		return record, reconcileIncomplete, fmt.Errorf("%w: source authority changed", ErrPlanConflict)
	}
	if plan.Operation == OperationPlannedDrain && value.LeaseUntilMs <= e.now().UnixMilli() {
		return record, reconcileIncomplete, fmt.Errorf("%w: source authority lease expired", ErrPlanConflict)
	}
	if plan.Operation == OperationPlannedDrain {
		sourceRecord, sourceFound, sourceErr := e.holderForPlan(ctx, plan, plan.SourceHolderID)
		if sourceErr != nil {
			return record, reconcileIncomplete, sourceErr
		}
		if !sourceFound || !canonicalHolderEndpoints(sourceRecord) || sourceRecord.Domain != plan.Domain ||
			sourceRecord.HolderID != plan.SourceHolderID || sourceRecord.BootID != plan.SourceBootID ||
			(!plan.SourceDeploymentIdentity.IsZero() && sourceRecord.DeploymentIdentity != plan.SourceDeploymentIdentity) ||
			sourceRecord.LogicEndpoint != plan.SourceEndpoint ||
			!plannedRuntimeAdmissible(sourceRecord, plan.OwnerScalePlanID != "") ||
			!sourceRecord.LeaseUntil.After(e.now()) {
			return record, reconcileIncomplete, fmt.Errorf("%w: source holder endpoints changed", ErrPlanConflict)
		}
	}
	route, source, err := snapshotRoute(snapshot, plan.Domain, plan.VirtualShard)
	if err != nil || route.State != actorrouting.ShardStateActive || source.InstanceID != plan.SourceHolderID ||
		source.BootID != plan.SourceBootID || source.Endpoint != plan.SourceEndpoint {
		return record, reconcileIncomplete, fmt.Errorf("%w: source snapshot changed", ErrPlanConflict)
	}
	if err := e.validateTargetForSnapshot(
		ctx,
		plan,
		initialTargetSnapshotRevision(plan, snapshot.GlobalRevision),
		plan.Operation == OperationCrashTakeover,
	); err != nil {
		return record, reconcileIncomplete, err
	}
	if plan.Operation == OperationCrashTakeover {
		if err := e.validateTakeoverProof(ctx, plan.Domain, value, plan.SourceHolderID, plan.SourceBootID, plan.TakeoverProof, plan.TargetDeploymentIdentity.Exact()); err != nil {
			return record, reconcileIncomplete, err
		}
	}
	return record, reconcileIncomplete, nil
}

func (e *PlanExecutor) validateTargetForSnapshot(
	ctx context.Context,
	plan Plan,
	snapshotRevision uint64,
	allowColdWarming bool,
) error {
	if ctx == nil {
		return ErrExecutorInvalid
	}
	const retryBackoff = 50 * time.Millisecond
	timer := time.NewTimer(retryBackoff)
	defer timer.Stop()
	for {
		ready, err := e.targetReadyForSnapshot(ctx, plan, snapshotRevision, allowColdWarming)
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		timer.Reset(retryBackoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// validateTargetForSnapshotOnce is used from Manager publication callbacks.
// Those callbacks run while Manager state is locked, so they must never poll
// or sleep. Convergence waits are owned by waitPendingProjectionTarget, which
// releases the Manager lock between attempts.
func (e *PlanExecutor) validateTargetForSnapshotOnce(
	ctx context.Context,
	plan Plan,
	snapshotRevision uint64,
	allowColdWarming bool,
) error {
	ready, err := e.targetReadyForSnapshot(ctx, plan, snapshotRevision, allowColdWarming)
	if err != nil {
		return err
	}
	if !ready {
		return fmt.Errorf("%w: target has not applied snapshot revision %d", ErrPlanConflict, snapshotRevision)
	}
	return nil
}

func (e *PlanExecutor) targetReadyForSnapshot(
	ctx context.Context,
	plan Plan,
	snapshotRevision uint64,
	allowColdWarming bool,
) (bool, error) {
	record, found, err := e.holderForPlan(ctx, plan, plan.TargetHolderID)
	if err != nil {
		if errors.Is(err, core.ErrHolderRegistryUnavailable) {
			return false, nil
		}
		return false, err
	}
	if !found {
		return false, nil
	}
	if !canonicalHolderEndpoints(record) ||
		record.Domain != plan.Domain ||
		record.HolderID != plan.TargetHolderID ||
		record.BootID != plan.TargetBootID ||
		(!plan.TargetDeploymentIdentity.IsZero() && record.DeploymentIdentity != plan.TargetDeploymentIdentity) ||
		record.LogicEndpoint != plan.TargetEndpoint ||
		record.HolderControlEndpoint == "" ||
		record.TLSDNSIdentity == "" ||
		record.CapacityWeight == 0 ||
		!targetRuntimeAdmissible(record, allowColdWarming, plan.OwnerScalePlanID != "") ||
		!record.LeaseUntil.After(e.now()) {
		return false, fmt.Errorf("%w: target holder no longer accepts the snapshot", ErrPlanConflict)
	}
	requiredRevision := snapshotRevision
	if plan.OwnerWaveID != "" {
		if baseRevision, ok := e.authorityWaveBatchBaseRevision(plan.OwnerWaveID); ok {
			requiredRevision = baseRevision
		}
	}
	return record.SnapshotRevision >= requiredRevision, nil
}

func initialTargetSnapshotRevision(plan Plan, currentRevision uint64) uint64 {
	if plan.OwnerScalePlanID != "" {
		// ScalePlan already pinned exact holder/deployment identities. Before
		// this shard publishes ASSIGNING, any non-zero locally-applied revision
		// is sufficient; the transition's own durable revision is enforced next.
		return 1
	}
	return currentRevision
}

func plannedRuntimeAdmissible(record core.HolderRecord, allowPinnedReconcile bool) bool {
	if record.ProcessReady && !record.Draining &&
		(record.State == core.HolderStateReadyZeroShard || record.State == core.HolderStateServing) {
		return true
	}
	// ScalePlan resolved and persisted the exact PodUID/workload UID/boot ID
	// before creating this child. During per-shard sliding execution, an
	// unrelated ASSIGNING shard may make assignment-health (and therefore
	// ProcessReady) false. The runtime is still healthy because fatal runtime
	// health sets Draining=true. Permit only that exact, already-pinned serving
	// generation to finish; it remains ineligible for new placement.
	return allowPinnedReconcile && !record.ProcessReady && !record.Draining &&
		record.State == core.HolderStateServing && record.ServingShardCount > 0
}

func targetRuntimeAdmissible(record core.HolderRecord, allowColdWarming, allowPinnedReconcile bool) bool {
	if plannedRuntimeAdmissible(record, allowPinnedReconcile) {
		return true
	}
	if !allowColdWarming || record.ProcessReady || record.Draining {
		return false
	}
	return (record.State == core.HolderStateWarming && record.ServingShardCount == 0) ||
		(record.State == core.HolderStateServing && record.ServingShardCount > 0)
}

func canonicalHolderEndpoints(record core.HolderRecord) bool {
	return netendpoint.Validate(record.LogicEndpoint, core.DefaultHolderMaxEndpointBytes) == nil &&
		netendpoint.Validate(record.HolderControlEndpoint, core.DefaultHolderMaxEndpointBytes) == nil
}

// pendingProjectionTargetValidator binds the Manager-owned live projection
// seam back to the one immutable durable Plan. It is read-only: Manager still
// owns transition generation, Authority reconciliation, and snapshot state.
func (e *PlanExecutor) pendingProjectionTargetValidator(planID string) authority.PendingProjectionTargetValidator {
	return func(ctx context.Context, recovery authority.TransitionRecovery, snapshotRevision uint64) error {
		record, found, err := e.store.Load(ctx, planID)
		if err != nil {
			return err
		}
		if !found || record.Plan.Phase.Terminal() {
			return fmt.Errorf("%w: projection plan unavailable", ErrPlanConflict)
		}
		plan := record.Plan
		plannedDrain := plan.Operation == OperationPlannedDrain
		if recovery.TransitionID != plan.TransitionID || recovery.Domain != plan.Domain ||
			recovery.VirtualShard != plan.VirtualShard || recovery.ExpectedFence != plan.ExpectedSourceFence ||
			recovery.NewHolderID != plan.TargetHolderID || recovery.NewHolderBootID != plan.TargetBootID ||
			recovery.Endpoint != plan.TargetEndpoint || recovery.RequestedTTLms != plan.RequestedTTLms ||
			recovery.InternalDrain != plannedDrain {
			return fmt.Errorf("%w: projection transition identity changed", ErrPlanConflict)
		}
		if plannedDrain {
			expectedMode := authority.TransferModeDrainHandoff
			if plan.Domain == actorrouting.DomainFarmWorker {
				expectedMode = authority.TransferModeWorkerHandoff
			}
			if recovery.Mode != expectedMode ||
				recovery.TakeoverProof != (authority.TransferTakeoverProof{}) {
				return fmt.Errorf("%w: projection handoff mode changed", ErrPlanConflict)
			}
		}
		if !plannedDrain && (recovery.Mode != authority.TransferModeCrashTakeover ||
			recovery.TakeoverProof.ExpectedHolderBootID != plan.TakeoverProof.ExpectedHolderBootID ||
			recovery.TakeoverProof.HolderUnavailable != plan.TakeoverProof.HolderUnavailable ||
			recovery.TakeoverProof.LeaseExpired != plan.TakeoverProof.LeaseExpired ||
			recovery.TakeoverProof.Evidence != plan.TakeoverProof.Evidence) {
			return fmt.Errorf("%w: projection takeover proof changed", ErrPlanConflict)
		}
		requiredRevision := snapshotRevision
		if plan.OwnerScalePlanID != "" {
			switch {
			case recovery.PublishedSnapshotRevision > 0:
				requiredRevision = recovery.PublishedSnapshotRevision
			case recovery.Published && recovery.LastState == actorrouting.ShardStateAssigning:
				return fmt.Errorf("%w: assigning snapshot revision missing", ErrPlanConflict)
			default:
				requiredRevision = 1
			}
		}
		return e.validateTargetForSnapshotOnce(
			ctx,
			plan,
			requiredRevision,
			// Once the exact Authority transition is durable but its ACTIVE route
			// projection is still pending, the target intentionally reports
			// WARMING with zero serving shards. Publishing ASSIGNING does not make
			// the target placement-ready; it remains WARMING until the exact
			// generation is prepared and ACTIVE is published. Admit that narrow
			// recovery state only, never WARMING as a fresh placement target.
			recovery.Committed && (!recovery.Published || recovery.LastState == actorrouting.ShardStateAssigning),
		)
	}
}

// workerHandoffSourceValidator proves that a FARM_WORKER Watch consumer has
// applied the Manager's DRAINING revision and removed the shard from its local
// claim set. It polls only HolderRegistry facts and never mutates Authority.
func (e *PlanExecutor) workerHandoffSourceValidator(planID string) authority.WorkerHandoffSourceValidator {
	return func(ctx context.Context, observation authority.WorkerHandoffSourceObservation) error {
		if ctx == nil {
			return ErrExecutorInvalid
		}
		const retryBackoff = 50 * time.Millisecond
		timer := time.NewTimer(retryBackoff)
		defer timer.Stop()
		for {
			record, found, err := e.store.Load(ctx, planID)
			if err != nil {
				return err
			}
			if !found || record.Plan.Phase.Terminal() ||
				record.Plan.Operation != OperationPlannedDrain ||
				record.Plan.Domain != actorrouting.DomainFarmWorker ||
				record.Plan.Domain != observation.Domain ||
				record.Plan.VirtualShard != observation.VirtualShard ||
				record.Plan.SourceHolderID != observation.SourceHolderID ||
				record.Plan.SourceBootID != observation.SourceHolderBootID {
				return fmt.Errorf("%w: worker handoff plan changed", ErrPlanConflict)
			}
			source, sourceFound, sourceErr := e.holderReader.Get(
				ctx,
				observation.Domain,
				observation.SourceHolderID,
			)
			if sourceErr != nil &&
				!errors.Is(sourceErr, core.ErrHolderRegistryUnavailable) {
				return sourceErr
			}
			if sourceErr == nil &&
				sourceFound &&
				source.Domain == observation.Domain &&
				source.HolderID == observation.SourceHolderID &&
				source.BootID == observation.SourceHolderBootID &&
				source.ProcessReady &&
				!source.Draining &&
				(source.State == core.HolderStateReadyZeroShard ||
					source.State == core.HolderStateServing) &&
				source.LeaseUntil.After(e.now()) &&
				// A complete deadline-refresh publication may advance the
				// global revision before the next Holder heartbeat. The worker
				// has still proven the DRAINING handoff once it reports that
				// revision or a later one with the expected shard removed.
				source.SnapshotRevision >= observation.SnapshotRevision &&
				source.ServingShardCount == observation.ExpectedServingShards {
				return e.advancePhase(ctx, planID, PhaseAssigning)
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			timer.Reset(retryBackoff)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}

// validateTargetDuringDrain runs at each holder callback boundary. The full
// operation TTL was checked immediately before entering Manager; this helper
// only refreshes mutable target facts during that already-bounded operation.
func (e *PlanExecutor) validateTargetDuringDrain(ctx context.Context, planID string, domain actorrouting.RouteDomain, shard uint32) error {
	record, found, err := e.store.Load(ctx, planID)
	if err != nil {
		return err
	}
	if !found || record.Plan.Operation != OperationPlannedDrain || record.Plan.Domain != domain || record.Plan.VirtualShard != shard || record.Plan.Phase.Terminal() {
		return fmt.Errorf("%w: drain facts plan changed", ErrPlanConflict)
	}
	snapshot, err := e.loadSnapshot(ctx)
	if err != nil {
		return err
	}
	source, sourceFound, err := e.holderForPlan(ctx, record.Plan, record.Plan.SourceHolderID)
	if err != nil {
		return err
	}
	if !sourceFound || source.Domain != record.Plan.Domain || source.HolderID != record.Plan.SourceHolderID ||
		source.BootID != record.Plan.SourceBootID ||
		(!record.Plan.SourceDeploymentIdentity.IsZero() && source.DeploymentIdentity != record.Plan.SourceDeploymentIdentity) ||
		source.LogicEndpoint != record.Plan.SourceEndpoint || !canonicalHolderEndpoints(source) || !source.LeaseUntil.After(e.now()) {
		return fmt.Errorf("%w: source deployment identity changed during drain", ErrPlanConflict)
	}
	return e.validateTargetForSnapshot(ctx, record.Plan, initialTargetSnapshotRevision(record.Plan, snapshot.GlobalRevision), false)
}

func (e *PlanExecutor) requireSideEffectBudget(plan Plan) error {
	nowMs := e.now().UnixMilli()
	if plan.ExpiresAtMs <= nowMs {
		return fmt.Errorf("%w: plan expired", ErrPlanFailed)
	}
	required := e.timeout + executorTTLSafety
	requiredMs := required / time.Millisecond
	if required%time.Millisecond != 0 {
		requiredMs++
	}
	if plan.ExpiresAtMs-nowMs <= int64(requiredMs) {
		return errPlanTTLInsufficient
	}
	return nil
}

func safeAddMilliseconds(left, right int64) (int64, bool) {
	if right < 0 || left > int64(^uint64(0)>>1)-right {
		return 0, false
	}
	return left + right, true
}

func (e *PlanExecutor) loadSnapshot(ctx context.Context) (actorrouting.RouteSnapshot, error) {
	if e.snapshotStore != nil {
		stored, found, err := e.snapshotStore.LoadCurrent(ctx)
		if err != nil {
			return actorrouting.RouteSnapshot{}, err
		}
		if !found {
			return actorrouting.RouteSnapshot{}, fmt.Errorf("%w: snapshot missing", ErrVerification)
		}
		return stored.Snapshot, nil
	}
	return e.manager.CurrentSnapshot(), nil
}

func (e *PlanExecutor) validateActiveSnapshot(ctx context.Context, plan Plan, value authoritystore.ShardWriteAuthority) error {
	snapshot, err := e.loadSnapshot(ctx)
	if err != nil {
		return err
	}
	if !routeMatchesAuthority(snapshot, plan, value) {
		return fmt.Errorf("%w: active snapshot tuple", ErrVerification)
	}
	return nil
}

func routeMatchesAuthority(snapshot actorrouting.RouteSnapshot, plan Plan, value authoritystore.ShardWriteAuthority) bool {
	route, holder, err := snapshotRoute(snapshot, plan.Domain, plan.VirtualShard)
	if err != nil {
		return false
	}
	return snapshot.Realm != "" && snapshot.GlobalRevision != 0 && value.AuthorityLayoutVersion != "" &&
		snapshot.HashVersion == value.HashVersion && snapshot.HashSeed == value.HashSeed &&
		value.Domain == plan.Domain && value.VirtualShard == plan.VirtualShard && value.HolderID == plan.TargetHolderID &&
		value.HolderBootID == plan.TargetBootID && value.LastTransitionID == plan.TransitionID &&
		value.State == authoritystore.ShardAuthorityActive && route.State == actorrouting.ShardStateActive &&
		holder.InstanceID == value.HolderID && holder.BootID == value.HolderBootID &&
		holder.Endpoint == plan.TargetEndpoint && route.ShardRevision == value.AssignmentRevision &&
		route.ExpectedWriterEpoch == value.WriterEpoch && route.ExpectedLeaseEpoch == value.LeaseEpoch &&
		route.LeaseUntilMs > 0 && route.LeaseUntilMs <= value.LeaseUntilMs
}

// authorityDeadlineRenewalCompatible accepts only a monotonic holder-authorized
// deadline extension of one exact Authority generation. Every identity, fence,
// state and transition field remains byte-for-byte equal.
func authorityDeadlineRenewalCompatible(previous, current authoritystore.ShardWriteAuthority) bool {
	if previous.LeaseUntilMs <= 0 || current.LeaseUntilMs < previous.LeaseUntilMs {
		return false
	}
	current.LeaseUntilMs = previous.LeaseUntilMs
	return current == previous
}

func snapshotRoute(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain, shard uint32) (actorrouting.ShardRoute, actorrouting.HolderRef, error) {
	var table actorrouting.DomainSnapshot
	switch domain {
	case actorrouting.DomainPlayer:
		table = snapshot.Player
	case actorrouting.DomainFarm:
		table = snapshot.Farm
	case actorrouting.DomainFarmWorker:
		table = snapshot.FarmWorker
	default:
		return actorrouting.ShardRoute{}, actorrouting.HolderRef{}, fmt.Errorf("%w: domain", ErrInvalidPlan)
	}
	if shard >= table.ShardCount || int(shard) >= len(table.Routes) {
		return actorrouting.ShardRoute{}, actorrouting.HolderRef{}, fmt.Errorf("%w: shard", ErrInvalidPlan)
	}
	route := table.Routes[shard]
	if route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
		return actorrouting.ShardRoute{}, actorrouting.HolderRef{}, fmt.Errorf("%w: holder index", ErrVerification)
	}
	return route, snapshot.Holders[route.HolderIndex], nil
}

func (e *PlanExecutor) handleExecutionError(record PlanRecord, err error) (PlanRecord, error) {
	// A Manager error occurs after holder callbacks or an authority attempt may
	// already have happened. Keep the durable record explicitly PAUSED until a
	// later operator/recovery pass reconciles the exact Authority/Snapshot
	// tuple; never turn an unknown external outcome into a retryable failure.
	reconcileCtx, cancel := e.persistenceContext()
	if errors.Is(err, authority.ErrAuthorityWaveStagePending) && e.authorityWaveActive(record.Plan) {
		buffered, found, loadErr := e.store.Load(reconcileCtx, record.Plan.PlanID)
		cancel()
		if loadErr != nil {
			return PlanRecord{}, loadErr
		}
		if !found {
			return PlanRecord{}, ErrPlanNotFound
		}
		return buffered, nil
	}
	state, _, _, reconcileErr := e.reconcile(reconcileCtx, record.Plan)
	if reconcileErr == nil && state == reconcileAuthorityCommittedSnapshotPending {
		if e.authorityWaveActive(record.Plan) {
			// Authority-committed is necessary but not sufficient evidence that
			// this attempt successfully re-buffered its projection. Target
			// admission, lease validation, or publication can fail before the
			// wave publisher receives the step. Swallowing that error leaves the
			// parent wave permanently reporting buffered=0 with no root cause.
			if !e.authorityWaveBatchContains(record.Plan.OwnerWaveID, record.Plan.Domain, record.Plan.VirtualShard) {
				cancel()
				return record, err
			}
			buffered, found, loadErr := e.store.Load(reconcileCtx, record.Plan.PlanID)
			cancel()
			if loadErr != nil {
				return PlanRecord{}, loadErr
			}
			if !found {
				return PlanRecord{}, ErrPlanNotFound
			}
			return buffered, nil
		}
		cancel()
		return e.pausePlan(record, ErrorAuthorityCommittedSnapshotPending, err)
	}
	cancel()
	return e.pausePlan(record, stableErrorCode(err), err)
}

func stableErrorCode(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "CANCELED"
	case errors.Is(err, context.DeadlineExceeded):
		return "TIMEOUT"
	case errors.Is(err, ErrTakeoverProof):
		return "TAKEOVER_PROOF"
	case errors.Is(err, ErrPlanConflict):
		return "FENCE_CONFLICT"
	case errors.Is(err, errPlanTTLInsufficient):
		return "PLAN_TTL_INSUFFICIENT"
	case errors.Is(err, ErrVerification):
		return "VERIFICATION"
	case errors.Is(err, authority.ErrAuthorityCommitUnknown):
		return "AUTHORITY_COMMIT_UNKNOWN"
	case errors.Is(err, authority.ErrAuthorityUnavailable):
		return "AUTHORITY_UNAVAILABLE"
	case errors.Is(err, authority.ErrProjectionRecovery):
		return "PROJECTION_RECOVERY"
	case errors.Is(err, authority.ErrDrainControllerRequired):
		return "DRAIN_CONTROLLER_REQUIRED"
	default:
		return "EXECUTION_FAILED"
	}
}

func (e *PlanExecutor) bumpAttempt(ctx context.Context, planID string) (Plan, error) {
	var result Plan
	// StartedAtMs and UpdatedAtMs are one audit linearization point. Sampling
	// the clock again inside mutate can cross a millisecond boundary and create
	// StartedAtMs > UpdatedAtMs, intermittently rejecting an untouched PENDING
	// Plan before any holder or Authority side effect.
	auditAtMs := e.now().UnixMilli()
	err := e.updatePlanAt(ctx, planID, auditAtMs, func(plan *Plan) error {
		plan.Attempt++
		if plan.StartedAtMs == 0 {
			plan.StartedAtMs = auditAtMs
		}
		return nil
	}, &result)
	return result, err
}

func (e *PlanExecutor) advancePhase(ctx context.Context, planID string, phase Phase) error {
	return e.updatePlanAt(ctx, planID, e.now().UnixMilli(), func(plan *Plan) error {
		if plan.Phase == phase || phaseRank(plan.Phase) > phaseRank(phase) {
			return nil
		}
		if err := ValidatePhaseAdvance(plan.Phase, phase); err != nil {
			return err
		}
		plan.Phase = phase
		return nil
	}, nil)
}

func (e *PlanExecutor) advanceReconciledPhase(ctx context.Context, planID string, phase Phase, auditAtMs int64) error {
	return e.updatePlanAt(ctx, planID, auditAtMs, func(plan *Plan) error {
		if plan.Phase == phase || phaseRank(plan.Phase) > phaseRank(phase) {
			return nil
		}
		if err := ValidatePhaseAdvance(plan.Phase, phase); err != nil {
			return err
		}
		plan.Phase = phase
		return nil
	}, nil)
}

func phaseRank(phase Phase) int {
	switch phase {
	case PhasePending:
		return 1
	case PhaseDraining:
		return 2
	case PhaseAssigning:
		return 3
	case PhasePreparing:
		return 4
	case PhaseActive:
		return 5
	case PhaseSucceeded:
		return 6
	default:
		return 0
	}
}

func (e *PlanExecutor) finishPlan(ctx context.Context, planID string) (PlanRecord, error) {
	for _, phase := range []Phase{PhaseAssigning, PhasePreparing, PhaseActive} {
		if err := e.advancePhase(ctx, planID, phase); err != nil {
			return PlanRecord{}, err
		}
	}
	if err := e.updatePlan(ctx, planID, func(plan *Plan) error {
		if plan.Phase == PhaseActive {
			if err := ValidatePhaseAdvance(plan.Phase, PhaseSucceeded); err != nil {
				return err
			}
			plan.Phase = PhaseSucceeded
		}
		return nil
	}, nil); err != nil {
		return PlanRecord{}, err
	}
	record, found, err := e.store.Load(ctx, planID)
	if err != nil {
		return PlanRecord{}, err
	}
	if !found {
		return PlanRecord{}, ErrPlanNotFound
	}
	return record, nil
}

// finishReconciledPlan records a success proven by current Authority/Snapshot
// facts. During physical audit retention the logical deadline may already be
// past, so intermediate monotonic phases use the last valid logical timestamp;
// only the final terminal audit update is stamped with the current time.
func (e *PlanExecutor) finishReconciledPlan(ctx context.Context, planID string) (PlanRecord, error) {
	record, found, err := e.store.Load(ctx, planID)
	if err != nil {
		return PlanRecord{}, err
	}
	if !found {
		return PlanRecord{}, ErrPlanNotFound
	}
	if record.Plan.ExpiresAtMs > e.now().UnixMilli() {
		return e.finishPlan(ctx, planID)
	}
	auditAtMs := record.Plan.ExpiresAtMs - 1
	for _, phase := range []Phase{PhaseAssigning, PhasePreparing, PhaseActive} {
		if err := e.advanceReconciledPhase(ctx, planID, phase, auditAtMs); err != nil {
			return PlanRecord{}, err
		}
	}
	return e.finishPlan(ctx, planID)
}

func (e *PlanExecutor) failPlan(record PlanRecord, code string, cause error) (PlanRecord, error) {
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return e.pausePlan(record, stableErrorCode(cause), cause)
	}
	if record.Plan.Phase.Terminal() {
		return record, cause
	}
	var updated Plan
	persistCtx, cancel := e.persistenceContext()
	defer cancel()
	err := e.updatePlan(persistCtx, record.Plan.PlanID, func(plan *Plan) error {
		plan.Phase = PhaseFailed
		plan.ErrorCode = stableCode(code)
		plan.CompletedAtMs = 0
		return nil
	}, &updated)
	if err != nil {
		return record, errors.Join(cause, err)
	}
	return PlanRecord{Plan: updated}, cause
}

func (e *PlanExecutor) pausePlan(record PlanRecord, code string, cause error) (PlanRecord, error) {
	if record.Plan.Phase.Terminal() {
		return record, cause
	}
	var updated Plan
	persistCtx, cancel := e.persistenceContext()
	defer cancel()
	err := e.updatePlan(persistCtx, record.Plan.PlanID, func(plan *Plan) error {
		plan.Phase = PhasePaused
		plan.ErrorCode = stableCode(code)
		plan.CompletedAtMs = 0
		return nil
	}, &updated)
	if err != nil {
		return record, errors.Join(cause, err)
	}
	return PlanRecord{Plan: updated}, cause
}

// unpauseForRetry returns a transiently paused wave member to PENDING so the
// wave recovery path can re-execute it. Only retryable pause codes may reach
// this transition; authority-committed members stay paused and are resolved by
// the wave commit instead.
func (e *PlanExecutor) unpauseForRetry(ctx context.Context, planID string) error {
	var updated Plan
	return e.updatePlan(ctx, planID, func(plan *Plan) error {
		if plan.Phase != PhasePaused {
			return nil
		}
		if !retryablePauseCode(plan.ErrorCode) && plan.ErrorCode != ErrorAuthorityCommittedSnapshotPending &&
			!retryableCrashTakeoverPause(*plan) {
			return fmt.Errorf("%w: non-retryable pause code %s", ErrPlanPaused, plan.ErrorCode)
		}
		plan.Phase = PhasePending
		plan.ErrorCode = ""
		plan.CompletedAtMs = 0
		return nil
	}, &updated)
}

func stableCode(code string) string {
	if code == "" {
		return "EXECUTION_FAILED"
	}
	if len(code) > MaxErrorCodeBytes {
		return "EXECUTION_FAILED"
	}
	return code
}

func (e *PlanExecutor) updatePlan(ctx context.Context, planID string, mutate func(*Plan) error, output *Plan) error {
	return e.updatePlanAt(ctx, planID, e.now().UnixMilli(), mutate, output)
}

func (e *PlanExecutor) updatePlanAt(ctx context.Context, planID string, auditAtMs int64, mutate func(*Plan) error, output *Plan) error {
	if mutate == nil {
		return ErrExecutorInvalid
	}
	for attempt := 0; attempt < 4; attempt++ {
		record, found, err := e.store.Load(ctx, planID)
		if err != nil {
			return err
		}
		if !found {
			return ErrPlanNotFound
		}
		plan := record.Plan.Clone()
		if err := mutate(&plan); err != nil {
			return err
		}
		if plan == record.Plan {
			if output != nil {
				*output = record.Plan
			}
			return nil
		}
		plan.Revision = record.Plan.Revision + 1
		plan.UpdatedAtMs = auditAtMs
		if plan.Phase.Terminal() && plan.CompletedAtMs == 0 {
			plan.CompletedAtMs = plan.UpdatedAtMs
		}
		if err := plan.Validate(); err != nil {
			return err
		}
		result, err := e.store.CompareAndSwap(ctx, record.Plan.Revision, plan)
		if err == nil && (result.Status == CASCommitted || result.Status == CASAlreadyCommitted) {
			if output != nil {
				*output = result.Record.Plan
			}
			return nil
		}
		if !errors.Is(err, ErrPlanConflict) && result.Status != CASRevisionConflict {
			return err
		}
	}
	return ErrPlanConflict
}

func (e *PlanExecutor) requireContextOrPause(ctx context.Context, _ string) error {
	if ctx == nil {
		return context.Canceled
	}
	return ctx.Err()
}

func (e *PlanExecutor) operationContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, ErrExecutorInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	bounded, cancel := context.WithTimeout(ctx, e.timeout)
	return bounded, cancel, nil
}

func (e *PlanExecutor) persistenceContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), e.timeout)
}

func (e *PlanExecutor) acquireShard(key shardKey, owner string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if current, ok := e.shards[key]; ok {
		return fmt.Errorf("%w: current_owner=%s requested_owner=%s", ErrShardBusy, current, owner)
	}
	e.shards[key] = owner
	return nil
}

func (e *PlanExecutor) releaseShard(key shardKey) {
	e.mu.Lock()
	delete(e.shards, key)
	e.mu.Unlock()
}

const (
	maxConcurrentRecoveryPlans = 32
	legacyRecoveryStartGather  = time.Second
	legacyRecoveryGatherPoll   = 50 * time.Millisecond
)

type recoveryResult struct {
	record PlanRecord
	err    error
}

// Recover groups a complete schema-v2 ScalePlan wave before any child effect.
// Authority transitions execute concurrently into the manager-owned buffer;
// one sealed jump CAS publishes the complete wave. Legacy records retain the
// original independent execution path.
func (e *PlanExecutor) Recover(ctx context.Context) error {
	if e == nil || ctx == nil {
		return ErrExecutorInvalid
	}
	releaseRecovery, err := e.acquireRecoveryGate(ctx)
	if err != nil {
		return err
	}
	defer releaseRecovery()
	listCtx, resolveList, err := e.control.Begin(ctx, fmt.Sprintf("routeplan/recovery-list/%d", e.now().UnixNano()), scaleplan.ControlOperationRecovery)
	if err != nil {
		return err
	}
	records, err := e.store.ListIncomplete(listCtx)
	if err != nil {
		return err
	}
	if resolveList != nil {
		if err := resolveList(); err != nil {
			return err
		}
	}
	if len(records) == 0 {
		return nil
	}
	if wave := completeAuthorityWave(records); len(wave) != 0 {
		return e.recoverAuthorityWave(ctx, wave)
	}
	legacy := make([]PlanRecord, 0, len(records))
	for _, record := range records {
		if record.Plan.OwnerWaveID == "" {
			legacy = append(legacy, record)
		}
	}
	if len(legacy) == 0 {
		// Child records for a new wave are created one CAS at a time. Do not let
		// an early recovery wake execute a partial wave.
		return nil
	}
	legacy, err = e.gatherLegacySlidingPlans(ctx, legacy)
	if err != nil {
		return err
	}
	if len(legacy) == 0 {
		return nil
	}
	limit := e.slidingRecoveryLimit()
	if len(legacy) > limit {
		legacy = legacy[:limit]
	}
	_, recoveryErr := e.executeRecoveryRecords(ctx, legacy, false)
	return recoveryErr
}

func (e *PlanExecutor) slidingRecoveryLimit() int {
	if e != nil && e.slidingRecoveryWindow > 0 {
		return e.slidingRecoveryWindow
	}
	return maxConcurrentRecoveryPlans
}

func (e *PlanExecutor) slidingRecoveryGatherTimeout() time.Duration {
	if e != nil && e.legacyRecoveryGatherTimeout > 0 {
		return e.legacyRecoveryGatherTimeout
	}
	return legacyRecoveryStartGather
}

func (e *PlanExecutor) slidingRecoveryGatherPollInterval() time.Duration {
	if e != nil && e.legacyRecoveryGatherPollInterval > 0 {
		return e.legacyRecoveryGatherPollInterval
	}
	return legacyRecoveryGatherPoll
}

// gatherLegacySlidingPlans waits, without holder effects, until the sliding
// inflight window is populated or the gather deadline elapses. Starting Drain
// on the first few children marks a scale-in victim draining and fail-closes
// later EnsureMove calls. After the window fills, one extra quiet poll lets
// ScalePlan finish the EnsureMove that created the last child.
func (e *PlanExecutor) gatherLegacySlidingPlans(ctx context.Context, initial []PlanRecord) ([]PlanRecord, error) {
	legacy := initial
	limit := e.slidingRecoveryLimit()
	if len(legacy) >= limit {
		return e.relistLegacySlidingPlans(ctx, e.slidingRecoveryGatherPollInterval())
	}
	deadline := time.Now().Add(e.slidingRecoveryGatherTimeout())
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(legacy) >= limit {
			return e.relistLegacySlidingPlans(ctx, e.slidingRecoveryGatherPollInterval())
		}
		wait := time.Until(deadline)
		if wait <= 0 {
			break
		}
		poll := e.slidingRecoveryGatherPollInterval()
		if wait > poll {
			wait = poll
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
		var err error
		legacy, err = e.listLegacySlidingPlans(ctx)
		if err != nil {
			return nil, err
		}
		if len(legacy) == 0 {
			return nil, nil
		}
	}
	return e.capLegacySlidingPlans(legacy), nil
}

func (e *PlanExecutor) relistLegacySlidingPlans(ctx context.Context, quiet time.Duration) ([]PlanRecord, error) {
	if quiet > 0 {
		timer := time.NewTimer(quiet)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	legacy, err := e.listLegacySlidingPlans(ctx)
	if err != nil {
		return nil, err
	}
	return e.capLegacySlidingPlans(legacy), nil
}

func (e *PlanExecutor) listLegacySlidingPlans(ctx context.Context) ([]PlanRecord, error) {
	incomplete, err := e.store.ListIncomplete(ctx)
	if err != nil {
		return nil, err
	}
	legacy := make([]PlanRecord, 0, len(incomplete))
	for _, record := range incomplete {
		if record.Plan.OwnerWaveID == "" {
			legacy = append(legacy, record)
		}
	}
	return legacy, nil
}

func (e *PlanExecutor) capLegacySlidingPlans(legacy []PlanRecord) []PlanRecord {
	limit := e.slidingRecoveryLimit()
	if len(legacy) > limit {
		return legacy[:limit]
	}
	return legacy
}

func completeAuthorityWave(records []PlanRecord) []PlanRecord {
	groups := make(map[string][]PlanRecord)
	for _, record := range records {
		plan := record.Plan
		if plan.OwnerWaveID == "" || plan.OwnerWaveSize == 0 {
			continue
		}
		switch plan.Phase {
		case PhasePending, PhaseDraining, PhaseAssigning, PhasePreparing:
			// Members interrupted mid-wave stay in a pre-terminal phase (the
			// tracking drain controller advances through Draining/Assigning/
			// Preparing before the wave buffer returns) and are re-executed
			// idempotently (committed transitions replay as ALREADY_APPLIED
			// and re-buffer their projection).
		case PhasePaused:
			// A transient pause (CANCELED/TIMEOUT) or an authority-committed
			// member whose wave commit never landed must not permanently break
			// the wave: the remaining members would never re-aggregate and the
			// parent ScalePlan would stay BLOCKED forever. Both are retried by
			// Execute after a PAUSED -> PENDING transition; the committed member
			// replays as ALREADY_APPLIED and re-buffers its projection.
			if !retryablePauseCode(plan.ErrorCode) && plan.ErrorCode != ErrorAuthorityCommittedSnapshotPending {
				continue
			}
		default:
			continue
		}
		groups[plan.OwnerWaveID] = append(groups[plan.OwnerWaveID], record)
	}
	ids := make([]string, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		group := groups[id]
		expected := int(group[0].Plan.OwnerWaveSize)
		if expected < 1 || expected > maxConcurrentRecoveryPlans || len(group) != expected {
			continue
		}
		sort.Slice(group, func(i, j int) bool { return group[i].Plan.OwnerWavePosition < group[j].Plan.OwnerWavePosition })
		valid := true
		for index := range group {
			plan := group[index].Plan
			if plan.OwnerWaveID != id || int(plan.OwnerWaveSize) != expected || int(plan.OwnerWavePosition) != index || plan.Operation != OperationPlannedDrain {
				valid = false
				break
			}
		}
		if valid {
			return group
		}
	}
	return nil
}

func (e *PlanExecutor) recoverAuthorityWave(ctx context.Context, records []PlanRecord) error {
	manager, ok := e.authorityWaveManager()
	if !ok || len(records) == 0 {
		return core.ErrAuthorityWaveBatchUnsupported
	}
	if e.executionGuard != nil {
		guard, ok := e.executionGuard.(AuthorityWaveExecutionGuard)
		if !ok {
			return ErrExecutorInvalid
		}
		release, err := guard.EnterAuthorityWave(ctx)
		if err != nil {
			return err
		}
		if release == nil {
			return ErrExecutorInvalid
		}
		defer release()
	}
	waveID := records[0].Plan.OwnerWaveID
	members := make([]core.AuthorityWaveMember, len(records))
	for index, record := range records {
		members[index] = core.AuthorityWaveMember{Domain: record.Plan.Domain, VirtualShard: record.Plan.VirtualShard}
	}
	var waveErr error
	for stagePass := 0; stagePass < 2; stagePass++ {
		if err := manager.BeginAuthorityWaveBatch(ctx, waveID, members); err != nil {
			return errors.Join(waveErr, err)
		}
		stage, stageOpen := manager.AuthorityWaveBatchStage(waveID)
		baseRevision, baseOpen := manager.AuthorityWaveBatchBaseRevision(waveID)
		if !stageOpen || !baseOpen || stage == core.AuthorityWaveBatchStageInvalid {
			return errors.Join(waveErr, core.ErrAuthorityWaveBatchInvalid)
		}

		// A previous recovery pass may already have buffered some members of
		// this stage before a peer timed out or its Holder facts became
		// temporarily unavailable. Replaying those members would attempt to
		// append the same stage transition twice. Execute only the members that
		// are still missing from the open in-memory batch.
		pending := make([]PlanRecord, 0, len(records))
		for _, record := range records {
			if !manager.AuthorityWaveBatchContains(waveID, record.Plan.Domain, record.Plan.VirtualShard) {
				pending = append(pending, record)
			}
		}
		_, executeErr := e.executeRecoveryRecords(ctx, pending, true)
		waveErr = errors.Join(waveErr, executeErr)
		buffered := 0
		for _, record := range records {
			if manager.AuthorityWaveBatchContains(waveID, record.Plan.Domain, record.Plan.VirtualShard) {
				buffered++
			}
		}
		if buffered != len(records) {
			// An incomplete stage is a retryable dependency state, not a corrupt
			// Wave. Keep the batch open so the next recovery pass can fill only
			// the missing members. Never seal or publish a partial stage.
			return errors.Join(waveErr, fmt.Errorf("%w: wave=%s stage=%d buffered=%d expected=%d", scaleplan.ErrPending, waveID, stage, buffered, len(records)))
		}

		commitID := fmt.Sprintf("routeplan/wave-commit/%s/base-%d/stage-%d", waveID, baseRevision, stage)
		commitCtx, resolveCommit, err := e.control.Begin(ctx, commitID, scaleplan.ControlOperationRecovery)
		if err != nil {
			return errors.Join(waveErr, err)
		}
		_, commitResult, commitErr := manager.CommitAuthorityWaveBatch(commitCtx, waveID)
		if resolveCommit != nil && (commitErr == nil || commitResult.Committed()) {
			commitErr = errors.Join(commitErr, resolveCommit())
		}
		waveErr = errors.Join(waveErr, commitErr)
		if commitErr != nil && !commitResult.Committed() {
			return waveErr
		}
		if stage == core.AuthorityWaveBatchStageActive {
			final, finalErr := e.executeRecoveryRecords(ctx, records, true)
			_ = final
			return errors.Join(waveErr, finalErr)
		}
	}
	return errors.Join(waveErr, fmt.Errorf("%w: wave=%s did not reach ACTIVE stage", core.ErrAuthorityWaveBatchInvalid, waveID))
}

func (e *PlanExecutor) executeRecoveryRecords(ctx context.Context, records []PlanRecord, authorityWaveGuardHeld bool) ([]recoveryResult, error) {
	results := make(chan recoveryResult, len(records))
	limit := make(chan struct{}, maxConcurrentRecoveryPlans)
	var workers sync.WaitGroup
	for _, record := range records {
		select {
		case limit <- struct{}{}:
		case <-ctx.Done():
			workers.Wait()
			return nil, ctx.Err()
		}
		planID := record.Plan.PlanID
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { <-limit }()
			result, executeErr := e.execute(ctx, planID, authorityWaveGuardHeld)
			results <- recoveryResult{record: result, err: executeErr}
		}()
	}
	workers.Wait()
	close(results)
	collected := make([]recoveryResult, 0, len(records))
	var recoveryErr error
	for result := range results {
		collected = append(collected, result)
		if result.err != nil &&
			!errors.Is(result.err, ErrControlOperationResolved) &&
			!(errors.Is(result.err, ErrPlanFailed) && result.record.Plan.Phase == PhaseFailed) &&
			result.record.Plan.Phase != PhasePaused {
			recoveryErr = errors.Join(recoveryErr, result.err)
		}
	}
	return collected, recoveryErr
}

func (e *PlanExecutor) Startup(ctx context.Context) error { return e.Recover(ctx) }

type trackingDrainController struct {
	inner    authority.DrainController
	executor *PlanExecutor
	planID   string
}

func (c *trackingDrainController) StopNewActivationAndTurns(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) error {
	if err := c.executor.advancePhase(ctx, c.planID, PhaseDraining); err != nil {
		return err
	}
	if err := c.executor.validateTargetDuringDrain(ctx, c.planID, domain, shard); err != nil {
		return err
	}
	return c.inner.StopNewActivationAndTurns(ctx, domain, shard)
}

func (c *trackingDrainController) WaitCurrentTurns(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) error {
	if err := c.executor.advancePhase(ctx, c.planID, PhaseDraining); err != nil {
		return err
	}
	if err := c.executor.validateTargetDuringDrain(ctx, c.planID, domain, shard); err != nil {
		return err
	}
	return c.inner.WaitCurrentTurns(ctx, domain, shard)
}

func (c *trackingDrainController) ConfirmFactsCommitted(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) error {
	if err := c.executor.advancePhase(ctx, c.planID, PhaseAssigning); err != nil {
		return err
	}
	if err := c.executor.validateTargetDuringDrain(ctx, c.planID, domain, shard); err != nil {
		return err
	}
	if err := c.inner.ConfirmFactsCommitted(ctx, domain, shard); err != nil {
		return err
	}
	return c.executor.validateTargetDuringDrain(ctx, c.planID, domain, shard)
}

func (c *trackingDrainController) ColdLoadNewHolder(ctx context.Context, domain actorrouting.RouteDomain, shard uint32, value authoritystore.ShardWriteAuthority) error {
	if err := c.executor.advancePhase(ctx, c.planID, PhasePreparing); err != nil {
		return err
	}
	if err := c.executor.validateTargetDuringDrain(ctx, c.planID, domain, shard); err != nil {
		return err
	}
	return c.inner.ColdLoadNewHolder(ctx, domain, shard, value)
}

// VerificationCheck is intentionally code-only. It is safe to return through
// routectl without exposing holder proof evidence or raw storage errors.
type VerificationCheck struct {
	Name string
	OK   bool
	Code string
}

type VerificationReport struct {
	Consistent bool
	Checks     []VerificationCheck
	Plan       PlanRecord
}

func (e *PlanExecutor) Verify(ctx context.Context, planID string) (VerificationReport, error) {
	if e == nil {
		return VerificationReport{}, ErrExecutorInvalid
	}
	record, found, err := e.store.Load(ctx, planID)
	if err != nil {
		return VerificationReport{}, err
	}
	if !found {
		return VerificationReport{}, ErrPlanNotFound
	}
	report := VerificationReport{Plan: record}
	add := func(name string, ok bool, code string) {
		report.Checks = append(report.Checks, VerificationCheck{Name: name, OK: ok, Code: code})
		if !ok {
			report.Consistent = false
		}
	}
	report.Consistent = true
	value, authorityFound, authorityErr := e.authorityReader.ReadShardWriteAuthority(ctx, record.Plan.Domain, record.Plan.VirtualShard)
	authorityReady := authorityErr == nil && authorityFound && value.State == authoritystore.ShardAuthorityActive && value.LeaseUntilMs > e.now().UnixMilli()
	add("authority_active_lease", authorityReady, stableVerificationCode(authorityErr, authorityFound))
	authorityTuple := false
	if authorityErr == nil && authorityFound {
		authorityTuple = value.Domain == record.Plan.Domain && value.VirtualShard == record.Plan.VirtualShard &&
			value.LastTransitionID == record.Plan.TransitionID && value.HolderID == record.Plan.TargetHolderID && value.HolderBootID == record.Plan.TargetBootID &&
			value.State == authoritystore.ShardAuthorityActive
	}
	add("authority_tuple", authorityTuple, "AUTHORITY_TUPLE")
	snapshot, snapshotErr := e.loadSnapshot(ctx)
	add("snapshot_read", snapshotErr == nil, stableVerificationCode(snapshotErr, snapshotErr == nil))
	snapshotTuple := snapshotErr == nil && authorityTuple && routeMatchesAuthority(snapshot, record.Plan, value)
	add("snapshot_tuple", snapshotTuple, "SNAPSHOT_TUPLE")
	targetOK := false
	var targetEvidence core.HolderRecord
	if e.holderReader != nil {
		target, targetFound, targetErr := e.holderForPlan(ctx, record.Plan, record.Plan.TargetHolderID)
		targetOK = targetErr == nil && targetFound && canonicalHolderEndpoints(target) && target.Domain == record.Plan.Domain && target.HolderID == record.Plan.TargetHolderID &&
			target.BootID == record.Plan.TargetBootID && target.LogicEndpoint == record.Plan.TargetEndpoint && target.HolderControlEndpoint != "" && target.TLSDNSIdentity != "" &&
			target.CapacityWeight > 0 && target.ProcessReady &&
			!target.Draining && (target.State == core.HolderStateReadyZeroShard || target.State == core.HolderStateServing) &&
			target.LeaseUntil.After(e.now()) && snapshotErr == nil && target.SnapshotRevision == snapshot.GlobalRevision
		if targetOK {
			targetEvidence = target
		}
		add("target_holder", targetOK, stableVerificationCode(targetErr, targetFound))
	} else {
		add("target_holder", false, "HOLDER_READER_MISSING")
	}
	if e.verification == nil {
		for _, name := range []string{"gate_resolution", "transition", "recovery", "tombstone"} {
			add(name, false, "VERIFICATION_DEPENDENCY_MISSING")
		}
	} else {
		// Manager evidence is checked before Gate evidence so the final Gate
		// observation is the last external read before the local freshness
		// bracket below. Every method receives the same immutable Authority and
		// durable snapshot values read above.
		transitionErr := e.verification.VerifyTransition(ctx, record.Plan, value, snapshot)
		recoveryErr := e.verification.VerifyRecovery(ctx, record.Plan, value, snapshot)
		tombstoneErr := e.verification.VerifyTombstone(ctx, record.Plan, value, snapshot)
		gateErr := e.verification.VerifyGateResolution(ctx, record.Plan, value, snapshot)
		add("gate_resolution", gateErr == nil, "GATE_RESOLUTION")
		add("transition", transitionErr == nil, "TRANSITION")
		add("recovery", recoveryErr == nil, "RECOVERY")
		add("tombstone", tombstoneErr == nil, "TOMBSTONE")
	}
	planStateOK := record.Plan.Phase == PhaseSucceeded ||
		(record.Plan.Phase == PhasePaused && authorityReady && authorityTuple && snapshotTuple && targetOK)
	add("plan_state", planStateOK, "PLAN_STATE")
	add("final_freshness", e.finalVerificationFresh(ctx, record.Plan, value, snapshot, targetEvidence, targetOK), "FINAL_FRESHNESS")
	return report, nil
}

// finalVerificationFresh is deliberately the final consistency predicate. It
// re-reads every mutable local fact after Gate/Manager evidence and compares it
// with the one immutable generation used by every VerificationPort method.
// The injected clock is sampled only after those reads, immediately before the
// result is returned.
func (e *PlanExecutor) finalVerificationFresh(ctx context.Context, plan Plan, expectedAuthority authoritystore.ShardWriteAuthority, expectedSnapshot actorrouting.RouteSnapshot, expectedTarget core.HolderRecord, targetWasReady bool) bool {
	if e == nil || ctx == nil || !targetWasReady {
		return false
	}
	currentAuthority, found, err := e.authorityReader.ReadShardWriteAuthority(ctx, plan.Domain, plan.VirtualShard)
	if err != nil || !found || !authorityDeadlineRenewalCompatible(expectedAuthority, currentAuthority) {
		return false
	}
	currentSnapshot, err := e.loadSnapshot(ctx)
	if err != nil || !reflect.DeepEqual(currentSnapshot, expectedSnapshot) || !routeMatchesAuthority(currentSnapshot, plan, currentAuthority) {
		return false
	}
	currentTarget, found, err := e.holderForPlan(ctx, plan, plan.TargetHolderID)
	if err != nil || !found || !reflect.DeepEqual(currentTarget, expectedTarget) {
		return false
	}
	now := e.now()
	if currentAuthority.LeaseUntilMs <= now.UnixMilli() || !currentTarget.LeaseUntil.After(now) {
		return false
	}
	if e.leaseCheckMode == actorrouting.LeaseCheckAuthorityDelegated {
		return true
	}
	route, _, err := snapshotRoute(currentSnapshot, plan.Domain, plan.VirtualShard)
	return err == nil && route.LeaseUntilMs > now.UnixMilli()
}

func stableVerificationCode(err error, found bool) string {
	if err != nil {
		return stableErrorCode(err)
	}
	if !found {
		return "NOT_FOUND"
	}
	return "OK"
}
