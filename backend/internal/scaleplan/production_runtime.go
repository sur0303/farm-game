package scaleplan

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	defaultMutationLeaseTTL  = 20 * time.Second
	maximumMutationLeaseTTL  = 60 * time.Second
	defaultMutationHeartbeat = 5 * time.Second
	// minimumMutationHeartbeat only stops a caller from hammering the store.
	// The property that matters is the ratio below: a lease must survive at
	// least two failed renewals before it expires, so that one lost packet
	// does not cancel a mutation that was going fine.
	minimumMutationHeartbeat  = 10 * time.Millisecond
	heartbeatToTTLSafetyRatio = 3
)

// ErrLeaseLost reports that renewal stopped succeeding while a guard was held.
// The guard context is already cancelled by the time a caller sees this.
var ErrLeaseLost = errors.New("scaleplan: mutation lease lost")

// DurableCreateCommand and DurableCASCommand are plain structs: a backend must
// be able to read every field to write it, and none of them is a credential.
type DurableCreateCommand struct {
	Submission IdempotencySubmission
	Plan       Plan
}

type DurableCASCommand struct {
	Expected uint64
	Plan     Plan
}

type RecoverableCursor struct {
	Token string
	Limit int
}

type RecoverablePage struct {
	Records []PlanRecord
	Next    RecoverableCursor
	Done    bool
}

// DurableRepositoryBackend is the only surface an out-of-package store
// implements. It deliberately excludes anything that could mint a terminal:
// ApplySealedBlockedTermination receives a seal, it does not create one.
type DurableRepositoryBackend interface {
	// RepositoryBindingID returns a stable, non-secret identity for the
	// physical repository/keyspace. It must not change for the backend's
	// lifetime and must be present in every lease the backend issues.
	RepositoryBindingID() string
	AcquireMutationLease(context.Context, MutationScope, MutationLeaseOwner) (BackendMutationLease, error)
	RenewMutationLease(context.Context, BackendMutationLease) (BackendMutationLease, error)
	ReleaseMutationLease(context.Context, BackendMutationLease) error
	CreateOrGet(context.Context, BackendMutationLease, DurableCreateCommand) (CreateResult, error)
	Load(context.Context, string) (PlanRecord, bool, error)
	CompareAndSwap(context.Context, BackendMutationLease, DurableCASCommand) (CASResult, error)
	ListRecoverable(context.Context, RecoverableCursor) (RecoverablePage, error)
	// ApplySealedBlockedTermination must reject the seal unless both
	// seal.Authorized(), seal.BoundTo(lease), and the lease's RepositoryID
	// equals RepositoryBindingID(). All checks happen before any store round
	// trip. Authorized() alone only says the seal came from some production
	// runtime; the stable lease tuple binds it to this repository and writer.
	ApplySealedBlockedTermination(context.Context, BackendMutationLease, SealedBlockedTerminationMutation) (BlockedTerminationResult, error)
}

// SealedBlockedTerminationMutation is exported so backends can receive it and
// opaque so they cannot forge it. Every field is private, which means the Go
// zero value and any composite literal an external package can write are both
// unauthorized by construction rather than by a runtime check that could be
// forgotten.
type SealedBlockedTerminationMutation struct {
	authority  *repositoryIdentity
	owner      *blockedTerminationSealer
	request    BlockedTerminationRequest
	proof      BlockedQuiescenceProof
	lease      BackendMutationLease
	checkpoint string
	evidence   string
}

// blockedTerminationSealer is a private capability handle. Holding a pointer to
// one is what distinguishes the runtime's own owner from any other.
type blockedTerminationSealer struct{ marker byte }

// Authorized reports whether this value was minted by a production runtime for
// the repository that is about to apply it. A backend must call it before any
// store round trip.
func (m SealedBlockedTerminationMutation) Authorized() bool {
	return m.authority != nil && m.owner != nil && m.checkpoint != "" && m.evidence != "" &&
		m.lease.RepositoryID != "" && m.lease.Fence != "" && m.request.PlanID != ""
}

func (m SealedBlockedTerminationMutation) Request() BlockedTerminationRequest { return m.request }
func (m SealedBlockedTerminationMutation) Proof() BlockedQuiescenceProof      { return m.proof }
func (m SealedBlockedTerminationMutation) Lease() BackendMutationLease        { return m.lease }
func (m SealedBlockedTerminationMutation) CheckpointDigest() string           { return m.checkpoint }
func (m SealedBlockedTerminationMutation) EvidenceDigest() string             { return m.evidence }

// BoundTo reports whether this seal was minted under the same stable write
// authority as the lease the caller is about to use.
//
// Authorized() is deliberately identity-blind: it can say a seal came from
// some production runtime but not which one, and the pointers that carry that
// answer are unexported, so an out-of-package backend cannot read them. The
// lease closes the gap from the other side. RepositoryID distinguishes
// independent stores; scope, owner and fence distinguish the current writer.
// ExpiresAtMs is deliberately not compared because a same-owner renewal is a
// liveness change, not an authority change.
//
// Backends must call this before touching the store. ApplySealedBlockedTermination
// says so in its contract, and the reference backend is the executable copy of
// that requirement.
func (m SealedBlockedTerminationMutation) BoundTo(lease BackendMutationLease) bool {
	return m.Authorized() && m.lease.RepositoryID == lease.RepositoryID &&
		m.lease.Scope == lease.Scope && m.lease.Owner == lease.Owner && m.lease.Fence == lease.Fence
}

// authorizedFor is the mint-time assertion. Note that it compares against the
// same facade that just built the value, so it cannot fail in the ordinary
// path; it exists to catch a future refactor that constructs a seal with a nil
// or borrowed capability. The cross-repository question it reads as answering
// is answered by BoundTo, at the backend, where the two identities can differ.
func (m SealedBlockedTerminationMutation) authorizedFor(identity *repositoryIdentity, sealer *blockedTerminationSealer) bool {
	return m.Authorized() && m.authority == identity && m.owner == sealer
}

// ProductionRuntimeOptions carries everything that must be decided once, at
// construction, for the pairing to mean anything.
type ProductionRuntimeOptions struct {
	RepositoryBackend DurableRepositoryBackend
	Quiescence        BlockedQuiescencePort
	Scope             MutationScope
	Controller        MutationLeaseOwner
	NonceSource       func() (string, error)
	LeaseTTL          time.Duration
	Heartbeat         time.Duration
	MaxProofAge       time.Duration
	Now               func() time.Time
}

// ProductionRuntime pairs a repository facade with the single owner allowed to
// seal terminals against it. Every capability it holds is unexported; callers
// get a plain ScalePlanRepository and an Executor.
type ProductionRuntime struct {
	facade *productionRepositoryFacade
	owner  *BlockedTerminationOwner
	scope  MutationScope
}

func NewProductionRuntime(options ProductionRuntimeOptions) (*ProductionRuntime, error) {
	if options.RepositoryBackend == nil || options.Quiescence == nil {
		return nil, fmt.Errorf("%w: production backend/quiescence required", ErrInvalidPlan)
	}
	if err := options.Scope.Validate(); err != nil {
		return nil, err
	}
	if err := validateOpaque("controller id", options.Controller.ControllerID, MaxOpaqueIDBytes); err != nil {
		return nil, err
	}
	if err := validateOpaque("controller boot id", options.Controller.ControllerBootID, MaxOpaqueIDBytes); err != nil {
		return nil, err
	}
	if options.Controller.OperationNonce != "" {
		return nil, fmt.Errorf("%w: controller nonce is allocated per acquire", ErrInvalidPlan)
	}
	if options.NonceSource == nil {
		return nil, fmt.Errorf("%w: nonce source required", ErrInvalidPlan)
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.LeaseTTL == 0 {
		options.LeaseTTL = defaultMutationLeaseTTL
	}
	if options.LeaseTTL <= 0 || options.LeaseTTL > maximumMutationLeaseTTL {
		return nil, fmt.Errorf("%w: mutation lease ttl", ErrInvalidPlan)
	}
	if options.Heartbeat == 0 {
		options.Heartbeat = defaultMutationHeartbeat
	}
	if options.Heartbeat < minimumMutationHeartbeat || options.Heartbeat*heartbeatToTTLSafetyRatio > options.LeaseTTL {
		return nil, fmt.Errorf("%w: heartbeat must leave room for retries inside the ttl", ErrInvalidPlan)
	}
	repositoryID := options.RepositoryBackend.RepositoryBindingID()
	if err := validateOpaque("repository binding id", repositoryID, MaxOpaqueIDBytes); err != nil {
		return nil, err
	}

	facade := &productionRepositoryFacade{
		identity:     &repositoryIdentity{},
		sealer:       &blockedTerminationSealer{},
		backend:      options.RepositoryBackend,
		repositoryID: repositoryID,
		scope:        options.Scope,
		controller:   options.Controller,
		nonceSource:  options.NonceSource,
		leaseTTL:     options.LeaseTTL,
		heartbeat:    options.Heartbeat,
		now:          options.Now,
	}
	owner, err := newPairedBlockedTerminationOwner(facade, BlockedTerminationOwnerOptions{
		Quiescence: options.Quiescence, Scope: options.Scope,
		Now: options.Now, MaxProofAge: options.MaxProofAge,
	})
	if err != nil {
		return nil, err
	}
	facade.owner = owner
	return &ProductionRuntime{facade: facade, owner: owner, scope: options.Scope}, nil
}

func (r *ProductionRuntime) Repository() ScalePlanRepository { return r.facade }

// EnterPlanMutation stays exported while the owner's equivalent does not: this
// is the sanctioned cross-package entry, and the reference adapter in another
// package needs it to exercise guard-context cancellation (§5.3).
func (r *ProductionRuntime) EnterPlanMutation(ctx context.Context, scope MutationScope) (PlanMutationGuard, error) {
	if r == nil || ctx == nil {
		return nil, ErrInvalidPlan
	}
	return r.owner.enterPlanMutation(ctx, scope)
}

func (r *ProductionRuntime) NewExecutor(options ExecutorOptions) (*Executor, error) {
	if r == nil {
		return nil, ErrInvalidPlan
	}
	options.Repository = r.facade
	options.BlockedTermination = r.owner
	options.Scope = r.scope
	return NewExecutor(options)
}

// productionRepositoryFacade is the unexported implementation of both the
// public repository and the package-private owner capability. Being unexported
// is the mechanism: another package cannot implement repositoryIdentity() and
// therefore cannot pose as a blockedTerminationRepository.
type productionRepositoryFacade struct {
	identity     *repositoryIdentity
	sealer       *blockedTerminationSealer
	backend      DurableRepositoryBackend
	repositoryID string
	owner        *BlockedTerminationOwner
	scope        MutationScope
	controller   MutationLeaseOwner
	nonceSource  func() (string, error)
	leaseTTL     time.Duration
	heartbeat    time.Duration
	now          func() time.Time

	mu      sync.Mutex
	current *leasedMutationGuard
}

func (f *productionRepositoryFacade) repositoryIdentity() *repositoryIdentity { return f.identity }

func (f *productionRepositoryFacade) enterPlanMutation(ctx context.Context, scope MutationScope) (PlanMutationGuard, error) {
	if ctx == nil {
		return nil, ErrInvalidPlan
	}
	if scope != f.scope {
		return nil, fmt.Errorf("%w: mutation scope outside facade realm", ErrInvalidPlan)
	}
	nonce, err := f.nonceSource()
	if err != nil {
		return nil, err
	}
	owner := f.controller
	owner.OperationNonce = nonce
	if err := owner.Validate(); err != nil {
		return nil, err
	}

	// Local serialization first. Re-entering the same facade must block here
	// rather than acquire a second durable lease under a second nonce, which
	// the backend would rightly treat as a competing owner.
	f.mu.Lock()
	if f.current != nil {
		f.mu.Unlock()
		return nil, fmt.Errorf("%w: mutation already held by this facade", ErrInvalidPlan)
	}
	f.mu.Unlock()

	backendLease, err := f.backend.AcquireMutationLease(ctx, scope, owner)
	if err != nil {
		return nil, err
	}
	lease, err := adoptBackendLease(backendLease)
	if err != nil {
		return nil, err
	}
	if lease.repositoryID != f.repositoryID || !lease.owner.equal(owner) || lease.scope != scope {
		return nil, fmt.Errorf("%w: backend returned a lease for another owner", ErrInvalidPlan)
	}

	guardCtx, cancel := context.WithCancel(ctx)
	guard := &leasedMutationGuard{facade: f, lease: lease, ctx: guardCtx, cancel: cancel,
		stop: make(chan struct{}), done: make(chan struct{})}
	f.mu.Lock()
	f.current = guard
	f.mu.Unlock()
	go guard.renew()
	return guard, nil
}

func (f *productionRepositoryFacade) clearGuard(guard *leasedMutationGuard) {
	f.mu.Lock()
	if f.current == guard {
		f.current = nil
	}
	f.mu.Unlock()
}

// heldLease returns the lease of the guard currently open on this facade. Every
// durable write goes through it, so a write attempted outside the barrier is a
// programming error caught here rather than at the store.
func (f *productionRepositoryFacade) heldLease() (BackendMutationLease, error) {
	f.mu.Lock()
	guard := f.current
	f.mu.Unlock()
	if guard == nil {
		return BackendMutationLease{}, fmt.Errorf("%w: durable write outside plan mutation barrier", ErrInvalidPlan)
	}
	if guard.lost() {
		return BackendMutationLease{}, ErrLeaseLost
	}
	return guard.currentLease().backend(), nil
}

func (f *productionRepositoryFacade) CreateOrGet(ctx context.Context, submission IdempotencySubmission, plan Plan) (CreateResult, error) {
	if ctx == nil {
		return CreateResult{}, ErrInvalidPlan
	}
	if err := ValidateInitialPlan(plan); err != nil {
		return CreateResult{}, err
	}
	lease, err := f.heldLease()
	if err != nil {
		return CreateResult{}, err
	}
	return f.backend.CreateOrGet(ctx, lease, DurableCreateCommand{Submission: submission, Plan: plan.Clone()})
}

func (f *productionRepositoryFacade) Load(ctx context.Context, id string) (PlanRecord, bool, error) {
	if ctx == nil {
		return PlanRecord{}, false, ErrInvalidPlan
	}
	return f.backend.Load(ctx, id)
}

func (f *productionRepositoryFacade) CompareAndSwap(ctx context.Context, expected uint64, plan Plan) (CASResult, error) {
	if ctx == nil {
		return CASResult{}, ErrInvalidPlan
	}
	lease, err := f.heldLease()
	if err != nil {
		return CASResult{}, err
	}
	return f.backend.CompareAndSwap(ctx, lease, DurableCASCommand{Expected: expected, Plan: plan.Clone()})
}

func (f *productionRepositoryFacade) ListRecoverable(ctx context.Context) ([]PlanRecord, error) {
	if ctx == nil {
		return nil, ErrInvalidPlan
	}
	var out []PlanRecord
	cursor := RecoverableCursor{}
	// Bounded so that a backend which never sets Done cannot spin here.
	for page := 0; page < 1024; page++ {
		result, err := f.backend.ListRecoverable(ctx, cursor)
		if err != nil {
			return nil, err
		}
		out = append(out, result.Records...)
		if result.Done {
			return out, nil
		}
		if result.Next == cursor {
			return nil, fmt.Errorf("%w: recoverable cursor did not advance", ErrCorruptRecord)
		}
		cursor = result.Next
	}
	return nil, fmt.Errorf("%w: recoverable page bound exhausted", ErrCorruptRecord)
}

// commitBlockedTermination is where the seal is minted. It is the only place in
// the tree that constructs a SealedBlockedTerminationMutation with a non-nil
// authority, and it is unexported.
func (f *productionRepositoryFacade) commitBlockedTermination(ctx context.Context, request BlockedTerminationRequest, proof BlockedQuiescenceProof) (BlockedTerminationResult, error) {
	if ctx == nil {
		return BlockedTerminationResult{}, ErrInvalidPlan
	}
	lease, err := f.heldLease()
	if err != nil {
		return BlockedTerminationResult{}, err
	}
	evidence, err := BlockedQuiescenceProofDigest(proof)
	if err != nil {
		return BlockedTerminationResult{}, err
	}
	sealed := SealedBlockedTerminationMutation{
		authority: f.identity, owner: f.sealer, request: request, proof: proof,
		lease: lease, checkpoint: request.ExpectedCheckpointDigest, evidence: evidence,
	}
	if !sealed.authorizedFor(f.identity, f.sealer) {
		return BlockedTerminationResult{}, fmt.Errorf("%w: seal did not bind", ErrVerification)
	}
	return f.backend.ApplySealedBlockedTermination(ctx, lease, sealed)
}

// leasedMutationGuard renews in the background and cancels the derived context
// the moment renewal stops succeeding.
type leasedMutationGuard struct {
	facade *productionRepositoryFacade
	ctx    context.Context
	cancel context.CancelFunc

	mu    sync.Mutex
	lease DurableMutationLease
	fail  bool

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

func (g *leasedMutationGuard) Context() context.Context { return g.ctx }

func (g *leasedMutationGuard) Lease() DurableMutationLease { return g.currentLease() }

func (g *leasedMutationGuard) currentLease() DurableMutationLease {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.lease
}

func (g *leasedMutationGuard) lost() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.fail
}

func (g *leasedMutationGuard) renew() {
	defer close(g.done)
	ticker := time.NewTicker(g.facade.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-g.stop:
			return
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			current := g.currentLease()
			renewed, err := g.facade.backend.RenewMutationLease(g.ctx, current.backend())
			if err == nil {
				var adopted DurableMutationLease
				if adopted, err = adoptBackendLease(renewed); err == nil {
					if !adopted.sameWriteAuthority(current) {
						err = fmt.Errorf("%w: renewal changed write authority", ErrLeaseLost)
					} else if adopted.expiresAtMs <= current.expiresAtMs {
						err = fmt.Errorf("%w: renewal deadline did not advance", ErrLeaseLost)
					} else {
						g.mu.Lock()
						g.lease = adopted
						g.mu.Unlock()
						continue
					}
				}
			}
			// Renewal is the only liveness signal the durable lease has. Once
			// it fails there is no way to know whether another owner has taken
			// over, so the protected work is cancelled rather than continued.
			g.mu.Lock()
			g.fail = true
			g.mu.Unlock()
			g.cancel()
			return
		}
	}
}

func (g *leasedMutationGuard) Release(ctx context.Context) error {
	if g == nil {
		return nil
	}
	var err error
	g.stopOnce.Do(func() {
		close(g.stop)
		// Joining the renewer before touching the durable lease is what makes
		// a nil return mean "no renewal can still be in flight".
		<-g.done
		g.cancel()
		g.facade.clearGuard(g)
		if ctx == nil {
			ctx = context.Background()
		}
		// Releasing on a cancelled context would leave the durable lease held
		// until expiry, so the compare-and-release gets its own budget.
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		err = g.facade.backend.ReleaseMutationLease(releaseCtx, g.currentLease().backend())
	})
	return err
}
