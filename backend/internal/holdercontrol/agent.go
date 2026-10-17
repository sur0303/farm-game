package holdercontrol

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderidentity"
	core "farmgame/internal/routecontrol"
)

var (
	ErrAgentInvalid          = errors.New("holdercontrol: invalid agent configuration")
	ErrAgentClosed           = errors.New("holdercontrol: agent is closed")
	ErrAgentServerNotStarted = errors.New("holdercontrol: internal server is not started")
	ErrAgentUnhealthy        = errors.New("holdercontrol: agent is unhealthy")
	ErrAgentCloseTimeout     = errors.New("holdercontrol: close timed out before zero shards")
	errHeartbeatDeferred     = errors.New("holdercontrol: heartbeat deferred while active process is temporarily unready")
)

type AgentState string

const (
	AgentStateNew       AgentState = "NEW"
	AgentStateRunning   AgentState = "RUNNING"
	AgentStateUnhealthy AgentState = "UNHEALTHY"
	AgentStateDraining  AgentState = "DRAINING"
	AgentStateDrained   AgentState = "DRAINED"
	AgentStateClosed    AgentState = "CLOSED"
)

// ProcessStatus is the only runtime status input consumed by HolderAgent.
// It contains no authority or writer capability.
type ProcessStatus struct {
	ProcessReady      bool
	Draining          bool
	ServingShardCount uint32
	SnapshotRevision  uint64
	CapacityWeight    uint32
	ResourceSummary   map[string]string
}

type ProcessStatusSource interface {
	Status(context.Context) (ProcessStatus, error)
}

type ProcessStatusSourceFunc func(context.Context) (ProcessStatus, error)

func (f ProcessStatusSourceFunc) Status(ctx context.Context) (ProcessStatus, error) {
	if f == nil {
		return ProcessStatus{}, ErrAgentInvalid
	}
	return f(ctx)
}

type HolderRegistryPort interface {
	Register(context.Context, core.HolderRegistration) (core.HolderRecord, error)
	Heartbeat(context.Context, core.HolderHeartbeat) (core.HolderRecord, error)
	Get(context.Context, actorrouting.RouteDomain, string) (core.HolderRecord, bool, error)
}

type HolderAgentOptions struct {
	Registry              HolderRegistryPort
	ProcessStatus         ProcessStatusSource
	Domain                actorrouting.RouteDomain
	HolderID              string
	DeploymentIdentity    holderidentity.DeploymentIdentity
	LogicEndpoint         string
	HolderControlEndpoint string
	TLSDNSIdentity        string
	CapacityWeight        uint32
	ResourceSummary       map[string]string
	// AllowInitialServing permits this Agent to request the exceptional first
	// WARMING -> SERVING heartbeat. It is not authorization: HolderRegistry must
	// independently verify the installed static assignment, exact holder/boot,
	// open gates, and owned ACTIVE authority guards before persisting it.
	// Dynamic zero-shard targets leave it false and enter READY_ZERO_SHARD.
	AllowInitialServing bool

	// ServerReady is a construction-time seam. The agent never starts the
	// server; Start is called only after the external server has started.
	ServerReady   func() bool
	ServerStarted func() bool
	BootID        func() (string, error)

	HeartbeatInterval           time.Duration
	HeartbeatTimeout            time.Duration
	CloseTimeout                time.Duration
	ClosePollInterval           time.Duration
	ConsecutiveFailureThreshold int
	LeaseTTL                    time.Duration
	Now                         func() time.Time
}

func DefaultHolderAgentOptions() HolderAgentOptions {
	return HolderAgentOptions{
		HeartbeatInterval:           5 * time.Second,
		HeartbeatTimeout:            2 * time.Second,
		CloseTimeout:                10 * time.Second,
		ClosePollInterval:           100 * time.Millisecond,
		ConsecutiveFailureThreshold: 3,
		LeaseTTL:                    core.DefaultHolderLeaseTTL,
		Now:                         time.Now,
	}
}

func (o HolderAgentOptions) normalized() (HolderAgentOptions, error) {
	if o.Registry == nil || o.ProcessStatus == nil {
		return HolderAgentOptions{}, fmt.Errorf("%w: registry and process status source are required", ErrAgentInvalid)
	}
	if o.Domain != actorrouting.DomainPlayer && o.Domain != actorrouting.DomainFarm {
		return HolderAgentOptions{}, fmt.Errorf("%w: domain", ErrAgentInvalid)
	}
	if o.HolderID == "" || o.LogicEndpoint == "" || o.HolderControlEndpoint == "" || o.TLSDNSIdentity == "" {
		return HolderAgentOptions{}, fmt.Errorf("%w: holder identity and both endpoints are required", ErrAgentInvalid)
	}
	if err := o.DeploymentIdentity.ValidateOptional(); err != nil {
		return HolderAgentOptions{}, fmt.Errorf("%w: deployment identity: %v", ErrAgentInvalid, err)
	}
	if o.ServerReady == nil && o.ServerStarted == nil {
		o.ServerReady = func() bool { return true }
	}
	if o.BootID == nil {
		o.BootID = holderidentity.RandomBootID
	}
	if o.HeartbeatInterval <= 0 {
		o.HeartbeatInterval = 5 * time.Second
	}
	if o.HeartbeatTimeout <= 0 {
		o.HeartbeatTimeout = 2 * time.Second
	}
	if o.CloseTimeout <= 0 {
		o.CloseTimeout = 10 * time.Second
	}
	if o.ClosePollInterval <= 0 {
		o.ClosePollInterval = 100 * time.Millisecond
	}
	if o.ConsecutiveFailureThreshold <= 0 {
		o.ConsecutiveFailureThreshold = 3
	}
	if o.LeaseTTL <= 0 {
		o.LeaseTTL = core.DefaultHolderLeaseTTL
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.CapacityWeight == 0 {
		o.CapacityWeight = 1
	}
	for name, value := range map[string]time.Duration{
		"heartbeat interval":  o.HeartbeatInterval,
		"heartbeat timeout":   o.HeartbeatTimeout,
		"close timeout":       o.CloseTimeout,
		"close poll interval": o.ClosePollInterval,
		"lease ttl":           o.LeaseTTL,
	} {
		if value < time.Millisecond {
			return HolderAgentOptions{}, fmt.Errorf("%w: %s must be at least 1ms", ErrAgentInvalid, name)
		}
	}
	if o.ClosePollInterval >= o.LeaseTTL {
		return HolderAgentOptions{}, fmt.Errorf("%w: close poll interval must be shorter than lease TTL", ErrAgentInvalid)
	}
	if !agentLeaseBudgetSafe(o.HeartbeatInterval, o.HeartbeatTimeout, o.LeaseTTL) {
		return HolderAgentOptions{}, fmt.Errorf(
			"%w: heartbeat interval plus two heartbeat timeouts must be strictly shorter than lease TTL",
			ErrAgentInvalid,
		)
	}
	if !agentLeaseBudgetSafe(o.ClosePollInterval, o.HeartbeatTimeout, o.LeaseTTL) {
		return HolderAgentOptions{}, fmt.Errorf(
			"%w: close poll interval plus two heartbeat timeouts must be strictly shorter than lease TTL",
			ErrAgentInvalid,
		)
	}
	o.ResourceSummary = cloneResourceSummary(o.ResourceSummary)
	return o, nil
}

// agentLeaseBudgetSafe proves interval + 2*timeout < leaseTTL without ever
// adding durations. time.Duration is an int64, so construction must not let a
// wrapped sum turn an unsafe cadence into an accepted one.
func agentLeaseBudgetSafe(interval, timeout, leaseTTL time.Duration) bool {
	if interval >= leaseTTL {
		return false
	}
	remaining := leaseTTL - interval
	if timeout >= remaining {
		return false
	}
	return timeout < remaining-timeout
}

type AgentStatus struct {
	State               AgentState
	Holder              core.HolderRecord
	ConsecutiveFailures int
	Unhealthy           bool
	LastError           error
	HeartbeatInFlight   bool
}

type HolderAgent struct {
	opts HolderAgentOptions

	mu                 sync.Mutex
	state              AgentState
	bootID             string
	bootResolved       bool
	record             core.HolderRecord
	failures           int
	lastErr            error
	ctx                context.Context
	cancel             context.CancelFunc
	done               chan struct{}
	doneClosed         bool
	lifecycle          agentLifecycleState
	startAttempt       *agentStartAttempt
	startJoinHook      func(*agentStartAttempt)
	startWaitHook      func(*agentStartAttempt)
	startFinishHook    func(*agentStartAttempt)
	closeDone          chan struct{}
	closeErr           error
	closeComplete      bool
	closeRequested     bool
	closeWorkerStarted bool
	closeDeadline      time.Time
	runStarted         bool
	inFlight           atomic.Bool
	// appliedKick coalesces Snapshot-apply notifications onto the heartbeat
	// loop. Buffer 1 is enough: overlapping kicks share one inFlight CAS.
	appliedKick chan struct{}
}

type agentStartAttempt struct {
	done     chan struct{}
	err      error
	complete bool
}

type agentLifecycleState uint8

const (
	agentLifecycleNew agentLifecycleState = iota
	agentLifecycleStarting
	agentLifecycleStarted
	agentLifecycleClosing
	agentLifecycleClosed
)

var _ HolderLifecyclePort = (*HolderAgent)(nil)

type HolderLifecyclePort interface {
	Start(context.Context) error
	Close(context.Context) error
	Status() AgentStatus
	BootID() string
}

func NewHolderAgent(opts HolderAgentOptions) (*HolderAgent, error) {
	normalized, err := opts.normalized()
	if err != nil {
		return nil, err
	}
	return &HolderAgent{
		opts:        normalized,
		state:       AgentStateNew,
		lifecycle:   agentLifecycleNew,
		appliedKick: make(chan struct{}, 1),
	}, nil
}

// NotifySnapshotApplied asks the running heartbeat loop to report the local
// SnapshotRevision immediately. Lease renewal still follows HeartbeatInterval.
// The status source decides which applied revision is publishable evidence; a
// revision that already emptied this holder stays withheld until the local
// shards are actually gone, so an immediate kick cannot spend the revision that
// proves READY_ZERO_SHARD.
func (a *HolderAgent) NotifySnapshotApplied() {
	if a == nil || a.appliedKick == nil {
		return
	}
	select {
	case a.appliedKick <- struct{}{}:
	default:
	}
}

func (a *HolderAgent) BootID() string {
	if a == nil {
		return ""
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.bootID
}

func (a *HolderAgent) Start(ctx context.Context) error {
	if a == nil {
		return ErrAgentInvalid
	}
	if ctx == nil {
		return ErrAgentInvalid
	}
	a.mu.Lock()
	var attempt *agentStartAttempt
	switch a.lifecycle {
	case agentLifecycleStarted:
		if a.closeRequested {
			a.mu.Unlock()
			return ErrAgentClosed
		}
		a.mu.Unlock()
		return nil
	case agentLifecycleStarting:
		attempt = a.startAttempt
		joinHook := a.startJoinHook
		a.mu.Unlock()
		if joinHook != nil {
			joinHook(attempt)
		}
		return a.waitForStart(ctx, attempt)
	case agentLifecycleClosing, agentLifecycleClosed:
		a.mu.Unlock()
		return ErrAgentClosed
	case agentLifecycleNew:
		a.lifecycle = agentLifecycleStarting
		attempt = &agentStartAttempt{done: make(chan struct{})}
		a.startAttempt = attempt
	}
	a.mu.Unlock()

	if !serverIsReady(a.opts) {
		return a.finishStartBeforeRegister(attempt, ErrAgentServerNotStarted)
	}
	bootID, err := a.resolveBootID()
	if err != nil {
		return a.finishStartBeforeRegister(attempt, err)
	}
	now := a.opts.Now()
	statusCtx, cancelStatus := context.WithTimeout(ctx, a.opts.HeartbeatTimeout)
	status, statusErr := boundedAgentCall(statusCtx, a.opts.ProcessStatus.Status)
	cancelStatus()
	if statusErr != nil {
		return a.finishStartBeforeRegister(attempt, statusErr)
	}
	a.mu.Lock()
	closeRequested := a.closeRequested
	a.mu.Unlock()
	if closeRequested {
		return a.finishStartBeforeRegister(attempt, ErrAgentClosed)
	}
	record := core.HolderRegistration{
		Domain: a.opts.Domain, HolderID: a.opts.HolderID, BootID: bootID,
		DeploymentIdentity: a.opts.DeploymentIdentity,
		LogicEndpoint:      a.opts.LogicEndpoint, HolderControlEndpoint: a.opts.HolderControlEndpoint,
		TLSDNSIdentity: a.opts.TLSDNSIdentity,
		State:          core.HolderStateWarming, ProcessReady: false,
		Draining: false, ServingShardCount: 0, SnapshotRevision: status.SnapshotRevision,
		StartedAt: now, HeartbeatAt: now, LeaseUntil: now.Add(a.opts.LeaseTTL),
		Sequence: 1, CapacityWeight: pickWeight(status.CapacityWeight, a.opts.CapacityWeight),
		ResourceSummary: pickSummary(status.ResourceSummary, a.opts.ResourceSummary),
	}

	registered, record, err := a.registerAfterPriorBootExpires(ctx, record)
	if a.finishLateStartAfterClosed(attempt) {
		return ErrAgentClosed
	}
	if err != nil {
		return a.reconcileUncertainRegistration(attempt, record, bootID, err)
	}
	registryOptions := core.DefaultHolderRegistryOptions()
	registryOptions.LeaseTTL = a.opts.LeaseTTL
	registryOptions.MinLeaseTTL = time.Millisecond
	if registryOptions.MaxLeaseTTL < a.opts.LeaseTTL {
		registryOptions.MaxLeaseTTL = a.opts.LeaseTTL
	}
	// Waiting out a prior boot's lease can span a lease TTL, which exceeds the
	// registry's skew allowance, and the retry loop restamps the candidate.
	responseNow := time.UnixMilli(a.opts.Now().UnixMilli())
	if validateErr := core.ValidateHolderRegistration(registered, responseNow, registryOptions); validateErr != nil {
		return a.reconcileUncertainRegistration(attempt, record, bootID, fmt.Errorf(
			"%w: invalid registration response: %v", ErrAgentInvalid, validateErr,
		))
	}
	if !core.HolderRecordEqual(registered, record) {
		return a.reconcileUncertainRegistration(attempt, record, bootID, fmt.Errorf(
			"%w: registration response differs from candidate", ErrAgentInvalid,
		))
	}

	// The running lifecycle is agent-owned. Start-stage status/Register/initial
	// heartbeat calls still use the caller context, but cancellation after a
	// successful Start cannot stop periodic liveness.
	runCtx, cancelRun := context.WithCancel(context.Background())
	a.mu.Lock()
	if a.closeComplete || a.lifecycle == agentLifecycleClosed {
		a.completeStartLocked(attempt, ErrAgentClosed)
		a.mu.Unlock()
		cancelRun()
		return ErrAgentClosed
	}
	a.state = AgentStateRunning
	a.bootID = bootID
	a.record = cloneRecord(registered)
	a.failures = 0
	a.lastErr = nil
	a.ctx, a.cancel = runCtx, cancelRun
	a.done = make(chan struct{})
	a.doneClosed = false
	a.runStarted = false
	closeRequested = a.closeRequested
	if closeRequested {
		a.lifecycle = agentLifecycleClosing
		a.state = AgentStateDraining
		a.closeRunDoneLocked()
		a.completeStartLocked(attempt, ErrAgentClosed)
	}
	a.mu.Unlock()
	if closeRequested {
		return ErrAgentClosed
	}

	if err := a.heartbeatOnceWithMode(ctx, true); err != nil {
		a.mu.Lock()
		if a.closeComplete || a.lifecycle == agentLifecycleClosed {
			a.completeStartLocked(attempt, ErrAgentClosed)
			a.mu.Unlock()
			return ErrAgentClosed
		}
		if errors.Is(err, ErrAgentClosed) && a.closeRequested {
			a.lifecycle = agentLifecycleClosing
			a.state = AgentStateDraining
			a.closeRunDoneLocked()
			a.completeStartLocked(attempt, ErrAgentClosed)
			a.mu.Unlock()
			if a.startFinishHook != nil {
				a.startFinishHook(attempt)
			}
			return ErrAgentClosed
		}
		a.closeRequested = true
		a.lifecycle = agentLifecycleClosing
		if a.state != AgentStateUnhealthy {
			a.state = AgentStateDraining
		}
		a.closeRunDoneLocked()
		closeDone := a.ensureCloseDoneLocked()
		startWorker := a.startCloseWorkerLocked()
		a.completeStartLocked(attempt, err)
		a.mu.Unlock()
		if startWorker {
			go a.runCloseWorker(err)
		}
		<-closeDone
		return err
	}
	a.mu.Lock()
	closeRequested = a.closeRequested || a.lifecycle == agentLifecycleClosing
	if a.lifecycle == agentLifecycleStarting {
		if closeRequested {
			a.lifecycle = agentLifecycleClosing
			a.state = AgentStateDraining
			a.closeRunDoneLocked()
			a.completeStartLocked(attempt, ErrAgentClosed)
		} else {
			// Start success, the run owner, and the immutable attempt result
			// linearize together. Once Close can observe Started, done already
			// has a goroutine owner which will close it after cancellation.
			a.lifecycle = agentLifecycleStarted
			a.runStarted = true
			go a.run(runCtx)
			a.completeStartLocked(attempt, nil)
		}
	}
	a.mu.Unlock()
	if a.startFinishHook != nil {
		a.startFinishHook(attempt)
	}
	if closeRequested {
		return ErrAgentClosed
	}
	return nil
}

func (a *HolderAgent) resolveBootID() (string, error) {
	a.mu.Lock()
	if a.bootResolved {
		bootID := a.bootID
		a.mu.Unlock()
		return bootID, nil
	}
	a.mu.Unlock()
	bootID, err := a.opts.BootID()
	if err != nil {
		return "", fmt.Errorf("%w: boot id: %v", ErrAgentInvalid, err)
	}
	if bootID == "" {
		return "", fmt.Errorf("%w: boot id is empty", ErrAgentInvalid)
	}
	a.mu.Lock()
	if !a.bootResolved {
		a.bootID = bootID
		a.bootResolved = true
	} else {
		bootID = a.bootID
	}
	a.mu.Unlock()
	return bootID, nil
}

func (a *HolderAgent) registerAfterPriorBootExpires(
	ctx context.Context,
	candidate core.HolderRegistration,
) (core.HolderRecord, core.HolderRegistration, error) {
	const retryBackoff = 100 * time.Millisecond
	for {
		registerCtx, cancelRegister := context.WithTimeout(
			ctx,
			a.opts.HeartbeatTimeout,
		)
		registered, err := a.opts.Registry.Register(
			registerCtx,
			candidate,
		)
		cancelRegister()
		if err == nil {
			return registered, candidate, err
		}
		if errors.Is(err, core.ErrHolderHeartbeatStale) || errors.Is(err, core.ErrHolderBootConflict) {
			// Holder discovery can attest and durably materialize this exact
			// Kubernetes/process generation before the process-owned Agent wins
			// its initial Register race.  Treat the later WARMING lease as the
			// same generation's idempotent report, then let the Agent's first
			// heartbeat take over sequence ownership.  A different boot,
			// deployment identity, endpoint, lifecycle, or regressing liveness
			// fact remains fail closed.
			reconcileCtx, cancelReconcile := a.newHeartbeatReconcileContext()
			durable, found, getErr := a.registryGet(
				reconcileCtx,
				candidate.Domain,
				candidate.HolderID,
			)
			cancelReconcile()
			if getErr == nil && found &&
				a.validateRegistryGetRecord(durable) == nil &&
				adoptableMaterializedRegistration(durable, candidate) {
				return durable, cloneRecord(durable), nil
			}
			if errors.Is(err, core.ErrHolderHeartbeatStale) {
				return core.HolderRecord{}, candidate, err
			}
			// A same-boot record which is not the exact provisional WARMING
			// generation above is not safe to adopt or wait through.  It may be
			// a conflicting writer or a lifecycle regression and remains
			// fail-closed.
			if getErr != nil || !found ||
				a.validateRegistryGetRecord(durable) != nil ||
				durable.Domain != candidate.Domain ||
				durable.HolderID != candidate.HolderID ||
				durable.BootID == candidate.BootID {
				return core.HolderRecord{}, candidate, err
			}
		}
		if !errors.Is(err, core.ErrHolderBootConflict) {
			return registered, candidate, err
		}

		// ErrHolderBootConflict normally means a prior process generation. If
		// the common adoption branch above did not load it already, load the
		// exact durable generation now and wait only for that older lease.
		reconcileCtx, cancelReconcile := a.newHeartbeatReconcileContext()
		durable, found, getErr := a.registryGet(reconcileCtx, candidate.Domain, candidate.HolderID)
		cancelReconcile()
		if getErr != nil || !found || a.validateRegistryGetRecord(durable) != nil ||
			durable.Domain != candidate.Domain || durable.HolderID != candidate.HolderID ||
			durable.BootID == candidate.BootID {
			return core.HolderRecord{}, candidate, err
		}

		wait := durable.LeaseUntil.Sub(a.opts.Now())
		if wait <= 0 {
			wait = time.Millisecond
		}
		if wait > retryBackoff {
			wait = retryBackoff
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return core.HolderRecord{}, candidate, ctx.Err()
		case <-timer.C:
		}
		now := time.UnixMilli(a.opts.Now().UnixMilli())
		candidate.StartedAt = now
		candidate.HeartbeatAt = now
		candidate.LeaseUntil = now.Add(a.opts.LeaseTTL)
	}
}

func adoptableMaterializedRegistration(durable, candidate core.HolderRecord) bool {
	return durable.Domain == candidate.Domain &&
		durable.HolderID == candidate.HolderID &&
		durable.BootID == candidate.BootID &&
		durable.DeploymentIdentity == candidate.DeploymentIdentity &&
		durable.LogicEndpoint == candidate.LogicEndpoint &&
		durable.HolderControlEndpoint == candidate.HolderControlEndpoint &&
		durable.TLSDNSIdentity == candidate.TLSDNSIdentity &&
		durable.State == core.HolderStateWarming &&
		!durable.ProcessReady &&
		!durable.Draining &&
		durable.ServingShardCount == 0 &&
		durable.Sequence == candidate.Sequence &&
		!durable.LeaseUntil.Before(candidate.LeaseUntil)
}

func (a *HolderAgent) waitForStart(ctx context.Context, attempt *agentStartAttempt) error {
	if attempt == nil || attempt.done == nil {
		return ErrAgentInvalid
	}
	select {
	case <-attempt.done:
		if a.startWaitHook != nil {
			a.startWaitHook(attempt)
		}
		return attempt.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *HolderAgent) finishStartBeforeRegister(attempt *agentStartAttempt, err error) error {
	a.mu.Lock()
	if a.closeComplete || a.lifecycle == agentLifecycleClosed {
		a.completeStartLocked(attempt, ErrAgentClosed)
		a.mu.Unlock()
		return ErrAgentClosed
	}
	if a.closeRequested {
		a.lifecycle = agentLifecycleClosing
		a.state = AgentStateClosed
	} else {
		a.lifecycle = agentLifecycleNew
		a.state = AgentStateNew
	}
	a.completeStartLocked(attempt, err)
	a.mu.Unlock()
	return err
}

// finishLateStartAfterClosed is the post-RPC fence for dependencies that return
// after the unique close worker has already exhausted its budget. A late
// Register commit may remain WARMING until its bounded lease/physical TTL
// expires, but it can never reopen the closed lifecycle or start the run loop.
func (a *HolderAgent) finishLateStartAfterClosed(attempt *agentStartAttempt) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.closeComplete && a.lifecycle != agentLifecycleClosed {
		return false
	}
	a.completeStartLocked(attempt, ErrAgentClosed)
	return true
}

// reconcileUncertainRegistration distinguishes a definite no-commit from a
// committed WARMING candidate after any Register error or response drift. The
// read is agent-owned and bounded because the Start caller may already be
// canceled. A proven candidate is installed only as a cleanup baseline; Start
// remains fail closed and the unique background close worker drains it.
func (a *HolderAgent) reconcileUncertainRegistration(
	attempt *agentStartAttempt,
	candidate core.HolderRegistration,
	bootID string,
	registerErr error,
) error {
	reconcileCtx, cancel := a.newHeartbeatReconcileContext()
	durable, found, getErr := a.registryGet(reconcileCtx, candidate.Domain, candidate.HolderID)
	cancel()
	if a.finishLateStartAfterClosed(attempt) {
		return ErrAgentClosed
	}
	if getErr == nil && found {
		getErr = a.validateRegistryGetRecord(durable)
	}
	if getErr == nil && !found {
		return a.finishStartBeforeRegister(attempt, registerErr)
	}
	if getErr != nil || !found || !core.HolderRecordEqual(durable, candidate) {
		reconcileErr := getErr
		if reconcileErr == nil {
			reconcileErr = fmt.Errorf("durable registration differs from candidate")
		}
		unhealthyErr := fmt.Errorf(
			"%w: uncertain registration: register=%v reconcile=%v",
			ErrAgentUnhealthy, registerErr, reconcileErr,
		)
		a.mu.Lock()
		if a.closeComplete || a.lifecycle == agentLifecycleClosed {
			a.completeStartLocked(attempt, ErrAgentClosed)
			a.mu.Unlock()
			return ErrAgentClosed
		}
		a.closeRequested = true
		a.lifecycle = agentLifecycleClosed
		a.state = AgentStateUnhealthy
		a.lastErr = unhealthyErr
		a.failures = a.opts.ConsecutiveFailureThreshold
		a.closeErr = unhealthyErr
		a.ensureCloseDoneLocked()
		if !a.closeComplete {
			a.closeComplete = true
			close(a.closeDone)
		}
		a.completeStartLocked(attempt, unhealthyErr)
		a.mu.Unlock()
		return unhealthyErr
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	a.mu.Lock()
	if a.closeComplete || a.lifecycle == agentLifecycleClosed {
		a.completeStartLocked(attempt, ErrAgentClosed)
		a.mu.Unlock()
		cancelRun()
		return ErrAgentClosed
	}
	a.state = AgentStateDraining
	a.bootID = bootID
	a.record = cloneRecord(candidate)
	a.failures = 0
	a.lastErr = registerErr
	a.ctx, a.cancel = runCtx, cancelRun
	a.done = make(chan struct{})
	a.doneClosed = false
	a.runStarted = false
	a.closeRequested = true
	a.lifecycle = agentLifecycleClosing
	a.closeRunDoneLocked()
	a.ensureCloseDoneLocked()
	startWorker := a.startCloseWorkerLocked()
	a.completeStartLocked(attempt, registerErr)
	a.mu.Unlock()
	if startWorker {
		go a.runCloseWorker(registerErr)
	}
	return registerErr
}

func (a *HolderAgent) completeStart(attempt *agentStartAttempt, err error) {
	a.mu.Lock()
	a.completeStartLocked(attempt, err)
	a.mu.Unlock()
}

func (a *HolderAgent) completeStartLocked(attempt *agentStartAttempt, err error) {
	if attempt == nil || attempt.complete {
		return
	}
	attempt.err = err
	attempt.complete = true
	close(attempt.done)
}

func (a *HolderAgent) run(runCtx context.Context) {
	ticker := time.NewTicker(a.opts.HeartbeatInterval)
	defer ticker.Stop()
	defer a.finishRun()
	for {
		select {
		case <-runCtx.Done():
			return
		case <-ticker.C:
			_ = a.heartbeatOnce(runCtx)
		case <-a.appliedKick:
			_ = a.heartbeatOnce(runCtx)
		}
	}
}

func (a *HolderAgent) heartbeatOnce(parent context.Context) error {
	return a.heartbeatOnceWithMode(parent, false)
}

func (a *HolderAgent) heartbeatOnceWithMode(parent context.Context, initial bool) error {
	if a == nil || parent == nil {
		return ErrAgentInvalid
	}
	if !a.inFlight.CompareAndSwap(false, true) {
		return nil
	}
	defer a.inFlight.Store(false)

	statusCtx, cancelStatus := context.WithTimeout(parent, a.opts.HeartbeatTimeout)
	status, err := boundedAgentCall(statusCtx, a.opts.ProcessStatus.Status)
	cancelStatus()
	if err != nil {
		if !initial {
			a.recordRuntimeFailure(err)
		}
		return err
	}
	heartbeatAt := a.opts.Now()
	a.mu.Lock()
	// This lock is the initial-heartbeat admission point. Close wins by setting
	// closeRequested before this check; once this check admits the RPC, a nil
	// response may publish READY_ZERO_SHARD and Close advances from that commit.
	if (initial && (a.lifecycle != agentLifecycleStarting || a.closeRequested)) ||
		(!initial && (a.lifecycle != agentLifecycleStarted || a.closeRequested)) ||
		a.state == AgentStateUnhealthy || a.state == AgentStateClosed {
		state := a.state
		closeRequested := a.closeRequested
		a.mu.Unlock()
		if state == AgentStateClosed || (initial && closeRequested) {
			return ErrAgentClosed
		}
		return ErrAgentUnhealthy
	}
	previous := a.record
	sequence, sequenceErr := nextAgentSequence(previous.Sequence)
	if sequenceErr != nil {
		a.mu.Unlock()
		if !initial {
			a.recordRuntimeFailure(sequenceErr)
		}
		return sequenceErr
	}
	if heartbeatAt.Before(previous.HeartbeatAt) {
		heartbeatAt = previous.HeartbeatAt
	}
	leaseUntil := heartbeatAt.Add(a.opts.LeaseTTL)
	if leaseUntil.Before(previous.LeaseUntil) {
		leaseUntil = previous.LeaseUntil
	}
	var nextState core.HolderState
	var stateErr error
	if initial {
		if !status.ProcessReady || status.Draining {
			stateErr = fmt.Errorf("%w: initial heartbeat requires ready non-draining status", ErrAgentUnhealthy)
		} else if status.ServingShardCount == 0 {
			nextState = core.HolderStateReadyZeroShard
		} else if a.opts.AllowInitialServing {
			nextState = core.HolderStateServing
		} else {
			stateErr = fmt.Errorf("%w: initial serving shards require explicit static-bootstrap admission", ErrAgentUnhealthy)
		}
	} else {
		nextState, stateErr = deriveHolderState(status, previous.State)
	}
	if stateErr != nil {
		a.mu.Unlock()
		if errors.Is(stateErr, errHeartbeatDeferred) {
			// Do not durably mutate READY_ZERO_SHARD/SERVING and do not consume
			// the Agent's terminal failure budget for a transient fail-closed
			// assignment outage. The existing Holder lease remains the hard
			// upper bound; recovery after lease expiry is still rejected by
			// HolderRegistry.
			return nil
		}
		if !initial {
			a.recordRuntimeFailure(stateErr)
		}
		return stateErr
	}
	snapshotRevision := maxUint64(status.SnapshotRevision, previous.SnapshotRevision)
	if previous.State == core.HolderStateServing &&
		nextState == core.HolderStateReadyZeroShard &&
		snapshotRevision <= previous.SnapshotRevision {
		// Redis HeartbeatHolderV6 rejects SERVING → READY_ZERO_SHARD unless
		// SnapshotRevision strictly increases. Wait for the proving revision
		// rather than burning the Agent failure budget on a known-illegal CAS.
		a.mu.Unlock()
		return nil
	}
	next := core.HolderHeartbeat{
		Domain: a.opts.Domain, HolderID: a.opts.HolderID, BootID: a.bootID,
		State: nextState, ProcessReady: status.ProcessReady, Draining: status.Draining,
		ServingShardCount: status.ServingShardCount,
		SnapshotRevision:  snapshotRevision,
		HeartbeatAt:       heartbeatAt, LeaseUntil: leaseUntil, Sequence: sequence,
		CapacityWeight:  pickWeight(status.CapacityWeight, a.opts.CapacityWeight),
		ResourceSummary: pickSummary(status.ResourceSummary, a.opts.ResourceSummary),
	}
	a.mu.Unlock()
	expected := holderRecordAfterHeartbeat(previous, next)

	callCtx, cancelCall := context.WithTimeout(parent, a.opts.HeartbeatTimeout)
	updated, err := a.registryHeartbeat(callCtx, next)
	cancelCall()
	if err == nil && core.HolderRecordEqual(updated, expected) {
		installed, closedAfterCommit := a.installHeartbeatCandidate(expected, initial, true, nil)
		if !installed {
			return ErrAgentClosed
		}
		if closedAfterCommit {
			return ErrAgentClosed
		}
		return nil
	}
	anomaly := err
	if anomaly == nil {
		anomaly = fmt.Errorf("%w: heartbeat response differs from candidate", ErrAgentInvalid)
	}
	// Reconciliation is always agent-owned. It must survive the run-context
	// cancellation which exposed an ambiguous periodic commit, while a Close
	// request constrains it to the unique worker's immutable deadline.
	_, _, _ = a.reconcileHeartbeatCandidate(previous, expected, initial, anomaly)
	return anomaly
}

func holderRecordAfterHeartbeat(previous core.HolderRecord, heartbeat core.HolderHeartbeat) core.HolderRecord {
	expected := previous
	expected.State = heartbeat.State
	expected.ProcessReady = heartbeat.ProcessReady
	expected.Draining = heartbeat.Draining
	expected.ServingShardCount = heartbeat.ServingShardCount
	expected.SnapshotRevision = heartbeat.SnapshotRevision
	expected.HeartbeatAt = heartbeat.HeartbeatAt
	expected.LeaseUntil = heartbeat.LeaseUntil
	expected.Sequence = heartbeat.Sequence
	expected.CapacityWeight = heartbeat.CapacityWeight
	expected.ResourceSummary = cloneResourceSummary(heartbeat.ResourceSummary)
	return expected
}

func (a *HolderAgent) installHeartbeatCandidate(
	expected core.HolderRecord,
	initial bool,
	resetFailures bool,
	anomaly error,
) (installed bool, closedAfterCommit bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closeDeadlineReachedLocked() {
		// A dependency result accepted after the shared close deadline could
		// reopen an already fail-closed owner. The bounded call's late result
		// goroutine may finish, but it never writes Agent state.
		return false, true
	}
	closedAfterCommit = a.closeRequested || a.closeComplete ||
		a.lifecycle == agentLifecycleClosing || a.lifecycle == agentLifecycleClosed ||
		(initial && a.lifecycle != agentLifecycleStarting) ||
		(!initial && a.lifecycle != agentLifecycleStarted)
	// Exact durable state is the sequence baseline even after Close has won.
	// The close worker waits for the run owner and will publish from this record.
	a.record = cloneRecord(expected)
	if resetFailures && !a.closeComplete && a.state != AgentStateUnhealthy {
		a.failures = 0
		a.lastErr = nil
	}
	if anomaly != nil && !initial && !closedAfterCommit {
		a.recordFailureLocked(anomaly)
	}
	return true, closedAfterCommit
}

func (a *HolderAgent) closeDeadlineReachedLocked() bool {
	if !a.closeRequested || a.closeDeadline.IsZero() {
		return false
	}
	return !time.Now().Before(a.closeDeadline)
}

func (a *HolderAgent) newHeartbeatReconcileContext() (context.Context, context.CancelFunc) {
	now := time.Now()
	heartbeatDeadline := now.Add(a.opts.HeartbeatTimeout)
	a.mu.Lock()
	closing := a.closeRequested
	closeDeadline := a.closeDeadline
	a.mu.Unlock()
	if closing {
		if closeDeadline.IsZero() {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx, func() {}
		}
		if closeDeadline.Before(heartbeatDeadline) {
			heartbeatDeadline = closeDeadline
		}
	}
	return context.WithDeadline(context.Background(), heartbeatDeadline)
}

type agentCallResult[T any] struct {
	value T
	err   error
}

// boundedAgentCall hard-bounds an interface which may ignore its context. One
// buffered result lets a late dependency return without blocking. Go cannot
// kill a malicious call which never returns, so that internal goroutine may
// remain, but the Agent owner completes and starts no follow-up call after its
// deadline. The caller is the only code allowed to consume the value and mutate
// Agent state.
func boundedAgentCall[T any](
	ctx context.Context,
	call func(context.Context) (T, error),
) (T, error) {
	var zero T
	if err := agentContextError(ctx); err != nil {
		return zero, err
	}
	result := make(chan agentCallResult[T], 1)
	go func() {
		value, err := call(ctx)
		result <- agentCallResult[T]{value: value, err: err}
	}()
	select {
	case <-ctx.Done():
		// Preserve a result which completed synchronously with cancellation,
		// but never prefer it to an expired deadline. This retains the
		// dependency's concrete anomaly without waiting for an uncooperative
		// implementation.
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			select {
			case completed := <-result:
				return completed.value, completed.err
			default:
			}
		}
		return zero, ctx.Err()
	case completed := <-result:
		if err := agentContextError(ctx); errors.Is(err, context.DeadlineExceeded) {
			return zero, err
		}
		return completed.value, completed.err
	}
}

func agentContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

type agentRegistryGetResult struct {
	record core.HolderRecord
	found  bool
}

func (a *HolderAgent) registryHeartbeat(
	ctx context.Context,
	heartbeat core.HolderHeartbeat,
) (core.HolderRecord, error) {
	return boundedAgentCall(ctx, func(callCtx context.Context) (core.HolderRecord, error) {
		return a.opts.Registry.Heartbeat(callCtx, heartbeat)
	})
}

func (a *HolderAgent) registryGet(
	ctx context.Context,
	domain actorrouting.RouteDomain,
	holderID string,
) (core.HolderRecord, bool, error) {
	result, err := boundedAgentCall(ctx, func(callCtx context.Context) (agentRegistryGetResult, error) {
		record, found, getErr := a.opts.Registry.Get(callCtx, domain, holderID)
		return agentRegistryGetResult{record: record, found: found}, getErr
	})
	return result.record, result.found, err
}

// reconcileHeartbeatCandidate fences every uncertain Heartbeat outcome with a
// bounded exact Get. Only a durable full candidate advances the local
// sequence. A proven previous record leaves the sequence unchanged; foreign,
// missing, or unreadable state is immediately sticky because sequence reuse is
// no longer safe.
func (a *HolderAgent) reconcileHeartbeatCandidate(
	previous core.HolderRecord,
	expected core.HolderRecord,
	initial bool,
	anomaly error,
) (bool, bool, error) {
	reconcileCtx, cancel := a.newHeartbeatReconcileContext()
	durable, found, getErr := a.registryGet(reconcileCtx, expected.Domain, expected.HolderID)
	cancel()
	if getErr == nil && found {
		getErr = a.validateRegistryGetRecord(durable)
	}
	if getErr == nil && found && core.HolderRecordEqual(durable, expected) {
		installed, closed := a.installHeartbeatCandidate(expected, initial, false, anomaly)
		return installed, closed, nil
	}
	if getErr == nil && found && core.HolderRecordEqual(durable, previous) {
		if !initial {
			a.recordRuntimeFailure(anomaly)
		}
		return false, false, nil
	}
	reconcileErr := getErr
	if reconcileErr == nil {
		if !found {
			reconcileErr = fmt.Errorf("durable heartbeat record missing")
		} else {
			reconcileErr = fmt.Errorf("durable heartbeat differs from previous and candidate")
		}
	}
	uncertainErr := fmt.Errorf(
		"%w: uncertain heartbeat: rpc=%v reconcile=%v", ErrAgentUnhealthy, anomaly, reconcileErr,
	)
	a.markUnhealthy(uncertainErr)
	if errors.Is(getErr, context.DeadlineExceeded) {
		a.mu.Lock()
		closeDeadlineReached := a.closeDeadlineReachedLocked()
		a.mu.Unlock()
		if closeDeadlineReached {
			return false, false, agentCloseTimeoutError("heartbeat reconcile", getErr)
		}
	}
	return false, false, uncertainErr
}

func (a *HolderAgent) validateRegistryGetRecord(record core.HolderRecord) error {
	registryOptions := core.DefaultHolderRegistryOptions()
	registryOptions.LeaseTTL = a.opts.LeaseTTL
	registryOptions.MinLeaseTTL = time.Millisecond
	if registryOptions.MaxLeaseTTL < a.opts.LeaseTTL {
		registryOptions.MaxLeaseTTL = a.opts.LeaseTTL
	}
	now := time.UnixMilli(a.opts.Now().UnixMilli())
	if err := core.ValidateHolderRecord(record, now, registryOptions); err != nil {
		return fmt.Errorf("%w: invalid Registry.Get response: %v", ErrAgentInvalid, err)
	}
	return nil
}

func (a *HolderAgent) Close(ctx context.Context) error {
	if a == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	a.mu.Lock()
	if a.lifecycle == agentLifecycleClosed {
		err := a.closeErr
		a.mu.Unlock()
		return err
	}
	if a.lifecycle == agentLifecycleNew {
		a.closeRequested = true
		a.lifecycle = agentLifecycleClosed
		a.state = AgentStateClosed
		a.ensureCloseDoneLocked()
		a.closeComplete = true
		close(a.closeDone)
		a.mu.Unlock()
		return nil
	}
	a.closeRequested = true
	if a.lifecycle == agentLifecycleStarted {
		a.lifecycle = agentLifecycleClosing
		if a.state != AgentStateUnhealthy {
			a.state = AgentStateDraining
		}
	}
	closeDone := a.ensureCloseDoneLocked()
	startWorker := a.startCloseWorkerLocked()
	a.mu.Unlock()
	if startWorker {
		go a.runCloseWorker(nil)
	}
	select {
	case <-closeDone:
		a.mu.Lock()
		err := a.closeErr
		a.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *HolderAgent) ensureCloseDoneLocked() chan struct{} {
	if a.closeDone == nil {
		a.closeDone = make(chan struct{})
	}
	return a.closeDone
}

func (a *HolderAgent) startCloseWorkerLocked() bool {
	if a.closeWorkerStarted || a.closeComplete {
		return false
	}
	if a.closeDeadline.IsZero() {
		// time.Time.Add performs saturating seconds arithmetic and strips an
		// overflowing monotonic reading, so even the largest positive Duration
		// cannot wrap this deadline into the past.
		a.closeDeadline = time.Now().Add(a.opts.CloseTimeout)
	}
	a.closeWorkerStarted = true
	return true
}

// runCloseWorker is the unique cleanup owner. It never inherits a Close
// caller's context: a caller may stop waiting while the worker spends its one
// CloseTimeout budget waiting for startup and, if startup converges in time,
// cleaning the durable candidate.
func (a *HolderAgent) runCloseWorker(startErr error) {
	a.mu.Lock()
	closeDeadline := a.closeDeadline
	attempt := a.startAttempt
	starting := a.lifecycle == agentLifecycleStarting
	a.mu.Unlock()
	closeCtx, cancelClose := context.WithDeadline(context.Background(), closeDeadline)
	defer cancelClose()
	if starting && attempt != nil {
		select {
		case <-attempt.done:
		case <-closeCtx.Done():
			a.completeStart(attempt, ErrAgentClosed)
			a.finishClose(agentCloseTimeoutError("start wait", closeCtx.Err()))
			return
		}
	}

	a.mu.Lock()
	if a.closeComplete {
		a.mu.Unlock()
		return
	}
	if a.lifecycle == agentLifecycleStarted || a.lifecycle == agentLifecycleStarting {
		a.lifecycle = agentLifecycleClosing
	}
	if a.record.HolderID == "" {
		a.mu.Unlock()
		a.finishClose(nil)
		return
	}
	unhealthyErr := error(nil)
	if a.state == AgentStateUnhealthy {
		unhealthyErr = a.unhealthyErrorLocked()
	} else {
		a.state = AgentStateDraining
	}
	cancel := a.cancel
	done := a.done
	a.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-closeCtx.Done():
			err := agentCloseTimeoutError("loop wait", closeCtx.Err())
			if unhealthyErr != nil {
				err = fmt.Errorf("%w: %v", unhealthyErr, err)
			}
			a.finishClose(err)
			return
		}
	}
	if unhealthyErr != nil {
		a.finishClose(unhealthyErr)
		return
	}
	cleanupErr := a.drainAndMarkClosed(closeCtx)
	if cleanupErr != nil && startErr != nil {
		cleanupErr = fmt.Errorf("%w: startup=%v cleanup=%v", ErrAgentUnhealthy, startErr, cleanupErr)
	}
	a.finishClose(cleanupErr)
}

func (a *HolderAgent) drainAndMarkClosed(ctx context.Context) error {
	if err := agentContextError(ctx); err != nil {
		return agentCloseTimeoutError("drain admission", err)
	}
	a.mu.Lock()
	if a.state == AgentStateUnhealthy || a.state == AgentStateClosed {
		err := a.unhealthyErrorLocked()
		a.mu.Unlock()
		return err
	}
	a.mu.Unlock()
	draining, err := a.nextCloseHeartbeat(core.HolderStateDraining, ProcessStatus{})
	if err != nil {
		return err
	}
	if err := a.publishCloseHeartbeat(ctx, draining); err != nil {
		return err
	}

	ticker := time.NewTicker(a.opts.ClosePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return agentCloseTimeoutError("drain poll", ctx.Err())
		case <-ticker.C:
		}
		statusCtx, cancelStatus := context.WithTimeout(ctx, a.opts.HeartbeatTimeout)
		status, statusErr := boundedAgentCall(statusCtx, a.opts.ProcessStatus.Status)
		cancelStatus()
		if statusErr != nil {
			if err := agentContextError(ctx); err != nil {
				return agentCloseTimeoutError("process status", err)
			}
			return statusErr
		}
		if status.ServingShardCount == 0 {
			drained, err := a.nextCloseHeartbeat(core.HolderStateDrained, status)
			if err != nil {
				return err
			}
			if err := a.publishCloseHeartbeat(ctx, drained); err != nil {
				return err
			}
			return nil
		}
		renewal, err := a.nextCloseHeartbeat(core.HolderStateDraining, status)
		if err != nil {
			return err
		}
		if err := a.publishCloseHeartbeat(ctx, renewal); err != nil {
			return err
		}
	}
}

func (a *HolderAgent) nextCloseHeartbeat(state core.HolderState, status ProcessStatus) (core.HolderHeartbeat, error) {
	heartbeatAt := a.opts.Now()
	a.mu.Lock()
	previous := cloneRecord(a.record)
	sequence, err := nextAgentSequence(previous.Sequence)
	if err != nil {
		a.mu.Unlock()
		return core.HolderHeartbeat{}, err
	}
	if heartbeatAt.Before(previous.HeartbeatAt) {
		heartbeatAt = previous.HeartbeatAt
	}
	leaseUntil := heartbeatAt.Add(a.opts.LeaseTTL)
	if leaseUntil.Before(previous.LeaseUntil) {
		leaseUntil = previous.LeaseUntil
	}
	heartbeat := core.HolderHeartbeat{
		Domain: a.opts.Domain, HolderID: a.opts.HolderID, BootID: a.bootID,
		State: state, ProcessReady: false, Draining: true,
		ServingShardCount: previous.ServingShardCount,
		SnapshotRevision:  maxUint64(status.SnapshotRevision, previous.SnapshotRevision),
		HeartbeatAt:       heartbeatAt, LeaseUntil: leaseUntil, Sequence: sequence,
		CapacityWeight:  pickWeight(status.CapacityWeight, previous.CapacityWeight),
		ResourceSummary: pickSummary(status.ResourceSummary, previous.ResourceSummary),
	}
	if state == core.HolderStateDraining && status.ServingShardCount != 0 {
		heartbeat.ServingShardCount = status.ServingShardCount
		heartbeat.ProcessReady = status.ProcessReady
	}
	if state == core.HolderStateDrained {
		heartbeat.ServingShardCount = 0
	}
	a.mu.Unlock()
	return heartbeat, nil
}

func (a *HolderAgent) publishCloseHeartbeat(ctx context.Context, heartbeat core.HolderHeartbeat) error {
	if err := agentContextError(ctx); err != nil {
		return agentCloseTimeoutError("heartbeat admission", err)
	}
	a.mu.Lock()
	previous := cloneRecord(a.record)
	a.mu.Unlock()
	expected := holderRecordAfterHeartbeat(previous, heartbeat)
	callCtx, cancelCall := context.WithTimeout(ctx, a.opts.HeartbeatTimeout)
	updated, err := a.registryHeartbeat(callCtx, heartbeat)
	cancelCall()
	if err == nil && core.HolderRecordEqual(updated, expected) {
		installed, _ := a.installHeartbeatCandidate(expected, false, true, nil)
		if !installed {
			return agentCloseTimeoutError("heartbeat result", context.DeadlineExceeded)
		}
		return nil
	}
	if closeErr := agentContextError(ctx); closeErr != nil {
		return agentCloseTimeoutError("heartbeat", closeErr)
	}
	anomaly := err
	if anomaly == nil {
		anomaly = fmt.Errorf("%w: close heartbeat response differs from candidate", ErrAgentInvalid)
	}
	committed, _, reconcileErr := a.reconcileHeartbeatCandidate(previous, expected, false, anomaly)
	if errors.Is(reconcileErr, ErrAgentCloseTimeout) {
		return reconcileErr
	}
	if closeErr := agentContextError(ctx); closeErr != nil {
		return agentCloseTimeoutError("heartbeat reconcile", closeErr)
	}
	if committed {
		a.mu.Lock()
		unhealthy := a.state == AgentStateUnhealthy
		unhealthyErr := a.unhealthyErrorLocked()
		a.mu.Unlock()
		if unhealthy {
			return unhealthyErr
		}
		return nil
	}
	return anomaly
}

func agentCloseTimeoutError(operation string, err error) error {
	return fmt.Errorf("%w: %s: %v", ErrAgentCloseTimeout, operation, err)
}

func (a *HolderAgent) finishClose(err error) {
	a.mu.Lock()
	if a.closeComplete {
		a.mu.Unlock()
		return
	}
	if a.state == AgentStateUnhealthy {
		if err == nil {
			err = a.unhealthyErrorLocked()
		} else if !errors.Is(err, ErrAgentUnhealthy) {
			err = fmt.Errorf("%w: %w", ErrAgentUnhealthy, err)
		}
	} else if err != nil {
		if !errors.Is(err, ErrAgentUnhealthy) {
			err = fmt.Errorf("%w: %w", ErrAgentUnhealthy, err)
		}
		a.state = AgentStateUnhealthy
		a.lastErr = err
		a.failures = a.opts.ConsecutiveFailureThreshold
	}
	if err != nil && a.closeErr == nil {
		a.closeErr = err
	}
	if err == nil && a.state != AgentStateUnhealthy {
		if a.record.HolderID == "" {
			a.state = AgentStateClosed
		} else {
			a.state = AgentStateDrained
		}
	}
	a.lifecycle = agentLifecycleClosed
	closeDone := a.closeDone
	if closeDone == nil {
		closeDone = make(chan struct{})
		a.closeDone = closeDone
	}
	a.closeComplete = true
	close(closeDone)
	a.mu.Unlock()
}

func (a *HolderAgent) unhealthyErrorLocked() error {
	if a.lastErr == nil {
		return ErrAgentUnhealthy
	}
	if errors.Is(a.lastErr, ErrAgentUnhealthy) {
		return a.lastErr
	}
	return fmt.Errorf("%w: %v", ErrAgentUnhealthy, a.lastErr)
}

func (a *HolderAgent) finishRun() {
	a.mu.Lock()
	a.closeRunDoneLocked()
	a.mu.Unlock()
}

func (a *HolderAgent) closeRunDoneLocked() {
	if a.done != nil && !a.doneClosed {
		close(a.done)
		a.doneClosed = true
	}
}

func (a *HolderAgent) recordRuntimeFailure(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closeRequested || a.lifecycle != agentLifecycleStarted ||
		a.state == AgentStateUnhealthy || a.state == AgentStateDrained || a.state == AgentStateClosed {
		return
	}
	a.recordFailureLocked(err)
}

func (a *HolderAgent) recordFailureLocked(err error) {
	a.failures++
	a.lastErr = err
	if a.failures >= a.opts.ConsecutiveFailureThreshold &&
		a.state != AgentStateDrained && a.state != AgentStateClosed {
		a.state = AgentStateUnhealthy
		if a.cancel != nil {
			a.cancel()
		}
	}
}

func (a *HolderAgent) markUnhealthy(err error) {
	a.mu.Lock()
	a.state = AgentStateUnhealthy
	a.lastErr = err
	a.failures = a.opts.ConsecutiveFailureThreshold
	if a.cancel != nil {
		a.cancel()
	}
	a.mu.Unlock()
}

func nextAgentSequence(previous uint64) (uint64, error) {
	if previous == ^uint64(0) {
		return 0, fmt.Errorf("%w: sequence overflow", ErrAgentInvalid)
	}
	return previous + 1, nil
}

func (a *HolderAgent) Status() AgentStatus {
	if a == nil {
		return AgentStatus{State: AgentStateClosed, Unhealthy: true, LastError: ErrAgentInvalid}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return AgentStatus{
		State: a.state, Holder: cloneRecord(a.record),
		ConsecutiveFailures: a.failures, Unhealthy: a.state == AgentStateUnhealthy,
		LastError: a.lastErr, HeartbeatInFlight: a.inFlight.Load(),
	}
}

func serverIsReady(opts HolderAgentOptions) bool {
	if opts.ServerReady != nil {
		return opts.ServerReady()
	}
	if opts.ServerStarted != nil {
		return opts.ServerStarted()
	}
	return true
}

func deriveHolderState(status ProcessStatus, previous core.HolderState) (core.HolderState, error) {
	if status.Draining {
		if status.ServingShardCount != 0 && previous == core.HolderStateWarming {
			return "", fmt.Errorf("%w: warming holder cannot drain with serving shards", ErrAgentInvalid)
		}
		return core.HolderStateDraining, nil
	}
	if !status.ProcessReady {
		if status.ServingShardCount != 0 {
			if previous == core.HolderStateServing {
				return previous, errHeartbeatDeferred
			}
			return "", fmt.Errorf("%w: process unready while serving", ErrAgentUnhealthy)
		}
		if previous == core.HolderStateWarming {
			return core.HolderStateWarming, nil
		}
		if previous == core.HolderStateReadyZeroShard {
			return previous, errHeartbeatDeferred
		}
		if previous == core.HolderStateServing {
			// Last shard already left, but process readiness has not caught up.
			// Redis rejects both SERVING with 0 shards and READY_ZERO_SHARD
			// while unready; keep the durable SERVING lease until the next
			// ready zero-shard heartbeat.
			return previous, errHeartbeatDeferred
		}
		return "", fmt.Errorf("%w: process readiness regressed", ErrAgentUnhealthy)
	}
	if status.ServingShardCount == 0 {
		return core.HolderStateReadyZeroShard, nil
	}
	return core.HolderStateServing, nil
}

func pickWeight(value, fallback uint32) uint32 {
	if value != 0 {
		return value
	}
	return fallback
}

func pickSummary(value, fallback map[string]string) map[string]string {
	if value == nil {
		return cloneResourceSummary(fallback)
	}
	return cloneResourceSummary(value)
}

func cloneResourceSummary(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func cloneRecord(input core.HolderRecord) core.HolderRecord {
	input.ResourceSummary = cloneResourceSummary(input.ResourceSummary)
	return input
}

func maxUint64(left, right uint64) uint64 {
	if left > right {
		return left
	}
	return right
}
