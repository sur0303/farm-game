package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	holdercontrolpb "farmgame/api/gen/holdercontrol"
	routecontrolpb "farmgame/api/gen/routecontrol"
	routeplanpb "farmgame/api/gen/routeplan"
	scaleplanpb "farmgame/api/gen/scaleplan"
	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/holdercontrol"
	"farmgame/internal/netendpoint"
	core "farmgame/internal/routecontrol"
	"farmgame/internal/routecontrol/gateverification"
	"farmgame/internal/routecontrol/transport"
	"farmgame/internal/routeplan"
	scaleplan "farmgame/internal/scaleplan"
	"farmgame/internal/storage/authoritystore"
	redisauthority "farmgame/internal/storage/redis/authority"
	"farmgame/internal/storage/redis/rediskey"
	redisroutecontrol "farmgame/internal/storage/redis/routecontrol"
	redisrouteplan "farmgame/internal/storage/redis/routeplan"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

type planRuntime struct {
	Executor               *routeplan.PlanExecutor
	Service                *routeplan.PlanService
	Authorizer             *transport.MethodAuthorizer
	ServerAuthorizer       *transport.MethodAuthorizer
	Registry               *core.HolderRegistry
	HolderReader           *switchableRoutePlanHolderReader
	Recovery               *pendingProjectionRecoveryOwner
	Verification           *routeplan.ProductionVerification
	LeaseProjection        *leaseProjectionRuntime
	Publisher              *core.DurableSnapshotPublisher
	InitialPublisher       *core.InitialAuthorityBatchPublisher
	Manager                *authority.RouteAssignmentManager
	SnapshotStore          core.RouteSnapshotStore
	AuthorityReader        *redisauthority.AuthorityReader
	TransitionStore        *redisroutecontrol.TransitionStore
	InitialTransitionStore *authority.SwitchableTransitionStore
	RoutePlanStore         routeplan.PlanStore
	recoveryCancel         context.CancelFunc
	recoveryDone           chan struct{}
	recoveryInterval       time.Duration
	routePlanRecovery      func(context.Context) error
	deadlineRefresh        func(context.Context) error
	routePlanWake          chan struct{}
	startupReady           *atomic.Bool
	ControlMutation        routeplan.ControlMutationOptions
	MetricsRegisterer      prometheus.Registerer
	HolderControlPool      *holderControlClientPool
}

func (r *planRuntime) Close() {
	if r == nil {
		return
	}
	if r.recoveryCancel != nil {
		r.recoveryCancel()
	}
	if r.recoveryDone != nil {
		<-r.recoveryDone
	}
	if r.LeaseProjection != nil {
		r.LeaseProjection.Close()
	}
	if r.Verification != nil {
		_ = r.Verification.Close()
	}
	if r.HolderControlPool != nil {
		_ = r.HolderControlPool.Close()
	}
	if r.Registry != nil {
		_ = r.Registry.Close()
	}
}

func (r *planRuntime) BootstrapLeaseProjection(ctx context.Context) error {
	if r == nil || r.LeaseProjection == nil || ctx == nil {
		return errors.New("route-controller: lease projection runtime unavailable")
	}
	return r.LeaseProjection.Bootstrap(ctx)
}

func (r *planRuntime) StartLeaseProjection(parent context.Context) error {
	if r == nil || r.LeaseProjection == nil {
		return errors.New("route-controller: lease projection runtime unavailable")
	}
	return r.LeaseProjection.Start(parent)
}

func (r *planRuntime) StartupReady() bool {
	return r != nil && r.startupReady != nil && r.startupReady.Load()
}

func (r *planRuntime) MarkStartupReady() {
	if r != nil && r.startupReady != nil {
		r.startupReady.Store(true)
	}
}

// WakeRoutePlanRecovery coalesces child RoutePlan work created by ScalePlan.
// The child is executed by the background owner after the parent ScalePlan
// releases its control mutation lease; it is never executed inline while the
// parent is still inside EnsureMove.
func (r *planRuntime) WakeRoutePlanRecovery() {
	if r == nil || r.routePlanWake == nil {
		return
	}
	select {
	case r.routePlanWake <- struct{}{}:
	default:
	}
}

func (r *planRuntime) recoverRuntimePlans(ctx context.Context) {
	if r == nil || ctx == nil {
		return
	}
	// RoutePlan recovery owns complete Authority waves and must reconstruct and
	// seal their shared stage before generic Manager recovery sees individual
	// pending projections. Reversing this order can publish one ACTIVE member of
	// a wave independently or fail startup on a valid partially buffered stage.
	if r.routePlanRecovery != nil {
		if err := r.routePlanRecovery(ctx); err != nil && ctx.Err() == nil {
			log.Printf("route-controller RoutePlan runtime recovery pass success=false err=%v", err)
			return
		}
	}
	if r.Recovery != nil {
		if err := r.Recovery.Recover(ctx); err != nil && ctx.Err() == nil {
			log.Printf("route-controller deadline refresh/recovery pass success=false err=%v", err)
		}
	}
}

func recoverRoutePlansUntilReady(ctx context.Context, recover func(context.Context) error) error {
	if ctx == nil || recover == nil {
		return routeplan.ErrExecutorInvalid
	}
	var lastErr error
	for attempt := 1; ; attempt++ {
		if err := recover(ctx); err == nil {
			return nil
		} else {
			lastErr = err
			if attempt == 1 {
				log.Printf("route-controller initial RoutePlan recovery retrying err=%v", err)
			}
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return errors.Join(lastErr, ctx.Err())
		case <-timer.C:
		}
	}
}

func (r *planRuntime) StartRecovery(parent context.Context) {
	if r == nil || (r.Recovery == nil && r.routePlanRecovery == nil && r.deadlineRefresh == nil) || parent == nil || r.recoveryCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	r.recoveryCancel = cancel
	r.recoveryDone = make(chan struct{})
	go func() {
		defer close(r.recoveryDone)
		interval := r.recoveryInterval
		if interval <= 0 {
			interval = defaultDeadlineRefreshInterval
		}
		var workers sync.WaitGroup
		workers.Add(1)
		go func() {
			defer workers.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					r.recoverRuntimePlans(ctx)
				case <-r.routePlanWake:
					r.recoverRuntimePlans(ctx)
				}
			}
		}()
		if r.deadlineRefresh != nil {
			workers.Add(1)
			go func() {
				defer workers.Done()
				ticker := time.NewTicker(interval)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						if err := r.deadlineRefresh(ctx); err != nil && ctx.Err() == nil {
							log.Printf("route-controller independent deadline refresh success=false err=%v", err)
						}
					}
				}
			}()
		}
		workers.Wait()
	}()
}

func routeControllerControlOperationID(kind string, cfg config) string {
	digest := sha256.Sum256([]byte(cfg.controllerID + "\x00" + cfg.boot))
	return fmt.Sprintf("route-controller/%s/%x", kind, digest[:])
}

// pendingProjectionRecoveryOwner is the process-local single owner shared by
// background recovery and every Plan submission/execution. A successful Enter
// proves all Manager durable projections are recovered before the guarded Plan
// can proceed; the token remains held until Plan execution returns.
type pendingProjectionRecoveryOwner struct {
	manager          *authority.RouteAssignmentManager
	targetValidator  authority.PendingProjectionTargetValidator
	drainFactory     authority.PendingDrainControllerFactory
	timeout          time.Duration
	refreshLead      time.Duration
	metrics          *routeDeadlineMetrics
	control          routeplan.ControlMutationOptions
	recoverySequence atomic.Uint64
	gateMu           sync.Mutex
	gateActive       int
	gateRecovering   bool
	gateChanged      chan struct{}
	// deadlineGate serializes the expensive Authority deadline read/revalidate/
	// publish wave. Recovery and the independent refresh loop must remain
	// independently schedulable, but they must never race the same Snapshot
	// projection against each other.
	deadlineGate               chan struct{}
	independentDeadlineRefresh bool
	deadlineWaiters            atomic.Int32
	deadlinePending            atomic.Bool
	maxConcurrent              int
}

func newPendingProjectionRecoveryOwner(manager *authority.RouteAssignmentManager, validator authority.PendingProjectionTargetValidator, factory authority.PendingDrainControllerFactory, timeout, refreshLead time.Duration, metrics ...*routeDeadlineMetrics) (*pendingProjectionRecoveryOwner, error) {
	if manager == nil || validator == nil || timeout <= 0 || timeout > maxPlanOperationTimeout || refreshLead < time.Millisecond || refreshLead > maxDeadlineRefreshLead {
		return nil, errors.New("route-controller: invalid pending projection recovery owner")
	}
	var observer *routeDeadlineMetrics
	if len(metrics) > 1 {
		return nil, errors.New("route-controller: one deadline metrics observer allowed")
	}
	if len(metrics) == 1 {
		observer = metrics[0]
	}
	return &pendingProjectionRecoveryOwner{
		manager: manager, targetValidator: validator, drainFactory: factory,
		timeout: timeout, refreshLead: refreshLead, metrics: observer,
		gateChanged: make(chan struct{}), deadlineGate: make(chan struct{}, 1),
		maxConcurrent: int(scaleplan.ProductionMaxRoutePlansInflight),
	}, nil
}

func newPendingProjectionRecoveryOwnerWithControl(manager *authority.RouteAssignmentManager, validator authority.PendingProjectionTargetValidator, factory authority.PendingDrainControllerFactory, timeout, refreshLead time.Duration, control routeplan.ControlMutationOptions, metrics ...*routeDeadlineMetrics) (*pendingProjectionRecoveryOwner, error) {
	if err := control.Validate(); err != nil {
		return nil, err
	}
	owner, err := newPendingProjectionRecoveryOwner(manager, validator, factory, timeout, refreshLead, metrics...)
	if err != nil {
		return nil, err
	}
	owner.control = control
	return owner, nil
}

// Enter performs one exclusive recovery before opening a bounded shared
// execution interval. Later callers join that interval instead of serializing
// complete RoutePlans behind the recovery owner. Background projection
// recovery cannot enter until every shared execution releases its slot.
func (o *pendingProjectionRecoveryOwner) Enter(ctx context.Context) (func(), error) {
	if o == nil || o.manager == nil || ctx == nil {
		return nil, routeplan.ErrExecutorInvalid
	}
	waitStarted := time.Now()
	leader, err := o.enterExecutionGate(ctx)
	if err != nil {
		return nil, err
	}
	if leader {
		err = o.runRecoveryPass(ctx)
		o.gateMu.Lock()
		o.gateRecovering = false
		if err == nil {
			o.gateActive = 1
		}
		o.signalGateLocked()
		o.gateMu.Unlock()
		if err != nil {
			return nil, err
		}
	}
	if waited := time.Since(waitStarted); waited >= time.Second {
		log.Printf("route-controller pending recovery gate wait_ms=%d", waited.Milliseconds())
	}
	var once sync.Once
	return func() { once.Do(o.releaseExecutionGate) }, nil
}

// EnterAuthorityWave gives PlanExecutor one exclusive process-local recovery
// window without first running generic Manager projection recovery. The wave
// owns those individual pending projections and must publish them as one stage
// CAS. Ordinary Enter and background Recover calls remain blocked until the
// returned release function runs.
func (o *pendingProjectionRecoveryOwner) EnterAuthorityWave(ctx context.Context) (func(), error) {
	if o == nil || o.manager == nil || ctx == nil {
		return nil, routeplan.ErrExecutorInvalid
	}
	for {
		o.gateMu.Lock()
		if !o.gateRecovering && o.gateActive == 0 {
			o.gateRecovering = true
			o.gateMu.Unlock()
			break
		}
		changed := o.gateChanged
		o.gateMu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			o.gateMu.Lock()
			o.gateRecovering = false
			o.signalGateLocked()
			o.gateMu.Unlock()
		})
	}, nil
}

func (o *pendingProjectionRecoveryOwner) enterExecutionGate(ctx context.Context) (bool, error) {
	for {
		o.gateMu.Lock()
		if !o.gateRecovering && o.gateActive > 0 && o.gateActive < o.maxConcurrent {
			o.gateActive++
			o.gateMu.Unlock()
			return false, nil
		}
		if !o.gateRecovering && o.gateActive == 0 {
			o.gateRecovering = true
			o.gateMu.Unlock()
			return true, nil
		}
		changed := o.gateChanged
		o.gateMu.Unlock()
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-changed:
		}
	}
}

func (o *pendingProjectionRecoveryOwner) releaseExecutionGate() {
	o.gateMu.Lock()
	if o.gateActive > 0 {
		o.gateActive--
		o.signalGateLocked()
	}
	o.gateMu.Unlock()
}

func (o *pendingProjectionRecoveryOwner) signalGateLocked() {
	if o.gateChanged != nil {
		close(o.gateChanged)
	}
	o.gateChanged = make(chan struct{})
}

func (o *pendingProjectionRecoveryOwner) runRecoveryPass(ctx context.Context) error {
	recoveryCtx, cancel := context.WithTimeout(ctx, o.timeout)
	controlCtx, resolveControl, err := o.control.Begin(recoveryCtx, fmt.Sprintf("route-controller/recovery/%d", o.recoverySequence.Add(1)), scaleplan.ControlOperationRecovery)
	if err != nil {
		cancel()
		return errors.Join(routeplan.ErrVerification, err)
	}
	recoveryCtx = controlCtx
	recoveryStarted := time.Now()
	var deadlineResult authority.DeadlineProjectionResult
	for attempt := 1; ; attempt++ {
		err = o.manager.RecoverPendingProjections(recoveryCtx, o.targetValidator, o.drainFactory)
		deadlineResult = authority.DeadlineProjectionResult{}
		deadlineAttempted := false
		// The standalone deadline owner remains schedulable while RoutePlan
		// recovery blocks. Once enabled, generic recovery must not repeat the
		// same expensive 2048-shard Authority read for the same Snapshot.
		if err == nil && !o.independentDeadlineRefresh {
			deadlineAttempted = true
			releaseDeadline, gateErr := o.acquireDeadlineGate(recoveryCtx)
			if gateErr != nil {
				err = gateErr
			} else {
				deadlineResult, err = o.manager.ReconcileRenewedAuthorityDeadlines(recoveryCtx, o.refreshLead)
				releaseDeadline()
			}
		}
		if deadlineAttempted {
			o.metrics.observe(deadlineResult, err)
		}
		if err == nil {
			break
		}
		// A deadline pass can lose its snapshot race after all reads but before
		// publication. No durable mutation was made in that case, so retaining
		// the top-level control token until the 30-minute plan timeout would
		// poison the process and make every later refresh fail closed. Release
		// only these explicitly known no-write outcomes; publish/transport
		// ambiguity remains unresolved and fail-closed.
		if deadlineAttempted && deadlineProjectionControlCanResolve(deadlineResult) {
			if resolveControl != nil {
				if resolveErr := resolveControl(); resolveErr != nil {
					cancel()
					return errors.Join(routeplan.ErrVerification, err, resolveErr)
				}
			}
			cancel()
			return errors.Join(routeplan.ErrVerification, err)
		}
		if attempt == 1 {
			log.Printf("route-controller pending recovery retrying err=%v", err)
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-recoveryCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			err = errors.Join(err, recoveryCtx.Err())
			goto recoveryDone
		case <-timer.C:
		}
	}

recoveryDone:
	if elapsed := time.Since(recoveryStarted); elapsed >= time.Second || deadlineResult.Changed || deadlineResult.StalledShards > 0 {
		log.Printf("route-controller recovery/deadline pass duration_ms=%d success=%t deadline_changed=%t changed_shards=%d stalled_shards=%d snapshot_revision=%d", elapsed.Milliseconds(), err == nil, deadlineResult.Changed, deadlineResult.ChangedShards, deadlineResult.StalledShards, deadlineResult.SnapshotRevision)
	}
	cancel()
	if err != nil {
		return errors.Join(routeplan.ErrVerification, err)
	}
	if resolveControl != nil {
		if resolveErr := resolveControl(); resolveErr != nil {
			return errors.Join(routeplan.ErrVerification, resolveErr)
		}
	}
	return nil
}

// deadlineProjectionControlCanResolve uses the Manager's positive write-boundary
// proof instead of inferring safety from an error string. Cancellation while
// reading thousands of Authorities is a known no-write outcome and must release
// its token; otherwise that abandoned token expires later and permanently
// poisons the in-process coordinator. Once publication was attempted, only the
// Manager-confirmed Changed result is safe to resolve. Ambiguity remains
// fail-closed.
func deadlineProjectionControlCanResolve(result authority.DeadlineProjectionResult) bool {
	return result.Changed || !result.PublicationAttempted
}

// RefreshDeadlines runs independently of RoutePlan recovery. A sliding window
// may keep RoutePlan execution continuously active for many lease periods, so
// deadline projection cannot be sequenced after that recovery returns. The
// Manager's exclusive operation lock gives this short maintenance writer
// preference over newly arriving per-shard transitions while allowing every
// already-running transition to leave a crash-safe checkpoint first.
func (o *pendingProjectionRecoveryOwner) acquireDeadlineGate(ctx context.Context) (func(), error) {
	if o == nil || ctx == nil || o.deadlineGate == nil {
		return nil, routeplan.ErrExecutorInvalid
	}
	select {
	case o.deadlineGate <- struct{}{}:
		return func() { <-o.deadlineGate }, nil
	default:
		// A refresh/recovery request arriving during an active wave is a
		// trailing request, not a reason to drop the event. The waiter will
		// acquire the next slot and run against the latest Manager snapshot.
		o.deadlineWaiters.Add(1)
		o.deadlinePending.Store(true)
		defer func() {
			if o.deadlineWaiters.Add(-1) == 0 {
				o.deadlinePending.Store(false)
			}
		}()
		select {
		case o.deadlineGate <- struct{}{}:
			return func() { <-o.deadlineGate }, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (o *pendingProjectionRecoveryOwner) DeadlineRefreshPending() bool {
	return o != nil && o.deadlinePending.Load()
}

func (o *pendingProjectionRecoveryOwner) RefreshDeadlines(ctx context.Context) error {
	if o == nil || o.manager == nil || ctx == nil {
		return routeplan.ErrExecutorInvalid
	}
	const maximumPass = 30 * time.Second
	timeout := o.timeout
	if timeout <= 0 || timeout > maximumPass {
		timeout = maximumPass
	}
	refreshCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	// Acquire before opening the control token so a refresh waiting behind the
	// recovery pass does not hold a second mutation token while it waits.
	releaseDeadline, err := o.acquireDeadlineGate(refreshCtx)
	if err != nil {
		return err
	}
	defer releaseDeadline()
	controlCtx, resolveControl, err := o.control.Begin(
		refreshCtx,
		fmt.Sprintf("route-controller/deadline-refresh/%d", o.recoverySequence.Add(1)),
		scaleplan.ControlOperationRecovery,
	)
	if err != nil {
		return errors.Join(routeplan.ErrVerification, err)
	}
	started := time.Now()
	result, err := o.manager.ReconcileRenewedAuthorityDeadlines(controlCtx, o.refreshLead)
	o.metrics.observe(result, err)
	if elapsed := time.Since(started); elapsed >= time.Second || result.Changed || result.StalledShards > 0 {
		log.Printf("route-controller independent deadline refresh duration_ms=%d success=%t deadline_changed=%t changed_shards=%d stalled_shards=%d snapshot_revision=%d", elapsed.Milliseconds(), err == nil, result.Changed, result.ChangedShards, result.StalledShards, result.SnapshotRevision)
	}
	if err != nil {
		if resolveControl != nil && deadlineProjectionControlCanResolve(result) {
			if resolveErr := resolveControl(); resolveErr != nil {
				return errors.Join(routeplan.ErrVerification, err, resolveErr)
			}
		}
		return errors.Join(routeplan.ErrVerification, err)
	}
	if resolveControl != nil {
		if resolveErr := resolveControl(); resolveErr != nil {
			return errors.Join(routeplan.ErrVerification, resolveErr)
		}
	}
	return nil
}

func (o *pendingProjectionRecoveryOwner) Recover(ctx context.Context) error {
	if o == nil || o.manager == nil || ctx == nil {
		return routeplan.ErrExecutorInvalid
	}
	for {
		o.gateMu.Lock()
		if !o.gateRecovering && o.gateActive == 0 {
			o.gateRecovering = true
			o.gateMu.Unlock()
			break
		}
		changed := o.gateChanged
		o.gateMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
	err := o.runRecoveryPass(ctx)
	o.gateMu.Lock()
	o.gateRecovering = false
	o.signalGateLocked()
	o.gateMu.Unlock()
	return err
}

func newPlanRuntime(ctx context.Context, cfg config, client redis.UniversalClient, keyspaceTuple rediskey.KeyspaceTuple,
	snapshotStore core.RouteSnapshotStore, hub *core.SnapshotWatchHub, stored core.StoredRouteSnapshot, metricsRegisterer prometheus.Registerer) (*planRuntime, error) {
	return newPlanRuntimeWithControl(ctx, cfg, client, keyspaceTuple, snapshotStore, hub, stored, metricsRegisterer, routeplan.ControlMutationOptions{})
}

func newPlanRuntimeWithControl(ctx context.Context, cfg config, client redis.UniversalClient, keyspaceTuple rediskey.KeyspaceTuple,
	snapshotStore core.RouteSnapshotStore, hub *core.SnapshotWatchHub, stored core.StoredRouteSnapshot, metricsRegisterer prometheus.Registerer, control routeplan.ControlMutationOptions) (runtime *planRuntime, returnErr error) {
	if ctx == nil || client == nil || snapshotStore == nil || hub == nil {
		return nil, errors.New("route-controller: invalid plan runtime inputs")
	}
	if err := control.Validate(); err != nil {
		return nil, err
	}
	controlCtx, resolveControl, err := control.Begin(ctx, routeControllerControlOperationID("bootstrap", cfg), scaleplan.ControlOperationBootstrap)
	if err != nil {
		return nil, err
	}
	initialMode, err := initialAssignmentModeFromConfig(cfg)
	if err != nil {
		return nil, err
	}
	ctx = controlCtx
	defer func() {
		if resolveControl != nil && returnErr == nil {
			returnErr = resolveControl()
		}
	}()
	snapshot := stored.Snapshot
	calculator, err := actorrouting.NewCalculator(snapshot.Realm,
		actorrouting.HashProfile{Version: snapshot.HashVersion, Seed: snapshot.HashSeed},
		snapshot.Player.ShardCount, snapshot.Farm.ShardCount)
	if err != nil {
		return nil, err
	}
	profile := keyspaceTuple.Profile()
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	namespace := keyspaceTuple.Namespace()
	authorityLeaseLimits := authoritystore.DefaultLeaseLimits()
	if mode, modeErr := initialAssignmentModeFromConfig(cfg); modeErr == nil && mode == initialAssignmentModeAuto {
		// Only the sealed initial batch requests this extended TTL. Ordinary
		// Holder and RoutePlan paths continue requesting their normal 60s lease.
		authorityLeaseLimits.MaxTTL = initialAssignmentClaimTTL
	}
	authorityStore, err := redisauthority.NewStore(client, nil, redisauthority.Options{
		Profile: profile, Resolver: calculator, LeaseLimits: authorityLeaseLimits,
	})
	if err != nil {
		return nil, err
	}
	readerOptions := redisauthority.DefaultAuthorityReaderOptions(profile, calculator)
	readerOptions.OperationTimeout = cfg.redisOperation
	reader, err := redisauthority.NewShardAuthorityReader(client, readerOptions)
	if err != nil {
		return nil, err
	}
	if err := authorityStore.Registry().EnsureLoaded(ctx); err != nil {
		return nil, err
	}

	transitionOptions := redisroutecontrol.DefaultOptions(namespace)
	transitionOptions.OperationTimeout = cfg.redisOperation
	// A fresh 1024+1024 PLAYER/FARM startup retains one complete Manager
	// recovery record per deterministic claim. The adapter default of 1024
	// physical transition items would fail closed halfway through that one-shot
	// initialization, so size the existing store bound from the immutable route
	// topology plus one normal Manager memo window. FARM_WORKER is included only
	// in this capacity budget; the initial assignment runtime never mutates it.
	transitionItems, capacityErr := routeControllerTransitionStoreItems(snapshot)
	if capacityErr != nil {
		return nil, capacityErr
	}
	transitionOptions.MaxTransitionItems = transitionItems
	transitionStore, err := redisroutecontrol.NewTransitionStore(client, transitionOptions)
	if err != nil {
		return nil, err
	}
	if err := transitionStore.EnsureLoaded(); err != nil {
		return nil, err
	}
	var initialTransitionStore *authority.SwitchableTransitionStore
	transitionEvidence := authority.TransitionTombstoneStore(transitionStore)
	if initialMode == initialAssignmentModeAuto {
		initialTransitionStore, err = authority.NewSwitchableTransitionStore(transitionStore, transitionItems)
		if err != nil {
			return nil, err
		}
		transitionEvidence = initialTransitionStore
	}

	holderOptions := redisroutecontrol.DefaultHolderStoreOptions(namespace)
	holderOptions.OperationTimeout = cfg.redisOperation
	holderStore, err := redisroutecontrol.NewHolderStore(client, holderOptions)
	if err != nil {
		return nil, err
	}
	if err := holderStore.EnsureLoaded(ctx); err != nil {
		return nil, err
	}
	registry, err := core.NewHolderRegistry(holderStore, core.DefaultHolderRegistryOptions())
	if err != nil {
		return nil, err
	}
	holderReader, err := newSwitchableRoutePlanHolderReader(registry)
	if err != nil {
		_ = registry.Close()
		return nil, err
	}

	planStore, err := newConfiguredPlanStore(cfg, client, namespace)
	if err != nil {
		return nil, err
	}
	if err := planStore.EnsureLoaded(ctx); err != nil {
		return nil, err
	}
	publisher, err := core.NewDurableSnapshotPublisher(snapshotStore, hub.Notifier(), hub.AuthorityNotifier())
	if err != nil {
		return nil, err
	}
	if control.Coordinator != nil {
		publisher.RequireControlToken()
	}
	var initialPublisher *core.InitialAuthorityBatchPublisher
	var managerPublisher authority.SnapshotPublisher = publisher
	if initialMode == initialAssignmentModeAuto {
		initialPublisher, err = core.NewInitialAuthorityBatchPublisher(publisher)
		if err != nil {
			return nil, err
		}
		managerPublisher = initialPublisher
	}
	manager, err := authority.NewRouteAssignmentManager(authority.ManagerOptions{
		Store: authorityStore, Publisher: managerPublisher, AuthorityReader: reader, InitialSnapshot: snapshot,
		MemoLimit:              transitionItems,
		AuthorityLayoutVersion: profile.AuthorityLayoutVersion, RouteManagerID: cfg.controllerID,
		RouteManagerBootID: cfg.boot, TombstoneStore: transitionEvidence, PlacementExclusions: registry, InitializationContext: ctx,
		ControlCoordinator: control.Coordinator, ControlScope: control.Scope,
		// The coordinator is inherited by initialization recovery through ctx.
		// Holders heartbeat SnapshotRevision on a multi-second interval after
		// applying Watch N. Bound revalidation before ColdLoad without weakening
		// exact HolderRegistry equality.
		TargetConvergenceTimeout:           targetSnapshotConvergenceTimeout,
		TargetConvergenceBackoff:           50 * time.Millisecond,
		DisableDeadlineSnapshotPublication: cfg.deadlineRefreshMode == deadlineRefreshModeAuthority,
		SnapshotCoalesceWindow:             authority.DefaultSnapshotCoalesceWindow,
	})
	if err != nil {
		_ = registry.Close()
		return nil, err
	}
	var leaseProjection *leaseProjectionRuntime
	if cfg.leaseProjectionMode == leaseProjectionModeAllActive {
		projectionInterval, projectionMinimum, projectionTimeout := effectiveLeaseProjectionConfig(cfg)
		leaseProjection, err = newLeaseProjectionRuntime(manager, projectionInterval, projectionMinimum, projectionTimeout, time.Now, control)
		if err != nil {
			_ = registry.Close()
			return nil, err
		}
	}
	leaseCheckMode := actorrouting.LeaseCheckSnapshot
	if cfg.deadlineRefreshMode == deadlineRefreshModeAuthority {
		leaseCheckMode = actorrouting.LeaseCheckAuthorityDelegated
	}
	gateTLS, err := transport.LoadClientTLSConfig(cfg.gateVerifierClientCert, cfg.gateVerifierClientKey, cfg.gateVerifierClientCA)
	if err != nil {
		_ = registry.Close()
		return nil, err
	}
	gateFleet, err := gateverification.NewClientFleet(cfg.gateVerifierPeers, gateTLS, gateverification.ClientOptions{
		CallTimeout: cfg.gateVerifierTimeout, MaxMessageBytes: cfg.maxMessageBytes,
		LeaseCheckMode: leaseCheckMode,
	})
	if err != nil {
		_ = registry.Close()
		return nil, err
	}
	verification, err := routeplan.NewProductionVerification(routeplan.ProductionVerificationOptions{
		Manager: manager, Gates: gateFleet, CallTimeout: cfg.gateVerifierTimeout, ExpectedPeers: len(cfg.gateVerifierPeers),
		LeaseCheckMode: leaseCheckMode,
	})
	if err != nil || !routeplan.IsProductionVerificationPort(verification) {
		_ = gateFleet.Close()
		_ = registry.Close()
		return nil, errors.New("route-controller: production verification port is required")
	}
	deadlineMetrics := newRouteDeadlineMetrics(metricsRegisterer)
	recoveryOwner, err := newPendingProjectionRecoveryOwnerWithControl(
		manager, newPendingProjectionTargetValidator(holderReader), newRecoveryDrainControllerFactory(cfg, holderReader), cfg.planOperation, effectiveDeadlineRefreshLead(cfg.deadlineRefreshLead), control, deadlineMetrics,
	)
	if err != nil {
		_ = verification.Close()
		_ = registry.Close()
		return nil, err
	}
	// StartRecovery installs a dedicated deadline goroutine below. Generic
	// Manager recovery remains responsible only for durable transition repair.
	recoveryOwner.independentDeadlineRefresh = true
	recoveryOwner.maxConcurrent = effectiveScalePlanParallelism(cfg.scalePlanRoutePlanParallelism)
	authorizer, err := newMethodAuthorizer(cfg.allowedClientIdentity, cfg.planReadIdentity, cfg.operatorIdentity)
	if err != nil {
		_ = verification.Close()
		_ = registry.Close()
		return nil, err
	}
	serverAuthorizer, err := newServerMethodAuthorizer(cfg.allowedClientIdentity, cfg.planReadIdentity, cfg.operatorIdentity)
	if err != nil {
		_ = verification.Close()
		_ = registry.Close()
		return nil, err
	}
	holderControlPool := newHolderControlClientPool(cfg.holderControlMaxClients, newProductionHolderControlDialer(cfg))
	executor, err := newConfiguredPlanExecutor(cfg, routeplan.PlanExecutorOptions{
		Store: planStore, Manager: manager, AuthorityReader: reader, SnapshotStore: snapshotStore,
		HolderReader: holderReader, DrainControllerFactory: newDrainControllerFactory(cfg, holderReader, holderControlPool),
		Now:              time.Now,
		SingleController: routeplan.SingleControllerAttestation{Contract: cfg.singleControllerContract, ControllerID: cfg.controllerID, BootID: cfg.boot},
		ExecutionGuard:   recoveryOwner,
		ControlMutation:  control,
		Verification:     verification,
		LeaseCheckMode:   leaseCheckMode,
	})
	if err != nil {
		_ = holderControlPool.Close()
		_ = verification.Close()
		_ = registry.Close()
		return nil, err
	}
	startupReady := new(atomic.Bool)
	service, err := routeplan.NewPlanService(routeplan.PlanServiceOptions{
		Executor: executor, SnapshotStore: snapshotStore, HolderReader: holderReader, AuthorityReader: reader,
		Authorizer: authorizer, ControllerID: cfg.controllerID, Boot: cfg.boot, Realm: snapshot.Realm,
		Ready: startupReady.Load,
	})
	if err != nil {
		_ = holderControlPool.Close()
		_ = verification.Close()
		_ = registry.Close()
		return nil, err
	}
	return &planRuntime{
		Executor: executor, Service: service, Authorizer: authorizer, ServerAuthorizer: serverAuthorizer,
		Registry: registry, HolderReader: holderReader, Recovery: recoveryOwner, Verification: verification, LeaseProjection: leaseProjection,
		Publisher: publisher, InitialPublisher: initialPublisher,
		Manager: manager, SnapshotStore: snapshotStore, AuthorityReader: reader,
		TransitionStore: transitionStore, InitialTransitionStore: initialTransitionStore, RoutePlanStore: planStore,
		recoveryInterval:  effectiveDeadlineRefreshInterval(cfg.deadlineRefreshInterval),
		routePlanRecovery: executor.Recover, deadlineRefresh: recoveryOwner.RefreshDeadlines,
		routePlanWake: make(chan struct{}, 1), startupReady: startupReady,
		ControlMutation:   control,
		MetricsRegisterer: metricsRegisterer,
		HolderControlPool: holderControlPool,
	}, nil
}

func routeControllerTransitionStoreItems(snapshot actorrouting.RouteSnapshot) (int, error) {
	const (
		transitionMemoWindow = 1024
		maxTransitionItems   = 1 << 20
	)
	total := uint64(snapshot.Player.ShardCount) + uint64(snapshot.Farm.ShardCount) + uint64(snapshot.FarmWorker.ShardCount)
	if total > uint64(maxTransitionItems-transitionMemoWindow) {
		return 0, fmt.Errorf("route-controller: route topology exceeds transition recovery capacity")
	}
	items := int(total) + transitionMemoWindow
	if items < transitionMemoWindow {
		items = transitionMemoWindow
	}
	return items, nil
}

func newConfiguredPlanExecutor(cfg config, options routeplan.PlanExecutorOptions) (*routeplan.PlanExecutor, error) {
	if err := validatePlanTiming(cfg.planOperation, cfg.planTTL); err != nil {
		return nil, err
	}
	options.OperationTimeout = cfg.planOperation
	options.PlanTTL = cfg.planTTL
	return routeplan.NewPlanExecutor(options)
}

func newConfiguredPlanStore(cfg config, client redis.UniversalClient, namespace rediskey.Namespace) (*redisrouteplan.PlanStore, error) {
	if err := validatePlanTiming(cfg.planOperation, cfg.planTTL); err != nil {
		return nil, err
	}
	options := redisrouteplan.DefaultOptions(namespace)
	options.OperationTimeout = cfg.redisOperation
	options.PlanTTL = cfg.planTTL
	// Production RoutePlan audit records must outlive the maximum ScalePlan
	// creation plus recovery horizon (7d + 30d); one extra day is retained as
	// an explicit operational margin. This is independent of child logical TTL.
	options.AuditRetention = productionPlanAuditRetention
	return redisrouteplan.NewPlanStore(client, options)
}

func newMethodAuthorizer(snapshotIdentities, planReadIdentities, operatorIdentities []string) (*transport.MethodAuthorizer, error) {
	if _, err := transport.ValidateAllowedClientIdentities(snapshotIdentities); err != nil {
		return nil, err
	}
	if _, err := transport.ValidateAllowedClientIdentities(planReadIdentities); err != nil {
		return nil, err
	}
	if _, err := transport.ValidateAllowedClientIdentities(operatorIdentities); err != nil {
		return nil, err
	}
	snapshotMethods := []string{
		routecontrolpb.RouteSnapshotService_GetSnapshot_FullMethodName,
		routecontrolpb.RouteSnapshotService_GetCurrentRevision_FullMethodName,
		routecontrolpb.RouteSnapshotService_WatchFrom_FullMethodName,
	}
	readMethods := []string{
		routeplanpb.RoutePlanService_GetStatus_FullMethodName,
		routeplanpb.RoutePlanService_GetSnapshot_FullMethodName,
		routeplanpb.RoutePlanService_GetHolders_FullMethodName,
		routeplanpb.RoutePlanService_GetShard_FullMethodName,
		routeplanpb.RoutePlanService_GetPlan_FullMethodName,
		routeplanpb.RoutePlanService_Verify_FullMethodName,
		scaleplanpb.ScalePlanService_GetScalePlan_FullMethodName,
		scaleplanpb.ScalePlanService_ListRecoverableScalePlans_FullMethodName,
	}
	mutationMethods := []string{
		routeplanpb.RoutePlanService_MoveShard_FullMethodName,
		routeplanpb.RoutePlanService_DrainHolder_FullMethodName,
		routeplanpb.RoutePlanService_TakeoverAfterExpiry_FullMethodName,
		scaleplanpb.ScalePlanService_SubmitScalePlan_FullMethodName,
		scaleplanpb.ScalePlanService_Pause_FullMethodName,
		scaleplanpb.ScalePlanService_Resume_FullMethodName,
		scaleplanpb.ScalePlanService_TerminateBlocked_FullMethodName,
	}
	rules := make(map[string][]string, len(snapshotMethods)+len(readMethods)+len(mutationMethods))
	for _, method := range snapshotMethods {
		rules[method] = append([]string(nil), snapshotIdentities...)
	}
	for _, method := range readMethods {
		rules[method] = append([]string(nil), planReadIdentities...)
	}
	for _, method := range mutationMethods {
		rules[method] = append([]string(nil), operatorIdentities...)
	}
	return transport.NewMethodAuthorizer(rules)
}

// newServerMethodAuthorizer authenticates only identities from the configured
// control-plane sets. PlanService applies the narrower read/operator ACL and
// maps an authenticated-but-unauthorized caller to PermissionDenied. Snapshot
// RPCs retain the original exact snapshot allowlist at both layers.
func newServerMethodAuthorizer(snapshotIdentities, planReadIdentities, operatorIdentities []string) (*transport.MethodAuthorizer, error) {
	if _, err := newMethodAuthorizer(snapshotIdentities, planReadIdentities, operatorIdentities); err != nil {
		return nil, err
	}
	known := make([]string, 0, len(snapshotIdentities)+len(planReadIdentities)+len(operatorIdentities))
	seen := make(map[string]struct{}, cap(known))
	for _, identities := range [][]string{snapshotIdentities, planReadIdentities, operatorIdentities} {
		for _, identity := range identities {
			if _, exists := seen[identity]; !exists {
				seen[identity] = struct{}{}
				known = append(known, identity)
			}
		}
	}
	rules := map[string][]string{
		routecontrolpb.RouteSnapshotService_GetSnapshot_FullMethodName:        append([]string(nil), snapshotIdentities...),
		routecontrolpb.RouteSnapshotService_GetCurrentRevision_FullMethodName: append([]string(nil), snapshotIdentities...),
		routecontrolpb.RouteSnapshotService_WatchFrom_FullMethodName:          append([]string(nil), snapshotIdentities...),
	}
	for _, method := range []string{
		routeplanpb.RoutePlanService_GetStatus_FullMethodName,
		routeplanpb.RoutePlanService_GetSnapshot_FullMethodName,
		routeplanpb.RoutePlanService_GetHolders_FullMethodName,
		routeplanpb.RoutePlanService_GetShard_FullMethodName,
		routeplanpb.RoutePlanService_GetPlan_FullMethodName,
		routeplanpb.RoutePlanService_Verify_FullMethodName,
		routeplanpb.RoutePlanService_MoveShard_FullMethodName,
		routeplanpb.RoutePlanService_DrainHolder_FullMethodName,
		routeplanpb.RoutePlanService_TakeoverAfterExpiry_FullMethodName,
		scaleplanpb.ScalePlanService_SubmitScalePlan_FullMethodName,
		scaleplanpb.ScalePlanService_GetScalePlan_FullMethodName,
		scaleplanpb.ScalePlanService_ListRecoverableScalePlans_FullMethodName,
		scaleplanpb.ScalePlanService_Pause_FullMethodName,
		scaleplanpb.ScalePlanService_Resume_FullMethodName,
		scaleplanpb.ScalePlanService_TerminateBlocked_FullMethodName,
	} {
		rules[method] = append([]string(nil), known...)
	}
	return transport.NewMethodAuthorizer(rules)
}

func newDrainControllerFactory(cfg config, registry recoveryHolderReader, pool *holderControlClientPool) routeplan.DrainControllerFactory {
	return func(ctx context.Context, plan routeplan.Plan) (authority.DrainController, func() error, error) {
		if registry == nil || pool == nil || ctx == nil || cfg.holderClientCert == "" || cfg.holderClientKey == "" || cfg.holderClientCA == "" {
			return nil, nil, errors.New("route-controller: holder control TLS client is not configured")
		}
		lookup := registry.Get
		if plan.OwnerWaveID != "" {
			if cached, ok := registry.(routeplan.CachedHolderReader); ok {
				lookup = cached.GetCached
			}
		}
		source, sourceFound, err := lookup(ctx, plan.Domain, plan.SourceHolderID)
		if err != nil || !sourceFound || !validPlanHolderEndpoints(source) || source.BootID != plan.SourceBootID ||
			(!plan.SourceDeploymentIdentity.IsZero() && source.DeploymentIdentity != plan.SourceDeploymentIdentity) || source.LogicEndpoint != plan.SourceEndpoint {
			if err == nil {
				err = errors.New("route-controller: source holder identity unavailable")
			}
			return nil, nil, err
		}
		target, targetFound, err := lookup(ctx, plan.Domain, plan.TargetHolderID)
		if err != nil || !targetFound || !validPlanHolderEndpoints(target) || target.BootID != plan.TargetBootID ||
			(!plan.TargetDeploymentIdentity.IsZero() && target.DeploymentIdentity != plan.TargetDeploymentIdentity) || target.LogicEndpoint != plan.TargetEndpoint {
			if err == nil {
				err = errors.New("route-controller: target holder identity unavailable")
			}
			return nil, nil, err
		}
		clientTimeout := holderControlClientTimeout(cfg.planOperation)
		sourceClient, releaseSource, err := pool.Acquire(ctx, source)
		if err != nil {
			return nil, nil, errors.New("route-controller: source holder control dial failed")
		}
		targetClient, releaseTarget, err := pool.Acquire(ctx, target)
		if err != nil {
			releaseSource()
			return nil, nil, errors.New("route-controller: target holder control dial failed")
		}
		controller, err := holdercontrol.NewRemoteDrainController(holdercontrol.RemoteDrainControllerOptions{
			Source: holdercontrol.HolderPeer{Client: sourceClient, Domain: plan.Domain, HolderID: source.HolderID, BootID: source.BootID, LogicEndpoint: source.LogicEndpoint, HolderControlEndpoint: source.HolderControlEndpoint, TLSDNSIdentity: source.TLSDNSIdentity},
			Target: holdercontrol.HolderPeer{Client: targetClient, Domain: plan.Domain, HolderID: target.HolderID, BootID: target.BootID, LogicEndpoint: target.LogicEndpoint, HolderControlEndpoint: target.HolderControlEndpoint, TLSDNSIdentity: target.TLSDNSIdentity},
			// The RoutePlan execution context and control lease retain the configured
			// 5-minute budget. Each HolderControl RPC remains within the transport's
			// independently enforced 30-second maximum.
			Domain: plan.Domain, VirtualShard: plan.VirtualShard, OperationTimeout: clientTimeout, VerifyStatus: true,
		})
		if err != nil {
			releaseSource()
			releaseTarget()
			return nil, nil, errors.New("route-controller: holder drain controller configuration failed")
		}
		return controller, func() error {
			releaseSource()
			releaseTarget()
			return nil
		}, nil
	}
}

type recoveryHolderReader interface {
	Get(context.Context, actorrouting.RouteDomain, string) (core.HolderRecord, bool, error)
}

func newPendingProjectionTargetValidator(registry recoveryHolderReader) authority.PendingProjectionTargetValidator {
	return func(ctx context.Context, recovery authority.TransitionRecovery, snapshotRevision uint64) error {
		if ctx == nil || registry == nil || recovery.TransitionID == "" || recovery.NewHolderID == "" || recovery.NewHolderBootID == "" || recovery.Endpoint == "" {
			return errors.New("route-controller: invalid pending projection target")
		}
		target, found, err := registry.Get(ctx, recovery.Domain, recovery.NewHolderID)
		now := time.Now()
		if err != nil || !found || !validPlanHolderEndpoints(target) || target.Domain != recovery.Domain || target.HolderID != recovery.NewHolderID ||
			target.BootID != recovery.NewHolderBootID ||
			(recovery.DeploymentIdentityPresent && (target.DeploymentIdentity.PodUID != recovery.TargetPodUID ||
				target.DeploymentIdentity.WorkloadUID != recovery.TargetWorkloadUID || target.DeploymentIdentity.Ordinal != recovery.TargetOrdinal)) ||
			target.LogicEndpoint != recovery.Endpoint ||
			target.TLSDNSIdentity == "" || target.CapacityWeight == 0 ||
			!recoveryTargetRuntimeAdmissible(target, !recovery.InternalDrain && !recovery.Published) ||
			!target.LeaseUntil.After(now) ||
			target.SnapshotRevision < snapshotRevision {
			return errors.New("route-controller: pending projection target holder is unavailable")
		}
		return nil
	}
}

// pendingColdLoadController deliberately exposes only the prepare callback.
// A committed Drain recovery has durable proof that Stop/Wait/Facts already
// completed; any attempt to invoke them is a Manager recovery bug and fails
// without contacting either holder.
type pendingColdLoadController struct {
	inner authority.DrainController
}

func (c pendingColdLoadController) StopNewActivationAndTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	return errors.New("route-controller: pending recovery cannot repeat Stop")
}
func (c pendingColdLoadController) WaitCurrentTurns(context.Context, actorrouting.RouteDomain, uint32) error {
	return errors.New("route-controller: pending recovery cannot repeat Wait")
}
func (c pendingColdLoadController) ConfirmFactsCommitted(context.Context, actorrouting.RouteDomain, uint32) error {
	return errors.New("route-controller: pending recovery cannot repeat Facts")
}
func (c pendingColdLoadController) ColdLoadNewHolder(ctx context.Context, domain actorrouting.RouteDomain, shard uint32, value authoritystore.ShardWriteAuthority) error {
	if c.inner == nil {
		return errors.New("route-controller: pending recovery target controller unavailable")
	}
	return c.inner.ColdLoadNewHolder(ctx, domain, shard, value)
}

func (c pendingColdLoadController) ColdLoadPrepared(ctx context.Context, domain actorrouting.RouteDomain, shard uint32, value authoritystore.ShardWriteAuthority) (bool, error) {
	getter, ok := c.inner.(interface {
		GetStatus(context.Context, bool) (*holdercontrolpb.ShardStatus, error)
	})
	if !ok {
		return false, errors.New("route-controller: pending recovery prepared inspector unavailable")
	}
	status, err := getter.GetStatus(ctx, true)
	if err != nil {
		return false, err
	}
	if status == nil || !status.Prepared {
		return false, nil
	}
	if status.PreparedGeneration == nil {
		return false, errors.New("route-controller: prepared generation missing")
	}
	prepared, err := holdercontrol.DecodeAuthorityGeneration(status.PreparedGeneration)
	if err != nil {
		return false, err
	}
	want := holdercontrol.AuthorityGeneration{
		HolderID: value.HolderID, HolderBootID: value.HolderBootID,
		AssignmentRevision: value.AssignmentRevision, WriterEpoch: value.WriterEpoch,
		LeaseEpoch: value.LeaseEpoch, LeaseUntilMs: value.LeaseUntilMs,
	}
	if prepared.HolderID != want.HolderID ||
		prepared.HolderBootID != want.HolderBootID ||
		prepared.AssignmentRevision != want.AssignmentRevision || prepared.WriterEpoch != want.WriterEpoch ||
		prepared.LeaseEpoch != want.LeaseEpoch ||
		domain != value.Domain || shard != value.VirtualShard {
		return false, errors.New("route-controller: prepared generation mismatch or expired")
	}
	// A controller deadline pass may extend durable Authority after the holder
	// prepared the same immutable generation. Returning false makes checkpoint-2
	// recovery repeat only the idempotent Prepare callback with the newer
	// deadline; it never repeats Stop, Wait, Facts, or the fenced transition.
	if prepared.ValidateAt(time.Now()) != nil ||
		prepared.LeaseUntilMs < want.LeaseUntilMs {
		return false, nil
	}
	return true, nil
}

func newRecoveryDrainControllerFactory(cfg config, registry recoveryHolderReader) authority.PendingDrainControllerFactory {
	return func(ctx context.Context, recovery authority.TransitionRecovery) (authority.DrainController, func() error, error) {
		if ctx == nil || registry == nil || !recovery.InternalDrain {
			return nil, nil, errors.New("route-controller: invalid pending drain recovery")
		}
		target, found, err := registry.Get(ctx, recovery.Domain, recovery.NewHolderID)
		if err != nil || !found || !validPlanHolderEndpoints(target) || target.Domain != recovery.Domain || target.HolderID != recovery.NewHolderID ||
			target.BootID != recovery.NewHolderBootID ||
			(recovery.DeploymentIdentityPresent && (target.DeploymentIdentity.PodUID != recovery.TargetPodUID ||
				target.DeploymentIdentity.WorkloadUID != recovery.TargetWorkloadUID || target.DeploymentIdentity.Ordinal != recovery.TargetOrdinal)) ||
			target.LogicEndpoint != recovery.Endpoint ||
			target.TLSDNSIdentity == "" || target.CapacityWeight == 0 ||
			!recoveryTargetRuntimeAdmissible(target, !recovery.InternalDrain && !recovery.Published) ||
			!target.LeaseUntil.After(time.Now()) {
			return nil, nil, errors.New("route-controller: pending drain target holder is unavailable")
		}
		if cfg.holderClientCert == "" || cfg.holderClientKey == "" || cfg.holderClientCA == "" {
			return nil, nil, errors.New("route-controller: holder control TLS client is not configured")
		}
		certPEM, err := os.ReadFile(cfg.holderClientCert)
		if err != nil {
			return nil, nil, errors.New("route-controller: holder client certificate unavailable")
		}
		keyPEM, err := os.ReadFile(cfg.holderClientKey)
		if err != nil {
			return nil, nil, errors.New("route-controller: holder client key unavailable")
		}
		caPEM, err := os.ReadFile(cfg.holderClientCA)
		if err != nil {
			return nil, nil, errors.New("route-controller: holder server CA unavailable")
		}
		clientTimeout := holderControlClientTimeout(cfg.planOperation)
		targetClient, err := holdercontrol.DialHolderControl(ctx, target.HolderControlEndpoint, certPEM, keyPEM, caPEM, holdercontrol.HolderControlDialOptions{
			ServerName: target.TLSDNSIdentity, ExpectedServerIdentity: target.TLSDNSIdentity,
			MaxMessageBytes: cfg.maxMessageBytes, DialTimeout: clientTimeout, RPCTimeout: clientTimeout,
		})
		if err != nil {
			return nil, nil, errors.New("route-controller: target holder control dial failed")
		}
		// RemoteDrainController's cold-load method uses only Target. A phantom
		// Source bound to the same authenticated target client satisfies its
		// immutable constructor contract while pendingColdLoadController makes
		// every source callback unreachable.
		controller, err := holdercontrol.NewRemoteDrainController(holdercontrol.RemoteDrainControllerOptions{
			Source: holdercontrol.HolderPeer{Client: targetClient, Domain: recovery.Domain,
				HolderID: recovery.ExpectedFence.ExpectedHolderID, BootID: recovery.ExpectedFence.ExpectedHolderBootID,
				LogicEndpoint: target.LogicEndpoint, HolderControlEndpoint: target.HolderControlEndpoint, TLSDNSIdentity: target.TLSDNSIdentity},
			Target: holdercontrol.HolderPeer{Client: targetClient, Domain: recovery.Domain,
				HolderID: target.HolderID, BootID: target.BootID, LogicEndpoint: target.LogicEndpoint, HolderControlEndpoint: target.HolderControlEndpoint, TLSDNSIdentity: target.TLSDNSIdentity},
			Domain: recovery.Domain, VirtualShard: recovery.VirtualShard, OperationTimeout: clientTimeout, VerifyStatus: true,
		})
		if err != nil {
			_ = targetClient.Close()
			return nil, nil, errors.New("route-controller: pending holder controller configuration failed")
		}
		return pendingColdLoadController{inner: controller}, targetClient.Close, nil
	}
}

func recoveryTargetRuntimeAdmissible(target core.HolderRecord, allowColdWarming bool) bool {
	if target.ProcessReady && !target.Draining &&
		(target.State == core.HolderStateReadyZeroShard || target.State == core.HolderStateServing) {
		return true
	}
	return allowColdWarming && !target.ProcessReady && !target.Draining &&
		target.State == core.HolderStateWarming && target.ServingShardCount == 0
}

func validPlanHolderEndpoints(record core.HolderRecord) bool {
	return netendpoint.Validate(record.LogicEndpoint, core.DefaultHolderMaxEndpointBytes) == nil &&
		netendpoint.Validate(record.HolderControlEndpoint, core.DefaultHolderMaxEndpointBytes) == nil
}
