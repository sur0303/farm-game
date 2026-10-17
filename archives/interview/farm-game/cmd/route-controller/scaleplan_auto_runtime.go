package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"farmgame/internal/actorrouting"
	k8sscaleplan "farmgame/internal/platform/kubernetes/scaleplan"
	"farmgame/internal/routecontrol/verifiedholder"
	"farmgame/internal/routeplan"
	"farmgame/internal/routeplan/autofailover"
	scaleplancore "farmgame/internal/scaleplan"
	scaleplancontrol "farmgame/internal/scaleplan/adapters/control"
	"farmgame/internal/scaleplan/autorebalance"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/client-go/kubernetes"
)

type autoScalePlanRuntimeOptions struct {
	Config             config
	Kubernetes         kubernetes.Interface
	Workloads          scaleplancore.WorkloadScaler
	Holders            scaleplancore.HolderStateReader
	Exclusions         scaleplancore.PlacementExclusionPort
	CompletePlacements map[actorrouting.RouteDomain]scaleplancontrol.CompletePlacementReader
	Executor           *scaleplancore.Executor
	WorkloadRefs       []scaleplancore.WorkloadRef
	Wake               <-chan struct{}
	Ready              func() bool
	RoutePlanExecutor  *routeplan.PlanExecutor
	Snapshots          autofailover.SnapshotReader
	Authorities        autofailover.AuthorityReader
	VerifiedHolders    *verifiedholder.Store
	PlacementRegistry  verifiedPlacementRegistry
	MetricsRegisterer  prometheus.Registerer
}

type autoScalePlanRuntime struct {
	controllers        []*autorebalance.Controller
	failover           *autofailover.Controller
	workloadController map[string]*autorebalance.Controller
	inputWake          <-chan struct{}
	wakes              []chan struct{}
	ctx                context.Context
	cancel             context.CancelFunc
	startOnce          sync.Once
	closeOnce          sync.Once
	wait               sync.WaitGroup
	started            atomic.Bool
}

func newAutoScalePlanRuntime(options autoScalePlanRuntimeOptions) (*autoScalePlanRuntime, error) {
	if options.Kubernetes == nil || options.Workloads == nil || options.Holders == nil || options.Exclusions == nil || options.Executor == nil ||
		len(options.WorkloadRefs) != 2 || options.Ready == nil || options.RoutePlanExecutor == nil ||
		options.Snapshots == nil || options.Authorities == nil || options.VerifiedHolders == nil || options.PlacementRegistry == nil {
		return nil, fmt.Errorf("%w: auto ScalePlan runtime dependencies", scaleplancore.ErrInvalidPlan)
	}
	desired, err := k8sscaleplan.NewDesiredReplicaReader(options.Kubernetes, options.Config.scalePlanSafeDesiredAnnotation)
	if err != nil {
		return nil, err
	}
	var rollout autorebalance.RolloutControl
	if options.Config.scalePlanRolloutEnabled {
		rollout, err = k8sscaleplan.NewRolloutManager(options.Kubernetes,
			options.Config.scalePlanRolloutAnnotation, options.Config.scalePlanRolloutIntentAnnotation,
			options.Config.scalePlanSafeDesiredAnnotation)
		if err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &autoScalePlanRuntime{
		inputWake: options.Wake, ctx: ctx, cancel: cancel,
		workloadController: make(map[string]*autorebalance.Controller, len(options.WorkloadRefs)),
	}
	rolloutMetrics := newScalePlanRolloutMetrics(options.MetricsRegisterer)
	limits, err := scaleplancore.FunctionalLimitsForParallelism(uint32(effectiveScalePlanParallelism(options.Config.scalePlanRoutePlanParallelism)))
	if err != nil {
		cancel()
		return nil, err
	}
	policy := autorebalance.DefaultPolicy()
	policy.Limits = limits
	reconcileMu := new(sync.Mutex)
	for _, workload := range options.WorkloadRefs {
		reader := options.CompletePlacements[workload.Domain]
		if reader == nil {
			cancel()
			return nil, fmt.Errorf("%w: %s complete placement reader", scaleplancore.ErrInvalidPlan, workload.Domain)
		}
		wake := make(chan struct{}, 1)
		runtime.wakes = append(runtime.wakes, wake)
		controller, createErr := autorebalance.NewController(autorebalance.ControllerOptions{
			Workload: workload, Workloads: options.Workloads, Holders: options.Holders, Exclusions: options.Exclusions,
			Capture: func(ctx context.Context) (scaleplancore.PlacementCaptureToken, []scaleplancore.ShardPlacement, error) {
				capture, captureErr := reader.CaptureComplete(ctx)
				return capture.Token, capture.Placements, captureErr
			},
			Revalidate: reader.Revalidate, Desired: desired, Rollout: rollout, Executor: options.Executor,
			Policy: policy, Interval: options.Config.scalePlanAutoInterval,
			StepTimeout: options.Config.planOperation, Wake: wake, Ready: options.Ready,
			OnResult: func(result autorebalance.Result) {
				rolloutMetrics.observe(result)
				logAutoScalePlanResult(result)
			}, ReconcileMu: reconcileMu,
		})
		if createErr != nil {
			cancel()
			return nil, createErr
		}
		runtime.controllers = append(runtime.controllers, controller)
		if _, duplicate := runtime.workloadController[workload.Key()]; duplicate {
			cancel()
			return nil, fmt.Errorf("%w: duplicate auto workload", scaleplancore.ErrInvalidPlan)
		}
		runtime.workloadController[workload.Key()] = controller
	}
	failoverWake := make(chan struct{}, 1)
	runtime.wakes = append(runtime.wakes, failoverWake)
	failoverMetrics := newAutoFailoverMetrics(options.MetricsRegisterer)
	failover, err := autofailover.New(autofailover.Options{
		Snapshots: options.Snapshots, Authorities: options.Authorities, Holders: options.VerifiedHolders,
		Executor: options.RoutePlanExecutor,
		PrepareTarget: func(ctx context.Context, holder verifiedholder.Holder) error {
			return materializeVerifiedPlacementCandidate(ctx, holder, options.PlacementRegistry)
		},
		Domains:  []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm},
		Interval: options.Config.routeFailoverInterval, StepTimeout: options.Config.planOperation,
		PlanTTL: options.Config.planTTL, MaxPlansPerPass: options.Config.routeFailoverMaxPlans,
		MaxAuthorityReads: options.Config.routeFailoverMaxAuthorityReads,
		Wake:              failoverWake, Ready: options.Ready, ReconcileMu: reconcileMu,
		OnResult: func(result autofailover.Result) {
			failoverMetrics.observe(result)
			logAutoFailoverResult(result)
		},
	})
	if err != nil {
		cancel()
		return nil, err
	}
	runtime.failover = failover
	return runtime, nil
}

// SeedRecoverable reconnects the in-process controllers to the durable plans
// left nonterminal after Executor.Startup made its first recovery pass. This
// matters when the topology has already changed enough that a fresh planner
// would correctly return "no new plan" while the existing parent still needs
// to verify or advance its next RoutePlan child.
func (r *autoScalePlanRuntime) SeedRecoverable(records []scaleplancore.PlanRecord) error {
	if r == nil {
		return scaleplancore.ErrInvalidPlan
	}
	seen := make(map[string]string, len(records))
	for _, record := range records {
		if record.Plan.Status.Terminal() {
			continue
		}
		key := record.Plan.Submission.Workload.Key()
		controller, ok := r.workloadController[key]
		if !ok {
			continue
		}
		if previous := seen[key]; previous != "" && previous != record.Plan.PlanID {
			return fmt.Errorf("%w: multiple recoverable plans for workload %s", scaleplancore.ErrCorruptRecord, key)
		}
		if err := controller.SeedActivePlan(record.Plan.PlanID); err != nil {
			return err
		}
		seen[key] = record.Plan.PlanID
	}
	return nil
}

func (r *autoScalePlanRuntime) Start() {
	if r == nil {
		return
	}
	r.startOnce.Do(func() {
		r.started.Store(true)
		for _, controller := range r.controllers {
			controller := controller
			r.wait.Add(1)
			go func() {
				defer r.wait.Done()
				if err := controller.Run(r.ctx); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("route-controller auto ScalePlan stopped: %v", err)
				}
			}()
		}
		if r.failover != nil {
			r.wait.Add(1)
			go func() {
				defer r.wait.Done()
				if err := r.failover.Run(r.ctx); err != nil && !errors.Is(err, context.Canceled) {
					log.Printf("route-controller auto failure recovery stopped: %v", err)
				}
			}()
		}
		r.wait.Add(1)
		go func() {
			defer r.wait.Done()
			r.broadcast(r.ctx)
		}()
	})
}

func (r *autoScalePlanRuntime) broadcast(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.inputWake:
			for _, wake := range r.wakes {
				select {
				case wake <- struct{}{}:
				default:
				}
			}
		}
	}
}

func (r *autoScalePlanRuntime) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
		r.cancel()
		if r.started.Load() {
			r.wait.Wait()
		}
	})
}

func logAutoScalePlanResult(result autorebalance.Result) {
	if result.Action == autorebalance.ActionNone && result.Err == nil {
		return
	}
	if result.Err != nil && (errors.Is(result.Err, scaleplancore.ErrPending) ||
		errors.Is(result.Err, scaleplancore.ErrPaused) || errors.Is(result.Err, scaleplancore.ErrBlocked)) {
		log.Printf("route-controller auto ScalePlan waiting domain=%s action=%s plan=%s status=%s err=%v",
			result.Workload.Domain, result.Action, result.PlanID, result.Status, result.Err)
		return
	}
	log.Printf("route-controller auto ScalePlan domain=%s action=%s plan=%s status=%s err=%v",
		result.Workload.Domain, result.Action, result.PlanID, result.Status, result.Err)
}

func logAutoFailoverResult(result autofailover.Result) {
	if result.PlansCompleted == 0 && result.MissingShards == 0 && result.Err == nil {
		return
	}
	log.Printf("route-controller auto failover completed=%d missing=%d waiting_lease=%d no_target=%d authority_reads=%d last_domain=%s last_shard=%d last_plan=%s err=%v",
		result.PlansCompleted, result.MissingShards, result.WaitingLease, result.NoTarget,
		result.AuthorityReads, result.LastDomain, result.LastShard, result.LastPlanID, result.Err)
}
