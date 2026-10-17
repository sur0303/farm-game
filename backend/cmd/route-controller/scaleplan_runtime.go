package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	scalepb "farmgame/api/gen/scaleplan"
	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	k8sscaleplan "farmgame/internal/platform/kubernetes/scaleplan"
	routecore "farmgame/internal/routecontrol"
	"farmgame/internal/routecontrol/verifiedholder"
	"farmgame/internal/routeplan"
	scaleplancore "farmgame/internal/scaleplan"
	scaleplancontrol "farmgame/internal/scaleplan/adapters/control"
	"farmgame/internal/storage/redis/rediskey"
	redisscaleplan "farmgame/internal/storage/redis/scaleplan"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	scalePlanModeOff                    = string(scaleplancore.RuntimeModeOff)
	minimumControlMutationLeaseTTL      = 2 * time.Minute
	controlMutationLeaseOperationBuffer = 30 * time.Second
)

var (
	ErrScalePlanRuntimeNotReady       = errors.New("route-controller: ScalePlan runtime is not ready")
	ErrScalePlanMissingControlBinding = errors.New("route-controller: ScalePlan control-token binding hook is not installed")
	ErrScalePlanMissingExclusionHook  = errors.New("route-controller: ScalePlan placement-exclusion release hook is not installed")
)

type scaleplanRegistrationHook func(grpc.ServiceRegistrar, *scaleplanRuntime)

type scaleplanExclusionInputs struct {
	Registry    *routecore.HolderRegistry
	Manager     *authority.RouteAssignmentManager
	Coordinator scaleplancore.ControlMutationCoordinator
	Scope       scaleplancore.MutationScope
}

type scaleplanRuntimeHooks struct {
	NewBackend           func(redis.UniversalClient, rediskey.Namespace, string) (scaleplancore.DurableRepositoryBackend, error)
	NewKubernetesClient  func() (kubernetes.Interface, error)
	NewCoordinator       func(scaleplancontrol.ControlMutationCoordinatorOptions) (scaleplancore.ControlMutationCoordinator, error)
	NewWorkloadScaler    func(kubernetes.Interface, []scaleplancore.WorkloadRef) (scaleplancore.WorkloadScaler, error)
	NewHolderStateReader func(scaleplancontrol.HolderStateReaderOptions) (scaleplancore.HolderStateReader, error)
	NewRoutePlanPort     func(scaleplancontrol.RoutePlanAdapterOptions) (scaleplancore.RoutePlanPort, error)
	NewPlacementReader   func(scaleplancontrol.ShardPlacementReaderOptions) (scaleplancore.ShardPlacementReader, error)
	NewQuiescence        func(scaleplancontrol.BlockedQuiescenceOptions) (scaleplancore.BlockedQuiescencePort, error)
	NewExclusions        func(scaleplanExclusionInputs) (scaleplancore.PlacementExclusionPort, error)
	NonceSource          func() (string, error)
	BindControlPlane     func(*planRuntime, scaleplancore.ControlMutationCoordinator) error
	RegisterService      scaleplanRegistrationHook
	Close                func() error
	WaitInitial          func(context.Context) error
	PreparePlanRecovery  func(context.Context) error
	AutoWake             <-chan struct{}
	VerifiedHolderView   *verifiedholder.Store
}

type scaleplanRuntime struct {
	Runtime    *scaleplancore.ProductionRuntime
	Executor   *scaleplancore.Executor
	Repository scaleplancore.ScalePlanRepository
	Service    *scaleplancore.ScalePlanService
	Ready      func() bool

	ready           *atomic.Bool
	registerService scaleplanRegistrationHook
	closeFn         func() error
	closeOnce       sync.Once
	auto            *autoScalePlanRuntime
}

func (r *scaleplanRuntime) Register(registrar grpc.ServiceRegistrar) {
	if r == nil || registrar == nil || r.registerService == nil {
		return
	}
	r.registerService(registrar, r)
}

func (r *scaleplanRuntime) Startup(ctx context.Context) error {
	if r == nil || r.Executor == nil || ctx == nil {
		return scaleplancore.ErrInvalidPlan
	}
	if err := r.Executor.Startup(ctx); err != nil {
		return err
	}
	if r.auto == nil {
		return nil
	}
	records, err := r.Repository.ListRecoverable(ctx)
	if err != nil {
		return err
	}
	return r.auto.SeedRecoverable(records)
}

func (r *scaleplanRuntime) MarkReady() {
	if r == nil || r.ready == nil {
		return
	}
	r.ready.Store(true)
	if r.auto != nil {
		r.auto.Start()
	}
}

func (r *scaleplanRuntime) RequireReady() error {
	if r == nil || r.Ready == nil || !r.Ready() {
		return ErrScalePlanRuntimeNotReady
	}
	return nil
}

func (r *scaleplanRuntime) Close() {
	if r == nil {
		return
	}
	if r.ready != nil {
		r.ready.Store(false)
	}
	r.closeOnce.Do(func() {
		if r.auto != nil {
			r.auto.Close()
		}
		if r.closeFn != nil {
			_ = r.closeFn()
		}
	})
}

func validateScalePlanMode(mode string) error {
	if mode != string(scaleplancore.RuntimeModeOff) && mode != string(scaleplancore.RuntimeModeManual) && mode != string(scaleplancore.RuntimeModeAuto) {
		return fmt.Errorf("route-controller: SCALEPLAN_MODE must be off, manual or auto")
	}
	return nil
}

func scalePlanModeFromConfig(cfg config) (scaleplancore.RuntimeMode, error) {
	mode := cfg.scalePlanMode
	if mode == "" {
		mode = scalePlanModeOff
	}
	if err := validateScalePlanMode(mode); err != nil {
		return "", err
	}
	return scaleplancore.RuntimeMode(mode), nil
}

func validateScalePlanConfig(cfg config) error {
	mode, err := scalePlanModeFromConfig(cfg)
	if err != nil {
		return err
	}
	if mode == scaleplancore.RuntimeModeOff {
		return scaleplancore.BindCoordinatorToMode(mode, nil)
	}
	if mode == scaleplancore.RuntimeModeAuto {
		if !holderDiscoveryUsesK8sCandidates(cfg.holderDiscoveryMode) {
			return errors.New("route-controller: ScalePlan auto mode requires Kubernetes holder discovery candidates")
		}
		if cfg.scalePlanAutoInterval <= 0 || strings.TrimSpace(cfg.scalePlanSafeDesiredAnnotation) == "" {
			return errors.New("route-controller: ScalePlan auto mode requires a positive reconcile interval and safe desired annotation")
		}
		if cfg.scalePlanRolloutEnabled && (strings.TrimSpace(cfg.scalePlanRolloutAnnotation) == "" ||
			strings.TrimSpace(cfg.scalePlanRolloutIntentAnnotation) == "" ||
			cfg.scalePlanRolloutAnnotation == cfg.scalePlanSafeDesiredAnnotation ||
			cfg.scalePlanRolloutIntentAnnotation == cfg.scalePlanSafeDesiredAnnotation ||
			cfg.scalePlanRolloutIntentAnnotation == cfg.scalePlanRolloutAnnotation) {
			return errors.New("route-controller: safe rollout requires distinct state, intent, and desired annotations")
		}
		if cfg.routeFailoverInterval <= 0 || cfg.routeFailoverInterval > time.Minute ||
			cfg.routeFailoverMaxPlans <= 0 || cfg.routeFailoverMaxPlans > 1024 ||
			cfg.routeFailoverMaxAuthorityReads < cfg.routeFailoverMaxPlans || cfg.routeFailoverMaxAuthorityReads > 4096 {
			return errors.New("route-controller: automatic failure recovery bounds are invalid")
		}
	}
	if cfg.scalePlanRolloutEnabled && mode != scaleplancore.RuntimeModeAuto {
		return errors.New("route-controller: safe rollout requires SCALEPLAN_MODE=auto")
	}
	if strings.TrimSpace(cfg.realm) == "" {
		return errors.New("route-controller: ScalePlan mutation mode requires realm")
	}
	if strings.TrimSpace(cfg.controllerID) == "" || strings.TrimSpace(cfg.boot) == "" {
		return errors.New("route-controller: ScalePlan mutation mode requires controller id and boot")
	}
	player, farm := scalePlanWorkloadRefs(cfg)
	if player.Namespace != farm.Namespace {
		return errors.New("route-controller: ScalePlan mutation mode requires one mutation namespace")
	}
	for _, ref := range []scaleplancore.WorkloadRef{player, farm} {
		if err := ref.Validate(); err != nil {
			return fmt.Errorf("route-controller: ScalePlan workload allowlist: %w", err)
		}
	}
	return nil
}

func scalePlanWorkloadRefs(cfg config) (scaleplancore.WorkloadRef, scaleplancore.WorkloadRef) {
	return scaleplancore.WorkloadRef{
			Realm: cfg.realm, Domain: actorrouting.DomainPlayer,
			Namespace: cfg.scalePlanPlayerNamespace, Kind: "StatefulSet", Name: cfg.scalePlanPlayerStatefulSet,
		},
		scaleplancore.WorkloadRef{
			Realm: cfg.realm, Domain: actorrouting.DomainFarm,
			Namespace: cfg.scalePlanFarmNamespace, Kind: "StatefulSet", Name: cfg.scalePlanFarmStatefulSet,
		}
}

func defaultScalePlanRuntimeHooks() scaleplanRuntimeHooks {
	return scaleplanRuntimeHooks{
		NewBackend: func(client redis.UniversalClient, namespace rediskey.Namespace, realm string) (scaleplancore.DurableRepositoryBackend, error) {
			return redisscaleplan.NewBackend(client, redisscaleplan.Options{Namespace: namespace, Realm: realm})
		},
		NewKubernetesClient: func() (kubernetes.Interface, error) {
			restConfig, err := rest.InClusterConfig()
			if err != nil {
				return nil, err
			}
			return kubernetes.NewForConfig(restConfig)
		},
		NewCoordinator: func(options scaleplancontrol.ControlMutationCoordinatorOptions) (scaleplancore.ControlMutationCoordinator, error) {
			return scaleplancontrol.NewControlMutationCoordinator(options)
		},
		NewWorkloadScaler: func(client kubernetes.Interface, refs []scaleplancore.WorkloadRef) (scaleplancore.WorkloadScaler, error) {
			if client == nil {
				return nil, fmt.Errorf("%w: Kubernetes client", scaleplancore.ErrInvalidPlan)
			}
			return newAllowlistedWorkloadScaler(k8sscaleplan.NewWorkloadScaler(client), refs)
		},
		NewHolderStateReader: func(options scaleplancontrol.HolderStateReaderOptions) (scaleplancore.HolderStateReader, error) {
			return scaleplancontrol.NewHolderStateReader(options)
		},
		NewRoutePlanPort: func(options scaleplancontrol.RoutePlanAdapterOptions) (scaleplancore.RoutePlanPort, error) {
			return scaleplancontrol.NewRoutePlanPort(options)
		},
		NewPlacementReader: func(options scaleplancontrol.ShardPlacementReaderOptions) (scaleplancore.ShardPlacementReader, error) {
			return scaleplancontrol.NewShardPlacementReader(options)
		},
		NewQuiescence: func(options scaleplancontrol.BlockedQuiescenceOptions) (scaleplancore.BlockedQuiescencePort, error) {
			return scaleplancontrol.NewBlockedQuiescencePort(options)
		},
		NewExclusions: func(inputs scaleplanExclusionInputs) (scaleplancore.PlacementExclusionPort, error) {
			if inputs.Registry == nil || inputs.Coordinator == nil {
				return nil, ErrScalePlanMissingExclusionHook
			}
			return scaleplancontrol.NewPlacementExclusionPort(scaleplancontrol.PlacementExclusionPortOptions{
				Store: inputs.Registry, Coordinator: inputs.Coordinator, Scope: inputs.Scope,
			})
		},
		NonceSource: randomScalePlanNonce,
		BindControlPlane: func(plan *planRuntime, coordinator scaleplancore.ControlMutationCoordinator) error {
			if plan == nil || plan.ControlMutation.Coordinator == nil || plan.ControlMutation.Coordinator != coordinator {
				return ErrScalePlanMissingControlBinding
			}
			return nil
		},
		RegisterService: func(registrar grpc.ServiceRegistrar, runtime *scaleplanRuntime) {
			if registrar != nil && runtime != nil && runtime.Service != nil {
				scalepb.RegisterScalePlanServiceServer(registrar, runtime.Service)
			}
		},
		Close: func() error { return nil },
	}
}

func mergeScalePlanRuntimeHooks(overrides scaleplanRuntimeHooks) scaleplanRuntimeHooks {
	hooks := defaultScalePlanRuntimeHooks()
	if overrides.NewBackend != nil {
		hooks.NewBackend = overrides.NewBackend
	}
	if overrides.NewKubernetesClient != nil {
		hooks.NewKubernetesClient = overrides.NewKubernetesClient
	}
	if overrides.NewCoordinator != nil {
		hooks.NewCoordinator = overrides.NewCoordinator
	}
	if overrides.NewWorkloadScaler != nil {
		hooks.NewWorkloadScaler = overrides.NewWorkloadScaler
	}
	if overrides.NewHolderStateReader != nil {
		hooks.NewHolderStateReader = overrides.NewHolderStateReader
	}
	if overrides.NewRoutePlanPort != nil {
		hooks.NewRoutePlanPort = overrides.NewRoutePlanPort
	}
	if overrides.NewPlacementReader != nil {
		hooks.NewPlacementReader = overrides.NewPlacementReader
	}
	if overrides.NewQuiescence != nil {
		hooks.NewQuiescence = overrides.NewQuiescence
	}
	if overrides.NewExclusions != nil {
		hooks.NewExclusions = overrides.NewExclusions
	}
	if overrides.NonceSource != nil {
		hooks.NonceSource = overrides.NonceSource
	}
	if overrides.BindControlPlane != nil {
		hooks.BindControlPlane = overrides.BindControlPlane
	}
	if overrides.RegisterService != nil {
		hooks.RegisterService = overrides.RegisterService
	}
	if overrides.Close != nil {
		hooks.Close = overrides.Close
	}
	if overrides.WaitInitial != nil {
		hooks.WaitInitial = overrides.WaitInitial
	}
	if overrides.AutoWake != nil {
		hooks.AutoWake = overrides.AutoWake
	}
	if overrides.VerifiedHolderView != nil {
		hooks.VerifiedHolderView = overrides.VerifiedHolderView
	}
	return hooks
}

func newRouteControllerControlMutation(cfg config) (routeplan.ControlMutationOptions, error) {
	mode, err := scalePlanModeFromConfig(cfg)
	if err != nil {
		return routeplan.ControlMutationOptions{}, err
	}
	if mode == scaleplancore.RuntimeModeOff {
		return routeplan.ControlMutationOptions{}, nil
	}
	playerRef, _ := scalePlanWorkloadRefs(cfg)
	scope := scaleplancore.MutationScope{Namespace: playerRef.Namespace, Realm: cfg.realm}
	owner := scaleplancore.MutationLeaseOwner{
		ControllerID: cfg.controllerID, ControllerBootID: cfg.boot,
	}
	coordinator, err := scaleplancontrol.NewControlMutationCoordinator(scaleplancontrol.ControlMutationCoordinatorOptions{
		Scope: scope, LeaseTTL: routeControllerControlMutationLeaseTTL(cfg.planOperation), Now: time.Now, Epoch: 1,
	})
	if err != nil {
		return routeplan.ControlMutationOptions{}, err
	}
	return routeplan.ControlMutationOptions{
		Coordinator: coordinator, Scope: scope, Owner: owner, NonceSource: randomScalePlanNonce,
	}, nil
}

func newScalePlanRuntime(ctx context.Context, cfg config, client redis.UniversalClient, keyspaceTuple rediskey.KeyspaceTuple,
	plan *planRuntime, override ...scaleplanRuntimeHooks) (*scaleplanRuntime, error) {
	mode, err := scalePlanModeFromConfig(cfg)
	if err != nil {
		return nil, err
	}
	if mode == scaleplancore.RuntimeModeOff {
		if err := scaleplancore.BindCoordinatorToMode(mode, nil); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if err := validateScalePlanConfig(cfg); err != nil {
		return nil, err
	}
	if ctx == nil || client == nil || plan == nil {
		return nil, fmt.Errorf("%w: manual runtime inputs", scaleplancore.ErrInvalidPlan)
	}
	if len(override) > 1 {
		return nil, fmt.Errorf("%w: one hook set allowed", scaleplancore.ErrInvalidPlan)
	}
	var hooks scaleplanRuntimeHooks
	if len(override) == 1 {
		hooks = mergeScalePlanRuntimeHooks(override[0])
	} else {
		hooks = defaultScalePlanRuntimeHooks()
	}

	playerRef, farmRef := scalePlanWorkloadRefs(cfg)
	scope := scaleplancore.MutationScope{Namespace: playerRef.Namespace, Realm: cfg.realm}
	controller := scaleplancore.MutationLeaseOwner{ControllerID: cfg.controllerID, ControllerBootID: cfg.boot}

	coordinator := plan.ControlMutation.Coordinator
	if coordinator == nil {
		coordinator, err = hooks.NewCoordinator(scaleplancontrol.ControlMutationCoordinatorOptions{
			Scope: scope, LeaseTTL: routeControllerControlMutationLeaseTTL(cfg.planOperation), Now: time.Now, Epoch: 1,
		})
		if err != nil {
			return nil, fmt.Errorf("route-controller: ScalePlan coordinator: %w", err)
		}
	}
	if err := scaleplancore.BindCoordinatorToMode(mode, coordinator); err != nil {
		return nil, err
	}

	backend, err := hooks.NewBackend(client, keyspaceTuple.Namespace(), cfg.realm)
	if err != nil {
		return nil, fmt.Errorf("route-controller: ScalePlan repository backend: %w", err)
	}
	if backend == nil {
		return nil, fmt.Errorf("%w: nil repository backend", scaleplancore.ErrInvalidPlan)
	}

	kubeClient, err := hooks.NewKubernetesClient()
	if err != nil {
		return nil, fmt.Errorf("route-controller: ScalePlan Kubernetes client: %w", err)
	}
	if kubeClient == nil {
		return nil, fmt.Errorf("%w: nil Kubernetes client", scaleplancore.ErrInvalidPlan)
	}
	workloads, err := hooks.NewWorkloadScaler(kubeClient, []scaleplancore.WorkloadRef{playerRef, farmRef})
	if err != nil {
		return nil, fmt.Errorf("route-controller: ScalePlan workload adapter: %w", err)
	}
	if workloads == nil {
		return nil, fmt.Errorf("%w: nil workload adapter", scaleplancore.ErrInvalidPlan)
	}

	holders, err := hooks.NewHolderStateReader(scaleplancontrol.HolderStateReaderOptions{
		Registry: plan.Registry, Exclusions: plan.Registry,
	})
	if err != nil {
		return nil, fmt.Errorf("route-controller: ScalePlan holder adapter: %w", err)
	}
	routePlans, err := hooks.NewRoutePlanPort(scaleplancontrol.RoutePlanAdapterOptions{
		Store: plan.RoutePlanStore, Authorities: plan.AuthorityReader, Snapshots: plan.SnapshotStore,
		Verification: plan.Verification, OnPlanEnsured: plan.WakeRoutePlanRecovery, Now: time.Now, PlanTTL: cfg.planTTL,
	})
	if err != nil {
		return nil, fmt.Errorf("route-controller: ScalePlan RoutePlan adapter: %w", err)
	}
	if routePlans == nil {
		return nil, fmt.Errorf("%w: nil RoutePlan adapter", scaleplancore.ErrInvalidPlan)
	}

	placements := make(map[actorrouting.RouteDomain]scaleplancore.ShardPlacementReader, 2)
	completePlacements := make(map[actorrouting.RouteDomain]scaleplancontrol.CompletePlacementReader, 2)
	leaseCheckMode := actorrouting.LeaseCheckSnapshot
	if cfg.deadlineRefreshMode == deadlineRefreshModeAuthority {
		leaseCheckMode = actorrouting.LeaseCheckAuthorityDelegated
	}
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		reader, readerErr := hooks.NewPlacementReader(scaleplancontrol.ShardPlacementReaderOptions{
			Snapshots: plan.SnapshotStore, Holders: plan.Registry, Authorities: plan.AuthorityReader,
			Domain: domain, Scope: "route-snapshot/" + string(domain), Now: time.Now, LeaseCheckMode: leaseCheckMode,
		})
		if readerErr != nil {
			return nil, fmt.Errorf("route-controller: ScalePlan %s placement adapter: %w", domain, readerErr)
		}
		if reader == nil {
			return nil, fmt.Errorf("%w: nil %s placement adapter", scaleplancore.ErrInvalidPlan, domain)
		}
		complete, ok := reader.(scaleplancontrol.CompletePlacementReader)
		if !ok || complete == nil {
			return nil, fmt.Errorf("%w: %s complete placement adapter required", scaleplancore.ErrInvalidPlan, domain)
		}
		placements[domain] = reader
		completePlacements[domain] = complete
	}
	placementReader := &multiDomainScalePlanPlacementReader{readers: placements}

	exclusiveCoordinator, ok := coordinator.(scaleplancontrol.ExclusiveControlCoordinator)
	if !ok || exclusiveCoordinator == nil {
		return nil, fmt.Errorf("%w: exclusive coordinator extension required", scaleplancore.ErrInvalidPlan)
	}
	quiescenceNonce, err := hooks.NonceSource()
	if err != nil {
		return nil, fmt.Errorf("route-controller: ScalePlan quiescence owner nonce: %w", err)
	}
	quiescenceOwner := controller
	quiescenceOwner.OperationNonce = quiescenceNonce
	if err := quiescenceOwner.Validate(); err != nil {
		return nil, fmt.Errorf("route-controller: ScalePlan quiescence owner: %w", err)
	}
	var durableStops routeplan.DurableStopStore
	if candidate, ok := plan.RoutePlanStore.(routeplan.DurableStopStore); ok {
		durableStops = candidate
	}
	var transitions scaleplancontrol.TransitionEnumeration
	if plan.TransitionStore != nil {
		transitions = plan.TransitionStore
	}
	exclusions, err := hooks.NewExclusions(scaleplanExclusionInputs{
		Registry: plan.Registry, Manager: plan.Manager, Coordinator: coordinator, Scope: scope,
	})
	if err != nil {
		return nil, fmt.Errorf("route-controller: ScalePlan exclusion adapter: %w", err)
	}
	if exclusions == nil {
		return nil, fmt.Errorf("%w: nil exclusion adapter", scaleplancore.ErrInvalidPlan)
	}
	quiescences := make(map[actorrouting.RouteDomain]scaleplancore.BlockedQuiescencePort, 2)
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		quiescence, quiescenceErr := hooks.NewQuiescence(scaleplancontrol.BlockedQuiescenceOptions{
			Coordinator: exclusiveCoordinator, Scope: scope, Owner: quiescenceOwner,
			Workloads: workloads, Holders: holders, Placements: completePlacements[domain],
			Exclusions: exclusions, RoutePlans: routePlans, DurableStops: durableStops, Transitions: transitions,
			Now: time.Now,
		})
		if quiescenceErr != nil {
			return nil, fmt.Errorf("route-controller: ScalePlan %s quiescence adapter: %w", domain, quiescenceErr)
		}
		if quiescence == nil {
			return nil, fmt.Errorf("%w: nil %s quiescence adapter", scaleplancore.ErrInvalidPlan, domain)
		}
		quiescences[domain] = quiescence
	}
	quiescence := &multiDomainScalePlanQuiescence{ports: quiescences}

	production, err := scaleplancore.NewProductionRuntime(scaleplancore.ProductionRuntimeOptions{
		RepositoryBackend: backend, Quiescence: quiescence, Scope: scope, Controller: controller,
		NonceSource: hooks.NonceSource, Now: time.Now,
	})
	if err != nil {
		return nil, fmt.Errorf("route-controller: ScalePlan production runtime: %w", err)
	}
	executor, err := production.NewExecutor(scaleplancore.ExecutorOptions{
		Workloads: workloads, RoutePlans: routePlans, Holders: holders,
		Placements: placementReader, Exclusions: exclusions, Now: time.Now,
	})
	if err != nil {
		return nil, fmt.Errorf("route-controller: ScalePlan executor: %w", err)
	}
	if err := hooks.BindControlPlane(plan, coordinator); err != nil {
		return nil, fmt.Errorf("route-controller: ScalePlan control binding: %w", err)
	}
	ready := new(atomic.Bool)
	var auto *autoScalePlanRuntime
	if mode == scaleplancore.RuntimeModeAuto {
		if hooks.VerifiedHolderView == nil {
			return nil, fmt.Errorf("%w: verified holder view required for automatic failure recovery", scaleplancore.ErrInvalidPlan)
		}
		auto, err = newAutoScalePlanRuntime(autoScalePlanRuntimeOptions{
			Config: cfg, Kubernetes: kubeClient, Workloads: workloads, Holders: holders, Exclusions: exclusions,
			CompletePlacements: completePlacements, Executor: executor,
			WorkloadRefs: []scaleplancore.WorkloadRef{playerRef, farmRef},
			Wake:         hooks.AutoWake, Ready: ready.Load,
			RoutePlanExecutor: plan.Executor, Snapshots: plan.Manager, Authorities: plan.AuthorityReader,
			VerifiedHolders:   hooks.VerifiedHolderView,
			PlacementRegistry: plan.Registry,
			MetricsRegisterer: plan.MetricsRegisterer,
		})
		if err != nil {
			return nil, fmt.Errorf("route-controller: auto ScalePlan runtime: %w", err)
		}
	}
	runtime := &scaleplanRuntime{
		Runtime: production, Executor: executor, Repository: production.Repository(),
		Ready: ready.Load, ready: ready, registerService: hooks.RegisterService,
		closeFn: hooks.Close, auto: auto,
	}
	// The authenticated mutation API is deliberately manual-mode only. Auto
	// mode remains annotation-driven and does not expose a second writer.
	if mode == scaleplancore.RuntimeModeManual && plan.Authorizer != nil {
		runtime.Service, err = scaleplancore.NewScalePlanService(scaleplancore.ScalePlanServiceOptions{
			Executor: executor, ReadSurface: production.Repository(), Authorizer: plan.Authorizer,
			Ready:     ready.Load,
			Allowlist: scaleplancore.WorkloadAllowlist{Realm: cfg.realm, Workloads: []scaleplancore.WorkloadRef{playerRef, farmRef}},
		})
		if err != nil {
			return nil, fmt.Errorf("route-controller: ScalePlan manual service: %w", err)
		}
	}
	return runtime, nil
}

func routeControllerControlMutationLeaseTTL(operationTimeout time.Duration) time.Duration {
	ttl := operationTimeout + controlMutationLeaseOperationBuffer
	if ttl < minimumControlMutationLeaseTTL {
		return minimumControlMutationLeaseTTL
	}
	return ttl
}

func randomScalePlanNonce() (string, error) {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

type allowlistedWorkloadScaler struct {
	inner   scaleplancore.WorkloadScaler
	allowed map[string]struct{}
}

func newAllowlistedWorkloadScaler(inner scaleplancore.WorkloadScaler, refs []scaleplancore.WorkloadRef) (scaleplancore.WorkloadScaler, error) {
	if inner == nil || len(refs) == 0 {
		return nil, fmt.Errorf("%w: workload allowlist", scaleplancore.ErrInvalidPlan)
	}
	allowed := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		if err := ref.Validate(); err != nil {
			return nil, err
		}
		allowed[ref.Key()] = struct{}{}
	}
	return &allowlistedWorkloadScaler{inner: inner, allowed: allowed}, nil
}

func (s *allowlistedWorkloadScaler) allowedRef(ref scaleplancore.WorkloadRef) error {
	if s == nil || s.inner == nil {
		return fmt.Errorf("%w: workload adapter", scaleplancore.ErrInvalidPlan)
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	if _, ok := s.allowed[ref.Key()]; !ok {
		return fmt.Errorf("%w: workload is outside the StatefulSet allowlist", scaleplancore.ErrInvalidPlan)
	}
	return nil
}

func (s *allowlistedWorkloadScaler) Get(ctx context.Context, ref scaleplancore.WorkloadRef) (scaleplancore.WorkloadState, error) {
	if err := s.allowedRef(ref); err != nil {
		return scaleplancore.WorkloadState{}, err
	}
	return s.inner.Get(ctx, ref)
}

func (s *allowlistedWorkloadScaler) Scale(ctx context.Context, ref scaleplancore.WorkloadRef, precondition scaleplancore.ScalePrecondition, desired int32) (scaleplancore.ScaleMutationResult, error) {
	if err := s.allowedRef(ref); err != nil {
		return scaleplancore.ScaleMutationResult{}, err
	}
	return s.inner.Scale(ctx, ref, precondition, desired)
}

type multiDomainScalePlanPlacementReader struct {
	readers map[actorrouting.RouteDomain]scaleplancore.ShardPlacementReader
}

func (r *multiDomainScalePlanPlacementReader) reader(domain actorrouting.RouteDomain) (scaleplancore.ShardPlacementReader, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: placement adapter", scaleplancore.ErrInvalidPlan)
	}
	reader, ok := r.readers[domain]
	if !ok || reader == nil {
		return nil, fmt.Errorf("%w: unsupported placement domain %s", scaleplancore.ErrInvalidPlan, domain)
	}
	return reader, nil
}

func (r *multiDomainScalePlanPlacementReader) CaptureShard(ctx context.Context, domain actorrouting.RouteDomain, shard uint32) (scaleplancore.ShardPlacementCapture, error) {
	reader, err := r.reader(domain)
	if err != nil {
		return scaleplancore.ShardPlacementCapture{}, err
	}
	return reader.CaptureShard(ctx, domain, shard)
}

func (r *multiDomainScalePlanPlacementReader) CaptureShards(ctx context.Context, keys []scaleplancore.ShardKey) (scaleplancore.ShardSetCapture, error) {
	if len(keys) == 0 {
		return scaleplancore.ShardSetCapture{}, fmt.Errorf("%w: empty placement key set", scaleplancore.ErrInvalidPlan)
	}
	domain := keys[0].Domain
	for _, key := range keys[1:] {
		if key.Domain != domain {
			return scaleplancore.ShardSetCapture{}, fmt.Errorf("%w: placement key domains differ", scaleplancore.ErrInvalidPlan)
		}
	}
	reader, err := r.reader(domain)
	if err != nil {
		return scaleplancore.ShardSetCapture{}, err
	}
	return reader.CaptureShards(ctx, keys)
}

func (r *multiDomainScalePlanPlacementReader) CaptureHolderShards(ctx context.Context, holder scaleplancore.HolderRef) (scaleplancore.HolderShardCapture, error) {
	reader, err := r.reader(holder.Domain)
	if err != nil {
		return scaleplancore.HolderShardCapture{}, err
	}
	return reader.CaptureHolderShards(ctx, holder)
}

func (r *multiDomainScalePlanPlacementReader) Revalidate(ctx context.Context, token scaleplancore.PlacementCaptureToken) error {
	for _, domain := range []actorrouting.RouteDomain{actorrouting.DomainPlayer, actorrouting.DomainFarm} {
		prefix := "route-snapshot/" + string(domain) + "/"
		if strings.HasPrefix(token.Scope, prefix) {
			reader, err := r.reader(domain)
			if err != nil {
				return err
			}
			return reader.Revalidate(ctx, token)
		}
	}
	return fmt.Errorf("%w: placement token scope", scaleplancore.ErrInvalidPlan)
}

type multiDomainScalePlanQuiescence struct {
	ports map[actorrouting.RouteDomain]scaleplancore.BlockedQuiescencePort
}

func (q *multiDomainScalePlanQuiescence) port(domain actorrouting.RouteDomain) (scaleplancore.BlockedQuiescencePort, error) {
	if q == nil {
		return nil, fmt.Errorf("%w: quiescence adapter", scaleplancore.ErrInvalidPlan)
	}
	port, ok := q.ports[domain]
	if !ok || port == nil {
		return nil, fmt.Errorf("%w: unsupported quiescence domain %s", scaleplancore.ErrInvalidPlan, domain)
	}
	return port, nil
}

func (q *multiDomainScalePlanQuiescence) AcquireBlockedQuiescence(ctx context.Context, plan scaleplancore.Plan) (scaleplancore.BlockedQuiescenceManifest, scaleplancore.BlockedQuiescenceFence, error) {
	port, err := q.port(plan.Submission.Domain)
	if err != nil {
		return scaleplancore.BlockedQuiescenceManifest{}, nil, err
	}
	return port.AcquireBlockedQuiescence(ctx, plan)
}

func (q *multiDomainScalePlanQuiescence) ReleaseExclusionForBlockedTermination(ctx context.Context, plan scaleplancore.Plan, fence scaleplancore.BlockedQuiescenceFence) (scaleplancore.BlockedQuiescenceManifest, error) {
	port, err := q.port(plan.Submission.Domain)
	if err != nil {
		return scaleplancore.BlockedQuiescenceManifest{}, err
	}
	releaser, ok := port.(scaleplancore.BlockedExclusionReleasePort)
	if !ok {
		return scaleplancore.BlockedQuiescenceManifest{}, ErrScalePlanMissingExclusionHook
	}
	return releaser.ReleaseExclusionForBlockedTermination(ctx, plan, fence)
}
