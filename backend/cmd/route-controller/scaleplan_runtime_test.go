package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	scaleplancore "farmgame/internal/scaleplan"
	scaleplancontrol "farmgame/internal/scaleplan/adapters/control"
	"farmgame/internal/storage/redis/rediskey"
	scaleplantestkit "farmgame/internal/testkit/scaleplan"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func TestDeadlineProjectionControlCanResolveUsesPublicationBoundaryProof(t *testing.T) {
	for _, stage := range []string{"", "cancelled", "read", "validate", "revalidate", "exclusive", "initial_authority_batch", "lease_projection_pending"} {
		if !deadlineProjectionControlCanResolve(authority.DeadlineProjectionResult{FailureStage: stage}) {
			t.Errorf("no-write failure stage %q did not release its control token", stage)
		}
	}
	if deadlineProjectionControlCanResolve(authority.DeadlineProjectionResult{PublicationAttempted: true, FailureStage: "publish"}) {
		t.Fatal("ambiguous publication was incorrectly treated as safe")
	}
	if !deadlineProjectionControlCanResolve(authority.DeadlineProjectionResult{Changed: true, PublicationAttempted: true, FailureStage: "publish"}) {
		t.Fatal("a manager-confirmed publication must release its control token")
	}
}

func TestScalePlanModeDefaultsOffAndRejectsUnknownValues(t *testing.T) {
	previous, present := os.LookupEnv("SCALEPLAN_MODE")
	t.Cleanup(func() {
		if present {
			_ = os.Setenv("SCALEPLAN_MODE", previous)
		} else {
			_ = os.Unsetenv("SCALEPLAN_MODE")
		}
	})
	_ = os.Unsetenv("SCALEPLAN_MODE")

	cfg, err := parseConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.scalePlanMode != scalePlanModeOff {
		t.Fatalf("mode=%q, want %q", cfg.scalePlanMode, scalePlanModeOff)
	}
	if _, err := parseConfig([]string{"-scaleplan-mode=unexpected"}); err == nil {
		t.Fatal("unknown ScalePlan mode was accepted")
	}
	if _, err := parseConfig([]string{"-scaleplan-mode="}); err == nil {
		t.Fatal("empty ScalePlan mode was accepted")
	}
}

func TestScalePlanParallelismDefaultsToEightAndAcceptsControlledProfiles(t *testing.T) {
	previous, present := os.LookupEnv("SCALEPLAN_ROUTEPLAN_PARALLELISM")
	t.Cleanup(func() {
		if present {
			_ = os.Setenv("SCALEPLAN_ROUTEPLAN_PARALLELISM", previous)
		} else {
			_ = os.Unsetenv("SCALEPLAN_ROUTEPLAN_PARALLELISM")
		}
	})
	_ = os.Unsetenv("SCALEPLAN_ROUTEPLAN_PARALLELISM")
	cfg, err := parseConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.scalePlanRoutePlanParallelism != 8 || cfg.holderControlMaxClients != 64 {
		t.Fatalf("defaults parallelism=%d clients=%d", cfg.scalePlanRoutePlanParallelism, cfg.holderControlMaxClients)
	}
	for _, profile := range []string{"1", "8", "16", "32"} {
		if _, err := parseConfig([]string{"-scaleplan-routeplan-parallelism=" + profile}); err != nil {
			t.Fatalf("profile %s rejected: %v", profile, err)
		}
	}
	if _, err := parseConfig([]string{"-scaleplan-routeplan-parallelism=2"}); err == nil {
		t.Fatal("unsupported profile 2 was accepted")
	}
}

func TestRouteControllerControlMutationDefersNonceAllocationToOperation(t *testing.T) {
	cfg := manualScalePlanConfig()
	cfg.scalePlanMode = string(scaleplancore.RuntimeModeAuto)
	cfg.holderDiscoveryMode = holderDiscoveryModeK8sCanary
	cfg.scalePlanAutoInterval = 5 * time.Second
	cfg.scalePlanSafeDesiredAnnotation = "scaleplan.farm/safe-desired-replicas"
	cfg.routeFailoverInterval = time.Second
	cfg.routeFailoverMaxPlans = 32
	cfg.routeFailoverMaxAuthorityReads = 128
	opts, err := newRouteControllerControlMutation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := opts.Validate(); err != nil {
		t.Fatalf("control mutation options invalid: %v", err)
	}
	if opts.Owner.OperationNonce != "" {
		t.Fatalf("composition owner retained operation nonce %q", opts.Owner.OperationNonce)
	}
}

func TestScalePlanOffHasNoConstructorOrRegistrationSideEffects(t *testing.T) {
	called := 0
	hooks := scaleplanRuntimeHooks{
		NewBackend: func(redis.UniversalClient, rediskey.Namespace, string) (scaleplancore.DurableRepositoryBackend, error) {
			called++
			return nil, nil
		},
		NewKubernetesClient: func() (kubernetes.Interface, error) {
			called++
			return nil, nil
		},
		NewCoordinator: func(scaleplancontrol.ControlMutationCoordinatorOptions) (scaleplancore.ControlMutationCoordinator, error) {
			called++
			return nil, nil
		},
		NewWorkloadScaler: func(kubernetes.Interface, []scaleplancore.WorkloadRef) (scaleplancore.WorkloadScaler, error) {
			called++
			return nil, nil
		},
		NewHolderStateReader: func(scaleplancontrol.HolderStateReaderOptions) (scaleplancore.HolderStateReader, error) {
			called++
			return nil, nil
		},
		NewRoutePlanPort: func(scaleplancontrol.RoutePlanAdapterOptions) (scaleplancore.RoutePlanPort, error) {
			called++
			return nil, nil
		},
		NewPlacementReader: func(scaleplancontrol.ShardPlacementReaderOptions) (scaleplancore.ShardPlacementReader, error) {
			called++
			return nil, nil
		},
		NewQuiescence: func(scaleplancontrol.BlockedQuiescenceOptions) (scaleplancore.BlockedQuiescencePort, error) {
			called++
			return nil, nil
		},
		NewExclusions: func(scaleplanExclusionInputs) (scaleplancore.PlacementExclusionPort, error) {
			called++
			return nil, nil
		},
		NonceSource: func() (string, error) {
			called++
			return "", nil
		},
		BindControlPlane: func(*planRuntime, scaleplancore.ControlMutationCoordinator) error {
			called++
			return nil
		},
		RegisterService: func(grpc.ServiceRegistrar, *scaleplanRuntime) {
			called++
		},
		Close: func() error {
			called++
			return nil
		},
	}
	runtime, err := newScalePlanRuntime(context.Background(), config{scalePlanMode: "off"}, nil, rediskey.KeyspaceTuple{}, nil, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if runtime != nil {
		t.Fatal("mode=off returned a ScalePlan runtime")
	}
	if called != 0 {
		t.Fatalf("mode=off invoked %d constructors/hooks", called)
	}
}

func TestScalePlanManualCompositionOrderReadinessAndClose(t *testing.T) {
	events := []string{}
	backend := &recordingScalePlanBackend{
		ReferenceBackend: scaleplantestkit.NewReferenceBackend(time.Now),
		events:           &events,
	}
	hooks := scalePlanTestHooks(&events, backend, true, true)
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	defer client.Close()

	runtime, err := newScalePlanRuntime(context.Background(), manualScalePlanConfig(), client, rediskey.KeyspaceTuple{}, &planRuntime{}, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if runtime == nil || runtime.Executor == nil || runtime.Repository == nil || runtime.Ready == nil {
		t.Fatal("manual composition did not expose Executor/Repository/Ready")
	}
	if err := runtime.RequireReady(); !errors.Is(err, ErrScalePlanRuntimeNotReady) {
		t.Fatalf("before startup RequireReady=%v, want not-ready", err)
	}

	events = append(events, "routeplan-recovery")
	if err := runtime.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(events, ","); !strings.Contains(got, "routeplan-recovery,scaleplan-startup") {
		t.Fatalf("startup ordering=%s, want RoutePlan recovery before ScalePlan startup", got)
	}
	if err := runtime.RequireReady(); !errors.Is(err, ErrScalePlanRuntimeNotReady) {
		t.Fatalf("startup marked runtime ready before MarkReady: %v", err)
	}
	runtime.MarkReady()
	if err := runtime.RequireReady(); err != nil {
		t.Fatalf("MarkReady did not open mutation gate: %v", err)
	}

	runtime.Close()
	runtime.Close()
	if runtime.Ready() {
		t.Fatal("Close left ScalePlan runtime ready")
	}
	if countEvent(events, "close") != 1 {
		t.Fatalf("close hook count=%d, want 1", countEvent(events, "close"))
	}
}

func TestScalePlanManualMissingHooksFailClosed(t *testing.T) {
	events := []string{}
	backend := &recordingScalePlanBackend{
		ReferenceBackend: scaleplantestkit.NewReferenceBackend(time.Now),
		events:           &events,
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:0"})
	defer client.Close()

	releaseMissing := scalePlanTestHooks(&events, backend, false, false)
	runtime, err := newScalePlanRuntime(context.Background(), manualScalePlanConfig(), client, rediskey.KeyspaceTuple{}, &planRuntime{}, releaseMissing)
	if runtime != nil || !errors.Is(err, ErrScalePlanMissingExclusionHook) {
		t.Fatalf("missing exclusion hook runtime=%v err=%v", runtime, err)
	}
	if countEvent(events, "k8s") != 1 || countEvent(events, "placement") != 2 || countEvent(events, "quiescence") != 0 {
		t.Fatalf("manual composition did not stop before quiescence construction: events=%v", events)
	}

	events = nil
	controlMissing := scalePlanTestHooks(&events, backend, true, false)
	controlMissing.NewExclusions = func(scaleplanExclusionInputs) (scaleplancore.PlacementExclusionPort, error) {
		return &scalePlanTestExclusions{}, nil
	}
	runtime, err = newScalePlanRuntime(context.Background(), manualScalePlanConfig(), client, rediskey.KeyspaceTuple{}, &planRuntime{}, controlMissing)
	if runtime != nil || !errors.Is(err, ErrScalePlanMissingControlBinding) {
		t.Fatalf("missing control hook runtime=%v err=%v", runtime, err)
	}
}

func TestScalePlanWorkloadAllowlistRejectsNonAllowlistedStatefulSet(t *testing.T) {
	inner := &scalePlanTestWorkloads{}
	allowed := scaleplancore.WorkloadRef{
		Realm: "realm-a", Domain: actorrouting.DomainPlayer, Namespace: "farm",
		Kind: "StatefulSet", Name: "player",
	}
	scaler, err := newAllowlistedWorkloadScaler(inner, []scaleplancore.WorkloadRef{allowed})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scaler.Get(context.Background(), allowed); err != nil {
		t.Fatal(err)
	}
	if inner.getCalls != 1 {
		t.Fatalf("allowlisted Get calls=%d, want 1", inner.getCalls)
	}
	foreign := allowed
	foreign.Name = "other"
	if _, err := scaler.Get(context.Background(), foreign); !errors.Is(err, scaleplancore.ErrInvalidPlan) {
		t.Fatalf("foreign StatefulSet error=%v, want invalid plan", err)
	}
	if inner.getCalls != 1 {
		t.Fatalf("foreign Get reached adapter, calls=%d", inner.getCalls)
	}
}

func manualScalePlanConfig() config {
	return config{
		scalePlanMode:              string(scaleplancore.RuntimeModeManual),
		realm:                      "realm-a",
		controllerID:               "controller-a",
		boot:                       "boot-a",
		scalePlanPlayerNamespace:   "farm",
		scalePlanPlayerStatefulSet: "player",
		scalePlanFarmNamespace:     "farm",
		scalePlanFarmStatefulSet:   "farm",
		planTTL:                    24 * time.Hour,
	}
}

func scalePlanTestHooks(events *[]string, backend scaleplancore.DurableRepositoryBackend, withExclusions, withBinding bool) scaleplanRuntimeHooks {
	hooks := scaleplanRuntimeHooks{
		NewBackend: func(redis.UniversalClient, rediskey.Namespace, string) (scaleplancore.DurableRepositoryBackend, error) {
			*events = append(*events, "backend")
			return backend, nil
		},
		NewKubernetesClient: func() (kubernetes.Interface, error) {
			*events = append(*events, "k8s")
			return fake.NewSimpleClientset(), nil
		},
		NewCoordinator: func(options scaleplancontrol.ControlMutationCoordinatorOptions) (scaleplancore.ControlMutationCoordinator, error) {
			*events = append(*events, "coordinator")
			return scaleplancontrol.NewControlMutationCoordinator(options)
		},
		NewWorkloadScaler: func(kubernetes.Interface, []scaleplancore.WorkloadRef) (scaleplancore.WorkloadScaler, error) {
			*events = append(*events, "workload")
			return &scalePlanTestWorkloads{}, nil
		},
		NewHolderStateReader: func(scaleplancontrol.HolderStateReaderOptions) (scaleplancore.HolderStateReader, error) {
			*events = append(*events, "holders")
			return &scalePlanTestHolders{}, nil
		},
		NewRoutePlanPort: func(scaleplancontrol.RoutePlanAdapterOptions) (scaleplancore.RoutePlanPort, error) {
			*events = append(*events, "route")
			return &scalePlanTestRoutePlans{}, nil
		},
		NewPlacementReader: func(options scaleplancontrol.ShardPlacementReaderOptions) (scaleplancore.ShardPlacementReader, error) {
			*events = append(*events, "placement")
			return &scalePlanTestPlacements{}, nil
		},
		NewQuiescence: func(options scaleplancontrol.BlockedQuiescenceOptions) (scaleplancore.BlockedQuiescencePort, error) {
			*events = append(*events, "quiescence")
			if options.Exclusions == nil {
				return nil, errors.New("test: quiescence exclusion dependency missing")
			}
			return &scalePlanTestQuiescence{}, nil
		},
		NonceSource: func() (string, error) {
			*events = append(*events, "nonce")
			return "nonce-a", nil
		},
	}
	if withExclusions {
		hooks.NewExclusions = func(scaleplanExclusionInputs) (scaleplancore.PlacementExclusionPort, error) {
			*events = append(*events, "exclusions")
			return &scalePlanTestExclusions{}, nil
		}
	}
	if withBinding {
		hooks.BindControlPlane = func(*planRuntime, scaleplancore.ControlMutationCoordinator) error {
			*events = append(*events, "bind")
			return nil
		}
	}
	hooks.Close = func() error {
		*events = append(*events, "close")
		return nil
	}
	return hooks
}

func countEvent(events []string, want string) int {
	count := 0
	for _, event := range events {
		if event == want {
			count++
		}
	}
	return count
}

type recordingScalePlanBackend struct {
	*scaleplantestkit.ReferenceBackend
	events *[]string
}

func (b *recordingScalePlanBackend) ListRecoverable(ctx context.Context, cursor scaleplancore.RecoverableCursor) (scaleplancore.RecoverablePage, error) {
	*b.events = append(*b.events, "scaleplan-startup")
	return b.ReferenceBackend.ListRecoverable(ctx, cursor)
}

type scalePlanTestWorkloads struct {
	getCalls int
}

func (w *scalePlanTestWorkloads) Get(context.Context, scaleplancore.WorkloadRef) (scaleplancore.WorkloadState, error) {
	w.getCalls++
	return scaleplancore.WorkloadState{}, nil
}

func (*scalePlanTestWorkloads) Scale(context.Context, scaleplancore.WorkloadRef, scaleplancore.ScalePrecondition, int32) (scaleplancore.ScaleMutationResult, error) {
	return scaleplancore.ScaleMutationResult{}, nil
}

type scalePlanTestHolders struct{}

func (*scalePlanTestHolders) GetHolder(context.Context, scaleplancore.HolderRef) (scaleplancore.HolderState, bool, error) {
	return scaleplancore.HolderState{}, false, nil
}

func (*scalePlanTestHolders) ListDomainHolders(context.Context, actorrouting.RouteDomain) ([]scaleplancore.HolderState, error) {
	return nil, nil
}

type scalePlanTestRoutePlans struct{}

func (*scalePlanTestRoutePlans) EnsureMove(context.Context, scaleplancore.RoutePlanMoveRequest) (scaleplancore.RoutePlanView, error) {
	return scaleplancore.RoutePlanView{}, nil
}

func (*scalePlanTestRoutePlans) GetMove(context.Context, scaleplancore.RoutePlanMoveRequest) (scaleplancore.RoutePlanView, bool, error) {
	return scaleplancore.RoutePlanView{}, false, nil
}

func (*scalePlanTestRoutePlans) Verify(context.Context, string) (scaleplancore.RoutePlanVerification, error) {
	return scaleplancore.RoutePlanVerification{}, nil
}

type scalePlanTestPlacements struct{}

func (*scalePlanTestPlacements) CaptureShard(context.Context, actorrouting.RouteDomain, uint32) (scaleplancore.ShardPlacementCapture, error) {
	return scaleplancore.ShardPlacementCapture{}, nil
}

func (*scalePlanTestPlacements) CaptureShards(context.Context, []scaleplancore.ShardKey) (scaleplancore.ShardSetCapture, error) {
	return scaleplancore.ShardSetCapture{}, nil
}

func (*scalePlanTestPlacements) CaptureHolderShards(context.Context, scaleplancore.HolderRef) (scaleplancore.HolderShardCapture, error) {
	return scaleplancore.HolderShardCapture{}, nil
}

func (*scalePlanTestPlacements) Revalidate(context.Context, scaleplancore.PlacementCaptureToken) error {
	return nil
}

func (*scalePlanTestPlacements) CaptureComplete(context.Context) (scaleplancontrol.CompletePlacementCapture, error) {
	return scaleplancontrol.CompletePlacementCapture{}, nil
}

type scalePlanTestQuiescence struct{}

func (*scalePlanTestQuiescence) AcquireBlockedQuiescence(context.Context, scaleplancore.Plan) (scaleplancore.BlockedQuiescenceManifest, scaleplancore.BlockedQuiescenceFence, error) {
	return scaleplancore.BlockedQuiescenceManifest{}, nil, nil
}

type scalePlanTestExclusions struct{}

func (*scalePlanTestExclusions) Get(context.Context, scaleplancore.HolderRef) (scaleplancore.PlacementExclusion, bool, error) {
	return scaleplancore.PlacementExclusion{}, false, nil
}

func (*scalePlanTestExclusions) Exclude(context.Context, scaleplancore.HolderRef, scaleplancore.PlacementExclusionPrecondition) (scaleplancore.PlacementExclusionResult, error) {
	return scaleplancore.PlacementExclusionResult{}, nil
}

func (*scalePlanTestExclusions) ReleaseForOwner(context.Context, scaleplancore.HolderRef, scaleplancore.PlacementExclusionRelease) (scaleplancore.PlacementExclusionResult, error) {
	return scaleplancore.PlacementExclusionResult{}, nil
}

func TestScalePlanManualConfigRequiresExactAllowlist(t *testing.T) {
	cfg := manualScalePlanConfig()
	if err := validateScalePlanConfig(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.scalePlanPlayerStatefulSet = ""
	if err := validateScalePlanConfig(cfg); err == nil {
		t.Fatal("manual mode accepted an empty player StatefulSet name")
	}
	cfg = manualScalePlanConfig()
	cfg.scalePlanFarmNamespace = "other"
	if err := validateScalePlanConfig(cfg); err == nil {
		t.Fatal("manual mode accepted split mutation namespaces")
	}
	cfg = manualScalePlanConfig()
	cfg.scalePlanMode = "observe"
	if err := validateScalePlanConfig(cfg); err == nil {
		t.Fatal("manual mode accepted an unsupported process mode")
	}
}

func TestScalePlanAutoConfigRequiresKubernetesCandidatesAndSafeIntent(t *testing.T) {
	cfg := manualScalePlanConfig()
	cfg.scalePlanMode = string(scaleplancore.RuntimeModeAuto)
	cfg.holderDiscoveryMode = holderDiscoveryModeK8s
	cfg.scalePlanAutoInterval = time.Second
	cfg.scalePlanSafeDesiredAnnotation = "scaleplan.farm/safe-desired-replicas"
	cfg.routeFailoverInterval = time.Second
	cfg.routeFailoverMaxPlans = 32
	cfg.routeFailoverMaxAuthorityReads = 128
	if err := validateScalePlanConfig(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.holderDiscoveryMode = holderDiscoveryModeLegacy
	if err := validateScalePlanConfig(cfg); err == nil {
		t.Fatal("auto mode accepted legacy holder candidates")
	}
	cfg.holderDiscoveryMode = holderDiscoveryModeK8s
	cfg.scalePlanSafeDesiredAnnotation = ""
	if err := validateScalePlanConfig(cfg); err == nil {
		t.Fatal("auto mode accepted an empty safe desired annotation")
	}
}

func TestRouteControllerControlMutationUsesPerOperationNonce(t *testing.T) {
	options, err := newRouteControllerControlMutation(manualScalePlanConfig())
	if err != nil {
		t.Fatal(err)
	}
	if options.Owner.OperationNonce != "" {
		t.Fatalf("composition owner retained operation nonce %q", options.Owner.OperationNonce)
	}
	if err := options.Validate(); err != nil {
		t.Fatalf("control mutation options invalid: %v", err)
	}
	_, resolve, err := options.Begin(context.Background(), "route-controller/bootstrap/test", scaleplancore.ControlOperationBootstrap)
	if err != nil {
		t.Fatal(err)
	}
	if err := resolve(); err != nil {
		t.Fatal(err)
	}
}

func TestRouteControllerControlMutationLeaseOutlivesPlanOperation(t *testing.T) {
	if got := routeControllerControlMutationLeaseTTL(30 * time.Second); got != 2*time.Minute {
		t.Fatalf("control lease ttl=%s want %s", got, 2*time.Minute)
	}
	if got := routeControllerControlMutationLeaseTTL(5 * time.Minute); got != 5*time.Minute+30*time.Second {
		t.Fatalf("long control lease ttl=%s want %s", got, 5*time.Minute+30*time.Second)
	}
}

func TestScalePlanManagedRolloutRequiresAutoModeAndDistinctAnnotation(t *testing.T) {
	cfg := manualScalePlanConfig()
	cfg.scalePlanRolloutEnabled = true
	cfg.scalePlanRolloutAnnotation = "scaleplan.farm/managed-rollout-state"
	cfg.scalePlanRolloutIntentAnnotation = "scaleplan.farm/rollout-intent"
	if err := validateScalePlanConfig(cfg); err == nil {
		t.Fatal("manual mode accepted managed rollout")
	}
	cfg.scalePlanMode = string(scaleplancore.RuntimeModeAuto)
	cfg.holderDiscoveryMode = holderDiscoveryModeK8s
	cfg.scalePlanAutoInterval = time.Second
	cfg.scalePlanSafeDesiredAnnotation = "scaleplan.farm/safe-desired-replicas"
	cfg.routeFailoverInterval = time.Second
	cfg.routeFailoverMaxPlans = 32
	cfg.routeFailoverMaxAuthorityReads = 128
	if err := validateScalePlanConfig(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.scalePlanRolloutAnnotation = cfg.scalePlanSafeDesiredAnnotation
	if err := validateScalePlanConfig(cfg); err == nil {
		t.Fatal("managed rollout accepted shared control annotation")
	}
	cfg.scalePlanRolloutAnnotation = "scaleplan.farm/managed-rollout-state"
	cfg.scalePlanRolloutIntentAnnotation = cfg.scalePlanRolloutAnnotation
	if err := validateScalePlanConfig(cfg); err == nil {
		t.Fatal("managed rollout accepted shared state/intent annotation")
	}
}
