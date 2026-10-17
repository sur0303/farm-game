package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/holdercontrol"
	"farmgame/internal/holderkey"
	"farmgame/internal/platform/kubernetes/holderdiscovery"
	"farmgame/internal/routecontrol/verifiedholder"
	scaleplancore "farmgame/internal/scaleplan"
	scaleplancontrol "farmgame/internal/scaleplan/adapters/control"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	holderDiscoveryModeLegacy    = "legacy"
	holderDiscoveryModeShadow    = "shadow"
	holderDiscoveryModeK8sCanary = "k8s-canary"
	holderDiscoveryModeK8s       = "k8s"

	defaultHolderDiscoveryAudience    = "classic-farm-holder-control"
	defaultHolderDiscoveryResync      = 30 * time.Second
	defaultHolderDiscoveryVerifiedTTL = 45 * time.Second
	defaultPlayerHolderLogicPort      = 9000
	defaultFarmHolderLogicPort        = 9000
	defaultPlayerHolderControlPort    = 19100
	defaultFarmHolderControlPort      = 19101
)

type holderDiscoveryRuntime struct {
	view *verifiedholder.Store
	// registry is used only to materialize the short-lived placement identity
	// for a newly attested Pod generation.  Discovery used to do this only at
	// controller startup, which left a replacement StatefulSet Pod visible in
	// the verified view but absent from the exact placement-exclusion store.
	// Keeping this in the discovery runtime makes the post-start informer path
	// match the startup recovery path without weakening any boot fence.
	registry            verifiedPlacementRegistry
	materializeTimeout  time.Duration
	workloads           []verifiedholder.WorkloadKey
	discoveries         []*holderdiscovery.Discovery
	refreshers          map[actorrouting.RouteDomain]func(context.Context) error
	refreshMu           sync.Mutex
	refreshing          map[actorrouting.RouteDomain]*holderDiscoveryRefreshFlight
	materializeRequests chan struct{}
	changes             chan struct{}
	keyReconcileMu      sync.RWMutex
	keyReconcile        func(context.Context) error
	keySequence         atomic.Uint64
	cancel              context.CancelFunc
	done                chan struct{}
	closeOnce           sync.Once
}

// holderDiscoveryRefreshFlight coalesces concurrent exact-holder refreshes
// for one route domain. Every waiter still validates the returned
// VerifiedHolderView against its own expected holder generation and snapshot
// revision; only the repeated Kubernetes, HolderControl and TokenReview I/O is
// shared.
type holderDiscoveryRefreshFlight struct {
	done    chan struct{}
	err     error
	waiters int
}

func validateHolderDiscoveryMode(mode string) error {
	mode = effectiveHolderDiscoveryMode(mode)
	switch mode {
	case holderDiscoveryModeLegacy, holderDiscoveryModeShadow, holderDiscoveryModeK8sCanary, holderDiscoveryModeK8s:
		return nil
	default:
		return errors.New("route-controller: holder discovery mode must be legacy, shadow, k8s-canary or k8s")
	}
}

func effectiveHolderDiscoveryMode(mode string) string {
	if mode == "" {
		return holderDiscoveryModeLegacy
	}
	return mode
}

func holderDiscoveryUsesK8sCandidates(mode string) bool {
	mode = effectiveHolderDiscoveryMode(mode)
	return mode == holderDiscoveryModeK8sCanary || mode == holderDiscoveryModeK8s
}

func validateHolderDiscoveryConfig(cfg config) error {
	if err := validateHolderDiscoveryMode(cfg.holderDiscoveryMode); err != nil {
		return err
	}
	if effectiveHolderDiscoveryMode(cfg.holderDiscoveryMode) == holderDiscoveryModeLegacy {
		return nil
	}
	if cfg.scalePlanMode != string(scaleplancore.RuntimeModeManual) && cfg.scalePlanMode != string(scaleplancore.RuntimeModeAuto) {
		return errors.New("route-controller: Kubernetes holder discovery requires SCALEPLAN_MODE=manual or auto")
	}
	if cfg.holderDiscoveryAudience == "" || cfg.holderClientCert == "" || cfg.holderClientKey == "" || cfg.holderClientCA == "" {
		return errors.New("route-controller: Kubernetes holder discovery requires audience and holder-control mTLS client material")
	}
	if cfg.holderDiscoveryResync <= 0 || cfg.holderDiscoveryVerifiedTTL <= cfg.holderDiscoveryResync {
		return errors.New("route-controller: holder verification TTL must exceed the discovery resync interval")
	}
	for name, port := range map[string]int{
		"player logic": cfg.holderDiscoveryPlayerLogicPort, "player control": cfg.holderDiscoveryPlayerControlPort,
		"farm logic": cfg.holderDiscoveryFarmLogicPort, "farm control": cfg.holderDiscoveryFarmControlPort,
	} {
		if port < 1 || port > 65535 {
			return fmt.Errorf("route-controller: invalid %s holder discovery port", name)
		}
	}
	return nil
}

func newKubernetesHolderDiscoveryHooks(parent context.Context, cfg config, plan *planRuntime) (scaleplanRuntimeHooks, func(), error) {
	if effectiveHolderDiscoveryMode(cfg.holderDiscoveryMode) == holderDiscoveryModeLegacy {
		return scaleplanRuntimeHooks{}, func() {}, nil
	}
	if parent == nil || plan == nil || plan.Registry == nil {
		return scaleplanRuntimeHooks{}, nil, errors.New("route-controller: holder discovery runtime inputs are required")
	}
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return scaleplanRuntimeHooks{}, nil, fmt.Errorf("route-controller: holder discovery Kubernetes config: %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return scaleplanRuntimeHooks{}, nil, fmt.Errorf("route-controller: holder discovery Kubernetes client: %w", err)
	}
	runtime, err := newHolderDiscoveryRuntime(parent, cfg, kubeClient, plan.Registry)
	if err != nil {
		return scaleplanRuntimeHooks{}, nil, err
	}
	publicKeys := verifiedHolderPublicKeyResolver{view: runtime.view}
	if err := plan.Manager.SetHolderPublicKeyResolver(publicKeys); err != nil {
		runtime.Close()
		return scaleplanRuntimeHooks{}, nil, err
	}
	runtime.setKeyReconciler(func(ctx context.Context) error {
		return reconcileVerifiedHolderPublicKeys(ctx, plan, runtime.keySequence.Add(1))
	})
	cleanup := func() { runtime.Close() }
	hooks := scaleplanRuntimeHooks{
		NewKubernetesClient: func() (kubernetes.Interface, error) { return kubeClient, nil },
		Close:               func() error { runtime.Close(); return nil },
		WaitInitial:         runtime.WaitInitial,
		PreparePlanRecovery: func(ctx context.Context) error {
			if err := runtime.ReconcileKeys(ctx); err != nil {
				return err
			}
			return materializeVerifiedPlacementCandidates(ctx, runtime.view, plan.Registry)
		},
		AutoWake:           runtime.Changes(),
		VerifiedHolderView: runtime.view,
	}
	if holderDiscoveryUsesK8sCandidates(cfg.holderDiscoveryMode) {
		reader, readerErr := newVerifiedRoutePlanHolderReaderWithRegistry(runtime.view, plan.Registry, runtime.RefreshDomain)
		if readerErr != nil {
			runtime.Close()
			return scaleplanRuntimeHooks{}, nil, readerErr
		}
		if plan.HolderReader == nil {
			runtime.Close()
			return scaleplanRuntimeHooks{}, nil, errors.New("route-controller: switchable RoutePlan holder reader unavailable")
		}
		if setErr := plan.HolderReader.Set(reader); setErr != nil {
			runtime.Close()
			return scaleplanRuntimeHooks{}, nil, setErr
		}
		hooks.NewHolderStateReader = func(options scaleplancontrol.HolderStateReaderOptions) (scaleplancore.HolderStateReader, error) {
			return scaleplancontrol.NewVerifiedHolderStateReader(scaleplancontrol.VerifiedHolderStateReaderOptions{
				View: runtime.view, Exclusions: options.Exclusions, Registry: options.Registry, Now: time.Now,
			})
		}
	}
	return hooks, cleanup, nil
}

func newHolderDiscoveryRuntime(parent context.Context, cfg config, kubeClient kubernetes.Interface, registry verifiedPlacementRegistry) (*holderDiscoveryRuntime, error) {
	if parent == nil || kubeClient == nil {
		return nil, errors.New("route-controller: holder discovery runtime inputs are required")
	}
	certPEM, err := os.ReadFile(cfg.holderClientCert)
	if err != nil {
		return nil, errors.New("route-controller: holder discovery client certificate unavailable")
	}
	keyPEM, err := os.ReadFile(cfg.holderClientKey)
	if err != nil {
		return nil, errors.New("route-controller: holder discovery client key unavailable")
	}
	caPEM, err := os.ReadFile(cfg.holderClientCA)
	if err != nil {
		return nil, errors.New("route-controller: holder discovery server CA unavailable")
	}
	provider := holderdiscovery.HolderControlClientProviderFunc(func(ctx context.Context, pod holderdiscovery.ObservedPod) (holderdiscovery.HolderControlIdentityClient, func(), error) {
		clientTimeout := holderControlClientTimeout(cfg.planOperation)
		client, dialErr := holdercontrol.DialHolderControl(ctx, pod.ControlEndpoint, certPEM, keyPEM, caPEM, holdercontrol.HolderControlDialOptions{
			ServerName: pod.TLSDNSIdentity, ExpectedServerIdentity: pod.TLSDNSIdentity,
			MaxMessageBytes: cfg.maxMessageBytes, DialTimeout: clientTimeout, RPCTimeout: clientTimeout,
		})
		if dialErr != nil {
			return nil, nil, dialErr
		}
		return client, func() { _ = client.Close() }, nil
	})
	verifier, err := holderdiscovery.NewHolderControlVerifier(provider)
	if err != nil {
		return nil, err
	}
	attestor, err := holderdiscovery.NewTokenReviewAttestor(kubeClient)
	if err != nil {
		return nil, err
	}
	view := verifiedholder.NewStore(time.Now)
	playerRef, farmRef := scalePlanWorkloadRefs(cfg)
	domainOptions := []struct {
		ref         scaleplancore.WorkloadRef
		logicPort   int
		controlPort int
		tlsName     string
	}{
		{ref: playerRef, logicPort: cfg.holderDiscoveryPlayerLogicPort, controlPort: cfg.holderDiscoveryPlayerControlPort, tlsName: cfg.holderDiscoveryPlayerTLSIdentity},
		{ref: farmRef, logicPort: cfg.holderDiscoveryFarmLogicPort, controlPort: cfg.holderDiscoveryFarmControlPort, tlsName: cfg.holderDiscoveryFarmTLSIdentity},
	}
	discoveries := make([]*holderdiscovery.Discovery, 0, len(domainOptions))
	refreshers := make(map[actorrouting.RouteDomain]func(context.Context) error, len(domainOptions))
	for _, item := range domainOptions {
		tlsName := item.tlsName
		serviceName := item.ref.Name + "-headless"
		discovery, createErr := holderdiscovery.New(holderdiscovery.Options{
			Client: kubeClient, Workload: item.ref, Verifier: verifier, Attestor: attestor,
			Audience: cfg.holderDiscoveryAudience, View: view,
			ResyncPeriod: cfg.holderDiscoveryResync, VerifiedTTL: cfg.holderDiscoveryVerifiedTTL,
			LogicPort: item.logicPort, ControlPort: item.controlPort,
			EndpointHost: func(podName, namespace string) string {
				return podName + "." + serviceName + "." + namespace + ".svc.cluster.local"
			},
			TLSServerName: func(podName, _ string) string {
				if tlsName != "" {
					return tlsName
				}
				return podName
			},
		})
		if createErr != nil {
			return nil, fmt.Errorf("route-controller: %s holder discovery: %w", item.ref.Domain, createErr)
		}
		discoveries = append(discoveries, discovery)
		current := discovery
		refreshers[item.ref.Domain] = func(ctx context.Context) error {
			_, reconcileErr := current.Reconcile(ctx)
			return reconcileErr
		}
	}
	ctx, cancel := context.WithCancel(parent)
	workloads := []verifiedholder.WorkloadKey{
		workloadKeyFromRef(playerRef),
		workloadKeyFromRef(farmRef),
	}
	runtime := &holderDiscoveryRuntime{
		view: view, registry: registry, materializeTimeout: holderControlClientTimeout(cfg.planOperation),
		workloads: workloads, discoveries: discoveries, refreshers: refreshers,
		refreshing:          make(map[actorrouting.RouteDomain]*holderDiscoveryRefreshFlight),
		materializeRequests: make(chan struct{}, 1),
		changes:             make(chan struct{}, 1), cancel: cancel, done: make(chan struct{}),
	}
	go runtime.run(ctx)
	return runtime, nil
}

// RefreshDomain performs one bounded Kubernetes + HolderControl verification
// pass for the requested route domain. It is used only on RoutePlan's exact
// holder lookup path; normal discovery still remains informer-driven with a
// periodic reconciliation safety net.
func (r *holderDiscoveryRuntime) RefreshDomain(ctx context.Context, domain actorrouting.RouteDomain) error {
	if r == nil || ctx == nil || r.refreshers == nil {
		return errors.New("route-controller: holder discovery refresh unavailable")
	}
	refresh := r.refreshers[domain]
	if refresh == nil {
		return errors.New("route-controller: holder discovery domain refresh unavailable")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.refreshMu.Lock()
	if r.refreshing == nil {
		r.refreshing = make(map[actorrouting.RouteDomain]*holderDiscoveryRefreshFlight)
	}
	if flight := r.refreshing[domain]; flight != nil {
		flight.waiters++
		done := flight.done
		r.refreshMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return flight.err
		}
	}
	flight := &holderDiscoveryRefreshFlight{done: make(chan struct{}), waiters: 1}
	r.refreshing[domain] = flight
	r.refreshMu.Unlock()

	err := refresh(ctx)
	r.refreshMu.Lock()
	flight.err = err
	if r.refreshing[domain] == flight {
		delete(r.refreshing, domain)
	}
	close(flight.done)
	r.refreshMu.Unlock()
	return err
}

func workloadKeyFromRef(ref scaleplancore.WorkloadRef) verifiedholder.WorkloadKey {
	return verifiedholder.WorkloadKey{
		Realm: ref.Realm, Domain: ref.Domain, Namespace: ref.Namespace,
		Kind: ref.Kind, Name: ref.Name,
	}
}

type verifiedHolderPublicKeyResolver struct {
	view *verifiedholder.Store
}

func (r verifiedHolderPublicKeyResolver) ResolveHolderPublicKey(domain actorrouting.RouteDomain, holderID, bootID string) (holderkey.Publication, bool, error) {
	if r.view == nil || holderID == "" || bootID == "" {
		return holderkey.Publication{}, false, nil
	}
	var selected *holderkey.Publication
	for _, holder := range r.view.ListVerifiedDomain(domain) {
		if holder.HolderID != holderID || holder.BootID != bootID {
			continue
		}
		if selected != nil {
			return holderkey.Publication{}, false, errors.New("route-controller: duplicate exact holder key publication")
		}
		if holder.CommitProofKey.KeyID == "" && len(holder.CommitProofKey.PublicKey) == 0 {
			continue
		}
		if err := holder.CommitProofKey.Validate(); err != nil {
			return holderkey.Publication{}, false, err
		}
		copy := holder.CommitProofKey.Clone()
		selected = &copy
	}
	if selected == nil {
		return holderkey.Publication{}, false, nil
	}
	return selected.Clone(), true, nil
}

func reconcileVerifiedHolderPublicKeys(ctx context.Context, plan *planRuntime, sequence uint64) error {
	if ctx == nil || plan == nil || plan.Manager == nil || sequence == 0 {
		return errors.New("route-controller: holder key reconciliation unavailable")
	}
	operationID := fmt.Sprintf("holder-key-projection/%d", sequence)
	bound, resolve, err := plan.ControlMutation.Begin(ctx, operationID, scaleplancore.ControlOperationProjection)
	if err != nil {
		return err
	}
	result, reconcileErr := plan.Manager.ReconcileHolderPublicKeys(bound)
	outcomeKnown := reconcileErr == nil || result.Committed || !result.PublicationAttempted
	if resolve != nil && outcomeKnown {
		if resolveErr := resolve(); resolveErr != nil {
			reconcileErr = errors.Join(reconcileErr, resolveErr)
		}
	}
	return reconcileErr
}

func (r *holderDiscoveryRuntime) setKeyReconciler(reconcile func(context.Context) error) {
	if r == nil {
		return
	}
	r.keyReconcileMu.Lock()
	r.keyReconcile = reconcile
	r.keyReconcileMu.Unlock()
}

func (r *holderDiscoveryRuntime) ReconcileKeys(ctx context.Context) error {
	if r == nil || ctx == nil {
		return errors.New("route-controller: holder key reconciliation unavailable")
	}
	r.keyReconcileMu.RLock()
	reconcile := r.keyReconcile
	r.keyReconcileMu.RUnlock()
	if reconcile == nil {
		return errors.New("route-controller: holder key reconciliation unavailable")
	}
	return reconcile(ctx)
}

var _ authority.HolderPublicKeyResolver = verifiedHolderPublicKeyResolver{}

// WaitInitial prevents RoutePlan and recovery callers from switching away from
// the durable legacy reader before both Kubernetes informer snapshots exist.
// A snapshot with zero verified holders is still meaningful: it says discovery
// has completed and failover must wait for an eligible target instead of using
// stale registrations.
func (r *holderDiscoveryRuntime) WaitInitial(ctx context.Context) error {
	if r == nil || r.view == nil || ctx == nil || len(r.workloads) == 0 {
		return errors.New("route-controller: holder discovery initial view unavailable")
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		complete := true
		for _, workload := range r.workloads {
			if _, found := r.view.Snapshot(workload); !found {
				complete = false
				break
			}
		}
		if complete {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *holderDiscoveryRuntime) run(ctx context.Context) {
	defer close(r.done)
	var wait sync.WaitGroup
	if r.registry != nil {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-r.materializeRequests:
					materializeCtx, cancel := context.WithTimeout(ctx, r.materializeTimeout)
					err := materializeVerifiedPlacementCandidates(materializeCtx, r.view, r.registry)
					if keyErr := r.ReconcileKeys(materializeCtx); keyErr != nil {
						err = errors.Join(err, keyErr)
					}
					cancel()
					if err != nil && !errors.Is(err, context.Canceled) {
						log.Printf("route-controller holder publication reconciliation after discovery change: %v", err)
					}
					// Wake the plan controller after both success and failure. A
					// failed attempt remains fail-closed; the next informer event or
					// periodic refresh can retry it without inventing a placement.
					r.publishChange()
				}
			}
		}()
	}
	for _, discovery := range r.discoveries {
		discovery := discovery
		wait.Add(2)
		go func() {
			defer wait.Done()
			if err := discovery.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("route-controller holder discovery stopped: %v", err)
			}
		}()
		go func() {
			defer wait.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-discovery.Changes():
					r.publishChange()
					r.requestMaterialization()
				}
			}
		}()
	}
	wait.Wait()
}

func (r *holderDiscoveryRuntime) requestMaterialization() {
	if r == nil || r.registry == nil || r.materializeRequests == nil {
		return
	}
	select {
	case r.materializeRequests <- struct{}{}:
	default:
		// One queued request is enough: the worker always reads the current
		// verified view, so bursts of Pod/informer updates are coalesced.
	}
}

func (r *holderDiscoveryRuntime) Changes() <-chan struct{} {
	if r == nil {
		return nil
	}
	return r.changes
}

func (r *holderDiscoveryRuntime) publishChange() {
	select {
	case r.changes <- struct{}{}:
	default:
	}
}

func (r *holderDiscoveryRuntime) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
		r.cancel()
		<-r.done
	})
}

// HolderControl transport rejects anything above 30s. RoutePlan may use a
// longer operation budget; discovery and control dials must stay inside the
// client limit or every candidate verify fails closed.
func holderControlClientTimeout(operationTimeout time.Duration) time.Duration {
	const maxClientTimeout = 30 * time.Second
	if operationTimeout > 0 && operationTimeout <= maxClientTimeout {
		return operationTimeout
	}
	return maxClientTimeout
}

func holderDiscoveryDomainTLSName(cfg config, domain actorrouting.RouteDomain) string {
	if domain == actorrouting.DomainPlayer {
		return cfg.holderDiscoveryPlayerTLSIdentity
	}
	return cfg.holderDiscoveryFarmTLSIdentity
}
