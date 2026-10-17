package holderdiscovery

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderkey"
	kubescale "farmgame/internal/platform/kubernetes/scaleplan"
	"farmgame/internal/routecontrol/verifiedholder"
	scaleplancore "farmgame/internal/scaleplan"
)

const (
	DefaultResyncPeriod     = 30 * time.Second
	DefaultVerifiedTTL      = 45 * time.Second
	DefaultLogicPort        = 9000
	DefaultControlPort      = 9001
	identityCaptureAttempts = 3
)

var (
	ErrInvalidDiscovery       = errors.New("holderdiscovery: invalid configuration")
	ErrRuntimeIdentity        = errors.New("holderdiscovery: invalid runtime identity")
	ErrWorkloadAttestation    = errors.New("holderdiscovery: workload attestation failed")
	ErrNoVerifiedReadyHolders = errors.New("holderdiscovery: no ready pod passed verification")
)

type ObservedPod struct {
	Workload           scaleplancore.WorkloadRef
	WorkloadUID        string
	PodName            string
	PodUID             string
	PodResourceVersion string
	HolderID           string
	Ordinal            int32
	PodIP              string
	ServiceAccountName string
	LogicEndpoint      string
	ControlEndpoint    string
	TLSDNSIdentity     string
}

type RuntimeIdentity struct {
	Domain                actorrouting.RouteDomain
	HolderID              string
	BootID                string
	WorkloadUID           string
	PodUID                string
	Ordinal               int32
	ProcessReady          bool
	Draining              bool
	ServingShardCount     uint32
	InflightTurnCount     uint32
	ObservedRouteRevision uint64
	Lifecycle             verifiedholder.LifecycleState
	CapacityWeight        uint32
	ObservedAt            time.Time
	WorkloadIdentityToken string
	CommitProofKey        holderkey.Publication
}

type RuntimeIdentityVerifier interface {
	Verify(context.Context, ObservedPod) (RuntimeIdentity, error)
}

type RuntimeIdentityVerifierFunc func(context.Context, ObservedPod) (RuntimeIdentity, error)

func (f RuntimeIdentityVerifierFunc) Verify(ctx context.Context, pod ObservedPod) (RuntimeIdentity, error) {
	if f == nil {
		return RuntimeIdentity{}, ErrRuntimeIdentity
	}
	return f(ctx, pod)
}

type AttestationRequest struct {
	Token              string
	Audience           string
	Namespace          string
	ServiceAccountName string
	PodName            string
	PodUID             string
}

type Attestation struct {
	SubjectHash string
}

type WorkloadIdentityAttestor interface {
	Attest(context.Context, AttestationRequest) (Attestation, error)
}

type WorkloadIdentityAttestorFunc func(context.Context, AttestationRequest) (Attestation, error)

func (f WorkloadIdentityAttestorFunc) Attest(ctx context.Context, request AttestationRequest) (Attestation, error) {
	if f == nil {
		return Attestation{}, ErrWorkloadAttestation
	}
	return f(ctx, request)
}

type SnapshotProvider interface {
	GetIdentity(context.Context, scaleplancore.WorkloadRef) (kubescale.KubernetesIdentitySnapshot, error)
}

type Options struct {
	Client           kubernetes.Interface
	Workload         scaleplancore.WorkloadRef
	SnapshotProvider SnapshotProvider
	Verifier         RuntimeIdentityVerifier
	Attestor         WorkloadIdentityAttestor
	Audience         string
	View             *verifiedholder.Store
	ResyncPeriod     time.Duration
	VerifiedTTL      time.Duration
	LogicPort        int
	ControlPort      int
	// EndpointHost optionally replaces the Pod IP used for the verified
	// holder's logic and control endpoints. StatefulSet holders use their
	// stable headless-service DNS name so the verified view, HolderRegistry
	// and RouteSnapshot all share one canonical endpoint representation.
	EndpointHost  func(podName, namespace string) string
	TLSServerName func(podName, namespace string) string
	Now           func() time.Time
}

type ReconcileResult struct {
	DiscoveryRevision uint64
	ObservedReadyPods uint32
	VerifiedHolders   uint32
	RejectedHolders   uint32
}

type Discovery struct {
	client        kubernetes.Interface
	workload      scaleplancore.WorkloadRef
	verifier      RuntimeIdentityVerifier
	attestor      WorkloadIdentityAttestor
	audience      string
	view          *verifiedholder.Store
	adapter       SnapshotProvider
	informer      cache.SharedIndexInformer
	resyncPeriod  time.Duration
	verifiedTTL   time.Duration
	logicPort     int
	controlPort   int
	endpointHost  func(string, string) string
	tlsServerName func(string, string) string
	now           func() time.Time
	wakeup        chan struct{}
	changes       chan verifiedholder.WorkloadSnapshot
	reconcileMu   sync.Mutex
	revision      atomic.Uint64
	lastMu        sync.RWMutex
	last          ReconcileResult
}

func New(options Options) (*Discovery, error) {
	if options.Client == nil || options.Verifier == nil || options.Attestor == nil || options.View == nil || strings.TrimSpace(options.Audience) == "" {
		return nil, fmt.Errorf("%w: client, verifier, attestor, audience and view are required", ErrInvalidDiscovery)
	}
	if err := options.Workload.Validate(); err != nil {
		return nil, fmt.Errorf("%w: workload: %v", ErrInvalidDiscovery, err)
	}
	if options.ResyncPeriod <= 0 {
		options.ResyncPeriod = DefaultResyncPeriod
	}
	if options.VerifiedTTL <= 0 {
		options.VerifiedTTL = DefaultVerifiedTTL
	}
	if options.LogicPort == 0 {
		options.LogicPort = DefaultLogicPort
	}
	if options.ControlPort == 0 {
		options.ControlPort = DefaultControlPort
	}
	if options.LogicPort < 1 || options.LogicPort > 65535 || options.ControlPort < 1 || options.ControlPort > 65535 {
		return nil, fmt.Errorf("%w: invalid holder ports", ErrInvalidDiscovery)
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.TLSServerName == nil {
		options.TLSServerName = func(podName, _ string) string { return podName }
	}
	provider := options.SnapshotProvider
	if provider == nil {
		provider = kubescale.NewAdapter(options.Client)
	}
	d := &Discovery{
		client: options.Client, workload: options.Workload, verifier: options.Verifier, attestor: options.Attestor,
		audience: options.Audience, view: options.View, adapter: provider,
		resyncPeriod: options.ResyncPeriod, verifiedTTL: options.VerifiedTTL, logicPort: options.LogicPort,
		controlPort: options.ControlPort, endpointHost: options.EndpointHost,
		tlsServerName: options.TLSServerName, now: options.Now,
		wakeup: make(chan struct{}, 1), changes: make(chan verifiedholder.WorkloadSnapshot, 1),
	}
	d.informer = d.newInformer()
	if _, err := d.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    d.onAdd,
		UpdateFunc: d.onUpdate,
		DeleteFunc: d.onDelete,
	}); err != nil {
		return nil, fmt.Errorf("%w: informer handler: %v", ErrInvalidDiscovery, err)
	}
	return d, nil
}

func (d *Discovery) Changes() <-chan verifiedholder.WorkloadSnapshot {
	if d == nil {
		return nil
	}
	return d.changes
}

func (d *Discovery) LastResult() ReconcileResult {
	if d == nil {
		return ReconcileResult{}
	}
	d.lastMu.RLock()
	defer d.lastMu.RUnlock()
	return d.last
}

func (d *Discovery) captureStableIdentity(ctx context.Context) (kubescale.KubernetesIdentitySnapshot, error) {
	var lastErr error
	for attempt := 0; attempt < identityCaptureAttempts; attempt++ {
		snapshot, err := d.adapter.GetIdentity(ctx, d.workload)
		if err == nil {
			return snapshot, nil
		}
		if ctx.Err() != nil {
			return kubescale.KubernetesIdentitySnapshot{}, ctx.Err()
		}
		if !errors.Is(err, scaleplancore.ErrConflict) {
			return kubescale.KubernetesIdentitySnapshot{}, err
		}
		lastErr = err
	}
	return kubescale.KubernetesIdentitySnapshot{}, lastErr
}

func (d *Discovery) Reconcile(ctx context.Context) (ReconcileResult, error) {
	if d == nil || ctx == nil {
		return ReconcileResult{}, ErrInvalidDiscovery
	}
	if err := ctx.Err(); err != nil {
		return ReconcileResult{}, err
	}
	d.reconcileMu.Lock()
	defer d.reconcileMu.Unlock()

	observed, err := d.captureStableIdentity(ctx)
	if err != nil {
		return ReconcileResult{}, err
	}
	now := d.now()
	revision := d.revision.Add(1)
	key := verifiedholder.WorkloadKey{Realm: d.workload.Realm, Domain: d.workload.Domain, Namespace: d.workload.Namespace, Kind: d.workload.Kind, Name: d.workload.Name}
	holders := make([]verifiedholder.Holder, 0, len(observed.Pods))
	result := ReconcileResult{DiscoveryRevision: revision}

	// A Pod can expose its sealed HolderControl endpoint before data-plane
	// readiness. Verifying that exact WARMING/zero-shard boot breaks the cold
	// start cycle without making the Pod placement eligible. Ready Pods remain
	// fail-closed: if none of them verifies, Reconcile returns an error below.
	for _, item := range observed.Pods {
		if !item.Present {
			continue
		}
		if item.Ready {
			result.ObservedReadyPods++
		}
		pod, getErr := d.client.CoreV1().Pods(d.workload.Namespace).Get(ctx, item.PodName, metav1.GetOptions{})
		if getErr != nil || pod == nil || string(pod.UID) != item.PodUID || pod.Status.PodIP == "" || pod.Spec.ServiceAccountName == "" {
			log.Printf("holderdiscovery %s reject pod=%s stage=kubernetes_identity err=%v", d.workload.Domain, item.PodName, getErr)
			if item.Ready {
				result.RejectedHolders++
			}
			continue
		}
		endpointHost := pod.Status.PodIP
		if d.endpointHost != nil {
			endpointHost = strings.TrimSpace(d.endpointHost(item.PodName, d.workload.Namespace))
			if endpointHost == "" {
				log.Printf("holderdiscovery %s reject pod=%s stage=endpoint_host", d.workload.Domain, item.PodName)
				if item.Ready {
					result.RejectedHolders++
				}
				continue
			}
		}
		observedPod := ObservedPod{
			Workload: d.workload, WorkloadUID: observed.UID, PodName: item.PodName, PodUID: item.PodUID,
			PodResourceVersion: pod.ResourceVersion, HolderID: item.HolderID, Ordinal: item.Ordinal,
			PodIP: pod.Status.PodIP, ServiceAccountName: pod.Spec.ServiceAccountName,
			LogicEndpoint:   net.JoinHostPort(endpointHost, fmt.Sprintf("%d", d.logicPort)),
			ControlEndpoint: net.JoinHostPort(endpointHost, fmt.Sprintf("%d", d.controlPort)),
			TLSDNSIdentity:  d.tlsServerName(item.PodName, d.workload.Namespace),
		}
		identity, verifyErr := d.verifier.Verify(ctx, observedPod)
		identityErr := validateRuntimeIdentity(observedPod, identity, now)
		if verifyErr != nil || identityErr != nil {
			log.Printf("holderdiscovery %s reject pod=%s stage=runtime_identity verify_err=%v identity_err=%v", d.workload.Domain, item.PodName, verifyErr, identityErr)
			if item.Ready {
				result.RejectedHolders++
			}
			continue
		}
		attestation, attestErr := d.attestor.Attest(ctx, AttestationRequest{
			Token: identity.WorkloadIdentityToken, Audience: d.audience, Namespace: d.workload.Namespace,
			ServiceAccountName: pod.Spec.ServiceAccountName, PodName: item.PodName, PodUID: item.PodUID,
		})
		if attestErr != nil || attestation.SubjectHash == "" {
			log.Printf("holderdiscovery %s reject pod=%s stage=workload_attestation err=%v", d.workload.Domain, item.PodName, attestErr)
			result.RejectedHolders++
			continue
		}
		verifiedUntil := now.Add(d.verifiedTTL)
		holders = append(holders, verifiedholder.Holder{
			WorkloadKey: key, WorkloadUID: observed.UID, Domain: d.workload.Domain,
			HolderID: item.HolderID, BootID: identity.BootID, PodName: item.PodName, PodUID: item.PodUID, Ordinal: item.Ordinal,
			LogicEndpoint: observedPod.LogicEndpoint, ControlEndpoint: observedPod.ControlEndpoint, TLSDNSIdentity: observedPod.TLSDNSIdentity,
			Lifecycle: identity.Lifecycle, ProcessReady: identity.ProcessReady, Draining: identity.Draining,
			ServingShardCount: identity.ServingShardCount, InflightTurnCount: identity.InflightTurnCount,
			ObservedRouteRevision: identity.ObservedRouteRevision, CapacityWeight: identity.CapacityWeight,
			CommitProofKey:  identity.CommitProofKey.Clone(),
			AttestationHash: attestation.SubjectHash, VerifiedAt: now, VerifiedUntil: verifiedUntil,
		})
		result.VerifiedHolders++
	}

	snapshot := verifiedholder.WorkloadSnapshot{
		Key: key, WorkloadUID: observed.UID, KubeGeneration: observed.Generation,
		KubeResourceVersion: observed.ResourceVersion, KubeListResourceVersion: observed.ListResourceVersion,
		DiscoveryRevision: revision, CapturedAt: now, Holders: holders,
	}
	if err := d.view.ReplaceWorkload(snapshot); err != nil {
		return result, err
	}
	d.lastMu.Lock()
	d.last = result
	d.lastMu.Unlock()
	d.publishChange(snapshot)
	if result.ObservedReadyPods > 0 && result.VerifiedHolders == 0 {
		return result, ErrNoVerifiedReadyHolders
	}
	return result, nil
}

func validateRuntimeIdentity(pod ObservedPod, identity RuntimeIdentity, now time.Time) error {
	if identity.Domain != pod.Workload.Domain || identity.HolderID != pod.HolderID || identity.BootID == "" || identity.WorkloadUID != pod.WorkloadUID || identity.PodUID != pod.PodUID || identity.Ordinal != pod.Ordinal {
		return ErrRuntimeIdentity
	}
	if identity.WorkloadIdentityToken == "" || identity.ObservedAt.IsZero() || identity.ObservedAt.After(now.Add(30*time.Second)) || identity.ObservedAt.Before(now.Add(-2*time.Minute)) {
		return ErrRuntimeIdentity
	}
	if identity.CommitProofKey.KeyID != "" || len(identity.CommitProofKey.PublicKey) != 0 {
		if err := identity.CommitProofKey.Validate(); err != nil {
			return ErrRuntimeIdentity
		}
	}
	probe := verifiedholder.Holder{
		WorkloadKey: verifiedholder.WorkloadKey{Realm: pod.Workload.Realm, Domain: pod.Workload.Domain, Namespace: pod.Workload.Namespace, Kind: pod.Workload.Kind, Name: pod.Workload.Name},
		WorkloadUID: pod.WorkloadUID, Domain: identity.Domain, HolderID: identity.HolderID, BootID: identity.BootID,
		PodName: pod.PodName, PodUID: pod.PodUID, Ordinal: pod.Ordinal, LogicEndpoint: pod.LogicEndpoint,
		ControlEndpoint: pod.ControlEndpoint, TLSDNSIdentity: pod.TLSDNSIdentity, Lifecycle: identity.Lifecycle,
		ProcessReady: identity.ProcessReady, Draining: identity.Draining, ServingShardCount: identity.ServingShardCount,
		InflightTurnCount: identity.InflightTurnCount, ObservedRouteRevision: identity.ObservedRouteRevision,
		CapacityWeight: identity.CapacityWeight, CommitProofKey: identity.CommitProofKey.Clone(),
		AttestationHash: "pending", VerifiedAt: now, VerifiedUntil: now.Add(time.Second),
	}
	return probe.Validate()
}

func (d *Discovery) Run(ctx context.Context) error {
	if d == nil || ctx == nil {
		return ErrInvalidDiscovery
	}
	go d.informer.RunWithContext(ctx)
	if !cache.WaitForCacheSync(ctx.Done(), d.informer.HasSynced) {
		return ctx.Err()
	}
	if _, err := d.Reconcile(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("holderdiscovery %s initial reconcile: %v", d.workload.Domain, err)
	}
	ticker := time.NewTicker(d.resyncPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.wakeup:
			if _, err := d.Reconcile(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("holderdiscovery %s wakeup reconcile: %v", d.workload.Domain, err)
			}
		case <-ticker.C:
			if _, err := d.Reconcile(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("holderdiscovery %s periodic reconcile: %v", d.workload.Domain, err)
			}
		}
	}
}

func (d *Discovery) newInformer() cache.SharedIndexInformer {
	pods := d.client.CoreV1().Pods(d.workload.Namespace)
	listWatch := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			return pods.List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
			return pods.Watch(ctx, options)
		},
	}
	return cache.NewSharedIndexInformer(listWatch, &corev1.Pod{}, 0, cache.Indexers{})
}

func (d *Discovery) onAdd(value interface{}) {
	if d.possiblePod(value) {
		d.enqueue()
	}
}
func (d *Discovery) onDelete(value interface{}) {
	if d.possiblePod(value) {
		d.enqueue()
	}
}
func (d *Discovery) onUpdate(oldValue, newValue interface{}) {
	oldPod, oldOK := podFromEvent(oldValue)
	newPod, newOK := podFromEvent(newValue)
	if !oldOK || !newOK || oldPod.ResourceVersion != newPod.ResourceVersion {
		if d.possiblePod(newValue) || d.possiblePod(oldValue) {
			d.enqueue()
		}
	}
}

func (d *Discovery) possiblePod(value interface{}) bool {
	pod, ok := podFromEvent(value)
	return ok && pod.Namespace == d.workload.Namespace && strings.HasPrefix(pod.Name, d.workload.Name+"-")
}

func podFromEvent(value interface{}) (*corev1.Pod, bool) {
	switch typed := value.(type) {
	case *corev1.Pod:
		return typed, typed != nil
	case cache.DeletedFinalStateUnknown:
		pod, ok := typed.Obj.(*corev1.Pod)
		return pod, ok && pod != nil
	default:
		return nil, false
	}
}

func (d *Discovery) enqueue() {
	select {
	case d.wakeup <- struct{}{}:
	default:
	}
}

func (d *Discovery) publishChange(snapshot verifiedholder.WorkloadSnapshot) {
	select {
	case d.changes <- snapshot:
	default:
		select {
		case <-d.changes:
		default:
		}
		select {
		case d.changes <- snapshot:
		default:
		}
	}
}
