package scaleplan

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	core "farmgame/internal/scaleplan"
)

const (
	PodIndexLabel           = "apps.kubernetes.io/pod-index"
	ControllerRevisionLabel = appsv1.ControllerRevisionHashLabelKey

	BarrierOperationAnnotation = "scaleplan.farm/barrier-operation"
	BarrierFenceAnnotation     = "scaleplan.farm/barrier-fence"
	BarrierNonceAnnotation     = "scaleplan.farm/barrier-nonce"

	defaultPodPageSize     int64 = 100
	defaultBarrierAttempts       = 3
)

var (
	ErrUnsupportedScaleMetadata = errors.New("scaleplan kubernetes: unsupported scale metadata")
	ErrNoopScale                = errors.New("scaleplan kubernetes: no-op scale")
	ErrPodIdentityMismatch      = errors.New("scaleplan kubernetes: pod identity mismatch")
	ErrOwnerReferenceMismatch   = errors.New("scaleplan kubernetes: owner reference mismatch")
	ErrListSnapshotInconsistent = errors.New("scaleplan kubernetes: inconsistent pod list snapshot")
	ErrBarrierNotEstablished    = errors.New("scaleplan kubernetes: cancellation barrier not established")
)

var fallbackNonce uint64

type Option func(*Adapter)

type Adapter struct {
	client             kubernetes.Interface
	pageSize           int64
	nonce              func() string
	barrierMaxAttempts int
}

func New(client kubernetes.Interface, options ...Option) *Adapter {
	return NewAdapter(client, options...)
}

func NewAdapter(client kubernetes.Interface, options ...Option) *Adapter {
	adapter := &Adapter{
		client:             client,
		pageSize:           defaultPodPageSize,
		nonce:              randomNonce,
		barrierMaxAttempts: defaultBarrierAttempts,
	}
	for _, option := range options {
		if option != nil {
			option(adapter)
		}
	}
	if adapter.pageSize <= 0 {
		adapter.pageSize = defaultPodPageSize
	}
	if adapter.barrierMaxAttempts <= 0 {
		adapter.barrierMaxAttempts = defaultBarrierAttempts
	}
	if adapter.nonce == nil {
		adapter.nonce = randomNonce
	}
	return adapter
}

func NewWorkloadScaler(client kubernetes.Interface, options ...Option) *Adapter {
	return NewAdapter(client, options...)
}

func WithPageSize(size int64) Option {
	return func(adapter *Adapter) {
		if size > 0 {
			adapter.pageSize = size
		}
	}
}

func WithBarrierMaxAttempts(attempts int) Option {
	return func(adapter *Adapter) {
		if attempts > 0 {
			adapter.barrierMaxAttempts = attempts
		}
	}
}

func WithBarrierNonceGenerator(generate func() string) Option {
	return func(adapter *Adapter) {
		if generate != nil {
			adapter.nonce = generate
		}
	}
}

var _ core.WorkloadScaler = (*Adapter)(nil)

type KubernetesIdentitySnapshot struct {
	Ref                 core.WorkloadRef
	Namespace           string
	Name                string
	UID                 string
	ResourceVersion     string
	Generation          int64
	ObservedGeneration  int64
	SpecReplicas        int32
	StatusReplicas      int32
	ReadyReplicas       int32
	CurrentRevision     string
	UpdateRevision      string
	UpdateStrategy      string
	ListResourceVersion string
	CaptureChecksum     string
	Pods                []core.WorkloadPodState
}

func (snapshot KubernetesIdentitySnapshot) WorkloadState() core.WorkloadState {
	return core.WorkloadState{
		Ref:                snapshot.Ref,
		UID:                snapshot.UID,
		ResourceVersion:    snapshot.ResourceVersion,
		Generation:         snapshot.Generation,
		ObservedGeneration: snapshot.ObservedGeneration,
		Replicas:           snapshot.SpecReplicas,
		ReadyReplicas:      snapshot.ReadyReplicas,
		CurrentRevision:    snapshot.CurrentRevision,
		UpdateRevision:     snapshot.UpdateRevision,
		UpdateStrategy:     snapshot.UpdateStrategy,
		Pods:               clonePods(snapshot.Pods),
	}
}

func (a *Adapter) Get(ctx context.Context, ref core.WorkloadRef) (core.WorkloadState, error) {
	snapshot, err := a.GetIdentity(ctx, ref)
	if err != nil {
		return core.WorkloadState{}, err
	}
	return snapshot.WorkloadState(), nil
}

func (a *Adapter) GetSnapshot(ctx context.Context, ref core.WorkloadRef) (KubernetesIdentitySnapshot, error) {
	return a.GetIdentity(ctx, ref)
}

func (a *Adapter) GetIdentity(ctx context.Context, ref core.WorkloadRef) (KubernetesIdentitySnapshot, error) {
	if err := a.validate(ctx, ref); err != nil {
		return KubernetesIdentitySnapshot{}, err
	}

	start, err := a.getStatefulSet(ctx, ref)
	if err != nil {
		return KubernetesIdentitySnapshot{}, err
	}
	pods, listRV, err := a.listPods(ctx, ref, start)
	if err != nil {
		return KubernetesIdentitySnapshot{}, err
	}
	end, err := a.getStatefulSet(ctx, ref)
	if err != nil {
		return KubernetesIdentitySnapshot{}, err
	}
	if !sameCaptureState(start, end) {
		return KubernetesIdentitySnapshot{}, fmt.Errorf("%w: statefulset changed during bracket", core.ErrConflict)
	}

	state := workloadState(ref, end, pods)
	snapshot := KubernetesIdentitySnapshot{
		Ref:                 ref,
		Namespace:           ref.Namespace,
		Name:                ref.Name,
		UID:                 state.UID,
		ResourceVersion:     state.ResourceVersion,
		Generation:          state.Generation,
		ObservedGeneration:  state.ObservedGeneration,
		SpecReplicas:        state.Replicas,
		StatusReplicas:      end.Status.Replicas,
		ReadyReplicas:       state.ReadyReplicas,
		CurrentRevision:     state.CurrentRevision,
		UpdateRevision:      state.UpdateRevision,
		UpdateStrategy:      state.UpdateStrategy,
		ListResourceVersion: listRV,
		Pods:                clonePods(state.Pods),
	}
	snapshot.CaptureChecksum = captureChecksum(snapshot)
	return snapshot, nil
}

func (a *Adapter) Scale(ctx context.Context, ref core.WorkloadRef, precondition core.ScalePrecondition, desired int32) (core.ScaleMutationResult, error) {
	if err := a.validate(ctx, ref); err != nil {
		if ctx != nil && ctx.Err() != nil {
			return core.ScaleMutationResult{}, unknownOutcome(ctx.Err())
		}
		return core.ScaleMutationResult{}, err
	}
	if desired < 0 {
		return core.ScaleMutationResult{}, fmt.Errorf("%w: negative desired replicas", core.ErrConflict)
	}
	if desired == precondition.ExpectedReplicas {
		return core.ScaleMutationResult{}, fmt.Errorf("%w: %w", ErrNoopScale, core.ErrConflict)
	}

	initial, err := a.getStatefulSet(ctx, ref)
	if err != nil {
		if isUnknownReadError(err) {
			return core.ScaleMutationResult{}, unknownOutcome(err)
		}
		return core.ScaleMutationResult{}, err
	}
	if string(initial.UID) != precondition.WorkloadUID ||
		initial.ResourceVersion != precondition.ResourceVersion ||
		replicasOf(initial) != precondition.ExpectedReplicas {
		return core.ScaleMutationResult{}, fmt.Errorf("%w: scale precondition", core.ErrConflict)
	}

	scale, err := a.client.AppsV1().StatefulSets(ref.Namespace).GetScale(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		if isUnknownReadError(err) {
			return core.ScaleMutationResult{}, unknownOutcome(err)
		}
		return core.ScaleMutationResult{}, err
	}
	if scale == nil || string(scale.UID) == "" || scale.ResourceVersion == "" {
		return core.ScaleMutationResult{}, ErrUnsupportedScaleMetadata
	}
	if string(scale.UID) != string(initial.UID) ||
		scale.ResourceVersion != initial.ResourceVersion ||
		scale.Spec.Replicas != precondition.ExpectedReplicas {
		return core.ScaleMutationResult{}, fmt.Errorf("%w: stale scale subresource", core.ErrConflict)
	}

	update := scale.DeepCopy()
	update.UID = initial.UID
	update.ResourceVersion = initial.ResourceVersion
	update.Spec.Replicas = desired
	_, updateErr := a.client.AppsV1().StatefulSets(ref.Namespace).UpdateScale(ctx, ref.Name, update, metav1.UpdateOptions{})

	observed, readErr := a.getStatefulSet(ctx, ref)
	if readErr != nil {
		return core.ScaleMutationResult{}, unknownOutcome(preferError(readErr, updateErr))
	}
	state := workloadState(ref, observed, nil)

	// A cancelled or timed-out request is never reported as a successful
	// mutation, even when the confirmation read happens to see the desired
	// value. The caller can safely adopt the observed state, but must retain
	// UNKNOWN outcome semantics for recovery.
	if ctx.Err() != nil {
		return core.ScaleMutationResult{State: state}, unknownOutcome(ctx.Err())
	}
	if string(observed.UID) != string(initial.UID) {
		return core.ScaleMutationResult{State: state}, fmt.Errorf("%w: workload UID changed during scale", core.ErrConflict)
	}

	confirmed := replicasOf(observed) == desired &&
		observed.Generation > initial.Generation &&
		observed.ResourceVersion != initial.ResourceVersion
	if confirmed {
		if updateErr == nil {
			return core.ScaleMutationResult{Committed: true, State: state}, nil
		}
		if isUnknownMutationError(updateErr) {
			return core.ScaleMutationResult{State: state}, unknownOutcome(updateErr)
		}
		if apierrors.IsConflict(updateErr) {
			// Another writer already achieved the immutable desired
			// observation. It is adopted, not claimed as our write.
			return core.ScaleMutationResult{State: state}, nil
		}
		return core.ScaleMutationResult{State: state}, unknownOutcome(updateErr)
	}

	if replicasOf(observed) == precondition.ExpectedReplicas &&
		observed.ResourceVersion == initial.ResourceVersion {
		switch {
		case updateErr == nil:
			return core.ScaleMutationResult{State: state}, fmt.Errorf("%w: scale confirmation did not advance", core.ErrConflict)
		case isUnknownMutationError(updateErr):
			return core.ScaleMutationResult{State: state}, unknownOutcome(updateErr)
		case apierrors.IsConflict(updateErr):
			return core.ScaleMutationResult{State: state}, fmt.Errorf("%w: stale scale resourceVersion", core.ErrConflict)
		default:
			return core.ScaleMutationResult{State: state}, updateErr
		}
	}

	if isUnknownMutationError(updateErr) {
		return core.ScaleMutationResult{State: state}, unknownOutcome(updateErr)
	}
	if updateErr != nil && apierrors.IsConflict(updateErr) {
		return core.ScaleMutationResult{State: state}, fmt.Errorf("%w: scale conflict", core.ErrConflict)
	}
	if updateErr != nil && apierrors.IsNotFound(updateErr) {
		return core.ScaleMutationResult{State: state}, updateErr
	}
	if updateErr != nil {
		return core.ScaleMutationResult{State: state}, updateErr
	}
	return core.ScaleMutationResult{State: state}, fmt.Errorf("%w: scale confirmation mismatch", core.ErrConflict)
}

func (a *Adapter) validate(ctx context.Context, ref core.WorkloadRef) error {
	if ctx == nil {
		return core.ErrInvalidPlan
	}
	if err := ref.Validate(); err != nil {
		return err
	}
	if a == nil || a.client == nil {
		return fmt.Errorf("%w: nil kubernetes client", core.ErrInvalidPlan)
	}
	return nil
}

func (a *Adapter) getStatefulSet(ctx context.Context, ref core.WorkloadRef) (*appsv1.StatefulSet, error) {
	set, err := a.client.AppsV1().StatefulSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if set == nil || set.UID == "" || set.ResourceVersion == "" || set.Spec.Replicas == nil || set.Spec.Selector == nil {
		return nil, fmt.Errorf("%w: incomplete statefulset identity", core.ErrConflict)
	}
	if *set.Spec.Replicas < 0 || set.Status.ReadyReplicas < 0 || set.Status.Replicas < 0 {
		return nil, fmt.Errorf("%w: negative statefulset replicas", core.ErrConflict)
	}
	return set.DeepCopy(), nil
}

func (a *Adapter) listPods(ctx context.Context, ref core.WorkloadRef, set *appsv1.StatefulSet) ([]core.WorkloadPodState, string, error) {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, "", fmt.Errorf("%w: invalid statefulset selector: %v", core.ErrConflict, err)
	}
	if selector.Empty() {
		return nil, "", fmt.Errorf("%w: empty statefulset selector", core.ErrConflict)
	}

	var (
		token        string
		listRV       string
		pods         []core.WorkloadPodState
		seenTokens   = make(map[string]struct{})
		seenOrdinals = make(map[int32]struct{})
		seenUIDs     = make(map[string]struct{})
		seenHolders  = make(map[string]struct{})
	)
	for {
		options := metav1.ListOptions{
			LabelSelector: selector.String(),
			Limit:         a.pageSize,
			Continue:      token,
		}
		list, err := a.client.CoreV1().Pods(ref.Namespace).List(ctx, options)
		if err != nil {
			return nil, "", err
		}
		if list == nil || list.ResourceVersion == "" {
			return nil, "", ErrListSnapshotInconsistent
		}
		if listRV == "" {
			listRV = list.ResourceVersion
		} else if list.ResourceVersion != listRV {
			return nil, "", ErrListSnapshotInconsistent
		}
		for i := range list.Items {
			pod, err := canonicalPod(ref, set, selector, &list.Items[i])
			if err != nil {
				return nil, "", err
			}
			if _, found := seenOrdinals[pod.Ordinal]; found {
				return nil, "", fmt.Errorf("%w: duplicate ordinal %d", ErrPodIdentityMismatch, pod.Ordinal)
			}
			if _, found := seenUIDs[pod.PodUID]; found {
				return nil, "", fmt.Errorf("%w: duplicate pod UID %q", ErrPodIdentityMismatch, pod.PodUID)
			}
			if _, found := seenHolders[pod.HolderID]; found {
				return nil, "", fmt.Errorf("%w: duplicate holder ID %q", ErrPodIdentityMismatch, pod.HolderID)
			}
			seenOrdinals[pod.Ordinal] = struct{}{}
			seenUIDs[pod.PodUID] = struct{}{}
			seenHolders[pod.HolderID] = struct{}{}
			pods = append(pods, pod)
		}

		if list.Continue == "" {
			if list.RemainingItemCount != nil && *list.RemainingItemCount > 0 {
				return nil, "", ErrListSnapshotInconsistent
			}
			break
		}
		if _, found := seenTokens[list.Continue]; found {
			return nil, "", ErrListSnapshotInconsistent
		}
		seenTokens[list.Continue] = struct{}{}
		token = list.Continue
	}
	sort.Slice(pods, func(i, j int) bool {
		if pods[i].Ordinal != pods[j].Ordinal {
			return pods[i].Ordinal < pods[j].Ordinal
		}
		return pods[i].PodName < pods[j].PodName
	})
	return pods, listRV, nil
}

func canonicalPod(ref core.WorkloadRef, set *appsv1.StatefulSet, selector labels.Selector, pod *corev1.Pod) (core.WorkloadPodState, error) {
	if pod == nil || pod.Namespace != ref.Namespace || pod.UID == "" ||
		!selector.Matches(labels.Set(pod.Labels)) {
		return core.WorkloadPodState{}, fmt.Errorf("%w: selector/namespace/UID", ErrPodIdentityMismatch)
	}

	var (
		controllerCount int
		controller      metav1.OwnerReference
	)
	for _, owner := range pod.OwnerReferences {
		if owner.Controller != nil && *owner.Controller {
			controllerCount++
			controller = owner
		}
	}
	if controllerCount != 1 ||
		controller.APIVersion != "apps/v1" ||
		controller.Kind != "StatefulSet" ||
		string(controller.UID) != string(set.UID) {
		return core.WorkloadPodState{}, fmt.Errorf("%w: pod %s", ErrOwnerReferenceMismatch, pod.Name)
	}

	rawOrdinal, found := pod.Labels[PodIndexLabel]
	if !found || !canonicalOrdinal(rawOrdinal) {
		return core.WorkloadPodState{}, fmt.Errorf("%w: pod %s index label", ErrPodIdentityMismatch, pod.Name)
	}
	ordinal64, _ := strconv.ParseInt(rawOrdinal, 10, 32)
	ordinal := int32(ordinal64)
	expectedName := fmt.Sprintf("%s-%d", set.Name, ordinal)
	if pod.Name != expectedName {
		return core.WorkloadPodState{}, fmt.Errorf("%w: pod %s expected %s", ErrPodIdentityMismatch, pod.Name, expectedName)
	}

	ready, err := podReady(pod)
	if err != nil {
		return core.WorkloadPodState{}, err
	}
	controllerRevision := strings.TrimSpace(pod.Labels[ControllerRevisionLabel])
	if controllerRevision == "" {
		return core.WorkloadPodState{}, fmt.Errorf("%w: pod %s controller revision", ErrPodIdentityMismatch, pod.Name)
	}
	containerImage, containerImageID, err := workloadContainerIdentity(ref, pod)
	if err != nil {
		return core.WorkloadPodState{}, err
	}
	return core.WorkloadPodState{
		PodName: pod.Name, PodUID: string(pod.UID), WorkloadUID: string(set.UID),
		ControllerRevision: controllerRevision, ContainerImage: containerImage, ContainerImageID: containerImageID,
		Ordinal: ordinal, HolderID: pod.Name,
		Present: true, Ready: ready,
	}, nil
}

func workloadContainerIdentity(ref core.WorkloadRef, pod *corev1.Pod) (string, string, error) {
	image := ""
	for _, container := range pod.Spec.Containers {
		if container.Name != ref.Name {
			continue
		}
		if image != "" {
			return "", "", fmt.Errorf("%w: duplicate workload container %s", ErrPodIdentityMismatch, ref.Name)
		}
		image = strings.TrimSpace(container.Image)
	}
	imageID := ""
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != ref.Name {
			continue
		}
		if imageID != "" {
			return "", "", fmt.Errorf("%w: duplicate workload container status %s", ErrPodIdentityMismatch, ref.Name)
		}
		imageID = strings.TrimSpace(status.ImageID)
	}
	if image == "" || imageID == "" {
		return "", "", fmt.Errorf("%w: workload container image identity %s", ErrPodIdentityMismatch, ref.Name)
	}
	return image, imageID, nil
}

func canonicalOrdinal(value string) bool {
	if value == "" || value[0] == '-' {
		return false
	}
	parsed, err := strconv.ParseInt(value, 10, 32)
	return err == nil && parsed >= 0 && strconv.FormatInt(parsed, 10) == value
}

func podReady(pod *corev1.Pod) (bool, error) {
	var (
		found bool
		ready bool
	)
	for _, condition := range pod.Status.Conditions {
		if condition.Type != corev1.PodReady {
			continue
		}
		if found {
			return false, fmt.Errorf("%w: duplicate ready condition for pod %s", ErrPodIdentityMismatch, pod.Name)
		}
		found = true
		ready = condition.Status == corev1.ConditionTrue
	}
	return ready, nil
}

func sameCaptureState(start, end *appsv1.StatefulSet) bool {
	if start == nil || end == nil || start.UID != end.UID ||
		start.ResourceVersion != end.ResourceVersion ||
		start.Generation != end.Generation ||
		replicasOf(start) != replicasOf(end) || start.Status.CurrentRevision != end.Status.CurrentRevision ||
		start.Status.UpdateRevision != end.Status.UpdateRevision || start.Spec.UpdateStrategy.Type != end.Spec.UpdateStrategy.Type {
		return false
	}
	startSelector, startErr := metav1.LabelSelectorAsSelector(start.Spec.Selector)
	endSelector, endErr := metav1.LabelSelectorAsSelector(end.Spec.Selector)
	return startErr == nil && endErr == nil && startSelector.String() == endSelector.String()
}

func replicasOf(set *appsv1.StatefulSet) int32 {
	if set == nil || set.Spec.Replicas == nil {
		return -1
	}
	return *set.Spec.Replicas
}

func workloadState(ref core.WorkloadRef, set *appsv1.StatefulSet, pods []core.WorkloadPodState) core.WorkloadState {
	return core.WorkloadState{
		Ref:                ref,
		UID:                string(set.UID),
		ResourceVersion:    set.ResourceVersion,
		Generation:         set.Generation,
		ObservedGeneration: set.Status.ObservedGeneration,
		Replicas:           replicasOf(set),
		ReadyReplicas:      set.Status.ReadyReplicas,
		CurrentRevision:    set.Status.CurrentRevision,
		UpdateRevision:     set.Status.UpdateRevision,
		UpdateStrategy:     string(set.Spec.UpdateStrategy.Type),
		Pods:               clonePods(pods),
	}
}

func clonePods(pods []core.WorkloadPodState) []core.WorkloadPodState {
	if pods == nil {
		return nil
	}
	return append([]core.WorkloadPodState(nil), pods...)
}

func captureChecksum(snapshot KubernetesIdentitySnapshot) string {
	payload := struct {
		Ref                 core.WorkloadRef
		UID                 string
		ResourceVersion     string
		Generation          int64
		ObservedGeneration  int64
		SpecReplicas        int32
		StatusReplicas      int32
		ReadyReplicas       int32
		CurrentRevision     string
		UpdateRevision      string
		UpdateStrategy      string
		ListResourceVersion string
		Pods                []core.WorkloadPodState
	}{
		Ref:                 snapshot.Ref,
		UID:                 snapshot.UID,
		ResourceVersion:     snapshot.ResourceVersion,
		Generation:          snapshot.Generation,
		ObservedGeneration:  snapshot.ObservedGeneration,
		SpecReplicas:        snapshot.SpecReplicas,
		StatusReplicas:      snapshot.StatusReplicas,
		ReadyReplicas:       snapshot.ReadyReplicas,
		CurrentRevision:     snapshot.CurrentRevision,
		UpdateRevision:      snapshot.UpdateRevision,
		UpdateStrategy:      snapshot.UpdateStrategy,
		ListResourceVersion: snapshot.ListResourceVersion,
		Pods:                snapshot.Pods,
	}
	data, _ := json.Marshal(payload)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func randomNonce() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "nonce-fallback-" + strconv.FormatUint(atomic.AddUint64(&fallbackNonce, 1), 10)
	}
	return hex.EncodeToString(buf)
}

func unknownOutcome(err error) error {
	if err == nil {
		return core.ErrOutcomeUnknown
	}
	return fmt.Errorf("%w: %v", core.ErrOutcomeUnknown, err)
}

func preferError(primary, fallback error) error {
	if primary != nil {
		return primary
	}
	return fallback
}

func isUnknownReadError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) ||
		apierrors.IsTooManyRequests(err) || errors.Is(err, io.EOF) {
		return true
	}
	var networkErr net.Error
	return errors.As(err, &networkErr) && (networkErr.Timeout() || networkErr.Temporary())
}

func isUnknownMutationError(err error) bool {
	if err == nil {
		return false
	}
	if apierrors.IsConflict(err) || apierrors.IsNotFound(err) || apierrors.IsInvalid(err) {
		return false
	}
	return true
}
