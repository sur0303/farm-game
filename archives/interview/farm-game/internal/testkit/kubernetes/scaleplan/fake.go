package scaleplan

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

type UpdateScaleMode uint8

const (
	UpdateScaleNormal UpdateScaleMode = iota
	UpdateScaleTimeoutBeforeCommit
	UpdateScaleApplyThenTimeout
)

type PatchMode uint8

const (
	PatchNormal PatchMode = iota
	PatchTimeoutBeforeCommit
	PatchApplyThenTimeout
)

type FakeAPIServer struct {
	mu sync.Mutex

	client *fake.Clientset

	statefulSets map[string]*appsv1.StatefulSet
	pods         map[string]*corev1.Pod
	listRV       string
	nextRV       int64

	pageSize int64

	ScaleMetadataAvailable     bool
	PatchHonorsResourceVersion bool
	UpdateScaleMode            UpdateScaleMode
	PatchMode                  PatchMode
	ObserveGenerationOnScale   bool
	GetError                   error
	ListError                  error
	ScaleGetError              error
	UpdateScaleError           error
	PatchError                 error
	PartialList                bool
	ExpireContinue             bool

	getCount int
	getHook  func(*FakeAPIServer, int)

	Actions []string
}

func NewFakeClient(statefulSet *appsv1.StatefulSet, pods ...*corev1.Pod) (*FakeAPIServer, *fake.Clientset) {
	server := &FakeAPIServer{
		statefulSets:               make(map[string]*appsv1.StatefulSet),
		pods:                       make(map[string]*corev1.Pod),
		pageSize:                   100,
		ScaleMetadataAvailable:     true,
		PatchHonorsResourceVersion: true,
		ObserveGenerationOnScale:   true,
		nextRV:                     100,
	}
	if statefulSet != nil {
		copySet := statefulSet.DeepCopy()
		if copySet.ResourceVersion == "" {
			copySet.ResourceVersion = server.allocateRVLocked()
		}
		server.statefulSets[objectKey(copySet.Namespace, copySet.Name)] = copySet
		server.listRV = copySet.ResourceVersion
	}
	for _, pod := range pods {
		if pod != nil {
			copyPod := pod.DeepCopy()
			server.pods[objectKey(copyPod.Namespace, copyPod.Name)] = copyPod
		}
	}

	client := fake.NewSimpleClientset()
	server.client = client
	client.PrependReactor("*", "*", server.react)
	return server, client
}

func (s *FakeAPIServer) Clientset() *fake.Clientset {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client
}

func (s *FakeAPIServer) SetPageSize(size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if size > 0 {
		s.pageSize = size
	}
}

func (s *FakeAPIServer) SetListResourceVersion(resourceVersion string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listRV = resourceVersion
}

func (s *FakeAPIServer) SetGetHook(hook func(*FakeAPIServer, int)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getHook = hook
}

func (s *FakeAPIServer) CurrentStatefulSet(namespace, name string) *appsv1.StatefulSet {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := s.statefulSets[objectKey(namespace, name)]
	if set == nil {
		return nil
	}
	return set.DeepCopy()
}

func (s *FakeAPIServer) SetStatefulSet(set *appsv1.StatefulSet) {
	if set == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	copySet := set.DeepCopy()
	if copySet.ResourceVersion == "" {
		copySet.ResourceVersion = s.allocateRVLocked()
	}
	s.statefulSets[objectKey(copySet.Namespace, copySet.Name)] = copySet
	if s.listRV == "" {
		s.listRV = copySet.ResourceVersion
	}
}

func (s *FakeAPIServer) AddPod(pod *corev1.Pod) {
	if pod == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pods[objectKey(pod.Namespace, pod.Name)] = pod.DeepCopy()
}

func (s *FakeAPIServer) RemovePod(namespace, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pods, objectKey(namespace, name))
}

func (s *FakeAPIServer) AdvanceStatefulSet(namespace, name string, mutate func(*appsv1.StatefulSet)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := s.statefulSets[objectKey(namespace, name)]
	if set == nil || mutate == nil {
		return
	}
	before := set.DeepCopy()
	mutate(set)
	if set.ResourceVersion == before.ResourceVersion && !sameObjectMetadataAndSpec(before, set) {
		set.ResourceVersion = s.allocateRVLocked()
	}
	if set.Generation == before.Generation && replicas(set) != replicas(before) {
		set.Generation++
	}
}

func (s *FakeAPIServer) ActionsSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.Actions...)
}

func (s *FakeAPIServer) react(action ktesting.Action) (bool, runtime.Object, error) {
	switch {
	case action.GetVerb() == "get" && action.GetResource().Resource == "statefulsets" && action.GetSubresource() == "":
		return s.getStatefulSet(action)
	case action.GetVerb() == "get" && action.GetResource().Resource == "statefulsets" && action.GetSubresource() == "scale":
		return s.getScale(action)
	case action.GetVerb() == "list" && action.GetResource().Resource == "pods":
		return s.listPods(action)
	case action.GetVerb() == "update" && action.GetResource().Resource == "statefulsets" && action.GetSubresource() == "scale":
		return s.updateScale(action)
	case action.GetVerb() == "patch" && action.GetResource().Resource == "statefulsets" && action.GetSubresource() == "":
		return s.patchStatefulSet(action)
	default:
		return false, nil, nil
	}
}

func (s *FakeAPIServer) getStatefulSet(action ktesting.Action) (bool, runtime.Object, error) {
	get, ok := action.(ktesting.GetAction)
	if !ok {
		return true, nil, fmt.Errorf("unexpected statefulset get action %T", action)
	}
	namespace, name := action.GetNamespace(), get.GetName()

	s.mu.Lock()
	s.getCount++
	count := s.getCount
	hook := s.getHook
	injectedError := s.GetError
	set := s.statefulSets[objectKey(namespace, name)]
	var result *appsv1.StatefulSet
	if set != nil {
		result = set.DeepCopy()
	}
	s.Actions = append(s.Actions, "get statefulsets/"+name)
	s.mu.Unlock()

	if hook != nil {
		hook(s, count)
	}
	if injectedError != nil {
		return true, nil, injectedError
	}
	if result == nil {
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, name)
	}
	return true, result, nil
}

func (s *FakeAPIServer) getScale(action ktesting.Action) (bool, runtime.Object, error) {
	get, ok := action.(ktesting.GetAction)
	if !ok {
		return true, nil, fmt.Errorf("unexpected scale get action %T", action)
	}
	namespace, name := action.GetNamespace(), get.GetName()

	s.mu.Lock()
	defer s.mu.Unlock()
	set := s.statefulSets[objectKey(namespace, name)]
	s.Actions = append(s.Actions, "get statefulsets/"+name+"/scale")
	if s.ScaleGetError != nil {
		return true, nil, s.ScaleGetError
	}
	if set == nil {
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, name)
	}
	if !s.ScaleMetadataAvailable {
		return true, &autoscalingv1.Scale{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec:       autoscalingv1.ScaleSpec{Replicas: replicas(set)},
		}, nil
	}
	return true, &autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			UID:             set.UID,
			ResourceVersion: set.ResourceVersion,
		},
		Spec: autoscalingv1.ScaleSpec{Replicas: replicas(set)},
	}, nil
}

func (s *FakeAPIServer) listPods(action ktesting.Action) (bool, runtime.Object, error) {
	listAction, ok := action.(ktesting.ListAction)
	if !ok {
		return true, nil, fmt.Errorf("unexpected pod list action %T", action)
	}
	namespace := action.GetNamespace()
	optionsAction, ok := listAction.(interface{ GetListOptions() metav1.ListOptions })
	if !ok {
		return true, nil, fmt.Errorf("list action does not expose options")
	}
	options := optionsAction.GetListOptions()
	selector := listAction.GetListRestrictions().Labels
	if selector == nil {
		selector = labels.Everything()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ListError != nil {
		return true, nil, s.ListError
	}
	if s.ExpireContinue && options.Continue != "" {
		return true, nil, apierrors.NewResourceExpired("pod list continue token expired")
	}

	rv := s.listRV
	if rv == "" {
		for _, set := range s.statefulSets {
			rv = set.ResourceVersion
			break
		}
	}
	start, err := parseContinue(options.Continue, rv)
	if err != nil {
		return true, nil, apierrors.NewResourceExpired("pod list continue token expired")
	}

	pods := make([]*corev1.Pod, 0, len(s.pods))
	for _, pod := range s.pods {
		if pod.Namespace == namespace && selector.Matches(labels.Set(pod.Labels)) {
			pods = append(pods, pod.DeepCopy())
		}
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })

	limit := options.Limit
	if limit <= 0 {
		limit = int64(len(pods))
	}
	end := start + int(limit)
	if end > len(pods) {
		end = len(pods)
	}
	items := make([]corev1.Pod, 0, end-start)
	for _, pod := range pods[start:end] {
		items = append(items, *pod)
	}

	result := &corev1.PodList{
		ListMeta: metav1.ListMeta{ResourceVersion: rv},
		Items:    items,
	}
	if end < len(pods) {
		result.Continue = makeContinue(rv, end)
		remaining := int64(len(pods) - end)
		result.RemainingItemCount = &remaining
	}
	if s.PartialList {
		remaining := int64(1)
		result.Continue = ""
		result.RemainingItemCount = &remaining
	}
	s.Actions = append(s.Actions, "list pods continue="+options.Continue)
	return true, result, nil
}

func (s *FakeAPIServer) updateScale(action ktesting.Action) (bool, runtime.Object, error) {
	update, ok := action.(ktesting.UpdateAction)
	if !ok {
		return true, nil, fmt.Errorf("unexpected scale update action %T", action)
	}
	scale, ok := update.GetObject().(*autoscalingv1.Scale)
	if !ok {
		return true, nil, fmt.Errorf("unexpected scale update object %T", update.GetObject())
	}
	namespace, name := action.GetNamespace(), scale.Name

	s.mu.Lock()
	defer s.mu.Unlock()
	set := s.statefulSets[objectKey(namespace, name)]
	s.Actions = append(s.Actions, "update statefulsets/"+name+"/scale")
	if set == nil {
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, name)
	}
	if scale.UID != set.UID || scale.ResourceVersion != set.ResourceVersion {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, name, fmt.Errorf("stale resourceVersion"))
	}
	if s.UpdateScaleError != nil {
		return true, nil, s.UpdateScaleError
	}
	if s.UpdateScaleMode == UpdateScaleTimeoutBeforeCommit {
		return true, nil, apierrors.NewTimeoutError("scale update timed out before commit", 0)
	}
	if replicas(set) != scale.Spec.Replicas {
		*set.Spec.Replicas = scale.Spec.Replicas
		set.Generation++
		set.ResourceVersion = s.allocateRVLocked()
		if s.ObserveGenerationOnScale {
			set.Status.ObservedGeneration = set.Generation
		}
	}
	result := &autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: namespace, UID: set.UID, ResourceVersion: set.ResourceVersion,
		},
		Spec: autoscalingv1.ScaleSpec{Replicas: replicas(set)},
	}
	if s.UpdateScaleMode == UpdateScaleApplyThenTimeout {
		return true, result, apierrors.NewTimeoutError("scale update timed out after commit", 0)
	}
	return true, result, nil
}

func (s *FakeAPIServer) patchStatefulSet(action ktesting.Action) (bool, runtime.Object, error) {
	patch, ok := action.(ktesting.PatchAction)
	if !ok {
		return true, nil, fmt.Errorf("unexpected statefulset patch action %T", action)
	}
	namespace, name := action.GetNamespace(), patch.GetName()

	var body struct {
		Metadata struct {
			ResourceVersion string
			Annotations     map[string]string
		}
	}
	if err := json.Unmarshal(patch.GetPatch(), &body); err != nil {
		return true, nil, apierrors.NewBadRequest("invalid merge patch")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	set := s.statefulSets[objectKey(namespace, name)]
	s.Actions = append(s.Actions, "patch statefulsets/"+name)
	if set == nil {
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, name)
	}
	if s.PatchHonorsResourceVersion && body.Metadata.ResourceVersion != set.ResourceVersion {
		return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "statefulsets"}, name, fmt.Errorf("stale resourceVersion"))
	}
	if s.PatchError != nil {
		return true, nil, s.PatchError
	}
	if s.PatchMode == PatchTimeoutBeforeCommit {
		return true, nil, apierrors.NewTimeoutError("barrier patch timed out before commit", 0)
	}

	next := set.DeepCopy()
	if next.Annotations == nil {
		next.Annotations = make(map[string]string)
	}
	for key, value := range body.Metadata.Annotations {
		next.Annotations[key] = value
	}
	if !sameAnnotations(set.Annotations, next.Annotations) {
		set.Annotations = next.Annotations
		set.ResourceVersion = s.allocateRVLocked()
	}
	result := set.DeepCopy()
	if s.PatchMode == PatchApplyThenTimeout {
		return true, result, apierrors.NewTimeoutError("barrier patch timed out after commit", 0)
	}
	return true, result, nil
}

func (s *FakeAPIServer) allocateRVLocked() string {
	s.nextRV++
	return strconv.FormatInt(s.nextRV, 10)
}

func objectKey(namespace, name string) string {
	return namespace + "/" + name
}

func makeContinue(rv string, offset int) string {
	return rv + ":" + strconv.Itoa(offset)
}

func parseContinue(token, rv string) (int, error) {
	if token == "" {
		return 0, nil
	}
	prefix := rv + ":"
	if !strings.HasPrefix(token, prefix) {
		return 0, fmt.Errorf("continue token resourceVersion mismatch")
	}
	offset, err := strconv.Atoi(strings.TrimPrefix(token, prefix))
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid continue offset")
	}
	return offset, nil
}

func replicas(set *appsv1.StatefulSet) int32 {
	if set == nil || set.Spec.Replicas == nil {
		return -1
	}
	return *set.Spec.Replicas
}

func sameAnnotations(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func sameObjectMetadataAndSpec(left, right *appsv1.StatefulSet) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.UID == right.UID &&
		left.ResourceVersion == right.ResourceVersion &&
		left.Generation == right.Generation &&
		left.Spec.Replicas != nil && right.Spec.Replicas != nil &&
		*left.Spec.Replicas == *right.Spec.Replicas &&
		sameAnnotations(left.Annotations, right.Annotations)
}
