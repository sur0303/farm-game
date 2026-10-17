package scaleplan_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"farmgame/internal/actorrouting"
	platform "farmgame/internal/platform/kubernetes/scaleplan"
	core "farmgame/internal/scaleplan"
	testkit "farmgame/internal/testkit/kubernetes/scaleplan"
)

func TestGetStatefulSetBracketsIdentityAndPaginatesCanonicalPods(t *testing.T) {
	ref := workloadRef()
	set := statefulSetFixture()
	pods := podFixtures(set, 3)
	server, client := testkit.NewFakeClient(set, pods...)
	adapter := platform.NewAdapter(client, platform.WithPageSize(1))

	snapshot, err := adapter.GetSnapshot(context.Background(), ref)
	if err != nil {
		t.Fatalf("GetSnapshot() error = %v", err)
	}
	if snapshot.UID != "workload-uid" || snapshot.ResourceVersion != "7" ||
		snapshot.Generation != 4 || snapshot.ObservedGeneration != 4 ||
		snapshot.SpecReplicas != 3 || snapshot.StatusReplicas != 3 ||
		snapshot.ReadyReplicas != 2 || snapshot.ListResourceVersion != "7" {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	if len(snapshot.Pods) != 3 {
		t.Fatalf("pod count = %d, want 3", len(snapshot.Pods))
	}
	for ordinal, pod := range snapshot.Pods {
		if pod.Ordinal != int32(ordinal) || pod.PodName != "player-"+strconv.Itoa(ordinal) ||
			pod.PodUID != "pod-"+strconv.Itoa(ordinal) || pod.WorkloadUID != "workload-uid" ||
			pod.HolderID != pod.PodName || !pod.Present {
			t.Fatalf("pod[%d] is not canonical: %#v", ordinal, pod)
		}
	}
	actions := server.ActionsSnapshot()
	if countActions(actions, "list pods") != 3 {
		t.Fatalf("list actions = %#v, want three paginated calls", actions)
	}
}

func TestGetRejectsNonCanonicalPodIdentity(t *testing.T) {
	tests := []struct {
		name string
		edit func(*corev1.Pod)
		want error
	}{
		{
			name: "owner uid",
			edit: func(pod *corev1.Pod) {
				pod.OwnerReferences[0].UID = types.UID("other-workload")
			},
			want: platform.ErrOwnerReferenceMismatch,
		},
		{
			name: "missing pod index",
			edit: func(pod *corev1.Pod) {
				delete(pod.Labels, platform.PodIndexLabel)
			},
			want: platform.ErrPodIdentityMismatch,
		},
		{
			name: "non canonical pod index",
			edit: func(pod *corev1.Pod) {
				pod.Labels[platform.PodIndexLabel] = "01"
			},
			want: platform.ErrPodIdentityMismatch,
		},
		{
			name: "pod name ordinal mismatch",
			edit: func(pod *corev1.Pod) {
				pod.Name = "player-9"
			},
			want: platform.ErrPodIdentityMismatch,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ref := workloadRef()
			set := statefulSetFixture()
			pod := podFixtures(set, 1)[0]
			test.edit(pod)
			_, client := testkit.NewFakeClient(set, pod)
			_, err := platform.NewAdapter(client).Get(context.Background(), ref)
			if !errors.Is(err, test.want) {
				t.Fatalf("Get() error = %v, want errors.Is(..., %v)", err, test.want)
			}
		})
	}
}

func TestGetRejectsPartialAndExpiredPodPages(t *testing.T) {
	ref := workloadRef()
	t.Run("partial final page", func(t *testing.T) {
		set := statefulSetFixture()
		server, client := testkit.NewFakeClient(set, podFixtures(set, 2)...)
		server.PartialList = true
		_, err := platform.NewAdapter(client, platform.WithPageSize(1)).Get(context.Background(), ref)
		if !errors.Is(err, platform.ErrListSnapshotInconsistent) {
			t.Fatalf("Get() error = %v, want partial-list rejection", err)
		}
	})
	t.Run("expired continue token", func(t *testing.T) {
		set := statefulSetFixture()
		server, client := testkit.NewFakeClient(set, podFixtures(set, 2)...)
		server.ExpireContinue = true
		_, err := platform.NewAdapter(client, platform.WithPageSize(1)).Get(context.Background(), ref)
		if !apierrors.IsResourceExpired(err) {
			t.Fatalf("Get() error = %v, want expired continue token", err)
		}
	})
}

func TestScaleScaleSubresourceTimeoutIsUnknown(t *testing.T) {
	ref := workloadRef()
	set := statefulSetFixture()
	server, client := testkit.NewFakeClient(set)
	server.ScaleGetError = apierrors.NewTimeoutError("scale subresource unavailable", 0)
	result, err := platform.NewAdapter(client).Scale(context.Background(), ref, scalePrecondition(), 4)
	if !errors.Is(err, core.ErrOutcomeUnknown) || result.Committed {
		t.Fatalf("Scale() result=%#v err=%v, want unknown", result, err)
	}
}

func TestGetRejectsStatefulSetDriftAcrossBracket(t *testing.T) {
	ref := workloadRef()
	set := statefulSetFixture()
	server, client := testkit.NewFakeClient(set, podFixtures(set, 1)...)
	server.SetGetHook(func(server *testkit.FakeAPIServer, count int) {
		if count == 1 {
			server.AdvanceStatefulSet(ref.Namespace, ref.Name, func(set *appsv1.StatefulSet) {
				set.Annotations = map[string]string{"third-party": "write"}
			})
		}
	})
	_, err := platform.NewAdapter(client).Get(context.Background(), ref)
	if !errors.Is(err, core.ErrConflict) {
		t.Fatalf("Get() error = %v, want conflict", err)
	}
}

func TestScaleUsesExactStatefulSetScaleCASAndConfirmation(t *testing.T) {
	ref := workloadRef()
	set := statefulSetFixture()
	server, client := testkit.NewFakeClient(set, podFixtures(set, 3)...)
	adapter := platform.NewAdapter(client)

	result, err := adapter.Scale(context.Background(), ref, core.ScalePrecondition{
		WorkloadUID:      "workload-uid",
		ResourceVersion:  "7",
		ExpectedReplicas: 3,
	}, 4)
	if err != nil {
		t.Fatalf("Scale() error = %v", err)
	}
	if !result.Committed || result.State.Replicas != 4 {
		t.Fatalf("unexpected scale result: %#v", result)
	}
	current := server.CurrentStatefulSet(ref.Namespace, ref.Name)
	if current == nil || *current.Spec.Replicas != 4 || current.Generation != 5 ||
		current.Status.ObservedGeneration != 5 || current.ResourceVersion == "7" {
		t.Fatalf("unexpected current StatefulSet: %#v", current)
	}
	actions := server.ActionsSnapshot()
	want := []string{
		"get statefulsets/player",
		"get statefulsets/player/scale",
		"update statefulsets/player/scale",
		"get statefulsets/player",
	}
	if !sameStrings(actions, want) {
		t.Fatalf("actions = %#v, want %#v", actions, want)
	}
}

func TestScaleRejectsNoopAndUnsupportedScaleMetadata(t *testing.T) {
	ref := workloadRef()
	set := statefulSetFixture()
	server, client := testkit.NewFakeClient(set)
	adapter := platform.NewAdapter(client)
	_, err := adapter.Scale(context.Background(), ref, core.ScalePrecondition{
		WorkloadUID: "workload-uid", ResourceVersion: "7", ExpectedReplicas: 3,
	}, 3)
	if !errors.Is(err, platform.ErrNoopScale) {
		t.Fatalf("no-op Scale() error = %v, want ErrNoopScale", err)
	}
	if len(server.ActionsSnapshot()) != 0 {
		t.Fatalf("no-op scale issued actions: %#v", server.ActionsSnapshot())
	}

	server.ScaleMetadataAvailable = false
	_, err = adapter.Scale(context.Background(), ref, core.ScalePrecondition{
		WorkloadUID: "workload-uid", ResourceVersion: "7", ExpectedReplicas: 3,
	}, 4)
	if !errors.Is(err, platform.ErrUnsupportedScaleMetadata) {
		t.Fatalf("unsupported scale metadata error = %v", err)
	}
	if current := server.CurrentStatefulSet(ref.Namespace, ref.Name); *current.Spec.Replicas != 3 {
		t.Fatalf("unsupported metadata changed replicas to %d", *current.Spec.Replicas)
	}
}

func TestScaleTimeoutBeforeAndAfterCommitRemainUnknown(t *testing.T) {
	ref := workloadRef()
	t.Run("before commit", func(t *testing.T) {
		set := statefulSetFixture()
		server, client := testkit.NewFakeClient(set)
		server.UpdateScaleMode = testkit.UpdateScaleTimeoutBeforeCommit
		result, err := platform.NewAdapter(client).Scale(context.Background(), ref, scalePrecondition(), 4)
		if !errors.Is(err, core.ErrOutcomeUnknown) || result.Committed {
			t.Fatalf("Scale() result=%#v err=%v, want unknown and not committed", result, err)
		}
		if current := server.CurrentStatefulSet(ref.Namespace, ref.Name); *current.Spec.Replicas != 3 {
			t.Fatalf("before-commit timeout changed replicas to %d", *current.Spec.Replicas)
		}
	})
	t.Run("after commit", func(t *testing.T) {
		set := statefulSetFixture()
		server, client := testkit.NewFakeClient(set)
		server.UpdateScaleMode = testkit.UpdateScaleApplyThenTimeout
		result, err := platform.NewAdapter(client).Scale(context.Background(), ref, scalePrecondition(), 4)
		if !errors.Is(err, core.ErrOutcomeUnknown) || result.Committed {
			t.Fatalf("Scale() result=%#v err=%v, want unknown and not committed", result, err)
		}
		if current := server.CurrentStatefulSet(ref.Namespace, ref.Name); *current.Spec.Replicas != 4 {
			t.Fatalf("after-commit timeout did not apply replicas: %d", *current.Spec.Replicas)
		}
	})
}

func TestScaleCancellationReturnsUnknown(t *testing.T) {
	ref := workloadRef()
	set := statefulSetFixture()
	server, client := testkit.NewFakeClient(set)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := platform.NewAdapter(client).Scale(ctx, ref, scalePrecondition(), 4)
	if !errors.Is(err, core.ErrOutcomeUnknown) || result.Committed {
		t.Fatalf("Scale() result=%#v err=%v, want unknown and not committed", result, err)
	}
	if current := server.CurrentStatefulSet(ref.Namespace, ref.Name); *current.Spec.Replicas != 4 {
		t.Fatalf("canceled scale should remain observable as applied/unknown, got %d", *current.Spec.Replicas)
	}
}

func TestCancellationBarrierAdvancesRVWithoutGenerationAndClosesNotCommitted(t *testing.T) {
	ref := workloadRef()
	set := statefulSetFixture()
	server, client := testkit.NewFakeClient(set)
	nonceCalls := 0
	adapter := platform.NewAdapter(client, platform.WithBarrierNonceGenerator(func() string {
		nonceCalls++
		return "nonce-" + strconv.Itoa(nonceCalls)
	}))
	result, err := adapter.ApplyCancellationBarrier(context.Background(), pendingOperation(ref))
	if err != nil {
		t.Fatalf("ApplyCancellationBarrier() error = %v", err)
	}
	if result.Status != platform.BarrierNotCommitted ||
		result.BarrierOperationStatus != platform.BarrierCommitted ||
		result.OwnershipRetained {
		t.Fatalf("unexpected barrier result: %#v", result)
	}
	current := server.CurrentStatefulSet(ref.Namespace, ref.Name)
	if current.ResourceVersion == "7" || current.Generation != 4 || *current.Spec.Replicas != 3 ||
		current.Annotations[platform.BarrierNonceAnnotation] != "nonce-1" {
		t.Fatalf("barrier changed unsafe fields or did not advance RV: %#v", current)
	}
}

func TestBarrierNoopPayloadKeepsPendingAndUsesNewNonce(t *testing.T) {
	ref := workloadRef()
	set := statefulSetFixture()
	set.Annotations = map[string]string{
		platform.BarrierOperationAnnotation: "operation-1",
		platform.BarrierFenceAnnotation:     "fence-1",
		platform.BarrierNonceAnnotation:     "nonce-1",
	}
	server, client := testkit.NewFakeClient(set)
	adapter := platform.NewAdapter(client, platform.WithBarrierNonceGenerator(func() string { return "nonce-2" }))
	result, err := adapter.ApplyCancellationBarrier(context.Background(), pendingOperationWithNonce(ref, "nonce-1"))
	if !errors.Is(err, core.ErrOutcomeUnknown) || result.Status != platform.BarrierPending ||
		result.BarrierOperationStatus != platform.BarrierNotCommitted || !result.OwnershipRetained ||
		result.NextAttemptNonce != "nonce-2" {
		t.Fatalf("unexpected no-op barrier result: %#v err=%v", result, err)
	}
	if current := server.CurrentStatefulSet(ref.Namespace, ref.Name); current.ResourceVersion != "7" {
		t.Fatalf("no-op barrier advanced RV to %s", current.ResourceVersion)
	}
}

func TestBarrierResolutionRowsKeepScalePendingWhenProofIsIncomplete(t *testing.T) {
	ref := workloadRef()

	t.Run("nonce equal closes barrier and not committed scale", func(t *testing.T) {
		set := statefulSetFixture()
		set.ResourceVersion = "8"
		set.Annotations = map[string]string{
			platform.BarrierOperationAnnotation: "operation-1",
			platform.BarrierFenceAnnotation:     "fence-1",
			platform.BarrierNonceAnnotation:     "nonce-1",
		}
		_, client := testkit.NewFakeClient(set)
		adapter := platform.NewAdapter(client)
		result, err := adapter.ResolveBarrierOperation(context.Background(), pendingOperationWithNonce(ref, "nonce-1"))
		if err != nil || result.Status != platform.BarrierNotCommitted ||
			result.BarrierOperationStatus != platform.BarrierCommitted || result.OwnershipRetained {
			t.Fatalf("result=%#v err=%v", result, err)
		}
	})
	t.Run("same RV closes barrier as not committed and registers replacement", func(t *testing.T) {
		set := statefulSetFixture()
		_, client := testkit.NewFakeClient(set)
		adapter := platform.NewAdapter(client, platform.WithBarrierNonceGenerator(func() string { return "nonce-2" }))
		result, err := adapter.ResolveBarrierOperation(context.Background(), pendingOperationWithNonce(ref, "nonce-1"))
		if !errors.Is(err, core.ErrOutcomeUnknown) || result.Status != platform.BarrierPending ||
			result.BarrierOperationStatus != platform.BarrierNotCommitted || !result.OwnershipRetained ||
			result.NextAttemptNonce != "nonce-2" {
			t.Fatalf("result=%#v err=%v", result, err)
		}
	})
	t.Run("different RV closes barrier as superseded and keeps scale pending", func(t *testing.T) {
		set := statefulSetFixture()
		set.ResourceVersion = "8"
		set.Annotations = map[string]string{"third-party": "write"}
		_, client := testkit.NewFakeClient(set)
		adapter := platform.NewAdapter(client, platform.WithBarrierNonceGenerator(func() string { return "nonce-2" }))
		result, err := adapter.ResolveBarrierOperation(context.Background(), pendingOperationWithNonce(ref, "nonce-1"))
		if !errors.Is(err, core.ErrOutcomeUnknown) || result.Status != platform.BarrierPending ||
			result.BarrierOperationStatus != platform.BarrierSuperseded || !result.OwnershipRetained {
			t.Fatalf("result=%#v err=%v", result, err)
		}
	})
	t.Run("third replica value is unknown and ownership retained", func(t *testing.T) {
		set := statefulSetFixture()
		*set.Spec.Replicas = 9
		set.ResourceVersion = "8"
		_, client := testkit.NewFakeClient(set)
		adapter := platform.NewAdapter(client)
		result, err := adapter.ResolveBarrierOperation(context.Background(), pendingOperationWithNonce(ref, "nonce-1"))
		if !errors.Is(err, core.ErrOutcomeUnknown) || result.Status != platform.BarrierUnknown || !result.OwnershipRetained {
			t.Fatalf("result=%#v err=%v", result, err)
		}
	})
}

func TestBarrierUIDDriftAndStalePatchRetainOwnership(t *testing.T) {
	ref := workloadRef()
	t.Run("uid drift", func(t *testing.T) {
		set := statefulSetFixture()
		set.UID = "replacement-uid"
		_, client := testkit.NewFakeClient(set)
		adapter := platform.NewAdapter(client)
		result, err := adapter.ResolveBarrierOperation(context.Background(), pendingOperation(ref))
		if !errors.Is(err, core.ErrConflict) || result.Status != platform.BarrierConflict || !result.OwnershipRetained {
			t.Fatalf("result=%#v err=%v", result, err)
		}
	})
	t.Run("stale patch 409", func(t *testing.T) {
		set := statefulSetFixture()
		server, client := testkit.NewFakeClient(set)
		server.SetGetHook(func(server *testkit.FakeAPIServer, count int) {
			if count == 1 {
				server.AdvanceStatefulSet(ref.Namespace, ref.Name, func(set *appsv1.StatefulSet) {
					set.Annotations = map[string]string{"third-party": "write"}
				})
			}
		})
		adapter := platform.NewAdapter(client, platform.WithBarrierNonceGenerator(func() string { return "nonce-2" }))
		result, err := adapter.ApplyCancellationBarrier(context.Background(), pendingOperation(ref))
		if !errors.Is(err, core.ErrOutcomeUnknown) || result.Status != platform.BarrierPending ||
			result.BarrierOperationStatus != platform.BarrierSuperseded || !result.OwnershipRetained {
			t.Fatalf("result=%#v err=%v", result, err)
		}
		if result.NextResourceVersion == "" || result.NextAttemptNonce != "nonce-2-next" {
			t.Fatalf("stale patch did not return replacement barrier attempt: %#v", result)
		}
	})
}

func TestPendingScaleRejectsNoopBeforeBarrierRegistration(t *testing.T) {
	ref := workloadRef()
	set := statefulSetFixture()
	_, client := testkit.NewFakeClient(set)
	adapter := platform.NewAdapter(client)
	operation := pendingOperation(ref)
	operation.DesiredReplicas = operation.ExpectedReplicas
	result, err := adapter.ApplyCancellationBarrier(context.Background(), operation)
	if !errors.Is(err, core.ErrInvalidPlan) || result.Status != platform.BarrierConflict {
		t.Fatalf("result=%#v err=%v, want pre-registration rejection", result, err)
	}
}

func workloadRef() core.WorkloadRef {
	return core.WorkloadRef{
		Realm: "farm-realm", Domain: actorrouting.DomainPlayer,
		Namespace: "game", Kind: "StatefulSet", Name: "player",
	}
}

func statefulSetFixture() *appsv1.StatefulSet {
	replicas := int32(3)
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "game", Name: "player", UID: "workload-uid",
			ResourceVersion: "7", Generation: 4,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:       &replicas,
			Selector:       &metav1.LabelSelector{MatchLabels: map[string]string{"app": "player"}},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
		},
		Status: appsv1.StatefulSetStatus{
			Replicas: 3, ReadyReplicas: 2, ObservedGeneration: 4,
			CurrentRevision: "revision-1", UpdateRevision: "revision-1",
		},
	}
}

func podFixtures(set *appsv1.StatefulSet, count int) []*corev1.Pod {
	pods := make([]*corev1.Pod, 0, count)
	controller := true
	for ordinal := 0; ordinal < count; ordinal++ {
		name := "player-" + strconv.Itoa(ordinal)
		pods = append(pods, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: set.Namespace, Name: name,
				UID: types.UID("pod-" + strconv.Itoa(ordinal)),
				Labels: map[string]string{
					"app": "player", platform.PodIndexLabel: strconv.Itoa(ordinal),
					platform.ControllerRevisionLabel: "revision-1",
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1", Kind: "StatefulSet", Name: set.Name,
					UID: set.UID, Controller: &controller,
				}},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "player", Image: "registry.test/player@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}},
			Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "player", ImageID: "registry.test/player@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}, Conditions: []corev1.PodCondition{{
				Type: corev1.PodReady, Status: corev1.ConditionTrue,
			}}},
		})
	}
	return pods
}

func scalePrecondition() core.ScalePrecondition {
	return core.ScalePrecondition{
		WorkloadUID: "workload-uid", ResourceVersion: "7", ExpectedReplicas: 3,
	}
}

func pendingOperation(ref core.WorkloadRef) platform.PendingScaleOperation {
	return pendingOperationWithNonce(ref, "")
}

func pendingOperationWithNonce(ref core.WorkloadRef, nonce string) platform.PendingScaleOperation {
	return platform.PendingScaleOperation{
		Ref: ref, WorkloadUID: "workload-uid",
		ExpectedResourceVersion: "7", ResourceVersion: "7",
		ExpectedReplicas: 3, DesiredReplicas: 4,
		OperationID: "operation-1", FenceEpoch: "fence-1",
		AttemptNonce: nonce, Nonce: nonce, ExpectedGeneration: 4,
	}
}

func countActions(actions []string, prefix string) int {
	count := 0
	for _, action := range actions {
		if len(action) >= len(prefix) && action[:len(prefix)] == prefix {
			count++
		}
	}
	return count
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
