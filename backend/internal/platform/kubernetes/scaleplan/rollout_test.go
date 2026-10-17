package scaleplan

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/scaleplan"
	"farmgame/internal/scaleplan/autorebalance"
)

const rolloutTestDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestRolloutManagerPersistsCheckpointAndDeletesOnlyExactPodUID(t *testing.T) {
	ctx := context.Background()
	ref := core.WorkloadRef{Realm: "realm-a", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	replicas := int32(2)
	set := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace, UID: types.UID("workload-a"), ResourceVersion: "10",
			Annotations: map[string]string{ManagedRolloutIntentAnnotation: mustIntentJSON(t, replicas)}},
		Spec: appsv1.StatefulSetSpec{Replicas: &replicas, UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: ref.Name, Image: "registry.test/farm-svr@" + rolloutTestDigest}}}}},
		Status: appsv1.StatefulSetStatus{CurrentRevision: "revision-1", UpdateRevision: "revision-2"},
	}
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "farm-svr-1", Namespace: ref.Namespace, UID: types.UID("pod-old"),
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: ref.Name, UID: set.UID, Controller: &controller}}}}
	client := fake.NewSimpleClientset(set, pod)
	manager, err := NewRolloutManager(client, ManagedRolloutAnnotation, ManagedRolloutIntentAnnotation, SafeDesiredReplicasAnnotation)
	if err != nil {
		t.Fatal(err)
	}
	observed := core.WorkloadState{Ref: ref, UID: "workload-a", ResourceVersion: "10", Replicas: 2,
		UpdateRevision: "revision-2", UpdateStrategy: string(appsv1.OnDeleteStatefulSetStrategyType)}
	state, err := manager.Begin(ctx, ref, observed, "revision-2")
	if err != nil {
		t.Fatal(err)
	}
	if state.Phase != autorebalance.RolloutPhaseExpanding || state.VictimOrdinal != -1 {
		t.Fatalf("begin=%+v", state)
	}
	if err := manager.EnsureDesiredReplicas(ctx, ref, state, 3); err != nil {
		t.Fatal(err)
	}
	current, err := client.AppsV1().StatefulSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil || replicasOf(current) != 3 {
		t.Fatalf("surge replicas=%d err=%v", replicasOf(current), err)
	}
	if _, exists := current.Annotations[SafeDesiredReplicasAnnotation]; exists {
		t.Fatal("managed surge leaked into ordinary safe desired replicas")
	}
	if err := manager.EnsureDesiredReplicas(ctx, ref, state, 3); err != nil {
		t.Fatalf("idempotent surge: %v", err)
	}

	draining := state
	draining.Phase = autorebalance.RolloutPhaseDraining
	if state, err = manager.Update(ctx, ref, state, draining); err != nil {
		t.Fatal(err)
	}
	bound := state
	bound.VictimHolderID, bound.VictimPodUID, bound.VictimBootID = "farm-svr-1", "pod-old", "boot-old"
	bound.VictimOrdinal, bound.DrainPlanID = 1, "drain-plan-a"
	if state, err = manager.Update(ctx, ref, state, bound); err != nil {
		t.Fatal(err)
	}
	deleting := state
	deleting.Phase = autorebalance.RolloutPhaseDeleting
	deleting.ExclusionRevision = 2
	if state, err = manager.Update(ctx, ref, state, deleting); err != nil {
		t.Fatal(err)
	}
	if done, err := manager.DeleteVictimPod(ctx, ref, state); err != nil || done {
		t.Fatalf("first delete done=%t err=%v", done, err)
	}
	if _, err := client.CoreV1().Pods(ref.Namespace).Get(ctx, pod.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("victim still exists: %v", err)
	}
	if done, err := manager.DeleteVictimPod(ctx, ref, state); err != nil || !done {
		t.Fatalf("idempotent delete done=%t err=%v", done, err)
	}

	waiting := state
	waiting.Phase = autorebalance.RolloutPhaseWaitingReplacement
	if state, err = manager.Update(ctx, ref, state, waiting); err != nil {
		t.Fatal(err)
	}
	draining = state
	draining.Phase = autorebalance.RolloutPhaseDraining
	draining.VictimHolderID, draining.VictimPodUID, draining.VictimBootID = "", "", ""
	draining.VictimOrdinal, draining.DrainPlanID, draining.ExclusionRevision = -1, "", 0
	if state, err = manager.Update(ctx, ref, state, draining); err != nil {
		t.Fatal(err)
	}
	contracting := state
	contracting.Phase = autorebalance.RolloutPhaseContracting
	if state, err = manager.Update(ctx, ref, state, contracting); err != nil {
		t.Fatal(err)
	}
	if err := manager.Complete(ctx, ref, state); err != nil {
		t.Fatal(err)
	}
	if _, found, err := manager.Observe(ctx, ref); err != nil || found {
		t.Fatalf("rollout checkpoint remains found=%t err=%v", found, err)
	}
}

func TestRolloutManagerDoesNotDeleteReplacementWithDifferentUID(t *testing.T) {
	ctx := context.Background()
	ref := core.WorkloadRef{Realm: "realm-a", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	state := autorebalance.RolloutState{
		SchemaVersion: autorebalance.RolloutSchemaVersion, RolloutID: "rollout-a", ReleaseID: "release-a",
		WorkloadUID: "workload-a", SourceRevision: "revision-1", TargetRevision: "revision-2",
		TargetImageDigest: rolloutTestDigest, ExpectedReplicas: 2, OriginalReplicas: 2,
		StartedAtMs: 1, DeadlineAtMs: time.Now().Add(time.Hour).UnixMilli(),
		IntentHash:     "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Phase:          autorebalance.RolloutPhaseDeleting,
		VictimHolderID: "farm-svr-1", VictimPodUID: "pod-old", VictimBootID: "boot-old", VictimOrdinal: 1, DrainPlanID: "plan-a",
		ExclusionRevision: 2,
	}
	intentRaw := mustIntentJSON(t, 2)
	intent, intentHash, err := decodeRolloutIntent(intentRaw)
	if err != nil {
		t.Fatal(err)
	}
	state.IntentHash, state.DeadlineAtMs = intentHash, intent.DeadlineAtMs
	spareReplicas := int32(3)
	set := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace, UID: types.UID("workload-a"), ResourceVersion: "10",
			Annotations: map[string]string{ManagedRolloutAnnotation: mustRolloutJSON(t, state), ManagedRolloutIntentAnnotation: intentRaw}},
		Spec: appsv1.StatefulSetSpec{Replicas: &spareReplicas, UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: ref.Name, Image: "registry.test/farm-svr@" + rolloutTestDigest}}}}},
		Status: appsv1.StatefulSetStatus{CurrentRevision: "revision-1", UpdateRevision: "revision-2"},
	}
	controller := true
	replacement := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "farm-svr-1", Namespace: ref.Namespace, UID: types.UID("pod-new"),
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: ref.Name, UID: set.UID, Controller: &controller}}}}
	client := fake.NewSimpleClientset(set, replacement)
	manager, err := NewRolloutManager(client, ManagedRolloutAnnotation, ManagedRolloutIntentAnnotation, SafeDesiredReplicasAnnotation)
	if err != nil {
		t.Fatal(err)
	}
	done, err := manager.DeleteVictimPod(ctx, ref, state)
	if err != nil || !done {
		t.Fatalf("done=%t err=%v", done, err)
	}
	if _, err := client.CoreV1().Pods(ref.Namespace).Get(ctx, replacement.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("replacement was deleted: %v", err)
	}
}

func TestRolloutManagerRequiresExplicitIntentBeforeBeginning(t *testing.T) {
	ctx := context.Background()
	ref := core.WorkloadRef{Realm: "realm-a", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	replicas := int32(2)
	set := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace,
		UID: types.UID("workload-a"), ResourceVersion: "10"},
		Spec: appsv1.StatefulSetSpec{Replicas: &replicas, UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: ref.Name, Image: "registry.test/farm-svr@" + rolloutTestDigest}}}}},
		Status: appsv1.StatefulSetStatus{CurrentRevision: "revision-1", UpdateRevision: "revision-2"}}
	manager, err := NewRolloutManager(fake.NewSimpleClientset(set), ManagedRolloutAnnotation, ManagedRolloutIntentAnnotation, SafeDesiredReplicasAnnotation)
	if err != nil {
		t.Fatal(err)
	}
	observed := core.WorkloadState{Ref: ref, UID: "workload-a", ResourceVersion: "10", Replicas: replicas,
		UpdateRevision: "revision-2", UpdateStrategy: string(appsv1.OnDeleteStatefulSetStrategyType)}
	if _, err := manager.Begin(ctx, ref, observed, "revision-2"); !errors.Is(err, autorebalance.ErrNoPlan) {
		t.Fatalf("revision drift started without intent: %v", err)
	}
}

func TestRolloutManagerConsumesExactAbortCommandAtomically(t *testing.T) {
	ctx := context.Background()
	ref := core.WorkloadRef{Realm: "realm-a", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	replicas := int32(2)
	intentRaw := mustIntentJSON(t, replicas)
	intent, intentHash, err := decodeRolloutIntent(intentRaw)
	if err != nil {
		t.Fatal(err)
	}
	blocked := autorebalance.RolloutState{
		SchemaVersion: autorebalance.RolloutSchemaVersion, RolloutID: "rollout-a", ReleaseID: "release-a",
		WorkloadUID: "workload-a", SourceRevision: "revision-1", TargetRevision: "revision-2",
		TargetImageDigest: rolloutTestDigest, ExpectedReplicas: replicas, OriginalReplicas: replicas,
		StartedAtMs: 1, DeadlineAtMs: intent.DeadlineAtMs, IntentHash: intentHash,
		Phase: autorebalance.RolloutPhaseBlocked, VictimOrdinal: -1,
		BlockedReason: "EXPANSION_REPLICA_DRIFT", BlockedFromPhase: autorebalance.RolloutPhaseExpanding, BlockedAtMs: 2,
	}
	recovery := RolloutRecovery{SchemaVersion: RolloutRecoverySchemaVersion, CommandID: "abort-1",
		RolloutID: blocked.RolloutID, IntentHash: blocked.IntentHash, Action: autorebalance.RolloutRecoveryAbort,
		DeadlineAtMs: time.Now().Add(time.Minute).UnixMilli()}
	recoveryRaw, _ := json.Marshal(recovery)
	set := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: ref.Name, Namespace: ref.Namespace,
		UID: types.UID("workload-a"), ResourceVersion: "10", Annotations: map[string]string{
			ManagedRolloutAnnotation: mustRolloutJSON(t, blocked), ManagedRolloutIntentAnnotation: intentRaw,
			ManagedRolloutRecoveryAnnotation: string(recoveryRaw),
		}}, Spec: appsv1.StatefulSetSpec{Replicas: &replicas,
		UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType},
		Template:       corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: ref.Name, Image: "registry.test/farm-svr@" + rolloutTestDigest}}}}},
		Status: appsv1.StatefulSetStatus{CurrentRevision: "revision-1", UpdateRevision: "revision-2"}}
	client := fake.NewSimpleClientset(set)
	manager, err := NewRolloutManager(client, ManagedRolloutAnnotation, ManagedRolloutIntentAnnotation, SafeDesiredReplicasAnnotation)
	if err != nil {
		t.Fatal(err)
	}
	request, found, err := manager.ObserveRecovery(ctx, ref)
	if err != nil || !found {
		t.Fatalf("recovery found=%t err=%v", found, err)
	}
	aborting := blocked
	aborting.Phase = autorebalance.RolloutPhaseAborting
	aborting.BlockedReason, aborting.BlockedFromPhase, aborting.BlockedAtMs = "", "", 0
	if _, err := manager.Recover(ctx, ref, blocked, request, aborting); err != nil {
		t.Fatal(err)
	}
	current, err := client.AppsV1().StatefulSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, found := current.Annotations[ManagedRolloutRecoveryAnnotation]; found {
		t.Fatal("recovery command was not consumed with checkpoint update")
	}
	if err := manager.Complete(ctx, ref, aborting); err != nil {
		t.Fatal(err)
	}
	if _, found, err := manager.Observe(ctx, ref); err != nil || found {
		t.Fatalf("aborted rollout remains found=%t err=%v", found, err)
	}
}

func mustIntentJSON(t *testing.T, replicas int32) string {
	t.Helper()
	raw, err := json.Marshal(RolloutIntent{SchemaVersion: RolloutIntentSchemaVersion, ReleaseID: "release-a",
		TargetImageDigest: rolloutTestDigest, ExpectedReplicas: replicas, DeadlineAtMs: time.Now().Add(time.Hour).UnixMilli()})
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func mustRolloutJSON(t *testing.T, state autorebalance.RolloutState) string {
	t.Helper()
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
