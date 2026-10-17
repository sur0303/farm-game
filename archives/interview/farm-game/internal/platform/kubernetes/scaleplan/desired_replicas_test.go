package scaleplan

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/scaleplan"
)

func TestDesiredReplicaReaderUsesSafeIntentAnnotation(t *testing.T) {
	ref := core.WorkloadRef{Realm: "realm-a", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	objects := []runtime.Object{&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: ref.Namespace, Name: ref.Name, UID: types.UID("uid-a"), ResourceVersion: "10",
		Annotations: map[string]string{SafeDesiredReplicasAnnotation: "2"},
	}}}
	reader, err := NewDesiredReplicaReader(fake.NewSimpleClientset(objects...), SafeDesiredReplicasAnnotation)
	if err != nil {
		t.Fatal(err)
	}
	desired, configured, err := reader.DesiredReplicas(context.Background(), ref)
	if err != nil || !configured || desired != 2 {
		t.Fatalf("desired=%d configured=%t err=%v", desired, configured, err)
	}
}

func TestDesiredReplicaReaderRejectsNonCanonicalIntent(t *testing.T) {
	ref := core.WorkloadRef{Realm: "realm-a", Domain: actorrouting.DomainPlayer, Namespace: "farm", Kind: "StatefulSet", Name: "player-svr"}
	set := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: ref.Namespace, Name: ref.Name, UID: types.UID("uid-a"), ResourceVersion: "10",
		Annotations: map[string]string{SafeDesiredReplicasAnnotation: "02"},
	}}
	reader, err := NewDesiredReplicaReader(fake.NewSimpleClientset(set), SafeDesiredReplicasAnnotation)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := reader.DesiredReplicas(context.Background(), ref); err == nil {
		t.Fatal("non-canonical desired replicas were accepted")
	}
}

func TestDesiredReplicaReaderAcknowledgesReachedIntentConditionally(t *testing.T) {
	ref := core.WorkloadRef{Realm: "realm-a", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	set := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: ref.Namespace, Name: ref.Name, UID: types.UID("uid-a"), ResourceVersion: "10",
		Annotations: map[string]string{SafeDesiredReplicasAnnotation: "2", "keep": "yes"},
	}}
	client := fake.NewSimpleClientset(set)
	reader, err := NewDesiredReplicaReader(client, SafeDesiredReplicasAnnotation)
	if err != nil {
		t.Fatal(err)
	}
	observed := core.WorkloadState{Ref: ref, UID: "uid-a", ResourceVersion: "10", Replicas: 2}
	if err := reader.AcknowledgeDesiredReplicas(context.Background(), ref, observed, 2); err != nil {
		t.Fatal(err)
	}
	current, err := client.AppsV1().StatefulSets(ref.Namespace).Get(context.Background(), ref.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, found := current.Annotations[SafeDesiredReplicasAnnotation]; found || current.Annotations["keep"] != "yes" {
		t.Fatalf("annotations=%v", current.Annotations)
	}
}

func TestDesiredReplicaReaderDoesNotDeleteConcurrentIntent(t *testing.T) {
	ref := core.WorkloadRef{Realm: "realm-a", Domain: actorrouting.DomainPlayer, Namespace: "farm", Kind: "StatefulSet", Name: "player-svr"}
	set := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Namespace: ref.Namespace, Name: ref.Name, UID: types.UID("uid-a"), ResourceVersion: "11",
		Annotations: map[string]string{SafeDesiredReplicasAnnotation: "3"},
	}}
	reader, err := NewDesiredReplicaReader(fake.NewSimpleClientset(set), SafeDesiredReplicasAnnotation)
	if err != nil {
		t.Fatal(err)
	}
	stale := core.WorkloadState{Ref: ref, UID: "uid-a", ResourceVersion: "10", Replicas: 2}
	if err := reader.AcknowledgeDesiredReplicas(context.Background(), ref, stale, 2); err == nil {
		t.Fatal("stale observation removed a concurrent intent")
	}
}
