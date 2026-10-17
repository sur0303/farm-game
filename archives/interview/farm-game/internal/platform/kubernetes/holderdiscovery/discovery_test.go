package holderdiscovery

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"farmgame/internal/actorrouting"
	kubescale "farmgame/internal/platform/kubernetes/scaleplan"
	"farmgame/internal/routecontrol/verifiedholder"
	scaleplancore "farmgame/internal/scaleplan"
)

type staticSnapshotProvider struct {
	snapshot kubescale.KubernetesIdentitySnapshot
	err      error
}

type conflictThenSnapshotProvider struct {
	snapshot  kubescale.KubernetesIdentitySnapshot
	conflicts int
	calls     int
}

func (p staticSnapshotProvider) GetIdentity(context.Context, scaleplancore.WorkloadRef) (kubescale.KubernetesIdentitySnapshot, error) {
	return p.snapshot, p.err
}

func (p *conflictThenSnapshotProvider) GetIdentity(context.Context, scaleplancore.WorkloadRef) (kubescale.KubernetesIdentitySnapshot, error) {
	p.calls++
	if p.calls <= p.conflicts {
		return kubescale.KubernetesIdentitySnapshot{}, scaleplancore.ErrConflict
	}
	return p.snapshot, nil
}

func TestDiscoveryRetriesOnlyCompleteIdentityCaptureConflicts(t *testing.T) {
	want := kubescale.KubernetesIdentitySnapshot{UID: "workload-1", ResourceVersion: "rv-2"}
	provider := &conflictThenSnapshotProvider{snapshot: want, conflicts: 1}
	discovery := &Discovery{adapter: provider, workload: scaleplancore.WorkloadRef{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}}
	got, err := discovery.captureStableIdentity(context.Background())
	if err != nil || got.UID != want.UID || provider.calls != 2 {
		t.Fatalf("snapshot=%+v calls=%d err=%v", got, provider.calls, err)
	}

	persistent := &conflictThenSnapshotProvider{conflicts: identityCaptureAttempts}
	discovery.adapter = persistent
	if _, err := discovery.captureStableIdentity(context.Background()); !errors.Is(err, scaleplancore.ErrConflict) || persistent.calls != identityCaptureAttempts {
		t.Fatalf("persistent calls=%d err=%v", persistent.calls, err)
	}
}

func TestReconcileBuildsAtomicVerifiedView(t *testing.T) {
	now := time.Unix(1000, 0)
	workload := scaleplancore.WorkloadRef{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "farm-svr-1", Namespace: "farm", UID: types.UID("pod-1"), ResourceVersion: "pod-rv"},
		Spec: corev1.PodSpec{ServiceAccountName: "farm-svr"}, Status: corev1.PodStatus{PodIP: "198.18.0.199"}}
	client := fake.NewSimpleClientset(pod)
	view := verifiedholder.NewStore(func() time.Time { return now })
	provider := staticSnapshotProvider{snapshot: kubescale.KubernetesIdentitySnapshot{
		Ref: workload, Namespace: "farm", Name: "farm-svr", UID: "workload-1", ResourceVersion: "sts-rv", Generation: 2,
		ListResourceVersion: "list-rv", Pods: []scaleplancore.WorkloadPodState{{PodName: "farm-svr-1", PodUID: "pod-1", WorkloadUID: "workload-1", Ordinal: 1, HolderID: "farm-svr-1", Present: true, Ready: true}},
	}}
	discovery, err := New(Options{
		Client: client, Workload: workload, SnapshotProvider: provider, View: view, Audience: "farm-holder",
		Now: func() time.Time { return now },
		EndpointHost: func(podName, namespace string) string {
			return podName + ".farm-svr-headless." + namespace + ".svc.cluster.local"
		},
		Verifier: RuntimeIdentityVerifierFunc(func(context.Context, ObservedPod) (RuntimeIdentity, error) {
			return RuntimeIdentity{Domain: actorrouting.DomainFarm, HolderID: "farm-svr-1", BootID: "boot-1", WorkloadUID: "workload-1", PodUID: "pod-1", Ordinal: 1,
				ProcessReady: true, Lifecycle: verifiedholder.LifecycleReadyZeroShard, CapacityWeight: 1, ObservedAt: now, WorkloadIdentityToken: "bound-token"}, nil
		}),
		Attestor: WorkloadIdentityAttestorFunc(func(context.Context, AttestationRequest) (Attestation, error) {
			return Attestation{SubjectHash: "sha256:ok"}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := discovery.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.ObservedReadyPods != 1 || result.VerifiedHolders != 1 || result.RejectedHolders != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	key := verifiedholder.WorkloadKey{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	snapshot, found := view.Snapshot(key)
	if !found || len(snapshot.Holders) != 1 ||
		snapshot.Holders[0].LogicEndpoint != "farm-svr-1.farm-svr-headless.farm.svc.cluster.local:9000" ||
		snapshot.Holders[0].ControlEndpoint != "farm-svr-1.farm-svr-headless.farm.svc.cluster.local:9001" {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
}

func TestReconcileVerifiesUnreadyWarmingControlEndpoint(t *testing.T) {
	now := time.Unix(1000, 0)
	workload := scaleplancore.WorkloadRef{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "farm-svr-1", Namespace: "farm", UID: types.UID("pod-1"), ResourceVersion: "pod-rv"},
		Spec: corev1.PodSpec{ServiceAccountName: "farm-svr"}, Status: corev1.PodStatus{PodIP: "198.18.0.199"}}
	provider := staticSnapshotProvider{snapshot: kubescale.KubernetesIdentitySnapshot{
		Ref: workload, UID: "workload-1", ResourceVersion: "sts-rv", Generation: 2, ListResourceVersion: "list-rv",
		Pods: []scaleplancore.WorkloadPodState{{PodName: "farm-svr-1", PodUID: "pod-1", WorkloadUID: "workload-1", Ordinal: 1, HolderID: "farm-svr-1", Present: true, Ready: false}},
	}}
	view := verifiedholder.NewStore(func() time.Time { return now })
	discovery, err := New(Options{
		Client: fake.NewSimpleClientset(pod), Workload: workload, SnapshotProvider: provider, View: view, Audience: "farm-holder", Now: func() time.Time { return now },
		Verifier: RuntimeIdentityVerifierFunc(func(context.Context, ObservedPod) (RuntimeIdentity, error) {
			return RuntimeIdentity{Domain: actorrouting.DomainFarm, HolderID: "farm-svr-1", BootID: "boot-1", WorkloadUID: "workload-1", PodUID: "pod-1", Ordinal: 1,
				ProcessReady: false, Lifecycle: verifiedholder.LifecycleWarming, CapacityWeight: 1, ObservedAt: now, WorkloadIdentityToken: "bound-token"}, nil
		}),
		Attestor: WorkloadIdentityAttestorFunc(func(context.Context, AttestationRequest) (Attestation, error) {
			return Attestation{SubjectHash: "sha256:ok"}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := discovery.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.ObservedReadyPods != 0 || result.VerifiedHolders != 1 || result.RejectedHolders != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	holders := view.ListVerifiedDomain(actorrouting.DomainFarm)
	if len(holders) != 1 || holders[0].Lifecycle != verifiedholder.LifecycleWarming || holders[0].ProcessReady {
		t.Fatalf("warming generation missing from verified view: %+v", holders)
	}
}

func TestReconcileFailsClosedWhenReadyPodCannotVerify(t *testing.T) {
	now := time.Unix(1000, 0)
	workload := scaleplancore.WorkloadRef{Realm: "r1", Domain: actorrouting.DomainPlayer, Namespace: "farm", Kind: "StatefulSet", Name: "player-svr"}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "player-svr-0", Namespace: "farm", UID: types.UID("pod-0"), ResourceVersion: "pod-rv"},
		Spec: corev1.PodSpec{ServiceAccountName: "player-svr"}, Status: corev1.PodStatus{PodIP: "198.18.0.2"}}
	provider := staticSnapshotProvider{snapshot: kubescale.KubernetesIdentitySnapshot{Ref: workload, UID: "workload", ResourceVersion: "rv", Generation: 1, ListResourceVersion: "list-rv",
		Pods: []scaleplancore.WorkloadPodState{{PodName: "player-svr-0", PodUID: "pod-0", WorkloadUID: "workload", HolderID: "player-svr-0", Present: true, Ready: true}}}}
	view := verifiedholder.NewStore(func() time.Time { return now })
	discovery, err := New(Options{Client: fake.NewSimpleClientset(pod), Workload: workload, SnapshotProvider: provider, View: view, Audience: "farm-holder", Now: func() time.Time { return now },
		Verifier: RuntimeIdentityVerifierFunc(func(context.Context, ObservedPod) (RuntimeIdentity, error) {
			return RuntimeIdentity{}, errors.New("unreachable")
		}),
		Attestor: WorkloadIdentityAttestorFunc(func(context.Context, AttestationRequest) (Attestation, error) { return Attestation{}, nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := discovery.Reconcile(context.Background())
	if !errors.Is(err, ErrNoVerifiedReadyHolders) || result.RejectedHolders != 1 {
		t.Fatalf("expected fail-closed result, got %+v err=%v", result, err)
	}
	key := verifiedholder.WorkloadKey{Realm: "r1", Domain: actorrouting.DomainPlayer, Namespace: "farm", Kind: "StatefulSet", Name: "player-svr"}
	snapshot, found := view.Snapshot(key)
	if !found || len(snapshot.Holders) != 0 {
		t.Fatalf("unverified pod leaked into view: %+v", snapshot)
	}
}
