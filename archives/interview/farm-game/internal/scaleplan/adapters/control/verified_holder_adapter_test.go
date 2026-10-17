package control

import (
	"context"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	routecore "farmgame/internal/routecontrol"
	"farmgame/internal/routecontrol/verifiedholder"
	core "farmgame/internal/scaleplan"
)

type verifiedExclusions struct {
	values map[string]routecore.PlacementExclusion
}

func (e verifiedExclusions) GetPlacementExclusion(_ context.Context, domain actorrouting.RouteDomain, holderID, bootID string) (routecore.PlacementExclusion, bool, error) {
	value, found := e.values[string(domain)+"/"+holderID+"/"+bootID]
	return value, found, nil
}

func TestVerifiedHolderReaderAdmitsOnlyFreshZeroShardWithoutLegacyExclusion(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	view := verifiedholder.NewStore(func() time.Time { return now })
	key := verifiedholder.WorkloadKey{Realm: "farm", Domain: actorrouting.DomainFarm, Namespace: "game", Kind: "StatefulSet", Name: "farm-svr"}
	holder := verifiedholder.Holder{
		WorkloadKey: key, WorkloadUID: "workload-1", Domain: actorrouting.DomainFarm,
		HolderID: "farm-svr-1", BootID: "boot-1", PodName: "farm-svr-1", PodUID: "pod-1", Ordinal: 1,
		LogicEndpoint: "198.18.0.199:9000", ControlEndpoint: "198.18.0.199:19101", TLSDNSIdentity: "farm-svr.game.svc",
		Lifecycle: verifiedholder.LifecycleReadyZeroShard, ProcessReady: true,
		ObservedRouteRevision: 42, CapacityWeight: 1, AttestationHash: "sha256:abc",
		VerifiedAt: now.Add(-time.Second), VerifiedUntil: now.Add(time.Minute),
	}
	if err := view.ReplaceWorkload(verifiedholder.WorkloadSnapshot{
		Key: key, WorkloadUID: "workload-1", KubeGeneration: 2, KubeResourceVersion: "rv-2",
		KubeListResourceVersion: "list-rv-2", DiscoveryRevision: 1, CapturedAt: now, Holders: []verifiedholder.Holder{holder},
	}); err != nil {
		t.Fatal(err)
	}
	reader, err := NewVerifiedHolderStateReader(VerifiedHolderStateReaderOptions{View: view, Exclusions: verifiedExclusions{values: map[string]routecore.PlacementExclusion{}}, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	state, found, err := reader.GetHolder(context.Background(), structHolderRef(actorrouting.DomainFarm, "farm-svr-1", 1))
	if err != nil || !found {
		t.Fatalf("GetHolder found=%v err=%v", found, err)
	}
	if !state.PlacementEligible || state.PlacementRevision != 1 || !state.AssignmentKnownFresh || state.SnapshotRevision != 42 || state.Ref.BootID != "boot-1" {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestVerifiedHolderReaderPrefersRegisteredLogicEndpointOverPodIP(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	view := verifiedholder.NewStore(func() time.Time { return now })
	key := verifiedholder.WorkloadKey{Realm: "farm", Domain: actorrouting.DomainFarm, Namespace: "game", Kind: "StatefulSet", Name: "farm-svr"}
	holder := verifiedholder.Holder{
		WorkloadKey: key, WorkloadUID: "workload-1", Domain: actorrouting.DomainFarm,
		HolderID: "farm-svr-2", BootID: "boot-2", PodName: "farm-svr-2", PodUID: "pod-2", Ordinal: 2,
		LogicEndpoint: "198.18.0.200:9090", ControlEndpoint: "198.18.0.200:19101", TLSDNSIdentity: "holder-control.game.svc",
		Lifecycle: verifiedholder.LifecycleReadyZeroShard, ProcessReady: true,
		ObservedRouteRevision: 42, CapacityWeight: 1, AttestationHash: "sha256:abc",
		VerifiedAt: now.Add(-time.Second), VerifiedUntil: now.Add(time.Minute),
	}
	if err := view.ReplaceWorkload(verifiedholder.WorkloadSnapshot{
		Key: key, WorkloadUID: "workload-1", KubeGeneration: 2, KubeResourceVersion: "rv-2",
		KubeListResourceVersion: "list-rv-2", DiscoveryRevision: 1, CapturedAt: now, Holders: []verifiedholder.Holder{holder},
	}); err != nil {
		t.Fatal(err)
	}
	registry := verifiedRecords{values: map[string]routecore.HolderRecord{
		"FARM/farm-svr-2": {
			Domain: actorrouting.DomainFarm, HolderID: "farm-svr-2", BootID: "boot-2",
			LogicEndpoint:         "farm-svr-2.farm-svr-headless.game.svc.cluster.local:9090",
			HolderControlEndpoint: "farm-svr-2.farm-svr-headless.game.svc.cluster.local:19101",
			TLSDNSIdentity:        "holder-control.game.svc",
		},
	}}
	reader, err := NewVerifiedHolderStateReader(VerifiedHolderStateReaderOptions{
		View: view, Exclusions: verifiedExclusions{values: map[string]routecore.PlacementExclusion{}},
		Registry: registry, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	state, found, err := reader.GetHolder(context.Background(), structHolderRef(actorrouting.DomainFarm, "farm-svr-2", 2))
	if err != nil || !found {
		t.Fatalf("GetHolder found=%v err=%v", found, err)
	}
	want := "farm-svr-2.farm-svr-headless.game.svc.cluster.local:9090"
	if state.LogicEndpoint != want {
		t.Fatalf("LogicEndpoint=%s, want registered DNS %s", state.LogicEndpoint, want)
	}
}

func TestVerifiedHolderReaderFailsClosedForServingHolderWithoutExclusion(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	view := verifiedholder.NewStore(func() time.Time { return now })
	key := verifiedholder.WorkloadKey{Realm: "farm", Domain: actorrouting.DomainFarm, Namespace: "game", Kind: "StatefulSet", Name: "farm-svr"}
	holder := verifiedholder.Holder{
		WorkloadKey: key, WorkloadUID: "workload-1", Domain: actorrouting.DomainFarm,
		HolderID: "farm-svr-0", BootID: "boot-0", PodName: "farm-svr-0", PodUID: "pod-0", Ordinal: 0,
		LogicEndpoint: "198.18.0.2:9000", ControlEndpoint: "198.18.0.2:19101", TLSDNSIdentity: "farm-svr.game.svc",
		Lifecycle: verifiedholder.LifecycleServing, ProcessReady: true, ServingShardCount: 64,
		ObservedRouteRevision: 42, CapacityWeight: 1, AttestationHash: "sha256:def",
		VerifiedAt: now.Add(-time.Second), VerifiedUntil: now.Add(time.Minute),
	}
	if err := view.ReplaceWorkload(verifiedholder.WorkloadSnapshot{
		Key: key, WorkloadUID: "workload-1", KubeGeneration: 1, KubeResourceVersion: "rv-1",
		KubeListResourceVersion: "list-rv-1", DiscoveryRevision: 1, CapturedAt: now, Holders: []verifiedholder.Holder{holder},
	}); err != nil {
		t.Fatal(err)
	}
	reader, err := NewVerifiedHolderStateReader(VerifiedHolderStateReaderOptions{View: view, Exclusions: verifiedExclusions{values: map[string]routecore.PlacementExclusion{}}, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	state, found, err := reader.GetHolder(context.Background(), structHolderRef(actorrouting.DomainFarm, "farm-svr-0", 0))
	if err != nil || !found {
		t.Fatalf("GetHolder found=%v err=%v", found, err)
	}
	if state.PlacementEligible || state.PlacementRevision != 0 {
		t.Fatalf("serving holder without exclusion must fail closed: %+v", state)
	}
}

func TestVerifiedHolderReaderExactLookupKeepsUnreadyServingEvidenceOutOfPlacement(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	view := verifiedholder.NewStore(func() time.Time { return now })
	key := verifiedholder.WorkloadKey{Realm: "farm", Domain: actorrouting.DomainFarm, Namespace: "game", Kind: "StatefulSet", Name: "farm-svr"}
	holder := verifiedholder.Holder{
		WorkloadKey: key, WorkloadUID: "workload-1", Domain: actorrouting.DomainFarm,
		HolderID: "farm-svr-0", BootID: "boot-0", PodName: "farm-svr-0", PodUID: "pod-0", Ordinal: 0,
		LogicEndpoint: "198.18.0.2:9000", ControlEndpoint: "198.18.0.2:19101", TLSDNSIdentity: "farm-svr.game.svc",
		Lifecycle: verifiedholder.LifecycleServing, ProcessReady: false, ServingShardCount: 64,
		ObservedRouteRevision: 42, CapacityWeight: 1, AttestationHash: "sha256:def",
		VerifiedAt: now.Add(-time.Second), VerifiedUntil: now.Add(time.Minute),
	}
	if err := view.ReplaceWorkload(verifiedholder.WorkloadSnapshot{
		Key: key, WorkloadUID: "workload-1", KubeGeneration: 1, KubeResourceVersion: "rv-1",
		KubeListResourceVersion: "list-rv-1", DiscoveryRevision: 1, CapturedAt: now, Holders: []verifiedholder.Holder{holder},
	}); err != nil {
		t.Fatal(err)
	}
	exclusion := routecore.PlacementExclusion{Domain: actorrouting.DomainFarm, HolderID: holder.HolderID, BootID: holder.BootID, Revision: 1}
	reader, err := NewVerifiedHolderStateReader(VerifiedHolderStateReaderOptions{
		View: view, Exclusions: verifiedExclusions{values: map[string]routecore.PlacementExclusion{
			"FARM/farm-svr-0/boot-0": exclusion,
		}}, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	state, found, err := reader.GetHolder(context.Background(), core.HolderRef{
		Domain: actorrouting.DomainFarm, HolderID: holder.HolderID, BootID: holder.BootID,
		PodUID: holder.PodUID, WorkloadUID: holder.WorkloadUID, Ordinal: holder.Ordinal,
	})
	if err != nil || !found || state.ProcessReady || state.State != core.HolderServing {
		t.Fatalf("exact control lookup found=%v state=%+v err=%v", found, state, err)
	}
	listed, err := reader.ListDomainHolders(context.Background(), actorrouting.DomainFarm)
	if err != nil || len(listed) != 0 {
		t.Fatalf("unready serving holder leaked into placement list: len=%d err=%v", len(listed), err)
	}
}

func structHolderRef(domain actorrouting.RouteDomain, holderID string, ordinal int32) core.HolderRef {
	return core.HolderRef{Domain: domain, HolderID: holderID, Ordinal: ordinal}
}

type verifiedRecords struct {
	values map[string]routecore.HolderRecord
}

func (r verifiedRecords) Get(_ context.Context, domain actorrouting.RouteDomain, holderID string) (routecore.HolderRecord, bool, error) {
	value, found := r.values[string(domain)+"/"+holderID]
	return value, found, nil
}

func (r verifiedRecords) List(context.Context, actorrouting.RouteDomain) ([]routecore.HolderRecord, error) {
	return nil, nil
}
