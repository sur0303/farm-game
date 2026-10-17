package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderidentity"
	"farmgame/internal/holderkey"
	routecore "farmgame/internal/routecontrol"
	"farmgame/internal/routecontrol/verifiedholder"
)

func TestVerifiedHolderPublicKeyResolverTracksExactBoot(t *testing.T) {
	now := time.Unix(100, 0)
	view := verifiedholder.NewStore(func() time.Time { return now })
	key := verifiedholder.WorkloadKey{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	makePublication := func() holderkey.Publication {
		public, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		publication, err := holderkey.New(public)
		if err != nil {
			t.Fatal(err)
		}
		return publication
	}
	oldPublication := makePublication()
	oldHolder := verifiedRoutePlanHolder(now, key, "farm-svr-1", "boot-old")
	oldHolder.CommitProofKey = oldPublication
	mustReplaceVerifiedHolder(t, view, key, 1, oldHolder)
	resolver := verifiedHolderPublicKeyResolver{view: view}
	got, found, err := resolver.ResolveHolderPublicKey(actorrouting.DomainFarm, oldHolder.HolderID, oldHolder.BootID)
	if err != nil || !found || !got.Equal(oldPublication) {
		t.Fatalf("old key found=%v got=%+v err=%v", found, got, err)
	}

	newPublication := makePublication()
	newHolder := verifiedRoutePlanHolder(now, key, "farm-svr-1", "boot-new")
	newHolder.CommitProofKey = newPublication
	mustReplaceVerifiedHolder(t, view, key, 2, newHolder)
	if _, found, err := resolver.ResolveHolderPublicKey(actorrouting.DomainFarm, oldHolder.HolderID, oldHolder.BootID); err != nil || found {
		t.Fatalf("retired boot key found=%v err=%v", found, err)
	}
	got, found, err = resolver.ResolveHolderPublicKey(actorrouting.DomainFarm, newHolder.HolderID, newHolder.BootID)
	if err != nil || !found || !got.Equal(newPublication) {
		t.Fatalf("new key found=%v got=%+v err=%v", found, got, err)
	}
}

func TestVerifiedRoutePlanHolderReaderUsesCurrentBootGeneration(t *testing.T) {
	now := time.Unix(100, 0)
	view := verifiedholder.NewStore(func() time.Time { return now })
	key := verifiedholder.WorkloadKey{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	mustReplaceVerifiedHolder(t, view, key, 1, verifiedRoutePlanHolder(now, key, "farm-svr-1", "boot-old"))
	readerValue, err := newVerifiedRoutePlanHolderReader(view)
	if err != nil {
		t.Fatal(err)
	}
	reader := readerValue.(*verifiedRoutePlanHolderReader)
	old, found, err := reader.Get(context.Background(), actorrouting.DomainFarm, "farm-svr-1")
	if err != nil || !found || old.BootID != "boot-old" {
		t.Fatalf("old=%+v found=%t err=%v", old, found, err)
	}

	mustReplaceVerifiedHolder(t, view, key, 2, verifiedRoutePlanHolder(now, key, "farm-svr-1", "boot-new"))
	current, found, err := reader.Get(context.Background(), actorrouting.DomainFarm, "farm-svr-1")
	if err != nil || !found || current.BootID != "boot-new" || current.HolderControlEndpoint == "" || current.SnapshotRevision != 10 {
		t.Fatalf("current=%+v found=%t err=%v", current, found, err)
	}
}

func TestVerifiedRoutePlanHolderReaderPrefersRegisteredLogicEndpointOverPodIP(t *testing.T) {
	now := time.Unix(100, 0)
	view := verifiedholder.NewStore(func() time.Time { return now })
	key := verifiedholder.WorkloadKey{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	mustReplaceVerifiedHolder(t, view, key, 1, verifiedRoutePlanHolder(now, key, "farm-svr-2", "boot-2"))
	registry := overlayRegistry{record: routecore.HolderRecord{
		Domain: actorrouting.DomainFarm, HolderID: "farm-svr-2", BootID: "boot-2",
		DeploymentIdentity:    holderidentity.DeploymentIdentity{PodUID: "farm-svr-2-uid", WorkloadUID: "workload-uid", Ordinal: 0},
		LogicEndpoint:         "farm-svr-2.farm-svr-headless.farm.svc.cluster.local:9090",
		HolderControlEndpoint: "farm-svr-2.farm-svr-headless.farm.svc.cluster.local:19101",
		TLSDNSIdentity:        "holder-control.farm.svc",
		State:                 routecore.HolderStateServing,
		ProcessReady:          true,
		ServingShardCount:     7,
		SnapshotRevision:      12,
		HeartbeatAt:           now,
		LeaseUntil:            now.Add(time.Minute),
		Sequence:              2,
		CapacityWeight:        1,
	}}
	reader, err := newVerifiedRoutePlanHolderReaderWithRegistry(view, registry)
	if err != nil {
		t.Fatal(err)
	}
	record, found, err := reader.Get(context.Background(), actorrouting.DomainFarm, "farm-svr-2")
	if err != nil || !found {
		t.Fatalf("found=%t err=%v", found, err)
	}
	want := "farm-svr-2.farm-svr-headless.farm.svc.cluster.local:9090"
	if record.LogicEndpoint != want {
		t.Fatalf("LogicEndpoint=%s, want %s", record.LogicEndpoint, want)
	}
	if record.SnapshotRevision != 12 || record.ServingShardCount != 7 || !record.ProcessReady {
		t.Fatalf("exact registered generation progress was not overlaid: %+v", record)
	}
}

func TestVerifiedRoutePlanHolderReaderDoesNotRegressToStaleMaterializedWarmingProgress(t *testing.T) {
	now := time.Unix(100, 0)
	view := verifiedholder.NewStore(func() time.Time { return now })
	key := verifiedholder.WorkloadKey{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	holder := verifiedRoutePlanHolder(now, key, "farm-svr-2", "boot-2")
	holder.Lifecycle = verifiedholder.LifecycleWarming
	holder.ProcessReady = false
	holder.ObservedRouteRevision = 12
	mustReplaceVerifiedHolder(t, view, key, 1, holder)
	registry := overlayRegistry{record: routecore.HolderRecord{
		Domain: actorrouting.DomainFarm, HolderID: "farm-svr-2", BootID: "boot-2",
		DeploymentIdentity:    holderidentity.DeploymentIdentity{PodUID: "farm-svr-2-uid", WorkloadUID: "workload-uid", Ordinal: 0},
		LogicEndpoint:         "farm-svr-2.farm-svr-headless.farm.svc.cluster.local:9090",
		HolderControlEndpoint: "farm-svr-2.farm-svr-headless.farm.svc.cluster.local:19101",
		TLSDNSIdentity:        "holder-control.farm.svc",
		State:                 routecore.HolderStateWarming,
		ProcessReady:          false,
		SnapshotRevision:      10,
		HeartbeatAt:           now,
		LeaseUntil:            now.Add(time.Minute),
		Sequence:              1,
		CapacityWeight:        1,
	}}
	reader, err := newVerifiedRoutePlanHolderReaderWithRegistry(view, registry)
	if err != nil {
		t.Fatal(err)
	}
	record, found, err := reader.Get(context.Background(), actorrouting.DomainFarm, "farm-svr-2")
	if err != nil || !found {
		t.Fatalf("found=%t err=%v", found, err)
	}
	if record.SnapshotRevision != 12 || record.State != routecore.HolderStateWarming || record.ProcessReady {
		t.Fatalf("stale materialized progress regressed verified holder: %+v", record)
	}
	if record.LogicEndpoint != registry.record.LogicEndpoint {
		t.Fatalf("canonical registered endpoint was not retained: %+v", record)
	}
}

func TestVerifiedRoutePlanHolderReaderRefreshesExactDomainBeforeRead(t *testing.T) {
	now := time.Unix(100, 0)
	view := verifiedholder.NewStore(func() time.Time { return now })
	key := verifiedholder.WorkloadKey{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	mustReplaceVerifiedHolder(t, view, key, 1, verifiedRoutePlanHolder(now, key, "farm-svr-1", "boot-1"))
	refreshes := 0
	readerValue, err := newVerifiedRoutePlanHolderReader(view, func(_ context.Context, domain actorrouting.RouteDomain) error {
		refreshes++
		if domain != actorrouting.DomainFarm {
			t.Fatalf("refresh domain=%s", domain)
		}
		holder := verifiedRoutePlanHolder(now, key, "farm-svr-1", "boot-1")
		holder.ObservedRouteRevision = 11
		mustReplaceVerifiedHolder(t, view, key, 2, holder)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	record, found, err := readerValue.Get(context.Background(), actorrouting.DomainFarm, "farm-svr-1")
	if err != nil || !found || record.SnapshotRevision != 11 || refreshes != 1 {
		t.Fatalf("record=%+v found=%t refreshes=%d err=%v", record, found, refreshes, err)
	}
}

func TestVerifiedRoutePlanHolderReaderRefreshFailureFailsClosed(t *testing.T) {
	now := time.Unix(100, 0)
	view := verifiedholder.NewStore(func() time.Time { return now })
	want := errors.New("refresh unavailable")
	reader, err := newVerifiedRoutePlanHolderReader(view, func(context.Context, actorrouting.RouteDomain) error { return want })
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := reader.Get(context.Background(), actorrouting.DomainPlayer, "player-svr-0"); !errors.Is(err, want) {
		t.Fatalf("Get error=%v, want refresh failure", err)
	}
}

func TestVerifiedRoutePlanHolderReaderRefreshFailureUsesUnexpiredExactCache(t *testing.T) {
	now := time.Unix(100, 0)
	view := verifiedholder.NewStore(func() time.Time { return now })
	key := verifiedholder.WorkloadKey{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	mustReplaceVerifiedHolder(t, view, key, 1, verifiedRoutePlanHolder(now, key, "farm-svr-1", "boot-1"))
	want := errors.New("refresh unavailable")
	reader, err := newVerifiedRoutePlanHolderReader(view, func(context.Context, actorrouting.RouteDomain) error { return want })
	if err != nil {
		t.Fatal(err)
	}
	record, found, err := reader.Get(context.Background(), actorrouting.DomainFarm, "farm-svr-1")
	if err != nil || !found || record.BootID != "boot-1" {
		t.Fatalf("record=%+v found=%t err=%v", record, found, err)
	}
}

func TestSwitchableRoutePlanHolderReaderChangesProviderAtomically(t *testing.T) {
	now := time.Unix(100, 0)
	legacyView := verifiedholder.NewStore(func() time.Time { return now })
	kubeView := verifiedholder.NewStore(func() time.Time { return now })
	key := verifiedholder.WorkloadKey{Realm: "r1", Domain: actorrouting.DomainPlayer, Namespace: "farm", Kind: "StatefulSet", Name: "player-svr"}
	mustReplaceVerifiedHolder(t, legacyView, key, 1, verifiedRoutePlanHolder(now, key, "player-svr-0", "legacy-boot"))
	mustReplaceVerifiedHolder(t, kubeView, key, 1, verifiedRoutePlanHolder(now, key, "player-svr-0", "kube-boot"))
	legacy, _ := newVerifiedRoutePlanHolderReader(legacyView)
	kube, _ := newVerifiedRoutePlanHolderReader(kubeView)
	reader, err := newSwitchableRoutePlanHolderReader(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Set(kube); err != nil {
		t.Fatal(err)
	}
	record, found, err := reader.Get(context.Background(), actorrouting.DomainPlayer, "player-svr-0")
	if err != nil || !found || record.BootID != "kube-boot" {
		t.Fatalf("record=%+v found=%t err=%v", record, found, err)
	}
}

func TestMaterializeVerifiedPlacementCandidatesRebuildsMissingExactGeneration(t *testing.T) {
	now := time.Unix(100, 0)
	view := verifiedholder.NewStore(func() time.Time { return now })
	key := verifiedholder.WorkloadKey{Realm: "r1", Domain: actorrouting.DomainPlayer, Namespace: "farm", Kind: "StatefulSet", Name: "player-svr"}
	mustReplaceVerifiedHolder(t, view, key, 1, verifiedRoutePlanHolder(now, key, "player-svr-2", "boot-2"))
	registry := &placementMaterializationRegistry{placements: make(map[string]routecore.PlacementExclusion)}
	if err := materializeVerifiedPlacementCandidates(context.Background(), view, registry); err != nil {
		t.Fatal(err)
	}
	if len(registry.registrations) != 1 {
		t.Fatalf("registrations=%d, want 1", len(registry.registrations))
	}
	candidate := registry.registrations[0]
	if candidate.BootID != "boot-2" || candidate.State != routecore.HolderStateWarming || candidate.ProcessReady || candidate.Draining || candidate.ServingShardCount != 0 {
		t.Fatalf("candidate=%+v", candidate)
	}
	if err := materializeVerifiedPlacementCandidates(context.Background(), view, registry); err != nil {
		t.Fatal(err)
	}
	if len(registry.registrations) != 1 {
		t.Fatalf("idempotent registrations=%d, want 1", len(registry.registrations))
	}
}

func TestMaterializeVerifiedCrashTargetInstallsWarmingGenerationIdempotently(t *testing.T) {
	now := time.Unix(100, 0)
	key := verifiedholder.WorkloadKey{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	holder := verifiedRoutePlanHolder(now, key, "farm-svr-2", "replacement-boot")
	holder.Lifecycle = verifiedholder.LifecycleWarming
	holder.ProcessReady = false
	registry := &placementMaterializationRegistry{placements: make(map[string]routecore.PlacementExclusion)}
	if err := materializeVerifiedPlacementCandidate(context.Background(), holder, registry); err != nil {
		t.Fatal(err)
	}
	if err := materializeVerifiedPlacementCandidate(context.Background(), holder, registry); err != nil {
		t.Fatal(err)
	}
	if len(registry.registrations) != 1 || registry.registrations[0].BootID != holder.BootID ||
		registry.registrations[0].State != routecore.HolderStateWarming || registry.registrations[0].ProcessReady {
		t.Fatalf("registrations=%+v", registry.registrations)
	}
}

func TestMaterializeVerifiedCrashTargetWaitsForPriorBootLease(t *testing.T) {
	now := time.Unix(100, 0)
	key := verifiedholder.WorkloadKey{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	holder := verifiedRoutePlanHolder(now, key, "farm-svr-2", "replacement-boot")
	holder.Lifecycle = verifiedholder.LifecycleWarming
	holder.ProcessReady = false
	registry := &placementMaterializationRegistry{
		placements: make(map[string]routecore.PlacementExclusion),
		registerErrors: []error{
			routecore.ErrHolderBootConflict,
			routecore.ErrHolderBootConflict,
		},
	}
	if err := materializeVerifiedPlacementCandidate(context.Background(), holder, registry); err != nil {
		t.Fatal(err)
	}
	if registry.registerCalls != 3 || len(registry.registrations) != 1 {
		t.Fatalf("register calls=%d registrations=%d, want 3/1", registry.registerCalls, len(registry.registrations))
	}
}

func TestMaterializeVerifiedCrashTargetDeadlineDoesNotOverwritePriorBoot(t *testing.T) {
	now := time.Unix(100, 0)
	key := verifiedholder.WorkloadKey{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	holder := verifiedRoutePlanHolder(now, key, "farm-svr-2", "replacement-boot")
	holder.Lifecycle = verifiedholder.LifecycleWarming
	holder.ProcessReady = false
	registry := &placementMaterializationRegistry{
		placements:     make(map[string]routecore.PlacementExclusion),
		registerErrors: []error{routecore.ErrHolderBootConflict},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	err := materializeVerifiedPlacementCandidate(ctx, holder, registry)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, routecore.ErrHolderBootConflict) {
		t.Fatalf("err=%v", err)
	}
	if registry.registerCalls != 1 || len(registry.registrations) != 0 || len(registry.placements) != 0 {
		t.Fatalf("register calls=%d registrations=%d placements=%d", registry.registerCalls, len(registry.registrations), len(registry.placements))
	}
}

func TestMaterializeVerifiedPlacementCandidatesPreservesExistingExclusion(t *testing.T) {
	now := time.Unix(100, 0)
	view := verifiedholder.NewStore(func() time.Time { return now })
	key := verifiedholder.WorkloadKey{Realm: "r1", Domain: actorrouting.DomainFarm, Namespace: "farm", Kind: "StatefulSet", Name: "farm-svr"}
	mustReplaceVerifiedHolder(t, view, key, 1, verifiedRoutePlanHolder(now, key, "farm-svr-1", "boot-1"))
	registry := &placementMaterializationRegistry{placements: map[string]routecore.PlacementExclusion{
		placementMaterializationKey(actorrouting.DomainFarm, "farm-svr-1", "boot-1"): {
			Domain: actorrouting.DomainFarm, HolderID: "farm-svr-1", BootID: "boot-1", Excluded: true, Revision: 7, OwnerPlanID: "drain-1",
		},
	}}
	if err := materializeVerifiedPlacementCandidates(context.Background(), view, registry); err != nil {
		t.Fatal(err)
	}
	if len(registry.registrations) != 0 {
		t.Fatalf("registrations=%d, want 0", len(registry.registrations))
	}
}

func TestMaterializeVerifiedPlacementCandidatesWaitsForPriorBootLease(t *testing.T) {
	now := time.Unix(100, 0)
	view := verifiedholder.NewStore(func() time.Time { return now })
	key := verifiedholder.WorkloadKey{Realm: "r1", Domain: actorrouting.DomainPlayer, Namespace: "farm", Kind: "StatefulSet", Name: "player-svr"}
	mustReplaceVerifiedHolder(t, view, key, 1, verifiedRoutePlanHolder(now, key, "player-svr-0", "boot-real"))
	registry := &placementMaterializationRegistry{
		placements: make(map[string]routecore.PlacementExclusion),
		registerErrors: []error{
			routecore.ErrHolderBootConflict,
			routecore.ErrHolderBootConflict,
		},
	}
	if err := materializeVerifiedPlacementCandidatesWithRetry(context.Background(), view, registry, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if registry.registerCalls != 3 {
		t.Fatalf("register calls=%d, want 3", registry.registerCalls)
	}
	if len(registry.registrations) != 1 || registry.registrations[0].BootID != "boot-real" {
		t.Fatalf("registrations=%+v", registry.registrations)
	}
}

func verifiedRoutePlanHolder(now time.Time, key verifiedholder.WorkloadKey, holderID, bootID string) verifiedholder.Holder {
	return verifiedholder.Holder{
		WorkloadKey: key, WorkloadUID: "workload-uid", Domain: key.Domain,
		HolderID: holderID, BootID: bootID, PodName: holderID, PodUID: holderID + "-uid", Ordinal: 0,
		LogicEndpoint: "198.18.0.2:9000", ControlEndpoint: "198.18.0.2:19101", TLSDNSIdentity: holderID,
		Lifecycle: verifiedholder.LifecycleReadyZeroShard, ProcessReady: true, CapacityWeight: 1,
		ObservedRouteRevision: 10, AttestationHash: "sha256:test", VerifiedAt: now.Add(-time.Second), VerifiedUntil: now.Add(time.Minute),
	}
}

func mustReplaceVerifiedHolder(t *testing.T, view *verifiedholder.Store, key verifiedholder.WorkloadKey, revision uint64, holder verifiedholder.Holder) {
	t.Helper()
	if err := view.ReplaceWorkload(verifiedholder.WorkloadSnapshot{
		Key: key, WorkloadUID: holder.WorkloadUID, KubeGeneration: 1,
		KubeResourceVersion: "rv", KubeListResourceVersion: "list-rv",
		DiscoveryRevision: revision, CapturedAt: holder.VerifiedAt, Holders: []verifiedholder.Holder{holder},
	}); err != nil {
		t.Fatal(err)
	}
}

type overlayRegistry struct {
	record routecore.HolderRecord
}

type placementMaterializationRegistry struct {
	placements     map[string]routecore.PlacementExclusion
	registrations  []routecore.HolderRegistration
	registerErrors []error
	registerCalls  int
}

func placementMaterializationKey(domain actorrouting.RouteDomain, holderID, bootID string) string {
	return string(domain) + "/" + holderID + "/" + bootID
}

func (r *placementMaterializationRegistry) GetPlacementExclusion(_ context.Context, domain actorrouting.RouteDomain, holderID, bootID string) (routecore.PlacementExclusion, bool, error) {
	value, found := r.placements[placementMaterializationKey(domain, holderID, bootID)]
	return value, found, nil
}

func (r *placementMaterializationRegistry) Register(_ context.Context, input routecore.HolderRegistration) (routecore.HolderRecord, error) {
	r.registerCalls++
	if len(r.registerErrors) > 0 {
		err := r.registerErrors[0]
		r.registerErrors = r.registerErrors[1:]
		return routecore.HolderRecord{}, err
	}
	r.registrations = append(r.registrations, input)
	key := placementMaterializationKey(input.Domain, input.HolderID, input.BootID)
	r.placements[key] = routecore.PlacementExclusion{Domain: input.Domain, HolderID: input.HolderID, BootID: input.BootID}
	return input, nil
}

func (o overlayRegistry) Get(context.Context, actorrouting.RouteDomain, string) (routecore.HolderRecord, bool, error) {
	return o.record, true, nil
}

func (o overlayRegistry) List(context.Context, actorrouting.RouteDomain) ([]routecore.HolderRecord, error) {
	return []routecore.HolderRecord{o.record}, nil
}
