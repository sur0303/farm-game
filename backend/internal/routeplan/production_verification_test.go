package routeplan

import (
	"context"
	"errors"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/actorrouting/authority"
	"farmgame/internal/authoritycomposition"
	"farmgame/internal/holderidentity"
	core "farmgame/internal/routecontrol"
	"farmgame/internal/routecontrol/gateverification"
	"farmgame/internal/storage/authoritystore"
)

type productionManagerFake struct {
	result  authority.TransitionObservation
	err     error
	request authority.TransitionObservationRequest
	calls   int
}

func (f *productionManagerFake) ObserveTransition(_ context.Context, request authority.TransitionObservationRequest) (authority.TransitionObservation, error) {
	f.request = request
	f.calls++
	return f.result, f.err
}

type productionGateFleetFake struct {
	observations []gateverification.PeerObservation
	err          error
	calls        int
	closed       int
	beforeReturn func()
}

func (f *productionGateFleetFake) ObserveShard(context.Context, string, actorrouting.RouteDomain, uint32) ([]gateverification.PeerObservation, error) {
	f.calls++
	if f.beforeReturn != nil {
		f.beforeReturn()
	}
	return append([]gateverification.PeerObservation(nil), f.observations...), f.err
}

func (f *productionGateFleetFake) Close() error { f.closed++; return nil }

type productionMemoryAuthorityReader struct{ store *authoritystore.MemoryStore }

func (r productionMemoryAuthorityReader) ReadShardWriteAuthority(_ context.Context, domain actorrouting.RouteDomain, shard uint32) (authoritystore.ShardWriteAuthority, bool, error) {
	value, found := r.store.InspectShardWriteAuthority(domain, shard)
	return value, found, nil
}

func productionVerificationFacts(t *testing.T) (Plan, authoritystore.ShardWriteAuthority, actorrouting.RouteSnapshot, authoritycomposition.ShardObservation) {
	t.Helper()
	now := time.Now().UnixMilli()
	plan := testPlan(now)
	plan.Phase = PhaseSucceeded
	plan.StartedAtMs = now
	plan.CompletedAtMs = now
	value := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "v1", HashVersion: actorrouting.HashVersionXXH64V2, HashSeed: 202608030001,
		Domain: plan.Domain, VirtualShard: plan.VirtualShard, HolderID: plan.TargetHolderID, HolderBootID: plan.TargetBootID,
		AssignmentRevision: 4, WriterEpoch: 5, LeaseEpoch: 6, LeaseUntilMs: now + 60_000,
		State: authoritystore.ShardAuthorityActive, LastTransitionID: plan.TransitionID, LastTransitionAtMs: now,
	}
	snapshot := actorrouting.RouteSnapshot{
		Realm: "realm-a", GlobalRevision: 9, HashVersion: value.HashVersion, HashSeed: value.HashSeed,
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: value.AssignmentRevision,
			ExpectedWriterEpoch: value.WriterEpoch, ExpectedLeaseEpoch: value.LeaseEpoch, LeaseUntilMs: value.LeaseUntilMs,
		}}},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 1, State: actorrouting.ShardStateActive, ShardRevision: 1,
			ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: value.LeaseUntilMs,
		}}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: plan.TargetHolderID, BootID: plan.TargetBootID, Endpoint: plan.TargetEndpoint, State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-a", BootID: "farm-boot", Endpoint: "farm:9000", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
	observed := authoritycomposition.ShardObservation{
		Realm: snapshot.Realm, Domain: plan.Domain, VirtualShard: plan.VirtualShard,
		HolderID: plan.TargetHolderID, HolderBootID: plan.TargetBootID, LogicEndpoint: plan.TargetEndpoint,
		State: actorrouting.ShardStateActive, AssignmentRevision: value.AssignmentRevision, WriterEpoch: value.WriterEpoch,
		LeaseEpoch: value.LeaseEpoch, LeaseUntilMs: value.LeaseUntilMs, SnapshotRevision: snapshot.GlobalRevision,
	}
	return plan, value, snapshot, observed
}

func productionManagerObservation(plan Plan, value authoritystore.ShardWriteAuthority, class authority.TransitionEvidenceClass) authority.TransitionObservation {
	result := authoritystore.ShardAuthorityTransitionResult{
		Status: authoritystore.ShardTransitionApplied, Authority: value, AcceptedTTLms: plan.RequestedTTLms,
	}
	fingerprint := "route-transition-fingerprint-v1:test"
	observation := authority.TransitionObservation{Class: class, Fingerprint: fingerprint, Result: result}
	switch class {
	case authority.TransitionEvidenceCompleteRecovery:
		observation.Recovery = authority.TransitionRecovery{
			TransitionID: plan.TransitionID, Fingerprint: fingerprint, Domain: plan.Domain, VirtualShard: plan.VirtualShard,
			ExpectedFence: plan.ExpectedSourceFence, NewHolderID: plan.TargetHolderID, NewHolderBootID: plan.TargetBootID,
			Endpoint: plan.TargetEndpoint, RequestedTTLms: plan.RequestedTTLms, InternalDrain: plan.Operation == OperationPlannedDrain,
			Result: result, LastState: actorrouting.ShardStateActive, Published: true, ColdLoaded: plan.Operation == OperationPlannedDrain,
			AuthorityAttempted: true, AuthorityAttemptedPresent: true, RecoveryWireVersion: authority.TransitionRecoveryWireAuthorityAttempted,
			Committed: true, CreatedAt: time.Now(), UpdatedAt: time.Now(),
		}
		if plan.Operation == OperationPlannedDrain {
			observation.Recovery.Mode = authority.TransferModeDrainHandoff
		}
		if plan.Operation == OperationCrashTakeover {
			observation.Recovery.Mode = authority.TransferModeCrashTakeover
			observation.Recovery.TakeoverProof = authority.TransferTakeoverProof{
				ExpectedHolderBootID: plan.TakeoverProof.ExpectedHolderBootID, HolderUnavailable: plan.TakeoverProof.HolderUnavailable,
				LeaseExpired: plan.TakeoverProof.LeaseExpired, Evidence: plan.TakeoverProof.Evidence,
			}
		}
	case authority.TransitionEvidenceTombstone:
		observation.Tombstone = authority.TransitionTombstone{
			TransitionID: plan.TransitionID, Fingerprint: fingerprint, Result: result, ExpiredAt: time.Now(),
		}
	}
	return observation
}

func TestManagerTransitionRequestForPlanSeparatesDrainAndCrash(t *testing.T) {
	plan, _, _, _ := productionVerificationFacts(t)
	drain, err := ManagerTransitionRequestForPlan(plan)
	if err != nil || !drain.PlannedDrain || drain.Transfer.Mode != authority.TransferModeUnspecified ||
		drain.Transfer.TakeoverProof != (authority.TransferTakeoverProof{}) || drain.Transfer.DeploymentIdentityPresent {
		t.Fatalf("drain=%+v err=%v", drain, err)
	}
	plan.Operation = OperationCrashTakeover
	plan.TakeoverProof = TakeoverProof{ExpectedHolderBootID: plan.SourceBootID, HolderUnavailable: true, Evidence: "process unavailable"}
	crash, err := ManagerTransitionRequestForPlan(plan)
	if err != nil || crash.PlannedDrain || crash.Transfer.Mode != authority.TransferModeCrashTakeover || crash.Transfer.DeploymentIdentityPresent ||
		crash.Transfer.TakeoverProof.ExpectedHolderBootID != plan.SourceBootID || !crash.Transfer.TakeoverProof.HolderUnavailable ||
		crash.Transfer.TakeoverProof.Evidence != plan.TakeoverProof.Evidence {
		t.Fatalf("crash=%+v err=%v", crash, err)
	}
}

func TestManagerTransitionRequestCarriesExactOwnedDeploymentIdentities(t *testing.T) {
	plan, _, _, _ := productionVerificationFacts(t)
	plan.SourceDeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-a", WorkloadUID: "workload", Ordinal: 0}
	plan.TargetDeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-b", WorkloadUID: "workload", Ordinal: 1}
	plan.OwnerScalePlanID = "scale-plan-1"
	plan.OwnerRetainUntilMs = plan.CreatedAtMs + time.Minute.Milliseconds()
	request, err := ManagerTransitionRequestForPlan(plan)
	if err != nil || !request.PlannedDrain || !request.Transfer.DeploymentIdentityPresent ||
		request.Transfer.SourcePodUID != "pod-a" || request.Transfer.SourceWorkloadUID != "workload" || request.Transfer.SourceOrdinal != 0 ||
		request.Transfer.TargetPodUID != "pod-b" || request.Transfer.TargetWorkloadUID != "workload" || request.Transfer.TargetOrdinal != 1 {
		t.Fatalf("request=%+v err=%v", request, err)
	}
}

func TestManagerTransitionRequestPinsOnlyExactTargetForCrashTakeover(t *testing.T) {
	plan, _, _, _ := productionVerificationFacts(t)
	plan.Operation = OperationCrashTakeover
	plan.SourceDeploymentIdentity = holderidentity.DeploymentIdentity{}
	plan.TargetDeploymentIdentity = holderidentity.DeploymentIdentity{PodUID: "pod-b", WorkloadUID: "workload", Ordinal: 1}
	plan.TakeoverProof = TakeoverProof{ExpectedHolderBootID: plan.SourceBootID, HolderUnavailable: true, Evidence: "source unavailable"}
	request, err := ManagerTransitionRequestForPlan(plan)
	if err != nil || request.PlannedDrain || !request.Transfer.DeploymentIdentityPresent ||
		request.Transfer.SourcePodUID != "" || request.Transfer.SourceWorkloadUID != "" || request.Transfer.SourceOrdinal != 0 ||
		request.Transfer.TargetPodUID != "pod-b" || request.Transfer.TargetWorkloadUID != "workload" || request.Transfer.TargetOrdinal != 1 {
		t.Fatalf("request=%+v err=%v", request, err)
	}
}

func TestProductionVerificationRejectsNilAndNoopComposition(t *testing.T) {
	if verification, err := NewProductionVerification(ProductionVerificationOptions{}); err == nil || verification != nil {
		t.Fatalf("nil production verification=%v err=%v", verification, err)
	}
	if IsProductionVerificationPort(executorVerificationFake{}) {
		t.Fatal("test noop was accepted as a production verification port")
	}
	var typedNil *ProductionVerification
	if IsProductionVerificationPort(typedNil) {
		t.Fatal("typed-nil production verification was accepted")
	}
}

func TestProductionVerificationRequiresEveryGateAndAllIndependentFacts(t *testing.T) {
	plan, value, snapshot, observed := productionVerificationFacts(t)
	manager := &productionManagerFake{result: productionManagerObservation(plan, value, authority.TransitionEvidenceCompleteRecovery)}
	gates := &productionGateFleetFake{observations: []gateverification.PeerObservation{
		{Peer: gateverification.Peer{Name: "gate-a"}, Observation: observed},
		{Peer: gateverification.Peer{Name: "gate-b"}, Observation: observed},
	}}
	verification, err := NewProductionVerification(ProductionVerificationOptions{Manager: manager, Gates: gates, ExpectedPeers: 2, CallTimeout: time.Second})
	if err != nil || !IsProductionVerificationPort(verification) {
		t.Fatalf("verification=%v err=%v", verification, err)
	}
	ctx := context.Background()
	if err := verification.VerifyGateResolution(ctx, plan, value, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := verification.VerifyTransition(ctx, plan, value, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := verification.VerifyRecovery(ctx, plan, value, snapshot); err != nil {
		t.Fatal(err)
	}
	manager.result = productionManagerObservation(plan, value, authority.TransitionEvidenceTombstone)
	if err := verification.VerifyTombstone(ctx, plan, value, snapshot); err != nil {
		t.Fatal(err)
	}
	if gates.calls != 1 || manager.calls != 3 || !manager.request.PlannedDrain {
		t.Fatalf("gate calls=%d manager calls=%d request=%+v", gates.calls, manager.calls, manager.request)
	}
	if err := verification.Close(); err != nil || gates.closed != 1 {
		t.Fatalf("close err=%v count=%d", err, gates.closed)
	}
}

func TestProductionVerificationDelegatedModeUsesAuthorityDeadlineForGateResolution(t *testing.T) {
	plan, value, snapshot, observed := productionVerificationFacts(t)
	snapshot.Player.Routes[0].LeaseUntilMs = time.Now().Add(-time.Second).UnixMilli()
	observed.LeaseUntilMs = snapshot.Player.Routes[0].LeaseUntilMs
	manager := &productionManagerFake{result: productionManagerObservation(plan, value, authority.TransitionEvidenceCompleteRecovery)}
	gates := &productionGateFleetFake{observations: []gateverification.PeerObservation{
		{Peer: gateverification.Peer{Name: "gate-a"}, Observation: observed},
	}}
	verification, err := NewProductionVerification(ProductionVerificationOptions{
		Manager: manager, Gates: gates, ExpectedPeers: 1, CallTimeout: time.Second,
		LeaseCheckMode: actorrouting.LeaseCheckAuthorityDelegated,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := verification.VerifyGateResolution(context.Background(), plan, value, snapshot); err != nil {
		t.Fatalf("delegated Gate verification rejected fresh Redis authority with stale snapshot deadline: %v", err)
	}
}

func TestManagerPlanExecutorProductionVerificationPassesRetainedRecoveryAndEvictedTombstone(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	leaseUntil := now.Add(time.Minute).UnixMilli()
	profile := actorrouting.HashProfile{Version: actorrouting.HashVersionXXH64V2, Seed: 202608040001}
	calculator, err := actorrouting.NewCalculator("classic-farm", profile, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	authorityStore, err := authoritystore.NewMemoryStore(authoritystore.MemoryOptions{
		Resolver: calculator, AuthorityLayoutVersion: "auth-v1", Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	source := authoritystore.ShardWriteAuthority{
		AuthorityLayoutVersion: "auth-v1", HashVersion: profile.Version, HashSeed: profile.Seed,
		Domain: actorrouting.DomainPlayer, VirtualShard: 0, HolderID: "player-source", HolderBootID: "source-boot",
		AssignmentRevision: 1, WriterEpoch: 1, LeaseEpoch: 1, LeaseUntilMs: leaseUntil,
		State: authoritystore.ShardAuthorityActive, LastTransitionID: "source-bootstrap", LastTransitionAtMs: now.Add(-time.Second).UnixMilli(),
	}
	if err := authorityStore.SetShardWriteAuthority(source); err != nil {
		t.Fatal(err)
	}
	initial := actorrouting.RouteSnapshot{
		Realm: "classic-farm", GlobalRevision: 1, HashVersion: profile.Version, HashSeed: profile.Seed,
		Player: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 0, State: actorrouting.ShardStateActive, ShardRevision: source.AssignmentRevision,
			ExpectedWriterEpoch: source.WriterEpoch, ExpectedLeaseEpoch: source.LeaseEpoch, LeaseUntilMs: source.LeaseUntilMs,
		}}},
		Farm: actorrouting.DomainSnapshot{ShardCount: 1, Routes: []actorrouting.ShardRoute{{
			HolderIndex: 1, State: actorrouting.ShardStateAssigning, ShardRevision: 1,
			ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1, LeaseUntilMs: leaseUntil,
		}}},
		Holders: []actorrouting.HolderRef{
			{InstanceID: source.HolderID, BootID: source.HolderBootID, Endpoint: "source.internal:443", State: actorrouting.HolderStateActive, Weight: 1},
			{InstanceID: "farm-source", BootID: "farm-source-boot", Endpoint: "farm-source.internal:443", State: actorrouting.HolderStateActive, Weight: 1},
		},
	}
	snapshotStore := core.NewMemoryRouteSnapshotStore()
	snapshotStore.SetNow(func() int64 { return now.UnixMilli() })
	if result, err := snapshotStore.Bootstrap(context.Background(), core.NestedControlOperationToken{}, initial); err != nil || !result.Committed() {
		t.Fatalf("snapshot bootstrap result=%+v err=%v", result, err)
	}
	publisher, err := core.NewDurableSnapshotPublisher(snapshotStore, nil,
		core.AuthoritySnapshotNotifierFunc(func(context.Context, actorrouting.RouteSnapshot) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	evidence := authority.NewMemoryTransitionTombstoneStore(8)
	reader := productionMemoryAuthorityReader{store: authorityStore}
	manager, err := authority.NewRouteAssignmentManager(authority.ManagerOptions{PlacementExclusions: newRoutePlanTestPlacementExclusions(),
		Store: authorityStore, AuthorityReader: reader, Publisher: publisher, InitialSnapshot: initial,
		AuthorityLayoutVersion: "auth-v1", RouteManagerID: "route-manager", RouteManagerBootID: "route-manager-boot",
		Now: func() time.Time { return now }, MemoLimit: 1, MemoRetention: time.Hour, TombstoneStore: evidence,
	})
	if err != nil {
		t.Fatal(err)
	}

	holders := &executorFakeHolderReader{records: map[string]core.HolderRecord{
		"player-target": {
			Domain: actorrouting.DomainPlayer, HolderID: "player-target", BootID: "target-boot",
			LogicEndpoint: "target.internal:443", HolderControlEndpoint: "target-control.internal:443", TLSDNSIdentity: "target-control.internal",
			State: core.HolderStateReadyZeroShard, ProcessReady: true, LeaseUntil: now.Add(time.Hour), SnapshotRevision: initial.GlobalRevision, CapacityWeight: 1,
		},
	}}
	planStore, err := NewMemoryPlanStore(MemoryPlanStoreOptions{Now: func() time.Time { return now }, PlanTTL: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	gates := &productionGateFleetFake{}
	verification, err := NewProductionVerification(ProductionVerificationOptions{
		Manager: manager, Gates: gates, ExpectedPeers: 2, CallTimeout: time.Second, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer verification.Close()
	executor, err := NewPlanExecutor(PlanExecutorOptions{
		Store: planStore, Manager: manager, AuthorityReader: reader, SnapshotStore: snapshotStore, HolderReader: holders,
		Now: func() time.Time { return now }, OperationTimeout: time.Second, PlanTTL: 24 * time.Hour, Verification: verification,
		SingleController: SingleControllerAttestation{Contract: SingleControllerContract, ControllerID: "route-controller", BootID: "controller-boot"},
	})
	if err != nil {
		t.Fatal(err)
	}
	fence := authoritystore.ShardWriteFence{
		ExpectedHolderID: source.HolderID, ExpectedHolderBootID: source.HolderBootID,
		ExpectedAssignmentRevision: source.AssignmentRevision, ExpectedWriterEpoch: source.WriterEpoch, ExpectedLeaseEpoch: source.LeaseEpoch,
	}
	record, err := executor.SubmitCrashTakeover(context.Background(), TakeoverRequest{
		PlanID: "plan-normal-completion", TransitionID: "transition-normal-completion", Domain: actorrouting.DomainPlayer, VirtualShard: 0,
		SourceHolderID: source.HolderID, SourceBootID: source.HolderBootID, SourceEndpoint: "source.internal:443",
		TargetHolderID: "player-target", TargetBootID: "target-boot", TargetEndpoint: "target.internal:443",
		ExpectedSourceFence: fence, RequestedTTL: time.Minute,
		Proof: TakeoverProof{ExpectedHolderBootID: source.HolderBootID, HolderUnavailable: true, Evidence: "source process unavailable"},
	})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || completed.Plan.Phase != PhaseSucceeded {
		t.Fatalf("completed=%+v err=%v", completed.Plan, err)
	}

	setVerificationProjection := func() authoritystore.ShardWriteAuthority {
		t.Helper()
		value, found, readErr := reader.ReadShardWriteAuthority(context.Background(), actorrouting.DomainPlayer, 0)
		if readErr != nil || !found {
			t.Fatalf("authority found=%t err=%v", found, readErr)
		}
		snapshot := manager.CurrentSnapshot()
		target := holders.records["player-target"]
		target.State = core.HolderStateServing
		target.SnapshotRevision = snapshot.GlobalRevision
		holders.records["player-target"] = target
		observation := authoritycomposition.ShardObservation{
			Realm: snapshot.Realm, Domain: actorrouting.DomainPlayer, VirtualShard: 0,
			HolderID: value.HolderID, HolderBootID: value.HolderBootID, LogicEndpoint: "target.internal:443",
			State: actorrouting.ShardStateActive, AssignmentRevision: value.AssignmentRevision, WriterEpoch: value.WriterEpoch,
			LeaseEpoch: value.LeaseEpoch, LeaseUntilMs: value.LeaseUntilMs, SnapshotRevision: snapshot.GlobalRevision,
		}
		gates.observations = []gateverification.PeerObservation{
			{Peer: gateverification.Peer{Name: "gate-a"}, Observation: observation},
			{Peer: gateverification.Peer{Name: "gate-b"}, Observation: observation},
		}
		return value
	}
	assertRoutectlCompatiblePass := func(stage string) {
		t.Helper()
		report, verifyErr := executor.Verify(context.Background(), record.Plan.PlanID)
		if verifyErr != nil || !report.Consistent || len(report.Checks) != 11 {
			t.Fatalf("%s report=%+v err=%v", stage, report, verifyErr)
		}
		want := map[string]string{
			"authority_active_lease": "OK", "authority_tuple": "AUTHORITY_TUPLE", "snapshot_read": "OK",
			"snapshot_tuple": "SNAPSHOT_TUPLE", "target_holder": "OK", "gate_resolution": "GATE_RESOLUTION",
			"transition": "TRANSITION", "recovery": "RECOVERY", "tombstone": "TOMBSTONE", "plan_state": "PLAN_STATE",
			"final_freshness": "FINAL_FRESHNESS",
		}
		for _, check := range report.Checks {
			code, ok := want[check.Name]
			if !ok || !check.OK || check.Code != code {
				t.Fatalf("%s routectl-incompatible check=%+v want=%v", stage, check, want)
			}
			delete(want, check.Name)
		}
		if len(want) != 0 {
			t.Fatalf("%s missing checks=%v", stage, want)
		}
	}

	value := setVerificationProjection()
	request, err := ManagerTransitionRequestForPlan(completed.Plan)
	if err != nil {
		t.Fatal(err)
	}
	observe := func() authority.TransitionObservation {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		observation, observeErr := manager.ObserveTransition(ctx, request)
		if observeErr != nil || observation.Result.Authority != value {
			t.Fatalf("observation=%+v err=%v", observation, observeErr)
		}
		return observation
	}
	if observation := observe(); observation.Class != authority.TransitionEvidenceCompleteRecovery {
		t.Fatalf("normal completion evidence=%+v", observation)
	}
	assertRoutectlCompatiblePass("retained-complete-recovery")

	farmClaim, err := manager.Claim(context.Background(), authority.ClaimRequest{
		TransitionID: "transition-farm-eviction", Domain: actorrouting.DomainFarm, VirtualShard: 0,
		NewHolderID: "farm-target", NewHolderBootID: "farm-target-boot", Endpoint: "farm-target.internal:443", RequestedTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	farmFence := authoritystore.ShardWriteFence{
		ExpectedHolderID: farmClaim.Authority.HolderID, ExpectedHolderBootID: farmClaim.Authority.HolderBootID,
		ExpectedAssignmentRevision: farmClaim.Authority.AssignmentRevision, ExpectedWriterEpoch: farmClaim.Authority.WriterEpoch,
		ExpectedLeaseEpoch: farmClaim.Authority.LeaseEpoch,
	}
	if _, err := manager.Transfer(context.Background(), authority.TransferRequest{
		TransitionID: "transition-farm-force-successor", Domain: actorrouting.DomainFarm, VirtualShard: 0, ExpectedFence: farmFence,
		NewHolderID: "farm-successor", NewHolderBootID: "farm-successor-boot", Endpoint: "farm-successor.internal:443", RequestedTTL: time.Minute,
		Mode: authority.TransferModeCrashTakeover, TakeoverProof: authority.TransferTakeoverProof{
			ExpectedHolderBootID: farmFence.ExpectedHolderBootID, HolderUnavailable: true, Evidence: "force bounded memo eviction",
		},
	}); err != nil {
		t.Fatal(err)
	}
	value = setVerificationProjection()
	if observation := observe(); observation.Class != authority.TransitionEvidenceTombstone || observation.Tombstone.ExpiredAt.IsZero() {
		t.Fatalf("evicted terminal evidence=%+v", observation)
	}
	assertRoutectlCompatiblePass("atomic-tombstone-successor")
}

func TestProductionVerificationFailsClosedForAnyGateOrManagerEvidence(t *testing.T) {
	plan, value, snapshot, observed := productionVerificationFacts(t)
	newVerification := func(observations []gateverification.PeerObservation, gateErr, managerErr error, class authority.TransitionEvidenceClass) *ProductionVerification {
		managerObservation := productionManagerObservation(plan, value, class)
		result, err := NewProductionVerification(ProductionVerificationOptions{
			Manager: &productionManagerFake{result: managerObservation, err: managerErr},
			Gates:   &productionGateFleetFake{observations: observations, err: gateErr}, ExpectedPeers: 2, CallTimeout: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	base := []gateverification.PeerObservation{{Peer: gateverification.Peer{Name: "a"}, Observation: observed}, {Peer: gateverification.Peer{Name: "b"}, Observation: observed}}
	for name, mutate := range map[string]func(*authoritycomposition.ShardObservation){
		"stale-revision":   func(v *authoritycomposition.ShardObservation) { v.SnapshotRevision-- },
		"ahead-revision":   func(v *authoritycomposition.ShardObservation) { v.SnapshotRevision++ },
		"wrong-endpoint":   func(v *authoritycomposition.ShardObservation) { v.LogicEndpoint += "-wrong" },
		"wrong-generation": func(v *authoritycomposition.ShardObservation) { v.WriterEpoch++ },
		"inactive":         func(v *authoritycomposition.ShardObservation) { v.State = actorrouting.ShardStateAssigning },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := append([]gateverification.PeerObservation(nil), base...)
			mutate(&candidate[1].Observation)
			verification := newVerification(candidate, nil, nil, authority.TransitionEvidenceCompleteRecovery)
			defer verification.Close()
			if err := verification.VerifyGateResolution(context.Background(), plan, value, snapshot); !errors.Is(err, ErrVerification) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	for _, test := range []struct {
		name         string
		observations []gateverification.PeerObservation
		gateErr      error
	}{
		{name: "missing-peer", observations: base[:1]},
		{name: "unavailable", observations: base, gateErr: errors.New("unavailable")},
	} {
		t.Run(test.name, func(t *testing.T) {
			verification := newVerification(test.observations, test.gateErr, nil, authority.TransitionEvidenceCompleteRecovery)
			defer verification.Close()
			if err := verification.VerifyGateResolution(context.Background(), plan, value, snapshot); !errors.Is(err, ErrVerification) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	for _, test := range []struct {
		name  string
		err   error
		class authority.TransitionEvidenceClass
	}{
		{name: "missing", class: authority.TransitionEvidenceUnspecified},
		{name: "corrupt", err: errors.New("corrupt")},
	} {
		t.Run("manager-"+test.name, func(t *testing.T) {
			verification := newVerification(base, nil, test.err, test.class)
			defer verification.Close()
			if err := verification.VerifyTransition(context.Background(), plan, value, snapshot); !errors.Is(err, ErrVerification) {
				t.Fatalf("transition err=%v", err)
			}
			if err := verification.VerifyRecovery(context.Background(), plan, value, snapshot); !errors.Is(err, ErrVerification) {
				t.Fatalf("recovery err=%v", err)
			}
			if err := verification.VerifyTombstone(context.Background(), plan, value, snapshot); !errors.Is(err, ErrVerification) {
				t.Fatalf("tombstone err=%v", err)
			}
		})
	}
}

func TestProductionVerificationBindsExactCurrentGenerationAndDistinctDurablePredicates(t *testing.T) {
	plan, value, snapshot, observed := productionVerificationFacts(t)
	now := time.UnixMilli(value.LastTransitionAtMs)
	manager := &productionManagerFake{result: productionManagerObservation(plan, value, authority.TransitionEvidenceCompleteRecovery)}
	verification, err := NewProductionVerification(ProductionVerificationOptions{
		Manager: manager, Gates: &productionGateFleetFake{observations: []gateverification.PeerObservation{{Observation: observed}}},
		ExpectedPeers: 1, CallTimeout: time.Second, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer verification.Close()
	ctx := context.Background()
	if err := verification.VerifyTransition(ctx, plan, value, snapshot); err != nil {
		t.Fatalf("complete transition: %v", err)
	}
	if err := verification.VerifyRecovery(ctx, plan, value, snapshot); err != nil {
		t.Fatalf("complete recovery: %v", err)
	}
	if err := verification.VerifyTombstone(ctx, plan, value, snapshot); err != nil {
		t.Fatalf("retained recovery replay floor: %v", err)
	}
	manager.result = productionManagerObservation(plan, value, authority.TransitionEvidenceTombstone)
	if err := verification.VerifyTransition(ctx, plan, value, snapshot); err != nil {
		t.Fatalf("replay-floor transition: %v", err)
	}
	if err := verification.VerifyRecovery(ctx, plan, value, snapshot); err != nil {
		t.Fatalf("replay-floor recovery: %v", err)
	}
	if err := verification.VerifyTombstone(ctx, plan, value, snapshot); err != nil {
		t.Fatalf("replay-floor tombstone: %v", err)
	}

	for name, mutate := range map[string]func(*authority.TransitionObservation){
		"nonterminal-result": func(v *authority.TransitionObservation) {
			v.Result.Status = authoritystore.ShardTransitionReconcileRequired
		},
		"accepted-ttl":   func(v *authority.TransitionObservation) { v.Result.AcceptedTTLms++ },
		"fingerprint":    func(v *authority.TransitionObservation) { v.Tombstone.Fingerprint += "-other" },
		"durable-result": func(v *authority.TransitionObservation) { v.Tombstone.Result.Authority.WriterEpoch++ },
		"missing-expiry": func(v *authority.TransitionObservation) { v.Tombstone.ExpiredAt = time.Time{} },
		"overlap": func(v *authority.TransitionObservation) {
			v.Recovery = productionManagerObservation(plan, value, authority.TransitionEvidenceCompleteRecovery).Recovery
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := productionManagerObservation(plan, value, authority.TransitionEvidenceTombstone)
			mutate(&candidate)
			manager.result = candidate
			if err := verification.VerifyTombstone(ctx, plan, value, snapshot); !errors.Is(err, ErrVerification) {
				t.Fatalf("mismatched durable evidence passed: %v", err)
			}
		})
	}
	for name, mutate := range map[string]func(*authority.TransitionObservation){
		"pending-publication": func(v *authority.TransitionObservation) { v.Recovery.Published = false },
		"mismatched-target":   func(v *authority.TransitionObservation) { v.Recovery.NewHolderBootID += "-other" },
		"wrong-drain-mode":    func(v *authority.TransitionObservation) { v.Recovery.Mode = authority.TransferModeUnspecified },
		"corrupt-result":      func(v *authority.TransitionObservation) { v.Recovery.Result.Authority.LeaseEpoch++ },
		"overlap": func(v *authority.TransitionObservation) {
			v.Tombstone = productionManagerObservation(plan, value, authority.TransitionEvidenceTombstone).Tombstone
		},
	} {
		t.Run("recovery-"+name, func(t *testing.T) {
			candidate := productionManagerObservation(plan, value, authority.TransitionEvidenceCompleteRecovery)
			mutate(&candidate)
			manager.result = candidate
			if err := verification.VerifyTombstone(ctx, plan, value, snapshot); !errors.Is(err, ErrVerification) {
				t.Fatalf("incomplete retained replay floor passed: %v", err)
			}
		})
	}
}

func TestProductionVerificationAcceptsClampedAcceptedTTL(t *testing.T) {
	plan, value, snapshot, observed := productionVerificationFacts(t)
	clamped := int64(15_000)
	if clamped <= 0 || clamped >= plan.RequestedTTLms {
		t.Fatalf("fixture RequestedTTLms=%d is not a clampable request", plan.RequestedTTLms)
	}
	observation := productionManagerObservation(plan, value, authority.TransitionEvidenceCompleteRecovery)
	observation.Result.AcceptedTTLms = clamped
	observation.Recovery.Result.AcceptedTTLms = clamped
	now := time.UnixMilli(value.LastTransitionAtMs)
	verification, err := NewProductionVerification(ProductionVerificationOptions{
		Manager:       &productionManagerFake{result: observation},
		Gates:         &productionGateFleetFake{observations: []gateverification.PeerObservation{{Observation: observed}}},
		ExpectedPeers: 1, CallTimeout: time.Second, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer verification.Close()
	ctx := context.Background()
	if err := verification.VerifyTransition(ctx, plan, value, snapshot); err != nil {
		t.Fatalf("clamped transition: %v", err)
	}
	if err := verification.VerifyRecovery(ctx, plan, value, snapshot); err != nil {
		t.Fatalf("clamped recovery: %v", err)
	}
	if err := verification.VerifyTombstone(ctx, plan, value, snapshot); err != nil {
		t.Fatalf("clamped retained recovery: %v", err)
	}

	overGrant := productionManagerObservation(plan, value, authority.TransitionEvidenceCompleteRecovery)
	overGrant.Result.AcceptedTTLms = plan.RequestedTTLms + 1
	overGrant.Recovery.Result.AcceptedTTLms = plan.RequestedTTLms + 1
	overVerification, err := NewProductionVerification(ProductionVerificationOptions{
		Manager:       &productionManagerFake{result: overGrant},
		Gates:         &productionGateFleetFake{observations: []gateverification.PeerObservation{{Observation: observed}}},
		ExpectedPeers: 1, CallTimeout: time.Second, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer overVerification.Close()
	if err := overVerification.VerifyTransition(ctx, plan, value, snapshot); !errors.Is(err, ErrVerification) {
		t.Fatalf("over-grant accepted TTL passed: %v", err)
	}
}

func TestProductionVerificationAcceptsMonotonicDeadlineRenewalOnly(t *testing.T) {
	plan, committed, snapshot, observed := productionVerificationFacts(t)
	current := committed
	current.LeaseUntilMs += time.Minute.Milliseconds()
	observation := productionManagerObservation(plan, committed, authority.TransitionEvidenceCompleteRecovery)
	// Manager observation reconciles its top-level result to current Authority;
	// the immutable CompleteRecovery remains bound to the deadline published in
	// the ACTIVE Snapshot.
	observation.Result.Authority = current
	manager := &productionManagerFake{result: observation}
	gates := &productionGateFleetFake{observations: []gateverification.PeerObservation{{Observation: observed}}}
	verification, err := NewProductionVerification(ProductionVerificationOptions{
		Manager: manager, Gates: gates, ExpectedPeers: 1, CallTimeout: time.Second,
		Now: func() time.Time { return time.UnixMilli(committed.LastTransitionAtMs) },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer verification.Close()
	for name, verify := range map[string]func() error{
		"gate":       func() error { return verification.VerifyGateResolution(context.Background(), plan, current, snapshot) },
		"transition": func() error { return verification.VerifyTransition(context.Background(), plan, current, snapshot) },
		"recovery":   func() error { return verification.VerifyRecovery(context.Background(), plan, current, snapshot) },
		"tombstone":  func() error { return verification.VerifyTombstone(context.Background(), plan, current, snapshot) },
	} {
		if err := verify(); err != nil {
			t.Fatalf("%s rejected monotonic deadline renewal: %v", name, err)
		}
	}

	// Manager-owned reconciliation may subsequently project the current
	// renewed deadline while the immutable CompleteRecovery remains bound to
	// the transition's original deadline. Verification must accept that exact
	// bounded ordering instead of requiring Snapshot == durable replay floor.
	projected := actorrouting.CloneRouteSnapshot(snapshot)
	projected.GlobalRevision++
	projected.Player.Routes[plan.VirtualShard].LeaseUntilMs = current.LeaseUntilMs
	manager.result = productionManagerObservation(plan, committed, authority.TransitionEvidenceCompleteRecovery)
	for name, verify := range map[string]func() error{
		"transition": func() error { return verification.VerifyTransition(context.Background(), plan, current, projected) },
		"recovery":   func() error { return verification.VerifyRecovery(context.Background(), plan, current, projected) },
		"tombstone":  func() error { return verification.VerifyTombstone(context.Background(), plan, current, projected) },
	} {
		if err := verify(); err != nil {
			t.Fatalf("%s rejected projected monotonic deadline renewal: %v", name, err)
		}
	}
	aboveAuthority := actorrouting.CloneRouteSnapshot(projected)
	aboveAuthority.GlobalRevision++
	aboveAuthority.Player.Routes[plan.VirtualShard].LeaseUntilMs++
	if err := verification.VerifyTransition(context.Background(), plan, current, aboveAuthority); !errors.Is(err, ErrVerification) {
		t.Fatalf("Snapshot deadline above current Authority passed: %v", err)
	}
	belowDurable := actorrouting.CloneRouteSnapshot(snapshot)
	belowDurable.GlobalRevision++
	belowDurable.Player.Routes[plan.VirtualShard].LeaseUntilMs--
	if err := verification.VerifyTransition(context.Background(), plan, current, belowDurable); !errors.Is(err, ErrVerification) {
		t.Fatalf("Snapshot deadline below durable replay floor passed: %v", err)
	}

	current.WriterEpoch++
	if err := verification.VerifyTransition(context.Background(), plan, current, snapshot); !errors.Is(err, ErrVerification) {
		t.Fatalf("identity drift passed: %v", err)
	}
}

func TestProductionVerificationGenerationNResultNeverPassesCurrentNPlusOne(t *testing.T) {
	plan, generationN, snapshotN, observedN := productionVerificationFacts(t)
	now := time.UnixMilli(generationN.LastTransitionAtMs)
	manager := &productionManagerFake{result: productionManagerObservation(plan, generationN, authority.TransitionEvidenceTombstone)}
	gates := &productionGateFleetFake{observations: []gateverification.PeerObservation{{Observation: observedN}}}
	verification, err := NewProductionVerification(ProductionVerificationOptions{
		Manager: manager, Gates: gates, ExpectedPeers: 1, CallTimeout: time.Second, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer verification.Close()

	generationNPlusOne := generationN
	generationNPlusOne.AssignmentRevision++
	generationNPlusOne.WriterEpoch++
	generationNPlusOne.LeaseEpoch++
	generationNPlusOne.LeaseUntilMs++
	generationNPlusOne.LastTransitionAtMs++
	snapshotNPlusOne := actorrouting.CloneRouteSnapshot(snapshotN)
	route := &snapshotNPlusOne.Player.Routes[plan.VirtualShard]
	route.ShardRevision = generationNPlusOne.AssignmentRevision
	route.ExpectedWriterEpoch = generationNPlusOne.WriterEpoch
	route.ExpectedLeaseEpoch = generationNPlusOne.LeaseEpoch
	route.LeaseUntilMs = generationNPlusOne.LeaseUntilMs
	observedNPlusOne := observedN
	observedNPlusOne.AssignmentRevision = generationNPlusOne.AssignmentRevision
	observedNPlusOne.WriterEpoch = generationNPlusOne.WriterEpoch
	observedNPlusOne.LeaseEpoch = generationNPlusOne.LeaseEpoch
	observedNPlusOne.LeaseUntilMs = generationNPlusOne.LeaseUntilMs
	gates.observations = []gateverification.PeerObservation{{Observation: observedNPlusOne}}

	for name, verify := range map[string]func() error{
		"transition": func() error {
			return verification.VerifyTransition(context.Background(), plan, generationNPlusOne, snapshotNPlusOne)
		},
		"recovery": func() error {
			return verification.VerifyRecovery(context.Background(), plan, generationNPlusOne, snapshotNPlusOne)
		},
		"tombstone": func() error {
			return verification.VerifyTombstone(context.Background(), plan, generationNPlusOne, snapshotNPlusOne)
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := verify(); !errors.Is(err, ErrVerification) {
				t.Fatalf("generation N result passed current N+1: %v", err)
			}
		})
	}
}

func TestProductionVerificationGateLeaseExpiresDuringObservation(t *testing.T) {
	plan, value, snapshot, observed := productionVerificationFacts(t)
	deadline := time.UnixMilli(value.LeaseUntilMs)
	clock := deadline.Add(-time.Millisecond)
	gates := &productionGateFleetFake{
		observations: []gateverification.PeerObservation{{Observation: observed}},
		beforeReturn: func() { clock = deadline },
	}
	verification, err := NewProductionVerification(ProductionVerificationOptions{
		Manager: &productionManagerFake{result: productionManagerObservation(plan, value, authority.TransitionEvidenceTombstone)},
		Gates:   gates, ExpectedPeers: 1, CallTimeout: time.Second, Now: func() time.Time { return clock },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer verification.Close()
	if err := verification.VerifyGateResolution(context.Background(), plan, value, snapshot); !errors.Is(err, ErrVerification) {
		t.Fatalf("lease expiring during Gate observation passed: %v", err)
	}
}
