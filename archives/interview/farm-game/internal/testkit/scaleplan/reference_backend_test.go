package scaleplan_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	"farmgame/internal/controlfence"
	core "farmgame/internal/scaleplan"
	"farmgame/internal/storage/authoritystore"
	testkit "farmgame/internal/testkit/scaleplan"
)

var referenceScope = core.MutationScope{Namespace: "farm", Realm: "realm-a"}

func referenceRuntime(t *testing.T) (*core.ProductionRuntime, *testkit.ReferenceBackend, *testkit.ManualClock) {
	t.Helper()
	clock := testkit.NewManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	backend := testkit.NewReferenceBackend(clock.Now)
	nonce := 0
	runtime, err := core.NewProductionRuntime(core.ProductionRuntimeOptions{
		RepositoryBackend: backend,
		Quiescence:        testkit.NewFakeQuiescence(clock.Now),
		Scope:             referenceScope,
		Controller:        core.MutationLeaseOwner{ControllerID: "controller-a", ControllerBootID: "boot-1"},
		NonceSource: func() (string, error) {
			nonce++
			return "nonce-" + string(rune('a'+nonce-1)), nil
		},
		// Fast enough that a renewal actually happens during a test, while
		// keeping the three-attempts-per-TTL ratio the contract requires.
		Heartbeat: 10 * time.Millisecond,
		LeaseTTL:  time.Second,
		Now:       clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return runtime, backend, clock
}

// Seam 1 and 2: an external backend receives a seal it could not have built,
// and the guard it runs under is bound to the lease.
func TestReferenceBackendReceivesSealItCannotMint(t *testing.T) {
	runtime, backend, _ := referenceRuntime(t)

	var forged core.SealedBlockedTerminationMutation
	if forged.Authorized() {
		t.Fatal("zero-value seal reports authorized")
	}
	result, err := backend.ApplySealedBlockedTermination(context.Background(), core.BackendMutationLease{}, forged)
	if !errors.Is(err, core.ErrVerification) || result.Status != core.BlockedTerminationUnsafe {
		t.Fatalf("backend accepted a forged seal: status=%s err=%v", result.Status, err)
	}
	if backend.UnauthorizedRejected() != 1 {
		t.Fatalf("rejection did not happen before store access: %d", backend.UnauthorizedRejected())
	}
	if backend.LeaseHeld() {
		t.Fatal("a rejected seal acquired a lease")
	}

	// The repository handed out is the plain interface; nothing on it can
	// produce a seal, which is the point of the pairing.
	if _, ok := runtime.Repository().(interface {
		Seal() core.SealedBlockedTerminationMutation
	}); ok {
		t.Fatal("repository exposes a seal constructor")
	}
}

func TestIndependentBackendsIssueDistinctRepositoryBoundLeases(t *testing.T) {
	runtimeA, backendA, _ := referenceRuntime(t)
	runtimeB, backendB, _ := referenceRuntime(t)
	guardA, err := runtimeA.EnterPlanMutation(context.Background(), referenceScope)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = guardA.Release(context.Background()) }()
	guardB, err := runtimeB.EnterPlanMutation(context.Background(), referenceScope)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = guardB.Release(context.Background()) }()

	leaseA, leaseB := guardA.Lease(), guardB.Lease()
	if leaseA.RepositoryID() == "" || leaseB.RepositoryID() == "" {
		t.Fatal("backend issued a lease without repository identity")
	}
	if leaseA.RepositoryID() == leaseB.RepositoryID() ||
		backendA.RepositoryBindingID() == backendB.RepositoryBindingID() {
		t.Fatal("independent repositories share the same binding identity")
	}
	// Both backends start at fence one for the same scope and controller. The
	// repository ID is therefore the field that prevents a cross-store seal
	// from looking like the current writer's authority.
	if leaseA.Scope() != leaseB.Scope() || leaseA.Owner() != leaseB.Owner() || leaseA.Fence() != leaseB.Fence() {
		t.Fatal("fixture no longer exercises colliding non-repository lease fields")
	}
}

func TestExpiredLeaseAllowsHigherFenceTakeoverAndRejectsFormerWriter(t *testing.T) {
	clock := testkit.NewManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	backend := testkit.NewReferenceBackend(clock.Now)
	ownerA := core.MutationLeaseOwner{ControllerID: "controller-a", ControllerBootID: "boot-a", OperationNonce: "nonce-a"}
	leaseA, err := backend.AcquireMutationLease(context.Background(), referenceScope, ownerA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.AcquireMutationLease(context.Background(), referenceScope,
		core.MutationLeaseOwner{ControllerID: "controller-b", ControllerBootID: "boot-b", OperationNonce: "early"}); err == nil {
		t.Fatal("a second owner acquired an unexpired lease")
	}

	clock.Advance(time.UnixMilli(leaseA.ExpiresAtMs).Sub(clock.Now()))
	if _, err := backend.CompareAndSwap(context.Background(), leaseA, core.DurableCASCommand{}); !errors.Is(err, testkit.ErrReferenceLeaseLost) {
		t.Fatalf("expired writer reached the plan store: %v", err)
	}
	if _, err := backend.RenewMutationLease(context.Background(), leaseA); !errors.Is(err, testkit.ErrReferenceLeaseLost) {
		t.Fatalf("expired writer renewed: %v", err)
	}

	ownerB := core.MutationLeaseOwner{ControllerID: "controller-b", ControllerBootID: "boot-b", OperationNonce: "nonce-b"}
	leaseB, err := backend.AcquireMutationLease(context.Background(), referenceScope, ownerB)
	if err != nil {
		t.Fatal(err)
	}
	fenceA, _ := strconv.ParseUint(leaseA.Fence, 10, 64)
	fenceB, _ := strconv.ParseUint(leaseB.Fence, 10, 64)
	if fenceB <= fenceA {
		t.Fatalf("takeover did not advance fence: old=%d new=%d", fenceA, fenceB)
	}
	if _, err := backend.CompareAndSwap(context.Background(), leaseA, core.DurableCASCommand{}); !errors.Is(err, testkit.ErrReferenceLeaseLost) {
		t.Fatalf("former writer reached the store after takeover: %v", err)
	}
	if err := backend.ReleaseMutationLease(context.Background(), leaseA); !errors.Is(err, testkit.ErrReferenceLeaseLost) {
		t.Fatalf("former writer released successor lease: %v", err)
	}
	if !backend.LeaseHeld() {
		t.Fatal("stale release cleared successor lease")
	}
	if err := backend.ReleaseMutationLease(context.Background(), leaseB); err != nil {
		t.Fatal(err)
	}
}

type renewalDeadlineBackend struct {
	*testkit.ReferenceBackend
	mode string
}

func (b *renewalDeadlineBackend) RenewMutationLease(ctx context.Context, lease core.BackendMutationLease) (core.BackendMutationLease, error) {
	renewed, err := b.ReferenceBackend.RenewMutationLease(ctx, lease)
	if err != nil {
		return renewed, err
	}
	switch b.mode {
	case "same":
		renewed.ExpiresAtMs = lease.ExpiresAtMs
	case "earlier":
		renewed.ExpiresAtMs = lease.ExpiresAtMs - 1
	}
	return renewed, nil
}

func TestGuardAcceptsStrictlyAdvancingRenewalDeadline(t *testing.T) {
	runtime, _, _ := referenceRuntime(t)
	guard, err := runtime.EnterPlanMutation(context.Background(), referenceScope)
	if err != nil {
		t.Fatal(err)
	}
	initial := guard.Lease().ExpiresAtMs()
	deadline := time.After(5 * time.Second)
	for guard.Lease().ExpiresAtMs() <= initial {
		select {
		case <-deadline:
			t.Fatal("successful renewal did not advance deadline")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if guard.Context().Err() != nil {
		t.Fatal("advancing renewal cancelled the guard")
	}
	if err := guard.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestGuardCancelsWhenRenewalDeadlineDoesNotAdvance(t *testing.T) {
	for _, mode := range []string{"same", "earlier"} {
		t.Run(mode, func(t *testing.T) {
			clock := testkit.NewManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
			backend := &renewalDeadlineBackend{ReferenceBackend: testkit.NewReferenceBackend(clock.Now), mode: mode}
			nonce := 0
			runtime, err := core.NewProductionRuntime(core.ProductionRuntimeOptions{
				RepositoryBackend: backend, Quiescence: testkit.NewFakeQuiescence(clock.Now), Scope: referenceScope,
				Controller:  core.MutationLeaseOwner{ControllerID: "controller-a", ControllerBootID: "boot-1"},
				NonceSource: func() (string, error) { nonce++; return "deadline-nonce-" + strconv.Itoa(nonce), nil },
				Heartbeat:   10 * time.Millisecond, LeaseTTL: time.Second, Now: clock.Now,
			})
			if err != nil {
				t.Fatal(err)
			}
			guard, err := runtime.EnterPlanMutation(context.Background(), referenceScope)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-guard.Context().Done():
			case <-time.After(5 * time.Second):
				t.Fatal("non-progressing renewal did not cancel guard")
			}
			if err := guard.Release(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSealMintedForOneRepositoryIsRejectedByAnotherBeforeStoreAccess(t *testing.T) {
	runtimeA, backendA, clock := referenceRuntime(t)
	now := clock.Now()
	workload := core.WorkloadRef{Realm: "realm-a", Domain: actorrouting.DomainPlayer, Namespace: "farm", Kind: "StatefulSet", Name: "player-svr"}
	uid := "workload-player-v1"
	source := referenceHolder(now, uid, "player-svr-0", "boot-0", 0, 1)
	target := referenceHolder(now, uid, "player-svr-1", "boot-1", 1, 0)
	holders := testkit.NewFakeHolders(source)
	workloads := testkit.NewFakeWorkloadScaler(core.WorkloadState{
		Ref: workload, UID: uid, ResourceVersion: "100", Generation: 1, ObservedGeneration: 1,
		Replicas: 1, ReadyReplicas: 1,
		Pods: []core.WorkloadPodState{{PodName: "player-svr-0", PodUID: source.Ref.PodUID, WorkloadUID: uid,
			Ordinal: 0, HolderID: source.Ref.HolderID, Present: true, Ready: true}},
	})
	workloads.SetScaleHook(func(state *core.WorkloadState, desired int32) {
		state.ReadyReplicas = desired
		if desired == 2 {
			state.Pods = append(state.Pods, core.WorkloadPodState{PodName: "player-svr-1", PodUID: target.Ref.PodUID,
				WorkloadUID: uid, Ordinal: 1, HolderID: target.Ref.HolderID, Present: true, Ready: true})
			holders.Set(target)
		}
	})
	placements := testkit.NewFakePlacements(referenceShard(now, 7, source.Ref, source.LogicEndpoint))
	routePlans := testkit.NewFakeRoutePlans()
	routePlans.SetNextPhase(core.RoutePlanPhasePaused)
	exclusions := testkit.NewFakeExclusions(holders,
		core.PlacementExclusion{Holder: source.Ref, Revision: 1},
		core.PlacementExclusion{Holder: target.Ref, Revision: 1})
	executor, err := runtimeA.NewExecutor(core.ExecutorOptions{
		Workloads: workloads, RoutePlans: routePlans, Holders: holders, Placements: placements,
		Exclusions: exclusions, Now: clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	submission := core.Submission{
		IdempotencyKey: "repository-binding-seal", Realm: workload.Realm, Domain: workload.Domain,
		Operation: core.OperationScaleOut, Workload: workload, ExpectedReplicas: 1, DesiredReplicas: 2,
		Moves: []core.ShardMoveIntent{{Domain: workload.Domain, VirtualShard: 7,
			SourceHolderID: source.Ref.HolderID, TargetHolderID: target.Ref.HolderID, TargetOrdinal: 1}},
		RoutePlanTTLms: time.Minute.Milliseconds(), PlanTTLms: time.Hour.Milliseconds(),
		RecoveryRetentionMs: (24 * time.Hour).Milliseconds(),
		Limits:              core.FunctionalLimits{MaxRoutePlansInflight: 1, MaxShardsPerWave: 1},
	}
	record, err := executor.Submit(context.Background(), submission)
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || blocked.Plan.Status != core.StatusBlocked {
		t.Fatalf("fixture did not reach blocked state: status=%s err=%v", blocked.Plan.Status, err)
	}
	request, err := core.BuildBlockedTerminationRequest(blocked.Plan, "terminate-for-binding-test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.OperatorFailBlocked(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	seals := backendA.SealsSeen()
	if len(seals) != 1 || !seals[0].Authorized() {
		t.Fatalf("production runtime did not expose exactly one genuine seal to its backend: %d", len(seals))
	}

	_, backendB, _ := referenceRuntime(t)
	sealLease := seals[0].Lease()
	wantFence, err := strconv.Atoi(sealLease.Fence)
	if err != nil || wantFence < 1 {
		t.Fatalf("unexpected seal fence %q", sealLease.Fence)
	}
	for fence := 1; fence < wantFence; fence++ {
		lease, acquireErr := backendB.AcquireMutationLease(context.Background(), referenceScope,
			core.MutationLeaseOwner{ControllerID: "advance", ControllerBootID: "advance", OperationNonce: strconv.Itoa(fence)})
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		if releaseErr := backendB.ReleaseMutationLease(context.Background(), lease); releaseErr != nil {
			t.Fatal(releaseErr)
		}
	}
	leaseB, err := backendB.AcquireMutationLease(context.Background(), referenceScope, sealLease.Owner)
	if err != nil {
		t.Fatal(err)
	}
	if leaseB.Scope != sealLease.Scope || leaseB.Owner != sealLease.Owner || leaseB.Fence != sealLease.Fence ||
		leaseB.RepositoryID == sealLease.RepositoryID {
		t.Fatalf("fixture did not isolate repository identity: seal=%+v foreign=%+v", sealLease, leaseB)
	}
	result, err := backendB.ApplySealedBlockedTermination(context.Background(), leaseB, seals[0])
	if !errors.Is(err, core.ErrVerification) || result.Status != core.BlockedTerminationUnsafe {
		t.Fatalf("foreign repository accepted a genuine seal: status=%s err=%v", result.Status, err)
	}
	if backendB.UnauthorizedRejected() != 1 {
		t.Fatal("foreign seal was not rejected by the pre-store authorization gate")
	}
}

func referenceHolder(now time.Time, workloadUID, id, boot string, ordinal int32, shards uint32) core.HolderState {
	state := core.HolderServing
	if shards == 0 {
		state = core.HolderReadyZeroShard
	}
	return core.HolderState{
		Ref: core.HolderRef{Domain: actorrouting.DomainPlayer, HolderID: id, BootID: boot,
			PodUID: "pod-uid-" + id, WorkloadUID: workloadUID, Ordinal: ordinal},
		PodName: id, LogicEndpoint: id + ".farm.svc:9000", HolderControlEndpoint: id + ".farm.svc:9443",
		TLSDNSIdentity: id + ".farm.svc", State: state, ProcessReady: true,
		ServingShardCount: shards, SnapshotRevision: 10, AssignmentKnownFresh: true,
		PlacementEligible: true, PlacementRevision: 1, LeaseUntil: now.Add(time.Hour),
	}
}

func referenceShard(now time.Time, id uint32, source core.HolderRef, endpoint string) core.ShardPlacement {
	return core.ShardPlacement{
		Domain: source.Domain, VirtualShard: id, Active: true, SnapshotRevision: 10, Holder: source, Endpoint: endpoint,
		ExpectedSourceFence: authoritystore.ShardWriteFence{ExpectedHolderID: source.HolderID,
			ExpectedHolderBootID: source.BootID, ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1},
		KnownFresh: true, LeaseUntil: now.Add(time.Hour),
	}
}

// Seam 2: guard context cancellation. This is why
// ProductionRuntime.EnterPlanMutation stays exported while the owner's does not.
func TestGuardContextIsCancelledWhenRenewalStops(t *testing.T) {
	runtime, backend, _ := referenceRuntime(t)
	guard, err := runtime.EnterPlanMutation(context.Background(), referenceScope)
	if err != nil {
		t.Fatal(err)
	}
	if !guard.Lease().Held() {
		t.Fatal("guard returned an unheld lease")
	}
	if guard.Context().Err() != nil {
		t.Fatal("guard context started cancelled")
	}

	backend.FailRenewals()
	select {
	case <-guard.Context().Done():
	case <-time.After(30 * time.Second):
		t.Fatal("losing renewal did not cancel the guard context")
	}

	// Release must still join the renewer and free the durable lease.
	if err := guard.Release(context.Background()); err != nil && !errors.Is(err, testkit.ErrReferenceLeaseLost) {
		t.Fatalf("release after lease loss: %v", err)
	}
	if backend.Releases() == 0 {
		t.Fatal("release never reached the backend")
	}
}

func TestWritesOutsideTheBarrierAreRefused(t *testing.T) {
	runtime, _, _ := referenceRuntime(t)
	repository := runtime.Repository()
	// No guard is open, so there is no lease to write under.
	if _, err := repository.CompareAndSwap(context.Background(), 1, core.Plan{}); !errors.Is(err, core.ErrInvalidPlan) {
		t.Fatalf("durable write outside the barrier was allowed: %v", err)
	}
}

func TestSecondGuardOnTheSameFacadeIsRefused(t *testing.T) {
	runtime, _, _ := referenceRuntime(t)
	first, err := runtime.EnterPlanMutation(context.Background(), referenceScope)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Release(context.Background()) }()
	if _, err := runtime.EnterPlanMutation(context.Background(), referenceScope); !errors.Is(err, core.ErrInvalidPlan) {
		t.Fatalf("facade issued a second concurrent lease: %v", err)
	}
}

func TestGuardRefusesScopeOutsideTheFacadeRealm(t *testing.T) {
	runtime, _, _ := referenceRuntime(t)
	other := core.MutationScope{Namespace: "farm", Realm: "realm-b"}
	if _, err := runtime.EnterPlanMutation(context.Background(), other); !errors.Is(err, core.ErrInvalidPlan) {
		t.Fatalf("facade leased a realm it does not own: %v", err)
	}
}

// Seam 3: nested token inheritance. A nested callee verifies, it does not
// re-acquire, and the zero token is never a credential.
func TestNestedTokenIsInheritedNotReacquired(t *testing.T) {
	issuer := controlfence.NewIssuer()
	top := issuer.Issue("operation-1", 7)
	if !top.Issued() || !issuer.IssuedBy(top) {
		t.Fatal("issued token failed its own verification")
	}
	if err := core.RequireControlToken(top); err != nil {
		t.Fatal(err)
	}

	var zero controlfence.NestedControlOperationToken
	if zero.Issued() || issuer.IssuedBy(zero) {
		t.Fatal("zero token passes as a credential")
	}
	if err := core.RequireControlToken(zero); err == nil {
		t.Fatal("zero token accepted by the gated-writer check")
	}

	// A different coordinator must not honor another's token, or a writer
	// fenced out of one gate could walk into the next one holding the receipt.
	if controlfence.NewIssuer().IssuedBy(top) {
		t.Fatal("a foreign issuer accepted this token")
	}
	if top.OperationID() != "operation-1" || top.Epoch() != 7 {
		t.Fatalf("token lost its identity: %q %d", top.OperationID(), top.Epoch())
	}
	// An operation with no durable identity is not one a crashed controller
	// could find again, so it is not issuable.
	if issuer.Issue("", 1).Issued() {
		t.Fatal("an anonymous operation was issued a token")
	}
}

func TestCoordinatorPresenceIsBoundToModeAtStartup(t *testing.T) {
	var present core.ControlMutationCoordinator = stubCoordinator{}
	for _, testCase := range []struct {
		name      string
		mode      core.RuntimeMode
		coord     core.ControlMutationCoordinator
		wantError bool
	}{
		{"off without coordinator", core.RuntimeModeOff, nil, false},
		{"manual with coordinator", core.RuntimeModeManual, present, false},
		{"manual without coordinator would write ungated", core.RuntimeModeManual, nil, true},
		{"off with coordinator would have side effects", core.RuntimeModeOff, present, true},
		{"unknown mode", core.RuntimeMode("observe"), present, true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := core.BindCoordinatorToMode(testCase.mode, testCase.coord)
			if (err != nil) != testCase.wantError {
				t.Fatalf("mode=%s coordinator=%t err=%v", testCase.mode, testCase.coord != nil, err)
			}
		})
	}
}

type stubCoordinator struct{}

func (stubCoordinator) Acquire(context.Context, core.ControlOperationRequest) (core.ControlOperationToken, error) {
	return core.ControlOperationToken{}, nil
}
func (stubCoordinator) Verify(context.Context, core.ControlOperationToken, core.MutationScope) error {
	return nil
}
func (stubCoordinator) Resolve(context.Context, core.ControlOperationToken) error { return nil }
func (stubCoordinator) Mode(context.Context, core.MutationScope) (core.ControlGateMode, error) {
	return core.ControlGateOpen, nil
}
