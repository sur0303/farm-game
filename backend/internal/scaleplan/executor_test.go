package scaleplan_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/scaleplan"
	"farmgame/internal/storage/authoritystore"
	testkit "farmgame/internal/testkit/scaleplan"
)

type scaleFixture struct {
	now                time.Time
	clock              *testkit.ManualClock
	repository         testkit.MemoryRepository
	workloads          *testkit.FakeWorkloadScaler
	holders            *testkit.FakeHolders
	placements         *testkit.FakePlacements
	exclusions         *testkit.FakeExclusions
	blockedTermination *core.BlockedTerminationOwner
	blockedQuiescence  *testkit.FakeQuiescence
	routePlans         *testkit.FakeRoutePlans
	executor           *core.Executor
	submission         core.Submission
}

// testMutationScope matches the fixture workload's namespace and realm. Submit
// rejects anything outside the leased scope, so the two must agree.
var testMutationScope = core.MutationScope{Namespace: "farm", Realm: "realm-a"}

func newBlockedOwner(t *testing.T, repository core.ScalePlanRepository, clock func() time.Time) (*core.BlockedTerminationOwner, *testkit.FakeQuiescence) {
	t.Helper()
	quiescence := testkit.NewFakeQuiescence(clock)
	owner, err := core.NewBlockedTerminationOwner(core.BlockedTerminationOwnerOptions{Repository: repository, Quiescence: quiescence, Scope: testMutationScope, Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	return owner, quiescence
}

func newExecutor(t *testing.T, f *scaleFixture) *core.Executor {
	t.Helper()
	executor, err := core.NewExecutor(core.ExecutorOptions{
		Repository: f.repository, Workloads: f.workloads, RoutePlans: f.routePlans,
		Holders: f.holders, Placements: f.placements, Exclusions: f.exclusions,
		BlockedTermination: f.blockedTermination, Scope: testMutationScope, Now: f.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return executor
}

func newScaleOutFixture(t *testing.T) *scaleFixture {
	t.Helper()
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	workload := core.WorkloadRef{Realm: "realm-a", Domain: actorrouting.DomainPlayer, Namespace: "farm", Kind: "StatefulSet", Name: "player-svr"}
	uid := "workload-player-v1"
	source := holder(now, uid, "player-svr-0", "boot-0", 0, core.HolderServing, 1)
	target := holder(now, uid, "player-svr-1", "boot-1", 1, core.HolderReadyZeroShard, 0)
	holders := testkit.NewFakeHolders(source)
	placement := shard(now, 7, source.Ref, source.LogicEndpoint)
	placements := testkit.NewFakePlacements(placement)
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
	routePlans := testkit.NewFakeRoutePlans()
	routePlans.SetEnsureHook(func(request core.RoutePlanMoveRequest) {
		placements.Move(request.Domain, request.VirtualShard, request.Target, request.TargetEndpoint)
		_ = holders.Update(source.Ref, func(value *core.HolderState) {
			value.ServingShardCount = 0
			value.State = core.HolderReadyZeroShard
		})
		_ = holders.Update(target.Ref, func(value *core.HolderState) {
			value.ServingShardCount = 1
			value.State = core.HolderServing
		})
	})
	exclusions := testkit.NewFakeExclusions(holders,
		core.PlacementExclusion{Holder: source.Ref, Revision: 1},
		core.PlacementExclusion{Holder: target.Ref, Revision: 1})
	clock := testkit.NewManualClock(now)
	repository := testkit.NewMemoryRepository(128, clock.Now)
	owner, quiescence := newBlockedOwner(t, repository, clock.Now)
	f := &scaleFixture{
		now: now, clock: clock, repository: repository, workloads: workloads,
		holders: holders, placements: placements, exclusions: exclusions,
		blockedTermination: owner, blockedQuiescence: quiescence, routePlans: routePlans,
		submission: core.Submission{
			IdempotencyKey: "scale-out-one", Realm: workload.Realm, Domain: workload.Domain, Operation: core.OperationScaleOut,
			Workload: workload, ExpectedReplicas: 1, DesiredReplicas: 2,
			Moves:          []core.ShardMoveIntent{{Domain: workload.Domain, VirtualShard: 7, SourceHolderID: source.Ref.HolderID, TargetHolderID: target.Ref.HolderID, TargetOrdinal: 1}},
			RoutePlanTTLms: time.Minute.Milliseconds(), PlanTTLms: time.Hour.Milliseconds(),
			RecoveryRetentionMs: 24 * time.Hour.Milliseconds(),
			Limits:              core.FunctionalLimits{MaxRoutePlansInflight: 1, MaxShardsPerWave: 1},
		},
	}
	f.executor = newExecutor(t, f)
	return f
}

func newScaleInFixture(t *testing.T) *scaleFixture {
	t.Helper()
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	workload := core.WorkloadRef{Realm: "realm-a", Domain: actorrouting.DomainPlayer, Namespace: "farm", Kind: "StatefulSet", Name: "player-svr"}
	uid := "workload-player-v1"
	target := holder(now, uid, "player-svr-0", "boot-0", 0, core.HolderServing, 1)
	victim := holder(now, uid, "player-svr-1", "boot-1", 1, core.HolderServing, 1)
	holders := testkit.NewFakeHolders(target, victim)
	placements := testkit.NewFakePlacements(shard(now, 8, victim.Ref, victim.LogicEndpoint))
	workloads := testkit.NewFakeWorkloadScaler(core.WorkloadState{
		Ref: workload, UID: uid, ResourceVersion: "200", Generation: 2, ObservedGeneration: 2,
		Replicas: 2, ReadyReplicas: 2,
		Pods: []core.WorkloadPodState{
			{PodName: "player-svr-0", PodUID: target.Ref.PodUID, WorkloadUID: uid, Ordinal: 0, HolderID: target.Ref.HolderID, Present: true, Ready: true},
			{PodName: "player-svr-1", PodUID: victim.Ref.PodUID, WorkloadUID: uid, Ordinal: 1, HolderID: victim.Ref.HolderID, Present: true, Ready: true},
		},
	})
	workloads.SetScaleHook(func(state *core.WorkloadState, desired int32) {
		state.ReadyReplicas = desired
		if desired == 1 {
			state.Pods = state.Pods[:1]
		}
	})
	routePlans := testkit.NewFakeRoutePlans()
	routePlans.SetEnsureHook(func(request core.RoutePlanMoveRequest) {
		placements.Move(request.Domain, request.VirtualShard, request.Target, request.TargetEndpoint)
		_ = holders.Update(victim.Ref, func(value *core.HolderState) {
			value.ServingShardCount = 0
			value.State = core.HolderReadyZeroShard
		})
		_ = holders.Update(target.Ref, func(value *core.HolderState) { value.ServingShardCount = 2 })
	})
	exclusions := testkit.NewFakeExclusions(holders,
		core.PlacementExclusion{Holder: target.Ref, Revision: 1},
		core.PlacementExclusion{Holder: victim.Ref, Revision: 1})
	clock := testkit.NewManualClock(now)
	repository := testkit.NewMemoryRepository(128, clock.Now)
	owner, quiescence := newBlockedOwner(t, repository, clock.Now)
	quiescence.SetExclusions(exclusions)
	f := &scaleFixture{
		now: now, clock: clock, repository: repository, workloads: workloads,
		holders: holders, placements: placements, exclusions: exclusions,
		blockedTermination: owner, blockedQuiescence: quiescence, routePlans: routePlans,
		submission: core.Submission{
			IdempotencyKey: "scale-in-one", Realm: workload.Realm, Domain: workload.Domain, Operation: core.OperationScaleIn,
			Workload: workload, ExpectedReplicas: 2, DesiredReplicas: 1,
			Moves:          []core.ShardMoveIntent{{Domain: workload.Domain, VirtualShard: 8, SourceHolderID: victim.Ref.HolderID, TargetHolderID: target.Ref.HolderID, TargetOrdinal: 0}},
			RoutePlanTTLms: time.Minute.Milliseconds(), PlanTTLms: time.Hour.Milliseconds(),
			RecoveryRetentionMs: 24 * time.Hour.Milliseconds(),
			Limits:              core.FunctionalLimits{MaxRoutePlansInflight: 1, MaxShardsPerWave: 1},
		},
	}
	f.executor = newExecutor(t, f)
	return f
}

func newDrainHolderFixture(t *testing.T) *scaleFixture {
	t.Helper()
	f := newScaleInFixture(t)
	f.submission.IdempotencyKey = "drain-holder-one"
	f.submission.Operation = core.OperationDrainHolder
	f.submission.DesiredReplicas = f.submission.ExpectedReplicas
	return f
}

func holder(now time.Time, workloadUID, id, boot string, ordinal int32, state core.HolderLifecycleState, shards uint32) core.HolderState {
	return core.HolderState{
		Ref: core.HolderRef{Domain: actorrouting.DomainPlayer, HolderID: id, BootID: boot,
			PodUID: "pod-uid-" + id, WorkloadUID: workloadUID, Ordinal: ordinal},
		PodName: id, LogicEndpoint: id + ".farm.svc:9000", HolderControlEndpoint: id + ".farm.svc:9443",
		TLSDNSIdentity: id + ".farm.svc", State: state, ProcessReady: true, Draining: state == core.HolderDraining || state == core.HolderDrained,
		ServingShardCount: shards, SnapshotRevision: 10, AssignmentKnownFresh: true,
		PlacementEligible: true, PlacementRevision: 1, LeaseUntil: now.Add(time.Hour),
	}
}

func shard(now time.Time, id uint32, source core.HolderRef, endpoint string) core.ShardPlacement {
	return core.ShardPlacement{
		Domain: source.Domain, VirtualShard: id, Active: true, SnapshotRevision: 10, Holder: source, Endpoint: endpoint,
		ExpectedSourceFence: authoritystore.ShardWriteFence{ExpectedHolderID: source.HolderID, ExpectedHolderBootID: source.BootID,
			ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1},
		KnownFresh: true, LeaseUntil: now.Add(time.Hour),
	}
}

func TestScalePlanScaleOutHappyPath(t *testing.T) {
	f := newScaleOutFixture(t)
	record, err := f.executor.Submit(context.Background(), f.submission)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil {
		t.Fatalf("execute: %v phase=%s status=%s", err, result.Plan.Phase, result.Plan.Status)
	}
	if result.Plan.Status != core.StatusSucceeded || len(result.Plan.ResolvedMoves) != 1 || !result.Plan.ResolvedMoves[0].Verified {
		t.Fatalf("unexpected result: %+v", result.Plan)
	}
	if f.workloads.Calls() != 1 || len(f.routePlans.Requests()) != 1 {
		t.Fatalf("calls scale=%d routeplans=%d", f.workloads.Calls(), len(f.routePlans.Requests()))
	}
}

func TestScalePlanAdoptsInformerObservedScaleOutBeforeMovingShards(t *testing.T) {
	f := newScaleOutFixture(t)
	target := holder(f.now, "workload-player-v1", "player-svr-1", "boot-1", 1, core.HolderReadyZeroShard, 0)
	state := f.workloads.State()
	state.Replicas = 2
	state.ReadyReplicas = 2
	state.Generation++
	state.ObservedGeneration = state.Generation
	state.ResourceVersion = "already-scaled-rv"
	state.Pods = append(state.Pods, core.WorkloadPodState{
		PodName: target.PodName, PodUID: target.Ref.PodUID, WorkloadUID: target.Ref.WorkloadUID,
		Ordinal: target.Ref.Ordinal, HolderID: target.Ref.HolderID, Present: true, Ready: true,
	})
	f.workloads.SetState(state)
	f.holders.Set(target)

	record, err := f.executor.Submit(context.Background(), f.submission)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil {
		t.Fatalf("execute: %v phase=%s status=%s", err, result.Plan.Phase, result.Plan.Status)
	}
	if result.Plan.Status != core.StatusSucceeded || result.Plan.ScaleIntent == nil ||
		result.Plan.ScaleIntent.Effect != core.EffectAdopted {
		t.Fatalf("external scale-out was not adopted: %+v", result.Plan)
	}
	if f.workloads.Calls() != 0 {
		t.Fatalf("adopted scale-out unexpectedly patched Kubernetes: calls=%d", f.workloads.Calls())
	}
}

func TestScalePlanScaleOutMultipleWavesRemainDeterministic(t *testing.T) {
	f := newScaleOutFixture(t)
	source, found, err := f.holders.GetHolder(context.Background(), core.HolderRef{Domain: f.submission.Domain, HolderID: "player-svr-0", BootID: "boot-0", Ordinal: 0})
	if err != nil || !found {
		t.Fatal("source holder missing")
	}
	_ = f.holders.Update(source.Ref, func(value *core.HolderState) { value.ServingShardCount = 2 })
	f.placements.Set(shard(f.now, 6, source.Ref, source.LogicEndpoint))
	f.submission.Moves = append(f.submission.Moves, core.ShardMoveIntent{Domain: f.submission.Domain, VirtualShard: 6,
		SourceHolderID: source.Ref.HolderID, TargetHolderID: "player-svr-1", TargetOrdinal: 1})
	f.routePlans.SetEnsureHook(func(request core.RoutePlanMoveRequest) {
		f.placements.Move(request.Domain, request.VirtualShard, request.Target, request.TargetEndpoint)
		_ = f.holders.Update(source.Ref, func(value *core.HolderState) {
			value.ServingShardCount--
			if value.ServingShardCount == 0 {
				value.State = core.HolderReadyZeroShard
			}
		})
		_ = f.holders.Update(request.Target, func(value *core.HolderState) {
			value.ServingShardCount++
			value.State = core.HolderServing
		})
	})
	record, err := f.executor.Submit(context.Background(), f.submission)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || result.Plan.Status != core.StatusSucceeded || len(result.Plan.ResolvedMoves) != 2 || result.Plan.WaveIndex != 1 {
		t.Fatalf("result=%s/%s wave=%d children=%d err=%v", result.Plan.Status, result.Plan.Phase, result.Plan.WaveIndex, len(result.Plan.ResolvedMoves), err)
	}
	if result.Plan.ResolvedMoves[0].Intent.VirtualShard != 6 || result.Plan.ResolvedMoves[1].Intent.VirtualShard != 7 {
		t.Fatalf("manifest not canonical: %+v", result.Plan.ResolvedMoves)
	}
	if result.Plan.ResolvedMoves[0].ChildPlanID == result.Plan.ResolvedMoves[1].ChildPlanID {
		t.Fatal("child IDs collided")
	}
}

func TestScalePlanScaleInHappyPath(t *testing.T) {
	f := newScaleInFixture(t)
	record, err := f.executor.Submit(context.Background(), f.submission)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil {
		t.Fatalf("execute: %v phase=%s status=%s code=%s", err, result.Plan.Phase, result.Plan.Status, result.Plan.ErrorCode)
	}
	if result.Plan.Status != core.StatusSucceeded || result.Plan.Victim.HolderID != "player-svr-1" || !result.Plan.ResolvedMoves[0].Verified {
		t.Fatalf("unexpected result: %+v", result.Plan)
	}
	if f.exclusions.Calls() != 1 || f.exclusions.ReleaseCalls() != 0 || f.workloads.Calls() != 1 || len(f.routePlans.Requests()) != 1 {
		t.Fatalf("calls exclusion=%d release=%d scale=%d route=%d", f.exclusions.Calls(), f.exclusions.ReleaseCalls(), f.workloads.Calls(), len(f.routePlans.Requests()))
	}
	state, found, err := f.exclusions.Get(context.Background(), result.Plan.Victim)
	if err != nil || !found || !state.Excluded || state.OwnerPlanID != result.Plan.PlanID {
		t.Fatalf("successful scale-in changed exclusion: found=%t state=%+v err=%v", found, state, err)
	}
}

func TestScalePlanDrainHolderHappyPathPreservesReplicaCountAndExclusion(t *testing.T) {
	f := newDrainHolderFixture(t)
	record, err := f.executor.Submit(context.Background(), f.submission)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil {
		t.Fatalf("execute: %v phase=%s status=%s code=%s", err, result.Plan.Phase, result.Plan.Status, result.Plan.ErrorCode)
	}
	if result.Plan.Status != core.StatusSucceeded || result.Plan.Victim.HolderID != "player-svr-1" ||
		!result.Plan.ResolvedMoves[0].Verified {
		t.Fatalf("unexpected result: %+v", result.Plan)
	}
	if f.workloads.Calls() != 0 || f.exclusions.Calls() != 1 || f.exclusions.ReleaseCalls() != 0 {
		t.Fatalf("calls scale=%d exclusion=%d release=%d", f.workloads.Calls(), f.exclusions.Calls(), f.exclusions.ReleaseCalls())
	}
	state, found, err := f.exclusions.Get(context.Background(), result.Plan.Victim)
	if err != nil || !found || !state.Excluded || state.OwnerPlanID != result.Plan.PlanID {
		t.Fatalf("successful holder drain lost exclusion: found=%t state=%+v err=%v", found, state, err)
	}
	workload := f.workloads.State()
	if workload.Replicas != 2 || len(workload.Pods) != 2 {
		t.Fatalf("holder drain changed StatefulSet replicas: %+v", workload)
	}
}

func TestScalePlanDrainHolderPinsExactReplacementGeneration(t *testing.T) {
	f := newDrainHolderFixture(t)
	target, found, err := f.holders.GetHolder(context.Background(), core.HolderRef{
		Domain: f.submission.Domain, HolderID: "player-svr-0", Ordinal: 0,
	})
	if err != nil || !found {
		t.Fatalf("load target: found=%t err=%v", found, err)
	}
	if err := f.holders.Update(target.Ref, func(value *core.HolderState) {
		value.ServingShardCount = 0
		value.State = core.HolderReadyZeroShard
	}); err != nil {
		t.Fatal(err)
	}
	f.submission.PinnedTarget = target.Ref

	record, err := f.executor.Submit(context.Background(), f.submission)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil {
		t.Fatalf("execute: %v phase=%s status=%s", err, result.Plan.Phase, result.Plan.Status)
	}
	if result.Plan.Target != target.Ref || result.Plan.ResolvedMoves[0].Target != target.Ref {
		t.Fatalf("exact replacement was not retained: plan=%+v move=%+v", result.Plan.Target, result.Plan.ResolvedMoves[0].Target)
	}
}

func TestScalePlanDrainHolderRejectsPinnedReplacementRestart(t *testing.T) {
	f := newDrainHolderFixture(t)
	target, found, err := f.holders.GetHolder(context.Background(), core.HolderRef{
		Domain: f.submission.Domain, HolderID: "player-svr-0", Ordinal: 0,
	})
	if err != nil || !found {
		t.Fatalf("load target: found=%t err=%v", found, err)
	}
	if err := f.holders.Update(target.Ref, func(value *core.HolderState) {
		value.ServingShardCount = 0
		value.State = core.HolderReadyZeroShard
	}); err != nil {
		t.Fatal(err)
	}
	f.submission.PinnedTarget = target.Ref
	record, err := f.executor.Submit(context.Background(), f.submission)
	if err != nil {
		t.Fatal(err)
	}
	restarted := target
	restarted.Ref.BootID = "boot-restarted"
	restarted.Ref.PodUID = "pod-uid-restarted"
	f.holders.Set(restarted)

	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || result.Plan.BlockedReason != "DRAIN_TARGET_GENERATION_CHANGED" ||
		len(result.Plan.ResolvedMoves) != 0 {
		t.Fatalf("replacement restart did not fail closed: status=%s reason=%s moves=%d err=%v",
			result.Plan.Status, result.Plan.BlockedReason, len(result.Plan.ResolvedMoves), err)
	}
}

func TestScalePlanScaleInHighestOrdinalAlreadyZeroShard(t *testing.T) {
	f := newScaleInFixture(t)
	if !f.placements.Unassign(f.submission.Domain, 8) {
		t.Fatal("failed to make victim zero-shard")
	}
	victim, found, err := f.holders.GetHolder(context.Background(), core.HolderRef{
		Domain: f.submission.Domain, HolderID: "player-svr-1", Ordinal: 1,
	})
	if err != nil || !found {
		t.Fatalf("load victim: found=%t err=%v", found, err)
	}
	if err := f.holders.Update(victim.Ref, func(value *core.HolderState) {
		value.ServingShardCount = 0
		value.State = core.HolderReadyZeroShard
	}); err != nil {
		t.Fatal(err)
	}
	f.submission.Moves = nil

	record, err := f.executor.Submit(context.Background(), f.submission)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil {
		t.Fatalf("execute: %v phase=%s status=%s code=%s", err, result.Plan.Phase, result.Plan.Status, result.Plan.ErrorCode)
	}
	if result.Plan.Status != core.StatusSucceeded || len(result.Plan.ResolvedMoves) != 0 || result.Plan.WaveIndex != 0 {
		t.Fatalf("unexpected scale-only result: %+v", result.Plan)
	}
	if f.exclusions.Calls() != 1 || f.workloads.Calls() != 1 || len(f.routePlans.Requests()) != 0 {
		t.Fatalf("calls exclusion=%d scale=%d route=%d", f.exclusions.Calls(), f.workloads.Calls(), len(f.routePlans.Requests()))
	}
}

func TestScalePlanDrainHolderAlreadyZeroShardStillOwnsExclusion(t *testing.T) {
	f := newDrainHolderFixture(t)
	if !f.placements.Unassign(f.submission.Domain, 8) {
		t.Fatal("failed to make drain victim zero-shard")
	}
	victim, found, err := f.holders.GetHolder(context.Background(), core.HolderRef{
		Domain: f.submission.Domain, HolderID: "player-svr-1", Ordinal: 1,
	})
	if err != nil || !found {
		t.Fatalf("load victim: found=%t err=%v", found, err)
	}
	if err := f.holders.Update(victim.Ref, func(value *core.HolderState) {
		value.ServingShardCount = 0
		value.State = core.HolderReadyZeroShard
	}); err != nil {
		t.Fatal(err)
	}
	f.submission.Moves = nil
	f.submission.VictimHolderID = victim.Ref.HolderID

	record, err := f.executor.Submit(context.Background(), f.submission)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil {
		t.Fatalf("execute: %v phase=%s status=%s code=%s", err, result.Plan.Phase, result.Plan.Status, result.Plan.ErrorCode)
	}
	if result.Plan.Status != core.StatusSucceeded || result.Plan.Victim != victim.Ref ||
		result.Plan.ExclusionIntent == nil || result.Plan.ExclusionIntent.ObservedRevision == 0 {
		t.Fatalf("unexpected zero-shard drain result: %+v", result.Plan)
	}
	workload := f.workloads.State()
	if workload.Replicas != f.submission.ExpectedReplicas {
		t.Fatalf("zero-shard holder drain changed replicas: %+v", workload)
	}
	exclusion, found, err := f.exclusions.Get(context.Background(), victim.Ref)
	if err != nil || !found || !exclusion.Excluded || exclusion.OwnerPlanID != result.Plan.PlanID {
		t.Fatalf("exact drain exclusion not retained: found=%t state=%+v err=%v", found, exclusion, err)
	}
}

func TestScalePlanIdempotencyAndWorkloadSingleFlight(t *testing.T) {
	f := newScaleOutFixture(t)
	const callers = 16
	ids := make(chan string, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			record, err := f.executor.Submit(context.Background(), f.submission)
			ids <- record.Plan.PlanID
			errs <- err
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	var expected string
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for id := range ids {
		if expected == "" {
			expected = id
		}
		if id != expected {
			t.Fatalf("id=%q want %q", id, expected)
		}
	}
	conflict := f.submission.Clone()
	conflict.RoutePlanTTLms += time.Second.Milliseconds()
	if _, err := f.executor.Submit(context.Background(), conflict); !errors.Is(err, core.ErrIdempotencyConflict) {
		t.Fatalf("idempotency conflict=%v", err)
	}
	busy := f.submission.Clone()
	busy.IdempotencyKey = "different-key"
	if _, err := f.executor.Submit(context.Background(), busy); !errors.Is(err, core.ErrWorkloadBusy) {
		t.Fatalf("workload busy=%v", err)
	}
}

func TestScalePlanTerminalExecuteIsNoOp(t *testing.T) {
	f := newScaleOutFixture(t)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	result, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil {
		t.Fatal(err)
	}
	revision := result.Plan.Revision
	scaleCalls := f.workloads.Calls()
	routeCalls := f.routePlans.EnsureCalls(result.Plan.ResolvedMoves[0].ChildPlanID)
	again, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if err != nil || again.Plan.Revision != revision || f.workloads.Calls() != scaleCalls || f.routePlans.EnsureCalls(result.Plan.ResolvedMoves[0].ChildPlanID) != routeCalls {
		t.Fatalf("terminal replay mutated: err=%v revision=%d/%d", err, again.Plan.Revision, revision)
	}
}
