package scaleplan_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	core "farmgame/internal/scaleplan"
	testkit "farmgame/internal/testkit/scaleplan"
)

func blockedTerminationFixture(t *testing.T) (*scaleFixture, core.Plan, *testkit.FakeQuiescence, *core.BlockedTerminationOwner) {
	t.Helper()
	f := newScaleOutFixture(t)
	f.routePlans.SetNextPhase(core.RoutePlanPhasePaused)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	blocked, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) {
		t.Fatalf("blocked=%s err=%v", blocked.Plan.Status, err)
	}
	quiescence := testkit.NewFakeQuiescence(f.clock.Now)
	owner, err := core.NewBlockedTerminationOwner(core.BlockedTerminationOwnerOptions{
		Repository: f.repository, Quiescence: quiescence, Scope: testMutationScope, Now: f.clock.Now, MaxProofAge: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return f, blocked.Plan, quiescence, owner
}

func TestExecutorRejectsBlockedOwnerFromDifferentRepository(t *testing.T) {
	f := newScaleOutFixture(t)
	otherRepository := testkit.NewMemoryRepository(8, f.clock.Now)
	otherOwner, _ := core.NewBlockedTerminationOwner(core.BlockedTerminationOwnerOptions{Repository: otherRepository,
		Quiescence: testkit.NewFakeQuiescence(f.clock.Now), Scope: testMutationScope, Now: f.clock.Now})
	_, err := core.NewExecutor(core.ExecutorOptions{Repository: f.repository, Workloads: f.workloads, RoutePlans: f.routePlans,
		Holders: f.holders, Placements: f.placements, Exclusions: f.exclusions, BlockedTermination: otherOwner, Scope: testMutationScope, Now: f.clock.Now})
	if !errors.Is(err, core.ErrInvalidPlan) {
		t.Fatalf("mismatched owner accepted: %v", err)
	}
}

func TestBlockedTerminationReceiptVersionIsIndependentOfPlanSchema(t *testing.T) {
	if core.BlockedTerminationReceiptVersion == core.SchemaVersion || core.BlockedTerminationReceiptVersion == core.BlockedTerminationProtocolVersion {
		t.Fatalf("receipt version is not independently frozen: plan=%s protocol=%s receipt=%s", core.SchemaVersion, core.BlockedTerminationProtocolVersion, core.BlockedTerminationReceiptVersion)
	}
}

func TestBlockedTerminationOwnerCommitsAndReplaysExactReceipt(t *testing.T) {
	f, blocked, _, owner := blockedTerminationFixture(t)
	request, err := core.BuildBlockedTerminationRequest(blocked, "termination-1")
	if err != nil {
		t.Fatal(err)
	}
	result, err := owner.TerminalizeBlocked(context.Background(), request)
	if err != nil || result.Status != core.BlockedTerminationCommitted || result.Record.Plan.BlockedTermination == nil ||
		result.Record.Plan.BlockedTermination.ProtocolVersion != core.BlockedTerminationReceiptVersion {
		t.Fatalf("result=%s record=%+v err=%v", result.Status, result.Record.Plan, err)
	}
	replay, err := owner.TerminalizeBlocked(context.Background(), request)
	if err != nil || replay.Status != core.BlockedTerminationAlreadyCommitted || replay.Record.Plan.Revision != result.Record.Plan.Revision {
		t.Fatalf("replay=%s revision=%d/%d err=%v", replay.Status, replay.Record.Plan.Revision, result.Record.Plan.Revision, err)
	}
	second := f.submission.Clone()
	second.IdempotencyKey = "owner-release"
	if _, err := f.executor.Submit(context.Background(), second); err != nil {
		t.Fatalf("lock not released: %v", err)
	}
}

func TestBlockedTerminationOwnerRejectsExpectationAndProofDriftWithoutRelease(t *testing.T) {
	tests := []struct {
		name          string
		mutateRequest func(*core.BlockedTerminationRequest)
		mutateProof   func(*core.BlockedQuiescenceProof)
	}{
		{name: "revision", mutateRequest: func(value *core.BlockedTerminationRequest) { value.ExpectedRevision++ }},
		{name: "reason", mutateRequest: func(value *core.BlockedTerminationRequest) { value.ExpectedBlockedReason += "-other" }},
		{name: "payload", mutateRequest: func(value *core.BlockedTerminationRequest) { value.ExpectedPayloadDigest = string(make([]byte, 64)) }},
		{name: "checkpoint", mutateRequest: func(value *core.BlockedTerminationRequest) { value.ExpectedCheckpointDigest = string(make([]byte, 64)) }},
		{name: "proof child count", mutateProof: func(value *core.BlockedQuiescenceProof) { value.ChildCount++ }},
		{name: "proof inventory", mutateProof: func(value *core.BlockedQuiescenceProof) { value.ChildInventoryDigest = strings.Repeat("A", 64) }},
		{name: "proof expired", mutateProof: func(value *core.BlockedQuiescenceProof) { value.ExpiresAtMs = value.CapturedAtMs }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f, blocked, quiescence, owner := blockedTerminationFixture(t)
			request, _ := core.BuildBlockedTerminationRequest(blocked, "termination-drift")
			if test.mutateRequest != nil {
				test.mutateRequest(&request)
			}
			if test.mutateProof != nil {
				proof, _ := quiescence.ProofForPlan(blocked)
				test.mutateProof(&proof)
				quiescence.SetProof(proof)
			}
			result, err := owner.TerminalizeBlocked(context.Background(), request)
			if err == nil && (result.Status == core.BlockedTerminationCommitted || result.Status == core.BlockedTerminationAlreadyCommitted) {
				t.Fatalf("unsafe commit=%s", result.Status)
			}
			second := f.submission.Clone()
			second.IdempotencyKey = "drift-" + test.name
			if _, err := f.executor.Submit(context.Background(), second); !errors.Is(err, core.ErrWorkloadBusy) {
				t.Fatalf("unsafe proof released workload: %v", err)
			}
		})
	}
}

func TestBlockedTerminationManifestRejectsValidFormatInconsistency(t *testing.T) {
	_, blocked, quiescence, owner := blockedTerminationFixture(t)
	manifestProof, err := quiescence.ProofForPlan(blocked)
	if err != nil {
		t.Fatal(err)
	}
	manifestProof.WorkloadDigest = strings.Repeat("a", 64)
	quiescence.SetProof(manifestProof)
	request, _ := core.BuildBlockedTerminationRequest(blocked, "valid-format-drift")
	result, err := owner.TerminalizeBlocked(context.Background(), request)
	if !errors.Is(err, core.ErrVerification) || result.Status != core.BlockedTerminationUnsafe {
		t.Fatalf("valid-format drift status=%s err=%v", result.Status, err)
	}
}

func TestBlockedTerminationManifestRejectsInvalidWorkloadHolderAndPlacementFacts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*core.BlockedQuiescenceManifest)
	}{
		{name: "ready replicas overflow", mutate: func(m *core.BlockedQuiescenceManifest) { m.Workload.State.ReadyReplicas++ }},
		{name: "empty pod name", mutate: func(m *core.BlockedQuiescenceManifest) { m.Workload.State.Pods[0].PodName = "" }},
		{name: "expired holder lease", mutate: func(m *core.BlockedQuiescenceManifest) {
			m.Workload.Holders[0].LeaseUntil = time.UnixMilli(m.CapturedAtMs)
		}},
		{name: "placement missing holder", mutate: func(m *core.BlockedQuiescenceManifest) { m.Placement.Placements[0].Holder = core.HolderRef{} }},
		{name: "placement missing endpoint", mutate: func(m *core.BlockedQuiescenceManifest) { m.Placement.Placements[0].Endpoint = "" }},
		{name: "placement nonexistent holder", mutate: func(m *core.BlockedQuiescenceManifest) {
			m.Placement.Placements[0].Holder.Ordinal = 99
			m.Placement.Placements[0].ExpectedSourceFence.ExpectedHolderID = m.Placement.Placements[0].Holder.HolderID
		}},
		{name: "placement cross domain", mutate: func(m *core.BlockedQuiescenceManifest) { m.Placement.Placements[0].Holder.Domain = "FARM" }},
		{name: "duplicate pod uid", mutate: func(m *core.BlockedQuiescenceManifest) {
			m.Workload.State.Pods[1].PodUID = m.Workload.State.Pods[0].PodUID
		}},
		{name: "duplicate holder id", mutate: func(m *core.BlockedQuiescenceManifest) {
			m.Workload.State.Pods[1].HolderID = m.Workload.State.Pods[0].HolderID
		}},
		{name: "holder wrong domain", mutate: func(m *core.BlockedQuiescenceManifest) { m.Workload.Holders[0].Ref.Domain = "FARM" }},
		{name: "readiness contradiction", mutate: func(m *core.BlockedQuiescenceManifest) { m.Workload.State.Pods[0].Ready = false }},
		{name: "placement endpoint mismatch", mutate: func(m *core.BlockedQuiescenceManifest) { m.Placement.Placements[0].Endpoint = "other:9000" }},
		{name: "placement holder zero serving", mutate: func(m *core.BlockedQuiescenceManifest) {
			for index := range m.Workload.Holders {
				if m.Workload.Holders[index].Ref == m.Placement.Placements[0].Holder {
					m.Workload.Holders[index].ServingShardCount = 0
				}
			}
		}},
		{name: "placement holder drained", mutate: func(m *core.BlockedQuiescenceManifest) {
			for index := range m.Workload.Holders {
				if m.Workload.Holders[index].Ref == m.Placement.Placements[0].Holder {
					m.Workload.Holders[index].State = core.HolderDrained
				}
			}
		}},
		{name: "placement holder process unready", mutate: func(m *core.BlockedQuiescenceManifest) {
			for index := range m.Workload.Holders {
				if m.Workload.Holders[index].Ref == m.Placement.Placements[0].Holder {
					m.Workload.Holders[index].ProcessReady = false
				}
			}
		}},
		{name: "placement holder draining", mutate: func(m *core.BlockedQuiescenceManifest) {
			for index := range m.Workload.Holders {
				if m.Workload.Holders[index].Ref == m.Placement.Placements[0].Holder {
					m.Workload.Holders[index].Draining = true
				}
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, blocked, quiescence, owner := blockedTerminationFixture(t)
			manifest := testkit.QuiescenceManifestForTest(blocked, time.UnixMilli(blocked.UpdatedAtMs))
			test.mutate(&manifest)
			quiescence.SetManifest(manifest)
			request, _ := core.BuildBlockedTerminationRequest(blocked, "invalid-facts-"+test.name)
			if result, err := owner.TerminalizeBlocked(context.Background(), request); !errors.Is(err, core.ErrVerification) || result.Status != core.BlockedTerminationUnsafe {
				t.Fatalf("status=%s err=%v", result.Status, err)
			}
		})
	}
}

// Section 8.6 replaced Manager.ObserveTransition as the sole quiescence source
// because that API takes one transition ID and therefore cannot state a
// negative over a set it was never given. The evidence has to be an
// enumeration with a cardinality assertion. These cases pin the failure side
// of that: per-move records alone must not be enough, and an enumeration that
// nobody ran must read as blocked rather than as quiet.
func universalQuiescenceRejections() []struct {
	name   string
	mutate func(*core.BlockedQuiescenceManifest)
} {
	return []struct {
		name   string
		mutate func(*core.BlockedQuiescenceManifest)
	}{
		{name: "enumeration never ran", mutate: func(m *core.BlockedQuiescenceManifest) {
			m.ManagerTransitions.Universal = core.ManagerQuiescenceEnumeration{}
		}},
		{name: "enumeration flag cleared but counts zero", mutate: func(m *core.BlockedQuiescenceManifest) {
			m.ManagerTransitions.Universal.Enumerated = false
		}},
		{name: "unfinished recovery in the domain", mutate: func(m *core.BlockedQuiescenceManifest) {
			m.ManagerTransitions.Universal.UnfinishedRecoveries = 1
		}},
		{name: "unfinished tombstone in the domain", mutate: func(m *core.BlockedQuiescenceManifest) {
			m.ManagerTransitions.Universal.UnfinishedTombstones = 1
		}},
		{name: "enumeration from another domain", mutate: func(m *core.BlockedQuiescenceManifest) {
			m.ManagerTransitions.Universal.Domain = "OTHER"
		}},
		{name: "enumeration not anchored to a revision", mutate: func(m *core.BlockedQuiescenceManifest) {
			m.ManagerTransitions.Universal.AssignmentRevision = 0
		}},
		{name: "enumeration not anchored in time", mutate: func(m *core.BlockedQuiescenceManifest) {
			m.ManagerTransitions.Universal.EnumeratedAtMs = 0
		}},
	}
}

func TestBlockedTerminationManifestRejectsIncompleteTopologyAndManagerJoin(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*core.BlockedQuiescenceManifest)
	}{
		{name: "holders omitted", mutate: func(m *core.BlockedQuiescenceManifest) { m.Workload.Holders = nil }},
		{name: "placements omitted", mutate: func(m *core.BlockedQuiescenceManifest) { m.Placement.Placements = nil; m.Placement.TotalActive = 0 }},
		{name: "placement snapshot identity missing", mutate: func(m *core.BlockedQuiescenceManifest) { m.Placement.SnapshotChecksum = "" }},
		{name: "placement total mismatch", mutate: func(m *core.BlockedQuiescenceManifest) { m.Placement.TotalActive++ }},
		{name: "placement snapshot revision zero", mutate: func(m *core.BlockedQuiescenceManifest) { m.Placement.Placements[0].SnapshotRevision = 0 }},
		{name: "placement snapshot revision mismatch", mutate: func(m *core.BlockedQuiescenceManifest) { m.Placement.Placements[0].SnapshotRevision++ }},
		{name: "manager transition mismatch", mutate: func(m *core.BlockedQuiescenceManifest) { m.Children.Children[0].ManagerTransitionID = "other" }},
		{name: "child nonterminal pending", mutate: func(m *core.BlockedQuiescenceManifest) { m.Children.Children[0].Phase = core.RoutePlanPhasePending }},
		{name: "child nonterminal active", mutate: func(m *core.BlockedQuiescenceManifest) { m.Children.Children[0].Phase = core.RoutePlanPhaseActive }},
		{name: "child not durably stopped", mutate: func(m *core.BlockedQuiescenceManifest) { m.Children.Children[0].DurablyStopped = false }},
		{name: "component retention too short", mutate: func(m *core.BlockedQuiescenceManifest) {
			m.Children.Children[0].EvidenceRetainUntilMs = m.ExpiresAtMs - 1
		}},
	}
	tests = append(tests, universalQuiescenceRejections()...)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, blocked, quiescence, owner := blockedTerminationFixture(t)
			manifest := testkit.QuiescenceManifestForTest(blocked, time.UnixMilli(blocked.UpdatedAtMs))
			test.mutate(&manifest)
			quiescence.SetManifest(manifest)
			request, _ := core.BuildBlockedTerminationRequest(blocked, "manifest-"+test.name)
			if result, err := owner.TerminalizeBlocked(context.Background(), request); !errors.Is(err, core.ErrVerification) || result.Status != core.BlockedTerminationUnsafe {
				t.Fatalf("status=%s err=%v", result.Status, err)
			}
		})
	}
}

func TestBlockedTerminationManifestAcceptsCompleteTopologyBeyondMoveSet(t *testing.T) {
	_, blocked, quiescence, owner := blockedTerminationFixture(t)
	manifest := testkit.QuiescenceManifestForTest(blocked, time.UnixMilli(blocked.UpdatedAtMs))
	extra := manifest.Placement.Placements[0]
	extra.VirtualShard++
	manifest.Placement.Placements = append(manifest.Placement.Placements, extra)
	manifest.Placement.TotalActive++
	quiescence.SetManifest(manifest)
	request, _ := core.BuildBlockedTerminationRequest(blocked, "complete-extra-topology")
	if result, err := owner.TerminalizeBlocked(context.Background(), request); err != nil || result.Status != core.BlockedTerminationCommitted {
		t.Fatalf("status=%s err=%v", result.Status, err)
	}
}

func TestBlockedTerminationManifestAcceptsMultiMoveSinglePlacementSnapshot(t *testing.T) {
	f := newScaleOutFixture(t)
	source, found, _ := f.holders.GetHolder(context.Background(), core.HolderRef{Domain: f.submission.Domain, HolderID: "player-svr-0", Ordinal: 0})
	if !found {
		t.Fatal("source missing")
	}
	_ = f.holders.Update(source.Ref, func(value *core.HolderState) { value.ServingShardCount = 2 })
	f.placements.Set(shard(f.now, 6, source.Ref, source.LogicEndpoint))
	f.submission.Moves = append(f.submission.Moves, core.ShardMoveIntent{Domain: f.submission.Domain, VirtualShard: 6,
		SourceHolderID: source.Ref.HolderID, TargetHolderID: "player-svr-1", TargetOrdinal: 1})
	f.routePlans.SetNextPhase(core.RoutePlanPhasePending)
	record, _ := f.executor.Submit(context.Background(), f.submission)
	record, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) {
		t.Fatalf("first pending=%v", err)
	}
	f.routePlans.SetView(record.Plan.ResolvedMoves[0].ChildPlanID, core.RoutePlanPhaseSucceeded)
	f.routePlans.SetNextPhase(core.RoutePlanPhasePending)
	record, err = f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrPending) || len(record.Plan.ResolvedMoves) != 2 {
		t.Fatalf("second pending children=%d err=%v", len(record.Plan.ResolvedMoves), err)
	}
	f.routePlans.SetView(record.Plan.ResolvedMoves[1].ChildPlanID, core.RoutePlanPhasePaused)
	blocked, err := f.executor.Execute(context.Background(), record.Plan.PlanID)
	if !errors.Is(err, core.ErrBlocked) || len(blocked.Plan.ResolvedMoves) != 2 {
		t.Fatalf("blocked=%s children=%d err=%v", blocked.Plan.Status, len(blocked.Plan.ResolvedMoves), err)
	}
	manifest := testkit.QuiescenceManifestForTest(blocked.Plan, f.clock.Now())
	for _, placement := range manifest.Placement.Placements {
		if placement.SnapshotRevision != manifest.Placement.SnapshotRevision {
			t.Fatalf("revision=%d/%d", placement.SnapshotRevision, manifest.Placement.SnapshotRevision)
		}
	}
	proof, err := core.BuildBlockedQuiescenceProof(blocked.Plan, manifest)
	if err != nil || proof.ChildCount == 0 {
		t.Fatalf("proof children=%d err=%v", proof.ChildCount, err)
	}
}

func TestBlockedTerminationOwnerRecoversApplyThenUnknownByExactReplay(t *testing.T) {
	f, blocked, _, owner := blockedTerminationFixture(t)
	request, _ := core.BuildBlockedTerminationRequest(blocked, "termination-unknown")
	f.repository.ApplyThenFailNextBlockedTermination(core.ErrOutcomeUnknown)
	first, err := owner.TerminalizeBlocked(context.Background(), request)
	if !errors.Is(err, core.ErrOutcomeUnknown) || first.Record.Plan.Status != core.StatusFailed {
		t.Fatalf("first status=%s plan=%s err=%v", first.Status, first.Record.Plan.Status, err)
	}
	replayed, err := owner.TerminalizeBlocked(context.Background(), request)
	if err != nil || replayed.Status != core.BlockedTerminationAlreadyCommitted || replayed.Record.Plan.Revision != first.Record.Plan.Revision {
		t.Fatalf("replay status=%s revision=%d/%d err=%v", replayed.Status, replayed.Record.Plan.Revision, first.Record.Plan.Revision, err)
	}
}

func TestBlockedTerminationReplayRejectsRetainedCheckpointDrift(t *testing.T) {
	f, blocked, _, owner := blockedTerminationFixture(t)
	request, _ := core.BuildBlockedTerminationRequest(blocked, "checkpoint-drift")
	committed, err := owner.TerminalizeBlocked(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := committed.Record.Plan.Clone()
	corrupt.BlockedTermination.ExpectedBlockedUpdatedAtMs++
	f.repository.ForceReplaceForTest(corrupt)
	if replay, err := owner.TerminalizeBlocked(context.Background(), request); err == nil || replay.Status == core.BlockedTerminationAlreadyCommitted {
		t.Fatalf("status=%s err=%v", replay.Status, err)
	}
}

func TestBlockedTerminationReplayRejectsProofReceiptExpectationDrift(t *testing.T) {
	f, blocked, _, owner := blockedTerminationFixture(t)
	request, _ := core.BuildBlockedTerminationRequest(blocked, "expectation-drift")
	committed, err := owner.TerminalizeBlocked(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := committed.Record.Plan.Clone()
	corrupt.BlockedTerminationProof.ExpectedBlockedReason = "OTHER"
	digest, _ := core.BlockedQuiescenceProofDigest(*corrupt.BlockedTerminationProof)
	corrupt.BlockedTermination.ProofDigest = digest
	f.repository.ForceReplaceForTest(corrupt)
	if replay, err := owner.TerminalizeBlocked(context.Background(), request); err == nil || replay.Status == core.BlockedTerminationAlreadyCommitted {
		t.Fatalf("status=%s err=%v", replay.Status, err)
	}
}

func TestBlockedTerminationReplayRejectsCanonicalProofDigestCorruption(t *testing.T) {
	f, blocked, _, owner := blockedTerminationFixture(t)
	request, _ := core.BuildBlockedTerminationRequest(blocked, "corrupt-replay")
	committed, err := owner.TerminalizeBlocked(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := committed.Record.Plan.Clone()
	corrupt.BlockedTermination.ProofDigest = strings.Repeat("a", 64)
	f.repository.ForceReplaceForTest(corrupt)
	if replay, err := owner.TerminalizeBlocked(context.Background(), request); err == nil || replay.Status == core.BlockedTerminationAlreadyCommitted {
		t.Fatalf("corrupt replay status=%s err=%v", replay.Status, err)
	}
}

func TestBlockedTerminationReleasesFenceReturnedWithAcquireError(t *testing.T) {
	_, blocked, quiescence, owner := blockedTerminationFixture(t)
	quiescence.SetAcquireErrorWithFence(core.ErrDependency)
	request, _ := core.BuildBlockedTerminationRequest(blocked, "acquire-error-fence")
	if _, err := owner.TerminalizeBlocked(context.Background(), request); !errors.Is(err, core.ErrDependency) {
		t.Fatalf("err=%v", err)
	}
	if calls := quiescence.ReleaseCalls(); calls != 1 {
		t.Fatalf("release calls=%d", calls)
	}
}

func TestBlockedTerminationCleanupIgnoresCanceledOperationContext(t *testing.T) {
	f, blocked, quiescence, owner := blockedTerminationFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	f.repository.SetBeforeBlockedTerminationCommit(cancel)
	var releaseCanceled bool
	quiescence.SetReleaseHook(func(ctx context.Context) { releaseCanceled = ctx.Err() != nil })
	request, _ := core.BuildBlockedTerminationRequest(blocked, "cancel-cleanup")
	result, err := owner.TerminalizeBlocked(ctx, request)
	if result.Status != core.BlockedTerminationCommitted || err != nil || releaseCanceled {
		t.Fatalf("status=%s err=%v release_canceled=%t", result.Status, err, releaseCanceled)
	}
}

func TestBlockedTerminationRepositoryChecksExpiryAtLinearization(t *testing.T) {
	f, blocked, _, owner := blockedTerminationFixture(t)
	request, _ := core.BuildBlockedTerminationRequest(blocked, "termination-stalled")
	f.repository.SetBeforeBlockedTerminationCommit(func() { f.clock.Advance(2 * time.Minute) })
	result, err := owner.TerminalizeBlocked(context.Background(), request)
	if !errors.Is(err, core.ErrVerification) || result.Status != core.BlockedTerminationProofExpired {
		t.Fatalf("stalled commit status=%s err=%v", result.Status, err)
	}
	second := f.submission.Clone()
	second.IdempotencyKey = "stalled-lock"
	if _, err := f.executor.Submit(context.Background(), second); !errors.Is(err, core.ErrWorkloadBusy) {
		t.Fatalf("expired linearization released workload: %v", err)
	}
}

// The barrier is now reached only through exported entry points, so these two
// tests probe it from inside the pre-commit hook, which runs while the first
// owner holds it. That is closer to how a second controller would actually
// collide than calling the barrier directly ever was.
func TestBlockedTerminationOwnersShareRepositoryBarrier(t *testing.T) {
	f, blocked, _, first := blockedTerminationFixture(t)
	second, err := core.NewBlockedTerminationOwner(core.BlockedTerminationOwnerOptions{Repository: f.repository,
		Quiescence: testkit.NewFakeQuiescence(f.clock.Now), Scope: testMutationScope, Now: f.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	secondExecutor, err := core.NewExecutor(core.ExecutorOptions{Repository: f.repository, Workloads: f.workloads,
		RoutePlans: f.routePlans, Holders: f.holders, Placements: f.placements, Exclusions: f.exclusions,
		BlockedTermination: second, Scope: testMutationScope, Now: f.clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	request, err := core.BuildBlockedTerminationRequest(blocked, "termination-barrier")
	if err != nil {
		t.Fatal(err)
	}
	var probeErr error
	f.repository.SetBeforeBlockedTerminationCommit(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		_, probeErr = secondExecutor.Pause(ctx, blocked.PlanID, "barrier probe")
	})
	if _, err := first.TerminalizeBlocked(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(probeErr, context.DeadlineExceeded) {
		t.Fatalf("second owner bypassed shared barrier: %v", probeErr)
	}
}

func TestBlockedTerminationOwnerMutationBarrierHonorsContextAndReopens(t *testing.T) {
	f, blocked, _, owner := blockedTerminationFixture(t)
	request, err := core.BuildBlockedTerminationRequest(blocked, "termination-reopen")
	if err != nil {
		t.Fatal(err)
	}
	var probeErr error
	f.repository.SetBeforeBlockedTerminationCommit(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		_, probeErr = f.executor.Pause(ctx, blocked.PlanID, "barrier probe")
	})
	if _, err := owner.TerminalizeBlocked(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(probeErr, context.DeadlineExceeded) {
		t.Fatalf("barrier waiter err=%v", probeErr)
	}
	f.repository.SetBeforeBlockedTerminationCommit(nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := f.executor.Step(ctx, blocked.PlanID); errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("barrier did not reopen after release")
	}
}

func TestBlockedTerminationOwnerRevalidationRaceFailsCAS(t *testing.T) {
	f, blocked, quiescence, owner := blockedTerminationFixture(t)
	request, _ := core.BuildBlockedTerminationRequest(blocked, "termination-race")
	quiescence.SetRevalidateHook(func() {
		current, _, _ := f.repository.Load(context.Background(), blocked.PlanID)
		candidate := current.Plan.Clone()
		candidate.BlockedReason = "RECOVERY_RETENTION_EXCEEDED"
		candidate.Revision++
		candidate.UpdatedAtMs++
		_, _ = f.repository.CompareAndSwap(context.Background(), current.Plan.Revision, candidate)
	})
	result, err := owner.TerminalizeBlocked(context.Background(), request)
	if err == nil || result.Status == core.BlockedTerminationCommitted {
		t.Fatalf("raced proof committed status=%s err=%v", result.Status, err)
	}
	second := f.submission.Clone()
	second.IdempotencyKey = "race-lock"
	if _, err := f.executor.Submit(context.Background(), second); !errors.Is(err, core.ErrWorkloadBusy) {
		t.Fatalf("race released workload: %v", err)
	}
}
