package control

import (
	"context"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	routecore "farmgame/internal/routecontrol"
	core "farmgame/internal/scaleplan"
	scaleplantestkit "farmgame/internal/testkit/scaleplan"
	testkit "farmgame/internal/testkit/scaleplancontrol"
)

type staticReleaseWorkloads struct {
	state core.WorkloadState
}

func (r *staticReleaseWorkloads) Get(ctx context.Context, ref core.WorkloadRef) (core.WorkloadState, error) {
	if err := ctx.Err(); err != nil {
		return core.WorkloadState{}, err
	}
	if r.state.Ref != ref {
		return core.WorkloadState{}, core.ErrConflict
	}
	return r.state, nil
}

type staticReleaseHolders struct {
	values []core.HolderState
}

func (r *staticReleaseHolders) GetHolder(ctx context.Context, ref core.HolderRef) (core.HolderState, bool, error) {
	if err := ctx.Err(); err != nil {
		return core.HolderState{}, false, err
	}
	for _, value := range r.values {
		if value.Ref == ref {
			return value, true, nil
		}
	}
	return core.HolderState{}, false, nil
}

func (r *staticReleaseHolders) ListDomainHolders(ctx context.Context, domain actorrouting.RouteDomain) ([]core.HolderState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	values := make([]core.HolderState, 0, len(r.values))
	for _, value := range r.values {
		if value.Ref.Domain == domain {
			values = append(values, value)
		}
	}
	return values, nil
}

type staticReleasePlacements struct {
	capture CompletePlacementCapture
}

func (r *staticReleasePlacements) CaptureComplete(ctx context.Context) (CompletePlacementCapture, error) {
	if err := ctx.Err(); err != nil {
		return CompletePlacementCapture{}, err
	}
	return r.capture, nil
}

func (r *staticReleasePlacements) Revalidate(ctx context.Context, _ core.PlacementCaptureToken) error {
	return ctx.Err()
}

type staticReleaseExclusive struct {
	token           core.ControlOperationToken
	revalidateCalls int
}

func (r *staticReleaseExclusive) ControlToken() core.ControlOperationToken {
	return r.token
}

func (r *staticReleaseExclusive) Revalidate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.revalidateCalls++
	return nil
}

func (r *staticReleaseExclusive) Release(ctx context.Context) error {
	return ctx.Err()
}

func blockedReleaseTestPlan(holder core.HolderRef, now time.Time) core.Plan {
	submission := core.Submission{
		IdempotencyKey: "idempotency-a", Realm: "realm-a", Domain: holder.Domain,
		Operation: core.OperationScaleIn, Workload: core.WorkloadRef{
			Realm: "realm-a", Domain: holder.Domain, Namespace: "farm",
			Kind: "StatefulSet", Name: "workload-a",
		},
		ExpectedReplicas: 2, DesiredReplicas: 1,
		Moves: []core.ShardMoveIntent{{
			Domain: holder.Domain, VirtualShard: 0,
			SourceHolderID: holder.HolderID, TargetHolderID: "target-a", TargetOrdinal: 0,
		}},
		RoutePlanTTLms: 60000, PlanTTLms: 120000, RecoveryRetentionMs: 120000,
		Limits: core.FunctionalLimits{MaxRoutePlansInflight: 1, MaxShardsPerWave: 1},
	}
	normalized, err := submission.Normalize()
	if err != nil {
		panic(err)
	}
	payloadDigest, err := core.SubmissionPayloadDigest(normalized)
	if err != nil {
		panic(err)
	}
	planID, err := core.DeterministicPlanID(normalized)
	if err != nil {
		panic(err)
	}
	return core.Plan{
		SchemaVersion: core.SchemaVersion, PlanID: planID,
		IdempotencyKey: normalized.IdempotencyKey, PayloadDigest: payloadDigest,
		Submission: normalized, Status: core.StatusBlocked,
		Phase: core.PhaseCapturingVictimShards, Revision: 3, Attempt: 1,
		BlockedReason:    "TEST_BLOCKED",
		WorkloadIdentity: core.WorkloadIdentity{UID: holder.WorkloadUID, ResourceVersion: "rv-a"},
		Victim:           holder,
		ExclusionIntent: &core.ExclusionIntent{
			Victim: holder, ExpectedRevision: 7, ObservedRevision: 8,
			OwnerPlanID: planID, Effect: core.EffectCommitted,
		},
		CreatedAtMs:           now.Add(-2 * time.Second).UnixMilli(),
		UpdatedAtMs:           now.Add(-time.Second).UnixMilli(),
		StartDeadlineMs:       now.Add(30 * time.Second).UnixMilli(),
		RecoveryRetainUntilMs: now.Add(2 * time.Minute).UnixMilli(),
	}
}

type blockedReleaseFixture struct {
	port      *blockedQuiescencePort
	fence     *blockedQuiescenceFence
	plan      core.Plan
	holders   *testkit.MemoryHolders
	exclusive *staticReleaseExclusive
}

func newBlockedReleaseFixture(t *testing.T, forced routecore.PlacementReleaseOutcome) blockedReleaseFixture {
	t.Helper()
	base := newPlacementAdapterFixture(t, true, 8, "plan-a")
	plan := blockedReleaseTestPlan(base.holder, base.clock.Now())
	if err := plan.Validate(); err != nil {
		t.Fatalf("test plan invalid: %v", err)
	}
	base.holders.SetExclusion(routecore.PlacementExclusion{
		Domain: base.holder.Domain, HolderID: base.holder.HolderID, BootID: base.holder.BootID,
		Excluded: true, Revision: 8, OwnerPlanID: plan.PlanID,
	})
	manifest := scaleplantestkit.QuiescenceManifestForTest(plan, base.clock.Now())
	manifest.Workload.Holders[0].LogicEndpoint = manifest.Placement.Placements[0].Endpoint
	manifest.Exclusion = core.BlockedExclusionManifest{
		Present: true,
		State: core.PlacementExclusion{
			Holder: base.holder, Excluded: true, Revision: 8, OwnerPlanID: plan.PlanID,
		},
	}
	if forced != "" {
		base.holders.SetReleaseOutcome(forced)
	}
	stopStore, err := testkit.NewMemoryPlanStore(base.clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	workloads := &staticReleaseWorkloads{state: manifest.Workload.State}
	holders := &staticReleaseHolders{values: manifest.Workload.Holders}
	placements := &staticReleasePlacements{capture: CompletePlacementCapture{
		Token: core.PlacementCaptureToken{
			ID: "capture-a", Scope: string(plan.Submission.Domain), WorkloadUID: plan.WorkloadIdentity.UID,
			SnapshotRevision:    manifest.Placement.SnapshotRevision,
			SnapshotChecksum:    manifest.Placement.SnapshotChecksum,
			TopologyFingerprint: manifest.Placement.TopologyFingerprint,
			ActiveCardinality:   uint64(len(manifest.Placement.Placements)),
			AssignmentRevision:  manifest.ManagerTransitions.Universal.AssignmentRevision,
		},
		Placements: manifest.Placement.Placements,
	}}
	quiescence, err := NewBlockedQuiescencePort(BlockedQuiescenceOptions{
		Coordinator: base.coordinator, Scope: testCoordinatorScope(),
		Owner:     testCoordinatorOwner("controller-a"),
		Workloads: workloads, Holders: holders, Placements: placements,
		Exclusions: base.port, RoutePlans: testkit.NewMemoryRoutePlans(),
		DurableStops: stopStore, Transitions: testkit.NewMemoryTransitions(),
		Now: base.clock.Now, ProofTTL: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	port := quiescence.(*blockedQuiescencePort)
	exclusive := &staticReleaseExclusive{token: base.token}
	fence := &blockedQuiescenceFence{
		port: port, plan: plan.Clone(), manifest: manifest,
		token: base.token, exclusive: exclusive,
	}
	return blockedReleaseFixture{port: port, fence: fence, plan: plan,
		holders: base.holders, exclusive: exclusive}
}

func TestBlockedExclusionReleaseUsesExclusiveTokenAndFreshCapture(t *testing.T) {
	f := newBlockedReleaseFixture(t, "")
	fresh, err := f.port.ReleaseExclusionForBlockedTermination(context.Background(), f.plan, f.fence)
	if err != nil {
		t.Fatalf("release/fresh capture err=%v", err)
	}
	if fresh.Exclusion.Present || fresh.Exclusion.State.Excluded || fresh.Exclusion.State.OwnerPlanID != "" ||
		fresh.Exclusion.State.Revision != 9 || fresh.Exclusion.ReleaseOutcome != core.PlacementReleaseReleased {
		t.Fatalf("fresh exclusion=%+v", fresh.Exclusion)
	}
	proof := f.fence.Proof()
	if proof.ScaleResolution != core.ScaleResolutionNotCommitted ||
		proof.ExclusionReleaseOutcome != core.PlacementReleaseReleased {
		t.Fatalf("fresh proof=%+v", proof)
	}
	if f.exclusive.revalidateCalls != 2 || f.holders.ReleaseCalls() != 1 {
		t.Fatalf("exclusive revalidations=%d release calls=%d", f.exclusive.revalidateCalls, f.holders.ReleaseCalls())
	}
}

func TestBlockedExclusionReleaseFailsClosedForUnsafeOutcomes(t *testing.T) {
	for _, outcome := range []routecore.PlacementReleaseOutcome{
		routecore.PlacementReleaseNotFound,
		routecore.PlacementReleaseOwnerMismatch,
		routecore.PlacementReleaseOutcome("UNKNOWN"),
	} {
		t.Run(string(outcome), func(t *testing.T) {
			f := newBlockedReleaseFixture(t, outcome)
			fresh, err := f.port.ReleaseExclusionForBlockedTermination(context.Background(), f.plan, f.fence)
			if fresh.Exclusion.ReleaseOutcome != "" || err == nil {
				t.Fatalf("unsafe outcome fresh=%+v err=%v", fresh.Exclusion, err)
			}
			state, found, getErr := f.holders.GetPlacementExclusion(context.Background(),
				f.plan.Victim.Domain, f.plan.Victim.HolderID, f.plan.Victim.BootID)
			if getErr != nil || !found || !state.Excluded || state.OwnerPlanID != f.plan.PlanID {
				t.Fatalf("unsafe outcome changed state=%+v found=%t err=%v", state, found, getErr)
			}
		})
	}
}
