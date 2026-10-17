package scaleplan

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"sync"
	"time"

	core "farmgame/internal/scaleplan"
	"farmgame/internal/storage/authoritystore"
)

type fakeQuiescenceFence struct {
	owner *FakeQuiescence
	proof core.BlockedQuiescenceProof
}

func (f *fakeQuiescenceFence) Proof() core.BlockedQuiescenceProof { return f.proof }
func (f *fakeQuiescenceFence) Revalidate(ctx context.Context) error {
	if err := fakeContextErr(ctx); err != nil {
		return err
	}
	f.owner.mu.Lock()
	f.owner.revalidateCalls++
	hook, err := f.owner.onRevalidate, f.owner.revalidateErr
	f.owner.mu.Unlock()
	if hook != nil {
		hook()
	}
	return err
}
func (f *fakeQuiescenceFence) Release(ctx context.Context) error {
	if err := fakeContextErr(ctx); err != nil {
		return err
	}
	f.owner.mu.Lock()
	f.owner.releaseCalls++
	hook, err := f.owner.onRelease, f.owner.releaseErr
	f.owner.mu.Unlock()
	if hook != nil {
		hook(ctx)
	}
	return err
}

type FakeQuiescence struct {
	mu                  sync.Mutex
	now                 func() time.Time
	manifest            *core.BlockedQuiescenceManifest
	proof               *core.BlockedQuiescenceProof
	exclusions          *FakeExclusions
	acquireErr          error
	fenceOnAcquireError bool
	revalidateErr       error
	releaseErr          error
	acquireCalls        int
	revalidateCalls     int
	releaseCalls        int
	onRevalidate        func()
	onRelease           func(context.Context)
}

func NewFakeQuiescence(now func() time.Time) *FakeQuiescence {
	if now == nil {
		now = time.Now
	}
	return &FakeQuiescence{now: now}
}

func (f *FakeQuiescence) ProofForPlan(plan core.Plan) (core.BlockedQuiescenceProof, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	manifest := fakeQuiescenceManifest(plan, f.now().UnixMilli())
	if f.manifest != nil {
		manifest = cloneManifest(*f.manifest)
	}
	return core.BuildBlockedQuiescenceProof(plan, manifest)
}

func (f *FakeQuiescence) AcquireBlockedQuiescence(ctx context.Context, plan core.Plan) (core.BlockedQuiescenceManifest, core.BlockedQuiescenceFence, error) {
	if err := fakeContextErr(ctx); err != nil {
		return core.BlockedQuiescenceManifest{}, nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acquireCalls++
	if f.acquireErr != nil {
		if f.fenceOnAcquireError {
			return core.BlockedQuiescenceManifest{}, &fakeQuiescenceFence{owner: f}, f.acquireErr
		}
		return core.BlockedQuiescenceManifest{}, nil, f.acquireErr
	}
	manifest := fakeQuiescenceManifest(plan, f.now().UnixMilli())
	if f.manifest != nil {
		manifest = cloneManifest(*f.manifest)
	}
	proof, err := core.BuildBlockedQuiescenceProof(plan, manifest)
	if err != nil {
		return core.BlockedQuiescenceManifest{}, nil, err
	}
	if f.proof != nil {
		proof = *f.proof
	}
	return manifest, &fakeQuiescenceFence{owner: f, proof: proof}, nil
}

func (f *FakeQuiescence) ReleaseExclusionForBlockedTermination(ctx context.Context, plan core.Plan, fence core.BlockedQuiescenceFence) (core.BlockedQuiescenceManifest, error) {
	if err := fakeContextErr(ctx); err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	f.mu.Lock()
	exclusions, now := f.exclusions, f.now().UnixMilli()
	f.mu.Unlock()
	if exclusions == nil || plan.ExclusionIntent == nil {
		return core.BlockedQuiescenceManifest{}, core.ErrVerification
	}
	result, err := exclusions.ReleaseForOwner(ctx, plan.Victim, core.PlacementExclusionRelease{
		OwnerPlanID: plan.PlanID, ExpectedRevision: plan.ExclusionIntent.ObservedRevision,
	})
	if err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	if !result.Outcome.TerminalForScalePlan() {
		return core.BlockedQuiescenceManifest{}, core.ErrVerification
	}
	manifest := fakeQuiescenceManifest(plan, now)
	manifest.Exclusion = core.BlockedExclusionManifest{ReleaseOutcome: result.Outcome}
	proof, err := core.BuildBlockedQuiescenceProof(plan, manifest)
	if err != nil {
		return core.BlockedQuiescenceManifest{}, err
	}
	fakeFence, ok := fence.(*fakeQuiescenceFence)
	if !ok {
		return core.BlockedQuiescenceManifest{}, core.ErrVerification
	}
	f.mu.Lock()
	fakeFence.proof = proof
	f.mu.Unlock()
	return manifest, nil
}

func cloneManifest(value core.BlockedQuiescenceManifest) core.BlockedQuiescenceManifest {
	value.Workload.State.Pods = append([]core.WorkloadPodState(nil), value.Workload.State.Pods...)
	value.Workload.Holders = append([]core.HolderState(nil), value.Workload.Holders...)
	value.Placement.Placements = append([]core.ShardPlacement(nil), value.Placement.Placements...)
	value.Children.Children = append([]core.BlockedChildManifest(nil), value.Children.Children...)
	value.ManagerTransitions.Records = append([]core.BlockedManagerQuiescenceRecord(nil), value.ManagerTransitions.Records...)
	return value
}

func QuiescenceManifestForTest(plan core.Plan, now time.Time) core.BlockedQuiescenceManifest {
	return fakeQuiescenceManifest(plan, now.UnixMilli())
}

func fakeQuiescenceManifest(plan core.Plan, now int64) core.BlockedQuiescenceManifest {
	replicas := plan.Submission.DesiredReplicas
	if plan.ScaleIntent == nil || plan.ScaleIntent.Effect == core.EffectPending {
		replicas = plan.Submission.ExpectedReplicas
	}
	workload := core.WorkloadState{Ref: plan.Submission.Workload, UID: plan.WorkloadIdentity.UID,
		ResourceVersion: plan.WorkloadIdentity.ResourceVersion, Generation: 1, ObservedGeneration: 1,
		Replicas: replicas, ReadyReplicas: replicas}
	refs := make(map[int32]core.HolderRef)
	for _, move := range plan.ResolvedMoves {
		refs[move.Source.Ordinal] = move.Source
		refs[move.Target.Ordinal] = move.Target
	}
	if plan.Target.Exact() {
		refs[plan.Target.Ordinal] = plan.Target
	}
	if plan.Victim.Exact() {
		refs[plan.Victim.Ordinal] = plan.Victim
	}
	for ordinal := int32(0); ordinal < workload.Replicas; ordinal++ {
		ref, found := refs[ordinal]
		if !found {
			ref = core.HolderRef{Domain: plan.Submission.Domain, HolderID: "holder-" + itoa(ordinal), BootID: "boot-" + itoa(ordinal), PodUID: "pod-" + itoa(ordinal), WorkloadUID: workload.UID, Ordinal: ordinal}
			refs[ordinal] = ref
		}
		workload.Pods = append(workload.Pods, core.WorkloadPodState{PodName: plan.Submission.Workload.Name + "-" + itoa(ordinal), PodUID: ref.PodUID,
			WorkloadUID: workload.UID, Ordinal: ordinal, HolderID: ref.HolderID, Present: true, Ready: true})
	}
	holders := make([]core.HolderState, 0, len(refs))
	for _, ref := range refs {
		holders = append(holders, core.HolderState{Ref: ref, PodName: plan.Submission.Workload.Name + "-" + itoa(ref.Ordinal),
			LogicEndpoint: plan.Submission.Workload.Name + "-" + itoa(ref.Ordinal) + ".farm.svc:9000",
			State:         core.HolderServing, ProcessReady: true, AssignmentKnownFresh: true,
			ServingShardCount: 1, LeaseUntil: time.UnixMilli(now + time.Minute.Milliseconds() + 1)})
	}
	sort.Slice(holders, func(i, j int) bool { return holders[i].Ref.Ordinal < holders[j].Ref.Ordinal })
	children := make([]core.BlockedChildManifest, len(plan.ResolvedMoves))
	managers := make([]core.BlockedManagerQuiescenceRecord, len(plan.ResolvedMoves))
	placements := make([]core.ShardPlacement, len(plan.ResolvedMoves))
	for index, move := range plan.ResolvedMoves {
		childPhase := core.RoutePlanPhasePaused
		if move.ChildPhase == core.RoutePlanPhaseSucceeded {
			childPhase = core.RoutePlanPhaseSucceeded
		}
		children[index] = core.BlockedChildManifest{PlanID: move.ChildPlanID, TransitionID: move.ChildTransitionID, PayloadDigest: move.ChildPayloadDigest,
			ParentObservedPhase: move.ChildPhase, Phase: childPhase, DurablyStopped: true,
			Source: move.Source, SourceEndpoint: move.SourceEndpoint, Target: move.Target, TargetEndpoint: move.TargetEndpoint,
			ExpectedSourceFence: move.ExpectedSourceFence, ManagerTransitionID: move.ChildTransitionID,
			ManagerRecoveryID: "recovery-" + move.ChildTransitionID, EvidenceRetainUntilMs: plan.RecoveryRetainUntilMs}
		managers[index] = core.BlockedManagerQuiescenceRecord{ChildPlanID: move.ChildPlanID, TransitionID: move.ChildTransitionID,
			RecoveryID: "recovery-" + move.ChildTransitionID, Quiescent: true, EvidenceRetainUntilMs: plan.RecoveryRetainUntilMs}
		placementHolder, placementEndpoint := move.Source, move.SourceEndpoint
		if move.ChildPhase == core.RoutePlanPhaseSucceeded {
			placementHolder, placementEndpoint = move.Target, move.TargetEndpoint
		}
		for holderIndex := range holders {
			if holders[holderIndex].Ref == placementHolder {
				holders[holderIndex].LogicEndpoint = placementEndpoint
			}
		}
		placementFence := move.ExpectedSourceFence
		placementFence.ExpectedHolderID, placementFence.ExpectedHolderBootID = placementHolder.HolderID, placementHolder.BootID
		placements[index] = core.ShardPlacement{Domain: move.Intent.Domain, VirtualShard: move.Intent.VirtualShard, Active: true,
			SnapshotRevision: 1, Holder: placementHolder, Endpoint: placementEndpoint,
			ExpectedSourceFence: placementFence,
			KnownFresh:          true, LeaseUntil: time.UnixMilli(now + time.Minute.Milliseconds() + 1)}
	}
	if len(placements) == 0 && plan.Submission.Operation == core.OperationScaleIn && plan.ExclusionIntent != nil {
		placements = make([]core.ShardPlacement, len(plan.Submission.Moves))
		for index, move := range plan.Submission.Moves {
			source := plan.Victim
			if source.HolderID != move.SourceHolderID {
				for _, ref := range refs {
					if ref.HolderID == move.SourceHolderID {
						source = ref
						break
					}
				}
			}
			placements[index] = core.ShardPlacement{
				Domain: move.Domain, VirtualShard: move.VirtualShard, Active: true,
				SnapshotRevision: 1, Holder: source, Endpoint: source.HolderID + ".farm.svc:9000",
				ExpectedSourceFence: authoritystore.ShardWriteFence{
					ExpectedHolderID: source.HolderID, ExpectedHolderBootID: source.BootID,
					ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1,
				},
				KnownFresh: true, LeaseUntil: time.UnixMilli(now + time.Minute.Milliseconds() + 1),
			}
		}
	}
	exclusion := core.BlockedExclusionManifest{}
	if plan.ExclusionIntent != nil {
		exclusion = core.BlockedExclusionManifest{Present: true, State: core.PlacementExclusion{Holder: plan.ExclusionIntent.Victim,
			Excluded: true, Revision: plan.ExclusionIntent.ObservedRevision, OwnerPlanID: plan.PlanID}}
	}
	snapshotDigest := testDigest("placement-snapshot")
	return core.BlockedQuiescenceManifest{Workload: core.BlockedWorkloadManifest{State: workload, Holders: holders},
		Placement: core.BlockedPlacementManifest{Complete: true, Domain: plan.Submission.Domain, SnapshotRevision: 1,
			SnapshotChecksum: snapshotDigest, TopologyFingerprint: testDigest("placement-topology"),
			TotalActive: uint32(len(placements)), Placements: placements}, Exclusion: exclusion,
		Children: core.BlockedChildInventoryManifest{Children: children},
		ManagerTransitions: core.BlockedManagerQuiescenceManifest{Records: managers,
			Universal: core.ManagerQuiescenceEnumeration{Domain: plan.Submission.Domain, Enumerated: true,
				AssignmentRevision: 1, EnumeratedAtMs: now}},
		CapturedAtMs: now, ExpiresAtMs: now + time.Minute.Milliseconds(), EvidenceRetainUntilMs: plan.RecoveryRetainUntilMs}
}

func testDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func itoa(value int32) string {
	if value == 0 {
		return "0"
	}
	digits := ""
	for value > 0 {
		digits = string(rune('0'+value%10)) + digits
		value /= 10
	}
	return digits
}
func (f *FakeQuiescence) SetAcquireErrorWithFence(err error) {
	f.mu.Lock()
	f.acquireErr, f.fenceOnAcquireError = err, true
	f.mu.Unlock()
}
func (f *FakeQuiescence) SetProof(proof core.BlockedQuiescenceProof) {
	f.mu.Lock()
	f.proof = &proof
	f.mu.Unlock()
}
func (f *FakeQuiescence) SetManifest(manifest core.BlockedQuiescenceManifest) {
	f.mu.Lock()
	copy := cloneManifest(manifest)
	f.manifest = &copy
	f.mu.Unlock()
}
func (f *FakeQuiescence) SetRevalidateError(err error) {
	f.mu.Lock()
	f.revalidateErr = err
	f.mu.Unlock()
}
func (f *FakeQuiescence) SetRevalidateHook(hook func()) {
	f.mu.Lock()
	f.onRevalidate = hook
	f.mu.Unlock()
}
func (f *FakeQuiescence) SetReleaseHook(hook func(context.Context)) {
	f.mu.Lock()
	f.onRelease = hook
	f.mu.Unlock()
}
func (f *FakeQuiescence) ReleaseCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.releaseCalls
}

func (f *FakeQuiescence) SetExclusions(exclusions *FakeExclusions) {
	f.mu.Lock()
	f.exclusions = exclusions
	f.mu.Unlock()
}
