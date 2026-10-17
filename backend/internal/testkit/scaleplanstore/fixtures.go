package scaleplanstore

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/scaleplan"
	"farmgame/internal/storage/authoritystore"
)

// ScaleInCleanupPlans returns a valid CAS sequence through the durable
// exclusion-release cleanup seam.  It keeps codec and Redis tests on the same
// semantically valid A0.6 fixture instead of hand-forging Plan snapshots.
func ScaleInCleanupPlans(t testing.TB, realm, key, workloadName string) []core.Plan {
	t.Helper()
	submission := core.Submission{
		IdempotencyKey: key, Realm: realm, Domain: actorrouting.DomainFarm, Operation: core.OperationScaleIn,
		Workload:         core.WorkloadRef{Realm: realm, Domain: actorrouting.DomainFarm, Namespace: "default", Kind: "StatefulSet", Name: workloadName},
		ExpectedReplicas: 2, DesiredReplicas: 1,
		Moves:          []core.ShardMoveIntent{{Domain: actorrouting.DomainFarm, VirtualShard: 1, SourceHolderID: "holder-b", TargetHolderID: "holder-a", TargetOrdinal: 0}},
		RoutePlanTTLms: time.Hour.Milliseconds(), PlanTTLms: time.Hour.Milliseconds(), RecoveryRetentionMs: time.Hour.Milliseconds(),
		Limits: core.FunctionalLimits{MaxRoutePlansInflight: 1, MaxShardsPerWave: 1},
	}
	current, err := core.BuildInitialPlan(submission, time.UnixMilli(1_700_000_000_100))
	if err != nil {
		t.Fatalf("initial scale-in plan: %v", err)
	}
	plans := []core.Plan{current}
	add := func(candidate core.Plan) {
		if err := core.ValidatePlanUpdate(current, candidate); err != nil {
			t.Fatalf("scale-in fixture update %s -> %s: %v", current.Phase, candidate.Phase, err)
		}
		plans = append(plans, candidate)
		current = candidate
	}
	next := func() core.Plan {
		candidate := current.Clone()
		candidate.Revision++
		candidate.UpdatedAtMs++
		return candidate
	}
	candidate := next()
	candidate.Phase = core.PhaseSelectingVictim
	add(candidate)

	uid := "workload-" + workloadName
	victim := core.HolderRef{Domain: actorrouting.DomainFarm, HolderID: "holder-b", BootID: "boot-b", PodUID: "pod-holder-b", WorkloadUID: uid, Ordinal: 1}
	candidate = next()
	candidate.Phase = core.PhaseExcludingVictim
	candidate.WorkloadIdentity = core.WorkloadIdentity{UID: uid, ResourceVersion: "rv-2"}
	candidate.Victim = victim
	candidate.ExclusionIntent = &core.ExclusionIntent{Victim: victim, ExpectedRevision: 1, OwnerPlanID: candidate.PlanID, Effect: core.EffectPending}
	add(candidate)

	candidate = next()
	candidate.Phase = core.PhaseCapturingVictimShards
	candidate.ExclusionIntent.Effect = core.EffectCommitted
	candidate.ExclusionIntent.ObservedRevision = 2
	add(candidate)

	candidate = next()
	candidate.Phase = core.PhaseMovingWave
	add(candidate)

	candidate = next()
	candidate.Phase = core.PhaseReleasingExclusion
	candidate.CleanupIntent = &core.CleanupIntent{FailureCode: "SCALE_IN_FAILURE", ScaleResolution: core.ScaleResolutionPending}
	add(candidate)

	candidate = next()
	candidate.CleanupIntent.ScaleResolution = core.ScaleResolutionNotCommitted
	add(candidate)

	candidate = next()
	candidate.CleanupIntent.ReleaseOutcome = core.PlacementReleaseReleased
	add(candidate)
	return plans
}

// ReleasingScaleInPlan is the final, released-exclusion state in
// ScaleInCleanupPlans and is suitable for direct codec round-trip tests.
func ReleasingScaleInPlan(t testing.TB, realm, key, workloadName string) core.Plan {
	t.Helper()
	plans := ScaleInCleanupPlans(t, realm, key, workloadName)
	return plans[len(plans)-1]
}

// SafeTerminatedScaleInPlan builds a valid operator-safe terminal record with
// both A0.6 proof fields populated.  It is used to exercise the proof DTO,
// including its digest binding, without reaching into codec internals.
func SafeTerminatedScaleInPlan(t testing.TB, realm, key, workloadName string) core.Plan {
	t.Helper()
	base := ReleasingScaleInPlan(t, realm, key, workloadName)
	blocked := base.Clone()
	blocked.Status = core.StatusBlocked
	blocked.BlockedReason = "EXCLUSION_RELEASE_REQUIRED"
	blocked.Revision++
	blocked.UpdatedAtMs++

	captureAt := blocked.UpdatedAtMs
	expiresAt := captureAt + time.Minute.Milliseconds()
	retainUntil := blocked.RecoveryRetainUntilMs
	uid := blocked.WorkloadIdentity.UID
	target := core.HolderRef{Domain: actorrouting.DomainFarm, HolderID: "holder-a", BootID: "boot-a", PodUID: "pod-holder-a", WorkloadUID: uid, Ordinal: 0}
	victim := blocked.Victim
	targetHolder := scaleInProofHolder(target, expiresAt)
	victimHolder := scaleInProofHolder(victim, expiresAt)
	workload := core.WorkloadState{
		Ref: blocked.Submission.Workload, UID: uid, ResourceVersion: blocked.WorkloadIdentity.ResourceVersion,
		Generation: 2, ObservedGeneration: 2, Replicas: 2, ReadyReplicas: 2,
		Pods: []core.WorkloadPodState{
			{PodName: targetHolder.PodName, PodUID: target.PodUID, WorkloadUID: uid, Ordinal: 0, HolderID: target.HolderID, Present: true, Ready: true},
			{PodName: victimHolder.PodName, PodUID: victim.PodUID, WorkloadUID: uid, Ordinal: 1, HolderID: victim.HolderID, Present: true, Ready: true},
		},
	}
	placement := core.ShardPlacement{
		Domain: actorrouting.DomainFarm, VirtualShard: 1, Active: true, SnapshotRevision: 1,
		Holder: victim, Endpoint: victimHolder.LogicEndpoint,
		ExpectedSourceFence: authoritystore.ShardWriteFence{ExpectedHolderID: victim.HolderID, ExpectedHolderBootID: victim.BootID, ExpectedAssignmentRevision: 1, ExpectedWriterEpoch: 1, ExpectedLeaseEpoch: 1},
		KnownFresh:          true, LeaseUntil: time.UnixMilli(expiresAt).Add(time.Hour),
	}
	manifest := core.BlockedQuiescenceManifest{
		Workload: core.BlockedWorkloadManifest{State: workload, Holders: []core.HolderState{targetHolder, victimHolder}},
		Placement: core.BlockedPlacementManifest{
			Complete: true, Domain: actorrouting.DomainFarm, SnapshotRevision: 1,
			SnapshotChecksum: scalePlanDigest("snapshot"), TopologyFingerprint: scalePlanDigest("topology"),
			TotalActive: 1, Placements: []core.ShardPlacement{placement},
		},
		Exclusion: core.BlockedExclusionManifest{ReleaseOutcome: core.PlacementReleaseReleased},
		ManagerTransitions: core.BlockedManagerQuiescenceManifest{Universal: core.ManagerQuiescenceEnumeration{
			Domain: actorrouting.DomainFarm, Enumerated: true, AssignmentRevision: 1, EnumeratedAtMs: captureAt,
		}},
		CapturedAtMs: captureAt, ExpiresAtMs: expiresAt, EvidenceRetainUntilMs: retainUntil,
	}
	proof, err := core.BuildBlockedQuiescenceProof(blocked, manifest)
	if err != nil {
		t.Fatalf("blocked proof fixture: %v", err)
	}
	checkpoint, err := core.BlockedCheckpointDigest(blocked)
	if err != nil {
		t.Fatalf("blocked checkpoint fixture: %v", err)
	}
	proofDigest, err := core.BlockedQuiescenceProofDigest(proof)
	if err != nil {
		t.Fatalf("blocked proof digest fixture: %v", err)
	}
	terminal := blocked.Clone()
	terminal.Status = core.StatusFailed
	terminal.BlockedReason = ""
	terminal.ErrorCode = "OPERATOR_SAFE_TERMINATION"
	terminal.Revision++
	terminal.UpdatedAtMs = proof.CapturedAtMs + 100
	terminal.CompletedAtMs = terminal.UpdatedAtMs
	terminal.BlockedTerminationProof = &proof
	terminal.BlockedTermination = &core.BlockedTerminationReceipt{
		ProtocolVersion: core.BlockedTerminationReceiptVersion, TerminationID: "safe-termination-1",
		ExpectedBlockedRevision: blocked.Revision, ExpectedBlockedReason: blocked.BlockedReason,
		ExpectedBlockedUpdatedAtMs: blocked.UpdatedAtMs, ExpectedPayloadDigest: blocked.PayloadDigest,
		ExpectedCheckpointDigest: checkpoint, ProofDigest: proofDigest, ProvedAtMs: proof.CapturedAtMs,
		ProofExpiresAtMs: proof.ExpiresAtMs, EvidenceRetainUntilMs: proof.EvidenceRetainUntilMs,
		CommittedAtMs: terminal.CompletedAtMs, IntentCount: 2, ChildCount: 0, ResultCode: "OPERATOR_SAFE_TERMINATION",
	}
	if err := terminal.Validate(); err != nil {
		t.Fatalf("terminal proof fixture: %v", err)
	}
	return terminal
}

func scaleInProofHolder(ref core.HolderRef, expiresAt int64) core.HolderState {
	return core.HolderState{
		Ref: ref, PodName: ref.HolderID, LogicEndpoint: ref.HolderID + ".farm.svc:9000",
		HolderControlEndpoint: ref.HolderID + ".farm.svc:9443", TLSDNSIdentity: ref.HolderID + ".farm.svc",
		State: core.HolderServing, ProcessReady: true, ServingShardCount: 1, SnapshotRevision: 1,
		AssignmentKnownFresh: true, PlacementEligible: true, PlacementRevision: 1,
		LeaseUntil: time.UnixMilli(expiresAt).Add(time.Hour),
	}
}

func scalePlanDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
