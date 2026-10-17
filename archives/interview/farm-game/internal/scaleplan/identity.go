package scaleplan

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"farmgame/internal/storage/authoritystore"
)

func SubmissionPayloadDigest(submission Submission) (string, error) {
	normalized, err := submission.Normalize()
	if err != nil {
		return "", err
	}
	parts := []string{
		SchemaVersion, normalized.Realm, string(normalized.Domain), string(normalized.Operation),
		normalized.Workload.Realm, string(normalized.Workload.Domain), normalized.Workload.Namespace,
		normalized.Workload.Kind, normalized.Workload.Name,
		strconv.FormatInt(int64(normalized.ExpectedReplicas), 10),
		strconv.FormatInt(int64(normalized.DesiredReplicas), 10),
		strconv.FormatInt(normalized.RoutePlanTTLms, 10), strconv.FormatInt(normalized.PlanTTLms, 10),
		strconv.FormatInt(normalized.RecoveryRetentionMs, 10),
		strconv.FormatUint(uint64(normalized.Limits.MaxRoutePlansInflight), 10),
		strconv.FormatUint(uint64(normalized.Limits.MaxShardsPerWave), 10),
	}
	// Existing non-empty DRAIN_HOLDER manifests already bind the victim via
	// every move's source. Only an empty drain needs the new explicit field in
	// the digest, preserving IDs for previously persisted non-empty plans.
	if normalized.Operation == OperationDrainHolder && len(normalized.Moves) == 0 {
		parts = append(parts, normalized.VictimHolderID)
	}
	if normalized.PinnedTarget != (HolderRef{}) {
		parts = append(parts, "pinned-target", holderRefDigestPart(normalized.PinnedTarget))
	}
	for _, move := range normalized.Moves {
		parts = append(parts, string(move.Domain), strconv.FormatUint(uint64(move.VirtualShard), 10),
			move.SourceHolderID, move.TargetHolderID, strconv.FormatInt(int64(move.TargetOrdinal), 10))
	}
	return digestParts("scaleplan-submission-v2", parts...), nil
}

func DeterministicPlanID(submission Submission) (string, error) {
	digest, err := SubmissionPayloadDigest(submission)
	if err != nil {
		return "", err
	}
	return "sp-v2-" + digestParts("scaleplan-id-v2", submission.IdempotencyKey, digest), nil
}

func BuildInitialPlan(submission Submission, now time.Time) (Plan, error) {
	normalized, err := submission.Normalize()
	if err != nil {
		return Plan{}, err
	}
	digest, err := SubmissionPayloadDigest(normalized)
	if err != nil {
		return Plan{}, err
	}
	id, err := DeterministicPlanID(normalized)
	if err != nil {
		return Plan{}, err
	}
	if now.IsZero() {
		return Plan{}, fmt.Errorf("%w: current time", ErrInvalidPlan)
	}
	startDeadline := now.Add(time.Duration(normalized.PlanTTLms) * time.Millisecond)
	plan := Plan{
		SchemaVersion: SchemaVersion, PlanID: id, IdempotencyKey: normalized.IdempotencyKey,
		PayloadDigest: digest, Submission: normalized, Status: StatusRunning, Phase: PhasePending,
		Revision: 1, CreatedAtMs: now.UnixMilli(), UpdatedAtMs: now.UnixMilli(),
		StartDeadlineMs:       startDeadline.UnixMilli(),
		RecoveryRetainUntilMs: startDeadline.Add(time.Duration(normalized.RecoveryRetentionMs) * time.Millisecond).UnixMilli(),
	}
	if err := ValidateInitialPlan(plan); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func ResolveChildMove(plan Plan, index int, placement ShardPlacement, target HolderState) (ResolvedMove, error) {
	if index < 0 || index >= len(plan.Submission.Moves) {
		return ResolvedMove{}, fmt.Errorf("%w: child index", ErrInvalidPlan)
	}
	intent := plan.Submission.Moves[index]
	if !placement.Active || !placement.KnownFresh || placement.Domain != intent.Domain || placement.VirtualShard != intent.VirtualShard ||
		!placement.Holder.Exact() || placement.Holder.HolderID != intent.SourceHolderID ||
		placement.Holder.WorkloadUID != plan.WorkloadIdentity.UID || placement.Endpoint == "" ||
		!placement.LeaseUntil.After(time.UnixMilli(plan.UpdatedAtMs)) {
		return ResolvedMove{}, fmt.Errorf("%w: source placement", ErrConflict)
	}
	fence := placement.ExpectedSourceFence
	if fence.ExpectedHolderID != placement.Holder.HolderID || fence.ExpectedHolderBootID != placement.Holder.BootID ||
		fence.ExpectedAssignmentRevision == 0 || fence.ExpectedWriterEpoch == 0 || fence.ExpectedLeaseEpoch == 0 {
		return ResolvedMove{}, fmt.Errorf("%w: source fence", ErrConflict)
	}
	if target.Ref.HolderID != intent.TargetHolderID || target.Ref.Ordinal != intent.TargetOrdinal || !target.Ref.Exact() ||
		target.Ref.WorkloadUID != plan.WorkloadIdentity.UID || !target.PlacementEligible || !target.AssignmentKnownFresh ||
		target.LogicEndpoint == "" || !target.LeaseUntil.After(time.UnixMilli(plan.UpdatedAtMs)) || target.Draining {
		return ResolvedMove{}, fmt.Errorf("%w: target holder", ErrConflict)
	}
	childID, transitionID, payload := childIdentities(plan, index, intent, placement.Holder, placement.Endpoint,
		target.Ref, target.LogicEndpoint, fence)
	return ResolvedMove{
		Intent: intent, WaveIndex: uint32(index), Source: placement.Holder, SourceEndpoint: placement.Endpoint,
		Target: target.Ref, TargetEndpoint: target.LogicEndpoint, ExpectedSourceFence: fence,
		ChildPlanID: childID, ChildTransitionID: transitionID, ChildPayloadDigest: payload,
	}, nil
}

func childIdentities(plan Plan, index int, intent ShardMoveIntent, source HolderRef, sourceEndpoint string,
	target HolderRef, targetEndpoint string, fence authoritystore.ShardWriteFence) (string, string, string) {
	payload := digestParts("scaleplan-child-payload-v2", plan.PlanID, strconv.Itoa(index), string(intent.Domain),
		strconv.FormatUint(uint64(intent.VirtualShard), 10), source.HolderID, source.BootID,
		source.PodUID, source.WorkloadUID, strconv.FormatInt(int64(source.Ordinal), 10), sourceEndpoint,
		target.HolderID, target.BootID, target.PodUID, target.WorkloadUID,
		strconv.FormatInt(int64(target.Ordinal), 10), targetEndpoint,
		fence.ExpectedHolderID, fence.ExpectedHolderBootID,
		strconv.FormatUint(fence.ExpectedAssignmentRevision, 10), strconv.FormatUint(fence.ExpectedWriterEpoch, 10),
		strconv.FormatUint(fence.ExpectedLeaseEpoch, 10), strconv.FormatInt(plan.Submission.RoutePlanTTLms, 10),
		plan.PlanID, strconv.FormatInt(plan.RecoveryRetainUntilMs, 10))
	childID := "sp-rp-v2-" + digestParts("scaleplan-child-id-v2", plan.PlanID, strconv.Itoa(index), string(intent.Domain),
		strconv.FormatUint(uint64(intent.VirtualShard), 10), intent.SourceHolderID, intent.TargetHolderID)
	transitionID := "sp-tr-v2-" + digestParts("scaleplan-transition-id-v2", childID, payload)
	return childID, transitionID, payload
}

func validateResolvedMoveIdentity(plan Plan, index int, move ResolvedMove) error {
	childID, transitionID, payload := childIdentities(plan, index, move.Intent, move.Source, move.SourceEndpoint,
		move.Target, move.TargetEndpoint, move.ExpectedSourceFence)
	if move.ChildPlanID != childID || move.ChildTransitionID != transitionID || move.ChildPayloadDigest != payload {
		return fmt.Errorf("%w: deterministic child identity %d", ErrInvalidPlan, index)
	}
	return nil
}

func (m ResolvedMove) RoutePlanRequest(ttlMs int64, ownerScalePlanID string, ownerRetainUntilMs int64, limits FunctionalLimits, totalMoves int) RoutePlanMoveRequest {
	// A ScalePlan is only the bulk source of durable migration intents. Each
	// child keeps its own shard fence. OwnerWave must remain empty: making
	// correctness depend on a fixed cross-shard membership set turns a slow
	// shard into a barrier for unrelated shards and freezes stale global
	// Snapshot revisions into later members. Already-ready Drain publications
	// may still share one Snapshot CAS; GlobalRevision stays a cursor.
	_ = limits
	_ = totalMoves
	return RoutePlanMoveRequest{
		PlanID: m.ChildPlanID, TransitionID: m.ChildTransitionID, Domain: m.Intent.Domain,
		VirtualShard: m.Intent.VirtualShard, Source: m.Source, SourceEndpoint: m.SourceEndpoint,
		Target: m.Target, TargetEndpoint: m.TargetEndpoint, ExpectedSourceFence: m.ExpectedSourceFence,
		RequestedTTLms: ttlMs, OwnerScalePlanID: ownerScalePlanID, OwnerRetainUntilMs: ownerRetainUntilMs,
		OwnerWaveID: "", OwnerWaveSize: 0, OwnerWavePosition: 0,
		PayloadDigest: m.ChildPayloadDigest,
	}
}

func BuildBlockedTerminationRequest(plan Plan, terminationID string) (BlockedTerminationRequest, error) {
	if err := validateOpaque("termination id", terminationID, MaxOpaqueIDBytes); err != nil {
		return BlockedTerminationRequest{}, err
	}
	digest, err := BlockedCheckpointDigest(plan)
	if err != nil {
		return BlockedTerminationRequest{}, err
	}
	return BlockedTerminationRequest{PlanID: plan.PlanID, ExpectedRevision: plan.Revision,
		ExpectedBlockedReason: plan.BlockedReason, ExpectedPayloadDigest: plan.PayloadDigest,
		ExpectedCheckpointDigest: digest, TerminationID: terminationID}, nil
}

func BlockedCheckpointDigest(plan Plan) (string, error) {
	if err := plan.Validate(); err != nil {
		return "", err
	}
	if plan.Status != StatusBlocked || plan.BlockedReason == "" || plan.BlockedTermination != nil {
		return "", fmt.Errorf("%w: blocked checkpoint required", ErrInvalidPlan)
	}
	parts := []string{plan.SchemaVersion, plan.PlanID, plan.IdempotencyKey, plan.PayloadDigest,
		string(plan.Status), string(plan.Phase), strconv.FormatUint(uint64(plan.WaveIndex), 10),
		strconv.FormatUint(plan.Attempt, 10), strconv.FormatUint(plan.Revision, 10),
		plan.WorkloadIdentity.UID, plan.WorkloadIdentity.ResourceVersion,
		holderRefDigestPart(plan.Target), holderRefDigestPart(plan.Victim),
		plan.PausedReason, plan.BlockedReason, plan.ErrorCode,
		strconv.FormatInt(plan.CreatedAtMs, 10), strconv.FormatInt(plan.UpdatedAtMs, 10),
		strconv.FormatInt(plan.CompletedAtMs, 10), strconv.FormatInt(plan.StartDeadlineMs, 10),
		strconv.FormatInt(plan.RecoveryRetainUntilMs, 10)}
	if plan.ScaleIntent == nil {
		parts = append(parts, "scale:none")
	} else {
		parts = append(parts, "scale:present", plan.ScaleIntent.WorkloadUID, plan.ScaleIntent.ExpectedResourceVersion,
			strconv.FormatInt(int64(plan.ScaleIntent.ExpectedReplicas), 10), strconv.FormatInt(int64(plan.ScaleIntent.DesiredReplicas), 10),
			string(plan.ScaleIntent.Effect))
	}
	if plan.ExclusionIntent == nil {
		parts = append(parts, "exclusion:none")
	} else {
		parts = append(parts, "exclusion:present", holderRefDigestPart(plan.ExclusionIntent.Victim),
			strconv.FormatUint(plan.ExclusionIntent.ExpectedRevision, 10), strconv.FormatUint(plan.ExclusionIntent.ObservedRevision, 10),
			plan.ExclusionIntent.OwnerPlanID, string(plan.ExclusionIntent.Effect))
	}
	if plan.CleanupIntent == nil {
		parts = append(parts, "cleanup:none")
	} else {
		parts = append(parts, "cleanup:present", plan.CleanupIntent.FailureCode,
			string(plan.CleanupIntent.ScaleResolution), string(plan.CleanupIntent.ReleaseOutcome))
	}
	parts = append(parts, strconv.Itoa(len(plan.ResolvedMoves)))
	for _, move := range plan.ResolvedMoves {
		parts = append(parts, string(move.Intent.Domain), strconv.FormatUint(uint64(move.Intent.VirtualShard), 10),
			move.Intent.SourceHolderID, move.Intent.TargetHolderID, strconv.FormatInt(int64(move.Intent.TargetOrdinal), 10),
			strconv.FormatUint(uint64(move.WaveIndex), 10), holderRefDigestPart(move.Source), move.SourceEndpoint,
			holderRefDigestPart(move.Target), move.TargetEndpoint,
			move.ExpectedSourceFence.ExpectedHolderID, move.ExpectedSourceFence.ExpectedHolderBootID,
			strconv.FormatUint(move.ExpectedSourceFence.ExpectedAssignmentRevision, 10),
			strconv.FormatUint(move.ExpectedSourceFence.ExpectedWriterEpoch, 10),
			strconv.FormatUint(move.ExpectedSourceFence.ExpectedLeaseEpoch, 10),
			move.ChildPlanID, move.ChildTransitionID, move.ChildPayloadDigest, move.ChildPhase,
			strconv.FormatBool(move.EnsureObserved), strconv.FormatBool(move.Verified), strconv.FormatBool(move.Reconciled))
	}
	return digestParts("scaleplan-blocked-checkpoint-v1", parts...), nil
}

func workloadPodManifestPart(pod WorkloadPodState) string {
	return digestParts("scaleplan-blocked-pod-v1", pod.PodName, pod.PodUID, pod.WorkloadUID,
		pod.ControllerRevision, strconv.FormatInt(int64(pod.Ordinal), 10), pod.HolderID,
		strconv.FormatBool(pod.Present), strconv.FormatBool(pod.Ready))
}

func holderStateManifestPart(holder HolderState) string {
	return digestParts("scaleplan-blocked-holder-v1", holderRefDigestPart(holder.Ref), holder.PodName, holder.LogicEndpoint,
		holder.HolderControlEndpoint, holder.TLSDNSIdentity, string(holder.State), strconv.FormatBool(holder.ProcessReady),
		strconv.FormatBool(holder.Draining), strconv.FormatUint(uint64(holder.ServingShardCount), 10),
		strconv.FormatUint(holder.SnapshotRevision, 10), strconv.FormatBool(holder.AssignmentKnownFresh),
		strconv.FormatBool(holder.PlacementEligible), strconv.FormatUint(holder.PlacementRevision, 10),
		strconv.FormatInt(holder.LeaseUntil.UnixMilli(), 10))
}

func placementManifestPart(placement ShardPlacement) string {
	return digestParts("scaleplan-blocked-placement-v1", string(placement.Domain),
		strconv.FormatUint(uint64(placement.VirtualShard), 10), strconv.FormatBool(placement.Active),
		strconv.FormatUint(placement.SnapshotRevision, 10), holderRefDigestPart(placement.Holder), placement.Endpoint,
		placement.ExpectedSourceFence.ExpectedHolderID, placement.ExpectedSourceFence.ExpectedHolderBootID,
		strconv.FormatUint(placement.ExpectedSourceFence.ExpectedAssignmentRevision, 10),
		strconv.FormatUint(placement.ExpectedSourceFence.ExpectedWriterEpoch, 10),
		strconv.FormatUint(placement.ExpectedSourceFence.ExpectedLeaseEpoch, 10), strconv.FormatBool(placement.KnownFresh),
		strconv.FormatInt(placement.LeaseUntil.UnixMilli(), 10))
}

func blockedExpectedWorkloadReplicas(plan Plan) int32 {
	if plan.ScaleIntent != nil && plan.ScaleIntent.Effect != EffectPending {
		return plan.Submission.DesiredReplicas
	}
	return plan.Submission.ExpectedReplicas
}

func blockedScaleNotCommittedEvidence(plan Plan, workload WorkloadState) bool {
	if plan.Submission.Operation == OperationDrainHolder {
		return workload.UID == plan.WorkloadIdentity.UID &&
			workload.ResourceVersion == plan.WorkloadIdentity.ResourceVersion &&
			workload.Replicas == plan.Submission.ExpectedReplicas
	}
	if plan.Submission.Operation != OperationScaleIn ||
		(plan.ScaleIntent != nil && plan.ScaleIntent.Effect != EffectPending) {
		return false
	}
	return workload.UID == plan.WorkloadIdentity.UID &&
		workload.ResourceVersion == plan.WorkloadIdentity.ResourceVersion &&
		workload.Replicas == plan.Submission.ExpectedReplicas
}

func blockedScaleCommittedEvidence(plan Plan, workload WorkloadState) bool {
	return plan.Submission.Operation == OperationScaleIn &&
		workload.UID == plan.WorkloadIdentity.UID &&
		workload.Replicas == plan.Submission.DesiredReplicas
}

func blockedWorkloadReplicaEvidenceValid(plan Plan, workload WorkloadState) bool {
	if plan.ScaleIntent != nil && plan.ScaleIntent.Effect == EffectPending {
		return workload.Replicas == plan.Submission.ExpectedReplicas ||
			workload.Replicas == plan.Submission.DesiredReplicas
	}
	return workload.Replicas == blockedExpectedWorkloadReplicas(plan)
}

func BuildBlockedQuiescenceProof(plan Plan, manifest BlockedQuiescenceManifest) (BlockedQuiescenceProof, error) {
	checkpoint, err := BlockedCheckpointDigest(plan)
	if err != nil {
		return BlockedQuiescenceProof{}, err
	}
	workload := manifest.Workload.State
	if manifest.CapturedAtMs < plan.UpdatedAtMs || manifest.ExpiresAtMs <= manifest.CapturedAtMs ||
		manifest.EvidenceRetainUntilMs < plan.RecoveryRetainUntilMs || manifest.EvidenceRetainUntilMs < manifest.ExpiresAtMs ||
		!manifest.Placement.Complete || manifest.Placement.Domain != plan.Submission.Domain || manifest.Placement.SnapshotRevision == 0 ||
		!canonicalSHA256(manifest.Placement.SnapshotChecksum) || !canonicalSHA256(manifest.Placement.TopologyFingerprint) ||
		manifest.Placement.TotalActive != uint32(len(manifest.Placement.Placements)) ||
		workload.Ref != plan.Submission.Workload || workload.UID != plan.WorkloadIdentity.UID ||
		workload.ResourceVersion == "" || !blockedWorkloadReplicaEvidenceValid(plan, workload) || workload.Replicas < 1 || workload.ReadyReplicas < 0 || workload.ReadyReplicas > workload.Replicas ||
		workload.ObservedGeneration < workload.Generation || len(workload.Pods) != int(workload.Replicas) || len(manifest.Workload.Holders) != len(workload.Pods) {
		return BlockedQuiescenceProof{}, fmt.Errorf("%w: blocked quiescence manifest", ErrVerification)
	}
	if len(manifest.Children.Children) != len(plan.ResolvedMoves) || len(manifest.ManagerTransitions.Records) != len(plan.ResolvedMoves) {
		return BlockedQuiescenceProof{}, fmt.Errorf("%w: blocked child/manager inventory size", ErrVerification)
	}
	podParts := make([]string, len(workload.Pods))
	readyPodCount := int32(0)
	seenPods := make(map[int32]WorkloadPodState, len(workload.Pods))
	seenPodNames := make(map[string]struct{}, len(workload.Pods))
	seenPodUIDs := make(map[string]struct{}, len(workload.Pods))
	seenHolderIDs := make(map[string]struct{}, len(workload.Pods))
	for index, pod := range workload.Pods {
		if !pod.Present || pod.PodName == "" || pod.PodUID == "" || pod.WorkloadUID != workload.UID || pod.Ordinal < 0 || pod.HolderID == "" {
			return BlockedQuiescenceProof{}, fmt.Errorf("%w: blocked workload pod %d", ErrVerification, index)
		}
		_, duplicateName := seenPodNames[pod.PodName]
		_, duplicateUID := seenPodUIDs[pod.PodUID]
		_, duplicateHolder := seenHolderIDs[pod.HolderID]
		if _, found := seenPods[pod.Ordinal]; found || pod.Ordinal != int32(index) || duplicateName || duplicateUID || duplicateHolder {
			return BlockedQuiescenceProof{}, fmt.Errorf("%w: noncanonical pod inventory", ErrVerification)
		}
		if pod.Ready {
			readyPodCount++
		}
		seenPods[pod.Ordinal] = pod
		seenPodNames[pod.PodName] = struct{}{}
		seenPodUIDs[pod.PodUID] = struct{}{}
		seenHolderIDs[pod.HolderID] = struct{}{}
		podParts[index] = workloadPodManifestPart(pod)
	}
	if readyPodCount != workload.ReadyReplicas {
		return BlockedQuiescenceProof{}, fmt.Errorf("%w: workload readiness count", ErrVerification)
	}
	holderParts := make([]string, len(manifest.Workload.Holders))
	holderIndex := make(map[string]HolderState, len(manifest.Workload.Holders))
	seenBootIDs := make(map[string]struct{}, len(manifest.Workload.Holders))
	for index, holder := range manifest.Workload.Holders {
		pod, found := seenPods[int32(index)]
		holderKey := holderRefDigestPart(holder.Ref)
		_, duplicateIdentity := holderIndex[holderKey]
		_, duplicateBoot := seenBootIDs[holder.Ref.BootID]
		if !holder.Ref.Exact() || holder.Ref.Domain != workload.Ref.Domain || holder.Ref.Ordinal != int32(index) ||
			holder.Ref.WorkloadUID != workload.UID || !holder.AssignmentKnownFresh || duplicateIdentity || duplicateBoot ||
			holder.PodName != pod.PodName || holder.PodName == "" || holder.LeaseUntil.UnixMilli() <= manifest.ExpiresAtMs ||
			!found || pod.PodUID != holder.Ref.PodUID || pod.HolderID != holder.Ref.HolderID {
			return BlockedQuiescenceProof{}, fmt.Errorf("%w: blocked holder/pod join %d", ErrVerification, index)
		}
		holderIndex[holderKey] = holder
		seenBootIDs[holder.Ref.BootID] = struct{}{}
		holderParts[index] = holderStateManifestPart(holder)
	}
	workloadParts := []string{plan.Submission.Workload.Key(), workload.UID, workload.ResourceVersion,
		strconv.FormatInt(workload.Generation, 10), strconv.FormatInt(workload.ObservedGeneration, 10),
		strconv.FormatInt(int64(workload.Replicas), 10), strconv.FormatInt(int64(workload.ReadyReplicas), 10),
		strconv.Itoa(len(podParts))}
	workloadParts = append(workloadParts, podParts...)
	workloadParts = append(workloadParts, strconv.Itoa(len(holderParts)))
	workloadParts = append(workloadParts, holderParts...)
	workloadDigest := digestParts("scaleplan-blocked-workload-manifest-v1", workloadParts...)
	moveKeys := make(map[ShardKey]struct{}, len(plan.Submission.Moves))
	for _, move := range plan.Submission.Moves {
		moveKeys[ShardKey{Domain: move.Domain, VirtualShard: move.VirtualShard}] = struct{}{}
	}
	seenPlacementKeys := make(map[ShardKey]struct{}, len(manifest.Placement.Placements))
	placementParts := []string{strconv.FormatBool(manifest.Placement.Complete), string(manifest.Placement.Domain),
		strconv.FormatUint(manifest.Placement.SnapshotRevision, 10), manifest.Placement.SnapshotChecksum,
		manifest.Placement.TopologyFingerprint, strconv.FormatUint(uint64(manifest.Placement.TotalActive), 10)}
	for index, placement := range manifest.Placement.Placements {
		key := ShardKey{Domain: placement.Domain, VirtualShard: placement.VirtualShard}
		_, duplicatePlacement := seenPlacementKeys[key]
		indexedHolder, holderKnown := holderIndex[holderRefDigestPart(placement.Holder)]
		if placement.SnapshotRevision != manifest.Placement.SnapshotRevision ||
			placement.Domain != manifest.Placement.Domain || (index > 0 && (manifest.Placement.Placements[index-1].Domain > placement.Domain ||
			(manifest.Placement.Placements[index-1].Domain == placement.Domain && manifest.Placement.Placements[index-1].VirtualShard >= placement.VirtualShard))) ||
			duplicatePlacement || !placement.Active ||
			!placement.Holder.Exact() || placement.Holder.Domain != placement.Domain || !holderKnown ||
			placement.Holder.WorkloadUID != workload.UID || placement.Endpoint == "" || placement.Endpoint != indexedHolder.LogicEndpoint ||
			placement.ExpectedSourceFence.ExpectedHolderID != placement.Holder.HolderID ||
			placement.ExpectedSourceFence.ExpectedHolderBootID != placement.Holder.BootID ||
			placement.ExpectedSourceFence.ExpectedAssignmentRevision == 0 || placement.ExpectedSourceFence.ExpectedWriterEpoch == 0 ||
			placement.ExpectedSourceFence.ExpectedLeaseEpoch == 0 || !placement.KnownFresh || placement.LeaseUntil.UnixMilli() <= manifest.ExpiresAtMs ||
			indexedHolder.State != HolderServing || !indexedHolder.ProcessReady || indexedHolder.Draining || indexedHolder.ServingShardCount == 0 {
			return BlockedQuiescenceProof{}, fmt.Errorf("%w: blocked placement %d", ErrVerification, index)
		}
		seenPlacementKeys[key] = struct{}{}
		placementParts = append(placementParts, placementManifestPart(placement))
	}
	for key := range moveKeys {
		if _, found := seenPlacementKeys[key]; !found {
			return BlockedQuiescenceProof{}, fmt.Errorf("%w: submitted move placement missing", ErrVerification)
		}
	}
	placementDigest := digestParts("scaleplan-blocked-placement-manifest-v1", placementParts...)
	scaleResolution := ScaleResolution("")
	releaseOutcome := PlacementReleaseOutcome("")
	if plan.ExclusionIntent != nil && blockedScaleCommittedEvidence(plan, workload) {
		scaleResolution = ScaleResolutionCommitted
	}
	if manifest.Exclusion.Present {
		if plan.ExclusionIntent == nil || manifest.Exclusion.State.Holder != plan.ExclusionIntent.Victim ||
			!manifest.Exclusion.State.Excluded || manifest.Exclusion.State.OwnerPlanID != plan.PlanID || manifest.Exclusion.ReleaseOutcome != "" ||
			manifest.Exclusion.State.Revision != plan.ExclusionIntent.ObservedRevision {
			return BlockedQuiescenceProof{}, fmt.Errorf("%w: blocked exclusion", ErrVerification)
		}
	} else if plan.ExclusionIntent != nil {
		if !blockedScaleNotCommittedEvidence(plan, workload) || !manifest.Exclusion.ReleaseOutcome.TerminalForScalePlan() {
			return BlockedQuiescenceProof{}, fmt.Errorf("%w: missing released exclusion evidence", ErrVerification)
		}
		scaleResolution = ScaleResolutionNotCommitted
		releaseOutcome = manifest.Exclusion.ReleaseOutcome
	} else if manifest.Exclusion.ReleaseOutcome != "" {
		return BlockedQuiescenceProof{}, fmt.Errorf("%w: release outcome without exclusion intent", ErrVerification)
	}
	exclusionDigest := digestParts("scaleplan-blocked-exclusion-manifest-v1", strconv.FormatBool(manifest.Exclusion.Present),
		holderRefDigestPart(manifest.Exclusion.State.Holder), strconv.FormatBool(manifest.Exclusion.State.Excluded),
		strconv.FormatUint(manifest.Exclusion.State.Revision, 10), manifest.Exclusion.State.OwnerPlanID, string(manifest.Exclusion.ReleaseOutcome))
	childParts := []string{strconv.Itoa(len(manifest.Children.Children))}
	for index, child := range manifest.Children.Children {
		move := plan.ResolvedMoves[index]
		manager := manifest.ManagerTransitions.Records[index]
		if child.PlanID != move.ChildPlanID || child.TransitionID != move.ChildTransitionID ||
			child.PayloadDigest != move.ChildPayloadDigest || child.ParentObservedPhase != move.ChildPhase || !child.DurablyStopped ||
			(child.Phase != RoutePlanPhaseSucceeded && child.Phase != RoutePlanPhaseFailed && child.Phase != RoutePlanPhasePaused) ||
			child.Source != move.Source || child.SourceEndpoint != move.SourceEndpoint || child.Target != move.Target ||
			child.TargetEndpoint != move.TargetEndpoint || !reflect.DeepEqual(child.ExpectedSourceFence, move.ExpectedSourceFence) ||
			child.ManagerTransitionID != manager.TransitionID || child.ManagerRecoveryID != manager.RecoveryID ||
			child.EvidenceRetainUntilMs < manifest.EvidenceRetainUntilMs {
			return BlockedQuiescenceProof{}, fmt.Errorf("%w: blocked child inventory %d", ErrVerification, index)
		}
		childParts = append(childParts, child.PlanID, child.TransitionID, child.PayloadDigest, child.ParentObservedPhase, child.Phase,
			child.ErrorCode, strconv.FormatBool(child.DurablyStopped),
			holderRefDigestPart(child.Source), child.SourceEndpoint, holderRefDigestPart(child.Target), child.TargetEndpoint,
			child.ExpectedSourceFence.ExpectedHolderID, child.ExpectedSourceFence.ExpectedHolderBootID,
			strconv.FormatUint(child.ExpectedSourceFence.ExpectedAssignmentRevision, 10),
			strconv.FormatUint(child.ExpectedSourceFence.ExpectedWriterEpoch, 10),
			strconv.FormatUint(child.ExpectedSourceFence.ExpectedLeaseEpoch, 10), child.ManagerTransitionID,
			child.ManagerRecoveryID, strconv.FormatInt(child.EvidenceRetainUntilMs, 10))
	}
	childDigest := digestParts("scaleplan-blocked-child-inventory-manifest-v1", childParts...)
	if len(manifest.ManagerTransitions.Records) != len(plan.ResolvedMoves) {
		return BlockedQuiescenceProof{}, fmt.Errorf("%w: manager inventory size", ErrVerification)
	}
	managerParts := []string{strconv.Itoa(len(manifest.ManagerTransitions.Records))}
	for index, record := range manifest.ManagerTransitions.Records {
		move := plan.ResolvedMoves[index]
		if record.ChildPlanID != move.ChildPlanID || record.TransitionID != move.ChildTransitionID || record.RecoveryID == "" ||
			!record.Quiescent || record.EvidenceRetainUntilMs < manifest.EvidenceRetainUntilMs {
			return BlockedQuiescenceProof{}, fmt.Errorf("%w: manager inventory %d", ErrVerification, index)
		}
		managerParts = append(managerParts, record.ChildPlanID, record.TransitionID, record.RecoveryID,
			strconv.FormatBool(record.Quiescent), strconv.FormatInt(record.EvidenceRetainUntilMs, 10))
	}
	// The per-move records above are existential. Section 8.6 requires the
	// universal statement too, and requires that a missing enumeration reads
	// as blocked rather than as quiet.
	universal := manifest.ManagerTransitions.Universal
	if !universal.Enumerated || universal.Domain != plan.Submission.Domain ||
		universal.AssignmentRevision == 0 || universal.EnumeratedAtMs <= 0 {
		return BlockedQuiescenceProof{}, fmt.Errorf("%w: manager quiescence enumeration missing", ErrVerification)
	}
	if universal.UnfinishedRecoveries != 0 || universal.UnfinishedTombstones != 0 {
		return BlockedQuiescenceProof{}, fmt.Errorf("%w: manager transitions not quiescent (%d recoveries, %d tombstones)",
			ErrVerification, universal.UnfinishedRecoveries, universal.UnfinishedTombstones)
	}
	managerParts = append(managerParts, string(universal.Domain), strconv.FormatBool(universal.Enumerated),
		strconv.FormatUint(uint64(universal.UnfinishedRecoveries), 10), strconv.FormatUint(uint64(universal.UnfinishedTombstones), 10),
		strconv.FormatUint(universal.AssignmentRevision, 10), strconv.FormatInt(universal.EnumeratedAtMs, 10))
	managerDigest := digestParts("scaleplan-blocked-manager-quiescence-manifest-v1", managerParts...)
	intentCount := blockedTerminationIntentCount(plan)
	childCount := uint32(len(plan.ResolvedMoves))
	evidenceDigest := digestParts("scaleplan-blocked-evidence-manifest-v1", workloadDigest, placementDigest,
		exclusionDigest, childDigest, managerDigest, strconv.FormatUint(uint64(intentCount), 10), strconv.FormatUint(uint64(childCount), 10), string(scaleResolution), string(releaseOutcome))
	return BlockedQuiescenceProof{ProtocolVersion: BlockedTerminationProtocolVersion, PlanID: plan.PlanID,
		ExpectedRevision: plan.Revision, ExpectedBlockedReason: plan.BlockedReason, ExpectedPayloadDigest: plan.PayloadDigest,
		ExpectedCheckpointDigest: checkpoint, EvidenceDigest: evidenceDigest, WorkloadDigest: workloadDigest,
		PlacementDigest: placementDigest, ExclusionDigest: exclusionDigest, ChildInventoryDigest: childDigest,
		ManagerQuiescenceDigest: managerDigest, CapturedAtMs: manifest.CapturedAtMs, ExpiresAtMs: manifest.ExpiresAtMs,
		EvidenceRetainUntilMs: manifest.EvidenceRetainUntilMs, IntentCount: intentCount, ChildCount: childCount,
		ScaleResolution: scaleResolution, ExclusionReleaseOutcome: releaseOutcome}, nil
}

func blockedEvidenceDigest(proof BlockedQuiescenceProof) string {
	return digestParts("scaleplan-blocked-evidence-manifest-v1", proof.WorkloadDigest, proof.PlacementDigest,
		proof.ExclusionDigest, proof.ChildInventoryDigest, proof.ManagerQuiescenceDigest,
		strconv.FormatUint(uint64(proof.IntentCount), 10), strconv.FormatUint(uint64(proof.ChildCount), 10), string(proof.ScaleResolution), string(proof.ExclusionReleaseOutcome))
}

func BlockedQuiescenceProofDigest(proof BlockedQuiescenceProof) (string, error) {
	if proof.ProtocolVersion != BlockedTerminationProtocolVersion || proof.PlanID == "" || proof.ExpectedRevision == 0 ||
		proof.ExpectedBlockedReason == "" || len(proof.ExpectedPayloadDigest) != 64 || len(proof.ExpectedCheckpointDigest) != 64 ||
		len(proof.EvidenceDigest) != 64 || len(proof.WorkloadDigest) != 64 || len(proof.PlacementDigest) != 64 ||
		len(proof.ExclusionDigest) != 64 || len(proof.ChildInventoryDigest) != 64 || len(proof.ManagerQuiescenceDigest) != 64 ||
		proof.CapturedAtMs <= 0 || proof.ExpiresAtMs <= proof.CapturedAtMs || proof.EvidenceRetainUntilMs < proof.ExpiresAtMs {
		return "", fmt.Errorf("%w: blocked quiescence proof", ErrVerification)
	}
	for _, value := range []string{proof.ExpectedPayloadDigest, proof.ExpectedCheckpointDigest, proof.EvidenceDigest,
		proof.WorkloadDigest, proof.PlacementDigest, proof.ExclusionDigest, proof.ChildInventoryDigest, proof.ManagerQuiescenceDigest} {
		if !canonicalSHA256(value) {
			return "", fmt.Errorf("%w: blocked quiescence digest", ErrVerification)
		}
	}
	if proof.ScaleResolution != "" && proof.ScaleResolution != ScaleResolutionNotCommitted && proof.ScaleResolution != ScaleResolutionCommitted {
		return "", fmt.Errorf("%w: blocked scale resolution", ErrVerification)
	}
	if proof.ExclusionReleaseOutcome != "" && (proof.ScaleResolution != ScaleResolutionNotCommitted ||
		!proof.ExclusionReleaseOutcome.Valid() || !proof.ExclusionReleaseOutcome.TerminalForScalePlan()) {
		return "", fmt.Errorf("%w: blocked release outcome", ErrVerification)
	}
	if proof.ScaleResolution == ScaleResolutionCommitted && proof.ExclusionReleaseOutcome != "" {
		return "", fmt.Errorf("%w: committed blocked scale has release outcome", ErrVerification)
	}
	if (proof.ScaleResolution == ScaleResolutionNotCommitted) != (proof.ExclusionReleaseOutcome != "") {
		return "", fmt.Errorf("%w: incomplete blocked release evidence", ErrVerification)
	}
	if proof.EvidenceDigest != blockedEvidenceDigest(proof) {
		return "", fmt.Errorf("%w: blocked evidence aggregate", ErrVerification)
	}
	return digestParts("scaleplan-blocked-quiescence-proof-v1", proof.ProtocolVersion, proof.PlanID,
		strconv.FormatUint(proof.ExpectedRevision, 10), proof.ExpectedBlockedReason, proof.ExpectedPayloadDigest,
		proof.ExpectedCheckpointDigest, proof.EvidenceDigest, proof.WorkloadDigest, proof.PlacementDigest,
		proof.ExclusionDigest, proof.ChildInventoryDigest, proof.ManagerQuiescenceDigest,
		strconv.FormatInt(proof.CapturedAtMs, 10), strconv.FormatInt(proof.ExpiresAtMs, 10),
		strconv.FormatInt(proof.EvidenceRetainUntilMs, 10), strconv.FormatUint(uint64(proof.IntentCount), 10),
		strconv.FormatUint(uint64(proof.ChildCount), 10), string(proof.ScaleResolution), string(proof.ExclusionReleaseOutcome)), nil
}

func blockedTerminationIntentCount(plan Plan) uint32 {
	var count uint32
	if plan.ScaleIntent != nil {
		count++
	}
	if plan.ExclusionIntent != nil {
		count++
	}
	if plan.CleanupIntent != nil {
		count++
	}
	count += uint32(len(plan.ResolvedMoves))
	return count
}

func canonicalSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && value == strings.ToLower(value)
}

func holderRefDigestPart(ref HolderRef) string {
	return digestParts("scaleplan-holder-ref-v1", string(ref.Domain), ref.HolderID, ref.BootID, ref.PodUID,
		ref.WorkloadUID, strconv.FormatInt(int64(ref.Ordinal), 10))
}

func digestParts(scope string, parts ...string) string {
	h := sha256.New()
	var size [4]byte
	write := func(value string) {
		binary.BigEndian.PutUint32(size[:], uint32(len(value)))
		_, _ = h.Write(size[:])
		_, _ = h.Write([]byte(value))
	}
	write(scope)
	for _, part := range parts {
		write(part)
	}
	return hex.EncodeToString(h.Sum(nil))
}
