// Package codec owns the versioned, explicit wire representation of a
// durable ScalePlan record.  It deliberately does not marshal the core
// model directly: the field order and names below are part of the Redis
// contract and must not move when the in-memory model is refactored.
package codec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"farmgame/internal/actorrouting"
	core "farmgame/internal/scaleplan"
	"farmgame/internal/storage/authoritystore"
)

const (
	RecordSchema      = "scale-plan-record-v1"
	ChecksumDomain    = "scale-plan-record-checksum-v1"
	MaxCanonicalBytes = 8 << 20
)

// planDTO is intentionally not an alias of core.Plan.  Adding a field to the
// core model must be an explicit schema decision before it can alter durable
// bytes.
type planDTO struct {
	SchemaVersion  string            `json:"schema_version"`
	PlanID         string            `json:"plan_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	PayloadDigest  string            `json:"payload_digest"`
	Submission     submissionDTO     `json:"submission"`
	Status         string            `json:"status"`
	Phase          string            `json:"phase"`
	WaveIndex      uint32            `json:"wave_index"`
	Attempt        uint64            `json:"attempt"`
	Revision       uint64            `json:"revision"`
	Workload       identityDTO       `json:"workload_identity"`
	Target         holderDTO         `json:"target"`
	Victim         holderDTO         `json:"victim"`
	ScaleIntent    *scaleIntentDTO   `json:"scale_intent,omitempty"`
	Exclusion      *exclusionDTO     `json:"exclusion_intent,omitempty"`
	Cleanup        *cleanupIntentDTO `json:"cleanup_intent,omitempty"`
	ResolvedMoves  []moveDTO         `json:"resolved_moves"`
	PausedReason   string            `json:"paused_reason"`
	BlockedReason  string            `json:"blocked_reason"`
	ErrorCode      string            `json:"error_code"`
	CreatedAtMs    int64             `json:"created_at_ms"`
	UpdatedAtMs    int64             `json:"updated_at_ms"`
	CompletedAtMs  int64             `json:"completed_at_ms"`
	StartDeadline  int64             `json:"start_deadline_ms"`
	RecoveryRetain int64             `json:"recovery_retain_until_ms"`
	Receipt        *receiptDTO       `json:"blocked_termination,omitempty"`
	Proof          *proofDTO         `json:"blocked_termination_proof,omitempty"`
}

type submissionDTO struct {
	IdempotencyKey string      `json:"idempotency_key"`
	Realm          string      `json:"realm"`
	Domain         string      `json:"domain"`
	Operation      string      `json:"operation"`
	Workload       workloadDTO `json:"workload"`
	VictimHolderID string      `json:"victim_holder_id,omitempty"`
	PinnedTarget   *holderDTO  `json:"pinned_target,omitempty"`
	Expected       int32       `json:"expected_replicas"`
	Desired        int32       `json:"desired_replicas"`
	Moves          []intentDTO `json:"moves"`
	RouteTTL       int64       `json:"route_plan_ttl_ms"`
	PlanTTL        int64       `json:"plan_ttl_ms"`
	RecoveryTTL    int64       `json:"recovery_retention_ms"`
	Limits         limitsDTO   `json:"limits"`
}

type workloadDTO struct {
	Realm     string `json:"realm"`
	Domain    string `json:"domain"`
	Namespace string `json:"namespace"`
	Kind      string `json:"kind"`
	Name      string `json:"name"`
}

type intentDTO struct {
	Domain  string `json:"domain"`
	Shard   uint32 `json:"virtual_shard"`
	Source  string `json:"source_holder_id"`
	Target  string `json:"target_holder_id"`
	Ordinal int32  `json:"target_ordinal"`
}

type limitsDTO struct {
	RoutePlans uint32 `json:"max_route_plans_inflight"`
	Shards     uint32 `json:"max_shards_per_wave"`
}

type identityDTO struct {
	UID             string `json:"uid"`
	ResourceVersion string `json:"resource_version"`
}

type holderDTO struct {
	Domain      string `json:"domain"`
	HolderID    string `json:"holder_id"`
	BootID      string `json:"boot_id"`
	PodUID      string `json:"pod_uid"`
	WorkloadUID string `json:"workload_uid"`
	Ordinal     int32  `json:"ordinal"`
}

type fenceDTO struct {
	HolderID   string `json:"expected_holder_id"`
	BootID     string `json:"expected_holder_boot_id"`
	Assignment uint64 `json:"expected_assignment_revision"`
	Writer     uint64 `json:"expected_writer_epoch"`
	Lease      uint64 `json:"expected_lease_epoch"`
}

type scaleIntentDTO struct {
	WorkloadUID     string `json:"workload_uid"`
	ResourceVersion string `json:"expected_resource_version"`
	Expected        int32  `json:"expected_replicas"`
	Desired         int32  `json:"desired_replicas"`
	Effect          string `json:"effect"`
}

type exclusionDTO struct {
	Victim   holderDTO `json:"victim"`
	Expected uint64    `json:"expected_revision"`
	Observed uint64    `json:"observed_revision"`
	Owner    string    `json:"owner_plan_id"`
	Effect   string    `json:"effect"`
}

type cleanupIntentDTO struct {
	FailureCode     string `json:"failure_code"`
	ScaleResolution string `json:"scale_resolution"`
	ReleaseOutcome  string `json:"release_outcome"`
}

type moveDTO struct {
	Intent          intentDTO `json:"intent"`
	Wave            uint32    `json:"wave_index"`
	Source          holderDTO `json:"source"`
	SourceEndpoint  string    `json:"source_endpoint"`
	Target          holderDTO `json:"target"`
	TargetEndpoint  string    `json:"target_endpoint"`
	Fence           fenceDTO  `json:"expected_source_fence"`
	ChildPlan       string    `json:"child_plan_id"`
	ChildTransition string    `json:"child_transition_id"`
	ChildPayload    string    `json:"child_payload_digest"`
	ChildPhase      string    `json:"child_phase"`
	EnsureObserved  bool      `json:"ensure_observed"`
	Verified        bool      `json:"verified"`
	Reconciled      bool      `json:"reconciled"`
}

type receiptDTO struct {
	Protocol           string `json:"protocol_version"`
	Termination        string `json:"termination_id"`
	ExpectedRevision   uint64 `json:"expected_blocked_revision"`
	ExpectedReason     string `json:"expected_blocked_reason"`
	ExpectedUpdated    int64  `json:"expected_blocked_updated_at_ms"`
	ExpectedPayload    string `json:"expected_payload_digest"`
	ExpectedCheckpoint string `json:"expected_checkpoint_digest"`
	ProofDigest        string `json:"proof_digest"`
	ProvedAt           int64  `json:"proved_at_ms"`
	ProofExpires       int64  `json:"proof_expires_at_ms"`
	EvidenceRetain     int64  `json:"evidence_retain_until_ms"`
	CommittedAt        int64  `json:"committed_at_ms"`
	IntentCount        uint32 `json:"intent_count"`
	ChildCount         uint32 `json:"child_count"`
	Result             string `json:"result_code"`
}

type proofDTO struct {
	Protocol           string `json:"protocol_version"`
	PlanID             string `json:"plan_id"`
	ExpectedRevision   uint64 `json:"expected_revision"`
	ExpectedReason     string `json:"expected_blocked_reason"`
	ExpectedPayload    string `json:"expected_payload_digest"`
	ExpectedCheckpoint string `json:"expected_checkpoint_digest"`
	Evidence           string `json:"evidence_digest"`
	Workload           string `json:"workload_digest"`
	Placement          string `json:"placement_digest"`
	Exclusion          string `json:"exclusion_digest"`
	Children           string `json:"child_inventory_digest"`
	Manager            string `json:"manager_quiescence_digest"`
	CapturedAt         int64  `json:"captured_at_ms"`
	ExpiresAt          int64  `json:"expires_at_ms"`
	EvidenceRetain     int64  `json:"evidence_retain_until_ms"`
	IntentCount        uint32 `json:"intent_count"`
	ChildCount         uint32 `json:"child_count"`
	ScaleResolution    string `json:"scale_resolution"`
	ReleaseOutcome     string `json:"exclusion_release_outcome"`
}

func EncodePlan(plan core.Plan) ([]byte, error) {
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(toDTO(plan))
	if err != nil {
		return nil, fmt.Errorf("scaleplan codec: encode: %w", err)
	}
	if len(b) > MaxCanonicalBytes {
		return nil, fmt.Errorf("scaleplan codec: record exceeds %d bytes", MaxCanonicalBytes)
	}
	return b, nil
}

func Encode(plan core.Plan) ([]byte, error) { return EncodePlan(plan) }

func DecodePlan(data []byte) (core.Plan, error) {
	if len(data) == 0 || len(data) > MaxCanonicalBytes {
		return core.Plan{}, fmt.Errorf("scaleplan codec: record size")
	}
	var dto planDTO
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&dto); err != nil {
		return core.Plan{}, fmt.Errorf("scaleplan codec: decode: %w", err)
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		return core.Plan{}, fmt.Errorf("scaleplan codec: trailing data")
	}
	canonical, err := json.Marshal(dto)
	if err != nil || !bytes.Equal(canonical, data) {
		return core.Plan{}, fmt.Errorf("scaleplan codec: non-canonical json")
	}
	plan, err := fromDTO(dto)
	if err != nil {
		return core.Plan{}, err
	}
	if err := plan.Validate(); err != nil {
		return core.Plan{}, err
	}
	return plan, nil
}

func Decode(data []byte) (core.Plan, error) { return DecodePlan(data) }

func Checksum(canonicalJSON []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte(ChecksumDomain))
	_, _ = h.Write(canonicalJSON)
	return hex.EncodeToString(h.Sum(nil))
}

func ChecksumSHA256(canonicalJSON []byte) string { return Checksum(canonicalJSON) }

func VerifyChecksum(canonicalJSON []byte, checksum string) bool {
	return Checksum(canonicalJSON) == checksum
}

func toDTO(p core.Plan) planDTO {
	d := planDTO{SchemaVersion: p.SchemaVersion, PlanID: p.PlanID, IdempotencyKey: p.IdempotencyKey, PayloadDigest: p.PayloadDigest,
		Submission: submissionToDTO(p.Submission), Status: string(p.Status), Phase: string(p.Phase), WaveIndex: p.WaveIndex,
		Attempt: p.Attempt, Revision: p.Revision, Workload: identityDTO{p.WorkloadIdentity.UID, p.WorkloadIdentity.ResourceVersion},
		Target: holderToDTO(p.Target), Victim: holderToDTO(p.Victim), PausedReason: p.PausedReason, BlockedReason: p.BlockedReason,
		ErrorCode: p.ErrorCode, CreatedAtMs: p.CreatedAtMs, UpdatedAtMs: p.UpdatedAtMs, CompletedAtMs: p.CompletedAtMs,
		StartDeadline: p.StartDeadlineMs, RecoveryRetain: p.RecoveryRetainUntilMs}
	if p.ScaleIntent != nil {
		v := p.ScaleIntent
		d.ScaleIntent = &scaleIntentDTO{v.WorkloadUID, v.ExpectedResourceVersion, v.ExpectedReplicas, v.DesiredReplicas, string(v.Effect)}
	}
	if p.ExclusionIntent != nil {
		v := p.ExclusionIntent
		d.Exclusion = &exclusionDTO{holderToDTO(v.Victim), v.ExpectedRevision, v.ObservedRevision, v.OwnerPlanID, string(v.Effect)}
	}
	if p.CleanupIntent != nil {
		v := p.CleanupIntent
		d.Cleanup = &cleanupIntentDTO{FailureCode: v.FailureCode, ScaleResolution: string(v.ScaleResolution), ReleaseOutcome: string(v.ReleaseOutcome)}
	}
	if p.ResolvedMoves != nil {
		d.ResolvedMoves = make([]moveDTO, len(p.ResolvedMoves))
		for i := range p.ResolvedMoves {
			d.ResolvedMoves[i] = moveToDTO(p.ResolvedMoves[i])
		}
	}
	if p.BlockedTermination != nil {
		v := p.BlockedTermination
		d.Receipt = &receiptDTO{v.ProtocolVersion, v.TerminationID, v.ExpectedBlockedRevision, v.ExpectedBlockedReason, v.ExpectedBlockedUpdatedAtMs, v.ExpectedPayloadDigest, v.ExpectedCheckpointDigest, v.ProofDigest, v.ProvedAtMs, v.ProofExpiresAtMs, v.EvidenceRetainUntilMs, v.CommittedAtMs, v.IntentCount, v.ChildCount, v.ResultCode}
	}
	if p.BlockedTerminationProof != nil {
		v := p.BlockedTerminationProof
		d.Proof = &proofDTO{Protocol: v.ProtocolVersion, PlanID: v.PlanID, ExpectedRevision: v.ExpectedRevision, ExpectedReason: v.ExpectedBlockedReason, ExpectedPayload: v.ExpectedPayloadDigest, ExpectedCheckpoint: v.ExpectedCheckpointDigest, Evidence: v.EvidenceDigest, Workload: v.WorkloadDigest, Placement: v.PlacementDigest, Exclusion: v.ExclusionDigest, Children: v.ChildInventoryDigest, Manager: v.ManagerQuiescenceDigest, CapturedAt: v.CapturedAtMs, ExpiresAt: v.ExpiresAtMs, EvidenceRetain: v.EvidenceRetainUntilMs, IntentCount: v.IntentCount, ChildCount: v.ChildCount, ScaleResolution: string(v.ScaleResolution), ReleaseOutcome: string(v.ExclusionReleaseOutcome)}
	}
	return d
}

func submissionToDTO(s core.Submission) submissionDTO {
	d := submissionDTO{
		IdempotencyKey: s.IdempotencyKey, Realm: s.Realm, Domain: string(s.Domain), Operation: string(s.Operation),
		Workload: workloadToDTO(s.Workload), VictimHolderID: s.VictimHolderID,
		Expected: s.ExpectedReplicas, Desired: s.DesiredReplicas,
		RouteTTL: s.RoutePlanTTLms, PlanTTL: s.PlanTTLms, RecoveryTTL: s.RecoveryRetentionMs,
		Limits: limitsDTO{RoutePlans: s.Limits.MaxRoutePlansInflight, Shards: s.Limits.MaxShardsPerWave},
	}
	if s.PinnedTarget != (core.HolderRef{}) {
		value := holderToDTO(s.PinnedTarget)
		d.PinnedTarget = &value
	}
	d.Moves = make([]intentDTO, len(s.Moves))
	for i := range s.Moves {
		d.Moves[i] = intentToDTO(s.Moves[i])
	}
	return d
}
func workloadToDTO(v core.WorkloadRef) workloadDTO {
	return workloadDTO{v.Realm, string(v.Domain), v.Namespace, v.Kind, v.Name}
}
func intentToDTO(v core.ShardMoveIntent) intentDTO {
	return intentDTO{string(v.Domain), v.VirtualShard, v.SourceHolderID, v.TargetHolderID, v.TargetOrdinal}
}
func holderToDTO(v core.HolderRef) holderDTO {
	return holderDTO{string(v.Domain), v.HolderID, v.BootID, v.PodUID, v.WorkloadUID, v.Ordinal}
}
func fenceToDTO(v authoritystore.ShardWriteFence) fenceDTO {
	return fenceDTO{v.ExpectedHolderID, v.ExpectedHolderBootID, v.ExpectedAssignmentRevision, v.ExpectedWriterEpoch, v.ExpectedLeaseEpoch}
}
func moveToDTO(v core.ResolvedMove) moveDTO {
	return moveDTO{intentToDTO(v.Intent), v.WaveIndex, holderToDTO(v.Source), v.SourceEndpoint, holderToDTO(v.Target), v.TargetEndpoint, fenceToDTO(v.ExpectedSourceFence), v.ChildPlanID, v.ChildTransitionID, v.ChildPayloadDigest, v.ChildPhase, v.EnsureObserved, v.Verified, v.Reconciled}
}

func fromDTO(d planDTO) (core.Plan, error) {
	s, err := submissionFromDTO(d.Submission)
	if err != nil {
		return core.Plan{}, err
	}
	p := core.Plan{SchemaVersion: d.SchemaVersion, PlanID: d.PlanID, IdempotencyKey: d.IdempotencyKey, PayloadDigest: d.PayloadDigest, Submission: s,
		Status: core.ExecutionStatus(d.Status), Phase: core.Phase(d.Phase), WaveIndex: d.WaveIndex, Attempt: d.Attempt, Revision: d.Revision,
		WorkloadIdentity: core.WorkloadIdentity{UID: d.Workload.UID, ResourceVersion: d.Workload.ResourceVersion}, Target: holderFromDTO(d.Target), Victim: holderFromDTO(d.Victim),
		PausedReason: d.PausedReason, BlockedReason: d.BlockedReason, ErrorCode: d.ErrorCode, CreatedAtMs: d.CreatedAtMs, UpdatedAtMs: d.UpdatedAtMs, CompletedAtMs: d.CompletedAtMs,
		StartDeadlineMs: d.StartDeadline, RecoveryRetainUntilMs: d.RecoveryRetain}
	if d.ScaleIntent != nil {
		v := d.ScaleIntent
		p.ScaleIntent = &core.ScaleIntent{WorkloadUID: v.WorkloadUID, ExpectedResourceVersion: v.ResourceVersion, ExpectedReplicas: v.Expected, DesiredReplicas: v.Desired, Effect: core.EffectState(v.Effect)}
	}
	if d.Exclusion != nil {
		v := d.Exclusion
		p.ExclusionIntent = &core.ExclusionIntent{Victim: holderFromDTO(v.Victim), ExpectedRevision: v.Expected, ObservedRevision: v.Observed, OwnerPlanID: v.Owner, Effect: core.EffectState(v.Effect)}
	}
	if d.Cleanup != nil {
		v := d.Cleanup
		p.CleanupIntent = &core.CleanupIntent{FailureCode: v.FailureCode, ScaleResolution: core.ScaleResolution(v.ScaleResolution), ReleaseOutcome: core.PlacementReleaseOutcome(v.ReleaseOutcome)}
	}
	if d.ResolvedMoves != nil {
		p.ResolvedMoves = make([]core.ResolvedMove, len(d.ResolvedMoves))
		for i := range d.ResolvedMoves {
			p.ResolvedMoves[i], err = moveFromDTO(d.ResolvedMoves[i])
			if err != nil {
				return core.Plan{}, err
			}
		}
	}
	if d.Receipt != nil {
		v := d.Receipt
		p.BlockedTermination = &core.BlockedTerminationReceipt{ProtocolVersion: v.Protocol, TerminationID: v.Termination, ExpectedBlockedRevision: v.ExpectedRevision, ExpectedBlockedReason: v.ExpectedReason, ExpectedBlockedUpdatedAtMs: v.ExpectedUpdated, ExpectedPayloadDigest: v.ExpectedPayload, ExpectedCheckpointDigest: v.ExpectedCheckpoint, ProofDigest: v.ProofDigest, ProvedAtMs: v.ProvedAt, ProofExpiresAtMs: v.ProofExpires, EvidenceRetainUntilMs: v.EvidenceRetain, CommittedAtMs: v.CommittedAt, IntentCount: v.IntentCount, ChildCount: v.ChildCount, ResultCode: v.Result}
	}
	if d.Proof != nil {
		v := d.Proof
		p.BlockedTerminationProof = &core.BlockedQuiescenceProof{ProtocolVersion: v.Protocol, PlanID: v.PlanID, ExpectedRevision: v.ExpectedRevision, ExpectedBlockedReason: v.ExpectedReason, ExpectedPayloadDigest: v.ExpectedPayload, ExpectedCheckpointDigest: v.ExpectedCheckpoint, EvidenceDigest: v.Evidence, WorkloadDigest: v.Workload, PlacementDigest: v.Placement, ExclusionDigest: v.Exclusion, ChildInventoryDigest: v.Children, ManagerQuiescenceDigest: v.Manager, CapturedAtMs: v.CapturedAt, ExpiresAtMs: v.ExpiresAt, EvidenceRetainUntilMs: v.EvidenceRetain, IntentCount: v.IntentCount, ChildCount: v.ChildCount, ScaleResolution: core.ScaleResolution(v.ScaleResolution), ExclusionReleaseOutcome: core.PlacementReleaseOutcome(v.ReleaseOutcome)}
	}
	return p, nil
}

func submissionFromDTO(d submissionDTO) (core.Submission, error) {
	s := core.Submission{IdempotencyKey: d.IdempotencyKey, Realm: d.Realm, Domain: actorrouting.RouteDomain(d.Domain), Operation: core.Operation(d.Operation), Workload: core.WorkloadRef{Realm: d.Workload.Realm, Domain: actorrouting.RouteDomain(d.Workload.Domain), Namespace: d.Workload.Namespace, Kind: d.Workload.Kind, Name: d.Workload.Name}, VictimHolderID: d.VictimHolderID, ExpectedReplicas: d.Expected, DesiredReplicas: d.Desired, RoutePlanTTLms: d.RouteTTL, PlanTTLms: d.PlanTTL, RecoveryRetentionMs: d.RecoveryTTL, Limits: core.FunctionalLimits{MaxRoutePlansInflight: d.Limits.RoutePlans, MaxShardsPerWave: d.Limits.Shards}}
	if d.PinnedTarget != nil {
		s.PinnedTarget = holderFromDTO(*d.PinnedTarget)
	}
	if d.Moves != nil {
		s.Moves = make([]core.ShardMoveIntent, len(d.Moves))
		for i, v := range d.Moves {
			s.Moves[i] = core.ShardMoveIntent{Domain: actorrouting.RouteDomain(v.Domain), VirtualShard: v.Shard, SourceHolderID: v.Source, TargetHolderID: v.Target, TargetOrdinal: v.Ordinal}
		}
	}
	return s, nil
}
func holderFromDTO(v holderDTO) core.HolderRef {
	return core.HolderRef{Domain: actorrouting.RouteDomain(v.Domain), HolderID: v.HolderID, BootID: v.BootID, PodUID: v.PodUID, WorkloadUID: v.WorkloadUID, Ordinal: v.Ordinal}
}
func fenceFromDTO(v fenceDTO) authoritystore.ShardWriteFence {
	return authoritystore.ShardWriteFence{ExpectedHolderID: v.HolderID, ExpectedHolderBootID: v.BootID, ExpectedAssignmentRevision: v.Assignment, ExpectedWriterEpoch: v.Writer, ExpectedLeaseEpoch: v.Lease}
}
func moveFromDTO(v moveDTO) (core.ResolvedMove, error) {
	return core.ResolvedMove{Intent: core.ShardMoveIntent{Domain: actorrouting.RouteDomain(v.Intent.Domain), VirtualShard: v.Intent.Shard, SourceHolderID: v.Intent.Source, TargetHolderID: v.Intent.Target, TargetOrdinal: v.Intent.Ordinal}, WaveIndex: v.Wave, Source: holderFromDTO(v.Source), SourceEndpoint: v.SourceEndpoint, Target: holderFromDTO(v.Target), TargetEndpoint: v.TargetEndpoint, ExpectedSourceFence: fenceFromDTO(v.Fence), ChildPlanID: v.ChildPlan, ChildTransitionID: v.ChildTransition, ChildPayloadDigest: v.ChildPayload, ChildPhase: v.ChildPhase, EnsureObserved: v.EnsureObserved, Verified: v.Verified, Reconciled: v.Reconciled}, nil
}
