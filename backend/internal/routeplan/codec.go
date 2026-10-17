package routeplan

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderidentity"
	"farmgame/internal/storage/authoritystore"
)

const (
	PlanSchemaVersion       = "route-plan-v2"
	LegacyPlanSchemaVersion = "route-plan-v1"

	// JSON may expand one allowed input byte to a six-byte \uXXXX escape. The
	// structural allowance covers every fixed field name, integer, boolean and
	// delimiter in canonicalPlan. These are hard decode-before-hash bounds.
	maxCanonicalPlanStringBytes = 15*MaxHolderIDBytes + 2*MaxEndpointBytes + MaxEvidenceBytes + 128
	maxCanonicalPlanStructure   = 4096
	MaxCanonicalPlanBytes       = 6*maxCanonicalPlanStringBytes + maxCanonicalPlanStructure
	MaxPlanChecksumBytes        = sha256.Size * 2
	MaxPlanEnvelopeBytes        = MaxCanonicalPlanBytes + MaxPlanChecksumBytes + MaxPlanIDBytes + 256
)

// PlanEnvelope is the fixed durable representation used by Redis adapters.
// CanonicalJSON is the only payload whose checksum is trusted.
type PlanEnvelope struct {
	SchemaVersion string
	PlanID        string
	Revision      uint64
	CanonicalJSON []byte
	Checksum      string
	UpdatedAtMs   int64
	ExpiresAtMs   int64
}

type canonicalFence struct {
	HolderID           string `json:"holder_id"`
	HolderBootID       string `json:"holder_boot_id"`
	AssignmentRevision uint64 `json:"assignment_revision"`
	WriterEpoch        uint64 `json:"writer_epoch"`
	LeaseEpoch         uint64 `json:"lease_epoch"`
}

type canonicalProof struct {
	ExpectedHolderBootID string `json:"expected_holder_boot_id"`
	HolderUnavailable    bool   `json:"holder_unavailable"`
	LeaseExpired         bool   `json:"lease_expired"`
	Evidence             string `json:"evidence"`
}

// canonicalPlan intentionally uses only scalar/fixed-order fields. No map is
// encoded, so the JSON bytes are stable across Go versions and Redis clients.
// legacyCanonicalPlan is the exact route-plan-v1 payload retained for
// read/recovery compatibility. New writes always use canonicalPlan/v2.
type legacyCanonicalPlan struct {
	SchemaVersion       string         `json:"schema_version"`
	PlanID              string         `json:"plan_id"`
	TransitionID        string         `json:"transition_id"`
	Domain              string         `json:"domain"`
	VirtualShard        uint32         `json:"virtual_shard"`
	SourceHolderID      string         `json:"source_holder_id"`
	SourceBootID        string         `json:"source_boot_id"`
	SourceEndpoint      string         `json:"source_endpoint"`
	TargetHolderID      string         `json:"target_holder_id"`
	TargetBootID        string         `json:"target_boot_id"`
	TargetEndpoint      string         `json:"target_endpoint"`
	ExpectedSourceFence canonicalFence `json:"expected_source_fence"`
	RequestedTTLms      int64          `json:"requested_ttl_ms"`
	Operation           string         `json:"operation"`
	Phase               string         `json:"phase"`
	Attempt             uint64         `json:"attempt"`
	Revision            uint64         `json:"revision"`
	CreatedAtMs         int64          `json:"created_at_ms"`
	UpdatedAtMs         int64          `json:"updated_at_ms"`
	StartedAtMs         int64          `json:"started_at_ms"`
	CompletedAtMs       int64          `json:"completed_at_ms"`
	ExpiresAtMs         int64          `json:"expires_at_ms"`
	ErrorCode           string         `json:"error_code"`
	TakeoverProof       canonicalProof `json:"takeover_proof"`
}

type canonicalDeploymentIdentity struct {
	PodUID      string `json:"pod_uid"`
	WorkloadUID string `json:"workload_uid"`
	Ordinal     int32  `json:"ordinal"`
}

type canonicalPlan struct {
	SchemaVersion string `json:"schema_version"`
	PlanID        string `json:"plan_id"`
	TransitionID  string `json:"transition_id"`
	Domain        string `json:"domain"`
	VirtualShard  uint32 `json:"virtual_shard"`

	SourceHolderID           string                      `json:"source_holder_id"`
	SourceBootID             string                      `json:"source_boot_id"`
	SourceDeploymentIdentity canonicalDeploymentIdentity `json:"source_deployment_identity"`
	SourceEndpoint           string                      `json:"source_endpoint"`
	TargetHolderID           string                      `json:"target_holder_id"`
	TargetBootID             string                      `json:"target_boot_id"`
	TargetDeploymentIdentity canonicalDeploymentIdentity `json:"target_deployment_identity"`
	TargetEndpoint           string                      `json:"target_endpoint"`

	ExpectedSourceFence canonicalFence `json:"expected_source_fence"`
	RequestedTTLms      int64          `json:"requested_ttl_ms"`
	Operation           string         `json:"operation"`
	Phase               string         `json:"phase"`
	Attempt             uint64         `json:"attempt"`
	Revision            uint64         `json:"revision"`
	CreatedAtMs         int64          `json:"created_at_ms"`
	UpdatedAtMs         int64          `json:"updated_at_ms"`
	StartedAtMs         int64          `json:"started_at_ms"`
	CompletedAtMs       int64          `json:"completed_at_ms"`
	ExpiresAtMs         int64          `json:"expires_at_ms"`
	OwnerScalePlanID    string         `json:"owner_scale_plan_id"`
	OwnerRetainUntilMs  int64          `json:"owner_retain_until_ms"`
	OwnerWaveID         string         `json:"owner_wave_id,omitempty"`
	OwnerWaveSize       uint32         `json:"owner_wave_size,omitempty"`
	OwnerWavePosition   uint32         `json:"owner_wave_position,omitempty"`
	ErrorCode           string         `json:"error_code"`
	TakeoverProof       canonicalProof `json:"takeover_proof"`
}

func canonicalForPlan(plan Plan) canonicalPlan {
	return canonicalPlan{
		SchemaVersion: PlanSchemaVersion, PlanID: plan.PlanID, TransitionID: plan.TransitionID,
		Domain: string(plan.Domain), VirtualShard: plan.VirtualShard,
		SourceHolderID: plan.SourceHolderID, SourceBootID: plan.SourceBootID,
		SourceDeploymentIdentity: canonicalDeploymentIdentity{PodUID: plan.SourceDeploymentIdentity.PodUID,
			WorkloadUID: plan.SourceDeploymentIdentity.WorkloadUID, Ordinal: plan.SourceDeploymentIdentity.Ordinal},
		SourceEndpoint: plan.SourceEndpoint,
		TargetHolderID: plan.TargetHolderID, TargetBootID: plan.TargetBootID,
		TargetDeploymentIdentity: canonicalDeploymentIdentity{PodUID: plan.TargetDeploymentIdentity.PodUID,
			WorkloadUID: plan.TargetDeploymentIdentity.WorkloadUID, Ordinal: plan.TargetDeploymentIdentity.Ordinal},
		TargetEndpoint: plan.TargetEndpoint,
		ExpectedSourceFence: canonicalFence{
			HolderID: plan.ExpectedSourceFence.ExpectedHolderID, HolderBootID: plan.ExpectedSourceFence.ExpectedHolderBootID,
			AssignmentRevision: plan.ExpectedSourceFence.ExpectedAssignmentRevision,
			WriterEpoch:        plan.ExpectedSourceFence.ExpectedWriterEpoch, LeaseEpoch: plan.ExpectedSourceFence.ExpectedLeaseEpoch,
		},
		RequestedTTLms: plan.RequestedTTLms, Operation: string(plan.Operation), Phase: plan.Phase.String(),
		Attempt: plan.Attempt, Revision: plan.Revision, CreatedAtMs: plan.CreatedAtMs, UpdatedAtMs: plan.UpdatedAtMs,
		StartedAtMs: plan.StartedAtMs, CompletedAtMs: plan.CompletedAtMs, ExpiresAtMs: plan.ExpiresAtMs,
		OwnerScalePlanID: plan.OwnerScalePlanID, OwnerRetainUntilMs: plan.OwnerRetainUntilMs,
		OwnerWaveID: plan.OwnerWaveID, OwnerWaveSize: plan.OwnerWaveSize, OwnerWavePosition: plan.OwnerWavePosition,
		ErrorCode: plan.ErrorCode,
		TakeoverProof: canonicalProof{ExpectedHolderBootID: plan.TakeoverProof.ExpectedHolderBootID,
			HolderUnavailable: plan.TakeoverProof.HolderUnavailable, LeaseExpired: plan.TakeoverProof.LeaseExpired,
			Evidence: plan.TakeoverProof.Evidence},
	}
}

func legacyCanonicalForPlan(plan Plan) legacyCanonicalPlan {
	return legacyCanonicalPlan{SchemaVersion: LegacyPlanSchemaVersion, PlanID: plan.PlanID, TransitionID: plan.TransitionID,
		Domain: string(plan.Domain), VirtualShard: plan.VirtualShard, SourceHolderID: plan.SourceHolderID,
		SourceBootID: plan.SourceBootID, SourceEndpoint: plan.SourceEndpoint, TargetHolderID: plan.TargetHolderID,
		TargetBootID: plan.TargetBootID, TargetEndpoint: plan.TargetEndpoint,
		ExpectedSourceFence: canonicalFence{HolderID: plan.ExpectedSourceFence.ExpectedHolderID,
			HolderBootID:       plan.ExpectedSourceFence.ExpectedHolderBootID,
			AssignmentRevision: plan.ExpectedSourceFence.ExpectedAssignmentRevision,
			WriterEpoch:        plan.ExpectedSourceFence.ExpectedWriterEpoch, LeaseEpoch: plan.ExpectedSourceFence.ExpectedLeaseEpoch},
		RequestedTTLms: plan.RequestedTTLms, Operation: string(plan.Operation), Phase: plan.Phase.String(),
		Attempt: plan.Attempt, Revision: plan.Revision, CreatedAtMs: plan.CreatedAtMs, UpdatedAtMs: plan.UpdatedAtMs,
		StartedAtMs: plan.StartedAtMs, CompletedAtMs: plan.CompletedAtMs, ExpiresAtMs: plan.ExpiresAtMs,
		ErrorCode: plan.ErrorCode,
		TakeoverProof: canonicalProof{ExpectedHolderBootID: plan.TakeoverProof.ExpectedHolderBootID,
			HolderUnavailable: plan.TakeoverProof.HolderUnavailable, LeaseExpired: plan.TakeoverProof.LeaseExpired,
			Evidence: plan.TakeoverProof.Evidence}}
}

func planFromLegacyCanonical(value legacyCanonicalPlan) (Plan, error) {
	phase, err := ParsePhase(value.Phase)
	if err != nil {
		return Plan{}, err
	}
	plan := Plan{
		PlanID: value.PlanID, TransitionID: value.TransitionID, Domain: actorrouting.RouteDomain(value.Domain),
		VirtualShard: value.VirtualShard, SourceHolderID: value.SourceHolderID, SourceBootID: value.SourceBootID,
		SourceEndpoint: value.SourceEndpoint, TargetHolderID: value.TargetHolderID, TargetBootID: value.TargetBootID,
		TargetEndpoint: value.TargetEndpoint,
		ExpectedSourceFence: authoritystore.ShardWriteFence{ExpectedHolderID: value.ExpectedSourceFence.HolderID,
			ExpectedHolderBootID: value.ExpectedSourceFence.HolderBootID, ExpectedAssignmentRevision: value.ExpectedSourceFence.AssignmentRevision,
			ExpectedWriterEpoch: value.ExpectedSourceFence.WriterEpoch, ExpectedLeaseEpoch: value.ExpectedSourceFence.LeaseEpoch},
		RequestedTTLms: value.RequestedTTLms, Operation: OperationKind(value.Operation), Phase: phase,
		Attempt: value.Attempt, Revision: value.Revision, CreatedAtMs: value.CreatedAtMs, UpdatedAtMs: value.UpdatedAtMs,
		StartedAtMs: value.StartedAtMs, CompletedAtMs: value.CompletedAtMs, ExpiresAtMs: value.ExpiresAtMs,
		ErrorCode: value.ErrorCode,
		TakeoverProof: TakeoverProof{ExpectedHolderBootID: value.TakeoverProof.ExpectedHolderBootID,
			HolderUnavailable: value.TakeoverProof.HolderUnavailable, LeaseExpired: value.TakeoverProof.LeaseExpired,
			Evidence: value.TakeoverProof.Evidence},
	}
	if err := plan.Validate(); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func planFromCanonical(value canonicalPlan) (Plan, error) {
	phase, err := ParsePhase(value.Phase)
	if err != nil {
		return Plan{}, err
	}
	plan := Plan{
		PlanID: value.PlanID, TransitionID: value.TransitionID, Domain: actorrouting.RouteDomain(value.Domain),
		VirtualShard: value.VirtualShard, SourceHolderID: value.SourceHolderID, SourceBootID: value.SourceBootID,
		SourceDeploymentIdentity: holderidentity.DeploymentIdentity{PodUID: value.SourceDeploymentIdentity.PodUID,
			WorkloadUID: value.SourceDeploymentIdentity.WorkloadUID, Ordinal: value.SourceDeploymentIdentity.Ordinal},
		SourceEndpoint: value.SourceEndpoint,
		TargetHolderID: value.TargetHolderID, TargetBootID: value.TargetBootID,
		TargetDeploymentIdentity: holderidentity.DeploymentIdentity{PodUID: value.TargetDeploymentIdentity.PodUID,
			WorkloadUID: value.TargetDeploymentIdentity.WorkloadUID, Ordinal: value.TargetDeploymentIdentity.Ordinal},
		TargetEndpoint: value.TargetEndpoint,
		ExpectedSourceFence: authoritystore.ShardWriteFence{ExpectedHolderID: value.ExpectedSourceFence.HolderID,
			ExpectedHolderBootID: value.ExpectedSourceFence.HolderBootID, ExpectedAssignmentRevision: value.ExpectedSourceFence.AssignmentRevision,
			ExpectedWriterEpoch: value.ExpectedSourceFence.WriterEpoch, ExpectedLeaseEpoch: value.ExpectedSourceFence.LeaseEpoch},
		RequestedTTLms: value.RequestedTTLms, Operation: OperationKind(value.Operation), Phase: phase,
		Attempt: value.Attempt, Revision: value.Revision, CreatedAtMs: value.CreatedAtMs, UpdatedAtMs: value.UpdatedAtMs,
		StartedAtMs: value.StartedAtMs, CompletedAtMs: value.CompletedAtMs, ExpiresAtMs: value.ExpiresAtMs,
		OwnerScalePlanID: value.OwnerScalePlanID, OwnerRetainUntilMs: value.OwnerRetainUntilMs,
		OwnerWaveID: value.OwnerWaveID, OwnerWaveSize: value.OwnerWaveSize, OwnerWavePosition: value.OwnerWavePosition,
		ErrorCode: value.ErrorCode,
		TakeoverProof: TakeoverProof{ExpectedHolderBootID: value.TakeoverProof.ExpectedHolderBootID,
			HolderUnavailable: value.TakeoverProof.HolderUnavailable, LeaseExpired: value.TakeoverProof.LeaseExpired,
			Evidence: value.TakeoverProof.Evidence},
	}
	if err := plan.Validate(); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

// EncodePlan returns the complete canonical JSON and its lowercase SHA-256.
func EncodePlan(plan Plan) (PlanEnvelope, error) {
	if err := plan.Validate(); err != nil {
		return PlanEnvelope{}, err
	}
	canonicalJSON, err := json.Marshal(canonicalForPlan(plan))
	if err != nil {
		return PlanEnvelope{}, fmt.Errorf("%w: encode: %v", ErrPlanCorrupt, err)
	}
	sum := sha256.Sum256(canonicalJSON)
	return PlanEnvelope{SchemaVersion: PlanSchemaVersion, PlanID: plan.PlanID, Revision: plan.Revision,
		CanonicalJSON: append([]byte(nil), canonicalJSON...), Checksum: hex.EncodeToString(sum[:]),
		UpdatedAtMs: plan.UpdatedAtMs, ExpiresAtMs: plan.ExpiresAtMs}, nil
}

func CanonicalizePlan(plan Plan) (PlanEnvelope, error) { return EncodePlan(plan) }

// DecodePlan verifies schema, checksum and exact canonical bytes. Unknown
// fields and trailing JSON are rejected, making corruption fail closed.
func DecodePlan(envelope PlanEnvelope) (Plan, error) {
	if len(envelope.CanonicalJSON) == 0 || len(envelope.CanonicalJSON) > MaxCanonicalPlanBytes ||
		len(envelope.Checksum) != MaxPlanChecksumBytes || len(envelope.PlanID) > MaxPlanIDBytes ||
		len(envelope.SchemaVersion)+len(envelope.PlanID)+len(envelope.CanonicalJSON)+len(envelope.Checksum)+256 > MaxPlanEnvelopeBytes {
		return Plan{}, fmt.Errorf("%w: envelope capacity", ErrPlanCorrupt)
	}
	if (envelope.SchemaVersion != PlanSchemaVersion && envelope.SchemaVersion != LegacyPlanSchemaVersion) ||
		envelope.PlanID == "" || envelope.Revision == 0 {
		return Plan{}, fmt.Errorf("%w: envelope header", ErrPlanCorrupt)
	}
	sum := sha256.Sum256(envelope.CanonicalJSON)
	if envelope.Checksum != hex.EncodeToString(sum[:]) {
		return Plan{}, fmt.Errorf("%w: checksum", ErrPlanCorrupt)
	}
	decoder := json.NewDecoder(bytes.NewReader(envelope.CanonicalJSON))
	decoder.DisallowUnknownFields()
	var plan Plan
	var reencoded []byte
	var err error
	if envelope.SchemaVersion == LegacyPlanSchemaVersion {
		var value legacyCanonicalPlan
		if err = decoder.Decode(&value); err == nil {
			if value.SchemaVersion != LegacyPlanSchemaVersion || value.PlanID != envelope.PlanID || value.Revision != envelope.Revision {
				return Plan{}, fmt.Errorf("%w: envelope tuple", ErrPlanCorrupt)
			}
			plan, err = planFromLegacyCanonical(value)
			if err == nil {
				reencoded, err = json.Marshal(value)
			}
		}
	} else {
		var value canonicalPlan
		if err = decoder.Decode(&value); err == nil {
			if value.SchemaVersion != PlanSchemaVersion || value.PlanID != envelope.PlanID || value.Revision != envelope.Revision {
				return Plan{}, fmt.Errorf("%w: envelope tuple", ErrPlanCorrupt)
			}
			plan, err = planFromCanonical(value)
			if err == nil {
				reencoded, err = json.Marshal(canonicalForPlan(plan))
			}
		}
	}
	if err != nil {
		return Plan{}, fmt.Errorf("%w: json/plan: %v", ErrPlanCorrupt, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return Plan{}, fmt.Errorf("%w: trailing JSON", ErrPlanCorrupt)
	}
	if !bytes.Equal(reencoded, envelope.CanonicalJSON) {
		return Plan{}, fmt.Errorf("%w: non-canonical JSON", ErrPlanCorrupt)
	}
	if plan.UpdatedAtMs != envelope.UpdatedAtMs || plan.ExpiresAtMs != envelope.ExpiresAtMs {
		return Plan{}, fmt.Errorf("%w: timestamp tuple", ErrPlanCorrupt)
	}
	return plan, nil
}

func PlanChecksum(plan Plan) (string, error) {
	envelope, err := EncodePlan(plan)
	if err != nil {
		return "", err
	}
	return envelope.Checksum, nil
}
