package workflow

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	workflowpb "farmgame/api/gen/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/protobuf/proto"
)

// legacyWorkType derives the generic work type of a legacy WorkflowRecord.
func legacyWorkType(kind workflowpb.WorkflowKind) (string, error) {
	name, err := StableKindName(kind)
	if err != nil {
		return "", err
	}
	return "LEGACY_" + name, nil
}

// legacyKind maps a generic legacy work type back to its proto kind.
func legacyKind(workType string) (workflowpb.WorkflowKind, error) {
	switch workType {
	case "LEGACY_PLANT":
		return workflowpb.WorkflowKind_PLANT, nil
	case "LEGACY_HARVEST":
		return workflowpb.WorkflowKind_HARVEST, nil
	case "LEGACY_THEFT":
		return workflowpb.WorkflowKind_THEFT, nil
	case "LEGACY_CARE":
		return workflowpb.WorkflowKind_CARE, nil
	default:
		return workflowpb.WorkflowKind_WORKFLOW_KIND_UNSPECIFIED, fmt.Errorf("%w: legacy kind %q", ErrInvalid, workType)
	}
}

func legacyState(state workflowpb.WorkflowState) WorkState {
	switch state {
	case workflowpb.WorkflowState_COMPLETED:
		return WorkStateCompleted
	case workflowpb.WorkflowState_COMPENSATED:
		return WorkStateCompensated
	case workflowpb.WorkflowState_RETRY_WAIT:
		return WorkStateRunning
	default:
		return WorkStateCreated
	}
}

func legacyProtoState(state WorkState) workflowpb.WorkflowState {
	switch state {
	case WorkStateCompleted:
		return workflowpb.WorkflowState_COMPLETED
	case WorkStateCompensated:
		return workflowpb.WorkflowState_COMPENSATED
	case WorkStateRunning, WorkStateStepUnknown:
		return workflowpb.WorkflowState_RETRY_WAIT
	default:
		return workflowpb.WorkflowState_PENDING
	}
}

// workRecordFromLegacy synthesizes the generic control view of a legacy
// record. The full proto is carried in Payload so the round trip is lossless;
// PayloadHash is the canonical digest of the original command hash.
func workRecordFromLegacy(record *workflowpb.WorkflowRecord) (*WorkRecord, error) {
	if err := Validate(record); err != nil {
		return nil, err
	}
	workType, err := legacyWorkType(record.Kind)
	if err != nil {
		return nil, err
	}
	invocationID, err := workidentity.ParticipantInvocationID(workType, record.WorkflowId, "LEGACY_PROCESS", 1)
	if err != nil {
		return nil, err
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(record)
	if err != nil {
		return nil, err
	}
	generic := &WorkRecord{
		Address: workidentity.WorkAddress{
			WorkID:        record.WorkflowId,
			WorkType:      workType,
			PlacementKind: workidentity.IndependentWorkBucket,
			BucketVersion: 1,
		},
		SubjectPlayerID: record.InitiatorPlayerId,
		OperationID:     record.OperationId,
		Payload:         payload,
		PayloadHash:     sha256.Sum256(record.PayloadHash),
		State:           legacyState(record.State),
		WorkVersion:     record.WorkflowRevision,
		Steps: []StepRecord{{
			Name:         "LEGACY_PROCESS",
			InvocationID: invocationID,
			Status:       StepPending,
		}},
		NextDueAtMs:     record.NextRetryAtMs,
		RetryCount:      uint64(record.Attempts),
		LastErrorClass:  record.LastError,
		DeadlineAtMs:    record.DeadlineAtMs,
		LeaseOwner:      record.LeaseOwner,
		LeaseEpoch:      record.LeaseEpoch,
		LeaseExpireAtMs: record.LeaseUntilMs,
		CreatedAtMs:     record.CreatedAtMs,
		UpdatedAtMs:     record.UpdatedAtMs,
	}
	if generic.Terminal() {
		generic.CompletedAtMs = record.UpdatedAtMs
	}
	if err := generic.Validate(); err != nil {
		return nil, err
	}
	return generic, nil
}

// legacyFromWorkRecord rebuilds the proto view, overlaying the fence fields
// advanced by the generic claim path.
func legacyFromWorkRecord(record *WorkRecord) (*workflowpb.WorkflowRecord, error) {
	if err := record.Validate(); err != nil {
		return nil, err
	}
	var stored workflowpb.WorkflowRecord
	if err := proto.Unmarshal(record.Payload, &stored); err != nil {
		return nil, fmt.Errorf("workflow: legacy payload decode: %w", err)
	}
	kind, err := legacyKind(record.Address.WorkType)
	if err != nil {
		return nil, err
	}
	stored.Kind = kind
	stored.State = legacyProtoState(record.State)
	stored.WorkflowRevision = record.WorkVersion
	stored.LeaseOwner = record.LeaseOwner
	stored.LeaseEpoch = record.LeaseEpoch
	stored.LeaseUntilMs = record.LeaseExpireAtMs
	stored.NextRetryAtMs = record.NextDueAtMs
	stored.Attempts = int32(record.RetryCount)
	stored.LastError = record.LastErrorClass
	return &stored, nil
}

// LegacyRepository exposes the existing legacy Store through the generic
// WorkRepository port so one RecoveryLoop drives both record families.
// Known deviations from the generic contract (documented in the WF1
// handoff): lease adjudication uses caller time instead of repository
// trusted time, and lease renew is not supported by the legacy family.
type LegacyRepository struct {
	store Store
	now   func() time.Time
}

func NewLegacyRepository(store Store, now func() time.Time) (*LegacyRepository, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: legacy store", ErrInvalid)
	}
	if now == nil {
		now = time.Now
	}
	return &LegacyRepository{store: store, now: now}, nil
}

func (r *LegacyRepository) CreateIfAbsent(ctx context.Context, candidate *WorkRecord) (*WorkRecord, bool, error) {
	legacy, err := legacyFromWorkRecord(candidate)
	if err != nil {
		return nil, false, err
	}
	stored, created, err := r.store.CreateOrLoad(ctx, legacy)
	if err != nil {
		return nil, false, err
	}
	generic, err := workRecordFromLegacy(stored)
	if err != nil {
		return nil, false, err
	}
	return generic, created, nil
}

func (r *LegacyRepository) Get(ctx context.Context, workType, workID string) (*WorkRecord, error) {
	if _, err := legacyKind(workType); err != nil {
		return nil, err
	}
	record, err := r.store.Get(ctx, workID)
	if err != nil {
		return nil, err
	}
	return workRecordFromLegacy(record)
}

func (r *LegacyRepository) Advance(ctx context.Context, next *WorkRecord, opts AdvanceOptions) (*WorkRecord, error) {
	legacy, err := legacyFromWorkRecord(next)
	if err != nil {
		return nil, err
	}
	updated, err := r.store.Update(ctx, legacy, UpdateOptions{
		ExpectedRevision: opts.ExpectedWorkVersion,
		LeaseOwner:       opts.LeaseOwner,
		LeaseEpoch:       opts.LeaseEpoch,
		Now:              r.now(),
	})
	if err != nil {
		return nil, err
	}
	return workRecordFromLegacy(updated)
}

func (r *LegacyRepository) ClaimDue(ctx context.Context, owner, token string, limit int, ttl time.Duration) ([]*ClaimedWork, error) {
	claimed, err := r.store.ClaimDue(ctx, owner, r.now(), limit, ttl)
	if err != nil {
		return nil, err
	}
	result := make([]*ClaimedWork, 0, len(claimed))
	for _, record := range claimed {
		generic, err := workRecordFromLegacy(record)
		if err != nil {
			return nil, err
		}
		result = append(result, &ClaimedWork{Record: generic, AcceptedTTL: ttl})
	}
	return result, nil
}

func (r *LegacyRepository) RenewLease(ctx context.Context, workType, workID, owner, token string, epoch uint64, ttl time.Duration) (*WorkRecord, error) {
	return nil, fmt.Errorf("%w: legacy family does not support lease renew", ErrInvalid)
}

func (r *LegacyRepository) PendingDue(ctx context.Context) (int64, error) {
	return r.store.PendingDue(ctx, r.now())
}

// LegacyHandler resumes a legacy record through the existing Runner, keeping
// the legacy business path unchanged while sharing the recovery mainline.
type LegacyHandler struct {
	Runner *Runner
	Now    func() time.Time
}

func (h LegacyHandler) Resume(ctx context.Context, record *WorkRecord, lease LeaseController) error {
	if h.Runner == nil {
		return fmt.Errorf("%w: legacy runner", ErrInvalid)
	}
	legacy, err := legacyFromWorkRecord(record)
	if err != nil {
		return err
	}
	fence := lease.Fence()
	legacy.LeaseOwner = fence.Owner
	legacy.LeaseEpoch = fence.Epoch
	err = h.Runner.Process(ctx, legacy)
	if err != nil {
		return ErrRetryScheduled
	}
	return nil
}
