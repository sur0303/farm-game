package autorebalance

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	core "farmgame/internal/scaleplan"
)

const RolloutSchemaVersion = "statefulset-holder-rollout-v2"

type RolloutPhase string

const (
	RolloutPhaseExpanding          RolloutPhase = "EXPANDING"
	RolloutPhaseDraining           RolloutPhase = "DRAINING_OLD_HOLDER"
	RolloutPhaseDeleting           RolloutPhase = "DELETING_OLD_POD"
	RolloutPhaseWaitingReplacement RolloutPhase = "WAITING_REPLACEMENT"
	RolloutPhaseContracting        RolloutPhase = "CONTRACTING_SPARE"
	RolloutPhaseAborting           RolloutPhase = "ABORTING"
	RolloutPhaseBlocked            RolloutPhase = "BLOCKED"
)

type RolloutRecoveryAction string

const (
	RolloutRecoveryResume RolloutRecoveryAction = "RESUME"
	RolloutRecoveryAbort  RolloutRecoveryAction = "ABORT"
)

// RolloutRecoveryRequest is an explicit operator/release-controller command.
// Merely fixing Kubernetes state never changes a BLOCKED checkpoint: the
// immutable command must name the exact rollout and intent generation.
type RolloutRecoveryRequest struct {
	CommandID    string
	RolloutID    string
	IntentHash   string
	Action       RolloutRecoveryAction
	DeadlineAtMs int64
}

func (r RolloutRecoveryRequest) Validate() error {
	if strings.TrimSpace(r.CommandID) == "" || len(r.CommandID) > 128 ||
		strings.TrimSpace(r.RolloutID) == "" || !validHexDigest(r.IntentHash) ||
		(r.Action != RolloutRecoveryResume && r.Action != RolloutRecoveryAbort) || r.DeadlineAtMs <= 0 {
		return fmt.Errorf("%w: rollout recovery request", ErrInvalidTopology)
	}
	return nil
}

// RolloutState is persisted as one StatefulSet annotation. ScalePlan and
// RoutePlan remain the durable owners of Shard movement; this record only
// remembers which safe orchestration step surrounds those child plans.
type RolloutState struct {
	SchemaVersion     string       `json:"schema_version"`
	RolloutID         string       `json:"rollout_id"`
	ReleaseID         string       `json:"release_id"`
	WorkloadUID       string       `json:"workload_uid"`
	SourceRevision    string       `json:"source_revision"`
	TargetRevision    string       `json:"target_revision"`
	TargetImageDigest string       `json:"target_image_digest"`
	ExpectedReplicas  int32        `json:"expected_replicas"`
	OriginalReplicas  int32        `json:"original_replicas"`
	StartedAtMs       int64        `json:"started_at_ms"`
	DeadlineAtMs      int64        `json:"deadline_at_ms"`
	IntentHash        string       `json:"intent_hash"`
	Phase             RolloutPhase `json:"phase"`

	VictimHolderID string `json:"victim_holder_id,omitempty"`
	VictimPodUID   string `json:"victim_pod_uid,omitempty"`
	VictimBootID   string `json:"victim_boot_id,omitempty"`
	VictimOrdinal  int32  `json:"victim_ordinal"`
	DrainPlanID    string `json:"drain_plan_id,omitempty"`
	// ExclusionRevision is copied from the completed DRAIN_HOLDER plan and
	// revalidated against its exact owner immediately before Pod deletion.
	ExclusionRevision uint64       `json:"exclusion_revision,omitempty"`
	BlockedReason     string       `json:"blocked_reason,omitempty"`
	BlockedFromPhase  RolloutPhase `json:"blocked_from_phase,omitempty"`
	BlockedAtMs       int64        `json:"blocked_at_ms,omitempty"`
}

// ValidateRolloutUpdate keeps the StatefulSet annotation a strict restart
// checkpoint instead of a free-form operator state machine. Shard movement is
// still owned by ScalePlan; this only constrains the orchestration edges around
// those durable plans.
func ValidateRolloutUpdate(current, next RolloutState) error {
	if err := current.Validate(); err != nil {
		return err
	}
	if err := next.Validate(); err != nil {
		return err
	}
	if current.SchemaVersion != next.SchemaVersion || current.RolloutID != next.RolloutID ||
		current.ReleaseID != next.ReleaseID || current.WorkloadUID != next.WorkloadUID ||
		current.SourceRevision != next.SourceRevision || current.TargetRevision != next.TargetRevision ||
		current.TargetImageDigest != next.TargetImageDigest || current.ExpectedReplicas != next.ExpectedReplicas ||
		current.OriginalReplicas != next.OriginalReplicas || current.StartedAtMs != next.StartedAtMs ||
		current.DeadlineAtMs != next.DeadlineAtMs || current.IntentHash != next.IntentHash {
		return fmt.Errorf("%w: immutable rollout identity changed", ErrInvalidTopology)
	}
	if current.Phase == RolloutPhaseBlocked {
		return fmt.Errorf("%w: blocked rollout is terminal", ErrInvalidTopology)
	}
	if next.Phase == RolloutPhaseBlocked {
		candidate := next
		candidate.Phase = current.Phase
		candidate.BlockedReason = current.BlockedReason
		candidate.BlockedFromPhase = current.BlockedFromPhase
		candidate.BlockedAtMs = current.BlockedAtMs
		if !reflect.DeepEqual(current, candidate) {
			return fmt.Errorf("%w: blocked transition changed checkpoint", ErrInvalidTopology)
		}
		return nil
	}

	sameVictimIdentity := func(left, right RolloutState) bool {
		return left.VictimHolderID == right.VictimHolderID && left.VictimPodUID == right.VictimPodUID &&
			left.VictimBootID == right.VictimBootID && left.VictimOrdinal == right.VictimOrdinal &&
			left.DrainPlanID == right.DrainPlanID
	}
	sameVictim := func(left, right RolloutState) bool {
		return sameVictimIdentity(left, right) && left.ExclusionRevision == right.ExclusionRevision
	}
	switch current.Phase {
	case RolloutPhaseExpanding:
		if next.Phase == RolloutPhaseDraining && next.DrainPlanID == "" {
			return nil
		}
	case RolloutPhaseDraining:
		if current.DrainPlanID == "" {
			if next.Phase == RolloutPhaseDraining && next.hasExactVictim() && next.DrainPlanID != "" {
				return nil
			}
			if next.Phase == RolloutPhaseContracting && next.DrainPlanID == "" {
				return nil
			}
		} else if next.Phase == RolloutPhaseDeleting && sameVictimIdentity(current, next) &&
			current.ExclusionRevision == 0 && next.ExclusionRevision > 0 {
			return nil
		}
	case RolloutPhaseDeleting:
		if next.Phase == RolloutPhaseWaitingReplacement && sameVictim(current, next) {
			return nil
		}
	case RolloutPhaseWaitingReplacement:
		if next.Phase == RolloutPhaseDraining && next.DrainPlanID == "" {
			return nil
		}
	}
	return fmt.Errorf("%w: illegal rollout phase transition %s -> %s", ErrInvalidTopology, current.Phase, next.Phase)
}

func (s RolloutState) Validate() error {
	if s.SchemaVersion != RolloutSchemaVersion || strings.TrimSpace(s.RolloutID) == "" ||
		strings.TrimSpace(s.ReleaseID) == "" || strings.TrimSpace(s.WorkloadUID) == "" ||
		strings.TrimSpace(s.SourceRevision) == "" || strings.TrimSpace(s.TargetRevision) == "" ||
		s.SourceRevision == s.TargetRevision || !validSHA256Digest(s.TargetImageDigest) ||
		s.ExpectedReplicas < 1 || s.ExpectedReplicas != s.OriginalReplicas || s.OriginalReplicas < 1 ||
		s.StartedAtMs <= 0 || s.DeadlineAtMs <= s.StartedAtMs || !validHexDigest(s.IntentHash) {
		return fmt.Errorf("%w: rollout identity", ErrInvalidTopology)
	}
	switch s.Phase {
	case RolloutPhaseExpanding, RolloutPhaseContracting:
		if s.VictimHolderID != "" || s.VictimPodUID != "" || s.VictimBootID != "" ||
			s.DrainPlanID != "" || s.ExclusionRevision != 0 || s.VictimOrdinal != -1 ||
			s.BlockedReason != "" || s.BlockedFromPhase != "" || s.BlockedAtMs != 0 {
			return fmt.Errorf("%w: rollout phase carries victim", ErrInvalidTopology)
		}
	case RolloutPhaseAborting:
		if s.VictimHolderID != "" || s.VictimPodUID != "" || s.VictimBootID != "" ||
			s.DrainPlanID != "" || s.ExclusionRevision != 0 || s.VictimOrdinal != -1 ||
			s.BlockedReason != "" || s.BlockedFromPhase != "" || s.BlockedAtMs != 0 {
			return fmt.Errorf("%w: aborting rollout carries victim", ErrInvalidTopology)
		}
	case RolloutPhaseDraining:
		if s.BlockedReason != "" || s.BlockedFromPhase != "" || s.BlockedAtMs != 0 || (s.DrainPlanID == "" &&
			(s.VictimHolderID != "" || s.VictimPodUID != "" || s.VictimBootID != "" || s.VictimOrdinal != -1)) {
			return fmt.Errorf("%w: draining checkpoint", ErrInvalidTopology)
		}
		if s.DrainPlanID != "" && (!s.hasExactVictim() || s.ExclusionRevision != 0) {
			return fmt.Errorf("%w: draining victim", ErrInvalidTopology)
		}
	case RolloutPhaseDeleting, RolloutPhaseWaitingReplacement:
		if !s.hasExactVictim() || s.DrainPlanID == "" || s.ExclusionRevision == 0 ||
			s.BlockedReason != "" || s.BlockedFromPhase != "" || s.BlockedAtMs != 0 {
			return fmt.Errorf("%w: replacement checkpoint", ErrInvalidTopology)
		}
	case RolloutPhaseBlocked:
		if strings.TrimSpace(s.BlockedReason) == "" || s.BlockedFromPhase == "" || s.BlockedFromPhase == RolloutPhaseBlocked ||
			s.BlockedAtMs <= 0 {
			return fmt.Errorf("%w: blocked reason", ErrInvalidTopology)
		}
		checkpoint := s
		checkpoint.Phase = s.BlockedFromPhase
		checkpoint.BlockedReason = ""
		checkpoint.BlockedFromPhase = ""
		checkpoint.BlockedAtMs = 0
		if err := checkpoint.Validate(); err != nil {
			return fmt.Errorf("%w: blocked checkpoint: %v", ErrInvalidTopology, err)
		}
	default:
		return fmt.Errorf("%w: rollout phase", ErrInvalidTopology)
	}
	return nil
}

func (s RolloutState) hasExactVictim() bool {
	return s.VictimHolderID != "" && s.VictimPodUID != "" && s.VictimBootID != "" && s.VictimOrdinal >= 0
}

type RolloutControl interface {
	Observe(context.Context, core.WorkloadRef) (RolloutState, bool, error)
	ObserveRecovery(context.Context, core.WorkloadRef) (RolloutRecoveryRequest, bool, error)
	Begin(context.Context, core.WorkloadRef, core.WorkloadState, string) (RolloutState, error)
	Update(context.Context, core.WorkloadRef, RolloutState, RolloutState) (RolloutState, error)
	Recover(context.Context, core.WorkloadRef, RolloutState, RolloutRecoveryRequest, RolloutState) (RolloutState, error)
	EnsureDesiredReplicas(context.Context, core.WorkloadRef, RolloutState, int32) error
	DeleteVictimPod(context.Context, core.WorkloadRef, RolloutState) (bool, error)
	Complete(context.Context, core.WorkloadRef, RolloutState) error
}

func resumeBlockedRollout(state RolloutState) (RolloutState, error) {
	if err := state.Validate(); err != nil || state.Phase != RolloutPhaseBlocked {
		return RolloutState{}, fmt.Errorf("%w: blocked rollout resume", ErrInvalidTopology)
	}
	next := state
	next.Phase = state.BlockedFromPhase
	next.BlockedReason = ""
	next.BlockedFromPhase = ""
	next.BlockedAtMs = 0
	if err := next.Validate(); err != nil {
		return RolloutState{}, err
	}
	return next, nil
}

// ResumeBlockedRolloutWithDeadline performs the only legal recovery that may
// extend a blocked rollout's deadline.  The extension is an explicit,
// operator-authenticated command carrying the exact rollout identity; ordinary
// reconciliation and ordinary checkpoint updates keep DeadlineAtMs immutable.
// A recovery may not shorten the deadline, and it must remain in the future.
func ResumeBlockedRolloutWithDeadline(state RolloutState, deadlineAtMs int64) (RolloutState, error) {
	if err := state.Validate(); err != nil || state.Phase != RolloutPhaseBlocked {
		return RolloutState{}, fmt.Errorf("%w: blocked rollout resume", ErrInvalidTopology)
	}
	if deadlineAtMs < state.DeadlineAtMs || deadlineAtMs <= time.Now().UnixMilli() {
		return RolloutState{}, fmt.Errorf("%w: rollout recovery deadline", ErrInvalidTopology)
	}
	resumed, err := resumeBlockedRollout(state)
	if err != nil {
		return RolloutState{}, err
	}
	resumed.DeadlineAtMs = deadlineAtMs
	if err := resumed.Validate(); err != nil {
		return RolloutState{}, err
	}
	return resumed, nil
}

func abortBlockedRollout(state RolloutState) (RolloutState, error) {
	if err := state.Validate(); err != nil || state.Phase != RolloutPhaseBlocked ||
		(state.BlockedFromPhase != RolloutPhaseExpanding && state.BlockedFromPhase != RolloutPhaseDraining) ||
		state.VictimHolderID != "" || state.VictimPodUID != "" || state.VictimBootID != "" ||
		state.DrainPlanID != "" || state.ExclusionRevision != 0 {
		return RolloutState{}, fmt.Errorf("%w: rollout can only abort before the first old Pod drain", ErrInvalidTopology)
	}
	next := clearVictim(state, RolloutPhaseAborting)
	if err := next.Validate(); err != nil {
		return RolloutState{}, err
	}
	return next, nil
}

// ValidateRolloutRecoveryUpdate is deliberately separate from
// ValidateRolloutUpdate: ordinary reconciliation may never leave BLOCKED.
// Only an exact, explicit recovery command is allowed to cross that boundary.
func ValidateRolloutRecoveryUpdate(current, next RolloutState, action RolloutRecoveryAction) error {
	var expected RolloutState
	var err error
	switch action {
	case RolloutRecoveryResume:
		expected, err = resumeBlockedRollout(current)
	case RolloutRecoveryAbort:
		expected, err = abortBlockedRollout(current)
	default:
		return fmt.Errorf("%w: rollout recovery action", ErrInvalidTopology)
	}
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, next) {
		return fmt.Errorf("%w: rollout recovery changed checkpoint", ErrInvalidTopology)
	}
	return nil
}

func clearVictim(state RolloutState, phase RolloutPhase) RolloutState {
	state.Phase = phase
	state.VictimHolderID = ""
	state.VictimPodUID = ""
	state.VictimBootID = ""
	state.VictimOrdinal = -1
	state.DrainPlanID = ""
	state.ExclusionRevision = 0
	state.BlockedReason = ""
	state.BlockedFromPhase = ""
	state.BlockedAtMs = 0
	return state
}

func blockRollout(state RolloutState, reason string) RolloutState {
	state.BlockedFromPhase = state.Phase
	state.BlockedAtMs = time.Now().UnixMilli()
	state.Phase = RolloutPhaseBlocked
	state.BlockedReason = reason
	return state
}

func validSHA256Digest(value string) bool {
	return strings.HasPrefix(value, "sha256:") && validHexDigest(strings.TrimPrefix(value, "sha256:"))
}

func validHexDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, ch := range value {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}
