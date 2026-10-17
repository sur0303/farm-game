package workflow

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"farmgame/internal/workidentity"
)

// WorkState is the durable lifecycle of a generic recoverable Work
// (independent Workflow or source-slotted Order). Terminal states are frozen
// by contract 27/29/30: once terminal a record is immutable and must leave
// the due index.
type WorkState uint8

const (
	WorkStateUnspecified WorkState = iota
	// WorkStateCreated: durable anchor exists, no step finished yet.
	WorkStateCreated
	// WorkStateRunning: at least one step finished, more steps remain.
	WorkStateRunning
	// WorkStateStepUnknown: the current step invocation was sent but its
	// outcome is unknown. The work must not advance past this step until the
	// same invocation is resolved through the participant receipt.
	WorkStateStepUnknown
	WorkStateCompleted   // terminal
	WorkStateCompensated // terminal: deterministic release/compensation branch done
	WorkStateRejected    // terminal: deterministic business rejection
	WorkStateSuperseded  // terminal: a newer fact made this work obsolete
	// WorkStateFailed is the dead-letter-like terminal: deadline exhausted or
	// a non-retryable internal fault. It requires operator attention and is
	// never retried silently.
	WorkStateFailed
)

func (s WorkState) String() string {
	switch s {
	case WorkStateCreated:
		return "CREATED"
	case WorkStateRunning:
		return "RUNNING"
	case WorkStateStepUnknown:
		return "STEP_UNKNOWN"
	case WorkStateCompleted:
		return "COMPLETED"
	case WorkStateCompensated:
		return "COMPENSATED"
	case WorkStateRejected:
		return "REJECTED"
	case WorkStateSuperseded:
		return "SUPERSEDED"
	case WorkStateFailed:
		return "FAILED"
	default:
		return "UNSPECIFIED"
	}
}

// WorkTerminal reports whether the state is immutable and out of the due index.
func WorkTerminal(s WorkState) bool {
	switch s {
	case WorkStateCompleted, WorkStateCompensated, WorkStateRejected, WorkStateSuperseded, WorkStateFailed:
		return true
	default:
		return false
	}
}

// StepStatus is the durable per-step receipt state.
type StepStatus uint8

const (
	StepStatusUnspecified StepStatus = iota
	StepPending
	// StepUnknown: invocation sent, outcome unknown. Replay of the same
	// invocation must hit the participant receipt before any advance.
	StepUnknown
	StepApplied
	StepRejected
	StepNoChange
)

func (s StepStatus) String() string {
	switch s {
	case StepPending:
		return "PENDING"
	case StepUnknown:
		return "UNKNOWN"
	case StepApplied:
		return "APPLIED"
	case StepRejected:
		return "REJECTED"
	case StepNoChange:
		return "NO_CHANGE"
	default:
		return "UNSPECIFIED"
	}
}

// StepTerminal reports whether the step has a durable terminal receipt.
func StepTerminal(s StepStatus) bool {
	switch s {
	case StepApplied, StepRejected, StepNoChange:
		return true
	default:
		return false
	}
}

// StepRecord is the durable receipt of one participant invocation. Payload
// and PayloadHash are frozen at first build; a resume that rebuilds a
// different payload for the same step must fail closed.
type StepRecord struct {
	Name                string   `json:"name"`
	InvocationID        string   `json:"invocation_id"`
	Payload             []byte   `json:"payload,omitempty"`
	PayloadHash         [32]byte `json:"payload_hash"`
	DependsOnResultHash [32]byte `json:"depends_on_result_hash"`
	Status              StepStatus `json:"status"`
	Result              []byte   `json:"result,omitempty"`
	ResultHash          [32]byte `json:"result_hash"`
	ErrorClass          string   `json:"error_class,omitempty"`
	Attempts            uint64   `json:"attempts"`
}

// WorkRecord is the single durable recovery anchor for a generic Workflow or
// Order. It mirrors the common control header frozen in contract 31 §9.2.
type WorkRecord struct {
	Address         workidentity.WorkAddress `json:"address"`
	SubjectPlayerID uint64                   `json:"subject_player_id"`
	OperationID     string                   `json:"operation_id"`
	Payload         []byte                   `json:"payload,omitempty"`
	PayloadHash     [32]byte                 `json:"payload_hash"`

	State       WorkState    `json:"state"`
	WorkVersion uint64       `json:"work_version"`
	CurrentStep int          `json:"current_step"`
	Steps       []StepRecord `json:"steps"`

	NextDueAtMs    int64  `json:"next_due_at_ms"`
	RetryCount     uint64 `json:"retry_count"`
	LastErrorClass string `json:"last_error_class,omitempty"`
	DeadlineAtMs   int64  `json:"deadline_at_ms"`

	LeaseOwner      string `json:"lease_owner,omitempty"`
	LeaseEpoch      uint64 `json:"lease_epoch,omitempty"`
	LeaseTokenHash  string `json:"lease_token_hash,omitempty"`
	LeaseExpireAtMs int64  `json:"lease_expire_at_ms,omitempty"`

	CreatedAtMs   int64 `json:"created_at_ms"`
	UpdatedAtMs   int64 `json:"updated_at_ms"`
	CompletedAtMs int64 `json:"completed_at_ms,omitempty"`
}

var (
	ErrStepDeficit     = errors.New("workflow: step records required")
	ErrStateIllegal    = errors.New("workflow: illegal state transition")
	ErrWorkRefMismatch = errors.New("workflow: work ref does not match record")
)

// NewWorkRecord builds the durable anchor with deterministic per-step
// invocation identities (contract 29 §6.2, step instance fixed to 1 in P0).
func NewWorkRecord(address workidentity.WorkAddress, subjectPlayerID uint64, operationID string,
	payload []byte, payloadHash [32]byte, stepNames []string, now, deadline time.Time) (*WorkRecord, error) {
	if len(stepNames) == 0 {
		return nil, ErrStepDeficit
	}
	steps := make([]StepRecord, len(stepNames))
	for i, name := range stepNames {
		invocationID, err := workidentity.ParticipantInvocationID(address.WorkType, address.WorkID, name, 1)
		if err != nil {
			return nil, err
		}
		steps[i] = StepRecord{Name: name, InvocationID: invocationID, Status: StepPending}
	}
	record := &WorkRecord{
		Address:         address,
		SubjectPlayerID: subjectPlayerID,
		OperationID:     operationID,
		Payload:         append([]byte(nil), payload...),
		PayloadHash:     payloadHash,
		State:           WorkStateCreated,
		Steps:           steps,
		NextDueAtMs:     now.UnixMilli(),
		CreatedAtMs:     now.UnixMilli(),
		UpdatedAtMs:     now.UnixMilli(),
	}
	if !deadline.IsZero() {
		record.DeadlineAtMs = deadline.UnixMilli()
	}
	if err := record.Validate(); err != nil {
		return nil, err
	}
	return record, nil
}

// Validate checks the durable shape shared by all Work families.
func (r *WorkRecord) Validate() error {
	if r == nil {
		return ErrInvalid
	}
	if err := r.Address.ValidateShape(); err != nil {
		return err
	}
	if r.SubjectPlayerID == 0 || strings.TrimSpace(r.OperationID) == "" || r.PayloadHash == ([32]byte{}) {
		return fmt.Errorf("%w: subject/operation/hash", ErrInvalid)
	}
	if r.State == WorkStateUnspecified || len(r.Steps) == 0 {
		return fmt.Errorf("%w: state/steps", ErrInvalid)
	}
	if r.CurrentStep < 0 || r.CurrentStep >= len(r.Steps) {
		return fmt.Errorf("%w: current step out of range", ErrInvalid)
	}
	for i := range r.Steps {
		step := &r.Steps[i]
		if !validStepName(step.Name) || workidentity.ValidateStableOpaqueID(step.InvocationID) != nil || step.Status == StepStatusUnspecified {
			return fmt.Errorf("%w: step %d", ErrInvalid, i)
		}
		want, err := workidentity.ParticipantInvocationID(r.Address.WorkType, r.Address.WorkID, step.Name, 1)
		if err != nil || want != step.InvocationID {
			return fmt.Errorf("%w: step %d invocation drift", ErrInvalid, i)
		}
	}
	return nil
}

func validStepName(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}

// Terminal reports whether the record reached an immutable end state.
func (r *WorkRecord) Terminal() bool { return WorkTerminal(r.State) }

// sameWorkIntent reports whether an existing record and a creation candidate
// describe the same immutable business intent.
func sameWorkIntent(existing, candidate *WorkRecord) bool {
	if existing == nil || candidate == nil {
		return false
	}
	if existing.Address != candidate.Address ||
		existing.SubjectPlayerID != candidate.SubjectPlayerID ||
		existing.OperationID != candidate.OperationID ||
		existing.PayloadHash != candidate.PayloadHash ||
		!bytes.Equal(existing.Payload, candidate.Payload) ||
		len(existing.Steps) != len(candidate.Steps) {
		return false
	}
	for i := range existing.Steps {
		if existing.Steps[i].Name != candidate.Steps[i].Name ||
			existing.Steps[i].InvocationID != candidate.Steps[i].InvocationID {
			return false
		}
	}
	return true
}

func cloneWorkRecord(record *WorkRecord) *WorkRecord {
	if record == nil {
		return nil
	}
	out := *record
	out.Payload = append([]byte(nil), record.Payload...)
	out.Steps = make([]StepRecord, len(record.Steps))
	for i, step := range record.Steps {
		out.Steps[i] = step
		out.Steps[i].Payload = append([]byte(nil), step.Payload...)
		out.Steps[i].Result = append([]byte(nil), step.Result...)
	}
	return &out
}

// encodeWorkRecord serializes the durable record for the Redis family. The
// encoding is storage-only; identity and idempotency never parse these bytes.
func encodeWorkRecord(record *WorkRecord) ([]byte, error) {
	return json.Marshal(record)
}

func decodeWorkRecord(data []byte) (*WorkRecord, error) {
	var record WorkRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("workflow: decode work record: %w", err)
	}
	if err := record.Validate(); err != nil {
		return nil, err
	}
	return &record, nil
}

// sortDue orders records by (NextDueAtMs, WorkID) for deterministic claims.
func sortDue(records []*WorkRecord) {
	sort.Slice(records, func(i, j int) bool {
		if records[i].NextDueAtMs == records[j].NextDueAtMs {
			return records[i].Address.WorkID < records[j].Address.WorkID
		}
		return records[i].NextDueAtMs < records[j].NextDueAtMs
	})
}

func leaseTokenHash(token string) string {
	sum := sha256.Sum256([]byte("wf-lease-token\x00" + token))
	return fmt.Sprintf("%x", sum)
}
