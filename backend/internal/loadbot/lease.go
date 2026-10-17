package loadbot

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const WorkerLeaseSchemaVersion = "mixed-farm-bot/worker-lease/v1"

var (
	ErrAssignmentExists    = errors.New("worker assignment already exists")
	ErrAssignmentNotFound  = errors.New("worker assignment not found")
	ErrLeaseExpired        = errors.New("worker lease expired")
	ErrLeaseFenced         = errors.New("worker lease fenced")
	ErrInvalidLeaseRequest = errors.New("invalid worker lease request")
	ErrLeaseTimeRollback   = errors.New("worker lease time moved backwards")
)

type WorkerAssignmentState string

const (
	WorkerAssignmentActive   WorkerAssignmentState = "ACTIVE"
	WorkerAssignmentExpired  WorkerAssignmentState = "EXPIRED"
	WorkerAssignmentReleased WorkerAssignmentState = "RELEASED"
)

type WorkerLeaseState string

const (
	WorkerLeaseActive   WorkerLeaseState = "ACTIVE"
	WorkerLeaseExpired  WorkerLeaseState = "EXPIRED"
	WorkerLeaseReleased WorkerLeaseState = "RELEASED"
)

type WorkerLease struct {
	SchemaVersion string           `json:"schema_version"`
	LeaseID       string           `json:"lease_id"`
	AssignmentID  string           `json:"assignment_id"`
	WorkerID      string           `json:"worker_id"`
	Epoch         uint64           `json:"epoch"`
	State         WorkerLeaseState `json:"state"`
	IssuedAt      time.Time        `json:"issued_at"`
	ExpiresAt     time.Time        `json:"expires_at"`
}

type WorkerAssignment struct {
	SchemaVersion string                `json:"schema_version"`
	RunID         string                `json:"run_id"`
	AssignmentID  string                `json:"assignment_id"`
	BotOrdinal    int                   `json:"bot_ordinal"`
	WorkerID      string                `json:"worker_id"`
	State         WorkerAssignmentState `json:"state"`
	Lease         WorkerLease           `json:"lease"`
	AssignedAt    time.Time             `json:"assigned_at"`
	UpdatedAt     time.Time             `json:"updated_at"`
}

func (assignment WorkerAssignment) Validate() error {
	if assignment.SchemaVersion != WorkerLeaseSchemaVersion {
		return fmt.Errorf("worker assignment schema_version must be %q", WorkerLeaseSchemaVersion)
	}
	if err := validateScopedIdentifier("run_id", assignment.RunID); err != nil {
		return err
	}
	if err := validateScopedIdentifier("assignment_id", assignment.AssignmentID); err != nil {
		return err
	}
	if assignment.BotOrdinal <= 0 {
		return errors.New("bot_ordinal must be > 0")
	}
	if err := validateScopedIdentifier("worker_id", assignment.WorkerID); err != nil {
		return err
	}
	if assignment.AssignedAt.IsZero() || assignment.UpdatedAt.IsZero() || assignment.UpdatedAt.Before(assignment.AssignedAt) {
		return errors.New("worker assignment timestamps are invalid")
	}
	switch assignment.State {
	case WorkerAssignmentActive, WorkerAssignmentExpired, WorkerAssignmentReleased:
	default:
		return fmt.Errorf("unknown worker assignment state %q", assignment.State)
	}
	if err := assignment.Lease.Validate(); err != nil {
		return err
	}
	if assignment.Lease.AssignmentID != assignment.AssignmentID || assignment.Lease.WorkerID != assignment.WorkerID {
		return errors.New("worker lease does not match assignment")
	}
	if assignment.Lease.IssuedAt.Before(assignment.AssignedAt) ||
		assignment.UpdatedAt.Before(assignment.Lease.IssuedAt) {
		return errors.New("worker assignment and lease times are not monotonic")
	}
	if assignment.State == WorkerAssignmentActive && assignment.Lease.State != WorkerLeaseActive {
		return errors.New("active assignment requires an active lease")
	}
	if assignment.State == WorkerAssignmentExpired && assignment.Lease.State != WorkerLeaseExpired {
		return errors.New("expired assignment requires an expired lease")
	}
	if assignment.State == WorkerAssignmentReleased && assignment.Lease.State != WorkerLeaseReleased {
		return errors.New("released assignment requires a released lease")
	}
	return nil
}

func (lease WorkerLease) Validate() error {
	if lease.SchemaVersion != WorkerLeaseSchemaVersion {
		return fmt.Errorf("worker lease schema_version must be %q", WorkerLeaseSchemaVersion)
	}
	if err := validateScopedIdentifier("lease_id", lease.LeaseID); err != nil {
		return err
	}
	if err := validateScopedIdentifier("assignment_id", lease.AssignmentID); err != nil {
		return err
	}
	if err := validateScopedIdentifier("worker_id", lease.WorkerID); err != nil {
		return err
	}
	if lease.Epoch == 0 {
		return errors.New("worker lease epoch must be > 0")
	}
	if lease.IssuedAt.IsZero() || lease.ExpiresAt.IsZero() || !lease.ExpiresAt.After(lease.IssuedAt) {
		return errors.New("worker lease timestamps are invalid")
	}
	switch lease.State {
	case WorkerLeaseActive, WorkerLeaseExpired, WorkerLeaseReleased:
		return nil
	default:
		return fmt.Errorf("unknown worker lease state %q", lease.State)
	}
}

func (assignment WorkerAssignment) LeaseValidAt(at time.Time) bool {
	return assignment.State == WorkerAssignmentActive &&
		assignment.Lease.State == WorkerLeaseActive &&
		!at.IsZero() && at.Before(assignment.Lease.ExpiresAt)
}

type WorkerAssignmentModel struct {
	runID    string
	leaseTTL time.Duration
	byBot    map[int]WorkerAssignment
}

func NewWorkerAssignmentModel(runID string, leaseTTL time.Duration) (*WorkerAssignmentModel, error) {
	if err := validateScopedIdentifier("run_id", runID); err != nil {
		return nil, err
	}
	if leaseTTL <= 0 || leaseTTL > 24*time.Hour {
		return nil, errors.New("lease TTL must be > 0 and <= 24h")
	}
	return &WorkerAssignmentModel{
		runID: strings.TrimSpace(runID), leaseTTL: leaseTTL,
		byBot: make(map[int]WorkerAssignment),
	}, nil
}

func (model *WorkerAssignmentModel) RunID() string {
	if model == nil {
		return ""
	}
	return model.runID
}

func (model *WorkerAssignmentModel) LeaseTTL() time.Duration {
	if model == nil {
		return 0
	}
	return model.leaseTTL
}

func (model *WorkerAssignmentModel) Assign(workerID string, botOrdinal int, at time.Time) (WorkerAssignment, error) {
	if model == nil {
		return WorkerAssignment{}, ErrInvalidLeaseRequest
	}
	if err := validateScopedIdentifier("worker_id", workerID); err != nil || botOrdinal <= 0 || at.IsZero() {
		return WorkerAssignment{}, ErrInvalidLeaseRequest
	}
	at = at.UTC()
	if current, exists := model.byBot[botOrdinal]; exists {
		if at.Before(current.UpdatedAt) {
			return WorkerAssignment{}, fmt.Errorf("%w: assignment %q", ErrLeaseTimeRollback, current.AssignmentID)
		}
		if current.LeaseValidAt(at) {
			return WorkerAssignment{}, fmt.Errorf("%w: bot %d is leased to worker %q", ErrAssignmentExists, botOrdinal, current.WorkerID)
		}
		if current.State == WorkerAssignmentActive {
			current.State = WorkerAssignmentExpired
			current.Lease.State = WorkerLeaseExpired
			current.UpdatedAt = at
			model.byBot[botOrdinal] = current
		}
	}
	epoch := uint64(1)
	if current, exists := model.byBot[botOrdinal]; exists {
		epoch = current.Lease.Epoch + 1
	}
	assignmentID := fmt.Sprintf("%s-bot-%d", slugIdentifier(model.runID), botOrdinal)
	leaseID := assignmentID + "-lease-" + strconv.FormatUint(epoch, 10)
	lease := WorkerLease{
		SchemaVersion: WorkerLeaseSchemaVersion, LeaseID: leaseID,
		AssignmentID: assignmentID, WorkerID: workerID, Epoch: epoch,
		State: WorkerLeaseActive, IssuedAt: at, ExpiresAt: at.Add(model.leaseTTL),
	}
	assignment := WorkerAssignment{
		SchemaVersion: WorkerLeaseSchemaVersion, RunID: model.runID,
		AssignmentID: assignmentID, BotOrdinal: botOrdinal, WorkerID: workerID,
		State: WorkerAssignmentActive, Lease: lease, AssignedAt: at, UpdatedAt: at,
	}
	model.byBot[botOrdinal] = assignment
	return assignment, nil
}

func (model *WorkerAssignmentModel) Renew(assignmentID, workerID string, epoch uint64, at time.Time) (WorkerAssignment, error) {
	assignment, err := model.lookup(assignmentID)
	if err != nil {
		return WorkerAssignment{}, err
	}
	if epoch == 0 || assignment.Lease.Epoch != epoch || assignment.WorkerID != workerID {
		return WorkerAssignment{}, fmt.Errorf("%w: assignment %q", ErrLeaseFenced, assignmentID)
	}
	if assignment.State != WorkerAssignmentActive || assignment.Lease.State != WorkerLeaseActive {
		return WorkerAssignment{}, ErrLeaseExpired
	}
	if at.IsZero() {
		return WorkerAssignment{}, ErrInvalidLeaseRequest
	}
	at = at.UTC()
	if at.Before(assignment.UpdatedAt) || at.Before(assignment.Lease.IssuedAt) {
		return WorkerAssignment{}, fmt.Errorf("%w: assignment %q", ErrLeaseTimeRollback, assignmentID)
	}
	if !at.Before(assignment.Lease.ExpiresAt) {
		assignment.State = WorkerAssignmentExpired
		assignment.Lease.State = WorkerLeaseExpired
		assignment.UpdatedAt = at
		model.byBot[assignment.BotOrdinal] = assignment
		return WorkerAssignment{}, ErrLeaseExpired
	}
	assignment.Lease.IssuedAt = at
	assignment.Lease.ExpiresAt = at.Add(model.leaseTTL)
	assignment.UpdatedAt = at
	model.byBot[assignment.BotOrdinal] = assignment
	return assignment, nil
}

func (model *WorkerAssignmentModel) Release(assignmentID, workerID string, epoch uint64, at time.Time) error {
	assignment, err := model.lookup(assignmentID)
	if err != nil {
		return err
	}
	if epoch == 0 || assignment.Lease.Epoch != epoch || assignment.WorkerID != workerID {
		return fmt.Errorf("%w: assignment %q", ErrLeaseFenced, assignmentID)
	}
	if at.IsZero() {
		return ErrInvalidLeaseRequest
	}
	if assignment.State != WorkerAssignmentActive || assignment.Lease.State != WorkerLeaseActive {
		return ErrLeaseExpired
	}
	at = at.UTC()
	if at.Before(assignment.UpdatedAt) || at.Before(assignment.Lease.IssuedAt) {
		return fmt.Errorf("%w: assignment %q", ErrLeaseTimeRollback, assignmentID)
	}
	assignment.State = WorkerAssignmentReleased
	assignment.Lease.State = WorkerLeaseReleased
	assignment.UpdatedAt = at
	model.byBot[assignment.BotOrdinal] = assignment
	return nil
}

func (model *WorkerAssignmentModel) ReapExpired(at time.Time) []WorkerAssignment {
	if model == nil || at.IsZero() {
		return nil
	}
	at = at.UTC()
	expired := make([]WorkerAssignment, 0)
	for botOrdinal, assignment := range model.byBot {
		if assignment.State == WorkerAssignmentActive && !at.Before(assignment.Lease.ExpiresAt) {
			assignment.State = WorkerAssignmentExpired
			assignment.Lease.State = WorkerLeaseExpired
			assignment.UpdatedAt = at
			model.byBot[botOrdinal] = assignment
			expired = append(expired, assignment)
		}
	}
	sort.Slice(expired, func(left, right int) bool { return expired[left].BotOrdinal < expired[right].BotOrdinal })
	return expired
}

func (model *WorkerAssignmentModel) Get(botOrdinal int) (WorkerAssignment, bool) {
	if model == nil {
		return WorkerAssignment{}, false
	}
	assignment, ok := model.byBot[botOrdinal]
	return assignment, ok
}

func (model *WorkerAssignmentModel) List() []WorkerAssignment {
	if model == nil {
		return nil
	}
	result := make([]WorkerAssignment, 0, len(model.byBot))
	for _, assignment := range model.byBot {
		result = append(result, assignment)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].BotOrdinal < result[right].BotOrdinal })
	return result
}

func (model *WorkerAssignmentModel) lookup(assignmentID string) (WorkerAssignment, error) {
	if model == nil || strings.TrimSpace(assignmentID) == "" {
		return WorkerAssignment{}, ErrAssignmentNotFound
	}
	for _, assignment := range model.byBot {
		if assignment.AssignmentID == assignmentID {
			return assignment, nil
		}
	}
	return WorkerAssignment{}, fmt.Errorf("%w: %s", ErrAssignmentNotFound, assignmentID)
}
