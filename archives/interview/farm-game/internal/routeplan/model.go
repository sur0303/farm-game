// Package routeplan contains the internal manual route-migration record and
// its single-controller execution seam. A Plan is an audit/recovery log; it
// is never an Authority or RouteSnapshot source of truth.
package routeplan

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"farmgame/internal/actorrouting"
	"farmgame/internal/holderidentity"
	"farmgame/internal/netendpoint"
	"farmgame/internal/storage/authoritystore"
)

const (
	MaxPlanIDBytes       = 128
	MaxTransitionIDBytes = 128
	MaxHolderIDBytes     = 128
	MaxEndpointBytes     = 512
	MaxErrorCodeBytes    = 128
	MaxEvidenceBytes     = 2048
	MaxPlanShard         = ^uint32(0)
	MaxRequestedTTL      = 24 * time.Hour
	MaxRequestedTTLms    = int64(MaxRequestedTTL / time.Millisecond)
)

var (
	ErrInvalidPlan     = errors.New("routeplan: invalid plan")
	ErrPlanConflict    = errors.New("routeplan: plan revision conflict")
	ErrPlanCapacity    = errors.New("routeplan: plan capacity exceeded")
	ErrPlanCorrupt     = errors.New("routeplan: corrupt plan")
	ErrPlanNotFound    = errors.New("routeplan: plan not found")
	ErrPlanPhase       = errors.New("routeplan: invalid plan phase transition")
	ErrExecutorInvalid = errors.New("routeplan: invalid executor")
	ErrShardBusy       = errors.New("routeplan: shard already has an in-flight plan")
	ErrTakeoverProof   = errors.New("routeplan: crash takeover proof is insufficient")
	ErrVerification    = errors.New("routeplan: verification failed")
	ErrPlanPaused      = errors.New("routeplan: plan is paused")
	ErrPlanFailed      = errors.New("routeplan: plan failed")
)

// Phase is the monotonic Plan progress marker. FAILED and PAUSED are explicit
// terminal outcomes; no implicit retry changes a terminal plan.
type Phase uint8

const (
	PhaseUnspecified Phase = iota
	PhasePending
	PhaseDraining
	PhaseAssigning
	PhasePreparing
	PhaseActive
	PhaseSucceeded
	PhaseFailed
	PhasePaused
)

func (p Phase) String() string {
	switch p {
	case PhasePending:
		return "PENDING"
	case PhaseDraining:
		return "DRAINING"
	case PhaseAssigning:
		return "ASSIGNING"
	case PhasePreparing:
		return "PREPARING"
	case PhaseActive:
		return "ACTIVE"
	case PhaseSucceeded:
		return "SUCCEEDED"
	case PhaseFailed:
		return "FAILED"
	case PhasePaused:
		return "PAUSED"
	default:
		return "UNSPECIFIED"
	}
}

func ParsePhase(value string) (Phase, error) {
	switch value {
	case "PENDING":
		return PhasePending, nil
	case "DRAINING":
		return PhaseDraining, nil
	case "ASSIGNING":
		return PhaseAssigning, nil
	case "PREPARING":
		return PhasePreparing, nil
	case "ACTIVE":
		return PhaseActive, nil
	case "SUCCEEDED":
		return PhaseSucceeded, nil
	case "FAILED":
		return PhaseFailed, nil
	case "PAUSED":
		return PhasePaused, nil
	default:
		return PhaseUnspecified, fmt.Errorf("%w: unknown phase %q", ErrInvalidPlan, value)
	}
}

func (p Phase) Terminal() bool {
	return p == PhaseSucceeded || p == PhaseFailed || p == PhasePaused
}

// OperationKind is deliberately not a generic Transfer mode. The executor
// chooses the Manager operation from this value and never computes epochs.
type OperationKind string

const (
	OperationPlannedDrain  OperationKind = "planned-drain"
	OperationCrashTakeover OperationKind = "crash-takeover"
)

// TakeoverProof is the operator-provided failure evidence accepted only for a
// crash takeover. Evidence is durable audit text, never a credential.
type TakeoverProof struct {
	ExpectedHolderBootID string
	HolderUnavailable    bool
	LeaseExpired         bool
	Evidence             string
}

// Plan is the complete durable manual migration record. The generation
// fields are copied observations; Manager remains the only generation writer.
type Plan struct {
	PlanID       string
	TransitionID string
	Domain       actorrouting.RouteDomain
	VirtualShard uint32

	SourceHolderID           string
	SourceBootID             string
	SourceDeploymentIdentity holderidentity.DeploymentIdentity
	SourceEndpoint           string
	TargetHolderID           string
	TargetBootID             string
	TargetDeploymentIdentity holderidentity.DeploymentIdentity
	TargetEndpoint           string

	ExpectedSourceFence authoritystore.ShardWriteFence
	RequestedTTLms      int64
	Operation           OperationKind
	Phase               Phase
	Attempt             uint64
	Revision            uint64
	CreatedAtMs         int64
	UpdatedAtMs         int64
	StartedAtMs         int64
	CompletedAtMs       int64
	ExpiresAtMs         int64
	OwnerScalePlanID    string
	OwnerRetainUntilMs  int64
	OwnerWaveID         string
	OwnerWaveSize       uint32
	OwnerWavePosition   uint32
	ErrorCode           string
	TakeoverProof       TakeoverProof
}

func (p Plan) Clone() Plan { return p }

func (p Plan) Validate() error {
	if err := ValidatePlanID(p.PlanID); err != nil {
		return err
	}
	if err := validateOpaque("transition_id", p.TransitionID, MaxTransitionIDBytes); err != nil {
		return err
	}
	if p.Domain != actorrouting.DomainPlayer &&
		p.Domain != actorrouting.DomainFarm &&
		p.Domain != actorrouting.DomainFarmWorker {
		return fmt.Errorf("%w: domain", ErrInvalidPlan)
	}
	if err := validateOpaque("source_holder_id", p.SourceHolderID, MaxHolderIDBytes); err != nil {
		return err
	}
	if err := validateOpaque("source_boot_id", p.SourceBootID, MaxHolderIDBytes); err != nil {
		return err
	}
	if err := p.SourceDeploymentIdentity.ValidateOptional(); err != nil {
		return fmt.Errorf("%w: source deployment identity: %v", ErrInvalidPlan, err)
	}
	if err := netendpoint.Validate(p.SourceEndpoint, MaxEndpointBytes); err != nil {
		return fmt.Errorf("%w: source_endpoint must be canonical host:port", ErrInvalidPlan)
	}
	if err := validateOpaque("target_holder_id", p.TargetHolderID, MaxHolderIDBytes); err != nil {
		return err
	}
	if err := validateOpaque("target_boot_id", p.TargetBootID, MaxHolderIDBytes); err != nil {
		return err
	}
	if err := p.TargetDeploymentIdentity.ValidateOptional(); err != nil {
		return fmt.Errorf("%w: target deployment identity: %v", ErrInvalidPlan, err)
	}
	if err := netendpoint.Validate(p.TargetEndpoint, MaxEndpointBytes); err != nil {
		return fmt.Errorf("%w: target_endpoint must be canonical host:port", ErrInvalidPlan)
	}
	if p.SourceHolderID == p.TargetHolderID && p.SourceBootID == p.TargetBootID {
		return fmt.Errorf("%w: source and target generation are identical", ErrInvalidPlan)
	}
	if p.ExpectedSourceFence.ExpectedHolderID != p.SourceHolderID ||
		p.ExpectedSourceFence.ExpectedHolderBootID != p.SourceBootID ||
		p.ExpectedSourceFence.ExpectedAssignmentRevision == 0 ||
		p.ExpectedSourceFence.ExpectedWriterEpoch == 0 ||
		p.ExpectedSourceFence.ExpectedLeaseEpoch == 0 {
		return fmt.Errorf("%w: complete expected source fence required", ErrInvalidPlan)
	}
	if p.RequestedTTLms <= 0 || p.RequestedTTLms > MaxRequestedTTLms {
		return fmt.Errorf("%w: requested ttl", ErrInvalidPlan)
	}
	switch p.Operation {
	case OperationPlannedDrain:
		if p.TakeoverProof != (TakeoverProof{}) {
			return fmt.Errorf("%w: planned drain cannot carry takeover proof", ErrInvalidPlan)
		}
	case OperationCrashTakeover:
		if err := p.TakeoverProof.Validate(p.SourceBootID); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: operation", ErrInvalidPlan)
	}
	if p.Phase < PhasePending || p.Phase > PhasePaused {
		return fmt.Errorf("%w: phase", ErrInvalidPlan)
	}
	if p.Revision == 0 {
		return fmt.Errorf("%w: revision", ErrInvalidPlan)
	}
	if p.CreatedAtMs <= 0 || p.UpdatedAtMs < p.CreatedAtMs || p.ExpiresAtMs <= p.CreatedAtMs {
		return fmt.Errorf("%w: plan timestamps", ErrInvalidPlan)
	}
	if p.OwnerScalePlanID == "" {
		if p.OwnerRetainUntilMs != 0 || !p.SourceDeploymentIdentity.IsZero() ||
			(!p.TargetDeploymentIdentity.IsZero() && p.Operation != OperationCrashTakeover) {
			return fmt.Errorf("%w: exact deployment identity requires ScalePlan owner retention", ErrInvalidPlan)
		}
	} else {
		if err := validateOpaque("owner_scale_plan_id", p.OwnerScalePlanID, MaxPlanIDBytes); err != nil {
			return err
		}
		if p.Operation != OperationPlannedDrain || !p.SourceDeploymentIdentity.Exact() ||
			!p.TargetDeploymentIdentity.Exact() || p.OwnerRetainUntilMs <= p.CreatedAtMs {
			return fmt.Errorf("%w: complete ScalePlan owner identity/retention required", ErrInvalidPlan)
		}
	}
	if p.OwnerWaveID == "" {
		if p.OwnerWaveSize != 0 || p.OwnerWavePosition != 0 {
			return fmt.Errorf("%w: partial owner wave identity", ErrInvalidPlan)
		}
	} else {
		if p.OwnerScalePlanID == "" || p.OwnerWaveSize == 0 || p.OwnerWaveSize > 32 || p.OwnerWavePosition >= p.OwnerWaveSize {
			return fmt.Errorf("%w: owner wave bounds", ErrInvalidPlan)
		}
		if err := validateOpaque("owner_wave_id", p.OwnerWaveID, MaxPlanIDBytes); err != nil {
			return err
		}
	}
	if !p.Phase.Terminal() && p.ExpiresAtMs <= p.UpdatedAtMs {
		return fmt.Errorf("%w: non-terminal plan expired before audit update", ErrInvalidPlan)
	}
	if p.StartedAtMs != 0 && (p.StartedAtMs < p.CreatedAtMs || p.StartedAtMs > p.UpdatedAtMs) {
		return fmt.Errorf("%w: started timestamp", ErrInvalidPlan)
	}
	if p.CompletedAtMs != 0 && (p.CompletedAtMs < p.CreatedAtMs || p.CompletedAtMs < p.UpdatedAtMs) {
		return fmt.Errorf("%w: completed timestamp", ErrInvalidPlan)
	}
	if p.Phase.Terminal() && p.CompletedAtMs == 0 {
		return fmt.Errorf("%w: terminal plan requires completed timestamp", ErrInvalidPlan)
	}
	if !p.Phase.Terminal() && (p.CompletedAtMs != 0 || p.ErrorCode != "") {
		return fmt.Errorf("%w: non-terminal audit fields", ErrInvalidPlan)
	}
	if p.Phase == PhaseSucceeded && p.ErrorCode != "" {
		return fmt.Errorf("%w: succeeded plan error code", ErrInvalidPlan)
	}
	if (p.Phase == PhaseFailed || p.Phase == PhasePaused) && p.ErrorCode == "" {
		return fmt.Errorf("%w: terminal error code required", ErrInvalidPlan)
	}
	if p.ErrorCode != "" {
		if err := validateOpaque("error_code", p.ErrorCode, MaxErrorCodeBytes); err != nil {
			return err
		}
	}
	return nil
}

func (p TakeoverProof) Validate(expectedBoot string) error {
	if err := validateOpaque("proof holder boot", p.ExpectedHolderBootID, MaxHolderIDBytes); err != nil {
		return err
	}
	if p.ExpectedHolderBootID != expectedBoot || (!p.HolderUnavailable && !p.LeaseExpired) {
		return ErrTakeoverProof
	}
	if err := validateOpaque("proof evidence", p.Evidence, MaxEvidenceBytes); err != nil {
		return err
	}
	return nil
}

// CanAdvancePhase enforces the durable monotonic phase contract. It permits a
// retry to update attempt/timestamps without changing the phase.
func CanAdvancePhase(from, to Phase) bool {
	if from == to {
		return true
	}
	// A transiently paused wave member (CANCELED/TIMEOUT) may be retried by
	// returning to PENDING. This is the only exit from the terminal Paused
	// state and is restricted to the wave recovery path.
	if from == PhasePaused {
		return to == PhasePending
	}
	if from.Terminal() {
		return false
	}
	if to == PhaseFailed || to == PhasePaused {
		return true
	}
	switch from {
	case PhasePending:
		return to == PhaseDraining || to == PhaseAssigning
	case PhaseDraining:
		return to == PhaseAssigning
	case PhaseAssigning:
		return to == PhasePreparing || to == PhaseActive
	case PhasePreparing:
		return to == PhaseActive
	case PhaseActive:
		return to == PhaseSucceeded
	default:
		return false
	}
}

func ValidatePhaseAdvance(from, to Phase) error {
	if !CanAdvancePhase(from, to) {
		return fmt.Errorf("%w: %s -> %s", ErrPlanPhase, from, to)
	}
	return nil
}

// ValidatePlanUpdate freezes the operator submission and permits only a
// monotonic audit progression. Stores call this immediately before committing
// a non-replay CAS so callers cannot rewrite a Plan in place.
func ValidatePlanUpdate(current, candidate Plan) error {
	if current.PlanID != candidate.PlanID || current.TransitionID != candidate.TransitionID ||
		current.Domain != candidate.Domain || current.VirtualShard != candidate.VirtualShard ||
		current.SourceHolderID != candidate.SourceHolderID || current.SourceBootID != candidate.SourceBootID ||
		current.SourceDeploymentIdentity != candidate.SourceDeploymentIdentity || current.SourceEndpoint != candidate.SourceEndpoint ||
		current.TargetHolderID != candidate.TargetHolderID || current.TargetBootID != candidate.TargetBootID ||
		current.TargetDeploymentIdentity != candidate.TargetDeploymentIdentity || current.TargetEndpoint != candidate.TargetEndpoint ||
		current.ExpectedSourceFence != candidate.ExpectedSourceFence || current.RequestedTTLms != candidate.RequestedTTLms ||
		current.Operation != candidate.Operation || current.CreatedAtMs != candidate.CreatedAtMs ||
		current.ExpiresAtMs != candidate.ExpiresAtMs || current.OwnerScalePlanID != candidate.OwnerScalePlanID ||
		current.OwnerRetainUntilMs != candidate.OwnerRetainUntilMs || current.OwnerWaveID != candidate.OwnerWaveID ||
		current.OwnerWaveSize != candidate.OwnerWaveSize || current.OwnerWavePosition != candidate.OwnerWavePosition ||
		current.TakeoverProof != candidate.TakeoverProof {
		return fmt.Errorf("%w: frozen plan fields changed", ErrPlanConflict)
	}
	if current.Phase.Terminal() && !(current.Phase == PhasePaused && candidate.Phase == PhasePending) {
		return fmt.Errorf("%w: terminal plan is immutable", ErrPlanConflict)
	}
	if candidate.UpdatedAtMs < current.UpdatedAtMs {
		return fmt.Errorf("%w: audit fields regressed", ErrPlanConflict)
	}
	if candidate.Attempt != current.Attempt {
		if current.Attempt == ^uint64(0) || candidate.Attempt != current.Attempt+1 {
			return fmt.Errorf("%w: attempt must stay equal or advance exactly once", ErrPlanConflict)
		}
	}
	if current.StartedAtMs != 0 && candidate.StartedAtMs != current.StartedAtMs {
		return fmt.Errorf("%w: started timestamp changed", ErrPlanConflict)
	}
	if current.StartedAtMs == 0 && candidate.StartedAtMs != 0 &&
		(candidate.StartedAtMs < current.CreatedAtMs || candidate.StartedAtMs > candidate.UpdatedAtMs) {
		return fmt.Errorf("%w: started timestamp progression", ErrPlanConflict)
	}
	if err := ValidatePhaseAdvance(current.Phase, candidate.Phase); err != nil {
		return err
	}
	if candidate.Phase.Terminal() {
		if candidate.CompletedAtMs != candidate.UpdatedAtMs {
			return fmt.Errorf("%w: terminal timestamp progression", ErrPlanConflict)
		}
	} else if candidate.CompletedAtMs != 0 || candidate.ErrorCode != "" {
		return fmt.Errorf("%w: non-terminal audit progression", ErrPlanConflict)
	}
	return nil
}

// ValidateInitialPlan is the first-CAS creation contract. A Plan cannot be
// inserted already in progress or terminal, nor can it fabricate an attempt
// or started/completed audit timestamp before Executor owns it.
func ValidateInitialPlan(plan Plan) error {
	if err := plan.Validate(); err != nil {
		return err
	}
	if plan.Phase != PhasePending || plan.Attempt != 0 || plan.StartedAtMs != 0 || plan.CompletedAtMs != 0 || plan.ErrorCode != "" {
		return fmt.Errorf("%w: initial plan audit state", ErrPlanConflict)
	}
	return nil
}

func validateOpaque(name, value string, max int) error {
	if value == "" || len([]byte(value)) > max || strings.TrimSpace(value) != value || !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s", ErrInvalidPlan, name)
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: %s contains control characters", ErrInvalidPlan, name)
		}
	}
	return nil
}

// ValidatePlanID is the public exact-key grammar shared by the service,
// stores, and local operator tooling. It deliberately excludes all whitespace
// and Unicode so every adapter has byte-identical acceptance semantics.
func ValidatePlanID(value string) error {
	if value == "" || len(value) > MaxPlanIDBytes {
		return fmt.Errorf("%w: plan_id", ErrInvalidPlan)
	}
	for index := 0; index < len(value); index++ {
		if value[index] < 0x21 || value[index] > 0x7e {
			return fmt.Errorf("%w: plan_id must contain visible ASCII without whitespace", ErrInvalidPlan)
		}
	}
	return nil
}
