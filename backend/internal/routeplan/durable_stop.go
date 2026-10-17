package routeplan

import (
	"context"
	"fmt"

	"farmgame/internal/controlfence"
)

// DurableStopErrorCode is the only error code a scale-driven stop may write.
// Fixing it means an operator reading a stopped child can tell that ScalePlan
// stopped it rather than that it failed on its own.
const DurableStopErrorCode = "SCALEPLAN_OPERATOR_SAFE_STOP"

type DurableStopStatus string

const (
	// DurableStopStopped means this call moved the child to PAUSED.
	DurableStopStopped DurableStopStatus = "STOPPED"
	// DurableStopAlreadyStopped is the exact replay of a previous stop.
	DurableStopAlreadyStopped DurableStopStatus = "ALREADY_STOPPED"
	// DurableStopAlreadyTerminal means the child reached a terminal phase on
	// its own. That is a safe outcome, but not one this call produced.
	DurableStopAlreadyTerminal  DurableStopStatus = "ALREADY_TERMINAL"
	DurableStopRevisionConflict DurableStopStatus = "REVISION_CONFLICT"
	DurableStopNotOwned         DurableStopStatus = "NOT_OWNED"
	DurableStopNotFound         DurableStopStatus = "NOT_FOUND"
	// DurableStopUnsafe means the outcome is unknown. The parent plan must
	// stay BLOCKED; it must not infer that nothing happened.
	DurableStopUnsafe DurableStopStatus = "UNSAFE"
)

// DurableStopCommand names the child exactly. Every field is an expectation
// rather than an instruction: a mismatch is a conflict, never a coercion.
type DurableStopCommand struct {
	OwnerScalePlanID string
	PlanID           string
	ExpectedRevision uint64
	ExpectedPhase    Phase
	NowMs            int64
	Fence            controlfence.NestedControlOperationToken
}

func (c DurableStopCommand) Validate() error {
	if err := validateOpaque("owner scaleplan id", c.OwnerScalePlanID, MaxPlanIDBytes); err != nil {
		return err
	}
	if err := validateOpaque("plan id", c.PlanID, MaxPlanIDBytes); err != nil {
		return err
	}
	if c.ExpectedRevision == 0 || c.NowMs <= 0 {
		return fmt.Errorf("%w: durable stop expectation", ErrInvalidPlan)
	}
	if !c.Fence.Issued() {
		return fmt.Errorf("%w: durable stop requires an issued control token", ErrInvalidPlan)
	}
	return nil
}

// DurableStopResult reports what the store observed.
//
// DurablyStopped is deliberately not a field the caller may set. It is derived
// from the record the store actually holds after the call, because a bool the
// caller can assign is a bool that survives the evidence it was based on.
type DurableStopResult struct {
	Status DurableStopStatus
	Record PlanRecord
}

// DurablyStopped reports whether the facts in this result support terminating
// the owning ScalePlan: the child is terminal, it carries this owner, and its
// terminal write is the one a stop produces or an earlier terminal it reached
// on its own.
func (r DurableStopResult) DurablyStopped(ownerScalePlanID string) bool {
	switch r.Status {
	case DurableStopStopped, DurableStopAlreadyStopped, DurableStopAlreadyTerminal:
	default:
		return false
	}
	return r.Record.Plan.Phase.Terminal() && r.Record.Plan.OwnerScalePlanID == ownerScalePlanID
}

// DurableStopStore is the atomic terminal entry point ScalePlan needs. The
// existing PlanStore has Load, CompareAndSwap and ListIncomplete and no way to
// write a terminal phase and leave the restart index in one step, so a
// controller crashing between the two would leave a terminal child that
// restart resurrects.
type DurableStopStore interface {
	PlanStore
	StopOwnedPlanForScaleTermination(context.Context, DurableStopCommand) (DurableStopResult, error)
}

// StopOwnedPlanForScaleTermination terminalizes an owned child as PAUSED.
//
// The in-memory restart index is derived from phase rather than stored beside
// it, so holding the store mutex across the read, the transition and the write
// gives the same atomicity the A1R Lua script must provide explicitly.
func (s *MemoryPlanStore) StopOwnedPlanForScaleTermination(ctx context.Context, command DurableStopCommand) (DurableStopResult, error) {
	if err := contextErr(ctx); err != nil {
		return DurableStopResult{Status: DurableStopUnsafe}, err
	}
	if err := command.Validate(); err != nil {
		return DurableStopResult{Status: DurableStopUnsafe}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.records[command.PlanID]
	if !ok {
		return DurableStopResult{Status: DurableStopNotFound}, nil
	}
	if record.Plan.OwnerScalePlanID != command.OwnerScalePlanID {
		return DurableStopResult{Status: DurableStopNotOwned, Record: record.Clone()}, nil
	}
	if record.Plan.Phase.Terminal() {
		// Replay must be exact. A child that this owner previously stopped
		// carries the fixed error code; anything else reached its terminal on
		// its own and is reported as such rather than relabelled.
		if record.Plan.Phase == PhasePaused && record.Plan.ErrorCode == DurableStopErrorCode {
			return DurableStopResult{Status: DurableStopAlreadyStopped, Record: record.Clone()}, nil
		}
		return DurableStopResult{Status: DurableStopAlreadyTerminal, Record: record.Clone()}, nil
	}
	if record.Plan.Revision != command.ExpectedRevision {
		return DurableStopResult{Status: DurableStopRevisionConflict, Record: record.Clone()}, nil
	}
	if command.ExpectedPhase != 0 && record.Plan.Phase != command.ExpectedPhase {
		return DurableStopResult{Status: DurableStopRevisionConflict, Record: record.Clone()}, nil
	}

	candidate := record.Plan.Clone()
	if !CanAdvancePhase(candidate.Phase, PhasePaused) {
		return DurableStopResult{Status: DurableStopUnsafe, Record: record.Clone()}, nil
	}
	candidate.Phase = PhasePaused
	candidate.ErrorCode = DurableStopErrorCode
	candidate.Revision++
	candidate.UpdatedAtMs = command.NowMs
	candidate.CompletedAtMs = command.NowMs
	if err := candidate.Validate(); err != nil {
		return DurableStopResult{Status: DurableStopUnsafe, Record: record.Clone()}, err
	}

	envelope, err := EncodePlan(candidate)
	if err != nil {
		return DurableStopResult{Status: DurableStopUnsafe, Record: record.Clone()}, err
	}
	stored := PlanRecord{Plan: candidate, Envelope: envelope}
	s.records[command.PlanID] = stored
	return DurableStopResult{Status: DurableStopStopped, Record: stored.Clone()}, nil
}

var _ DurableStopStore = (*MemoryPlanStore)(nil)
