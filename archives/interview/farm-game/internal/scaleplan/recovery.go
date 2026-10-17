package scaleplan

import (
	"context"
	"errors"
)

// Recover enumerates the bounded durable nonterminal set. It never resumes an
// explicitly paused plan and never reconstructs IDs from process memory.
func (e *Executor) Recover(ctx context.Context) error {
	if e == nil || ctx == nil {
		return ErrInvalidPlan
	}
	records, err := e.repository.ListRecoverable(ctx)
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.Plan.Status == StatusPaused || record.Plan.Status.Terminal() {
			continue
		}
		if record.Plan.Status == StatusBlocked {
			_, reconcileErr := e.ReconcileBlocked(ctx, record.Plan.PlanID)
			if reconcileErr == nil || errors.Is(reconcileErr, ErrBlocked) {
				continue
			}
			return reconcileErr
		}
		result, executeErr := e.Execute(ctx, record.Plan.PlanID)
		if executeErr == nil || errors.Is(executeErr, ErrPending) || errors.Is(executeErr, ErrPaused) ||
			errors.Is(executeErr, ErrBlocked) || result.Plan.Status.Terminal() {
			continue
		}
		return executeErr
	}
	return nil
}

func (e *Executor) Startup(ctx context.Context) error { return e.Recover(ctx) }
