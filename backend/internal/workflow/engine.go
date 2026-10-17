package workflow

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"farmgame/internal/workidentity"
)

var (
	// ErrRetryScheduled marks a pause: the record was durably rescheduled in
	// the due index and will be resumed by the dispatcher or a worker.
	ErrRetryScheduled = errors.New("workflow: retry scheduled")
	// ErrDefinitionDrift marks a frozen step payload that no longer matches
	// its definition; the work is failed closed instead of guessed.
	ErrDefinitionDrift = errors.New("workflow: step definition drift")
	// ErrUnknownDefinition marks an unregistered work type.
	ErrUnknownDefinition = errors.New("workflow: unknown work definition")
	// ErrStaleWorkProof marks a participant proof failure caused by another
	// driver (DirectDispatcher or worker) having advanced the same WorkRecord
	// after this driver loaded it (WorkVersion mismatch). It is an internal
	// CAS/proof race, NOT a business rejection: the engine must reload the
	// authoritative record and re-evaluate, and it must never leak as a public
	// REJECTED (WF-WORKVERSION-PROOF-RACE).
	ErrStaleWorkProof = errors.New("workflow: stale work proof (WorkVersion advanced by another driver)")
)

// StepOutcome is the participant-visible result classification frozen by
// contract 29 §7.7 (APPLIED / REJECTED / NO_CHANGE / TIMEOUT_UNKNOWN).
type StepOutcome uint8

const (
	StepOutcomeUnspecified StepOutcome = iota
	StepOutcomeApplied
	StepOutcomeRejected
	StepOutcomeNoChange
	StepOutcomeTimeoutUnknown
)

// StepCall is one deterministic participant invocation. InvocationID,
// Payload and PayloadHash are frozen in the durable record before the first
// invoke; retries, coordinator-to-worker handover and lease takeovers reuse
// them unchanged (contract 29 §7.5).
type StepCall struct {
	Work                workidentity.WorkAddress
	WorkVersion         uint64
	StepIndex           int
	StepName            string
	InvocationID        string
	Payload             []byte
	PayloadHash         [32]byte
	DependsOnResultHash [32]byte
	OperationID         string
	SubjectPlayerID     uint64
}

// StepResult is the participant response. Replayed mirrors
// replayed_from_receipt: the effect was not re-executed.
type StepResult struct {
	Outcome    StepOutcome
	Result     []byte
	ResultHash [32]byte
	ErrorClass string
	Replayed   bool
}

// ParticipantInvoker is the generic participant port. Production adapters
// (AR1/PI1/FI1) implement it over the trusted Participant RPC; tests
// implement scriptable fakes. It must honor the receipt contract: same
// invocation + same payload hash returns the original result.
type ParticipantInvoker interface {
	Invoke(ctx context.Context, call StepCall) (StepResult, error)
}

// Decision tells the engine where the workflow goes after a terminal step
// result. Terminal zero means continue at NextStep (strictly forward).
type Decision struct {
	Terminal WorkState
	NextStep int
}

// Definition binds a work type to its deterministic step plan. It must be
// pure: the same record always yields the same payloads and decisions, so a
// crashed process and a recovering worker rebuild identical invocations.
type Definition interface {
	WorkType() string
	StepNames() []string
	BuildPayload(record *WorkRecord, stepIndex int) ([]byte, error)
	Invoker(stepIndex int) ParticipantInvoker
	Decide(record *WorkRecord, stepIndex int, result StepResult) (Decision, error)
}

// LeaseFence identifies the current work lease holder; zero value means the
// caller is an unleased coordinator/dispatcher.
type LeaseFence struct {
	Owner string
	Epoch uint64
}

func (f LeaseFence) held() bool { return f.Owner != "" }

// Engine drives generic Work records through their step plan with per-step
// CAS persistence. It contains no business rules; all scenario semantics
// live in Definition implementations.
type Engine struct {
	repo    WorkRepository
	defs    map[string]Definition
	backoff BackoffConfig
	rng     *rand.Rand
	now     func() time.Time
}

type EngineOption func(*Engine)

func WithEngineBackoff(config BackoffConfig) EngineOption {
	return func(e *Engine) { e.backoff = config.withDefaults() }
}

func WithEngineRand(rng *rand.Rand) EngineOption {
	return func(e *Engine) { e.rng = rng }
}

func WithEngineClock(now func() time.Time) EngineOption {
	return func(e *Engine) {
		if now != nil {
			e.now = now
		}
	}
}

func NewEngine(repo WorkRepository, defs []Definition, opts ...EngineOption) (*Engine, error) {
	if repo == nil || len(defs) == 0 {
		return nil, fmt.Errorf("%w: repository and definitions required", ErrInvalid)
	}
	engine := &Engine{
		repo:    repo,
		defs:    make(map[string]Definition, len(defs)),
		backoff: BackoffConfig{}.withDefaults(),
		now:     time.Now,
	}
	for _, def := range defs {
		if def == nil || !validStepName(def.WorkType()) || len(def.StepNames()) == 0 {
			return nil, fmt.Errorf("%w: definition", ErrInvalid)
		}
		for _, name := range def.StepNames() {
			if !validStepName(name) {
				return nil, fmt.Errorf("%w: step name %q", ErrInvalid, name)
			}
		}
		if _, dup := engine.defs[def.WorkType()]; dup {
			return nil, fmt.Errorf("%w: duplicate definition %s", ErrInvalid, def.WorkType())
		}
		engine.defs[def.WorkType()] = def
	}
	for _, opt := range opts {
		opt(engine)
	}
	return engine, nil
}

// Create derives the durable anchor and atomically CreateIfAbsent's it. The
// same business intent always derives the same WorkAddress and intent fields,
// so a crash before or after this call converges to exactly one record.
func (e *Engine) Create(ctx context.Context, def Definition, address workidentity.WorkAddress,
	subjectPlayerID uint64, operationID string, payload []byte, payloadHash [32]byte,
	deadline time.Time) (*WorkRecord, bool, error) {
	if def == nil {
		return nil, false, ErrUnknownDefinition
	}
	if registered, ok := e.defs[def.WorkType()]; !ok || registered != def {
		return nil, false, fmt.Errorf("%w: %s", ErrUnknownDefinition, def.WorkType())
	}
	record, err := NewWorkRecord(address, subjectPlayerID, operationID, payload, payloadHash, def.StepNames(), e.now(), deadline)
	if err != nil {
		return nil, false, err
	}
	return e.repo.CreateIfAbsent(ctx, record)
}

// Resume advances the record until it is terminal, the budget stops it, or a
// step must be retried later. Every durable transition is one CAS; a crash
// between steps resumes from the last persisted boundary. stop, when set, is
// the worker's conservative local lease deadline: once it fires no new step
// is started.
func (e *Engine) Resume(ctx context.Context, record *WorkRecord, fence LeaseFence, stop func() bool) error {
	if err := record.Validate(); err != nil {
		return err
	}
	def, ok := e.defs[record.Address.WorkType]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownDefinition, record.Address.WorkType)
	}
	for !record.Terminal() {
		if record.DeadlineAtMs > 0 && e.now().UnixMilli() >= record.DeadlineAtMs {
			record.State = WorkStateFailed
			record.LastErrorClass = "DEADLINE_EXCEEDED"
			reload, deferRound, err := e.persistWithRaceHandling(ctx, record, fence, false)
			if err != nil {
				return err
			}
			if deferRound {
				return nil
			}
			if reload {
				continue
			}
			return nil
		}
		if stop != nil && stop() {
			return e.pause(ctx, record, fence, "LEASE_BUDGET_EXHAUSTED")
		}
		stepIndex := record.CurrentStep
		step := &record.Steps[stepIndex]

		if StepTerminal(step.Status) {
			// Crash between result fold and pointer advance is impossible
			// (one CAS), but a terminal step at the cursor means the decision
			// fold was persisted; re-fold deterministically and continue.
			decision, err := def.Decide(record, stepIndex, stepResultFromRecord(step))
			if err != nil {
				return e.failClosed(ctx, record, fence, "DECIDE_FAULT", err)
			}
			if err := applyDecision(record, decision, stepIndex); err != nil {
				return e.failClosed(ctx, record, fence, "DECIDE_FAULT", err)
			}
			reload, deferRound, err := e.persistWithRaceHandling(ctx, record, fence, false)
			if err != nil {
				return err
			}
			if deferRound {
				return nil
			}
			if reload {
				continue
			}
			continue
		}

		if len(step.Payload) == 0 {
			payload, err := def.BuildPayload(record, stepIndex)
			if err != nil {
				return e.failClosed(ctx, record, fence, "BUILD_PAYLOAD_FAULT", err)
			}
			step.Payload = append([]byte(nil), payload...)
			step.PayloadHash = sha256.Sum256(payload)
			step.DependsOnResultHash = previousResultHash(record, stepIndex)
			reload, deferRound, err := e.persistWithRaceHandling(ctx, record, fence, false)
			if err != nil {
				return err
			}
			if deferRound {
				return nil
			}
			if reload {
				continue
			}
			// persist swaps the record; rebind the step pointer.
			step = &record.Steps[stepIndex]
		} else {
			rebuilt, err := def.BuildPayload(record, stepIndex)
			if err != nil || sha256.Sum256(rebuilt) != step.PayloadHash {
				return e.failClosed(ctx, record, fence, "DEFINITION_DRIFT", ErrDefinitionDrift)
			}
		}

		// WF-WORKVERSION-PROOF-RACE C2: never send a participant call with a
		// stale WorkVersion or without proving this driver still holds the
		// work. DirectDispatcher and farm-worker both drive the same record;
		// when the payload was already persisted the code used to skip the
		// CAS/fence entirely and send a call built from the loaded version.
		// Fresh-read: if the version moved (another driver advanced it) reload
		// and re-evaluate; if a live worker lease fences a dispatcher, defer
		// this round (the worker recovers it from DueIndex).
		fresh, preflightErr := e.repo.Get(ctx, record.Address.WorkType, record.Address.WorkID)
		if preflightErr != nil {
			return e.failClosed(ctx, record, fence, "PREFLIGHT_READ", preflightErr)
		}
		if fresh.WorkVersion != record.WorkVersion {
			log.Printf("workflow_preflight_reload workflow=%s step=%s invocation=%s loaded_v=%d fresh_v=%d",
				record.Address.WorkID, step.Name, step.InvocationID, record.WorkVersion, fresh.WorkVersion)
			*record = *cloneWorkRecord(fresh)
			continue
		}
		if fenceErr := fenceWorkLease(fresh, AdvanceOptions{LeaseOwner: fence.Owner, LeaseEpoch: fence.Epoch}, e.now()); fenceErr != nil {
			if fence.Owner == "" {
				// Direct dispatcher raced a live worker lease: defer; the
				// worker will recover the work from DueIndex. No participant
				// call, no state mutation, no business side effect.
				log.Printf("workflow_preflight_dispatcher_defer workflow=%s step=%s invocation=%s err=%v",
					record.Address.WorkID, step.Name, step.InvocationID, fenceErr)
				return nil
			}
			log.Printf("workflow_preflight_lease_lost workflow=%s step=%s invocation=%s err=%v",
				record.Address.WorkID, step.Name, step.InvocationID, fenceErr)
			*record = *cloneWorkRecord(fresh)
			continue
		}

		invoker := def.Invoker(stepIndex)
		if invoker == nil {
			return e.failClosed(ctx, record, fence, "INVOKER_MISSING", fmt.Errorf("%w: invoker", ErrInvalid))
		}
		call := StepCall{
			Work:                record.Address,
			WorkVersion:         record.WorkVersion,
			StepIndex:           stepIndex,
			StepName:            step.Name,
			InvocationID:        step.InvocationID,
			Payload:             append([]byte(nil), step.Payload...),
			PayloadHash:         step.PayloadHash,
			DependsOnResultHash: step.DependsOnResultHash,
			OperationID:         record.OperationID,
			SubjectPlayerID:     record.SubjectPlayerID,
		}
	result, err := invoker.Invoke(ctx, call)
	if err != nil {
		// WF-WORKVERSION-PROOF-RACE: an internal WorkVersion proof mismatch means
		// another driver (DirectDispatcher or worker) advanced this WorkRecord
		// after we loaded it. This is an internal CAS/proof race, NOT a business
		// outcome: reload the authoritative record and re-evaluate the current
		// step. Never mark StepUnknown / pause / surface as a public REJECTED.
		if errors.Is(err, ErrStaleWorkProof) {
			reloaded, reloadErr := e.repo.Get(ctx, record.Address.WorkType, record.Address.WorkID)
			if reloadErr != nil {
				return e.failClosed(ctx, record, fence, "STALE_WORK_PROOF_RELOAD", reloadErr)
			}
			log.Printf("workflow_stale_proof_reload workflow=%s step=%s invocation=%s loaded_v=%d reloaded_v=%d state=%s",
				record.Address.WorkID, step.Name, step.InvocationID, call.WorkVersion, reloaded.WorkVersion, reloaded.State)
			*record = *cloneWorkRecord(reloaded)
			continue // re-evaluate current step / terminal state from the authoritative record
		}
		// Q1 WF-PARTICIPANT-INVOCATION-UNKNOWN-DIAG: workflow transition into
		// StepUnknown. Emits the physical invocation cause so the worker log
		// reveals why this step never converges (transport vs server vs
		// response-loss vs classification).
		log.Printf("workflow_step_unknown workflow=%s step=%s invocation=%s attempt=%d op=%s player=%d err_class=INVOCATION_UNKNOWN cause=%v",
			record.Address.WorkID, step.Name, step.InvocationID, step.Attempts+1, record.OperationID, record.SubjectPlayerID, err)
		step.Status = StepUnknown
		step.Attempts++
		record.State = WorkStateStepUnknown
		record.LastErrorClass = "INVOCATION_UNKNOWN"
			// Preserve the durable retry marker while returning the physical
			// invocation cause to the recovery boundary.  The cause is never
			// exposed as a public result; it lets the worker distinguish a
			// typed Participant rejection/route failure from a lost response
			// and keeps receipt-first recovery diagnosable.
			return errors.Join(e.pause(ctx, record, fence, "INVOCATION_UNKNOWN"), err)
		}
		step.Attempts++
		switch result.Outcome {
		case StepOutcomeApplied, StepOutcomeNoChange:
			if result.Outcome == StepOutcomeApplied {
				step.Status = StepApplied
			} else {
				step.Status = StepNoChange
			}
			step.Result = append([]byte(nil), result.Result...)
			step.ResultHash = result.ResultHash
			step.ErrorClass = ""
			record.LastErrorClass = ""
		case StepOutcomeRejected:
			step.Status = StepRejected
			step.Result = append([]byte(nil), result.Result...)
			step.ResultHash = result.ResultHash
			step.ErrorClass = result.ErrorClass
			record.LastErrorClass = result.ErrorClass
		case StepOutcomeTimeoutUnknown:
			step.Status = StepUnknown
			record.State = WorkStateStepUnknown
			record.LastErrorClass = "PARTICIPANT_TIMEOUT_UNKNOWN"
			return e.pause(ctx, record, fence, "PARTICIPANT_TIMEOUT_UNKNOWN")
		default:
			return e.failClosed(ctx, record, fence, "OUTCOME_INVALID", fmt.Errorf("%w: outcome", ErrInvalid))
		}

		decision, err := def.Decide(record, stepIndex, result)
		if err != nil {
			return e.failClosed(ctx, record, fence, "DECIDE_FAULT", err)
		}
		if err := applyDecision(record, decision, stepIndex); err != nil {
			return e.failClosed(ctx, record, fence, "DECIDE_FAULT", err)
		}
		// WF-WORKVERSION-PROOF-RACE C2 follow-up: the decision persist has the
		// same double-driver race as deadline/build-payload persists. Another
		// driver (DirectDispatcher or worker) may have advanced the record
		// while this driver's participant call was in flight; a bare persist
		// here would surface ErrRevisionConflict to the caller (e.g. the
		// synchronous command submit path), which farm/ingress maps to the
		// public ERR_STORE_CAS_CONFLICT (9002). Route it through
		// persistWithRaceHandling like every other durable transition: the
		// dispatcher defers (worker recovers from DueIndex), the worker reloads
		// the authoritative record and re-evaluates.
		reload, deferRound, err := e.persistWithRaceHandling(ctx, record, fence, false)
		if err != nil {
			return err
		}
		if deferRound {
			return nil
		}
		if reload {
			continue
		}
	}
	return nil
}

// Supersede terminally marks a pending record as obsolete (e.g. a newer
// source fact replaced an older order). It is idempotent and never revives
// terminal records.
func (e *Engine) Supersede(ctx context.Context, workType, workID string) (*WorkRecord, error) {
	record, err := e.repo.Get(ctx, workType, workID)
	if err != nil {
		return nil, err
	}
	if record.State == WorkStateSuperseded {
		return record, nil
	}
	if record.Terminal() {
		return nil, ErrTerminal
	}
	record.State = WorkStateSuperseded
	record.LastErrorClass = "SUPERSEDED"
	updated, err := e.repo.Advance(ctx, record, AdvanceOptions{ExpectedWorkVersion: record.WorkVersion})
	if err != nil {
		return nil, err
	}
	return updated, nil
}

// pause durably reschedules the record in the due index and releases any
// lease so the due time (not the old lease) governs the next attempt.
func (e *Engine) pause(ctx context.Context, record *WorkRecord, fence LeaseFence, errorClass string) error {
	record.RetryCount++
	record.NextDueAtMs = e.now().Add(e.backoff.NextBackoff(record.RetryCount, e.rng)).UnixMilli()
	if errorClass != "" {
		record.LastErrorClass = errorClass
	}
	if _, err := e.persist(context.WithoutCancel(ctx), record, fence, true); err != nil {
		return err
	}
	return ErrRetryScheduled
}

// failClosed moves the record to the dead-letter terminal instead of
// guessing across a contract or definition violation.
func (e *Engine) failClosed(ctx context.Context, record *WorkRecord, fence LeaseFence, errorClass string, cause error) error {
	record.State = WorkStateFailed
	record.LastErrorClass = errorClass
	if _, err := e.persist(context.WithoutCancel(ctx), record, fence, false); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (e *Engine) persist(ctx context.Context, record *WorkRecord, fence LeaseFence, releaseLease bool) (*WorkRecord, error) {
	updated, err := e.repo.Advance(ctx, record, AdvanceOptions{
		ExpectedWorkVersion: record.WorkVersion,
		LeaseOwner:          fence.Owner,
		LeaseEpoch:          fence.Epoch,
		ReleaseLease:        releaseLease,
	})
	if err != nil {
		return nil, err
	}
	*record = *cloneWorkRecord(updated)
	return updated, nil
}

// persistWithRaceHandling advances the record and handles a revision conflict
// caused by another driver (DirectDispatcher or worker) having advanced the
// same WorkRecord after we loaded it (WF-WORKVERSION-PROOF-RACE C2):
//   - persisted:            reload=false, defer=false, err=nil (caller proceeds)
//   - dispatcher loser:     reload=false, defer=true,  err=nil (return nil; the
//     worker recovers the work from DueIndex, no stale call, no side effect)
//   - worker reload:        reload=true,  defer=false, err=nil (caller reloaded
//     the authoritative record and should continue re-evaluating)
//   - other error:          returned as-is.
//
// ErrTerminal is treated as the same benign double-driver race: the other
// driver advanced the record all the way to a terminal state while this
// driver's decision was in flight (the repository checks Terminal before
// WorkVersion, so it surfaces as ErrTerminal rather than ErrRevisionConflict).
// A dispatcher defers (work already finished), a worker reloads and the loop
// exits on the terminal state — never leaking ErrTerminal to the caller.
func (e *Engine) persistWithRaceHandling(ctx context.Context, record *WorkRecord, fence LeaseFence, releaseLease bool) (reload bool, deferRound bool, err error) {
	_, err = e.persist(ctx, record, fence, releaseLease)
	if err == nil {
		return false, false, nil
	}
	if !errors.Is(err, ErrRevisionConflict) && !errors.Is(err, ErrTerminal) {
		return false, false, err
	}
	fresh, gerr := e.repo.Get(ctx, record.Address.WorkType, record.Address.WorkID)
	if gerr != nil {
		return false, false, e.failClosed(ctx, record, fence, "RACE_RELOAD_READ", gerr)
	}
	if fence.Owner == "" {
		// Direct dispatcher raced a live worker lease (or another driver's
		// advance): defer this round, never send a stale participant call.
		log.Printf("workflow_persist_dispatcher_defer workflow=%s loaded_v=%d fresh_v=%d",
			record.Address.WorkID, record.WorkVersion, fresh.WorkVersion)
		return false, true, nil
	}
	log.Printf("workflow_persist_worker_reload workflow=%s loaded_v=%d fresh_v=%d",
		record.Address.WorkID, record.WorkVersion, fresh.WorkVersion)
	*record = *cloneWorkRecord(fresh)
	return true, false, nil
}

func applyDecision(record *WorkRecord, decision Decision, stepIndex int) error {
	if decision.Terminal != WorkStateUnspecified {
		if !WorkTerminal(decision.Terminal) {
			return fmt.Errorf("%w: decision terminal %s", ErrStateIllegal, decision.Terminal)
		}
		record.State = decision.Terminal
		record.NextDueAtMs = 0
		return nil
	}
	if decision.NextStep <= stepIndex || decision.NextStep >= len(record.Steps) {
		return fmt.Errorf("%w: decision next step %d from %d", ErrStateIllegal, decision.NextStep, stepIndex)
	}
	record.CurrentStep = decision.NextStep
	record.State = WorkStateRunning
	return nil
}

func stepResultFromRecord(step *StepRecord) StepResult {
	result := StepResult{
		Result:     append([]byte(nil), step.Result...),
		ResultHash: step.ResultHash,
		ErrorClass: step.ErrorClass,
		Replayed:   true,
	}
	switch step.Status {
	case StepApplied:
		result.Outcome = StepOutcomeApplied
	case StepRejected:
		result.Outcome = StepOutcomeRejected
	case StepNoChange:
		result.Outcome = StepOutcomeNoChange
	}
	return result
}

// previousResultHash returns the result hash of the nearest terminally
// executed step before stepIndex, implementing the dependency hash chain of
// contract 29 §7.11.
func previousResultHash(record *WorkRecord, stepIndex int) [32]byte {
	for i := stepIndex - 1; i >= 0; i-- {
		step := &record.Steps[i]
		if (step.Status == StepApplied || step.Status == StepNoChange) && step.ResultHash != ([32]byte{}) {
			return step.ResultHash
		}
	}
	return [32]byte{}
}
