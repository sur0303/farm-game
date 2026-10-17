package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"farmgame/internal/workidentity"
)

// TestWorkflow is the generic proof workflow of WF1: it has no business
// meaning, only a deterministic step plan used to prove crash recovery.
const (
	testWorkType  = "TEST_WORKFLOW"
	testOrderType = "TEST_ORDER"
	stepReserve   = "RESERVE"
	stepCommit    = "COMMIT"
	stepFinalize  = "FINALIZE"
	stepRelease   = "RELEASE"
	stepApply     = "APPLY"
)

// fakeFault scripts one participant invocation attempt.
type fakeFault uint8

const (
	faultNone fakeFault = iota
	// faultCrashBeforeApply: transport failure provably before any effect.
	faultCrashBeforeApply
	// faultApplyLoseResponse: effect applied and receipt written, but the
	// response is lost.
	faultApplyLoseResponse
	// faultTimeoutUnknown: effect applied and receipt written, outcome
	// reported as TIMEOUT_UNKNOWN.
	faultTimeoutUnknown
	// faultStaleWorkProof: another driver advanced the WorkRecord after this
	// driver loaded it (WorkVersion proof mismatch). The engine must reload
	// and re-evaluate, never marking StepUnknown or surfacing a REJECTED
	// (WF-WORKVERSION-PROOF-RACE).
	faultStaleWorkProof
)

type fakeReceipt struct {
	hash   [32]byte
	result StepResult
}

// fakeParticipant models a contract-compliant participant: it persists a
// receipt per invocation and replays the original result for the same
// invocation + payload hash, rejecting the same invocation with a different
// hash.
type fakeParticipant struct {
	mu       sync.Mutex
	name     string
	receipts map[string]fakeReceipt
	applies  map[string]int
	scripts  map[string][]fakeFault
	rejects  map[string]string // invocationID → error class for durable reject
	calls    []StepCall
}

func newFakeParticipant(name string) *fakeParticipant {
	return &fakeParticipant{
		name:     name,
		receipts: make(map[string]fakeReceipt),
		applies:  make(map[string]int),
		scripts:  make(map[string][]fakeFault),
		rejects:  make(map[string]string),
	}
}

func (p *fakeParticipant) script(invocationID string, faults ...fakeFault) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.scripts[invocationID] = append([]fakeFault(nil), faults...)
}

func (p *fakeParticipant) reject(invocationID, errorClass string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rejects[invocationID] = errorClass
}

func (p *fakeParticipant) applyCount(invocationID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.applies[invocationID]
}

func (p *fakeParticipant) totalApplies() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	total := 0
	for _, n := range p.applies {
		total += n
	}
	return total
}

func (p *fakeParticipant) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

func (p *fakeParticipant) Invoke(_ context.Context, call StepCall) (StepResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, call)
	if receipt, ok := p.receipts[call.InvocationID]; ok {
		if receipt.hash != call.PayloadHash {
			return StepResult{}, fmt.Errorf("PARTICIPANT_IDEMPOTENCY_CONFLICT: %s", call.InvocationID)
		}
		replayed := receipt.result
		replayed.Replayed = true
		return replayed, nil
	}
	var fault fakeFault
	if queue := p.scripts[call.InvocationID]; len(queue) > 0 {
		fault = queue[0]
		p.scripts[call.InvocationID] = queue[1:]
	}
	switch fault {
	case faultCrashBeforeApply:
		return StepResult{}, errors.New("rpc: connection reset before apply")
	case faultStaleWorkProof:
		// Simulate the participant verifier rejecting the call because the
		// persisted WorkRecord version differs from the call's expected
		// version (a stale driver view). The engine must reload and retry.
		return StepResult{}, ErrStaleWorkProof
	}
	// The invocation reaches the participant: build the deterministic result.
	result := StepResult{
		Outcome: StepOutcomeApplied,
		Result:  []byte(fmt.Sprintf("%s:%s", p.name, call.StepName)),
	}
	result.ResultHash = sha256.Sum256(result.Result)
	if errorClass, ok := p.rejects[call.InvocationID]; ok {
		result.Outcome = StepOutcomeRejected
		result.ErrorClass = errorClass
		result.Result = []byte(errorClass)
		result.ResultHash = sha256.Sum256(result.Result)
	}
	p.receipts[call.InvocationID] = fakeReceipt{hash: call.PayloadHash, result: result}
	p.applies[call.InvocationID]++
	switch fault {
	case faultApplyLoseResponse:
		return StepResult{}, errors.New("rpc: response lost after apply")
	case faultTimeoutUnknown:
		unknown := result
		unknown.Outcome = StepOutcomeTimeoutUnknown
		unknown.Result = nil
		unknown.ResultHash = [32]byte{}
		return unknown, nil
	default:
		return result, nil
	}
}

// testWorkflowDefinition is the four-step plan
// RESERVE → COMMIT → FINALIZE with a RELEASE compensation branch.
type testWorkflowDefinition struct {
	workType     string
	steps        []string
	participants map[string]ParticipantInvoker
}

func newTestWorkflowDefinition(p1, p2 ParticipantInvoker) *testWorkflowDefinition {
	return &testWorkflowDefinition{
		workType: testWorkType,
		steps:    []string{stepReserve, stepCommit, stepFinalize, stepRelease},
		participants: map[string]ParticipantInvoker{
			stepReserve:  p1,
			stepCommit:   p2,
			stepFinalize: p1,
			stepRelease:  p1,
		},
	}
}

func (d *testWorkflowDefinition) WorkType() string   { return d.workType }
func (d *testWorkflowDefinition) StepNames() []string { return d.steps }

func (d *testWorkflowDefinition) BuildPayload(record *WorkRecord, stepIndex int) ([]byte, error) {
	return json.Marshal(map[string]string{
		"work_id": record.Address.WorkID,
		"step":    d.steps[stepIndex],
		"payload": string(record.Payload),
	})
}

func (d *testWorkflowDefinition) Invoker(stepIndex int) ParticipantInvoker {
	return d.participants[d.steps[stepIndex]]
}

func (d *testWorkflowDefinition) Decide(_ *WorkRecord, stepIndex int, result StepResult) (Decision, error) {
	step := d.steps[stepIndex]
	if result.Outcome == StepOutcomeRejected {
		switch step {
		case stepReserve:
			return Decision{Terminal: WorkStateCompensated}, nil
		case stepCommit:
			return Decision{NextStep: 3}, nil // RELEASE branch
		case stepRelease:
			return Decision{Terminal: WorkStateCompensated}, nil
		default:
			return Decision{Terminal: WorkStateRejected}, nil
		}
	}
	switch step {
	case stepReserve:
		return Decision{NextStep: 1}, nil
	case stepCommit:
		return Decision{NextStep: 2}, nil
	case stepFinalize:
		return Decision{Terminal: WorkStateCompleted}, nil
	case stepRelease:
		return Decision{Terminal: WorkStateCompensated}, nil
	default:
		return Decision{}, fmt.Errorf("%w: no decision for %s", ErrInvalid, step)
	}
}

// testOrderDefinition is a single-step source-order style work used for
// SUPERSEDED and dispatcher tests.
type testOrderDefinition struct {
	participant ParticipantInvoker
}

func (d *testOrderDefinition) WorkType() string    { return testOrderType }
func (d *testOrderDefinition) StepNames() []string { return []string{stepApply} }
func (d *testOrderDefinition) BuildPayload(record *WorkRecord, _ int) ([]byte, error) {
	return json.Marshal(map[string]string{"work_id": record.Address.WorkID, "payload": string(record.Payload)})
}
func (d *testOrderDefinition) Invoker(int) ParticipantInvoker { return d.participant }
func (d *testOrderDefinition) Decide(_ *WorkRecord, _ int, result StepResult) (Decision, error) {
	if result.Outcome == StepOutcomeRejected {
		return Decision{Terminal: WorkStateRejected}, nil
	}
	return Decision{Terminal: WorkStateCompleted}, nil
}

func testInvocationID(t interface{ Helper() }, workType, workID, step string) string {
	t.Helper()
	id, err := workidentity.ParticipantInvocationID(workType, workID, step, 1)
	if err != nil {
		panic(err)
	}
	return id
}
