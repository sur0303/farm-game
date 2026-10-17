package contracts

import (
	"bytes"
	"context"
	"sync"

	contractspb "farmgame/api/gen/contracts"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// StepHandler is the business hook of the fake participant server. It is
// invoked only after every pre-mailbox contract check has passed, at most
// once per (participant actor, invocation) terminal outcome. The handler
// decides the outcome; the fake owns receipt persistence, replay and
// conflict semantics.
type StepHandler func(ctx context.Context, req *contractspb.ParticipantRequest, payload proto.Message) (*StepOutcome, error)

// StepOutcome is the handler's decision for one participant step.
type StepOutcome struct {
	Outcome contractspb.ParticipantOutcome
	// ReplayedFromReceipt is set by a durable Participant handler when the
	// business result was decoded from the Actor-local receipt instead of
	// executing a second domain mutation. Fake handlers normally leave it
	// false because the fake owns replay bookkeeping.
	ReplayedFromReceipt bool
	// DurableReceiptCommitted is an internal proof bit, never sent on the
	// wire. Production Plant sets it only after the PlayerActor's one
	// authoritative commit contains the terminal receipt; Server rejects
	// terminal results from handlers that cannot provide that proof.
	DurableReceiptCommitted bool
	// CommittedResultHash is the canonical result hash read from (or written
	// into) the same durable receipt proof as Result. Production replay must
	// verify it against Result and return this committed value, rather than
	// manufacturing a fresh hash from replay-time data.
	CommittedResultHash []byte
	// CommittedRecordVersion is the exact Actor RecordVersion returned by the
	// same authoritative commit. The fake never synthesizes this value; a
	// terminal handler outcome with zero version fails closed.
	CommittedRecordVersion uint64
	// Result is the typed step result; when non-nil its descriptor must
	// exactly match the registered result schema of the command, or the
	// call fails closed without writing a receipt. nil is allowed for
	// REJECTED and NO_CHANGE outcomes without a typed body.
	Result             proto.Message
	Failure            *contractspb.ParticipantFailure
	CreatedSourceOrder *contractspb.CreatedSourceOrderEnvelope
}

type storedReceipt struct {
	stepHash               [32]byte
	outcome                contractspb.ParticipantOutcome
	typedResult            []byte
	resultHash             []byte
	committedRecordVersion uint64
	failure                *contractspb.ParticipantFailure
}

// inflightCall tracks one executing invocation so concurrent duplicates
// wait for the first terminal outcome instead of re-entering the handler.
type inflightCall struct {
	done chan struct{}
}

// FakeParticipantServer is an in-process fake of CoordinatorParticipantService
// and WorkerRecoveryIngress for contract and workflow tests. It enforces
// the frozen pre-mailbox checks of contract 29 §7.6 (registration, route
// header binding, caller context shape, step/command binding, work address
// shape and source authority, deterministic invocation binding, canonical
// step hash, typed result schema) and the ParticipantReceipt
// replay/conflict rules, without any business adjudication.
type FakeParticipantServer struct {
	contractspb.UnimplementedCoordinatorParticipantServiceServer
	contractspb.UnimplementedWorkerRecoveryIngressServer

	mu             sync.Mutex
	receipts       map[string]*storedReceipt
	inflight       map[string]*inflightCall
	handlers       map[contractspb.ParticipantCommandType]StepHandler
	ingressOptions ParticipantIngressOptions
}

// NewFakeParticipantServer creates an empty fake.
func NewFakeParticipantServer() *FakeParticipantServer {
	return NewFakeParticipantServerWithIngressOptions(ParticipantIngressOptions{})
}

// NewFakeParticipantServerWithIngressOptions creates the same fake with the
// shared physical ingress verifier seams enabled. The no-argument constructor
// remains a contract-only compatibility fake; production adapters must pass
// route, bucket and WorkRecord binding verifiers through their own ingress.
func NewFakeParticipantServerWithIngressOptions(options ParticipantIngressOptions) *FakeParticipantServer {
	return &FakeParticipantServer{
		receipts:       make(map[string]*storedReceipt),
		inflight:       make(map[string]*inflightCall),
		handlers:       make(map[contractspb.ParticipantCommandType]StepHandler),
		ingressOptions: options,
	}
}

// RegisterHandler installs the business hook for one command. Tests that
// do not register a handler get codes.Unimplemented, mirroring the
// fail-closed rule that unknown commands never reach an actor.
func (s *FakeParticipantServer) RegisterHandler(cmd contractspb.ParticipantCommandType, h StepHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[cmd] = h
}

// ExecuteStep implements CoordinatorParticipantService; it only accepts
// CoordinatorWorkContext (contract 29 §7.7).
func (s *FakeParticipantServer) ExecuteStep(ctx context.Context, req *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error) {
	return s.serve(ctx, req, ParticipantCallerCoordinator)
}

// ResumeStep implements WorkerRecoveryIngress; it only accepts
// InternalCallerContext (contract 29 §7.7).
func (s *FakeParticipantServer) ResumeStep(ctx context.Context, req *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error) {
	return s.serve(ctx, req, ParticipantCallerWorkerRecovery)
}

func (s *FakeParticipantServer) serve(ctx context.Context, req *contractspb.ParticipantRequest, callerKind ParticipantCallerKind) (*contractspb.ParticipantResponse, error) {
	call, err := ValidateAndDecodeWithOptions(ctx, req, callerKind, s.ingressOptions)
	if err != nil {
		return nil, err
	}
	receiptKey := call.ActorKey + "/" + call.InvocationID

	for {
		s.mu.Lock()
		if existing, ok := s.receipts[receiptKey]; ok {
			s.mu.Unlock()
			return replayReceipt(req, call.InvocationID, existing, call.CanonicalStepPayloadHash)
		}
		if call, ok := s.inflight[receiptKey]; ok {
			s.mu.Unlock()
			select {
			case <-call.done:
				// The leader finished; loop to replay the stored receipt,
				// surface the conflict, or become the new leader after an
				// error / TIMEOUT_UNKNOWN released the in-flight slot.
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		flight := &inflightCall{done: make(chan struct{})}
		s.inflight[receiptKey] = flight
		s.mu.Unlock()

		resp, err := s.lead(ctx, call, req, receiptKey)

		s.mu.Lock()
		delete(s.inflight, receiptKey)
		close(flight.done)
		s.mu.Unlock()
		// TIMEOUT_UNKNOWN and errors persist nothing: a concurrent waiter
		// loops and becomes the next leader (contract 29 §7.7).
		return resp, err
	}
}

// lead executes the handler for the in-flight leader.
func (s *FakeParticipantServer) lead(ctx context.Context, call *ParticipantCall, req *contractspb.ParticipantRequest, receiptKey string) (*contractspb.ParticipantResponse, error) {
	s.mu.Lock()
	handler, ok := s.handlers[req.GetParticipantCommand()]
	s.mu.Unlock()
	if !ok {
		return nil, status.Error(codes.Unimplemented, "contracts: no fake handler registered for "+string(call.Spec.Step))
	}

	bounded, cancel, err := ParticipantBudgetContext(ctx, call)
	if err != nil {
		return nil, err
	}
	defer cancel()
	outcome, err := handler(bounded, req, call.Payload)
	if err != nil {
		return nil, err
	}
	if outcome == nil {
		return nil, status.Error(codes.Internal, "contracts: fake handler returned nil outcome")
	}
	resp := &contractspb.ParticipantResponse{
		ParticipantCommand:      req.GetParticipantCommand(),
		ParticipantInvocationId: call.InvocationID,
		Outcome:                 outcome.Outcome,
		Failure:                 cloneFailure(outcome.Failure),
	}
	switch outcome.Outcome {
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
		contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED,
		contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE:
		if outcome.CommittedRecordVersion == 0 {
			return nil, status.Error(codes.Internal, "contracts: fake handler omitted committed record version")
		}
		if outcome.Outcome == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED && outcome.Result == nil {
			return nil, status.Error(codes.Internal, "contracts: APPLIED outcome requires typed result")
		}
		typedResult, resultHash, err := ValidateTypedResult(call.Spec, outcome.Result)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		s.receipts[receiptKey] = &storedReceipt{
			stepHash:               call.CanonicalStepPayloadHash,
			outcome:                outcome.Outcome,
			typedResult:            cloneBytes(typedResult),
			resultHash:             cloneBytes(resultHash),
			committedRecordVersion: outcome.CommittedRecordVersion,
			failure:                cloneFailure(outcome.Failure),
		}
		s.mu.Unlock()
		resp.TypedResult = typedResult
		resp.ParticipantResultHash = resultHash
		resp.CommittedRecordVersion = outcome.CommittedRecordVersion
		if outcome.CreatedSourceOrder != nil {
			resp.CreatedSourceOrder = proto.Clone(outcome.CreatedSourceOrder).(*contractspb.CreatedSourceOrderEnvelope)
		}
		return resp, nil
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN:
		// Not a participant terminal state: nothing is persisted, resolution
		// follows contract 30 unknown handling.
		return resp, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "contracts: fake handler returned unspecified outcome")
	}
}

// replayReceipt applies the frozen receipt rules of contract 29 §7.6: same
// invocation + same canonical step hash replays the original outcome; same
// invocation + different hash is PARTICIPANT_IDEMPOTENCY_CONFLICT. Stored
// state is cloned so callers can never mutate the receipt.
func replayReceipt(req *contractspb.ParticipantRequest, invocationID string, existing *storedReceipt, stepHash [32]byte) (*contractspb.ParticipantResponse, error) {
	if existing.stepHash != stepHash {
		return nil, status.Error(codes.FailedPrecondition, "PARTICIPANT_IDEMPOTENCY_CONFLICT")
	}
	return &contractspb.ParticipantResponse{
		ParticipantCommand:      req.GetParticipantCommand(),
		ParticipantInvocationId: invocationID,
		Outcome:                 existing.outcome,
		ReplayedFromReceipt:     true,
		TypedResult:             cloneBytes(existing.typedResult),
		ParticipantResultHash:   cloneBytes(existing.resultHash),
		Failure:                 cloneFailure(existing.failure),
		CommittedRecordVersion:  existing.committedRecordVersion,
	}, nil
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return bytes.Clone(b)
}

func cloneFailure(f *contractspb.ParticipantFailure) *contractspb.ParticipantFailure {
	if f == nil {
		return nil
	}
	return proto.Clone(f).(*contractspb.ParticipantFailure)
}

// FakeParticipantClient is an in-process client of FakeParticipantServer
// with the same call shape as the generated gRPC clients.
type FakeParticipantClient struct {
	server *FakeParticipantServer
}

// NewFakeParticipantClient wraps a fake server.
func NewFakeParticipantClient(server *FakeParticipantServer) *FakeParticipantClient {
	return &FakeParticipantClient{server: server}
}

// ExecuteStep calls the fake CoordinatorParticipantService.
func (c *FakeParticipantClient) ExecuteStep(ctx context.Context, req *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error) {
	return c.server.ExecuteStep(ctx, req)
}

// ResumeStep calls the fake WorkerRecoveryIngress.
func (c *FakeParticipantClient) ResumeStep(ctx context.Context, req *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error) {
	return c.server.ResumeStep(ctx, req)
}
