// Package participant implements the PlayerActor side of the frozen
// Coordinator/Worker Participant RPC boundary. It owns no Work repository and
// no farm persistence; its only durable business authority is the injected
// PlayerActor Registry.
package participant

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/contracts"
	playeractor "farmgame/internal/player/actor"
	playeringress "farmgame/internal/player/ingress"
	playertask "farmgame/internal/player/task"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/worker/sourceorder"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"log"
)

// WorkVersionVerifier is called after the shared request kernel and before
// the PlayerActor mailbox. It must verify the Work Repository's authoritative
// version; the caller-provided expected version is never trusted by itself.
type WorkVersionVerifier interface {
	VerifyWorkVersion(context.Context, *contracts.ParticipantCall) error
}

// WorkLeaseVerifier is the WorkerRecoveryIngress equivalent. It must verify
// owner token, epoch, boot identity and trusted expiry before mailbox entry.
type WorkLeaseVerifier interface {
	VerifyWorkLease(context.Context, *contracts.ParticipantCall) error
}

// WorkVersionVerifierFunc adapts a function to WorkVersionVerifier.
type WorkVersionVerifierFunc func(context.Context, *contracts.ParticipantCall) error

func (f WorkVersionVerifierFunc) VerifyWorkVersion(ctx context.Context, call *contracts.ParticipantCall) error {
	return f(ctx, call)
}

// WorkLeaseVerifierFunc adapts a function to WorkLeaseVerifier.
type WorkLeaseVerifierFunc func(context.Context, *contracts.ParticipantCall) error

func (f WorkLeaseVerifierFunc) VerifyWorkLease(ctx context.Context, call *contracts.ParticipantCall) error {
	return f(ctx, call)
}

// PlantDependencyVerifier proves the immutable Plant WorkRecord and Farm
// decision binding before any Plant Player step can change PlayerRecord. A
// concrete verifier must bind player/farm/plot, crop/seed/quantity, rule
// version/hash, reservation/cycle identity, workflow step/version and the
// Commit/Release Farm terminal decision state/hash. It never reads or writes
// Farm persistence here; terminal receipt replay bypasses it because the
// original proof was committed with the receipt.
type PlantDependencyVerifier interface {
	VerifyPlantDependency(context.Context, *contracts.ParticipantCall) error
}

// PlantDependencyVerifierFunc adapts a function to PlantDependencyVerifier.
type PlantDependencyVerifierFunc func(context.Context, *contracts.ParticipantCall) error

func (f PlantDependencyVerifierFunc) VerifyPlantDependency(ctx context.Context, call *contracts.ParticipantCall) error {
	return f(ctx, call)
}

// UnlockDependencyVerifier proves the durable UnlockPlot WorkRecord and its
// cross-step result hash before a coin participant enters PlayerActor.
type UnlockDependencyVerifier interface {
	VerifyUnlockDependency(context.Context, *contracts.ParticipantCall) error
}

type UnlockDependencyVerifierFunc func(context.Context, *contracts.ParticipantCall) error

func (f UnlockDependencyVerifierFunc) VerifyUnlockDependency(ctx context.Context, call *contracts.ParticipantCall) error {
	return f(ctx, call)
}

// HarvestDependencyVerifier proves the immutable Harvest WorkRecord and Farm
// commit binding before GrantHarvestReward can enter the PlayerActor mailbox.
// In particular, an implementation must prove the saved COMMIT_HARVEST result
// hash, the FARM_HARVEST_COMMITTED/PLAYER_REWARDING state, and the
// player/workflow/farm/cycle bindings. The Player Participant deliberately has
// no Farm repository access and therefore cannot turn a missing physical proof
// into an allow decision.
type HarvestDependencyVerifier interface {
	VerifyHarvestDependency(context.Context, *contracts.ParticipantCall) error
}

// HarvestDependencyVerifierFunc adapts a Harvest dependency proof callback.
type HarvestDependencyVerifierFunc func(context.Context, *contracts.ParticipantCall) error

func (f HarvestDependencyVerifierFunc) VerifyHarvestDependency(ctx context.Context, call *contracts.ParticipantCall) error {
	return f(ctx, call)
}

type HarvestSourceVerifier interface {
	VerifyHarvestSource(context.Context, *contracts.ParticipantCall) error
}

type HarvestSourceVerifierFunc func(context.Context, *contracts.ParticipantCall) error

func (f HarvestSourceVerifierFunc) VerifyHarvestSource(ctx context.Context, call *contracts.ParticipantCall) error {
	return f(ctx, call)
}

type TheftSourceVerifier interface {
	VerifyTheftSource(context.Context, *contracts.ParticipantCall) error
}

// TheftDependencyVerifier proves the immutable Theft WorkRecord and the
// persisted FARM_DECIDED / visitor-settlement evidence before either Player
// command enters the mailbox. The implementation belongs to the Workflow
// proof adapter; the PlayerActor receives no source repository handle.
type TheftDependencyVerifier interface {
	VerifyTheftDependency(context.Context, *contracts.ParticipantCall) error
}

// TheftDependencyVerifierFunc adapts a test or composition proof callback.
type TheftDependencyVerifierFunc func(context.Context, *contracts.ParticipantCall) error

func (f TheftDependencyVerifierFunc) VerifyTheftDependency(ctx context.Context, call *contracts.ParticipantCall) error {
	return f(ctx, call)
}

// CareRewardOrderState is the source-authority business state proven by a
// CareDependencyVerifier. A Player Participant may dispatch only an order
// that is still pending or already in the worker/direct-dispatch execution
// state; terminal orders are never a new reward admission.
type CareRewardOrderState uint8

const (
	CareRewardOrderStateUnspecified CareRewardOrderState = iota
	CareRewardOrderStatePending
	CareRewardOrderStateRewardDispatching
)

// CareDependencyExecutionMode describes the source Order's invocation fence.
// Coordinator delivery is unleased; recovery delivery must carry the exact
// currently valid worker lease. The verifier owns the trusted-time check.
type CareDependencyExecutionMode uint8

const (
	CareDependencyExecutionModeUnspecified CareDependencyExecutionMode = iota
	CareDependencyExecutionModeCoordinator
	CareDependencyExecutionModeRecoveryLease
)

// CareDependencyProof is the structured evidence returned by the source-order
// verifier. It is intentionally richer than an error-only callback so a fake
// and the future CR3 Redis adapter have one explicit proof contract. The
// Participant validates these bindings again before mailbox entry; the
// PlayerActor turn receives no Farm client and never performs a cross-service
// read.
type CareDependencyProof struct {
	// SourceOrderFound proves this is a real Farm Authority SourceOrder, not a
	// self-consistent WorkRecord invented by the caller.
	SourceOrderFound bool
	SourceOrderID    string
	SourceOrderType  contractspb.WorkType
	SourceAuthority  *contractspb.SourceAuthorityRef
	Snapshot         *contractspb.CareResolutionSnapshot
	SnapshotHash     []byte
	OrderState       CareRewardOrderState
	DueIndexBound    bool

	// These fields bind the source Work version and the exact Participant
	// invocation. FenceValid is the source repository's trusted route/claim
	// result. LeaseValid is an explicit source-repository assertion that the
	// exact lease tuple below passed its trusted-time expiry check.
	WorkVersion     uint64
	InvocationID    string
	ParticipantStep string
	ExecutionMode   CareDependencyExecutionMode
	FenceValid      bool
	LeaseValid      bool
	LeaseOwnerToken string
	LeaseEpoch      uint64
	LeaseExpireAtMs int64
}

// CareDependencyVerifier proves the immutable Farm Authority CareRewardOrder
// and its current dispatch admission before the PlayerActor mailbox. A valid
// implementation must prove SourceOrder existence/identity and CARE_REWARD
// type, source farm/incident/carer and exact frozen snapshot/hash equality,
// allowed PENDING/REWARD_DISPATCHING state, DueIndex binding, and legal
// invocation/lease/fence semantics. It may perform repository/Redis I/O here;
// it must not be called from the PlayerActor turn.
type CareDependencyVerifier interface {
	VerifyCareDependency(context.Context, *contracts.ParticipantCall) (CareDependencyProof, error)
}

// CareDependencyVerifierFunc adapts a structured proof callback.
type CareDependencyVerifierFunc func(context.Context, *contracts.ParticipantCall) (CareDependencyProof, error)

func (f CareDependencyVerifierFunc) VerifyCareDependency(ctx context.Context, call *contracts.ParticipantCall) (CareDependencyProof, error) {
	return f(ctx, call)
}

// ReceiptResolver performs a durable receipt preflight. It is separate from
// Handler so a terminal same-hash retry can be answered before stale Work or
// Lease fencing.
type ReceiptResolver interface {
	ResolveParticipantReceipt(context.Context, *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error)
}

// Handler is shared by Coordinator ExecuteStep and Worker ResumeStep. The
// two RPC methods only differ in their pre-mailbox verifier; they must never
// have separate business implementations.
type Handler interface {
	Handle(context.Context, *contracts.ParticipantCall) (*contracts.StepOutcome, error)
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, *contracts.ParticipantCall) (*contracts.StepOutcome, error)

func (f HandlerFunc) Handle(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, error) {
	return f(ctx, call)
}

// testOnlyParticipantHandler is deliberately package-private. It prevents an
// arbitrary production package from smuggling a Handler plus ReceiptResolver
// into the Participant constructor; package-local tests can opt into a
// non-persistent spy explicitly, while checked composition can never use it.
type testOnlyParticipantHandler interface {
	Handler
	ReceiptResolver
	participantTestOnly()
}

// Options controls the reusable Player Participant adapter. A nil verifier
// is intentionally fail-closed; the authority composition root supplies the
// Redis-backed Care verifier for production.
type Options struct {
	Registry                  *playeractor.Registry
	WorkVersionVerifier       WorkVersionVerifier
	WorkLeaseVerifier         WorkLeaseVerifier
	RouteFenceVerifier        contracts.ParticipantRouteFenceVerifier
	WorkAddressVerifier       contracts.ParticipantWorkAddressVerifier
	IdentityBindingVerifier   contracts.ParticipantIdentityBindingVerifier
	PlantDependencyVerifier   PlantDependencyVerifier
	UnlockDependencyVerifier  UnlockDependencyVerifier
	HarvestDependencyVerifier HarvestDependencyVerifier
	HarvestSourceVerifier     HarvestSourceVerifier
	TheftDependencyVerifier   TheftDependencyVerifier
	TheftSourceVerifier       TheftSourceVerifier
	CareDependencyVerifier    CareDependencyVerifier
	// BestEffortNoSourcePersistence accepts a validated in-memory SourceOrder
	// envelope for Coordinator calls. It never relaxes route fencing or Worker
	// recovery and is enabled only by dirty_best_effort composition.
	BestEffortNoSourcePersistence bool
	// CommitProofMode defaults to off. Shadow/prefer are explicit opt-ins;
	// Worker ResumeStep always verifies its durable Redis lease/order.
	CommitProofMode        sourceorder.CommitProofMode
	CommitProofValidator   *sourceorder.CommitProofValidator
	CommitProofSourceRealm string
	CommitProofMetrics     *CommitProofMetrics
	// RequireRouteLeaseProof enables the post-rollout strict route lease gate.
	// It is opt-in; compatibility mode still rejects partial or mismatched proof.
	RequireRouteLeaseProof bool
	// TaskConfigRepository is the immutable, versioned TaskConfigRepository
	// used by Care's TaskDomain call. Missing production configuration is a
	// Care admission failure; the old TaskInstance carrier is never a fallback.
	TaskConfigRepository playertask.Repository
	// TaskTimezone is the immutable game-timezone configuration used only to
	// classify a frozen CareResolutionSnapshot occurrence. Care reward amount
	// and eligibility never come from this option or any live configuration.
	TaskTimezone string
	// testHandler is intentionally unexported. Production composition cannot
	// inject a Handler or a look-alike ReceiptResolver; package-local tests may
	// use this seam to exercise verifier ordering and timeout behavior.
	testHandler Handler
	NowMs       func() int64
	Now         func() time.Time
}

// Server is the Player Participant implementation. The default Handler
// implements the three Plant Player steps, GrantHarvestReward, and
// ApplyCareReward; all other Player commands return gRPC Unimplemented
// without touching the registry.
type Server struct {
	contractspb.UnimplementedCoordinatorParticipantServiceServer
	contractspb.UnimplementedWorkerRecoveryIngressServer

	workVersion            WorkVersionVerifier
	workLease              WorkLeaseVerifier
	dependency             PlantDependencyVerifier
	unlockDependency       UnlockDependencyVerifier
	harvestDependency      HarvestDependencyVerifier
	harvestSource          HarvestSourceVerifier
	theftDependency        TheftDependencyVerifier
	theftSource            TheftSourceVerifier
	careDependency         CareDependencyVerifier
	commitProofMode        sourceorder.CommitProofMode
	commitProof            *sourceorder.CommitProofValidator
	commitProofSourceRealm string
	commitProofMetrics     *CommitProofMetrics
	registry               *playeractor.Registry
	ingress                contracts.ParticipantIngressOptions
	handler                Handler
	durableHandler         bool
	testOnlyHandler        bool
	configurationErr       error
}

var _ contractspb.CoordinatorParticipantServiceServer = (*Server)(nil)
var _ contractspb.WorkerRecoveryIngressServer = (*Server)(nil)

// NewServer creates a reusable Participant adapter. It does not alter any
// cmd composition root and does not create a second persistence path.
func NewServer(opts Options) *Server {
	workVersion := opts.WorkVersionVerifier
	if workVersion == nil {
		workVersion = rejectingWorkVersionVerifier{}
	}
	workLease := opts.WorkLeaseVerifier
	if workLease == nil {
		workLease = rejectingWorkLeaseVerifier{}
	}
	routeFence := opts.RouteFenceVerifier
	if routeFence == nil {
		routeFence = rejectingRouteFenceVerifier{}
	}
	workAddress := opts.WorkAddressVerifier
	if workAddress == nil {
		workAddress = rejectingWorkAddressVerifier{}
	}
	identityBinding := opts.IdentityBindingVerifier
	if identityBinding == nil {
		identityBinding = rejectingIdentityBindingVerifier{}
	}
	dependency := opts.PlantDependencyVerifier
	if dependency == nil {
		dependency = rejectingPlantDependencyVerifier{}
	}
	unlockDependency := opts.UnlockDependencyVerifier
	if unlockDependency == nil {
		unlockDependency = rejectingUnlockDependencyVerifier{}
	}
	careDependency := opts.CareDependencyVerifier
	if careDependency == nil {
		careDependency = rejectingCareDependencyVerifier{}
	}
	handler := opts.testHandler
	testOnlyHandler := handler != nil
	if handler == nil {
		handler = newPlayerHandler(opts.Registry, opts.NowMs, opts.TaskTimezone, opts.TaskConfigRepository)
	}
	var configurationErr error
	if _, ok := handler.(ReceiptResolver); !ok {
		configurationErr = errors.New("player participant durable receipt capability is not configured")
	}
	if err := opts.CommitProofMode.ValidatePreferRollout(); err != nil {
		configurationErr = err
	} else if opts.CommitProofMode == sourceorder.CommitProofModePrefer && (opts.CommitProofValidator == nil || opts.CommitProofSourceRealm == "") {
		configurationErr = errors.New("player participant prefer commit proof composition is incomplete")
	}
	commitProofMetrics := opts.CommitProofMetrics
	if commitProofMetrics == nil {
		commitProofMetrics = &CommitProofMetrics{}
	}
	if testOnlyHandler {
		if _, ok := handler.(testOnlyParticipantHandler); !ok && configurationErr == nil {
			configurationErr = errors.New("player participant custom Handler is test-only and must use the explicit test seam")
		}
	} else if opts.Registry == nil {
		configurationErr = errors.New("player participant PlayerActor Registry is not configured")
	} else if err := opts.Registry.ParticipantAdmissionError(); err != nil {
		configurationErr = fmt.Errorf("player participant durable authority capability: %w", err)
	}
	return &Server{
		workVersion:            workVersion,
		workLease:              workLease,
		dependency:             dependency,
		unlockDependency:       unlockDependency,
		harvestDependency:      opts.HarvestDependencyVerifier,
		harvestSource:          opts.HarvestSourceVerifier,
		theftDependency:        opts.TheftDependencyVerifier,
		theftSource:            opts.TheftSourceVerifier,
		careDependency:         careDependency,
		commitProofMode:        opts.CommitProofMode,
		commitProof:            opts.CommitProofValidator,
		commitProofSourceRealm: opts.CommitProofSourceRealm,
		commitProofMetrics:     commitProofMetrics,
		registry:               opts.Registry,
		ingress: contracts.ParticipantIngressOptions{
			RouteFenceVerifier:         routeFence,
			WorkAddressVerifier:        workAddress,
			IdentityBindingVerifier:    identityBinding,
			AllowBestEffortSourceOrder: opts.BestEffortNoSourcePersistence,
			RequirePhysicalProof:       true,
			RequireRouteLeaseProof:     opts.RequireRouteLeaseProof,
			Now:                        opts.Now,
		},
		handler:          handler,
		durableHandler:   !testOnlyHandler,
		testOnlyHandler:  testOnlyHandler,
		configurationErr: configurationErr,
	}
}

// NewServerChecked is the fail-fast constructor for composition roots. The
// legacy NewServer signature remains source-compatible and returns a server
// that reports the same configuration error before admission.
func NewServerChecked(opts Options) (*Server, error) {
	server := NewServer(opts)
	if server.testOnlyHandler {
		return nil, errors.New("player participant custom Handler injection is test-only")
	}
	if opts.CareDependencyVerifier == nil {
		return nil, errors.New("player participant Care source-order verifier is required")
	}
	if opts.TaskConfigRepository == nil {
		return nil, errors.New("player participant TaskConfigRepository is required")
	}
	if server.configurationErr != nil {
		return nil, server.configurationErr
	}
	return server, nil
}

// ExecuteStep validates the shared ingress, resolves an already-terminal
// receipt, and only then verifies WorkVersion for a first execution.
func (s *Server) ExecuteStep(ctx context.Context, req *contractspb.ParticipantRequest) (resp *contractspb.ParticipantResponse, err error) {
	if s == nil {
		return nil, status.Error(codes.Unavailable, "player participant server unavailable")
	}
	if s.configurationErr != nil {
		return nil, status.Error(codes.FailedPrecondition, s.configurationErr.Error())
	}
	call, err := contracts.ValidateAndDecodeWithOptions(ctx, req, contracts.ParticipantCallerCoordinator, s.ingress)
	if err != nil {
		return nil, normalizeParticipantError(err)
	}
	// Q1 WF-PARTICIPANT-INVOCATION-UNKNOWN-DIAG: player participant server
	// begin/end witness. Aligns with worker client begin/end via workflow_id +
	// invocation_id. Any server_begin without server_end => handler/Actor stuck.
	begin := time.Now()
	log.Printf("participant_rpc_server_begin player=%d workflow=%s type=%s step=%s invocation=%s op=%s version=%d budget_ms=%d caller=COORDINATOR",
		call.SubjectPlayerID, call.Work.GetWorkId(), call.Work.GetWorkType(), call.ParticipantStep,
		call.InvocationID, call.OperationID, call.ExpectedWorkVersion, call.RemainingBudgetMs)
	defer func() {
		outcome := ""
		if resp != nil {
			outcome = resp.GetOutcome().String()
		}
		log.Printf("participant_rpc_server_end player=%d workflow=%s step=%s invocation=%s duration_ms=%d grpc_code=%s outcome=%s replay=%v",
			call.SubjectPlayerID, call.Work.GetWorkId(), call.ParticipantStep, call.InvocationID,
			time.Since(begin).Milliseconds(), status.Code(err), outcome, resp != nil && resp.GetReplayedFromReceipt())
	}()
	if replay, found, err := s.resolveReceipt(ctx, call); err != nil {
		return nil, normalizeParticipantError(err)
	} else if found {
		return s.handleOutcome(call, replay)
	}
	// CareRewardOrder is proven by CareDependencyVerifier against Farm Authority
	// SourceOrder. Generic WorkRepository WorkVersion/WorkLease do not apply.
	if call.BestEffortSourceOrder == nil && !skipsGenericWorkRecordFence(call) {
		if s.workVersion == nil {
			return nil, status.Error(codes.FailedPrecondition, "player participant work version verifier is not configured")
		}
		proofCtx, cancel, err := contracts.ParticipantBudgetContext(ctx, call)
		if err != nil {
			return nil, err
		}
		err = s.workVersion.VerifyWorkVersion(proofCtx, call)
		cancel()
		if err != nil {
			if replay, found, resolveErr := s.resolveReceipt(ctx, call); resolveErr != nil {
				return nil, normalizeParticipantError(resolveErr)
			} else if found {
				return s.handleOutcome(call, replay)
			}
			return nil, normalizeParticipantError(err)
		}
	}
	if err := contracts.CheckParticipantBudget(ctx, call); err != nil {
		return nil, err
	}
	if err := s.admitFirstExecution(ctx, call); err != nil {
		if replay, found, resolveErr := s.resolveReceipt(ctx, call); resolveErr != nil {
			return nil, normalizeParticipantError(resolveErr)
		} else if found {
			return s.handleOutcome(call, replay)
		}
		return nil, normalizeParticipantError(err)
	}
	if err := contracts.CheckParticipantBudget(ctx, call); err != nil {
		return nil, err
	}
	return s.handle(ctx, call)
}

// ResumeStep has the same receipt-first behavior as ExecuteStep, then applies
// the Worker WorkLease verifier only for a first execution.
func (s *Server) ResumeStep(ctx context.Context, req *contractspb.ParticipantRequest) (resp *contractspb.ParticipantResponse, err error) {
	if s == nil {
		return nil, status.Error(codes.Unavailable, "player participant server unavailable")
	}
	if s.configurationErr != nil {
		return nil, status.Error(codes.FailedPrecondition, s.configurationErr.Error())
	}
	call, err := contracts.ValidateAndDecodeWithOptions(ctx, req, contracts.ParticipantCallerWorkerRecovery, s.ingress)
	if err != nil {
		return nil, normalizeParticipantError(err)
	}
	// Q1 WF-PARTICIPANT-INVOCATION-UNKNOWN-DIAG: server begin/end witness
	// (WORKER caller kind). Same alignment key as worker client begin/end.
	begin := time.Now()
	log.Printf("participant_rpc_server_begin player=%d workflow=%s type=%s step=%s invocation=%s op=%s version=%d budget_ms=%d caller=WORKER",
		call.SubjectPlayerID, call.Work.GetWorkId(), call.Work.GetWorkType(), call.ParticipantStep,
		call.InvocationID, call.OperationID, call.ExpectedWorkVersion, call.RemainingBudgetMs)
	defer func() {
		outcome := ""
		if resp != nil {
			outcome = resp.GetOutcome().String()
		}
		log.Printf("participant_rpc_server_end player=%d workflow=%s step=%s invocation=%s duration_ms=%d grpc_code=%s outcome=%s replay=%v",
			call.SubjectPlayerID, call.Work.GetWorkId(), call.ParticipantStep, call.InvocationID,
			time.Since(begin).Milliseconds(), status.Code(err), outcome, resp != nil && resp.GetReplayedFromReceipt())
	}()
	if replay, found, err := s.resolveReceipt(ctx, call); err != nil {
		return nil, normalizeParticipantError(err)
	} else if found {
		return s.handleOutcome(call, replay)
	}
	// Care worker lease/version fences live on Authority SourceOrder and are
	// proven by CareDependencyVerifier, not the generic Plant/Harvest WorkRecord.
	if !skipsGenericWorkRecordFence(call) {
		if s.workLease == nil {
			return nil, status.Error(codes.FailedPrecondition, "player participant work lease verifier is not configured")
		}
		proofCtx, cancel, err := contracts.ParticipantBudgetContext(ctx, call)
		if err != nil {
			return nil, err
		}
		err = s.workLease.VerifyWorkLease(proofCtx, call)
		cancel()
		if err != nil {
			if replay, found, resolveErr := s.resolveReceipt(ctx, call); resolveErr != nil {
				return nil, normalizeParticipantError(resolveErr)
			} else if found {
				return s.handleOutcome(call, replay)
			}
			return nil, normalizeParticipantError(err)
		}
		if s.workVersion == nil {
			return nil, status.Error(codes.FailedPrecondition, "player participant work version verifier is not configured")
		}
		proofCtx, cancel, err = contracts.ParticipantBudgetContext(ctx, call)
		if err != nil {
			return nil, err
		}
		err = s.workVersion.VerifyWorkVersion(proofCtx, call)
		cancel()
		if err != nil {
			if replay, found, resolveErr := s.resolveReceipt(ctx, call); resolveErr != nil {
				return nil, normalizeParticipantError(resolveErr)
			} else if found {
				return s.handleOutcome(call, replay)
			}
			return nil, normalizeParticipantError(err)
		}
	}
	if err := contracts.CheckParticipantBudget(ctx, call); err != nil {
		return nil, err
	}
	if err := s.admitFirstExecution(ctx, call); err != nil {
		if replay, found, resolveErr := s.resolveReceipt(ctx, call); resolveErr != nil {
			return nil, normalizeParticipantError(resolveErr)
		} else if found {
			return s.handleOutcome(call, replay)
		}
		return nil, normalizeParticipantError(err)
	}
	if err := contracts.CheckParticipantBudget(ctx, call); err != nil {
		return nil, err
	}
	return s.handle(ctx, call)
}

// admitFirstExecution contains all dependency proof work that can complete before
// the PlayerActor business turn. In particular, WorkRecord/Farm proof is
// never performed by the turn callback: a verifier may do network I/O here,
// while the callback is restricted to local validation, decision and the
// single authoritative commit.
func (s *Server) admitFirstExecution(ctx context.Context, call *contracts.ParticipantCall) error {
	var dependency func(context.Context, *contracts.ParticipantCall) error
	var validate func(*contracts.ParticipantCall) error
	bestEffort := call != nil && call.BestEffortSourceOrder != nil
	switch {
	case bestEffort:
		switch {
		case isHarvestPlayerCommand(call):
			validate = validateHarvestPayload
		case isTheftPlayerCommand(call):
			validate = validateTheftPayload
		case isCarePlayerCommand(call):
			if careHandler, ok := s.handler.(*plantHandler); ok {
				if careHandler.taskTimezoneErr != nil || careHandler.taskLocation == nil {
					return status.Error(codes.FailedPrecondition, "player participant Care task timezone is unavailable")
				}
				if careHandler.taskDomainErr != nil || careHandler.taskDomain == nil {
					return status.Error(codes.FailedPrecondition, "player participant Care TaskDomain configuration is unavailable")
				}
			}
			validate = validateCarePayload
		default:
			return status.Error(codes.FailedPrecondition, "player participant command does not support best-effort source admission")
		}
	case plantSourcePlacement(call):
		validate = validatePlantPayload
		if err := validate(call); err != nil {
			return err
		}
		return nil
	case unlockSourcePlacement(call):
		validate = validateUnlockPayload
		if err := validate(call); err != nil {
			return err
		}
		return nil
	case isPlantPlayerCommand(call):
		if s.dependency == nil {
			return status.Error(codes.FailedPrecondition, "player participant Farm dependency verifier is not configured")
		}
		dependency = s.dependency.VerifyPlantDependency
		validate = validatePlantPayload
	case isUnlockPlayerCommand(call):
		if s.unlockDependency == nil {
			return status.Error(codes.FailedPrecondition, "player participant UnlockPlot dependency verifier is not configured")
		}
		dependency = s.unlockDependency.VerifyUnlockDependency
		validate = validateUnlockPayload
	case isHarvestPlayerCommand(call) && sourcePlacement(call):
		if s.harvestSource == nil {
			return status.Error(codes.FailedPrecondition, "player participant Harvest SourceOrder verifier is not configured")
		}
		validate = validateHarvestPayload
		if err := validate(call); err != nil {
			return err
		}
		return s.verifyHarvestSource(ctx, call)
	case isHarvestPlayerCommand(call):
		if s.harvestDependency == nil {
			return status.Error(codes.FailedPrecondition, "player participant Harvest dependency verifier is not configured")
		}
		dependency = s.harvestDependency.VerifyHarvestDependency
		validate = validateHarvestPayload
	case isTheftPlayerCommand(call) && sourcePlacement(call):
		if s.theftSource == nil {
			return status.Error(codes.FailedPrecondition, "player participant Theft SourceOrder verifier is not configured")
		}
		validate = validateTheftPayload
		if err := validate(call); err != nil {
			return err
		}
		return s.theftSource.VerifyTheftSource(ctx, call)
	case isTheftPlayerCommand(call):
		if s.theftDependency == nil {
			return status.Error(codes.FailedPrecondition, "player participant Theft dependency verifier is not configured")
		}
		dependency = s.theftDependency.VerifyTheftDependency
		validate = validateTheftPayload
	case isCarePlayerCommand(call):
		if s.careDependency == nil {
			return status.Error(codes.FailedPrecondition, "player participant Care source-order verifier is not configured")
		}
		if careHandler, ok := s.handler.(*plantHandler); ok {
			if careHandler.taskTimezoneErr != nil || careHandler.taskLocation == nil {
				return status.Error(codes.FailedPrecondition, "player participant Care task timezone is unavailable")
			}
			if careHandler.taskDomainErr != nil || careHandler.taskDomain == nil {
				return status.Error(codes.FailedPrecondition, "player participant Care TaskDomain configuration is unavailable")
			}
		}
		validate = validateCarePayload
	default:
		return nil
	}
	if err := validate(call); err != nil {
		return err
	}
	var err error
	if bestEffort {
		// Exact source bindings were already validated by the shared contract
		// kernel. There is intentionally no Redis SourceOrder/DueIndex in this
		// experiment mode, so continue directly to destination capacity checks.
	} else if isCarePlayerCommand(call) {
		proof, proofErr := s.verifyCareDependency(ctx, call)
		if proofErr != nil {
			return proofErr
		}
		if err := validateCareDependencyProof(call, proof); err != nil {
			return err
		}
	} else if dependency != nil {
		proofCtx, cancel, err := contracts.ParticipantBudgetContext(ctx, call)
		if err != nil {
			return err
		}
		err = dependency(proofCtx, call)
		cancel()
		if err != nil {
			return err
		}
	} else if err := contracts.CheckParticipantBudget(ctx, call); err != nil {
		return err
	}
	if !s.testOnlyHandler && call.Spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR && call.TrustedResolvedAtMs <= 0 {
		return status.Error(codes.FailedPrecondition, "player participant Theft source resolved time is not proven")
	}
	if s.testOnlyHandler {
		return contracts.CheckParticipantBudget(ctx, call)
	}
	if !call.AdmissionCapacityKnown || call.AdmissionRecordVersion == 0 || s.registry == nil {
		return participantReceiptCapacityError()
	}
	estimate := playeringress.ParticipantCapacityEstimate{
		ReceiptItems:       call.AdmissionCandidateReceiptItems,
		ModelBytes:         call.AdmissionCandidateModelBytes,
		ReceiptBytes:       call.AdmissionCandidateReceiptBytes,
		SingleReceiptBytes: call.AdmissionCandidateSingleReceiptBytes,
	}
	if s.registry.ParticipantCapacityProjectionRequired() {
		if err := playeringress.CheckParticipantRecordCapacity(estimate); err != nil {
			return participantReceiptCapacityError()
		}
	}
	capacityOutcome := authoritystore.StoredOperationOutcome{Status: authoritystore.TerminalSucceeded}
	if isHarvestPlayerCommand(call) {
		capacityOutcome = harvestOperationResultCapacityEstimate()
	} else if isTheftPlayerCommand(call) {
		capacityOutcome = theftOperationResultCapacityEstimate(call.Spec.Command)
	} else if isCarePlayerCommand(call) {
		capacityOutcome = careOperationResultCapacityEstimate()
	}
	err = s.registry.CheckParticipantAuthorityCapacityWithOutcome(ctx, call.RouteHeader.GetActorId(), call.AdmissionRecordVersion, estimate.ModelBytes, capacityOutcome)
	if errors.Is(err, playeractor.ErrParticipantCapacityAdmissionStale) {
		// A different local commit may legitimately advance the record between
		// receipt lookup and dependency proof. Refresh once; missing/stale Store
		// usage remains ErrParticipantPhysicalCapacity and is never reconstructed.
		if _, found, resolveErr := s.resolveReceipt(ctx, call); resolveErr != nil {
			return resolveErr
		} else if found {
			return err
		}
		estimate = playeringress.ParticipantCapacityEstimate{
			ReceiptItems:       call.AdmissionCandidateReceiptItems,
			ModelBytes:         call.AdmissionCandidateModelBytes,
			ReceiptBytes:       call.AdmissionCandidateReceiptBytes,
			SingleReceiptBytes: call.AdmissionCandidateSingleReceiptBytes,
		}
		if s.registry.ParticipantCapacityProjectionRequired() {
			if recordErr := playeringress.CheckParticipantRecordCapacity(estimate); recordErr != nil {
				return participantReceiptCapacityError()
			}
		}
		err = s.registry.CheckParticipantAuthorityCapacityWithOutcome(ctx, call.RouteHeader.GetActorId(), call.AdmissionRecordVersion, estimate.ModelBytes, capacityOutcome)
	}
	if err != nil {
		return participantReceiptCapacityError()
	}
	return contracts.CheckParticipantBudget(ctx, call)
}

func (s *Server) resolveReceipt(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error) {
	resolver, ok := s.handler.(ReceiptResolver)
	if !ok {
		return nil, false, status.Error(codes.FailedPrecondition, "player participant durable receipt resolver is not configured")
	}
	lookupCtx, cancel, err := contracts.ParticipantBudgetContext(ctx, call)
	if err != nil {
		return nil, false, err
	}
	outcome, found, err := resolver.ResolveParticipantReceipt(lookupCtx, call)
	cancel()
	if err != nil {
		return nil, false, err
	}
	if found && outcome == nil {
		return nil, false, status.Error(codes.Internal, "player participant receipt resolver returned nil outcome")
	}
	if !found {
		if err := contracts.CheckParticipantBudget(ctx, call); err != nil {
			return nil, false, err
		}
	}
	return outcome, found, nil
}

func (s *Server) handle(ctx context.Context, call *contracts.ParticipantCall) (*contractspb.ParticipantResponse, error) {
	if s == nil || s.handler == nil {
		return nil, status.Error(codes.Unavailable, "player participant handler unavailable")
	}
	bounded, cancel, err := contracts.ParticipantBudgetContext(ctx, call)
	if err != nil {
		return nil, err
	}
	defer cancel()
	outcome, err := s.handler.Handle(bounded, call)
	if err != nil {
		return nil, normalizeParticipantError(err)
	}
	return s.handleOutcome(call, outcome)
}

type rejectingPlantDependencyVerifier struct{}

func (rejectingPlantDependencyVerifier) VerifyPlantDependency(context.Context, *contracts.ParticipantCall) error {
	return status.Error(codes.FailedPrecondition, "player participant Farm dependency verifier is not configured")
}

type rejectingCareDependencyVerifier struct{}

func (rejectingCareDependencyVerifier) VerifyCareDependency(context.Context, *contracts.ParticipantCall) (CareDependencyProof, error) {
	return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "player participant Care source-order verifier is not configured")
}

func (s *Server) verifyCareDependency(ctx context.Context, call *contracts.ParticipantCall) (CareDependencyProof, error) {
	var (
		statement  sourceorder.CommitProofStatement
		proofValid bool
	)
	if call != nil && call.CallerKind == contracts.ParticipantCallerCoordinator {
		var err error
		statement, proofValid, err = s.preferCommitProof(ctx, call)
		if err != nil {
			return CareDependencyProof{}, err
		}
		if s.commitProofMode == sourceorder.CommitProofModePrefer && proofValid {
			proof, proofErr := careDependencyProofFromCommitProof(call)
			if proofErr != nil {
				return CareDependencyProof{}, status.Error(codes.FailedPrecondition, "route-fenced CARE proof binding is invalid")
			}
			return proof, nil
		}
	}
	proofCtx, cancel, err := contracts.ParticipantBudgetContext(ctx, call)
	if err != nil {
		return CareDependencyProof{}, err
	}
	proof, err := s.careDependency.VerifyCareDependency(proofCtx, call)
	cancel()
	if err != nil {
		if call != nil && call.CallerKind == contracts.ParticipantCallerCoordinator && s.commitProofMode == sourceorder.CommitProofModeShadow {
			s.commitProofMetrics.observeDecision(call, commitProofDecisionShadowMismatch)
		}
		return CareDependencyProof{}, normalizeParticipantError(err)
	}
	if call != nil && call.CallerKind == contracts.ParticipantCallerCoordinator && s.commitProofMode == sourceorder.CommitProofModeShadow {
		decision := commitProofDecisionShadowMismatch
		if proofValid && shadowProofMatchesRedis(statement, call) {
			decision = commitProofDecisionShadowMatch
		}
		s.commitProofMetrics.observeDecision(call, decision)
	}
	if err := contracts.CheckParticipantBudget(ctx, call); err != nil {
		return CareDependencyProof{}, err
	}
	return proof, nil
}

// normalizeParticipantError is the single transport boundary for physical
// capacity failures. MemoryStore, Redis/Lua and any FaultStore wrapper all
// expose the same authoritystore.ErrCapacityExceeded sentinel, which must
// never leak as gRPC Unknown from a Participant RPC.
func normalizeParticipantError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "player participant deadline/budget exhausted")
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "player participant request canceled")
	}
	if errors.Is(err, authoritystore.ErrCapacityExceeded) || errors.Is(err, playeringress.ErrParticipantReceiptCapacity) {
		return participantReceiptCapacityError()
	}
	return err
}

func (s *Server) handleOutcome(call *contracts.ParticipantCall, outcome *contracts.StepOutcome) (*contractspb.ParticipantResponse, error) {
	if outcome == nil {
		return nil, status.Error(codes.Internal, "player participant handler returned nil outcome")
	}
	bestEffort := call != nil && call.BestEffortSourceOrder != nil
	if bestEffort && outcome.DurableReceiptCommitted {
		outcome.BestEffortReceiptAccepted = true
		outcome.DurableReceiptCommitted = false
	}
	response := &contractspb.ParticipantResponse{
		ParticipantCommand:      call.Spec.Command,
		ParticipantInvocationId: call.InvocationID,
		Outcome:                 outcome.Outcome,
		ReplayedFromReceipt:     outcome.ReplayedFromReceipt,
		Failure:                 cloneFailure(outcome.Failure),
		CommittedRecordVersion:  outcome.CommittedRecordVersion,
	}
	if outcome.CreatedSourceOrder != nil {
		response.CreatedSourceOrder = proto.Clone(outcome.CreatedSourceOrder).(*contractspb.CreatedSourceOrderEnvelope)
	}
	switch outcome.Outcome {
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
		contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED,
		contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE:
		if bestEffort && !outcome.BestEffortReceiptAccepted {
			return nil, status.Error(codes.Internal, "player participant terminal outcome lacks best-effort receipt proof")
		}
		if !bestEffort && !outcome.DurableReceiptCommitted {
			return nil, status.Error(codes.Internal, "player participant terminal outcome lacks durable receipt proof")
		}
		if outcome.Outcome == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED && outcome.Result == nil {
			return nil, status.Error(codes.Internal, "player participant APPLIED outcome requires typed result")
		}
		typedResult, resultHash, err := contracts.ValidateTypedResult(call.Spec, outcome.Result)
		if err != nil {
			return nil, err
		}
		if len(outcome.CommittedResultHash) != sha256.Size || !bytes.Equal(resultHash, outcome.CommittedResultHash) {
			return nil, status.Error(codes.Internal, "player participant terminal result does not match committed result proof")
		}
		response.TypedResult = typedResult
		response.ParticipantResultHash = append([]byte(nil), outcome.CommittedResultHash...)
		if outcome.CommittedRecordVersion == 0 {
			return nil, status.Error(codes.Internal, "player participant missing committed record version")
		}
		return response, nil
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN:
		// TIMEOUT_UNKNOWN is deliberately response-visible but never carries a
		// terminal result/version. The handler must not have written a receipt.
		if outcome.Result != nil || outcome.CommittedRecordVersion != 0 || outcome.ReplayedFromReceipt {
			return nil, status.Error(codes.Internal, "player participant timeout outcome carried terminal data")
		}
		return response, nil
	default:
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("player participant invalid outcome %d", outcome.Outcome))
	}
}

type rejectingWorkVersionVerifier struct{}

func (rejectingWorkVersionVerifier) VerifyWorkVersion(context.Context, *contracts.ParticipantCall) error {
	return status.Error(codes.FailedPrecondition, "player participant work version verifier is not configured")
}

type rejectingWorkLeaseVerifier struct{}

func (rejectingWorkLeaseVerifier) VerifyWorkLease(context.Context, *contracts.ParticipantCall) error {
	return status.Error(codes.FailedPrecondition, "player participant work lease verifier is not configured")
}

type rejectingUnlockDependencyVerifier struct{}

func (rejectingUnlockDependencyVerifier) VerifyUnlockDependency(context.Context, *contracts.ParticipantCall) error {
	return status.Error(codes.FailedPrecondition, "player participant UnlockPlot dependency verifier is not configured")
}

type rejectingRouteFenceVerifier struct{}

func (rejectingRouteFenceVerifier) VerifyRoute(context.Context, *contracts.ParticipantCall) error {
	return status.Error(codes.FailedPrecondition, "player participant route fence verifier is not configured")
}

type rejectingWorkAddressVerifier struct{}

func (rejectingWorkAddressVerifier) VerifyWorkAddress(context.Context, *contracts.ParticipantCall) error {
	return status.Error(codes.FailedPrecondition, "player participant WorkAddress bucket verifier is not configured")
}

type rejectingIdentityBindingVerifier struct{}

func (rejectingIdentityBindingVerifier) VerifyIdentityBinding(context.Context, *contracts.ParticipantCall) error {
	return status.Error(codes.FailedPrecondition, "player participant subject/operation binding verifier is not configured")
}

func cloneFailure(f *contractspb.ParticipantFailure) *contractspb.ParticipantFailure {
	if f == nil {
		return nil
	}
	return proto.Clone(f).(*contractspb.ParticipantFailure)
}
