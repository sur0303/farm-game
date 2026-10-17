// Package ingress contains the Farm-side Participant RPC adapter.  It owns
// request admission and registration only; FarmActor remains the sole
// business writer.
package ingress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/contracts"
	farmactor "farmgame/internal/farm/actor"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// WorkVersionVerifier proves the authoritative WorkRecord version before a
// first execution.  It runs outside the FarmActor mailbox and must honor the
// budget context supplied by the server.
type WorkVersionVerifier interface {
	VerifyWorkVersion(context.Context, *contracts.ParticipantCall) error
}

type WorkVersionVerifierFunc func(context.Context, *contracts.ParticipantCall) error

func (f WorkVersionVerifierFunc) VerifyWorkVersion(ctx context.Context, call *contracts.ParticipantCall) error {
	return f(ctx, call)
}

// WorkLeaseVerifier is the WorkerRecoveryIngress proof of the current lease,
// owner token, epoch, boot identity and trusted expiry.
type WorkLeaseVerifier interface {
	VerifyWorkLease(context.Context, *contracts.ParticipantCall) error
}

type WorkLeaseVerifierFunc func(context.Context, *contracts.ParticipantCall) error

func (f WorkLeaseVerifierFunc) VerifyWorkLease(ctx context.Context, call *contracts.ParticipantCall) error {
	return f(ctx, call)
}

// CommitPlantDependencyVerifier proves the immutable WorkRecord/Farm
// dependency snapshot before mailbox entry.  The verifier is the composition
// seam for Work status, Farm target/address, owner, reservation Result Hash,
// CropRuleSnapshot hash/version/full fields and the frozen lease binding.
// It must not write Farm authority.
type CommitPlantDependencyVerifier interface {
	VerifyCommitPlantDependency(context.Context, *contracts.ParticipantCall) error
}

type CommitPlantDependencyVerifierFunc func(context.Context, *contracts.ParticipantCall) error

func (f CommitPlantDependencyVerifierFunc) VerifyCommitPlantDependency(ctx context.Context, call *contracts.ParticipantCall) error {
	return f(ctx, call)
}

// CommitUnlockDependencyVerifier proves the durable UnlockPlot WorkRecord,
// including the prior Player reserve result hash, before FarmActor entry.
type CommitUnlockDependencyVerifier interface {
	VerifyUnlockDependency(context.Context, *contracts.ParticipantCall) error
}

type CommitUnlockDependencyVerifierFunc func(context.Context, *contracts.ParticipantCall) error

func (f CommitUnlockDependencyVerifierFunc) VerifyUnlockDependency(ctx context.Context, call *contracts.ParticipantCall) error {
	return f(ctx, call)
}

// CommitHarvestDependencyVerifier proves the immutable WorkRecord/Farm
// dependency snapshot before a COMMIT_HARVEST mailbox turn. The verifier must
// prove the frozen owner/plot/cycle/version binding and must fail closed when
// the physical Work/Farm proof is unavailable; it never writes Farm authority.
type CommitHarvestDependencyVerifier interface {
	VerifyCommitHarvestDependency(context.Context, *contracts.ParticipantCall) error
}

type CommitHarvestDependencyVerifierFunc func(context.Context, *contracts.ParticipantCall) error

func (f CommitHarvestDependencyVerifierFunc) VerifyCommitHarvestDependency(ctx context.Context, call *contracts.ParticipantCall) error {
	return f(ctx, call)
}

// DecideTheftDependencyVerifier proves the submitted Friend Action Admission
// plus the frozen Work/Route/subject/Execution-Accept binding before the Farm
// mailbox.  It never writes Farm authority.
type DecideTheftDependencyVerifier interface {
	VerifyDecideTheftDependency(context.Context, *contracts.ParticipantCall) error
}

type DecideTheftDependencyVerifierFunc func(context.Context, *contracts.ParticipantCall) error

func (f DecideTheftDependencyVerifierFunc) VerifyDecideTheftDependency(ctx context.Context, call *contracts.ParticipantCall) error {
	return f(ctx, call)
}

// DecideTheftRecoveryVerifier proves a source-placed DECIDE_THEFT directly
// against the FRIEND_PAIR dispatch order and its current recovery lease. The
// proof must bind WorkVersion, holder, boot, token, epoch, trusted expiry and
// the Pair DueIndex; it must not fall back to a destination WorkRecord that
// does not exist on the SourceOrder path.
type DecideTheftRecoveryVerifier interface {
	VerifyDecideTheftRecovery(context.Context, *contracts.ParticipantCall) error
}

type DecideTheftRecoveryVerifierFunc func(context.Context, *contracts.ParticipantCall) error

func (f DecideTheftRecoveryVerifierFunc) VerifyDecideTheftRecovery(ctx context.Context, call *contracts.ParticipantCall) error {
	return f(ctx, call)
}

// ReceiptResolver is deliberately separate from Handler: a terminal retry is
// allowed to replay its durable receipt before WorkVersion/Lease fencing.
type ReceiptResolver interface {
	ResolveParticipantReceipt(context.Context, *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error)
}

// CommitPlantObserver is the implementation-level bridge to the existing
// Farm ingress change delivery path.  It is called only after the FarmActor
// Participant mailbox turn and the local FarmSubscriptionHub handoff return;
// the observer owns any Gate/downlink work.
type CommitPlantObserver func(farmID uint64, action *farmactor.ActionResult)

// CommitHarvestObserver has the same post-mailbox delivery contract as the
// existing Plant observer. It is an alias so a composition root may reuse its
// one FarmChange delivery adapter for both typed Farm Participants.
type CommitHarvestObserver = CommitPlantObserver

// DecideTheftObserver is called only after the FarmActor turn has committed
// and the existing local FarmChange handoff has returned.
type DecideTheftObserver = CommitPlantObserver

// ApplyGuardSnapshotObserver hands a real guard-pet appearance change to the
// existing FarmChange delivery adapter after the GuardSync mailbox turn.
// Feed/expiry snapshots that do not change public appearance never call it.
type ApplyGuardSnapshotObserver = CommitPlantObserver

// Handler is the one business hook shared by ExecuteStep and ResumeStep.
type Handler interface {
	Handle(context.Context, *contracts.ParticipantCall) (*contracts.StepOutcome, error)
}

type HandlerFunc func(context.Context, *contracts.ParticipantCall) (*contracts.StepOutcome, error)

func (f HandlerFunc) Handle(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, error) {
	return f(ctx, call)
}

// testOnlyParticipantHandler is package-private by design.  Production
// composition cannot replace the durable FarmActor handler; package tests can
// use the explicit seam to test admission ordering without a store.
type testOnlyParticipantHandler interface {
	Handler
	ReceiptResolver
	participantTestOnly()
}

// Options describes the reusable Farm Participant server.  Nil physical
// verifiers are replaced with rejecting verifiers, so a partially composed
// server fails closed instead of silently accepting an unproven WorkRecord.
type Options struct {
	Registry                *farmactor.Registry
	WorkVersionVerifier     WorkVersionVerifier
	WorkLeaseVerifier       WorkLeaseVerifier
	RouteFenceVerifier      contracts.ParticipantRouteFenceVerifier
	WorkAddressVerifier     contracts.ParticipantWorkAddressVerifier
	IdentityBindingVerifier contracts.ParticipantIdentityBindingVerifier
	CommitPlantDependency   CommitPlantDependencyVerifier
	CommitUnlockDependency  CommitUnlockDependencyVerifier
	CommitHarvestDependency CommitHarvestDependencyVerifier
	DecideTheftDependency   DecideTheftDependencyVerifier
	DecideTheftRecovery     DecideTheftRecoveryVerifier
	// RequireRouteLeaseProof enables strict route lease proof after rollout.
	// Compatibility mode remains the default and still rejects partial proof.
	RequireRouteLeaseProof     bool
	CommitPlantObserver        CommitPlantObserver
	CommitHarvestObserver      CommitHarvestObserver
	DecideTheftObserver        DecideTheftObserver
	ApplyGuardSnapshotObserver ApplyGuardSnapshotObserver
	PlayerSourceVerifier       PlayerSourceOrderVerifier
	// BestEffortNoSourcePersistence accepts a validated in-memory SourceOrder
	// envelope for Coordinator calls. It is experiment-only; durable mode and
	// Worker recovery continue to require Redis source proofs.
	BestEffortNoSourcePersistence bool
	// testHandler is intentionally unexported.  It is only for package-local
	// ordering tests and is rejected by NewServerChecked.
	testHandler Handler
	NowMs       func() int64
	Now         func() time.Time
}

// Server implements both frozen Participant unary services and dispatches the
// registered Farm-owned steps through one shared handler for both caller
// contexts.
type Server struct {
	contractspb.UnimplementedCoordinatorParticipantServiceServer
	contractspb.UnimplementedWorkerRecoveryIngressServer

	workVersion       WorkVersionVerifier
	workLease         WorkLeaseVerifier
	plantDependency   CommitPlantDependencyVerifier
	unlockDependency  CommitUnlockDependencyVerifier
	harvestDependency CommitHarvestDependencyVerifier
	theftDependency   DecideTheftDependencyVerifier
	theftRecovery     DecideTheftRecoveryVerifier
	playerSource      PlayerSourceOrderVerifier
	registry          *farmactor.Registry
	ingress           contracts.ParticipantIngressOptions
	handler           Handler

	testOnlyHandler  bool
	configurationErr error
	theftLanes       *participantAdmissionLanes
}

const defaultTheftParticipantQueuePerFarm = 512

// participantAdmissionLanes serialize the complete DECIDE_THEFT ingress
// kernel for one Farm authority. The FarmActor mailbox already serializes the
// final mutation, but receipt lookup, dependency proof and capacity admission
// happen before mailbox entry. Letting hundreds of callers race those three
// phases makes almost every request observe a stale Farm record and retry.
//
// The lane is local to the Holder process, bounded per farm, and context
// cancellable. Actor routing guarantees that one Farm authority is served by
// one Holder lineage; writer_epoch remains the cross-process correctness
// fence during a handoff. Different farms use independent lanes.
type participantAdmissionLanes struct {
	mu       sync.Mutex
	maxQueue int
	lanes    map[uint64]*participantAdmissionLane
}

type participantAdmissionLane struct {
	token   chan struct{}
	waiters int
}

func newParticipantAdmissionLanes(maxQueue int) *participantAdmissionLanes {
	if maxQueue <= 0 {
		maxQueue = defaultTheftParticipantQueuePerFarm
	}
	return &participantAdmissionLanes{maxQueue: maxQueue, lanes: make(map[uint64]*participantAdmissionLane)}
}

func (l *participantAdmissionLanes) acquire(ctx context.Context, farmID uint64) (func(), error) {
	if l == nil || farmID == 0 {
		return nil, status.Error(codes.Unavailable, "FARM_THEFT_ADMISSION_LANE_UNAVAILABLE")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	l.mu.Lock()
	lane := l.lanes[farmID]
	if lane == nil {
		lane = &participantAdmissionLane{token: make(chan struct{}, 1)}
		lane.token <- struct{}{}
		l.lanes[farmID] = lane
	}
	if lane.waiters >= l.maxQueue {
		l.mu.Unlock()
		return nil, status.Error(codes.ResourceExhausted, "FARM_THEFT_ADMISSION_QUEUE_FULL")
	}
	lane.waiters++
	l.mu.Unlock()

	select {
	case <-lane.token:
	case <-ctx.Done():
		l.releaseWaiter(farmID, lane)
		return nil, ctx.Err()
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			lane.token <- struct{}{}
			l.releaseWaiter(farmID, lane)
		})
	}, nil
}

func (l *participantAdmissionLanes) releaseWaiter(farmID uint64, lane *participantAdmissionLane) {
	l.mu.Lock()
	if lane.waiters > 0 {
		lane.waiters--
	}
	if lane.waiters == 0 && l.lanes[farmID] == lane {
		delete(l.lanes, farmID)
	}
	l.mu.Unlock()
}

var _ contractspb.CoordinatorParticipantServiceServer = (*Server)(nil)
var _ contractspb.WorkerRecoveryIngressServer = (*Server)(nil)

// NewServer creates the reusable adapter without touching cmd/** or any
// composition root.  NewServerChecked is the fail-fast production form.
func NewServer(opts Options) *Server {
	workVersion := opts.WorkVersionVerifier
	if workVersion == nil {
		workVersion = rejectingFarmWorkVersionVerifier{}
	}
	workLease := opts.WorkLeaseVerifier
	if workLease == nil {
		workLease = rejectingFarmWorkLeaseVerifier{}
	}
	routeFence := opts.RouteFenceVerifier
	if routeFence == nil {
		routeFence = rejectingFarmRouteFenceVerifier{}
	}
	workAddress := opts.WorkAddressVerifier
	if workAddress == nil {
		workAddress = rejectingFarmWorkAddressVerifier{}
	}
	identityBinding := opts.IdentityBindingVerifier
	if identityBinding == nil {
		identityBinding = rejectingFarmIdentityBindingVerifier{}
	}
	dependency := opts.CommitPlantDependency
	if dependency == nil {
		dependency = rejectingCommitPlantDependencyVerifier{}
	}
	unlockDependency := opts.CommitUnlockDependency
	if unlockDependency == nil {
		unlockDependency = rejectingCommitUnlockDependencyVerifier{}
	}
	harvestDependency := opts.CommitHarvestDependency
	if harvestDependency == nil {
		harvestDependency = rejectingCommitHarvestDependencyVerifier{}
	}
	theftDependency := opts.DecideTheftDependency
	if theftDependency == nil {
		theftDependency = rejectingDecideTheftDependencyVerifier{}
	}
	theftRecovery := opts.DecideTheftRecovery
	if theftRecovery == nil {
		theftRecovery = rejectingDecideTheftRecoveryVerifier{}
	}

	handler := opts.testHandler
	testOnlyHandler := handler != nil
	if handler == nil {
		harvestObserver := opts.CommitHarvestObserver
		if harvestObserver == nil {
			// The existing FarmChange composition seam is safe to share while
			// callers migrate from the Plant-specific option.
			harvestObserver = opts.CommitPlantObserver
		}
		theftObserver := opts.DecideTheftObserver
		if theftObserver == nil {
			theftObserver = opts.CommitPlantObserver
		}
		guardObserver := opts.ApplyGuardSnapshotObserver
		if guardObserver == nil {
			guardObserver = opts.CommitPlantObserver
		}
		handler = newFarmParticipantHandler(opts.Registry, opts.NowMs, opts.CommitPlantObserver, harvestObserver, theftObserver, guardObserver)
	}

	var configurationErr error
	if _, ok := handler.(ReceiptResolver); !ok {
		configurationErr = errors.New("farm participant durable receipt capability is not configured")
	}
	if testOnlyHandler {
		if _, ok := handler.(testOnlyParticipantHandler); !ok && configurationErr == nil {
			configurationErr = errors.New("farm participant custom Handler is test-only and must use the explicit test seam")
		}
	} else if opts.Registry == nil {
		configurationErr = errors.New("farm participant FarmActor Registry is not configured")
	} else if err := opts.Registry.ParticipantAdmissionError(); err != nil {
		configurationErr = fmt.Errorf("farm participant durable authority capability: %w", err)
	}

	return &Server{
		workVersion:       workVersion,
		workLease:         workLease,
		plantDependency:   dependency,
		unlockDependency:  unlockDependency,
		harvestDependency: harvestDependency,
		theftDependency:   theftDependency,
		theftRecovery:     theftRecovery,
		playerSource:      opts.PlayerSourceVerifier,
		registry:          opts.Registry,
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
		testOnlyHandler:  testOnlyHandler,
		configurationErr: configurationErr,
		theftLanes:       newParticipantAdmissionLanes(defaultTheftParticipantQueuePerFarm),
	}
}

// NewServerChecked is the production constructor.  It refuses test handler
// injection and refuses a Registry that cannot expose physical authority
// capacity plus atomic receipt reads.
func NewServerChecked(opts Options) (*Server, error) {
	server := NewServer(opts)
	if server.testOnlyHandler {
		return nil, errors.New("farm participant custom Handler injection is test-only")
	}
	if server.configurationErr != nil {
		return nil, server.configurationErr
	}
	return server, nil
}

// ExecuteStep admits CoordinatorWorkContext.
func (s *Server) ExecuteStep(ctx context.Context, req *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error) {
	return s.serve(ctx, req, contracts.ParticipantCallerCoordinator)
}

// ResumeStep admits InternalCallerContext and proves the Work lease before a
// first execution.
func (s *Server) ResumeStep(ctx context.Context, req *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error) {
	return s.serve(ctx, req, contracts.ParticipantCallerWorkerRecovery)
}

func (s *Server) serve(ctx context.Context, req *contractspb.ParticipantRequest, caller contracts.ParticipantCallerKind) (*contractspb.ParticipantResponse, error) {
	if s == nil {
		return nil, status.Error(codes.Unavailable, "farm participant server unavailable")
	}
	if s.configurationErr != nil {
		return nil, status.Error(codes.FailedPrecondition, s.configurationErr.Error())
	}
	call, err := contracts.ValidateAndDecodeWithOptions(ctx, req, caller, s.ingress)
	if err != nil {
		return nil, normalizeFarmParticipantError(err)
	}
	if call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT &&
		call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK &&
		call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST &&
		call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT &&
		call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT {
		return nil, status.Error(codes.Unimplemented, "farm participant command is not implemented")
	}
	if call.Spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT {
		release, laneErr := s.theftLanes.acquire(ctx, call.RouteHeader.GetActorId())
		if laneErr != nil {
			return nil, normalizeFarmParticipantError(laneErr)
		}
		defer release()
	}
	if replay, found, err := s.resolveReceipt(ctx, call); err != nil {
		if errors.Is(err, blobstore.ErrTimeoutUnknown) {
			return s.handleOutcome(call, participantTimeoutUnknownOutcome())
		}
		return nil, normalizeFarmParticipantError(err)
	} else if found {
		return s.handleOutcome(call, replay)
	}

	if call.BestEffortSourceOrder != nil {
		// The shared contract kernel already proved exact WorkAddress,
		// WorkVersion, operation, subject, typed payload and canonical hash
		// equality with the in-memory source envelope. Keep business validation
		// here, but deliberately do not read a SourceOrder that dirty mode never
		// persisted.
		switch call.Spec.Command {
		case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT:
			err = farmactor.ValidateCommitPlantPayload(call)
		case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK:
			err = farmactor.ValidateCommitPlotUnlockPayload(call)
		case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT:
			err = farmactor.ValidateApplyGuardSnapshotPayload(call)
		default:
			err = status.Error(codes.FailedPrecondition, "farm participant command does not support best-effort source admission")
		}
		if err != nil {
			return nil, normalizeFarmParticipantError(err)
		}
	} else if skipsDecideTheftWorkRecordFence(call) {
		if caller == contracts.ParticipantCallerWorkerRecovery {
			if err := farmactor.ValidateDecideTheftPayload(call); err != nil {
				return nil, normalizeFarmParticipantError(err)
			}
			if err := s.verifyDecideTheftRecovery(ctx, call); err != nil {
				return nil, normalizeFarmParticipantError(err)
			}
		} else if err := s.verifyDependency(ctx, call); err != nil {
			return nil, normalizeFarmParticipantError(err)
		}
	} else if skipsFarmWorkRecordFence(call) {
		if s.playerSource == nil {
			return nil, status.Error(codes.FailedPrecondition, "farm participant Player SourceOrder verifier is not configured")
		}
		if err := farmactor.ValidateCommitPlantPayload(call); err != nil &&
			call.Spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT {
			return nil, normalizeFarmParticipantError(err)
		}
		if call.Spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK {
			if err := farmactor.ValidateCommitPlotUnlockPayload(call); err != nil {
				return nil, normalizeFarmParticipantError(err)
			}
		}
		if err := s.playerSource.VerifyPlayerSource(ctx, call); err != nil {
			return nil, normalizeFarmParticipantError(err)
		}
	} else if caller == contracts.ParticipantCallerCoordinator {
		if err := s.verifyWorkVersion(ctx, call); err != nil {
			return nil, normalizeFarmParticipantError(err)
		}
		if err := s.verifyDependency(ctx, call); err != nil {
			return nil, normalizeFarmParticipantError(err)
		}
	} else {
		if err := s.verifyWorkLease(ctx, call); err != nil {
			return nil, normalizeFarmParticipantError(err)
		}
		if err := s.verifyWorkVersion(ctx, call); err != nil {
			return nil, normalizeFarmParticipantError(err)
		}
		if err := s.verifyDependency(ctx, call); err != nil {
			return nil, normalizeFarmParticipantError(err)
		}
	}
	if err := s.admitCapacity(ctx, call); err != nil {
		if replay, found, resolveErr := s.resolveReceipt(ctx, call); resolveErr != nil {
			if errors.Is(resolveErr, blobstore.ErrTimeoutUnknown) {
				return s.handleOutcome(call, participantTimeoutUnknownOutcome())
			}
			return nil, normalizeFarmParticipantError(resolveErr)
		} else if found {
			return s.handleOutcome(call, replay)
		}
		if errors.Is(err, blobstore.ErrTimeoutUnknown) {
			return s.handleOutcome(call, participantTimeoutUnknownOutcome())
		}
		return nil, normalizeFarmParticipantError(err)
	}
	if err := contracts.CheckParticipantBudget(ctx, call); err != nil {
		return nil, normalizeFarmParticipantError(err)
	}
	return s.handle(ctx, call)
}

func (s *Server) verifyWorkVersion(ctx context.Context, call *contracts.ParticipantCall) error {
	if s.workVersion == nil {
		return status.Error(codes.FailedPrecondition, "farm participant WorkVersion verifier is not configured")
	}
	proofCtx, cancel, err := contracts.ParticipantBudgetContext(ctx, call)
	if err != nil {
		return err
	}
	err = s.workVersion.VerifyWorkVersion(proofCtx, call)
	cancel()
	if err != nil {
		return err
	}
	return contracts.CheckParticipantBudget(ctx, call)
}

func (s *Server) verifyWorkLease(ctx context.Context, call *contracts.ParticipantCall) error {
	if s.workLease == nil {
		return status.Error(codes.FailedPrecondition, "farm participant WorkLease verifier is not configured")
	}
	proofCtx, cancel, err := contracts.ParticipantBudgetContext(ctx, call)
	if err != nil {
		return err
	}
	err = s.workLease.VerifyWorkLease(proofCtx, call)
	cancel()
	if err != nil {
		return err
	}
	return contracts.CheckParticipantBudget(ctx, call)
}

func (s *Server) verifyDecideTheftRecovery(ctx context.Context, call *contracts.ParticipantCall) error {
	if s.theftRecovery == nil {
		return status.Error(codes.FailedPrecondition, "farm participant Pair Theft recovery verifier is not configured")
	}
	proofCtx, cancel, err := contracts.ParticipantBudgetContext(ctx, call)
	if err != nil {
		return err
	}
	err = s.theftRecovery.VerifyDecideTheftRecovery(proofCtx, call)
	cancel()
	if err != nil {
		return err
	}
	return contracts.CheckParticipantBudget(ctx, call)
}

func (s *Server) verifyDependency(ctx context.Context, call *contracts.ParticipantCall) error {
	if call.Spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK {
		if err := farmactor.ValidateCommitPlotUnlockPayload(call); err != nil {
			return err
		}
		if s.unlockDependency == nil {
			return status.Error(codes.FailedPrecondition, "farm participant UnlockPlot dependency verifier is not configured")
		}
		proofCtx, cancel, err := contracts.ParticipantBudgetContext(ctx, call)
		if err != nil {
			return err
		}
		err = s.unlockDependency.VerifyUnlockDependency(proofCtx, call)
		cancel()
		if err != nil {
			return err
		}
		return contracts.CheckParticipantBudget(ctx, call)
	}
	if call.Spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT {
		return farmactor.ValidateApplyGuardSnapshotPayload(call)
	}
	if call.Spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT {
		if s.theftDependency == nil {
			return status.Error(codes.FailedPrecondition, "farm participant Theft dependency verifier is not configured")
		}
		if err := farmactor.ValidateDecideTheftPayload(call); err != nil {
			return err
		}
		proofCtx, cancel, err := contracts.ParticipantBudgetContext(ctx, call)
		if err != nil {
			return err
		}
		err = s.theftDependency.VerifyDecideTheftDependency(proofCtx, call)
		cancel()
		if err != nil {
			return err
		}
		return contracts.CheckParticipantBudget(ctx, call)
	}
	if call.Spec.Command == contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST {
		if s.harvestDependency == nil {
			return status.Error(codes.FailedPrecondition, "farm participant Harvest dependency verifier is not configured")
		}
		if err := farmactor.ValidateCommitHarvestPayload(call); err != nil {
			return err
		}
		proofCtx, cancel, err := contracts.ParticipantBudgetContext(ctx, call)
		if err != nil {
			return err
		}
		err = s.harvestDependency.VerifyCommitHarvestDependency(proofCtx, call)
		cancel()
		if err != nil {
			return err
		}
		return contracts.CheckParticipantBudget(ctx, call)
	}
	if s.plantDependency == nil {
		return status.Error(codes.FailedPrecondition, "farm participant Work/Farm dependency verifier is not configured")
	}
	// Business payload validation is intentionally after receipt-first lookup:
	// a used invocation with a different canonical hash must report the stable
	// idempotency conflict even if the retry also carries bad business fields.
	if err := farmactor.ValidateCommitPlantPayload(call); err != nil {
		return err
	}
	proofCtx, cancel, err := contracts.ParticipantBudgetContext(ctx, call)
	if err != nil {
		return err
	}
	err = s.plantDependency.VerifyCommitPlantDependency(proofCtx, call)
	cancel()
	if err != nil {
		return err
	}
	return contracts.CheckParticipantBudget(ctx, call)
}

func (s *Server) resolveReceipt(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error) {
	resolver, ok := s.handler.(ReceiptResolver)
	if !ok {
		return nil, false, status.Error(codes.FailedPrecondition, "farm participant durable receipt resolver is not configured")
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
		return nil, false, status.Error(codes.Internal, "farm participant receipt resolver returned nil outcome")
	}
	if !found {
		if err := contracts.CheckParticipantBudget(ctx, call); err != nil {
			return nil, false, err
		}
	}
	return outcome, found, nil
}

// admitCapacity is the last mailbox-free proof.  The authoritative read that
// populated AdmissionRecordVersion happens in resolveReceipt; a stale version
// receives one fresh read and is never guessed from model bytes.
func (s *Server) admitCapacity(ctx context.Context, call *contracts.ParticipantCall) error {
	if s.testOnlyHandler {
		return contracts.CheckParticipantBudget(ctx, call)
	}
	if s.registry == nil || !call.AdmissionCapacityKnown || call.AdmissionRecordVersion == 0 ||
		(s.registry.ParticipantCapacityProjectionRequired() && call.AdmissionCandidateModelBytes <= 0) {
		return farmactor.ErrParticipantCapacityUnavailable
	}
	err := s.registry.CheckParticipantAuthorityCapacity(ctx, call.RouteHeader.GetActorId(), call.AdmissionRecordVersion, call.AdmissionCandidateModelBytes)
	if !errors.Is(err, farmactor.ErrParticipantCapacityAdmissionStale) {
		return err
	}
	if _, found, resolveErr := s.resolveReceipt(ctx, call); resolveErr != nil {
		return resolveErr
	} else if found {
		return farmactor.ErrParticipantCapacityAdmissionStale
	}
	return s.registry.CheckParticipantAuthorityCapacity(ctx, call.RouteHeader.GetActorId(), call.AdmissionRecordVersion, call.AdmissionCandidateModelBytes)
}

func (s *Server) handle(ctx context.Context, call *contracts.ParticipantCall) (*contractspb.ParticipantResponse, error) {
	if s == nil || s.handler == nil {
		return nil, status.Error(codes.Unavailable, "farm participant handler unavailable")
	}
	bounded, cancel, err := contracts.ParticipantBudgetContext(ctx, call)
	if err != nil {
		return nil, normalizeFarmParticipantError(err)
	}
	defer cancel()
	outcome, err := s.handler.Handle(bounded, call)
	if err != nil {
		return nil, normalizeFarmParticipantError(err)
	}
	return s.handleOutcome(call, outcome)
}

func (s *Server) handleOutcome(call *contracts.ParticipantCall, outcome *contracts.StepOutcome) (*contractspb.ParticipantResponse, error) {
	if outcome == nil {
		return nil, status.Error(codes.Internal, "farm participant handler returned nil outcome")
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
		Failure:                 cloneFarmParticipantFailure(outcome.Failure),
		CommittedRecordVersion:  outcome.CommittedRecordVersion,
	}
	switch outcome.Outcome {
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED,
		contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_REJECTED,
		contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_NO_CHANGE:
		if bestEffort && !outcome.BestEffortReceiptAccepted {
			return nil, status.Error(codes.Internal, "farm participant terminal outcome lacks best-effort receipt proof")
		}
		if !bestEffort && !outcome.DurableReceiptCommitted {
			return nil, status.Error(codes.Internal, "farm participant terminal outcome lacks durable receipt proof")
		}
		if outcome.Outcome == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED && outcome.Result == nil {
			return nil, status.Error(codes.Internal, "farm participant APPLIED outcome requires typed result")
		}
		typedResult, resultHash, err := contracts.ValidateTypedResult(call.Spec, outcome.Result)
		if err != nil {
			return nil, err
		}
		if len(outcome.CommittedResultHash) != sha256.Size || !bytes.Equal(resultHash, outcome.CommittedResultHash) {
			return nil, status.Error(codes.Internal, "farm participant terminal result does not match durable result proof")
		}
		if outcome.CommittedRecordVersion == 0 {
			return nil, status.Error(codes.Internal, "farm participant missing committed record version")
		}
		response.TypedResult = typedResult
		response.ParticipantResultHash = append([]byte(nil), outcome.CommittedResultHash...)
		return response, nil
	case contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN:
		if outcome.Result != nil || outcome.CommittedRecordVersion != 0 || outcome.ReplayedFromReceipt || len(outcome.CommittedResultHash) != 0 {
			return nil, status.Error(codes.Internal, "farm participant timeout outcome carried terminal data")
		}
		return response, nil
	default:
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("farm participant invalid outcome %d", outcome.Outcome))
	}
}

func participantTimeoutUnknownOutcome() *contracts.StepOutcome {
	return &contracts.StepOutcome{
		Outcome: contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_TIMEOUT_UNKNOWN,
		Failure: &contractspb.ParticipantFailure{
			Error:                    &logicrpcpb.ErrorDetail{ErrorCode: commonpb.ErrorCode_ERR_TIMEOUT},
			RetryableAfterResolution: true,
		},
	}
}

type commitPlantHandler struct {
	registry *farmactor.Registry
	nowMs    func() int64
	observer CommitPlantObserver
}

type commitPlotUnlockHandler struct {
	registry *farmactor.Registry
	nowMs    func() int64
	observer CommitPlantObserver
}

func newCommitPlotUnlockHandler(registry *farmactor.Registry, nowMs func() int64, observer CommitPlantObserver) *commitPlotUnlockHandler {
	if nowMs == nil {
		nowMs = func() int64 { return time.Now().UnixMilli() }
	}
	return &commitPlotUnlockHandler{registry: registry, nowMs: nowMs, observer: observer}
}

func (h *commitPlotUnlockHandler) ResolveParticipantReceipt(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error) {
	if call == nil || call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK {
		return nil, false, nil
	}
	if h == nil || h.registry == nil {
		return nil, false, status.Error(codes.Unavailable, "farm actor registry unavailable")
	}
	record, recordVersion, err := h.registry.ReadOutsideMailbox(ctx, call.RouteHeader.GetActorId())
	if err != nil {
		switch {
		case errors.Is(err, blobstore.ErrWriterEpochMismatch):
			return nil, false, status.Error(codes.FailedPrecondition, "WRITER_EPOCH_MISMATCH")
		case errors.Is(err, blobstore.ErrUnavailable):
			return nil, false, status.Error(codes.Unavailable, "FARM_AUTHORITY_UNAVAILABLE")
		default:
			return nil, false, err
		}
	}
	outcome, found, err := farmactor.ResolveCommitPlotUnlockParticipantReceipt(record, recordVersion, call)
	if err != nil || found {
		return outcome, found, err
	}
	modelBytes := 0
	if h.registry.ParticipantCapacityProjectionRequired() {
		modelBytes, err = farmactor.EstimateParticipantCommitPlotUnlockModelBytes(record)
		if err != nil {
			return nil, false, err
		}
	}
	call.AdmissionCapacityKnown = true
	call.AdmissionRecordVersion = recordVersion
	call.AdmissionCandidateModelBytes = modelBytes
	return nil, false, nil
}

func (h *commitPlotUnlockHandler) Handle(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, error) {
	if call == nil || call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK {
		return nil, status.Error(codes.Unimplemented, "farm participant command is not implemented")
	}
	if h == nil || h.registry == nil {
		return nil, status.Error(codes.Unavailable, "farm actor registry unavailable")
	}
	value, err := h.registry.SubmitParticipant(ctx, call.RouteHeader.GetActorId(), func(turnCtx context.Context, actor *farmactor.FarmActor, guard farmactor.ParticipantCapacityGuard) (any, error) {
		return actor.CommitPlotUnlockWithAction(turnCtx, call, guard, h.nowMs)
	})
	if err != nil {
		if errors.Is(err, blobstore.ErrTimeoutUnknown) {
			return participantTimeoutUnknownOutcome(), nil
		}
		return nil, err
	}
	turn, ok := value.(*farmactor.CommitPlotUnlockTurnResult)
	if !ok || turn == nil || turn.Outcome == nil {
		return nil, status.Error(codes.Internal, "farm participant CommitPlotUnlock returned an invalid outcome")
	}
	if h.observer != nil && shouldObserveCommitPlotUnlock(turn) {
		h.observer(call.RouteHeader.GetActorId(), turn.Action)
	}
	return turn.Outcome, nil
}

func shouldObserveCommitPlotUnlock(turn *farmactor.CommitPlotUnlockTurnResult) bool {
	return turn != nil && turn.Outcome != nil && turn.Outcome.Outcome == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED &&
		!turn.Outcome.ReplayedFromReceipt && turn.Action != nil && turn.Action.Change != nil
}

func newCommitPlantHandler(registry *farmactor.Registry, nowMs func() int64, observer CommitPlantObserver) Handler {
	if nowMs == nil {
		nowMs = func() int64 { return time.Now().UnixMilli() }
	}
	return &commitPlantHandler{registry: registry, nowMs: nowMs, observer: observer}
}

func (h *commitPlantHandler) ResolveParticipantReceipt(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error) {
	if call == nil || call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT {
		return nil, false, nil
	}
	if h == nil || h.registry == nil {
		return nil, false, status.Error(codes.Unavailable, "farm actor registry unavailable")
	}
	record, recordVersion, err := h.registry.ReadOutsideMailbox(ctx, call.RouteHeader.GetActorId())
	if err != nil {
		switch {
		case errors.Is(err, blobstore.ErrWriterEpochMismatch):
			return nil, false, status.Error(codes.FailedPrecondition, "WRITER_EPOCH_MISMATCH")
		case errors.Is(err, blobstore.ErrUnavailable):
			return nil, false, status.Error(codes.Unavailable, "FARM_AUTHORITY_UNAVAILABLE")
		default:
			return nil, false, err
		}
	}
	outcome, found, err := farmactor.ResolveCommitPlantParticipantReceipt(record, recordVersion, call)
	if err != nil || found {
		return outcome, found, err
	}
	modelBytes := 0
	if h.registry.ParticipantCapacityProjectionRequired() {
		modelBytes, err = farmactor.EstimateParticipantCommitPlantModelBytes(record)
		if err != nil {
			return nil, false, err
		}
	}
	call.AdmissionCapacityKnown = true
	call.AdmissionRecordVersion = recordVersion
	call.AdmissionCandidateModelBytes = modelBytes
	return nil, false, nil
}

func (h *commitPlantHandler) Handle(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, error) {
	if call == nil || call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT {
		return nil, status.Error(codes.Unimplemented, "farm participant command is not implemented")
	}
	if h == nil || h.registry == nil {
		return nil, status.Error(codes.Unavailable, "farm actor registry unavailable")
	}
	value, err := h.registry.SubmitParticipant(ctx, call.RouteHeader.GetActorId(), func(turnCtx context.Context, actor *farmactor.FarmActor, guard farmactor.ParticipantCapacityGuard) (any, error) {
		return actor.CommitPlantWithAction(turnCtx, call, guard, h.nowMs)
	})
	if err != nil {
		if errors.Is(err, blobstore.ErrTimeoutUnknown) {
			return participantTimeoutUnknownOutcome(), nil
		}
		return nil, err
	}
	turn, ok := value.(*farmactor.CommitPlantTurnResult)
	if !ok || turn == nil || turn.Outcome == nil {
		return nil, status.Error(codes.Internal, "farm participant CommitPlant returned an invalid outcome")
	}
	// SubmitParticipant has returned, so the Actor mailbox turn is complete.
	// CommitPlantWithAction already performed only the existing local,
	// serialized FarmSubscriptionHub handoff.  The observer may now enqueue
	// Gate/network downlink work; delivery failure cannot change the committed
	// Participant result.
	action := turn.Action
	if h.observer != nil && shouldObserveCommitPlant(turn) {
		h.observer(call.RouteHeader.GetActorId(), action)
	}
	return turn.Outcome, nil
}

func shouldObserveCommitPlant(turn *farmactor.CommitPlantTurnResult) bool {
	return turn != nil && turn.Outcome != nil &&
		turn.Outcome.Outcome == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED &&
		!turn.Outcome.ReplayedFromReceipt && turn.Action != nil && turn.Action.Change != nil
}

type farmParticipantHandler struct {
	plant     *commitPlantHandler
	unlock    *commitPlotUnlockHandler
	harvest   *commitHarvestHandler
	theft     *decideTheftHandler
	guardSync *applyGuardSnapshotHandler
}

func newFarmParticipantHandler(registry *farmactor.Registry, nowMs func() int64, plantObserver CommitPlantObserver, harvestObserver CommitHarvestObserver, theftObserver DecideTheftObserver, guardObserver ApplyGuardSnapshotObserver) Handler {
	return &farmParticipantHandler{
		plant:     newCommitPlantHandler(registry, nowMs, plantObserver).(*commitPlantHandler),
		unlock:    newCommitPlotUnlockHandler(registry, nowMs, plantObserver),
		harvest:   newCommitHarvestHandler(registry, nowMs, harvestObserver),
		theft:     newDecideTheftHandler(registry, nowMs, theftObserver),
		guardSync: newApplyGuardSnapshotHandler(registry, nowMs, guardObserver),
	}
}

func (h *farmParticipantHandler) Handle(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, error) {
	if h == nil || call == nil {
		return nil, status.Error(codes.InvalidArgument, "farm participant handler call is incomplete")
	}
	switch call.Spec.Command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK:
		return h.unlock.Handle(ctx, call)
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT:
		return h.plant.Handle(ctx, call)
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST:
		return h.harvest.Handle(ctx, call)
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT:
		return h.theft.Handle(ctx, call)
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT:
		return h.guardSync.Handle(ctx, call)
	default:
		return nil, status.Error(codes.Unimplemented, "farm participant command is not implemented")
	}
}

func (h *farmParticipantHandler) ResolveParticipantReceipt(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error) {
	if h == nil || call == nil {
		return nil, false, nil
	}
	switch call.Spec.Command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK:
		return h.unlock.ResolveParticipantReceipt(ctx, call)
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT:
		return h.plant.ResolveParticipantReceipt(ctx, call)
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST:
		return h.harvest.ResolveParticipantReceipt(ctx, call)
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT:
		return h.theft.ResolveParticipantReceipt(ctx, call)
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT:
		return h.guardSync.ResolveParticipantReceipt(ctx, call)
	default:
		return nil, false, nil
	}
}

type commitHarvestHandler struct {
	registry *farmactor.Registry
	nowMs    func() int64
	observer CommitHarvestObserver
}

func newCommitHarvestHandler(registry *farmactor.Registry, nowMs func() int64, observer CommitHarvestObserver) *commitHarvestHandler {
	if nowMs == nil {
		nowMs = func() int64 { return time.Now().UnixMilli() }
	}
	return &commitHarvestHandler{registry: registry, nowMs: nowMs, observer: observer}
}

func (h *commitHarvestHandler) ResolveParticipantReceipt(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error) {
	if call == nil || call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST {
		return nil, false, nil
	}
	if h == nil || h.registry == nil {
		return nil, false, status.Error(codes.Unavailable, "farm actor registry unavailable")
	}
	record, recordVersion, err := h.registry.ReadOutsideMailbox(ctx, call.RouteHeader.GetActorId())
	if err != nil {
		switch {
		case errors.Is(err, blobstore.ErrWriterEpochMismatch):
			return nil, false, status.Error(codes.FailedPrecondition, "WRITER_EPOCH_MISMATCH")
		case errors.Is(err, blobstore.ErrUnavailable):
			return nil, false, status.Error(codes.Unavailable, "FARM_AUTHORITY_UNAVAILABLE")
		default:
			return nil, false, err
		}
	}
	outcome, found, err := farmactor.ResolveCommitHarvestParticipantReceipt(record, recordVersion, call)
	if err != nil || found {
		return outcome, found, err
	}
	modelBytes := 0
	if h.registry.ParticipantCapacityProjectionRequired() {
		modelBytes, err = farmactor.EstimateParticipantCommitHarvestModelBytes(record)
		if err != nil {
			return nil, false, err
		}
	}
	call.AdmissionCapacityKnown = true
	call.AdmissionRecordVersion = recordVersion
	call.AdmissionCandidateModelBytes = modelBytes
	return nil, false, nil
}

func (h *commitHarvestHandler) Handle(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, error) {
	if call == nil || call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST {
		return nil, status.Error(codes.Unimplemented, "farm participant command is not implemented")
	}
	if h == nil || h.registry == nil {
		return nil, status.Error(codes.Unavailable, "farm actor registry unavailable")
	}
	value, err := h.registry.SubmitParticipant(ctx, call.RouteHeader.GetActorId(), func(turnCtx context.Context, actor *farmactor.FarmActor, guard farmactor.ParticipantCapacityGuard) (any, error) {
		return actor.CommitHarvestWithAction(turnCtx, call, guard, h.nowMs)
	})
	if err != nil {
		if errors.Is(err, blobstore.ErrTimeoutUnknown) {
			return participantTimeoutUnknownOutcome(), nil
		}
		return nil, err
	}
	turn, ok := value.(*farmactor.CommitHarvestTurnResult)
	if !ok || turn == nil || turn.Outcome == nil {
		return nil, status.Error(codes.Internal, "farm participant CommitHarvest returned an invalid outcome")
	}
	if h.observer != nil && shouldObserveCommitHarvest(turn) {
		h.observer(call.RouteHeader.GetActorId(), turn.Action)
	}
	return turn.Outcome, nil
}

func shouldObserveCommitHarvest(turn *farmactor.CommitHarvestTurnResult) bool {
	return turn != nil && turn.Outcome != nil &&
		turn.Outcome.Outcome == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED &&
		!turn.Outcome.ReplayedFromReceipt && turn.Action != nil && turn.Action.Change != nil
}

type decideTheftHandler struct {
	registry *farmactor.Registry
	nowMs    func() int64
	observer DecideTheftObserver
}

func newDecideTheftHandler(registry *farmactor.Registry, nowMs func() int64, observer DecideTheftObserver) *decideTheftHandler {
	if nowMs == nil {
		nowMs = func() int64 { return time.Now().UnixMilli() }
	}
	return &decideTheftHandler{registry: registry, nowMs: nowMs, observer: observer}
}

func (h *decideTheftHandler) ResolveParticipantReceipt(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error) {
	if call == nil || call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT {
		return nil, false, nil
	}
	if h == nil || h.registry == nil {
		return nil, false, status.Error(codes.Unavailable, "farm actor registry unavailable")
	}
	record, recordVersion, err := h.registry.ReadOutsideMailbox(ctx, call.RouteHeader.GetActorId())
	if err != nil {
		switch {
		case errors.Is(err, blobstore.ErrWriterEpochMismatch):
			return nil, false, status.Error(codes.FailedPrecondition, "WRITER_EPOCH_MISMATCH")
		case errors.Is(err, blobstore.ErrUnavailable):
			return nil, false, status.Error(codes.Unavailable, "FARM_AUTHORITY_UNAVAILABLE")
		default:
			return nil, false, err
		}
	}
	outcome, found, err := farmactor.ResolveDecideTheftParticipantReceipt(record, recordVersion, call)
	if err != nil || found {
		return outcome, found, err
	}
	modelBytes := 0
	if h.registry.ParticipantCapacityProjectionRequired() {
		modelBytes, err = farmactor.EstimateParticipantDecideTheftModelBytes(record)
		if err != nil {
			return nil, false, err
		}
	}
	call.AdmissionCapacityKnown = true
	call.AdmissionRecordVersion = recordVersion
	call.AdmissionCandidateModelBytes = modelBytes
	return nil, false, nil
}

func (h *decideTheftHandler) Handle(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, error) {
	if call == nil || call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT {
		return nil, status.Error(codes.Unimplemented, "farm participant command is not implemented")
	}
	if h == nil || h.registry == nil {
		return nil, status.Error(codes.Unavailable, "farm actor registry unavailable")
	}
	value, err := h.registry.SubmitParticipant(ctx, call.RouteHeader.GetActorId(), func(turnCtx context.Context, actor *farmactor.FarmActor, guard farmactor.ParticipantCapacityGuard) (any, error) {
		return actor.DecideTheftWithAction(turnCtx, call, guard, h.nowMs)
	})
	if err != nil {
		if errors.Is(err, blobstore.ErrTimeoutUnknown) {
			return participantTimeoutUnknownOutcome(), nil
		}
		return nil, err
	}
	turn, ok := value.(*farmactor.DecideTheftTurnResult)
	if !ok || turn == nil || turn.Outcome == nil {
		return nil, status.Error(codes.Internal, "farm participant DECIDE_THEFT returned an invalid outcome")
	}
	if h.observer != nil && shouldObserveDecideTheft(turn) {
		h.observer(call.RouteHeader.GetActorId(), turn.Action)
	}
	return turn.Outcome, nil
}

func shouldObserveDecideTheft(turn *farmactor.DecideTheftTurnResult) bool {
	return turn != nil && turn.Outcome != nil &&
		turn.Outcome.Outcome == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED &&
		!turn.Outcome.ReplayedFromReceipt && turn.Action != nil && turn.Action.Change != nil
}

type applyGuardSnapshotHandler struct {
	registry *farmactor.Registry
	nowMs    func() int64
	observer ApplyGuardSnapshotObserver
}

func newApplyGuardSnapshotHandler(registry *farmactor.Registry, nowMs func() int64, observer ApplyGuardSnapshotObserver) *applyGuardSnapshotHandler {
	if nowMs == nil {
		nowMs = func() int64 { return time.Now().UnixMilli() }
	}
	return &applyGuardSnapshotHandler{registry: registry, nowMs: nowMs, observer: observer}
}

func (h *applyGuardSnapshotHandler) ResolveParticipantReceipt(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, bool, error) {
	if call == nil || call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT {
		return nil, false, nil
	}
	if h == nil || h.registry == nil {
		return nil, false, status.Error(codes.Unavailable, "farm actor registry unavailable")
	}
	record, recordVersion, err := h.registry.ReadOutsideMailbox(ctx, call.RouteHeader.GetActorId())
	if err != nil {
		switch {
		case errors.Is(err, blobstore.ErrWriterEpochMismatch):
			return nil, false, status.Error(codes.FailedPrecondition, "WRITER_EPOCH_MISMATCH")
		case errors.Is(err, blobstore.ErrUnavailable):
			return nil, false, status.Error(codes.Unavailable, "FARM_AUTHORITY_UNAVAILABLE")
		default:
			return nil, false, err
		}
	}
	outcome, found, err := farmactor.ResolveApplyGuardSnapshotParticipantReceipt(record, recordVersion, call)
	if err != nil || found {
		return outcome, found, err
	}
	modelBytes := 0
	if h.registry.ParticipantCapacityProjectionRequired() {
		modelBytes, err = farmactor.EstimateParticipantApplyGuardSnapshotModelBytes(record)
		if err != nil {
			return nil, false, err
		}
	}
	call.AdmissionCapacityKnown = true
	call.AdmissionRecordVersion = recordVersion
	call.AdmissionCandidateModelBytes = modelBytes
	return nil, false, nil
}

func (h *applyGuardSnapshotHandler) Handle(ctx context.Context, call *contracts.ParticipantCall) (*contracts.StepOutcome, error) {
	if call == nil || call.Spec.Command != contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_APPLY_GUARD_SNAPSHOT {
		return nil, status.Error(codes.Unimplemented, "farm participant command is not implemented")
	}
	if h == nil || h.registry == nil {
		return nil, status.Error(codes.Unavailable, "farm actor registry unavailable")
	}
	value, err := h.registry.SubmitParticipant(ctx, call.RouteHeader.GetActorId(), func(turnCtx context.Context, actor *farmactor.FarmActor, guard farmactor.ParticipantCapacityGuard) (any, error) {
		return actor.ApplyGuardSnapshotWithAction(turnCtx, call, guard, h.nowMs)
	})
	if err != nil {
		if errors.Is(err, blobstore.ErrTimeoutUnknown) {
			return participantTimeoutUnknownOutcome(), nil
		}
		return nil, err
	}
	turn, ok := value.(*farmactor.ApplyGuardSnapshotTurnResult)
	if !ok || turn == nil || turn.Outcome == nil {
		return nil, status.Error(codes.Internal, "farm participant GuardSync returned an invalid outcome")
	}
	if h.observer != nil && shouldObserveApplyGuardSnapshot(turn) {
		h.observer(call.RouteHeader.GetActorId(), turn.Action)
	}
	return turn.Outcome, nil
}

func shouldObserveApplyGuardSnapshot(turn *farmactor.ApplyGuardSnapshotTurnResult) bool {
	return turn != nil && turn.Outcome != nil &&
		turn.Outcome.Outcome == contractspb.ParticipantOutcome_PARTICIPANT_OUTCOME_APPLIED &&
		!turn.Outcome.ReplayedFromReceipt && turn.Action != nil && turn.Action.Change != nil
}

func normalizeFarmParticipantError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "farm participant deadline/budget exhausted")
	}
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "farm participant request canceled")
	}
	if errors.Is(err, authoritystore.ErrCapacityExceeded) || errors.Is(err, farmactor.ErrParticipantPhysicalCapacity) {
		return farmParticipantCapacityError()
	}
	if errors.Is(err, farmactor.ErrParticipantCapacityUnavailable) {
		return status.Error(codes.Unavailable, "FARM_PARTICIPANT_CAPACITY_UNAVAILABLE")
	}
	if errors.Is(err, farmactor.ErrParticipantAdmissionBusy) {
		return status.Error(codes.Unavailable, "FARM_PARTICIPANT_ADMISSION_BUSY")
	}
	if errors.Is(err, farmactor.ErrParticipantReceiptAuthorityUnavailable) {
		return status.Error(codes.Unavailable, "FARM_PARTICIPANT_AUTHORITY_UNAVAILABLE")
	}
	if errors.Is(err, blobstore.ErrTimeoutUnknown) {
		return status.Error(codes.Unavailable, "FARM_PARTICIPANT_TIMEOUT_UNKNOWN")
	}
	if errors.Is(err, blobstore.ErrWriterEpochMismatch) {
		return status.Error(codes.FailedPrecondition, "WRITER_EPOCH_MISMATCH")
	}
	if errors.Is(err, blobstore.ErrCASConflict) || errors.Is(err, farmactor.ErrParticipantCapacityAdmissionStale) {
		return status.Error(codes.Aborted, "ERR_VERSION_CONFLICT")
	}
	if errors.Is(err, blobstore.ErrUnavailable) {
		return status.Error(codes.Unavailable, "FARM_AUTHORITY_UNAVAILABLE")
	}
	if status.Code(err) == codes.Unknown {
		return status.Error(codes.Internal, "farm participant internal failure")
	}
	return err
}

func farmParticipantCapacityError() error {
	stable := status.New(codes.ResourceExhausted, "ERR_RATE_LIMITED")
	withDetails, err := stable.WithDetails(&contractspb.ParticipantFailure{
		Error: &logicrpcpb.ErrorDetail{ErrorCode: commonpb.ErrorCode_ERR_RATE_LIMITED},
	})
	if err != nil {
		return stable.Err()
	}
	return withDetails.Err()
}

type rejectingFarmWorkVersionVerifier struct{}

func (rejectingFarmWorkVersionVerifier) VerifyWorkVersion(context.Context, *contracts.ParticipantCall) error {
	return status.Error(codes.FailedPrecondition, "farm participant WorkVersion verifier is not configured")
}

type rejectingFarmWorkLeaseVerifier struct{}

func (rejectingFarmWorkLeaseVerifier) VerifyWorkLease(context.Context, *contracts.ParticipantCall) error {
	return status.Error(codes.FailedPrecondition, "farm participant WorkLease verifier is not configured")
}

type rejectingDecideTheftRecoveryVerifier struct{}

func (rejectingDecideTheftRecoveryVerifier) VerifyDecideTheftRecovery(context.Context, *contracts.ParticipantCall) error {
	return status.Error(codes.FailedPrecondition, "farm participant Pair Theft recovery verifier is not configured")
}

type rejectingCommitPlantDependencyVerifier struct{}

func (rejectingCommitPlantDependencyVerifier) VerifyCommitPlantDependency(context.Context, *contracts.ParticipantCall) error {
	return status.Error(codes.FailedPrecondition, "farm participant Work/Farm dependency verifier is not configured")
}

type rejectingCommitUnlockDependencyVerifier struct{}

func (rejectingCommitUnlockDependencyVerifier) VerifyUnlockDependency(context.Context, *contracts.ParticipantCall) error {
	return status.Error(codes.FailedPrecondition, "farm participant UnlockPlot dependency verifier is not configured")
}

type rejectingCommitHarvestDependencyVerifier struct{}

func (rejectingCommitHarvestDependencyVerifier) VerifyCommitHarvestDependency(context.Context, *contracts.ParticipantCall) error {
	return status.Error(codes.FailedPrecondition, "farm participant Harvest dependency verifier is not configured")
}

type rejectingDecideTheftDependencyVerifier struct{}

func (rejectingDecideTheftDependencyVerifier) VerifyDecideTheftDependency(context.Context, *contracts.ParticipantCall) error {
	return status.Error(codes.FailedPrecondition, "farm participant Theft dependency verifier is not configured")
}

type rejectingFarmRouteFenceVerifier struct{}

func (rejectingFarmRouteFenceVerifier) VerifyRoute(context.Context, *contracts.ParticipantCall) error {
	return status.Error(codes.FailedPrecondition, "farm participant route fence verifier is not configured")
}

type rejectingFarmWorkAddressVerifier struct{}

func (rejectingFarmWorkAddressVerifier) VerifyWorkAddress(context.Context, *contracts.ParticipantCall) error {
	return status.Error(codes.FailedPrecondition, "farm participant WorkAddress bucket verifier is not configured")
}

type rejectingFarmIdentityBindingVerifier struct{}

func (rejectingFarmIdentityBindingVerifier) VerifyIdentityBinding(context.Context, *contracts.ParticipantCall) error {
	return status.Error(codes.FailedPrecondition, "farm participant subject/operation binding verifier is not configured")
}

func cloneFarmParticipantFailure(failure *contractspb.ParticipantFailure) *contractspb.ParticipantFailure {
	if failure == nil {
		return nil
	}
	return proto.Clone(failure).(*contractspb.ParticipantFailure)
}

// Registration is a production-reusable registration description.  It does
// not open listeners or alter cmd composition; the caller supplies the gRPC
// ServiceRegistrar owned by its process.
type Registration struct {
	Server *Server
}

func NewRegistration(opts Options) (*Registration, error) {
	server, err := NewServerChecked(opts)
	if err != nil {
		return nil, err
	}
	return &Registration{Server: server}, nil
}

// NewParticipantRegistration is a descriptive alias for callers that keep
// several Participant registrations in one process.
func NewParticipantRegistration(opts Options) (*Registration, error) {
	return NewRegistration(opts)
}

func (r *Registration) Register(registrar grpc.ServiceRegistrar) error {
	if r == nil || r.Server == nil || registrar == nil {
		return errors.New("farm participant registration is incomplete")
	}
	contractspb.RegisterCoordinatorParticipantServiceServer(registrar, r.Server)
	contractspb.RegisterWorkerRecoveryIngressServer(registrar, r.Server)
	return nil
}

func (s *Server) Register(registrar grpc.ServiceRegistrar) error {
	if s == nil {
		return errors.New("farm participant server is unavailable")
	}
	if s.configurationErr != nil {
		return s.configurationErr
	}
	return (&Registration{Server: s}).Register(registrar)
}
