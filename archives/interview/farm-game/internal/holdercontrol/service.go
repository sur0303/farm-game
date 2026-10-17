package holdercontrol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
	"unicode/utf8"

	holdercontrolpb "farmgame/api/gen/holdercontrol"
	"farmgame/internal/holderidentity"
	"farmgame/internal/holderkey"
	"farmgame/internal/logic/actor/runtime"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Authorizer is intentionally narrower than transport.MethodAuthorizer so
// tests can inject a deterministic authorizer. The production value is the
// exact full-method transport.MethodAuthorizer.
type Authorizer interface {
	Authorize(context.Context, string) error
}

const MaxWorkloadIdentityTokenBytes = 16 << 10

var (
	ErrRuntimeIdentityUnavailable  = errors.New("holdercontrol: runtime identity unavailable")
	ErrRuntimeIdentityPrecondition = errors.New("holdercontrol: runtime identity precondition not met")
	ErrWorkloadIdentityToken       = errors.New("holdercontrol: workload identity token unavailable")
)

type WorkloadIdentityTokenSource interface {
	Token(context.Context) (string, error)
}

type TokenSource = WorkloadIdentityTokenSource

type WorkloadIdentityTokenSourceFunc func(context.Context) (string, error)

func (f WorkloadIdentityTokenSourceFunc) Token(ctx context.Context) (string, error) {
	if f == nil {
		return "", ErrWorkloadIdentityToken
	}
	return f(ctx)
}

type FileWorkloadIdentityTokenSource struct {
	Path string
}

func (s FileWorkloadIdentityTokenSource) Token(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", ErrWorkloadIdentityToken
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if s.Path == "" {
		return "", ErrWorkloadIdentityToken
	}
	file, err := os.Open(s.Path)
	if err != nil {
		return "", ErrWorkloadIdentityToken
	}
	defer file.Close()
	tokenBytes, err := io.ReadAll(io.LimitReader(file, MaxWorkloadIdentityTokenBytes+1))
	if err != nil {
		return "", ErrWorkloadIdentityToken
	}
	return validateWorkloadIdentityToken(string(tokenBytes))
}

func validateWorkloadIdentityToken(token string) (string, error) {
	if token == "" || !utf8.ValidString(token) {
		return "", ErrWorkloadIdentityToken
	}
	if len([]byte(token)) > MaxWorkloadIdentityTokenBytes {
		return "", fmt.Errorf("%w: token exceeds %d bytes", ErrWorkloadIdentityToken, MaxWorkloadIdentityTokenBytes)
	}
	return token, nil
}

// ShardRuntime is the per-domain holder runtime port. It contains no caller
// identity and no authority credentials; the runtime owns those capabilities.
type ShardRuntime interface {
	StopNewActivationAndTurns(context.Context, uint32) error
	WaitCurrentTurns(context.Context, uint32) error
	ConfirmFactsCommitted(context.Context, uint32) error
	GetShardStatus(context.Context, uint32) (runtime.ShardDrainStatus, error)
}

type ShardRuntimeRearmer interface {
	RearmShard(context.Context, uint32) error
}

type ShardGenerationPreparer interface {
	PrepareShardGeneration(context.Context, uint32, AuthorityGeneration) error
}

// PreparedGenerationStatus is the current holder-side cache state for one
// shard. The LocalService reads it from the preparer rather than maintaining a
// second prepared-generation state machine.
type PreparedGenerationStatus struct {
	Prepared   bool
	Generation *AuthorityGeneration
	Committing bool
}

// ShardGenerationStatusReader is the narrow status seam paired with a
// ShardGenerationPreparer. Implementations must report their current exact
// cached generation after applying expiry/commit deletion rules.
type ShardGenerationStatusReader interface {
	PreparedShardGeneration(context.Context, uint32) (PreparedGenerationStatus, error)
}

type DomainRuntimeOptions struct {
	Runtime    ShardRuntime
	Preparer   ShardGenerationPreparer
	ShardCount uint32
}

type LocalServiceOptions struct {
	Authorizer Authorizer

	// These values are copied at construction and are included in every
	// response. LocalBootID is the preferred name; LocalHolderBootID is kept
	// as a spelling-compatible input for existing composition code.
	LocalHolderID     string
	LocalBootID       string
	LocalHolderBootID string
	Now               func() time.Time
	OperationTimeout  time.Duration

	// New runtime identity RPCs fail closed when any of these optional
	// compatibility dependencies is absent.
	DeploymentIdentity          holderidentity.DeploymentIdentity
	ProcessStatusSource         ProcessStatusSource
	ProcessStatus               ProcessStatusSource
	TokenSource                 TokenSource
	WorkloadIdentityTokenSource WorkloadIdentityTokenSource

	// CommitProofKey is public Ed25519 verification material generated for the
	// exact LocalHolderID/LocalBootID process. An empty value supports mixed
	// rollout; a partial or invalid value is rejected at construction.
	CommitProofKey holderkey.Publication

	Player DomainRuntimeOptions
	Farm   DomainRuntimeOptions
}

type controlPhase uint8

const (
	controlPhaseOpen controlPhase = iota
	controlPhaseStopped
	controlPhaseWaited
	controlPhaseFactsCommitted
)

const defaultHolderControlServiceTimeout = 5 * time.Second

type controlShard struct {
	opMu  sync.Mutex
	mu    sync.Mutex
	phase controlPhase
}

type controlDomain struct {
	runtime  ShardRuntime
	preparer ShardGenerationPreparer
	shards   []controlShard
}

// LocalService implements HolderControlService without depending on the
// transport interceptor. Each method repeats exact authorizer validation.
type LocalService struct {
	holdercontrolpb.UnimplementedHolderControlServiceServer

	authorizer         Authorizer
	localHolderID      string
	localBootID        string
	now                func() time.Time
	operationTimeout   time.Duration
	deploymentIdentity holderidentity.DeploymentIdentity
	processStatus      ProcessStatusSource
	tokenSource        WorkloadIdentityTokenSource
	commitProofKey     holderkey.Publication
	player             controlDomain
	farm               controlDomain
}

var _ holdercontrolpb.HolderControlServiceServer = (*LocalService)(nil)

func NewLocalService(opts LocalServiceOptions) (*LocalService, error) {
	if opts.Authorizer == nil {
		return nil, fmt.Errorf("%w: authorizer required", ErrAuthorization)
	}
	bootID := opts.LocalBootID
	if opts.LocalHolderBootID != "" {
		if bootID != "" && bootID != opts.LocalHolderBootID {
			return nil, fmt.Errorf("%w: local boot identity conflict", ErrInvalidRequest)
		}
		bootID = opts.LocalHolderBootID
	}
	if err := validateHolderIdentity(opts.LocalHolderID, bootID); err != nil {
		return nil, err
	}
	if err := opts.DeploymentIdentity.ValidateOptional(); err != nil {
		return nil, fmt.Errorf("%w: deployment identity: %v", ErrInvalidRequest, err)
	}
	if opts.Now == nil {
		return nil, fmt.Errorf("%w: Now is required", ErrInvalidRequest)
	}
	if opts.OperationTimeout < 0 {
		return nil, fmt.Errorf("%w: service operation timeout", ErrHolderBudget)
	}
	if opts.OperationTimeout == 0 {
		opts.OperationTimeout = defaultHolderControlServiceTimeout
	}
	if err := validateHolderControlTimeout("service operation timeout", opts.OperationTimeout); err != nil {
		return nil, err
	}
	processStatus := opts.ProcessStatusSource
	if processStatus == nil {
		processStatus = opts.ProcessStatus
	}
	tokenSource := opts.WorkloadIdentityTokenSource
	if tokenSource == nil {
		tokenSource = opts.TokenSource
	}
	if opts.CommitProofKey.KeyID != "" || len(opts.CommitProofKey.PublicKey) != 0 {
		if err := opts.CommitProofKey.Validate(); err != nil {
			return nil, fmt.Errorf("%w: commit proof public key: %v", ErrInvalidRequest, err)
		}
	}

	if (opts.Player.Runtime == nil) != (opts.Player.ShardCount == 0) ||
		(opts.Farm.Runtime == nil) != (opts.Farm.ShardCount == 0) {
		return nil, fmt.Errorf("%w: configured domain runtime requires a positive shard count", ErrRuntimeUnavailable)
	}
	if opts.Player.Runtime == nil && opts.Farm.Runtime == nil {
		return nil, fmt.Errorf("%w: at least one domain runtime is required", ErrRuntimeUnavailable)
	}
	return &LocalService{
		authorizer: opts.Authorizer, localHolderID: opts.LocalHolderID, localBootID: bootID,
		deploymentIdentity: opts.DeploymentIdentity, processStatus: processStatus, tokenSource: tokenSource,
		commitProofKey: opts.CommitProofKey.Clone(),
		now:            opts.Now, operationTimeout: opts.OperationTimeout,
		player: controlDomain{runtime: opts.Player.Runtime, preparer: opts.Player.Preparer, shards: make([]controlShard, opts.Player.ShardCount)},
		farm:   controlDomain{runtime: opts.Farm.Runtime, preparer: opts.Farm.Preparer, shards: make([]controlShard, opts.Farm.ShardCount)},
	}, nil
}

func (s *LocalService) rpcContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if s == nil {
		return nil, nil, serviceError(codes.Unavailable, "holder control runtime unavailable", ErrRuntimeUnavailable)
	}
	if ctx == nil {
		return nil, nil, serviceError(codes.InvalidArgument, "holder control request is invalid", ErrInvalidRequest)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, mapLocalError(err, serviceErrorGeneric)
	}
	if err := validateHolderControlTimeout("service operation timeout", s.operationTimeout); err != nil {
		return nil, nil, serviceError(codes.Internal, "holder control service configuration is invalid", err)
	}
	callCtx, cancel := context.WithTimeout(ctx, s.operationTimeout)
	return callCtx, cancel, nil
}

func (s *LocalService) authorize(ctx context.Context, method string) error {
	if s == nil || s.authorizer == nil || ctx == nil {
		return ErrAuthorization
	}
	return s.authorizer.Authorize(ctx, method)
}

func (s *LocalService) domain(value string) (*controlDomain, error) {
	switch value {
	case "PLAYER":
		return &s.player, nil
	case "FARM":
		return &s.farm, nil
	default:
		return nil, fmt.Errorf("%w: unsupported domain %q", ErrInvalidRequest, value)
	}
}

func (s *LocalService) target(value string, shard uint32) (*controlDomain, *controlShard, error) {
	domain, err := s.domain(value)
	if err != nil {
		return nil, nil, err
	}
	if domain.runtime == nil {
		return nil, nil, ErrRuntimeUnavailable
	}
	if err := validateShard(shard, uint32(len(domain.shards))); err != nil {
		return nil, nil, err
	}
	return domain, &domain.shards[shard], nil
}

func phaseAt(value controlPhase) holdercontrolpb.DrainPhase {
	switch value {
	case controlPhaseOpen:
		return holdercontrolpb.DrainPhase_DRAIN_PHASE_OPEN
	case controlPhaseStopped:
		return holdercontrolpb.DrainPhase_DRAIN_PHASE_STOPPED
	case controlPhaseWaited:
		return holdercontrolpb.DrainPhase_DRAIN_PHASE_WAITED
	case controlPhaseFactsCommitted:
		return holdercontrolpb.DrainPhase_DRAIN_PHASE_FACTS_COMMITTED
	default:
		return holdercontrolpb.DrainPhase_DRAIN_PHASE_UNSPECIFIED
	}
}

func (s *LocalService) currentPhase(state *controlShard) controlPhase {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.phase
}

func (s *LocalService) setPhase(state *controlShard, phase controlPhase) {
	state.mu.Lock()
	state.phase = phase
	state.mu.Unlock()
}

func (s *LocalService) response(ctx context.Context, domain string, shard uint32, state *controlShard, control *controlDomain) (*holdercontrolpb.HolderControlResponse, error) {
	if control == nil || control.runtime == nil {
		return nil, ErrRuntimeUnavailable
	}
	if ctx == nil {
		return nil, ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	runtimeStatus, err := control.runtime.GetShardStatus(ctx, shard)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	admitting, err := runtimeAdmitting(runtimeStatus)
	if err != nil {
		return nil, err
	}
	phase := phaseAt(s.currentPhase(state))
	if err := validateLocalRuntimeFlags(phase, runtimeStatus, admitting); err != nil {
		return nil, err
	}
	now := s.now()
	prepared, err := s.currentPreparedStatus(ctx, control.preparer, shard, now)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	status := &holdercontrolpb.ShardStatus{
		Domain:           domain,
		Shard:            shard,
		Phase:            phase,
		AcceptedUserJobs: runtimeStatus.AcceptedJobs,
		WaitComplete:     runtimeStatus.WaitComplete,
		Admitting:        admitting,
		HolderId:         s.localHolderID,
		HolderBootId:     s.localBootID,
	}
	if prepared.Prepared {
		status.Prepared = true
		status.PreparedGeneration = encodeGeneration(*prepared.Generation)
	}
	if err := validateShardStatusFields(status, domain, shard, s.localHolderID, s.localBootID, now); err != nil {
		return nil, err
	}
	return &holdercontrolpb.HolderControlResponse{Status: status}, nil
}

func (s *LocalService) currentPreparedStatus(ctx context.Context, preparer ShardGenerationPreparer, shard uint32, now time.Time) (PreparedGenerationStatus, error) {
	if preparer == nil {
		return PreparedGenerationStatus{}, nil
	}
	reader, ok := preparer.(ShardGenerationStatusReader)
	if !ok {
		return PreparedGenerationStatus{}, ErrRuntimeUnavailable
	}
	status, err := reader.PreparedShardGeneration(ctx, shard)
	if err != nil {
		return PreparedGenerationStatus{}, err
	}
	if !status.Prepared {
		if status.Generation != nil {
			return PreparedGenerationStatus{}, ErrPreparedIdentity
		}
		return status, nil
	}
	if status.Generation == nil {
		return PreparedGenerationStatus{}, ErrPreparedIdentity
	}
	if err := status.Generation.ValidateAt(now); err != nil {
		return PreparedGenerationStatus{}, err
	}
	if status.Generation.HolderID != s.localHolderID || status.Generation.HolderBootID != s.localBootID {
		return PreparedGenerationStatus{}, ErrPreparedIdentity
	}
	return status, nil
}

func runtimeAdmitting(status runtime.ShardDrainStatus) (bool, error) {
	switch status.Phase {
	case runtime.ShardDrainPhaseOpen:
		return true, nil
	case runtime.ShardDrainPhaseDraining:
		return false, nil
	default:
		return false, ErrRuntimeUnavailable
	}
}

func validateLocalRuntimeFlags(phase holdercontrolpb.DrainPhase, runtimeStatus runtime.ShardDrainStatus, admitting bool) error {
	switch phase {
	case holdercontrolpb.DrainPhase_DRAIN_PHASE_OPEN:
		if !admitting || runtimeStatus.WaitComplete {
			return ErrRuntimeUnavailable
		}
	case holdercontrolpb.DrainPhase_DRAIN_PHASE_STOPPED:
		if admitting || runtimeStatus.WaitComplete {
			return ErrRuntimeUnavailable
		}
	case holdercontrolpb.DrainPhase_DRAIN_PHASE_WAITED,
		holdercontrolpb.DrainPhase_DRAIN_PHASE_FACTS_COMMITTED:
		if admitting || !runtimeStatus.WaitComplete || runtimeStatus.AcceptedJobs != 0 {
			return ErrRuntimeUnavailable
		}
	default:
		return ErrRuntimeUnavailable
	}
	return nil
}

func (s *LocalService) StopNewActivationAndTurns(ctx context.Context, req *holdercontrolpb.StopNewActivationAndTurnsRequest) (response *holdercontrolpb.HolderControlResponse, err error) {
	callCtx, cancel, err := s.rpcContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := s.authorize(callCtx, holdercontrolpb.HolderControlService_StopNewActivationAndTurns_FullMethodName); err != nil {
		return nil, mapLocalError(err, serviceErrorAuth)
	}
	if err := callCtx.Err(); err != nil {
		return nil, mapLocalError(err, serviceErrorGeneric)
	}
	if req == nil {
		return nil, mapLocalError(ErrInvalidRequest, serviceErrorInput)
	}
	domain, err := decodeDomain(req.GetDomain())
	if err != nil {
		return nil, mapLocalError(err, serviceErrorInput)
	}
	control, state, err := s.target(string(domain), req.GetShard())
	if err != nil {
		return nil, mapLocalError(err, serviceErrorInput)
	}
	state.opMu.Lock()
	defer state.opMu.Unlock()
	switch s.currentPhase(state) {
	case controlPhaseOpen:
		if err := control.runtime.StopNewActivationAndTurns(callCtx, req.GetShard()); err != nil {
			return nil, mapLocalError(err, serviceErrorRuntime)
		}
		s.setPhase(state, controlPhaseStopped)
	case controlPhaseStopped, controlPhaseWaited, controlPhaseFactsCommitted:
	default:
		return nil, mapLocalError(ErrShardPhaseOrder, serviceErrorPhase)
	}
	response, err = s.response(callCtx, string(domain), req.GetShard(), state, control)
	if err != nil {
		return nil, mapLocalError(err, serviceErrorRuntime)
	}
	return response, nil
}

func (s *LocalService) WaitCurrentTurns(ctx context.Context, req *holdercontrolpb.WaitCurrentTurnsRequest) (response *holdercontrolpb.HolderControlResponse, err error) {
	callCtx, cancel, err := s.rpcContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := s.authorize(callCtx, holdercontrolpb.HolderControlService_WaitCurrentTurns_FullMethodName); err != nil {
		return nil, mapLocalError(err, serviceErrorAuth)
	}
	if err := callCtx.Err(); err != nil {
		return nil, mapLocalError(err, serviceErrorGeneric)
	}
	if req == nil {
		return nil, mapLocalError(ErrInvalidRequest, serviceErrorInput)
	}
	domain, err := decodeDomain(req.GetDomain())
	if err != nil {
		return nil, mapLocalError(err, serviceErrorInput)
	}
	control, state, err := s.target(string(domain), req.GetShard())
	if err != nil {
		return nil, mapLocalError(err, serviceErrorInput)
	}
	state.opMu.Lock()
	defer state.opMu.Unlock()
	switch s.currentPhase(state) {
	case controlPhaseOpen:
		return nil, mapLocalError(ErrShardPhaseOrder, serviceErrorPhase)
	case controlPhaseStopped:
		if err := control.runtime.WaitCurrentTurns(callCtx, req.GetShard()); err != nil {
			return nil, mapLocalError(err, serviceErrorRuntime)
		}
		s.setPhase(state, controlPhaseWaited)
	case controlPhaseWaited, controlPhaseFactsCommitted:
	default:
		return nil, mapLocalError(ErrShardPhaseOrder, serviceErrorPhase)
	}
	response, err = s.response(callCtx, string(domain), req.GetShard(), state, control)
	if err != nil {
		return nil, mapLocalError(err, serviceErrorRuntime)
	}
	return response, nil
}

func (s *LocalService) ConfirmFactsCommitted(ctx context.Context, req *holdercontrolpb.ConfirmFactsCommittedRequest) (response *holdercontrolpb.HolderControlResponse, err error) {
	callCtx, cancel, err := s.rpcContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := s.authorize(callCtx, holdercontrolpb.HolderControlService_ConfirmFactsCommitted_FullMethodName); err != nil {
		return nil, mapLocalError(err, serviceErrorAuth)
	}
	if err := callCtx.Err(); err != nil {
		return nil, mapLocalError(err, serviceErrorGeneric)
	}
	if req == nil {
		return nil, mapLocalError(ErrInvalidRequest, serviceErrorInput)
	}
	domain, err := decodeDomain(req.GetDomain())
	if err != nil {
		return nil, mapLocalError(err, serviceErrorInput)
	}
	control, state, err := s.target(string(domain), req.GetShard())
	if err != nil {
		return nil, mapLocalError(err, serviceErrorInput)
	}
	state.opMu.Lock()
	defer state.opMu.Unlock()
	switch s.currentPhase(state) {
	case controlPhaseOpen, controlPhaseStopped:
		return nil, mapLocalError(ErrShardPhaseOrder, serviceErrorPhase)
	case controlPhaseWaited:
		if err := control.runtime.ConfirmFactsCommitted(callCtx, req.GetShard()); err != nil {
			return nil, mapLocalError(err, serviceErrorRuntime)
		}
		s.setPhase(state, controlPhaseFactsCommitted)
	case controlPhaseFactsCommitted:
	default:
		return nil, mapLocalError(ErrShardPhaseOrder, serviceErrorPhase)
	}
	response, err = s.response(callCtx, string(domain), req.GetShard(), state, control)
	if err != nil {
		return nil, mapLocalError(err, serviceErrorRuntime)
	}
	return response, nil
}

func (s *LocalService) PrepareShardGeneration(ctx context.Context, req *holdercontrolpb.PrepareShardGenerationRequest) (response *holdercontrolpb.HolderControlResponse, err error) {
	callCtx, cancel, err := s.rpcContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := s.authorize(callCtx, holdercontrolpb.HolderControlService_PrepareShardGeneration_FullMethodName); err != nil {
		return nil, mapLocalError(err, serviceErrorAuth)
	}
	if err := callCtx.Err(); err != nil {
		return nil, mapLocalError(err, serviceErrorGeneric)
	}
	if req == nil {
		return nil, mapLocalError(ErrInvalidRequest, serviceErrorInput)
	}
	domain, err := decodeDomain(req.GetDomain())
	if err != nil {
		return nil, mapLocalError(err, serviceErrorInput)
	}
	generation, err := decodeGeneration(req.GetGeneration())
	if err != nil {
		return nil, mapLocalError(err, serviceErrorPhase)
	}
	if generation.HolderID != s.localHolderID || generation.HolderBootID != s.localBootID {
		return nil, mapLocalError(ErrPreparedIdentity, serviceErrorPhase)
	}
	if err := generation.ValidateAt(s.now()); err != nil {
		return nil, mapLocalError(err, serviceErrorPhase)
	}
	control, state, err := s.target(string(domain), req.GetShard())
	if err != nil {
		return nil, mapLocalError(err, serviceErrorInput)
	}
	if control.preparer == nil {
		return nil, mapLocalError(ErrRuntimeUnavailable, serviceErrorRuntime)
	}
	state.opMu.Lock()
	defer state.opMu.Unlock()
	_, err = s.openTargetForGeneration(
		callCtx,
		control,
		state,
		req.GetShard(),
	)
	if err != nil {
		return nil, err
	}
	if err := control.preparer.PrepareShardGeneration(callCtx, req.GetShard(), generation); err != nil {
		return nil, mapLocalError(err, serviceErrorRuntime)
	}
	response, err = s.response(callCtx, string(domain), req.GetShard(), state, control)
	if err != nil {
		return nil, mapLocalError(err, serviceErrorRuntime)
	}
	return response, nil
}

func (s *LocalService) openTargetForGeneration(
	ctx context.Context,
	control *controlDomain,
	state *controlShard,
	shard uint32,
) (runtime.ShardDrainStatus, error) {
	phase := s.currentPhase(state)
	switch phase {
	case controlPhaseOpen:
	case controlPhaseFactsCommitted:
		rearmer, ok := control.runtime.(ShardRuntimeRearmer)
		if !ok {
			return runtime.ShardDrainStatus{}, mapLocalError(
				ErrRuntimeUnavailable,
				serviceErrorRuntime,
			)
		}
		if err := rearmer.RearmShard(ctx, shard); err != nil {
			return runtime.ShardDrainStatus{}, mapLocalError(
				err,
				serviceErrorRuntime,
			)
		}
	default:
		return runtime.ShardDrainStatus{}, mapLocalError(
			ErrShardPhaseOrder,
			serviceErrorPhase,
		)
	}
	status, err := control.runtime.GetShardStatus(ctx, shard)
	if err != nil {
		return runtime.ShardDrainStatus{}, mapLocalError(
			err,
			serviceErrorRuntime,
		)
	}
	if status.Phase != runtime.ShardDrainPhaseOpen ||
		status.AcceptedJobs != 0 ||
		status.WaitComplete {
		return runtime.ShardDrainStatus{}, mapLocalError(
			ErrShardPhaseOrder,
			serviceErrorPhase,
		)
	}
	if phase == controlPhaseFactsCommitted {
		s.setPhase(state, controlPhaseOpen)
	}
	return status, nil
}

func (s *LocalService) GetShardStatus(ctx context.Context, req *holdercontrolpb.GetShardStatusRequest) (*holdercontrolpb.GetShardStatusResponse, error) {
	callCtx, cancel, err := s.rpcContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := s.authorize(callCtx, holdercontrolpb.HolderControlService_GetShardStatus_FullMethodName); err != nil {
		return nil, mapLocalError(err, serviceErrorAuth)
	}
	if err := callCtx.Err(); err != nil {
		return nil, mapLocalError(err, serviceErrorGeneric)
	}
	if req == nil {
		return nil, mapLocalError(ErrInvalidRequest, serviceErrorInput)
	}
	domain, err := decodeDomain(req.GetDomain())
	if err != nil {
		return nil, mapLocalError(err, serviceErrorInput)
	}
	control, state, err := s.target(string(domain), req.GetShard())
	if err != nil {
		return nil, mapLocalError(err, serviceErrorInput)
	}
	state.opMu.Lock()
	defer state.opMu.Unlock()
	response, err := s.response(callCtx, string(domain), req.GetShard(), state, control)
	if err != nil {
		return nil, mapLocalError(err, serviceErrorRuntime)
	}
	return &holdercontrolpb.GetShardStatusResponse{Status: response.GetStatus()}, nil
}

func (s *LocalService) runtimeIdentityDomain(value string) (string, error) {
	domain, err := decodeDomain(value)
	if err != nil {
		return "", err
	}
	control, err := s.domain(string(domain))
	if err != nil || control == nil || control.runtime == nil {
		return "", fmt.Errorf("%w: domain %q is not configured", ErrRuntimeIdentityPrecondition, value)
	}
	return string(domain), nil
}

func (s *LocalService) runtimeIdentity(ctx context.Context, domain string) (*holdercontrolpb.RuntimeIdentity, error) {
	if s == nil || ctx == nil || s.processStatus == nil || s.tokenSource == nil || !s.deploymentIdentity.Exact() {
		return nil, ErrRuntimeIdentityUnavailable
	}
	if err := s.deploymentIdentity.ValidateExact(); err != nil {
		return nil, ErrRuntimeIdentityUnavailable
	}
	token, err := s.tokenSource.Token(ctx)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, ErrRuntimeIdentityUnavailable
	}
	token, err = validateWorkloadIdentityToken(token)
	if err != nil {
		return nil, ErrRuntimeIdentityUnavailable
	}
	processStatus, err := s.processStatus.Status(ctx)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, ErrRuntimeIdentityUnavailable
	}
	lifecycleState, err := runtimeLifecycleState(processStatus)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	response := &holdercontrolpb.RuntimeIdentity{
		Domain:                domain,
		HolderId:              s.localHolderID,
		BootId:                s.localBootID,
		WorkloadUid:           s.deploymentIdentity.WorkloadUID,
		PodUid:                s.deploymentIdentity.PodUID,
		Ordinal:               s.deploymentIdentity.Ordinal,
		ProcessReady:          processStatus.ProcessReady,
		Draining:              processStatus.Draining,
		ServingShardCount:     processStatus.ServingShardCount,
		ObservedRouteRevision: processStatus.SnapshotRevision,
		LifecycleState:        lifecycleState,
		ObservedAtMs:          s.now().UnixMilli(),
		WorkloadIdentityToken: token,
	}
	if s.commitProofKey.KeyID != "" || len(s.commitProofKey.PublicKey) != 0 {
		if err := s.commitProofKey.Validate(); err != nil {
			return nil, ErrRuntimeIdentityUnavailable
		}
		response.CommitProofKeyId = s.commitProofKey.KeyID
		response.CommitProofPublicKey = append([]byte(nil), s.commitProofKey.PublicKey...)
	}
	return response, nil
}

func runtimeLifecycleState(status ProcessStatus) (string, error) {
	if status.Draining {
		return "DRAINING", nil
	}
	if !status.ProcessReady {
		if status.ServingShardCount != 0 {
			// ProcessReady also contains assignment-health. While a new route
			// snapshot is being reconciled, a healthy process can therefore be
			// temporarily unready while it continues to serve its already-owned
			// shards. Keep its authenticated runtime identity observable; callers
			// still use ProcessReady to exclude it from new placement.
			return "SERVING", nil
		}
		return "WARMING", nil
	}
	if status.ServingShardCount == 0 {
		return "READY_ZERO_SHARD", nil
	}
	return "SERVING", nil
}

func (s *LocalService) GetRuntimeIdentity(ctx context.Context, req *holdercontrolpb.GetRuntimeIdentityRequest) (*holdercontrolpb.RuntimeIdentity, error) {
	callCtx, cancel, err := s.rpcContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := s.authorize(callCtx, holdercontrolpb.HolderControlService_GetRuntimeIdentity_FullMethodName); err != nil {
		return nil, mapLocalError(err, serviceErrorAuth)
	}
	if err := callCtx.Err(); err != nil {
		return nil, mapLocalError(err, serviceErrorGeneric)
	}
	if req == nil {
		return nil, mapLocalError(ErrInvalidRequest, serviceErrorInput)
	}
	domain, err := s.runtimeIdentityDomain(req.GetDomain())
	if err != nil {
		return nil, mapLocalError(err, serviceErrorInput)
	}
	response, err := s.runtimeIdentity(callCtx, domain)
	if err != nil {
		return nil, mapLocalError(err, serviceErrorRuntime)
	}
	return response, nil
}

func (s *LocalService) ConfirmZeroShard(ctx context.Context, req *holdercontrolpb.ConfirmZeroShardRequest) (*holdercontrolpb.RuntimeIdentity, error) {
	callCtx, cancel, err := s.rpcContext(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if err := s.authorize(callCtx, holdercontrolpb.HolderControlService_ConfirmZeroShard_FullMethodName); err != nil {
		return nil, mapLocalError(err, serviceErrorAuth)
	}
	if err := callCtx.Err(); err != nil {
		return nil, mapLocalError(err, serviceErrorGeneric)
	}
	if req == nil {
		return nil, mapLocalError(ErrInvalidRequest, serviceErrorInput)
	}
	domain, err := s.runtimeIdentityDomain(req.GetDomain())
	if err != nil {
		return nil, mapLocalError(err, serviceErrorInput)
	}
	if req.GetBootId() != s.localBootID {
		return nil, mapLocalError(fmt.Errorf("%w: exact boot_id required", ErrRuntimeIdentityPrecondition), serviceErrorPhase)
	}
	response, err := s.runtimeIdentity(callCtx, domain)
	if err != nil {
		return nil, mapLocalError(err, serviceErrorRuntime)
	}
	if !response.GetProcessReady() || response.GetServingShardCount() != 0 {
		return nil, mapLocalError(fmt.Errorf("%w: process is not ready at zero shards", ErrRuntimeIdentityPrecondition), serviceErrorPhase)
	}
	return response, nil
}

type serviceErrorKind uint8

const (
	serviceErrorGeneric serviceErrorKind = iota
	serviceErrorAuth
	serviceErrorInput
	serviceErrorPhase
	serviceErrorRuntime
)

type localServiceStatusError struct {
	code  codes.Code
	msg   string
	cause error
}

func (e *localServiceStatusError) Error() string {
	return e.msg
}

func (e *localServiceStatusError) Unwrap() error {
	return e.cause
}

func (e *localServiceStatusError) GRPCStatus() *status.Status {
	return status.New(e.code, e.msg)
}

func serviceError(code codes.Code, message string, cause error) error {
	return &localServiceStatusError{code: code, msg: message, cause: cause}
}

func mapLocalError(err error, kind serviceErrorKind) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*localServiceStatusError); ok {
		return err
	}
	if errors.Is(err, context.Canceled) {
		return serviceError(codes.Canceled, "holder control request canceled", err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return serviceError(codes.DeadlineExceeded, "holder control request deadline exceeded", err)
	}
	switch status.Code(err) {
	case codes.Unauthenticated:
		return serviceError(codes.Unauthenticated, "holder control authentication required", err)
	case codes.PermissionDenied:
		return serviceError(codes.PermissionDenied, "holder control authorization denied", err)
	}
	switch {
	case errors.Is(err, ErrRuntimeIdentityPrecondition):
		return serviceError(codes.FailedPrecondition, "holder control runtime identity precondition failed", err)
	case errors.Is(err, ErrInvalidRequest):
		return serviceError(codes.InvalidArgument, "holder control request is invalid", err)
	case errors.Is(err, ErrShardPhaseOrder),
		errors.Is(err, ErrGenerationConflict),
		errors.Is(err, ErrFactsPrecondition),
		errors.Is(err, ErrPreparedNotFound),
		errors.Is(err, ErrPreparedExpired),
		errors.Is(err, ErrPreparedBusy),
		errors.Is(err, ErrPreparedIdentity):
		return serviceError(codes.FailedPrecondition, "holder control phase or generation precondition failed", err)
	case errors.Is(err, ErrPreparedOverflow):
		return serviceError(codes.ResourceExhausted, "holder control prepared state is full", err)
	case errors.Is(err, ErrRuntimeIdentityUnavailable),
		errors.Is(err, ErrRuntimeUnavailable):
		return serviceError(codes.Unavailable, "holder control runtime unavailable", err)
	}
	switch kind {
	case serviceErrorAuth:
		return serviceError(codes.PermissionDenied, "holder control authorization denied", err)
	case serviceErrorInput:
		return serviceError(codes.InvalidArgument, "holder control request is invalid", err)
	case serviceErrorPhase:
		return serviceError(codes.FailedPrecondition, "holder control phase or generation precondition failed", err)
	case serviceErrorRuntime:
		return serviceError(codes.Unavailable, "holder control runtime unavailable", err)
	default:
		return serviceError(codes.Internal, "holder control internal failure", err)
	}
}
