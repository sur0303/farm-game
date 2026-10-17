// Package plantrecovery composes the SC1 Plant definition with the generic
// worker recovery runtime. It owns only process composition: durable work
// state, scenario rules, and Participant admission remain in their existing
// packages.
package plantrecovery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/scenario/plant"
	"farmgame/internal/scenario/unlockplot"
	"farmgame/internal/workflow"

	"google.golang.org/grpc"
)

const (
	FamilyPlant      = "PLANT"
	FamilyUnlockPlot = "UNLOCK_PLOT"
)

var (
	ErrInvalidConfig         = errors.New("plant recovery: invalid configuration")
	ErrPlantRouteUnavailable = errors.New("plant recovery: Plant route or Participant clients are unavailable")
)

// Config contains the physical dependencies of the worker composition. The
// generated Participant clients are keyed by the endpoint named by the
// existing static Actor route snapshot.
type Config struct {
	Repository workflow.WorkRepository

	RouteResolver actorrouting.Resolver
	Realm         string
	PlayerClients map[string]contractspb.WorkerRecoveryIngressClient
	FarmClients   map[string]contractspb.WorkerRecoveryIngressClient

	Owner        string
	Token        string
	BootID       string
	ClaimLimit   int
	LeaseTTL     time.Duration
	SafetyMargin time.Duration
	ClaimTiming  *workflow.ClaimTimingRegistry

	// OperationTimeout bounds one Plant or UnlockPlot resume attempt. The
	// generic engine still owns its lease-deadline stop check; this is only
	// the RPC operation budget.
	OperationTimeout    time.Duration
	ParticipantBudgetMs uint32

	Now func() time.Time
}

// Composition is the generic WorkRecord recovery composition for Plant and
// UnlockPlot. Production FirstHop recovery is SourceOrder; these loops remain
// for tests and any leftover generic WorkRecord scan.
type Composition struct {
	PlantLoop       *workflow.RecoveryLoop
	UnlockLoop      *workflow.RecoveryLoop
	PlantRepository workflow.WorkRepository
	PlantHandler    workflow.WorkHandler
	UnlockHandler   workflow.WorkHandler
}

// RoundStats contains one bounded scan result per owned storage family.
type RoundStats struct {
	Plant  workflow.RecoveryStats
	Unlock workflow.RecoveryStats
}

// New constructs the generic Plant and UnlockPlot recovery loops. A missing
// Plant route/client set is fail-closed: no loop is started.
func New(config Config) (*Composition, error) {
	if config.Repository == nil || strings.TrimSpace(config.Owner) == "" || strings.TrimSpace(config.Token) == "" {
		return nil, fmt.Errorf("%w: repository, owner, and token are required", ErrInvalidConfig)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.OperationTimeout <= 0 {
		config.OperationTimeout = 3 * time.Second
	}
	if config.ClaimTiming == nil {
		config.ClaimTiming = workflow.NewClaimTimingRegistry()
	}

	composition := &Composition{}
	plantConfigured := config.RouteResolver != nil || len(config.PlayerClients) != 0 || len(config.FarmClients) != 0
	if plantConfigured {
		if err := validatePlantConfig(config); err != nil {
			return nil, err
		}
		playerClient := &routedRecoveryClient{
			resolver: config.RouteResolver,
			realm:    config.Realm,
			domain:   actorrouting.DomainPlayer,
			clients:  config.PlayerClients,
		}
		farmClient := &routedRecoveryClient{
			resolver: config.RouteResolver,
			realm:    config.Realm,
			domain:   actorrouting.DomainFarm,
			clients:  config.FarmClients,
		}
		leaseTTL, safetyMargin := recoveryLeaseDefaults(config.LeaseTTL, config.SafetyMargin)
		handler := &plantHandler{
			repository:          config.Repository,
			claimTiming:         config.ClaimTiming,
			resolver:            config.RouteResolver,
			realm:               config.Realm,
			player:              playerClient,
			farm:                farmClient,
			token:               config.Token,
			bootID:              config.BootID,
			operationTimeout:    config.OperationTimeout,
			participantBudgetMs: config.ParticipantBudgetMs,
			now:                 config.Now,
		}
		loop, err := workflow.NewRecoveryLoop(config.Repository, map[string]workflow.WorkHandler{
			FamilyPlant: handler,
		}, workflow.RecoveryConfig{
			Owner:        config.Owner,
			Token:        config.Token,
			ClaimLimit:   config.ClaimLimit,
			LeaseTTL:     leaseTTL,
			SafetyMargin: safetyMargin,
			ClaimTiming:  config.ClaimTiming,
			ClaimNow:     config.Now,
		})
		if err != nil {
			return nil, fmt.Errorf("%w: Plant RecoveryLoop: %v", ErrInvalidConfig, err)
		}
		composition.PlantLoop = loop
		composition.PlantRepository = config.Repository
		composition.PlantHandler = handler

		unlockHandler := &unlockHandler{plantHandler: handler}
		unlockLoop, err := workflow.NewRecoveryLoop(config.Repository, map[string]workflow.WorkHandler{
			FamilyUnlockPlot: unlockHandler,
		}, workflow.RecoveryConfig{
			Owner:        config.Owner,
			Token:        config.Token,
			ClaimLimit:   config.ClaimLimit,
			LeaseTTL:     leaseTTL,
			SafetyMargin: safetyMargin,
			ClaimTiming:  config.ClaimTiming,
			ClaimNow:     config.Now,
		})
		if err != nil {
			return nil, fmt.Errorf("%w: UnlockPlot RecoveryLoop: %v", ErrInvalidConfig, err)
		}
		composition.UnlockLoop = unlockLoop
		composition.UnlockHandler = unlockHandler
	}
	return composition, nil
}

func validatePlantConfig(config Config) error {
	if config.RouteResolver == nil || strings.TrimSpace(config.Realm) == "" ||
		len(config.PlayerClients) == 0 || len(config.FarmClients) == 0 ||
		strings.TrimSpace(config.BootID) == "" {
		return fmt.Errorf("%w: %v", ErrPlantRouteUnavailable, ErrInvalidConfig)
	}
	return nil
}

// RunOnce keeps both storage families available even if one claim path has a
// transient failure. The returned error is joined after both bounded scans.
func (c *Composition) RunOnce(ctx context.Context) (RoundStats, error) {
	if c == nil {
		return RoundStats{}, fmt.Errorf("%w: nil composition", ErrInvalidConfig)
	}
	var (
		stats RoundStats
		errs  []error
	)
	if c.PlantLoop != nil {
		var err error
		stats.Plant, err = c.PlantLoop.RunOnce(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("Plant recovery: %w", err))
		}
	}
	if c.UnlockLoop != nil {
		var err error
		stats.Unlock, err = c.UnlockLoop.RunOnce(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("UnlockPlot recovery: %w", err))
		}
	}
	return stats, errors.Join(errs...)
}

// PendingDue returns the sum of the Plant/Unlock due-index views. Both loops
// share one physical index, so the gauge is read once.
func (c *Composition) PendingDue(ctx context.Context) (int64, error) {
	if c == nil {
		return 0, fmt.Errorf("%w: nil composition", ErrInvalidConfig)
	}
	var (
		total int64
		errs  []error
	)
	// PlantLoop and UnlockLoop share the same physical due index. PendingDue
	// is an aggregate repository gauge, so read it once rather than counting
	// the same index twice.
	if c.PlantLoop != nil {
		pending, err := c.PlantLoop.PendingDue(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("Plant backlog: %w", err))
		} else {
			total += pending
		}
	}
	if c.PlantLoop == nil && c.UnlockLoop != nil {
		pending, err := c.UnlockLoop.PendingDue(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("UnlockPlot backlog: %w", err))
		} else {
			total += pending
		}
	}
	return total, errors.Join(errs...)
}

type plantHandler struct {
	repository  workflow.WorkRepository
	claimTiming workflow.ClaimTimingSource
	resolver    actorrouting.Resolver
	realm       string
	player      contractspb.WorkerRecoveryIngressClient
	farm        contractspb.WorkerRecoveryIngressClient
	token       string
	bootID      string

	operationTimeout    time.Duration
	participantBudgetMs uint32
	now                 func() time.Time
}

func (h *plantHandler) Resume(ctx context.Context, record *workflow.WorkRecord, lease workflow.LeaseController) error {
	if h == nil || h.repository == nil || h.claimTiming == nil || h.resolver == nil || h.player == nil || h.farm == nil {
		return fmt.Errorf("%w: Plant handler dependencies", ErrInvalidConfig)
	}
	if record == nil || record.Address.WorkType != FamilyPlant {
		return fmt.Errorf("%w: Plant record", ErrInvalidConfig)
	}
	fence := lease.Fence()
	if fence.Owner == "" || fence.Epoch == 0 {
		return workflow.ErrLeaseLost
	}
	safeDeadline, foundDeadline := h.claimTiming.SafeDeadline(record)
	stop := lease.Expired
	if foundDeadline {
		stop = func() bool {
			return lease.Expired() || !h.now().Before(safeDeadline)
		}
	}
	playerInvoker, err := plant.NewWorkerRecoveryInvoker(h.player, h.route, plant.WorkerLeaseContext{
		CallerInstanceID: fence.Owner,
		CallerBootID:     h.bootID,
		LeaseOwnerToken:  h.token,
		LeaseEpoch:       fence.Epoch,
	}, workerInvokerOptions(h.participantBudgetMs)...)
	if err != nil {
		return err
	}
	farmInvoker, err := plant.NewWorkerRecoveryInvoker(h.farm, h.route, plant.WorkerLeaseContext{
		CallerInstanceID: fence.Owner,
		CallerBootID:     h.bootID,
		LeaseOwnerToken:  h.token,
		LeaseEpoch:       fence.Epoch,
	}, workerInvokerOptions(h.participantBudgetMs)...)
	if err != nil {
		return err
	}
	definition, err := plant.NewDefinition(playerInvoker, farmInvoker)
	if err != nil {
		return err
	}
	engine, err := workflow.NewEngine(h.repository, []workflow.Definition{definition}, workflow.WithEngineClock(h.now))
	if err != nil {
		return err
	}
	if h.operationTimeout > 0 {
		operationCtx, cancel := context.WithTimeout(ctx, h.operationTimeout)
		defer cancel()
		return engine.Resume(operationCtx, record, fence, stop)
	}
	return engine.Resume(ctx, record, fence, stop)
}

type unlockHandler struct{ plantHandler *plantHandler }

func (h *unlockHandler) Resume(ctx context.Context, record *workflow.WorkRecord, lease workflow.LeaseController) error {
	base := h.plantHandler
	if base == nil || base.repository == nil || base.claimTiming == nil || base.resolver == nil || base.player == nil || base.farm == nil {
		return fmt.Errorf("%w: UnlockPlot handler dependencies", ErrInvalidConfig)
	}
	if record == nil || record.Address.WorkType != FamilyUnlockPlot {
		return fmt.Errorf("%w: UnlockPlot record", ErrInvalidConfig)
	}
	fence := lease.Fence()
	if fence.Owner == "" || fence.Epoch == 0 {
		return workflow.ErrLeaseLost
	}
	safeDeadline, foundDeadline := base.claimTiming.SafeDeadline(record)
	stop := lease.Expired
	if foundDeadline {
		stop = func() bool {
			return lease.Expired() || !base.now().Before(safeDeadline)
		}
	}
	playerInvoker, err := plant.NewWorkerRecoveryInvoker(base.player, base.route, plant.WorkerLeaseContext{
		CallerInstanceID: fence.Owner,
		CallerBootID:     base.bootID,
		LeaseOwnerToken:  base.token,
		LeaseEpoch:       fence.Epoch,
	}, workerInvokerOptions(base.participantBudgetMs)...)
	if err != nil {
		return err
	}
	farmInvoker, err := plant.NewWorkerRecoveryInvoker(base.farm, base.route, plant.WorkerLeaseContext{
		CallerInstanceID: fence.Owner,
		CallerBootID:     base.bootID,
		LeaseOwnerToken:  base.token,
		LeaseEpoch:       fence.Epoch,
	}, workerInvokerOptions(base.participantBudgetMs)...)
	if err != nil {
		return err
	}
	definition, err := unlockplot.NewDefinition(playerInvoker, farmInvoker)
	if err != nil {
		return err
	}
	engine, err := workflow.NewEngine(base.repository, []workflow.Definition{definition}, workflow.WithEngineClock(base.now))
	if err != nil {
		return err
	}
	if base.operationTimeout > 0 {
		operationCtx, cancel := context.WithTimeout(ctx, base.operationTimeout)
		defer cancel()
		return engine.Resume(operationCtx, record, fence, stop)
	}
	return engine.Resume(ctx, record, fence, stop)
}

func recoveryLeaseDefaults(leaseTTL, safetyMargin time.Duration) (time.Duration, time.Duration) {
	if leaseTTL <= 0 {
		leaseTTL = 10 * time.Second
	}
	if safetyMargin <= 0 {
		safetyMargin = leaseTTL / 5
	}
	if safetyMargin >= leaseTTL {
		safetyMargin = leaseTTL / 5
	}
	return leaseTTL, safetyMargin
}

func workerInvokerOptions(budget uint32) []plant.WorkerRPCInvokerOption {
	if budget == 0 {
		return nil
	}
	return []plant.WorkerRPCInvokerOption{plant.WithWorkerRPCBudgetMs(budget)}
}

func (h *plantHandler) route(ctx context.Context, call workflow.StepCall, command contractspb.ParticipantCommandType, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
	domain, routeDomain, ok := participantRouteDomain(command)
	if !ok || actorID == 0 {
		return nil, fmt.Errorf("%w: unsupported Plant participant route", ErrInvalidConfig)
	}
	assignment, err := h.resolver.Resolve(ctx, actorrouting.Address{Realm: h.realm, Kind: domain, ID: actorID})
	if err != nil {
		return nil, err
	}
	if assignment.Endpoint == "" || assignment.HolderPod == "" || assignment.HolderBootID == "" || assignment.ShardRevision == 0 || assignment.WriterEpoch == 0 || assignment.LeaseEpoch == 0 || assignment.LeaseUntilMs <= 0 {
		return nil, fmt.Errorf("%w: incomplete %s route assignment", actorrouting.ErrStaleRoute, domain)
	}
	leaseUntilMs := uint64(assignment.LeaseUntilMs)
	return &logicrpcpb.ActorRouteHeader{
		RouteDomain:          routeDomain,
		ActorId:              actorID,
		VirtualShard:         assignment.ShardID,
		ShardRevision:        assignment.ShardRevision,
		ExpectedHolderId:     assignment.HolderPod,
		ExpectedHolderBootId: assignment.HolderBootID,
		ExpectedWriterEpoch:  assignment.WriterEpoch,
		ExpectedLeaseEpoch:   assignment.LeaseEpoch,
		ExpectedLeaseUntilMs: &leaseUntilMs,
	}, nil
}

func participantRouteDomain(command contractspb.ParticipantCommandType) (actorrouting.RouteDomain, logicrpcpb.RouteDomain, bool) {
	switch command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_SEED,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_SEED_RESERVATION,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_SEED_RESERVATION:
		return actorrouting.DomainPlayer, logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, true
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLANT:
		return actorrouting.DomainFarm, logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, true
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_PLOT_UNLOCK:
		return actorrouting.DomainFarm, logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, true
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RESERVE_UNLOCK_COIN,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_UNLOCK_COIN_RESERVATION,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_RELEASE_UNLOCK_COIN_RESERVATION:
		return actorrouting.DomainPlayer, logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, true
	default:
		return "", logicrpcpb.RouteDomain_ROUTE_DOMAIN_UNSPECIFIED, false
	}
}

// routedRecoveryClient selects the existing generated Participant client by
// the current static route assignment and attaches the same route metadata as
// the legacy static Actor clients. It deliberately has no Redis dependency.
type routedRecoveryClient struct {
	resolver actorrouting.Resolver
	realm    string
	domain   actorrouting.RouteDomain
	clients  map[string]contractspb.WorkerRecoveryIngressClient
}

func (c *routedRecoveryClient) ResumeStep(ctx context.Context, request *contractspb.ParticipantRequest, options ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	if c == nil || c.resolver == nil || len(c.clients) == 0 || request == nil || request.GetRouteHeader() == nil {
		return nil, fmt.Errorf("%w: routed recovery client", ErrInvalidConfig)
	}
	header := request.GetRouteHeader()
	wantDomain, wantProtoDomain, ok := participantRouteDomain(request.GetParticipantCommand())
	if !ok || wantDomain != c.domain || header.GetRouteDomain() != wantProtoDomain {
		return nil, fmt.Errorf("%w: Participant route-domain separation", ErrInvalidConfig)
	}
	if header.GetActorId() == 0 {
		return nil, fmt.Errorf("%w: Participant actor id", ErrInvalidConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	address := actorrouting.Address{Realm: c.realm, Kind: c.domain, ID: header.GetActorId()}
	assignment, err := c.resolver.Resolve(ctx, address)
	if err != nil {
		return nil, err
	}
	if !sameAssignment(header, assignment) {
		return nil, fmt.Errorf("%w: Participant route header is stale", actorrouting.ErrStaleRoute)
	}
	client := c.clients[assignment.Endpoint]
	if client == nil {
		return nil, fmt.Errorf("%w: Participant endpoint %s", actorrouting.ErrNoAssignment, assignment.Endpoint)
	}
	return client.ResumeStep(actorrouting.WithRoute(ctx, address, assignment), request, options...)
}

func sameAssignment(header *logicrpcpb.ActorRouteHeader, assignment actorrouting.Assignment) bool {
	return header != nil &&
		header.GetVirtualShard() == assignment.ShardID &&
		header.GetShardRevision() == assignment.ShardRevision &&
		header.GetExpectedHolderId() == assignment.HolderPod &&
		header.GetExpectedHolderBootId() == assignment.HolderBootID &&
		header.GetExpectedWriterEpoch() == assignment.WriterEpoch &&
		compatibleLeaseProof(header, assignment)
}

func compatibleLeaseProof(header *logicrpcpb.ActorRouteHeader, assignment actorrouting.Assignment) bool {
	hasEpoch := header.GetExpectedLeaseEpoch() != 0
	hasDeadline := header.ExpectedLeaseUntilMs != nil
	if !hasEpoch && !hasDeadline {
		return true
	}
	return hasEpoch && hasDeadline && header.GetExpectedLeaseUntilMs() != 0 &&
		assignment.LeaseEpoch != 0 && assignment.LeaseUntilMs > 0 &&
		header.GetExpectedLeaseEpoch() == assignment.LeaseEpoch &&
		uint64(assignment.LeaseUntilMs) >= header.GetExpectedLeaseUntilMs()
}
