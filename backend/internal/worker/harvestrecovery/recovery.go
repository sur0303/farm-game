// Package harvestrecovery composes the generic worker runtime with the
// two-step Harvest definition. It never creates a new operation and never
// touches the legacy workflow queue.
package harvestrecovery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	"farmgame/internal/scenario/harvest"
	"farmgame/internal/workflow"

	"google.golang.org/grpc"
)

const FamilyHarvest = "HARVEST"

var (
	ErrInvalidConfig           = errors.New("harvest recovery: invalid configuration")
	ErrHarvestRouteUnavailable = errors.New("harvest recovery: Harvest route or Participant clients are unavailable")
)

type Config struct {
	Repository workflow.WorkRepository

	RouteResolver actorrouting.Resolver
	Realm         string
	PlayerClients map[string]harvest.WorkerRecoveryParticipantClient
	FarmClients   map[string]harvest.WorkerRecoveryParticipantClient
	Player        harvest.WorkerRecoveryParticipantClient
	Farm          harvest.WorkerRecoveryParticipantClient
	Route         harvest.RouteProvider

	Owner        string
	Token        string
	BootID       string
	ClaimLimit   int
	LeaseTTL     time.Duration
	SafetyMargin time.Duration
	ClaimTiming  *workflow.ClaimTimingRegistry

	OperationTimeout    time.Duration
	ParticipantBudgetMs uint32
	Now                 func() time.Time
}

type Composition struct {
	HarvestLoop       *workflow.RecoveryLoop
	HarvestRepository workflow.WorkRepository
	HarvestHandler    workflow.WorkHandler
}

type RoundStats struct {
	Harvest workflow.RecoveryStats
}

func New(config Config) (*Composition, error) {
	if config.Repository == nil || strings.TrimSpace(config.Owner) == "" || strings.TrimSpace(config.Token) == "" || strings.TrimSpace(config.BootID) == "" {
		return nil, fmt.Errorf("%w: repository, owner, token, and boot id are required", ErrInvalidConfig)
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
	if config.Route == nil {
		if config.RouteResolver == nil || strings.TrimSpace(config.Realm) == "" {
			return nil, fmt.Errorf("%w: %v", ErrHarvestRouteUnavailable, ErrInvalidConfig)
		}
		config.Route = recoveryRouteProvider(config.RouteResolver, config.Realm)
	}
	player, farm := config.Player, config.Farm
	if player == nil || farm == nil {
		if config.RouteResolver == nil || strings.TrimSpace(config.Realm) == "" || len(config.PlayerClients) == 0 || len(config.FarmClients) == 0 || strings.TrimSpace(config.BootID) == "" {
			return nil, fmt.Errorf("%w: %v", ErrHarvestRouteUnavailable, ErrInvalidConfig)
		}
		var err error
		player, err = newRoutedRecoveryClient(config.RouteResolver, config.Realm, actorrouting.DomainPlayer, config.PlayerClients)
		if err != nil {
			return nil, err
		}
		farm, err = newRoutedRecoveryClient(config.RouteResolver, config.Realm, actorrouting.DomainFarm, config.FarmClients)
		if err != nil {
			return nil, err
		}
	}
	leaseTTL, safetyMargin := recoveryLeaseDefaults(config.LeaseTTL, config.SafetyMargin)
	handler := &harvestHandler{
		repository: config.Repository, claimTiming: config.ClaimTiming, route: config.Route,
		player: player, farm: farm, token: config.Token, bootID: config.BootID,
		operationTimeout: config.OperationTimeout, participantBudgetMs: config.ParticipantBudgetMs,
		now: config.Now,
	}
	loop, err := workflow.NewRecoveryLoop(config.Repository, map[string]workflow.WorkHandler{FamilyHarvest: handler}, workflow.RecoveryConfig{
		Owner: config.Owner, Token: config.Token, ClaimLimit: config.ClaimLimit,
		LeaseTTL: leaseTTL, SafetyMargin: safetyMargin,
		ClaimTiming: config.ClaimTiming, ClaimNow: config.Now,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: Harvest RecoveryLoop: %v", ErrInvalidConfig, err)
	}
	return &Composition{HarvestLoop: loop, HarvestRepository: config.Repository, HarvestHandler: handler}, nil
}

func (c *Composition) RunOnce(ctx context.Context) (RoundStats, error) {
	if c == nil || c.HarvestLoop == nil {
		return RoundStats{}, fmt.Errorf("%w: nil composition", ErrInvalidConfig)
	}
	stats, err := c.HarvestLoop.RunOnce(ctx)
	return RoundStats{Harvest: stats}, err
}

func (c *Composition) PendingDue(ctx context.Context) (int64, error) {
	if c == nil || c.HarvestLoop == nil {
		return 0, fmt.Errorf("%w: nil composition", ErrInvalidConfig)
	}
	return c.HarvestLoop.PendingDue(ctx)
}

type harvestHandler struct {
	repository  workflow.WorkRepository
	claimTiming workflow.ClaimTimingSource
	route       harvest.RouteProvider
	player      harvest.WorkerRecoveryParticipantClient
	farm        harvest.WorkerRecoveryParticipantClient
	token       string
	bootID      string

	operationTimeout    time.Duration
	participantBudgetMs uint32
	now                 func() time.Time
}

func (h *harvestHandler) Resume(ctx context.Context, record *workflow.WorkRecord, lease workflow.LeaseController) error {
	if h == nil || h.repository == nil || h.claimTiming == nil || h.route == nil || h.player == nil || h.farm == nil {
		return fmt.Errorf("%w: Harvest handler dependencies", ErrInvalidConfig)
	}
	if record == nil || record.Address.WorkType != FamilyHarvest {
		return fmt.Errorf("%w: Harvest record", ErrInvalidConfig)
	}
	fence := lease.Fence()
	if fence.Owner == "" || fence.Epoch == 0 {
		return workflow.ErrLeaseLost
	}
	safeDeadline, foundDeadline := h.claimTiming.SafeDeadline(record)
	stop := lease.Expired
	if foundDeadline {
		stop = func() bool { return lease.Expired() || !h.now().Before(safeDeadline) }
	}
	playerInvoker, err := harvest.NewWorkerRecoveryInvoker(h.player, h.route, harvest.WorkerLeaseContext{
		CallerInstanceID: fence.Owner, CallerBootID: h.bootID, LeaseOwnerToken: h.token, LeaseEpoch: fence.Epoch,
	}, workerInvokerOptions(h.participantBudgetMs)...)
	if err != nil {
		return err
	}
	farmInvoker, err := harvest.NewWorkerRecoveryInvoker(h.farm, h.route, harvest.WorkerLeaseContext{
		CallerInstanceID: fence.Owner, CallerBootID: h.bootID, LeaseOwnerToken: h.token, LeaseEpoch: fence.Epoch,
	}, workerInvokerOptions(h.participantBudgetMs)...)
	if err != nil {
		return err
	}
	definition, err := harvest.NewDefinition(playerInvoker, farmInvoker)
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

func workerInvokerOptions(budget uint32) []harvest.WorkerRPCInvokerOption {
	if budget == 0 {
		return nil
	}
	return []harvest.WorkerRPCInvokerOption{harvest.WithWorkerRPCBudgetMs(budget)}
}

func recoveryLeaseDefaults(leaseTTL, safetyMargin time.Duration) (time.Duration, time.Duration) {
	if leaseTTL <= 0 {
		leaseTTL = 10 * time.Second
	}
	if safetyMargin <= 0 || safetyMargin >= leaseTTL {
		safetyMargin = leaseTTL / 5
	}
	return leaseTTL, safetyMargin
}

func recoveryRouteProvider(resolver actorrouting.Resolver, realm string) harvest.RouteProvider {
	return func(ctx context.Context, _ workflow.StepCall, command contractspb.ParticipantCommandType, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
		if resolver == nil || realm == "" || actorID == 0 {
			return nil, fmt.Errorf("%w: route resolver", ErrInvalidConfig)
		}
		domain, protoDomain, ok := participantRouteDomain(command)
		if !ok {
			return nil, fmt.Errorf("%w: unsupported Harvest participant route", ErrInvalidConfig)
		}
		assignment, err := resolver.Resolve(ctx, actorrouting.Address{Realm: realm, Kind: domain, ID: actorID})
		if err != nil {
			return nil, err
		}
		if assignment.Endpoint == "" || assignment.HolderPod == "" || assignment.HolderBootID == "" || assignment.ShardRevision == 0 || assignment.WriterEpoch == 0 || assignment.LeaseEpoch == 0 || assignment.LeaseUntilMs <= 0 {
			return nil, fmt.Errorf("%w: incomplete route assignment", actorrouting.ErrStaleRoute)
		}
		leaseUntilMs := uint64(assignment.LeaseUntilMs)
		return &logicrpcpb.ActorRouteHeader{
			RouteDomain: protoDomain, ActorId: actorID, VirtualShard: assignment.ShardID,
			ShardRevision: assignment.ShardRevision, ExpectedHolderId: assignment.HolderPod,
			ExpectedHolderBootId: assignment.HolderBootID, ExpectedWriterEpoch: assignment.WriterEpoch,
			ExpectedLeaseEpoch: assignment.LeaseEpoch, ExpectedLeaseUntilMs: &leaseUntilMs,
		}, nil
	}
}

func participantRouteDomain(command contractspb.ParticipantCommandType) (actorrouting.RouteDomain, logicrpcpb.RouteDomain, bool) {
	switch command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_COMMIT_HARVEST:
		return actorrouting.DomainFarm, logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, true
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_GRANT_HARVEST_REWARD:
		return actorrouting.DomainPlayer, logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, true
	default:
		return "", logicrpcpb.RouteDomain_ROUTE_DOMAIN_UNSPECIFIED, false
	}
}

type routedRecoveryClient struct {
	resolver actorrouting.Resolver
	realm    string
	domain   actorrouting.RouteDomain
	clients  map[string]harvest.WorkerRecoveryParticipantClient
}

func newRoutedRecoveryClient(resolver actorrouting.Resolver, realm string, domain actorrouting.RouteDomain, clients map[string]harvest.WorkerRecoveryParticipantClient) (*routedRecoveryClient, error) {
	if resolver == nil || realm == "" || (domain != actorrouting.DomainPlayer && domain != actorrouting.DomainFarm) || len(clients) == 0 {
		return nil, fmt.Errorf("%w: routed recovery client configuration", ErrInvalidConfig)
	}
	copyClients := make(map[string]harvest.WorkerRecoveryParticipantClient, len(clients))
	for endpoint, client := range clients {
		if endpoint != "" && client != nil {
			copyClients[endpoint] = client
		}
	}
	if len(copyClients) == 0 {
		return nil, fmt.Errorf("%w: routed recovery clients are empty", ErrInvalidConfig)
	}
	return &routedRecoveryClient{resolver: resolver, realm: realm, domain: domain, clients: copyClients}, nil
}

func (c *routedRecoveryClient) ResumeStep(ctx context.Context, request *contractspb.ParticipantRequest, options ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	if c == nil || c.resolver == nil || request == nil || request.GetRouteHeader() == nil || request.GetRouteHeader().GetActorId() == 0 {
		return nil, fmt.Errorf("%w: routed recovery request", ErrInvalidConfig)
	}
	header := request.GetRouteHeader()
	wantDomain, wantProtoDomain, ok := participantRouteDomain(request.GetParticipantCommand())
	if !ok || wantDomain != c.domain || header.GetRouteDomain() != wantProtoDomain {
		return nil, fmt.Errorf("%w: Participant route-domain separation", ErrInvalidConfig)
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
	return header != nil && header.GetVirtualShard() == assignment.ShardID &&
		header.GetShardRevision() == assignment.ShardRevision && header.GetExpectedHolderId() == assignment.HolderPod &&
		header.GetExpectedHolderBootId() == assignment.HolderBootID && header.GetExpectedWriterEpoch() == assignment.WriterEpoch &&
		compatibleLeaseProof(header, assignment)
}

func compatibleLeaseProof(header *logicrpcpb.ActorRouteHeader, assignment actorrouting.Assignment) bool {
	return header != nil && actorrouting.CompatibleLeaseProof(
		header.GetExpectedLeaseEpoch(), header.ExpectedLeaseUntilMs, assignment, true,
	)
}
