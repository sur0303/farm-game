// Package theftonline owns the production composition boundary for THEFT.
// Pair remains the only execution-accept authority; dest propagation is
// THEFT_VISITOR/THEFT_GUARD SourceOrders. This package does not create dest WorkRecords.
package theftonline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	contractspb "farmgame/api/gen/contracts"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/actorrouting"
	farmingress "farmgame/internal/farm/ingress"
	"farmgame/internal/friend"
	"farmgame/internal/scenario/theft"
	"farmgame/internal/worker/theftrecovery"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc"
)

var (
	ErrInvalidComposition  = errors.New("theft online: invalid composition")
	ErrCoordinatorDown     = errors.New("theft online: coordinator unavailable")
	ErrDispatchUnavailable = errors.New("theft online: dispatch unavailable")
)

// Config is deliberately physical: production must provide the Redis
// WorkRepository, a versioned placement resolver, and real typed Participant
// routes. Direct invokers are retained for the local Farm Participant server
// and hermetic composition tests; no constructor creates a memory fallback.
type Config struct {
	Repository     workflow.WorkRepository
	BucketResolver workidentity.BucketResolver
	BucketVersion  uint32
	Now            func() time.Time
	Deadline       time.Duration

	RouteResolver   actorrouting.Resolver
	Realm           string
	PlayerClients   map[string]theft.CoordinatorParticipantClient
	FarmClients     map[string]theft.CoordinatorParticipantClient
	Player          workflow.ParticipantInvoker
	Farm            workflow.ParticipantInvoker
	ResultPublisher theft.ResultPublisher
}

// Coordinator is the online fast path. A public admission may call Start
// once; all subsequent attempts converge through CreateIfAbsent and the same
// WorkRecord. A non-terminal or uncertain participant result is returned as
// PROCESSING by the caller, while the durable DueIndex remains authoritative.
type Coordinator struct {
	scenario  *theft.Coordinator
	publisher theft.ResultPublisher
}

func New(config Config) (*Coordinator, error) {
	if config.Repository == nil || config.BucketResolver == nil || config.BucketVersion == 0 || config.ResultPublisher == nil {
		return nil, fmt.Errorf("%w: Redis WorkRepository, placement, bucket version and ResultPublisher are required", ErrInvalidComposition)
	}
	if _, ok := config.Repository.(*workflow.RedisWorkRepository); !ok {
		return nil, fmt.Errorf("%w: THEFT Coordinator requires the production Redis WorkRepository", ErrInvalidComposition)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	player, farm := config.Player, config.Farm
	if player == nil || farm == nil {
		if config.RouteResolver == nil || strings.TrimSpace(config.Realm) == "" || len(config.PlayerClients) == 0 || len(config.FarmClients) == 0 {
			return nil, fmt.Errorf("%w: both direct invokers or complete Player/Farm route clients are required", ErrInvalidComposition)
		}
		playerClient, err := newRoutedCoordinatorClient(config.RouteResolver, config.Realm, actorrouting.DomainPlayer, config.PlayerClients)
		if err != nil {
			return nil, err
		}
		farmClient, err := newRoutedCoordinatorClient(config.RouteResolver, config.Realm, actorrouting.DomainFarm, config.FarmClients)
		if err != nil {
			return nil, err
		}
		player, err = theft.NewCoordinatorInvoker(playerClient, CoordinatorRouteProvider(config.RouteResolver, config.Realm))
		if err != nil {
			return nil, err
		}
		farm, err = theft.NewCoordinatorInvoker(farmClient, CoordinatorRouteProvider(config.RouteResolver, config.Realm))
		if err != nil {
			return nil, err
		}
	}
	scenario, err := theft.NewCoordinator(theft.CoordinatorConfig{
		Repository: config.Repository, Visitor: player, Owner: player, Farm: farm,
		BucketResolver: config.BucketResolver, BucketVersion: config.BucketVersion,
		Deadline: config.Deadline, Now: config.Now,
	})
	if err != nil {
		return nil, err
	}
	return &Coordinator{scenario: scenario, publisher: config.ResultPublisher}, nil
}

func (c *Coordinator) Scenario() *theft.Coordinator {
	if c == nil {
		return nil
	}
	return c.scenario
}

func (c *Coordinator) CreateOrLoad(ctx context.Context, input theft.Input) (*workflow.WorkRecord, bool, error) {
	if c == nil || c.scenario == nil {
		return nil, false, ErrCoordinatorDown
	}
	return c.scenario.Create(ctx, input)
}

func (c *Coordinator) Start(ctx context.Context, input theft.Input) (*workflow.WorkRecord, bool, error) {
	if c == nil || c.scenario == nil {
		return nil, false, ErrCoordinatorDown
	}
	record, created, err := c.scenario.Start(ctx, input)
	if record != nil && err == nil && record.Terminal() && theft.OutcomeFor(record) != theft.OutcomeReconcileRequired {
		if publishErr := c.scenario.PublishResult(ctx, record, c.publisher); publishErr != nil {
			return record, created, publishErr
		}
	}
	return record, created, err
}

func (c *Coordinator) Resume(ctx context.Context, record *workflow.WorkRecord) error {
	if c == nil || c.scenario == nil {
		return ErrCoordinatorDown
	}
	return c.scenario.Resume(ctx, record)
}

// Dispatch is the concrete public TheftDispatchPort. It accepts only the
// persisted Pair order ID, reconstructs target facts from the durable sidecar,
// and commits THEFT_VISITOR/THEFT_GUARD SourceOrders. When configured with a
// WorkLease and the validated ResultOrderProcessor it also accelerates Pair
// terminalization; the durable DueIndex worker remains the recovery fallback.
type Dispatch struct {
	pair               *theftrecovery.FriendPairDispatchRepository
	target             TargetBindingStore
	sourceCommit       func(context.Context, theft.Input) (visitorOrderID, guardOrderID string, err error)
	enqueueVisitor     func(context.Context, uint64, string) error
	enqueueGuard       func(context.Context, uint64, string) error
	leases             theftrecovery.WorkLeaseRepository
	fastPathHolderID   string
	fastPathBootID     string
	fastPathLeaseToken string
	fastPathLeaseTTL   time.Duration
	resultProcessor    theftrecovery.PairResultOrderProcessor
	finalizeNoChange   func(context.Context, theftrecovery.DispatchOrder) error
}

type DispatchConfig struct {
	PairRepositories *friend.RedisFriendshipRepositories
	TargetStore      TargetBindingStore
	MaxPairBuckets   int
	SourceCommit     func(context.Context, theft.Input) (visitorOrderID, guardOrderID string, err error)
	EnqueueVisitor   func(context.Context, uint64, string) error
	EnqueueGuard     func(context.Context, uint64, string) error
	// The optional fast-path lease lets the online accelerator terminalize the
	// Pair dispatch safely. Without it the durable worker remains the owner,
	// preserving the original recovery-only behavior for tests and fallback
	// compositions.
	FastPathHolderID   string
	FastPathBootID     string
	FastPathLeaseToken string
	FastPathLeaseTTL   time.Duration
	ResultProcessor    theftrecovery.PairResultOrderProcessor
	FinalizeNoChange   func(context.Context, theftrecovery.DispatchOrder) error
}

func NewDispatch(config DispatchConfig) (*Dispatch, error) {
	if config.PairRepositories == nil || config.TargetStore == nil || config.SourceCommit == nil {
		return nil, fmt.Errorf("%w: real Pair repositories, target store and source commit are required", ErrDispatchUnavailable)
	}
	fastPathConfigured := strings.TrimSpace(config.FastPathHolderID) != "" ||
		strings.TrimSpace(config.FastPathBootID) != "" || strings.TrimSpace(config.FastPathLeaseToken) != ""
	if fastPathConfigured && (strings.TrimSpace(config.FastPathHolderID) == "" ||
		strings.TrimSpace(config.FastPathBootID) == "" || strings.TrimSpace(config.FastPathLeaseToken) == "") {
		return nil, fmt.Errorf("%w: complete fast-path lease identity is required", ErrDispatchUnavailable)
	}
	if fastPathConfigured && config.FastPathLeaseTTL <= 0 {
		config.FastPathLeaseTTL = 10 * time.Second
	}
	pair, err := theftrecovery.NewFriendPairDispatchRepository(theftrecovery.FriendPairDispatchRepositoryConfig{
		Repository:   config.PairRepositories,
		TargetDecode: targetDecoder(config.TargetStore),
		MaxBuckets:   config.MaxPairBuckets,
	})
	if err != nil {
		return nil, err
	}
	var leases theftrecovery.WorkLeaseRepository
	if fastPathConfigured {
		leases, err = theftrecovery.NewFriendPairWorkLeaseRepository(config.PairRepositories, 100*time.Millisecond, 2*time.Minute)
		if err != nil {
			return nil, err
		}
	}
	return &Dispatch{
		pair: pair, target: config.TargetStore,
		sourceCommit: config.SourceCommit, enqueueVisitor: config.EnqueueVisitor, enqueueGuard: config.EnqueueGuard,
		leases: leases, fastPathHolderID: config.FastPathHolderID, fastPathBootID: config.FastPathBootID,
		fastPathLeaseToken: config.FastPathLeaseToken, fastPathLeaseTTL: config.FastPathLeaseTTL,
		resultProcessor: config.ResultProcessor, finalizeNoChange: config.FinalizeNoChange,
	}, nil
}

var _ farmingress.TheftDispatchPort = (*Dispatch)(nil)
var _ farmingress.TheftTargetBindingRecorder = (*Dispatch)(nil)

func (d *Dispatch) RecordTheftTargetBinding(ctx context.Context, binding farmingress.TheftTargetBinding) error {
	if d == nil || d.target == nil {
		return ErrDispatchUnavailable
	}
	return d.target.Put(ctx, binding)
}

func (d *Dispatch) DispatchTheft(ctx context.Context, orderID string) error {
	if d == nil || d.pair == nil {
		return ErrDispatchUnavailable
	}
	locator, ok := d.target.(DispatchTargetBindingStore)
	if !ok {
		return fmt.Errorf("%w: O(1) dispatch locator is required", ErrDispatchUnavailable)
	}
	binding, err := locator.LookupByDispatchOrderID(ctx, orderID)
	if err != nil {
		return err
	}
	order, err := d.pair.LoadByPairID(ctx, binding.PairID, orderID)
	if err != nil {
		return err
	}
	order, err = theftrecovery.NormalizeDispatchOrder(order)
	if err != nil {
		return err
	}
	input, err := inputFromDispatch(order)
	if err != nil {
		return err
	}
	if d.sourceCommit == nil {
		return fmt.Errorf("%w: theft source commit is required", ErrDispatchUnavailable)
	}

	// The source-order write is idempotent, but Pair terminalization still
	// requires the same durable WorkLease fence used by recovery. A fast-path
	// caller never marks Pair complete merely because a local callback returned.
	var fence theftrecovery.LeaseFence
	if d.leases != nil {
		claim, claimErr := d.leases.Claim(ctx, theftrecovery.ClaimRequest{Order: order,
			HolderID: d.fastPathHolderID, BootID: d.fastPathBootID, LeaseToken: d.fastPathLeaseToken,
			RequestedTTL: d.fastPathLeaseTTL})
		if claimErr != nil {
			return claimErr
		}
		switch claim.Status {
		case theftrecovery.LeaseHeld, theftrecovery.LeaseNotDue, theftrecovery.LeaseNotFound, theftrecovery.LeaseNotClaimable:
			// Another recovery owner, a deferred order, or an already terminal
			// order remains the durable path. Never invoke the Farm mutation
			// without this process's accepted lease.
			return nil
		case theftrecovery.LeaseClaimed:
			order.WorkVersion = claim.WorkVersion
			fence = theftrecovery.LeaseFence{HolderID: d.fastPathHolderID, BootID: d.fastPathBootID,
				LeaseToken: d.fastPathLeaseToken, LeaseEpoch: claim.LeaseEpoch, WorkVersion: claim.WorkVersion}
		default:
			return fmt.Errorf("%w: fast-path lease status %d", ErrDispatchUnavailable, claim.Status)
		}
	}

	visitorID, guardID, commitErr := d.sourceCommit(ctx, input)
	if commitErr != nil {
		return commitErr
	}
	if visitorID == "" && guardID == "" {
		// NO_CHANGE intentionally has no destination SourceOrder. If the
		// optional terminalizer is absent, leave the Pair order due for the
		// durable worker rather than inventing a completion proof.
		if d.finalizeNoChange == nil {
			return nil
		}
		if err := d.finalizeNoChange(ctx, order); err != nil {
			return err
		}
	} else {
		if visitorID != "" && d.enqueueVisitor != nil {
			_ = d.enqueueVisitor(ctx, input.TargetFarmID, visitorID)
		}
		if guardID != "" && d.enqueueGuard != nil {
			_ = d.enqueueGuard(ctx, input.TargetFarmID, guardID)
		}
	}

	if d.leases != nil {
		proof := theftrecovery.CompletionProof{PayloadHash: order.PayloadHash,
			SourceProofHash: theftrecovery.SourceProofHash(order),
			ResultProofHash: SourceOrderAdvanceProof(order, visitorID, guardID)}
		completed, completeErr := d.leases.Complete(ctx, theftrecovery.CompleteRequest{Order: order, Fence: fence, Proof: proof})
		if completeErr != nil {
			return completeErr
		}
		if completed.Status != theftrecovery.LeaseCompleted && completed.Status != theftrecovery.LeaseAlreadyCompleted {
			return fmt.Errorf("%w: fast-path completion status %d", ErrDispatchUnavailable, completed.Status)
		}
	}

	// Publish the Pair result through the same validated processor used by
	// recovery. A failure here leaves the result order due; the worker retries
	// it, so this acceleration never becomes a second authority.
	if d.resultProcessor != nil {
		candidate, candidateErr := d.pair.ResultCandidateByOperation(ctx, input.VisitorPlayerID, input.OwnerPlayerID, input.OperationID)
		if candidateErr != nil {
			return candidateErr
		}
		if _, processErr := d.resultProcessor.ProcessResultOrder(ctx, candidate); processErr != nil {
			return processErr
		}
	}
	return nil
}

func inputFromDispatch(order theftrecovery.DispatchOrder) (theft.Input, error) {
	permit, err := actionPermitProto(order.Permit)
	if err != nil {
		return theft.Input{}, err
	}
	return theft.Input{
		VisitorPlayerID: order.VisitorPlayerID, OwnerPlayerID: order.OwnerPlayerID,
		TargetFarmID: order.TargetFarmID, PlotID: order.PlotID, CropCycleID: order.CropCycleID,
		OperationID: order.OperationID, ActionPermitSnapshot: permit,
		AdmissionHash:        append([]byte(nil), order.TargetBindingHash...),
		WorkflowDeadlineAtMs: order.DeadlineAtMs,
	}, nil
}

func actionPermitProto(permit friend.ActionPermitSnapshot) (*contractspb.ActionPermitSnapshot, error) {
	if err := friend.VerifyAdmittedPermit(permit); err != nil {
		return nil, fmt.Errorf("%w: admitted permit: %v", ErrDispatchUnavailable, err)
	}
	// The domain enum reserves VISIT_FARM and therefore numbers CARE/THEFT
	// differently from the wire ActionPermit scope enum.  Never rely on a
	// numeric cast across these independently frozen registries.
	var scope contractspb.FriendActionScope
	switch permit.Scope {
	case friend.ActionScopeCare:
		scope = contractspb.FriendActionScope_FRIEND_ACTION_SCOPE_CARE
	case friend.ActionScopeTheft:
		scope = contractspb.FriendActionScope_FRIEND_ACTION_SCOPE_THEFT
	default:
		return nil, fmt.Errorf("%w: unsupported ActionPermit scope %d", ErrDispatchUnavailable, permit.Scope)
	}
	return &contractspb.ActionPermitSnapshot{
		PermitId: permit.PermitID, VisitorPlayerId: permit.VisitorPlayerID,
		OwnerPlayerId: permit.OwnerPlayerID, TargetFarmId: permit.TargetFarmID,
		OperationId:          permit.OperationID,
		ActionScope:          scope,
		FriendRelationEpoch:  permit.RelationEpoch,
		CanonicalPayloadHash: append([]byte(nil), permit.PayloadHash...),
		TargetBindingHash:    append([]byte(nil), permit.TargetBindingHash...),
		IssuedAtAuthorityMs:  permit.IssuedAtAuthorityMs, ExpireAtAuthorityMs: permit.ExpireAtAuthorityMs,
		ExecutionAcceptedAtAuthorityMs: permit.ExecutionAcceptedAtAuthorityMs,
	}, nil
}

func targetDecoder(store TargetBindingStore) theftrecovery.FriendPairTheftTargetDecoder {
	return func(ctx context.Context, source friend.PairRecoverySourceOrder, permit friend.ActionPermitSnapshot) (theftrecovery.FriendPairTheftTarget, error) {
		if store == nil {
			return theftrecovery.FriendPairTheftTarget{}, ErrTargetBindingUnavailable
		}
		binding, err := store.Lookup(ctx, TargetBindingLookup{
			PairID: permit.RelationID, VisitorPlayerID: permit.VisitorPlayerID,
			OwnerPlayerID: permit.OwnerPlayerID, TargetFarmID: permit.TargetFarmID,
			OperationID: permit.OperationID, PayloadHash: permit.PayloadHash,
			TargetBindingHash: permit.TargetBindingHash,
		})
		if err != nil {
			return theftrecovery.FriendPairTheftTarget{}, err
		}
		if binding.DispatchOrderID != "" && binding.DispatchOrderID != source.WorkID {
			return theftrecovery.FriendPairTheftTarget{}, fmt.Errorf("%w: dispatch order binding", ErrTargetBindingConflict)
		}
		return theftrecovery.FriendPairTheftTarget{PlotID: binding.PlotID, CropCycleID: binding.CropCycleID}, nil
	}
}

// NewFriendPairTargetDecoder exposes the same durable sidecar decoder to the
// farm-worker composition. Both public dispatch and DueIndex recovery must
// reconstruct exactly the same target tuple.
func NewFriendPairTargetDecoder(store TargetBindingStore) theftrecovery.FriendPairTheftTargetDecoder {
	return targetDecoder(store)
}

// LocalCoordinatorClient is used only for the FarmActor holder in farm-svr;
// Player participants still travel through the routed typed client.
type LocalCoordinatorClient struct {
	Server interface {
		ExecuteStep(context.Context, *contractspb.ParticipantRequest) (*contractspb.ParticipantResponse, error)
	}
}

func (c LocalCoordinatorClient) ExecuteStep(ctx context.Context, request *contractspb.ParticipantRequest, _ ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	if c.Server == nil {
		return nil, ErrCoordinatorDown
	}
	return c.Server.ExecuteStep(ctx, request)
}

// CoordinatorRouteProvider resolves a current actor fence for each retry;
// WorkAddress, invocation and payload remain frozen in the WorkRecord.
func CoordinatorRouteProvider(resolver actorrouting.Resolver, realm string) theft.RouteProvider {
	return func(ctx context.Context, _ workflow.StepCall, command contractspb.ParticipantCommandType, actorID uint64) (*logicrpcpb.ActorRouteHeader, error) {
		if resolver == nil || strings.TrimSpace(realm) == "" || actorID == 0 {
			return nil, fmt.Errorf("%w: route resolver unavailable", ErrInvalidComposition)
		}
		domain, protoDomain, err := routeDomainPair(command)
		if err != nil {
			return nil, err
		}
		assignment, err := resolver.Resolve(ctx, actorrouting.Address{Realm: realm, Kind: domain, ID: actorID})
		if err != nil {
			return nil, err
		}
		if assignment.Endpoint == "" || assignment.HolderPod == "" || assignment.HolderBootID == "" || assignment.ShardRevision == 0 || assignment.WriterEpoch == 0 || assignment.LeaseEpoch == 0 || assignment.LeaseUntilMs <= 0 {
			return nil, fmt.Errorf("%w: incomplete actor route", ErrInvalidComposition)
		}
		leaseUntilMs := uint64(assignment.LeaseUntilMs)
		return &logicrpcpb.ActorRouteHeader{RouteDomain: protoDomain, ActorId: actorID,
			VirtualShard: assignment.ShardID, ShardRevision: assignment.ShardRevision,
			ExpectedHolderId: assignment.HolderPod, ExpectedHolderBootId: assignment.HolderBootID,
			ExpectedWriterEpoch: assignment.WriterEpoch, ExpectedLeaseEpoch: assignment.LeaseEpoch,
			ExpectedLeaseUntilMs: &leaseUntilMs}, nil
	}
}

type routedCoordinatorClient struct {
	resolver actorrouting.Resolver
	realm    string
	domain   actorrouting.RouteDomain
	clients  map[string]theft.CoordinatorParticipantClient
}

func newRoutedCoordinatorClient(resolver actorrouting.Resolver, realm string, domain actorrouting.RouteDomain, clients map[string]theft.CoordinatorParticipantClient) (*routedCoordinatorClient, error) {
	if resolver == nil || strings.TrimSpace(realm) == "" || (domain != actorrouting.DomainPlayer && domain != actorrouting.DomainFarm) || len(clients) == 0 {
		return nil, fmt.Errorf("%w: typed route clients are incomplete", ErrInvalidComposition)
	}
	copyClients := make(map[string]theft.CoordinatorParticipantClient, len(clients))
	for endpoint, client := range clients {
		if strings.TrimSpace(endpoint) != "" && client != nil {
			copyClients[endpoint] = client
		}
	}
	if len(copyClients) == 0 {
		return nil, fmt.Errorf("%w: typed route clients are empty", ErrInvalidComposition)
	}
	return &routedCoordinatorClient{resolver: resolver, realm: realm, domain: domain, clients: copyClients}, nil
}

func (c *routedCoordinatorClient) ExecuteStep(ctx context.Context, request *contractspb.ParticipantRequest, options ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	if c == nil || request == nil || request.GetRouteHeader() == nil || request.GetRouteHeader().GetActorId() == 0 {
		return nil, fmt.Errorf("%w: typed route request", ErrInvalidComposition)
	}
	header := request.GetRouteHeader()
	wantDomain, protoDomain, err := routeDomainPair(request.GetParticipantCommand())
	if err != nil || wantDomain != c.domain || header.GetRouteDomain() != protoDomain {
		return nil, fmt.Errorf("%w: participant route domain", ErrInvalidComposition)
	}
	assignment, err := c.resolver.Resolve(ctx, actorrouting.Address{Realm: c.realm, Kind: c.domain, ID: header.GetActorId()})
	if err != nil {
		return nil, err
	}
	if !sameAssignment(header, assignment) {
		return nil, fmt.Errorf("%w: stale participant route", actorrouting.ErrStaleRoute)
	}
	client := c.clients[assignment.Endpoint]
	if client == nil {
		return nil, fmt.Errorf("%w: missing Participant endpoint", actorrouting.ErrNoAssignment)
	}
	return client.ExecuteStep(actorrouting.WithRoute(ctx, actorrouting.Address{Realm: c.realm, Kind: c.domain, ID: header.GetActorId()}, assignment), request, options...)
}

func routeDomainPair(command contractspb.ParticipantCommandType) (actorrouting.RouteDomain, logicrpcpb.RouteDomain, error) {
	switch command {
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_DECIDE_THEFT:
		return actorrouting.DomainFarm, logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, nil
	case contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_SETTLE_THEFT_VISITOR,
		contractspb.ParticipantCommandType_PARTICIPANT_COMMAND_TYPE_CREDIT_GUARD_INCOME:
		return actorrouting.DomainPlayer, logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, nil
	default:
		return "", logicrpcpb.RouteDomain_ROUTE_DOMAIN_UNSPECIFIED, fmt.Errorf("%w: unsupported Theft Participant command %s", ErrInvalidComposition, command.String())
	}
}

func sameAssignment(header *logicrpcpb.ActorRouteHeader, assignment actorrouting.Assignment) bool {
	return header != nil && header.GetVirtualShard() == assignment.ShardID &&
		header.GetShardRevision() == assignment.ShardRevision &&
		header.GetExpectedHolderId() == assignment.HolderPod &&
		header.GetExpectedHolderBootId() == assignment.HolderBootID &&
		header.GetExpectedWriterEpoch() == assignment.WriterEpoch &&
		compatibleLeaseProof(header, assignment) &&
		(header.GetSourceSnapshotRevision() == 0 || header.GetSourceSnapshotRevision() == assignment.SnapshotRevision)
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

// Keep these compile-time checks close to the composition boundary. The
// typed RPC adapters must continue to use the shared Participant contracts.
var _ theft.RouteProvider = CoordinatorRouteProvider(nil, "")
