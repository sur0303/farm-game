package theftonline

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	contractspb "farmgame/api/gen/contracts"
	"farmgame/internal/actorrouting"
	"farmgame/internal/identity"
	"farmgame/internal/scenario/theft"
	"farmgame/internal/worker/theftrecovery"
	"farmgame/internal/workflow"
	"farmgame/internal/workidentity"

	"google.golang.org/grpc"
)

// WorkerAdvancerConfig is the recovery-only physical composition. It
// requires exact WorkRecord leasing in addition to the Pair source lease so
// WorkerRecoveryIngress can present a real WorkLease proof to Farm/Player.
type WorkerAdvancerConfig struct {
	Repository       workflow.WorkRepository
	BucketResolver   workidentity.BucketResolver
	BucketVersion    uint32
	RouteResolver    actorrouting.Resolver
	Realm            string
	PlayerClients    map[string]theft.WorkerRecoveryParticipantClient
	FarmClients      map[string]theft.WorkerRecoveryParticipantClient
	ResultPublisher  theft.ResultPublisher
	Now              func() time.Time
	LeaseTTL         time.Duration
	OperationTimeout time.Duration
}

type WorkerAdvancer struct {
	repository    workflow.WorkRepository
	leases        workflow.SpecificWorkLeaseRepository
	bucket        workidentity.BucketResolver
	bucketVersion uint32
	resolver      actorrouting.Resolver
	realm         string
	players       map[string]theft.WorkerRecoveryParticipantClient
	farms         map[string]theft.WorkerRecoveryParticipantClient
	publisher     theft.ResultPublisher
	now           func() time.Time
	leaseTTL      time.Duration
	timeout       time.Duration
}

func NewWorkerAdvancer(config WorkerAdvancerConfig) (*WorkerAdvancer, error) {
	if config.Repository == nil || config.BucketResolver == nil || config.BucketVersion == 0 || config.ResultPublisher == nil ||
		config.RouteResolver == nil || strings.TrimSpace(config.Realm) == "" || len(config.PlayerClients) == 0 || len(config.FarmClients) == 0 {
		return nil, fmt.Errorf("%w: Worker Theft requires Redis Work, placement, route and recovery Participant dependencies", ErrInvalidComposition)
	}
	if _, ok := config.Repository.(*workflow.RedisWorkRepository); !ok {
		return nil, fmt.Errorf("%w: THEFT Worker requires the production Redis WorkRepository", ErrInvalidComposition)
	}
	leases, ok := config.Repository.(workflow.SpecificWorkLeaseRepository)
	if !ok || leases == nil {
		return nil, fmt.Errorf("%w: exact WorkRecord lease implementation is required", ErrInvalidComposition)
	}
	players := copyRecoveryClients(config.PlayerClients)
	farms := copyRecoveryClients(config.FarmClients)
	if len(players) == 0 || len(farms) == 0 {
		return nil, fmt.Errorf("%w: recovery Participant clients are empty", ErrInvalidComposition)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.LeaseTTL <= 0 {
		config.LeaseTTL = 30 * time.Second
	}
	if config.OperationTimeout <= 0 {
		config.OperationTimeout = 3 * time.Second
	}
	return &WorkerAdvancer{repository: config.Repository, leases: leases, bucket: config.BucketResolver,
		bucketVersion: config.BucketVersion, resolver: config.RouteResolver, realm: config.Realm,
		players: players, farms: farms, publisher: config.ResultPublisher, now: config.Now,
		leaseTTL: config.LeaseTTL, timeout: config.OperationTimeout}, nil
}

var _ theftrecovery.TheftWorkflowAdvancer = (*WorkerAdvancer)(nil)

func (a *WorkerAdvancer) CreateIfAbsentOrContinue(ctx context.Context, request theftrecovery.AdvanceRequest) (theftrecovery.AdvanceResult, error) {
	if a == nil || a.repository == nil || a.leases == nil {
		return theftrecovery.AdvanceResult{}, ErrCoordinatorDown
	}
	order, err := theftrecovery.NormalizeDispatchOrder(request.Order)
	if err != nil {
		return theftrecovery.AdvanceResult{}, err
	}
	ids, err := theftrecovery.DeriveTheftIdentity(order)
	if err != nil || ids.WorkflowID != request.Identity.WorkflowID {
		return theftrecovery.AdvanceResult{}, fmt.Errorf("%w: worker identity mismatch", ErrInvalidComposition)
	}
	record, _, err := a.createRecord(ctx, order)
	if err != nil {
		return theftrecovery.AdvanceResult{}, err
	}
	if record.Terminal() {
		return a.publishAndProject(ctx, record, true)
	}
	claimed, err := a.leases.ClaimWork(ctx, string(identity.WorkTypeTheft), ids.WorkflowID,
		request.Lease.HolderID, request.Lease.LeaseToken, a.leaseTTL)
	if err != nil {
		if errors.Is(err, workflow.ErrNotDue) {
			return theftrecovery.AdvanceResult{Status: theftrecovery.AdvanceNotCommitted, ReceiptID: ids.WorkflowID}, err
		}
		return theftrecovery.AdvanceResult{}, err
	}
	return a.resumeClaim(ctx, order, ids, claimed.Record, request.Lease)
}

func (a *WorkerAdvancer) ReceiptFirst(ctx context.Context, request theftrecovery.ReceiptRequest) (theftrecovery.AdvanceResult, error) {
	if a == nil || a.repository == nil {
		return theftrecovery.AdvanceResult{}, ErrCoordinatorDown
	}
	ids := request.Identity
	if strings.TrimSpace(ids.WorkflowID) == "" {
		return theftrecovery.AdvanceResult{}, theftrecovery.ErrReceiptUnavailable
	}
	record, err := a.repository.Get(ctx, string(identity.WorkTypeTheft), ids.WorkflowID)
	if err != nil {
		if errors.Is(err, workflow.ErrNotFound) {
			return theftrecovery.AdvanceResult{Status: theftrecovery.AdvanceNotCommitted, ReceiptID: ids.WorkflowID}, theftrecovery.ErrReceiptUnavailable
		}
		return theftrecovery.AdvanceResult{}, err
	}
	if !recordMatchesOrder(record, request.Order) {
		return theftrecovery.AdvanceResult{}, fmt.Errorf("%w: WorkRecord root does not match Pair order", ErrInvalidComposition)
	}
	if !record.Terminal() {
		return theftrecovery.AdvanceResult{Status: theftrecovery.AdvanceNotCommitted, ReceiptID: ids.WorkflowID}, theftrecovery.ErrReceiptUnavailable
	}
	return a.publishAndProject(ctx, record, false)
}

func (a *WorkerAdvancer) createRecord(ctx context.Context, order theftrecovery.DispatchOrder) (*workflow.WorkRecord, bool, error) {
	permit, err := actionPermitProto(order.Permit)
	if err != nil {
		return nil, false, err
	}
	root, err := theft.BuildRoot(theft.Input{VisitorPlayerID: order.VisitorPlayerID, OwnerPlayerID: order.OwnerPlayerID,
		TargetFarmID: order.TargetFarmID, PlotID: order.PlotID, CropCycleID: order.CropCycleID,
		OperationID: order.OperationID, ActionPermitSnapshot: permit, AdmissionHash: order.TargetBindingHash})
	if err != nil {
		return nil, false, err
	}
	if order.WorkflowID != "" && root.WorkflowID != order.WorkflowID {
		return nil, false, fmt.Errorf("%w: root/Pair workflow identity mismatch", ErrInvalidComposition)
	}
	address, err := root.AddressForResolver(a.bucketVersion, a.bucket)
	if err != nil {
		return nil, false, err
	}
	// Pair freezes an independent workflow deadline at admission. A restart
	// reuses that exact absolute value; short permit expiry never truncates an
	// already accepted workflow and restart time never extends it.
	deadline := time.UnixMilli(order.DeadlineAtMs)
	candidate, err := workflow.NewWorkRecord(address, order.VisitorPlayerID, order.OperationID,
		root.PayloadBytes, root.PayloadHash,
		[]string{string(identity.StepDecideTheft), string(identity.StepSettleTheftVisitor), string(identity.StepCreditGuardIncome)},
		a.now(), deadline)
	if err != nil {
		return nil, false, err
	}
	return a.repository.CreateIfAbsent(ctx, candidate)
}

func (a *WorkerAdvancer) resumeClaim(ctx context.Context, order theftrecovery.DispatchOrder, ids theftrecovery.TheftIdentity, record *workflow.WorkRecord, pairLease theftrecovery.LeaseFence) (theftrecovery.AdvanceResult, error) {
	playerClient, err := newRoutedRecoveryClient(a.resolver, a.realm, actorrouting.DomainPlayer, a.players)
	if err != nil {
		return theftrecovery.AdvanceResult{}, err
	}
	farmClient, err := newRoutedRecoveryClient(a.resolver, a.realm, actorrouting.DomainFarm, a.farms)
	if err != nil {
		return theftrecovery.AdvanceResult{}, err
	}
	lease := theft.WorkerLeaseContext{CallerInstanceID: pairLease.HolderID, CallerBootID: pairLease.BootID,
		LeaseOwnerToken: pairLease.LeaseToken, LeaseEpoch: record.LeaseEpoch}
	player, err := theft.NewWorkerRecoveryInvoker(playerClient, CoordinatorRouteProvider(a.resolver, a.realm), lease)
	if err != nil {
		return theftrecovery.AdvanceResult{}, err
	}
	farm, err := theft.NewWorkerRecoveryInvoker(farmClient, CoordinatorRouteProvider(a.resolver, a.realm), lease)
	if err != nil {
		return theftrecovery.AdvanceResult{}, err
	}
	definition, err := theft.NewDefinition(player, player, farm)
	if err != nil {
		return theftrecovery.AdvanceResult{}, err
	}
	engine, err := workflow.NewEngine(a.repository, []workflow.Definition{definition}, workflow.WithEngineClock(a.now))
	if err != nil {
		return theftrecovery.AdvanceResult{}, err
	}
	operationCtx, cancel := context.WithTimeout(ctx, a.timeout)
	err = engine.Resume(operationCtx, record, workflow.LeaseFence{Owner: pairLease.HolderID, Epoch: record.LeaseEpoch}, func() bool {
		return operationCtx.Err() != nil
	})
	cancel()
	if err != nil && !errors.Is(err, workflow.ErrRetryScheduled) {
		return projectRecord(record, false), err
	}
	if record.Terminal() {
		return a.publishAndProject(ctx, record, false)
	}
	return projectRecord(record, false), err
}

func (a *WorkerAdvancer) publishAndProject(ctx context.Context, record *workflow.WorkRecord, already bool) (theftrecovery.AdvanceResult, error) {
	if record.Terminal() && theft.OutcomeFor(record) != theft.OutcomeReconcileRequired {
		if err := theft.PublishResult(ctx, record, a.publisher); err != nil {
			return theftrecovery.AdvanceResult{}, err
		}
	}
	return projectRecord(record, already), nil
}

func projectRecord(record *workflow.WorkRecord, already bool) theftrecovery.AdvanceResult {
	if record == nil {
		return theftrecovery.AdvanceResult{Status: theftrecovery.AdvanceNotCommitted}
	}
	result := theftrecovery.AdvanceResult{ReceiptID: record.Address.WorkID}
	if record.State == workflow.WorkStateCompleted {
		if already {
			result.Status = theftrecovery.AdvanceAlreadyApplied
		} else if theft.OutcomeFor(record) == theft.OutcomeNoChange {
			result.Status = theftrecovery.AdvanceNoChange
		} else {
			result.Status = theftrecovery.AdvanceApplied
		}
	} else if record.State == workflow.WorkStateRejected {
		if theft.OutcomeFor(record) == theft.OutcomeNoChange {
			result.Status = theftrecovery.AdvanceNoChange
		} else {
			result.Status = theftrecovery.AdvanceRejected
		}
	} else if record.State == workflow.WorkStateFailed {
		result.Status = theftrecovery.AdvanceReconcileRequired
		result.ErrorClass = record.LastErrorClass
		return result
	} else {
		result.Status = theftrecovery.AdvanceNotCommitted
	}
	result.ResultProofHash = terminalProof(record)
	return result
}

func terminalProof(record *workflow.WorkRecord) []byte {
	h := sha256.New()
	_, _ = h.Write([]byte(record.Address.WorkID))
	_, _ = h.Write([]byte{byte(record.State)})
	for _, step := range record.Steps {
		_, _ = h.Write([]byte(step.Name))
		_, _ = h.Write([]byte{byte(step.Status)})
		_, _ = h.Write(step.ResultHash[:])
	}
	return h.Sum(nil)
}

func recordMatchesOrder(record *workflow.WorkRecord, order theftrecovery.DispatchOrder) bool {
	if record == nil || record.Address.WorkType != string(identity.WorkTypeTheft) || record.OperationID != order.OperationID ||
		record.SubjectPlayerID != order.VisitorPlayerID || record.DeadlineAtMs != order.DeadlineAtMs {
		return false
	}
	root, err := theft.DecodeRoot(record)
	if err != nil {
		return false
	}
	return root.GetVisitorPlayerId() == order.VisitorPlayerID && root.GetOwnerPlayerId() == order.OwnerPlayerID &&
		root.GetFarmId() == order.TargetFarmID && root.GetPlotId() == order.PlotID && root.GetCropCycleId() == order.CropCycleID
}

func copyRecoveryClients(clients map[string]theft.WorkerRecoveryParticipantClient) map[string]theft.WorkerRecoveryParticipantClient {
	copyClients := make(map[string]theft.WorkerRecoveryParticipantClient, len(clients))
	for endpoint, client := range clients {
		if strings.TrimSpace(endpoint) != "" && client != nil {
			copyClients[endpoint] = client
		}
	}
	return copyClients
}

type routedRecoveryClient struct {
	resolver actorrouting.Resolver
	realm    string
	domain   actorrouting.RouteDomain
	clients  map[string]theft.WorkerRecoveryParticipantClient
}

func newRoutedRecoveryClient(resolver actorrouting.Resolver, realm string, domain actorrouting.RouteDomain, clients map[string]theft.WorkerRecoveryParticipantClient) (*routedRecoveryClient, error) {
	if resolver == nil || realm == "" || len(clients) == 0 {
		return nil, fmt.Errorf("%w: recovery route clients", ErrInvalidComposition)
	}
	return &routedRecoveryClient{resolver: resolver, realm: realm, domain: domain, clients: clients}, nil
}

func NewRoutedFarmRecoveryClient(
	resolver actorrouting.Resolver,
	realm string,
	clients map[string]theft.WorkerRecoveryParticipantClient,
) (FarmRecoveryClient, error) {
	return newRoutedRecoveryClient(resolver, realm, actorrouting.DomainFarm, clients)
}

func (c *routedRecoveryClient) ResumeStep(ctx context.Context, request *contractspb.ParticipantRequest, options ...grpc.CallOption) (*contractspb.ParticipantResponse, error) {
	if c == nil || request == nil || request.GetRouteHeader() == nil {
		return nil, fmt.Errorf("%w: recovery route request", ErrInvalidComposition)
	}
	header := request.GetRouteHeader()
	want, protoDomain, err := routeDomainPair(request.GetParticipantCommand())
	if err != nil || want != c.domain || header.GetRouteDomain() != protoDomain {
		return nil, fmt.Errorf("%w: recovery route domain", ErrInvalidComposition)
	}
	assignment, err := c.resolver.Resolve(ctx, actorrouting.Address{Realm: c.realm, Kind: c.domain, ID: header.GetActorId()})
	if err != nil {
		return nil, err
	}
	if !sameAssignment(header, assignment) {
		return nil, fmt.Errorf("%w: stale recovery route", actorrouting.ErrStaleRoute)
	}
	client := c.clients[assignment.Endpoint]
	if client == nil {
		return nil, fmt.Errorf("%w: missing recovery Participant endpoint", actorrouting.ErrNoAssignment)
	}
	return client.ResumeStep(actorrouting.WithRoute(ctx, actorrouting.Address{Realm: c.realm, Kind: c.domain, ID: header.GetActorId()}, assignment), request, options...)
}
