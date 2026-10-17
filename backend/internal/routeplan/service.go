package routeplan

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	routeplanpb "farmgame/api/gen/routeplan"
	"farmgame/internal/actorrouting"
	"farmgame/internal/holderidentity"
	core "farmgame/internal/routecontrol"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MethodAuthorizer interface {
	Authorize(context.Context, string) error
}

type PlanServiceOptions struct {
	Executor        *PlanExecutor
	SnapshotStore   core.RouteSnapshotStore
	HolderReader    HolderReader
	AuthorityReader AuthorityReader
	Authorizer      MethodAuthorizer
	ControllerID    string
	Boot            string
	Realm           string
	Now             func() time.Time
	// Ready is false while the controller serves Watch during startup recovery.
	// Read-only status remains available, but no Plan mutation may be submitted
	// until Manager and Plan durable recovery complete in authority order.
	Ready func() bool
}

// PlanService is an internal-only, authenticated aggregation service. It
// contains no caller_role field and never exposes the takeover evidence.
type PlanService struct {
	routeplanpb.UnimplementedRoutePlanServiceServer
	executor        *PlanExecutor
	snapshotStore   core.RouteSnapshotStore
	holderReader    HolderReader
	authorityReader AuthorityReader
	authorizer      MethodAuthorizer
	controllerID    string
	boot            string
	realm           string
	now             func() time.Time
	ready           func() bool
}

var _ routeplanpb.RoutePlanServiceServer = (*PlanService)(nil)

func NewPlanService(options PlanServiceOptions) (*PlanService, error) {
	if options.Executor == nil || options.HolderReader == nil || options.AuthorityReader == nil || options.Authorizer == nil {
		return nil, fmt.Errorf("%w: service dependencies", ErrExecutorInvalid)
	}
	if options.ControllerID == "" || options.Boot == "" || options.Realm == "" {
		return nil, fmt.Errorf("%w: controller identity", ErrExecutorInvalid)
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Ready == nil {
		options.Ready = func() bool { return true }
	}
	return &PlanService{
		executor: options.Executor, snapshotStore: options.SnapshotStore, holderReader: options.HolderReader,
		authorityReader: options.AuthorityReader, authorizer: options.Authorizer,
		controllerID: options.ControllerID, boot: options.Boot, realm: options.Realm, now: options.Now, ready: options.Ready,
	}, nil
}

func (s *PlanService) authorize(ctx context.Context, method string) error {
	if s == nil || s.authorizer == nil {
		return status.Error(codes.PermissionDenied, "route plan method is not authorized")
	}
	if err := s.authorizer.Authorize(ctx, method); err != nil {
		return status.Error(codes.PermissionDenied, "route plan method is not authorized")
	}
	return nil
}

func (s *PlanService) requireReady() error {
	if s == nil || s.ready == nil || !s.ready() {
		return status.Error(codes.Unavailable, "route controller recovery is not complete")
	}
	return nil
}

func (s *PlanService) GetStatus(ctx context.Context, _ *routeplanpb.GetStatusRequest) (*routeplanpb.GetStatusResponse, error) {
	if err := s.authorize(ctx, routeplanpb.RoutePlanService_GetStatus_FullMethodName); err != nil {
		return nil, err
	}
	records, err := s.executor.store.ListIncomplete(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	snapshot, err := s.loadSnapshot(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	response := &routeplanpb.GetStatusResponse{ControllerId: s.controllerID, Boot: s.boot, Realm: s.realm,
		SnapshotRevision: snapshot.GlobalRevision, Ready: s.ready != nil && s.ready()}
	for _, record := range records {
		response.IncompletePlans = append(response.IncompletePlans, planProto(record.Plan))
	}
	return response, nil
}

func (s *PlanService) GetSnapshot(ctx context.Context, _ *routeplanpb.GetSnapshotRequest) (*routeplanpb.GetSnapshotResponse, error) {
	if err := s.authorize(ctx, routeplanpb.RoutePlanService_GetSnapshot_FullMethodName); err != nil {
		return nil, err
	}
	stored, err := s.loadStoredSnapshot(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	return &routeplanpb.GetSnapshotResponse{Schema: stored.SchemaVersion, Realm: stored.Realm, Revision: stored.Revision,
		CanonicalJson: append([]byte(nil), stored.CanonicalJSON...), ChecksumSha256: stored.ChecksumSHA256,
		TopologyFingerprint: stored.TopologyFingerprint, CommittedAtMs: stored.CommittedAtMs}, nil
}

func (s *PlanService) GetHolders(ctx context.Context, request *routeplanpb.GetHoldersRequest) (*routeplanpb.GetHoldersResponse, error) {
	if err := s.authorize(ctx, routeplanpb.RoutePlanService_GetHolders_FullMethodName); err != nil {
		return nil, err
	}
	domain, err := parseObservedDomain(request.GetDomain())
	if err != nil {
		return nil, rpcError(err)
	}
	records, err := s.holderReader.List(ctx, domain)
	if err != nil {
		return nil, rpcError(err)
	}
	response := &routeplanpb.GetHoldersResponse{Holders: make([]*routeplanpb.HolderView, 0, len(records))}
	for _, record := range records {
		response.Holders = append(response.Holders, holderProto(record))
	}
	return response, nil
}

func (s *PlanService) GetPlan(ctx context.Context, request *routeplanpb.GetPlanRequest) (*routeplanpb.GetPlanResponse, error) {
	if err := s.authorize(ctx, routeplanpb.RoutePlanService_GetPlan_FullMethodName); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, rpcError(ErrInvalidPlan)
	}
	if err := ValidatePlanID(request.GetPlanId()); err != nil {
		return nil, rpcError(err)
	}
	record, found, err := s.executor.store.Load(ctx, request.GetPlanId())
	if err != nil {
		return nil, rpcError(err)
	}
	if !found {
		return nil, rpcError(ErrPlanNotFound)
	}
	if record.Plan.PlanID != request.GetPlanId() {
		return nil, rpcError(ErrPlanCorrupt)
	}
	return &routeplanpb.GetPlanResponse{Plan: planProto(record.Plan)}, nil
}

func (s *PlanService) GetShard(ctx context.Context, request *routeplanpb.GetShardRequest) (*routeplanpb.GetShardResponse, error) {
	if err := s.authorize(ctx, routeplanpb.RoutePlanService_GetShard_FullMethodName); err != nil {
		return nil, err
	}
	domain, err := parseObservedDomain(request.GetDomain())
	if err != nil {
		return nil, rpcError(err)
	}
	snapshot, err := s.loadSnapshot(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	route, _, err := snapshotRoute(snapshot, domain, request.GetShard())
	if err != nil {
		return nil, rpcError(err)
	}
	authorityValue, found, err := s.authorityReader.ReadShardWriteAuthority(ctx, domain, request.GetShard())
	if err != nil {
		return nil, rpcError(err)
	}
	incomplete, err := s.executor.store.ListIncomplete(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	response := &routeplanpb.GetShardResponse{Domain: string(domain), Shard: request.GetShard(), SnapshotState: string(route.State),
		SnapshotRevision: snapshot.GlobalRevision}
	if found {
		response.Authority = authorityProto(authorityValue)
	}
	for _, record := range incomplete {
		if record.Plan.Domain == domain && record.Plan.VirtualShard == request.GetShard() {
			response.RelatedPlans = append(response.RelatedPlans, planProto(record.Plan))
		}
	}
	return response, nil
}

func (s *PlanService) MoveShard(ctx context.Context, request *routeplanpb.MoveShardRequest) (*routeplanpb.MoveShardResponse, error) {
	if err := s.authorize(ctx, routeplanpb.RoutePlanService_MoveShard_FullMethodName); err != nil {
		return nil, err
	}
	if err := s.requireReady(); err != nil {
		return nil, err
	}
	if request.GetOwnerScalePlanId() != "" || request.GetOwnerRetainUntilMs() != 0 ||
		request.GetSourceGeneration() != nil || request.GetTargetGeneration() != nil {
		return nil, rpcError(fmt.Errorf("%w: ScalePlan-owned children cannot be created through MoveShard", ErrInvalidPlan))
	}
	fence, err := fenceFromProto(request.GetExpectedSourceFence())
	if err != nil {
		return nil, rpcError(err)
	}
	domain, err := parseDomain(request.GetDomain())
	if err != nil {
		return nil, rpcError(err)
	}
	requestedTTL, err := requestedTTLFromMilliseconds(request.GetRequestedTtlMs())
	if err != nil {
		return nil, rpcError(err)
	}
	sourceGeneration, err := holderGenerationFromProto(request.GetSourceGeneration())
	if err != nil {
		return nil, rpcError(err)
	}
	targetGeneration, err := holderGenerationFromProto(request.GetTargetGeneration())
	if err != nil {
		return nil, rpcError(err)
	}
	if request.GetSourceGeneration() != nil &&
		(request.GetSourceGeneration().GetHolderId() != request.GetSourceHolderId() || request.GetSourceGeneration().GetBootId() != request.GetSourceBootId()) {
		return nil, rpcError(ErrInvalidPlan)
	}
	if request.GetTargetGeneration() != nil &&
		(request.GetTargetGeneration().GetHolderId() != request.GetTargetHolderId() || request.GetTargetGeneration().GetBootId() != request.GetTargetBootId()) {
		return nil, rpcError(ErrInvalidPlan)
	}
	record, err := s.executor.SubmitMove(ctx, MoveRequest{
		PlanID: request.GetPlanId(), TransitionID: request.GetTransitionId(), Domain: domain, VirtualShard: request.GetShard(),
		SourceHolderID: request.GetSourceHolderId(), SourceBootID: request.GetSourceBootId(),
		SourceDeploymentIdentity: sourceGeneration, SourceEndpoint: request.GetSourceEndpoint(),
		TargetHolderID: request.GetTargetHolderId(), TargetBootID: request.GetTargetBootId(),
		TargetDeploymentIdentity: targetGeneration, TargetEndpoint: request.GetTargetEndpoint(),
		OwnerScalePlanID: request.GetOwnerScalePlanId(), OwnerRetainUntilMs: request.GetOwnerRetainUntilMs(),
		ExpectedSourceFence: fence, RequestedTTL: requestedTTL,
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return s.executeResponse(ctx, record, func(planID string) (PlanRecord, error) { return s.executor.Execute(ctx, planID) })
}

func (s *PlanService) DrainHolder(ctx context.Context, request *routeplanpb.DrainHolderRequest) (*routeplanpb.DrainHolderResponse, error) {
	if err := s.authorize(ctx, routeplanpb.RoutePlanService_DrainHolder_FullMethodName); err != nil {
		return nil, err
	}
	if err := s.requireReady(); err != nil {
		return nil, err
	}
	domain, err := parseDomain(request.GetDomain())
	if err != nil {
		return nil, rpcError(err)
	}
	requestedTTL, err := requestedTTLFromMilliseconds(request.GetRequestedTtlMs())
	if err != nil {
		return nil, rpcError(err)
	}
	if request.GetPlanIdPrefix() == "" || request.GetTransitionIdPrefix() == "" || request.GetSourceHolderId() == "" || request.GetSourceBootId() == "" || request.GetTargetHolderId() == "" || request.GetTargetBootId() == "" || request.GetTargetEndpoint() == "" {
		return nil, rpcError(ErrInvalidPlan)
	}
	sourceRecord, sourceFound, err := s.holderReader.Get(ctx, domain, request.GetSourceHolderId())
	if err != nil {
		return nil, rpcError(err)
	}
	if !sourceFound {
		return nil, status.Error(codes.NotFound, "source holder not found")
	}
	if !canonicalHolderEndpoints(sourceRecord) || sourceRecord.Domain != domain || sourceRecord.HolderID != request.GetSourceHolderId() ||
		sourceRecord.BootID != request.GetSourceBootId() || !sourceRecord.LeaseUntil.After(s.now()) {
		return nil, status.Error(codes.FailedPrecondition, "source holder generation is not fresh")
	}
	snapshot, err := s.loadSnapshot(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	target, found, err := s.holderReader.Get(ctx, domain, request.GetTargetHolderId())
	if err != nil || !found || !canonicalHolderEndpoints(target) || target.BootID != request.GetTargetBootId() || target.LogicEndpoint != request.GetTargetEndpoint() {
		if err == nil {
			err = ErrPlanConflict
		}
		return nil, rpcError(err)
	}
	_ = target
	response := &routeplanpb.DrainHolderResponse{}
	table := snapshotDomainForService(snapshot, domain)
	matched := 0
	for shard, route := range table.Routes {
		if route.State != actorrouting.ShardStateActive || route.HolderIndex < 0 || route.HolderIndex >= len(snapshot.Holders) {
			continue
		}
		source := snapshot.Holders[route.HolderIndex]
		if source.InstanceID != request.GetSourceHolderId() || source.BootID != request.GetSourceBootId() {
			continue
		}
		matched++
		authorityValue, authorityFound, err := s.authorityReader.ReadShardWriteAuthority(ctx, domain, uint32(shard))
		if err != nil || !authorityFound {
			if err == nil {
				err = ErrPlanConflict
			}
			return nil, rpcError(err)
		}
		plan, err := s.executor.SubmitMove(ctx, MoveRequest{
			PlanID:       request.GetPlanIdPrefix() + "/" + string(domain) + "/" + strconv.Itoa(shard),
			TransitionID: request.GetTransitionIdPrefix() + "/" + string(domain) + "/" + strconv.Itoa(shard),
			Domain:       domain, VirtualShard: uint32(shard), SourceHolderID: source.InstanceID, SourceBootID: source.BootID,
			SourceEndpoint: source.Endpoint, TargetHolderID: request.GetTargetHolderId(), TargetBootID: request.GetTargetBootId(),
			TargetEndpoint: request.GetTargetEndpoint(), ExpectedSourceFence: authorityFence(authorityValue),
			RequestedTTL: requestedTTL,
		})
		if err != nil {
			return nil, rpcError(err)
		}
		result, executeErr := s.executeOrAwaitOwner(ctx, plan, func(planID string) (PlanRecord, error) {
			return s.executor.Execute(ctx, planID)
		})
		if executeErr == nil || result.Plan.PlanID != "" {
			response.Plans = append(response.Plans, planProto(result.Plan))
		}
		if executeErr != nil && (result.Plan.PlanID == "" || !result.Plan.Phase.Terminal()) {
			return nil, rpcError(executeErr)
		}
	}
	if matched == 0 {
		return nil, status.Error(codes.FailedPrecondition, "source holder has no matching active shards")
	}
	return response, nil
}

func (s *PlanService) TakeoverAfterExpiry(ctx context.Context, request *routeplanpb.TakeoverAfterExpiryRequest) (*routeplanpb.TakeoverAfterExpiryResponse, error) {
	if err := s.authorize(ctx, routeplanpb.RoutePlanService_TakeoverAfterExpiry_FullMethodName); err != nil {
		return nil, err
	}
	if err := s.requireReady(); err != nil {
		return nil, err
	}
	fence, err := fenceFromProto(request.GetExpectedSourceFence())
	if err != nil {
		return nil, rpcError(err)
	}
	domain, err := parseDomain(request.GetDomain())
	if err != nil {
		return nil, rpcError(err)
	}
	requestedTTL, err := requestedTTLFromMilliseconds(request.GetRequestedTtlMs())
	if err != nil {
		return nil, rpcError(err)
	}
	snapshot, err := s.loadSnapshot(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	_, source, err := snapshotRoute(snapshot, domain, request.GetShard())
	if err != nil {
		return nil, rpcError(err)
	}
	record, err := s.executor.SubmitCrashTakeover(ctx, TakeoverRequest{
		PlanID: request.GetPlanId(), TransitionID: request.GetTransitionId(), Domain: domain, VirtualShard: request.GetShard(),
		SourceHolderID: request.GetOldHolderId(), SourceBootID: request.GetOldBootId(), SourceEndpoint: source.Endpoint,
		TargetHolderID: request.GetTargetHolderId(), TargetBootID: request.GetTargetBootId(), TargetEndpoint: request.GetTargetEndpoint(),
		ExpectedSourceFence: fence, RequestedTTL: requestedTTL,
		Proof: TakeoverProof{ExpectedHolderBootID: request.GetOldBootId(), HolderUnavailable: request.GetHolderUnavailable(), LeaseExpired: request.GetLeaseExpired(), Evidence: request.GetEvidence()},
	})
	if err != nil {
		log.Printf(
			"route takeover submission rejected domain=%s shard=%d code=%s",
			domain,
			request.GetShard(),
			submissionRejectCode(err),
		)
		return nil, rpcError(err)
	}
	result, executeErr := s.executeOrAwaitOwner(ctx, record, func(planID string) (PlanRecord, error) {
		return s.executor.Execute(ctx, planID)
	})
	if executeErr != nil {
		log.Printf(
			"route takeover execution result domain=%s shard=%d phase=%s plan_code=%s error_code=%s",
			domain,
			request.GetShard(),
			result.Plan.Phase,
			result.Plan.ErrorCode,
			stableErrorCode(executeErr),
		)
	}
	if executeErr != nil && (result.Plan.PlanID == "" || !result.Plan.Phase.Terminal()) {
		return nil, rpcError(executeErr)
	}
	return &routeplanpb.TakeoverAfterExpiryResponse{Plan: planProto(result.Plan)}, nil
}

func (s *PlanService) Verify(ctx context.Context, request *routeplanpb.VerifyRequest) (*routeplanpb.VerifyResponse, error) {
	if err := s.authorize(ctx, routeplanpb.RoutePlanService_Verify_FullMethodName); err != nil {
		return nil, err
	}
	report, err := s.executor.Verify(ctx, request.GetPlanId())
	if err != nil {
		return nil, rpcError(err)
	}
	response := &routeplanpb.VerifyResponse{Consistent: report.Consistent, Plan: planProto(report.Plan.Plan)}
	for _, check := range report.Checks {
		response.Checks = append(response.Checks, &routeplanpb.VerifyCheck{Name: check.Name, Ok: check.OK, ErrorCode: check.Code})
	}
	return response, nil
}

func (s *PlanService) executeResponse(ctx context.Context, record PlanRecord, execute func(string) (PlanRecord, error)) (*routeplanpb.MoveShardResponse, error) {
	result, err := s.executeOrAwaitOwner(ctx, record, execute)
	if err != nil && (result.Plan.PlanID == "" || !result.Plan.Phase.Terminal()) {
		if result.Plan.PlanID != "" {
			log.Printf("route plan nonterminal execution phase=%s attempt=%d error=%v", result.Plan.Phase, result.Plan.Attempt, err)
		}
		return nil, rpcError(err)
	}
	if result.Plan.PlanID == "" {
		result = record
	}
	return &routeplanpb.MoveShardResponse{Plan: planProto(result.Plan)}, nil
}

// executeOrAwaitOwner closes the narrow race where periodic recovery acquires
// the shard after Submit but before the request's synchronous Execute. The
// competing owner is already executing this exact durable Plan; never replay
// it. Wait only on ErrShardBusy and return the terminal record that owner
// commits, bounded by the caller context.
func (s *PlanService) executeOrAwaitOwner(ctx context.Context, submitted PlanRecord, execute func(string) (PlanRecord, error)) (PlanRecord, error) {
	result, err := execute(submitted.Plan.PlanID)
	if !errors.Is(err, ErrShardBusy) {
		return result, err
	}
	for {
		current, found, loadErr := s.executor.store.Load(ctx, submitted.Plan.PlanID)
		if loadErr != nil {
			return current, loadErr
		}
		if !found {
			return PlanRecord{}, ErrPlanNotFound
		}
		if current.Plan.Phase.Terminal() {
			switch current.Plan.Phase {
			case PhaseSucceeded:
				return current, nil
			case PhasePaused:
				return current, ErrPlanPaused
			default:
				return current, ErrPlanFailed
			}
		}
		select {
		case <-ctx.Done():
			return current, errors.Join(ctx.Err(), err)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *PlanService) loadStoredSnapshot(ctx context.Context) (core.StoredRouteSnapshot, error) {
	if s.snapshotStore != nil {
		stored, found, err := s.snapshotStore.LoadCurrent(ctx)
		if err != nil {
			return core.StoredRouteSnapshot{}, err
		}
		if !found {
			return core.StoredRouteSnapshot{}, ErrVerification
		}
		return stored, nil
	}
	snapshot, err := s.loadSnapshot(ctx)
	if err != nil {
		return core.StoredRouteSnapshot{}, err
	}
	return core.CanonicalizeRouteSnapshot(snapshot)
}

func (s *PlanService) loadSnapshot(ctx context.Context) (actorrouting.RouteSnapshot, error) {
	if s.snapshotStore != nil {
		stored, found, err := s.snapshotStore.LoadCurrent(ctx)
		if err != nil {
			return actorrouting.RouteSnapshot{}, err
		}
		if !found {
			return actorrouting.RouteSnapshot{}, ErrVerification
		}
		return stored.Snapshot, nil
	}
	return s.executor.manager.CurrentSnapshot(), nil
}

func parseDomain(value string) (actorrouting.RouteDomain, error) {
	domain := actorrouting.RouteDomain(value)
	if domain != actorrouting.DomainPlayer &&
		domain != actorrouting.DomainFarm &&
		domain != actorrouting.DomainFarmWorker {
		return "", fmt.Errorf("%w: domain", ErrInvalidPlan)
	}
	return domain, nil
}

func parseObservedDomain(value string) (actorrouting.RouteDomain, error) {
	domain := actorrouting.RouteDomain(value)
	if domain != actorrouting.DomainPlayer &&
		domain != actorrouting.DomainFarm &&
		domain != actorrouting.DomainFarmWorker {
		return "", fmt.Errorf("%w: domain", ErrInvalidPlan)
	}
	return domain, nil
}

func requestedTTLFromMilliseconds(value int64) (time.Duration, error) {
	if value < 1 || value > MaxRequestedTTLms {
		return 0, fmt.Errorf("%w: requested ttl", ErrInvalidPlan)
	}
	return time.Duration(value) * time.Millisecond, nil
}

func fenceFromProto(value *routeplanpb.AuthorityFence) (authoritystore.ShardWriteFence, error) {
	if value == nil {
		return authoritystore.ShardWriteFence{}, fmt.Errorf("%w: expected fence required", ErrInvalidPlan)
	}
	return authoritystore.ShardWriteFence{ExpectedHolderID: value.GetHolderId(), ExpectedHolderBootID: value.GetHolderBootId(), ExpectedAssignmentRevision: value.GetAssignmentRevision(), ExpectedWriterEpoch: value.GetWriterEpoch(), ExpectedLeaseEpoch: value.GetLeaseEpoch()}, nil
}

func holderGenerationFromProto(value *routeplanpb.HolderGeneration) (holderidentity.DeploymentIdentity, error) {
	if value == nil || !value.GetDeploymentIdentityPresent() {
		return holderidentity.DeploymentIdentity{}, nil
	}
	identity := holderidentity.DeploymentIdentity{PodUID: value.GetPodUid(), WorkloadUID: value.GetWorkloadUid(), Ordinal: value.GetOrdinal()}
	if err := identity.ValidateExact(); err != nil {
		return holderidentity.DeploymentIdentity{}, fmt.Errorf("%w: deployment identity: %v", ErrInvalidPlan, err)
	}
	return identity, nil
}

func holderGenerationProto(holderID, bootID string, identity holderidentity.DeploymentIdentity) *routeplanpb.HolderGeneration {
	return &routeplanpb.HolderGeneration{HolderId: holderID, BootId: bootID, PodUid: identity.PodUID,
		WorkloadUid: identity.WorkloadUID, Ordinal: identity.Ordinal, DeploymentIdentityPresent: !identity.IsZero()}
}

func authorityFence(value authoritystore.ShardWriteAuthority) authoritystore.ShardWriteFence {
	return authoritystore.ShardWriteFence{ExpectedHolderID: value.HolderID, ExpectedHolderBootID: value.HolderBootID, ExpectedAssignmentRevision: value.AssignmentRevision, ExpectedWriterEpoch: value.WriterEpoch, ExpectedLeaseEpoch: value.LeaseEpoch}
}

func planProto(plan Plan) *routeplanpb.Plan {
	checksum, _ := PlanChecksum(plan)
	return &routeplanpb.Plan{PlanId: plan.PlanID, TransitionId: plan.TransitionID, Domain: string(plan.Domain), Shard: plan.VirtualShard,
		SourceHolderId: plan.SourceHolderID, SourceBootId: plan.SourceBootID, SourceEndpoint: plan.SourceEndpoint,
		TargetHolderId: plan.TargetHolderID, TargetBootId: plan.TargetBootID, TargetEndpoint: plan.TargetEndpoint,
		ExpectedSourceFence: &routeplanpb.AuthorityFence{HolderId: plan.ExpectedSourceFence.ExpectedHolderID, HolderBootId: plan.ExpectedSourceFence.ExpectedHolderBootID, AssignmentRevision: plan.ExpectedSourceFence.ExpectedAssignmentRevision, WriterEpoch: plan.ExpectedSourceFence.ExpectedWriterEpoch, LeaseEpoch: plan.ExpectedSourceFence.ExpectedLeaseEpoch},
		Operation:           operationProto(plan.Operation), Phase: phaseProto(plan.Phase), Attempt: plan.Attempt, Revision: plan.Revision,
		CreatedAtMs: plan.CreatedAtMs, UpdatedAtMs: plan.UpdatedAtMs, StartedAtMs: plan.StartedAtMs, CompletedAtMs: plan.CompletedAtMs,
		ExpiresAtMs: plan.ExpiresAtMs, ErrorCode: plan.ErrorCode, ChecksumSha256: checksum,
		RequestedTtlMs: plan.RequestedTTLms, TakeoverHolderUnavailable: plan.TakeoverProof.HolderUnavailable,
		TakeoverLeaseExpired: plan.TakeoverProof.LeaseExpired,
		SourceGeneration:     holderGenerationProto(plan.SourceHolderID, plan.SourceBootID, plan.SourceDeploymentIdentity),
		TargetGeneration:     holderGenerationProto(plan.TargetHolderID, plan.TargetBootID, plan.TargetDeploymentIdentity),
		OwnerScalePlanId:     plan.OwnerScalePlanID, OwnerRetainUntilMs: plan.OwnerRetainUntilMs}
}

func phaseProto(value Phase) routeplanpb.PlanPhase {
	return map[Phase]routeplanpb.PlanPhase{PhasePending: routeplanpb.PlanPhase_PLAN_PHASE_PENDING, PhaseDraining: routeplanpb.PlanPhase_PLAN_PHASE_DRAINING, PhaseAssigning: routeplanpb.PlanPhase_PLAN_PHASE_ASSIGNING, PhasePreparing: routeplanpb.PlanPhase_PLAN_PHASE_PREPARING, PhaseActive: routeplanpb.PlanPhase_PLAN_PHASE_ACTIVE, PhaseSucceeded: routeplanpb.PlanPhase_PLAN_PHASE_SUCCEEDED, PhaseFailed: routeplanpb.PlanPhase_PLAN_PHASE_FAILED, PhasePaused: routeplanpb.PlanPhase_PLAN_PHASE_PAUSED}[value]
}

func operationProto(value OperationKind) routeplanpb.OperationKind {
	if value == OperationCrashTakeover {
		return routeplanpb.OperationKind_OPERATION_KIND_CRASH_TAKEOVER
	}
	return routeplanpb.OperationKind_OPERATION_KIND_PLANNED_DRAIN
}

func authorityProto(value authoritystore.ShardWriteAuthority) *routeplanpb.AuthorityView {
	return &routeplanpb.AuthorityView{Fence: &routeplanpb.AuthorityFence{HolderId: value.HolderID, HolderBootId: value.HolderBootID, AssignmentRevision: value.AssignmentRevision, WriterEpoch: value.WriterEpoch, LeaseEpoch: value.LeaseEpoch}, Domain: string(value.Domain), Shard: value.VirtualShard, State: value.State.String(), LeaseUntilMs: value.LeaseUntilMs, LastTransitionId: value.LastTransitionID, LastTransitionAtMs: value.LastTransitionAtMs}
}

func holderProto(value core.HolderRecord) *routeplanpb.HolderView {
	return &routeplanpb.HolderView{
		Domain: string(value.Domain), HolderId: value.HolderID, BootId: value.BootID,
		LogicEndpoint: value.LogicEndpoint, HolderControlEndpoint: value.HolderControlEndpoint,
		TlsDnsIdentity: value.TLSDNSIdentity,
		State:          value.State.String(), ProcessReady: value.ProcessReady, Draining: value.Draining,
		ServingShardCount: value.ServingShardCount, SnapshotRevision: value.SnapshotRevision,
		HeartbeatAtMs: value.HeartbeatAt.UnixMilli(), LeaseUntilMs: value.LeaseUntil.UnixMilli(),
		Sequence: value.Sequence, CapacityWeight: value.CapacityWeight,
		PodUid: value.DeploymentIdentity.PodUID, WorkloadUid: value.DeploymentIdentity.WorkloadUID,
		Ordinal: value.DeploymentIdentity.Ordinal, DeploymentIdentityPresent: !value.DeploymentIdentity.IsZero(),
	}
}

func snapshotDomainForService(snapshot actorrouting.RouteSnapshot, domain actorrouting.RouteDomain) actorrouting.DomainSnapshot {
	if domain == actorrouting.DomainPlayer {
		return snapshot.Player
	}
	return snapshot.Farm
}

func submissionRejectCode(err error) string {
	switch {
	case errors.Is(err, ErrShardBusy):
		return "SHARD_BUSY"
	case errors.Is(err, ErrTakeoverProof):
		return "TAKEOVER_PROOF"
	case errors.Is(err, ErrPlanConflict):
		message := err.Error()
		switch {
		case strings.Contains(message, "target holder not ready"):
			return "TARGET_NOT_READY"
		case strings.Contains(message, "source snapshot tuple"):
			return "SOURCE_SNAPSHOT"
		case strings.Contains(message, "source authority fence"):
			return "SOURCE_FENCE"
		case strings.Contains(message, "source authority holder"):
			return "SOURCE_HOLDER"
		default:
			return "FENCE_CONFLICT"
		}
	default:
		return stableErrorCode(err)
	}
}

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	case errors.Is(err, ErrPlanConflict), errors.Is(err, ErrShardBusy):
		return status.Error(codes.Aborted, "route plan conflict")
	case errors.Is(err, ErrInvalidPlan), errors.Is(err, ErrTakeoverProof):
		return status.Error(codes.InvalidArgument, "invalid route plan request")
	case errors.Is(err, ErrPlanNotFound):
		return status.Error(codes.NotFound, "route plan not found")
	case errors.Is(err, ErrVerification):
		return status.Error(codes.FailedPrecondition, "route plan verification failed")
	case errors.Is(err, ErrPlanPaused), errors.Is(err, ErrPlanFailed):
		return status.Error(codes.FailedPrecondition, "route plan is terminal")
	default:
		return status.Error(codes.Internal, "route plan operation failed")
	}
}
