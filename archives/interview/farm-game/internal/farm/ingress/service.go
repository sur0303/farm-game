// Package ingress exposes farm-svr's FarmActor command boundary and coordinates
// idempotent PlayerActor participant steps for cross-authority workflows.
package ingress

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	commonpb "farmgame/api/gen/common"
	contractspb "farmgame/api/gen/contracts"
	farmpb "farmgame/api/gen/farm"
	friendpb "farmgame/api/gen/friend"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	workflowpb "farmgame/api/gen/workflow"
	farmactor "farmgame/internal/farm/actor"
	"farmgame/internal/farm/domain"
	"farmgame/internal/farm/subscription"
	frienddomain "farmgame/internal/friend"
	"farmgame/internal/gateway/session"
	"farmgame/internal/harvestonline"
	"farmgame/internal/identity"
	"farmgame/internal/logic/actor/activation"
	"farmgame/internal/logic/trusted"
	"farmgame/internal/publicprofile"
	"farmgame/internal/storage/authoritystore"
	"farmgame/internal/storage/blobstore"
	"farmgame/internal/storage/redis/rediskey"
	"farmgame/internal/unlockplotonline"
	"farmgame/internal/workflow"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type FriendOpportunityHintPublisher interface {
	PublishFriendOpportunityHint(context.Context, uint64, uint64, uint64) error
}

type SnapshotEnqueuer interface {
	Enqueue(*logicrpcpb.DownlinkItem) error
}

type ResultEnqueuer interface {
	SnapshotEnqueuer
	EnqueueWithResult(*logicrpcpb.DownlinkItem, func(*logicrpcpb.DeliverBatchItemResult)) error
}

// ControlResultEnqueuer provides a reserved per-Gate queue for terminal
// subscription controls. Test transports may implement only ResultEnqueuer;
// the production Dispatcher implements both.
type ControlResultEnqueuer interface {
	EnqueueControlWithResult(*logicrpcpb.DownlinkItem, func(*logicrpcpb.DeliverBatchItemResult)) error
}

// SubscriptionActivator is implemented by the asynchronous downlink
// dispatcher. ActivateSubscription queues SUBSCRIPTION_READY in the exact
// Gate connection writer and returns only the queue-acceptance result; it does
// not wait for the browser to receive or apply the frame.
type SubscriptionActivator interface {
	ActivateSubscription(context.Context, *logicrpcpb.ActivateSubscriptionRequest) (*logicrpcpb.ActivateSubscriptionResponse, error)
}

type FriendPermitAuthority interface {
	Issue(context.Context, uint64, uint64, string, friendpb.FriendAction) (*friendpb.ActionPermit, error)
	VerifySignature(*friendpb.ActionPermit, uint64, uint64, string, friendpb.FriendAction) error
}

// FriendVisitAuthorization is the canonical Pair-backed VisitPermit issue and
// bind seam. The permit remains internal and is bound to one join attempt;
// neither H5 nor an acceleration projection can mint it.
type FriendVisitAuthorization interface {
	AuthorizeVisit(context.Context, uint64, uint64, uint64, string, string, int64, int64) (frienddomain.VisitPermitSnapshot, frienddomain.Outcome, error)
	BindCheck(context.Context, frienddomain.VisitPermitSnapshot, int64) (frienddomain.Outcome, error)
}

// FriendActionAdmission is the Pair Authority Execution-Accept port for
// friend Care/Theft (33 §8.3). Production CARE friend path must use this
// rather than a short-TTL signed permit.
type FriendActionAdmission interface {
	CommitFriendActionAdmission(context.Context, frienddomain.ActionAdmissionCommand) (frienddomain.ActionAdmissionOutcome, error)
	GetFriendActionAdmission(context.Context, frienddomain.ActionAdmissionCommand) (frienddomain.ActionAdmissionOutcome, error)
}

// CareOrderEnqueuer is a best-effort address-only fast-path hint used by
// legacy/multi-order paths such as THEFT. DueIndex remains durable authority.
type CareOrderEnqueuer func(context.Context, uint64, string) error

// CommittedSourceOrderEnqueuer carries the exact immutable AttachedOrder only
// after its source-actor Authority Commit succeeded. It avoids an immediate
// Redis readback; losing the hint is safe because DueIndex remains durable.
type CommittedSourceOrderEnqueuer func(context.Context, uint64, authoritystore.AttachedOrder, *authoritystore.ExactCommitEvidence) error

type Service struct {
	logicrpcpb.UnimplementedLogicServiceServer
	logicrpcpb.UnimplementedFarmWorkflowServiceServer
	registry      *farmactor.Registry
	player        PlayerParticipant
	profileReader publicprofile.Reader
	downlink      SnapshotEnqueuer
	// continuations 是下行投递失败收尾的有界执行器。Dispatcher 在 Gateway
	// lane goroutine 内同步回调投递结果;失效收尾(MarkSubscriptionResyncRequired
	// + Control-lane REJOIN_REQUIRED)必须从 lane 上卸下,避免一次失败阻塞同
	// Gate 的所有连接。并发与排队均有界,过载时 fail closed 同步执行。
	continuations                  *boundedContinuation
	friendPermits                  FriendPermitAuthority
	friendVisitAuthorizer          FriendVisitAuthorization
	friendOpportunityReader        FriendOpportunityReader
	friendOpportunityHints         FriendOpportunityHintPublisher
	friendNavigationVisited        frienddomain.NavigationVisitedRepository
	theftPair                      TheftAdmissionRepository
	theftDispatch                  TheftDispatchPort
	theftTargetRecorder            TheftTargetBindingRecorder
	theftAdmissionStripes          [64]sync.Mutex
	legacyTheftEnabled             bool
	friendActionAdmission          FriendActionAdmission
	careOrderEnqueuer              CommittedSourceOrderEnqueuer
	harvestOrderEnqueuer           CommittedSourceOrderEnqueuer
	sourceCommitEvidence           bool
	playerCoordinator              PlayerCoordinatorExecute
	playerRoute                    ActorRouteResolver
	playerBucketFor                func(uint64) (rediskey.AuthorityBucketRef, error)
	plantOrderDispatcher           SourceOrderDispatcher
	unlockOrderDispatcher          SourceOrderDispatcher
	plantCommittedOrderDispatcher  CommittedPlayerSourceOrderDispatcher
	unlockCommittedOrderDispatcher CommittedPlayerSourceOrderDispatcher
	theftVisitorEnqueuer           CareOrderEnqueuer
	theftGuardEnqueuer             CareOrderEnqueuer
	workflows                      workflow.Store
	plantObserverConfigured        bool
	legacyCareEnabled              bool
	matureAfter                    time.Duration
	now                            func() time.Time
}

func NewService(registry *farmactor.Registry, player PlayerParticipant) *Service {
	service := &Service{
		registry:      registry,
		player:        player,
		now:           time.Now,
		continuations: newBoundedContinuation(defaultDeliveryContinuationWorkers),
	}
	if profileParticipant, ok := player.(publicProfileParticipant); ok {
		service.profileReader = participantProfileReader{participant: profileParticipant}
	}
	return service
}

// SetProfileReader overrides the PlayerParticipant adapter when composition
// already owns a direct or static-routed public-profile reader.
func (s *Service) SetProfileReader(reader publicprofile.Reader) {
	if s != nil {
		s.profileReader = reader
	}
}

func (s *Service) SetDownlinkEnqueuer(enqueuer SnapshotEnqueuer) {
	s.downlink = enqueuer
	if s.registry != nil {
		s.registry.SetResyncNotifier(s.notifyResync)
	}
}

// EmitChange exposes the existing FarmChange delivery path as the
// implementation-level observer accepted by the typed Participant server.
// It is a thin adapter only; all resync/fail-presence behavior remains in
// emitChange.
func (s *Service) EmitChange(farmID uint64, action *farmactor.ActionResult) {
	s.emitChange(farmID, action)
	if s != nil && s.friendOpportunityHints != nil && farmID != 0 && action != nil &&
		action.FriendOpportunityInvalidated && action.Change != nil && action.Change.FarmVersion > 0 {
		opportunityRevision, err := domain.FarmOpportunityRevisionForSource(action.Change.FarmVersion)
		if err == nil {
			publishCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_ = s.friendOpportunityHints.PublishFriendOpportunityHint(publishCtx, farmID, action.Change.FarmVersion, opportunityRevision)
			cancel()
		}
	}
}

// SetPlantParticipantObserverConfigured records that the typed Farm
// Participant was constructed with EmitChange. Once configured, the public
// PLANT command must leave FarmChange publication to the post-CAS Participant
// observer; otherwise the same COMMIT_PLANT would enter the hub twice.
func (s *Service) SetPlantParticipantObserverConfigured() {
	if s != nil {
		s.plantObserverConfigured = true
	}
}

func (s *Service) SetFriendPermitAuthority(authority FriendPermitAuthority) {
	s.friendPermits = authority
}
func (s *Service) SetFriendVisitAuthorizer(authorizer FriendVisitAuthorization) {
	if s != nil {
		s.friendVisitAuthorizer = authorizer
	}
}
func (s *Service) SetFriendOpportunityReader(reader FriendOpportunityReader) {
	if s != nil {
		s.friendOpportunityReader = reader
	}
}
func (s *Service) SetFriendOpportunityHintPublisher(publisher FriendOpportunityHintPublisher) {
	if s != nil {
		s.friendOpportunityHints = publisher
	}
}
func (s *Service) SetFriendNavigationVisitedRepository(repository frienddomain.NavigationVisitedRepository) {
	if s != nil {
		s.friendNavigationVisited = repository
	}
}

// SetTheftPairRepository wires the FRIEND_PAIR ActionAdmission authority used
// by the public THEFT FirstHop. It is deliberately narrower than the full
// PairRepository so the ingress cannot accidentally call unrelated pair
// mutations.
func (s *Service) SetTheftPairRepository(repository TheftAdmissionRepository) {
	s.theftPair = repository
}

// SetTheftDispatchPort wires the fast path to the already-persisted
// FRIEND_ACTION_DISPATCH_ORDER. The port receives only its durable order ID;
// it cannot accept a client-supplied permit or theft result.
func (s *Service) SetTheftDispatchPort(dispatch TheftDispatchPort) {
	s.theftDispatch = dispatch
}

// SetTheftTargetBindingRecorder wires the durable target sidecar used by the
// production dispatch adapter. A missing recorder keeps THEFT not-ready; the
// legacy PrepareTheft path is never used as a substitute.
func (s *Service) SetTheftTargetBindingRecorder(recorder TheftTargetBindingRecorder) {
	s.theftTargetRecorder = recorder
}

// SetLegacyTheftRollbackEnabled is an explicit rollback/test switch. It is
// intentionally false by default and must never be enabled by production
// composition implicitly.
func (s *Service) SetLegacyTheftRollbackEnabled(enabled bool) {
	s.legacyTheftEnabled = enabled
}

// TheftIngressReady reports whether the new THEFT ingress has both authority
// and dispatch dependencies. A missing dependency is an unavailable feature,
// never a reason to silently route to PrepareTheft.
func (s *Service) TheftIngressReady() bool {
	return s != nil && s.theftPair != nil && s.theftDispatch != nil && s.theftTargetRecorder != nil
}

func (s *Service) SetFriendActionAdmission(admission FriendActionAdmission) {
	if s != nil {
		s.friendActionAdmission = admission
	}
}
func (s *Service) SetCareOrderEnqueuer(enqueuer CommittedSourceOrderEnqueuer) {
	if s != nil {
		s.careOrderEnqueuer = enqueuer
	}
}
func (s *Service) SetHarvestOrderEnqueuer(enqueuer CommittedSourceOrderEnqueuer) {
	if s != nil {
		s.harvestOrderEnqueuer = enqueuer
	}
}

// SetSourceCommitEvidenceEnabled opts the source actor into the expensive
// process-local exact-commit carrier. It is false by default and production
// enables it only for an explicit shadow/prefer proof mode. The normal Direct
// AttachedOrder hint remains available while off.
func (s *Service) SetSourceCommitEvidenceEnabled(enabled bool) {
	if s != nil {
		s.sourceCommitEvidence = enabled
	}
}

// SetLegacyCareEnabled is an explicit rollback/test switch. A legacy Farm
// registry never enables CARE implicitly merely because AuthorityEnabled is
// false.
func (s *Service) SetLegacyCareEnabled(enabled bool) {
	if s != nil {
		s.legacyCareEnabled = enabled
	}
}
func (s *Service) SetWorkflowStore(store workflow.Store) { s.workflows = store }
func (s *Service) SetMatureAfter(value time.Duration) {
	if value > 0 {
		s.matureAfter = value
	}
}

func leaveFenceFromTarget(subscriptionID string, target *logicrpcpb.DownlinkTargetFence) farmactor.LeaveRequest {
	if target == nil {
		return farmactor.LeaveRequest{}
	}
	return farmactor.LeaveRequest{
		SubscriptionID: subscriptionID, ViewerPlayerID: target.TargetPlayerId,
		SessionEpoch: target.SessionEpoch, GatewayID: target.GatewayId,
		GatewayBootID: target.GatewayBootId, ConnectionID: target.ConnectionId,
		TransportGeneration: target.ConnectionGeneration,
		RouteRevision:       target.SessionRouteRevision,
	}
}

func validateJoinControl(subscription *logicrpcpb.SubscriptionFence,
	target *logicrpcpb.DownlinkTargetFence) error {
	if subscription == nil || subscription.FarmId == 0 ||
		strings.TrimSpace(subscription.SubscriptionId) == "" ||
		strings.TrimSpace(subscription.JoinAttemptId) == "" {
		return status.Error(codes.InvalidArgument, "complete subscription fence required")
	}
	if !validDownlinkTarget(target) {
		return status.Error(codes.InvalidArgument, "complete downlink target fence required")
	}
	return nil
}

func validDownlinkTarget(target *logicrpcpb.DownlinkTargetFence) bool {
	return target != nil && target.TargetPlayerId != 0 && target.SessionEpoch != 0 &&
		target.GatewayId != "" && target.GatewayBootId != "" && target.ConnectionId != "" &&
		target.ConnectionGeneration != 0 && target.SessionRouteRevision != 0
}

// ArmSnapshot is the explicit distributed ordering barrier after Gate has
// queued the public ACCEPTED. It installs one Snapshot(V) baseline and starts
// the asynchronous downlink continuation; it never waits on network inside an
// Actor turn.
func (s *Service) ArmSnapshot(ctx context.Context, req *logicrpcpb.ArmSnapshotRequest) (*logicrpcpb.ArmSnapshotResponse, error) {
	if req == nil || validateJoinControl(req.Subscription, req.Target) != nil {
		return nil, status.Error(codes.InvalidArgument, "complete ArmSnapshot fence required")
	}
	if s.registry == nil {
		return &logicrpcpb.ArmSnapshotResponse{Status: logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_UNAVAILABLE}, nil
	}
	// The owner profile is read before entering the FarmActor mailbox. The
	// bounded dependency call cannot hold the actor lane, and every failure is
	// closed by removing exactly this joining presence before any downlink.
	profile, profileErr := s.readOwnerProfile(ctx, req.Subscription.FarmId)
	if profileErr != nil {
		s.cleanupJoinControl(req.Subscription, req.Target)
		return &logicrpcpb.ArmSnapshotResponse{Status: logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_UNAVAILABLE}, nil
	}
	value, err := s.registry.Submit(ctx, req.Subscription.FarmId, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		join, replayed, armErr := actor.ArmSnapshot(farmactor.ArmSnapshotRequest{
			SubscriptionID:     req.Subscription.SubscriptionId,
			JoinAttemptID:      req.Subscription.JoinAttemptId,
			Fence:              leaveFenceFromTarget(req.Subscription.SubscriptionId, req.Target),
			OwnerPublicProfile: profile,
		}, s.now())
		if armErr != nil {
			return nil, armErr
		}
		return struct {
			join     *farmactor.JoinResult
			replayed bool
		}{join, replayed}, nil
	})
	if err != nil {
		s.cleanupJoinControl(req.Subscription, req.Target)
		if errors.Is(err, farmactor.ErrJoinNotFound) {
			return &logicrpcpb.ArmSnapshotResponse{Status: logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_NOT_FOUND}, nil
		}
		return &logicrpcpb.ArmSnapshotResponse{Status: logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_UNAVAILABLE}, nil
	}
	armed := value.(struct {
		join     *farmactor.JoinResult
		replayed bool
	})
	if armed.join == nil || armed.join.Snapshot == nil {
		s.cleanupJoinControl(req.Subscription, req.Target)
		return &logicrpcpb.ArmSnapshotResponse{Status: logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_UNAVAILABLE}, nil
	}
	if err := s.enqueueArmedSnapshot(req.Subscription.FarmId, armed.join, req.Target); err != nil {
		return &logicrpcpb.ArmSnapshotResponse{Status: logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_UNAVAILABLE}, nil
	}
	responseStatus := logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_ARMED
	if armed.replayed {
		responseStatus = logicrpcpb.ArmSnapshotStatus_ARM_SNAPSHOT_ALREADY_ARMED
	}
	version := armed.join.Snapshot.FarmVersion
	return &logicrpcpb.ArmSnapshotResponse{Status: responseStatus, SnapshotVersion: &version}, nil
}

const snapshotProfileReadTimeout = 2 * time.Second

func (s *Service) readOwnerProfile(ctx context.Context, ownerID uint64) (*publicpb.PublicProfileView, error) {
	if s == nil || s.profileReader == nil || ownerID == 0 {
		return nil, errors.New("farm ingress: owner profile reader unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	readCtx, cancel := context.WithTimeout(ctx, snapshotProfileReadTimeout)
	defer cancel()
	profile, err := s.profileReader.Read(readCtx, ownerID)
	if err != nil {
		return nil, err
	}
	if err := publicprofile.Validate(profile, ownerID); err != nil {
		return nil, err
	}
	return proto.Clone(profile).(*publicpb.PublicProfileView), nil
}

func (s *Service) SnapshotQueueAccepted(ctx context.Context, req *logicrpcpb.SnapshotQueueAcceptedRequest) (*logicrpcpb.SnapshotQueueAcceptedResponse, error) {
	if req == nil || validateJoinControl(req.Subscription, req.Target) != nil {
		return nil, status.Error(codes.InvalidArgument, "complete SnapshotQueueAccepted fence required")
	}
	value, err := s.registry.Submit(ctx, req.Subscription.FarmId, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		changes, cut, activating, epoch, callbackErr := actor.SnapshotQueueAccepted(
			req.Subscription.SubscriptionId, req.Subscription.JoinAttemptId, req.SnapshotVersion,
			leaveFenceFromTarget(req.Subscription.SubscriptionId, req.Target))
		replayed := errors.Is(callbackErr, subscription.ErrJoinCallbackAlreadyApplied)
		if callbackErr != nil && !replayed {
			return nil, callbackErr
		}
		return struct {
			changes    []*farmpb.FarmChange
			cut        uint64
			activating bool
			epoch      uint64
			replayed   bool
		}{changes, cut, activating, epoch, replayed}, nil
	})
	if err != nil {
		return &logicrpcpb.SnapshotQueueAcceptedResponse{Status: logicrpcpb.JoinCallbackStatus_JOIN_CALLBACK_STALE_FENCE}, nil
	}
	advanced := value.(struct {
		changes    []*farmpb.FarmChange
		cut        uint64
		activating bool
		epoch      uint64
		replayed   bool
	})
	if advanced.replayed {
		return &logicrpcpb.SnapshotQueueAcceptedResponse{
			Status:          logicrpcpb.JoinCallbackStatus_JOIN_CALLBACK_ALREADY_APPLIED,
			ReadyCutVersion: advanced.cut,
		}, nil
	}
	if advanced.activating {
		s.activateJoinAsync(req.Subscription, req.Target, advanced.cut)
	} else if err := s.enqueueCatchupBatch(req.Subscription, req.Target, advanced.changes, advanced.epoch); err != nil {
		return &logicrpcpb.SnapshotQueueAcceptedResponse{Status: logicrpcpb.JoinCallbackStatus_JOIN_CALLBACK_UNAVAILABLE}, nil
	}
	return &logicrpcpb.SnapshotQueueAcceptedResponse{
		Status: logicrpcpb.JoinCallbackStatus_JOIN_CALLBACK_ADVANCED, ReadyCutVersion: advanced.cut,
	}, nil
}

func (s *Service) CatchupBatchQueueAccepted(ctx context.Context, req *logicrpcpb.CatchupBatchQueueAcceptedRequest) (*logicrpcpb.CatchupBatchQueueAcceptedResponse, error) {
	if req == nil || req.FromVersion == 0 || req.ThroughVersion < req.FromVersion ||
		validateJoinControl(req.Subscription, req.Target) != nil {
		return nil, status.Error(codes.InvalidArgument, "complete CatchupBatchQueueAccepted fence required")
	}
	value, err := s.registry.Submit(ctx, req.Subscription.FarmId, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		activating, epoch, callbackErr := actor.CatchupBatchQueueAccepted(req.Subscription.SubscriptionId,
			req.Subscription.JoinAttemptId, req.FromVersion, req.ThroughVersion,
			leaveFenceFromTarget(req.Subscription.SubscriptionId, req.Target))
		replayed := errors.Is(callbackErr, subscription.ErrJoinCallbackAlreadyApplied)
		if callbackErr != nil && !replayed {
			return nil, callbackErr
		}
		return struct {
			activating bool
			epoch      uint64
			replayed   bool
		}{activating, epoch, replayed}, nil
	})
	if err != nil {
		return &logicrpcpb.CatchupBatchQueueAcceptedResponse{Status: logicrpcpb.JoinCallbackStatus_JOIN_CALLBACK_INVALID_RANGE}, nil
	}
	advanced := value.(struct {
		activating bool
		epoch      uint64
		replayed   bool
	})
	if advanced.replayed {
		return &logicrpcpb.CatchupBatchQueueAcceptedResponse{
			Status:                 logicrpcpb.JoinCallbackStatus_JOIN_CALLBACK_ALREADY_APPLIED,
			AcceptedThroughVersion: req.ThroughVersion,
		}, nil
	}
	if advanced.activating {
		s.activateJoinAsync(req.Subscription, req.Target, req.ThroughVersion)
	}
	return &logicrpcpb.CatchupBatchQueueAcceptedResponse{
		Status:                 logicrpcpb.JoinCallbackStatus_JOIN_CALLBACK_ADVANCED,
		AcceptedThroughVersion: req.ThroughVersion,
	}, nil
}

func (s *Service) CleanupConnection(ctx context.Context, _ uint64, connectionID string) error {
	if s == nil || s.registry == nil {
		return nil
	}
	return s.registry.CleanupConnection(ctx, connectionID)
}

func (s *Service) CleanupConnectionFence(ctx context.Context, route session.ActiveRoute) error {
	if s == nil || s.registry == nil {
		return nil
	}
	return s.registry.CleanupConnectionFence(ctx, route)
}

// CancelJoinAttempt resolves the short window where FirstHop may have created
// an unarmed JOINING but Gate has not learned subscription_id. The complete
// attempt/operation/connection fence is routed to the exact Farm Hub lane.
func (s *Service) CancelJoinAttempt(ctx context.Context,
	req *logicrpcpb.CancelJoinAttemptRequest) (*logicrpcpb.CancelJoinAttemptResponse, error) {
	if req == nil || req.TargetFarmId == 0 || strings.TrimSpace(req.JoinAttemptId) == "" ||
		strings.TrimSpace(req.JoinOperationId) == "" || !validDownlinkTarget(req.Target) {
		return nil, status.Error(codes.InvalidArgument, "complete CancelJoinAttempt fence required")
	}
	if s.registry == nil {
		return &logicrpcpb.CancelJoinAttemptResponse{
			Status: logicrpcpb.CancelJoinAttemptStatus_CANCEL_JOIN_ATTEMPT_UNAVAILABLE,
		}, nil
	}
	subscriptionID := ""
	if req.SubscriptionId != nil {
		subscriptionID = strings.TrimSpace(*req.SubscriptionId)
	}
	fence := leaveFenceFromTarget(subscriptionID, req.Target)
	fence.JoinAttemptID = req.JoinAttemptId
	result, err := s.registry.CancelJoinAttempt(ctx, req.TargetFarmId,
		farmactor.CancelJoinAttemptRequest{
			JoinAttemptID: req.JoinAttemptId, OperationID: req.JoinOperationId,
			SubscriptionID: subscriptionID, Fence: fence, Now: s.now(),
		})
	if err != nil {
		return &logicrpcpb.CancelJoinAttemptResponse{
			Status: logicrpcpb.CancelJoinAttemptStatus_CANCEL_JOIN_ATTEMPT_UNAVAILABLE,
		}, nil
	}
	response := &logicrpcpb.CancelJoinAttemptResponse{}
	switch result.Status {
	case farmactor.CancelAttemptCancelled:
		response.Status = logicrpcpb.CancelJoinAttemptStatus_CANCEL_JOIN_ATTEMPT_CANCELLED
	case farmactor.CancelAttemptAlreadyProgressed:
		response.Status = logicrpcpb.CancelJoinAttemptStatus_CANCEL_JOIN_ATTEMPT_ALREADY_PROGRESSED
		if result.SubscriptionID != "" {
			response.SubscriptionId = &result.SubscriptionID
		}
	default:
		response.Status = logicrpcpb.CancelJoinAttemptStatus_CANCEL_JOIN_ATTEMPT_STALE_FENCE
	}
	return response, nil
}

// CleanupSubscription is the normal exact path for Disconnect, SwitchFarm,
// Session replacement, and a progressed Join cancellation. It never scans all
// FarmActors and is safe to replay with the same full fence.
func (s *Service) CleanupSubscription(ctx context.Context,
	req *logicrpcpb.CleanupSubscriptionRequest) (*logicrpcpb.CleanupSubscriptionResponse, error) {
	if req == nil || validateJoinControl(req.Subscription, req.Target) != nil {
		return nil, status.Error(codes.InvalidArgument, "complete CleanupSubscription fence required")
	}
	if s.registry == nil {
		return nil, status.Error(codes.Unavailable, "farm actor registry unavailable")
	}
	fence := leaveFenceFromTarget(req.Subscription.SubscriptionId, req.Target)
	fence.JoinAttemptID = req.Subscription.JoinAttemptId
	removed, err := s.registry.CleanupSubscription(ctx, req.Subscription.FarmId, fence)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &logicrpcpb.CleanupSubscriptionResponse{Removed: removed}, nil
}

// BatchRenewSubscriptions is the W5B Holder boundary. One trusted wall-clock
// sample is shared by the whole batch; each entry is independently serialized
// in its target Farm Hub lane and never activates FarmActor.
func (s *Service) BatchRenewSubscriptions(ctx context.Context,
	req *logicrpcpb.BatchRenewSubscriptionsRequest) (*logicrpcpb.BatchRenewSubscriptionsResponse, error) {
	if req == nil || strings.TrimSpace(req.SourceGatewayId) == "" ||
		strings.TrimSpace(req.SourceGatewayBootId) == "" {
		return nil, status.Error(codes.InvalidArgument, "source gateway fence required")
	}
	trustedNow := time.Now()
	if s != nil && s.now != nil {
		trustedNow = s.now()
	}
	response := &logicrpcpb.BatchRenewSubscriptionsResponse{
		Results: make([]*logicrpcpb.SubscriptionLeaseResult, 0, len(req.Entries)),
	}
	for _, entry := range req.Entries {
		result := &logicrpcpb.SubscriptionLeaseResult{
			Status: logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_UNAVAILABLE,
		}
		if entry != nil {
			result.TargetFarmId = entry.TargetFarmId
			result.SubscriptionId = entry.SubscriptionId
			result.JoinAttemptId = entry.JoinAttemptId
		}
		serverTimeMS := trustedNow.UnixMilli()
		result.ServerTimeMs = &serverTimeMS
		if entry == nil || entry.TargetFarmId == 0 || strings.TrimSpace(entry.SubscriptionId) == "" ||
			strings.TrimSpace(entry.JoinAttemptId) == "" || entry.RequestedTtlMs == 0 ||
			!validDownlinkTarget(entry.Target) {
			response.Results = append(response.Results, result)
			continue
		}
		if entry.Target.GatewayId != req.SourceGatewayId ||
			entry.Target.GatewayBootId != req.SourceGatewayBootId {
			result.Status = logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_STALE_FENCE
			response.Results = append(response.Results, result)
			continue
		}
		if s.registry == nil || s.registry.SubscriptionHub() == nil {
			response.Results = append(response.Results, result)
			continue
		}
		if err := s.registry.ValidateFarmHolder(entry.TargetFarmId); err != nil {
			result.Status = logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_ROUTE_STALE
			response.Results = append(response.Results, result)
			continue
		}
		fence := leaveFenceFromTarget(entry.SubscriptionId, entry.Target)
		fence.JoinAttemptID = entry.JoinAttemptId
		domainResult, err := s.registry.SubscriptionHub().RenewSubscription(ctx, entry.TargetFarmId,
			subscription.SubscriptionLeaseRequest{
				SubscriptionID: entry.SubscriptionId,
				JoinAttemptID:  entry.JoinAttemptId,
				Fence:          fence,
				RequestedTTL:   time.Duration(entry.RequestedTtlMs) * time.Millisecond,
			}, trustedNow)
		if err == nil {
			result.Status = subscriptionLeaseStatus(domainResult.Status)
			result.AcceptedTtlMs = durationMillis(domainResult.AcceptedTTL)
		}
		response.Results = append(response.Results, result)
	}
	return response, nil
}

func subscriptionLeaseStatus(value subscription.SubscriptionLeaseStatus) logicrpcpb.SubscriptionLeaseStatus {
	switch value {
	case subscription.SubscriptionLeaseRenewed:
		return logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_RENEWED
	case subscription.SubscriptionLeaseNotFound:
		return logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_NOT_FOUND
	case subscription.SubscriptionLeaseStaleFence:
		return logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_STALE_FENCE
	case subscription.SubscriptionLeaseClosed:
		return logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_CLOSED
	case subscription.SubscriptionLeaseRouteStale:
		return logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_ROUTE_STALE
	default:
		return logicrpcpb.SubscriptionLeaseStatus_SUBSCRIPTION_LEASE_UNAVAILABLE
	}
}

func durationMillis(value time.Duration) uint32 {
	if value <= 0 {
		return 0
	}
	milliseconds := value / time.Millisecond
	if milliseconds == 0 {
		return 1
	}
	const maxUint32 = uint64(1<<32 - 1)
	if uint64(milliseconds) > maxUint32 {
		return uint32(maxUint32)
	}
	return uint32(milliseconds)
}

func (s *Service) ExecuteCommand(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest) (*logicrpcpb.ExecuteCommandResponse, error) {
	if req == nil || req.Command == nil || req.Trusted == nil {
		return nil, status.Error(codes.InvalidArgument, "command and trusted context required")
	}
	result := &logicrpcpb.BusinessCommandOutcome{Status: commonpb.CommandStatus_SUCCEEDED}
	// 28 §3.6 + 29 §5:FirstHop 独立解码 raw payload、复核 TrustedContext 与
	// DispatchContext(RouteKey 重算必须与 Gate 路由一致),并生成唯一权威
	// CanonicalPayloadHash。disabled/未注册命令与畸形 payload 在业务接受前
	// 明确拒绝,不进入任何 Actor。
	verified, verifyErr := trusted.ValidateCommand(req.Command, req.Trusted, req.Dispatch)
	if verifyErr != nil {
		switch {
		case errors.Is(verifyErr, trusted.ErrCommandDisabled):
			rejectOutcome(result, commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED, "命令已登记但尚未开放")
			return businessResponse(result), nil
		case errors.Is(verifyErr, trusted.ErrPayloadInvalid):
			rejectOutcome(result, commonpb.ErrorCode_ERR_DECODE, verifyErr.Error())
			return businessResponse(result), nil
		default:
			return nil, status.Error(codes.Unauthenticated, verifyErr.Error())
		}
	}
	cmd := req.Command
	if s.registry == nil && !(cmd.CommandType == commonpb.CommandType_THEFT && !s.legacyTheftEnabled) {
		return nil, status.Error(codes.Unavailable, "farm actor registry unavailable")
	}
	sess := req.Trusted.Session
	now := s.now()
	// target 是本端重算并已与 DispatchContext 比对一致的 RouteKey:农场命令即
	// Typed Payload 的 target_farm_id(28 §6.4-13);客户端旧 target_owner_id
	// 兼容字段不再存在,也无法影响目标。
	target := verified.RouteKey

	var action *farmactor.ActionResult
	var harvestResult *harvestonline.CommandResult
	var unlockResult *unlockplotonline.CommandResult
	var value any
	var err error
	switch cmd.CommandType {
	case commonpb.CommandType_JOIN_FARM:
		if target != sess.PlayerId {
			if err := s.authorizeFriendVisit(ctx, req, target, now); err != nil {
				applyError(result, err)
				return businessResponse(result), nil
			}
		}
		value, err = s.join(ctx, req, target, now)
	case commonpb.CommandType_PLANT:
		action, err = s.plant(ctx, req, verified, now)
	case commonpb.CommandType_UNLOCK_PLOT:
		unlockResult, err = s.unlockPlot(ctx, req, verified)
	case commonpb.CommandType_HARVEST:
		harvestResult, err = s.harvestOnlineCommand(ctx, req, verified)
	case commonpb.CommandType_CARE:
		action, err = s.care(ctx, req, verified, now)
	case commonpb.CommandType_THEFT:
		if !s.legacyTheftEnabled {
			admissionOutcome := result
			err := s.admitTheft(ctx, req, verified, now, admissionOutcome)
			if err != nil {
				applyError(admissionOutcome, err)
			}
			return businessResponse(admissionOutcome), nil
		}
		action, err = s.theft(ctx, req, verified, now)
	default:
		err = &farmactor.Error{Code: commonpb.ErrorCode_ERR_UNKNOWN_COMMAND, Message: "command is not routed to farm-svr"}
	}
	if cmd.CommandType == commonpb.CommandType_HARVEST {
		if harvestResult != nil {
			applyHarvestCommandResult(result, harvestResult)
			return businessResponse(result), nil
		}
		applyError(result, err)
		return businessResponse(result), nil
	}
	if cmd.CommandType == commonpb.CommandType_UNLOCK_PLOT {
		if unlockResult == nil {
			applyError(result, err)
			return businessResponse(result), nil
		}
		applyUnlockPlotCommandResult(result, unlockResult)
		if err != nil && result.Status == commonpb.CommandStatus_PROCESSING {
			// SourceOrder is durable; response loss/worker handover is
			// represented as PROCESSING rather than an internal rejection.
			result.RetryDirective = logicrpcpb.RetryDirective_POLL_OPERATION_RESULT
			result.RetryAfterMs = uint32(workflow.InitialRecoveryDelay / time.Millisecond)
		}
		return businessResponse(result), nil
	}
	if err != nil {
		applyError(result, err)
		return businessResponse(result), nil
	}
	if action != nil {
		if cmd.CommandType == commonpb.CommandType_CARE {
			applyCareCommandResult(result, action, target)
			s.EmitChange(target, action)
			return businessResponse(result), nil
		}
		result.Status = action.Status
		if cmd.CommandType == commonpb.CommandType_PLANT && action.Status == commonpb.CommandStatus_PROCESSING {
			result.RetryDirective = logicrpcpb.RetryDirective_POLL_OPERATION_RESULT
			result.RetryAfterMs = uint32(workflow.InitialRecoveryDelay / time.Millisecond)
		}
		result.FarmVersion = action.Result.GetSnapshot().GetFarmVersion()
		result.ResultPayload, err = proto.MarshalOptions{Deterministic: true}.Marshal(action.Result)
		if err != nil {
			applyError(result, err)
		}
		if cmd.CommandType != commonpb.CommandType_PLANT || !s.plantObserverConfigured {
			s.EmitChange(target, action)
		}
		return businessResponse(result), nil
	}
	switch got := value.(type) {
	case *farmactor.JoinResult:
		result.Status = commonpb.CommandStatus_ACCEPTED
		result.OutcomeContext = &logicrpcpb.BusinessCommandOutcome_JoinAccepted{
			JoinAccepted: &logicrpcpb.JoinAcceptedMetadata{
				TargetFarmId: target, SubscriptionId: got.SubscriptionID,
				JoinAttemptId:             got.JoinAttemptID,
				AcceptedSubscriptionTtlMs: durationMillis(got.AcceptedSubscriptionTTL),
			},
		}
	case *farmactor.ActionResult:
		result.Status = got.Status
		result.FarmVersion = got.Result.GetSnapshot().GetFarmVersion()
		result.ResultPayload, _ = proto.MarshalOptions{Deterministic: true}.Marshal(got.Result)
	default:
		applyError(result, errors.New("farm command returned an invalid result"))
	}
	return businessResponse(result), nil
}

func applyHarvestCommandResult(outcome *logicrpcpb.BusinessCommandOutcome, command *harvestonline.CommandResult) {
	if outcome == nil || command == nil {
		return
	}
	outcome.Status = command.Status
	if command.Status == commonpb.CommandStatus_PROCESSING {
		outcome.RetryDirective = logicrpcpb.RetryDirective_POLL_OPERATION_RESULT
		outcome.RetryAfterMs = uint32(workflow.InitialRecoveryDelay / time.Millisecond)
	}
	if command.Error != nil {
		outcome.Error = &logicrpcpb.ErrorDetail{
			ErrorCode:    command.Error.ErrorCode,
			ErrorMessage: command.Error.ErrorMessage,
		}
	}
	if command.Result == nil {
		return
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(command.Result)
	if err != nil {
		outcome.Status = commonpb.CommandStatus_UNAVAILABLE
		outcome.Error = &logicrpcpb.ErrorDetail{ErrorCode: commonpb.ErrorCode_ERR_INTERNAL, ErrorMessage: err.Error()}
		return
	}
	outcome.FarmVersion = command.Result.GetCommittedFarmVersion()
	outcome.ResultPayload = payload
}

// applyCareCommandResult projects the internal CareActionResult into the
// catalog-declared public.CareResult wire payload (28/32). The explicit legacy
// rollback path has no CareActionResult and keeps the previous FarmCommandResult
// envelope so rollback tests remain usable.
func applyCareCommandResult(outcome *logicrpcpb.BusinessCommandOutcome, action *farmactor.ActionResult, targetFarmID uint64) {
	if outcome == nil || action == nil {
		return
	}
	outcome.Status = action.Status
	care := action.CareResult
	if care == nil {
		if action.Result == nil {
			return
		}
		payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(action.Result)
		if err != nil {
			outcome.Status = commonpb.CommandStatus_UNAVAILABLE
			outcome.Error = &logicrpcpb.ErrorDetail{ErrorCode: commonpb.ErrorCode_ERR_INTERNAL, ErrorMessage: err.Error()}
			return
		}
		outcome.FarmVersion = action.Result.GetSnapshot().GetFarmVersion()
		outcome.ResultPayload = payload
		return
	}
	publicResult := &publicpb.CareResult{
		TargetFarmId:            targetFarmID,
		PlotId:                  care.PlotID,
		CropCycleId:             care.CycleID,
		IncidentId:              care.IncidentID,
		CareType:                publicCareType(care.CareType),
		Outcome:                 publicCareOutcome(care.Outcome),
		ResolvedByPlayerId:      care.ResolvedByPlayerID,
		CommittedPlotVersion:    care.CommittedPlotVersion,
		CommittedFarmVersion:    care.CommittedFarmVersion,
		CareRewardOrderId:       care.CareRewardOrderID,
		RewardObligationCreated: care.RewardObligationCreated,
	}
	// Public plot_id uses the same 0-based layout as CareRequest.plot_id.
	if publicResult.PlotId > 0 {
		publicResult.PlotId = publicResult.PlotId - 1
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(publicResult)
	if err != nil {
		outcome.Status = commonpb.CommandStatus_UNAVAILABLE
		outcome.Error = &logicrpcpb.ErrorDetail{ErrorCode: commonpb.ErrorCode_ERR_INTERNAL, ErrorMessage: err.Error()}
		return
	}
	outcome.FarmVersion = care.CommittedFarmVersion
	outcome.ResultPayload = payload
}

func publicCareType(value domain.CareType) publicpb.CareType {
	switch value {
	case domain.CareTypeWater:
		return publicpb.CareType_CARE_TYPE_WATER
	case domain.CareTypeRemovePest:
		return publicpb.CareType_CARE_TYPE_PEST
	default:
		return publicpb.CareType_CARE_TYPE_UNSPECIFIED
	}
}

func publicCareOutcome(value domain.CareOutcome) publicpb.CareOutcome {
	switch value {
	case domain.CareOutcomeResolved:
		return publicpb.CareOutcome_CARE_OUTCOME_RESOLVED
	case domain.CareOutcomeAlreadyResolved:
		return publicpb.CareOutcome_CARE_OUTCOME_ALREADY_RESOLVED
	default:
		return publicpb.CareOutcome_CARE_OUTCOME_UNSPECIFIED
	}
}

// businessResponse 把业务结果包装进合同 29 §9.1 的 oneof business 分支。
func businessResponse(outcome *logicrpcpb.BusinessCommandOutcome) *logicrpcpb.ExecuteCommandResponse {
	return &logicrpcpb.ExecuteCommandResponse{
		Outcome: &logicrpcpb.ExecuteCommandResponse_Business{Business: outcome},
	}
}

func rejectOutcome(outcome *logicrpcpb.BusinessCommandOutcome, code commonpb.ErrorCode, message string) {
	outcome.Status = commonpb.CommandStatus_REJECTED
	outcome.Error = &logicrpcpb.ErrorDetail{ErrorCode: code, ErrorMessage: message}
}

func (s *Service) plant(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest, verified *trusted.VerifiedCommand, now time.Time) (*farmactor.ActionResult, error) {
	return s.plantSourceOrder(ctx, req, verified, now)
}

func (s *Service) unlockPlot(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest, verified *trusted.VerifiedCommand) (*unlockplotonline.CommandResult, error) {
	return s.unlockPlotSourceOrder(ctx, req, verified)
}

func applyUnlockPlotCommandResult(outcome *logicrpcpb.BusinessCommandOutcome, command *unlockplotonline.CommandResult) {
	if outcome == nil || command == nil {
		return
	}
	outcome.Status = command.Status
	if command.Status == commonpb.CommandStatus_PROCESSING {
		return
	}
	if command.Result == nil {
		return
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(command.Result)
	if err != nil {
		outcome.Status = commonpb.CommandStatus_UNAVAILABLE
		outcome.Error = &logicrpcpb.ErrorDetail{ErrorCode: commonpb.ErrorCode_ERR_INTERNAL, ErrorMessage: err.Error()}
		return
	}
	outcome.FarmVersion = command.Result.GetCommittedFarmVersion()
	outcome.ResultPayload = payload
}

// harvestOnlineCommand is the public HARVEST FirstHop. Farm is the source
// actor: one Lua writes the empty plot, receipt, GRANT order and DueIndex.
func (s *Service) harvestOnlineCommand(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest, verified *trusted.VerifiedCommand) (*harvestonline.CommandResult, error) {
	typed, ok := verified.Decoded.(*publicpb.HarvestRequest)
	if !ok || typed == nil {
		return nil, errors.New("Harvest payload is not admitted")
	}
	target := verified.RouteKey
	if target != req.Trusted.Session.PlayerId {
		return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_PERMISSION_DENIED, Message: "只能收获自己的农场"}
	}
	return s.harvestSourceOrder(ctx, req, verified, typed, target)
}

func (s *Service) harvestSourceOrder(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest, verified *trusted.VerifiedCommand, typed *publicpb.HarvestRequest, target uint64) (*harvestonline.CommandResult, error) {
	if s.registry == nil {
		return nil, status.Error(codes.Unavailable, "farm actor registry unavailable")
	}
	now := s.now()
	value, err := s.registry.Submit(ctx, target, func(ctx context.Context, actor *farmactor.FarmActor) (any, error) {
		return actor.HarvestOwner(ctx, farmactor.HarvestOwnerRequest{
			OperationID: req.Command.OperationId, PayloadHash: verified.PayloadHash,
			OwnerPlayerID: req.Trusted.Session.PlayerId, PlotID: typed.GetPlotId() + 1,
			CycleID: typed.GetCropCycleId(), ExpectedPlotVersion: typed.GetExpectedPlotVersion(),
			TrustedNowMs: now.UnixMilli(), WantSourceCommitEvidence: s.sourceCommitEvidence,
		})
	})
	if err != nil {
		return nil, err
	}
	action := value.(*farmactor.ActionResult)
	s.EmitChange(target, action)
	result := &harvestonline.CommandResult{Status: action.Status, Result: action.HarvestPublic}
	if action.Status == commonpb.CommandStatus_SUCCEEDED && action.HarvestOrderID != "" &&
		action.CommittedSourceOrder != nil && s.harvestOrderEnqueuer != nil {
		_ = s.harvestOrderEnqueuer(ctx, target, *action.CommittedSourceOrder, action.SourceCommitEvidence)
	}
	if action.Status == commonpb.CommandStatus_PROCESSING {
		result.Status = commonpb.CommandStatus_PROCESSING
	}
	return result, nil
}

func (s *Service) theft(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest, verified *trusted.VerifiedCommand, now time.Time) (*farmactor.ActionResult, error) {
	cmd := req.Command
	typed := verified.Decoded.(*publicpb.TheftRequest)
	target := verified.RouteKey
	if target == req.Trusted.Session.PlayerId {
		return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_PERMISSION_DENIED, Message: "不能偷自己的作物"}
	}
	request := &farmpb.BeginTheftRequest{PlotIndex: typed.GetPlotId()}
	payloadHash := verified.PayloadHash
	record, err := s.beginWorkflow(ctx, cmd.OperationId, workflowpb.WorkflowKind_THEFT, target, req.Trusted.Session.PlayerId, payloadHash, now)
	if err != nil {
		return nil, err
	}
	permit, err := s.issueFriendPermit(ctx, req.Trusted.Session.PlayerId, target, cmd.OperationId, friendpb.FriendAction_THEFT)
	if err != nil {
		_ = s.finishWorkflow(ctx, record, workflowpb.WorkflowState_COMPENSATED, now)
		return nil, err
	}
	value, err := s.registry.Submit(ctx, target, func(ctx context.Context, actor *farmactor.FarmActor) (any, error) {
		if err := s.verifyFriendPermit(permit, req.Trusted.Session.PlayerId, target, cmd.OperationId, friendpb.FriendAction_THEFT); err != nil {
			return nil, err
		}
		return actor.PrepareTheft(ctx, cmd.OperationId, payloadHash, req.Trusted.Session.PlayerId, permit.RelationEpoch, request, 0, 0, typed.GetCropCycleId(), now)
	})
	if err != nil {
		var domain *farmactor.Error
		if errors.As(err, &domain) {
			s.finishWorkflow(ctx, record, workflowpb.WorkflowState_COMPENSATED, now)
		}
		return nil, err
	}
	action := value.(*farmactor.ActionResult)
	if action.Status == commonpb.CommandStatus_SUCCEEDED {
		_ = s.finishWorkflow(ctx, record, workflowpb.WorkflowState_COMPLETED, now)
		return action, nil
	}
	if err := s.markWorkflow(ctx, record, workflowpb.WorkflowState_FARM_COMMITTED, now); err != nil {
		return action, nil
	}
	completed, err := s.settleAndComplete(ctx, target, cmd.OperationId, payloadHash, req.Trusted.Session.PlayerId, action, now)
	if err == nil && completed.Status == commonpb.CommandStatus_SUCCEEDED {
		_ = s.finishWorkflow(ctx, record, workflowpb.WorkflowState_COMPLETED, now)
	}
	return completed, err
}

func (s *Service) beginWorkflow(ctx context.Context, operationID string, kind workflowpb.WorkflowKind, farmID, initiator uint64, payloadHash []byte, now time.Time) (*workflowpb.WorkflowRecord, error) {
	workflowID, err := workflow.StableID(initiator, kind, operationID)
	if err != nil {
		return nil, err
	}
	record := workflow.NewRecord(workflowID, operationID, kind, farmID, initiator, payloadHash, now)
	if s.workflows == nil {
		return record, nil
	}
	stored, _, err := s.workflows.CreateOrLoad(ctx, record)
	return stored, err
}

func (s *Service) markWorkflow(ctx context.Context, record *workflowpb.WorkflowRecord, state workflowpb.WorkflowState, now time.Time) error {
	if s.workflows == nil || record == nil {
		return nil
	}
	expectedRevision := record.WorkflowRevision
	record.State = state
	record.UpdatedAtMs = now.UnixMilli()
	if workflow.Terminal(state) {
		record.NextRetryAtMs = 0
	} else if state == workflowpb.WorkflowState_FARM_COMMITTED {
		// Give the request path a short chance to finish its idempotent
		// participant call before a worker takes over the same operation.
		record.NextRetryAtMs = now.Add(workflow.InitialRecoveryDelay).UnixMilli()
	} else {
		record.NextRetryAtMs = now.UnixMilli()
	}
	updated, err := s.workflows.Update(ctx, record, workflow.UpdateOptions{
		ExpectedRevision: expectedRevision,
		Now:              now,
	})
	if err != nil {
		return err
	}
	proto.Reset(record)
	proto.Merge(record, updated)
	return nil
}

func (s *Service) finishWorkflow(ctx context.Context, record *workflowpb.WorkflowRecord, state workflowpb.WorkflowState, now time.Time) error {
	if record == nil {
		return nil
	}
	record.LastError = ""
	record.NextRetryAtMs = 0
	return s.markWorkflow(ctx, record, state, now)
}

func (s *Service) settleAndComplete(ctx context.Context, farmID uint64, operationID string, payloadHash []byte, initiator uint64, action *farmactor.ActionResult, now time.Time) (*farmactor.ActionResult, error) {
	if s.player == nil || action.Settlement == nil {
		return action, nil
	}
	participants := action.Settlement.Participants
	if len(participants) == 0 && action.Settlement.BeneficiaryPlayerId != 0 {
		participants = []*farmpb.FarmSettlementParticipant{{
			PlayerId: action.Settlement.BeneficiaryPlayerId,
			ItemId:   action.Settlement.ItemId, ItemCount: action.Settlement.ItemCount,
			ExpDelta: action.Settlement.ItemCount,
		}}
	}
	var privateBytes []byte
	for _, participant := range participants {
		if participant == nil {
			continue
		}
		rewardHash, err := workflow.RewardPayloadHash(payloadHash, participant)
		if err != nil {
			return action, err
		}
		applied, err := s.player.ApplyReward(ctx, &logicrpcpb.ApplyRewardRequest{
			PlayerId: participant.PlayerId, OperationId: operationID,
			ItemId: participant.ItemId, ItemCount: participant.ItemCount,
			CoinDelta: participant.CoinDelta, ExpDelta: participant.ExpDelta,
			CodexCropDefinitionId: participant.CodexCropDefinitionId,
			PayloadHash:           rewardHash,
		})
		if err != nil || applied.Status == commonpb.CommandStatus_REJECTED {
			return action, nil
		}
		if participant.PlayerId == initiator {
			privateBytes, _ = proto.MarshalOptions{Deterministic: true}.Marshal(applied.PrivateState)
		}
	}
	if len(privateBytes) > 0 {
		action.Result.PrivateState = privateBytes
	}
	return s.complete(ctx, farmID, operationID, action, now)
}

func (s *Service) complete(ctx context.Context, farmID uint64, operationID string, action *farmactor.ActionResult, now time.Time) (*farmactor.ActionResult, error) {
	value, err := s.registry.Submit(ctx, farmID, func(ctx context.Context, actor *farmactor.FarmActor) (any, error) {
		return actor.CompleteOperation(ctx, operationID, action.Result, now)
	})
	if err != nil {
		return action, nil
	}
	completed := value.(*farmactor.ActionResult)
	completed.Change = action.Change
	completed.Recipients = action.Recipients
	completed.Resyncs = action.Resyncs
	return completed, nil
}

// care is the production CARE ingress. Authority registries enter the frozen
// FarmActor Incident path directly; the old Workflow/FarmSettlement route is
// retained only for explicit legacy Registry compatibility.
func (s *Service) care(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest, verified *trusted.VerifiedCommand, now time.Time) (*farmactor.ActionResult, error) {
	if s == nil || s.registry == nil {
		return nil, farmactor.ErrCareOrderAuthorityUnavailable
	}
	if s.registry.AuthorityEnabled() {
		return s.careAuthority(ctx, req, verified, now)
	}
	if !s.legacyCareEnabled {
		return nil, farmactor.ErrCareOrderAuthorityUnavailable
	}
	return s.careLegacy(ctx, req, verified, now)
}

func (s *Service) careAuthority(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest, verified *trusted.VerifiedCommand, now time.Time) (*farmactor.ActionResult, error) {
	cmd := req.Command
	typed, ok := verified.Decoded.(*publicpb.CareRequest)
	if !ok || typed == nil {
		return nil, errors.New("CARE payload is not admitted")
	}
	target := verified.RouteKey
	careType, careName, err := authorityCareType(typed.GetCareType())
	if err != nil {
		return nil, err
	}
	cycleID := typed.GetCropCycleId()
	incidentID := typed.GetIncidentId()
	if incidentID == "" {
		incidentID, err = identity.CareIncidentID(cycleID, careName, 1)
		if err != nil {
			return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_DECODE, Message: "CARE incident_id is required"}
		}
	}
	contextValue := contractspb.CareContext_CARE_CONTEXT_OWNER
	resolver := req.Trusted.Session.PlayerId
	relationEpoch := uint64(0)
	var admittedPermit *frienddomain.ActionPermitSnapshot
	if target != resolver {
		contextValue = contractspb.CareContext_CARE_CONTEXT_FRIEND
		admittedPermit, err = s.commitFriendCareAdmission(ctx, resolver, target, cmd.OperationId, verified.PayloadHash, typed.GetPlotId()+1, cycleID, incidentID, careType, now.UnixMilli())
		if err != nil {
			return nil, err
		}
		relationEpoch = admittedPermit.RelationEpoch
	}
	value, err := s.registry.Submit(ctx, target, func(ctx context.Context, actor *farmactor.FarmActor) (any, error) {
		return actor.ResolveCareIncident(ctx, farmactor.CareResolutionRequest{
			OperationID: cmd.OperationId, PayloadHash: verified.PayloadHash,
			ResolverPlayerID: resolver, PlotID: typed.GetPlotId() + 1,
			CycleID: cycleID, IncidentID: incidentID, CareType: careType,
			Context: contextValue, RelationEpoch: relationEpoch,
			AdmittedPermit: admittedPermit, TrustedNowMs: now.UnixMilli(),
			WantSourceCommitEvidence: s.sourceCommitEvidence,
		})
	})
	if err != nil {
		return nil, err
	}
	action := value.(*farmactor.ActionResult)
	if !action.Replayed && action.Status == commonpb.CommandStatus_SUCCEEDED && action.CareResult != nil &&
		action.CareResult.RewardObligationCreated && action.CommittedSourceOrder != nil && s.careOrderEnqueuer != nil {
		// Enqueue is only a latency optimization. A failure cannot undo the
		// already committed SourceOrder; the bounded Worker will discover it
		// from DueIndex.
		_ = s.careOrderEnqueuer(ctx, target, *action.CommittedSourceOrder, action.SourceCommitEvidence)
	}
	return action, nil
}

func authorityCareType(value publicpb.CareType) (domain.CareType, string, error) {
	switch value {
	case publicpb.CareType_CARE_TYPE_WATER:
		return domain.CareTypeWater, "WATER", nil
	case publicpb.CareType_CARE_TYPE_PEST:
		// CARE_TYPE_PEST is the frozen public compatibility spelling for the
		// contracts enum CARE_TYPE_REMOVE_PEST. No public Proto is changed.
		return domain.CareTypeRemovePest, "REMOVE_PEST", nil
	default:
		return domain.CareTypeUnspecified, "", &farmactor.Error{Code: commonpb.ErrorCode_ERR_COMMAND_UNIMPLEMENTED, Message: "CARE type is not wired"}
	}
}

// careLegacy is the minimum compatibility adapter for non-Authority tests or
// explicitly selected rollback registries. It is never called by production
// AuthorityRegistry and is the only remaining caller of FarmSettlement CARE.
func (s *Service) careLegacy(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest, verified *trusted.VerifiedCommand, now time.Time) (*farmactor.ActionResult, error) {
	cmd := req.Command
	typed := verified.Decoded.(*publicpb.CareRequest)
	target := verified.RouteKey
	request := &farmpb.WaterCropRequest{PlotIndex: typed.GetPlotId()}
	payloadHash := verified.PayloadHash
	if target == req.Trusted.Session.PlayerId {
		value, err := s.registry.Submit(ctx, target, func(ctx context.Context, actor *farmactor.FarmActor) (any, error) {
			return actor.Water(ctx, cmd.OperationId, payloadHash, request, 0, 0, typed.GetCropCycleId(), now)
		})
		if err != nil {
			return nil, err
		}
		return value.(*farmactor.ActionResult), nil
	}
	record, err := s.beginWorkflow(ctx, cmd.OperationId, workflowpb.WorkflowKind_CARE, target, req.Trusted.Session.PlayerId, payloadHash, now)
	if err != nil {
		return nil, err
	}
	permit, err := s.issueFriendPermit(ctx, req.Trusted.Session.PlayerId, target, cmd.OperationId, friendpb.FriendAction_CARE)
	if err != nil {
		_ = s.finishWorkflow(ctx, record, workflowpb.WorkflowState_COMPENSATED, now)
		return nil, err
	}
	value, err := s.registry.Submit(ctx, target, func(ctx context.Context, actor *farmactor.FarmActor) (any, error) {
		if err := s.verifyFriendPermit(permit, req.Trusted.Session.PlayerId, target, cmd.OperationId, friendpb.FriendAction_CARE); err != nil {
			return nil, err
		}
		return actor.PrepareCare(ctx, cmd.OperationId, payloadHash, req.Trusted.Session.PlayerId, permit.RelationEpoch, request, 0, 0, typed.GetCropCycleId(), now)
	})
	if err != nil {
		var domain *farmactor.Error
		if errors.As(err, &domain) {
			_ = s.finishWorkflow(ctx, record, workflowpb.WorkflowState_COMPENSATED, now)
		}
		return nil, err
	}
	action := value.(*farmactor.ActionResult)
	if action.Status == commonpb.CommandStatus_NO_CHANGE || action.Status == commonpb.CommandStatus_SUCCEEDED {
		_ = s.finishWorkflow(ctx, record, workflowpb.WorkflowState_COMPLETED, now)
		return action, nil
	}
	if err := s.markWorkflow(ctx, record, workflowpb.WorkflowState_FARM_COMMITTED, now); err != nil {
		return action, nil
	}
	completed, err := s.settleAndComplete(ctx, target, cmd.OperationId, payloadHash, req.Trusted.Session.PlayerId, action, now)
	if err == nil && completed.Status == commonpb.CommandStatus_SUCCEEDED {
		_ = s.finishWorkflow(ctx, record, workflowpb.WorkflowState_COMPLETED, now)
	}
	return completed, err
}

// commitFriendCareAdmission is the friend CARE Execution-Accept: Pair Authority
// atomically freezes ActionPermitSnapshot + DispatchOrder + DueIndex. A lost
// Commit response is recovered by GetFriendActionAdmission with the same
// identity and payload hash. FarmActor only consumes the frozen permit.
func (s *Service) commitFriendCareAdmission(ctx context.Context, visitor, owner uint64, operationID string, payloadHash []byte, plotID uint32, cycleID, incidentID string, careType domain.CareType, nowMs int64) (*frienddomain.ActionPermitSnapshot, error) {
	if s == nil || s.friendActionAdmission == nil {
		return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, Message: "好友护理准入权威不可用"}
	}
	if len(payloadHash) == 0 || operationID == "" || owner == 0 || visitor == 0 {
		return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, Message: "好友护理准入证明不完整"}
	}
	cmd := frienddomain.ActionAdmissionCommand{
		Identity: frienddomain.CommitIdentity{
			SubjectPlayerID: visitor, CommandType: "CARE", OperationID: operationID,
		},
		PayloadHash:       append([]byte(nil), payloadHash...),
		VisitorPlayerID:   visitor,
		OwnerPlayerID:     owner,
		TargetFarmID:      owner,
		Scope:             frienddomain.ActionScopeCare,
		TargetBindingHash: farmactor.CareTargetBindingHash(plotID, cycleID, incidentID, careType),
		NowMs:             nowMs,
		PermitTTLMs:       frienddomain.DefaultActionPermitTTL.Milliseconds(),
	}
	var admitted frienddomain.ActionAdmissionOutcome
	var err error
	// Concurrent friend CARE (same pair, different operation_id) races Pair
	// record version. Retry bounded optimistic conflicts; same-identity lost
	// responses still recover via GetFriendActionAdmission.
	for attempt := 0; attempt < 16; attempt++ {
		admitted, err = s.friendActionAdmission.CommitFriendActionAdmission(ctx, cmd)
		if err == nil {
			break
		}
		if errors.Is(err, frienddomain.ErrFriendRecordVersionConflict) {
			continue
		}
		if recovered, recoverErr := s.friendActionAdmission.GetFriendActionAdmission(ctx, cmd); recoverErr == nil {
			admitted = recovered
			err = nil
			break
		} else {
			if errors.Is(err, frienddomain.ErrIdempotentConflict) || errors.Is(recoverErr, frienddomain.ErrIdempotentConflict) {
				return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT, Message: "好友护理准入幂等冲突"}
			}
			return nil, err
		}
	}
	if err != nil {
		if errors.Is(err, frienddomain.ErrFriendRecordVersionConflict) {
			return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_STORE_CAS_CONFLICT, Message: "好友护理准入冲突，请重试"}
		}
		return nil, err
	}
	if admitted.Outcome.Status != frienddomain.StatusSucceeded {
		return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, Message: "好友关系无效或护理准入被拒绝"}
	}
	permit := admitted.Permit
	if err := frienddomain.VerifyAdmittedPermit(permit); err != nil {
		return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, Message: "好友护理准入证明无效"}
	}
	return &permit, nil
}

func (s *Service) issueFriendPermit(ctx context.Context, viewer, target uint64, operationID string, action friendpb.FriendAction) (*friendpb.ActionPermit, error) {
	if s.friendPermits == nil {
		return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, Message: "好友操作授权服务不可用"}
	}
	permit, err := s.friendPermits.Issue(ctx, viewer, target, operationID, action)
	if err != nil {
		if errors.Is(err, frienddomain.ErrPermitInvalid) {
			return nil, &farmactor.Error{Code: commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, Message: "好友关系无效或授权已过期"}
		}
		return nil, err
	}
	return permit, nil
}

func (s *Service) verifyFriendPermit(permit *friendpb.ActionPermit, viewer, target uint64, operationID string, action friendpb.FriendAction) error {
	if s.friendPermits == nil || s.friendPermits.VerifySignature(permit, viewer, target, operationID, action) != nil {
		return &farmactor.Error{Code: commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, Message: "好友操作授权无效"}
	}
	return nil
}

func (s *Service) authorizeFriendVisit(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest, target uint64, now time.Time) error {
	if s == nil || s.friendVisitAuthorizer == nil {
		return errors.New("friend visit authority unavailable")
	}
	if req == nil || req.Command == nil || req.Trusted == nil || req.Trusted.Session == nil {
		return &farmactor.Error{Code: commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, Message: "好友访问上下文无效"}
	}
	attempt := req.Trusted.GetJoinAttempt()
	if attempt == nil || strings.TrimSpace(attempt.JoinAttemptId) == "" || attempt.TargetFarmId != target {
		return &farmactor.Error{Code: commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, Message: "好友访问 Join 绑定无效"}
	}
	permit, outcome, err := s.friendVisitAuthorizer.AuthorizeVisit(ctx,
		req.Trusted.Session.PlayerId, target, target, req.Command.OperationId,
		attempt.JoinAttemptId, now.UnixMilli(), frienddomain.DefaultVisitPermitTTLMs)
	if err != nil {
		return fmt.Errorf("friend visit authorization: %w", err)
	}
	if outcome.Status != frienddomain.StatusSucceeded {
		return &farmactor.Error{Code: commonpb.ErrorCode_ERR_NOT_FRIEND, Message: "好友关系无效或已被拉黑"}
	}
	bound, err := s.friendVisitAuthorizer.BindCheck(ctx, permit, now.UnixMilli())
	if err != nil {
		return fmt.Errorf("friend visit permit bind: %w", err)
	}
	if bound.Status != frienddomain.StatusSucceeded || bound.CommittedEpoch != permit.RelationEpoch {
		return &farmactor.Error{Code: commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID, Message: "好友访问授权已过期或关系代次变化"}
	}
	return nil
}

func applyError(outcome *logicrpcpb.BusinessCommandOutcome, err error) {
	outcome.Status = commonpb.CommandStatus_UNAVAILABLE
	code := commonpb.ErrorCode_ERR_INTERNAL
	var domain *farmactor.Error
	if errors.As(err, &domain) {
		// A missing hot receipt/settlement is an unknown historical outcome,
		// not an authoritative "the command was never accepted" answer.
		if domain.Code != commonpb.ErrorCode_ERR_SETTLEMENT_NOT_FOUND {
			outcome.Status = commonpb.CommandStatus_REJECTED
		}
		code = domain.Code
	} else if errors.Is(err, farmactor.ErrJoinStale) {
		// A Join operation is bound to its original subscription/attempt. A
		// retry must establish a Fresh Join instead of replaying the stale
		// operation, and this is not an internal server failure.
		outcome.Status = commonpb.CommandStatus_REJECTED
		outcome.RetryDirective = logicrpcpb.RetryDirective_FRESH_JOIN
		code = commonpb.ErrorCode_ERR_NOT_IN_FARM
	} else if errors.Is(err, workflow.ErrIdempotencyConflict) {
		outcome.Status = commonpb.CommandStatus_REJECTED
		code = commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT
	} else if errors.Is(err, frienddomain.ErrIdempotentConflict) {
		outcome.Status = commonpb.CommandStatus_REJECTED
		code = commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT
	} else if errors.Is(err, workflow.ErrRevisionConflict) {
		code = commonpb.ErrorCode_ERR_STORE_CAS_CONFLICT
	} else if errors.Is(err, workflow.ErrLeaseLost) {
		code = commonpb.ErrorCode_ERR_STORE_UNAVAILABLE
	} else if errors.Is(err, blobstore.ErrCASConflict) {
		code = commonpb.ErrorCode_ERR_STORE_CAS_CONFLICT
	} else if errors.Is(err, blobstore.ErrTimeoutUnknown) {
		outcome.Status = commonpb.CommandStatus_PROCESSING
		code = commonpb.ErrorCode_ERR_TIMEOUT
	} else if errors.Is(err, frienddomain.ErrRedisTimeoutUnknown) {
		outcome.Status = commonpb.CommandStatus_PROCESSING
		code = commonpb.ErrorCode_ERR_TIMEOUT
	} else if errors.Is(err, activation.ErrActorBusy) {
		code = commonpb.ErrorCode_ERR_SERVER_BUSY
	} else if errors.Is(err, blobstore.ErrUnavailable) || errors.Is(err, blobstore.ErrNotFound) {
		code = commonpb.ErrorCode_ERR_STORE_UNAVAILABLE
	} else if errors.Is(err, context.DeadlineExceeded) {
		code = commonpb.ErrorCode_ERR_TIMEOUT
	}
	outcome.Error = &logicrpcpb.ErrorDetail{ErrorCode: code, ErrorMessage: err.Error()}
}
