package ingress

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	farmactor "farmgame/internal/farm/actor"
	"farmgame/internal/farm/subscription"
	frienddomain "farmgame/internal/friend"
	gateprotocol "farmgame/internal/gateway/protocol"

	"google.golang.org/protobuf/proto"
)

const (
	resyncReasonDownlinkQueueFull      = "downlink_queue_full"
	resyncReasonDownlinkDeliveryFailed = "downlink_delivery_failed"
)

// defaultDeliveryContinuationWorkers 是有界 continuation executor 的并发上限。
// 单次失效收尾只做本地 Hub Fence + Control-lane 入队,都是短操作;4 路并发
// 足以吸收同 Gate 连接批量失败,同时不会为过载放大出无界 goroutine。
const defaultDeliveryContinuationWorkers = 4

// boundedContinuation 在 Gateway lane 之外执行投递失败收尾。
// Dispatcher 在 Gateway lane goroutine 内同步调用 DeliverBatch 结果 callback;
// 失效收尾(MarkSubscriptionResyncRequired + Control-lane REJOIN_REQUIRED)必须
// 从 lane 上卸下,否则一次失败会阻塞同 Gate 的所有连接。并发与排队均有界:
// 并发槽耗尽时 fail closed 同步执行,宁可短暂阻塞也绝不丢失失效收尾——
// 丢失会留下"已提交 FarmChange 既未入队也未标记失效"的第三种结果。
type boundedContinuation struct {
	sem chan struct{}
	wg  sync.WaitGroup
}

func newBoundedContinuation(maxConcurrent int) *boundedContinuation {
	if maxConcurrent <= 0 {
		maxConcurrent = 1
	}
	return &boundedContinuation{sem: make(chan struct{}, maxConcurrent)}
}

// submit 非阻塞提交;并发槽已满时同步执行(fail closed)。
func (c *boundedContinuation) submit(task func()) {
	if c == nil || task == nil {
		return
	}
	select {
	case c.sem <- struct{}{}:
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			defer func() { <-c.sem }()
			task()
		}()
	default:
		task()
	}
}

// wait 等待已排队的收尾任务执行完毕(测试确定性用)。
func (c *boundedContinuation) wait() {
	if c != nil {
		c.wg.Wait()
	}
}

// join implements W2A CreateOrReuseJoining. It creates only an unarmed Hub
// Presence and returns Execution-Accept metadata. Snapshot generation starts
// exclusively from the later ArmSnapshot RPC after Gate has queued ACCEPTED.
//
// target 是 FirstHop 复核后的 JoinFarmRequest.target_farm_id(与
// join_attempt.target_farm_id、RouteKey 三者一致,29 §3.4/§5);不再读取旧
// envelope 兼容字段。
func (s *Service) join(ctx context.Context, req *logicrpcpb.ExecuteCommandRequest, target uint64, now time.Time) (*farmactor.JoinResult, error) {
	sess := req.Trusted.Session
	joinAttempt := req.Trusted.GetJoinAttempt()
	if joinAttempt == nil || strings.TrimSpace(joinAttempt.JoinAttemptId) == "" {
		return nil, errors.New("farm ingress: join_attempt_id required")
	}
	if joinAttempt.TargetFarmId != target {
		return nil, errors.New("farm ingress: join target mismatch")
	}
	gatewayID := strings.TrimSpace(sess.GatewayId)
	gatewayBootID := strings.TrimSpace(sess.GatewayBootId)
	sessionEpoch := sess.ActiveSessionEpoch
	routeRevision := sess.SessionRouteRevision
	if gatewayID == "" {
		gatewayID = "default"
	}
	if gatewayBootID == "" {
		gatewayBootID = "legacy"
	}
	if sessionEpoch == 0 {
		sessionEpoch = 1
	}
	if routeRevision == 0 {
		routeRevision = 1
	}
	value, err := s.registry.Submit(ctx, target, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		return actor.CreateOrReuseJoining(farmactor.JoinRequest{
			OperationID: req.Command.OperationId, JoinAttemptID: joinAttempt.JoinAttemptId,
			ForceFresh:     joinAttempt.ForceFresh,
			ViewerPlayerID: sess.PlayerId, SessionEpoch: sessionEpoch,
			GatewayID: gatewayID, GatewayBootID: gatewayBootID,
			ConnectionID: sess.ConnectionId, TransportGeneration: sess.ConnectionGeneration,
			RouteRevision: routeRevision,
		}, now)
	})
	if err != nil {
		return nil, err
	}
	join, _ := value.(*farmactor.JoinResult)
	if join == nil {
		return nil, errors.New("farm ingress: empty joining result")
	}
	return join, nil
}

// enqueueArmedSnapshot publishes the one baseline installed by ArmSnapshot.
// The callback is deliberately asynchronous: the Dispatcher lane must remain
// free to serve other connections while FarmActor advances its Join state.
func (s *Service) enqueueArmedSnapshot(farmID uint64, join *farmactor.JoinResult,
	target *logicrpcpb.DownlinkTargetFence) error {
	if join == nil || join.Snapshot == nil || farmID == 0 || target == nil {
		return errors.New("farm ingress: incomplete armed snapshot")
	}
	enqueuer, ok := s.downlink.(ResultEnqueuer)
	if !ok {
		return errors.New("farm ingress: W2A join requires result-aware downlink")
	}
	subscriptionFence := &logicrpcpb.SubscriptionFence{
		FarmId: farmID, SubscriptionId: join.SubscriptionID, JoinAttemptId: join.JoinAttemptID,
	}
	snapshotVersion := join.Snapshot.FarmVersion
	payload := &farmpb.FarmSnapshotPush{
		JoinAttemptId: join.JoinAttemptID, SnapshotVersion: snapshotVersion, Snapshot: join.Snapshot,
	}
	frame, err := pushFrame(join.ViewerPlayerID, farmID, join.SubscriptionID,
		commonpb.PushType_FARM_SNAPSHOT, payload)
	if err != nil {
		return err
	}
	item := &logicrpcpb.DownlinkItem{
		ItemId:         fmt.Sprintf("%s:snapshot:%d", join.SubscriptionID, snapshotVersion),
		SubscriptionId: join.SubscriptionID, GatewayId: target.GatewayId,
		ConnectionId: target.ConnectionId, TransportGeneration: target.ConnectionGeneration,
		PresenceEpoch: join.PresenceEpoch, Frame: frame,
		JoinFence: cloneSubscriptionFence(subscriptionFence), TargetFence: cloneTargetFence(target),
		SnapshotVersion: snapshotVersion,
	}
	return enqueuer.EnqueueWithResult(item, func(result *logicrpcpb.DeliverBatchItemResult) {
		if result == nil || result.Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
			log.Printf("farm ingress: join failed stage=SNAPSHOT_DELIVERY subscription=%s result=%v",
				subscriptionFence.SubscriptionId, result)
			s.cleanupJoinControlAsync(subscriptionFence, target)
			return
		}
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), joinContinuationTimeout)
			defer cancel()
			response, callbackErr := s.SnapshotQueueAccepted(ctx, &logicrpcpb.SnapshotQueueAcceptedRequest{
				Subscription: cloneSubscriptionFence(subscriptionFence), SnapshotVersion: snapshotVersion,
				Target: cloneTargetFence(target),
			})
			if callbackErr != nil || response == nil ||
				(response.Status != logicrpcpb.JoinCallbackStatus_JOIN_CALLBACK_ADVANCED &&
					response.Status != logicrpcpb.JoinCallbackStatus_JOIN_CALLBACK_ALREADY_APPLIED) {
				log.Printf("farm ingress: join failed stage=SNAPSHOT_CALLBACK subscription=%s err=%v response=%v",
					subscriptionFence.SubscriptionId, callbackErr, response)
				s.cleanupJoinControlAsync(subscriptionFence, target)
			}
		}()
	})
}

// enqueueCatchupBatch publishes the finite V+1..N prefix frozen when the
// Snapshot reached Gate. Changes after N stay in PostReadyBuffer and cannot
// extend this Join indefinitely on a hot farm.
// presenceEpoch 与 SNAPSHOT 条目同源(同一 Lane 校验过的 Presence),保证
// Catch-up 下行与 Join 其他条目携带一致的全量 Fence(26 §3.5)。
func (s *Service) enqueueCatchupBatch(subscriptionFence *logicrpcpb.SubscriptionFence,
	target *logicrpcpb.DownlinkTargetFence, changes []*farmpb.FarmChange, presenceEpoch uint64) error {
	if subscriptionFence == nil || target == nil || len(changes) == 0 {
		return errors.New("farm ingress: incomplete catch-up batch")
	}
	enqueuer, ok := s.downlink.(ResultEnqueuer)
	if !ok {
		return errors.New("farm ingress: W2A join requires result-aware downlink")
	}
	fromVersion := changes[0].GetFarmVersion()
	throughVersion := fromVersion
	for index, change := range changes {
		if change == nil || change.GetFarmVersion() != fromVersion+uint64(index) {
			return errors.New("farm ingress: catch-up batch is not contiguous")
		}
		throughVersion = change.GetFarmVersion()
	}
	payload := &farmpb.FarmDeltaBatchPush{
		JoinAttemptId: subscriptionFence.JoinAttemptId, FromVersion: fromVersion,
		ThroughVersion: throughVersion, Deltas: changes,
	}
	frame, err := pushFrame(target.TargetPlayerId, subscriptionFence.FarmId,
		subscriptionFence.SubscriptionId, commonpb.PushType_FARM_DELTA_BATCH, payload)
	if err != nil {
		return err
	}
	item := &logicrpcpb.DownlinkItem{
		ItemId:         fmt.Sprintf("%s:catchup:%d:%d", subscriptionFence.SubscriptionId, fromVersion, throughVersion),
		SubscriptionId: subscriptionFence.SubscriptionId, GatewayId: target.GatewayId,
		ConnectionId: target.ConnectionId, TransportGeneration: target.ConnectionGeneration,
		PresenceEpoch: presenceEpoch, Frame: frame,
		JoinFence: cloneSubscriptionFence(subscriptionFence), TargetFence: cloneTargetFence(target),
		FromVersion: fromVersion, ThroughVersion: throughVersion,
	}
	return enqueuer.EnqueueWithResult(item, func(result *logicrpcpb.DeliverBatchItemResult) {
		if result == nil || result.Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
			log.Printf("farm ingress: join failed stage=CATCHUP_DELIVERY subscription=%s result=%v",
				subscriptionFence.SubscriptionId, result)
			s.cleanupJoinControlAsync(subscriptionFence, target)
			return
		}
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), joinContinuationTimeout)
			defer cancel()
			response, callbackErr := s.CatchupBatchQueueAccepted(ctx, &logicrpcpb.CatchupBatchQueueAcceptedRequest{
				Subscription: cloneSubscriptionFence(subscriptionFence), FromVersion: fromVersion,
				ThroughVersion: throughVersion, Target: cloneTargetFence(target),
			})
			if callbackErr != nil || response == nil ||
				(response.Status != logicrpcpb.JoinCallbackStatus_JOIN_CALLBACK_ADVANCED &&
					response.Status != logicrpcpb.JoinCallbackStatus_JOIN_CALLBACK_ALREADY_APPLIED) {
				log.Printf("farm ingress: join failed stage=CATCHUP_CALLBACK subscription=%s range=%d..%d err=%v response=%v",
					subscriptionFence.SubscriptionId, fromVersion, throughVersion, callbackErr, response)
				s.cleanupJoinControlAsync(subscriptionFence, target)
			}
		}()
	})
}

// activateJoinAsync asks Gate to queue SUBSCRIPTION_READY(N). Only after Gate
// acknowledges that exact queue operation may Hub become IN_FARM. Any N+1
// changes captured meanwhile are then enqueued behind Ready.
func (s *Service) activateJoinAsync(subscriptionFence *logicrpcpb.SubscriptionFence,
	target *logicrpcpb.DownlinkTargetFence, readyThrough uint64) {
	activator, ok := s.downlink.(SubscriptionActivator)
	if !ok || subscriptionFence == nil || target == nil {
		s.cleanupJoinControlAsync(subscriptionFence, target)
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), joinContinuationTimeout)
		response, err := activator.ActivateSubscription(ctx, &logicrpcpb.ActivateSubscriptionRequest{
			Subscription: cloneSubscriptionFence(subscriptionFence), ReadyThroughVersion: readyThrough,
			Target: cloneTargetFence(target),
		})
		cancel()
		if err != nil || response == nil || response.Status != logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED ||
			response.ReadyThroughVersion != readyThrough {
			log.Printf("farm ingress: join activation failed stage=SUBSCRIPTION_READY subscription=%s err=%v response=%v",
				subscriptionFence.SubscriptionId, err, response)
			s.cleanupJoinControlAsync(subscriptionFence, target)
			return
		}

		ctx, cancel = context.WithTimeout(context.Background(), joinContinuationTimeout)
		value, actorErr := s.registry.Submit(ctx, subscriptionFence.FarmId,
			func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
				changes, epoch, readyErr := actor.SubscriptionReadyQueueAccepted(subscriptionFence.SubscriptionId,
					subscriptionFence.JoinAttemptId, readyThrough,
					leaveFenceFromTarget(subscriptionFence.SubscriptionId, target))
				return readyResult{changes: changes, epoch: epoch}, readyErr
			})
		cancel()
		if errors.Is(actorErr, subscription.ErrJoinCallbackAlreadyApplied) {
			return
		}
		if actorErr != nil {
			log.Printf("farm ingress: join activation failed stage=READY_CALLBACK subscription=%s: %v",
				subscriptionFence.SubscriptionId, actorErr)
			s.cleanupJoinControlAsync(subscriptionFence, target)
			return
		}
		ready, _ := value.(readyResult)
		if s.friendNavigationVisited != nil && target.TargetPlayerId != subscriptionFence.FarmId {
			markCtx, markCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			markErr := s.friendNavigationVisited.MarkVisited(markCtx, frienddomain.VisitFence{
				ViewerPlayerID: target.TargetPlayerId, TargetFarmID: subscriptionFence.FarmId,
				ActiveSessionEpoch: target.SessionEpoch, SubscriptionID: subscriptionFence.SubscriptionId,
			})
			markCancel()
			if markErr != nil {
				log.Printf("farm ingress: friend navigation mark failed subscription=%s: %v", subscriptionFence.SubscriptionId, markErr)
			}
		}
		// Ready 在同一 Lane 内返回的 PresenceEpoch 与 SNAPSHOT/Catch-up 条目同源,
		// post-ready 下行必须携带它才能通过真实 Gate 的 Fence 校验(26 §3.5)。
		for _, change := range ready.changes {
			if change == nil {
				continue
			}
			if err := s.enqueuePostReadyChange(subscriptionFence, target, change, ready.epoch); err != nil {
				// 本地入队失败同样走 level-triggered 失效收尾(failPresenceDelivery
				// 已随 callback 路径覆盖远端拒绝;本地拒绝在此 fail closed),不再
				// 只清理 Presence——否则已提交 change 会落入第三种结果。
				log.Printf("farm ingress: post-ready delivery rejected subscription=%s version=%d: %v",
					subscriptionFence.SubscriptionId, change.GetFarmVersion(), err)
				return
			}
		}
	}()
}

type readyResult struct {
	changes []*farmpb.FarmChange
	epoch   uint64
}

func (s *Service) enqueuePostReadyChange(subscriptionFence *logicrpcpb.SubscriptionFence,
	target *logicrpcpb.DownlinkTargetFence, change *farmpb.FarmChange, presenceEpoch uint64) error {
	if s == nil || s.downlink == nil || subscriptionFence == nil || target == nil || change == nil {
		return errors.New("farm ingress: incomplete post-ready change")
	}
	frame, err := pushFrame(target.TargetPlayerId, subscriptionFence.FarmId,
		subscriptionFence.SubscriptionId, commonpb.PushType_FARM_CHANGE,
		&farmpb.FarmChangePush{Change: change})
	if err != nil {
		return err
	}
	enqueuer, ok := s.downlink.(ResultEnqueuer)
	if !ok {
		return errors.New("farm ingress: delivery guard requires result-aware downlink")
	}
	item := &logicrpcpb.DownlinkItem{
		ItemId:         fmt.Sprintf("%s:post-ready:%d", subscriptionFence.SubscriptionId, change.FarmVersion),
		SubscriptionId: subscriptionFence.SubscriptionId, GatewayId: target.GatewayId,
		ConnectionId: target.ConnectionId, TransportGeneration: target.ConnectionGeneration,
		PresenceEpoch: presenceEpoch, Frame: frame,
		JoinFence: cloneSubscriptionFence(subscriptionFence), TargetFence: cloneTargetFence(target),
		FromVersion: change.FarmVersion, ThroughVersion: change.FarmVersion,
	}
	presence := presenceFromFences(subscriptionFence, target, presenceEpoch)
	enqueueErr := enqueuer.EnqueueWithResult(item, func(result *logicrpcpb.DeliverBatchItemResult) {
		if result == nil || result.Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
			s.runDeliveryFailure(subscriptionFence.FarmId, presence, resyncReasonDownlinkDeliveryFailed,
				change.GetFarmVersion(), 0)
		}
	})
	if enqueueErr != nil {
		// 本地拒绝(lane 满/关闭/Gate 未解析):item 从未进入任何投递队列,
		// 该已提交 change 同样必须落入 level-triggered 失效而不是被静默丢弃。
		s.runDeliveryFailure(subscriptionFence.FarmId, presence, resyncReasonDownlinkDeliveryFailed,
			change.GetFarmVersion(), 0)
	}
	return enqueueErr
}

// runDeliveryFailure 把失效收尾从 Dispatcher 的 Gateway lane 上卸到有界
// continuation executor;executor 过载时 fail closed 同步执行,保证失效不丢失。
func (s *Service) runDeliveryFailure(farmID uint64, presence farmactor.Presence, reason string,
	latest, activationEpoch uint64) {
	if s == nil || farmID == 0 || presence.SubscriptionID == "" {
		return
	}
	task := func() { s.failPresenceDelivery(farmID, presence, reason, latest, activationEpoch) }
	if s.continuations == nil {
		task()
		return
	}
	s.continuations.submit(task)
}

// presenceFromFences 从 Join/Ready 回调的完整 Fence 还原投递失败要失效的
// Presence。携带 PresenceEpoch 后,MarkSubscriptionResyncRequired 的
// matchesLeaveFence 会拒绝迟到/陈旧回调,绝不关闭复用同一连接的新订阅。
func presenceFromFences(subscriptionFence *logicrpcpb.SubscriptionFence,
	target *logicrpcpb.DownlinkTargetFence, presenceEpoch uint64) farmactor.Presence {
	return farmactor.Presence{
		JoinAttemptID:       subscriptionFence.JoinAttemptId,
		SubscriptionID:      subscriptionFence.SubscriptionId,
		ViewerPlayerID:      target.TargetPlayerId,
		SessionEpoch:        target.SessionEpoch,
		GatewayID:           target.GatewayId,
		GatewayBootID:       target.GatewayBootId,
		ConnectionID:        target.ConnectionId,
		TransportGeneration: target.ConnectionGeneration,
		RouteRevision:       target.SessionRouteRevision,
		PresenceEpoch:       presenceEpoch,
	}
}

func (s *Service) cleanupJoinControlAsync(subscriptionFence *logicrpcpb.SubscriptionFence,
	target *logicrpcpb.DownlinkTargetFence) {
	if s == nil || s.registry == nil || subscriptionFence == nil || target == nil {
		return
	}
	go func() {
		s.cleanupJoinControl(subscriptionFence, target)
	}()
}

// cleanupJoinControl removes only the exact joining presence represented by
// the subscription/connection fence. It is synchronous for fail-closed Arm
// admission, while delivery callbacks use the async wrapper above.
func (s *Service) cleanupJoinControl(subscriptionFence *logicrpcpb.SubscriptionFence,
	target *logicrpcpb.DownlinkTargetFence) {
	if s == nil || s.registry == nil || subscriptionFence == nil || target == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), joinContinuationTimeout)
	defer cancel()
	_, _ = s.registry.Submit(ctx, subscriptionFence.FarmId,
		func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
			return actor.LeaveFenced(leaveFenceFromTarget(subscriptionFence.SubscriptionId, target)), nil
		})
}

func cloneSubscriptionFence(value *logicrpcpb.SubscriptionFence) *logicrpcpb.SubscriptionFence {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*logicrpcpb.SubscriptionFence)
}

func cloneTargetFence(value *logicrpcpb.DownlinkTargetFence) *logicrpcpb.DownlinkTargetFence {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*logicrpcpb.DownlinkTargetFence)
}

const joinContinuationTimeout = 5 * time.Second

func (s *Service) emitChange(farmID uint64, action *farmactor.ActionResult) {
	if action == nil {
		return
	}
	if farmID == 0 && action.Change != nil {
		farmID = action.Change.OwnerId
	}
	s.emitResyncs(farmID, action.Resyncs)
	if s.downlink == nil || action.Change == nil {
		return
	}
	latest := action.Change.FarmVersion
	for _, recipient := range action.Recipients {
		if err := s.enqueueChange(recipient, action.Change, action.ActivationEpoch); err != nil {
			s.failPresenceDelivery(farmID, recipient, resyncReasonDownlinkQueueFull, latest, action.ActivationEpoch)
		}
	}
}

func (s *Service) failPresenceDelivery(farmID uint64, presence farmactor.Presence, reason string, latest, activationEpoch uint64) {
	if s == nil || s.registry == nil || farmID == 0 {
		return
	}
	// Stop future changes for the exact failed Presence before emitting the
	// terminal control. A stale fence is ignored so an old failure cannot close
	// a newer subscription that reused the same player or connection.
	ctx, cancel := context.WithTimeout(context.Background(), joinContinuationTimeout)
	notice, matched, markErr := s.registry.MarkSubscriptionResyncRequired(ctx, farmID,
		presenceLeaveRequest(presence), reason, latest, activationEpoch)
	cancel()
	if markErr != nil {
		log.Printf("farm ingress: terminal fence failed farm=%d subscription=%s reason=%s: %v",
			farmID, presence.SubscriptionID, reason, markErr)
		// The business change is already committed. Conservatively try to tell the
		// exact old client to rejoin even when the local Hub continuation failed.
		notice = &farmactor.ResyncNotice{Presence: presence, Reason: reason,
			LatestKnownFarmVersion: latest, ActivationEpoch: activationEpoch}
		matched = true
	}
	if !matched || notice == nil {
		return
	}
	if err := s.emitResyncNoticeForFarm(farmID, *notice); err != nil {
		log.Printf("farm ingress: change resync enqueue failed farm=%d subscription=%s reason=%s: %v",
			farmID, presence.SubscriptionID, reason, err)
	}
}

func (s *Service) notifyResync(farmID uint64, targets []farmactor.Presence, reason string, latest, activationEpoch uint64) {
	notices := make([]farmactor.ResyncNotice, 0, len(targets))
	for _, target := range targets {
		notices = append(notices, farmactor.ResyncNotice{
			Presence: target, Reason: reason, LatestKnownFarmVersion: latest,
			ActivationEpoch: activationEpoch,
		})
	}
	s.emitResyncs(farmID, notices)
}

func (s *Service) emitResyncs(farmID uint64, notices []farmactor.ResyncNotice) {
	for _, notice := range notices {
		if err := s.emitResyncNoticeForFarm(farmID, notice); err != nil {
			log.Printf("farm ingress: resync enqueue failed subscription=%s reason=%s: %v",
				notice.Presence.SubscriptionID, notice.Reason, err)
		}
	}
}

// emitResyncNoticeForFarm 在已知目标农场时携带 farm id(28 §2.4 push context 需要);
// farmID 为 0 时退回 presence 无法定位农场的旧行为(context 不含 farm_id)。
func (s *Service) emitResyncNoticeForFarm(farmID uint64, notice farmactor.ResyncNotice) error {
	if farmID == 0 {
		return errors.New("farm ingress: farm id required for terminal rejoin")
	}
	enqueuer, ok := s.downlink.(ResultEnqueuer)
	if !ok {
		s.cleanupTerminatedSubscriptionAsync(farmID, notice.Presence)
		return errors.New("farm ingress: terminal rejoin requires result-aware downlink")
	}
	item, err := s.rejoinItem(farmID, notice)
	if err != nil {
		return err
	}
	callback := func(result *logicrpcpb.DeliverBatchItemResult) {
		if result == nil {
			return
		}
		switch result.Status {
		case logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
			logicrpcpb.DownlinkItemStatus_CONNECTION_NOT_FOUND,
			logicrpcpb.DownlinkItemStatus_STALE_CONNECTION,
			logicrpcpb.DownlinkItemStatus_STALE_SUBSCRIPTION,
			logicrpcpb.DownlinkItemStatus_QUEUE_FULL:
			s.cleanupTerminatedSubscriptionAsync(farmID, notice.Presence)
		case logicrpcpb.DownlinkItemStatus_UNAVAILABLE:
			log.Printf("farm ingress: terminal rejoin delivery unavailable farm=%d subscription=%s reason=%s",
				farmID, notice.Presence.SubscriptionID, notice.Reason)
			// Delivery could not reach the target Gate. Retire the exact old
			// Presence so a reconnect/Fresh Join is not rejected for the full
			// fallback TTL. The complete Fence prevents touching a newer Join.
			s.cleanupTerminatedSubscriptionAsync(farmID, notice.Presence)
		}
	}
	var enqueueErr error
	if control, ok := s.downlink.(ControlResultEnqueuer); ok {
		enqueueErr = control.EnqueueControlWithResult(item, callback)
	} else {
		enqueueErr = enqueuer.EnqueueWithResult(item, callback)
	}
	if enqueueErr != nil {
		// The terminal item never entered a delivery lane. Fail closed by
		// retiring only this exact Presence; otherwise the next Fresh Join can
		// remain blocked until the long fallback TTL expires.
		s.cleanupTerminatedSubscriptionAsync(farmID, notice.Presence)
	}
	return enqueueErr
}

func (s *Service) rejoinItem(farmID uint64, notice farmactor.ResyncNotice) (*logicrpcpb.DownlinkItem, error) {
	presence := notice.Presence
	if presence.SubscriptionID == "" || presence.JoinAttemptID == "" || presence.PresenceEpoch == 0 ||
		presence.ViewerPlayerID == 0 || presence.GatewayID == "" || presence.GatewayBootID == "" ||
		presence.ConnectionID == "" || presence.TransportGeneration == 0 || presence.SessionEpoch == 0 ||
		presence.RouteRevision == 0 {
		return nil, errors.New("farm ingress: complete subscription and target fence required for terminal rejoin")
	}
	payload := &farmpb.RejoinRequiredPush{Reason: rejoinReason(notice.Reason)}
	if notice.LatestKnownFarmVersion != 0 {
		invalidFrom := notice.LatestKnownFarmVersion
		payload.InvalidFromVersion = &invalidFrom
	}
	frame, err := pushFrame(presence.ViewerPlayerID, farmID, presence.SubscriptionID,
		commonpb.PushType_REJOIN_REQUIRED, payload)
	if err != nil {
		return nil, err
	}
	return &logicrpcpb.DownlinkItem{
		ItemId:         presence.SubscriptionID + ":rejoin:" + notice.Reason,
		SubscriptionId: presence.SubscriptionID,
		GatewayId:      presence.GatewayID, ConnectionId: presence.ConnectionID,
		TransportGeneration: presence.TransportGeneration,
		PresenceEpoch:       presence.PresenceEpoch, Frame: frame,
		JoinFence: &logicrpcpb.SubscriptionFence{
			FarmId: farmID, SubscriptionId: presence.SubscriptionID, JoinAttemptId: presence.JoinAttemptID,
		},
		TargetFence: &logicrpcpb.DownlinkTargetFence{
			TargetPlayerId: presence.ViewerPlayerID, GatewayId: presence.GatewayID,
			GatewayBootId: presence.GatewayBootID, ConnectionId: presence.ConnectionID,
			ConnectionGeneration: presence.TransportGeneration, SessionEpoch: presence.SessionEpoch,
			SessionRouteRevision: presence.RouteRevision,
		},
	}, nil
}

func rejoinReason(reason string) farmpb.RejoinReason {
	switch reason {
	case resyncReasonDownlinkQueueFull, resyncReasonDownlinkDeliveryFailed:
		return farmpb.RejoinReason_REJOIN_REASON_DELIVERY_FAILED
	case "holder_rebuilt", "actor_panic_guard":
		return farmpb.RejoinReason_REJOIN_REASON_HOLDER_REBUILT
	case "subscription_lease_lost", "lease_lost":
		return farmpb.RejoinReason_REJOIN_REASON_SUBSCRIPTION_LEASE_LOST
	default:
		return farmpb.RejoinReason_REJOIN_REASON_STREAM_GAP
	}
}

func (s *Service) cleanupTerminatedSubscriptionAsync(farmID uint64, presence farmactor.Presence) {
	if s == nil || s.registry == nil || farmID == 0 {
		return
	}
	fence := presenceLeaveRequest(presence)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), joinContinuationTimeout)
		defer cancel()
		if _, err := s.registry.CleanupSubscription(ctx, farmID, fence); err != nil {
			log.Printf("farm ingress: exact terminal cleanup failed farm=%d subscription=%s: %v",
				farmID, presence.SubscriptionID, err)
		}
	}()
}

func presenceLeaveRequest(presence farmactor.Presence) farmactor.LeaveRequest {
	return farmactor.LeaveRequest{
		SubscriptionID: presence.SubscriptionID, JoinAttemptID: presence.JoinAttemptID,
		ViewerPlayerID: presence.ViewerPlayerID, SessionEpoch: presence.SessionEpoch,
		GatewayID: presence.GatewayID, GatewayBootID: presence.GatewayBootID,
		ConnectionID: presence.ConnectionID, TransportGeneration: presence.TransportGeneration,
		RouteRevision: presence.RouteRevision, PresenceEpoch: presence.PresenceEpoch,
	}
}

// enqueueChange 是已提交 FarmChange 进入实时流的唯一入口(27 §5.17 唯一
// Writer)。它总是通过 EnqueueWithResult 索取可证明的 QueueAccepted 结果:
// 本地入队成功但远端拒绝(missing/非 QUEUE_ACCEPTED)时,由 callback 异步触发
// 精确 Fence 的失效收尾,消除"本地 Enqueue 成功、远端失败、客户端永久旧状态"
// 的第三种结果。callback 捕获的 presence/epoch 是 enqueue 时刻的快照,失效
// 收尾的 MarkSubscriptionResyncRequired 会按完整 Fence 匹配,迟到回调不会
// 关闭复用同一连接的新订阅。
func (s *Service) enqueueChange(presence farmactor.Presence, change *farmpb.FarmChange, activationEpoch uint64) error {
	item, err := s.changeItem(presence, change)
	if err != nil {
		return err
	}
	enqueuer, ok := s.downlink.(ResultEnqueuer)
	if !ok {
		return errors.New("farm ingress: delivery guard requires result-aware downlink")
	}
	farmID := change.GetOwnerId()
	latest := change.GetFarmVersion()
	return enqueuer.EnqueueWithResult(item, func(result *logicrpcpb.DeliverBatchItemResult) {
		if result == nil || result.Status != logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED {
			s.runDeliveryFailure(farmID, presence, resyncReasonDownlinkDeliveryFailed, latest, activationEpoch)
		}
	})
}

func (s *Service) changeItem(presence farmactor.Presence, change *farmpb.FarmChange) (*logicrpcpb.DownlinkItem, error) {
	frame, err := pushFrame(presence.ViewerPlayerID, change.OwnerId, presence.SubscriptionID,
		commonpb.PushType_FARM_CHANGE, &farmpb.FarmChangePush{Change: change})
	if err != nil {
		return nil, err
	}
	return &logicrpcpb.DownlinkItem{
		ItemId:         fmt.Sprintf("%s:change:%d", presence.SubscriptionID, change.FarmVersion),
		SubscriptionId: presence.SubscriptionID, GatewayId: presence.GatewayID, ConnectionId: presence.ConnectionID,
		TransportGeneration: presence.TransportGeneration, PresenceEpoch: presence.PresenceEpoch, Frame: frame,
		JoinFence: &logicrpcpb.SubscriptionFence{
			FarmId: change.OwnerId, SubscriptionId: presence.SubscriptionID, JoinAttemptId: presence.JoinAttemptID,
		},
		TargetFence: &logicrpcpb.DownlinkTargetFence{
			TargetPlayerId: presence.ViewerPlayerID, GatewayId: presence.GatewayID,
			GatewayBootId: presence.GatewayBootID, ConnectionId: presence.ConnectionID,
			ConnectionGeneration: presence.TransportGeneration, SessionEpoch: presence.SessionEpoch,
			SessionRouteRevision: presence.RouteRevision,
		},
	}, nil
}

// pushFrame 构造 28 §2.4 公开推送:farm_subscription context 只携带"发给谁"与
// Subscription Fence;activation/presence epoch 与 farm_version 属于内部 Fence/版本,
// 不再泄漏进公开信封(内部 DownlinkItem 仍携带 presence_epoch 供 Gate 侧校验)。
func pushFrame(targetPlayerID, targetFarmID uint64, subscriptionID string, pushType commonpb.PushType, payload proto.Message) ([]byte, error) {
	payloadBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		return nil, err
	}
	body := &commonpb.ServerPushEnvelope{
		PushType: pushType,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{
			FarmSubscription: &commonpb.FarmSubscriptionPushContext{
				TargetPlayerId: targetPlayerID, TargetFarmId: targetFarmID, SubscriptionId: subscriptionID,
			},
		},
		Payload: payloadBytes,
	}
	// ConnectionSeq 由 Gate ConnectionWriter 在入队时分配(L2-PROTO-26);
	// 此处仅占位 1,normalizeFrameSequence 会覆写为真实序号。
	return proto.MarshalOptions{Deterministic: true}.Marshal(
		gateprotocol.NewServerPushFrame(1, body))
}
