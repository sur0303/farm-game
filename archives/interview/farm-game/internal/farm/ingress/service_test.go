package ingress

import (
	"context"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	logicrpcpb "farmgame/api/gen/logicrpc"
	playerpb "farmgame/api/gen/player"
	publicpb "farmgame/api/gen/public"
	workflowpb "farmgame/api/gen/workflow"
	farmactor "farmgame/internal/farm/actor"
	"farmgame/internal/friend"
	"farmgame/internal/gateway/commandcatalog"
	playeractor "farmgame/internal/player/actor"
	playeringress "farmgame/internal/player/ingress"
	"farmgame/internal/storage/blobstore"
	"farmgame/internal/workflow"

	"google.golang.org/protobuf/proto"
)

type ingressPairReader struct{ snapshot friend.PairSnapshot }

func activeIngressPairReader(t *testing.T, left, right uint64) *ingressPairReader {
	t.Helper()
	relationID, err := friend.RelationIDFor(left, right)
	if err != nil {
		t.Fatal(err)
	}
	low, high := friend.CanonicalPair(left, right)
	return &ingressPairReader{snapshot: friend.PairSnapshot{LowPlayerID: low, HighPlayerID: high,
		Relation: friend.PairRelation{RelationID: relationID, RelationEpoch: 1, Status: friend.RelationStatusActive}}}
}

func (r *ingressPairReader) GetPair(context.Context, uint64, uint64) (friend.PairSnapshot, error) {
	return r.snapshot, nil
}

type testNavigationVisited struct{ marked chan friend.VisitFence }

func (n *testNavigationVisited) MarkVisited(_ context.Context, fence friend.VisitFence) error {
	n.marked <- fence
	return nil
}
func (n *testNavigationVisited) ListVisited(context.Context, uint64) (map[uint64]struct{}, error) {
	return map[uint64]struct{}{}, nil
}

type testVisitAuthorizer struct {
	authorizeCalls int
	bindCalls      int
	reject         bool
	epoch          uint64
	lastPermit     friend.VisitPermitSnapshot
}

func (a *testVisitAuthorizer) AuthorizeVisit(_ context.Context, viewer, owner, target uint64,
	operationID, attemptID string, nowMs, ttlMs int64) (friend.VisitPermitSnapshot, friend.Outcome, error) {
	a.authorizeCalls++
	if a.reject {
		return friend.VisitPermitSnapshot{}, friend.Outcome{Status: friend.StatusRejected, Reason: friend.ReasonVisitNotPermitted}, nil
	}
	epoch := a.epoch
	if epoch == 0 {
		epoch = 1
	}
	permit := friend.VisitPermitSnapshot{
		PermitID: "visit-test", ViewerPlayerID: viewer, OwnerPlayerID: owner, TargetFarmID: target,
		RelationID: "relation-test", RelationEpoch: epoch, JoinOperationID: operationID,
		JoinAttemptID: attemptID, Scope: friend.ActionScopeVisitFarm,
		IssuedAtAuthorityMs: nowMs, ExpireAtAuthorityMs: nowMs + ttlMs,
	}
	a.lastPermit = permit
	return permit, friend.Outcome{Status: friend.StatusSucceeded, CommittedEpoch: epoch}, nil
}

func (a *testVisitAuthorizer) BindCheck(_ context.Context, permit friend.VisitPermitSnapshot, _ int64) (friend.Outcome, error) {
	a.bindCalls++
	if a.reject || permit.PermitID == "" {
		return friend.Outcome{Status: friend.StatusRejected, Reason: friend.ReasonVisitNotPermitted}, nil
	}
	return friend.Outcome{Status: friend.StatusSucceeded, CommittedEpoch: permit.RelationEpoch}, nil
}

type testStack struct {
	farm       *Service
	player     *playeringress.Service
	visit      *testVisitAuthorizer
	navigation *testNavigationVisited
	workflows  *workflow.MemoryStore
	now        *time.Time
}

type captureDownlink struct {
	items []*logicrpcpb.DownlinkItem
}

func testWorkflowID(subjectPlayerID uint64, kind workflowpb.WorkflowKind, operationID string) string {
	workflowID, err := workflow.StableID(subjectPlayerID, kind, operationID)
	if err != nil {
		panic(err)
	}
	return workflowID
}

func (c *captureDownlink) Enqueue(item *logicrpcpb.DownlinkItem) error {
	c.items = append(c.items, item)
	return nil
}

func (c *captureDownlink) EnqueueWithResult(item *logicrpcpb.DownlinkItem,
	callback func(*logicrpcpb.DeliverBatchItemResult)) error {
	if err := c.Enqueue(item); err != nil {
		return err
	}
	if callback != nil {
		callback(&logicrpcpb.DeliverBatchItemResult{
			ItemId: item.ItemId, Status: logicrpcpb.DownlinkItemStatus_QUEUE_ACCEPTED,
		})
	}
	return nil
}

func (c *captureDownlink) ActivateSubscription(_ context.Context,
	req *logicrpcpb.ActivateSubscriptionRequest) (*logicrpcpb.ActivateSubscriptionResponse, error) {
	return &logicrpcpb.ActivateSubscriptionResponse{
		Status:              logicrpcpb.DeliveryStatus_DELIVERY_QUEUE_ACCEPTED,
		ReadyThroughVersion: req.ReadyThroughVersion,
	}, nil
}

func newTestStack(t *testing.T) testStack {
	t.Helper()
	current := time.UnixMilli(1_800_000_000_000)
	return newSourceOrderTestStack(t, &current, 9, 10)
}

func (s testStack) executeFarm(t *testing.T, playerID uint64, command *commonpb.ClientCommandEnvelope) *logicrpcpb.BusinessCommandOutcome {
	t.Helper()
	trusted, dispatch := gateSideForTest(t, playerID, "c", command)
	response, err := s.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command: command, Trusted: trusted, Dispatch: dispatch,
	})
	if err != nil {
		t.Fatal(err)
	}
	return response.GetBusiness()
}

// typedCommand 构造 28 §3 公网信封:request/operation ID + command_type +
// typed payload bytes;不含任何旧兼容字段。
func typedCommand(t *testing.T, operationID, requestID string, commandType commonpb.CommandType, payload proto.Message) *commonpb.ClientCommandEnvelope {
	t.Helper()
	raw, err := proto.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return &commonpb.ClientCommandEnvelope{
		OperationId: operationID, RequestId: requestID,
		CommandType: commandType, Payload: raw,
	}
}

// gateSideForTest 复刻 Gate 的 Catalog 接线(28 §3.6/29 §4):独立解码 typed
// payload、按 RouteKeyExtractor 提取 RouteKey、生成 DispatchContext 与
// TrustedCommandContext(29 §3.4)。FirstHop 复核必须与之一致才放行。
func gateSideForTest(t *testing.T, playerID uint64, connectionID string, command *commonpb.ClientCommandEnvelope) (*logicrpcpb.TrustedCommandContext, *logicrpcpb.DispatchContext) {
	t.Helper()
	spec, ok := commandcatalog.Lookup(command.CommandType)
	if !ok {
		t.Fatalf("unregistered command %s", command.CommandType)
	}
	decoded, err := spec.Decoder(command.Payload)
	if err != nil {
		t.Fatalf("decode %s: %v", command.CommandType, err)
	}
	trusted := &logicrpcpb.TrustedCommandContext{
		Session: &logicrpcpb.SessionPermitSnapshot{
			PlayerId: playerID, ConnectionId: connectionID, ConnectionGeneration: 1,
			GatewayId: "gate-a", GatewayBootId: "boot-a", ActiveSessionEpoch: 1, SessionRouteRevision: 1,
		},
		RecoveryCompleteAtAdmission: true,
	}
	if command.CommandType == commonpb.CommandType_JOIN_FARM {
		target, _ := commandcatalog.TargetFarmIDFromPayload(decoded)
		trusted.JoinAttempt = &logicrpcpb.JoinAdmissionContext{JoinAttemptId: "attempt-" + connectionID, TargetFarmId: target}
	} else if spec.SessionAdmission.RequiresCurrentSubscription {
		target, _ := commandcatalog.TargetFarmIDFromPayload(decoded)
		trusted.FarmContext = &logicrpcpb.FarmAdmissionContext{AdmittedFarmId: target, AdmittedSubscriptionId: "sub-test"}
	}
	dispatch := &logicrpcpb.DispatchContext{RouteMode: logicrpcpb.RouteMode_ROUTE_MODE_NONE}
	switch spec.RouteMode {
	case commandcatalog.RouteModeActorAffinity:
		routeKey, err := spec.RouteKeyExtractor(decoded, playerID)
		if err != nil {
			t.Fatalf("route key %s: %v", command.CommandType, err)
		}
		domain := logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER
		if spec.RouteDomain == commandcatalog.RouteDomainFarm {
			domain = logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM
		}
		dispatch.RouteMode = logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY
		dispatch.ActorRoute = &logicrpcpb.ActorRouteHeader{RouteDomain: domain, ActorId: routeKey}
	case commandcatalog.RouteModeServiceLB:
		dispatch.RouteMode = logicrpcpb.RouteMode_ROUTE_MODE_SERVICE_LB
	}
	return trusted, dispatch
}

// farmActorDispatch 为手写 TrustedContext 的 Join 测试提供匹配的
// DispatchContext(Farm Actor Affinity,actor_id=target_farm_id)。
func farmActorDispatch(farmID uint64) *logicrpcpb.DispatchContext {
	return &logicrpcpb.DispatchContext{
		RouteMode:  logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY,
		ActorRoute: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, ActorId: farmID},
	}
}

// playerActorDispatch 为直连 player ingress 的测试提供匹配的
// DispatchContext(Player Actor Affinity,actor_id=session.player_id)。
func playerActorDispatch(playerID uint64) *logicrpcpb.DispatchContext {
	return &logicrpcpb.DispatchContext{
		RouteMode:  logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY,
		ActorRoute: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: playerID},
	}
}

func TestJoinRetryWithDifferentRequestIDReusesSubscription(t *testing.T) {
	stack := newTestStack(t)
	first := stack.executeFarm(t, 9, typedCommand(t, "join-intent", "trace-1",
		commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}))
	second := stack.executeFarm(t, 9, typedCommand(t, "join-intent", "trace-2",
		commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}))
	firstAccepted := first.GetJoinAccepted()
	secondAccepted := second.GetJoinAccepted()
	if first.Status != commonpb.CommandStatus_ACCEPTED || second.Status != commonpb.CommandStatus_ACCEPTED ||
		firstAccepted == nil || secondAccepted == nil || firstAccepted.SubscriptionId == "" ||
		firstAccepted.SubscriptionId != secondAccepted.SubscriptionId ||
		firstAccepted.JoinAttemptId != secondAccepted.JoinAttemptId {
		t.Fatalf("join results first=%v second=%v", first, second)
	}
	if len(first.ResultPayload) != 0 || len(second.ResultPayload) != 0 {
		t.Fatalf("join ACCEPTED must carry metadata, not a result payload: first=%d second=%d",
			len(first.ResultPayload), len(second.ResultPayload))
	}
	value, err := stack.farm.registry.Submit(context.Background(), 9, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		return actor.PresenceCount(), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count := value.(int); count != 1 {
		t.Fatalf("presence count=%d, want 1", count)
	}
}

func TestJoinRequiresOperationID(t *testing.T) {
	stack := newTestStack(t)
	result := stack.executeFarm(t, 9, typedCommand(t, "", "trace-no-operation-id",
		commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}))
	if result.Status != commonpb.CommandStatus_REJECTED || result.Error.ErrorCode != commonpb.ErrorCode_ERR_DECODE {
		t.Fatalf("join without operation_id=%v", result)
	}
}

func TestCleanupSubscriptionRPCRequiresAndAppliesFullFence(t *testing.T) {
	stack := newTestStack(t)
	value, err := stack.farm.registry.Submit(context.Background(), 9, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		return actor.Join(farmactor.JoinRequest{
			OperationID: "cleanup-rpc-join", JoinAttemptID: "cleanup-attempt", ViewerPlayerID: 9, SessionEpoch: 7,
			GatewayID: "gate-a", GatewayBootID: "boot-a", ConnectionID: "conn-a",
			TransportGeneration: 3, RouteRevision: 11,
		}, *stack.now)
	})
	if err != nil {
		t.Fatal(err)
	}
	join := value.(*farmactor.JoinResult)
	if _, err := stack.farm.CleanupSubscription(context.Background(), &logicrpcpb.CleanupSubscriptionRequest{
		Subscription: &logicrpcpb.SubscriptionFence{FarmId: 9, SubscriptionId: join.SubscriptionID, JoinAttemptId: "cleanup-attempt"},
		Target:       &logicrpcpb.DownlinkTargetFence{TargetPlayerId: 9},
	}); err == nil {
		t.Fatal("incomplete connection fence was accepted")
	}
	request := &logicrpcpb.CleanupSubscriptionRequest{
		Subscription: &logicrpcpb.SubscriptionFence{
			FarmId: 9, SubscriptionId: join.SubscriptionID, JoinAttemptId: "cleanup-attempt",
		},
		Target: &logicrpcpb.DownlinkTargetFence{
			TargetPlayerId: 9, GatewayId: "gate-a", GatewayBootId: "boot-a",
			ConnectionId: "conn-a", ConnectionGeneration: 3,
			SessionEpoch: 7, SessionRouteRevision: 11,
		},
	}
	response, err := stack.farm.CleanupSubscription(context.Background(), request)
	if err != nil || response == nil || !response.Removed {
		t.Fatalf("cleanup response=%v err=%v", response, err)
	}
	response, err = stack.farm.CleanupSubscription(context.Background(), request)
	if err != nil || response == nil || response.Removed {
		t.Fatalf("idempotent replay response=%v err=%v", response, err)
	}
	if scans := stack.farm.registry.CleanupActorScanTotal(); scans != 0 {
		t.Fatalf("RPC cleanup used legacy actor scan: %d", scans)
	}
}

func (s testStack) buySeed(t *testing.T, playerID uint64, operationID string) *playerpb.PrivateState {
	t.Helper()
	response, err := s.player.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command:  typedCommand(t, operationID, "r-"+operationID, commonpb.CommandType_BUY, &publicpb.BuyRequest{OfferId: playeractor.OfferCarrotSeed, Count: 1, ExpectedOfferVersion: 1}),
		Trusted:  &logicrpcpb.TrustedCommandContext{Session: &logicrpcpb.SessionPermitSnapshot{PlayerId: playerID, ConnectionId: "c"}, RecoveryCompleteAtAdmission: true},
		Dispatch: playerActorDispatch(playerID),
	})
	if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("buy response=%v err=%v", response, err)
	}
	var result playerpb.EconomyCommandResult
	if err := proto.Unmarshal(response.GetBusiness().ResultPayload, &result); err != nil {
		t.Fatal(err)
	}
	return result.PrivateState
}

func TestPersonalAndEconomyLoop(t *testing.T) {
	stack := newTestStack(t)
	stack.buySeed(t, 9, "buy-1")
	planted := stack.executeFarm(t, 9, typedCommand(t, "plant-1", "r-plant", commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: 0, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1}))
	if planted.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("plant=%v", planted)
	}
	var plantResult farmpb.FarmCommandResult
	if err := proto.Unmarshal(planted.ResultPayload, &plantResult); err != nil {
		t.Fatal(err)
	}
	if len(plantResult.Snapshot.Plots) != farmactor.DefaultPlotCount || plantResult.Snapshot.Plots[0].CropPhase != farmpb.CropPhase_GROWING {
		t.Fatalf("plant snapshot=%v", plantResult.Snapshot)
	}
	if plantResult.Snapshot.Plots[0].CropCycleId == "" {
		t.Fatal("plant did not assign a crop cycle")
	}
	var afterPlant playerpb.PrivateState
	if err := proto.Unmarshal(plantResult.PrivateState, &afterPlant); err != nil {
		t.Fatal(err)
	}
	if itemCount(&afterPlant, playeractor.ItemCarrotSeed) != 0 {
		t.Fatalf("seed was not committed: %+v", &afterPlant)
	}
	if legacyID := testWorkflowID(9, workflowpb.WorkflowKind_PLANT, "plant-1"); func() bool {
		_, legacyErr := stack.workflows.Get(context.Background(), legacyID)
		return legacyErr == nil
	}() {
		t.Fatalf("online Plant created legacy workflow %q", legacyID)
	}

	*stack.now = stack.now.Add(11 * time.Second)
	plot := plantResult.Snapshot.Plots[0]
	harvested := stack.executeFarm(t, 9, typedCommand(t, "harvest-1", "r-harvest", commonpb.CommandType_HARVEST,
		&publicpb.HarvestRequest{PlotId: 0, TargetFarmId: 9, CropCycleId: plot.CropCycleId, ExpectedPlotVersion: plot.PlotVersion}))
	if harvested.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("harvest=%v", harvested)
	}
	var harvestResult publicpb.HarvestResult
	if err := proto.Unmarshal(harvested.ResultPayload, &harvestResult); err != nil {
		t.Fatal(err)
	}
	afterHarvestResponse, err := stack.player.GetPrivateState(context.Background(), &logicrpcpb.GetPrivateStateRequest{PlayerId: 9})
	if err != nil {
		t.Fatal(err)
	}
	if itemCount(afterHarvestResponse.PrivateState, playeractor.ItemCarrot) != 500 {
		t.Fatalf("harvest inventory=%+v", afterHarvestResponse.PrivateState)
	}
	if len(afterHarvestResponse.PrivateState.CodexEntries) != 1 ||
		afterHarvestResponse.PrivateState.CodexEntries[0].CropDefinitionId != 1 ||
		afterHarvestResponse.PrivateState.CodexEntries[0].SourceOperationId == "" {
		t.Fatalf("harvest did not unlock codex exactly once: %+v", afterHarvestResponse.PrivateState)
	}

	sold, err := stack.player.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command:  typedCommand(t, "sell-1", "r-sell", commonpb.CommandType_SELL, &publicpb.SellRequest{ItemId: playeractor.ItemCarrot, Count: 1}),
		Trusted:  &logicrpcpb.TrustedCommandContext{Session: &logicrpcpb.SessionPermitSnapshot{PlayerId: 9, ConnectionId: "c"}, RecoveryCompleteAtAdmission: true},
		Dispatch: playerActorDispatch(9),
	})
	if err != nil || sold.GetBusiness().Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("sell=%v err=%v", sold, err)
	}
	var sellResult playerpb.EconomyCommandResult
	_ = proto.Unmarshal(sold.GetBusiness().ResultPayload, &sellResult)
	if sellResult.PrivateState.Coin != 105 || itemCount(sellResult.PrivateState, playeractor.ItemCarrot) != 499 {
		t.Fatalf("sell state=%+v", sellResult.PrivateState)
	}
}

func TestPlantWorkflowRejectsOperationIDWithDifferentIntent(t *testing.T) {
	stack := newTestStack(t)
	stack.buySeed(t, 9, "buy-idempotency")
	first := stack.executeFarm(t, 9, typedCommand(t, "plant-idempotency", "plant-first", commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: 0, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1}))
	if first.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("first plant=%v", first)
	}
	conflict := stack.executeFarm(t, 9, typedCommand(t, "plant-idempotency", "plant-conflict", commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: 1, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1}))
	if conflict.Status != commonpb.CommandStatus_REJECTED || conflict.Error.ErrorCode != commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT {
		t.Fatalf("conflicting plant=%v", conflict)
	}
	if legacyID := testWorkflowID(9, workflowpb.WorkflowKind_PLANT, "plant-idempotency"); func() bool {
		_, legacyErr := stack.workflows.Get(context.Background(), legacyID)
		return legacyErr == nil
	}() {
		t.Fatalf("online Plant created legacy workflow %q", legacyID)
	}
}

func TestOnlinePlantReplayDoesNotDuplicateEffects(t *testing.T) {
	stack := newTestStack(t)
	stack.buySeed(t, 9, "buy-online-replay")
	command := typedCommand(t, "plant-online-replay", "plant-online-replay-r1", commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: 0, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1})
	first := stack.executeFarm(t, 9, command)
	if first.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("first Plant=%v", first)
	}
	replay := stack.executeFarm(t, 9, typedCommand(t, "plant-online-replay", "plant-online-replay-r2", commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: 0, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1}))
	if replay.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("same-payload Plant replay=%v", replay)
	}
	private, err := stack.player.GetPrivateState(context.Background(), &logicrpcpb.GetPrivateStateRequest{PlayerId: 9})
	if err != nil {
		t.Fatal(err)
	}
	if itemCount(private.PrivateState, playeractor.ItemCarrotSeed) != 0 {
		t.Fatalf("same-operation replay deducted another seed: %+v", private.PrivateState)
	}
	conflict := stack.executeFarm(t, 9, typedCommand(t, "plant-online-replay", "plant-online-replay-r3", commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: 1, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1}))
	if conflict.Status != commonpb.CommandStatus_REJECTED || conflict.Error.GetErrorCode() != commonpb.ErrorCode_ERR_IDEMPOTENCY_CONFLICT {
		t.Fatalf("different-payload Plant replay=%v", conflict)
	}
	private, err = stack.player.GetPrivateState(context.Background(), &logicrpcpb.GetPrivateStateRequest{PlayerId: 9})
	if err != nil {
		t.Fatal(err)
	}
	if itemCount(private.PrivateState, playeractor.ItemCarrotSeed) != 0 {
		t.Fatalf("idempotency conflict deducted another seed: %+v", private.PrivateState)
	}
}

type failPlayerRewardParticipant struct {
	PlayerParticipant
	failPlayerID uint64
}

func (p *failPlayerRewardParticipant) ApplyReward(ctx context.Context, request *logicrpcpb.ApplyRewardRequest) (*logicrpcpb.ApplyRewardResponse, error) {
	if request.PlayerId == p.failPlayerID {
		return nil, context.DeadlineExceeded
	}
	return p.PlayerParticipant.ApplyReward(ctx, request)
}

func TestTheftWorkflowRecoversBetweenVisitorAndOwnerRewards(t *testing.T) {
	stack := newTestStack(t)
	stack.farm.SetLegacyTheftRollbackEnabled(true)
	stack.buySeed(t, 9, "buy-theft-recovery")
	planted := stack.executeFarm(t, 9, typedCommand(t, "plant-theft-recovery", "plant-theft-recovery-r", commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: 0, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1}))
	if planted.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("plant=%v", planted)
	}
	var plantResult farmpb.FarmCommandResult
	_ = proto.Unmarshal(planted.ResultPayload, &plantResult)
	*stack.now = stack.now.Add(11 * time.Second)
	plot := plantResult.Snapshot.Plots[0]
	stack.farm.player = &failPlayerRewardParticipant{
		PlayerParticipant: stack.player, failPlayerID: 9,
	}
	stolen := stack.executeFarm(t, 10, typedCommand(t, "theft-recovery", "theft-recovery-r", commonpb.CommandType_THEFT,
		&publicpb.TheftRequest{PlotId: 0, TargetFarmId: 9, CropCycleId: plot.CropCycleId}))
	if stolen.Status != commonpb.CommandStatus_PROCESSING {
		t.Fatalf("theft should remain processing: %v", stolen)
	}
	visitorBeforeRecovery, _ := stack.player.GetPrivateState(context.Background(), &logicrpcpb.GetPrivateStateRequest{PlayerId: 10})
	ownerBeforeRecovery, _ := stack.player.GetPrivateState(context.Background(), &logicrpcpb.GetPrivateStateRequest{PlayerId: 9})
	if itemCount(visitorBeforeRecovery.PrivateState, playeractor.ItemCarrot) != 1 ||
		ownerBeforeRecovery.PrivateState.Coin != 90 {
		t.Fatalf("partial settlement visitor=%v owner=%v", visitorBeforeRecovery, ownerBeforeRecovery)
	}
	record, err := stack.workflows.Get(context.Background(), testWorkflowID(10, workflowpb.WorkflowKind_THEFT, "theft-recovery"))
	if err != nil || record.State != workflowpb.WorkflowState_FARM_COMMITTED {
		t.Fatalf("workflow=%v err=%v", record, err)
	}
	claimAt := stack.now.Add(workflow.InitialRecoveryDelay)
	claimed, err := stack.workflows.ClaimDue(context.Background(), "theft-worker", claimAt, 1, 5*time.Second)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	runner, err := workflow.NewRunner(stack.workflows, stack.player, stack.farm)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Process(context.Background(), claimed[0]); err != nil {
		t.Fatal(err)
	}
	visitorAfterRecovery, _ := stack.player.GetPrivateState(context.Background(), &logicrpcpb.GetPrivateStateRequest{PlayerId: 10})
	ownerAfterRecovery, _ := stack.player.GetPrivateState(context.Background(), &logicrpcpb.GetPrivateStateRequest{PlayerId: 9})
	if itemCount(visitorAfterRecovery.PrivateState, playeractor.ItemCarrot) != 1 ||
		ownerAfterRecovery.PrivateState.Coin != 91 {
		t.Fatalf("recovered settlement visitor=%v owner=%v", visitorAfterRecovery, ownerAfterRecovery)
	}
	recovered, _ := stack.workflows.Get(context.Background(), testWorkflowID(10, workflowpb.WorkflowKind_THEFT, "theft-recovery"))
	if recovered.State != workflowpb.WorkflowState_COMPLETED {
		t.Fatalf("recovered workflow=%v", recovered)
	}
}

func TestFriendCareAndTheftLoop(t *testing.T) {
	stack := newTestStack(t)
	stack.farm.SetLegacyTheftRollbackEnabled(true)
	stack.buySeed(t, 9, "buy-social")
	planted := stack.executeFarm(t, 9, typedCommand(t, "plant-social", "r-plant-social", commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: 0, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1}))
	var plantResult farmpb.FarmCommandResult
	_ = proto.Unmarshal(planted.ResultPayload, &plantResult)
	plot := plantResult.Snapshot.Plots[0]
	*stack.now = stack.now.Add(3 * time.Second)
	watered := stack.executeFarm(t, 10, typedCommand(t, "water-social", "r-water", commonpb.CommandType_CARE,
		&publicpb.CareRequest{PlotId: 0, TargetFarmId: 9, CropCycleId: plot.CropCycleId, CareType: publicpb.CareType_CARE_TYPE_WATER}))
	if watered.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("water=%v", watered)
	}
	helperAfterCare, err := stack.player.GetPrivateState(context.Background(), &logicrpcpb.GetPrivateStateRequest{PlayerId: 10})
	if err != nil {
		t.Fatal(err)
	}
	ownerAfterCare, err := stack.player.GetPrivateState(context.Background(), &logicrpcpb.GetPrivateStateRequest{PlayerId: 9})
	// SourceOrder CARE dest only grants the frozen carer RewardExp (1). The
	// owner is not a dest of APPLY_CARE_REWARD.
	if err != nil || helperAfterCare.PrivateState.Exp != 1 || ownerAfterCare.PrivateState.Exp != 0 {
		t.Fatalf("care helper=%v owner=%v err=%v", helperAfterCare, ownerAfterCare, err)
	}
	*stack.now = stack.now.Add(11 * time.Second)
	theftCommand := typedCommand(t, "theft-social", "r-theft", commonpb.CommandType_THEFT,
		&publicpb.TheftRequest{PlotId: 0, TargetFarmId: 9, CropCycleId: plot.CropCycleId})
	stolen := stack.executeFarm(t, 10, theftCommand)
	if stolen.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("theft=%v", stolen)
	}
	var theftResult farmpb.FarmCommandResult
	_ = proto.Unmarshal(stolen.ResultPayload, &theftResult)
	var visitor playerpb.PrivateState
	if err := proto.Unmarshal(theftResult.PrivateState, &visitor); err != nil {
		t.Fatal(err)
	}
	ownerAfterTheft, err := stack.player.GetPrivateState(context.Background(), &logicrpcpb.GetPrivateStateRequest{PlayerId: 9})
	if itemCount(&visitor, playeractor.ItemCarrot) != 1 || visitor.Exp != 2 ||
		theftResult.Snapshot.Plots[0].StolenTotal != 1 || err != nil ||
		ownerAfterTheft.PrivateState.Coin != 91 {
		t.Fatalf("theft result=%+v visitor=%+v", &theftResult, &visitor)
	}
	if record, err := stack.workflows.Get(context.Background(), testWorkflowID(10, workflowpb.WorkflowKind_THEFT, "theft-social")); err != nil ||
		record.Kind != workflowpb.WorkflowKind_THEFT || record.State != workflowpb.WorkflowState_COMPLETED {
		t.Fatalf("theft workflow=%v err=%v", record, err)
	}
	repeated := stack.executeFarm(t, 10, typedCommand(t, "theft-social", "r-theft-repeat", commonpb.CommandType_THEFT,
		&publicpb.TheftRequest{PlotId: 0, TargetFarmId: 9, CropCycleId: plot.CropCycleId}))
	if repeated.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("replayed theft=%v", repeated)
	}
	visitorAfterReplay, _ := stack.player.GetPrivateState(context.Background(), &logicrpcpb.GetPrivateStateRequest{PlayerId: 10})
	ownerAfterReplay, _ := stack.player.GetPrivateState(context.Background(), &logicrpcpb.GetPrivateStateRequest{PlayerId: 9})
	if itemCount(visitorAfterReplay.PrivateState, playeractor.ItemCarrot) != 1 ||
		ownerAfterReplay.PrivateState.Coin != 91 {
		t.Fatalf("replay duplicated settlement visitor=%v owner=%v", visitorAfterReplay, ownerAfterReplay)
	}
}

func TestFriendCareFailsClosedWithoutAdmission(t *testing.T) {
	stack := newTestStack(t)
	stack.farm.SetFriendActionAdmission(nil)
	stack.buySeed(t, 9, "friend-care-closed-seed")
	planted := stack.executeFarm(t, 9, typedCommand(t, "friend-care-closed-plant", "friend-care-closed-plant-r", commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: 0, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1}))
	if planted.Status != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("plant=%v", planted)
	}
	var plantResult farmpb.FarmCommandResult
	if err := proto.Unmarshal(planted.ResultPayload, &plantResult); err != nil {
		t.Fatal(err)
	}
	plot := plantResult.Snapshot.Plots[0]
	blocked := stack.executeFarm(t, 10, typedCommand(t, "friend-care-closed-blocked", "friend-care-closed-blocked-r", commonpb.CommandType_CARE,
		&publicpb.CareRequest{PlotId: 0, TargetFarmId: 9, CropCycleId: plot.CropCycleId, CareType: publicpb.CareType_CARE_TYPE_WATER}))
	if blocked.Status != commonpb.CommandStatus_REJECTED ||
		blocked.Error.GetErrorCode() != commonpb.ErrorCode_ERR_FRIEND_PERMIT_INVALID {
		t.Fatalf("friend CARE without admission should fail closed: %v", blocked)
	}
}

func TestFriendJoinFailsClosedBeforePresenceWhenVisitPermitIsRejected(t *testing.T) {
	stack := newTestStack(t)
	stack.visit.reject = true
	trusted := joinTrusted(10, "rejected-friend-conn")
	trusted.JoinAttempt.TargetFarmId = 9
	response, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command: typedCommand(t, "rejected-friend-join", "rejected-friend-join-r", commonpb.CommandType_JOIN_FARM,
			&publicpb.JoinFarmRequest{TargetFarmId: 9}),
		Trusted: trusted, Dispatch: farmActorDispatch(9),
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome := response.GetBusiness()
	if outcome.GetStatus() != commonpb.CommandStatus_REJECTED || outcome.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_NOT_FRIEND {
		t.Fatalf("rejected VisitPermit join=%v", outcome)
	}
	if stack.visit.authorizeCalls != 1 || stack.visit.bindCalls != 0 {
		t.Fatalf("rejected permit calls authorize=%d bind=%d", stack.visit.authorizeCalls, stack.visit.bindCalls)
	}
	value, err := stack.farm.registry.Submit(context.Background(), 9, func(_ context.Context, actor *farmactor.FarmActor) (any, error) {
		return actor.PresenceCount(), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := value.(int); got != 0 {
		t.Fatalf("rejected VisitPermit created %d presences", got)
	}
}

func TestFriendJoinAndCareBroadcastsVersionedChange(t *testing.T) {
	stack := newTestStack(t)
	stack.buySeed(t, 9, "buy-broadcast")
	planted := stack.executeFarm(t, 9, typedCommand(t, "plant-broadcast", "r-plant-broadcast", commonpb.CommandType_PLANT,
		&publicpb.PlantRequest{PlotId: 0, CropDefinitionId: 1, TargetFarmId: 9, ExpectedPlotVersion: 1}))
	var plantResult farmpb.FarmCommandResult
	_ = proto.Unmarshal(planted.ResultPayload, &plantResult)
	capture := &captureDownlink{}
	stack.farm.SetDownlinkEnqueuer(capture)
	for _, join := range []struct {
		playerID   uint64
		connection string
		requestID  string
	}{
		{9, "owner-conn", "owner-join"},
		{10, "friend-conn", "friend-join"},
	} {
		trusted := joinTrusted(join.playerID, join.connection)
		trusted.JoinAttempt.TargetFarmId = 9
		response, err := stack.farm.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
			Command:  typedCommand(t, "op-"+join.requestID, join.requestID, commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{TargetFarmId: 9}),
			Trusted:  trusted,
			Dispatch: farmActorDispatch(9),
		})
		if err != nil || response.GetBusiness().Status != commonpb.CommandStatus_ACCEPTED {
			t.Fatalf("join=%v err=%v", response, err)
		}
		armJoin(t, stack, trusted, response)
		waitPresencePhase(t, stack, 9, joinSubscription(t, response), farmactor.PresenceInFarm)
	}
	if len(capture.items) != 2 {
		t.Fatalf("snapshot items=%d", len(capture.items))
	}
	if stack.visit.authorizeCalls != 1 || stack.visit.bindCalls != 1 ||
		stack.visit.lastPermit.ViewerPlayerID != 10 || stack.visit.lastPermit.OwnerPlayerID != 9 ||
		stack.visit.lastPermit.JoinOperationID != "op-friend-join" || stack.visit.lastPermit.JoinAttemptID == "" {
		t.Fatalf("friend Join did not bind one canonical VisitPermit: %+v", stack.visit)
	}
	select {
	case fence := <-stack.navigation.marked:
		if fence.ViewerPlayerID != 10 || fence.TargetFarmID != 9 || fence.ActiveSessionEpoch == 0 || fence.SubscriptionID == "" {
			t.Fatalf("visited fence=%+v", fence)
		}
	case <-time.After(time.Second):
		t.Fatal("friend SUBSCRIPTION_READY did not create server visited fence")
	}
	plot := plantResult.Snapshot.Plots[0]
	plantedFarmVersion := plantResult.Snapshot.GetFarmVersion()
	*stack.now = stack.now.Add(3 * time.Second)
	watered := stack.executeFarm(t, 10, typedCommand(t, "water-broadcast", "r-water-broadcast", commonpb.CommandType_CARE,
		&publicpb.CareRequest{PlotId: 0, TargetFarmId: 9, CropCycleId: plot.CropCycleId, CareType: publicpb.CareType_CARE_TYPE_WATER}))
	if watered.Status != commonpb.CommandStatus_SUCCEEDED || len(capture.items) != 4 {
		t.Fatalf("water=%v downlinks=%d", watered, len(capture.items))
	}
	for _, item := range capture.items[2:] {
		var frame commonpb.FarmFrame
		if err := proto.Unmarshal(item.Frame, &frame); err != nil {
			t.Fatal(err)
		}
		push := frame.GetServerPush()
		if push == nil {
			t.Fatal("missing server push")
		}
		var payload farmpb.FarmChangePush
		if err := proto.Unmarshal(push.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if push.PushType != commonpb.PushType_FARM_CHANGE || payload.Change == nil ||
			payload.Change.GetFarmVersion() <= plantedFarmVersion {
			t.Fatalf("push=%v payload=%v planted_farm_version=%d", push, &payload, plantedFarmVersion)
		}
	}
}

func itemCount(state *playerpb.PrivateState, itemID int32) int64 {
	for _, stack := range state.Inventory {
		if stack != nil && stack.ItemId == itemID {
			return stack.Count
		}
	}
	return 0
}

func TestMissingFarmOperationResultRemainsUnknown(t *testing.T) {
	result := &logicrpcpb.BusinessCommandOutcome{}
	applyError(result, &farmactor.Error{
		Code:    commonpb.ErrorCode_ERR_SETTLEMENT_NOT_FOUND,
		Message: "operation not found",
	})
	if result.Status != commonpb.CommandStatus_UNAVAILABLE {
		t.Fatalf("missing historical result status=%v, want UNAVAILABLE", result.Status)
	}
}

func TestFarmTimeoutUnknownReturnsProcessing(t *testing.T) {
	result := &logicrpcpb.BusinessCommandOutcome{}
	applyError(result, blobstore.ErrTimeoutUnknown)
	if result.Status != commonpb.CommandStatus_PROCESSING || result.Error.ErrorCode != commonpb.ErrorCode_ERR_TIMEOUT {
		t.Fatalf("timeout unknown result=%v", result)
	}
}

func TestFarmStaleJoinRequestsFreshJoin(t *testing.T) {
	result := &logicrpcpb.BusinessCommandOutcome{}
	applyError(result, farmactor.ErrJoinStale)
	if result.Status != commonpb.CommandStatus_REJECTED ||
		result.Error.ErrorCode != commonpb.ErrorCode_ERR_NOT_IN_FARM ||
		result.RetryDirective != logicrpcpb.RetryDirective_FRESH_JOIN {
		t.Fatalf("stale Join result=%v, want REJECTED/ERR_NOT_IN_FARM/FRESH_JOIN", result)
	}
}
