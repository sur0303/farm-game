package trusted

import (
	"errors"
	"testing"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/gateway/commandcatalog"

	"google.golang.org/protobuf/proto"
)

func testSession(playerID uint64) *logicrpcpb.SessionPermitSnapshot {
	return &logicrpcpb.SessionPermitSnapshot{
		PlayerId: playerID, ActiveSessionEpoch: 7,
		GatewayId: "gate-a", GatewayBootId: "boot-a", ConnectionId: "connection-a",
		ConnectionGeneration: 3, SessionRouteRevision: 11,
	}
}

func actorDispatch(domain logicrpcpb.RouteDomain, actorID uint64) *logicrpcpb.DispatchContext {
	return &logicrpcpb.DispatchContext{
		RouteMode:  logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY,
		ActorRoute: &logicrpcpb.ActorRouteHeader{RouteDomain: domain, ActorId: actorID},
	}
}

func playerDispatch(playerID uint64) *logicrpcpb.DispatchContext {
	return actorDispatch(logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, playerID)
}

func farmDispatch(farmID uint64) *logicrpcpb.DispatchContext {
	return actorDispatch(logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM, farmID)
}

func mustPayload(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	raw, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func buyCommand(t *testing.T, playerID uint64) *commonpb.ClientCommandEnvelope {
	t.Helper()
	return &commonpb.ClientCommandEnvelope{
		RequestId: "r1", OperationId: "o1", CommandType: commonpb.CommandType_BUY,
		Payload: mustPayload(t, &publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1}),
	}
}

func plantCommand(t *testing.T, farmID uint64) *commonpb.ClientCommandEnvelope {
	t.Helper()
	return &commonpb.ClientCommandEnvelope{
		RequestId: "r1", OperationId: "o1", CommandType: commonpb.CommandType_PLANT,
		Payload: mustPayload(t, &publicpb.PlantRequest{
			PlotId: 3, CropDefinitionId: 1, TargetFarmId: farmID, ExpectedPlotVersion: 9,
		}),
	}
}

func joinCommand(t *testing.T, farmID uint64) *commonpb.ClientCommandEnvelope {
	t.Helper()
	return &commonpb.ClientCommandEnvelope{
		RequestId: "r1", OperationId: "o1", CommandType: commonpb.CommandType_JOIN_FARM,
		Payload: mustPayload(t, &publicpb.JoinFarmRequest{TargetFarmId: farmID}),
	}
}

func TestValidateCommandAdmissionPolicy(t *testing.T) {
	const playerID = uint64(10001)
	tests := []struct {
		name      string
		cmd       *commonpb.ClientCommandEnvelope
		ctx       *logicrpcpb.TrustedCommandContext
		dispatch  *logicrpcpb.DispatchContext
		wantErr   bool
		wantClass error
	}{
		{
			name:     "player command ok",
			cmd:      buyCommand(t, playerID),
			ctx:      &logicrpcpb.TrustedCommandContext{Session: testSession(playerID), RecoveryCompleteAtAdmission: true},
			dispatch: playerDispatch(playerID),
		},
		{
			name:     "player command forged actor route mismatch",
			cmd:      buyCommand(t, playerID),
			ctx:      &logicrpcpb.TrustedCommandContext{Session: testSession(playerID), RecoveryCompleteAtAdmission: true},
			dispatch: playerDispatch(playerID + 1),
			wantErr:  true,
		},
		{
			name:     "player command wrong route domain",
			cmd:      buyCommand(t, playerID),
			ctx:      &logicrpcpb.TrustedCommandContext{Session: testSession(playerID), RecoveryCompleteAtAdmission: true},
			dispatch: farmDispatch(playerID),
			wantErr:  true,
		},
		{
			name:     "player command missing actor route",
			cmd:      buyCommand(t, playerID),
			ctx:      &logicrpcpb.TrustedCommandContext{Session: testSession(playerID), RecoveryCompleteAtAdmission: true},
			dispatch: &logicrpcpb.DispatchContext{RouteMode: logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY},
			wantErr:  true,
		},
		{
			name: "player command must not carry farm context",
			cmd:  buyCommand(t, playerID),
			ctx: &logicrpcpb.TrustedCommandContext{
				Session: testSession(playerID), RecoveryCompleteAtAdmission: true,
				FarmContext: &logicrpcpb.FarmAdmissionContext{AdmittedFarmId: playerID, AdmittedSubscriptionId: "sub-1"},
			},
			dispatch: playerDispatch(playerID),
			wantErr:  true,
		},
		{
			name: "farm command ok",
			cmd:  plantCommand(t, playerID),
			ctx: &logicrpcpb.TrustedCommandContext{
				Session: testSession(playerID), RecoveryCompleteAtAdmission: true,
				FarmContext: &logicrpcpb.FarmAdmissionContext{AdmittedFarmId: playerID, AdmittedSubscriptionId: "sub-1"},
			},
			dispatch: farmDispatch(playerID),
		},
		{
			name:     "farm command missing farm context",
			cmd:      plantCommand(t, playerID),
			ctx:      &logicrpcpb.TrustedCommandContext{Session: testSession(playerID), RecoveryCompleteAtAdmission: true},
			dispatch: farmDispatch(playerID),
			wantErr:  true,
		},
		{
			name: "farm command admitted farm mismatch",
			cmd:  plantCommand(t, playerID),
			ctx: &logicrpcpb.TrustedCommandContext{
				Session: testSession(playerID), RecoveryCompleteAtAdmission: true,
				FarmContext: &logicrpcpb.FarmAdmissionContext{AdmittedFarmId: playerID + 1, AdmittedSubscriptionId: "sub-1"},
			},
			dispatch: farmDispatch(playerID),
			wantErr:  true,
		},
		{
			name: "farm command missing subscription id",
			cmd:  plantCommand(t, playerID),
			ctx: &logicrpcpb.TrustedCommandContext{
				Session: testSession(playerID), RecoveryCompleteAtAdmission: true,
				FarmContext: &logicrpcpb.FarmAdmissionContext{AdmittedFarmId: playerID},
			},
			dispatch: farmDispatch(playerID),
			wantErr:  true,
		},
		{
			name: "farm command incomplete session fence",
			cmd:  plantCommand(t, playerID),
			ctx: &logicrpcpb.TrustedCommandContext{
				Session:                     &logicrpcpb.SessionPermitSnapshot{PlayerId: playerID},
				RecoveryCompleteAtAdmission: true,
				FarmContext:                 &logicrpcpb.FarmAdmissionContext{AdmittedFarmId: playerID, AdmittedSubscriptionId: "sub-1"},
			},
			dispatch: farmDispatch(playerID),
			wantErr:  true,
		},
		{
			name: "farm command route key differs from payload target",
			cmd:  plantCommand(t, playerID),
			ctx: &logicrpcpb.TrustedCommandContext{
				Session: testSession(playerID), RecoveryCompleteAtAdmission: true,
				FarmContext: &logicrpcpb.FarmAdmissionContext{AdmittedFarmId: playerID, AdmittedSubscriptionId: "sub-1"},
			},
			dispatch: farmDispatch(playerID + 1),
			wantErr:  true,
		},
		{
			name: "farm command must not carry join attempt",
			cmd:  plantCommand(t, playerID),
			ctx: &logicrpcpb.TrustedCommandContext{
				Session: testSession(playerID), RecoveryCompleteAtAdmission: true,
				FarmContext: &logicrpcpb.FarmAdmissionContext{AdmittedFarmId: playerID, AdmittedSubscriptionId: "sub-1"},
				JoinAttempt: &logicrpcpb.JoinAdmissionContext{JoinAttemptId: "ja-1", TargetFarmId: playerID},
			},
			dispatch: farmDispatch(playerID),
			wantErr:  true,
		},
		{
			name:     "write command requires recovery complete",
			cmd:      buyCommand(t, playerID),
			ctx:      &logicrpcpb.TrustedCommandContext{Session: testSession(playerID)},
			dispatch: playerDispatch(playerID),
			wantErr:  true,
		},
		{
			name: "join farm ok",
			cmd:  joinCommand(t, 20002),
			ctx: &logicrpcpb.TrustedCommandContext{
				Session:     testSession(playerID),
				JoinAttempt: &logicrpcpb.JoinAdmissionContext{JoinAttemptId: "ja-1", TargetFarmId: 20002},
			},
			dispatch: farmDispatch(20002),
		},
		{
			name:     "join farm missing join attempt",
			cmd:      joinCommand(t, 20002),
			ctx:      &logicrpcpb.TrustedCommandContext{Session: testSession(playerID)},
			dispatch: farmDispatch(20002),
			wantErr:  true,
		},
		{
			name: "join farm must not carry farm context",
			cmd:  joinCommand(t, 20002),
			ctx: &logicrpcpb.TrustedCommandContext{
				Session:     testSession(playerID),
				FarmContext: &logicrpcpb.FarmAdmissionContext{AdmittedFarmId: 20002, AdmittedSubscriptionId: "sub-1"},
				JoinAttempt: &logicrpcpb.JoinAdmissionContext{JoinAttemptId: "ja-1", TargetFarmId: 20002},
			},
			dispatch: farmDispatch(20002),
			wantErr:  true,
		},
		{
			name: "join farm target mismatch",
			cmd:  joinCommand(t, 20002),
			ctx: &logicrpcpb.TrustedCommandContext{
				Session:     testSession(playerID),
				JoinAttempt: &logicrpcpb.JoinAdmissionContext{JoinAttemptId: "ja-1", TargetFarmId: 30003},
			},
			dispatch: farmDispatch(20002),
			wantErr:  true,
		},
		{
			name: "join farm requires attempt id",
			cmd:  joinCommand(t, 20002),
			ctx: &logicrpcpb.TrustedCommandContext{
				Session:     testSession(playerID),
				JoinAttempt: &logicrpcpb.JoinAdmissionContext{TargetFarmId: 20002},
			},
			dispatch: farmDispatch(20002),
			wantErr:  true,
		},
		{
			name: "join farm zero payload target",
			cmd:  joinCommand(t, 0),
			ctx: &logicrpcpb.TrustedCommandContext{
				Session:     testSession(playerID),
				JoinAttempt: &logicrpcpb.JoinAdmissionContext{JoinAttemptId: "ja-1"},
			},
			dispatch:  &logicrpcpb.DispatchContext{RouteMode: logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY, ActorRoute: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM}},
			wantErr:   true,
			wantClass: ErrPayloadInvalid,
		},
		{
			name:     "missing session",
			cmd:      buyCommand(t, playerID),
			ctx:      &logicrpcpb.TrustedCommandContext{},
			dispatch: playerDispatch(playerID),
			wantErr:  true,
		},
		{
			name:     "missing player id",
			cmd:      buyCommand(t, playerID),
			ctx:      &logicrpcpb.TrustedCommandContext{Session: testSession(0)},
			dispatch: playerDispatch(playerID),
			wantErr:  true,
		},
		{
			name: "missing request id",
			cmd: &commonpb.ClientCommandEnvelope{
				OperationId: "o1", CommandType: commonpb.CommandType_BUY,
				Payload: mustPayload(t, &publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1}),
			},
			ctx:      &logicrpcpb.TrustedCommandContext{Session: testSession(playerID)},
			dispatch: playerDispatch(playerID),
			wantErr:  true,
		},
		{
			name: "disabled command rejected before decode",
			cmd: &commonpb.ClientCommandEnvelope{
				RequestId: "r1", OperationId: "o1", CommandType: commonpb.CommandType_REDEEM_SHARE,
				Payload: mustPayload(t, &publicpb.GetMailListRequest{}),
			},
			ctx:       &logicrpcpb.TrustedCommandContext{Session: testSession(playerID)},
			dispatch:  &logicrpcpb.DispatchContext{RouteMode: logicrpcpb.RouteMode_ROUTE_MODE_SERVICE_LB},
			wantErr:   true,
			wantClass: ErrCommandDisabled,
		},
		{
			name: "unregistered command rejected",
			cmd: &commonpb.ClientCommandEnvelope{
				RequestId: "r1", OperationId: "o1", CommandType: commonpb.CommandType(7),
			},
			ctx:      &logicrpcpb.TrustedCommandContext{Session: testSession(playerID)},
			dispatch: &logicrpcpb.DispatchContext{RouteMode: logicrpcpb.RouteMode_ROUTE_MODE_NONE},
			wantErr:  true,
		},
		{
			name: "missing operation id",
			cmd: &commonpb.ClientCommandEnvelope{
				RequestId: "r1", CommandType: commonpb.CommandType_BUY,
				Payload: mustPayload(t, &publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1}),
			},
			ctx:       &logicrpcpb.TrustedCommandContext{Session: testSession(playerID), RecoveryCompleteAtAdmission: true},
			dispatch:  playerDispatch(playerID),
			wantErr:   true,
			wantClass: ErrPayloadInvalid,
		},
		{
			name: "malformed payload",
			cmd: &commonpb.ClientCommandEnvelope{
				RequestId: "r1", OperationId: "o1", CommandType: commonpb.CommandType_BUY,
				Payload: []byte{0xff, 0xff, 0xff},
			},
			ctx:       &logicrpcpb.TrustedCommandContext{Session: testSession(playerID), RecoveryCompleteAtAdmission: true},
			dispatch:  playerDispatch(playerID),
			wantErr:   true,
			wantClass: ErrPayloadInvalid,
		},
		{
			name: "payload with unknown fields fails closed",
			cmd: &commonpb.ClientCommandEnvelope{
				RequestId: "r1", OperationId: "o1", CommandType: commonpb.CommandType_BUY,
				// field 1 varint 1 + unknown field 15 varint 7
				Payload: []byte{0x08, 0x01, 0x78, 0x07},
			},
			ctx:       &logicrpcpb.TrustedCommandContext{Session: testSession(playerID), RecoveryCompleteAtAdmission: true},
			dispatch:  playerDispatch(playerID),
			wantErr:   true,
			wantClass: ErrPayloadInvalid,
		},
		{
			name: "care non-water slice disabled",
			cmd: &commonpb.ClientCommandEnvelope{
				RequestId: "r1", OperationId: "o1", CommandType: commonpb.CommandType_CARE,
				Payload: mustPayload(t, &publicpb.CareRequest{
					PlotId: 3, TargetFarmId: playerID, CareType: publicpb.CareType_CARE_TYPE_UNSPECIFIED,
				}),
			},
			ctx: &logicrpcpb.TrustedCommandContext{
				Session: testSession(playerID), RecoveryCompleteAtAdmission: true,
				FarmContext: &logicrpcpb.FarmAdmissionContext{AdmittedFarmId: playerID, AdmittedSubscriptionId: "sub-1"},
			},
			dispatch:  farmDispatch(playerID),
			wantErr:   true,
			wantClass: ErrCommandDisabled,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			verified, err := ValidateCommand(test.cmd, test.ctx, test.dispatch)
			if test.wantErr && err == nil {
				t.Fatal("expected rejection, got nil")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("expected acceptance, got %v", err)
			}
			if test.wantClass != nil && !errors.Is(err, test.wantClass) {
				t.Fatalf("expected error class %v, got %v", test.wantClass, err)
			}
			if err == nil && verified == nil {
				t.Fatal("accepted command must return verified output")
			}
		})
	}
}

// TestValidateCommandCanonicalHash 钉死 FirstHop 权威 hash 语义:同 operation 同
// 语义 payload 重放稳定;request_id/operation_id/连接/路由噪声不进入 hash;
// 非规范字段顺序得到相同 hash;不同语义 payload 得到不同 hash(28 §3.5)。
func TestValidateCommandCanonicalHash(t *testing.T) {
	const playerID = uint64(10001)
	ctx := &logicrpcpb.TrustedCommandContext{
		Session: testSession(playerID), RecoveryCompleteAtAdmission: true,
		FarmContext: &logicrpcpb.FarmAdmissionContext{AdmittedFarmId: playerID, AdmittedSubscriptionId: "sub-1"},
	}
	dispatch := farmDispatch(playerID)

	base := plantCommand(t, playerID)
	first, err := ValidateCommand(base, ctx, dispatch)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.PayloadHash) != 32 {
		t.Fatalf("payload hash length=%d, want 32", len(first.PayloadHash))
	}

	// 传输噪声(request_id/operation_id 变化)不改变权威 hash。
	noisy := plantCommand(t, playerID)
	noisy.RequestId = "r2-retry"
	noisy.OperationId = "o1-retry-request"
	second, err := ValidateCommand(noisy, ctx, dispatch)
	if err != nil {
		t.Fatal(err)
	}
	if string(first.PayloadHash) != string(second.PayloadHash) {
		t.Fatal("transport noise changed authoritative hash")
	}

	// 不同语义 payload(不同 plot)得到不同 hash → IDEMPOTENCY_CONFLICT 的输入。
	changed, err := ValidateCommand(plantCommand(t, playerID), ctx, dispatch)
	if err != nil {
		t.Fatal(err)
	}
	changedCmd := plantCommand(t, playerID)
	changedCmd.Payload = mustPayload(t, &publicpb.PlantRequest{
		PlotId: 4, CropDefinitionId: 1, TargetFarmId: playerID, ExpectedPlotVersion: 9,
	})
	other, err := ValidateCommand(changedCmd, ctx, dispatch)
	if err != nil {
		t.Fatal(err)
	}
	if string(changed.PayloadHash) == string(other.PayloadHash) {
		t.Fatal("different semantic payload produced identical hash")
	}

	// 非规范字段顺序(先写 field5 再写 field1)解码后语义相同,hash 必须相同。
	ordered := mustPayload(t, &publicpb.PlantRequest{
		PlotId: 3, CropDefinitionId: 1, TargetFarmId: playerID, ExpectedPlotVersion: 9,
	})
	// 直接按非规范 wire 顺序编码:field4(epv=9) 先于 field1(plot=3) 等,解码后等价。
	manual := []byte{
		0x20, 0x09, // field 4 varint 9 (expected_plot_version)
		0x08, 0x03, // field 1 varint 3 (plot_id)
		0x18, 0x91, 0x4e, // field 3 varint 10001 (target_farm_id)
		0x10, 0x01, // field 2 varint 1 (crop_definition_id)
	}
	reordered := plantCommand(t, playerID)
	reordered.Payload = manual
	third, err := ValidateCommand(reordered, ctx, dispatch)
	if err != nil {
		t.Fatal(err)
	}
	canonical := plantCommand(t, playerID)
	canonical.Payload = ordered
	fourth, err := ValidateCommand(canonical, ctx, dispatch)
	if err != nil {
		t.Fatal(err)
	}
	if string(third.PayloadHash) != string(fourth.PayloadHash) {
		t.Fatal("field reorder changed authoritative hash")
	}
}

// TestFirstHopCompletenessAcrossCatalog 是 28 §6.4 的 FirstHop 侧完整性测试:
// 每个 enabled 命令都能由 FirstHop 独立 decode/route/hash;每个 disabled 命令
// 在业务接受前以 ErrCommandDisabled 明确拒绝,不被误启用。
func TestFirstHopCompletenessAcrossCatalog(t *testing.T) {
	const playerID = uint64(10001)
	const farmID = uint64(20002)
	// 每条 enabled 命令的最小合法 typed payload 与对应 Gate 侧上下文。
	enabled := map[commonpb.CommandType]proto.Message{
		commonpb.CommandType_JOIN_FARM:             &publicpb.JoinFarmRequest{TargetFarmId: farmID},
		commonpb.CommandType_GET_PLAYER_BOOTSTRAP:  &publicpb.GetPlayerBootstrapRequest{},
		commonpb.CommandType_GET_TASK_LIST:         &publicpb.GetTaskListRequest{},
		commonpb.CommandType_CLAIM_TASK:            &publicpb.ClaimTaskRequest{TaskInstanceId: "task-1"},
		commonpb.CommandType_CONFIRM_FRIEND_VISIT:  &publicpb.ConfirmFriendVisitRequest{TargetFarmId: farmID},
		commonpb.CommandType_GET_MAIL_LIST:         &publicpb.GetMailListRequest{},
		commonpb.CommandType_GET_MAIL_DETAIL:       &publicpb.GetMailDetailRequest{MailId: "mail-1"},
		commonpb.CommandType_MARK_MAIL_READ:        &publicpb.MarkMailReadRequest{MailId: "mail-1"},
		commonpb.CommandType_DELETE_MAIL:           &publicpb.DeleteMailRequest{MailId: "mail-1"},
		commonpb.CommandType_CLAIM_MAIL_ATTACHMENT: &publicpb.ClaimMailAttachmentRequest{MailId: "mail-1"},
		commonpb.CommandType_GET_PET_STATE:         &publicpb.GetPetStateRequest{},
		commonpb.CommandType_PET_UNLOCK_BY_COIN: &publicpb.PetUnlockByCoinRequest{
			PetId: 1, ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 1,
		},
		commonpb.CommandType_PET_FEED: &publicpb.PetFeedRequest{
			Quantity: 1, ExpectedFeedRuleVersion: 1, ExpectedActivePetConfigVersion: 1,
			ExpectedGuardRuleVersion: 1,
		},
		commonpb.CommandType_PET_SWITCH: &publicpb.PetSwitchRequest{
			TargetPetId: 1, ExpectedPetConfigVersion: 1, ExpectedGuardRuleVersion: 1,
		},
		commonpb.CommandType_GET_COMPENDIUM:      &publicpb.GetCompendiumRequest{},
		commonpb.CommandType_ACK_COMPENDIUM_SEEN: &publicpb.AckCompendiumSeenRequest{ObservedRevision: 1},
		commonpb.CommandType_BUY:                 &publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1},
		commonpb.CommandType_SELL:                &publicpb.SellRequest{ItemId: 1, Count: 1},
		commonpb.CommandType_GET_OPERATION_RESULT: &publicpb.GetOperationResultRequest{
			OriginalCommandType: commonpb.CommandType_PLANT,
			OriginalOperationId: "plant-completeness",
			OriginalTarget: &publicpb.OperationTargetLocator{
				Target: &publicpb.OperationTargetLocator_TargetFarmId{TargetFarmId: farmID},
			},
		},
		// OWN_CURRENT_FARM(28 §6.1):target 必须同时等于 admitted farm 与
		// session.player_id;合法样例只能以 playerID 为目标。
		commonpb.CommandType_UNLOCK_PLOT: &publicpb.UnlockPlotRequest{
			PlotId: 5, TargetFarmId: playerID, ExpectedPlotVersion: 1, ExpectedUnlockRuleVersion: 1,
		},
		commonpb.CommandType_PLANT: &publicpb.PlantRequest{
			PlotId: 3, CropDefinitionId: 1, TargetFarmId: playerID, ExpectedPlotVersion: 9,
		},
		commonpb.CommandType_HARVEST: &publicpb.HarvestRequest{
			PlotId: 3, TargetFarmId: playerID, CropCycleId: "cc-1", ExpectedPlotVersion: 9,
		},
		// CURRENT_FARM:target == admitted farm(可以是他人的农场)。
		commonpb.CommandType_CARE: &publicpb.CareRequest{
			PlotId: 3, TargetFarmId: farmID, CropCycleId: "cc-1", CareType: publicpb.CareType_CARE_TYPE_WATER,
		},
		commonpb.CommandType_THEFT: &publicpb.TheftRequest{PlotId: 3, TargetFarmId: farmID, CropCycleId: "cc-1"},
	}
	specs := commandcatalog.All()
	if len(specs) != 33 {
		t.Fatalf("catalog size=%d, want 33", len(specs))
	}
	enabledSeen := 0
	disabledSeen := 0
	for _, spec := range specs {
		t.Run(spec.CommandType.String(), func(t *testing.T) {
			ctx := &logicrpcpb.TrustedCommandContext{
				Session: testSession(playerID), RecoveryCompleteAtAdmission: true,
			}
			var dispatch *logicrpcpb.DispatchContext
			var payload proto.Message
			if spec.Availability.Enabled {
				payload = enabled[spec.CommandType]
				if payload == nil {
					t.Fatalf("enabled command %s missing minimal payload fixture", spec.CommandType)
				}
				enabledSeen++
			} else {
				disabledSeen++
				// disabled 命令在解码前拒绝;payload 内容不影响结论。
				payload = &publicpb.GetPlayerBootstrapRequest{}
			}
			raw := mustPayload(t, payload)
			// farm_context.admitted_farm_id 与 dispatch route key 跟随该命令的
			// typed target(复刻 Gate 接线):OWN_CURRENT_FARM 样例即 playerID,
			// CURRENT_FARM 样例即当前订阅 farmID。
			typedTarget, _ := commandcatalog.TargetFarmIDFromPayload(payload)
			switch {
			case spec.CommandType == commonpb.CommandType_JOIN_FARM:
				ctx.RecoveryCompleteAtAdmission = false // JOIN 不要求恢复完成
				ctx.JoinAttempt = &logicrpcpb.JoinAdmissionContext{JoinAttemptId: "ja-1", TargetFarmId: farmID}
				dispatch = farmDispatch(farmID)
			case spec.RouteMode == commandcatalog.RouteModeServiceLB:
				dispatch = &logicrpcpb.DispatchContext{RouteMode: logicrpcpb.RouteMode_ROUTE_MODE_SERVICE_LB}
			case spec.SessionAdmission.RequiresCurrentSubscription:
				ctx.FarmContext = &logicrpcpb.FarmAdmissionContext{AdmittedFarmId: typedTarget, AdmittedSubscriptionId: "sub-1"}
				if spec.RouteDomain == commandcatalog.RouteDomainPlayer {
					dispatch = playerDispatch(playerID)
				} else {
					dispatch = farmDispatch(typedTarget)
				}
			default:
				dispatch = playerDispatch(playerID)
			}
			cmd := &commonpb.ClientCommandEnvelope{
				RequestId: "r-completeness", OperationId: "o-completeness",
				CommandType: spec.CommandType, Payload: raw,
			}
			verified, err := ValidateCommand(cmd, ctx, dispatch)
			if !spec.Availability.Enabled {
				if !errors.Is(err, ErrCommandDisabled) {
					t.Fatalf("disabled %s err=%v, want ErrCommandDisabled", spec.CommandType, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("enabled %s rejected: %v", spec.CommandType, err)
			}
			if verified.Decoded == nil {
				t.Fatalf("enabled %s missing decoded payload", spec.CommandType)
			}
			if spec.RouteMode == commandcatalog.RouteModeActorAffinity && verified.RouteKey == 0 {
				t.Fatalf("enabled %s missing route key", spec.CommandType)
			}
			if spec.Idempotency.RequiresCanonicalBuilder() && len(verified.PayloadHash) != 32 {
				t.Fatalf("enabled %s missing authoritative hash", spec.CommandType)
			}
		})
	}
	if enabledSeen != 24 || disabledSeen != 9 {
		t.Fatalf("enabled=%d disabled=%d, want 24/9", enabledSeen, disabledSeen)
	}
}

// TestOversizePayloadFailsClosed 钉死 28 §3.4/§6.4-6:超过命令级上限的 payload
// 在业务接受前 fail closed。
func TestOversizePayloadFailsClosed(t *testing.T) {
	const playerID = uint64(10001)
	oversize := make([]byte, commandcatalog.DefaultMaxPayloadBytes+1)
	cmd := &commonpb.ClientCommandEnvelope{
		RequestId: "r1", OperationId: "o1", CommandType: commonpb.CommandType_BUY,
		Payload: oversize,
	}
	_, err := ValidateCommand(cmd, &logicrpcpb.TrustedCommandContext{
		Session: testSession(playerID), RecoveryCompleteAtAdmission: true,
	}, playerDispatch(playerID))
	if !errors.Is(err, ErrPayloadInvalid) {
		t.Fatalf("oversize err=%v, want ErrPayloadInvalid", err)
	}
}

// TestRetiredEnvelopeFieldsCannotForged 钉死 L2-PROTO-05:旧 target_owner_id/
// expected_* 字段已 reserved 删除,客户端在 wire 上强行携带(成为未知字段)
// 时不能改变目标、路由键或权威 hash。
func TestRetiredEnvelopeFieldsCannotForged(t *testing.T) {
	const playerID = uint64(10001)
	ctx := &logicrpcpb.TrustedCommandContext{
		Session: testSession(playerID), RecoveryCompleteAtAdmission: true,
		FarmContext: &logicrpcpb.FarmAdmissionContext{AdmittedFarmId: playerID, AdmittedSubscriptionId: "sub-1"},
	}
	dispatch := farmDispatch(playerID)
	base := plantCommand(t, playerID)
	first, err := ValidateCommand(base, ctx, dispatch)
	if err != nil {
		t.Fatal(err)
	}
	// 在合法信封 wire 尾部追加已退役字段(field5 target_owner_id=999、
	// field7 expected_plot_version=999);解码后它们只是未知字段。
	wire, mErr := proto.Marshal(base)
	if mErr != nil {
		t.Fatal(mErr)
	}
	wire = append(wire, 0x28, 0xe7, 0x07) // field 5 varint 999
	wire = append(wire, 0x38, 0xe7, 0x07) // field 7 varint 999
	parsed := &commonpb.ClientCommandEnvelope{}
	if err := proto.Unmarshal(wire, parsed); err != nil {
		t.Fatal(err)
	}
	second, err := ValidateCommand(parsed, ctx, dispatch)
	if err != nil {
		t.Fatalf("retired wire fields must be ignored, got %v", err)
	}
	if string(first.PayloadHash) != string(second.PayloadHash) {
		t.Fatal("retired wire fields changed authoritative hash")
	}
	if first.RouteKey != second.RouteKey {
		t.Fatal("retired wire fields changed route key")
	}
}

// TestTargetFarmPolicyFirstHopEnforcement 是主集成审查 P1 的定点回归:
// FirstHop 必须独立执行 28 §6.1 的完整 TargetFarmPolicy,而不是只检查
// target==admitted_farm。
func TestTargetFarmPolicyFirstHopEnforcement(t *testing.T) {
	const playerA = uint64(10001)
	const farmB = uint64(20002)
	farmCtx := func(farm uint64) *logicrpcpb.TrustedCommandContext {
		return &logicrpcpb.TrustedCommandContext{
			Session: testSession(playerA), RecoveryCompleteAtAdmission: true,
			FarmContext: &logicrpcpb.FarmAdmissionContext{AdmittedFarmId: farm, AdmittedSubscriptionId: "sub-1"},
		}
	}
	plantOn := func(farm uint64) *commonpb.ClientCommandEnvelope {
		return &commonpb.ClientCommandEnvelope{
			RequestId: "r1", OperationId: "o1", CommandType: commonpb.CommandType_PLANT,
			Payload: mustPayload(t, &publicpb.PlantRequest{PlotId: 3, CropDefinitionId: 1, TargetFarmId: farm, ExpectedPlotVersion: 9}),
		}
	}
	harvestOn := func(farm uint64) *commonpb.ClientCommandEnvelope {
		return &commonpb.ClientCommandEnvelope{
			RequestId: "r1", OperationId: "o1", CommandType: commonpb.CommandType_HARVEST,
			Payload: mustPayload(t, &publicpb.HarvestRequest{PlotId: 3, TargetFarmId: farm, CropCycleId: "cc-1"}),
		}
	}
	careOn := func(farm uint64) *commonpb.ClientCommandEnvelope {
		return &commonpb.ClientCommandEnvelope{
			RequestId: "r1", OperationId: "o1", CommandType: commonpb.CommandType_CARE,
			Payload: mustPayload(t, &publicpb.CareRequest{PlotId: 3, TargetFarmId: farm, CropCycleId: "cc-1", CareType: publicpb.CareType_CARE_TYPE_WATER}),
		}
	}
	theftOn := func(farm uint64) *commonpb.ClientCommandEnvelope {
		return &commonpb.ClientCommandEnvelope{
			RequestId: "r1", OperationId: "o1", CommandType: commonpb.CommandType_THEFT,
			Payload: mustPayload(t, &publicpb.TheftRequest{PlotId: 3, TargetFarmId: farm, CropCycleId: "cc-1"}),
		}
	}

	// session=A、admitted=B、PLANT/HARVEST target=B → OWN_CURRENT_FARM 违规,拒绝。
	for _, cmd := range []*commonpb.ClientCommandEnvelope{plantOn(farmB), harvestOn(farmB)} {
		if _, err := ValidateCommand(cmd, farmCtx(farmB), farmDispatch(farmB)); err == nil {
			t.Fatalf("%s on friend farm must be rejected at FirstHop", cmd.CommandType)
		}
	}
	// session=A、admitted=A、PLANT/HARVEST target=A → 接受。
	for _, cmd := range []*commonpb.ClientCommandEnvelope{plantOn(playerA), harvestOn(playerA)} {
		if _, err := ValidateCommand(cmd, farmCtx(playerA), farmDispatch(playerA)); err != nil {
			t.Fatalf("%s on own farm rejected: %v", cmd.CommandType, err)
		}
	}
	// CURRENT_FARM:CARE/THEFT target==admitted(B)时接受。
	for _, cmd := range []*commonpb.ClientCommandEnvelope{careOn(farmB), theftOn(farmB)} {
		if _, err := ValidateCommand(cmd, farmCtx(farmB), farmDispatch(farmB)); err != nil {
			t.Fatalf("%s on current farm rejected: %v", cmd.CommandType, err)
		}
	}
	// target≠admitted(CARE)与 route header≠target(THEFT)仍 fail closed。
	if _, err := ValidateCommand(careOn(farmB), farmCtx(playerA), farmDispatch(farmB)); err == nil {
		t.Fatal("CARE target != admitted must fail closed")
	}
	if _, err := ValidateCommand(theftOn(farmB), farmCtx(farmB), farmDispatch(playerA)); err == nil {
		t.Fatal("THEFT route header != recomputed target must fail closed")
	}
}
