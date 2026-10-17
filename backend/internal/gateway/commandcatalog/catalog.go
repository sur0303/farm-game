package commandcatalog

import (
	"errors"
	"fmt"

	commonpb "farmgame/api/gen/common"
)

// DefaultProtocolVersion 是 P0_BASE 语义占位对应的现行协商协议版本。
// 28 §6.1:P0_BASE 的最终数值在 Proto/滚动兼容评审时一次性冻结;本目录与
// internal/gateway/protocol.ProtocolVersion 保持一致。
const DefaultProtocolVersion uint32 = 4

// allSpecs 是第 28 号 §6.1 冻结的 33 类公开命令完整 CommandSpec 注册表。
// 每个公开 CommandType 恰好一个 CommandSpec;不允许第二套 Catalog,也不允许
// 注册表外命令。Availability.Enabled=false 的命令已注册合同但业务 Handler
// 尚未接线:Gate 在准入阶段以 ERR_COMMAND_UNIMPLEMENTED 明确拒绝。
//
// 本轮接线状态(ICR-P1B-PROTOCOL-CONSUMERS 受限范围):JOIN_FARM / BUY / SELL
// 的 Typed Payload 与旧 FirstHop 解码字节兼容,保持接线;本轮另接通
// GET_OPERATION_RESULT 的只读 OperationReadAPI。其余命令的 Payload/Result
// 需要 FirstHop/Ingress 重接(超出本轮受限 ICR),统一 feature-disabled,绝不
// 伪造业务成功。逐条阻塞原因见 docs/verification/p1b-command-catalog-handoff.md。
var allSpecs = []CommandSpec{
	// ── 1. JOIN_FARM(28 §5.1)──
	{
		CommandType: commonpb.CommandType_JOIN_FARM,
		PayloadType: "public.JoinFarmRequest", ResultType: "NO_RESULT_PAYLOAD",
		MaxPayloadBytes: DefaultMaxPayloadBytes,
		Availability:    AvailabilityPolicy{Enabled: true, FeatureKey: "feature.join_farm"},
		SessionAdmission: SessionAdmissionPolicy{
			RequiresAuthenticated: true, TargetFarmPolicy: TargetFarmPayloadTarget,
		},
		ExecutionKind:   ExecutionDomainRole,
		ExecutionAccept: ExecutionAcceptRuntimeOperationAccepted,
		TerminalOutcome: TerminalOutcomeNoDurable,
		FirstHop:        FirstHopFarmSvr, FirstHopName: "farm-svr / JoinIngress",
		RouteMode:      RouteModeActorAffinity,
		RouteDomain:    RouteDomainFarm,
		RouteKeySource: RouteKeySourcePayloadField,
		OperationRoot:  OperationRootNone,
		Idempotency:    IdempotencyRuntimeRequired,
		Retry:          RetryPolicy{Kind: RetrySameOperation},
		Recovery:       RecoveryFreshJoin,
		Timeout:        TimeoutBudget{ConfigKey: "cfg.timeout.join_farm"},
		RateLimit:      RateLimitPolicy{ConfigKey: "cfg.rate.join_farm"},
		PermitClass:    PermitJoinFarm,
	},
	// ── 2. GET_PLAYER_BOOTSTRAP(28 §3.7,L2-PROTO-62)──
	{
		CommandType: commonpb.CommandType_GET_PLAYER_BOOTSTRAP,
		PayloadType: "public.GetPlayerBootstrapRequest", ResultType: "public.GetPlayerBootstrapResult",
		MaxPayloadBytes: DefaultMaxPayloadBytes,
		Availability:    AvailabilityPolicy{Enabled: true, FeatureKey: "feature.player_bootstrap"},
		SessionAdmission: SessionAdmissionPolicy{
			RequiresAuthenticated: true, TargetFarmPolicy: TargetFarmSessionPlayer,
		},
		ExecutionKind:   ExecutionActorDirect,
		ExecutionAccept: ExecutionAcceptActorMailboxAccepted,
		TerminalOutcome: TerminalOutcomeNoDurable,
		FirstHop:        FirstHopPlayerSvr, FirstHopName: "player-svr / PlayerBootstrapIngress",
		RouteMode:      RouteModeActorAffinity,
		RouteDomain:    RouteDomainPlayer,
		RouteKeySource: RouteKeySourceTrustedContextField,
		OperationRoot:  OperationRootNone,
		Idempotency:    IdempotencyNone,
		Retry:          RetryPolicy{Kind: RetrySafeQuery},
		Recovery:       RecoveryRequery,
		Timeout:        TimeoutBudget{ConfigKey: "cfg.timeout.player_bootstrap"},
		RateLimit:      RateLimitPolicy{ConfigKey: "cfg.rate.player_bootstrap"},
		PermitClass:    PermitReadOnly,
	},
	// ── 3. GET_FRIEND_LIST ──
	playerServiceQuery(commonpb.CommandType_GET_FRIEND_LIST,
		"public.GetFriendListRequest", "public.GetFriendListResult",
		"feature.friend_list", "player-svr / FriendReadAPI",
		"cfg.timeout.get_friend_list", "cfg.rate.get_friend_list"),
	// ── 4. GET_NEXT_FRIEND_OPPORTUNITY(L2-PROTO-63)──
	playerServiceQuery(commonpb.CommandType_GET_NEXT_FRIEND_OPPORTUNITY,
		"public.GetNextFriendOpportunityRequest", "public.GetNextFriendOpportunityResult",
		"feature.friend_opportunity", "player-svr / FriendOpportunityNavigation",
		"cfg.timeout.friend_opportunity", "cfg.rate.friend_opportunity"),
	// ── 5. GET_OPERATION_RESULT(28 §3.8)──
	{
		CommandType: commonpb.CommandType_GET_OPERATION_RESULT,
		PayloadType: "public.GetOperationResultRequest", ResultType: "public.GetOperationResultResult",
		MaxPayloadBytes: DefaultMaxPayloadBytes,
		Availability:    AvailabilityPolicy{Enabled: true, FeatureKey: "feature.operation_result"},
		SessionAdmission: SessionAdmissionPolicy{
			RequiresAuthenticated: true, TargetFarmPolicy: TargetFarmSubjectRootOperation,
		},
		ExecutionKind:   ExecutionDomainRole,
		ExecutionAccept: ExecutionAcceptStatelessHandlerStarted,
		TerminalOutcome: TerminalOutcomeNoDurable,
		FirstHop:        FirstHopPlayerSvr, FirstHopName: "player-svr / OperationReadAPI",
		RouteMode:      RouteModeServiceLB,
		RouteDomain:    RouteDomainUnspecified,
		RouteKeySource: RouteKeySourceNone,
		OperationRoot:  OperationRootNone,
		Idempotency:    IdempotencyNone,
		Retry:          RetryPolicy{Kind: RetryPollOperationResult},
		Recovery:       RecoveryRequery,
		Timeout:        TimeoutBudget{ConfigKey: "cfg.timeout.get_operation_result"},
		RateLimit:      RateLimitPolicy{ConfigKey: "cfg.rate.get_operation_result"},
		PermitClass:    PermitReadOnly,
	},
	// ── 6. GET_TASK_LIST(L2-PROTO-51)──
	func() CommandSpec {
		spec := playerActorQuery(commonpb.CommandType_GET_TASK_LIST,
			"public.GetTaskListRequest", "public.GetTaskListResult",
			"feature.task", "player-svr / TaskReadAPI",
			"cfg.timeout.task_read", "cfg.rate.get_task_list", true)
		spec.Availability.Enabled = true
		return spec
	}(),
	// ── 7. CLAIM_TASK(L2-PROTO-52)──
	playerOwnerWrite(commonpb.CommandType_CLAIM_TASK,
		"public.ClaimTaskRequest", "public.ClaimTaskResult",
		"feature.task", "player-svr / TaskCommandIngress",
		"cfg.timeout.task_write", "cfg.rate.claim_task",
		ExecutionActorDirect, ExecutionAcceptActorMailboxAccepted, true),
	// ── 8. CONFIRM_FRIEND_VISIT(L2-PROTO-53)──
	{
		CommandType: commonpb.CommandType_CONFIRM_FRIEND_VISIT,
		PayloadType: "public.ConfirmFriendVisitRequest", ResultType: "public.ConfirmFriendVisitResult",
		MaxPayloadBytes: DefaultMaxPayloadBytes,
		Availability:    AvailabilityPolicy{Enabled: true, FeatureKey: "feature.task"},
		SessionAdmission: SessionAdmissionPolicy{
			RequiresAuthenticated: true, RequiresRecoveryComplete: true,
			RequiresCurrentSubscription: true,
			TargetFarmPolicy:            TargetFarmCurrentFriendFarm,
		},
		ExecutionKind:   ExecutionActorDirect,
		ExecutionAccept: ExecutionAcceptActorMailboxAccepted,
		TerminalOutcome: TerminalOutcomeDurableAfterAccept,
		FirstHop:        FirstHopPlayerSvr, FirstHopName: "player-svr / TaskCommandIngress",
		RouteMode:      RouteModeActorAffinity,
		RouteDomain:    RouteDomainPlayer,
		RouteKeySource: RouteKeySourceTrustedContextField,
		OperationRoot:  OperationRootPlayerActor,
		Idempotency:    IdempotencyRequired,
		Retry:          RetryPolicy{Kind: RetrySameOperation},
		Recovery:       RecoveryFarmFreshJoinThenRetry,
		Timeout:        TimeoutBudget{ConfigKey: "cfg.timeout.task_write"},
		RateLimit:      RateLimitPolicy{ConfigKey: "cfg.rate.confirm_friend_visit"},
		PermitClass:    PermitOwnerWrite,
	},
	// ── 9~12. 邮件读/单调写(28 §6.1,L2-PROTO-55)──
	playerServiceQuerySessionPlayer(commonpb.CommandType_GET_MAIL_LIST,
		"public.GetMailListRequest", "public.GetMailListResult",
		"feature.mail", "player-svr / PlayerMailReadAPI",
		"cfg.timeout.mail_read", "cfg.rate.get_mail_list"),
	playerServiceQuerySessionPlayer(commonpb.CommandType_GET_MAIL_DETAIL,
		"public.GetMailDetailRequest", "public.GetMailDetailResult",
		"feature.mail", "player-svr / PlayerMailReadAPI",
		"cfg.timeout.mail_read", "cfg.rate.get_mail_detail"),
	playerServiceQuerySessionPlayer(commonpb.CommandType_MARK_MAIL_READ,
		"public.MarkMailReadRequest", "public.MarkMailReadResult",
		"feature.mail", "player-svr / PlayerMailCommandAPI",
		"cfg.timeout.mail_write", "cfg.rate.mark_mail_read"),
	playerServiceQuerySessionPlayer(commonpb.CommandType_DELETE_MAIL,
		"public.DeleteMailRequest", "public.DeleteMailResult",
		"feature.mail", "player-svr / PlayerMailCommandAPI",
		"cfg.timeout.mail_write", "cfg.rate.delete_mail"),
	// ── 13. CLAIM_MAIL_ATTACHMENT(L2-PROTO-55;Root=PLAYER_MAIL_ENTRY:subject/mail_id)──
	func() CommandSpec {
		spec := playerOwnerWrite(commonpb.CommandType_CLAIM_MAIL_ATTACHMENT,
			"public.ClaimMailAttachmentRequest", "public.ClaimMailAttachmentResult",
			"feature.mail", "player-svr / PlayerMailClaimIngress",
			"cfg.timeout.mail_claim", "cfg.rate.claim_mail_attachment",
			ExecutionDomainRole, ExecutionAcceptActorMailboxAccepted, true)
		spec.OperationRoot = OperationRootPlayerMailEntry
		return spec
	}(),
	// ── 14. GET_PET_STATE(L2-PROTO-58)──
	func() CommandSpec {
		spec := playerActorQuery(commonpb.CommandType_GET_PET_STATE,
			"public.GetPetStateRequest", "public.GetPetStateResult",
			"feature.pet", "player-svr / PlayerReadAPI",
			"cfg.timeout.pet_read", "cfg.rate.get_pet_state", true)
		spec.Availability.Enabled = true
		return spec
	}(),
	// ── 15~17. 宠物写 ──
	playerOwnerWrite(commonpb.CommandType_PET_UNLOCK_BY_COIN,
		"public.PetUnlockByCoinRequest", "public.PetUnlockByCoinResult",
		"feature.pet", "player-svr / PlayerIngress",
		"cfg.timeout.pet_write", "cfg.rate.pet_unlock",
		ExecutionActorDirect, ExecutionAcceptActorMailboxAccepted, true),
	playerOwnerWrite(commonpb.CommandType_PET_FEED,
		"public.PetFeedRequest", "public.PetFeedResult",
		"feature.pet", "player-svr / PlayerIngress",
		"cfg.timeout.pet_write", "cfg.rate.pet_feed",
		ExecutionActorDirect, ExecutionAcceptActorMailboxAccepted, true),
	playerOwnerWrite(commonpb.CommandType_PET_SWITCH,
		"public.PetSwitchRequest", "public.PetSwitchResult",
		"feature.pet", "player-svr / PlayerIngress",
		"cfg.timeout.pet_write", "cfg.rate.pet_switch",
		ExecutionActorDirect, ExecutionAcceptActorMailboxAccepted, true),
	// ── 18. GET_COMPENDIUM(L2-PROTO-59:CompendiumReadIngress,Actor Turn 外组装)──
	{
		CommandType: commonpb.CommandType_GET_COMPENDIUM,
		PayloadType: "public.GetCompendiumRequest", ResultType: "public.GetCompendiumResult",
		MaxPayloadBytes: DefaultMaxPayloadBytes,
		Availability:    AvailabilityPolicy{Enabled: true, FeatureKey: "feature.compendium"},
		SessionAdmission: SessionAdmissionPolicy{
			RequiresAuthenticated: true, RequiresRecoveryComplete: true,
			TargetFarmPolicy: TargetFarmSessionPlayer,
		},
		ExecutionKind:   ExecutionDomainRole,
		ExecutionAccept: ExecutionAcceptStatelessHandlerStarted,
		TerminalOutcome: TerminalOutcomeNoDurable,
		FirstHop:        FirstHopPlayerSvr, FirstHopName: "player-svr / CompendiumReadIngress",
		RouteMode:      RouteModeActorAffinity,
		RouteDomain:    RouteDomainPlayer,
		RouteKeySource: RouteKeySourceTrustedContextField,
		OperationRoot:  OperationRootNone,
		Idempotency:    IdempotencyNone,
		Retry:          RetryPolicy{Kind: RetrySafeQuery},
		Recovery:       RecoveryRequery,
		Timeout:        TimeoutBudget{ConfigKey: "cfg.timeout.compendium_read"},
		RateLimit:      RateLimitPolicy{ConfigKey: "cfg.rate.get_compendium"},
		PermitClass:    PermitReadOnly,
	},
	// ── 19. ACK_COMPENDIUM_SEEN(L2-PROTO-60:单调 Ack 安全重入)──
	{
		CommandType: commonpb.CommandType_ACK_COMPENDIUM_SEEN,
		PayloadType: "public.AckCompendiumSeenRequest", ResultType: "public.AckCompendiumSeenResult",
		MaxPayloadBytes: DefaultMaxPayloadBytes,
		Availability:    AvailabilityPolicy{Enabled: true, FeatureKey: "feature.compendium"},
		SessionAdmission: SessionAdmissionPolicy{
			RequiresAuthenticated: true, RequiresRecoveryComplete: true,
			TargetFarmPolicy: TargetFarmSessionPlayer,
		},
		ExecutionKind:   ExecutionActorDirect,
		ExecutionAccept: ExecutionAcceptActorMailboxAccepted,
		TerminalOutcome: TerminalOutcomeNoDurable,
		FirstHop:        FirstHopPlayerSvr, FirstHopName: "player-svr / PlayerIngress",
		RouteMode:      RouteModeActorAffinity,
		RouteDomain:    RouteDomainPlayer,
		RouteKeySource: RouteKeySourceTrustedContextField,
		OperationRoot:  OperationRootNone,
		Idempotency:    IdempotencyNone,
		Retry:          RetryPolicy{Kind: RetrySafeMonotonic},
		Recovery:       RecoveryRequery,
		Timeout:        TimeoutBudget{ConfigKey: "cfg.timeout.compendium_write"},
		RateLimit:      RateLimitPolicy{ConfigKey: "cfg.rate.ack_compendium_seen"},
		PermitClass:    PermitOwnerWrite,
	},
	// ── 20~22. Share Token(28 §5.7;Root=FRIEND_SHARE_ISSUER:subject)──
	shareTokenWrite(commonpb.CommandType_GENERATE_SHARE_TOKEN,
		"public.GenerateShareTokenRequest", "cfg.rate.share_token_write"),
	shareTokenWrite(commonpb.CommandType_ROTATE_SHARE_TOKEN,
		"public.RotateShareTokenRequest", "cfg.rate.share_token_write"),
	shareTokenWrite(commonpb.CommandType_REVOKE_SHARE_TOKEN,
		"public.RevokeShareTokenRequest", "cfg.rate.share_token_write"),
	// ── 23. REDEEM_SHARE(WORKFLOW_COORDINATOR / WORK_CREATED)──
	{
		CommandType: commonpb.CommandType_REDEEM_SHARE,
		PayloadType: "public.RedeemShareRequest", ResultType: "public.RedeemShareResult",
		MaxPayloadBytes: DefaultMaxPayloadBytes,
		Availability:    AvailabilityPolicy{Enabled: false, FeatureKey: "feature.share_friend"},
		SessionAdmission: SessionAdmissionPolicy{
			RequiresAuthenticated: true, RequiresRecoveryComplete: true,
			TargetFarmPolicy: TargetFarmTokenInviterSessionRedeemer,
		},
		ExecutionKind:   ExecutionWorkflowCoordinator,
		ExecutionAccept: ExecutionAcceptWorkCreated,
		TerminalOutcome: TerminalOutcomeDurableAfterAccept,
		FirstHop:        FirstHopPlayerSvr, FirstHopName: "player-svr / AddFriendCoordinator",
		RouteMode:      RouteModeServiceLB,
		RouteDomain:    RouteDomainUnspecified,
		RouteKeySource: RouteKeySourceNone,
		OperationRoot:  OperationRootFriendPair,
		Idempotency:    IdempotencyRequired,
		Retry:          RetryPolicy{Kind: RetrySameOperation},
		Recovery:       RecoveryPlayerRetrySameOperation,
		Timeout:        TimeoutBudget{ConfigKey: "cfg.timeout.add_friend_workflow"},
		RateLimit:      RateLimitPolicy{ConfigKey: "cfg.rate.redeem_share"},
		PermitClass:    PermitOwnerWrite,
	},
	// ── 24~26. 好友关系写(SESSION_CALLER + PAYLOAD_PEER)──
	friendRelationWrite(commonpb.CommandType_DELETE_FRIEND, "public.DeleteFriendRequest", "public.DeleteFriendResult"),
	friendRelationWrite(commonpb.CommandType_BLOCK_PLAYER, "public.BlockPlayerRequest", "public.BlockPlayerResult"),
	friendRelationWrite(commonpb.CommandType_UNBLOCK_PLAYER, "public.UnblockPlayerRequest", "public.UnblockPlayerResult"),
	// ── 27~28. BUY / SELL(L2-PROTO-39:RouteKey 只来自可信 Session)──
	playerOwnerWrite(commonpb.CommandType_BUY,
		"public.BuyRequest", "public.BuyResult",
		"feature.buy", "player-svr / PlayerIngress",
		"cfg.timeout.buy", "cfg.rate.buy",
		ExecutionActorDirect, ExecutionAcceptActorMailboxAccepted, true),
	playerOwnerWrite(commonpb.CommandType_SELL,
		"public.SellRequest", "public.SellResult",
		"feature.sell", "player-svr / PlayerIngress",
		"cfg.timeout.sell", "cfg.rate.sell",
		ExecutionActorDirect, ExecutionAcceptActorMailboxAccepted, true),
	// ── 29. UNLOCK_PLOT(28 §5.2A)──
	farmWorkflowOwn(commonpb.CommandType_UNLOCK_PLOT,
		"public.UnlockPlotRequest", "public.UnlockPlotResult",
		"feature.unlock_plot", "farm-svr / UnlockPlotCoordinator",
		"cfg.timeout.unlock_plot", "cfg.rate.unlock_plot", true),
	// ── 30~31. PLANT / HARVEST(Payload 字段 1/2 与旧 FirstHop 解码布局
	// 兼容;expected_crop_rule_version 的规则版本校验待 32 接线)──
	farmWorkflowOwn(commonpb.CommandType_PLANT,
		"public.PlantRequest", "public.PlantResult",
		"feature.plant", "farm-svr / PlantCoordinator",
		"cfg.timeout.plant", "cfg.rate.plant", true),
	farmWorkflowOwn(commonpb.CommandType_HARVEST,
		"public.HarvestRequest", "public.HarvestResult",
		"feature.harvest", "farm-svr / HarvestCoordinator",
		"cfg.timeout.harvest", "cfg.rate.harvest", true),
	// ── 32. CARE(L2-PROTO-43 双接受边界)──
	{
		CommandType: commonpb.CommandType_CARE,
		PayloadType: "public.CareRequest", ResultType: "public.CareResult",
		MaxPayloadBytes: DefaultMaxPayloadBytes,
		Availability:    AvailabilityPolicy{Enabled: true, FeatureKey: "feature.care"},
		SessionAdmission: SessionAdmissionPolicy{
			RequiresAuthenticated: true, RequiresRecoveryComplete: true,
			RequiresCurrentSubscription: true,
			TargetFarmPolicy:            TargetFarmCurrentFarm,
		},
		ExecutionKind:   ExecutionDomainRole,
		ExecutionAccept: ExecutionAcceptOwnerMailboxFriendAdmission,
		TerminalOutcome: TerminalOutcomeDurableAfterAccept,
		FirstHop:        FirstHopFarmSvr, FirstHopName: "farm-svr / FriendFarmIngress",
		RouteMode:      RouteModeActorAffinity,
		RouteDomain:    RouteDomainFarm,
		RouteKeySource: RouteKeySourcePayloadField,
		OperationRoot:  OperationRootCareDual,
		Idempotency:    IdempotencyRequired,
		Retry:          RetryPolicy{Kind: RetrySameOperation},
		Recovery:       RecoveryFarmFreshJoinThenRetry,
		Timeout:        TimeoutBudget{ConfigKey: "cfg.timeout.care"},
		RateLimit:      RateLimitPolicy{ConfigKey: "cfg.rate.care"},
		PermitClass:    PermitOwnerWrite,
	},
	// ── 33. THEFT ──
	{
		CommandType: commonpb.CommandType_THEFT,
		PayloadType: "public.TheftRequest", ResultType: "public.TheftResult",
		MaxPayloadBytes: DefaultMaxPayloadBytes,
		Availability:    AvailabilityPolicy{Enabled: true, FeatureKey: "feature.theft"},
		SessionAdmission: SessionAdmissionPolicy{
			RequiresAuthenticated: true, RequiresRecoveryComplete: true,
			RequiresCurrentSubscription: true,
			TargetFarmPolicy:            TargetFarmCurrentFarm,
		},
		ExecutionKind:   ExecutionWorkflowCoordinator,
		ExecutionAccept: ExecutionAcceptFriendActionAdmissionCommitted,
		TerminalOutcome: TerminalOutcomeDurableAfterAccept,
		FirstHop:        FirstHopFarmSvr, FirstHopName: "farm-svr / FriendFarmCoordinator",
		RouteMode:      RouteModeActorAffinity,
		RouteDomain:    RouteDomainFarm,
		RouteKeySource: RouteKeySourcePayloadField,
		OperationRoot:  OperationRootFriendPair,
		Idempotency:    IdempotencyRequired,
		Retry:          RetryPolicy{Kind: RetrySameOperation},
		Recovery:       RecoveryFarmFreshJoinThenRetry,
		Timeout:        TimeoutBudget{ConfigKey: "cfg.timeout.theft"},
		RateLimit:      RateLimitPolicy{ConfigKey: "cfg.rate.theft"},
		PermitClass:    PermitOwnerWrite,
	},
}

// playerServiceQuery 是 SERVICE_LB 无状态玩家域查询(无 RouteDomain,
// viewer/subject 只来自 Session)。
func playerServiceQuery(commandType commonpb.CommandType, payloadType, resultType,
	featureKey, firstHopName, timeoutKey, rateKey string) CommandSpec {
	return CommandSpec{
		CommandType: commandType,
		PayloadType: payloadType, ResultType: resultType,
		MaxPayloadBytes: DefaultMaxPayloadBytes,
		Availability:    AvailabilityPolicy{Enabled: false, FeatureKey: featureKey},
		SessionAdmission: SessionAdmissionPolicy{
			RequiresAuthenticated: true, TargetFarmPolicy: TargetFarmNone,
		},
		ExecutionKind:   ExecutionDomainRole,
		ExecutionAccept: ExecutionAcceptStatelessHandlerStarted,
		TerminalOutcome: TerminalOutcomeNoDurable,
		FirstHop:        FirstHopPlayerSvr, FirstHopName: firstHopName,
		RouteMode:      RouteModeServiceLB,
		RouteDomain:    RouteDomainUnspecified,
		RouteKeySource: RouteKeySourceNone,
		OperationRoot:  OperationRootNone,
		Idempotency:    IdempotencyNone,
		Retry:          RetryPolicy{Kind: RetrySafeQuery},
		Recovery:       RecoveryRequery,
		Timeout:        TimeoutBudget{ConfigKey: timeoutKey},
		RateLimit:      RateLimitPolicy{ConfigKey: rateKey},
		PermitClass:    PermitReadOnly,
	}
}

// playerServiceQuerySessionPlayer 是目标策略为 SESSION_PLAYER 的 SERVICE_LB
// 查询/单调写(邮件列表/详情/已读/删除;subject 只来自 Session)。
func playerServiceQuerySessionPlayer(commandType commonpb.CommandType, payloadType, resultType,
	featureKey, firstHopName, timeoutKey, rateKey string) CommandSpec {
	spec := playerServiceQuery(commandType, payloadType, resultType, featureKey, firstHopName, timeoutKey, rateKey)
	spec.Availability.Enabled = true
	spec.SessionAdmission.TargetFarmPolicy = TargetFarmSessionPlayer
	return spec
}

// playerActorQuery 是 PLAYER Actor Affinity 只读命令(ACTOR_DIRECT,可信
// Session 提取 RouteKey;requiresRecovery 按 28 §6.1 行决定)。
func playerActorQuery(commandType commonpb.CommandType, payloadType, resultType,
	featureKey, firstHopName, timeoutKey, rateKey string, requiresRecovery bool) CommandSpec {
	return CommandSpec{
		CommandType: commandType,
		PayloadType: payloadType, ResultType: resultType,
		MaxPayloadBytes: DefaultMaxPayloadBytes,
		Availability:    AvailabilityPolicy{Enabled: false, FeatureKey: featureKey},
		SessionAdmission: SessionAdmissionPolicy{
			RequiresAuthenticated:    true,
			RequiresRecoveryComplete: requiresRecovery,
			TargetFarmPolicy:         TargetFarmSessionPlayer,
		},
		ExecutionKind:   ExecutionActorDirect,
		ExecutionAccept: ExecutionAcceptActorMailboxAccepted,
		TerminalOutcome: TerminalOutcomeNoDurable,
		FirstHop:        FirstHopPlayerSvr, FirstHopName: firstHopName,
		RouteMode:      RouteModeActorAffinity,
		RouteDomain:    RouteDomainPlayer,
		RouteKeySource: RouteKeySourceTrustedContextField,
		OperationRoot:  OperationRootNone,
		Idempotency:    IdempotencyNone,
		Retry:          RetryPolicy{Kind: RetrySafeQuery},
		Recovery:       RecoveryRequery,
		Timeout:        TimeoutBudget{ConfigKey: timeoutKey},
		RateLimit:      RateLimitPolicy{ConfigKey: rateKey},
		PermitClass:    PermitReadOnly,
	}
}

// playerOwnerWrite 是 PLAYER Actor Affinity 持久写命令
// (Root=PLAYER_ACTOR:session.player_id,DURABLE_TERMINAL_AFTER_EXECUTION_ACCEPT,
// PLAYER_RETRY_SAME_OPERATION)。
func playerOwnerWrite(commandType commonpb.CommandType, payloadType, resultType,
	featureKey, firstHopName, timeoutKey, rateKey string,
	executionKind ExecutionKind, accept ExecutionAcceptPolicy, enabled bool) CommandSpec {
	return CommandSpec{
		CommandType: commandType,
		PayloadType: payloadType, ResultType: resultType,
		MaxPayloadBytes: DefaultMaxPayloadBytes,
		Availability:    AvailabilityPolicy{Enabled: enabled, FeatureKey: featureKey},
		SessionAdmission: SessionAdmissionPolicy{
			RequiresAuthenticated: true, RequiresRecoveryComplete: true,
			TargetFarmPolicy: TargetFarmSessionPlayer,
		},
		ExecutionKind:   executionKind,
		ExecutionAccept: accept,
		TerminalOutcome: TerminalOutcomeDurableAfterAccept,
		FirstHop:        FirstHopPlayerSvr, FirstHopName: firstHopName,
		RouteMode:      RouteModeActorAffinity,
		RouteDomain:    RouteDomainPlayer,
		RouteKeySource: RouteKeySourceTrustedContextField,
		OperationRoot:  OperationRootPlayerActor,
		Idempotency:    IdempotencyRequired,
		Retry:          RetryPolicy{Kind: RetrySameOperation},
		Recovery:       RecoveryPlayerRetrySameOperation,
		Timeout:        TimeoutBudget{ConfigKey: timeoutKey},
		RateLimit:      RateLimitPolicy{ConfigKey: rateKey},
		PermitClass:    PermitOwnerWrite,
	}
}

// shareTokenWrite 是 ShareIssuer 写(28 §5.7:SERVICE_LB,RouteKeySource 为
// TRUSTED_CONTEXT_FIELD,Root=FRIEND_SHARE_ISSUER:session.player_id)。
func shareTokenWrite(commandType commonpb.CommandType, payloadType, rateKey string) CommandSpec {
	return CommandSpec{
		CommandType: commandType,
		PayloadType: payloadType, ResultType: "public.ShareTokenResult",
		MaxPayloadBytes: DefaultMaxPayloadBytes,
		Availability:    AvailabilityPolicy{Enabled: false, FeatureKey: "feature.share_friend"},
		SessionAdmission: SessionAdmissionPolicy{
			RequiresAuthenticated: true, RequiresRecoveryComplete: true,
			TargetFarmPolicy: TargetFarmSessionPlayer,
		},
		ExecutionKind:   ExecutionDomainRole,
		ExecutionAccept: ExecutionAcceptAuthorityCommitted,
		TerminalOutcome: TerminalOutcomeDurableAfterAccept,
		FirstHop:        FirstHopPlayerSvr, FirstHopName: "player-svr / ShareTokenAPI",
		RouteMode:      RouteModeServiceLB,
		RouteDomain:    RouteDomainUnspecified,
		RouteKeySource: RouteKeySourceTrustedContextField,
		OperationRoot:  OperationRootFriendShareIssuer,
		Idempotency:    IdempotencyRequired,
		Retry:          RetryPolicy{Kind: RetrySameOperation},
		Recovery:       RecoveryPlayerRetrySameOperation,
		Timeout:        TimeoutBudget{ConfigKey: "cfg.timeout.friend_authority_write"},
		RateLimit:      RateLimitPolicy{ConfigKey: rateKey},
		PermitClass:    PermitOwnerWrite,
	}
}

// friendRelationWrite 是 Pair 写(28 §5.7:SERVICE_LB,FirstHop 用 Session
// caller+payload peer 计算 canonical pair,Root=FRIEND_PAIR)。
func friendRelationWrite(commandType commonpb.CommandType, payloadType, resultType string) CommandSpec {
	return CommandSpec{
		CommandType: commandType,
		PayloadType: payloadType, ResultType: resultType,
		MaxPayloadBytes: DefaultMaxPayloadBytes,
		Availability:    AvailabilityPolicy{Enabled: false, FeatureKey: "feature.friend_relation_write"},
		SessionAdmission: SessionAdmissionPolicy{
			RequiresAuthenticated: true, RequiresRecoveryComplete: true,
			TargetFarmPolicy: TargetFarmSessionCallerPayloadPeer,
		},
		ExecutionKind:   ExecutionDomainRole,
		ExecutionAccept: ExecutionAcceptAuthorityCommitted,
		TerminalOutcome: TerminalOutcomeDurableAfterAccept,
		FirstHop:        FirstHopPlayerSvr, FirstHopName: "player-svr / FriendshipPairAPI",
		RouteMode:      RouteModeServiceLB,
		RouteDomain:    RouteDomainUnspecified,
		RouteKeySource: RouteKeySourceNone,
		OperationRoot:  OperationRootFriendPair,
		Idempotency:    IdempotencyRequired,
		Retry:          RetryPolicy{Kind: RetrySameOperation},
		Recovery:       RecoveryPlayerRetrySameOperation,
		Timeout:        TimeoutBudget{ConfigKey: "cfg.timeout.friend_authority_write"},
		RateLimit:      RateLimitPolicy{ConfigKey: "cfg.rate.friend_relation_write"},
		PermitClass:    PermitOwnerWrite,
	}
}

// farmWorkflowOwn 是 OWN_CURRENT_FARM 的 FARM Workflow 协调命令
// (Root=FARM_ACTOR:target_farm_id,WORK_CREATED,FARM_FRESH_JOIN_THEN_RETRY)。
func farmWorkflowOwn(commandType commonpb.CommandType, payloadType, resultType,
	featureKey, firstHopName, timeoutKey, rateKey string, enabled bool) CommandSpec {
	return CommandSpec{
		CommandType: commandType,
		PayloadType: payloadType, ResultType: resultType,
		MaxPayloadBytes: DefaultMaxPayloadBytes,
		Availability:    AvailabilityPolicy{Enabled: enabled, FeatureKey: featureKey},
		SessionAdmission: SessionAdmissionPolicy{
			RequiresAuthenticated: true, RequiresRecoveryComplete: true,
			RequiresCurrentSubscription: true,
			TargetFarmPolicy:            TargetFarmOwnCurrentFarm,
		},
		ExecutionKind:   ExecutionWorkflowCoordinator,
		ExecutionAccept: ExecutionAcceptWorkCreated,
		TerminalOutcome: TerminalOutcomeDurableAfterAccept,
		FirstHop:        FirstHopFarmSvr, FirstHopName: firstHopName,
		RouteMode:      RouteModeActorAffinity,
		RouteDomain:    RouteDomainFarm,
		RouteKeySource: RouteKeySourcePayloadField,
		OperationRoot:  OperationRootFarmActor,
		Idempotency:    IdempotencyRequired,
		Retry:          RetryPolicy{Kind: RetrySameOperation},
		Recovery:       RecoveryFarmFreshJoinThenRetry,
		Timeout:        TimeoutBudget{ConfigKey: timeoutKey},
		RateLimit:      RateLimitPolicy{ConfigKey: rateKey},
		PermitClass:    PermitOwnerWrite,
	}
}

// specIndex 是唯一查找索引;MustInit 在包初始化时完成完整性校验。
var specIndex = func() map[commonpb.CommandType]CommandSpec {
	index := make(map[commonpb.CommandType]CommandSpec, len(allSpecs))
	for i := range allSpecs {
		spec := allSpecs[i]
		if err := validateSpec(spec); err != nil {
			panic(fmt.Sprintf("commandcatalog: invalid spec for %s: %v", spec.CommandType, err))
		}
		if err := wireSharedSchema(&spec); err != nil {
			panic(fmt.Sprintf("commandcatalog: schema wiring for %s: %v", spec.CommandType, err))
		}
		allSpecs[i] = spec
		if _, dup := index[spec.CommandType]; dup {
			panic(fmt.Sprintf("commandcatalog: duplicate spec for %s", spec.CommandType))
		}
		index[spec.CommandType] = spec
	}
	if len(index) != 33 {
		panic(fmt.Sprintf("commandcatalog: spec count=%d, want 33", len(index)))
	}
	return index
}()

// wireSharedSchema 绑定共享解析层(Decoder / RouteKeyExtractor /
// CanonicalPayloadBuilder)。Gate 与 FirstHop 共用同一实现,不维护两套。
func wireSharedSchema(spec *CommandSpec) error {
	spec.Decoder = decoderFor(spec.CommandType)
	if spec.Decoder == nil {
		return errors.New("missing payload decoder")
	}
	// 28 §6.1:ProtocolRange = P0_BASE..OPEN_ENDED。P0_BASE 是语义占位名,
	// 数值与现行协商协议版本一致;OPEN_ENDED 以 0 表示。
	if spec.MinProtocolVersion == 0 {
		spec.MinProtocolVersion = DefaultProtocolVersion
	}
	if spec.MaxProtocolVersion != OpenEndedProtocolVersion && spec.MaxProtocolVersion < spec.MinProtocolVersion {
		return errors.New("max protocol version below min")
	}
	if spec.RouteMode == RouteModeActorAffinity {
		switch spec.RouteKeySource {
		case RouteKeySourcePayloadField:
			spec.RouteKeyExtractor = routeKeyFromPayloadField
		case RouteKeySourceTrustedContextField:
			spec.RouteKeyExtractor = routeKeyFromSession
		default:
			return errors.New("ACTOR_AFFINITY requires PAYLOAD_FIELD or TRUSTED_CONTEXT_FIELD route key source")
		}
	}
	if spec.Idempotency.RequiresCanonicalBuilder() {
		spec.CanonicalPayloadBuilder = canonicalBuilder(spec.CommandType)
	}
	if spec.CommandType == commonpb.CommandType_CARE {
		spec.Validator = validateCareIncident
	}
	return nil
}

func validateSpec(spec CommandSpec) error {
	if spec.CommandType == commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED {
		return errors.New("command type unspecified")
	}
	if spec.PayloadType == "" || spec.ResultType == "" {
		return errors.New("payload/result type required")
	}
	if spec.MaxPayloadBytes == 0 ||
		(spec.MaxPayloadBytes < 0 && spec.MaxPayloadBytes != MaxPayloadBytesUnlimited) {
		return errors.New("max payload bytes must be positive or unlimited")
	}
	if spec.Availability.FeatureKey == "" {
		return errors.New("availability feature key required")
	}
	if !spec.SessionAdmission.RequiresAuthenticated {
		return errors.New("P0 commands require an authenticated session")
	}
	if spec.ExecutionKind == ExecutionKindUnspecified {
		return errors.New("execution kind required")
	}
	if spec.ExecutionAccept == ExecutionAcceptUnspecified {
		return errors.New("execution accept policy required")
	}
	if spec.TerminalOutcome == TerminalOutcomeUnspecified {
		return errors.New("terminal outcome policy required")
	}
	if spec.FirstHop == FirstHopUnspecified || spec.FirstHopName == "" {
		return errors.New("first hop required")
	}
	if spec.RouteMode == RouteModeUnspecified {
		return errors.New("route mode required")
	}
	switch spec.RouteMode {
	case RouteModeActorAffinity:
		if spec.RouteDomain == RouteDomainUnspecified {
			return errors.New("ACTOR_AFFINITY requires route domain")
		}
		if spec.RouteKeySource != RouteKeySourcePayloadField &&
			spec.RouteKeySource != RouteKeySourceTrustedContextField {
			return errors.New("ACTOR_AFFINITY requires declared route key source")
		}
	case RouteModeServiceLB, RouteModeNone:
		if spec.RouteDomain != RouteDomainUnspecified {
			return errors.New("SERVICE_LB/NONE must not declare actor route domain")
		}
	}
	if spec.RouteKeySource == RouteKeySourceUnspecified {
		return errors.New("route key source must be declared (NONE allowed)")
	}
	if spec.Idempotency == IdempotencyUnspecified {
		return errors.New("idempotency policy required")
	}
	if spec.Retry.Kind == RetryUnspecified {
		return errors.New("retry policy required")
	}
	if spec.Recovery == RecoveryUnspecified {
		return errors.New("recovery policy required")
	}
	if spec.Timeout.ConfigKey == "" || spec.RateLimit.ConfigKey == "" {
		return errors.New("timeout/rate limit config keys required")
	}
	if spec.PermitClass == 0 {
		return errors.New("permit class required")
	}
	return nil
}

// Lookup 返回命令类型的注册说明书。
func Lookup(commandType commonpb.CommandType) (CommandSpec, bool) {
	spec, ok := specIndex[commandType]
	return spec, ok
}

// All 返回全部注册的 CommandSpec(稳定顺序)。
func All() []CommandSpec {
	out := make([]CommandSpec, len(allSpecs))
	copy(out, allSpecs)
	return out
}

// MustInit 在启动期强制执行完整性自检;任何漂移都 fail closed。
func MustInit() {
	if err := ValidateCommandCatalog(); err != nil {
		panic(err)
	}
	if err := ValidatePushCatalog(); err != nil {
		panic(err)
	}
}
