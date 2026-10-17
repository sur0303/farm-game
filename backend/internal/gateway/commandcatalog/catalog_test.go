package commandcatalog

import (
	"testing"

	commonpb "farmgame/api/gen/common"
	publicpb "farmgame/api/gen/public"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// mustDecode 是测试共用的 typed decode 助手。
func mustDecode(t *testing.T, spec CommandSpec, payload []byte) proto.Message {
	t.Helper()
	decoded, err := spec.Decoder(payload)
	if err != nil {
		t.Fatalf("decode %s: %v", spec.CommandType, err)
	}
	return decoded
}

func marshal(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	raw, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestExactlyThirtyThreeSpecs 证明 33/33 exactly-once 注册:无重复、无遗漏,
// 且每个公开 CommandType 恰好一个 CommandSpec(28 §6.4 测试 1/3)。
func TestExactlyThirtyThreeSpecs(t *testing.T) {
	all := All()
	if len(all) != 33 {
		t.Fatalf("spec count=%d, want 33", len(all))
	}
	seen := make(map[commonpb.CommandType]int, len(all))
	for _, spec := range all {
		seen[spec.CommandType]++
		if spec.CommandType == commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED {
			t.Fatal("UNSPECIFIED command registered")
		}
	}
	for commandType, count := range seen {
		if count != 1 {
			t.Fatalf("command %s registered %d times", commandType, count)
		}
	}
	// 枚举中每个公开命令(排除 UNSPECIFIED 与已删除的 7/10)都必须有 Spec,
	// 已删除命令必须没有 Spec(旧命令不存在双注册)。
	for number, name := range commonpb.CommandType_name {
		commandType := commonpb.CommandType(number)
		if commandType == commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED {
			continue
		}
		_, registered := Lookup(commandType)
		switch commandType {
		case commonpb.CommandType(7), commonpb.CommandType(10): // LEAVE_FARM / GET_FARM_SNAPSHOT(已删除)
			if registered {
				t.Fatalf("removed command %s has a registered spec (double mainline)", name)
			}
		default:
			if !registered {
				t.Fatalf("public command %s missing spec", name)
			}
		}
	}
	if err := ValidateCommandCatalog(); err != nil {
		t.Fatalf("ValidateCommandCatalog: %v", err)
	}
	if err := ValidatePushCatalog(); err != nil {
		t.Fatalf("ValidatePushCatalog: %v", err)
	}
}

// TestEverySpecDeclaresFullContract 证明每条 CommandSpec 都声明
// Payload/Result descriptor、max bytes、Admission、FirstHop、RouteDomain/
// RouteKey、Root、Idempotency、RetryDirective 合同、TimeoutBudget、RateLimit
// (28 §6.4 测试 2;原始 prompt 第 3 条)。
func TestEverySpecDeclaresFullContract(t *testing.T) {
	for _, spec := range All() {
		if spec.PayloadType == "" || spec.ResultType == "" {
			t.Errorf("%s: payload/result descriptor missing", spec.CommandType)
			continue
		}
		if spec.MaxPayloadBytes <= 0 {
			t.Errorf("%s: max payload bytes must be a bounded positive value", spec.CommandType)
		}
		if spec.Decoder == nil {
			t.Errorf("%s: decoder missing", spec.CommandType)
		}
		if !spec.SessionAdmission.RequiresAuthenticated {
			t.Errorf("%s: must require authenticated session", spec.CommandType)
		}
		if spec.Availability.FeatureKey == "" {
			t.Errorf("%s: availability feature key missing", spec.CommandType)
		}
		if spec.ExecutionKind == ExecutionKindUnspecified || spec.ExecutionAccept == ExecutionAcceptUnspecified ||
			spec.TerminalOutcome == TerminalOutcomeUnspecified {
			t.Errorf("%s: execution/accept/terminal policy incomplete", spec.CommandType)
		}
		if spec.FirstHop == FirstHopUnspecified || spec.FirstHopName == "" {
			t.Errorf("%s: first hop missing", spec.CommandType)
		}
		if spec.RouteMode == RouteModeUnspecified || spec.RouteKeySource == RouteKeySourceUnspecified {
			t.Errorf("%s: route mode/key source missing", spec.CommandType)
		}
		if spec.RouteMode == RouteModeActorAffinity {
			if spec.RouteDomain == RouteDomainUnspecified || spec.RouteKeyExtractor == nil {
				t.Errorf("%s: ACTOR_AFFINITY requires route domain and key extractor", spec.CommandType)
			}
		} else if spec.RouteDomain != RouteDomainUnspecified {
			t.Errorf("%s: SERVICE_LB/NONE must not declare actor route domain", spec.CommandType)
		}
		if spec.OperationRoot == OperationRootUnspecified {
			t.Errorf("%s: operation root policy missing", spec.CommandType)
		}
		if spec.Idempotency == IdempotencyUnspecified {
			t.Errorf("%s: idempotency policy missing", spec.CommandType)
		}
		if spec.Idempotency.RequiresCanonicalBuilder() && spec.CanonicalPayloadBuilder == nil {
			t.Errorf("%s: REQUIRED/RUNTIME_REQUIRED idempotency needs CanonicalPayloadBuilder", spec.CommandType)
		}
		if spec.Retry.Kind == RetryUnspecified || spec.Recovery == RecoveryUnspecified {
			t.Errorf("%s: retry/recovery policy missing", spec.CommandType)
		}
		if spec.Timeout.ConfigKey == "" || spec.RateLimit.ConfigKey == "" {
			t.Errorf("%s: timeout/rate limit cfg keys missing", spec.CommandType)
		}
		if spec.MinProtocolVersion == 0 {
			t.Errorf("%s: min protocol version missing", spec.CommandType)
		}
		if spec.PermitClass == 0 {
			t.Errorf("%s: permit class missing", spec.CommandType)
		}
	}
}

// TestPayloadAndResultDescriptorsResolve 证明每条 PayloadType/ResultType 名称
// 都能在 protoregistry 中解析为真实消息 descriptor(JOIN_FARM 的
// NO_RESULT_PAYLOAD 除外,其元信息走 JoinResultContext)。
func TestPayloadAndResultDescriptorsResolve(t *testing.T) {
	for _, spec := range All() {
		for _, typeName := range []string{spec.PayloadType, spec.ResultType} {
			if typeName == "NO_RESULT_PAYLOAD" {
				if spec.CommandType != commonpb.CommandType_JOIN_FARM {
					t.Errorf("%s: NO_RESULT_PAYLOAD is only legal for JOIN_FARM", spec.CommandType)
				}
				continue
			}
			descriptor, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(typeName))
			if err != nil || descriptor == nil {
				t.Errorf("%s: message %s not resolvable: %v", spec.CommandType, typeName, err)
			}
		}
	}
}

// TestRouteKeyExtraction 证明每条 ACTOR_AFFINITY 命令的 RouteKey 提取:
// 正常、零 ID、缺字段输入;TrustedContext 来源不得回退读取 Payload 中的
// 操作者 ID(28 §6.4 测试 4)。
func TestRouteKeyExtraction(t *testing.T) {
	const playerID = uint64(10001)
	payloadKeyCases := map[commonpb.CommandType]proto.Message{
		commonpb.CommandType_JOIN_FARM:   &publicpb.JoinFarmRequest{TargetFarmId: 20002},
		commonpb.CommandType_PLANT:       &publicpb.PlantRequest{TargetFarmId: 20002},
		commonpb.CommandType_HARVEST:     &publicpb.HarvestRequest{TargetFarmId: 20002},
		commonpb.CommandType_CARE:        &publicpb.CareRequest{TargetFarmId: 20002},
		commonpb.CommandType_THEFT:       &publicpb.TheftRequest{TargetFarmId: 20002},
		commonpb.CommandType_UNLOCK_PLOT: &publicpb.UnlockPlotRequest{TargetFarmId: 20002},
	}
	for commandType, payload := range payloadKeyCases {
		spec, ok := Lookup(commandType)
		if !ok || spec.RouteKeyExtractor == nil {
			t.Fatalf("%s: payload-key command missing extractor", commandType)
		}
		key, err := spec.RouteKeyExtractor(payload, playerID)
		if err != nil || key != 20002 {
			t.Errorf("%s: route key=%d err=%v, want 20002", commandType, key, err)
		}
		// 零 ID fail closed。
		zero := payload.ProtoReflect().New().Interface()
		if _, err := spec.RouteKeyExtractor(zero, playerID); err == nil {
			t.Errorf("%s: zero target_farm_id must fail closed", commandType)
		}
	}
	// TRUSTED_CONTEXT_FIELD:即使 Payload 伪造了 target_farm_id,RouteKey 也只
	// 能来自可信 session.player_id。
	for _, commandType := range []commonpb.CommandType{
		commonpb.CommandType_GET_PLAYER_BOOTSTRAP,
		commonpb.CommandType_BUY,
		commonpb.CommandType_SELL,
		commonpb.CommandType_CLAIM_TASK,
		commonpb.CommandType_CONFIRM_FRIEND_VISIT,
		commonpb.CommandType_GET_TASK_LIST,
		commonpb.CommandType_GET_PET_STATE,
		commonpb.CommandType_PET_FEED,
		commonpb.CommandType_ACK_COMPENDIUM_SEEN,
		commonpb.CommandType_GET_COMPENDIUM,
		commonpb.CommandType_CLAIM_MAIL_ATTACHMENT,
	} {
		spec, ok := Lookup(commandType)
		if !ok || spec.RouteKeyExtractor == nil {
			t.Fatalf("%s: session-key command missing extractor", commandType)
		}
		decoded := mustDecode(t, spec, nil)
		key, err := spec.RouteKeyExtractor(decoded, playerID)
		if err != nil || key != playerID {
			t.Errorf("%s: trusted route key=%d err=%v, want %d", commandType, key, err, playerID)
		}
		if _, err := spec.RouteKeyExtractor(decoded, 0); err == nil {
			t.Errorf("%s: zero session player must fail closed", commandType)
		}
	}
	// SERVICE_LB / NONE 不生成 Actor RouteKey。
	for _, spec := range All() {
		if spec.RouteMode != RouteModeActorAffinity && spec.RouteKeyExtractor != nil {
			t.Errorf("%s: non ACTOR_AFFINITY must not have route key extractor", spec.CommandType)
		}
	}
}

// TestAdmissionMatrixPins28 钉死 28 §6.1 的 Session 准入矩阵
// (authenticated/recovery/subscription/target policy)。
func TestAdmissionMatrixPins28(t *testing.T) {
	type admission struct {
		recovery     bool
		subscription bool
		target       TargetFarmPolicy
	}
	want := map[commonpb.CommandType]admission{
		commonpb.CommandType_JOIN_FARM:                   {false, false, TargetFarmPayloadTarget},
		commonpb.CommandType_GET_PLAYER_BOOTSTRAP:        {false, false, TargetFarmSessionPlayer},
		commonpb.CommandType_GET_FRIEND_LIST:             {false, false, TargetFarmNone},
		commonpb.CommandType_GET_NEXT_FRIEND_OPPORTUNITY: {false, false, TargetFarmNone},
		commonpb.CommandType_GET_OPERATION_RESULT:        {false, false, TargetFarmSubjectRootOperation},
		commonpb.CommandType_GET_TASK_LIST:               {true, false, TargetFarmSessionPlayer},
		commonpb.CommandType_CLAIM_TASK:                  {true, false, TargetFarmSessionPlayer},
		commonpb.CommandType_CONFIRM_FRIEND_VISIT:        {true, true, TargetFarmCurrentFriendFarm},
		commonpb.CommandType_GET_MAIL_LIST:               {false, false, TargetFarmSessionPlayer},
		commonpb.CommandType_GET_MAIL_DETAIL:             {false, false, TargetFarmSessionPlayer},
		commonpb.CommandType_MARK_MAIL_READ:              {false, false, TargetFarmSessionPlayer},
		commonpb.CommandType_DELETE_MAIL:                 {false, false, TargetFarmSessionPlayer},
		commonpb.CommandType_CLAIM_MAIL_ATTACHMENT:       {true, false, TargetFarmSessionPlayer},
		commonpb.CommandType_GET_PET_STATE:               {true, false, TargetFarmSessionPlayer},
		commonpb.CommandType_PET_UNLOCK_BY_COIN:          {true, false, TargetFarmSessionPlayer},
		commonpb.CommandType_PET_FEED:                    {true, false, TargetFarmSessionPlayer},
		commonpb.CommandType_PET_SWITCH:                  {true, false, TargetFarmSessionPlayer},
		commonpb.CommandType_GET_COMPENDIUM:              {true, false, TargetFarmSessionPlayer},
		commonpb.CommandType_ACK_COMPENDIUM_SEEN:         {true, false, TargetFarmSessionPlayer},
		commonpb.CommandType_GENERATE_SHARE_TOKEN:        {true, false, TargetFarmSessionPlayer},
		commonpb.CommandType_ROTATE_SHARE_TOKEN:          {true, false, TargetFarmSessionPlayer},
		commonpb.CommandType_REVOKE_SHARE_TOKEN:          {true, false, TargetFarmSessionPlayer},
		commonpb.CommandType_REDEEM_SHARE:                {true, false, TargetFarmTokenInviterSessionRedeemer},
		commonpb.CommandType_DELETE_FRIEND:               {true, false, TargetFarmSessionCallerPayloadPeer},
		commonpb.CommandType_BLOCK_PLAYER:                {true, false, TargetFarmSessionCallerPayloadPeer},
		commonpb.CommandType_UNBLOCK_PLAYER:              {true, false, TargetFarmSessionCallerPayloadPeer},
		commonpb.CommandType_BUY:                         {true, false, TargetFarmSessionPlayer},
		commonpb.CommandType_SELL:                        {true, false, TargetFarmSessionPlayer},
		commonpb.CommandType_UNLOCK_PLOT:                 {true, true, TargetFarmOwnCurrentFarm},
		commonpb.CommandType_PLANT:                       {true, true, TargetFarmOwnCurrentFarm},
		commonpb.CommandType_HARVEST:                     {true, true, TargetFarmOwnCurrentFarm},
		commonpb.CommandType_CARE:                        {true, true, TargetFarmCurrentFarm},
		commonpb.CommandType_THEFT:                       {true, true, TargetFarmCurrentFarm},
	}
	if len(want) != 33 {
		t.Fatalf("admission matrix rows=%d, want 33", len(want))
	}
	for commandType, expected := range want {
		spec, ok := Lookup(commandType)
		if !ok {
			t.Fatalf("%s missing", commandType)
		}
		got := spec.SessionAdmission
		if !got.RequiresAuthenticated || got.RequiresRecoveryComplete != expected.recovery ||
			got.RequiresCurrentSubscription != expected.subscription || got.TargetFarmPolicy != expected.target {
			t.Errorf("%s admission={auth=%v recovery=%v subscription=%v target=%s}, want recovery=%v subscription=%v target=%s",
				commandType, got.RequiresAuthenticated, got.RequiresRecoveryComplete,
				got.RequiresCurrentSubscription, got.TargetFarmPolicy,
				expected.recovery, expected.subscription, expected.target)
		}
	}
}

// TestRetryRecoveryMatrixPins28 钉死 28 §6.1 的 Idempotency/Retry/Recovery 矩阵。
func TestRetryRecoveryMatrixPins28(t *testing.T) {
	type reliability struct {
		idempotency IdempotencyPolicy
		retry       RetryPolicyKind
		recovery    RecoveryPolicy
		builder     bool
	}
	want := map[commonpb.CommandType]reliability{
		commonpb.CommandType_JOIN_FARM:            {IdempotencyRuntimeRequired, RetrySameOperation, RecoveryFreshJoin, true},
		commonpb.CommandType_GET_PLAYER_BOOTSTRAP: {IdempotencyNone, RetrySafeQuery, RecoveryRequery, false},
		commonpb.CommandType_GET_OPERATION_RESULT: {IdempotencyNone, RetryPollOperationResult, RecoveryRequery, false},
		commonpb.CommandType_CLAIM_TASK:           {IdempotencyRequired, RetrySameOperation, RecoveryPlayerRetrySameOperation, true},
		commonpb.CommandType_CONFIRM_FRIEND_VISIT: {IdempotencyRequired, RetrySameOperation, RecoveryFarmFreshJoinThenRetry, true},
		commonpb.CommandType_ACK_COMPENDIUM_SEEN:  {IdempotencyNone, RetrySafeMonotonic, RecoveryRequery, false},
		commonpb.CommandType_BUY:                  {IdempotencyRequired, RetrySameOperation, RecoveryPlayerRetrySameOperation, true},
		commonpb.CommandType_SELL:                 {IdempotencyRequired, RetrySameOperation, RecoveryPlayerRetrySameOperation, true},
		commonpb.CommandType_PLANT:                {IdempotencyRequired, RetrySameOperation, RecoveryFarmFreshJoinThenRetry, true},
		commonpb.CommandType_HARVEST:              {IdempotencyRequired, RetrySameOperation, RecoveryFarmFreshJoinThenRetry, true},
		commonpb.CommandType_CARE:                 {IdempotencyRequired, RetrySameOperation, RecoveryFarmFreshJoinThenRetry, true},
		commonpb.CommandType_THEFT:                {IdempotencyRequired, RetrySameOperation, RecoveryFarmFreshJoinThenRetry, true},
		commonpb.CommandType_UNLOCK_PLOT:          {IdempotencyRequired, RetrySameOperation, RecoveryFarmFreshJoinThenRetry, true},
		commonpb.CommandType_REDEEM_SHARE:         {IdempotencyRequired, RetrySameOperation, RecoveryPlayerRetrySameOperation, true},
		commonpb.CommandType_DELETE_FRIEND:        {IdempotencyRequired, RetrySameOperation, RecoveryPlayerRetrySameOperation, true},
		commonpb.CommandType_GENERATE_SHARE_TOKEN: {IdempotencyRequired, RetrySameOperation, RecoveryPlayerRetrySameOperation, true},
		commonpb.CommandType_PET_FEED:             {IdempotencyRequired, RetrySameOperation, RecoveryPlayerRetrySameOperation, true},
	}
	for commandType, expected := range want {
		spec, ok := Lookup(commandType)
		if !ok {
			t.Fatalf("%s missing", commandType)
		}
		if spec.Idempotency != expected.idempotency || spec.Retry.Kind != expected.retry ||
			spec.Recovery != expected.recovery {
			t.Errorf("%s reliability={%s %s %s}, want {%s %s %s}",
				commandType, spec.Idempotency, spec.Retry.Kind, spec.Recovery,
				expected.idempotency, expected.retry, expected.recovery)
		}
		if (spec.CanonicalPayloadBuilder != nil) != expected.builder {
			t.Errorf("%s canonical builder presence=%v, want %v",
				commandType, spec.CanonicalPayloadBuilder != nil, expected.builder)
		}
	}
	// 全部 REQUIRED/RUNTIME_REQUIRED 命令都必须有 builder(28 §6.4 测试 9)。
	for _, spec := range All() {
		if spec.Idempotency.RequiresCanonicalBuilder() && spec.CanonicalPayloadBuilder == nil {
			t.Errorf("%s: idempotency %s without canonical builder", spec.CommandType, spec.Idempotency)
		}
	}
}

// TestAvailabilitySplit 钉死本轮接线边界:已接线命令 Enabled,未接线命令
// Disabled 但合同完整注册(不得伪造业务成功)。
func TestAvailabilitySplit(t *testing.T) {
	enabled := map[commonpb.CommandType]bool{
		commonpb.CommandType_JOIN_FARM:            true,
		commonpb.CommandType_GET_PLAYER_BOOTSTRAP: true,
		commonpb.CommandType_GET_OPERATION_RESULT: true,
		commonpb.CommandType_GET_TASK_LIST:        true,
		commonpb.CommandType_CLAIM_TASK:           true,
		commonpb.CommandType_CONFIRM_FRIEND_VISIT: true,
		commonpb.CommandType_BUY:                  true,
		commonpb.CommandType_SELL:                 true,
		commonpb.CommandType_PLANT:                true,
		commonpb.CommandType_HARVEST:              true,
		commonpb.CommandType_CARE:                 true,
		commonpb.CommandType_THEFT:                true,
		commonpb.CommandType_UNLOCK_PLOT:           true,
		commonpb.CommandType_GET_COMPENDIUM:        true,
		commonpb.CommandType_ACK_COMPENDIUM_SEEN:   true,
		commonpb.CommandType_GET_MAIL_LIST:         true,
		commonpb.CommandType_GET_MAIL_DETAIL:       true,
		commonpb.CommandType_MARK_MAIL_READ:        true,
		commonpb.CommandType_DELETE_MAIL:           true,
		commonpb.CommandType_CLAIM_MAIL_ATTACHMENT: true,
		commonpb.CommandType_GET_PET_STATE:         true,
		commonpb.CommandType_PET_UNLOCK_BY_COIN:    true,
		commonpb.CommandType_PET_FEED:              true,
		commonpb.CommandType_PET_SWITCH:            true,
	}
	for _, spec := range All() {
		want := enabled[spec.CommandType]
		if spec.Availability.Enabled != want {
			t.Errorf("%s availability=%v, want %v", spec.CommandType, spec.Availability.Enabled, want)
		}
	}
}

// TestGetPlayerBootstrapAndFriendOpportunityRegistered 证明
// GET_PLAYER_BOOTSTRAP 与 GET_NEXT_FRIEND_OPPORTUNITY 均已正式登记
// (28 L2-PROTO-62/63;原始 prompt 第 3 条)。
func TestGetPlayerBootstrapAndFriendOpportunityRegistered(t *testing.T) {
	bootstrap, ok := Lookup(commonpb.CommandType_GET_PLAYER_BOOTSTRAP)
	if !ok {
		t.Fatal("GET_PLAYER_BOOTSTRAP not registered")
	}
	if bootstrap.PayloadType != "public.GetPlayerBootstrapRequest" ||
		bootstrap.ResultType != "public.GetPlayerBootstrapResult" ||
		bootstrap.RouteDomain != RouteDomainPlayer ||
		bootstrap.RouteMode != RouteModeActorAffinity ||
		bootstrap.Idempotency != IdempotencyNone ||
		bootstrap.Retry.Kind != RetrySafeQuery ||
		bootstrap.SessionAdmission.RequiresCurrentSubscription ||
		bootstrap.SessionAdmission.RequiresRecoveryComplete ||
		!bootstrap.SessionAdmission.RequiresAuthenticated ||
		!bootstrap.Availability.Enabled {
		t.Errorf("GET_PLAYER_BOOTSTRAP spec drifted: %+v", bootstrap)
	}
	opportunity, ok := Lookup(commonpb.CommandType_GET_NEXT_FRIEND_OPPORTUNITY)
	if !ok {
		t.Fatal("GET_NEXT_FRIEND_OPPORTUNITY not registered")
	}
	if opportunity.PayloadType != "public.GetNextFriendOpportunityRequest" ||
		opportunity.ResultType != "public.GetNextFriendOpportunityResult" ||
		opportunity.RouteMode != RouteModeServiceLB ||
		opportunity.Idempotency != IdempotencyNone ||
		opportunity.ExecutionAccept != ExecutionAcceptStatelessHandlerStarted {
		t.Errorf("GET_NEXT_FRIEND_OPPORTUNITY spec drifted: %+v", opportunity)
	}
}

// TestCareValidatorFailsClosedOnUnwiredIncident 证明 CARE 的非 WATER
// Incident 护理 fail closed(不得伪造业务成功)。
func TestCareValidatorFailsClosedOnUnwiredIncident(t *testing.T) {
	spec, ok := Lookup(commonpb.CommandType_CARE)
	if !ok || spec.Validator == nil {
		t.Fatal("CARE validator missing")
	}
	if err := spec.Validator(&publicpb.CareRequest{TargetFarmId: 1, CareType: publicpb.CareType_CARE_TYPE_WATER}); err != nil {
		t.Errorf("water care rejected: %v", err)
	}
	if err := spec.Validator(&publicpb.CareRequest{TargetFarmId: 1, CareType: publicpb.CareType_CARE_TYPE_PEST}); err == nil {
		t.Error("pest incident care must fail closed before wiring")
	}
	if err := spec.Validator(&publicpb.CareRequest{TargetFarmId: 1, CareType: publicpb.CareType_CARE_TYPE_WATER, IncidentId: "inc-1"}); err == nil {
		t.Error("incident_id care must fail closed before wiring")
	}
}
