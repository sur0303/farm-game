// Package trusted is the FirstHop independent re-validation boundary
// (contract 28 §3.6/L2-PROTO-14, 29 §5).
//
// FirstHop 不信任客户端提交的任何身份/目标字段,也不信任 Gate 已解码的 Typed
// Payload:它从原始 payload bytes 按 CommandSpec 重新解码,复核
// TrustedCommandContext 形状(29 §3.4)、按 RouteKeyExtractor 重算 RouteKey 并与
// Gate 生成的 DispatchContext 比对(29 §4/L2-PROTO-23),最后生成唯一权威
// CanonicalPayloadHash(28 §3.5/L2-PROTO-13)。任何一步失败都 fail closed,
// 不得把请求投给"猜测正确"的 Actor。
//
// 合同 29 §3.5:FirstHop 信任的是内部 RPC 连接的受信服务身份,上下文本身只做
// 结构与一致性校验,不做逐命令签名验证。
package trusted

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	"farmgame/internal/gateway/commandcatalog"

	"google.golang.org/protobuf/proto"
)

const MaxRequestIDBytes = 128

// ErrCommandDisabled 标记"合同已注册但业务 Handler 尚未接线"的命令
// (Availability.Enabled=false 或 StructuralValidator 报告的未接线功能切片)。
// FirstHop 必须以 ERR_COMMAND_UNIMPLEMENTED 明确拒绝,不得伪造业务成功。
var ErrCommandDisabled = errors.New("trusted: command is feature-disabled")

// ErrPayloadInvalid 标记 typed payload 在业务接受前 fail closed:超限、畸形、
// 错型、未知字段、结构校验失败或缺失必需 operation_id。
var ErrPayloadInvalid = errors.New("trusted: typed payload invalid")

// VerifiedCommand 是 FirstHop 复核的唯一产出:已解码 Typed Payload、重算的
// RouteKey 与唯一权威 CanonicalPayloadHash。调用方不得再解码 raw payload,
// 也不得从旧 envelope 兼容字段取目标/版本。
type VerifiedCommand struct {
	Spec    commandcatalog.CommandSpec
	Decoded proto.Message
	// RouteKey 仅在 Spec.RouteMode==ACTOR_AFFINITY 时有效(已与
	// DispatchContext.ActorRoute.ActorId 比对一致)。
	RouteKey uint64
	// PayloadHash 是 28 §3.5 的 CanonicalPayloadHash;仅当命令注册了
	// CanonicalPayloadBuilder(REQUIRED/RUNTIME_REQUIRED)时非空。
	PayloadHash []byte
}

// ValidateCommand 按合同 29 §5 的复核顺序校验一条 Gate 转发的命令:
//
//	查 CommandSpec → Availability → operation_id 策略 → payload 大小 →
//	独立解码 raw payload → StructuralValidator → TrustedContext 形状(29 §3.4)→
//	重算 RouteKey 并复核 DispatchContext → 生成唯一权威 CanonicalPayloadHash。
//
// 返回错误可按 errors.Is 分类:ErrCommandDisabled/ErrPayloadInvalid 是业务接受前
// 的明确拒绝;其余错误是可信上下文/路由结构违规,调用方应以 RPC 层错误 fail closed。
func ValidateCommand(cmd *commonpb.ClientCommandEnvelope, ctx *logicrpcpb.TrustedCommandContext, dispatch *logicrpcpb.DispatchContext) (*VerifiedCommand, error) {
	if cmd == nil || ctx == nil {
		return nil, errors.New("command and trusted context required")
	}
	sess := ctx.Session
	if sess == nil {
		return nil, errors.New("trusted session snapshot required")
	}
	if sess.PlayerId == 0 {
		return nil, errors.New("session.player_id required")
	}
	if strings.TrimSpace(cmd.RequestId) == "" || len(cmd.RequestId) > MaxRequestIDBytes {
		return nil, errors.New("request_id missing or too long")
	}
	if cmd.CommandType == commonpb.CommandType_COMMAND_TYPE_UNSPECIFIED {
		return nil, errors.New("command_type required")
	}

	spec, ok := commandcatalog.Lookup(cmd.CommandType)
	if !ok {
		return nil, fmt.Errorf("unregistered command type %s", cmd.CommandType)
	}
	// 28 §6.1 AvailabilityPolicy:已注册但尚未接线的命令在业务接受前明确拒绝。
	if !spec.Availability.Enabled {
		return nil, fmt.Errorf("%w: %s", ErrCommandDisabled, cmd.CommandType)
	}
	if spec.Idempotency.RequiresOperationID() && strings.TrimSpace(cmd.OperationId) == "" {
		return nil, fmt.Errorf("%w: operation_id required for %s", ErrPayloadInvalid, cmd.CommandType)
	}
	if commandcatalog.PayloadExceedsLimit(spec.MaxPayloadBytes, len(cmd.Payload)) {
		return nil, fmt.Errorf("%w: %s payload exceeds %d bytes", ErrPayloadInvalid, cmd.CommandType, spec.MaxPayloadBytes)
	}
	// 28 §3.6:FirstHop 对原始 typed payload 独立解码;Gate 的解码结果不进入本边界。
	decoded, err := spec.Decoder(cmd.Payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %s decode: %v", ErrPayloadInvalid, cmd.CommandType, err)
	}
	if spec.Validator != nil {
		if validateErr := spec.Validator(decoded); validateErr != nil {
			if errors.Is(validateErr, commandcatalog.ErrFeatureDisabled) {
				return nil, fmt.Errorf("%w: %s: %v", ErrCommandDisabled, cmd.CommandType, validateErr)
			}
			return nil, fmt.Errorf("%w: %s validate: %v", ErrPayloadInvalid, cmd.CommandType, validateErr)
		}
	}
	if spec.SessionAdmission.RequiresRecoveryComplete && !ctx.RecoveryCompleteAtAdmission {
		return nil, errors.New("recovery_complete_at_admission required for command")
	}
	if err := validateTrustedShape(cmd.CommandType, spec, decoded, ctx, sess.PlayerId); err != nil {
		return nil, err
	}
	verified := &VerifiedCommand{Spec: spec, Decoded: decoded}
	if err := verifyDispatch(spec, decoded, sess.PlayerId, dispatch, verified); err != nil {
		return nil, err
	}
	// 唯一权威 payload hash:command_type + CanonicalPayloadBuilder(typed payload);
	// 禁止 hash 原始 bytes/重 marshal 信封/传输噪声(28 §3.5,L2-PROTO-13)。
	if spec.CanonicalPayloadBuilder != nil {
		preimage, err := spec.CanonicalPayloadBuilder(decoded)
		if err != nil {
			return nil, fmt.Errorf("%w: %s canonical payload: %v", ErrPayloadInvalid, cmd.CommandType, err)
		}
		sum := sha256.Sum256(preimage)
		verified.PayloadHash = sum[:]
	}
	return verified, nil
}

// validateTrustedShape 执行 29 §3.4 的互斥形状校验与 28 §6.1 的完整
// TargetFarmPolicy 复核:JOIN_FARM 携带 join_attempt 且 farm_context 不存在,
// join_attempt.target_farm_id 与 Typed Payload 目标一致;要求当前
// Subscription 的命令携带 farm_context 且 join_attempt 不存在;其余命令两者
// 都不得携带。目标策略在 FirstHop 独立复核,fail closed:
//
//	PAYLOAD_TARGET:          typed target 存在且非 0(目前仅 JOIN_FARM)
//	SESSION_PLAYER:          操作者只来自 session.player_id,payload/上下文不得改向他人
//	OWN_CURRENT_FARM:        target == admitted_farm_id == session.player_id
//	CURRENT_FARM:            target == admitted_farm_id
//	CURRENT_FRIEND_FARM:     target == admitted_farm_id 且 target != session.player_id
//
// 其他 policy 当前全部对应 disabled 命令(Availability 已先行拒绝);若某个
// 未覆盖的 policy 出现在 enabled 命令上,必须 fail closed 而不是默认放行。
func validateTrustedShape(commandType commonpb.CommandType, spec commandcatalog.CommandSpec, decoded proto.Message, ctx *logicrpcpb.TrustedCommandContext, sessionPlayerID uint64) error {
	if commandType == commonpb.CommandType_JOIN_FARM {
		if ctx.JoinAttempt == nil {
			return errors.New("join_attempt required for JOIN_FARM")
		}
		if ctx.FarmContext != nil {
			return errors.New("farm_context must be absent for JOIN_FARM")
		}
		if strings.TrimSpace(ctx.JoinAttempt.JoinAttemptId) == "" {
			return errors.New("join_attempt.join_attempt_id required")
		}
		target, hasTarget := commandcatalog.TargetFarmIDFromPayload(decoded)
		if !hasTarget || target == 0 {
			return fmt.Errorf("%w: JOIN_FARM payload target_farm_id required", ErrPayloadInvalid)
		}
		if ctx.JoinAttempt.TargetFarmId != target {
			return errors.New("join_attempt.target_farm_id mismatch")
		}
		return nil
	}
	if ctx.JoinAttempt != nil {
		return errors.New("join_attempt must be absent for non-join command")
	}
	if spec.SessionAdmission.RequiresCurrentSubscription {
		if err := validateCurrentSubscriptionFence(ctx, ctx.Session); err != nil {
			return err
		}
	} else if ctx.FarmContext != nil {
		return errors.New("farm_context must be absent for command without current subscription")
	}
	return validateTargetFarmPolicy(spec, decoded, ctx, sessionPlayerID)
}

// validateCurrentSubscriptionFence is the FirstHop complete current-subscription
// fence for CARE/THEFT and other RequiresCurrentSubscription commands
// (26/29): farm_id, non-empty subscription_id, and the full SessionPermit
// generation/connection identity must all be present so a switched or forged
// context cannot enter the Actor turn.
func validateCurrentSubscriptionFence(ctx *logicrpcpb.TrustedCommandContext, sess *logicrpcpb.SessionPermitSnapshot) error {
	if ctx.FarmContext == nil {
		return errors.New("farm_context required for farm subscription command")
	}
	if ctx.FarmContext.AdmittedFarmId == 0 {
		return errors.New("farm_context.admitted_farm_id required")
	}
	if strings.TrimSpace(ctx.FarmContext.AdmittedSubscriptionId) == "" {
		return errors.New("farm_context.admitted_subscription_id required")
	}
	if sess == nil || sess.PlayerId == 0 {
		return errors.New("session.player_id required")
	}
	if sess.ActiveSessionEpoch == 0 {
		return errors.New("session.active_session_epoch required")
	}
	if strings.TrimSpace(sess.GatewayId) == "" {
		return errors.New("session.gateway_id required")
	}
	if strings.TrimSpace(sess.GatewayBootId) == "" {
		return errors.New("session.gateway_boot_id required")
	}
	if strings.TrimSpace(sess.ConnectionId) == "" {
		return errors.New("session.connection_id required")
	}
	if sess.ConnectionGeneration == 0 {
		return errors.New("session.connection_generation required")
	}
	if sess.SessionRouteRevision == 0 {
		return errors.New("session.session_route_revision required")
	}
	return nil
}

// validateTargetFarmPolicy 是 28 §6.1 Session 准入目标策略的 FirstHop 独立
// 复核(主集成审查 P1 修复):不再只做"target==admitted"的弱检查。
func validateTargetFarmPolicy(spec commandcatalog.CommandSpec, decoded proto.Message, ctx *logicrpcpb.TrustedCommandContext, sessionPlayerID uint64) error {
	target, hasTarget := commandcatalog.TargetFarmIDFromPayload(decoded)
	admitted := ctx.FarmContext.GetAdmittedFarmId()
	switch spec.SessionAdmission.TargetFarmPolicy {
	case commandcatalog.TargetFarmPayloadTarget:
		// 目标只来自 Typed Payload(当前仅 JOIN_FARM,已在上方专项复核);
		// 若未来出现同策略的非 Join 命令,仍要求显式非零目标。
		if !hasTarget || target == 0 {
			return fmt.Errorf("%w: %s payload target_farm_id required", ErrPayloadInvalid, spec.CommandType)
		}
		return nil
	case commandcatalog.TargetFarmSessionPlayer:
		// 操作者/路由主体只来自 session.player_id(BUY/SELL 等);
		// farm_context 已在上方排除,actor route 与 session 的一致性由
		// verifyDispatch 复核,这里无 payload 目标可校验。
		return nil
	case commandcatalog.TargetFarmOwnCurrentFarm:
		if !hasTarget || target == 0 {
			return fmt.Errorf("%w: %s payload target_farm_id required", ErrPayloadInvalid, spec.CommandType)
		}
		if target != admitted || target != sessionPlayerID {
			return errors.New("own-current-farm target mismatch")
		}
		return nil
	case commandcatalog.TargetFarmCurrentFarm:
		if !hasTarget || target == 0 {
			return fmt.Errorf("%w: %s payload target_farm_id required", ErrPayloadInvalid, spec.CommandType)
		}
		if target != admitted {
			return errors.New("current-farm target mismatch")
		}
		return nil
	case commandcatalog.TargetFarmCurrentFriendFarm:
		if !hasTarget || target == 0 {
			return fmt.Errorf("%w: %s payload target_farm_id required", ErrPayloadInvalid, spec.CommandType)
		}
		if target != admitted || target == sessionPlayerID {
			return errors.New("current-friend-farm target mismatch")
		}
		return nil
	case commandcatalog.TargetFarmSubjectRootOperation:
		// GET_OPERATION_RESULT is a stateless read. Its original command,
		// operation ID and root locator are validated by the operation-result
		// reader using the trusted session subject; there is deliberately no
		// current Farm subscription to compare here.
		return nil
	default:
		// TOKEN_INVITER_SESSION_REDEEMER /
		// SESSION_CALLER_PAYLOAD_PEER / NONE 等当前仅存在于 disabled 命令;
		// enabled 命令出现未覆盖策略时 fail closed。
		return fmt.Errorf("trusted: target farm policy %s not enforceable for enabled %s",
			spec.SessionAdmission.TargetFarmPolicy, spec.CommandType)
	}
}

// verifyDispatch 按 RouteMode 复核 Gate 生成的 DispatchContext(29 §4.0/§5 步骤
// 7~9,L2-PROTO-23):ACTOR_AFFINITY 必须携带与本端重算 RouteKey 一致的
// ActorRouteHeader;SERVICE_LB/NONE 不得伪造 Actor Route。Gate 路由与本端重算
// 不一致时 fail closed。
func verifyDispatch(spec commandcatalog.CommandSpec, decoded proto.Message, sessionPlayerID uint64, dispatch *logicrpcpb.DispatchContext, verified *VerifiedCommand) error {
	switch spec.RouteMode {
	case commandcatalog.RouteModeActorAffinity:
		routeKey, err := spec.RouteKeyExtractor(decoded, sessionPlayerID)
		if err != nil {
			return fmt.Errorf("route key recompute: %w", err)
		}
		route := dispatch.GetActorRoute()
		if dispatch.GetRouteMode() != logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY || route == nil {
			return errors.New("ACTOR_AFFINITY command requires actor route header")
		}
		wantDomain := logicrpcpb.RouteDomain_ROUTE_DOMAIN_UNSPECIFIED
		switch spec.RouteDomain {
		case commandcatalog.RouteDomainPlayer:
			wantDomain = logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER
		case commandcatalog.RouteDomainFarm:
			wantDomain = logicrpcpb.RouteDomain_ROUTE_DOMAIN_FARM
		}
		if route.GetRouteDomain() != wantDomain {
			return errors.New("actor route domain mismatch")
		}
		if route.GetActorId() != routeKey {
			return errors.New("actor route key mismatch")
		}
		verified.RouteKey = routeKey
		return nil
	case commandcatalog.RouteModeServiceLB:
		if dispatch.GetRouteMode() != logicrpcpb.RouteMode_ROUTE_MODE_SERVICE_LB || dispatch.GetActorRoute() != nil {
			return errors.New("SERVICE_LB command must not carry actor route")
		}
		return nil
	default:
		if dispatch.GetRouteMode() != logicrpcpb.RouteMode_ROUTE_MODE_NONE || dispatch.GetActorRoute() != nil {
			return errors.New("NONE route command must not carry actor route")
		}
		return nil
	}
}
