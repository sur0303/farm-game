package commandcatalog

import (
	"errors"
	"fmt"

	publicpb "farmgame/api/gen/public"

	"google.golang.org/protobuf/proto"
)

// RouteKeyExtractor 实现(28 §6.1 RouteKeySource/RouteKeyExtractor 列;
// §6.4 测试 4:正常、缺字段、零 ID 与超范围输入均须有明确行为)。

var errRouteKeyUnavailable = errors.New("commandcatalog: route key unavailable")

// routeKeyFromSession 是 TRUSTED_CONTEXT_FIELD 来源的唯一提取器:只使用
// TrustedCommandContext.session.player_id,不得回退读取 Payload 中的操作者
// ID(L2-PROTO-39)。
func routeKeyFromSession(_ proto.Message, sessionPlayerID uint64) (uint64, error) {
	if sessionPlayerID == 0 {
		return 0, fmt.Errorf("%w: trusted session player_id is zero", errRouteKeyUnavailable)
	}
	return sessionPlayerID, nil
}

// routeKeyFromPayloadField 提取 Typed Payload 的 target_farm_id 字段。
// 零 ID 与类型不符均 fail closed。
func routeKeyFromPayloadField(decoded proto.Message, _ uint64) (uint64, error) {
	var target uint64
	switch request := decoded.(type) {
	case *publicpb.JoinFarmRequest:
		target = request.GetTargetFarmId()
	case *publicpb.UnlockPlotRequest:
		target = request.GetTargetFarmId()
	case *publicpb.PlantRequest:
		target = request.GetTargetFarmId()
	case *publicpb.HarvestRequest:
		target = request.GetTargetFarmId()
	case *publicpb.CareRequest:
		target = request.GetTargetFarmId()
	case *publicpb.TheftRequest:
		target = request.GetTargetFarmId()
	default:
		return 0, fmt.Errorf("%w: payload %T has no target_farm_id", errRouteKeyUnavailable, decoded)
	}
	if target == 0 {
		return 0, fmt.Errorf("%w: payload target_farm_id is zero", errRouteKeyUnavailable)
	}
	return target, nil
}

// TargetFarmIDFromPayload 提取 Typed Payload 中的目标农场,用于 Gate 的
// TargetFarmPolicy 准入校验(OWN_CURRENT_FARM / CURRENT_FARM /
// CURRENT_FRIEND_FARM / PAYLOAD_TARGET)。它不是 Actor RouteKey:
// CONFIRM_FRIEND_VISIT 的 RouteKey 是 session.player_id,但其准入目标仍来自
// Payload(28 §5.6)。
func TargetFarmIDFromPayload(decoded proto.Message) (uint64, bool) {
	switch request := decoded.(type) {
	case *publicpb.JoinFarmRequest:
		return request.GetTargetFarmId(), true
	case *publicpb.UnlockPlotRequest:
		return request.GetTargetFarmId(), true
	case *publicpb.PlantRequest:
		return request.GetTargetFarmId(), true
	case *publicpb.HarvestRequest:
		return request.GetTargetFarmId(), true
	case *publicpb.CareRequest:
		return request.GetTargetFarmId(), true
	case *publicpb.TheftRequest:
		return request.GetTargetFarmId(), true
	case *publicpb.ConfirmFriendVisitRequest:
		return request.GetTargetFarmId(), true
	default:
		return 0, false
	}
}
