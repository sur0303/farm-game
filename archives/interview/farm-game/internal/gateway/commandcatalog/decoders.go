package commandcatalog

import (
	"errors"
	"fmt"

	commonpb "farmgame/api/gen/common"
	publicpb "farmgame/api/gen/public"

	"google.golang.org/protobuf/proto"
)

// ErrFeatureDisabled 标记"合同已注册但业务 Handler 尚未接线"的功能切片。
// CARE 的两个冻结 Incident 类型已经进入 FarmActor authority path；旧
// WATER 请求不带 incident_id 的形状仍保留为兼容窗口，由 ingress 补齐
// P0 默认 Incident 身份。
var ErrFeatureDisabled = errors.New("commandcatalog: feature slice not wired")

// PayloadExceedsLimit reports whether the payload exceeds the per-command cap.
// MaxPayloadBytesUnlimited disables the check; zero or negative-other caps are
// rejected at registration time.
func PayloadExceedsLimit(maxPayloadBytes int, payloadBytes int) bool {
	if maxPayloadBytes == MaxPayloadBytesUnlimited {
		return false
	}
	return payloadBytes > maxPayloadBytes
}

// decodeInto returns a PayloadDecoder that always unmarshals into a fresh
// message of the given type. Empty payloads still decode once so malformed
// bytes fail closed at Gate (28 §3.4/§6.4 测试 5).
func decodeInto(template proto.Message) PayloadDecoder {
	return func(payload []byte) (proto.Message, error) {
		msg := template.ProtoReflect().New().Interface()
		if err := proto.Unmarshal(payload, msg); err != nil {
			return nil, fmt.Errorf("decode %s: %w", template.ProtoReflect().Descriptor().FullName(), err)
		}
		return msg, nil
	}
}

// payloadDecoders 是 33 类公开命令的唯一 Decoder 注册表(28 §6.4:
// 不存在缺失 Decoder 的公开命令)。以变量初始化器登记,保证 catalog
// specIndex 初始化前就绪。
var payloadDecoders = func() map[commonpb.CommandType]PayloadDecoder {
	registry := make(map[commonpb.CommandType]PayloadDecoder, 33)
	register := func(commandType commonpb.CommandType, template proto.Message) {
		registry[commandType] = decodeInto(template)
	}
	register(commonpb.CommandType_JOIN_FARM, &publicpb.JoinFarmRequest{})
	register(commonpb.CommandType_GET_PLAYER_BOOTSTRAP, &publicpb.GetPlayerBootstrapRequest{})
	register(commonpb.CommandType_GET_FRIEND_LIST, &publicpb.GetFriendListRequest{})
	register(commonpb.CommandType_GET_NEXT_FRIEND_OPPORTUNITY, &publicpb.GetNextFriendOpportunityRequest{})
	register(commonpb.CommandType_GET_OPERATION_RESULT, &publicpb.GetOperationResultRequest{})
	register(commonpb.CommandType_GET_TASK_LIST, &publicpb.GetTaskListRequest{})
	register(commonpb.CommandType_CLAIM_TASK, &publicpb.ClaimTaskRequest{})
	register(commonpb.CommandType_CONFIRM_FRIEND_VISIT, &publicpb.ConfirmFriendVisitRequest{})
	register(commonpb.CommandType_GET_MAIL_LIST, &publicpb.GetMailListRequest{})
	register(commonpb.CommandType_GET_MAIL_DETAIL, &publicpb.GetMailDetailRequest{})
	register(commonpb.CommandType_MARK_MAIL_READ, &publicpb.MarkMailReadRequest{})
	register(commonpb.CommandType_DELETE_MAIL, &publicpb.DeleteMailRequest{})
	register(commonpb.CommandType_CLAIM_MAIL_ATTACHMENT, &publicpb.ClaimMailAttachmentRequest{})
	register(commonpb.CommandType_GET_PET_STATE, &publicpb.GetPetStateRequest{})
	register(commonpb.CommandType_PET_UNLOCK_BY_COIN, &publicpb.PetUnlockByCoinRequest{})
	register(commonpb.CommandType_PET_FEED, &publicpb.PetFeedRequest{})
	register(commonpb.CommandType_PET_SWITCH, &publicpb.PetSwitchRequest{})
	register(commonpb.CommandType_GET_COMPENDIUM, &publicpb.GetCompendiumRequest{})
	register(commonpb.CommandType_ACK_COMPENDIUM_SEEN, &publicpb.AckCompendiumSeenRequest{})
	register(commonpb.CommandType_GENERATE_SHARE_TOKEN, &publicpb.GenerateShareTokenRequest{})
	register(commonpb.CommandType_ROTATE_SHARE_TOKEN, &publicpb.RotateShareTokenRequest{})
	register(commonpb.CommandType_REVOKE_SHARE_TOKEN, &publicpb.RevokeShareTokenRequest{})
	register(commonpb.CommandType_REDEEM_SHARE, &publicpb.RedeemShareRequest{})
	register(commonpb.CommandType_DELETE_FRIEND, &publicpb.DeleteFriendRequest{})
	register(commonpb.CommandType_BLOCK_PLAYER, &publicpb.BlockPlayerRequest{})
	register(commonpb.CommandType_UNBLOCK_PLAYER, &publicpb.UnblockPlayerRequest{})
	register(commonpb.CommandType_BUY, &publicpb.BuyRequest{})
	register(commonpb.CommandType_SELL, &publicpb.SellRequest{})
	register(commonpb.CommandType_UNLOCK_PLOT, &publicpb.UnlockPlotRequest{})
	register(commonpb.CommandType_PLANT, &publicpb.PlantRequest{})
	register(commonpb.CommandType_HARVEST, &publicpb.HarvestRequest{})
	register(commonpb.CommandType_CARE, &publicpb.CareRequest{})
	register(commonpb.CommandType_THEFT, &publicpb.TheftRequest{})
	return registry
}()

// decoderFor returns the registered decoder for a command type.
func decoderFor(commandType commonpb.CommandType) PayloadDecoder {
	return payloadDecoders[commandType]
}

// validateCareIncident is CARE's structural boundary. Public proto keeps the
// frozen compatibility enum name CARE_TYPE_PEST; the Farm adapter maps its
// value to contracts.CARE_TYPE_REMOVE_PEST without changing Proto.
func validateCareIncident(decoded proto.Message) error {
	request, ok := decoded.(*publicpb.CareRequest)
	if !ok {
		return errors.New("commandcatalog: care payload type mismatch")
	}
	switch request.GetCareType() {
	case publicpb.CareType_CARE_TYPE_WATER:
		if request.GetIncidentId() != "" && request.GetCropCycleId() == "" {
			return fmt.Errorf("%w: CARE incident requires crop_cycle_id", ErrFeatureDisabled)
		}
		return nil
	case publicpb.CareType_CARE_TYPE_PEST:
		if request.GetIncidentId() == "" || request.GetCropCycleId() == "" {
			return fmt.Errorf("%w: CARE_TYPE_PEST requires plot, crop_cycle and incident", ErrFeatureDisabled)
		}
		return nil
	default:
		return fmt.Errorf("%w: CARE type is not wired", ErrFeatureDisabled)
	}
}
