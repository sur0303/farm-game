package commandcatalog

import (
	"testing"

	commonpb "farmgame/api/gen/common"
	publicpb "farmgame/api/gen/public"

	"google.golang.org/protobuf/proto"
)

// TestEveryCommandDecodesEmptyAndValidPayload 证明 33 条命令的 Decoder 都能
// 解码空 Payload(空消息)与合法 Payload;decode 失败路径由畸形输入覆盖
// (28 §6.4 测试 3/5)。
func TestEveryCommandDecodesEmptyAndValidPayload(t *testing.T) {
	validPayloads := map[commonpb.CommandType]proto.Message{
		commonpb.CommandType_JOIN_FARM:                   &publicpb.JoinFarmRequest{TargetFarmId: 1},
		commonpb.CommandType_GET_PLAYER_BOOTSTRAP:        &publicpb.GetPlayerBootstrapRequest{},
		commonpb.CommandType_GET_FRIEND_LIST:             &publicpb.GetFriendListRequest{},
		commonpb.CommandType_GET_NEXT_FRIEND_OPPORTUNITY: &publicpb.GetNextFriendOpportunityRequest{},
		commonpb.CommandType_GET_OPERATION_RESULT:        &publicpb.GetOperationResultRequest{OriginalCommandType: commonpb.CommandType_PLANT, OriginalOperationId: "op-1"},
		commonpb.CommandType_GET_TASK_LIST:               &publicpb.GetTaskListRequest{},
		commonpb.CommandType_CLAIM_TASK:                  &publicpb.ClaimTaskRequest{TaskInstanceId: "task-1"},
		commonpb.CommandType_CONFIRM_FRIEND_VISIT:        &publicpb.ConfirmFriendVisitRequest{TargetFarmId: 2},
		commonpb.CommandType_GET_MAIL_LIST:               &publicpb.GetMailListRequest{},
		commonpb.CommandType_GET_MAIL_DETAIL:             &publicpb.GetMailDetailRequest{MailId: "m-1"},
		commonpb.CommandType_MARK_MAIL_READ:              &publicpb.MarkMailReadRequest{MailId: "m-1"},
		commonpb.CommandType_DELETE_MAIL:                 &publicpb.DeleteMailRequest{MailId: "m-1"},
		commonpb.CommandType_CLAIM_MAIL_ATTACHMENT:       &publicpb.ClaimMailAttachmentRequest{MailId: "m-1"},
		commonpb.CommandType_GET_PET_STATE:               &publicpb.GetPetStateRequest{},
		commonpb.CommandType_PET_UNLOCK_BY_COIN:          &publicpb.PetUnlockByCoinRequest{PetId: 1},
		commonpb.CommandType_PET_FEED:                    &publicpb.PetFeedRequest{Quantity: 1},
		commonpb.CommandType_PET_SWITCH:                  &publicpb.PetSwitchRequest{TargetPetId: 1},
		commonpb.CommandType_GET_COMPENDIUM:              &publicpb.GetCompendiumRequest{},
		commonpb.CommandType_ACK_COMPENDIUM_SEEN:         &publicpb.AckCompendiumSeenRequest{ObservedRevision: 3},
		commonpb.CommandType_GENERATE_SHARE_TOKEN:        &publicpb.GenerateShareTokenRequest{},
		commonpb.CommandType_ROTATE_SHARE_TOKEN:          &publicpb.RotateShareTokenRequest{},
		commonpb.CommandType_REVOKE_SHARE_TOKEN:          &publicpb.RevokeShareTokenRequest{},
		commonpb.CommandType_REDEEM_SHARE:                &publicpb.RedeemShareRequest{OpaqueShareToken: "token"},
		commonpb.CommandType_DELETE_FRIEND:               &publicpb.DeleteFriendRequest{PeerPlayerId: 2},
		commonpb.CommandType_BLOCK_PLAYER:                &publicpb.BlockPlayerRequest{PeerPlayerId: 2},
		commonpb.CommandType_UNBLOCK_PLAYER:              &publicpb.UnblockPlayerRequest{PeerPlayerId: 2},
		commonpb.CommandType_BUY:                         &publicpb.BuyRequest{OfferId: 1, Count: 1, ExpectedOfferVersion: 1},
		commonpb.CommandType_SELL:                        &publicpb.SellRequest{ItemId: 1, Count: 1},
		commonpb.CommandType_UNLOCK_PLOT:                 &publicpb.UnlockPlotRequest{TargetFarmId: 1, PlotId: 2},
		commonpb.CommandType_PLANT:                       &publicpb.PlantRequest{TargetFarmId: 1, PlotId: 2, CropDefinitionId: 3},
		commonpb.CommandType_HARVEST:                     &publicpb.HarvestRequest{TargetFarmId: 1, PlotId: 2},
		commonpb.CommandType_CARE:                        &publicpb.CareRequest{TargetFarmId: 1, PlotId: 2, CareType: publicpb.CareType_CARE_TYPE_WATER},
		commonpb.CommandType_THEFT:                       &publicpb.TheftRequest{TargetFarmId: 1, PlotId: 2},
	}
	if len(validPayloads) != 33 {
		t.Fatalf("valid payload cases=%d, want 33", len(validPayloads))
	}
	for _, spec := range All() {
		want, ok := validPayloads[spec.CommandType]
		if !ok {
			t.Errorf("%s: missing valid payload case", spec.CommandType)
			continue
		}
		decoded, err := spec.Decoder(nil)
		if err != nil || decoded == nil {
			t.Errorf("%s: empty payload decode failed: %v", spec.CommandType, err)
			continue
		}
		if decoded.ProtoReflect().Descriptor().FullName() != want.ProtoReflect().Descriptor().FullName() {
			t.Errorf("%s: decoder produced %s, want %s",
				spec.CommandType, decoded.ProtoReflect().Descriptor().FullName(),
				want.ProtoReflect().Descriptor().FullName())
		}
		raw, err := proto.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err = spec.Decoder(raw)
		if err != nil || decoded == nil {
			t.Errorf("%s: valid payload decode failed: %v", spec.CommandType, err)
		}
	}
}

// TestMalformedPayloadRejected 证明 command_type + malformed payload 在
// Decoder 层 fail closed,不进入后端(28 §6.4 测试 5)。
func TestMalformedPayloadRejected(t *testing.T) {
	malformed := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02}
	for _, spec := range All() {
		if _, err := spec.Decoder(malformed); err == nil {
			t.Errorf("%s: malformed payload accepted", spec.CommandType)
		}
	}
}

// TestPayloadExceedsLimitBoundaries 证明 payload 上限边界:恰好上限放行、
// 超一字节拒绝、Unlimited 永不拒绝(28 §6.4 测试 6)。
func TestPayloadExceedsLimitBoundaries(t *testing.T) {
	if PayloadExceedsLimit(8, 8) {
		t.Error("payload at limit must pass")
	}
	if !PayloadExceedsLimit(8, 9) {
		t.Error("payload over limit must be rejected")
	}
	if PayloadExceedsLimit(MaxPayloadBytesUnlimited, 1<<30) {
		t.Error("unlimited must never reject")
	}
	for _, spec := range All() {
		if !PayloadExceedsLimit(spec.MaxPayloadBytes, spec.MaxPayloadBytes+1) {
			t.Errorf("%s: limit+1 must be rejected", spec.CommandType)
		}
		if PayloadExceedsLimit(spec.MaxPayloadBytes, spec.MaxPayloadBytes) {
			t.Errorf("%s: at-limit must pass", spec.CommandType)
		}
	}
}

// TestDecoderRejectsTruncatedVarint 覆盖另一类畸形输入(截断的 varint)。
func TestDecoderRejectsTruncatedVarint(t *testing.T) {
	spec, ok := Lookup(commonpb.CommandType_PLANT)
	if !ok {
		t.Fatal("PLANT missing")
	}
	if _, err := spec.Decoder([]byte{0x08}); err == nil {
		t.Error("truncated varint accepted")
	}
	if _, err := spec.Decoder([]byte{0x0a, 0x05, 0x01}); err == nil {
		t.Error("truncated length-delimited field accepted")
	}
}

// TestDecoderUnknownFieldsPreservedButHashFailsClosed 证明演进边界:
// 合法未知字段(滚动窗口内的新字段)不导致 Gate 解码失败、不影响路由键;
// 但 Canonical Hash 对未知字段 fail closed(29 §7.11 纪律:Hash 只覆盖
// 已声明的语义字段集合,未知字段意味着客户端/服务端 Schema 偏移,
// 幂等边界必须严格)。
func TestDecoderUnknownFieldsPreservedButHashFailsClosed(t *testing.T) {
	spec, ok := Lookup(commonpb.CommandType_PLANT)
	if !ok {
		t.Fatal("PLANT missing")
	}
	base, err := proto.Marshal(&publicpb.PlantRequest{TargetFarmId: 7, PlotId: 1, CropDefinitionId: 2})
	if err != nil {
		t.Fatal(err)
	}
	// 追加未知 field 100(LEN,空值)。
	raw := append(append([]byte(nil), base...), 0xA2, 0x06, 0x00)
	decoded, err := spec.Decoder(raw)
	if err != nil {
		t.Fatalf("unknown future field must stay decodable: %v", err)
	}
	key, err := spec.RouteKeyExtractor(decoded, 0)
	if err != nil || key != 7 {
		t.Fatalf("route key with unknown field=%d err=%v, want 7", key, err)
	}
	if _, err := CanonicalHash(&commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_PLANT, Payload: raw}); err == nil {
		t.Fatal("canonical hash must fail closed on unknown fields")
	}
	if _, err := CanonicalHash(&commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_PLANT, Payload: base}); err != nil {
		t.Fatalf("canonical hash on declared fields: %v", err)
	}
}
