package protocoltest

import (
	"encoding/hex"
	"testing"

	commonpb "farmgame/api/gen/common"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/gateway/commandcatalog"

	"google.golang.org/protobuf/proto"
)

// P1B 公网 Canonical Payload 黄金(H5 消费):同 operation + 同 payload 稳定
// 重放,同 operation + 不同 payload 稳定冲突;编码规则 = uint32_be(command_type)
// || CanonicalBytes(decodedPayload)(28 §3.5 + 29 §7.11)。
//
// 黄金原像手工推导(整数定宽大端、字符串 LP(u32be len || bytes)、无字段 tag):
//
//	PLANT(4) PlantRequest{plot_id=3, crop_definition_id=1, target_farm_id=101,
//	expected_plot_version=9, expected_crop_rule_version=2}
const (
	goldenPlantCanonicalPreimageHex = "000000040000000300000001000000000000006500000000000000090000000000000002"
	goldenPlantCanonicalHashHex     = "583aa0b435d99a54c3f771f980eb77da2b99036baf6b4ad9b6ea6539fd08d7b1"
)

func mustMarshalPayload(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	raw, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestP1BPlantCanonicalGolden 逐字节钉死 PLANT Canonical Payload 原像与
// SHA-256 Hash;H5 端按同一规则复算必须得到相同值。
func TestP1BPlantCanonicalGolden(t *testing.T) {
	cmd := &commonpb.ClientCommandEnvelope{
		CommandType: commonpb.CommandType_PLANT,
		Payload: mustMarshalPayload(t, &publicpb.PlantRequest{
			PlotId: 3, CropDefinitionId: 1, TargetFarmId: 101,
			ExpectedPlotVersion: 9, ExpectedCropRuleVersion: 2,
		}),
	}
	preimage, err := commandcatalog.CanonicalPreimage(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(preimage); got != goldenPlantCanonicalPreimageHex {
		t.Fatalf("preimage=%s want=%s", got, goldenPlantCanonicalPreimageHex)
	}
	hash, err := commandcatalog.CanonicalHash(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(hash); got != goldenPlantCanonicalHashHex {
		t.Fatalf("hash=%s want=%s", got, goldenPlantCanonicalHashHex)
	}
}

// TestP1BCanonicalGoldenFamilies 覆盖多命令的稳定重放/冲突家族:
// 每条命令同 payload 两次 Hash 一致;任一语义字段变化即冲突。
func TestP1BCanonicalGoldenFamilies(t *testing.T) {
	cases := []struct {
		name    string
		base    *commonpb.ClientCommandEnvelope
		changed *commonpb.ClientCommandEnvelope
	}{
		{
			name: "join-farm-target",
			base: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_JOIN_FARM,
				Payload: mustMarshalPayload(t, &publicpb.JoinFarmRequest{TargetFarmId: 101})},
			changed: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_JOIN_FARM,
				Payload: mustMarshalPayload(t, &publicpb.JoinFarmRequest{TargetFarmId: 102})},
		},
		{
			name: "buy-offer-count",
			base: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_BUY,
				Payload: mustMarshalPayload(t, &publicpb.BuyRequest{OfferId: 1, Count: 2, ExpectedOfferVersion: 1})},
			changed: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_BUY,
				Payload: mustMarshalPayload(t, &publicpb.BuyRequest{OfferId: 1, Count: 3, ExpectedOfferVersion: 1})},
		},
		{
			name: "harvest-cycle",
			base: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_HARVEST,
				Payload: mustMarshalPayload(t, &publicpb.HarvestRequest{PlotId: 1, TargetFarmId: 1, CropCycleId: "cc-1", ExpectedPlotVersion: 7})},
			changed: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_HARVEST,
				Payload: mustMarshalPayload(t, &publicpb.HarvestRequest{PlotId: 1, TargetFarmId: 1, CropCycleId: "cc-2", ExpectedPlotVersion: 7})},
		},
		{
			name: "confirm-friend-visit-target",
			base: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_CONFIRM_FRIEND_VISIT,
				Payload: mustMarshalPayload(t, &publicpb.ConfirmFriendVisitRequest{TargetFarmId: 20002})},
			changed: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_CONFIRM_FRIEND_VISIT,
				Payload: mustMarshalPayload(t, &publicpb.ConfirmFriendVisitRequest{TargetFarmId: 20003})},
		},
		{
			name: "pet-feed-quantity",
			base: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_PET_FEED,
				Payload: mustMarshalPayload(t, &publicpb.PetFeedRequest{Quantity: 2, ExpectedFeedRuleVersion: 1})},
			changed: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_PET_FEED,
				Payload: mustMarshalPayload(t, &publicpb.PetFeedRequest{Quantity: 3, ExpectedFeedRuleVersion: 1})},
		},
		{
			name: "rotate-reactivation-peer",
			base: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_ROTATE_SHARE_TOKEN,
				Payload: mustMarshalPayload(t, &publicpb.RotateShareTokenRequest{})},
			changed: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_ROTATE_SHARE_TOKEN,
				Payload: mustMarshalPayload(t, &publicpb.RotateShareTokenRequest{ReactivationPeerPlayerId: proto.Uint64(20002)})},
		},
		{
			name: "claim-mail-id",
			base: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_CLAIM_MAIL_ATTACHMENT,
				Payload: mustMarshalPayload(t, &publicpb.ClaimMailAttachmentRequest{MailId: "m-1"})},
			changed: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_CLAIM_MAIL_ATTACHMENT,
				Payload: mustMarshalPayload(t, &publicpb.ClaimMailAttachmentRequest{MailId: "m-2"})},
		},
		{
			name: "claim-task-instance",
			base: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_CLAIM_TASK,
				Payload: mustMarshalPayload(t, &publicpb.ClaimTaskRequest{TaskInstanceId: "task-1"})},
			changed: &commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_CLAIM_TASK,
				Payload: mustMarshalPayload(t, &publicpb.ClaimTaskRequest{TaskInstanceId: "task-2"})},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hashA, err := commandcatalog.CanonicalHash(tc.base)
			if err != nil {
				t.Fatal(err)
			}
			hashReplay, err := commandcatalog.CanonicalHash(tc.base)
			if err != nil {
				t.Fatal(err)
			}
			if string(hashA) != string(hashReplay) {
				t.Fatal("same operation + same payload must replay stable")
			}
			hashChanged, err := commandcatalog.CanonicalHash(tc.changed)
			if err != nil {
				t.Fatal(err)
			}
			if string(hashA) == string(hashChanged) {
				t.Fatal("same operation + different payload must conflict stable")
			}
		})
	}
}

// TestP1BCanonicalRejectsRawBytesDrift 证明不直接 Hash 原始 bytes:
// 同语义不同合法编码(字段顺序不同)Hash 一致(28 §3.5/L2-PROTO-13)。
func TestP1BCanonicalRejectsRawBytesDrift(t *testing.T) {
	plant := &publicpb.PlantRequest{PlotId: 1, CropDefinitionId: 2, TargetFarmId: 3}
	ordered := mustMarshalPayload(t, plant)
	// 手工逆序编码同样三个 varint 字段。
	reversed := []byte{0x18, 0x03, 0x10, 0x02, 0x08, 0x01}
	if string(ordered) == string(reversed) {
		t.Fatal("premise broken")
	}
	hashA, err := commandcatalog.CanonicalHash(&commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_PLANT, Payload: ordered})
	if err != nil {
		t.Fatal(err)
	}
	hashB, err := commandcatalog.CanonicalHash(&commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_PLANT, Payload: reversed})
	if err != nil {
		t.Fatal(err)
	}
	if string(hashA) != string(hashB) {
		t.Fatal("semantically equal encodings must share hash (no raw-bytes hashing)")
	}
}
