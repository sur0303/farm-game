package protocoltest

import (
	"testing"

	commonpb "farmgame/api/gen/common"
	"farmgame/internal/gateway/commandcatalog"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func protoreflectFieldNumber(v int32) protoreflect.FieldNumber {
	return protoreflect.FieldNumber(v)
}

// TestP1BCommandTypeMigrationMatrix 钉死 12→33 迁移矩阵的枚举编号纪律:
// 保留 4 条沿用编号、重命名 6 条沿用编号(旧符号名称已 reserved 删除)、
// 删除 2 条(编号+名称 reserved)、新增 23 条自 14 起。
func TestP1BCommandTypeMigrationMatrix(t *testing.T) {
	// 保留(编号不变)。
	kept := map[string]int32{
		"JOIN_FARM": 6, "GET_OPERATION_RESULT": 11,
		"CLAIM_TASK": 12, "GET_PLAYER_BOOTSTRAP": 13,
	}
	// 重命名(新名沿用旧编号;旧符号名称已 reserved)。
	renamed := map[string]int32{
		"BUY": 2, "SELL": 3, "PLANT": 4, "HARVEST": 5, "CARE": 8, "THEFT": 9,
	}
	// 新增(14 起)。
	added := map[string]int32{
		"GET_FRIEND_LIST": 14, "GET_NEXT_FRIEND_OPPORTUNITY": 15,
		"GET_TASK_LIST": 16, "CONFIRM_FRIEND_VISIT": 17,
		"GET_MAIL_LIST": 18, "GET_MAIL_DETAIL": 19, "MARK_MAIL_READ": 20,
		"DELETE_MAIL": 21, "CLAIM_MAIL_ATTACHMENT": 22,
		"GET_PET_STATE": 23, "PET_UNLOCK_BY_COIN": 24, "PET_FEED": 25, "PET_SWITCH": 26,
		"GET_COMPENDIUM": 27, "ACK_COMPENDIUM_SEEN": 28,
		"GENERATE_SHARE_TOKEN": 29, "ROTATE_SHARE_TOKEN": 30, "REVOKE_SHARE_TOKEN": 31,
		"REDEEM_SHARE": 32, "DELETE_FRIEND": 33, "BLOCK_PLAYER": 34, "UNBLOCK_PLAYER": 35,
		"UNLOCK_PLOT": 36,
	}
	for name, number := range kept {
		if got := commonpb.CommandType_value[name]; got != number {
			t.Errorf("kept %s=%d, want %d", name, got, number)
		}
	}
	for name, number := range renamed {
		if got := commonpb.CommandType_value[name]; got != number {
			t.Errorf("renamed %s=%d, want %d", name, got, number)
		}
	}
	for name, number := range added {
		if got := commonpb.CommandType_value[name]; got != number {
			t.Errorf("added %s=%d, want %d", name, got, number)
		}
	}
	// 旧符号名称已全部 reserved 删除(P1B-CONSUMERS-2);编号 2/3/4/5/8/9 仍由
	// 新名占用,不得 reserved 数值。
	for _, alias := range []string{
		"BUY_OFFER", "SELL_ITEM", "PLANT_CROP", "HARVEST_CROP", "WATER_CROP", "BEGIN_THEFT",
	} {
		if _, ok := commonpb.CommandType_value[alias]; ok {
			t.Errorf("deprecated alias %s must be reserved away", alias)
		}
	}
	// 已删除命令:编号 7/10 与名称一并 reserved,绝不在 Catalog。
	for _, name := range []string{"LEAVE_FARM", "GET_FARM_SNAPSHOT"} {
		if _, ok := commonpb.CommandType_value[name]; ok {
			t.Errorf("removed command name %s must be reserved away", name)
		}
	}
	if _, ok := commandcatalog.Lookup(commonpb.CommandType(7)); ok {
		t.Fatal("LEAVE_FARM must not be registered")
	}
	if _, ok := commandcatalog.Lookup(commonpb.CommandType(10)); ok {
		t.Fatal("GET_FARM_SNAPSHOT must not be registered")
	}
	// 33 条公开命令恰好注册 33 个 Spec。
	if got := len(commandcatalog.All()); got != 33 {
		t.Fatalf("registered specs=%d, want 33", got)
	}
}

// TestP1BEnvelopeSchemaShape 钉死 ClientCommandEnvelope 的公开形状
// (28 §3/L2-PROTO-03):只有 request_id/operation_id/command_type/payload
// 是活动字段;旧 target_owner_id/expected_* 已 reserved 删除
// (P1B-CONSUMERS-2),字段号 5/6/7/8 不得再以任何名称存在。
func TestP1BEnvelopeSchemaShape(t *testing.T) {
	descriptor := (&commonpb.ClientCommandEnvelope{}).ProtoReflect().Descriptor()
	active := map[string]int32{
		"request_id": 10, "operation_id": 2, "command_type": 4, "payload": 15,
	}
	for name, number := range active {
		field := descriptor.Fields().ByNumber(protoreflectFieldNumber(number))
		if field == nil || string(field.Name()) != name {
			t.Fatalf("active field %s(%d) missing", name, number)
		}
	}
	for _, number := range []int32{5, 6, 7, 8} {
		if field := descriptor.Fields().ByNumber(protoreflectFieldNumber(number)); field != nil {
			t.Fatalf("retired envelope field number %d reappeared as %s", number, field.Name())
		}
	}
}

// TestP1BFrameSchemaShape 钉死 FarmFrame:protocol_version=1、sequence=8
// (uint64)、oneof frame_body 六个稳定成员;CommandResult 与 ServerPush 是
// 不同分支。旧 kind=2/connection_seq=3/body=15 已 reserved 删除
// (P1B-CONSUMERS-2),不得以任何名称存在。
func TestP1BFrameSchemaShape(t *testing.T) {
	descriptor := (&commonpb.FarmFrame{}).ProtoReflect().Descriptor()
	if descriptor.Fields().ByNumber(1) == nil {
		t.Fatal("protocol_version field missing")
	}
	sequence := descriptor.Fields().ByNumber(8)
	if sequence == nil || sequence.Name() != "sequence" {
		t.Fatal("sequence field 8 missing")
	}
	for _, number := range []protoreflect.FieldNumber{2, 3, 15} {
		if field := descriptor.Fields().ByNumber(number); field != nil {
			t.Fatalf("retired frame field number %d reappeared as %s", number, field.Name())
		}
	}
	oneof := descriptor.Oneofs().ByName("frame_body")
	if oneof == nil || oneof.Fields().Len() != 6 {
		t.Fatalf("frame_body oneof members=%d, want 6", oneof.Fields().Len())
	}
	members := map[string]int32{}
	for i := 0; i < oneof.Fields().Len(); i++ {
		field := oneof.Fields().Get(i)
		members[string(field.Name())] = int32(field.Number())
	}
	want := map[string]int32{
		"client_hello": 10, "server_hello": 11, "client_command": 12,
		"command_result": 13, "server_push": 14, "control": 16,
	}
	for name, number := range want {
		if members[name] != number {
			t.Errorf("oneof member %s=%d, want %d", name, members[name], number)
		}
	}
}
