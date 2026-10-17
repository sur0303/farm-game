package protocoltest

import (
	"encoding/hex"
	"testing"

	commonpb "farmgame/api/gen/common"
	farmpb "farmgame/api/gen/farm"
	gateprotocol "farmgame/internal/gateway/protocol"

	"google.golang.org/protobuf/proto"
)

// TestV04EnvelopeRoundTrip 验证 P1B 冻结的 ClientCommandEnvelope:
// 活动字段只有 request_id/operation_id/command_type/payload(28 §3,
// L2-PROTO-03);操作者身份与目标由 TrustedCommandContext 与 Typed Payload
// 承接,不再进入公共 Envelope。
func TestV04EnvelopeRoundTrip(t *testing.T) {
	cmd := &commonpb.ClientCommandEnvelope{
		OperationId: "op-abc-123",
		RequestId:   "req-1",
		CommandType: commonpb.CommandType_JOIN_FARM,
		Payload:     []byte("join-payload"),
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	got := &commonpb.ClientCommandEnvelope{}
	if err := proto.Unmarshal(raw, got); err != nil {
		t.Fatalf("unmarshal command: %v", err)
	}
	if got.RequestId != cmd.RequestId || got.OperationId != cmd.OperationId ||
		got.CommandType != commonpb.CommandType_JOIN_FARM || string(got.Payload) != "join-payload" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}

	push := &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_FARM_SNAPSHOT,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{
			FarmSubscription: &commonpb.FarmSubscriptionPushContext{
				TargetPlayerId: 1, TargetFarmId: 1, SubscriptionId: "sub-1",
			},
		},
		Payload: []byte("snapshot"),
	}
	raw, err = proto.MarshalOptions{Deterministic: true}.Marshal(push)
	if err != nil {
		t.Fatal(err)
	}
	decoded := &commonpb.ServerPushEnvelope{}
	if err := proto.Unmarshal(raw, decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.PushType != commonpb.PushType_FARM_SNAPSHOT ||
		decoded.GetFarmSubscription().GetSubscriptionId() != "sub-1" ||
		decoded.GetFarmSubscription().GetTargetFarmId() != 1 {
		t.Fatalf("push mismatch: %+v", decoded)
	}
}

func TestFarmFrameAndViewRoundTrip(t *testing.T) {
	view := &farmpb.FarmSnapshot{OwnerId: 1, SnapshotTimeMs: 100, FarmVersion: 2, Plots: []*farmpb.PlotView{{PlotIndex: 0, PlotVersion: 1}}}
	viewBytes, err := proto.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	push := &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_FARM_SNAPSHOT,
		PushContext: &commonpb.ServerPushEnvelope_FarmSubscription{
			FarmSubscription: &commonpb.FarmSubscriptionPushContext{
				TargetPlayerId: 1, TargetFarmId: 1, SubscriptionId: "sub-1",
			},
		},
		Payload: viewBytes,
	}
	frame := gateprotocol.NewServerPushFrame(1, push)
	raw, err := proto.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	gotFrame := &commonpb.FarmFrame{}
	if err := proto.Unmarshal(raw, gotFrame); err != nil {
		t.Fatal(err)
	}
	if gotFrame.GetSequence() != 1 || gotFrame.GetServerPush() == nil {
		t.Fatalf("frame mismatch: %+v", gotFrame)
	}
	gotView := &farmpb.FarmSnapshot{}
	if err := proto.Unmarshal(gotFrame.GetServerPush().GetPayload(), gotView); err != nil {
		t.Fatal(err)
	}
	if gotView.OwnerId != 1 || gotView.Plots[0].PlotVersion != 1 {
		t.Fatalf("view mismatch: %+v", gotView)
	}
}

func TestStatusAndErrorEnums(t *testing.T) {
	if commonpb.CommandStatus_ACCEPTED != 6 || commonpb.CommandStatus_UNAVAILABLE != 5 {
		t.Fatalf("CommandStatus enum values drifted")
	}
	if commonpb.ErrorCode_ERR_CONNECTION_TICKET_INVALID != 2005 {
		t.Fatalf("ticket error code drifted")
	}
}

// W11A 黄金向量:与 classic-farm-h5/src/test/w11a-golden.test.ts 共用同一组
// 固定字节样例,两端互验 28 §2.2/§2.4 冻结结构的编解码一致性。
// 重新生成方式见该文件头注释(go run, deterministc marshal)。
const (
	goldenPushHex       = "30027a1f0a1d08914e102a1a16080310071a0463632d312001280230e80738d00f4001620f08914e10914e1a077375622d616263"
	goldenJoinResultHex = "0a046f702d3110063a057265712d31b80106ca011208914e12077375622d6162631a046a612d31"
	goldenErrorHex      = "0a046f702d3210043a057265712d32a20112088927120d706c6f74206f63637570696564b80104"
)

func mustHex(t *testing.T, value string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestGoldenPushVector(t *testing.T) {
	decoded := &commonpb.ServerPushEnvelope{}
	if err := proto.Unmarshal(mustHex(t, goldenPushHex), decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.PushType != commonpb.PushType_FARM_CHANGE {
		t.Fatalf("push_type=%s, want FARM_CHANGE", decoded.PushType)
	}
	context := decoded.GetFarmSubscription()
	if context.GetTargetPlayerId() != 10001 || context.GetTargetFarmId() != 10001 ||
		context.GetSubscriptionId() != "sub-abc" {
		t.Fatalf("farm_subscription context mismatch: %+v", context)
	}
	payload := &farmpb.FarmChangePush{}
	if err := proto.Unmarshal(decoded.Payload, payload); err != nil {
		t.Fatal(err)
	}
	change := payload.GetChange()
	if change == nil || change.OwnerId != 10001 || change.FarmVersion != 42 || len(change.ChangedPlots) != 1 ||
		change.ChangedPlots[0].CropCycleId != "cc-1" {
		t.Fatalf("change payload mismatch: %+v", change)
	}
	// 确定性重编码必须逐字节一致。
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(encoded) != goldenPushHex {
		t.Fatalf("re-encode drift: %s", hex.EncodeToString(encoded))
	}
}

func TestGoldenJoinResultVector(t *testing.T) {
	decoded := &commonpb.CommandResultEnvelope{}
	if err := proto.Unmarshal(mustHex(t, goldenJoinResultHex), decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.OperationId != "op-1" || decoded.RequestId != "req-1" ||
		decoded.Status != commonpb.CommandStatus_ACCEPTED ||
		decoded.CommandType != commonpb.CommandType_JOIN_FARM {
		t.Fatalf("join result mismatch: %+v", decoded)
	}
	join := decoded.GetJoin()
	if join.GetTargetFarmId() != 10001 || join.GetSubscriptionId() != "sub-abc" ||
		join.GetJoinAttemptId() != "ja-1" {
		t.Fatalf("join context mismatch: %+v", join)
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(encoded) != goldenJoinResultHex {
		t.Fatalf("re-encode drift: %s", hex.EncodeToString(encoded))
	}
}

func TestGoldenErrorResultVector(t *testing.T) {
	decoded := &commonpb.CommandResultEnvelope{}
	if err := proto.Unmarshal(mustHex(t, goldenErrorHex), decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Status != commonpb.CommandStatus_REJECTED ||
		decoded.Error.GetErrorCode() != commonpb.ErrorCode_ERR_PLOT_NOT_EMPTY ||
		decoded.Error.GetErrorMessage() != "plot occupied" {
		t.Fatalf("error result mismatch: %+v", decoded)
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(encoded) != goldenErrorHex {
		t.Fatalf("re-encode drift: %s", hex.EncodeToString(encoded))
	}
}
