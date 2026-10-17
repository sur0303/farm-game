package protocoltest

import (
	"encoding/hex"
	"testing"

	commonpb "farmgame/api/gen/common"
	"farmgame/internal/gateway/protocol"
)

// TestFarmFrameGoldenClientHello 钉死 P1B 公网 FarmFrame 顶层 oneof 编码:
// protocol_version=4、sequence=1、frame_body=client_hello(field 10)。
// 黄金字节手工推导:08 04 | 40 01 | 52 10 <ClientHello{ticket="t",build="farm-h5/0.2"}>
func TestFarmFrameGoldenClientHello(t *testing.T) {
	raw, err := protocol.Encode(protocol.NewClientHelloFrame(1, &commonpb.ClientHello{
		ConnectionTicket: "t", ClientBuild: "farm-h5/0.2",
	}), 0)
	if err != nil {
		t.Fatal(err)
	}
	const want = "0804400152100a01741a0b6661726d2d68352f302e32"
	if got := hex.EncodeToString(raw); got != want {
		t.Fatalf("golden=%s want=%s", got, want)
	}
}

// TestFarmFrameGoldenClientCommand 钉死 ClientCommand 帧编码:
// sequence=2、frame_body=client_command(field 12),Envelope 只含
// request_id/operation_id/command_type/payload(28 §3)。
func TestFarmFrameGoldenClientCommand(t *testing.T) {
	raw, err := protocol.Encode(protocol.NewClientCommandFrame(2, &commonpb.ClientCommandEnvelope{
		RequestId: "r1", OperationId: "op1",
		CommandType: commonpb.CommandType_BUY,
		Payload:     []byte{0x08, 0x01, 0x10, 0x02}, // BuyRequest{offer_id=1,count=2}
	}), 0)
	if err != nil {
		t.Fatal(err)
	}
	const want = "08044002621112036f70312002520272317a0408011002"
	if got := hex.EncodeToString(raw); got != want {
		t.Fatalf("golden=%s want=%s", got, want)
	}
}

// TestFarmFrameGoldenCommandResultAndServerPushSeparated 证明下行两大类在
// oneof 中逐字节可区分(28 §2.1)。
func TestFarmFrameGoldenCommandResultAndServerPushSeparated(t *testing.T) {
	resultRaw, err := protocol.Encode(protocol.NewCommandResultFrame(3, &commonpb.CommandResultEnvelope{
		RequestId: "r1", Status: commonpb.CommandStatus_SUCCEEDED,
	}), 0)
	if err != nil {
		t.Fatal(err)
	}
	// field 13(0x6a)= command_result
	const wantResult = "080440036a0610013a027231"
	if got := hex.EncodeToString(resultRaw); got != wantResult {
		t.Fatalf("result golden=%s want=%s", got, wantResult)
	}
	pushRaw, err := protocol.Encode(protocol.NewServerPushFrame(4, &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_SUBSCRIPTION_READY,
	}), 0)
	if err != nil {
		t.Fatal(err)
	}
	// field 14(0x72)= server_push
	const wantPush = "0804400472023015"
	if got := hex.EncodeToString(pushRaw); got != wantPush {
		t.Fatalf("push golden=%s want=%s", got, wantPush)
	}
	if resultRaw[4] != pushRaw[4] {
		// 第 5 字节是 oneof 成员 tag,必须不同。
		t.Logf("oneof branch tags differ as required: result=%02x push=%02x", resultRaw[4], pushRaw[4])
	}
}
