package protocol

import (
	"bytes"
	"testing"

	commonpb "farmgame/api/gen/common"
	"google.golang.org/protobuf/proto"
)

func sampleCommandFrame() *commonpb.FarmFrame {
	return NewClientCommandFrame(7, &commonpb.ClientCommandEnvelope{
		RequestId:   "req-7",
		OperationId: "op-7",
		CommandType: commonpb.CommandType_BUY,
		Payload:     []byte("hello-body"),
	})
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	buf, err := Encode(sampleCommandFrame(), 0)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	f, err := Decode(buf, 0)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if f.GetProtocolVersion() != ProtocolVersion || f.GetSequence() != 7 {
		t.Fatalf("frame mismatch: %+v", f)
	}
	cmd := f.GetClientCommand()
	if cmd == nil || cmd.GetRequestId() != "req-7" || !bytes.Equal(cmd.GetPayload(), []byte("hello-body")) {
		t.Fatalf("command body mismatch: %v", cmd)
	}
	if err := ValidateClientFrame(f); err != nil {
		t.Fatalf("client direction rejected legal frame: %v", err)
	}
}

func TestRejectsFutureVersion(t *testing.T) {
	raw, err := proto.Marshal(&commonpb.FarmFrame{
		ProtocolVersion: ProtocolVersion + 1, Sequence: 1,
		FrameBody: &commonpb.FarmFrame_ClientCommand{ClientCommand: &commonpb.ClientCommandEnvelope{RequestId: "r"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(raw, DefaultMaxFrameSize); err != ErrBadVersion {
		t.Fatalf("want ErrBadVersion, got %v", err)
	}
}

func TestRejectsLegacyKindBodyShape(t *testing.T) {
	// 旧 kind/body 编译窗口已删除(reserved 2/3/15):手工构造带这两个
	// 未知字段的 wire bytes,它们对现行 schema 是未知字段,frame_body 仍为空,
	// 必须 fail closed。
	raw := []byte{
		0x08, 0x04, // field 1 varint 4 (protocol_version)
		0x10, 0x03, // field 2 varint 3 (旧 kind=CLIENT_COMMAND,现为未知字段)
		0x40, 0x01, // field 8 varint 1 (sequence)
		0x7a, 0x01, 0x78, // field 15 len 1 "x" (旧 body,现为未知字段)
	}
	if _, err := Decode(raw, DefaultMaxFrameSize); err != ErrEmptyBody {
		t.Fatalf("want ErrEmptyBody for legacy shape, got %v", err)
	}
}

func TestRejectsInvalidBoundary(t *testing.T) {
	if _, err := Decode(nil, 0); err != ErrEmptyFrame {
		t.Fatalf("want ErrEmptyFrame, got %v", err)
	}
	oversize := NewServerPushFrame(1, &commonpb.ServerPushEnvelope{
		PushType: commonpb.PushType_SYSTEM_NOTICE, Payload: make([]byte, 128),
	})
	if _, err := Encode(oversize, 32); err != ErrFrameTooLarge {
		t.Fatalf("want ErrFrameTooLarge, got %v", err)
	}
	// sequence 必须非零。
	raw, err := proto.Marshal(&commonpb.FarmFrame{
		ProtocolVersion: ProtocolVersion,
		FrameBody: &commonpb.FarmFrame_ClientCommand{
			ClientCommand: &commonpb.ClientCommandEnvelope{RequestId: "r"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(raw, 0); err != ErrBadSequence {
		t.Fatalf("want ErrBadSequence, got %v", err)
	}
	// 空 oneof body 拒绝。
	raw, err = proto.Marshal(&commonpb.FarmFrame{ProtocolVersion: ProtocolVersion, Sequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(raw, 0); err != ErrEmptyBody {
		t.Fatalf("want ErrEmptyBody, got %v", err)
	}
}

// TestSequenceUint64Boundary 覆盖连接序号的 uint64 边界:大于 uint32 最大值
// 的序号必须无损往返(28 §2.5,L2-PROTO-08/11)。
func TestSequenceUint64Boundary(t *testing.T) {
	for _, seq := range []uint64{1, uint64(1)<<32 - 1, uint64(1) << 32, uint64(1)<<63 - 1} {
		raw, err := Encode(NewCommandResultFrame(seq, &commonpb.CommandResultEnvelope{
			RequestId: "r", Status: commonpb.CommandStatus_SUCCEEDED,
		}), 0)
		if err != nil {
			t.Fatalf("seq=%d encode: %v", seq, err)
		}
		f, err := Decode(raw, 0)
		if err != nil {
			t.Fatalf("seq=%d decode: %v", seq, err)
		}
		if f.GetSequence() != seq {
			t.Fatalf("sequence lost: got %d want %d", f.GetSequence(), seq)
		}
		if f.GetCommandResult() == nil {
			t.Fatalf("seq=%d command result missing", seq)
		}
	}
}

// TestDirectionValidation 覆盖 28 §2 的方向违规:客户端发送
// CommandResult/ServerPush/ServerHello 或服务端作用域 Control 均为协议违规;
// 服务端发送 ClientCommand/ClientHello 同理。oneof 能解码不构成放行理由。
func TestDirectionValidation(t *testing.T) {
	serverHello := NewServerHelloFrame(1, &commonpb.ServerHello{PlayerId: 1})
	if err := ValidateClientFrame(serverHello); err == nil {
		t.Fatal("client-sent ServerHello accepted")
	}
	if err := ValidateServerFrame(serverHello); err != nil {
		t.Fatalf("ServerHello must be legal server->client: %v", err)
	}
	result := NewCommandResultFrame(2, &commonpb.CommandResultEnvelope{RequestId: "r"})
	if err := ValidateClientFrame(result); err == nil {
		t.Fatal("client-sent CommandResult accepted")
	}
	push := NewServerPushFrame(3, &commonpb.ServerPushEnvelope{PushType: commonpb.PushType_KICK})
	if err := ValidateClientFrame(push); err == nil {
		t.Fatal("client-sent ServerPush accepted")
	}
	clientCmd := NewClientCommandFrame(4, &commonpb.ClientCommandEnvelope{RequestId: "r"})
	if err := ValidateServerFrame(clientCmd); err == nil {
		t.Fatal("server-sent ClientCommand accepted")
	}
	recovery := NewControlFrame(5, &commonpb.ControlFrame{
		Body: &commonpb.ControlFrame_ClientRecoveryComplete{ClientRecoveryComplete: &commonpb.ClientRecoveryComplete{}},
	})
	if err := ValidateClientFrame(recovery); err != nil {
		t.Fatalf("client recovery control must be legal client->server: %v", err)
	}
	if err := ValidateServerFrame(recovery); err == nil {
		t.Fatal("client-scoped control accepted from server")
	}
	connErr := NewControlFrame(6, &commonpb.ControlFrame{
		Body: &commonpb.ControlFrame_ConnectionError{ConnectionError: &commonpb.ConnectionError{}},
	})
	if err := ValidateServerFrame(connErr); err != nil {
		t.Fatalf("connection error control must be legal server->client: %v", err)
	}
	if err := ValidateClientFrame(connErr); err == nil {
		t.Fatal("server-scoped control accepted from client")
	}
}

// TestCommandResultAndServerPushAreDistinctBranches 证明 CommandResult 与
// ServerPush 在顶层 oneof 中是可区分的不同分支(28 §2.1)。
func TestCommandResultAndServerPushAreDistinctBranches(t *testing.T) {
	resultRaw, err := Encode(NewCommandResultFrame(1, &commonpb.CommandResultEnvelope{RequestId: "r"}), 0)
	if err != nil {
		t.Fatal(err)
	}
	pushRaw, err := Encode(NewServerPushFrame(2, &commonpb.ServerPushEnvelope{PushType: commonpb.PushType_FARM_CHANGE}), 0)
	if err != nil {
		t.Fatal(err)
	}
	resultFrame, err := Decode(resultRaw, 0)
	if err != nil {
		t.Fatal(err)
	}
	if resultFrame.GetCommandResult() == nil || resultFrame.GetServerPush() != nil {
		t.Fatalf("command result branch polluted: %T", resultFrame.GetFrameBody())
	}
	pushFrame, err := Decode(pushRaw, 0)
	if err != nil {
		t.Fatal(err)
	}
	if pushFrame.GetServerPush() == nil || pushFrame.GetCommandResult() != nil {
		t.Fatalf("server push branch polluted: %T", pushFrame.GetFrameBody())
	}
}

// TestUnknownOneofMemberFailsClosed 证明未知 oneof 成员(未来协议版本新增的
// 大类)在旧实现上 fail closed,而不是被当成合法帧。
func TestUnknownOneofMemberFailsClosed(t *testing.T) {
	// 手工编码 frame_body field 20(未注册成员,LEN 线型)。
	payload := []byte{0x08, byte(ProtocolVersion), 0x40, 0x01, 0xA2, 0x01, 0x00}
	if _, err := Decode(payload, 0); err == nil {
		t.Fatal("unknown oneof member accepted")
	}
}

func TestDeterministicEncoding(t *testing.T) {
	a, err := Encode(sampleCommandFrame(), 0)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Encode(sampleCommandFrame(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("same frame produced different bytes")
	}
}
