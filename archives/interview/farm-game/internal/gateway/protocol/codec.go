// Package protocol implements the public FarmFrame boundary (contract 28 §2).
//
// A WebSocket binary message contains exactly one serialized FarmFrame
// (L2-PROTO-01). The FarmFrame top-level oneof distinguishes the small set of
// stable message categories (L2-PROTO-02); business commands never enter that
// oneof — they travel as ClientCommandEnvelope.command_type + bytes payload.
package protocol

import (
	"errors"
	"fmt"

	commonpb "farmgame/api/gen/common"

	"google.golang.org/protobuf/proto"
)

const (
	// ProtocolVersion 是 FarmFrame 当前协议版本。v4 = P1B 公网 33 命令唯一迁移
	// (FarmFrame 顶层 oneof、ClientCommandEnvelope 仅 request_id/operation_id/
	// command_type/payload);旧版本客户端被 Gate 显式拒绝
	// (protocol_version_rejected_total)。
	ProtocolVersion     uint32 = 4
	DefaultMaxFrameSize        = 1 << 20
)

var (
	ErrEmptyFrame    = errors.New("protocol: empty frame")
	ErrEmptyBody     = errors.New("protocol: empty frame body")
	ErrFrameTooLarge = errors.New("protocol: frame exceeds max size")
	ErrMalformed     = errors.New("protocol: malformed FarmFrame")
	ErrBadVersion    = errors.New("protocol: unsupported protocol version")
	ErrBadDirection  = errors.New("protocol: frame category not allowed in this direction")
	ErrBadSequence   = errors.New("protocol: invalid connection sequence")
)

// Encode serializes one complete FarmFrame. The frame must carry exactly one
// oneof body; the serialized size must fit maxSize.
func Encode(frame *commonpb.FarmFrame, maxSize int) ([]byte, error) {
	if frame == nil {
		return nil, ErrMalformed
	}
	if maxSize <= 0 {
		maxSize = DefaultMaxFrameSize
	}
	if frame.GetFrameBody() == nil {
		return nil, ErrEmptyBody
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(frame)
	if err != nil {
		return nil, fmt.Errorf("protocol: marshal FarmFrame: %w", err)
	}
	if len(raw) > maxSize {
		return nil, ErrFrameTooLarge
	}
	return raw, nil
}

// Decode parses one complete FarmFrame and enforces the transport-level
// invariants: known protocol version, non-zero connection sequence and an
// assigned oneof body. Direction and per-category rules are enforced by
// ValidateClientFrame / ValidateServerFrame.
func Decode(raw []byte, maxSize int) (*commonpb.FarmFrame, error) {
	if maxSize <= 0 {
		maxSize = DefaultMaxFrameSize
	}
	if len(raw) == 0 {
		return nil, ErrEmptyFrame
	}
	if len(raw) > maxSize {
		return nil, ErrFrameTooLarge
	}
	frame := &commonpb.FarmFrame{}
	if err := proto.Unmarshal(raw, frame); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if frame.GetProtocolVersion() != ProtocolVersion {
		return nil, ErrBadVersion
	}
	if frame.GetSequence() == 0 {
		return nil, ErrBadSequence
	}
	if frame.GetFrameBody() == nil {
		return nil, ErrEmptyBody
	}
	return frame, nil
}

// ValidateClientFrame enforces the Client → Server direction contract
// (28 §2): only ClientHello, ClientCommand and client-side ControlFrame are
// legal; a client sending CommandResult/ServerPush/ServerHello or a
// server-scoped control is a protocol violation even when the oneof decodes.
func ValidateClientFrame(frame *commonpb.FarmFrame) error {
	switch frame.GetFrameBody().(type) {
	case *commonpb.FarmFrame_ClientHello, *commonpb.FarmFrame_ClientCommand:
		return nil
	case *commonpb.FarmFrame_Control:
		if frame.GetControl().GetClientRecoveryComplete() != nil {
			return nil
		}
		return fmt.Errorf("%w: server-scoped control from client", ErrBadDirection)
	default:
		return fmt.Errorf("%w: %T", ErrBadDirection, frame.GetFrameBody())
	}
}

// ValidateServerFrame enforces the Server → Client direction contract:
// ServerHello, CommandResult, ServerPush and server-side ControlFrame.
func ValidateServerFrame(frame *commonpb.FarmFrame) error {
	switch frame.GetFrameBody().(type) {
	case *commonpb.FarmFrame_ServerHello, *commonpb.FarmFrame_CommandResult, *commonpb.FarmFrame_ServerPush:
		return nil
	case *commonpb.FarmFrame_Control:
		if frame.GetControl().GetConnectionError() != nil {
			return nil
		}
		return fmt.Errorf("%w: client-scoped control from server", ErrBadDirection)
	default:
		return fmt.Errorf("%w: %T", ErrBadDirection, frame.GetFrameBody())
	}
}

// ── 构造助手:oneof 包装的唯一写路径,避免各生产方手工拼装 body 分支。 ──

func NewClientHelloFrame(sequence uint64, hello *commonpb.ClientHello) *commonpb.FarmFrame {
	return &commonpb.FarmFrame{ProtocolVersion: ProtocolVersion, Sequence: sequence,
		FrameBody: &commonpb.FarmFrame_ClientHello{ClientHello: hello}}
}

func NewServerHelloFrame(sequence uint64, hello *commonpb.ServerHello) *commonpb.FarmFrame {
	return &commonpb.FarmFrame{ProtocolVersion: ProtocolVersion, Sequence: sequence,
		FrameBody: &commonpb.FarmFrame_ServerHello{ServerHello: hello}}
}

func NewClientCommandFrame(sequence uint64, command *commonpb.ClientCommandEnvelope) *commonpb.FarmFrame {
	return &commonpb.FarmFrame{ProtocolVersion: ProtocolVersion, Sequence: sequence,
		FrameBody: &commonpb.FarmFrame_ClientCommand{ClientCommand: command}}
}

func NewCommandResultFrame(sequence uint64, result *commonpb.CommandResultEnvelope) *commonpb.FarmFrame {
	return &commonpb.FarmFrame{ProtocolVersion: ProtocolVersion, Sequence: sequence,
		FrameBody: &commonpb.FarmFrame_CommandResult{CommandResult: result}}
}

func NewServerPushFrame(sequence uint64, push *commonpb.ServerPushEnvelope) *commonpb.FarmFrame {
	return &commonpb.FarmFrame{ProtocolVersion: ProtocolVersion, Sequence: sequence,
		FrameBody: &commonpb.FarmFrame_ServerPush{ServerPush: push}}
}

func NewControlFrame(sequence uint64, control *commonpb.ControlFrame) *commonpb.FarmFrame {
	return &commonpb.FarmFrame{ProtocolVersion: ProtocolVersion, Sequence: sequence,
		FrameBody: &commonpb.FarmFrame_Control{Control: control}}
}
