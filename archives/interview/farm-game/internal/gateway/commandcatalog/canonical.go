package commandcatalog

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"

	commonpb "farmgame/api/gen/common"
	"farmgame/internal/contracts"

	"google.golang.org/protobuf/proto"
)

// CanonicalPayloadBuilder 实现(28 §3.5 + 29 §7.11):
//
//	CanonicalPayload = uint32_be(command_type) || CanonicalBytes(decodedPayload)
//	CanonicalPayloadHash = SHA-256(CanonicalPayload)
//
// CanonicalBytes 来自 SCB-1(internal/contracts):protoreflect 按 field number
// 顺序遍历、定宽整数、presence bit、计数前缀,拒绝 map/扩展/未知字段。它作用
// 于解码后的 Typed Payload,因此同语义不同合法编码(字段顺序、未知字段、
// 实现差异)得到相同 Hash;绝不直接 Hash 原始 Protobuf bytes,也不经过
// JSON/map/非确定编码。
//
// 唯一权威 Hash 由 FirstHop 在重新解码后生成;Gate 如生成诊断 Hash 必须标记
// 为非权威,不参与幂等对比(L2-PROTO-13/14)。
func canonicalBuilder(commandType commonpb.CommandType) CanonicalPayloadBuilder {
	return func(decoded proto.Message) ([]byte, error) {
		if decoded == nil {
			return nil, errors.New("commandcatalog: canonical builder requires decoded payload")
		}
		body, err := contracts.CanonicalBytes(decoded)
		if err != nil {
			return nil, fmt.Errorf("commandcatalog: canonical payload for %s: %w", commandType, err)
		}
		out := make([]byte, 0, 4+len(body))
		out = binary.BigEndian.AppendUint32(out, uint32(commandType))
		return append(out, body...), nil
	}
}

// CanonicalPreimage 返回命令的 Canonical Payload 原像(解码 + 逐命令
// builder)。黄金向量用它逐字节钉死稳定重放/稳定冲突语义。
func CanonicalPreimage(cmd *commonpb.ClientCommandEnvelope) ([]byte, error) {
	if cmd == nil {
		return nil, errors.New("commandcatalog: command required")
	}
	spec, ok := Lookup(cmd.GetCommandType())
	if !ok {
		return nil, fmt.Errorf("commandcatalog: unregistered command type %s", cmd.GetCommandType())
	}
	if spec.Decoder == nil || spec.CanonicalPayloadBuilder == nil {
		return nil, fmt.Errorf("commandcatalog: %s has no canonical payload builder", cmd.GetCommandType())
	}
	decoded, err := spec.Decoder(cmd.GetPayload())
	if err != nil {
		return nil, err
	}
	return spec.CanonicalPayloadBuilder(decoded)
}

// CanonicalHash 返回 CanonicalPayloadHash(SHA-256 of CanonicalPreimage)。
// 相同 EffectiveIdempotencyKey + 相同 Hash = 同一业务意图的重试;相同 Key +
// 不同 Hash = IDEMPOTENCY_CONFLICT(28 §3.5)。
func CanonicalHash(cmd *commonpb.ClientCommandEnvelope) ([]byte, error) {
	preimage, err := CanonicalPreimage(cmd)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(preimage)
	return sum[:], nil
}
