package commandcatalog

import (
	"bytes"
	"encoding/hex"
	"testing"

	commonpb "farmgame/api/gen/common"
	publicpb "farmgame/api/gen/public"

	"google.golang.org/protobuf/proto"
)

func commandOf(t *testing.T, commandType commonpb.CommandType, payload proto.Message) *commonpb.ClientCommandEnvelope {
	t.Helper()
	raw, err := proto.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return &commonpb.ClientCommandEnvelope{
		RequestId: "req-golden", OperationId: "op-golden",
		CommandType: commandType, Payload: raw,
	}
}

// TestCanonicalPreimageGolden 逐字节钉死 Canonical Payload 原像:
// uint32_be(command_type) || CanonicalBytes(decodedPayload)。
// 黄金值手工构造,不经过被测实现,任何编码漂移都会破坏比对。
func TestCanonicalPreimageGolden(t *testing.T) {
	plant := &publicpb.PlantRequest{
		PlotId: 3, CropDefinitionId: 1, TargetFarmId: 101,
		ExpectedPlotVersion: 9, ExpectedCropRuleVersion: 2,
	}
	preimage, err := CanonicalPreimage(commandOf(t, commonpb.CommandType_PLANT, plant))
	if err != nil {
		t.Fatal(err)
	}
	// 手工原像(29 §7.11):command_type=4(PLANT)的 uint32 BE 前缀,随后按
	// field number 序编码语义字段,整数定宽大端、无字段 tag:
	//   f1 plot_id=3(u32);f2 crop_definition_id=1(u32);f3 target_farm_id=101(u64);
	//   f4 expected_plot_version=9(u64);f5 expected_crop_rule_version=2(u64)
	want := buildGoldenPreimage(4,
		u32be(3), u32be(1), u64be(101), u64be(9), u64be(2))
	if !bytes.Equal(preimage, want) {
		t.Fatalf("PLANT canonical preimage:\n got %x\nwant %x", preimage, want)
	}
	hash, err := CanonicalHash(commandOf(t, commonpb.CommandType_PLANT, plant))
	if err != nil {
		t.Fatal(err)
	}
	if len(hash) != 32 {
		t.Fatalf("canonical hash length=%d, want 32", len(hash))
	}
	t.Logf("PLANT canonical hash golden: %s", hex.EncodeToString(hash))
}

// u32be/u64be 构造 CanonicalBytes 的定宽大端整数编码,仅用于黄金比对。
func u32be(v uint32) []byte {
	return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

func u64be(v uint64) []byte {
	return []byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32),
		byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

func appendUvarintForTest(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

func buildGoldenPreimage(commandType uint32, fields ...[]byte) []byte {
	out := []byte{byte(commandType >> 24), byte(commandType >> 16), byte(commandType >> 8), byte(commandType)}
	for _, field := range fields {
		out = append(out, field...)
	}
	return out
}

// TestCanonicalReplayStable 证明同一 operation + 同一 payload 稳定重放;
// 同 operation + 不同 payload 稳定冲突(28 §3.5)。
func TestCanonicalReplayStable(t *testing.T) {
	base := commandOf(t, commonpb.CommandType_PLANT, &publicpb.PlantRequest{
		PlotId: 3, CropDefinitionId: 1, TargetFarmId: 101, ExpectedPlotVersion: 9,
	})
	hashA, err := CanonicalHash(base)
	if err != nil {
		t.Fatal(err)
	}
	hashB, err := CanonicalHash(base)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(hashA, hashB) {
		t.Fatal("same operation + same payload must replay stable")
	}
	conflict := commandOf(t, commonpb.CommandType_PLANT, &publicpb.PlantRequest{
		PlotId: 4, CropDefinitionId: 1, TargetFarmId: 101, ExpectedPlotVersion: 9,
	})
	hashC, err := CanonicalHash(conflict)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(hashA, hashC) {
		t.Fatal("same operation + different payload must conflict stable")
	}
}

// TestCanonicalEnvelopeNoiseInvariance 证明 request_id / operation_id /
// envelope 噪声不进入 Canonical Hash(28 §3.5:Hash 回答"是否仍是原来的
// 业务意图",不是网络包校验和)。
func TestCanonicalEnvelopeNoiseInvariance(t *testing.T) {
	payload := &publicpb.PlantRequest{PlotId: 3, CropDefinitionId: 1, TargetFarmId: 101}
	first := commandOf(t, commonpb.CommandType_PLANT, payload)
	first.RequestId = "req-1"
	first.OperationId = "op-same"
	second := commandOf(t, commonpb.CommandType_PLANT, payload)
	second.RequestId = "req-2"
	second.OperationId = "op-same"
	hashA, err := CanonicalHash(first)
	if err != nil {
		t.Fatal(err)
	}
	hashB, err := CanonicalHash(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(hashA, hashB) {
		t.Fatal("envelope noise must not change canonical hash")
	}
}

// TestCanonicalEncodingOrderInvariance 证明同语义不同合法编码(字段顺序差异)
// 得到相同 Hash(28 §3.5/29 §7.11:禁止直接 Hash 原始 bytes)。
func TestCanonicalEncodingOrderInvariance(t *testing.T) {
	plant := &publicpb.PlantRequest{PlotId: 3, CropDefinitionId: 1, TargetFarmId: 101, ExpectedPlotVersion: 9}
	// 正序编码。
	canonical, err := proto.Marshal(plant)
	if err != nil {
		t.Fatal(err)
	}
	// 手工逆序编码:f5,f4,f3,f2,f1(均为 varint 字段)。
	// 值:plot_id=3(f1),crop_definition_id=1(f2),target_farm_id=101(f3),
	// expected_plot_version=9(f4),expected_crop_rule_version=0(f5,显式编码零值)。
	reversed := []byte{}
	reversed = appendUvarintForTest(reversed, 5<<3|0)
	reversed = appendUvarintForTest(reversed, 0)
	reversed = appendUvarintForTest(reversed, 4<<3|0)
	reversed = appendUvarintForTest(reversed, 9)
	reversed = appendUvarintForTest(reversed, 3<<3|0)
	reversed = appendUvarintForTest(reversed, 101)
	reversed = appendUvarintForTest(reversed, 2<<3|0)
	reversed = appendUvarintForTest(reversed, 1)
	reversed = appendUvarintForTest(reversed, 1<<3|0)
	reversed = appendUvarintForTest(reversed, 3)
	if bytes.Equal(canonical, reversed) {
		t.Fatal("test premise broken: encodings unexpectedly identical")
	}
	hashA, err := CanonicalHash(&commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_PLANT, Payload: canonical})
	if err != nil {
		t.Fatal(err)
	}
	hashB, err := CanonicalHash(&commonpb.ClientCommandEnvelope{CommandType: commonpb.CommandType_PLANT, Payload: reversed})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(hashA, hashB) {
		t.Fatal("semantically equal encodings must share canonical hash")
	}
}

// TestCanonicalUint64Boundaries 覆盖 uint64/string 边界值进入 Canonical Hash
// 的确定性(28 §7/L2-PROTO-08)。
func TestCanonicalUint64Boundaries(t *testing.T) {
	values := []uint64{0, 1, 255, 256, 1<<32 - 1, 1 << 32, 1<<63 - 1, 1<<64 - 1}
	seen := make(map[string]uint64, len(values))
	for _, value := range values {
		hash, err := CanonicalHash(commandOf(t, commonpb.CommandType_HARVEST,
			&publicpb.HarvestRequest{PlotId: 1, TargetFarmId: value, CropCycleId: "cc-1", ExpectedPlotVersion: 1<<64 - 1}))
		if err != nil {
			t.Fatalf("value=%d: %v", value, err)
		}
		key := string(hash)
		if other, dup := seen[key]; dup {
			t.Fatalf("hash collision between target_farm_id=%d and %d", other, value)
		}
		seen[key] = value
	}
	// string 边界:空串、最长 64 字节 operation/cycle 类字段、UTF-8。
	for _, cycle := range []string{"", "c", string(bytes.Repeat([]byte("x"), 64)), "茬次-🌱"} {
		if _, err := CanonicalHash(commandOf(t, commonpb.CommandType_THEFT,
			&publicpb.TheftRequest{PlotId: 1, TargetFarmId: 1, CropCycleId: cycle})); err != nil {
			t.Errorf("cycle %q: %v", cycle, err)
		}
	}
}

// TestCanonicalBuilderCoverage 证明每条 REQUIRED/RUNTIME_REQUIRED 命令的
// CanonicalPayloadBuilder 都可用且确定(28 §6.4 测试 9)。
func TestCanonicalBuilderCoverage(t *testing.T) {
	for _, spec := range All() {
		if !spec.Idempotency.RequiresCanonicalBuilder() {
			continue
		}
		if spec.CanonicalPayloadBuilder == nil {
			t.Errorf("%s: missing canonical builder", spec.CommandType)
			continue
		}
		decoded := mustDecode(t, spec, nil)
		preA, err := spec.CanonicalPayloadBuilder(decoded)
		if err != nil {
			t.Errorf("%s: builder on zero payload: %v", spec.CommandType, err)
			continue
		}
		decodedAgain := mustDecode(t, spec, nil)
		preB, err := spec.CanonicalPayloadBuilder(decodedAgain)
		if err != nil {
			t.Errorf("%s: builder replay: %v", spec.CommandType, err)
			continue
		}
		if !bytes.Equal(preA, preB) {
			t.Errorf("%s: builder not deterministic", spec.CommandType)
		}
		if len(preA) < 4 {
			t.Errorf("%s: preimage missing command_type prefix", spec.CommandType)
		}
	}
}
