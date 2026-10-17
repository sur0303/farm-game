import { describe, expect, it } from 'vitest';

import { CommandResultEnvelope, CommandType, PushType, ServerPushEnvelope } from '../protocol/generated/common/envelope';
import { CommandStatus, ErrorCode } from '../protocol/generated/common/enums';
import { FarmChangePush } from '../protocol/generated/farm/farm_view';

// W11A 黄金向量:与 farm-game/internal/protocoltest/envelope_test.go 共用同一组
// 固定字节样例,两端互验 28 §2.2/§2.4 冻结结构的编解码一致性。
// 重新生成方式:在 farm-game 仓库执行 `go run /tmp/goldgen.go`(见 Go 侧测试注释),
// 输出 PUSH_HEX / RESULT_HEX / ERROR_RESULT_HEX 后同步两边常量。
const GOLDEN_PUSH_HEX = '30027a1f0a1d08914e102a1a16080310071a0463632d312001280230e80738d00f4001620f08914e10914e1a077375622d616263';
const GOLDEN_JOIN_RESULT_HEX = '0a046f702d3110063a057265712d31b80106ca011208914e12077375622d6162631a046a612d31';
const GOLDEN_ERROR_RESULT_HEX = '0a046f702d3210043a057265712d32a20112088927120d706c6f74206f63637570696564b80104';

const fromHex = (hex: string): Uint8Array => {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < bytes.length; i += 1) bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return bytes;
};

describe('W11A cross-language golden vectors', () => {
  it('decodes the Go-generated FARM_CHANGE push with farm_subscription context', () => {
    const push = ServerPushEnvelope.decode(fromHex(GOLDEN_PUSH_HEX));
    expect(push.pushType).toBe(PushType.FARM_CHANGE);
    expect(push.farmSubscription?.targetPlayerId).toBe(10001n);
    expect(push.farmSubscription?.targetFarmId).toBe(10001n);
    expect(push.farmSubscription?.subscriptionId).toBe('sub-abc');
    const change = FarmChangePush.decode(push.payload).change;
    expect(change).toBeDefined();
    if (!change) throw new Error('golden FARM_CHANGE payload is missing change');
    expect(change.ownerId).toBe(10001n);
    expect(change.farmVersion).toBe(42n);
    expect(change.changedPlots).toHaveLength(1);
    expect(change.changedPlots[0].cropCycleId).toBe('cc-1');
    // 两端编码器字段发射顺序可能不同(同为合法 protobuf),做往返一致性而非逐字节相等。
    const roundTrip = ServerPushEnvelope.decode(ServerPushEnvelope.encode(push).finish());
    expect(roundTrip.pushType).toBe(push.pushType);
    expect(roundTrip.farmSubscription?.subscriptionId).toBe('sub-abc');
    expect(roundTrip.payload).toEqual(push.payload);
  });

  it('decodes the Go-generated Join ACCEPTED result with JoinResultContext', () => {
    const result = CommandResultEnvelope.decode(fromHex(GOLDEN_JOIN_RESULT_HEX));
    expect(result.operationId).toBe('op-1');
    expect(result.requestId).toBe('req-1');
    expect(result.status).toBe(CommandStatus.ACCEPTED);
    expect(result.commandType).toBe(CommandType.JOIN_FARM);
    expect(result.join?.targetFarmId).toBe(10001n);
    expect(result.join?.subscriptionId).toBe('sub-abc');
    expect(result.join?.joinAttemptId).toBe('ja-1');
    const roundTrip = CommandResultEnvelope.decode(CommandResultEnvelope.encode(result).finish());
    expect(roundTrip.join?.joinAttemptId).toBe('ja-1');
    expect(roundTrip.status).toBe(CommandStatus.ACCEPTED);
  });

  it('decodes the Go-generated error result with ErrorDetail', () => {
    const result = CommandResultEnvelope.decode(fromHex(GOLDEN_ERROR_RESULT_HEX));
    expect(result.status).toBe(CommandStatus.REJECTED);
    expect(result.error?.errorCode).toBe(ErrorCode.ERR_PLOT_NOT_EMPTY);
    expect(result.error?.errorMessage).toBe('plot occupied');
    const roundTrip = CommandResultEnvelope.decode(CommandResultEnvelope.encode(result).finish());
    expect(roundTrip.error?.errorCode).toBe(ErrorCode.ERR_PLOT_NOT_EMPTY);
  });
});
