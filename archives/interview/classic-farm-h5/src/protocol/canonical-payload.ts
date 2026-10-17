/**
 * Canonical Payload Hash(28 §3.5 + 29 §7.11,SCB-1 规则)的 H5 复算实现。
 *
 * 用途:Operation Journal 的重放/冲突判定材料(同 operation_id + 同 hash =
 * 同一业务意图重试;同 operation_id + 不同 hash = 本地即可发现的
 * IDEMPOTENCY_CONFLICT 前兆)。唯一权威 Hash 仍由 FirstHop 生成;本实现必须与
 * farm-game/internal/protocoltest/p1b_canonical_golden_test.go 的黄金逐字节
 * 一致,否则视为实现错误。
 *
 * 规则(farm-game/internal/contracts/canonical.go):
 *   preimage = uint32_be(command_type) ‖ CanonicalBytes(payload)
 *   CanonicalBytes:字段号升序;proto3 非 optional 标量总是编码(含零值);
 *   整数定宽大端(intN 取低 N 位);string/bytes 用 LP(u32be len ‖ bytes);
 *   explicit presence 字段前置 1 字节 presence bit;repeated 前置 u32 计数。
 *   本客户端接线的命令 Payload 均为扁平标量,无 repeated/嵌套/optional
 *   (ROTATE_SHARE_TOKEN 的 optional peer 不在发送面)。
 */

import { CommandType as ProtoCommandType } from './generated/common/envelope';
import type { CommandPayload } from './types';
import { parseUint64 } from './uint64';

type CanonicalScalar =
  | { kind: 'uint32' | 'int32' | 'enum'; value: number }
  | { kind: 'uint64' | 'int64'; value: bigint }
  | { kind: 'string'; value: string };

const textEncoder = new TextEncoder();

const COMMAND_TYPE_NUMBER: Record<CommandPayload['type'], number> = {
  JOIN_FARM: ProtoCommandType.JOIN_FARM,
  UNLOCK_PLOT: ProtoCommandType.UNLOCK_PLOT,
  BUY: ProtoCommandType.BUY,
  SELL: ProtoCommandType.SELL,
  PLANT: ProtoCommandType.PLANT,
  HARVEST: ProtoCommandType.HARVEST,
  CARE: ProtoCommandType.CARE,
  THEFT: ProtoCommandType.THEFT,
  GET_PLAYER_BOOTSTRAP: ProtoCommandType.GET_PLAYER_BOOTSTRAP,
  GET_TASK_LIST: ProtoCommandType.GET_TASK_LIST,
  CLAIM_TASK: ProtoCommandType.CLAIM_TASK,
  CONFIRM_FRIEND_VISIT: ProtoCommandType.CONFIRM_FRIEND_VISIT,
};

export const commandTypeNumber = (type: CommandPayload['type']): number => COMMAND_TYPE_NUMBER[type];

const uint64Field = (value: string | number | bigint, field: string): CanonicalScalar => ({
  kind: 'uint64',
  value: parseUint64(value, field),
});

const uint32Field = (value: number, field: string): CanonicalScalar => {
  if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) throw new Error(`${field} is outside uint32 range`);
  return { kind: 'uint32', value };
};

const int32Field = (value: number, field: string): CanonicalScalar => {
  if (!Number.isInteger(value) || value < -0x80000000 || value > 0x7fffffff) throw new Error(`${field} is outside int32 range`);
  return { kind: 'int32', value };
};

/** 每条命令的 CanonicalPayload 字段集合(字段号升序,与 FirstHop builder 对齐)。 */
const canonicalFields = (payload: CommandPayload): CanonicalScalar[] => {
  switch (payload.type) {
    case 'JOIN_FARM':
      return [uint64Field(payload.targetFarmId, 'target_farm_id')];
    case 'UNLOCK_PLOT':
      return [
        uint64Field(payload.targetFarmId, 'target_farm_id'),
        uint32Field(payload.plotId, 'plot_id'),
        uint64Field(payload.expectedPlotVersion, 'expected_plot_version'),
        uint64Field(payload.expectedUnlockRuleVersion, 'expected_unlock_rule_version'),
      ];
    case 'BUY':
      // 服务端 CanonicalBytes 按 field number 反射全部字段,新增的
      // expected_offer_version(3)自动进入原像;这里必须跟着补,否则两端算出
      // 不同的 CanonicalPayloadHash。
      return [
        int32Field(payload.offerId, 'offer_id'),
        int32Field(payload.count, 'count'),
        uint64Field(payload.expectedOfferVersion, 'expected_offer_version'),
      ];
    case 'SELL':
      return [int32Field(payload.itemId, 'item_id'), {
        kind: 'int64',
        value: BigInt.asIntN(64, parseUint64(payload.count, 'count')),
      }];
    case 'PLANT':
      return [
        uint32Field(payload.plotId, 'plot_id'),
        uint32Field(payload.cropDefinitionId, 'crop_definition_id'),
        uint64Field(payload.targetFarmId, 'target_farm_id'),
        uint64Field(payload.expectedPlotVersion, 'expected_plot_version'),
        uint64Field(payload.expectedCropRuleVersion, 'expected_crop_rule_version'),
      ];
    case 'HARVEST':
      return [
        uint32Field(payload.plotId, 'plot_id'),
        uint64Field(payload.targetFarmId, 'target_farm_id'),
        { kind: 'string', value: payload.cropCycleId },
        uint64Field(payload.expectedPlotVersion, 'expected_plot_version'),
      ];
    case 'CARE': {
      // 字段集合与 FirstHop builder 对齐(incident_id/care_type 恒在);取值随
      // 实际 Payload:WATER 时 incident_id 为空、care_type=1;PEST 时携带权威
      // incident_id、care_type=2。
      const careType = payload.careType ?? 'WATER';
      return [
        uint32Field(payload.plotId, 'plot_id'),
        uint64Field(payload.targetFarmId, 'target_farm_id'),
        { kind: 'string', value: payload.cropCycleId },
        { kind: 'string', value: payload.incidentId ?? '' },
        { kind: 'enum', value: careType === 'PEST' ? 2 : 1 },
      ];
    }
    case 'THEFT':
      return [
        uint32Field(payload.plotId, 'plot_id'),
        uint64Field(payload.targetFarmId, 'target_farm_id'),
        { kind: 'string', value: payload.cropCycleId },
      ];
    case 'GET_PLAYER_BOOTSTRAP':
    case 'GET_TASK_LIST':
      return [];
    case 'CLAIM_TASK':
      return [{ kind: 'string', value: payload.taskInstanceId }];
    case 'CONFIRM_FRIEND_VISIT':
      return [uint64Field(payload.targetFarmId, 'target_farm_id')];
  }
};

/** CanonicalPreimage = uint32_be(command_type) ‖ CanonicalBytes(payload)。 */
export const canonicalPreimage = (payload: CommandPayload): Uint8Array => {
  const fields = canonicalFields(payload);
  const parts: Uint8Array[] = [];
  let total = 4;
  for (const field of fields) {
    let bytes: Uint8Array;
    switch (field.kind) {
      case 'uint32':
      case 'int32':
      case 'enum': {
        bytes = new Uint8Array(4);
        new DataView(bytes.buffer).setUint32(0, field.value >>> 0, false);
        break;
      }
      case 'uint64':
      case 'int64': {
        bytes = new Uint8Array(8);
        new DataView(bytes.buffer).setBigUint64(0, BigInt.asUintN(64, field.value), false);
        break;
      }
      case 'string': {
        const encoded = textEncoder.encode(field.value);
        bytes = new Uint8Array(4 + encoded.length);
        new DataView(bytes.buffer).setUint32(0, encoded.length, false);
        bytes.set(encoded, 4);
        break;
      }
    }
    parts.push(bytes);
    total += bytes.length;
  }
  const out = new Uint8Array(total);
  new DataView(out.buffer).setUint32(0, commandTypeNumber(payload.type) >>> 0, false);
  let offset = 4;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
};

const sha256Sync = (input: Uint8Array): Promise<ArrayBuffer> => {
  if (!globalThis.crypto?.subtle) return Promise.reject(new Error('Web Crypto SHA-256 is unavailable'));
  const buffer = new ArrayBuffer(input.byteLength);
  new Uint8Array(buffer).set(input);
  return globalThis.crypto.subtle.digest('SHA-256', buffer);
};

/** CanonicalPayloadHash(SHA-256,小写 hex)。 */
export const canonicalPayloadHash = async (payload: CommandPayload): Promise<string> => {
  const digest = new Uint8Array(await sha256Sync(canonicalPreimage(payload)));
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, '0')).join('');
};
