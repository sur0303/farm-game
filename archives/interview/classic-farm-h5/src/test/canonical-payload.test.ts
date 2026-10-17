import { describe, expect, it } from 'vitest';
import { canonicalPayloadHash, canonicalPreimage, commandTypeNumber } from '../protocol/canonical-payload';
import { CommandType as ProtoCommandType } from '../protocol/generated/common/envelope';

const toHex = (bytes: Uint8Array): string =>
  Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');

// 与 farm-game/internal/protocoltest/p1b_canonical_golden_test.go 共用同一组
// 黄金:uint32_be(command_type) ‖ CanonicalBytes(payload) → SHA-256。
describe('canonical payload hash (SCB-1 golden)', () => {
  it('reproduces the PLANT golden preimage and hash byte-exactly', async () => {
    const preimage = canonicalPreimage({
      type: 'PLANT',
      plotId: 3,
      cropDefinitionId: 1,
      targetFarmId: 101,
      expectedPlotVersion: 9,
      expectedCropRuleVersion: 2,
    });
    expect(toHex(preimage)).toBe('000000040000000300000001000000000000006500000000000000090000000000000002');
    await expect(canonicalPayloadHash({
      type: 'PLANT',
      plotId: 3,
      cropDefinitionId: 1,
      targetFarmId: 101,
      expectedPlotVersion: 9,
      expectedCropRuleVersion: 2,
    })).resolves.toBe('583aa0b435d99a54c3f771f980eb77da2b99036baf6b4ad9b6ea6539fd08d7b1');
  });

  it('is stable for replay and conflicts on any semantic field change', async () => {
    const base = { type: 'HARVEST', plotId: 1, targetFarmId: 1, cropCycleId: 'cc-1', expectedPlotVersion: 7 } as const;
    const hashA = await canonicalPayloadHash(base);
    expect(await canonicalPayloadHash(base)).toBe(hashA);
    expect(await canonicalPayloadHash({ ...base, cropCycleId: 'cc-2' })).not.toBe(hashA);
    expect(await canonicalPayloadHash({ ...base, expectedPlotVersion: 8 })).not.toBe(hashA);

    const join = { type: 'JOIN_FARM', targetFarmId: 101 } as const;
    expect(await canonicalPayloadHash({ type: 'JOIN_FARM', targetFarmId: 102 })).not.toBe(await canonicalPayloadHash(join));

    const buy = { type: 'BUY', offerId: 1, count: 2 } as const;
    expect(await canonicalPayloadHash({ type: 'BUY', offerId: 1, count: 3 })).not.toBe(await canonicalPayloadHash(buy));

    const claim = { type: 'CLAIM_TASK', taskInstanceId: 'task-1' } as const;
    expect(await canonicalPayloadHash({ type: 'CLAIM_TASK', taskInstanceId: 'task-2' })).not.toBe(await canonicalPayloadHash(claim));
  });

  it('encodes LP strings and fixed-width integers per the SCB-1 rules', () => {
    // CLAIM_TASK(12) + LP("task-1")
    expect(toHex(canonicalPreimage({ type: 'CLAIM_TASK', taskInstanceId: 'task-1' }))).toBe(
      '0000000c00000006' + Array.from('task-1', (char) => char.charCodeAt(0).toString(16).padStart(2, '0')).join(''),
    );
    // JOIN_FARM(6) + uint64_be(101)
    expect(toHex(canonicalPreimage({ type: 'JOIN_FARM', targetFarmId: 101 }))).toBe(
      '000000060000000000000065',
    );
  });

  it('maps command types to the frozen 33-catalog numbers', () => {
    expect(commandTypeNumber('BUY')).toBe(ProtoCommandType.BUY);
    expect(commandTypeNumber('SELL')).toBe(ProtoCommandType.SELL);
    expect(commandTypeNumber('PLANT')).toBe(ProtoCommandType.PLANT);
    expect(commandTypeNumber('HARVEST')).toBe(ProtoCommandType.HARVEST);
    expect(commandTypeNumber('JOIN_FARM')).toBe(ProtoCommandType.JOIN_FARM);
    expect(commandTypeNumber('CARE')).toBe(ProtoCommandType.CARE);
    expect(commandTypeNumber('THEFT')).toBe(ProtoCommandType.THEFT);
    expect(commandTypeNumber('CLAIM_TASK')).toBe(ProtoCommandType.CLAIM_TASK);
    expect(commandTypeNumber('GET_PLAYER_BOOTSTRAP')).toBe(ProtoCommandType.GET_PLAYER_BOOTSTRAP);
    expect(commandTypeNumber('UNLOCK_PLOT')).toBe(ProtoCommandType.UNLOCK_PLOT);
  });

  it('canonicalizes UNLOCK_PLOT in frozen field-number order', () => {
    expect(toHex(canonicalPreimage({
      type: 'UNLOCK_PLOT',
      targetFarmId: '10001',
      plotId: 6,
      expectedPlotVersion: '11',
      expectedUnlockRuleVersion: '9',
    }))).toBe(
      '00000024'
      + '0000000000002711'
      + '00000006'
      + '000000000000000b'
      + '0000000000000009',
    );
  });

  it('keeps the WATER canonical image stable and reflects care type/incident fence for PEST', () => {
    const hex = (bytes: Uint8Array) => Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');
    const water = hex(canonicalPreimage({ type: 'CARE', plotId: 1, targetFarmId: '10001', cropCycleId: 'cycle-1' }));
    const waterExplicit = hex(canonicalPreimage({
      type: 'CARE', plotId: 1, targetFarmId: '10001', cropCycleId: 'cycle-1', careType: 'WATER', incidentId: '',
    }));
    // WATER 省略与显式空 fence 必须产生同一 canonical image(向后兼容既有 golden)。
    expect(waterExplicit).toBe(water);
    expect(water.endsWith('00000001')).toBe(true); // care_type = CARE_TYPE_WATER(1),uint32 BE

    const pest = hex(canonicalPreimage({
      type: 'CARE', plotId: 1, targetFarmId: '10001', cropCycleId: 'cycle-1', careType: 'PEST', incidentId: 'inc-9',
    }));
    expect(pest).not.toBe(water);
    expect(pest.endsWith('00000002')).toBe(true); // care_type = CARE_TYPE_PEST(2)
  });
});
