import { describe, expect, it } from 'vitest';
import {
  decodeReceiptRefV1,
  decodeWorkRefV1,
  encodeReceiptRefV1,
  encodeWorkRefV1,
  operationDigest,
  stableOpaqueId,
  validateStableOpaqueId,
  workflowId,
} from '../protocol/work-identity';

describe('contract 31 stable work identities', () => {
  it('matches the Go StableOpaqueID and operation digest golden vectors', async () => {
    const plantWorkflow = await workflowId('10001', 'PLANT', 'op-0001');
    expect(plantWorkflow).toBe('sid1_pK6aJIBpMU-PpnlrN1v-fHZrdkGG_fgNJOFHG70Bg4s');

    await expect(stableOpaqueId(
      'farm-source-order-id-v1',
      'CARE_REWARD',
      'FARM',
      '20002',
      'care-plot-3-cycle-7',
      '12',
    )).resolves.toBe('sid1_b5wzCfNHotiHpKRT4j50m8cG9Z81yurh409UIDCzGR0');

    await expect(stableOpaqueId(
      'farm-participant-invocation-v1',
      'PLANT',
      plantWorkflow,
      'RESERVE_SEED',
      '1',
    )).resolves.toBe('sid1_sj6dghgqP6O6OZwt9H35c70SKGQVldNqQCd5FPAlf8A');

    await expect(operationDigest('10001', 'CARE', 'op-care-0001')).resolves.toBe(
      '3bb259228c635917cc44509eb7fed0ba49b3af6e0515a297500dbdd9ce1548ee',
    );
  });

  it('matches the Go WorkRefV1 and ReceiptRefV1 vectors', () => {
    const workId = 'sid1_b5wzCfNHotiHpKRT4j50m8cG9Z81yurh409UIDCzGR0';
    const workRef = encodeWorkRefV1('CARE_REWARD', workId);
    expect(workRef).toBe(
      'wr1_AAAAC0NBUkVfUkVXQVJEAAAAMHNpZDFfYjV3ekNmTkhvdGlIcEtSVDRqNTBtOGNHOVo4MXl1cmg0MDlVSURDekdSMA',
    );
    expect(decodeWorkRefV1(workRef)).toEqual({ workType: 'CARE_REWARD', workId });

    const digest = '3bb259228c635917cc44509eb7fed0ba49b3af6e0515a297500dbdd9ce1548ee';
    const receiptRef = encodeReceiptRefV1('20002', 'AUTHORITY_RECEIPT', digest);
    expect(receiptRef).toBe(
      'rr1_AAAABTIwMDAyAAAAEUFVVEhPUklUWV9SRUNFSVBUAAAAQDNiYjI1OTIyOGM2MzU5MTdjYzQ0NTA5ZWI3ZmVkMGJhNDliM2FmNmUwNTE1YTI5NzUwMGRiZGQ5Y2UxNTQ4ZWU',
    );
    expect(decodeReceiptRefV1(receiptRef)).toEqual({
      actorId: '20002',
      receiptKind: 'AUTHORITY_RECEIPT',
      operationDigest: digest,
    });
  });

  it('fails closed on non-canonical or ambiguous values', async () => {
    await expect(workflowId('010001', 'PLANT', 'op')).rejects.toThrow(/canonical uint64/);
    await expect(workflowId('10001', 'Plant', 'op')).rejects.toThrow(/stable name/);
    await expect(workflowId('10001', 'PLANT', '\ud800')).rejects.toThrow(/unpaired surrogate/);
    expect(() => validateStableOpaqueId('sid1_short')).toThrow(/encoding/);

    const valid = await workflowId('1', 'PLANT', 'op');
    expect(() => validateStableOpaqueId(`${valid}=`)).toThrow();
    expect(() => decodeWorkRefV1('wr1_AAAAAQ')).toThrow();
    expect(() => encodeReceiptRefV1('02002', 'AUTHORITY_RECEIPT', 'a'.repeat(64))).toThrow(
      /canonical uint64/,
    );
  });
});
